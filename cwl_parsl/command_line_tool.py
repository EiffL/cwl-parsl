from __future__ import absolute_import

import copy
import hashlib
import json
import locale
import logging
import os
import re
import shutil
from functools import cmp_to_key, partial
from typing import (Any, Callable, Dict,  # pylint: disable=unused-import
                    Generator, List, Optional, Set, Text, Type, TYPE_CHECKING,
                    Union, cast)

import schema_salad.validate as validate
from schema_salad.ref_resolver import file_uri, uri_file_path
from schema_salad.sourceline import SourceLine

import cwltool
from cwltool.process import (Process, UnsupportedRequirement,
                      _logger_validation_warnings, compute_checksums,
                      normalizeFilesDirs, shortname, uniquename)
from cwltool.loghandler import _logger
from cwltool.flatten import flatten
from cwltool.context import (LoadingContext,  # pylint: disable=unused-import
                      RuntimeContext, getdefault)

from .job import ParslJob

OutputPorts = Dict[Text, Union[None, Text, List[Union[Dict[Text, Any], Text]], Dict[Text, Any]]]

class ParslCommandLineTool(cwltool.process.Process):
    def __init__(self, toolpath_object, loadingContext):
        # type: (Dict[Text, Any], LoadingContext) -> None
        super(ParslCommandLineTool, self).__init__(toolpath_object, loadingContext)
        self.prov_obj = loadingContext.prov_obj

    def make_job_runner(self,
                        runtimeContext       # type: RuntimeContext
                       ):  # type: (...) -> Type[JobBase]

            # Initialises a Parsl job
            return ParslJob

    def job(self,
            job_order,         # type: Dict[Text, Text]
            output_callbacks,  # type: Callable[[Any, Any], Any]
            runtimeContext     # RuntimeContext
           ):
        # type: (...) -> Generator[Union[JobBase, CallbackJob], None, None]

        require_prefix = ""
        if self.metadata["cwlVersion"] == "v1.0":
            require_prefix = "http://commonwl.org/cwltool#"

        jobname = uniquename(runtimeContext.name or shortname(self.tool.get("id", "job")))
        print(jobname)

        # TODO: Here, overide the outdir, stage dir and tmp dir for this job
        runtimeContext.outdir='./out_'+jobname
        runtimeContext.tmpdir='./tmp_'+jobname
        runtimeContext.stagedir='./stage_'+jobname



        builder = self._init_job(job_order, runtimeContext)
        reffiles = copy.deepcopy(builder.files)

        # TODO: Use a Parsl compatible path mapper
        j = self.make_job_runner(runtimeContext)(
            builder, builder.job, None, self.requirements, self.hints, jobname)
        j.prov_obj = self.prov_obj
        j.successCodes = self.tool.get("successCodes")
        j.temporaryFailCodes = self.tool.get("temporaryFailCodes")
        j.permanentFailCodes = self.tool.get("permanentFailCodes")

        debug = _logger.isEnabledFor(logging.DEBUG)

        builder.requirements = j.requirements

        if self.tool.get("stdin"):
            with SourceLine(self.tool, "stdin", validate.ValidationException, debug):
                j.stdin = builder.do_eval(self.tool["stdin"])
                assert j.stdin is not None
                reffiles.append({"class": "File", "path": j.stdin})

        if self.tool.get("stderr"):
            with SourceLine(self.tool, "stderr", validate.ValidationException, debug):
                j.stderr = builder.do_eval(self.tool["stderr"])
                assert j.stderr is not None
                if os.path.isabs(j.stderr) or ".." in j.stderr:
                    raise validate.ValidationException(
                        "stderr must be a relative path, got '%s'" % j.stderr)

        if self.tool.get("stdout"):
            with SourceLine(self.tool, "stdout", validate.ValidationException, debug):
                j.stdout = builder.do_eval(self.tool["stdout"])
                assert j.stdout is not None
                if os.path.isabs(j.stdout) or ".." in j.stdout or not j.stdout:
                    raise validate.ValidationException(
                        "stdout must be a relative path, got '%s'" % j.stdout)

        j.outdir = builder.outdir
        j.tmpdir = builder.tmpdir
        j.stagedir = builder.stagedir

        readers = {}  # type: Dict[Text, Any]
        muts = set()  # type: Set[Text]

        shellcmd = self.get_requirement("ShellCommandRequirement")[0]
        if shellcmd:
            cmd = []  # type: List[Text]
            for b in builder.bindings:
                arg = builder.generate_arg(b)
                if b.get("shellQuote", True):
                    arg = [shellescape.quote(a) for a in aslist(arg)]
                cmd.extend(aslist(arg))
            j.command_line = ["/bin/sh", "-c", " ".join(cmd)]
        else:
            j.command_line = flatten(list(map(builder.generate_arg, builder.bindings)))

        j.pathmapper = builder.pathmapper
        j.collect_outputs = partial(
            self.collect_output_ports, self.tool["outputs"], builder,
            compute_checksum=getdefault(runtimeContext.compute_checksum, True),
            jobname=jobname,
            readers=readers)
        j.output_callback = output_callbacks

        yield j

    def collect_output_ports(self,
                             ports,                  # type: Set[Dict[Text, Any]]
                             builder,                # type: Builder
                             outdir,                 # type: Text
                             compute_checksum=True,  # type: bool
                             jobname="",             # type: Text
                             readers=None            # type: Dict[Text, Any]
                            ):  # type: (...) -> OutputPorts
        ret = {}  # type: OutputPorts
        debug = _logger.isEnabledFor(logging.DEBUG)
        try:
            fs_access = builder.make_fs_access(outdir)
            custom_output = fs_access.join(outdir, "cwl.output.json")
            if fs_access.exists(custom_output):
                with fs_access.open(custom_output, "r") as f:
                    ret = json.load(f)
                if debug:
                    _logger.debug(u"Raw output from %s: %s", custom_output,
                                  json_dumps(ret, indent=4))
            else:
                for i, port in enumerate(ports):
                    def makeWorkflowException(msg):
                        return WorkflowException(
                            u"Error collecting output for parameter '%s':\n%s"
                            % (shortname(port["id"]), msg))
                    with SourceLine(ports, i, makeWorkflowException, debug):
                        fragment = shortname(port["id"])
                        ret[fragment] = self.collect_output(port, builder, outdir, fs_access,
                                                            compute_checksum=compute_checksum)
            if ret:
                revmap = partial(revmap_file, builder, outdir)
                adjustDirObjs(ret, trim_listing)
                visit_class(ret, ("File", "Directory"), cast(Callable[[Any], Any], revmap))
                visit_class(ret, ("File", "Directory"), remove_path)
                normalizeFilesDirs(ret)
                visit_class(ret, ("File", "Directory"), partial(check_valid_locations, fs_access))

                if compute_checksum:
                    adjustFileObjs(ret, partial(compute_checksums, fs_access))

            validate.validate_ex(
                self.names.get_name("outputs_record_schema", ""), ret,
                strict=False, logger=_logger_validation_warnings)
            if ret is not None and builder.mutation_manager is not None:
                adjustFileObjs(ret, builder.mutation_manager.set_generation)
            return ret if ret is not None else {}
        except validate.ValidationException as e:
            raise WorkflowException(
                "Error validating output record. " + Text(e) + "\n in " +
                json_dumps(ret, indent=4))
        finally:
            if builder.mutation_manager and readers:
                for r in readers.values():
                    builder.mutation_manager.release_reader(jobname, r)

    def collect_output(self,
                       schema,                # type: Dict[Text, Any]
                       builder,               # type: Builder
                       outdir,                # type: Text
                       fs_access,             # type: StdFsAccess
                       compute_checksum=True  # type: bool
                      ):
        # type: (...) -> Optional[Union[Dict[Text, Any], List[Union[Dict[Text, Any], Text]]]]
        r = []  # type: List[Any]
        empty_and_optional = False
        debug = _logger.isEnabledFor(logging.DEBUG)
        if "outputBinding" in schema:
            binding = schema["outputBinding"]
            globpatterns = []  # type: List[Text]

            revmap = partial(revmap_file, builder, outdir)

            if "glob" in binding:
                with SourceLine(binding, "glob", WorkflowException, debug):
                    for gb in aslist(binding["glob"]):
                        gb = builder.do_eval(gb)
                        if gb:
                            globpatterns.extend(aslist(gb))

                    for gb in globpatterns:
                        if gb.startswith(outdir):
                            gb = gb[len(outdir) + 1:]
                        elif gb == ".":
                            gb = outdir
                        elif gb.startswith("/"):
                            raise WorkflowException(
                                "glob patterns must not start with '/'")
                        try:
                            prefix = fs_access.glob(outdir)
                            r.extend([{"location": g,
                                       "path": fs_access.join(builder.outdir,
                                           g[len(prefix[0])+1:]),
                                       "basename": os.path.basename(g),
                                       "nameroot": os.path.splitext(
                                           os.path.basename(g))[0],
                                       "nameext": os.path.splitext(
                                           os.path.basename(g))[1],
                                       "class": "File" if fs_access.isfile(g)
                                       else "Directory"}
                                      for g in sorted(fs_access.glob(
                                          fs_access.join(outdir, gb)),
                                          key=cmp_to_key(cast(
                                              Callable[[Text, Text],
                                                  int], locale.strcoll)))])
                        except (OSError, IOError) as e:
                            _logger.warning(Text(e))
                        except Exception:
                            _logger.error("Unexpected error from fs_access", exc_info=True)
                            raise

                for files in r:
                    rfile = files.copy()
                    revmap(rfile)
                    if files["class"] == "Directory":
                        ll = builder.loadListing or (binding and binding.get("loadListing"))
                        if ll and ll != "no_listing":
                            get_listing(fs_access, files, (ll == "deep_listing"))
                    else:
                        with fs_access.open(rfile["location"], "rb") as f:
                            contents = b""
                            if binding.get("loadContents") or compute_checksum:
                                contents = f.read(CONTENT_LIMIT)
                            if binding.get("loadContents"):
                                files["contents"] = contents
                            if compute_checksum:
                                checksum = hashlib.sha1()
                                while contents != b"":
                                    checksum.update(contents)
                                    contents = f.read(1024 * 1024)
                                files["checksum"] = "sha1$%s" % checksum.hexdigest()
                            f.seek(0, 2)
                            filesize = f.tell()
                        files["size"] = filesize

            optional = False
            single = False
            if isinstance(schema["type"], list):
                if "null" in schema["type"]:
                    optional = True
                if "File" in schema["type"] or "Directory" in schema["type"]:
                    single = True
            elif schema["type"] == "File" or schema["type"] == "Directory":
                single = True

            if "outputEval" in binding:
                with SourceLine(binding, "outputEval", WorkflowException, debug):
                    r = builder.do_eval(binding["outputEval"], context=r)

            if single:
                if not r and not optional:
                    with SourceLine(binding, "glob", WorkflowException, debug):
                        raise WorkflowException("Did not find output file with glob pattern: '{}'".format(globpatterns))
                elif not r and optional:
                    pass
                elif isinstance(r, list):
                    if len(r) > 1:
                        raise WorkflowException("Multiple matches for output item that is a single file.")
                    else:
                        r = r[0]

            if "secondaryFiles" in schema:
                with SourceLine(schema, "secondaryFiles", WorkflowException, debug):
                    for primary in aslist(r):
                        if isinstance(primary, dict):
                            primary.setdefault("secondaryFiles", [])
                            pathprefix = primary["path"][0:primary["path"].rindex("/")+1]
                            for sf in aslist(schema["secondaryFiles"]):
                                if isinstance(sf, dict) or "$(" in sf or "${" in sf:
                                    sfpath = builder.do_eval(sf, context=primary)
                                    subst = False
                                else:
                                    sfpath = sf
                                    subst = True
                                for sfitem in aslist(sfpath):
                                    if isinstance(sfitem, string_types):
                                        if subst:
                                            sfitem = {"path": substitute(primary["path"], sfitem)}
                                        else:
                                            sfitem = {"path": pathprefix+sfitem}
                                    if "path" in sfitem and "location" not in sfitem:
                                        revmap(sfitem)
                                    if fs_access.isfile(sfitem["location"]):
                                        sfitem["class"] = "File"
                                        primary["secondaryFiles"].append(sfitem)
                                    elif fs_access.isdir(sfitem["location"]):
                                        sfitem["class"] = "Directory"
                                        primary["secondaryFiles"].append(sfitem)

            if "format" in schema:
                for primary in aslist(r):
                    primary["format"] = builder.do_eval(schema["format"], context=primary)

            # Ensure files point to local references outside of the run environment
            adjustFileObjs(r, cast(  # known bug in mypy
                # https://github.com/python/mypy/issues/797
                Callable[[Any], Any], revmap))

            if not r and optional:
                return None

        if (not empty_and_optional and isinstance(schema["type"], dict)
                and schema["type"]["type"] == "record"):
            out = {}
            for f in schema["type"]["fields"]:
                out[shortname(f["name"])] = self.collect_output(  # type: ignore
                    f, builder, outdir, fs_access,
                    compute_checksum=compute_checksum)
            return out
        return r
