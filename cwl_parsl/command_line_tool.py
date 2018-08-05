import sys
import os
import tempfile
import logging
import shutil

import shellescape
from schema_salad.validate import ValidationException
from typing import (IO, Any, AnyStr, Callable,  # pylint: disable=unused-import
                    Dict, Iterable, List, MutableMapping, Optional, Text,
                    Union, cast, TYPE_CHECKING)
import cwltool
from cwltool.context import (RuntimeContext,  # pylint: disable=unused-import
                      getdefault)
from cwltool.loghandler import _logger
from cwltool.context import LoadingContext, RuntimeContext
from cwltool.flatten import flatten
from cwltool.command_line_tool import CommandLineTool
from cwltool.errors import WorkflowException
from cwltool.utils import (  # pylint: disable=unused-import
    DEFAULT_TMP_PREFIX, Directory, copytree_with_merge, json_dump, json_dumps,
    onWindows, subprocess, bytes2str_in_dicts)
from cwltool.argparser import arg_parser
from cwltool.docker import DockerCommandLineJob
from cwltool.singularity import SingularityCommandLineJob
from .shifter import ShifterCommandLineJob
from cwltool.job import CommandLineJob, SHELL_COMMAND_TEMPLATE, PYTHON_RUN_SCRIPT
from cwltool.job import needs_shell_quoting_re
from io import IOBase, open  # pylint: disable=redefined-builtin

import parsl
from parsl.app.app import python_app, bash_app

def customMakeTool(toolpath_object, loadingContext):
    """Factory function passed to load_tool() which creates instances of the
    custom CommandLineTool which supports shifter jobs.
    """

    if isinstance(toolpath_object, dict) and toolpath_object.get("class") == "CommandLineTool":
        return customCommandLineTool(toolpath_object, loadingContext)
    return cwltool.context.default_make_tool(toolpath_object, loadingContext)

@bash_app
def run_process(job_dir, job_script, stdout='stdout.txt', stderr='stderr.txt'):
    return f"cd {job_dir} && bash {job_script}"

def _job_popen(
        commands,                  # type: List[Text]
        stdin_path,                # type: Optional[Text]
        stdout_path,               # type: Optional[Text]
        stderr_path,               # type: Optional[Text]
        env,                       # type: MutableMapping[AnyStr, AnyStr]
        cwd,                       # type: Text
        job_dir,                   # type: Text
        job_script_contents=None,  # type: Text
        timelimit=None,            # type: int
        name=None                  # type: Text
       ):  # type: (...) -> int

    if not job_script_contents:
        job_script_contents = SHELL_COMMAND_TEMPLATE

    env_copy = {}
    key = None  # type: Any
    for key in env:
        env_copy[key] = env[key]

    job_description = dict(
        commands=commands,
        cwd=cwd,
        env=env_copy,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        stdin_path=stdin_path,
    )
    with open(os.path.join(job_dir, "job.json"), encoding='utf-8', mode="w") as job_file:
        json_dump(job_description, job_file, ensure_ascii=False)
    try:
        job_script = os.path.join(job_dir, "run_job.bash")
        with open(job_script, "wb") as _:
            _.write(job_script_contents.encode('utf-8'))
        job_run = os.path.join(job_dir, "run_job.py")
        with open(job_run, "wb") as _:
            _.write(PYTHON_RUN_SCRIPT.encode('utf-8'))

        proc = run_process(job_dir, job_script)

        rcode = proc.result()

        return rcode
    finally:
        shutil.rmtree(job_dir)

def _parsl_execute(self,
             runtime,                # type: List[Text]
             env,                    # type: MutableMapping[Text, Text]
             runtimeContext          # type: RuntimeContext
            ):  # type: (...) -> None

    scr, _ = self.get_requirement("ShellCommandRequirement")

    shouldquote = needs_shell_quoting_re.search   # type: Callable[[Any], Any]
    if scr:
        shouldquote = lambda x: False

    _logger.info(u"[job %s] %s$ %s%s%s%s",
                 self.name,
                 self.outdir,
                 " \\\n    ".join([shellescape.quote(Text(arg)) if shouldquote(Text(arg)) else Text(arg) for arg in
                                   (runtime + self.command_line)]),
                 u' < %s' % self.stdin if self.stdin else '',
                 u' > %s' % os.path.join(self.outdir, self.stdout) if self.stdout else '',
                 u' 2> %s' % os.path.join(self.outdir, self.stderr) if self.stderr else '')
    if self.joborder and runtimeContext.research_obj:
        job_order = self.joborder
        assert runtimeContext.prov_obj
        runtimeContext.prov_obj.used_artefacts(
            job_order, runtimeContext.process_run_id,
            runtimeContext.reference_locations, str(self.name))
    outputs = {}  # type: Dict[Text,Text]
    try:
        stdin_path = None
        if self.stdin:
            rmap = self.pathmapper.reversemap(self.stdin)
            if not rmap:
                raise WorkflowException(
                    "{} missing from pathmapper".format(self.stdin))
            else:
                stdin_path = rmap[1]

        stderr_path = None
        if self.stderr:
            abserr = os.path.join(self.outdir, self.stderr)
            dnerr = os.path.dirname(abserr)
            if dnerr and not os.path.exists(dnerr):
                os.makedirs(dnerr)
            stderr_path = abserr

        stdout_path = None
        if self.stdout:
            absout = os.path.join(self.outdir, self.stdout)
            dn = os.path.dirname(absout)
            if dn and not os.path.exists(dn):
                os.makedirs(dn)
            stdout_path = absout

        commands = [Text(x) for x in (runtime + self.command_line)]
        if runtimeContext.secret_store:
            commands = runtimeContext.secret_store.retrieve(commands)
            env = runtimeContext.secret_store.retrieve(env)

        job_script_contents = None  # type: Optional[Text]
        builder = getattr(self, "builder", None)  # type: Builder
        if builder is not None:
            job_script_contents = builder.build_job_script(commands)

        print("Running my own execution layer")
        rcode = _job_popen(commands,
            stdin_path=stdin_path,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            env=env,
            cwd=self.outdir,
            job_dir=tempfile.mkdtemp(prefix=getdefault(runtimeContext.tmp_outdir_prefix, DEFAULT_TMP_PREFIX)),
            job_script_contents=job_script_contents,
            timelimit=self.timelimit,
            name=self.name)

        if self.successCodes and rcode in self.successCodes:
            processStatus = "success"
        elif self.temporaryFailCodes and rcode in self.temporaryFailCodes:
            processStatus = "temporaryFail"
        elif self.permanentFailCodes and rcode in self.permanentFailCodes:
            processStatus = "permanentFail"
        elif rcode == 0:
            processStatus = "success"
        else:
            processStatus = "permanentFail"

        if self.generatefiles["listing"]:
            assert self.generatemapper is not None
            relink_initialworkdir(
                self.generatemapper, self.outdir, self.builder.outdir,
                inplace_update=self.inplace_update)

        outputs = self.collect_outputs(self.outdir)
        outputs = bytes2str_in_dicts(outputs)  # type: ignore
    except OSError as e:
        if e.errno == 2:
            if runtime:
                _logger.error(u"'%s' not found", runtime[0])
            else:
                _logger.error(u"'%s' not found", self.command_line[0])
        else:
            _logger.exception("Exception while running job")
        processStatus = "permanentFail"
    except WorkflowException as e:
        _logger.error(u"[job %s] Job error:\n%s" % (self.name, e))
        processStatus = "permanentFail"
    except Exception as e:
        _logger.exception("Exception while running job")
        processStatus = "permanentFail"
    if runtimeContext.research_obj and self.prov_obj and \
            runtimeContext.process_run_id:
        #creating entities for the outputs produced by each step (in the provenance document)
        self.prov_obj.generate_output_prov(
            outputs, runtimeContext.process_run_id, str(self.name))
        self.prov_obj.document.wasEndedBy(
            runtimeContext.process_run_id, None, self.prov_obj.workflow_run_uri,
            datetime.datetime.now())
    if processStatus != "success":
        _logger.warning(u"[job %s] completed %s", self.name, processStatus)
    else:
        _logger.info(u"[job %s] completed %s", self.name, processStatus)

    if _logger.isEnabledFor(logging.DEBUG):
        _logger.debug(u"[job %s] %s", self.name,
                      json_dumps(outputs, indent=4))

    if self.generatemapper and runtimeContext.secret_store:
        # Delete any runtime-generated files containing secrets.
        for f, p in self.generatemapper.items():
            if p.type == "CreateFile":
                if runtimeContext.secret_store.has_secret(p.resolved):
                    host_outdir = self.outdir
                    container_outdir = self.builder.outdir
                    host_outdir_tgt = p.target
                    if p.target.startswith(container_outdir+"/"):
                        host_outdir_tgt = os.path.join(
                            host_outdir, p.target[len(container_outdir)+1:])
                    os.remove(host_outdir_tgt)

    if runtimeContext.workflow_eval_lock is None:
        raise WorkflowException("runtimeContext.workflow_eval_lock must not be None")

    with runtimeContext.workflow_eval_lock:
        self.output_callback(outputs, processStatus)

    if self.stagedir and os.path.exists(self.stagedir):
        _logger.debug(u"[job %s] Removing input staging directory %s", self.name, self.stagedir)
        shutil.rmtree(self.stagedir, True)

    if runtimeContext.rm_tmpdir:
        _logger.debug(u"[job %s] Removing temporary directory %s", self.name, self.tmpdir)
        shutil.rmtree(self.tmpdir, True)

class customCommandLineTool(cwltool.command_line_tool.CommandLineTool):

    def make_job_runner(self,
                        runtimeContext       # type: RuntimeContext
                       ):  # type: (...) -> Type[JobBase]
        dockerReq, _ = self.get_requirement("DockerRequirement")
        if not dockerReq and runtimeContext.use_container:
            if runtimeContext.find_default_container:
                default_container = runtimeContext.find_default_container(self)
                if default_container:
                    self.requirements.insert(0, {
                        "class": "DockerRequirement",
                        "dockerPull": default_container
                    })
                    dockerReq = self.requirements[0]
                    if default_container == windows_default_container_id and runtimeContext.use_container and onWindows():
                        _logger.warning(DEFAULT_CONTAINER_MSG % (windows_default_container_id, windows_default_container_id))

        if dockerReq and runtimeContext.use_container:
            if runtimeContext.singularity:
                SingularityCommandLineJob._execute = _parsl_execute
                return SingularityCommandLineJob
            elif runtimeContext.shifter:
                ShifterCommandLineJob._execute = _parsl_execute
                return ShifterCommandLineJob
            else:
                DockerCommandLineJob._execute = _parsl_execute
                return DockerCommandLineJob
        else:
            for t in reversed(self.requirements):
                if t["class"] == "DockerRequirement":
                    raise UnsupportedRequirement(
                        "--no-container, but this CommandLineTool has "
                        "DockerRequirement under 'requirements'.")
            CommandLineJob._execute = _parsl_execute
            return CommandLineJob
