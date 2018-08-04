"""Support for executing Docker containers using Shifter."""
from __future__ import absolute_import

import logging
import os
import os.path
import re
import shutil
import sys
import threading
from io import open  # pylint: disable=redefined-builtin
from typing import (Dict, List,  # pylint: disable=unused-import
                    MutableMapping, Optional, Text)

from schema_salad.sourceline import SourceLine

from cwltool.errors import WorkflowException
from cwltool.job import ContainerCommandLineJob
from cwltool.pathmapper import (PathMapper,  # pylint: disable=unused-import
                         ensure_writable)
from cwltool.process import UnsupportedRequirement
from cwltool.utils import docker_windows_path_adjust, subprocess
from cwltool.context import RuntimeContext

found_shifter_images = set()  # type: Set[Text]
found_shifter_images_lock = threading.Lock()

_logger = logging.getLogger("cwltool")

class ShifterCommandLineJob(ContainerCommandLineJob):

    @staticmethod
    def get_image(dockerRequirement,  # type: Dict[Text, Text]
                  pull_image,         # type: bool
                  force_pull=False    # type: bool
                 ):
        # type: (...) -> bool
        """
        Acquire the software container image in the specified dockerRequirement
        using Shifterimg and returns the success as a bool. Note that shifter
        doesn't support dockerImageId, so only dockerPull is considered.
        """
        found = False

        if "dockerImageId" not in dockerRequirement and "dockerPull" in dockerRequirement:
            dockerRequirement["dockerImageId"] = dockerRequirement["dockerPull"]

        with found_shifter_images_lock:
            if dockerRequirement["dockerImageId"] in found_images:
                return True

        for ln in subprocess.check_output(["shifterimg", "images"]).decode('utf-8').splitlines():
            try:
                m = ln.split()
                sp = dockerRequirement["dockerImageId"].split(":")
                if len(sp) == 1:
                    sp.append("latest")
                elif len(sp) == 2:
                    #  if sp[1] doesn't  match valid tag names, it is a part of repository
                    if not re.match(r'[\w][\w.-]{0,127}', sp[1]):
                        sp[0] = sp[0] + ":" + sp[1]
                        sp[1] = "latest"
                elif len(sp) == 3:
                    if re.match(r'[\w][\w.-]{0,127}', sp[2]):
                        sp[0] = sp[0] + ":" + sp[1]
                        sp[1] = sp[2]
                        del sp[2]

                # check for repository:tag match or image id match
                if (m and (((sp[0]+':'+sp[1]) == m[-1]) or dockerRequirement["dockerImageId"] == m[-1])):
                    found = True
                    break
            except ValueError:
                pass

        if (force_pull or not found) and pull_image:
            cmd = []  # type: List[Text]
            if "dockerPull" in dockerRequirement:
                cmd = ["shifterimg", "pull", str(dockerRequirement["dockerPull"])]
                _logger.info(Text(cmd))
                check_call(cmd, stdout=sys.stderr)
                found = True
            elif "dockerFile" in dockerRequirement:
                raise WorkflowException(SourceLine(
                    dockerRequirement, 'dockerFile').makeError(
                    "dockerFile is not currently supported when using the "
                    "Shifter runtime for Docker containers."))
            elif "dockerLoad" in dockerRequirement:
                raise WorkflowException(SourceLine(
                    dockerRequirement, 'dockerLoad').makeError(
                    "dockerLoad is not currently supported when using the "
                    "Shifter runtime for Docker containers."))
            elif "dockerImport" in dockerRequirement:
                raise WorkflowException(SourceLine(
                    dockerRequirement, 'dockerImport').makeError(
                    "dockerImport is not currently supported when using the "
                    "Shifter runtime for Docker containers."))

        if found:
            with found_shifter_images_lock:
                found_shifter_images.add(dockerRequirement["dockerImageId"])

        return found

    def get_from_requirements(self,
                              r,                      # type: Optional[Dict[Text, Text]]
                              req,                    # type: bool
                              pull_image,             # type: bool
                              force_pull=False,       # type: bool
                              tmp_outdir_prefix=None  # type: Text
                             ):
        # type: (...) -> Optional[Text]
        """
        Returns the filename of the Singularity image (e.g.
        hello-world-latest.img).
        """

        if r:
            errmsg = None
            try:
                check_output(["shifterimg", "--help"])
            except CalledProcessError as err:
                errmsg = "Cannot execute 'shifterimg --help' {}".format(err)
            except OSError as err:
                errmsg = "'shifterimg' executable not found: {}".format(err)

            if errmsg:
                if req:
                    raise WorkflowException(errmsg)
                else:
                    return None

            if self.get_image(r, pull_image, force_pull):
                return 'docker:'+r["dockerImageId"]
            else:
                if req:
                    raise WorkflowException(u"Container image {} not "
                                            "found".format(r["dockerImageId"]))

        return None

    def add_volumes(self, pathmapper, mounts, secret_store=None):
        # type: (PathMapper, List[Text], SecretStore) -> None

        host_outdir = self.outdir
        container_outdir = self.builder.outdir
        mounts = []
        for src, vol in pathmapper.items():
            if not vol.staged:
                continue
            host_outdir_tgt = None  # type: Optional[Text]
            if vol.target.startswith(container_outdir+"/"):
                host_outdir_tgt = os.path.join(
                    host_outdir, vol.target[len(container_outdir)+1:])
            if vol.type in ("File", "Directory"):
                if not vol.resolved.startswith("_:"):
                    mounts.append(u"%s:%s:ro" % (
                        docker_windows_path_adjust(vol.resolved),
                        docker_windows_path_adjust(vol.target)))
            elif vol.type == "WritableFile":
                if self.inplace_update:
                    mounts.append(u"%s:%s:rw" % (
                        docker_windows_path_adjust(vol.resolved),
                        docker_windows_path_adjust(vol.target)))
                else:
                    if host_outdir_tgt:
                        shutil.copy(vol.resolved, host_outdir_tgt)
                        ensure_writable(host_outdir_tgt)
                    else:
                        raise WorkflowException(
                            "Unable to compute host_outdir_tgt for "
                            "WriteableFile.")
            elif vol.type == "WritableDirectory":
                if vol.resolved.startswith("_:"):
                    if host_outdir_tgt:
                        os.makedirs(host_outdir_tgt, 0o0755)
                    else:
                        raise WorkflowException(
                            "Unable to compute host_outdir_tgt for "
                            "WritableDirectory.")
                else:
                    if self.inplace_update:
                        mounts.append(u"%s:%s:rw" % (
                            docker_windows_path_adjust(vol.resolved),
                            docker_windows_path_adjust(vol.target)))
                    else:
                        if host_outdir_tgt:
                            shutil.copytree(vol.resolved, host_outdir_tgt)
                            ensure_writable(host_outdir_tgt)
                        else:
                            raise WorkflowException(
                                "Unable to compute host_outdir_tgt for "
                                "WritableDirectory.")
            elif vol.type == "CreateFile":
                if secret_store:
                    contents = secret_store.retrieve(vol.resolved)
                else:
                    contents = vol.resolved
                if host_outdir_tgt:
                    with open(host_outdir_tgt, "wb") as f:
                        f.write(contents.encode("utf-8"))
                else:
                    fd, createtmp = tempfile.mkstemp(dir=self.tmpdir)
                    with os.fdopen(fd, "wb") as f:
                        f.write(contents.encode("utf-8"))
                    mounts.append(u"%s:%s:rw" % (
                        docker_windows_path_adjust(createtmp),
                        docker_windows_path_adjust(vol.target)))
        return mounts


    def create_runtime(self,
                       env,                        # type: MutableMapping[Text, Text]
                       runtimeContext              # type: RuntimeContext
                      ):
        # type: (...) -> List
        """ Returns the Shifter runtime list of commands and options."""

        runtime = [u"shifter"]
        mounts = []

        mounts.append(u"%s:%s:rw" % (
            docker_windows_path_adjust(os.path.realpath(self.outdir)),
            self.builder.outdir))
        mounts.append(u"%s:%s:rw" % (
            docker_windows_path_adjust(os.path.realpath(self.tmpdir)), "/tmp"))

        self.add_volumes(self.pathmapper, mounts, secret_store=runtimeContext.secret_store)
        if self.generatemapper:
            self.add_volumes(self.generatemapper, mounts, secret_store=runtimeContext.secret_store)

        # if user_space_docker_cmd:
        #     runtime = [x.replace(":ro", "") for x in runtime]
        #     runtime = [x.replace(":rw", "") for x in runtime]

        if len(mounts) > 0:
            runtime.append(u"--volume="+ (';'.join(mounts)))

        runtime.append(u"--workdir=%s" % (
            docker_windows_path_adjust(self.builder.outdir)))

        runtime.append(u"--env=TMPDIR=/tmp")

        # spec currently says "HOME must be set to the designated output
        # directory." but spec might change to designated temp directory.
        # runtime.append("--env=HOME=/tmp")
        runtime.append(u"--env=HOME=%s" % self.builder.outdir)

        if runtimeContext.custom_net is not None:
            raise UnsupportedRequirement(
                "Shifter implementation does not support custom networking")
        if runtimeContext.disable_net:
            raise UnsupportedRequirement(
                "Shifter implementation does not support custom networking")
        if runtimeContext.record_container_id:
            raise UnsupportedRequirement(
                "Shifter implementation does not support recording container id")

        for t, v in self.environment.items():
            runtime.append(u"--env=%s=%s" % (t, v))

        runtime.append(u"--image=")

        return runtime
