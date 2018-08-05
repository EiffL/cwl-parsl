import sys
import os
from schema_salad.validate import ValidationException
import cwltool
from cwltool.loghandler import _logger
from cwltool.context import LoadingContext, RuntimeContext
from cwltool.flatten import flatten
from cwltool.command_line_tool import CommandLineTool
from cwltool.errors import WorkflowException
from cwltool.argparser import arg_parser
from cwltool.docker import DockerCommandLineJob
from cwltool.singularity import SingularityCommandLineJob
from .shifter import ShifterCommandLineJob
from cwltool.job import CommandLineJob

def customMakeTool(toolpath_object, loadingContext):
    """Factory function passed to load_tool() which creates instances of the
    custom CommandLineTool which supports shifter jobs.
    """

    if isinstance(toolpath_object, dict) and toolpath_object.get("class") == "CommandLineTool":
        return customCommandLineTool(toolpath_object, loadingContext)
    return cwltool.context.default_make_tool(toolpath_object, loadingContext)

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
                return SingularityCommandLineJob
            elif runtimeContext.shifter:
                return ShifterCommandLineJob
            else:
                return DockerCommandLineJob
        else:
            for t in reversed(self.requirements):
                if t["class"] == "DockerRequirement":
                    raise UnsupportedRequirement(
                        "--no-container, but this CommandLineTool has "
                        "DockerRequirement under 'requirements'.")
            return CommandLineJob
