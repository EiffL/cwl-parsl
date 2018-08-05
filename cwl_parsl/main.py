import sys
import os
from schema_salad.validate import ValidationException
import cwltool
import cwltool.main
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

import concurrent
from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED
import parsl
from parsl import DataFlowKernel, App
from .shifter import ShifterCommandLineJob

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

class ParslExecutor(cwltool.executors.JobExecutor):
    """
    Custom Parsl-based executor for a worflow
    """
    def __init__(self): # type: () -> None
        super(ParslExecutor, self).__init__()
        self.futures = set()

    def run_jobs(self,
                 process,           # type: Process
                 job_order_object,  # type: Dict[Text, Any]
                 logger,
                 runtimeContext     # type: RuntimeContext
                ):  # type: (...) -> None
        """ Execute the jobs for the given Process. """

        local_config = {
            "sites" : [
                { "site" : "Threads",
                  "auth" : { "channel" : None },
                  "execution" : {
                      "executor" : "threads",
                      "provider" : None,
                      "maxThreads" : 4
                  }
                }],
            "globals" : {"lazyErrors" : True}
        }
        dfk = DataFlowKernel(config=local_config)

        @App('python', dfk)
        def parsl_runner(job, rc, inputs=[]):
            """
            The inputs are dummy futures provided by the parent steps of the
            workflow, if there are some
            """
            return job.run(rc)

        process_run_id = None  # type: Optional[str]
        reference_locations = {}  # type: Dict[Text,Text]

        # define provenance profile for single commandline tool
        if not isinstance(process, cwltool.workflow.Workflow) \
                and runtimeContext.research_obj is not None:
            orcid = runtimeContext.orcid
            full_name = runtimeContext.cwl_full_name
            process.provenance_object = CreateProvProfile(
                runtimeContext.research_obj, orcid, full_name)
            process.parent_wf = process.provenance_object
        jobiter = process.job(job_order_object, self.output_callback, runtimeContext)

        try:
            for job in jobiter:
                if job:
                    if runtimeContext.builder is not None:
                        job.builder = runtimeContext.builder
                    if job.outdir:
                        self.output_dirs.add(job.outdir)
                    if runtimeContext.research_obj is not None:
                        if not isinstance(process, Workflow):
                            runtimeContext.prov_obj = process.provenance_object
                        else:
                            runtimeContext.prov_obj = job.prov_obj
                        assert runtimeContext.prov_obj
                        process_run_id, reference_locations = \
                                runtimeContext.prov_obj.evaluate(
                                        process, job, job_order_object,
                                        runtimeContext.make_fs_access,
                                        runtimeContext)
                        runtimeContext = runtimeContext.copy()
                        runtimeContext.process_run_id = process_run_id
                        runtimeContext.reference_locations = reference_locations

                    # Run the job within a Parsl App
                    #res = parsl_runner(job, runtimeContext)
                    job.run(runtimeContext)
                    # Adds the future to the set of futures
                    #self.futures.add(res.parent)
                else:
                    print(self.futures)
                    if self.futures:
                        done, notdone = concurrent.futures.wait(self.futures, return_when=FIRST_COMPLETED)
                        self.futures = notdone
                    else:
                        logger.error("Workflow cannot make any more progress.")
                        break

            # At the end, make sure all the steps have been processed
            if self.futures:
                concurrent.futures.wait(self.futures, return_when=ALL_COMPLETED)
        except (ValidationException, WorkflowException):
            raise
        except Exception as e:
            logger.exception("Got workflow error")
            raise WorkflowException(Text(e))

if __name__ == "__main__":

    parser = arg_parser()
    parser.description = 'Parsl executor for Common Workflow Language'
    parser.add_argument("--shifter",  action="store_true",
                             default=False, help="[experimental] Use "
                             "Shifter runtime for running containers at NERSC.")

    parsed_args = parser.parse_args(sys.argv[1:])

    # Trigger the argparse message if the cwl file is missing
    # Otherwise cwltool will use the default argparser
    if not parsed_args.workflow:
        if os.path.isfile("CWLFile"):
            setattr(parsed_args, "workflow", "CWLFile")
        else:
            _logger.error("")
            _logger.error("CWL document required, no input file was provided")
            parser.print_help()
            sys.exit(1)

    rc = RuntimeContext()
    rc.shifter = False

    if parsed_args.shifter:
        rc.shifter = True
        # Change default path as /var is not accessible in shifter
        rc.docker_outdir='/spool'
        rc.docker_libdir='/libdir'

    sys.exit(cwltool.main.main(
             args=parsed_args,
             executor=ParslExecutor(),
             loadingContext=LoadingContext(kwargs={'construct_tool_object':customMakeTool}),
             runtimeContext=rc))
