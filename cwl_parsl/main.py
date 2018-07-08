import sys
import cwltool
import cwltool.main
from cwltool.loghandler import _logger
from cwltool.context import LoadingContext, RuntimeContext
from cwltool.flatten import flatten
from .command_line_tool import ParslCommandLineTool

import parsl
from parsl import DataFlowKernel


def parslMakeTool(toolpath_object, loadingContext):
    """Factory function passed to load_tool() which creates instances of the
    custom ParslCommandLineTool.
    """

    if isinstance(toolpath_object, dict) and toolpath_object.get("class") == "CommandLineTool":
        return ParslCommandLineTool(toolpath_object, loadingContext)
    return cwltool.context.default_make_tool(toolpath_object, loadingContext)


class ParslExecutor(cwltool.executors.JobExecutor):
    """
    Custom Parsl-based executor for a worflow
    """
    # We can visit the workflow with this
    def visitSteps(self, t, op):
        if isinstance(t, cwltool.workflow.Workflow):
            for s in t.steps:
                op(s.tool)
                self.visitSteps(s.embedded_tool, op)
        else:
            print(t)

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
        runtimeContext.dfk = dfk
        print(runtimeContext.dfk)
        print("--------------- Beginning visit")
        self.visitSteps(process, lambda x: print(x['id']))
        print("--------------- Done visit")
        jobiter = process.job(job_order_object, self.output_callback, runtimeContext)
        for j in jobiter:
            if j:
                print(j)
                res = j.run(runtimeContext)
                if res:
                    print(res.result())
            else:
                logger.error("Workflow cannot make any more progress.")
                break

    def execute(self,
                process,           # type: Process
                job_order_object,  # type: Dict[Text, Any]
                runtimeContext,    # type: RuntimeContext
                logger=_logger,
               ):  # type: (...) -> Tuple[Optional[Dict[Text, Any]], Text]
        """ Execute the process. """

        self.run_jobs(process, job_order_object, logger, runtimeContext)

        return  (None, "permanentFail")

if __name__ == "__main__":

    sys.exit(cwltool.main.main(sys.argv[1:],
             executor=ParslExecutor(),
             loadingContext=LoadingContext(kwargs={'construct_tool_object':parslMakeTool})))
