import sys
import os
import parsl
import cwltool
import cwltool.main
from cwltool.loghandler import _logger
from cwltool.context import LoadingContext, RuntimeContext
from cwltool.argparser import arg_parser
from typing import (Dict, List,  # pylint: disable=unused-import
                    MutableMapping, Optional, cast, Text)

from .command_line_tool import customMakeTool
from .configs import threads_config, slurm_config

def main():
    parser = arg_parser()
    parser.description = 'Parsl executor for Common Workflow Language'
    parser.add_argument("--shifter",  action="store_true",
                             default=False, help="[experimental] Use "
                             "Shifter runtime for running containers at NERSC.")

    parser.add_argument("--parsl_config", type=Text, default="threads",
                        help="Parsl execution site. Either threads, or slurm.")

    parsed_args = parser.parse_args(sys.argv[1:])

    # Load the requested parsl configuration
    if parsed_args.parsl_config == 'slurm':
        parsl.load(slurm_config)
    else:
        parsl.load(threads_config)

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

    rc = RuntimeContext(vars(parsed_args))
    rc.shifter = False

    if parsed_args.shifter:
        rc.shifter = True
        # Change default path as /var is not accessible in shifter
        rc.docker_outdir='/spooldir'
        rc.docker_stagedir='/stagedir'
        rc.docker_tmpdir='/tmpdir'

    lc = LoadingContext(vars(parsed_args))
    lc.construct_tool_object = customMakeTool

    sys.exit(cwltool.main.main(
             args=parsed_args,
             loadingContext=lc,
             runtimeContext=rc))

if __name__ == "__main__":
    main()
