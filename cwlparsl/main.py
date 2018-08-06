import sys
import os
import parsl
import cwltool
import argparse
import cwltool.main
from cwltool.loghandler import _logger
from cwltool.context import LoadingContext, RuntimeContext
from .argparser import arg_parser
from .command_line_tool import customMakeTool
from .configs import threads_config, slurm_config


def main():
    parser = arg_parser()
    parsed_args = parser.parse_args(sys.argv[1:])

    # Load the requested parsl configuration
    if parsed_args.parsl == 'slurm':
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
    elif not parsed_args.basedir:
        _logger.error("")
        _logger.error("Basedir is required for storing itermediate results")
        parser.print_help()
        sys.exit(1)

    rc = RuntimeContext(vars(parsed_args))
    rc.shifter = False
    parsed_args.__dict__['parallel'] = True

    rc.tmpdir_prefix = rc.basedir+'/tmp/tmp'
    rc.tmp_outdir_prefix = rc.basedir+'/out/out' # type: Text
    if parsed_args.shifter:
        rc.shifter = True
        rc.docker_outdir='/spooldir'
        rc.docker_stagedir=rc.basedir+'/stage'
        rc.docker_tmpdir='/tmpdir'

    lc = LoadingContext(vars(parsed_args))
    lc.construct_tool_object = customMakeTool

    sys.exit(cwltool.main.main(
             args=parsed_args,
             loadingContext=lc,
             runtimeContext=rc))


if __name__ == "__main__":
    main()
