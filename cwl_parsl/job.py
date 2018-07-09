import cwltool
from parsl import App

class ParslJob(cwltool.job.JobBase):
    """
    A Parsl Job
    """

    def run(self, runtimeContext):
        """
        Actually runs the parsl app
        """
        print(self.command_line)
        print(self.stdout)
        print(self.outdir)
        print(self.stagedir)
        print(self.tmpdir)
        print(self.generatefiles)
        print(self.pathmapper.files())

        # Setup step, creates outdir, cheks that all necessary files are
        # accessible

        # File staging step, move or symlink the input files

        # Execute step, create path for outputs

        @App('bash', runtimeContext.dfk)
        def bash_app(stdout=self.stdout, stderr=self.stderr):
            return " ".join(self.command_line)

        # Runs app
        app_future = bash_app()

        # Define process status...


        #outputs = self.collect_outputs(self.outdir)
        #outputs = bytes2str_in_dicts(outputs)  # type: ignore

        return app_future
