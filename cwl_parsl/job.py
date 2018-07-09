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
        print(self.)

        @App('bash', runtimeContext.dfk)
        def bash_app(stdout=self.stdout, stderr=self.stderr):
            return " ".join(self.command_line)

        app_future = bash_app()

        return app_future
