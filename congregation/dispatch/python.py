from subprocess import call
from congregation.dispatch.dispatcher import Dispatcher
from congregation.config import Config
from congregation.job import PythonJob


class PythonDispatcher(Dispatcher):
    def __init__(self, peer, config: Config):
        super().__init__(peer, config)
        self.dispatch_type = "PYTHON"

    def dispatch(self, job: PythonJob):

        cmd = f"{job.code_dir}/{job.name}/workflow.py"
        print(f"Running python job at {job.code_dir}/{job.name}/workflow.py")
        call(["python", cmd])
