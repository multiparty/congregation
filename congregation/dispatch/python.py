from subprocess import call
from congregation.dispatch.dispatcher import Dispatcher
from congregation.net import Peer
from congregation.config import Config
from congregation.job import PythonJob


class PythonDispatcher(Dispatcher):
    def __init__(self, peer: Peer, config: Config):
        super().__init__(peer, config)

    def dispatch(self, job: PythonJob):

        cmd = f"{job.code_dir}/{job.name}/workflow.py"
        print(f"Running python job at {job.code_dir}/{job.name}/workflow.py")
        call(["python", cmd])

    def synchronize(self):
        """
        TODO: talk to other parties and exit once all have exchanged ready messages
        TODO: might need async def here, then can call like:
         dispatcher.synchronize()
         dispatcher.dispatch(job)
        """
        pass
