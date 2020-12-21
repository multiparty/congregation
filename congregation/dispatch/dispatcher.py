from congregation.net import Peer
from congregation.config import Config
from congregation.job.job import Job


class Dispatcher:
    def __init__(self, peer: Peer, config: Config):
        self.peer = peer
        self.config = config
        self.pid = config.system_configs["CODEGEN"].pid

    def dispatch(self, job: Job):
        """ Overridden in subclasses """
        pass

    def synchronize(self):
        """ Overridden in subclasses """
        pass
