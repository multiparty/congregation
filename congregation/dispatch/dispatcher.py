import asyncio
from congregation.config import Config
from congregation.job.job import Job


class Dispatcher:
    def __init__(self, peer, config: Config):
        self.dispatch_type = "DISPATCHER"
        self.peer = peer
        self.config = config
        self.pid = config.system_configs["CODEGEN"].pid
        self.parties_ready = {
            p: asyncio.Future()
            for p in config.system_configs["CODEGEN"].all_pids
            if p != self.pid
        }
        self.parties_config = {
            p: {"CFG": asyncio.Future(), "ACK": asyncio.Future()}
            for p in config.system_configs["CODEGEN"].all_pids
            if p != self.pid
        }
        self.config_to_exchange = self.setup_config()

    def dispatch(self, job: Job):
        """ overridden in subclasses """
        pass

    def setup_dispatch(self, job: Job):

        self.peer.register_dispatcher(self)
        self.synchronize(job)

    def synchronize(self, job: Job):
        """ overridden in subclasses """
        pass

    def setup_config(self):
        return {}
