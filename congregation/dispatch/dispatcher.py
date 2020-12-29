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
        self.peer.register_dispatcher(self)

    def synchronize(self):
        """ Overridden in subclasses """
        pass

    def setup_config(self):
        return {}

    def receive_msg(self, msg):

        ready_peer = msg.pid
        if ready_peer in self.parties_ready:
            self.parties_ready[ready_peer].set_result = True
        else:
            print(f"Received ReadyMsg from unrecognized party: {msg.pid}")
