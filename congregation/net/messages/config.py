from congregation.net.messages.msg import Msg


class ConfigMsg(Msg):
    def __init__(self, pid: int, config: dict, job_type: str):
        super().__init__(pid)
        self.msg_type = "CONFIG"
        self.config = config
        self.job_type = job_type

    def __str__(self):
        return f"ConfigMsg({self.pid}): {self.job_type}"
