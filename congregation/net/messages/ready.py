from congregation.net.messages.msg import Msg


class ReadyMsg(Msg):
    def __init__(self, pid: int, job_type: str):
        super().__init__(pid)
        self.msg_type = "READY"
        self.job_type = job_type

    def __str__(self):
        return f"ReadyMsg({self.pid}) : {self.job_type}"
