from congregation.net.messages.msg import Msg


class RequestMsg(Msg):
    def __init__(self, pid: int, request_type: str, job_type: str):
        super().__init__(pid)
        self.msg_type = "REQUEST"
        self.request_type = request_type
        self.job_type = job_type

    def __str__(self):
        return f"RequestMsg({self.pid}) : {self.job_type} for {self.request_type}"
