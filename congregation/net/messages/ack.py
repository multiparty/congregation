from congregation.net.messages.msg import Msg


class AckMsg(Msg):
    def __init__(self, pid: int, ack_type: str, job_type: str):
        super().__init__(pid)
        self.msg_type = "ACK"
        self.ack_type = ack_type
        self.job_type = job_type

    def __str__(self):
        return f"AckMsg({self.pid}): {self.job_type} for {self.ack_type}"
