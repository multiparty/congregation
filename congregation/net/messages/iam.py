from congregation.net.messages.msg import Msg


class IAMMsg(Msg):
    def __init__(self, pid: int):
        super().__init__(pid)
        self.msg_type = "IAM"

    def __str__(self):
        return f"IAMMsg({self.pid})"
