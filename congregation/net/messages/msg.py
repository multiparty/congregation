

class Msg:
    def __init__(self, pid):
        self.pid = pid
        self.msg_type = "MSG"

    def __str__(self):
        return f"Msg({self.pid})"
