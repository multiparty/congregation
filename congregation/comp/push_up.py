from congregation.comp.base import DagRewriter


class PushUp(DagRewriter):
    def __init__(self):
        super(PushUp, self).__init__()
        self.reverse = True
