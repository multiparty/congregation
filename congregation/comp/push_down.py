from congregation.comp.base import DagRewriter


class PushDown(DagRewriter):
    def __init__(self):
        super(PushDown, self).__init__()
