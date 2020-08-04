from congregation.dag.nodes import UnaryOpNode
from congregation.dag.nodes.base import OpNode
from congregation.datasets import Relation


class Store(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode):
        super(Store, self).__init__("store", out_rel, parent)

    def is_reversible(self):
        return True


class Persist(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode):
        super(Persist, self).__init__("persist", out_rel, parent)

    def is_reversible(self):
        return True


class Send(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode):
        super(Send, self).__init__("send", out_rel, parent)

    def is_reversible(self):
        return True


class Index(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, idx_col_name: str):
        super(Index, self).__init__("index", out_rel, parent)
        self.idx_col_name = idx_col_name


class Shuffle(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode):
        super(Shuffle, self).__init__("shuffle", out_rel, parent)

    def is_reversible(self):
        return True


class Open(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: [OpNode, None]):
        """ Initialize Open object. """
        super(Open, self).__init__("open", out_rel, parent)
        self.is_mpc = True
        self.is_boundary = True

    def is_reversible(self):
        return True


class Close(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: [OpNode, None]):
        super(Close, self).__init__("close", out_rel, parent)
        self.is_mpc = True
        self.is_boundary = True

    def is_reversible(self):
        return True
