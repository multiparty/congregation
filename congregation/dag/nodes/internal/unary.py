from congregation.dag.nodes import UnaryOpNode
from congregation.dag.nodes.base import OpNode
from congregation.dag.nodes import AggregateMean
from congregation.datasets import Relation
from congregation.datasets import Column
from congregation.utils import *


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
        self.is_boundary = True

    def is_reversible(self):
        return True


class Close(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: [OpNode, None]):
        super(Close, self).__init__("close", out_rel, parent)
        self.is_boundary = True

    def is_reversible(self):
        return True


class AggregateSumCountCol(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, group_cols: list, agg_col: Column):
        super(AggregateSumCountCol, self).__init__("aggregate_sum_count_col", out_rel, parent)
        self.group_cols = group_cols
        self.agg_col = agg_col
        self.count_col = self.gen_count_col()

    def gen_count_col(self):
        return self.update_count_col()

    @staticmethod
    def from_agg_mean(node: AggregateMean):

        out_rel = copy.deepcopy(node.out_rel)
        parent = copy.deepcopy(node.parent)
        group_cols = copy.deepcopy(node.group_cols)
        agg_col = copy.deepcopy(node.agg_col)

        return AggregateSumCountCol(out_rel, parent, group_cols, agg_col)

    def update_count_col(self):

        min_trust = min_trust_with_from_columns(self.group_cols)
        min_pt = min_pt_set_from_cols(self.group_cols)

        return Column(
            self.get_in_rel().name, "__COUNT__", len(self.group_cols),
            "INTEGER", min_trust, min_pt
        )

    def update_op_specific_cols(self):

        self.group_cols = [self.get_in_rel().columns[group_col.idx] for group_col in self.group_cols]
        self.agg_col = self.get_in_rel().columns[self.agg_col.idx]
        self.count_col = self.update_count_col()

    def update_out_rel_cols(self):

        self.update_op_specific_cols()
        self.out_rel.columns = self.group_cols + [self.agg_col, self.count_col]
