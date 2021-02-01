from congregation.dag.nodes import UnaryOpNode
from congregation.dag.nodes.node import OpNode
from congregation.dag.nodes import *
from congregation.datasets import Relation
from congregation.datasets import Column
from congregation.utils import *


class Store(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: [OpNode, None]):
        super(Store, self).__init__("store", out_rel, parent)

    def is_reversible(self):
        return True

    def update_out_rel_cols(self):
        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        self.out_rel.columns = temp_cols
        self.out_rel.update_columns()


class Read(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: [OpNode, None]):
        super(Read, self).__init__("read", out_rel, parent)

    def is_reversible(self):
        return True

    def requires_mpc(self):
        return False


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

    def requires_mpc(self):
        return False


class Index(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, idx_col_name: str):
        super(Index, self).__init__("index", out_rel, parent)
        self.idx_col_name = idx_col_name

    def is_reversible(self):
        return True


class Shuffle(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode):
        super(Shuffle, self).__init__("shuffle", out_rel, parent)

    def is_reversible(self):
        return True


class Open(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: [OpNode, None]):
        """ Initialize Open object. """
        super(Open, self).__init__("open", out_rel, parent)

    def is_reversible(self):
        return True

    def requires_mpc(self):
        return True


class Close(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: [OpNode, None], holding_party: list):
        super(Close, self).__init__("close", out_rel, parent)
        # parties who hold this data in plaintext
        self.holding_party = self._resolve_holding_party(holding_party)

    @staticmethod
    def _resolve_holding_party(holding_party):
        if len(holding_party) > 1 or len(holding_party[0]) > 1:
            raise Exception(f"Holding party for Close() node should be singular: {holding_party}")
        return holding_party[0].pop()

    def is_reversible(self):
        return True

    def requires_mpc(self):
        return True


class AggregateSumCountCol(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, group_cols: [list, None], agg_col: Column):
        super(AggregateSumCountCol, self).__init__("aggregate_sum_count_col", out_rel, parent)
        self.group_cols = group_cols if group_cols else []
        self.agg_col = agg_col
        self.count_col = self.gen_count_col()

    def gen_count_col(self):
        return self.update_count_col()

    def update_count_col(self):

        if self.group_cols:
            min_trust = min_trust_with_from_columns(self.group_cols)
            min_pt = min_pt_set_from_cols(self.group_cols)
        else:
            # count col will just be the number of rows, which
            # all parties storing this data already know
            min_trust = max_set(self.out_rel.stored_with)
            min_pt = max_set(self.out_rel.stored_with)

        return Column(
            self.get_in_rel().name, "__COUNT__", len(self.group_cols) + 1,
            "INTEGER", min_trust, min_pt
        )

    @staticmethod
    def from_existing_agg(node: AggregateMean):

        out_rel = copy.deepcopy(node.out_rel)
        parent = copy.deepcopy(node.parent)
        group_cols = copy.deepcopy(node.group_cols)
        agg_col = copy.deepcopy(node.agg_col)

        out_node = AggregateSumCountCol(out_rel, parent, group_cols, agg_col)
        out_node.update_out_rel_cols()
        return out_node

    def update_op_specific_cols(self):

        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        self.group_cols = [temp_cols[group_col.idx] for group_col in self.group_cols]
        self.agg_col = temp_cols[self.agg_col.idx]
        self.count_col = self.update_count_col()

    def update_out_rel_cols(self):

        self.update_op_specific_cols()
        self.out_rel.columns = \
            copy.deepcopy(self.group_cols) + \
            [copy.deepcopy(self.agg_col), copy.deepcopy(self.count_col)]
        self.out_rel.update_columns()


class AggregateSumSquaresAndCount(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, group_cols: [list, None], agg_col: Column):
        super(AggregateSumSquaresAndCount, self).__init__("aggregate_sum_squares_and_count", out_rel, parent)
        self.group_cols = group_cols if group_cols else []
        self.agg_col = agg_col
        self.squares_col = self.gen_squares_col()
        self.count_col = self.gen_count_col()

    def gen_squares_col(self):
        return self.update_squares_col()

    def update_squares_col(self):

        trust_set = copy.deepcopy(self.agg_col.trust_with)
        pt_set = copy.deepcopy(self.agg_col.plaintext)
        typ = copy.copy(self.agg_col.type_str)

        return Column(
            self.get_in_rel().name,
            "__SQUARES__",
            len(self.group_cols) + 1,
            typ,
            trust_set,
            pt_set
        )

    def gen_count_col(self):
        return self.update_count_col()

    def update_count_col(self):

        if self.group_cols:
            min_trust = min_trust_with_from_columns(self.group_cols)
            min_pt = min_pt_set_from_cols(self.group_cols)
        else:
            # count col will just be the number of rows, which
            # all parties storing this data already know
            min_trust = max_set(self.out_rel.stored_with)
            min_pt = max_set(self.out_rel.stored_with)

        return Column(
            self.get_in_rel().name,
            "__COUNT__",
            len(self.group_cols) + 2,
            "INTEGER",
            min_trust,
            min_pt
        )

    @staticmethod
    def from_existing_agg(node: [AggregateStdDev, AggregateVariance]):

        out_rel = copy.deepcopy(node.out_rel)
        out_rel.rename(f"{copy.copy(node.out_rel.name)}_local_squares_and_count")
        parent = copy.deepcopy(node.parent)
        group_cols = copy.deepcopy(node.group_cols)
        agg_col = copy.deepcopy(node.agg_col)

        out_node = AggregateSumSquaresAndCount(out_rel, parent, group_cols, agg_col)
        out_node.update_out_rel_cols()
        return out_node

    def update_op_specific_cols(self):

        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        self.group_cols = [temp_cols[group_col.idx] for group_col in self.group_cols]
        self.agg_col = temp_cols[self.agg_col.idx]
        self.squares_col = self.update_squares_col()
        self.count_col = self.update_count_col()

    def update_out_rel_cols(self):

        self.update_op_specific_cols()
        self.out_rel.columns = \
            copy.deepcopy(self.group_cols) + \
            [
                copy.deepcopy(self.agg_col),
                copy.deepcopy(self.squares_col),
                copy.deepcopy(self.count_col)
            ]
        self.out_rel.update_columns()


class AggregateStdDevLocalSqrt(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode):
        super(AggregateStdDevLocalSqrt, self).__init__("aggregate_std_dev_local_sqrt", out_rel, parent)

    def requires_mpc(self):
        return False

    @staticmethod
    def from_existing_agg(node: [AggregateStdDev, AggregateVariance]):

        out_rel = copy.deepcopy(node.out_rel)
        out_rel.rename(f"{copy.copy(node.out_rel.name)}_local_sqrt")
        parent = copy.deepcopy(node.parent)
        out_node = AggregateStdDevLocalSqrt(out_rel, parent)

        return out_node

    def update_out_rel_cols(self):

        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        # just filter out column that gets generated by parent AggregateStdDev op
        self.out_rel.columns = [c for c in temp_cols if c.name != "__MEAN_SQUARES__"]
        self.out_rel.update_columns()


class AggregateVarianceLocalDiff(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode):
        super(AggregateVarianceLocalDiff, self).__init__("aggregate_variance_local_diff", out_rel, parent)

    def requires_mpc(self):
        return False

    @staticmethod
    def from_existing_agg(node: AggregateVariance):

        out_rel = copy.deepcopy(node.out_rel)
        out_rel.rename(f"{copy.copy(node.out_rel.name)}_local_diff")
        parent = copy.deepcopy(node.parent)
        out_node = AggregateVarianceLocalDiff(out_rel, parent)

        return out_node

    def update_out_rel_cols(self):

        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        # just filter out column that gets generated by parent AggregateVariance op
        self.out_rel.columns = [c for c in temp_cols if c.name != "__MEAN_SQUARES__"]
        self.out_rel.update_columns()


class ColSum(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode):
        super(ColSum, self).__init__("col_sum", out_rel, parent)

    def update_out_rel_cols(self):

        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        self.out_rel.columns = temp_cols
        self.out_rel.update_columns()

    @staticmethod
    def from_num_rows(node: NumRows):

        out_rel = copy.deepcopy(node.out_rel)
        out_rel.rename(f"{copy.copy(node.out_rel.name)}_local_sum")
        parent = copy.deepcopy(node.parent)
        return ColSum(out_rel, parent)
