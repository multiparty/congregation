import copy
from congregation.dag.nodes.node import OpNode
from congregation.datasets import Relation
from congregation.datasets import Column
from congregation.utils import *


class UnaryOpNode(OpNode):
    def __init__(self, name: str, out_rel: Relation, parent: [OpNode, None]):
        super(UnaryOpNode, self).__init__(name, out_rel)
        self.parent = parent
        if parent:
            self.parents.add(parent)

    def get_in_rel(self):
        return self.parent.out_rel

    def update_stored_with(self):
        self.out_rel.stored_with = copy.copy(self.get_in_rel().stored_with)

    def make_orphan(self):

        super(UnaryOpNode, self).make_orphan()
        self.parent = None

    def replace_parent(self, old_parent: OpNode, new_parent: OpNode):
        """ Replace this node's parent with another node. """

        super(UnaryOpNode, self).replace_parent(old_parent, new_parent)
        self.parent = new_parent

    def remove_parent(self, parent):
        """ Remove this node's parent. """

        super(UnaryOpNode, self).remove_parent(parent)
        self.parent = None


class Create(UnaryOpNode):
    def __init__(self, out_rel: Relation):
        super(Create, self).__init__(f"create-{out_rel.name}", out_rel, None)

    def requires_mpc(self):
        """
        Stored with sets for Create nodes are either:
            1. Of length 1, ex. [{1}], which indicates that the data
            is held by a single party in plaintext.
            2. Of length > 1 ex. [{1, 2}], which indicates that the data
            is held as secret shares between parties 1 & 2
        -> A Create node can only require mpc in the second case

        TODO: Might want to think more about TW and PT sets here. It will
         influence where a Send node is inserted in cases where we're only
         operating over columns that can be revealed to a particular party.
        """

        min_sw = min_set(self.out_rel.stored_with)
        return not len(min_sw) == 1


class AggregateCount(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, group_cols: list, count_col: Column):
        super(AggregateCount, self).__init__("aggregate_count", out_rel, parent)
        self.group_cols = group_cols
        self.count_col = count_col

    def update_op_specific_cols(self):

        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        self.group_cols = [temp_cols[group_col.idx] for group_col in self.group_cols]
        self.count_col = self.update_count_col()

    def update_count_col(self):
        """
        Won't be part of input relation, need to generate fresh
        """

        min_trust = min_trust_with_from_columns(self.group_cols)
        min_pt = min_pt_set_from_cols(self.group_cols)
        return Column(
            self.get_in_rel().name, self.count_col.name, len(self.group_cols),
            self.count_col.type_str, min_trust, min_pt
        )

    def update_out_rel_cols(self):

        self.update_op_specific_cols()
        temp_cols = self.group_cols + [self.count_col]
        self.out_rel.columns = copy.deepcopy(temp_cols)
        self.out_rel.update_columns()


class AggregateSum(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, group_cols: list, agg_col: Column):
        super(AggregateSum, self).__init__("aggregate_sum", out_rel, parent)
        self.group_cols = group_cols
        self.agg_col = agg_col

    def update_op_specific_cols(self):

        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        self.group_cols = [temp_cols[group_col.idx] for group_col in self.group_cols]

        min_trust_set = min_trust_with_from_columns(self.group_cols + [temp_cols[self.agg_col.idx]])
        min_pt = min_pt_set_from_cols(self.group_cols + [temp_cols[self.agg_col.idx]])
        self.agg_col = temp_cols[self.agg_col.idx]
        self.agg_col.trust_with = min_trust_set
        self.agg_col.plaintext = min_pt

    def update_out_rel_cols(self):

        self.update_op_specific_cols()
        temp_cols = self.group_cols + [self.agg_col]
        self.out_rel.columns = copy.deepcopy(temp_cols)
        self.out_rel.update_columns()

    @staticmethod
    def from_agg_count(node: AggregateCount):

        temp_node = copy.deepcopy(node)
        node = AggregateSum(
            temp_node.out_rel,
            temp_node.parent,
            temp_node.out_rel.columns[:-1],
            temp_node.out_rel.columns[-1]
        )
        node.out_rel.update_columns()

        return node


class AggregateMean(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, group_cols: list, agg_col: Column,
                 with_count_col: [bool, None] = False):
        super(AggregateMean, self).__init__("aggregate_mean", out_rel, parent)
        self.group_cols = group_cols
        self.agg_col = agg_col
        self.with_count_col = with_count_col

    def update_op_specific_cols(self):

        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        self.group_cols = [temp_cols[group_col.idx] for group_col in self.group_cols]

        min_trust_set = min_trust_with_from_columns(self.group_cols + [temp_cols[self.agg_col.idx]])
        min_pt = min_pt_set_from_cols(self.group_cols + [temp_cols[self.agg_col.idx]])
        self.agg_col = temp_cols[self.agg_col.idx]
        self.agg_col.trust_with = min_trust_set
        self.agg_col.plaintext = min_pt

    def update_out_rel_cols(self):

        self.update_op_specific_cols()
        temp_cols = self.group_cols + [self.agg_col]
        self.out_rel.columns = copy.deepcopy(temp_cols)
        self.out_rel.update_columns()


class AggregateStdDev(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, group_cols: list, agg_col: Column,
                 with_count_col: [bool, None] = False):
        super(AggregateStdDev, self).__init__("aggregate_std_dev", out_rel, parent)
        self.group_cols = group_cols
        self.agg_col = agg_col
        self.with_count_col = with_count_col

    def update_op_specific_cols(self):

        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        self.group_cols = [temp_cols[group_col.idx] for group_col in self.group_cols]

        min_trust_set = min_trust_with_from_columns(self.group_cols + [temp_cols[self.agg_col.idx]])
        min_pt = min_pt_set_from_cols(self.group_cols + [temp_cols[self.agg_col.idx]])
        self.agg_col = temp_cols[self.agg_col.idx]
        self.agg_col.trust_with = min_trust_set
        self.agg_col.plaintext = min_pt

    def update_out_rel_cols(self):

        self.update_op_specific_cols()
        temp_cols = self.group_cols + [self.agg_col]
        self.out_rel.columns = copy.deepcopy(temp_cols)
        self.out_rel.update_columns()


class Project(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, selected_cols: list):
        super(Project, self).__init__("project", out_rel, parent)
        self.selected_cols = selected_cols

    def is_reversible(self):
        return len(self.selected_cols) == len(self.get_in_rel().columns)

    def update_op_specific_cols(self):

        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        self.selected_cols = [temp_cols[c.idx] for c in self.selected_cols]

    def update_out_rel_cols(self):

        self.update_op_specific_cols()
        self.out_rel.columns = copy.deepcopy(self.selected_cols)
        self.out_rel.update_columns()


class Multiply(UnaryOpNode):

    def __init__(self, out_rel: Relation, parent: OpNode, target_col: Column, operands: list):
        super(Multiply, self).__init__("multiply", out_rel, parent)
        self.operands = operands
        self.target_col = target_col

    def is_reversible(self):
        return all([op != 0 for op in self.operands])

    def update_op_specific_cols(self):

        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        old_operands = copy.deepcopy(self.operands)
        self.operands = [temp_cols[o.idx] if isinstance(o, Column) else o for o in old_operands]

        if self.target_col.idx == len(temp_cols):
            temp_target_col = copy.deepcopy(self.target_col)
            all_cols = [o for o in self.operands if isinstance(o, Column)]
        else:
            temp_target_col = copy.deepcopy(temp_cols[self.target_col.idx])
            all_cols = [o for o in self.operands if isinstance(o, Column)] + [temp_target_col]

        target_col_trust_set = min_trust_with_from_columns(all_cols)
        target_col_pt_set = min_pt_set_from_cols(all_cols)
        temp_target_col.trust_with = target_col_trust_set
        temp_target_col.plaintext = target_col_pt_set

        self.target_col = temp_target_col

    def update_out_rel_cols(self):

        self.update_op_specific_cols()
        out_rel_cols = copy.deepcopy(self.get_in_rel().columns)
        temp_target_col = copy.deepcopy(self.target_col)

        if self.target_col.idx == len(out_rel_cols):
            out_rel_cols = out_rel_cols + [temp_target_col]
        else:
            out_rel_cols[self.target_col.idx] = temp_target_col

        self.out_rel.columns = out_rel_cols
        self.out_rel.update_columns()


class Divide(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, target_col: Column, operands: list):
        super(Divide, self).__init__("divide", out_rel, parent)
        self.operands = operands
        self.target_col = target_col

    def is_reversible(self):
        return True

    def update_op_specific_cols(self):

        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        old_operands = copy.deepcopy(self.operands)
        self.operands = [temp_cols[o.idx] if isinstance(o, Column) else o for o in old_operands]

        if self.target_col.idx == len(temp_cols):
            temp_target_col = copy.deepcopy(self.target_col)
            all_cols = [o for o in self.operands if isinstance(o, Column)]
        else:
            temp_target_col = copy.deepcopy(temp_cols[self.target_col.idx])
            all_cols = [o for o in self.operands if isinstance(o, Column)] + [temp_target_col]

        target_col_trust_set = min_trust_with_from_columns(all_cols)
        target_col_pt_set = min_pt_set_from_cols(all_cols)
        temp_target_col.trust_with = target_col_trust_set
        temp_target_col.plaintext = target_col_pt_set

        self.target_col = temp_target_col

    def update_out_rel_cols(self):

        self.update_op_specific_cols()
        out_rel_cols = copy.deepcopy(self.get_in_rel().columns)
        temp_target_col = copy.deepcopy(self.target_col)

        if self.target_col.idx == len(out_rel_cols):
            out_rel_cols = out_rel_cols + [temp_target_col]
        else:
            out_rel_cols[self.target_col.idx] = temp_target_col

        self.out_rel.columns = out_rel_cols
        self.out_rel.update_columns()


class Limit(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, num: int):
        super(Limit, self).__init__("limit", out_rel, parent)
        self.num = num


class Distinct(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, selected_cols: list):
        super(Distinct, self).__init__("distinct", out_rel, parent)
        self.selected_cols = selected_cols

    def update_op_specific_cols(self):

        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        old_cols = copy.copy(self.selected_cols)
        self.selected_cols = [temp_cols[c.idx] for c in old_cols]

    def update_out_rel_cols(self):

        self.update_op_specific_cols()
        out_rel_cols = copy.deepcopy(self.get_in_rel().columns)
        self.out_rel.columns = out_rel_cols
        self.out_rel.update_columns()


class FilterAgainstCol(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, filter_col: Column, operator: str, against_col: Column):
        super(FilterAgainstCol, self).__init__("filter_against_col", out_rel, parent)
        self.filter_col = filter_col
        self.against_col = against_col
        self.operator = self.verify_operator(operator)

    @staticmethod
    def verify_operator(op):

        if op not in [">", "<", "=="]:
            raise Exception("Filter operation only supports {<, >, ==} operators.")
        return op

    def update_op_specific_cols(self):

        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        self.filter_col = temp_cols[self.filter_col.idx]
        self.against_col = temp_cols[self.against_col.idx]

    def _update_out_rel_cols(self):

        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        min_trust_set = min_trust_with_from_columns([temp_cols[self.filter_col.idx], temp_cols[self.against_col.idx]])
        min_pt_set = min_pt_set_from_cols([temp_cols[self.filter_col.idx], temp_cols[self.against_col.idx]])
        temp_cols[self.filter_col.idx].trust_with = min_trust_set
        temp_cols[self.filter_col.idx].plaintext = min_pt_set
        temp_cols[self.against_col.idx].trust_with = min_trust_set
        temp_cols[self.against_col.idx].plaintext = min_pt_set

        return temp_cols

    def update_out_rel_cols(self):

        self.update_op_specific_cols()
        self.out_rel.columns = self._update_out_rel_cols()
        self.out_rel.update_columns()


class FilterAgainstScalar(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, filter_col: Column, operator: str, scalar: int):
        super(FilterAgainstScalar, self).__init__("filter_against_scalar", out_rel, parent)
        self.filter_col = filter_col
        self.scalar = scalar
        self.operator = operator

    @staticmethod
    def verify_operator(op):
        if op not in [">", "<", "=="]:
            raise Exception("Filter operation only supports {<, >, =} operators.")
        return op

    def update_op_specific_cols(self):

        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        self.filter_col = temp_cols[self.filter_col.idx]

    def update_out_rel_cols(self):

        self.update_op_specific_cols()
        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        self.out_rel.columns = temp_cols
        self.out_rel.update_columns()


class SortBy(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, sort_by_col: Column, increasing: [bool, None] = True):
        super(SortBy, self).__init__("sort_by", out_rel, parent)
        self.sort_by_col = sort_by_col
        self.increasing = increasing

    def is_reversible(self):
        return True

    def update_op_specific_cols(self):

        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        self.sort_by_col = temp_cols[self.sort_by_col.idx]

    def update_out_rel_cols(self):

        self.update_op_specific_cols()
        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        self.out_rel.columns = temp_cols
        self.out_rel.update_columns()


class NumRows(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, col_name: str):
        super(NumRows, self).__init__("num_rows", out_rel, parent)
        self.col_name = col_name

    def update_out_rel_cols(self):
        """
        Using max TW/PT sets here because if you either had
        or were trusted with a single column, you could have
        counted the number of rows it contained.
        """

        temp_cols = copy.deepcopy(self.get_in_rel().columns)
        max_trust_set = max_trust_with_from_columns(temp_cols)
        max_pt_set = max_pt_set_from_cols(temp_cols)

        out_col = Column(
            self.out_rel.name, self.col_name, 0,
            "INTEGER", max_trust_set, max_pt_set
        )

        self.out_rel.columns = [out_col]
        self.out_rel.update_columns()


class Collect(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode):
        super(Collect, self).__init__("collect", out_rel, parent)

    def is_reversible(self):
        return True

    def requires_mpc(self):
        return False
