import copy
from congregation.dag.nodes.base import OpNode
from congregation.datasets import Relation
from congregation.datasets import Column


class UnaryOpNode(OpNode):
    def __init__(self, name: str, out_rel: Relation, parent: [OpNode, None]):
        super(UnaryOpNode, self).__init__(name, out_rel)
        self.parent = parent
        if parent:
            self.parents.add(parent)

    def get_in_rel(self):
        return self.parent.out_rel

    def requires_mpc(self):
        return self.get_in_rel().is_shared()

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
        return False


class AggregateSum(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, group_cols: list, agg_col: Column):
        super(AggregateSum, self).__init__("aggregate_sum", out_rel, parent)
        self.group_cols = group_cols
        self.agg_col = agg_col

    def update_op_specific_cols(self):

        self.group_cols = [self.get_in_rel().columns[group_col.idx] for group_col in self.group_cols]
        self.agg_col = self.get_in_rel().columns[self.agg_col.idx]


class AggregateCount(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, group_cols: list, with_count_col: [bool, None] = False):
        super(AggregateCount, self).__init__("aggregate_count", out_rel, parent)
        self.group_cols = group_cols
        self.with_count_col = with_count_col

    def update_op_specific_cols(self):
        self.group_cols = [self.get_in_rel().columns[group_col.idx] for group_col in self.group_cols]


class AggregateMean(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, group_cols: list, agg_col: Column,
                 with_count_col: [bool, None] = False):
        super(AggregateMean, self).__init__("aggregate_mean", out_rel, parent)
        self.group_cols = group_cols
        self.agg_col = agg_col
        self.with_count_col = with_count_col

    def update_op_specific_cols(self):

        self.group_cols = [self.get_in_rel().columns[group_col.idx] for group_col in self.group_cols]
        self.agg_col = self.get_in_rel().columns[self.agg_col.idx]


class AggregateStdDev(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, group_cols: list, agg_col: Column,
                 with_count_col: [bool, None] = False):
        super(AggregateStdDev, self).__init__("aggregate_std_dev", out_rel, parent)
        self.group_cols = group_cols
        self.agg_col = agg_col
        self.with_count_col = with_count_col

    def update_op_specific_cols(self):

        self.group_cols = [self.get_in_rel().columns[group_col.idx] for group_col in self.group_cols]
        self.agg_col = self.get_in_rel().columns[self.agg_col.idx]


class Project(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, selected_cols: list):
        super(Project, self).__init__("project", out_rel, parent)
        self.selected_cols = selected_cols

    def is_reversible(self):
        return len(self.selected_cols) == len(self.get_in_rel().columns)

    def update_op_specific_cols(self):

        temp_cols = self.get_in_rel().columns
        self.selected_cols = [temp_cols[col.idx] for col in self.selected_cols]


class Multiply(UnaryOpNode):

    def __init__(self, out_rel: Relation, parent: OpNode, target_col: Column, operands: list):
        super(Multiply, self).__init__("multiply", out_rel, parent)
        self.operands = operands
        self.target_col = target_col

    def is_reversible(self):
        return all([op != 0 for op in self.operands])

    def update_op_specific_cols(self):

        temp_cols = self.get_in_rel().columns
        old_operands = copy.copy(self.operands)
        self.operands = [temp_cols[col.idx] if isinstance(col, Column) else col for col in old_operands]


class Divide(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, target_col: Column, operands: list):
        super(Divide, self).__init__("divide", out_rel, parent)
        self.operands = operands
        self.target_col = target_col

    def is_reversible(self):
        return True

    def update_op_specific_cols(self):
        temp_cols = self.get_in_rel().columns
        old_operands = copy.copy(self.operands)
        self.operands = [temp_cols[col.idx] if isinstance(col, Column) else col for col in old_operands]


class Limit(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, num: int):
        super(Limit, self).__init__("limit", out_rel, parent)
        self.num = num


class Distinct(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, selected_cols: list):
        super(Distinct, self).__init__("distinct", out_rel, parent)
        self.selected_cols = selected_cols

    def update_op_specific_cols(self):

        temp_cols = self.get_in_rel().columns
        old_cols = copy.copy(self.selected_cols)
        self.selected_cols = [temp_cols[col.idx] for col in old_cols]


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

    def is_reversible(self):
        return False

    def update_op_specific_cols(self):

        temp_cols = self.get_in_rel().columns
        self.filter_col = temp_cols[self.filter_col.idx]
        self.against_col = temp_cols[self.against_col.idx]


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

    def is_reversible(self):
        return False

    def update_op_specific_cols(self):

        temp_cols = self.get_in_rel().columns
        self.filter_col = temp_cols[self.filter_col.idx]


class SortBy(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, sort_by_col: Column, increasing: [bool, None] = True):
        super(SortBy, self).__init__("sort_by", out_rel, parent)
        self.sort_by_col = sort_by_col
        self.increasing = increasing

    def update_op_specific_cols(self):
        self.sort_by_col = self.get_in_rel().columns[self.sort_by_col.idx]


class NumRows(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode, col_name: str):
        super(NumRows, self).__init__("num_rows", out_rel, parent)
        self.col_name = col_name


class Collect(UnaryOpNode):
    def __init__(self, out_rel: Relation, parent: OpNode):
        super(Collect, self).__init__("collect", out_rel, parent)

    def is_reversible(self):
        return True
