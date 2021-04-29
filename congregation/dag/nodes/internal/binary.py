from congregation.dag.nodes import BinaryOpNode
from congregation.dag.nodes.node import OpNode
from congregation.dag.nodes.unary import AggregateMean
from congregation.datasets import Relation
from congregation.datasets import Column
from congregation.utils import *
import copy


class MemberFilter(BinaryOpNode):
    """
    Filter a relation for rows that are in a set
    of values which are specified in another relation
    """
    def __init__(
            self,
            out_rel: Relation,
            input_op_node: OpNode,
            by_op_node: OpNode,
            filter_col: Column,
            in_flag: bool
    ):
        super(MemberFilter, self).__init__("filter_by", out_rel, input_op_node, by_op_node)
        self.filter_col = filter_col
        # flag to filter by whether values in filter col
        # are *in* the set of values from by_op_node
        self.in_flag = in_flag
        self.verify_by_op(by_op_node)

    @staticmethod
    def verify_by_op(by_op):

        if len(by_op.out_rel.columns) != 1:
            raise Exception("ByOp node must have single column in it's output relation.")

    def update_op_specific_cols(self):

        temp_cols = self.get_left_in_rel().columns()
        self.filter_col = temp_cols[self.filter_col.idx]


class ColumnUnion(BinaryOpNode):
    def __init__(
            self,
            out_rel: Relation,
            left_parent: OpNode,
            right_parent: OpNode,
            left_col: Column,
            right_col: Column
    ):
        super(ColumnUnion, self).__init__("union", out_rel, left_parent, right_parent)
        self.left_col = left_col
        self.right_col = right_col

    def update_op_specific_cols(self):

        temp_cols = self.get_left_in_rel().columns
        self.left_col = temp_cols[self.left_col.idx]
        self.right_col = temp_cols[self.right_col.idx]


class OptimizedStdDev(BinaryOpNode):
    def __init__(
            self,
            out_rel: Relation,
            left_parent: AggregateMean,
            right_parent: OpNode,
            group_cols: [list, None],
            agg_col: Column
    ):
        super(OptimizedStdDev, self).__init__("optimized_std_dev", out_rel, left_parent, right_parent)
        self.group_cols = group_cols if group_cols else []
        self.agg_col = agg_col
        self.agg_mean_node = left_parent
        self.original_records_node = right_parent

    def update_op_specific_cols(self):

        # both in_rels have identical schema for this op
        temp_cols = copy.deepcopy(self.get_in_rels()[0].columns)
        self.group_cols = [temp_cols[group_col.idx] for group_col in self.group_cols]

        min_trust_set = min_trust_with_from_cols(self.group_cols + [temp_cols[self.agg_col.idx]])
        min_pt = min_pt_set_from_cols(self.group_cols + [temp_cols[self.agg_col.idx]])
        self.agg_col = temp_cols[self.agg_col.idx]
        self.agg_col.trust_with = min_trust_set
        self.agg_col.plaintext = min_pt

    def update_out_rel_cols(self):

        self.update_op_specific_cols()
        temp_cols = self.group_cols + [self.agg_col]
        self.out_rel.columns = copy.deepcopy(temp_cols)
        self.out_rel.update_columns()
