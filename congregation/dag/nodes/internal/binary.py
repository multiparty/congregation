from congregation.dag.nodes import BinaryOpNode
from congregation.dag.nodes.node import OpNode
from congregation.datasets import Relation
from congregation.datasets import Column


class MemberFilter(BinaryOpNode):
    """
    Filter a relation for rows that are in a set
    of values which are specified in another relation
    """
    def __init__(self, out_rel: Relation, input_op_node: OpNode, by_op_node: OpNode,
                 filter_col: Column, in_flag: bool):
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
    def __init__(self, out_rel: Relation, left_parent: OpNode, right_parent: OpNode,
                 left_col: Column, right_col: Column):
        super(ColumnUnion, self).__init__("union", out_rel, left_parent, right_parent)
        self.left_col = left_col
        self.right_col = right_col

    def update_op_specific_cols(self):

        temp_cols = self.get_left_in_rel().columns
        self.left_col = temp_cols[self.left_col.idx]
        self.right_col = temp_cols[self.right_col.idx]