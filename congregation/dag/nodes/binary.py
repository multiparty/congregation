from congregation.dag.nodes import OpNode
from congregation.dag.nodes.unary import Open, Close
from congregation.datasets import Relation
from congregation.datasets import Column


class BinaryOpNode(OpNode):
    def __init__(self, name: str, out_rel: Relation, left_parent: OpNode, right_parent: OpNode):
        super(BinaryOpNode, self).__init__(name, out_rel)
        self.left_parent = left_parent
        self.right_parent = right_parent
        self.parents.add(left_parent)
        self.parents.add(right_parent)

    def get_left_in_rel(self):
        return self.left_parent.out_rel

    def get_right_in_rel(self):
        return self.right_parent.out_rel

    def requires_mpc(self):

        left_stored_with = self.get_left_in_rel().stored_with
        right_stored_with = self.get_right_in_rel().stored_with
        combined = left_stored_with.union(right_stored_with)
        return (len(combined) > 1) and not self.is_local

    def make_orphan(self):

        super(BinaryOpNode, self).make_orphan()
        self.left_parent = None
        self.right_parent = None

    def replace_parent(self, old_parent: OpNode, new_parent: OpNode):

        super(BinaryOpNode, self).replace_parent(old_parent, new_parent)
        if old_parent == self.left_parent:
            self.left_parent = new_parent
        elif old_parent == self.right_parent:
            self.right_parent = new_parent
        else:
            print(f"ERROR: Parent node {old_parent.name} not recognized in current parent set.")

    def remove_parent(self, parent: OpNode):

        super(BinaryOpNode, self).remove_parent(parent)
        if parent == self.left_parent:
            self.left_parent = None
        elif parent == self.right_parent:
            self.right_parent = None
        else:
            print(f"ERROR: Parent node {parent.__str__} not recognized in current parent set.")

    def is_upper_boundary(self):
        return self.is_mpc and not any([par.is_mpc and not isinstance(par, Close) for par in self.parents])

    def is_lower_boundary(self):
        return self.is_mpc and not any([child.is_mpc and not isinstance(child, Open) for child in self.children])


class Join(BinaryOpNode):
    def __init__(self, out_rel: Relation, left_parent: OpNode, right_parent: OpNode,
                 left_join_cols: list, right_join_cols: list):
        super(Join, self).__init__("join", out_rel, left_parent, right_parent)
        self.left_join_cols = left_join_cols
        self.right_join_cols = right_join_cols

    def verify_join_cols(self):

        if len(self.left_join_cols) != len(self.right_join_cols):
            raise Exception("Unequal number of join columns passed to Join operator.")

        for group in [self.left_join_cols, self.right_join_cols]:
            for col in group:
                if not isinstance(col, Column):
                    raise Exception(f"Value {col} passed to Join operator is not Column type.")

    def update_op_specific_cols(self):

        self.left_join_cols = [self.get_left_in_rel().columns[left_join_col.idx]
                               for left_join_col in self.left_join_cols]
        self.right_join_cols = [self.get_right_in_rel().columns[right_join_col.idx]
                                for right_join_col in self.right_join_cols]


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