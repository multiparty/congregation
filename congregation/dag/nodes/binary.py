from congregation.dag.nodes.base import OpNode
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

    def get_in_rels(self):
        return [self.left_parent.out_rel, self.right_parent.out_rel]

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
