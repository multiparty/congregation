from congregation.dag.nodes.node import OpNode
from congregation.datasets import Relation
from congregation.datasets import Column
from congregation.utils import *
import copy


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
    def __init__(
            self,
            out_rel: Relation,
            left_parent: OpNode,
            right_parent: OpNode,
            left_join_cols: list,
            right_join_cols: list
    ):
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

    def update_out_key_cols(self):
        """
        Generate fresh output key columns with updated trust_with and plaintext sets.
        """

        ret = []
        for i in range(len(self.left_join_cols)):
            col_from_left = copy.copy(self.left_join_cols[i])
            col_from_right = copy.copy(self.right_join_cols[i])

            min_trust_set = col_from_left.trust_with.intersection(col_from_right.trust_with)
            min_plaintext_set = col_from_left.plaintext.intersection(col_from_right.plaintext)

            if self.left_join_cols[i].type_str != self.right_join_cols[i].type_str:
                raise Exception(
                    f"Can't do join between columns of different type:\n"
                    f"LEFT COL: {self.left_join_cols[i].type_str}\n"
                    f"RIGHT COL: {self.right_join_cols[i].type_str}\n")

            ret.append(
                Column(
                    copy.copy(self.out_rel.name),
                    copy.copy(self.left_join_cols[i].name),
                    i,
                    copy.copy(self.left_join_cols[i].type_str),
                    min_trust_set,
                    min_plaintext_set
                )
            )
        return ret

    def update_out_rel_cols(self):

        self.update_op_specific_cols()
        out_key_cols = self.update_out_key_cols()

        left_in_rel_cols = copy.deepcopy(self.get_left_in_rel().columns)
        right_in_rel_cols = copy.deepcopy(self.get_right_in_rel().columns)

        start_idx = len(out_key_cols)
        continue_idx = len(left_in_rel_cols)

        left_non_key_data = \
            non_key_cols_from_rel(
                self.out_rel.name,
                start_idx,
                left_in_rel_cols,
                [c.idx for c in self.left_join_cols]
            )
        left_non_key_cols = [Column(*d) for d in left_non_key_data]

        right_non_key_data = \
            non_key_cols_from_rel(
                self.out_rel.name,
                continue_idx,
                right_in_rel_cols,
                [c.idx for c in self.right_join_cols]
            )
        right_non_key_cols = [Column(*d) for d in right_non_key_data]

        self.out_rel.columns = out_key_cols + left_non_key_cols + right_non_key_cols
        self.out_rel.update_columns()
