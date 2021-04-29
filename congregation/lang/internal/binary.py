import copy
from congregation.datasets import Relation, Column
from congregation.utils.col import *
from congregation.dag.nodes.internal import *


def member_filter(input_op_node: OpNode, name: str, filter_col_name: str, by_op_node: OpNode, in_flag: bool = True):

    in_rel = input_op_node.out_rel
    out_rel_cols = copy.deepcopy(in_rel.columns)

    filter_col = find(in_rel.columns, filter_col_name)
    if filter_col is None:
        raise Exception(f"Column {filter_col_name} not found in relation {in_rel.name}.")

    out_stored_with = copy.copy(in_rel.stored_with) + copy.copy(by_op_node.out_rel.stored_with)
    out_rel = Relation(name, out_rel_cols, out_stored_with)
    out_rel.update_columns()

    op = MemberFilter(out_rel, input_op_node, by_op_node, filter_col, in_flag)
    input_op_node.children.add(op)
    by_op_node.children.add(op)

    return op


def column_union(left_input_node: OpNode, right_input_node: OpNode,
                 name: str, left_col_name: str, right_col_name: str):

    left_in_rel = left_input_node.out_rel
    right_in_rel = right_input_node.out_rel

    left_col = find(left_in_rel.columns, left_col_name)
    if left_col is None:
        raise Exception(f"Column {left_col_name} not found in relation {left_in_rel.name}.")

    right_col = find(right_input_node.out_rel.columns, right_col_name)
    if right_col is None:
        raise Exception(f"Column {right_col_name} not found in relation {right_in_rel.name}.")

    new_trust_set = min_trust_with_from_cols([left_col, right_col])
    pt = min_pt_set_from_cols([left_col, right_col])
    out_col = Column(name, left_col_name, 0, "INTEGER", new_trust_set, plaintext=pt)
    out_stored_with = copy.copy(left_in_rel.stored_with) + copy.copy(right_in_rel.stored_with)

    out_rel = Relation(name, [out_col], out_stored_with)
    out_rel.update_columns()

    op = ColumnUnion(out_rel, left_input_node, right_input_node, left_col, right_col)
    left_input_node.children.add(op)
    right_input_node.children.add(op)

    return op
