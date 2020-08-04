import copy
from congregation.datasets import Relation, Column
from congregation.dag.nodes.binary import *
from congregation.utils.col import *
from congregation.utils.rel import *


def cols_from_rel(output_name, start_idx: int, rel: Relation, key_col_idxs: list):
    """
    TODO: Might need to rethink how I propagate trust_with sets here. Main concern
     is whether some out_rel (non-key) column c should inherit it's parent trust_with
     set. The output column will be made up of all the data in the original column, but
     with removals & duplications in accordance with the output of the join. Will need
     to think more on what this (on it's own) leaks before determining how to propagate
     trust sets. For now, it will just pass normal inheritance.
    """

    ret_cols = []
    for i, col in enumerate(rel.columns):
        if col.idx not in set(key_col_idxs):

            indx = i + start_idx - len(key_col_idxs)
            new_col = \
                Column(output_name, col.name, indx, col.type_str, copy.copy(col.trust_with), copy.copy(col.plaintext))
            ret_cols.append(new_col)

    return ret_cols


def join(left_input_node: OpNode, right_input_node: OpNode, name: str,
         left_col_names: list, right_col_names: list):

    if len(left_col_names) != len(right_col_names):
        raise Exception(
            f"Unequal number of left and right join cols passed to join():"
            f"\nLeft cols: {len(left_col_names)}"
            f"\nRight cols: {len(right_col_names)}"
        )

    left_in_rel = left_input_node.out_rel
    right_in_rel = right_input_node.out_rel

    left_join_cols = [find(left_in_rel.columns, col_name) for col_name in left_col_names]
    check_cols_for_missing_entries(left_join_cols, left_in_rel.name)
    right_join_cols = [find(right_in_rel.columns, col_name) for col_name in right_col_names]
    check_cols_for_missing_entries(right_join_cols, right_in_rel.name)

    out_key_cols = []
    for i in range(len(left_join_cols)):
        col_from_left = copy.copy(left_join_cols[i])
        col_from_right = copy.copy(right_join_cols[i])

        min_trust_set = col_from_left.trust_with.intersection(col_from_right.trust_with)
        min_plaintext_set = col_from_left.plaintext.intersection(col_from_right.plaintext)

        if left_join_cols[i].type_str != right_join_cols[i].type_str:
            raise Exception(
                f"Can't do join between columns of different type:\n"
                f"LEFT COL: {left_join_cols[i].type_str}\n"
                f"RIGHT COL: {right_join_cols[i].type_str}\n")

        out_key_cols.append(
            Column(name, left_join_cols[i].name, i, copy.copy(left_join_cols[i].type_str),
                   min_trust_set, min_plaintext_set)
        )

    start_idx = len(out_key_cols)
    continue_idx = len(left_in_rel.columns)
    left_non_key_cols = cols_from_rel(name, start_idx, left_in_rel, [lcol.idx for lcol in left_join_cols])
    right_non_key_cols = cols_from_rel(name, continue_idx, right_in_rel, [rcol.idx for rcol in right_join_cols])

    out_rel_cols = out_key_cols + left_non_key_cols + right_non_key_cols
    out_stored_with = stored_with_from_rels([left_in_rel, right_in_rel])
    out_rel = Relation(name, out_rel_cols, out_stored_with)
    out_rel.update_columns()

    op = Join(out_rel, left_input_node, right_input_node, left_join_cols, right_join_cols)
    left_input_node.children.add(op)
    right_input_node.children.add(op)

    return op



