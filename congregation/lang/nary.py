import copy
from congregation.datasets import *
from congregation.dag.nodes import *
from congregation.utils.col import *
from congregation.utils.rel import *


def _concat_check_for_errors(input_op_nodes: list, column_names: list):

    if len(input_op_nodes) < 2:
        raise Exception("Must pass at least 2 nodes to concat() function.")

    in_rels = [in_node.out_rel for in_node in input_op_nodes]
    check_input_stored_with(in_rels)

    num_cols = len(in_rels[0].columns)
    if column_names is not None:
        if len(column_names) != num_cols:
            raise Exception("Number of column names for output relation does not match number of columns.")


def concat(input_op_nodes: list, name: str, column_names: [list, None] = None):

    _concat_check_for_errors(input_op_nodes, column_names)

    in_rels = [in_node.out_rel for in_node in input_op_nodes]
    all_trust_sets = resolve_trust_sets_from_rels(in_rels)
    all_plaintext_sets = resolve_plaintext_sets_from_rels(in_rels)

    out_rel_cols = copy.deepcopy(in_rels[0].columns)
    for (i, col) in enumerate(out_rel_cols):
        if column_names is not None:
            col.name = column_names[i]
        col.trust_with = all_trust_sets[i]
        col.plaintext = all_plaintext_sets[i]

    out_stored_with = []
    for in_rel in in_rels:
        out_stored_with = out_stored_with + in_rel.stored_with

    out_rel = Relation(name, out_rel_cols, out_stored_with)
    out_rel.update_columns()

    op = Concat(out_rel, input_op_nodes)
    for input_op_node in input_op_nodes:
        input_op_node.children.add(op)

    return op
