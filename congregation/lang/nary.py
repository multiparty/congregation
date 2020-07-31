import copy
from congregation.datasets import *
from congregation.dag.nodes import *
from congregation.utils.col import *
from congregation.utils.rel import *


def _check_input_stored_with_for_concat(in_rels):
    """
    Placeholder check until this issue gets resolved.

    There might be issues when operating over pre-shared datasets owned
    by different groups of compute parties.
    EX:
    rel1 = ["a", "b", "c"]      # stored_with = {1,2,3}
    rel2 = ["d", "e", "f"]      # stored_with = {4,5,6}

    rel3 = concat(rel1, rel2)   # stored_with = {1,2,3,4,5,6}

    then we have 6 compute parties in one secret shared relation (rel3)
    parties 1,2,3 have shares for the first half of the rows in rel3,
    and parties 4,5,6 have shares for the second half of the rows in rel3.

    the codegen would have to support this specific case and others like it.

    so for now, we can only support the case where either every in_rel has
    a stored_with set of size 1, or all in_rels have the same stored_with set.
    """

    stored_with_sets = []
    for in_rel in in_rels:
        stored_with_sets.append(copy.copy(in_rel.stored_with))

    if all([len(s) == 1 for s in stored_with_sets]):
        return True
    elif len(set().union(*stored_with_sets)) == len(stored_with_sets[0]):
        return True
    else:
        return False


def _concat_check_for_errors(input_op_nodes: list, column_names: list):

    if len(input_op_nodes) < 2:
        raise Exception("Must pass at least 2 nodes to concat() function.")

    in_rels = [in_node.out_rel for in_node in input_op_nodes]
    if not _check_input_stored_with_for_concat(in_rels):
        raise Exception(
            "Input relations are shared between different parties. "
            "Congregation does not currently support these kinds of workflows."
        )

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

    in_stored_with = [in_rel.stored_with for in_rel in in_rels]
    out_stored_with = set().union(*in_stored_with)

    out_rel = Relation(name, out_rel_cols, out_stored_with)
    out_rel.update_columns()

    op = Concat(out_rel, input_op_nodes)
    for input_op_node in input_op_nodes:
        input_op_node.children.add(op)

    return op
