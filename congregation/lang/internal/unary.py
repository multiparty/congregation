import copy
from congregation.datasets import Relation, Column
from congregation.utils.col import *
from congregation.dag.nodes import *


def _persist(input_op_node: OpNode, name: str):

    out_rel = copy.deepcopy(input_op_node.out_rel)
    out_rel.rename(name)
    op = Persist(out_rel, input_op_node)
    input_op_node.children.add(op)

    return op


def _open(input_op_node: OpNode, name: str, target_party: int):

    out_rel = copy.deepcopy(input_op_node.out_rel)
    out_rel.stored_with = {target_party}
    out_rel.rename(name)
    op = Open(out_rel, input_op_node)
    input_op_node.children.add(op)

    return op


def _close(input_op_node: OpNode, name: str, target_parties: set):

    out_rel = copy.deepcopy(input_op_node.out_rel)
    out_rel.stored_with = target_parties
    out_rel.rename(name)
    op = Close(out_rel, input_op_node)
    input_op_node.children.add(op)

    return op


def _comp_neighs(input_op_node: OpNode, name: str, comp_col_name: str):

    in_rel = input_op_node.out_rel
    out_rel_cols = copy.deepcopy(in_rel.columns)

    comp_col = find(in_rel.columns, comp_col_name)
    if comp_col is None:
        raise Exception(f"Comparator column {comp_col_name} not found in relation {in_rel.name}.")

    out_rel = Relation(name, [copy.deepcopy(comp_col)], copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = CompareNeighbors(out_rel, input_op_node, comp_col)
    input_op_node.children.add(op)

    return op


def index(input_op_node: OpNode, name: str, idx_col_name: str = "index"):

    in_rel = input_op_node.out_rel
    min_trust_set = min_trust_with_from_columns(in_rel.columns)
    index_col = Column(name, idx_col_name, len(in_rel.columns), "INTEGER", set())
    out_rel_cols = [index_col] + copy.deepcopy(in_rel.columns)

    out_rel = Relation(name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = Index(out_rel, input_op_node, idx_col_name)
    input_op_node.children.add(op)

    return op


def num_rows(input_op_node: OpNode, name: str, count_col_name: str = "num_rows"):

    in_rel = input_op_node.out_rel
    count_col = Column(name, count_col_name, len(in_rel.columns), "INTEGER", set())
    out_rel_cols = [count_col]

    out_rel = Relation(name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = NumRows(out_rel, input_op_node, count_col_name)
    input_op_node.children.add(op)

    return op


def shuffle(input_op_node: OpNode, name: str):

    in_rel = input_op_node.out_rel
    out_rel_cols = copy.deepcopy(in_rel.columns)

    out_rel = Relation(name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = Shuffle(out_rel, input_op_node)
    input_op_node.children.add(op)

    return op
