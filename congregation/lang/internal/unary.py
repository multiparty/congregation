import copy
from congregation.datasets import Relation, Column
from congregation.utils.col import *
from congregation.dag.nodes.internal import *


def store(input_op_node: OpNode, name: str):

    out_rel = copy.deepcopy(input_op_node.out_rel)
    out_rel.rename(name)

    op = Store(out_rel, input_op_node)
    input_op_node.children.add(op)

    return op


def persist(input_op_node: OpNode, name: str):

    out_rel = copy.deepcopy(input_op_node.out_rel)
    out_rel.rename(name)

    op = Persist(out_rel, input_op_node)
    input_op_node.children.add(op)

    return op


def send(input_op_node: OpNode, name: str):

    out_rel = copy.deepcopy(input_op_node.out_rel)
    out_rel.rename(name)

    op = Send(out_rel, input_op_node)
    input_op_node.children.add(op)

    return op


def index(input_op_node: OpNode, name: str, idx_col_name: str = "index"):

    in_rel = input_op_node.out_rel
    trust_set_union = max_trust_with_from_columns(in_rel.columns)
    pt_set_union = max_trust_with_from_columns(in_rel.columns)
    index_col = Column(name, idx_col_name, len(in_rel.columns), "INTEGER", trust_set_union, pt_set_union)
    out_rel_cols = [index_col] + copy.deepcopy(in_rel.columns)

    out_rel = Relation(name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = Index(out_rel, input_op_node, idx_col_name)
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


def _open(input_op_node: OpNode, name: str, target_party: int):

    out_rel = copy.deepcopy(input_op_node.out_rel)
    out_rel.stored_with = [{target_party}]
    out_rel.rename(name)

    op = Open(out_rel, input_op_node)
    input_op_node.children.add(op)

    return op


def _close(input_op_node: OpNode, name: str, target_parties: list):

    out_rel = copy.deepcopy(input_op_node.out_rel)
    out_rel.stored_with = target_parties
    out_rel.rename(name)
    op = Close(out_rel, input_op_node)
    input_op_node.children.add(op)

    return op
