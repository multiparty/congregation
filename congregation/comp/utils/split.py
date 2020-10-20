import copy
from congregation.dag.nodes import OpNode
from congregation.dag.nodes import Concat
from congregation.dag.nodes import *
from congregation.dag.nodes.internal import *
from congregation.comp.utils.dag import *


def split_default(node: [AggregateSum, Distinct]):

    if not len(node.children) <= 1:
        print("WARN: Can't split aggregate for children > 1.")
        return

    clone = copy.deepcopy(node)
    clone.out_rel.rename(f"{node.out_rel.name}_obl")
    clone.parents = set()
    clone.children = set()

    child = next(iter(node.children), None)
    insert_clone(node, child, clone)


def split_distinct(node: Distinct):
    """
    clone an AggregateSum or AggregateCount node
    """

    if not len(node.children) <= 1:
        print("WARN: Can't split aggregate for children > 1.")
        return

    clone = copy.deepcopy(node)
    clone.out_rel.rename(f"{node.out_rel.name}_obl")
    clone.parents = set()
    clone.children = set()

    child = next(iter(node.children), None)
    insert_clone(node, child, clone)


def split_agg_sum(node: AggregateSum):

    if not len(node.children) <= 1:
        print("WARN: Can't split aggregate for children > 1.")
        return

    clone = copy.deepcopy(node)
    clone.out_rel.rename(f"{node.out_rel.name}_obl")
    clone.parents = set()
    clone.children = set()

    child = next(iter(node.children), None)
    insert_clone(node, child, clone)


def split_agg_count(node: AggregateCount):

    if not len(node.children) <= 1:
        print("WARN: Can't split aggregate for children > 1.")
        return

    clone = AggregateSum.from_agg_count(node)
    clone.out_rel.rename(f"{node.out_rel.name}_obl")
    clone.parents = set()
    clone.children = set()

    child = next(iter(node.children), None)
    insert_between(node, child, clone)


def split_agg_mean(node: AggregateMean, parent: Concat):

    if not len(node.children) <= 1:
        print("WARN: Can't split aggregate for children > 1.")
        return

    node.with_count_col = True
    clone = AggregateSumCountCol.from_agg_mean(node)
    clone.parents = set()
    clone.children = set()

    insert_between(parent, node, clone)


def split_num_rows(node: NumRows):

    if not len(node.children) <= 1:
        print("WARN: Can't split num_rows for children > 1.")
        return

    clone = ColSum.from_num_rows(node)
    clone.out_rel.rename(f"{node.out_rel.name}_obl")
    clone.parents = set()
    clone.children = set()

    child = next(iter(node.children), None)
    insert_between(node, child, clone)
