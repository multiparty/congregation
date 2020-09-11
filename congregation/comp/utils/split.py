import copy
from congregation.dag.nodes import OpNode
from congregation.dag.nodes import Concat
from congregation.dag.nodes import *
from congregation.dag.nodes.internal import *
from congregation.comp.utils.dag import *


def split_default(node: [AggregateSum, Distinct]):
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
    insert_between(node, child, clone)


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

    clone = AggregateSumCountCol.from_agg_mean(node)
    clone.parents = set()
    clone.children = set()

    insert_between(parent, node, clone)
