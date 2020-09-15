import copy
from congregation.dag.nodes import OpNode
from congregation.dag.nodes import Concat
from congregation.dag.nodes import AggregateSum, AggregateCount, AggregateMean, AggregateStdDev
from congregation.dag.nodes import UnaryOpNode
from congregation.dag.nodes.internal import *


def remove_between(parent: OpNode, child: OpNode, to_remove: OpNode):
    """
    TODO: extend to general case (currently limited to UnaryOpNode)
    """

    if not len(to_remove.children) < 2 or len(to_remove.parents) != 1:
        raise Exception(
            f"Removed node should be Unary."
            f"\nNumber of children: {len(to_remove.children)} (should be < 2)."
            f"\nNumber of parents: {len(to_remove.parents)} (should be 1)."
        )

    if child:
        child.replace_parent(to_remove, parent)
        parent.replace_child(to_remove, child)
    else:
        parent.children.remove(to_remove)

    to_remove.make_orphan()
    to_remove.children = set()


def _update_child_on_insert(parent: OpNode, child: OpNode, to_insert: OpNode):
    child.replace_parent(parent, to_insert)

    if child in parent.children:
        parent.children.remove(child)

    child.update_op_specific_cols()
    to_insert.children.add(child)


def insert_between(parent: OpNode, child: OpNode, to_insert: OpNode):

    if to_insert.children or to_insert.parents:
        raise Exception(
            f"Inserted node should be orphan."
            f"\nNumber of children: {len(to_insert.children)} (should be 0)."
            f"\nNumber of parents: {len(to_insert.parents)} (should be 0)."
        )

    if not isinstance(to_insert, UnaryOpNode):
        raise Exception("Inserted node should be Unary.")

    to_insert.parents.add(parent)
    to_insert.parent = parent
    parent.children.add(to_insert)
    to_insert.update_op_specific_cols()
    to_insert.update_out_rel_cols()

    if child:
        _update_child_on_insert(parent, child, to_insert)


def insert_between_children(parent: OpNode, to_insert: OpNode):
    """
    TODO: extend to general case (currently limited to UnaryOpNode)
    """

    if to_insert.children or to_insert.parents:
        raise Exception(
            f"Inserted node should be orphan."
            f"\nNumber of children: {len(to_insert.children)} (should be 0)."
            f"\nNumber of parents: {len(to_insert.parents)} (should be 0)."
        )

    if not isinstance(to_insert, UnaryOpNode):
        raise Exception("Inserted node should be Unary.")

    to_insert.parent = parent
    to_insert.parents.add(parent)

    children = copy.copy(parent.children)
    for child in children:
        _update_child_on_insert(parent, child, to_insert)

    parent.children.add(to_insert)


def insert_clone(parent: OpNode, child: OpNode, to_insert: OpNode):

    if to_insert.children or to_insert.parents:
        raise Exception(
            f"Inserted node should be orphan."
            f"\nNumber of children: {len(to_insert.children)} (should be 0)."
            f"\nNumber of parents: {len(to_insert.parents)} (should be 0)."
        )

    if not isinstance(to_insert, UnaryOpNode):
        raise Exception("Inserted node should be Unary.")

    to_insert.parents.add(parent)
    to_insert.parent = parent
    parent.children.add(to_insert)

    if child:
        _update_child_on_insert(parent, child, to_insert)


def push_parent_op_node_down(top_node: OpNode, bottom_node: OpNode):

    if not len(bottom_node.children) <= 1:
        print("TODO: Push OpNode down for children > 1.")
        return

    child = next(iter(bottom_node.children), None)
    remove_between(top_node, child, bottom_node)
    top_node_parents = copy.copy(top_node.get_sorted_parents())

    for idx, top_node_parent in enumerate(top_node_parents):
        node_to_insert = copy.deepcopy(bottom_node)
        node_to_insert.out_rel.rename(f"{node_to_insert.out_rel.name}_{str(idx)}")
        node_to_insert.parents = set()
        node_to_insert.children = set()
        insert_between(top_node_parent, top_node, node_to_insert)
        node_to_insert.update_stored_with()
        node_to_insert.update_out_rel_cols()


def fork_node(node: Concat):
    """
    Concat nodes are often MPC boundaries. This method forks a Concat
    node that has more than one child node into a separate Concat node
    for each of it's children.
    """

    # skip first child
    child_iter = enumerate(copy.copy(node.get_sorted_children()))
    next(child_iter)

    for idx, child in child_iter:

        clone = copy.deepcopy(node)
        clone.out_rel.rename(f"{node.out_rel.name}_{str(idx)}")
        clone.parents = copy.copy(node.parents)
        clone.ordered = copy.copy(node.ordered)
        clone.children = {child}

        for parent in clone.parents:
            parent.children.add(clone)

        node.children.remove(child)
        # make cloned node the child's new parent
        child.replace_parent(node, clone)
        child.update_op_specific_cols()
