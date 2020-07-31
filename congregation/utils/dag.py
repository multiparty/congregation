import copy
from congregation.dag.nodes import OpNode
from congregation.dag.nodes import UnaryOpNode


def remove_between(parent: OpNode, child: OpNode, to_remove: OpNode):
    """
    TODO: extend to general case (currently limited to UnaryOpNode)
    """

    if len(to_remove.children) < 2 or len(to_remove.parents) != 1:
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

    if child:
        _update_child_on_insert(parent, child, to_insert)