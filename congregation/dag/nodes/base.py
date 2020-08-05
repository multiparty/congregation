from congregation.datasets import Relation


class Node:
    def __init__(self, name: str):
        self.name = name
        self.out_rel = None
        self.children = set()
        self.parents = set()

    def __str__(self):

        children_str = str([n.name for n in self.children])
        parent_str = str([n.name for n in self.parents])
        out_rel_str = str(self.out_rel if self.out_rel is not None else "N/A\n")
        return f"NODE NAME: {self.name}\n" \
               f"CHILDREN: {children_str}\n" \
               f"PARENTS: {parent_str}\n" \
               f"OUT RELATION:\n{out_rel_str}"

    def is_leaf(self):
        return len(self.children) == 0

    def is_root(self):
        return len(self.parents) == 0


class OpNode(Node):
    def __init__(self, name, out_rel: Relation):
        super(OpNode, self).__init__(name)
        self.out_rel = out_rel
        self.is_mpc = False
        self.is_boundary = False
        self.is_local = self.out_rel.is_local()

    def __str__(self):
        return f"{super(OpNode, self).__str__()}{'*MPC*' if self.is_mpc else ''}"

    def is_reversible(self):
        """
        Reversible in the sense that, given the output of the operation, we reconstruct it's inputs.

        An example of this property could be Multiplication, where if you have the output and knowledge
        that the second column was multiplied by 3, you could reconstruct the original column.

        An example of a non-reversible operation is Aggregation, where you cannot infer the original data
        given only the output, the aggregator, and the columns that were grouped over. At present, we
        consider whether an entire relation is reversible as opposed to column-level reversibility.
        OpNodes are not reversible by default.
        """
        return False

    def update_op_specific_cols(self):
        """ Overridden in subclasses. """
        pass

    def update_out_rel_cols(self):
        """ Overridden in subclasses. """
        pass

    def requires_mpc(self):
        """Overridden in subclasses."""
        pass

    def update_stored_with(self):
        """ Overridden in subclasses. """
        pass

    def make_orphan(self):
        """ Remove link between this node and it's parent nodes. """
        self.parents = set()

    def remove_parent(self, parent: Node):
        """ Remove link between this node and a specific parent node. """
        self.parents.remove(parent)

    def replace_parent(self, old_parent: Node, new_parent: Node):
        """ Replace a specific parent of this node with another parent node. """

        self.parents.remove(old_parent)
        self.parents.add(new_parent)

    def replace_child(self, old_child: Node, new_child: Node):
        """ Replace a specific child of this node with another child node. """

        self.children.remove(old_child)
        self.children.add(new_child)

    def get_sorted_children(self):
        """ Return a list of this node's child nodes in alphabetical order. """
        return sorted(list(self.children), key=lambda x: x.out_rel.name)

    def get_sorted_parents(self):
        """ Return a list of this node's parent nodes in alphabetical order. """
        return sorted(list(self.parents), key=lambda x: x.out_rel.name)

    def is_upper_boundary(self):
        return self.is_mpc and not any([par.is_mpc and not par.is_boundary for par in self.parents])

    def is_lower_boundary(self):
        return self.is_mpc and not any([child.is_mpc and not child.is_boundary for child in self.children])
