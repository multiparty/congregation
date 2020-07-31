import copy
from congregation.dag.nodes import OpNode
from congregation.dag.nodes.unary import Open, Close
from congregation.datasets.relation import Relation


class NaryOpNode(OpNode):
    def __init__(self, name: str, out_rel: Relation, parents: set):
        super(NaryOpNode, self).__init__(name, out_rel)
        self.parents = parents

    def get_in_rels(self):
        return set([parent.out_rel for parent in self.parents])

    def requires_mpc(self):

        in_stored_with_sets = [in_rel.stored_with for in_rel in self.get_in_rels()]
        is_shared = len(set().union(*in_stored_with_sets)) > 1
        return is_shared and not self.is_local

    def is_upper_boundary(self):
        return self.is_mpc and not any([par.is_mpc and not isinstance(par, Close) for par in self.parents])

    def is_lower_boundary(self):
        return self.is_mpc and not any([child.is_mpc and not isinstance(child, Open) for child in self.children])


class Concat(NaryOpNode):
    def __init__(self, out_rel: Relation, parents: list):
        self.check_parents(parents)
        super(Concat, self).__init__("concat", out_rel, set(parents))
        self.ordered = parents

    @staticmethod
    def check_parents(parents: list):

        if len(set(parents)) != len(parents):
            raise Exception("Parents list passed to Concat() node has duplicates.")

    def is_reversible(self):
        return True

    def get_in_rels(self):
        return [parent.out_rel for parent in self.ordered]

    def replace_parent(self, old_parent: OpNode, new_parent: OpNode):

        super(Concat, self).replace_parent(old_parent, new_parent)
        idx = self.ordered.index(old_parent)
        self.ordered[idx] = new_parent

    def remove_parent(self, parent: OpNode):

        super(Concat, self).remove_parent(parent)
        idx = self.ordered.index(parent)
        del self.ordered[idx]

    def update_out_rel_cols(self):

        in_rel_cols = copy.deepcopy(self.get_in_rels()[0].columns)
        self.out_rel.columns = in_rel_cols
        self.out_rel.update_columns()
