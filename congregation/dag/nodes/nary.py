import copy
from congregation.dag.nodes.base import OpNode
from congregation.datasets.relation import Relation
from congregation.utils import *


class NaryOpNode(OpNode):
    def __init__(self, name: str, out_rel: Relation, parents: set):
        super(NaryOpNode, self).__init__(name, out_rel)
        self.parents = parents


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

        all_in_rels = self.get_in_rels()
        in_cols_copy = copy.deepcopy(all_in_rels[0].columns)
        all_trust_sets = resolve_trust_sets_from_rels(all_in_rels)
        all_plaintext_sets = resolve_plaintext_sets_from_rels(all_in_rels)

        for (i, c) in enumerate(in_cols_copy):
            c.trust_with = all_trust_sets[i]
            c.plaintext = all_plaintext_sets[i]

        self.out_rel.columns = in_cols_copy
        self.out_rel.update_columns()
