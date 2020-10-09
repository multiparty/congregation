import copy
from congregation.dag.nodes import *
from congregation.dag.nodes.internal import *
from congregation.comp.rewriter import DagRewriter
from congregation.comp.utils import *


class InsertCloseOps(DagRewriter):
    def __init__(self):
        super(InsertCloseOps, self).__init__()

    @staticmethod
    def _rewrite_default(node: [Concat, Join]):

        if node.is_upper_boundary():

            out_stored_with = node.out_rel.stored_with
            parents = node.get_sorted_parents()
            for parent in parents:
                par_stored_with = parent.out_rel.stored_with
                if out_stored_with != par_stored_with:

                    out_rel = copy.deepcopy(parent.out_rel)
                    out_rel.rename(f"{out_rel.name}_close")
                    out_rel.stored_with = copy.copy(out_stored_with)
                    op = Close(out_rel, None)
                    insert_between(parent, node, op)

    def _rewrite_concat(self, node: Concat):
        self._rewrite_default(node)

    def _rewrite_join(self, node: Join):
        self._rewrite_default(node)



