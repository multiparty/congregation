import copy
from congregation.comp.rewriter import DagRewriter
from congregation.comp.utils import *


class InsertStoreOps(DagRewriter):
    """
    Insert Store nodes before Close nodes and after Open nodes
    """
    def __init__(self):
        super(InsertStoreOps, self).__init__()

    def _rewrite_open(self, node: Open):

        out_rel = copy.deepcopy(node.out_rel)
        out_stored_with = out_rel.stored_with
        flat_sw = [{s} for c in out_stored_with for s in c]
        sw_to_set = set().union(*flat_sw)
        out_rel.stored_with = flat_sw
        out_rel.assign_new_plaintext(copy.copy(sw_to_set))
        out_rel.assign_new_trust(copy.copy(sw_to_set))
        out_rel.rename(f"{out_rel.name}_store")
        op = Store(out_rel, None)
        insert_between_children(node, op)

    def _rewrite_close(self, node: Close):

        in_rel = copy.deepcopy(node.get_in_rel())
        in_stored_with = in_rel.stored_with
        flat_sw = [{s} for c in in_stored_with for s in c]
        sw_to_set = set().union(*flat_sw)
        in_rel.stored_with = flat_sw
        in_rel.assign_new_plaintext(copy.copy(sw_to_set))
        in_rel.assign_new_trust(copy.copy(sw_to_set))
        in_rel.rename(f"{in_rel.name}_store")
        op = Store(in_rel, None)
        insert_between(node.parent, node, op)
