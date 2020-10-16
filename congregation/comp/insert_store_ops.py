import copy
from congregation.comp.rewriter import DagRewriter
from congregation.comp.utils import *


class InsertStoreOps(DagRewriter):
    """
    Insert Store nodes before Close nodes and after Open nodes
    """
    def __init__(self):
        super(InsertStoreOps, self).__init__()

    def _rewrite_close(self, node: Close):

        if not isinstance(node.parent, Store):
            in_rel = copy.deepcopy(node.get_in_rel())
            op = Store(in_rel, None)
            insert_between(node.parent, node, op)
