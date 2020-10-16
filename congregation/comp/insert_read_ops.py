import copy
from congregation.dag.nodes import *
from congregation.dag.nodes.internal import *
from congregation.comp.rewriter import DagRewriter
from congregation.comp.utils import *


class InsertReadOps(DagRewriter):
    def __init__(self):
        super(InsertReadOps, self).__init__()

    def _rewrite_open(self, node):

        for idx, c in enumerate(node.children):
            if not isinstance(c, Read):
                out_rel = copy.deepcopy(node.out_rel)
                read_op = Read(out_rel, None)
                read_op.name = f"read-{idx}"
                insert_between(node, c, read_op)
