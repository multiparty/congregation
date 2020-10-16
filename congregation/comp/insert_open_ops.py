import copy
from congregation.dag.nodes import *
from congregation.dag.nodes.internal import *
from congregation.comp.rewriter import DagRewriter
from congregation.comp.utils import *


class InsertOpenOps(DagRewriter):
    def __init__(self):
        super(InsertOpenOps, self).__init__()

    @staticmethod
    def _rewrite_default(node: [UnaryOpNode, NaryOpNode]):

        if node.is_lower_boundary():
            out_rel = copy.deepcopy(node.out_rel)
            out_stored_with = out_rel.stored_with
            flat_sw = [{s} for c in out_stored_with for s in c]
            sw_to_set = set().union(*flat_sw)
            out_rel.stored_with = flat_sw
            out_rel.assign_new_plaintext(copy.copy(sw_to_set))
            out_rel.assign_new_trust(copy.copy(sw_to_set))
            out_rel.rename(f"{out_rel.name}_open")
            op = Open(out_rel, None)
            insert_between_children(node, op)

    def _rewrite_aggregate_sum(self, node: AggregateSum):
        self._rewrite_default(node)

    def _rewrite_aggregate_count(self, node: AggregateCount):
        self._rewrite_default(node)

    def _rewrite_aggregate_mean(self, node: AggregateMean):
        self._rewrite_default(node)

    def _rewrite_aggregate_std_dev(self, node: AggregateStdDev):
        self._rewrite_default(node)

    def _rewrite_project(self, node: Project):
        self._rewrite_default(node)

    def _rewrite_multiply(self, node: Multiply):
        self._rewrite_default(node)

    def _rewrite_divide(self, node: Divide):
        self._rewrite_default(node)

    def _rewrite_limit(self, node: Limit):
        self._rewrite_default(node)

    def _rewrite_distinct(self, node: Distinct):
        self._rewrite_default(node)

    def _rewrite_filter_against_col(self, node: FilterAgainstCol):
        self._rewrite_default(node)

    def _rewrite_filter_against_scalar(self, node: FilterAgainstScalar):
        self._rewrite_default(node)

    def _rewrite_sort_by(self, node: SortBy):
        self._rewrite_default(node)

    def _rewrite_num_rows(self, node: NumRows):
        self._rewrite_default(node)

    def _rewrite_join(self, node: Join):
        self._rewrite_default(node)

    def _rewrite_concat(self, node: Concat):
        self._rewrite_default(node)

    def _rewrite_aggregate_sum_count_col(self, node: AggregateSumCountCol):
        self._rewrite_default(node)

    def _rewrite_col_sum(self, node: ColSum):
        self._rewrite_default(node)
