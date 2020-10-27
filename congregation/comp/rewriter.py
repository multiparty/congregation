from congregation.dag import Dag
from congregation.dag.nodes import *
from congregation.dag.nodes.internal import *


class DagRewriter:
    def __init__(self):
        self.reverse = False

    def rewrite(self, dag: Dag):

        ordered = dag.top_sort()
        if self.reverse:
            ordered = ordered[::-1]

        for node in ordered:
            if isinstance(node, Create):
                self._rewrite_create(node)
            elif isinstance(node, AggregateSum):
                self._rewrite_aggregate_sum(node)
            elif isinstance(node, AggregateCount):
                self._rewrite_aggregate_count(node)
            elif isinstance(node, AggregateMean):
                self._rewrite_aggregate_mean(node)
            elif isinstance(node, AggregateStdDev):
                self._rewrite_aggregate_std_dev(node)
            elif isinstance(node, Project):
                self._rewrite_project(node)
            elif isinstance(node, Multiply):
                self._rewrite_multiply(node)
            elif isinstance(node, Divide):
                self._rewrite_divide(node)
            elif isinstance(node, Limit):
                self._rewrite_limit(node)
            elif isinstance(node, Distinct):
                self._rewrite_distinct(node)
            elif isinstance(node, FilterAgainstCol):
                self._rewrite_filter_against_col(node)
            elif isinstance(node, FilterAgainstScalar):
                self._rewrite_filter_against_scalar(node)
            elif isinstance(node, SortBy):
                self._rewrite_sort_by(node)
            elif isinstance(node, NumRows):
                self._rewrite_num_rows(node)
            elif isinstance(node, Collect):
                self._rewrite_collect(node)
            elif isinstance(node, Join):
                self._rewrite_join(node)
            elif isinstance(node, Concat):
                self._rewrite_concat(node)
            elif isinstance(node, Open):
                self._rewrite_open(node)
            elif isinstance(node, Close):
                self._rewrite_close(node)
            elif isinstance(node, Store):
                self._rewrite_store(node)
            elif isinstance(node, Read):
                self._rewrite_read(node)
            elif isinstance(node, Persist):
                self._rewrite_persist(node)
            elif isinstance(node, Send):
                self._rewrite_send(node)
            elif isinstance(node, Index):
                self._rewrite_index(node)
            elif isinstance(node, Shuffle):
                self._rewrite_shuffle(node)
            elif isinstance(node, AggregateSumCountCol):
                self._rewrite_aggregate_sum_count_col(node)
            elif isinstance(node, AggregateSumSquaresAndCount):
                self._rewrite_aggregate_sum_squares_and_count(node)
            elif isinstance(node, AggregateStdDevLocalSqrt):
                self._rewrite_aggregate_std_dev_local_sqrt(node)
            elif isinstance(node, ColSum):
                self._rewrite_col_sum(node)
            elif isinstance(node, MemberFilter):
                self._rewrite_member_filter(node)
            elif isinstance(node, ColumnUnion):
                self._rewrite_column_union(node)
            else:
                raise Exception(f"Unknown class {type(node).__name__}.")

    def _rewrite_create(self, node: Create):
        pass

    def _rewrite_aggregate_sum(self, node: AggregateSum):
        pass

    def _rewrite_aggregate_count(self, node: AggregateCount):
        pass

    def _rewrite_aggregate_mean(self, node: AggregateMean):
        pass

    def _rewrite_aggregate_std_dev(self, node: AggregateStdDev):
        pass

    def _rewrite_project(self, node: Project):
        pass

    def _rewrite_multiply(self, node: Multiply):
        pass

    def _rewrite_divide(self, node: Divide):
        pass

    def _rewrite_limit(self, node: Limit):
        pass

    def _rewrite_distinct(self, node: Distinct):
        pass

    def _rewrite_filter_against_col(self, node: FilterAgainstCol):
        pass

    def _rewrite_filter_against_scalar(self, node: FilterAgainstScalar):
        pass

    def _rewrite_sort_by(self, node: SortBy):
        pass

    def _rewrite_num_rows(self, node: NumRows):
        pass

    def _rewrite_collect(self, node: Collect):
        pass

    def _rewrite_join(self, node: Join):
        pass

    def _rewrite_concat(self, node: Concat):
        pass

    def _rewrite_open(self, node: Open):
        pass

    def _rewrite_close(self, node: Close):
        pass

    def _rewrite_store(self, node: Store):
        pass

    def _rewrite_read(self, node: Read):
        pass

    def _rewrite_persist(self, node: Persist):
        pass

    def _rewrite_send(self, node: Send):
        pass

    def _rewrite_index(self, node: Index):
        pass

    def _rewrite_shuffle(self, node: Shuffle):
        pass

    def _rewrite_aggregate_sum_count_col(self, node: AggregateSumCountCol):
        pass

    def _rewrite_aggregate_sum_squares_and_count(self, node: AggregateSumSquaresAndCount):
        pass

    def _rewrite_aggregate_std_dev_local_sqrt(self, node: AggregateStdDevLocalSqrt):
        pass

    def _rewrite_col_sum(self, node: ColSum):
        pass

    def _rewrite_member_filter(self, node: MemberFilter):
        pass

    def _rewrite_column_union(self, node: ColumnUnion):
        pass
