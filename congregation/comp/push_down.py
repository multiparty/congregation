from congregation.dag.nodes import *
from congregation.dag.nodes.internal import *
from congregation.comp.base import DagRewriter
from congregation.dag.nodes import OpNode
from congregation.comp.utils import *


class PushDown(DagRewriter):
    def __init__(self):
        super(PushDown, self).__init__()

    @staticmethod
    def _rewrite_default(node: OpNode):
        node.is_mpc = node.requires_mpc()

    @staticmethod
    def _rewrite_unary_default(node: OpNode):
        """
        PushDown rewrite for Project, Multiply, and Divide op nodes.
        """

        if node.is_leaf():
            node.is_mpc = node.requires_mpc()
            return

        parent_node = next(iter(node.parents))
        if parent_node.requires_mpc():
            if isinstance(parent_node, Concat) and parent_node.is_upper_boundary():
                push_parent_op_node_down(parent_node, node)
                parent_node.update_out_rel_cols()
            else:
                node.is_mpc = True
        else:
            pass

    def _rewrite_aggregate_sum(self, node: AggregateSum):

        parent = next(iter(node.parents))
        if parent.requires_mpc():
            if isinstance(parent, Concat) and parent.is_upper_boundary():
                split_agg_simple(node)
                push_parent_op_node_down(parent, node)
                parent.update_out_rel_cols()
                # TODO: node parent is coming up as None
            else:
                node.is_mpc = True
        else:
            pass

    def _rewrite_aggregate_count(self, node: AggregateCount):
        # TODO SPLIT OP
        pass

    def _rewrite_aggregate_mean(self, node: AggregateMean):
        # TODO SPLIT OP
        pass

    def _rewrite_aggregate_std_dev(self, node: AggregateStdDev):
        # TODO SPLIT OP
        pass

    def _rewrite_project(self, node: Project):
        self._rewrite_unary_default(node)

    def _rewrite_multiply(self, node: Multiply):
        self._rewrite_unary_default(node)

    def _rewrite_divide(self, node: Divide):
        self._rewrite_unary_default(node)

    def _rewrite_limit(self, node: Limit):
        # TODO SPLIT OP
        pass

    def _rewrite_distinct(self, node: Distinct):
        # TODO SPLIT OP
        pass

    def _rewrite_filter_against_col(self, node: FilterAgainstCol):
        # TODO SPLIT OP
        pass

    def _rewrite_filter_against_scalar(self, node: FilterAgainstScalar):
        # TODO SPLIT OP
        pass

    def _rewrite_sort_by(self, node: SortBy):
        self._rewrite_default(node)

    def _rewrite_num_rows(self, node: NumRows):
        # TODO SPLIT OP
        pass

    def _rewrite_join(self, node: Join):
        self._rewrite_default(node)

    def _rewrite_concat(self, node: Concat):

        if node.requires_mpc():
            if len(node.children) > 1 and node.is_upper_boundary():
                fork_node(node)

    def _rewrite_store(self, node: Store):
        self._rewrite_default(node)

    def _rewrite_persist(self, node: Persist):
        self._rewrite_default(node)

    def _rewrite_send(self, node: Send):
        self._rewrite_default(node)

    def _rewrite_index(self, node: Index):
        self._rewrite_default(node)

    def _rewrite_shuffle(self, node: Shuffle):
        self._rewrite_default(node)

    def _rewrite_member_filter(self, node: MemberFilter):
        self._rewrite_default(node)

    def _rewrite_column_union(self, node: ColumnUnion):
        self._rewrite_default(node)

