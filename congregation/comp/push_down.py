from congregation.dag.nodes import *
from congregation.dag.nodes.internal import *
from congregation.comp.rewriter import DagRewriter
from congregation.dag.nodes import OpNode
from congregation.comp.utils import *


class PushDown(DagRewriter):
    """
    TODO: Extend _rewrite_unary_default to instance where
     parent is a Join, think about others
    """
    def __init__(self):
        super(PushDown, self).__init__()

    @staticmethod
    def _rewrite_unary_default(node: UnaryOpNode):
        """
        Default operation that pushes the MPC boundary past
        this node, allowing it to be done locally.
        """

        if node.is_leaf():
            return

        parent = next(iter(node.parents))
        if parent.requires_mpc():
            if isinstance(parent, Concat) and parent.is_upper_boundary():
                push_parent_op_node_down(parent, node)
                parent.update_out_rel_cols()

    @staticmethod
    def _update_bottom_node(parent_node: Concat):

        if len(parent_node.children) != 1:
            raise Exception("TODO: Handle split ops with more than one child.")

        out_rel_cols_copy = copy.deepcopy(parent_node.out_rel.columns)
        child = next(iter(parent_node.children))
        if isinstance(child, (AggregateSum, AggregateMean, AggregateStdDev)):
            num_group_cols = len(child.group_cols)
            child.group_cols = out_rel_cols_copy[:num_group_cols]
            child.agg_col = out_rel_cols_copy[num_group_cols]
        elif isinstance(child, Distinct):
            child.selected_cols = out_rel_cols_copy
        else:
            raise Exception(f"Unexpected node type encountered in default split op: {type(child)}")
        child.update_out_rel_cols()

    def _rewrite_split_default(self, node: [AggregateSum, Distinct]):

        parent = next(iter(node.parents))
        if parent.requires_mpc():
            if isinstance(parent, Concat) and parent.is_upper_boundary():
                split_default(node)
                push_parent_op_node_down(parent, node)
                self._update_bottom_node(parent)

    def _rewrite_aggregate_sum(self, node: AggregateSum):
        self._rewrite_split_default(node)

    def _rewrite_aggregate_count(self, node: AggregateCount):

        parent = next(iter(node.parents))
        if parent.requires_mpc():
            if isinstance(parent, Concat) and parent.is_upper_boundary():
                split_agg_count(node)
                push_parent_op_node_down(parent, node)

    def _rewrite_aggregate_mean(self, node: AggregateMean):

        parent = next(iter(node.parents))
        if parent.requires_mpc():
            if isinstance(parent, Concat) and parent.is_upper_boundary():
                split_agg_mean(node, parent)
                # node.parent is now AggregateSumCountCol
                push_parent_op_node_down(parent, node.parent)
                self._update_bottom_node(parent)

    def _rewrite_aggregate_std_dev(self, node: AggregateStdDev):

        parent = next(iter(node.parents))
        if parent.requires_mpc():
            if isinstance(parent, Concat) and parent.is_upper_boundary():
                split_agg_std_dev(node, parent)
                # node.parent is now AggregateSumSquaresAndCount
                push_parent_op_node_down(parent, node.parent)
                self._update_bottom_node(parent)

    def _rewrite_project(self, node: Project):
        self._rewrite_unary_default(node)

    def _rewrite_multiply(self, node: Multiply):
        self._rewrite_unary_default(node)

    def _rewrite_divide(self, node: Divide):
        self._rewrite_unary_default(node)

    def _rewrite_limit(self, node: Limit):
        pass

    def _rewrite_distinct(self, node: Distinct):
        self._rewrite_split_default(node)

    def _rewrite_filter_against_col(self, node: FilterAgainstCol):
        self._rewrite_unary_default(node)

    def _rewrite_filter_against_scalar(self, node: FilterAgainstScalar):
        self._rewrite_unary_default(node)

    def _rewrite_sort_by(self, node: SortBy):
        pass

    def _rewrite_num_rows(self, node: NumRows):

        parent = next(iter(node.parents))
        if parent.requires_mpc():
            if isinstance(parent, Concat) and parent.is_upper_boundary():
                split_num_rows(node)
                push_parent_op_node_down(parent, node)
                parent.update_out_rel_cols()

    def _rewrite_join(self, node: Join):
        pass

    def _rewrite_concat(self, node: Concat):

        if node.requires_mpc():
            if len(node.children) > 1 and node.is_upper_boundary():
                fork_node(node)

    def _rewrite_store(self, node: Store):
        pass

    def _rewrite_persist(self, node: Persist):
        pass

    def _rewrite_send(self, node: Send):
        pass

    def _rewrite_index(self, node: Index):
        pass

    def _rewrite_shuffle(self, node: Shuffle):
        pass

    def _rewrite_member_filter(self, node: MemberFilter):
        pass

    def _rewrite_column_union(self, node: ColumnUnion):
        pass

