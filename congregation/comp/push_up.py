from congregation.dag.nodes import *
from congregation.dag.nodes.internal import *
from congregation.comp.rewriter import DagRewriter
from congregation.dag.nodes import OpNode
from congregation.comp.utils import *


class PushUp(DagRewriter):
    def __init__(self):
        super(PushUp, self).__init__()
        self.reverse = True

    @staticmethod
    def _rewrite_unary_default(node: UnaryOpNode):
        """
        If a UnaryOpNode is at a lower MPC boundary
        and is reversible, it can be computed locally.
        """

        par = next(iter(node.parents))
        if node.is_reversible() and node.is_lower_boundary() and not par.is_root():
            temp_sw = copy.copy(node.out_rel.stored_with)
            flat_sw = [{s} for c in temp_sw for s in c]
            sw_to_set = set().union(*flat_sw)
            node.out_rel.stored_with = copy.copy(flat_sw)
            node.out_rel.assign_new_plaintext(copy.copy(sw_to_set))
            node.out_rel.assign_new_trust(copy.copy(sw_to_set))
            """
            NOTE TO SELF: changing the metadata on the parent relation
            is NOT dangerous because the way Open() nodes will be inserted
            will depend on the node.requires_mpc() function, which relies on
            the metadata of the PARENT and not the referenced node.
            """
            par.out_rel.assign_new_plaintext(copy.copy(sw_to_set))
            par.out_rel.assign_new_trust(copy.copy(sw_to_set))

    def _rewrite_aggregate_std_dev(self, node: AggregateStdDev):

        if node.is_lower_boundary():
            local_sqrt = AggregateStdDevLocalSqrt.from_existing_agg(node)
            local_sqrt.parents = set()
            local_sqrt.children = set()
            insert_between(node, next(iter(node.children)), local_sqrt)

            temp_sw = copy.copy(node.out_rel.stored_with)
            flat_sw = [{s} for c in temp_sw for s in c]
            sw_to_set = set().union(*flat_sw)

            node.push_up_optimized = True
            # node.update_out_rel_cols()
            node.out_rel.stored_with = copy.copy(flat_sw)
            node.out_rel.assign_new_plaintext(copy.copy(sw_to_set))
            node.out_rel.assign_new_trust(copy.copy(sw_to_set))

            local_sqrt.out_rel.stored_with = copy.copy(flat_sw)
            local_sqrt.out_rel.assign_new_plaintext(copy.copy(sw_to_set))
            local_sqrt.out_rel.assign_new_trust(copy.copy(sw_to_set))
        else:
            print(
                f"INFO: Encountered standard deviation node that couldn't "
                f"be split. The output column that represents the standard"
                f"deviation will instead represent the variance. The standard"
                f"deviation can be inferred from this value by taking its square"
                f"root."
            )

    def _rewrite_project(self, node: Project):
        self._rewrite_unary_default(node)

    def _rewrite_multiply(self, node: Multiply):
        self._rewrite_unary_default(node)

    def _rewrite_divide(self, node: Divide):
        self._rewrite_unary_default(node)

    def _rewrite_sort_by(self, node: SortBy):
        self._rewrite_unary_default(node)
