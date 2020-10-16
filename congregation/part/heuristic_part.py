from congregation.dag import Dag
from congregation.dag.nodes import *
from congregation.comp.utils.dag import disconnect_from_children
import copy


class HeuristicPart:
    """
    Non-exhaustive partition. Returns best
    partition with respect to certain heuristics.

    More specifically, this partitioning assumes the following:
        - that all nodes in the DAG have exactly one child, and
          that there exists exactly one output (collect) node.
        - that compute parties between different partitions flip
          in a "requires mpc" / "does not require mpc" pattern,
          i.e. - there are no sequential partitions that require
          mpc between some set of compute parties, and then those
          shares are passed to be computed over by some different
          (possibly overlapping) set of compute parties.
    """
    def __init__(
            self,
            dag: Dag,
            mpc_framework: [str, None] = "jiff",
            local_framework: [str, None] = "python"
    ):
        self.dag = dag
        self.mpc_framework = mpc_framework
        self.local_framework = local_framework

    def partition(self, iteration_limit: [int, None] = 100):

        ret = []
        next_dag = copy.deepcopy(self.dag)

        iterations = 0
        while next_dag.roots:

            if iterations >= iteration_limit:
                raise Exception("Maximum iterations reached while partitioning.")

            # TODO: this needs to return the next partition
            # TODO: this also needs to assign new roots to the DAG
            #  (if there are none left) before returning the partition
            next_partition = self.get_next_partition(next_dag)
            if next_partition is None:
                break
            ret.append(next_partition)

            iterations += 1
        print(f"Successfully partitioned DAG into {len(ret)} after {iterations} iterations.")

        return ret

    def get_next_partition(self, dag: Dag):
        """
        TODO: This needs to add roots to roots_in_partition
            - once a root is added to roots_in_partition, we disconnect it
            from the parent DAG. While disconnecting it, add it's children
            to candidate_roots set
            - then, iterate over remaining roots of the parent DAG to see
            if they belong to this partition
            - if no roots are left in the parent DAG before returning the
            partition, assign candidate_roots to parent DAG's roots set
        """

        # roots of returned partition
        roots_in_partition = set()
        # roots to assign if this partition exhausts all remaining roots
        candidate_roots = set()

        # there will always be a next root, bc of while loop in parent method
        next_root = dag.roots.pop()
        possible_roots = self._get_next_partition(next_root, next_root.requires_mpc())
        roots_in_partition.add(next_root)
        for r in possible_roots:
            # can assume these amount to a single DAG because we assume a single collect() node
            candidate_roots.add(r)

        for r in dag.roots:
            if r.out_rel.stored_with == next_root.out_rel.stored_with:
                possible_roots = self._get_next_partition(r, next_root.requires_mpc())
                roots_in_partition.add(r)
                for rr in possible_roots:
                    candidate_roots.add(rr)

        if not dag.roots and candidate_roots:
            dag.roots = candidate_roots

        return Dag(roots_in_partition)

    def _get_next_partition(self, node: OpNode, requires_mpc: bool):

        if len(node.children) > 1:
            raise Exception("Can't partition DAG with nodes that have more than one child.")

        if node.children:
            c = next(iter(node.children))
            if not (c.requires_mpc() == requires_mpc):
                if requires_mpc:
                    return self._handle_disconnect_from_mpc(node)
                else:
                    return self._handle_disconnect_to_mpc(node)
            else:
                return self._get_next_partition(c, requires_mpc)
        else:
            return []

    @staticmethod
    def _handle_disconnect_to_mpc(node: OpNode):
        # TODO: do insert store/read shit too

        disconnected_children = disconnect_from_children(node)
        return disconnected_children

    @staticmethod
    def _handle_disconnect_from_mpc(node: OpNode):
        # TODO: do insert store/read shit too

        disconnected_children = disconnect_from_children(node)
        return disconnected_children

    def resolve_framework(self, dag: Dag):

        requires_mpc = [node.requires_mpc() for node in dag.roots]

        if not all(requires_mpc) and any(requires_mpc):
            raise Exception(f"Mix of frameworks detected for this DAG: \n{str(dag)}")

        if all(requires_mpc):
            return self.mpc_framework
        else:
            return self.local_framework



