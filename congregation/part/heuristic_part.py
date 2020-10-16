from congregation.dag import Dag
from congregation.dag.nodes import *
from congregation.dag.nodes.internal import *
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
          mpc between different (possibly overlapping) sets of
          compute parties
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
        candidate_roots = set()
        while next_dag.roots:

            if iterations >= iteration_limit:
                raise Exception("Maximum iterations reached while partitioning.")

            next_partition, candidate_roots = self.get_next_partition(next_dag, candidate_roots)
            if next_partition is None:
                break
            ret.append(next_partition)

            iterations += 1
        print(f"Successfully partitioned DAG into {len(ret)} after {iterations} iterations.")

        return [(d, self.resolve_framework(d)) for d in ret]

    def get_next_partition(self, dag: Dag, candidate_roots: set):

        # roots of returned partition
        roots_in_partition = set()

        # there will always be a next root, bc of while loop in parent method
        next_root = dag.roots.pop()
        possible_roots = self._get_next_partition(next_root, next_root.requires_mpc())
        roots_in_partition.add(next_root)
        for r in possible_roots:
            # can assume these amount to a single DAG because we assume a single collect() node
            candidate_roots.add(r)

        remaining_available_roots = list(dag.roots)
        for r in remaining_available_roots:
            if r.out_rel.stored_with == next_root.out_rel.stored_with:
                dag.roots.remove(r)
                possible_roots = self._get_next_partition(r, next_root.requires_mpc())
                roots_in_partition.add(r)
                for rr in possible_roots:
                    candidate_roots.add(rr)

        if not dag.roots and candidate_roots:
            dag.roots = candidate_roots
            return Dag(roots_in_partition), set()

        return Dag(roots_in_partition), candidate_roots

    def _get_next_partition(self, node: OpNode, requires_mpc: bool):
        """
        This method can only handle cases where, if a node has multiple
        children and is an upper / lower boundary, it is a boundary
        with respect to all of it's child nodes and not just a subset of
        them.
        """

        if len(node.children) > 1:
            raise Exception("Can't partition DAG with nodes that have more than one child.")

        if node.children:
            c = next(iter(node.children))
            if not (c.requires_mpc() == requires_mpc):
                disconnected_children = disconnect_from_children(node)
                return disconnected_children
            else:
                return self._get_next_partition(c, requires_mpc)
        else:
            return []

    def resolve_framework(self, dag: Dag):

        requires_mpc = [node.requires_mpc() for node in dag.roots]

        if not all(requires_mpc) and any(requires_mpc):
            raise Exception(f"Mix of frameworks detected for this DAG: \n{str(dag)}")

        if all(requires_mpc):
            return self.mpc_framework
        else:
            return self.local_framework
