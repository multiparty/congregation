from congregation.dag import Dag
from congregation.dag.nodes import *
from congregation.dag.nodes.internal import *
from congregation.comp.utils.dag import disconnect_from_child
import copy


class HeuristicPart:
    """
    Non-exhaustive partition. Returns best
    partition with respect to certain heuristics.

    More specifically, this partitioning assumes the following:
        - that there exists exactly one output (collect) node.
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
            else:
                next_partition, candidate_roots = self.get_next_partition(next_dag, candidate_roots)
                ret.append(next_partition)
                iterations += 1
        print(f"Successfully partitioned DAG into {len(ret)} jobs after {iterations} iterations.")

        return [(d, self.resolve_framework(d)) for d in ret]

    def get_next_partition(self, dag: Dag, candidate_roots: set):

        # there will always be a next root, bc of while loop condition in parent method
        next_root = dag.roots.pop()
        roots_in_partition = {next_root}

        possible_roots = self._get_next_partition(next_root, next_root.requires_mpc(), set())
        candidate_roots = candidate_roots.union(possible_roots)

        roots_in_partition, candidate_roots = \
            self._iterate_over_remaining_roots(dag, next_root, candidate_roots, roots_in_partition)

        if not dag.roots and candidate_roots:
            dag.roots = candidate_roots
            return Dag(roots_in_partition), set()
        else:
            return Dag(roots_in_partition), candidate_roots

    def _iterate_over_remaining_roots(
            self,
            dag: Dag,
            next_root: OpNode,
            candidate_roots: set,
            roots_in_partition: set
    ):

        remaining_available_roots = list(dag.roots)
        for r in remaining_available_roots:
            if r.out_rel.stored_with == next_root.out_rel.stored_with:

                dag.roots.remove(r)
                roots_in_partition.add(r)

                possible_roots = self._get_next_partition(r, next_root.requires_mpc(), set())
                for rr in possible_roots:
                    candidate_roots.add(rr)

        return roots_in_partition, candidate_roots

    def _get_next_partition(self, node: OpNode, requires_mpc: bool, next_roots: set):
        """
        Follow all possible paths down from node passed, disconnect
        nodes when MPC boundary detected, and recursively build up
        next_roots set to pass back to the partitioning process.
        """

        if node.children:
            ch = list(node.children)
            for c in ch:
                if not (c.requires_mpc() == requires_mpc):
                    disconnect_from_child(node, c)
                    next_roots.add(c)
                    self._get_next_partition(node, requires_mpc, next_roots)
                else:
                    self._get_next_partition(c, requires_mpc, next_roots)

        return next_roots

    def resolve_framework(self, dag: Dag):

        requires_mpc = [node.requires_mpc() for node in dag.roots]

        if not all(requires_mpc) and any(requires_mpc):
            raise Exception(f"Mix of frameworks detected for this DAG: \n{str(dag)}")

        if all(requires_mpc):
            return self.mpc_framework
        else:
            return self.local_framework
