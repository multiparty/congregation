from congregation.dag import Dag


class HeuristicPart:
    def __init__(self, dag: Dag, mpc_frameworks: list, local_frameworks: list):
        self.dag = dag
        self.mpc_frameworks = mpc_frameworks
        self.local_frameworks = local_frameworks


