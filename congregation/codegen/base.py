from congregation.dag import Dag


class CodeGen:
    def __init__(self, config, dag: Dag):
        self.config = config
        self.dag = dag

    def generate(self, job_name: str):
        """ Overridden in subclasses """
        pass

    def write_code(self, jobe_name: str, code: str):
        """ Overridden in subclasses """
        pass
