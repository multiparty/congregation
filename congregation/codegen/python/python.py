from congregation.codegen.base import CodeGen
from congregation.dag import Dag
import os
import pystache


class PythonCodeGen(CodeGen):
    def __init__(self, config, dag: Dag):
        super(PythonCodeGen, self).__init__(config, dag)
        self.templates_dir = f"{os.path.dirname(os.path.realpath(__file__))}/templates/"

    def generate(self, job_name: str):
        """ TODO """

    def write_code(self, jobe_name: str, code: str):
        """ TODO """

