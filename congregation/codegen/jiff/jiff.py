from congregation.codegen.codegen import CodeGen
from congregation.dag import Dag
from congregation.dag.nodes import *
from congregation.dag.nodes.internal import *
import os
import pystache


class JiffCodeGen(CodeGen):
    def __init__(self, config, dag: Dag):
        super(JiffCodeGen, self).__init__(config, dag)
        self.templates_dir = f"{os.path.dirname(os.path.realpath(__file__))}/templates/"
        self.codegen_config = config.system_configs["JIFF_CODEGEN"]

    def generate(self, job_name: str):
        """ TODO """

        op_code = self._generate_code()
        # self.write_code() for each key in dict returned by self._generate_code()
        # return single JiffJob

    def _generate_code(self):
        """ TODO """

        ret = dict()
        ret["mpc.js"] = super()._generate_code()
        if self.codegen_config.server_pid == self.codegen_config.pid:
            ret["server.js"] = self._generate_server()
        # return a dict whose keys are filenames (party.js, mpc.js, etc.)
        # and whose values are all code as str
        return ret

    def _generate_server(self):
        """ TODO """

        server_template = open(f"{self.templates_dir}/server/server.tmpl").read()
        data = {
            "JIFF_PATH": self.codegen_config.jiff_lib_path
        }
        # JIFF_PATH, BIG_NUMBER
        return ""

    def _generate_job(self, job_name: str):
        """ TODO """

    def _generate_create(self, node: Create):
        raise Exception("Create node encountered during Jiff code generation.")

    def _generate_aggregate_count(self, node: AggregateCount):
        return ""

    def _generate_aggregate_sum(self, node: AggregateSum):
        return ""

    def _generate_aggregate_mean(self, node: AggregateMean):
        return ""

    def _generate_aggregate_std_dev(self, node: AggregateStdDev):
        return ""

    def _generate_project(self, node: Project):
        return ""

    def _generate_multiply(self, node: Multiply):
        return ""

    def _generate_divide(self, node: Divide):
        return ""

    def _generate_limit(self, node: Limit):
        return ""

    def _generate_distinct(self, node: Distinct):
        return ""

    def _generate_filter_against_col(self, node: FilterAgainstCol):
        return ""

    def _generate_filter_against_scalar(self, node: FilterAgainstScalar):
        return ""

    def _generate_sort_by(self, node: SortBy):
        return ""

    def _generate_num_rows(self, node: NumRows):
        return ""

    def _generate_collect(self, node: Collect):
        return ""

    def _generate_join(self, node: Join):
        return ""

    def _generate_concat(self, node: Concat):
        return ""

    def _generate_store(self, node: Store):
        return ""

    def _generate_read(self, node: Read):
        raise Exception("Read node encountered during Jiff code generation.")

    def _generate_persist(self, node: Persist):
        return ""

    def _generate_send(self, node: Send):
        raise Exception("Send node encountered during Jiff code generation.")

    def _generate_index(self, node: Index):
        return ""

    def _generate_shuffle(self, node: Shuffle):
        return ""

    def _generate_open(self, node: Open):
        return ""

    def _generate_close(self, node: Close):
        return ""

    def _generate_aggregate_sum_count_col(self, node: AggregateSumCountCol):
        raise Exception("AggregateSumCountCol node encountered during Jiff code generation.")

    def _generate_aggregate_sum_squares_and_count(self, node: AggregateSumSquaresAndCount):
        raise Exception("AggregateSumSquaresAndCount node encountered during Jiff code generation.")

    def _generate_aggregate_std_dev_local_sqrt(self, node: AggregateStdDevLocalSqrt):
        raise Exception("AggregateStdDevLocalSqrt node encountered during Jiff code generation.")

    def _generate_col_sum(self, node: ColSum):
        return ""
