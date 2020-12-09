from congregation.codegen.codegen import CodeGen
from congregation.dag import Dag
from congregation.dag.nodes import *
from congregation.dag.nodes.internal import *
from congregation.job import JiffJob
import os
import pystache


class JiffCodeGen(CodeGen):
    def __init__(self, config, dag: Dag):
        super(JiffCodeGen, self).__init__(config, dag)
        self.templates_dir = f"{os.path.dirname(os.path.realpath(__file__))}/templates/"
        self.codegen_config = config.system_configs["JIFF_CODEGEN"]

    def generate(self, job_name: str):

        op_code = self._generate_code()
        for k in op_code.keys():
            if op_code[k]:
                self.write_code(job_name, op_code[k], k)
        job = self._generate_job(job_name)
        return job

    def _generate_code(self):

        ret = dict()
        ret["helpers.js"] = open(f"{self.templates_dir}/modules/helpers.tmpl").read()
        ret["methods.js"] = open(f"{self.templates_dir}/modules/methods.tmpl").read()
        ret["mpc.js"] = self._generate_mpc()
        ret["party.js"] = self._generate_party()
        ret["server.js"] = self._generate_server()
        ret["run.sh"] = self._generate_bash()

        return ret

    def _generate_mpc(self):

        template = open(f"{self.templates_dir}/mpc/mpc.tmpl").read()
        data = {
            "JIFF_LIB_PATH": self.codegen_config.jiff_lib_path,
            "BIG_NUMBER":
                self._generate_big_number("mpc")
                if self.codegen_config.extensions["big_number"]["use"]
                else "",
            "FIXED_POINT":
                self._generate_fixed_point("mpc")
                if self.codegen_config.extensions["fixed_point"]["use"]
                else "",
            "NEGATIVE_NUMBER": self._generate_negative_number(),
            "SHARE_STR": self._generate_share(),
            "INPUTS_STR": self._generate_inputs(),
            "OP_CODE": super()._generate_code()
        }

        return pystache.render(template, data)

    def _generate_party(self):

        template = open(f"{self.templates_dir}/party/party.tmpl").read()
        data = {
            "BIG_NUMBER":
                self._generate_big_number("party")
                if self.codegen_config.extensions["big_number"]["use"]
                else "",
            "NUM_PARTIES": len(self.codegen_config.all_pids),
            "PARTY_ID": self.codegen_config.pid,
            "ZP": self.codegen_config.zp,
            "FIXED_POINT":
                self._generate_fixed_point("party")
                if self.codegen_config.extensions["fixed_point"]["use"]
                else "",
            "WRITE": 1,
            "OUTPUT_PATH": self._get_output_path(),
            "SERVER_IP_PORT": f"http://{self.codegen_config.server_ip}:{self.codegen_config.server_port}",
            "COMPUTATION_ID": self.codegen_config.workflow_name
        }

        return pystache.render(template, data)

    def _generate_server(self):

        if self.codegen_config.server_pid == self.codegen_config.pid:
            template = open(f"{self.templates_dir}/server/server.tmpl").read()
            data = {
                "JIFF_LIB_PATH": self.codegen_config.jiff_lib_path,
                "BIG_NUMBER":
                    self._generate_big_number("server")
                    if self.codegen_config.extensions["big_number"]["use"]
                    else ""
            }
            return pystache.render(template, data)
        else:
            return ""

    def _generate_bash(self):
        return ""

    def _get_output_path(self):

        ordered = self.dag.top_sort()
        if not isinstance(ordered[-1], Open):
            raise Exception(f"Terminal node of MPC job not Open(). Is type {type(ordered[-1])}")

        return f"{self.codegen_config.output_path}/{ordered[-1].out_rel.name}.csv"

    def _generate_big_number(self, file_type):

        try:
            template = open(f"{self.templates_dir}/{file_type}/big_number.tmpl").read()
            data = {
                "JIFF_LIB_PATH": self.codegen_config.jiff_lib_path
            }
            return pystache.render(template, data)
        except FileNotFoundError:
            print(f"ERROR: Unrecognized file_type: {file_type}.")
            return ""

    def _generate_fixed_point(self, file_type):

        try:
            template = open(f"{self.templates_dir}/{file_type}/fixed_point.tmpl").read()
        except FileNotFoundError:
            print(f"ERROR: Unrecognized file_type: {file_type}.")
            return ""

        if file_type == "mpc":
            data = {
                "JIFF_LIB_PATH": self.codegen_config.jiff_lib_path
            }
        elif file_type == "party":
            data = {
                "DECIMAL_DIGITS": int(self.codegen_config.extensions["fixed_point"]["decimal_digits"]),
                "INTEGER_DIGITS": int(self.codegen_config.extensions["fixed_point"]["integer_digits"])
            }
        else:
            raise Exception(
                f"ERROR: Unrecognized file_type: {file_type}."
            )

        return pystache.render(template, data)

    def _generate_negative_number(self):

        template = open(f"{self.templates_dir}/mpc/negative_number.tmpl").read()
        data = {
            "JIFF_LIB_PATH": self.codegen_config.jiff_lib_path
        }

        return pystache.render(template, data)

    def _generate_share(self):
        return ""

    def _generate_inputs(self):
        return ""

    def _generate_job(self, job_name: str):
        return JiffJob(job_name, f"{self.codegen_config.code_path}/{job_name}")

    def _generate_create(self, node: Create):
        return ""

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
