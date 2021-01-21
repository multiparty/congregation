from congregation.codegen.codegen import CodeGen
from congregation.dag import Dag
from congregation.dag.nodes import *
from congregation.dag.nodes.internal import *
from congregation.job import JiffJob
import os
import pystache


class JiffCodeGen(CodeGen):
    def __init__(self, config, dag: Dag, job_name: [str, None] = None):
        super(JiffCodeGen, self).__init__(config, dag, job_name)
        self.templates_dir = f"{os.path.dirname(os.path.realpath(__file__))}/templates/"
        self.codegen_config = config.system_configs["JIFF_CODEGEN"]

    def generate(self):

        op_code = self._generate_code()
        for k in op_code.keys():
            if op_code[k]:
                self.write_code(op_code[k], k)

    def _generate_code(self):

        ret = dict()
        ret["helpers.js"] = open(f"{self.templates_dir}/modules/helpers.tmpl").read()
        ret["methods.js"] = open(f"{self.templates_dir}/modules/methods.tmpl").read()
        ret["mpc.js"] = self._generate_mpc()
        ret["party.js"] = self._generate_party()
        ret["server.js"] = self._generate_server()
        ret["run_client.sh"] = self._generate_client_bash()
        ret["run_server.sh"] = self._generate_server_bash()

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
            "NEGATIVE_NUMBER":
                self._generate_negative_number()
                if self.codegen_config.extensions["negative_number"]["use"]
                else "",
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
            "HEADERS": self._get_output_header(),
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
                    else "",
                "SERVER_PORT": self.codegen_config.server_port
            }
            return pystache.render(template, data)
        else:
            return ""

    def _generate_client_bash(self):

        template = open(f"{self.templates_dir}/bash/run_client.tmpl").read()
        data = {
            "JIFF_LIB_PATH": self.codegen_config.jiff_lib_path,
            "CODE_PATH": f"{self.codegen_config.code_path}/{self.job_name}"
        }

        return pystache.render(template, data)

    def _generate_server_bash(self):

        if self.codegen_config.server_pid == self.codegen_config.pid:
            template = open(f"{self.templates_dir}/bash/run_server.tmpl").read()
            data = {
                "JIFF_LIB_PATH": self.codegen_config.jiff_lib_path,
                "CODE_PATH": f"{self.codegen_config.code_path}/{self.job_name}"
            }

            return pystache.render(template, data)
        else:
            return ""

    def _get_output_path(self):

        ordered = self.dag.top_sort()
        if not isinstance(ordered[-1], Open):
            raise Exception(f"Terminal node of MPC job not Open(). Is type {type(ordered[-1])}")

        return f"{self.codegen_config.output_path}/{ordered[-1].out_rel.name}.csv"

    def _get_output_header(self):

        ordered = self.dag.top_sort()
        if not isinstance(ordered[-1], Open):
            raise Exception(f"Terminal node of MPC job not Open(). Is type {type(ordered[-1])}")

        return ",".join([c.name for c in ordered[-1].out_rel.columns])

    def _generate_big_number(self, file_type):

        try:
            template = open(f"{self.templates_dir}/{file_type}/extensions/big_number.tmpl").read()
            data = {
                "JIFF_LIB_PATH": self.codegen_config.jiff_lib_path
            }
            return pystache.render(template, data)
        except FileNotFoundError:
            print(f"ERROR: Unrecognized file_type: {file_type}.")
            return ""

    def _generate_fixed_point(self, file_type):

        try:
            template = open(f"{self.templates_dir}/{file_type}/extensions/fixed_point.tmpl").read()
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

        template = open(f"{self.templates_dir}/mpc/extensions/negative_number.tmpl").read()
        data = {
            "JIFF_LIB_PATH": self.codegen_config.jiff_lib_path
        }

        return pystache.render(template, data)

    def _add_share_plaintext(self, close_node: Close):

        data = {
            "VAR_NAME": close_node.out_rel.name,
            "USE_BIG_NUMBER": int(self.codegen_config.extensions["big_number"]["use"]),
            "INPUT_PARTY": close_node.holding_party,
            "ALL_PARTIES": self.codegen_config.all_pids
        }

        # if this party owns the data for this node, generate a
        # share string with the appropriate file path. if not,
        # generate a share string with the file path set to null
        if close_node.holding_party == self.codegen_config.pid:
            template = open(f"{self.templates_dir}/mpc/share/share_plaintext.tmpl").read()
            data["FILE_PATH"] = f"{self.codegen_config.input_path}/{close_node.out_rel.name}.csv"
        else:
            template = open(f"{self.templates_dir}/mpc/share/share_plaintext_null.tmpl").read()

        return f"{pystache.render(template, data)}\n"

    def _add_share_secret(self, create_node: Create):

        template = open(f"{self.templates_dir}/mpc/share/share_secret.tmpl").read()
        data = {
            "FILE_PATH": f"{self.codegen_config.input_path}/{create_node.out_rel.name}.csv",
            "USE_BIG_NUMBER": int(self.codegen_config.extensions["big_number"]["use"]),
            "COMPUTE_PARTIES": self.codegen_config.all_pids
        }

        return f"{pystache.render(template, data)}\n"

    def _generate_share(self):

        ret = ""
        for r in self.sorted_roots:
            if isinstance(r, Close):
                # stored data is plaintext, needs to be shared
                ret += self._add_share_plaintext(r)
            elif isinstance(r, Create):
                # stored data is shares, needs to be passed to Share constructor
                ret += self._add_share_secret(r)
            else:
                raise Exception(
                    f"Roots of DAG passed to Jiff codegen should be of type Create or Close, not {type(r)}."
                )

        return ret

    def _generate_inputs(self):

        ret = ""

        for idx, r in enumerate(self.sorted_roots):
            data = {
                "VAR_NAME": r.out_rel.name,
                "INDEX_SHARE": idx * 2,
                "INDEX_KEEP": (idx * 2) + 1
            }
            if isinstance(r, Close):
                template = open(f"{self.templates_dir}/mpc/share/assign_from_plaintext.tmpl").read()
                data["PARTY_ID"] = r.holding_party
            elif isinstance(r, Create):
                template = open(f"{self.templates_dir}/mpc/share/assign_from_share.tmpl").read()
            else:
                raise Exception(
                    f"Roots of DAG passed to Jiff codegen should be of type Create or Close, not {type(r)}."
                )
            ret += f"{pystache.render(template, data)}\n"

        return ret

    def generate_job(self):
        return JiffJob(self.job_name, self.codegen_config.code_path)

    def _generate_create(self, node: Create):

        if node not in self.dag.roots:
            raise Exception(
                f"Create node with out_rel {node.out_rel.name} not in DAG "
                f"roots for Jiff job {self.job_name}.")

        return ""

    def _generate_aggregate_count(self, node: AggregateCount):

        if len(node.group_cols) > 1:
            raise Exception("Multiple key columns for aggregation in JIFF not yet implemented.")

        template = open(f"{self.templates_dir}/mpc/methods/agg_count.tmpl").read()
        data = {
            "OUT_REL": node.out_rel.name,
            "IN_REL": node.get_in_rel().name,
            "KEY_COL": "null" if len(node.group_cols) == 0 else [n.idx for n in node.group_cols][0]
        }

        return pystache.render(template, data)

    def _generate_aggregate_sum(self, node: AggregateSum):

        if len(node.group_cols) > 1:
            raise Exception("Multiple key columns for aggregation in JIFF not yet implemented.")

        template = open(f"{self.templates_dir}/mpc/methods/agg_sum.tmpl").read()
        data = {
            "OUT_REL": node.out_rel.name,
            "IN_REL": node.get_in_rel().name,
            "KEY_COL": "null" if len(node.group_cols) == 0 else [n.idx for n in node.group_cols][0],
            "AGG_COL": node.agg_col.idx
        }

        return pystache.render(template, data)

    def _generate_aggregate_mean(self, node: AggregateMean):

        if len(node.group_cols) > 1:
            raise Exception("Multiple key columns for aggregation in JIFF not yet implemented.")

        template = open(f"{self.templates_dir}/mpc/methods/agg_mean.tmpl").read()
        data = {
            "OUT_REL": node.out_rel.name,
            "IN_REL": node.get_in_rel().name,
            "KEY_COL": "null" if len(node.group_cols) == 0 else [n.idx for n in node.group_cols][0],
            "AGG_COL": node.agg_col.idx,
            "COUNT_COL": 1 if node.with_count_col else 0
        }

        return pystache.render(template, data)

    def _generate_aggregate_std_dev(self, node: AggregateStdDev):

        if len(node.group_cols) > 1:
            raise Exception("Multiple key columns for aggregation in JIFF not yet implemented.")

        template = open(f"{self.templates_dir}/mpc/methods/agg_std_dev.tmpl").read()
        data = {
            "OUT_REL": node.out_rel.name,
            "IN_REL": node.get_in_rel().name,
            "KEY_COL": "null" if len(node.group_cols) == 0 else [n.idx for n in node.group_cols][0],
            "AGG_COL": node.agg_col.idx,
            "SQUARES_COL": "null" if not node.push_down_optimized else len(node.get_in_rel().columns) - 2,
            "COUNT_COL": "null" if not node.push_down_optimized else len(node.get_in_rel().columns) - 1,
            "DO_DIFF_FLAG": "true" if not node.push_up_optimized else "false"
        }

        return pystache.render(template, data)

    def _generate_project(self, node: Project):

        template = open(f"{self.templates_dir}/mpc/methods/project.tmpl").read()
        data = {
            "OUT_REL": node.out_rel.name,
            "IN_REL": node.get_in_rel().name,
            "SELECTED_COLS": [c.idx for c in node.selected_cols]
        }

        return pystache.render(template, data)

    @staticmethod
    def _generate_arithmetic_commutative(node: [Add, Multiply]):

        new_col = 1 if node.target_col.idx == len(node.get_in_rel().columns) else 0

        return {
            "OUT_REL": node.out_rel.name,
            "IN_REL": node.get_in_rel().name,
            "COL_OPERANDS": [c.idx for c in node.operands if isinstance(c, Column)],
            "SCALAR_OPERANDS": [n for n in node.operands if not isinstance(n, Column)],
            "TARGET_COL": node.target_col.idx,
            "NEW_COL": new_col
        }

    @staticmethod
    def _generate_arithmetic_non_commutative(node: [Subtract, Divide]):

        new_col = 1 if node.target_col.idx == len(node.get_in_rel().columns) else 0
        operands = [
            {"__TYPE__": "col", "v": o.idx}
            if isinstance(o, Column)
            else {"__TYPE__": "scal", "v": o}
            for o in node.operands
        ]

        return {
            "OPERANDS": operands,
            "OUT_REL": node.out_rel.name,
            "IN_REL": node.get_in_rel().name,
            "TARGET_COL": node.target_col.idx,
            "NEW_COL": new_col
        }

    def _generate_add(self, node: Add):

        template = open(f"{self.templates_dir}/mpc/methods/add.tmpl").read()
        data = self._generate_arithmetic_commutative(node)

        return pystache.render(template, data)

    def _generate_subtract(self, node: Subtract):

        template = open(f"{self.templates_dir}/mpc/methods/subtract.tmpl").read()
        data = self._generate_arithmetic_non_commutative(node)

        return pystache.render(template, data)

    def _generate_multiply(self, node: Multiply):

        template = open(f"{self.templates_dir}/mpc/methods/multiply.tmpl").read()
        data = self._generate_arithmetic_commutative(node)

        return pystache.render(template, data)

    def _generate_divide(self, node: Divide):

        if len(node.get_in_rel().columns) == len(node.get_in_rel().columns) \
                and not isinstance(node.operands[0], Column):
            # can't have operands list starting with a scalar because we can't do
            # arithmetic operations in jiff like <scalar>.<operation>(<share>)
            # note that this is only the case for division since it isn't commutative
            raise Exception(
                f"Encountered operands list for Divide node whose first element is not of "
                f"Column type in code generation for Jiff job {self.job_name}."
            )

        template = open(f"{self.templates_dir}/mpc/methods/divide.tmpl").read()
        data = self._generate_arithmetic_non_commutative(node)

        return pystache.render(template, data)

    def _generate_limit(self, node: Limit):

        template = open(f"{self.templates_dir}/mpc/methods/limit.tmpl").read()
        data = {
            "OUT_REL": node.out_rel.name,
            "IN_REL": node.get_in_rel().name,
            "NUM": node.num
        }

        return pystache.render(template, data)

    def _generate_distinct(self, node: Distinct):
        """
        TODO: Need to extend this to support selected_col_names list of size > 1. Right now,
         we require size == 1 because the bubbleSort() method is limited to a single column.
        """

        if len(node.selected_cols) != 1:
            raise Exception("Distinct operator does not yet support more than one selected column.")

        template = open(f"{self.templates_dir}/mpc/methods/distinct.tmpl").read()
        data = {
            "OUT_REL": node.out_rel.name,
            "IN_REL": node.get_in_rel().name,
            "KEY_COL": node.selected_cols[0].idx
        }

        return pystache.render(template, data)

    def _generate_filter_against_col(self, node: FilterAgainstCol):

        template = open(f"{self.templates_dir}/mpc/methods/filter_against_col.tmpl").read()
        data = {
            "OUT_REL": node.out_rel.name,
            "IN_REL": node.get_in_rel().name,
            "KEY_COL": node.filter_col.idx,
            "AGAINST_COL": node.against_col.idx,
            "OPERATOR": node.operator
        }

        return pystache.render(template, data)

    def _generate_filter_against_scalar(self, node: FilterAgainstScalar):

        template = open(f"{self.templates_dir}/mpc/methods/filter_against_scalar.tmpl").read()
        data = {
            "OUT_REL": node.out_rel.name,
            "IN_REL": node.get_in_rel().name,
            "KEY_COL": node.filter_col.idx,
            "SCALAR": node.scalar,
            "OPERATOR": node.operator
        }

        return pystache.render(template, data)

    def _generate_sort_by(self, node: SortBy):

        template = open(f"{self.templates_dir}/mpc/methods/bubble_sort.tmpl").read()
        data = {
            "OUT_REL": node.out_rel.name,
            "IN_REL": node.get_in_rel().name,
            "KEY_COL": node.sort_by_col.idx
        }

        return pystache.render(template, data)

    def _generate_num_rows(self, node: NumRows):

        template = open(f"{self.templates_dir}/mpc/methods/num_rows.tmpl").read()
        data = {
            "OUT_REL": node.out_rel.name,
            "IN_REL": node.get_in_rel().name
        }

        return pystache.render(template, data)

    def _generate_collect(self, node: Collect):
        raise Exception("Collect node encountered during Jiff code generation.")

    def _generate_join(self, node: Join):

        if len(node.left_join_cols) > 1 or len(node.right_join_cols) > 1:
            raise Exception("Join operator does not yet support more than one key column.")

        template = open(f"{self.templates_dir}/mpc/methods/join.tmpl").read()
        data = {
            "OUT_REL": node.out_rel.name,
            "LEFT_IN_REL": node.get_left_in_rel().name,
            "RIGHT_IN_REL": node.get_right_in_rel().name,
            "LEFT_KEY": [n.idx for n in node.left_join_cols][0],
            "RIGHT_KEY": [n.idx for n in node.right_join_cols][0]
        }

        return pystache.render(template, data)

    def _generate_concat(self, node: Concat):

        template = open(f"{self.templates_dir}/mpc/methods/concat.tmpl").read()
        data = {
            "OUT_REL": node.out_rel.name,
            "IN_RELS": ",".join(r.name for r in node.get_in_rels()),
            "IN_RELS_KEEP": ",".join(f"{r.name}_keep_rows" for r in node.get_in_rels())
        }

        return pystache.render(template, data)

    def _generate_store(self, node: Store):
        raise Exception("Store node encountered during Jiff code generation.")

    def _generate_read(self, node: Read):
        raise Exception("Read node encountered during Jiff code generation.")

    def _generate_persist(self, node: Persist):
        raise Exception("Persist node encountered during Jiff code generation.")

    def _generate_send(self, node: Send):
        raise Exception("Send node encountered during Jiff code generation.")

    def _generate_index(self, node: Index):
        raise Exception("Index node encountered during Jiff code generation.")

    def _generate_shuffle(self, node: Shuffle):
        raise Exception("Shuffle node encountered during Jiff code generation.")

    def _generate_open(self, node: Open):

        template = open(f"{self.templates_dir}/mpc/methods/open.tmpl").read()
        data = {
            "IN_REL": node.get_in_rel().name
        }

        return pystache.render(template, data)

    def _generate_close(self, node: Close):

        if node not in self.dag.roots:
            raise Exception(
                f"Close node with out_rel {node.out_rel.name} not in DAG "
                f"roots for Jiff job {self.codegen_config.job_name}.")

        return ""

    def _generate_aggregate_sum_count_col(self, node: AggregateSumCountCol):
        raise Exception("AggregateSumCountCol node encountered during Jiff code generation.")

    def _generate_aggregate_sum_squares_and_count(self, node: AggregateSumSquaresAndCount):
        raise Exception("AggregateSumSquaresAndCount node encountered during Jiff code generation.")

    def _generate_aggregate_std_dev_local_sqrt(self, node: AggregateStdDevLocalSqrt):
        raise Exception("AggregateStdDevLocalSqrt node encountered during Jiff code generation.")

    def _generate_col_sum(self, node: ColSum):

        if len(node.get_in_rel().columns) > 0:
            raise Exception(f"ColSum node encountered with more than 1 column in input relation.")

        template = open(f"{self.templates_dir}/mpc/methods/agg_sum.tmpl").read()
        data = {
            "OUT_REL": node.out_rel.name,
            "IN_REL": node.get_in_rel().name,
            "KEY_COL": "null",
            "AGG_COL": 0
        }

        return pystache.render(template, data)
