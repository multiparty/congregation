from congregation.codegen.codegen import CodeGen
from congregation.dag import Dag
from congregation.dag.nodes import *
from congregation.dag.nodes.internal import *
from congregation.job.python import PythonJob
from congregation.config import CodeGenConfig
import os
import pystache
space = "    "


class PythonCodeGen(CodeGen):
    def __init__(self, config: CodeGenConfig, dag: Dag):
        super(PythonCodeGen, self).__init__(config, dag)
        self.templates_dir = f"{os.path.dirname(os.path.realpath(__file__))}/templates/"

    def generate(self, job_name):

        op_code = self._generate_code()
        self.write_code(job_name, op_code, "workflow.py")
        job = self._generate_job(job_name)
        return job

    def _generate_code(self):

        op_code = super()._generate_code()
        template = open(f"{self.templates_dir}/top_level.tmpl").read()
        data = {"OP_CODE": op_code}
        return pystache.render(template, data)

    def _generate_job(self, job_name: str):
        return PythonJob(job_name, self.config.code_path)

    def _generate_create(self, node: Create):
        return f"\n{space}{node.out_rel.name} = " \
               f"create(\"{self.config.input_path}/{node.out_rel.name}.csv\")"

    def _generate_aggregate_count(self, node: AggregateCount):

        group_cols_idx = [c.idx for c in node.group_cols]
        return f"\n{space}{node.out_rel.name} = " \
               f"aggregate_count({node.get_in_rel().name}, {group_cols_idx})"

    def _generate_aggregate_sum(self, node: AggregateSum):

        group_cols_idx = [c.idx for c in node.group_cols]
        agg_col_idx = node.agg_col.idx
        return f"\n{space}{node.out_rel.name} = " \
               f"aggregate_sum({node.get_in_rel().name}, {group_cols_idx}, {agg_col_idx})"

    def _generate_aggregate_mean(self, node: AggregateMean):

        group_cols_idx = [c.idx for c in node.group_cols]
        agg_col_idx = node.agg_col.idx
        return f"\n{space}{node.out_rel.name} = " \
               f"aggregate_mean({node.get_in_rel().name}, {group_cols_idx}, {agg_col_idx})"

    def _generate_aggregate_std_dev(self, node: AggregateStdDev):
        # TODO
        return ""

    def _generate_project(self, node: Project):

        proj_cols_idx = [c.idx for c in node.selected_cols]
        return f"\n{space}{node.out_rel.name} = " \
               f"project({node.get_in_rel().name}, {proj_cols_idx})"

    def _generate_multiply(self, node: Multiply):

        col_operands = [c.idx for c in node.operands if isinstance(c, Column)]
        scalar_operands = [n for n in node.operands if not isinstance(n, Column)]
        return f"\n{space}{node.out_rel.name} = " \
               f"multiply({node.get_in_rel().name}, {col_operands}, {scalar_operands}, {node.target_col.idx})"

    def _generate_divide(self, node: Divide):

        operands = []
        for o in node.operands:
            if isinstance(o, Column):
                operands.append({"__TYPE__": "col", "v": o.idx})
            else:
                operands.append({"__TYPE__": "scal", "v": o})

        return f"\n{space}{node.out_rel.name} = " \
               f"divide({node.get_in_rel().name}, {operands}, {node.target_col.idx})"

    def _generate_limit(self, node: Limit):
        return f"\n{space}{node.out_rel.name} = " \
               f"limit({node.get_in_rel().name}, {node.num})"

    def _generate_distinct(self, node: Distinct):

        select_cols_idx = [c.idx for c in node.selected_cols]
        return f"\n{space}{node.out_rel.name} = " \
               f"distinct({node.get_in_rel().name}, {select_cols_idx})"

    def _generate_filter_against_col(self, node: FilterAgainstCol):
        return f"\n{space}{node.out_rel.name} = " \
               f"filter_against_col({node.get_in_rel().name}, {node.filter_col.idx}, " \
               f"{node.against_col.idx}, \"{node.operator}\")"

    def _generate_filter_against_scalar(self, node: FilterAgainstScalar):
        return f"\n{space}{node.out_rel.name} = " \
               f"filter_against_scalar({node.get_in_rel().name}, {node.filter_col.idx}, " \
               f"{node.scalar}, \"{node.operator}\")"

    def _generate_sort_by(self, node: SortBy):
        return f"\n{space}{node.out_rel.name} = " \
               f"sort_by({node.get_in_rel().name}, {node.sort_by_col.idx}, {node.increasing})"

    def _generate_num_rows(self, node: NumRows):
        return f"\n{space}{node.out_rel.name} = " \
               f"num_rows({node.get_in_rel().name})"

    def _generate_collect(self, node: Collect):

        output_path = f"{self.config.input_path}/{node.out_rel.name}.csv"
        return f"\n{space}collect({node.get_in_rel().name}, " \
               f"{[c.name for c in node.out_rel.columns]}, \"{output_path}\")"

    def _generate_join(self, node: Join):

        left_cols = [c.idx for c in node.left_join_cols]
        right_cols = [c.idx for c in node.right_join_cols]
        return f"\n{space}{node.out_rel.name} = " \
               f"join({node.get_left_in_rel().name}, \"{node.get_right_in_rel().name}\", " \
               f"{left_cols}, {right_cols})"

    def _generate_concat(self, node: Concat):
        return f"\n{space}{node.out_rel.name} = concat({[n.name for n in node.get_in_rels()]})"

    def _generate_store(self, node: Store):

        col_names = [f"\"{c.name}\"" for c in node.out_rel.columns]
        output_path = f"{self.config.input_path}/{node.out_rel.name}.csv"
        return f"\n{space}store({node.get_in_rel().name}, " \
               f"{col_names}, \"{output_path}\")"

    def _generate_persist(self, node: Persist):

        col_names = [f"\"{c.name}\"" for c in node.out_rel.columns]
        output_path = f"{self.config.input_path}/{node.out_rel.name}.csv"
        return f"\n{space}persist({node.get_in_rel().name}, " \
               f"{col_names}, {output_path})"

    def _generate_send(self, node: Send):
        # TODO
        return ""

    def _generate_index(self, node: Index):
        return f"\n{space}{node.out_rel.name} = " \
               f"index({node.get_in_rel().name})"

    def _generate_shuffle(self, node: Shuffle):
        return f"\n{space}{node.out_rel.name} = " \
               f"shuffle({node.get_in_rel().name})"

    def _generate_open(self, node: Open):
        raise Exception("Open node encountered during Python code generation.")

    def _generate_close(self, node: Close):
        raise Exception("Close node encountered during Python code generation.")

    def _generate_aggregate_sum_count_col(self, node: AggregateSumCountCol):

        group_cols_idx = [c.idx for c in node.group_cols]
        return f"\n{space}{node.out_rel.name} = " \
               f"aggregate_sum_count_col({node.get_in_rel().name}, {group_cols_idx}, {node.agg_col.idx})"

    def _generate_col_sum(self, node: ColSum):
        return f"\n{space}{node.out_rel.name} = col_sum({node.get_in_rel().name})"

    def _generate_member_filter(self, node: MemberFilter):
        return ""

    def _generate_column_union(self, node: ColumnUnion):
        return ""
