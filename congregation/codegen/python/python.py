from congregation.codegen.codegen import CodeGen
from congregation.dag import Dag
from congregation.dag.nodes import *
from congregation.dag.nodes.internal import *
from congregation.job.python import PythonJob
from congregation.config import Config
import os
import pystache


class PythonCodeGen(CodeGen):
    def __init__(self, config: Config, dag: Dag, job_name: [str, None] = None):
        super(PythonCodeGen, self).__init__(config, dag, job_name)
        self.templates_dir = f"{os.path.dirname(os.path.realpath(__file__))}/templates/"
        self.space = "    "

    def generate(self):

        op_code = self._generate_code()
        self.write_code(op_code, "workflow.py")

    def _generate_code(self):

        op_code = super()._generate_code()
        template = open(f"{self.templates_dir}/top_level.tmpl").read()
        data = {"OP_CODE": op_code}
        return pystache.render(template, data)

    def generate_job(self):
        return PythonJob(self.job_name, self.codegen_config.code_path)

    def _generate_create(self, node: Create):

        file_path = \
            node.input_path \
            if node.input_path is not None \
            else f"\"{self.codegen_config.input_path}/{node.out_rel.name}.csv\""

        return \
            f"\n{self.space}{node.out_rel.name} = " \
            f"create({file_path}, {self.codegen_config.use_floats})"

    def _generate_aggregate_count(self, node: AggregateCount):

        group_cols_idx = [c.idx for c in node.group_cols]
        return f"\n{self.space}{node.out_rel.name} = " \
               f"aggregate_count({node.get_in_rel().name}, {group_cols_idx})"

    def _generate_aggregate_sum(self, node: AggregateSum):

        group_cols_idx = [c.idx for c in node.group_cols]
        return f"\n{self.space}{node.out_rel.name} = " \
               f"aggregate_sum({node.get_in_rel().name}, {group_cols_idx}, {node.agg_col.idx})"

    def _generate_aggregate_mean(self, node: AggregateMean):

        group_cols_idx = [c.idx for c in node.group_cols]
        return f"\n{self.space}{node.out_rel.name} = " \
               f"aggregate_mean({node.get_in_rel().name}, {group_cols_idx}, {node.agg_col.idx})"

    def _generate_aggregate_std_dev(self, node: AggregateStdDev):

        group_cols_idx = [c.idx for c in node.group_cols]
        return f"\n{self.space}{node.out_rel.name} = " \
               f"aggregate_std_dev({node.get_in_rel().name}, {group_cols_idx}, {node.agg_col.idx})"

    def _generate_aggregate_variance(self, node: AggregateVariance):

        group_cols_idx = [c.idx for c in node.group_cols]
        return f"\n{self.space}{node.out_rel.name} = " \
               f"aggregate_variance({node.get_in_rel().name}, {group_cols_idx}, {node.agg_col.idx})"

    def _generate_aggregate_min_max_median(self, node: MinMaxMedian):

        group_cols_idx = [c.idx for c in node.group_cols]
        return f"\n{self.space}{node.out_rel.name} = " \
               f"min_max_median({node.get_in_rel().name}, {group_cols_idx}, {node.agg_col.idx})"

    def _generate_deciles(self, node: Deciles):

        group_cols_idx = [c.idx for c in node.group_cols]
        return f"\n{self.space}{node.out_rel.name} = " \
               f"deciles({node.get_in_rel().name}, {group_cols_idx}, {node.agg_col.idx})"

    def _generate_all_stats(self, node: AllStats):
        group_cols_idx = [c.idx for c in node.group_cols]
        return f"\n{self.space}{node.out_rel.name} = " \
               f"all_stats({node.get_in_rel().name}, {group_cols_idx}, {node.agg_col.idx})"

    def _generate_project(self, node: Project):

        proj_cols_idx = [c.idx for c in node.selected_cols]
        return f"\n{self.space}{node.out_rel.name} = " \
               f"project({node.get_in_rel().name}, {proj_cols_idx})"

    def _generate_add(self, node: Add):

        col_operands = [c.idx for c in node.operands if isinstance(c, Column)]
        scalar_operands = [n for n in node.operands if not isinstance(n, Column)]
        return f"\n{self.space}{node.out_rel.name} = " \
               f"add({node.get_in_rel().name}, {col_operands}, {scalar_operands}, {node.target_col.idx})"

    def _generate_subtract(self, node: Subtract):

        operands = [
            {"__TYPE__": "col", "v": o.idx}
            if isinstance(o, Column)
            else {"__TYPE__": "scal", "v": o}
            for o in node.operands
        ]

        return f"\n{self.space}{node.out_rel.name} = " \
               f"subtract({node.get_in_rel().name}, {operands}, {node.target_col.idx})"

    def _generate_multiply(self, node: Multiply):

        col_operands = [c.idx for c in node.operands if isinstance(c, Column)]
        scalar_operands = [n for n in node.operands if not isinstance(n, Column)]
        return f"\n{self.space}{node.out_rel.name} = " \
               f"multiply({node.get_in_rel().name}, {col_operands}, {scalar_operands}, {node.target_col.idx})"

    def _generate_divide(self, node: Divide):

        operands = [
            {"__TYPE__": "col", "v": o.idx}
            if isinstance(o, Column)
            else {"__TYPE__": "scal", "v": o}
            for o in node.operands
        ]

        return f"\n{self.space}{node.out_rel.name} = " \
               f"divide({node.get_in_rel().name}, {operands}, {node.target_col.idx})"

    def _generate_limit(self, node: Limit):
        return f"\n{self.space}{node.out_rel.name} = " \
               f"limit({node.get_in_rel().name}, {node.num})"

    def _generate_distinct(self, node: Distinct):

        select_cols_idx = [c.idx for c in node.selected_cols]
        return f"\n{self.space}{node.out_rel.name} = " \
               f"distinct({node.get_in_rel().name}, {select_cols_idx})"

    def _generate_filter_against_col(self, node: FilterAgainstCol):
        return f"\n{self.space}{node.out_rel.name} = " \
               f"filter_against_col({node.get_in_rel().name}, {node.filter_col.idx}, " \
               f"{node.against_col.idx}, \"{node.operator}\")"

    def _generate_filter_against_scalar(self, node: FilterAgainstScalar):
        return f"\n{self.space}{node.out_rel.name} = " \
               f"filter_against_scalar({node.get_in_rel().name}, {node.filter_col.idx}, " \
               f"{node.scalar}, \"{node.operator}\")"

    def _generate_sort_by(self, node: SortBy):
        return f"\n{self.space}{node.out_rel.name} = " \
               f"sort_by({node.get_in_rel().name}, {node.sort_by_col.idx}, {node.increasing})"

    def _generate_num_rows(self, node: NumRows):
        return f"\n{self.space}{node.out_rel.name} = " \
               f"num_rows({node.get_in_rel().name})"

    def _generate_collect(self, node: Collect):

        output_path = f"{self.codegen_config.input_path}/{node.out_rel.name}.csv"
        return f"\n{self.space}collect({node.get_in_rel().name}, " \
               f"{[c.name for c in node.out_rel.columns]}, \"{output_path}\")"

    def _generate_join(self, node: Join):

        left_cols = [c.idx for c in node.left_join_cols]
        right_cols = [c.idx for c in node.right_join_cols]
        return f"\n{self.space}{node.out_rel.name} = " \
               f"join({node.get_left_in_rel().name}, \"{node.get_right_in_rel().name}\", " \
               f"{left_cols}, {right_cols})"

    def _generate_concat(self, node: Concat):
        return f"\n{self.space}{node.out_rel.name} = concat({[n.name for n in node.get_in_rels()]})"

    def _generate_store(self, node: Store):

        col_names = [f"\"{c.name}\"" for c in node.out_rel.columns]
        output_path = f"{self.codegen_config.input_path}/{node.out_rel.name}.csv"
        return f"\n{self.space}store({node.get_in_rel().name}, " \
               f"{col_names}, \"{output_path}\")"

    def _generate_read(self, node: Read):

        return f"\n{self.space}{node.out_rel.name} = " \
               f"read(\"{self.codegen_config.input_path}/{node.out_rel.name}.csv\", " \
               f"{self.codegen_config.use_floats})"

    def _generate_persist(self, node: Persist):

        col_names = [f"\"{c.name}\"" for c in node.out_rel.columns]
        output_path = f"{self.codegen_config.input_path}/{node.out_rel.name}.csv"
        return f"\n{self.space}persist({node.get_in_rel().name}, " \
               f"{col_names}, {output_path})"

    def _generate_send(self, node: Send):
        raise NotImplementedError("Send not yet implemented for Python codegen.")

    def _generate_index(self, node: Index):
        return f"\n{self.space}{node.out_rel.name} = " \
               f"index({node.get_in_rel().name})"

    def _generate_shuffle(self, node: Shuffle):
        return f"\n{self.space}{node.out_rel.name} = " \
               f"shuffle({node.get_in_rel().name})"

    def _generate_open(self, node: Open):
        raise Exception("Open node encountered during Python code generation.")

    def _generate_close(self, node: Close):
        raise Exception("Close node encountered during Python code generation.")

    def _generate_aggregate_sum_count_col(self, node: AggregateSumCountCol):

        group_cols_idx = [c.idx for c in node.group_cols]
        return f"\n{self.space}{node.out_rel.name} = " \
               f"aggregate_sum_count_col({node.get_in_rel().name}, {group_cols_idx}, {node.agg_col.idx})"

    def _generate_aggregate_sum_squares_and_count(self, node: AggregateSumSquaresAndCount):

        groups_cols_idx = [c.idx for c in node.group_cols]
        return f"\n{self.space}{node.out_rel.name} = " \
               f"aggregate_sum_squares_and_count({node.get_in_rel().name}, " \
               f"{groups_cols_idx}, {node.agg_col.idx})"

    def _generate_aggregate_std_dev_local_sqrt(self, node: AggregateStdDevLocalSqrt):
        return f"\n{self.space}{node.out_rel.name} = " \
               f"aggregate_std_dev_local_sqrt({node.get_in_rel().name})"

    def _generate_aggregate_variance_local_diff(self, node: AggregateVarianceLocalDiff):
        return f"\n{self.space}{node.out_rel.name} = " \
               f"aggregate_variance_local_diff({node.get_in_rel().name})"

    def _generate_all_stats_local_sqrt(self, node: AllStatsLocalSqrt):
        return f"\n{self.space}{node.out_rel.name} = " \
               f"all_stats_local_sqrt({node.get_in_rel().name})"

    def _generate_col_sum(self, node: ColSum):
        return f"\n{self.space}{node.out_rel.name} = col_sum({node.get_in_rel().name})"

    def _generate_member_filter(self, node: MemberFilter):
        raise NotImplementedError("MemberFilter not yet implemented for Python codegen.")

    def _generate_column_union(self, node: ColumnUnion):
        raise NotImplementedError("ColumnUnion not yet implemented for Python codegen.")
