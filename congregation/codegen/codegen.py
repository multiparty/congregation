from congregation.dag import Dag
from congregation.dag.nodes import *
from congregation.dag.nodes.internal import *
from congregation.config import Config
import os


class CodeGen:
    def __init__(self, config: Config, dag: Dag, job_name: [str, None] = None):
        self.config = config
        self.codegen_config = config.system_configs["CODEGEN"]
        self.network_config = config.system_configs["NETWORK"]
        self.dag = dag
        self.job_name = "JOB" if job_name is None else job_name
        self.sorted_roots = sorted(list(dag.roots), key=lambda r: r.out_rel.name)
        self.pid = self.codegen_config.pid

    def generate(self):
        """ Overridden in subclasses """
        pass

    def write_code(self, code: str, filename: str):

        os.makedirs(f"{self.codegen_config.code_path}/{self.job_name}", exist_ok=True)
        code_file = open(f"{self.codegen_config.code_path}/{self.job_name}/{filename}", "w")
        code_file.write(code)
        code_file.close()

    def _generate_code(self):

        op_code = ""
        nodes = self.dag.top_sort()
        for node in nodes:
            gen_func = self._lookup(node)
            op_code += gen_func(node)

        return op_code

    def _generate_job(self):
        """ Overridden in subclasses """
        pass

    def _lookup(self, node: OpNode):

        node_lookup = {
            Create: self._generate_create,
            AggregateCount: self._generate_aggregate_count,
            AggregateSum: self._generate_aggregate_sum,
            AggregateMean: self._generate_aggregate_mean,
            AggregateStdDev: self._generate_aggregate_std_dev,
            Project: self._generate_project,
            Multiply: self._generate_multiply,
            Divide: self._generate_divide,
            Limit: self._generate_limit,
            Distinct: self._generate_distinct,
            FilterAgainstCol: self._generate_filter_against_col,
            FilterAgainstScalar: self._generate_filter_against_scalar,
            SortBy: self._generate_sort_by,
            NumRows: self._generate_num_rows,
            Collect: self._generate_collect,
            Join: self._generate_join,
            Concat: self._generate_concat,
            Store: self._generate_store,
            Read: self._generate_read,
            Persist: self._generate_persist,
            Send: self._generate_send,
            Index: self._generate_index,
            Shuffle: self._generate_shuffle,
            Open: self._generate_open,
            Close: self._generate_close,
            AggregateSumCountCol: self._generate_aggregate_sum_count_col,
            AggregateSumSquaresAndCount: self._generate_aggregate_sum_squares_and_count,
            AggregateStdDevLocalSqrt: self._generate_aggregate_std_dev_local_sqrt,
            ColSum: self._generate_col_sum,
            MemberFilter: self._generate_member_filter,
            ColumnUnion: self._generate_column_union
        }

        return node_lookup[type(node)]

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
        return ""

    def _generate_persist(self, node: Persist):
        return ""

    def _generate_send(self, node: Send):
        return ""

    def _generate_index(self, node: Index):
        return ""

    def _generate_shuffle(self, node: Shuffle):
        return ""

    def _generate_open(self, node: Open):
        return ""

    def _generate_close(self, node: Close):
        return ""

    def _generate_aggregate_sum_count_col(self, node: AggregateSumCountCol):
        return ""

    def _generate_aggregate_sum_squares_and_count(self, node: AggregateSumSquaresAndCount):
        return ""

    def _generate_aggregate_std_dev_local_sqrt(self, node: AggregateStdDevLocalSqrt):
        return ""

    def _generate_col_sum(self, node: ColSum):
        return ""

    def _generate_member_filter(self, node: MemberFilter):
        return ""

    def _generate_column_union(self, node: ColumnUnion):
        return ""
