import copy
from congregation.datasets import *
from congregation.dag.nodes.unary import *
from congregation.utils.col import *


def create(name: str, columns: list, stored_with: [set, list], input_path: [str, None] = None):

    cols_in_rel = [
        Column(name, col_name, idx, type_str, trust_set, plaintext_set)
        for idx, (col_name, type_str, trust_set, plaintext_set) in enumerate(columns)
    ]

    if isinstance(stored_with, set):
        stored_with = [stored_with]
    out_rel = Relation(name, cols_in_rel, stored_with)
    op = Create(out_rel, input_path=input_path)

    return op


def aggregate(input_op_node: OpNode, name: str, group_col_names: [list, None], agg_col_name: str,
              agg_type: str, agg_out_col_name: [str, None] = None):

    in_rel = input_op_node.out_rel
    in_cols = in_rel.columns

    group_cols, out_group_cols = construct_group_cols(in_cols, group_col_names)
    agg_out_col = construct_target_col(in_cols, agg_col_name, out_group_cols, agg_out_col_name)
    out_rel_cols = out_group_cols + [copy.deepcopy(agg_out_col)]

    out_rel = Relation(name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    if agg_type == "sum":
        op = AggregateSum(out_rel, input_op_node, group_cols, agg_out_col)
    elif agg_type == "mean":
        op = AggregateMean(out_rel, input_op_node, group_cols, agg_out_col)
    elif agg_type == "std_dev":
        op = AggregateStdDev(out_rel, input_op_node, group_cols, agg_out_col)
    elif agg_type == "variance":
        op = AggregateVariance(out_rel, input_op_node, group_cols, agg_out_col)
    else:
        raise Exception(
            f"Aggregate type {agg_type} not recognized. \n"
            f"Must be one of the following: [sum, mean, std_dev, variance, min/max/median]."
        )

    input_op_node.children.add(op)
    return op


def _build_cols(rel_name: str, col_names: list, dtype: str, min_ts: set, min_pt: set, start_idx: int):

    return [
        Column(
            rel_name,
            col_names[i],
            i + start_idx,
            dtype,
            min_ts,
            min_pt
        )
        for i in range(len(col_names))
    ]


def min_max_median(input_op_node: OpNode, name: str, group_col_names: [list, None], target_col_name: str):

    in_rel = input_op_node.out_rel
    in_cols = in_rel.columns

    group_cols, out_group_cols = construct_group_cols(in_cols, group_col_names)
    agg_out_col = construct_target_col(in_cols, target_col_name, out_group_cols)
    mmm_cols = _build_cols(
        name,
        ["__MIN__", "__MAX__", "__MEDIAN__"],
        "INTEGER",
        agg_out_col.trust_with,
        agg_out_col.plaintext,
        len(group_cols)
    )
    out_rel_cols = out_group_cols + copy.deepcopy(mmm_cols)

    out_rel = Relation(name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = MinMaxMedian(out_rel, input_op_node, group_cols, agg_out_col)
    input_op_node.children.add(op)

    return op


def deciles(input_op_node: OpNode, name: str, group_col_names: [list, None], target_col_name: str):

    in_rel = input_op_node.out_rel
    in_cols = in_rel.columns

    group_cols, out_group_cols = construct_group_cols(in_cols, group_col_names)
    agg_out_col = construct_target_col(in_cols, target_col_name, out_group_cols)
    decile_cols = _build_cols(
        name,
        ["1-DECILE", "2-DECILE", "3-DECILE", "4-DECILE", "5-DECILE", "6-DECILE", "7-DECILE", "8-DECILE", "9-DECILE"],
        "INTEGER",
        agg_out_col.trust_with,
        agg_out_col.plaintext,
        len(group_cols)
    )
    out_rel_cols = out_group_cols + copy.deepcopy(decile_cols)

    out_rel = Relation(name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()


def aggregate_count(input_op_node: OpNode, name: str, group_col_names: list, count_col_name: [str, None] = "__COUNT__"):

    in_rel = input_op_node.out_rel
    in_cols = in_rel.columns
    group_cols = sorted([find(in_cols, group_col_name) for group_col_name in group_col_names], key=lambda c: c.idx)
    count_col = Column(name, count_col_name, len(group_cols), "INTEGER", set(), set())

    min_trust = min_trust_with_from_cols(group_cols)
    min_pt = min_pt_set_from_cols(group_cols)
    out_rel_cols = [copy.deepcopy(group_col) for group_col in group_cols] + [count_col]

    for c in out_rel_cols:
        c.plaintext = min_pt
        c.trust_with = min_trust

    out_rel = Relation(name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = AggregateCount(out_rel, input_op_node, group_cols, count_col)
    input_op_node.children.add(op)

    return op


def project(input_op_node: OpNode, name: str, selected_col_names: list):

    in_rel = input_op_node.out_rel
    selected_cols = [find(in_rel.columns, col_name) for col_name in selected_col_names]
    out_rel_cols = copy.deepcopy(selected_cols)

    out_rel = Relation(name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = Project(out_rel, input_op_node, selected_cols)
    input_op_node.children.add(op)

    return op


def _arithmetic_build_out_rel(in_rel: Relation, name: str, target_col_name: str, operands: list):

    out_rel_cols = copy.deepcopy(in_rel.columns)
    operands = build_operands_from_in_rel(in_rel, operands)

    target_col = find(out_rel_cols, target_col_name)
    if target_col is None:
        cols_only = [c for c in operands if isinstance(c, Column)]
        min_trust_set = min_trust_with_from_cols(cols_only)
        min_pt_set = min_pt_set_from_cols(cols_only)
        col_type = infer_output_type(cols_only)
        target_col = Column(name, target_col_name, len(in_rel.columns), col_type, min_trust_set, plaintext=min_pt_set)
        out_rel_cols.append(target_col)
    else:
        # need to re-compute target column's trust set to reflect min trust set across
        # all target column + all operand columns. same for pt
        all_cols = [c for c in operands if isinstance(c, Column)] + [target_col]
        min_trust_set = min_trust_with_from_cols(all_cols)
        min_pt_set = min_pt_set_from_cols(all_cols)
        target_col.trust_with = min_trust_set
        target_col.plaintext = min_pt_set

    out_rel = Relation(name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    return out_rel, target_col


def add(input_op_node: OpNode, name: str, target_col_name: str, operands: list):

    in_rel = input_op_node.out_rel
    operands = build_operands_from_in_rel(in_rel, operands)
    out_rel, target_col = _arithmetic_build_out_rel(in_rel, name, target_col_name, operands)
    op = Add(out_rel, input_op_node, target_col, operands)
    input_op_node.children.add(op)

    return op


def subtract(input_op_node: OpNode, name: str, target_col_name: str, operands: list):

    in_rel = input_op_node.out_rel
    operands = build_operands_from_in_rel(in_rel, operands)
    out_rel, target_col = _arithmetic_build_out_rel(in_rel, name, target_col_name, operands)
    op = Subtract(out_rel, input_op_node, target_col, operands)
    input_op_node.children.add(op)

    return op


def multiply(input_op_node: OpNode, name: str, target_col_name: str, operands: list):

    in_rel = input_op_node.out_rel
    operands = build_operands_from_in_rel(in_rel, operands)
    out_rel, target_col = _arithmetic_build_out_rel(in_rel, name, target_col_name, operands)
    op = Multiply(out_rel, input_op_node, target_col, operands)
    input_op_node.children.add(op)

    return op


def divide(input_op_node: OpNode, name: str, target_col_name: str, operands: list):

    in_rel = input_op_node.out_rel
    operands = build_operands_from_in_rel(in_rel, operands)
    out_rel, target_col = _arithmetic_build_out_rel(in_rel, name, target_col_name, operands)
    op = Divide(out_rel, input_op_node, target_col, operands)
    input_op_node.children.add(op)

    return op


def limit(input_op_node: OpNode, name: str, limit_num: int):

    in_rel = input_op_node.out_rel
    out_rel_cols = copy.deepcopy(in_rel.columns)

    out_rel = Relation(name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = Limit(out_rel, input_op_node, limit_num)
    input_op_node.children.add(op)

    return op


def distinct(input_op_node: OpNode, name: str, selected_col_names: [list, None] = None):

    in_rel = input_op_node.out_rel
    if selected_col_names is None:
        selected_cols = in_rel.columns
    else:
        selected_cols = [find(in_rel.columns, col_name) for col_name in selected_col_names]

    if not all([c is not None for c in selected_cols]):
        raise Exception(
            f"One of the following columns was not found in relation {in_rel.name}:\n{selected_col_names}"
        )

    out_rel_cols = copy.deepcopy(selected_cols)
    out_rel = Relation(name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = Distinct(out_rel, input_op_node, selected_cols)
    input_op_node.children.add(op)

    return op


def filter_by(input_op_node: OpNode, name: str, filter_col_name: str, operator: str, filter_against: [str, int]):

    in_rel = input_op_node.out_rel
    out_rel_cols = copy.deepcopy(in_rel.columns)

    out_rel = Relation(name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    filter_col = find(in_rel.columns, filter_col_name)
    if filter_col is None:
        raise Exception(f"Column {filter_col_name} not found in relation {in_rel.name}.")

    if isinstance(filter_against, str):
        against_col = find(in_rel.columns, filter_against)
        if against_col is None:
            raise Exception(f"Column {filter_against} not found in relation {in_rel.name}.")

        min_trust = min_trust_with_from_cols([filter_col, against_col])
        min_pt = min_pt_set_from_cols([filter_col, against_col])
        out_rel.columns[filter_col.idx].trust_with = min_trust
        out_rel.columns[filter_col.idx].plaintext = min_pt
        out_rel.columns[against_col.idx].trust_with = min_trust
        out_rel.columns[against_col.idx].plaintext = min_pt

        op = FilterAgainstCol(out_rel, input_op_node, copy.deepcopy(filter_col), operator, copy.deepcopy(against_col))
        input_op_node.children.add(op)

    elif isinstance(filter_against, int):
        op = FilterAgainstScalar(out_rel, input_op_node, copy.deepcopy(filter_col), operator, filter_against)
        input_op_node.children.add(op)

    else:
        raise Exception(
            f"Can't filter against value {filter_against}. Acceptable types are str and int."
        )

    return op


def sort_by(input_op_node: OpNode, name: str, sort_by_col_name: str, increasing: [bool, None] = True):

    in_rel = input_op_node.out_rel
    out_rel_cols = copy.deepcopy(in_rel.columns)
    sort_by_col = find(in_rel.columns, sort_by_col_name)

    if sort_by_col is None:
        raise Exception(f"Column {sort_by_col_name} not found in relation {in_rel.name}.")

    out_rel = Relation(name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = SortBy(out_rel, input_op_node, sort_by_col, increasing=increasing)
    input_op_node.children.add(op)

    return op


def num_rows(input_op_node: OpNode, name: str, count_col_name: str = "num_rows"):

    in_rel = input_op_node.out_rel
    cols_in_rel = copy.deepcopy(in_rel.columns)

    # max bc if you can know the content of any given column, you can know how many rows it has
    max_trust_set = max_trust_with_from_cols(cols_in_rel)
    max_pt_set = max_pt_set_from_cols(cols_in_rel)
    count_col = Column(name, count_col_name, len(in_rel.columns), "INTEGER", max_trust_set, max_pt_set)
    out_rel_col = [count_col]

    out_rel = Relation(name, out_rel_col, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = NumRows(out_rel, input_op_node, count_col_name)
    input_op_node.children.add(op)

    return op


def collect(input_op_node: OpNode, target_parties: set):

    in_rel = input_op_node.out_rel
    out_rel_cols = copy.deepcopy(in_rel.columns)

    for c in out_rel_cols:
        c.trust_with = c.trust_with.union(target_parties)
        c.plaintext = c.plaintext.union(target_parties)

    out_rel = Relation(f"{in_rel.name}_collect", out_rel_cols, [{p} for p in target_parties])
    out_rel.update_columns()
    op = Collect(out_rel, input_op_node)
    input_op_node.children.add(op)
