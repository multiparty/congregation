import copy
from congregation.datasets import *
from congregation.dag.nodes.unary import *
from congregation.utils.col import *


def create(name: str, columns: list, stored_with: [set, list]):

    cols_in_rel = [
        Column(name, col_name, idx, type_str, trust_set, plaintext_set)
        for idx, (col_name, type_str, trust_set, plaintext_set) in enumerate(columns)
    ]

    if isinstance(stored_with, set):
        stored_with = [stored_with]
    out_rel = Relation(name, cols_in_rel, stored_with)
    op = Create(out_rel)

    return op


def aggregate(input_op_node: OpNode, name: str, group_col_names: list, agg_col_name: str,
              agg_type: str, agg_out_col_name: [str, None] = None):

    in_rel = input_op_node.out_rel
    in_cols = in_rel.columns
    group_cols = sorted([find(in_cols, group_col_name) for group_col_name in group_col_names], key=lambda c: c.idx)
    agg_col = find(in_cols, agg_col_name)

    agg_out_col = copy.deepcopy(agg_col)
    if agg_out_col_name is not None:
        agg_out_col.name = agg_out_col_name

    out_rel_cols = [copy.deepcopy(group_col) for group_col in group_cols] + [copy.deepcopy(agg_out_col)]
    min_pt = min_pt_set_from_cols(out_rel_cols)
    min_trust = min_trust_with_from_columns(out_rel_cols)
    for col in out_rel_cols:
        col.plaintext = min_pt
        col.min_trust = min_trust

    out_rel = Relation(name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    if agg_type == "sum":
        op = AggregateSum(out_rel, input_op_node, group_cols, agg_col)
    elif agg_type == "mean":
        op = AggregateMean(out_rel, input_op_node, group_cols, agg_col)
    elif agg_type == "std_dev":
        op = AggregateStdDev(out_rel, input_op_node, group_cols, agg_col)
    else:
        raise Exception(
            f"Aggregate type {agg_type} not recognized. Must be of the following: [sum, mean, std_dev]."
        )

    input_op_node.children.add(op)
    return op


def aggregate_count(input_op_node: OpNode, name: str, group_col_names: list, count_col_name: [str, None] = "count"):

    in_rel = input_op_node.out_rel
    in_cols = in_rel.columns
    group_cols = sorted([find(in_cols, group_col_name) for group_col_name in group_col_names], key=lambda c: c.idx)
    count_col = Column(name, count_col_name, len(group_cols), "INTEGER", set(), set())

    min_trust = min_trust_with_from_columns(group_cols)
    min_pt = min_pt_set_from_cols(group_cols)
    out_rel_cols = [copy.deepcopy(group_col) for group_col in group_cols] + [count_col]

    for col in out_rel_cols:
        col.plaintext = min_pt
        col.trust_with = min_trust

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


def multiply(input_op_node: OpNode, name: str, target_col_name: str, operands: list):

    in_rel = input_op_node.out_rel
    out_rel_cols = copy.deepcopy(in_rel.columns)
    operands = build_operands_from_in_rel(in_rel, operands)

    target_col = find(out_rel_cols, target_col_name)
    if target_col is None:

        cols_only = [col for col in operands if isinstance(col, Column)]
        target_col_trust_set = min_trust_with_from_columns(cols_only)
        pt = min_pt_set_from_cols(cols_only)
        col_type = infer_output_type(cols_only)

        target_col = Column(name, target_col_name, len(in_rel.columns), col_type, target_col_trust_set, plaintext=pt)
        out_rel_cols.append(target_col)
    else:
        # need to re-compute target column's trust set to reflect min trust set across
        # all target column + all operand columns. same for pt
        all_cols = [col for col in operands if isinstance(col, Column)] + [target_col]
        target_col_trust_set = min_trust_with_from_columns(all_cols)
        pt = min_pt_set_from_cols(all_cols)
        target_col.trust_with = target_col_trust_set
        target_col.plaintext = pt

    out_rel = Relation(name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = Multiply(out_rel, input_op_node, target_col, operands)
    input_op_node.children.add(op)

    return op


def divide(input_op_node: OpNode, name: str, target_col_name: str, operands: list):

    in_rel = input_op_node.out_rel
    out_rel_cols = copy.deepcopy(in_rel.columns)
    operands = build_operands_from_in_rel(in_rel, operands)

    target_col = find(out_rel_cols, target_col_name)
    if target_col is None:

        cols_only = [col for col in operands if isinstance(col, Column)]
        target_col_trust_set = min_trust_with_from_columns(cols_only)
        pt = min_pt_set_from_cols(cols_only)
        col_type = infer_output_type(cols_only)

        target_col = Column(name, target_col_name, len(in_rel.columns), col_type, target_col_trust_set, plaintext=pt)
        out_rel_cols.append(target_col)

    else:

        # need to re-compute target column's trust set to reflect min trust set across
        # all target column + all operand columns. same for pt
        all_cols = [col for col in operands if isinstance(col, Column)] + [target_col]
        target_col_trust_set = min_trust_with_from_columns(all_cols)
        pt = min_pt_set_from_cols(all_cols)
        target_col.trust_with = target_col_trust_set
        target_col.plaintext = pt

    out_rel = Relation(name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

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


def distinct(input_op_node: OpNode, name: str, selected_col_names: list):

    in_rel = input_op_node.out_rel
    selected_cols = [find(in_rel.columns, col_name) for col_name in selected_col_names]

    if not all([col is not None for col in selected_cols]):
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

        op = FilterAgainstCol(out_rel, input_op_node, filter_col, operator, against_col)
        input_op_node.children.add(op)

    elif isinstance(filter_against, int):
        op = FilterAgainstScalar(out_rel, input_op_node, filter_col, operator, filter_against)
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

    min_trust_set = min_trust_with_from_columns(cols_in_rel)
    min_pt_set = min_pt_set_from_cols(cols_in_rel)
    count_col = Column(name, count_col_name, len(in_rel.columns), "INTEGER", min_trust_set, min_pt_set)
    out_rel_col = [count_col]

    out_rel = Relation(name, out_rel_col, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = NumRows(out_rel, input_op_node, count_col_name)
    input_op_node.children.add(op)

    return op


def collect(input_op_node: OpNode, target_parties: set):

    in_rel = input_op_node.out_rel
    out_rel_cols = copy.deepcopy(in_rel.columns)

    for col in out_rel_cols:
        col.trust_with = col.trust_with.union(target_parties)
        col.plaintext = col.plaintext.union(target_parties)

    out_rel = Relation(f"{in_rel.name}->collect", out_rel_cols, [set([p]) for p in target_parties])
    out_rel.update_columns()
    op = Collect(out_rel, input_op_node)
    input_op_node.children.add(op)
