import math
from typing import List
from copy import deepcopy
from congregation.codegen.python.libs.utils import *


def create(path_to_rel: str, use_floats: [bool, None] = False):
    return read_rel(path_to_rel, use_floats=use_floats)


def aggregate_count(rel: list, group_cols: list):

    acc = {}
    for row in rel:
        k = tuple(row[idx] for idx in group_cols)
        if k in acc:
            acc[k] += 1
        else:
            acc[k] = 1

    ret = []
    for k in acc.keys():
        ret.append(list(k) + [acc[k]])

    return ret


def aggregate_sum(rel: list, group_cols: list, agg_col: int):

    acc = {}
    for row in rel:
        k = tuple(row[idx] for idx in group_cols)
        if k in acc:
            acc[k] += row[agg_col]
        else:
            acc[k] = row[agg_col]

    ret = []
    for k in acc.keys():
        ret.append(list(k) + [acc[k]])

    return ret


def aggregate_mean(rel: list, group_cols: list, agg_col: int):

    acc = {}
    for row in rel:
        k = tuple(row[idx] for idx in group_cols)
        if k in acc:
            acc[k]["__SUM__"] += row[agg_col]
            acc[k]["__COUNT__"] += 1
        else:
            acc[k] = {}
            acc[k]["__SUM__"] = row[agg_col]
            acc[k]["__COUNT__"] = 1

    ret = []
    for k in acc.keys():
        m = acc[k]["__SUM__"] / acc[k]["__COUNT__"]
        ret.append(list(k) + [m])

    return ret


def aggregate_variance(rel: list, group_cols: list, agg_col: int):

    acc = {}
    for row in rel:
        k = tuple(row[idx] for idx in group_cols)
        if k in acc:
            acc[k]["__VALUES__"].append(row[agg_col])
            acc[k]["__SUM__"] += row[agg_col]
            acc[k]["__COUNT__"] += 1
        else:
            acc[k] = {}
            acc[k]["__VALUES__"] = [row[agg_col]]
            acc[k]["__SUM__"] = row[agg_col]
            acc[k]["__COUNT__"] = 1

    ret = []
    for k in acc.keys():

        count = acc[k]["__COUNT__"]
        m = acc[k]["__SUM__"] / acc[k]["__COUNT__"]
        squared_mean = math.pow(m, 2)
        sum_squares = sum([math.pow(v, 2) for v in acc[k]["__VALUES__"]])
        sum_squares_mean = sum_squares / count
        std_dev = sum_squares_mean - squared_mean
        ret.append(list(k) + [std_dev])

    return ret


def aggregate_std_dev(rel: list, group_cols: list, agg_col: int):

    variance_rel = aggregate_variance(rel, group_cols, agg_col)

    ret = []
    for row in variance_rel:
        v = math.sqrt(row[-1])
        ret.append(row[:-1] + [v])

    return ret


def _min_max_median_with_group_cols(acc: dict):

    ret = []
    for k in acc.keys():
        acc[k].sort()
        middle_idx = int(len(acc[k]) / 2)
        mmm = [acc[k][0], acc[k][-1], acc[k][middle_idx]]
        ret.append(list(k) + mmm)
    return ret


def min_max_median(rel: list, group_cols: list, agg_col: int):

    if group_cols:
        acc = {}
        for row in rel:
            k = tuple(row[idx] for idx in group_cols)
            if k in acc:
                acc[k].append(row[agg_col])
            else:
                acc[k] = [row[agg_col]]
        return _min_max_median_with_group_cols(acc)
    else:
        rel_copy = deepcopy(rel)
        rel_copy.sort(key=lambda r: r[agg_col])
        middle_idx = int(len(rel_copy) / 2)
        return [rel_copy[0][agg_col], rel_copy[-1][agg_col], rel_copy[middle_idx][agg_col]]


def _get_deciles_from_group(values: list):
    """
    Note: this method assumes the input list is already sorted
    """

    ret = []
    deciles = [.1, .2, .3, .4, .5, .6, .7, .8, .9]

    for d in deciles:
        d_idx = int(len(values) * d)
        ret.append(values[d_idx])

    return ret


def _decile_with_group_cols(acc: dict):

    ret = []
    for k in acc.keys():
        acc[k].sort()
        dcs = _get_deciles_from_group(acc[k])
        ret.append(list(k) + dcs)

    return ret


def deciles(rel: list, group_cols: list, agg_col: int):

    if group_cols:
        acc = {}
        for row in rel:
            k = tuple(row[idx] for idx in group_cols)
            if k in acc:
                acc[k].append(row[agg_col])
            else:
                acc[k] = [row[agg_col]]
        return _decile_with_group_cols(acc)
    else:
        ac = [r[agg_col] for r in rel]
        rel_copy = deepcopy(ac)
        rel_copy.sort()
        return [_get_deciles_from_group(rel_copy)]


def project(rel: list, selected_cols: list):
    return [[row[idx] for idx in selected_cols] for row in rel]


def _add_new_col(rel: list, col_operands: list, scalar_operands: list):

    ret = []
    for row in rel:
        col_sum = sum([row[i] for i in col_operands])
        scalar_sum = sum(scalar_operands)
        ret.append(row + [col_sum + scalar_sum])
    return ret


def _add(rel: list, col_operands: list, scalar_operands: list, target_col_idx: int):

    ret = []
    for row in rel:
        col_sum = sum([row[i] for i in col_operands])
        scalar_sum = sum(scalar_operands)
        ret.append(
            [
                row[i]
                if i != target_col_idx
                else sum([col_sum, scalar_sum, row[target_col_idx]])
                for i in range(len(row))
            ]
        )
    return ret


def add(rel: list, col_operands: list, scalar_operands: list, target_col_idx: int):

    if target_col_idx > len(rel[0]):
        raise Exception(
            f"Input relation has only {len(rel[0])} columns. "
            f"Can't add column with idx {target_col_idx}."
        )

    if len(rel[0]) == target_col_idx:
        return _add_new_col(rel, col_operands, scalar_operands)
    else:
        return _add(rel, col_operands, scalar_operands, target_col_idx)


def _sub_list(li: list):

    if len(li) == 0:
        return 0

    ret = li[0]
    for i in li[1:]:
        ret = ret - i
    return ret


def _subtract_new_col(rel: list, operands: List[dict]):

    ret = []
    for row in rel:
        vals = [row[o["v"]] if o["__TYPE__"] == "col" else o["v"] for o in operands]
        sub_result = _sub_list(vals)
        ret.append(row + [sub_result])
    return ret


def _subtract(rel: list, operands: List[dict], target_col_idx: int):

    ret = []
    for row in rel:
        vals = [row[o["v"]] if o["__TYPE__"] == "col" else o["v"] for o in operands]
        ret.append(
            [
                row[i]
                if i != target_col_idx
                else _sub_list([row[target_col_idx]] + vals)
                for i in range(len(row))
            ]
        )
    return ret


def subtract(rel: list, operands: List[dict], target_col_idx: int):

    if target_col_idx > len(rel[0]):
        raise Exception(
            f"Input relation has only {len(rel[0])} columns. "
            f"Can't add column with idx {target_col_idx}."
        )

    if len(rel[0]) == target_col_idx:
        return _subtract_new_col(rel, operands)
    else:
        return _subtract(rel, operands, target_col_idx)


def _multiply_new_col(rel: list, col_operands: list, scalar_operands: list):

    ret = []
    for row in rel:
        col_product = math.prod([row[i] for i in col_operands])
        scalar_product = math.prod(scalar_operands)
        ret.append(row + [col_product * scalar_product])
    return ret


def _multiply(rel: list, col_operands: list, scalar_operands: list, target_col_idx: int):

    ret = []
    for row in rel:
        col_product = math.prod([row[i] for i in col_operands])
        scalar_product = math.prod(scalar_operands)
        ret.append(
            [
                row[i]
                if i != target_col_idx
                else math.prod([col_product, scalar_product, row[target_col_idx]])
                for i in range(len(row))
            ]
        )
    return ret


def multiply(rel: list, col_operands: list, scalar_operands: list, target_col_idx: int):

    if target_col_idx > len(rel[0]):
        raise Exception(
            f"Input relation has only {len(rel[0])} columns. "
            f"Can't add column with idx {target_col_idx}."
        )

    if len(rel[0]) == target_col_idx:
        return _multiply_new_col(rel, col_operands, scalar_operands)
    else:
        return _multiply(rel, col_operands, scalar_operands, target_col_idx)


def _divide_list(li: list):

    if len(li) == 0:
        return 0

    ret = li[0]
    for i in li[1:]:
        ret = ret / i
    return ret


def _divide_new_col(rel: list, operands: List[dict]):

    ret = []
    for row in rel:
        vals = [
            row[o["v"]]
            if o["__TYPE__"] == "col"
            else o["v"] for o in operands
        ]
        div_result = _divide_list(vals)
        ret.append(row + [div_result])
    return ret


def _divide(rel: list, operands: List[dict], target_col_idx: int):

    ret = []
    for row in rel:
        vals = [
            row[o["v"]]
            if o["__TYPE__"] == "col"
            else o["v"]
            for o in operands
        ]
        ret.append(
            [
                row[i]
                if i != target_col_idx
                else _divide_list([row[target_col_idx]] + vals)
                for i in range(len(row))
            ]
        )
    return ret


def divide(rel: list, operands: List[dict], target_col_idx: int):

    if target_col_idx > len(rel[0]):
        raise Exception(
            f"Input relation has only {len(rel[0])} columns. "
            f"Can't add column with idx {target_col_idx}."
        )

    if len(rel[0]) == target_col_idx:
        return _divide_new_col(rel, operands)
    else:
        return _divide(rel, operands, target_col_idx)


def limit(rel: list, n: int):
    return rel[:n]


def distinct(rel: list, selected_cols: list):

    d = set()
    ret = []

    for row in rel:
        dist_keys = ",".join([str(row[i]) for i in selected_cols])
        if dist_keys not in d:
            d.add(dist_keys)
            ret.append([row[i] for i in selected_cols])
    return ret


def _filter_against_col_lt(rel: list, filter_col: int, against_col: int):

    ret = []
    for row in rel:
        if row[filter_col] < row[against_col]:
            ret.append(row)
    return ret


def _filter_against_col_gt(rel: list, filter_col: int, against_col: int):

    ret = []
    for row in rel:
        if row[filter_col] > row[against_col]:
            ret.append(row)
    return ret


def _filter_against_col_eq(rel: list, filter_col: int, against_col: int):

    ret = []
    for row in rel:
        if row[filter_col] == row[against_col]:
            ret.append(row)
    return ret


def filter_against_col(rel: list, filter_col: int, against_col: int, operator: str):

    if operator == "<":
        return _filter_against_col_lt(rel, filter_col, against_col)
    elif operator == ">":
        return _filter_against_col_gt(rel, filter_col, against_col)
    elif operator == "==":
        return _filter_against_col_eq(rel, filter_col, against_col)
    else:
        raise Exception(f"Unknown operator: {operator}")


def _filter_against_scalar_lt(rel: list, filter_col: int, scalar: [int, float]):

    ret = []
    for row in rel:
        if row[filter_col] < scalar:
            ret.append(row)
    return ret


def _filter_against_scalar_gt(rel: list, filter_col: int, scalar: [int, float]):

    ret = []
    for row in rel:
        if row[filter_col] > scalar:
            ret.append(row)
    return ret


def _filter_against_scalar_eq(rel: list, filter_col: int, scalar: [int, float]):

    ret = []
    for row in rel:
        if row[filter_col] == scalar:
            ret.append(row)
    return ret


def filter_against_scalar(rel: list, filter_col: int, scalar: [int, float], operator: str):

    if operator == "<":
        return _filter_against_scalar_lt(rel, filter_col, scalar)
    elif operator == ">":
        return _filter_against_scalar_gt(rel, filter_col, scalar)
    elif operator == "==":
        return _filter_against_scalar_eq(rel, filter_col, scalar)
    else:
        raise Exception(f"Unknown operator: {operator}")


def sort_by(rel: list, sort_by_col: int, increasing: bool = True):

    ret = deepcopy(rel)
    if increasing:
        ret.sort(key=lambda r: r[sort_by_col])
    else:
        ret.sort(key=lambda r: r[sort_by_col], reverse=True)
    return ret


def num_rows(rel: list):
    return [[len(rel)]]


def collect(rel: list, header: list, output_path: str):
    write_rel(output_path, rel, header)
