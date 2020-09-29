import math
from typing import List


def write_rel(output_dir: str, rel_name: str, rel: list, header: list):

    print(f"Writing python job output to {output_dir}/{rel_name}")
    with open(f"{output_dir}{rel_name}.csv", "w") as f:
        f.write(f"{','.join(header)}\n")
        rows_formatted = [",".join(str(v) for v in row) for row in rel]
        f.write("\n".join(r for r in rows_formatted))


def read_rel(path_to_rel: str):

    rows = []
    with open(path_to_rel, "r") as f:
        itr = iter(f.readlines())
        for row in itr:
            try:
                # TODO: not necessarily ints we're working with
                rows.append([int(v) for v in row.split(",")])
            except ValueError:
                # skip header row
                pass
    return rows


def create(path_to_rel: str):
    return read_rel(path_to_rel)


def aggregate_count(rel: list, group_cols_idx: list):

    acc = {}
    for row in rel:
        k = tuple(row[idx] for idx in group_cols_idx)
        if k in acc:
            acc[k] += 1
        else:
            acc[k] = 1

    ret = []
    for k in acc.keys():
        ret.append(list(k) + [acc[k]])

    return ret


def aggregate_sum(rel: list, group_cols_idx: list, agg_col: int):

    acc = {}
    for row in rel:
        k = tuple(row[idx] for idx in group_cols_idx)
        if k in acc:
            acc[k] += row[agg_col]
        else:
            acc[k] = row[agg_col]

    ret = []
    for k in acc.keys():
        ret.append(list(k) + [acc[k]])

    return ret


def aggregate_mean(rel: list, group_cols_idx: list, agg_col: int):

    acc = {}
    for row in rel:
        k = tuple(row[idx] for idx in group_cols_idx)
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


def aggregate_std_dev(rel: list, group_cols_idx: list, agg_col: int):
    # TODO
    return []


def project(rel: list, selected_cols: list):
    return [[row[idx] for idx in selected_cols] for row in rel]


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
        target_col_result = col_product * scalar_product * row[target_col_idx]
        ret.append([row[i] if i != target_col_idx else target_col_result for i in range(len(row))])
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


def _divide_list(l: list):

    if len(l) == 0:
        return 1
    elif len(l) == 1:
        return l[0]
    else:
        ret = l[0]
        for i in l[1:]:
            ret = ret / i
        return ret


def _divide_new_col(rel: list, operands: List[dict]):

    ret = []
    for row in rel:
        vals = [row[o["v"]] if o["__TYPE__"] == "col" else o["v"] for o in operands]
        div_result = _divide_list(vals)
        ret.append(row + [div_result])
    return ret


def _divide(rel: list, operands: List[dict], target_col_idx: int):

    ret = []
    for row in rel:
        vals = [row[o["v"]] if o["__TYPE__"] == "col" else o["v"] for o in operands]
        target_col_result = _divide_list([row[target_col_idx]] + vals)
        ret.append([row[i] if i != target_col_idx else target_col_result for i in range(len(row))])
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


a = [[1,2,3], [4,5,6], [1,2,3], [4,5,6], [1,2,1]]
# b = aggregate_count(a, [1,2])
# b = aggregate_sum(a, [0, 1], 2)
# b = aggregate_mean(a, [0,1], 2)
# b = multiply(a, [1, 2], [1], 0)
# b = limit(a, 3)
b = distinct(a, [0, 1])

# c = [[100, 20, 4], [200, 5, 2]]
# o = [
#     {
#         "v": 1,
#         "__TYPE__": "col"
#     },
#     {
#         "v": 10,
#         "__TYPE__": "val"
#     },
#     {
#         "v": 2,
#         "__TYPE__": "col"
#     }
# ]
# b = divide(c, o, 3)

print(b)

"""
EXTERNAL UNARY:
AggregateStdDev
FilterAgainstCol
FilterAgainstScalar
SortBy
NumRows
"""