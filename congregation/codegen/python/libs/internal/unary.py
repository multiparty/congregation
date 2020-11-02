from congregation.codegen.python.libs.utils import *
import socket
import random
import math
import copy


def store(rel: list, header: list, output_path: str):
    write_rel(output_path, rel, header)


def read(path_to_rel: str, use_floats: [bool, None] = False):
    return read_rel(path_to_rel, use_floats=use_floats)


def persist(rel: list, header: list, output_path: str):
    write_rel(output_path, rel, header)


def send(rel: list, sock: socket):
    """
    TODO
    """
    return


def index(rel: list):

    idxs = [i for i in range(len(rel))]
    zipped = zip(rel, idxs)
    return [i[0] + [i[1]] for i in zipped]


def shuffle(rel: list):

    ret = copy.deepcopy(rel)
    # TODO: temporarily using python random lib, fix later
    random.shuffle(ret)
    return ret


def aggregate_sum_count_col(rel: list, group_cols: list, agg_col: int):

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
        ret.append(list(k) + [acc[k]["__SUM__"], acc[k]["__COUNT__"]])

    return ret


def aggregate_sum_squares_and_count(rel: list, group_cols: list, agg_col: int):

    acc = {}
    for row in rel:
        k = tuple(row[idx] for idx in group_cols)
        if k in acc:
            acc[k]["__SUM__"] += row[agg_col]
            acc[k]["__SQUARES__"] += math.pow(row[agg_col], 2)
            acc[k]["__COUNT__"] += 1
        else:
            acc[k] = {}
            acc[k]["__SUM__"] = row[agg_col]
            acc[k]["__SQUARES__"] = math.pow(row[agg_col], 2)
            acc[k]["__COUNT__"] = 1

    ret = []
    for k in acc.keys():
        ret.append(list(k) + [acc[k]["__SUM__"], acc[k]["__SQUARES__"], acc[k]["__COUNT__"]])

    return ret


def aggregate_std_dev_local_sqrt(rel: list):

    ret = copy.deepcopy(rel)
    for row in ret:
        row[-1] = math.sqrt(row[-1])

    return ret


def col_sum(rel: list):
    return [[sum(r) for r in zip(*rel)]]
