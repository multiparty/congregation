import socket
import random
from copy import deepcopy
from congregation.codegen.python.libs.utils import *


def store(rel: list, header: list, output_path: str):
    write_rel(output_path, rel, header)


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

    ret = deepcopy(rel)
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


def col_sum(rel: list):
    return [[sum(r) for r in zip(*rel)]]
