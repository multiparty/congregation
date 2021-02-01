from congregation.comp.push_down import PushDown
from congregation.comp.push_up import PushUp
from congregation.comp.insert_open_ops import InsertOpenOps
from congregation.comp.insert_close_ops import InsertCloseOps
from congregation.comp.insert_read_ops import InsertReadOps
from congregation.comp.insert_store_ops import InsertStoreOps
from congregation.dag import Dag


def compile_dag(d: Dag):

    steps = [
        PushDown(),
        PushUp(),
        InsertCloseOps(),
        InsertOpenOps(),
        InsertReadOps(),
        InsertStoreOps()
    ]
    for s in steps:
        s.rewrite(d)


def compile_dag_without_optimizations(d: Dag):

    steps = [
        InsertCloseOps(),
        InsertOpenOps(),
        InsertReadOps(),
        InsertStoreOps()
    ]
    for s in steps:
        s.rewrite(d)
