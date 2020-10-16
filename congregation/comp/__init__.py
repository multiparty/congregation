from congregation.comp.push_down import PushDown
from congregation.comp.push_up import PushUp
from congregation.comp.insert_open_ops import InsertOpenOps
from congregation.comp.insert_close_ops import InsertCloseOps
from congregation.comp.insert_read_ops import InsertReadOps
from congregation.comp.insert_store_ops import InsertStoreOps
from congregation.dag import Dag


def compile_dag(d: Dag):

    pd = PushDown()
    pu = PushUp()
    ic = InsertCloseOps()
    io = InsertOpenOps()
    ir = InsertReadOps()
    iso = InsertStoreOps()
    pd.rewrite(d)
    pu.rewrite(d)
    ic.rewrite(d)
    io.rewrite(d)
    ir.rewrite(d)
    iso.rewrite(d)
