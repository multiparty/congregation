from congregation import *
import sys
import json


def protocol():

    a = create_column("a", "INTEGER")
    b = create_column("b", "INTEGER")
    c = create_column("c", "INTEGER")

    d = create_column("a", "INTEGER")
    e = create_column("b", "INTEGER")
    f = create_column("c", "INTEGER")

    g = create_column("a", "INTEGER")
    h = create_column("b", "INTEGER")
    i = create_column("c", "INTEGER")

    rel_one = create("in1", [a, b, c], {1})
    rel_two = create("in2", [d, e, f], {2})
    rel_three = create("in3", [g, h, i], {3})

    cc = concat([rel_one, rel_two, rel_three], "cc")
    agg = aggregate(cc, "agg", ["b"], "a", "sum")
    collect(agg, {1, 2, 3})

    return {rel_one, rel_two, rel_three}


cfg = json.loads(open(sys.argv[1], "r").read())
cfg["jiff"]["jiff_lib_path"] = sys.argv[2]

a = Assemble()
a.generate_and_dispatch(protocol, cfg)
