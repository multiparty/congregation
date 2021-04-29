from congregation import create_column, lang, Assemble
import sys
import json


def protocol():

    # 3 columns for party 1
    a = create_column("a", "INTEGER")
    b = create_column("b", "INTEGER")
    c = create_column("c", "INTEGER")

    # 3 columns for party 2
    d = create_column("d", "INTEGER")
    e = create_column("e", "INTEGER")
    f = create_column("f", "INTEGER")

    # 3 columns for party 3
    g = create_column("g", "INTEGER")
    h = create_column("h", "INTEGER")
    i = create_column("i", "INTEGER")

    # create all input relations
    rel_one = lang.create("in1", [a, b, c], {1})
    rel_two = lang.create("in2", [d, e, f], {2})
    rel_three = lang.create("in3", [g, h, i], {3})

    # concatenate input relations
    cc = lang.concat([rel_one, rel_two, rel_three], "cc")

    # compute deciles of values in column "a"
    agg = lang.deciles(cc, "dec_out", [], "a")

    # collect output
    lang.collect(agg, {1, 2, 3})

    # return the workflow's root nodes
    return {rel_one, rel_two, rel_three}


cfg = json.loads(open(sys.argv[1], "r").read())
a = Assemble()
a.generate_and_dispatch(protocol, cfg)
