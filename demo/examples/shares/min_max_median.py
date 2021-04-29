from congregation import create_column, lang, Assemble
import sys
import json


def protocol():

    # 3 columns for input dataset (shared between parties {1, 2, 3}
    a = create_column("a", "INTEGER")
    b = create_column("b", "INTEGER")
    c = create_column("c", "INTEGER")

    # create shared input relation
    rel_one = lang.create("inpt", [a, b, c], {1, 2, 3})

    # compute deciles of values in column "a"
    mmm = lang.min_max_median(rel_one, "deciles", [], "a")

    # reveal output to parties 1, 2, and 3
    lang.collect(mmm, {1, 2, 3})

    # return the workflow's root node
    return {rel_one}


cfg = json.loads(open(sys.argv[1], "r").read())
cfg["jiff"]["jiff_lib_path"] = sys.argv[2]

# the Assemble class (at congregation/assemble/assemble.py) exposes
# all the main steps related to workflow generation and dispatching
a = Assemble()
a.generate_and_dispatch(protocol, cfg)
