from congregation.lang import *
from congregation.dag import Dag
from congregation.utils import create_column
import pytest


def _create_cols(col_names, trust_with_sets):

    ret = []
    for i in range(len(col_names)):
        ret.append(create_column(col_names[i], "INTEGER", trust_with_sets[i], set()))

    return ret


@pytest.mark.parametrize("col_names, trust_with_sets, expected_sets", [
    (
            ["a", "b"],
            [set(), set()],
            [[set(), set()], [set(), set()], [set(), set()]]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, set()],
            [[{1, 2, 3}, set()], [{1, 2, 3}, set()], [{1, 2, 3}, set()]]
    )
])
def test_aggregate_single_rel(col_names, trust_with_sets, expected_sets):

    input_cols = _create_cols(col_names, trust_with_sets)
    c = create("a", input_cols, {1, 2, 3})
    a = aggregate(c, "agg", [col_names[0]], col_names[1], "sum")
    collect(a, {1, 2, 3})
    d = Dag({c})

    sorted_nodes = d.top_sort()
    for i in range(len(sorted_nodes)):
        node = sorted_nodes[i]

        if i == 0:
            cols = node.out_rel.columns
            for j in range(len(cols)):
                assert cols[j].trust_with == expected_sets[i][j]
        else:
            in_rel_cols = node.get_in_rel().columns
            out_rel_cols = node.out_rel.columns

            for j in range(len(in_rel_cols)):
                assert in_rel_cols[j].trust_with == expected_sets[i-1][j]

            for k in range(len(out_rel_cols)):
                assert out_rel_cols[k].trust_with == expected_sets[i-1][k]


