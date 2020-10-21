from congregation.lang import *
from congregation.dag import Dag
from congregation.dag.nodes.internal import *
from congregation.comp import compile_dag
from congregation.part import HeuristicPart
from tests.utils import create_cols, compare_partition_to_expected
import pytest


@pytest.mark.parametrize("party_data, expected", [
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1},
                "plaintext_sets": [{1}, {1}],
                "trust_with_sets": [{1}, {1}]
            },
            {
                "col_names": ["c", "d"],
                "stored_with": {1},
                "plaintext_sets": [{1}, {1}],
                "trust_with_sets": [{1}, {1}]
            }
        ],
        [
            {
                "node_order": [Create, Create, Join, AggregateMean, Divide, Collect],
                "backend": "python"
            }
        ]
    ),
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1},
                "plaintext_sets": [{1}, {1}],
                "trust_with_sets": [{1}, {1}]
            },
            {
                "col_names": ["c", "d"],
                "stored_with": {2},
                "plaintext_sets": [{2}, {2}],
                "trust_with_sets": [{2}, {2}]
            }
        ],
        [
            {
                "node_order": [Create, Store],
                "backend": "python"
            },
            {
                "node_order": [Create, Store],
                "backend": "python"
            },
            {
                "node_order": [Close, Close, Join, AggregateMean, Open, Store],
                "backend": "jiff"
            },
            {
                "node_order": [Read, Divide, Collect],
                "backend": "python"
            }
        ]
    ),
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            },
            {
                "col_names": ["c", "d"],
                "stored_with": {1, 2},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            }
        ],
        [
            {
                "node_order": [Create, Create, Join, AggregateMean, Open],
                "backend": "jiff"
            },
            {
                "node_order": [Read, Divide, Collect],
                "backend": "python"
            }
        ]
    )
])
def test_join(party_data, expected):

    cols_in_one = create_cols(party_data[0])
    cols_in_two = create_cols(party_data[1])

    rel_one = create("in1", cols_in_one, party_data[0]["stored_with"])
    rel_two = create("in2", cols_in_two, party_data[1]["stored_with"])

    j = join(rel_one, rel_two, "join", [party_data[0]["col_names"][0]], [party_data[1]["col_names"][0]])
    agg = aggregate(j, "agg", [party_data[0]["col_names"][0]], party_data[0]["col_names"][1], "mean")
    div = divide(agg, "div", party_data[0]["col_names"][1], [10])
    collect(div, {1, 2})

    d = Dag({rel_one, rel_two})
    compile_dag(d)
    p = HeuristicPart(d)
    parts = p.partition(100)

    compare_partition_to_expected(parts, expected)
