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
            }
        ],
        [
            {
                "node_order": [Create, AggregateMean, Divide, Collect],
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
            }
        ],
        [
            {
                "node_order": [Create, AggregateMean, Open],
                "backend": "jiff"
            },
            {
                "node_order": [Read, Divide, Collect],
                "backend": "python"
            }
        ]
    )
])
def test_single_dataset(party_data, expected):

    cols_in_one = create_cols(party_data[0])
    rel_one = create("in1", cols_in_one, party_data[0]["stored_with"])
    agg = aggregate(rel_one, "agg", party_data[0]["col_names"][:1], party_data[0]["col_names"][1], "mean")
    div = divide(agg, "div", party_data[0]["col_names"][1], [10])
    collect(div, {1, 2})

    d = Dag({rel_one})
    compile_dag(d)
    p = HeuristicPart(d)
    parts = p.partition(100)

    compare_partition_to_expected(parts, expected)
