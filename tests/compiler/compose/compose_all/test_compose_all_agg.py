from congregation.lang import *
from congregation.dag import Dag
from congregation.dag.nodes.internal import Close, Open, AggregateSumCountCol
from congregation.comp import PushDown, PushUp, InsertCloseOps, InsertOpenOps
from tests.utils import create_cols, compare_to_expected
import pytest


"""
Tests for correct propagation of the following relation-level
and column-level attributes after the Pushdown, PushUp, InsertCloseOps, 
and InsertOpenOps phases of the compiler have been run:
    - DAG node order
    - node.requires_mpc() attribute
    - relation-level stored_with sets
    - column-level plaintext sets
    - column-level trust_with sets
"""


@pytest.mark.parametrize("party_data, expected", [
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            }
        ],
        {
            "node_order": [Create, AggregateMean, Open, Divide, Collect],
            "requires_mpc": [True, True, True, False, False],
            "ownership_data":[
                {
                    "stored_with": [{1, 2}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1, 2}],
                    "plaintext_sets": [{1, 2}, {1, 2}],
                    "trust_with_sets": [{1, 2}, {1, 2}]
                },
                {
                    "stored_with": [{1}, {2}],
                    "plaintext_sets": [{1, 2}, {1, 2}],
                    "trust_with_sets": [{1, 2}, {1, 2}]
                },
                {
                    "stored_with": [{1}, {2}],
                    "plaintext_sets": [{1, 2}, {1, 2}],
                    "trust_with_sets": [{1, 2}, {1, 2}]
                },
                {
                    "stored_with": [{1}, {2}],
                    "plaintext_sets": [{1, 2}, {1, 2}],
                    "trust_with_sets": [{1, 2}, {1, 2}]
                }
            ]
        }
    )
])
def test_agg(party_data, expected):

    cols_in_one = create_cols(party_data[0])
    rel_one = create("in1", cols_in_one, party_data[0]["stored_with"])
    agg = aggregate(rel_one, "agg", party_data[0]["col_names"][:1], party_data[0]["col_names"][1], "mean")
    div = divide(agg, "div", party_data[0]["col_names"][1], [10])
    collect(div, {1, 2})

    d = Dag({rel_one})
    pd = PushDown()
    pd.rewrite(d)
    pu = PushUp()
    pu.rewrite(d)
    ic = InsertCloseOps()
    ic.rewrite(d)
    io = InsertOpenOps()
    io.rewrite(d)

    compare_to_expected(d, expected)
