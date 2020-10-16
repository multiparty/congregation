from congregation.lang import *
from congregation.dag import Dag
from congregation.dag.nodes.internal import Close, Open
from congregation.comp import InsertCloseOps, InsertOpenOps
from tests.utils import create_cols, compare_to_expected
import pytest


"""
Tests for correct propagation of the following relation-level
and column-level attributes after both the InsertCloseOps() 
and the InsertOpenOps() phases of the compiler have been run:
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
        {
            "node_order": [Create, Close, Create, Close, Concat, Open, Collect],
            "requires_mpc": [False, True, False, True, True, True, False],
            "ownership_data":[
                {
                    "stored_with": [{1}],
                    "plaintext_sets": [{1}, {1}],
                    "trust_with_sets": [{1}, {1}]
                },
                {
                    "stored_with": [{1}, {2}],
                    "plaintext_sets": [{1}, {1}],
                    "trust_with_sets": [{1}, {1}]
                },
                {
                    "stored_with": [{2}],
                    "plaintext_sets": [{2}, {2}],
                    "trust_with_sets": [{2}, {2}]
                },
                {
                    "stored_with": [{1}, {2}],
                    "plaintext_sets": [{2}, {2}],
                    "trust_with_sets": [{2}, {2}]
                },
                {
                    "stored_with": [{1}, {2}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
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
        {
            "node_order": [Create, Create, Concat, Open, Collect],
            "requires_mpc": [True, True, True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1, 2}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1, 2}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
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
def test_concat(party_data, expected):

    cols_in_one = create_cols(party_data[0])
    cols_in_two = create_cols(party_data[1])

    rel_one = create("in1", cols_in_one, party_data[0]["stored_with"])
    rel_two = create("in2", cols_in_two, party_data[1]["stored_with"])

    cc = concat([rel_one, rel_two], "concat", party_data[0]["col_names"])
    collect(cc, {1, 2})

    d = Dag({rel_one, rel_two})
    ic = InsertCloseOps()
    ic.rewrite(d)
    io = InsertOpenOps()
    io.rewrite(d)

    compare_to_expected(d, expected)


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
                "stored_with": {2},
                "plaintext_sets": [{2}, {2}],
                "trust_with_sets": [{2}, {2}]
            }
        ],
        {
            "node_order":
                [Create, Create, Divide, Close, Multiply, Close, Concat, AggregateSum, Project, Open, Collect],
            "requires_mpc": [False, False, False, True, False, True, True, True, True, True, False],
            "ownership_data":[
                {
                    "stored_with": [{1}],
                    "plaintext_sets": [{1}, {1}],
                    "trust_with_sets": [{1}, {1}]
                },
                {
                    "stored_with": [{2}],
                    "plaintext_sets": [{2}, {2}],
                    "trust_with_sets": [{2}, {2}]
                },
                {
                    "stored_with": [{2}],
                    "plaintext_sets": [{2}, {2}],
                    "trust_with_sets": [{2}, {2}]
                },
                {
                    "stored_with": [{1}, {2}],
                    "plaintext_sets": [{2}, {2}],
                    "trust_with_sets": [{2}, {2}]
                },
                {
                    "stored_with": [{1}],
                    "plaintext_sets": [{1}, {1}],
                    "trust_with_sets": [{1}, {1}]
                },
                {
                    "stored_with": [{1}, {2}],
                    "plaintext_sets": [{1}, {1}],
                    "trust_with_sets": [{1}, {1}]
                },
                {
                    "stored_with": [{1}, {2}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1}, {2}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1}, {2}],
                    "plaintext_sets": [set()],
                    "trust_with_sets": [set()]
                },
                {
                    "stored_with": [{1}, {2}],
                    "plaintext_sets": [{1, 2}],
                    "trust_with_sets": [{1, 2}]
                },
                {
                    "stored_with": [{1}, {2}],
                    "plaintext_sets": [{1, 2}],
                    "trust_with_sets": [{1, 2}]
                }
            ]
        }
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
        {
            "node_order": [Create, Create, Divide, Multiply, Concat, AggregateSum, Project, Open, Collect],
            "requires_mpc": [True, True, True, True, True, True, True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1, 2}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1, 2}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1, 2}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1, 2}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1, 2}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1, 2}],
                    "plaintext_sets": [set()],
                    "trust_with_sets": [set()]
                },
                {
                    "stored_with": [{1}, {2}],
                    "plaintext_sets": [{1, 2}],
                    "trust_with_sets": [{1, 2}]
                },
                {
                    "stored_with": [{1}, {2}],
                    "plaintext_sets": [{1, 2}],
                    "trust_with_sets": [{1, 2}]
                }
            ]
        }
    )
])
def test_concat_composite(party_data, expected):

    cols_in_one = create_cols(party_data[0])
    cols_in_two = create_cols(party_data[1])

    rel_one = create("in1", cols_in_one, party_data[0]["stored_with"])
    rel_two = create("in2", cols_in_two, party_data[1]["stored_with"])

    mult_one = multiply(rel_one, "mult_one", party_data[0]["col_names"][0], [party_data[0]["col_names"][1], 5])
    div_two = divide(rel_two, "div_two", party_data[1]["col_names"][0], [party_data[1]["col_names"][1], 5])
    cc = concat([mult_one, div_two], "concat", party_data[0]["col_names"])
    agg = aggregate(cc, "agg", [party_data[0]["col_names"][0]], party_data[0]["col_names"][1], "sum")
    p = project(agg, "proj", [party_data[0]["col_names"][0]])
    collect(p, {1, 2})

    d = Dag({rel_one, rel_two})
    ic = InsertCloseOps()
    ic.rewrite(d)
    io = InsertOpenOps()
    io.rewrite(d)

    compare_to_expected(d, expected)
