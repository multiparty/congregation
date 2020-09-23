from congregation.lang import *
from congregation.dag import Dag
from tests.utils import create_cols, compare_to_expected
import pytest


"""
Tests for correct propagation of the following relation-level
and column-level attributes prior to any work being done by
the compiler:
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
            },
            {
                "col_names": ["e", "f"],
                "stored_with": {3},
                "plaintext_sets": [{3}, {3}],
                "trust_with_sets": [{3}, {3}]
            }
        ],
        {
            "node_order": [Create, Create, Create, Concat, Collect],
            "requires_mpc": [False, False, False, True, False],
            "ownership_data": [
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
                    "stored_with": [{3}],
                    "plaintext_sets": [{3}, {3}],
                    "trust_with_sets": [{3}, {3}]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1, 2, 3}, {1, 2, 3}],
                    "trust_with_sets": [{1, 2, 3}, {1, 2, 3}]
                }
            ]
        }
    ),
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1},
                "plaintext_sets": [{1, 2}, {1}],
                "trust_with_sets": [{1, 2}, {1}]
            },
            {
                "col_names": ["c", "d"],
                "stored_with": {2},
                "plaintext_sets": [{2, 3}, {2}],
                "trust_with_sets": [{2}, {2}]
            },
            {
                "col_names": ["e", "f"],
                "stored_with": {3},
                "plaintext_sets": [{3}, {3}],
                "trust_with_sets": [{3}, {3}]
            }
        ],
        {
            "node_order": [Create, Create, Create, Concat, Collect],
            "requires_mpc": [False, False, False, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1}],
                    "plaintext_sets": [{1, 2}, {1}],
                    "trust_with_sets": [{1, 2}, {1}]
                },
                {
                    "stored_with": [{2}],
                    "plaintext_sets": [{2, 3}, {2}],
                    "trust_with_sets": [{2}, {2}]
                },
                {
                    "stored_with": [{3}],
                    "plaintext_sets": [{3}, {3}],
                    "trust_with_sets": [{3}, {3}]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1, 2, 3}, {1, 2, 3}],
                    "trust_with_sets": [{1, 2, 3}, {1, 2, 3}]
                }
            ]
        }
    ),
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            },
            {
                "col_names": ["c", "d"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            },
            {
                "col_names": ["e", "f"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            }
        ],
        {
            "node_order": [Create, Create, Create, Concat, Collect],
            "requires_mpc": [True, True, True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1, 2, 3}, {1, 2, 3}],
                    "trust_with_sets": [{1, 2, 3}, {1, 2, 3}]
                }
            ]
        }
    )
])
def test_concat_simple(party_data, expected):

    cols_in_one = create_cols(party_data[0])
    cols_in_two = create_cols(party_data[1])
    cols_in_three = create_cols(party_data[2])

    rel_one = create("in1", cols_in_one, party_data[0]["stored_with"])
    rel_two = create("in2", cols_in_two, party_data[1]["stored_with"])
    rel_three = create("in3", cols_in_three, party_data[2]["stored_with"])

    cc = concat([rel_one, rel_two, rel_three], "concat", party_data[0]["col_names"])
    collect(cc, {1, 2, 3})

    d = Dag({rel_one, rel_two, rel_three})
    compare_to_expected(d, expected)
