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
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            },
            {
                "col_names": ["c", "d"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            }
        ],
        {
            "node_order": [Create, Create, Join, Collect],
            "requires_mpc": [True, True, True, False],
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
                    "plaintext_sets": [set(), set(), set()],
                    "trust_with_sets": [set(), set(), set()]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}],
                    "trust_with_sets": [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}]
                }
            ]
        }
    ),
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{1, 2}, set()],
                "trust_with_sets": [{1, 2}, set()]
            },
            {
                "col_names": ["c", "d"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{3}, {2}],
                "trust_with_sets": [{3}, {2}]
            }
        ],
        {
            "node_order": [Create, Create, Join, Collect],
            "requires_mpc": [True, True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, set()],
                    "trust_with_sets": [{1, 2}, set()]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{3}, {2}],
                    "trust_with_sets": [{3}, {2}]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [set(), set(), {2}],
                    "trust_with_sets": [set(), set(), {2}]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}],
                    "trust_with_sets": [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}]
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
                "trust_with_sets": [{1}, {1}]
            },
            {
                "col_names": ["c", "d"],
                "stored_with": {2},
                "plaintext_sets": [{2}, {2}],
                "trust_with_sets": [{1, 2}, {2}]
            }
        ],
        {
            "node_order": [Create, Create, Join, Collect],
            "requires_mpc": [False, False, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1}],
                    "plaintext_sets": [{1, 2}, {1}],
                    "trust_with_sets": [{1}, {1}]
                },
                {
                    "stored_with": [{2}],
                    "plaintext_sets": [{2}, {2}],
                    "trust_with_sets": [{1, 2}, {2}]
                },
                {
                    "stored_with": [{1}, {2}],
                    "plaintext_sets": [{2}, {1}, {2}],
                    "trust_with_sets": [{1}, {1}, {2}]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}],
                    "trust_with_sets": [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}]
                }
            ]
        }
    )
])
def test_join_simple(party_data, expected):

    cols_in_one = create_cols(party_data[0])
    cols_in_two = create_cols(party_data[1])

    rel_one = create("in1", cols_in_one, party_data[0]["stored_with"])
    rel_two = create("in2", cols_in_two, party_data[1]["stored_with"])

    j = join(rel_one, rel_two, "join", [rel_one.out_rel.columns[0].name], [rel_two.out_rel.columns[0].name])
    collect(j, {1, 2, 3})

    d = Dag({rel_one, rel_two})
    compare_to_expected(d, expected)
