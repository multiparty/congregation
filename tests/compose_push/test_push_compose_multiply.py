from congregation.lang import *
from congregation.utils import create_column
from congregation.dag import Dag
from congregation.comp import PushDown, PushUp
import pytest


"""
Tests for correct propagation of the following relation-level
and column-level attributes after both the PushDown() and the 
PushUp() phases of the compiler have been run:
    - DAG node order
    - node.requires_mpc() attribute
    - relation-level stored_with sets
    - column-level plaintext sets
    - column-level trust_with sets
"""


def _create_cols(party_data):

    ret = []
    for i in range(len(party_data["col_names"])):
        ret.append(
            create_column(
                party_data["col_names"][i],
                "INTEGER",
                party_data["trust_with_sets"][i],
                party_data["plaintext_sets"][i]
            )
        )

    return ret


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
            "node_order": [Create, Create, Multiply, Multiply, Concat, Collect],
            "requires_mpc": [False, False, False, False, True, False],
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
                    "stored_with": [{1}, {2}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
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
            "node_order": [Create, Create, Concat, Multiply, Collect],
            "requires_mpc": [True, True, True, False, False],
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
def test_multiply(party_data, expected):

    cols_in_one = _create_cols(party_data[0])
    cols_in_two = _create_cols(party_data[1])

    rel_one = create("in1", cols_in_one, party_data[0]["stored_with"])
    rel_two = create("in2", cols_in_two, party_data[1]["stored_with"])

    cc = concat([rel_one, rel_two], "concat", party_data[0]["col_names"])
    p = multiply(cc, "mult", party_data[0]["col_names"][0], [party_data[0]["col_names"][1], 10])
    collect(p, {1, 2})

    d = Dag({rel_one, rel_two})
    pd = PushDown()
    pd.rewrite(d)
    pu = PushUp()
    pu.rewrite(d)

    zip_node_order = zip(d.top_sort(), expected["node_order"])
    node_order_checks = [isinstance(z[0], z[1]) for z in zip_node_order]
    assert all(node_order_checks)

    zip_requires_mpc = zip(d.top_sort(), expected["requires_mpc"])
    requires_mpc_checks = [z[0].requires_mpc() == z[1] for z in zip_requires_mpc]
    assert all(requires_mpc_checks)

    zip_stored_with = zip(d.top_sort(), [e["stored_with"] for e in expected["ownership_data"]])
    ownership_checks = [z[0].out_rel.stored_with == z[1] for z in zip_stored_with]
    assert all(ownership_checks)

    zip_plaintext = zip(d.top_sort(), [e["plaintext_sets"] for e in expected["ownership_data"]])
    plaintext_checks = [[c.plaintext for c in z[0].out_rel.columns] == z[1] for z in zip_plaintext]
    assert all(plaintext_checks)

    zip_trust_with = zip(d.top_sort(), [e["trust_with_sets"] for e in expected["ownership_data"]])
    trust_with_checks = [[c.trust_with for c in z[0].out_rel.columns] == z[1] for z in zip_trust_with]
    assert all(trust_with_checks)