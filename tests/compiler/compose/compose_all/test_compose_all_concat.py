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
            "requires_mpc": [False, True, False, True, True, False, False],
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
    pd = PushDown()
    pd.rewrite(d)
    pu = PushUp()
    pu.rewrite(d)
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
            "node_order": [
                Create,
                Create,
                Multiply,
                AggregateSum,
                Close,
                Multiply,
                AggregateSum,
                Close,
                Concat,
                AggregateSum,
                Project,
                Open,
                Collect
            ],
            "requires_mpc": [False, False, False, False, True, False, False, True, True, True, True, False, False],
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
            "node_order": [Create, Create, Concat, Multiply, AggregateSum, Project, Open, Collect],
            "requires_mpc": [True, True, True, True, True, True, False, False],
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

    cc = concat([rel_one, rel_two], "concat", party_data[0]["col_names"])
    mult = multiply(cc, "mult", party_data[0]["col_names"][0], [party_data[0]["col_names"][1], 5])
    agg = aggregate(mult, "agg", [party_data[0]["col_names"][0]], party_data[0]["col_names"][1], "sum")
    p = project(agg, "proj", [party_data[0]["col_names"][0]])
    collect(p, {1, 2})

    d = Dag({rel_one, rel_two})
    pd = PushDown()
    pd.rewrite(d)
    pu = PushUp()
    pu.rewrite(d)
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
            },
            {
                "col_names": ["e", "f"],
                "stored_with": {3},
                "plaintext_sets": [{3}, {3}],
                "trust_with_sets": [{3}, {3}]
            }
        ],
        {
            "node_order": [
                Create, Create, Create,
                Multiply, AggregateSum, Close,
                Multiply, AggregateSum, Close,
                Multiply, AggregateSum, Close,
                Concat, AggregateSum, Project,
                Open, Collect
            ],
            "requires_mpc": [
                False, False, False,
                False, False, True,
                False, False, True,
                False, False, True,
                True, True, True,
                False, False
            ],
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
                    "stored_with": [{3}],
                    "plaintext_sets": [{3}, {3}],
                    "trust_with_sets": [{3}, {3}]
                },
                {
                    "stored_with": [{1}],
                    "plaintext_sets": [{1}, {1}],
                    "trust_with_sets": [{1}, {1}]
                },
                {
                    "stored_with": [{1}],
                    "plaintext_sets": [{1}, {1}],
                    "trust_with_sets": [{1}, {1}]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
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
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{2}, {2}],
                    "trust_with_sets": [{2}, {2}]
                },
                {
                    "stored_with": [{3}],
                    "plaintext_sets": [{3}, {3}],
                    "trust_with_sets": [{3}, {3}]
                },
                {
                    "stored_with": [{3}],
                    "plaintext_sets": [{3}, {3}],
                    "trust_with_sets": [{3}, {3}]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
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
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [set()],
                    "trust_with_sets": [set()]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1, 2, 3}],
                    "trust_with_sets": [{1, 2, 3}]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1, 2, 3}],
                    "trust_with_sets": [{1, 2, 3}]
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
            "node_order": [
                Create, Create, Create,
                Concat, Multiply, AggregateSum,
                Project, Open, Collect
            ],
            "requires_mpc": [
                True, True, True,
                True, True, True,
                True, False, False
            ],
            "ownership_data":[
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
                    "plaintext_sets": [set()],
                    "trust_with_sets": [set()]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1, 2, 3}],
                    "trust_with_sets": [{1, 2, 3}]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1, 2, 3}],
                    "trust_with_sets": [{1, 2, 3}]
                }
            ]
        }
    )
])
def test_concat_composite_three_party_agg_sum(party_data, expected):

    cols_in_one = create_cols(party_data[0])
    cols_in_two = create_cols(party_data[1])
    cols_in_three = create_cols(party_data[2])

    rel_one = create("in1", cols_in_one, party_data[0]["stored_with"])
    rel_two = create("in2", cols_in_two, party_data[1]["stored_with"])
    rel_three = create("in3", cols_in_three, party_data[2]["stored_with"])

    cc = concat([rel_one, rel_two, rel_three], "concat", party_data[0]["col_names"])
    mult = multiply(cc, "mult", party_data[0]["col_names"][0], [party_data[0]["col_names"][1], 5])
    agg = aggregate(mult, "agg", [party_data[0]["col_names"][0]], party_data[0]["col_names"][1], "sum")
    p = project(agg, "proj", [party_data[0]["col_names"][0]])
    collect(p, {1, 2, 3})

    d = Dag({rel_one, rel_two, rel_three})
    pd = PushDown()
    pd.rewrite(d)
    pu = PushUp()
    pu.rewrite(d)
    ic = InsertCloseOps()
    ic.rewrite(d)
    io = InsertOpenOps()
    io.rewrite(d)

    f = d.top_sort()

    compare_to_expected(d, expected)


@pytest.mark.parametrize("party_data, expected", [
    (
        [
            {
                "col_names": ["a", "b", "c"],
                "stored_with": {1},
                "plaintext_sets": [{1}, {1}, {1}],
                "trust_with_sets": [{1}, {1}, {1}]
            },
            {
                "col_names": ["d", "e", "f"],
                "stored_with": {2},
                "plaintext_sets": [{1, 2}, {1, 2}, {2}],
                "trust_with_sets": [{1, 2}, {1, 2}, {2}]
            },
            {
                "col_names": ["g", "h", "i"],
                "stored_with": {3},
                "plaintext_sets": [{1, 3}, {1, 3}, {3}],
                "trust_with_sets": [{1, 3}, {1, 3}, {3}]
            }
        ],
        {
            "node_order": [
                Create, Project, Create, Project, Create, Project, Concat, Multiply, AggregateMean, Collect
            ],
            "requires_mpc": [
                False, False, False, False, False, False, False, False, False, False
            ],
            "ownership_data":[
                {
                    "stored_with": [{1}],
                    "plaintext_sets": [{1}, {1}, {1}],
                    "trust_with_sets": [{1}, {1}, {1}]
                },
                {
                    "stored_with": [{1}],
                    "plaintext_sets": [{1}, {1}],
                    "trust_with_sets": [{1}, {1}]
                },
                {
                    "stored_with": [{2}],
                    "plaintext_sets": [{1, 2}, {1, 2}, {2}],
                    "trust_with_sets": [{1, 2}, {1, 2}, {2}]
                },
                {
                    "stored_with": [{2}],
                    "plaintext_sets": [{1, 2}, {1, 2}],
                    "trust_with_sets": [{1, 2}, {1, 2}]
                },
                {
                    "stored_with": [{3}],
                    "plaintext_sets": [{1, 3}, {1, 3}, {3}],
                    "trust_with_sets": [{1, 3}, {1, 3}, {3}]
                },
                {
                    "stored_with": [{3}],
                    "plaintext_sets": [{1, 3}, {1, 3}],
                    "trust_with_sets": [{1, 3}, {1, 3}]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1}, {1}],
                    "trust_with_sets": [{1}, {1}]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1}, {1}],
                    "trust_with_sets": [{1}, {1}]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1}, {1}],
                    "trust_with_sets": [{1}, {1}]
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
                "col_names": ["a", "b", "c"],
                "stored_with": {1},
                "plaintext_sets": [{1}, {1}, {1}],
                "trust_with_sets": [{1}, {1}, {1}]
            },
            {
                "col_names": ["d", "e", "f"],
                "stored_with": {2},
                "plaintext_sets": [{2}, {2}, {2}],
                "trust_with_sets": [{2}, {2}, {2}]
            },
            {
                "col_names": ["g", "h", "i"],
                "stored_with": {3},
                "plaintext_sets": [{3}, {3}, {3}],
                "trust_with_sets": [{3}, {3}, {3}]
            }
        ],
        {
            "node_order": [
                Create, Project,
                Create, Project,
                Create, Project,
                Multiply, AggregateSumCountCol, Close,
                Multiply, AggregateSumCountCol, Close,
                Multiply, AggregateSumCountCol, Close,
                Concat, AggregateMean, Open, Collect
            ],
            "requires_mpc": [
                False, False, False,
                False, False, False,
                False, False, True,
                False, False, True,
                False, False, True,
                True, True, False, False
            ],
            "ownership_data": [
                {
                    "stored_with": [{1}],
                    "plaintext_sets": [{1}, {1}, {1}],
                    "trust_with_sets": [{1}, {1}, {1}]
                },
                {
                    "stored_with": [{1}],
                    "plaintext_sets": [{1}, {1}],
                    "trust_with_sets": [{1}, {1}]
                },
                {
                    "stored_with": [{2}],
                    "plaintext_sets": [{2}, {2}, {2}],
                    "trust_with_sets": [{2}, {2}, {2}]
                },
                {
                    "stored_with": [{2}],
                    "plaintext_sets": [{2}, {2}],
                    "trust_with_sets": [{2}, {2}]
                },
                {
                    "stored_with": [{3}],
                    "plaintext_sets": [{3}, {3}, {3}],
                    "trust_with_sets": [{3}, {3}, {3}]
                },
                {
                    "stored_with": [{3}],
                    "plaintext_sets": [{3}, {3}],
                    "trust_with_sets": [{3}, {3}]
                },
                {
                    "stored_with": [{1}],
                    "plaintext_sets": [{1}, {1}],
                    "trust_with_sets": [{1}, {1}]
                },
                {
                    "stored_with": [{1}],
                    "plaintext_sets": [{1}, {1}, {1}],
                    "trust_with_sets": [{1}, {1}, {1}]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1}, {1}, {1}],
                    "trust_with_sets": [{1}, {1}, {1}]
                },
                {
                    "stored_with": [{2}],
                    "plaintext_sets": [{2}, {2}],
                    "trust_with_sets": [{2}, {2}]
                },
                {
                    "stored_with": [{2}],
                    "plaintext_sets": [{2}, {2}, {2}],
                    "trust_with_sets": [{2}, {2}, {2}]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{2}, {2}, {2}],
                    "trust_with_sets": [{2}, {2}, {2}]
                },
                {
                    "stored_with": [{3}],
                    "plaintext_sets": [{3}, {3}],
                    "trust_with_sets": [{3}, {3}]
                },
                {
                    "stored_with": [{3}],
                    "plaintext_sets": [{3}, {3}, {3}],
                    "trust_with_sets": [{3}, {3}, {3}]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{3}, {3}, {3}],
                    "trust_with_sets": [{3}, {3}, {3}]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [set(), set(), set()],
                    "trust_with_sets": [set(), set(), set()]
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
def test_concat_composite_three_party_agg_mean(party_data, expected):

    cols_in_one = create_cols(party_data[0])
    cols_in_two = create_cols(party_data[1])
    cols_in_three = create_cols(party_data[2])

    rel_one = create("in1", cols_in_one, party_data[0]["stored_with"])
    rel_two = create("in2", cols_in_two, party_data[1]["stored_with"])
    rel_three = create("in3", cols_in_three, party_data[2]["stored_with"])

    cc = concat([rel_one, rel_two, rel_three], "concat", party_data[0]["col_names"])
    p = project(cc, "conc", party_data[0]["col_names"][:2])
    mult = multiply(p, "mult", party_data[0]["col_names"][0], [5])
    agg = aggregate(mult, "agg", [party_data[0]["col_names"][0]], party_data[0]["col_names"][1], "mean")
    collect(agg, {1, 2, 3})

    d = Dag({rel_one, rel_two, rel_three})
    pd = PushDown()
    pd.rewrite(d)
    pu = PushUp()
    pu.rewrite(d)
    ic = InsertCloseOps()
    ic.rewrite(d)
    io = InsertOpenOps()
    io.rewrite(d)

    compare_to_expected(d, expected)
