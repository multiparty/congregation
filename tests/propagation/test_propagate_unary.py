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
            }
        ],
        {
            "node_order": [Create, Collect],
            "requires_mpc": [True, False],
            "ownership_data": [
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
    ),
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{1, 2, 3}, set()],
                "trust_with_sets": [{1, 2, 3}, set()]
            }
        ],
        {
            "node_order": [Create, Collect],
            "requires_mpc": [True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2, 3}, set()],
                    "trust_with_sets": [{1, 2, 3}, set()]
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
                "plaintext_sets": [{1}, {2}],
                "trust_with_sets": [{1}, {2}]
            }
        ],
        {
            "node_order": [Create, Collect],
            "requires_mpc": [True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, {2}],
                    "trust_with_sets": [{1}, {2}]
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
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{1, 2}, {3}, {1}],
                "trust_with_sets": [{1, 2}, {3}, {1}]
            }
        ],
        {
            "node_order": [Create, Collect],
            "requires_mpc": [True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, {3}, {1}],
                    "trust_with_sets": [{1, 2}, {3}, {1}]
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
def test_create(party_data, expected):

    input_cols = create_cols(party_data[0])
    c = create("in1", input_cols, party_data[0]["stored_with"])
    collect(c, {1, 2, 3})

    d = Dag({c})
    compare_to_expected(d, expected)


@pytest.mark.parametrize("party_data, expected", [
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            }
        ],
        {
            "node_order": [Create, AggregateSum, Collect],
            "requires_mpc": [True, True, False],
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
                "plaintext_sets": [{1, 2, 3}, set()],
                "trust_with_sets": [{1, 2, 3}, set()]
            }
        ],
        {
            "node_order": [Create, AggregateSum, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2, 3}, set()],
                    "trust_with_sets": [{1, 2, 3}, set()]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2, 3}, set()],
                    "trust_with_sets": [{1, 2, 3}, set()]
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
                "plaintext_sets": [{1}, {2}],
                "trust_with_sets": [{1}, {2}]
            }
        ],
        {
            "node_order": [Create, AggregateSum, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, {2}],
                    "trust_with_sets": [{1}, {2}]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, set()],
                    "trust_with_sets": [{1}, set()]
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
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{1, 2}, {3}, {1}],
                "trust_with_sets": [{1, 2}, {3}, {1}]
            }
        ],
        {
            "node_order": [Create, AggregateSum, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, {3}, {1}],
                    "trust_with_sets": [{1, 2}, {3}, {1}]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, set()],
                    "trust_with_sets": [{1, 2}, set()]
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
def test_agg_simple(party_data, expected):

    input_cols = create_cols(party_data[0])
    c = create("in1", input_cols, party_data[0]["stored_with"])
    a = aggregate(c, "agg", [party_data[0]["col_names"][0]], party_data[0]["col_names"][1], "sum")
    collect(a, {1, 2, 3})

    d = Dag({c})
    compare_to_expected(d, expected)


@pytest.mark.parametrize("party_data, expected", [
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            }
        ],
        {
            "node_order": [Create, AggregateSum, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
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
                }
            ]
        }
    ),
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{1, 2, 3}, set()],
                "trust_with_sets": [{1, 2, 3}, set()]
            }
        ],
        {
            "node_order": [Create, AggregateSum, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2, 3}, set()],
                    "trust_with_sets": [{1, 2, 3}, set()]
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
                }
            ]
        }
    ),
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{1}, {2}],
                "trust_with_sets": [{1}, {2}]
            }
        ],
        {
            "node_order": [Create, AggregateSum, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, {2}],
                    "trust_with_sets": [{1}, {2}]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{2}],
                    "trust_with_sets": [{2}]
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
                "col_names": ["a", "b", "c"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{1, 2}, {3}, {1}],
                "trust_with_sets": [{1, 2}, {3}, {1}]
            }
        ],
        {
            "node_order": [Create, AggregateSum, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, {3}, {1}],
                    "trust_with_sets": [{1, 2}, {3}, {1}]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{3}],
                    "trust_with_sets": [{3}]
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
def test_agg_sum_no_group_cols(party_data, expected):

    input_cols = create_cols(party_data[0])
    c = create("in1", input_cols, party_data[0]["stored_with"])
    a = aggregate(c, "agg", [], party_data[0]["col_names"][1], "sum")
    collect(a, {1, 2, 3})

    d = Dag({c})
    compare_to_expected(d, expected)


@pytest.mark.parametrize("party_data, expected", [
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            }
        ],
        {
            "node_order": [Create, MinMaxMedian, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [set(), set()],
                    "trust_with_sets": [set(), set()]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [set(), set(), set(), set()],
                    "trust_with_sets": [set(), set(), set(), set()]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}],
                    "trust_with_sets": [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}]
                }
            ]
        }
    ),
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{1, 2, 3}, set()],
                "trust_with_sets": [{1, 2, 3}, set()]
            }
        ],
        {
            "node_order": [Create, MinMaxMedian, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2, 3}, set()],
                    "trust_with_sets": [{1, 2, 3}, set()]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2, 3}, set(), set(), set()],
                    "trust_with_sets": [{1, 2, 3}, set(), set(), set()]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}],
                    "trust_with_sets": [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}]
                }
            ]
        }
    ),
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{1}, {2}],
                "trust_with_sets": [{1}, {2}]
            }
        ],
        {
            "node_order": [Create, MinMaxMedian, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, {2}],
                    "trust_with_sets": [{1}, {2}]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, set(), set(), set()],
                    "trust_with_sets": [{1}, set(), set(), set()]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}],
                    "trust_with_sets": [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}]
                }
            ]
        }
    ),
    (
        [
            {
                "col_names": ["a", "b", "c"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{1, 2}, {3}, {1}],
                "trust_with_sets": [{1, 2}, {3}, {1}]
            }
        ],
        {
            "node_order": [Create, MinMaxMedian, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, {3}, {1}],
                    "trust_with_sets": [{1, 2}, {3}, {1}]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, set(), set(), set()],
                    "trust_with_sets": [{1, 2}, set(), set(), set()]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}],
                    "trust_with_sets": [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}]
                }
            ]
        }
    )
])
def test_mmm(party_data, expected):

    input_cols = create_cols(party_data[0])
    c = create("in1", input_cols, party_data[0]["stored_with"])
    a = min_max_median(c, "agg", [party_data[0]["col_names"][0]], party_data[0]["col_names"][1])
    collect(a, {1, 2, 3})

    d = Dag({c})
    compare_to_expected(d, expected)


@pytest.mark.parametrize("party_data, expected", [
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            }
        ],
        {
            "node_order": [Create, MinMaxMedian, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
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
                "plaintext_sets": [{1, 2, 3}, set()],
                "trust_with_sets": [{1, 2, 3}, set()]
            }
        ],
        {
            "node_order": [Create, MinMaxMedian, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2, 3}, set()],
                    "trust_with_sets": [{1, 2, 3}, set()]
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
                "plaintext_sets": [{1}, {2}],
                "trust_with_sets": [{1}, {2}]
            }
        ],
        {
            "node_order": [Create, MinMaxMedian, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, {2}],
                    "trust_with_sets": [{1}, {2}]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{2}, {2}, {2}],
                    "trust_with_sets": [{2}, {2}, {2}]
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
                "col_names": ["a", "b", "c"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{1, 2}, {3}, {1}],
                "trust_with_sets": [{1, 2}, {3}, {1}]
            }
        ],
        {
            "node_order": [Create, MinMaxMedian, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, {3}, {1}],
                    "trust_with_sets": [{1, 2}, {3}, {1}]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{3}, {3}, {3}],
                    "trust_with_sets": [{3}, {3}, {3}]
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
def test_mmm_no_group_col(party_data, expected):

    input_cols = create_cols(party_data[0])
    c = create("in1", input_cols, party_data[0]["stored_with"])
    a = min_max_median(c, "agg", [], party_data[0]["col_names"][1])
    collect(a, {1, 2, 3})

    d = Dag({c})
    compare_to_expected(d, expected)


@pytest.mark.parametrize("party_data, expected", [
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            }
        ],
        {
            "node_order": [Create, Project, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
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
                }
            ]
        }
    ),
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{1, 2, 3}, set()],
                "trust_with_sets": [{1, 2, 3}, set()]
            }
        ],
        {
            "node_order": [Create, Project, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2, 3}, set()],
                    "trust_with_sets": [{1, 2, 3}, set()]
                },
                {
                    "stored_with": [{1, 2, 3}],
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
                "plaintext_sets": [{1}, {2}],
                "trust_with_sets": [{1}, {2}]
            }
        ],
        {
            "node_order": [Create, Project, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, {2}],
                    "trust_with_sets": [{1}, {2}]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}],
                    "trust_with_sets": [{1}]
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
def test_project_simple(party_data, expected):

    input_cols = create_cols(party_data[0])
    c = create("in1", input_cols, party_data[0]["stored_with"])
    p = project(c, "proj", [party_data[0]["col_names"][0]])
    collect(p, {1, 2, 3})

    d = Dag({c})
    compare_to_expected(d, expected)


@pytest.mark.parametrize("party_data, expected", [
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            }
        ],
        {
            "node_order": [Create, Multiply, Collect],
            "requires_mpc": [True, True, False],
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
                "plaintext_sets": [{1, 2, 3}, set()],
                "trust_with_sets": [{1, 2, 3}, set()]
            }
        ],
        {
            "node_order": [Create, Multiply, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2, 3}, set()],
                    "trust_with_sets": [{1, 2, 3}, set()]
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
    ),
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{1}, {1, 2}],
                "trust_with_sets": [{1}, {1, 2}]
            }
        ],
        {
            "node_order": [Create, Multiply, Collect],
            "requires_mpc": [True, False, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, {1, 2}],
                    "trust_with_sets": [{1}, {1, 2}]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, {1, 2}],
                    "trust_with_sets": [{1}, {1, 2}]
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
def test_mult_simple(party_data, expected):

    input_cols = create_cols(party_data[0])
    c = create("in1", input_cols, party_data[0]["stored_with"])
    m = multiply(c, "mult", party_data[0]["col_names"][0], [3, party_data[0]["col_names"][1]])
    collect(m, {1, 2, 3})

    d = Dag({c})
    compare_to_expected(d, expected)


@pytest.mark.parametrize("party_data, expected", [
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            }
        ],
        {
            "node_order": [Create, Multiply, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
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
                "plaintext_sets": [{1}, set()],
                "trust_with_sets": [{1}, set()]
            }
        ],
        {
            "node_order": [Create, Multiply, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, set()],
                    "trust_with_sets": [{1}, set()]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, set(), set()],
                    "trust_with_sets": [{1}, set(), set()]
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
                "plaintext_sets": [{1}, {1, 2}],
                "trust_with_sets": [{1}, {1, 2}]
            }
        ],
        {
            "node_order": [Create, Multiply, Collect],
            "requires_mpc": [True, False, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, {1, 2}],
                    "trust_with_sets": [{1}, {1, 2}]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, {1, 2}, {1}],
                    "trust_with_sets": [{1}, {1, 2}, {1}]
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
                "col_names": ["a", "b", "c"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{1, 2}, {3}, {1}],
                "trust_with_sets": [{1, 2}, {3}, {1}]
            }
        ],
        {
            "node_order": [Create, Multiply, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, {3}, {1}],
                    "trust_with_sets": [{1, 2}, {3}, {1}]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, {3}, {1}, set()],
                    "trust_with_sets": [{1, 2}, {3}, {1}, set()]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}],
                    "trust_with_sets": [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}]
                }
            ]
        }
    ),
    (
        [
            {
                "col_names": ["a", "b", "c"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{1, 2}, {2, 3}, {1}],
                "trust_with_sets": [{1, 2}, {2, 3}, {1}]
            }
        ],
        {
            "node_order": [Create, Multiply, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, {2, 3}, {1}],
                    "trust_with_sets": [{1, 2}, {2, 3}, {1}]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, {2, 3}, {1}, {2}],
                    "trust_with_sets": [{1, 2}, {2, 3}, {1}, {2}]
                },
                {
                    "stored_with": [{1}, {2}, {3}],
                    "plaintext_sets": [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}],
                    "trust_with_sets": [{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}]
                }
            ]
        }
    )
])
def test_mult_new_col(party_data, expected):

    input_cols = create_cols(party_data[0])
    c = create("in1", input_cols, party_data[0]["stored_with"])
    m = multiply(c, "mult", "new_col", [party_data[0]["col_names"][0], party_data[0]["col_names"][1]])
    collect(m, {1, 2, 3})

    d = Dag({c})
    compare_to_expected(d, expected)


@pytest.mark.parametrize("party_data, expected", [
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            }
        ],
        {
            "node_order": [Create, Divide, Collect],
            "requires_mpc": [True, True, False],
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
                "plaintext_sets": [{1, 2, 3}, set()],
                "trust_with_sets": [{1, 2, 3}, set()]
            }
        ],
        {
            "node_order": [Create, Divide, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2, 3}, set()],
                    "trust_with_sets": [{1, 2, 3}, set()]
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
    ),
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{1}, {1, 2}],
                "trust_with_sets": [{1}, {1, 2}]
            }
        ],
        {
            "node_order": [Create, Divide, Collect],
            "requires_mpc": [True, False, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, {1, 2}],
                    "trust_with_sets": [{1}, {1, 2}]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, {1, 2}],
                    "trust_with_sets": [{1}, {1, 2}]
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
                "plaintext_sets": [{1, 2}, {3}],
                "trust_with_sets": [{1, 2}, {3}]
            }
        ],
        {
            "node_order": [Create, Divide, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, {3}],
                    "trust_with_sets": [{1, 2}, {3}]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [set(), {3}],
                    "trust_with_sets": [set(), {3}]
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
def test_div_simple(party_data, expected):

    input_cols = create_cols(party_data[0])
    c = create("in1", input_cols, party_data[0]["stored_with"])
    d = divide(c, "div", party_data[0]["col_names"][0], [3, party_data[0]["col_names"][1]])
    collect(d, {1, 2, 3})

    d = Dag({c})
    compare_to_expected(d, expected)


@pytest.mark.parametrize("party_data, expected", [
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            }
        ],
        {
            "node_order": [Create, Divide, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
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
                "plaintext_sets": [{1, 2, 3}, set()],
                "trust_with_sets": [{1, 2, 3}, set()]
            }
        ],
        {
            "node_order": [Create, Divide, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2, 3}, set()],
                    "trust_with_sets": [{1, 2, 3}, set()]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2, 3}, set(), set()],
                    "trust_with_sets": [{1, 2, 3}, set(), set()]
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
                "plaintext_sets": [{1, 2}, {1}],
                "trust_with_sets": [{1, 2}, {1}]
            }
        ],
        {
            "node_order": [Create, Divide, Collect],
            "requires_mpc": [True, False, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, {1}],
                    "trust_with_sets": [{1, 2}, {1}]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, {1}, {1}],
                    "trust_with_sets": [{1, 2}, {1}, {1}]
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
                "plaintext_sets": [{1, 2}, {3}],
                "trust_with_sets": [{1, 2}, {3}]
            }
        ],
        {
            "node_order": [Create, Divide, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, {3}],
                    "trust_with_sets": [{1, 2}, {3}]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, {3}, set()],
                    "trust_with_sets": [{1, 2}, {3}, set()]
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
def test_div_new_col(party_data, expected):

    input_cols = create_cols(party_data[0])
    c = create("in1", input_cols, {1, 2, 3})
    d = divide(c, "div", "new_col", [party_data[0]["col_names"][0], party_data[0]["col_names"][1]])
    collect(d, {1, 2, 3})

    d = Dag({c})
    compare_to_expected(d, expected)


@pytest.mark.parametrize("party_data, expected", [
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            }
        ],
        {
            "node_order": [Create, Limit, Collect],
            "requires_mpc": [True, True, False],
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
                "plaintext_sets": [{1, 2, 3}, set()],
                "trust_with_sets": [{1, 2, 3}, set()]
            }
        ],
        {
            "node_order": [Create, Limit, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2, 3}, set()],
                    "trust_with_sets": [{1, 2, 3}, set()]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2, 3}, set()],
                    "trust_with_sets": [{1, 2, 3}, set()]
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
def test_limit_simple(party_data, expected):

    input_cols = create_cols(party_data[0])
    c = create("in1", input_cols, party_data[0]["stored_with"])
    l = limit(c, "lim", 10)
    collect(l, {1, 2, 3})

    d = Dag({c})
    compare_to_expected(d, expected)


@pytest.mark.parametrize("party_data, expected", [
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            }
        ],
        {
            "node_order": [Create, Distinct, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
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
                }
            ]
        }
    ),
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{1}, set()],
                "trust_with_sets": [{1}, set()]
            }
        ],
        {
            "node_order": [Create, Distinct, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, set()],
                    "trust_with_sets": [{1}, set()]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}],
                    "trust_with_sets": [{1}]
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
def test_distinct_simple(party_data, expected):

    input_cols = create_cols(party_data[0])
    c = create("in1", input_cols, party_data[0]["stored_with"])
    dis = distinct(c, "dis", [party_data[0]["col_names"][0]])
    collect(dis, {1, 2, 3})

    d = Dag({c})
    compare_to_expected(d, expected)


@pytest.mark.parametrize("party_data, expected", [
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            }
        ],
        {
            "node_order": [Create, FilterAgainstCol, Collect],
            "requires_mpc": [True, True, False],
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
                "plaintext_sets": [{1}, set()],
                "trust_with_sets": [{1, 2}, set()]
            }
        ],
        {
            "node_order": [Create, FilterAgainstCol, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, set()],
                    "trust_with_sets": [{1, 2}, set()]
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
    ),
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [{1}, {1, 2}],
                "trust_with_sets": [{1, 2}, {1}]
            }
        ],
        {
            "node_order": [Create, FilterAgainstCol, Collect],
            "requires_mpc": [True, False, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1}, {1, 2}],
                    "trust_with_sets": [{1, 2}, {1}]
                },
                {
                    "stored_with": [{1, 2, 3}],
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
    )
])
def test_filter_by_col_simple(party_data, expected):

    input_cols = create_cols(party_data[0])
    c = create("in1", input_cols, party_data[0]["stored_with"])
    f = filter_by(c, "filt", party_data[0]["col_names"][0], "==", party_data[0]["col_names"][1])
    collect(f, {1, 2, 3})

    d = Dag({c})
    compare_to_expected(d, expected)


@pytest.mark.parametrize("party_data, expected", [
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            }
        ],
        {
            "node_order": [Create, FilterAgainstScalar, Collect],
            "requires_mpc": [True, True, False],
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
                "plaintext_sets": [{1, 2}, set()],
                "trust_with_sets": [{1, 2}, set()]
            }
        ],
        {
            "node_order": [Create, FilterAgainstScalar, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, set()],
                    "trust_with_sets": [{1, 2}, set()]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, set()],
                    "trust_with_sets": [{1, 2}, set()]
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
def test_filter_by_scalar_simple(party_data, expected):

    input_cols = create_cols(party_data[0])
    c = create("in1", input_cols, party_data[0]["stored_with"])
    f = filter_by(c, "filt", party_data[0]["col_names"][0], "==", 7)
    collect(f, {1, 2, 3})

    d = Dag({c})
    compare_to_expected(d, expected)


@pytest.mark.parametrize("party_data, expected", [
    (
        [
            {
                "col_names": ["a", "b"],
                "stored_with": {1, 2, 3},
                "plaintext_sets": [set(), set()],
                "trust_with_sets": [set(), set()]
            }
        ],
        {
            "node_order": [Create, SortBy, Collect],
            "requires_mpc": [True, True, False],
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
                "plaintext_sets": [{1, 2}, set()],
                "trust_with_sets": [{1, 2}, set()]
            }
        ],
        {
            "node_order": [Create, SortBy, Collect],
            "requires_mpc": [True, True, False],
            "ownership_data": [
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, set()],
                    "trust_with_sets": [{1, 2}, set()]
                },
                {
                    "stored_with": [{1, 2, 3}],
                    "plaintext_sets": [{1, 2}, set()],
                    "trust_with_sets": [{1, 2}, set()]
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
def test_sort_by_simple(party_data, expected):

    input_cols = create_cols(party_data[0])
    c = create("in1", input_cols, party_data[0]["stored_with"])
    s = sort_by(c, "sort", party_data[0]["col_names"][0])
    collect(s, {1, 2, 3})

    d = Dag({c})
    compare_to_expected(d, expected)
