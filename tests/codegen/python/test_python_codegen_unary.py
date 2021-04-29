from congregation.codegen.python.libs import *
import pytest
import os


"""
Tests for correct relation calculation under the congregation python module
"""


inputs_path = f"{os.path.dirname(os.path.realpath(__file__))}/inputs"


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
        f"{inputs_path}/rel_one.csv",
        False,
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    ),
    (
        f"{inputs_path}/rel_one.csv",
        True,
        [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]
    ),
    (
        f"{inputs_path}/rel_two.csv",
        False,
        [[1, 3, 6], [7, 1, 8]]
    ),
    (
        f"{inputs_path}/rel_two.csv",
        True,
        [[1.4, 3.2, 6.7], [7.0, 1.6, 8.9]]
    ),
    (
        f"{inputs_path}/rel_invalid.csv",
        False,
        [[2, 3, 4]]

    ),
    (
        f"{inputs_path}/rel_invalid.csv",
        True,
        [[2.0, 3.0, 4.0]]
    )
])
def test_create(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    assert r == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
        f"{inputs_path}/rel_three.csv",
        False,
        [[1, 2, 2], [4, 5, 2], [1, 6, 1], [2, 4, 1], [2, 8, 1]]
    ),
    (
        f"{inputs_path}/rel_three.csv",
        True,
        [[1.0, 2.0, 2], [4.0, 5.0, 2], [1.0, 6.0, 1], [2.0, 4.0, 1], [2.0, 8.0, 1]]
    ),
    (
        f"{inputs_path}/rel_four.csv",
        False,
        [[1, 2, 3], [4, 5, 2], [2, 4, 1], [2, 8, 1]]
    ),
    (
        f"{inputs_path}/rel_four.csv",
        True,
        [[1.1, 2.3, 2], [4.5, 5.4, 2], [1.1, 2.6, 1], [2.1, 4.3, 1], [2.1, 8.8, 1]]
    )
])
def test_aggregate_count(path_to_rel, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    acount = aggregate_count(r, [0, 1])

    assert acount == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
        f"{inputs_path}/rel_three.csv",
        False,
        [[1, 2, 6], [4, 5, 10], [1, 6, 7], [2, 4, 8], [2, 8, 9]]
    ),
    (
        f"{inputs_path}/rel_three.csv",
        True,
        [[1.0, 2.0, 6.0], [4.0, 5.0, 10.0], [1.0, 6.0, 7.0], [2.0, 4.0, 8.0], [2.0, 8.0, 9.0]]
    ),
    (
        f"{inputs_path}/rel_four.csv",
        False,
        [[1, 2, 10], [4, 5, 13], [2, 4, 8], [2, 8, 9]]
    ),
    (
        f"{inputs_path}/rel_four.csv",
        True,
        [[1.1, 2.3, 7.6], [4.5, 5.4, 13.8], [1.1, 2.6, 3.7], [2.1, 4.3, 8.1], [2.1, 8.8, 9.6]]
    )
])
def test_aggregate_sum(path_to_rel, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    asum = aggregate_sum(r, [0, 1], 2)

    assert asum == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
        f"{inputs_path}/rel_three.csv",
        False,
        [[1, 2, 3.0], [4, 5, 5.0], [1, 6, 7.0], [2, 4, 8.0], [2, 8, 9.0]]
    ),
    (
        f"{inputs_path}/rel_three.csv",
        True,
        [[1.0, 2.0, 3.0], [4.0, 5.0, 5.0], [1.0, 6.0, 7.0], [2.0, 4.0, 8.0], [2.0, 8.0, 9.0]]
    ),
    (
        f"{inputs_path}/rel_four.csv",
        False,
        [[1, 2, 3.3333333333333335], [4, 5, 6.5], [2, 4, 8.0], [2, 8, 9.0]]
    ),
    (
        f"{inputs_path}/rel_four.csv",
        True,
        [[1.1, 2.3, 3.8], [4.5, 5.4, 6.9], [1.1, 2.6, 3.7], [2.1, 4.3, 8.1], [2.1, 8.8, 9.6]]
    )
])
def test_aggregate_mean(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    amean = aggregate_mean(r, [0, 1], 2)

    assert amean == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
        f"{inputs_path}/rel_three.csv",
        False,
        [[1, 2, 0], [4, 5, 1], [1, 6, 0], [2, 4, 0], [2, 8, 0]]
    ),
    (
        f"{inputs_path}/rel_three.csv",
        True,
        [[1.0, 2.0, 0], [4.0, 5.0, 1], [1.0, 6.0, 0], [2.0, 4.0, 0], [2.0, 8.0, 0]]
    ),
    (
        f"{inputs_path}/rel_four.csv",
        False,
        [[1, 2, 0.47140452079103085], [4, 5, 0.5], [2, 4, 0.0], [2, 8, 0.0]]
    ),
    (
        f"{inputs_path}/rel_four.csv",
        True,
        [
            [1.1, 2.3, 0.20000000000000231],
            [4.5, 5.4, 0.8999999999999974],
            [1.1, 2.6, 0.0],
            [2.1, 4.3, 0.0],
            [2.1, 8.8, 0.0]
        ]
    )
])
def test_aggregate_std_dev(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    std_dev = aggregate_std_dev(r, [0, 1], 2)

    assert std_dev == expected


@pytest.mark.parametrize("path_to_rel, group_cols, expected", [
    (
            f"{inputs_path}/rel_three.csv",
            [0, 1],
            [
                [1, 2, 3, 3, 3],
                [4, 5, 4, 6, 6],
                [1, 6, 7, 7, 7],
                [2, 4, 8, 8, 8],
                [2, 8, 9, 9, 9]
            ]
    ),
    (
            f"{inputs_path}/rel_three.csv",
            None,
            [3, 9, 6]
    )
])
def test_aggregate_min_max_median(path_to_rel: str, group_cols: [list, None], expected: list):

    r = create(path_to_rel)
    mmm = min_max_median(r, group_cols, 2)

    assert mmm == expected


@pytest.mark.parametrize("path_to_rel, group_cols, expected", [
    (
        f"{inputs_path}/rel_seven.csv",
        [0, 1],
        [
            [2, 3, 1, 1, 1, 2, 3, 3, 6, 7, 10],
            [4, 5, 1, 2, 6, 6, 7, 7, 8, 9, 9]
        ]
    ),
    (
        f"{inputs_path}/rel_seven.csv",
        None,
        [
            [1, 1, 2, 3, 6, 7, 7, 9, 9]
        ]
    ),
    (
        f"{inputs_path}/rel_six.csv",
        None,
        [
            [52, 52, 52, 52, 83, 83, 83, 83, 83]
        ]
    )
])
def test_deciles(path_to_rel: str, group_cols: [list, None], expected: list):

    r = create(path_to_rel)
    dec = deciles(r, group_cols, 2)

    assert dec == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
        f"{inputs_path}/rel_one.csv",
        False,
        [[3, 2], [6, 5], [9, 8]]
    ),
    (
        f"{inputs_path}/rel_one.csv",
        True,
        [[3.0, 2.0], [6.0, 5.0], [9.0, 8.0]]
    ),
    (
        f"{inputs_path}/rel_two.csv",
        False,
        [[6, 3], [8, 1]]
    ),
    (
        f"{inputs_path}/rel_two.csv",
        True,
        [[6.7, 3.2], [8.9, 1.6]]
    ),
    (
        f"{inputs_path}/rel_invalid.csv",
        False,
        [[4, 3]]

    ),
    (
        f"{inputs_path}/rel_invalid.csv",
        True,
        [[4, 3]]
    )
])
def test_project(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    proj = project(r, [2, 1])

    assert proj == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
            f"{inputs_path}/rel_one.csv",
            False,
            [[1, 2, 10], [4, 5, 19], [7, 8, 28]]
    ),
    (
            f"{inputs_path}/rel_two.csv",
            False,
            [[1, 3, 14], [7, 1, 20]]
    ),
    (
            f"{inputs_path}/rel_two.csv",
            True,
            [[1.4, 3.2, 15.3], [7.0, 1.6, 21.5]]
    ),
    (
            f"{inputs_path}/rel_invalid.csv",
            False,
            [[2, 3, 13]]

    )
])
def test_add(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    a = add(r, [0, 1], [-1, 5], 2)

    assert a == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
            f"{inputs_path}/rel_one.csv",
            False,
            [[1, 2, 3, 7], [4, 5, 6, 13], [7, 8, 9, 19]]
    ),
    (
            f"{inputs_path}/rel_two.csv",
            False,
            [[1, 3, 6, 8], [7, 1, 8, 12]]
    ),
    (
            f"{inputs_path}/rel_two.csv",
            True,
            [[1.4, 3.2, 6.7, 8.6], [7.0, 1.6, 8.9, 12.6]]
    ),
    (
            f"{inputs_path}/rel_invalid.csv",
            False,
            [[2, 3, 4, 9]]

    )
])
def test_add_new_col(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    a = add(r, [0, 1], [-1, 5], 3)

    assert a == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
        f"{inputs_path}/rel_one.csv",
        False,
        [[12, 2, 3], [240, 5, 6], [1008, 8, 9]]
    ),
    (
        f"{inputs_path}/rel_one.csv",
        True,
        [[12.0, 2.0, 3.0], [240.0, 5.0, 6.0], [1008.0, 8.0, 9.0]]
    ),
    (
        f"{inputs_path}/rel_two.csv",
        False,
        [[36, 3, 6], [112, 1, 8]]
    ),
    (
        f"{inputs_path}/rel_two.csv",
        True,
        [[60.032, 3.2, 6.7], [199.36, 1.6, 8.9]]
    ),
    (
        f"{inputs_path}/rel_invalid.csv",
        False,
        [[48, 3, 4]]

    ),
    (
        f"{inputs_path}/rel_invalid.csv",
        True,
        [[48.0, 3.0, 4.0]]
    )
])
def test_multiply(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    mult = multiply(r, [1, 2], [2], 0)

    assert mult == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
        f"{inputs_path}/rel_one.csv",
        False,
        [[1, 2, 3, 6], [4, 5, 6, 60], [7, 8, 9, 168]]
    ),
    (
        f"{inputs_path}/rel_one.csv",
        True,
        [[1.0, 2.0, 3.0, 6.0], [4.0, 5.0, 6.0, 60.0], [7.0, 8.0, 9.0, 168.0]]
    ),
    (
        f"{inputs_path}/rel_two.csv",
        False,
        [[1, 3, 6, 9], [7, 1, 8, 21]]
    ),
    (
        f"{inputs_path}/rel_two.csv",
        True,
        [[1.4, 3.2, 6.7, 13.439999999999998], [7.0, 1.6, 8.9, 33.6]]
    ),
    (
        f"{inputs_path}/rel_invalid.csv",
        False,
        [[2, 3, 4, 18]]

    ),
    (
        f"{inputs_path}/rel_invalid.csv",
        True,
        [[2.0, 3.0, 4.0, 18.0]]
    )
])
def test_multiply_new_col(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    mult = multiply(r, [0, 1], [3], 3)

    assert mult == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
            f"{inputs_path}/rel_one.csv",
            False,
            [[-4, 2, 3], [-4, 5, 6], [-4, 8, 9]]
    ),
    (
            f"{inputs_path}/rel_two.csv",
            False,
            [[-7, 3, 6], [-3, 1, 8]]
    ),
    (
            f"{inputs_path}/rel_invalid.csv",
            False,
            [[-4, 3, 4]]

    )
])
def test_subtract(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    ops = [
        {"__TYPE__": "col", "v": 2},
        {"__TYPE__": "scal", "v": 2}
    ]
    s = subtract(r, ops, 0)

    assert s == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
            f"{inputs_path}/rel_one.csv",
            False,
            [[1, 2, 3, 1], [4, 5, 6, 4], [7, 8, 9, 7]]
    ),
    (
            f"{inputs_path}/rel_two.csv",
            False,
            [[1, 3, 6, 4], [7, 1, 8, 6]]
    ),
    (
            f"{inputs_path}/rel_invalid.csv",
            False,
            [[2, 3, 4, 2]]

    )
])
def test_subtract_new_col(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    ops = [
        {"__TYPE__": "col", "v": 2},
        {"__TYPE__": "scal", "v": 2}
    ]
    s = subtract(r, ops, 3)

    assert s == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
        f"{inputs_path}/rel_one.csv",
        False,
        [[0.16666666666666666, 2, 3], [0.3333333333333333, 5, 6], [0.3888888888888889, 8, 9]]
    ),
    (
        f"{inputs_path}/rel_one.csv",
        True,
        [[0.16666666666666666, 2.0, 3.0], [0.3333333333333333, 5.0, 6.0], [0.3888888888888889, 8.0, 9.0]]
    ),
    (
        f"{inputs_path}/rel_two.csv",
        False,
        [[0.08333333333333333, 3, 6], [0.4375, 1, 8]]
    ),
    (
        f"{inputs_path}/rel_two.csv",
        True,
        [[0.1044776119402985, 3.2, 6.7], [0.3932584269662921, 1.6, 8.9]]
    ),
    (
        f"{inputs_path}/rel_invalid.csv",
        False,
        [[0.25, 3, 4]]

    ),
    (
        f"{inputs_path}/rel_invalid.csv",
        True,
        [[0.25, 3.0, 4.0]]
    )
])
def test_divide(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    ops = [
        {"__TYPE__": "col", "v": 2},
        {"__TYPE__": "scal", "v": 2}
    ]
    div = divide(r, ops, 0)

    assert div == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
        f"{inputs_path}/rel_one.csv",
        False,
        [[1, 2, 3, 0.16666666666666666], [4, 5, 6, 0.3333333333333333], [7, 8, 9, 0.3888888888888889]]
    ),
    (
        f"{inputs_path}/rel_one.csv",
        True,
        [[1.0, 2.0, 3.0, 0.16666666666666666], [4.0, 5.0, 6.0, 0.3333333333333333], [7.0, 8.0, 9.0, 0.3888888888888889]]
    ),
    (
        f"{inputs_path}/rel_two.csv",
        False,
        [[1, 3, 6, 0.08333333333333333], [7, 1, 8, 0.4375]]
    ),
    (
        f"{inputs_path}/rel_two.csv",
        True,
        [[1.4, 3.2, 6.7, 0.1044776119402985], [7.0, 1.6, 8.9, 0.3932584269662921]]
    ),
    (
        f"{inputs_path}/rel_invalid.csv",
        False,
        [[2, 3, 4, 0.25]]

    ),
    (
        f"{inputs_path}/rel_invalid.csv",
        True,
        [[2.0, 3.0, 4.0, 0.25]]
    )
])
def test_divide_new_col(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    ops = [
        {"__TYPE__": "col", "v": 0},
        {"__TYPE__": "col", "v": 2},
        {"__TYPE__": "scal", "v": 2}
    ]
    div = divide(r, ops, 3)

    assert div == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
        f"{inputs_path}/rel_one.csv",
        False,
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    ),
    (
        f"{inputs_path}/rel_two.csv",
        True,
        [[1.4, 3.2, 6.7], [7.0, 1.6, 8.9]]
    ),
    (
        f"{inputs_path}/rel_three.csv",
        False,
        [[1, 2, 3], [4, 5, 6], [1, 2, 3], [1, 6, 7]]
    ),
    (
        f"{inputs_path}/rel_four.csv",
        True,
        [[1.1, 2.3, 3.6], [4.5, 5.4, 6.0], [1.1, 2.6, 3.7], [4.5, 5.4, 7.8]]
    ),
    (
        f"{inputs_path}/rel_invalid.csv",
        False,
        [[2, 3, 4]]

    )
])
def test_limit(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    lim = limit(r, 4)

    assert lim == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
        f"{inputs_path}/rel_one.csv",
        False,
        [[1], [4], [7]]
    ),
    (
        f"{inputs_path}/rel_two.csv",
        True,
        [[1.4], [7.0]]
    ),
    (
        f"{inputs_path}/rel_three.csv",
        False,
        [[1], [4], [2]]
    ),
    (
        f"{inputs_path}/rel_four.csv",
        True,
        [[1.1], [4.5], [2.1]]
    ),
    (
        f"{inputs_path}/rel_invalid.csv",
        False,
        [[2]]

    )
])
def test_distinct(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    dis = distinct(r, [0])

    assert dis == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected, operator", [
    (
        f"{inputs_path}/rel_one.csv",
        False,
        [],
        "=="
    ),
    (
        f"{inputs_path}/rel_one.csv",
        True,
        [],
        "<"
    ),
    (
        f"{inputs_path}/rel_one.csv",
        True,
        [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]],
        ">"
    )
])
def test_filter_against_col(path_to_rel: str, use_floats: bool, expected: list, operator: str):

    r = create(path_to_rel, use_floats=use_floats)
    fil = filter_against_col(r, 2, 0, operator)

    assert fil == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected, operator", [
    (
        f"{inputs_path}/rel_one.csv",
        False,
        [],
        "=="
    ),
    (
        f"{inputs_path}/rel_one.csv",
        True,
        [[1.0, 2.0, 3.0]],
        "<"
    ),
    (
        f"{inputs_path}/rel_one.csv",
        True,
        [[4.0, 5.0, 6.0], [7.0, 8.0, 9.0]],
        ">"
    )
])
def test_filter_against_scalar(path_to_rel: str, use_floats: bool, expected: list, operator: str):

    r = create(path_to_rel, use_floats=use_floats)
    fil = filter_against_scalar(r, 2, 5, operator=operator)

    assert fil == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected, increasing", [
    (
        f"{inputs_path}/rel_one.csv",
        False,
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        True
    ),
    (
        f"{inputs_path}/rel_two.csv",
        False,
        [[7, 1, 8], [1, 3, 6]],
        True
    ),
    (
        f"{inputs_path}/rel_two.csv",
        True,
        [[1.4, 3.2, 6.7], [7.0, 1.6, 8.9]],
        False
    )
])
def test_sort_by(path_to_rel: str, use_floats: bool, expected: list, increasing: bool):

    r = create(path_to_rel, use_floats=use_floats)
    sb = sort_by(r, 1, increasing=increasing)

    assert sb == expected


@pytest.mark.parametrize("path_to_rel, expected", [
    (
        f"{inputs_path}/rel_one.csv",
        [[3]]
    ),
    (
        f"{inputs_path}/rel_two.csv",
        [[2]]
    ),
    (
        f"{inputs_path}/rel_three.csv",
        [[7]]
    ),
    (
        f"{inputs_path}/rel_invalid.csv",
        [[1]]
    )
])
def test_num_rows(path_to_rel: str, expected: list):

    r = create(path_to_rel)
    nr = num_rows(r)

    assert nr == expected


@pytest.mark.parametrize("path_to_rel, expected", [
    (
        f"{inputs_path}/rel_one.csv",
        [[1, 2, 3, 0], [4, 5, 6, 1], [7, 8, 9, 2]]
    ),
    (
        f"{inputs_path}/rel_two.csv",
        [[1, 3, 6, 0], [7, 1, 8, 1]]
    ),
    (
        f"{inputs_path}/rel_invalid.csv",
        [[2, 3, 4, 0]]

    )
])
def test_index(path_to_rel: str, expected: list):

    r = create(path_to_rel)
    idx = index(r)

    assert idx == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
        f"{inputs_path}/rel_one.csv",
        False,
        [[1, 3, 1], [4, 6, 1], [7, 9, 1]]
    ),
    (
        f"{inputs_path}/rel_three.csv",
        False,
        [[1, 13, 3], [4, 10, 2], [2, 17, 2]]
    ),
    (
        f"{inputs_path}/rel_four.csv",
        True,
        [[1.1, 11.3, 3], [4.5, 13.8, 2], [2.1, 17.7, 2]]
    )
])
def test_aggregate_sum_count_col(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    agg = aggregate_sum_count_col(r, [0], 2)

    assert agg == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
        f"{inputs_path}/rel_one.csv",
        False,
        [[1, 3, 9, 1], [4, 6, 36, 1], [7, 9, 81, 1]]
    ),
    (
        f"{inputs_path}/rel_three.csv",
        False,
        [[1, 13, 67, 3], [4, 10, 52, 2], [2, 17, 145, 2]]
    ),
    (
        f"{inputs_path}/rel_four.csv",
        True,
        [[1.1, 11.3, 42.650000000000006, 3], [4.5, 13.8, 96.84, 2], [2.1, 17.7, 157.76999999999998, 2]]
    )
])
def test_aggregate_sum_squares_and_count(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    agg = aggregate_sum_squares_and_count(r, [0], 2)

    assert agg == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
        f"{inputs_path}/rel_five.csv",
        False,
        [[1, 0], [4, 0], [7, 0]]
    ),
    (
        f"{inputs_path}/rel_six.csv",
        False,
        [[1, 6.0], [2, 6.855654600401044]]
    )
])
def test_aggregate_std_dev_local_sqrt(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    agg = aggregate_std_dev_local_sqrt(r)

    assert agg == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
            f"{inputs_path}/rel_five.csv",
            False,
            [[1, 0], [4, 0], [7, 0]]
    ),
    (
            f"{inputs_path}/rel_six.csv",
            False,
            [[1, 36.0], [2, 47]]
    )
])
def test_aggregate_variance_local_diff(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    agg = aggregate_variance_local_diff(r)

    assert agg == expected


@pytest.mark.parametrize("path_to_rel, use_floats, expected", [
    (
        f"{inputs_path}/rel_one.csv",
        False,
        [[12, 15, 18]]
    ),
    (
        f"{inputs_path}/rel_two.csv",
        False,
        [[8, 4, 14]]
    ),
    (
        f"{inputs_path}/rel_two.csv",
        True,
        [[8.4, 4.800000000000001, 15.600000000000001]]
    )
])
def test_col_sum(path_to_rel: str, use_floats: bool, expected: list):

    r = create(path_to_rel, use_floats=use_floats)
    cs = col_sum(r)

    assert cs == expected
