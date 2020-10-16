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

