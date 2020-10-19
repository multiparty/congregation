from congregation.codegen.python.libs import *
import pytest
import os


"""
Tests for correct relation calculation under the congregation python module
"""


inputs_path = f"{os.path.dirname(os.path.realpath(__file__))}/inputs"


@pytest.mark.parametrize("path_to_rel_one, path_to_rel_two, use_floats, expected", [
    (
        f"{inputs_path}/rel_three.csv",
        f"{inputs_path}/rel_four.csv",
        False,
        [[4, 5, 6, 1, 2], [4, 5, 4, 1, 2]]
    ),
    (
        f"{inputs_path}/rel_three.csv",
        f"{inputs_path}/rel_four.csv",
        True,
        [[4.0, 5.0, 6.0, 1.1, 2.3], [4.0, 5.0, 4.0, 1.1, 2.3]]
    )
])
def test_join(path_to_rel_one: str, path_to_rel_two: str, use_floats: bool, expected: list):

    r_one = create(path_to_rel_one, use_floats=use_floats)
    r_two = create(path_to_rel_two, use_floats=use_floats)

    j = join(r_one, r_two, [0], [2])
    assert j == expected

