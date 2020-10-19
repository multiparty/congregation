from congregation.codegen.python.libs import *
import pytest
import os


"""
Tests for correct relation calculation under the congregation python module
"""


inputs_path = f"{os.path.dirname(os.path.realpath(__file__))}/inputs"


@pytest.mark.parametrize("paths_to_rels, use_floats, expected", [
    (
        [f"{inputs_path}/rel_three.csv", f"{inputs_path}/rel_four.csv"],
        False,
        [
            [1, 2, 3], [4, 5, 6], [1, 2, 3],
            [1, 6, 7], [2, 4, 8], [4, 5, 4],
            [2, 8, 9], [1, 2, 3], [4, 5, 6],
            [1, 2, 3], [4, 5, 7], [2, 4, 8],
            [1, 2, 4], [2, 8, 9]
        ]
    ),
    (
        [f"{inputs_path}/rel_three.csv", f"{inputs_path}/rel_four.csv"],
        True,
        [
            [1, 2, 3], [4, 5, 6], [1, 2, 3],
            [1, 6, 7], [2, 4, 8], [4, 5, 4],
            [2, 8, 9], [1.1, 2.3, 3.6], [4.5, 5.4, 6.0],
            [1.1, 2.6, 3.7], [4.5, 5.4, 7.8], [2.1, 4.3, 8.1],
            [1.1, 2.3, 4.0], [2.1, 8.8, 9.6]
        ]
    )
])
def test_concat(paths_to_rels: list, use_floats: bool, expected: list):

    rels = [create(path_to_rel, use_floats=use_floats) for path_to_rel in paths_to_rels]
    cc = concat(rels)

    assert cc == expected
