from congregation.lang import *
from congregation.utils import create_column
import pytest


def _create_cols(col_names):

    ret = []
    for i in range(len(col_names)):
        ret.append(create_column(col_names[i], "INTEGER", set(), set()))

    return ret


@pytest.mark.parametrize("col_names, stored_with, expected_sets", [
    (
            ["a", "b"],
            [{1}],
            [{1}]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}],
            [{1, 2, 3}]
    ),
    (
            ["a", "b"],
            [{1, 2}],
            [{1, 2}]
    ),
    (
            ["a", "b", "c"],
            [{1, 2}, {3, 4}],
            [{1, 2}, {3, 4}]
    )
])
def test_create(col_names, stored_with, expected_sets):

    input_cols = _create_cols(col_names)
    c = create("in1", input_cols, stored_with)
    collect(c, {1, 2, 3})

    assert c.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with, expected_sets", [
    (
            ["a", "b"],
            [{1, 2, 3}],
            [{1, 2, 3}]
    ),
    (
            ["a", "b"],
            {1, 2, 3},
            [{1, 2, 3}]
    ),
    (
            ["a", "b"],
            [{1}],
            [{1}]
    ),
    (
            ["a", "b"],
            {1, 2, 3, 4, 5, 6},
            [{1, 2, 3, 4, 5, 6}]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, {4, 5, 6}],
            [{1, 2, 3}, {4, 5, 6}]
    )
])
def test_agg_simple(col_names, stored_with, expected_sets):

    input_cols = _create_cols(col_names)
    c = create("in1", input_cols, stored_with)
    a = aggregate(c, "agg", [col_names[0]], col_names[1], "sum")
    collect(a, {1, 2, 3})

    assert a.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with, expected_sets", [
    (
            ["a", "b"],
            [{1, 2, 3}],
            [{1, 2, 3}]
    ),
    (
            ["a", "b"],
            {1, 2, 3},
            [{1, 2, 3}]
    ),
    (
            ["a", "b"],
            [{1}],
            [{1}]
    ),
    (
            ["a", "b"],
            {1, 2, 3, 4, 5, 6},
            [{1, 2, 3, 4, 5, 6}]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, {4, 5, 6}],
            [{1, 2, 3}, {4, 5, 6}]
    )
])
def test_project_simple(col_names, stored_with, expected_sets):

    input_cols = _create_cols(col_names)
    c = create("in1", input_cols, stored_with)
    p = project(c, "proj", [col_names[0]])
    collect(p, {1, 2, 3})

    assert p.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with, expected_sets", [
    (
            ["a", "b"],
            [{1, 2, 3}],
            [{1, 2, 3}]
    ),
    (
            ["a", "b"],
            [{1}],
            [{1}]
    ),
    (
            ["a", "b"],
            {1, 2, 3, 4, 5, 6},
            [{1, 2, 3, 4, 5, 6}]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, {4, 5, 6}],
            [{1, 2, 3}, {4, 5, 6}]
    )
])
def test_mult_simple(col_names, stored_with, expected_sets):

    input_cols = _create_cols(col_names)
    c = create("in1", input_cols, stored_with)
    m = multiply(c, "mult", col_names[0], [3, col_names[1]])
    collect(m, {1, 2, 3})

    assert m.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with, expected_sets", [
    (
            ["a", "b"],
            [{1, 2, 3}],
            [{1, 2, 3}]
    ),
    (
            ["a", "b"],
            [{1}],
            [{1}]
    ),
    (
            ["a", "b"],
            {1, 2, 3, 4, 5, 6},
            [{1, 2, 3, 4, 5, 6}]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, {4, 5, 6}],
            [{1, 2, 3}, {4, 5, 6}]
    )
])
def test_mult_new_col(col_names, stored_with, expected_sets):

    input_cols = _create_cols(col_names)
    c = create("in1", input_cols, stored_with)
    m = multiply(c, "mult", "new_col", [col_names[0], col_names[1]])
    collect(m, {1, 2, 3})

    assert m.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with, expected_sets", [
    (
            ["a", "b"],
            [{1, 2, 3}],
            [{1, 2, 3}]
    ),
    (
            ["a", "b"],
            [{1}],
            [{1}]
    ),
    (
            ["a", "b"],
            {1, 2, 3, 4, 5, 6},
            [{1, 2, 3, 4, 5, 6}]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, {4, 5, 6}],
            [{1, 2, 3}, {4, 5, 6}]
    )
])
def test_div_simple(col_names, stored_with, expected_sets):

    input_cols = _create_cols(col_names)
    c = create("in1", input_cols, stored_with)
    d = divide(c, "div", col_names[0], [3, col_names[1]])
    collect(d, {1, 2, 3})

    assert d.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with, expected_sets", [
    (
            ["a", "b"],
            [{1, 2, 3}],
            [{1, 2, 3}]
    ),
    (
            ["a", "b"],
            [{1}],
            [{1}]
    ),
    (
            ["a", "b"],
            {1, 2, 3, 4, 5, 6},
            [{1, 2, 3, 4, 5, 6}]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, {4, 5, 6}],
            [{1, 2, 3}, {4, 5, 6}]
    )
])
def test_div_new_col(col_names, stored_with, expected_sets):

    input_cols = _create_cols(col_names)
    c = create("in1", input_cols, stored_with)
    d = divide(c, "div", "new_col", [col_names[0], col_names[1]])
    collect(d, {1, 2, 3})

    assert d.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with, expected_sets", [
    (
            ["a", "b"],
            [{1, 2, 3}],
            [{1, 2, 3}]
    ),
    (
            ["a", "b"],
            [{1}],
            [{1}]
    ),
    (
            ["a", "b"],
            {1, 2, 3, 4, 5, 6},
            [{1, 2, 3, 4, 5, 6}]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, {4, 5, 6}],
            [{1, 2, 3}, {4, 5, 6}]
    )
])
def test_limit_simple(col_names, stored_with, expected_sets):

    input_cols = _create_cols(col_names)
    c = create("in1", input_cols, stored_with)
    l = limit(c, "lim", 10)
    collect(l, {1, 2, 3})

    assert l.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with, expected_sets", [
    (
            ["a", "b"],
            [{1, 2, 3}],
            [{1, 2, 3}]
    ),
    (
            ["a", "b"],
            [{1}],
            [{1}]
    ),
    (
            ["a", "b"],
            {1, 2, 3, 4, 5, 6},
            [{1, 2, 3, 4, 5, 6}]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, {4, 5, 6}],
            [{1, 2, 3}, {4, 5, 6}]
    )
])
def test_distinct_simple(col_names, stored_with, expected_sets):

    input_cols = _create_cols(col_names)
    c = create("in1", input_cols, stored_with)
    d = distinct(c, "dis", [col_names[0]])
    collect(d, {1, 2, 3})

    assert d.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with, expected_sets", [
    (
            ["a", "b"],
            [{1, 2, 3}],
            [{1, 2, 3}]
    ),
    (
            ["a", "b"],
            [{1}],
            [{1}]
    ),
    (
            ["a", "b"],
            {1, 2, 3, 4, 5, 6},
            [{1, 2, 3, 4, 5, 6}]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, {4, 5, 6}],
            [{1, 2, 3}, {4, 5, 6}]
    )
])
def test_filter_by_col_simple(col_names, stored_with, expected_sets):

    input_cols = _create_cols(col_names)
    c = create("in1", input_cols, stored_with)
    f = filter_by(c, "filt", col_names[0], "==", col_names[1])
    collect(f, {1, 2, 3})

    assert f.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with, expected_sets", [
    (
            ["a", "b"],
            [{1, 2, 3}],
            [{1, 2, 3}]
    ),
    (
            ["a", "b"],
            [{1}],
            [{1}]
    ),
    (
            ["a", "b"],
            {1, 2, 3, 4, 5, 6},
            [{1, 2, 3, 4, 5, 6}]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, {4, 5, 6}],
            [{1, 2, 3}, {4, 5, 6}]
    )
])
def test_filter_by_scalar_simple(col_names, stored_with, expected_sets):

    input_cols = _create_cols(col_names)
    c = create("in1", input_cols, stored_with)
    f = filter_by(c, "filt", col_names[0], "==", 7)
    collect(f, {1, 2, 3})

    assert f.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with, expected_sets", [
    (
            ["a", "b"],
            [{1, 2, 3}],
            [{1, 2, 3}]
    ),
    (
            ["a", "b"],
            [{1}],
            [{1}]
    ),
    (
            ["a", "b"],
            {1, 2, 3, 4, 5, 6},
            [{1, 2, 3, 4, 5, 6}]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, {4, 5, 6}],
            [{1, 2, 3}, {4, 5, 6}]
    )
])
def test_sort_by_simple(col_names, stored_with, expected_sets):

    input_cols = _create_cols(col_names)
    c = create("in1", input_cols, stored_with)
    s = sort_by(c, "sort", col_names[0])
    collect(s, {1, 2, 3})

    assert s.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with_sets, expected_sets", [
    (
            [["a", "b"], ["a", "b"]],
            [[{1, 2, 3}], [{1, 2, 3}]],
            [{1, 2, 3}]
    ),
    (
            [["a", "b"], ["a", "b"]],
            [[{1}], [{2}]],
            [{1}, {2}]
    ),
    (
            [["a", "b"], ["a", "b"]],
            [[{1, 2, 3}], [{4, 5, 6}]],
            [{1, 2, 3}, {4, 5, 6}]
    ),
    (
            [["a", "b"], ["a", "b"]],
            [[{1, 2}], [{2, 3}]],
            [{1, 2}, {2, 3}]
    )
])
def test_join_simple(col_names, stored_with_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0])
    cols_in_two = _create_cols(col_names[1])

    rel_one = create("in1", cols_in_one, stored_with_sets[0])
    rel_two = create("in2", cols_in_two, stored_with_sets[1])

    j = join(rel_one, rel_two, "join", [rel_one.out_rel.columns[0].name], [rel_two.out_rel.columns[0].name])
    collect(j, {1, 2, 3})

    assert j.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with_sets, expected_sets", [
    (
            [["a", "b"], ["a", "b"], ["a", "b"]],
            [[{1, 2, 3}], [{1, 2, 3}], [{1, 2, 3}]],
            [{1, 2, 3}]
    ),
    (
            [["a", "b"], ["a", "b"], ["a", "b"]],
            [[{1}], [{2}], [{3}]],
            [{1}, {2}, {3}]
    ),
    (
            [["a", "b"], ["a", "b"], ["a", "b"]],
            [[{1, 2, 3}], [{4, 5, 6}], [{7, 8, 9}]],
            [{1, 2, 3}, {4, 5, 6}, {7, 8, 9}]
    ),
    (
            [["a", "b"], ["a", "b"], ["a", "b"]],
            [[{1, 2}], [{2, 3}], [{3, 4}]],
            [{1, 2}, {2, 3}, {3, 4}]
    )
])
def test_concat_simple(col_names, stored_with_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0])
    cols_in_two = _create_cols(col_names[1])
    cols_in_three = _create_cols(col_names[2])

    rel_one = create("in1", cols_in_one, stored_with_sets[0])
    rel_two = create("in2", cols_in_two, stored_with_sets[1])
    rel_three = create("in3", cols_in_three, stored_with_sets[2])

    cc = concat([rel_one, rel_two, rel_three], "concat", col_names[0])
    collect(cc, {1, 2, 3})

    assert cc.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with_sets, expected_sets", [
    (
            [["a", "b"], ["a", "b"], ["a", "b"]],
            [[{1, 2, 3}], [{1, 2, 3}], [{1, 2, 3}]],
            [{1, 2, 3}]
    ),
    (
            [["a", "b"], ["a", "b"], ["a", "b"]],
            [[{1}], [{2}], [{3}]],
            [{1}, {2}, {3}]
    ),
    (
            [["a", "b"], ["a", "b"], ["a", "b"]],
            [[{1, 2, 3}], [{4, 5, 6}], [{7, 8, 9}]],
            [{1, 2, 3}, {4, 5, 6}, {7, 8, 9}]
    ),
    (
            [["a", "b"], ["a", "b"], ["a", "b"]],
            [[{1, 2}], [{2, 3}], [{3, 4}]],
            [{1, 2}, {2, 3}, {3, 4}]
    )
])
def test_concat_agg(col_names, stored_with_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0])
    cols_in_two = _create_cols(col_names[1])
    cols_in_three = _create_cols(col_names[2])

    rel_one = create("in1", cols_in_one, stored_with_sets[0])
    rel_two = create("in2", cols_in_two, stored_with_sets[1])
    rel_three = create("in3", cols_in_three, stored_with_sets[2])

    cc = concat([rel_one, rel_two, rel_three], "concat", col_names[0])
    agg = aggregate(cc, "agg", [col_names[0][0]], col_names[0][1], "sum")
    collect(agg, {1, 2, 3})

    assert agg.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with_sets, expected_sets", [
    (
            [["a", "b"], ["a", "b"]],
            [[{1, 2, 3}], [{1, 2, 3}]],
            [{1, 2, 3}]
    ),
    (
            [["a", "b"], ["a", "b"]],
            [[{1}], [{2}]],
            [{1}, {2}]
    ),
    (
            [["a", "b"], ["a", "b"]],
            [[{1, 2, 3}], [{4, 5, 6}]],
            [{1, 2, 3}, {4, 5, 6}]
    ),
    (
            [["a", "b"], ["a", "b"]],
            [[{1, 2}], [{2, 3}]],
            [{1, 2}, {2, 3}]
    )
])
def test_concat_project(col_names, stored_with_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0])
    cols_in_two = _create_cols(col_names[1])

    rel_one = create("in1", cols_in_one, stored_with_sets[0])
    rel_two = create("in2", cols_in_two, stored_with_sets[1])

    cc = concat([rel_one, rel_two], "concat", col_names[0])
    p = project(cc, "proj", [col_names[0][0], col_names[0][1]])
    collect(p, {1, 2, 3})

    assert p.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with_sets, expected_sets", [
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1, 2, 3}], [{1, 2, 3}]],
            [{1, 2, 3}]
    ),
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1}], [{2}]],
            [{1}, {2}]
    ),
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1, 2, 3}], [{4, 5, 6}]],
            [{1, 2, 3}, {4, 5, 6}]
    ),
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1, 2}], [{2, 3}]],
            [{1, 2}, {2, 3}]
    )
])
def test_concat_mult(col_names, stored_with_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0])
    cols_in_two = _create_cols(col_names[1])

    rel_one = create("in1", cols_in_one, stored_with_sets[0])
    rel_two = create("in2", cols_in_two, stored_with_sets[1])

    cc = concat([rel_one, rel_two], "concat", col_names[0])
    m = multiply(cc, "mult", col_names[0][0], [3, col_names[0][1], col_names[0][2]])

    assert m.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with_sets, expected_sets", [
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1, 2, 3}], [{1, 2, 3}]],
            [{1, 2, 3}]
    ),
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1}], [{2}]],
            [{1}, {2}]
    ),
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1, 2, 3}], [{4, 5, 6}]],
            [{1, 2, 3}, {4, 5, 6}]
    ),
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1, 2}], [{2, 3}]],
            [{1, 2}, {2, 3}]
    )
])
def test_concat_div(col_names, stored_with_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0])
    cols_in_two = _create_cols(col_names[1])

    rel_one = create("in1", cols_in_one, stored_with_sets[0])
    rel_two = create("in2", cols_in_two, stored_with_sets[1])

    cc = concat([rel_one, rel_two], "concat", col_names[0])
    d = divide(cc, "div", col_names[0][0], [3, col_names[0][1], col_names[0][2]])

    assert d.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with_sets, expected_sets", [
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1, 2, 3}], [{1, 2, 3}]],
            [{1, 2, 3}]
    ),
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1}], [{2}]],
            [{1}, {2}]
    ),
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1, 2, 3}], [{4, 5, 6}]],
            [{1, 2, 3}, {4, 5, 6}]
    ),
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1, 2}], [{2, 3}]],
            [{1, 2}, {2, 3}]
    )
])
def test_concat_limit(col_names, stored_with_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0])
    cols_in_two = _create_cols(col_names[1])

    rel_one = create("in1", cols_in_one, stored_with_sets[0])
    rel_two = create("in2", cols_in_two, stored_with_sets[1])

    cc = concat([rel_one, rel_two], "concat", col_names[0])
    l = limit(cc, "lim", 100)

    assert l.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with_sets, expected_sets", [
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1, 2, 3}], [{1, 2, 3}]],
            [{1, 2, 3}]
    ),
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1}], [{2}]],
            [{1}, {2}]
    ),
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1, 2, 3}], [{4, 5, 6}]],
            [{1, 2, 3}, {4, 5, 6}]
    ),
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1, 2}], [{2, 3}]],
            [{1, 2}, {2, 3}]
    )
])
def test_concat_distinct(col_names, stored_with_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0])
    cols_in_two = _create_cols(col_names[1])

    rel_one = create("in1", cols_in_one, stored_with_sets[0])
    rel_two = create("in2", cols_in_two, stored_with_sets[1])

    cc = concat([rel_one, rel_two], "concat", col_names[0])
    d = distinct(cc, "dis", [col_names[0][1]])

    assert d.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with_sets, expected_sets", [
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1, 2, 3}], [{1, 2, 3}]],
            [{1, 2, 3}]
    ),
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1}], [{2}]],
            [{1}, {2}]
    ),
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1, 2, 3}], [{4, 5, 6}]],
            [{1, 2, 3}, {4, 5, 6}]
    ),
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1, 2}], [{2, 3}]],
            [{1, 2}, {2, 3}]
    )
])
def test_concat_filter_by(col_names, stored_with_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0])
    cols_in_two = _create_cols(col_names[1])

    rel_one = create("in1", cols_in_one, stored_with_sets[0])
    rel_two = create("in2", cols_in_two, stored_with_sets[1])

    cc = concat([rel_one, rel_two], "concat", col_names[0])
    f = filter_by(cc, "filt", col_names[0][0], "==", col_names[0][1])

    assert f.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with_sets, expected_sets", [
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1, 2, 3}], [{1, 2, 3}]],
            [{1, 2, 3}]
    ),
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1}], [{2}]],
            [{1}, {2}]
    ),
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1, 2, 3}], [{4, 5, 6}]],
            [{1, 2, 3}, {4, 5, 6}]
    ),
    (
            [["a", "b", "c"], ["a", "b", "c"]],
            [[{1, 2}], [{2, 3}]],
            [{1, 2}, {2, 3}]
    )
])
def test_concat_sort_by(col_names, stored_with_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0])
    cols_in_two = _create_cols(col_names[1])

    rel_one = create("in1", cols_in_one, stored_with_sets[0])
    rel_two = create("in2", cols_in_two, stored_with_sets[1])

    cc = concat([rel_one, rel_two], "concat", col_names[0])
    s = sort_by(cc, "filt", col_names[0][0])

    assert s.out_rel.stored_with == expected_sets


@pytest.mark.parametrize("col_names, stored_with_sets, expected_sets", [
    (
            [["a", "b"], ["a", "b"], ["a", "b"]],
            [[{1, 2, 3}], [{1, 2, 3}], [{1, 2, 3}]],
            [{1, 2, 3}]
    ),
    (
            [["a", "b"], ["a", "b"], ["a", "b"]],
            [[{1}], [{2}], [{3}]],
            [{1}, {2}, {3}]
    ),
    (
            [["a", "b"], ["a", "b"], ["a", "b"]],
            [[{1, 2, 3}], [{4, 5, 6}], [{7, 8, 9}]],
            [{1, 2, 3}, {4, 5, 6}, {7, 8, 9}]
    ),
    (
            [["a", "b"], ["a", "b"], ["a", "b"]],
            [[{1, 2}], [{2, 3}], [{3, 4}]],
            [{1, 2}, {2, 3}, {3, 4}]
    )
])
def test_concat_join(col_names, stored_with_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0])
    cols_in_two = _create_cols(col_names[1])
    cols_in_three = _create_cols(col_names[2])

    rel_one = create("in1", cols_in_one, stored_with_sets[0])
    rel_two = create("in2", cols_in_two, stored_with_sets[1])
    rel_three = create("in3", cols_in_three, stored_with_sets[2])

    cc = concat([rel_one, rel_two], "concat", col_names[0])
    j = join(cc, rel_three, "join", [cc.out_rel.columns[0].name], [rel_three.out_rel.columns[0].name])
    collect(j, {1, 2, 3})

    assert j.out_rel.stored_with == expected_sets
