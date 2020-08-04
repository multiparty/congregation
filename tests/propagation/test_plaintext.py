from congregation.lang import *
from congregation.utils import create_column
import pytest


def _create_cols(col_names, plaintext_sets):

    ret = []
    for i in range(len(col_names)):
        ret.append(create_column(col_names[i], "INTEGER", set(), plaintext_sets[i]))

    return ret


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            ["a", "b"],
            [set(), set()],
            [set(), set()]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, set()],
            [{1, 2, 3}, set()]
    ),
    (
            ["a", "b"],
            [{1}, {2}],
            [{1}, {2}]
    ),
    (
            ["a", "b", "c"],
            [{1, 2}, {3}, {1}],
            [{1, 2}, {3}, {1}]
    )
])
def test_create(col_names, plaintext_sets, expected_sets):

    input_cols = _create_cols(col_names, plaintext_sets)
    c = create("in1", input_cols, {1, 2, 3})
    collect(c, {1, 2, 3})

    out_cols = c.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            ["a", "b"],
            [set(), set()],
            [set(), set()]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, set()],
            [set(), set()]
    ),
    (
            ["a", "b"],
            [{1}, {1, 2}],
            [{1}, {1}]
    ),
    (
            ["a", "b", "c"],
            [{1, 2}, {3}, {1}],
            [set(), set()]
    ),
    (
            ["a", "b", "c"],
            [{1, 2}, {2, 3}, {1}],
            [{2}, {2}]
    )
])
def test_agg_simple(col_names, plaintext_sets, expected_sets):

    input_cols = _create_cols(col_names, plaintext_sets)
    c = create("in1", input_cols, {1, 2, 3})
    a = aggregate(c, "agg", [col_names[0]], col_names[1], "sum")
    collect(a, {1, 2, 3})

    out_cols = a.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            ["a", "b"],
            [set(), set()],
            [set()]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, set()],
            [{1, 2, 3}]
    ),
    (
            ["a", "b"],
            [set(), {1, 2, 3}],
            [set()]
    ),
    (
            ["a", "b"],
            [{1}, {2}],
            [{1}]
    ),
    (
            ["a", "b", "c"],
            [{1, 2}, {3}, {1}],
            [{1, 2}]
    )
])
def test_project_simple(col_names, plaintext_sets, expected_sets):

    input_cols = _create_cols(col_names, plaintext_sets)
    c = create("in1", input_cols, {1, 2, 3})
    p = project(c, "proj", [col_names[0]])
    collect(p, {1, 2, 3})

    out_cols = p.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            ["a", "b"],
            [set(), set()],
            [set(), set()]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, set()],
            [set(), set()]
    ),
    (
            ["a", "b"],
            [{1}, {1, 2}],
            [{1}, {1, 2}]
    ),
    (
            ["a", "b", "c"],
            [{1, 2}, {3}, {1}],
            [set(), {3}, {1}]
    )
])
def test_mult_simple(col_names, plaintext_sets, expected_sets):

    input_cols = _create_cols(col_names, plaintext_sets)
    c = create("in1", input_cols, {1, 2, 3})
    m = multiply(c, "mult", col_names[0], [3, col_names[1]])
    collect(m, {1, 2, 3})

    out_cols = m.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            ["a", "b"],
            [set(), set()],
            [set(), set(), set()]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, set()],
            [{1, 2, 3}, set(), set()]
    ),
    (
            ["a", "b"],
            [{1}, {1, 2}],
            [{1}, {1, 2}, {1}]
    ),
    (
            ["a", "b", "c"],
            [{1, 2}, {3}, {1}],
            [{1, 2}, {3}, {1}, set()]
    ),
    (
            ["a", "b", "c"],
            [{1, 2}, {2, 3}, {1}],
            [{1, 2}, {2, 3}, {1}, {2}]
    )
])
def test_mult_new_col(col_names, plaintext_sets, expected_sets):

    input_cols = _create_cols(col_names, plaintext_sets)
    c = create("in1", input_cols, {1, 2, 3})
    m = multiply(c, "mult", "new_col", [col_names[0], col_names[1]])
    collect(m, {1, 2, 3})

    out_cols = m.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            ["a", "b"],
            [set(), set()],
            [set(), set()]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, set()],
            [set(), set()]
    ),
    (
            ["a", "b"],
            [{1}, {1, 2}],
            [{1}, {1, 2}]
    ),
    (
            ["a", "b", "c"],
            [{1, 2}, {3}, {1}],
            [set(), {3}, {1}]
    )
])
def test_div_simple(col_names, plaintext_sets, expected_sets):

    input_cols = _create_cols(col_names, plaintext_sets)
    c = create("in1", input_cols, {1, 2, 3})
    d = divide(c, "div", col_names[0], [3, col_names[1]])
    collect(d, {1, 2, 3})

    out_cols = d.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            ["a", "b"],
            [set(), set()],
            [set(), set(), set()]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, set()],
            [{1, 2, 3}, set(), set()]
    ),
    (
            ["a", "b"],
            [{1}, {1, 2}],
            [{1}, {1, 2}, {1}]
    ),
    (
            ["a", "b", "c"],
            [{1, 2}, {3}, {1}],
            [{1, 2}, {3}, {1}, set()]
    ),
    (
            ["a", "b", "c"],
            [{1, 2}, {2, 3}, {1}],
            [{1, 2}, {2, 3}, {1}, {2}]
    )
])
def test_div_new_col(col_names, plaintext_sets, expected_sets):

    input_cols = _create_cols(col_names, plaintext_sets)
    c = create("in1", input_cols, {1, 2, 3})
    d = divide(c, "div", "new_col", [col_names[0], col_names[1]])
    collect(d, {1, 2, 3})

    out_cols = d.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            ["a", "b"],
            [set(), set()],
            [set(), set()]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, set()],
            [{1, 2, 3}, set()]
    ),
    (
            ["a", "b"],
            [{1}, {1, 2}],
            [{1}, {1, 2}]
    ),
    (
            ["a", "b", "c"],
            [{1, 2}, {3}, {1}],
            [{1, 2}, {3}, {1}]
    )
])
def test_limit_simple(col_names, plaintext_sets, expected_sets):

    input_cols = _create_cols(col_names, plaintext_sets)
    c = create("in1", input_cols, {1, 2, 3})
    l = limit(c, "lim", 10)
    collect(l, {1, 2, 3})

    out_cols = l.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            ["a", "b"],
            [set(), set()],
            [set(), set()]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, set()],
            [{1, 2, 3}, set()]
    ),
    (
            ["a", "b"],
            [{1}, {1, 2}],
            [{1}, {1, 2}]
    ),
    (
            ["a", "b", "c"],
            [{1, 2}, {3}, {1}],
            [{1, 2}, {3}, {1}]
    )
])
def test_distinct_simple(col_names, plaintext_sets, expected_sets):

    input_cols = _create_cols(col_names, plaintext_sets)
    c = create("in1", input_cols, {1, 2, 3})
    d = distinct(c, "dis", [col_names[0]])
    collect(d, {1, 2, 3})

    out_cols = d.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            ["a", "b"],
            [set(), set()],
            [set(), set()]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, set()],
            [{1, 2, 3}, set()]
    ),
    (
            ["a", "b"],
            [{1}, {1, 2}],
            [{1}, {1, 2}]
    ),
    (
            ["a", "b", "c"],
            [{1, 2}, {3}, {1}],
            [{1, 2}, {3}, {1}]
    )
])
def test_filter_by_col_simple(col_names, plaintext_sets, expected_sets):

    input_cols = _create_cols(col_names, plaintext_sets)
    c = create("in1", input_cols, {1, 2, 3})
    f = filter_by(c, "filt", col_names[0], "==", col_names[1])
    collect(f, {1, 2, 3})

    out_cols = f.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            ["a", "b"],
            [set(), set()],
            [set(), set()]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, set()],
            [{1, 2, 3}, set()]
    ),
    (
            ["a", "b"],
            [{1}, {1, 2}],
            [{1}, {1, 2}]
    ),
    (
            ["a", "b", "c"],
            [{1, 2}, {3}, {1}],
            [{1, 2}, {3}, {1}]
    )
])
def test_filter_by_scalar_simple(col_names, plaintext_sets, expected_sets):

    input_cols = _create_cols(col_names, plaintext_sets)
    c = create("in1", input_cols, {1, 2, 3})
    f = filter_by(c, "filt", col_names[0], "==", 7)
    collect(f, {1, 2, 3})

    out_cols = f.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            ["a", "b"],
            [set(), set()],
            [set(), set()]
    ),
    (
            ["a", "b"],
            [{1, 2, 3}, set()],
            [{1, 2, 3}, set()]
    ),
    (
            ["a", "b"],
            [{1}, {1, 2}],
            [{1}, {1, 2}]
    ),
    (
            ["a", "b", "c"],
            [{1, 2}, {3}, {1}],
            [{1, 2}, {3}, {1}]
    )
])
def test_sort_by_simple(col_names, plaintext_sets, expected_sets):

    input_cols = _create_cols(col_names, plaintext_sets)
    c = create("in1", input_cols, {1, 2, 3})
    s = sort_by(c, "sort", col_names[0])
    collect(s, {1, 2, 3})

    out_cols = s.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            [["a", "b"], ["c", "d"]],
            [[set(), set()], [set(), set()]],
            [set(), set(), set()]
    ),
    (
            [["a", "b"], ["c", "d"]],
            [[{1, 2}, set()], [{3}, set()]],
            [set(), set(), set()]
    ),
    (
            [["a", "b"], ["c", "d"]],
            [[{1}, {2}], [{1}, set()]],
            [{1}, {2}, set()]
    ),
    (
            [["a", "b"], ["c", "d"]],
            [[{1, 2}, set()], [{2}, {3}]],
            [{2}, set(), {3}]
    )
])
def test_join_simple(col_names, plaintext_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0], plaintext_sets[0])
    cols_in_two = _create_cols(col_names[1], plaintext_sets[1])

    rel_one = create("in1", cols_in_one, {1, 2, 3})
    rel_two = create("in2", cols_in_two, {1, 2, 3})

    j = join(rel_one, rel_two, "join", [rel_one.out_rel.columns[0].name], [rel_two.out_rel.columns[0].name])
    collect(j, {1, 2, 3})

    out_cols = j.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            [["a", "b"], ["c", "d"], ["e", "f"]],
            [[set(), set()], [set(), set()], [set(), set()]],
            [set(), set()]
    ),
    (
            [["a", "b"], ["c", "d"], ["e", "f"]],
            [[{1}, {1}], [{2}, {2}], [{3}, {3}]],
            [set(), set()]
    ),
    (
            [["a", "b"], ["c", "d"], ["e", "f"]],
            [[{1}, {1}], [{1, 2}, {1, 2}], [{1, 3}, {1, 3}]],
            [{1}, {1}]
    ),
    (
            [["a", "b"], ["c", "d"], ["e", "f"]],
            [[{1}, {1}], [{2, 3}, {2, 3}], [{1, 3}, {1, 3}]],
            [set(), set()]
    )
])
def test_concat_simple(col_names, plaintext_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0], plaintext_sets[0])
    cols_in_two = _create_cols(col_names[1], plaintext_sets[1])
    cols_in_three = _create_cols(col_names[2], plaintext_sets[2])

    rel_one = create("in1", cols_in_one, {1, 2, 3})
    rel_two = create("in2", cols_in_two, {1, 2, 3})
    rel_three = create("in3", cols_in_three, {1, 2, 3})

    cc = concat([rel_one, rel_two, rel_three], "concat", col_names[0])
    collect(cc, {1, 2, 3})

    out_cols = cc.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            [["a", "b"], ["c", "d"], ["e", "f"]],
            [[set(), set()], [set(), set()], [set(), set()]],
            [set(), set()]
    ),
    (
            [["a", "b"], ["c", "d"], ["e", "f"]],
            [[{1}, {1}], [{2}, {2}], [{3}, {3}]],
            [set(), set()]
    ),
    (
            [["a", "b"], ["c", "d"], ["e", "f"]],
            [[{1}, {1}], [{1, 2}, {1, 2}], [{1, 3}, {1, 3}]],
            [{1}, {1}]
    ),
    (
            [["a", "b"], ["c", "d"], ["e", "f"]],
            [[{1}, {1}], [{2, 3}, {2, 3}], [{1, 3}, {1, 3}]],
            [set(), set()]
    )
])
def test_concat_agg(col_names, plaintext_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0], plaintext_sets[0])
    cols_in_two = _create_cols(col_names[1], plaintext_sets[1])
    cols_in_three = _create_cols(col_names[2], plaintext_sets[2])

    rel_one = create("in1", cols_in_one, {1, 2, 3})
    rel_two = create("in2", cols_in_two, {1, 2, 3})
    rel_three = create("in3", cols_in_three, {1, 2, 3})

    cc = concat([rel_one, rel_two, rel_three], "concat", col_names[0])
    agg = aggregate(cc, "agg", [col_names[0][0]], col_names[0][1], "sum")
    collect(agg, {1, 2, 3})

    out_cols = agg.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            [["a", "b"], ["c", "d"]],
            [[set(), set()], [set(), set()]],
            [set(), set()]
    ),
    (
            [["a", "b"], ["c", "d"]],
            [[{1, 2, 3}, set()], [{1, 2, 3}, set()]],
            [{1, 2, 3}, set()]
    ),
    (
            [["a", "b"], ["c", "d"]],
            [[set(), {1, 2, 3}], [{1, 2, 3}, set()]],
            [set(), set()]
    ),
    (
            [["a", "b"], ["c", "d"]],
            [[{1}, {2}], [{1}, set()]],
            [{1}, set()]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1, 2}, {3}, {2}], [{1, 2}, {1, 2}, {1}]],
            [{1, 2}, set()]
    )
])
def test_concat_project(col_names, plaintext_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0], plaintext_sets[0])
    cols_in_two = _create_cols(col_names[1], plaintext_sets[1])

    rel_one = create("in1", cols_in_one, {1, 2, 3})
    rel_two = create("in2", cols_in_two, {1, 2, 3})

    cc = concat([rel_one, rel_two], "concat", col_names[0])
    p = project(cc, "proj", [col_names[0][0], col_names[0][1]])
    collect(p, {1, 2, 3})

    out_cols = p.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[set(), set(), set()], [set(), set(), set()]],
            [set(), set(), set()]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1}, {1}, {1}], [{2}, {2}, {2}]],
            [set(), set(), set()]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1}, {1}, {1}], [{1, 2}, {1, 2}, {1, 2}]],
            [{1}, {1}, {1}]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1, 2}, {1, 2}, {1, 2}], [{1, 2}, {1, 2}, {1, 2}]],
            [{1, 2}, {1, 2}, {1, 2}]
    ),
])
def test_concat_mult(col_names, plaintext_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0], plaintext_sets[0])
    cols_in_two = _create_cols(col_names[1], plaintext_sets[1])

    rel_one = create("in1", cols_in_one, {1, 2, 3})
    rel_two = create("in2", cols_in_two, {1, 2, 3})

    cc = concat([rel_one, rel_two], "concat", col_names[0])
    m = multiply(cc, "mult", col_names[0][0], [3, col_names[0][1], col_names[0][2]])
    collect(m, {1, 2, 3})

    out_cols = m.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[set(), set(), set()], [set(), set(), set()]],
            [set(), set(), set()]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1}, {1}, {1}], [{2}, {2}, {2}]],
            [set(), set(), set()]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1}, {1}, {1}], [{1, 2}, {1, 2}, {1, 2}]],
            [{1}, {1}, {1}]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1, 2}, {1, 2}, {1, 2}], [{1, 2}, {1, 2}, {1, 2}]],
            [{1, 2}, {1, 2}, {1, 2}]
    ),
])
def test_concat_div(col_names, plaintext_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0], plaintext_sets[0])
    cols_in_two = _create_cols(col_names[1], plaintext_sets[1])

    rel_one = create("in1", cols_in_one, {1, 2, 3})
    rel_two = create("in2", cols_in_two, {1, 2, 3})

    cc = concat([rel_one, rel_two], "concat", col_names[0])
    d = divide(cc, "mult", col_names[0][0], [3, col_names[0][1], col_names[0][2]])
    collect(d, {1, 2, 3})

    out_cols = d.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[set(), set(), set()], [set(), set(), set()]],
            [set(), set(), set()]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1}, {1}, {1}], [{2}, {2}, {2}]],
            [set(), set(), set()]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1}, {1}, {1}], [{1, 2}, {1, 2}, {1, 2}]],
            [{1}, {1}, {1}]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1, 2}, {1, 2}, {1, 2}], [{1, 2}, {1, 2}, {1, 2}]],
            [{1, 2}, {1, 2}, {1, 2}]
    ),
])
def test_concat_limit(col_names, plaintext_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0], plaintext_sets[0])
    cols_in_two = _create_cols(col_names[1], plaintext_sets[1])

    rel_one = create("in1", cols_in_one, {1, 2, 3})
    rel_two = create("in2", cols_in_two, {1, 2, 3})

    cc = concat([rel_one, rel_two], "concat", col_names[0])
    l = limit(cc, "lim", 100)
    collect(l, {1, 2, 3})

    out_cols = l.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[set(), set(), set()], [set(), set(), set()]],
            [set(), set(), set()]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1}, {1}, {1}], [{2}, {2}, {2}]],
            [set(), set(), set()]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1}, {1}, {1}], [{1, 2}, {1, 2}, {1, 2}]],
            [{1}, {1}, {1}]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1, 2}, {1, 2}, {1, 2}], [{1, 2}, {1, 2}, {1, 2}]],
            [{1, 2}, {1, 2}, {1, 2}]
    ),
])
def test_concat_distinct(col_names, plaintext_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0], plaintext_sets[0])
    cols_in_two = _create_cols(col_names[1], plaintext_sets[1])

    rel_one = create("in1", cols_in_one, {1, 2, 3})
    rel_two = create("in2", cols_in_two, {1, 2, 3})

    cc = concat([rel_one, rel_two], "concat", col_names[0])
    d = distinct(cc, "dis", [col_names[0][1]])
    collect(d, {1, 2, 3})

    out_cols = d.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[set(), set(), set()], [set(), set(), set()]],
            [set(), set(), set()]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1}, {1}, {1}], [{2}, {2}, {2}]],
            [set(), set(), set()]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1}, {1}, {1}], [{1, 2}, {1, 2}, {1, 2}]],
            [{1}, {1}, {1}]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1, 2}, {1, 2}, {1, 2}], [{1, 2}, {1, 2}, {1, 2}]],
            [{1, 2}, {1, 2}, {1, 2}]
    ),
])
def test_concat_filter_by(col_names, plaintext_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0], plaintext_sets[0])
    cols_in_two = _create_cols(col_names[1], plaintext_sets[1])

    rel_one = create("in1", cols_in_one, {1, 2, 3})
    rel_two = create("in2", cols_in_two, {1, 2, 3})

    cc = concat([rel_one, rel_two], "concat", col_names[0])
    f = filter_by(cc, "filt", col_names[0][0], "==", col_names[0][1])
    collect(f, {1, 2, 3})

    out_cols = f.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[set(), set(), set()], [set(), set(), set()]],
            [set(), set(), set()]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1}, {1}, {1}], [{2}, {2}, {2}]],
            [set(), set(), set()]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1}, {1}, {1}], [{1, 2}, {1, 2}, {1, 2}]],
            [{1}, {1}, {1}]
    ),
    (
            [["a", "b", "c"], ["d", "e", "f"]],
            [[{1, 2}, {1, 2}, {1, 2}], [{1, 2}, {1, 2}, {1, 2}]],
            [{1, 2}, {1, 2}, {1, 2}]
    ),
])
def test_concat_sort_by(col_names, plaintext_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0], plaintext_sets[0])
    cols_in_two = _create_cols(col_names[1], plaintext_sets[1])

    rel_one = create("in1", cols_in_one, {1, 2, 3})
    rel_two = create("in2", cols_in_two, {1, 2, 3})

    cc = concat([rel_one, rel_two], "concat", col_names[0])
    s = sort_by(cc, "sort", col_names[0][0])
    collect(s, {1, 2, 3})

    out_cols = s.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]


@pytest.mark.parametrize("col_names, plaintext_sets, expected_sets", [
    (
            [["a", "b"], ["c", "d"], ["e", "f"]],
            [[set(), set()], [set(), set()], [set(), set()]],
            [set(), set(), set()]
    ),
    (
            [["a", "b"], ["c", "d"], ["e", "f"]],
            [[{1}, {1}], [{2}, {2}], [{3}, {3}]],
            [set(), set(), {3}]
    ),
    (
            [["a", "b"], ["c", "d"], ["e", "f"]],
            [[{1}, {1}], [{1, 2}, {1, 2}], [{1, 3}, {1, 3}]],
            [{1}, {1}, {1, 3}]
    ),
    (
            [["a", "b"], ["c", "d"], ["e", "f"]],
            [[{1}, {1}], [{2, 3}, {2, 3}], [{1, 3}, {1, 3}]],
            [set(), set(), {1, 3}]
    ),
    (
            [["a", "b"], ["c", "d"], ["e", "f"]],
            [[{1}, {1}], [{1, 2, 3}, {2, 3}], [set(), set()]],
            [set(), set(), set()]
    ),
    (
            [["a", "b"], ["c", "d"], ["e", "f"]],
            [[{1}, {1}], [{1, 2}, {2, 3}], [{1}, set()]],
            [{1}, set(), set()]
    )
])
def test_concat_join(col_names, plaintext_sets, expected_sets):

    cols_in_one = _create_cols(col_names[0], plaintext_sets[0])
    cols_in_two = _create_cols(col_names[1], plaintext_sets[1])
    cols_in_three = _create_cols(col_names[2], plaintext_sets[2])

    rel_one = create("in1", cols_in_one, {1, 2, 3})
    rel_two = create("in2", cols_in_two, {1, 2, 3})
    rel_three = create("in3", cols_in_three, {1, 2, 3})

    cc = concat([rel_one, rel_two], "concat", col_names[0])
    j = join(cc, rel_three, "join", [cc.out_rel.columns[0].name], [rel_three.out_rel.columns[0].name])
    collect(j, {1, 2, 3})

    out_cols = j.out_rel.columns
    for i in range(len(out_cols)):
        assert out_cols[i].plaintext == expected_sets[i]



