import copy


def min_set(all_sets: list):

    if not all_sets:
        return set()

    if len(all_sets) > 1:
        return all_sets[0].intersection(*all_sets[1:])
    else:
        return all_sets[0]


def max_set(all_sets: list):

    if not all_sets:
        return set()

    if len(all_sets) > 1:
        return set().union(*all_sets)
    else:
        return all_sets[0]


def min_trust_with_from_cols(columns: list):

    all_trust_sets = [copy.copy(col.trust_with) for col in columns if col.trust_with is not None]
    return min_set(all_trust_sets)


def max_trust_with_from_cols(columns: list):

    all_trust_sets = [copy.copy(col.trust_with) for col in columns if col.trust_with is not None]
    return max_set(all_trust_sets)


def min_pt_set_from_cols(columns: list):

    all_pt_sets = [copy.copy(col.plaintext) for col in columns]
    return min_set(all_pt_sets)


def max_pt_set_from_cols(columns: list):

    all_pt_sets = [copy.copy(col.plaintext) for col in columns]
    return max_set(all_pt_sets)


def infer_output_type(columns: list):

    all_types = set([col.type_str for col in columns])

    if len(all_types) == 1:
        return all_types.pop()
    if len(all_types) == 2:
        if "INTEGER" in all_types and "FLOAT" in all_types:
            return "FLOAT"

    raise Exception(f"Can't infer output type from the following: {all_types}.")


def find(columns: list, col_name: str):

    try:
        return next(iter([col for col in columns if col.name == col_name]))
    except StopIteration:
        return None


def build_operands_from_in_rel(in_rel, operands):
    ret = []
    for op in operands:
        if isinstance(op, str):
            col = find(in_rel.columns, op)
            if col is None:
                raise Exception(
                    f"Column with name {op} not found in relation {in_rel.name}."
                )
            else:
                ret.append(col)
        else:
            ret.append(op)

    return ret


def check_cols_for_missing_entries(cols: list, rel_name: str):

    for col in cols:
        if col is None:
            raise Exception(f"Join column from relation {rel_name} not found.")


def create_column(name: str, type_str: str, trust_set: [set, None] = None, plaintext_set: [set, None] = None):

    if isinstance(trust_set, dict):
        raise Exception("Input trust set must be SET, not dict.")
    if isinstance(plaintext_set, dict):
        raise Exception("Input plaintext set must be SET, not dict")

    return name, type_str, trust_set, plaintext_set


def construct_group_cols(in_cols: list, group_col_names: [list, None]):

    if group_col_names is None:
        in_group_cols = []
    else:
        in_group_cols = sorted(
            [find(in_cols, group_col_name) for group_col_name in group_col_names],
            key=lambda c: c.idx
        )

    out_group_cols = [copy.deepcopy(group_col) for group_col in in_group_cols]
    min_pt = min_pt_set_from_cols(out_group_cols)
    min_ts = min_trust_with_from_cols(out_group_cols)

    for gc in out_group_cols:
        gc.plaintext = min_pt
        gc.trust_with = min_ts

    return in_group_cols, out_group_cols


def construct_target_col(
        in_cols: list,
        target_col_name: str,
        group_cols: list,
        out_target_col_name: [str, None] = None
):

    target_col = find(in_cols, target_col_name)
    out_target_col = copy.deepcopy(target_col)

    if out_target_col_name is not None:
        out_target_col.name = out_target_col_name

    min_pt = min_pt_set_from_cols(group_cols + [out_target_col])
    min_ts = min_trust_with_from_cols(group_cols + [out_target_col])

    out_target_col.plaintext = min_pt
    out_target_col.trust_with = min_ts

    return out_target_col

