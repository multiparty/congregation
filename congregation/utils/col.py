import copy


def _min_set(all_sets: list):
    return all_sets[0].intersection(*all_sets[1:])


def min_trust_with_from_columns(columns: list):

    all_trust_sets = [copy.copy(col.trust_with) for col in columns if col.trust_with is not None]
    return _min_set(all_trust_sets)


def min_pt_set_from_cols(columns: list):

    all_pt_sets = [copy.copy(col.plaintext) for col in columns]
    return _min_set(all_pt_sets)


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
