

def all_rels_have_equal_num_cols(in_rels: list):

    num_cols = len(in_rels[0].columns)
    for in_rel in in_rels:
        if len(in_rel.columns) != num_cols:
            return False

    return True


def resolve_plaintext_sets_from_rels(in_rels: list):

    if not all_rels_have_equal_num_cols(in_rels):
        raise Exception(
            f"The following input relations do not have an equal number of columns: "
            f"{[in_rel.name for in_rel in in_rels]}"
        )

    ret = []
    num_cols = len(in_rels[0].columns)

    for i in range(num_cols):
        cur_pt_set = set()
        for in_rel in in_rels:
            cur_pt_set = cur_pt_set.intersection(in_rel.columns[i].plaintext)
        ret.append(cur_pt_set)

    return ret


def resolve_trust_sets_from_rels(in_rels: list):

    if not all_rels_have_equal_num_cols(in_rels):
        raise Exception(
            f"The following input relations do not have an equal number of columns: "
            f"{[in_rel.name for in_rel in in_rels]}"
        )

    ret = []
    num_cols = len(in_rels[0].columns)

    for i in range(num_cols):
        cur_trust_set = set()
        for in_rel in in_rels:
            cur_trust_set = cur_trust_set.intersection(in_rel.columns[i].trust_with)
        ret.append(cur_trust_set)

    return ret
