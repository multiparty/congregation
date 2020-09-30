

def join(left_rel: list, right_rel: list, left_join_cols: list, right_join_cols: list):

    ret = []
    for lrow in left_rel:
        lvals = [lrow[i] for i in left_join_cols]
        non_key_lvals = [lrow[i] for i in range(len(lrow)) if i not in left_join_cols]
        for rrow in right_rel:
            rvals = [rrow[i] for i in right_join_cols]
            if lvals == rvals:
                non_key_rvals = [rrow[i] for i in range(len(rrow)) if i not in right_join_cols]
                new_row = lvals + non_key_lvals + non_key_rvals
                ret.append(new_row)
    return ret
