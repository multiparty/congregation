

def write_rel(output_path: str, rel: list, header: list, use_floats: [bool, None] = False):

    print(f"Writing python job output to {output_path}")
    with open(output_path, "w") as f:

        f.write(f"{','.join(header)}\n")
        if not use_floats:
            rows_formatted = [",".join(str(int(v)) for v in row) for row in rel]
        else:
            rows_formatted = [",".join(str(float(v)) for v in row) for row in rel]
        f.write("\n".join(r for r in rows_formatted))


def read_rel(input_path: str, use_floats: [bool, None] = False):

    rows = []
    print(f"Python reading input from {input_path}")
    with open(input_path, "r") as f:
        itr = iter(f.readlines())
        header = next(itr)
        print(f"Skipping header: {header}")
        for row in itr:
            try:
                if not use_floats:
                    rows.append([int(float(v)) for v in row.split(",")])
                else:
                    rows.append([float(v) for v in row.split(",")])
            except ValueError:
                # skip header row
                typ_str = "float" if use_floats else "int"
                print(f"Encountered an invalid value for {typ_str} conversion in the following row: {row}")
                pass
    return rows





def construct_acc_dict(
        rel: list,
        group_cols: list,
        agg_col: [int, None],
        include_sum: bool = False,
        include_count: bool = False,
        include_values: bool = False
):
    """
    construct a dictionary whose keys are the group columns expressed
    as a tuple, and whose values are a list of the values from the
    aggregated column for each group
    """

    acc = {}
    for row in rel:

        k = tuple(row[idx] for idx in group_cols)
        if k in acc:

            if include_sum:
                acc[k]["__SUM__"] += row[agg_col]
            if include_count:
                acc[k]["__COUNT__"] += 1
            if include_values:
                acc[k]["__VALUES__"].append(row[agg_col])
        else:

            entry = {}
            if include_sum:
                entry["__SUM__"] = row[agg_col]
            if include_count:
                entry["__COUNT__"] = 1
            if include_values:
                entry["__VALUES__"] = [row[agg_col]]

            acc[k] = entry

    return acc
