

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
