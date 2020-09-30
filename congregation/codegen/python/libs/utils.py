

def write_rel(output_dir: str, rel_name: str, rel: list, header: list):

    print(f"Writing python job output to {output_dir}/{rel_name}")
    with open(f"{output_dir}{rel_name}.csv", "w") as f:
        f.write(f"{','.join(header)}\n")
        rows_formatted = [",".join(str(v) for v in row) for row in rel]
        f.write("\n".join(r for r in rows_formatted))


def read_rel(path_to_rel: str):

    rows = []
    with open(path_to_rel, "r") as f:
        itr = iter(f.readlines())
        for row in itr:
            try:
                # TODO: not necessarily ints we're working with
                rows.append([int(v) for v in row.split(",")])
            except ValueError:
                # skip header row
                pass
    return rows