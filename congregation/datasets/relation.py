

class Relation:
    def __init__(self, name: str, columns: list, stored_with: list):
        self.name = name
        self.columns = columns
        self.stored_with = stored_with

    def __str__(self):

        col_str = "".join([str(col) for col in self.columns])
        stored_with_str = ", ".join([str(sw) for sw in self.stored_with])
        return f"NAME: {self.name}\n" \
               f"STORED WITH: {stored_with_str}\n" \
               f"COLUMNS: {col_str}\n"

    def rename(self, new_name: str):

        self.name = new_name
        for col in self.columns:
            col.rel_name = new_name

    def represent_cols(self):

        ret = {}
        for idx, col in enumerate(self.columns):
            ret[idx] = {col}

        return ret

    def is_local(self):
        """
        Returns whether there exists a plaintext copy of all data in
        this relation at some single party. Does not indicate whether
        multiple parties have plaintext copies of the data in this relation.
        """

        if len(self.columns) > 1:
            all_pt = [col.plaintext for col in self.columns]
            common_pt = all_pt[0].intersection(*all_pt[1:])
            return len(common_pt) > 0
        else:
            return len(self.columns[0].plaintext) > 0

    def is_shared(self):
        return not self.is_local()

    def update_column_indexes(self):

        for idx, col in enumerate(self.columns):
            col.idx = idx

    def update_columns(self):

        self.update_column_indexes()
        for col in self.columns:
            col.rel_name = self.name
