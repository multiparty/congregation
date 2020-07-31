

class Relation:
    def __init__(self, name: str, columns: list, stored_with: set):
        self.name = name
        self.columns = columns
        self.stored_with = stored_with

    def __str__(self):

        col_str = "".join([str(col) for col in self.columns])
        return f"NAME: {self.name}\n" \
               f"STORED WITH: {self.stored_with}\n" \
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
        this relation at one or more parties. Does not indicate whether
        multiple parties have plaintext copies of the data in this relation.
        """
        return all([col.plaintext for col in self.columns])

    def is_shared(self):
        return not self.is_local()

    def update_column_indexes(self):

        for idx, col in enumerate(self.columns):
            col.idx = idx

    def update_columns(self):

        self.update_column_indexes()
        for col in self.columns:
            col.rel_name = self.name
