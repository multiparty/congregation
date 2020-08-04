

class Relation:
    def __init__(self, name: str, columns: list, stored_with: list):
        self.name = name
        self.columns = columns
        self.stored_with = self._resolve_stored_with(stored_with)

    def __str__(self):

        col_str = "".join([str(col) for col in self.columns])
        stored_with_str = ", ".join([str(sw) for sw in self.stored_with])
        return f"NAME: {self.name}\n" \
               f"STORED WITH: {stored_with_str}\n" \
               f"COLUMNS: {col_str}\n"

    def _resolve_stored_with(self, stored_with):

        if not self.check_stored_with_nonempty(stored_with):
            raise Exception(f"Can't pass empty stored_with set to relation {self.name}.")

        if isinstance(stored_with, set):
            return [stored_with]
        elif isinstance(stored_with, list):
            if self.get_stored_with_lens(stored_with):
                return stored_with
            else:
                raise Exception(
                    f"All input stored_with sets must be of same length. "
                    f"Can't initialize relation {self.name} with stored_with sets {stored_with}."
                )
        else:
            raise Exception(
                f"WARN: Unrecognized type for other stored_with set(s) argument: {type(stored_with)}."
            )

    @staticmethod
    def check_stored_with_nonempty(stored_with):

        if isinstance(stored_with, set):
            if len(stored_with) == 0:
                return False
        elif isinstance(stored_with, list):
            for sw in stored_with:
                if len(sw) == 0:
                    return False
        else:
            raise Exception(
                f"WARN: Unrecognized type for other stored_with set(s) argument: {type(stored_with)}."
            )
        return True

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

    def get_stored_with_lens(self, stored_with: list):
        """
        Check for illegal stored_with set merging before creating a relation.

        Overview: Make sure every stored_with set between all relations is of the same
        length. This is to ensure something like the following doesn't happen:

        rel1.stored_with = [{1,2,3}]
        rel2.stored_with = [{4,5,6,7}]

        These two relations can't be combined because the secret sharing schemes between
        the two would (I assume) be incompatible.

        TODO: Can there be overlap between the stored_with parties? This will depend on
         limitations within which MPC backend is being used.
        """

        lens = set([len(sw) for sw in stored_with])

        if len(lens) > 1:
            return False
        return True

    def merge_stored_with(self, other: [set, list]):

        if isinstance(other, set):
            if self.get_stored_with_lens([other] + self.stored_with):
                self.stored_with.append(other)
            else:
                raise Exception(
                    f"All input stored_with sets must be of same length."
                    f" Can't merge {other} into {self.stored_with}."
                )
        elif isinstance(other, list):
            if self.get_stored_with_lens(other + self.stored_with):
                self.stored_with.extend(other)
            else:
                raise Exception(
                    f"All input stored_with sets must be of same length."
                    f" Can't merge {other} into {self.stored_with}."
                )
        else:
            raise Exception(
                f"WARN: Unrecognized type for other stored_with set(s) argument: {type(other)}."
            )


