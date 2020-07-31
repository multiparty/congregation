

class Column:

    def __init__(
            self,
            rel_name: str,
            name: str,
            idx: int,
            type_str: str,
            trust_with: [set, None] = None,
            plaintext: [set, None] = None
    ):

        self.rel_name = rel_name
        self.name = name
        self.idx = idx
        self.type_str = self.check_type_str(type_str)
        self.trust_with = set() if trust_with is None else trust_with
        self.plaintext = set() if plaintext is None else plaintext

    def __str__(self):

        plaintext_str = " ".join(sorted([str(i) for i in self.plaintext]))
        trust_with_str = " ".join(sorted([str(i) for i in self.trust_with]))

        return f"\n" \
               f"--\n" \
               f"COLUMN: {self.name}\n" \
               f"IN RELATION: {self.rel_name}\n" \
               f"STORED IN PLAINTEXT BY: {plaintext_str}\n" \
               f"TRUSTED WITH: {trust_with_str}\n" \
               f"--\n"

    @staticmethod
    def check_type_str(type_str):

        ok_types = ["INTEGER", "FLOAT", "STRING"]
        if type_str not in ok_types:
            raise Exception(f"Type not supported {type_str}.")
        return type_str

    def merge_trust_sets_in(self, other_trust_sets: [set, list]):

        if isinstance(other_trust_sets, list):
            self.trust_with = self.trust_with.intersection(*other_trust_sets)
        elif isinstance(other_trust_sets, set):
            self.trust_with = self.trust_with.intersection(other_trust_sets)
        else:
            raise Exception(
                f"WARN: Unrecognized type for other_coll_sets argument: {type(other_trust_sets)}."
            )


