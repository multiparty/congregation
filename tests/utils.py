from congregation.utils import create_column


def create_cols(party_data):

    ret = []
    for i in range(len(party_data["col_names"])):
        ret.append(
            create_column(
                party_data["col_names"][i],
                "INTEGER",
                party_data["trust_with_sets"][i],
                party_data["plaintext_sets"][i]
            )
        )

    return ret


def compare_to_expected(d, expected):

    zip_node_order = zip(d.top_sort(), expected["node_order"])
    node_order_checks = [isinstance(z[0], z[1]) for z in zip_node_order]
    assert all(node_order_checks)

    zip_requires_mpc = zip(d.top_sort(), expected["requires_mpc"])
    requires_mpc_checks = [z[0].requires_mpc() == z[1] for z in zip_requires_mpc]
    assert all(requires_mpc_checks)

    zip_stored_with = zip(d.top_sort(), [e["stored_with"] for e in expected["ownership_data"]])
    ownership_checks = [z[0].out_rel.stored_with == z[1] for z in zip_stored_with]
    assert all(ownership_checks)

    zip_plaintext = zip(d.top_sort(), [e["plaintext_sets"] for e in expected["ownership_data"]])
    plaintext_checks = [[c.plaintext for c in z[0].out_rel.columns] == z[1] for z in zip_plaintext]
    assert all(plaintext_checks)

    zip_trust_with = zip(d.top_sort(), [e["trust_with_sets"] for e in expected["ownership_data"]])
    trust_with_checks = [[c.trust_with for c in z[0].out_rel.columns] == z[1] for z in zip_trust_with]
    assert all(trust_with_checks)
