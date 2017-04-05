import pytest

import storage

@pytest.mark.storage_filler_marker(
    storage_env_file='storage_env.json',
    storage_diff={}
)

def test_storage_object(storage_filler):
    storage_json = storage_filler

    assert 13 in storage.groups
    assert 14 in storage.groups
    assert 15 in storage.groups

    group13 = storage.groups[13]
    c = group13.couple
    assert c
    for g in c.groups:
        assert g.group_id in [13, 14, 15]
        nb = g.node_backends[0]
        assert nb.group == g.group_id

    assert c.namespace == "default"



