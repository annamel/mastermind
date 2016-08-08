/*
   Copyright (c) YANDEX LLC, 2015. All rights reserved.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 3.0 of the License, or (at your option) any later version.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with this library.
*/

#include <GroupHistoryEntry.h>
#include <Storage.h>
#include <StorageUpdater.h>

#include <mongo/bson/bson.h>
#include <mongo/db/json.h>

#include <gtest/gtest.h>

#define BACKEND1_KEY "2001:db8:0:1122::14:1025:10/1"
#define BACKEND2_KEY "2001:db8:0:1122::15:1025:10/1"
#define BACKEND3_KEY "2001:db8:0:1122::15:1025:10/3"
#define BACKEND4_KEY "2001:db8:0:1122::14:1025:10/4"

typedef std::vector<std::string> vs;

namespace {

void check_group_backends(Storage & storage, const std::vector<std::string> & expected_keys)
{
    ASSERT_EQ(1, storage.get_groups().size());

    Group & group = storage.get_groups().begin()->second;
    EXPECT_EQ(1, group.get_id());

    auto & backends = group.get_backends();
    ASSERT_EQ(expected_keys.size(), group.get_backends().size());

    std::vector<std::string> backend_keys;
    for (auto & backend : backends)
        backend_keys.push_back(backend.get().get_key());
    std::sort(backend_keys.begin(), backend_keys.end());

    EXPECT_EQ(expected_keys, backend_keys);
}

} // unnamed namespace

TEST(GroupHistoryEntry, EmptyHistory)
{
    // Construct GroupHistoryEntry from BSON object with
    // non-empty group id and empty nodes.

    const char *json = R"END(
    {
        "group_id": 17,
        "nodes": []
    }
    )END";

    mongo::BSONObj obj = mongo::fromjson(json);

    try {
        GroupHistoryEntry entry(obj);
        EXPECT_EQ(17, entry.get_group_id());
        EXPECT_EQ(true, entry.get_backends().empty());
        EXPECT_EQ(0.0, entry.get_timestamp());
        EXPECT_EQ(true, entry.empty());
    } catch (std::exception & e) {
        FAIL() << e.what();
    } catch (...) {
        FAIL() << "Unknown exception thrown";
    }
}

TEST(GroupHistoryEntry, NoGroupId)
{
    // Try to construct GroupHistoryEntry from BSON object
    // without group_id. Exception must be thrown.

    const char *json = "{ \"nodes\": [] }";

    mongo::BSONObj obj = mongo::fromjson(json);

    try {
        GroupHistoryEntry entry(obj);
        FAIL() << "No exception thrown";
    } catch (std::exception & e) {
    } catch (...) {
        FAIL() << "Unknown exception thrown";
    }
}

TEST(GroupHistoryEntry, OneBackend)
{
    // Construct GroupHistoryEntry from BSON with a single
    // complete backend description

    const char *json = R"END(
    {
        "group_id": 29,
        "nodes": [
            {
                "timestamp": 1449240697,
                "type": "manual",
                "set": [
                    {
                        "path": "/path/to/storage/1/2/",
                        "backend_id": 31,
                        "hostname": "node1.example.com",
                        "port": 1025,
                        "family": 10
                    }
                ]
            }
        ]
    }
    )END";

    mongo::BSONObj obj = mongo::fromjson(json);

    try {
        GroupHistoryEntry entry(obj);
        EXPECT_EQ(29, entry.get_group_id());
        EXPECT_EQ(1449240697.0, entry.get_timestamp());
        EXPECT_EQ(0, entry.empty());
        ASSERT_EQ(1, entry.get_backends().size());

        auto & backend = *entry.get_backends().begin();
        EXPECT_EQ("node1.example.com", std::get<0>(backend));
        EXPECT_EQ(1025, std::get<1>(backend));
        EXPECT_EQ(10, std::get<2>(backend));
        EXPECT_EQ(31, std::get<3>(backend));
    } catch (std::exception & e) {
        FAIL() << e.what();
    } catch (...) {
        FAIL() << "Unknown exception thrown";
    }
}

TEST(GroupHistory, NoChanges)
{
    // Verify applying of history database entry with set of backends unchanged.

    const char *init_json = R"END(
    {
        "hosts": {
            "2001:db8:0:1122::14": {
                "name": "node1.example.com"
            },
            "2001:db8:0:1122::15": {
                "name": "node2.example.com"
            }
        },
        "groups": {
            "1": {
                "backends": [
                    "2001:db8:0:1122::14:1025:10/1",
                    "2001:db8:0:1122::15:1025:10/1"
                ]
            }
        }
    }
    )END";

    StorageSnapshot snapshot(init_json);

    Storage storage;

    // Check initial state.
    StorageUpdater updater(storage, snapshot);
    updater.update_all();

    check_group_backends(storage, vs({BACKEND1_KEY, BACKEND2_KEY}));

    return;

    // Apply update.

    const char *no_changes = R"END(
    {
        "history": {
            "entries": [
                {
                    "group_id": 1,
                    "nodes": [
                        {
                            "timestamp": 1449841652,
                            "type": "manual",
                            "set": [
                                {
                                    "path": "/path/to/storage/1/1",
                                    "backend_id": 1,
                                    "hostname": "node1.example.com",
                                    "port": 1025,
                                    "family": 10
                                },
                                {
                                    "path": "/path/to/storage/1/1",
                                    "backend_id": 1,
                                    "hostname": "node2.example.com",
                                    "port": 1025,
                                    "family": 10
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    }
    )END";

    snapshot.update(no_changes);
    updater.update_all();

    check_group_backends(storage, vs({BACKEND1_KEY, BACKEND2_KEY}));
}

#if 0

TEST(GroupHistory, PlusNewBackend)
{
    // Apply entry with the same set of backends plus new backends. Nothing must change.

    const char *init_json = R"END(
    {
        "hosts": {
            "2001:db8:0:1122::14": {
                "name": "node1.example.com"
            },
            "2001:db8:0:1122::15": {
                "name": "node2.example.com"
            }
        },
        "groups": {
            "1": {
                "backends": [
                    "2001:db8:0:1122::14:1025:10/1",
                    "2001:db8:0:1122::15:1025:10/1"
                ]
            }
        }
    }
    )END";

    StorageSnapshot snapshot(init_json);

    Storage storage;

    // Check initial state.
    StorageUpdater updater(storage, snapshot);
    updater.update_all();

    check_group_backends(storage, vs({BACKEND1_KEY, BACKEND2_KEY}));

    // Apply update.

    const char *plus_new = R"END(
    {
        "groups": {
            "1": {
                "backends": [
                    "2001:db8:0:1122::14:1025:10/1",
                    "2001:db8:0:1122::15:1025:10/1",
                    "2001:db8:0:1122::15:1025:10/3"
                ]
            }
        },
        "history": {
            "entries": [
                {
                    "group_id": 1,
                    "nodes": [
                        {
                            "timestamp": 1449841663,
                            "type": "job",
                            "set": [
                                {
                                    "path": "/path/to/storage/1/1",
                                    "backend_id": 1,
                                    "hostname": "node1.example.com",
                                    "port": 1025,
                                    "family": 10
                                },
                                {
                                    "path": "/path/to/storage/1/1",
                                    "backend_id": 1,
                                    "hostname": "node2.example.com",
                                    "port": 1025,
                                    "family": 10
                                },
                                {
                                    "path": "/path/to/storage/3/3",
                                    "backend_id": 3,
                                    "hostname": "node2.example.com",
                                    "port": 1025,
                                    "family": 10
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    }
    )END";

    snapshot.update(plus_new);
    snapshot.complete();
    updater.update_all();

    check_group_backends(storage, vs({BACKEND1_KEY, BACKEND2_KEY, BACKEND3_KEY}));
}

TEST(GroupHistory, RemoveOneBackend)
{
    // Verify applying of history database entry with one backend removed.

    const char *init_json = R"END(
    {
        "hosts": {
            "2001:db8:0:1122::14": {
                "name": "node1.example.com"
            },
            "2001:db8:0:1122::15": {
                "name": "node2.example.com"
            }
        },
        "groups": {
            "1": {
                "backends": [
                    "2001:db8:0:1122::14:1025:10/1",
                    "2001:db8:0:1122::15:1025:10/1"
                ]
            }
        }
    }
    )END";

    StorageSnapshot snapshot(init_json);

    Storage storage;

    // Check initial state.
    StorageUpdater updater(storage, snapshot);
    updater.update_all();

    check_group_backends(storage, vs({BACKEND1_KEY, BACKEND2_KEY}));

    // Remove one backend.

    const char *one_removed = R"END(
    {
        "backends": {
            "2001:db8:0:1122::15:1025:10/1": null
        },
        "history": {
            "entries": [
                {
                    "group_id": 1,
                    "nodes": [
                        {
                            "timestamp": 1449841652,
                            "type": "manual",
                            "set": [
                                {
                                    "path": "/path/to/storage/1/1",
                                    "backend_id": 1,
                                    "hostname": "node1.example.com",
                                    "port": 1025,
                                    "family": 10
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    }
    )END";

    snapshot.update(one_removed);
    updater.update_all();

    check_group_backends(storage, vs({BACKEND1_KEY}));
}

TEST(GroupHistory, AllRemoved)
{
    // Verify applying of history database entry with empty set of backends.

    const char *init_json = R"END(
    {
        "hosts": {
            "2001:db8:0:1122::14": {
                "name": "node1.example.com"
            },
            "2001:db8:0:1122::15": {
                "name": "node2.example.com"
            }
        },
        "groups": {
            "1": {
                "backends": [
                    "2001:db8:0:1122::14:1025:10/1",
                    "2001:db8:0:1122::15:1025:10/1"
                ]
            }
        }
    }
    )END";

    StorageSnapshot snapshot(init_json);

    Storage storage;

    // Check initial state.
    StorageUpdater updater(storage, snapshot);
    updater.update_all();

    check_group_backends(storage, vs({BACKEND1_KEY, BACKEND2_KEY}));

    // Remove all.

    const char *empty_set = R"END(
    {
        "backends": {
            "2001:db8:0:1122::14:1025:10/1": null,
            "2001:db8:0:1122::15:1025:10/1": null
        },
        "history": {
            "entries": [
                {
                    "group_id": 1,
                    "nodes": [
                        {
                            "timestamp": 1449841653,
                            "type": "manual",
                            "set": [ ]
                        }
                    ]
                }
            ]
        }
    }
    )END";

    snapshot.update(empty_set);
    updater.update_all();

    check_group_backends(storage, vs());
}

TEST(GroupHistory, DifferentSet)
{
    // Apply history entry with totally different set of backends.
    // These backends are not yet received from monitor stats.
    // List of group's backends must become empty.

    const char *init_json = R"END(
    {
        "hosts": {
            "2001:db8:0:1122::14": {
                "name": "node1.example.com"
            },
            "2001:db8:0:1122::15": {
                "name": "node2.example.com"
            }
        },
        "groups": {
            "1": {
                "backends": [
                    "2001:db8:0:1122::14:1025:10/1",
                    "2001:db8:0:1122::15:1025:10/1"
                ]
            }
        }
    }
    )END";

    StorageSnapshot snapshot(init_json);

    Storage storage;

    // Check initial state.
    StorageUpdater updater(storage, snapshot);
    updater.update_all();

    check_group_backends(storage, vs({BACKEND1_KEY, BACKEND2_KEY}));

    // Remove all.

    const char *different_set = R"END(
    {
        "backends": {
            "2001:db8:0:1122::14:1025:10/1": null,
            "2001:db8:0:1122::15:1025:10/1": null
        },
        "history": {
            "entries": [
                {
                    "group_id": 1,
                    "nodes": [
                        {
                            "timestamp": 1450179193,
                            "type": "manual",
                            "set": [
                                {
                                    "path": "/path/to/storage/3/3",
                                    "backend_id": 3,
                                    "hostname": "node2.example.com",
                                    "port": 1025,
                                    "family": 10
                                },
                                {
                                    "path": "/path/to/storage/4/4",
                                    "backend_id": 4,
                                    "hostname": "node1.example.com",
                                    "port": 1025,
                                    "family": 10
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    }
    )END";

    snapshot.update(different_set);
    updater.update_all();

    check_group_backends(storage, vs());
}

#endif
