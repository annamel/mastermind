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

#include <Couple.h>
#include <Group.h>
#include <Storage.h>

#include <StorageUpdater.h>
#include <TestUtil.h>

#include <gtest/gtest.h>

TEST(Couple, Creation)
{
    const char *json = R"END(
    {
        "groups": {
            "1": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2, 3 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/101"
                ]
            },
            "2": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2, 3 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1122::14:1025:10/103"
                ]
            },
            "3": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2, 3 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1133::b:1025:10/107"
                ]
            }
        }
    }
    )END";

    StorageSnapshot snapshot;
    snapshot.update(json);
    snapshot.complete();

    Storage storage;

    StorageUpdater updater(storage, snapshot);
    updater.update_all();

    EXPECT_EQ(3, storage.get_groups().size());
    EXPECT_EQ(1, storage.get_couples().size());

    const Couple & couple = storage.get_couples().begin()->second;
    EXPECT_EQ("1:2:3", couple.get_key());
    EXPECT_EQ(3, couple.get_groups().size());
}

TEST(Couple, SingleGroup)
{
    const char *json = R"END(
    {
        "groups": {
            "1": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1"
                ]
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    EXPECT_EQ(1, storage.get_couples().size());

    const Couple & couple = storage.get_couples().begin()->second;
    EXPECT_EQ("1", couple.get_key());
    EXPECT_EQ(1, couple.get_groups().size());
}

TEST(Couple, BADNoMetadata)
{
    // Couple has a group with no metadata. Status must be BAD.

    const char *json = R"END(
    {
        "groups": {
            "1": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/101"
                ]
            },
            "2": {
                "backends": [
                    "2001:db8:0:1122::14:1025:10/103"
                ]
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    EXPECT_EQ(1, storage.get_couples().size());

    const Couple & couple = storage.get_couples().begin()->second;
    EXPECT_EQ(Couple::BAD, couple.get_status());
}

TEST(Couple, OtherTypeJob)
{
    // One of groups has no metadata, couple has an active job which is neither
    // of type 'move' nor 'restore'. State must be BAD.

    const char *json = R"END(
    {
        "groups": {
            "1": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/101"
                ]
            },
            "2": {
                "backends": [
                    "2001:db8:0:1122::14:1025:10/103"
                ]
            },
            "3": {
                "metadata": {
                    "version": 2,
                    "couple": [ 3, 4 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/107"
                ]
            },
            "4": {
                "backends": [
                    "2001:db8:0:1122::14:1025:10/109"
                ]
            }
        },
        "jobs": {
            "entries": [
                {
                    "id": "fe783944",
                    "group": 2,
                    "status": "executing",
                    "type": "recover_dc_job"
                },
                {
                    "id": "7765f194",
                    "group": 4,
                    "status": "executing",
                    "type": "couple_defrag_job"
                }
            ]
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    auto & couples = storage.get_couples();

    auto it = couples.find("1:2");
    ASSERT_FALSE(it == couples.end());
    EXPECT_EQ(Couple::BAD, it->second.get_status());

    it = couples.find("3:4");
    ASSERT_FALSE(it == couples.end());
    EXPECT_EQ(Couple::BAD, it->second.get_status());
}

TEST(Couple, Service)
{
    // This test checks statuses SERVICE_STALLED and SERVICE_ACTIVE.

    const char *json = R"END(
    {
        "groups": {
            "1": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/101"
                ]
            },
            "2": {
                "backends": [
                    "2001:db8:0:1122::14:1025:10/103"
                ]
            },
            "3": {
                "metadata": {
                    "version": 2,
                    "couple": [ 3, 4 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/107"
                ]
            },
            "4": {
                "backends": [
                    "2001:db8:0:1122::14:1025:10/109"
                ]
            },
            "5": {
                "metadata": {
                    "version": 2,
                    "couple": [ 5, 6 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/113"
                ]
            },
            "6": {
                "backends": [
                    "2001:db8:0:1122::14:1025:10/127"
                ]
            },
            "7": {
                "metadata": {
                    "version": 2,
                    "couple": [ 7, 8 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/131"
                ]
            },
            "8": {
                "backends": [
                    "2001:db8:0:1122::14:1025:10/137"
                ]
            },
            "9": {
                "metadata": {
                    "version": 2,
                    "couple": [ 9, 10 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/139"
                ]
            },
            "10": {
                "backends": [
                    "2001:db8:0:1122::14:1025:10/149"
                ]
            }
        },
        "jobs": {
            "entries": [
                {
                    "id": "fe783944",
                    "group": 2,
                    "status": "new",
                    "type": "move_job"
                },
                {
                    "id": "7765f194",
                    "group": 4,
                    "status": "executing",
                    "type": "restore_group_job"
                },
                {
                    "id": "0863226f",
                    "group": 6,
                    "status": "pending",
                    "type": "move_job"
                },
                {
                    "id": "87d4982d",
                    "group": 8,
                    "status": "not_approved",
                    "type": "restore_group_job"
                },
                {
                    "id": "1717e74c",
                    "group": 10,
                    "status": "broken",
                    "type": "restore_group_job"
                }
            ]
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    auto & couples = storage.get_couples();

    auto it = couples.find("1:2");
    ASSERT_FALSE(it == couples.end());
    EXPECT_EQ(Couple::SERVICE_ACTIVE, it->second.get_status());

    it = couples.find("3:4");
    ASSERT_FALSE(it == couples.end());
    EXPECT_EQ(Couple::SERVICE_ACTIVE, it->second.get_status());

    it = couples.find("5:6");
    ASSERT_FALSE(it == couples.end());
    EXPECT_EQ(Couple::SERVICE_STALLED, it->second.get_status());

    it = couples.find("7:8");
    ASSERT_FALSE(it == couples.end());
    EXPECT_EQ(Couple::SERVICE_STALLED, it->second.get_status());

    it = couples.find("9:10");
    ASSERT_FALSE(it == couples.end());
    EXPECT_EQ(Couple::SERVICE_STALLED, it->second.get_status());
}

TEST(Couple, NamespaceNotMatch)
{
    // Groups have different namespaces. Couple must be in state BAD.

    const char *json = R"END(
    {
        "groups": {
            "1": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1381"
                ]
            },
            "2": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "storage"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1399"
                ]
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    EXPECT_EQ(1, storage.get_couples().size());

    const Couple & couple = storage.get_couples().begin()->second;
    EXPECT_EQ("1:2", couple.get_key());
    EXPECT_EQ(Couple::BAD, couple.get_status());
}

TEST(Couple, MetadataConflictBAD)
{
    // Groups have different metadata. There is no active job.
    // Couple must be in state BAD.

    const char *json = R"END(
    {
        "groups": {
            "1": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1409"
                ]
            },
            "2": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 19 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1423"
                ]
            },
            "3": {
                "metadata": {
                    "version": 2,
                    "couple": [ 3, 4 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1427"
                ]
            },
            "4": {
                "metadata": {
                    "version": 2,
                    "couple": [ 3, 4 ],
                    "namespace": "default",
                    "type": "cache"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1429"
                ]
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    auto & couples = storage.get_couples();

    auto it = couples.find("1:2");
    ASSERT_FALSE(it == couples.end());
    EXPECT_EQ(Couple::BAD, it->second.get_status());

    it = couples.find("3:4");
    ASSERT_FALSE(it == couples.end());
    EXPECT_EQ(Couple::BAD, it->second.get_status());
}

TEST(Couple, MetadataConflictJob)
{
    // Groups have different metadata. There is active migrating job.
    // Couple must be in state SERVICE_ACTIVE. Note that in this point
    // we don't retest account_job_in_status() (SERVICE_* test).

    const char *json = R"END(
    {
        "groups": {
            "1": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1409"
                ]
            },
            "2": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 19 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1423"
                ]
            },
            "3": {
                "metadata": {
                    "version": 2,
                    "couple": [ 3, 4 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1427"
                ]
            },
            "4": {
                "metadata": {
                    "version": 2,
                    "couple": [ 3, 4 ],
                    "namespace": "default",
                    "type": "cache"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1429"
                ]
            }
        },
        "jobs": {
            "entries": [
                {
                    "id": "fe783944",
                    "group": 1,
                    "status": "new",
                    "type": "move_job"
                },
                {
                    "id": "7765f194",
                    "group": 3,
                    "status": "executing",
                    "type": "restore_group_job"
                }
            ]
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    auto & couples = storage.get_couples();

    auto it = couples.find("1:2");
    ASSERT_FALSE(it == couples.end());
    EXPECT_EQ(Couple::SERVICE_ACTIVE, it->second.get_status());

    it = couples.find("3:4");
    ASSERT_FALSE(it == couples.end());
    EXPECT_EQ(Couple::SERVICE_ACTIVE, it->second.get_status());
}

TEST(Couple, Frozen)
{
    // One of couple's groups is frozen. Couple must be in state FROZEN.

    const char *json = R"END(
    {
        "groups": {
            "1": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/101"
                ]
            },
            "2": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "default",
                    "frozen": true
                },
                "backends": [
                    "2001:db8:0:1122::14:1025:10/103"
                ]
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    ASSERT_EQ(1, storage.get_couples().size());

    const Couple & couple = storage.get_couples().begin()->second;
    EXPECT_EQ(Couple::FROZEN, couple.get_status());
}

TEST(Couple, ForbiddenDCSharing)
{
    // Option 'forbidden_dc_sharing_among_groups' is set, couple has
    // two groups in the same DC. State must be BROKEN.

    ConfigGuard<uint64_t> opt(app::test_config().forbidden_dc_sharing_among_groups);
    app::test_config().forbidden_dc_sharing_among_groups = 1;

    const char *json = R"END(
    {
        "hosts": {
            "2001:db8:0:1111::11": {
                "name": "node01.example.net",
                "dc": "yelcho"
            },
            "2001:db8:0:1122::14": {
                "name": "node11.example.net",
                "dc": "palena"
            },
            "2001:db8:0:1133::b": {
                "name": "node12.example.net",
                "dc": "palena"
            }
        },
        "groups": {
            "1": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2, 3 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/101"
                ]
            },
            "2": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2, 3 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1122::14:1025:10/103"
                ]
            },
            "3": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2, 3 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1133::b:1025:10/107"
                ]
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    ASSERT_EQ(1, storage.get_couples().size());

    const Couple & couple = storage.get_couples().begin()->second;
    EXPECT_EQ(Couple::BROKEN, couple.get_status());
}

TEST(Couple, NSWithoutSettings)
{
    // Option 'forbidden_ns_without_settings' is set,
    // namespace has default settings. State must be BROKEN.

    ConfigGuard<uint64_t> opt(app::test_config().forbidden_ns_without_settings);
    app::test_config().forbidden_ns_without_settings = 1;

    const char *json = R"END(
    {
        "groups": {
            "1": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "storage"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1381"
                ]
            },
            "2": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "storage"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1399"
                ]
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    EXPECT_EQ(1, storage.get_couples().size());

    const Couple & couple = storage.get_couples().begin()->second;
    EXPECT_EQ(Couple::BROKEN, couple.get_status());
}

TEST(Couple, UnmatchedSpace)
{
    // Option 'forbidden_unmatched_group_total_space' is set,
    // total space differs for a pair of groups. State must be BROKEN.

    ConfigGuard<uint64_t> opt(app::test_config().forbidden_unmatched_group_total_space);
    app::test_config().forbidden_unmatched_group_total_space = 1;

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/1381": {
                "group": 1,
                "state": 1,
                "blob_size_limit": 32321,
                "fsid": 3118623887
            },
            "2001:db8:0:1111::11:1025:10/1399": {
                "group": 2,
                "state": 1,
                "blob_size_limit": 31627,
                "fsid": 157181539
            }
        },
        "groups": {
            "1": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "storage"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1381"
                ]
            },
            "2": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "storage"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1399"
                ]
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    EXPECT_EQ(1, storage.get_couples().size());

    const Couple & couple = storage.get_couples().begin()->second;
    EXPECT_EQ(Couple::BROKEN, couple.get_status());
}

TEST(Couple, Full)
{
    // One of backends has no free space. State must be FULL.

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/1381": {
                "group": 1,
                "state": 1,
                "blob_size_limit": 32321,
                "base_size": 32321,
                "fsid": 3118623887
            }
        },
        "groups": {
            "1": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "storage"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1381"
                ]
            },
            "2": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "storage"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1399"
                ]
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    EXPECT_EQ(1, storage.get_couples().size());

    const Couple & couple = storage.get_couples().begin()->second;
    EXPECT_EQ(Couple::FULL, couple.get_status());
}

TEST(Couple, StatusOK)
{
    const char *json = R"END(
    {
        "groups": {
            "1": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1381"
                ]
            },
            "2": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1399"
                ]
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    EXPECT_EQ(1, storage.get_couples().size());

    const Couple & couple = storage.get_couples().begin()->second;
    EXPECT_EQ(Couple::OK, couple.get_status());
}

TEST(Couple, BrokenGroup)
{
    // Couple contains broken group. State must be BROKEN.

    ConfigGuard<uint64_t> dht_guard(app::test_config().forbidden_dht_groups);
    app::test_config().forbidden_dht_groups = 1;

    const char *json = R"END(
    {
        "groups": {
            "1": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1381"
                ]
            },
            "2": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::13:1025:10/1399",
                    "2001:db8:0:1111::17:1025:10/1409"
                ]
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    EXPECT_EQ(1, storage.get_couples().size());

    const Couple & couple = storage.get_couples().begin()->second;
    EXPECT_EQ(Couple::BROKEN, couple.get_status());
}

TEST(Couple, GroupBadJob)
{
    // Group's backend is getting STALLED, couple becomes BAD.
    // Then a job is created, and couple turns into SERVICE_ACTIVE.

    const char *json = R"END(
    {
        "timestamp": {
            "tv_sec": 597933449,
            "tv_usec": 439063
        },
        "groups": {
            "13": {
                "metadata": {
                    "version": 2,
                    "couple": [ 13, 15 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1009"
                ]
            },
            "15": {
                "metadata": {
                    "version": 2,
                    "couple": [ 13, 15 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1013"
                ]
            }
        }
    }
    )END";

    // + ~1s
    set_test_clock(597933450, 239567);

    StorageSnapshot snapshot;
    snapshot.update(json);
    snapshot.complete();

    Storage storage;

    StorageUpdater updater(storage, snapshot);
    updater.update_all();

    // + ~10min
    set_test_clock(597934067, 757201);

    const char *remove_backend = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/1009": null
        }
    }
    )END";

    snapshot.update(remove_backend);
    updater.update_all();

    EXPECT_EQ(1, storage.get_couples().size());

    const Couple & couple = storage.get_couples().begin()->second;
    EXPECT_EQ(Couple::BAD, couple.get_status());

    // Create job.

    set_test_clock(597934163, 18859);

    const char *create_job = R"END(
    {
        "groups": {
            "13": {
                "metadata": {
                    "version": 2,
                    "couple": [ 13, 15 ],
                    "namespace": "default",
                    "service": {
                        "migrating": true,
                        "job_id": "f74409fb"
                    }
                }
            }
        },
        "jobs": {
            "entries": [
                {
                    "id": "f74409fb",
                    "group": 13,
                    "status": "new",
                    "type": "move_job"
                }
            ]
        }
    }
    )END";

    snapshot.update(create_job);
    updater.update_all();

    EXPECT_EQ(Couple::SERVICE_ACTIVE, couple.get_status());

    // Restore timer.
    set_test_clock(0, 0);
}

TEST(Couple, GroupBadROMigrating)
{
    // Couple 271:277 has one read-only group (271) with job metadata defined,
    // but no active job.
    // Couple 281:283 has one read-only group (281) without job metadata.
    // Couples must be in state BAD.
    // Update: move job in state 'new' is created for group 271, and move job
    // in state 'executing' is created for group 281, its job metadata is set.
    // Couples must turn into SERVICE_ACTIVE and SERVICE_STALLED.

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/4111": {
                "group": 271,
                "state": 1,
                "read_only": true,
                "fsid": 1242422443
            },
            "2001:db8:0:1111::11:1025:10/4129": {
                "group": 281,
                "state": 1,
                "read_only": true,
                "fsid": 67571269
            }
        },
        "groups": {
            "271": {
                "metadata": {
                    "version": 2,
                    "couple": [ 271, 277 ],
                    "namespace": "default",
                    "service": {
                        "migrating": true,
                        "job_id": "4ebb6284"
                    }
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/4111"
                ]
            },
            "277": {
                "metadata": {
                    "version": 2,
                    "couple": [ 271, 277 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::17:1025:10/4127"
                ]
            },
            "281": {
                "metadata": {
                    "version": 2,
                    "couple": [ 281, 283 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/4129"
                ]
            },
            "283": {
                "metadata": {
                    "version": 2,
                    "couple": [ 281, 283 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::17:1025:10/4133"
                ]
            }
        }
    }
    )END";

    StorageSnapshot snapshot;
    snapshot.update(json);
    snapshot.complete();

    Storage storage;

    StorageUpdater updater(storage, snapshot);
    updater.update_all();

    auto & couples = storage.get_couples();

    auto it = couples.find("271:277");
    ASSERT_FALSE(it == couples.end());
    EXPECT_EQ(Couple::BAD, it->second.get_status());

    it = couples.find("281:283");
    ASSERT_FALSE(it == couples.end());
    EXPECT_EQ(Couple::BAD, it->second.get_status());

    // Create jobs, update metadata for group 281.

    const char *create_jobs = R"END(
    {
        "groups": {
            "281": {
                "metadata": {
                    "version": 2,
                    "couple": [ 281, 283 ],
                    "namespace": "default",
                    "service": {
                        "migrating": true,
                        "job_id": "ee1c9851"
                    }
                }
            }
        },
        "jobs": {
            "entries": [
                {
                    "id": "4ebb6284",
                    "group": 271,
                    "status": "new",
                    "type": "move_job"
                },
                {
                    "id": "ee1c9851",
                    "group": 281,
                    "status": "pending",
                    "type": "move_job"
                }
            ]
        }
    }
    )END";

    snapshot.update(create_jobs);
    updater.update_all();

    it = couples.find("271:277");
    ASSERT_FALSE(it == couples.end());
    EXPECT_EQ(Couple::SERVICE_ACTIVE, it->second.get_status());

    it = couples.find("281:283");
    ASSERT_FALSE(it == couples.end());
    EXPECT_EQ(Couple::SERVICE_STALLED, it->second.get_status());
}
