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

#include <Group.h>

#include <StorageUpdater.h>
#include <TestUtil.h>

#include <msgpack.hpp>

#include <gtest/gtest.h>

TEST(Group, Ctor)
{
    // This test checks initialization of Group object.

    Group g(0);

    g.~Group();
    // Write junk bytes and check members initialization afterwards.
    memset(&g, 0x5a, sizeof(g));
    new (&g) Group(113);

    EXPECT_EQ(113, g.get_id());
    EXPECT_TRUE(g.get_backends().empty());
    EXPECT_EQ(0, g.get_update_time());
    EXPECT_FALSE(g.has_active_job());

    const Group::Metadata & md = g.get_metadata();
    EXPECT_EQ(0, md.version);
    EXPECT_FALSE(md.frozen);
    EXPECT_TRUE(md.couple.empty());
    EXPECT_TRUE(md.namespace_name.empty());
    EXPECT_TRUE(md.type.empty());
    EXPECT_FALSE(md.service.migrating);
    EXPECT_TRUE(md.service.job_id.empty());

    EXPECT_FALSE(g.metadata_parsed());
    EXPECT_EQ(0, g.get_metadata_parse_duration());

    EXPECT_EQ(Group::DATA, g.get_type());
    EXPECT_EQ(Group::INIT, g.get_status());
}

TEST(Group, ParseMetadataV1)
{
    // This test checks metadata parsing for version 1 metadata
    // which is msgpack:ed array of groups in couple.

    // Prepare metadata.

    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(&buffer);
    packer.pack_array(3);
    packer.pack(17);
    packer.pack(19);
    packer.pack(23);

    // Create group and parse metadata.

    Group g(17);
    g.save_metadata(buffer.data(), buffer.size(), ::time(nullptr)*1000000000ULL);
    ASSERT_TRUE(g.parse_metadata());
    EXPECT_TRUE(g.metadata_parsed());

    // Check metadata values.

    const Group::Metadata & md = g.get_metadata();
    EXPECT_EQ(1, md.version);
    EXPECT_FALSE(md.frozen);
    EXPECT_EQ(std::vector<int>({17, 19, 23}), md.couple);
    EXPECT_EQ("default", md.namespace_name);
    EXPECT_TRUE(md.type.empty());
    EXPECT_FALSE(md.service.migrating);
    EXPECT_TRUE(md.service.job_id.empty());

    // Check if type and status weren't affected.

    g.calculate_type();

    EXPECT_EQ(Group::DATA, g.get_type());
    EXPECT_EQ(Group::INIT, g.get_status());

    // Check that other values didn't change.

    EXPECT_EQ(17, g.get_id());
    EXPECT_TRUE(g.get_backends().empty());
    EXPECT_FALSE(g.has_active_job());
}

TEST(Group, ParseMetadataV2)
{
    // This test checks metadata parsing for version 2
    // metadata which is msgpack:ed map.

    // Prepare metadata.

    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(&buffer);
    packer.pack_map(6);

    packer.pack(std::string("version"));
    packer.pack(2);

    packer.pack(std::string("frozen"));
    packer.pack(true);

    packer.pack(std::string("couple"));
    packer.pack_array(3);
    packer.pack(29);
    packer.pack(31);
    packer.pack(37);

    packer.pack(std::string("namespace"));
    packer.pack(std::string("storage"));

    packer.pack(std::string("type"));
    packer.pack(std::string("cache"));

    packer.pack(std::string("service"));
    packer.pack_map(2);
    packer.pack(std::string("status"));
    packer.pack(std::string("MIGRATING"));
    packer.pack(std::string(std::string("job_id")));
    packer.pack(std::string("12345"));

    // Create group and parse metadata.

    Group g(29);
    g.save_metadata(buffer.data(), buffer.size(), ::time(nullptr)*1000000000ULL);
    ASSERT_TRUE(g.parse_metadata());
    EXPECT_TRUE(g.metadata_parsed());

    // Check metadata values.

    const Group::Metadata & md = g.get_metadata();
    EXPECT_EQ(2, md.version);
    EXPECT_TRUE(md.frozen);
    EXPECT_EQ(std::vector<int>({29, 31, 37}), md.couple);
    EXPECT_EQ("storage", md.namespace_name);
    EXPECT_EQ("cache", md.type);
    EXPECT_TRUE(md.service.migrating);
    EXPECT_EQ("12345", md.service.job_id);

    // Check whether the type is taken into account.

    g.calculate_type();

    EXPECT_EQ(Group::CACHE, g.get_type());
    EXPECT_EQ(Group::INIT, g.get_status());

    // Check that other values didn't change.

    EXPECT_EQ(29, g.get_id());
    EXPECT_TRUE(g.get_backends().empty());
    EXPECT_FALSE(g.has_active_job());
}

TEST(Group, StatusINITNoBackends)
{
    // Group 2 has no backends and must be in state INIT.

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
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    const auto & groups = storage.get_groups();
    ASSERT_EQ(2, groups.size());

    auto it = groups.find(2);
    ASSERT_FALSE(it == groups.end());

    const Group & group = it->second;
    EXPECT_EQ(Group::INIT, group.get_status());
}

TEST(Group, StatusBROKENForbiddenDHT)
{
    // Group 1 has two backends but DHT groups are forbidden.
    // State must be BROKEN.

    ConfigGuard<uint64_t> dht_guard(app::test_config().forbidden_dht_groups);

    app::test_config().forbidden_dht_groups = 1;

    const char *json = R"END(
    {
        "groups": {
            "1": {
                "backends": [
                    "2001:db8:0:1111::11:1025:10/101",
                    "2001:db8:0:1117::11:1025:10/211"
                ]
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    const auto & groups = storage.get_groups();
    ASSERT_EQ(1, groups.size());

    const Group & group = groups.begin()->second;
    EXPECT_EQ(Group::BROKEN, group.get_status());
}

TEST(Group, StatusINITNoMetadata)
{
    // Metadata for group 1 was not read. Group must be in state INIT.

    const char *json = R"END(
    {
        "groups": {
            "1": {
                "backends": [
                    "2001:db8:0:1111::11:1025:10/101"
                ]
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    const auto & groups = storage.get_groups();
    ASSERT_EQ(1, groups.size());

    const Group & group = groups.begin()->second;
    EXPECT_EQ(Group::INIT, group.get_status());
}

TEST(Group, BrokenBackends)
{
    // Group has broken backends. Group's state must be BROKEN.

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/11": {
                "group": 1,
                "state": 1,
                "blob_size_limit": 409709,
                "fsid": 1
            }
        },
        "filesystems": {
            "2001:db8:0:1111::11:1025:10/1": {
                "vfs": {
                    "blocks": 100,
                    "bsize": 4096
                }
            }
        },
        "groups": {
            "1": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 7 ],
                    "namespace": "default"
                }
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    const auto & groups = storage.get_groups();
    ASSERT_EQ(2, groups.size());

    auto it = groups.find(1);
    ASSERT_FALSE(it == groups.end());

    const Group & group = it->second;
    EXPECT_EQ(Group::BROKEN, group.get_status());
}

TEST(Group, CacheGroupOK)
{
    // Group is a cache group, all backends are OK.
    // Group's state must be COUPLED.

    const char *json = R"END(
    {
        "groups": {
            "13": {
                "metadata": {
                    "version": 2,
                    "couple": [ 13 ],
                    "namespace": "storage_cache",
                    "type": "cache"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1009"
                ]
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    auto & groups = storage.get_groups();
    ASSERT_EQ(1, groups.size());

    auto it = groups.find(13);
    ASSERT_FALSE(it == groups.end());

    EXPECT_EQ(Group::COUPLED, it->second.get_status());
}

TEST(Group, CacheGroupBAD)
{
    // Cache group has stalled backend. State must be BAD.

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
                    "couple": [ 13 ],
                    "namespace": "storage_cache",
                    "type": "cache"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1009"
                ]
            }
        }
    }
    )END";

    // + ~1s
    set_test_clock(597933450, 239567);

    Storage storage = StorageUpdater::create(json);

    // + ~10min
    set_test_clock(597934067, 757201);

    // Update storage status. Backend must turn into state STALLED.
    storage.process_node_backends();
    storage.update();

    auto & groups = storage.get_groups();
    auto it = groups.find(13);
    ASSERT_FALSE(it == groups.end());

    EXPECT_EQ(Group::BAD, it->second.get_status());

    // Restore timer.
    set_test_clock(0, 0);
}

TEST(Group, CacheGroupRO)
{
    // Cache group has RO backends, but no SERVICE_MIGRATING in metadata.
    // State must be RO.

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/907": {
                "group": 911,
                "state": 1,
                "read_only": true,
                "fsid": 1242422443
            }
        },
        "groups": {
            "911": {
                "metadata": {
                    "version": 2,
                    "couple": [ 907 ],
                    "namespace": "storage_cache",
                    "type": "cache"
                }
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    auto & groups = storage.get_groups();
    auto it = groups.find(911);
    ASSERT_FALSE(it == groups.end());

    EXPECT_EQ(Group::RO, it->second.get_status());
}

TEST(Group, CacheGroupMigrating)
{
    // Cache group has RO backend, SERVICE_MIGRATING is set, a job is active.
    // State must be MIGRATING.

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/907": {
                "group": 911,
                "state": 1,
                "read_only": true,
                "fsid": 1242422443
            }
        },
        "groups": {
            "911": {
                "metadata": {
                    "version": 2,
                    "couple": [ 907 ],
                    "namespace": "storage_cache",
                    "type": "cache",
                    "service": {
                        "migrating": true,
                        "job_id": "f1c33865"
                    }
                }
            }
        },
        "jobs": {
            "entries": [
                {
                    "id": "f1c33865",
                    "group": 911,
                    "status": "executing",
                    "type": "move_job"
                }
            ]
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    auto & groups = storage.get_groups();
    auto it = groups.find(911);
    ASSERT_FALSE(it == groups.end());

    EXPECT_EQ(Group::MIGRATING, it->second.get_status());
}

TEST(Group, CacheGroupBadActiveJob)
{
    // Cache group has RO backend, SERVICE_MIGRATING is set,
    // a job is active but has different job_id.
    // State must be BAD.

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/907": {
                "group": 911,
                "state": 1,
                "read_only": true,
                "fsid": 1242422443
            }
        },
        "groups": {
            "911": {
                "metadata": {
                    "version": 2,
                    "couple": [ 907 ],
                    "namespace": "storage_cache",
                    "type": "cache",
                    "service": {
                        "migrating": true,
                        "job_id": "f1c33865"
                    }
                }
            }
        },
        "jobs": {
            "entries": [
                {
                    "id": "0161e342",
                    "group": 911,
                    "status": "executing",
                    "type": "move_job"
                }
            ]
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    auto & groups = storage.get_groups();
    auto it = groups.find(911);
    ASSERT_FALSE(it == groups.end());

    EXPECT_EQ(Group::BAD, it->second.get_status());
}

TEST(Group, CacheGroupBadNoJob)
{
    // Cache group has RO backend, SERVICE_MIGRATING is set,
    // but there is no active job.
    // State must be BAD.

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/907": {
                "group": 911,
                "state": 1,
                "read_only": true,
                "fsid": 1242422443
            }
        },
        "groups": {
            "911": {
                "metadata": {
                    "version": 2,
                    "couple": [ 907 ],
                    "namespace": "storage_cache",
                    "type": "cache",
                    "service": {
                        "migrating": true,
                        "job_id": "f1c33865"
                    }
                }
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    auto & groups = storage.get_groups();
    auto it = groups.find(911);
    ASSERT_FALSE(it == groups.end());

    EXPECT_EQ(Group::BAD, it->second.get_status());
}

TEST(Group, InitNoCouple)
{
    // Group has no 'couple' in metadata. State must be INIT.

    const char *json = R"END(
    {
        "groups": {
            "13591": {
                "metadata": {
                    "version": 2,
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/16871"
                ]
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    auto & groups = storage.get_groups();
    auto it = groups.find(13591);
    ASSERT_FALSE(it == groups.end());

    EXPECT_EQ(Group::INIT, it->second.get_status());
}

TEST(Group, NotInCouple)
{
    // Group metadata contains couple which already exists and doesn't
    // contain it. State must be BAD.

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

    const char *update = R"END(
    {
        "groups": {
            "4": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2, 3 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1133::b:1025:10/109"
                ]
            }
        }
    }
    )END";

    snapshot.update(update);
    snapshot.complete();

    updater.update_all();

    auto & groups = storage.get_groups();
    ASSERT_EQ(4, groups.size());

    auto it = groups.find(4);
    ASSERT_FALSE(it == groups.end());

    EXPECT_EQ(Group::BAD, it->second.get_status());
}

TEST(Group, DifferentCoupleSet)
{
    // A group in couple has different 'couple' set in metadata.
    // States of all groups in the couple must be BAD.

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
                    "couple": [ 1, 3, 4 ],
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

    auto & groups = storage.get_groups();

    auto it = groups.find(1);
    ASSERT_FALSE(it == groups.end());
    EXPECT_EQ(Group::BAD, it->second.get_status());

    it = groups.find(2);
    ASSERT_FALSE(it == groups.end());
    EXPECT_EQ(Group::BAD, it->second.get_status());

    it = groups.find(3);
    ASSERT_FALSE(it == groups.end());
    EXPECT_EQ(Group::BAD, it->second.get_status());
}

TEST(Group, EmptyNamespace)
{
    // Group has no 'namespace' value in metadata. State must be BAD.

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
                    "couple": [ 1, 2 ]
                },
                "backends": [
                    "2001:db8:0:1122::14:1025:10/103"
                ]
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    auto & groups = storage.get_groups();

    auto it = groups.find(2);
    ASSERT_FALSE(it == groups.end());
    EXPECT_EQ(Group::BAD, it->second.get_status());
}

#if 0

TEST(Group, NoIdInMetadata)
{
    // Group doesn't have its own id in metadata's couple.
    // State must be BROKEN.

    const char *json = R"END(
    {
        "groups": {
            "337": {
                "metadata": {
                    "version": 2,
                    "couple": [ 347 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/101"
                ]
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    auto & groups = storage.get_groups();

    auto it = groups.find(337);
    ASSERT_FALSE(it == groups.end());
    EXPECT_EQ(Group::BROKEN, it->second.get_status());
}

#endif

TEST(Group, GroupOK)
{
    // All group's backends are OK. Group's state must be COUPLED.

    const char *json = R"END(
    {
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

    Storage storage = StorageUpdater::create(json);

    auto & groups = storage.get_groups();
    ASSERT_EQ(2, groups.size());

    auto it = groups.find(13);
    ASSERT_FALSE(it == groups.end());

    EXPECT_EQ(Group::COUPLED, it->second.get_status());
}

TEST(Group, GroupBAD)
{
    // Group has stalled backend. State must be BAD.

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

    Storage storage = StorageUpdater::create(json);

    // + ~10min
    set_test_clock(597934067, 757201);

    // Update storage status. Backend must turn into state STALLED.
    storage.process_node_backends();
    storage.update();

    auto & groups = storage.get_groups();
    auto it = groups.find(13);
    ASSERT_FALSE(it == groups.end());

    EXPECT_EQ(Group::BAD, it->second.get_status());

    // Restore timer.
    set_test_clock(0, 0);
}

TEST(Group, GroupRO)
{
    // Group has RO backends, but no SERVICE_MIGRATING in metadata.
    // State must be RO.

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/1009": {
                "group": 13,
                "state": 1,
                "read_only": true,
                "fsid": 1242422443
            }
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

    Storage storage = StorageUpdater::create(json);

    auto & groups = storage.get_groups();
    auto it = groups.find(13);
    ASSERT_FALSE(it == groups.end());

    EXPECT_EQ(Group::RO, it->second.get_status());
}

TEST(Group, GroupMigrating)
{
    // Group has RO backend, SERVICE_MIGRATING is set, a job is active.
    // State must be MIGRATING.

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/1009": {
                "group": 13,
                "state": 1,
                "read_only": true,
                "fsid": 1242422443
            }
        },
        "groups": {
            "13": {
                "metadata": {
                    "version": 2,
                    "couple": [ 13, 15 ],
                    "namespace": "default",
                    "service": {
                        "migrating": true,
                        "job_id": "f1c33865"
                    }
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
        },
        "jobs": {
            "entries": [
                {
                    "id": "f1c33865",
                    "group": 13,
                    "status": "executing",
                    "type": "move_job"
                }
            ]
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    auto & groups = storage.get_groups();
    auto it = groups.find(13);
    ASSERT_FALSE(it == groups.end());

    EXPECT_EQ(Group::MIGRATING, it->second.get_status());
}

TEST(Group, GroupBadActiveJob)
{
    // Group has RO backend, SERVICE_MIGRATING is set,
    // a job is active but has different job_id.
    // State must be BAD.

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/1009": {
                "group": 13,
                "state": 1,
                "read_only": true,
                "fsid": 1242422443
            }
        },
        "groups": {
            "13": {
                "metadata": {
                    "version": 2,
                    "couple": [ 13, 15 ],
                    "namespace": "default",
                    "service": {
                        "migrating": true,
                        "job_id": "f1c33865"
                    }
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
        },
        "jobs": {
            "entries": [
                {
                    "id": "27940dce",
                    "group": 13,
                    "status": "executing",
                    "type": "move_job"
                }
            ]
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    auto & groups = storage.get_groups();
    auto it = groups.find(13);
    ASSERT_FALSE(it == groups.end());

    EXPECT_EQ(Group::BAD, it->second.get_status());
}

TEST(Group, GroupBadNoJob)
{
    // Group has RO backend, SERVICE_MIGRATING is set,
    // but there is no active job.
    // State must be BAD.

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/1009": {
                "group": 13,
                "state": 1,
                "read_only": true,
                "fsid": 1242422443
            }
        },
        "groups": {
            "13": {
                "metadata": {
                    "version": 2,
                    "couple": [ 13, 15 ],
                    "namespace": "default",
                    "service": {
                        "migrating": true,
                        "job_id": "f1c33865"
                    }
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

    Storage storage = StorageUpdater::create(json);

    auto & groups = storage.get_groups();
    auto it = groups.find(13);
    ASSERT_FALSE(it == groups.end());

    EXPECT_EQ(Group::BAD, it->second.get_status());
}
