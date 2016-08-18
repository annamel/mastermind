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

#include <Backend.h>
#include <Storage.h>

#include <StorageUpdater.h>
#include <TestUtil.h>

#include <gtest/gtest.h>

TEST(Backend, StatusOK)
{
    // Check backend status OK.
    // Enabled, Up-to-date, Read-Write.

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/1": {
                "group": 1,
                "state": 1,
                "read_only": false,
                "fsid": 1125798601
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    ASSERT_EQ(1, storage.get_nodes().size());

    Node & node = storage.get_nodes().begin()->second;
    ASSERT_EQ(1, node.get_backends().size());

    Backend & backend = node.get_backends().begin()->second;
    EXPECT_EQ(Backend::OK, backend.get_status());
}

TEST(Backend, StatusRO)
{
    // Check backend status RO.
    // Enabled, Up-to-date, Read-Only.

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/1": {
                "group": 1,
                "state": 1,
                "read_only": true,
                "fsid": 103948711
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    ASSERT_EQ(1, storage.get_nodes().size());

    Node & node = storage.get_nodes().begin()->second;
    ASSERT_EQ(1, node.get_backends().size());

    Backend & backend = node.get_backends().begin()->second;
    EXPECT_EQ(Backend::RO, backend.get_status());
}

TEST(Backend, StatusOK2RO)
{
    // Check backend state change from OK to RO.
    // Init: Enabled, Up-to-date, Read-Write.
    // Update: Enabled, Up-to-date, Read-Only.

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/1": {
                "group": 1,
                "state": 1,
                "read_only": false,
                "fsid": 1991409923
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

    ASSERT_EQ(1, storage.get_nodes().size());

    Node & node = storage.get_nodes().begin()->second;
    ASSERT_EQ(1, node.get_backends().size());

    Backend & backend = node.get_backends().begin()->second;
    EXPECT_EQ(Backend::OK, backend.get_status());

    const char *json_ro = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/1": {
                "read_only": true
            }
        }
    }
    )END";

    snapshot.update(json_ro);
    updater.update_all();

    // State must become RO.
    EXPECT_EQ(Backend::RO, backend.get_status());
}

TEST(Backend, StaleStatistics)
{
    // Check backend status STALLED.
    // Enabled, Stale statistics, Read-Write.

    const char *json = R"END(
    {
        "timestamp": {
            "tv_sec": 597933449,
            "tv_usec": 439063
        },
        "backends": {
            "2001:db8:0:1111::11:1025:10/1": {
                "group": 1,
                "state": 1,
                "read_only": false,
                "fsid": 103948711
            }
        }
    }
    )END";

    // + ~1s
    set_test_clock(597933450, 239567);

    Storage storage = StorageUpdater::create(json);

    ASSERT_EQ(1, storage.get_nodes().size());

    Node & node = storage.get_nodes().begin()->second;
    ASSERT_EQ(1, node.get_backends().size());

    // Now it must be OK.
    Backend & backend = node.get_backends().begin()->second;
    EXPECT_EQ(Backend::OK, backend.get_status());

    // + ~10min
    set_test_clock(597934067, 757201);

    // Update storage status. Backend must turn into state STALLED.
    storage.process_node_backends();
    storage.update();
    EXPECT_EQ(Backend::STALLED, backend.get_status());

    // Restore timer.
    set_test_clock(0, 0);
}

TEST(Backend, NotEnabled)
{
    // Check backend status STALLED.
    // Disabled, Up-to-date, Read-Write.

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/1": {
                "group": 1,
                "state": 1,
                "read_only": false,
                "fsid": 1246592323
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

    ASSERT_EQ(1, storage.get_nodes().size());

    Node & node = storage.get_nodes().begin()->second;
    ASSERT_EQ(1, node.get_backends().size());

    // Now it must be OK.
    Backend & backend = node.get_backends().begin()->second;
    EXPECT_EQ(Backend::OK, backend.get_status());

    // Disable backend.
    const char *json_disabled = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/1": {
                "state": 0
            }
        }
    }
    )END";

    snapshot.update(json_disabled);
    updater.update_all();

    // State must become STALLED.
    EXPECT_EQ(Backend::STALLED, backend.get_status());
}

TEST(Backend, BlobSizeLimit)
{
    // Backend::Calculated::total_space must contain value of
    // backend/config/blob_size_limit (if set).

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/1": {
                "group": 1,
                "state": 1,
                "blob_size_limit": 135211301,
                "fsid": 1246592323
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    ASSERT_EQ(1, storage.get_nodes().size());

    Node & node = storage.get_nodes().begin()->second;
    ASSERT_EQ(1, node.get_backends().size());

    // Now it must be OK.
    Backend & backend = node.get_backends().begin()->second;
    EXPECT_EQ(135211301, backend.get_calculated().total_space);
}

TEST(Backend, Broken)
{
    // Sum of backends' blob size limit is more than filesystem capacity.
    // All backends on the filesystem must be in state BROKEN.

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/11": {
                "group": 1,
                "state": 1,
                "blob_size_limit": 409709,
                "fsid": 1
            },
            "2001:db8:0:1111::11:1025:10/21": {
                "group": 2,
                "state": 1,
                "blob_size_limit": 409517,
                "fsid": 2
            },
            "2001:db8:0:1111::11:1025:10/22": {
                "group": 3,
                "state": 1,
                "blob_size_limit": 4096,
                "fsid": 2
            }
        },
        "filesystems": {
            "2001:db8:0:1111::11:1025:10/1": {
                "vfs": {
                    "blocks": 100,
                    "bsize": 4096
                }
            },
            "2001:db8:0:1111::11:1025:10/2": {
                "vfs": {
                    "blocks": 100,
                    "bsize": 4096
                }
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    auto & nodes = storage.get_nodes();
    ASSERT_EQ(1, nodes.size());

    auto & backends = nodes.begin()->second.get_backends();
    ASSERT_EQ(3, backends.size());

    auto it = backends.find(11);
    ASSERT_FALSE(it == backends.end());
    EXPECT_EQ(Backend::BROKEN, it->second.get_status());

    it = backends.find(21);
    ASSERT_FALSE(it == backends.end());
    EXPECT_EQ(Backend::BROKEN, it->second.get_status());

    it = backends.find(22);
    ASSERT_FALSE(it == backends.end());
    EXPECT_EQ(Backend::BROKEN, it->second.get_status());
}
