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

#include <FS.h>
#include <Node.h>
#include <Storage.h>

#include <StorageUpdater.h>

#include <gtest/gtest.h>

TEST(Filesystem, StatusOK)
{
    // Test filesystem in state OK.
    // It is OK when FS total_space >= sum of backend total spaces.
    // Samples include filesystems with total space > sum and == sum,
    // having 0, 1, or 2 backends.
    //
    // Filesystems with greater total space:
    // FS 1: 1 backend (11)
    // FS 2: 2 backends (21, 22)
    // Filesystems with equal total space:
    // FS 3: 1 backend (31)
    // FS 4: 2 backends (41, 42)

    const char *json = R"END(
    {
        "backends": {
            "2001:db8:0:1111::11:1025:10/11": {
                "group": 1,
                "state": 1,
                "blob_size_limit": 21001,
                "fsid": 1
            },
            "2001:db8:0:1111::11:1025:10/21": {
                "group": 2,
                "state": 1,
                "blob_size_limit": 31013,
                "fsid": 2
            },
            "2001:db8:0:1111::11:1025:10/22": {
                "group": 3,
                "state": 1,
                "blob_size_limit": 32003,
                "fsid": 2
            },
            "2001:db8:0:1111::11:1025:10/31": {
                "group": 4,
                "state": 1,
                "blob_size_limit": 409600,
                "fsid": 3
            },
            "2001:db8:0:1111::11:1025:10/41": {
                "group": 5,
                "state": 1,
                "blob_size_limit": 167936,
                "fsid": 4
            },
            "2001:db8:0:1111::11:1025:10/42": {
                "group": 6,
                "state": 1,
                "blob_size_limit": 241664,
                "fsid": 4
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
            },
            "2001:db8:0:1111::11:1025:10/3": {
                "vfs": {
                    "blocks": 100,
                    "bsize": 4096
                }
            },
            "2001:db8:0:1111::11:1025:10/4": {
                "vfs": {
                    "blocks": 100,
                    "bsize": 4096
                }
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
    auto & filesystems = node.get_filesystems();

    ASSERT_EQ(4, filesystems.size());

    auto it = filesystems.find(1);
    ASSERT_FALSE(it == filesystems.end());
    EXPECT_EQ(FS::OK, it->second.get_status());

    it = filesystems.find(2);
    ASSERT_FALSE(it == filesystems.end());
    EXPECT_EQ(FS::OK, it->second.get_status());

    it = filesystems.find(3);
    ASSERT_FALSE(it == filesystems.end());
    EXPECT_EQ(FS::OK, it->second.get_status());

    it = filesystems.find(4);
    ASSERT_FALSE(it == filesystems.end());
    EXPECT_EQ(FS::OK, it->second.get_status());
}

TEST(Filesystem, StatusBROKEN)
{
    // Test filesystem in state BROKEN.
    // It is BROKEN when FS total_space < sum of backend total spaces.
    //
    // Sample filesystems:
    // FS 1: 1 backend (11)
    // FS 2: 2 backends (21, 22)

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

    StorageSnapshot snapshot;
    snapshot.update(json);
    snapshot.complete();

    Storage storage;

    StorageUpdater updater(storage, snapshot);
    updater.update_all();

    ASSERT_EQ(1, storage.get_nodes().size());

    Node & node = storage.get_nodes().begin()->second;
    auto & filesystems = node.get_filesystems();

    ASSERT_EQ(2, filesystems.size());

    auto it = filesystems.find(1);
    ASSERT_FALSE(it == filesystems.end());
    EXPECT_EQ(FS::BROKEN, it->second.get_status());

    it = filesystems.find(2);
    ASSERT_FALSE(it == filesystems.end());
    EXPECT_EQ(FS::BROKEN, it->second.get_status());
}
