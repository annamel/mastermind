/*
   Copyright (c) YANDEX LLC, 2015. All rights reserved.
   This file is part of Mastermind.

   Mastermind is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 3.0 of the License, or (at your option) any later version.

   Mastermind is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with Mastermind.
*/

#ifndef __94562dbe_210a_4cc2_83bb_389b3549224b
#define __94562dbe_210a_4cc2_83bb_389b3549224b

#include "StorageSnapshot.h"

#include <Storage.h>

#include <rapidjson/document.h>

class Group;

// StorageUpdater is used to fully construct Storage object from a layout
// represented by StorageSnapshot. Following steps will be performed:
// 1) Hosts will be passed to storage with call to get_host() (it is
//    normally done by Discovery).
// 2) Nodes will be added (Discovery does it).
// 3) All nodes will receive JSON in a format of monitor stats.
// 4) All groups will receive msgpack'ed metadata (Group::save_metadata).
// 5) Storage methods will be called in the same way as Round does it.
//
// So the result is the same as if all data would be received in the usual way.

class StorageUpdater
{
public:
    StorageUpdater(Storage & storage, StorageSnapshot & snapshot);

    // Pass hosts and nodes to Storage.
    void update_nodes();
    // Pass monitor_stats stuff.
    void update_monitor_stats();
    // Pass msgpacked metadata to Group objects (Round normally does it).
    void update_metadata();
    // Do update tasks normally performed by Round.
    void update();

    // Perform all actions above.
    void update_all();

    static Storage create(const char *json)
    {
        StorageSnapshot snapshot;
        snapshot.update(json);
        snapshot.complete();

        Storage storage;

        StorageUpdater updater(storage, snapshot);
        updater.update_all();

        return storage;
    }

private:
    void add_node(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            const StorageSnapshot::Node & node);

    static void add_backend(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            const StorageSnapshot::Backend & backend, const StorageSnapshot::FS & fs);

    static void add_metadata(Group & st_group, const StorageSnapshot::Group & sn_group);
    static void add_metadata_V1(Group & st_group, const StorageSnapshot::Group & sn_group);
    static void add_metadata_V2(Group & st_group, const StorageSnapshot::Group & sn_group);

private:
    Storage & m_storage;
    StorageSnapshot & m_snapshot;
};

#endif

