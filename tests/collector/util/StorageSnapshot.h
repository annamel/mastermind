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

#ifndef __1d79a13a_c6da_41c1_ac83_40048c030738
#define __1d79a13a_c6da_41c1_ac83_40048c030738

#include <map>
#include <ostream>
#include <string>
#include <vector>

#include <rapidjson/writer.h>

#include <GroupHistoryEntry.h>

// The idea is to have an ability to describe test setup using minimalistic
// JSON document containing only key information about items.
//
// For instance, suppose you want to test if given two groups having enabled
// backends and metadata a new couple is created and has correct status
// calculated.
//
// In this case you don't care about particular values (e.g. filesystem block
// size) and don't want to provide full information about the cluster. All you
// need is basic information. StorageSnapshot and StorageUpdater allow you to
// build Storage object with Nodes, Backends, and so on using following simple
// schema:
//
// {
//     "groups": {
//         "1": {
//             "metadata": {
//                 "version": 2,
//                 "couple": [ 1, 2 ],
//                 "namespace": "default"
//             },
//             "backends": [
//                 "2001:db8:0:1111::11:1025:10/101"
//             ]
//         },
//         "2": {
//             "metadata": {
//                 "version": 2,
//                 "couple": [ 1, 2 ],
//                 "namespace": "default"
//             },
//             "backends": [
//                 "2001:db8:0:1122::14:1025:10/907"
//             ]
//         }
//     }
// }
//
// Although in real life this layout could be built after receiving a lot of
// information from different sources (elliptics nodes, monitor stats, group
// metadata), some set of default values is enough for a large number of tests.
// Of course, you can specify more details, for example, adding description
// for some backends or nodes.
//
// Note that it is not required to define intermediate objects, for example,
// if you want to tell explicitly in which data centers backends reside, you
// will only need to fill "hosts" section in the document.
//
// After processing this document SorageSnapshot will end up building all
// implied objects with default values. All items are represented by
// StorageSnapshot's own data structures, there are no dependencies to
// Storage objects.
//
// When complete storage snapshot is built, you may use StorageUpdater to
// build Storage structure from it. See StorageUpdater class description.
//
// Here is an example document with all currently supported values set.
//
// {
//     "timestamp": {
//         "tv_sec": 1450875367,
//         "tv_usec": 734537
//     },
//     "hosts": {
//         "2001:db8:0:1111::11": {
//             "name": "node01.example.net",
//             "dc": "manzhou"
//         },
//         "2001:db8:0:1122::14": {
//             "name": "node11.example.net",
//             "dc": "vaal"
//         }
//     },
//     "nodes": {
//         "2001:db8:0:1111::11:1025:10": {
//             "la": 31,
//             "tx_bytes": 1778628811,
//             "rx_bytes": 1219421323
//         }
//     },
//     "backends": {
//         "2001:db8:0:1111::11:1025:10/101": {
//             "base_size": 232524191,
//             "records_total": 30029,
//             "records_removed": 22171,
//             "records_removed_size": 138139,
//             "group": 1,
//             "data_path": "/path/to/data/1/1",
//             "state": 1,
//             "read_only": 0,
//             "last_start": {
//                 "tv_sec": 1450875311,
//                 "tv_usec": 16103
//             },
//             "blob_size_limit": 220237111,
//             "fsid": 1164930671
//         }
//     },
//     "filesystems": {
//         "2001:db8:0:1111::11:1025:10/1164930671": {
//             "dstat": {
//                 "read_ios": 204140011,
//                 "write_ios": 1518120647,
//                 "read_ticks": 19469599,
//                 "write_ticks": 38566807,
//                 "io_ticks": 412932577,
//                 "read_sectors": 116130083,
//                 "error": 0
//             },
//             "vfs": {
//                 "blocks": 255075559,
//                 "bavail": 255073001,
//                 "bsize": 4096,
//                 "error": 0
//             }
//         }
//     },
//     "groups": {
//         "1": {
//             "metadata": {
//                 "version": 2,
//                 "couple": [ 1, 2 ],
//                 "namespace": "default",
//                 "frozen": true,
//                 "type": "cache",
//                 "service": {
//                     "migrating": true,
//                     "job_id": "abcdef"
//                 }
//             },
//             "backends": [
//                 "2001:db8:0:1111::11:1025:10/101"
//             ]
//         }
//     },
//     "history": {
//         "timestamp": 1450875247000123000,
//         "entries": [
//             {
//                 "group_id": 3,
//                 "nodes": [
//                     {
//                         "timestamp": 1449841663,
//                         "type": "job",
//                         "set": [
//                             {
//                                 "path": "/path/to/storage/3/3",
//                                 "backend_id": 3,
//                                 "hostname": "node2.example.com",
//                                 "port": 1025,
//                                 "family": 10
//                             }
//                         ]
//                     }
//                 ]
//             }
//         ]
//     }
// }
//
// StorageSnapshot is initialized with JSON passed to constructor or to update()
// method. Method complete() is used to generate all implied objects.
//
// Subsequent calls to update() will modify current structure. You may also want
// to delete an item, i.e. information about it will not reach storage on the
// next call to StorageUpdater. To do this, put null value instead of object
// description in correspondent section:
//
// {
//     "backends": {
//         "2001:db8:0:1111::11:1025:10/101": null
//     }
// }

class StorageSnapshot
{
public:
    struct Host
    {
        std::string addr;
        std::string name;
        std::string dc;

        void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const;
    };

    struct Node
    {
        Node();

        std::string addr;
        int port;
        int family;

        int la;
        uint64_t tx_bytes;
        uint64_t rx_bytes;

        static void split_key(const std::string & key, std::string & addr, int & port, int & family);

        static std::string create_key(const std::string & addr, int port, int family);

        void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const;
    };

    struct Backend
    {
        Backend();

        std::string node;

        int id;

        int base_size;
        int records_total;
        int records_removed;
        int records_removed_size;

        int group;

        std::string data_path;

        int state;
        bool read_only;

        struct {
            uint64_t tv_sec;
            uint64_t tv_usec;
        } last_start;

        uint64_t blob_size_limit;

        uint64_t fsid;

        static void split_key(const std::string & key, std::string & node, int & id);

        static std::string create_key(const std::string & node, uint64_t id);

        void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const;
    };

    struct FS
    {
        FS();

        std::string node;
        uint64_t fsid;

        struct {
            int read_ios;
            int write_ios;
            int read_ticks;
            int write_ticks;
            int io_ticks;
            int read_sectors;
            int error;
        } dstat;

        struct {
            int64_t blocks;
            int64_t bavail;
            int bsize;
            int error;
        } vfs;

        static void split_key(const std::string & key, std::string & node, uint64_t & fsid);

        static std::string create_key(const std::string & node, uint64_t fsid);

        void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const;
    };

    struct Group
    {
        Group();

        int id;
        struct {
            int version;
            bool frozen;
            std::vector<int> couple;
            std::string ns;
            std::string type;
            struct {
                bool migrating;
                std::string job_id;
            } service;
        } metadata;
        std::vector<std::string> backends;

        void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const;
    };

public:
    StorageSnapshot();
    StorageSnapshot(const char *json);

    // Update current schema.
    void update(const char *json);

    // Generate all implied objects (see the description above).
    void complete();

    const std::map<std::string, Host> & get_hosts() const
    { return m_hosts; }

    const std::map<std::string, Node> & get_nodes() const
    { return m_nodes; }

    const std::map<std::string, Backend> & get_backends() const
    { return m_backends; }

    const std::map<std::string, FS> & get_filesystems() const
    { return m_filesystems; }

    const std::map<int, Group> & get_groups() const
    { return m_groups; }

    std::vector<GroupHistoryEntry> pick_group_history()
    { return std::move(m_history); }

    uint64_t get_default_ts() const
    { return m_default_ts; }

    uint64_t get_history_ts() const
    { return m_history_ts; }

    // Generate JSON schema. Currently this method is used for debugging
    // purposes.
    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const;

private:
    void create_host(const std::string & addr);
    void complete_nodes();
    void create_node(const std::string & key);
    void complete_filesystems();
    uint64_t create_filesystem(const std::string & node, uint64_t fsid);
    void complete_backends();
    void create_backend(const std::string & key, int group);
    void complete_groups();

private:
    // Default timestamp
    uint64_t m_default_ts;
    // Address -> Host
    std::map<std::string, Host> m_hosts;
    // address:port:family -> Node
    std::map<std::string, Node> m_nodes;
    // address:port:family/id -> Backend
    std::map<std::string, Backend> m_backends;
    // address:port:family/fsid -> FS
    std::map<std::string, FS> m_filesystems;
    // Group id -> Group
    std::map<int, Group> m_groups;
    // Group history
    std::vector<GroupHistoryEntry> m_history;
    // History timestamp
    uint64_t m_history_ts;
};

std::ostream & operator << (std::ostream & ostr, const StorageSnapshot & snapshot);

#endif

