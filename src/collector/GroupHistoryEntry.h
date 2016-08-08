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

#ifndef __9aa43a1c_afa6_4bf7_a90c_b16d0ba55bb1
#define __9aa43a1c_afa6_4bf7_a90c_b16d0ba55bb1

#include <mongo/bson/bson.h>
#include <rapidjson/writer.h>

#include <ostream>
#include <set>
#include <string>
#include <tuple>

// Sample history database entry:
//
// {
//     "_id" : ObjectId("5617ce09e9024701cf86922e"),
//     "group_id" : 200,
//     "nodes" : [
//         {
//             "timestamp" : 1446731759,
//             "type" : "automatic",
//             "set" : [
//                 {
//                     "path" : "/path/to/storage/1/2/",
//                     "backend_id" : 100,
//                     "hostname" : "node01.example.com",
//                     "port" : 1025,
//                     "family" : 10
//                 }
//             ]
//         },
//         {
//             "timestamp" : 1446738868,
//             "type" : "job",
//             "set" : [ ]
//         }
//     ]
// }
//
// Array "nodes" contains audit of set of node backends serving the group. Each
// entry has fields "timestamp", "type", and "set".
//
// "timestamp" is a point when the entry was created (for example, when
// a job completed).
// "type" can either be "automatic" (created by mastermind), "manual" (created
// by user request), "job" (created by job mechanism).
// "set" is the current full set of node backends.

class GroupHistoryEntry
{
public:
    // Each tuple contains (hostname, port, family, backend_id)
    typedef std::set<std::tuple<std::string, int, int, uint64_t>> Backends;

    GroupHistoryEntry(mongo::BSONObj & obj);

    int get_group_id() const
    { return m_group_id; }

    const Backends & get_backends() const
    { return m_backends; }

    double get_timestamp() const
    { return m_timestamp; }

    // Empty means that information about this group found in
    // the database is valid but there were nothing we were
    // looking for (i.e. no records of type "job" and "manual").
    bool empty() const
    { return m_empty; }

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const;

private:
    void parse_backend_history_entry(mongo::BSONObj & obj);

private:
    int m_group_id;
    Backends m_backends;
    double m_timestamp;
    bool m_empty;
};

std::ostream & operator << (std::ostream & ostr, const GroupHistoryEntry & entry);

#endif

