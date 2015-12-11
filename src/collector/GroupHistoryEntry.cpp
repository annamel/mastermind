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

#include "WorkerApplication.h"

#include "GroupHistoryEntry.h"

#include <vector>

GroupHistoryEntry::GroupHistoryEntry(mongo::BSONObj & obj)
    :
    m_group_id(0),
    m_timestamp(0.0),
    m_empty(true)
{
    m_group_id = obj["group_id"].Number();

    std::vector<mongo::BSONElement> nodes = obj["nodes"].Array();
    for (mongo::BSONElement & node : nodes) {
        mongo::BSONObj node_obj = node.Obj();
        parse_backend_history_entry(node_obj);
    }
}

void GroupHistoryEntry::parse_backend_history_entry(mongo::BSONObj & obj)
{
    Backends backends;
    double cur_ts = 0.0;

    // Insert the most recent backend.
    cur_ts = obj["timestamp"].Number();
    if (cur_ts < m_timestamp)
        return;

    // We are not interested in entries of type automatic.
    std::string type = obj["type"].String();
    if (type == "automatic")
        return;

    std::vector<mongo::BSONElement> set = obj["set"].Array();
    for (mongo::BSONElement & back_elem : set) {
        // TODO: Check element types in database records created by mastermind

        uint64_t backend_id = back_elem["backend_id"].Number();
        std::string hostname = back_elem["hostname"].String();
        int port = back_elem["port"].Number();
        int family = back_elem["family"].Number();

        backends.insert(std::tie(hostname, port, family, backend_id));
    }

    m_backends.swap(backends);
    m_timestamp = cur_ts;
    m_empty = false;
}

void GroupHistoryEntry::print_json(
        rapidjson::Writer<rapidjson::StringBuffer> & writer) const
{
    // {
    //     "timestamp": 1449822159,
    //     "group_id": 31,
    //     "backends": [
    //         {
    //             "hostname": "node1.example.com",
    //             "port": 1025,
    //             "family": 10,
    //             "id": 101
    //         },
    //         {
    //             "hostname": "node2.example.com",
    //             "port": 1025,
    //             "family": 10,
    //             "id": 593
    //         }
    //     ]
    // }

    writer.StartObject();
    writer.Key("timestamp");
    writer.Double(m_timestamp);
    writer.Key("group_id");
    writer.Int(m_group_id);
    writer.Key("backends");
    writer.StartArray();
        for (const auto & backend : m_backends) {
            writer.StartObject();
            writer.Key("hostname");
            writer.String(std::get<0>(backend).c_str());
            writer.Key("port");
            writer.Int(std::get<1>(backend));
            writer.Key("family");
            writer.Int(std::get<2>(backend));
            writer.Uint64(std::get<3>(backend));
            writer.EndObject();
        }
    writer.EndArray();
    writer.EndObject();
}

std::ostream & operator << (std::ostream & ostr, const GroupHistoryEntry & entry)
{
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);

    entry.print_json(writer);

    ostr << buf.GetString();
    return ostr;
}
