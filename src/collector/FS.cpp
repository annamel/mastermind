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

#include "Backend.h"
#include "Filter.h"
#include "FS.h"
#include "Metrics.h"
#include "Node.h"
#include "Storage.h"
#include "WorkerApplication.h"

FS::FS(Node & node, uint64_t fsid)
    :
    m_node(node),
    m_fsid(fsid),
    m_status(OK)
{
    std::memset(&m_stat, 0, sizeof(m_stat));
    m_key = node.get_key() + "/" + std::to_string(fsid);
}

FS::FS(Node & node)
    :
    m_node(node),
    m_fsid(0),
    m_status(OK)
{
    std::memset(&m_stat, 0, sizeof(m_stat));
}

void FS::clone_from(const FS & other)
{
    m_fsid = other.m_fsid;
    m_key = other.m_key;
    std::memcpy(&m_stat, &other.m_stat, sizeof(m_stat));
    m_status = other.m_status;

    if (!other.m_backends.empty()) {
        BH_LOG(m_node.get_storage().get_app().get_logger(), DNET_LOG_ERROR,
                "Internal inconsistency detected: cloning FS '%s' from other "
                "one with non-empty set of backends", m_key.c_str());
    }
}

void FS::update(const Backend & backend)
{
    const BackendStat & stat = backend.get_stat();
    m_stat.ts_sec = stat.ts_sec;
    m_stat.ts_usec = stat.ts_usec;
    m_stat.total_space = backend.get_vfs_total_space();
}

void FS::update_status()
{
    Status prev = m_status;

    uint64_t total_space = 0;
    for (Backend *backend : m_backends) {
        Backend::Status status = backend->get_status();
        if (status != Backend::OK && status != Backend::BROKEN)
            continue;
        total_space += backend->get_total_space();
    }

    m_status = (total_space <= m_stat.total_space) ? OK : BROKEN;
    if (m_status != prev)
        BH_LOG(m_node.get_storage().get_app().get_logger(), DNET_LOG_INFO,
                "FS %s/%lu status change %d -> %d",
                m_node.get_key().c_str(), m_fsid, int(prev), int(m_status));
}

void FS::merge(const FS & other)
{
    uint64_t my_ts = m_stat.ts_sec * 1000000 + m_stat.ts_usec;
    uint64_t other_ts = other.m_stat.ts_sec * 1000000 + other.m_stat.ts_usec;
    if (my_ts < other_ts) {
        std::memcpy(&m_stat, &other.m_stat, sizeof(m_stat));
        m_status = other.m_status;
    }
}

bool FS::match(const Filter & filter, uint32_t item_types) const
{
    if ((item_types & Filter::FS) && !filter.filesystems.empty()) {
        if (!std::binary_search(filter.filesystems.begin(),
                    filter.filesystems.end(), m_key))
            return false;
    }

    if ((item_types & Filter::Node) && !filter.nodes.empty()) {
        if (!std::binary_search(filter.nodes.begin(), filter.nodes.end(),
                    m_node.get_key()))
            return false;
    }

    if ((item_types & Filter::Backend) && !filter.backends.empty()) {
        bool found_backend = false;

        for (Backend *backend : m_backends) {
            if (std::binary_search(filter.backends.begin(), filter.backends.end(),
                        backend->get_key())) {
                found_backend = true;
                break;
            }
        }

        if (!found_backend)
            return false;
    }

    bool check_groups = (item_types & Filter::Group) && !filter.groups.empty();
    bool check_couples = (item_types & Filter::Couple) && !filter.couples.empty();
    bool check_namespaces = (item_types && Filter::Namespace) && !filter.namespaces.empty();

    if (check_groups || check_couples || check_namespaces) {
        bool matched = false;

        for (Backend *backend : m_backends) {
            if (backend->get_group() == NULL)
                continue;
            if (backend->get_group()->match(filter,
                        item_types & (Filter::Group|Filter::Couple|Filter::Namespace))) {
                matched = true;
                break;
            }
        }

        if (!matched)
            return false;
    }

    return true;
}

void FS::print_info(std::ostream & ostr) const
{
    ostr << "FS {\n"
            "  node: " << m_node.get_key() << "\n"
            "  fsid: " << m_fsid << "\n"
            "  Stat {\n"
            "    ts: " << timeval_user_friendly(m_stat.ts_sec, m_stat.ts_usec) << "\n"
            "    total_space: " << m_stat.total_space << "\n"
            "  }\n"
            "  number of backends: " << m_backends.size() << "\n"
            "  status: " << status_str(m_status) << "\n"
            "}";
}

void FS::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const
{
    writer.StartObject();

    writer.Key("timestamp");
    writer.StartObject();
    writer.Key("tv_sec");
    writer.Uint64(m_stat.ts_sec);
    writer.Key("tv_usec");
    writer.Uint64(m_stat.ts_usec);
    writer.EndObject();

    writer.Key("host");
    writer.String(m_node.get_host().c_str());
    writer.Key("fsid");
    writer.Uint64(m_fsid);
    writer.Key("total_space");
    writer.Uint64(m_stat.total_space);
    writer.Key("status");
    writer.String(status_str(m_status));

    writer.EndObject();
}

const char *FS::status_str(Status status)
{
    switch (status)
    {
    case OK:
        return "OK";
    case BROKEN:
        return "BROKEN";
    }
    return "UNKNOWN";
}
