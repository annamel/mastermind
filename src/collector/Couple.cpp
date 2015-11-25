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

#include "Backend.h"
#include "Couple.h"
#include "Filter.h"
#include "FS.h"
#include "Group.h"
#include "Host.h"
#include "Namespace.h"
#include "Node.h"

#include <algorithm>

Couple::Couple(const std::vector<std::reference_wrapper<Group>> & groups, Namespace & ns)
    :
    m_groups(groups),
    m_namespace(ns),
    m_status(INIT),
    m_modified_time(0),
    m_update_status_duration(0)
{
    for (size_t i = 0; i < groups.size(); ++i) {
        m_key += std::to_string(groups[i].get().get_id());
        if (i != (groups.size() - 1))
            m_key += ':';
    }
}

void Couple::update_status()
{
    Stopwatch watch(m_update_status_duration);

    for (Group & group : m_groups)
        group.update_status();

    std::ostringstream ostr;

    for (const Group & group : m_groups) {
        if (!group.get_metadata().version) {
            m_status = BAD;
            if (!account_job_in_status()) {
                ostr << "Group " << group.get_id() << " has empty metadata.";
                m_status_text = ostr.str();
            }
            return;
        }
        if (m_namespace.get().get_id() != group.get_metadata().namespace_name) {
            m_status = BAD;
            if (!account_job_in_status()) {
                ostr << "Couple's namespace '" << m_namespace.get().get_id() << "' doesn't match group's "
                        "namespace '" << group.get_metadata().namespace_name << "'.";
                m_status_text = ostr.str();
            }
            return;
        }
    }

    for (size_t i = 1; i < m_groups.size(); ++i) {
        if (m_groups[0].get().have_metadata_conflict(m_groups[i].get())) {
            if (!account_job_in_status()) {
                m_status = BAD;
                ostr << "Groups " << m_groups[0].get().get_id() << " and "
                     << m_groups[i].get().get_id() << " have different metadata.";
                m_status_text = ostr.str();
            }
            return;
        }
    }

    auto it = std::find_if(m_groups.begin(), m_groups.end(),
            [] (const Group & group) { return group.get_metadata().frozen; });

    if (it != m_groups.end()) {
        m_status = FROZEN;
        ostr << "Group " << it->get().get_id() << " is frozen.";
        m_status_text = ostr.str();
        return;
    }

    if (app::config().forbidden_dc_sharing_among_groups) {
        if (check_dc_sharing() != 0)
            return;
    }

    if (app::config().forbidden_ns_without_settings) {
        if (m_namespace.get().default_settings()) {
            m_status = BROKEN;
            ostr << "Couple " << m_key << " is assigned to namespace '"
                 << m_namespace.get().get_id() << "' which is not set up";
            m_status_text = ostr.str();
            return;
        }
    }

    size_t nr_coupled = std::count_if(m_groups.begin(), m_groups.end(),
            [] (const Group & group) { return group.get_status() == Group::COUPLED; });
    if (nr_coupled == m_groups.size()) {
        if (app::config().forbidden_unmatched_group_total_space) {
            for (size_t i = 1; i < m_groups.size(); ++i) {
                if (m_groups[i].get().get_total_space() != m_groups[0].get().get_total_space()) {
                    m_status = BROKEN;
                    ostr << "Couple " << m_key << " has unequal total space in groups "
                         << m_groups[0].get().get_id() << " and " << m_groups[i].get().get_id() << '.';
                    m_status_text = ostr.str();
                    return;
                }
            }
        }
        if (full()) {
            m_status = FULL;
            ostr << "Couple " << m_key << " is full.";
            m_status_text = ostr.str();
        } else {
            m_status = OK;
            ostr << "Couple " << m_key << " is OK.";
            m_status_text = ostr.str();
        }
        return;
    }

    do {
        it = std::find_if(m_groups.begin(), m_groups.end(),
                [] (const Group & group) { return group.get_status() == Group::INIT; });

        if (it != m_groups.end()) {
            m_status = INIT;
            ostr << "Couple " << m_key << " has uninitialized group " << it->get().get_id() << '.';
            m_status_text = ostr.str();
            break;
        }

        it = std::find_if(m_groups.begin(), m_groups.end(),
                [] (const Group & group) { return group.get_status() == Group::BROKEN; });

        if (it != m_groups.end()) {
            m_status = BROKEN;
            ostr << "Couple " << m_key << " has broken group " << it->get().get_id() << '.';
            m_status_text = ostr.str();
            break;
        }

        // Couple in state BAD may turn into SERVICE_ACTIVE or SERVICE_STALLED
        // by the end of this method (active job will be checked)

        it = std::find_if(m_groups.begin(), m_groups.end(),
                [] (const Group & group) { return group.get_status() == Group::BAD; });

        if (it != m_groups.end()) {
            m_status = BAD;
            ostr << "Couple " << m_key << " has bad group " << it->get().get_id() << '.';
            m_status_text = ostr.str();
            break;
        }

        it = std::find_if(m_groups.begin(), m_groups.end(),
                [] (const Group & group) {
            return (group.get_status() == Group::RO || group.get_status() == Group::MIGRATING); });

        if (it != m_groups.end()) {
            m_status = BAD;
            ostr << "Couple " << m_key << " has read-only group " << it->get().get_id() << '.';
            m_status_text = ostr.str();
            break;
        }

        m_status = BAD;
        ostr << "Couple " << m_key << " is bad for unknown reason.";
        m_status_text = ostr.str();
    }
    while (0);

    account_job_in_status();
}

bool Couple::check_groups(const std::vector<int> & group_ids) const
{
    if (group_ids.size() != m_groups.size())
        return false;

    for (const Group & group : m_groups) {
        if (group.get_metadata().couple != group_ids)
            return false;
    }

    return true;
}

void Couple::merge(const Couple & other, bool & have_newer)
{
    if (m_modified_time > other.m_modified_time) {
        have_newer = true;
        return;
    }

    m_status = other.m_status;
    m_status_text = other.m_status_text;
    m_update_status_duration = other.m_update_status_duration;
}

void Couple::push_items(std::vector<std::reference_wrapper<Group>> & groups) const
{
    groups.insert(groups.end(), m_groups.begin(), m_groups.end());
}

void Couple::push_items(std::vector<std::reference_wrapper<Namespace>> & namespaces) const
{
    namespaces.push_back(m_namespace.get());
}

void Couple::push_items(std::vector<std::reference_wrapper<Node>> & nodes) const
{
    for (Group & group : m_groups)
        group.push_items(nodes);
}

void Couple::push_items(std::vector<std::reference_wrapper<Backend>> & backends) const
{
    for (Group & group : m_groups)
        group.push_items(backends);
}

void Couple::push_items(std::vector<std::reference_wrapper<FS>> & filesystems) const
{
    for (Group & group : m_groups)
        group.push_items(filesystems);
}

bool Couple::account_job_in_status()
{
    if (m_status != BAD)
        return false;

    for (Group & group : m_groups) {
        if (group.has_active_job()) {
            const Job & job = group.get_active_job();
            Job::Type type = job.get_type();
            Job::Status status = job.get_status();

            if (type != Job::MOVE_JOB && type != Job::RESTORE_GROUP_JOB)
                return false;

            if (status == Job::NEW || status == Job::EXECUTING) {
                m_status = SERVICE_ACTIVE;
                m_status_text = "Couple has active job ";
                m_status_text += job.get_id();
            } else {
                m_status = SERVICE_STALLED;
                m_status_text = "Couple has stalled job ";
                m_status_text += job.get_id();
            }

            if (m_modified_time < group.get_update_time())
                m_modified_time = group.get_update_time();

            return true;
        }
    }

    return false;
}

int Couple::check_dc_sharing()
{
    std::vector<std::string> all_dcs;

    for (Group & group : m_groups) {
        const auto & backends = group.get_backends();

        std::vector<std::string> dcs;
        dcs.reserve(backends.size());

        for (const Backend & backend : backends) {
            const std::string & dc = backend.get_node().get_host().get_dc();
            if (dc.empty()) {
                std::ostringstream ostr;
                ostr << "Group " << group.get_id() << ": Failed to resolve "
                        "DC for node " << backend.get_node().get_key();

                m_status = BAD;
                m_status_text = ostr.str();

                BH_LOG(app::logger(), DNET_LOG_ERROR, "%s", ostr.str());
                return -1;
            }
            dcs.push_back(dc);
        }

        std::sort(dcs.begin(), dcs.end());
        auto it = std::unique(dcs.begin(), dcs.end());
        if (it != dcs.end())
            dcs.erase(it, dcs.end());
        all_dcs.insert(all_dcs.end(), dcs.begin(), dcs.end());
    }

    std::sort(all_dcs.begin(), all_dcs.end());
    auto it = std::unique(all_dcs.begin(), all_dcs.end());
    if  (it != all_dcs.end()) {
        m_status = BROKEN;
        m_status_text = "Couple has nodes sharing the same DC";
        return -1;
    }

    return 0;
}

uint64_t Couple::get_effective_space() const
{
    if (m_groups.empty())
        return 0;

    uint64_t group_effective_space = m_groups[0].get().get_effective_space();
    for (size_t i = 1; i < m_groups.size(); ++i) {
        const Group & group = m_groups[i].get();
        uint64_t eff = group.get_effective_space();
        if (eff < group_effective_space)
            group_effective_space = eff;
    }

    double ns_reserved = m_namespace.get().get_settings().reserved_space;
    return uint64_t(std::floor(double(group_effective_space) * (1.0 - ns_reserved)));
}

uint64_t Couple::get_effective_free_space() const
{
    if (m_groups.empty())
        return 0;

    int64_t group_free_space = m_groups[0].get().get_free_space();
    int64_t group_total_space = m_groups[0].get().get_total_space();

    for (size_t i = 1; i < m_groups.size(); ++i) {
        const Group & group = m_groups[i].get();
        int64_t free = group.get_free_space();
        int64_t total = group.get_total_space();

        if (free < group_free_space)
            group_free_space = free;
        if (total < group_total_space)
            group_total_space = total;
    }

    int64_t eff = get_effective_space();

    if (group_free_space > (group_total_space - eff))
        return group_free_space - (group_total_space - eff);

    return 0;
}

bool Couple::full()
{
    double ns_reserved = m_namespace.get().get_settings().reserved_space;

    for (const Group & group : m_groups) {
        if (group.full(ns_reserved))
            return true;
    }

    if (!get_effective_free_space())
        return true;

    return false;
}

void Couple::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer, bool show_internals) const
{
    writer.StartObject();

    writer.Key("id");
    writer.String(m_key.c_str());

    writer.Key("groups");
    writer.StartArray();
    for (Group & group : m_groups)
        writer.Uint64(group.get_id());
    writer.EndArray();

    writer.Key("effective_space");
    writer.Uint64(get_effective_space());
    writer.Key("effective_free_space");
    writer.Uint64(get_effective_free_space());

    writer.Key("status");
    writer.String(status_str(m_status));
    writer.Key("status_text");
    writer.String(m_status_text.c_str());

    if (show_internals) {
        writer.Key("update_status_duration");
        writer.Uint64(m_update_status_duration);
        writer.Key("modified_time");
        writer.Uint64(m_modified_time);
    }

    writer.EndObject();
}

const char *Couple::status_str(Status status)
{
    switch (status)
    {
    case INIT:
        return "INIT";
    case OK:
        return "OK";
    case FULL:
        return "FULL";
    case BAD:
        return "BAD";
    case BROKEN:
        return "BROKEN";
    case RO:
        return "RO";
    case FROZEN:
        return "FROZEN";
    case MIGRATING:
        return "MIGRATING";
    case SERVICE_ACTIVE:
        return "SERVICE_ACTIVE";
    case SERVICE_STALLED:
        return "SERVICE_STALLED";
    }
    return "UNKNOWN";
}
