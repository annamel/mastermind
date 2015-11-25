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

#include "Couple.h"
#include "Filter.h"
#include "FS.h"
#include "GroupHistoryEntry.h"
#include "Node.h"
#include "WorkerApplication.h"

#include "Group.h"
#include "Storage.h"

#include <sstream>

namespace {

template <typename T>
struct RefLess
{
    bool operator () (const T & a, const T & b) const
    { return &a < &b; }
};

template <typename T>
struct RefEq
{
    bool operator () (const T & a, const T & b) const
    { return &a == &b; }
};

template<typename T>
void remove_duplicates(std::vector<std::reference_wrapper<T>> & v)
{
    std::sort(v.begin(), v.end(), RefLess<T>());
    auto it = std::unique(v.begin(), v.end(), RefEq<T>());
    v.erase(it, v.end());
}

class CoupleKey
{
public:
    CoupleKey(const std::vector<int> & group_ids)
        : m_group_ids(group_ids)
    {}

    operator const std::string()
    {
        if (m_group_ids.empty())
            return std::string();

        std::ostringstream ostr;
        size_t i = 0;
        for (; i < (m_group_ids.size() - 1); ++i)
            ostr << m_group_ids[i] << ':';
        ostr << m_group_ids[i];

        return ostr.str();
    }

private:
    const std::vector<int> & m_group_ids;
};

} // unnamed namespace

void Storage::Entries::sort()
{
    std::sort(couples.begin(), couples.end(), RefLess<Couple>());
    std::sort(groups.begin(), groups.end(), RefLess<Group>());
    std::sort(backends.begin(), backends.end(), RefLess<Backend>());
    std::sort(nodes.begin(), nodes.end(), RefLess<Node>());
    std::sort(filesystems.begin(), filesystems.end(), RefLess<FS>());
}

Storage::Storage()
    :
    m_jobs_timestamp(0),
    m_group_history_ts(0)
{}

Storage::Storage(const Storage & other)
    :
    m_jobs_timestamp(0),
    m_group_history_ts(0)
{
    bool have_newer;
    merge(other, have_newer);
}

Storage::~Storage()
{}

Host & Storage::get_host(const std::string & addr)
{
    auto it = m_hosts.lower_bound(addr);
    if (it == m_hosts.end() || it->first != addr) {
        BH_LOG(app::logger(), DNET_LOG_INFO, "New host %s", addr);
        it = m_hosts.insert(it, std::make_pair(addr, Host(addr)));
    }
    return it->second;
}

bool Storage::has_node(const char *host, int port, int family) const
{
    auto it = m_nodes.find(Node::key(host, port, family));
    return (it != m_nodes.end());
}

void Storage::add_node(const Host & host, int port, int family)
{
    const std::string key = Node::key(host.get_addr().c_str(), port, family);

    auto it = m_nodes.lower_bound(key);
    if (it == m_nodes.end() || it->first != key) {
        BH_LOG(app::logger(), DNET_LOG_INFO, "New node %s", key);
        m_nodes.insert(it, std::make_pair(key, Node(host, port, family)));
    } else {
        BH_LOG(app::logger(), DNET_LOG_DEBUG, "Node %s already exists", key);
        return;
    }
}

void Storage::handle_backend(Backend & backend)
{
    BH_LOG(app::logger(), DNET_LOG_DEBUG, "Storage: Handle backend %s", backend.get_key());

    auto it = m_groups.lower_bound(backend.get_stat().group);

    if (it != m_groups.end() && it->first == int(backend.get_stat().group)) {
        BH_LOG(app::logger(), DNET_LOG_DEBUG, "Adding backend to group %lu", backend.get_stat().group);
        it->second.add_backend(backend);
    } else {
        BH_LOG(app::logger(), DNET_LOG_DEBUG, "New group %lu", backend.get_stat().group);
        it = m_groups.insert(it, std::make_pair(backend.get_stat().group, Group(backend.get_stat().group)));
        it->second.add_backend(backend);
    }

    if (backend.group_changed()) {
        BH_LOG(app::logger(), DNET_LOG_DEBUG, "Backend group changed");

        int old_id = backend.get_old_group_id();
        auto it_old = m_groups.find(old_id);
        if (it_old != m_groups.end()) {
            BH_LOG(app::logger(), DNET_LOG_INFO,
                    "Backend %s has moved from group %d to group %d",
                    backend.get_key().c_str(), old_id, int(backend.get_stat().group));

            it_old->second.remove_backend(backend);
        } else {
            BH_LOG(app::logger(), DNET_LOG_ERROR,
                    "Internal inconsistency: Backend %s has moved from group %d to group %d "
                    "but there is no old group in Storage", backend.get_key().c_str(),
                    old_id, int(backend.get_stat().group));
        }
    }

    backend.set_group(it->second);
}

void Storage::save_new_jobs(std::vector<Job> new_jobs, uint64_t timestamp)
{
    if (m_jobs_timestamp > timestamp) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Storage: Internal error: New jobs have timestamp in the past (%lu > %lu)",
                m_jobs_timestamp, timestamp);
        return;
    }

    m_new_jobs.clear();
    for (Job & job : new_jobs) {
        int group_id = job.get_group_id();
        m_new_jobs.insert(std::make_pair(group_id, std::move(job)));
    }

    m_jobs_timestamp = timestamp;
}

void Storage::save_group_history(std::vector<GroupHistoryEntry> history, uint64_t timestamp)
{
    m_group_history = std::move(history);
    m_group_history_ts = timestamp;
}

void Storage::process_node_backends()
{
    update_group_structure();
    for (auto it = m_nodes.begin(); it != m_nodes.end(); ++it)
        it->second.update_backend_status();
}

void Storage::process_node_backends(std::vector<std::reference_wrapper<Node>> & nodes)
{
    update_group_structure();
    for (Node & node : nodes)
        node.update_backend_status();
}

void Storage::update_group_structure()
{
    BH_LOG(app::logger(), DNET_LOG_INFO, "Updating group structure");

    for (auto it = m_nodes.begin(); it != m_nodes.end(); ++it) {
        Node & node = it->second;
        std::vector<std::reference_wrapper<Backend>> backends = node.pick_new_backends();
        for (Backend & backend : backends)
            handle_backend(backend);
    }

    for (const GroupHistoryEntry & entry : m_group_history) {
        auto it = m_groups.find(entry.get_group_id());
        if (it != m_groups.end()) {
            it->second.apply(entry);
        } else {
            BH_LOG(app::logger(), DNET_LOG_DEBUG, "History database contains record for "
                    "unknown group %d", entry.get_group_id());
        }
    }
}

void Storage::process_new_jobs()
{
    merge_jobs(m_new_jobs);
    m_new_jobs.clear();
}

Namespace & Storage::get_namespace(const std::string & name)
{
    auto it = m_namespaces.lower_bound(name);
    if (it == m_namespaces.end() || it->first != name)
        it = m_namespaces.insert(it, std::make_pair(name, Namespace(name)));
    return it->second;
}

void Storage::update()
{
    BH_LOG(app::logger(), DNET_LOG_INFO, "Storage: updating filesystems, groups, and couples");

    // Create/update filesystems depending on backend stats.
    for (auto it = m_nodes.begin(); it != m_nodes.end(); ++it)
        it->second.update_filesystems();

    // Update group status, first pass.
    for (auto it = m_groups.begin(); it != m_groups.end(); ++it)
        it->second.update_status();

    // Process group metadata and jobs.
    for (auto it = m_groups.begin(); it != m_groups.end(); ++it) {
        Group & group = it->second;

        // Bind or clear an active job.
        auto jit = m_jobs.find(group.get_id());
        if (jit == m_jobs.end())
            group.clear_active_job();
        else
            group.set_active_job(jit->second);

        if (group.parse_metadata() != 0)
            continue;

        group.calculate_type();
    }

    // Create namespaces.
    for (auto it = m_groups.begin(); it != m_groups.end(); ++it) {
        const std::string & ns_name = it->second.get_metadata().namespace_name;
        if (!ns_name.empty())
            get_namespace(ns_name);
    }

    // Create/update couples depending on changes in group metadata and structure
    for (auto it = m_groups.begin(); it != m_groups.end(); ++it) {
        Group & group = it->second;
        if (!group.metadata_parsed())
            continue;

        const std::vector<int> & group_ids = group.get_metadata().couple;
        if (group_ids.empty())
            continue;

        std::vector<std::reference_wrapper<Group>> groups;
        groups.reserve(group_ids.size());
        for (int id : group_ids) {
            if (id == it->first) {
                groups.push_back(group);
            } else {
                auto git = m_groups.lower_bound(id);
                if (git == m_groups.end() || git->first != id) {
                    git = m_groups.insert(git, std::make_pair(id, Group(id)));

                    // Check whether we have a job for this group.
                    auto jit = m_jobs.find(id);
                    if (jit != m_jobs.end()) {
                        BH_LOG(app::logger(), DNET_LOG_INFO,
                                "Have job %s for newly created group %d", jit->second.get_id(), id);
                        git->second.set_active_job(jit->second);
                    }

                    // result must be status=INIT, status_text="No node backends"
                    git->second.update_status();
                }
                groups.push_back(git->second);
            }
        }

        // check if groups are associated with the same existing couple
        bool have_same_couples = true;
        for (size_t i = 1; i < groups.size(); ++i) {
            if (!groups[i].get().match_couple(groups[0])) {
                have_same_couples = false;
                break;
            }
        }
        if (have_same_couples)
            continue;

        std::string key = CoupleKey(group_ids);
        auto cit = m_couples.lower_bound(key);
        if (cit == m_couples.end() || cit->first != key) {
            std::string ns_name;
            for (const Group & g : groups) {
                if (!g.get_metadata().namespace_name.empty()) {
                    ns_name = g.get_metadata().namespace_name;
                    break;
                }
            }

            if (ns_name.empty()) {
                BH_LOG(app::logger(), DNET_LOG_ERROR,
                        "All groups in couple %s have empty namespace", ns_name);
                continue;
            }

            Namespace & ns = get_namespace(ns_name);
            cit = m_couples.insert(cit, std::make_pair(key, Couple(groups, ns)));

            Couple & couple = cit->second;
            ns.add_couple(couple);

            for (Group & group : groups)
                group.set_couple(couple);
        }
    }

    for (auto it = m_groups.begin(); it != m_groups.end(); ++it)
        it->second.update_status_recursive();

    BH_LOG(app::logger(), DNET_LOG_INFO, "Storage update completed");
}

void Storage::merge_groups(const Storage & other_storage, bool & have_newer)
{
    auto my = m_groups.begin();
    auto other = other_storage.m_groups.begin();

    while (other != other_storage.m_groups.end()) {
        while (my != m_groups.end() && my->first < other->first)
            ++my;
        if (my != m_groups.end() && my->first == other->first) {
            my->second.merge(other->second, have_newer);
        } else {
            const Group & other_group = other->second;
            my = m_groups.insert(my, std::make_pair(other->first, Group(other_group.get_id())));
            my->second.merge(other_group, have_newer);
        }
        ++other;
    }

    if (m_groups.size() > other_storage.m_groups.size())
        have_newer = true;
}

void Storage::merge_jobs(const Storage & other_storage, bool & have_newer)
{
    if (m_jobs_timestamp > other_storage.m_jobs_timestamp) {
        if (m_jobs.size() != other_storage.m_jobs.size()) {
            have_newer = true;
            return;
        }

        auto my = m_jobs.begin();
        auto other = other_storage.m_jobs.begin();
        for (; my != m_jobs.end() && other != other_storage.m_jobs.end(); ++my, ++other) {
            if (*my != *other) {
                have_newer = true;
                return;
            }
        }

        return;
    }

    if (m_jobs_timestamp == other_storage.m_jobs_timestamp)
        return;

    merge_jobs(other_storage.m_jobs, &have_newer);

    m_jobs_timestamp = other_storage.m_jobs_timestamp;
}

void Storage::merge_jobs(const std::map<int, Job> & new_jobs, bool *have_newer)
{
    // Apply new set of jobs downloaded from MongoDB.
    // Disappeared jobs must be unbound from groups and erased from m_jobs.
    // Newly coming jobs must be added to m_jobs and bound to groups.
    // Remaining jobs must be updated.

    BH_LOG(app::logger(), DNET_LOG_INFO, "Merge jobs (current count: %lu, new: %lu)",
            m_jobs.size(), new_jobs.size());

    auto old = m_jobs.begin();
    auto fresh = new_jobs.begin();

    while (old != m_jobs.end() || fresh != new_jobs.end()) {
        if (old != m_jobs.end()) {
            if (fresh == new_jobs.end() || old->first < fresh->first) {
                // old job is not present in new set

                BH_LOG(app::logger(), DNET_LOG_INFO, "Job %s disappeared", old->second.get_id());

                // unbind old job from the group
                auto it = m_groups.find(old->first);
                if (it != m_groups.end()) {
                    BH_LOG(app::logger(), DNET_LOG_INFO, "Unbinding job %s from group %d",
                            old->second.get_id(), old->first);
                    it->second.clear_active_job();
                }

                // erase disappeared job
                m_jobs.erase(old++);
                continue;
            } else if (old->first == fresh->first) {
                // both old and new jobs are on the same group, update old one

                BH_LOG(app::logger(), DNET_LOG_INFO, "Updating job for group %d (old id: %s, new id: %s)",
                        old->second.get_group_id(), old->second.get_id(), fresh->second.get_id());
                old->second.merge(fresh->second, have_newer);

                ++old;
                ++fresh;
                continue;
            }
        }

        if (fresh != new_jobs.end()) {
            // new job is not present in existing set

            BH_LOG(app::logger(), DNET_LOG_INFO, "New job %s (group %d)",
                    fresh->second.get_id(), fresh->second.get_group_id());
            m_jobs.insert(*fresh);
            ++fresh;
        }
    }
}

void Storage::merge_namespaces(const Storage & other_storage, bool & have_newer)
{
    // Merge two sets of namespaces.

    auto my = m_namespaces.begin();
    auto other = other_storage.m_namespaces.begin();

    for (; other != other_storage.m_namespaces.end(); ++other) {
        // Skip next few old namespaces which are not present in other_storage.
        while (my != m_namespaces.end() && my->first < other->first)
            ++my;

        // TODO: update existing namespace settings, copy new settings from other storage.
        // It must be done as soon as fetching namespace settings is implemented.

        if (my == m_namespaces.end() || my->first != other->first)
            get_namespace(other->first);
    }

    // If some namespaces are absent in other_storage, we definitely have something new.
    if (m_namespaces.size() > other_storage.m_namespaces.size())
        have_newer = true;
}

void Storage::merge_couples(const Storage & other_storage, bool & have_newer)
{
    auto my = m_couples.begin();
    auto other = other_storage.m_couples.begin();
    while (other != other_storage.m_couples.end()) {
        while (my != m_couples.end() && my->first < other->first)
            ++my;
        if (my != m_couples.end() && my->first == other->first) {
            my->second.merge(other->second, have_newer);
        } else {
            const Couple & other_couple = other->second;
            const auto & other_groups = other_couple.get_groups();

            std::vector<std::reference_wrapper<Group>> my_groups;
            my_groups.reserve(other_groups.size());

            for (size_t i = 0; i < other_groups.size(); ++i) {
                int id = other_groups[i].get().get_id();
                auto gr = m_groups.find(id);

                if (gr != m_groups.end()) {
                    my_groups.push_back(gr->second);
                } else {
                    BH_LOG(app::logger(), DNET_LOG_ERROR,
                            "Merge storage: internal inconsistency: have no group %d for couple", id);
                }
            }

            std::string ns_name;
            for (const Group & group : my_groups) {
                if (!group.get_metadata().namespace_name.empty()) {
                    ns_name = group.get_metadata().namespace_name;
                    break;
                }
            }

            if (ns_name.empty()) {
                // User was already warned in Storage::update()
                // so we should silently skip this couple.
                continue;
            }

            Namespace & my_ns = get_namespace(ns_name);
            my = m_couples.insert(my, std::make_pair(other_couple.get_key(), Couple(my_groups, my_ns)));

            Couple & my_couple = my->second;
            my_ns.add_couple(my_couple);

            for (Group & group : my_groups)
                group.set_couple(my_couple);

            my_couple.merge(other_couple, have_newer);
        }
        ++other;
    }
    if (m_couples.size() > other_storage.m_couples.size())
        have_newer = true;
}

void Storage::merge_nodes(const Storage & other_storage, bool & have_newer)
{
    // Merge two sets of nodes. Newly connected nodes should be cloned into
    // the current map. Old nodes should be updated.

    auto my = m_nodes.begin();
    auto other = other_storage.m_nodes.begin();

    for (; other != other_storage.m_nodes.end(); ++other) {
        // Skip next few old nodes which are not present in other_storage.
        while (my != m_nodes.end() && my->first < other->first)
            ++my;

        if (my != m_nodes.end() && my->first == other->first) {
            // Node already exists
            my->second.merge(other->second, have_newer);
        } else {
            // Find Host
            auto host_it = m_hosts.find(other->second.get_host().get_addr());
            if (host_it != m_hosts.end()) {
                // Create and insert a new Node
                my = m_nodes.insert(my, std::make_pair(other->first, Node(host_it->second)));
                my->second.clone_from(other->second);
            } else {
                // Hosts must have been merged before nodes
                BH_LOG(app::logger(), DNET_LOG_ERROR,
                        "Merge storage: internal inconsistency: "
                        "have no Host for Node %s (other storage has host %s)!!!",
                        other->first.c_str(), other->second.get_host().get_name().c_str());
            }
        }
    }

    // If some nodes are absent in other_storage, we definitely have something new
    if (m_nodes.size() > other_storage.m_nodes.size())
        have_newer = true;
}

void Storage::merge_hosts(const Storage & other, bool & have_newer)
{
    auto my = m_hosts.begin();
    auto other_it = other.m_hosts.begin();

    for (; other_it != other.m_hosts.end(); ++other_it) {
        while (my != m_hosts.end() && my->first < other_it->first)
            ++my;

        if (my != m_hosts.end() && my->first == other_it->first)
            my->second.merge(other_it->second, have_newer);
        else
            my = m_hosts.insert(my, *other_it);
    }

    if (m_hosts.size() > other.m_hosts.size())
        have_newer = true;
}

void Storage::merge(const Storage & other, bool & have_newer)
{
    BH_LOG(app::logger(), DNET_LOG_INFO, "Merging storage");

    have_newer = false;

    merge_hosts(other, have_newer);
    merge_nodes(other, have_newer);
    update_group_structure();
    merge_groups(other, have_newer);
    merge_jobs(other, have_newer);
    merge_namespaces(other, have_newer);
    merge_couples(other, have_newer);

    BH_LOG(app::logger(), DNET_LOG_INFO, "Merge done, %s",
            (have_newer ? "this instance has some more recent data"
                        : "source instance is up-to-date"));
}

template<typename SourceItem, typename ResultItem>
void Storage::filter_related_items(
        std::vector<std::reference_wrapper<SourceItem>> & source_items,
        std::vector<std::reference_wrapper<ResultItem>> & current_set,
        bool & first_pass)
{
    // Intersection with empty set.
    if (current_set.empty() && !first_pass)
        return;

    // Obtain a set of related objects using overloaded method push_items().
    // For example, if we need to get all backends stored on a set of
    // nodes, SourceItem == Node, and ResultItem == Backend, so the method
    // Node::push_items(std::vector<std::reference_wrapper<Backend>>&)
    // will be called for each node.
    std::vector<std::reference_wrapper<ResultItem>> related_items;
    for (SourceItem & source_item : source_items)
        source_item.push_items(related_items);

    if (first_pass || related_items.empty()) {
        current_set = related_items;
        first_pass = false;
        return;
    }

    // push_items() may return duplicates.
    remove_duplicates(related_items);

    // Intersect the set of newly found items with the result of previous passes.
    struct {
        bool operator () (const std::reference_wrapper<ResultItem> & item1,
                const std::reference_wrapper<ResultItem> & item2) const
        { return &item1.get() < &item2.get(); }
    } comp;
    auto it = std::set_intersection(related_items.begin(), related_items.end(),
            current_set.begin(), current_set.end(),
            current_set.begin(),
            comp);
    current_set.erase(it, current_set.end());
}

void Storage::select(Filter & filter, Entries & entries)
{
    filter.sort();

    // map node -> pair(backend ids, fs ids)
    std::map<std::string, std::pair<std::vector<uint64_t>, std::vector<uint64_t>>> node_item_ids;
    for (const std::string & key : filter.backends) {
        std::string node;
        uint64_t backend_id;
        if (split_node_num(key, node, backend_id))
            node_item_ids[node].first.push_back(backend_id);
    }
    for (const std::string & key : filter.filesystems) {
        std::string node;
        uint64_t fsid;
        if (split_node_num(key, node, fsid))
            node_item_ids[node].second.push_back(fsid);
    }

    std::vector<std::reference_wrapper<Backend>> backends;
    std::vector<std::reference_wrapper<FS>> filesystems;
    for (auto nit = node_item_ids.begin(); nit != node_item_ids.end(); ++nit) {
        auto it = m_nodes.find(nit->first);
        if (it == m_nodes.end())
            continue;
        Node & node = it->second;

        const std::vector<uint64_t> & backend_ids = nit->second.first;
        const std::vector<uint64_t> & fs_ids = nit->second.second;
        for (uint64_t backend_id : backend_ids) {
            auto & node_backends = node.get_backends();
            auto it = node_backends.find(backend_id);
            if (it != node_backends.end())
                backends.push_back(it->second);
        }
        for (uint64_t fsid : fs_ids) {
            auto & node_filesystems = node.get_filesystems();
            auto it = node_filesystems.find(fsid);
            if (it != node_filesystems.end())
                filesystems.push_back(it->second);
        }
    }

    std::vector<std::reference_wrapper<Namespace>> namespaces;
    std::vector<std::reference_wrapper<Couple>> couples;
    std::vector<std::reference_wrapper<Group>> groups;
    std::vector<std::reference_wrapper<Node>> nodes;

    for (const std::string & name : filter.namespaces) {
        auto it = m_namespaces.find(name);
        if (it != m_namespaces.end())
            namespaces.push_back(it->second);
    }
    for (const std::string & key : filter.couples) {
        auto it = m_couples.find(key);
        if (it != m_couples.end())
            couples.push_back(it->second);
    }
    for (int id : filter.groups) {
        auto it = m_groups.find(id);
        if (it != m_groups.end())
            groups.push_back(it->second);
    }
    for (const std::string & key : filter.nodes) {
        auto it = m_nodes.find(key);
        if (it != m_nodes.end())
            nodes.push_back(it->second);
    }

    if (filter.item_types & Filter::Group) {
        if (!filter.groups.empty()) {
            entries.groups = groups;
        } else {
            bool first_pass = true;

            if (!(filter.item_types & Filter::Couple) && !couples.empty())
                filter_related_items(couples, entries.groups, first_pass);

            if (!(filter.item_types & Filter::Backend) && !backends.empty())
                filter_related_items(backends, entries.groups, first_pass);

            if (!(filter.item_types & Filter::FS) && !filesystems.empty())
                filter_related_items(filesystems, entries.groups, first_pass);

            if (!(filter.item_types & Filter::Node) && !nodes.empty())
                filter_related_items(nodes, entries.groups, first_pass);

            if (!(filter.item_types & Filter::Namespace) && !namespaces.empty())
                filter_related_items(namespaces, entries.groups, first_pass);
        }
    }

    if (filter.item_types & Filter::Couple) {
        if (!filter.couples.empty()) {
            entries.couples = couples;
        } else {
            bool first_pass = true;

            if (!(filter.item_types & Filter::Group) && !groups.empty())
                filter_related_items(groups, entries.couples, first_pass);

            if (!(filter.item_types & Filter::Backend) && !backends.empty())
                filter_related_items(backends, entries.couples, first_pass);

            if (!(filter.item_types & Filter::FS) && !filesystems.empty())
                filter_related_items(filesystems, entries.couples, first_pass);

            if (!(filter.item_types & Filter::Node) && !nodes.empty())
                filter_related_items(nodes, entries.couples, first_pass);

            if (!(filter.item_types & Filter::Namespace) && !namespaces.empty())
                filter_related_items(namespaces, entries.couples, first_pass);
        }
    }

    if (filter.item_types & Filter::Node) {
        if (!filter.nodes.empty()) {
            entries.nodes = nodes;
        } else {
            bool first_pass = true;

            if (!(filter.item_types & Filter::Group) && !groups.empty())
                filter_related_items(groups, entries.nodes, first_pass);

            if (!(filter.item_types & Filter::Backend) && !backends.empty())
                filter_related_items(backends, entries.nodes, first_pass);

            if (!(filter.item_types & Filter::FS) && !filesystems.empty())
                filter_related_items(filesystems, entries.nodes, first_pass);

            if (!(filter.item_types & Filter::Couple) && !couples.empty())
                filter_related_items(couples, entries.nodes, first_pass);

            if (!(filter.item_types & Filter::Namespace) && !namespaces.empty())
                filter_related_items(namespaces, entries.nodes, first_pass);
        }
    }

    if (filter.item_types & Filter::Backend) {
        if (!filter.backends.empty()) {
            entries.backends = backends;
        } else {
            bool first_pass = true;

            if (!(filter.item_types & Filter::Group) && !groups.empty())
                filter_related_items(groups, entries.backends, first_pass);

            if (!(filter.item_types & Filter::Node) && !nodes.empty())
                filter_related_items(nodes, entries.backends, first_pass);

            if (!(filter.item_types & Filter::FS) && !filesystems.empty())
                filter_related_items(filesystems, entries.backends, first_pass);

            if (!(filter.item_types & Filter::Couple) && !couples.empty())
                filter_related_items(couples, entries.backends, first_pass);

            if (!(filter.item_types & Filter::Namespace) && !namespaces.empty())
                filter_related_items(namespaces, entries.backends, first_pass);
        }
    }

    if (filter.item_types & Filter::FS) {
        if (!filter.filesystems.empty()) {
            entries.filesystems = filesystems;
        } else {
            bool first_pass = true;

            if (!(filter.item_types & Filter::Group) && !groups.empty())
                filter_related_items(groups, entries.filesystems, first_pass);

            if (!(filter.item_types & Filter::Node) && !nodes.empty())
                filter_related_items(nodes, entries.filesystems, first_pass);

            if (!(filter.item_types & Filter::Backend) && !backends.empty())
                filter_related_items(backends, entries.filesystems, first_pass);

            if (!(filter.item_types & Filter::Couple) && !couples.empty())
                filter_related_items(couples, entries.filesystems, first_pass);

            if (!(filter.item_types & Filter::Namespace) && !namespaces.empty())
                filter_related_items(namespaces, entries.filesystems, first_pass);
        }
    }

    if (filter.item_types & Filter::Namespace) {
        if (!filter.namespaces.empty()) {
            entries.namespaces = namespaces;
        } else {
            bool first_pass = true;

            if (!(filter.item_types & Filter::Group) && !groups.empty())
                filter_related_items(groups, entries.namespaces, first_pass);

            if (!(filter.item_types & Filter::Node) && !nodes.empty())
                filter_related_items(nodes, entries.namespaces, first_pass);

            if (!(filter.item_types & Filter::Backend) && !backends.empty())
                filter_related_items(backends, entries.namespaces, first_pass);

            if (!(filter.item_types & Filter::Couple) && !couples.empty())
                filter_related_items(couples, entries.namespaces, first_pass);

            if (!(filter.item_types & Filter::FS) && !filesystems.empty())
                filter_related_items(filesystems, entries.namespaces, first_pass);
        }
    }
}

void Storage::print_json(uint32_t item_types, bool show_internals, std::string & str)
{
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);

    writer.StartObject();
    print_json(writer, item_types, show_internals);
    writer.EndObject();

    str = buf.GetString();
}

void Storage::print_json(Filter & filter, bool show_internals, std::string & str)
{
    filter.sort();

    Entries entries;
    select(filter, entries);

    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);

    writer.StartObject();
    print_json(writer, entries, filter.item_types, show_internals);
    writer.EndObject();

    str = buf.GetString();
}

void Storage::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
        Entries & entries, uint32_t item_types, bool show_internals)
{
    entries.sort();

    if (!entries.nodes.empty()) {
        writer.Key("nodes");
        writer.StartArray();
        for (Node & node : entries.nodes)
            node.print_json(writer, entries.backends, entries.filesystems,
                    !!(item_types & Filter::Backend), !!(item_types & Filter::FS), show_internals);
        writer.EndArray();
    } else {
        if (!entries.backends.empty()) {
            writer.Key("backends");
            writer.StartArray();
            for (Backend & backend : entries.backends)
                backend.print_json(writer, show_internals);
            writer.EndArray();
        }
        if (!entries.filesystems.empty()) {
            writer.Key("filesystems");
            writer.StartArray();
            for (FS & fs : entries.filesystems)
                fs.print_json(writer, show_internals);
            writer.EndArray();
        }
    }

    if (!entries.groups.empty()) {
        writer.Key("groups");
        writer.StartArray();
        for (Group & group : entries.groups)
            group.print_json(writer, show_internals);
        writer.EndArray();
    }

    if (!entries.couples.empty()) {
        writer.Key("couples");
        writer.StartArray();
        for (Couple & couple : entries.couples)
            couple.print_json(writer, show_internals);
        writer.EndArray();
    }

    if (!entries.namespaces.empty()) {
        writer.Key("namespaces");
        writer.StartArray();
        for (Namespace & ns : entries.namespaces)
            ns.print_json(writer);
        writer.EndArray();
    }
}

void Storage::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
        uint32_t item_types, bool show_internals)
{
    if (!!(item_types & (Filter::Node | Filter::Backend | Filter::FS))) {
        writer.Key("nodes");
        writer.StartArray();
        for (auto it = m_nodes.begin(); it != m_nodes.end(); ++it) {
            it->second.print_json(writer,
                    std::vector<std::reference_wrapper<Backend>>(),
                    std::vector<std::reference_wrapper<FS>>(),
                    !!(item_types & Filter::Backend), !!(item_types & Filter::FS), show_internals);
        }
        writer.EndArray();
    }

    if (!!(item_types & Filter::Group)) {
        writer.Key("groups");
        writer.StartArray();
        for (auto it = m_groups.begin(); it != m_groups.end(); ++it)
            it->second.print_json(writer, show_internals);
        writer.EndArray();
    }

    if (!!(item_types & Filter::Couple)) {
        writer.Key("couples");
        writer.StartArray();
        for (auto it = m_couples.begin(); it != m_couples.end(); ++it)
            it->second.print_json(writer, show_internals);
        writer.EndArray();
    }

    if (!!(item_types & Filter::Namespace)) {
        writer.Key("namespaces");
        writer.StartArray();
        for (auto it = m_namespaces.begin(); it != m_namespaces.end(); ++it)
            it->second.print_json(writer);
        writer.EndArray();
    }

    if (!!(item_types & Filter::Job)) {
        writer.Key("jobs");
        writer.StartArray();
        for (auto it = m_jobs.begin(); it != m_jobs.end(); ++it)
            it->second.print_json(writer);
        writer.EndArray();
    }
}

bool Storage::split_node_num(const std::string & key, std::string & node, uint64_t & id)
{
    size_t slash_pos = key.rfind('/');
    if (slash_pos == std::string::npos)
        return false;

    node = key.substr(0, slash_pos);

    std::string id_str = key.substr(slash_pos + 1);
    try {
        id = std::stol(id_str);
    } catch (...) {
        return false;
    }

    return true;
}
