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

#ifndef __f8057e8e_b6f5_475b_bb31_771928953bf8
#define __f8057e8e_b6f5_475b_bb31_771928953bf8

#include "Couple.h"
#include "Group.h"
#include "Host.h"
#include "Namespace.h"
#include "Node.h"

#include "Job.h"

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

class Job;

class Storage
{
public:
    struct Entries
    {
        std::vector<std::reference_wrapper<Couple>> couples;
        std::vector<std::reference_wrapper<Group>> groups;
        std::vector<std::reference_wrapper<Backend>> backends;
        std::vector<std::reference_wrapper<Node>> nodes;
        std::vector<std::reference_wrapper<FS>> filesystems;
        std::vector<std::reference_wrapper<Namespace>> namespaces;
        std::vector<std::reference_wrapper<const Host>> hosts;

        void sort();
    };

public:
    Storage();
    Storage(const Storage & other);
    ~Storage();

    Host & get_host(const std::string & addr);

    bool has_node(const char *host, int port, int family) const;

    // host must be owned by this Storage
    void add_node(const Host & host, int port, int family);

    std::map<std::string, Node> & get_nodes()
    { return m_nodes; }

    std::map<int, Group> & get_groups()
    { return m_groups; }

    std::map<std::string, Couple> & get_couples()
    { return m_couples; }

    std::map<std::string, Namespace> & get_namespaces()
    { return m_namespaces; }

    std::map<int, Job> & get_jobs()
    { return m_jobs; }

    // save jobs received from MongoDB
    void save_new_jobs(std::vector<Job> new_jobs, uint64_t timestamp);

    // save group history received from MongoDB
    void save_group_history(std::vector<GroupHistoryEntry> history, uint64_t timestamp);

    // last group history update
    uint64_t get_group_history_ts() const
    { return m_group_history_ts; }

    // process new backends, check if some backends are stalled, update backend statuses
    void process_node_backends();
    void process_node_backends(std::vector<std::reference_wrapper<Node>> & nodes);

    // process newly received jobs, bind them to groups, update existing jobs,
    // remove completed/cancelled jobs, unbind them from groups
    void process_new_jobs();

    // process downloaded metadata, recalculate states, etc.
    void update();

    void merge(const Storage & other, bool & have_newer);

    // select entries matching filter
    void select(Filter & filter, Entries & entries);

    void print_json(uint32_t item_types, bool show_internals, std::string & str);
    void print_json(Filter & filter, bool show_internals, std::string & str);

private:
    // process newly received backends, i.e. create Group objects
    void update_group_structure();

    void handle_backend(Backend & backend);

    Namespace & get_namespace(const std::string & name);

    void merge_groups(const Storage & other_storage, bool & have_newer);
    void merge_jobs(const Storage & other_storage, bool & have_newer);
    void merge_namespaces(const Storage & other_storage, bool & have_newer);
    void merge_couples(const Storage & other_storage, bool & have_newer);
    void merge_nodes(const Storage & other_storage, bool & have_newer);
    void merge_hosts(const Storage & other_storage, bool & have_newer);

    void merge_jobs(const std::map<int, Job> & new_jobs, bool *have_newer = nullptr);

    // find an intersection of sets of ResultItem objects
    // each of which is related to a set of SourceItem:s
    template<typename SourceItem, typename ResultItem>
    static void filter_related_items(
            std::vector<std::reference_wrapper<SourceItem>> & source_items,
            std::vector<std::reference_wrapper<ResultItem>> & current_set,
            bool & first_pass);

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            Entries & entries, uint32_t item_types, bool show_internals);

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            uint32_t item_types, bool show_internals);

public:
    template<typename T, typename K, typename V>
    static void merge_map(T & self, std::map<K, V> & map,
            const std::map<K, V> & other_map, bool & have_newer)
    {
        auto my = map.begin();
        auto other = other_map.begin();

        while (other != other_map.end()) {
            while (my != map.end() && my->first < other->first)
                ++my;
            if (my != map.end() && my->first == other->first) {
                my->second.merge(other->second, have_newer);
            } else {
                my = map.insert(my, std::make_pair(other->first, V(self)));
                my->second.clone_from(other->second);
            }
            ++other;
        }

        if (map.size() > other_map.size())
            have_newer = true;
    }

    static bool split_node_num(const std::string & key, std::string & node, uint64_t & id);

private:
    // host addr -> Host
    std::map<std::string, Host> m_hosts;
    // host:port:family -> Node
    std::map<std::string, Node> m_nodes;
    // group id -> Group
    std::map<int, Group> m_groups;
    // gid1:gid2:gid3 -> Couple
    std::map<std::string, Couple> m_couples;
    // namespace -> Namespace
    std::map<std::string, Namespace> m_namespaces;

    // map group id -> job
    std::map<int, Job> m_jobs;
    // jobs received from MongoDB but not processed yet
    std::map<int, Job> m_new_jobs;
    // time database query was completed
    uint64_t m_jobs_timestamp;

    // group history received from MongoDB
    std::vector<GroupHistoryEntry> m_group_history;
    uint64_t m_group_history_ts;
};

#endif

