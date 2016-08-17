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

#ifndef __046c58ff_e6c4_49a6_a920_eee87b111685
#define __046c58ff_e6c4_49a6_a920_eee87b111685

#include <blackhole/attribute.hpp>
#include <blackhole/attributes.hpp>

#include <functional>
#include <iostream>
#include <rapidjson/writer.h>
#include <set>
#include <string>
#include <vector>

#include "Job.h"

class Backend;
class Couple;
class Filter;
class FS;
class Namespace;
class Node;
class Storage;

class Group
{
public:
    enum Status {
        INIT,
        COUPLED,
        BAD,
        BROKEN,
        RO,
        MIGRATING
    };

    enum Type {
        DATA,
        CACHE,
        UNMARKED
    };

    static const char *status_str(Status status);

    static const char *type_str(Type type);

    struct BackendLess
    {
        bool operator () (const Backend & b1, const Backend & b2) const
        { return &b1 < &b2; }
    };
    typedef std::set<std::reference_wrapper<Backend>, BackendLess> Backends;

    struct Metadata
    {
        void clear();

        int version;
        bool frozen;
        std::vector<int> couple;
        std::string namespace_name;
        std::string type;
        struct {
            bool migrating;
            std::string job_id;
        } service;
    };

public:
    Group(int id);

    int get_id() const
    { return m_id; }

    int get_key() const
    { return m_id; }

    Type get_type() const
    { return m_type; }

    Status get_status() const
    { return m_status; }

    const Backends & get_backends() const
    { return m_backends; }

    bool full(double reserved_space = 0.0) const;

    uint64_t get_total_space() const;

    uint64_t get_free_space() const;

    uint64_t get_effective_space() const;

    bool has_active_job() const;

    // have_active_job() must be checked first
    const Job & get_active_job() const;

    const Metadata & get_metadata() const
    { return m_metadata; }

    void add_backend(Backend & backend);
    void remove_backend(Backend & backend);

    // Apply a new set of backends from history database.
    // Group backends which are absent in this set will be removed.
    void update_backends(const std::vector<std::string> & backends, uint64_t timestamp);

    void handle_metadata_download_failed(const std::string & why);
    void save_metadata(const char *metadata, size_t size, uint64_t timestamp);
    bool parse_metadata();
    void calculate_type();

    bool metadata_parsed() const
    { return m_metadata_parsed; }

    uint64_t get_update_time() const
    { return m_update_time; }

    // the most recently updated backend
    uint64_t get_backend_update_time() const;

    void set_active_job(const Job & job);
    void clear_active_job();

    void update_status();
    void update_status_recursive();

    bool have_metadata_conflict(const Group & other);

    void set_couple(Couple & couple);

    bool match_couple(const Group & other) const;

    void merge(const Group & other, bool & have_newer);

    // Obtain a list of items of certain types related to this group,
    // e.g. couple it belongs to, backends serving it.
    // References to objects will be pushed into specified vector.
    // Note that some items may be duplicated.
    void push_items(std::vector<std::reference_wrapper<Couple>> & couples) const;
    void push_items(std::vector<std::reference_wrapper<Namespace>> & namespaces) const;
    void push_items(std::vector<std::reference_wrapper<Node>> & nodes) const;
    void push_items(std::vector<std::reference_wrapper<Backend>> & backends) const;
    void push_items(std::vector<std::reference_wrapper<FS>> & filesystems) const;

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            bool show_internals) const;

    uint64_t get_metadata_parse_duration() const
    { return m_metadata_parse_duration; }

private:
    bool update_storage_group_status();

private:
    int m_id;

    blackhole::attributes_t m_attr;

    // Set of references to backends serving this group. They shouldn't be
    // modified directly but only used to obtain related items and calculate the
    // state of the group.
    Backends m_backends;

    bool m_clean;
    std::vector<char> m_metadata_file;

    // timestamp of the most recently modified group in the couple
    uint64_t m_update_time;

    // values extracted from file
    Metadata m_metadata;

    bool m_metadata_parsed;
    uint64_t m_metadata_parse_duration;

    // Pointers to a couple, active job, and a namespace of this group.
    // If the information is unknown, e.g. metadata was not loaded,
    // the values are set to nullptr. These objects shouldn't be modified
    // directly except that Couple::update_status() is invoked from
    // update_status_recursive(); also items can be collected by push_items().
    Couple *m_couple;
    const Job *m_active_job;

    Type m_type;

    std::string m_status_text;
    Status m_status;
};

#endif

