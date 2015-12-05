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

#ifndef __399b96bc_e6ca_4613_afad_b47d434d2bea
#define __399b96bc_e6ca_4613_afad_b47d434d2bea

class Couple;
class Group;
class Filter;
class FS;
class Namespace;
class Node;

#include <cstdint>
#include <iostream>
#include <rapidjson/writer.h>
#include <vector>

struct BackendStat
{
    BackendStat();

    uint64_t get_timestamp() const
    { return ts_sec * 1000000UL + ts_usec; }

    uint64_t ts_sec;
    uint64_t ts_usec;
    uint64_t backend_id;
    uint64_t state;

    uint64_t vfs_blocks;
    uint64_t vfs_bavail;
    uint64_t vfs_bsize;
    uint64_t vfs_error;

    uint64_t records_total;
    uint64_t records_removed;
    uint64_t records_removed_size;
    uint64_t base_size;

    uint64_t fsid;
    uint64_t defrag_state;
    uint64_t want_defrag;

    uint64_t read_ios;
    uint64_t write_ios;
    uint64_t read_ticks;
    uint64_t write_ticks;
    uint64_t io_ticks;
    uint64_t read_sectors;
    uint64_t dstat_error;

    uint64_t blob_size_limit;
    uint64_t max_blob_base_size;
    uint64_t blob_size;
    uint64_t group;

    uint64_t read_only;
    uint64_t last_start_ts_sec;
    uint64_t last_start_ts_usec;

    uint64_t stat_commit_rofs_errors;

    uint64_t ell_cache_write_size;
    uint64_t ell_cache_write_time;
    uint64_t ell_disk_write_size;
    uint64_t ell_disk_write_time;
    uint64_t ell_cache_read_size;
    uint64_t ell_cache_read_time;
    uint64_t ell_disk_read_size;
    uint64_t ell_disk_read_time;

    std::string data_path;
    std::string file_path;
};

struct CommandStat
{
    CommandStat();

    void calculate(const BackendStat & old_stat, const BackendStat & new_stat);

    void clear();
    CommandStat & operator += (const CommandStat & other);

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const;

    double ell_disk_read_rate;
    double ell_disk_write_rate;
    double ell_net_read_rate;
    double ell_net_write_rate;
};

class Backend
{
public:
    enum Status
    {
        INIT = 0, // No updates yet
        OK,       // Enabled, no errors
        RO,       // Read-Only
        STALLED,  // Disabled or information is outdated
        BROKEN    // Misconfig
    };

    static const char *status_str(Status status);

    struct Calculated
    {
        uint64_t vfs_free_space;
        uint64_t vfs_total_space;
        uint64_t vfs_used_space;

        uint64_t records;

        int64_t free_space;
        int64_t total_space;
        int64_t used_space;
        int64_t effective_space;
        int64_t effective_free_space;

        double fragmentation;

        int read_rps;
        int write_rps;
        int max_read_rps;
        int max_write_rps;

        uint64_t stat_commit_rofs_errors_diff;

        bool stalled;

        Status status;

        std::string base_path;

        CommandStat command_stat;
    };

public:
    Backend(Node & node);

    void init(const BackendStat & stat);

    void clone_from(const Backend & other);

    const Node & get_node() const
    { return m_node; }

    const std::string & get_key() const
    { return m_key; }

    const BackendStat & get_stat() const
    { return m_stat; }

    Status get_status() const
    { return m_calculated.status; }

    const Calculated & get_calculated() const
    { return m_calculated; }

    const std::string & get_base_path() const
    { return m_calculated.base_path; }

    bool full(double reserved_space = 0.0) const;

    void update(const BackendStat & stat);
    void set_fs(FS & fs);
    void recalculate();

    void check_stalled();
    void update_status();

    // Returns true if bound Group object differs from one in a new stat.
    // If group is unchanged or not bound, returns false.
    bool group_changed() const;
    // Id of currently bound Group.
    int get_old_group_id() const;
    // Bind current Group object
    void set_group(Group & group);

    void merge(const Backend & other, bool & have_newer);

    // Obtain a list of items of certain types related to this backend,
    // e.g. filesystem it's stored on, group's couple.
    // References to objects will be pushed into specified vector.
    // Note that some items may be duplicated.
    void push_items(std::vector<std::reference_wrapper<Couple>> & couples) const;
    void push_items(std::vector<std::reference_wrapper<Namespace>> & namespaces) const;
    void push_items(std::vector<std::reference_wrapper<Node>> & nodes) const;
    void push_items(std::vector<std::reference_wrapper<Group>> & groups) const;
    void push_items(std::vector<std::reference_wrapper<FS>> & filesystems) const;

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            bool show_internals) const;

private:
    void calculate_base_path(const BackendStat & stat);

private:
    Node & m_node;

    // Pointers to a filesystem and a group. If the objects are not created yet,
    // the values are nullptr. These objects shouldn't be modified directly but
    // only used for status checks and by push_items().
    FS *m_fs;
    Group *m_group;

    std::string m_key;

    BackendStat m_stat;

    Calculated m_calculated;
};

#endif

