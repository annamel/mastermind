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

#ifndef __c065a812_f800_4562_8686_a77b1e60e201
#define __c065a812_f800_4562_8686_a77b1e60e201

#include <cstdint>
#include <functional>
#include <rapidjson/writer.h>
#include <set>
#include <string>
#include <vector>

#include "Backend.h"

class Filter;
class Node;
class Storage;

struct FSStat
{
    uint64_t get_timestamp() const
    { return ts_sec * 1000000ULL + ts_usec; }

    void reset(const BackendStat & bstat);

    uint64_t ts_sec;
    uint64_t ts_usec;
    uint64_t read_ticks;
    uint64_t write_ticks;
    uint64_t read_sectors;
    uint64_t io_ticks;
};

class FS
{
public:
    enum Status {
        OK,
        BROKEN
    };

    static const char *status_str(Status status);

    struct BackendLess
    {
        bool operator () (const Backend & b1, const Backend & b2) const
        { return &b1 < &b2; }
    };
    typedef std::set<std::reference_wrapper<Backend>, BackendLess> Backends;

    struct Calculated
    {
        uint64_t total_space;
        uint64_t free_space;

        double disk_util;
        double disk_util_read;
        double disk_util_write;

        double disk_read_rate;
        double disk_write_rate;

        CommandStat command_stat;
    };

public:
    FS(Node & node, uint64_t fsid);
    FS(Node & node);

    void clone_from(const FS & other);

    uint64_t get_fsid() const
    { return m_fsid; }

    const std::string & get_key() const
    { return m_key; }

    const FSStat & get_stat() const
    { return m_stat; }

    Status get_status() const
    { return m_status; }

    void add_backend(Backend & backend)
    { m_backends.insert(backend); }

    void remove_backend(Backend & backend)
    { m_backends.erase(backend); }

    void update(const Backend & backend);
    void update_status();
    void update_command_stat();

    void merge(const FS & other, bool & have_newer);

    // Obtain a list of items of certain types related to this filesystem,
    // e.g. backends stored on it, groups served by these backends.
    // References to objects will be pushed into specified vector.
    // Note that some items may be duplicated.
    void push_items(std::vector<std::reference_wrapper<Couple>> & couples) const;
    void push_items(std::vector<std::reference_wrapper<Namespace>> & namespaces) const;
    void push_items(std::vector<std::reference_wrapper<Backend>> & backends) const;
    void push_items(std::vector<std::reference_wrapper<Group>> & groups) const;
    void push_items(std::vector<std::reference_wrapper<Node>> & nodes) const;

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            bool show_internals) const;

private:
    Node & m_node;
    uint64_t m_fsid;
    std::string m_key;

    // 'id' field in JSON.
    std::string m_id;

    // Set of references to backends stored on this filesystem. They shouldn't be
    // modified directly but only used to obtain related items and calculate the
    // state of the backend.
    Backends m_backends;

    FSStat m_stat;
    Calculated m_calculated;

    Status m_status;
    std::string m_status_text;
};

#endif

