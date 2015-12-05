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
#include "Host.h"
#include "Metrics.h"
#include "Node.h"
#include "WorkerApplication.h"

#include "Storage.h"

void FSStat::reset(const BackendStat & bstat)
{
    ts_sec = bstat.ts_sec;
    ts_usec = bstat.ts_usec;
    read_ticks = bstat.read_ticks;
    write_ticks = bstat.write_ticks;
    read_sectors = bstat.read_sectors;
    io_ticks = bstat.io_ticks;
}

FS::FS(Node & node, uint64_t fsid)
    :
    m_node(node),
    m_fsid(fsid),
    m_stat(),
    m_calculated(),
    m_status(OK),
    m_status_text("No updates yet")
{
    std::memset(&m_stat, 0, sizeof(m_stat));
    m_key = node.get_key() + "/" + std::to_string(fsid);
}

FS::FS(Node & node)
    :
    m_node(node),
    m_fsid(0),
    m_stat(),
    m_calculated(),
    m_status(OK),
    m_status_text("No updates yet")
{
    std::memset(&m_stat, 0, sizeof(m_stat));
}

void FS::clone_from(const FS & other)
{
    m_fsid = other.m_fsid;
    m_key = other.m_key;
    std::memcpy(&m_stat, &other.m_stat, sizeof(m_stat));
    m_calculated = other.m_calculated;
    m_status = other.m_status;
    m_status_text = other.m_status_text;
}

void FS::update(const Backend & backend)
{
    const BackendStat & new_bstat = backend.get_stat();

    double dt = [&]() -> double {
        double ts1 = double(m_stat.get_timestamp()) / 1000000.0;
        double ts2 = double(new_bstat.get_timestamp()) / 1000000.0;
        return ts2 - ts1;
    } ();

    if (dt <= 1.0)
        return;

    // dstat

    if (!new_bstat.dstat_error) {
        // io_ticks is total time a block device has been active, in milliseconds
        if (new_bstat.io_ticks > m_stat.io_ticks)
            m_calculated.disk_util = (double(new_bstat.io_ticks) - double(m_stat.io_ticks)) / dt / 1000.0;

        uint64_t read_ticks = 0;
        if (new_bstat.read_ticks > m_stat.read_ticks)
            read_ticks = new_bstat.read_ticks - m_stat.read_ticks;

        uint64_t write_ticks = 0;
        if (new_bstat.write_ticks > m_stat.write_ticks)
            write_ticks = new_bstat.write_ticks - m_stat.write_ticks;

        uint64_t total_rw_ticks = read_ticks + write_ticks;

        m_calculated.disk_util_read = read_ticks ?
            m_calculated.disk_util * double(read_ticks) / double(total_rw_ticks) : 0.0;

        m_calculated.disk_util_write = write_ticks ?
            m_calculated.disk_util * double(write_ticks) / double(total_rw_ticks) : 0.0;

        // assume 512 byte sectors
        if (new_bstat.read_sectors > m_stat.read_sectors)
            m_calculated.disk_read_rate = double(new_bstat.read_sectors - m_stat.read_sectors) * 512.0 / dt;
        else
            m_calculated.disk_read_rate = 0.0;
    }

    // VFS

    uint64_t new_free_space = backend.get_calculated().vfs_free_space;
    if (!new_bstat.vfs_error && new_free_space < m_calculated.free_space)
        m_calculated.disk_write_rate = double(new_free_space - m_calculated.free_space) / dt;

    m_calculated.total_space = backend.get_calculated().vfs_total_space;
    m_calculated.free_space = backend.get_calculated().vfs_free_space;

    if (!new_bstat.dstat_error && !new_bstat.vfs_error)
        m_stat.reset(new_bstat);
    else
        m_stat = FSStat();
}

void FS::update_status()
{
    uint64_t total_space = 0;
    for (Backend & backend : m_backends) {
        Backend::Status status = backend.get_status();
        if (status != Backend::OK && status != Backend::BROKEN)
            continue;
        total_space += backend.get_calculated().total_space;
    }

    if (total_space <= m_calculated.total_space) {
        m_status = OK;
        m_status_text = "Filesystem is OK";
    } else {
        m_status = BROKEN;

        std::ostringstream ostr;
        ostr << "Total space calculated from backends is " << total_space
             << " which is greater than " << m_calculated.total_space
             << " from monitor stats";
        m_status_text = ostr.str();
    }
}

void FS::update_command_stat()
{
    m_calculated.command_stat.clear();
    for (Backend & backend : m_backends) {
        if (backend.get_calculated().status != Backend::STALLED)
            m_calculated.command_stat += backend.get_calculated().command_stat;
    }
}

void FS::merge(const FS & other, bool & have_newer)
{
    uint64_t my_ts = m_stat.ts_sec * 1000000 + m_stat.ts_usec;
    uint64_t other_ts = other.m_stat.ts_sec * 1000000 + other.m_stat.ts_usec;
    if (my_ts < other_ts) {
        std::memcpy(&m_stat, &other.m_stat, sizeof(m_stat));
        m_calculated = other.m_calculated;
        m_status = other.m_status;
        m_status_text = other.m_status_text;
    } else if (my_ts > other_ts) {
        have_newer = true;
    }
}

void FS::push_items(std::vector<std::reference_wrapper<Couple>> & couples) const
{
    for (Backend & backend : m_backends)
        backend.push_items(couples);
}

void FS::push_items(std::vector<std::reference_wrapper<Namespace>> & namespaces) const
{
    for (Backend & backend : m_backends)
        backend.push_items(namespaces);
}

void FS::push_items(std::vector<std::reference_wrapper<Backend>> & backends) const
{
    backends.insert(backends.end(), m_backends.begin(), m_backends.end());
}

void FS::push_items(std::vector<std::reference_wrapper<Group>> & groups) const
{
    for (Backend & backend : m_backends)
        backend.push_items(groups);
}

void FS::push_items(std::vector<std::reference_wrapper<Node>> & nodes) const
{
    nodes.push_back(m_node);
}

void FS::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
        bool show_internals) const
{
    // JSON looks like this:
    // {
    //     "timestamp": {
    //         "tv_sec": 1445348936,
    //         "tv_usec": 615421,
    //         "user_friendly": "2015-10-20 16:48:56.615421"
    //     },
    //     "commands_stat": {
    //         "ell_disk_read_rate": 0.0,
    //         "ell_disk_write_rate": 0.0,
    //         "ell_net_read_rate": 0.0,
    //         "ell_net_write_rate": 0.0
    //     }
    //     "node": "::1:1025:10",
    //     "fsid": 158919948,
    //     "total_space": 983547510784,
    //     "status": "OK",
    //     "status_text": "Filesystem is OK"
    // }

    writer.StartObject();

    writer.Key("timestamp");
    writer.StartObject();
    writer.Key("tv_sec");
    writer.Uint64(m_stat.ts_sec);
    writer.Key("tv_usec");
    writer.Uint64(m_stat.ts_usec);
    if (show_internals) {
        writer.Key("user_friendly");
        writer.String(timeval_user_friendly(m_stat.ts_sec, m_stat.ts_usec).c_str());
    }
    writer.EndObject();

    writer.Key("node");
    writer.String(m_node.get_key().c_str());
    writer.Key("fsid");
    writer.Uint64(m_fsid);
    writer.Key("status");
    writer.String(status_str(m_status));
    writer.Key("status_text");
    writer.String(m_status_text.c_str());

    writer.Key("read_ticks");
    writer.Uint64(m_stat.read_ticks);
    writer.Key("write_ticks");
    writer.Uint64(m_stat.write_ticks);
    writer.Key("read_sectors");
    writer.Uint64(m_stat.read_sectors);
    writer.Key("io_ticks");
    writer.Uint64(m_stat.io_ticks);

    writer.Key("total_space");
    writer.Uint64(m_calculated.total_space);
    writer.Key("free_space");
    writer.Uint64(m_calculated.free_space);
    writer.Key("disk_util");
    writer.Double(m_calculated.disk_util);
    writer.Key("disk_util_read");
    writer.Double(m_calculated.disk_util_read);
    writer.Key("disk_util_write");
    writer.Double(m_calculated.disk_util_write);
    writer.Key("disk_read_rate");
    writer.Double(m_calculated.disk_read_rate);
    writer.Key("disk_write_rate");
    writer.Double(m_calculated.disk_write_rate);

    writer.Key("commands_stat");
    m_calculated.command_stat.print_json(writer);

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
