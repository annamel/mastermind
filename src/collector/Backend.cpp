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
#include "WorkerApplication.h"

#include "Storage.h"

#include <elliptics/packet.h>

BackendStat::BackendStat()
    :
    ts_sec(0),
    ts_usec(0),
    backend_id(0),
    state(0),
    vfs_blocks(0),
    vfs_bavail(0),
    vfs_bsize(0),
    vfs_error(0),
    records_total(0),
    records_removed(0),
    records_removed_size(0),
    base_size(0),
    fsid(0),
    defrag_state(0),
    want_defrag(0),
    read_ios(0),
    write_ios(0),
    read_ticks(0),
    write_ticks(0),
    io_ticks(0),
    read_sectors(0),
    dstat_error(0),
    blob_size_limit(0),
    max_blob_base_size(0),
    blob_size(0),
    group(0),
    read_only(0),
    last_start_ts_sec(0),
    last_start_ts_usec(0),
    stat_commit_rofs_errors(0),
    ell_cache_write_size(0),
    ell_cache_write_time(0),
    ell_disk_write_size(0),
    ell_disk_write_time(0),
    ell_cache_read_size(0),
    ell_cache_read_time(0),
    ell_disk_read_size(0),
    ell_disk_read_time(0),
    io_blocking_size(0),
    io_nonblocking_size(0)
{}

CommandStat::CommandStat()
{
    clear();
}

void CommandStat::calculate(const BackendStat & old_stat, const BackendStat & new_stat)
{
    double dt = new_stat.get_timestamp() / 1000000.0 - old_stat.get_timestamp() / 1000000.0;
    if (dt <= 1.0)
        return;

    int64_t disk_read = int64_t(new_stat.ell_disk_read_size) - int64_t(old_stat.ell_disk_read_size);
    int64_t disk_written = int64_t(new_stat.ell_disk_write_size) - int64_t(old_stat.ell_disk_write_size);
    int64_t cache_read = int64_t(new_stat.ell_cache_read_size) - int64_t(old_stat.ell_cache_read_size);
    int64_t cache_written = int64_t(new_stat.ell_cache_write_size) - int64_t(old_stat.ell_cache_write_size);

    if (disk_read >= 0) {
        ell_disk_read_rate = double(disk_read) / dt;
        if (cache_read >= 0)
            ell_net_read_rate = double(disk_read + cache_read) / dt;
    }

    if (disk_written >= 0) {
        ell_disk_write_rate = double(disk_written) / dt;
        if (cache_written >= 0)
            ell_net_write_rate = double(disk_written + cache_written) / dt;
    }
}

void CommandStat::clear()
{
    ell_disk_read_rate = 0.0;
    ell_disk_write_rate = 0.0;
    ell_net_read_rate = 0.0;
    ell_net_write_rate = 0.0;
}

CommandStat & CommandStat::operator += (const CommandStat & other)
{
    ell_disk_read_rate += other.ell_disk_read_rate;
    ell_disk_write_rate += other.ell_disk_write_rate;
    ell_net_read_rate += other.ell_net_read_rate;
    ell_net_write_rate += other.ell_net_write_rate;

    return *this;
}

void CommandStat::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const
{
    // JSON looks like this:
    // {
    //     "ell_disk_read_rate": 0.0,
    //     "ell_disk_write_rate": 0.0,
    //     "ell_net_read_rate": 0.0,
    //     "ell_net_write_rate": 0.0
    // }

    writer.StartObject();
        writer.Key("ell_disk_read_rate");
        writer.Double(ell_disk_read_rate);
        writer.Key("ell_disk_write_rate");
        writer.Double(ell_disk_write_rate);
        writer.Key("ell_net_read_rate");
        writer.Double(ell_net_read_rate);
        writer.Key("ell_net_write_rate");
        writer.Double(ell_net_write_rate);
    writer.EndObject();
}

Backend::Backend(Node & node)
    :
    m_node(node),
    m_fs(nullptr),
    m_group(nullptr),
    m_calculated()
{}

void Backend::init(const BackendStat & stat)
{
    m_stat = stat;
    m_key = m_node.get_key() + '/' + std::to_string(stat.backend_id);
    calculate_base_path(stat);
}

void Backend::clone_from(const Backend & other)
{
    m_key = other.m_key;

    m_stat = other.m_stat;
    m_calculated = other.m_calculated;
}

bool Backend::full(double reserved_space) const
{
    if (m_calculated.used_space >= int64_t(double(m_calculated.effective_space) * (1.0 - reserved_space)))
        return true;
    if (m_calculated.effective_free_space <= 0)
        return true;
    return false;
}

void Backend::update(const BackendStat & stat)
{
    double ts1 = double(m_stat.get_timestamp()) / 1000000.0;
    double ts2 = double(stat.get_timestamp()) / 1000000.0;
    double d_ts = ts2 - ts1;

    // Calculating only when d_ts is long enough to make result more smooth.
    // With forced update we can get two updates within short interval.
    // In reality, this situation is very rare.
    if (d_ts > 1.0 && !stat.dstat_error) {
        m_calculated.read_rps = int(double(stat.read_ios - m_stat.read_ios) / d_ts);
        m_calculated.write_rps = int(double(stat.write_ios - m_stat.write_ios) / d_ts);

        // XXX RPS_FORMULA
        m_calculated.max_read_rps = int(std::max(double(m_calculated.read_rps) /
                    std::max(m_node.get_stat().load_average, 0.01), 100.0));
        m_calculated.max_write_rps = int(std::max(double(m_calculated.write_rps) /
                    std::max(m_node.get_stat().load_average, 0.01), 100.0));
    }

    m_calculated.command_stat.calculate(m_stat, stat);

    uint64_t last_start_old = m_stat.last_start_ts_sec * 1000000ULL + stat.last_start_ts_usec;
    uint64_t last_start_new = stat.last_start_ts_sec * 1000000ULL + stat.last_start_ts_usec;
    if (last_start_old < last_start_new || m_stat.stat_commit_rofs_errors > stat.stat_commit_rofs_errors) {
        m_calculated.stat_commit_rofs_errors_diff = 0;
    } else {
        uint64_t d = stat.stat_commit_rofs_errors - m_stat.stat_commit_rofs_errors;
        m_calculated.stat_commit_rofs_errors_diff += d;
    }

    calculate_base_path(stat);
    m_stat = stat;
}

void Backend::set_fs(FS & fs)
{
    m_fs = &fs;
}

void Backend::recalculate()
{
    m_calculated.vfs_total_space = m_stat.vfs_blocks * m_stat.vfs_bsize;
    m_calculated.vfs_free_space = m_stat.vfs_bavail * m_stat.vfs_bsize;
    m_calculated.vfs_used_space = m_calculated.vfs_total_space - m_calculated.vfs_free_space;

    m_calculated.records = m_stat.records_total - m_stat.records_removed;
    m_calculated.fragmentation = double(m_stat.records_removed) / double(std::max(m_stat.records_total, 1UL));

    if (m_stat.blob_size_limit) {
        // vfs_total_space can be less than blob_size_limit in case of misconfiguration
        m_calculated.total_space = std::min(m_stat.blob_size_limit, m_calculated.vfs_total_space);
        m_calculated.used_space = m_stat.base_size;
        m_calculated.free_space = std::min(int64_t(m_calculated.vfs_free_space),
                std::max(0L, m_calculated.total_space - m_calculated.used_space));
    } else {
        m_calculated.total_space = m_calculated.vfs_total_space;
        m_calculated.free_space = m_calculated.vfs_free_space;
        m_calculated.used_space = m_calculated.vfs_used_space;
    }

    double share = double(m_calculated.total_space) / double(m_calculated.vfs_total_space);
    int64_t free_space_req_share = std::ceil(double(app::config().reserved_space) * share);
    m_calculated.effective_space = std::max(0L, m_calculated.total_space - free_space_req_share);

    m_calculated.effective_free_space =
        std::max(m_calculated.free_space - (m_calculated.total_space - m_calculated.effective_space), 0L);
}

void Backend::check_stalled()
{
    uint64_t ts_now = 0;
    clock_get_real(ts_now);
    ts_now /= 1000000000ULL;

    if (ts_now <= m_stat.ts_sec) {
        m_calculated.stalled = false;
        return;
    }

    m_calculated.stalled = ((ts_now - m_stat.ts_sec) > app::config().node_backend_stat_stale_timeout);
}

void Backend::update_status()
{
    if (m_calculated.stalled || m_stat.state != DNET_BACKEND_ENABLED || m_fs == nullptr)
        m_calculated.status = STALLED;
    else if (m_fs->get_status() == FS::BROKEN)
        m_calculated.status = BROKEN;
    else if (m_stat.read_only || m_calculated.stat_commit_rofs_errors_diff)
        m_calculated.status = RO;
    else
        m_calculated.status = OK;
}

bool Backend::group_changed() const
{
    if (m_group == nullptr)
        return false;

    return (m_group->get_id() != int(m_stat.group));
}

int Backend::get_old_group_id() const
{
    if (m_group == nullptr)
        return -1;

    return m_group->get_id();
}

void Backend::set_group(Group & group)
{
    m_group = &group;
}

void Backend::merge(const Backend & other, bool & have_newer)
{
    uint64_t my_ts = m_stat.get_timestamp();
    uint64_t other_ts = other.m_stat.get_timestamp();
    if (my_ts < other_ts) {
        m_stat = other.m_stat;
        m_calculated = other.m_calculated;
    } else if (my_ts > other_ts) {
        have_newer = true;
    }
}

void Backend::push_items(std::vector<std::reference_wrapper<Couple>> & couples) const
{
    if (m_group != nullptr)
        m_group->push_items(couples);
}

void Backend::push_items(std::vector<std::reference_wrapper<Namespace>> & namespaces) const
{
    if (m_group != nullptr)
        m_group->push_items(namespaces);
}

void Backend::push_items(std::vector<std::reference_wrapper<Node>> & nodes) const
{
    nodes.push_back(m_node);
}

void Backend::push_items(std::vector<std::reference_wrapper<Group>> & groups) const
{
    if (m_group != nullptr)
        groups.push_back(*m_group);
}

void Backend::push_items(std::vector<std::reference_wrapper<FS>> & filesystems) const
{
    if (m_fs != nullptr)
        filesystems.push_back(*m_fs);
}

void Backend::calculate_base_path(const BackendStat & stat)
{
    if (!stat.data_path.empty())
        m_calculated.base_path = stat.data_path;
    else if (!stat.file_path.empty())
        m_calculated.base_path = stat.file_path;
}

void Backend::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
        bool show_internals) const
{
    // JSON looks like this:
    // {
    //     "addr": "::1:1025:10/10",
    //     "all_stat_commit_errors": 0,
    //     "backend_id": 10,
    //     "base_size": 2333049958,
    //     "blob_size": 53687091200,
    //     "blob_size_limit": 5368709120,
    //     "commands_stat": {
    //         "ell_disk_read_rate": 0.0,
    //         "ell_disk_write_rate": 0.0,
    //         "ell_net_read_rate": 0.0,
    //         "ell_net_write_rate": 0.0
    //     }
    //     "defrag_state": 0,
    //     "effective_free_space": 2728233006,
    //     "effective_space": 5061282964,
    //     "error": 0,
    //     "fragmentation": 0.08474519068511643,
    //     "free_space": 3035659162,
    //     "fsid": 8323278684798404738,
    //     "group": 83,
    //     "io_blocking_size": 0,
    //     "io_nonblocking_size": 0,
    //     "last_start": {
    //         "ts_sec": 1444498430,
    //         "ts_usec": 864588
    //     },
    //     "max_blob_base_size": 2333049958,
    //     "max_read_rps": 100,
    //     "max_write_rps": 100,
    //     "node": "::1:1025:10",
    //     "read_ios": 89340,
    //     "read_only": false,
    //     "read_rps": 21,
    //     "records": 27119,
    //     "records_removed": 2511,
    //     "records_removed_size": 258561169,
    //     "records_total": 29630,
    //     "stalled": 0,
    //     "stat_commit_rofs_errors_diff": 0,
    //     "state": 1,
    //     "status": "OK",
    //     "timestamp": {
    //         "tv_sec": 1445866995,
    //         "tv_usec": 468262,
    //         "user_friendly": "2015-10-26 16:43:15.468262"
    //     },
    //     "total_space": 5368709120,
    //     "used_space": 2333049958,
    //     "vfs_bavail": 477906313,
    //     "vfs_blocks": 480682466,
    //     "vfs_bsize": 4096,
    //     "vfs_free_space": 1957504258048,
    //     "vfs_total_space": 1968875380736,
    //     "vfs_used_space": 11371122688,
    //     "want_defrag": 0,
    //     "write_ios": 1632389,
    //     "write_rps": 12
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
    writer.Key("backend_id");
    writer.Uint64(m_stat.backend_id);
    writer.Key("addr");
    writer.String(m_key.c_str());
    writer.Key("state");
    writer.Uint64(m_stat.state);
    writer.Key("vfs_blocks");
    writer.Uint64(m_stat.vfs_blocks);
    writer.Key("vfs_bavail");
    writer.Uint64(m_stat.vfs_bavail);
    writer.Key("vfs_bsize");
    writer.Uint64(m_stat.vfs_bsize);
    writer.Key("records_total");
    writer.Uint64(m_stat.records_total);
    writer.Key("records_removed");
    writer.Uint64(m_stat.records_removed);
    writer.Key("records_removed_size");
    writer.Uint64(m_stat.records_removed_size);
    writer.Key("base_size");
    writer.Uint64(m_stat.base_size);
    writer.Key("fsid");
    writer.Uint64(m_stat.fsid);
    writer.Key("defrag_state");
    writer.Uint64(m_stat.defrag_state);
    writer.Key("want_defrag");
    writer.Uint64(m_stat.want_defrag);
    writer.Key("read_ios");
    writer.Uint64(m_stat.read_ios);
    writer.Key("write_ios");
    writer.Uint64(m_stat.write_ios);
    writer.Key("dstat_error");
    writer.Uint64(m_stat.dstat_error);
    writer.Key("blob_size_limit");
    writer.Uint64(m_stat.blob_size_limit);
    writer.Key("max_blob_base_size");
    writer.Uint64(m_stat.max_blob_base_size);
    writer.Key("blob_size");
    writer.Uint64(m_stat.blob_size);
    writer.Key("group");
    writer.Uint64(m_stat.group);

    writer.Key("vfs_free_space");
    writer.Uint64(m_calculated.vfs_free_space);
    writer.Key("vfs_total_space");
    writer.Uint64(m_calculated.vfs_total_space);
    writer.Key("vfs_used_space");
    writer.Uint64(m_calculated.vfs_used_space);
    writer.Key("records");
    writer.Uint64(m_calculated.records);
    writer.Key("free_space");
    writer.Uint64(m_calculated.free_space);
    writer.Key("total_space");
    writer.Uint64(m_calculated.total_space);
    writer.Key("used_space");
    writer.Uint64(m_calculated.used_space);
    writer.Key("effective_space");
    writer.Uint64(m_calculated.effective_space);
    writer.Key("effective_free_space");
    writer.Uint64(m_calculated.effective_free_space);
    writer.Key("fragmentation");
    writer.Double(m_calculated.fragmentation);
    writer.Key("read_rps");
    writer.Uint64(m_calculated.read_rps);
    writer.Key("write_rps");
    writer.Uint64(m_calculated.write_rps);
    writer.Key("max_read_rps");
    writer.Uint64(m_calculated.max_read_rps);
    writer.Key("max_write_rps");
    writer.Uint64(m_calculated.max_write_rps);
    writer.Key("status");
    writer.String(status_str(m_calculated.status));

    writer.Key("last_start");
    writer.StartObject();
    writer.Key("ts_sec");
    writer.Uint64(m_stat.last_start_ts_sec);
    writer.Key("ts_usec");
    writer.Uint64(m_stat.last_start_ts_usec);
    writer.EndObject();

    writer.Key("commands_stat");
    m_calculated.command_stat.print_json(writer);

    writer.Key("read_only");
    writer.Bool(!!m_stat.read_only);
    writer.Key("stat_commit_rofs_errors_diff");
    writer.Uint64(m_calculated.stat_commit_rofs_errors_diff); // XXX

    if (show_internals) {
        writer.Key("stat_commit_rofs_errors");
        writer.Uint64(m_stat.stat_commit_rofs_errors);
        writer.Key("stalled");
        writer.Uint64(m_calculated.stalled);
        writer.Key("data_path");
        writer.String(m_stat.data_path.c_str());
        writer.Key("file_path");
        writer.String(m_stat.file_path.c_str());
    }

    writer.Key("base_path");
    writer.String(m_calculated.base_path.c_str());

    writer.EndObject();
}

const char *Backend::status_str(Status status)
{
    switch (status) {
    case INIT:
        return "INIT";
    case OK:
        return "OK";
    case RO:
        return "RO";
    case STALLED:
        return "STALLED";
    case BROKEN:
        return "BROKEN";
    }
    return "UNKNOWN";
}
