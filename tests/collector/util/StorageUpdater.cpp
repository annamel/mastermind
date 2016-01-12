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

#include "StorageUpdater.h"

#include <Group.h>
#include <Host.h>
#include <Storage.h>

#include <msgpack.hpp>

StorageUpdater::StorageUpdater(Storage & storage, StorageSnapshot & snapshot)
    :
    m_storage(storage),
    m_snapshot(snapshot)
{}

void StorageUpdater::update_nodes()
{
    // Update hosts.
    {
        const auto & hosts = m_snapshot.get_hosts();
        for (auto it = hosts.begin(); it != hosts.end(); ++it) {
            const auto & sn_host = it->second;
            Host & st_host = m_storage.get_host(it->first);
            st_host.set_name(sn_host.name);
            st_host.set_dc(sn_host.dc);
        }
    }

    // Update nodes.
    const auto & nodes = m_snapshot.get_nodes();
    for (auto it = nodes.begin(); it != nodes.end(); ++it) {
        const auto & sn_node = it->second;
        if (!m_storage.has_node(sn_node.addr.c_str(), sn_node.port, sn_node.family)) {
            const Host & st_host = m_storage.get_host(sn_node.addr);
            m_storage.add_node(st_host, sn_node.port, sn_node.family);
        }
    }
}

void StorageUpdater::update_monitor_stats()
{
    auto & sn_backends = m_snapshot.get_backends();
    auto & sn_nodes = m_snapshot.get_nodes();
    auto & sn_filesystems = m_snapshot.get_filesystems();

    for (auto it = sn_nodes.begin(); it != sn_nodes.end(); ++it) {
        const std::string & node_key = it->first;
        const auto & sn_node = it->second;

        rapidjson::StringBuffer buf;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buf);

        // Start the entire document.
        writer.StartObject();

        // Add procfs and timestamp.
        add_node(writer, sn_node);

        // Add backends living on current node.
        writer.Key("backends");
        writer.StartObject();
        for (auto bit = sn_backends.begin(); bit != sn_backends.end(); ++bit) {
            const auto & sn_backend = bit->second;
            if (sn_backend.node == node_key) {
                writer.Key(std::to_string(sn_backend.id).c_str());
                add_backend(writer, sn_backend, sn_filesystems.find(
                        StorageSnapshot::FS::create_key(node_key, sn_backend.fsid))->second);
            }
        }
        writer.EndObject();

        // End of the document.
        writer.EndObject();

        // This is full monitor stats JSON for this node.
        std::string json_str = buf.GetString();

        // Apply JSON to node.
        auto & st_nodes = m_storage.get_nodes();
        Node & st_node = st_nodes.find(node_key)->second;
        st_node.add_download_data(json_str.c_str(), json_str.size());
        Node::parse_stats(&st_node);

    }

    m_storage.save_group_history(std::move(m_snapshot.pick_group_history()),
        m_snapshot.get_history_ts());

    // Update group structure, etc.
    m_storage.process_node_backends();
}

void StorageUpdater::update_metadata()
{
    // For each group generate msgpack:ed group metadata and pass buffer to the group.

    auto & st_groups = m_storage.get_groups();
    auto & sn_groups = m_snapshot.get_groups();

    for (auto it = st_groups.begin(); it != st_groups.end(); ++it) {
        int id = it->first;
        Group & st_group = it->second;

        // It is not necessary that the group is defined in snapshot.
        // In real life it would be "metadata download failed".
        auto jt = sn_groups.find(id);
        if (jt != sn_groups.end())
            add_metadata(st_group, jt->second);
    }
}

void StorageUpdater::update()
{
    m_storage.update();
}

void StorageUpdater::update_all()
{
    update_nodes();
    update_monitor_stats();
    update_metadata();
    update();
}

void StorageUpdater::add_node(rapidjson::Writer<rapidjson::StringBuffer> & writer,
        const StorageSnapshot::Node & node)
{
    // Add procfs and timestamp to monitor_stats.

    uint64_t sec = m_snapshot.get_default_ts() / 1000000000ULL;
    uint64_t usec = (m_snapshot.get_default_ts() / 1000ULL) % 1000000ULL;

    writer.Key("timestamp");
    writer.StartObject();
        writer.Key("tv_sec");
        writer.Uint64(sec);
        writer.Key("tv_usec");
        writer.Uint64(usec);
    writer.EndObject();

    writer.Key("procfs");
    writer.StartObject();
        writer.Key("vm");
        writer.StartObject();
            writer.Key("la");
            writer.StartArray();
                writer.Int(node.la);
                writer.Int(node.la);
                writer.Int(node.la);
            writer.EndArray();
        writer.EndObject();

        writer.Key("net");
        writer.StartObject();
            writer.Key("net_interfaces");
            writer.StartObject();
                writer.Key("eth0");
                writer.StartObject();
                    writer.Key("receive");
                    writer.StartObject();
                        writer.Key("bytes");
                        writer.Uint64(node.rx_bytes);
                    writer.EndObject();
                    writer.Key("transmit");
                    writer.StartObject();
                        writer.Key("bytes");
                        writer.Uint64(node.tx_bytes);
                    writer.EndObject();
                writer.EndObject();
            writer.EndObject();
        writer.EndObject();
    writer.EndObject();
}

void StorageUpdater::add_backend(rapidjson::Writer<rapidjson::StringBuffer> & writer,
        const StorageSnapshot::Backend & backend, const StorageSnapshot::FS & fs)
{
    writer.StartObject();

    writer.Key("backend_id");
    writer.Int(backend.id);

    writer.Key("status");
    writer.StartObject();
        writer.Key("state");
        writer.Int(backend.state);
        writer.Key("read_only");
        writer.Bool(backend.read_only);
        writer.Key("last_start");
        writer.StartObject();
            writer.Key("tv_sec");
            writer.Uint64(backend.last_start.tv_sec);
            writer.Key("tv_usec");
            writer.Uint64(backend.last_start.tv_usec);
        writer.EndObject();
    writer.EndObject();

    writer.Key("backend");
    writer.StartObject();
        writer.Key("summary_stats");
        writer.StartObject();
            writer.Key("base_size");
            writer.Int(backend.base_size);
            writer.Key("records_total");
            writer.Int(backend.records_total);
            writer.Key("records_removed");
            writer.Int(backend.records_removed);
            writer.Key("records_removed_size");
            writer.Int(backend.records_removed_size);
        writer.EndObject();

        writer.Key("config");
        writer.StartObject();
            writer.Key("group");
            writer.Int(backend.group);
            writer.Key("data");
            writer.String(backend.data_path.c_str());
            writer.Key("blob_size_limit");
            writer.Uint64(backend.blob_size_limit);
        writer.EndObject();

        writer.Key("vfs");
        writer.StartObject();
            writer.Key("fsid");
            writer.Int64(fs.fsid);
            writer.Key("blocks");
            writer.Int64(fs.vfs.blocks);
            writer.Key("bavail");
            writer.Int64(fs.vfs.bavail);
            writer.Key("bsize");
            writer.Int(fs.vfs.bsize);
            if (fs.vfs.error) {
                writer.Key("error");
                writer.Int(fs.vfs.error);
            }
        writer.EndObject();

        writer.Key("dstat");
        writer.StartObject();
            writer.Key("read_ios");
            writer.Int(fs.dstat.read_ios);
            writer.Key("write_ios");
            writer.Int(fs.dstat.write_ios);
            writer.Key("read_ticks");
            writer.Int(fs.dstat.read_ticks);
            writer.Key("write_ticks");
            writer.Int(fs.dstat.write_ticks);
            writer.Key("io_ticks");
            writer.Int(fs.dstat.io_ticks);
            writer.Key("read_sectors");
            writer.Int(fs.dstat.read_sectors);
            if (fs.dstat.error) {
                writer.Key("error");
                writer.Int(fs.dstat.error);
            }
        writer.EndObject();
    writer.EndObject();

    writer.EndObject();
}

void StorageUpdater::add_metadata(Group & st_group, const StorageSnapshot::Group & sn_group)
{
    // Both metadata of version 1 and 2 are supported.

    switch (sn_group.metadata.version)
    {
    case 1:
        add_metadata_V1(st_group, sn_group);
        break;
    case 2:
        add_metadata_V2(st_group, sn_group);
        break;
    default:
        break;
    }
}

void StorageUpdater::add_metadata_V1(Group & st_group, const StorageSnapshot::Group & sn_group)
{
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(&buffer);
    packer.pack_array(sn_group.metadata.couple.size());
    for (int id : sn_group.metadata.couple)
        packer.pack(id);

    st_group.save_metadata(buffer.data(), buffer.size(), ::time(nullptr)*1000000000ULL);
}

void StorageUpdater::add_metadata_V2(Group & st_group, const StorageSnapshot::Group & sn_group)
{
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(&buffer);

    int map_size = 1;
    if (sn_group.metadata.frozen)
        ++map_size;
    if (!sn_group.metadata.couple.empty())
        ++map_size;
    if (!sn_group.metadata.ns.empty())
        ++map_size;
    if (!sn_group.metadata.type.empty())
        ++map_size;
    if (sn_group.metadata.service.migrating ||
            !sn_group.metadata.service.job_id.empty())
        ++map_size;

    packer.pack_map(map_size);

    packer.pack(std::string("version"));
    packer.pack(2);

    if (sn_group.metadata.frozen) {
        packer.pack(std::string("frozen"));
        packer.pack(true);
    }

    if (!sn_group.metadata.couple.empty()) {
        packer.pack(std::string("couple"));
        packer.pack_array(sn_group.metadata.couple.size());
        for (int id : sn_group.metadata.couple)
            packer.pack(id);
    }

    if (!sn_group.metadata.ns.empty()) {
        packer.pack(std::string("namespace"));
        packer.pack(sn_group.metadata.ns);
    }

    if (!sn_group.metadata.type.empty()) {
        packer.pack(std::string("type"));
        packer.pack(sn_group.metadata.type);
    }

    if (sn_group.metadata.service.migrating ||
            !sn_group.metadata.service.job_id.empty()) {
        packer.pack(std::string("service"));
        packer.pack_map(2);
        if (sn_group.metadata.service.migrating) {
            packer.pack(std::string("status"));
            packer.pack(std::string("MIGRATING"));
        }
        if (!sn_group.metadata.service.job_id.empty()) {
            packer.pack(std::string(std::string("job_id")));
            packer.pack(sn_group.metadata.service.job_id);
        }
    }

    st_group.save_metadata(buffer.data(), buffer.size(), ::time(nullptr)*1000000000ULL);
}
