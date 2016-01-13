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

#include "StorageSnapshot.h"
#include "StorageUpdater.h"

#include <Metrics.h>

#include <stdexcept>

#include <mongo/db/json.h>
#include <rapidjson/document.h>

#include <iostream>
#include <sstream>

#define DEFAULT_DC "manzhou"

void StorageSnapshot::Host::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const
{
    writer.StartObject();
    writer.Key("addr");
    writer.String(addr.c_str());
    writer.Key("name");
    writer.String(name.c_str());
    writer.Key("dc");
    writer.String(dc.c_str());
    writer.EndObject();
}

StorageSnapshot::Node::Node()
    :
    la(),
    tx_bytes(),
    rx_bytes()
{}

std::string StorageSnapshot::Node::create_key(const std::string & addr, int port, int family)
{
    std::ostringstream ostr;
    ostr << addr << ':' << port << ':' << family;
    return ostr.str();
}

void StorageSnapshot::Node::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const
{
    writer.StartObject();
    writer.Key("addr");
    writer.String(addr.c_str());
    writer.Key("port");
    writer.Int(port);
    writer.Key("family");
    writer.Int(family);
    writer.Key("la");
    writer.Int(la);
    writer.Key("tx_bytes");
    writer.Uint64(tx_bytes);
    writer.Key("rx_bytes");
    writer.Uint64(rx_bytes);
    writer.EndObject();
}

void StorageSnapshot::Node::split_key(const std::string & key,
        std::string & addr, int & port, int & family)
{
    size_t pos_f = key.rfind(':');
    if (pos_f == std::string::npos)
        throw std::invalid_argument("node key with no family");
    family = std::stoi(key.substr(pos_f + 1));

    size_t pos_p = key.rfind(':', pos_f - 1);
    if (pos_p == std::string::npos)
        throw std::invalid_argument("node key with no port");
    port = std::stoi(key.substr(pos_p + 1, pos_f - pos_p - 1));

    addr = key.substr(0, pos_p);
}

StorageSnapshot::Backend::Backend()
    :
    id(),
    base_size(),
    records_total(),
    records_removed(),
    records_removed_size(),
    group(),
    state(),
    read_only(),
    last_start(),
    blob_size_limit(),
    fsid()
{}

void StorageSnapshot::Backend::split_key(const std::string & key, std::string & node, int & id)
{
    size_t pos = key.rfind('/');
    if (pos == std::string::npos)
        throw std::invalid_argument("backend key with no slash");
    id = std::stoi(key.substr(pos + 1));
    node = key.substr(0, pos);
}

std::string StorageSnapshot::Backend::create_key(const std::string & node, uint64_t id)
{
    return node + '/' + std::to_string(id);
}

void StorageSnapshot::Backend::print_json(
        rapidjson::Writer<rapidjson::StringBuffer> & writer) const
{
    writer.StartObject();

    writer.Key("node");
    writer.String(node.c_str());
    writer.Key("id");
    writer.Int(id);

    writer.Key("base_size");
    writer.Int(base_size);
    writer.Key("records_total");
    writer.Int(records_total);
    writer.Key("records_removed");
    writer.Int(records_removed);
    writer.Key("records_removed_size");
    writer.Int(records_removed_size);
    writer.Key("group");
    writer.Int(group);

    writer.Key("data_path");
    writer.String(data_path.c_str());

    writer.Key("state");
    writer.Int(state);
    writer.Key("read_only");
    writer.Bool(read_only);

    writer.Key("last_start");
    writer.StartObject();
        writer.Key("tv_sec");
        writer.Uint64(last_start.tv_sec);
        writer.Key("tv_usec");
        writer.Uint64(last_start.tv_usec);
    writer.EndObject();

    writer.Key("fsid");
    writer.Uint64(fsid);

    writer.Key("blob_size_limit");
    writer.Uint64(blob_size_limit);

    writer.EndObject();
}

StorageSnapshot::FS::FS()
    :
    fsid(),
    dstat(),
    vfs()
{}

void StorageSnapshot::FS::split_key(const std::string & key, std::string & node, uint64_t & fsid)
{
    size_t pos = key.rfind('/');
    if (pos == std::string::npos)
        throw std::invalid_argument("filesystem key with no slash");
    fsid = std::stoll(key.substr(pos + 1));
    node = key.substr(0, pos);
}

std::string StorageSnapshot::FS::create_key(const std::string & node, uint64_t fsid)
{
    return node + '/' + std::to_string(fsid);
}

void StorageSnapshot::FS::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const
{
    writer.StartObject();

    writer.Key("node");
    writer.String(node.c_str());
    writer.Key("fsid");
    writer.Uint64(fsid);

    writer.Key("dstat");
    writer.StartObject();
        writer.Key("read_ios");
        writer.Int(dstat.read_ios);
        writer.Key("write_ios");
        writer.Int(dstat.write_ios);
        writer.Key("read_ticks");
        writer.Int(dstat.read_ticks);
        writer.Key("write_ticks");
        writer.Int(dstat.write_ticks);
        writer.Key("io_ticks");
        writer.Int(dstat.io_ticks);
        writer.Key("read_sectors");
        writer.Int(dstat.read_sectors);
        writer.Key("error");
        writer.Int(dstat.error);
    writer.EndObject();

    writer.Key("vfs");
    writer.StartObject();
        writer.Key("blocks");
        writer.Int64(vfs.blocks);
        writer.Key("bavail");
        writer.Int64(vfs.bavail);
        writer.Key("bsize");
        writer.Int(vfs.bsize);
        writer.Key("error");
        writer.Int(vfs.error);
    writer.EndObject();

    writer.EndObject();
}

StorageSnapshot::Group::Group()
    :
    id(),
    metadata()
{}

void StorageSnapshot::Group::print_json(
        rapidjson::Writer<rapidjson::StringBuffer> & writer) const
{
    writer.StartObject();

    writer.Key("id");
    writer.Int(id);

    writer.Key("metadata");
    writer.StartObject();
        if (metadata.version) {
            writer.Key("version");
            writer.Int(metadata.version);
        }
        if (metadata.frozen) {
            writer.Key("frozen");
            writer.Bool(metadata.frozen);
        }
        if (!metadata.couple.empty()) {
            writer.Key("couple");
            writer.StartArray();
                for (int g : metadata.couple)
                    writer.Int(g);
            writer.EndArray();
        }
        if (!metadata.ns.empty()) {
            writer.Key("namespace");
            writer.String(metadata.ns.c_str());
        }
        if (!metadata.type.empty()) {
            writer.Key("type");
            writer.String(metadata.type.c_str());
        }
        if (metadata.service.migrating || !metadata.service.job_id.empty()) {
            writer.Key("service");
            writer.StartObject();
                if (metadata.service.migrating) {
                    writer.Key("migrating");
                    writer.Bool(metadata.service.migrating);
                    writer.Key("job_id");
                    writer.String(metadata.service.job_id.c_str());
                }
            writer.EndObject();
        }
    writer.EndObject();

    writer.Key("backends");
    writer.StartArray();
        for (const std::string & b : backends)
            writer.String(b.c_str());
    writer.EndArray();

    writer.EndObject();
}

StorageSnapshot::StorageSnapshot()
    :
    m_default_ts(0),
    m_history_ts(0),
    m_jobs_ts(0)
{}

StorageSnapshot::StorageSnapshot(const char *json)
    :
    m_history_ts(0),
    m_jobs_ts(0)
{
    update(json);
    complete();
}

void StorageSnapshot::update(const char *json)
{
    using namespace rapidjson;

    m_default_ts = clock_get_real();

    Document doc;
    doc.Parse(json);

    if (doc.HasParseError())
        throw std::invalid_argument("Wrong JSON");

    if (doc.HasMember("timestamp")) {
        Value & t = doc["timestamp"];
        if (!t.IsObject())
            throw std::invalid_argument("default timestamp is not an object");

        m_default_ts = t["tv_sec"].GetInt64() * 1000000000ULL + t["tv_usec"].GetInt64() * 1000ULL;
    }

    if (doc.HasMember("hosts")) {
        Value & hosts = doc["hosts"];
        if (!hosts.IsObject())
            throw std::invalid_argument("hosts is not an object");

        for (Value::MemberIterator it = hosts.MemberBegin(); it != hosts.MemberEnd(); ++it) {
            std::string addr = it->name.GetString();
            const Value & h = it->value;

            Host & host = m_hosts[addr];
            host.addr = addr;

            if (h.HasMember("name"))
                host.name = h["name"].GetString();
            if (h.HasMember("dc"))
                host.dc = h["dc"].GetString();
        }
    }

    if (doc.HasMember("nodes")) {
        Value & nodes = doc["nodes"];
        if (!nodes.IsObject())
            throw std::invalid_argument("nodes is not an object");

        for (Value::MemberIterator it = nodes.MemberBegin(); it != nodes.MemberEnd(); ++it) {
            std::string key = it->name.GetString();
            const Value & n = it->value;

            if (n.IsNull()) {
                m_nodes.erase(key);
                continue;
            }

            Node & node = m_nodes[key];

            Node::split_key(key, node.addr, node.port, node.family);

            if (n.HasMember("la"))
                node.la = n["la"].GetInt();
            if (n.HasMember("tx_bytes"))
                node.tx_bytes = n["tx_bytes"].GetUint64();
            if (n.HasMember("rx_bytes"))
                node.rx_bytes = n["rx_bytes"].GetUint64();
        }
    }

    if (doc.HasMember("backends")) {
        Value & backends = doc["backends"];

        if (!backends.IsObject())
            throw std::invalid_argument("backends is not an object");

        for (Value::MemberIterator it = backends.MemberBegin();
                it != backends.MemberEnd(); ++it) {
            std::string key = it->name.GetString();
            const Value & b = it->value;

            if (b.IsNull()) {
                m_backends.erase(key);
                continue;
            }

            Backend & backend = m_backends[key];

            Backend::split_key(key, backend.node, backend.id);

            if (b.HasMember("base_size"))
                backend.base_size = b["base_size"].GetInt();
            if (b.HasMember("records_total"))
                backend.records_total = b["records_total"].GetInt();
            if (b.HasMember("records_removed"))
                backend.records_removed = b["records_removed"].GetInt();
            if (b.HasMember("records_removed_size"))
                backend.records_removed_size = b["records_removed_size"].GetInt();

            if (b.HasMember("group"))
                backend.group = b["group"].GetInt();

            if (b.HasMember("data_path"))
                backend.data_path = b["data_path"].GetString();

            if (b.HasMember("state"))
                backend.state = b["state"].GetInt();
            if (b.HasMember("read_only"))
                backend.read_only = b["read_only"].GetBool();

            if (b.HasMember("last_start")) {
                const Value & ls = b["last_start"];
                if (ls.HasMember("tv_sec"))
                    backend.last_start.tv_sec = ls["tv_sec"].GetInt64();
                if (ls.HasMember("tv_usec"))
                    backend.last_start.tv_usec = ls["tv_usec"].GetInt64();
            }

            if (b.HasMember("blob_size_limit"))
                backend.blob_size_limit = b["blob_size_limit"].GetInt64();
            if (b.HasMember("fsid"))
                backend.fsid = b["fsid"].GetInt64();
        }
    }

    if (doc.HasMember("filesystems")) {
        Value & filesystems = doc["filesystems"];

        if (!filesystems.IsObject())
            throw std::invalid_argument("filesystems is not an object");

        for (Value::MemberIterator it = filesystems.MemberBegin();
                it != filesystems.MemberEnd(); ++it) {
            std::string key = it->name.GetString();
            const Value & f = it->value;

            if (f.IsNull()) {
                m_filesystems.erase(key);
                continue;
            }

            FS & fs = m_filesystems[key];

            FS::split_key(key, fs.node, fs.fsid);

            if (f.HasMember("dstat")) {
                const Value & d = f["dstat"];
                if (d.HasMember("read_ios"))
                    fs.dstat.read_ios = d["read_ios"].GetInt();
                if (d.HasMember("write_ios"))
                    fs.dstat.write_ios = d["write_ios"].GetInt();
                if (d.HasMember("read_ticks"))
                    fs.dstat.read_ticks = d["read_ticks"].GetInt();
                if (d.HasMember("write_ticks"))
                    fs.dstat.write_ticks = d["write_ticks"].GetInt();
                if (d.HasMember("io_ticks"))
                    fs.dstat.io_ticks = d["io_ticks"].GetInt();
                if (d.HasMember("read_sectors"))
                    fs.dstat.read_sectors = d["read_sectors"].GetInt();
                if (d.HasMember("error"))
                    fs.dstat.error = d["error"].GetInt();
            }

            if (f.HasMember("vfs")) {
                const Value & v = f["vfs"];
                if (v.HasMember("blocks"))
                    fs.vfs.blocks = v["blocks"].GetInt64();
                if (v.HasMember("bavail"))
                    fs.vfs.bavail = v["bavail"].GetInt64();
                if (v.HasMember("bsize"))
                    fs.vfs.bsize = v["bsize"].GetInt();
                if (v.HasMember("error"))
                    fs.vfs.error = v["error"].GetInt();
            }
        }
    }

    if (doc.HasMember("groups")) {
        Value & groups = doc["groups"];

        if (!groups.IsObject())
            throw std::invalid_argument("groups is not an object");

        for (Value::MemberIterator it = groups.MemberBegin();
                it != groups.MemberEnd(); ++it) {
            std::string id_str = it->name.GetString();
            const Value & g = it->value;

            int id = std::stoi(id_str);

            if (g.IsNull()) {
                m_groups.erase(id);
                continue;
            }

            Group & group = m_groups[id];
            group.id = id;

            if (g.HasMember("metadata")) {
                const Value & m = g["metadata"];
                if (m.HasMember("version"))
                    group.metadata.version = m["version"].GetInt();
                if (m.HasMember("frozen"))
                    group.metadata.frozen = m["frozen"].GetBool();
                if (m.HasMember("couple")) {
                    const Value & c = m["couple"];
                    if (!c.IsArray())
                        throw std::invalid_argument("couple is not an array");
                    for (SizeType i = 0; i < c.Size(); ++i)
                        group.metadata.couple.push_back(c[i].GetInt());
                }
                if (m.HasMember("namespace"))
                    group.metadata.ns = m["namespace"].GetString();
                if (m.HasMember("type"))
                    group.metadata.type = m["type"].GetString();
                if (m.HasMember("service")) {
                    const Value & s = m["service"];
                    if (s.HasMember("migrating"))
                        group.metadata.service.migrating = s["migrating"].GetBool();
                    if (s.HasMember("job_id"))
                        group.metadata.service.job_id = s["job_id"].GetString();
                }
            }

            if (g.HasMember("backends")) {
                const Value & b = g["backends"];
                if (!b.IsArray())
                    throw std::invalid_argument("group backends is not an array");

                group.backends.clear();
                for (SizeType i = 0; i < b.Size(); ++i)
                    group.backends.push_back(b[i].GetString());
            }
        }
    }

    if (doc.HasMember("history")) {
        const Value & history = doc["history"];

        if (!history.IsObject())
            throw std::invalid_argument("history is not an object");

        if (history.HasMember("timestamp"))
            m_history_ts = history["timestamp"].GetInt64();
        else
            m_history_ts = m_default_ts;

        if (history.HasMember("entries")) {
            const Value & s = history["entries"];
            if (!s.IsArray())
                throw std::invalid_argument("history/entries is not an array");

            for (Value::ConstValueIterator it = s.Begin(); it != s.End(); ++it) {
                const Value & e = *it;

                StringBuffer buf;
                Writer<StringBuffer> writer(buf);
                e.Accept(writer);

                mongo::BSONObj bson = mongo::fromjson(buf.GetString());
                GroupHistoryEntry entry(bson);

                m_history.emplace_back(std::move(entry));
            }
        }
    }

    if (doc.HasMember("jobs")) {
        const Value & jobs = doc["jobs"];

        if (!jobs.IsObject())
            throw std::invalid_argument("jobs is not an object");

        if (jobs.HasMember("timestamp"))
            m_jobs_ts = jobs["timestamp"].GetInt64();
        else
            m_jobs_ts = m_default_ts;

        if (jobs.HasMember("entries")) {
            const Value & s = jobs["entries"];
            if (!s.IsArray())
                throw std::invalid_argument("jobs/entries is not an array");

            for (Value::ConstValueIterator it = s.Begin(); it != s.End(); ++it) {
                const Value & j = *it;

                StringBuffer buf;
                Writer<StringBuffer> writer(buf);
                j.Accept(writer);

                mongo::BSONObj bson = mongo::fromjson(buf.GetString());
                Job job(bson, m_jobs_ts);

                m_jobs.emplace_back(std::move(job));
            }
        }
    }
}

void StorageSnapshot::complete()
{
    complete_nodes();
    complete_filesystems();
    complete_backends();
    complete_groups();
}

void StorageSnapshot::create_host(const std::string & addr)
{
    Host host;
    host.addr = addr;
    do {
        static int counter = 0;
        host.name = "node" + std::to_string(++counter) + ".example.com";
    } while (m_hosts.find(host.name) != m_hosts.end());
    host.dc = DEFAULT_DC;
    m_hosts[host.addr] = host;
}

void StorageSnapshot::complete_nodes()
{
    // Create hosts if needed.
    for (auto it = m_nodes.begin(); it != m_nodes.end(); ++it) {
        const Node & node = it->second;
        if (m_hosts.find(node.addr) == m_hosts.end())
            create_host(node.addr);
    }
}

void StorageSnapshot::create_node(const std::string & key)
{
    Node & node = m_nodes[key];
    Node::split_key(key, node.addr, node.port, node.family);

    if (m_hosts.find(node.addr) == m_hosts.end())
        create_host(node.addr);
}

void StorageSnapshot::complete_filesystems()
{
    // Create nodes if needed.
    for (auto it = m_filesystems.begin(); it != m_filesystems.end(); ++it) {
        const FS & fs = it->second;
        if (m_nodes.find(fs.node) == m_nodes.end())
            create_node(fs.node);
    }
}

uint64_t StorageSnapshot::create_filesystem(const std::string & node, uint64_t fsid)
{
    std::string key;
    if (!fsid) {
        static uint64_t fsid_gen = 1224124459;
        do {
            fsid = fsid_gen++;
            key = FS::create_key(node, fsid);
        } while (m_filesystems.find(key) != m_filesystems.end());
    } else {
        key = FS::create_key(node, fsid);
    }

    FS & fs = m_filesystems[key];
    fs.node = node;
    fs.fsid = fsid;

    fs.vfs.blocks = 0x40000000; // 4TB
    fs.vfs.bavail = fs.vfs.blocks - 11681;
    fs.vfs.bsize = 4096;

    if (m_nodes.find(node) == m_nodes.end())
        create_node(node);

    return fsid;
}

void StorageSnapshot::complete_backends()
{
    // Create filesystems if needed.
    for (auto it = m_backends.begin(); it != m_backends.end(); ++it) {
        const Backend & backend = it->second;
        if (m_filesystems.find(FS::create_key(backend.node, backend.fsid)) ==
                m_filesystems.end())
            create_filesystem(backend.node, backend.fsid);
    }
}

void StorageSnapshot::create_backend(const std::string & key, int group)
{
    std::string node;
    int id = 0;
    Backend::split_key(key, node, id);

    Backend & backend = m_backends[key];
    backend.node = node;
    backend.id = id;

    backend.group = group;
    backend.data_path = "/path/to/data/1/1";

    backend.state = 1;

    backend.blob_size_limit = 916ULL << 30;
    backend.fsid = create_filesystem(node, 0);
}

void StorageSnapshot::complete_groups()
{
    // Create backends if needed.
    for (auto it = m_groups.begin(); it != m_groups.end(); ++it) {
        const Group & group = it->second;
        for (const std::string & bkey : group.backends) {
            if (m_backends.find(bkey) == m_backends.end())
                create_backend(bkey, group.id);
        }
    }
}

void StorageSnapshot::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const
{
    writer.StartObject();
        writer.Key("default_ts");
        writer.Uint64(m_default_ts);

        writer.Key("hosts");
        writer.StartObject();
        for (auto it = m_hosts.begin(); it != m_hosts.end(); ++it) {
            const Host & host = it->second;
            writer.Key(it->first.c_str());
            host.print_json(writer);
        }
        writer.EndObject();

        writer.Key("nodes");
        writer.StartObject();
        for (auto it = m_nodes.begin(); it != m_nodes.end(); ++it) {
            const Node & node = it->second;
            writer.Key(it->first.c_str());
            node.print_json(writer);
        }
        writer.EndObject();

        writer.Key("backends");
        writer.StartObject();
        for (auto it = m_backends.begin(); it != m_backends.end(); ++it) {
            const Backend & backend = it->second;
            writer.Key(it->first.c_str());
            backend.print_json(writer);
        }
        writer.EndObject();

        writer.Key("filesystems");
        writer.StartObject();
        for (auto it = m_filesystems.begin(); it != m_filesystems.end(); ++it) {
            const FS & fs = it->second;
            writer.Key(it->first.c_str());
            fs.print_json(writer);
        }
        writer.EndObject();

        writer.Key("groups");
        writer.StartObject();
        for (auto it = m_groups.begin(); it != m_groups.end(); ++it) {
            const Group & group = it->second;
            writer.Key(std::to_string(it->first).c_str());
            group.print_json(writer);
        }
        writer.EndObject();

        writer.Key("history");
        writer.StartObject();
            writer.Key("timestamp");
            writer.Uint64(m_history_ts);
            writer.Key("entries");
            writer.StartArray();
            // TODO: uncomment after GroupHistoryEntry::print_json() is ready
            // for (const auto & entry : m_history)
            //    entry.print_json(writer);
            writer.EndArray();
        writer.EndObject();

        writer.Key("jobs");
        writer.StartObject();
            writer.Key("timestamp");
            writer.Uint64(m_jobs_ts);
            writer.Key("entries");
            writer.StartArray();
            for (const auto & job : m_jobs)
               job.print_json(writer);
            writer.EndArray();
        writer.EndObject();
    writer.EndObject();
}

std::ostream & operator << (std::ostream & ostr, const StorageSnapshot & snapshot)
{
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);

    snapshot.print_json(writer);
    ostr << buf.GetString();
    return ostr;
}
