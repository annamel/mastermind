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
#include "Config.h"
#include "Couple.h"
#include "Filter.h"
#include "FS.h"
#include "Group.h"
#include "GroupHistoryEntry.h"
#include "Logger.h"
#include "Metrics.h"
#include "Node.h"
#include "Storage.h"

#include <msgpack.hpp>

#include <algorithm>
#include <cstring>
#include <sstream>

namespace {

bool parse_couple(msgpack::object & obj, std::vector<int> & couple)
{
    if (obj.type != msgpack::type::ARRAY)
        return false;

    for (uint32_t i = 0; i < obj.via.array.size; ++i) {
        msgpack::object & gr_obj = obj.via.array.ptr[i];
        if (gr_obj.type != msgpack::type::POSITIVE_INTEGER)
            return false;
        couple.push_back(int(gr_obj.via.u64));
    }

    std::sort(couple.begin(), couple.end());

    return true;
}

} // unnamed namespace

void Group::Metadata::clear()
{
    version = 0;
    frozen = false;
    couple.clear();
    namespace_name.clear();
    type.clear();
    service.migrating = false;
    service.job_id.clear();
}

Group::Group(int id)
    :
    m_id(id),
    m_attr{{"group", id}},
    m_clean(true),
    m_update_time(0),
    m_metadata(),
    m_metadata_parsed(false),
    m_metadata_parse_duration(0),
    m_couple(nullptr),
    m_active_job(nullptr),
    m_type(DATA),
    m_status(INIT)
{}

bool Group::full(double reserved_space) const
{
    // Group is full if any backend is full.
    for (const Backend & backend : m_backends) {
        if (backend.full(reserved_space))
            return true;
    }
    return false;
}

uint64_t Group::get_total_space() const
{
    uint64_t res = 0;

    for (const Backend & backend : m_backends)
        res += backend.get_calculated().total_space;
    return res;
}

uint64_t Group::get_free_space() const
{
    uint64_t res = 0;

    for (const Backend & backend : m_backends)
        res += backend.get_calculated().free_space;
    return res;
}

uint64_t Group::get_effective_space() const
{
    uint64_t res = 0;

    for (const Backend & backend : m_backends)
        res += backend.get_calculated().effective_space;
    return res;
}

bool Group::has_active_job() const
{
    return m_active_job != nullptr;
}

const Job & Group::get_active_job() const
{
    return *m_active_job;
}

void Group::add_backend(Backend & backend)
{
    blackhole::scope::holder_t holder{app::logging::logger(), m_attr};
    LOG_DEBUG("Add backend {}", backend.get_key());

    m_backends.insert(backend);
}

void Group::remove_backend(Backend & backend)
{
    blackhole::scope::holder_t holder{app::logging::logger(), m_attr};
    LOG_DEBUG("Remove backend {}", backend.get_key());

    m_backends.erase(backend);
}

void Group::update_backends(const std::vector<std::string> & backends, uint64_t timestamp)
{
    blackhole::scope::holder_t holder{app::logging::logger(), m_attr};
    LOG_DEBUG("Applying history entry with timestamp {}", timestamp);

    bool changed = false;
    for (Backends::iterator it = m_backends.begin(); it != m_backends.end();) {
        const std::string & key = it->get().get_key();
        if (std::find(backends.begin(), backends.end(), key) == backends.end()) {
            m_backends.erase(it++);
            changed = true;
        } else {
            ++it;
        }
    }
    if (changed) {
        if (double(m_update_time) < timestamp)
            m_update_time = timestamp;
        else
            m_update_time = clock_get_real();
    }
}

void Group::handle_metadata_download_failed(const std::string & why)
{
    blackhole::scope::holder_t holder{app::logging::logger(), m_attr};
    LOG_ERROR("Metadata download failed: {}", why);

    m_metadata.clear();
    m_clean = true;
}

void Group::save_metadata(const char *metadata, size_t size, uint64_t timestamp)
{
    if (timestamp > m_update_time)
        m_update_time = timestamp;

    if (m_clean && !m_metadata_file.empty() && m_metadata_file.size() == size &&
            !std::memcmp(&m_metadata_file[0], metadata, size))
        return;

    m_update_time = clock_get_real();
    m_metadata_file.assign(metadata, metadata + size);
    m_clean = false;
}

bool Group::parse_metadata()
{
    if (m_clean)
        return true;

    m_clean = true;
    m_metadata_parsed = false;

    Stopwatch watch(m_metadata_parse_duration);

    m_status_text.clear();

    msgpack::unpacked result;
    msgpack::object obj;

    std::ostringstream ostr;
    try {
        msgpack::unpack(&result, &m_metadata_file[0], m_metadata_file.size());
        obj = result.get();
    } catch (std::exception & e) {
        ostr << "msgpack could not parse group metadata: " << e.what();
    } catch (...) {
        ostr << "msgpack could not parse group metadta with unknown reason";
    }

    if (!ostr.str().empty()) {
        m_status_text = ostr.str();
        m_status = BAD;
        return false;
    }

    int version = 0;
    std::vector<int> couple;
    std::string ns;
    bool frozen = false;
    std::string type;
    bool service_migrating = false;
    std::string service_job_id;

    if (obj.type == msgpack::type::MAP) {
        for (uint32_t i = 0; i < obj.via.map.size; ++i) {
            msgpack::object_kv & kv = obj.via.map.ptr[i];
            if (kv.key.type != msgpack::type::RAW)
                continue;

            uint32_t size = kv.key.via.raw.size;
            const char *ptr = kv.key.via.raw.ptr;

            if (size == 7 && !std::strncmp(ptr, "version", 7)) {
                if (kv.val.type == msgpack::type::POSITIVE_INTEGER) {
                    version = int(kv.val.via.u64);
                } else {
                    ostr << "Invalid 'version' value type " << kv.val.type;
                    break;
                }
            }
            else if (size == 6 && !std::strncmp(ptr, "couple", 6)) {
                if (!parse_couple(kv.val, couple)) {
                    ostr << "Couldn't parse 'couple'" << std::endl;
                    break;
                }
            }
            else if (size == 9 && !std::strncmp(ptr, "namespace", 9)) {
                if (kv.val.type == msgpack::type::RAW) {
                    ns.assign(kv.val.via.raw.ptr, kv.val.via.raw.size);
                } else {
                    ostr << "Invalid 'namespace' value type " << kv.val.type;
                    break;
                }
            }
            else if (size == 6 && !std::strncmp(ptr, "frozen", 6)) {
                if (kv.val.type == msgpack::type::BOOLEAN) {
                    frozen = kv.val.via.boolean;
                } else {
                    ostr << "Invalid 'frozen' value type " << kv.val.type;
                    break;
                }
            }
            else if (size == 4 && !std::strncmp(ptr, "type", 4)) {
                if (kv.val.type == msgpack::type::RAW) {
                    type.assign(kv.val.via.raw.ptr, kv.val.via.raw.size);
                } else {
                    ostr << "Invalid 'type' value type " << kv.val.type;
                    break;
                }
            }
            else if (size == 7 && !std::strncmp(ptr, "service", 7)) {
                if (kv.val.type == msgpack::type::MAP) {
                    bool ok = true;

                    for (uint32_t j = 0; j < kv.val.via.map.size; ++j) {
                        msgpack::object_kv & srv_kv = kv.val.via.map.ptr[j];
                        if (srv_kv.key.type != msgpack::type::RAW)
                            continue;

                        uint32_t srv_size = srv_kv.key.via.raw.size;
                        const char *srv_ptr = srv_kv.key.via.raw.ptr;

                        if (srv_size == 6 && !std::strncmp(srv_ptr, "status", 6)) {
                            // XXX
                            if (srv_kv.val.type == msgpack::type::RAW) {
                                uint32_t st_size = srv_kv.val.via.raw.size;
                                const char *st_ptr = srv_kv.val.via.raw.ptr;
                                if (st_size == 9 && !std::strncmp(st_ptr, "MIGRATING", 9))
                                    service_migrating = true;
                            }
                        }
                        else if (srv_size == 6 && !std::strncmp(srv_ptr, "job_id", 6)) {
                            if (srv_kv.val.type == msgpack::type::RAW) {
                                service_job_id.assign(srv_kv.val.via.raw.ptr, srv_kv.val.via.raw.size);
                            } else {
                                ostr << "Invalid 'job_id' value type " << srv_kv.val.type;
                                ok = false;
                                break;
                            }
                        }
                    }
                    if (!ok)
                        break;
                } else {
                    ostr << "Invalid 'service' value type " << kv.val.type;
                    break;
                }
            }
        }
    }
    else if (obj.type == msgpack::type::ARRAY) {
        version = 1;
        ns = "default";
        if (!parse_couple(obj, couple))
            ostr << "Couldn't parse couple (format of version 1)";
    }

    if (!ostr.str().empty()) {
        m_metadata.clear();
        m_clean = true;
        m_status_text = ostr.str();
        m_status = BAD;

        blackhole::scope::holder_t holder{app::logging::logger(), m_attr};
        LOG_ERROR("Metadata parse error: {}", m_status_text);
        return false;
    }

    m_metadata.version = version;
    m_metadata.frozen = frozen;
    m_metadata.couple = couple;
    m_metadata.namespace_name = ns;
    m_metadata.type = type;
    m_metadata.service.migrating = service_migrating;
    m_metadata.service.job_id = service_job_id;
    m_metadata_parsed = true;

    return true;
}

void Group::calculate_type()
{
    const std::string & cache_group_path_prefix = app::config().cache_group_path_prefix;

    if (!m_metadata.version) {
        if (!cache_group_path_prefix.empty()) {
            for (Backend & backend : m_backends) {
                if (!backend.get_base_path().compare(0,
                            cache_group_path_prefix.length(), cache_group_path_prefix)) {
                    m_type = UNMARKED;
                    return;
                }
            }
        }
    } else if (m_metadata.type == "cache") {
        m_type = CACHE;
        return;
    }
    m_type = DATA;
}

uint64_t Group::get_backend_update_time() const
{
    uint64_t res = 0;
    for (Backend & backend : m_backends) {
        uint64_t cur = backend.get_stat().get_timestamp() * 1000ULL;
        if (cur > res)
            res = cur;
    }
    return res;
}

void Group::set_active_job(const Job & job)
{
    m_active_job = &job;
}

void Group::clear_active_job()
{
    m_active_job = nullptr;
}

void Group::update_status()
{
    std::ostringstream ostr;

    // No backends -> state INIT
    if (m_backends.empty()) {
        m_status = INIT;
        ostr << "Group " << m_id << " is in state INIT because there are "
                "no node backends serving this group";
        m_status_text = ostr.str();
        return;
    }

    // Report BROKEN if the group has several backends but DHT groups are forbidden.
    if (app::config().forbidden_dht_groups && m_backends.size() > 1) {
        m_status = BROKEN;
        ostr << "Group " << m_id << " is in state BROKEN because it has "
             << m_backends.size() << " backends but an option "
                "'forbidden_dht_groups' is set";
        m_status_text = ostr.str();
        return;
    }

    // No metadata -> state INIT
    if (!m_metadata.version) {
        m_status = INIT;
        ostr << "Group " << m_id << " is in state INIT because meta key "
                "was not read from it.";
        m_status_text = ostr.str();
    }

    auto it = std::find_if(m_backends.begin(), m_backends.end(),
                [] (const Backend & backend) { return backend.get_status() == Backend::BROKEN; });

    // Have BROKEN backend -> state BROKEN
    if (it != m_backends.end()) {
        m_status = BROKEN;
        ostr << "Group " << m_id << " is in state BROKEN because backend "
             << it->get().get_key() << " is broken.";
        m_status_text = ostr.str();
        return;
    }

    if (m_type == DATA) {
        // Metadata & couple checks
        if (update_storage_group_status())
            return;
    }

    for (const Backend & backend : m_backends) {
        if (backend.get_status() == Backend::RO) {
            if (m_metadata.service.migrating) {
                if (m_active_job != nullptr && m_active_job->get_id() == m_metadata.service.job_id) {
                    // MIGRATING if job is found
                    m_status = MIGRATING;
                    ostr << "Group " << m_id << " is migrating, job id is "
                         << m_metadata.service.job_id << '.';
                    m_status_text = ostr.str();
                } else {
                    // Job is not found -> state BAD
                    m_status = BAD;
                    ostr << "Group " << m_id << " has no active job but marked as migrating "
                            "by job id " << m_metadata.service.job_id << '.';
                    m_status_text = ostr.str();
                }
            } else {
                // Have Read-Only backends -> state RO
                m_status = RO;
                ostr << "Group " << m_id << " is Read-Only because backend "
                     << backend.get_key() << " is Read-Only.";
            }
            return;
        }
    }

    it = std::find_if(m_backends.begin(), m_backends.end(),
                [] (const Backend & backend) { return backend.get_status() != Backend::OK; });

    // Have BAD backends -> state BAD
    if (it != m_backends.end()) {
        m_status = BAD;
        ostr << "Group " << m_id << " is in state BAD because backend " << it->get().get_key()
             << " is in state " << Backend::status_str(it->get().get_status());
        m_status_text = ostr.str();
        return;
    }

    // Everything is fine.
    m_status = COUPLED;
    ostr << "Group " << m_id << " is OK";
    m_status_text = ostr.str();
}

void Group::update_status_recursive()
{
    if (m_couple != nullptr)
        m_couple->update_status();
    else
        this->update_status();
}

bool Group::have_metadata_conflict(const Group & other)
{
    if (!m_metadata.version)
        return false;

    auto my = std::tie(m_metadata.couple,
            m_metadata.namespace_name, m_metadata.type);

    auto oth = std::tie(other.m_metadata.couple,
            other.m_metadata.namespace_name, other.m_metadata.type);

    return (my != oth);
}

bool Group::update_storage_group_status()
{
    std::ostringstream ostr;

    if (m_metadata.couple.empty()) {
        m_status = INIT;
        ostr << "Group " << m_id << " is in state INIT because there is no coupling info.";
        m_status_text = ostr.str();
        return true;
    }

    if (m_couple == nullptr) {
        m_status = BAD;
        ostr << "Group " << m_id << " is in state BAD because couple was not created.";
        m_status_text = ostr.str();
        return true;
    }

    if (!m_couple->check_groups(m_metadata.couple)) {
        m_status = BAD;
        ostr << "Group " << m_id << " is in state BAD because couple check fails.";
        m_status_text = ostr.str();
        return true;
    }

    if (m_metadata.namespace_name.empty()) {
        m_status = BAD;
        ostr << "Group " << m_id << " is in state BAD because there is no namespace assigned to it.";
        m_status_text = ostr.str();
        return true;
    }

    if (std::find(m_metadata.couple.begin(), m_metadata.couple.end(), m_id) == m_metadata.couple.end()) {
        m_status = BROKEN;
        ostr << "Group " << m_id << " is in state BROKEN because its id is missing in coupling info.";
        m_status_text = ostr.str();
        return true;
    }

    return false;
}

void Group::set_couple(Couple & couple)
{
    m_couple = &couple;
}

bool Group::match_couple(const Group & other) const
{
    if (m_couple == nullptr || other.m_couple == nullptr)
        return false;
    return m_couple == other.m_couple;
}

void Group::merge(const Group & other, bool & have_newer)
{
    /// XXX
    // Merge logic should be removed in the near future.

    if (m_update_time > other.m_update_time) {
        have_newer = true;
        return;
    }

    if (m_update_time == other.m_update_time)
        return;

    for (Backends::iterator it = m_backends.begin(); it != m_backends.end();) {
        const std::string & key = it->get().get_key();
        auto oth_it = std::find_if(other.m_backends.begin(), other.m_backends.end(),
                [key] (const Backend & backend) { return backend.get_key() == key; });
        if (oth_it == other.m_backends.end())
            m_backends.erase(it++);
        else
            ++it;
    }

    m_clean = other.m_clean;
    m_metadata_file = other.m_metadata_file;
    m_update_time = other.m_update_time;

    m_metadata = other.m_metadata;

    m_metadata_parsed = other.m_metadata_parsed;
    m_metadata_parse_duration = other.m_metadata_parse_duration;

    m_type = other.m_type;
    m_status_text = other.m_status_text;
    m_status = other.m_status;
}

void Group::push_items(std::vector<std::reference_wrapper<Couple>> & couples) const
{
    if (m_couple != nullptr)
        couples.push_back(*m_couple);
}

void Group::push_items(std::vector<std::reference_wrapper<Namespace>> & namespaces) const
{
    if (m_couple != nullptr)
        m_couple->push_items(namespaces);
}

void Group::push_items(std::vector<std::reference_wrapper<Node>> & nodes) const
{
    for (Backend & backend : m_backends)
        backend.push_items(nodes);
}

void Group::push_items(std::vector<std::reference_wrapper<Backend>> & backends) const
{
    backends.insert(backends.end(), m_backends.begin(), m_backends.end());
}

void Group::push_items(std::vector<std::reference_wrapper<FS>> & filesystems) const
{
    for (Backend & backend : m_backends)
        backend.push_items(filesystems);
}

void Group::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
        bool show_internals) const
{
    writer.StartObject();

    writer.Key("id");
    writer.Uint64(m_id);

    if (m_couple != nullptr) {
        writer.Key("couple");
        writer.String(m_couple->get_key().c_str());
    }

    writer.Key("backends");
    writer.StartArray();
    for (Backend & backend : m_backends)
        writer.String(backend.get_key().c_str());
    writer.EndArray();

    writer.Key("status_text");
    writer.String(m_status_text.c_str());
    writer.Key("status");
    writer.String(status_str(m_status));
    writer.Key("type");
    writer.String(type_str(m_type));

    if (m_metadata_parsed) {
        writer.Key("frozen");
        writer.Bool(m_metadata.frozen);
        writer.Key("version");
        writer.Uint64(m_metadata.version);
        writer.Key("namespace");
        writer.String(m_metadata.namespace_name.c_str());
        if (m_metadata.service.migrating || !m_metadata.service.job_id.empty()) {
            writer.Key("service");
            writer.StartObject();
            writer.Key("migrating");
            writer.Bool(m_metadata.service.migrating);
            writer.Key("job_id");
            writer.String(m_metadata.service.job_id.c_str());
            writer.EndObject();
        }
    }

    if (m_active_job != nullptr) {
        writer.Key("active_job");
        writer.StartObject();
        writer.Key("type");
        writer.String(Job::type_str(m_active_job->get_type()));
        writer.Key("status");
        writer.String(Job::status_str(m_active_job->get_status()));
        writer.EndObject();
    }

    if (show_internals) {
        writer.Key("clean");
        writer.Bool(m_clean);
        writer.Key("update_time");
        writer.Uint64(m_update_time);
        writer.Key("metadata_parsed");
        writer.Bool(m_metadata_parsed);
        writer.Key("metadata_parse_duration");
        writer.Uint64(m_metadata_parse_duration);

        writer.Key("metadata_internal");
        writer.StartObject();
            writer.Key("version");
            writer.Uint64(m_metadata.version);
            writer.Key("frozen");
            writer.Bool(m_metadata.frozen);
            writer.Key("couple");
            writer.StartArray();
            for (int id : m_metadata.couple)
                writer.Uint64(id);
            writer.EndArray();
            writer.Key("namespace_name");
            writer.String(m_metadata.namespace_name.c_str());
            if (!m_metadata.type.empty()) {
                writer.Key("type");
                writer.String(m_metadata.type.c_str());
            }
            writer.Key("service");
            writer.StartObject();
                writer.Key("migrating");
                writer.Bool(m_metadata.service.migrating);
                writer.Key("job_id");
                writer.String(m_metadata.service.job_id.c_str());
            writer.EndObject();
        writer.EndObject();
    }

    writer.EndObject();
}

const char *Group::status_str(Status status)
{
    switch (status)
    {
    case INIT:
        return "INIT";
    case COUPLED:
        return "COUPLED";
    case BAD:
        return "BAD";
    case BROKEN:
        return "BROKEN";
    case RO:
        return "RO";
    case MIGRATING:
        return "MIGRATING";
    }
    return "UNKNOWN";
}

const char *Group::type_str(Type type)
{
    switch (type)
    {
    case DATA:
        return "DATA";
    case CACHE:
        return "CACHE";
    case UNMARKED:
        return "UNMARKED";
    }
    return "UNKNOWN";
}
