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

#ifndef __bbd6c304_2898_4ea0_afae_ee8b16e21893
#define __bbd6c304_2898_4ea0_afae_ee8b16e21893

#include <memory>
#include <string>

#include <cocaine/framework/services/app.hpp>
#include <dispatch/dispatch.h>
#include <mongo/client/dbclient.h>

// Inventory provides an information about DC layout.
// Currently it implements get_dc_by_host().
//
// For implementation details see comments on class members, and
// on methods get_dc_by_host() and execute_reload() in .cpp file.

class Inventory
{
public:
    Inventory();
    ~Inventory();

    void init();

    // Download all hostname-dc entries from MongoDB.
    // Should be called before the first round.
    void download_initial();

    void stop();

    // Get name of DC by hostname. First it looks for information in
    // in-memory cache. If not found, tries to call the cocaine worker.
    // If the worker is unavailable, value of addr is returned.
    std::string get_dc_by_host(const std::string & addr);

private:
    struct HostInfo
    {
        HostInfo()
            : timestamp(0)
        {}

        HostInfo(const mongo::BSONObj & obj);

        mongo::BSONObj obj() const;

        std::string host;
        std::string dc;
        time_t timestamp;
    };

    void cocaine_connect();

    int cache_db_connect();

    // Update host entry in database.
    void cache_db_update(const HostInfo & info);

    void dispatch_next_reload();

    // Download host information from MongoDB.
    std::vector<HostInfo> load_cache_db();

    // Prepare complete host information.
    std::vector<HostInfo> load_hosts();

    // Functions below are executed in common queue.
    // Structures *Data are declared here to be able to add member of type HostInfo.
    struct GetDcData;
    static void execute_get_dc_by_host(void *arg);
    static void execute_reload(void *arg);
    struct SaveUpdateData;
    static void execute_save_update(void *arg);

    // Functions below are executed in update queue.
    struct CacheDbUpdateData;
    static void execute_cache_db_update(void *arg);

    int fetch_from_cocaine(HostInfo & info);

private:
    // Host info cache.
    std::map<std::string, HostInfo> m_host_info;

    std::shared_ptr<cocaine::framework::service_manager_t> m_manager;
    std::shared_ptr<cocaine::framework::app_service_t> m_service;

    std::unique_ptr<mongo::DBClientReplicaSet> m_conn;
    std::string m_collection_name;
    double m_last_update_time;

    // Common queue is used to serialize access to m_host_info. This is serial queue.
    // Two kinds of events are handled in it:
    // 1) execute_get_dc_by_host() is synchronously dispatched when the user calls get_dc_by_host().
    // 2) When some entries were updated in the database and downloaded during regular reload,
    //    the data is put into container asynchronously in this queue.
    dispatch_queue_t m_common_queue;
    // Update queue is used for operations on database. This is concurrent queue.
    // Two kinds events are handled in it:
    // 1) Regular database reload. It is executed by timer (using dispatch_after_f).
    // 2) Adding database record. When get_dc_by_host() fails to find an information in
    //    m_host_info it calls cocaine handle. When this job is done, it creates
    //    asynchronous event to put the record into the database.
    dispatch_queue_t m_update_queue;

    bool m_stopped;
};

#endif

