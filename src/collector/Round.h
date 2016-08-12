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

#ifndef __7e0141a2_6399_4571_bb8b_251f3747c5cd
#define __7e0141a2_6399_4571_bb8b_251f3747c5cd

#include "Discovery.h"
#include "Filter.h"
#include "Storage.h"

#include <cocaine/framework/worker.hpp>
#include <curl/curl.h>
#include <dispatch/dispatch.h>
#include <elliptics/session.hpp>

#include <atomic>
#include <memory>

class Collector;
class Node;

class on_force_update;
class on_refresh;

class Round
{
public:
    enum Type {
        REGULAR,
        FORCED_FULL,
        FORCED_PARTIAL
    };

    Round(Collector & collector);
    Round(Collector & collector, cocaine::framework::worker::sender tx);
    Round(Collector & collector, cocaine::framework::worker::sender tx, Filter filter);
    ~Round();

    Collector & get_collector()
    { return m_collector; }

    Storage & get_storage()
    { return *m_storage; }

    uint64_t get_old_storage_version() const
    { return m_old_storage_version; }

    const Storage::Entries & get_entries()
    { return m_entries; }

    ioremap::elliptics::session & get_session()
    { return m_session; }

    Type get_type() const
    { return m_type; }

    cocaine::framework::worker::sender & get_cocaine_sender()
    { return *m_cocaine_sender; }

    void swap_storage(std::unique_ptr<Storage> & storage)
    { m_storage.swap(storage); }

    void update_storage(Storage & storage, uint64_t version, bool & have_newer);

    void start();

private:
    static void step2_1_jobs_and_history(void *arg);
    static void step2_2_curl_download(void *arg);
    static void step3_prepare_metadata_download(void *arg);
    static void step4_perform_update(void *arg);

    int perform_download();
    int add_download(Node & node);

    static void request_metadata_apply_helper(void *arg);
    static void request_group_metadata(void *arg, size_t idx);

    // done reading a metakey from a single group
    void result(size_t group_idx, const ioremap::elliptics::read_result_entry & entry);
    void final(size_t group_idx, const ioremap::elliptics::error_info & error);

public:
    CURL *create_easy_handle(Node *node);

    static int handle_socket(CURL *easy, curl_socket_t fd,
            int action, void *userp, void *socketp);

    static int handle_timer(CURLM *multi, long timeout_ms, void *userp);

    static size_t write_func(char *ptr, size_t size,
            size_t nmemb, void *userdata);

public:
    struct ClockStat
    {
        ClockStat();

        ClockStat & operator = (const ClockStat & other);

        uint64_t total;
        uint64_t mongo;
        uint64_t perform_download;
        uint64_t finish_monitor_stats_and_jobs;
        uint64_t metadata_download;
        uint64_t storage_update;
        uint64_t merge_time;
    };

    ClockStat & get_clock()
    { return m_clock; }

private:
    Collector & m_collector;

    std::unique_ptr<Storage> m_storage;
    uint64_t m_old_storage_version;

    ioremap::elliptics::session m_session;

    Type m_type;
    std::unique_ptr<cocaine::framework::worker::sender> m_cocaine_sender;
    std::unique_ptr<Filter> m_filter;

    Storage::Entries m_entries;

    std::vector<std::reference_wrapper<Group>> m_groups_to_read;
    std::vector<ioremap::elliptics::session> m_group_read_sessions;

    dispatch_queue_t m_queue;

    int m_epollfd;
    CURLM *m_curl_handle;
    long m_timeout_ms;

    std::atomic<int> m_nr_groups;

    ClockStat m_clock;
};

#endif

