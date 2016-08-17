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

#include "Config.h"
#include "Discovery.h"
#include "Host.h"
#include "Logger.h"
#include "Round.h"
#include "WorkerApplication.h"

#include <blackhole/wrapper.hpp>
#include <curl/curl.h>
#include <mongo/client/dbclient.h>
#include <netdb.h>
#include <sys/socket.h>

#include <cstring>
#include <set>

#include <netdb.h>
#include <sys/socket.h>

using namespace ioremap;

namespace {

struct dnet_addr_compare
{
    bool operator () (const dnet_addr & a, const dnet_addr & b) const
    {
        return memcmp(&a, &b, sizeof(dnet_addr)) < 0;
    }
};

} // unnamed namespace

Discovery::Discovery(Collector & collector)
    :
    m_collector(collector),
    m_resolve_nodes_duration(0)
{}

Discovery::~Discovery()
{}

int Discovery::init_curl()
{
    if (curl_global_init(CURL_GLOBAL_ALL)) {
        LOG_ERROR("Failed to initialize libcurl");
        return -1;
    }
    return 0;
}

int Discovery::init_elliptics()
{
    const Config & config = app::config();

    dnet_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.wait_timeout = config.wait_timeout;
    cfg.net_thread_num = config.net_thread_num;
    cfg.io_thread_num = config.io_thread_num;
    cfg.nonblocking_io_thread_num = config.nonblocking_io_thread_num;

    std::unique_ptr<dnet_logger> logger;
    logger.reset(new blackhole::wrapper_t{app::logging::elliptics_logger(), {}});

    m_node.reset(new elliptics::node{std::move(logger)});

    LOG_NOTICE("Initializing discovery");

    for (size_t i = 0; i < config.nodes.size(); ++i) {
        const Config::NodeInfo & info = config.nodes[i];

        try {
            m_node->add_remote(elliptics::address(info.host, info.port, info.family));
        } catch (std::exception & e) {
            LOG_WARNING("Failed to add remote '{}': {}", info.host, e.what());
            continue;
        } catch (...) {
            LOG_WARNING("Failed to add remote '{}' for unknown reason", info.host);
            continue;
        }
    }

    m_session.reset(new elliptics::session(*m_node));
    m_session->set_cflags(DNET_FLAGS_NOLOCK);

    return 0;
}

int Discovery::init_mongo()
{
    mongo::Status status = mongo::client::initialize(mongo::client::Options().setIPv6Enabled(true));
    if (!status.isOK()) {
        LOG_ERROR("Failed to initialize mongo client: {}", status.toString());
        return -1;
    }

    return 0;
}

void Discovery::resolve_nodes(Round & round)
{
    Stopwatch watch(m_resolve_nodes_duration);

    if (m_session == NULL) {
        LOG_WARNING("resolve_nodes: session is empty");
        return;
    }

    std::set<dnet_addr, dnet_addr_compare> addresses;

    std::vector<dnet_route_entry> routes;
    routes = m_session->get_routes();

    for (size_t i = 0; i < routes.size(); ++i)
        addresses.insert(routes[i].addr);

    for (auto it = addresses.begin(); it != addresses.end(); ++it) {
        const dnet_addr & addr = *it;

        const char *host_addr = dnet_addr_host_string(&addr);
        int port = dnet_addr_port(&addr);

        Storage & storage = round.get_storage();
        Host & host = storage.get_host(host_addr);

        if (host.get_name().empty()) {
            char hostname[NI_MAXHOST];

            int rc = getnameinfo((const sockaddr *) addr.addr, addr.addr_len,
                    hostname, sizeof(hostname), nullptr, 0, 0);

            if (rc != 0) {
                LOG_ERROR("Failed to resolve hostname for node {}:{}:{}: {}",
                        host_addr, port, addr.family, gai_strerror(rc));
            } else {
                host.set_name(hostname);
            }
        }

        if (!host.get_name().empty()) {
            std::string dc = m_collector.get_inventory().get_dc_by_host(host.get_name().c_str());
            host.set_dc(dc);
        }

        if (!storage.has_node(host_addr, port, addr.family))
            storage.add_node(host, port, addr.family);
    }
}

void Discovery::stop_mongo()
{
    mongo::client::shutdown();
}

void Discovery::stop_elliptics()
{
    m_session.reset();
    m_node.reset();
}

void Discovery::stop_curl()
{
    curl_global_cleanup();
}
