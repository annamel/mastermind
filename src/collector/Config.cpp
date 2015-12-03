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

constexpr uint64_t Config::Default::monitor_port;
constexpr uint64_t Config::Default::wait_timeout;
constexpr uint64_t Config::Default::forbidden_dht_groups;
constexpr uint64_t Config::Default::forbidden_unmatched_group_total_space;
constexpr uint64_t Config::Default::forbidden_ns_without_settings;
constexpr uint64_t Config::Default::forbidden_dc_sharing_among_groups;
constexpr uint64_t Config::Default::reserved_space;
constexpr uint64_t Config::Default::node_backend_stat_stale_timeout;
constexpr uint64_t Config::Default::dnet_log_mask;
constexpr uint64_t Config::Default::net_thread_num;
constexpr uint64_t Config::Default::io_thread_num;
constexpr uint64_t Config::Default::nonblocking_io_thread_num;
constexpr uint64_t Config::Default::infrastructure_dc_cache_update_period;
constexpr uint64_t Config::Default::infrastructure_dc_cache_valid_time;
constexpr uint64_t Config::Default::inventory_worker_timeout;
constexpr uint64_t Config::Default::metadata_options_connectTimeoutMS;

constexpr const char *Config::Default::config_file;
constexpr const char *Config::Default::log_file;
constexpr const char *Config::Default::elliptics_log_file;
