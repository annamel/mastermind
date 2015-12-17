/*
   Copyright (c) YANDEX LLC, 2015. All rights reserved.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 3.0 of the License, or (at your option) any later version.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with this library.
*/

#include <Config.h>
#include <ConfigParser.h>

#include <rapidjson/writer.h>

#include <gtest/gtest.h>

TEST(ConfigParserTest, ParseFull)
{
    // This test verifies parsing of config with all values set.
    // Values are chosen to be different from defaults.
    // This way we make sure no values are getting lost.

    // Make sure our example differs from defaults.
    ASSERT_EQ(0, Config::Default::forbidden_dht_groups);
    ASSERT_EQ(0, Config::Default::forbidden_unmatched_group_total_space);
    ASSERT_EQ(0, Config::Default::forbidden_ns_without_settings);
    ASSERT_EQ(0, Config::Default::forbidden_dc_sharing_among_groups);

    const char *str = R"END(
        {
            "app_name": "43672b96-267d-410b-8a3b-b36bd5438e6e",
            "elliptics": {
                "monitor_port": 803827663,
                "wait_timeout": 2981119697,
                "nodes": [
                    [ "3c76e872-3e6a-4b2e-992c-7f5cd38c29ea", 27611, 31957 ],
                    [ "28d9781a-0136-4036-bb2a-47e25f38b883", 28669, 17443 ]
                ]
            },
            "forbidden_dht_groups": true,
            "forbidden_unmatched_group_total_space": true,
            "forbidden_ns_without_settings": true,
            "forbidden_dc_sharing_among_groups": true,
            "reserved_space": 183788617,
            "node_backend_stat_stale_timeout": 2492923109,
            "dnet_log_mask": 1259513999,
            "net_thread_num": 722817013,
            "io_thread_num": 238712039,
            "nonblocking_io_thread_num": 2672210171,
            "infrastructure_dc_cache_update_period": 2696525719,
            "infrastructure_dc_cache_valid_time": 274434691,
            "inventory_worker_timeout": 2549324119,
            "cache": {
                "group_path_prefix": "dd06e0d5-a770-4b44-b432-de6f8e37080c"
            },
            "metadata": {
                "url": "29e1bdec-b495-4c56-9922-be888bee0e38",
                "options": {
                    "connectTimeoutMS": 1949230429
                },
                "history": {
                    "db": "718afe27-e553-4c2d-92fa-7f81ed1e0eb7"
                },
                "inventory": {
                    "db": "1fadfbc2-1b9e-419b-b9ca-ec10bde1d36a"
                },
                "jobs": {
                    "db": "687a97b1-6ec7-4dec-bc23-91649208dfd0"
                }
            }
        }
    )END";

    Config config;
    ConfigParser parser(config);

    rapidjson::Reader reader;
    rapidjson::StringStream stream(str);
    reader.Parse(stream, parser);

    ASSERT_TRUE(parser.good());

    EXPECT_EQ("43672b96-267d-410b-8a3b-b36bd5438e6e", config.app_name);
    EXPECT_EQ(803827663ULL, config.monitor_port);
    EXPECT_EQ(2981119697ULL, config.wait_timeout);
    EXPECT_EQ(1, config.forbidden_dht_groups);
    EXPECT_EQ(1, config.forbidden_unmatched_group_total_space);
    EXPECT_EQ(1, config.forbidden_ns_without_settings);
    EXPECT_EQ(1, config.forbidden_dc_sharing_among_groups);
    EXPECT_EQ(183788617ULL, config.reserved_space);
    EXPECT_EQ(2492923109ULL, config.node_backend_stat_stale_timeout);
    EXPECT_EQ(1259513999ULL, config.dnet_log_mask);
    EXPECT_EQ(722817013ULL, config.net_thread_num);
    EXPECT_EQ(238712039ULL, config.io_thread_num);
    EXPECT_EQ(2672210171ULL, config.nonblocking_io_thread_num);
    EXPECT_EQ(2696525719ULL, config.infrastructure_dc_cache_update_period);
    EXPECT_EQ(274434691ULL, config.infrastructure_dc_cache_valid_time);
    EXPECT_EQ(2549324119ULL, config.inventory_worker_timeout);
    EXPECT_EQ("dd06e0d5-a770-4b44-b432-de6f8e37080c", config.cache_group_path_prefix);
    EXPECT_EQ("29e1bdec-b495-4c56-9922-be888bee0e38", config.metadata.url);
    EXPECT_EQ(1949230429ULL, config.metadata.options.connectTimeoutMS);
    EXPECT_EQ("718afe27-e553-4c2d-92fa-7f81ed1e0eb7", config.metadata.history.db);
    EXPECT_EQ("1fadfbc2-1b9e-419b-b9ca-ec10bde1d36a", config.metadata.inventory.db);
    EXPECT_EQ("687a97b1-6ec7-4dec-bc23-91649208dfd0", config.metadata.jobs.db);


    ASSERT_EQ(2, config.nodes.size());
    EXPECT_EQ("3c76e872-3e6a-4b2e-992c-7f5cd38c29ea", config.nodes[0].host);
    EXPECT_EQ(27611, config.nodes[0].port);
    EXPECT_EQ(31957, config.nodes[0].family);
    EXPECT_EQ("28d9781a-0136-4036-bb2a-47e25f38b883", config.nodes[1].host);
    EXPECT_EQ(28669, config.nodes[1].port);
    EXPECT_EQ(17443, config.nodes[1].family);
}

TEST(ConfigParserTest, ConfigCtor)
{
    // Verify Config object construction. All values must be set to defaults.

    Config config;
    config.~Config();
    // Write junk bytes and check members initialization afterwards.
    memset(&config, 0x5A, sizeof(config));
    new (&config) Config;

    EXPECT_EQ(Config::Default::monitor_port, config.monitor_port);
    EXPECT_EQ(Config::Default::wait_timeout, config.wait_timeout);
    EXPECT_EQ(Config::Default::forbidden_dht_groups, config.forbidden_dht_groups);
    EXPECT_EQ(Config::Default::forbidden_unmatched_group_total_space, config.forbidden_unmatched_group_total_space);
    EXPECT_EQ(Config::Default::forbidden_ns_without_settings, config.forbidden_ns_without_settings);
    EXPECT_EQ(Config::Default::forbidden_dc_sharing_among_groups, config.forbidden_dc_sharing_among_groups);
    EXPECT_EQ(Config::Default::reserved_space, config.reserved_space);
    EXPECT_EQ(Config::Default::node_backend_stat_stale_timeout, config.node_backend_stat_stale_timeout);
    EXPECT_EQ(Config::Default::dnet_log_mask, config.dnet_log_mask);
    EXPECT_EQ(Config::Default::net_thread_num, config.net_thread_num);
    EXPECT_EQ(Config::Default::io_thread_num, config.io_thread_num);
    EXPECT_EQ(Config::Default::nonblocking_io_thread_num, config.nonblocking_io_thread_num);
    EXPECT_EQ(Config::Default::infrastructure_dc_cache_update_period, config.infrastructure_dc_cache_update_period);
    EXPECT_EQ(Config::Default::infrastructure_dc_cache_valid_time, config.infrastructure_dc_cache_valid_time);
    EXPECT_EQ(Config::Default::inventory_worker_timeout, config.inventory_worker_timeout);
    EXPECT_EQ(Config::Default::metadata_options_connectTimeoutMS, config.metadata.options.connectTimeoutMS);
}
