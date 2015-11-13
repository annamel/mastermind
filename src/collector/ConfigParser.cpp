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

#include "ConfigParser.h"

#include <cstddef>

namespace {

enum ConfigKey
{
    Elliptics                         = 2,
    ForbiddenDhtGroups                = 4,
    ForbiddenUnmatchedGroupTotalSpace = 8,
    ReservedSpace                     = 0x10,
    DnetLogMask                       = 0x20,
    NetThreadNum                      = 0x40,
    IoThreadNum                       = 0x80,
    NonblockingIoThreadNum            = 0x100,
    Nodes                             = 0x200,
    MonitorPort                       = 0x400,
    WaitTimeout                       = 0x800,
    Metadata                          = 0x1000,
    Url                               = 0x2000,
    Jobs                              = 0x4000,
    History                           = 0x8000,
    Inventory                         = 0x10000,
    Db                                = 0x20000,
    Options                           = 0x40000,
    ConnectTimeoutMS                  = 0x80000,
    NodeBackendStatStaleTimeout       = 0x100000,
    Cache                             = 0x200000,
    GroupPathPrefix                   = 0x400000,
    ForbiddenNsWithoutSettings        = 0x800000,
    ForbiddenDcSharingAmongGroups     = 0x1000000,
    AppName                           = 0x2000000,
    InfrastructureDcCacheUpdatePeriod = 0x4000000,
    InfrastructureDcCacheValidTime    = 0x8000000,
    InventoryWorkerTimeout            = 0x10000000
};

std::vector<Parser::FolderVector> config_folders = {
    {
        { "elliptics",                             0, Elliptics                         },
        { "forbidden_dht_groups",                  0, ForbiddenDhtGroups                },
        { "forbidden_unmatched_group_total_space", 0, ForbiddenUnmatchedGroupTotalSpace },
        { "forbidden_ns_without_settings",         0, ForbiddenNsWithoutSettings        },
        { "forbidden_dc_sharing_among_groups",     0, ForbiddenDcSharingAmongGroups     },
        { "app_name",                              0, AppName                           },
        { "reserved_space",                        0, ReservedSpace                     },
        { "node_backend_stat_stale_timeout",       0, NodeBackendStatStaleTimeout       },
        { "dnet_log_mask",                         0, DnetLogMask                       },
        { "net_thread_num",                        0, NetThreadNum                      },
        { "io_thread_num",                         0, IoThreadNum                       },
        { "nonblocking_io_thread_num",             0, NonblockingIoThreadNum            },
        { "infrastructure_dc_cache_update_period", 0, InfrastructureDcCacheUpdatePeriod },
        { "infrastructure_dc_cache_valid_time",    0, InfrastructureDcCacheValidTime    },
        { "inventory_worker_timeout",              0, InventoryWorkerTimeout            },
        { "metadata",                              0, Metadata                          },
        { "cache",                                 0, Cache                             }
    },
    {
        { "nodes",             Elliptics, Nodes           },
        { "monitor_port",      Elliptics, MonitorPort     },
        { "wait_timeout",      Elliptics, WaitTimeout     },
        { "url",               Metadata,  Url             },
        { "history",           Metadata,  History         },
        { "inventory",         Metadata,  Inventory       },
        { "jobs",              Metadata,  Jobs            },
        { "options",           Metadata,  Options         },
        { "group_path_prefix", Cache,     GroupPathPrefix }
    },
    {
        { "db",               Metadata|History,   Db               },
        { "db",               Metadata|Inventory, Db               },
        { "db",               Metadata|Jobs,      Db               },
        { "connectTimeoutMS", Metadata|Options,   ConnectTimeoutMS }
    }
};

Parser::UIntInfoVector config_uint_info = {
    { Elliptics|MonitorPort,             SET, offsetof(Config, monitor_port)                          },
    { Elliptics|WaitTimeout,             SET, offsetof(Config, wait_timeout)                          },
    { ForbiddenDhtGroups,                SET, offsetof(Config, forbidden_dht_groups)                  },
    { ForbiddenUnmatchedGroupTotalSpace, SET, offsetof(Config, forbidden_unmatched_group_total_space) },
    { ForbiddenNsWithoutSettings,        SET, offsetof(Config, forbidden_ns_without_settings)         },
    { ForbiddenDcSharingAmongGroups,     SET, offsetof(Config, forbidden_dc_sharing_among_groups)     },
    { ReservedSpace,                     SET, offsetof(Config, reserved_space)                        },
    { NodeBackendStatStaleTimeout,       SET, offsetof(Config, node_backend_stat_stale_timeout)       },
    { DnetLogMask,                       SET, offsetof(Config, dnet_log_mask)                         },
    { NetThreadNum,                      SET, offsetof(Config, net_thread_num)                        },
    { IoThreadNum,                       SET, offsetof(Config, io_thread_num)                         },
    { NonblockingIoThreadNum,            SET, offsetof(Config, nonblocking_io_thread_num)             },
    { Metadata|Options|ConnectTimeoutMS, SET, offsetof(Config, metadata.options.connectTimeoutMS)     },
    { InfrastructureDcCacheUpdatePeriod, SET, offsetof(Config, infrastructure_dc_cache_update_period) },
    { InfrastructureDcCacheValidTime,    SET, offsetof(Config, infrastructure_dc_cache_valid_time)    },
    { InventoryWorkerTimeout,            SET, offsetof(Config, inventory_worker_timeout)              }
};

Parser::StringInfoVector config_string_info = {
    { Metadata|Url,          offsetof(Config, metadata.url)            },
    { Metadata|History|Db,   offsetof(Config, metadata.history.db)     },
    { Metadata|Inventory|Db, offsetof(Config, metadata.inventory.db)   },
    { Metadata|Jobs|Db,      offsetof(Config, metadata.jobs.db)        },
    { AppName,               offsetof(Config, app_name)                },
    { Cache|GroupPathPrefix, offsetof(Config, cache_group_path_prefix) }
};

} // unnamed namespace

ConfigParser::ConfigParser(Config & config)
    :
    super(config_folders, config_uint_info, config_string_info, (uint8_t *) &config),
    m_array_depth(0),
    m_config(config)
{
    m_current_node.port = -1;
    m_current_node.family = -1;
}

bool ConfigParser::String(const char* str, rapidjson::SizeType length, bool copy)
{
    if (m_keys == (Elliptics|Nodes|1) && m_array_depth == 2) {
        m_current_node.host = std::string(str, length);
        return true;
    }

    return super::String(str, length, copy);
}

bool ConfigParser::StartArray()
{
    ++m_array_depth;
    return true;
}

bool ConfigParser::UInteger(uint64_t val)
{
    if (m_keys == (Elliptics|Nodes|1) && m_array_depth == 2) {
        if (m_current_node.port < 0)
            m_current_node.port = int(val);
        else
            m_current_node.family = int(val);
    } else {
        return super::UInteger(val);
    }
    return true;
}

bool ConfigParser::EndArray(rapidjson::SizeType nr_elements)
{
    if (m_keys == (Elliptics|Nodes|1)) {
        if (m_array_depth == 2) {
            m_config.nodes.push_back(m_current_node);

            m_current_node.host.clear();
            m_current_node.port = -1;
            m_current_node.family = -1;
        } else if (m_array_depth == 1) {
            clear_key();
        }
    }

    --m_array_depth;
    return true;
}
