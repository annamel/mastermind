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

#include "StatsParser.h"

#include <cstddef>

#include <errno.h>

namespace {

const uint64_t Backends                           = 2ULL;
    const uint64_t BackendFolder                  = 4ULL;
        const uint64_t Backend                    = 8ULL;
            const uint64_t Dstat                  = 0x10ULL;
                const uint64_t ReadIos            = 0x20ULL;
                const uint64_t WriteIos           = 0x40ULL;
                const uint64_t ReadTicks          = 0x80ULL;
                const uint64_t WriteTicks         = 0x100ULL;
                const uint64_t IoTicks            = 0x200ULL;
                const uint64_t ReadSectors        = 0x400ULL;
                const uint64_t Error              = 0x800ULL;
            const uint64_t Vfs                    = 0x20ULL;
                const uint64_t Blocks             = 0x40ULL;
                const uint64_t Bavail             = 0x80ULL;
                const uint64_t Bsize              = 0x100ULL;
                const uint64_t Fsid               = 0x200ULL;
//                             Error              = 0x800ULL;
            const uint64_t SummaryStats           = 0x40ULL;
                const uint64_t RecordsTotal       = 0x80ULL;
                const uint64_t RecordsRemoved     = 0x100ULL;
                const uint64_t RecordsRemovedSize = 0x200ULL;
                const uint64_t WantDefrag         = 0x400ULL;
                const uint64_t BaseSize           = 0x800ULL;
            const uint64_t Config                 = 0x80ULL;
                const uint64_t BlobSizeLimit      = 0x100ULL;
                const uint64_t BlobSize           = 0x200ULL;
                const uint64_t Group              = 0x400ULL;
                const uint64_t DataPath           = 0x800ULL;
                const uint64_t FilePath           = 0x1000ULL;
            const uint64_t BaseStats              = 0x100ULL;
                const uint64_t BlobFilename       = 0x200ULL;
                    const uint64_t BlobBaseSize   = 0x400ULL;
        const uint64_t BackendId                  = 0x10ULL;
        const uint64_t Status                     = 0x20ULL;
            const uint64_t DefragState            = 0x40ULL;
            const uint64_t State                  = 0x80ULL;
            const uint64_t ReadOnly               = 0x100ULL;
            const uint64_t LastStart              = 0x200ULL;
                const uint64_t LastStartTvSec     = 0x400ULL;
                const uint64_t LastStartTvUsec    = 0x800ULL;
        const uint64_t Commands                   = 0x40ULL;
            const uint64_t Write                  = 0x80ULL;
//                             Cache              = 0x200ULL;
//                             Disk               = 0x400ULL;
//                                 CommandSource  = 0x800ULL;
//                                     Size       = 0x1000ULL;
//                                     Time       = 0x2000ULL;
            const uint64_t NotWrite               = 0x100ULL;
                const uint64_t Cache              = 0x200ULL;
                const uint64_t Disk               = 0x400ULL;
                    const uint64_t CommandSource  = 0x800ULL;
                        const uint64_t Size       = 0x1000ULL;
                        const uint64_t Time       = 0x2000ULL;
        const uint64_t Io                         = 0x80ULL;
            const uint64_t Blocking               = 0x100ULL;
//                             CurrentSize        = 0x400ULL;
            const uint64_t Nonblocking            = 0x200ULL;
                const uint64_t CurrentSize        = 0x400ULL;

const uint64_t Timestamp  = 4ULL;
    const uint64_t TvSec  = 8ULL;
    const uint64_t TvUsec = 0x10ULL;

const uint64_t Procfs                       = 8ULL;
    const uint64_t Vm                       = 0x10ULL;
        const uint64_t La                   = 0x20ULL;
    const uint64_t Net                      = 0x20ULL;
        const uint64_t NetInterfaces        = 0x40ULL;
            const uint64_t NetInterfaceName = 0x80ULL;
                const uint64_t Receive      = 0x100ULL;
//                                 Bytes    = 0x400ULL;
                const uint64_t Transmit     = 0x200ULL;
                    const uint64_t Bytes    = 0x400ULL;

const uint64_t Stats         = 0x10ULL;
    const uint64_t StatName  = 0x20ULL;
        const uint64_t Count = 0x40ULL;

std::vector<Parser::FolderVector> backend_folders = {
    {
        { "backends",  0, Backends  },
        { "timestamp", 0, Timestamp },
        { "procfs",    0, Procfs    },
        { "stats",     0, Stats     }
    },
    {
        { MATCH_ANY, Backends,  BackendFolder },
        { "tv_sec",  Timestamp, TvSec         },
        { "tv_usec", Timestamp, TvUsec        },
        { "vm",      Procfs,    Vm            },
        { "net",     Procfs,    Net           },
        { MATCH_ANY, Stats,     StatName      }
    },
    {
        { "backend",        Backends|BackendFolder, Backend       },
        { "backend_id",     Backends|BackendFolder, BackendId     },
        { "status",         Backends|BackendFolder, Status        },
        { "commands",       Backends|BackendFolder, Commands      },
        { "io",             Backends|BackendFolder, Io            },
        { "la",             Procfs|Vm,              La            },
        { "net_interfaces", Procfs|Net,             NetInterfaces },
        { "count",          Stats|StatName,         Count         }
    },
    {
        { "dstat",           Backends|BackendFolder|Backend,  Dstat            },
        { "vfs",             Backends|BackendFolder|Backend,  Vfs              },
        { "summary_stats",   Backends|BackendFolder|Backend,  SummaryStats     },
        { "config",          Backends|BackendFolder|Backend,  Config           },
        { "base_stats",      Backends|BackendFolder|Backend,  BaseStats        },
        { "defrag_state",    Backends|BackendFolder|Status,   DefragState      },
        { "state",           Backends|BackendFolder|Status,   State            },
        { "read_only",       Backends|BackendFolder|Status,   ReadOnly         },
        { "last_start",      Backends|BackendFolder|Status,   LastStart        },
        { "WRITE",           Backends|BackendFolder|Commands, Write            },
        { NOT_MATCH "WRITE", Backends|BackendFolder|Commands, NotWrite         },
        { "blocking",        Backends|BackendFolder|Io,       Blocking         },
        { "nonblocking",     Backends|BackendFolder|Io,       Nonblocking      },
        { NOT_MATCH "lo",    Procfs|Net|NetInterfaces,        NetInterfaceName }
    },
    {
        { "read_ios",             Backends|BackendFolder|Backend|Dstat,        ReadIos            },
        { "write_ios",            Backends|BackendFolder|Backend|Dstat,        WriteIos           },
        { "error",                Backends|BackendFolder|Backend|Dstat,        Error              },
        { "read_ticks",           Backends|BackendFolder|Backend|Dstat,        ReadTicks          },
        { "write_ticks",          Backends|BackendFolder|Backend|Dstat,        WriteTicks         },
        { "io_ticks",             Backends|BackendFolder|Backend|Dstat,        IoTicks            },
        { "read_sectors",         Backends|BackendFolder|Backend|Dstat,        ReadSectors        },
        { "blocks",               Backends|BackendFolder|Backend|Vfs,          Blocks             },
        { "bavail",               Backends|BackendFolder|Backend|Vfs,          Bavail             },
        { "bsize",                Backends|BackendFolder|Backend|Vfs,          Bsize              },
        { "fsid",                 Backends|BackendFolder|Backend|Vfs,          Fsid               },
        { "error",                Backends|BackendFolder|Backend|Vfs,          Error              },
        { "records_total",        Backends|BackendFolder|Backend|SummaryStats, RecordsTotal       },
        { "records_removed",      Backends|BackendFolder|Backend|SummaryStats, RecordsRemoved     },
        { "records_removed_size", Backends|BackendFolder|Backend|SummaryStats, RecordsRemovedSize },
        { "want_defrag",          Backends|BackendFolder|Backend|SummaryStats, WantDefrag         },
        { "base_size",            Backends|BackendFolder|Backend|SummaryStats, BaseSize           },
        { "blob_size_limit",      Backends|BackendFolder|Backend|Config,       BlobSizeLimit      },
        { "blob_size",            Backends|BackendFolder|Backend|Config,       BlobSize           },
        { "group",                Backends|BackendFolder|Backend|Config,       Group              },
        { "data",                 Backends|BackendFolder|Backend|Config,       DataPath           },
        { "file",                 Backends|BackendFolder|Backend|Config,       FilePath           },
        { MATCH_ANY,              Backends|BackendFolder|Backend|BaseStats,    BlobFilename       },
        { "tv_sec",               Backends|BackendFolder|Status|LastStart,     LastStartTvSec     },
        { "tv_usec",              Backends|BackendFolder|Status|LastStart,     LastStartTvUsec    },
        { "cache",                Backends|BackendFolder|Commands|Write,       Cache              },
        { "disk",                 Backends|BackendFolder|Commands|Write,       Disk               },
        { "cache",                Backends|BackendFolder|Commands|NotWrite,    Cache              },
        { "disk",                 Backends|BackendFolder|Commands|NotWrite,    Disk               },
        { "current_size",         Backends|BackendFolder|Io|Blocking,          CurrentSize        },
        { "current_size",         Backends|BackendFolder|Io|Nonblocking,       CurrentSize        },
        { "receive",              Procfs|Net|NetInterfaces|NetInterfaceName,   Receive            },
        { "transmit",             Procfs|Net|NetInterfaces|NetInterfaceName,   Transmit           }
    },
    {
        { "base_size", Backends|BackendFolder|Backend|BaseStats|BlobFilename, BlobBaseSize  },
        { "bytes",     Procfs|Net|NetInterfaces|NetInterfaceName|Receive,     Bytes         },
        { "bytes",     Procfs|Net|NetInterfaces|NetInterfaceName|Transmit,    Bytes         },
        { MATCH_ANY,   Backends|BackendFolder|Commands|Write|Cache,           CommandSource },
        { MATCH_ANY,   Backends|BackendFolder|Commands|Write|Disk,            CommandSource },
        { MATCH_ANY,   Backends|BackendFolder|Commands|NotWrite|Cache,        CommandSource },
        { MATCH_ANY,   Backends|BackendFolder|Commands|NotWrite|Disk,         CommandSource }
    },
    {
        { "size", Backends|BackendFolder|Commands|Write|Cache|CommandSource,    Size },
        { "time", Backends|BackendFolder|Commands|Write|Cache|CommandSource,    Time },
        { "size", Backends|BackendFolder|Commands|Write|Disk|CommandSource,     Size },
        { "time", Backends|BackendFolder|Commands|Write|Disk|CommandSource,     Time },
        { "size", Backends|BackendFolder|Commands|NotWrite|Cache|CommandSource, Size },
        { "time", Backends|BackendFolder|Commands|NotWrite|Cache|CommandSource, Time },
        { "size", Backends|BackendFolder|Commands|NotWrite|Disk|CommandSource,  Size },
        { "time", Backends|BackendFolder|Commands|NotWrite|Disk|CommandSource,  Time }
    }
};

#define BOFF(field) offsetof(StatsParser::Data, backend.field)
#define NOFF(field) offsetof(StatsParser::Data, node.field)
#define SOFF(field) offsetof(StatsParser::Data, stat_commit.field)

Parser::UIntInfoVector backend_uint_info = {
    { Backends|BackendFolder|BackendId,                                   SET, BOFF(backend_id)           },
    { Backends|BackendFolder|Backend|Dstat|ReadIos,                       SET, BOFF(read_ios)             },
    { Backends|BackendFolder|Backend|Dstat|WriteIos,                      SET, BOFF(write_ios)            },
    { Backends|BackendFolder|Backend|Dstat|ReadTicks,                     SET, BOFF(read_ticks)           },
    { Backends|BackendFolder|Backend|Dstat|WriteTicks,                    SET, BOFF(write_ticks)          },
    { Backends|BackendFolder|Backend|Dstat|IoTicks,                       SET, BOFF(io_ticks)             },
    { Backends|BackendFolder|Backend|Dstat|ReadSectors,                   SET, BOFF(read_sectors)         },
    { Backends|BackendFolder|Backend|Dstat|Error,                         SET, BOFF(dstat_error)          },
    { Backends|BackendFolder|Backend|Vfs|Blocks,                          SET, BOFF(vfs_blocks)           },
    { Backends|BackendFolder|Backend|Vfs|Bavail,                          SET, BOFF(vfs_bavail)           },
    { Backends|BackendFolder|Backend|Vfs|Bsize,                           SET, BOFF(vfs_bsize)            },
    { Backends|BackendFolder|Backend|Vfs|Fsid,                            SET, BOFF(fsid)                 },
    { Backends|BackendFolder|Backend|Vfs|Error,                           SET, BOFF(vfs_error)            },
    { Backends|BackendFolder|Backend|SummaryStats|RecordsTotal,           SET, BOFF(records_total)        },
    { Backends|BackendFolder|Backend|SummaryStats|RecordsRemoved,         SET, BOFF(records_removed)      },
    { Backends|BackendFolder|Backend|SummaryStats|RecordsRemovedSize,     SET, BOFF(records_removed_size) },
    { Backends|BackendFolder|Backend|SummaryStats|WantDefrag,             SET, BOFF(want_defrag)          },
    { Backends|BackendFolder|Backend|SummaryStats|BaseSize,               SET, BOFF(base_size)            },
    { Backends|BackendFolder|Backend|Config|BlobSizeLimit,                SET, BOFF(blob_size_limit)      },
    { Backends|BackendFolder|Backend|Config|BlobSize,                     SET, BOFF(blob_size)            },
    { Backends|BackendFolder|Backend|Config|Group,                        SET, BOFF(group)                },
    { Backends|BackendFolder|Backend|BaseStats|BlobFilename|BlobBaseSize, MAX, BOFF(max_blob_base_size)   },
    { Backends|BackendFolder|Status|DefragState,                          SET, BOFF(defrag_state)         },
    { Backends|BackendFolder|Status|State,                                SET, BOFF(state)                },
    { Backends|BackendFolder|Status|ReadOnly,                             SET, BOFF(read_only)            },
    { Backends|BackendFolder|Status|LastStart|LastStartTvSec,             SET, BOFF(last_start_ts_sec)    },
    { Backends|BackendFolder|Status|LastStart|LastStartTvUsec,            SET, BOFF(last_start_ts_usec)   },
    { Backends|BackendFolder|Commands|Write|Cache|CommandSource|Size,     SUM, BOFF(ell_cache_write_size) },
    { Backends|BackendFolder|Commands|Write|Cache|CommandSource|Time,     SUM, BOFF(ell_cache_write_time) },
    { Backends|BackendFolder|Commands|Write|Disk|CommandSource|Size,      SUM, BOFF(ell_disk_write_size)  },
    { Backends|BackendFolder|Commands|Write|Disk|CommandSource|Time,      SUM, BOFF(ell_disk_write_time)  },
    { Backends|BackendFolder|Commands|NotWrite|Cache|CommandSource|Size,  SUM, BOFF(ell_cache_read_size)  },
    { Backends|BackendFolder|Commands|NotWrite|Cache|CommandSource|Time,  SUM, BOFF(ell_cache_read_time)  },
    { Backends|BackendFolder|Commands|NotWrite|Disk|CommandSource|Size,   SUM, BOFF(ell_disk_read_size)   },
    { Backends|BackendFolder|Commands|NotWrite|Disk|CommandSource|Time,   SUM, BOFF(ell_disk_read_time)   },
    { Backends|BackendFolder|Io|Blocking|CurrentSize,                     SET, BOFF(io_blocking_size)     },
    { Backends|BackendFolder|Io|Nonblocking|CurrentSize,                  SET, BOFF(io_nonblocking_size)  },
    { Timestamp|TvSec,                                                    SET, NOFF(ts_sec)               },
    { Timestamp|TvUsec,                                                   SET, NOFF(ts_usec)              },
    { Procfs|Vm|La,                                                       SET, NOFF(la1)                  },
    { Procfs|Net|NetInterfaces|NetInterfaceName|Receive|Bytes,            SUM, NOFF(rx_bytes)             },
    { Procfs|Net|NetInterfaces|NetInterfaceName|Transmit|Bytes,           SUM, NOFF(tx_bytes)             },
    { Stats|StatName|Count,                                               SET, SOFF(count)                }
};

Parser::StringInfoVector backend_string_info = {
    { Backends|BackendFolder|Backend|Config|DataPath, BOFF(data_path) },
    { Backends|BackendFolder|Backend|Config|FilePath, BOFF(file_path) }
};

} // unnamed namespace

StatsParser::StatsParser()
    :
    super(backend_folders, backend_uint_info, backend_string_info, (uint8_t *) &m_data)
{}

bool StatsParser::Key(const char* str, rapidjson::SizeType length, bool copy)
{
    if (!super::Key(str, length, copy))
        return false;

    if (m_keys == (Stats|StatName|1) && m_depth == 2) {
        unsigned int id = 0;
        unsigned int err = 0;
        if (sscanf(str, "eblob.%u.disk.stat_commit.errors.%u", &id, &err) == 2) {
            m_data.stat_commit.backend = id;
            m_data.stat_commit.err = err;
        }
    }

    return true;
}

bool StatsParser::EndObject(rapidjson::SizeType nr_members)
{
    if (m_keys == (Backends|BackendFolder|1) && m_depth == 3) {
        m_backend_stats.push_back(m_data.backend);
        m_data.backend.~BackendStat();
        new (&m_data.backend) BackendStat;
    } else if (m_keys == (Stats|StatName|1) && m_depth == 3) {
        if (m_data.stat_commit.err == EROFS)
            m_rofs_errors[m_data.stat_commit.backend] = m_data.stat_commit.count;
        std::memset(&m_data.stat_commit, 0, sizeof(m_data.stat_commit));
    }

    return super::EndObject(nr_members);
}
