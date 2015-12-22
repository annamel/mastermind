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

#include <Backend.h>
#include <StatsParser.h>

#include <rapidjson/writer.h>

#include <gtest/gtest.h>

namespace {

// Test statistics for "parse full". Obviously this is not real life example.
// We must set all values non-zero to make sure that they were fetched from JSON
// The test knows that the default constructor initializes all members to zero.

struct TestNodeStat : public NodeStat
{
    TestNodeStat()
    {
        ts_sec = 1449495977;
        ts_usec = 514751;
        la1 = 11;
        tx_bytes = 991;
        rx_bytes = 997;
    }
};

struct TestBackendStat : public BackendStat
{
    TestBackendStat(uint64_t id)
    {
        backend_id = id;

        // dstat
        read_ios = 11047 + id;
        write_ios = 153719 + id;
        read_ticks = 28219 + id;
        write_ticks = 756463 + id;
        io_ticks = 779573 + id;
        read_sectors = 1508509 + id;
        dstat_error = 3 + id;

        // vfs
        fsid = 8323278684798404783 + id;
        vfs_blocks = 480682469 + id;
        vfs_bavail = 477906337 + id;
        vfs_bsize = 4099 + id;
        vfs_error = 5 + id;

        // summary_stats
        base_size = 2333049977 + id;
        records_total = 29633 + id;
        records_removed = 2521 + id;
        records_removed_size = 258561179 + id;
        want_defrag = 2 + id;

        // config
        blob_size_limit = 5368709131 + id;
        blob_size = 53687091251 + id;
        group = 571 + id;
        data_path = "/data/path/3" + std::to_string(id);
        file_path = "/file/path/5" + std::to_string(id);

        // base_stats
        max_blob_base_size = 2333049977 + id;

        // status
        state = 2 + id;
        defrag_state = 337 + id;
        read_only = 1;
        last_start_ts_sec = 1449503129 + id;
        last_start_ts_usec = 424961 + id;

        // commands
        ell_cache_write_size = 29053811 + id;
        ell_cache_write_time = 23011 + id;
        ell_disk_write_size = 32427323 + id;
        ell_disk_write_time = 19051 + id;
        ell_cache_read_size = 106845253 + id;
        ell_cache_read_time = 25523 + id;
        ell_disk_read_size = 4116967 + id;
        ell_disk_read_time = 31957 + id;

        // io queues
        io_blocking_size = 499 + id;
        io_nonblocking_size = 743 + id;

        // stats
        stat_commit_rofs_errors = 24749 + id;
    }
};

const TestNodeStat s_node_stat;
const TestBackendStat s_bstat_1(11);
const TestBackendStat s_bstat_2(20);

void print_node_json(rapidjson::Writer<rapidjson::StringBuffer> & writer)
{
    // Example:
    // {
    //     "timestamp": {
    //         "tv_sec": 1449497960,
    //         "tv_usec": 100
    //     },
    //     "procfs": {
    //         "vm": {
    //             "la": [ 10, 40, 50 ]
    //         },
    //         "net": {
    //             "net_interfaces": {
    //                 "eth0": {
    //                     "receive": {
    //                         "bytes": 710009597
    //                     },
    //                     "transmit": {
    //                         "bytes": 38043292
    //                     }
    //                 },
    //                 "eth1": {
    //                     "receive": {
    //                         "bytes": 15335807301
    //                     },
    //                     "transmit": {
    //                         "bytes": 10702349567
    //                     }
    //                 },
    //                 "lo": {
    //                     "receive": {
    //                         "bytes": 5980567201
    //                     },
    //                     "transmit": {
    //                         "bytes": 5980567201
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // }

    writer.Key("timestamp");
    writer.StartObject();
        writer.Key("tv_sec");
        writer.Uint64(s_node_stat.ts_sec);
        writer.Key("tv_usec");
        writer.Uint64(s_node_stat.ts_usec);
    writer.EndObject();

    writer.Key("procfs");
    writer.StartObject();

        writer.Key("vm");
        writer.StartObject();
        writer.Key("la");
        writer.StartArray();
            writer.Uint64(s_node_stat.la1);
            writer.Uint64(s_node_stat.la1 * 3);
            writer.Uint64(s_node_stat.la1 * 4);
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
                        writer.Uint64(s_node_stat.rx_bytes);
                    writer.EndObject();
                    writer.Key("transmit");
                    writer.StartObject();
                        writer.Key("bytes");
                        writer.Uint64(s_node_stat.tx_bytes);
                    writer.EndObject();
                writer.EndObject();
            writer.EndObject();
        writer.EndObject();

    writer.EndObject();
}

void print_backend_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
        const BackendStat & stat)
{
    // Example:
    // {
    //     "backend_id": 10,
    //     "backend": {
    //         "base_stats": {
    //             "data-0.0": {
    //                 "base_size": 2333049958
    //             }
    //         },
    //         "config": {
    //             "blob_size": 53687091200,
    //             "blob_size_limit": 5368709120,
    //             "data": "/path/to/1/1/data",
    //             "group": 83,
    //         },
    //         "dstat": {
    //             "error": 0,
    //             "io_ticks": 780772,
    //             "read_ios": 11041,
    //             "read_sectors": 1508506,
    //             "read_ticks": 28212,
    //             "write_ios": 153977,
    //             "write_ticks": 757656
    //         },
    //         "summary_stats": {
    //             "base_size": 2333049958,
    //             "records_removed": 2511,
    //             "records_removed_size": 258561169,
    //             "records_total": 29630,
    //             "want_defrag": 0
    //         },
    //         "vfs": {
    //             "bavail": 477906313,
    //             "blocks": 480682466,
    //             "bsize": 4096,
    //             "error": 0,
    //             "fsid": 8323278684798404738
    //         }
    //     },
    //     "commands": {
    //         "LOOKUP": {
    //             "cache": {
    //                 "internal": {
    //                     "size": 0,
    //                     "time": 0
    //                 },
    //                 "outside": {
    //                     "size": 0,
    //                     "time": 6428828
    //                 }
    //             },
    //             "disk": {
    //                 "internal": {
    //                     "size": 0,
    //                     "time": 0
    //                 },
    //                 "outside": {
    //                     "size": 0,
    //                     "time": 6365100
    //                 }
    //             }
    //         },
    //         "READ": {
    //             "cache": {
    //                 "internal": {
    //                     "size": 0,
    //                     "time": 0
    //                 },
    //                 "outside": {
    //                     "size": 0,
    //                     "time": 0
    //                 }
    //             },
    //             "disk": {
    //                 "internal": {
    //                     "size": 0,
    //                     "time": 0
    //                 },
    //                 "outside": {
    //                     "size": 51160088,
    //                     "time": 619274893
    //                 }
    //             }
    //         },
    //         "WRITE": {
    //             "cache": {
    //                 "internal": {
    //                     "size": 0,
    //                     "time": 0
    //                 },
    //                 "outside": {
    //                     "size": 0,
    //                     "time": 0
    //                 }
    //             },
    //             "disk": {
    //                 "internal": {
    //                     "size": 0,
    //                     "time": 0
    //                 },
    //                 "outside": {
    //                     "size": 2371,
    //                     "time": 6991
    //                 }
    //             }
    //         }
    //     },
    //     "io": {
    //         "blocking": {
    //             "current_size": 0
    //         },
    //         "nonblocking": {
    //             "current_size": 0
    //         }
    //     },
    //     "status": {
    //         "defrag_state": 0,
    //         "last_start": {
    //             "tv_sec": 1448983349,
    //             "tv_usec": 745011
    //         },
    //         "read_only": false,
    //         "state": 1
    //     }
    // }

    writer.Key(std::to_string(stat.backend_id).c_str());
    writer.StartObject();
        writer.Key("backend_id");
        writer.Uint(stat.backend_id);
        writer.Key("backend");
        writer.StartObject();
            writer.Key("base_stats");
            writer.StartObject();
                writer.Key("data-0.0");
                writer.StartObject();
                    writer.Key("base_size");
                    writer.Uint(stat.max_blob_base_size);
                writer.EndObject();
            writer.EndObject();
            writer.Key("config");
            writer.StartObject();
                writer.Key("blob_size");
                writer.Uint64(stat.blob_size);
                writer.Key("blob_size_limit");
                writer.Uint64(stat.blob_size_limit);
                writer.Key("data");
                writer.String(stat.data_path.c_str());
                writer.Key("file");
                writer.String(stat.file_path.c_str());
                writer.Key("group");
                writer.Uint(stat.group);
            writer.EndObject();
            writer.Key("dstat");
            writer.StartObject();
                writer.Key("error");
                writer.Uint(stat.dstat_error);
                writer.Key("io_ticks");
                writer.Uint(stat.io_ticks);
                writer.Key("read_ios");
                writer.Uint(stat.read_ios);
                writer.Key("read_sectors");
                writer.Uint(stat.read_sectors);
                writer.Key("read_ticks");
                writer.Uint(stat.read_ticks);
                writer.Key("write_ios");
                writer.Uint(stat.write_ios);
                writer.Key("write_ticks");
                writer.Uint(stat.write_ticks);
            writer.EndObject();
            writer.Key("summary_stats");
            writer.StartObject();
                writer.Key("base_size");
                writer.Uint(stat.base_size);
                writer.Key("records_removed");
                writer.Uint(stat.records_removed);
                writer.Key("records_removed_size");
                writer.Uint(stat.records_removed_size);
                writer.Key("records_total");
                writer.Uint(stat.records_total);
                writer.Key("want_defrag");
                writer.Uint(stat.want_defrag);
            writer.EndObject();
            writer.Key("vfs");
            writer.StartObject();
                writer.Key("bavail");
                writer.Uint(stat.vfs_bavail);
                writer.Key("blocks");
                writer.Uint(stat.vfs_blocks);
                writer.Key("bsize");
                writer.Uint(stat.vfs_bsize);
                writer.Key("error");
                writer.Uint(stat.vfs_error);
                writer.Key("fsid");
                writer.Uint64(stat.fsid);
            writer.EndObject();
        writer.EndObject();
        writer.Key("commands");
        writer.StartObject();
            writer.Key("READ");
            writer.StartObject();
                writer.Key("cache");
                writer.StartObject();
                    writer.Key("internal");
                    writer.StartObject();
                        writer.Key("size");
                        writer.Uint(stat.ell_cache_read_size);
                        writer.Key("time");
                        writer.Uint(stat.ell_cache_read_time);
                    writer.EndObject();
                writer.EndObject();
                writer.Key("disk");
                writer.StartObject();
                    writer.Key("outside");
                    writer.StartObject();
                        writer.Key("size");
                        writer.Uint(stat.ell_disk_read_size);
                        writer.Key("time");
                        writer.Uint(stat.ell_disk_read_time);
                    writer.EndObject();
                writer.EndObject();
            writer.EndObject();
            writer.Key("WRITE");
            writer.StartObject();
                writer.Key("cache");
                writer.StartObject();
                    writer.Key("outside");
                    writer.StartObject();
                        writer.Key("size");
                        writer.Uint(stat.ell_cache_write_size);
                        writer.Key("time");
                        writer.Uint(stat.ell_cache_write_time);
                    writer.EndObject();
                writer.EndObject();
                writer.Key("disk");
                writer.StartObject();
                    writer.Key("internal");
                    writer.StartObject();
                        writer.Key("size");
                        writer.Uint(stat.ell_disk_write_size);
                        writer.Key("time");
                        writer.Uint(stat.ell_disk_write_time);
                    writer.EndObject();
                writer.EndObject();
            writer.EndObject();
        writer.EndObject();
        writer.Key("io");
        writer.StartObject();
            writer.Key("blocking");
            writer.StartObject();
                writer.Key("current_size");
                writer.Uint(stat.io_blocking_size);
            writer.EndObject();
            writer.Key("nonblocking");
            writer.StartObject();
                writer.Key("current_size");
                writer.Uint(stat.io_nonblocking_size);
            writer.EndObject();
        writer.EndObject();
        writer.Key("status");
        writer.StartObject();
            writer.Key("defrag_state");
            writer.Uint(stat.defrag_state);
            writer.Key("last_start");
            writer.StartObject();
                writer.Key("tv_sec");
                writer.Uint(stat.last_start_ts_sec);
                writer.Key("tv_usec");
                writer.Uint(stat.last_start_ts_usec);
            writer.EndObject();
            writer.Key("read_only");
            writer.Bool(stat.read_only);
            writer.Key("state");
            writer.Uint(stat.state);
        writer.EndObject();
    writer.EndObject();
}

void print_stats_json(rapidjson::Writer<rapidjson::StringBuffer> & writer)
{
    // "stats": {
    //     "eblob.111.disk.stat_commit.errors.9": {
    //         "count": 27011
    //     },
    //     "eblob.111.disk.stat_commit.errors.30": {
    //         "count": 3119
    //     },
    //     "eblob.112.disk.stat_commit.errors.30": {
    //         "count": 4673
    //     }
    // }

    std::string badf_1 = "eblob." + std::to_string(s_bstat_1.backend_id) + ".disk.stat_commit.errors.9";
    std::string rofs_1 = "eblob." + std::to_string(s_bstat_1.backend_id) + ".disk.stat_commit.errors.30";
    std::string rofs_2 = "eblob." + std::to_string(s_bstat_2.backend_id) + ".disk.stat_commit.errors.30";

    writer.Key("stats");
    writer.StartObject();
        writer.Key(badf_1.c_str());
        writer.StartObject();
            writer.Key("count");
            writer.Uint(s_bstat_1.stat_commit_rofs_errors + 13);
        writer.EndObject();
        writer.Key(rofs_1.c_str());
        writer.StartObject();
            writer.Key("count");
            writer.Uint(s_bstat_1.stat_commit_rofs_errors);
        writer.EndObject();
        writer.Key(rofs_2.c_str());
        writer.StartObject();
            writer.Key("count");
            writer.Uint(s_bstat_2.stat_commit_rofs_errors);
        writer.EndObject();
    writer.EndObject();
}

void check_backend_stat(const BackendStat & reference, const BackendStat & stat)
{
    EXPECT_EQ(reference.backend_id, stat.backend_id);
    EXPECT_EQ(reference.read_ios, stat.read_ios);
    EXPECT_EQ(reference.write_ios, stat.write_ios);
    EXPECT_EQ(reference.read_ticks, stat.read_ticks);
    EXPECT_EQ(reference.write_ticks, stat.write_ticks);
    EXPECT_EQ(reference.io_ticks, stat.io_ticks);
    EXPECT_EQ(reference.read_sectors, stat.read_sectors);
    EXPECT_EQ(reference.dstat_error, stat.dstat_error);
    EXPECT_EQ(reference.fsid, stat.fsid);
    EXPECT_EQ(reference.vfs_blocks, stat.vfs_blocks);
    EXPECT_EQ(reference.vfs_bavail, stat.vfs_bavail);
    EXPECT_EQ(reference.vfs_bsize, stat.vfs_bsize);
    EXPECT_EQ(reference.vfs_error, stat.vfs_error);
    EXPECT_EQ(reference.base_size, stat.base_size);
    EXPECT_EQ(reference.records_total, stat.records_total);
    EXPECT_EQ(reference.records_removed, stat.records_removed);
    EXPECT_EQ(reference.records_removed_size, stat.records_removed_size);
    EXPECT_EQ(reference.want_defrag, stat.want_defrag);
    EXPECT_EQ(reference.blob_size_limit, stat.blob_size_limit);
    EXPECT_EQ(reference.blob_size, stat.blob_size);
    EXPECT_EQ(reference.group, stat.group);
    EXPECT_EQ(reference.data_path, stat.data_path);
    EXPECT_EQ(reference.file_path, stat.file_path);
    EXPECT_EQ(reference.max_blob_base_size, stat.max_blob_base_size);
    EXPECT_EQ(reference.state, stat.state);
    EXPECT_EQ(reference.defrag_state, stat.defrag_state);
    EXPECT_EQ(reference.read_only, stat.read_only);
    EXPECT_EQ(reference.last_start_ts_sec, stat.last_start_ts_sec);
    EXPECT_EQ(reference.last_start_ts_usec, stat.last_start_ts_usec);
    EXPECT_EQ(reference.ell_cache_write_size, stat.ell_cache_write_size);
    EXPECT_EQ(reference.ell_cache_write_time, stat.ell_cache_write_time);
    EXPECT_EQ(reference.ell_disk_write_size, stat.ell_disk_write_size);
    EXPECT_EQ(reference.ell_disk_write_time, stat.ell_disk_write_time);
    EXPECT_EQ(reference.ell_cache_read_size, stat.ell_cache_read_size);
    EXPECT_EQ(reference.ell_cache_read_time, stat.ell_cache_read_time);
    EXPECT_EQ(reference.ell_disk_read_size, stat.ell_disk_read_size);
    EXPECT_EQ(reference.ell_disk_read_time, stat.ell_disk_read_time);
    EXPECT_EQ(reference.io_blocking_size, stat.io_blocking_size);
    EXPECT_EQ(reference.io_nonblocking_size, stat.io_nonblocking_size);
    EXPECT_EQ(reference.stat_commit_rofs_errors, stat.stat_commit_rofs_errors);
}

} // unnamed namespace

TEST(StatsParserTest, ParseFull)
{
    // This test verifies parsing of monitor_stats JSON with all (known) fields
    // set non-zero.

    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);

    writer.StartObject();
        print_node_json(writer);
        writer.Key("backends");
        writer.StartObject();
            print_backend_json(writer, s_bstat_1);
            print_backend_json(writer, s_bstat_2);
        writer.EndObject();
        print_stats_json(writer);
    writer.EndObject();

    std::string json_str = buf.GetString();

    StatsParser parser;

    rapidjson::Reader reader;
    rapidjson::StringStream ss(json_str.c_str());
    reader.Parse(ss, parser);

    ASSERT_TRUE(parser.good());

    const NodeStat & parsed_stat = parser.get_node_stat();
    EXPECT_EQ(s_node_stat.ts_sec, parsed_stat.ts_sec);
    EXPECT_EQ(s_node_stat.ts_usec, parsed_stat.ts_usec);
    EXPECT_EQ(s_node_stat.la1, parsed_stat.la1);
    EXPECT_EQ(s_node_stat.tx_bytes, parsed_stat.tx_bytes);
    EXPECT_EQ(s_node_stat.rx_bytes, parsed_stat.rx_bytes);

    std::vector<BackendStat> & bstats = parser.get_backend_stats();
    ASSERT_EQ(2, bstats.size());

    // Current API requires us to iterate over rofs_errors and set
    // values in BackendStat:s manually.

    std::map<unsigned int, uint64_t> & rofs_err = parser.get_rofs_errors();
    ASSERT_EQ(2, rofs_err.size());
    bstats[0].stat_commit_rofs_errors = rofs_err[bstats[0].backend_id];
    bstats[1].stat_commit_rofs_errors = rofs_err[bstats[1].backend_id];

    check_backend_stat(s_bstat_1, bstats[0]);
    check_backend_stat(s_bstat_2, bstats[1]);
}

TEST(StatsParserTest, NetInterfaces)
{
    // This test verifies if NodeStat members rx_bytes and tx_bytes are
    // calculated correctly. Loopback counters must be ignored.

    const char *json = R"END(
    {
         "procfs": {
             "net": {
                 "net_interfaces": {
                     "eth0": {
                         "receive": {
                             "bytes": 710009597
                         },
                         "transmit": {
                             "bytes": 38043292
                         }
                     },
                     "eth1": {
                         "receive": {
                             "bytes": 15335807301
                         },
                         "transmit": {
                             "bytes": 10702349567
                         }
                     },
                     "lo": {
                         "receive": {
                             "bytes": 5980567201
                         },
                         "transmit": {
                             "bytes": 5980567201
                         }
                     }
                 }
             }
         }
    }
    )END";

    StatsParser parser;

    rapidjson::Reader reader;
    rapidjson::StringStream ss(json);
    reader.Parse(ss, parser);

    ASSERT_TRUE(parser.good());

    const NodeStat & stat = parser.get_node_stat();
    EXPECT_EQ(16045816898ULL, stat.rx_bytes);
    EXPECT_EQ(10740392859ULL, stat.tx_bytes);
}

TEST(StatsParserTest, MaxBlobBaseSize)
{
    // This test verifies calculation of BackendStat::max_blob_base_size.
    // It must be a maximum of backends/<id>/backend/base_stats/<filename>/base_size.

    const char *json = R"END(
    {
        "backends": {
            "7949": {
                "backend_id": 7949,
                "backend": {
                    "base_stats": {
                        "data-0.0": {
                            "base_size": 2503
                        },
                        "data-1.0": {
                            "base_size": 7011
                        },
                        "data-2.0": {
                            "base_size": 5101
                        }
                    }
                }
            }
        }
    }
    )END";

    StatsParser parser;

    rapidjson::Reader reader;
    rapidjson::StringStream ss(json);
    reader.Parse(ss, parser);

    ASSERT_TRUE(parser.good());

    const std::vector<BackendStat> & stats = parser.get_backend_stats();
    ASSERT_EQ(1, stats.size());

    const BackendStat & stat = stats[0];
    EXPECT_EQ(7949, stat.backend_id);
    EXPECT_EQ(7011, stat.max_blob_base_size);
}

TEST(StatsParserTest, Commands)
{
    // This test verifies parsing of commands statistic.
    // The following BackendStat members are checked up:
    //
    // ell_cache_write_size
    // ell_cache_write_time
    // ell_disk_write_size
    // ell_disk_write_time
    // ell_cache_read_size
    // ell_cache_read_time
    // ell_disk_read_size
    // ell_disk_read_time

    const char *json = R"END(
    {
        "backends": {
            "13687": {
                "backend_id": 13687,
                "commands": {
                    "LOOKUP": {
                        "cache": {
                            "internal": {
                                "size": 23569810725173,
                                "time": 984787292977
                            },
                            "outside": {
                                "size": 28971867612377,
                                "time": 101891706627377
                            }
                        },
                        "disk": {
                            "internal": {
                                "size": 312502641817337,
                                "time": 2090731958971
                            },
                            "outside": {
                                "size": 1144666813351,
                                "time": 251893066721771
                            }
                        }
                    },
                    "READ": {
                        "cache": {
                            "internal": {
                                "size": 15521512425161,
                                "time": 22543623921839
                            },
                            "outside": {
                                "size": 140743022331809,
                                "time": 293701205228491
                            }
                        },
                        "disk": {
                            "internal": {
                                "size": 296541659217403,
                                "time": 87071764919387
                            },
                            "outside": {
                                "size": 16480592113031,
                                "time": 19792174930169
                            }
                        }
                    },
                    "WRITE": {
                        "cache": {
                            "internal": {
                                "size": 307251808920601,
                                "time": 30006316647227
                            },
                            "outside": {
                                "size": 314502224221261,
                                "time": 23647697221787
                            }
                        },
                        "disk": {
                            "internal": {
                                "size": 6127806619027,
                                "time": 169951005011401
                            },
                            "outside": {
                                "size": 6416988325967,
                                "time": 9534169012801
                            }
                        }
                    }
                }
            }
        }
    }
    )END";

    StatsParser parser;

    rapidjson::Reader reader;
    rapidjson::StringStream ss(json);
    reader.Parse(ss, parser);

    ASSERT_TRUE(parser.good());

    const std::vector<BackendStat> & stats = parser.get_backend_stats();
    ASSERT_EQ(1, stats.size());

    const BackendStat & stat = stats[0];
    EXPECT_EQ(13687, stat.backend_id);
    EXPECT_EQ(621754033141862ULL, stat.ell_cache_write_size);
    EXPECT_EQ(53654013869014ULL, stat.ell_cache_write_time);
    EXPECT_EQ(12544794944994ULL, stat.ell_disk_write_size);
    EXPECT_EQ(179485174024202ULL, stat.ell_disk_write_time);
    EXPECT_EQ(208806213094520ULL, stat.ell_cache_read_size);
    EXPECT_EQ(419121323070684ULL, stat.ell_cache_read_time);
    EXPECT_EQ(626669559961122ULL, stat.ell_disk_read_size);
    EXPECT_EQ(360847738530298ULL, stat.ell_disk_read_time);
}
