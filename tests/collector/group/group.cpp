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

#include <Group.h>

#include <msgpack.hpp>

#include <gtest/gtest.h>

namespace {

} // unnamed namespace

TEST(Group, Ctor)
{
    // This test checks initialization of Group object.

    Group g(0);

    g.~Group();
    // Write junk bytes and check members initialization afterwards.
    memset(&g, 0x5a, sizeof(g));
    new (&g) Group(113);

    EXPECT_EQ(113, g.get_id());
    EXPECT_TRUE(g.get_backends().empty());
    EXPECT_EQ(0, g.get_update_time());
    EXPECT_FALSE(g.has_active_job());

    const Group::Metadata & md = g.get_metadata();
    EXPECT_EQ(0, md.version);
    EXPECT_FALSE(md.frozen);
    EXPECT_TRUE(md.couple.empty());
    EXPECT_TRUE(md.namespace_name.empty());
    EXPECT_TRUE(md.type.empty());
    EXPECT_FALSE(md.service.migrating);
    EXPECT_TRUE(md.service.job_id.empty());

    EXPECT_FALSE(g.metadata_parsed());
    EXPECT_EQ(0, g.get_metadata_parse_duration());

    EXPECT_EQ(Group::DATA, g.get_type());
    EXPECT_EQ(Group::INIT, g.get_status());
}

TEST(Group, ParseMetadataV1)
{
    // This test checks metadata parsing for version 1 metadata
    // which is msgpack:ed array of groups in couple.

    // Prepare metadata.

    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(&buffer);
    packer.pack_array(3);
    packer.pack(17);
    packer.pack(19);
    packer.pack(23);

    // Create group and parse metadata.

    Group g(17);
    g.save_metadata(buffer.data(), buffer.size(), ::time(nullptr)*1000000000ULL);
    ASSERT_TRUE(g.parse_metadata());
    EXPECT_TRUE(g.metadata_parsed());

    // Check metadata values.

    const Group::Metadata & md = g.get_metadata();
    EXPECT_EQ(1, md.version);
    EXPECT_FALSE(md.frozen);
    EXPECT_EQ(std::vector<int>({17, 19, 23}), md.couple);
    EXPECT_EQ("default", md.namespace_name);
    EXPECT_TRUE(md.type.empty());
    EXPECT_FALSE(md.service.migrating);
    EXPECT_TRUE(md.service.job_id.empty());

    // Check if type and status weren't affected.

    g.calculate_type();

    EXPECT_EQ(Group::DATA, g.get_type());
    EXPECT_EQ(Group::INIT, g.get_status());

    // Check that other values didn't change.

    EXPECT_EQ(17, g.get_id());
    EXPECT_TRUE(g.get_backends().empty());
    EXPECT_FALSE(g.has_active_job());
}

TEST(Group, ParseMetadataV2)
{
    // This test checks metadata parsing for version 2
    // metadata which is msgpack:ed map.

    // Prepare metadata.

    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(&buffer);
    packer.pack_map(6);

    packer.pack(std::string("version"));
    packer.pack(2);

    packer.pack(std::string("frozen"));
    packer.pack(true);

    packer.pack(std::string("couple"));
    packer.pack_array(3);
    packer.pack(29);
    packer.pack(31);
    packer.pack(37);

    packer.pack(std::string("namespace"));
    packer.pack(std::string("storage"));

    packer.pack(std::string("type"));
    packer.pack(std::string("cache"));

    packer.pack(std::string("service"));
    packer.pack_map(2);
    packer.pack(std::string("status"));
    packer.pack(std::string("MIGRATING"));
    packer.pack(std::string(std::string("job_id")));
    packer.pack(std::string("12345"));

    // Create group and parse metadata.

    Group g(29);
    g.save_metadata(buffer.data(), buffer.size(), ::time(nullptr)*1000000000ULL);
    ASSERT_TRUE(g.parse_metadata());
    EXPECT_TRUE(g.metadata_parsed());

    // Check metadata values.

    const Group::Metadata & md = g.get_metadata();
    EXPECT_EQ(2, md.version);
    EXPECT_TRUE(md.frozen);
    EXPECT_EQ(std::vector<int>({29, 31, 37}), md.couple);
    EXPECT_EQ("storage", md.namespace_name);
    EXPECT_EQ("cache", md.type);
    EXPECT_TRUE(md.service.migrating);
    EXPECT_EQ("12345", md.service.job_id);

    // Check whether the type is taken into account.

    g.calculate_type();

    EXPECT_EQ(Group::CACHE, g.get_type());
    EXPECT_EQ(Group::INIT, g.get_status());

    // Check that other values didn't change.

    EXPECT_EQ(29, g.get_id());
    EXPECT_TRUE(g.get_backends().empty());
    EXPECT_FALSE(g.has_active_job());
}
