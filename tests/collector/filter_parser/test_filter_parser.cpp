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

#include <Filter.h>
#include <FilterParser.h>

#include <rapidjson/writer.h>

#include <gtest/gtest.h>

namespace {

void test_item_types(const std::vector<std::string> & type_names, uint32_t item_bits)
{
    // Generates filter JSON with item types specified in type_names.
    // Example:
    // {
    //   "item_types": [ "backend", "group", "couple" ]
    // }

    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);

    writer.StartObject();
    writer.Key("item_types");
    writer.StartArray();
    for (auto & t : type_names)
        writer.String(t.c_str());
    writer.EndArray();
    writer.EndObject();

    std::string json = buf.GetString();

    Filter filter;
    FilterParser parser(filter);

    rapidjson::Reader reader;
    rapidjson::StringStream stream(json.c_str());
    reader.Parse(stream, parser);

    ASSERT_TRUE(parser.good()) << "Failed to parse filter with item bits " << item_bits;
    EXPECT_EQ(item_bits, filter.item_types) << "Incorrect item_types (expected " << item_bits << ')';
    EXPECT_EQ(true, filter.empty());
}

} // unnamed namespace

TEST(FilterParserTest, FilterCtor)
{
    // This test checks initialization of Filter object.

    Filter filter;
    filter.~Filter();
    // Write junk bytes and check members initialization afterwards.
    memset(&filter, 0x5A, sizeof(filter));
    new (&filter) Filter;

    EXPECT_EQ(0, filter.show_internals);
    EXPECT_EQ(0, filter.item_types);
    EXPECT_EQ(true, filter.empty());
}

TEST(FilterParserTest, SingleItemType)
{
    // This test verifies parsing of item_types with single type specified.

    typedef std::vector<std::string> vs;
    test_item_types(vs({"group"}), Filter::Group);
    test_item_types(vs({"couple"}), Filter::Couple);
    test_item_types(vs({"namespace"}), Filter::Namespace);
    test_item_types(vs({"node"}), Filter::Node);
    test_item_types(vs({"backend"}), Filter::Backend);
    test_item_types(vs({"fs"}), Filter::FS);
    test_item_types(vs({"job"}), Filter::Job);
    test_item_types(vs({"host"}), Filter::Host);
}

TEST(FilterParserTest, MultipleItemTypes)
{
    // This test verifies parsing of item_types with multiple types specified.

    typedef std::vector<std::string> vs;
    test_item_types(vs({"group", "couple", "node"}), Filter::Group|Filter::Couple|Filter::Node);
    test_item_types(vs({"namespace", "backend"}), Filter::Namespace|Filter::Backend);
    test_item_types(vs({"fs", "job"}), Filter::FS|Filter::Job);
}

TEST(FilterParserTest, AllItemTypes)
{
    // This test verifies parsing of item_types with all types specified.

    typedef std::vector<std::string> vs;
    test_item_types(vs({"group", "couple", "namespace", "node", "backend", "fs", "job", "host"}),
            Filter::Group|Filter::Couple|Filter::Namespace|Filter::Node|
                Filter::Backend|Filter::FS|Filter::Job|Filter::Host);
}

TEST(FilterParserTest, WrongItemType)
{
    // This test verifies if parser fails when unknown item type specified.

    const char *json = "{\"item_types\":[\"group\",\"8e518dd1-58b1-419e-a8ca-696b8a361bd8\"]}";

    Filter filter;
    FilterParser parser(filter);

    rapidjson::Reader reader;
    rapidjson::StringStream stream(json);
    reader.Parse(stream, parser);

    ASSERT_FALSE(parser.good());
}

TEST(FilterParserTest, Options)
{
    // This test checks whether an option show_internals (the only supported
    // for now) is taken into account.

    const char *json = "{\"options\":{\"show_internals\":1}}";

    Filter filter;
    FilterParser parser(filter);

    rapidjson::Reader reader;
    rapidjson::StringStream stream(json);
    reader.Parse(stream, parser);

    ASSERT_TRUE(parser.good());
    EXPECT_EQ(1, filter.show_internals);
}

TEST(FilterParserTest, Items)
{
    // This test checks:
    // 1) how the lists of items are being parsed;
    // 2) Filter::sort() which sorts and removes duplicates.
    // The test knows something about how filter parser currently works.
    // FilterParser doesn't sort/check for duplicates during parsing.
    // It is assumed that sorting in Filter::sort() uses the same code for each
    // container, so the test only checks if no container is being skipped and
    // doesn't run full set of cases for each container.

    const char *json = R"END(
    {
       "filter":
       {
           "groups": [ 1, 5, 3, 2, 3 ],
           "couples": [ "7:8:9", "4:5:6", "7:8:9" ],
           "namespaces": [ "storage", "default" ],
           "nodes": [ "::1:1026:10", "::1:1025:10" ],
           "backends": [ "::1:1025:10/2", "::1:1025:10/1" ],
           "filesystems": [ "::1:1026:10/4", "::1:1026:10/3" ]
       }
    }
    )END";

    Filter filter;
    FilterParser parser(filter);

    rapidjson::Reader reader;
    rapidjson::StringStream stream(json);
    reader.Parse(stream, parser);

    ASSERT_TRUE(parser.good());
    EXPECT_EQ(0, filter.empty());

    typedef std::vector<int> vi;
    typedef std::vector<std::string> vs;
    EXPECT_EQ(vi({1, 5, 3, 2, 3}), filter.groups);
    EXPECT_EQ(vs({"7:8:9", "4:5:6", "7:8:9"}), filter.couples);
    EXPECT_EQ(vs({"storage", "default"}), filter.namespaces);
    EXPECT_EQ(vs({"::1:1026:10", "::1:1025:10"}), filter.nodes);
    EXPECT_EQ(vs({"::1:1025:10/2", "::1:1025:10/1"}), filter.backends);
    EXPECT_EQ(vs({"::1:1026:10/4", "::1:1026:10/3"}), filter.filesystems);

    filter.sort();

    EXPECT_EQ(vi({1, 2, 3, 5}), filter.groups);
    EXPECT_EQ(vs({"4:5:6", "7:8:9"}), filter.couples);
    EXPECT_EQ(vs({"default", "storage"}), filter.namespaces);
    EXPECT_EQ(vs({"::1:1025:10", "::1:1026:10"}), filter.nodes);
    EXPECT_EQ(vs({"::1:1025:10/1", "::1:1025:10/2"}), filter.backends);
    EXPECT_EQ(vs({"::1:1026:10/3", "::1:1026:10/4"}), filter.filesystems);
}
