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
#include <Storage.h>
#include <StorageUpdater.h>
#include <TestUtil.h>

#include <gtest/gtest.h>

TEST(Select, SelectGroups1)
{
    // This test checks intersection of couple and node filters.

    const char *json = R"END(
    {
        "groups": {
            "1": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 5, 6 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/1"
                ]
            },
            "2": {
                "metadata": {
                    "version": 2,
                    "couple": [ 2, 3, 7 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/2"
                ]
            },
            "3": {
                "metadata": {
                    "version": 2,
                    "couple": [ 2, 3, 7 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::12:1025:10/3"
                ]
            },
            "5": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 5, 6 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::12:1025:10/5"
                ]
            },
            "6": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 5, 6 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::12:1025:10/6"
                ]
            },
            "7": {
                "metadata": {
                    "version": 2,
                    "couple": [ 2, 3, 7 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::12:1025:10/7"
                ]
            }
        }
    }
    )END";

    Storage storage = StorageUpdater::create(json);

    Filter filter;
    filter.item_types = Filter::Group;
    filter.couples.push_back("1:5:6");
    filter.couples.push_back("2:3:7");
    filter.nodes.push_back("2001:db8:0:1111::11:1025:10");

    Storage::Entries entries;
    storage.select(filter, entries);

    ASSERT_EQ(2, entries.groups.size());
    std::sort(entries.groups.begin(), entries.groups.end(),
        [] (const std::reference_wrapper<Group> & g1, const std::reference_wrapper<Group> & g2)
            { return g1.get().get_id() < g2.get().get_id(); });

    EXPECT_EQ(1, entries.groups[0].get().get_id());
    EXPECT_EQ(2, entries.groups[1].get().get_id());
}
