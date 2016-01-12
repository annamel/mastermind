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

#include <Couple.h>
#include <Group.h>
#include <Storage.h>

#include <StorageUpdater.h>

#include <gtest/gtest.h>

TEST(Couple, Creation)
{
    const char *json = R"END(
    {
        "groups": {
            "1": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2, 3 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1111::11:1025:10/101"
                ]
            },
            "2": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2, 3 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1122::14:1025:10/103"
                ]
            },
            "3": {
                "metadata": {
                    "version": 2,
                    "couple": [ 1, 2, 3 ],
                    "namespace": "default"
                },
                "backends": [
                    "2001:db8:0:1133::b:1025:10/107"
                ]
            }
        }
    }
    )END";

    StorageSnapshot snapshot;
    snapshot.update(json);
    snapshot.complete();

    Storage storage;

    StorageUpdater updater(storage, snapshot);
    updater.update_all();

    EXPECT_EQ(3, storage.get_groups().size());
    EXPECT_EQ(1, storage.get_couples().size());

    const Couple & couple = storage.get_couples().begin()->second;
    EXPECT_EQ("1:2:3", couple.get_key());
    EXPECT_EQ(3, couple.get_groups().size());
}
