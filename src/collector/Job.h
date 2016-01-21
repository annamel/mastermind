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

#ifndef __19d1f67a_e4dc_4c98_9f61_5882632cb0a1
#define __19d1f67a_e4dc_4c98_9f61_5882632cb0a1

#include <mongo/client/dbclient.h>
#include <rapidjson/writer.h>

#include <string>

class Job
{
public:
    enum Type
    {
        EMPTY,
        MOVE_JOB,
        RECOVER_DC_JOB,
        COUPLE_DEFRAG_JOB,
        RESTORE_GROUP_JOB
    };

    enum Status
    {
        NEW,
        NOT_APPROVED,
        EXECUTING,
        PENDING,
        BROKEN,
        COMPLETED,
        CANCELLED
    };

    static const char *type_str(Type type);
    static const char *status_str(Status status);

public:
    // timestamp is in nanoseconds
    Job(mongo::BSONObj & obj, uint64_t timestamp);

    const std::string & get_id() const
    { return m_id; }

    Type get_type() const
    { return m_type; }

    Status get_status() const
    { return m_status; }

    int get_group_id() const
    { return m_group_id; }

    void merge(const Job & other, bool *have_newer);

    bool operator == (const Job & other) const;

    bool operator != (const Job & other) const
    { return !(*this == other); }

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const;

private:
    std::string m_id;
    Type m_type;
    Status m_status;
    int m_group_id;

    uint64_t m_timestamp;
};

#endif

