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

#include "Job.h"

Job::Job(mongo::BSONObj & obj, uint64_t timestamp)
    :
    m_type(EMPTY),
    m_status(NEW),
    m_group_id(0),
    m_timestamp(timestamp)
{
    for (mongo::BSONObj::iterator it = obj.begin(); it.more();) {
        // BSONObj::iterator::next extracts a current element
        // and increments the iterator
        mongo::BSONElement element = it.next();
        const char *field_name = element.fieldName();

        if (!std::strcmp(field_name, "id")) {
            m_id = element.String();
        } else if (!std::strcmp(field_name, "group")) {
            m_group_id = element.Int();
        } else if (!std::strcmp(field_name, "status")) {
            std::string str = element.String();

            if (str == "new")
                m_status = NEW;
            else if (str == "executing")
                m_status = EXECUTING;
            else if (str == "broken")
                m_status = BROKEN;
            else if (str == "pending")
                m_status = PENDING;
            else if (str == "not_approved")
                m_status = NOT_APPROVED;
            else if (str == "cancelled")
                m_status = CANCELLED;
            else if (str == "completed")
                m_status = COMPLETED;
            else
                throw std::runtime_error("Unknown job status " + str);

        } else if (!std::strcmp(field_name, "type")) {
            std::string str = element.String();

            if (str == "move_job")
                m_type = MOVE_JOB;
            else if (str == "recover_dc_job")
                m_type = RECOVER_DC_JOB;
            else if (str == "couple_defrag_job")
                m_type = COUPLE_DEFRAG_JOB;
            else if (str == "restore_group_job")
                m_type = RESTORE_GROUP_JOB;
            else
                throw std::runtime_error("Unknown job type " + str);
        }
    }

    if (m_id.empty())
        throw std::runtime_error("No job identifier");
}

void Job::merge(const Job & other, bool *have_newer)
{
    if (m_timestamp > other.m_timestamp) {
        if (have_newer != nullptr)
            *have_newer = true;
        return;
    }

    if (m_timestamp == other.m_timestamp)
        return;

    m_id = other.m_id;
    m_type = other.m_type;
    m_status = other.m_status;
    m_group_id = other.m_group_id;
    m_timestamp = other.m_timestamp;
}

bool Job::operator == (const Job & other) const
{
    if (this == &other)
        return true;

    return std::tie(m_id, m_type, m_status, m_group_id) ==
        std::tie(other.m_id, other.m_type, other.m_status, other.m_group_id);
}

void Job::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const
{
    // JSON looks like this:
    // {
    //     "group": 23,
    //     "id": "222fb6cc9cbe42aba4bbdfb6fcca9748",
    //     "status": "NEW",
    //     "type": "MOVE_JOB"
    // }

    writer.StartObject();
    writer.Key("id");
    writer.String(m_id.c_str());
    writer.Key("type");
    writer.String(type_str(m_type));
    writer.Key("status");
    writer.String(status_str(m_status));
    writer.Key("group");
    writer.Uint64(m_group_id);
    writer.EndObject();
}

const char *Job::type_str(Type type)
{
    switch (type)
    {
    case EMPTY:
        return "EMPTY";
    case MOVE_JOB:
        return "MOVE_JOB";
    case RECOVER_DC_JOB:
        return "RECOVER_DC_JOB";
    case COUPLE_DEFRAG_JOB:
        return "COUPLE_DEFRAG_JOB";
    case RESTORE_GROUP_JOB:
        return "RESTORE_GROUP_JOB";
    }

    // unreachable
    return "UNKNOWN";
}

const char *Job::status_str(Status status)
{
    switch (status) {
    case NEW:
        return "NEW";
    case NOT_APPROVED:
        return "NOT_APPROVED";
    case EXECUTING:
        return "EXECUTING";
    case PENDING:
        return "PENDING";
    case BROKEN:
        return "BROKEN";
    case COMPLETED:
        return "COMPLETED";
    case CANCELLED:
        return "CANCELLED";
    }

    // unreachable
    return "UNKNOWN";
}
