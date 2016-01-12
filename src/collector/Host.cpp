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

#include "Host.h"

void Host::merge(const Host & other, bool & have_newer)
{
    // TODO: Add timestamp

    if (m_name.empty()) {
        if (!other.m_name.empty())
            m_name = other.m_name;
    } else if (other.m_name.empty()) {
        have_newer = true;
    }

    if (m_dc.empty()) {
        if (!other.m_dc.empty())
            m_dc = other.m_dc;
    } else if (other.m_dc.empty()) {
        have_newer = true;
    }
}

void Host::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const
{
    // JSON looks like this:
    // {
    //     "id": "2001:cdba::3257:9652",
    //     "name": "node1.elliptics.mystorage.com",
    //     "dc": "changbu"
    // }

    writer.StartObject();
    writer.Key("id");
    writer.String(m_addr.c_str());
    writer.Key("name");
    writer.String(m_name.c_str());
    writer.Key("dc");
    writer.String(m_dc.c_str());
    writer.EndObject();
}
