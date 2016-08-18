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

#include "CocaineHandlers.h"
#include "FilterParser.h"
#include "Logger.h"

void on_summary::on_chunk(const char *chunk, size_t size)
{
    app::logging::DefaultAttributes holder;

    m_app.get_collector().summary(shared_from_this());
}

void on_force_update::on_chunk(const char *chunk, size_t size)
{
    app::logging::DefaultAttributes holder;

    LOG_INFO("Request to force update");
    m_app.get_collector().force_update(shared_from_this());
}

void on_get_snapshot::on_chunk(const char *chunk, size_t size)
{
    app::logging::DefaultAttributes holder;

    std::string request(chunk, size);

    LOG_INFO("Snapshot requested: '{}'", request);

    if (!request.empty()) {
        FilterParser parser(m_filter);

        rapidjson::Reader reader;
        rapidjson::StringStream ss(request.c_str());
        reader.Parse(ss, parser);

        if (!parser.good()) {
            response()->error(-1, "Incorrect filter syntax");
            response()->close();
            return;
        }
    }

    m_app.get_collector().get_snapshot(shared_from_this());
}

void on_refresh::on_chunk(const char *chunk, size_t size)
{
    app::logging::DefaultAttributes holder;

    std::string request(chunk, size);

    LOG_INFO("Refresh requested: '{}'", request);

    if (!request.empty()) {
        FilterParser parser(m_filter);

        rapidjson::Reader reader;
        rapidjson::StringStream ss(request.c_str());
        reader.Parse(ss, parser);

        if (!parser.good()) {
            response()->error(-1, "Incorrect filter syntax");
            response()->close();
            return;
        }
    }

    m_app.get_collector().refresh(shared_from_this());
}
