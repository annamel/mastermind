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

#include "WorkerApplication.h"
#include "ConfigParser.h"
#include "FilterParser.h"
#include "Logger.h"

#include "Storage.h"

#include <rapidjson/reader.h>
#include <rapidjson/filereadstream.h>

#include <cstdio>
#include <stdexcept>

namespace {

Config s_config;

void load_config()
{
    ConfigParser parser(s_config);

    FILE *f = fopen(Config::Default::config_file, "rb");
    if (f == nullptr)
        throw std::runtime_error(std::string("Cannot open ") + Config::Default::config_file);

    static char buf[65536];
    rapidjson::FileReadStream is(f, buf, sizeof(buf));
    rapidjson::Reader reader;
    reader.Parse(is, parser);

    fclose(f);

    if (!parser.good())
        throw std::runtime_error(std::string("Error parsing ") + Config::Default::config_file);

    if (s_config.reserved_space == 0)
        throw std::runtime_error("Incorrect value 0 for reserved_space");

    if (s_config.app_name.empty())
        s_config.app_name = "mastermind";
}

} // unnamed namespace

WorkerApplication::WorkerApplication()
    :
    m_initialized(false)
{}

WorkerApplication::~WorkerApplication()
{
    stop();
}

void WorkerApplication::init()
{
    load_config();

    app::logging::init_logger(
            Config::Default::log_file,
            Config::Default::elliptics_log_file,
            s_config.dnet_log_mask);

    app::logging::DefaultAttributes holder;

    LOG_INFO("Loaded config from {}", Config::Default::config_file);

    if (m_collector.init())
        throw std::runtime_error("failed to initialize collector");

    m_initialized = true;
}

void WorkerApplication::stop()
{
    // invoke stop() exactly one time
    if (m_initialized) {
        app::logging::DefaultAttributes holder;

        m_collector.stop();
        m_initialized = false;
    }
}

void WorkerApplication::start()
{
    app::logging::DefaultAttributes holder;

    m_collector.start();
}

void WorkerApplication::force_update(cocaine::framework::worker::sender tx,
        cocaine::framework::worker::receiver rx)
{
    app::logging::DefaultAttributes holder;

    LOG_INFO("Request to force update");

    m_collector.force_update(std::move(tx));
}

void WorkerApplication::get_snapshot(cocaine::framework::worker::sender tx,
            cocaine::framework::worker::receiver rx)
{
    app::logging::DefaultAttributes holder;

    std::string request = [&]() {
        auto opt = rx.recv().get();
        if (opt)
            return *opt;
        return std::string{};
    }();

    LOG_INFO("Snapshot requested: '{}'", request);

    Filter filter;
    if (!request.empty()) {
        FilterParser parser(filter);

        rapidjson::Reader reader;
        rapidjson::StringStream ss(request.c_str());
        reader.Parse(ss, parser);

        if (!parser.good()) {
            tx.error(-1, "Incorrect filter syntax");
            return;
        }
    }

    m_collector.get_snapshot(std::move(tx), std::move(filter));
}

void WorkerApplication::refresh(cocaine::framework::worker::sender tx,
        cocaine::framework::worker::receiver rx)
{
    app::logging::DefaultAttributes holder;

    std::string request = [&]() {
        auto opt = rx.recv().get();
        if (opt)
            return *opt;
        return std::string{};
    }();

    LOG_INFO("Refresh requested: '{}'", request);

    Filter filter;
    if (!request.empty()) {
        FilterParser parser(filter);

        rapidjson::Reader reader;
        rapidjson::StringStream ss(request.c_str());
        reader.Parse(ss, parser);

        if (!parser.good()) {
            tx.error(-1, "Incorrect filter syntax");
            return;
        }
    }

    m_collector.refresh(std::move(tx), std::move(filter));
}

void WorkerApplication::summary(cocaine::framework::worker::sender tx,
        cocaine::framework::worker::receiver rx)
{
    app::logging::DefaultAttributes holder;

    m_collector.summary(std::move(tx));
}

namespace app
{

const Config & config()
{
    return s_config;
}

} // app
