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
#include "ConfigParser.h"
#include "WorkerApplication.h"

#include "Storage.h"

#include <elliptics/logger.hpp>
#include <rapidjson/reader.h>
#include <rapidjson/filereadstream.h>

#include <cstdio>
#include <stdexcept>

namespace {

std::unique_ptr<ioremap::elliptics::logger_base> s_logger;
std::unique_ptr<ioremap::elliptics::logger_base> s_elliptics_logger;

Config s_config;

void load_config()
{
    ConfigParser parser(s_config);

    FILE *f = fopen(Config::config_file, "rb");
    if (f == nullptr)
        throw std::runtime_error(std::string("Cannot open ") + Config::config_file);

    static char buf[65536];
    rapidjson::FileReadStream is(f, buf, sizeof(buf));
    rapidjson::Reader reader;
    reader.Parse(is, parser);

    fclose(f);

    if (!parser.good())
        throw std::runtime_error(std::string("Error parsing ") + Config::config_file);

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

WorkerApplication::WorkerApplication(cocaine::framework::dispatch_t & d)
{
    init();

    d.on<on_summary>("summary", *this);
    d.on<on_force_update>("force_update", *this);
    d.on<on_get_snapshot>("get_snapshot", *this);
    d.on<on_refresh>("refresh", *this);

    start();
}

WorkerApplication::~WorkerApplication()
{
    stop();
}

void WorkerApplication::init()
{
    load_config();

    s_logger.reset(new ioremap::elliptics::file_logger(
            Config::log_file, ioremap::elliptics::log_level(s_config.dnet_log_mask)));

    s_elliptics_logger.reset(new ioremap::elliptics::file_logger(
            Config::elliptics_log_file, ioremap::elliptics::log_level(s_config.dnet_log_mask)));

    const char *config_file = Config::config_file;
    BH_LOG(app::logger(), DNET_LOG_INFO, "Loaded config from %s:\n%s", config_file, s_config);

    if (m_collector.init())
        throw std::runtime_error("failed to initialize collector");

    m_initialized = true;
}

void WorkerApplication::stop()
{
    // invoke stop() exactly one time
    if (m_initialized) {
        m_collector.stop();
        m_initialized = false;
    }
}

void WorkerApplication::start()
{
    m_collector.start();
}

namespace app
{

ioremap::elliptics::logger_base & logger()
{
    return *s_logger;
}

ioremap::elliptics::logger_base & elliptics_logger()
{
    return *s_elliptics_logger;
}

const Config & config()
{
    return s_config;
}

} // app
