/*
   Copyright (c) YANDEX LLC, 2016. All rights reserved.
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

#ifndef __e0c91505_90c3_48d8_9359_465cf9a89f96
#define __e0c91505_90c3_48d8_9359_465cf9a89f96

#include <blackhole/extensions/facade.hpp>
#include <blackhole/logger.hpp>
#include <blackhole/scope/holder.hpp>

#include <string>

namespace app {

namespace logging {

enum severity {
    debug = 0,
    notice,
    info,
    warning,
    error
};

// This must be called from the main thread on start.
void init_logger(const std::string & app_log_file,
        const std::string &elliptics_log_file, int severity_min);

// NOTE: do not use before init_logger().
blackhole::logger_t & logger();

// NOTE: do not use before init_logger().
blackhole::logger_t & elliptics_logger();

class DefaultAttributes : public blackhole::scope::holder_t {
public:
    DefaultAttributes();
};

} // namespace logging

} // namespace app

#define __LOG__(__severity__, ...) \
    ::blackhole::logger_facade<blackhole::logger_t>( \
        ::app::logging::logger()).log((__severity__), __VA_ARGS__)

#define LOG_DEBUG(...) __LOG__(::app::logging::debug, __VA_ARGS__)

#define LOG_NOTICE(...) __LOG__(::app::logging::notice, __VA_ARGS__)

#define LOG_INFO(...) __LOG__(::app::logging::info, __VA_ARGS__)

#define LOG_WARNING(...) __LOG__(::app::logging::warning, __VA_ARGS__)

#define LOG_ERROR(...) __LOG__(::app::logging::error, __VA_ARGS__)

#endif // __e0c91505_90c3_48d8_9359_465cf9a89f96

