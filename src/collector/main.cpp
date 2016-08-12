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

#include <cocaine/framework/worker.hpp>

#include <syslog.h>

using namespace cocaine;
using namespace cocaine::framework;

int main(int argc, char **argv)
{
    // Include PID in syslog messages and print to stderr as well.
    // See SYSLOG(3) for details on openlog() and syslog().
    openlog(nullptr, LOG_PID|LOG_PERROR, LOG_USER);

    WorkerApplication app;

    try {
        app.init();
    } catch (const std::exception & e) {
        syslog(LOG_ERR, "%s", e.what());
        return 1;
    }

    worker_t worker{options_t{argc, argv}};

    worker.on("summary", [&](worker::sender tx, worker::receiver rx) {
        app.summary(std::move(tx), std::move(rx));
    });

    worker.on("force_update", [&](worker::sender tx, worker::receiver rx) {
        app.force_update(std::move(tx), std::move(rx));
    });

    worker.on("get_snapshot", [&](worker::sender tx, worker::receiver rx) {
        app.get_snapshot(std::move(tx), std::move(rx));
    });

    worker.on("refresh", [&](worker::sender tx, worker::receiver rx) {
        app.refresh(std::move(tx), std::move(rx));
    });

    try {
        app.start();
    } catch (const std::exception & e) {
        syslog(LOG_ERR, "%s", e.what());
        return 1;
    }

    return worker.run();
}
