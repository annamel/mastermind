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

#ifndef __8004357e_4c54_4db0_8553_edf86980616b
#define __8004357e_4c54_4db0_8553_edf86980616b

#include "Collector.h"
#include "Config.h"

#include <cocaine/framework/worker.hpp>

#include <memory>

class WorkerApplication
{
public:
    WorkerApplication();
    ~WorkerApplication();

    void init();
    void start();
    void stop();

    void force_update(cocaine::framework::worker::sender tx,
            cocaine::framework::worker::receiver rx);
    void get_snapshot(cocaine::framework::worker::sender tx,
            cocaine::framework::worker::receiver rx);
    void refresh(cocaine::framework::worker::sender tx,
            cocaine::framework::worker::receiver rx);
    void summary(cocaine::framework::worker::sender tx,
            cocaine::framework::worker::receiver rx);

    Collector & get_collector()
    { return m_collector; }

private:
    Collector m_collector;

    bool m_initialized;
};

namespace app
{

const Config & config();

}

#endif

