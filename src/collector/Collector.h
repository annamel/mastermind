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

#ifndef __4e5e1746_2d12_42c1_9384_b1a59c632ca3
#define __4e5e1746_2d12_42c1_9384_b1a59c632ca3

#include "Discovery.h"
#include "Inventory.h"
#include "Round.h"
#include "Storage.h"

#include <dispatch/dispatch.h>

#include <memory>

class WorkerApplication;

class on_force_update;
class on_get_snapshot;
class on_refresh;
class on_summary;

class Collector
{
public:
    Collector();

    Discovery & get_discovery()
    { return m_discovery; }

    Inventory & get_inventory()
    { return m_inventory; }

    Storage & get_storage()
    { return *m_storage; }

    uint64_t get_storage_version() const
    { return m_storage_version; }

    int init();

    void start();

    void finalize_round(Round *round);

    void stop();

public:
    void force_update(std::shared_ptr<on_force_update> handler);
    void get_snapshot(std::shared_ptr<on_get_snapshot> handler);
    void summary(std::shared_ptr<on_summary> handler);
    void refresh(std::shared_ptr<on_refresh> handler);

private:
    static void step0_start_inventory(void *arg);
    static void step1_start_round(void *arg);
    static void step1_start_forced(void *arg);
    static void step1_start_refresh(void *arg);
    static void step5_compare_and_swap(void *arg);
    static void step6_merge_and_try_again(void *arg);

    void schedule_next_round();

    static void execute_force_update(void *arg);
    static void execute_get_snapshot(void *arg);
    static void execute_summary(void *arg);

private:
    Discovery m_discovery;
    Inventory m_inventory;

    dispatch_queue_t m_queue;

    std::unique_ptr<Storage> m_storage;
    uint64_t m_storage_version;

    Round::ClockStat m_round_clock;
};

#endif

