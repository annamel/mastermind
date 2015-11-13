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
#include "FS.h"
#include "Metrics.h"
#include "Node.h"
#include "WorkerApplication.h"

#include "Job.h"

Collector::Collector()
    :
    m_discovery(*this),
    m_storage_version(1)
{
    m_queue = dispatch_queue_create("collector", DISPATCH_QUEUE_CONCURRENT);
    dispatch_set_target_queue(m_queue, dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_HIGH, 0));

    m_storage.reset(new Storage());
}

int Collector::init()
{
    if (m_discovery.init_curl())
        return -1;

    if (m_discovery.init_elliptics()) {
        m_discovery.stop_curl();
        return -1;
    }

    if (m_discovery.init_mongo()) {
        m_discovery.stop_elliptics();
        m_discovery.stop_curl();
        return -1;
    }

    m_inventory.init();

    return 0;
}

void Collector::start()
{
    BH_LOG(app::logger(), DNET_LOG_INFO, "Collector: Dispatching step 0");
    dispatch_async_f(m_queue, this, &Collector::step0_start_inventory);
}

void Collector::finalize_round(Round *round)
{
    dispatch_barrier_async_f(m_queue, round, &Collector::step5_compare_and_swap);
}

void Collector::stop()
{
    m_inventory.stop();
    m_discovery.stop_mongo();
    m_discovery.stop_elliptics();
    m_discovery.stop_curl();
}

void Collector::step0_start_inventory(void *arg)
{
    Collector & self = *static_cast<Collector*>(arg);

    BH_LOG(app::logger(), DNET_LOG_INFO, "Collector: Starting inventory (initial download)");
    self.m_inventory.download_initial();

    BH_LOG(app::logger(), DNET_LOG_INFO, "Collector: Dispatching step 1");
    dispatch_async_f(self.m_queue, &self, &Collector::step1_start_round);
}

void Collector::step1_start_round(void *arg)
{
    Collector & self = *static_cast<Collector*>(arg);

    BH_LOG(app::logger(), DNET_LOG_INFO, "Collector round: step 1");

    Round *round = new Round(self);
    self.m_discovery.resolve_nodes(*round);
    round->start();
}

void Collector::step1_start_forced(void *arg)
{
    std::unique_ptr<std::shared_ptr<on_force_update>> handler_ptr(
            static_cast<std::shared_ptr<on_force_update>*>(arg));
    Collector & self = (*handler_ptr)->get_app().get_collector();

    BH_LOG(app::logger(), DNET_LOG_INFO, "Collector user-requested full round: step 1");

    Round *round = new Round(self, *handler_ptr);
    self.m_discovery.resolve_nodes(*round);
    round->start();
}

void Collector::step1_start_refresh(void *arg)
{
    std::unique_ptr<std::shared_ptr<on_refresh>> handler_ptr(
            static_cast<std::shared_ptr<on_refresh>*>(arg));
    Collector & self = (*handler_ptr)->get_app().get_collector();

    BH_LOG(app::logger(), DNET_LOG_INFO, "Collector user-requested refresh round: step 1");

    Round *round = new Round(self, *handler_ptr);
    round->start();
}

void Collector::step5_compare_and_swap(void *arg)
{
    std::unique_ptr<Round> round(static_cast<Round*>(arg));
    Collector & self = round->get_collector();

    if (self.m_storage_version == round->get_old_storage_version()) {
        BH_LOG(app::logger(), DNET_LOG_INFO, "Swapping storage");
        round->swap_storage(self.m_storage);
        ++self.m_storage_version;
    } else {
        BH_LOG(app::logger(), DNET_LOG_INFO,
                "Collector's storage has newer version %lu (Round's one has %lu)",
                self.m_storage_version, round->get_old_storage_version());
        dispatch_async_f(self.m_queue, round.release(), &Collector::step6_merge_and_try_again);
        return;
    }

    Round::ClockStat & round_clock = round->get_clock();
    clock_stop(round_clock.total);

    Round::Type type = round->get_type();
    if (type == Round::REGULAR || type == Round::FORCED_FULL) {
        self.m_round_clock = round->get_clock();
        if (type == Round::REGULAR) {
            self.schedule_next_round();
        } else {
            std::shared_ptr<on_force_update> handler = round->get_on_force_handler();

            std::ostringstream ostr;
            ostr << "Update completed in " << MSEC(round_clock.total) << " ms";
            handler->response()->write(ostr.str());
            handler->response()->close();
        }
    } else {
        std::shared_ptr<on_refresh> handler = round->get_on_refresh_handler();

        std::ostringstream ostr;
        ostr << "Refresh completed in " << MSEC(round_clock.total) << " ms";
        handler->response()->write(ostr.str());
        handler->response()->close();
    }
}

void Collector::step6_merge_and_try_again(void *arg)
{
    Round *round = static_cast<Round*>(arg);
    Collector & self = round->get_collector();

    bool have_newer = false;
    round->update_storage(*self.m_storage, self.m_storage_version, have_newer);

    if (!have_newer) {
        BH_LOG(app::logger(), DNET_LOG_INFO,
                "Existing storage is up-to-date, not performing swap");
        Round::Type type = round->get_type();
        if (type == Round::REGULAR) {
            self.schedule_next_round();
        } else {
            static const std::string response = "Round completed, but nothing to update yet";
            if (type == Round::FORCED_FULL) {
                std::shared_ptr<on_force_update> handler = round->get_on_force_handler();
                handler->response()->write(response);
                handler->response()->close();
            } else {
                std::shared_ptr<on_refresh> handler = round->get_on_refresh_handler();
                handler->response()->write(response);
                handler->response()->close();
            }
        }
    }

    BH_LOG(app::logger(), DNET_LOG_INFO, "Storage updated, scheduling a new CAS");
    dispatch_barrier_async_f(self.m_queue, round, &Collector::step5_compare_and_swap);
}

void Collector::schedule_next_round()
{
    BH_LOG(app::logger(), DNET_LOG_INFO, "Scheduling next round");

    dispatch_after_f(dispatch_time(DISPATCH_TIME_NOW, 60000000000L),
            m_queue, this, &Collector::step1_start_round);
}

void Collector::force_update(std::shared_ptr<on_force_update> handler)
{
    dispatch_async_f(m_queue, new std::shared_ptr<on_force_update>(handler), &Collector::step1_start_forced);
}

void Collector::get_snapshot(std::shared_ptr<on_get_snapshot> handler)
{
    dispatch_async_f(m_queue, new std::shared_ptr<on_get_snapshot>(handler), &Collector::execute_get_snapshot);
}

void Collector::summary(std::shared_ptr<on_summary> handler)
{
    dispatch_async_f(m_queue, new std::shared_ptr<on_summary>(handler), &Collector::execute_summary);
}

void Collector::refresh(std::shared_ptr<on_refresh> handler)
{
    dispatch_async_f(m_queue, new std::shared_ptr<on_refresh>(handler), &Collector::step1_start_refresh);
}

void Collector::execute_get_snapshot(void *arg)
{
    std::unique_ptr<std::shared_ptr<on_get_snapshot>> handler_ptr(
            static_cast<std::shared_ptr<on_get_snapshot>*>(arg));
    Collector & self = (*handler_ptr)->get_app().get_collector();

    Filter & filter = (*handler_ptr)->get_filter();
    std::string result;

    if (filter.empty())
        self.m_storage->print_json(filter.item_types, !!filter.show_internals, result);
    else
        self.m_storage->print_json(filter, !!filter.show_internals, result);

    (*handler_ptr)->response()->write(result);
    (*handler_ptr)->response()->close();
}

void Collector::execute_summary(void *arg)
{
    std::unique_ptr<std::shared_ptr<on_summary>> handler_ptr(
            static_cast<std::shared_ptr<on_summary>*>(arg));
    Collector & self = (*handler_ptr)->get_app().get_collector();

    std::map<std::string, Node> & nodes = self.m_storage->get_nodes();
    std::map<int, Group> & groups = self.m_storage->get_groups();
    std::map<std::string, Couple> & couples = self.m_storage->get_couples();
    std::map<int, Job> & jobs = self.m_storage->get_jobs();

    std::map<Backend::Status, int> backend_status;
    std::map<Group::Status, int> group_status;
    std::map<Group::Type, int> group_type;
    std::map<Couple::Status, int> couple_status;
    std::map<FS::Status, int> fs_status;
    std::map<Job::Status, int> job_status;
    size_t nr_backends = 0;
    size_t nr_filesystems = 0;

    for (auto it = groups.begin(); it != groups.end(); ++it) {
        ++group_status[it->second.get_status()];
        ++group_type[it->second.get_type()];
    }

    for (auto it = couples.begin(); it != couples.end(); ++it)
        ++couple_status[it->second.get_status()];

    for (auto nit = nodes.begin(); nit != nodes.end(); ++nit) {
        Node & node = nit->second;

        const std::map<int, Backend> & backends = node.get_backends();
        nr_backends += node.get_backends().size();
        for (auto bit = backends.begin(); bit != backends.end(); ++bit)
            ++backend_status[bit->second.get_status()];

        const std::map<uint64_t, FS> & fs = node.get_filesystems();
        nr_filesystems += fs.size();
        for (auto fsit = fs.begin(); fsit != fs.end(); ++fsit)
            ++fs_status[fsit->second.get_status()];
    }

    for (auto it = jobs.begin(); it != jobs.end(); ++it)
        ++job_status[it->second.get_status()];

    std::ostringstream ostr;

    ostr << "Storage contains:\n"
         << nodes.size() << " nodes\n";

    ostr << nr_filesystems << " filesystems\n  ( ";
    for (auto it = fs_status.begin(); it != fs_status.end(); ++it)
        ostr << it->second << ' ' << FS::status_str(it->first) << ' ';

    ostr << ")\n" << nr_backends << " backends\n  ( ";
    for (auto it = backend_status.begin(); it != backend_status.end(); ++it)
        ostr << it->second << ' ' << Backend::status_str(it->first) << ' ';

    ostr << ")\n" << groups.size() << " groups\n  ( ";
    for (auto it = group_status.begin(); it != group_status.end(); ++it)
        ostr << it->second << ' ' << Group::status_str(it->first) << ' ';
    ostr << ")\n  ( ";
    for (auto it = group_type.begin(); it != group_type.end(); ++it)
        ostr << it->second << ' ' << Group::type_str(it->first) << ' ';

    ostr << ")\n" << couples.size() << " couples\n  ( ";
    for (auto it = couple_status.begin(); it != couple_status.end(); ++it)
        ostr << it->second << ' ' << Couple::status_str(it->first) << ' ';
    ostr << ")\n";

    ostr << self.m_storage->get_namespaces().size() << " namespaces\n"
         << jobs.size() << " jobs\n  ( ";
    for (auto it = job_status.begin(); it != job_status.end(); ++it)
        ostr << it->second << ' ' << Job::status_str(it->first) << ' ';
    ostr << ")\n";

    ostr << "Round metrics:\n"
            "  Total time: " << MSEC(self.m_round_clock.total) << " ms\n"
            "  Resolve nodes: " << MSEC(self.m_discovery.get_resolve_nodes_duration()) << " ms\n"
            "  Jobs & history databases: " << MSEC(self.m_round_clock.mongo) << " ms\n"
            "  HTTP download time: " << MSEC(self.m_round_clock.perform_download) << " ms\n"
            "  Remaining JSON parsing and jobs processing after HTTP download completed: "
                << MSEC(self.m_round_clock.finish_monitor_stats_and_jobs) << " ms\n"
            "  Metadata download: " << MSEC(self.m_round_clock.metadata_download) << " ms\n"
            "  Storage update: " << MSEC(self.m_round_clock.storage_update) << " ms\n"
            "  Storage merge: " << MSEC(self.m_round_clock.merge_time) << " ms\n";

    {
        Distribution distrib_stats_parse;
        Distribution distrib_update_fs;

        for (auto it = nodes.begin(); it != nodes.end(); ++it) {
            const Node::ClockStat & stat = it->second.get_clock_stat();
            distrib_stats_parse.add_sample(stat.stats_parse);
            distrib_update_fs.add_sample(stat.update_fs);
        }

        ostr << "\nDistribution for node stats parsing:\n" << distrib_stats_parse.str() << "\n"
                "Distribution for node fs update:\n" << distrib_update_fs.str() << '\n';
    }

    {
        Distribution distrib;
        for (auto it = groups.begin(); it != groups.end(); ++it)
            distrib.add_sample(it->second.get_metadata_parse_duration());
        ostr << "Distribution for group metadata processing:\n" << distrib.str() << '\n';
    }

    {
        Distribution distrib;
        for (auto it = couples.begin(); it != couples.end(); ++it)
            distrib.add_sample(it->second.get_update_status_duration());
        ostr << "Distribution for couple update_status:\n" << distrib.str();
    }

    (*handler_ptr)->response()->write(ostr.str());
    (*handler_ptr)->response()->close();
}
