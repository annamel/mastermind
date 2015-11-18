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

#ifndef __d3b51655_27e8_4227_a7eb_a598183c39cd
#define __d3b51655_27e8_4227_a7eb_a598183c39cd

#include <elliptics/session.hpp>

#include <memory>

class Collector;
class Round;

// Discovery is responsible for maintaining elliptics connection,
// root session, provides Storage with a list of nodes, and
// performs global initialization and deinitialization of cURL,
// elliptics, and MongoDB driver. Only a single instance of
// this object is created.
class Discovery
{
public:
    Discovery(Collector & collector);
    ~Discovery();

    int init_curl();
    int init_elliptics();
    int init_mongo();

    ioremap::elliptics::session & get_session()
    { return *m_session; }

    void resolve_nodes(Round & round);

    void stop_mongo();
    void stop_elliptics();
    void stop_curl();

    uint64_t get_resolve_nodes_duration() const
    { return m_resolve_nodes_duration; }

private:
    Collector & m_collector;

    std::unique_ptr<ioremap::elliptics::node> m_node;
    std::unique_ptr<ioremap::elliptics::session> m_session;

    uint64_t m_resolve_nodes_duration;
};

#endif

