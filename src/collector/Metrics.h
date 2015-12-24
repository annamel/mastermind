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

#ifndef __79e93520_3d35_496a_83b9_fd0f7764b0ff
#define __79e93520_3d35_496a_83b9_fd0f7764b0ff

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include <time.h>

#define MSEC(nsec) (double(nsec) / 1000000.0)

class Distribution
{
public:
    Distribution(int nr_bins = 10);
    ~Distribution();

    void add_sample(uint64_t sample);

    bool empty() const;

    std::string str();

private:
    struct Bin
    {
        Bin()
            :
            value(),
            count()
        {}

        bool operator < (const Bin & other) const
        { return value < other.value; }

        uint64_t value;
        uint64_t count;
    };

    std::vector<Bin> m_bins;
};

inline void clock_get(uint64_t & value)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    value = ts.tv_sec * 1000000000 + ts.tv_nsec;
}

uint64_t clock_get_real();

inline void clock_start(uint64_t & value)
{
    clock_get(value);
}

inline void clock_stop(uint64_t & value)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    value = ts.tv_sec * 1000000000 + ts.tv_nsec - value;
}

class Stopwatch
{
public:
    Stopwatch(uint64_t & record)
        :
        m_record(record),
        m_stopped(false)
    {
        clock_start(m_clock);
    }

    Stopwatch(uint64_t & record, uint64_t init)
        :
        m_record(record),
        m_clock(init),
        m_stopped(false)
    {}

    ~Stopwatch()
    {
        if (!m_stopped)
            stop();
    }

    void stop()
    {
        clock_stop(m_clock);
        m_record = m_clock;
        m_stopped = true;
    }

private:
    uint64_t & m_record;
    uint64_t m_clock;
    bool m_stopped;
};


std::string timeval_user_friendly(time_t sec, int usec);

#endif

