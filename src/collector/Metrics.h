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
#include <sstream>

#include <time.h>

#define SECONDS(nsec) (double(nsec) / 1000000000.0)

template<typename COUNT>
class Distribution
{
public:
    Distribution()
    {
        for (size_t i = 0; i < sizeof(m_count)/sizeof(m_count[0]); ++i)
            m_count[i] = 0;
    }

    void add_sample(uint64_t nsec)
    {
        if (!nsec)
            return;
        if (nsec < 1000)
            ++m_count[0];
        else if (nsec < 10000)
            ++m_count[1];
        else if (nsec < 100000)
            ++m_count[2];
        else if (nsec < 1000000)
            ++m_count[3];
        else if (nsec < 10000000)
            ++m_count[4];
        else if (nsec < 100000000)
            ++m_count[5];
        else if (nsec < 1000000000)
            ++m_count[6];
        else if (nsec < 10000000000)
            ++m_count[7];
        else if (nsec < 100000000000)
            ++m_count[8];
        else
            ++m_count[9];
    }

    bool empty() const
    {
        for (size_t i = 0; i < sizeof(m_count)/sizeof(m_count[0]); ++i) {
            if (m_count[i])
                return false;
        }
        return true;
    }

    std::string str() const
    {
        static const char * const order[] = {
            "  1 us: ",
            " 10 us: ",
            "100 us: ",
            "  1 ms: ",
            " 10 ms: ",
            "100 ms: ",
            "  1  s: ",
            " 10  s: ",
            "100  s: ",
            "   inf: "
        };

        std::ostringstream ostr;
        for (size_t i = 0; i < sizeof(m_count)/sizeof(m_count[0]); ++i) {
            if (m_count[i])
                ostr << order[i] << m_count[i] << '\n';
        }

        return ostr.str();
    }

private:
    COUNT m_count[10];
};

typedef Distribution<int> SerialDistribution;
typedef Distribution<std::atomic<int>> ConcurrentDistribution;

inline void clock_start(uint64_t & value)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    value = ts.tv_sec * 1000000000 + ts.tv_nsec;
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

