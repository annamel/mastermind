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

#include "Metrics.h"

#include <algorithm>
#include <sstream>

// Histogram building algorithm is described in a paper
// "A Streaming Parallel Decision Tree Algorithm"
// Yael Ben-Haim, Elad Tom-Tov. 2010.
// http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf

Distribution::Distribution(int nr_bins)
    :
    m_bins(nr_bins + 1)
{}

Distribution::~Distribution()
{}

void Distribution::add_sample(uint64_t sample)
{
    m_bins.back().value = sample;
    m_bins.back().count = 1;

    std::sort(m_bins.begin(), m_bins.end());

    uint64_t min_diff = m_bins[1].value - m_bins[0].value;
    size_t idx_min = 0;
    for (size_t i = 1; i < (m_bins.size() - 1); ++i) {
        uint64_t cur_diff = m_bins[i + 1].value - m_bins[i].value;
        if (cur_diff < min_diff) {
            min_diff = cur_diff;
            idx_min = i;
        }
    }

    uint64_t vi = m_bins[idx_min].value;
    uint64_t vi1 = m_bins[idx_min + 1].value;
    uint64_t ci = m_bins[idx_min].count;
    uint64_t ci1 = m_bins[idx_min + 1].count;

    if ((ci + ci1) > 0)
        m_bins[idx_min].value = (vi * ci + vi1 * ci1) / (ci + ci1);
    else
        m_bins[idx_min].value = 0;
    m_bins[idx_min].count = (ci + ci1);

    m_bins.erase(m_bins.begin() + idx_min + 1);
    m_bins.push_back(Bin());
}

bool Distribution::empty() const
{
    for (const Bin & bin : m_bins) {
        if (bin.count)
            return false;
    }
    return true;
}

std::string Distribution::str()
{
    if (empty())
        return std::string("<empty>\n");

    std::ostringstream ostr;
    for (size_t i = 0; i < (m_bins.size() - 1); ++i) {
        uint64_t value = m_bins[i].value;
        uint64_t count = m_bins[i].count;

        if (value < 1000ULL)
            ostr << value << " ns: " << count << '\n';
        else if (value < 1000000ULL)
            ostr << value / 1000ULL << " us: " << count << '\n';
        else if (value < 10000000000ULL)
            ostr << value / 1000000ULL << " ms: " << count << '\n';
        else
            ostr << value / 1000000000ULL << " s " << value % 1000000000ULL << " ms: " << count << '\n';
    }
    return ostr.str();
}

std::string timeval_user_friendly(time_t sec, int usec)
{
    struct tm tm_buf;
    struct tm *res = localtime_r(&sec, &tm_buf);
    if (res == NULL)
        return std::string();

    // res->tm_year is the number of years since 1900
    // res->tm_mon is the number of months since January (0-11)
    // res->tm_mday is in the range 1-31
    char buf[64];
    sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d.%06d",
            res->tm_year + 1900, res->tm_mon + 1, res->tm_mday,
            res->tm_hour, res->tm_min, res->tm_sec, usec);
    return std::string(buf);
}
