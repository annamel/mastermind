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

#include <Metrics.h>

#include "TestUtil.h"

namespace {

uint64_t test_clock = 0;

} // unnamed namespace

void set_test_clock(uint64_t sec, uint64_t usec)
{
    test_clock = sec * 1000000000ULL + usec * 1000ULL;
}

uint64_t clock_get_real()
{
    if (test_clock)
        return test_clock;

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (ts.tv_sec * 1000000000ULL + ts.tv_nsec);
}
