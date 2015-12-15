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

#ifndef __6b5a24ec_1b65_491c_9c5b_5840b729edb0
#define __6b5a24ec_1b65_491c_9c5b_5840b729edb0

// set_test_clock -- set value returned by clock_get_real().
// If values are zero, value of CLOCK_REALTIME will be retrieved.
// See clock_gettime(2).
void set_test_clock(uint64_t sec, uint64_t usec);

#endif

