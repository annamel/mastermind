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

#ifndef __8d66b172_08ed_4284_a984_514790ef612e
#define __8d66b172_08ed_4284_a984_514790ef612e

#include "Filter.h"
#include "Parser.h"

class FilterParser : public Parser
{
    typedef Parser super;

public:
    FilterParser(Filter & filter);

    virtual bool String(const char* str, rapidjson::SizeType length, bool copy);

    virtual bool StartArray()
    {
        ++m_array_depth;
        return true;
    }

    virtual bool UInteger(uint64_t val);

    virtual bool EndArray(rapidjson::SizeType nr_elements)
    {
        if (! --m_array_depth)
            clear_key();
        return true;
    }

private:
    Filter & m_filter;
    int m_array_depth;
};

#endif

