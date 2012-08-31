//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/08/31
// Author: Sriram Rao
//         Mike Ovsiannikov
//
// Copyright 2008-2012 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
//
// This file is part of Kosmos File System (KFS).
//
// Licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// \brief Miscellaneous utility functions.
//
//----------------------------------------------------------------------------

#include "utils.h"
#include "common/RequestParser.h"

#include <sys/time.h>
#include <sys/types.h>
#include <sys/select.h>
#include <unistd.h>

namespace KFS
{
namespace client
{

void
Sleep(int secs)
{
    if (secs <= 0) {
        return;
    }
    struct timeval end;
    gettimeofday(&end, 0);
    end.tv_sec += secs;
    struct timeval tm;
    tm.tv_sec  = secs;
    tm.tv_usec = 0;
    for(; ;) {
        if (select(0, 0, 0, 0, &tm) == 0) {
            break;
        }
        gettimeofday(&tm, 0);
        if (tm.tv_sec + secs + 1 < end.tv_sec || // backward clock jump
                end.tv_sec < tm.tv_sec ||
                (end.tv_sec == tm.tv_sec &&
                end.tv_usec <= tm.tv_usec + 10000)) {
            break;
        }
        if (end.tv_usec < tm.tv_usec) {
            tm.tv_sec  = end.tv_sec - tm.tv_sec - 1;
            tm.tv_usec = 1000000 - tm.tv_usec + end.tv_usec;
        } else {
            tm.tv_sec  = end.tv_sec  - tm.tv_sec;
            tm.tv_usec = end.tv_usec - tm.tv_usec;
        }
    }
}

void
GetTimeval(const char* s, struct timeval& tv)
{
    const char*       p = s;
    const char* const e = p + (p ? strlen(s) : 0);
    if (! p ||
            ! DecIntParser::Parse(p, e - p, tv.tv_sec) ||
            ! DecIntParser::Parse(p, e - p, tv.tv_usec)) {
        tv.tv_sec  = 0;
        tv.tv_usec = 0;
    }
}

}
}
