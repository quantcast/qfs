//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2016/1/5
// Author: Mike Ovsiannikov
//
// Copyright 2016 Quantcast Corporation. All rights reserved.
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
// Julian day number, and unix time calculations.
//
//
//----------------------------------------------------------------------------

#ifndef KFS_COMMON_JULIAN_TIME_H
#define KFS_COMMON_JULIAN_TIME_H

#include <inttypes.h>

namespace KFS
{

    inline static int64_t
ToJulianDay(
    int inYear,
    int inMonth,
    int inDay)
{
    return ((int64_t(1461) *
        (inYear + 4800 + (inMonth - 14) / 12)) / 4 +
        (367 * (inMonth - 2 - 12 * ((inMonth - 14) / 12))) / 12 -
        (3 * ((inYear + 4900 + (inMonth - 14) / 12) / 100)) / 4 +
        inDay - 32075
    );
}

const int64_t kUnixJulianTimeStartSec =
    ToJulianDay(1970, 1, 1) * 24 * 60 * 60;

    inline static int64_t
ToUnixTime(
    int inYear,
    int inMonth,
    int inDay,
    int inHour,
    int inMinute,
    int inSecond)
{
    return (((
        ToJulianDay(inYear, inMonth, inDay) * 24 +
        inHour) * 60 + inMinute) * 60 + inSecond - kUnixJulianTimeStartSec
    );
}

} // namespace KFS

#endif /* KFS_COMMON_JULIAN_TIME_H */
