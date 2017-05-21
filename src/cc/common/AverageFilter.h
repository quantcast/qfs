//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2017/05/09
// Author: Mike Ovsiannikov
//
// Copyright 2017 Quantcast Corp.
//
// This file is part of Quantcast File System (QFS).
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
// "Value" average IIR filter..
//
//----------------------------------------------------------------------------

#ifndef KFS_COMMON_AVERAGE_FILTER_H
#define KFS_COMMON_AVERAGE_FILTER_H

#include <inttypes.h>

namespace KFS
{

class AverageFilter
{
public:
    enum { kAvgFracBits               = 12 };
    // DecayExponent = (1 << kAvgFracBits) / exp(1. / AvgIntervalSec)
    enum { kAvg5SecondsDecayExponent   = 3353 };
    enum { kAvg10SecondsDecayExponent  = 3706 };
    enum { kAvg15SecondsDecayExponent  = 3832 };
    enum { kAvg300SecondsDecayExponent = 4082 };
    enum { kAvg600SecondsDecayExponent = 4089 };
    enum { kAvg900SecondsDecayExponent = 4091 };

    static int64_t Calculate(
        int64_t inAvg,
        int64_t inSample,
        int64_t inExponent)
    {
        // IIR filter
        const int64_t kAvgFixed_1 = int64_t(1) << kAvgFracBits;
        return ((
            inAvg * inExponent +
            (inSample << kAvgFracBits) * (kAvgFixed_1 - inExponent)
        ) >> kAvgFracBits);
    }
};

} // namespace KFS

#endif /* KFS_COMMON_AVERAGE_FILTER_H */
