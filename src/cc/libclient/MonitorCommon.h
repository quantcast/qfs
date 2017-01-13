//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2016/02/17
// Author: Mehmet Can Kurt
//
// Copyright 2016-2017 Quantcast Corporation. All rights reserved.
//
// This file is part of Quantcast File System.
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
//----------------------------------------------------------------------------

#ifndef SRC_CC_LIBCLIENT_MONITORCOMMON_H_
#define SRC_CC_LIBCLIENT_MONITORCOMMON_H_

#include "common/kfsdecls.h"

#include <fstream>
#include <map>
#include <string>

#include <stdint.h>

namespace KFS
{
using std::ofstream;
using std::map;
using std::string;

typedef int64_t Counter;

struct ErrorCounters
{
    ErrorCounters()
        : mErrorParametersCount(0),
          mErrorIOCount(0),
          mErrorTryAgainCount(0),
          mErrorNoEntryCount(0),
          mErrorBusyCount(0),
          mErrorChecksumCount(0),
          mErrorLeaseExpiredCount(0),
          mErrorFaultCount(0),
          mErrorInvalChunkSizeCount(0),
          mErrorPermissionsCount(0),
          mErrorMaxRetryReachedCount(0),
          mErrorRequeueRequiredCount(0),
          mErrorOtherCount(0),
          mTotalErrorCount(0)
        {}
    void Clear()
        { *this = ErrorCounters(); }
    Counter mErrorParametersCount;
    Counter mErrorIOCount;
    Counter mErrorTryAgainCount;
    Counter mErrorNoEntryCount;
    Counter mErrorBusyCount;
    Counter mErrorChecksumCount;
    Counter mErrorLeaseExpiredCount;
    Counter mErrorFaultCount;
    Counter mErrorInvalChunkSizeCount;
    Counter mErrorPermissionsCount;
    Counter mErrorMaxRetryReachedCount;
    Counter mErrorRequeueRequiredCount;
    Counter mErrorOtherCount;
    Counter mTotalErrorCount;
};

struct ChunkserverErrorCounters
{
    ErrorCounters readErrors;
    ErrorCounters writeErrors;
    void Clear()
    {
        readErrors.Clear();
        writeErrors.Clear();
    }
};

typedef map<ServerLocation, ChunkserverErrorCounters> ChunkServerErrorMap;
typedef map<string, Counter> ClientCounters;

}

#endif /* SRC_CC_LIBCLIENT_MONITORCOMMON_H_ */
