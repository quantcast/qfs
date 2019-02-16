//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2019/02/09
// Author: Mike Ovsiannikov
//
// Copyright 2019 Quantcast Corporation. All rights reserved.
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
// Generic watchdog. Thread polls [registered] values, and aborts the
// process if one or more values remain the same after configured time elapses.
//
//----------------------------------------------------------------------------

#ifndef KFS_COMMON_WATCHDOG_H
#define KFS_COMMON_WATCHDOG_H

#include "kfstypes.h"
#include "kfsatomic.h"

namespace KFS
{

class Properties;

class Watchdog
{
private:
    class Impl;
public:
    class Watched
    {
    public:
        Watched(
            const char* inNamePtr)
            : mNamePtr(inNamePtr ? inNamePtr : ""),
              mWatchdogPtr(0)
            {}
        virtual uint64_t Poll() const = 0;
        const char* GetName() const
            { return mNamePtr; }
    protected:
        virtual ~Watched()
        {
            if (mWatchdogPtr) {
                mWatchdogPtr->Unregister(*this);
            }
        }
    private:
        const char* const mNamePtr;
        Watchdog*         mWatchdogPtr; // Mutable.
    friend class Impl;
    };
    class Counters
    {
    public:
        typedef uint64_t Counter;

        Counter mPollCount;
        Counter mTimeoutCount;
        Counter mTotalTimeoutCount;
        Counter mLastChangedTimeAgoUsec;
        char    mName[64];

        Counters()
            : mPollCount(0),
              mTimeoutCount(0),
              mTotalTimeoutCount(0),
              mLastChangedTimeAgoUsec(0)
            { mName[0] = 0; }
        Counters& Add(
            const Counters& inRhs)
        {
            mPollCount         += inRhs.mPollCount;
            mTimeoutCount      += inRhs.mTimeoutCount;
            mTotalTimeoutCount += inRhs.mTotalTimeoutCount;
            if (mLastChangedTimeAgoUsec < inRhs.mLastChangedTimeAgoUsec) {
                mLastChangedTimeAgoUsec = inRhs.mLastChangedTimeAgoUsec;
            }
            return *this;
        }
    };

    Watchdog(
        bool inMustBeStrobedFlag = false);
    ~Watchdog();
    void SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters);
    void Register(
        const Watched& inWatched);
    void Unregister(
        const Watched& inWatched);
    void SetMustBeStrobed(
        bool inFlag);
    bool GetCounters(
        int       inIndex,
        Counters& outCounters);
    void Start();
    void Stop();
    void Suspend();
    void Resume();
    void Strobe()
        { SyncAddAndFetch(mStrobedValue, uint64_t(1)); }
    uint64_t GetTimeoutCount() const
        { return mTimeoutCount; }
    uint64_t GetPollCount() const
        { return mPollCount; }
    uint64_t GetTimerOverrunCount() const
        { return mTimerOverrunCount; }
    uint64_t GetTimerOverrunUsecCount() const
        { return mTimerOverrunUsecCount; }
private:
    volatile uint64_t mStrobedValue;
    uint64_t          mTimeoutCount;
    uint64_t          mPollCount;
    uint64_t          mTimerOverrunCount;
    uint64_t          mTimerOverrunUsecCount;
    Impl&             mImpl;

private:
    Watchdog(
        const Watchdog& inWatchdog);
    Watchdog& operator=(
        const Watchdog& inWatchdog);
};

}

#endif /* KFS_COMMON_WATCHDOG_H */
