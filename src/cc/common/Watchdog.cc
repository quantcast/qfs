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

#include "Watchdog.h"
#include "Properties.h"
#include "MsgLogger.h"
#include "time.h"

#include "qcdio/QCMutex.h"
#include "qcdio/qcstutils.h"
#include "qcdio/QCUtils.h"
#include "qcdio/QCThread.h"
#include "qcdio/QCThread.h"

#include <vector>
#include <algorithm>

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>

namespace KFS
{
using std::vector;
using std::max;

class Watchdog::Impl : private QCRunnable
{
public:
    Impl(
        volatile uint64_t const& inStrobedValue,
        uint64_t&                inTimoutCount,
        uint64_t&                inPollCount)
        : QCRunnable(),
          mWatchedList(),
          mMutex(),
          mCond(),
          mStrobed(inStrobedValue),
          mThread(this, "Watchdog"),
          mPollIntervalUsec(int64_t(16) * 1000 * 1000),
          mMinPollIntervalUsec(mPollIntervalUsec * 3 / 4),
          mPollIntervalNanoSec(QCMutex::Time(mPollIntervalUsec) * 1000),
          mTimeoutLogIntervalUsec(0),
          mMaxTimeoutCount(-1),
          mRunFlag(false),
          mSuspendFlag(false),
          mPollStartTime(),
          mPollEndTime(),
          mTimoutCount(inTimoutCount),
          mPollCount(inPollCount),
          mFailureWriteRet()
    {}
    ~Impl()
    {
        Impl::Stop();
        QCStMutexLocker theLocker(mMutex);
        for (WatchedList::iterator theIt = mWatchedList.begin();
                mWatchedList.end() != theIt; ++theIt) {
            SetWatchdogPtr(*(theIt->GetWatchedPtr()), 0);
        }
    }
    void SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters)
    {
        Properties::String theName(inPrefixPtr ? inPrefixPtr : "");
        const size_t       thePrefixLen = theName.length();
        mPollIntervalUsec = (uint64_t)(1e6 * max(4., inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("pollIntervalSec"),
            mPollIntervalUsec * 1e-6
        )));
        mMinPollIntervalUsec = mPollIntervalUsec * 3 / 4;
        mPollIntervalNanoSec = QCMutex::Time(mPollIntervalUsec) * 1000;
        mMaxTimeoutCount = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("maxTimeoutCount"),
            mMaxTimeoutCount
        );
        mTimeoutLogIntervalUsec = (int64_t)(1e6 * inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("timeoutLogIntervalSec"),
            mTimeoutLogIntervalUsec * 1e-6
        ));
    }
    void Register(
        const Watched& inWatched,
        Watchdog&      inWatchdog)
    {
        QCStMutexLocker theLocker(mMutex);

        if (Find(inWatched) == mWatchedList.end()) {
            mWatchedList.push_back(WatchedListEntry(&inWatched));
            QCRTASSERT(! inWatched.mWatchdogPtr ||
                    &inWatchdog == inWatched.mWatchdogPtr);
            SetWatchdogPtr(inWatched, &inWatchdog);
        }
    }
    void Unregister(
        const Watched& inWatched,
        Watchdog&      inWatchdog)
    {
        QCStMutexLocker theLocker(mMutex);

        WatchedList::iterator const theIt = Find(inWatched);
        if (mWatchedList.end() != theIt) {
            QCRTASSERT(&inWatchdog == inWatched.mWatchdogPtr);
            mWatchedList.erase(theIt);
            SetWatchdogPtr(inWatched, 0);
        }
    }
    bool GetCounters(
        size_t    inIndex,
        Counters& outCounters)
    {
        QCStMutexLocker theLocker(mMutex);

        if (mWatchedList.size() <= inIndex) {
            return false;
        }
        mWatchedList[inIndex].GetCounters(outCounters);
        return true;
    }
    void Start()
    {
        QCStMutexLocker theLocker(mMutex);

        if (mRunFlag) {
            return;
        }
        mRunFlag = true;
        const int kStackSize = 128 << 10;
        mThread.Start(this, kStackSize);
    }
    void Stop()
    {
        QCStMutexLocker theLocker(mMutex);

        mRunFlag = false;
        mCond.Notify();
        theLocker.Unlock();
        mThread.Join();
    }
    void Suspend()
    {
        QCStMutexLocker theLocker(mMutex);

        mSuspendFlag = true;
    }
    void Resume()
    {
        QCStMutexLocker theLocker(mMutex);

        mSuspendFlag = false;
    }
    const Watched& GetStrobed() const
        { return mStrobed; }

private:
    typedef Counters::Counter Counter;
    class WatchedListEntry
    {
    public:
        WatchedListEntry(
            const Watched* inWatchedPtr = 0)
            : mWatchedPtr(inWatchedPtr),
              mValue(),
              mTimeoutCount(0),
              mPollCount(0),
              mTotalTimeoutCount(0),
              mLastChangedTime(),
              mLastReportedTime()
            {}
        void Poll(
            Impl& inOuter)
        {
            uint64_t const theValue = mWatchedPtr->Poll();
            ++mPollCount;
            if (theValue == mValue && 1 < mPollCount) {
                ++mTimeoutCount;
                ++mTotalTimeoutCount;
                ++inOuter.mTimoutCount;
                PollTimedout(inOuter);
            } else {
                mTimeoutCount = 0;
                mLastChangedTime = inOuter.mPollStartTime;
            }
            mValue = theValue;
        }
        const Watched* GetWatchedPtr() const
            { return mWatchedPtr; }
        void GetCounters(
            Counters& outCounters)
        {
            outCounters.mPollCount              = mPollCount;
            outCounters.mTimeoutCount           = mTimeoutCount;
            outCounters.mTotalTimeoutCount      = mTotalTimeoutCount;
            outCounters.mLastChangedTimeAgoUsec =
                microseconds() - mLastChangedTime;
            const size_t theMaxLen = sizeof(outCounters.mName) /
                sizeof(outCounters.mName[0]) - 1;
            strncpy(outCounters.mName, mWatchedPtr->GetName(), theMaxLen);
            outCounters.mName[theMaxLen] = 0;
        }
    private:
        const Watched* mWatchedPtr;
        uint64_t       mValue;
        Counter        mTimeoutCount;
        Counter        mPollCount;
        Counter        mTotalTimeoutCount;
        int64_t        mLastChangedTime;
        int64_t        mLastReportedTime;

        void PollTimedout(
            Impl& inOuter)
        {
            const bool    theFatalFlag = 0 <= inOuter.mMaxTimeoutCount &&
                mTimeoutCount < Counter(inOuter.mMaxTimeoutCount);
            const int64_t theNow       = microseconds();
            if (theFatalFlag) {
                inOuter.FatalPollFailure(mWatchedPtr->GetName());
            } else {
                if (1 < mTotalTimeoutCount && theNow <=
                        mLastReportedTime + inOuter.mTimeoutLogIntervalUsec) {
                    return;
                }
                mLastReportedTime = theNow;
            }
            KFS_LOG_STREAM(theFatalFlag ?
                    MsgLogger::kLogLevelFATAL : MsgLogger::kLogLevelERROR) <<
                mWatchedPtr->GetName() <<
                " " << (void*)mWatchedPtr <<
                ": watchdog timed out"
                " " << mTimeoutCount << " times"
                " total: " << mTotalTimeoutCount <<
                " poll count: " << mPollCount <<
                " last changed: " <<
                    (theNow - mLastChangedTime) * 1e-6 <<
                    " sec. ago"
                " polled:"
                " " << (theNow - inOuter.mPollStartTime) * 1e-6 <<
                    " sec. ago" <<
                " last: " << (theNow - inOuter.mPollEndTime) * 1e-6 <<
                    " sec. ago"
                " total:"
                " poll count: " << inOuter.mPollCount <<
                " timeouts: " << inOuter.mTimoutCount <<
            KFS_LOG_EOM;
            if (! theFatalFlag) {
                return;
            }
            abort();
        }
    };
    friend class WatchedListEntry;
    typedef vector<WatchedListEntry> WatchedList;
    class Strobed : public Watched
    {
    public:
        Strobed(
            volatile uint64_t const& inValue)
            : Watched("Watchdog::Strobed"),
              mValue(inValue)
            {}
        virtual ~Strobed()
            {}
        virtual uint64_t Poll() const
            { return mValue; }
    private:
        volatile uint64_t const& mValue;
    private:
        Strobed(
            const Strobed& inStrobed);
        Strobed& operator=(
            const Strobed& inStrobed);
    };

    enum { kMaxFailurMsgLen = 256 };

    WatchedList   mWatchedList;
    QCMutex       mMutex;
    QCCondVar     mCond;
    Strobed       mStrobed;
    QCThread      mThread;
    int64_t       mPollIntervalUsec;
    int64_t       mMinPollIntervalUsec;
    QCMutex::Time mPollIntervalNanoSec;
    int64_t       mTimeoutLogIntervalUsec;
    int           mMaxTimeoutCount;
    bool          mRunFlag;
    bool          mSuspendFlag;
    int64_t       mPollStartTime;
    int64_t       mPollEndTime;
    uint64_t&     mTimoutCount;
    uint64_t&     mPollCount;
    ssize_t       mFailureWriteRet;
    char          mFailureMsg[kMaxFailurMsgLen + 1];

    static void SetWatchdogPtr(
        const Watched& inWatched,
        Watchdog*      inWatchdogPtr)
        { const_cast<Watched&>(inWatched).mWatchdogPtr = inWatchdogPtr; }
    void FatalPollFailure(
        const char* inNamePtr)
    {
        mFailureMsg[kMaxFailurMsgLen] = 0;
        strncpy(mFailureMsg, inNamePtr, kMaxFailurMsgLen);
        size_t len = strlen(mFailureMsg);
        if (len < size_t(kMaxFailurMsgLen)) {
            strncat(mFailureMsg, ": watchdog timed out\n",
                kMaxFailurMsgLen - len);
            len = strlen(mFailureMsg);
        }
        signal(SIGALRM, SIG_DFL);
        alarm(20);
        mFailureWriteRet = write(2, mFailureMsg, len);
    }
    virtual void Run()
    {
        QCStMutexLocker theLocker(mMutex);

        for (; ;) {
            mCond.Wait(mMutex, mPollIntervalNanoSec);
            if (! mRunFlag) {
                break;
            }
            if (mSuspendFlag) {
                continue;
            }
            const int64_t theNow = microseconds();
            if (theNow < mPollEndTime + mMinPollIntervalUsec) {
                continue;
            }
            mPollStartTime = theNow;
            for (WatchedList::iterator theIt = mWatchedList.begin();
                    mWatchedList.end() != theIt; ++theIt) {
                ++mPollCount;
                theIt->Poll(*this);
            }
            mPollEndTime = microseconds();
        }
    }
    WatchedList::iterator Find(
        const Watched& inWatched)
    {
        WatchedList::iterator theIt = mWatchedList.begin();
        while (mWatchedList.end() != theIt &&
                &inWatched != theIt->GetWatchedPtr()) {
            ++theIt;
        }
        return theIt;
    }
private:
    Impl(
        const Impl& /* inImpl */);
    Impl& operator=(
        const Impl& /* inImpl */);
};

Watchdog::Watchdog(
    bool inMustBeStrobedFlag)
    : mStrobedValue(0),
      mTimeoutCount(0),
      mPollCount(0),
      mImpl(*new Impl(mStrobedValue, mTimeoutCount, mPollCount))
{
    Watchdog::SetMustBeStrobed(inMustBeStrobedFlag);
}

Watchdog::~Watchdog()
{
    delete &mImpl;
}

    void
Watchdog::SetParameters(
    const char*       inPrefixPtr,
    const Properties& inParameters)
{
    mImpl.SetParameters(inPrefixPtr, inParameters);
}

    void
Watchdog::Register(
    const Watchdog::Watched& inWatched)
{
    mImpl.Register(inWatched, *this);
}

    void
Watchdog::Unregister(
    const Watchdog::Watched& inWatched)
{
    mImpl.Unregister(inWatched, *this);
}

    void
Watchdog::SetMustBeStrobed(
    bool inFlag)
{
    if (inFlag) {
        Register(mImpl.GetStrobed());
    } else {
        Unregister(mImpl.GetStrobed());
    }
}

    bool
Watchdog::GetCounters(
    int                 inIndex,
    Watchdog::Counters& outCounters)
{
    return 0 <= inIndex && mImpl.GetCounters(size_t(inIndex), outCounters);
}

    void
Watchdog::Start()
{
    mImpl.Start();
}

    void
Watchdog::Stop()
{
    mImpl.Stop();
}

    void
Watchdog::Suspend()
{
    mImpl.Suspend();
}

    void
Watchdog::Resume()
{
    mImpl.Resume();
}

}
