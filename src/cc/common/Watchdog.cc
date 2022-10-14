//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2019/02/09
// Author: Mike Ovsiannikov
//
// Copyright 2019 Quantcast Corporation. All rights reserved.
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
#include <limits>

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
        uint64_t&                inTimeoutCount,
        uint64_t&                inPollCount,
        uint64_t&                inTimerOverrunCount,
        uint64_t&                inTimerOverrunUsecCount,
        uint64_t&                inLastTimerOverrunTime)
        : QCRunnable(),
          mWatchedList(),
          mMutex(),
          mCond(),
          mStrobed(inStrobedValue),
          mThread(this, "Watchdog"),
          mParamPollIntervalUsec(int64_t(1) * 1150 * 1000),
          mPollIntervalUsec(),
          mMinPollIntervalUsec(),
          mPollIntervalNanoSec(),
          mTimeoutLogIntervalUsec(0),
          mTimerOverrunThresholdUsec(1000 * 1000),
          mMaxTimeoutCount(-1),
          mRunFlag(false),
          mSuspendFlag(false),
          mPollStartTime(),
          mPollEndTime(),
          mTimeoutCount(inTimeoutCount),
          mPollCount(inPollCount),
          mTimerOverrunCount(inTimerOverrunCount),
          mTimerOverrunUsecCount(inTimerOverrunUsecCount),
          mLastTimerOverrunTime(inLastTimerOverrunTime),
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
        mMaxTimeoutCount = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("maxTimeoutCount"),
            mMaxTimeoutCount
        );
        // Enforce min 4 seconds interval to protect against accidentally
        // setting the interval too small, and allowing changing the minimum for
        // debugging and testing purposes.
        const double theMinIntervalSec = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("minPollIntervalSec"),
            mMaxTimeoutCount < 0 ? 1.05 :
                max(1.25, 4.0 / max(1, mMaxTimeoutCount))
        );
        const double theInfinity     = std::numeric_limits<double>::infinity();
        const double thePollInterval = inParameters.getValue(
                theName.Truncate(thePrefixLen).Append("pollIntervalSec"),
                theInfinity);
        if (theInfinity == thePollInterval) {
            // The use defaults if not set.
            mParamPollIntervalUsec = theInfinity == thePollInterval ?
                (int64_t)(mMaxTimeoutCount < 0 ? 1150 : 16 * 1000) * 1000 :
                (int64_t)(1e6 * max(theMinIntervalSec, thePollInterval));
        }
        // Enforce min 0.5 second timer overrun threshold.
        const double theMinTimerOverrunThresholdSec = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("minTimerOverrunThresholdSec"),
            double(0.5)
        );
        mTimerOverrunThresholdUsec = (int64_t)(1e6 * max(
            theMinTimerOverrunThresholdSec,
            inParameters.getValue(
                theName.Truncate(thePrefixLen).Append("timerOverrunThresholdSec"),
                mTimerOverrunThresholdUsec * 1e-6
        )));
        mTimeoutLogIntervalUsec = (int64_t)(1e6 * inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("timeoutLogIntervalSec"),
            mTimeoutLogIntervalUsec * 1e-6
        ));
        if (mParamPollIntervalUsec < mPollIntervalUsec && mRunFlag) {
            QCStMutexLocker theLocker(mMutex);
            if (mParamPollIntervalUsec < mPollIntervalUsec) {
                 // Wake up thread to update the interval.
                mCond.Notify();
            }
        }
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
              mLastTimeoutTime()
            {}
        void Poll(
            Impl& inOuter)
        {
            uint64_t const theValue = mWatchedPtr->Poll();
            ++mPollCount;
            if (theValue == mValue && 1 < mPollCount) {
                ++mTimeoutCount;
                ++mTotalTimeoutCount;
                ++inOuter.mTimeoutCount;
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
            const int64_t theNow = microseconds();
            outCounters.mPollCount               = mPollCount;
            outCounters.mTimeoutCount            = mTimeoutCount;
            outCounters.mTotalTimeoutCount       = mTotalTimeoutCount;
            outCounters.mLastChangedTimeAgoUsec  =
                theNow - mLastChangedTime;
            outCounters.mLastTimeoutTimeAgoUsec =
                theNow - mLastTimeoutTime;
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
        int64_t        mLastTimeoutTime;

        void PollTimedout(
            Impl& inOuter)
        {
            const bool    theFatalFlag = 0 <= inOuter.mMaxTimeoutCount &&
                Counter(inOuter.mMaxTimeoutCount) < mTimeoutCount;
            const int64_t theNow       = microseconds();
            if (theFatalFlag) {
                inOuter.FatalPollFailure(mWatchedPtr->GetName());
            } else {
                if (1 < mTotalTimeoutCount && theNow <=
                        mLastTimeoutTime + inOuter.mTimeoutLogIntervalUsec) {
                    return;
                }
                mLastTimeoutTime = theNow;
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
                " poll started:"
                " " << (theNow - inOuter.mPollStartTime) * 1e-6 <<
                    " sec. ago" <<
                " last poll ended:"
                    " " << (theNow - inOuter.mPollEndTime) * 1e-6 <<
                    " sec. ago"
                " total:"
                " poll count: " << inOuter.mPollCount <<
                " timeouts: " << inOuter.mTimeoutCount <<
                " timer overruns: " << inOuter.mTimerOverrunCount <<
                " timer overruns seconds: " <<
                    inOuter.mTimerOverrunUsecCount * 1e-6 <<
            KFS_LOG_EOM;
            if (! theFatalFlag) {
                return;
            }
            MsgLogger::Stop();
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
    int64_t       mParamPollIntervalUsec;
    int64_t       mPollIntervalUsec;
    int64_t       mMinPollIntervalUsec;
    QCMutex::Time mPollIntervalNanoSec;
    int64_t       mTimeoutLogIntervalUsec;
    int64_t       mTimerOverrunThresholdUsec;
    int           mMaxTimeoutCount;
    bool          mRunFlag;
    bool          mSuspendFlag;
    int64_t       mPollStartTime;
    int64_t       mPollEndTime;
    uint64_t&     mTimeoutCount;
    uint64_t&     mPollCount;
    uint64_t&     mTimerOverrunCount;
    uint64_t&     mTimerOverrunUsecCount;
    uint64_t&     mLastTimerOverrunTime;
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
    void UpdatePollInterval()
    {
        const int64_t theParamPollIntervalUsec = mParamPollIntervalUsec;
        if (theParamPollIntervalUsec != mPollIntervalUsec) {
            mPollIntervalUsec    = theParamPollIntervalUsec;
            mMinPollIntervalUsec = mPollIntervalUsec * 3 / 4;
            mPollIntervalNanoSec = QCMutex::Time(mPollIntervalUsec) * 1000;
        }
    }
    virtual void Run()
    {
        QCStMutexLocker theLocker(mMutex);

        UpdatePollInterval();
        for (bool thePollEndFlag = true; ;) {
            UpdatePollInterval();
            const int64_t theStartWait = microseconds();
            if (thePollEndFlag) {
                mPollEndTime   = theStartWait;
                thePollEndFlag = false;
            }
            mCond.Wait(mMutex, mPollIntervalNanoSec);
            if (! mRunFlag) {
                break;
            }
            const int64_t theNow = microseconds();
            if (theStartWait + mPollIntervalUsec + mTimerOverrunThresholdUsec <
                    theNow) {
                const uint64_t theOverrun =
                    theNow - theStartWait - mPollIntervalUsec;
                ++mTimerOverrunCount;
                mLastTimerOverrunTime = theNow;
                mTimerOverrunUsecCount += theOverrun;
                KFS_LOG_STREAM_ERROR <<
                    "watchdog: detected timer overrun"
                    ": " << mTimerOverrunCount <<
                    " of " << theOverrun * 1e-6 << " sec." <<
                KFS_LOG_EOM;
            }
            if (mSuspendFlag) {
                continue;
            }
            if (theNow < mPollEndTime + mMinPollIntervalUsec) {
                continue;
            }
            mPollStartTime = theNow;
            for (WatchedList::iterator theIt = mWatchedList.begin();
                    mWatchedList.end() != theIt; ++theIt) {
                ++mPollCount;
                theIt->Poll(*this);
            }
            thePollEndFlag = true;
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
      mTimerOverrunCount(0),
      mTimerOverrunUsecCount(0),
      mLastTimerOverrunTime(0),
      mImpl(*new Impl(mStrobedValue, mTimeoutCount, mPollCount,
        mTimerOverrunCount, mTimerOverrunUsecCount, mLastTimerOverrunTime))
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
