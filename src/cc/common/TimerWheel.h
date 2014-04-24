//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2014/02/11
// Author: Mike Ovsiannikov.
//
// Copyright 2014 Quantcast Corp.
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
//
// \file TimerWheel.h
// \brief Generic timer wheel implementation.
//
//----------------------------------------------------------------------------

#ifndef TIMER_WHEEL_H
#define TIMER_WHEEL_H

#include <stddef.h>
#include <stdlib.h>
#include <assert.h>

namespace KFS
{

template<
    typename T,
    typename ListT,
    typename TimeT,
    size_t   BucketCntT,
    size_t   TimerResolutionT
>
class TimerWheel
{
public:
    TimerWheel(
        TimeT inNextRunTime)
        : mCurBucket(0),
          mNextRunTime(inNextRunTime),
          mTmpList()
        {}
    void Schedule(
        T&    inEntry,
        TimeT inExpires)
    {
        // Round to the next slot to ensure the expiration time will be less
        // than the current time at the moment of the the slot traversal.
        size_t theIdx = inExpires <= mNextRunTime ?
            size_t(0) :
            ((size_t)(inExpires - mNextRunTime) + TimerResolutionT - 1) /
                TimerResolutionT;
        if (BucketCntT <= theIdx) {
            // Max timeout.
            theIdx = (mCurBucket == 0 ? BucketCntT : mCurBucket) - 1;
        } else if (BucketCntT <= (theIdx = mCurBucket + theIdx)) {
            theIdx -= BucketCntT;
        }
        ListT::Insert(inEntry, mBuckets[theIdx]);
    }
    template<typename FT>
    void Run(
        TimeT inNow,
        FT&   inFunctor)
    {
        if (inNow < mNextRunTime) {
            return;
        }
        size_t theBucketCnt = (size_t)(inNow - mNextRunTime) / TimerResolutionT;
        mNextRunTime += (theBucketCnt + 1) * TimerResolutionT;
        if (BucketCntT <= theBucketCnt) {
            theBucketCnt = BucketCntT - 1;
        }
        do {
            ListT::Insert(mTmpList, mBuckets[mCurBucket]);
            ListT::Remove(mBuckets[mCurBucket]);
            if (BucketCntT <= ++mCurBucket) {
                mCurBucket = 0;
            }
            while (ListT::IsInList(mTmpList)) {
                T& theCur = ListT::GetNext(mTmpList);
                inFunctor(theCur);
                if (&theCur == &ListT::GetNext(mTmpList)) {
                    assert(! "TimerWheel::Run: the element still the list.");
                    abort();
                }
            }
        } while (0 < theBucketCnt--);
    }
    template<typename FT>
    void Apply(
        FT& inFunctor) const
        { ApplySelf(inFunctor, &mTmpList); }
    template<typename FT>
    void Apply(
        FT& inFunctor)
        { ApplySelf(inFunctor, &mTmpList); }
private:
    size_t mCurBucket;
    TimeT  mNextRunTime;
    T      mTmpList;
    T      mBuckets[BucketCntT];

    template<typename FT, typename ET>
    void ApplySelf(
        FT& inFunctor,
        ET* /* inConstQualifiedElementTypePtr */) const
    {
        for (size_t i = 0, k = mCurBucket; i < BucketCntT; i++) {
            ET& theList = mBuckets[k];
            ET* thePtr  = &theList;
            while (&theList != (thePtr = ListT::GetNextPtr(thePtr))) {
                inFunctor(*thePtr);
            }
            if (BucketCntT <= ++k) {
                k = 0;
            }
        }
    }
private:
    TimerWheel(
        const TimerWheel& inTimerWheel);
    TimerWheel& operator=(
        const TimerWheel& inTimerWheel);
};

} // namespace KFS

#endif /* TIMER_WHEEL_H */

