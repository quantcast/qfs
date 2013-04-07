//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2009/06/06
// Author: Mike Ovsiannikov
//
// Copyright 2009-2012 Quantcast Corp.
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
//----------------------------------------------------------------------------

#include <algorithm>

#include "BufferManager.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcdebug.h"
#include "kfsio/NetManager.h"
#include "kfsio/Globals.h"

namespace KFS
{

using libkfsio::globalNetManager;
using std::min;
using std::max;

// Chunk server disk and network io buffer manager implementation.
BufferManager::Client::Client()
    : mManagerPtr(0),
      mByteCount(0),
      mWaitingForByteCount(0),
      mWaitStart(0),
      mOverQuotaWaitingFlag(false)
{
    WaitQueue::Init(*this);
}

    inline void
BufferManager::Client::Reset()
{
    mManagerPtr           = 0;
    mByteCount            = 0;
    mWaitingForByteCount  = 0;
    mWaitStart            = 0;
    mOverQuotaWaitingFlag = false;
}

BufferManager::BufferManager(
    bool inEnabledFlag /* = true */)
    : ITimeout(),
      mTotalCount(0),
      mMaxClientQuota(0),
      mRemainingCount(0),
      mWaitingByteCount(0),
      mOverQuotaWaitingByteCount(0),
      mGetRequestCount(0),
      mPutRequestCount(0),
      mClientsWihtBuffersCount(0),
      mMinBufferCount(0),
      mWaitingCount(0),
      mOverQuotaWaitingCount(0),
      mInitedFlag(false),
      mDiskOverloadedFlag(false),
      mEnabledFlag(inEnabledFlag),
      mWaitingAvgIntervalIdx(0),
      mWaitingAvgExp(0),
      mWaitingAvgUsecsLast(microseconds()),
      mWaitingAvgNext(globalNetManager().Now()),
      mWaitingAvgBytes(0),
      mWaitingAvgCount(0),
      mWaitingAvgUsecs(0),
      mCounters()
{
    WaitQueue::Init(mWaitQueuePtr);
    WaitQueue::Init(mOverQuotaWaitQueuePtr);
    mCounters.Clear();
    BufferManager::SetWaitingAvgInterval(20);
}

BufferManager::~BufferManager()
{
    QCRTASSERT(
        WaitQueue::IsEmpty(mWaitQueuePtr) &&
        WaitQueue::IsEmpty(mOverQuotaWaitQueuePtr)
    );
    globalNetManager().UnRegisterTimeoutHandler(this);
}

    void
BufferManager::Init(
    QCIoBufferPool*          inBufferPoolPtr,
    BufferManager::ByteCount inTotalCount,
    BufferManager::ByteCount inMaxClientQuota,
    int                      inMinBufferCount)
{
    QCRTASSERT(! mInitedFlag);
    mInitedFlag                = true;
    mWaitingCount              = 0;
    mOverQuotaWaitingCount     = 0;
    mWaitingByteCount          = 0;
    mOverQuotaWaitingByteCount = 0;
    mGetRequestCount           = 0;
    mPutRequestCount           = 0;
    mClientsWihtBuffersCount   = 0;
    mBufferPoolPtr             = inBufferPoolPtr;
    mTotalCount                = inTotalCount;
    mRemainingCount            = mTotalCount;
    mMinBufferCount            = inMinBufferCount;
    mMaxClientQuota            = min(mTotalCount, inMaxClientQuota);
    mDiskOverloadedFlag        = false;
    globalNetManager().RegisterTimeoutHandler(this);
}

void
BufferManager::ChangeOverQuotaWait(
    BufferManager::Client& inClient,
    bool                   inFlag)
{
    QCASSERT(IsWaiting(inClient));
    if (inClient.mOverQuotaWaitingFlag == inFlag) {
        return;
    }
    WaitQueue::Remove(
        inClient.mOverQuotaWaitingFlag ? mOverQuotaWaitQueuePtr : mWaitQueuePtr,
        inClient
    );
    if (inClient.mOverQuotaWaitingFlag) {
        mOverQuotaWaitingCount--;
        mOverQuotaWaitingByteCount -= inClient.mWaitingForByteCount;
    } else {
        mWaitingCount--;
        mWaitingByteCount -= inClient.mWaitingForByteCount;
    }
    inClient.mOverQuotaWaitingFlag = inFlag;
    WaitQueue::PushBack(
        inClient.mOverQuotaWaitingFlag ? mOverQuotaWaitQueuePtr : mWaitQueuePtr,
        inClient
    );
    if (inClient.mOverQuotaWaitingFlag) {
        mOverQuotaWaitingCount++;
        mOverQuotaWaitingByteCount += inClient.mWaitingForByteCount;
    } else {
        mWaitingCount++;
        mWaitingByteCount += inClient.mWaitingForByteCount;
    }
}

    bool
BufferManager::Modify(
    BufferManager::Client&   inClient,
    BufferManager::ByteCount inByteCount,
    bool                     inForDiskIoFlag)
{
    if (! mEnabledFlag) {
        return true;
    }
    QCASSERT(inClient.mByteCount >= 0 && inClient.mWaitingForByteCount >= 0);
    QCASSERT(inClient.mManagerPtr ||
        inClient.mWaitingForByteCount + inClient.mByteCount == 0);
    QCASSERT(! inClient.mManagerPtr || inClient.mManagerPtr == this);
    QCASSERT(inClient.IsWaiting() || inClient.mWaitingForByteCount == 0);
    QCASSERT(mRemainingCount + inClient.mByteCount <= mTotalCount);

    const bool theHadBuffersFlag = inClient.mByteCount > 0;
    mRemainingCount += inClient.mByteCount;
    if (inByteCount < 0) {
        mPutRequestCount++;
        inClient.mByteCount += inByteCount;
        if (inClient.mByteCount < 0) {
            inClient.mByteCount = 0;
        }
        mRemainingCount -= inClient.mByteCount;
        if (theHadBuffersFlag && inClient.mByteCount <= 0) {
            mClientsWihtBuffersCount--;
        }
        if (inClient.mOverQuotaWaitingFlag) {
            QCRTASSERT(WaitQueue::IsInList(mOverQuotaWaitQueuePtr, inClient));
            ChangeOverQuotaWait(inClient, IsOverQuota(inClient));
        }
        return true;
    }
    mCounters.mRequestCount++;
    mCounters.mRequestByteCount += inByteCount;
    mGetRequestCount++;
    inClient.mManagerPtr = this;
    const ByteCount theReqByteCount  =
        inClient.mWaitingForByteCount + inClient.mByteCount + inByteCount;
    const bool      theOverQuotaFlag = mMaxClientQuota < theReqByteCount;
    const bool      theGrantedFlag   = ! inClient.IsWaiting() && (
        theReqByteCount <= 0 || (
            (! inForDiskIoFlag || ! mDiskOverloadedFlag) &&
            ! IsLowOnBuffers() &&
            theReqByteCount < mRemainingCount &&
            ! theOverQuotaFlag
        )
    );
    if (theGrantedFlag) {
        inClient.mByteCount = theReqByteCount;
        mRemainingCount -= theReqByteCount;
        mCounters.mRequestGrantedCount++;
        mCounters.mRequestGrantedByteCount += inByteCount;
    } else {
        if (theOverQuotaFlag) {
            mCounters.mOverQuotaRequestDeniedCount++;
            mCounters.mOverQuotaRequestDeniedByteCount += inByteCount;
        } else {
            mCounters.mRequestDeniedCount++;
            mCounters.mRequestDeniedByteCount += inByteCount;
        }
        // If already waiting leave him in the same place in the queue, unless
        // it's over quota.
        if (inClient.IsWaiting()) {
            ChangeOverQuotaWait(inClient, theOverQuotaFlag);
        } else {
            inClient.mWaitStart            = microseconds();
            inClient.mOverQuotaWaitingFlag = theOverQuotaFlag;
            WaitQueue::PushBack(
                theOverQuotaFlag ? mOverQuotaWaitQueuePtr : mWaitQueuePtr,
                inClient
            );
            if (theOverQuotaFlag) {
                mOverQuotaWaitingCount++;
            } else {
                mWaitingCount++;
            }
        }
        if (inClient.mOverQuotaWaitingFlag) {
            mOverQuotaWaitingByteCount += inByteCount;
        } else {
            mWaitingByteCount += inByteCount;
        }
        mRemainingCount -= inClient.mByteCount;
        inClient.mWaitingForByteCount += inByteCount;
    }
    QCASSERT(mRemainingCount >= 0 && mRemainingCount <= mTotalCount);
    QCASSERT(inClient.IsWaiting() || inClient.mWaitingForByteCount == 0);
    if (! theHadBuffersFlag && inClient.mByteCount > 0) {
        mClientsWihtBuffersCount++;
    }
    return theGrantedFlag;
}

    void
BufferManager::Unregister(
    BufferManager::Client& inClient)
{
    if (! inClient.mManagerPtr) {
        return;
    }
    QCRTASSERT(inClient.mManagerPtr == this);
    if (IsWaiting(inClient)) {
        if (inClient.mOverQuotaWaitingFlag) {
            mOverQuotaWaitingCount--;
            mOverQuotaWaitingByteCount -= inClient.mWaitingForByteCount;
        } else {
            mWaitingCount--;
            mWaitingByteCount -= inClient.mWaitingForByteCount;
        }
        WaitQueue::Remove(
            inClient.mOverQuotaWaitingFlag ?
                mOverQuotaWaitQueuePtr : mWaitQueuePtr,
            inClient
        );
    }
    inClient.mWaitingForByteCount = 0;
    Put(inClient, inClient.mByteCount);
    QCASSERT(! inClient.IsWaiting() && inClient.mByteCount == 0);
    inClient.Reset();
}

    void
BufferManager::CancelRequest(
    Client& inClient)
{
    if (! inClient.mManagerPtr) {
        return;
    }
    QCRTASSERT(inClient.mManagerPtr == this);
    if (! IsWaiting(inClient)) {
        QCASSERT(inClient.mWaitingForByteCount == 0);
        return;
    }
    mCounters.mReqeustCanceledCount++;
    mCounters.mReqeustCanceledBytes += inClient.mWaitingForByteCount;
    WaitQueue::Remove(
        inClient.mOverQuotaWaitingFlag ? mOverQuotaWaitQueuePtr : mWaitQueuePtr,
        inClient
    );
    if (inClient.mOverQuotaWaitingFlag) {
        mOverQuotaWaitingCount--;
        mOverQuotaWaitingByteCount -= inClient.mWaitingForByteCount;
    } else {
        mWaitingCount--;
        mWaitingByteCount -= inClient.mWaitingForByteCount;
    }
    inClient.mWaitingForByteCount  = 0;
    inClient.mOverQuotaWaitingFlag = false;
}

    bool
BufferManager::IsLowOnBuffers() const
{
    return (
        mBufferPoolPtr &&
        mBufferPoolPtr->GetFreeBufferCount() < max(
            ByteCount(mMinBufferCount),
            mRemainingCount / mBufferPoolPtr->GetBufferSize() + 1
        )
    );
}

    /* virtual */ void
BufferManager::Timeout()
{
    bool    theSetTimeFlag = true;
    int64_t theNowUsecs    = 0;
    while (! mDiskOverloadedFlag && ! IsLowOnBuffers()) {
        Client* const theClientPtr = WaitQueue::Front(mWaitQueuePtr);
        if (! theClientPtr ||
                theClientPtr->mWaitingForByteCount > mRemainingCount) {
            break;
        }
        WaitQueue::Remove(mWaitQueuePtr, *theClientPtr);
        mWaitingCount--;
        const ByteCount theGrantedCount = theClientPtr->mWaitingForByteCount;
        QCASSERT(theGrantedCount > 0);
        mRemainingCount -= theGrantedCount;
        QCASSERT(mRemainingCount <= mTotalCount);
        mWaitingByteCount -= theGrantedCount;
        if (theClientPtr->mByteCount <= 0 && theGrantedCount > 0) {
            mClientsWihtBuffersCount++;
        }
        if (theSetTimeFlag) {
            theSetTimeFlag = false;
            theNowUsecs    = microseconds();
        }
        mCounters.mRequestWaitUsecs += max(int64_t(0),
            theNowUsecs - theClientPtr->mWaitStart);
        mCounters.mRequestGrantedCount++;
        mCounters.mRequestGrantedByteCount += theGrantedCount;
        theClientPtr->mByteCount += theGrantedCount;
        theClientPtr->mWaitingForByteCount = 0;
        theClientPtr->Granted(theGrantedCount);
    }
    UpdateWaitingAvg();
}

static const int64_t kWaitingAvgExp[] = {
1507,2484,2935,3190,3354,3467,3551,3615,3665,3706,
3740,3769,3793,3814,3832,3848,3862,3875,3886,3896,
3906,3914,3922,3929,3935,3941,3947,3952,3957,3962,
3966,3970,3974,3977,3981,3984,3987,3990,3992,3995,
3997,4000,4002,4004,4006,4008,4010,4012,4013,4015,
4016,4018,4019,4021,4022,4024,4025,4026,4027,4028,
4029,4030,4031,4032,4033,4034,4035,4036,4037,4038,
4039,4040,4040,4041,4042,4042,4043,4044,4044,4045,
4046,4046,4047,4048,4048,4049,4049,4050,4050,4051,
4051,4052,4052,4053,4053,4054,4054,4054,4055,4055,
4056,4056,4056,4057,4057,4058,4058,4058,4059,4059,
4059,4060,4060,4060,4061,4061,4061,4061,4062,4062,
4062,4063,4063,4063,4063,4064,4064,4064,4064,4065,
4065,4065,4065,4066,4066,4066,4066,4066,4067,4067,
4067,4067,4067,4068,4068,4068,4068,4068,4069,4069,
4069,4069,4069,4069,4070,4070,4070,4070,4070,4070,
4071,4071,4071,4071,4071,4071,4072,4072,4072,4072,
4072,4072,4072,4073,4073,4073,4073,4073,4073,4073,
4073,4074,4074,4074,4074,4074,4074,4074,4074,4074,
4075,4075,4075,4075,4075,4075,4075,4075,4075,4076,
4076,4076,4076,4076,4076,4076,4076,4076,4076,4077,
4077,4077,4077,4077,4077,4077,4077,4077,4077,4077,
4078,4078,4078,4078,4078,4078,4078,4078,4078,4078,
4078,4078,4078,4079,4079,4079,4079,4079,4079,4079,
4079,4079,4079,4079,4079,4079,4079,4080,4080,4080,
4080,4080,4080,4080,4080,4080,4080,4080,4080,4080,
4080,4080,4080,4081,4081,4081,4081,4081,4081,4081,
4081,4081,4081,4081,4081,4081,4081,4081,4081,4081,
4081,4082,4082,4082,4082,4082,4082,4082,4082,4082,
4082,4082,4082,4082,4082,4082,4082,4082,4082,4082,
4082,4082,4083,4083,4083,4083,4083,4083,4083,4083,
4083,4083,4083,4083,4083,4083,4083,4083,4083,4083,
4083,4083,4083,4083,4083,4083,4083,4084,4084,4084,
4084,4084,4084,4084,4084,4084,4084,4084,4084,4084,
4084,4084,4084,4084,4084,4084,4084,4084,4084,4084,
4084,4084,4084,4084,4084,4085,4085,4085,4085,4085,
4085,4085,4085,4085,4085,4085,4085,4085,4085,4085,
4085,4085,4085,4085,4085,4085,4085,4085,4085,4085,
4085,4085,4085,4085,4085,4085,4085,4085,4085,4086,
4086,4086,4086,4086,4086,4086,4086,4086,4086,4086,
4086,4086,4086,4086,4086,4086,4086,4086,4086,4086,
4086,4086,4086,4086,4086,4086,4086,4086,4086,4086,
4086,4086,4086,4086,4086,4086,4086,4086,4086,4086,
4087,4087,4087,4087,4087,4087,4087,4087,4087,4087,
4087,4087,4087,4087,4087,4087,4087,4087,4087,4087,
4087,4087,4087,4087,4087,4087,4087,4087,4087,4087,
4087,4087,4087,4087,4087,4087,4087,4087,4087,4087,
4087,4087,4087,4087,4087,4087,4087,4087,4087,4087,
4087,4088,4088,4088,4088,4088,4088,4088,4088,4088,
4088,4088,4088,4088,4088,4088,4088,4088,4088,4088,
4088,4088,4088,4088,4088,4088,4088,4088,4088,4088,
4088,4088,4088,4088,4088,4088,4088,4088,4088,4088,
4088,4088,4088,4088,4088,4088,4088,4088,4088,4088,
4088,4088,4088,4088,4088,4088,4088,4088,4088,4088,
4088,4088,4088,4088,4088,4089,4089,4089,4089,4089,
4089,4089,4089,4089,4089,4089,4089,4089,4089,4089,
4089,4089,4089,4089,4089,4089,4089,4089,4089,4089,
4089,4089,4089,4089,4089,4089,4089,4089,4089,4089,
4089,4089,4089,4089,4089,4089,4089,4089,4089,4089,
4089,4089,4089,4089,4089,4089,4089,4089,4089,4089,
4089,4089,4089,4089,4089,4089,4089,4089,4089,4089,
4089,4089,4089,4089,4089,4089,4089,4089,4089,4089,
4089,4089,4089,4089,4089,4089,4089,4089,4089,4090,
4090,4090,4090,4090,4090,4090,4090,4090,4090,4090,
4090,4090,4090,4090,4090,4090,4090,4090,4090,4090,
4090,4090,4090,4090,4090,4090,4090,4090,4090,4090,
4090,4090,4090,4090,4090,4090,4090,4090,4090,4090,
4090,4090,4090,4090,4090,4090,4090,4090,4090,4090,
4090,4090,4090,4090,4090,4090,4090,4090,4090,4090,
4090,4090,4090,4090,4090,4090,4090,4090,4090,4090,
4090,4090,4090,4090,4090,4090,4090,4090,4090,4090,
4090,4090,4090,4090,4090,4090,4090,4090,4090,4090,
4090,4090,4090,4090,4090,4090,4090,4090,4090,4090,
4090,4090,4090,4090,4090,4090,4090,4090,4090,4090,
4090,4090,4090,4090,4091,4091,4091,4091,4091,4091,
4091,4091,4091,4091,4091,4091,4091,4091,4091,4091,
4091,4091,4091,4091,4091,4091,4091,4091,4091,4091,
4091,4091,4091,4091,4091,4091,4091,4091,4091,4091,
4091,4091,4091,4091,4091,4091,4091,4091,4091,4091,
4091,4091,4091,4091,4091,4091,4091,4091,4091,4091,
4091,4091,4091,4091,4091,4091,4091,4091,4091,4091,
4091,4091,4091,4091,4091,4091,4091,4091,4091,4091,
4091,4091,4091,4091,4091,4091,4091,4091,4091,4091,
4091,4091,4091,4091,4091,4091,4091,4091,4091,4091,
4091,4091,4091,4091,4091,4091,4091,4091,4091,4091,
4091,4091,4091,4091,4091,4091,4091,4091,4091,4091,
4091,4091,4091,4091,4091,4091,4091,4091,4091,4091,
4091,4091,4091,4091,4091,4091,4091,4091,4091,4091,
4091,4091,4091,4091,4091,4091,4091,4091,4091,4091,
4091,4091,4091,4091,4091,4091,4091,4091,4091,4091,
4091,4091,4091,4091,4091,4091,4091,4091,4091,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4092,
4092,4092,4092,4092,4092,4092,4092,4092,4092,4093,
4093,4093,4093,4093,4093,4093,4093,4093,4093,4093,
4093,4093,4093,4093,4093,4093,4093,4093,4093,4093,
4093,4093,4093,4093,4093,4093,4093,4093,4093,4093
};

// Generate the exp table the above from 1 to 1200 sec average
/*
#!/bin/sh

awk -v mx=1200 -v samp_interval=1 -v frac_bits=12 'BEGIN {
    f1 = lshift(1, frac_bits);
    for(i = samp_interval; i <= mx; i += samp_interval) {
        printf("%.f,%s", f1 / exp(samp_interval / i), \
            i % (10 * samp_interval) == 0 ? "\n" : "");
    }
    exit(0);
}'
*/

void
BufferManager::SetWaitingAvgInterval(
    int inSecs)
{
    mWaitingAvgIntervalIdx = (int)min(
        size_t(max(1, (inSecs + kWaitingAvgSampleIntervalSec - 1) /
            kWaitingAvgSampleIntervalSec)),
        sizeof(kWaitingAvgExp) / sizeof(kWaitingAvgExp[0])) - 1;
    mWaitingAvgExp = kWaitingAvgExp[mWaitingAvgIntervalIdx];
}

int64_t
BufferManager::CalcWaitingAvg(
    int64_t inAvg,
    int64_t inSample) const
{
    // IIR filter
    const int64_t kWaitingAvgFixed_1 = int64_t(1) << kWaitingAvgFracBits;
    return ((
        inAvg * mWaitingAvgExp +
        (inSample << kWaitingAvgFracBits) *
            (kWaitingAvgFixed_1 - mWaitingAvgExp)
    ) >> kWaitingAvgFracBits);
}

void
BufferManager::UpdateWaitingAvg()
{
    const int64_t kWaitingAvgIntervalUsec =
        int64_t(kWaitingAvgSampleIntervalSec) * 1000 * 1000;

    const time_t theNow = globalNetManager().Now();
    if (theNow < mWaitingAvgNext) {
        return;
    }
    const int64_t theNowUsecs  = microseconds();
    const int64_t theEnd       = theNowUsecs - kWaitingAvgIntervalUsec;
    const int64_t theWaitUsecs = WaitQueue::IsEmpty(mWaitQueuePtr) ?
        int64_t(0) : max(int64_t(0),
            theNowUsecs - WaitQueue::Front(mWaitQueuePtr)->mWaitStart);
    while (mWaitingAvgUsecsLast <= theEnd) {
        mWaitingAvgBytes = CalcWaitingAvg(mWaitingAvgBytes, mWaitingByteCount);
        mWaitingAvgCount = CalcWaitingAvg(mWaitingAvgCount, mWaitingCount);
        mWaitingAvgUsecs = CalcWaitingAvg(mWaitingAvgUsecs, theWaitUsecs);
        mWaitingAvgUsecsLast += kWaitingAvgIntervalUsec;
        mWaitingAvgNext      += kWaitingAvgSampleIntervalSec;
    }
}

} /* namespace KFS */
