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

#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

#include <stdint.h>
#include "qcdio/QCDLList.h"
#include "qcdio/QCIoBufferPool.h"
#include "kfsio/ITimeout.h"

namespace KFS
{

// Chunk server disk and network io buffer manager. The intent is "fair" io
// buffer allocation between clients [connections]. The buffer pool size fixed
// at startup. Clients added to the wait queue if not enough buffers available
// to serve the request, and the client request processing suspended. The
// client's request processing resumed once enough buffers become available.
//
// Exponentially decaying average request wait time presently used by the meta
// server as feedback chunk server "load" metric in chunk placement. The load
// metric presently has the most effect for write append chunk placement with
// large number of append clients in radix sort.
class BufferManager : private ITimeout
{
public:
    typedef int64_t ByteCount;
    typedef int64_t RequestCount;
    struct Counters
    {
        typedef int64_t Counter;

        Counter mRequestCount;
        Counter mRequestByteCount;
        Counter mRequestDeniedCount;
        Counter mRequestDeniedByteCount;
        Counter mRequestGrantedCount;
        Counter mRequestGrantedByteCount;
        Counter mReqeustCanceledCount;
        Counter mReqeustCanceledBytes;
        Counter mRequestWaitUsecs;
        Counter mOverQuotaRequestDeniedCount;
        Counter mOverQuotaRequestDeniedByteCount;

        void Clear()
        {
            mRequestCount                    = 0;
            mRequestByteCount                = 0;
            mRequestDeniedCount              = 0;
            mRequestDeniedByteCount          = 0;
            mRequestGrantedCount             = 0;
            mRequestGrantedByteCount         = 0;
            mReqeustCanceledCount            = 0;
            mReqeustCanceledBytes            = 0;
            mRequestWaitUsecs                = 0;
            mOverQuotaRequestDeniedCount     = 0;
            mOverQuotaRequestDeniedByteCount = 0;
        }
    };

    class Client
    {
    public:
        typedef BufferManager::ByteCount ByteCount;

        virtual void Granted(
            ByteCount inByteCount) = 0;
        // virtual int EmeregencyRelease(
        //    ByteCount inByteCount)
        //    { return 0; }
        ByteCount GetByteCount() const
            { return mByteCount; }
        ByteCount GetWaitingForByteCount() const
            { return mWaitingForByteCount; }
        bool IsWaiting() const
            { return (mManagerPtr && mManagerPtr->IsWaiting(*this)); }
        void CancelRequest()
        {
            if (mManagerPtr) {
                mManagerPtr->CancelRequest(*this);
            }
        }
        void Unregister()
        {
            if (mManagerPtr) {
                mManagerPtr->Unregister(*this);
            }
        }
    protected:
        Client();
        virtual ~Client()
            { Client::Unregister(); }
    private:
        Client*        mPrevPtr[1];
        Client*        mNextPtr[1];
        BufferManager* mManagerPtr;
        ByteCount      mByteCount;
        ByteCount      mWaitingForByteCount;
        int64_t        mWaitStart;
        bool           mOverQuotaWaitingFlag;

        inline void Reset();

        friend class BufferManager;
        friend class QCDLListOp<Client, 0>;
    private:
        Client(
            const Client& inClient);
        Client& operator=(
            const Client& inClient);
    };
    BufferManager(
        bool inEnabledFlag);
    ~BufferManager();
    void Init(
        QCIoBufferPool* inBufferPoolPtr,
        ByteCount       inTotalCount,
        ByteCount       inMaxClientQuota,
        int             inMinBufferCount);
    ByteCount GetMaxClientQuota() const
        { return mMaxClientQuota; }
    bool IsOverQuota(
        Client&   inClient,
        ByteCount inByteCount = 0)
    {
        return (mMaxClientQuota <
            inClient.mByteCount + inClient.mWaitingForByteCount + inByteCount);
    }
    bool Get(
        Client&   inClient,
        ByteCount inByteCount,
        bool      inForDiskIoFlag = false)
    {
        return (inByteCount <= 0 ||
            Modify(inClient, inByteCount, inForDiskIoFlag));
    }
    bool Put(
        Client&   inClient,
        ByteCount inByteCount)
        { return (inByteCount <= 0 || Modify(inClient, -inByteCount, false)); }
    bool GetForDiskIo(
        Client&   inClient,
        ByteCount inByteCount)
        { return Get(inClient, inByteCount, true); }
    ByteCount GetTotalCount() const
        { return mTotalCount; }
    bool IsLowOnBuffers() const;
    virtual void Timeout();
    bool IsWaiting(
        const Client& inClient) const
    {
        return (
            WaitQueue::IsInList(mWaitQueuePtr, inClient) ||
            WaitQueue::IsInList(mOverQuotaWaitQueuePtr, inClient)
        );
    }
    void Unregister(
        Client& inClient);
    void CancelRequest(
        Client& inClient);
    ByteCount GetTotalByteCount() const
        { return mTotalCount; }
    ByteCount GetRemainingByteCount() const
        { return mRemainingCount; }
    ByteCount GetUsedByteCount() const
        { return (mTotalCount - mRemainingCount); }
    int GetFreeBufferCount() const
        { return (mBufferPoolPtr ? mBufferPoolPtr->GetFreeBufferCount() : 0); }
    int GetMinBufferCount() const
        { return mMinBufferCount; }
    int GetTotalBufferCount() const
    {
        const int theSize = mBufferPoolPtr ? mBufferPoolPtr->GetBufferSize() : 0;
        return (theSize > 0 ? mTotalCount / theSize : 0);
    }
    int GetWaitingCount() const
        { return mWaitingCount; }
    int GetOverQuotaWaitingCount() const
        { return mOverQuotaWaitingCount; }
    ByteCount GetWaitingByteCount() const
        { return mWaitingByteCount; }
    ByteCount GetOverQuotaWaitingByteCount() const
        { return mOverQuotaWaitingByteCount; }
    RequestCount GetGetRequestCount() const
        { return mGetRequestCount; }
    RequestCount GetPutRequestCount() const
        { return mPutRequestCount; }
    int GetClientsWihtBuffersCount() const
        { return mClientsWihtBuffersCount; }
    void GetCounters(
        Counters& outCounters) const
        { outCounters = mCounters; }
    void SetDiskOverloaded(
        bool inFlag)
        { mDiskOverloadedFlag = inFlag; }
    int64_t GetWaitingAvgBytes() const
        { return (mWaitingAvgBytes >> kWaitingAvgFracBits); }
    int64_t GetWaitingAvgUsecs() const
        { return (mWaitingAvgUsecs >> kWaitingAvgFracBits); }
    int64_t GetWaitingAvgCount() const
        { return (mWaitingAvgCount >> kWaitingAvgFracBits); }
    void SetWaitingAvgInterval(
        int inSecs);
    int GetWaitingAvgInterval() const
    {
        return ((mWaitingAvgIntervalIdx + 1) * kWaitingAvgSampleIntervalSec);
    }
private:
    typedef QCDLList<Client, 0> WaitQueue;
    // 39 bits integer part -- max 0.5TB bytes waiting
    // 24 bits after 12 bits fractional part multiplication -- should be sufficent
    // for 2 sec resolution.
    enum { kWaitingAvgFracBits = 12 };
    enum { kWaitingAvgSampleIntervalSec = 1 };

    Client*         mWaitQueuePtr[1];
    Client*         mOverQuotaWaitQueuePtr[1];
    QCIoBufferPool* mBufferPoolPtr;
    ByteCount       mTotalCount;
    ByteCount       mMaxClientQuota;
    ByteCount       mRemainingCount;
    ByteCount       mWaitingByteCount;
    ByteCount       mOverQuotaWaitingByteCount;
    RequestCount    mGetRequestCount;
    RequestCount    mPutRequestCount;
    int             mClientsWihtBuffersCount;
    int             mMinBufferCount;
    int             mWaitingCount;
    int             mOverQuotaWaitingCount;
    bool            mInitedFlag;
    bool            mDiskOverloadedFlag;
    const bool      mEnabledFlag;
    int             mWaitingAvgIntervalIdx;
    int64_t         mWaitingAvgExp;
    int64_t         mWaitingAvgUsecsLast;
    time_t          mWaitingAvgNext;
    int64_t         mWaitingAvgBytes;
    int64_t         mWaitingAvgCount;
    int64_t         mWaitingAvgUsecs;
    Counters        mCounters;

    bool Modify(
        Client&   inClient,
        ByteCount inByteCount,
        bool      inForDiskIoFlag);
    void UpdateWaitingAvg();
    int64_t CalcWaitingAvg(
        int64_t inAvg,
        int64_t inSample) const;
    void ChangeOverQuotaWait(
        BufferManager::Client& inClient,
        bool                   inFlag);
    BufferManager(
        const BufferManager& inManager);
    BufferManager& operator=(
        const BufferManager& inManager);
};

} /* namespace KFS */
#endif /* BUFFER_MANAGER_H */
