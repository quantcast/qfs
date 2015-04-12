//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2009/10/10
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

#include "KfsProtocolWorker.h"
#include "KfsOps.h"

#include "kfsio/IOBuffer.h"
#include "kfsio/NetConnection.h"
#include "kfsio/NetManager.h"
#include "kfsio/ITimeout.h"
#include "kfsio/CryptoKeys.h"
#include "kfsio/Globals.h"
#include "common/kfstypes.h"
#include "common/kfsdecls.h"
#include "common/time.h"
#include "common/MsgLogger.h"
#include "common/StdAllocator.h"
#include "common/IntToString.h"
#include "qcdio/QCUtils.h"
#include "qcdio/QCThread.h"
#include "qcdio/QCMutex.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"
#include "qcdio/QCDLList.h"
#include "qcdio/qcdebug.h"

#include "WriteAppender.h"
#include "Writer.h"
#include "Reader.h"
#include "ClientPool.h"

#include <algorithm>
#include <map>
#include <string>
#include <sstream>
#include <cerrno>
#include <limits>

namespace KFS
{
namespace client
{

using std::string;
using std::max;
using std::min;
using std::make_pair;
using std::ostringstream;
using std::pair;
using std::map;
using std::less;
using KFS::libkfsio::globals;

// KFS client side protocol worker thread implementation.
class KfsProtocolWorker::Impl :
    public QCRunnable,
    public ITimeout
{
public:
    typedef KfsNetClient         MetaServer;
    typedef QCDLList<Request, 0> WorkQueue;

    Impl(
        string            inMetaHost,
        int               inMetaPort,
        const Parameters& inParameters)
        : QCRunnable(),
          ITimeout(),
          mNetManager(),
          mMetaServer(
            mNetManager,
            inMetaHost,
            inMetaPort,
            inParameters.mMetaMaxRetryCount,
            inParameters.mMetaTimeSecBetweenRetries,
            inParameters.mMetaOpTimeoutSec,
            inParameters.mMetaIdleTimeoutSec,
            inParameters.mMetaInitialSeqNum > 0 ?
                inParameters.mMetaInitialSeqNum : GetInitalSeqNum(),
            inParameters.mMetaLogPrefixPtr ?
                inParameters.mMetaLogPrefixPtr : "PWM",
            true,    // inResetConnectionOnOpTimeoutFlag
            inParameters.mMaxMetaServerContentLength,
            false,  // inFailAllOpsOnOpTimeoutFlag
            false,  // inMaxOneOutstandingOpFlag
            inParameters.mAuthContextPtr
          ),
          mMetaOpTimeout(mMetaServer.GetOpTimeout()),
          mMetaTimeBetweenRetries(mMetaServer.GetTimeSecBetweenRetries()),
          mMetaMaxRetryCount(mMetaServer.GetMaxRetryCount()),
          mMetaParamsUpdateFlag(false),
          mWorkers(),
          mMaxRetryCount(inParameters.mMaxRetryCount),
          mTimeSecBetweenRetries(inParameters.mTimeSecBetweenRetries),
          mWriteAppendThreshold(
            inParameters.mWriteAppendThreshold),
          mDefaultSpaceReservationSize(inParameters.mDefaultSpaceReservationSize),
          mPreferredAppendSize(inParameters.mPreferredAppendSize),
          mOpTimeoutSec(inParameters.mOpTimeoutSec),
          mIdleTimeoutSec(inParameters.mIdleTimeoutSec),
          mLogPrefixPtr(inParameters.mLogPrefixPtr ?
            inParameters.mLogPrefixPtr : "PW"),
          mPreAllocateFlag(inParameters.mPreAllocateFlag),
          mMaxWriteSize(inParameters.mMaxWriteSize),
          mRandomWriteThreshold(inParameters.mRandomWriteThreshold),
          mMaxReadSize(inParameters.mMaxReadSize),
          mReadLeaseRetryTimeout(inParameters.mReadLeaseRetryTimeout),
          mLeaseWaitTimeout(inParameters.mLeaseWaitTimeout),
          mChunkServerInitialSeqNum(
            inParameters.mChunkServerInitialSeqNum > 0 ?
                inParameters.mChunkServerInitialSeqNum :
                GetInitalSeqNum()),
          mDoNotDeallocate(),
          mStopRequest(),
          mWorker(this, "KfsProtocolWorker"),
          mMutex(),
          mClientPoolPtr(inParameters.mUseClientPoolFlag ?
            new ClientPool(
                mNetManager,
                0,                          // inMaxRetryCount,
                0,                          // inTimeSecBetweenRetries,
                inParameters.mOpTimeoutSec,
                inParameters.mIdleTimeoutSec,
                mChunkServerInitialSeqNum,
                inParameters.mLogPrefixPtr ? inParameters.mLogPrefixPtr : "PWC",
                false,                       // inResetConnectionOnOpTimeoutFlag
                true,                        // inRetryConnectOnlyFlag
                (int)min(
                    int64_t(mMaxReadSize) + (64 << 10),
                    int64_t(std::numeric_limits<int>::max())
                ), // inMaxContentLength
                false,                       // inFailAllOpsOnOpTimeoutFlag
                false,                       // inMaxOneOutstandingOpFlag
                0                            // inAuthContextPtr
            ) : 0
        ),
        mReadStats(),
        mWriteStats(),
        mAppendStats()
    {
        WorkQueue::Init(mWorkQueue);
        FreeSyncRequests::Init(mFreeSyncRequests);
        CleanupList::Init(mCleanupList);
    }
    virtual ~Impl()
        { Impl::Stop(); }
    virtual void Run()
    {
        mNetManager.RegisterTimeoutHandler(this);
        mNetManager.MainLoop();
        mNetManager.UnRegisterTimeoutHandler(this);
    }
    void Start()
    {
        if (mWorker.IsStarted()) {
            return;
        }
        mStopRequest.mState = Request::kStateNone;
        const int kStackSize = 64 << 10;
        mWorker.Start(this, kStackSize);
    }
    void Stop()
    {
        {
            QCStMutexLocker lock(mMutex);
            if (mStopRequest.mState == Request::kStateNone) {
                Enqueue(mStopRequest);
            }
        }
        mWorker.Join();
        {
            QCStMutexLocker lock(mMutex);
            QCRTASSERT(
                mWorkers.empty() &&
                CleanupList::IsEmpty(mCleanupList)
            );
            SyncRequest* theReqPtr;
            while ((theReqPtr =
                    FreeSyncRequests::PopFront(mFreeSyncRequests))) {
                delete theReqPtr;
            }
        }
    }
    virtual void Timeout()
    {
        Request* theWorkQueue[1];
        {
            QCStMutexLocker theLock(mMutex);
            theWorkQueue[0] = mWorkQueue[0];
            WorkQueue::Init(mWorkQueue);
            if (mMetaParamsUpdateFlag) {
                mMetaServer.SetOpTimeoutSec(mMetaOpTimeout);
                mMetaServer.SetTimeSecBetweenRetries(mMetaTimeBetweenRetries);
                mMetaServer.SetMaxRetryCount(mMetaMaxRetryCount);
                mMetaParamsUpdateFlag = false;
            }
        }
        bool theShutdownFlag = false;
        Request* theReqPtr;
        while ((theReqPtr = WorkQueue::PopFront(theWorkQueue))) {
            Request& theReq = *theReqPtr;
            QCRTASSERT(theReq.mState == Request::kStateInFlight);
            theShutdownFlag = theShutdownFlag || &mStopRequest == theReqPtr;
            if (theShutdownFlag) {
                Done(theReq, kErrShutdown);
                continue;
            }
            if (theReq.mRequestType == kRequestTypeMetaOp) {
                MetaRequest(theReq);
                continue;
            }
            if (theReq.mRequestType == kRequestTypeGetStatsOp) {
                StatsRequest(theReq);
                continue;
            }
            WorkerKey const theKey(theReq.mFileInstance, theReq.mFileId);
            Workers::iterator theIt;
            if (NeedNewWorker(theReq)) {
                theIt = mWorkers.insert(
                    make_pair(theKey, (Worker*)0)).first;
                if (! theIt->second && ! NewWorker(theReq, theIt)) {
                    continue;
                }
            } else {
                theIt = mWorkers.find(theKey);
                if (theIt == mWorkers.end()) {
                    Done(
                        theReq,
                        theReq.mRequestType == kRequestTypeWriteAppendShutdown ?
                            kErrNone : kErrProtocol
                    );
                    continue;
                }
            }
            QCASSERT(IsValid(theReq));
            theIt->second->Process(theReq);
        }
        while (! CleanupList::IsEmpty(mCleanupList)) {
            delete CleanupList::Front(mCleanupList);
        }
        if (theShutdownFlag) {
            Workers theWorkers;
            theWorkers.swap(mWorkers);
            for (Workers::iterator theIt = theWorkers.begin();
                    theIt != theWorkers.end();
                    ) {
                delete theIt++->second;
            }
            QCASSERT(mWorkers.empty());
            mNetManager.Shutdown();
        }
    }
    int64_t Execute(
        RequestType            inRequestType,
        FileInstance           inFileInstance,
        FileId                 inFileId,
        const Request::Params* inParamsPtr,
        void*                  inBufferPtr,
        int                    inSize,
        int                    inMaxPending,
        int64_t                inOffset)
    {
        if (IsSync(inRequestType)) {
            SyncRequest& theReq = GetSyncRequest(
                inRequestType,
                inFileInstance,
                inFileId,
                inParamsPtr,
                inBufferPtr,
                inSize,
                inMaxPending,
                inOffset
            );
            const int64_t theRet = theReq.Execute(*this);
            PutSyncRequest(theReq);
            return theRet;
        }
        return Enqueue(AsyncRequest::Create(
            inRequestType,
            inFileInstance,
            inFileId,
            inParamsPtr,
            inBufferPtr,
            inSize,
            inMaxPending,
            inOffset
        ));
    }
    int64_t Enqueue(
        Request& inRequest)
    {
        if (! IsValid(inRequest)) {
            inRequest.mState = Request::kStateDone;
            inRequest.Done(kErrParameters);
            return kErrParameters;
        }
        if (! IsSync(inRequest.mRequestType) && inRequest.mSize <= 0) {
            inRequest.mState = Request::kStateDone;
            inRequest.Done(kErrNone);
            return kErrParameters;
        }
        {
            QCStMutexLocker theLock(mMutex);
            if (mStopRequest.mState != Request::kStateNone) {
                theLock.Unlock();
                Done(inRequest, kErrShutdown);
                return kErrShutdown;
            }
            QCRTASSERT(inRequest.mState != Request::kStateInFlight);
            inRequest.mState = Request::kStateInFlight;
            WorkQueue::PushBack(mWorkQueue, inRequest);
        }
        mNetManager.Wakeup();
        return 0;
    }
    static bool IsAsync(
        RequestType inRequestType)
    {
        return (
            inRequestType == kRequestTypeWriteAppendAsync ||
            inRequestType == kRequestTypeWriteAppendAsyncNoCopy ||
            inRequestType == kRequestTypeWriteAsync ||
            inRequestType == kRequestTypeWriteAsyncNoCopy ||
            inRequestType == kRequestTypeReadAsync
        );
    }
    static bool IsSync(
        RequestType inRequestType)
        { return (! IsAsync(inRequestType)); }
    static bool IsSync(
        const Request& inRequest)
        { return (IsSync(inRequest.mRequestType)); }
    static bool IsAppend(
        const Request& inRequest)
    {
        switch (inRequest.mRequestType) {
            case kRequestTypeWriteAppend:
            case kRequestTypeWriteAppendThrottle:
                return true;
            case kRequestTypeWriteAppendClose:
                return (inRequest.mSize > 0);
            case kRequestTypeWriteAppendAsync:
            case kRequestTypeWriteAppendAsyncNoCopy:
                return (inRequest.mSize > 0 ||
                    inRequest.mMaxPendingOrEndPos >= 0);
            default:
                break;
        }
        return false;
    }
    static bool IsWrite(
        const Request& inRequest)
    {
        switch (inRequest.mRequestType) {
            case kRequestTypeWrite:
            case kRequestTypeWriteThrottle:
                return true;
            case kRequestTypeWriteClose:
                return (inRequest.mSize > 0);
            case kRequestTypeWriteAsync:
            case kRequestTypeWriteAsyncNoCopy:
                return (inRequest.mSize > 0 ||
                    inRequest.mMaxPendingOrEndPos >= 0);
            default:
                break;
        }
        return false;
    }
    static bool IsRead(
        const Request& inRequest)
    {
        switch (inRequest.mRequestType) {
            case kRequestTypeRead:
            case kRequestTypeReadAsync:
                return true;
            case kRequestTypeReadClose:
                return (inRequest.mSize > 0);
            default:
                break;
        }
        return false;
    }
    static bool NeedNewWorker(
        const Request& inRequest)
    {
        return (
            IsAppend(inRequest) ||
            IsWrite(inRequest) ||
            IsRead(inRequest)
        );
    }
    static bool IsValid(
        const Request& inRequest)
    {
        if (inRequest.mFileId <= 0) {
            return false;
        }
        switch (inRequest.mRequestType) {
            case kRequestTypeWriteAppend:
            case kRequestTypeWriteAppendClose:
            case kRequestTypeWriteAppendAsync:
            case kRequestTypeWriteAppendAsyncNoCopy:
            case kRequestTypeWriteAppendThrottle:
                return (inRequest.mBufferPtr || inRequest.mSize <= 0);
                return (inRequest.mSize == 0);
            case kRequestTypeWriteAppendSetWriteThreshold:
                return true;

            case kRequestTypeWrite:
            case kRequestTypeWriteClose:
            case kRequestTypeWriteAsync:
            case kRequestTypeWriteAsyncNoCopy:
            case kRequestTypeWriteThrottle:
            case kRequestTypeRead:
            case kRequestTypeReadAsync:
            case kRequestTypeReadClose:
                return (
                    (inRequest.mBufferPtr || inRequest.mSize <= 0) &&
                    (inRequest.mOffset >= 0 || inRequest.mSize <= 0)
                );
            case kRequestTypeWriteAppendShutdown:
            case kRequestTypeWriteShutdown:
            case kRequestTypeReadShutdown:
                return (inRequest.mSize == 0);
            case kRequestTypeWriteSetWriteThreshold:
                return true;
            case kRequestTypeMetaOp:
            case kRequestTypeGetStatsOp:
                return (inRequest.mBufferPtr != 0);

            default:
                break;
        }
        return false;
    }
    void SetMetaMaxRetryCount(
        int inMaxRetryCount)
    {
        QCStMutexLocker theLock(mMutex);
        if (mMetaMaxRetryCount != inMaxRetryCount) {
            mMetaMaxRetryCount = inMaxRetryCount;
            mMetaParamsUpdateFlag = true;
            mNetManager.Wakeup();
        }
    }
    void SetMetaTimeSecBetweenRetries(
        int inSecs)
    {
        QCStMutexLocker theLock(mMutex);
        if (mMetaTimeBetweenRetries != inSecs) {
            mMetaTimeBetweenRetries = inSecs;
            mMetaParamsUpdateFlag = true;
            mNetManager.Wakeup();
        }
    }
    void SetMaxRetryCount(
        int inMaxRetryCount)
    {
        QCStMutexLocker theLock(mMutex);
        mMaxRetryCount = inMaxRetryCount;
    }
    void SetTimeSecBetweenRetries(
        int inSecs)
    {
        QCStMutexLocker theLock(mMutex);
        mTimeSecBetweenRetries = inSecs;
    }
    void SetMetaOpTimeoutSec(
        int inSecs)
    {
        QCStMutexLocker theLock(mMutex);
        if (mMetaOpTimeout != inSecs) {
            mMetaOpTimeout = inSecs;
            mMetaParamsUpdateFlag = true;
            mNetManager.Wakeup();
        }
    }
    void SetOpTimeoutSec(
        int inSecs)
    {
        QCStMutexLocker theLock(mMutex);
        mOpTimeoutSec = inSecs;
    }
private:
    class StopRequest : public Request
    {
    public:
        StopRequest()
            : Request(kRequestTypeWriteAppendClose, 0, 1)
            {}
        virtual void Done(
            int64_t Status)
            {}
    };
    class AsyncRequest : public Request
    {
    public:
        static AsyncRequest& Create(
            RequestType   inRequestType,
            FileInstance  inFileInstance,
            FileId        inFileId,
            const Params* inParamsPtr,
            void*         inBufferPtr,
            int           inSize,
            int           inMaxPending,
            int64_t       inOffset)
        {
            QCASSERT(IsAsync(inRequestType));
            const bool theCopyFlag = inSize > 0 && ! (
                inRequestType == kRequestTypeWriteAppendAsyncNoCopy ||
                inRequestType == kRequestTypeWriteAsyncNoCopy ||
                inRequestType == kRequestTypeReadAsync
            );
            const size_t       theReqHdr   = sizeof(AsyncRequest) +
                (inParamsPtr ? sizeof(*inParamsPtr) : size_t(0));
            const unsigned int kAlign      = 16u;
            const unsigned int theBufAlign = (theCopyFlag && inOffset > 0) ?
                (unsigned int)(inOffset % kAlign) : 0u;
            char* const        theAllocPtr = new char[
                theReqHdr +
                (theCopyFlag ? (inSize + kAlign + theBufAlign) : 0)
            ];
            void* theBufferPtr;
            if (theCopyFlag) {
                const char* const  kNullPtr    = 0;
                char* const        thePtr      = theAllocPtr + theReqHdr;
                const unsigned int thePtrAlign =
                    (unsigned int)(thePtr - kNullPtr) % kAlign;
                theBufferPtr = thePtr + (0 < thePtrAlign ?
                    (kAlign - thePtrAlign) : 0) + theBufAlign;
                memcpy(theBufferPtr, inBufferPtr, inSize);
            } else {
                theBufferPtr = inBufferPtr;
            }
            AsyncRequest* const theRetPtr = new(theAllocPtr) AsyncRequest(
                inRequestType,
                inFileInstance,
                inFileId,
                inParamsPtr ?
                    new(theAllocPtr + sizeof(AsyncRequest)) Params(*inParamsPtr)
                    : 0,
                theBufferPtr,
                inSize,
                inMaxPending,
                inOffset
            );
            QCASSERT(reinterpret_cast<char*>(theRetPtr) == theAllocPtr);
            return *theRetPtr;
        }
        virtual void Done(
            int64_t /* inStatus */)
        {
            if (mParamsPtr) {
                const Params* const theParamsPtr = mParamsPtr;
                mParamsPtr = 0;
                theParamsPtr->~Params();
            }
            this->~AsyncRequest();
            delete [] reinterpret_cast<char*>(this);
        }
    private:
        AsyncRequest(
            RequestType   inRequestType,
            FileInstance  inFileInstance,
            FileId        inFileId,
            const Params* inParamsPtr,
            void*         inBufferPtr,
            int           inSize,
            int           inMaxPending,
            int64_t       inOffset)
            : Request(
                inRequestType,
                inFileInstance,
                inFileId,
                inParamsPtr,
                inBufferPtr,
                inSize,
                inMaxPending,
                inOffset)
            {}
        virtual ~AsyncRequest()
            {}
    private:
        AsyncRequest(
            const AsyncRequest& inReq);
        AsyncRequest& operator=(
            const AsyncRequest& inReq);
    };
    class SyncRequest :
        public Request,
        public KfsNetClient::OpOwner
    {
    public:
        SyncRequest(
            RequestType   inRequestType  = kRequestTypeUnknown,
            FileInstance  inFileInstance = 0,
            FileId        inFileId       = -1,
            const Params* inParamsPtr    = 0,
            void*         inBufferPtr    = 0,
            int           inSize         = 0,
            int           inMaxPending   = -1,
            int64_t       inOffset       = -1)
            : Request(
                inRequestType,
                inFileInstance,
                inFileId,
                inParamsPtr,
                inBufferPtr,
                inSize,
                inMaxPending,
                inOffset),
              KfsNetClient::OpOwner(),
              mMutex(),
              mCond(),
              mRetStatus(0),
              mWaitingFlag(false)
            { FreeSyncRequests::Init(*this); }
        SyncRequest& Reset(
            RequestType   inRequestType  = kRequestTypeUnknown,
            FileInstance  inFileInstance = 0,
            FileId        inFileId       = -1,
            const Params* inParamsPtr    = 0,
            void*         inBufferPtr    = 0,
            int           inSize         = 0,
            int           inMaxPending   = -1,
            int64_t       inOffset       = -1)
        {
            QCRTASSERT(! mWaitingFlag);
            Request::Reset(
                inRequestType,
                inFileInstance,
                inFileId,
                inParamsPtr,
                inBufferPtr,
                inSize,
                inMaxPending,
                inOffset
            );
            mRetStatus   = 0;
            mWaitingFlag = 0;
            return *this;
        }
        virtual ~SyncRequest()
            { QCRTASSERT(! mWaitingFlag); }
        virtual void Done(
            int64_t inStatus)
        {
            QCStMutexLocker theLock(mMutex);
            mRetStatus   = inStatus;
            mWaitingFlag = false;
            mCond.Notify();
        }
        int64_t Execute(
            Impl& inWorker)
        {
            mWaitingFlag = true;
            inWorker.Enqueue(*this);
            QCStMutexLocker theLock(mMutex);
            while (mWaitingFlag && mCond.Wait(mMutex))
                {}
            return mRetStatus;
        }
        virtual void OpDone(
            KfsOp*    inOpPtr,
            bool      inCanceledFlag,
            IOBuffer* inBufferPtr)
        {
            QCRTASSERT(inOpPtr && ! inBufferPtr && inOpPtr == mBufferPtr);
            if (inCanceledFlag && inOpPtr->status == 0) {
                inOpPtr->status    = -ECANCELED;
                inOpPtr->statusMsg = "canceled";
            }
            Impl::Done(*this, 0);
        }
    private:
        QCMutex      mMutex;
        QCCondVar    mCond;
        int64_t      mRetStatus;
        bool         mWaitingFlag;
        SyncRequest* mPrevPtr[1];
        SyncRequest* mNextPtr[1];
        friend class QCDLListOp<SyncRequest, 0>;
    private:
        SyncRequest(
            const SyncRequest& inReq);
        SyncRequest& operator=(
            const SyncRequest& inReq);
    };
    void MetaRequest(
        Request& inRequest)
    {
        KfsOp* const theOpPtr = reinterpret_cast<KfsOp*>(inRequest.mBufferPtr);
        if (! theOpPtr) {
            Done(inRequest, kErrParameters);
            return;
        }
        if (! mMetaServer.Enqueue(
                theOpPtr, static_cast<SyncRequest*>(&inRequest))) {
            theOpPtr->status    = kErrParameters;
            theOpPtr->statusMsg = "failed to enqueue op";
        }
    }
    void StatsRequest(
        Request& inRequest)
    {
        Properties* const theStatsPtr =
            reinterpret_cast<Properties*>(inRequest.mBufferPtr);
        if (! theStatsPtr) {
            Done(inRequest, kErrParameters);
            return;
        }
        Properties theStats = GetStats();
        theStatsPtr->swap(theStats);
        Done(inRequest, 0);
    }
    typedef QCDLList<SyncRequest, 0> FreeSyncRequests;
    class DoNotDeallocate : public libkfsio::IOBufferAllocator
    {
    public:
        DoNotDeallocate()
            : mCurBufSize(0)
            {}
        virtual size_t GetBufferSize() const
            { return mCurBufSize; }
        virtual char* Allocate()
        {
            QCRTASSERT(! "unexpected invocation");
            return 0;
        }
        virtual void Deallocate(
            char* /* inBufferPtr */)
            {}
        libkfsio::IOBufferAllocator& Get(
            size_t inBufSize)
        {
            mCurBufSize = inBufSize;
            return *this;
        }
    private:
        size_t mCurBufSize;
    };
    class RequestDeallocator
    {
    public:
        RequestDeallocator(
            Request& inRequest)
            : mRequest(inRequest)
            { QCRTASSERT(inRequest.mSize > 0); }
        void operator()(
            char* inBufferPtr)
        {
            QCRTASSERT(inBufferPtr == mRequest.mBufferPtr);
            Impl::Done(mRequest);
        }
    private:
        Request& mRequest;
    };
    friend class RequestDeallocator;

    class Worker;
    typedef QCDLList<Worker, 0> CleanupList;
    typedef pair<FileInstance, FileId> WorkerKey;
    typedef map<
        WorkerKey,
        Worker*,
        less<WorkerKey>,
        StdFastAllocator<
            pair<const WorkerKey, Worker*>
        >
    > Workers;
    class Worker
    {
    public:
        template <typename T>
        class Stats
        {
        public:
            typename T::Stats   mStats;
            KfsNetClient::Stats mCSStats;
            void Clear()
            {
                mStats.Clear();
                mCSStats.Clear();
            }
            void Add(
                const Stats& inStats)
            {
                mStats.Add(inStats.mStats);
                mCSStats.Add(inStats.mCSStats);
            }
        };
        typedef KfsProtocolWorker::Impl Owner;
        Worker(
            Owner&            inOwner,
            Workers::iterator inWorkersIt)
            : mOwner(inOwner),
              mWorkersIt(inWorkersIt),
              mDeleteScheduledFlag(false)
            {  CleanupList::Init(*this); }
        virtual ~Worker()
            { CleanupList::Remove(mOwner.mCleanupList, *this); }
        virtual int Open(
            FileId                 inFileId,
            const Request::Params& inParams) = 0;
        virtual void Process(
            Request& inRequest) = 0;
        bool IsDeleteScheduled() const
            { return mDeleteScheduledFlag; }
        virtual void AddStats() const = 0;
    protected:
        Owner& mOwner;

        void ScheduleDelete()
        {
            if (mDeleteScheduledFlag) {
                return;
            }
            mDeleteScheduledFlag = true;
            mOwner.mWorkers.erase(mWorkersIt);
            CleanupList::PushBack(mOwner.mCleanupList, *this);
        }

    private:
        Worker*                 mPrevPtr[1];
        Worker*                 mNextPtr[1];
        const Workers::iterator mWorkersIt;
        bool                    mDeleteScheduledFlag;
        friend class QCDLListOp<Worker, 0>;

    private:
        Worker(
            const Worker& inWorker);
        Worker& operator=(
            const Worker& inWorker);
    };
    friend class Worker;

    class Appender : public Worker, public WriteAppender::Completion
    {
        enum { kNoBufferCompaction = -1 };
    public:
        typedef Worker::Stats<WriteAppender> Stats;

        Appender(
            Owner&            inOwner,
            Workers::iterator inWorkersIt,
            const char*       inLogPrefixPtr)
            : Worker(inOwner, inWorkersIt),
              WriteAppender::Completion(),
              mWAppender(
                inOwner.mMetaServer,
                this,
                inOwner.mMaxRetryCount,
                inOwner.mWriteAppendThreshold,
                inOwner.mTimeSecBetweenRetries,
                inOwner.mDefaultSpaceReservationSize,
                inOwner.mPreferredAppendSize,
                kNoBufferCompaction, // no buffer compaction
                inOwner.mOpTimeoutSec,
                inOwner.mIdleTimeoutSec,
                inLogPrefixPtr,
                inOwner.mChunkServerInitialSeqNum,
                inOwner.mPreAllocateFlag
              ),
              mWriteThreshold(inOwner.mWriteAppendThreshold),
              mPending(0),
              mCurPos(0),
              mDonePos(0),
              mLastSyncReqPtr(0),
              mCloseReqPtr(0)
            { WorkQueue::Init(mWorkQueue); }
        virtual ~Appender()
        {
            mWAppender.Shutdown();
            QCASSERT(! mWAppender.IsActive());
            const int64_t thePending = mPending;
            mPending = 0;
            Appender::Done(thePending, kErrShutdown);
            QCRTASSERT(WorkQueue::IsEmpty(mWorkQueue));
            mWAppender.Unregister(this);
            mOwner.AddTotalStats(*this);
        }
        void GetStats(
            Stats& outStats) const
            { mWAppender.GetStats(outStats.mStats, outStats.mCSStats); }
        virtual void AddStats() const
            { mOwner.AddStats(*this); }
        virtual int Open(
            FileId                 inFileId,
            const Request::Params& inParams)
            { return mWAppender.Open(inFileId, inParams.mPathName.c_str()); }
        virtual void Done(
            WriteAppender& inAppender,
            int            inStatusCode)
        {
            const int theRem = inStatusCode == 0 ?
                mWAppender.GetPendingSize() : 0;
            QCRTASSERT(&inAppender == &mWAppender && theRem <= mPending);
            const int theDone = mPending - theRem;
            mPending = theRem;
            Done(theDone, inStatusCode);
        }
        virtual void Process(
            Request& inRequest)
        {
            const bool theShutdownFlag =
                inRequest.mRequestType == kRequestTypeWriteAppendShutdown;
            if (theShutdownFlag) {
                mWAppender.Shutdown();
            } else if (inRequest.mRequestType ==
                    kRequestTypeWriteAppendSetWriteThreshold) {
                mWriteThreshold = inRequest.mSize;
                Impl::Done(inRequest, mWAppender.SetWriteThreshold(
                    mLastSyncReqPtr != 0 ? 0 : mWriteThreshold));
            } else {
                QCRTASSERT(
                    (IsAppend(inRequest) ||
                    inRequest.mRequestType == kRequestTypeWriteAppendClose
                    ) &&
                    inRequest.mState == Request::kStateInFlight &&
                    (inRequest.mBufferPtr || inRequest.mSize <= 0)
                );
                const bool theCloseFlag =
                    inRequest.mRequestType == kRequestTypeWriteAppendClose;
                const bool theFlushFlag =
                    ! theCloseFlag &&
                    inRequest.mRequestType != kRequestTypeWriteAppendAsync &&
                    (inRequest.mRequestType != kRequestTypeWriteAppendThrottle ||
                        (inRequest.mMaxPendingOrEndPos >= 0 &&
                        inRequest.mMaxPendingOrEndPos <
                            mPending + max(0, inRequest.mSize)));
                if (theFlushFlag) {
                    mLastSyncReqPtr = &inRequest;
                }
                if (inRequest.mSize <= 0) {
                    inRequest.mMaxPendingOrEndPos = mCurPos;
                    if (theCloseFlag) {
                        if (mCloseReqPtr || ! mWAppender.IsOpen()) {
                            if (&inRequest != mCloseReqPtr) {
                                Impl::Done(inRequest, kErrProtocol);
                            }
                        } else {
                            mCloseReqPtr = &inRequest;
                            const int theStatus = mWAppender.Close();
                            if (theStatus != 0 && &inRequest == mCloseReqPtr) {
                                mCloseReqPtr = 0;
                                Impl::Done(inRequest, theStatus);
                            }
                        }
                    }
                    if (theFlushFlag) {
                        if (mPending > 0) {
                            WorkQueue::PushBack(mWorkQueue, inRequest);
                        } else {
                            mLastSyncReqPtr = 0;
                            // SetWriteThreshold() should be effectively a
                            // no op, it is here to get status.
                            Impl::Done(inRequest,
                                mWAppender.SetWriteThreshold(mWriteThreshold));
                        }
                    } else if (inRequest.mRequestType ==
                            kRequestTypeWriteAppendThrottle) {
                        const int theStatus = mWAppender.GetErrorCode();
                        Impl::Done(inRequest,
                            theStatus == 0 ? mPending : theStatus);
                    }
                } else {
                    IOBuffer theBuf;
                    const bool theAsyncThrottle = ! theFlushFlag &&
                        inRequest.mRequestType ==
                            kRequestTypeWriteAppendThrottle;
                    if (theAsyncThrottle) {
                        theBuf.CopyIn(
                            reinterpret_cast<char*>(inRequest.mBufferPtr),
                            inRequest.mSize
                        );
                    } else {
                        theBuf.Append(IOBufferData(
                            reinterpret_cast<char*>(inRequest.mBufferPtr),
                            0,
                            inRequest.mSize,
                            mOwner.mDoNotDeallocate.Get(inRequest.mSize)
                        ));
                        WorkQueue::PushBack(mWorkQueue, inRequest);
                    }
                    mCurPos += inRequest.mSize;
                    inRequest.mMaxPendingOrEndPos = mCurPos;
                    const int theStatus =
                        mWAppender.Append(theBuf, inRequest.mSize);
                    if (theStatus <= 0) {
                        if (! theAsyncThrottle) {
                            WorkQueue::Remove(mWorkQueue, inRequest);
                        }
                        Impl::Done(inRequest, theStatus);
                    } else {
                        QCRTASSERT(theStatus == inRequest.mSize);
                        mPending = mWAppender.GetPendingSize();
                        if (theAsyncThrottle) {
                            // Tell the caller the pending size.
                            Impl::Done(inRequest, mPending);
                        }
                    }
                    if (theCloseFlag) {
                        mWAppender.Close();
                    }
                }
                if (theFlushFlag && mLastSyncReqPtr && mWriteThreshold > 0) {
                    mWAppender.SetWriteThreshold(0);
                }
            }
            if (! mWAppender.IsActive()) {
                const int thePending = mPending;
                mPending = 0;
                Done(thePending, kErrShutdown);
            }
            if (theShutdownFlag) {
                Impl::Done(inRequest, kErrNone);
            }
        }
    private:
        WriteAppender mWAppender;
        int           mWriteThreshold;
        int64_t       mPending;
        int64_t       mCurPos;
        int64_t       mDonePos;
        Request*      mLastSyncReqPtr;
        Request*      mCloseReqPtr;
        Request*      mWorkQueue[1];

        void Done(
            int     inDone,
            int64_t inStatus)
        {
            QCRTASSERT(inDone >= 0 && mDonePos + inDone <= mCurPos);
            mDonePos += inDone;
            const bool theHadSynRequestFlag = mLastSyncReqPtr != 0;
            Request* theReqPtr;
            while ((theReqPtr = WorkQueue::Front(mWorkQueue)) &&
                    theReqPtr->mMaxPendingOrEndPos <= mDonePos) {
                Request& theReq = *theReqPtr;
                QCRTASSERT(theReq.mMaxPendingOrEndPos >= 0);
                if (&theReq == mLastSyncReqPtr) {
                    mLastSyncReqPtr = 0;
                }
                WorkQueue::PopFront(mWorkQueue);
                Impl::Done(theReq, inStatus);
            }
            if (mCloseReqPtr &&
                    WorkQueue::IsEmpty(mWorkQueue) && ! mWAppender.IsActive()) {
                Request& theReq = *mCloseReqPtr;
                mCloseReqPtr = 0;
                ScheduleDelete();
                Impl::Done(theReq, inStatus);
                return;
            }
            if (theHadSynRequestFlag && mWriteThreshold > 0 &&
                    ! mLastSyncReqPtr) {
                mWAppender.SetWriteThreshold(mWriteThreshold);
            }
        }
        Appender(
            const Appender& inAppender);
        Appender& operator=(
            const Appender& inAppender);
    };
    friend class Appender;

    class FileWriter : public Worker, public Writer::Completion
    {
        enum { kNoBufferCompaction = -1 };
    public:
        typedef Writer::Offset        Offset;
        typedef Worker::Stats<Writer> Stats;

        FileWriter(
            Owner&            inOwner,
            Workers::iterator inWorkersIt,
            const char*       inLogPrefixPtr)
            : Worker(inOwner, inWorkersIt),
              Writer::Completion(),
              mWriter(
                inOwner.mMetaServer,
                this,
                inOwner.mMaxRetryCount,
                inOwner.mRandomWriteThreshold,
                kNoBufferCompaction,
                inOwner.mTimeSecBetweenRetries,
                inOwner.mOpTimeoutSec,
                inOwner.mIdleTimeoutSec,
                inOwner.mMaxWriteSize,
                inLogPrefixPtr,
                inOwner.mChunkServerInitialSeqNum
              ),
              mCurRequestPtr(0)
            { WorkQueue::Init(mWorkQueue); }
        virtual ~FileWriter()
        {
            mWriter.Shutdown();
            while (! WorkQueue::IsEmpty(mWorkQueue)) {
                Impl::Done(
                    *WorkQueue::PopFront(mWorkQueue),
                    kErrShutdown
                );
            }
            mWriter.Unregister(this);
            mOwner.AddTotalStats(*this);
        }
        void GetStats(
            Stats& outStats) const
            { mWriter.GetStats(outStats.mStats, outStats.mCSStats); }
        virtual void AddStats() const
            { mOwner.AddStats(*this); }
        virtual int Open(
            FileId                 inFileId,
            const Request::Params& inParams)
        {
            return mWriter.Open(
                inFileId,
                inParams.mPathName.c_str(),
                inParams.mFileSize,
                inParams.mStriperType,
                inParams.mStripeSize,
                inParams.mStripeCount,
                inParams.mRecoveryStripeCount,
                inParams.mReplicaCount
            );
        }
        virtual void Process(
            Request& inRequest)
        {
            if (! mWriter.IsOpen()) {
                Impl::Done(inRequest, kErrProtocol);
                return;
            }
            const int theErrorCode = mWriter.GetErrorCode();
            if (theErrorCode != 0) {
                Impl::Done(inRequest, theErrorCode);
                return;
            }
            switch (inRequest.mRequestType) {
                case kRequestTypeWriteAsync:
                    if (inRequest.mSize <= 0) {
                        Impl::Done(inRequest, 0);
                        return;
                    }
                case kRequestTypeWriteThrottle:
                    if (inRequest.mSize <= 0 &&
                            (inRequest.mMaxPendingOrEndPos < 0 ||
                                mWriter.GetPendingSize() <=
                                    inRequest.mMaxPendingOrEndPos)) {
                        const int theStatus = mWriter.GetErrorCode();
                        Impl::Done(inRequest, theStatus == 0 ?
                            mWriter.GetPendingSize() : int64_t(theStatus));
                        return;
                    }
                case kRequestTypeWriteClose:
                case kRequestTypeWrite:
                    QCRTASSERT(inRequest.mBufferPtr || inRequest.mSize <= 0);
                    if (! WorkQueue::IsEmpty(mWorkQueue)) {
                        // For now allow only one flush, block all writers
                        // (threads) until in flight flush done.
                        WorkQueue::PushBack(mWorkQueue, inRequest);
                        return;
                    }
                    if (inRequest.mSize > 0) {
                        Write(inRequest);
                    } else {
                        const bool theCloseFlag =
                            inRequest.mRequestType == kRequestTypeWriteClose;
                        if (! theCloseFlag && mWriter.GetPendingSize() <= 0) {
                            Impl::Done(inRequest, mWriter.GetErrorCode());
                            return;
                        }
                        mCurRequestPtr = &inRequest;
                        WorkQueue::PushBack(mWorkQueue, inRequest);
                        const int theRet = theCloseFlag ?
                            mWriter.Close() : mWriter.Flush();
                        if (mCurRequestPtr == &inRequest) {
                            mCurRequestPtr = 0;
                            if (theRet < 0) {
                                WorkQueue::Remove(mWorkQueue, inRequest);
                                Impl::Done(inRequest, theRet);
                                return;
                            }
                        }
                    }
                    return;
                case kRequestTypeWriteShutdown:
                    QCASSERT(inRequest.mSize <= 0);
                    mWriter.Shutdown();
                    Impl::Done(inRequest, kErrNone);
                    return;
                case kRequestTypeWriteSetWriteThreshold:
                    Impl::Done(
                        inRequest,
                        mWriter.SetWriteThreshold(inRequest.mSize)
                    );
                    return;
                default:
                    QCRTASSERT(! "unexpected request");
                    return;
            }
        }
        virtual void Done(
            Writer& inWriter,
            int     inStatusCode,
            Offset  /* inOffset */,
            Offset  /* inSize */)
        {
            // See comment in Writer.h about inOffset and inSize for striped
            // files.
            QCRTASSERT(&inWriter == &mWriter);
            if (IsDeleteScheduled()) {
                return;
            }
            Request* theWorkQueue[1];
            theWorkQueue[0] = mWorkQueue[0];
            WorkQueue::Init(mWorkQueue);
            mCurRequestPtr = 0;
            Request* theReqPtr;
            if (inStatusCode != 0) {
                const int theStatus =
                    inStatusCode <= 0 ? inStatusCode : -inStatusCode;
                // Error: fail all pending requests.
                mWriter.Stop();
                while ((theReqPtr = WorkQueue::PopFront(theWorkQueue))) {
                    Impl::Done(*theReqPtr, theStatus);
                }
            }
            if (! mWriter.IsOpen() && ! mWriter.IsActive()) {
                ScheduleDelete();
                while ((theReqPtr = WorkQueue::PopFront(theWorkQueue))) {
                    const int theStatus = mWriter.GetErrorCode();
                    Impl::Done(
                        *theReqPtr,
                        theReqPtr->mRequestType == kRequestTypeWriteClose ?
                            theStatus : kErrProtocol
                    );
                }
                return;
            }
            if (! (theReqPtr = WorkQueue::Front(theWorkQueue))) {
                return;
            }
            // Check in flight request completion.
            Request&      theReq         = *theReqPtr;
            const int64_t thePendingSize = mWriter.GetPendingSize();
            if ((theReq.mRequestType == kRequestTypeWriteThrottle ?
                        (theReq.mMaxPendingOrEndPos >= 0 &&
                            thePendingSize > theReq.mMaxPendingOrEndPos) :
                        (thePendingSize > 0)) ||
                    theReq.mRequestType == kRequestTypeWriteClose) {
                mWorkQueue[0] = theWorkQueue[0];
                return; // Wait for completion.
            }
            WorkQueue::PopFront(theWorkQueue);
            Impl::Done(
                theReq,
                theReq.mRequestType == kRequestTypeWriteThrottle ?
                    thePendingSize : mWriter.GetErrorCode()
            );
            // Process the remaining requests, if any.
            while ((theReqPtr = WorkQueue::PopFront(theWorkQueue))) {
                Process(*theReqPtr);
            }
        }
    private:
        Writer         mWriter;
        const Request* mCurRequestPtr;
        Request*       mWorkQueue[1];

        void Write(
            Request& inRequest)
        {
            QCASSERT(inRequest.mSize > 0 && inRequest.mBufferPtr);

            IOBuffer    theBuf;
            const bool  theThrottleFlag =
                inRequest.mRequestType == kRequestTypeWriteThrottle;
            char* const theBufferPtr    =
                reinterpret_cast<char*>(inRequest.mBufferPtr);
            if (inRequest.mRequestType == kRequestTypeWriteAsync ||
                    inRequest.mRequestType == kRequestTypeWriteAsyncNoCopy ||
                    (theThrottleFlag && (
                        inRequest.mMaxPendingOrEndPos < 0 || (
                        inRequest.mMaxPendingOrEndPos > 0 &&
                        inRequest.mMaxPendingOrEndPos +
                            max(0, mOwner.mRandomWriteThreshold) >
                        max(inRequest.mSize, 0) +
                            mWriter.GetPendingSize())))) {
                if (theThrottleFlag) {
                    theBuf.CopyIn(theBufferPtr, inRequest.mSize);
                } else {
                    theBuf.Append(IOBufferData(
                        IOBufferData::IOBufferBlockPtr(
                            theBufferPtr,
                            RequestDeallocator(inRequest)
                        ),
                        inRequest.mSize,
                        0,
                        inRequest.mSize
                    ));
                }
                // The request might get deleted by write, save the params.
                const int  theSize    = inRequest.mSize;
                const bool kFlushFlag = false;
                const int  theRet     = mWriter.Write(
                    theBuf,
                    theSize,
                    inRequest.mOffset,
                    kFlushFlag,
                    (int)inRequest.mMaxPendingOrEndPos
                );
                QCASSERT(theRet < 0 || theRet == theSize);
                if (theThrottleFlag) {
                    Impl::Done(inRequest, theRet >= 0 ?
                        mWriter.GetPendingSize() : int64_t(theRet));
                }
                return;
            }
            if (theThrottleFlag && inRequest.mMaxPendingOrEndPos > 0) {
                theBuf.CopyIn(theBufferPtr, inRequest.mSize);
            } else {
                theBuf.Append(IOBufferData(
                    theBufferPtr,
                    0,
                    inRequest.mSize,
                    mOwner.mDoNotDeallocate.Get(inRequest.mSize)
                ));
                inRequest.mMaxPendingOrEndPos = 0;
            }
            WorkQueue::PushBack(mWorkQueue, inRequest);
            mCurRequestPtr = &inRequest;
            const bool theCloseFlag =
                inRequest.mRequestType == kRequestTypeWriteClose;
            const bool theFlushFlag = ! theThrottleFlag;
            const int  theRet       = mWriter.Write(
                theBuf,
                inRequest.mSize,
                inRequest.mOffset,
                theFlushFlag,
                theThrottleFlag ? (int)inRequest.mMaxPendingOrEndPos : -1
            );
            QCASSERT(theRet < 0 || theRet == inRequest.mSize);
            if (theRet < 0) {
                if (mCurRequestPtr == &inRequest) {
                    mCurRequestPtr = 0;
                    WorkQueue::Remove(mWorkQueue, inRequest);
                    Impl::Done(inRequest, theRet);
                }
                return;
            }
            mCurRequestPtr = 0;
            if (theCloseFlag) {
                mWriter.Close();
            }
        }
    private:
        FileWriter(
            const FileWriter& inWriter);
        FileWriter& operator=(
            const FileWriter& inWriter);
    };
    friend class FileWriter;

    class FileReader : public Worker, public Reader::Completion
    {
    public:
        typedef Reader::Offset        Offset;
        typedef Reader::RequestId     RequestId;
        typedef Worker::Stats<Reader> Stats;

        FileReader(
            Owner&            inOwner,
            Workers::iterator inWorkersIt,
            const char*       inLogPrefixPtr)
            : Worker(inOwner, inWorkersIt),
              Reader::Completion(),
              mReader(
                inOwner.mMetaServer,
                this,
                inOwner.mMaxRetryCount,
                inOwner.mTimeSecBetweenRetries,
                inOwner.mOpTimeoutSec,
                inOwner.mIdleTimeoutSec,
                inOwner.mMaxReadSize,
                inOwner.mReadLeaseRetryTimeout,
                inOwner.mLeaseWaitTimeout,
                inLogPrefixPtr,
                inOwner.mChunkServerInitialSeqNum,
                inOwner.mClientPoolPtr),
              mCurRequestPtr(0),
              mAsyncReadStatus(0),
              mAsyncReadDoneCount(0)
            { WorkQueue::Init(mWorkQueue); }
        virtual ~FileReader()
        {
            mReader.Shutdown();
            while (! WorkQueue::IsEmpty(mWorkQueue)) {
                Impl::Done(
                    *WorkQueue::PopFront(mWorkQueue),
                    kErrShutdown
                );
            }
            mReader.Unregister(this);
            mOwner.AddTotalStats(*this);
        }
        void GetStats(
            Stats& outStats) const
            { mReader.GetStats(outStats.mStats, outStats.mCSStats); }
        virtual void AddStats() const
            { mOwner.AddStats(*this); }
        virtual int Open(
            FileId                 inFileId,
            const Request::Params& inParams)
        {
            const bool   kUseDefaultBufferAllocatorFlag = false;
            const Offset kRecoverChunkPos               = -1;
            return mReader.Open(
                inFileId,
                inParams.mPathName.c_str(),
                inParams.mFileSize,
                inParams.mStriperType,
                inParams.mStripeSize,
                inParams.mStripeCount,
                inParams.mRecoveryStripeCount,
                inParams.mSkipHolesFlag,
                kUseDefaultBufferAllocatorFlag,
                kRecoverChunkPos,
                inParams.mFailShortReadsFlag
            );
        }
        virtual void Process(
            Request& inRequest)
        {
            if (! mReader.IsOpen()) {
                Impl::Done(inRequest, kErrProtocol);
                return;
            }
            const int theErrorCode = mReader.GetErrorCode();
            if (theErrorCode != 0) {
                Impl::Done(inRequest, theErrorCode);
                return;
            }
            int theStatus = 0;
            switch (inRequest.mRequestType) {
                case kRequestTypeReadAsync:
                case kRequestTypeRead:
                    if (inRequest.mSize <= 0 && WorkQueue::IsEmpty(mWorkQueue)) {
                        Done(inRequest, 0);
                        return;
                    }
                case kRequestTypeReadClose:
                    QCRTASSERT(inRequest.mBufferPtr || inRequest.mSize <= 0);
                    WorkQueue::PushBack(mWorkQueue, inRequest);
                    inRequest.mMaxPendingOrEndPos = -1;
                    inRequest.mStatus = 0;
                    mCurRequestPtr = &inRequest;
                    if (inRequest.mSize > 0) {
                        IOBuffer theBuf;
                        theBuf.Append(IOBufferData(
                            reinterpret_cast<char*>(inRequest.mBufferPtr),
                            0,
                            0,
                            mOwner.mDoNotDeallocate.Get(inRequest.mSize)
                        ));
                        inRequest.mMaxPendingOrEndPos = inRequest.mSize;
                        RequestId theReqId = RequestId();
                        theReqId.mPtr = &inRequest;
                        theStatus = mReader.Read(
                            theBuf,
                            inRequest.mSize,
                            inRequest.mOffset,
                            theReqId
                        );
                    }
                    if (inRequest.mRequestType == kRequestTypeReadClose &&
                            theStatus == 0 && mCurRequestPtr == &inRequest) {
                        theStatus = mReader.Close();
                    }
                    if (mCurRequestPtr == &inRequest) {
                        mCurRequestPtr = 0;
                        if (theStatus != 0) {
                            WorkQueue::Remove(mWorkQueue, inRequest);
                            Done(inRequest, theStatus);
                        }
                    }
                    return;
                case kRequestTypeReadShutdown:
                    QCASSERT(inRequest.mSize <= 0);
                    mReader.Shutdown();
                    Done(inRequest, kErrNone);
                    return;
                default:
                    QCRTASSERT(! "unexpected request");
                    return;
            }
        }
        virtual void Done(
            Reader&   inReader,
            int       inStatusCode,
            Offset    inOffset,
            Offset    inSize,
            IOBuffer* inBufferPtr,
            RequestId inRequestId)
        {
            QCRTASSERT(&inReader == &mReader);
            const int theBufSize =
                inBufferPtr ? inBufferPtr->BytesConsumable() : 0;
            if (inBufferPtr) {
                // De-reference buffer data, as completion can free the buffer.
                // The reference counter will not have effect after this point,
                // as the buffer was allocated by the caller, and
                // no op  / "do not de-allocate" de-allocator is used by the
                // reader to pass the buffer.
                inBufferPtr->Clear();
            }
            if (IsDeleteScheduled()) {
                return;
            }
            mCurRequestPtr = 0;
            Request* theReqPtr = static_cast<Request*>(inRequestId.mPtr);
            if (inStatusCode != 0 && mReader.GetErrorCode() != 0) {
                const int theStatus = mReader.GetErrorCode();
                // Fatal error: fail all pending requests.
                mReader.Stop();
                while ((theReqPtr = WorkQueue::PopFront(mWorkQueue))) {
                    Done(*theReqPtr, theStatus);
                }
            }
            if (theReqPtr && ! WorkQueue::IsEmpty(mWorkQueue)) {
                Request& theReq = *theReqPtr;
                if (theReq.mMaxPendingOrEndPos > 0) {
                    QCRTASSERT(
                        theReq.mMaxPendingOrEndPos >= inSize &&
                        (inSize <= 0 ||
                        (theReq.mOffset <= inOffset &&
                        inOffset + inSize <= theReq.mOffset + theReq.mSize))
                    );
                    theReq.mMaxPendingOrEndPos -= inSize;
                }
                if (inStatusCode != 0) {
                    theReq.mStatus =
                        inStatusCode <= 0 ? inStatusCode : -inStatusCode;
                } else if (inBufferPtr && theReq.mStatus >= 0) {
                    theReq.mStatus += theBufSize;
                    QCRTASSERT(theReq.mStatus <= theReq.mSize);
                }
                if (theReq.mMaxPendingOrEndPos <= 0) {
                    WorkQueue::Remove(mWorkQueue, theReq);
                    Done(theReq, theReq.mStatus);
                }
            }
            // Process completion wait requests, if any.
            while ((theReqPtr = WorkQueue::Front(mWorkQueue)) &&
                    theReqPtr->mSize <= 0 &&
                    (theReqPtr->mRequestType == kRequestTypeRead ||
                    theReqPtr->mRequestType == kRequestTypeReadAsync)) {
                WorkQueue::Remove(mWorkQueue, *theReqPtr);
                Done(*theReqPtr, 0);
            }
            if (! mReader.IsOpen() && ! mReader.IsActive()) {
                ScheduleDelete();
                const int theStatus = mReader.GetErrorCode();
                while ((theReqPtr = WorkQueue::PopFront(mWorkQueue))) {
                    Done(
                        *theReqPtr,
                        theReqPtr->mRequestType == kRequestTypeReadClose ?
                            theStatus : kErrProtocol
                    );
                }
            }
        }
    private:
        Reader   mReader;
        Request* mCurRequestPtr;
        int      mAsyncReadStatus;
        int      mAsyncReadDoneCount;
        Request* mWorkQueue[1];

        void Done(
            Request& inReq,
            int      inStatus)
        {
            int theStatus = inStatus;
            if (inReq.mRequestType == kRequestTypeReadAsync &&
                    inReq.mSize > 0) {
                if (inStatus >= 0) {
                    mAsyncReadStatus += inStatus;
                } else {
                    mAsyncReadStatus = inStatus;
                }
                mAsyncReadDoneCount++;
            } else if (inReq.mSize <= 0 && (
                    inReq.mRequestType == kRequestTypeRead ||
                    inReq.mRequestType == kRequestTypeReadAsync)) {
                theStatus = mAsyncReadStatus;
                mAsyncReadStatus    = 0;
                mAsyncReadDoneCount = 0;
            }
            Impl::Done(inReq, theStatus);
        }

    private:
        FileReader(
            const FileReader& inReader);
        FileReader& operator=(
            const FileReader& inReader);
    };
    friend class FileReader;

    NetManager           mNetManager;
    MetaServer           mMetaServer;
    int                  mMetaOpTimeout;
    int                  mMetaTimeBetweenRetries;
    int                  mMetaMaxRetryCount;
    bool                 mMetaParamsUpdateFlag;
    Workers              mWorkers;
    int                  mMaxRetryCount;
    int                  mTimeSecBetweenRetries;
    const int            mWriteAppendThreshold;
    const int            mDefaultSpaceReservationSize;
    const int            mPreferredAppendSize;
    int                  mOpTimeoutSec;
    const int            mIdleTimeoutSec;
    const char* const    mLogPrefixPtr;
    const bool           mPreAllocateFlag;
    const int            mMaxWriteSize;
    const int            mRandomWriteThreshold;
    const int            mMaxReadSize;
    const int            mReadLeaseRetryTimeout;
    const int            mLeaseWaitTimeout;
    int64_t              mChunkServerInitialSeqNum;
    DoNotDeallocate      mDoNotDeallocate;
    StopRequest          mStopRequest;
    QCThread             mWorker;
    QCMutex              mMutex;
    ClientPool* const    mClientPoolPtr;
    FileReader::Stats    mReadStats;
    FileWriter::Stats    mWriteStats;
    Appender::Stats      mAppendStats;
    FileReader::Stats    mTotalReadStats;
    FileWriter::Stats    mTotalWriteStats;
    Appender::Stats      mTotalAppendStats;
    Request*             mWorkQueue[1];
    SyncRequest*         mFreeSyncRequests[1];
    Worker*              mCleanupList[1];

    static void Done(
        Request& inRequest,
        int      inStatus)
    {
        if (inRequest.mState == Request::kStateDone) {
            return;
        }
        QCRTASSERT(inRequest.mState == Request::kStateInFlight);
        inRequest.mState  = Request::kStateDone;
        inRequest.mStatus = inStatus;
        inRequest.Done(inStatus);
    }
    static void Done(
        Request& inRequest)
        { Done(inRequest, inRequest.mStatus); }
    bool NewWorker(
        Request&           inRequest,
        Workers::iterator& inWorkersIt)
    {
        if (! inRequest.mParamsPtr) {
            return false;
        }
        string theName = inRequest.mParamsPtr->mPathName;
        size_t thePos  = theName.find_last_of('/');
        if (thePos != string::npos && ++thePos < theName.length()) {
            theName = theName.substr(thePos);
        }
        ostringstream theStream;
        theStream <<
            mLogPrefixPtr <<
            " " << inRequest.mParamsPtr->mMsgLogId <<
            "," << inRequest.mFileId <<
            "," << inRequest.mFileInstance <<
            "," << theName
        ;
        const string  theLogPrefix = theStream.str();
        Worker* const theRetPtr    = IsAppend(inRequest) ?
            static_cast<Worker*>(new Appender(
                *this, inWorkersIt, theLogPrefix.c_str())) :
            (IsWrite(inRequest) ?
                static_cast<Worker*>(new FileWriter(
                    *this, inWorkersIt, theLogPrefix.c_str())) :
            (IsRead(inRequest) ?
                static_cast<Worker*>(new FileReader(
                    *this, inWorkersIt, theLogPrefix.c_str())) :
                0
        ));
        QCRTASSERT(theRetPtr);
        const int theStatus = theRetPtr->Open(
            inRequest.mFileId, *inRequest.mParamsPtr);
        if (theStatus != kErrNone) {
            mWorkers.erase(inWorkersIt);
            delete theRetPtr;
            Done(inRequest, theStatus);
            return false;
        }
        inWorkersIt->second = theRetPtr;
        mChunkServerInitialSeqNum += 100000;
        return true;
    }
    SyncRequest& GetSyncRequest(
        RequestType            inRequestType,
        FileInstance           inFileInstance,
        FileId                 inFileId,
        const Request::Params* inParamsPtr,
        void*                  inBufferPtr,
        int                    inSize,
        int                    inMaxPending,
        int64_t                inOffset)
    {
        QCStMutexLocker lock(mMutex);
        SyncRequest* theReqPtr = FreeSyncRequests::PopFront(mFreeSyncRequests);
        return (theReqPtr ? theReqPtr->Reset(
            inRequestType,
            inFileInstance,
            inFileId,
            inParamsPtr,
            inBufferPtr,
            inSize,
            inMaxPending,
            inOffset
        ) : *(new SyncRequest(
            inRequestType,
            inFileInstance,
            inFileId,
            inParamsPtr,
            inBufferPtr,
            inSize,
            inMaxPending,
            inOffset
        )));
    }
    void PutSyncRequest(
        SyncRequest& inRequest)
    {
        QCStMutexLocker lock(mMutex);
        FreeSyncRequests::PushFront(mFreeSyncRequests, inRequest);
    }
    static int64_t GetInitalSeqNum()
    {
        int64_t theRet = 0;
        CryptoKeys::PseudoRand(&theRet, sizeof(theRet));
        return ((theRet < 0 ? -theRet : theRet) >> 1);
    }
    template<typename T>
    void AddTotalStats(
        const T& inWorker)
    {
        AddStats(inWorker, true);
    }
    template<typename T>
    void AddStats(
        const T& inWorker,
        bool     inAddTotalsFlag = false)
    {
        typename T::Stats theStats;
        inWorker.GetStats(theStats);
        AddStats(theStats, inAddTotalsFlag);
    }
    void AddStats(
        FileReader::Stats& inStats,
        bool               inAddTotalsFlag)
    {
        (inAddTotalsFlag ? mTotalReadStats : mReadStats).Add(inStats);
    }
    void AddStats(
        FileWriter::Stats& inStats,
        bool               inAddTotalsFlag)
    {
        (inAddTotalsFlag ? mTotalWriteStats : mWriteStats).Add(inStats);
    }
    void AddStats(
        Appender::Stats& inStats,
        bool             inAddTotalsFlag)
    {
        (inAddTotalsFlag ? mTotalAppendStats : mAppendStats).Add(inStats);
    }
    class StatsEnumerator
    {
    public:
        StatsEnumerator(
            Properties& inProperties)
            : mProperties(inProperties),
              mPrefix(),
              mPrefixLen(mPrefix.length()),
              mValue()
            {}
        template<typename KT, typename VT>
        void operator()(
            const KT& inKey,
            const VT& inVal)
        {
            mValue.clear();
            AppendDecIntToString(mValue, inVal);
            mProperties.setValue(
                mPrefix.Truncate(mPrefixLen).Append(inKey),
                mValue
            );
        }
        StatsEnumerator& SetPrefix(
            const char* inPrefixPtr)
        {
            mPrefix.Copy(inPrefixPtr, strlen(inPrefixPtr));
            mPrefixLen = mPrefix.length();
            return *this;
        }
    private:
        Properties&        mProperties;
        Properties::String mPrefix;
        size_t             mPrefixLen;
        Properties::String mValue;
    private:
        StatsEnumerator(
            const StatsEnumerator& inEnumerator);
        StatsEnumerator& operator=(
            const StatsEnumerator& inEnumerator);
    };
    Properties GetStats()
    {
        mReadStats   = mTotalReadStats;
        mWriteStats  = mTotalWriteStats;
        mAppendStats = mTotalAppendStats;
        for (Workers::const_iterator theIt = mWorkers.begin();
                theIt != mWorkers.end();
                ++theIt) {
            theIt->second->AddStats();
        }
        Properties      theRet;
        StatsEnumerator theEnumerator(theRet);
        mReadStats.mStats.Enumerate(
            theEnumerator.SetPrefix("Read."));
        mReadStats.mCSStats.Enumerate(
            theEnumerator.SetPrefix("Read.ChunkServer."));
        mWriteStats.mStats.Enumerate(
            theEnumerator.SetPrefix("Write."));
        mWriteStats.mCSStats.Enumerate(
            theEnumerator.SetPrefix("Write.ChunkServer."));
        mAppendStats.mStats.Enumerate(
            theEnumerator.SetPrefix("Append."));
        mAppendStats.mCSStats.Enumerate(
            theEnumerator.SetPrefix("Append.ChunkServer."));
        KfsNetClient::Stats theStats;
        mMetaServer.GetStats(theStats);
        theStats.Enumerate(theEnumerator.SetPrefix("MetaServer."));
        if (mClientPoolPtr) {
            mClientPoolPtr->GetStats(theStats);
            theStats.Enumerate(theEnumerator.SetPrefix("ChunkServer.Pool."));
            theEnumerator("Size", mClientPoolPtr->GetSize());
        }
        theEnumerator.SetPrefix("Network.");
        theEnumerator("Sockets",       globals().ctrOpenNetFds.GetValue());
        theEnumerator("BytesSent",     globals().ctrNetBytesWritten.GetValue());
        theEnumerator("BytesReceived", globals().ctrNetBytesRead.GetValue());
        return theRet;
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

KfsProtocolWorker::Request::Request(
    KfsProtocolWorker::RequestType            inRequestType  /* = kRequestTypeUnknown */,
    KfsProtocolWorker::FileInstance           inFileInstance /* = 0 */,
    KfsProtocolWorker::FileId                 inFileId       /* = -1 */,
    const KfsProtocolWorker::Request::Params* inParamsPtr    /* = 0 */,
    void*                                     inBufferPtr    /* = 0 */,
    int                                       inSize         /* = 0 */,
    int                                       inMaxPending   /* = -1 */,
    int64_t                                   inOffset       /* = -1 */)
    : mRequestType(inRequestType),
      mFileInstance(inFileInstance),
      mFileId(inFileId),
      mParamsPtr(inParamsPtr),
      mBufferPtr(inBufferPtr),
      mSize(inSize),
      mState(KfsProtocolWorker::Request::kStateNone),
      mStatus(0),
      mMaxPendingOrEndPos(inMaxPending),
      mOffset(inOffset)
{
    KfsProtocolWorker::Impl::WorkQueue::Init(*this);
}

void
KfsProtocolWorker::Request::Reset(
    KfsProtocolWorker::RequestType            inRequestType  /* = kRequestTypeUnknown */,
    KfsProtocolWorker::FileInstance           inFileInstance /* = 0 */,
    KfsProtocolWorker::FileId                 inFileId       /* = -1 */,
    const KfsProtocolWorker::Request::Params* inParamsPtr    /* = 0 */,
    void*                                     inBufferPtr    /* = 0 */,
    int                                       inSize         /* = 0 */,
    int                                       inMaxPending   /* = -1 */,
    int64_t                                   inOffset       /* = -1 */)
{
    mRequestType        = inRequestType;
    mFileInstance       = inFileInstance;
    mFileId             = inFileId;
    mParamsPtr          = inParamsPtr;
    mBufferPtr          = inBufferPtr;
    mSize               = inSize;
    mMaxPendingOrEndPos = inMaxPending;
    mState              = KfsProtocolWorker::Request::kStateNone;
    mStatus             = 0;
    mOffset             = inOffset;
}

/* virtual */
KfsProtocolWorker::Request::~Request()
{
    QCRTASSERT(mState != kStateInFlight);
}

KfsProtocolWorker::KfsProtocolWorker(
        string                               inMetaHost,
        int                                  inMetaPort,
        const KfsProtocolWorker::Parameters* inParametersPtr /* = 0 */)
    : mImpl(*(new Impl(
        inMetaHost,
        inMetaPort,
        inParametersPtr ? *inParametersPtr : KfsProtocolWorker::Parameters()
    )))
{
}

KfsProtocolWorker::~KfsProtocolWorker()
{
    delete &mImpl;
}

void
KfsProtocolWorker::Start()
{
    mImpl.Start();
}

void
KfsProtocolWorker::Stop()
{
    mImpl.Stop();
}

int64_t
KfsProtocolWorker::Execute(
    KfsProtocolWorker::RequestType            inRequestType,
    KfsProtocolWorker::FileInstance           inFileInstance,
    KfsProtocolWorker::FileId                 inFileId,
    const KfsProtocolWorker::Request::Params* inParamsPtr,
    void*                                     inBufferPtr,
    int                                       inSize,
    int                                       inMaxPending,
    int64_t                                   inOffset)
{
    return mImpl.Execute(
        inRequestType,
        inFileInstance,
        inFileId,
        inParamsPtr,
        inBufferPtr,
        inSize,
        inMaxPending,
        inOffset
    );
}

void
KfsProtocolWorker::ExecuteMeta(
    KfsOp& inOp)
{
    const int64_t theRet = mImpl.Execute(
        kRequestTypeMetaOp,
        1,
        1,
        0,
        &inOp,
        0,
        0,
        0
    );
    if (theRet < 0 && 0 <= inOp.status) {
        inOp.status = (int)theRet;
    }
}

Properties
KfsProtocolWorker::GetStats()
{
    Properties theRet;
    mImpl.Execute(
        kRequestTypeGetStatsOp,
        1,
        1,
        0,
        &theRet,
        0,
        0,
        0
    );
    return theRet;
}

void
KfsProtocolWorker::Enqueue(
    Request& inRequest)
{
    if (inRequest.mRequestType == kRequestTypeMetaOp ||
            inRequest.mRequestType == kRequestTypeGetStatsOp) {
        QCASSERT(! "invalid request code");
        const int theStatus = kErrProtocol;
        inRequest.mState  = Request::kStateDone;
        inRequest.mStatus = theStatus;
        inRequest.Done(theStatus);
        return;
    }
    mImpl.Enqueue(inRequest);
}

void
KfsProtocolWorker::SetMetaMaxRetryCount(
    int inMaxRetryCount)
{
    mImpl.SetMetaMaxRetryCount(inMaxRetryCount);
}

void
KfsProtocolWorker::SetMetaTimeSecBetweenRetries(
    int inSecs)
{
    mImpl.SetMetaTimeSecBetweenRetries(inSecs);
}

void
KfsProtocolWorker::SetMaxRetryCount(
    int inMaxRetryCount)
{
    mImpl.SetMaxRetryCount(inMaxRetryCount);
}

void
KfsProtocolWorker::SetTimeSecBetweenRetries(
    int inSecs)
{
    mImpl.SetTimeSecBetweenRetries(inSecs);
}

void
KfsProtocolWorker::SetMetaOpTimeoutSec(
    int inSecs)
{
    mImpl.SetMetaOpTimeoutSec(inSecs);
}

void
KfsProtocolWorker::SetOpTimeoutSec(
    int inSecs)
{
    mImpl.SetOpTimeoutSec(inSecs);
}

}} /* namespace client KFS */
