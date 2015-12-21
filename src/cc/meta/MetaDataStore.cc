//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/12/15
// Author: Mike Ovsiannikov
//
// Copyright 2015 Quantcast Corp.
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

#include "MetaDataStore.h"
#include "util.h"

#include "common/Properties.h"
#include "common/SingleLinkedQueue.h"
#include "common/StdAllocator.h"
#include "common/kfsatomic.h"
#include "common/MsgLogger.h"

#include "qcdio/QCThread.h"
#include "qcdio/qcstutils.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcdebug.h"

#include "kfsio/ITimeout.h"
#include "kfsio/NetManager.h"

#include "MetaRequest.h"

#include <map>
#include <vector>
#include <functional>
#include <utility>

#include <errno.h>
#include <unistd.h>

namespace KFS
{

using std::map;
using std::less;
using std::vector;
using std::make_pair;

class MetaDataStore::Impl : public ITimeout
{
private:
    typedef SingleLinkedQueue<MetaRequest, MetaRequest::GetNext> Queue;
    typedef vector<string>                                       DeleteList;
    class Worker : public QCThread
    {
    public:
        Worker()
            : QCThread(0, "MetaDataStoreWorker"),
              mOuterPtr(0),
              mQueue(),
              mCond(),
              mPruneFlag(false)
            {}
        void PushBack(
            MetaReadMetaData& inReq)
            { mQueue.PushBack(inReq); }
        MetaReadMetaData* PopFront()
            { return static_cast<MetaReadMetaData*>(mQueue.PopFront()); }
        void Start(
            Impl& inOuter)
        {
            mOuterPtr = &inOuter;
            const int kThreadStackSize = 64 << 10;
            QCThread::Start(0, kThreadStackSize);
        }
        virtual void Run()
            { mOuterPtr->Run(*this); }
    private:
        Impl*     mOuterPtr;
        Queue     mQueue;
        QCCondVar mCond;
        bool      mPruneFlag;
    friend class Impl;
    };
    class Enntry
    {
    public:
        Enntry(
            seq_t       inLogSeq      = -1,
            seq_t       inLogEndSeq   = -1,
            const char* inFileNamePtr = "",
            int         inThreadIdx   = -1)
            : mLogSeq(inLogSeq),
              mLogEndSeq(inLogEndSeq),
              mFileName(inFileNamePtr),
              mThreadIdx(inThreadIdx),
              mFd(-1),
              mUseCount(0),
              mAccessTime(0)
            {}
        bool Expire(
            time_t inExpireTime)
        {
            if (0 <= mFd && mUseCount <= 0 && mAccessTime < inExpireTime) {
                close(mFd);
                mFd       = -1;
                mUseCount = 0;
            }
            return (mFd < 0);
        }
        seq_t  mLogSeq;
        seq_t  mLogEndSeq;
        string mFileName;
        int    mThreadIdx;
        int    mFd;
        int    mUseCount;
        time_t mAccessTime;
    };
    typedef Enntry Checkpoint;
    typedef Enntry LogSegment;
    typedef map<
        seq_t,
        Checkpoint,
        less<seq_t>,
        StdFastAllocator<pair<const seq_t, Checkpoint> >
    > Checkpoints;
    typedef map<
        seq_t,
        LogSegment,
        less<seq_t>,
        StdFastAllocator<pair<const seq_t, LogSegment> >
    > LogSegments;
public:
    Impl(
        NetManager& inNetManager)
        : ITimeout(),
          mWorkersPtr(0),
          mWorkersCount(0),
          mDoneCount(0),
          mMutex(),
          mStopFlag(false),
          mDoneQueue(),
          mCheckpoints(),
          mLogSegments(),
          mMaxInactiveTime(60),
          mMaxCheckpointsToKeepCount(16),
          mOpenCheckpontsCount(0),
          mOpenLogSegmentsCount(0),
          mCurThreadIdx(0),
          mNetManager(inNetManager),
          mNow(inNetManager.Now())
    {
        mNetManager.RegisterTimeoutHandler(this);
    }
    ~Impl()
    {
        mNetManager.UnRegisterTimeoutHandler(this);
        Impl::Shutdown();
    }
    void SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters)
    {
        
    }
    void Handle(
        MetaReadMetaData& inReadOp)
    {
        QCStMutexLocker theLock(mMutex);
        if (! mWorkersPtr) {
            inReadOp.status    = -ENOENT;
            inReadOp.statusMsg = "shutdown";
            return;
        }
        if (inReadOp.checkpointFlag) {
            if (mCheckpoints.empty()) {
                inReadOp.status    = -ENOENT;
                inReadOp.statusMsg = "no checkpoint exists";
                return;
            }
            Checkpoint* theCheckpointPtr;
            if (inReadOp.startLogSeq < 0) {
                theCheckpointPtr = &(mCheckpoints.rbegin()->second);
                inReadOp.startLogSeq = theCheckpointPtr->mLogSeq;
                inReadOp.readPos     = 0;
            } else {
                Checkpoints::iterator const theIt =
                    mCheckpoints.find(inReadOp.startLogSeq);
                if (theIt == mCheckpoints.end()) {
                    inReadOp.status    = -ENOENT;
                    inReadOp.statusMsg = "no such checkpoint";
                    return;
                }
                theCheckpointPtr = &(theIt->second);
            }
            Checkpoint& theCheckpoint = *theCheckpointPtr;
            theCheckpoint.mAccessTime = mNow;
            theCheckpoint.mUseCount++;
            QCASSERT(0 <= theCheckpoint.mThreadIdx &&
                theCheckpoint.mThreadIdx < mWorkersCount);
            Worker& theWorker = mWorkersPtr[theCheckpoint.mThreadIdx];
            theWorker.PushBack(inReadOp);
            theWorker.mCond.Notify();
            return;
        }
        if (inReadOp.startLogSeq < 0) {
            inReadOp.status    = -EINVAL;
            inReadOp.statusMsg = "invalid log sequence";
            return;
        }
        LogSegment* theLogSegmentPtr;
        if (0 < inReadOp.readPos) {
            LogSegments::iterator const theIt =
                mLogSegments.find(inReadOp.startLogSeq);
            if (theIt == mLogSegments.end()) {
                inReadOp.status    = -EINVAL;
                inReadOp.statusMsg = "no such log sequence";
                return;
            }
            theLogSegmentPtr = &(theIt->second);
        } else {
            LogSegments::iterator theIt =
                mLogSegments.lower_bound(inReadOp.startLogSeq);
            if (mLogSegments.end() == theIt ||
                    inReadOp.startLogSeq < theIt->second.mLogSeq) {
                if (theIt == mLogSegments.begin()) {
                    inReadOp.status    = -ENOENT;
                    inReadOp.statusMsg = "no such log segment";
                    return;
                }
                --theIt;
            }
            if (theIt->second.mLogEndSeq < inReadOp.startLogSeq) {
                inReadOp.status    = -EFAULT;
                inReadOp.statusMsg = "missing log segment";
                return;
            }
            QCASSERT(theIt->second.mLogSeq <= inReadOp.startLogSeq &&
                theIt->first == theIt->second.mLogSeq);
            inReadOp.startLogSeq = theIt->second.mLogSeq;
            theLogSegmentPtr = &(theIt->second);
        }
        LogSegment& theLogSegment = *theLogSegmentPtr;
        theLogSegment.mAccessTime = mNow;
        theLogSegment.mUseCount++;
        QCASSERT(0 <= theLogSegment.mThreadIdx &&
            theLogSegment.mThreadIdx < mWorkersCount);
        Worker& theWorker = mWorkersPtr[theLogSegment.mThreadIdx];
        theWorker.PushBack(inReadOp);
        theWorker.mCond.Notify();
        return;
    }
    void RegisterCheckpoint(
        const char* inFileNamePtr,
        seq_t       inLogSeq)
    {
        QCStMutexLocker theLock(mMutex);
        if (! inFileNamePtr || ! *inFileNamePtr || inLogSeq < 0 ||
                ! mCheckpoints.insert(make_pair(inLogSeq,
                    Checkpoint(inLogSeq, inLogSeq, inFileNamePtr, mCurThreadIdx)
                )).second) {
            KFS_LOG_STREAM_FATAL <<
                "invalid checkpoint:"
                " sequence: " << inLogSeq <<
                " file: "     << (inFileNamePtr ? inFileNamePtr : "null") <<
            KFS_LOG_EOM;
            panic("invalid checkpoint registration attempt");
        }
        mCurThreadIdx++;
        if (mCurThreadIdx <= mWorkersCount) {
            mCurThreadIdx = 0;
        }
    }
    void RegisterLogSegment(
        const char* inFileNamePtr,
        seq_t       inStartSeq,
        seq_t       inEndSeq)
    {
        QCStMutexLocker theLock(mMutex);
        if (! inFileNamePtr || ! *inFileNamePtr || inStartSeq < 0 ||
                inEndSeq < inStartSeq ||
                ! mLogSegments.insert(make_pair(inStartSeq,
                    LogSegment(
                        inStartSeq, inEndSeq, inFileNamePtr, mCurThreadIdx)
                )).second) {
            KFS_LOG_STREAM_FATAL <<
                "invalid log segment:"
                " sequence: " << inStartSeq <<
                " end seq: "  << inEndSeq   <<
                " file: "     << (inFileNamePtr ? inFileNamePtr : "null") <<
            KFS_LOG_EOM;
            panic("invalid log segment registration attempt");
        }
        mCurThreadIdx++;
        if (mCurThreadIdx <= mWorkersCount) {
            mCurThreadIdx = 0;
        }
    }
    int Start()
    {
        if (mWorkersPtr || mWorkersCount <= 0) {
            return -EINVAL;
        }
        mStopFlag = false;
        mWorkersPtr = new Worker[mWorkersCount];
        for (int i = 0; i < mWorkersCount; i++) {
            mWorkersPtr[i].Start(*this);
        }
        return 0;
    }
    void Shutdown()
    {
        QCStMutexLocker theLock(mMutex);
        if (mStopFlag || ! mWorkersPtr) {
            return;
        }
        mStopFlag = true;
        for (int i = 0; i < mWorkersCount; i++) {
            mWorkersPtr[i].mCond.Notify();
            QCStMutexUnlocker theUnlock(mMutex);
            mWorkersPtr[i].Join();
        }
        mWorkersCount = 0;
        delete [] mWorkersPtr;
        mWorkersPtr = 0;
    }
    void Run(
        Worker& inWorker)
    {
        DeleteList theDeleteList;
        QCStMutexLocker theLock(mMutex);
        while (! mStopFlag) {
            inWorker.mCond.Wait(mMutex);
            MetaReadMetaData* thePtr;
            while ((thePtr = inWorker.PopFront())) {
                MetaReadMetaData& theCur = *thePtr;
                if (mStopFlag) {
                    theCur.status    = -ECANCELED;
                    theCur.statusMsg = "canceled by shutdown";
                } else {
                    Process(theCur);
                }
                mDoneQueue.PushBack(theCur);
                SyncAddAndFetch(mDoneCount, 1);
            }
            if (inWorker.mPruneFlag) {
                theDeleteList.clear();
                mOpenCheckpontsCount = 0;
                time_t const theExpireTime = mNow - mMaxInactiveTime;
                for (Checkpoints::iterator theIt = mCheckpoints.begin();
                        theIt != mCheckpoints.end();
                        ) {
                    const bool theClosedFlag =
                        theIt->second.Expire(theExpireTime);
                    if (theClosedFlag &&
                            mMaxCheckpointsToKeepCount < mCheckpoints.size()) {
                        theDeleteList.push_back(theIt->second.mFileName);
                        mCheckpoints.erase(theIt++);
                    } else {
                        if (! theClosedFlag) {
                            mOpenCheckpontsCount++;
                        }
                        ++theIt;
                    }
                }
                const seq_t theMinLogSeq = mCheckpoints.empty() ?
                    seq_t(-1) : mCheckpoints.begin()->second.mLogSeq;
                mOpenLogSegmentsCount = 0;
                for (LogSegments::iterator theIt = mLogSegments.begin();
                        theIt != mLogSegments.end();
                        ++theIt) {
                    const bool theClosedFlag =
                        theIt->second.Expire(theExpireTime); 
                    if (theClosedFlag && theIt->second.mLogSeq < theMinLogSeq) {
                        theDeleteList.push_back(theIt->second.mFileName);
                        mCheckpoints.erase(theIt++);
                    } else {
                        if (! theClosedFlag) {
                            mOpenLogSegmentsCount++;
                        }
                        ++theIt;
                    }
                }
                if (! theDeleteList.empty()) {
                    QCStMutexUnlocker theUnlock(mMutex);
                    for (DeleteList::iterator theIt = theDeleteList.begin();
                            theIt != theDeleteList.end();
                            ++theIt) {
                        if (unlink(theIt->c_str())) {
                            const int theErr = errno;
                            KFS_LOG_STREAM_ERROR <<
                                "delete " << *theIt << ": " <<
                                QCUtils::SysError(theErr) <<
                            KFS_LOG_EOM;
                        }
                    }
                    theDeleteList.clear();
                }
            }
        }
    }
    virtual void Timeout()
    {
        const time_t theNow = mNetManager.Now();
        if (SyncAddAndFetch(mDoneCount, 0) <= 0 &&
                theNow == mNow) {
            return;
        }
        Queue theDoneQueue;
        QCStMutexLocker theLock(mMutex);
        mNow = theNow;
        mDoneQueue.Swap(theDoneQueue);
        mDoneCount = 0;
        theLock.Unlock();
        Queue::Entry* thePtr;
        while ((thePtr = theDoneQueue.PopFront())) {
            submit_request(thePtr);
        }
    }
private:
    Worker*      mWorkersPtr;
    int          mWorkersCount;
    volatile int mDoneCount;
    QCMutex      mMutex;
    bool         mStopFlag;
    Queue        mDoneQueue;
    Checkpoints  mCheckpoints;
    LogSegments  mLogSegments;
    int          mMaxInactiveTime;
    int          mMaxCheckpointsToKeepCount;
    int          mOpenCheckpontsCount;
    int          mOpenLogSegmentsCount;
    int          mCurThreadIdx;
    NetManager&  mNetManager;
    time_t       mNow;

    void Process(
        MetaReadMetaData& inReadOp)
    {
        QCStMutexUnlocker theUnlock(mMutex);
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};
/*
MetaDataStore::MetaDataStore(
    NetManager& inNetManager)
    : mImpl(*(new Impl(inNetManager)))
    {}

MetaDataStore::~MetaDataStore()
{
    delete &mImpl;
}

    void
MetaDataStore::SetParameters(
    const char*       inPrefixPtr,
    const Properties& inParameters)
{
    mImpl.SetParameters(inPrefixPtr, inParameters);
}

    void
MetaDataStore::Handle(
    MetaReadMetaData& inReadOp)
{
    mImpl.Handle(inReadOp);
}

    void
MetaDataStore::RegisterCheckpoint(
    const char* inFileNamePtr,
    seq_t       inLogSeq)
{
    mImpl.RegisterCheckpoint(inFileNamePtr, inLogSeq);
}

    void
MetaDataStore::RegisterLogSegment(
    const char* inFileNamePtr,
    seq_t       inStartSeq,
    seq_t       inEndSeq)
{
    mImpl.RegisterLogSegment(inFileNamePtr, inStartSeq, inEndSeq);
}

    int
MetaDataStore::Start()
{
    return mImpl.Start();
}

    void
MetaDataStore::Shutdown()
{
    mImpl.Shutdown();
}
*/
} // namespace KFS
