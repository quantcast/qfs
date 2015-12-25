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
#include "qcdio/QCDLList.h"
#include "qcdio/qcdebug.h"

#include "kfsio/ITimeout.h"
#include "kfsio/NetManager.h"

#include "MetaRequest.h"

#include <map>
#include <vector>
#include <functional>
#include <utility>
#include <algorithm>

#include <errno.h>
#include <unistd.h>
#include <fcntl.h>

namespace KFS
{

using std::map;
using std::less;
using std::vector;
using std::make_pair;
using std::min;

class MetaDataStore::Impl : public ITimeout
{
private:
    typedef SingleLinkedQueue<MetaRequest, MetaRequest::GetNext> Queue;
    typedef vector<string>                                       DeleteList;
    typedef vector<int>                                          CloseList;
    class Worker : public QCThread
    {
    public:
        Worker()
            : QCThread(0, "MetaDataStoreWorker"),
              mOuterPtr(0),
              mQueue(),
              mCond()
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
    friend class Impl;
    };
    class Entry
    {
    public:
        typedef QCDLListOp<Entry> List;

        Entry(
            seq_t       inLogSeq      = -1,
            seq_t       inLogEndSeq   = -1,
            const char* inFileNamePtr =  0,
            int         inThreadIdx   = -1)
            : mLogSeq(inLogSeq),
              mLogEndSeq(inLogEndSeq),
              mFileName(inFileNamePtr ? inFileNamePtr : ""),
              mThreadIdx(inThreadIdx),
              mFd(-1),
              mUseCount(0),
              mAccessTime(0),
              mPendingDeleteFlag(false)
            { List::Init(*this); }
        Entry(
            const Entry& inEntry)
            : mLogSeq(inEntry.mLogSeq),
              mLogEndSeq(inEntry.mLogEndSeq),
              mFileName(inEntry.mFileName),
              mThreadIdx(inEntry.mThreadIdx),
              mFd(inEntry.mFd),
              mUseCount(inEntry.mUseCount),
              mAccessTime(inEntry.mAccessTime),
              mPendingDeleteFlag(inEntry.mPendingDeleteFlag)
            { List::Init(*this); }
        ~Entry()
            { List::Remove(*this); }
        bool Expire(
            time_t inExpireTime)
        {
            if (mUseCount <= 0 && (mAccessTime < inExpireTime || mFd < 0)) {
                List::Remove(*this);
                return true;
            }
            return false;
        }
        void UpdateLru(
            Entry& inLru,
            time_t inNow)
        {
            if (mUseCount <= 0 && mFd < 0) {
                if (mPendingDeleteFlag) {
                    List::Insert(*this, inLru);
                } else {
                    List::Remove(*this);
                }
            } else {
                List::Insert(*this, List::GetPrev(inLru));
            }
            mAccessTime = inNow;
        }
        bool IsInUse() const
            { return (0 < mFd || mUseCount <= 0); }
        seq_t  mLogSeq;
        seq_t  mLogEndSeq;
        string mFileName;
        int    mThreadIdx;
        int    mFd;
        int    mUseCount;
        time_t mAccessTime;
        bool   mPendingDeleteFlag;
    private:
        Entry* mPrevPtr[1];
        Entry* mNextPtr[1];
        friend class QCDLListOp<Entry>;
    };
    typedef Entry Checkpoint;
    typedef Entry LogSegment;
    typedef map<
        seq_t,
        Checkpoint,
        less<seq_t>,
        StdFastAllocator<pair<const seq_t, Checkpoint> >
    > Checkpoints;
    typedef Checkpoint::List CheckpointLru;
    typedef map<
        seq_t,
        LogSegment,
        less<seq_t>,
        StdFastAllocator<pair<const seq_t, LogSegment> >
    > LogSegments;
    typedef LogSegment::List LogSegmentsLru;
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
          mCheckpointsLru(),
          mLogSegmentsLru(),
          mMinLogSeq(-1),
          mPendingDeleteCount(0),
          mMaxReadSize(2 << 20),
          mMaxInactiveTime(60),
          mMaxCheckpointsToKeepCount(16),
          mCurThreadIdx(0),
          mPendingCount(0),
          mNetManager(inNetManager),
          mNow(inNetManager.Now())
        { mNetManager.RegisterTimeoutHandler(this); }
    ~Impl()
    {
        mNetManager.UnRegisterTimeoutHandler(this);
        Impl::Shutdown();
    }
    void SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters)
    {
        Properties::String theName(inPrefixPtr ? inPrefixPtr : "");
        const size_t       thePrefLen = theName.GetSize();
        QCStMutexLocker theLock(mMutex);
        mMaxReadSize = max(64 << 10, inParameters.getValue(
            theName.Truncate(thePrefLen).Append("maxReadSize"),
            mMaxReadSize));
        mMaxInactiveTime = max(10, inParameters.getValue(
            theName.Truncate(thePrefLen).Append("maxInactiveTime"),
            mMaxInactiveTime));
        mMaxCheckpointsToKeepCount = max(1, inParameters.getValue(
            theName.Truncate(thePrefLen).Append("maxCheckpointsToKeepCount"),
            mMaxCheckpointsToKeepCount));
        if (! mWorkersPtr) {
            mWorkersCount = max(1, inParameters.getValue(
                theName.Truncate(thePrefLen).Append("threadCount"),
                mWorkersCount));
        }
        if (mPendingCount <= 0 && mWorkersPtr && ! mStopFlag) {
            mWorkersPtr[0].mCond.Notify();
        }
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
            theCheckpoint.mUseCount++;
            theCheckpoint.UpdateLru(mCheckpointsLru, mNow);
            QCASSERT(0 <= theCheckpoint.mThreadIdx &&
                theCheckpoint.mThreadIdx < mWorkersCount);
            Worker& theWorker = mWorkersPtr[theCheckpoint.mThreadIdx];
            theWorker.PushBack(inReadOp);
            mPendingCount++;
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
        theLogSegment.mUseCount++;
        theLogSegment.UpdateLru(mLogSegmentsLru, mNow);
        QCASSERT(0 <= theLogSegment.mThreadIdx &&
            theLogSegment.mThreadIdx < mWorkersCount);
        Worker& theWorker = mWorkersPtr[theLogSegment.mThreadIdx];
        theWorker.PushBack(inReadOp);
        mPendingCount++;
        theWorker.mCond.Notify();
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
        if (mWorkersCount <= mCurThreadIdx) {
            mCurThreadIdx = 0;
        }
        if (mPendingCount <= 0 && mWorkersPtr && ! mStopFlag) {
            mWorkersPtr[0].mCond.Notify();
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
        if (mWorkersCount <= mCurThreadIdx) {
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
        CloseList  theCloseList;
        theDeleteList.reserve(16);
        theCloseList.reserve(32);
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
                QCASSERT(0 < mPendingCount);
                mPendingCount--;
                mDoneQueue.PushBack(theCur);
                SyncAddAndFetch(mDoneCount, 1);
            }
            theDeleteList.clear();
            theCloseList.clear();
            Expire(theDeleteList, theCloseList);
            int thePruneCount =
                (int)mCheckpoints.size() - mMaxCheckpointsToKeepCount -
                mPendingDeleteCount;
            const seq_t thePrevMinLogSeq = mMinLogSeq;
            for (Checkpoints::iterator theIt = mCheckpoints.begin();
                    0 < thePruneCount;
                    thePruneCount--) {
                if (mMinLogSeq < theIt->second.mLogSeq) {
                    mMinLogSeq = theIt->second.mLogSeq;
                }
                if (theIt->second.IsInUse()) {
                    if (! theIt->second.mPendingDeleteFlag) {
                        mPendingDeleteCount++;
                        theIt->second.mPendingDeleteFlag = true;
                    }
                    ++theIt;
                } else {
                    if (0 <= theIt->second.mFd) {
                        theCloseList.push_back(theIt->second.mFd);
                        theIt->second.mFd = -1;
                    }
                    theDeleteList.push_back(theIt->second.mFileName);
                    mCheckpoints.erase(theIt++);
                }
            }
            if (thePrevMinLogSeq < mMinLogSeq) {
                LogSegments::iterator theIt =
                    mLogSegments.find(thePrevMinLogSeq);
                if (mLogSegments.end() == theIt) {
                    theIt = mLogSegments.begin();
                }
                while (theIt != mLogSegments.end() &&
                            theIt->second.mLogSeq < mMinLogSeq) {
                    if (theIt->second.IsInUse()) {
                        theIt->second.mPendingDeleteFlag = true;
                        ++theIt;
                    } else {
                        if (0 <= theIt->second.mFd) {
                            theCloseList.push_back(theIt->second.mFd);
                            theIt->second.mFd = -1;
                        }
                        theDeleteList.push_back(theIt->second.mFileName);
                        mLogSegments.erase(theIt++);
                    }
                }
            }
            if (! theDeleteList.empty() || ! theCloseList.empty()) {
                QCStMutexUnlocker theUnlock(mMutex);
                while (! theCloseList.empty()) {
                    close(theCloseList.back());
                    theCloseList.pop_back();
                }
                while (! theDeleteList.empty()) {
                    const string& theName = theDeleteList.back();
                    if (unlink(theName.c_str())) {
                        const int theErr = errno;
                        KFS_LOG_STREAM_ERROR <<
                            "delete " << theName << ": " <<
                            QCUtils::SysError(theErr) <<
                        KFS_LOG_EOM;
                    }
                    theDeleteList.pop_back();
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
        if (mPendingCount <= 0 && mWorkersPtr && ! mStopFlag) {
            time_t const theExpireTime = mNow - mMaxInactiveTime;
            if (HasExpired(mCheckpointsLru, theExpireTime) ||
                    HasExpired(mLogSegmentsLru, theExpireTime)) {
                mWorkersPtr[0].mCond.Notify();
            }
        }
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
    Checkpoint   mCheckpointsLru;
    LogSegment   mLogSegmentsLru;
    seq_t        mMinLogSeq;
    int          mPendingDeleteCount;
    int          mMaxReadSize;
    int          mMaxInactiveTime;
    int          mMaxCheckpointsToKeepCount;
    int          mCurThreadIdx;
    int          mPendingCount;
    NetManager&  mNetManager;
    time_t       mNow;

    
    template<typename EntryT, typename TableT>
    void Read(
        EntryT&           inLru,
        TableT&           inTable,
        MetaReadMetaData& inReadOp)
    {
        typename TableT::iterator const theIt =
            inTable.find(inReadOp.startLogSeq);
        if (theIt == inTable.end()) {
            inReadOp.status    = -EFAULT;
            inReadOp.statusMsg = "internal error -- no such entry";
            return;
        }
        EntryT& theEntry = theIt->second;
        QCRTASSERT(0 < theEntry.mUseCount);
        theEntry.UpdateLru(inLru, mNow);
        const int theMaxRead = mMaxReadSize;
        QCStMutexUnlocker theUnlock(mMutex);
        if (theEntry.mFd < 0) {
            theEntry.mFd = open(theEntry.mFileName.c_str(), O_RDONLY);
        }
        if (theEntry.mFd < 0) {
            const int theErr = errno;
            KFS_LOG_STREAM_ERROR <<
                "open: " << theEntry.mFileName << ": " <<
                QCUtils::SysError(theErr) <<
            KFS_LOG_EOM;
            inReadOp.status    = -EIO;
            inReadOp.statusMsg = "failed to open file";
        } else {
            const int theNumRd = inReadOp.data.Read(
                theEntry.mFd, min(theMaxRead, inReadOp.readSize));
            if (theNumRd < 0) {
                inReadOp.status    = -EIO;
                inReadOp.statusMsg = QCUtils::SysError(-theNumRd);
                KFS_LOG_STREAM_ERROR <<
                    "read: " << inReadOp.Show() << " " << inReadOp.statusMsg <<
                KFS_LOG_EOM;
            } else {
                KFS_LOG_STREAM_DEBUG <<
                    "read: " << inReadOp.Show() << " " <<
                    inReadOp.data.BytesConsumable()  <<
                KFS_LOG_EOM;
            }
        }
        theUnlock.Lock();
        theEntry.mUseCount--;
        QCASSERT(0 <= theEntry.mUseCount);
        theEntry.UpdateLru(inLru, mNow);
    }
    void Process(
        MetaReadMetaData& inReadOp)
    {
        if (inReadOp.checkpointFlag) {
            Read(mCheckpointsLru, mCheckpoints, inReadOp);
        } else {
            Read(mLogSegmentsLru, mLogSegments, inReadOp);
        }
    }
    template<typename EntryT, typename TableT> static
    void Expire(
        EntryT&     inLru,
        TableT&     inTable,
        time_t      inExpireTime,
        DeleteList& inDeleteList,
        CloseList&  inCloseList)
    {
        for (; ;) {
            EntryT& theEntry = EntryT::List::GetNext(inLru);
            if (&theEntry == &inLru || ! theEntry.Expire(inExpireTime)) {
                break;
            }
            if (0 <= theEntry.mFd) {
                inCloseList.push_back(theEntry.mFd);
                theEntry.mFd = -1;
            }
            if (theEntry.mPendingDeleteFlag) {
                inDeleteList.push_back(theEntry.mFileName);
                inTable.erase(theEntry.mLogSeq);
            }
        }
    }
    void Expire(
        DeleteList& inDeleteList,
        CloseList&  inCloseList)
    {
        time_t const theExpireTime = mNow - mMaxInactiveTime;
        const size_t theSz = inDeleteList.size();
        Expire(mCheckpointsLru, mCheckpoints, theExpireTime,
            inDeleteList, inCloseList);
        const int theDelta = (int)(inDeleteList.size() - theSz);
        QCASSERT(0 <= theDelta && theDelta <= mPendingDeleteCount);
        mPendingDeleteCount -= theDelta;
        Expire(mLogSegmentsLru, mLogSegments, theExpireTime,
            inDeleteList, inCloseList);
    }
    template<typename EntryT>
    bool HasExpired(
        const EntryT& inLru,
        time_t        inExpireTime)
    {
        EntryT& theEntry =  EntryT::List::GetNext(inLru);
        return (&inLru != &theEntry && theEntry.mAccessTime < inExpireTime);
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

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

} // namespace KFS
