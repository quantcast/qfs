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
#include "MetaVrLogSeq.h"
#include "util.h"

#include "common/Properties.h"
#include "common/SingleLinkedQueue.h"
#include "common/StdAllocator.h"
#include "common/kfsatomic.h"
#include "common/MsgLogger.h"
#include "common/RequestParser.h"

#include "qcdio/QCThread.h"
#include "qcdio/qcstutils.h"
#include "qcdio/QCUtils.h"
#include "qcdio/QCDLList.h"
#include "qcdio/qcdebug.h"

#include "kfsio/ITimeout.h"
#include "kfsio/NetManager.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/checksum.h"

#include "MetaRequest.h"

#include <map>
#include <vector>
#include <functional>
#include <utility>
#include <algorithm>

#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>

namespace KFS
{

using std::map;
using std::multimap;
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
    typedef MetaVrLogSeq                                         VrLogSeq;
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
            const VrLogSeq& inLogSeq      = VrLogSeq(),
            const VrLogSeq& inLogEndSeq   = VrLogSeq(),
            const char*     inFileNamePtr =  0,
            int             inThreadIdx   = -1)
            : mLogSeq(inLogSeq),
              mLogEndSeq(inLogEndSeq),
              mFileName(inFileNamePtr ? inFileNamePtr : ""),
              mFileSize(-1),
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
              mFileSize(inEntry.mFileSize),
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
            { return (0 <= mFd || 0 < mUseCount); }
        VrLogSeq mLogSeq;
        VrLogSeq mLogEndSeq;
        string   mFileName;
        int64_t  mFileSize;
        int      mThreadIdx;
        int      mFd;
        int      mUseCount;
        time_t   mAccessTime;
        bool     mPendingDeleteFlag;
    private:
        Entry* mPrevPtr[1];
        Entry* mNextPtr[1];
        friend class QCDLListOp<Entry>;
    };
    typedef Entry Checkpoint;
    typedef Entry LogSegment;
    typedef map<
        VrLogSeq,
        Checkpoint,
        less<VrLogSeq>,
        StdFastAllocator<pair<const VrLogSeq, Checkpoint> >
    > Checkpoints;
    typedef Checkpoint::List CheckpointLru;
    typedef multimap<
        VrLogSeq,
        LogSegment,
        less<VrLogSeq>,
        StdFastAllocator<pair<const VrLogSeq, LogSegment> >
    > LogSegments;
    typedef LogSegment::List LogSegmentsLru;
public:
    Impl(
        NetManager& inNetManager)
        : ITimeout(),
          mWorkersPtr(0),
          mWorkersCount(1),
          mDoneCount(0),
          mMutex(),
          mStopFlag(false),
          mDoneQueue(),
          mCheckpoints(),
          mLogSegments(),
          mCheckpointsLru(),
          mLogSegmentsLru(),
          mPendingDeleteCount(0),
          mMaxReadSize(2 << 20),
          mMaxInactiveTime(60),
          mMaxCheckpointsToKeepCount(16),
          mCurThreadIdx(0),
          mPendingCount(0),
          mNetManager(inNetManager),
          mNow(inNetManager.Now()),
          mMetaMd(),
          mClusterKey()
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
        mClusterKey = inParameters.getValue(
            kMetaClusterKeyParamNamePtr, mClusterKey);
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
        if (inReadOp.status < 0) {
            return;
        }
        inReadOp.clusterKey = mClusterKey;
        inReadOp.metaMd     = mMetaMd;
        QCStMutexLocker theLock(mMutex);
        if (! mWorkersPtr) {
            inReadOp.status    = -ECANCELED;
            inReadOp.statusMsg = "shutdown";
            return;
        }
        if (inReadOp.readSize <= 0) {
            inReadOp.readSize = mMaxReadSize;
        }
        if (inReadOp.checkpointFlag) {
            if (mCheckpoints.empty()) {
                inReadOp.status    = -ENOENT;
                inReadOp.statusMsg = "no checkpoint exists";
                return;
            }
            Checkpoint* theCheckpointPtr;
            if (! inReadOp.startLogSeq.IsValid()) {
                theCheckpointPtr = &(mCheckpoints.rbegin()->second);
                if (! theCheckpointPtr->mLogEndSeq.IsValid()) {
                    inReadOp.status    = -ENOENT;
                    inReadOp.statusMsg = "no valid checkpoint exists";
                    return;
                }
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
                inReadOp.readPos = max(int64_t(0), inReadOp.readPos);
            }
            inReadOp.suspended = true;
            Checkpoint& theCheckpoint = *theCheckpointPtr;
            SetInUse(theCheckpoint);
            theCheckpoint.UpdateLru(mCheckpointsLru, mNow);
            QCASSERT(0 <= theCheckpoint.mThreadIdx &&
                theCheckpoint.mThreadIdx < mWorkersCount);
            Worker& theWorker = mWorkersPtr[theCheckpoint.mThreadIdx];
            theWorker.PushBack(inReadOp);
            mPendingCount++;
            theWorker.mCond.Notify();
            return;
        }
        if (! inReadOp.startLogSeq.IsValid()) {
            inReadOp.status    = -EINVAL;
            inReadOp.statusMsg = "invalid log sequence";
            return;
        }
        LogSegment* theLogSegmentPtr;
        if (0 < inReadOp.readPos) {
            LogSegments::iterator theIt =
                mLogSegments.find(inReadOp.startLogSeq);
            while (theIt->second.mLogEndSeq == theIt->second.mLogSeq &&
                    theIt != mLogSegments.end()) {
                theIt++;
            }
            if (theIt == mLogSegments.end()) {
                inReadOp.status    = -EINVAL;
                inReadOp.statusMsg = "no such log sequence, non 0 position";
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
                while (theIt != mLogSegments.begin() &&
                        theIt->second.mLogEndSeq == theIt->second.mLogSeq) {
                    theIt--;
                }
            }
            while (theIt != mLogSegments.end() &&
                    theIt->second.mLogSeq == theIt->second.mLogEndSeq) {
                ++theIt;
            }
            if (theIt == mLogSegments.end()) {
                inReadOp.status    = -ERANGE;
                inReadOp.statusMsg = "log sequence is past the current log seq";
                return;
            }
            LogSegments::const_iterator theNextIt = theIt;
            if (theIt->second.mLogSeq == theIt->second.mLogEndSeq ||
                    (theIt->second.mLogEndSeq <= inReadOp.startLogSeq &&
                    (theIt->second.mLogEndSeq.IsValid() ||
                        mLogSegments.end() != ++theNextIt))) {
                inReadOp.status    = -EFAULT;
                inReadOp.statusMsg = "missing log segment";
                return;
            }
            inReadOp.readPos = 0;
            QCASSERT(theIt->second.mLogSeq <= inReadOp.startLogSeq &&
                theIt->first == theIt->second.mLogSeq);
            inReadOp.startLogSeq = theIt->second.mLogSeq;
            theLogSegmentPtr = &(theIt->second);
        }
        inReadOp.suspended = true;
        LogSegment& theLogSegment = *theLogSegmentPtr;
        SetInUse(theLogSegment);
        theLogSegment.UpdateLru(mLogSegmentsLru, mNow);
        QCASSERT(0 <= theLogSegment.mThreadIdx &&
            theLogSegment.mThreadIdx < mWorkersCount);
        Worker& theWorker = mWorkersPtr[theLogSegment.mThreadIdx];
        theWorker.PushBack(inReadOp);
        mPendingCount++;
        theWorker.mCond.Notify();
    }
    void RegisterCheckpoint(
        const char*     inFileNamePtr,
        const VrLogSeq& inStartSeq,
        seq_t           inLogSegmentNumber)
    {
        QCStMutexLocker theLock(mMutex);
        if (! inFileNamePtr || ! *inFileNamePtr ||
                ! inStartSeq.IsValid() ||
                (! mCheckpoints.empty() &&
                    inStartSeq <= mCheckpoints.rbegin()->second.mLogSeq) ||
                ! mCheckpoints.insert(make_pair(
                    inStartSeq,
                    Checkpoint(
                        inStartSeq,
                        VrLogSeq(0, 0, inLogSegmentNumber),
                        inFileNamePtr)
                    )).second) {
            KFS_LOG_STREAM_FATAL <<
                "invalid checkpoint:"
                " sequence: " << inStartSeq <<
                " file: "     << (inFileNamePtr ? inFileNamePtr : "null") <<
            KFS_LOG_EOM;
            panic("invalid checkpoint registration attempt");
            return;
        }
        if (mPendingCount <= 0 && mWorkersPtr && ! mStopFlag) {
            mWorkersPtr[0].mCond.Notify();
        }
    }
    void RegisterLogSegment(
        const char*     inFileNamePtr,
        const VrLogSeq& inStartSeq,
        seq_t           inLogSegmentNumber)
    {
        QCStMutexLocker theLock(mMutex);
        if (mLogSegments.empty()) {
            QCASSERT(! mWorkersPtr);
            return;
        }
        LogSegment* const theLastPtr =  mLogSegments.empty() ?
            0 : &(mLogSegments.rbegin()->second);
        if (! inFileNamePtr || ! *inFileNamePtr ||
                ! inStartSeq.IsValid() || inLogSegmentNumber < 0 ||
                (theLastPtr && theLastPtr->mLogEndSeq.IsValid() &&
                    theLastPtr->mLogEndSeq != inStartSeq)) {
            KFS_LOG_STREAM_FATAL <<
                "invalid log segment:"
                " sequence: " << inStartSeq <<
                " log num: "  << inLogSegmentNumber   <<
                " file: "     << (inFileNamePtr ? inFileNamePtr : "null") <<
            KFS_LOG_EOM;
            panic("invalid log segment registration attempt");
            return;
        }
        if (theLastPtr && ! theLastPtr->mLogEndSeq.IsValid()) {
            theLastPtr->mLogEndSeq = inStartSeq;
        }
        mLogSegments.insert(make_pair(
            inStartSeq,
            LogSegment(
                inStartSeq,
                VrLogSeq(-1, -1, -inLogSegmentNumber),
                inFileNamePtr
            )
        ));
    }
    int Load(
        const char* inCheckpointDirPtr,
        const char* inLogDirPtr,
        bool        inRemoveTmpCheckupointsFlag,
        bool        inIgnoreMissingSegmentsFlag,
        const char* inMetaMdPtr)
    {
        if (mWorkersPtr || mWorkersCount <= 0) {
            KFS_LOG_STREAM_ERROR <<
                "invalid attempt to load meta data after Start() invocation" <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        mMetaMd = inMetaMdPtr ? inMetaMdPtr : "";
        return LoadSelf(
            inCheckpointDirPtr,
            inLogDirPtr,
            inRemoveTmpCheckupointsFlag,
            inIgnoreMissingSegmentsFlag
        );
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
        CloseAll();
    }
    void Run(
        Worker& inWorker)
    {
        DeleteList theDeleteList;
        CloseList  theCloseList;
        theDeleteList.reserve(16);
        theCloseList.reserve(32);
        QCStMutexLocker theLock(mMutex);
        // Runs once the first worker in order to perform cleanup.
        bool theWaitFlag = &inWorker != mWorkersPtr;
        while (! mStopFlag) {
            if (theWaitFlag) {
                inWorker.mCond.Wait(mMutex);
            }
            theWaitFlag = true;
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
                if (SyncAddAndFetch(mDoneCount, 1) <= 1) {
                    mNetManager.Wakeup();
                }
            }
            theDeleteList.clear();
            theCloseList.clear();
            Expire(theDeleteList, theCloseList);
            int thePruneCount =
                (int)mCheckpoints.size() - mMaxCheckpointsToKeepCount -
                mPendingDeleteCount;
            for (Checkpoints::iterator theIt = mCheckpoints.begin();
                    0 < thePruneCount;
                    thePruneCount--) {
                if (theIt->second.IsInUse()) {
                    if (! theIt->second.mPendingDeleteFlag) {
                        mPendingDeleteCount++;
                        theIt->second.mPendingDeleteFlag = true;
                    }
                    KFS_LOG_STREAM_DEBUG <<
                        "checkpoint delete pending: " <<
                            theIt->second.mFileName <<
                    KFS_LOG_EOM;
                    ++theIt;
                } else {
                    KFS_LOG_STREAM_DEBUG <<
                        "checkpoint delete: " << theIt->second.mFileName <<
                    KFS_LOG_EOM;
                    if (0 <= theIt->second.mFd) {
                        theCloseList.push_back(theIt->second.mFd);
                        theIt->second.mFd = -1;
                    }
                    theDeleteList.push_back(theIt->second.mFileName);
                    mCheckpoints.erase(theIt++);
                }
            }
            const VrLogSeq theMinLogSeq = mCheckpoints.empty() ? VrLogSeq() :
                mCheckpoints.begin()->second.mLogSeq;
            // Always keep two last log segments: one written into, and the
            // previous one, pointed by "last" hard link, even if checkpoint
            // points to the currently open for write.
            for (LogSegments::iterator theIt = mLogSegments.begin();
                        2 < mLogSegments.size() &&
                        theIt->second.mLogSeq < theMinLogSeq &&
                        theIt->second.mLogEndSeq <= theMinLogSeq;
                    ) {
                if (theIt->second.IsInUse()) {
                    theIt->second.mPendingDeleteFlag = true;
                    KFS_LOG_STREAM_DEBUG <<
                        "log segment delete pending: " <<
                            theIt->second.mFileName <<
                    KFS_LOG_EOM;
                    // Do not delete subsequent log segments, as those
                    // will likely be required to complete state sync.
                    break;
                }
                KFS_LOG_STREAM_DEBUG <<
                    "log segment delete: " << theIt->second.mFileName <<
                KFS_LOG_EOM;
                if (0 <= theIt->second.mFd) {
                    theCloseList.push_back(theIt->second.mFd);
                    theIt->second.mFd = -1;
                }
                theDeleteList.push_back(theIt->second.mFileName);
                mLogSegments.erase(theIt++);
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
        if (theNow == mNow && SyncAddAndFetch(mDoneCount, 0) <= 0) {
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
            thePtr->suspended = false;
            submit_request(thePtr);
        }
    }
    void PrepareToFork()
        { mMutex.Lock(); }
    void ForkDone()
        { mMutex.Unlock(); }
    void ChildAtFork()
    {
        mStopFlag = true;
        CloseAll();
    }
    static bool GetLogSequenceFromFileName(
        const char*   inFileNamePtr,
        size_t        inFileNameLen,
        MetaVrLogSeq& outLogSeq,
        seq_t*        outSegNumPtr)
    {
        const int   kSeparator = '.';
        const char* theCurPtr  = reinterpret_cast<const char*>(
            memchr(inFileNamePtr, kSeparator, inFileNameLen));
        if (! theCurPtr) {
            return false;
        }
        theCurPtr++;
        const char* const theEndPtr = inFileNamePtr + inFileNameLen;
        if (ParseVrLogSeq(
                    theCurPtr,
                    theEndPtr,
                    kSeparator,
                    outSegNumPtr ? kSeparator : 0,
                    outLogSeq) &&
                (! outSegNumPtr ||
                ParseSeqNum(theCurPtr, theEndPtr, 0, *outSegNumPtr)) &&
                theCurPtr == theEndPtr) {
            return true;
        }
        outLogSeq = MetaVrLogSeq();
        if (outSegNumPtr) {
            *outSegNumPtr = -1;
        }
        return false;
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
    int          mPendingDeleteCount;
    int          mMaxReadSize;
    int          mMaxInactiveTime;
    int          mMaxCheckpointsToKeepCount;
    int          mCurThreadIdx;
    int          mPendingCount;
    NetManager&  mNetManager;
    time_t       mNow;
    string       mMetaMd;
    string       mClusterKey;

    template<typename T>
    static void CloseAll(
        T inStartIt,
        T inEndIt)
    {
        for (T theIt = inStartIt; theIt != inEndIt; ++theIt) {
            if (0 <= theIt->second.mFd) {
                close(theIt->second.mFd);
                theIt->second.mFd = -1;
            }
        }
    }
    void CloseAll()
    {
        CloseAll(mCheckpoints.begin(), mCheckpoints.end());
        CloseAll(mLogSegments.begin(), mLogSegments.end());
    }
    template<typename T>
    void SetInUse(
        T& inEntry)
    {
        if (! inEntry.IsInUse()) {
            if (mWorkersCount <= ++mCurThreadIdx) {
                mCurThreadIdx = 0;
            }
            inEntry.mThreadIdx = mCurThreadIdx;
        }
        inEntry.mUseCount++;
    }
    template<typename EntryT, typename TableT>
    void Read(
        EntryT&           inLru,
        TableT&           inTable,
        MetaReadMetaData& inReadOp,
        bool              inSetSizeFlag)
    {
        QCRTASSERT(0 <= inReadOp.readPos && 0 <= inReadOp.status);
        typename TableT::iterator theIt =
            inTable.find(inReadOp.startLogSeq);
        if (! inReadOp.checkpointFlag) {
            while (theIt != inTable.end() &&
                    theIt->second.mLogSeq == theIt->second.mLogEndSeq) {
                ++theIt;
            }
        }
        if (theIt == inTable.end()) {
            inReadOp.status    = -EFAULT;
            inReadOp.statusMsg = "internal error -- no such entry";
            return;
        }
        EntryT& theEntry = theIt->second;
        QCRTASSERT(0 < theEntry.mUseCount);
        theEntry.UpdateLru(inLru, mNow);
        inReadOp.maxReadSize = mMaxReadSize;
        QCStMutexUnlocker theUnlock(mMutex);
        if (inReadOp.readPos <= 0) {
            const size_t thePos = theEntry.mFileName.rfind('/');
            if (string::npos != thePos) {
                inReadOp.filename = theEntry.mFileName.substr(thePos + 1);
            } else {
                inReadOp.filename = theEntry.mFileName;
            }
        }
        if (theEntry.mFd < 0) {
            theEntry.mFd = open(theEntry.mFileName.c_str(), O_RDONLY);
            if (theEntry.mFileSize < 0 && 0 <= theEntry.mFd &&
                    (inSetSizeFlag || theEntry.mLogEndSeq.IsValid())) {
                const off_t theSize = lseek(theEntry.mFd, 0, SEEK_END);
                if (theSize < 0) {
                    const int theErr = errno;
                    KFS_LOG_STREAM_ERROR <<
                        "lseek: " << theEntry.mFileName << ": " <<
                        QCUtils::SysError(theErr) <<
                    KFS_LOG_EOM;
                } else {
                    theEntry.mFileSize = theSize;
                }
            }
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
            int theNumRd;
            if (0 <= theEntry.mFileSize &&
                    theEntry.mFileSize < inReadOp.readPos) {
                theNumRd = 0;
            } else if (lseek(theEntry.mFd, inReadOp.readPos, SEEK_SET) !=
                    inReadOp.readPos) {
                theNumRd = -errno;
            } else {
                theNumRd = inReadOp.data.Read(
                    theEntry.mFd, min(inReadOp.maxReadSize, inReadOp.readSize));
            }
            if (theNumRd < 0) {
                inReadOp.status    = -EIO;
                inReadOp.statusMsg = QCUtils::SysError(-theNumRd);
                KFS_LOG_STREAM_ERROR <<
                    "read: " << inReadOp.Show() << " " << inReadOp.statusMsg <<
                KFS_LOG_EOM;
            } else {
                inReadOp.checksum = ComputeCrc32(
                    &inReadOp.data, inReadOp.data.BytesConsumable());
                inReadOp.fileSize = theEntry.mFileSize;
                KFS_LOG_STREAM_DEBUG <<
                    "read: "       << inReadOp.Show() <<
                    " size: "      << inReadOp.data.BytesConsumable()  <<
                    " checksum: "  << inReadOp.checksum <<
                    " file size: " << inReadOp.fileSize <<
                KFS_LOG_EOM;
            }
        }
        theUnlock.Lock();
        inReadOp.endLogSeq = inReadOp.checkpointFlag ?
            VrLogSeq() : theEntry.mLogEndSeq;
        theEntry.mUseCount--;
        QCASSERT(0 <= theEntry.mUseCount);
        theEntry.UpdateLru(inLru, mNow);
    }
    void Process(
        MetaReadMetaData& inReadOp)
    {
        if (inReadOp.checkpointFlag) {
            const bool kSetSizeFlag = true;
            Read(mCheckpointsLru, mCheckpoints, inReadOp, kSetSizeFlag);
        } else {
            const bool kSetSizeFlag = false;
            Read(mLogSegmentsLru, mLogSegments, inReadOp, kSetSizeFlag);
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
    static bool IsDigit(
        char inSym)
    {
        const int theSym = inSym & 0xFF;
        return ('0' <= theSym && theSym <= '9');
    }
    template<typename T>
    static bool ParseSeqNum(
        const char*& ioPtr,
        const char*  inEndPtr,
        int          inSep,
        T&           outVal)
    {
        return (
            IsDigit(*ioPtr) &&
            DecIntParser::Parse(ioPtr, inEndPtr - ioPtr, outVal) &&
            0 <= outVal &&
            (0 == inSep || (ioPtr < inEndPtr && (*ioPtr++ & 0xFF) == inSep))
        );
    }
    static bool ParseVrLogSeq(
        const char*& ioPtr,
        const char*  inEndPtr,
        int          inSeqSeparator,
        int          inEndSeparator,
        VrLogSeq&    outVal)
    {
        seq_t theEpochSeq = -1;
        seq_t theViewSeq  = -1;
        seq_t theLogSeq   = -1;
        if (
                ! ParseSeqNum(ioPtr, inEndPtr, inSeqSeparator, theEpochSeq) ||
                ! ParseSeqNum(ioPtr, inEndPtr, inSeqSeparator, theViewSeq) ||
                ! ParseSeqNum(ioPtr, inEndPtr, inEndSeparator, theLogSeq)) {
            return false;
        }
        outVal = VrLogSeq(theEpochSeq, theViewSeq, theLogSeq);
        return true;
    }
    template<typename T>
    static int LoadDir(
        const char* inDirNamePtr,
        const char* inNamePrefixPtr,
        const char* inLatestNamePtr,
        const char* inTmpSuffixPtr,
        char        inSeqSeparator,
        bool        inRemoveTmpFlag,
        bool        inLatestRequiredFlag,
        bool        inHasSeqNumFlag,
        T&          inFunctor)
    {
        DIR* const theDirPtr = opendir(inDirNamePtr);
        if (! theDirPtr) {
            const int theErr = errno;
            KFS_LOG_STREAM_ERROR <<
                "opendir: " << inDirNamePtr <<
                ": " << QCUtils::SysError(theErr) <<
            KFS_LOG_EOM;
            return (0 < theErr ? -theErr : -EINVAL);
        }
        struct stat theStat = {0};
        string thePathNameStr;
        thePathNameStr.reserve(1 << 10);
        thePathNameStr = inDirNamePtr;
        thePathNameStr += "/";
        const size_t theDirNamePrefixLen = thePathNameStr.length();
        bool         theHasLatestFlag    = false;
        if (inLatestNamePtr) {
            thePathNameStr += inLatestNamePtr;
            if (stat(thePathNameStr.c_str(), &theStat)) {
                const int theErr = errno;
                if (ENOENT != theErr || inLatestRequiredFlag) {
                    KFS_LOG_STREAM_ERROR <<
                        "stat: " << thePathNameStr <<
                        ": " << QCUtils::SysError(theErr) <<
                    KFS_LOG_EOM;
                    return -EINVAL;
                }
            } else {
                theHasLatestFlag = true;
            }
        }
        int                  theRet              = 0;
        size_t const         thePrefixLen        = strlen(inNamePrefixPtr);
        size_t               theTmpSufLen        =
            inTmpSuffixPtr ? strlen(inTmpSuffixPtr) : 0;
        bool                 theUseDirentInoFlag = false;
        bool                 theLatestFlag       = false;
        const struct dirent* thePtr;
        while ((thePtr = readdir(theDirPtr))) {
            const char* const theNamePtr = thePtr->d_name;
            if (strncmp(theNamePtr, inNamePrefixPtr, thePrefixLen) != 0) {
                continue;
            }
            const char*       theCurPtr = theNamePtr + thePrefixLen;
            const char* const theEndPtr = theCurPtr + strlen(theCurPtr);
            VrLogSeq          theLogSeq;
            seq_t             theSegNum = -1;
            if (! ParseVrLogSeq(
                        theCurPtr,
                        theEndPtr,
                        inSeqSeparator,
                        inHasSeqNumFlag ? inSeqSeparator : 0,
                        theLogSeq) ||
                    (inHasSeqNumFlag &&
                    ! ParseSeqNum(theCurPtr, theEndPtr, 0, theSegNum)) ||
                    theCurPtr != theEndPtr) {
                if (inTmpSuffixPtr && (0 == theTmpSufLen ||
                        (theNamePtr + thePrefixLen + theTmpSufLen <= theEndPtr &&
                        memcmp(theEndPtr - theTmpSufLen,
                            inTmpSuffixPtr, theTmpSufLen) == 0))) {
                    thePathNameStr.erase(theDirNamePrefixLen);
                    thePathNameStr += theNamePtr;
                    KFS_LOG_STREAM_DEBUG <<
                        (inRemoveTmpFlag ?
                            "removing" : "ignoring") << ": " << thePathNameStr <<
                    KFS_LOG_EOM;
                    if (inRemoveTmpFlag && remove(thePathNameStr.c_str())) {
                        const int theErr = errno;
                        KFS_LOG_STREAM_ERROR <<
                            "remove: " << thePathNameStr <<
                            ": " << QCUtils::SysError(theErr) <<
                        KFS_LOG_EOM;
                        theRet = 0 < theErr ? -theErr : -EINVAL;
                        break;
                    }
                    continue;
                }
                KFS_LOG_STREAM_ERROR <<
                    "malformed file name: " << theNamePtr <<
                KFS_LOG_EOM;
                theRet = -ENXIO;
                break;
            }
            if (theHasLatestFlag) {
                // Check if d_ino field can be used by comparing its value with
                // st_ino. If these match for the first entry, then use d_ino,
                // otherwise use stat to get i-node number.
                if (theUseDirentInoFlag) {
                    theLatestFlag = theStat.st_ino == thePtr->d_ino;
                } else {
                    thePathNameStr.erase(theDirNamePrefixLen);
                    thePathNameStr += theNamePtr;
                    struct stat theCurStat;
                    if (stat(thePathNameStr.c_str(), &theCurStat)) {
                        const int theErr = errno;
                        KFS_LOG_STREAM_ERROR <<
                            "stat: " << thePathNameStr <<
                            ": " << QCUtils::SysError(theErr) <<
                        KFS_LOG_EOM;
                        theRet = 0 < theErr ? -theErr : -EINVAL;
                        break;
                    }
                    theLatestFlag = theStat.st_ino == theCurStat.st_ino;
                    theUseDirentInoFlag = theCurStat.st_ino == thePtr->d_ino;
                }
            }
            if (0 != (theRet = inFunctor(theLogSeq, theSegNum, theNamePtr,
                        theLatestFlag))) {
                break;
            }
        }
        closedir(theDirPtr);
        return theRet;
    }
    int LoadCheckpoint(
        const VrLogSeq&   inLogSeq,
        const char* const inNamePtr)
    {
        if (! mCheckpoints.insert(make_pair(
                    inLogSeq,
                    Checkpoint(inLogSeq, VrLogSeq(), inNamePtr)
                )).second) {
            KFS_LOG_STREAM_ERROR <<
                "duplicate checkpoint log sequence number: " <<
                inNamePtr <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        return 0;
    }
    typedef map<
        seq_t,
        LogSegment,
        less<seq_t>,
        StdFastAllocator<pair<const seq_t, LogSegment> >
    > LogSegmentNums;
    int LoadLogSegment(
        LogSegmentNums&   inLogSegmentNums,
        const VrLogSeq&   inLogSeq,
        seq_t             inNumSeq,
        const char* const inNamePtr,
        char*             inReadBufferPtr,
        size_t            inReadBufferSize,
        bool              inLastFlag,
        VrLogSeq&         ioLastSeq,
        seq_t&            ioLogSegmentWithSegNum)
    {
        if (mCheckpoints.empty()) {
            KFS_LOG_STREAM_ERROR <<
                "no checkpoints exist: " <<
                inNamePtr <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        if (! inLogSeq.IsValid()) {
            KFS_LOG_STREAM_ERROR <<
                "invalid start log sequence: " << inLogSeq <<
                " " << inNamePtr <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        VrLogSeq theStartSeq = inLogSeq;
        seq_t    theEndSeq   = inNumSeq;
        if (0 <= inNumSeq) {
            if (ioLogSegmentWithSegNum < 0) {
                ioLogSegmentWithSegNum = inNumSeq;
            } else {
                ioLogSegmentWithSegNum = min(ioLogSegmentWithSegNum, inNumSeq);
            }
        } else {
            // "Old" style segment -- find corresponding checkpoint, and
            // assign 0 range to the log segment.
            if (0 <= ioLogSegmentWithSegNum) {
                KFS_LOG_STREAM_ERROR <<
                    "invalid log segment name with no sequence: " <<
                        inNamePtr <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            Checkpoints::iterator theIt = mCheckpoints.begin();
            while (mCheckpoints.end() != theIt) {
                if (! theIt->second.mLogEndSeq.IsValid()) {
                    VrLogSeq    theLogSeq;
                    const seq_t theLogNum = GetLogSegmentNumber(
                        theIt->second.mFileName.c_str(),
                        inReadBufferPtr,
                        inReadBufferSize,
                        theLogSeq
                    );
                    if (theLogNum < 0) {
                        return -EINVAL;
                    }
                    theIt->second.mLogEndSeq = VrLogSeq(0, 0, theLogNum);
                    if (theLogSeq.IsValid()) {
                        if (ioLogSegmentWithSegNum < 0) {
                            ioLogSegmentWithSegNum = theLogNum;
                        }
                        if (ioLogSegmentWithSegNum <= theStartSeq.mLogSeq) {
                            KFS_LOG_STREAM_ERROR <<
                                "invalid log segment name with no sequence: " <<
                                    inNamePtr <<
                            KFS_LOG_EOM;
                            return -EINVAL;
                        }
                    }
                }
                if (inLogSeq < theIt->second.mLogEndSeq) {
                    if (mCheckpoints.begin() == theIt) {
                        theStartSeq = theIt->second.mLogSeq;
                        theStartSeq.mLogSeq--;
                        if (! theStartSeq.IsValid()) {
                            KFS_LOG_STREAM_ERROR <<
                                "not sufficient log sequence space:"
                                " "        << theIt->second.mLogSeq <<
                                " delta: " << theIt->second.mLogEndSeq <<
                                " for old style log segment: " << inNamePtr <<
                            KFS_LOG_EOM;
                            return -EINVAL;
                        }
                        break;
                    }
                    --theIt;
                    theStartSeq = theIt->second.mLogSeq;
                    break;
                }
                if (inLogSeq == theIt->second.mLogEndSeq) {
                    theStartSeq = theIt->second.mLogEndSeq;
                    break;
                }
                ++theIt;
            }
            if (mCheckpoints.end() == theIt) {
                --theIt;
                theStartSeq = theIt->second.mLogSeq;
            }
            theEndSeq = -(inLogSeq.mLogSeq + 1);
        }
        pair<LogSegmentNums::iterator, bool> theRes = inLogSegmentNums.insert(
            make_pair(
                0 <= inNumSeq ? inNumSeq : inLogSeq.mLogSeq,
                LogSegment(theStartSeq, VrLogSeq(0, 0, theEndSeq), inNamePtr)));
        if (! theRes.second) {
            KFS_LOG_STREAM_ERROR <<
                "duplicate log segment number:"
                " "     << theRes.first->second.mFileName <<
                " and " << inNamePtr <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        if (inLastFlag) {
            ioLastSeq = theStartSeq;
        }
        return 0;
    }
    class DataLoader
    {
    public:
        DataLoader(
            Impl&       inImpl,
            const char* inDirNamePtr)
            : mImpl(inImpl),
              mTmpName(inDirNamePtr),
              mPrefixSize(0)
        {
            if (! mTmpName.empty() && '/' != *mTmpName.rbegin()) {
                mTmpName += '/';
            }
            mPrefixSize = mTmpName.size();
        }
    protected:
        Impl&  mImpl;
        string mTmpName;
        size_t mPrefixSize;
    };
    class CheckpointLoader : public DataLoader
    {
    public:
        CheckpointLoader(
            Impl&       inImpl,
            const char* inDirNamePtr)
            : DataLoader(inImpl, inDirNamePtr),
              mLatestSeq()
            {}
        int operator()(
            const VrLogSeq&   inLogSeq,
            seq_t             inNumSeq,
            const char* const inNamePtr,
            bool              inLatestFlag)
        {
            if (0 <= inNumSeq) {
                return -EINVAL;
            }
            if (inLatestFlag) {
                mLatestSeq = inLogSeq;
            }
            mTmpName.erase(mPrefixSize);
            mTmpName += inNamePtr;
            return mImpl.LoadCheckpoint(inLogSeq, mTmpName.c_str());
        }
        VrLogSeq GetLatest() const
            { return mLatestSeq; }
    private:
        VrLogSeq mLatestSeq;
        CheckpointLoader(
            const CheckpointLoader& inLoader);
        CheckpointLoader& operator=(
            const CheckpointLoader& inLoader);
    };
    friend class CheckpointLoader;
    class LogSegmentLoader : public DataLoader
    {
    public:
        LogSegmentLoader(
            Impl&       inImpl,
            const char* inDirNamePtr)
            : DataLoader(inImpl, inDirNamePtr),
              mLastSeq(),
              mLogSegmentWithSegNum(-1),
              mTmpBuffer(),
              mLogSegmentNums()
            { mTmpBuffer.Resize(4 << 10); }
        int operator()(
            const VrLogSeq&   inLogSeq,
            seq_t             inNumSeq,
            const char* const inNamePtr,
            bool              inLastFlag)
        {
            mTmpName.erase(mPrefixSize);
            mTmpName += inNamePtr;
            return mImpl.LoadLogSegment(
                mLogSegmentNums,
                inLogSeq,
                inNumSeq,
                mTmpName.c_str(),
                mTmpBuffer.GetPtr(),
                mTmpBuffer.GetSize(),
                inLastFlag,
                mLastSeq,
                mLogSegmentWithSegNum
            );
        }
        const VrLogSeq& GetLast() const
            { return mLastSeq; }
        LogSegmentNums& GetLogSegments()
            { return mLogSegmentNums; }
        char* GetTmpBufferPtr()
            { return mTmpBuffer.GetPtr(); }
        size_t GetTmpBufferSize()
            { return mTmpBuffer.GetSize(); }
    private:
        VrLogSeq           mLastSeq;
        seq_t              mLogSegmentWithSegNum;
        StBufferT<char, 1> mTmpBuffer;
        LogSegmentNums     mLogSegmentNums;
    };
    friend class LogSegmentLoader;
    int LoadSelf(
        const char* inCheckpointDirPtr,
        const char* inLogDirPtr,
        bool        inRemoveTmpFilesFlag,
        bool        inIgnoreMissingSegmentsFlag)
    {
        if (! inCheckpointDirPtr || ! inLogDirPtr) {
            KFS_LOG_STREAM_ERROR <<
                "invalid parameters: "
                " checkpont directory: " <<
                    (inCheckpointDirPtr ? inCheckpointDirPtr : "null") <<
                " log directory: " << (inLogDirPtr ? inLogDirPtr : "null") <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        mCheckpoints.clear();
        mLogSegments.clear();
        CheckpointLoader theCheckpointLoader(*this, inCheckpointDirPtr);
        const bool       kLatestFileRequiredFlag = true;
        const bool       kCpHasSeqNumFlag        = false;
        int theRet = LoadDir(
            inCheckpointDirPtr,
            MetaDataStore::GetCheckpointFileNamePrefixPtr(),
            MetaDataStore::GetCheckpointLatestFileNamePtr(),
            ".tmp",
            '.',
            inRemoveTmpFilesFlag,
            kLatestFileRequiredFlag,
            kCpHasSeqNumFlag,
            theCheckpointLoader
        );
        if (0 != theRet) {
            return theRet;
        }
        if (mCheckpoints.empty()) {
            KFS_LOG_STREAM_ERROR <<
                "no checkpoint exists: " << inCheckpointDirPtr <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        Checkpoint& theLatest = mCheckpoints.rbegin()->second;
        if (theCheckpointLoader.GetLatest() != theLatest.mLogSeq) {
            KFS_LOG_STREAM_ERROR <<
                "invalid or missing latest checkpoint link:" <<
                " higest checkpoint: " << theLatest.mLogSeq <<
                " link seq: " << theCheckpointLoader.GetLatest() <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        LogSegmentLoader  theLogSegmentLoader(*this, inLogDirPtr);
        const char* const kLastFileNamePtr      =
            MetaDataStore::GetLogSegmentLastFileNamePtr();
        const bool        kLastFileRequiredFlag = false;
        const bool        kHasSeqNumFlag        = true;
        theRet = LoadDir(
            inLogDirPtr,
            MetaDataStore::GetLogSegmentFileNamePrefixPtr(),
            kLastFileNamePtr,
            ".tmp",
            '.',
            inRemoveTmpFilesFlag,
            kLastFileRequiredFlag,
            kHasSeqNumFlag,
            theLogSegmentLoader
        );
        if (0 != theRet) {
            return theRet;
        }
        LogSegmentNums& theLogSegments = theLogSegmentLoader.GetLogSegments();
        if (theLogSegments.empty()) {
            KFS_LOG_STREAM_ERROR <<
                inLogDirPtr << ": no log segments found" <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        if (! theLogSegmentLoader.GetLast().IsValid() &&
                1 != theLogSegments.size()) {
            KFS_LOG_STREAM_ERROR <<
                inLogDirPtr << "/" << kLastFileNamePtr <<
                ": not found" <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        if (! theLatest.mLogEndSeq.IsValid()) {
            VrLogSeq theLogSeq;
            theLatest.mLogEndSeq = VrLogSeq(0, 0, GetLogSegmentNumber(
                theLatest.mFileName.c_str(),
                theLogSegmentLoader.GetTmpBufferPtr(),
                theLogSegmentLoader.GetTmpBufferSize(),
                theLogSeq
            ));
            if (! theLatest.mLogEndSeq.IsValid()) {
                return (int)theLatest.mLogEndSeq.mLogSeq;
            }
        }
        bool theCPLogFoundFlag = false;
        bool theCPLogSeqFlag   = false;
        LogSegmentNums::iterator thePrevIt = theLogSegments.begin();
        if (theLogSegments.end() != thePrevIt) {
            bool theMustHaveSeqFlag = thePrevIt->second.mLogEndSeq.IsValid();
            for (LogSegmentNums::iterator theIt = thePrevIt;
                    theLogSegments.end() != ++theIt;
                    thePrevIt = theIt) {
                if (thePrevIt->first == theLatest.mLogEndSeq.mLogSeq) {
                    theCPLogFoundFlag = true;
                    theCPLogSeqFlag = thePrevIt->second.mLogEndSeq.IsValid();
                }
                if (theMustHaveSeqFlag &&
                        ! theIt->second.mLogEndSeq.IsValid()) {
                    KFS_LOG_STREAM_ERROR <<
                        "invalid log segment with no log sequence:"
                        " name: "     << theIt->second.mFileName <<
                        " previous: " << thePrevIt->second.mFileName <<
                        " sequence: " << thePrevIt->second.mLogSeq <<
                        " : "         << thePrevIt->second.mLogEndSeq <<
                    KFS_LOG_EOM;
                    if (theLatest.mLogEndSeq.mLogSeq <= theIt->first ||
                            theLatest.mLogSeq <= theIt->second.mLogSeq) {
                        theRet = -EINVAL;
                        break;
                    }
                }
                theMustHaveSeqFlag = theMustHaveSeqFlag ||
                    thePrevIt->second.mLogEndSeq.IsValid();
                if (thePrevIt->first + 1 != theIt->first) {
                    KFS_LOG_STREAM_ERROR <<
                        "missing log segment:"
                        " name: "     << thePrevIt->second.mFileName <<
                        " next: "     << theIt->second.mFileName <<
                        " sequence: " << thePrevIt->second.mLogSeq <<
                        " : "         << thePrevIt->second.mLogEndSeq <<
                        " next: "     << theIt->second.mLogSeq <<
                    KFS_LOG_EOM;
                    if (! inIgnoreMissingSegmentsFlag ||
                            theLatest.mLogEndSeq.mLogSeq < theIt->first ||
                            theLatest.mLogSeq < theIt->second.mLogSeq) {
                        theRet = -EINVAL;
                        break;
                    }
                    // The segment end sequence cannot be determined without
                    // opending and reading end of segment.
                    thePrevIt->second.mLogEndSeq = thePrevIt->second.mLogSeq;
                } else if (! thePrevIt->second.mLogEndSeq.IsValid()) {
                    // Allow sequence gaps between old style segments, and set
                    // set end sequence equal to beginning sequence.
                    thePrevIt->second.mLogEndSeq = thePrevIt->second.mLogSeq;
                } else if (theIt->second.mLogSeq < thePrevIt->second.mLogSeq) {
                    KFS_LOG_STREAM_ERROR <<
                        "invalid log segments:"
                        " name: "     << thePrevIt->second.mFileName <<
                        " next: "     << theIt->second.mFileName <<
                        " sequence: " << thePrevIt->second.mLogSeq <<
                        " : "         << thePrevIt->second.mLogEndSeq <<
                        " next: "     << theIt->second.mLogSeq <<
                    KFS_LOG_EOM;
                    theRet = -EINVAL;
                    break;
                } else {
                    thePrevIt->second.mLogEndSeq = theIt->second.mLogSeq;
                }
                mLogSegments.insert(make_pair(
                    thePrevIt->second.mLogSeq, thePrevIt->second));
            }
            if (thePrevIt->first == theLatest.mLogEndSeq.mLogSeq) {
                theCPLogFoundFlag = true;
                theCPLogSeqFlag = thePrevIt->second.mLogEndSeq.IsValid();
            }
            thePrevIt->second.mLogEndSeq = VrLogSeq();
            mLogSegments.insert(make_pair(
                thePrevIt->second.mLogSeq, thePrevIt->second));
        }
        if (theCPLogFoundFlag) {
            KFS_LOG_STREAM_DEBUG <<
                "last checkpoint new log format: " << theCPLogSeqFlag <<
                " segment: " << theLatest.mLogEndSeq <<
            KFS_LOG_EOM;
            // Mark assign negative end sequence if checkpoint points to a
            // old style log segment to denote that checkpoint can not be used
            // to boot strap.
            if (! theCPLogSeqFlag) {
                theLatest.mLogEndSeq.mLogSeq =
                    -(theLatest.mLogEndSeq.mLogSeq + 1);
            }
        } else {
            KFS_LOG_STREAM_ERROR <<
                "no latest checkpoint log segment: " << theLatest.mLogSeq <<
            KFS_LOG_EOM;
            theRet = -EINVAL;
        }
        return theRet;
    }
    static seq_t GetLogSegmentNumber(
        const char* inNamePtr,
        char*       inReadBufPtr,
        size_t      inReadBufSize,
        VrLogSeq&   outLogSeq)
    {
        const int theFd = open(inNamePtr, O_RDONLY);
        if (theFd < 0) {
            const int theErr = errno;
            KFS_LOG_STREAM_ERROR <<
                "open: " << inNamePtr <<
                ": " << QCUtils::SysError(theErr) <<
            KFS_LOG_EOM;
            return (0 < theErr ? -theErr : -EINVAL);
        }
        seq_t         theRet   = -EINVAL;
        const ssize_t theRdLen = read(theFd, inReadBufPtr, inReadBufSize);
        if (theRdLen < 0) {
            const int theErr = errno;
            KFS_LOG_STREAM_ERROR <<
                "read: " << inNamePtr <<
                ": " << QCUtils::SysError(theErr) <<
            KFS_LOG_EOM;
            theRet = 0 < theErr ? -theErr : -EINVAL;
        } else {
            const char* theStPtr  = strstr(inReadBufPtr, "\nlog/");
            if (theStPtr) {
                theStPtr += 5;
            }
            const int         kDot      = '.';
            const char* const theEndPtr = theStPtr ? strchr(theStPtr, '\n') : 0;
            if (theEndPtr) {
                const char* thePtr    = theEndPtr - 1;
                int         theDotCnt = 0;
                while (theStPtr <= thePtr) {
                    const int theSym = *thePtr & 0xFF;
                    if (kDot == theSym) {
                        if (4 <= ++theDotCnt) {
                            break;
                        }
                    } else if (theSym < '0' || '9' < theSym) {
                        break;
                    }
                    --thePtr;
                }
                ++thePtr;
                if (4 <= theDotCnt) {
                    if (! ParseVrLogSeq(thePtr, theEndPtr, kDot, kDot,
                                outLogSeq) ||
                           ! ParseSeqNum(thePtr, theEndPtr, 0, theRet) ||
                           thePtr != theEndPtr) {
                        theRet = -EINVAL;
                    }
                } else if (1 == theDotCnt) {
                    if (! ParseSeqNum(thePtr, theEndPtr, 0, theRet)) {
                        theRet = -EINVAL;
                    }
                }
            }
            if (theRet < 0) {
                KFS_LOG_STREAM_INFO <<
                    "no start log segment found: " << inNamePtr <<
                KFS_LOG_EOM;
            }
        }
        close(theFd);
        return theRet;
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
    const char*         inFileNamePtr,
    const MetaVrLogSeq& inStartSeq,
    seq_t               inLogSegmentNumber)
{
    mImpl.RegisterCheckpoint(inFileNamePtr, inStartSeq, inLogSegmentNumber);
}

    void
MetaDataStore::RegisterLogSegment(
    const char*         inFileNamePtr,
    const MetaVrLogSeq& inStartSeq,
    seq_t               inLogSegmentNumber)
{
    mImpl.RegisterLogSegment(inFileNamePtr, inStartSeq, inLogSegmentNumber);
}

int
MetaDataStore::Load(
    const char* inCheckpointDirPtr,
    const char* inLogDirPtr,
    bool        inRemoveTmpFilesFlag,
    bool        inIgnoreMissingSegmentsFlag,
    const char* inMetaMdPtr)
{
    return mImpl.Load(
        inCheckpointDirPtr,
        inLogDirPtr,
        inRemoveTmpFilesFlag,
        inIgnoreMissingSegmentsFlag,
        inMetaMdPtr
    );
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

    void
MetaDataStore::PrepareToFork()
{
    mImpl.PrepareToFork();
}

    void
MetaDataStore::ForkDone()
{
    mImpl.ForkDone();
}

    void
MetaDataStore::ChildAtFork()
{
    mImpl.ChildAtFork();
}

    /* static */ bool
MetaDataStore::GetLogSequenceFromFileName(
    const char*   inFileNamePtr,
    size_t        inFileNameLen,
    MetaVrLogSeq& outLogSeq,
    seq_t*        outSegNumPtr)
{
    return Impl::GetLogSequenceFromFileName(
        inFileNamePtr, inFileNameLen, outLogSeq, outSegNumPtr);
}

} // namespace KFS
