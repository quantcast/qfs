//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/03/21
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
// Transaction log writer.
//
//
//----------------------------------------------------------------------------

#include "LogWriter.h"
#include "LogTransmitter.h"
#include "MetaRequest.h"
#include "MetaDataStore.h"
#include "MetaVrSM.h"
#include "Checkpoint.h"
#include "MetaVrLogSeq.h"
#include "MetaVrOps.h"
#include "Replay.h"
#include "util.h"

#include "common/MsgLogger.h"
#include "common/MdStream.h"
#include "common/kfserrno.h"
#include "common/RequestParser.h"
#include "common/SingleLinkedQueue.h"
#include "common/IntToString.h"

#include "kfsio/NetManager.h"
#include "kfsio/ITimeout.h"
#include "kfsio/checksum.h"
#include "kfsio/PrngIsaac64.h"

#include "qcdio/QCThread.h"
#include "qcdio/QCMutex.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"

#include <errno.h>
#include <unistd.h>
#include <fcntl.h>

namespace KFS
{
using std::ofstream;

class LogWriter::Impl :
    private ITimeout,
    private QCRunnable,
    private LogTransmitter::CommitObserver,
    private NetManager::Dispatcher
{
public:
    Impl()
        : ITimeout(),
          QCRunnable(),
          LogTransmitter::CommitObserver(),
          NetManager::Dispatcher(),
          mNetManagerPtr(0),
          mMetaDataStorePtr(0),
          mNetManager(),
          mLogTransmitter(mNetManager, *this),
          mMetaVrSM(mLogTransmitter),
          mVrStatus(0),
          mEnqueueVrStatus(0),
          mTransmitCommitted(),
          mTransmitterUpFlag(false),
          mMaxDoneLogSeq(),
          mCommitted(),
          mThread(),
          mMutex(),
          mStopFlag(false),
          mOmitDefaultsFlag(true),
          mCommitUpdatedFlag(false),
          mSetReplayStateFlag(false),
          mMaxBlockSize(256),
          mPendingCount(0),
          mLogDir("./kfslog"),
          mPendingQueue(),
          mInQueue(),
          mOutQueue(),
          mPendingAckQueue(),
          mReplayCommitQueue(),
          mPendingCommitted(),
          mInFlightCommitted(),
          mPendingReplayLogSeq(),
          mReplayLogSeq(),
          mNextLogSeq(),
          mNextBlockSeq(-1),
          mLastLogSeq(),
          mBlockChecksum(kKfsNullChecksum),
          mNextBlockChecksum(kKfsNullChecksum),
          mLogFd(-1),
          mError(0),
          mMdStream(
            this,
            false,     // inSyncFlag
            string(),  // inDigestName
            1 << 20,   // inBufSize,
            true       // inResizeFlag
            ),
          mReqOstream(mMdStream),
          mCurLogStartTime(-1),
          mCurLogStartSeq(),
          mLogNum(0),
          mLogName(),
          mWriteState(kWriteStateNone),
          mLogRotateInterval(600),
          mPanicOnIoErrorFlag(true),
          mSyncFlag(false),
          mWokenFlag(false),
          mLastLogPath(mLogDir + "/" +
            MetaDataStore::GetLogSegmentLastFileNamePtr()),
          mLogFilePos(0),
          mLogFilePrevPos(0),
          mLogFileMaxSize(8 << 20),
          mFailureSimulationInterval(0),
          mPrepareToForkFlag(false),
          mPrepareToForkDoneFlag(false),
          mLastLogReceivedTime(-1),
          mVrLastLogReceivedTime(-1),
          mPrepareToForkCond(),
          mForkDoneCond(),
          mRandom(),
          mLogFileNamePrefix("log")
        { mLogName.reserve(1 << 10); }
    ~Impl()
        { Impl::Shutdown(); }
    int Start(
        NetManager&         inNetManager,
        MetaDataStore&      inMetaDataStore,
        MetaDataSync&       inMetaDataSync,
        seq_t               inLogNum,
        const MetaVrLogSeq& inLogSeq,
        const MetaVrLogSeq& inCommittedLogSeq,
        fid_t               inCommittedFidSeed,
        int64_t             inCommittedErrCheckSum,
        int                 inCommittedStatus,
        const MdStateCtx*   inLogAppendMdStatePtr,
        const MetaVrLogSeq& inLogAppendStartSeq,
        seq_t               inLogAppendLastBlockSeq,
        bool                inLogAppendHexFlag,
        bool                inLogNameHasSeqFlag,
        const char*         inParametersPrefixPtr,
        const Properties&   inParameters,
        string&             outCurLogFileName)
    {
        if (inLogNum < 0 || ! inLogSeq.IsValid() ||
                (inLogAppendMdStatePtr && inLogSeq < inLogAppendStartSeq) ||
                (mThread.IsStarted() || mNetManagerPtr)) {
            return -EINVAL;
        }
        mNextBlockChecksum = ComputeBlockChecksum(kKfsNullChecksum, "\n", 1);
        mLogNum = inLogNum;
        const int theErr = SetParameters(inParametersPrefixPtr, inParameters);
        if (0 != theErr) {
            return theErr;
        }
        mMdStream.Reset(this);
        mCommitted.mErrChkSum = inCommittedErrCheckSum;
        mCommitted.mSeq       = inCommittedLogSeq;
        mCommitted.mFidSeed   = inCommittedFidSeed;
        mCommitted.mStatus    = inCommittedStatus;
        mReplayLogSeq         = inLogSeq;
        mPendingReplayLogSeq  = inLogSeq;
        mPendingCommitted  = mCommitted;
        mInFlightCommitted = mPendingCommitted;
        mMetaDataStorePtr  = &inMetaDataStore;
        if (0 != (mError = mMetaVrSM.Start(inMetaDataSync, mCommitted.mSeq))) {
            return mError;
        }
        if (inLogAppendMdStatePtr) {
            SetLogName(inLogSeq,
                inLogNameHasSeqFlag ? inLogAppendStartSeq : MetaVrLogSeq());
            mCurLogStartTime = mNetManager.Now();
            mCurLogStartSeq  = inLogAppendStartSeq;
            mMdStream.SetMdState(*inLogAppendMdStatePtr);
            if (! mMdStream) {
                KFS_LOG_STREAM_ERROR <<
                    "log append:" <<
                    " failed to set md context" <<
                KFS_LOG_EOM;
                return -EIO;
            }
            Close();
            mError = 0;
            if ((mLogFd = open(mLogName.c_str(), O_WRONLY, 0666)) < 0) {
                IoError(errno);
                return mError;
            }
            const off_t theSize = lseek(mLogFd, 0, SEEK_END);
            if (theSize < 0) {
                IoError(errno);
                return mError;
            }
            if (theSize == 0) {
                KFS_LOG_STREAM_ERROR <<
                    "log append: invalid empty file"
                    " file: " << mLogName <<
                    " size: " << theSize <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            KFS_LOG_STREAM_INFO <<
                "log append:" <<
                " idx: "      << mLogNum <<
                " start: "    << mCurLogStartSeq <<
                " cur: "      << mNextLogSeq <<
                " block: "    << inLogAppendLastBlockSeq <<
                " hex: "      << inLogAppendHexFlag <<
                " file: "     << mLogName <<
                " size: "     << theSize <<
                " checksum: " << mMdStream.GetMd() <<
            KFS_LOG_EOM;
            mMdStream.setf(
                inLogAppendHexFlag ? ostream::hex : ostream::dec,
                ostream::basefield
            );
            mLogFilePos     = theSize;
            mLogFilePrevPos = mLogFilePos;
            mNextBlockSeq = inLogAppendLastBlockSeq;
            if (inLogAppendLastBlockSeq < 0 || ! inLogAppendHexFlag ||
                    ! inLogNameHasSeqFlag) {
                // Previous / "old" log format.
                // Close the log segment even if it is empty and start new one.
                StartNextLog();
            } else {
                StartBlock(mNextBlockChecksum);
            }
        } else {
            NewLog(inLogSeq);
        }
        if (! IsLogStreamGood()) {
            return mError;
        }
        outCurLogFileName      = mLogName;
        mStopFlag              = false;
        mNetManagerPtr         = &inNetManager;
        mLastLogReceivedTime   = mNetManagerPtr->Now() - 365 * 24 * 60 * 60;
        mVrLastLogReceivedTime = mLastLogReceivedTime;
        const int kStackSize = 64 << 10;
        mThread.Start(this, kStackSize, "LogWriter");
        mNetManagerPtr->RegisterTimeoutHandler(this);
        return 0;
    }
    bool Enqueue(
        MetaRequest& inRequest)
    {
        inRequest.next = 0;
        if (mStopFlag) {
            inRequest.status    = -ELOGFAILED;
            inRequest.statusMsg = "log writer is not running";
            return false;
        }
        int* const theCounterPtr = inRequest.GetLogQueueCounter();
        if (((mPendingCount <= 0 ||
                    ! theCounterPtr || *theCounterPtr <= 0) &&
                (MetaRequest::kLogNever == inRequest.logAction ||
                (MetaRequest::kLogIfOk == inRequest.logAction &&
                    0 != inRequest.status))) && mEnqueueVrStatus == 0) {
            return false;
        }
        if (theCounterPtr) {
            if (++*theCounterPtr <= 0) {
                panic("request enqueue: invalid log queue counter");
            }
        }
        inRequest.commitPendingFlag = true;
        if (++mPendingCount <= 0) {
            panic("log writer: invalid pending count");
        }
        mPendingQueue.PushBack(inRequest);
        return true;
    }
    void RequestCommitted(
        MetaRequest& inRequest,
        fid_t        inFidSeed)
    {
        if (! inRequest.commitPendingFlag) {
            return;
        }
        int* const theCounterPtr = inRequest.GetLogQueueCounter();
        if (theCounterPtr) {
            if (--*theCounterPtr < 0) {
                panic("request committed: invalid log queue counter");
            }
        }
        inRequest.commitPendingFlag = false;
        if (! inRequest.logseq.IsValid()) {
            return;
        }
        if (inRequest.suspended) {
            panic("request committed: invalid suspended state");
        }
        if (mCommitted.mSeq.IsValid() && (inRequest.logseq <= mCommitted.mSeq ||
                (inRequest.logseq.mEpochSeq == mCommitted.mSeq.mEpochSeq &&
                inRequest.logseq.mViewSeq == mCommitted.mSeq.mViewSeq &&
                inRequest.logseq.mLogSeq != mCommitted.mSeq.mLogSeq + 1))) {
            panic("request committed: invalid out of order log sequence");
            return;
        }
        const int theStatus = inRequest.status < 0 ?
            SysToKfsErrno(-inRequest.status) : 0;
        mCommitted.mErrChkSum += theStatus;
        mCommitted.mSeq       = inRequest.logseq;
        mCommitted.mFidSeed   = inFidSeed;
        mCommitted.mStatus    = theStatus;
        if (0 != theStatus) {
            KFS_LOG_STREAM_DEBUG <<
                "committed:"
                " seq: "    << mCommitted.mSeq <<
                " seed: "   << mCommitted.mFidSeed <<
                " status: " << mCommitted.mStatus <<
                " / "       << inRequest.status <<
                " chksum: " << mCommitted.mErrChkSum <<
            KFS_LOG_EOM;
        }
        mCommitUpdatedFlag = true;
    }
    MetaVrLogSeq GetCommittedLogSeq() const
        { return mCommitted.mSeq; }
    void GetCommitted(
        MetaVrLogSeq& outLogSeq,
        int64_t&      outErrChecksum,
        fid_t&        outFidSeed,
        int&          outStatus) const
    {
        outLogSeq      = mCommitted.mSeq;
        outErrChecksum = mCommitted.mErrChkSum;
        outFidSeed     = mCommitted.mFidSeed;
        outStatus      = mCommitted.mStatus;
    }
    void SetCommitted(
        const MetaVrLogSeq& inLogSeq,
        int64_t             inErrChecksum,
        fid_t               inFidSeed,
        int                 inStatus,
        const MetaVrLogSeq& inLastReplayLogSeq)
    {
        mCommitted.mSeq       = inLogSeq;
        mCommitted.mErrChkSum = inErrChecksum;
        mCommitted.mFidSeed   = inFidSeed;
        mCommitted.mStatus    = inStatus;
        mReplayLogSeq         = inLastReplayLogSeq;
        mCommitUpdatedFlag    = true;
    }
    void ScheduleFlush()
    {
        if (mPendingQueue.IsEmpty() && ! mCommitUpdatedFlag) {
            return;
        }
        mCommitUpdatedFlag = false;
        QCStMutexLocker theLock(mMutex);
        mPendingCommitted = mCommitted;
        mPendingReplayLogSeq = mReplayLogSeq;
        mInQueue.PushBack(mPendingQueue);
        mVrLastLogReceivedTime = mLastLogReceivedTime;
        theLock.Unlock();
        mNetManager.Wakeup();
    }
    void Shutdown()
    {
        if (! mThread.IsStarted() || mStopFlag) {
            return;
        }
        QCStMutexLocker theLock(mMutex);
        // Mark everything committed to cleanup queues.
        mTransmitCommitted = mNextLogSeq;
        mStopFlag = true;
        mNetManager.Wakeup();
        theLock.Unlock();
        mThread.Join();
        if (mNetManagerPtr) {
            mNetManagerPtr->UnRegisterTimeoutHandler(this);
            mNetManagerPtr = 0;
        }
    }
    void PrepareToFork()
    {
        QCStMutexLocker theLocker(mMutex);
        if (mPrepareToForkFlag) {
            panic("log writer: invalid prepare to fork invocation");
            return;
        }
        mPrepareToForkFlag = true;
        mNetManager.Wakeup();
        while (! mPrepareToForkDoneFlag) {
            mPrepareToForkCond.Wait(mMutex);
        }
    }
    void ForkDone()
    {
        QCStMutexLocker theLocker(mMutex);
        if (! mPrepareToForkDoneFlag || ! mPrepareToForkFlag) {
            panic("log writer: invalid fork done invocation");
            return;
        }
        mPrepareToForkDoneFlag = false;
        mPrepareToForkFlag     = false;
        mForkDoneCond.Notify();
    }
    void ChildAtFork()
    {
        mNetManager.ChildAtFork();
        Close();
    }
    virtual void Notify(
        const MetaVrLogSeq& inSeq)
    {
        const bool theWakeupFlag = mTransmitCommitted < inSeq &&
            ! mWokenFlag;
        mMetaVrSM.Commit(inSeq);
        mTransmitCommitted = inSeq;
        mTransmitterUpFlag = mLogTransmitter.IsUp();
        if (theWakeupFlag) {
            mWokenFlag = true;
            // mNetManager.Wakeup();
        }
    }
    void SetLastLogReceivedTime(
        time_t inTime)
        { mLastLogReceivedTime = inTime; }
    MetaVrSM& GetMetaVrSM()
        { return mMetaVrSM; }
private:
    typedef uint32_t Checksum;
    class Committed
    {
    public:
        MetaVrLogSeq mSeq;
        fid_t        mFidSeed;
        int64_t      mErrChkSum;
        int          mStatus;
        Committed()
            : mSeq(),
              mFidSeed(-1),
              mErrChkSum(0),
              mStatus(0)
            {}
    };
    typedef SingleLinkedQueue<MetaRequest, MetaRequest::GetNext> Queue;
    typedef MdStreamT<Impl> MdStream;
    enum WriteState
    {
        kWriteStateNone,
        kUpdateBlockChecksum
    };

    NetManager*    mNetManagerPtr;
    MetaDataStore* mMetaDataStorePtr;
    NetManager     mNetManager;
    LogTransmitter mLogTransmitter;
    MetaVrSM       mMetaVrSM;
    int            mVrStatus;
    volatile int   mEnqueueVrStatus;
    MetaVrLogSeq   mTransmitCommitted;
    bool           mTransmitterUpFlag;
    MetaVrLogSeq   mMaxDoneLogSeq;
    Committed      mCommitted;
    QCThread       mThread;
    QCMutex        mMutex;
    bool           mStopFlag;
    bool           mOmitDefaultsFlag;
    bool           mCommitUpdatedFlag;
    bool           mSetReplayStateFlag;
    int            mMaxBlockSize;
    int            mPendingCount;
    string         mLogDir;
    Queue          mPendingQueue;
    Queue          mInQueue;
    Queue          mOutQueue;
    Queue          mPendingAckQueue;
    Queue          mReplayCommitQueue;
    Committed      mPendingCommitted;
    Committed      mInFlightCommitted;
    MetaVrLogSeq   mPendingReplayLogSeq;
    MetaVrLogSeq   mReplayLogSeq;
    MetaVrLogSeq   mNextLogSeq;
    seq_t          mNextBlockSeq;
    MetaVrLogSeq   mLastLogSeq;
    Checksum       mBlockChecksum;
    Checksum       mNextBlockChecksum;
    int            mLogFd;
    int            mError;
    MdStream       mMdStream;
    ReqOstream     mReqOstream;
    time_t         mCurLogStartTime;
    MetaVrLogSeq   mCurLogStartSeq;
    seq_t          mLogNum;
    string         mLogName;
    WriteState     mWriteState;
    time_t         mLogRotateInterval;
    bool           mPanicOnIoErrorFlag;
    bool           mSyncFlag;
    bool           mWokenFlag;
    string         mLastLogPath;
    int64_t        mLogFilePos;
    int64_t        mLogFilePrevPos;
    int64_t        mLogFileMaxSize;
    int64_t        mFailureSimulationInterval;
    bool           mPrepareToForkFlag;
    bool           mPrepareToForkDoneFlag;
    time_t         mLastLogReceivedTime;
    time_t         mVrLastLogReceivedTime;
    QCCondVar      mPrepareToForkCond;
    QCCondVar      mForkDoneCond;
    PrngIsaac64    mRandom;
    const string   mLogFileNamePrefix;

    virtual void Timeout()
    {
        if (mPendingCount <= 0 && ! mSetReplayStateFlag) {
            return;
        }
        Queue theDoneQueue;
        QCStMutexLocker theLock(mMutex);
        theDoneQueue.Swap(mOutQueue);
        MetaRequest* const theReplayCommitHeadPtr =
            mReplayCommitQueue.Front();
        mReplayCommitQueue.Reset();
        const bool theSetReplayStateFlag =
            mSetReplayStateFlag || theReplayCommitHeadPtr;
        mSetReplayStateFlag = false;
        theLock.Unlock();
        MetaRequest* thePtr;
        while ((thePtr = theDoneQueue.PopFront())) {
            MetaRequest& theReq = *thePtr;
            if (theReq.logseq.IsValid()) {
                if (theReq.logseq <= mMaxDoneLogSeq) {
                    panic("log writer: invalid log sequence number");
                }
                mMaxDoneLogSeq = theReq.logseq;
            }
            if (--mPendingCount < 0) {
                panic("log writer: request completion invalid pending count");
            }
            submit_request(&theReq);
        }
        if (theSetReplayStateFlag && ! replayer.setReplayState(
                mCommitted.mSeq,
                mCommitted.mErrChkSum,
                mCommitted.mStatus,
                theReplayCommitHeadPtr)) {
            panic("log writer: set replay state failed");
        }
    }
    void ProcessPendingAckQueue(
        Queue& inDoneQueue,
        bool   inSetReplayStateFlag)
    {
        mWokenFlag = false;
        mPendingAckQueue.PushBack(inDoneQueue);
        MetaRequest* thePtr = 0;
        if (mTransmitCommitted < mNextLogSeq) {
            thePtr = mPendingAckQueue.Front();
            MetaRequest* thePrevPtr = 0;
            while (thePtr) {
                if (mTransmitCommitted < thePtr->logseq &&
                        0 == thePtr->status) {
                    if (thePrevPtr) {
                        thePrevPtr->next = 0;
                        inDoneQueue.Set(mPendingAckQueue.Front(), thePrevPtr);
                        mPendingAckQueue.Set(thePtr, mPendingAckQueue.Back());
                    }
                    break;
                }
                thePrevPtr = thePtr;
                thePtr     = thePtr->next;
            }
        }
        if (! thePtr) {
            inDoneQueue.PushBack(mPendingAckQueue);
        }
        if (inDoneQueue.IsEmpty() &&
                (0 == mVrStatus || mPendingAckQueue.IsEmpty())) {
            return;
        }
        QCStMutexLocker theLocker(mMutex);
        mOutQueue.PushBack(inDoneQueue);
        if (0 != mVrStatus) {
            mReplayCommitQueue.PushBack(mPendingAckQueue);
            mSetReplayStateFlag =
                inSetReplayStateFlag || ! mReplayCommitQueue.IsEmpty();
        }
        mNetManagerPtr->Wakeup();
    }
    virtual void DispatchStart()
    {
        QCStMutexLocker theLocker(mMutex);
        if (mPrepareToForkFlag) {
            mPrepareToForkDoneFlag = true;
            mPrepareToForkCond.Notify();
            while (mPrepareToForkFlag) {
                mForkDoneCond.Wait(mMutex);
            }
        }
        const bool theStopFlag = mStopFlag;
        if (theStopFlag) {
            mNetManager.Shutdown();
        }
        mInFlightCommitted = mPendingCommitted;
        const time_t       theVrLastLogReceivedTime = mVrLastLogReceivedTime;
        const MetaVrLogSeq theReplayLogSeq          = mPendingReplayLogSeq;
        Queue              theWriteQueue;
        mInQueue.Swap(theWriteQueue);
        theLocker.Unlock();
        mWokenFlag = true;
        if (theStopFlag) {
            mMetaVrSM.Shutdown();
        }
        int theVrStatus = mVrStatus;
        MetaRequest* theReqPtr = 0;
        mMetaVrSM.Process(mNetManager.Now(), theVrLastLogReceivedTime,
            mInFlightCommitted.mSeq, theReplayLogSeq, theVrStatus, theReqPtr);
        if (theReqPtr) {
            theWriteQueue.PushBack(*theReqPtr);
        }
        const bool theVrBecameNonPrimaryFlag =
            0 != theVrStatus && 0 == mVrStatus;
        mVrStatus = theVrStatus;
        if (theVrBecameNonPrimaryFlag && 0 == mEnqueueVrStatus) {
            mEnqueueVrStatus = theVrStatus - 1;
            SyncAddAndFetch(mEnqueueVrStatus, 1);
        }
        if (! theWriteQueue.IsEmpty()) {
            Write(*theWriteQueue.Front());
        }
        ProcessPendingAckQueue(theWriteQueue, theVrBecameNonPrimaryFlag);
    }
    virtual void DispatchEnd()
    {
        if (mWokenFlag) {
            Queue theTmp;
            ProcessPendingAckQueue(theTmp, false);
        }
    }
    virtual void DispatchExit()
        {}
    virtual void Run()
    {
        QCMutex* const kMutexPtr             = 0;
        bool const     kWakeupAndCleanupFlag = true;
        mNetManager.MainLoop(kMutexPtr, kWakeupAndCleanupFlag, this);
        Sync();
        Close();
    }
    void Write(
        MetaRequest& inHead)
    {
        if (! IsLogStreamGood()) {
            if (mCurLogStartSeq < mNextLogSeq) {
                StartNextLog();
            } else {
                NewLog(mNextLogSeq);
            }
        }
        if (! mTransmitterUpFlag) {
            mTransmitterUpFlag = mLogTransmitter.IsUp();
        }
        mMdStream.SetSync(false);
        ostream&     theStream = mMdStream;
        MetaRequest* theCurPtr = &inHead;
        while (theCurPtr) {
            mLastLogSeq = mNextLogSeq;
            MetaRequest*          thePtr                 = theCurPtr;
            seq_t                 theEndBlockSeq         =
                mNextLogSeq.mLogSeq + mMaxBlockSize;
            const bool            theSimulateFailureFlag = IsSimulateFailure();
            const bool            theTransmitterUpFlag   =
                mTransmitterUpFlag && 0 == mVrStatus;
            MetaLogWriterControl* theCtlPtr              = 0;
            for ( ; thePtr; thePtr = thePtr->next) {
                if (mMetaVrSM.Handle(*thePtr, mLastLogSeq)) {
                    continue;
                }
                if (META_LOG_WRITER_CONTROL == thePtr->op) {
                    theCtlPtr = static_cast<MetaLogWriterControl*>(thePtr);
                    if (Control(*theCtlPtr)) {
                        break;
                    }
                    theCtlPtr = 0;
                    theEndBlockSeq = mNextLogSeq.mLogSeq + mMaxBlockSize;
                    continue;
                }
                if (META_VR_LOG_START_VIEW == thePtr->op && theStream) {
                    if (thePtr != theCurPtr) {
                        break;
                    }
                } else if (! theStream || ! theTransmitterUpFlag) {
                    continue;
                }
                if (((MetaRequest::kLogIfOk == thePtr->logAction &&
                            0 == thePtr->status) ||
                        MetaRequest::kLogAlways == thePtr->logAction)) {
                    if (theSimulateFailureFlag) {
                        KFS_LOG_STREAM_ERROR <<
                            "log writer: simulating write error:"
                            " " << thePtr->Show() <<
                        KFS_LOG_EOM;
                        break;
                    }
                    ++mLastLogSeq.mLogSeq;
                    thePtr->logseq = mLastLogSeq;
                    if (! thePtr->WriteLog(theStream, mOmitDefaultsFlag)) {
                        panic("log writer: invalid request ");
                    }
                    if (! theStream) {
                        --mLastLogSeq.mLogSeq;
                        LogError(*thePtr);
                    }
                }
                if (theEndBlockSeq <= mLastLogSeq.mLogSeq) {
                    break;
                }
                if (mMdStream.GetBufferedStart() +
                            mMdStream.GetBufferSize() / 4 * 3 <
                        mMdStream.GetBufferedEnd()) {
                    break;
                }
            }
            MetaRequest* const theEndPtr = thePtr ? thePtr->next : thePtr;
            if (mNextLogSeq < mLastLogSeq &&
                    (theTransmitterUpFlag ||
                        META_VR_LOG_START_VIEW == theCurPtr->op) &&
                    IsLogStreamGood()) {
                const int theBlkLen =
                    (int)(mLastLogSeq.mLogSeq - mNextLogSeq.mLogSeq);
                if (META_VR_LOG_START_VIEW == theCurPtr->op) {
                    MetaVrLogStartView& theOp =
                        *static_cast<MetaVrLogStartView*>(theCurPtr);
                    mLastLogSeq    = theOp.mNewLogSeq;
                    thePtr->logseq = mLastLogSeq;
                }
                FlushBlock(mLastLogSeq, theBlkLen);
            }
            if (IsLogStreamGood() && ! theSimulateFailureFlag &&
                    (theTransmitterUpFlag ||
                        META_VR_LOG_START_VIEW == theCurPtr->op)) {
                mNextLogSeq = mLastLogSeq;
                if (META_VR_LOG_START_VIEW == theCurPtr->op) {
                    mVrStatus = mMetaVrSM.GetStatus();
                    if (0 == mVrStatus) {
                        mEnqueueVrStatus = 0;
                    }
                    mNetManager.Wakeup();
                }
            } else {
                mLastLogSeq = mNextLogSeq;
                // Write failure.
                for (thePtr = theCurPtr;
                        theEndPtr != thePtr;
                        thePtr = thePtr->next) {
                    if (META_LOG_WRITER_CONTROL != thePtr->op &&
                            (((MetaRequest::kLogIfOk == thePtr->logAction &&
                                0 == thePtr->status) ||
                            MetaRequest::kLogAlways == thePtr->logAction) ||
                                0 != mVrStatus)) {
                        LogError(*thePtr);
                    }
                }
            }
            if (theCtlPtr &&
                    MetaLogWriterControl::kWriteBlock == theCtlPtr->type) {
                WriteBlock(*theCtlPtr);
            }
            theCurPtr = theEndPtr;
        }
        if (mCurLogStartSeq < mNextLogSeq && IsLogStreamGood() &&
                (mLogFileMaxSize <= mLogFilePos ||
                mCurLogStartTime + mLogRotateInterval < mNetManager.Now())) {
            StartNextLog();
        }
    }
    void StartNextLog()
    {
        CloseLog();
        mLogNum++;
        NewLog(mLastLogSeq);
    }
    void LogError(
        MetaRequest& inReq)
    {
        inReq.logseq = MetaVrLogSeq();
        if (-EVRNOTPRIMARY == mVrStatus) {
            inReq.status    = mVrStatus;
            inReq.statusMsg = "not primary";
        } else {
            inReq.status    = -ELOGFAILED;
            inReq.statusMsg = "transaction log write error";
        }
    }
    void StartBlock(
        Checksum inStartCheckSum)
    {
        mMdStream.SetSync(false);
        mWriteState    = kUpdateBlockChecksum;
        mBlockChecksum = inStartCheckSum;
    }
    void FlushBlock(
        const MetaVrLogSeq& inLogSeq,
        int                 inBlockLen = -1)
    {
        const int theVrStatus = mMetaVrSM.HandleLogBlock(
            mNextLogSeq,
            inLogSeq,
            mInFlightCommitted.mSeq
        );
        const int theBlockLen = inBlockLen < 0 ?
            (int)(inLogSeq.mLogSeq - mNextLogSeq.mLogSeq) : inBlockLen;
        ++mNextBlockSeq;
        mReqOstream << "c"
            "/" << mInFlightCommitted.mSeq <<
            "/" << mInFlightCommitted.mFidSeed <<
            "/" << mInFlightCommitted.mErrChkSum <<
            "/" << mInFlightCommitted.mStatus <<
            "/" << inLogSeq <<
            "/" << theBlockLen <<
            "/"
        ;
        mReqOstream.flush();
        const char*  theStartPtr = mMdStream.GetBufferedStart();
        const size_t theTxLen    = mMdStream.GetBufferedEnd() - theStartPtr;
        mBlockChecksum = ComputeBlockChecksum(
            mBlockChecksum, theStartPtr, theTxLen);
        Checksum const theTxChecksum = mBlockChecksum;
        mReqOstream <<
            mNextBlockSeq <<
            "/";
        mReqOstream.flush();
        theStartPtr = mMdStream.GetBufferedStart() + theTxLen;
        mBlockChecksum = ComputeBlockChecksum(
            mBlockChecksum,
            theStartPtr,
            mMdStream.GetBufferedEnd() - theStartPtr);
        mWriteState = kWriteStateNone;
        mReqOstream << mBlockChecksum << "\n";
        mReqOstream.flush();
        // Transmit all blocks, except first one, which has only block header.
        if (0 < mNextBlockSeq && 0 == theVrStatus) {
            theStartPtr = mMdStream.GetBufferedStart();
            int theStatus;
            if (mMdStream.GetBufferedEnd() < theStartPtr + theTxLen) {
                panic("invalid log write write buffer length");
                theStatus = -EFAULT;
            } else {
                theStatus = mLogTransmitter.TransmitBlock(
                    inLogSeq,
                    theBlockLen,
                    theStartPtr,
                    theTxLen,
                    theTxChecksum,
                    theTxLen
                );
            }
            KFS_LOG_STREAM_DEBUG <<
                "flush log block: block transmit" <<
                    (0 == theStatus ? "OK" : "failure") <<
                " seq: "    << inLogSeq  <<
                " status: " << theStatus <<
                (theStatus < 0 ?
                    " " + ErrorCodeToString(theStatus) : string()) <<
            KFS_LOG_EOM;
        }
        LogStreamFlush();
        mMetaVrSM.LogBlockWriteDone(
            mNextLogSeq,
            inLogSeq,
            mInFlightCommitted.mSeq,
            IsLogStreamGood()
        );
        StartBlock(mNextBlockChecksum);
    }
    void LogStreamFlush()
    {
        mMdStream.SetSync(true);
        mLogFilePrevPos = mLogFilePos;
        mReqOstream.flush();
        Sync();
    }
    void Sync()
    {
        if (mLogFd < 0 || ! mSyncFlag) {
            return;
        }
        if (fsync(mLogFd)) {
            IoError(-errno);
        }
    }
    void IoError(
        int         inError,
        const char* inMsgPtr = 0)
    {
        mError = inError;
        if (0 < mError) {
            mError = -mError;
        } else if (mError == 0) {
            mError = -EIO;
        }
        KFS_LOG_STREAM_ERROR <<
            (inMsgPtr ? inMsgPtr : "transaction log writer error:") <<
             " " << mLogName << ": " << QCUtils::SysError(inError) <<
        KFS_LOG_EOM;
        if (mPanicOnIoErrorFlag) {
            panic("transaction log io failure");
        }
    }
    void IoError(
        int           inError,
        const string& inMsg)
        { IoError(inError, inMsg.c_str()); }
    bool Control(
        MetaLogWriterControl& inRequest)
    {
        KFS_LOG_STREAM_DEBUG <<
            inRequest.Show() <<
        KFS_LOG_EOM;
        bool theRetFlag = true;
        switch (inRequest.type) {
            default:
            case MetaLogWriterControl::kNop:
                theRetFlag = false;
                break;
            case MetaLogWriterControl::kCheckpointNewLog:
                mMetaVrSM.Checkpoint(
                    Checkpoint::kHexIntFormatFlag, inRequest.vrCheckpont);
                // Fall through.
            case MetaLogWriterControl::kNewLog:
                if (mCurLogStartSeq < mLastLogSeq) {
                    StartNextLog();
                }
                break;
            case MetaLogWriterControl::kWriteBlock:
                return true;
            case MetaLogWriterControl::kSetParameters:
                SetParameters(
                    inRequest.paramsPrefix.c_str(),
                    inRequest.params
                );
                return false; // Do not start new record block.
        }
        inRequest.committed     = mInFlightCommitted.mSeq;
        inRequest.lastLogSeq    = mLastLogSeq;
        inRequest.logName       = mLogName;
        inRequest.logSegmentNum = mLogNum;
        return (theRetFlag && IsLogStreamGood());
    }
    void WriteBlock(
        MetaLogWriterControl& inRequest)
    {
        if (inRequest.blockData.BytesConsumable() <= 0) {
            panic("write block: invalid block length");
            inRequest.status = -EFAULT;
            return;
        }
        if (inRequest.blockLines.IsEmpty()) {
            panic("write block: invalid invocation, no log lines");
            inRequest.status = -EFAULT;
            return;
        }
        if (mLastLogSeq != mNextLogSeq) {
            panic("invalid write block invocation");
            inRequest.status = -EFAULT;
            return;
        }
        inRequest.committed = mLastLogSeq;
        if (inRequest.blockStartSeq != mLastLogSeq) {
            inRequest.status    = -EINVAL;
            inRequest.statusMsg = "invalid block start sequence";
            return;
        }
        if (! IsLogStreamGood()) {
            inRequest.status    = -EIO;
            inRequest.statusMsg = "log write error";
            return;
        }
        // Copy block data, and write block sequence and updated checksum.
        mMdStream.SetSync(false);
        mWriteState = kWriteStateNone;
        // To include leading \n, if any, "combine" block checksum.
        mBlockChecksum = ChecksumBlocksCombine(
            mBlockChecksum,
            inRequest.blockChecksum,
            inRequest.blockData.BytesConsumable()
        );
        const size_t thePos = mMdStream.GetBufferedEnd() -
            mMdStream.GetBufferedStart();
        for (IOBuffer::iterator theIt = inRequest.blockData.begin();
                theIt != inRequest.blockData.end();
                ++theIt) {
            const char* const thePtr = theIt->Consumer();
            mReqOstream.write(thePtr, theIt->Producer() - thePtr);
        }
        const size_t theLen = mMdStream.GetBufferedEnd() -
            (mMdStream.GetBufferedStart() + thePos);
        ++mNextBlockSeq;
        mReqOstream <<
            mNextBlockSeq <<
            "/";
        mReqOstream.flush();
        const char* thePtr        = mMdStream.GetBufferedStart() +
            thePos + theLen;
        size_t      theTrailerLen = mMdStream.GetBufferedEnd() - thePtr;
        mBlockChecksum = ComputeBlockChecksum(
            mBlockChecksum, thePtr, theTrailerLen);
        mReqOstream << mBlockChecksum << "\n";
        mReqOstream.flush();
        // Append trailer to make block replay work, and parse committed.
        thePtr        = mMdStream.GetBufferedStart() + thePos + theLen;
        theTrailerLen = mMdStream.GetBufferedEnd() - thePtr;
        inRequest.blockData.CopyIn(thePtr, theTrailerLen);
        const char* const theEndPtr = thePtr;
        thePtr = theEndPtr - inRequest.blockLines.Back();
        inRequest.blockLines.Back() += theTrailerLen;
        inRequest.blockCommitted = MetaVrLogSeq();
        MetaVrLogSeq theLogSeq;
        int          theBlockLen = -1;
        Committed    theBlockCommitted;
        if (thePtr + 2 < theEndPtr &&
                (*thePtr & 0xFF) == 'c' && (thePtr[1] & 0xFF) == '/') {
            thePtr += 2;
            if (ParseField(thePtr, theEndPtr, theBlockCommitted.mSeq) &&
                    ParseField(thePtr, theEndPtr, theBlockCommitted.mFidSeed) &&
                    ParseField(thePtr, theEndPtr, theBlockCommitted.mErrChkSum) &&
                    ParseField(thePtr, theEndPtr, theBlockCommitted.mStatus) &&
                    ParseField(thePtr, theEndPtr, theLogSeq) &&
                    ParseField(thePtr, theEndPtr, theBlockLen) &&
                    theBlockCommitted.mSeq.IsValid() &&
                    0 <= theBlockCommitted.mStatus &&
                    theBlockCommitted.mSeq <= theLogSeq &&
                    theLogSeq == inRequest.blockEndSeq &&
                    inRequest.blockStartSeq.mLogSeq + theBlockLen ==
                        inRequest.blockEndSeq.mLogSeq) {
                inRequest.blockCommitted = theBlockCommitted.mSeq;
            }
        }
        if (! inRequest.blockCommitted.IsValid()) {
            mMdStream.ClearBuffer();
            --mNextBlockSeq;
            inRequest.status    = -EINVAL;
            inRequest.statusMsg = "log write: invalid block format";
            return;
        }
        const int theVrStatus = mMetaVrSM.HandleLogBlock(
            inRequest.blockStartSeq,
            inRequest.blockEndSeq,
            theBlockCommitted.mSeq
        );
        if (0 == theVrStatus) {
            const int theStatus = mLogTransmitter.TransmitBlock(
                    inRequest.blockEndSeq,
                    (int)(inRequest.blockEndSeq.mLogSeq -
                        inRequest.blockStartSeq.mLogSeq),
                    mMdStream.GetBufferedStart() + thePos,
                    theLen,
                    inRequest.blockChecksum,
                    theLen
                );
            KFS_LOG_STREAM_DEBUG <<
                "write block: block transmit" <<
                    (0 == theStatus ? "OK" : "failure") <<
                ": ["   << inRequest.blockStartSeq  <<
                ":"     << inRequest.blockEndSeq <<
                "]"
                " status: " << theStatus <<
                (theStatus < 0 ?
                    " " + ErrorCodeToString(theStatus) : string()) <<
            KFS_LOG_EOM;
        }
        LogStreamFlush();
        const bool theStreamGoodFlag = IsLogStreamGood();
        mMetaVrSM.LogBlockWriteDone(
            inRequest.blockStartSeq,
            inRequest.blockEndSeq,
            theBlockCommitted.mSeq,
            theStreamGoodFlag
        );
        if (theStreamGoodFlag) {
            inRequest.blockSeq  = mNextBlockSeq;
            mLastLogSeq         = inRequest.blockEndSeq;
            mNextLogSeq         = mLastLogSeq;
            inRequest.status    = 0;
            inRequest.committed = mLastLogSeq;
            mInFlightCommitted  = theBlockCommitted;
            StartBlock(mNextBlockChecksum);
        } else {
            inRequest.status    = -EIO;
            inRequest.statusMsg = "log write error";
        }
    }
    class FieldParser
    {
    public:
        template<typename T>
        static bool Parse(
            const char*& ioPtr,
            size_t       inLen,
            T&           outVal)
            { return HexIntParser::Parse(ioPtr, inLen, outVal); }
        static bool Parse(
            const char*&  ioPtr,
            size_t        inLen,
            MetaVrLogSeq& outVal)
            { return outVal.Parse<HexIntParser>(ioPtr, inLen); }
    };
    template<typename T>
    bool ParseField(
        const char*& ioPtr,
        const char*  inEndPtr,
        T&           outVal)
    {
        const char* theStartPtr = ioPtr;
        while (ioPtr < inEndPtr && (*ioPtr & 0xFF) != '/') {
            ++ioPtr;
        }
        if (ioPtr < inEndPtr &&
                FieldParser::Parse(theStartPtr, ioPtr - theStartPtr, outVal)) {
            ++ioPtr;
            return true;
        }
        return false;
    }
    void CloseLog()
    {
        if (IsLogStreamGood()) {
            if (mLastLogSeq != mNextLogSeq) {
                FlushBlock(mLastLogSeq);
                if (! IsLogStreamGood()) {
                    mLastLogSeq = mNextLogSeq;
                    return;
                }
            }
            mWriteState = kWriteStateNone;
            mMdStream << "time/" << DisplayIsoDateTime() << "\n";
            const string theChecksum = mMdStream.GetMd();
            mMdStream << "checksum/" << theChecksum << "\n";
            LogStreamFlush();
        } else {
            mLastLogSeq = mNextLogSeq;
            Sync();
        }
        if (0 <= mLogFd && Close() && link_latest(mLogName, mLastLogPath)) {
            IoError(errno, "failed to link to: " + mLastLogPath);
        }
    }
    void NewLog(
        const MetaVrLogSeq& inLogSeq)
    {
        Close();
        mCurLogStartTime = mNetManager.Now();
        mLogFilePos      = 0;
        mLogFilePrevPos  = 0;
        mNextBlockSeq    = -1;
        mError           = 0;
        mWriteState      = kWriteStateNone;
        SetLogName(inLogSeq);
        if ((mLogFd = open(
                mLogName.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0666)) < 0) {
            IoError(errno);
            return;
        }
        StartBlock(kKfsNullChecksum);
        mMdStream.Reset(this);
        mMdStream.clear();
        mReqOstream.Get().clear();
        mMdStream.setf(ostream::dec, ostream::basefield);
        mMdStream.SetSync(false);
        mMdStream <<
            "version/" << int(LogWriter::VERSION) << "\n"
            "checksum/last-line\n"
            "setintbase/16\n"
            "time/" << DisplayIsoDateTime() << "\n"
        ;
        mMdStream.setf(ostream::hex, ostream::basefield);
        FlushBlock(mLastLogSeq);
        if (IsLogStreamGood()) {
            mNextLogSeq = mLastLogSeq;
            mMetaDataStorePtr->RegisterLogSegment(
                mLogName.c_str(), mCurLogStartSeq, mLogNum);
        } else {
            mLastLogSeq = mNextLogSeq;
        }
    }
    void SetLogName(
        const MetaVrLogSeq& inLogSeq)
        { SetLogName(inLogSeq, inLogSeq); }
    void SetLogName(
        const MetaVrLogSeq& inLogSeq,
        const MetaVrLogSeq& inLogStartSeqNum)
    {
        mCurLogStartSeq = inLogSeq;
        mNextLogSeq     = inLogSeq;
        mLastLogSeq     = inLogSeq;
        mLogName.assign(mLogDir.data(), mLogDir.size());
        if (! mLogName.empty() && '/' != *mLogName.rbegin()) {
            mLogName += '/';
        }
        mLogName += mLogFileNamePrefix;
        if (inLogStartSeqNum.IsValid()) {
            mLogName += '.';
            AppendDecIntToString(mLogName, inLogStartSeqNum.mEpochSeq);
            mLogName += '.';
            AppendDecIntToString(mLogName, inLogStartSeqNum.mViewSeq);
            mLogName += '.';
            AppendDecIntToString(mLogName, inLogStartSeqNum.mLogSeq);
        }
        mLogName += '.';
        AppendDecIntToString(mLogName, mLogNum);
    }
    int SetParameters(
        const char*       inParametersPrefixPtr,
        const Properties& inParameters)
    {
        Properties::String theName(
            inParametersPrefixPtr ? inParametersPrefixPtr : "");
        const size_t       thePrefixLen = theName.length();
        mOmitDefaultsFlag = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("omitDefaults"),
            mOmitDefaultsFlag ? 1 : 0) != 0;
        mMaxBlockSize = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("maxBlockSize"),
            mMaxBlockSize);
        mLogDir = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("logDir"),
            mLogDir);
        mLogRotateInterval = (time_t)max(4., inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("rotateIntervalSec"),
            mLogRotateInterval * 1.));
        mPanicOnIoErrorFlag = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("panicOnIoError"),
            mPanicOnIoErrorFlag ? 1 : 0) != 0;
        mSyncFlag = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("sync"),
            mSyncFlag ? 1 : 0) != 0;
        mFailureSimulationInterval = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("failureSimulationInterval"),
            mFailureSimulationInterval);
        mLastLogPath = mLogDir + "/" + MetaDataStore::GetLogSegmentLastFileNamePtr();
        mLogFileMaxSize = max(int64_t(64 << 10), inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("logFileMaxSize"),
            mLogFileMaxSize));
        const int theVrStatus = mMetaVrSM.SetParameters(
            theName.Truncate(thePrefixLen).Append("Vr.").c_str(),
            inParameters
        );
        const int theStatus = mLogTransmitter.SetParameters(
            theName.Truncate(thePrefixLen).Append("transmitter.").c_str(),
            inParameters
        );
        return (0 == theStatus ? theVrStatus : theStatus);
    }
    bool IsLogStreamGood()
    {
        if (0 != mError || mLogFd < 0) {
            return false;
        }
        if (! mMdStream) {
            IoError(EIO, "log md5 failure");
            return false;
        }
        return true;
    }
    // File write methods.
    friend class MdStreamT<Impl>;
    bool write(
        const char* inBufPtr,
        size_t      inSize)
    {
        if (! mMdStream.IsSync()) {
            panic("invalid write invocation");
            return false;
        }
        if (kUpdateBlockChecksum == mWriteState) {
            mBlockChecksum = ComputeBlockChecksum(
                mBlockChecksum, inBufPtr, inSize);
        }
        if (0 < inSize && IsLogStreamGood()) {
            const char*       thePtr    = inBufPtr;
            const char* const theEndPtr = thePtr + inSize;
            ssize_t           theNWr    = 0;
            while (thePtr < theEndPtr && 0 <= (theNWr = ::write(
                        mLogFd, inBufPtr, theEndPtr - thePtr))) {
                thePtr      += theNWr;
                mLogFilePos += theNWr;
            }
            if (theNWr < 0) {
                const int theErr = errno;
                if (0 <= mLogFilePrevPos && mLogFilePrevPos <= mLogFilePos) {
                    // Attempt to truncate the log segment in the hope that
                    // in case of out of disk space doing so will produce valid
                    // log segment.
                    KFS_LOG_STREAM_ERROR <<
                        "transaction log writer error:" <<
                         " " << mLogName << ": " << QCUtils::SysError(theErr) <<
                        " current position: "          << mLogFilePos <<
                        " attempting to truncate to: " << mLogFilePrevPos <<
                    KFS_LOG_EOM;
                    if (ftruncate(mLogFd, mLogFilePrevPos)) {
                        const int theErr = errno;
                        KFS_LOG_STREAM_ERROR <<
                            "transaction log truncate error:" <<
                             " " << mLogName << ": " <<
                                QCUtils::SysError(theErr) <<
                        KFS_LOG_EOM;
                    } else {
                        const off_t thePos =
                            lseek(mLogFd, mLogFilePrevPos, SEEK_SET);
                        if (thePos < 0) {
                            const int theErr = errno;
                            KFS_LOG_STREAM_ERROR <<
                                "transaction log seek error:" <<
                                 " "  << mLogName <<
                                 ": " << QCUtils::SysError(theErr) <<
                            KFS_LOG_EOM;
                        } else {
                            if (thePos != mLogFilePrevPos) {
                                KFS_LOG_STREAM_ERROR <<
                                    "transaction log seek error:" <<
                                     " " << mLogName << ": "
                                     "expected position: " << mLogFilePrevPos <<
                                     " actual: "           << thePos <<
                                KFS_LOG_EOM;
                            } else {
                                mLogFilePos = mLogFilePrevPos;
                                // MD stream state has no be restored in order
                                // to produce correct trailer's MD.
                            }
                        }
                    }
                }
                IoError(theErr);
            }
        }
        return IsLogStreamGood();
    }
    bool flush()
        { return IsLogStreamGood(); }
    bool IsSimulateFailure()
    {
        return (
            0 < mFailureSimulationInterval &&
            0 == (mRandom.Rand() % mFailureSimulationInterval)
        );
    }
    bool Close()
    {
        if (mLogFd <= 0) {
            return false;
        }
        const bool theOkFlag = close(mLogFd) == 0;
        if (! theOkFlag) {
            IoError(errno);
        }
        mLogFd = -1;
        return theOkFlag;
    }
};

LogWriter::LogWriter()
    : mNextSeq(0),
      mImpl(*(new Impl()))
{}

LogWriter::~LogWriter()
{
    delete &mImpl;
}

    int
LogWriter::Start(
    NetManager&         inNetManager,
    MetaDataStore&      inMetaDataStore,
    MetaDataSync&       inMetaDataSync,
    seq_t               inLogNum,
    const MetaVrLogSeq& inLogSeq,
    const MetaVrLogSeq& inCommittedLogSeq,
    fid_t               inCommittedFidSeed,
    int64_t             inCommittedErrCheckSum,
    int                 inCommittedStatus,
    const MdStateCtx*   inLogAppendMdStatePtr,
    const MetaVrLogSeq& inLogAppendStartSeq,
    seq_t               inLogAppendLastBlockSeq,
    bool                inLogAppendHexFlag,
    bool                inLogNameHasSeqFlag,
    const char*         inParametersPrefixPtr,
    const Properties&   inParameters,
    string&             outCurLogFileName)
{
    return mImpl.Start(
        inNetManager,
        inMetaDataStore,
        inMetaDataSync,
        inLogNum,
        inLogSeq,
        inCommittedLogSeq,
        inCommittedFidSeed,
        inCommittedErrCheckSum,
        inCommittedStatus,
        inLogAppendMdStatePtr,
        inLogAppendStartSeq,
        inLogAppendLastBlockSeq,
        inLogAppendHexFlag,
        inLogNameHasSeqFlag,
        inParametersPrefixPtr,
        inParameters,
        outCurLogFileName
    );
}

    bool
LogWriter::Enqueue(
    MetaRequest& inRequest)
{
    inRequest.seqno = GetNextSeq();
    return mImpl.Enqueue(inRequest);
}

    void
LogWriter::Committed(
    MetaRequest& inRequest,
    fid_t        inFidSeed)
{
    mImpl.RequestCommitted(inRequest, inFidSeed);
}

    void
LogWriter::GetCommitted(
    MetaVrLogSeq& outLogSeq,
    int64_t&      outErrChecksum,
    fid_t&        outFidSeed,
    int&          outStatus) const
{
    mImpl.GetCommitted(
        outLogSeq,
        outErrChecksum,
        outFidSeed,
        outStatus
    );
}

    void
LogWriter::SetCommitted(
    const MetaVrLogSeq& inLogSeq,
    int64_t             inErrChecksum,
    fid_t               inFidSeed,
    int                 inStatus,
    const MetaVrLogSeq& inLastReplayLogSeq)
{
    mImpl.SetCommitted(
        inLogSeq,
        inErrChecksum,
        inFidSeed,
        inStatus,
        inLastReplayLogSeq
    );
}

    MetaVrLogSeq
LogWriter::GetCommittedLogSeq() const
{
    return mImpl.GetCommittedLogSeq();
}

    void
LogWriter::ScheduleFlush()
{
   mImpl.ScheduleFlush();
}
    void
LogWriter::PrepareToFork()
{
   mImpl.PrepareToFork();
}

    void
LogWriter::ForkDone()
{
   mImpl.ForkDone();
}

    void
LogWriter::ChildAtFork()
{
    mImpl.ChildAtFork();
}

    void
LogWriter::Shutdown()
{
   mImpl.Shutdown();
}

    void
LogWriter::SetLastLogReceivedTime(
    time_t inTime)
{
    mImpl.SetLastLogReceivedTime(inTime);
}

    MetaVrSM&
LogWriter::GetMetaVrSM()
{
    return mImpl.GetMetaVrSM();
}

}
 // namespace KFS
