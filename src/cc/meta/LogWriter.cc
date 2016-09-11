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
    Impl(
        volatile int& inVrStatus)
        : ITimeout(),
          QCRunnable(),
          LogTransmitter::CommitObserver(),
          NetManager::Dispatcher(),
          mNetManagerPtr(0),
          mMetaDataStorePtr(0),
          mReplayerPtr(0),
          mNetManager(),
          mLogTransmitter(mNetManager, *this),
          mMetaVrSM(mLogTransmitter),
          mVrStatus(0),
          mEnqueueVrStatus(inVrStatus),
          mTransmitCommitted(),
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
          mExraPendingCount(0),
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
          mVrNodeId(-1),
          mPrepareToForkCond(),
          mForkDoneCond(),
          mRandom(),
          mTmpBuffer(),
          mLogFileNamePrefix("log"),
          mNotPrimaryErrorMsg(ErrorCodeToString(-ELOGFAILED)),
          mLogWriteErrorMsg(ErrorCodeToString(-EVRNOTPRIMARY))
        { mLogName.reserve(1 << 10); }
    ~Impl()
        { Impl::Shutdown(); }
    int Start(
        NetManager&           inNetManager,
        MetaDataStore&        inMetaDataStore,
        MetaDataSync&         inMetaDataSync,
        const UniqueID&       inFileId,
        Replay&               inReplayer,
        seq_t                 inLogNum,
        const char*           inParametersPrefixPtr,
        const Properties&     inParameters,
        int64_t               inFileSystemId,
        const ServerLocation& inDataStoreLocation,
        const string&         inMetaMd,
        string&               outCurLogFileName)
    {
        if (inLogNum < 0 || ! inReplayer.getLastLogSeq().IsValid() ||
                (mThread.IsStarted() || mNetManagerPtr) || inFileSystemId < 0) {
            return -EINVAL;
        }
        mReplayerPtr = &inReplayer;
        mLogTransmitter.SetFileSystemId(inFileSystemId);
        mNextBlockChecksum = ComputeBlockChecksum(kKfsNullChecksum, "\n", 1);
        mLogNum = inLogNum;
        const int theErr = SetParameters(inParametersPrefixPtr, inParameters);
        if (0 != theErr) {
            return theErr;
        }
        if (0 != (mError = mMetaVrSM.Start(
                inMetaDataSync,
                mNetManager,
                inFileId,
                *mReplayerPtr,
                inFileSystemId,
                inDataStoreLocation,
                inMetaMd))) {
            return mError;
        }
        mMdStream.Reset(this);
        mReplayerPtr = &inReplayer;
        mCommitted.mErrChkSum = mReplayerPtr->getErrChksum();
        mCommitted.mSeq       = mReplayerPtr->getCommitted();
        mCommitted.mFidSeed   = inFileId.getseed();
        mCommitted.mStatus    = mReplayerPtr->getLastCommittedStatus();
        mReplayLogSeq         = mReplayerPtr->getLastLogSeq();
        mPendingReplayLogSeq  = mReplayLogSeq;
        mPendingCommitted     = mCommitted;
        mInFlightCommitted    = mPendingCommitted;
        mMetaDataStorePtr     = &inMetaDataStore;
        if (mReplayerPtr->getAppendToLastLogFlag()) {
            const bool theLogAppendHexFlag =
                16 == mReplayerPtr->getLastLogIntBase();
            const bool theHasLogSeqFlag    =
                mReplayerPtr->logSegmentHasLogSeq();
            mCurLogStartSeq                = mReplayerPtr->getLastLogStart();
            SetLogName(mReplayLogSeq,
                theHasLogSeqFlag ? mCurLogStartSeq : MetaVrLogSeq());
            mCurLogStartTime = mNetManager.Now();
            mMdStream.SetMdState(mReplayerPtr->getMdState());
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
            const seq_t theLogAppendLastBlockSeq =
                mReplayerPtr->getLastBlockSeq();
            KFS_LOG_STREAM_INFO <<
                "log append:" <<
                " idx: "      << mLogNum <<
                " start: "    << mCurLogStartSeq <<
                " cur: "      << mNextLogSeq <<
                " block: "    << theLogAppendLastBlockSeq <<
                " hex: "      << theLogAppendHexFlag <<
                " file: "     << mLogName <<
                " size: "     << theSize <<
                " checksum: " << mMdStream.GetMd() <<
            KFS_LOG_EOM;
            mMdStream.setf(
                theLogAppendHexFlag ? ostream::hex : ostream::dec,
                ostream::basefield
            );
            mLogFilePos     = theSize;
            mLogFilePrevPos = mLogFilePos;
            mNextBlockSeq   = theLogAppendLastBlockSeq;
            if (theLogAppendLastBlockSeq < 0 || ! theLogAppendHexFlag ||
                    ! theHasLogSeqFlag) {
                // Previous / "old" log format.
                // Close the log segment even if it is empty and start new one.
                StartNextLog();
            }
        } else {
            NewLog(mReplayLogSeq);
        }
        if (! IsLogStreamGood()) {
            return mError;
        }
        mVrNodeId              = mMetaVrSM.GetNodeId();
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
    bool EnqueueStart(
        MetaRequest& inRequest)
    {
        if (inRequest.next) {
            panic("log writer enqueue: invalid request non null next field");
            return false;
        }
        if (inRequest.suspended) {
            panic("log writer enqueue: invalid request suspended");
            return false;
        }
        if (MetaRequest::kLogNever != inRequest.logAction) {
            if (inRequest.replayFlag) {
                panic("log writer enqueue: invalid request replay flag");
                return false;
            }
            const bool theLogFlag = inRequest.start();
            if (1 != inRequest.submitCount || inRequest.next ||
                    inRequest.suspended) {
                panic("log writer enqueue: invalid request start completion");
                return false;
            }
            if (! theLogFlag) {
                inRequest.logAction = MetaRequest::kLogNever;
            }
        }
        return (! inRequest.replayFlag);
    }
    bool Enqueue(
        MetaRequest& inRequest)
    {
        if (! EnqueueStart(inRequest)) {
            return false;
        }
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
                panic("log writer enqueue: invalid log queue counter");
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
            // Check if this log start view, and if it is, then the committed
            // status has already been updated.
            if (META_VR_LOG_START_VIEW != inRequest.op ||
                    0 != inRequest.status ||
                    mCommitted.mSeq <= inRequest.logseq ||
                    0 != mCommitted.mStatus ||
                    mCommitted.mFidSeed != inFidSeed) {
                panic("request committed: invalid out of order log sequence");
            }
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
        QCStMutexLocker theLock(mMutex);
        const bool theSetReplayStateFlag = mSetReplayStateFlag;
        mPendingCommitted      = mCommitted;
        mPendingReplayLogSeq   = mReplayLogSeq;
        mVrLastLogReceivedTime = mLastLogReceivedTime;
        mInQueue.PushBack(mPendingQueue);
        theLock.Unlock();
        mNetManager.Wakeup();
        mCommitUpdatedFlag = ! theSetReplayStateFlag;
    }
    void Shutdown()
    {
        if (! mThread.IsStarted() || mStopFlag) {
            return;
        }
        QCStMutexLocker theLock(mMutex);
        // Mark everything committed to cleanup queues.
        mSetReplayStateFlag = false;
        mTransmitCommitted  = mNextLogSeq;
        mStopFlag           = true;
        mNetManager.Wakeup();
        theLock.Unlock();
        mThread.Join();
        if (mNetManagerPtr) {
            mNetManagerPtr->UnRegisterTimeoutHandler(this);
            mNetManagerPtr = 0;
        }
        Release(mInQueue);
        Release(mOutQueue);
        Release(mPendingQueue);
        Release(mPendingAckQueue);
        Release(mReplayCommitQueue);
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
        mWokenFlag = mWokenFlag || mTransmitCommitted < inSeq;
        mMetaVrSM.Commit(inSeq);
        if (mTransmitCommitted < inSeq) {
            mTransmitCommitted = inSeq;
        }
    }
    void SetLastLogReceivedTime(
        time_t inTime)
    {
        mLastLogReceivedTime = inTime;
        mCommitUpdatedFlag   = true;
    }
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
    typedef MetaVrSM::NodeId     NodeId;
    typedef StBufferT<char, 128> TmpBuffer;

    NetManager*    mNetManagerPtr;
    MetaDataStore* mMetaDataStorePtr;
    Replay*        mReplayerPtr;
    NetManager     mNetManager;
    LogTransmitter mLogTransmitter;
    MetaVrSM       mMetaVrSM;
    int            mVrStatus;
    volatile int&  mEnqueueVrStatus;
    MetaVrLogSeq   mTransmitCommitted;
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
    int            mExraPendingCount;
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
    Checksum       mNextBlockChecksum;
    int            mLogFd;
    int            mError;
    MdStream       mMdStream;
    ReqOstream     mReqOstream;
    time_t         mCurLogStartTime;
    MetaVrLogSeq   mCurLogStartSeq;
    seq_t          mLogNum;
    string         mLogName;
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
    NodeId         mVrNodeId;
    QCCondVar      mPrepareToForkCond;
    QCCondVar      mForkDoneCond;
    PrngIsaac64    mRandom;
    TmpBuffer      mTmpBuffer;
    const string   mLogFileNamePrefix;
    const string   mNotPrimaryErrorMsg;
    const string   mLogWriteErrorMsg;

    void Release(
        Queue& inQueue)
    {
        MetaRequest* thePtr;
        while ((thePtr = inQueue.PopFront())) {
            if (! thePtr->clnt) {
                --mPendingCount;
                MetaRequest::Release(thePtr);
            }
        }
    }
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
        const bool theSetReplayStateFlag = mSetReplayStateFlag;
        if (0 != theReplayCommitHeadPtr && ! theSetReplayStateFlag) {
            panic("log writer: invalid set replay state flag");
        }
        mSetReplayStateFlag = false;
        mPendingCount += mExraPendingCount;
        mExraPendingCount = 0;
        const bool theWakeupFlag = theSetReplayStateFlag &&
            (mCommitUpdatedFlag || ! mInQueue.IsEmpty());
        theLock.Unlock();
        if (theWakeupFlag) {
            mNetManager.Wakeup();
        }
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
            if (IsMetaLogWriteOrVrError(thePtr->status) ||
                    thePtr->replayBypassFlag ||
                    ! mReplayerPtr->submit(*thePtr)) {
                submit_request(&theReq);
            }
        }
        if (theSetReplayStateFlag) {
            const MetaRequest* thePtr = theReplayCommitHeadPtr;
            while (thePtr) {
                if (--mPendingCount < 0) {
                    panic("log writer: set replay state invalid pending count");
                }
                thePtr = thePtr->next;
            }
            mReplayerPtr->setReplayState(
                mCommitted.mSeq,
                mCommitted.mFidSeed,
                mCommitted.mStatus,
                mCommitted.mErrChkSum,
                theReplayCommitHeadPtr
            );
        }
    }
    void ProcessPendingAckQueue(
        Queue& inDoneQueue,
        bool   inSetReplayStateFlag,
        int    inExtraReqCount)
    {
        mPendingAckQueue.PushBack(inDoneQueue);
        MetaRequest* thePtr     = 0;
        MetaRequest* thePrevPtr = 0;
        if (mTransmitCommitted < mNextLogSeq) {
            thePtr = mPendingAckQueue.Front();
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
        if (inDoneQueue.IsEmpty() && 0 == inExtraReqCount &&
                (0 == mVrStatus ||
                (! inSetReplayStateFlag && mPendingAckQueue.IsEmpty()))) {
            return;
        }
        thePtr     = 0;
        thePrevPtr = 0;
        int theVrStatus = mVrStatus;
        if (0 != mVrStatus && 0 == (theVrStatus = mMetaVrSM.GetStatus())) {
            MetaRequest* thePrevLastPtr = 0;
            MetaRequest* theCurPtr      = mPendingAckQueue.Front();
            while (theCurPtr) {
                if (META_VR_LOG_START_VIEW == theCurPtr->op) {
                    thePrevLastPtr = thePrevPtr;
                    thePtr         = theCurPtr;
                }
                thePrevPtr = theCurPtr;
                theCurPtr  = theCurPtr->next;
            }
            if (thePtr) {
                if (mPendingAckQueue.Front() == thePtr) {
                    mPendingAckQueue.Reset();
                } else {
                    thePrevLastPtr->next = 0;
                    mPendingAckQueue.Set(
                        mPendingAckQueue.Front(), thePrevLastPtr);
                }
            }
        }
        QCStMutexLocker theLocker(mMutex);
        mOutQueue.PushBack(inDoneQueue);
        if (0 != mVrStatus) {
            mReplayCommitQueue.PushBack(mPendingAckQueue);
            if (! mSetReplayStateFlag) {
                mSetReplayStateFlag =
                    inSetReplayStateFlag || ! mReplayCommitQueue.IsEmpty();
                if (mSetReplayStateFlag) {
                    KFS_LOG_STREAM_DEBUG <<
                        "scheduling set replay state:"
                        " replay queue empty: " <<
                            mReplayCommitQueue.IsEmpty() <<
                        " set replay state flag: " << inSetReplayStateFlag <<
                    KFS_LOG_EOM;
                }
            }
            if (! thePtr && 0 == theVrStatus) {
                mVrStatus        = theVrStatus;
                mEnqueueVrStatus = theVrStatus;
                KFS_LOG_STREAM_DEBUG <<
                    "log start view:"
                    " comitted: "           << mTransmitCommitted <<
                    " last log: "           << mNextLogSeq <<
                    " replay queue empty: " << mReplayCommitQueue.IsEmpty() <<
                    " set replay state: "   << mSetReplayStateFlag <<
                KFS_LOG_EOM;
            }
        }
        mExraPendingCount += inExtraReqCount;
        theLocker.Unlock();
        if (thePtr) {
            mPendingAckQueue.Set(thePtr, thePrevPtr);
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
        if (mSetReplayStateFlag) {
            KFS_LOG_STREAM_DEBUG <<
                "set replay state pending"
                " write queue empty: " << mInQueue.IsEmpty() <<
            KFS_LOG_EOM;
            return;
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
        mMetaVrSM.Process(
            mNetManager.Now(),
            theVrLastLogReceivedTime,
            mInFlightCommitted.mSeq,
            mInFlightCommitted.mErrChkSum,
            mInFlightCommitted.mFidSeed,
            mInFlightCommitted.mStatus,
            theReplayLogSeq,
            theVrStatus,
            theReqPtr
        );
        if (theReqPtr) {
            if (0 != theReqPtr->submitCount) {
                panic("log writer: invalid VR log write request");
            } else {
                // Mark as submitted, as in MetaRequest::Submit(),
                // with 0 process time.
                theReqPtr->submitCount       = 1;
                theReqPtr->commitPendingFlag = true;
                theReqPtr->submitTime        = microseconds();
                theReqPtr->processTime       = 0;
                if (! EnqueueStart(*theReqPtr)) {
                    panic("log writer: VR log write start failure");
                }
            }
            theWriteQueue.PushBack(*theReqPtr);
        }
        const bool theVrBecameNonPrimaryFlag =
            0 != theVrStatus && 0 == mVrStatus;
        if (theVrBecameNonPrimaryFlag) {
            mVrStatus        = theVrStatus;
            mEnqueueVrStatus = theVrStatus;
            SyncAddAndFetch(mEnqueueVrStatus, 0);
            Queue theTmp;
            ProcessPendingAckQueue(theTmp, theVrBecameNonPrimaryFlag, 0);
        } else if (0 != theVrStatus) {
            // Update status if it's primary.
            mVrStatus = theVrStatus;
        }
        if (! theWriteQueue.IsEmpty()) {
            Write(*theWriteQueue.Front());
        }
        mWokenFlag = false;
        ProcessPendingAckQueue(theWriteQueue, false, theReqPtr ? 1 : 0);
    }
    virtual void DispatchEnd()
    {
        bool theVrBecameNonPrimaryFlag = 0 == mVrStatus;
        if (theVrBecameNonPrimaryFlag) {
            const int theVrStatus = mMetaVrSM.GetStatus();
            theVrBecameNonPrimaryFlag = 0 != theVrStatus;
            if (theVrBecameNonPrimaryFlag) {
                mVrStatus        = theVrStatus;
                mEnqueueVrStatus = theVrStatus;
                SyncAddAndFetch(mEnqueueVrStatus, 0);
            }
        }
        if (mWokenFlag || theVrBecameNonPrimaryFlag) {
            Queue theTmp;
            ProcessPendingAckQueue(theTmp, theVrBecameNonPrimaryFlag, 0);
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
        ostream&     theStream = mMdStream;
        MetaRequest* theCurPtr = &inHead;
        while (theCurPtr) {
            mLastLogSeq = mNextLogSeq;
            MetaRequest*          theVrPtr               = 0;
            MetaRequest*          thePtr                 = theCurPtr;
            seq_t                 theEndBlockSeq         =
                mNextLogSeq.mLogSeq + mMaxBlockSize;
            const bool            theSimulateFailureFlag = IsSimulateFailure();
            const bool            theTransmitterUpFlag   = 0 == mVrStatus;
            bool                  theStartViewFlag       = false;
            MetaLogWriterControl* theCtlPtr              = 0;
            for ( ; thePtr; thePtr = thePtr->next) {
                if (mMetaVrSM.Handle(*thePtr, mLastLogSeq)) {
                    theVrPtr = thePtr;
                    break;
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
                    theStartViewFlag = true;
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
                if (theEndBlockSeq <= mLastLogSeq.mLogSeq || theStartViewFlag) {
                    break;
                }
                if (mMdStream.GetBufferedStart() +
                            mMdStream.GetBufferSize() / 4 * 3 <
                        mMdStream.GetBufferedEnd()) {
                    break;
                }
            }
            MetaRequest* const theEndPtr = thePtr ? thePtr->next : thePtr;
            if (mNextLogSeq < mLastLogSeq && ! theSimulateFailureFlag &&
                    (theTransmitterUpFlag || theStartViewFlag) &&
                    IsLogStreamGood()) {
                const int theBlkLen =
                    (int)(mLastLogSeq.mLogSeq - mNextLogSeq.mLogSeq);
                if (theStartViewFlag) {
                    MetaVrLogStartView& theOp =
                        *static_cast<MetaVrLogStartView*>(theCurPtr);
                    if (theOp.logseq != mLastLogSeq || ! theOp.Validate() ||
                            0 != theOp.status) {
                        panic("log writer: invalid VR log start view");
                    }
                    mLastLogSeq = theOp.mNewLogSeq;
                    KFS_LOG_STREAM_DEBUG <<
                        "writing: " << theOp.Show() <<
                    KFS_LOG_EOM;
                }
                FlushBlock(mLastLogSeq, theBlkLen);
            }
            if (IsLogStreamGood() && ! theSimulateFailureFlag &&
                    (theTransmitterUpFlag || theStartViewFlag)) {
                mNextLogSeq = mLastLogSeq;
                if (theStartViewFlag) {
                    // Set sequence to one to the left of the start of the view,
                    // in for both pending ACK queue advancement, and op
                    // validate method to work.
                    theCurPtr->logseq = mLastLogSeq;
                    theCurPtr->logseq.mLogSeq--;
                    if (! theCurPtr->logseq.IsValid()) {
                        panic("log writer: invalid VR log start view sequence");
                    }
                }
            } else {
                mLastLogSeq = mNextLogSeq;
                // Write failure.
                MetaRequest* const theLastPtr = theVrPtr ? theVrPtr : theEndPtr;
                for (thePtr = theCurPtr;
                        theLastPtr != thePtr;
                        thePtr = thePtr->next) {
                    if (META_LOG_WRITER_CONTROL != thePtr->op &&
                            (META_READ_META_DATA != thePtr->op ||
                                 ! static_cast<const MetaReadMetaData*>(
                                    thePtr)->allowNotPrimaryFlag) &&
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
                theCtlPtr->primaryNodeId = mMetaVrSM.GetPrimaryNodeId();
                theCtlPtr->lastLogSeq    = mLastLogSeq;
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
            inReq.statusMsg = mNotPrimaryErrorMsg;
        } else {
            inReq.status    = -ELOGFAILED;
            inReq.statusMsg = mLogWriteErrorMsg;
        }
    }
    void FlushBlock(
        const MetaVrLogSeq& inLogSeq,
        int                 inBlockLen = -1)
    {
        const int theVrStatus = mMetaVrSM.HandleLogBlock(
            mNextLogSeq,
            inLogSeq,
            mInFlightCommitted.mSeq,
            mVrNodeId
        );
        const int theBlockLen = inBlockLen < 0 ?
            (int)(inLogSeq.mLogSeq - mNextLogSeq.mLogSeq) : inBlockLen;
        ++mNextBlockSeq;
        KFS_LOG_STREAM_DEBUG <<
            "flush block: " << inLogSeq <<
            " seq: "        << mNextBlockSeq <<
            " len: "        << theBlockLen <<
            " VR status: "  << theVrStatus <<
            " "             << ErrorCodeToString(theVrStatus) <<
        KFS_LOG_EOM;
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
        const char*  theStartPtr      = mMdStream.GetBufferedStart();
        const size_t theTxLen         =
            mMdStream.GetBufferedEnd() - theStartPtr;
        Checksum     theBlockChecksum = ComputeBlockChecksum(
            0 < mNextBlockSeq ? mNextBlockChecksum : kKfsNullChecksum,
            theStartPtr, theTxLen);
        Checksum const theTxChecksum = theBlockChecksum;
        mReqOstream <<
            mNextBlockSeq <<
            "/";
        mReqOstream.flush();
        theStartPtr = mMdStream.GetBufferedStart() + theTxLen;
        theBlockChecksum = ComputeBlockChecksum(
            theBlockChecksum,
            theStartPtr,
            mMdStream.GetBufferedEnd() - theStartPtr);
        mReqOstream << theBlockChecksum << "\n";
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
                "flush log block: block transmit " <<
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
    }
    void LogStreamFlush()
    {
        mMdStream.SetSync(true);
        mLogFilePrevPos = mLogFilePos;
        mReqOstream.flush();
        if (IsLogStreamGood()) {
            Sync();
            if (mLogFilePrevPos < mLogFilePos && ! IsLogStreamGood()) {
                TruncateOnError(-mError);
            }
        }
        mMdStream.SetSync(false);
        // Buffer should be empty if write succeeded, and has to be cleared /
        // discarded in case of failure.
        mMdStream.ClearBuffer();
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
        inRequest.primaryNodeId = mMetaVrSM.GetPrimaryNodeId();
        inRequest.committed     = mInFlightCommitted.mSeq;
        inRequest.lastLogSeq    = mLastLogSeq;
        inRequest.logName       = mLogName;
        inRequest.logSegmentNum = mLogNum;
        return (theRetFlag && IsLogStreamGood());
    }
    void WriteBlock(
        MetaLogWriterControl& inRequest)
    {
        if (mNextBlockSeq < 0) {
            panic("log writer: write block: invalid block sequence");
            inRequest.status = -EFAULT;
            return;
        }
        if (mLastLogSeq != mNextLogSeq) {
            panic("invalid write block invocation");
            inRequest.status = -EFAULT;
            return;
        }
        if (0 != inRequest.status) {
            KFS_LOG_STREAM_ERROR <<
                "write block:"
                " status: " << inRequest.status <<
                " "         << inRequest.statusMsg <<
                " "         << inRequest.Show() <<
            KFS_LOG_EOM;
            return;
        }
        if (inRequest.blockStartSeq == inRequest.blockEndSeq) {
            if (! inRequest.blockLines.IsEmpty() ||
                    0 < inRequest.blockData.BytesConsumable()) {
                inRequest.status    = -EINVAL;
                inRequest.statusMsg = "invalid non empty block / heartbeat";
            } else if (inRequest.blockStartSeq != mLastLogSeq) {
                inRequest.status    = -EINVAL;
                inRequest.statusMsg = "invalid heartbeat sequence";
            } else {
                // Valid heartbeat.
                const int theVrStatus = mMetaVrSM.HandleLogBlock(
                    inRequest.blockStartSeq,
                    inRequest.blockEndSeq,
                    mInFlightCommitted.mSeq,
                    inRequest.transmitterId
                );
                if (0 != theVrStatus &&
                        ! IsMetaLogWriteOrVrError(theVrStatus)) {
                    inRequest.status    = theVrStatus;
                    inRequest.statusMsg = "VR error";
                    return;
                }
                mMetaVrSM.LogBlockWriteDone(
                    inRequest.blockStartSeq,
                    inRequest.blockEndSeq,
                    mInFlightCommitted.mSeq,
                    IsLogStreamGood()
                );
            }
            return;
        }
        if (inRequest.blockData.BytesConsumable() <= 0) {
            panic("write block: invalid block: no data");
            inRequest.statusMsg = "invalid block: no data";
            inRequest.status    = -EFAULT;
            return;
        }
        if (inRequest.blockLines.IsEmpty()) {
            panic("write block: invalid block: no log lines");
            inRequest.statusMsg = "invalid block: no log lines";
            inRequest.status    = -EFAULT;
            return;
        }
        if (0 == mVrStatus) {
            inRequest.statusMsg = "block write rejected: VR state is primary";
            inRequest.status    = -EROFS;
            return;
        }
        if (inRequest.blockStartSeq != mLastLogSeq) {
            int theLnLen = -1;
            if (inRequest.blockStartSeq < inRequest.blockEndSeq &&
                    mLastLogSeq < inRequest.blockStartSeq &&
                    2 == inRequest.blockLines.GetSize() &&
                    3 < (theLnLen = inRequest.blockLines.Front()) &&
                    theLnLen < inRequest.blockData.BytesConsumable()) {
                // Set size to 0 to avoid data copy, use resize instead of
                // clear, as resize does not free the buffer, just sets the
                // size to 0.
                mTmpBuffer.Resize(0);
                char* const  thePtr    = mTmpBuffer.Resize(theLnLen);
                MetaRequest* theReqPtr = 0;
                if (inRequest.blockData.CopyOut(thePtr, theLnLen) == theLnLen &&
                        'a' == (thePtr[0] & 0xFF) && '/' == (thePtr[1] & 0xFF) &&
                        (theReqPtr = MetaRequest::ReadReplay(
                            thePtr + 2, theLnLen - 2)) &&
                        META_VR_LOG_START_VIEW == theReqPtr->op) {
                    MetaVrLogStartView& theOp =
                        *static_cast<MetaVrLogStartView*>(theReqPtr);
                    if (! theOp.Validate() ||
                            theOp.mNewLogSeq != inRequest.blockEndSeq ||
                            mLastLogSeq < theOp.mCommittedSeq ||
                            theOp.mCommittedSeq < mInFlightCommitted.mSeq) {
                        inRequest.status    = -EINVAL;
                        inRequest.statusMsg = "invalid log start view entry";
                        theLnLen = -1;
                    }
                } else {
                    theLnLen = -1;
                }
                MetaRequest::Release(theReqPtr);
            }
            if (theLnLen <= 0) {
                if (0 <= inRequest.status) {
                    inRequest.status    = -EINVAL;
                    inRequest.statusMsg = "invalid block start sequence";
                }
                return;
             }
        }
        if (! IsLogStreamGood()) {
            inRequest.status    = -EIO;
            inRequest.statusMsg = "log write error";
            return;
        }
        // Copy block data, and write block sequence and updated checksum.
        // To include leading \n, if any, "combine" block checksum.
        Checksum theBlockChecksum = ChecksumBlocksCombine(
            mNextBlockChecksum,
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
        theBlockChecksum = ComputeBlockChecksum(
            theBlockChecksum, thePtr, theTrailerLen);
        mReqOstream << theBlockChecksum << "\n";
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
            mInFlightCommitted.mSeq,
            inRequest.transmitterId
        );
        if (0 != theVrStatus &&
                ! IsMetaLogWriteOrVrError(theVrStatus)) {
            mMdStream.ClearBuffer();
            --mNextBlockSeq;
            inRequest.status    = theVrStatus;
            inRequest.statusMsg = "VR error";
            return;
        }
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
                "write block: block transmit " <<
                    (0 == theStatus ? "OK" : "failure") <<
                ": ["   << inRequest.blockStartSeq  <<
                ":"     << inRequest.blockEndSeq <<
                "]"
                " status: " << theStatus <<
                (theStatus < 0 ? " " : "") <<
                (theStatus < 0 ? ErrorCodeToString(theStatus) : string()) <<
            KFS_LOG_EOM;
        }
        LogStreamFlush();
        const bool theStreamGoodFlag = IsLogStreamGood();
        mMetaVrSM.LogBlockWriteDone(
            inRequest.blockStartSeq,
            inRequest.blockEndSeq,
            mInFlightCommitted.mSeq,
            theStreamGoodFlag
        );
        if (theStreamGoodFlag) {
            inRequest.blockSeq   = mNextBlockSeq;
            mLastLogSeq          = inRequest.blockEndSeq;
            mNextLogSeq          = mLastLogSeq;
            inRequest.status     = 0;
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
    static bool ParseField(
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
        SetLogName(inLogSeq);
        if ((mLogFd = open(
                mLogName.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0666)) < 0) {
            IoError(errno);
            return;
        }
        mMdStream.Reset(this);
        mMdStream.clear();
        mReqOstream.Get().clear();
        mMdStream.setf(ostream::dec, ostream::basefield);
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
        mNextLogSeq = inLogSeq;
        mLastLogSeq = inLogSeq;
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
            mCurLogStartSeq = inLogStartSeqNum;
        } else {
            mCurLogStartSeq = inLogSeq;
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
                TruncateOnError(theErr);
                IoError(theErr);
            }
        }
        return IsLogStreamGood();
    }
    bool flush()
        { return IsLogStreamGood(); }
    void TruncateOnError(
        int inError)
    {
        // Attempt to truncate the log segment in the hope that
        // in case of out of disk space doing so will produce valid
        // log segment.
        KFS_LOG_STREAM_ERROR <<
            "transaction log writer error:" <<
             " " << mLogName << ": " << QCUtils::SysError(inError) <<
            " current position: "          << mLogFilePos <<
            " attempting to truncate to: " << mLogFilePrevPos <<
        KFS_LOG_EOM;
        if (mLogFilePrevPos < 0 || mLogFilePos <= mLogFilePrevPos) {
            return;
        }
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
                    if (fsync(mLogFd)) {
                        const int theErr = errno;
                        KFS_LOG_STREAM_ERROR <<
                            "transaction log fsync error:" <<
                             " "  << mLogName <<
                             ": " << QCUtils::SysError(theErr) <<
                        KFS_LOG_EOM;
                    } else {
                        mLogFilePos = mLogFilePrevPos;
                    }
                }
            }
        }
    }
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
      mVrStatus(0),
      mImpl(*(new Impl(mVrStatus)))
{}

LogWriter::~LogWriter()
{
    delete &mImpl;
}

    int
LogWriter::Start(
    NetManager&           inNetManager,
    MetaDataStore&        inMetaDataStore,
    MetaDataSync&         inMetaDataSync,
    const UniqueID&       inFileId,
    Replay&               inReplayer,
    seq_t                 inLogNum,
    const char*           inParametersPrefixPtr,
    const Properties&     inParameters,
    int64_t               inFileSystemId,
    const ServerLocation& inDataStoreLocation,
    const string&         inMetaMd,
    string&               outCurLogFileName)
{
    return mImpl.Start(
        inNetManager,
        inMetaDataStore,
        inMetaDataSync,
        inFileId,
        inReplayer,
        inLogNum,
        inParametersPrefixPtr,
        inParameters,
        inFileSystemId,
        inDataStoreLocation,
        inMetaMd,
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
