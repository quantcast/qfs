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
#include "common/AverageFilter.h"
#include "common/time.h"

#include "kfsio/NetManager.h"
#include "kfsio/ITimeout.h"
#include "kfsio/checksum.h"
#include "kfsio/PrngIsaac64.h"
#include "kfsio/NetErrorSimulator.h"
#include "kfsio/NetManagerWatcher.h"

#include "qcdio/QCThread.h"
#include "qcdio/QCMutex.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"

#include <errno.h>
#include <unistd.h>
#include <fcntl.h>

#include <vector>

#include <boost/static_assert.hpp>

namespace KFS
{
using std::ofstream;
using std::vector;

class LogWriter::Impl :
    private ITimeout,
    private QCRunnable,
    private LogTransmitter::CommitObserver,
    private NetManager::Dispatcher
{
public:
    Impl(
        volatile int&     inVrStatus,
        volatile int64_t& inPrimaryLeaseEndTimeUsec)
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
          mPrimaryFlag(true),
          mVrStatus(0),
          mEnqueueVrStatus(inVrStatus),
          mPrimaryLeaseEndTimeUsec(inPrimaryLeaseEndTimeUsec),
          mTransmitCommitted(),
          mMaxDoneLogSeq(),
          mCommitted(),
          mCpuAffinityIndex(-1),
          mThread(),
          mMutex(),
          mStopFlag(true),
          mOmitDefaultsFlag(true),
          mCommitUpdatedFlag(false),
          mSetReplayStateFlag(false),
          mMaxBlockSize(512),
          mMaxBlockBytes(128 << 10),
          mMaxReceiverRetryQueueLimit(8 << 10),
          mMaxReceiverRetryQueueSize(mMaxReceiverRetryQueueLimit),
          mPendingCount(0),
          mExraPendingCount(0),
          mReceiverRetryQueueSize(0),
          mLogDir("./kfslog"),
          mPendingQueue(),
          mInQueue(),
          mOutQueue(),
          mPendingAckQueue(),
          mReplayCommitQueue(),
          mReceiverRetryQueue(),
          mPendingCommitted(),
          mInFlightCommitted(),
          mLastWriteCommitted(),
          mReplayLastWriteCommitted(),
          mSetReplayLastWriteCommitted(),
          mPendingReplayLogSeq(),
          mReplayLogSeq(),
          mNextLogSeq(),
          mLastViewEndSeq(),
          mLastNonEmptyViewEndSeq(),
          mLastLogSeq(),
          mViewStartSeq(),
          mNextBlockSeq(-1),
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
          mMaxClientOpsPendingCount(20 << 10),
          mMaxPendingAckByteCount(8 << 20),
          mLastLogPath(mLogDir + "/" +
            MetaDataStore::GetLogSegmentLastFileNamePtr()),
          mLogFilePos(0),
          mLogFilePrevPos(0),
          mLogFileMaxSize(8 << 20),
          mFailureSimulationInterval(0),
          mLogTimeUsec(0),
          mLogTimeOpsCount(0),
          mLogErrorOpsCount(0),
          mPrevLogTimeOpsCount(0),
          mPrevLogTimeUsec(0),
          mPrevLogWriteUsecs(0),
          mLogAvgUsecsNextTimeUsec(0),
          mLog5SecAvgUsec(0),
          mLog10SecAvgUsec(0),
          mLog15SecAvgUsec(0),
          mLog5SecAvgReqRate(0),
          mLog10SecAvgReqRate(0),
          mLog15SecAvgReqRate(0),
          mLogOpWrite5SecAvgUsec(0),
          mLogOpWrite10SecAvgUsec(0),
          mLogOpWrite15SecAvgUsec(0),
          mExceedLogQueueDepthFailureCount(0),
          mPrevExceedLogQueueDepthFailureCount(0),
          mExceedLogQueueDepthFailureCount300SecAvg(0),
          mTotalRequestCount(0),
          mIoCounters(),
          mWorkerIoCounters(),
          mCurIoCounters(),
          mPrepareToForkFlag(false),
          mPrepareToForkDoneFlag(false),
          mVrNodeId(-1),
          mPrepareToForkCond(),
          mForkDoneCond(),
          mRandom(),
          mErrorSimulatorConfig(),
          mTmpBuffer(),
          mLogFileNamePrefix("log"),
          mNotPrimaryErrorMsg(ErrorCodeToString(-ELOGFAILED)),
          mLogWriteErrorMsg(ErrorCodeToString(-EVRNOTPRIMARY)),
          mLogWriterVrBackupErrorMsg(ErrorCodeToString(-EVRBACKUP)),
          mInvalidBlockStartSegmentErrorMsg("invalid block start sequence"),
          mInvalidHeartbeatSequenceErrorMsg("invalid heartbeat sequence"),
          mPrimaryRejectedBlockWriteErrorMsg(
            "block write rejected: VR state is primary"),
          mLogWriterIsNotRunningErrorMsg("log writer is not running"),
          mMaxExceededPendingLogWriteDepthErrorMsg(
            "meta server busy: exceed max transaction log write queue depth"),
          mLogStartViewPrefix(
            string(kLogWriteAheadPrefixPtr) +
            kLogVrStatViewNamePtr +
            kMetaIoPropertiesSeparator
          ),
          mLogAppendPrefixLen(strlen(kLogWriteAheadPrefixPtr)),
          mLogStartViewPrefixPtr(mLogStartViewPrefix.data()),
          mLogStartViewPrefixLen(mLogStartViewPrefix.size()),
          mNetManagerWatcher("LogWriter", mNetManager),
          mWatchdogPtr(0),
          mDebugHistoryCommittedRing(4 << 10)
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
        const char*           inVrResetTypeStrPtr,
        Watchdog*             inWatchdogPtr,
        int                   inMaxReceiverRetryQueueLimit,
        string&               outCurLogFileName)
    {
        const int theError = StartSelf(
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
            inVrResetTypeStrPtr,
            inWatchdogPtr,
            inMaxReceiverRetryQueueLimit,
            outCurLogFileName
        );
        if (0 != theError) {
            mMetaVrSM.Shutdown();
        }
        return theError;
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
            inRequest.statusMsg = mLogWriterIsNotRunningErrorMsg;
            return false;
        }
        if (inRequest.fromClientSMFlag &&
                (mMaxClientOpsPendingCount <= mPendingCount ||
                    mMaxPendingAckByteCount <= mIoCounters.mPendingAckByteCount)
                ) {
            // Shed load / limit log write / replication in flight by failing
            // *all* client requests, regardless if the request itself requires
            // transaction log write or not, as the request processing might, in
            // turn, create request(s) that would require log write (chunk
            // allocation, for example).
            mExceedLogQueueDepthFailureCount++;
            KFS_LOG_STREAM(0 == (mExceedLogQueueDepthFailureCount & 0x1FF) ?
                    MsgLogger::kLogLevelERROR : MsgLogger::kLogLevelDEBUG) <<
                "exceeded max log queue depth: " << mPendingCount <<
               " bytes: "                        <<
                    mIoCounters.mPendingAckByteCount <<
                " failed count: "                <<
                    mExceedLogQueueDepthFailureCount <<
                " "                              << inRequest.Show() <<
            KFS_LOG_EOM;
            inRequest.status    = -ELOGFAILED;
            inRequest.statusMsg = mMaxExceededPendingLogWriteDepthErrorMsg;
            return false;
        }
        int* const theCounterPtr = inRequest.GetLogQueueCounter();
        if (((mPendingCount <= 0 ||
                    ! theCounterPtr || *theCounterPtr <= 0) &&
                (MetaRequest::kLogNever == inRequest.logAction ||
                (MetaRequest::kLogIfOk == inRequest.logAction &&
                    0 != inRequest.status))) && mEnqueueVrStatus == 0 &&
                    inRequest.submitTime < mPrimaryLeaseEndTimeUsec) {
            return false;
        }
        if (theCounterPtr) {
            if (++*theCounterPtr <= 0) {
                panic("log writer enqueue: invalid log queue counter");
            }
        }
        inRequest.commitPendingFlag = true;
        mTotalRequestCount++;
        if (++mPendingCount <= 0) {
            panic("log writer: invalid pending count");
        }
        if (META_LOG_WRITER_CONTROL == inRequest.op) {
            const MetaLogWriterControl& theReq =
                static_cast<const MetaLogWriterControl&>(inRequest);
            if (MetaLogWriterControl::kSetParameters == theReq.type) {
                mDebugHistoryCommittedRing.SetParameters(
                    theReq.paramsPrefix.c_str(), theReq.params);
            }
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
        if (! inRequest.logseq.IsValid() || mStopFlag) {
            return;
        }
        if (inRequest.suspended) {
            panic("request committed: invalid suspended state");
        }
        if (META_VR_LOG_START_VIEW == inRequest.op) {
            // Replayer must invoke SetCommitted() to update committed and
            // view start.
            MetaVrLogStartView& theReq =
                static_cast<MetaVrLogStartView&>(inRequest);
            if (theReq.mNewLogSeq != mViewStartSeq ||
                    0 != theReq.status ||
                    ! theReq.Validate() ||
                    mCommitted.mFidSeed != inFidSeed) {
                panic("request committed: invalid log start view sequence");
                return;
            }
            return;
        }
        if (mCommitted.mSeq.IsValid() && (inRequest.logseq <= mCommitted.mSeq ||
                (inRequest.logseq.IsSameView(mCommitted.mSeq) &&
                    inRequest.logseq.mLogSeq != mCommitted.mSeq.mLogSeq + 1) ||
                (inRequest.logseq.IsSameView(mViewStartSeq) &&
                    inRequest.logseq.mLogSeq <= mViewStartSeq.mLogSeq))) {
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
            mDebugHistoryCommittedRing.Put(mCommitted);
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
        const MetaVrLogSeq& inLastReplayLogSeq,
        const MetaVrLogSeq& inViewStartSeq)
    {
        mCommitted.mSeq       = inLogSeq;
        mCommitted.mErrChkSum = inErrChecksum;
        mCommitted.mFidSeed   = inFidSeed;
        mCommitted.mStatus    = inStatus;
        mReplayLogSeq         = inLastReplayLogSeq;
        mViewStartSeq         = inViewStartSeq;
        mCommitUpdatedFlag    = true;
    }
    void ScheduleFlush()
    {
        if (mPendingQueue.IsEmpty() && ! mCommitUpdatedFlag) {
            return;
        }
        QCStMutexLocker theLock(mMutex);
        const bool theSetReplayStateFlag = mSetReplayStateFlag;
        mPendingCommitted    = mCommitted;
        mPendingReplayLogSeq = mReplayLogSeq;
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
        mMetaVrSM.Shutdown();
        mLogTransmitter.Shutdown();
        NetErrorSimulatorConfigure(mNetManager, 0);
        const string kStatusMsg("canceled due to shutdown");
        Cancel(mInQueue, kStatusMsg);
        mPendingCount -= Cancel(mOutQueue, kStatusMsg);
        mPendingCount -= Cancel(mPendingQueue, kStatusMsg);
        mPendingCount -= Cancel(mPendingAckQueue, kStatusMsg);
        mPendingCount -= Cancel(mReplayCommitQueue, kStatusMsg);
        mPendingCount -= Cancel(mReceiverRetryQueue, kStatusMsg);
        mReceiverRetryQueueSize = 0;
        if (MsgLogger::GetLogger() && MsgLogger::GetLogger()->IsLogLevelEnabled(
                    MsgLogger::kLogLevelDEBUG)) {
            // Force to write debug info, if any, by pretending to transition
            // from primary.
            const bool   thePrimaryFlag = false;
            const int    theVrStatus    = -ELOGFAILED;
            const time_t theNow         = microseconds() / (1000 * 1000);
            mDebugHistoryCommittedRing.Process(
                thePrimaryFlag, theVrStatus, theNow);
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
        theLocker.Unlock();
        if (mMetaDataStorePtr) {
            mMetaDataStorePtr->PrepareToFork();
        }
    }
    void ForkDone()
    {
        if (mMetaDataStorePtr) {
            mMetaDataStorePtr->ForkDone();
        }
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
        if (mMetaDataStorePtr) {
            mMetaDataStorePtr->ChildAtFork();
        }
        mNetManager.ChildAtFork();
        if (0 <= mLogFd) {
            close(mLogFd);
            mLogFd = -1;
        }
    }
    virtual void Notify(
        const MetaVrLogSeq& inSeq,
        int                 inPendingAckByteCount)
    {
        mWokenFlag = mWokenFlag || mTransmitCommitted < inSeq;
        mMetaVrSM.Commit(inSeq);
        if (mTransmitCommitted < inSeq) {
            mTransmitCommitted = inSeq;
        }
        if (inPendingAckByteCount < mWorkerIoCounters.mPendingAckByteCount) {
            mWorkerIoCounters.mPendingAckByteCount = inPendingAckByteCount;
        }
    }
    MetaVrSM& GetMetaVrSM()
        { return mMetaVrSM; }
    int WriteNewLogSegment(
        const char*   inLogDirPtr,
        const Replay& inReplayer,
        string&       outLogSegmentFileName)
    {
        if (inReplayer.getLogNum() < 0 ||
                ! inReplayer.getLastLogSeq().IsValid() ||
                ! inLogDirPtr || 0 == *inLogDirPtr ||
                mThread.IsStarted() || mNetManagerPtr) {
            return -EINVAL;
        }
        mNextBlockChecksum = ComputeBlockChecksum(kKfsNullChecksum, "\n", 1);
        inReplayer.getLastLogBlockCommitted(
            mLastWriteCommitted.mSeq,
            mLastWriteCommitted.mFidSeed,
            mLastWriteCommitted.mStatus,
            mLastWriteCommitted.mErrChkSum
        );
        if (inReplayer.getCommitted() != mLastWriteCommitted.mSeq) {
            panic("log writer: replay committed sequence mismatch");
            return -EFAULT;
        }
        mLogNum     = inReplayer.getLogNum() + 1;
        mLastLogSeq = mLastWriteCommitted.mSeq;
        if (mLastLogSeq.IsPastViewStart()) {
            mLastNonEmptyViewEndSeq = mLastLogSeq;
        }
        QCStValueChanger<string> theChanger(mLogDir, inLogDirPtr);
        NewLog(mLastLogSeq);
        if (! IsLogStreamGood()) {
            Close();
            return mError;
        }
        Replay::CommitQueue theQueue;
        inReplayer.getReplayCommitQueue(theQueue);
        for (Replay::CommitQueue::const_iterator theIt = theQueue.begin();
                theQueue.end() != theIt;
                ++theIt) {
            // Write each entry in a separate log block, to avoid possible
            // problems with block boundaries due to view change and such.
            if (! *theIt) {
                continue;
            }
            const MetaRequest& theOp = **theIt;
            if (! theOp.logseq.IsValid()) {
                continue;
            }
            if (! theOp.WriteLog(mMdStream, mOmitDefaultsFlag)) {
                panic("log writer: invalid request");
                Close();
                mError = -EFAULT;
                break;
            }
            if (! IsLogStreamGood()) {
                break;
            }
            const MetaVrLogSeq& theLogSeq = META_VR_LOG_START_VIEW == theOp.op ?
                static_cast<const MetaVrLogStartView&>(theOp).mNewLogSeq :
                theOp.logseq;
            Checksum theTxChecksum = 0;
            ++mNextBlockSeq;
            WriteBlockTrailer(theLogSeq, mLastWriteCommitted, 1, theTxChecksum);
            LogStreamFlush();
            if (! IsLogStreamGood()) {
                --mNextBlockSeq;
                break;
            }
            mLastLogSeq = theLogSeq;
            mNextLogSeq = mLastLogSeq;
            if (mLastLogSeq.IsPastViewStart()) {
                mLastNonEmptyViewEndSeq = mLastLogSeq;
            }
        }
        const bool theOkFlag = Close() && 0 == mError;
        if (theOkFlag) {
            if (mLastLogSeq < inReplayer.getLastLogSeq()) {
                panic("invalid replay queue or last log sequence");
                return -EFAULT;
            }
            outLogSegmentFileName = mLogName;
        }
        return mError;
    }
    void GetCounters(
        Counters& outCounters)
    {
        outCounters.mDiskWriteTimeUsec  = mIoCounters.mDiskWriteTimeUsec;
        outCounters.mDiskWriteByteCount = mIoCounters.mDiskWriteByteCount;
        outCounters.mDiskWriteCount     = mIoCounters.mDiskWriteCount;
        outCounters.mPendingOpsCount    = mPendingCount;
        outCounters.mLogTimeUsec        = mLogTimeUsec;
        outCounters.mLogTimeOpsCount    = mLogTimeOpsCount;
        outCounters.mLogErrorOpsCount   = mLogErrorOpsCount;
        outCounters.mLog5SecAvgUsec     =
            mLog5SecAvgUsec  >> AverageFilter::kAvgFracBits;
        outCounters.mLog10SecAvgUsec    =
            mLog10SecAvgUsec >> AverageFilter::kAvgFracBits;
        outCounters.mLog15SecAvgUsec    =
            mLog15SecAvgUsec >> AverageFilter::kAvgFracBits;
        outCounters.mLog5SecAvgReqRate  =
            mLog5SecAvgReqRate >> AverageFilter::kAvgFracBits;
        outCounters.mLog10SecAvgReqRate =
            mLog10SecAvgReqRate >> AverageFilter::kAvgFracBits;
        outCounters.mLog15SecAvgReqRate =
            mLog15SecAvgReqRate >> AverageFilter::kAvgFracBits;
        outCounters.mLogOpWrite5SecAvgUsec      =
            mLogOpWrite5SecAvgUsec >> AverageFilter::kAvgFracBits;
        outCounters.mLogOpWrite10SecAvgUsec =
            mLogOpWrite10SecAvgUsec >> AverageFilter::kAvgFracBits;
        outCounters.mLogOpWrite15SecAvgUsec =
            mLogOpWrite15SecAvgUsec >> AverageFilter::kAvgFracBits;
        outCounters.mExceedLogQueueDepthFailureCount =
            mExceedLogQueueDepthFailureCount;
        outCounters.mPendingByteCount = mIoCounters.mPendingAckByteCount;
        outCounters.mTotalRequestCount = mTotalRequestCount;
        outCounters.mExceedLogQueueDepthFailureCount300SecAvg =
            mExceedLogQueueDepthFailureCount300SecAvg >>
            AverageFilter::kAvgFracBits;
    }
    int GetPendingAckBytesOverage() const
    {
        return max(0, (int)(mIoCounters.mPendingAckByteCount -
            mMaxPendingAckByteCount));
    }
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
    enum { kLogAvgIntervalUsec = 1000 * 1000 };
    typedef MetaVrSM::NodeId     NodeId;
    typedef StBufferT<char, 128> TmpBuffer;

    class IoCounters
    {
    public:
        typedef Counters::Counter Counter;

        IoCounters()
            : mDiskWriteTimeUsec(0),
              mDiskWriteByteCount(0),
              mDiskWriteCount(0),
              mPendingAckByteCount(0)
            {}
        Counter mDiskWriteTimeUsec;
        Counter mDiskWriteByteCount;
        Counter mDiskWriteCount;
        Counter mPendingAckByteCount;
    };

    class CommittedRing
    {
    public:
        CommittedRing(
            int inSize)
            : mSize(inSize),
              mEmptyFlag(true),
              mWriteIntervalSec(60),
              mLastWriteTime(0),
              mFileName(),
              mFileNameTmp(),
              mTraceTee(),
              mPos(0),
              mRing()
            {}
        void SetParameters(
            const char*       inParametersPrefixPtr,
            const Properties& inParameters)
        {
            Properties::String theName(
                inParametersPrefixPtr ? inParametersPrefixPtr : "");
            theName.Append("debugCommitted.");
            const size_t       thePrefixLen = theName.length();
            mSize = min(128 << 10, max(0, inParameters.getValue(
                theName.Truncate(thePrefixLen).Append("size"),
                (int)mSize)));
            mWriteIntervalSec = (int64_t)inParameters.getValue(
                theName.Truncate(thePrefixLen).Append("intervalSec"),
                (double)mWriteIntervalSec);
            mFileName = inParameters.getValue(
                theName.Truncate(thePrefixLen).Append("fileName"),
                mFileName);
            mFileNameTmp = mFileName;
            if (! mFileNameTmp.empty()) {
                 mFileNameTmp += ".tmp";
            }
       }
        void Put(
            const Committed& inCommitted)
        {
            if (mRing.size() != mSize) {
                mPos = 0;
                mRing.clear();
                mRing.resize(mSize);
                mEmptyFlag = true;
            }
            if (0 < mSize) {
                mEmptyFlag     = false;
                mLastWriteTime = 0;
                if (mSize <= mPos) {
                    mPos = 0;
                }
                mRing[mPos++] = inCommitted;
            }
        }
        ostream& Write(
            ostream& inStream) const
        {
            if (mEmptyFlag || mRing.empty()) {
                inStream << "history committed: 0\n";
                return inStream;
            }
            size_t                     theSize  = mRing.size();
            Ring::const_iterator       theIt    = mRing.begin() + mPos;
            Ring::const_iterator const theEndIt = theIt;
            if (theIt < mRing.end() && ! theIt->mSeq.IsValid()) {
                theIt = mRing.begin(); // Ring is not full.
                theSize = theEndIt - theIt;
            }
            inStream << "history committed: " << theSize <<
                " log seq: seed: status: error checksum:\n";
            do {
                if (mRing.end() <= theIt) {
                    theIt = mRing.begin();
                }
                const Committed& theCommitted = *theIt;
                inStream <<
                    " " << theCommitted.mSeq <<
                    " " << theCommitted.mFidSeed <<
                    " " << theCommitted.mStatus <<
                    " " << theCommitted.mErrChkSum <<
                "\n";
            } while (++theIt != theEndIt);
            return inStream;
        }
        void Process(
            bool   inPrimaryFlag,
            int    inVrStatus,
            time_t inTimeNow)
        {
            if (mEmptyFlag || (0 == inVrStatus && inPrimaryFlag)) {
                return;
            }
            if (-EVRBACKUP == inVrStatus) {
                mRing.clear();
                mPos = 0;
                mEmptyFlag = true;
                return;
            }
            // On transition from primary periodically write into trace log
            // statuses of the last committed transactions in order to
            // facilitate debugging case where primary and backup commit status
            // diverged, and backups are down.
            if (inTimeNow < mLastWriteTime + mWriteIntervalSec) {
                return;
            }
            mLastWriteTime = inTimeNow;
            if (! mFileNameTmp.empty()) {
                mTraceTee.open(mFileNameTmp.c_str(),
                    ofstream::trunc | ofstream::out);
                if (! mTraceTee) {
                    const int theErr = errno;
                    KFS_LOG_STREAM_ERROR <<
                        "open: " << mFileNameTmp <<
                         ": " << QCUtils::SysError(theErr) <<
                    KFS_LOG_EOM;
                }
            }
            // Use error trace level if the level is less than info to write the
            // debug info into trace log.
            const MsgLogger::LogLevel theLogLevel = (MsgLogger::GetLogger() &&
                MsgLogger::GetLogger()->IsLogLevelEnabled(
                    MsgLogger::kLogLevelINFO)) ?
                MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR;
            KFS_LOG_STREAM_START_TEE(theLogLevel, theLogStream,
                    mTraceTee.is_open() ? &mTraceTee : 0);
                Write(theLogStream.GetStream());
            KFS_LOG_STREAM_END;
            if (mTraceTee.is_open()) {
                mTraceTee.close();
                if (::rename(mFileNameTmp.c_str() , mFileName.c_str())) {
                    const int theErr = errno;
                    KFS_LOG_STREAM_ERROR <<
                        "rename: " << mFileNameTmp << " to: " << mFileName <<
                         ": " << QCUtils::SysError(theErr) <<
                    KFS_LOG_EOM;
                }
            }
        }

    private:
        typedef vector<Committed> Ring;

        size_t   mSize;
        bool     mEmptyFlag;
        int64_t  mWriteIntervalSec;
        int64_t  mLastWriteTime;
        string   mFileName;
        string   mFileNameTmp;
        ofstream mTraceTee;
        size_t   mPos;
        Ring     mRing;

    private:
        CommittedRing(
            const CommittedRing&);
        CommittedRing& operator=(
            const CommittedRing&);
    };

    NetManager*       mNetManagerPtr;
    MetaDataStore*    mMetaDataStorePtr;
    Replay*           mReplayerPtr;
    NetManager        mNetManager;
    LogTransmitter    mLogTransmitter;
    MetaVrSM          mMetaVrSM;
    bool              mPrimaryFlag;
    int               mVrStatus;
    volatile int&     mEnqueueVrStatus;
    volatile int64_t& mPrimaryLeaseEndTimeUsec;
    MetaVrLogSeq      mTransmitCommitted;
    MetaVrLogSeq      mMaxDoneLogSeq;
    Committed         mCommitted;
    int               mCpuAffinityIndex;
    QCThread          mThread;
    QCMutex           mMutex;
    bool              mStopFlag;
    bool              mOmitDefaultsFlag;
    bool              mCommitUpdatedFlag;
    bool              mSetReplayStateFlag;
    int               mMaxBlockSize;
    int               mMaxBlockBytes;
    int               mMaxReceiverRetryQueueLimit;
    int               mMaxReceiverRetryQueueSize;
    int               mPendingCount;
    int               mExraPendingCount;
    int               mReceiverRetryQueueSize;
    string            mLogDir;
    Queue             mPendingQueue;
    Queue             mInQueue;
    Queue             mOutQueue;
    Queue             mPendingAckQueue;
    Queue             mReplayCommitQueue;
    Queue             mReceiverRetryQueue;
    Committed         mPendingCommitted;
    Committed         mInFlightCommitted;
    Committed         mLastWriteCommitted;
    Committed         mReplayLastWriteCommitted;
    Committed         mSetReplayLastWriteCommitted;
    MetaVrLogSeq      mPendingReplayLogSeq;
    MetaVrLogSeq      mReplayLogSeq;
    MetaVrLogSeq      mNextLogSeq;
    MetaVrLogSeq      mLastViewEndSeq;
    MetaVrLogSeq      mLastNonEmptyViewEndSeq;
    MetaVrLogSeq      mLastLogSeq;
    MetaVrLogSeq      mViewStartSeq;
    seq_t             mNextBlockSeq;
    Checksum          mNextBlockChecksum;
    int               mLogFd;
    int               mError;
    MdStream          mMdStream;
    ReqOstream        mReqOstream;
    time_t            mCurLogStartTime;
    MetaVrLogSeq      mCurLogStartSeq;
    seq_t             mLogNum;
    string            mLogName;
    time_t            mLogRotateInterval;
    bool              mPanicOnIoErrorFlag;
    bool              mSyncFlag;
    bool              mWokenFlag;
    int               mMaxClientOpsPendingCount;
    int               mMaxPendingAckByteCount;
    string            mLastLogPath;
    int64_t           mLogFilePos;
    int64_t           mLogFilePrevPos;
    int64_t           mLogFileMaxSize;
    int64_t           mFailureSimulationInterval;
    int64_t           mLogTimeUsec;
    int64_t           mLogTimeOpsCount;
    int64_t           mLogErrorOpsCount;
    int64_t           mPrevLogTimeOpsCount;
    int64_t           mPrevLogTimeUsec;
    int64_t           mPrevLogWriteUsecs;
    int64_t           mLogAvgUsecsNextTimeUsec;
    int64_t           mLog5SecAvgUsec;
    int64_t           mLog10SecAvgUsec;
    int64_t           mLog15SecAvgUsec;
    int64_t           mLog5SecAvgReqRate;
    int64_t           mLog10SecAvgReqRate;
    int64_t           mLog15SecAvgReqRate;
    int64_t           mLogOpWrite5SecAvgUsec;
    int64_t           mLogOpWrite10SecAvgUsec;
    int64_t           mLogOpWrite15SecAvgUsec;
    int64_t           mExceedLogQueueDepthFailureCount;
    int64_t           mPrevExceedLogQueueDepthFailureCount;
    int64_t           mExceedLogQueueDepthFailureCount300SecAvg;
    int64_t           mTotalRequestCount;
    IoCounters        mIoCounters;
    IoCounters        mWorkerIoCounters;
    IoCounters        mCurIoCounters;
    bool              mPrepareToForkFlag;
    bool              mPrepareToForkDoneFlag;
    NodeId            mVrNodeId;
    QCCondVar         mPrepareToForkCond;
    QCCondVar         mForkDoneCond;
    PrngIsaac64       mRandom;
    string            mErrorSimulatorConfig;
    TmpBuffer         mTmpBuffer;
    const string      mLogFileNamePrefix;
    const string      mNotPrimaryErrorMsg;
    const string      mLogWriteErrorMsg;
    const string      mLogWriterVrBackupErrorMsg;
    const string      mInvalidBlockStartSegmentErrorMsg;
    const string      mInvalidHeartbeatSequenceErrorMsg;
    const string      mPrimaryRejectedBlockWriteErrorMsg;
    const string      mLogWriterIsNotRunningErrorMsg;
    const string      mMaxExceededPendingLogWriteDepthErrorMsg;
    const string      mLogStartViewPrefix;
    const size_t      mLogAppendPrefixLen;
    const char* const mLogStartViewPrefixPtr;
    const size_t      mLogStartViewPrefixLen;
    NetManagerWatcher mNetManagerWatcher;
    Watchdog*         mWatchdogPtr;
    CommittedRing     mDebugHistoryCommittedRing;

    int StartSelf(
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
        const char*           inVrResetTypeStrPtr,
        Watchdog*             inWatchdogPtr,
        int                   inMaxReceiverRetryQueueLimit,
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
        mNetManager.SetResolverParameters(
            inNetManager.GetResolverOsFlag(),
            inNetManager.GetResolverCacheSize(),
            inNetManager.GetResolverCacheExpiration()
        );
        mMaxReceiverRetryQueueLimit = inMaxReceiverRetryQueueLimit;
        mDebugHistoryCommittedRing.SetParameters(
            inParametersPrefixPtr, inParameters);
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
        mReplayerPtr->getLastLogBlockCommitted(
            mLastWriteCommitted.mSeq,
            mLastWriteCommitted.mFidSeed,
            mLastWriteCommitted.mStatus,
            mLastWriteCommitted.mErrChkSum
        );
        // Log start view's are all executed at this point -- set last view end
        // to the last committed.
        mLastViewEndSeq              = mCommitted.mSeq;
        mLastNonEmptyViewEndSeq      =
            mReplayerPtr->getLastNonEmptyViewEndSeq();
        mReplayLastWriteCommitted    = mLastWriteCommitted;
        mSetReplayLastWriteCommitted = mReplayLastWriteCommitted;
        mReplayLogSeq                = mReplayerPtr->getLastLogSeq();
        mPendingReplayLogSeq         = mReplayLogSeq;
        mPendingCommitted            = mCommitted;
        mInFlightCommitted           = mPendingCommitted;
        mMetaDataStorePtr            = &inMetaDataStore;
        mViewStartSeq                = mReplayerPtr->getViewStartSeq();
        if (MetaVrLogSeq(0, 0, 0) == mReplayLogSeq) {
            mLastWriteCommitted = mCommitted;
        }
        if (! mCommitted.mSeq.IsPastViewStart() ||
                ! mLastWriteCommitted.mSeq.IsPastViewStart() ||
                ! mLastNonEmptyViewEndSeq.IsPastViewStart()) {
            panic("log writer: invalid replay committed sequence");
            return -EINVAL;
        }
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
            const int64_t theStart = microseconds();
            if ((mLogFd = open(mLogName.c_str(), O_WRONLY, 0666)) < 0) {
                IoError(errno);
                return mError;
            }
            mWorkerIoCounters.mDiskWriteTimeUsec += microseconds() - theStart;
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
        mVrNodeId         = mMetaVrSM.GetNodeId();
        outCurLogFileName = mLogName;
        mStopFlag         = false;
        mNetManagerPtr    = &inNetManager;
        if (inVrResetTypeStrPtr) {
            if (mMetaVrSM.GetConfig().IsEmpty()) {
                KFS_LOG_STREAM_ERROR <<
                    "VR configuration is empty" <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            // Write configuration reset followed by log start view.
            MetaVrReconfiguration& theRcOp = *(new MetaVrReconfiguration());
            theRcOp.mOpType = inVrResetTypeStrPtr;
            if (theRcOp.mOpType ==
                    MetaVrReconfiguration::GetInactivateAllNodesName()) {
                if (mMetaVrSM.GetQuorum() <= 0) {
                    KFS_LOG_STREAM_ERROR <<
                        "VR no active nodes, quorum: " << mMetaVrSM.GetQuorum() <<
                    KFS_LOG_EOM;
                    MetaRequest::Release(&theRcOp);
                    return -EINVAL;
                }
            } else if (theRcOp.mOpType != MetaVrReconfiguration::GetResetOpName()) {
                KFS_LOG_STREAM_ERROR <<
                    "VR invalid reset op type: " << theRcOp.mOpType <<
                KFS_LOG_EOM;
                MetaRequest::Release(&theRcOp);
                return -EINVAL;
            }
            theRcOp.logseq = mLastLogSeq;
            theRcOp.logseq.mLogSeq++;
            MetaVrLogStartView& theSvOp = *(new MetaVrLogStartView());
            theSvOp.logseq = theRcOp.logseq;
            theSvOp.logseq.mLogSeq++;
            theSvOp.mNodeId             = mVrNodeId < 0 ? NodeId(0) : mVrNodeId;
            theSvOp.mTime               = mNetManager.Now(),
            theSvOp.mCommittedSeq       = theRcOp.logseq;
            theSvOp.mNewLogSeq          = MetaVrLogSeq(
                theSvOp.mCommittedSeq.mEpochSeq + 1,
                kMetaVrLogStartEpochViewSeq,
                kMetaVrLogStartViewLogSeq
            );
            if (! theSvOp.Validate()) {
                panic("log writer: invalid VR log start view op");
                mError = -EFAULT;
            }
            theRcOp.next = &theSvOp;
            ostream& theStream = mMdStream;
            MetaRequest* thePtr = &theRcOp;
            while (thePtr) {
                MetaRequest& theOp = *thePtr;
                thePtr = theOp.next;
                theOp.next = 0;
                if (IsLogStreamGood()) {
                    if (! theOp.WriteLog(theStream, mOmitDefaultsFlag)) {
                        panic("log writer: invalid request");
                        mError = -EFAULT;
                    }
                    if (IsLogStreamGood()) {
                        const MetaVrLogSeq& theLogSeq = &theOp == &theSvOp ?
                            theSvOp.mNewLogSeq : theOp.logseq;
                        ++mNextBlockSeq;
                        Checksum theTxChecksum = 0;
                        WriteBlockTrailer(
                            theLogSeq, mCommitted, 1, theTxChecksum);
                        LogStreamFlush();
                        if (IsLogStreamGood()) {
                            mLastLogSeq = theLogSeq;
                            mNextLogSeq = mLastLogSeq;
                            if (mLastLogSeq.IsPastViewStart()) {
                                mLastNonEmptyViewEndSeq = mLastLogSeq;
                            }
                        } else {
                            --mNextBlockSeq;
                        }
                    }
                }
                MetaRequest::Release(&theOp);
            }
            if (! IsLogStreamGood()) {
                KFS_LOG_STREAM_ERROR <<
                    "VR reset transaction log write has failed" <<
                KFS_LOG_EOM;
            }
            Close();
            if (0 == mError) {
                KFS_LOG_STREAM_INFO <<
                    "VR configuration: " << inVrResetTypeStrPtr <<
                    " transaction log write complete" <<
                KFS_LOG_EOM;
            }
            return mError;
        }
        mEnqueueVrStatus = mMetaVrSM.GetStatus();
        mPrimaryLeaseEndTimeUsec = int64_t(1000) * 1000 * (mNetManager.Now() +
            (0 == mEnqueueVrStatus ? 2 : -(24 * 60 * 60)));
        mLogAvgUsecsNextTimeUsec = microseconds() + kLogAvgIntervalUsec;
        mWatchdogPtr = inWatchdogPtr;
        const int kStackSize = 128 << 10;
        mThread.Start(this, kStackSize, "MetaLogWriter",
            QCThread::CpuAffinity(mCpuAffinityIndex));
        mNetManagerPtr->RegisterTimeoutHandler(this);
        return 0;
    }
    int Cancel(
        Queue&        inQueue,
        const string& inStatusMsg)
    {
        Queue theQueue;
        theQueue.Swap(inQueue);
        int          theCnt = 0;
        MetaRequest* thePtr;
        while ((thePtr = theQueue.PopFront())) {
            thePtr->status    = -ECANCELED;
            thePtr->statusMsg = inStatusMsg;
            KFS_LOG_STREAM_DEBUG <<
                inStatusMsg <<
                " " << thePtr->Show() <<
            KFS_LOG_EOM;
            submit_request(thePtr);
            theCnt++;
        }
        return theCnt;
    }
    static int64_t CalcLogAvg(
        int64_t inAvg,
        int64_t inSample,
        int64_t inExponent)
        { return AverageFilter::Calculate(inAvg, inSample, inExponent); }
    void UpdateLogAvg(
        int64_t inTimeNowUsec)
    {
        if (inTimeNowUsec < mLogAvgUsecsNextTimeUsec) {
            return;
        }
        const int64_t theOpsCount      =
            mLogTimeOpsCount - mPrevLogTimeOpsCount;
        const int64_t theLogUsecs      = mLogTimeUsec     - mPrevLogTimeUsec;
        const int64_t theLogWriteUsecs =
            mIoCounters.mDiskWriteTimeUsec - mPrevLogWriteUsecs;
        const int64_t theOpLogRate     = (theOpsCount << Counters::kRateFracBits) *
            1000 * 1000 / (kLogAvgIntervalUsec +
                inTimeNowUsec - mLogAvgUsecsNextTimeUsec);
        mPrevLogTimeOpsCount = mLogTimeOpsCount;
        mPrevLogTimeUsec     = mLogTimeUsec;
        mPrevLogWriteUsecs   = mIoCounters.mDiskWriteTimeUsec;
        const int64_t theOpLogUsecs = 0 < theOpsCount ?
            theLogUsecs / theOpsCount : int64_t(0);
        const int64_t theOpLogWriteUsecs = 0 < theOpsCount ?
            theLogWriteUsecs / theOpsCount : int64_t(0);
        const int64_t theDroppedCountDelta = mExceedLogQueueDepthFailureCount -
            mPrevExceedLogQueueDepthFailureCount;
        mPrevExceedLogQueueDepthFailureCount = mExceedLogQueueDepthFailureCount;
        while (mLogAvgUsecsNextTimeUsec <= inTimeNowUsec) {
            mLog5SecAvgUsec  = CalcLogAvg(mLog5SecAvgUsec, theOpLogUsecs,
                AverageFilter::kAvg5SecondsDecayExponent);
            mLog10SecAvgUsec = CalcLogAvg(mLog10SecAvgUsec, theOpLogUsecs,
                AverageFilter::kAvg10SecondsDecayExponent);
            mLog15SecAvgUsec = CalcLogAvg(mLog15SecAvgUsec, theOpLogUsecs,
                AverageFilter::kAvg15SecondsDecayExponent);
            mLog5SecAvgReqRate  = CalcLogAvg(mLog5SecAvgReqRate, theOpLogRate,
                AverageFilter::kAvg5SecondsDecayExponent);
            mLog10SecAvgReqRate = CalcLogAvg(mLog10SecAvgReqRate, theOpLogRate,
                AverageFilter::kAvg10SecondsDecayExponent);
            mLog15SecAvgReqRate = CalcLogAvg(mLog15SecAvgReqRate, theOpLogRate,
                AverageFilter::kAvg15SecondsDecayExponent);
            mLogOpWrite5SecAvgUsec = CalcLogAvg(mLogOpWrite5SecAvgUsec,
                theOpLogWriteUsecs,
                AverageFilter::kAvg5SecondsDecayExponent);
            mLogOpWrite10SecAvgUsec = CalcLogAvg(mLogOpWrite10SecAvgUsec,
                theOpLogWriteUsecs,
                AverageFilter::kAvg10SecondsDecayExponent);
            mLogOpWrite15SecAvgUsec = CalcLogAvg(mLogOpWrite15SecAvgUsec,
                theOpLogWriteUsecs,
                AverageFilter::kAvg15SecondsDecayExponent);
            mExceedLogQueueDepthFailureCount300SecAvg = CalcLogAvg(
                mExceedLogQueueDepthFailureCount300SecAvg,
                theDroppedCountDelta,
                AverageFilter::kAvg15SecondsDecayExponent);
            mLogAvgUsecsNextTimeUsec += kLogAvgIntervalUsec;
        }
    }
    virtual void Timeout()
    {
        mDebugHistoryCommittedRing.Process(
            mPrimaryFlag, mEnqueueVrStatus, mNetManagerPtr->Now());
        if (mPendingCount <= 0 && ! mSetReplayStateFlag) {
            UpdateLogAvg(mNetManagerPtr->NowUsec());
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
        if (theSetReplayStateFlag) {
            mSetReplayLastWriteCommitted = mReplayLastWriteCommitted;
        }
        mIoCounters = mCurIoCounters;
        theLock.Unlock();
        if (theWakeupFlag) {
            mNetManager.Wakeup();
        }
        Queue theReplayQueue;
        MetaRequest*  thePtr;
        int64_t const theStartTime     = microseconds();
        bool          theFirstItemFlag = true;
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
            if (theSetReplayStateFlag &&
                    META_LOG_WRITER_CONTROL == thePtr->op &&
                    thePtr->replayBypassFlag &&
                    0 == thePtr->status &&
                    MetaLogWriterControl::kWriteBlock ==
                        static_cast<MetaLogWriterControl*>(thePtr)->type) {
                // Run after setting replay state.
                theReplayQueue.PushBack(*thePtr);
            } else if (IsMetaLogWriteOrVrError(thePtr->status) ||
                    thePtr->replayBypassFlag ||
                    ! mReplayerPtr->submit(*thePtr)) {
                const int64_t theUsecsNow = theFirstItemFlag ?
                    theStartTime : microseconds();
                theFirstItemFlag = false;
                if (META_LOG_WRITER_CONTROL != thePtr->op) {
                    if (0 == thePtr->status) {
                        mLogTimeUsec += theStartTime - theReq.submitTime;
                        mLogTimeOpsCount++;
                    } else {
                        mLogErrorOpsCount++;
                    }
                }
                theReq.Submit(theUsecsNow);
            }
        }
        UpdateLogAvg(theStartTime);
        if (theSetReplayStateFlag) {
            thePtr = theReplayCommitHeadPtr;
            while (thePtr) {
                if (--mPendingCount < 0) {
                    panic("log writer: set replay state invalid pending count");
                }
                thePtr = thePtr->next;
            }
            mReplayerPtr->setReplayState(
                mCommitted.mSeq,
                mViewStartSeq,
                mCommitted.mFidSeed,
                mCommitted.mStatus,
                mCommitted.mErrChkSum,
                theReplayCommitHeadPtr,
                mSetReplayLastWriteCommitted.mSeq,
                mSetReplayLastWriteCommitted.mFidSeed,
                mSetReplayLastWriteCommitted.mStatus,
                mSetReplayLastWriteCommitted.mErrChkSum,
                mLastNonEmptyViewEndSeq
            );
            while ((thePtr = theReplayQueue.PopFront())) {
                submit_request(thePtr);
            }
        }
    }
    void ProcessPendingAckQueue(
        Queue& inDoneQueue,
        bool   inSetReplayStateFlag,
        int    inExtraReqCount,
        bool   inHasReplayBypassFlag)
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
        if (thePtr) {
            if (inHasReplayBypassFlag) {
                // Move into done queue entries that bypass replay.
                thePrevPtr = thePtr;
                thePtr     = thePtr->next;
                while (thePtr) {
                    MetaRequest* const theNextPtr = thePtr->next;
                    if (thePtr->replayBypassFlag &&
                            ! thePtr->logseq.IsValid()) {
                        thePrevPtr->next = theNextPtr;
                        thePtr->next = 0;
                        inDoneQueue.PushBack(*thePtr);
                    } else {
                        thePrevPtr = thePtr;
                    }
                    thePtr = theNextPtr;
                }
                mPendingAckQueue.Set(mPendingAckQueue.Front(), thePrevPtr);
            }
        } else {
            inDoneQueue.PushBack(mPendingAckQueue);
        }
        if (inDoneQueue.IsEmpty() && 0 == inExtraReqCount &&
                (0 == mVrStatus || mPrimaryFlag ||
                (! inSetReplayStateFlag && mPendingAckQueue.IsEmpty()))) {
            return;
        }
        thePtr     = 0;
        thePrevPtr = 0;
        int theVrStatus = mVrStatus;
        if (0 != mVrStatus && 0 == (theVrStatus = mMetaVrSM.GetStatus()) &&
                ! mPrimaryFlag) {
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
        if (0 != mVrStatus && ! mPrimaryFlag) {
            mReplayCommitQueue.PushBack(mPendingAckQueue);
            if (! mSetReplayStateFlag) {
                mSetReplayStateFlag =
                    inSetReplayStateFlag || ! mReplayCommitQueue.IsEmpty();
                if (mSetReplayStateFlag) {
                    mReplayLastWriteCommitted = mLastWriteCommitted;
                    KFS_LOG_STREAM_DEBUG <<
                        "scheduling set replay state:"
                        " replay queue empty: " <<
                            mReplayCommitQueue.IsEmpty() <<
                        " set replay state: " << inSetReplayStateFlag <<
                        " log committed: "    <<
                            mReplayLastWriteCommitted.mSeq <<
                    KFS_LOG_EOM;
                }
            }
            if (! thePtr && 0 == theVrStatus) {
                mVrStatus        = theVrStatus;
                mEnqueueVrStatus = theVrStatus;
                mPrimaryFlag     = true;
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
        mCurIoCounters = mWorkerIoCounters;
        const bool theStopFlag = mStopFlag;
        if (theStopFlag) {
            mSetReplayStateFlag = false;
            mNetManager.Shutdown();
        }
        if (mSetReplayStateFlag) {
            KFS_LOG_STREAM_DEBUG <<
                "set replay state pending"
                " write queue empty: " << mInQueue.IsEmpty() <<
            KFS_LOG_EOM;
            theLocker.Unlock();
            mMetaVrSM.ProcessReplay(mNetManager.Now());
            return;
        }
        mInFlightCommitted = mPendingCommitted;
        const MetaVrLogSeq theReplayLogSeq = mPendingReplayLogSeq;
        Queue              theWriteQueue;
        mInQueue.Swap(theWriteQueue);
        theLocker.Unlock();
        mWokenFlag = true;
        if (theStopFlag) {
            mMetaVrSM.Shutdown();
        }
        int theVrStatus = mVrStatus;
        MetaRequest* theReqPtr = 0;
        const time_t thePrimaryLeaseEndTime = mMetaVrSM.Process(
            mNetManager.Now(),
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
        if (0 != theVrStatus) {
            if (mEnqueueVrStatus != theVrStatus) {
                mEnqueueVrStatus = theVrStatus;
                SyncAddAndFetch(mEnqueueVrStatus, 0);
            }
            mVrStatus = theVrStatus;
        } else {
            SyncAddAndFetch(mPrimaryLeaseEndTimeUsec,
                int64_t(thePrimaryLeaseEndTime) * 1000 * 1000 -
                mPrimaryLeaseEndTimeUsec);
        }
        const bool theVrBecameNonPrimaryFlag =
            mPrimaryFlag && 0 != mVrStatus && ! mMetaVrSM.IsPrimary();
        if (theVrBecameNonPrimaryFlag) {
            mPrimaryFlag = false;
            Queue theTmp;
            ProcessPendingAckQueue(theTmp, theVrBecameNonPrimaryFlag, 0, false);
        }
        const bool theHasReplayBypassFlag = ! theWriteQueue.IsEmpty() &&
            Write(theWriteQueue);
        mWokenFlag = false;
        ProcessPendingAckQueue(theWriteQueue, false, theReqPtr ? 1 : 0,
            theHasReplayBypassFlag);
    }
    virtual void DispatchEnd()
    {
        if (0 == mVrStatus) {
            const int theVrStatus = mMetaVrSM.GetStatus();
            if (0 != theVrStatus) {
                mVrStatus        = theVrStatus;
                mEnqueueVrStatus = theVrStatus;
                SyncAddAndFetch(mEnqueueVrStatus, 0);
            }
        }
        const bool theVrBecameNonPrimaryFlag =
            mPrimaryFlag && 0 != mVrStatus && ! mMetaVrSM.IsPrimary();
        if (theVrBecameNonPrimaryFlag) {
            mPrimaryFlag = false;
        }
        if (mWokenFlag || theVrBecameNonPrimaryFlag) {
            Queue theTmp;
            ProcessPendingAckQueue(theTmp, theVrBecameNonPrimaryFlag, 0, false);
        }
    }
    virtual void DispatchExit()
        {}
    virtual void Run()
    {
        QCMutex* const kMutexPtr             = 0;
        bool const     kWakeupAndCleanupFlag = true;
        if (mWatchdogPtr) {
            mWatchdogPtr->Register(mNetManagerWatcher);
        }
        mNetManager.MainLoop(kMutexPtr, kWakeupAndCleanupFlag, this);
        if (mWatchdogPtr) {
            mWatchdogPtr->Unregister(mNetManagerWatcher);
        }
        Sync();
        Close();
    }
    bool Write(
        Queue& inQueue)
    {
        if (! IsLogStreamGood()) {
            if (mCurLogStartSeq < mNextLogSeq) {
                StartNextLog();
            } else {
                NewLog(mNextLogSeq);
            }
        }
        const MetaVrLogSeq theLastLogSeq          = mLastLogSeq;
        bool               theRunRetryQueueFlag   = false;
        bool               theHasReplayBypassFlag = false;
        ostream&           theStream              = mMdStream;
        MetaRequest*       theCurPtr              = inQueue.Front();
        MetaRequest*       theCtlPrevPtr          = 0;
        bool               theWriteOkFlag         = 0 == mVrStatus;
        while (theCurPtr) {
            mLastLogSeq = mNextLogSeq;
            MetaRequest*          theVrPtr               = 0;
            MetaRequest*          thePtr                 = theCurPtr;
            seq_t                 theEndBlockSeq         =
                mNextLogSeq.mLogSeq + mMaxBlockSize;
            const bool            theSimulateFailureFlag = IsSimulateFailure();
            bool                  theFailureInjectedFlag = false;
            bool                  theStartViewFlag       = false;
            MetaLogWriterControl* theCtlPtr              = 0;
            for (MetaRequest* thePrevPtr = 0;
                    thePtr;
                    thePrevPtr = thePtr, theCtlPrevPtr = thePtr,
                    thePtr = thePtr->next) {
                theHasReplayBypassFlag = theHasReplayBypassFlag ||
                    thePtr->replayBypassFlag;
                if (mMetaVrSM.Handle(*thePtr, mLastLogSeq)) {
                    theHasReplayBypassFlag = theHasReplayBypassFlag ||
                        thePtr->replayBypassFlag;
                    theVrPtr = thePtr;
                    break;
                }
                if (META_LOG_WRITER_CONTROL == thePtr->op) {
                    theCtlPtr = static_cast<MetaLogWriterControl*>(thePtr);
                    if (Control(*theCtlPtr)) {
                        break;
                    }
                    theCtlPtr = 0;
                    continue;
                }
                if (! theStream) {
                    continue;
                }
                if (META_VR_LOG_START_VIEW == thePtr->op) {
                    // Ensure that VR log start view the only op in the log
                    // block.
                    if (thePrevPtr) {
                        thePtr = thePrevPtr;
                        break;
                    }
                    theStartViewFlag = true;
                } else if (! theWriteOkFlag) {
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
                        theFailureInjectedFlag = true;
                        break;
                    }
                    ++mLastLogSeq.mLogSeq;
                    thePtr->logseq = mLastLogSeq;
                    if (! thePtr->WriteLog(theStream, mOmitDefaultsFlag)) {
                        panic("log writer: invalid request");
                    }
                    if (! theStream) {
                        --mLastLogSeq.mLogSeq;
                        LogError(*thePtr);
                    }
                }
                if (theEndBlockSeq <= mLastLogSeq.mLogSeq ||
                        theStartViewFlag ||
                        (META_VR_RECONFIGURATION == thePtr->op &&
                            thePtr->logseq.IsValid()) ||
                        mMdStream.GetBufferedStart() + mMaxBlockBytes <=
                            mMdStream.GetBufferedEnd() ||
                        mLogFileMaxSize <= mLogFilePos +
                            (mMdStream.GetBufferedEnd() -
                                mMdStream.GetBufferedStart())) {
                    break;
                }
            }
            const MetaVrLogSeq thePrevLastViewEndSeq = mLastViewEndSeq;
            MetaRequest* const theEndPtr = thePtr ? thePtr->next : thePtr;
            if ((mNextLogSeq < mLastLogSeq || theFailureInjectedFlag) &&
                    (theWriteOkFlag || theStartViewFlag) && IsLogStreamGood()) {
                const int theBlkLen =
                    (int)(mLastLogSeq.mLogSeq - mNextLogSeq.mLogSeq);
                if (theStartViewFlag && ! theFailureInjectedFlag) {
                    MetaVrLogStartView& theOp =
                        *static_cast<MetaVrLogStartView*>(theCurPtr);
                    if (theOp.logseq != mLastLogSeq || ! theOp.Validate() ||
                            0 != theOp.status) {
                        panic("log writer: invalid VR log start view");
                    }
                    mLastLogSeq     = theOp.mNewLogSeq;
                    mLastViewEndSeq = theOp.mCommittedSeq;
                    KFS_LOG_STREAM_DEBUG <<
                        "writing: " << theOp.Show() <<
                    KFS_LOG_EOM;
                }
                FlushBlock(mLastLogSeq, theBlkLen, theFailureInjectedFlag);
            }
            if ((theWriteOkFlag || theStartViewFlag) && IsLogStreamGood() &&
                    ! theFailureInjectedFlag) {
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
                } else if (thePtr &&
                        META_VR_RECONFIGURATION == thePtr->op &&
                        thePtr->logseq.IsValid() && 0 != mMetaVrSM.GetStatus()) {
                    if (thePtr->logseq != mNextLogSeq) {
                        panic("log writer: VR reconfiguration"
                            " is not at the end of log block");
                    }
                    KFS_LOG_STREAM_INFO <<
                        "last log: "   << mNextLogSeq <<
                        " VR status: " << mMetaVrSM.GetStatus() <<
                        " primary: "   << mMetaVrSM.IsPrimary() <<
                        " / "          << mPrimaryFlag <<
                        " "            << thePtr->Show() <<
                    KFS_LOG_EOM;
                    // Fail the remaining ops, if any, in the log block.
                    theWriteOkFlag = false;
                }
            } else {
                // Write failure.
                mLastLogSeq     = mNextLogSeq;
                mLastViewEndSeq = thePrevLastViewEndSeq;
                MetaRequest* const theLastPtr = theVrPtr ? theVrPtr : theEndPtr;
                for (thePtr = theCurPtr;
                        theLastPtr != thePtr;
                        thePtr = thePtr->next) {
                    if (META_LOG_WRITER_CONTROL != thePtr->op &&
                            (0 != mVrStatus ||
                            ((MetaRequest::kLogIfOk == thePtr->logAction &&
                                0 == thePtr->status) ||
                            MetaRequest::kLogAlways == thePtr->logAction))) {
                        LogError(*thePtr);
                    }
                }
            }
            if (theCtlPtr &&
                    MetaLogWriterControl::kWriteBlock == theCtlPtr->type) {
                bool theRetryFlag = WriteBlock(*theCtlPtr) &&
                    0 < mMaxReceiverRetryQueueSize &&
                    theCtlPtr->logReceiverFlag;
                if (theRetryFlag) {
                    const MetaLogWriterControl* const theBackPtr =
                        static_cast<const MetaLogWriterControl*>(
                            mReceiverRetryQueue.Back());
                    theRetryFlag = ! theBackPtr || (
                        theBackPtr->blockEndSeq <= theCtlPtr->blockStartSeq &&
                        theBackPtr->blockStartSeq != theCtlPtr->blockStartSeq);
                }
                theCtlPtr->primaryNodeId =
                    mMetaVrSM.GetPrimaryNodeId(mLastLogSeq);
                theCtlPtr->lastLogSeq    = mLastLogSeq;
                KFS_LOG_STREAM_DEBUG <<
                    "pending: "  << mReceiverRetryQueueSize <<
                    " last: "    << mLastLogSeq <<
                    " primary: " << theCtlPtr->primaryNodeId <<
                    " done: "    << reinterpret_cast<const void*>(theCtlPtr) <<
                    " retry: "   << theRetryFlag <<
                    " "          << theCtlPtr->Show() <<
                KFS_LOG_EOM;
                if (theRetryFlag) {
                    // Move from input to retry queue.
                    if (theCtlPrevPtr) {
                        if (theCtlPtr->next) {
                            theCtlPrevPtr->next = theCtlPtr->next;
                            theCtlPtr->next = 0;
                        } else {
                            if (inQueue.Back() != theCtlPtr) {
                                panic("invalid control back RPC pointer");
                            }
                            theCtlPrevPtr->next = 0;
                            inQueue.Set(inQueue.Front(), theCtlPrevPtr);
                        }
                    } else if (inQueue.PopFront() != theCtlPtr) {
                        panic("invalid control front RPC pointer");
                    }
                    ++mReceiverRetryQueueSize;
                    mReceiverRetryQueue.PushBack(*theCtlPtr);
                    theCurPtr = theCtlPrevPtr;
                } else if (0 == theCtlPtr->status &&
                        theCtlPtr->logReceiverFlag) {
                    // Trigger cleanup on successful write or heartbeat.
                    theRunRetryQueueFlag = true;
                }
            }
            theCtlPrevPtr = theCurPtr;
            theCurPtr     = theEndPtr;
            if (theCurPtr && mLogFileMaxSize <= mLogFilePos &&
                    mCurLogStartSeq < mNextLogSeq && IsLogStreamGood()) {
                StartNextLog();
            }
        }
        if (mCurLogStartSeq < mNextLogSeq && IsLogStreamGood() &&
                (mLogFileMaxSize <= mLogFilePos ||
                mCurLogStartTime + mLogRotateInterval < mNetManager.Now())) {
            StartNextLog();
        }
        ProcessReceiverRetryQueue(inQueue,
            theRunRetryQueueFlag || theLastLogSeq != mLastLogSeq);
        return theHasReplayBypassFlag;
    }
    void ProcessReceiverRetryQueue(
        Queue& inQueue,
        bool   inRetryFlag)
    {
        if (mReceiverRetryQueue.IsEmpty()) {
            return;
        }
        const int theSize = 0 == mVrStatus ? 0 : mMaxReceiverRetryQueueSize;
        if (! inRetryFlag && mReceiverRetryQueueSize <= theSize) {
            return;
        }
        vrNodeId_t   thePrimaryNodeId = mMetaVrSM.GetPrimaryNodeId(mLastLogSeq);
        MetaRequest* thePtr;
        while ((thePtr = mReceiverRetryQueue.Front())) {
            if (mReceiverRetryQueueSize <= 0) {
                panic("invalid receiver retry queue counter");
            }
            if (META_LOG_WRITER_CONTROL != thePtr->op) {
                panic("invalid receiver retry queue entry");
            }
            MetaLogWriterControl& theReq        =
                *static_cast<MetaLogWriterControl*>(thePtr);
            bool                  theRemoveFlag = theSize <= 0 ||
                theReq.blockEndSeq <= mLastLogSeq;
            const bool            theRetryFlag  = ! theRemoveFlag &&
                inRetryFlag && theReq.blockStartSeq <= mLastLogSeq;
            if (theRetryFlag) {
                theReq.status    = 0;
                theReq.statusMsg = string();
                theRemoveFlag = ! WriteBlock(theReq);
                thePrimaryNodeId = mMetaVrSM.GetPrimaryNodeId(mLastLogSeq);
                theReq.primaryNodeId = thePrimaryNodeId;
                theReq.lastLogSeq    = mLastLogSeq;
                KFS_LOG_STREAM_DEBUG <<
                    "pending: "  << mReceiverRetryQueueSize <<
                    " retried:"
                    " last: "    << mLastLogSeq <<
                    " primary: " << theReq.primaryNodeId <<
                    " done: "    << reinterpret_cast<const void*>(&theReq) <<
                    " retry: "   << ! theRemoveFlag <<
                    " "          << theReq.Show() <<
                KFS_LOG_EOM;
            }
            if (! theRemoveFlag && mReceiverRetryQueueSize <= theSize) {
                break;
            }
            if (! theRetryFlag) {
                theReq.primaryNodeId = thePrimaryNodeId;
                theReq.lastLogSeq    = mLastLogSeq;
            }
            if (mReceiverRetryQueue.PopFront() != thePtr) {
                panic("invalid retry queue or RPC pointer");
            }
            --mReceiverRetryQueueSize;
            inQueue.PushBack(*thePtr);
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
        } else if (-EVRBACKUP == mVrStatus) {
            inReq.status    = mVrStatus;
            inReq.statusMsg = mLogWriterVrBackupErrorMsg;
        } else {
            inReq.status    = -ELOGFAILED;
            inReq.statusMsg = mLogWriteErrorMsg;
        }
    }
    void FlushBlock(
        const MetaVrLogSeq& inLogSeq,
        int                 inBlockLen            = -1,
        bool                inSimulateFailureFlag = false)
    {
        const int theBlockLen = inBlockLen < 0 ?
            (int)(inLogSeq.mLogSeq - mNextLogSeq.mLogSeq) : inBlockLen;
        if (theBlockLen <= 0 && ! inSimulateFailureFlag) {
            panic("invalid log block start sequence or length");
        }
        const int theVrStatus = mMetaVrSM.HandleLogBlock(
            mNextLogSeq,
            inLogSeq,
            mInFlightCommitted.mSeq,
            mVrNodeId
        );
        ++mNextBlockSeq;
        KFS_LOG_STREAM_DEBUG <<
            "flush block: " << inLogSeq <<
            " seq: "        << mNextBlockSeq <<
            " len: "        << theBlockLen <<
            " VR status: "  << theVrStatus <<
            " "             << ErrorCodeToString(theVrStatus) <<
        KFS_LOG_EOM;
        Checksum     theTxChecksum = 0;
        const size_t theTxLen      = theBlockLen <= 0 ? size_t(0) :
            WriteBlockTrailer(
                inLogSeq, mInFlightCommitted, theBlockLen, theTxChecksum);
        // Here only transmit is conditional on possible VR status change in
        // primary write path, as mVrStatus takes precedence.
        if (0 == theVrStatus && 0 < theTxLen) {
            const char* const thePtr = mMdStream.GetBufferedStart();
            int theStatus;
            if (mMdStream.GetBufferedEnd() < thePtr + theTxLen) {
                panic("invalid log write write buffer length");
                theStatus = -EFAULT;
            } else {
                mWorkerIoCounters.mPendingAckByteCount += theTxLen;
                theStatus = mLogTransmitter.TransmitBlock(
                    inLogSeq,
                    theBlockLen,
                    thePtr,
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
        const bool theUpdateFlag = IsLogStreamGood() && 0 < theBlockLen;
        if (theUpdateFlag) {
            mLastWriteCommitted = mInFlightCommitted;
            if (inLogSeq.IsPastViewStart()) {
                mLastNonEmptyViewEndSeq = inLogSeq;
            }
        } else {
            --mNextBlockSeq;
        }
        const NodeId thePrimaryNodeId = mMetaVrSM.LogBlockWriteDone(
            mNextLogSeq,
            inLogSeq,
            mInFlightCommitted.mSeq,
            mLastViewEndSeq,
            ! inSimulateFailureFlag && IsLogStreamGood()
        );
        if (theUpdateFlag) {
            mLogTransmitter.NotifyAck(mVrNodeId, inLogSeq, thePrimaryNodeId);
        }
    }
    size_t WriteBlockTrailer(
        const MetaVrLogSeq& inLogSeq,
        const Committed&    inCommitted,
        int                 inBlockLen,
        Checksum&           outTxChecksum)
    {
        mReqOstream << "c"
            "/" << inCommitted.mSeq <<
            "/" << inCommitted.mFidSeed <<
            "/" << inCommitted.mErrChkSum <<
            "/" << inCommitted.mStatus <<
            "/" << inLogSeq <<
            "/" << inBlockLen <<
            "/"
        ;
        mReqOstream.flush();
        const char*  theStartPtr      = mMdStream.GetBufferedStart();
        const size_t theTxLen         =
            mMdStream.GetBufferedEnd() - theStartPtr;
        Checksum     theBlockChecksum = ComputeBlockChecksum(
            0 < mNextBlockSeq ? mNextBlockChecksum : kKfsNullChecksum,
            theStartPtr, theTxLen);
        outTxChecksum = theBlockChecksum;
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
        return theTxLen;
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
            case MetaLogWriterControl::kLogFetchDone:
                return false;
            case MetaLogWriterControl::kSetParameters:
                mNetManager.SetResolverParameters(
                    inRequest.resolverOsFlag,
                    inRequest.resolverCacheSize,
                    inRequest.resolverCacheExpiration
                );
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
    void ClearBufferAndSetError(
        MetaLogWriterControl& inRequest,
        int                   inStatus,
        const char*           inMsgPtr)
    {
        mMdStream.ClearBuffer();
        --mNextBlockSeq;
        inRequest.status    = inStatus;
        inRequest.statusMsg = inMsgPtr;
    }
    bool WriteBlock(
        MetaLogWriterControl& inRequest)
    {
        if (mNextBlockSeq < 0) {
            panic("log writer: write block: invalid block sequence");
            inRequest.status = -EFAULT;
            return false;
        }
        if (mLastLogSeq != mNextLogSeq) {
            panic("invalid write block invocation");
            inRequest.status = -EFAULT;
            return false;
        }
        if (0 != inRequest.status) {
            KFS_LOG_STREAM_ERROR <<
                "write block:"
                " status: " << inRequest.status <<
                " "         << inRequest.statusMsg <<
                " "         << inRequest.Show() <<
            KFS_LOG_EOM;
            return false;
        }
        if (inRequest.blockStartSeq == inRequest.blockEndSeq) {
            if (! inRequest.blockLines.IsEmpty() ||
                    0 < inRequest.blockData.BytesConsumable()) {
                inRequest.status    = -EINVAL;
                inRequest.statusMsg = "invalid non empty block / heartbeat";
            } else if (inRequest.blockStartSeq != mLastLogSeq) {
                inRequest.status    = -EROFS;
                inRequest.statusMsg = mInvalidHeartbeatSequenceErrorMsg;
                mMetaVrSM.HandleLogBlockFailed(
                    inRequest.blockEndSeq,
                    inRequest.transmitterId
                );
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
                    return false;
                }
                mMetaVrSM.LogBlockWriteDone(
                    inRequest.blockStartSeq,
                    inRequest.blockEndSeq,
                    mInFlightCommitted.mSeq,
                    mLastViewEndSeq,
                    IsLogStreamGood()
                );
            }
            return false;
        }
        if (inRequest.blockData.BytesConsumable() <= 0) {
            panic("write block: invalid block: no data");
            inRequest.statusMsg = "invalid block: no data";
            inRequest.status    = -EFAULT;
            return false;
        }
        if (inRequest.blockLines.IsEmpty()) {
            panic("write block: invalid block: no log lines");
            inRequest.statusMsg = "invalid block: no log lines";
            inRequest.status    = -EFAULT;
            return false;
        }
        if (0 == mVrStatus) {
            inRequest.statusMsg = mPrimaryRejectedBlockWriteErrorMsg;
            inRequest.status    = -EROFS;
            return false;
        }
        const MetaVrLogSeq thePrevLastViewEndSeq = mLastViewEndSeq;
        if (inRequest.blockStartSeq != mLastLogSeq) {
            inRequest.status = -EROFS;
            // Check if this is valid vr log start view op.
            int theLnLen;
            if (inRequest.blockStartSeq < inRequest.blockEndSeq &&
                    mLastLogSeq < inRequest.blockStartSeq &&
                    2 == inRequest.blockLines.GetSize() &&
                    (int)mLogStartViewPrefixLen <
                        (theLnLen = inRequest.blockLines.Front()) &&
                    theLnLen < inRequest.blockData.BytesConsumable()) {
                MetaRequest*      theReqPtr = 0;
                IOBuffer::BufPos  theLen    = theLnLen;
                const char* const thePtr    =
                    inRequest.blockData.CopyOutOrGetBufPtr(
                        mTmpBuffer.Reserve(theLnLen), theLen);
                if (theLen != theLnLen) {
                    const char* const kMsgPtr =
                        "write block: invalid line length";
                    panic(kMsgPtr);
                    inRequest.statusMsg = kMsgPtr;
                    inRequest.status    = -EFAULT;
                } else if (0 != memcmp(mLogStartViewPrefixPtr, thePtr,
                            mLogStartViewPrefixLen)) {
                    inRequest.status = -EROFS;
                } else if ((theReqPtr = MetaRequest::ReadReplay(
                            thePtr + mLogAppendPrefixLen,
                            theLnLen - mLogAppendPrefixLen)) &&
                        META_VR_LOG_START_VIEW == theReqPtr->op) {
                    MetaVrLogStartView& theOp =
                        *static_cast<MetaVrLogStartView*>(theReqPtr);
                    if (! theOp.Validate() ||
                            theOp.mNewLogSeq != inRequest.blockEndSeq) {
                        inRequest.status = -EINVAL;
                    } else if (mLastLogSeq < theOp.mCommittedSeq ||
                                theOp.mNewLogSeq < mLastLogSeq ||
                            // Commit must be always in the last non empty view,
                            // or in the previous view if no ops in this view
                            // are committed.
                            ((! theOp.mCommittedSeq.IsSameView(
                                mLastNonEmptyViewEndSeq) ||
                            mLastNonEmptyViewEndSeq.mLogSeq <
                                theOp.mCommittedSeq.mLogSeq) &&
                            theOp.mCommittedSeq != mLastViewEndSeq
                            ) ||
                            (theOp.mCommittedSeq.IsSameView(mLastViewEndSeq) &&
                                theOp.mCommittedSeq.mLogSeq <
                                    mLastViewEndSeq.mLogSeq)) {
                        inRequest.status = -EROFS;
                    } else if (theOp.mCommittedSeq < mInFlightCommitted.mSeq ||
                            theOp.mCommittedSeq < mLastWriteCommitted.mSeq) {
                        inRequest.statusMsg =
                            "invalid start view end is less than committed";
                        inRequest.status    = -EINVAL;
                    } else {
                        inRequest.status = 0;
                        mLastViewEndSeq  = theOp.mCommittedSeq;
                    }
                } else {
                    inRequest.status = -EINVAL;
                }
                MetaRequest::Release(theReqPtr);
            }
            if (0 != inRequest.status) {
                if (inRequest.statusMsg.empty()) {
                    if (-EINVAL == inRequest.status) {
                        inRequest.statusMsg = "invalid log start view entry";
                    } else {
                        inRequest.statusMsg = mInvalidBlockStartSegmentErrorMsg;
                    }
                }
                return mMetaVrSM.HandleLogBlockFailed(
                    inRequest.blockEndSeq,
                    inRequest.transmitterId) && -EINVAL != inRequest.status;
            }
        }
        if (! IsLogStreamGood()) {
            inRequest.status    = -EIO;
            inRequest.statusMsg = "log write error";
            return false;
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
        BOOST_STATIC_ASSERT(
            2 // c/
            + 2 + 2 * sizeof(mInFlightCommitted.mSeq.mEpochSeq)
            + 2 + 2 * sizeof(mInFlightCommitted.mSeq.mViewSeq)
            + 2 + 2 * sizeof(mInFlightCommitted.mSeq.mLogSeq)
            + 2 + 2 * sizeof(mInFlightCommitted.mFidSeed)
            + 2 + 2 * sizeof(mInFlightCommitted.mErrChkSum)
            + 2 + 2 * sizeof(mInFlightCommitted.mStatus)
            + 2 + 2 * sizeof(inRequest.blockEndSeq.mEpochSeq)
            + 2 + 2 * sizeof(inRequest.blockEndSeq.mViewSeq)
            + 2 + 2 * sizeof(inRequest.blockEndSeq.mLogSeq)
            + 2 + 2 * sizeof(mNextBlockSeq)
            + 2 + 2 * sizeof(theBlockChecksum) <=
                (sizeof(inRequest.blockTrailer) < 0xFF ?
                    sizeof(inRequest.blockTrailer) : 0xFF)
        );
        if (sizeof(inRequest.blockTrailer) <= theTrailerLen) {
            const char* const theMsgPtr =
                "log writer: block trailer exceeds buffer space";
            panic(theMsgPtr);
            ClearBufferAndSetError(inRequest, -EFAULT, theMsgPtr);
            return false;
        }
        inRequest.blockTrailer[0] = (char)theTrailerLen;
        memcpy(inRequest.blockTrailer + 1, thePtr, theTrailerLen);
        const char* const theEndPtr             = thePtr;
        const char* const theCommitLineStartPtr =
            theEndPtr - inRequest.blockLines.Back();
        inRequest.blockCommitted = MetaVrLogSeq();
        MetaVrLogSeq theLogSeq;
        size_t       theTxLen      = theLen;
        Checksum     theTxChecksum = inRequest.blockChecksum;
        int          theBlockLen   = -1;
        Committed    theBlockCommitted;
        thePtr = theCommitLineStartPtr + 2;
        if (thePtr < theEndPtr &&
                (thePtr[-2] & 0xFF) == 'c' && (thePtr[-1] & 0xFF) == '/' &&
                ParseField(thePtr, theEndPtr, theBlockCommitted.mSeq) &&
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
                    inRequest.blockEndSeq.mLogSeq &&
                (mLastWriteCommitted.mSeq <= theBlockCommitted.mSeq ||
                    (! theLogSeq.IsSameView(theBlockCommitted.mSeq) &&
                    theBlockCommitted.mSeq.IsSameView(mLastViewEndSeq)))) {
            if (theBlockCommitted.mSeq < mLastWriteCommitted.mSeq) {
                // Replace block trailer, due to greater flight commit. Commit
                // might be greater in the case if primary has failed to start
                // view immediately after successfully writing start view
                // record, and then backup had written another start view record
                // with lesser commit sequence. The block trailer's commit
                // sequence isn't relevant in such case, as the actual commit /
                // view end is in start view record.
                const size_t theBodyLen =
                    theCommitLineStartPtr - mMdStream.GetBufferedStart();
                if (mMdStream.TruncateBuffer(theBodyLen) != theBodyLen) {
                    const char* const theMsgPtr =
                        "log write: block trailer rewrite internal error";
                    panic(theMsgPtr);
                    ClearBufferAndSetError(inRequest, -EFAULT, theMsgPtr);
                    return false;
                }
                theTxLen = WriteBlockTrailer(
                    theLogSeq, mLastWriteCommitted, theBlockLen, theTxChecksum);
                const size_t theCommitLineLen = mMdStream.GetBufferedEnd() -
                    mMdStream.GetBufferedStart() - theBodyLen;
                if (sizeof(inRequest.blockTrailer) < theCommitLineLen + 1 ||
                        0xFF < theCommitLineLen) {
                    const char* const theMsgPtr =
                        "log writer: replacement trailer invalid length";
                    panic(theMsgPtr);
                    ClearBufferAndSetError(inRequest, -EFAULT, theMsgPtr);
                    return false;
                }
                theBlockCommitted = mLastWriteCommitted;
                inRequest.blockLines.Back() = 0;
                inRequest.blockTrailer[0] = (char)theCommitLineLen;
                memcpy(inRequest.blockTrailer + 1,
                    mMdStream.GetBufferedEnd() - theCommitLineLen,
                    theCommitLineLen);
            }
            inRequest.blockCommitted = theBlockCommitted.mSeq;
        } else {
            ClearBufferAndSetError(inRequest,
                -EINVAL, "log write: invalid block format");
            return false;
        }
        const int theVrStatus = mMetaVrSM.HandleLogBlock(
            inRequest.blockStartSeq,
            inRequest.blockEndSeq,
            mInFlightCommitted.mSeq,
            inRequest.transmitterId
        );
        if (0 != theVrStatus &&
                ! IsMetaLogWriteOrVrError(theVrStatus)) {
            ClearBufferAndSetError(inRequest, theVrStatus, "VR error");
            return false;
        }
        if (0 == theVrStatus) {
            const int theStatus = mLogTransmitter.TransmitBlock(
                inRequest.blockEndSeq,
                (int)(inRequest.blockEndSeq.mLogSeq -
                    inRequest.blockStartSeq.mLogSeq),
                mMdStream.GetBufferedStart() + thePos,
                theTxLen,
                theTxChecksum,
                theTxLen
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
            mLastViewEndSeq,
            theStreamGoodFlag
        );
        if (theStreamGoodFlag) {
            inRequest.blockSeq  = mNextBlockSeq;
            mLastLogSeq         = inRequest.blockEndSeq;
            mNextLogSeq         = mLastLogSeq;
            inRequest.status    = 0;
            mLastWriteCommitted = theBlockCommitted;
            if (mLastLogSeq.IsPastViewStart()) {
                mLastNonEmptyViewEndSeq = mLastLogSeq;
            }
        } else {
            mLastViewEndSeq = thePrevLastViewEndSeq;
            inRequest.status    = -EIO;
            inRequest.statusMsg = "log write error";
        }
        return false;
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
        mNextBlockSeq    = 0;
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
        Checksum theTxChecksum = 0;
        WriteBlockTrailer(mLastLogSeq, mLastWriteCommitted, 0, theTxChecksum);
        LogStreamFlush();
        if (IsLogStreamGood()) {
            mNextLogSeq = mLastLogSeq;
            if (mMetaDataStorePtr) {
                mMetaDataStorePtr->RegisterLogSegment(
                    mLogName.c_str(), mCurLogStartSeq, mLogNum);
            }
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
        mMaxBlockSize = max(1, inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("maxBlockSize"),
            mMaxBlockSize));
        mMaxBlockBytes = max(4 << 10, inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("maxBlockBytes"),
            mMaxBlockBytes));
        mMaxReceiverRetryQueueSize = min(mMaxReceiverRetryQueueLimit,
            inParameters.getValue(
                theName.Truncate(thePrefixLen).Append(
                    "maxReceiverRetryQueueSize"), mMaxReceiverRetryQueueSize));
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
        mCpuAffinityIndex = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("cpuAffinityIndex"),
            mCpuAffinityIndex);
        mMaxClientOpsPendingCount = max(16, inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("maxClientOpsPendingCount"),
            mMaxClientOpsPendingCount));
        mMaxPendingAckByteCount = max(64 << 10, inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("maxPendingAckByteCount"),
            mMaxPendingAckByteCount));
        mFailureSimulationInterval = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("failureSimulationInterval"),
            mFailureSimulationInterval);
        mLastLogPath = mLogDir + "/" + MetaDataStore::GetLogSegmentLastFileNamePtr();
        mLogFileMaxSize = max(int64_t(64 << 10), inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("logFileMaxSize"),
            mLogFileMaxSize));
        const int theVrStatus = mMetaVrSM.SetParameters(
            kMetaVrParametersPrefixPtr,
            inParameters
        );
        const int theStatus = mLogTransmitter.SetParameters(
            theName.Truncate(thePrefixLen).Append("transmitter.").c_str(),
            inParameters
        );
        const string thePrevErrorSimulatorConfig = mErrorSimulatorConfig;
        mErrorSimulatorConfig = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append(
            "netErrorSimulator"), mErrorSimulatorConfig);
        if (thePrevErrorSimulatorConfig != mErrorSimulatorConfig &&
                NetErrorSimulatorConfigure(
                    mNetManager,
                    mErrorSimulatorConfig.c_str())) {
            KFS_LOG_STREAM_INFO <<
                "network error simulator configured: " <<
                mErrorSimulatorConfig <<
            KFS_LOG_EOM;
        }
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
            const int64_t     theStart  = microseconds();
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
            } else {
                mWorkerIoCounters.mDiskWriteTimeUsec +=
                    microseconds() - theStart;
                mWorkerIoCounters.mDiskWriteByteCount += inSize;
                mWorkerIoCounters.mDiskWriteCount++;
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
        const int64_t theStart  = microseconds();
        const bool    theOkFlag = close(mLogFd) == 0;
        if (theOkFlag) {
            mWorkerIoCounters.mDiskWriteTimeUsec += microseconds() - theStart;
        } else {
            IoError(errno);
        }
        mLogFd = -1;
        return theOkFlag;
    }
};

LogWriter::LogWriter()
    : mNextSeq(0),
      mVrStatus(0),
      mPrimaryLeaseEndTimeUsec(0),
      mImpl(*(new Impl(mVrStatus, mPrimaryLeaseEndTimeUsec)))
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
    const char*           inVrResetTypeStrPtr,
    Watchdog*             inWatchdogPtr,
    int                   inMaxReceiverRetryQueueLimit,
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
        inVrResetTypeStrPtr,
        inWatchdogPtr,
        inMaxReceiverRetryQueueLimit,
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
    const MetaVrLogSeq& inLastReplayLogSeq,
    const MetaVrLogSeq& inViewStartSeq)
{
    mImpl.SetCommitted(
        inLogSeq,
        inErrChecksum,
        inFidSeed,
        inStatus,
        inLastReplayLogSeq,
        inViewStartSeq
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

    MetaVrSM&
LogWriter::GetMetaVrSM()
{
    return mImpl.GetMetaVrSM();
}

    int
LogWriter::WriteNewLogSegment(
    const char*   inLogDirPtr,
    const Replay& inReplayer,
    string&       outLogSegmentFileName)
{
    return mImpl.WriteNewLogSegment(
        inLogDirPtr, inReplayer, outLogSegmentFileName);
}

    void
LogWriter::GetCounters(
    Counters& outCounters)
{
    mImpl.GetCounters(outCounters);
}

    int
LogWriter::GetPendingAckBytesOverage() const
{
    return mImpl.GetPendingAckBytesOverage();
}

}
 // namespace KFS
