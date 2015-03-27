/*!
 * $Id$
 *
 * Copyright 2015 Quantcast Corp.
 *
 * This file is part of Kosmos File System (KFS).
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * \brief metadata transaction log writer.
 * \author Mike Ovsiannikov.
 */

#include "LogWriter.h"
#include "MetaRequest.h"
#include "util.h"

#include "common/MsgLogger.h"
#include "common/MdStream.h"
#include "common/time.h"
#include "common/kfserrno.h"

#include "kfsio/NetManager.h"
#include "kfsio/ITimeout.h"
#include "kfsio/checksum.h"

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
    private QCRunnable
{
public:
    Impl()
        : ITimeout(),
          QCRunnable(),
          mNetManagerPtr(0),
          mNextSeq(-1),
          mMaxDoneSeq(-1),
          mMaxDoneLogSeq(-1),
          mCommitted(),
          mThread(),
          mMutex(),
          mCond(),
          mStopFlag(false),
          mOmitDefaultsFlag(true),
          mMaxBlockSize(256),
          mLogDir("./kfslog"),
          mPendingQueueHeadPtr(0),
          mPendingQueueTailPtr(0),
          mInQueueHeadPtr(0),
          mInQueueTailPtr(0),
          mOutQueueHeadPtr(0),
          mOutQueueTailPtr(0),
          mPendingCommitted(),
          mInFlightCommitted(),
          mNextLogSeq(-1),
          mLastLogSeq(-1),
          mBlockChecksum(kKfsNullChecksum),
          mLogFd(-1),
          mError(0),
          mMdStream(this),
          mReqOstream(mMdStream),
          mCurLogStartTime(-1),
          mCurLogStartSeq(-1),
          mLogNum(0),
          mLogName(),
          mWriteState(kWriteStateNone),
          mLogRotateInterval(600 * 1000 * 1000),
          mPanicOnIoErrorFlag(false),
          mSyncFlag(false),
          mLastLogName("last"),
          mLastLogPath(mLogDir + "/" + mLastLogName),
          mLogFileNamePrefix("log")
        {}
    ~Impl()
        { Impl::Shutdown(); }
    int Start(
        NetManager&       inNetManager,
        seq_t             inLogNum,
        seq_t             inLogSeq,
        seq_t             inCommittedLogSeq,
        fid_t             inCommittedFidSeed,
        int64_t           inCommittedErrCheckSum,
        int               inCommittedStatus,
        const MdStateCtx* inLogAppendMdStatePtr,
        bool              inLogAppendHexFlag,
        const char*       inParametersPrefixPtr,
        const Properties& inParameters,
        string&           outCurLogFileName)
    {
        if (inLogNum < 0 || inLogSeq < 0 ||
                (mThread.IsStarted() || mNetManagerPtr)) {
            return -EINVAL;
        }
        mLogNum = inLogNum;
        SetParameters(inParametersPrefixPtr, inParameters);
        mMdStream.Reset(this);
        mCurLogStartTime = microseconds();
        if (inLogAppendMdStatePtr) {
            SetLogName(inLogSeq);
            mMdStream.SetMdState(*inLogAppendMdStatePtr);
            if (! mMdStream) {
                KFS_LOG_STREAM_ERROR <<
                    "log append:" <<
                    " failed to set md context" <<
                KFS_LOG_EOM;
                return -EIO;
            }
            if (0 <= mLogFd) {
                close(mLogFd);
            }
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
                " hex: "  << inLogAppendHexFlag <<
                " file: " << mLogName <<
                " size: " << theSize <<
            KFS_LOG_EOM;
            mMdStream.setf(inLogAppendHexFlag ? ostream::hex : ostream::dec);
        } else {
            NewLog(inLogSeq);
            if (! IsLogStreamGood()) {
                return mError;
            }
        }
        outCurLogFileName     = mLogName;
        mCommitted.mErrChkSum = inCommittedErrCheckSum;
        mCommitted.mSeq       = inCommittedLogSeq;
        mCommitted.mFidSeed   = inCommittedFidSeed;
        mCommitted.mStatus    = inCommittedStatus;
        mPendingCommitted = mCommitted;
        mStopFlag         = false;
        mNetManagerPtr    = &inNetManager;
        const int kStackSize = 64 << 10;
        mThread.Start(this, kStackSize, "LogWriter");
        mNetManagerPtr->RegisterTimeoutHandler(this);
        return 0;
    }
    void Enqueue(
        MetaRequest& inRequest)
    {
        inRequest.seqno = ++mNextSeq;
        if (mStopFlag) {
            inRequest.status    = -EIO;
            inRequest.statusMsg = "log writer stopped";
            submit_request(&inRequest);
            return;
        }
        inRequest.next = 0;
        if (mMaxDoneSeq + 1 == inRequest.seqno &&
                (MetaRequest::kLogNever == inRequest.logAction ||
                (MetaRequest::kLogIfOk == inRequest.logAction &&
                    0 != inRequest.status))) {
            mMaxDoneSeq = inRequest.seqno;
            submit_request(&inRequest);
            return;
        }
        if (mPendingQueueHeadPtr) {
            mPendingQueueTailPtr->next = &inRequest;
        } else {
            mPendingQueueHeadPtr = &inRequest;
            mPendingQueueTailPtr = &inRequest;
        }
        // FIXME:
        ScheduleFlush();
    }
    void RequestCommitted(
        MetaRequest& inRequest,
        fid_t        inFidSeed)
    {
        if (! inRequest.commitPendingFlag) {
            return;
        }
        inRequest.commitPendingFlag = false;
        if (inRequest.logseq < 0) {
            return;
        }
        const int theStatus = inRequest.status < 0 ?
            SysToKfsErrno(-inRequest.status) : 0;
        mCommitted.mErrChkSum += theStatus;
        mCommitted.mSeq       = inRequest.logseq;
        mCommitted.mFidSeed   = inFidSeed;
        mCommitted.mStatus    = theStatus;
    }
    seq_t GetCommittedLogSeq() const
        { return mCommitted.mSeq; }
    void GetCommitted(
        seq_t&   outLogSeq,
        int64_t& outErrChecksum,
        fid_t&   outFidSeed,
        int&     outStatus) const
    {
        outLogSeq      = mCommitted.mSeq;
        outErrChecksum = mCommitted.mErrChkSum;
        outFidSeed     = mCommitted.mFidSeed;
        outStatus      = mCommitted.mStatus;
    }
    void ScheduleFlush()
    {
        if (! mPendingQueueHeadPtr) {
            return;
        }
        QCStMutexLocker theLock(mMutex);
        mPendingCommitted = mCommitted;
        if (mInQueueTailPtr) {
            mInQueueTailPtr->next = mPendingQueueHeadPtr;
        } else {
            mInQueueHeadPtr = mPendingQueueHeadPtr;
        }
        mInQueueTailPtr = mPendingQueueTailPtr;
        mPendingQueueHeadPtr = 0;
        mPendingQueueTailPtr = 0;
        mCond.Notify();
    }
    void Shutdown()
    {
        if (! mThread.IsStarted() || mStopFlag) {
            return;
        }
        QCStMutexLocker theLock(mMutex);
        mStopFlag = true;
        mCond.Notify();
        theLock.Unlock();
        mThread.Join();
        if (mNetManagerPtr) {
            mNetManagerPtr->UnRegisterTimeoutHandler(this);
            mNetManagerPtr = 0;
        }
    }
private:
    class Committed
    {
    public:
        seq_t   mSeq;
        fid_t   mFidSeed;
        int64_t mErrChkSum;
        int     mStatus;
        Committed()
            : mSeq(-1),
              mFidSeed(-1),
              mErrChkSum(0),
              mStatus(0)
            {}
    };
    typedef MdStreamT<Impl> MdStream;
    enum WriteState
    {
        kWriteStateNone,
        kAppendBlockChecksum,
        kWriteBlockChecksum
    };

    NetManager*  mNetManagerPtr;
    seq_t        mNextSeq;
    seq_t        mMaxDoneSeq;
    seq_t        mMaxDoneLogSeq;
    Committed    mCommitted;
    QCThread     mThread;
    QCMutex      mMutex;
    QCCondVar    mCond;
    bool         mStopFlag;
    bool         mOmitDefaultsFlag;
    int          mMaxBlockSize;
    string       mLogDir;
    MetaRequest* mPendingQueueHeadPtr;
    MetaRequest* mPendingQueueTailPtr;
    MetaRequest* mInQueueHeadPtr;
    MetaRequest* mInQueueTailPtr;
    MetaRequest* mOutQueueHeadPtr;
    MetaRequest* mOutQueueTailPtr;
    Committed    mPendingCommitted;
    Committed    mInFlightCommitted;
    seq_t        mNextLogSeq;
    seq_t        mLastLogSeq;
    uint32_t     mBlockChecksum;
    int          mLogFd;
    int          mError;
    MdStream     mMdStream;
    ReqOstream   mReqOstream;
    int64_t      mCurLogStartTime;
    seq_t        mCurLogStartSeq;
    seq_t        mLogNum;
    string       mLogName;
    WriteState   mWriteState;
    int64_t      mLogRotateInterval;
    bool         mPanicOnIoErrorFlag;
    bool         mSyncFlag;
    string       mLastLogName;
    string       mLastLogPath;
    const string mLogFileNamePrefix;

    virtual void Timeout()
    {
        MetaRequest* theDoneHeadPtr = 0;
        if (mMaxDoneSeq < mNextSeq) {
            QCStMutexLocker theLock(mMutex);
            theDoneHeadPtr  = mOutQueueHeadPtr;
            mOutQueueHeadPtr = 0;
            mOutQueueTailPtr = 0;
        }
        while (theDoneHeadPtr) {
            MetaRequest& theReq = *theDoneHeadPtr;
            theDoneHeadPtr = theReq.next;
            theReq.next = 0;
            if (theReq.seqno <= mMaxDoneSeq) {
                panic("log writer: invalid sequence number");
            }
            if (0 <= theReq.logseq) {
                if (theReq.logseq <= mMaxDoneLogSeq) {
                    panic("log writer: invalid log sequence number");
                }
                mMaxDoneLogSeq = theReq.logseq;
            }
            mMaxDoneSeq = theReq.seqno;
            submit_request(&theReq);
        }
    }
    virtual void Run()
    {
        QCStMutexLocker theLock(mMutex);
        while (! mStopFlag) {
            while (! mStopFlag && ! mInQueueHeadPtr) {
                mCond.Wait(mMutex);
            }
            if (! mInQueueHeadPtr) {
                continue;
            }
            MetaRequest* const theHeadPtr = mInQueueHeadPtr;
            MetaRequest* const theTailPtr = mInQueueTailPtr;
            mInQueueHeadPtr = 0;
            mInQueueTailPtr = 0;
            mInFlightCommitted = mPendingCommitted;
            QCStMutexUnlocker theUnlocker(mMutex);
            Write(*theHeadPtr);
            theUnlocker.Lock();
            if (mOutQueueTailPtr) {
                mOutQueueTailPtr->next = theHeadPtr;
            } else {
                mOutQueueHeadPtr = theHeadPtr;
            } 
            mOutQueueTailPtr = theTailPtr;
            mNetManagerPtr->Wakeup();
        }
    }
    void Write(
        MetaRequest& inHead)
    {
        ostream&     theStream = mMdStream;
        MetaRequest* theCurPtr = &inHead;
        while (theCurPtr) {
            mLastLogSeq = mNextLogSeq;
            MetaRequest* thePtr         = theCurPtr;
            seq_t        theEndBlockSeq = mNextLogSeq + mMaxBlockSize;
            for ( ; thePtr; thePtr = thePtr->next) {
                if (META_LOG_WRITER_CONTROL == thePtr->op) {
                    if (Control(
                            *static_cast<MetaLogWriterControl*>(thePtr),
                            mLastLogSeq)) {
                        break;
                    }
                    theEndBlockSeq = mNextLogSeq + mMaxBlockSize;
                    continue;
                }
                if (! theStream) {
                    continue;
                }
                if (((MetaRequest::kLogIfOk == thePtr->logAction &&
                            0 == thePtr->status) ||
                        MetaRequest::kLogAlways == thePtr->logAction)) {
                    if (! thePtr->WriteLog(theStream, mOmitDefaultsFlag)) {
                        panic("log writer: invalid request ");
                    }
                    if (theStream) {
                        thePtr->logseq            = ++mLastLogSeq;
                        thePtr->commitPendingFlag = true;
                    }
                }
                if (theEndBlockSeq <= mLastLogSeq) {
                    break;
                }
            }
            MetaRequest* const theEndPtr = thePtr ? thePtr->next : thePtr;
            if (mNextLogSeq < mLastLogSeq && IsLogStreamGood()) {
                FlushBlock(mLastLogSeq);
            }
            if (IsLogStreamGood()) {
                mNextLogSeq = mLastLogSeq;
            } else {
                mLastLogSeq = mNextLogSeq;
                // Write failure.
                thePtr = &inHead;
                for (thePtr = theCurPtr;
                        theEndPtr != thePtr;
                        thePtr = thePtr->next) {
                    if (((MetaRequest::kLogIfOk == thePtr->logAction &&
                                0 == thePtr->status) ||
                            MetaRequest::kLogAlways == thePtr->logAction)) {
                        thePtr->logseq            = -1;
                        thePtr->commitPendingFlag = false;
                        thePtr->status            = -EIO;
                        thePtr->statusMsg         = "transaction log write error";
                    }
                }
            }
            theCurPtr = theEndPtr;
        }
        if (mCurLogStartSeq < mNextLogSeq && IsLogStreamGood() &&
                mCurLogStartTime + mLogRotateInterval <
                microseconds()) {
            CloseLog();
            NewLog(mNextLogSeq + 1);
        }
    }
    void FlushBlock(
        seq_t inLogSeq)
    {
        mReqOstream << "c"
            "/" << mInFlightCommitted.mSeq <<
            "/" << mInFlightCommitted.mFidSeed <<
            "/" << mInFlightCommitted.mErrChkSum <<
            "/" << mInFlightCommitted.mStatus <<
            "/" << inLogSeq
        ;
        mWriteState = kAppendBlockChecksum;
        mReqOstream.write("/", 1);
        if (kWriteStateNone != mWriteState) {
            mReqOstream.flush();
            if (kWriteStateNone != mWriteState) {
                panic("log writer flush block: invalid write state");
            }
        }
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
        MetaLogWriterControl& inRequest,
        seq_t                 inLogSeq)
    {
        inRequest.committed  = mInFlightCommitted.mSeq;
        inRequest.lastLogSeq = mLastLogSeq;
        switch (inRequest.type) {
            default:
            case MetaLogWriterControl::kNop:
                return false;
            case MetaLogWriterControl::kNewLog:
                CloseLog();
                NewLog(inLogSeq + 1);
                inRequest.logName = mLogName;
                inRequest.status  = mError;
                break;
            case MetaLogWriterControl::kSetParameters:
                return SetParameters(
                    inRequest.paramsPrefix.c_str(),
                    inRequest.params);
        }
        return IsLogStreamGood();
    }
    void CloseLog()
    {
        if (IsLogStreamGood()) {
            if (kWriteStateNone != mWriteState) {
                panic("log writer: close log invalid state");
            }
            if (mLastLogSeq != mNextLogSeq) {
                FlushBlock(mLastLogSeq);
                if (! IsLogStreamGood()) {
                    return;
                }
            }
            mMdStream << "time/" << DisplayIsoDateTime() << '\n';
            const string theChecksum = mMdStream.GetMd();
            mMdStream << "checksum/" << theChecksum << '\n';
            if (link_latest(mLogName, mLastLogPath)) {
                IoError(errno, "failed to link to: " + mLastLogPath);
            }
        }
        Sync();
        if (0 <= mLogFd && close(mLogFd)) {
            IoError(errno);
        }
        ++mLogNum;
    }
    void NewLog(
        seq_t inLogSeq)
    {
        if (0 <= mLogFd) {
            close(mLogFd);
        }
        mError      = 0;
        mWriteState = kWriteStateNone;
        SetLogName(inLogSeq);
        if ((mLogFd = open(
                mLogName.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0666)) < 0) {
            IoError(errno);
            return;
        }
        mMdStream.Reset(this);
        mMdStream.setf(ostream::dec);
        mMdStream <<
            "version/" << LogWriter::VERSION << "\n"
            "checksum/last-line\n"
            "setintbase/16\n"
            "time/" << DisplayIsoDateTime() << '\n'
        ;
        mMdStream.setf(ostream::hex);
        mMdStream.flush();
        Sync();
    }
    void SetLogName(
        seq_t inLogSeq)
    {
        mCurLogStartSeq = inLogSeq;
        mLogName        = makename(mLogDir, mLogFileNamePrefix, mLogNum);
    }
    bool SetParameters(
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
        mLastLogName = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("lastLogName"),
            mLastLogName);
        mLogRotateInterval = (int64_t)(inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("rotateIntervalSec"),
            mLogRotateInterval * 1e-6) * 1e6);
        mPanicOnIoErrorFlag = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("panicOnIoError"),
            mPanicOnIoErrorFlag ? 1 : 0) != 0;
        mSyncFlag = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("sync"),
            mSyncFlag ? 1 : 0) != 0;
        mLastLogPath = mLogDir + "/" + mLastLogName;
        return false;
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
        if (kWriteBlockChecksum == mWriteState) {
            mWriteState    = kWriteStateNone;
            mBlockChecksum = kKfsNullChecksum;
        } else {
            mBlockChecksum = ComputeBlockChecksum(
                mBlockChecksum, inBufPtr, inSize);
            if (mWriteState == kAppendBlockChecksum) {
                mWriteState = kWriteBlockChecksum;
                mReqOstream << mBlockChecksum;
                mReqOstream.write("\n", 1);
                if (kWriteBlockChecksum == mWriteState) {
                    mReqOstream.flush();
                }
                if (kWriteStateNone != mWriteState) {
                    panic("log writer: invalid write state");
                }
                return IsLogStreamGood();
            }
        }
        if (0 < inSize && IsLogStreamGood()) {
            const char*       thePtr    = inBufPtr;
            const char* const theEndPtr = thePtr + inSize;
            ssize_t           theNWr    = 0;
            while (thePtr < theEndPtr && 0 <= (theNWr = ::write(
                        mLogFd, inBufPtr, theEndPtr - thePtr))) {
                thePtr += theNWr;
            }
            if (theNWr < 0) {
                IoError(errno);
            }
        }
        return IsLogStreamGood();
    }
    bool flush()
        { return IsLogStreamGood(); }
};

LogWriter::LogWriter()
    : mImpl(*(new Impl()))
{}

LogWriter::~LogWriter()
{
    delete &mImpl;
}

    int
LogWriter::Start(
    NetManager&       inNetManager,
    seq_t             inLogNum,
    seq_t             inLogSeq,
    seq_t             inCommittedLogSeq,
    fid_t             inCommittedFidSeed,
    int64_t           inCommittedErrCheckSum,
    int               inCommittedStatus,
    const MdStateCtx* inLogAppendMdStatePtr,
    bool              inLogAppendHexFlag,
    const char*       inParametersPrefixPtr,
    const Properties& inParameters,
    string&           outCurLogFileName)
{
    return mImpl.Start(
        inNetManager,
        inLogNum,
        inLogSeq,
        inCommittedLogSeq,
        inCommittedFidSeed,
        inCommittedErrCheckSum,
        inCommittedStatus,
        inLogAppendMdStatePtr,
        inLogAppendHexFlag,
        inParametersPrefixPtr,
        inParameters,
        outCurLogFileName
    );
}

    void
LogWriter::Enqueue(
    MetaRequest& inRequest)
{
    mImpl.Enqueue(inRequest);
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
    seq_t&   outLogSeq,
    int64_t& outErrChecksum,
    fid_t&   outFidSeed,
    int&     outStatus) const
{
    mImpl.GetCommitted(
        outLogSeq,
        outErrChecksum,
        outFidSeed,
        outStatus
    );
}

    seq_t
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
LogWriter::Shutdown()
{
   mImpl.Shutdown();
}

} // namespace KFS
