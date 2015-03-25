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

#include "kfsio/NetManager.h"
#include "kfsio/ITimeout.h"
#include "kfsio/checksum.h"

#include "qcdio/QCThread.h"
#include "qcdio/QCMutex.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"

#include <errno.h>
#include <fstream>

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
          mHasPendingFlag(false),
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
          mLogFileStream(),
          mMdStream(this),
          mReqOstream(mMdStream),
          mCurLogStartTime(-1),
          mCurLogStartSeq(-1),
          mLogName(),
          mWriteState(kWriteStateNone),
          mLogRotateInterval(600 * 1000 * 1000),
          mLogFileNamePrefix("log")
        {}
    ~Impl()
        { Impl::Shutdown(); }
    int Start(
        NetManager&       inNetManager,
        seq_t             inLogSeq,
        const MdStateCtx* inLogAppendMdStatePtr,
        bool              inLogAppendHexFlag,
        const char*       inParametersPrefixPtr,
        const Properties& inParameters)
    {
        if (inLogSeq < 0 && (mThread.IsStarted() || mNetManagerPtr)) {
            return -EINVAL;
        }
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
            mLogFileStream.open(
                mLogName.c_str(), ofstream::app | ofstream::binary);
            if (! mLogFileStream) {
                const int theErr = errno;
                KFS_LOG_STREAM_ERROR <<
                    "log append:" <<
                    " failed to open log segment: " << mLogName <<
                    " for append" <<
                    " error: " << QCUtils::SysError(theErr) <<
                KFS_LOG_EOM;
                return -EIO;
            }
            KFS_LOG_STREAM_INFO <<
                "log append:" <<
                " hex: "  << inLogAppendHexFlag <<
                " file: " << mLogName <<
            KFS_LOG_EOM;
        } else {
            NewLog(inLogSeq);
            if (! IsLogStreamGood()) {
                return -EIO;
            }
        }
        mStopFlag      = false;
        mNetManagerPtr = &inNetManager;
        const int kStackSize = 64 << 10;
        mThread.Start(this, kStackSize, "LogWriter");
        mNetManagerPtr->RegisterTimeoutHandler(this);
        return 0;
    }
    void Enqueue(
        MetaRequest& inRequest)
    {
        inRequest.logseq = ++mNextSeq;
        if (mStopFlag) {
            inRequest.status    = -EIO;
            inRequest.statusMsg = "log writer stopped";
            submit_request(&inRequest);
            return;
        }
        inRequest.next = 0;
        if (! mHasPendingFlag &&
                (MetaRequest::kLogNever == inRequest.logAction ||
                (MetaRequest::kLogIfOk == inRequest.logAction &&
                    0 != inRequest.status))) {
            submit_request(&inRequest);
            return;
        }
        mHasPendingFlag = true;
        if (mPendingQueueHeadPtr) {
            mPendingQueueTailPtr->next = &inRequest;
        } else {
            mPendingQueueHeadPtr = &inRequest;
            mPendingQueueTailPtr = &inRequest;
        }
    }
    void SetCommitted(
        seq_t    inLogSeq,
        seq_t    inFidSeed,
        uint64_t inErrChksum,
        int      inStatus)
    {
        mCommitted.mSeq       = inLogSeq;
        mCommitted.mFidSeed   = inFidSeed;
        mCommitted.mErrChkSum = inErrChksum;
        mCommitted.mStatus    = inStatus;
    }
    void ScheduleFlush()
    {
        if (! mPendingQueueHeadPtr) {
            return;
        }
        mHasPendingFlag = true;
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
        mStopFlag = false;
        mCond.Notify();
        theLock.Unlock();
        mThread.Join();
        if (mNetManagerPtr) {
            mNetManagerPtr->UnRegisterTimeoutHandler(this);
            mNetManagerPtr = 0;
        }
    }
private:
    struct Committed
    {
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
    bool         mHasPendingFlag;
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
    ofstream     mLogFileStream;
    MdStream     mMdStream;
    ReqOstream   mReqOstream;
    int64_t      mCurLogStartTime;
    seq_t        mCurLogStartSeq;
    string       mLogName;
    WriteState   mWriteState;
    int64_t      mLogRotateInterval;
    const string mLogFileNamePrefix;

    virtual void Timeout()
    {
        MetaRequest* theDoneHeadPtr = 0;
        if (mHasPendingFlag) {
            QCStMutexLocker theLock(mMutex);
            mHasPendingFlag = mInQueueHeadPtr != 0 || mPendingQueueHeadPtr != 0;
            theDoneHeadPtr  = mOutQueueHeadPtr;
            mOutQueueHeadPtr = 0;
            mOutQueueTailPtr = 0;
        }
        while (theDoneHeadPtr) {
            MetaRequest& theReq = *theDoneHeadPtr;
            theDoneHeadPtr = theReq.next;
            theReq.next = 0;
            if (theReq.seqno <= mMaxDoneLogSeq) {
                panic("log writer: invalid sequence number");
            }
            if (0 <= theReq.logseq) {
                if (theReq.logseq <= mMaxDoneLogSeq) {
                    panic("log writer: invalid log sequence number");
                }
                mMaxDoneLogSeq = theReq.logseq;
            }
            mMaxDoneLogSeq = theReq.seqno;
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
            if (IsLogStreamGood()) {
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
        mReqOstream << "c/"
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
        mLogFileStream.flush();
    }
    bool Control(
        MetaLogWriterControl& inRequest,
        seq_t                 inLogSeq)
    {
        inRequest.committed = inLogSeq;
        switch (inRequest.type) {
            default:
            case MetaLogWriterControl::kNop:
                return false;
            case MetaLogWriterControl::kNewLog:
                CloseLog();
                NewLog(inLogSeq + 1);
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
        }
        mLogFileStream.flush();
        mLogFileStream.close();
        if (! mLogFileStream) {
            const int theErr = errno;
            KFS_LOG_STREAM_ERROR <<
                "failed to close transaction log segment: " << mLogName <<
                " error: " << QCUtils::SysError(theErr) <<
            KFS_LOG_EOM;
        }
    }
    void NewLog(
        seq_t inLogSeq)
    {
        mLogFileStream.close();
        mLogFileStream.clear();
        mWriteState = kWriteStateNone;
        SetLogName(inLogSeq);
        mLogFileStream.open(
            mLogName.c_str(), ofstream::binary);
        if (! mLogFileStream) {
            const int theErr = errno;
            KFS_LOG_STREAM_ERROR <<
                "failed to open transaction log segment: " << mLogName <<
                " error: " << QCUtils::SysError(theErr) <<
            KFS_LOG_EOM;
            return;
        }
        mMdStream.Reset(this);
        mMdStream <<
            "version/" << LogWriter::VERSION << "\n"
            "checksum/last-line\n"
            "setintbase/16\n"
            "time/" << DisplayIsoDateTime() << '\n'
        ;
        mMdStream.setf(ostream::hex);
    }
    void SetLogName(
        seq_t inLogSeq)
    {
        mCurLogStartSeq = inLogSeq;
        mLogName        = makename(mLogDir, mLogFileNamePrefix, inLogSeq);
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
        mLogRotateInterval = (int64_t)(inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("rotateIntervalSec"),
            mLogRotateInterval * 1e-6) * 1e6);
        return false;
    }
    bool IsLogStreamGood() const
        { return (mMdStream && mLogFileStream); }
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
            mBlockChecksum = ComputeBlockChecksum(mBlockChecksum, inBufPtr, inSize);
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
        mLogFileStream.write(inBufPtr, inSize);
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
    seq_t             inLogSeq,
    const MdStateCtx* inLogAppendMdStatePtr,
    bool              inLogAppendHexFlag,
    const char*       inParametersPrefixPtr,
    const Properties& inParameters)
{
    return mImpl.Start(
        inNetManager,
        inLogSeq,
        inLogAppendMdStatePtr,
        inLogAppendHexFlag,
        inParametersPrefixPtr,
        inParameters
    );
}

    void
LogWriter::Enqueue(
    MetaRequest& inRequest)
{
    mImpl.Enqueue(inRequest);
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
