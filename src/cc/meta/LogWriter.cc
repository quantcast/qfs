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

#include "common/MdStream.h"

#include "kfsio/NetManager.h"
#include "kfsio/ITimeout.h"
#include "kfsio/checksum.h"

#include "qcdio/QCThread.h"
#include "qcdio/QCMutex.h"
#include "qcdio/qcstutils.h"

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
          mCommitted(),
          mThread(),
          mMutex(),
          mCond(),
          mStopFlag(false),
          mOmitDefaultsFlag(true),
          mPendingQueueHeadPtr(0),
          mPendingQueueTailPtr(0),
          mInQueueHeadPtr(0),
          mInQueueTailPtr(0),
          mOutQueueHeadPtr(0),
          mOutQueueTailPtr(0),
          mPendingCommitted(),
          mInFlightCommitted(),
          mNextLogSeq(-1),
          mBlockChecksum(kKfsNullChecksum),
          mLogFileStream(),
          mMdStream(this),
          mReqOstream(mMdStream)
        {}
    ~Impl()
        { Impl::Shutdown(); }
    int Start(
        NetManager&       inNetManager,
        const char*       inParametersPrefixPtr,
        const Properties& inParameters)
    {
        if (mThread.IsStarted() || mNetManagerPtr) {
            return -EINVAL;
        }
        SetParameters(inParametersPrefixPtr, inParameters);
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
    Committed    mCommitted;
    QCThread     mThread;
    QCMutex      mMutex;
    QCCondVar    mCond;
    bool         mStopFlag;
    bool         mOmitDefaultsFlag;
    MetaRequest* mPendingQueueHeadPtr;
    MetaRequest* mPendingQueueTailPtr;
    MetaRequest* mInQueueHeadPtr;
    MetaRequest* mInQueueTailPtr;
    MetaRequest* mOutQueueHeadPtr;
    MetaRequest* mOutQueueTailPtr;
    Committed    mPendingCommitted;
    Committed    mInFlightCommitted;
    seq_t        mNextLogSeq;
    uint32_t     mBlockChecksum;
    ofstream     mLogFileStream;
    MdStream     mMdStream;
    ReqOstream   mReqOstream;
    WriteState   mWriteState;

    virtual void Timeout()
    {
        MetaRequest* theDoneHeadPtr = 0;
        if (mHasPendingFlag) {
            QCStMutexLocker theLock(mMutex);
            mHasPendingFlag = mInQueueHeadPtr != 0 || mPendingQueueHeadPtr != 0;
            theDoneHeadPtr = mOutQueueHeadPtr;
            mOutQueueHeadPtr = 0;
            mOutQueueTailPtr = 0;
        }
        while (theDoneHeadPtr) {
            MetaRequest& theReq = *theDoneHeadPtr;
            theDoneHeadPtr = theReq.next;
            theReq.next = 0;
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
            MetaRequest* thePtr    = theCurPtr;
            seq_t        theLogSeq = mNextLogSeq;
            while (thePtr) {
                if (META_LOG_WRITER_CONTROL == thePtr->op) {
                    if (Control(
                            *static_cast<MetaLogWriterControl*>(thePtr),
                            theLogSeq)) {
                        break;
                    }
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
                        thePtr->logseq            = ++theLogSeq;
                        thePtr->commitPendingFlag = true;
                    }
                }
            }
            theCurPtr = thePtr->next;
            thePtr->next = 0;
            if (theStream) {
                FlushBlock(theLogSeq);
                theStream.flush();
            }
            if (theStream) {
                mNextLogSeq = theLogSeq;
                return;
            }
            // Write failure.
            thePtr = &inHead;
            while (thePtr) {
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
                break;
            case MetaLogWriterControl::kNewLog:
                NewLog(inLogSeq);
                break;
            case MetaLogWriterControl::kSetParameters:
                SetParameters(
                    inRequest.paramsPrefix.c_str(),
                    inRequest.params);
                return false;
        }
        return IsLogStreamGood();
    }
    void NewLog(
        seq_t inLogSeq)
    {
    }
    void SetParameters(
        const char*       inParametersPrefixPtr,
        const Properties& inParams)
    {
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
    const char*       inParametersPrefixPtr,
    const Properties& inParameters)
{
    return mImpl.Start(inNetManager, inParametersPrefixPtr, inParameters);
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
