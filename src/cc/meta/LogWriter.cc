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
          mLogFileStream(),
          mMdStream()
        {}
    ~Impl()
        { Impl::Shutdown(); }
    int Start(
        NetManager&       inNetManager,
        const Properties& inParameters)
    {
        if (mThread.IsStarted() || mNetManagerPtr) {
            return -EINVAL;
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
        if (mStopFlag) {
            
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
    void ScheduleFlush()
    {
        if (! mPendingQueueHeadPtr) {
            return;
        }
        mHasPendingFlag = true;
        QCStMutexLocker theLock(mMutex);
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
    NetManager*  mNetManagerPtr;
    bool         mHasPendingFlag;
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
    ofstream     mLogFileStream;
    MdStream     mMdStream;

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
        MetaRequest* thePtr    = &inHead;
        ostream&     theStream = mMdStream;
        while (thePtr) {
            if (((MetaRequest::kLogIfOk == thePtr->logAction &&
                        0 == thePtr->status) ||
                    MetaRequest::kLogAlways == thePtr->logAction)) {
                if (! thePtr->WriteLog(theStream, mOmitDefaultsFlag)) {
                    panic("log writer: invalid request ");
                }
                if (! theStream) {
                    thePtr->status    = -EIO;
                    thePtr->statusMsg = "transaction log write error";
                }
            }
            if (META_LOG_WRITER_CONTROL == thePtr->op) {
                Control(*static_cast<MetaLogWriterControl*>(thePtr));
            }
        }
    }
    void Control(
        MetaLogWriterControl& inRequest)
    {
    }
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
    const Properties& inParameters)
{
    return mImpl.Start(inNetManager, inParameters);
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
