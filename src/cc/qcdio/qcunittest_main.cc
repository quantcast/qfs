//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: $
//
// Created 2008/11/17
// Author: Mike Ovsiannikov
//
// Copyright 2008-2010 Quantcast Corp.
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
// Disk queue and io buffer pool unit test.
//
//----------------------------------------------------------------------------

#include "QCIoBufferPool.h"
#include "QCDiskQueue.h"
#include "qcstutils.h"
#include "QCUtils.h"
#include "qcdebug.h"

#include <string>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <fstream>

using namespace std;

class QCDiskQueueTest
{
public:
    class Iterator :
        public QCDiskQueue::OutputIterator,
        public QCDiskQueue::InputIterator
    {
    public:
        Iterator(
            size_t          inBufCount,
            QCIoBufferPool* inPoolPtr = 0)
         : mBufsPtr(new char* [inBufCount]),
           mCurPtr(mBufsPtr),
           mEndPtr(mBufsPtr + inBufCount),
           mPoolPtr(inPoolPtr)
        {
            while (mCurPtr < mEndPtr) {
                *mCurPtr++ = 0;
            }
            mCurPtr = mBufsPtr;
        }
        ~Iterator()
        {
            Iterator::Release();
            delete [] mBufsPtr;
        }
        Iterator& Release()
        {
            if (mPoolPtr) {
                mPoolPtr->Put(Reset(), MaxSize());
            }
            mCurPtr = mBufsPtr;
            while (mCurPtr < mEndPtr) {
                *mCurPtr++ = 0;
            }
            return Reset();
        }
        virtual void Put(
            char* inBufferPtr)
        {
            QCRTASSERT(mCurPtr < mEndPtr);
            *mCurPtr++ = inBufferPtr;
        }
        virtual char* Get()
            { return (mCurPtr < mEndPtr ? *mCurPtr++ : 0); }
        Iterator& Reset()
        {
            mCurPtr = mBufsPtr;
            return *this;
        }
        int MaxSize() const
            { return (int)(mEndPtr - mBufsPtr); }
    private:
        char** const    mBufsPtr;
        char**          mCurPtr;
        char** const    mEndPtr;
        QCIoBufferPool* mPoolPtr;

    private:
        Iterator(
            const Iterator& inItr);
        Iterator& operator=(
            const Iterator& inItr);
    };

    class RequestWaiter : public QCDiskQueue::IoCompletion
    {
    public:
        RequestWaiter()
            : mMutex(),
              mDoneCond(),
              mRequestCount(0)
            {}
        virtual ~RequestWaiter()
            { RequestWaiter::Wait(); }
        virtual bool Done(
            QCDiskQueue::RequestId      inRequestId,
            QCDiskQueue::FileIdx        inFileIdx,
            QCDiskQueue::BlockIdx       inStartBlockIdx,
            QCDiskQueue::InputIterator& inBufferItr,
            int                         inBufferCount,
            QCDiskQueue::Error          inCompletionCode,
            int                         inSysErrorCode,
            int64_t                     inIoByteCount)
        {
            QCStMutexLocker theLock(mMutex);
            if (--mRequestCount <= 0) {
                mDoneCond.Notify();
            }
            const int theRequestCount = mRequestCount;
            theLock.Unlock();
            ostringstream theStream;
            theStream <<
                "requests queued: " << theRequestCount <<
                " request id: "    << inRequestId << " done " <<
                " buffers: "       << inBufferCount <<
                " buffer[0] "      << (void*)inBufferItr.Get() <<
                " io bytes: "      << inIoByteCount <<
                " status: "        << ToString(
                    QCDiskQueue::CompletionStatus(
                        inCompletionCode, inSysErrorCode
                    )) <<
            endl;
            cout << theStream.str();
            return false; // Tell caller to free the buffers.
        }
        QCDiskQueue::EnqueueStatus Add(
            const QCDiskQueue::EnqueueStatus inStatus)
        {
            if (inStatus.IsGood()) {
                QCStMutexLocker theLock(mMutex);
                mRequestCount++;
            }
            return inStatus;
        }
        void Wait()
        {
            QCStMutexLocker theLock(mMutex);
            while (mRequestCount > 0) {
                mDoneCond.Wait(mMutex);
            }
        }
    private:
        QCMutex   mMutex;
        QCCondVar mDoneCond;
        int       mRequestCount;

    private:
        RequestWaiter(
            const RequestWaiter& inWaiter);
        RequestWaiter& operator=(
            const RequestWaiter& inWaiter);
    };

    class BPClient : public QCIoBufferPool::Client
    {
    public:
        BPClient(
            size_t inGetCount,
            int    inMaxReleaseCount)
            : mItr(inGetCount),
              mMaxReleaseCount(inMaxReleaseCount)
            {}
        virtual ~BPClient()
        {
            QCIoBufferPool* const thePoolPtr = GetPoolPtr();
            if (thePoolPtr) {
                thePoolPtr->Put(mItr, mItr.MaxSize());
                thePoolPtr->UnRegister(*this);
            }
        }
        bool Get(
            int inGetCount        = -1,
            int inMaxReleaseCount = -1)
        {
            if (inMaxReleaseCount >= 0) {
                mMaxReleaseCount = inMaxReleaseCount;
            }
            QCIoBufferPool* const thePoolPtr = GetPoolPtr();
            if (! thePoolPtr) {
                return false;
            }
            int theGetCount = mItr.MaxSize();
            if (inGetCount >= 0 && inGetCount < theGetCount) {
                theGetCount = inGetCount;
            }
            thePoolPtr->Put(mItr, mItr.MaxSize());
            const bool theRet = thePoolPtr->Get(mItr.Release(), theGetCount);
            cout << "client get: " << theGetCount << " " <<
                (theRet ? "OK" : "failed") << "\n";
            if (theRet) {
                mItr.Reset();
            }
            return theRet;
        }
        virtual void Release(
            QCIoBufferPool::RefillReqId inReqId,
            int                         inBufCount)
        {
            cout << "BPClient::Release: request id: " << inReqId <<
                " requested: " << inBufCount <<
                " max release: " << mMaxReleaseCount <<
            endl;
            QCASSERT(IsRegistered());
            GetPoolPtr()->Put(mItr, mMaxReleaseCount);
        }
    private:
        Iterator mItr;
        int      mMaxReleaseCount;
    private:
        BPClient(
            const BPClient& inClient);
        BPClient& operator=(
            const BPClient& inClient);
    };

    static string ToString(
        const QCDiskQueue::EnqueueStatus& inStatus)
    {
        ostringstream theStream;
        theStream <<
            "error: " <<
            QCDiskQueue::ToString(inStatus.GetError()) <<
            " request id: "  << inStatus.GetRequestId()
        ;
        return theStream.str();
    }

    static string ToString(
        const QCDiskQueue::CompletionStatus& inStatus)
    {
        return (
            string("error: ") +
            QCDiskQueue::ToString(inStatus.GetError()) + " / " +
            QCUtils::SysError(inStatus.GetSysError())
        );
    }

    int AllocFileSpace(
        int          inFileCount,
        const char** inFileNamesPtr,
        int64_t      inSize)
    {
        int theRet = 0;
        for (int i = 0; i < inFileCount; i++) {
            theRet = QCUtils::AllocateFileSpace(inFileNamesPtr[i], inSize);
            if (theRet) {
                cerr << inFileNamesPtr[i] << ": failed to allocate " <<
                    inSize << " bytes, " << QCUtils::SysError(theRet) <<
                endl;
                break;
            }
        }
        return theRet;
    }

    int DoTest(
        int          inFileCount,
        const char** inFileNamesPtr)
    {
        const int      thePartitionCount            = 2;
        const int      thePartitionBufferCount      = (1 << 10) - 2;
        const int      theBufferSize                = 4 << 10;
        const bool     theLockMemoryFlag            = false;
        const int      theThreadCount               = 2;
        const int      theMaxQueueDepth             = 16;
        const int      theMaxBuffersPerRequestCount = (1 << 9) - 3;
        const int      thePoolClientMaxReleaseCount = 1 << 10;
        const size_t   thePoolClientBufCount        =
            thePartitionBufferCount * thePartitionCount;
        const int      kRequestBufferCount          = 1 << 10;
        if (AllocFileSpace(inFileCount, inFileNamesPtr,
                kRequestBufferCount * theBufferSize)) {
            return 1;
        }
        QCIoBufferPool theBufPool;
        int theSysErr = theBufPool.Create(
                thePartitionCount,
                thePartitionBufferCount,
                theBufferSize,
                theLockMemoryFlag);
        if (theSysErr) {
            cerr << "failed to create buffer pool: " <<
                QCUtils::SysError(theSysErr) << endl;
            return 1;
        }
        QCDiskQueue theQueue;
        const int theErrCode = theQueue.Start(
            theThreadCount,
            theMaxQueueDepth,
            theMaxBuffersPerRequestCount,
            inFileCount,
            inFileNamesPtr,
            theBufPool);
        if (theErrCode != 0) {
            cerr << "failed to create disk queue: " <<
                QCUtils::SysError(theErrCode) << endl;
            return 1;
        }
        BPClient thePoolClient(
            thePoolClientBufCount, thePoolClientMaxReleaseCount);
        theBufPool.Register(thePoolClient);
        if (! thePoolClient.Get()) {
            cerr << "thePoolClient.Get() failed.\n";
        }
        Iterator theItr(kRequestBufferCount, &theBufPool);
        if (theBufPool.Get(theItr, theItr.MaxSize())) {
            cerr << "theBufPool.Get() returned true" << endl;
            return 1;
        } else {
            const QCIoBufferPool::RefillReqId kRefillId = 12345;
            cout << "out of io buffers, trying to refill id: " <<
                kRefillId << endl;
            if (! theBufPool.Get(theItr, theItr.MaxSize(), kRefillId)) {
                cout << "out of io buffers, refill failed" << endl;
                return 1;
            }
        }
        QCDiskQueue::FileIdx  theFileIdx  = 0;
        QCDiskQueue::BlockIdx theBlockIdx = 0;
        QCDiskQueue::CompletionStatus theStatus = theQueue.SyncWrite(
            theFileIdx,
            theBlockIdx,
            &theItr.Reset(),
            theItr.MaxSize()
        );
        cout << "SyncWrite: " << ToString(theStatus) << endl;
        if (theStatus.IsError()) {
            return 1;
        }
        theItr.Release();
        thePoolClient.Get();
        theStatus = theQueue.SyncRead(
            theFileIdx,
            theBlockIdx,
            0,
            theItr.MaxSize(),
            &theItr.Release()
        );
        cout << "SyncRead: " << ToString(theStatus) << endl;
        if (theStatus.IsError()) {
            return 1;
        }
        theItr.Release();
        thePoolClient.Get(-1, 10);
        theStatus = theQueue.SyncRead(
            theFileIdx,
            theBlockIdx,
            0,
            theItr.MaxSize(),
            &theItr.Release()
        );
        cout << "SyncRead: " << ToString(theStatus) << endl;
        if (theStatus.GetError() != QCDiskQueue::kErrorOutOfBuffers) {
            return 1;
        }
        theItr.Release();
        thePoolClient.Get(thePoolClientBufCount, thePoolClientBufCount);
        for (int i = 0; i < theMaxQueueDepth * 4; i++) {
            theStatus = theQueue.SyncRead(
                theFileIdx,
                theBlockIdx,
                0,
                theItr.MaxSize(),
                &theItr.Release()
            );
            cout << i << " SyncRead: " << ToString(theStatus) << endl;
            if (theStatus.IsError()) {
                return 1;
            }
        }
        theItr.Release();
        RequestWaiter theWaiter;
        const int theReqBlockCount =
            thePartitionBufferCount * thePartitionCount / theThreadCount;
        for (int i = 0; i < theMaxQueueDepth * 3 / 2; i++) {
            for (theFileIdx = 0; theFileIdx < inFileCount; theFileIdx++) {
                QCDiskQueue::EnqueueStatus const theStatus =
                    theWaiter.Add(theQueue.Read(
                        theFileIdx,
                        theBlockIdx,
                        0,
                        theReqBlockCount,
                        &theWaiter
                    ));
                cout << i << " " << theFileIdx << " Read: " <<
                    ToString(theStatus) << endl;
                if (theStatus.IsError()) {
                    return 1;
                }
            }
        }
        cout << "waiting for completion" << endl;
        theWaiter.Wait();
        cout << "all requests done" << endl;
        return 0;
    }

    QCDiskQueueTest()
        {}
    ~QCDiskQueueTest()
        {}

private:
    QCDiskQueueTest(
        const QCDiskQueueTest& inTest);
    QCDiskQueueTest& operator=(
        const QCDiskQueueTest& inTest);
};

int
main(int argc, char** argv)
{
    if (argc == 1 || (!strcmp(argv[1], "-h") || !strcmp(argv[1], "--help"))) {
        printf("Usage: %s [file1name] [file2name] ...\n", argv[0]);
        return 0;
    }

    QCDiskQueueTest theTest;
    return theTest.DoTest(argc - 1, (const char**)(argv + 1));
}
