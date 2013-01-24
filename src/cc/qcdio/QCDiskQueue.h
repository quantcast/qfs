//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/11/11
// Author: Mike Ovsiannikov
//
// Copyright 2008-2012 Quantcast Corp.
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
// Disk io queue.
// Starts the specified number of io threads to make disk io requests non
// blocking "a la aio". Each read and write request has scatter / gather io
// buffer list. For read requests the io buffers can be allocated from io buffer
// pool immediately before the corresponding disk readv commences. Read and
// write requests are processed by all available io threads, and can be
// processed in any order, i.e. can be re-ordered. As long as write requests are
// non overlapping request order should not make a difference.
//
// All other types of requests -- meta requests (open, close, remove, etc) are
// processed in order they were enqueued in respect to other meta requests, and
// in respect to read and write requests for a particular file. In other words
// close that is queued after read request will be executed after the read
// request completes.
//
//----------------------------------------------------------------------------

#ifndef QCDISKQUEUE_H
#define QCDISKQUEUE_H

#include <stdint.h>
#include "QCIoBufferPool.h"
#include "QCMutex.h"
#include "QCThread.h"

class QCDiskQueue
{
public:
    enum ReqType
    {
        kReqTypeNone             = 0,
        kReqTypeRead             = 1,
        kReqTypeWrite            = 2,
        // The following are internal, except kReqTypeWriteSync.
        kReqTypeOpen             = 3,
        kReqTypeOpenRO           = 4,
        kReqTypeDelete           = 5,
        kReqTypeRename           = 6,
        kReqTypeCreate           = 7,
        kReqTypeCreateRO         = 8,
        kReqTypeGetFsAvailable   = 9,
        kReqTypeCheckDirReadable = 10,
        kReqTypeClose            = 11,
        kReqTypeWriteSync        = 12,
        kReqTypeMax
    };

    enum Error
    {
        kErrorNone                 = 0,
        kErrorRead                 = 1,
        kErrorWrite                = 2,
        kErrorCancel               = 3,
        kErrorSeek                 = 4,
        kErrorEnqueue              = 5,
        kErrorOutOfBuffers         = 6,
        kErrorParameter            = 7,
        kErrorQueueStopped         = 8,
        kErrorFileIdxOutOfRange    = 9,
        kErrorBlockIdxOutOfRange   = 10,
        kErrorBlockCountOutOfRange = 11,
        kErrorOutOfRequests        = 12,
        kErrorOpen                 = 13,
        kErrorClose                = 14,
        kErrorHasPendingRequests   = 15,
        kErrorSpaceAlloc           = 16,
        kErrorDelete               = 17,
        kErrorRename               = 18,
        kErrorGetFsAvailable       = 19,
        kErrorCheckDirReadable     = 20
    };

    enum { kRequestIdNone = -1 };

    typedef int      RequestId;
    typedef int      FileIdx;
    typedef int64_t  BlockIdx;
    typedef QCIoBufferPool::InputIterator  InputIterator;
    typedef QCIoBufferPool::OutputIterator OutputIterator;
    typedef QCMutex::Time                  Time;
    typedef QCThread::CpuAffinity          CpuAffinity;

    class Status
    {
    public:
        Status(
            Error inError    = kErrorNone,
            int   inSysError = 0)
            : mError(inError),
              mSysError(inSysError)
            {}
        bool IsError() const
            { return (mError != kErrorNone || mSysError != 0); }
        bool IsGood() const
            { return (! IsError()); }
        Error GetError() const
            { return mError; }
        int GetSysError() const
            { return mSysError; }
    private:
        Error mError;
        int   mSysError;
    };

    class CompletionStatus : public Status
    {
    public:
        CompletionStatus(
            Error   inError       = kErrorNone,
            int     inSysError    = 0,
            int64_t inIoByteCount = 0)
            : Status(inError, inSysError),
              mIoByteCount(inIoByteCount)
            {}
            int GetIoByteCount() const
                { return mIoByteCount; }
        private:
            int64_t mIoByteCount;
    };

    typedef Status CloseFileStatus;

    class EnqueueStatus
    {
    public:
        EnqueueStatus(
            RequestId inRequestId = kRequestIdNone,
            Error     inError     = kErrorNone,
            int       inSysError  = 0)
            : mRequestId(inRequestId),
              mError(inError),
              mSysError(0)
            {}
        bool IsError() const
            { return (mError != kErrorNone || mRequestId == kRequestIdNone); }
        bool IsGood() const
            { return (! IsError()); }
        Error GetError() const
            { return mError; }
        RequestId GetRequestId() const
            { return mRequestId; }
        int GetSysError() const
            { return mSysError; }
    private:
        RequestId mRequestId;
        Error     mError;
        int       mSysError;
    };

    class OpenFileStatus : public Status
    {
    public:
        OpenFileStatus(
            FileIdx inFileIdx  = -1,
            Error   inError    = kErrorParameter,
            int     inSysError = 0)
            : Status(inError, inSysError),
              mFileIdx(inFileIdx)
            {}
        FileIdx GetFileIdx() const
            { return mFileIdx; }
    private:
        FileIdx mFileIdx;
    };

    class IoCompletion
    {
    public:
        // If returns false then caller will free buffers.
        virtual bool Done(
            RequestId      inRequestId,
            FileIdx        inFileIdx,
            BlockIdx       inStartBlockIdx,
            InputIterator& inBufferItr,
            int            inBufferCount,
            Error          inCompletionCode,
            int            inSysErrorCode,
            int64_t        inIoBytes) = 0;
    protected:
        IoCompletion()
            {}
        virtual ~IoCompletion()
            {}
    };

    // The following is primarily to simulate disk stalls.
    // Notify() gets invoked from io thread before io starts.
    // This allows to get notifications for ios with no completion handler.
    class IoStartObserver
    {
    public:
        virtual void Notify(
            ReqType   inReqType,
            RequestId inRequestId,
            FileIdx   inFileIdx,
            BlockIdx  inStartBlockIdx,
            int       inBufferCount) = 0;
    protected:
        IoStartObserver()
            {}
        virtual ~IoStartObserver()
            {}
    };

    class DebugTracer
    {
    public:
        virtual void TraceMsg(
            const char* inMsgPtr,
            int         inMsgLen) = 0;
    protected:
        DebugTracer()
            {}
        virtual ~DebugTracer()
            {}
    };

    static bool IsValidRequestId(
        RequestId inReqId)
    { 
        return (inReqId != kRequestIdNone); 
    }

    static const char* ToString(
        Error inErrorCode);

    QCDiskQueue();
    ~QCDiskQueue();

    int Start(
        int              inThreadCount,
        int              inMaxQueueDepth,
        int              inMaxBuffersPerRequestCount,
        int              inFileCount,
        const char**     inFileNamesPtr,
        QCIoBufferPool&  inBufferPool,
        IoStartObserver* inIoStartObserverPtr = 0,
        CpuAffinity      inCpuAffinity        = CpuAffinity::None(),
        DebugTracer*     inDebugTracerPtr     = 0,
        bool             inBufferedIoFlag     = false);

    void Stop();

    EnqueueStatus Enqueue(
        ReqType        inReqType,
        FileIdx        inFileIdx,
        BlockIdx       inStartBlockIdx,
        InputIterator* inBufferIteratorPtr,
        int            inBufferCount,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec = -1);

    EnqueueStatus Read(
        FileIdx        inFileIdx,
        BlockIdx       inStartBlockIdx,
        InputIterator* inBufferIteratorPtr,
        int            inBufferCount,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec = -1)
    {
        return Enqueue(
            kReqTypeRead,
            inFileIdx,
            inStartBlockIdx,
            inBufferIteratorPtr,
            inBufferCount,
            inIoCompletionPtr,
            inTimeWaitNanoSec);
    }

    EnqueueStatus Write(
        FileIdx        inFileIdx,
        BlockIdx       inStartBlockIdx,
        InputIterator* inBufferIteratorPtr,
        int            inBufferCount,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec = -1,
        bool           inSyncFlag        = false)
    {
        return Enqueue(
            inSyncFlag ? kReqTypeWriteSync : kReqTypeWrite,
            inFileIdx,
            inStartBlockIdx,
            inBufferIteratorPtr,
            inBufferCount,
            inIoCompletionPtr,
            inTimeWaitNanoSec);
    }

    CompletionStatus SyncIo(
        ReqType         inReqType,
        FileIdx         inFileIdx,
        BlockIdx        inStartBlockIdx,
        InputIterator*  inBufferIteratorPtr,
        int             inBufferCount,
        OutputIterator* inOutBufferIteratroPtr);

    CompletionStatus SyncRead(
        FileIdx         inFileIdx,
        BlockIdx        inStartBlockIdx,
        InputIterator*  inBufferIteratorPtr,
        int             inBufferCount,
        OutputIterator* inOutBufferIteratroPtr = 0)
    {
        return SyncIo(
            kReqTypeRead,
            inFileIdx,
            inStartBlockIdx,
            inBufferIteratorPtr,
            inBufferCount,
            inOutBufferIteratroPtr);
    }

    CompletionStatus SyncWrite(
        FileIdx         inFileIdx,
        BlockIdx        inStartBlockIdx,
        InputIterator*  inBufferIteratorPtr,
        int             inBufferCount,
        OutputIterator* inOutBufferIteratroPtr = 0,
        bool            inSyncFlag             = false)
    {
        return SyncIo(
            inSyncFlag ? kReqTypeWriteSync : kReqTypeWrite,
            inFileIdx,
            inStartBlockIdx,
            inBufferIteratorPtr,
            inBufferCount,
            inOutBufferIteratroPtr);
    }

    EnqueueStatus Sync(
        FileIdx       inFileIdx,
        IoCompletion* inIoCompletionPtr,
        Time          inTimeWaitNanoSec = -1);

    bool Cancel(
        RequestId inRequestId);

    IoCompletion* CancelOrSetCompletionIfInFlight(
        RequestId     inRequestId,
        IoCompletion* inCompletionIfInFlightPtr);

    void GetPendingCount(
        int&     outFreeRequestCount,
        int&     outRequestCount,
        int64_t& outReadBlockCount,
        int64_t& outWriteBlockCount);

    OpenFileStatus OpenFile(
        const char* inFileNamePtr,
        int64_t     inMaxFileSize           = -1,
        bool        inReadOnlyFlag          = false,
        bool        inAllocateFileSpaceFlag = false,
        bool        inCreateFlag            = false,
        bool        inBufferedIoFlag        = false);

    CloseFileStatus CloseFile(
        FileIdx inFileIdx,
        int64_t inFileSize = -1);

    EnqueueStatus Rename(
        const char*    inSrcFileNamePtr,
        const char*    inDstFileNamePtr,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec = -1);

    EnqueueStatus Delete(
        const char*    inFileNamePtr,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec = -1);

    EnqueueStatus GetFsSpaceAvailable(
        const char*    inFileNamePtr,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec = -1);

    EnqueueStatus CheckDirReadable(
        const char*    inDirNamePtr,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec = -1);

    int GetBlockSize() const;

    Status AllocateFileSpace(
        FileIdx inFileIdx);

private:
    class Queue;
    class RequestWaiter;
    Queue* mQueuePtr;

    QCDiskQueue(
        QCDiskQueue& inQueue);
    QCDiskQueue& operator=(
        const QCDiskQueue& inQueue);
};

#endif /* QCDISKQUEUE_H */
