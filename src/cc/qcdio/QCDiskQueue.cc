//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/11/11
// Author: Mike Ovsiannikov
//
// Copyright 2008-2011,2016 Quantcast Corporation. All rights reserved.
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
// Disk io queue implementation.
//
//----------------------------------------------------------------------------

#include "QCDiskQueue.h"
#include "QCThread.h"
#include "QCMutex.h"
#include "QCUtils.h"
#include "qcstutils.h"
#include "qcdebug.h"

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/statvfs.h>
#include <limits.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <dirent.h>

#ifdef QC_OS_NAME_DARWIN
#include <sys/param.h>
#include <sys/mount.h>
#endif

// Ensure that all valid entries are positive, to make equal to 0 condition work
// for 0 slot.
static const unsigned int kPendingCloseListIdxOff = 1;
static const unsigned int kEndOfPendingCloseList  = ~((unsigned int)0);

class QCDiskQueue::Request
{};

class QCDiskQueue::Queue
{
public:
    Queue()
        : mMutex(),
          mFreeReqCond(),
          mWorkCondPtr(0),
          mBufferPoolPtr(0),
          mThreadsPtr(0),
          mBuffersPtr(0),
          mRequestsPtr(0),
          mFdPtr(0),
          mFilePendingReqCountPtr(0),
          mIoVecPtr(0),
          mFileInfoPtr(0),
          mPendingReadBlockCount(0),
          mPendingWriteBlockCount(0),
          mPendingCloseHeadPtr(0),
          mPendingCloseTailPtr(0),
          mPendingCount(0),
          mFreeCount(0),
          mTotalCount(0),
          mThreadCount(0),
          mRequestQueueCount(0),
          mRequestBufferCount(0),
          mCompletionRunningCount(0),
          mFileCount(0),
          mFdCount(0),
          mBlockSize(0),
          mIoVecPerThreadCount(0),
          mFreeFdHead(kFreeFdEnd),
          mReqWaitersCount(0),
          mDebugTracerPtr(0),
          mIoStartObserverPtr(0),
          mRequestProcessorsPtr(0),
          mNextThreadIdx(0),
          mCreateExclusiveFlag(true),
          mRunFlag(false),
          mRequestAffinityFlag(false),
          mSerializeMetaRequestsFlag(true),
          mBarrierFlag(false)
        {}
    virtual ~Queue()
        { Queue::Stop(); }
    inline void Done(
        QCDiskQueue::RequestProcessor& inProcessor,
        QCDiskQueue::Request&          inReq,
        Error                          inError,
        int                            inSysError,
        int64_t                        inIoByteCount,
        BlockIdx                       inBlockIdx,
        QCDiskQueue::InputIterator*    inInputIteratorPtr);
    int Start(
        int                      inThreadCount,
        int                      inMaxQueueDepth,
        int                      inMaxBuffersPerRequestCount,
        int                      inFileCount,
        const char**             inFileNamesPtr,
        QCIoBufferPool&          inBufferPool,
        IoStartObserver*         inIoStartObserverPtr,
        QCDiskQueue::CpuAffinity inCpuAffinity,
        DebugTracer*             inDebugTracerPtr,
        bool                     inBufferedIoFlag,
        bool                     inCreateExclusiveFlag,
        bool                     inRequestAffinityFlag,
        bool                     inSerializeMetaRequestsFlag,
        RequestProcessor**       inRequestProcessorsPtr);
    void Stop()
    {
        QCStMutexLocker theLocker(mMutex);
        StopSelf();
    }
    void Run(
        int inThreadIndex);
    EnqueueStatus Enqueue(
        ReqType        inReqType,
        FileIdx        inFileIdx,
        BlockIdx       inStartBlockIdx,
        InputIterator* inBufferIteratorPtr,
        int            inBufferCount,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec,
        int64_t        inEofHint);
    bool Cancel(
        RequestId inRequestId);
    IoCompletion* CancelOrSetCompletionIfInFlight(
        RequestId     inRequestId,
        IoCompletion* inCompletionIfInFlightPtr);
    void GetPendingCount(
        int&     outFreeRequestCount,
        int&     outRequestCount,
        int64_t& outReadBlockCount,
        int64_t& outWriteBlockCount)
    {
        QCStMutexLocker theLocker(mMutex);
        outFreeRequestCount = mFreeCount;
        outRequestCount     = mPendingCount;
        outReadBlockCount   = mPendingReadBlockCount;
        outWriteBlockCount  = mPendingWriteBlockCount;
    }
    OpenFileStatus OpenFile(
        const char* inFileNamePtr,
        int64_t     inMaxFileSize,
        bool        inReadOnlyFlag,
        bool        inAllocateFileSpaceFlag,
        bool        inCreateFlag,
        bool        inBufferedIoFlag);
    CloseFileStatus CloseFile(
        FileIdx inFileIdx,
        int64_t inFileSize);
    void CloseAllFiles();
    int GetBlockSize() const
        { return mBlockSize; }
    EnqueueStatus CheckOpenStatus(
        FileIdx       inFileIdx,
        IoCompletion* inIoCompletionPtr,
        Time          inTimeWaitNanoSec);
    Status AllocateFileSpace(
        FileIdx inFileIdx);
    EnqueueStatus Rename(
        const char*    inSrcFileNamePtr,
        const char*    inDstFileNamePtr,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec);
    EnqueueStatus Delete(
        const char*    inFileNamePtr,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec);
    EnqueueStatus GetFsSpaceAvailable(
        const char*    inPathNamePtr,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec);
    EnqueueStatus CheckDirReadable(
        const char*    inDirNamePtr,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec);
    EnqueueStatus CheckDirWritable(
        const char*    inTestFileNamePtr,
        bool           inBufferedIoFlag,
        bool           inAllocSpaceFlag,
        int64_t        inWriteSize,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec);
    EnqueueStatus EnqueueMeta(
        ReqType        inReqType,
        const char*    inFileName1Ptr,
        const char*    inFileName2Ptr,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec);

    static bool IsBarrierReqType(
        ReqType inReqType)
    {
        return (
            inReqType == kReqTypeCreate   ||
            inReqType == kReqTypeCreateRO ||
            inReqType == kReqTypeOpen     ||
            inReqType == kReqTypeOpenRO   ||
            inReqType == kReqTypeDelete   ||
            inReqType == kReqTypeRename
        );
    }
    static bool IsMetaReqType(
        ReqType inReqType)
    {
        // For now dir check is not barrier, because directory delete is not
        // supported.
        return (
            IsBarrierReqType(inReqType) ||
            inReqType == kReqTypeGetFsAvailable ||
            inReqType == kReqTypeCheckDirReadable ||
            inReqType == kReqTypeCheckDirWritable
        );
    }
    static bool IsWriteReqType(
        ReqType inReqType)
    {
        return (inReqType == kReqTypeWrite || inReqType == kReqTypeWriteSync);
    }
private:
    typedef unsigned int RequestIdx;
    enum
    {
           kBlockBitCount       = 48,
           kBlockOffsetBitCount = 64 - kBlockBitCount,
           kFileIndexBitCount   = 16
    };

    class Request : public QCDiskQueue::Request
    {
    public:
        Request()
            : QCDiskQueue::Request(),
              mPrevIdx(0),
              mNextIdx(0),
              mReqType(kReqTypeNone),
              mInFlightFlag(false),
              mFreeBuffersIfNoIoCompletionFlag(false),
              mBufferCount(0),
              mFileIdx(0),
              mBlockIdx(0),
              mIoCompletionPtr(0)
            {}
        ~Request()
            {}
        bool IsBarrier() const
            { return IsBarrierReqType(mReqType); }
        bool IsMeta() const
            { return (IsMetaReqType(mReqType)); }
        RequestIdx    mPrevIdx;
        RequestIdx    mNextIdx;
        ReqType       mReqType:8;
        bool          mInFlightFlag:1;
        bool          mFreeBuffersIfNoIoCompletionFlag:1;
        int           mBufferCount;
        uint64_t      mFileIdx:16;
        uint64_t      mBlockIdx:48;
        IoCompletion* mIoCompletionPtr;
    };

    template <typename T> T static Min(
        T inA,
        T inB)
        { return (inA < inB ? inA : inB); }
    template <typename T> T static Max(
        T inA,
        T inB)
        { return (inA > inB ? inA : inB); }

    class BuffersIterator :
        public OutputIterator,
        public InputIterator
    {
    public:
        BuffersIterator(
            Queue&   inQueue,
            Request& inReq,
            int      inBufferCount)
         : OutputIterator(),
           InputIterator(),
           mQueue(inQueue),
           mCurPtr(inQueue.GetBuffersPtr(inReq)),
           mCurCount(Min(inBufferCount, inQueue.mRequestBufferCount)),
           mBufferCount(inBufferCount - mCurCount),
           mReqHeadIdx(inQueue.mRequestsPtr[inReq.mPrevIdx].mNextIdx),
           mReqIdx(mReqHeadIdx)
        {}
        virtual void Put(
            char* inBufferPtr)
        {
            char** const thePtr = Next();
            QCRTASSERT(thePtr);
            *thePtr = inBufferPtr;
        }
        virtual char* Get()
        {
            char** const thePtr = Next();
            return (thePtr ? *thePtr : 0);
        }
    private:
        Queue&           mQueue;
        char**           mCurPtr;
        int              mCurCount;
        int              mBufferCount;
        const RequestIdx mReqHeadIdx;
        RequestIdx       mReqIdx;

        char** Next()
        {
            for (; ;) {
                if (mCurCount > 0) {
                    mCurCount--;
                    return mCurPtr++;
                }
                mReqIdx = mQueue.mRequestsPtr[mReqIdx].mNextIdx;
                if (mReqIdx == mReqHeadIdx) {
                    return 0;
                }
                Request& theReq = mQueue.mRequestsPtr[mReqIdx];
                mCurPtr = mQueue.GetBuffersPtr(theReq);
                mCurCount = Min(mBufferCount, mQueue.mRequestBufferCount);
                mBufferCount -= mCurCount;
            }
        }

    private:
        BuffersIterator(
            const BuffersIterator& inItr);
        BuffersIterator& operator=(
            const BuffersIterator& inItr);
    };
    friend class BuffersIterator;

    class IoThread : public QCThread
    {
    public:
        IoThread()
            : mThreadIndex(0),
              mQueuePtr(0)
            {}
        virtual ~IoThread()
            {}
        virtual void Run()
        {
            QCASSERT(mQueuePtr);
            mQueuePtr->Run(mThreadIndex);
        }
        int Start(
            Queue&                   inQueue,
            int                      inThreadIndex,
            int                      inStackSize,
            const char*              inNamePtr,
            QCDiskQueue::CpuAffinity inCpuAffinity)
        {
            mThreadIndex = inThreadIndex;
            mQueuePtr    = &inQueue;
            return TryToStart(this, inStackSize, inNamePtr, inCpuAffinity);
        }
    private:
        int    mThreadIndex;
        Queue* mQueuePtr;
    };

    enum OpenError
    {
        kOpenErrorNone   = 0,
        kOpenErrorNFile  = 1,
        kOpenErrorAccess = 2,
        kOpenErrorIo     = 3
    };
    static int Open2SysError(
        OpenError inError)
    {
        switch (inError) {
            case kOpenErrorNone:
                return 0;
            case kOpenErrorNFile:
                return ENFILE;
            case kOpenErrorAccess:
                return EACCES;
            case kOpenErrorIo:
            default:
                break;
        }
        return EIO;
    }
    static OpenError Sys2OpenError(
        int inError)
    {
        switch (inError) {
            case 0:
                return kOpenErrorNone;
            case ENFILE:
                return kOpenErrorNFile;
            case EACCES:
                return kOpenErrorAccess;
            default:
                break;
        }
        return kOpenErrorIo;
    }

    struct FileInfo
    {
        FileInfo()
            : mLastBlockIdx(0),
              mSpaceAllocPendingFlag(false),
              mOpenPendingFlag(false),
              mOpenError(kOpenErrorNone),
              mClosedFlag(false),
              mCloseFileSize(-1),
              mThreadIdx(0)
            {}
        uint64_t  mLastBlockIdx:48;
        bool      mSpaceAllocPendingFlag:1;
        bool      mOpenPendingFlag:1;
        OpenError mOpenError:2;
        bool      mClosedFlag:1;
        bool      mBufferedIoFlag:1;
        int64_t   mCloseFileSize;
        int       mThreadIdx;
    };

    QCMutex            mMutex;
    QCCondVar          mFreeReqCond;
    QCCondVar*         mWorkCondPtr;
    QCIoBufferPool*    mBufferPoolPtr;
    IoThread*          mThreadsPtr;
    char**             mBuffersPtr;
    Request*           mRequestsPtr;
    int*               mFdPtr;
    unsigned int*      mFilePendingReqCountPtr;
    struct iovec*      mIoVecPtr;
    FileInfo*          mFileInfoPtr;
    int64_t            mPendingReadBlockCount;
    int64_t            mPendingWriteBlockCount;
    unsigned int*      mPendingCloseHeadPtr;
    unsigned int*      mPendingCloseTailPtr;
    int                mPendingCount;
    int                mFreeCount;
    int                mTotalCount;
    int                mThreadCount;
    int                mRequestQueueCount;
    int                mRequestBufferCount;
    int                mCompletionRunningCount;
    int                mFileCount;
    int                mFdCount;
    int                mBlockSize;
    int                mIoVecPerThreadCount;
    int                mFreeFdHead;
    int                mReqWaitersCount;
    DebugTracer*       mDebugTracerPtr;
    IoStartObserver*   mIoStartObserverPtr;
    RequestProcessor** mRequestProcessorsPtr;
    int                mNextThreadIdx;
    bool               mCreateExclusiveFlag;
    bool               mRunFlag;
    bool               mRequestAffinityFlag;
    bool               mSerializeMetaRequestsFlag;
    bool               mBarrierFlag; // New req. can not be processed
                                   // until in flight req. done.

    enum
    {
        kFreeQueueIdx = 0,
        kIoQueueIdx   = 1,
        kRequestQueueCount
    };
    enum
    {
        kFreeFdOffset  = 2,
        kFreeFdEnd     = -1,
        kOpenPendingFd = 0x7FFFFFFF
    };

    static int GetOpenCommonFlags(
        bool inBufferedIoFlag)
    {
#ifndef O_DIRECT
        (void)inBufferedIoFlag;
#endif
        return (0
#ifdef O_DIRECT
        | (inBufferedIoFlag ? 0 : O_DIRECT)
#endif
#ifdef O_NOATIME
        | O_NOATIME
#endif
        );
    }
    char** GetBuffersPtr(
        Request& inReq)
    {
        return (mBuffersPtr +
            ((&inReq - mRequestsPtr) - mRequestQueueCount) * mRequestBufferCount
        );
    }
    void Init(
        Request& inReq)
    {
        const RequestIdx theIdx(&inReq - mRequestsPtr);
        inReq.mPrevIdx = theIdx;
        inReq.mNextIdx = theIdx;
    }
    bool IsInList(
        Request& inReq)
    {
        const RequestIdx theIdx(&inReq - mRequestsPtr);
        return (inReq.mPrevIdx != theIdx || inReq.mNextIdx != theIdx);
    }
    void Insert(
        Request& inBefore,
        Request& inReq)
    {
        mRequestsPtr[inBefore.mPrevIdx].mNextIdx =
            RequestIdx(&inReq - mRequestsPtr);
        mRequestsPtr[inReq.mPrevIdx].mNextIdx    =
            RequestIdx(&inBefore - mRequestsPtr);
        const RequestIdx theTmp = inReq.mPrevIdx;
        inReq.mPrevIdx    = inBefore.mPrevIdx;
        inBefore.mPrevIdx = theTmp;
    }
    void Remove(
        Request& inReq)
    {
        mRequestsPtr[inReq.mPrevIdx].mNextIdx = inReq.mNextIdx;
        mRequestsPtr[inReq.mNextIdx].mPrevIdx = inReq.mPrevIdx;
        Init(inReq);
    }
    Request* PopFront(
        RequestIdx inIdx)
    {
        const RequestIdx theIdx = mRequestsPtr[inIdx].mNextIdx;
        if (theIdx == inIdx) {
            return 0;
        }
        Request& theReq = mRequestsPtr[theIdx];
        Remove(theReq);
        return &theReq;
    }
    Request* Front(
        RequestIdx inIdx)
    {
        const RequestIdx theIdx = mRequestsPtr[inIdx].mNextIdx;
        return (theIdx == inIdx ? 0 : mRequestsPtr + theIdx);
    }
    const Request* Front(
        RequestIdx inIdx) const
    {
        const RequestIdx theIdx = mRequestsPtr[inIdx].mNextIdx;
        return (theIdx == inIdx ? 0 : mRequestsPtr + theIdx);
    }
    bool Empty(
        RequestIdx inIdx) const
        { return (mRequestsPtr[inIdx].mNextIdx == inIdx); }
    bool HasPendingReq(
        int inThreadIdx) const
        { return (! Empty(kIoQueueIdx + inThreadIdx)); }
    bool HasPendingNonBarrierReq(
        int inThreadIdx) const
    {
        const Request* const theReqPtr = Front(kIoQueueIdx + inThreadIdx);
        return (theReqPtr && ! theReqPtr->IsBarrier());
    }
    int GetReqListSize(
        Request& inReq)
    {
        const RequestIdx theHeadIdx(&inReq - mRequestsPtr);
        RequestIdx       theIdx  = theHeadIdx;
        int              theSize = 1;
        while ((theIdx = mRequestsPtr[theIdx].mNextIdx) != theHeadIdx) {
            theSize++;
        }
        return theSize;
    }
    int GetReqListSize(
        int inBufferCount)
    {
        return (inBufferCount <= mRequestBufferCount ? 1 :
            (inBufferCount + mRequestBufferCount - 1) / mRequestBufferCount);
    }
    void Put(
        Request* inReqPtr)
    {
        if (inReqPtr) {
            Put(*inReqPtr);
        }
    }
    void Put(
        Request& inReq)
    {
        mFreeCount += GetReqListSize(inReq);
        inReq.mReqType         = kReqTypeNone;
        inReq.mInFlightFlag    = false;
        inReq.mIoCompletionPtr = 0;
        inReq.mBufferCount     = 0;
        Insert(mRequestsPtr[kFreeQueueIdx], inReq);
        if (mReqWaitersCount > 0) {
            QCASSERT(mFreeCount > 0);
            mFreeReqCond.NotifyAll(); // Give them all a chance to retry.
        }
    }
    Request* Get(
        int inBufferCount)
    {
        int theReqCount = GetReqListSize(inBufferCount);
        if (mFreeCount < theReqCount) {
            return 0;
        }
        Request* const theRetPtr = PopFront(kFreeQueueIdx);
        if (! theRetPtr) {
            return theRetPtr;
        }
        mFreeCount--;
        while (--theReqCount > 0) {
            Request* const thePtr = PopFront(kFreeQueueIdx);
            QCASSERT(thePtr);
            Insert(*theRetPtr, *thePtr);
            mFreeCount--;
        }
        return theRetPtr;
    }
    void TrimRequestList(
        Request& inReq,
        int      inBufferCount)
    {
        const RequestIdx theHeadIdx(&inReq - mRequestsPtr);
        RequestIdx theNextIdx = inReq.mNextIdx;
        int theBufCount = inBufferCount;
        while (theNextIdx != theHeadIdx &&
                (theBufCount -= mRequestBufferCount) > 0) {
            theNextIdx = mRequestsPtr[theNextIdx].mNextIdx;
        }
        while (theNextIdx != theHeadIdx) {
            Request& theReq = mRequestsPtr[theNextIdx];
            theNextIdx = theReq.mNextIdx;
            // Should rarely, if ever get here: efficiency is not of great
            // concern.
            Remove(theReq);
            Put(theReq);
        }
    }
    void Enqueue(
        Request& inReq,
        int      inThreadIdx)
    {
        Trace("enqueue", inReq);
        Insert(mRequestsPtr[kIoQueueIdx + inThreadIdx], inReq);
        mPendingCount++;
        mFilePendingReqCountPtr[inReq.mFileIdx]++;
        if (inReq.mReqType == kReqTypeRead) {
            mPendingReadBlockCount += inReq.mBufferCount;
        } else if (IsWriteReqType(inReq.mReqType)) {
            mPendingWriteBlockCount += inReq.mBufferCount;
        } else if (inReq.mReqType <= kReqTypeNone ||
                inReq.mReqType >= kReqTypeMax) {
            QCRTASSERT(! "Bad request type");
        }
    }
    Request* Dequeue(
        int inThreadIdx)
    {
        Request* const theReqPtr = Front(kIoQueueIdx + inThreadIdx);
        if (theReqPtr) {
            RemoveWithSubRequests(*theReqPtr);
        }
        return theReqPtr;
    }
    void RemoveWithSubRequests(
        Request& inReq)
    {
        // If there are more than one "sub request" then the list head has
        // buffer count larger than request max buffers per request.
        int      theBufCount = inReq.mBufferCount;
        Request* theNextPtr  = mRequestsPtr + inReq.mNextIdx;
        Remove(inReq);
        while ((theBufCount -= mRequestBufferCount) > 0) {
            Request& theReq = *theNextPtr;
            QCRTASSERT(
                mRequestsPtr + mRequestQueueCount <= theNextPtr &&
                theReq.mReqType == kReqTypeNone
            );
            theNextPtr = mRequestsPtr + theReq.mNextIdx;
            Remove(theReq);
            Insert(inReq, theReq);
        }
    }
    RequestId GetRequestId(
        const Request& inReq) const
        { return (RequestId)(&inReq - mRequestsPtr); }
    bool Cancel(
        Request& inReq)
    {
        if (inReq.mReqType == kReqTypeNone) {
            return false; // Not in flight, or in the queue.
        }
        Trace("cancel", inReq);
        RemoveWithSubRequests(inReq);
        RequestComplete(inReq, kErrorCancel, 0, 0);
        return true;
    }
    void Process(
        Request&      inReq,
        int*          inFdPtr,
        struct iovec* inIoVecPtr,
        int           inThreadIdx);
    void ProcessOpenOrCreate(
        Request& inReq,
        int      inThreadIdx);
    void ProcessClose(
        unsigned int inFileIdx,
        int          inThreadIdx);
    void ProcessMeta(
        Request&      inReq,
        struct iovec* inIoVecPtr,
        int           inThreadIdx);
    void RequestComplete(
        Request& inReq,
        Error    inError,
        int      inSysError,
        int64_t  inIoByteCount,
        bool     inFreeBuffersIfNoIoCompletionFlag = false,
        BlockIdx inBlockIdx                        = -1)
    {
        QCASSERT(mMutex.IsOwned());
        QCRTASSERT(
            mPendingCount > 0 &&
            // inReq.mFileIdx >= 0 && always true: unsigned
            int(inReq.mFileIdx) < mFileCount &&
            mFilePendingReqCountPtr[inReq.mFileIdx] > 0
        );
        if (inReq.mReqType == kReqTypeRead) {
            mPendingReadBlockCount -= inReq.mBufferCount;
        } else if (IsWriteReqType(inReq.mReqType)) {
            mPendingWriteBlockCount -= inReq.mBufferCount;
        }
        BlockIdx theBlockIdx;
        if (inReq.IsMeta()) {
            // The first "buffer" has file name allocated with "new char[]".
            char** const theFileNamePtr = GetBuffersPtr(inReq);
            delete [] *theFileNamePtr;
            *theFileNamePtr = 0;
            theBlockIdx = inBlockIdx;
        } else {
            theBlockIdx = (BlockIdx)inReq.mBlockIdx;
        }
        BuffersIterator theItr(*this, inReq, inReq.mBufferCount);
        Trace("done", inReq);
        inReq.mReqType = kReqTypeNone;
        mCompletionRunningCount++;
        if (inReq.mIoCompletionPtr) {
            QCStMutexUnlocker theUnlock(mMutex);
            if (! inReq.mIoCompletionPtr->Done(
                    GetRequestId(inReq),
                    inReq.mFileIdx,
                    theBlockIdx,
                    theItr,
                    inReq.mBufferCount,
                    inError,
                    inSysError,
                    inIoByteCount)) {
                // Free buffers.
                BuffersIterator theItr(*this, inReq, inReq.mBufferCount);
                mBufferPoolPtr->Put(theItr, inReq.mBufferCount);
            }
        } else {
            if (inFreeBuffersIfNoIoCompletionFlag && inReq.mBufferCount > 0) {
                QCStMutexUnlocker theUnlock(mMutex);
                mBufferPoolPtr->Put(theItr, inReq.mBufferCount);
            }
        }
        mCompletionRunningCount--;
        mPendingCount--;
        if (--mFilePendingReqCountPtr[inReq.mFileIdx] <= 0 &&
                mFileInfoPtr[inReq.mFileIdx].mClosedFlag) {
            ScheduleClose(
                inReq.mFileIdx, mFileInfoPtr[inReq.mFileIdx].mThreadIdx);
        }
        Put(inReq);
    }
    void ScheduleClose(
        unsigned int inFileIdx,
        int          inThreadIdx)
    {
        unsigned int& thePendingCloseTail = mPendingCloseTailPtr[inThreadIdx];
        if (thePendingCloseTail != kEndOfPendingCloseList) {
            QCASSERT(
                mFilePendingReqCountPtr[thePendingCloseTail] ==
                kEndOfPendingCloseList
            );
            mFilePendingReqCountPtr[thePendingCloseTail] =
                inFileIdx + kPendingCloseListIdxOff;
        } else {
            unsigned int& thePendingCloseHead =
                mPendingCloseHeadPtr[inThreadIdx];
            QCASSERT(thePendingCloseHead == kEndOfPendingCloseList);
            thePendingCloseHead = inFileIdx + kPendingCloseListIdxOff;
        }
        thePendingCloseTail = inFileIdx;
        mFilePendingReqCountPtr[inFileIdx] = kEndOfPendingCloseList;
    }
    Request* GetRequest(
        QCStMutexLocker& inLocker,
        int              inBufferCount,
        Time             inTimeWaitNanoSec)
    {
        Request* theReqPtr;
        while (! (theReqPtr = Get(inBufferCount))) {
            QCStValueIncrementor<int> theIncr(mReqWaitersCount, 1);
            if (inTimeWaitNanoSec < 0) {
                mFreeReqCond.Wait(mMutex);
            } else if (inTimeWaitNanoSec == 0 ||
                    ! mFreeReqCond.Wait(mMutex, inTimeWaitNanoSec)) {
                if (inTimeWaitNanoSec != 0) {
                    inLocker.Detach();
                }
                return theReqPtr;
            }
        }
        QCASSERT(theReqPtr);
        return theReqPtr;
    }
    void StopSelf();
    void Trace(
        const char*    inMsgPtr,
        const Request& inReq)
    {
        if (! mDebugTracerPtr) {
            return;
        }
        char theBuf[256];
        const int theLen = snprintf(theBuf, sizeof(theBuf),
        "%-16s: tid: %08lx type: %2d id: %3d file idx: %3d buf: %4d compl: %p",
            inMsgPtr,
            (long)pthread_self(),
            (int)inReq.mReqType,
            GetRequestId(inReq),
            (int)inReq.mFileIdx,
            inReq.mBufferCount,
            inReq.mIoCompletionPtr
        );
        if (theLen <= 0) {
            return;
        }
        mDebugTracerPtr->TraceMsg(theBuf, theLen);
    }
    static int CreateFile(
        const char* inFileNamePtr,
        int         inFlags,
        int         inPerms,
        bool        inCreateExclusiveFlag)
    {
        const int theFlags = inFlags | O_CREAT |
            (inCreateExclusiveFlag ? O_EXCL : 0);
        int       theFd;
        while ((theFd = open(inFileNamePtr, theFlags, inPerms)) < 0 &&
                errno == EEXIST &&
                unlink(inFileNamePtr) == 0)
            {}
        return theFd;
    }
    static off_t GetFileSize(
        int inFd)
    {
        struct stat theStat;
        return (fstat(inFd, &theStat) == 0 ? theStat.st_size : off_t(-1));
    }
    void Notify(
        int inThreadIdx)
    {
        if (mRequestProcessorsPtr) {
            mRequestProcessorsPtr[inThreadIdx]->Wakeup();
            return;
        }
        mWorkCondPtr[inThreadIdx].Notify();
    }
    void NotifyAll()
    {
        if (mRequestProcessorsPtr) {
            for (int i = 0; i < mThreadCount; i++) {
                mRequestProcessorsPtr[i]->Wakeup();
            }
            return;
        }
        if (mRequestAffinityFlag) {
            for (int i = 0; i < mThreadCount; i++) {
                mWorkCondPtr[i].Notify();
            }
            return;
        }
        mWorkCondPtr[0].NotifyAll();
    }
    void NotifyAllWithPending()
    {
        if (mRequestProcessorsPtr) {
            if (! mRequestAffinityFlag && ! HasPendingReq(0)) {
                return;
            }
            for (int i = 0; i < mThreadCount; i++) {
                if (! mRequestAffinityFlag || HasPendingReq(i)) {
                    mRequestProcessorsPtr[i]->Wakeup();
                }
            }
            return;
        }
        if (mRequestAffinityFlag) {
            for (int i = 0; i < mThreadCount; i++) {
                if (HasPendingReq(i)) {
                    mWorkCondPtr[i].Notify();
                }
            }
            return;
        }
        if (HasPendingReq(0)) {
            mWorkCondPtr[0].NotifyAll();
        }
    }
    void Wait(
        int inThreadIdx)
    {
        if (mRequestProcessorsPtr) {
            QCStMutexUnlocker theUnlock(mMutex);
            mRequestProcessorsPtr[inThreadIdx]->ProcessAndWait();
            return;
        }
        mWorkCondPtr[inThreadIdx].Wait(mMutex);
    }
private:
    Queue(
        const Queue& inQueue);
    Queue& operator=(
        const Queue& inQueue);
};

    inline void
QCDiskQueue::Queue::Done(
    QCDiskQueue::RequestProcessor& inProcessor,
    QCDiskQueue::Request&          inReq,
    QCDiskQueue::Error             inError,
    int                            inSysError,
    int64_t                        inIoByteCount,
    QCDiskQueue::BlockIdx          inBlockIdx,
    QCDiskQueue::InputIterator*    inInputIteratorPtr)
{
    Request& theReq = static_cast<Request&>(inReq);
    if (inInputIteratorPtr) {
        QCRTASSERT(
            inProcessor.AllocatesReadBuffers() &&
            kReqTypeRead == theReq.mReqType &&
            0 < theReq.mBufferCount &&
            ! GetBuffersPtr(theReq)[0]
        );
        BuffersIterator theIt(*this, theReq, theReq.mBufferCount);
        char*           thePtr;
        while ((thePtr = inInputIteratorPtr->Get())) {
            theIt.Put(thePtr);
        }
    }
    QCStMutexLocker theLocker(mMutex);
    QCRTASSERT(mRequestProcessorsPtr);
    RequestComplete(
        theReq,
        inError,
        inSysError,
        inIoByteCount,
        theReq.mFreeBuffersIfNoIoCompletionFlag,
        inBlockIdx
    );
}

    void
QCDiskQueue::Queue::StopSelf()
{
    QCASSERT(mMutex.IsOwned());
    mRunFlag = false;
    if (mWorkCondPtr || mRequestProcessorsPtr) {
        NotifyAll();
    }
    for (int i = 0; i < mThreadCount; i++) {
        QCThread& theThread = mThreadsPtr[i];
        QCStMutexUnlocker theUnlock(mMutex);
        theThread.Join();
    }
    QCASSERT(mCompletionRunningCount == 0);
    if (mRequestsPtr) {
        Request* theReqPtr;
        for (int i = 0; i < (mRequestAffinityFlag ? mThreadCount : 1); i++) {
            while ((theReqPtr = Dequeue(i))) {
                Cancel(*theReqPtr);
            }
        }
        QCASSERT(mPendingCount == 0);
    }
    while (mFdCount > 0) {
        if (mFdPtr[--mFdCount] >= 0) {
            close(mFdPtr[mFdCount]);
        }
    }
    delete [] mWorkCondPtr;
    mWorkCondPtr = 0;
    delete [] mFdPtr;
    mFdPtr = 0;
    delete [] mFilePendingReqCountPtr;
    mFilePendingReqCountPtr = 0;
    delete [] mFileInfoPtr;
    mFileInfoPtr = 0;
    delete [] mThreadsPtr;
    mThreadsPtr = 0;
    delete [] mBuffersPtr;
    mBuffersPtr = 0;
    mRequestBufferCount = 0;
    delete [] mRequestsPtr;
    mRequestsPtr = 0;
    delete [] mPendingCloseHeadPtr;
    mPendingCloseHeadPtr = 0;
    mPendingCloseTailPtr = 0;
    mFreeCount = 0;
    mTotalCount = 0;
    mPendingCount = 0;
    mBufferPoolPtr = 0;
    delete [] mIoVecPtr;
    mIoVecPtr = 0;
    mIoVecPerThreadCount = 0;
    mThreadCount = 0;
    mFreeFdHead = kFreeFdEnd;
    mFileCount = 0;
    mReqWaitersCount = 0;
    mDebugTracerPtr = 0;
    mIoStartObserverPtr = 0;
}

    int
QCDiskQueue::Queue::Start(
    int                             inThreadCount,
    int                             inMaxQueueDepth,
    int                             inMaxBuffersPerRequestCount,
    int                             inFileCount,
    const char**                    inFileNamesPtr,
    QCIoBufferPool&                 inBufferPool,
    QCDiskQueue::IoStartObserver*   inIoStartObserverPtr,
    QCDiskQueue::CpuAffinity        inCpuAffinity,
    QCDiskQueue::DebugTracer*       inDebugTracerPtr,
    bool                            inBufferedIoFlag,
    bool                            inCreateExclusiveFlag,
    bool                            inRequestAffinityFlag,
    bool                            inSerializeMetaRequestsFlag,
    QCDiskQueue::RequestProcessor** inRequestProcessorsPtr)
{
    QCStMutexLocker theLocker(mMutex);
    StopSelf();
    if (inFileCount <= 0 || inThreadCount <= 0 ||
            inThreadCount <= 0 || inMaxQueueDepth <= 0 ||
            inMaxBuffersPerRequestCount <= 0) {
        return 0;
    }
    if (inFileCount >= (1 << kFileIndexBitCount)) {
        return EINVAL;
    }
    mBufferPoolPtr = &inBufferPool;
    if (mRequestProcessorsPtr) {
        for (int i = 0; i < inThreadCount; i++) {
            if (! mRequestProcessorsPtr[i]) {
                return EINVAL;
            }
        }
    }
#ifdef IOV_MAX
    const int kMaxIoVecCount = IOV_MAX;
#else
    const int kMaxIoVecCount = 1 << 10;
#endif
    mRequestProcessorsPtr = inRequestProcessorsPtr;
    mSerializeMetaRequestsFlag =
        inSerializeMetaRequestsFlag || ! inRequestAffinityFlag;
    mNextThreadIdx = 0;
    mDebugTracerPtr = inDebugTracerPtr;
    mIoStartObserverPtr = inIoStartObserverPtr;
    mIoVecPerThreadCount = mRequestProcessorsPtr ? 0 : Min(
        Min(kMaxIoVecCount, Min(4 << 10, inMaxBuffersPerRequestCount * 32)),
        inMaxQueueDepth * inMaxBuffersPerRequestCount
    );
    mRequestAffinityFlag = inRequestAffinityFlag;
    if (! mRequestProcessorsPtr) {
        mWorkCondPtr = new QCCondVar[
            mRequestAffinityFlag ? inThreadCount : 1];
    }
    // The last entry is pseudo file for meta requests.
    mFileCount = inFileCount + 1;
    if (0 < mIoVecPerThreadCount) {
        mIoVecPtr = new struct iovec[mIoVecPerThreadCount * inThreadCount];
    }
    mBlockSize = inBufferPool.GetBufferSize();
    const int theFdCount = mRequestAffinityFlag ?
        mFileCount : inThreadCount * mFileCount;
    mFdPtr = new int[theFdCount];
    mFilePendingReqCountPtr = new unsigned int[mFileCount];
    const int theCloseQueueCnt =
        2 * (inRequestAffinityFlag ? inThreadCount : 1);
    mPendingCloseHeadPtr = new unsigned int[theCloseQueueCnt];
    for (int i = 0; i < theCloseQueueCnt; i++) {
        mPendingCloseHeadPtr[i] = kEndOfPendingCloseList;
    }
    mPendingCloseTailPtr = mPendingCloseHeadPtr + theCloseQueueCnt / 2;
    mFileInfoPtr = new FileInfo[mFileCount];
    mFileInfoPtr[mFileCount - 1].mThreadIdx = 0;
    mFreeFdHead = kFreeFdEnd;
    mCreateExclusiveFlag = inCreateExclusiveFlag;
    for (mFdCount = 0; mFdCount < theFdCount; ) {
        int theError = 0;
        for (int i = 0; i < mFileCount; i++) {
            const bool theOpenFileFlag = inFileNamesPtr && i < inFileCount;
            int& theFd = mFdPtr[mFdCount];
            theFd = theOpenFileFlag ? open(inFileNamesPtr[i],
                GetOpenCommonFlags(inBufferedIoFlag) | O_RDWR) : -1;
            if (theFd < 0 && theOpenFileFlag) {
                theError = errno;
                break;
            }
            if (theFd >= 0 && fcntl(theFd, F_SETFD, FD_CLOEXEC)) {
                theError = errno;
                break;
            }
            if (++mFdCount > mFileCount) {
                continue;
            }
            const off_t theSize = theFd >= 0 ? GetFileSize(theFd) : 0;
            if (theSize < 0) {
                theError = errno;
                break;
            }
            // Allow last partial block.
            const int64_t theBlkIdx =
                (int64_t(theSize) + mBlockSize - 1) / mBlockSize;
            if (theBlkIdx >= (int64_t(1) << kBlockBitCount)) {
                theError = EOVERFLOW;
                break;
            }
            if (! mRequestAffinityFlag || mThreadCount <= mNextThreadIdx) {
                mNextThreadIdx = 0;
            }
            mFilePendingReqCountPtr[i] = 0;
            mFileInfoPtr[i].mLastBlockIdx          = theBlkIdx;
            mFileInfoPtr[i].mSpaceAllocPendingFlag = false;
            mFileInfoPtr[i].mOpenPendingFlag       = false;
            mFileInfoPtr[i].mOpenError             = kOpenErrorNone;
            mFileInfoPtr[i].mClosedFlag            = false;
            mFileInfoPtr[i].mCloseFileSize         = -1;
            mFileInfoPtr[i].mThreadIdx             = mNextThreadIdx++;
            if (theFd < 0 && i < inFileCount) {
                theFd = mFreeFdHead;
                mFreeFdHead = -(i + kFreeFdOffset);
            }
        }
        if (theError) {
            StopSelf();
            return theError;
        }
    }
    mBuffersPtr = new char*[inMaxQueueDepth * inMaxBuffersPerRequestCount];
    mRequestBufferCount = inMaxBuffersPerRequestCount;
    mRequestQueueCount  = kRequestQueueCount +
        (mRequestAffinityFlag ? inThreadCount - 1 : 0);
    const int theReqCnt = mRequestQueueCount + inMaxQueueDepth;
    mRequestsPtr = new Request[theReqCnt];
    // Init list heads: kFreeQueueIdx kIoQueueIdx.
    for (mTotalCount = 0; mTotalCount < mRequestQueueCount; mTotalCount++) {
        Init(mRequestsPtr[mTotalCount]);
    }
    // Make free list.
    for (; mTotalCount < theReqCnt; mTotalCount++) {
        Request& theReq = mRequestsPtr[mTotalCount];
        Init(theReq);
        Put(theReq);
    }
    mThreadsPtr = new IoThread[inThreadCount];
    mRunFlag    = true;
    const int         kStackSize = (inRequestProcessorsPtr ? 512 : 64) << 10;
    const char* const kNamePtr   = "IO";
    for (mThreadCount = 0; mThreadCount < inThreadCount; mThreadCount++) {
        const int theRet = mThreadsPtr[mThreadCount].Start(
            *this, mThreadCount, kStackSize, kNamePtr, inCpuAffinity);
        if (theRet != 0) {
            StopSelf();
            return theRet;
        }
    }
    return 0;
}

    QCDiskQueue::EnqueueStatus
QCDiskQueue::Queue::Enqueue(
    QCDiskQueue::ReqType        inReqType,
    QCDiskQueue::FileIdx        inFileIdx,
    QCDiskQueue::BlockIdx       inBlockIdx,
    QCDiskQueue::InputIterator* inBufferIteratorPtr,
    int                         inBufferCount,
    QCDiskQueue::IoCompletion*  inIoCompletionPtr,
    QCDiskQueue::Time           inTimeWaitNanoSec,
    int64_t                     inEofHint)
{
    if ((inReqType != kReqTypeRead && ! IsWriteReqType(inReqType)) ||
            inBufferCount <= 0 ||
            inBufferCount > (mRequestBufferCount *
                (mTotalCount - mRequestQueueCount)) ||
            (! inBufferIteratorPtr && IsWriteReqType(inReqType))) {
        return EnqueueStatus(kRequestIdNone, kErrorParameter);
    }
    QCStMutexLocker theLocker(mMutex);
    if (! mRunFlag) {
        return EnqueueStatus(kRequestIdNone, kErrorQueueStopped);
    }
    if (inFileIdx < 0 || inFileIdx >= mFileCount || mFdPtr[inFileIdx] < 0) {
        return EnqueueStatus(kRequestIdNone, kErrorFileIdxOutOfRange);
    }
    const OpenError theOpenError = mFileInfoPtr[inFileIdx].mOpenError;
    if (theOpenError != kOpenErrorNone) {
        return EnqueueStatus(kRequestIdNone, kErrorOpen,
            Open2SysError(theOpenError));
    }
    if (mFileInfoPtr[inFileIdx].mClosedFlag) {
        return EnqueueStatus(kRequestIdNone, kErrorFileIdxOutOfRange);
    }
    if (inBlockIdx < 0 || (! mFileInfoPtr[inFileIdx].mOpenPendingFlag &&
            inBlockIdx + (inBufferIteratorPtr ? 0 : inBufferCount) >
            int64_t(mFileInfoPtr[inFileIdx].mLastBlockIdx))) {
        return EnqueueStatus(kRequestIdNone, kErrorBlockIdxOutOfRange);
    }
    Request* const theReqPtr = GetRequest(
        theLocker, inBufferCount, inTimeWaitNanoSec);
    if (! theReqPtr) {
        return EnqueueStatus(kRequestIdNone, kErrorOutOfRequests);
    }
    Request& theReq = *theReqPtr;
    theReq.mReqType         = inReqType;
    theReq.mBufferCount     = 0;
    theReq.mFileIdx         = inFileIdx;
    theReq.mBlockIdx        = inBlockIdx;
    theReq.mIoCompletionPtr = inIoCompletionPtr;
    if (inBufferIteratorPtr) {
        BuffersIterator theItr(*this, theReq, inBufferCount);
        for (int i = 0; i < inBufferCount; i++) {
            char* const thePtr = inBufferIteratorPtr->Get();
            if (! thePtr) {
                break;
            }
            theItr.Put(thePtr);
            theReq.mBufferCount++;
        }
        if (theReq.mBufferCount < inBufferCount) {
            // Free unused requests if any.
            TrimRequestList(theReq, theReq.mBufferCount);
        }
    } else if (inReqType == kReqTypeRead) {
        // Defer buffer allocation.
        GetBuffersPtr(theReq)[0] = 0;
        theReq.mBufferCount = inBufferCount;
    }
    if (! mFileInfoPtr[theReq.mFileIdx].mOpenPendingFlag &&
            theReq.mBlockIdx + theReq.mBufferCount >
                uint64_t(mFileInfoPtr[theReq.mFileIdx].mLastBlockIdx)) {
        Put(theReq);
        return EnqueueStatus(kRequestIdNone, kErrorBlockIdxOutOfRange);
    }
    if (theReq.mBufferCount <= 0) {
        Put(theReq);
        return EnqueueStatus(kRequestIdNone, kErrorBlockCountOutOfRange);
    }
    if (kReqTypeWriteSync == inReqType && 0 <= inEofHint) {
        mFileInfoPtr[inFileIdx].mCloseFileSize = inEofHint;
    }
    const int theThreadIdx = mFileInfoPtr[inFileIdx].mThreadIdx;
    Enqueue(theReq, theThreadIdx);
    if (! mBarrierFlag) {
        Notify(theThreadIdx);
    }
    return EnqueueStatus(GetRequestId(theReq), kErrorNone);
}

    bool
QCDiskQueue::Queue::Cancel(
    QCDiskQueue::RequestId inRequestId)
{
    QCStMutexLocker theLocker(mMutex);
    return (
        mPendingCount > 0 &&
        inRequestId >= mRequestQueueCount &&
        inRequestId < mTotalCount &&
        ! mRequestsPtr[inRequestId].mInFlightFlag &&
        Cancel(mRequestsPtr[inRequestId])
    );
}

    QCDiskQueue::IoCompletion*
QCDiskQueue::Queue::CancelOrSetCompletionIfInFlight(
    QCDiskQueue::RequestId     inRequestId,
    QCDiskQueue::IoCompletion* inCompletionIfInFlightPtr)
{
    QCStMutexLocker theLocker(mMutex);
    if (mPendingCount <= 0 ||
            inRequestId < mRequestQueueCount ||
            inRequestId >= mTotalCount) {
        return 0;
    }
    Request& theReq = mRequestsPtr[inRequestId];
    if (theReq.mReqType == kReqTypeNone) {
        // Completion is already running, or done.
        Trace("cancel: runing", theReq);
        return theReq.mIoCompletionPtr;
    }
    if (! theReq.mInFlightFlag) {
        IoCompletion* const theIoCompletionPtr = theReq.mIoCompletionPtr;
        return (Cancel(theReq) ? theIoCompletionPtr : 0);
    }
    // In flight, change io completion.
    Trace("cancel: inflight", theReq);
    theReq.mIoCompletionPtr = inCompletionIfInFlightPtr;
    return inCompletionIfInFlightPtr;
}

    void
QCDiskQueue::Queue::Run(
    int inThreadIndex)
{
    QCStMutexLocker theLocker(mMutex);
    QCASSERT(inThreadIndex >= 0 && inThreadIndex < mThreadCount);
    int* const          theFdPtr    = mFdPtr +
        (mRequestAffinityFlag ? 0 : mFdCount / mThreadCount * inThreadIndex);
    struct iovec* const theIoVecPtr = mIoVecPtr +
        mIoVecPerThreadCount * inThreadIndex;
    const int theThreadQueueIdx = mRequestAffinityFlag ? inThreadIndex : 0;
    unsigned int& thePendingCloseHead = mPendingCloseHeadPtr[theThreadQueueIdx];
    unsigned int& thePendingCloseTail = mPendingCloseTailPtr[theThreadQueueIdx];
    bool theBarrierFlag = false;
    while (mRunFlag) {
        Request* theReqPtr = 0;
        while ((mBarrierFlag ||
                    (thePendingCloseHead == kEndOfPendingCloseList &&
                    ! (theReqPtr = Dequeue(theThreadQueueIdx)))) &&
                    mRunFlag) {
            Wait(theThreadQueueIdx);
        }
        if (! mRunFlag) {
            if (theReqPtr) {
                Cancel(*theReqPtr);
            }
            break;
        }
        mBarrierFlag = mSerializeMetaRequestsFlag && (theReqPtr ?
            theReqPtr->IsBarrier() :
            ! mRequestAffinityFlag &&
                thePendingCloseHead != kEndOfPendingCloseList);
        if (theBarrierFlag && ! mBarrierFlag && 0 < mThreadCount) {
            // Wake up other threads after barrier req.
            NotifyAllWithPending();
        }
        theBarrierFlag = mBarrierFlag;
        if (theReqPtr) {
            QCASSERT(thePendingCloseHead == kEndOfPendingCloseList);
            Process(*theReqPtr, theFdPtr, theIoVecPtr, inThreadIndex);
        } else {
            QCASSERT(
                thePendingCloseHead != kEndOfPendingCloseList &&
                kPendingCloseListIdxOff <= thePendingCloseHead &&
                (mBarrierFlag || mRequestAffinityFlag)
            );
            unsigned int theHead = thePendingCloseHead;
            thePendingCloseHead = kEndOfPendingCloseList;
            thePendingCloseTail = kEndOfPendingCloseList;
            while (theHead != kEndOfPendingCloseList) {
                const unsigned int theFileIdx =
                    theHead - kPendingCloseListIdxOff;
                theHead = mFilePendingReqCountPtr[theFileIdx];
                ProcessClose(theFileIdx, theThreadQueueIdx);
            }
        }
        if (theBarrierFlag) {
            QCRTASSERT(mBarrierFlag);
            mBarrierFlag = false;
        }
    }
    if (mRequestProcessorsPtr) {
        mRequestProcessorsPtr[inThreadIndex]->Stop();
    }
    NotifyAll(); // Wakeup all worker threads on exit.
}

    void
QCDiskQueue::Queue::Process(
    Request&      inReq,
    int*          inFdPtr,
    struct iovec* inIoVecPtr,
    int           inThreadIdx)
{
    QCASSERT(mMutex.IsOwned());
    QCASSERT((0 < mIoVecPerThreadCount || mRequestProcessorsPtr) &&
        mBufferPoolPtr);
    if (inReq.mReqType == kReqTypeOpen ||
            inReq.mReqType == kReqTypeCreate ||
            inReq.mReqType == kReqTypeOpenRO ||
            inReq.mReqType == kReqTypeCreateRO) {
        ProcessOpenOrCreate(inReq, inThreadIdx);
        return;
    }
    if (inReq.IsMeta()) {
        ProcessMeta(inReq, inIoVecPtr, inThreadIdx);
        return;
    }

    const int     theFd        = inFdPtr[inReq.mFileIdx];
    char** const  theBufPtr    = GetBuffersPtr(inReq);
    const off_t   theOffset    = (off_t)inReq.mBlockIdx * mBlockSize;
    const bool    theReadFlag  = inReq.mReqType == kReqTypeRead;
    const bool    theSyncFlag  = inReq.mReqType == kReqTypeWriteSync;
    const int64_t theAllocSize = (IsWriteReqType(inReq.mReqType) &&
        mFileInfoPtr[inReq.mFileIdx].mSpaceAllocPendingFlag) ?
            mFileInfoPtr[inReq.mFileIdx].mLastBlockIdx * mBlockSize : 0;
    QCRTASSERT((theReadFlag || IsWriteReqType(inReq.mReqType)) && theFd >= 0);
    inReq.mInFlightFlag = true;
    const OpenError theOpenError = mFileInfoPtr[inReq.mFileIdx].mOpenError;
    if (theOpenError != kOpenErrorNone) {
        RequestComplete(inReq, kErrorOpen, Open2SysError(theOpenError),
            0, ! theBufPtr[0]);
        return;
    }
    if ((IsWriteReqType(inReq.mReqType) || inReq.mReqType == kReqTypeRead) &&
            ! mFileInfoPtr[inReq.mFileIdx].mOpenPendingFlag &&
            inReq.mBlockIdx + inReq.mBufferCount >
            uint64_t(mFileInfoPtr[inReq.mFileIdx].mLastBlockIdx)) {
        RequestComplete(inReq, kErrorBlockIdxOutOfRange, 0, 0, ! theBufPtr[0]);
        return;
    };
    int64_t theFileSize;
    if (theSyncFlag) {
        theFileSize = mFileInfoPtr[inReq.mFileIdx].mCloseFileSize;
        if (theFileSize >= 0 && theFileSize >= mBlockSize *
                int64_t(mFileInfoPtr[inReq.mFileIdx].mLastBlockIdx)) {
            theFileSize = -1;
        }
    } else {
        theFileSize = -1;
    }
    if (mRequestProcessorsPtr && 0 < theAllocSize) {
        mFileInfoPtr[inReq.mFileIdx].mSpaceAllocPendingFlag = false;
    }
    const RequestId theReqId = GetRequestId(inReq);
    QCStMutexUnlocker theUnlock(mMutex);

    Trace("process", inReq);
    if (mIoStartObserverPtr) {
        mIoStartObserverPtr->Notify(
            inReq.mReqType,
            theReqId,
            inReq.mFileIdx,
            inReq.mBlockIdx,
            inReq.mBufferCount
        );
    }
    if (mRequestProcessorsPtr) {
        const bool theGetBufFlag = ! theBufPtr[0] &&
            ! mRequestProcessorsPtr[inThreadIdx]->AllocatesReadBuffers();
        inReq.mFreeBuffersIfNoIoCompletionFlag = theGetBufFlag;
        if (theGetBufFlag) {
            QCASSERT(theReadFlag);
            BuffersIterator theIt(*this, inReq, inReq.mBufferCount);
            // Allocate buffers for read request.
            if (! mBufferPoolPtr->Get(theIt, inReq.mBufferCount,
                    QCIoBufferPool::kRefillReqIdRead)) {
                theUnlock.Lock();
                RequestComplete(inReq, kErrorOutOfBuffers, 0, 0, theGetBufFlag);
                return;
            }
        }
        const int theBufferCount = (theGetBufFlag || 0 != theBufPtr[0]) ?
            inReq.mBufferCount : 0;
        BuffersIterator theIt(*this, inReq, theBufferCount);
        mRequestProcessorsPtr[inThreadIdx]->StartIo(
            inReq,
            inReq.mReqType,
            theFd,
            inReq.mBlockIdx,
            inReq.mBufferCount,
            0 < theBufferCount ? &theIt : 0,
            theAllocSize,
            theFileSize
        );
        return;
    }

    Error theError    = kErrorNone;
    int   theSysError = 0;
    if (theAllocSize > 0) {
        // Theoretically space allocation can be simultaneously invoked from
        // more than one io thread. This is to ensure that allocation always
        // happen before the first write.
        // OS can deal with concurrent allocations.
        const int64_t theResv = QCUtils::ReserveFileSpace(theFd, theAllocSize);
        if (theResv < 0) {
            theError = kErrorSpaceAlloc;
            theSysError = int(-theResv);
        }
        if (theResv > 0 && ftruncate(theFd, theAllocSize)) {
            theError = kErrorSpaceAlloc;
            theSysError = errno;
        }
        if (theError == kErrorNone) {
            QCStMutexLocker theLocker(mMutex);
            mFileInfoPtr[inReq.mFileIdx].mSpaceAllocPendingFlag = false;
        }
    }

    const bool theGetBufFlag = ! theBufPtr[0];
    if (theError == kErrorNone && theGetBufFlag) {
        QCASSERT(theReadFlag);
        BuffersIterator theIt(*this, inReq, inReq.mBufferCount);
        // Allocate buffers for read request.
        if (! mBufferPoolPtr->Get(theIt, inReq.mBufferCount,
                QCIoBufferPool::kRefillReqIdRead)) {
            theError = kErrorOutOfBuffers;
        }
    }
    if (theError == kErrorNone &&
            lseek(theFd, theOffset, SEEK_SET) != theOffset) {
        theError    = kErrorSeek;
        theSysError = errno;
    }
    BuffersIterator theItr(*this, inReq, inReq.mBufferCount);
    int             theBufCnt    = inReq.mBufferCount;
    int64_t         theIoByteCnt = 0;
    while (theBufCnt > 0 && theError == kErrorNone) {
        ssize_t theIoBytes  = 0;
        int     theIoVecCnt = 0;
        char*   thePtr;
        while (theIoVecCnt < mIoVecPerThreadCount && (thePtr = theItr.Get())) {
            inIoVecPtr[theIoVecCnt  ].iov_base = thePtr;
            inIoVecPtr[theIoVecCnt++].iov_len  = mBlockSize;
            theIoBytes += mBlockSize;
            theBufCnt--;
        }
        QCRTASSERT(theIoVecCnt > 0);
        if (theReadFlag) {
            const ssize_t theNRd = readv(theFd, inIoVecPtr, theIoVecCnt);
            if (theNRd < 0) {
                theError = kErrorRead;
                theSysError = theNRd < 0 ? errno : 0;
                break;
            }
            theIoByteCnt += theNRd;
            if (theNRd < theIoBytes) {
                if (theGetBufFlag) {
                    // Short read -- release extra buffers.
                    mBufferPoolPtr->Put(theItr, theBufCnt);
                    inReq.mBufferCount -= theBufCnt;
                    int i = (theNRd + mBlockSize - 1) / mBlockSize;
                    inReq.mBufferCount -= theIoVecCnt - i;
                    while (i < theIoVecCnt) {
                        mBufferPoolPtr->Put((char*)inIoVecPtr[i++].iov_base);
                    }
                }
                break;
            }
        } else {
            const ssize_t theNWr = writev(theFd, inIoVecPtr, theIoVecCnt);
            if (theNWr > 0) {
                theIoByteCnt += theNWr;
            }
            if (theNWr != theIoBytes) {
                theError = kErrorWrite;
                theSysError = errno;
                break;
            }
        }
    }
    if (theGetBufFlag && theError != kErrorNone && theBufPtr[0]) {
        BuffersIterator theIt(*this, inReq, inReq.mBufferCount);
        mBufferPoolPtr->Put(theIt, inReq.mBufferCount);
        theBufPtr[0] = 0;
    }
    if (theSyncFlag && theError == kErrorNone && fsync(theFd)) {
        theError    = kErrorWrite;
        theSysError = errno;
    }
    theUnlock.Lock();
    RequestComplete(inReq, theError, theSysError, theIoByteCnt, theGetBufFlag);
}

    void
QCDiskQueue::Queue::ProcessOpenOrCreate(
    Request& inReq,
    int      inThreadIdx)
{
    QCASSERT(
        mMutex.IsOwned() &&
        (inReq.mReqType == kReqTypeOpen     ||
         inReq.mReqType == kReqTypeCreate   ||
         inReq.mReqType == kReqTypeOpenRO   ||
         inReq.mReqType == kReqTypeCreateRO)
    );
    inReq.mInFlightFlag = true;
    const int         theIdx          = inReq.mFileIdx;
    const int64_t     theMaxFileSize  =
        mFileInfoPtr[theIdx].mLastBlockIdx > 0 ?
        int64_t(mFileInfoPtr[theIdx].mLastBlockIdx) * mBlockSize : -1;
    const char* const theFileNamePtr  = GetBuffersPtr(inReq)[0];
    const bool        theReadOnlyFlag =
        inReq.mReqType == kReqTypeOpenRO ||
        inReq.mReqType == kReqTypeCreateRO;
    const bool        theCreateFlag   =
        inReq.mReqType == kReqTypeCreate ||
        inReq.mReqType == kReqTypeCreateRO;
    const RequestId   theReqId        = GetRequestId(inReq);
    const int         theOpenFlags    =
        (theReadOnlyFlag ? O_RDONLY : O_RDWR) |
        GetOpenCommonFlags(mFileInfoPtr[theIdx].mBufferedIoFlag);
    const bool        theCreateExclusiveFlag = mCreateExclusiveFlag;

    QCRTASSERT(theIdx >= 0 && theIdx < mFileCount && theFileNamePtr);
    QCStMutexUnlocker theUnlock(mMutex);

    Trace("process: open", inReq);
    if (mIoStartObserverPtr) {
        mIoStartObserverPtr->Notify(
            inReq.mReqType,
            theReqId,
            inReq.mFileIdx,
            theMaxFileSize,
            0
        );
    }

    int theSysErr = 0;
    int i;
    for (i = theIdx; i < mFdCount; i += mFileCount) {
        QCRTASSERT(mFdPtr[i] == kOpenPendingFd);
        int64_t theSize;
        if (mRequestProcessorsPtr) {
            theSize = theMaxFileSize;
            const int theFd =  mRequestProcessorsPtr[inThreadIdx]->Open(
                theFileNamePtr,
                theReadOnlyFlag,
                i == theIdx && theCreateFlag,
                i == theIdx && theCreateExclusiveFlag,
                theSize
            );
            if (theFd < 0) {
                theSysErr = -theFd;
                break;
            }
            if (theSize < 0) {
                theSysErr = EINVAL;
                break;
            }
            mFdPtr[i] = theFd;
            if (mFileCount <= i) {
                continue;
            }
        } else {
            const int theFd    = (theCreateFlag && i == theIdx) ?
                CreateFile(theFileNamePtr, theOpenFlags, S_IRUSR | S_IWUSR,
                    theCreateExclusiveFlag) :
                open(theFileNamePtr, theOpenFlags);
            if (theFd < 0 || fcntl(theFd, F_SETFD, FD_CLOEXEC)) {
                theSysErr = errno ? errno : -1;
                break;
            }
            mFdPtr[i] = theFd;
            if (mFileCount <= i) {
                continue;
            }
            theSize = GetFileSize(theFd);
            if (theSize < 0) {
                theSysErr = errno;
                break;
            }
        }
        const int64_t theBlkIdx =
            (int64_t(theMaxFileSize < 0 ? theSize : theMaxFileSize) +
                mBlockSize - 1) / mBlockSize;
        if (theBlkIdx >= (int64_t(1) << kBlockBitCount)) {
            theSysErr = EOVERFLOW;
            break;
        }
        mFileInfoPtr[i].mLastBlockIdx = theBlkIdx;
        mFileInfoPtr[i].mSpaceAllocPendingFlag =
            mFileInfoPtr[i].mSpaceAllocPendingFlag &&
            ! theReadOnlyFlag && theMaxFileSize > 0 && theSize < theMaxFileSize;
    }
    if (theSysErr) {
        while ((i -= mFileCount) >= theIdx) {
            if (0 <= mFdPtr[i]) {
                close(mFdPtr[i]);
            }
            mFdPtr[i] = kOpenPendingFd;
        }
    }

    theUnlock.Lock();
    mFileInfoPtr[theIdx].mOpenError       = Sys2OpenError(theSysErr);
    mFileInfoPtr[theIdx].mOpenPendingFlag = false;
    RequestComplete(inReq, theSysErr ? kErrorOpen : kErrorNone, theSysErr, 0);
}

    void
QCDiskQueue::Queue::ProcessClose(
    unsigned int inFileIdx,
    int          inThreadIdx)
{
    QCASSERT(mMutex.IsOwned());
    QCRTASSERT((int)inFileIdx < mFileCount);
    int64_t theFileSize = mFileInfoPtr[inFileIdx].mCloseFileSize;
    if (theFileSize >= 0 && theFileSize >= mBlockSize *
            int64_t(mFileInfoPtr[inFileIdx].mLastBlockIdx)) {
        theFileSize = -1;
    }
    int theSysErr = 0;
    if (mFileInfoPtr[inFileIdx].mOpenError == kOpenErrorNone) {
        QCStMutexUnlocker theUnlock(mMutex);

        if (mIoStartObserverPtr) {
            mIoStartObserverPtr->Notify(
                kReqTypeClose,
                kRequestIdNone,
                inFileIdx,
                theFileSize,
                0
            );
        }

        const int theFd = mRequestProcessorsPtr ? -1 : mFdPtr[inFileIdx];
        if (! mRequestProcessorsPtr) {
            mFdPtr[inFileIdx] = -1;
        }
        for (int k = inThreadIdx,
                i = inFileIdx + (mRequestProcessorsPtr ? 0 : mFileCount);
                i < mFdCount;
                i += mFileCount) {
            if (mRequestProcessorsPtr) {
                const int theErr =
                    mRequestProcessorsPtr[k++]->Close(mFdPtr[i], theFileSize);
                if (theErr) {
                    theSysErr = theErr;
                }
            } else if (close(mFdPtr[i])) {
                theSysErr = errno ? errno : -1;
            }
            mFdPtr[i] = -1;
        }
        if (theFd >= 0) {
            if (theFileSize >= 0 && ftruncate(theFd, (off_t)theFileSize)) {
                theSysErr = errno ? errno : -1;
            }
            if (close(theFd)) {
                theSysErr = errno ? errno : -1;
            }
        }
    }
    // Close cannot fail -- it must at least close the file descriptor.
    // Truncate failure handling and discovery left to the "app" -- the logical
    // file size might be larger in the case if truncate fails.
    // All other errors should be reported by io completion as io is direct or
    // at least the write is synchronous.
    if (mDebugTracerPtr) {
        char theBuf[128];
        const int theLen = snprintf(theBuf, sizeof(theBuf),
            "%-16s: tid: %08lx file idx: %5d error: %d",
            "close",
            (long)pthread_self(),
            (int)inFileIdx,
            theSysErr
        );
        if (theLen > 0) {
            mDebugTracerPtr->TraceMsg(theBuf, theLen);
        }
    }
    mFdPtr[inFileIdx] = mFreeFdHead;
    mFreeFdHead = -(inFileIdx + kFreeFdOffset);
}

    inline static int
GetFsAvailable(
    const char* inPathPtr,
    int64_t&    outBytesAvail,
    int64_t&    outBytesTotal)
{
#ifdef QC_OS_NAME_DARWIN
    struct statfs theStat;
    const int theRet = statfs(inPathPtr, &theStat);
    if (theRet == 0) {
        outBytesAvail = (int64_t)theStat.f_bavail * theStat.f_bsize;
        outBytesTotal = (int64_t)theStat.f_blocks * theStat.f_bsize;
    }
#else
    struct statvfs theStat;
    const int theRet = statvfs(inPathPtr, &theStat);
    if (theRet == 0) {
        outBytesAvail = (int64_t)theStat.f_bavail * theStat.f_frsize;
        outBytesTotal = (int64_t)theStat.f_blocks * theStat.f_frsize;
    }
#endif
    return theRet;
}

    void
QCDiskQueue::Queue::ProcessMeta(
    Request&      inReq,
    struct iovec* inIoVecPtr,
    int           inThreadIdx)
{
    QCASSERT(
        mMutex.IsOwned() &&
        (inReq.mReqType == kReqTypeDelete ||
         inReq.mReqType == kReqTypeRename ||
         inReq.mReqType == kReqTypeGetFsAvailable ||
         inReq.mReqType == kReqTypeCheckDirReadable ||
         inReq.mReqType == kReqTypeCheckDirWritable) &&
         (int)inReq.mFileIdx == mFileCount - 1 // always the last pseudo entry
    );
    inReq.mInFlightFlag = true;
    const char* const theNamePtr             = GetBuffersPtr(inReq)[0];
    const ReqType     theReqType             = inReq.mReqType;
    const size_t      theNextNameStart       = inReq.mBlockIdx;
    const RequestId   theReqId               = GetRequestId(inReq);
    const int         theBlockSize           = mBlockSize;
    const bool        theCreateExclusiveFlag = mCreateExclusiveFlag;
    QCASSERT(theNamePtr);
    QCStMutexUnlocker theUnlock(mMutex);

    Trace("process: meta", inReq);
    if (mIoStartObserverPtr) {
        mIoStartObserverPtr->Notify(
            inReq.mReqType,
            theReqId,
            inReq.mFileIdx,
            0,
            0
        );
    }

    if (mRequestProcessorsPtr) {
        inReq.mFreeBuffersIfNoIoCompletionFlag = false;
        mRequestProcessorsPtr[inThreadIdx]->StartMeta(
            inReq,
            inReq.mReqType,
            theNamePtr,
            kReqTypeRename == inReq.mReqType ?
                theNamePtr + theNextNameStart : 0
        );
        return;
    }
    BlockIdx theBlkIdx   = -1;
    int64_t  theRetCount = 0;
    int      theSysErr   = 0;
    Error    theError    = kErrorNone;
    switch (theReqType) {
        case kReqTypeDelete:
            if (unlink(theNamePtr)) {
                theSysErr = errno;
                theError  = kErrorDelete;
            }
            break;
        case kReqTypeRename:
            if (rename(theNamePtr, theNamePtr + theNextNameStart)) {
                theSysErr = errno;
                theError  = kErrorRename;
            }
            break;
        case kReqTypeGetFsAvailable: {
                int64_t theTotalCount = 0;
                if (GetFsAvailable(theNamePtr, theRetCount, theTotalCount)) {
                    theSysErr = errno;
                    theError  = kErrorGetFsAvailable;
                } else {
                    theBlkIdx = (BlockIdx)(theTotalCount / theBlockSize);
                }
            }
            break;
        case kReqTypeCheckDirReadable: {
                struct stat theStat = { 0 };
                if (stat(theNamePtr, &theStat)) {
                    theSysErr = errno;
                    theError  = kErrorCheckDirReadable;
                } else {
                    DIR* const theDirPtr = opendir(theNamePtr);
                    if (! theDirPtr) {
                        theSysErr = errno;
                        theError  = kErrorCheckDirReadable;
                    } else {
                        if (closedir(theDirPtr)) {
                            theSysErr = errno;
                            theError  = kErrorCheckDirReadable;
                        }
                    }
                }
            }
            break;
        case kReqTypeCheckDirWritable: {
                const char* thePtr = theNamePtr + theNextNameStart;
                const bool theBufferedIoFlag    = (*thePtr++ & 0xFF) != '0';
                const bool theAllocateSpaceFlag = (*thePtr++ & 0xFF) != '0';
                int64_t    theSize              = 0;
                int        theSym;
                while ((theSym = (*thePtr++ & 0xFF))) {
                    theSym -= '0';
                    QCASSERT((theSym & ~0xF) == 0);
                    theSize <<= 4;
                    theSize |= theSym & 0xF;
                }
                const int theOpenFlags =
                    O_RDWR | GetOpenCommonFlags(theBufferedIoFlag);
                const int theFd        = CreateFile(
                    theNamePtr, theOpenFlags, S_IRUSR | S_IWUSR,
                    theCreateExclusiveFlag);
                if (theFd < 0) {
                    theSysErr = errno;
                    theError  = kErrorCheckDirWritable;
                } else {
                    if (0 < theSize) {
                        if (theAllocateSpaceFlag) {
                            const int64_t theResv = QCUtils::ReserveFileSpace(
                                theFd, theSize);
                            if (theResv < 0) {
                                theError  = kErrorCheckDirWritable;
                                theSysErr = int(-theResv);
                            }
                        }
                        if (theError == kErrorNone) {
                            char* const theBufPtr = mBufferPoolPtr->Get();
                            if (theBufPtr) {
                                memset(theBufPtr, 0xF9, mBlockSize);
                                int64_t theIoBytes = 0;
                                while (theIoBytes < theSize) {
                                    int theIoVecCnt = 0;
                                    while (theIoBytes < theSize &&
                                            theIoVecCnt <
                                                mIoVecPerThreadCount) {
                                        inIoVecPtr[theIoVecCnt  ].iov_base =
                                            theBufPtr;
                                        inIoVecPtr[theIoVecCnt++].iov_len  =
                                            mBlockSize;
                                        theIoBytes += mBlockSize;
                                    }
                                    const ssize_t theNWr = writev(
                                        theFd, inIoVecPtr, theIoVecCnt);
                                    if (theNWr !=
                                            (ssize_t)theIoVecCnt * mBlockSize) {
                                        theSysErr = errno;
                                        theError  = kErrorCheckDirWritable;
                                        break;
                                    }
                                }
                                mBufferPoolPtr->Put(theBufPtr);
                            }
                            // Out of buffers silently ignored for now.
                        }
                    }
                    if (close(theFd) && theError == kErrorNone) {
                        theSysErr = errno;
                        theError  = kErrorCheckDirWritable;
                    }
                    if (unlink(theNamePtr) && theError == kErrorNone) {
                        theSysErr = errno;
                        theError  = kErrorCheckDirWritable;
                    }
                }
            }
            break;
        default:
            theSysErr = EINVAL;
            break;
    }

    theUnlock.Lock();
    RequestComplete(inReq, theError, theSysErr, theRetCount, false, theBlkIdx);
}

    QCDiskQueue::OpenFileStatus
QCDiskQueue::Queue::OpenFile(
    const char* inFileNamePtr,
    int64_t     inMaxFileSize,
    bool        inReadOnlyFlag,
    bool        inAllocateFileSpaceFlag,
    bool        inCreateFlag,
    bool        inBufferedIoFlag)
{
    if (! inFileNamePtr || ! *inFileNamePtr) {
        return OpenFileStatus(-1, kErrorParameter, EINVAL);
    }

    QCStMutexLocker theLocker(mMutex);
    if (! mRunFlag) {
        return OpenFileStatus(-1, kErrorQueueStopped, 0);
    }
    const int64_t theBlkIdx = (int64_t(inMaxFileSize < 0 ? 0 : inMaxFileSize) +
        mBlockSize - 1) / mBlockSize;
    if (theBlkIdx >= (int64_t(1) << kBlockBitCount)) {
        return OpenFileStatus(-1, kErrorParameter, EOVERFLOW);
    }
    if (mFreeFdHead == kFreeFdEnd) {
        return OpenFileStatus(-1, kErrorFileIdxOutOfRange, 0);
    }
    Request* const theReqPtr = Get(1);
    if (! theReqPtr) {
        return OpenFileStatus(-1, kErrorOutOfRequests, 0);
    }
    const int theIdx = -mFreeFdHead - kFreeFdOffset;
    QCRTASSERT(
        theIdx >= 0 && theIdx < mFileCount - 1 && mFdPtr[theIdx] <= kFreeFdEnd);
    mFreeFdHead = mFdPtr[theIdx];
    for (int i = theIdx; i < mFdCount; i += mFileCount) {
        mFdPtr[i] = kOpenPendingFd;
    }
    if (! mRequestAffinityFlag || mThreadCount <= mNextThreadIdx) {
        mNextThreadIdx = 0;
    }
    mFilePendingReqCountPtr[theIdx] = 0;
    mFileInfoPtr[theIdx].mOpenError             = kOpenErrorNone;
    mFileInfoPtr[theIdx].mOpenPendingFlag       = true;
    mFileInfoPtr[theIdx].mClosedFlag            = false;
    mFileInfoPtr[theIdx].mLastBlockIdx          = theBlkIdx;
    mFileInfoPtr[theIdx].mCloseFileSize         = -1;
    mFileInfoPtr[theIdx].mSpaceAllocPendingFlag = inAllocateFileSpaceFlag &&
        ! inReadOnlyFlag && inMaxFileSize > 0;
    mFileInfoPtr[theIdx].mBufferedIoFlag        = inBufferedIoFlag;
    mFileInfoPtr[theIdx].mThreadIdx             = mNextThreadIdx++;

    Request& theReq = *theReqPtr;
    const size_t theFileNameLen = strlen(inFileNamePtr) + 1;
    char* const  theFileNamePtr = new char[theFileNameLen];
    memcpy(theFileNamePtr, inFileNamePtr, theFileNameLen);
    GetBuffersPtr(theReq)[0] = theFileNamePtr;
    theReq.mReqType         = inCreateFlag ?
        (inReadOnlyFlag ? kReqTypeCreateRO : kReqTypeCreate) :
        (inReadOnlyFlag ? kReqTypeOpenRO   : kReqTypeOpen)
    ;
    theReq.mBufferCount     = 0;
    theReq.mFileIdx         = theIdx;
    theReq.mBlockIdx        = theBlkIdx;
    theReq.mIoCompletionPtr = 0;
    const int theThreadIdx = mFileInfoPtr[theIdx].mThreadIdx;
    Enqueue(theReq, theThreadIdx);
    Notify(theThreadIdx);
    return OpenFileStatus(theIdx, kErrorNone, 0);
}

    QCDiskQueue::CloseFileStatus
QCDiskQueue::Queue::CloseFile(
    QCDiskQueue::FileIdx inFileIdx,
    int64_t              inFileSize)
{
    QCStMutexLocker theLocker(mMutex);
    if (inFileIdx < 0 || inFileIdx >= mFileCount || mFdPtr[inFileIdx] < 0) {
        return CloseFileStatus(kErrorFileIdxOutOfRange, 0);
    }
    if (mFileInfoPtr[inFileIdx].mClosedFlag) {
        return CloseFileStatus(kErrorClose, 0);
    }
    mFileInfoPtr[inFileIdx].mClosedFlag    = true;
    mFileInfoPtr[inFileIdx].mCloseFileSize = inFileSize;
    OpenError theOpenError;
    if (mFilePendingReqCountPtr[inFileIdx] <= 0 &&
            (theOpenError = mFileInfoPtr[inFileIdx].mOpenError) !=
                kOpenErrorNone) {
        mFdPtr[inFileIdx] = mFreeFdHead;
        mFreeFdHead = -(inFileIdx + kFreeFdOffset);
        return CloseFileStatus(kErrorOpen, Open2SysError(theOpenError));
    }
    if (mFilePendingReqCountPtr[inFileIdx] <= 0) {
        const int theThreadIdx = mFileInfoPtr[inFileIdx].mThreadIdx;
        ScheduleClose(inFileIdx, theThreadIdx);
        Notify(theThreadIdx);
    }
    // Else if request are pending the last request will perform close.
    return CloseFileStatus(kErrorNone, 0);
}

    void
QCDiskQueue::Queue::CloseAllFiles()
{
    QCStMutexLocker theLocker(mMutex);
    int theLastThreadIdx = -1;
    for (int i = 0; i < mFileCount; i++) {
        if (mFdPtr[i] < 0 || mFileInfoPtr[i].mClosedFlag) {
            continue;
        }
        const int theThreadIdx = mFileInfoPtr[i].mThreadIdx;
        mFileInfoPtr[i].mClosedFlag = true;
        // mFileInfoPtr[i].mCloseFileSize = -1;
        if (mFilePendingReqCountPtr[i] <= 0 &&
                mFileInfoPtr[i].mOpenError != kOpenErrorNone) {
            mFdPtr[i] = mFreeFdHead;
            mFreeFdHead = -(i + kFreeFdOffset);
            continue;
        }
        if (mFilePendingReqCountPtr[i] <= 0) {
            ScheduleClose(i, theThreadIdx);
            if (theThreadIdx == theLastThreadIdx) {
                continue;
            }
            Notify(theThreadIdx);
            theLastThreadIdx = theThreadIdx;
        }
    }
}

    QCDiskQueue::EnqueueStatus
QCDiskQueue::Queue::CheckOpenStatus(
    QCDiskQueue::FileIdx       inFileIdx,
    QCDiskQueue::IoCompletion* inIoCompletionPtr,
    QCDiskQueue::Time          inTimeWaitNanoSec)
{
    QCStMutexLocker theLocker(mMutex);
    if (! mRunFlag) {
        return EnqueueStatus(kRequestIdNone, kErrorQueueStopped);
    }
    if (inFileIdx < 0 || inFileIdx >= mFileCount || mFdPtr[inFileIdx] < 0) {
        return EnqueueStatus(kRequestIdNone, kErrorFileIdxOutOfRange);
    }
    Request* const theReqPtr = GetRequest(theLocker, 0, inTimeWaitNanoSec);
    if (! theReqPtr) {
        return EnqueueStatus(kRequestIdNone, kErrorOutOfRequests);
    }
    // Queue empty read request.
    Request& theReq = *theReqPtr;
    theReq.mReqType         = kReqTypeRead;
    theReq.mBufferCount     = 0;
    theReq.mFileIdx         = inFileIdx;
    theReq.mBlockIdx        = 0;
    theReq.mIoCompletionPtr = inIoCompletionPtr;
    GetBuffersPtr(theReq)[0] = 0;
    const int theThreadIdx = mFileInfoPtr[inFileIdx].mThreadIdx;
    Enqueue(theReq, theThreadIdx);
    Notify(theThreadIdx);
    return EnqueueStatus(GetRequestId(theReq), kErrorNone);
}

    QCDiskQueue::Status
QCDiskQueue::Queue::AllocateFileSpace(
    QCDiskQueue::FileIdx inFileIdx)
{
    QCStMutexLocker theLocker(mMutex);
    if (! mRunFlag) {
        return Status(kErrorQueueStopped);
    }
    if (inFileIdx < 0 || inFileIdx >= mFileCount || mFdPtr[inFileIdx] < 0) {
        return Status(kErrorFileIdxOutOfRange);
    }
    mFileInfoPtr[inFileIdx].mSpaceAllocPendingFlag = true;
    return Status(kErrorNone);
}

    QCDiskQueue::EnqueueStatus
QCDiskQueue::Queue::Rename(
        const char*                inSrcFileNamePtr,
        const char*                inDstFileNamePtr,
        QCDiskQueue::IoCompletion* inIoCompletionPtr,
        QCDiskQueue::Time          inTimeWaitNanoSec /* = -1 */)
{
    if (! inSrcFileNamePtr || ! *inSrcFileNamePtr ||
            ! inDstFileNamePtr || ! *inDstFileNamePtr) {
        return EnqueueStatus(kRequestIdNone, kErrorParameter);
    }
    return EnqueueMeta(
        kReqTypeRename,
        inSrcFileNamePtr,
        inDstFileNamePtr,
        inIoCompletionPtr,
        inTimeWaitNanoSec
    );
}

    QCDiskQueue::EnqueueStatus
QCDiskQueue::Queue::Delete(
    const char*                inFileNamePtr,
    QCDiskQueue::IoCompletion* inIoCompletionPtr,
    QCDiskQueue::Time          inTimeWaitNanoSec)
{
    if (! inFileNamePtr || ! *inFileNamePtr) {
        return EnqueueStatus(kRequestIdNone, kErrorParameter);
    }
    return EnqueueMeta(
        kReqTypeDelete,
        inFileNamePtr,
        0,
        inIoCompletionPtr,
        inTimeWaitNanoSec
    );
}

    QCDiskQueue::EnqueueStatus
QCDiskQueue::Queue::GetFsSpaceAvailable(
    const char*                inPathNamePtr,
    QCDiskQueue::IoCompletion* inIoCompletionPtr,
    QCDiskQueue::Time          inTimeWaitNanoSec)
{
    if (! inPathNamePtr || ! *inPathNamePtr) {
        return EnqueueStatus(kRequestIdNone, kErrorParameter);
    }
    return EnqueueMeta(
        kReqTypeGetFsAvailable,
        inPathNamePtr,
        0,
        inIoCompletionPtr,
        inTimeWaitNanoSec
    );
}

    QCDiskQueue::EnqueueStatus
QCDiskQueue::Queue::CheckDirReadable(
    const char*                inPathNamePtr,
    QCDiskQueue::IoCompletion* inIoCompletionPtr,
    QCDiskQueue::Time          inTimeWaitNanoSec)
{
    if (! inPathNamePtr || ! *inPathNamePtr) {
        return EnqueueStatus(kRequestIdNone, kErrorParameter);
    }
    return EnqueueMeta(
        kReqTypeCheckDirReadable,
        inPathNamePtr,
        0,
        inIoCompletionPtr,
        inTimeWaitNanoSec
    );
}

    QCDiskQueue::EnqueueStatus
QCDiskQueue::Queue::CheckDirWritable(
    const char*                inTestFileNamePtr,
    bool                       inBufferedIoFlag,
    bool                       inAllocSpaceFlag,
    int64_t                    inWriteSize,
    QCDiskQueue::IoCompletion* inIoCompletionPtr,
    QCDiskQueue::Time          inTimeWaitNanoSec)
{
    if (! inTestFileNamePtr || ! *inTestFileNamePtr) {
        return EnqueueStatus(kRequestIdNone, kErrorParameter);
    }
    char  theParams[1 + sizeof(inWriteSize) * 2 + 2];
    char* thePtr = theParams + sizeof(theParams) / sizeof(theParams[0]);
    *--thePtr = 0;
    if (inWriteSize <= 0) {
        *--thePtr = (char)'0';
    } else {
        int64_t theVal = inWriteSize;
        while (0 < theVal) {
            *--thePtr = (char)('0' + (theVal & 0xF));
            theVal >>= 4;
        }
    }
    *--thePtr = (char)(inBufferedIoFlag ? '1' : '0');
    *--thePtr = (char)(inAllocSpaceFlag ? '1' : '0');
    return EnqueueMeta(
        kReqTypeCheckDirWritable,
        inTestFileNamePtr,
        thePtr,
        inIoCompletionPtr,
        inTimeWaitNanoSec
    );
}

    QCDiskQueue::EnqueueStatus
QCDiskQueue::Queue::EnqueueMeta(
    QCDiskQueue::ReqType       inReqType,
    const char*                inFileName1Ptr,
    const char*                inFileName2Ptr,
    QCDiskQueue::IoCompletion* inIoCompletionPtr,
    QCDiskQueue::Time          inTimeWaitNanoSec)
{
    if (! IsMetaReqType(inReqType)) {
        return EnqueueStatus(kRequestIdNone, kErrorParameter);
    }
    QCStMutexLocker theLocker(mMutex);
    if (! mRunFlag) {
        return EnqueueStatus(kRequestIdNone, kErrorQueueStopped);
    }
    Request* const theReqPtr = GetRequest(theLocker, 1, inTimeWaitNanoSec);
    if (! theReqPtr) {
        return EnqueueStatus(kRequestIdNone, kErrorOutOfRequests);
    }
    const size_t theFileName1Len =
        inFileName1Ptr ? strlen(inFileName1Ptr) + 1 : 0;
    const size_t theFileName2Len =
        inFileName2Ptr ? strlen(inFileName2Ptr) + 1 : 0;
    char* const  theFileNamesPtr = (theFileName1Len + theFileName2Len > 0) ?
        new char[theFileName1Len + theFileName2Len] : 0;
    if (theFileNamesPtr) {
        if (inFileName1Ptr) {
            memcpy(theFileNamesPtr, inFileName1Ptr, theFileName1Len);
        }
        if (inFileName2Ptr) {
            memcpy(theFileNamesPtr + theFileName1Len, inFileName2Ptr,
                theFileName2Len);
        }
    }
    if (! mRequestAffinityFlag || mThreadCount <= mNextThreadIdx) {
        mNextThreadIdx = 0;
    }
    Request& theReq = *theReqPtr;
    theReq.mReqType          = inReqType;
    theReq.mBufferCount      = 0;
    theReq.mFileIdx          = mFileCount - 1;
    theReq.mBlockIdx         = inFileName2Ptr ? theFileName1Len : 0;
    theReq.mIoCompletionPtr  = inIoCompletionPtr;
    GetBuffersPtr(theReq)[0] = theFileNamesPtr;
    const int theThreadIdx = mNextThreadIdx++;
    Enqueue(theReq, theThreadIdx);
    Notify(theThreadIdx);
    return EnqueueStatus(GetRequestId(theReq), kErrorNone);
}

class QCDiskQueue::RequestWaiter : public QCDiskQueue::IoCompletion
{
public:
    typedef QCDiskQueue::CompletionStatus CompletionStatus;

    RequestWaiter(
        OutputIterator* inOutIteratorPtr)
        : mMutex(),
          mDoneCond(),
          mOutIteratorPtr(inOutIteratorPtr),
          mCompletionStatus(kErrorEnqueue),
          mDoneFlag(false)
        {}
    virtual ~RequestWaiter()
    {
        if (! mDoneFlag) {
            RequestWaiter::Wait();
        }
    }
    virtual bool Done(
        RequestId      inRequestId,
        FileIdx        inFileIdx,
        BlockIdx       inStartBlockIdx,
        InputIterator& inBufferItr,
        int            inBufferCount,
        Error          inCompletionCode,
        int            inSysErrorCode,
        int64_t        inIoByteCount)
    {
        QCStMutexLocker theLocker(mMutex);
        mDoneFlag = true;
        mCompletionStatus =
            CompletionStatus(inCompletionCode, inSysErrorCode, inIoByteCount);
        CopyBufs(&inBufferItr, inBufferCount);
        mDoneCond.Notify();
        return true;
    }
    CompletionStatus Wait(
        EnqueueStatus  inStatus,
        InputIterator* inBufferItrPtr,
        int            inBufferCount)
    {
        if (inStatus.IsError()) {
            mDoneFlag = true;
            mCompletionStatus = CompletionStatus(inStatus.GetError());
            CopyBufs(inBufferItrPtr, inBufferCount);
            return mCompletionStatus;
        }
        return Wait();
    }
private:
    QCMutex               mMutex;
    QCCondVar             mDoneCond;
    OutputIterator* const mOutIteratorPtr;
    CompletionStatus      mCompletionStatus;
    bool                  mDoneFlag;

    CompletionStatus Wait()
    {
        QCStMutexLocker theLocker(mMutex);
        while (! mDoneFlag) {
            mDoneCond.Wait(mMutex);
        }
        return mCompletionStatus;
    }
    void CopyBufs(
        InputIterator* inBufferItrPtr,
        int            inBufferCount)
    {
        if (! mOutIteratorPtr || ! inBufferItrPtr) {
            return;
        }
        for (int i = 0; i < inBufferCount; i++) {
            char* const theBufPtr = inBufferItrPtr->Get();
            if (! theBufPtr) {
                break;
            }
            mOutIteratorPtr->Put(theBufPtr);
        }
    }
};

    /* static */ const char*
QCDiskQueue::ToString(
    QCDiskQueue::Error inErrorCode)
{
    switch (inErrorCode)
    {
        case kErrorNone:                 return "none";
        case kErrorRead:                 return "read";
        case kErrorWrite:                return "write";
        case kErrorCancel:               return "io cancelled";
        case kErrorSeek:                 return "seek";
        case kErrorEnqueue:              return "enqueue";
        case kErrorOutOfBuffers:         return "out of io buffers";
        case kErrorParameter:            return "invalid parameter";
        case kErrorQueueStopped:         return "queue stopped";
        case kErrorFileIdxOutOfRange:    return "file index out of range";
        case kErrorBlockIdxOutOfRange:   return "block index out of range";
        case kErrorBlockCountOutOfRange: return "block count out of range";
        case kErrorOutOfRequests:        return "out of requests";
        case kErrorOpen:                 return "open";
        case kErrorClose:                return "close";
        case kErrorHasPendingRequests:   return "has pending requests";
        case kErrorSpaceAlloc:           return "space allocation";
        case kErrorDelete:               return "delete";
        case kErrorRename:               return "rename";
        case kErrorGetFsAvailable:       return "get fs available";
        case kErrorCheckDirReadable:     return "dir readable";
        case kErrorCheckDirWritable:     return "dir writable";
        default:                         return "invalid error code";
    }
}

QCDiskQueue::QCDiskQueue()
    : mQueuePtr(0)
{
}

QCDiskQueue::~QCDiskQueue()
{
    QCDiskQueue::Stop();
}

    void
QCDiskQueue::Done(
    QCDiskQueue::RequestProcessor& inProcessor,
    QCDiskQueue::Request&          inReq,
    QCDiskQueue::Error             inError,
    int                            inSysError,
    int64_t                        inIoByteCount,
    QCDiskQueue::BlockIdx          inBlockIdx,
    QCDiskQueue::InputIterator*    inInputIteratorPtr)
{
    QCASSERT(mQueuePtr);
    mQueuePtr->Done(
        inProcessor, inReq, inError, inSysError, inIoByteCount, inBlockIdx,
        inInputIteratorPtr);
}

    int
QCDiskQueue::Start(
    int                             inThreadCount,
    int                             inMaxQueueDepth,
    int                             inMaxBuffersPerRequestCount,
    int                             inFileCount,
    const char**                    inFileNamesPtr,
    QCIoBufferPool&                 inBufferPool,
    QCDiskQueue::IoStartObserver*   inIoStartObserverPtr        /* = 0 */,
    QCDiskQueue::CpuAffinity        inCpuAffinity /* = CpuAffinity::None() */,
    QCDiskQueue::DebugTracer*       inDebugTracerPtr            /* = 0 */,
    bool                            inBufferedIoFlag            /* = false */,
    bool                            inCreateExclusiveFlag       /* = true  */,
    bool                            inRequestAffinityFlag       /* = false */,
    bool                            inSerializeMetaRequestsFlag /* = true  */,
    QCDiskQueue::RequestProcessor** inRequestProcessorsPtr      /* = 0 */)
{
    Stop();
    mQueuePtr = new Queue();
    const int theRet = mQueuePtr->Start(
        inThreadCount,
        inMaxQueueDepth,
        inMaxBuffersPerRequestCount,
        inFileCount,
        inFileNamesPtr,
        inBufferPool,
        inIoStartObserverPtr,
        inCpuAffinity,
        inDebugTracerPtr,
        inBufferedIoFlag,
        inCreateExclusiveFlag,
        inRequestAffinityFlag,
        inSerializeMetaRequestsFlag,
        inRequestProcessorsPtr
    );
    if (theRet != 0) {
        Stop();
    }
    return theRet;
}

    void
QCDiskQueue::Stop()
{
    if (! mQueuePtr) {
        return;
    }
    Queue* const theQueuePtr = mQueuePtr;
    mQueuePtr = 0;
    theQueuePtr->Stop();
    delete theQueuePtr;
}

    QCDiskQueue::EnqueueStatus
QCDiskQueue::Enqueue(
    QCDiskQueue::ReqType        inReqType,
    QCDiskQueue::FileIdx        inFileIdx,
    QCDiskQueue::BlockIdx       inStartBlockIdx,
    QCDiskQueue::InputIterator* inBufferIteratorPtr,
    int                         inBufferCount,
    QCDiskQueue::IoCompletion*  inIoCompletionPtr,
    QCDiskQueue::Time           inTimeWaitNanoSec,
    int64_t                     inEofHint)
{
    if (! mQueuePtr) {
        return EnqueueStatus(kRequestIdNone, kErrorParameter);
    }
    return mQueuePtr->Enqueue(
        inReqType,
        inFileIdx,
        inStartBlockIdx,
        inBufferIteratorPtr,
        inBufferCount,
        inIoCompletionPtr,
        inTimeWaitNanoSec,
        inEofHint);
}

    bool
QCDiskQueue::Cancel(
    QCDiskQueue::RequestId inRequestId)
{
    return (mQueuePtr && mQueuePtr->Cancel(inRequestId));
}

    QCDiskQueue::IoCompletion*
QCDiskQueue::CancelOrSetCompletionIfInFlight(
    QCDiskQueue::RequestId     inRequestId,
    QCDiskQueue::IoCompletion* inCompletionIfInFlightPtr)
{
    return (mQueuePtr ? mQueuePtr->CancelOrSetCompletionIfInFlight(
        inRequestId, inCompletionIfInFlightPtr) : 0);
}

    void
QCDiskQueue::GetPendingCount(
    int&     outFreeRequestCount,
    int&     outRequestCount,
    int64_t& outReadBlockCount,
    int64_t& outWriteBlockCount)
{
    if (mQueuePtr) {
        mQueuePtr->GetPendingCount(outFreeRequestCount,
            outRequestCount, outReadBlockCount, outWriteBlockCount);
    } else {
        outFreeRequestCount = 0;
        outRequestCount     = 0;
        outReadBlockCount   = 0;
        outWriteBlockCount  = 0;
    }
}

    QCDiskQueue::CompletionStatus
QCDiskQueue::SyncIo(
    QCDiskQueue::ReqType         inReqType,
    QCDiskQueue::FileIdx         inFileIdx,
    QCDiskQueue::BlockIdx        inStartBlockIdx,
    QCDiskQueue::InputIterator*  inBufferIteratorPtr,
    int                          inBufferCount,
    QCDiskQueue::OutputIterator* inOutBufferIteratroPtr)
{
    if (inBufferCount <= 0) {
        return CompletionStatus();
    }
    if (! inBufferIteratorPtr && ! inOutBufferIteratroPtr) {
        return CompletionStatus(kErrorEnqueue);
    }
    RequestWaiter theWaiter(inOutBufferIteratroPtr);
    return theWaiter.Wait(
        Enqueue(inReqType,
                inFileIdx,
                inStartBlockIdx,
                inBufferIteratorPtr,
                inBufferCount,
                &theWaiter),
        inBufferIteratorPtr,
        inBufferCount
    );
}

    QCDiskQueue::OpenFileStatus
QCDiskQueue::OpenFile(
    const char* inFileNamePtr,
    int64_t     inMaxFileSize           /* = -1 */,
    bool        inReadOnlyFlag          /* = false */,
    bool        inAllocateFileSpaceFlag /* = false */,
    bool        inCreateFlag            /* false */,
    bool        inBufferedIoFlag        /* false */)
{
    return ((mQueuePtr && inFileNamePtr) ?
        mQueuePtr->OpenFile(inFileNamePtr, inMaxFileSize,
            inReadOnlyFlag, inAllocateFileSpaceFlag, inCreateFlag,
            inBufferedIoFlag) :
        OpenFileStatus(-1, kErrorParameter, 0)
    );
}

    QCDiskQueue::CloseFileStatus
QCDiskQueue::CloseFile(
    QCDiskQueue::FileIdx inFileIdx,
    int64_t              inFileSize /* = -1 */)
{
    return (mQueuePtr ?
        mQueuePtr->CloseFile(inFileIdx, inFileSize) :
        CloseFileStatus(kErrorParameter, 0)
    );
}

    void
QCDiskQueue::CloseAllFiles()
{
    if (mQueuePtr) {
        mQueuePtr->CloseAllFiles();
    }
}

    QCDiskQueue::EnqueueStatus
QCDiskQueue::CheckOpenStatus(
    QCDiskQueue::FileIdx       inFileIdx,
    QCDiskQueue::IoCompletion* inIoCompletionPtr,
    QCDiskQueue::Time          inTimeWaitNanoSec /* = -1 */)
{
    return (mQueuePtr ?
        mQueuePtr->CheckOpenStatus(
            inFileIdx, inIoCompletionPtr, inTimeWaitNanoSec) :
        EnqueueStatus(kRequestIdNone, kErrorParameter)
    );
}

    QCDiskQueue::EnqueueStatus
QCDiskQueue::Rename(
    const char*                inSrcFileNamePtr,
    const char*                inDstFileNamePtr,
    QCDiskQueue::IoCompletion* inIoCompletionPtr,
    QCDiskQueue::Time          inTimeWaitNanoSec /* = -1 */)
{
    return (mQueuePtr ?
        mQueuePtr->Rename(inSrcFileNamePtr, inDstFileNamePtr,
            inIoCompletionPtr, inTimeWaitNanoSec) :
        EnqueueStatus(kRequestIdNone, kErrorParameter)
    );
}

    QCDiskQueue::EnqueueStatus
QCDiskQueue::Delete(
    const char*                inFileNamePtr,
    QCDiskQueue::IoCompletion* inIoCompletionPtr,
    QCDiskQueue::Time          inTimeWaitNanoSec /* = -1 */)
{
    return (mQueuePtr ?
        mQueuePtr->Delete(inFileNamePtr, inIoCompletionPtr, inTimeWaitNanoSec) :
        EnqueueStatus(kRequestIdNone, kErrorParameter)
    );
}

    QCDiskQueue::EnqueueStatus
QCDiskQueue::GetFsSpaceAvailable(
    const char*                inPathNamePtr,
    QCDiskQueue::IoCompletion* inIoCompletionPtr,
    QCDiskQueue::Time          inTimeWaitNanoSec /* = -1 */)
{
    return (mQueuePtr ?
        mQueuePtr->GetFsSpaceAvailable(inPathNamePtr, inIoCompletionPtr,
            inTimeWaitNanoSec) :
        EnqueueStatus(kRequestIdNone, kErrorParameter)
    );
}

    QCDiskQueue::EnqueueStatus
QCDiskQueue::CheckDirReadable(
    const char*                inDirNamePtr,
    QCDiskQueue::IoCompletion* inIoCompletionPtr,
    QCDiskQueue::Time          inTimeWaitNanoSec /* = -1 */)
{
    return (mQueuePtr ?
        mQueuePtr->CheckDirReadable(inDirNamePtr, inIoCompletionPtr,
            inTimeWaitNanoSec) :
        EnqueueStatus(kRequestIdNone, kErrorParameter)
    );
}

    QCDiskQueue::EnqueueStatus
QCDiskQueue::CheckDirWritable(
    const char*                inTestFileNamePtr,
    bool                       inBufferedIoFlag,
    bool                       inAllocSpaceFlag,
    int64_t                    inWriteSize,
    QCDiskQueue::IoCompletion* inIoCompletionPtr,
    QCDiskQueue::Time          inTimeWaitNanoSec /* = -1 */)
{
    return (mQueuePtr ?
        mQueuePtr->CheckDirWritable(inTestFileNamePtr, inBufferedIoFlag,
            inAllocSpaceFlag, inWriteSize,
            inIoCompletionPtr, inTimeWaitNanoSec) :
        EnqueueStatus(kRequestIdNone, kErrorParameter)
    );
}

    int
QCDiskQueue::GetBlockSize() const
{
    return (mQueuePtr ? mQueuePtr->GetBlockSize() : 0);
}

    QCDiskQueue::Status
QCDiskQueue::AllocateFileSpace(
    QCDiskQueue::FileIdx inFileIdx)
{
    return (mQueuePtr ?
        mQueuePtr->AllocateFileSpace(inFileIdx) :
        Status(kErrorParameter)
    );
}
