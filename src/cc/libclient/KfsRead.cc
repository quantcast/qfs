//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2010/09/08
// Author: Mike Ovsiannikov
//
// Copyright 2008-2012 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
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
// Kfs client blocking read and read ahead implementation.
//----------------------------------------------------------------------------

#include "KfsClientInt.h"
#include "KfsProtocolWorker.h"
#include "common/MsgLogger.h"
#include "qcdio/qcstutils.h"
#include "qcdio/QCDLList.h"
#include "qcdio/qcdebug.h"
#include "qcdio/QCUtils.h"

#include <cerrno>
#include <string>
#include <limits>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace KFS
{
namespace client
{

using std::string;
using std::max;
using std::min;
using std::numeric_limits;

// Blocking read conditional variables with free/unused list "next" pointer.
class ReadRequestCondVar : public QCCondVar
{
public:
    ReadRequestCondVar()
        : QCCondVar(),
          mNextPtr(0)
        {}
    ReadRequestCondVar* mNextPtr;
};

// Non-blocking read request with blocking completion "wait" used to implement
// blocking read.
class ReadRequest : public KfsProtocolWorker::Request
{
public:
    static int64_t GetEof(
        const FileTableEntry& inEntry)
    {
        return (inEntry.eofMark < 0 ?
            inEntry.fattr.fileSize :
            min(inEntry.eofMark, inEntry.fattr.fileSize)
        );
    }
    static int MaxRequestSize(
        const FileTableEntry& inEntry,
        int                   inSize,
        int64_t               inOffset)
    {
        const int64_t kChunkSize = (int64_t)CHUNKSIZE;
        return (int)max(int64_t(0), min(
            GetEof(inEntry) - inOffset,
            inEntry.skipHoles ?
                min(int64_t(inSize), kChunkSize - inOffset % kChunkSize) :
                int64_t(inSize)
        ));
    }
    static ReadRequest* Create(
        QCMutex&        inMutex,
        FileTableEntry& inEntry,
        void*           inBufPtr,
        int             inSize,
        int64_t         inOffset,
        int             inMsgLogId)
    {
        const int theSize = MaxRequestSize(inEntry, inSize, inOffset);
        if (theSize <= 0) {
            return 0;
        }
        ReadRequest& theReq = *(new ReadRequest(inMutex));
        if (theReq.Init(
                inEntry, inBufPtr, theSize, inOffset, inMsgLogId) <= 0) {
            delete &theReq;
            return 0;
        }
        return &theReq;
    }
    virtual void Done(
        int64_t inStatus)
    {
        QCStMutexLocker theLocker(mMutex);
        QCASSERT(! mDoneFlag && (mCondVarPtr || mWaitingCount == 0));
        mDoneFlag = true;
        mStatus   = inStatus;
        if (mCondVarPtr) {
            mCondVarPtr->Notify();
        }
        if (mCanceledFlag && mWaitingCount == 0) {
            theLocker.Unlock();
            delete this;
        }
    }
    int64_t Wait(
        QCMutex&             inClientMutex,
        ReadRequestCondVar*& ioFreeCondVarsHeadPtr,
        FileTableEntry&      inEntry)
    {
        QCASSERT(inClientMutex.IsOwned() && &inClientMutex != &mMutex);
        QCStMutexLocker theLocker(mMutex);
        if (++mWaitingCount <= 1 && ! mDoneFlag) {
            QCRTASSERT(! mCondVarPtr);
            if (ioFreeCondVarsHeadPtr) {
                mCondVarPtr = ioFreeCondVarsHeadPtr;
                ioFreeCondVarsHeadPtr = mCondVarPtr->mNextPtr;
                mCondVarPtr->mNextPtr = 0;
            } else {
                mCondVarPtr = new ReadRequestCondVar();
            }
        }
        if (! mDoneFlag) {
            QCStMutexUnlocker theUnlockerClient(inClientMutex);
            QCASSERT(! inClientMutex.IsOwned());
            while (! mDoneFlag) {
                QCASSERT(mCondVarPtr);
                mCondVarPtr->Wait(mMutex);
            }
            // Release the request completion mutex and re-acquire client mutex,
            // to maintain the lock acquisition ordering in order to avoid dead
            // lock.
            // Note that there is no race between mWaitingCount decrement below
            // and the mWaitingCount == 0 condition evaluation in Done(), as the
            // later is only invoked once, and must already be invoked by the
            // time the decrement performed, therefore the decrement can be done
            // without re-acquiring the completion mutex.
            theLocker.Unlock();
        }
        const int64_t theStatus = mCanceledFlag ? -ECANCELED : mStatus;
        // Do not access inEntry if request was canceled. inEntry might not be
        // valid in the case if one thread waits, while the other closes
        // the fd.
        if (! mCanceledFlag && inEntry.buffer.mReadReq == this) {
            inEntry.buffer.mStatus =
                (theStatus == -ENOENT && inEntry.skipHoles) ?
                0 : (int)theStatus;
        }
        QCASSERT(mWaitingCount > 0);
        if (--mWaitingCount > 0) {
            QCASSERT(mCondVarPtr);
            // Wake up the next thread waiting the request completion.
            // The mutex here must already be released, by the request
            // completion wait logic the above. Re-acquire the mutex in order
            // for Notify() to work correctly.
            QCStMutexLocker theLocker(mMutex);
            mCondVarPtr->Notify();
        } else {
            if (mCondVarPtr) {
                mCondVarPtr->mNextPtr = ioFreeCondVarsHeadPtr;
                ioFreeCondVarsHeadPtr = mCondVarPtr;
                mCondVarPtr = 0;
            }
            if (! mCanceledFlag) {
                Queue::Remove(inEntry.mReadQueue, *this);
                if (inEntry.buffer.mReadReq == this) {
                    inEntry.buffer.mReadReq = 0;
                }
            }
            theLocker.Unlock();
            delete this;
        }
        return theStatus;
    }
    void Cancel(
        FileTableEntry& inEntry)
    {
        QCStMutexLocker theLocker(mMutex);
        Queue::Remove(inEntry.mReadQueue, *this);
        if (inEntry.buffer.mReadReq == this) {
            if (! mDoneFlag) {
                mBufToDeletePtr = inEntry.buffer.DetachBuffer();
            }
            // Even if request is done, mark it as canceled, to make any
            // threads waiting in GetReadAhead() unwind immediately
            // without accessing the file table entry.
            // The file table entry might be invalid when wait returns in the
            // case when one thread calls close while the other threads
            // are blocked in read.
            inEntry.buffer.mStatus = -ECANCELED;
            inEntry.buffer.mReadReq = 0;
        }
        mCanceledFlag = true;
        if (mWaitingCount == 0 && mDoneFlag) {
            theLocker.Unlock();
            delete this;
        }
    }
    static int64_t Wait(
        QCMutex&             inClientMutex,
        ReadRequestCondVar*& ioFreeCondVarsHeadPtr,
        FileTableEntry&      inEntry,
        int64_t              inOffset,
        int                  inSize)
    {
        Queue::Iterator theIt(inEntry.mReadQueue);
        const int64_t   theEndPos = inOffset + inSize;
        ReadRequest*    thePtr;
        while ((thePtr = theIt.Next())) {
            const int64_t theReqStart = thePtr->GetOffset();
            const int64_t theReqEnd   = theReqStart + thePtr->GetSize();
            if (theReqStart < theEndPos && inOffset < theReqEnd) {
                return thePtr->Wait(
                    inClientMutex, ioFreeCondVarsHeadPtr, inEntry);
            }
        }
        return 0;
    }
    static ReadRequest* Find(
        FileTableEntry& inEntry,
        void*           inBufPtr,
        int64_t         inSize,
        int64_t         inOffset)
    {
        QCASSERT(inSize >= 0);
        const char* const theLPtr = reinterpret_cast<const char*>(inBufPtr);
        const char* const theRPtr = theLPtr + inSize;
        Queue::Iterator   theIt(inEntry.mReadQueue);
        ReadRequest*      thePtr;
        while ((thePtr = theIt.Next())) {
            const char* const theBPtr =
                reinterpret_cast<const char*>(thePtr->GetBufferPtr());
            const char* const theEPtr = theBPtr + thePtr->GetSize();
            if (theLPtr < theEPtr && theBPtr <= theRPtr) {
                return thePtr;
            }
        }
        return 0;
    }
    static void CancelAll(
        FileTableEntry& inEntry)
    {
        while (! Queue::IsEmpty(inEntry.mReadQueue)) {
            Queue::Front(inEntry.mReadQueue)->Cancel(inEntry);
        }
        QCRTASSERT(! inEntry.buffer.mReadReq);
    }
    static void InitEntry(
        FileTableEntry& inEntry)
    {
        Queue::Init(inEntry.mReadQueue);
    }
    static int GetReadAhead(
        QCMutex&             inClientMutex,
        ReadRequestCondVar*& ioFreeCondVarsHeadPtr,
        FileTableEntry&      inEntry,
        void*                inBufPtr,
        int                  inSize,
        int64_t              inOffset,
        bool&                outShortReadFlag)
    {
        outShortReadFlag = false;
        if (inOffset < inEntry.buffer.mStart ||
                inEntry.buffer.mStatus < 0 ||
                inEntry.buffer.mSize <= 0 ||
                inEntry.buffer.mStart < 0 ||
                ! inEntry.buffer.mBuf ||
                inSize <= 0 ||
                inEntry.buffer.mStart + inEntry.buffer.mSize <= inOffset) {
            return 0;
        }
        if (inEntry.buffer.mReadReq) {
            const int64_t theRet = inEntry.buffer.mReadReq->Wait(
                inClientMutex, ioFreeCondVarsHeadPtr, inEntry);
            // The last thread leaving wait sets inEntry.buffer.mReadReq to 0,
            // this guarantees that read ahead buffer and result remains valid,
            // and corresponds to the read ahead request that was waited for.
            // All other fields of inEntry, can change.
            if (theRet <= 0) {
                // inEntry might be invalid -- it might not exist anymore.
                return (int)theRet;
            }
        }
        return CopyReadAhead(
            inEntry, inBufPtr, inSize, inOffset, outShortReadFlag);
    }
    static int GetReadAheadSize(
        FileTableEntry& inEntry,
        int64_t         inOffset)
    {
        const int theBufSize = inEntry.buffer.GetBufSize();
        if (theBufSize <= 0) {
            return 0;
        }
        int theSize = MaxRequestSize(inEntry, theBufSize, inOffset);
        if (theSize <= 0) {
            return 0;
        }
        char* const thePtr = inEntry.buffer.GetBufPtr();
        if (! thePtr) {
            return 0;
        }
        // SetReadAheadSize() sets "optimal" buffer size, try to align the
        // end of the request to this size, to make all subsequent requests
        // aligned.
        if (inOffset + theSize < GetEof(inEntry)) {
            const int theTail = (int)((inOffset + theSize) % theBufSize);
            if (theSize > theTail) {
                theSize -= theTail;
            }
        }
        return theSize;
    }
    static ReadRequest* InitReadAhead(
        QCMutex&             inMutex,
        FileTableEntry&      inEntry,
        int                  inMsgLogId,
        chunkOff_t           inPos)
    {
        if (inEntry.buffer.mReadReq ||
                inPos >= GetEof(inEntry) ||
                (inEntry.buffer.mSize > 0 &&
                 inEntry.buffer.mStatus > 0 &&
                 inPos < inEntry.buffer.mStart + inEntry.buffer.mStatus)) {
            return 0;
        }
        inEntry.buffer.mStatus = 0;
        inEntry.buffer.mSize   = 0;
        inEntry.buffer.mStart  = -1;
        const int64_t theOffset = inPos;
        int           theSize   = GetReadAheadSize(inEntry, theOffset);
        if (theSize <= 0) {
            return 0;
        }
        char* const thePtr = inEntry.buffer.GetBufPtr();
        QCASSERT(thePtr);
        ReadRequest& theReq = *(new ReadRequest(inMutex));
        if (theReq.Init(inEntry, thePtr, theSize, theOffset, inMsgLogId) <= 0) {
            delete &theReq;
            return 0;
        }
        inEntry.buffer.mStart   = theReq.GetOffset();
        inEntry.buffer.mSize    = theReq.GetSize();
        inEntry.buffer.mReadReq = &theReq;
        return &theReq;
    }
private:
    typedef QCDLList<ReadRequest, 0> Queue;

    Params              mOpenParams;
    QCMutex&            mMutex;
    ReadRequestCondVar* mCondVarPtr;
    int                 mWaitingCount;
    bool                mDoneFlag:1;
    bool                mCanceledFlag:1;
    char*               mBufToDeletePtr;
    int64_t             mStatus;
    ReadRequest*        mPrevPtr[1];
    ReadRequest*        mNextPtr[1];

    friend class QCDLListOp<ReadRequest,0>;

    ReadRequest(
        QCMutex& inMutex)
        : Request(),
          mOpenParams(),
          mMutex(inMutex),
          mCondVarPtr(0),
          mWaitingCount(0),
          mDoneFlag(false),
          mCanceledFlag(false),
          mBufToDeletePtr(0),
          mStatus(0)
        { Queue::Init(*this); }
    virtual ~ReadRequest()
    {
        QCASSERT(mWaitingCount == 0 && mCondVarPtr == 0);
        delete [] mBufToDeletePtr;
    }
    int Init(
        FileTableEntry& inEntry,
        void*           inBufPtr,
        int             inSize,
        int64_t         inOffset,
        int             inMsgLogId)
    {
        QCASSERT(! mCondVarPtr);
        if (inOffset < 0 || ! inBufPtr) {
            return -EINVAL;
        }
        if (inSize <= 0) {
            return 0;
        }
        Reset(
            KfsProtocolWorker::kRequestTypeReadAsync,
            inEntry.instance + 1,
            inEntry.fattr.fileId,
            &mOpenParams,
            inBufPtr,
            inSize,
            0, // inMaxPending,
            inOffset
        );
        if (GetSize() <= 0) {
            return 0;
        }
        mOpenParams.mPathName            = inEntry.pathname;
        mOpenParams.mFileSize            = inEntry.fattr.fileSize;
        mOpenParams.mStriperType         = inEntry.fattr.striperType;
        mOpenParams.mStripeSize          = inEntry.fattr.stripeSize;
        mOpenParams.mStripeCount         = inEntry.fattr.numStripes;
        mOpenParams.mRecoveryStripeCount = inEntry.fattr.numRecoveryStripes;
        mOpenParams.mReplicaCount        = inEntry.fattr.numReplicas;
        mOpenParams.mSkipHolesFlag       = inEntry.skipHoles;
        mOpenParams.mFailShortReadsFlag  = inEntry.failShortReadsFlag;
        mOpenParams.mMsgLogId            = inMsgLogId;
        mWaitingCount = 0;
        mDoneFlag     = false;
        mCanceledFlag = false;
        mStatus       = 0;
        Queue::PushBack(inEntry.mReadQueue, *this);
        return GetSize();
    }
    static bool IsReadAheadInFlight(
        FileTableEntry& inEntry)
    {
        return (inEntry.buffer.mReadReq &&
            ! inEntry.buffer.mReadReq->mDoneFlag);
    }
    static int CopyReadAhead(
        FileTableEntry& inEntry,
        void*           inBufPtr,
        int             inSize,
        int64_t         inOffset,
        bool&           outShortReadFlag)
    {
        QCASSERT(! IsReadAheadInFlight(inEntry));
        outShortReadFlag =
            inEntry.buffer.mStatus >= 0 &&
            inEntry.buffer.mStatus < inEntry.buffer.mSize &&
            inEntry.buffer.mStart + inEntry.buffer.mStatus < inOffset + inSize;
        const int64_t thePos = inOffset - inEntry.buffer.mStart;
        QCASSERT(thePos >= 0);
        const int     theLen = (int)min(
            int64_t(inSize), inEntry.buffer.mStatus - thePos);
        if (theLen <= 0) {
            return 0;
        }
        memcpy(inBufPtr, inEntry.buffer.mBuf + (size_t)thePos, (size_t)theLen);
        return theLen;
    }
private:
    ReadRequest(
        const ReadRequest& inReq);
    ReadRequest& operator=(
        const ReadRequest& inReq);
};

void
KfsClientImpl::InitPendingRead(
    FileTableEntry& inEntry)
{
    QCASSERT(mMutex.IsOwned());
    ReadRequest::InitEntry(inEntry);
}

void
KfsClientImpl::CancelPendingRead(
    FileTableEntry& inEntry)
{
    QCASSERT(mMutex.IsOwned());
    ReadRequest::CancelAll(inEntry);
}

void
KfsClientImpl::CleanupPendingRead()
{
    while (mFreeCondVarsHead) {
        ReadRequestCondVar* const thePtr = mFreeCondVarsHead;
        mFreeCondVarsHead = thePtr->mNextPtr;
        delete thePtr;
    }
}

int
KfsClientImpl::ReadPrefetch(
    int    inFd,
    char*  inBufPtr,
    size_t inSize)
{
    if (! inBufPtr) {
        return -EINVAL;
    }

    QCStMutexLocker theLocker(mMutex);

    if (! valid_fd(inFd)) {
        KFS_LOG_STREAM_ERROR <<
            "read prefetch error invalid fd: " << inFd <<
        KFS_LOG_EOM;
        return -EBADF;
    }
    FileTableEntry& theEntry = *mFileTable[inFd];
    if (theEntry.openMode == O_WRONLY ||
            theEntry.currPos.fileOffset < 0 ||
            theEntry.cachedAttrFlag) {
        return -EINVAL;
    }
    if (theEntry.fattr.isDirectory) {
        return -EISDIR;
    }
    if (inSize <= 0) {
        return 0;
    }
    const int64_t theOffset = theEntry.currPos.fileOffset;
    if (theOffset >= ReadRequest::GetEof(theEntry) ||
            ReadRequest::Find(theEntry, inBufPtr, (int64_t)inSize, theOffset)) {
        return 0;
    }
    StartProtocolWorker();
    ReadRequest* const theReqPtr = ReadRequest::Create(
        mReadCompletionMutex,
        theEntry,
        inBufPtr,
        (int)min(inSize, (size_t)numeric_limits<int>::max()),
        theOffset,
        inFd
    );
    if (! theReqPtr) {
        return 0;
    }
    theEntry.readUsedProtocolWorkerFlag = true;
    const int theRet = theReqPtr->GetSize();
    theLocker.Unlock();
    QCASSERT(! mMutex.IsOwned());

    mProtocolWorker->Enqueue(*theReqPtr);
    return theRet;
}

inline static int64_t
SkipChunkTail(
    int64_t inPos,
    int64_t inEof)
{
    const int64_t kChunkSize = (int64_t)CHUNKSIZE;
    const int64_t theTail    = inPos % kChunkSize;
    return (theTail > 0 ? min(inPos - theTail + kChunkSize, inEof) : inPos);
}

ssize_t
KfsClientImpl::Read(
    int         inFd,
    char*       inBufPtr,
    size_t      inSize,
    chunkOff_t* inPosPtr /* = 0 */)
{
    QCStMutexLocker theLocker(mMutex);

    if (! valid_fd(inFd)) {
        KFS_LOG_STREAM_ERROR <<
            "read error invalid fd: " << inFd <<
        KFS_LOG_EOM;
        return -EBADF;
    }
    FileTableEntry& theEntry = *mFileTable[inFd];
    if (theEntry.openMode == O_WRONLY || theEntry.cachedAttrFlag) {
        return -EINVAL;
    }
    if (theEntry.fattr.isDirectory) {
        return ReadDirectory(inFd, inBufPtr, inSize);
    }

    chunkOff_t& theFilePos = inPosPtr ? *inPosPtr : theEntry.currPos.fileOffset;
    int64_t     theFdPos   = theFilePos;
    if (theFdPos < 0) {
        return -EINVAL;
    }

    const KfsProtocolWorker::FileId       theFileId   = theEntry.fattr.fileId;
    const KfsProtocolWorker::FileInstance theInstance = theEntry.instance + 1;

    const int64_t kChunkSize       = (int64_t)CHUNKSIZE;
    const int64_t theEof           = ReadRequest::GetEof(theEntry);
    int           theRet           = 0;
    int64_t       thePos           = theFdPos;
    int64_t       theLen           = min(theEof - thePos, (int64_t)inSize);
    const int     theSize          = (int)theLen;
    const bool    theSkipHolesFlag = theEntry.skipHoles;
    if (theLen <= 0) {
        return 0;
    }
    // Wait for prefetch with this buffer, if any.
    ReadRequest* const theReqPtr = ReadRequest::Find(
        theEntry, inBufPtr, (int64_t)inSize, thePos);
    if (theReqPtr) {
        void* const   theBufPtr  = theReqPtr->GetBufferPtr();
        const int64_t theReqPos  = theReqPtr->GetOffset();
        const int     theReqSize = theReqPtr->GetSize();
        int64_t       theRes     = theReqPtr->Wait(
            mMutex, mFreeCondVarsHead, theEntry);
        if (theSkipHolesFlag && theRes == -ENOENT) {
            theRes = 0;
        }
        if (theRes < 0) {
            return (ssize_t)theRes;
        }
        // For now discard pre-fetch if pre-fetch position is larger than the
        // current.
        if (theReqPos <= thePos && theSize == theLen) {
            const int64_t theNRd =
                min(int64_t(theSize) - theRet, theRes - (thePos - theReqPos));
            if (theNRd > 0) {
                if (inBufPtr + theRet != theBufPtr || thePos != theReqPos) {
                    memmove(inBufPtr + theRet, theBufPtr, (size_t)theNRd);
                }
                thePos += theNRd;
                theRet += theNRd;
                if (theSkipHolesFlag &&
                        theRes < theReqSize &&
                        theReqPos + theRes <= thePos) {
                    // Move to the next chunk if read was short.
                    thePos = SkipChunkTail(thePos, theEof);
                }
            }
        }
        // Request wait releases mutex, ensure that the fd wasn't closed by
        // other thread.
        if (! valid_fd(inFd) || mFileTable[inFd] != &theEntry ||
                theEntry.instance + 1 != theInstance) {
            return theRet;
        }
        if (theFilePos == theFdPos) {
            theFilePos = thePos;
            theFdPos   = thePos;
        }
    }
    // Do the check here to allow to de-queue prefetch regardless of the buffer
    // size.
    if (theSize != theLen) {
        return -EOVERFLOW;
    }
    if (theEof <= thePos) {
        return theRet;
    }
    // Do not return if nothing more to read -- start the read ahead.
    StartProtocolWorker();
    theEntry.readUsedProtocolWorkerFlag = true;

    bool theShortReadFlag = false;
    const int theRes = ReadRequest::GetReadAhead(
        mMutex,
        mFreeCondVarsHead,
        theEntry,
        inBufPtr + theRet,
        theSize - theRet,
        thePos,
        theShortReadFlag
    );
    if (theRes < 0) {
        return theRes;
    }
    thePos += theRes;
    theRet += theRes;
    if (theSkipHolesFlag && theShortReadFlag) {
        // Move to the next chunk if read was short.
        thePos = SkipChunkTail(thePos, theEof);
    }
    // Wait in GetReadAhead() releases the mutex, ensure that
    // the file position remains the same before updating it, or using it.
    // Wait returns an error if theEntry (theRet < 0) becomes invalid as result
    // of Close() call for example.
    // Use read ahead to get the remainder of the request if the remainder is
    // small enough.
    if (theSize <= theRet ||
            (theFdPos == theFilePos &&
                (theSize - theRet) <=
                    ReadRequest::GetReadAheadSize(theEntry, thePos) / 2)) {
        if (theFdPos == theFilePos) {
            theFilePos = thePos;
            theFdPos   = thePos;
        }
        ReadRequest* const theReqPtr = ReadRequest::InitReadAhead(
            mReadCompletionMutex, theEntry, inFd, theFilePos);
        if (theReqPtr) {
            mProtocolWorker->Enqueue(*theReqPtr);
            if (theSize <= theRet) {
                return theRet;
            }
        }
        const int theRes = ReadRequest::GetReadAhead(
            mMutex,
            mFreeCondVarsHead,
            theEntry,
            inBufPtr + theRet,
            theSize - theRet,
            thePos,
            theShortReadFlag
        );
        if (theRes < 0) {
            return theRes;
        }
        thePos += theRes;
        theRet += theRes;
        if (theSkipHolesFlag && theShortReadFlag) {
            // Move to the next chunk if read was short.
            thePos = SkipChunkTail(thePos, theEof);
        }
        if (theFdPos == theFilePos) {
            theFilePos = thePos;
            theFdPos   = thePos;
        }
        if (theSize <= theRet) {
            return theRet;
        }
    }
    if (theEof <= thePos) {
        if (theFdPos == theFilePos) {
            theFilePos = theEof;
        }
        return theRet;
    }
    KfsProtocolWorker::Request::Params theOpenParams;
    theOpenParams.mPathName            = theEntry.pathname;
    theOpenParams.mFileSize            = theEntry.fattr.fileSize;
    theOpenParams.mStriperType         = theEntry.fattr.striperType;
    theOpenParams.mStripeSize          = theEntry.fattr.stripeSize;
    theOpenParams.mStripeCount         = theEntry.fattr.numStripes;
    theOpenParams.mRecoveryStripeCount = theEntry.fattr.numRecoveryStripes;
    theOpenParams.mReplicaCount        = theEntry.fattr.numReplicas;
    theOpenParams.mSkipHolesFlag       = theSkipHolesFlag;
    theOpenParams.mFailShortReadsFlag  = theEntry.failShortReadsFlag;
    theOpenParams.mMsgLogId            = inFd;

    theLocker.Unlock();
    QCASSERT(! mMutex.IsOwned());

    int64_t theChunkEnd = theSkipHolesFlag ?
        min(theEof, (thePos - thePos % kChunkSize + kChunkSize)) : theEof;
    for (; ;) {
        const int theRdSize =
            min(int64_t(theSize) - theRet, theChunkEnd - thePos);
        if (theRdSize <= 0) {
            break;
        }
        int theStatus = (int)mProtocolWorker->Execute(
            KfsProtocolWorker::kRequestTypeRead,
            theInstance,
            theFileId,
            &theOpenParams,
            inBufPtr + theRet,
            theRdSize,
            0,
            thePos
        );
        if (theSkipHolesFlag && theStatus == -ENOENT) {
            theStatus = 0;
        }
        if (theStatus < 0) {
            theRet = theStatus;
            break;
        }
        QCRTASSERT(theStatus <= theRdSize);
        theRet += theStatus;
        thePos += theStatus;
        if (theSize <= theRet) {
            break;
        }
        if (! theSkipHolesFlag || thePos >= theEof) {
            break;
        }
        thePos = theChunkEnd;
        if (thePos >= theEof) {
            thePos = theEof;
            break;
        }
        theChunkEnd = min(theEof, theChunkEnd + kChunkSize);
    }
    ReadRequest* theReadAheadReqPtr = 0;
    if (theRet > 0) {
        QCStMutexLocker theLocker(mMutex);
        if (! valid_fd(inFd) || mFileTable[inFd] != &theEntry) {
            return theRet;
        }
        if (theEntry.instance + 1 == theInstance && theFilePos == theFdPos) {
            QCASSERT(mProtocolWorker);
            theFilePos = thePos;
            theReadAheadReqPtr = ReadRequest::InitReadAhead(
                mReadCompletionMutex, theEntry, inFd, theFilePos);
        }
    }
    if (theReadAheadReqPtr) {
        mProtocolWorker->Enqueue(*theReadAheadReqPtr);
    }
    return theRet;
}

ssize_t
KfsClientImpl::SetReadAheadSize(
    int    inFd,
    size_t inSize)
{
    QCStMutexLocker theLocker(mMutex);
    if (! valid_fd(inFd)) {
        KFS_LOG_STREAM_ERROR <<
            "read error invalid inFd: " << inFd <<
        KFS_LOG_EOM;
        return -EBADF;
    }
    return SetReadAheadSize(*mFileTable[inFd], inSize);
}

ssize_t
KfsClientImpl::SetReadAheadSize(
    FileTableEntry& inEntry,
    size_t          inSize,
    bool            inOptimalFlag)
{
    QCASSERT(mMutex.IsOwned());

    int theSize = (int)min((size_t)numeric_limits<int>::max(),
        (inSize + CHECKSUM_BLOCKSIZE - 1) /
            CHECKSUM_BLOCKSIZE * CHECKSUM_BLOCKSIZE);
    FileAttr& theAttr = inEntry.fattr;
    if (theSize > 0 &&
            theAttr.striperType != KFS_STRIPED_FILE_TYPE_NONE &&
            theAttr.stripeSize > 0 &&
            theAttr.numStripes > 0 &&
            theAttr.stripeSize < theSize) {
        const int theStride = theAttr.stripeSize * theAttr.numStripes;
        theSize = (max(inOptimalFlag ?
                mTargetDiskIoSize * theAttr.numStripes : 0, theSize) +
            theStride - 1) / theStride * theStride;
    }
    inEntry.buffer.SetBufSize(theSize);
    return inEntry.buffer.GetBufSize();
}

ssize_t
KfsClientImpl::GetReadAheadSize(
    int inFd) const
{
    QCStMutexLocker theLocker(const_cast<KfsClientImpl*>(this)->mMutex);

    if (! valid_fd(inFd)) {
        KFS_LOG_STREAM_ERROR <<
            "read error invalid inFd: " << inFd <<
        KFS_LOG_EOM;
        return -EBADF;
    }
    return mFileTable[inFd]->buffer.GetBufSize();
}

}}
