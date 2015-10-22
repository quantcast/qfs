//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2010/07/13
// Author: Mike Ovsiannikov
//
// Copyright 2010-2012 Quantcast Corp.
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
//
//----------------------------------------------------------------------------

#include "Reader.h"

#include "kfsio/IOBuffer.h"
#include "kfsio/NetManager.h"
#include "kfsio/checksum.h"
#include "kfsio/ITimeout.h"
#include "kfsio/ClientAuthContext.h"

#include "common/kfsdecls.h"
#include "common/MsgLogger.h"

#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"
#include "qcdio/qcdebug.h"
#include "qcdio/QCDLList.h"

#include "KfsOps.h"
#include "utils.h"
#include "KfsClient.h"
#include "RSStriper.h"
#include "ClientPool.h"

#include <sstream>
#include <algorithm>
#include <cerrno>
#include <sstream>
#include <limits>
#include <string.h>

namespace KFS
{
namespace client
{

using std::min;
using std::max;
using std::string;
using std::ostream;
using std::ostringstream;
using std::random_shuffle;
using std::vector;
using std::pair;
using std::make_pair;

// Kfs client read state machine implementation.
class Reader::Impl : public QCRefCountedObj
{
public:
    typedef QCRefCountedObj::StRef StRef;

    enum
    {
        kErrorNone           = 0,
        kErrorParameters     = -EINVAL,
        kErrorIO             = -EIO,
        kErrorTryAgain       = -EAGAIN,
        kErrorNoEntry        = -ENOENT,
        kErrorBusy           = -EBUSY,
        kErrorChecksum       = -EBADCKSUM,
        kErrorLeaseExpired   = -ELEASEEXPIRED,
        kErrorFault          = -EFAULT,
        kErrorInvalChunkSize = -EINVALCHUNKSIZE,
        kErrorPermissions    = -EPERM
    };

    Impl(
        Reader&     inOuter,
        MetaServer& inMetaServer,
        Completion* inCompletionPtr,
        int         inMaxRetryCount,
        int         inTimeSecBetweenRetries,
        int         inOpTimeoutSec,
        int         inIdleTimeoutSec,
        int         inMaxReadSize,
        int         inLeaseRetryTimeout,
        int         inLeaseWaitTimeout,
        string      inLogPrefix,
        int64_t     inChunkServerInitialSeqNum,
        ClientPool* inClientPoolPtr)
        : QCRefCountedObj(),
          mOuter(inOuter),
          mMetaServer(inMetaServer),
          mPathName(),
          mFileId(-1),
          mClosingFlag(false),
          mErrorCode(0),
          mIdleTimeoutSec(inIdleTimeoutSec),
          mOpTimeoutSec(inOpTimeoutSec),
          mMaxRetryCount(inMaxRetryCount),
          mTimeSecBetweenRetries(inTimeSecBetweenRetries),
          mMaxReadSize(inMaxReadSize),
          mLeaseRetryTimeout(inLeaseRetryTimeout),
          mLeaseWaitTimeout(inLeaseWaitTimeout),
          mSkipHolesFlag(false),
          mFailShortReadsFlag(false),
          mMaxGetAllocRetryCount(inMaxRetryCount),
          mOffset(0),
          mOpenChunkBlockSize(0),
          mChunkServerInitialSeqNum(inChunkServerInitialSeqNum),
          mClientPoolPtr(inClientPoolPtr),
          mCompletionPtr(inCompletionPtr),
          mLogPrefix(inLogPrefix),
          mStats(),
          mChunkServersStats(),
          mNetManager(mMetaServer.GetNetManager()),
          mStriperPtr(0),
          mCompletionDepthCount(0)
        { Readers::Init(mReaders); }
    int Open(
        kfsFileId_t inFileId,
        const char* inFileNamePtr,
        Offset      inFileSize,
        int         inStriperType,
        int         inStripeSize,
        int         inStripeCount,
        int         inRecoveryStripeCount,
        bool        inSkipHolesFlag,
        bool        inUseDefaultBufferAllocatorFlag,
        Offset      inRecoverChunkPos,
        bool        inFailShortReadsFlag)
    {
        const char* const theFileNamePtr = inFileNamePtr ? inFileNamePtr : "";
        if (inFileId <= 0 || (! *theFileNamePtr && inRecoverChunkPos < 0)) {
            return kErrorParameters;
        }
        if (mFileId > 0) {
            if (inFileId == mFileId &&
                    theFileNamePtr == mPathName) {
                return mErrorCode;
            }
            return kErrorParameters;
        }
        if (IsOpen() && mErrorCode != 0) {
            return (mErrorCode < 0 ? mErrorCode : -mErrorCode);
        }
        if (mClosingFlag) {
            return kErrorTryAgain;
        }
        QCASSERT(Readers::IsEmpty(mReaders));
        delete mStriperPtr;
        string theErrMsg;
        mStriperPtr = 0;
        mOpenChunkBlockSize = Offset(CHUNKSIZE);
        mStriperPtr = Striper::Create(
            inStriperType,
            inStripeCount,
            inRecoveryStripeCount,
            inStripeSize,
            mMaxReadSize,
            inUseDefaultBufferAllocatorFlag,
            inFailShortReadsFlag,
            inRecoverChunkPos,
            inFileSize,
            mChunkServerInitialSeqNum,
            mLogPrefix,
            *this,
            mOpenChunkBlockSize,
            theErrMsg
        );
        if (! theErrMsg.empty()) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                theErrMsg <<
            KFS_LOG_EOM;
            return kErrorParameters;
        }
        if (! mStriperPtr || mOpenChunkBlockSize < Offset(CHUNKSIZE)) {
            mOpenChunkBlockSize = Offset(CHUNKSIZE);
        }
        mStats.Clear();
        mSkipHolesFlag      = inSkipHolesFlag;
        mPathName           = theFileNamePtr;
        mErrorCode          = 0;
        mFileId             = inFileId;
        mFailShortReadsFlag = inFailShortReadsFlag;
        return 0;
    }
    int Close()
    {
        if (! IsOpen()) {
            return 0;
        }
        if (mErrorCode != 0) {
            return mErrorCode;
        }
        if (mClosingFlag) {
            return kErrorTryAgain;
        }
        mClosingFlag = true;
        return StartRead();
    }
    int Read(
        IOBuffer&  inBuffer,
        int        inLength,
        Offset     inOffset,
        RequestId  inRequestId)
    {
        if (inOffset < 0) {
            return kErrorParameters;
        }
        if (mErrorCode != 0) {
            return mErrorCode;
        }
        if (mClosingFlag || ! IsOpen()) {
            return kErrorParameters;
        }
        if (inLength <= 0) {
            IOBuffer theBuf;
            return (
                ReportCompletion(0, 0, 0, inOffset, &theBuf, inRequestId) ?
                mErrorCode : 0
            );
        }
        return StartRead(inBuffer, inLength, inOffset, inRequestId);
    }
    void Stop()
    {
        while (! Readers::IsEmpty(mReaders)) {
            delete Readers::Front(mReaders);
        }
        mClosingFlag = false;
    }
    void Shutdown()
    {
        Stop();
        delete mStriperPtr;
        mStriperPtr = 0;
        mFileId     = -1;
        mErrorCode  = 0;
    }
    bool IsOpen() const
        { return (mFileId > 0); }
    bool IsClosing() const
        { return (IsOpen() && mClosingFlag); }
    bool IsActive() const
    {
        return (
            IsOpen() && (
                ! Readers::IsEmpty(mReaders) ||
                mClosingFlag)
        );
    }
    void DisableCompletion()
        { mCompletionPtr = 0; }
    void Register(
        Completion* inCompletionPtr)
    {
        if (inCompletionPtr == mCompletionPtr) {
            return;
        }
        if (mCompletionPtr) {
            mCompletionPtr->Unregistered(mOuter);
        }
        mCompletionPtr = inCompletionPtr;
    }
    bool Unregister(
        Completion* inCompletionPtr)
    {
        if (inCompletionPtr != mCompletionPtr) {
            return false;
        }
        mCompletionPtr = 0;
        return true;
    }
    void GetStats(
        Stats&               outStats,
        KfsNetClient::Stats& outChunkServersStats) const
    {
        outStats             = mStats;
        outChunkServersStats = mChunkServersStats;
    }
    bool GetErrorCode() const
        { return mErrorCode; }

private:
    typedef KfsNetClient ChunkServer;

    class ChunkReader : private ITimeout, private KfsNetClient::OpOwner
    {
    public:
        class ReadOp;
        typedef QCDLList<ReadOp,      0> Queue;
        typedef QCDLList<ChunkReader, 0> Readers;

        class ReadOp : public KFS::client::ReadOp
        {
        public:
            struct RequestEntry
            {
                RequestEntry(
                    size_t    inSize             = 0,
                    RequestId inRequestId        = RequestId(),
                    RequestId inStriperRequestId = RequestId())
                    : mSize(inSize),
                      mRequestId(inRequestId),
                      mStriperRequestId(inStriperRequestId),
                      mCancelFlag(false)
                    {}

                size_t    mSize;
                RequestId mRequestId;
                RequestId mStriperRequestId;
                bool      mCancelFlag;
            };
            typedef vector<RequestEntry> Requests;

            time_t    mOpStartTime;
            IOBuffer  mBuffer;
            IOBuffer  mTmpBuffer;
            RequestId mRequestId;
            RequestId mStriperRequestId;
            Requests  mRequests;
            bool      mRetryIfFailsFlag;
            bool      mFailShortReadFlag;
            bool      mCancelFlag;

            ReadOp(
                int       inOpSize,
                Offset    inOffset,
                RequestId inRequestId,
                RequestId inStriperRequestId,
                bool      inRetryIfFailsFlag,
                bool      inFailShortReadFlag)
                : KFS::client::ReadOp(-1, -1, -1),
                  mOpStartTime(0),
                  mBuffer(),
                  mTmpBuffer(),
                  mRequestId(inRequestId),
                  mStriperRequestId(inStriperRequestId),
                  mRequests(),
                  mRetryIfFailsFlag(inRetryIfFailsFlag),
                  mFailShortReadFlag(inFailShortReadFlag),
                  mCancelFlag(false)
            {
                Queue::Init(*this);
                numBytes                   = inOpSize;
                offset                     = inOffset;
                skipVerifyDiskChecksumFlag = true;
            }
            void Delete(
                ReadOp** inQueuePtr)
            {
                Queue::Remove(inQueuePtr, *this);
                delete this;
            }
        private:
            ReadOp* mPrevPtr[1];
            ReadOp* mNextPtr[1];

            friend class QCDLListOp<ReadOp, 0>;
            virtual ~ReadOp()
                {}
        private:
            ReadOp(
                const ReadOp& inOp);
            ReadOp& operator=(
                const ReadOp& inOp);
        };

        ChunkReader(
            Impl&         inOuter,
            int64_t       inSeqNum,
            const string& inLogPrefix)
            : ITimeout(),
              KfsNetClient::OpOwner(),
              mOuter(inOuter),
              mChunkServer(
                inOuter.mNetManager,
                string(), -1, // host, port
                // All chunk server retries are handled here
                0, // inMaxRetryCount
                0, // inTimeSecBetweenRetries,
                inOuter.mOpTimeoutSec,
                inOuter.mIdleTimeoutSec,
                inSeqNum,
                inLogPrefix.c_str(),
                // Just fail the op. Error handler will reset connection and
                // cancel all pending ops by calling Stop()
                false, // inResetConnectionOnOpTimeoutFlag
                // Allow some slack and ensure that content size limit is
                // reasonably large.
                int(min(
                    int64_t(inOuter.mMaxReadSize) + (64 << 10),
                    int64_t(std::numeric_limits<int>::max())
                ))
              ),
              mChunkServerPtr(0),
              mErrorCode(0),
              mRetryCount(0),
              mOpenChunkBlockFileOffset(-1),
              mOpStartTime(0),
              mGetAllocOp(0, -1, -1),
              mLeaseAcquireOp(0, -1, ""),
              mLeaseRenewOp(0, -1, 0, ""),
              mLeaseRelinquishOp(0, -1, 0),
              mSizeOp(0, -1, 0),
              mChunkServerAccess(),
              mChunkAccess(),
              mLastOpPtr(0),
              mLastMetaOpPtr(0),
              mChunkServerIdx(0),
              mLeaseRenewTime(Now() - 1),
              mLeaseExpireTime(mLeaseRenewTime),
              mLeaseWaitStartTime(0),
              mChunkServerAccessExpires(0),
              mLeaseRetryCount(0),
              mSleepingFlag(false),
              mClosingFlag(false),
              mChunkServerSetFlag(false),
              mNoCSAccessFlag(false),
              mStartReadRunningFlag(false),
              mRestartStartReadFlag(false),
              mSizeOpInFlightFlag(false),
              mLeaseToRelinquish(-1),
              mLogPrefix(inLogPrefix),
              mOpsNoRetryCount(0),
              mDeletedFlagPtr(0),
              mRunningCompletionPtr(0)
        {
            Queue::Init(mPendingQueue);
            Queue::Init(mInFlightQueue);
            Queue::Init(mCompletionQueue);
            Readers::Init(*this);
            Readers::PushFront(mOuter.mReaders, *this);
            mChunkServer.SetRetryConnectOnly(true);
            mGetAllocOp.fileOffset  = -1;
            mGetAllocOp.chunkId     = -1;
            mLeaseAcquireOp.chunkId = -1;
            mLeaseAcquireOp.leaseId = -1;
            mSizeOp.size            = -1;
            mSizeOp.chunkVersion    = -1;
            mGetAllocOp.status      = 0;
        }
        ~ChunkReader()
        {
            ChunkReader::Shutdown();
            ChunkServer::Stats theStats;
            mChunkServer.GetStats(theStats);
            mOuter.mChunkServersStats.Add(theStats);
            Readers::Remove(mOuter.mReaders, *this);
            if (mDeletedFlagPtr) {
                *mDeletedFlagPtr = true;
            }
            StRunningCompletion::Delete(mRunningCompletionPtr);
        }
        Offset GetSize() const
            { return mSizeOp.size; }
        void CancelClose()
        {
            if (mClosingFlag) {
                if (mLastOpPtr == &mLeaseRelinquishOp) {
                    mOuter.mMetaServer.Cancel(mLastOpPtr, this);
                }
                mClosingFlag = false;
            }
        }
        // The QueueRead() guarantees that completion will not be invoked.
        // The reads will be queued even if the reader is already in the error
        // state: mErrorCode != 0. In the case of fatal error all pending writes
        // are discarded when the writer gets deleted.
        //
        // StartRead() must be called in order to start executing pending
        // reads.
        // This allows the caller to properly update its state before the reads
        // get executed, and the corresponding completion(s) invoked.
        int QueueRead(
            IOBuffer& inBuffer,
            int       inSize,
            Offset    inOffset,
            RequestId inRequestId,
            RequestId inStriperRequestId,
            bool      inRetryIfFailsFlag,
            bool      inFailShortReadFlag)
        {
            int theSize = inSize;
            if (theSize <= 0) {
                return 0;
            }
            if (inOffset < 0) {
                return kErrorParameters;
            }
            QCRTASSERT(inOffset >= 0 && ! mClosingFlag);
            const Offset kMaxChunkSize  = (Offset)CHUNKSIZE;
            const Offset theChunkOffset = inOffset % kMaxChunkSize;
            if (mGetAllocOp.fileOffset < 0) {
                mGetAllocOp.fid        = mOuter.mFileId;
                mGetAllocOp.filename   = mOuter.mPathName;
                mGetAllocOp.fileOffset = inOffset - theChunkOffset;
                mOpenChunkBlockFileOffset = mGetAllocOp.fileOffset -
                    mGetAllocOp.fileOffset % mOuter.mOpenChunkBlockSize;
            } else {
                QCRTASSERT(mGetAllocOp.fileOffset == inOffset - theChunkOffset);
            }
            Offset thePos = theChunkOffset;
            theSize = min(theSize, (int)(kMaxChunkSize - thePos));
            QCASSERT(theSize > 0);
            // Try to use the last pending op.
            ReadOp* const theLastOpPtr = Queue::Back(mPendingQueue);
            if (theLastOpPtr) {
                ReadOp& theOp = *theLastOpPtr;
                const int    theOpSize = theOp.numBytes;
                const Offset theOpPos  = theOp.offset;
                if (theOpPos + theOpSize == thePos &&
                        theOp.mRetryIfFailsFlag == inRetryIfFailsFlag &&
                        theOp.mFailShortReadFlag == inFailShortReadFlag &&
                        theOpSize + theSize <= mOuter.mMaxReadSize) {
                    if (theOp.mRequests.empty()) {
                        theOp.mRequests.push_back(ReadOp::RequestEntry(
                            theOp.numBytes,
                            theOp.mRequestId,
                            theOp.mStriperRequestId
                        ));
                    }
                    QCVERIFY(theOpSize <=
                        theOp.mBuffer.EnsureSpaceAvailable(theOpSize)
                    );
                    theOp.mRequests.push_back(ReadOp::RequestEntry(
                        size_t(theSize),
                        inRequestId,
                        inStriperRequestId
                    ));
                    theOp.numBytes += theSize;
                    thePos         += theSize;
                    theOp.mBuffer.MoveSpaceAvailable(&inBuffer, theSize);
                    theSize = 0;
                }
            }
            const int theMaxReadSize = max(1, mOuter.mMaxReadSize);
            while (theSize > 0) {
                const int theOpSize = min(theMaxReadSize, theSize);
                ReadOp& theOp = *(new ReadOp(
                    theOpSize,
                    thePos,
                    inRequestId,
                    inStriperRequestId,
                    inRetryIfFailsFlag,
                    inFailShortReadFlag
                ));
                if (! inRetryIfFailsFlag) {
                    mOpsNoRetryCount++;
                }
                thePos        += theOpSize;
                theSize       -= theOpSize;
                theOp.mBuffer.MoveSpaceAvailable(&inBuffer, theOpSize);
                Queue::PushBack(mPendingQueue, theOp);
            }
            QCRTASSERT(thePos <= kMaxChunkSize && theSize >= 0);
            return (int)(thePos - theChunkOffset);
        }
        void StartRead()
        {
            // Unwind recursion form the possible synchronous op completion.
            if (mStartReadRunningFlag) {
                KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                    "unwiding recursion:" <<
                    " filepos: " << mGetAllocOp.fileOffset <<
                    " chunkid: " << mGetAllocOp.chunkId <<
                    " restart: " << mRestartStartReadFlag  <<
                KFS_LOG_EOM;
                mRestartStartReadFlag = true;
                return;
            }
            mStartReadRunningFlag = true;
            QCStDeleteNotifier theDeleteNotifier(mDeletedFlagPtr);
            do {
                mRestartStartReadFlag = false;
                StartReadSelf();
                if (theDeleteNotifier.IsDeleted()) {
                    return; // Unwind.
                }
            } while (mRestartStartReadFlag);
            mStartReadRunningFlag = false;
        }
        void Close()
        {
            if (! mClosingFlag && mErrorCode == 0 && IsOpen()) {
                mClosingFlag = true;
                StartRead();
            }
        }
        void Shutdown()
        {
            // Put lease release on the "wire", if possible, but do not
            // wait for completion. Reset will remove the op from the
            // meta server's queue, and reply will be discarded.
            if (0 < mLeaseAcquireOp.chunkId &&
                    0 <= mLeaseAcquireOp.leaseId &&
                    &mLeaseRelinquishOp != mLastMetaOpPtr) {
                CloseChunk();
            }
            Reset();
            QCRTASSERT(Queue::IsEmpty(mInFlightQueue));
            Queue::PushBackList(mCompletionQueue, mPendingQueue);
            while (! Queue::IsEmpty(mCompletionQueue)) {
                Queue::Front(mCompletionQueue)->Delete(mCompletionQueue);
            }
            QCRTASSERT(
                Queue::IsEmpty(mInFlightQueue) &&
                Queue::IsEmpty(mPendingQueue) &&
                Queue::IsEmpty(mCompletionQueue)
            );
            mClosingFlag     = false;
            mErrorCode       = 0;
            mOpsNoRetryCount = 0;
        }
        Offset GetFileOffset() const
        {
            return ((mErrorCode == 0 && ! mClosingFlag) ?
                mGetAllocOp.fileOffset : -1);
        }
        Offset GetOpenChunkBlockFileOffset() const
        {
            return (GetFileOffset() >= 0 ? mOpenChunkBlockFileOffset : -1);
        }
        bool IsIdle() const
        {
            return (
                Queue::IsEmpty(mPendingQueue) &&
                Queue::IsEmpty(mInFlightQueue) &&
                Queue::IsEmpty(mCompletionQueue) &&
                ! mClosingFlag
            );
        }
        bool IsOpen() const
        {
            return (
                mErrorCode == 0 &&
                mGetAllocOp.fileOffset >= 0 &&
                ! mClosingFlag
            );
        }
        int GetErrorCode() const
            { return mErrorCode; }
        void CancelRead()
        {
            QCASSERT(mOuter.mStriperPtr);
            const bool theRestartFlag =
                ! Queue::IsEmpty(mInFlightQueue) &&
                Queue::IsEmpty(mPendingQueue);
            CancelRead(mInFlightQueue);
            if (Queue::IsEmpty(mInFlightQueue)) {
                StopChunkServer(); // Discard replies if any.
                mChunkServerSetFlag = false;
            }
            CancelRead(mPendingQueue);
            CancelRead(mCompletionQueue);
            StRunningCompletion::Cancel(
                mRunningCompletionPtr, *mOuter.mStriperPtr);
            if (mSleepingFlag) {
                if (! CanRead()) {
                    Timeout();
                }
            } else if (theRestartFlag && ! Queue::IsEmpty(mPendingQueue)) {
                StartRead();
            }
        }
        ChunkReader* GetPrevPtr()
        {
            ChunkReader& thePrev = ReadersListOp::GetPrev(*this);
            return (&thePrev == this ? 0 : &thePrev);
        }
        kfsChunkId_t GetChunkId() const
            { return mGetAllocOp.chunkId; }
        int64_t GetChunkVersion() const
        {
            return (mGetAllocOp.chunkId >= 0 ?
                mGetAllocOp.chunkVersion : int64_t(-1));
        }
    private:
        class StRunningCompletion
        {
        public:
            StRunningCompletion(
                StRunningCompletion*& inHeadPtr,
                ReadOp::Requests&     inRequests)
                : mRequests(),
                  mNextPtr(0),
                  mHeadPtr(0)
            {
                if (inRequests.empty()) {
                    return;
                }
                mNextPtr  = inHeadPtr;
                inHeadPtr = this;
                mHeadPtr  = &inHeadPtr;
                mRequests.swap(inRequests);
            }
            ~StRunningCompletion()
            {
                if (! mHeadPtr) {
                    return;
                }
                *mHeadPtr = mNextPtr;
            }
            static void Cancel(
                StRunningCompletion* inHeadPtr,
                Striper&             inStriper)
            {
                StRunningCompletion* thePtr = inHeadPtr;
                while (thePtr) {
                    thePtr->CancelSelf(inStriper);
                    thePtr = thePtr->mNextPtr;
                }
            }
            static void Delete(
                StRunningCompletion*& inHeadPtr)
            {
                StRunningCompletion* thePtr = inHeadPtr;
                while (thePtr) {
                    thePtr->mHeadPtr = 0;
                    thePtr = thePtr->mNextPtr;
                }
                inHeadPtr = 0;
            }
            ReadOp::Requests      mRequests;
        private:
            StRunningCompletion*  mNextPtr;
            StRunningCompletion** mHeadPtr;

            void CancelSelf(
                Striper& inStriper)
            {
                for (ReadOp::Requests::iterator theIt = mRequests.begin();
                        theIt != mRequests.end();
                        ++theIt) {
                    if (! theIt->mCancelFlag &&
                            inStriper.CanCancelRead(theIt->mStriperRequestId)) {
                        theIt->mCancelFlag = true;
                    }
                }
            }
        private:
            StRunningCompletion(
                StRunningCompletion& inCompl);
            StRunningCompletion& operator=(
                StRunningCompletion& inCompl);
        };

        Impl&                mOuter;
        ChunkServer          mChunkServer;
        ChunkServer*         mChunkServerPtr;
        int                  mErrorCode;
        int                  mRetryCount;
        Offset               mOpenChunkBlockFileOffset;
        time_t               mOpStartTime;
        GetAllocOp           mGetAllocOp;
        LeaseAcquireOp       mLeaseAcquireOp;
        LeaseRenewOp         mLeaseRenewOp;
        LeaseRelinquishOp    mLeaseRelinquishOp;
        SizeOp               mSizeOp;
        ChunkServerAccess    mChunkServerAccess;
        ChunkServerAccess    mChunkAccess;
        KfsOp*               mLastOpPtr;
        KfsOp*               mLastMetaOpPtr;
        size_t               mChunkServerIdx;
        time_t               mLeaseRenewTime;
        time_t               mLeaseExpireTime;
        time_t               mLeaseWaitStartTime;
        time_t               mChunkServerAccessExpires;
        int                  mLeaseRetryCount;
        bool                 mSleepingFlag;
        bool                 mClosingFlag;
        bool                 mChunkServerSetFlag;
        bool                 mNoCSAccessFlag;
        bool                 mStartReadRunningFlag;
        bool                 mRestartStartReadFlag;
        bool                 mSizeOpInFlightFlag;
        int64_t              mLeaseToRelinquish;
        string const         mLogPrefix;
        int                  mOpsNoRetryCount;
        bool*                mDeletedFlagPtr;
        StRunningCompletion* mRunningCompletionPtr;
        ReadOp*              mPendingQueue[1];
        ReadOp*              mInFlightQueue[1];
        ReadOp*              mCompletionQueue[1];
        ChunkReader*         mPrevPtr[1];
        ChunkReader*         mNextPtr[1];

        friend class QCDLListOp<ChunkReader, 0>;
        typedef QCDLListOp<ChunkReader, 0> ReadersListOp;

        bool CanRead()
            { return (! Queue::IsEmpty(mPendingQueue)); }
        bool IsMetaOp(
            const KfsOp* inOpPtr) const
        {
            return (
                inOpPtr == &mGetAllocOp ||
                inOpPtr == &mLeaseAcquireOp ||
                inOpPtr == &mLeaseRenewOp ||
                inOpPtr == &mLeaseRelinquishOp
            );
        }
        void CancelMetaOps()
        {
            if (! mLastMetaOpPtr) {
                return;
            }
            if (! mOuter.mMetaServer.Cancel(mLastMetaOpPtr, this)) {
                mOuter.InternalError("failed to cancel meta op");
            }
            mLastMetaOpPtr = 0;
        }
        void StartReadSelf()
        {
            if (mSleepingFlag) {
                return;
            }
            if (mErrorCode != 0) {
                mClosingFlag = false;
                return;
            }
            if (mClosingFlag && ! CanRead()) {
                if (! Queue::IsEmpty(mInFlightQueue)) {
                    return;
                }
                StopChunkServer();
                if (mLeaseAcquireOp.leaseId >= 0 &&
                        mLeaseAcquireOp.chunkId > 0) {
                    if (mLastOpPtr != &mLeaseRelinquishOp) {
                        CloseChunk();
                    }
                    return;
                }
                CancelMetaOps();
                mClosingFlag = false;
                mGetAllocOp.fileOffset  = -1;
                mGetAllocOp.chunkId     = -1;
                mLeaseAcquireOp.leaseId = -1;
                mChunkServerSetFlag     = false;
                ReportCompletion();
                return;
            }
            if (! CanRead()) {
                return;
            }
            if (mGetAllocOp.chunkId > 0 && mChunkServerSetFlag &&
                    WasChunkServerDisconnected()) {
                KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                    "detected chunk server disconnect: " <<
                        GetChunkServer().GetServerLocation() <<
                    " queue: " << (Queue::IsEmpty(mPendingQueue) ? "" : "not") <<
                        " empty" <<
                KFS_LOG_EOM;
                Reset();
                if (! CanRead()) {
                    return;
                }
            }
            // Return immediately after calling Read() and GetAlloc(), as
            // these can invoke completion. Completion, in turn, can delete
            // this.
            // Other methods of this class have to return immediately (unwind)
            // after invoking Read().
            if (mGetAllocOp.chunkId > 0) {
                Read();
            } else if (mGetAllocOp.status == kErrorNoEntry) {
                Done(mGetAllocOp, false, 0);
            } else if (! mLastOpPtr) {
                Reset();
                GetAlloc();
            }
        }
        void Read()
        {
            if (mLeaseAcquireOp.leaseId < 0 ||
                    mLeaseAcquireOp.chunkId != mGetAllocOp.chunkId ||
                    mLeaseExpireTime <= Now()) {
                GetLease();
                return;
            }
            QCStDeleteNotifier theDeleteNotifier(mDeletedFlagPtr);
            if (mLeaseRenewTime <= Now()) {
                RenewLease();
                if (theDeleteNotifier.IsDeleted() ||
                        &mLeaseRenewOp != mLastMetaOpPtr) {
                    return; // Unwind.
                }
                // OK read while renew is in flight, as long as the lease hasn't
                // expired yet.
            }
            if (! mChunkServerSetFlag) {
                QCASSERT(mChunkServerIdx < mGetAllocOp.chunkServers.size());
                const bool theCSClearTextAllowedFlag =
                    mOuter.IsChunkServerClearTextAllowed();
                mChunkServerSetFlag = true;
                mNoCSAccessFlag     = ! theCSClearTextAllowedFlag ||
                    ! mLeaseAcquireOp.allowCSClearTextFlag;
                const ServerLocation& theLocation =
                    mGetAllocOp.chunkServers[mChunkServerIdx];
                if (mOuter.mClientPoolPtr) {
                    mChunkServerPtr = &mOuter.mClientPoolPtr->Get(theLocation);
                } else {
                    mChunkServerPtr = &mChunkServer;
                }
                mChunkServerPtr->SetShutdownSsl(
                    mLeaseAcquireOp.allowCSClearTextFlag &&
                    theCSClearTextAllowedFlag
                );
                if (mChunkServerAccess.IsEmpty()) {
                    mChunkServerPtr->SetKey(0, 0, 0, 0);
                    mChunkServerPtr->SetAuthContext(0);
                    mSizeOp.access.clear();
                } else {
                    CryptoKeys::Key theKey;
                    const ChunkServerAccess::Entry* const thePtr =
                        mChunkServerAccess.Get(
                            theLocation,
                            mGetAllocOp.chunkId,
                            theKey
                        );
                    if (thePtr) {
                        mChunkServerPtr->SetKey(
                            thePtr->chunkServerAccessId.mPtr,
                            thePtr->chunkServerAccessId.mLen,
                            theKey.GetPtr(),
                            theKey.GetSize()
                        );
                        if (mChunkAccess.IsEmpty()) {
                            mSizeOp.access.assign(
                                thePtr->chunkAccess.mPtr,
                                thePtr->chunkAccess.mLen
                            );
                        } else {
                            mSizeOp.access = mChunkAccess.GetChunkAccess(
                                theLocation, mGetAllocOp.chunkId);
                        }
                        mNoCSAccessFlag = mSizeOp.access.empty();
                    } else {
                        mNoCSAccessFlag = true;
                    }
                    if (! mNoCSAccessFlag &&
                            ! mChunkServerPtr->GetAuthContext()) {
                        mChunkServerPtr->SetAuthContext(
                            mOuter.mMetaServer.GetAuthContext());
                    }
                }
                if (mNoCSAccessFlag) {
                    mChunkServer.SetKey(0, 0, 0, 0);
                    mChunkServer.SetAuthContext(0);
                    mSizeOp.access.clear();
                } else if (&mChunkServer == mChunkServerPtr) {
                    mChunkServer.SetServer(theLocation);
                }
                KFS_LOG_STREAM(mNoCSAccessFlag ?
                        MsgLogger::kLogLevelERROR :
                        MsgLogger::kLogLevelDEBUG) << mLogPrefix <<
                    "chunk: "      << mGetAllocOp.chunkId <<
                    " cs access: " << (! mChunkServerAccess.IsEmpty()) <<
                    " access: "    << mSizeOp.access <<
                    " cleartext: " << mChunkServerPtr->IsShutdownSsl() <<
                    " allowed: "   << mLeaseAcquireOp.allowCSClearTextFlag <<
                    " CS access expires in: " <<
                        (mChunkServerAccessExpires - Now()) <<
                    " server:"     << theLocation <<
                    " idx: "       << mChunkServerIdx <<
                KFS_LOG_EOM;
            }
            if (mSizeOp.size < 0) {
                if (! mSizeOpInFlightFlag) {
                    GetChunkSize();
                }
                return;
            }
            Queue::Iterator theIt(mPendingQueue);
            ReadOp* theOpPtr;
            while ( ! mRestartStartReadFlag &&
                    ! mSleepingFlag &&
                    mErrorCode == 0 &&
                    mGetAllocOp.chunkId > 0 &&
                    (theOpPtr = theIt.Next())) {
                Read(*theOpPtr);
                if (theDeleteNotifier.IsDeleted()) {
                    return; // Unwind.
                }
            }
        }
        void GetAlloc()
        {
            QCASSERT(mGetAllocOp.fileOffset >= 0 && mGetAllocOp.fid > 0);
            Reset(mGetAllocOp);
            mGetAllocOp.chunkServers.clear();
            mGetAllocOp.serversOrderedFlag = false;
            EnqueueMeta(mGetAllocOp);
        }
        void Done(
            GetAllocOp& inOp,
            bool        inCanceledFlag,
            IOBuffer*   inBufferPtr)
        {
            QCASSERT(&inOp == &mGetAllocOp && ! inBufferPtr);
            if (inCanceledFlag) {
                return;
            }
            if (inOp.status == kErrorNoEntry) {
                // Fail all ops.
                inOp.chunkId = -1;
                ReportCompletionForAll(inOp.status);
                return;
            }
            if (inOp.status != 0 || mGetAllocOp.chunkServers.empty()) {
                mGetAllocOp.chunkId = -1;
                HandleError(inOp);
                return;
            }
            if (! mGetAllocOp.serversOrderedFlag) {
                random_shuffle(
                    mGetAllocOp.chunkServers.begin(),
                    mGetAllocOp.chunkServers.end()
                );
            }
            mChunkServerIdx = 0;
            StartRead();
        }
        void GetLease()
        {
            QCASSERT(mGetAllocOp.fileOffset >= 0 && mGetAllocOp.fid > 0);
            if (mLeaseAcquireOp.chunkId != mGetAllocOp.chunkId ||
                    mLeaseAcquireOp.status != kErrorBusy) {
                mLeaseWaitStartTime = Now();
                mLeaseRetryCount = 0;
            }
            CancelMetaOps();
            Reset(mLeaseAcquireOp);
            mLeaseAcquireOp.chunkId  = mGetAllocOp.chunkId;
            mLeaseAcquireOp.pathname = mGetAllocOp.filename.c_str();
            mLeaseAcquireOp.leaseId  = -1;
            mLeaseAcquireOp.chunkAccessCount              = 0;
            mLeaseAcquireOp.chunkServerAccessValidForTime = 0;
            mLeaseAcquireOp.chunkServerAccessIssuedTime   = 0;
            mLeaseAcquireOp.allowCSClearTextFlag          = false;
            mLeaseExpireTime = Now() + LEASE_INTERVAL_SECS;
            mLeaseRenewTime  = Now() + (LEASE_INTERVAL_SECS + 1) / 2;
            mOuter.mStats.mGetLeaseCount++;
            mChunkAccess.Clear();
            mChunkServerAccess.Clear();
            mSizeOp.access.clear();
            EnqueueMeta(mLeaseAcquireOp);
        }
        void Done(
            LeaseAcquireOp& inOp,
            bool            inCanceledFlag,
            IOBuffer*       inBufferPtr)
        {
            QCASSERT(&inOp == &mLeaseAcquireOp && ! inBufferPtr);
            if (inCanceledFlag) {
                return;
            }
            if (inOp.status == 0 && mLeaseExpireTime < Now()) {
                inOp.status = kErrorLeaseExpired;
            }
            if (inOp.status == 0 &&
                    (inOp.chunkAccessCount <= 0 ||
                        inOp.chunkServerAccessValidForTime <= 0) &&
                    mOuter.IsAuthEnabled()) {
                inOp.status    = kErrorPermissions;
                inOp.statusMsg = "no chunk server access with lease response";
            }
            if (inOp.status == 0 && 0 < inOp.chunkAccessCount) {
                const bool         kHasChunkServerAccessFlag = true;
                const int          kBufPos                   = 0;
                const bool         kOwnsBufferFlag           = true;
                const kfsChunkId_t kChunkId                  = -1;
                const int          theRet = mChunkServerAccess.Parse(
                    inOp.chunkAccessCount,
                    kHasChunkServerAccessFlag,
                    kChunkId,
                    inOp.contentBuf,
                    kBufPos,
                    inOp.contentLength,
                    kOwnsBufferFlag
                );
                inOp.ReleaseContentBuf();
                if (theRet < 0) {
                    inOp.status    = theRet;
                    inOp.statusMsg = "invalid chunk access response";
                } else if (0 < inOp.chunkServerAccessValidForTime) {
                    mChunkServerAccessExpires =
                        inOp.chunkServerAccessIssuedTime +
                        inOp.chunkServerAccessValidForTime -
                        LEASE_INTERVAL_SECS;
                }
            }
            if (inOp.status != 0) {
                mLeaseAcquireOp.leaseId = -1;
                mLeaseRenewTime  = Now() - 1;
                mLeaseExpireTime = mLeaseRenewTime;
                HandleError(inOp);
                return;
            }
            if (0 <= mLeaseToRelinquish && 0 <= mLeaseAcquireOp.chunkId) {
                mOuter.mStats.mMetaOpsQueuedCount++;
                const int64_t theLeaseId = mLeaseToRelinquish;
                mLeaseToRelinquish = -1;
                // Tell meta server to relinquish the previous lease.
                mOuter.mMetaServer.Enqueue(new LeaseRelinquishOp(
                    0, mLeaseAcquireOp.chunkId, theLeaseId), 0);
            }
            StartRead();
        }
        void RenewLease()
        {
            QCASSERT(
                mGetAllocOp.fileOffset >= 0 &&
                mGetAllocOp.fid > 0 &&
                mGetAllocOp.chunkId > 0 &&
                mLeaseAcquireOp.leaseId >= 0 &&
                (! mLastMetaOpPtr || mLastMetaOpPtr == &mLeaseRenewOp)
            );
            CancelMetaOps();
            Reset(mLeaseRenewOp);
            const time_t theNow = Now();
            mLeaseRenewOp.chunkId         = mLeaseAcquireOp.chunkId;
            mLeaseRenewOp.pathname        = mGetAllocOp.filename.c_str();
            mLeaseRenewOp.leaseId         = mLeaseAcquireOp.leaseId;
            mLeaseRenewOp.getCSAccessFlag = ! mChunkServerAccess.IsEmpty() &&
                mChunkServerAccessExpires <= theNow;
            mLeaseRenewOp.chunkAccessCount              = 0;
            mLeaseRenewOp.chunkServerAccessValidForTime = 0;
            mLeaseRenewOp.chunkServerAccessIssuedTime   = 0;
            mLeaseRenewOp.allowCSClearTextFlag          = false;
            mLeaseExpireTime = theNow + LEASE_INTERVAL_SECS;
            mLeaseRenewTime  = theNow + (LEASE_INTERVAL_SECS + 1) / 2;
            EnqueueMeta(mLeaseRenewOp);
        }
        void Done(
            LeaseRenewOp& inOp,
            bool          inCanceledFlag,
            IOBuffer*     inBufferPtr)
        {
            QCASSERT(&inOp == &mLeaseRenewOp && ! inBufferPtr);
            if (inCanceledFlag) {
                return;
            }
            if (inOp.status == 0 &&
                    ((inOp.chunkAccessCount <= 0 && mOuter.IsAuthEnabled()) ||
                    (mLeaseRenewOp.getCSAccessFlag &&
                        inOp.chunkServerAccessValidForTime <= 0))) {
                inOp.status    = kErrorPermissions;
                inOp.statusMsg =
                    "no chunk or chunk server access with lease response";
            }
            if (inOp.status == 0 && 0 < inOp.chunkAccessCount) {
                mChunkAccess.Clear();
                const bool         theHasChunkServerAccessFlag =
                    0 < inOp.chunkServerAccessValidForTime;
                const int          kBufPos                     = 0;
                const bool         kOwnsBufferFlag             = true;
                ChunkServerAccess& theAccess                   =
                    theHasChunkServerAccessFlag ?
                        mChunkServerAccess : mChunkAccess;
                const int  theRet                              =
                    theAccess.Parse(
                        inOp.chunkAccessCount,
                        theHasChunkServerAccessFlag,
                        inOp.chunkId,
                        inOp.contentBuf,
                        kBufPos,
                        inOp.contentLength,
                        kOwnsBufferFlag
                    );
                inOp.ReleaseContentBuf();
                if (theRet < 0) {
                    inOp.status    = theRet;
                    inOp.statusMsg = "invalid chunk access response";
                } else if (theHasChunkServerAccessFlag) {
                    mChunkServerAccessExpires =
                        inOp.chunkServerAccessIssuedTime +
                        inOp.chunkServerAccessValidForTime -
                        LEASE_INTERVAL_SECS;
                }
                if (inOp.status == 0 && mChunkServerPtr) {
                    string theChunkAccess = theAccess.GetChunkAccess(
                        mChunkServerPtr->GetServerLocation(), inOp.chunkId);
                    // If chunk server disappeared / down let the read to
                    // to discover that.
                    if (theChunkAccess.empty()) {
                        inOp.status    = kErrorParameters;
                        inOp.statusMsg = "chunk access: no such location";
                    } else {
                        mSizeOp.access.swap(theChunkAccess);
                        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                            "chunk: "    << mSizeOp.chunkId <<
                            " update"
                            " access: "  << mSizeOp.access <<
                            " CS access:"
                            " updated: " << theHasChunkServerAccessFlag <<
                            " cleartext: "  << mChunkServerPtr->IsShutdownSsl() <<
                            " allowed: "    << inOp.allowCSClearTextFlag <<
                            " expires in: " <<
                                (mChunkServerAccessExpires - Now()) <<
                        KFS_LOG_EOM;
                        if (! inOp.allowCSClearTextFlag &&
                                mChunkServerPtr->IsShutdownSsl()) {
                            KFS_LOG_STREAM_INFO << mLogPrefix <<
                                "chunk: " << mSizeOp.chunkId <<
                                " cleartext is no longer allowed" <<
                            KFS_LOG_EOM;
                            // Clear lease acquire flag to ensure that the
                            // change has effect on other chunk servers.
                            mLeaseAcquireOp.allowCSClearTextFlag = false;
                            mChunkServerPtr->SetShutdownSsl(false);
                        }
                    }
                }
            }
            if (inOp.status == 0 &&
                    ! mSizeOp.access.empty() && inOp.chunkAccessCount <= 0) {
                inOp.status    = kErrorParameters;
                inOp.statusMsg = "no chunk acces";
            }
            if (inOp.status != 0) {
                mLeaseAcquireOp.leaseId = -1;
                mLeaseRenewOp.leaseId   = -1;
                mLeaseRenewTime  = Now() - 1;
                mLeaseExpireTime = mLeaseRenewTime;
                HandleError(inOp);
                return;
            }
            if (Queue::IsEmpty(mInFlightQueue)) {
                StartRead();
            }
        }
        void Read(
            ReadOp& inReadOp)
        {
            QCASSERT(
                mGetAllocOp.fileOffset >= 0 &&
                mGetAllocOp.fid > 0 &&
                mGetAllocOp.chunkId > 0 &&
                mLeaseAcquireOp.leaseId >= 0 &&
                mSizeOp.size >= 0
            );
            Reset(inReadOp);
            inReadOp.mTmpBuffer.Clear();
            // Use tmp buffer until the op passes checksum verification to use
            // the same buffers with retries.
            inReadOp.mTmpBuffer.UseSpaceAvailable(
                &inReadOp.mBuffer, inReadOp.numBytes);
            inReadOp.chunkId      = mGetAllocOp.chunkId;
            inReadOp.chunkVersion = mGetAllocOp.chunkVersion;
            inReadOp.mOpStartTime = Now();
            Queue::Remove(mPendingQueue, inReadOp);
            Queue::PushBack(mInFlightQueue, inReadOp);
            if (inReadOp.offset >= mSizeOp.size) {
                QCASSERT(inReadOp.offset + inReadOp.numBytes <= CHUNKSIZE);
                // Read past end of chunk.
                inReadOp.status    = 0;
                inReadOp.statusMsg = "read offset past end of chunk";
                Done(inReadOp, false, &inReadOp.mTmpBuffer);
                return;
            }
            inReadOp.access = mSizeOp.access;
            mOuter.mStats.mOpsReadCount++;
            Enqueue(inReadOp, &inReadOp.mTmpBuffer);
        }
        void Done(
            ReadOp&   inOp,
            bool      inCanceledFlag,
            IOBuffer* inBufferPtr)
        {
            QCASSERT(
                inBufferPtr == &inOp.mTmpBuffer &&
                Queue::IsInList(mInFlightQueue, inOp)
            );
            if (inOp.status == kErrorNoEntry &&
                    mGetAllocOp.status != kErrorNoEntry) {
                inOp.status = kErrorIO;
            }
            if (inCanceledFlag || inOp.status < 0 || ! VerifyChecksum(inOp) ||
                    ! VerifyRead(inOp)) {
                Queue::Remove(mInFlightQueue, inOp);
                Queue::PushBack(mPendingQueue, inOp);
                inOp.mTmpBuffer.Clear();
                if (inCanceledFlag) {
                    return;
                }
                mOpStartTime = inOp.mOpStartTime;
                if (! inOp.mRetryIfFailsFlag && inOp.status != kErrorChecksum &&
                        mChunkServerIdx + 1 >=
                            mGetAllocOp.chunkServers.size()) {
                    if (ReportCompletion(inOp, mPendingQueue)) {
                        StartRead();
                    }
                } else {
                    HandleError(inOp);
                }
                return;
            }
            const int theDoneCount = (int)inOp.contentLength;
            QCASSERT(
                theDoneCount >= 0 &&
                theDoneCount <= inOp.mTmpBuffer.BytesConsumable() &&
                inOp.contentLength <= inOp.numBytes
            );
            mOuter.mStats.mReadCount++;
            mOuter.mStats.mReadByteCount += theDoneCount;
            if (theDoneCount < inOp.mTmpBuffer.BytesConsumable()) {
                // Move available space, if any, to the end of the short read.
                IOBuffer theBuf;
                theBuf.MoveSpaceAvailable(&inOp.mBuffer, theDoneCount);
                inOp.mTmpBuffer.RemoveSpaceAvailable();
                inOp.mTmpBuffer.Trim(theDoneCount);
                inOp.mTmpBuffer.MoveSpaceAvailable(
                    &inOp.mBuffer, (int)inOp.numBytes - theDoneCount);
            }
            inOp.mBuffer.RemoveSpaceAvailable();
            inOp.mBuffer.Move(&inOp.mTmpBuffer);
            QCASSERT(theDoneCount == inOp.mBuffer.BytesConsumable());
            if (ReportCompletion(inOp, mInFlightQueue)) {
                StartRead();
            }
        }
        bool ReportCompletion(
            ReadOp&  inOp,
            ReadOp** inQueuePtr)
        {
            IOBuffer theBuffer;
            theBuffer.Move(&inOp.mBuffer);
            StRunningCompletion theCompl(mRunningCompletionPtr, inOp.mRequests);
            const int       theSize             = (int)inOp.numBytes;
            const RequestId theRequestId        = inOp.mRequestId;
            const RequestId theStriperRequestId = inOp.mStriperRequestId;
            const int       theStatus           = min(0, inOp.status);
            Offset          theOffset = mGetAllocOp.fileOffset + inOp.offset;
            if (mOpsNoRetryCount > 0 &&
                    ! inOp.mRetryIfFailsFlag &&
                    (inQueuePtr == mPendingQueue ||
                    inQueuePtr == mInFlightQueue)) {
                mOpsNoRetryCount--;
            }
            inOp.Delete(inQueuePtr);
            if (theCompl.mRequests.empty()) {
                if (! ReportCompletion(
                        theStatus,
                        theOffset,
                        theSize,
                        &theBuffer,
                        theRequestId,
                        theStriperRequestId)) {
                    return false;
                }
            } else {
                for (ReadOp::Requests::iterator
                        theIt = theCompl.mRequests.begin();
                        theIt != theCompl.mRequests.end();
                        ++theIt) {
                    IOBuffer theReqBuf;
                    theReqBuf.MoveSpace(&theBuffer, theIt->mSize);
                    if (! theIt->mCancelFlag) {
                        theIt->mCancelFlag = true;
                        if (! ReportCompletion(
                                theStatus,
                                theOffset,
                                theIt->mSize,
                                &theReqBuf,
                                theIt->mRequestId,
                                theIt->mStriperRequestId)) {
                            return false;
                        }
                    }
                    theOffset += theIt->mSize;
                }
            }
            return true;
        }
        bool VerifyChecksum(
            ReadOp& inOp)
        {
            if (inOp.contentLength <= 0 && inOp.checksums.empty()) {
                return true;
            }
            if (inOp.skipVerifyDiskChecksumFlag) {
                vector<uint32_t>::const_iterator const theOpEndIt =
                    inOp.checksums.end();
                vector<uint32_t>::const_iterator       theOpIt    =
                    inOp.checksums.begin();
                IOBuffer::iterator const theEndIt     = inOp.mTmpBuffer.end();
                IOBuffer::iterator       theIt        = inOp.mTmpBuffer.begin();
                int                      theTLen      = inOp.contentLength;
                const char*              thePtr       = 0;
                const char*              theEndPtr    = 0;
                size_t                   theIdx       = 0;
                uint32_t                 theChecksum  = kKfsNullChecksum;
                bool                     theErrorFlag = false;
                int                      theLen       = min(theTLen,
                    (int)(CHECKSUM_BLOCKSIZE -
                        inOp.offset % CHECKSUM_BLOCKSIZE));
                while (0 < theLen) {
                    theChecksum = kKfsNullChecksum;
                    int theRem = theLen;
                    for ( ; ; ) {
                        if (theEndPtr <= thePtr) {
                            if (theIt == theEndIt) {
                                break;
                            }
                            thePtr    = theIt->Consumer();
                            theEndPtr = theIt->Producer();
                            ++theIt;
                        }
                        const int theBLen =
                            min((int)(theEndPtr - thePtr), theRem);
                        if (theBLen <= 0) {
                            continue;
                        }
                        theChecksum = ComputeBlockChecksum(
                            theChecksum, thePtr, (size_t)theBLen);
                        thePtr += theBLen;
                        if ((theRem -= theBLen) <= 0) {
                            break;
                        }
                    }
                    if (theRem < theLen) {
                        if (theOpEndIt == theOpIt ||
                                theChecksum != *theOpIt) {
                            theErrorFlag = true;
                            break;
                        }
                        ++theIdx;
                        ++theOpIt;
                    }
                    if (0 < theRem) {
                        break;
                    }
                    theTLen -= theLen;
                    theLen = min(theTLen, (int)CHECKSUM_BLOCKSIZE);
                }
                if (! theErrorFlag && theOpEndIt == theOpIt) {
                    return true;
                }
                KFS_LOG_STREAM_ERROR << mLogPrefix <<
                    "checksum vector mismatch:"
                    " chunk: "    << inOp.chunkId <<
                    " version: "  << inOp.chunkVersion <<
                    " offset: "   << inOp.offset <<
                    " length: "   << inOp.contentLength <<
                    " idx: "      << theIdx <<
                    " got: "      << inOp.checksums.size() <<
                    " expect: "   << (theErrorFlag ?
                        (int64_t)theChecksum : int64_t(-1)) <<
                    " got: "      << (theOpEndIt != theOpIt ?
                        (int64_t)*theOpIt : int64_t(-1)) <<
                KFS_LOG_EOM;
                inOp.status    = kErrorChecksum;
                inOp.statusMsg = "received checksum mismatch";
                return false;
            }
            vector<uint32_t> const theChecksums =
                    ComputeChecksums(&inOp.mTmpBuffer, inOp.contentLength);
            if (theChecksums == inOp.checksums) {
                return true;
            }
            if (theChecksums.size() != inOp.checksums.size()) {
                KFS_LOG_STREAM_ERROR << mLogPrefix <<
                    "checksum vector length mismatch:"
                    " chunk: "    << inOp.chunkId <<
                    " version: "  << inOp.chunkVersion <<
                    " offset: "   << inOp.offset <<
                    " length: "   << inOp.contentLength <<
                    " expected: " << theChecksums.size() <<
                    " got: "      << inOp.checksums.size() <<
                KFS_LOG_EOM;
            } else {
                for (size_t i = 0; i < theChecksums.size(); i++) {
                    if (inOp.checksums[i] == theChecksums[i]) {
                        continue;
                    }
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "checksum mismatch:"
                        " chunk: "    << inOp.chunkId <<
                        " version: "  << inOp.chunkVersion <<
                        " offset: "   << inOp.offset <<
                            "+"     << i << "*" << CHECKSUM_BLOCKSIZE <<
                        " expected: " << theChecksums[i] <<
                        " got: "      << inOp.checksums[i] <<
                    KFS_LOG_EOM;
                }
            }
            inOp.status    = kErrorChecksum;
            inOp.statusMsg = "received checksum mismatch";
            return false;
        }
        bool VerifyRead(
            ReadOp& inOp)
        {
            if (inOp.status < 0) {
                return false;
            }
            QCRTASSERT(
                inOp.mTmpBuffer.BytesConsumable() == (int)inOp.contentLength);
            if (inOp.numBytes < inOp.contentLength) {
                inOp.status    = kErrorParameters;
                inOp.statusMsg = "read returned more than requested";
            }
            const bool theShortReadExpectedFlag = 0 <= inOp.status &&
                inOp.offset + (Offset)inOp.numBytes > mSizeOp.size;
            if (0 <= inOp.status) {
                if ((! inOp.mFailShortReadFlag && theShortReadExpectedFlag) ||
                        inOp.contentLength >= inOp.numBytes) {
                    return true;
                }
                if (theShortReadExpectedFlag) {
                    inOp.status    = kErrorInvalChunkSize;
                    inOp.statusMsg = "short read detected";
                } else {
                    inOp.status    = kErrorIO;
                    inOp.statusMsg = "incomplete read detected";
                }
            }
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                inOp.statusMsg << ":"
                " chunk: "     << inOp.chunkId <<
                " version: "   << inOp.chunkVersion <<
                " server: "    << GetChunkServer().GetServerLocation() <<
                " pos: "       << inOp.offset <<
                " requested: " << inOp.numBytes <<
                " returned: "  << inOp.contentLength <<
                " size: "      << mSizeOp.size <<
            KFS_LOG_EOM;
            if (inOp.chunkId > 0 && theShortReadExpectedFlag) {
                // Report short chunk to the meta server.
                mOuter.ReportInvalidChunk(inOp.chunkId, inOp.chunkVersion,
                    kErrorInvalChunkSize, inOp.statusMsg.c_str());
            }
            return false;
        }
        void CloseChunk()
        {
            QCASSERT(
                mLeaseAcquireOp.chunkId > 0 &&
                mLeaseAcquireOp.leaseId >= 0 &&
                &mLeaseRelinquishOp != mLastMetaOpPtr
            );
            // Cancel in flight lease renew if any.
            CancelMetaOps();
            Reset(mLeaseRelinquishOp);
            mLeaseRelinquishOp.chunkId = mLeaseAcquireOp.chunkId;
            mLeaseRelinquishOp.leaseId = mLeaseAcquireOp.leaseId;
            mLeaseAcquireOp.leaseId = -1;
            EnqueueMeta(mLeaseRelinquishOp);
        }
        void Done(
            LeaseRelinquishOp&  inOp,
            bool                inCanceledFlag,
            IOBuffer*           inBufferPtr)
        {
            QCASSERT(&mLeaseRelinquishOp == &inOp && ! inBufferPtr);
            if (inCanceledFlag) {
                return;
            }
            if (inOp.status != 0) {
                KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                    "lease relinquish failure, status: " << inOp.status <<
                    " " << inOp.statusMsg <<
                    " ignored" <<
                KFS_LOG_EOM;
            }
            Reset();
            mLeaseRenewOp.leaseId   = -1;
            mLeaseAcquireOp.leaseId = -1;
            mLeaseAcquireOp.chunkId = -1;
            mGetAllocOp.fileOffset  = -1;
            StartRead();
        }
        void GetChunkSize()
        {
            QCASSERT(
                mGetAllocOp.chunkId > 0 &&
                mLeaseAcquireOp.chunkId > 0 &&
                mLeaseAcquireOp.leaseId >= 0 &&
                ! mSizeOpInFlightFlag
            );
            Reset(mSizeOp);
            mSizeOp.chunkId      = mGetAllocOp.chunkId;
            mSizeOp.chunkVersion = mGetAllocOp.chunkVersion;
            mSizeOpInFlightFlag  = true;
            Enqueue(mSizeOp);
        }
        void Done(
            SizeOp&   inOp,
            bool      inCanceledFlag,
            IOBuffer* inBufferPtr)
        {
            QCASSERT(&mSizeOp == &inOp && ! inBufferPtr && mSizeOpInFlightFlag);
            mSizeOpInFlightFlag = false;
            if (inCanceledFlag) {
                return;
            }
            if (inOp.status != 0) {
                HandleError(inOp);
                return;
            }
            StartRead();
        }
        virtual void OpDone(
            KfsOp*    inOpPtr,
            bool      inCanceledFlag,
            IOBuffer* inBufferPtr)
        {
            if (inOpPtr) {
                KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                    "<- " << (inCanceledFlag ? "canceled " : "") <<
                    inOpPtr->Show() <<
                    " status: " << inOpPtr->status <<
                    " msg: "    << inOpPtr->statusMsg <<
                    " seq: "    << inOpPtr->seq <<
                    " len: "    << inOpPtr->contentLength <<
                    " buffer: " << static_cast<const void*>(inBufferPtr) <<
                    "/" << (inBufferPtr ? inBufferPtr->BytesConsumable() : 0) <<
                KFS_LOG_EOM;
            } else {
                KFS_LOG_STREAM_ERROR << mLogPrefix <<
                    "<- " << (inCanceledFlag ? "canceled " : "") <<
                    "NULL operation completion?" <<
                    " buffer: " << static_cast<const void*>(inBufferPtr) <<
                    "/" << (inBufferPtr ? inBufferPtr->BytesConsumable() : 0) <<
                KFS_LOG_EOM;
            }
            if (inCanceledFlag && IsMetaOp(inOpPtr)) {
                mOuter.mStats.mMetaOpsCancelledCount++;
            }
            if (mLastOpPtr == inOpPtr) {
                mLastOpPtr = 0;
            }
            if (mLastMetaOpPtr == inOpPtr) {
                mLastMetaOpPtr = 0;
            }
            if (&mGetAllocOp == inOpPtr) {
                Done(mGetAllocOp, inCanceledFlag, inBufferPtr);
            } else if (&mLeaseAcquireOp == inOpPtr) {
                Done(mLeaseAcquireOp, inCanceledFlag, inBufferPtr);
            } else if (&mLeaseRenewOp == inOpPtr) {
                Done(mLeaseRenewOp, inCanceledFlag, inBufferPtr);
            } else if (&mLeaseRelinquishOp == inOpPtr) {
                Done(mLeaseRelinquishOp, inCanceledFlag, inBufferPtr);
            } else if (&mSizeOp == inOpPtr) {
                Done(mSizeOp, inCanceledFlag, inBufferPtr);
            } else if (inOpPtr && inOpPtr->op == CMD_READ) {
                Done(*static_cast<ReadOp*>(inOpPtr),
                    inCanceledFlag, inBufferPtr);
            } else {
                mOuter.InternalError("unexpected operation completion");
            }
        }
        void Enqueue(
            KfsOp&    inOp,
            IOBuffer* inBufferPtr = 0)
        {
            if (mNoCSAccessFlag) {
                inOp.status    = kErrorParameters;
                inOp.statusMsg = "no chunk server key";
                OpDone(&inOp, false, inBufferPtr);
            } else {
                EnqueueSelf(inOp, inBufferPtr,
                    mChunkServerPtr ? mChunkServerPtr : &mChunkServer);
            }
        }
        void EnqueueMeta(
            KfsOp&    inOp,
            IOBuffer* inBufferPtr = 0)
            { EnqueueSelf(inOp, inBufferPtr, 0); }
        void Reset()
        {
            CancelMetaOps();
            mLastOpPtr = 0;
            StopChunkServer();
            mChunkServerSetFlag = false;
            QCASSERT(Queue::IsEmpty(mInFlightQueue));
            if (mSleepingFlag) {
                mOuter.mNetManager.UnRegisterTimeoutHandler(this);
                mSleepingFlag = false;
            }
        }
        static void Reset(
            KfsOp& inOp)
        {
            inOp.seq           = 0;
            inOp.status        = 0;
            inOp.statusMsg.clear();
            inOp.checksum      = 0;
            inOp.contentLength = 0;
            inOp.DeallocContentBuf();
        }
        int GetTimeToNextRetry() const
        {
            return max(mRetryCount > 1 ? 1 : 0,
                mOuter.mTimeSecBetweenRetries - int(Now() - mOpStartTime));
        }
        void HandleError(
            KfsOp& inOp)
        {
            ostringstream theOStream;
            inOp.Request(theOStream);
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "operation"
                " failure, seq: "         << inOp.seq       <<
                " status: "               << inOp.status    <<
                " msg: "                  << inOp.statusMsg <<
                " op: "                   << inOp.Show()    <<
                " current chunk server: " << (mChunkServerSetFlag ?
                        GetChunkServer().GetServerLocation() :
                        ServerLocation()) <<
                " chunkserver: "          << (GetChunkServer().IsDataSent() ?
                    (GetChunkServer().IsAllDataSent() ? "all" : "partial") :
                    "no") << " data sent" <<
                "\nRequest:\n"            << theOStream.str() <<
            KFS_LOG_EOM;
            int  theTimeToNextRetry          = 0;
            bool theFailFlag                 = false;
            bool theReadLeaseOtherFalureFlag = false;
            if ((&mLeaseRenewOp == &inOp || &mLeaseAcquireOp == &inOp) &&
                    ! (theReadLeaseOtherFalureFlag =
                        inOp.status != kErrorBusy &&
                        inOp.status != kErrorLeaseExpired &&
                        inOp.status != KfsNetClient::kErrorMaxRetryReached)) {
                mOuter.mStats.mGetLeaseRetryCount++;
                mLeaseRetryCount++;
                theTimeToNextRetry = max(1, min(
                    mOuter.mLeaseRetryTimeout * max(1, mLeaseRetryCount) -
                    int(Now() - mOpStartTime),
                    int(mLeaseWaitStartTime + mOuter.mLeaseWaitTimeout - Now())
                ));
                // Meta ops communication failures are automatically
                // retried, declare failure if it isn't lease busy error.
                theFailFlag = (inOp.status != kErrorBusy &&
                    (&mLeaseRenewOp != &inOp ||
                        inOp.status != kErrorLeaseExpired)) ||
                    mLeaseWaitStartTime + mOuter.mLeaseWaitTimeout <= Now();
            } else if (&mGetAllocOp == &inOp) {
                if (inOp.status == kErrorTryAgain) {
                    // No servers with this chunk available.
                    // Read should not be in flight, as the chunk id has not
                    // been determined yet.
                    QCASSERT(Queue::IsEmpty(mInFlightQueue));
                    if (++mRetryCount >= mOuter.mMaxGetAllocRetryCount) {
                        theFailFlag = true;
                    } else {
                        theTimeToNextRetry = GetTimeToNextRetry();
                    }
                } else {
                    // Meta ops communication failures are automatically
                    // retried, declare failure.
                    // Either chunk does not exists, or meta comm. failure has
                    // been declared.
                    theFailFlag = true;
                }
            } else {
                mOuter.mStats.mRetriesCount++;
                if (inOp.op == CMD_READ) {
                    mOuter.mStats.mReadErrorsCount++;
                    if (inOp.status == kErrorChecksum) {
                        mOuter.mStats.mReadChecksumErrorsCount++;
                    }
                }
                bool thePossibleDiskCheckusmErrorFlag = false;
                if (inOp.op == CMD_READ && inOp.status == kErrorChecksum) {
                    ReadOp& theReadOp = static_cast<ReadOp&>(inOp);
                    thePossibleDiskCheckusmErrorFlag =
                        theReadOp.skipVerifyDiskChecksumFlag;
                    theReadOp.skipVerifyDiskChecksumFlag = false;
                }
                if (! thePossibleDiskCheckusmErrorFlag) {
                    if (inOp.op == CMD_READ || &mSizeOp == &inOp ||
                            theReadLeaseOtherFalureFlag) {
                        if (theReadLeaseOtherFalureFlag ||
                                ++mChunkServerIdx >=
                                    mGetAllocOp.chunkServers.size() ||
                                (! mSizeOp.access.empty() &&
                                    inOp.status == kErrorPermissions)) {
                            mChunkServerIdx = 0;
                            if (inOp.op != CMD_READ ||
                                    inOp.status != kErrorChecksum) {
                                theTimeToNextRetry = GetTimeToNextRetry();
                            }
                            mRetryCount++;
                            // Restart from get alloc, chunk might have been
                            // moved or re-replicated.
                            mGetAllocOp.status  = 0;
                            mGetAllocOp.chunkId = -1;
                            if (mNoCSAccessFlag ||
                                    ! mSizeOp.access.empty() ||
                                    ! mChunkServerAccess.IsEmpty() ||
                                    ! mChunkAccess.IsEmpty()) {
                                // Start from lease acquire, relinquish current
                                // lease only after lease acquisition.
                                if (mLeaseToRelinquish < 0) {
                                    mLeaseToRelinquish = mLeaseAcquireOp.leaseId;
                                }
                                mLeaseAcquireOp.leaseId = -1;
                                mSizeOp.access.clear();
                            }
                            mChunkAccess.Clear();
                            mChunkServerAccess.Clear();
                        }
                        // Always restart from get chunk size, [first] read
                        // failure might imply that reported chunk size wasn't
                        // valid.
                        // Chunk servers don't initially load chunk headers,
                        // instead stat() system call is used to compute chunk
                        // size.
                        mSizeOp.size = -1;
                    } else {
                        theTimeToNextRetry = GetTimeToNextRetry();
                    }
                }
                // Do not increment retry count with possible disk checksum
                // failure.
            }
            if (mRetryCount >= mOuter.mMaxRetryCount || theFailFlag) {
                KFS_LOG_STREAM_ERROR << mLogPrefix <<
                    "giving up, retry count: " << mRetryCount <<
                KFS_LOG_EOM;
                // Fail all ops.
                ReportCompletionForAll(
                    inOp.status < 0 ? inOp.status : kErrorIO);
                return;
            }
            if (&mGetAllocOp == &inOp || &mSizeOp == &inOp ||
                    theReadLeaseOtherFalureFlag) {
                if (! ReportCompletionForPendingWithNoRetryOnly(
                        inOp.status < 0 ? inOp.status : kErrorIO)) {
                    return; // Unwind.
                }
                if (Queue::IsEmpty(mPendingQueue) &&
                        Queue::IsEmpty(mInFlightQueue)) {
                    Reset();
                    mRetryCount = 0;
                    StartRead();
                    return;
                }
            }
            // Retry.
            KFS_LOG_STREAM_INFO << mLogPrefix <<
                "scheduling retry: " << mRetryCount <<
                " of "  << mOuter.mMaxRetryCount <<
                " in "  << theTimeToNextRetry << " sec." <<
                " op: " << inOp.Show() <<
            KFS_LOG_EOM;
            mErrorCode = 0;
            Reset();
            Sleep(theTimeToNextRetry);
            if (! mSleepingFlag) {
               Timeout();
            }
        }
        bool ReportCompletionForPendingWithNoRetryOnly(
            int inStatus)
        {
            if (mOpsNoRetryCount <= 0) {
                return true;
            }
            mOpsNoRetryCount = 0;
            Queue::Iterator theIt(mPendingQueue);
            ReadOp* theOpPtr;
            while ((theOpPtr = theIt.Next())) {
                if (! theOpPtr->mRetryIfFailsFlag) {
                    Queue::Remove(mPendingQueue, *theOpPtr);
                    Queue::PushBack(mCompletionQueue, *theOpPtr);
                }
            }
            return RunCompletionQueue(inStatus);
        }
        bool ReportCompletionForAll(
            int inStatus)
        {
            Reset();
            QCRTASSERT(Queue::IsEmpty(mInFlightQueue));
            mOpsNoRetryCount = 0;
            Queue::PushBackList(mCompletionQueue, mPendingQueue);
            if (Queue::IsEmpty(mCompletionQueue)) {
                return ReportCompletion();
            }
            return RunCompletionQueue(inStatus);
        }
        bool RunCompletionQueue(
            int inStatus)
        {
            const int theStatus = (inStatus == kErrorNoEntry &&
                mGetAllocOp.status != kErrorNoEntry) ? kErrorIO : inStatus;
            ReadOp* theOpPtr;
            while ((theOpPtr = Queue::Front(mCompletionQueue))) {
                theOpPtr->status = theStatus;
                if (theOpPtr->mFailShortReadFlag && theStatus == kErrorNoEntry) {
                    ReadOp& theOp   = *theOpPtr;
                    theOp.status    = kErrorInvalChunkSize;
                    theOp.statusMsg = "no such chunk -- hole";
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        theOp.statusMsg << ":"
                        " pos: "        << mGetAllocOp.fileOffset <<
                        " + "           << theOp.offset           <<
                        " requested: "  << theOp.numBytes         <<
                    KFS_LOG_EOM;
                }
                if (! ReportCompletion(*theOpPtr, mCompletionQueue)) {
                    return false;
                }
            }
            return true;
        }
        bool Sleep(
            int inSec)
        {
            if (inSec <= 0 || mSleepingFlag) {
                return false;
            }
            KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                "sleeping: " << inSec <<
                (mRestartStartReadFlag ? "resetting restart flag" : "") <<
            KFS_LOG_EOM;
            mRestartStartReadFlag = false;
            mSleepingFlag = true;
            mOuter.mStats.mSleepTimeSec += inSec;
            const bool kResetTimerFlag = true;
            SetTimeoutInterval(inSec * 1000, kResetTimerFlag);
            mOuter.mNetManager.RegisterTimeoutHandler(this);
            return true;
        }
        virtual void Timeout()
        {
            KFS_LOG_STREAM_DEBUG << mLogPrefix << "timeout" <<
            KFS_LOG_EOM;
            if (mSleepingFlag) {
                mOuter.mNetManager.UnRegisterTimeoutHandler(this);
                mSleepingFlag = false;
            }
            StartRead();
        }
        bool ReportCompletion(
            int       inStatus           = 0,
            Offset    inOffset           = 0,
            Offset    inSize             = 0,
            IOBuffer* inBufferPtr        = 0,
            RequestId inRequestId        = RequestId(),
            RequestId inStriperRequestId = RequestId())
        {
            if (mErrorCode == 0 &&
                    (inStatus >= 0 || inStatus == kErrorNoEntry)) {
                // Reset retry counts on successful completion.
                mRetryCount = 0;
            }
            return mOuter.ReportCompletion(
                inStatus,
                this,
                inOffset,
                inSize,
                inBufferPtr,
                inRequestId,
                inStriperRequestId
            );
        }
        time_t Now() const
            { return mOuter.mNetManager.Now(); }
        void EnqueueSelf(
            KfsOp&        inOp,
            IOBuffer*     inBufferPtr,
            KfsNetClient* inServerPtr)
        {
            mLastOpPtr   = &inOp;
            mOpStartTime = Now();
            KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                "+> " << (inServerPtr ? "" : "meta ") << inOp.Show() <<
                " buffer: " << (void*)inBufferPtr <<
                "/" << (inBufferPtr ? inBufferPtr->BytesConsumable() : 0) <<
            KFS_LOG_EOM;
            if (inServerPtr) {
                mOuter.mStats.mChunkOpsQueuedCount++;
            } else {
                if (mLastMetaOpPtr) {
                    mOuter.InternalError("more than one meta op in flight");
                }
                mLastMetaOpPtr = &inOp;
                mOuter.mStats.mMetaOpsQueuedCount++;
            }
            if (! (inServerPtr ? *inServerPtr : mOuter.mMetaServer).Enqueue(
                    &inOp, this, inBufferPtr)) {
                mOuter.InternalError(inServerPtr ?
                    "chunk op enqueue failure" :
                    "meta op enqueue failure"
                );
                inOp.status = kErrorFault;
                OpDone(&inOp, false, inBufferPtr);
            }
        }
        void CancelRead(
            ReadOp** inQueuePtr)
        {
            Queue::Iterator theIt(inQueuePtr);
            ReadOp* theOpPtr;
            while ((theOpPtr = theIt.Next())) {
                QCASSERT(mOuter.mStriperPtr);
                if (CancelRead(inQueuePtr, *theOpPtr)) {
                    if (inQueuePtr == mInFlightQueue) {
                        // Cancel will move the request into the pending queue.
                        mChunkServerPtr->Cancel(theOpPtr, this);
                    } else {
                        theOpPtr->Delete(inQueuePtr);
                    }
                }
            }
        }
        bool CancelRead(
            ReadOp** inQueuePtr,
            ReadOp&  inOp)
        {
            QCASSERT(mOuter.mStriperPtr);
            if (inOp.mCancelFlag) {
                return true;
            }
            if (inOp.mRequests.empty()) {
                if (mOuter.mStriperPtr->CanCancelRead(inOp.mStriperRequestId)) {
                    inOp.mCancelFlag = true;
                }
                return inOp.mCancelFlag;
            }
            size_t theCanceledCnt = 0;
            for (ReadOp::Requests::iterator theIt = inOp.mRequests.begin();
                    theIt != inOp.mRequests.end();
                    ++theIt) {
                if (theIt->mCancelFlag) {
                    theCanceledCnt++;
                } else if (mOuter.mStriperPtr->CanCancelRead(
                        theIt->mStriperRequestId)) {
                    theIt->mCancelFlag = true;
                    theCanceledCnt++;
                }
            }
            if (theCanceledCnt <= 0) {
                return false;
            }
            if (theCanceledCnt == inOp.mRequests.size()) {
                inOp.mCancelFlag = true;
                return true;
            }
            if (inQueuePtr == mCompletionQueue) {
                // Report completion uses mCancelFlag to skip over it.
                return false;
            }
            if (inQueuePtr == mInFlightQueue) {
                // The request has to be moved into the pending queue first,
                // then this method has to be called again.
                return true;
            }
            ReadOp* theDeleteQueue[1];
            Queue::Init(theDeleteQueue);
            if (inQueuePtr == mPendingQueue) {
                // Remove from pending to prevent adding pieces back to it.
                Queue::Remove(mPendingQueue, inOp);
                Queue::PushBack(theDeleteQueue, inOp);
            }
            // Re-queue the left over pieces.
            IOBuffer theBuf;
            Offset   theOffset = mGetAllocOp.fileOffset + inOp.offset;
            for (ReadOp::Requests::iterator theIt = inOp.mRequests.begin();
                    theIt != inOp.mRequests.end();
                    ++theIt) {
                if (theIt->mCancelFlag) {
                    theBuf.MoveSpace(&inOp.mTmpBuffer, theIt->mSize);
                    theBuf.Clear();
                } else {
                    QCVERIFY((int)theIt->mSize ==
                        QueueRead(
                            inOp.mTmpBuffer,
                            theIt->mSize,
                            theOffset,
                            theIt->mRequestId,
                            theIt->mStriperRequestId,
                            inOp.mRetryIfFailsFlag,
                            inOp.mFailShortReadFlag
                    ));
                }
                theOffset += theIt->mSize;
            }
            if (inQueuePtr == mPendingQueue) {
                inOp.Delete(theDeleteQueue);
                return false; // The original request canceled.
            }
            return true; // Cancel the original request.
        }
        void StopChunkServer()
        {
            if (&mChunkServer == mChunkServerPtr) {
                mChunkServer.Stop();
                return;
            }
            if (! mChunkServerPtr) {
                return;
            }
            if (mSizeOpInFlightFlag) {
                mChunkServerPtr->Cancel(&mSizeOp, this);
            }
            if (! Queue::IsEmpty(mInFlightQueue)) {
                mChunkServerPtr->CancelAllWithOwner(this);
            }
        }
        KfsNetClient& GetChunkServer()
        {
            return (mChunkServerPtr ? *mChunkServerPtr : mChunkServer);
        }
        bool WasChunkServerDisconnected()
        {
            return GetChunkServer().WasDisconnected();
        }
    private:
        ChunkReader(
            const ChunkReader& inChunkReader);
        ChunkReader& operator=(
            const ChunkReader& inChunkReader);
    };
    class ReportInvalidChunkOp : public CreateOp
    {
    public:
        ReportInvalidChunkOp(
            kfsChunkId_t inChunkId,
            int64_t      inVersion)
            : CreateOp(0, ROOTFID, 0, 1, true),
              mPath(MakePathName(inChunkId, inVersion))
        {
            filename = mPath.c_str();
        }
    private:
        const string mPath;

        static string MakePathName(
            kfsChunkId_t inChunkId,
            int64_t      inVersion)
        {
            ostringstream theStream;
            theStream << "/proc/invalid_chunks/" <<
                inChunkId << "." << inVersion;
            return theStream.str();
        }
    private:
        ReportInvalidChunkOp(
            const ReportInvalidChunkOp& inOp);
        ReportInvalidChunkOp& operator=(
            const ReportInvalidChunkOp& inOp);
    };
    friend class ChunkReader;
    friend class Striper;

    typedef ChunkReader::Readers Readers;

    Reader&             mOuter;
    MetaServer&         mMetaServer;
    string              mPathName;
    kfsFileId_t         mFileId;
    bool                mClosingFlag;
    int                 mErrorCode;
    const int           mIdleTimeoutSec;
    const int           mOpTimeoutSec;
    const int           mMaxRetryCount;
    const int           mTimeSecBetweenRetries;
    const int           mMaxReadSize;
    const int           mLeaseRetryTimeout;
    const int           mLeaseWaitTimeout;
    bool                mSkipHolesFlag;
    bool                mFailShortReadsFlag;
    int                 mMaxGetAllocRetryCount;
    Offset              mOffset;
    Offset              mOpenChunkBlockSize;
    int64_t             mChunkServerInitialSeqNum;
    ClientPool* const   mClientPoolPtr;
    Completion*         mCompletionPtr;
    string const        mLogPrefix;
    Stats               mStats;
    KfsNetClient::Stats mChunkServersStats;
    NetManager&         mNetManager;
    Striper*            mStriperPtr;
    int                 mCompletionDepthCount;
    ChunkReader*        mReaders[1];

    void InternalError(
            const char* inMsgPtr = 0)
    {
        if (inMsgPtr) {
            KFS_LOG_STREAM_FATAL << inMsgPtr << KFS_LOG_EOM;
        }
        MsgLogger::Stop();
        abort();
    }

    virtual ~Impl()
    {
        DisableCompletion();
        Impl::Shutdown();
    }
    int StartRead(
        IOBuffer& inBuffer,
        int       inLength,
        Offset    inOffset,
        RequestId inRequestId)
    {
        mOffset = inOffset;
        int theRem = inLength;
        while (theRem > 0) {
            const int thePrevRefCount = GetRefCount();
            const int theRet          = QueueRead(
                inBuffer, theRem, mOffset, inRequestId);
            if (thePrevRefCount > GetRefCount()) {
                return mErrorCode; // Unwind.
            }
            if (theRet <= 0) {
                QCASSERT(theRet < 0);
                if (mErrorCode == 0) {
                    mErrorCode = theRet;
                }
                break;
            }
            theRem -= theRet;
            mOffset += theRet;
        }
        return StartRead();
    }
    int StartRead()
    {
        if (! mClosingFlag) {
            return mErrorCode;
        }
        if (Readers::IsEmpty(mReaders)) {
            return ((! ReportCompletion()) ?  0 : mErrorCode);
        }
        Readers::Iterator theIt(mReaders);
        ChunkReader*      thePtr;
        while ((thePtr = theIt.Next())) {
            if (! thePtr->IsOpen()) {
                continue;
            }
            const int thePrevRefCount = GetRefCount();
            thePtr->Close();
            if (thePrevRefCount > GetRefCount()) {
                return mErrorCode; // Unwind.
            }
            // Restart from the beginning as close can invoke completion
            // and remove or close more than one reader in TryToCloseIdle().
            theIt.Reset();
        }
        return mErrorCode;
    }
    int QueueRead(
        IOBuffer& inBuffer,
        int       inLength,
        Offset    inOffset,
        RequestId inRequestId)
    {
        if (mStriperPtr) {
            return mStriperPtr->Process(
                inBuffer, inLength, inOffset, inRequestId);
        }
        const int theQueuedCount = QueueChunkRead(
            inBuffer, inLength, inOffset, inRequestId,
            RequestId(), true, mFailShortReadsFlag);
        if (theQueuedCount > 0) {
            StartQueuedRead(theQueuedCount);
        }
        return theQueuedCount;
    }
    int QueueChunkRead(
        IOBuffer& inBuffer,
        int       inSize,
        Offset    inOffset,
        RequestId inRequestId,
        RequestId inStriperRequestId,
        bool      inRetryIfFailsFlag,
        bool      inFailShortReadFlag)
    {
        QCASSERT(inOffset >= 0);
        if (inSize <= 0) {
            return 0;
        }
        const Offset theFileOffset = inOffset - inOffset % CHUNKSIZE;
        Readers::Iterator theIt(mReaders);
        ChunkReader* thePtr;
        while ((thePtr = theIt.Next())) {
            if (thePtr->GetFileOffset() == theFileOffset) {
                break;
            }
        }
        if (thePtr) {
            Readers::PushFront(mReaders, *thePtr);
            thePtr->CancelClose();
        } else {
            mChunkServerInitialSeqNum += 10000;
            thePtr = new ChunkReader(
                *this, mChunkServerInitialSeqNum, mLogPrefix);
        }
        QCASSERT(Readers::Front(mReaders) == thePtr);
        return thePtr->QueueRead(
            inBuffer,
            inSize,
            inOffset,
            inRequestId,
            inStriperRequestId,
            inRetryIfFailsFlag,
            inFailShortReadFlag
        );
    }
    void StartQueuedRead(
        int inQueuedCount)
    {
        if (inQueuedCount <= 0) {
            return;
        }
        QCASSERT(! Readers::IsEmpty(mReaders));
        Readers::Front(mReaders)->StartRead();
    }
    void CancelRead()
    {
        Readers::Iterator theIt(mReaders);
        ChunkReader* thePtr;
        while ((thePtr = theIt.Next())) {
            thePtr->CancelRead();
        }
    }
    void FatalError(
        int inErrorCode = 0)
    {
        if (mErrorCode == 0) {
            mErrorCode = inErrorCode;
        }
        if (mErrorCode == 0) {
            mErrorCode = -1;
        }
        mClosingFlag = false;
        ReportCompletion(mErrorCode);
    }
    bool CanClose(
        ChunkReader& inReader)
    {
        if (! inReader.IsIdle()) {
            return false;
        }
        if (! inReader.IsOpen() || (mClosingFlag &&
                (! mStriperPtr || mStriperPtr->IsIdle()))) {
            return true;
        }
        // The most recently used should always be first.
        const ChunkReader* const thePtr = Readers::Front(mReaders);
        if (! thePtr) {
            return true;
        }
        if (thePtr == &inReader) {
            return false;
        }
        const Offset theLeftEdge = thePtr->GetOpenChunkBlockFileOffset();
        if (theLeftEdge < 0) {
            return false;
        }
        const Offset theRightEdge = theLeftEdge + mOpenChunkBlockSize;
        const Offset theOffset    = inReader.GetFileOffset();
        return (theOffset < theLeftEdge || theRightEdge <= theOffset);
    }
    bool TryToCloseIdle(
        const ChunkReader* inReaderPtr)
    {
        ChunkReader* thePtr = Readers::Back(mReaders);
        if (! thePtr) {
            return (! inReaderPtr); // Already deleted.
        }
        bool theRetFlag = true;
        while (thePtr) {
            ChunkReader& theReader = *thePtr;
            thePtr = (thePtr == Readers::Front(mReaders)) ?
                0 : theReader.GetPrevPtr();
            if (CanClose(theReader)) {
                const bool theOpenFlag = theReader.IsOpen();
                if (theOpenFlag) {
                    theReader.Close();
                }
                // Handle "synchronous" Close(). ReportCompletion, calls
                // this method only when mCompletionDepthCount <= 1
                if (! theOpenFlag ||
                        (! theReader.IsOpen() && CanClose(theReader))) {
                    if (&theReader == inReaderPtr) {
                        theRetFlag = false;
                    }
                    delete &theReader;
                }
            } else if (theReader.IsIdle() && theReader.IsOpen()) {
                // Stop at the first idle that can not be closed.
                break;
            }
        }
        return theRetFlag;
    }
    bool ReportCompletion(
        int          inStatus           = 0,
        ChunkReader* inReaderPtr        = 0,
        Offset       inOffset           = 0,
        Offset       inSize             = 0,
        IOBuffer*    inBufferPtr        = 0,
        RequestId    inRequestId        = RequestId(),
        RequestId    inStriperRequestId = RequestId(),
        bool         inStiperDoneFlag   = false,
        int64_t      inRecoveriesCount  = 0)
    {
        // Order matters here, as StRef desctructor can delete this.
        StRef                     theRef(*this);
        QCStValueIncrementor<int> theIncrement(mCompletionDepthCount, 1);

        if (inStiperDoneFlag && mStriperPtr) {
            mStats.mReadRecoveriesCount = inRecoveriesCount;
        }
        if (inReaderPtr && mErrorCode == 0) {
            mErrorCode = inReaderPtr->GetErrorCode();
        }
        const int thePrevRefCount = GetRefCount();
        if (mStriperPtr && inReaderPtr && inBufferPtr && ! inStiperDoneFlag) {
            // The following can (and normally will) recursively this method
            // with inStiperDoneFlag set
            mStriperPtr->ReadCompletion(
                inStatus,
                *inBufferPtr,
                (int)inSize,
                inOffset,
                inRequestId,
                inStriperRequestId,
                inReaderPtr->GetChunkId(),
                inReaderPtr->GetChunkVersion(),
                inReaderPtr->GetSize()
            );
        }
        if ((! mStriperPtr || inStiperDoneFlag) &&
                (! mClosingFlag || inBufferPtr)) {
            int theStatus = mErrorCode == 0 ? inStatus : mErrorCode;
            if (! mSkipHolesFlag && inBufferPtr && mErrorCode == 0 &&
                    (inStatus == kErrorNoEntry || inStatus == 0)) {
                const int theLen = inBufferPtr->BytesConsumable();
                if (theLen < inSize) {
                    inBufferPtr->ZeroFillSpaceAvailable(inSize - theLen);
                }
                theStatus = 0;
            }
            if (mCompletionPtr) {
                mCompletionPtr->Done(
                    mOuter,
                    theStatus,
                    inOffset,
                    inSize,
                    inBufferPtr,
                    inRequestId
                );
            }
        }
        bool theRetFlag = true;
        if (mCompletionDepthCount <= 1 && thePrevRefCount <= GetRefCount()) {
            theRetFlag = TryToCloseIdle(inReaderPtr);
            if (mClosingFlag &&
                    Readers::IsEmpty(mReaders) &&
                    ! inStiperDoneFlag) {
                mClosingFlag = false;
                mFileId = -1;
                Striper* const theStriperPtr = mStriperPtr;
                mStriperPtr = 0;
                QCASSERT(! IsOpen());
                delete theStriperPtr;
                theRetFlag = false;
                if (mCompletionPtr) {
                    mCompletionPtr->Done(
                        mOuter,
                        mErrorCode,
                        0,
                        0,
                        0,
                        RequestId()
                    );
                }
            }
        }
        return (theRetFlag && thePrevRefCount <= GetRefCount());
    }
    void ReportInvalidChunk(
        kfsChunkId_t inChunkId,
        int64_t      inChunkVersion,
        int          inStatus,
        const char*  inStatusMsgPtr)
    {
        KFS_LOG_STREAM_WARN << mLogPrefix <<
            "invalid"
            " chunk: "   << inChunkId <<
            " version: " << inChunkVersion <<
            " status: "  << inStatus <<
            ((inStatusMsgPtr && *inStatusMsgPtr) ? " msg: " : "") <<
            ((inStatusMsgPtr && *inStatusMsgPtr) ? inStatusMsgPtr : "") <<
        KFS_LOG_EOM;
        mMetaServer.Enqueue(
            new ReportInvalidChunkOp(inChunkId, inChunkVersion),
            0
        );
    }
    bool IsChunkServerClearTextAllowed() const
    {
        ClientAuthContext* const theCtxPtr = mMetaServer.GetAuthContext();
        return (! theCtxPtr || theCtxPtr->IsChunkServerClearTextAllowed());
    }
    bool IsAuthEnabled() const
    {
        ClientAuthContext* const theCtxPtr = mMetaServer.GetAuthContext();
        return (theCtxPtr && theCtxPtr->IsEnabled());
    }
private:
    Impl(
        const Impl& inReader);
    Impl& operator=(
        const Impl& inReader);
};

/* static */ Reader::Striper*
Reader::Striper::Create(
    int                      inType,
    int                      inStripeCount,
    int                      inRecoveryStripeCount,
    int                      inStripeSize,
    int                      inMaxAtomicReadRequestSize,
    bool                     inUseDefaultBufferAllocatorFlag,
    bool                     inFailShortReadsFlag,
    Reader::Striper::Offset  inRecoverChunkPos,
    Reader::Striper::Offset  inFileSize,
    Reader::Striper::SeqNum  inInitialSeqNum,
    const string&            inLogPrefix,
    Reader::Striper::Impl&   inOuter,
    Reader::Striper::Offset& outOpenChunkBlockSize,
    string&                  outErrMsg)
{
    switch (inType) {
        case KFS_STRIPED_FILE_TYPE_NONE:
            outOpenChunkBlockSize = Offset(CHUNKSIZE);
        break;
        default:
            return RSStriperCreate(
                inType,
                inStripeCount,
                inRecoveryStripeCount,
                inStripeSize,
                inMaxAtomicReadRequestSize,
                inUseDefaultBufferAllocatorFlag,
                inFailShortReadsFlag,
                inRecoverChunkPos,
                inFileSize,
                inInitialSeqNum,
                inLogPrefix,
                inOuter,
                outOpenChunkBlockSize,
                outErrMsg
            );
    }
    return 0;
}

int
Reader::Striper::QueueRead(
    IOBuffer&                  inBuffer,
    int                        inSize,
    Reader::Striper::Offset    inOffset,
    Reader::Striper::RequestId inOriginalRequestId,
    Reader::Striper::RequestId inRequestId,
    bool                       inRetryIfFailsFlag,
    bool                       inFailShortReadFlag)
{
    return mOuter.QueueChunkRead(
        inBuffer,
        inSize,
        inOffset,
        inOriginalRequestId,
        inRequestId,
        inRetryIfFailsFlag,
        inFailShortReadFlag
    );
}

void
Reader::Striper::StartQueuedRead(
    int inQueuedCount)
{
    mOuter.StartQueuedRead(inQueuedCount);
}

void
Reader::Striper::CancelRead()
{
    mOuter.CancelRead();
}

bool
Reader::Striper::ReportCompletion(
    int                        inStatus,
    IOBuffer&                  inBuffer,
    int                        inLength,
    Reader::Striper::Offset    inOffset,
    Reader::Striper::RequestId inRequestId,
    int64_t                    inRecoveriesCount)
{
    return mOuter.ReportCompletion(
        inStatus,
        0,
        inOffset,
        inLength,
        &inBuffer,
        inRequestId,
        RequestId(),
        true,
        inRecoveriesCount
    );
}

void
Reader::Striper::ReportInvalidChunk(
        kfsChunkId_t inChunkId,
        int64_t      inChunkVersion,
        int          inStatus,
        const char*  inStatusMsgPtr)
{
    mOuter.ReportInvalidChunk(
        inChunkId,
        inChunkVersion,
        inStatus,
        inStatusMsgPtr
    );
}

Reader::Reader(
    Reader::MetaServer& inMetaServer,
    Reader::Completion* inCompletionPtr            /* = 0 */,
    int                 inMaxRetryCount            /* = 6 */,
    int                 inTimeSecBetweenRetries    /* = 15 */,
    int                 inOpTimeoutSec             /* = 30 */,
    int                 inIdleTimeoutSec           /* = 5 * 30 */,
    int                 inMaxReadSize              /* = 1 << 20 */,
    int                 inLeaseRetryTimeout        /* = 3 */,
    int                 inLeaseWaitTimeout         /* = 900 */,
    const char*         inLogPrefixPtr             /* = 0 */,
    int64_t             inChunkServerInitialSeqNum /* = 1 */,
    ClientPool*         inClientPoolPtr            /* = 0 */)
    : mImpl(*new Reader::Impl(
        *this,
        inMetaServer,
        inCompletionPtr,
        inMaxRetryCount,
        inTimeSecBetweenRetries,
        inOpTimeoutSec,
        inIdleTimeoutSec,
        inMaxReadSize,
        inLeaseRetryTimeout,
        inLeaseWaitTimeout,
        (inLogPrefixPtr && inLogPrefixPtr[0]) ?
            (inLogPrefixPtr + string(" ")) : string(),
        inChunkServerInitialSeqNum,
        inClientPoolPtr
    ))
{
    mImpl.Ref();
}

/* virtual */
Reader::~Reader()
{
    mImpl.DisableCompletion();
    mImpl.UnRef();
}

int
Reader::Open(
    kfsFileId_t inFileId,
    const char*    inFileNamePtr,
    Reader::Offset inFileSize,
    int            inStriperType,
    int            inStripeSize,
    int            inStripeCount,
    int            inRecoveryStripeCount,
    bool           inSkipHolesFlag,
    bool           inUseDefaultBufferAllocatorFlag,
    Reader::Offset inRecoverChunkPos,
    bool           inFailShortReadsFlag)
{
    Impl::StRef theRef(mImpl);
    return mImpl.Open(
        inFileId,
        inFileNamePtr,
        inFileSize,
        inStriperType,
        inStripeSize,
        inStripeCount,
        inRecoveryStripeCount,
        inSkipHolesFlag,
        inUseDefaultBufferAllocatorFlag,
        inRecoverChunkPos,
        inFailShortReadsFlag
    );
}

int
Reader::Close()
{
    Impl::StRef theRef(mImpl);
    return mImpl.Close();
}

int
Reader::Read(
    IOBuffer&         inBuffer,
    int               inLength,
    Reader::Offset    inOffset,
    Reader::RequestId inRequestId)
{
    Impl::StRef theRef(mImpl);
    return mImpl.Read(inBuffer, inLength, inOffset, inRequestId);
}

void
Reader::Stop()
{
    Impl::StRef theRef(mImpl);
    mImpl.Stop();
}

void
Reader::Shutdown()
{
    Impl::StRef theRef(mImpl);
    mImpl.Shutdown();
}

bool
Reader::IsOpen() const
{
    Impl::StRef theRef(mImpl);
    return (mImpl.IsOpen() && ! IsClosing());
}

bool
Reader::IsClosing() const
{
    Impl::StRef theRef(mImpl);
    return mImpl.IsClosing();
}

bool
Reader::IsActive() const
{
    Impl::StRef theRef(mImpl);
    return mImpl.IsActive();
}

int
Reader::GetErrorCode() const
{
    Impl::StRef theRef(mImpl);
    return mImpl.GetErrorCode();
}

void
Reader::Register(
    Reader::Completion* inCompletionPtr)
{
    Impl::StRef theRef(mImpl);
    mImpl.Register(inCompletionPtr);
}

bool
Reader::Unregister(
   Reader::Completion* inCompletionPtr)
{
    Impl::StRef theRef(mImpl);
    return mImpl.Unregister(inCompletionPtr);
}

void
Reader::GetStats(
    Stats&               outStats,
    KfsNetClient::Stats& outChunkServersStats) const
{
    Impl::StRef theRef(mImpl);
    mImpl.GetStats(outStats, outChunkServersStats);
}

}}
