//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2010/06/11
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

#include "Writer.h"

#include <sstream>
#include <algorithm>
#include <cerrno>
#include <sstream>
#include <bitset>
#include <string.h>

#include "kfsio/IOBuffer.h"
#include "kfsio/NetManager.h"
#include "kfsio/checksum.h"
#include "kfsio/ITimeout.h"
#include "common/kfsdecls.h"
#include "common/MsgLogger.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"
#include "qcdio/qcdebug.h"
#include "qcdio/QCDLList.h"
#include "RSStriper.h"
#include "KfsOps.h"
#include "utils.h"
#include "KfsClient.h"

namespace KFS
{
namespace client
{

using std::min;
using std::max;
using std::string;
using std::ostream;
using std::ostringstream;
using std::istringstream;

// Kfs client write state machine implementation.
class Writer::Impl :
    public QCRefCountedObj,
    private ITimeout,
    private KfsNetClient::OpOwner
{
public:
    typedef QCRefCountedObj::StRef StRef;

    enum
    {
        kErrorNone       = 0,
        kErrorParameters = -EINVAL,
        kErrorTryAgain   = -EAGAIN,
        kErrorFault      = -EFAULT,
        kErrorNoEntry    = -ENOENT
    };

    Impl(
        Writer&     inOuter,
        MetaServer& inMetaServer,
        Completion* inCompletionPtr,
        int         inMaxRetryCount,
        int         inWriteThreshold,
        int         inMaxPartialBuffersCount,
        int         inTimeSecBetweenRetries,
        int         inOpTimeoutSec,
        int         inIdleTimeoutSec,
        int         inMaxWriteSize,
        string      inLogPrefix,
        int64_t     inChunkServerInitialSeqNum)
        : QCRefCountedObj(),
          ITimeout(),
          KfsNetClient::OpOwner(),
          mOuter(inOuter),
          mMetaServer(inMetaServer),
          mPathName(),
          mFileId(-1),
          mClosingFlag(false),
          mSleepingFlag(false),
          mErrorCode(0),
          mWriteThreshold(max(0, inWriteThreshold)),
          mPartialBuffersCount(0),
          mPendingCount(0),
          mIdleTimeoutSec(inIdleTimeoutSec),
          mOpTimeoutSec(inOpTimeoutSec),
          mMaxRetryCount(inMaxRetryCount),
          mTimeSecBetweenRetries(inTimeSecBetweenRetries),
          mMaxPartialBuffersCount(inMaxPartialBuffersCount),
          mMaxWriteSize(min((int)CHUNKSIZE,
            (int)((max(0, inMaxWriteSize) + CHECKSUM_BLOCKSIZE - 1) /
                CHECKSUM_BLOCKSIZE * CHECKSUM_BLOCKSIZE))),
          mReplicaCount(-1),
          mRetryCount(0),
          mFileSize(0),
          mOffset(0),
          mOpenChunkBlockSize(CHUNKSIZE),
          mChunkServerInitialSeqNum(inChunkServerInitialSeqNum),
          mCompletionPtr(inCompletionPtr),
          mBuffer(),
          mLogPrefix(inLogPrefix),
          mStats(),
          mNetManager(mMetaServer.GetNetManager()),
          mTruncateOp(0, 0, -1, 0),
          mOpStartTime(0),
          mCompletionDepthCount(0),
          mStriperProcessCount(0),
          mStriperPtr(0)
        { Writers::Init(mWriters); }
    int Open(
        kfsFileId_t inFileId,
        const char* inFileNamePtr,
        Offset      inFileSize,
        int         inStriperType,
        int         inStripeSize,
        int         inStripeCount,
        int         inRecoveryStripeCount,
        int         inReplicaCount)
    {
        if (inFileId <= 0 || ! inFileNamePtr || ! *inFileNamePtr) {
            return kErrorParameters;
        }
        if (mFileId > 0) {
            if (inFileId == mFileId &&
                    inFileNamePtr == mPathName) {
                return mErrorCode;
            }
            return kErrorParameters;
        }
        if (IsOpen() && mErrorCode != 0) {
            return (mErrorCode < 0 ? mErrorCode : -mErrorCode);
        }
        if (mClosingFlag || mSleepingFlag) {
            return kErrorTryAgain;
        }
        delete mStriperPtr;
        string theErrMsg;
        mStriperPtr = 0;
        mOpenChunkBlockSize = Offset(CHUNKSIZE);
        mStriperPtr = Striper::Create(
            inStriperType,
            inStripeCount,
            inRecoveryStripeCount,
            inStripeSize,
            inFileSize,
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
        mBuffer.Clear();
        mStats.Clear();
        mReplicaCount          = inReplicaCount;
        mFileSize              = inFileSize;
        mPartialBuffersCount   = 0;
        mPathName              = inFileNamePtr;
        mErrorCode             = 0;
        mFileId                = inFileId;
        mTruncateOp.fid        = -1;
        mTruncateOp.pathname   = 0;
        mTruncateOp.fileOffset = mFileSize;
        mRetryCount            = 0;
        return StartWrite();
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
        return StartWrite();
    }
    int Write(
        IOBuffer& inBuffer,
        int       inLength,
        Offset    inOffset,
        bool      inFlushFlag,
        int       inWriteThreshold)
    {
        if (inOffset < 0) {
            return kErrorParameters;
        }
        if (mErrorCode != 0) {
            return (mErrorCode < 0 ? mErrorCode : -mErrorCode);
        }
        if (mClosingFlag || ! IsOpen()) {
            return kErrorParameters;
        }
        if (inLength <= 0) {
            return (
                (ReportCompletion(0, inLength, inOffset) && inFlushFlag) ?
                StartWrite(true) : 0
            );
        }
        if (inOffset != mOffset + mBuffer.BytesConsumable()) {
            // Just flush for now, do not try to optimize buffer rewrite.
            const int thePrevRefCount = GetRefCount();
            const int theRet = Flush();
            if (theRet < 0) {
                return theRet;
            }
            if (thePrevRefCount > GetRefCount()) {
                return (mErrorCode < 0 ? mErrorCode : -mErrorCode);
            }
            mOffset = inOffset;
        }
        if (mMaxPartialBuffersCount == 0 ||
                inLength < IOBufferData::GetDefaultBufferSize() * 2) {
            // If write size is small, then copy it into the last buffer.
            mBuffer.ReplaceKeepBuffersFull(
                &inBuffer, mBuffer.BytesConsumable(), inLength);
        } else {
            if (mBuffer.IsEmpty()) {
                mPartialBuffersCount = 0;
            }
            mBuffer.Move(&inBuffer, inLength);
            mPartialBuffersCount++;
            if (mMaxPartialBuffersCount >= 0 &&
                    mPartialBuffersCount >= mMaxPartialBuffersCount) {
                mBuffer.MakeBuffersFull();
                mPartialBuffersCount = 0;
                mStats.mBufferCompactionCount++;
            }
        }
        if (inWriteThreshold >= 0) {
            mWriteThreshold = inWriteThreshold;
        }
        const int theErrorCode = StartWrite(inFlushFlag);
        return (theErrorCode == 0 ? inLength :
            (theErrorCode < 0 ? theErrorCode : -theErrorCode));
    }
    int Flush()
    {
        const int theErrorCode = StartWrite(true);
        return (theErrorCode < 0 ? theErrorCode : -theErrorCode);
    }
    void Stop()
    {
        while (! Writers::IsEmpty(mWriters)) {
            delete Writers::Front(mWriters);
        }
        if (mTruncateOp.fid >= 0) {
            mMetaServer.Cancel(&mTruncateOp, this);
        }
        if (mSleepingFlag) {
            mNetManager.UnRegisterTimeoutHandler(this);
            mSleepingFlag = false;
        }
        mClosingFlag = false;
        mBuffer.Clear();
    }
    void Shutdown()
    {
        Stop();
        mFileId      = -1;
        mErrorCode   = 0;
    }
    bool IsOpen() const
        { return (mFileId > 0); }
    bool IsClosing() const
        { return (IsOpen() && mClosingFlag); }
    bool IsActive() const
    {
        return (
            IsOpen() && (
                ! mBuffer.IsEmpty() ||
                ! Writers::IsEmpty(mWriters) ||
                mClosingFlag)
        );
    }
    Offset GetPendingSize() const
        { return (GetPendingSizeSelf() + mPendingCount); }
    int SetWriteThreshold(
        int inThreshold)
    {
        const int  theThreshold      = max(0, inThreshold);
        const bool theStartWriteFlag = mWriteThreshold > theThreshold;
        mWriteThreshold = theThreshold;
        return ((theStartWriteFlag && IsOpen() && mErrorCode == 0) ?
            StartWrite() : mErrorCode
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
        KfsNetClient::Stats& outChunkServersStats)
    {
        outStats             = mStats;
        outChunkServersStats = mChunkServersStats;
    }
    bool GetErrorCode() const
        { return mErrorCode; }

private:
    typedef KfsNetClient ChunkServer;

    class ChunkWriter : private ITimeout, private KfsNetClient::OpOwner
    {
    public:
        struct WriteOp;
        typedef QCDLList<WriteOp, 0> Queue;
        typedef QCDLList<ChunkWriter, 0> Writers;

        struct WriteOp : public KfsOp
        {
            WritePrepareOp mWritePrepareOp;
            WriteSyncOp    mWriteSyncOp;
            IOBuffer       mBuffer;
            size_t         mBeginBlock;
            size_t         mEndBlock;
            time_t         mOpStartTime;
            bool           mChecksumValidFlag;
            WriteOp*       mPrevPtr[1];
            WriteOp*       mNextPtr[1];

            WriteOp()
                : KfsOp(CMD_WRITE, 0),
                  mWritePrepareOp(0, 0, 0),
                  mWriteSyncOp(),
                  mBuffer(),
                  mBeginBlock(0),
                  mEndBlock(0),
                  mOpStartTime(0),
                  mChecksumValidFlag(false)
                { Queue::Init(*this); }
            void Delete(
                WriteOp** inListPtr)
            {
                Queue::Remove(inListPtr, *this);
                delete this;
            }
            virtual void Request(
                ostream& inStream)
            {
                if (mWritePrepareOp.replyRequestedFlag) {
                    mWritePrepareOp.seq = seq;
                } else {
                    mWriteSyncOp.seq    = seq;
                    mWritePrepareOp.seq = seq + 1;
                }
                mWritePrepareOp.Request(inStream);
            }
            virtual bool NextRequest(
                kfsSeq_t inSeqNum,
                ostream& inStream)
            {
                if (mWritePrepareOp.replyRequestedFlag) {
                    return false;
                }
                QCASSERT(seq <= inSeqNum && inSeqNum <= mWritePrepareOp.seq + 1);
                if (mWritePrepareOp.seq < inSeqNum) {
                    return false;
                }
                mWriteSyncOp.Request(inStream);
                return true;
            }
            virtual string Show() const
            {
                string theRet = mWritePrepareOp.Show();
                if (! mWritePrepareOp.replyRequestedFlag) {
                    theRet += " ";
                    theRet += mWriteSyncOp.Show();
                }
                return theRet;
            }
            virtual void ParseResponseHeaderSelf(
                const Properties& inProps)
            {
                if (contentLength > 0) {
                    KFS_LOG_STREAM_ERROR <<
                        "invalid response content length: " << contentLength <<
                        " " << mWriteSyncOp.Show() <<
                    KFS_LOG_EOM;
                    contentLength = 0;
                }
                mWritePrepareOp.status    = status;
                mWritePrepareOp.statusMsg = statusMsg;
                mWriteSyncOp.status       = status;
                mWriteSyncOp.statusMsg    = statusMsg;
                if (mWritePrepareOp.replyRequestedFlag) {
                    mWritePrepareOp.ParseResponseHeaderSelf(inProps);
                } else {
                    mWriteSyncOp.ParseResponseHeaderSelf(inProps);
                }
            }
            void InitBlockRange()
            {
                QCASSERT(
                    mWritePrepareOp.offset >= 0 &&
                    mWritePrepareOp.offset +
                        mBuffer.BytesConsumable() <= (Offset)CHUNKSIZE
                );
                mBeginBlock = mWritePrepareOp.offset / CHECKSUM_BLOCKSIZE;
                mEndBlock   = mBeginBlock +
                    (mBuffer.BytesConsumable() + CHECKSUM_BLOCKSIZE - 1) /
                    CHECKSUM_BLOCKSIZE;
            }
        private:
            virtual ~WriteOp()
                {}
            WriteOp(
                const WriteOp& inWriteOp);
            WriteOp& operator=(
                const WriteOp& inWriteOp);
        };

        ChunkWriter(
            Impl&         inOuter,
            int64_t       inSeqNum,
            const string& inLogPrefix)
            : ITimeout(),
              KfsNetClient::OpOwner(),
              mOuter(inOuter),
              mChunkServer(
                inOuter.mNetManager,
                string(), -1,
                // All chunk server retries are handled here
                0, // inMaxRetryCount
                0, // inTimeSecBetweenRetries,
                inOuter.mOpTimeoutSec,
                inOuter.mIdleTimeoutSec,
                inSeqNum,
                inLogPrefix.c_str(),
                // Just fail the op. Error handler will reset connection and
                // cancel all pending ops by calling Stop()
                false // inResetConnectionOnOpTimeoutFlag
              ),
              mErrorCode(0),
              mRetryCount(0),
              mPendingCount(0),
              mOpenChunkBlockFileOffset(-1),
              mOpStartTime(0),
              mWriteIds(),
              mAllocOp(0, 0, ""),
              mWriteIdAllocOp(0, 0, 0, 0, 0),
              mCloseOp(0, 0),
              mLastOpPtr(0),
              mSleepingFlag(false),
              mClosingFlag(false),
              mLogPrefix(inLogPrefix),
              mOpDoneFlagPtr(0),
              mInFlightBlocks()
        {
            Queue::Init(mPendingQueue);
            Queue::Init(mInFlightQueue);
            Writers::Init(*this);
            Writers::PushFront(mOuter.mWriters, *this);
            mChunkServer.SetRetryConnectOnly(true);
            mAllocOp.fileOffset        = -1;
            mAllocOp.invalidateAllFlag = false;
        }
        ~ChunkWriter()
        {
            ChunkWriter::Shutdown();
            ChunkServer::Stats theStats;
            mChunkServer.GetStats(theStats);
            mOuter.mChunkServersStats.Add(theStats);
            Writers::Remove(mOuter.mWriters, *this);
        }
        void CancelClose()
            { mClosingFlag = false; }
        // The QueueWrite() guarantees that completion will not be invoked.
        // The writes will be queued even if the writer is already in the error
        // state: mErrorCode != 0. In the case of fatal error all pending writes
        // are discarded when the writer gets deleted.
        //
        // StartWrite() must be called in order to start executing pending
        // writes.
        // This allows the caller to properly update its state before the writes
        // get executed, and the corresponding completion(s) invoked.
        int QueueWrite(
            IOBuffer& inBuffer,
            int       inSize,
            Offset    inOffset,
            int       inWriteThreshold)
        {
            int theSize = min(inBuffer.BytesConsumable(), inSize);
            if (theSize <= 0) {
                return 0;
            }
            const Offset kChunkSize         = (Offset)CHUNKSIZE;
            const int    kChecksumBlockSize = (int)CHECKSUM_BLOCKSIZE;
            QCRTASSERT(inOffset >= 0 && ! mClosingFlag);
            const Offset theChunkOffset = inOffset % kChunkSize;
            if (mAllocOp.fileOffset < 0) {
                mAllocOp.fileOffset = inOffset - theChunkOffset;
                mOpenChunkBlockFileOffset = mAllocOp.fileOffset -
                    mAllocOp.fileOffset % mOuter.mOpenChunkBlockSize;
            } else {
                QCRTASSERT(mAllocOp.fileOffset == inOffset - theChunkOffset);
            }
            theSize = min(theSize, (int)(kChunkSize - theChunkOffset));
            QCASSERT(theSize > 0);
            Offset thePos = theChunkOffset;
            // Try to append to the last pending op.
            WriteOp* const theLastOpPtr = Queue::Back(mPendingQueue);
            if (theLastOpPtr) {
                WriteOp& theOp = *theLastOpPtr;
                const int    theOpSize = theOp.mBuffer.BytesConsumable();
                const Offset theOpPos  = theOp.mWritePrepareOp.offset;
                if (theOpPos + theOpSize == thePos) {
                    const int theHead = (int)(theOpPos % kChecksumBlockSize);
                    int       theNWr  = min(theSize,
                        (theHead == 0 ?
                            mOuter.mMaxWriteSize :
                            kChecksumBlockSize - theHead
                        ) - theOpSize
                    );
                    if (theNWr > 0 &&
                            theOpSize + theNWr > kChecksumBlockSize) {
                        theNWr -= (theOpSize + theNWr) % kChecksumBlockSize;
                    }
                    if (theNWr > 0) {
                        theOp.mBuffer.Move(&inBuffer, theNWr);
                        // Force checksum recomputation.
                        theOp.mChecksumValidFlag = false;
                        theOp.mWritePrepareOp.checksums.clear();
                        // Update last the block index.
                        const int theCurBegin = theOp.mBeginBlock;
                        theOp.InitBlockRange();
                        theOp.mBeginBlock = theCurBegin;
                        // The op is already in the pending queue.
                        theSize -= theNWr;
                        thePos  += theNWr;
                    }
                }
            }
            const int theWriteThreshold = thePos + theSize >= kChunkSize ?
                1 : max(inWriteThreshold, 1);
            const int theBlockOff       = (int)(thePos % kChecksumBlockSize);
            if (theBlockOff > 0 && (theSize >= theWriteThreshold ||
                    theBlockOff + theSize >= kChecksumBlockSize)) {
                WriteOp* const theWriteOpPtr = new WriteOp();
                theWriteOpPtr->mWritePrepareOp.offset = thePos;
                const int theNWr = theWriteOpPtr->mBuffer.Move(
                    &inBuffer,
                    min(theSize, kChecksumBlockSize - theBlockOff)
                );
                theSize -= theNWr;
                thePos  += theNWr;
                theWriteOpPtr->InitBlockRange();
                Queue::PushBack(mPendingQueue, *theWriteOpPtr);
            }
            while (theSize >= theWriteThreshold) {
                int theOpSize = min(mOuter.mMaxWriteSize, theSize);
                if (theOpSize > kChecksumBlockSize) {
                    theOpSize -= theOpSize % kChecksumBlockSize;
                }
                WriteOp* const theWriteOpPtr = new WriteOp();
                theWriteOpPtr->mWritePrepareOp.offset = thePos;
                const int theNWr =
                    theWriteOpPtr->mBuffer.Move(&inBuffer, theOpSize);
                theSize -= theNWr;
                thePos  += theNWr;
                theWriteOpPtr->InitBlockRange();
                Queue::PushBack(mPendingQueue, *theWriteOpPtr);
            }
            QCRTASSERT(thePos <= kChunkSize && theSize >= 0);
            const Offset theNWr = thePos - theChunkOffset;
            // The following must be updated before invoking StartWrite(),
            // as it could invoke completion immediately (in the case of
            // failure).
            mPendingCount += theNWr;
            return theNWr;
        }
        void StartWrite()
        {
            if (mSleepingFlag) {
                return;
            }
            if (mErrorCode != 0 && ! mAllocOp.invalidateAllFlag) {
                if (mLastOpPtr) {
                    Reset();
                }
                mClosingFlag = false;
                return;
            }
            if (mClosingFlag && ! CanWrite()) {
                if (! Queue::IsEmpty(mInFlightQueue)) {
                    return;
                }
                if (mLastOpPtr == &mCloseOp) {
                    return;
                }
                // Try to close chunk even if chunk server disconnected, to
                // release the write lease.
                if (mAllocOp.chunkId > 0) {
                    CloseChunk();
                    return;
                }
                mChunkServer.Stop();
                if (mLastOpPtr == &mAllocOp) {
                    mOuter.mMetaServer.Cancel(mLastOpPtr, this);
                }
                mClosingFlag        = false;
                mAllocOp.fileOffset = -1;
                mAllocOp.chunkId    = -1;
                ReportCompletion();
                return;
            }
            if (! CanWrite()) {
                return;
            }
            if (mAllocOp.chunkId > 0 && mChunkServer.WasDisconnected()) {
                // When chunk server disconnects it might clean up write lease.
                // Start from the beginning -- chunk allocation.
                KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                    "detected chunk server disconnect: " <<
                        mChunkServer.GetServerLocation() <<
                    " starting from chunk allocation, pending:" <<
                    " queue: " << (Queue::IsEmpty(mPendingQueue) ? "" : "not") <<
                        " empty" <<
                KFS_LOG_EOM;
                Reset();
                if (! CanWrite()) {
                    // Do not try to preallocate chunk after inactivity timeout
                    // or error, if no data pending.
                    return;
                }
            }
            // Return immediately after calling Write() and AllocateChunk(), as
            // these can invoke completion. Completion, in turn, can delete
            // this.
            // Other methods of this class have to return immediately (unwind)
            // after invoking StartWrite().
            if (mAllocOp.chunkId > 0 && ! mWriteIds.empty()) {
                Write();
            } else if (! mLastOpPtr) { // Close can be in flight.
                Reset();
                AllocateChunk();
            }
        }
        void Close()
        {
            if (! mClosingFlag && IsOpen()) {
                mClosingFlag = true;
                StartWrite();
            }
        }
        void Shutdown()
        {
            Reset();
            QCRTASSERT(Queue::IsEmpty(mInFlightQueue));
            while (! Queue::IsEmpty(mPendingQueue)) {
                Queue::Front(mPendingQueue)->Delete(mPendingQueue);
            }
            mClosingFlag  = false;
            mErrorCode    = 0;
            mPendingCount = 0;
        }
        Offset GetFileOffset() const
        {
            return (mErrorCode == 0 ? mAllocOp.fileOffset : -1);
        }
        bool IsIdle() const
        {
            return (
                Queue::IsEmpty(mPendingQueue) &&
                Queue::IsEmpty(mInFlightQueue) &&
                ! mClosingFlag
            );
        }
        bool IsOpen() const
        {
            return (
                mErrorCode == 0 &&
                mAllocOp.fileOffset >= 0 &&
                ! mClosingFlag
            );
        }
        int GetErrorCode() const
            { return mErrorCode; }
        Offset GetPendingCount() const
            { return mPendingCount; }
        ChunkWriter* GetPrevPtr()
        {
            ChunkWriter& thePrev = ChunkWritersListOp::GetPrev(*this);
            return (&thePrev == this ? 0 : &thePrev);
        }
        Offset GetOpenChunkBlockFileOffset() const
        {
            return (mAllocOp.fileOffset >= 0 ? mOpenChunkBlockFileOffset : -1);
        }

    private:
        typedef std::vector<WriteInfo> WriteIds;
        typedef std::bitset<CHUNKSIZE / CHECKSUM_BLOCKSIZE> ChecksumBlocks;

        Impl&          mOuter;
        ChunkServer    mChunkServer;
        int            mErrorCode;
        int            mRetryCount;
        Offset         mPendingCount;
        Offset         mOpenChunkBlockFileOffset;
        time_t         mOpStartTime;
        WriteIds       mWriteIds;
        AllocateOp     mAllocOp;
        WriteIdAllocOp mWriteIdAllocOp;
        CloseOp        mCloseOp;
        KfsOp*         mLastOpPtr;
        bool           mSleepingFlag;
        bool           mClosingFlag;
        string const   mLogPrefix;
        bool*          mOpDoneFlagPtr;
        ChecksumBlocks mInFlightBlocks;
        WriteOp*       mPendingQueue[1];
        WriteOp*       mInFlightQueue[1];
        ChunkWriter*   mPrevPtr[1];
        ChunkWriter*   mNextPtr[1];

        friend class QCDLListOp<ChunkWriter, 0>;
        typedef QCDLListOp<ChunkWriter, 0> ChunkWritersListOp;

        void AllocateChunk()
        {
            QCASSERT(
                mOuter.mFileId > 0 &&
                mAllocOp.fileOffset >= 0 &&
                ! Queue::IsEmpty(mPendingQueue)
            );
            Reset(mAllocOp);
            mAllocOp.fid                  = mOuter.mFileId;
            mAllocOp.pathname             = mOuter.mPathName;
            mAllocOp.append               = false;
            mAllocOp.chunkId              = -1;
            mAllocOp.chunkVersion         = -1;
            mAllocOp.spaceReservationSize = 0;
            mAllocOp.maxAppendersPerChunk = 0;
            mAllocOp.chunkServers.clear();
            mOuter.mStats.mChunkAllocCount++;
            EnqueueMeta(mAllocOp);
        }
        void Done(
            AllocateOp& inOp,
            bool        inCanceledFlag,
            IOBuffer*   inBufferPtr)
        {
            QCASSERT(&mAllocOp == &inOp && ! inBufferPtr);
            if (inCanceledFlag) {
                return;
            }
            if (inOp.status != 0 || (mAllocOp.chunkServers.empty() &&
                    ! mAllocOp.invalidateAllFlag)) {
                mAllocOp.chunkId = 0;
                HandleError(inOp);
                return;
            }
            if (mAllocOp.invalidateAllFlag) {
                // Report all writes completed. Completion does not expect the
                // offset to match the original write offset with striper.
                KFS_LOG_STREAM_INFO << mLogPrefix <<
                    "invalidate done:"
                    " chunk: "   << mAllocOp.chunkId <<
                    " offset: "  << mAllocOp.fileOffset <<
                    " status: "  << inOp.status <<
                    " pending: " << mPendingCount <<
                    " w-empty: " << Queue::IsEmpty(mPendingQueue) <<
                KFS_LOG_EOM;
                const int    theSize   = mPendingCount;
                const Offset theOffset = theSize > 0 ? mAllocOp.fileOffset : 0;
                mAllocOp.invalidateAllFlag = false;
                Shutdown();
                ReportCompletion(theOffset, theSize);
                return;
            }
            AllocateWriteId();
        }
        bool CanWrite()
        {
            return (
                ! Queue::IsEmpty(mPendingQueue) ||
                mAllocOp.invalidateAllFlag
            );
        }
        void AllocateWriteId()
        {
            QCASSERT(mAllocOp.chunkId > 0 && ! mAllocOp.chunkServers.empty());
            Reset(mWriteIdAllocOp);
            mWriteIdAllocOp.chunkId                     = mAllocOp.chunkId;
            mWriteIdAllocOp.chunkVersion                = mAllocOp.chunkVersion;
            mWriteIdAllocOp.isForRecordAppend           = false;
            mWriteIdAllocOp.chunkServerLoc              = mAllocOp.chunkServers;
            mWriteIdAllocOp.offset                      = 0;
            mWriteIdAllocOp.numBytes                    = 0;
            mWriteIdAllocOp.writePrepReplySupportedFlag = false;
            if (mChunkServer.SetServer(mAllocOp.chunkServers[0])) {
                Enqueue(mWriteIdAllocOp);
            } else {
                HandleError(mWriteIdAllocOp);
            }
        }
        void Done(
            WriteIdAllocOp& inOp,
            bool            inCanceledFlag,
            IOBuffer*       inBufferPtr)
        {
            QCASSERT(&mWriteIdAllocOp == &inOp && ! inBufferPtr);
            mWriteIds.clear();
            if (inCanceledFlag) {
                return;
            }
            if (inOp.status < 0) {
                HandleError(inOp);
                return;
            }
            const size_t theServerCount = inOp.chunkServerLoc.size();
            mWriteIds.reserve(theServerCount);
            istringstream theStream(inOp.writeIdStr);
            for (size_t i = 0; i < theServerCount; i++) {
                WriteInfo theWInfo;
                if (! (theStream >>
                        theWInfo.serverLoc.hostname >>
                        theWInfo.serverLoc.port >>
                        theWInfo.writeId)) {
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "write id alloc:"
                        " at index: "         << i <<
                        " of: "               << theServerCount <<
                        " invalid response: " << inOp.writeIdStr <<
                    KFS_LOG_EOM;
                    break;
                }
                mWriteIds.push_back(theWInfo);
            }
            if (theServerCount != mWriteIds.size()) {
                HandleError(inOp);
                return;
            }
            StartWrite();
        }
        void Write()
        {
            if (mOpDoneFlagPtr) {
                return;
            }
            bool theOpDoneFlag = false;
            mOpDoneFlagPtr = &theOpDoneFlag;
            Queue::Iterator theIt(mPendingQueue);
            WriteOp* theOpPtr;
            while (! mSleepingFlag &&
                    mErrorCode == 0 &&
                    mAllocOp.chunkId > 0 &&
                    (theOpPtr = theIt.Next())) {
                Write(*theOpPtr);
                if (theOpDoneFlag) {
                    return; // Unwind. "this" might be deleted.
                }
            }
            mOpDoneFlagPtr = 0;
        }
        void Write(
            WriteOp& inWriteOp)
        {
            while (inWriteOp.mBeginBlock < inWriteOp.mEndBlock) {
                if (mInFlightBlocks.test(inWriteOp.mBeginBlock)) {
                    return; // Wait until the in flight write done.
                }
                mInFlightBlocks.set(inWriteOp.mBeginBlock++, 1);
            }
            Reset(inWriteOp);
            inWriteOp.contentLength =
                size_t(inWriteOp.mBuffer.BytesConsumable());
            inWriteOp.mWritePrepareOp.chunkId            = mAllocOp.chunkId;
            inWriteOp.mWritePrepareOp.chunkVersion       =
                mAllocOp.chunkVersion;
            inWriteOp.mWritePrepareOp.writeInfo          = mWriteIds;
            inWriteOp.mWritePrepareOp.contentLength      =
                inWriteOp.contentLength;
            inWriteOp.mWritePrepareOp.numBytes           =
                inWriteOp.contentLength;
            inWriteOp.mWritePrepareOp.replyRequestedFlag =
                mWriteIdAllocOp.writePrepReplySupportedFlag;
            // No need to recompute checksums on retry. Presently the buffer
            // remains the unchanged.
            if (inWriteOp.mWritePrepareOp.replyRequestedFlag) {
                if (! inWriteOp.mChecksumValidFlag) {
                    inWriteOp.mWritePrepareOp.checksum = ComputeBlockChecksum(
                        &inWriteOp.mBuffer,
                        inWriteOp.mWritePrepareOp.numBytes
                    );
                    inWriteOp.mChecksumValidFlag = true;
                }
                inWriteOp.mWritePrepareOp.checksums.clear();
            } else {
                if (inWriteOp.mWritePrepareOp.checksums.empty()) {
                    inWriteOp.mWritePrepareOp.checksums = ComputeChecksums(
                        &inWriteOp.mBuffer,
                        inWriteOp.mWritePrepareOp.numBytes,
                        &inWriteOp.mWritePrepareOp.checksum
                    );
                    inWriteOp.mChecksumValidFlag = true;
                }
                inWriteOp.mWriteSyncOp.chunkId      =
                    inWriteOp.mWritePrepareOp.chunkId;
                inWriteOp.mWriteSyncOp.chunkVersion =
                    inWriteOp.mWritePrepareOp.chunkVersion;
                inWriteOp.mWriteSyncOp.offset       =
                    inWriteOp.mWritePrepareOp.offset;
                inWriteOp.mWriteSyncOp.numBytes     =
                    inWriteOp.mWritePrepareOp.numBytes;
                inWriteOp.mWriteSyncOp.writeInfo    =
                    inWriteOp.mWritePrepareOp.writeInfo;
                inWriteOp.mWriteSyncOp.checksums    =
                    inWriteOp.mWritePrepareOp.checksums;
            }
            inWriteOp.mOpStartTime = Now();
            Queue::Remove(mPendingQueue, inWriteOp);
            Queue::PushBack(mInFlightQueue, inWriteOp);
            mOuter.mStats.mOpsWriteCount++;
            Enqueue(inWriteOp, &inWriteOp.mBuffer);
        }
        void Done(
            WriteOp&  inOp,
            bool      inCanceledFlag,
            IOBuffer* inBufferPtr)
        {
            QCASSERT(
                inBufferPtr == &inOp.mBuffer &&
                Queue::IsInList(mInFlightQueue, inOp)
            );
            inOp.InitBlockRange();
            for (size_t i = inOp.mBeginBlock; i < inOp.mEndBlock; i++) {
                mInFlightBlocks.set(i, 0);
            }
            if (inCanceledFlag || inOp.status < 0) {
                Queue::Remove(mInFlightQueue, inOp);
                Queue::PushBack(mPendingQueue, inOp);
                if (! inCanceledFlag) {
                    mOpStartTime = inOp.mOpStartTime;
                    HandleError(inOp);
                }
                return;
            }
            const Offset theOffset    = inOp.mWritePrepareOp.offset;
            const Offset theDoneCount = inOp.mBuffer.BytesConsumable();
            QCASSERT(
                theDoneCount >= 0 &&
                mPendingCount >= theDoneCount
            );
            mPendingCount -= theDoneCount;
            inOp.Delete(mInFlightQueue);
            if (! ReportCompletion(theOffset, theDoneCount)) {
                return;
            }
            StartWrite();
        }
        void CloseChunk()
        {
            QCASSERT(mAllocOp.chunkId > 0);
            Reset(mCloseOp);
            mCloseOp.chunkId   = mAllocOp.chunkId;
            mCloseOp.writeInfo = mWriteIds;
            if (mCloseOp.writeInfo.empty()) {
                mCloseOp.chunkServerLoc = mAllocOp.chunkServers;
            } else {
                mCloseOp.chunkServerLoc.clear();
            }
            mWriteIds.clear();
            mAllocOp.chunkId = -1;
            Enqueue(mCloseOp);
        }
        void Done(
            CloseOp&  inOp,
            bool      inCanceledFlag,
            IOBuffer* inBufferPtr)
        {
            QCASSERT(&mCloseOp == &inOp && ! inBufferPtr);
            if (inCanceledFlag) {
                return;
            }
            if (mCloseOp.status != 0) {
                KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                    "chunk close failure, status: " << mCloseOp.status <<
                    " ignored" <<
                KFS_LOG_EOM;
            }
            Reset();
            StartWrite();
        }
        virtual void OpDone(
            KfsOp*    inOpPtr,
            bool      inCanceledFlag,
            IOBuffer* inBufferPtr)
        {
            if (mOpDoneFlagPtr) {
                *mOpDoneFlagPtr = true;
                mOpDoneFlagPtr = 0;
            }
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
            if (inCanceledFlag && inOpPtr == &mAllocOp) {
                mOuter.mStats.mMetaOpsCancelledCount++;
            }
            if (mLastOpPtr == inOpPtr) {
                mLastOpPtr = 0;
            }
            if (&mAllocOp == inOpPtr) {
                Done(mAllocOp, inCanceledFlag, inBufferPtr);
            } else if (&mWriteIdAllocOp == inOpPtr) {
                Done(mWriteIdAllocOp, inCanceledFlag, inBufferPtr);
            } else if (&mAllocOp == inOpPtr) {
                Done(mAllocOp, inCanceledFlag, inBufferPtr);
            } else if (&mCloseOp == inOpPtr) {
                Done(mCloseOp, inCanceledFlag, inBufferPtr);
            } else if (inOpPtr && inOpPtr->op == CMD_WRITE) {
                Done(*static_cast<WriteOp*>(inOpPtr),
                    inCanceledFlag, inBufferPtr);
            } else {
                mOuter.InternalError("unexpected operation completion");
            }
        }
        void Enqueue(
            KfsOp&    inOp,
            IOBuffer* inBufferPtr = 0)
            { EnqueueSelf(inOp, inBufferPtr, &mChunkServer); }
        void EnqueueMeta(
            KfsOp&    inOp,
            IOBuffer* inBufferPtr = 0)
            { EnqueueSelf(inOp, inBufferPtr, 0); }
        void Reset()
        {
            if (mLastOpPtr == &mAllocOp) {
                mOuter.mMetaServer.Cancel(mLastOpPtr, this);
            }
            Reset(mAllocOp);
            mWriteIds.clear();
            mAllocOp.chunkId = 0;
            mLastOpPtr       = 0;
            mChunkServer.Stop();
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
            inOp.contentBufLen = 0;
            delete [] inOp.contentBuf;
            inOp.contentBuf    = 0;
        }
        int GetTimeToNextRetry() const
        {
            return max(mRetryCount >= 1 ? 1 : 0,
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
                " current chunk server: " << mChunkServer.GetServerLocation() <<
                " chunkserver: "          << (mChunkServer.IsDataSent() ?
                    (mChunkServer.IsAllDataSent() ? "all" : "partial") :
                    "no") << " data sent" <<
                "\nRequest:\n"            << theOStream.str() <<
            KFS_LOG_EOM;
            int theStatus = inOp.status;
            if (&inOp == &mAllocOp && theStatus == kErrorNoEntry) {
                // File deleted, and lease expired or meta server restarted.
                KFS_LOG_STREAM_ERROR << mLogPrefix <<
                    "file does not exist, giving up" <<
                KFS_LOG_EOM;
                mErrorCode = theStatus;
                Reset();
                mOuter.FatalError(theStatus);
                return;
            }
            if (mOuter.mStriperPtr && ! mAllocOp.invalidateAllFlag &&
                    mAllocOp.fileOffset >= 0 &&
                    ! mOuter.mStriperPtr->IsWriteRetryNeeded(
                        mAllocOp.fileOffset,
                        mRetryCount,
                        mOuter.mMaxRetryCount,
                        theStatus)) {
                KFS_LOG_STREAM_INFO << mLogPrefix <<
                    "invalidate:"
                    " chunk: "   << mAllocOp.chunkId <<
                    " offset: "  << mAllocOp.fileOffset <<
                    " status: "  << inOp.status <<
                    " => "       << theStatus <<
                    " pending: " << mPendingCount <<
                    " w-empty: " << Queue::IsEmpty(mPendingQueue) <<
                KFS_LOG_EOM;
                mErrorCode = theStatus;
                mAllocOp.invalidateAllFlag = true;
                mRetryCount = 0;
                Reset();
                QCASSERT(CanWrite());
                StartWrite();
                return;
            }
            if (++mRetryCount > mOuter.mMaxRetryCount) {
                KFS_LOG_STREAM_ERROR << mLogPrefix <<
                    "max retry reached: " << mRetryCount << ", giving up" <<
                KFS_LOG_EOM;
                mErrorCode = theStatus < 0 ? theStatus : -1;
                Reset();
                mOuter.FatalError(theStatus < 0 ? theStatus : -1);
                return;
            }
            // Treat alloc failure the same as chunk server failure.
            if (&mAllocOp == mLastOpPtr) {
                mOuter.mStats.mAllocRetriesCount++;
            }
            mOuter.mStats.mRetriesCount++;
            const int theTimeToNextRetry = GetTimeToNextRetry();
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
        bool Sleep(
            int inSec)
        {
            if (inSec <= 0 || mSleepingFlag) {
                return false;
            }
            KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                "sleeping: " << inSec <<
            KFS_LOG_EOM;
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
            StartWrite();
        }
        bool ReportCompletion(
            Offset inOffset = 0,
            Offset inSize   = 0)
        {
            if (mErrorCode == 0) {
                // Reset retry counts on successful completion.
                mRetryCount = 0;
            }
            return mOuter.ReportCompletion(this, inOffset, inSize);
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
    private:
        ChunkWriter(
            const ChunkWriter& inChunkWriter);
        ChunkWriter& operator=(
            const ChunkWriter& inChunkWriter);
    };
    friend class ChunkWriter;
    friend class Striper;

    typedef ChunkWriter::Writers Writers;

    Writer&             mOuter;
    MetaServer&         mMetaServer;
    string              mPathName;
    kfsFileId_t         mFileId;
    bool                mClosingFlag;
    bool                mSleepingFlag;
    int                 mErrorCode;
    int                 mWriteThreshold;
    int                 mPartialBuffersCount;
    Offset              mPendingCount;
    const int           mIdleTimeoutSec;
    const int           mOpTimeoutSec;
    const int           mMaxRetryCount;
    const int           mTimeSecBetweenRetries;
    const int           mMaxPartialBuffersCount;
    const int           mMaxWriteSize;
    int                 mReplicaCount;
    int                 mRetryCount;
    Offset              mFileSize;
    Offset              mOffset;
    Offset              mOpenChunkBlockSize;
    int64_t             mChunkServerInitialSeqNum;
    Completion*         mCompletionPtr;
    IOBuffer            mBuffer;
    string const        mLogPrefix;
    Stats               mStats;
    KfsNetClient::Stats mChunkServersStats;
    NetManager&         mNetManager;
    TruncateOp          mTruncateOp;
    time_t              mOpStartTime;
    int                 mCompletionDepthCount;
    int                 mStriperProcessCount;
    Striper*            mStriperPtr;
    ChunkWriter*        mWriters[1];

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
        Impl::DisableCompletion();
        Impl::Shutdown();
        delete mStriperPtr;
    }
    Offset GetPendingSizeSelf() const
    {
        return (mBuffer.BytesConsumable() +
            (mStriperPtr ?
                max(Offset(0), mStriperPtr->GetPendingSize()) :
                Offset(0)
        ));
    }
    int StartWrite(
        bool inFlushFlag = false)
    {
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "start write:" <<
            " pending: " << GetPendingSizeSelf() <<
            " thresh: "  << mWriteThreshold <<
            " flush: "   << inFlushFlag <<
            (mSleepingFlag ? " SLEEPING" : "") <<
        KFS_LOG_EOM;

        if (mSleepingFlag) {
            return mErrorCode;
        }
        const bool theFlushFlag      = inFlushFlag || mClosingFlag;
        const int  theWriteThreshold =
            max(1, theFlushFlag ? 1 : mWriteThreshold);
        while (mErrorCode == 0 && GetPendingSizeSelf() >= theWriteThreshold) {
            const int thePrevRefCount = GetRefCount();
            QueueWrite(theWriteThreshold);
            if (thePrevRefCount > GetRefCount()) {
                return mErrorCode; // Unwind
            }
            if (mBuffer.IsEmpty()) {
                break;
            }
        }
        if (! mClosingFlag) {
            return mErrorCode;
        }
        if (Writers::IsEmpty(mWriters)) {
            ReportCompletion();
            return mErrorCode;
        }
        Writers::Iterator theIt(mWriters);
        ChunkWriter*      thePtr;
        while ((thePtr = theIt.Next())) {
            if (! thePtr->IsOpen()) {
                continue;
            }
            const int thePrevRefCount = GetRefCount();
            thePtr->Close();
            if (thePrevRefCount > GetRefCount()) {
                return mErrorCode; // Unwind
            }
            // Restart from the beginning as close can invoke completion
            // and remove or close more than one writer in TryToCloseIdle().
            theIt.Reset();
        }
        if (Writers::IsEmpty(mWriters) && mClosingFlag) {
            SetFileSize();
        }
        return mErrorCode;
    }
    void SetFileSize()
    {
        if (! mStriperPtr || mErrorCode != 0 || mTruncateOp.fid >= 0) {
            return;
        }
        const Offset theSize = mStriperPtr->GetFileSize();
        if (theSize < 0 || theSize <= mTruncateOp.fileOffset) {
            return;
        }
        mOpStartTime           = mNetManager.Now();
        mTruncateOp.pathname   = mPathName.c_str();
        mTruncateOp.fid        = mFileId;
        mTruncateOp.fileOffset = theSize;
        mTruncateOp.status     = 0;
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "meta +> " << mTruncateOp.Show() <<
        KFS_LOG_EOM;
        if (! mMetaServer.Enqueue(&mTruncateOp, this, 0)) {
            InternalError("meta truncate enqueue failure");
            mTruncateOp.status = kErrorFault;
            OpDone(&mTruncateOp, false, 0);
        }
    }
    virtual void OpDone(
        KfsOp*    inOpPtr,
        bool      inCanceledFlag,
        IOBuffer* inBufferPtr)
    {
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "meta <- " << (inOpPtr ? inOpPtr->Show() : string("null")) <<
            (inCanceledFlag ? " canceled" : "") <<
            " status: " << (inOpPtr ? inOpPtr->status : 0) <<
            " " << (inOpPtr ? inOpPtr->statusMsg : string()) <<
        KFS_LOG_EOM;
        QCASSERT(inOpPtr == &mTruncateOp);
        if (inOpPtr != &mTruncateOp) {
            return;
        }
        mTruncateOp.pathname = 0;
        mTruncateOp.fid      = -1;
        if (inCanceledFlag) {
            mTruncateOp.fileOffset = -1;
            return;
        }
        if (mTruncateOp.status != 0) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "set size failure:"
                " offset: " << mTruncateOp.fileOffset <<
                " status: " << mTruncateOp.status <<
                " msg: "    << mTruncateOp.statusMsg <<
                " retry: "  << mRetryCount <<
                " of: "     << mMaxRetryCount <<
            KFS_LOG_EOM;
            mTruncateOp.fileOffset = -1;
            if (++mRetryCount < mMaxRetryCount) {
                Sleep(max(
                    mRetryCount > 1 ? 1 : 0,
                    mTimeSecBetweenRetries -
                        int(mNetManager.Now() - mOpStartTime)
                ));
                if (! mSleepingFlag) {
                    StartWrite();
                }
            } else {
                FatalError(mTruncateOp.status);
            }
        } else {
            mRetryCount = 0;
            ReportCompletion();
        }
    }
    bool Sleep(
        int inSec)
    {
        if (inSec <= 0 || mSleepingFlag) {
            return false;
        }
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "sleeping: " << inSec <<
        KFS_LOG_EOM;
        mSleepingFlag = true;
        mStats.mSleepTimeSec += inSec;
        const bool kResetTimerFlag = true;
        SetTimeoutInterval(inSec * 1000, kResetTimerFlag);
        mNetManager.RegisterTimeoutHandler(this);
        return true;
    }
    virtual void Timeout()
    {
        KFS_LOG_STREAM_DEBUG << mLogPrefix << "timeout" <<
        KFS_LOG_EOM;
        if (mSleepingFlag) {
            mNetManager.UnRegisterTimeoutHandler(this);
            mSleepingFlag = false;
        }
        StartWrite();
    }
    void QueueWrite(
        int inWriteThreshold)
    {
        if (mStriperPtr) {
            QCStValueIncrementor<int> theIncrement(mStriperProcessCount, 1);
            const int theErrCode =
                mStriperPtr->Process(mBuffer, mOffset, inWriteThreshold);
            if (theErrCode != 0 && mErrorCode == 0) {
                mErrorCode = theErrCode;
            }
            return;
        }
        const int theQueuedCount = QueueWrite(
            mBuffer,
            mBuffer.BytesConsumable(),
            mOffset,
            inWriteThreshold
        );
        if (theQueuedCount > 0) {
            mOffset += theQueuedCount;
            StartQueuedWrite(theQueuedCount);
        }
    }
    int QueueWrite(
        IOBuffer& inBuffer,
        int       inSize,
        Offset    inOffset,
        int       inWriteThreshold)
    {
        QCASSERT(inOffset >= 0);
        if (inSize <= 0 || inBuffer.BytesConsumable() <= 0) {
            return 0;
        }
        const Offset theFileOffset = inOffset - inOffset % CHUNKSIZE;
        Writers::Iterator theIt(mWriters);
        ChunkWriter* thePtr;
        while ((thePtr = theIt.Next())) {
            if (thePtr->GetFileOffset() == theFileOffset) {
                break;
            }
        }
        if (thePtr) {
            Writers::PushFront(mWriters, *thePtr);
            thePtr->CancelClose();
        } else {
            mChunkServerInitialSeqNum += 10000;
            thePtr = new ChunkWriter(
                *this, mChunkServerInitialSeqNum, mLogPrefix);
        }
        const int theQueuedCount = thePtr->QueueWrite(
            inBuffer, inSize, inOffset, inWriteThreshold);
        QCASSERT(Writers::Front(mWriters) == thePtr);
        return theQueuedCount;
    }
    void StartQueuedWrite(
        int inQueuedCount)
    {
        if (inQueuedCount <= 0) {
            return;
        }
        QCASSERT(! Writers::IsEmpty(mWriters));
        mPendingCount += inQueuedCount;
        Writers::Front(mWriters)->StartWrite();
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
        ReportCompletion();
    }
    bool CanClose(
        ChunkWriter& inWriter)
    {
        if (! inWriter.IsIdle()) {
            return false;
        }
        if (! inWriter.IsOpen() || mClosingFlag) {
            return true;
        }
        // The most recently used should always be first.
        const ChunkWriter* const thePtr = Writers::Front(mWriters);
        if (! thePtr) {
            return true;
        }
        if (thePtr == &inWriter) {
            return false;
        }
        const Offset theLeftEdge = thePtr->GetOpenChunkBlockFileOffset();
        if (theLeftEdge < 0) {
            return false;
        }
        const Offset theRightEdge = theLeftEdge + mOpenChunkBlockSize;
        const Offset theOffset    = inWriter.GetFileOffset();
        return (theOffset < theLeftEdge || theRightEdge <= theOffset);
    }
    bool TryToCloseIdle(
        const ChunkWriter* inWriterPtr)
    {
        ChunkWriter* thePtr = Writers::Back(mWriters);
        if (! thePtr) {
            return (! inWriterPtr); // Already deleted.
        }
        bool theRetFlag = true;
        while (thePtr) {
            ChunkWriter& theWriter = *thePtr;
            thePtr = (thePtr == Writers::Front(mWriters)) ?
                0 : theWriter.GetPrevPtr();
            if (CanClose(theWriter)) {
                const bool theOpenFlag = theWriter.IsOpen();
                if (theOpenFlag) {
                    theWriter.Close();
                }
                // Handle "synchronous" Close(). ReportCompletion, calls
                // this method only when mCompletionDepthCount <= 1
                if (! theOpenFlag ||
                        (! theWriter.IsOpen() && CanClose(theWriter))) {
                    if (&theWriter == inWriterPtr) {
                        theRetFlag = false;
                    }
                    delete &theWriter;
                }
            } else if (theWriter.IsIdle() && theWriter.IsOpen()) {
                // Stop at the first idle that can not be closed.
                break;
            }
        }
        return theRetFlag;
    }
    bool ReportCompletion(
        ChunkWriter* inWriterPtr = 0,
        Offset       inOffset    = 0,
        Offset       inSize      = 0)
    {
        // Order matters here, as StRef desctructor can delete this.
        StRef                     theRef(*this);
        QCStValueIncrementor<int> theIncrement(mCompletionDepthCount, 1);

        QCRTASSERT(mPendingCount >= 0 && mPendingCount >= inSize);
        mPendingCount -= inSize;
        if (inWriterPtr && mErrorCode == 0) {
            mErrorCode = inWriterPtr->GetErrorCode();
        }
        const int thePrevRefCount = GetRefCount();
        if (mCompletionPtr) {
            mCompletionPtr->Done(mOuter, mErrorCode, inOffset, inSize);
        }
        bool theRet = true;
        if (mCompletionDepthCount <= 1 && mStriperProcessCount <= 0) {
            theRet = TryToCloseIdle(inWriterPtr);
            if (mClosingFlag && Writers::IsEmpty(mWriters) && ! mSleepingFlag) {
                SetFileSize();
                if (mTruncateOp.fid < 0 && ! mSleepingFlag) {
                    mClosingFlag = false;
                    mFileId = -1;
                    Striper* const theStriperPtr = mStriperPtr;
                    mStriperPtr = 0;
                    delete theStriperPtr;
                    theRet = false;
                    if (mCompletionPtr) {
                        mCompletionPtr->Done(mOuter, mErrorCode, 0, 0);
                    }
                }
            }
        }
        return (theRet && thePrevRefCount <= GetRefCount());
    }
private:
    Impl(
        const Impl& inWriter);
    Impl& operator=(
        const Impl& inWriter);
};

/* static */ Writer::Striper*
Writer::Striper::Create(
    int                      inType,
    int                      inStripeCount,
    int                      inRecoveryStripeCount,
    int                      inStripeSize,
    Writer::Striper::Offset  inFileSize,
    string                   inLogPrefix,
    Writer::Striper::Impl&   inOuter,
    Writer::Striper::Offset& outOpenChunkBlockSize,
    string&                  outErrMsg)
{
    switch (inType) {
        case kStriperTypeNone:
            outOpenChunkBlockSize = Offset(CHUNKSIZE);
        break;
        case kStriperTypeRS:
            return RSStriperCreate(
                kStriperTypeRS,
                inStripeCount,
                inRecoveryStripeCount,
                inStripeSize,
                inFileSize,
                inLogPrefix,
                inOuter,
                outOpenChunkBlockSize,
                outErrMsg
            );
        default:
            outErrMsg = "unsupported striper type";
        break;
    }
    return 0;
}

int
Writer::Striper::QueueWrite(
    IOBuffer&       inBuffer,
    int             inSize,
    Writer::Offset  inOffset,
    int             inWriteThreshold)
{
    const int theQueuedCount = mOuter.QueueWrite(
        inBuffer, inSize, inOffset, inWriteThreshold);
    mWriteQueuedFlag = theQueuedCount > 0;
    return theQueuedCount;
}

void
Writer::Striper::StartQueuedWrite(
    int inQueuedCount)
{
    if (! mWriteQueuedFlag) {
        return;
    }
    mWriteQueuedFlag = false;
    mOuter.StartQueuedWrite(inQueuedCount);
}

Writer::Writer(
    Writer::MetaServer& inMetaServer,
    Writer::Completion* inCompletionPtr               /* = 0 */,
    int                 inMaxRetryCount               /* = 6 */,
    int                 inWriteThreshold              /* = 1 << 20 */,
    int                 inMaxPartialBuffersCount      /* = 16 */,
    int                 inTimeSecBetweenRetries       /* = 15 */,
    int                 inOpTimeoutSec                /* = 30 */,
    int                 inIdleTimeoutSec              /* = 5 * 30 */,
    int                 inMaxWriteSize                /* = 1 << 20 */,
    const char*         inLogPrefixPtr                /* = 0 */,
    int64_t             inChunkServerInitialSeqNum    /* = 1 */)
    : mImpl(*new Writer::Impl(
        *this,
        inMetaServer,
        inCompletionPtr,
        inMaxRetryCount,
        inWriteThreshold,
        inMaxPartialBuffersCount,
        inTimeSecBetweenRetries,
        inOpTimeoutSec,
        inIdleTimeoutSec,
        inMaxWriteSize,
        (inLogPrefixPtr && inLogPrefixPtr[0]) ?
            (inLogPrefixPtr + string(" ")) : string(),
        inChunkServerInitialSeqNum
    ))
{
    mImpl.Ref();
}

/* virtual */
Writer::~Writer()
{
    mImpl.DisableCompletion();
    mImpl.UnRef();
}

int
Writer::Open(
    kfsFileId_t inFileId,
    const char* inFileNamePtr,
    Offset      inFileSize,
    int         inStriperType,
    int         inStripeSize,
    int         inStripeCount,
    int         inRecoveryStripeCount,
    int         inReplicaCount)
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
        inReplicaCount
    );
}

int
Writer::Close()
{
    Impl::StRef theRef(mImpl);
    return mImpl.Close();
}

int
Writer::Write(
    IOBuffer&      inBuffer,
    int            inLength,
    Writer::Offset inOffset,
    bool           inFlushFlag,
    int            inWriteThreshold)
{
    Impl::StRef theRef(mImpl);
    return mImpl.Write(
        inBuffer, inLength, inOffset, inFlushFlag, inWriteThreshold);
}

bool
Writer::IsOpen() const
{
    Impl::StRef theRef(mImpl);
    return (mImpl.IsOpen() && ! IsClosing());
}

bool
Writer::IsClosing() const
{
    Impl::StRef theRef(mImpl);
    return mImpl.IsClosing();
}

bool
Writer::IsActive() const
{
    Impl::StRef theRef(mImpl);
    return mImpl.IsActive();
}

Writer::Offset
Writer::GetPendingSize() const
{
    Impl::StRef theRef(mImpl);
    return mImpl.GetPendingSize();
}

int
Writer::GetErrorCode() const
{
    Impl::StRef theRef(mImpl);
    return mImpl.GetErrorCode();
}

int
Writer::SetWriteThreshold(
    int inThreshold)
{
    Impl::StRef theRef(mImpl);
    return mImpl.SetWriteThreshold(inThreshold);
}

int
Writer::Flush()
{
    Impl::StRef theRef(mImpl);
    return mImpl.Flush();
}

void
Writer::Stop()
{
    Impl::StRef theRef(mImpl);
    mImpl.Stop();
}

void
Writer::Shutdown()
{
    Impl::StRef theRef(mImpl);
    mImpl.Shutdown();
}

void
Writer::Register(
    Writer::Completion* inCompletionPtr)
{
    Impl::StRef theRef(mImpl);
    mImpl.Register(inCompletionPtr);
}

bool
Writer::Unregister(
   Writer::Completion* inCompletionPtr)
{
    Impl::StRef theRef(mImpl);
    return mImpl.Unregister(inCompletionPtr);
}

void
Writer::GetStats(
    Stats&               outStats,
    KfsNetClient::Stats& outChunkServersStats)
{
    Impl::StRef theRef(mImpl);
    mImpl.GetStats(outStats, outChunkServersStats);
}

}}
