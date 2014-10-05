//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: AtomicRecordAppender.h $
//
// Created 2009/03/19
// Author: Sriram Rao
//
// Copyright 2009-2012 Quantcast Corporation.
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
// \brief Data structure for tracking record appends to chunks.
//----------------------------------------------------------------------------

#ifndef CHUNK_ATOMICRECORDAPPENDER_H
#define CHUNK_ATOMICRECORDAPPENDER_H

#include <string>

#include "DiskIo.h"
#include "KfsOps.h"
#include "common/kfsdecls.h"
#include "common/LinearHash.h"

class QCMutex;

namespace KFS
{
class AtomicRecordAppender;
class Properties;

// Write append append globals.
class AtomicRecordAppendManager {
public:
    struct Counters
    {
        typedef int64_t Counter;

        Counter mAppendCount;
        Counter mAppendByteCount;
        Counter mAppendErrorCount;
        Counter mReplicationErrorCount;
        Counter mReplicationTimeoutCount;
        Counter mAppenderAllocCount;
        Counter mAppenderAllocMasterCount;
        Counter mAppenderAllocErrorCount;
        Counter mWriteIdAllocCount;
        Counter mWriteIdAllocErrorCount;
        Counter mWriteIdAllocNoAppenderCount;
        Counter mSpaceReserveCount;
        Counter mSpaceReserveByteCount;
        Counter mSpaceReserveDeniedCount;
        Counter mSpaceReserveErrorCount;
        Counter mBeginMakeStableCount;
        Counter mBeginMakeStableErrorCount;
        Counter mMakeStableCount;
        Counter mMakeStableErrorCount;
        Counter mMakeStableLengthErrorCount;
        Counter mMakeStableChecksumErrorCount;
        Counter mChecksumErrorCount;
        Counter mReadErrorCount;
        Counter mWriteErrorCount;
        Counter mGetOpStatusCount;
        Counter mGetOpStatusErrorCount;
        Counter mGetOpStatusKnownCount;
        Counter mLeaseExpiredCount;
        Counter mTimeoutLostCount;
        Counter mLostChunkCount;
        Counter mPendingByteCount;
        Counter mLowOnBuffersFlushCount;

        void Clear()
        {
            mAppendCount = 0;
            mAppendByteCount = 0;
            mAppendErrorCount = 0;
            mReplicationErrorCount = 0;
            mReplicationTimeoutCount = 0;
            mAppenderAllocCount = 0;
            mAppenderAllocMasterCount = 0;
            mAppenderAllocErrorCount = 0;
            mWriteIdAllocCount = 0;
            mWriteIdAllocErrorCount = 0;
            mWriteIdAllocNoAppenderCount = 0;
            mSpaceReserveCount = 0;
            mSpaceReserveByteCount = 0;
            mSpaceReserveDeniedCount = 0;
            mSpaceReserveErrorCount = 0;
            mBeginMakeStableCount = 0;
            mBeginMakeStableErrorCount = 0;
            mMakeStableCount = 0;
            mMakeStableErrorCount = 0;
            mMakeStableLengthErrorCount = 0;
            mMakeStableChecksumErrorCount = 0;
            mChecksumErrorCount = 0;
            mReadErrorCount = 0;
            mWriteErrorCount = 0;
            mGetOpStatusCount = 0;
            mGetOpStatusErrorCount = 0;
            mGetOpStatusKnownCount = 0;
            mLeaseExpiredCount = 0;
            mTimeoutLostCount = 0;
            mLostChunkCount = 0;
            mPendingByteCount = 0;
            mLowOnBuffersFlushCount = 0;
        }
    };
    AtomicRecordAppendManager();
    ~AtomicRecordAppendManager();
    void SetParameters(const Properties& props);
    void AllocateChunk(AllocChunkOp *op, int replicationPos,
        const ServerLocation& peerLoc, const DiskIo::FilePtr& chunkFileHandle);
    void AllocateWriteId(WriteIdAllocOp *op, int replicationPos,
        const ServerLocation& peerLoc, const DiskIo::FilePtr& chunkFileHandle);
    int GetCleanUpSec() const
        { return mCleanUpSec; }
    int GetCloseEmptyWidStateSec() const
        { return mCloseEmptyWidStateSec; }
    int GetFlushIntervalSec() const
        { return mFlushIntervalSec; }
    int GetSendCommitAckTimeoutSec() const
        { return mSendCommitAckTimeoutSec; }
    int GetReplicationTimeoutSec() const
        { return mReplicationTimeoutSec; }
    int GetMetaMinUptimeSec() const
        { return mMinMetaUptimeSec; }
    int GetFlushLimit() const { return mFlushLimit; }
    double GetBufferLimitRatio() const
        { return mBufferLimitRatio; }
    int GetMaxWriteIdsPerChunk() const
        { return mMaxWriteIdsPerChunk; }
    int GetCloseOutOfSpaceThreshold() const
        { return mCloseOutOfSpaceThreshold; }
    int GetCloseOutOfSpaceSec()       const
        { return mCloseOutOfSpaceSec; }
    chunkOff_t GetCloseMinChunkSize() const
        { return mCloseMinChunkSize; }
    bool IsChunkStable(kfsChunkId_t chunkId) const;
    /// For record appends, (1) clients will reserve space in a chunk and
    /// then write and (2) clients can release their reserved space.
    /// As long as space is reserved on a chunk, the chunkserver will
    /// renew the write lease with the metaserver.
    /// @param[in] chunkId  id of the chunk for which space should be
    ///                     reserved/released
    /// @param[in] nbytes  # of bytes of space reservation/release
    /// @retval status code
    ///
    int ChunkSpaceReserve(kfsChunkId_t chunkId, int64_t writeId,
        size_t nBytes, std::string* errMsg = 0);
    int ChunkSpaceRelease(kfsChunkId_t chunkId, int64_t writeId,
        size_t nBytes, std::string* errMsg = 0);
    int InvalidateWriteId(kfsChunkId_t chunkId, int64_t writeId,
        bool declareFailureFlag = false);
    int InvalidateWriteIdDeclareFailure(kfsChunkId_t chunkId, int64_t writeId) {
        return InvalidateWriteId(chunkId, writeId, true);
    }
    int64_t GetOpenAppendersCount() const {
        return mOpenAppendersCount;
    }
    int64_t GetAppendersWithWidCount() const {
        return mAppendersWithWidCount;
    }
    bool IsSpaceReservedInChunk(kfsChunkId_t chunkId);
    int GetAlignmentAndFwdFlag(kfsChunkId_t chunkId, bool& forwardFlag) const;
    bool CloseChunk(CloseOp* op, int64_t writeId, bool& forwardFlag);
    bool BeginMakeChunkStable(BeginMakeChunkStableOp* op);
    bool MakeChunkStable(MakeChunkStableOp* op);
    void AppendBegin(RecordAppendOp* op, int replicationPos,
        const ServerLocation& peerLoc);
    void GetOpStatus(GetRecordAppendOpStatus* op);
    bool WantsToKeepLease(kfsChunkId_t chunkId) const;
    void Timeout();
    void FlushIfLowOnBuffers();
    void DeleteChunk(kfsChunkId_t chunkId);
    void Shutdown();
    size_t GetAppendersCount() const
        { return mAppenders.GetSize(); }
    void GetCounters(Counters& outCounters)
        { outCounters = mCounters; }

    void UpdateAppenderFlushLimit(const AtomicRecordAppender* appender = 0);
    int GetFlushLimit(AtomicRecordAppender& appender, int addBytes = 0);
    int GetAppendDropLockMinSize() const
        { return mAppendDropLockMinSize; }
    inline void UpdatePendingFlush(AtomicRecordAppender& appender);
    inline void Detach(AtomicRecordAppender& appender);
    inline void DecOpenAppenderCount();
    inline void IncAppendersWithWidCount();
    inline void DecAppendersWithWidCount();
    inline Counters& Cntrs();

private:
    typedef KVPair<kfsChunkId_t, AtomicRecordAppender*> ARAMapEntry;
    typedef LinearHash<
        ARAMapEntry,
        KeyCompare<kfsChunkId_t>,
        DynamicArray<
            SingleLinkedList<ARAMapEntry>*,
            8 // 256 entries
        >,
        StdFastAllocator<ARAMapEntry>
    > ARAMap;

    ARAMap                mAppenders;
    int                   mCleanUpSec;
    int                   mCloseEmptyWidStateSec;
    int                   mFlushIntervalSec;
    int                   mSendCommitAckTimeoutSec;
    int                   mReplicationTimeoutSec;
    int                   mMinMetaUptimeSec;
    int                   mFlushLimit;
    int                   mMaxAppenderBytes;
    int64_t               mTotalBuffersBytes;
    int64_t               mTotalPendingBytes;
    int64_t               mActiveAppendersCount;
    int64_t               mOpenAppendersCount;
    int64_t               mAppendersWithWidCount;
    double                mBufferLimitRatio;
    int                   mMaxWriteIdsPerChunk;
    int                   mCloseOutOfSpaceThreshold;
    int                   mCloseOutOfSpaceSec;
    int                   mRecursionCount;
    int                   mAppendDropLockMinSize;
    chunkOff_t            mCloseMinChunkSize;
    int                   mMutexesCount;
    int                   mCurMutexIdx;
    QCMutex*              mMutexes;
    AtomicRecordAppender* mPendingFlushList[1];
    AtomicRecordAppender* mCurUpdateFlush;
    AtomicRecordAppender* mCurUpdateLowBufFlush;
    const uint64_t        mInstanceNum;
    Counters              mCounters;

    inline void UpdatePendingFlushIterators(AtomicRecordAppender& appender);
};

extern AtomicRecordAppendManager gAtomicRecordAppendManager;
}

#endif // CHUNK_ATOMICRECORDAPPENDER_H
