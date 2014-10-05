//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: AtomicRecordAppender.cc $
//
// Created 2009/03/19
// Author: Sriram Rao
//
// 2009/10/19
// Mike Ovsiannikov
// Fault tolerant write append protocol
//
// Copyright 2009-2012 Quantcast Corporation.  All rights reserved.
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

/*

Fault tolerant (reliable) write append protocol overview.

At the high level the synchronous replication is the same as the replication
used in the "normal" (random) KFS write.

The client sends data to the first chunk server in the replication chain:
"append master", then the "master" sends to the first "slave", then first slave
sends the data to the next slave, and so on, until the last participant in the
chain is reached.

The replication acknowledgment is propagated trough the replication chain in
the opposite direction. Positive ACK received by the master guarantees that all
other replication participants have successfully received the data.

Each chunk server maintains protocol memory for each chunk replica opened for
"write append".

The protocol state is needed to deal with unreliable communications, including
crash. Crash can be regarded as special case of communication failure. Timeout
can be regarded as another special case.

The protocol state is a table (WriteIdState) that keeps track of the status of
the last write append operation for a every client that issued (or can
potentially issue write append to the chunk). In KFS terms this corresponds to
"write id allocation". Thus the table will have one entry per write id (normally
<chunk size> / <space reservation size> or 64M/1M = 64 entries).

From protocol point of view the append operation status in the table can be one
of the three following:
"in progress" (kErrStatusInProgress),
"success"     (kErrNone),
"fail"        (!= kErrStatusInProgress && != kErrNone).

The client (appender) can query the table in order to unambiguously recover from
the communication failure by first querying [write append] master for the last
operation status for the given chunk, and write id.

In the case where the communication with chunk master fails, the client queries
other participating chunk servers for the append operation status.

The status inquiry has the following side effect.
The status inquiry always makes write id "read only", and un-reserves the space,
thus disallowing further appends with this write id. This guarantees that after
status inquiry returns, no other [new, or stale] appends with this write id can
succeed.

For status recovery to work correctly it is required that if "Begin make chunk
stable" is executed, then the replica is truncated to the last known
successfully replicated size ("commit offset").  See comment in
BeginMakeStable().

To reduce append status recovery latency chunk master piggies back its view of
replication status on the replication RPC. See comment in
UpdateMasterCommittedOffset().

The chunk can be in two states "unstable" -- dirty / non readable, and "stable"
-- read only. Only meta server can make a decition to transition chunk replica
into the "stable" state.

When chunk master declares write append synchronous replication failure, or
decides to stop accepting write appends for other [normal] reasons (chunk full,
no clients etc), it stops accepting write appends, sends meta server its replica
state: chunk checksum and size. "Lease Relinquish" RPC with the chunk size and
chunk checksum is used in this case.

The meta server broadcasts this information to all participants, including
master. Each participant attempts to converge its replica to the state
broadcasted.

In case of failure the chunk replica is declared "corrupted", and scheduled for
re-replication by the meta server.

The meta server keeps track of the chunk master operational status, as it does
for "normal" writes: heartbeat, and write lease renewal mechanisms.

If meta server decides that the chunk master is non operational, it takes over,
and performs recovery as follows.

First meta server instructs all chunk servers hosting the chunk to stop
accepting write appends to the chunk, and report back the chunk replica state:
size, and checksum. "Begin Make Chunk Stable" RPC is used for this purpose.

Once meta server gets sufficient # of replies, it picks the shortest replica,
and instructs all participants to converge replicas to the selected state.
"Make Chunk Stable" RPC is used to convey this information.

Then meta server updates list of chunk servers hosting the chunk based on the
"Make Chunk Stable" RPC reply status.

*/

#include "AtomicRecordAppender.h"
#include "ChunkManager.h"
#include "LeaseClerk.h"
#include "DiskIo.h"
#include "BufferManager.h"
#include "ClientSM.h"
#include "ChunkServer.h"
#include "MetaServerSM.h"
#include "DiskIo.h"
#include "utils.h"
#include "ClientManager.h"
#include "ClientThread.h"

#include "common/MsgLogger.h"
#include "common/StdAllocator.h"
#include "common/RequestParser.h"
#include "common/IntToString.h"

#include "kfsio/Globals.h"
#include "kfsio/ChunkAccessToken.h"

#include "qcdio/QCDLList.h"
#include "qcdio/QCMutex.h"
#include "qcdio/qcstutils.h"

#include <algorithm>
#include <vector>
#include <map>
#include <cerrno>

namespace KFS {
using std::map;
using std::less;
using std::pair;
using std::make_pair;
using std::min;
using std::max;
using std::string;

#define WAPPEND_LOG_STREAM_PREFIX << "W" << mInstanceNum << "A "
#define WAPPEND_LOG_STREAM(pri)  KFS_LOG_STREAM(pri)  WAPPEND_LOG_STREAM_PREFIX
#define WAPPEND_LOG_STREAM_DEBUG KFS_LOG_STREAM_DEBUG WAPPEND_LOG_STREAM_PREFIX
#define WAPPEND_LOG_STREAM_WARN  KFS_LOG_STREAM_WARN  WAPPEND_LOG_STREAM_PREFIX
#define WAPPEND_LOG_STREAM_INFO  KFS_LOG_STREAM_INFO  WAPPEND_LOG_STREAM_PREFIX
#define WAPPEND_LOG_STREAM_ERROR KFS_LOG_STREAM_ERROR WAPPEND_LOG_STREAM_PREFIX
#define WAPPEND_LOG_STREAM_FATAL KFS_LOG_STREAM_FATAL WAPPEND_LOG_STREAM_PREFIX

typedef QCDLList<RecordAppendOp> AppendReplicationList;

RecordAppendOp::RecordAppendOp(kfsSeq_t s)
    : ChunkAccessRequestOp(CMD_RECORD_APPEND, s),
      clientSeq(s),
      chunkVersion(-1),
      numBytes(0),
      offset(-1),
      fileOffset(-1),
      numServers(0),
      checksum(0),
      servers(),
      masterCommittedOffset(-1),
      dataBuf(),
      chunkAccessLength(0),
      accessFwdLength(0),
      syncReplicationAccess(),
      origClnt(0),
      origSeq(s),
      replicationStartTime(0),
      devBufMgr(0),
      clientSeqVal()
{
    AppendReplicationList::Init(*this);
}

RecordAppendOp::~RecordAppendOp()
{
    assert(! origClnt && ! QCDLListOp<RecordAppendOp>::IsInList(*this));
}

/* virtual */ ostream&
RecordAppendOp::ShowSelf(ostream& os) const
{
    return os <<
        "record-append:"
        " seq: "              << seq <<
        " chunk: "            << chunkId <<
        " chunkversion: "     << chunkVersion <<
        " file-offset: "      << fileOffset <<
        " writeId = "         << writeId <<
        " offset: "           << offset <<
        " numBytes: "         << numBytes <<
        " servers: "          << servers <<
        " checksum: "         << checksum <<
        " client-seq: "       << clientSeq <<
        " master-committed: " << masterCommittedOffset
    ;
}

typedef QCDLList<AtomicRecordAppender> PendingFlushList;

inline AtomicRecordAppendManager::Counters& AtomicRecordAppendManager::Cntrs()
    { return mCounters; }

inline void
AtomicRecordAppendManager::IncAppendersWithWidCount()
{
    mAppendersWithWidCount++;
    assert(mAppendersWithWidCount > 0);
}

inline void
AtomicRecordAppendManager::DecAppendersWithWidCount()
{
    assert(mAppendersWithWidCount > 0);
    if (mAppendersWithWidCount > 0) {
        mAppendersWithWidCount--;
    }
}

// One per chunk
class AtomicRecordAppender : public KfsCallbackObj
{
public:
    enum
    {
        kErrNone              = 0,
        kErrParameters        = -EINVAL,
        kErrProtocolState     = -ENXIO,
        kErrStatusInProgress  = -EAGAIN,
        kErrWidReadOnly       = -EROFS,
        kErrFailedState       = -EFAULT,
        kErrOutOfSpace        = -ENOSPC,
        kErrNotFound          = -ENOENT,
        kErrReplicationFailed = -EHOSTUNREACH,
        kErrBadChecksum       = -EBADCKSUM
    };
    template<typename T>
    AtomicRecordAppender(
        const DiskIo::FilePtr& chunkFileHandle,
        kfsChunkId_t           chunkId,
        int64_t                chunkVersion,
        uint32_t               numServers,
        const T&               servers,
        ServerLocation         peerLoc,
        int                    replicationPos,
        int64_t                chunkSize,
        const RemoteSyncSMPtr& peer,
        QCMutex*               mutex);
    void MakeChunkStableEx(MakeChunkStableOp* op)
    {
        QCStMutexLocker lock(mMutex);
        MakeChunkStable(op);
    }
    bool IsChunkStable() const
    {
        return (
            mState != kStateOpen &&
            mState != kStateClosed &&
            mState != kStateReplicationFailed
        );
    }
    int GetAlignmentAndFwdFlag(bool& forwardFlag) const
    {
        forwardFlag = IsOpen() && mPeer;
        return mAlignment;
    }
    kfsChunkId_t GetChunkId() const
        { return mChunkId; }
    size_t SpaceReserved() const
        { return mBytesReserved; }
    bool WantsToKeepLease() const
        { return (IsMaster() && ! IsChunkStable()); }
    void AllocateWriteId(WriteIdAllocOp *op, int replicationPos,
        ServerLocation peerLoc, const DiskIo::FilePtr& chunkFileHandle);
    int  ChangeChunkSpaceReservaton(
        int64_t writeId, size_t nBytesIn, bool releaseFlag, string* errMsg);
    int  InvalidateWriteId(int64_t writeId, bool declareFailureFlag);
    void AppendChunkBegin(RecordAppendOp *op, int replicationPos,
        ServerLocation peerLoc)
    {
        QCStMutexLocker lock(mMutex);
        AppendBegin(op, replicationPos, peerLoc);
    }
    void GetOpStatus(GetRecordAppendOpStatus* op);
    void CloseChunk(CloseOp* op, int64_t writeId, bool& forwardFlag);
    bool CanDoLowOnBuffersFlush() const
        { return mCanDoLowOnBuffersFlushFlag; }
    void LowOnBuffersFlush()
    {
        QCStMutexLocker lock(mMutex);
        FlushFullBlocks();
    }
    void UpdateFlushLimit(int flushLimit)
    {
        if (mAppendInProgressFlag) {
            // Do not attempt to acquire the mutex, as doing so will reduce
            // the number of concurrent checksum calculations / appends.
            return;
        }
        QCStMutexLocker lock(mMutex);
        if (mBuffer.BytesConsumable() > flushLimit) {
            FlushFullBlocks();
        }
    }
    int  EventHandler(int code, void *data);
    void DeleteChunk();
    bool Delete();
    bool IsOpen() const
        { return (mState == kStateOpen); }
    template<typename T>
    int  CheckParameters(
        int64_t chunkVersion, uint32_t numServers, const T& servers,
        int replicationPos, ServerLocation peerLoc,
        const DiskIo::FilePtr& fileHandle, string& msg);
    static bool ComputeChecksum(
        kfsChunkId_t chunkId, int64_t chunkVersion,
        int64_t& chunkSize, uint32_t& chunkChecksum);
    void FatalError(const char* msg = "AtomicRecordAppender internal error")
        { die(msg); }
    void BeginChunkMakeStable(BeginMakeChunkStableOp* op)
    {
        QCStMutexLocker lock(mMutex);
        BeginMakeStable(op);
    }

private:
    enum State
    {
        kStateNone              = 0,
        kStateOpen              = 1,
        kStateClosed            = 2,
        kStateReplicationFailed = 3,
        kStateStable            = 4,
        kStateChunkLost         = 5,
        kStatePendingDelete     = 6,
        kNumStates
    };
    struct WIdState
    {
        WIdState()
            : mBytesReserved(0),
              mLength(0),
              mOffset(-1),
              mSeq(0),
              mAppendCount(0),
              mStatus(0),
              mReadOnlyFlag(false)
            {}
        size_t   mBytesReserved;
        size_t   mLength;
        int64_t  mOffset;
        kfsSeq_t mSeq;
        uint64_t mAppendCount;
        int      mStatus;
        bool     mReadOnlyFlag;
    };
    typedef map<int64_t, WIdState, less<int64_t>,
        StdFastAllocator<pair<const int64_t, WIdState > >
    > WriteIdState;
    typedef NetManager::Timer Timer;

    const int               mReplicationPos;
    const uint32_t          mNumServers;
    const kfsChunkId_t      mChunkId;
    const int64_t           mChunkVersion;
    // Bump file ref. count to prevent chunk manager from closing the file and
    // unloading checksums.
    DiskIo::FilePtr         mChunkFileHandle;
    const StringBufT<256>   mCommitAckServers;
    const ServerLocation    mPeerLocation;
    // Protocol state.
    State                   mState;
    // The list of ops to be notified once finalize is
    // finished (as in, all dirty data is flushed out to disk and
    // the  metadata is also pushed down).
    MakeChunkStableOp*      mMakeChunkStableOp;
    MakeChunkStableOp*      mLastMakeChunkStableOp;
    BeginMakeChunkStableOp* mBeginMakeChunkStableOp;
    BeginMakeChunkStableOp* mLastBeginMakeChunkStableOp;
    // Timer
    time_t                  mLastActivityTime;
    time_t                  mLastAppendActivityTime;
    time_t                  mLastFlushTime;
    // when records are streamed in from clients, they are
    // buffered by this object and then committed to disk.  This
    // field tracks the next offset in the file at which a record
    // should be appended.
    int64_t                 mNextOffset;
    // Next committed append offset.
    int64_t                 mNextCommitOffset;
    int64_t                 mCommitOffsetAckSent;
    // Disk write position.
    int64_t                 mNextWriteOffset;
    int64_t                 mMasterCommittedOffset;
    // Disk write buffer.
    IOBuffer                mBuffer;
    int                     mBufFrontPadding;
    int                     mAlignment;
    int                     mIoOpsInFlight;
    int                     mReplicationsInFlight;
    size_t                  mBytesReserved;
    uint64_t                mAppendCommitCount;
    uint32_t                mChunkChecksum;
    int64_t                 mChunkSize; // To report to meta server
    bool                    mStaggerRMWInFlightFlag:1;
    bool                    mRestartFlushFlag:1;
    bool                    mFlushFullBlocksFlag:1;
    bool                    mCanDoLowOnBuffersFlushFlag:1;
    bool                    mMakeStableSucceededFlag:1;
    bool                    mFirstFwdOpFlag:1;
    bool                    mPendingBadChecksumFlag:1;
    // Do not use bit field for mAppendInProgressFlag, as it can be read with no
    // mMutex acquired. See UpdateFlushLimit()
    bool                    mAppendInProgressFlag;
    const uint64_t          mInstanceNum;
    int                     mConsecutiveOutOfSpaceCount;
    int                     mWritableWidCount;
    WriteIdState            mWriteIdState;
    vector<uint32_t>        mTmpChecksums;
    Timer                   mTimer;
    const RemoteSyncSMPtr   mPeer;
    QCMutex* const          mMutex;
    RecordAppendOp*         mPendingSubmitQueue;
    int                     mFlushStartByteCount;
    RecordAppendOp*         mReplicationList[1];
    AtomicRecordAppender*   mPrevPtr[1];
    AtomicRecordAppender*   mNextPtr[1];

    friend class QCDLListOp<AtomicRecordAppender, 0>;

    ~AtomicRecordAppender();
    static inline time_t Now()
        { return libkfsio::globalNetManager().Now(); }
    void SetState(State state, bool notifyIfLostFlag = true);
    const char* GetStateAsStr() const
        { return GetStateAsStr(mState); }
    const char* GetStateAsStr(
        State state) const
    {
        return ((state < kNumStates && state >= 0) ?
            sStateNames[state] : "invalid");
    }
    int64_t GetChunkSize() const
    {
        const ChunkInfo_t* const info = gChunkManager.GetChunkInfo(mChunkId);
        return (info ? info->chunkSize : -1);
    }
    bool IsChunkOpen() const
    {
        const ChunkInfo_t* const info = gChunkManager.GetChunkInfo(mChunkId);
        return (info && (info->chunkBlockChecksum || info->chunkSize == 0));
    }
    inline void SetCanDoLowOnBuffersFlushFlag(bool flag);
    void UpdateMasterCommittedOffset(int64_t masterCommittedOffset);
    void AppendCommit(RecordAppendOp *op);
    // helper function that flushes the buffered data.  the input
    // argument specifies whether the flush on the buffered data
    // should be aligned to checksum blocks.
    void FlushSelf(bool flushFullChecksumBlocks);
    void FlushFullBlocks()
        { FlushSelf(true); }
    void FlushAll()
        { FlushSelf(false); }
    int GetNextReplicationTimeout() const;
    void OpDone(WriteOp* op);
    void OpDone(RecordAppendOp* op);
    void OpDone(ReadOp* op);
    bool DeleteIfNeeded()
    {
        if (mState == kStatePendingDelete) {
            DeleteSelf();
            return true;
        }
        return false;
    }
    void Timeout();
    template<typename T>
    void CheckLeaseAndChunk(const char* prefix, T* op);
    void MetaWriteDone(int status);
    void MakeChunkStableDone();
    bool ComputeChecksum();
    void SubmitResponse(MakeChunkStableOp& op);
    void SubmitResponse(BeginMakeChunkStableOp& op);
    bool TryToCloseChunk();
    void TrimToLastCommit(const char* inMsgPtr);
    void NotifyChunkClosed();
    void SendCommitAck();
    void IncAppendersWithWidCount()
        { gAtomicRecordAppendManager.IncAppendersWithWidCount(); }
    void DecAppendersWithWidCount()
        { gAtomicRecordAppendManager.DecAppendersWithWidCount(); }
    bool DeleteSelf();
    void BeginMakeStable(BeginMakeChunkStableOp* op = 0);
    void MakeChunkStable(MakeChunkStableOp* op = 0);
    void AppendBegin(RecordAppendOp *op, int replicationPos,
        ServerLocation peerLoc);
    bool IsMaster() const
        { return (mReplicationPos == 0); }
    void Relock(ClientThread& cliThread);
    void RunPendingSubmitQueue();
    bool IsMasterAck(const RecordAppendOp& op) const
    {
        return (
            op.numBytes == 0 && op.writeId == -1 &&
            (IsMaster() ?
                ((op.origClnt ? op.origClnt : op.clnt) == this) :
                (op.masterCommittedOffset >= 0))
        );
    }
    int GetCloseEmptyWidStateSec()
    {
        if (gAtomicRecordAppendManager.GetCloseOutOfSpaceThreshold() <
                    mConsecutiveOutOfSpaceCount ||
                (mBytesReserved <= 0 &&
                    gAtomicRecordAppendManager.GetCloseMinChunkSize() <=
                    mNextOffset)) {
            return gAtomicRecordAppendManager.GetCloseOutOfSpaceSec();
        }
        return gAtomicRecordAppendManager.GetCloseEmptyWidStateSec();
    }
    void UpdateAlignment()
    {
        // Front padding can only be non 0 if buffer is empty. See assertion in
        // FlushSelf().
        mAlignment = mBufFrontPadding + mBuffer.BytesConsumableLast();
    }
    template<typename T> bool UpdateSession(T& op, int& err, string& errMsg)
    {
        if (! mPeer || ! op.syncReplicationAccess.chunkServerAccess) {
            return true;
        }
        SRChunkServerAccess& csa = *op.syncReplicationAccess.chunkServerAccess;
        mPeer->UpdateSession(
                csa.token.mPtr,
                csa.token.mLen,
                csa.key.mPtr,
                csa.key.mLen,
                IsMaster(),
                err,
                errMsg
        );
        return (err == 0);
    }
    template <typename OpT> void SubmitResponse(OpT*& listHead, OpT*& listTail)
    {
        OpT* op = listHead;
        listHead = 0;
        listTail = 0;
        while (op) {
            OpT& cur = *op;
            op = op->next;
            SubmitResponse(cur);
        }
    }
    template <typename OpT> void PushBack(
        OpT*& listHead, OpT*& listTail, OpT* op)
    {
        if (listHead) {
            listTail->next = op;
            while (listTail->next) {
                listTail = listTail->next;
            }
        } else {
            listHead = op;
            listTail = op;
        }
    }
    template<typename T>
    static StringBufT<256> MakeCommitAckServers(
        uint32_t numServers, const T& servers)
    {
        StringBufT<256>   ret;
        const char*       ptr = servers.data();
        const char* const end = ptr + servers.size();
        for (uint32_t i = 0; i < numServers; ) {
            // Host Port
            for (int k = 0; k < 2; k++) {
                while (ptr < end && (*ptr & 0xFF) <= ' ') {
                    ++ptr;
                }
                const char* const sptr = ptr;
                while (ptr < end && ' ' < (*ptr & 0xFF)) {
                    ++ptr;
                }
                ret.Append(sptr, ptr - sptr).Append(' ');
            }
            ret.Append(++i < numServers ? "-1 " : "-1");  // Write id.
        }
        return ret;
    }
    static uint64_t          sInstanceNum;
    static const char* const sStateNames[kNumStates];
    static AtomicRecordAppendManager::Counters& Cntrs()
        { return gAtomicRecordAppendManager.Cntrs(); }
    bool MasterUseShortCloseTimeout() const
    {
        return (mWritableWidCount <= 0 ||
            (mBytesReserved <= 0 &&
                gAtomicRecordAppendManager.GetCloseMinChunkSize() <=
                    mNextOffset)
        );
    }
    void DecrementWritableWidCount()
    {
        mWritableWidCount--;
        if (mWritableWidCount <= 0) {
            assert(mWritableWidCount == 0);
            DecAppendersWithWidCount();
        }
    }
private:
    // No copy.
    AtomicRecordAppender(const AtomicRecordAppender&);
    AtomicRecordAppender& operator=(const AtomicRecordAppender&);
};

const char* const AtomicRecordAppender::sStateNames[kNumStates] =
{
    "none",
    "open",
    "closed",
    "replication failed",
    "stable",
    "chunk lost",
    "pending delete"
};
uint64_t AtomicRecordAppender::sInstanceNum = 10000;

inline void
AtomicRecordAppendManager::UpdatePendingFlushIterators(
    AtomicRecordAppender& appender)
{
    if (mCurUpdateFlush == &appender &&
            (mCurUpdateFlush = &PendingFlushList::GetNext(appender)) ==
            PendingFlushList::Front(mPendingFlushList)) {
        mCurUpdateFlush = 0;
    }
    if (mCurUpdateLowBufFlush == &appender &&
            (mCurUpdateLowBufFlush = &PendingFlushList::GetNext(appender)) ==
            PendingFlushList::Front(mPendingFlushList)) {
        mCurUpdateLowBufFlush = 0;
    }
}

inline void
AtomicRecordAppendManager::UpdatePendingFlush(AtomicRecordAppender& appender)
{
    if (appender.CanDoLowOnBuffersFlush()) {
        if (! PendingFlushList::IsInList(mPendingFlushList, appender)) {
            PendingFlushList::PushFront(mPendingFlushList, appender);
        }
    } else {
        UpdatePendingFlushIterators(appender);
        PendingFlushList::Remove(mPendingFlushList, appender);
    }
}

inline void
AtomicRecordAppendManager::Detach(AtomicRecordAppender& appender)
{
    const size_t cnt = mAppenders.Erase(appender.GetChunkId());
    if (cnt != 1) {
        WAPPEND_LOG_STREAM_FATAL <<
            "appender detach: "  << (const void*)&appender <<
            " chunk: "           << appender.GetChunkId() <<
            " appenders count: " << mAppenders.GetSize() <<
        KFS_LOG_EOM;
        appender.FatalError();
    }
    UpdatePendingFlushIterators(appender);
    PendingFlushList::Remove(mPendingFlushList, appender);
}

inline void
AtomicRecordAppendManager::DecOpenAppenderCount()
{
    if (mOpenAppendersCount > 0) {
        mOpenAppendersCount--;
    }
}

inline void
AtomicRecordAppender::SetCanDoLowOnBuffersFlushFlag(bool flag)
{
    if (mCanDoLowOnBuffersFlushFlag != flag) {
        mCanDoLowOnBuffersFlushFlag = flag;
        gAtomicRecordAppendManager.UpdatePendingFlush(*this);
    }
}

template<typename T>
AtomicRecordAppender::AtomicRecordAppender(
    const DiskIo::FilePtr& chunkFileHandle,
    kfsChunkId_t           chunkId,
    int64_t                chunkVersion,
    uint32_t               numServers,
    const T&               servers,
    ServerLocation         peerLoc,
    int                    replicationPos,
    int64_t                chunkSize,
    const RemoteSyncSMPtr& peer,
    QCMutex*               mutex)
    : KfsCallbackObj(),
      mReplicationPos(replicationPos),
      mNumServers(numServers),
      mChunkId(chunkId),
      mChunkVersion(chunkVersion),
      mChunkFileHandle(chunkFileHandle),
      mCommitAckServers(MakeCommitAckServers(numServers, servers)),
      mPeerLocation(peerLoc),
      mState(kStateOpen),
      mMakeChunkStableOp(0),
      mLastMakeChunkStableOp(0),
      mBeginMakeChunkStableOp(0),
      mLastBeginMakeChunkStableOp(0),
      mLastActivityTime(Now()),
      mLastAppendActivityTime(mLastActivityTime),
      mLastFlushTime(Now()),
      mNextOffset(chunkSize),
      mNextCommitOffset(chunkSize),
      mCommitOffsetAckSent(mNextCommitOffset),
      mNextWriteOffset(chunkSize),
      mMasterCommittedOffset(-1),
      mBuffer(),
      mBufFrontPadding(0),
      mAlignment(0),
      mIoOpsInFlight(0),
      mReplicationsInFlight(0),
      mBytesReserved(0),
      mAppendCommitCount(0),
      mChunkChecksum(0),
      mChunkSize(-1),
      mStaggerRMWInFlightFlag(false),
      mRestartFlushFlag(false),
      mFlushFullBlocksFlag(false),
      mCanDoLowOnBuffersFlushFlag(false),
      mMakeStableSucceededFlag(false),
      mFirstFwdOpFlag(true),
      mPendingBadChecksumFlag(false),
      mAppendInProgressFlag(false),
      mInstanceNum(++sInstanceNum),
      mConsecutiveOutOfSpaceCount(0),
      mWritableWidCount(0),
      mWriteIdState(),
      mTmpChecksums(),
      mTimer(
        libkfsio::globalNetManager(),
        *this,
        gAtomicRecordAppendManager.GetCleanUpSec()
      ),
      mPeer(peer),
      mMutex(mutex),
      mPendingSubmitQueue(0),
      mFlushStartByteCount(-1)
{
    assert(
        chunkSize >= 0 &&
        mChunkFileHandle && mChunkFileHandle->IsOpen() &&
        IsChunkOpen()
    );
    SET_HANDLER(this, &AtomicRecordAppender::EventHandler);
    PendingFlushList::Init(*this);
    AppendReplicationList::Init(mReplicationList);
    mNextOffset = GetChunkSize();
    // Reserve 2MB of 64K blocks, 1MB is default flus threshold.
    mTmpChecksums.reserve(32);
    WAPPEND_LOG_STREAM_DEBUG <<
        "ctor" <<
        " chunk: "    << mChunkId <<
        " offset: "   << mNextOffset <<
        " repl-pos: " << mReplicationPos <<
    KFS_LOG_EOM;
}

AtomicRecordAppender::~AtomicRecordAppender()
{
    if (mState != kStatePendingDelete ||
            mIoOpsInFlight != 0 ||
            mReplicationsInFlight != 0 ||
            ! mWriteIdState.empty() ||
            ! AppendReplicationList::IsEmpty(mReplicationList) ||
            gChunkManager.IsWriteAppenderOwns(mChunkId)) {
        WAPPEND_LOG_STREAM_FATAL <<
            " invalid dtor invocation:"
            " state: "        << GetStateAsStr() <<
            " chunk: "        << mChunkId <<
            " in flight: "    << mIoOpsInFlight <<
            " replications: " << mReplicationsInFlight <<
            " wids: "         << mWriteIdState.size() <<
        KFS_LOG_EOM;
        FatalError();
    }
    WAPPEND_LOG_STREAM_DEBUG <<
        "dtor" <<
        " chunk: "  << mChunkId <<
        " offset: " << mNextOffset <<
    KFS_LOG_EOM;
    if (mPeer) {
        mPeer->Finish();
    }
    mState = kStateNone; // To catch double free;
    const_cast<QCMutex*&>(mMutex) = 0;
}

void
AtomicRecordAppender::SetState(State state, bool notifyIfLostFlag /* = true */)
{
    if (state == mState || mState == kStatePendingDelete) {
        return;
    }
    const State prevState     = mState;
    const bool  wasStableFlag = IsChunkStable();
    mState = state;
    const bool  nowStableFlag = IsChunkStable();
    if ((wasStableFlag && ! nowStableFlag) ||
            (mState == kStateReplicationFailed && prevState != kStateOpen)) {
        // Presently transition from stable to open is not allowed.
        WAPPEND_LOG_STREAM_FATAL <<
            " invalid state transition:"
            " from: "      << GetStateAsStr(prevState) <<
            " to: "        << GetStateAsStr() <<
            " chunk: "     << mChunkId <<
            " offset: "    << mNextOffset <<
            " wid count: " << mWriteIdState.size() <<
            " / "          << mWritableWidCount <<
        KFS_LOG_EOM;
        FatalError();
    }
    if (prevState == kStateOpen) {
        gAtomicRecordAppendManager.DecOpenAppenderCount();
    }
    if (wasStableFlag != nowStableFlag) {
        if (nowStableFlag) {
            const int bytes = mFlushStartByteCount < 0 ?
                mBuffer.BytesConsumable() : mFlushStartByteCount;
            mBuffer.Clear(); // no more ios.
            mFlushStartByteCount  = -1; // Prevent flush attempts.
            mAppendInProgressFlag = false;
            if (0 < bytes) {
                // Update total.
                gAtomicRecordAppendManager.GetFlushLimit(*this, -bytes);
            }
        }
        gAtomicRecordAppendManager.UpdateAppenderFlushLimit(this);
    }
    if (mState == kStateStable || mState == kStateChunkLost) {
        mTimer.SetTimeout(gAtomicRecordAppendManager.GetCleanUpSec());
    }
    mMakeStableSucceededFlag =
        mMakeStableSucceededFlag || mState == kStateStable;
    if (mState == kStateStable) {
        mChunkFileHandle.reset();
    } else if (mState == kStateChunkLost) {
        if (notifyIfLostFlag) {
            // Currently smart pointer copy is not strictly necessary.
            // All ChunkIOFailed does is pointer comparison, thus the pointer
            // does not have to be valid. The copy is more fool proof though.
            DiskIo::FilePtr const chunkFileHandle(mChunkFileHandle);
            assert(nowStableFlag);
            mChunkFileHandle.reset();
            gChunkManager.ChunkIOFailed(mChunkId, 0, chunkFileHandle.get());
        }
        Cntrs().mLostChunkCount++;
    } else if (mState == kStateReplicationFailed) {
        TryToCloseChunk();
    }
}

bool
AtomicRecordAppender::DeleteSelf()
{
    if (mState != kStatePendingDelete) {
        if (int(mState) <= kStateNone || int(mState) >= kNumStates) {
            // Invalid state, most likely double free.
            FatalError();
        }
        mTimer.RemoveTimeout();
        const int bytes = mFlushStartByteCount < 0 ?
            mBuffer.BytesConsumable() : mFlushStartByteCount;
        mBuffer.Clear();
        mFlushStartByteCount  = -1;
        mAppendInProgressFlag = false;
        if (0 < bytes) {
            // Update total.
            gAtomicRecordAppendManager.GetFlushLimit(*this, -bytes);
        }
        SetCanDoLowOnBuffersFlushFlag(false);
        mWriteIdState.clear();
        if (0 < mWritableWidCount) {
            mWritableWidCount = 0;
            DecAppendersWithWidCount();
        }
        gAtomicRecordAppendManager.Detach(*this);
        SetState(kStatePendingDelete);
    }
    if (mIoOpsInFlight > 0 || mReplicationsInFlight > 0) {
        return false; // wait for in flight ops to finish
    }
    delete this;
    return true;
}

template<typename T> int
AtomicRecordAppender::CheckParameters(
    int64_t chunkVersion, uint32_t numServers, const T& servers,
    int replicationPos, ServerLocation peerLoc,
    const DiskIo::FilePtr& fileHandle, string& msg)
{
    QCStMutexLocker lock(mMutex);

    int status = 0;
    if (chunkVersion != mChunkVersion) {
        msg    = "invalid chunk version";
        status = kErrParameters;
    } else if (mReplicationPos != replicationPos) {
        status = kErrParameters;
        msg    = "invalid replication chain position";
    } else if (mPeerLocation != peerLoc) {
        status = kErrParameters;
        msg    = "invalid replication chain peer: " +
            peerLoc.ToString() + " expected: " + mPeerLocation.ToString();
    } else if (mNumServers != numServers) {
        status = kErrParameters;
        msg    = "invalid replication factor";
    } else if (mState != kStateOpen) {
        msg    = GetStateAsStr();
        status = kErrProtocolState;
    } else if (MakeCommitAckServers(numServers, servers) !=
            mCommitAckServers) {
        status = kErrParameters;
        msg    = "invalid replication chain";
    } else if (fileHandle.get() != mChunkFileHandle.get()) {
        status = kErrParameters;
        msg    = "invalid file handle";
    }
    return status;
}

bool
AtomicRecordAppender::Delete()
{
    QCStMutexLocker lock(mMutex);
    return DeleteSelf();
}

void
AtomicRecordAppender::DeleteChunk()
{
    QCStMutexLocker lock(mMutex);
    WAPPEND_LOG_STREAM_DEBUG <<
        "delete: " <<
        " chunk: "      << mChunkId <<
        " state: "      << GetStateAsStr() <<
        " offset: "     << mNextOffset <<
        " wid count: "  << mWriteIdState.size() <<
        " / "           << mWritableWidCount <<
    KFS_LOG_EOM;
    if (mState == kStatePendingDelete) {
        // Only AtomicRecordAppendManager calls this method.
        // Pending delete shouldn't be in AtomicRecordAppendManager::mAppenders.
        FatalError();
    }
    // Prevent recursion:
    // SetState(kStateChunkLost) => StaleChunk() => here
    // make sure that StaleChunk() will not be invoked.
    // Never invoke Delete() here.
    SetState(kStateChunkLost, false);
}

int
AtomicRecordAppender::EventHandler(int code, void* data)
{
    QCStMutexLocker lock(mMutex);
    switch(code) {
        case EVENT_INACTIVITY_TIMEOUT:
            Timeout();
        break;
        case EVENT_DISK_ERROR:
        case EVENT_DISK_WROTE:
        case EVENT_DISK_RENAME_DONE: {
            const int status = data ?
                (code == EVENT_DISK_RENAME_DONE ?
                    (int)*reinterpret_cast<int64_t*>(data) :
                    *reinterpret_cast<int*>(data)
                ) : -1;
            MetaWriteDone(
                (code == EVENT_DISK_ERROR && status > 0) ? -1 : status
            );
        }
        break;
        case EVENT_CMD_DONE: {
            KfsOp* const op = reinterpret_cast<KfsOp*>(data);
            assert(op && op->clnt == this);
            switch (op->op) {
                case CMD_WRITE:
                    OpDone(static_cast<WriteOp*>(op));
                break;
                case CMD_RECORD_APPEND:
                    OpDone(static_cast<RecordAppendOp*>(op));
                break;
                case CMD_READ:
                    OpDone(static_cast<ReadOp*>(op));
                break;
                default:
                    WAPPEND_LOG_STREAM_FATAL << "unexpected op: " << op->Show() <<
                    KFS_LOG_EOM;
                    FatalError();
                break;
            }
        }
        break;
        default:
            WAPPEND_LOG_STREAM_FATAL << "unexpected event code: " << code <<
            KFS_LOG_EOM;
            FatalError();
        break;
    }
    return 0;
}

template<typename T> void
AtomicRecordAppender::CheckLeaseAndChunk(const char* prefix, T* op)
{
    const bool hasChunkAccessFlag =
        op && op->chunkAccessTokenValidFlag && 0 <= op->status;
    bool allowCSClearTextFlag = hasChunkAccessFlag &&
        (op->chunkAccessFlags & ChunkAccessToken::kAllowClearTextFlag) != 0;
    if (! IsChunkStable() &&
            (! mChunkFileHandle || ! mChunkFileHandle->IsOpen())) {
        WAPPEND_LOG_STREAM_ERROR << (prefix ? prefix : "") <<
            ": chunk manager discarded chunk: " << mChunkId << "?" <<
            " state: " << GetStateAsStr() <<
        KFS_LOG_EOM;
        SetState(kStateChunkLost);
    } else if (mState == kStateOpen && IsMaster() &&
            ! gLeaseClerk.IsLeaseValid(mChunkId,
                op ? &op->syncReplicationAccess : 0,
                &allowCSClearTextFlag)) {
        WAPPEND_LOG_STREAM_ERROR << (prefix ? prefix : "") <<
            ": write lease has expired, no further append allowed" <<
            " chunk: " << mChunkId <<
        KFS_LOG_EOM;
        // Handle this exactly the same way as replication failure: trim to last
        // commit, and relinquish the lease.
        // Transitioning into closed won't relinquish the lease. Without
        // explicit lease release it might stay in closed state until no
        // activity timer goes off. Status inquiry is considered an
        // activity: op status recovery cannot succeed because make chunk
        // stable will not be issued until no activity timer goes off.
        Cntrs().mLeaseExpiredCount++;
        SetState(kStateReplicationFailed);
    } else if (op && mPeer && op->syncReplicationAccess.chunkAccess &&
            mPeer->GetShutdownSslFlag() != allowCSClearTextFlag) {
        WAPPEND_LOG_STREAM(mFirstFwdOpFlag ?
                MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelINFO) <<
            (prefix ? prefix : "") << ":"
            " chunk: " << mChunkId <<
            " peer clear text access has changed to: " <<
            (allowCSClearTextFlag ? "allowed" : "not allowed") <<
            " state: "  << GetStateAsStr() <<
            " master: " << IsMaster() <<
            " hasCA: "  << hasChunkAccessFlag <<
            " CA: "     <<
                op->syncReplicationAccess.chunkAccess->token.ToString() <<
            " " << op->Show() <<
        KFS_LOG_EOM;
        mPeer->SetShutdownSslFlag(allowCSClearTextFlag);
    }
}

void
AtomicRecordAppender::AllocateWriteId(
    WriteIdAllocOp *op, int replicationPos, ServerLocation peerLoc,
    const DiskIo::FilePtr& chunkFileHandle)
{
    QCStMutexLocker lock(mMutex);
    mLastActivityTime = Now();
    if (! IsChunkStable() && chunkFileHandle != mChunkFileHandle) {
        WAPPEND_LOG_STREAM_FATAL <<
            "invalid chunk file handle: " <<
            (const void*)chunkFileHandle.get() <<
            " / " << (const void*)mChunkFileHandle.get() <<
        KFS_LOG_EOM;
        FatalError();
    }
    CheckLeaseAndChunk("allocate write id", op);

    int    status = 0;
    string msg;
    if (op->chunkId != mChunkId) {
        msg    = "invalid chunk id";
        status = kErrParameters;
    } else if (op->chunkVersion != mChunkVersion) {
        msg    = "invalid chunk version";
        status = kErrParameters;
    } else if (mReplicationPos != replicationPos) {
        status = kErrParameters;
        msg    = "invalid replication chain position";
    } else if (mPeerLocation != peerLoc) {
        status = kErrParameters;
        msg    = "invalid replication chain peer: " +
            peerLoc.ToString() + " expected: " + mPeerLocation.ToString();
    } else if (mNumServers != op->numServers) {
        status = kErrParameters;
        msg    = "invalid replication factor";
    } else if (! UpdateSession(*op, status, msg)) {
        // Malformed syncronous replication access.
    } else if (mState != kStateOpen) {
        msg    = GetStateAsStr();
        status = kErrProtocolState;
    } else if (int(mWriteIdState.size()) >=
            gAtomicRecordAppendManager.GetMaxWriteIdsPerChunk()) {
        msg    = "too many write ids";
        status = kErrOutOfSpace;
    } else {
        pair<WriteIdState::iterator, bool> res = mWriteIdState.insert(
            make_pair(op->writeId, WIdState()));
        if (! res.second) {
            WAPPEND_LOG_STREAM_FATAL <<
                "allocate write id: duplicate write id: " << op->writeId <<
            KFS_LOG_EOM;
            FatalError();
        } else {
            res.first->second.mSeq = op->clientSeq;
            mWritableWidCount++;
            if (mWritableWidCount == 1) {
                IncAppendersWithWidCount();
            }
            op->appendPeer = mPeer;
        }
    }
    op->status = status;
    if (status != 0) {
        op->statusMsg = msg;
    }
    WAPPEND_LOG_STREAM(status == 0 ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        "allocate write id: " <<
            (status != 0 ? msg : string("ok")) <<
        " state: "     << GetStateAsStr() <<
        " chunk: "     << mChunkId <<
        " wid count: " << mWriteIdState.size() <<
        " / "          << mWritableWidCount <<
        " offset: "    << mNextOffset <<
        " reserved: "  << mBytesReserved <<
        " writeId: "   << op->writeId <<
        " seq: "       << op->seq <<
        " cli seq: "   << op->clientSeq <<
        " status: "    << status <<
    KFS_LOG_EOM;
}

// Ideally space reservation should *not* use write id, but its own token
// "space reservation id" instead. Separate reservation token makes write append
// pipelining more efficient by reducing # of round trips required by the
// protocol: after single reservation multiple write append ops can be started.
// At the time of writing this type of request pipelining is not implemented and
// is not planned to be implemented in the near future by the client.
int
AtomicRecordAppender::ChangeChunkSpaceReservaton(
    int64_t writeId, size_t nBytes, bool releaseFlag, string* errMsg)
{
    QCStMutexLocker lock(mMutex);
    mLastActivityTime = Now();
    RecordAppendOp* const kNullOp = 0;
    CheckLeaseAndChunk(
        releaseFlag ? "space reserve" : "space release", kNullOp);

    int                    status       = 0;
    const char*            msg          = "ok";
    const size_t           prevReserved = mBytesReserved;
    WriteIdState::iterator it;
    if (! IsMaster()) {
        msg    = "not master";
        status = kErrParameters;
    } else if (mState != kStateOpen) {
        msg    = GetStateAsStr();
        status = kErrProtocolState;
    } else if ((it = mWriteIdState.find(writeId)) == mWriteIdState.end()) {
        if (! releaseFlag) {
            msg    = "invalid write id";
            status = kErrParameters;
        }
    } else if (releaseFlag) {
        if (it->second.mReadOnlyFlag) {
            if (it->second.mBytesReserved > 0) {
                WAPPEND_LOG_STREAM_FATAL <<
                    "invalid write id state: " <<
                    it->second.mBytesReserved <<
                    " bytes reserved in read only state" <<
                KFS_LOG_EOM;
                FatalError();
            }
        } else if (it->second.mBytesReserved >= (size_t)nBytes) {
            it->second.mBytesReserved -= nBytes;
            mBytesReserved            -= nBytes;
        } else {
            mBytesReserved -= it->second.mBytesReserved;
            it->second.mBytesReserved = 0;
        }
    } else if (it->second.mReadOnlyFlag) {
        msg    = "no appends allowed with this write id";
        status = kErrParameters;
    } else {
        if (mNextOffset + mBytesReserved + nBytes > int64_t(CHUNKSIZE)) {
            msg    = "out of space";
            status = kErrOutOfSpace;
            mConsecutiveOutOfSpaceCount++;
        } else {
            mBytesReserved            += nBytes;
            it->second.mBytesReserved += nBytes;
            mConsecutiveOutOfSpaceCount = 0;
        }
    }
    if (errMsg) {
        (*errMsg) += msg;
    }
    if (status == 0 && mBytesReserved <= 0) {
        mBytesReserved = 0;
        mTimer.ScheduleTimeoutNoLaterThanIn(Timer::MinTimeout(
            gAtomicRecordAppendManager.GetCleanUpSec(),
            GetCloseEmptyWidStateSec()
        ));
    }
    WAPPEND_LOG_STREAM(status == 0 ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelINFO) <<
        (releaseFlag ? "release: " : "reserve: ") << msg <<
        " state: "     << GetStateAsStr() <<
        " chunk: "    << mChunkId <<
        " writeId: "  << writeId <<
        " bytes: "    << nBytes <<
        " offset: "   << mNextOffset <<
        " reserved: " << mBytesReserved <<
        " delta: "    << ssize_t(prevReserved - mBytesReserved) <<
        " status: "   << status <<
    KFS_LOG_EOM;
    return status;
}

int
AtomicRecordAppender::InvalidateWriteId(int64_t writeId, bool declareFailureFlag)
{
    QCStMutexLocker lock(mMutex);
    int status = 0;
    WriteIdState::iterator const it = mWriteIdState.find(writeId);
    if (it != mWriteIdState.end() &&
            it->second.mStatus == 0 &&
            it->second.mAppendCount == 0 &&
            ! it->second.mReadOnlyFlag) {
        // Entry with no appends, clean it up.
        // This is not orderly close, do not shorten close timeout.
        mWriteIdState.erase(it);
        DecrementWritableWidCount();
        if (declareFailureFlag && mState == kStateOpen) {
            SetState(kStateReplicationFailed);
        }
    } else {
        status = kErrParameters;
    }
    WAPPEND_LOG_STREAM(status == 0 ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        "invalidate write id" <<
        (declareFailureFlag ? " declare failure:" : ":") <<
        " wid: "        << writeId <<
        " chunk: "      << mChunkId <<
        " status: "     << status <<
        " state:  "     << GetStateAsStr() <<
        " wids count: " << mWriteIdState.size() <<
    KFS_LOG_EOM;
    return status;
}

void
AtomicRecordAppender::UpdateMasterCommittedOffset(int64_t masterCommittedOffset)
{
    // Master piggies back its ack on the write append replication.
    // The ack can lag because of replication request pipelining. The ack is
    // used to determine op status if / when client has to use slave to perform
    // "get op status" in the case when the client can not communicate with the
    // master.
    // This is needed only to reduce client's failure resolution latency. By
    // comparing append end with this ack value it might be possible to
    // determine the append op status. If the ack value is greater than the
    // append end, then the append was successfully committed by all replication
    // participants.
    if (masterCommittedOffset >= mMasterCommittedOffset &&
            masterCommittedOffset <= mNextCommitOffset) {
        mMasterCommittedOffset = masterCommittedOffset;
    } else {
        WAPPEND_LOG_STREAM_ERROR <<
            "out of window master committed"
            " offset: " << masterCommittedOffset <<
            "[" << mMasterCommittedOffset <<
            "," << mNextCommitOffset << "]" <<
        KFS_LOG_EOM;
    }
}

void
AtomicRecordAppender::Relock(ClientThread& cliThread)
{
    if (! mMutex || ! mMutex->IsOwned()) {
        FatalError("AtomicRecordAppender::Relock: invalid mutex state");
        return;
    }
    mMutex->Unlock();
    if (mMutex->IsOwned()) {
        FatalError("AtomicRecordAppender::Relock: failed to release mutex");
        return;
    }
    // Re-acquire in the same order.
    cliThread.Lock();
    mMutex->Lock();
    if (mState == kStateNone) {
        FatalError("AtomicRecordAppender::Relock:"
            " possible unexpected deletion");
        return;
    }
    if (mFlushStartByteCount < 0) {
        mAppendInProgressFlag = false;
    }
}

void
AtomicRecordAppender::AppendBegin(
    RecordAppendOp* op, int replicationPos, ServerLocation peerLoc)
{
    if (op->numBytes < size_t(op->dataBuf.BytesConsumable()) ||
            op->origClnt) {
        WAPPEND_LOG_STREAM_FATAL <<
            "begin: short op buffer: " <<
            " req. size: "   << op->numBytes <<
            " buffer: "      << op->dataBuf.BytesConsumable() <<
            " or non null"
            " orig client: " << op->origClnt <<
            " " << op->Show() <<
        KFS_LOG_EOM;
        FatalError();
    }
    mLastActivityTime = Now();
    CheckLeaseAndChunk("begin", op);

    ClientThread* cliThread = 0;
    int           status    = 0;
    string msg;
    ClientSM*   client = 0;
    if (op->chunkId != mChunkId) {
        status = kErrParameters;
        msg    = "invalid chunk id";
    } else if (mState != kStateOpen) {
        msg    = GetStateAsStr();
        status = kErrProtocolState;
    } else if (mPendingBadChecksumFlag) {
        msg    = "pending replication failed";
        status = kErrProtocolState;
    } else if (mPeerLocation != peerLoc) {
        status = kErrParameters;
        msg    = "invalid replication chain peer: " +
            peerLoc.ToString() + " expected: " + mPeerLocation.ToString();
    } else if (mReplicationPos != replicationPos) {
        status = kErrParameters;
        msg    = "invalid replication chain position";
    } else if (IsMaster() && op->fileOffset >= 0) {
        status = kErrParameters;
        msg    = "protocol error: offset specified for master";
    } else if (! IsMaster() && op->fileOffset < 0) {
        status = kErrParameters;
        msg    = "protocol error: offset not specified for slave";
    } else if (mNumServers != op->numServers) {
        status = kErrParameters;
        msg    = "invalid replication factor";
    } else if (mNextOffset + op->numBytes > int64_t(CHUNKSIZE)) {
        msg    = "out of chunk space";
        status = kErrParameters;
    } else if (! UpdateSession(*op, status, msg)) {
        // Malformed syncronous replication access.
    } else if (IsMaster() && op->clnt != this &&
            (client = op->GetClientSM()) &&
            client->GetReservedSpace(mChunkId, op->writeId) < op->numBytes) {
        status = kErrParameters;
        msg    = "out of client reserved space";
    }
    if ((status != 0 || ! IsMaster()) && op->clnt == this) {
        WAPPEND_LOG_STREAM_FATAL <<
            "begin: bad internal op: " << op->Show() <<
        KFS_LOG_EOM;
        FatalError();
    }

    // Check if it is master 0 ack: no payload just commit offset.
    const bool masterAckflag = status == 0 && IsMasterAck(*op);
    WriteIdState::iterator const widIt = (masterAckflag || status != 0) ?
        mWriteIdState.end() : mWriteIdState.find(op->writeId);
    if (masterAckflag) {
        if (IsMaster()) {
            op->fileOffset = mNextOffset;
        } else if (op->fileOffset != mNextOffset) {
            // Out of order.
            msg    = "master 0 ack has invalid offset";
            status = kErrParameters;
            SetState(kStateReplicationFailed);
        } else {
            UpdateMasterCommittedOffset(op->masterCommittedOffset);
        }
    } else if (status == 0) {
        if (widIt == mWriteIdState.end()) {
            status = kErrParameters;
            msg    = "invalid write id";
        } else {
            WIdState& ws = widIt->second;
            if (! IsMaster()) {
                if (op->fileOffset != mNextOffset) {
                    // Out of order replication.
                    msg    = "invalid append offset";
                    status = kErrParameters;
                    SetState(kStateReplicationFailed);
                } else {
                    UpdateMasterCommittedOffset(op->masterCommittedOffset);
                    if (ws.mStatus == kErrStatusInProgress &&
                            mMasterCommittedOffset >=
                                ws.mOffset + int64_t(ws.mLength)) {
                        ws.mStatus = 0; // Master committed.
                    }
                }
            }
            if (status != 0) {
                // Failed already.
            } if (ws.mReadOnlyFlag) {
                status = kErrWidReadOnly;
                msg    = "no appends allowed with this write id";
            } else if (ws.mStatus != 0) {
                // Force client to use multiple write ids to do request
                // pipelining, and allocate new write id after a failure.
                status = kErrParameters;
                msg    = ws.mStatus == kErrStatusInProgress ?
                    "has operation in flight" :
                    "invalid write id: previous append failed";
            }
        }
        if (status == 0 && IsMaster()) {
            // Only on the write master is space reserved
            assert(mNextOffset + mBytesReserved <= int64_t(CHUNKSIZE));
            // Decrease space reservation for this client connection.
            // ClientSM space un-reservation in case of the subsequent
            // failures, except checksum mismatch, is not needed because
            // these failures will at least prevent any further writes with
            // this write id.
            // The client must handle checksum mismatch by re-reserving
            // space prior to retry, to resolve potential race due to
            // re-locking.
            if (widIt->second.mBytesReserved < op->numBytes) {
                status = kErrParameters;
                msg    = "write id out of reserved space";
            } else if (mBytesReserved < op->numBytes) {
                msg    = "out of reserved space";
                status = kErrParameters;
            } else {
                assert(
                    widIt != mWriteIdState.end() &&
                    widIt->second.mStatus == 0
                );
                mBytesReserved -= op->numBytes;
                widIt->second.mBytesReserved -= op->numBytes;
                if (client) {
                    client->UseReservedSpace(
                        mChunkId, op->writeId, op->numBytes);
                }
            }
        }

        if (status == 0) {
            if (mMutex &&
                    gAtomicRecordAppendManager.GetAppendDropLockMinSize() <
                    (int)op->numBytes) {
                cliThread = gClientManager.GetCurrentClientThreadPtr();
                if (cliThread) {
                    if (mFlushStartByteCount < 0) {
                        mAppendInProgressFlag = true;
                    }
                    if (0 < op->numBytes) {
                        // Update next block alignment here. In the case of
                        // checksum mismatch RunPendingSubmitQueue() will update
                        // it again (undo this update).
                        // Alignment is optimization only, and, in general,
                        // should reduce cpu utilization on slaves, or in case
                        // of low number of concurrent appends on the master.
                        mAlignment = (int)((mNextOffset + op->numBytes) %
                            IOBufferData::GetDefaultBufferSize());
                    }
                    cliThread->Unlock();
                }
            }
            uint32_t     checksum       = kKfsNullChecksum;
            const size_t lastLen        = mNextOffset % CHECKSUM_BLOCKSIZE;
            const size_t rem            = min(op->numBytes,
                CHECKSUM_BLOCKSIZE - lastLen);
            uint32_t     lastChksum     = 0;
            const size_t prevSize       = mTmpChecksums.size();
            const bool   headChksumFlag = 0 < lastLen && 0 < prevSize;
            if (headChksumFlag) {
                lastChksum = mTmpChecksums.back();
                mTmpChecksums.pop_back();
            }
            AppendToChecksumVector(op->dataBuf, op->numBytes, &checksum, rem,
                mTmpChecksums);
            if (prevSize - (headChksumFlag ? 1 : 0) +
                    (lastLen + op->numBytes + CHECKSUM_BLOCKSIZE - 1) /
                        CHECKSUM_BLOCKSIZE != mTmpChecksums.size()) {
                WAPPEND_LOG_STREAM_FATAL <<
                    "begin: invalid empty checksum vector" <<
                    " chunk: "        << mChunkId <<
                    " chunkVersion: " << mChunkVersion <<
                    " reserved: "     << mBytesReserved <<
                    " offset: "       << mNextCommitOffset <<
                    " nextOffset: "   << mNextOffset <<
                    " " << op->Show() <<
                KFS_LOG_EOM;
                FatalError();
                // The following should never be reached, unless FatalError()
                // will be changed in the future no to abort() the process.
                status = kErrBadChecksum;
                if (! IsMaster()) {
                    mPendingBadChecksumFlag = true;
                }
            } else {
                if (op->checksum == checksum) {
                    if (headChksumFlag) {
                        mTmpChecksums[prevSize - 1] = 0 < rem ?
                            ChecksumBlocksCombine(
                                lastChksum, mTmpChecksums[prevSize - 1], rem) :
                            lastChksum;
                    }
                } else {
                    mTmpChecksums.resize(prevSize);
                    if (headChksumFlag) {
                        mTmpChecksums[prevSize - 1] = lastChksum;
                    }
                    msg = "checksum mismatch: received: ";
                    AppendDecIntToString(msg, op->checksum);
                    msg += " actual: ";
                    AppendDecIntToString(msg, checksum);
                    status = kErrBadChecksum;
                    if (! IsMaster()) {
                        mPendingBadChecksumFlag = true;
                    }
                }
            }
        }
        if (status == 0) {
            assert(cliThread || IsChunkOpen());
            if (IsMaster()) {
                // Commit the execution.
                op->fileOffset = mNextOffset;
            }
            mNextOffset += op->numBytes;
        }
    }
    // Empty appends (0 bytes) are always forwarded.
    // This is used by propagate master commit ack, and potentially can be used
    // for replication health check.
    if (status == 0 && mPeer) {
        op->origSeq  = op->seq;
        op->origClnt = op->clnt;
        op->clnt     = this;
    }
    if (status == 0 && ! masterAckflag) {
        // Write id table is updated only in the case when execution is
        // committed. Otherwise the op is discarded, and treated like
        // it was never received.
        assert(widIt != mWriteIdState.end() && widIt->second.mStatus == 0);
        WIdState& ws = widIt->second;
        ws.mStatus = kErrStatusInProgress;
        ws.mLength = op->numBytes;
        ws.mOffset = op->fileOffset;
        ws.mSeq    = op->clientSeq;

        // Move blocks into the internal buffer.
        // The main reason to do this now, and not to wait for the replication
        // completion is to save io buffer space. Io buffers can be reclaimed
        // immediately after the data goes onto disk, and on the wire. Another
        // way to look at this: socket buffer space becomes an extension to
        // write appender io buffer space.
        // The price is "undoing" writes, which might be necessary in the case of
        // replication failure. Undoing writes is a simple truncate, besides the
        // failures aren't expected to be frequent enough to matter.
        if (mFlushStartByteCount < 0) {
            mFlushStartByteCount = mBuffer.BytesConsumable();
        }
        // Always try to append to the last buffer.
        // Flush() keeps track of the write offset and "slides" buffers
        // accordingly.
        if (op->numBytes > 0) {
            assert(mBufFrontPadding == 0 || mBuffer.IsEmpty());
            IOBuffer dataBuf;
            if (op->origClnt) {
                // Copy buffer before moving data into appender's write buffer.
                // Enqueue for replication at the end.
                // Replication completion invokes AppendCommit().
                dataBuf.Copy(&op->dataBuf, op->numBytes);
            }
            mBuffer.ReplaceKeepBuffersFull(
                op->origClnt ? &dataBuf : &op->dataBuf,
                mBuffer.BytesConsumable() + mBufFrontPadding,
                op->numBytes
            );
            if (mBufFrontPadding > 0) {
                mBuffer.Consume(mBufFrontPadding);
                mBufFrontPadding = 0;
                assert(! mBuffer.IsEmpty());
            }
        }
    }
    // Update in flight queue to ensure that the appender cannot be deleted, by
    // re-locking when the appender's mutex released and re-acquired.
    op->status = status;
    if (status != 0) {
        op->statusMsg = msg;
    }
    mReplicationsInFlight++;
    assert(0 < mReplicationsInFlight);
    op->replicationStartTime = mLastActivityTime;
    AppendReplicationList::PushBack(mReplicationList, *op);
    if (! mPendingSubmitQueue) {
        mPendingSubmitQueue = op;
    }
    if (cliThread) {
        Relock(*cliThread);
    }
    RunPendingSubmitQueue();
}

void
AtomicRecordAppender::RunPendingSubmitQueue()
{
    // The first thread that gets here after re-locking does the flush, and
    // submits all pending ops. All other threads still need to do relock, to
    // to re-acquire the client manager's mutex before returning.
    if (! mPendingSubmitQueue) {
        return;
    }
    RecordAppendOp*       next                = mPendingSubmitQueue;
    RecordAppendOp* const last                =
        AppendReplicationList::Back(mReplicationList);
    bool                  scheduleTimeoutFlag =
        next == AppendReplicationList::Front(mReplicationList);
    assert(next && last);
    if (0 <= mFlushStartByteCount && IsOpen()) {
        const int newBytes = mBuffer.BytesConsumable() - mFlushStartByteCount;
        mFlushStartByteCount  = -1;
        mAppendInProgressFlag = false;
        // Do space accounting and flush if needed.
        if (0 < newBytes && gAtomicRecordAppendManager.GetFlushLimit(
                    *this, newBytes) <= mBuffer.BytesConsumable()) {
            // Align the flush to checksum boundaries.
            FlushFullBlocks();
        } else {
            const int pendingBytes = mBuffer.BytesConsumable();
            if (0 < pendingBytes) {
                mTimer.ScheduleTimeoutNoLaterThanIn(
                    gAtomicRecordAppendManager.GetFlushIntervalSec());
            }
            SetCanDoLowOnBuffersFlushFlag(
                (int)CHECKSUM_BLOCKSIZE <= pendingBytes);
            UpdateAlignment();
        }
    }
    mPendingSubmitQueue = 0;
    for (; ;) {
        if (mReplicationsInFlight <= 0 || mState == kStateNone) {
            FatalError("AtomicRecordAppender::RunPendingSubmitQueue:"
                " possible unexpected deletion");
            return;
        }
        RecordAppendOp& cur = *next;
        next = &AppendReplicationList::GetNext(cur);
        const bool statusOkFlag = 0 == cur.status;
        const bool enqueueFlag  = cur.origClnt && kStateOpen == mState;
        // Ensure that state is still open after re-locking.
        if (enqueueFlag) {
            assert(statusOkFlag);
            if (IsMaster()) {
                cur.masterCommittedOffset = mNextCommitOffset;
                mCommitOffsetAckSent = mNextCommitOffset;
            }
            if (scheduleTimeoutFlag) {
                scheduleTimeoutFlag = false;
                mTimer.ScheduleTimeoutNoLaterThanIn(
                    gAtomicRecordAppendManager.GetReplicationTimeoutSec());
            }
        } else {
            if (statusOkFlag) {
                if (kStateOpen != mState) {
                    cur.status    = kErrStatusInProgress;
                    cur.statusMsg = "no longer open for append";
                }
            } else {
                if (cur.status == kErrBadChecksum) {
                    UpdateAlignment();
                    if (! IsMaster()) {
                        mPendingBadChecksumFlag = false;
                        SetState(kStateReplicationFailed);
                    }
                    Cntrs().mChecksumErrorCount++;
                }
            }
        }
        WAPPEND_LOG_STREAM(cur.status == 0 ?
                MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
            "begin: "           << cur.statusMsg <<
                (statusOkFlag && IsMasterAck(cur) ? " master ack" : "") <<
            " state: "        << GetStateAsStr() <<
            " reserved: "     << mBytesReserved <<
            " offset: next: " << mNextOffset <<
            " commit: "       << mNextCommitOffset <<
            " master: "       << mMasterCommittedOffset <<
            " in flight:"
            " replicaton: "   << mReplicationsInFlight <<
            " ios: "          << mIoOpsInFlight <<
            " last: "         << (&cur == last) <<
            " status: "       << cur.status <<
            " " << cur.Show() <<
        KFS_LOG_EOM;
        if (enqueueFlag) {
            mFirstFwdOpFlag = false;
            mPeer->Enqueue(&cur);
        } else {
            OpDone(&cur);
        }
        if (&cur == last) {
            break;
        }
    }
}

int
AtomicRecordAppender::GetNextReplicationTimeout() const
{
    if (mReplicationsInFlight <= 0 || mState != kStateOpen) {
        return -1;
    }
    const int timeout = gAtomicRecordAppendManager.GetReplicationTimeoutSec();
    if (timeout < 0) {
        return -1;
    }
    const time_t now               = Now();
    const RecordAppendOp* const op =
            AppendReplicationList::Front(mReplicationList);
    assert(op);
    const time_t end               = op->replicationStartTime + timeout;
    return (now < end ? end - now : 0);
}

void
AtomicRecordAppender::OpDone(RecordAppendOp* op)
{
    assert(
        mReplicationsInFlight > 0 &&
        AppendReplicationList::IsInList(mReplicationList, *op)
    );
    mReplicationsInFlight--;
    AppendReplicationList::Remove(mReplicationList, *op);
    if (mReplicationsInFlight > 0 && mState == kStateOpen) {
        mTimer.ScheduleTimeoutNoLaterThanIn(GetNextReplicationTimeout());
    }
    // Do not commit malformed client requests.
    const bool commitFlag = ! IsMaster() || op->origClnt || op->status == 0;
    if (op->origClnt) {
        op->seq   = op->origSeq;
        op->clnt  = op->origClnt;
        op->origClnt = 0;
    }
    if (commitFlag) {
        AppendCommit(op);
    }
    // Delete if commit ack.
    const bool deleteOpFlag = op->clnt == this;
    if (deleteOpFlag) {
        delete op;
    }
    const bool deletedFlag = DeleteIfNeeded();
    if (! deleteOpFlag) {
        Cntrs().mAppendCount++;
        if (op->status >= 0) {
            Cntrs().mAppendByteCount += op->numBytes;
        } else {
            Cntrs().mAppendErrorCount++;
            if (! deletedFlag && mState == kStateReplicationFailed) {
                Cntrs().mReplicationErrorCount++;
            }
        }
        SubmitOpResponse(op);
    }
}

void
AtomicRecordAppender::AppendCommit(RecordAppendOp *op)
{
    mLastActivityTime = Now();
    if (mState != kStateOpen) {
        op->status    = kErrStatusInProgress; // Don't know
        op->statusMsg = "closed for append; op status undefined; state: ";
        op->statusMsg += GetStateAsStr();
        WAPPEND_LOG_STREAM_ERROR <<
            "commit: " << op->statusMsg <<
            " status: " << op->status <<
            " " << op->Show() <<
        KFS_LOG_EOM;
        return;
    }
    // Always declare failure here if op status is non zero.
    // Theoretically it is possible to recover from errors such "write id is
    // read only", *by moving buffer append here* in AppendCommit, from
    // AppendBegin. In such case the protocol ensures that no partial
    // replication can succeed if an error status is *received*.
    // The problem is that there might be more than one replications in flight,
    // and waiting for all replications that are currently in flight to fail is
    // required to successfully recover.
    // On the other hand the price for declaring a failure is only partial
    // (non full) chunk.
    // For now assume that the replication failures are infrequent enough to
    // have any significant effect on the chunk size, and more efficient use of
    // io buffer space is more important (see comment in AppendBegin).
    if (op->status != 0) {
        op->statusMsg += " op (forwarding) failed, op status undefined"
            "; state: ";
        op->statusMsg += GetStateAsStr();
        WAPPEND_LOG_STREAM_ERROR <<
            "commit: " << op->statusMsg <<
            " status: "       << op->status <<
            " reserved: "     << mBytesReserved <<
            " offset: "       << mNextCommitOffset <<
            " nextOffset: "   << mNextOffset <<
            " " << op->Show() <<
        KFS_LOG_EOM;
        op->status = kErrStatusInProgress; // Don't know
        SetState(kStateReplicationFailed);
        return;
    }
    // AppendBegin checks if write id is read only.
    // If write id wasn't read only in the append begin, it cannot transition
    // into into read only between AppendBegin and AppendCommit, as it should
    // transition into "in progress" in the AppendBegin, and stay "in progress"
    // at least until here.
    // If the op is internally generated 0 ack verify that it has no payload.
    WriteIdState::iterator const widIt = op->clnt == this ?
        mWriteIdState.end() : mWriteIdState.find(op->writeId);
    if (op->fileOffset != mNextCommitOffset ||
            op->chunkId != mChunkId ||
            op->chunkVersion != mChunkVersion ||
            (widIt == mWriteIdState.end() ?
                ((IsMaster() ? (op->clnt != this) :
                    (op->masterCommittedOffset < 0)) ||
                op->numBytes != 0 || op->writeId != -1) :
                widIt->second.mStatus != kErrStatusInProgress)) {
        WAPPEND_LOG_STREAM_FATAL <<
            "commit: out of order or invalid op" <<
            " chunk: "        << mChunkId <<
            " chunkVersion: " << mChunkVersion <<
            " reserved: "     << mBytesReserved <<
            " offset: "       << mNextCommitOffset <<
            " nextOffset: "   << mNextOffset <<
            " " << op->Show() <<
        KFS_LOG_EOM;
        FatalError();
        return;
    }
    // Do not pay attention to the lease expiration here.
    // If lease has expired, but no make stable was received, then
    // commit the append anyway.
    op->status = 0;
    if (widIt != mWriteIdState.end()) {
        mNextCommitOffset += op->numBytes;
        mAppendCommitCount++;
        widIt->second.mAppendCount++;
    }
    if (IsMaster()) {
        // Only write master can declare a success, he is the last to commit,
        // and only in the case if all slaves committed.
        if (widIt != mWriteIdState.end()) {
            widIt->second.mStatus = 0;
        }
        // Schedule to send commit ack.
        if (mNumServers > 1 && mReplicationsInFlight <= 0 &&
                mNextCommitOffset > mCommitOffsetAckSent) {
            mTimer.ScheduleTimeoutNoLaterThanIn(
                gAtomicRecordAppendManager.GetSendCommitAckTimeoutSec());
        }
    }
    WAPPEND_LOG_STREAM_DEBUG <<
        "commit:"
        " state: "        << GetStateAsStr() <<
        " reserved: "     << mBytesReserved <<
        " offset: next: " << mNextOffset <<
        " commit: "       << mNextCommitOffset <<
        " master: "       << mMasterCommittedOffset <<
        " in flight:"
        " replicaton: "   << mReplicationsInFlight <<
        " ios: "          << mIoOpsInFlight <<
        " status: "       << op->status <<
        " " << op->Show() <<
    KFS_LOG_EOM;
}

void
AtomicRecordAppender::GetOpStatus(GetRecordAppendOpStatus* op)
{
    QCStMutexLocker lock(mMutex);
    mLastActivityTime = Now();

    int         status = 0;
    const char* msg    = "ok";
    if (op->chunkId != mChunkId) {
        msg    = "invalid chunk id";
        status = kErrParameters;
    } else {
        WriteIdState::iterator const widIt = mWriteIdState.find(op->writeId);
        if (widIt == mWriteIdState.end()) {
            msg    = "no such write id";
            status = kErrNotFound;
        } else {
            WIdState& ws = widIt->second;
            assert(
                ws.mBytesReserved == 0 ||
                (IsMaster() && ! ws.mReadOnlyFlag &&
                mBytesReserved >= ws.mBytesReserved)
            );
            if (ws.mStatus == kErrStatusInProgress) {
                const int64_t end = ws.mOffset + ws.mLength;
                if (mMakeStableSucceededFlag) {
                    ws.mStatus = mChunkSize >= end ? 0 : kErrFailedState;
                } else if (! IsMaster() &&  mMasterCommittedOffset >= end) {
                    ws.mStatus = 0;
                }
                if (ws.mStatus != kErrStatusInProgress) {
                    WAPPEND_LOG_STREAM_DEBUG <<
                        "get op status:"
                        " changed status from in progress"
                        " to: "               << ws.mStatus <<
                        " chunk: "            << mChunkId <<
                        " writeId: "          << op->writeId <<
                        " op end: "           << (ws.mOffset + ws.mLength) <<
                        " master committed: " << mMasterCommittedOffset <<
                        " state: "            << GetStateAsStr() <<
                        " chunk size: "       << mChunkSize <<
                    KFS_LOG_EOM;
                }
            }
            op->opStatus           = ws.mStatus;
            op->opLength           = ws.mLength;
            op->opOffset           = ws.mOffset;
            op->opSeq              = ws.mSeq;
            op->widAppendCount     = ws.mAppendCount;
            op->widBytesReserved   = ws.mBytesReserved;
            op->widWasReadOnlyFlag = ws.mReadOnlyFlag;

            op->chunkVersion       = mChunkVersion;
            op->chunkBytesReserved = mBytesReserved;
            op->remainingLeaseTime = IsMaster() ?
                gLeaseClerk.GetLeaseExpireTime(mChunkId) - Now() : -1;
            op->masterFlag         = IsMaster();
            op->stableFlag         = mState == kStateStable;
            op->appenderState      = mState;
            op->appenderStateStr   = GetStateAsStr();
            op->openForAppendFlag  = mState == kStateOpen;
            op->masterCommitOffset = mMasterCommittedOffset;
            op->nextCommitOffset   = mNextCommitOffset;

            // The status inquiry always makes write id read only, and
            // un-reserves the space, thus disallowing further appends with this
            // write id. This guarantees that after status inquiry returns, no
            // other [new, or stale] appends with this write id can succeed.
            //
            // For now this the behaviour is the same for all replication
            // participants.
            // This speeds up recovery for the clients that can not communicate
            // with the replication master.
            // The price for this is lower resistance to "DoS", where status
            // inquiry with any replication slave when append replication is
            // still in flight but haven't reached the slave can make chunks
            // less full.
            if (mBytesReserved >= ws.mBytesReserved) {
                mBytesReserved -= ws.mBytesReserved;
            } else {
                mBytesReserved = 0;
            }
            ws.mBytesReserved = 0;
            const bool readOnlyFlag = ws.mReadOnlyFlag;
            ws.mReadOnlyFlag  = true;
            op->widReadOnlyFlag = ws.mReadOnlyFlag;
            if (! readOnlyFlag) {
                DecrementWritableWidCount();
            }
        }
    }
    op->status = status;
    if (status != 0) {
        op->statusMsg = msg;
    }
    WAPPEND_LOG_STREAM(status == 0 ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelINFO) <<
        "get op status: " << msg <<
        " state: "           << GetStateAsStr() <<
        " chunk: "           << mChunkId <<
        " wid count: "       << mWriteIdState.size() <<
        " offset: "          << mNextOffset <<
        " reserved: "        << mBytesReserved <<
        " writeId: "         << op->writeId <<
        " status: "          << status <<
        " master comitted: " << mMasterCommittedOffset <<
        " " << op->Show() <<
    KFS_LOG_EOM;
}

void
AtomicRecordAppender::CloseChunk(CloseOp* op, int64_t writeId, bool& forwardFlag)
{
    QCStMutexLocker lock(mMutex);
    mLastActivityTime = Now();

    int         status = 0;
    const char* msg    = "ok";
    if (op->chunkId != mChunkId) {
        msg    = "invalid chunk id";
        status = kErrParameters;
    } else if (op->hasWriteId) {
        WriteIdState::iterator const widIt = mWriteIdState.find(writeId);
        if (widIt == mWriteIdState.end()) {
            msg    = "no such write id";
            status = kErrNotFound;
        } else {
            if (! IsMaster()) {
                // Update master commit offset, and last op status if needed,
                // and possible.
                if (op->masterCommitted >= 0) {
                    UpdateMasterCommittedOffset(op->masterCommitted);
                }
                WIdState& ws = widIt->second;
                if (ws.mStatus == kErrStatusInProgress) {
                    const int64_t end = ws.mOffset + ws.mLength;
                    if ((mMakeStableSucceededFlag ?
                            mChunkSize : mMasterCommittedOffset) >= end) {
                        ws.mStatus = 0;
                    }
                }
            }
            if (widIt->second.mStatus == kErrStatusInProgress) {
                msg    = "write id has op in flight";
                status = kErrStatusInProgress;
            } else if (widIt->second.mReadOnlyFlag) {
                msg    = "write id is read only";
                status = kErrParameters;
            } else if (widIt->second.mStatus != 0) {
                msg    = "append failed with this write id";
                status = kErrParameters;
            } else {
                // The entry is in good state, and the client indicates that he
                // does not intend to use this write id for any purpose in the
                // future. Reclaim the reserved space, and discard the entry.
                if (mBytesReserved >= widIt->second.mBytesReserved) {
                    mBytesReserved -= widIt->second.mBytesReserved;
                } else {
                    mBytesReserved = 0;
                }
                const bool readOnlyFlag = widIt->second.mReadOnlyFlag;
                mWriteIdState.erase(widIt);
                if (! readOnlyFlag) {
                    DecrementWritableWidCount();
                }
                if (IsMaster() && mState == kStateOpen &&
                        MasterUseShortCloseTimeout()) {
                    // For orderly close case, and when chunk over min size
                    // shorten close timeout.
                    mTimer.ScheduleTimeoutNoLaterThanIn(Timer::MinTimeout(
                        gAtomicRecordAppendManager.GetCleanUpSec(),
                        GetCloseEmptyWidStateSec()
                    ));
                }
            }
        }
    }
    forwardFlag = forwardFlag && status == 0;
    op->status  = status;
    if (status != 0) {
        op->statusMsg = msg;
    }
    if (IsMaster()) {
        // Always send commit offset, it might be need to transition write id
        // into "good" state.
        // Write ids are deleted only if these are in good state, mostly for
        // extra "safety", and to simplify debugging.
        // For now do not update the acked: close is "no reply", it can be
        // simply dropped.
        op->masterCommitted = (forwardFlag && op->hasWriteId) ?
            mNextCommitOffset : -1;
    }
    if (forwardFlag || ! mPeer) {
        mLastAppendActivityTime = Now();
    }
    if (forwardFlag && mPeer) {
        forwardFlag = false;
        CloseOp* const fwdOp = new CloseOp(0, *op);
        fwdOp->needAck = false;
        const bool allowCSClearTextFlag = op->chunkAccessTokenValidFlag &&
            (op->chunkAccessFlags & ChunkAccessToken::kAllowClearTextFlag) != 0;
        SET_HANDLER(fwdOp, &CloseOp::HandlePeerReply);
        if (fwdOp->syncReplicationAccess.chunkAccess &&
                    ! allowCSClearTextFlag && mPeer->GetShutdownSslFlag()) {
            WAPPEND_LOG_STREAM(mFirstFwdOpFlag ?
                    MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelINFO) <<
                "chunk: " << mChunkId <<
                " peer clear text access has changed to: not allowed" <<
                " state: "  << GetStateAsStr() <<
                " master: " << IsMaster() <<
                " hasCA: "  << op->chunkAccessTokenValidFlag <<
                " CA: "     <<
                  fwdOp->syncReplicationAccess.chunkAccess->token.ToString() <<
                " " << op->Show() <<
            KFS_LOG_EOM;
            mPeer->SetShutdownSslFlag(allowCSClearTextFlag);
        }
        mFirstFwdOpFlag = false;
        mPeer->Enqueue(fwdOp);
    }
    WAPPEND_LOG_STREAM(status == 0 ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        "close chunk status: " << msg <<
        " state: "     << GetStateAsStr() <<
        " chunk: "     << mChunkId <<
        " wid count: " << mWriteIdState.size() <<
        " offset: "    << mNextOffset <<
        " reserved: "  << mBytesReserved <<
        " writeId: "   << writeId <<
        " status: "    << status <<
        " " << op->Show() <<
    KFS_LOG_EOM;
}

bool
AtomicRecordAppender::ComputeChecksum()
{
    const bool ok = ComputeChecksum(
        mChunkId, mChunkVersion, mChunkSize, mChunkChecksum);
    if (! ok) {
        mChunkChecksum = 0;
        mChunkSize     = -1;
        SetState(kStateChunkLost);
    }
    return ok;
}

bool
AtomicRecordAppender::ComputeChecksum(
    kfsChunkId_t chunkId, int64_t chunkVersion,
    int64_t& chunkSize, uint32_t& chunkChecksum)
{
    const ChunkInfo_t* const info = gChunkManager.GetChunkInfo(chunkId);
    if (! info ||
            (! info->chunkBlockChecksum && info->chunkSize != 0) ||
            chunkVersion != info->chunkVersion) {
        return false;
    }
    chunkSize = info->chunkSize;
    const uint32_t* checksums = info->chunkBlockChecksum;
    // Print it as text, to make byte order independent.
    chunkChecksum = kKfsNullChecksum;
    for (int64_t i = 0; i < chunkSize; i += (int64_t)CHECKSUM_BLOCKSIZE) {
        ConvertInt<uint32_t, 10> const decInt(*checksums++);
        chunkChecksum = ComputeBlockChecksum(
            chunkChecksum, decInt.GetPtr(), decInt.GetSize());
    }
    return true;
}

void
AtomicRecordAppender::SubmitResponse(BeginMakeChunkStableOp& op)
{
    mLastActivityTime = Now();
    if (op.status == 0) {
        op.status = mState == kStateClosed ? 0 : kErrFailedState;
        if (op.status != 0) {
            op.statusMsg = "record append failure; state: ";
            op.statusMsg += GetStateAsStr();
        } else {
            if (mChunkSize < 0) {
                WAPPEND_LOG_STREAM_FATAL <<
                    "begin make stable response: "
                    " chunk: "        << mChunkId <<
                    " invalid size: " << mChunkSize <<
                KFS_LOG_EOM;
                FatalError();
            }
            op.status        = 0;
            op.chunkSize     = mChunkSize;
            op.chunkChecksum = mChunkChecksum;
        }
    }
    WAPPEND_LOG_STREAM(op.status == 0 ?
        MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        "begin make stable done: " << op.statusMsg <<
        " "             << op.Show() <<
        " size: "       << mChunkSize <<
        " checksum: "   << mChunkChecksum <<
        " state: "      << GetStateAsStr() <<
        " wid count: "  << mWriteIdState.size() <<
    KFS_LOG_EOM;
    Cntrs().mBeginMakeStableCount++;
    if (op.status < 0) {
        Cntrs().mBeginMakeStableErrorCount++;
    }
    SubmitOpResponse(&op);
}

void
AtomicRecordAppender::BeginMakeStable(
    BeginMakeChunkStableOp* op /* = 0 */)
{
    WAPPEND_LOG_STREAM_DEBUG <<
        "begin make stable: "
        " chunk: "      << mChunkId <<
        " state: "      << GetStateAsStr() <<
        " wid count: "  << mWriteIdState.size() <<
        " offset: "     << mNextOffset <<
        " reserved: "   << mBytesReserved <<
        " in flight:"
        " replicaton: " << mReplicationsInFlight <<
        " ios: "        << mIoOpsInFlight <<
        " op: "         << KfsOp::ShowOp(op) <<
    KFS_LOG_EOM;

    mLastActivityTime = Now();
    if (op) {
        if (mChunkVersion != op->chunkVersion) {
            op->statusMsg = "invalid chunk version";
            op->status    = kErrParameters;
            SubmitResponse(*op);
            return;
        }
        op->status = 0;
    }
    // Only meta server issues this command, when it decides that
    // the append master is not operational.
    PushBack(mBeginMakeChunkStableOp, mLastBeginMakeChunkStableOp, op);
    if (mMakeChunkStableOp) {
        return; // Wait for make stable to finish, it will send the reply.
    }
    if (mState == kStateOpen || mState == kStateReplicationFailed) {
        SetState(kStateClosed);
    }
    if (mState == kStateClosed) {
        FlushAll();
        if (mNextCommitOffset < mNextWriteOffset) {
            // Always truncate to the last commit offset.
            // This is need to properly handle status inquiry that makes write
            // id read only (prevents further ops with this write id to
            // succeed). If the append is in flight and status inquiry op
            // reaches this node first, then this will guarantee that no
            // participant will commit the in flight append, as it will never
            // receive the append (down the replicaton chain), or replication
            // succeeded status (up the replicaton chain).
            // If the status inquiry op comes in after the append op, then
            // the last op status will be "in progress" (undefined), or already
            // known.
            TrimToLastCommit("begin make stable");
            return;
        }
    }
    if (mState == kStateClosed && mIoOpsInFlight > 0) {
        return; // Completion will be invoked later.
    }
    if (mState == kStateClosed && mChunkSize < 0) {
        ComputeChecksum();
    }
    SubmitResponse(mBeginMakeChunkStableOp, mLastBeginMakeChunkStableOp);
}

void
AtomicRecordAppender::Timeout()
{
    if (DeleteIfNeeded()) {
        return;
    }
    int         nextTimeout    = -1; // infinite
    const int   flushInterval  =
        gAtomicRecordAppendManager.GetFlushIntervalSec();
    // Slaves keep chunk open longer, waiting for [begin] make stable from meta.
    // Ideally the slave timeout should come from master.
    // For now assume that the master and slave have the same value.
    const int   kSlaveTimeoutRatio  = 4;
    const int   kMasterMaxIdleRatio = 2;
    const int   cleanupSec     = gAtomicRecordAppendManager.GetCleanUpSec();
    const int   cleanupTimeout = cleanupSec *
        ((IsMaster() || mState != kStateOpen) ? 1 : kSlaveTimeoutRatio);
    const time_t now           = Now();
    if (mBuffer.BytesConsumable() >=
                gAtomicRecordAppendManager.GetFlushLimit(*this) ||
            (flushInterval >= 0 && mLastFlushTime + flushInterval <= now)) {
        FlushFullBlocks();
    } else if (! mBuffer.IsEmpty() && flushInterval >= 0) {
        nextTimeout = Timer::MinTimeout(nextTimeout,
            int(mLastFlushTime + flushInterval - now));
    }
    if (IsMaster() && mState == kStateOpen) {
        const int ackTm =
            gAtomicRecordAppendManager.GetSendCommitAckTimeoutSec();
        if (ackTm > 0 &&
                mNumServers > 1 && mReplicationsInFlight <= 0 &&
                mNextCommitOffset > mCommitOffsetAckSent) {
            if (mLastActivityTime + ackTm <= now) {
                SendCommitAck();
            } else {
                nextTimeout = Timer::MinTimeout(nextTimeout,
                    int(mLastActivityTime + ackTm - now));
            }
        }
        // If no activity, and no reservations, then master closes the chunk.
        const int closeTimeout = MasterUseShortCloseTimeout() ?
            Timer::MinTimeout(cleanupTimeout, GetCloseEmptyWidStateSec()) :
            cleanupTimeout;
        if (mState == kStateOpen && 0 <= closeTimeout &&
                (mBytesReserved <= 0 || (0 <= cleanupSec &&
                    mLastAppendActivityTime +
                    min(int64_t(cleanupSec) * kMasterMaxIdleRatio,
                        int64_t(cleanupSec) * (int64_t)CHUNKSIZE /
                            max(int64_t(1), mNextOffset)) <= now))) {
            if (mLastAppendActivityTime + closeTimeout <= now) {
                if (! TryToCloseChunk()) {
                    // TryToCloseChunk hasn't scheduled new activity, most
                    // likely are replications in flight. If this is the case
                    // then the cleanup timeout is too short, just retry in 3
                    // sec.
                    // To avoid this "busy wait"  with short timeout an
                    // additional state is needed in the state machine.
                    assert(mReplicationsInFlight > 0);
                    if (mLastAppendActivityTime + closeTimeout <= now) {
                        nextTimeout = Timer::MinTimeout(nextTimeout, 3);
                    }
                }
            } else {
                nextTimeout = Timer::MinTimeout(nextTimeout,
                    int(mLastAppendActivityTime + closeTimeout - now));
            }
        }
    } else if (cleanupTimeout >= 0 &&
            mIoOpsInFlight <= 0 && mReplicationsInFlight <= 0 &&
            mLastActivityTime + cleanupTimeout <= now) {
        time_t metaUptime;
        int    minMetaUptime;
        if (! mMakeStableSucceededFlag && mState != kStateChunkLost &&
                (metaUptime = gMetaServerSM.ConnectionUptime()) <
                (minMetaUptime = max(
                    cleanupTimeout,
                    gAtomicRecordAppendManager.GetMetaMinUptimeSec()
                ))) {
            WAPPEND_LOG_STREAM_INFO << "timeout:"
                " short meta connection uptime;"
                " required uptime: "   << minMetaUptime << " sec."
                " last activty: "      << (now - mLastActivityTime) << " ago" <<
                " new last activity: " << metaUptime                << " ago" <<
            KFS_LOG_EOM;
            mLastActivityTime = now - max(time_t(0), metaUptime);
        } else {
            WAPPEND_LOG_STREAM(mMakeStableSucceededFlag ?
                    MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
                "timeout: deleting write appender"
                " chunk: "     << mChunkId <<
                " state: "     << GetStateAsStr() <<
                " size: "      << mNextWriteOffset <<
                " / "          << mChunkSize <<
                " wid count: " << mWriteIdState.size() <<
                " / "          << mWritableWidCount <<
            KFS_LOG_EOM;
            if (! mMakeStableSucceededFlag) {
                Cntrs().mTimeoutLostCount++;
            }
            if (mState != kStateStable) {
                SetState(kStateChunkLost);
            }
            DeleteSelf();
            return;
        }
    }
    nextTimeout = Timer::MinTimeout(nextTimeout,
        mLastActivityTime + cleanupTimeout > now ?
        int(mLastActivityTime + cleanupTimeout - now) : cleanupTimeout);
    if (mState == kStateOpen && mReplicationsInFlight > 0) {
        const int replicationTimeout = GetNextReplicationTimeout();
        if (replicationTimeout == 0) {
            const RecordAppendOp* const op =
                AppendReplicationList::Front(mReplicationList);
            WAPPEND_LOG_STREAM_ERROR <<
                "replication timeout:"
                " chunk: "   << mChunkId <<
                " state: "   << GetStateAsStr() <<
                " optime: "  << (now - op->replicationStartTime) <<
                " cmd: " << op->Show() <<
            KFS_LOG_EOM;
            Cntrs().mReplicationTimeoutCount++;
            SetState(kStateReplicationFailed);
        } else {
            nextTimeout = Timer::MinTimeout(replicationTimeout, nextTimeout);
        }
    }
    WAPPEND_LOG_STREAM_DEBUG <<
        "set timeout:"
        " chunk: "   << mChunkId <<
        " state: "   << GetStateAsStr() <<
        " timeout: " << nextTimeout <<
    KFS_LOG_EOM;
    mTimer.SetTimeout(nextTimeout);
}

void
AtomicRecordAppender::SubmitResponse(MakeChunkStableOp& op)
{
    op.status = mState == kStateStable ? 0 : kErrFailedState;
    if (op.status != 0) {
        if (! op.statusMsg.empty()) {
            op.statusMsg += " ";
        }
        op.statusMsg += "record append failure; state: ";
        op.statusMsg += GetStateAsStr();
    }
    WAPPEND_LOG_STREAM(op.status == 0 ?
            MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
        "make chunk stable done:"
        " chunk: "       << mChunkId <<
        " state: "       << GetStateAsStr() <<
        " size: "        << mNextWriteOffset << " / " << mChunkSize <<
        " checksum: "    << mChunkChecksum <<
        " in flight:"
        " replication: " << mReplicationsInFlight <<
        " ios: "         << mIoOpsInFlight <<
        " " << op.Show() <<
    KFS_LOG_EOM;
    if (op.clnt == this) {
        delete &op;
    } else {
        Cntrs().mMakeStableCount++;
        if (op.status != 0) {
            Cntrs().mMakeStableErrorCount++;
        }
        if (mChunkSize >= 0) {
            if (mChunkSize < op.chunkSize && op.chunkSize >= 0) {
                Cntrs().mMakeStableLengthErrorCount++;
            }
            if (op.hasChecksum && mChunkChecksum != op.chunkChecksum) {
                Cntrs().mMakeStableChecksumErrorCount++;
            }
        }
        SubmitOpResponse(&op);
    }
}

void
AtomicRecordAppender::MakeChunkStableDone()
{
    mLastActivityTime = Now();
    SubmitResponse(mBeginMakeChunkStableOp, mLastBeginMakeChunkStableOp);
    SubmitResponse(mMakeChunkStableOp, mLastMakeChunkStableOp);
}

void
AtomicRecordAppender::OpDone(ReadOp* op)
{
    // Only read to truncate the chunk should ever get here, and it should be
    // the only one op in flight.
    if (! op ||
            ! mMakeChunkStableOp ||
            mIoOpsInFlight != 1 ||
            (mState != kStateClosed &&
                mState != kStatePendingDelete &&
                mState != kStateChunkLost) ||
            (mMakeChunkStableOp->chunkSize >= 0 &&
            mMakeChunkStableOp->chunkSize !=
                op->offset + int64_t(op->numBytes)) ||
            op->offset < 0 ||
            op->numBytes <= 0 ||
            op->offset % CHECKSUM_BLOCKSIZE != 0 ||
            op->numBytes >= CHECKSUM_BLOCKSIZE) {
        WAPPEND_LOG_STREAM_FATAL <<
            "make chunk stable read:" <<
            " internal error"
            " chunk: "          << mChunkId <<
            " read op: "        << (const void*)op <<
            " make stable op: " << (const void*)mMakeChunkStableOp <<
            " in flight:"
            " replication: "    << mReplicationsInFlight <<
            " ios: "            << mIoOpsInFlight <<
        KFS_LOG_EOM;
        FatalError();
    }
    mIoOpsInFlight--;
    if (DeleteIfNeeded()) {
        delete op;
        return;
    }
    if (op->status >= 0 && ssize_t(op->numBytes) == op->numBytesIO) {
        ChunkInfo_t* const info = gChunkManager.GetChunkInfo(mChunkId);
        if (! info || (! info->chunkBlockChecksum && info->chunkSize != 0)) {
            WAPPEND_LOG_STREAM_FATAL <<
                "make chunk stable read:"
                " failed to get chunk info" <<
                " chunk: "     << mChunkId <<
                " checksums: " <<
                    (const void*)(info ? info->chunkBlockChecksum : 0) <<
                " size: "      << (info ? info->chunkSize : -1) <<
            KFS_LOG_EOM;
            FatalError();
            SetState(kStateChunkLost);
        } else {
            const int64_t newSize = op->offset + op->numBytes;
            if (info->chunkSize < newSize) {
                mChunkSize = -1;
                SetState(kStateChunkLost);
            } else {
                if (newSize > 0) {
                    op->dataBuf.ZeroFill(CHECKSUM_BLOCKSIZE - op->numBytes);
                    info->chunkBlockChecksum[
                        OffsetToChecksumBlockNum(newSize)] =
                    ComputeBlockChecksum(&op->dataBuf,
                        op->dataBuf.BytesConsumable());
                }
                // Truncation done, set the new size.
                gChunkManager.SetChunkSize(*info, newSize);
                mNextWriteOffset = newSize;
            }
        }
    } else {
        Cntrs().mReadErrorCount++;
        SetState(kStateChunkLost);
    }
    WAPPEND_LOG_STREAM(mState != kStateChunkLost ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        "make chunk stable read:"
        " status: "      << op->status <<
        " requested: "   << op->numBytes <<
        " read: "        << op->numBytesIO <<
        " chunk: "       << mChunkId <<
        " state: "       << GetStateAsStr() <<
        " size: "        << mNextWriteOffset << " / " << mChunkSize <<
        " checksum: "    << mChunkChecksum << " / " <<
            mMakeChunkStableOp->chunkChecksum <<
        " in flight:"
        " replication: " << mReplicationsInFlight <<
        " ios: "         << mIoOpsInFlight <<
    KFS_LOG_EOM;
    delete op;
    MakeChunkStable();
}

void
AtomicRecordAppender::MakeChunkStable(MakeChunkStableOp *op /* = 0 */)
{
    mLastActivityTime = Now();
    if (op) {
        MakeChunkStableOp* eo = mMakeChunkStableOp;
        if (eo && eo->clnt == this) {
            eo = eo->next; // get "external" op
            if (eo && eo->clnt == this) {
                FatalError(); // only one "internal" op
            }
        }
        if (op->chunkId != mChunkId) {
            op->status    = kErrParameters;
            op->statusMsg = "invalid chunk id";
        } else if (op->chunkVersion != mChunkVersion) {
            op->status    = kErrParameters;
            op->statusMsg = "invalid chunk version";
        } else if (eo && (
                eo->chunkVersion != op->chunkVersion ||
                ((eo->chunkSize >= 0 && op->chunkSize >= 0 &&
                    eo->chunkSize != op->chunkSize) ||
                (eo->hasChecksum && op->hasChecksum &&
                    eo->chunkChecksum != op->chunkChecksum)))) {
            op->status    = kErrParameters;
            op->statusMsg =
                "request parameters differ from the initial request";
        }
        if (op->status != 0) {
            WAPPEND_LOG_STREAM_ERROR <<
                "make chunk stable: bad request ignored " << op->statusMsg <<
                " chunk: "   << mChunkId      <<
                " version: " << mChunkVersion <<
                " "          << op->Show() <<
            KFS_LOG_EOM;
            if (op->clnt == this) {
                FatalError();
                delete op;
            } else {
                Cntrs().mMakeStableCount++;
                Cntrs().mMakeStableErrorCount++;
                SubmitOpResponse(op);
            }
            return;
        }
        PushBack(mMakeChunkStableOp, mLastMakeChunkStableOp, op);
    }
    WAPPEND_LOG_STREAM(mMakeChunkStableOp ?
            MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelFATAL) <<
        "make chunk stable " <<
            ((mMakeChunkStableOp && mMakeChunkStableOp->clnt == this) ?
                "internal" : "external") << ":" <<
        " chunk: "       << mChunkId <<
        " state: "       << GetStateAsStr() <<
        " size: "        << mNextWriteOffset <<
        " in flight:"
        " replication: " << mReplicationsInFlight <<
        " ios: "         << mIoOpsInFlight <<
        " op: "          << KfsOp::ShowOp(op) <<
    KFS_LOG_EOM;
    if (! mMakeChunkStableOp) {
        FatalError();
    }
    if (mState == kStateOpen || mState == kStateReplicationFailed) {
        SetState(kStateClosed);
        FlushAll();
    }
    // Wait for previously submitted ops to finish
    if (mIoOpsInFlight > 0) {
        return;
    }
    if (mState != kStateClosed) {
        MakeChunkStableDone();
        return;
    }
    if (mMakeChunkStableOp->chunkSize >= 0) {
        const int64_t newSize = mMakeChunkStableOp->chunkSize;
        if (newSize > mNextWriteOffset) {
            SetState(kStateChunkLost);
        } else if (newSize < mNextWriteOffset) {
            WAPPEND_LOG_STREAM_INFO <<
                "make chunk stable: truncating chunk to: " << newSize <<
                " current size: " << mNextWriteOffset << " / " << mChunkSize <<
            KFS_LOG_EOM;
            if (newSize > 0 && newSize % CHECKSUM_BLOCKSIZE != 0) {
                ReadOp* const rop = new ReadOp(0);
                rop->chunkId      = mChunkId;
                rop->chunkVersion = mChunkVersion;
                rop->offset       =
                    newSize / CHECKSUM_BLOCKSIZE * CHECKSUM_BLOCKSIZE;
                rop->numBytes     = newSize - rop->offset;
                rop->clnt         = this;
                mIoOpsInFlight++;
                const int res = gChunkManager.ReadChunk(rop);
                if (res < 0) {
                    rop->status = res;
                    OpDone(rop);
                }
                return;
            }
            // No last block read and checksum update is needed.
            ChunkInfo_t* const info = gChunkManager.GetChunkInfo(mChunkId);
            if (! info || (! info->chunkBlockChecksum && info->chunkSize != 0)) {
                WAPPEND_LOG_STREAM_FATAL <<
                    "make chunk stable:"
                    " failed to get chunk info" <<
                    " chunk: "     << mChunkId <<
                    " checksums: " <<
                        (const void*)(info ? info->chunkBlockChecksum : 0) <<
                    " size: "      << (info ? info->chunkSize : -1) <<
                KFS_LOG_EOM;
                FatalError();
                SetState(kStateChunkLost);
            }
            if (info->chunkSize < newSize) {
                SetState(kStateChunkLost);
            } else {
                // Truncation done, set the new size.
                gChunkManager.SetChunkSize(*info, newSize);
                mNextWriteOffset = newSize;
            }
        }
    }
    if (mState == kStateClosed && (
            ! ComputeChecksum() ||
            (mMakeChunkStableOp->hasChecksum &&
                mChunkChecksum != mMakeChunkStableOp->chunkChecksum) ||
            (mMakeChunkStableOp->chunkSize >= 0 &&
                mChunkSize != mMakeChunkStableOp->chunkSize))) {
        SetState(kStateChunkLost);
    }
    if (mState != kStateClosed) {
        MakeChunkStableDone();
        return;
    }
    if (mMakeChunkStableOp->clnt == this) {
        // Internally generated op done, see if there are other ops.
        MakeChunkStableOp* const iop = mMakeChunkStableOp;
        mMakeChunkStableOp = iop->next;
        delete iop;
        if (! mMakeChunkStableOp) {
            mLastMakeChunkStableOp = 0;
            if (mBeginMakeChunkStableOp) {
                BeginMakeStable(); // Restart (send response) begin make stable.
            } else {
                // Internal make chunk stable doesn't transition into the
                // "stable" state, it only truncates the chunk, recalculates the
                // checksum, and notifies meta server that chunk append is done.
                NotifyChunkClosed();
            }
            return;
        }
        if ((mMakeChunkStableOp->hasChecksum &&
                mChunkChecksum != mMakeChunkStableOp->chunkChecksum) ||
            (mMakeChunkStableOp->chunkSize >= 0 &&
                mChunkSize != mMakeChunkStableOp->chunkSize)) {
            SetState(kStateChunkLost);
            MakeChunkStableDone();
            return;
        }
    }
    WAPPEND_LOG_STREAM_INFO <<
        "make chunk stable:"
        " starting sync of the metadata"
        " chunk: " << mChunkId <<
        " size: "  << mNextWriteOffset << " / " << GetChunkSize() <<
    KFS_LOG_EOM;
    mIoOpsInFlight++;
    const bool appendFlag = true;
    const int  res        = gChunkManager.MakeChunkStable(
        mChunkId,
        mChunkVersion,
        appendFlag,
        this,
        mMakeChunkStableOp->statusMsg
    );
    if (res < 0) {
        MetaWriteDone(res);
    }
}

void
AtomicRecordAppender::FlushSelf(bool flushFullChecksumBlocks)
{
    if (0 <= mFlushStartByteCount) {
        // Update total count to reflect pending flush, in the case when this
        // method is invoked from GetFlushLimit() by a client thread different
        // than the one that is about to return from re-lock.
        const int prevNumBytes = mFlushStartByteCount;
        mFlushStartByteCount = -1;
        gAtomicRecordAppendManager.GetFlushLimit(
            *this, mBuffer.BytesConsumable() - prevNumBytes);
    }
    mLastFlushTime = Now();
    SetCanDoLowOnBuffersFlushFlag(false);
    if (mStaggerRMWInFlightFlag) {
        mRestartFlushFlag    = ! mBuffer.IsEmpty();
        mFlushFullBlocksFlag = mFlushFullBlocksFlag || flushFullChecksumBlocks;
        UpdateAlignment();
        return;
    }
    mRestartFlushFlag = false;
    while (mState == kStateOpen ||
            mState == kStateClosed ||
            mState == kStateReplicationFailed) {
        const int nBytes = mBuffer.BytesConsumable();
        if (nBytes <= (flushFullChecksumBlocks ? (int)CHECKSUM_BLOCKSIZE : 0)) {
            UpdateAlignment();
            return;
        }
        assert(! mStaggerRMWInFlightFlag);
        size_t bytesToFlush(nBytes <= int(CHECKSUM_BLOCKSIZE) ?
            nBytes : nBytes / CHECKSUM_BLOCKSIZE * CHECKSUM_BLOCKSIZE);
        // assert(IsChunkOpen()); // OK to flush deleted chunk.

        // The chunk manager write code requires writes where the # of bytes ==
        // size of checksum block to be aligned to checksum boundaries;
        // otherwise, the writes should be less than a checksum block.
        //
        // Set RMW flag to allow *only one* concurrent partial checksum block
        // write withing the same checksum block. This is need because io
        // completion order is undefined, and partial checksum block write does
        // read modify write. Obviously two such writes withing the same block
        // need to be ordered.
        //
        const int blkOffset = (int)(mNextWriteOffset % (int)CHECKSUM_BLOCKSIZE);
        if (blkOffset > 0) {
            mStaggerRMWInFlightFlag =
                blkOffset + bytesToFlush < CHECKSUM_BLOCKSIZE;
            if (! mStaggerRMWInFlightFlag) {
                bytesToFlush = CHECKSUM_BLOCKSIZE - blkOffset;
            }
            assert(! mStaggerRMWInFlightFlag || bytesToFlush == size_t(nBytes));
        } else {
            mStaggerRMWInFlightFlag = bytesToFlush < CHECKSUM_BLOCKSIZE;
        }
        WriteOp* const wop = new WriteOp(mChunkId, mChunkVersion);
        wop->InitForRecordAppend();
        wop->clnt     = this;
        wop->offset   = mNextWriteOffset;
        wop->numBytes = bytesToFlush;
        if (bytesToFlush < CHECKSUM_BLOCKSIZE) {
            // Buffer don't have to be full and aligned, chunk manager will have
            // to do partial checksum block write, and invoke
            // ReplaceKeepBuffersFull() anyway.
            wop->dataBuf.Move(&mBuffer, bytesToFlush);
        } else {
            // Buffer size should always be multiple of checksum block size.
            assert(mNextWriteOffset %
                IOBufferData::GetDefaultBufferSize() == 0);
            wop->dataBuf.ReplaceKeepBuffersFull(&mBuffer, 0, bytesToFlush);
        }
        const int newLimit = gAtomicRecordAppendManager.GetFlushLimit(
            *this, mBuffer.BytesConsumable() - nBytes);

        WAPPEND_LOG_STREAM_DEBUG <<
            "flush write"
            " state: "       << GetStateAsStr() <<
            " chunk: "       << wop->chunkId <<
            " offset: "      << wop->offset <<
            " bytes: "       << wop->numBytes <<
            " buffered: "    << mBuffer.BytesConsumable() <<
            " flush limit: " << newLimit <<
            " in flight:"
            " replicaton: "  << mReplicationsInFlight <<
            " ios: "         << mIoOpsInFlight <<
            " chksum cnt: "  << mTmpChecksums.size() <<
        KFS_LOG_EOM;
        // The checksum for partial block is always removed here, in both cases
        // where this is read modify write, or just last partial block.
        // The [partial] block checksum is always added in the append begin. In
        // case of read modify write the checksum is the checksum of the
        // newly added "tail" of the block, and the beginning of the block was
        // previously written.
        const size_t blkCnt = (bytesToFlush + CHECKSUM_BLOCKSIZE - 1) /
            CHECKSUM_BLOCKSIZE;
        if (mTmpChecksums.size() < blkCnt) {
            WAPPEND_LOG_STREAM_FATAL <<
                "invalid checksum vector"
                " size: "        << mTmpChecksums.size() <<
                " less than: "   << blkCnt <<
                " state: "       << GetStateAsStr() <<
                " chunk: "       << wop->chunkId <<
                " offset: "      << wop->offset <<
                " bytes: "       << wop->numBytes <<
                " buffered: "    << mBuffer.BytesConsumable() <<
                " flush limit: " << newLimit <<
                " in flight:"
                " replicaton: "  << mReplicationsInFlight <<
                " ios: "         << mIoOpsInFlight <<
            KFS_LOG_EOM;
            FatalError();
            UpdateAlignment();
            int res = kErrParameters;
            wop->status = res;;
            wop->HandleEvent(EVENT_DISK_ERROR, &res);
            return;
        }
        // Do not assign the checksum tail of RMW block, the checksum
        // has to be re-computed anyway.
        bool eraseFlag = true;
        if (blkOffset <= 0) {
            if (mTmpChecksums.size() <= max(blkCnt, size_t(24))) {
                wop->checksums.swap(mTmpChecksums);
                mTmpChecksums.reserve(
                    max(size_t(32), wop->checksums.size() - blkCnt));
                mTmpChecksums.assign(wop->checksums.begin() + blkCnt,
                    wop->checksums.end());
                wop->checksums.resize(blkCnt);
                eraseFlag = false;
            } else {
                wop->checksums.assign(
                    mTmpChecksums.begin(), mTmpChecksums.begin() + blkCnt);
            }
        }
        if (eraseFlag) {
            mTmpChecksums.erase(
                mTmpChecksums.begin(), mTmpChecksums.begin() + blkCnt);
        }

        mNextWriteOffset += bytesToFlush;
        mBufFrontPadding = 0;
        if (bytesToFlush < CHECKSUM_BLOCKSIZE) {
            const int off = (int)(
                mNextWriteOffset % IOBufferData::GetDefaultBufferSize());
            if (off == 0) {
                mBuffer.MakeBuffersFull();
            } else {
                assert(off > 0 && mBuffer.IsEmpty());
                mBufFrontPadding = off;
            }
        }
        mIoOpsInFlight++;
        int res = gChunkManager.WriteChunk(wop);
        if (res < 0) {
            // Failed to start write, call error handler and return immediately.
            // Assume that error handler can delete this.
            UpdateAlignment();
            wop->status = res;
            wop->HandleEvent(EVENT_DISK_ERROR, &res);
            return;
        }
    }
}

void
AtomicRecordAppender::OpDone(WriteOp *op)
{
    assert(
        op->chunkId == mChunkId && mIoOpsInFlight > 0 &&
        (mStaggerRMWInFlightFlag || op->numBytes >= CHECKSUM_BLOCKSIZE)
    );
    mIoOpsInFlight--;
    const bool failedFlag =
        op->status < 0 || size_t(op->status) < op->numBytes;
    WAPPEND_LOG_STREAM(failedFlag ?
            MsgLogger::kLogLevelERROR : MsgLogger::kLogLevelDEBUG) <<
        "write " << (failedFlag ? "FAILED" : "done") <<
        " chunk: "      << mChunkId <<
        " offset: "     << op->offset <<
        " size: "       << op->numBytes <<
        " commit: "     << mNextCommitOffset <<
        " in flight:"
        " replicaton: " << mReplicationsInFlight <<
        " ios: "        << mIoOpsInFlight <<
        " status: "     << op->status <<
        " chunk size: " << GetChunkSize() <<
    KFS_LOG_EOM;
    const int64_t end = op->offset + op->numBytes;
    delete op;
    if (DeleteIfNeeded()) {
        return;
    }
    if (failedFlag) {
        Cntrs().mWriteErrorCount++;
        SetState(kStateChunkLost);
    }
    // There could be more that one write in flight, but only one stagger.
    // The stagger end, by definition, is not on checksum block boundary, but
    // all other writes should end exactly on checksum block boundary.
    if (mStaggerRMWInFlightFlag && end % CHECKSUM_BLOCKSIZE != 0) {
        mStaggerRMWInFlightFlag = false;
        if (mRestartFlushFlag) {
            FlushSelf(mFlushFullBlocksFlag);
        }
    }
    if (mIoOpsInFlight <= 0 && mBeginMakeChunkStableOp) {
        BeginMakeStable();
    }
    if (mIoOpsInFlight <= 0 && mMakeChunkStableOp) {
        MakeChunkStable();
    }
}

void
AtomicRecordAppender::MetaWriteDone(int status)
{
    assert(mIoOpsInFlight > 0);
    mIoOpsInFlight--;
    WAPPEND_LOG_STREAM(status >= 0 ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        "meta write " << (status < 0 ? "FAILED" : "done") <<
        " chunk: "      << mChunkId <<
        " in flight:"
        " replicaton: " << mReplicationsInFlight <<
        " ios: "        << mIoOpsInFlight <<
        " commit: "     << mNextCommitOffset <<
        " status: "     << status <<
        " owner: "      << gChunkManager.IsWriteAppenderOwns(mChunkId) <<
    KFS_LOG_EOM;
    if (DeleteIfNeeded()) {
        return;
    }
    if (status < 0) {
        Cntrs().mWriteErrorCount++;
        SetState(kStateChunkLost);
    }
    if (mState == kStateClosed) {
        SetState(kStateStable);
    }
    MakeChunkStable();
}

bool
AtomicRecordAppender::TryToCloseChunk()
{
    if (! IsMaster()) {
        return false;
    }
    SendCommitAck();
    if (mState == kStateOpen && 0 < mReplicationsInFlight) {
        return false;
    }
    if (mState == kStateOpen || mState == kStateReplicationFailed) {
        if (mMakeChunkStableOp || mBeginMakeChunkStableOp) {
            FatalError();
        }
        TrimToLastCommit("try to close");
    }
    return true;
}

void
AtomicRecordAppender::TrimToLastCommit(
    const char* inMsgPtr)
{
    if (mMakeChunkStableOp) {
        FatalError();
    }
    WAPPEND_LOG_STREAM_DEBUG <<
        (inMsgPtr ? inMsgPtr : "trim to last commit") <<
        " chunk: "   << mChunkId <<
        " version: " << mChunkVersion <<
        " size: "    << mNextCommitOffset <<
    KFS_LOG_EOM;
    // Trim the chunk on failure to the last committed offset, if needed.
    MakeChunkStableOp* const op = new MakeChunkStableOp(0);
    op->chunkId      = mChunkId;
    op->chunkVersion = mChunkVersion;
    op->clnt         = this;
    op->chunkSize    = mNextCommitOffset;
    MakeChunkStable(op);
}

void
AtomicRecordAppender::NotifyChunkClosed()
{
    assert(IsMaster() && mState == kStateClosed);
    WAPPEND_LOG_STREAM_DEBUG <<
        "notify closed:"
        " chunk: "    << mChunkId <<
        " size: "     << mChunkSize <<
        " checksum: " << mChunkChecksum <<
    KFS_LOG_EOM;
    gLeaseClerk.RelinquishLease(mChunkId, mChunkSize, true, mChunkChecksum);
}

void
AtomicRecordAppender::SendCommitAck()
{
    // Use write offset as seq. # for debugging
    RecordAppendOp* const op = (
            ! IsMaster() ||
            kStateOpen != mState ||
            mNumServers <= 1 ||
            0 < mReplicationsInFlight ||
            mNextCommitOffset <= mCommitOffsetAckSent
        ) ? 0 : new RecordAppendOp(mNextWriteOffset);
    CheckLeaseAndChunk("send commit ack", op);
    if (! op) {
        return;
    }
    if (kStateOpen != mState) {
        delete op;
        return;
    }
    WAPPEND_LOG_STREAM_DEBUG <<
        "send commit ack"
        " chunk: "    << mChunkId <<
        " last ack: " << mCommitOffsetAckSent <<
        " size: "     << mNextCommitOffset <<
        " unacked: "  << (mNextCommitOffset - mCommitOffsetAckSent) <<
    KFS_LOG_EOM;
    op->clnt         = this;
    op->chunkId      = mChunkId;
    op->chunkVersion = mChunkVersion;
    op->numServers   = mNumServers;
    op->servers      = mCommitAckServers;
    op->numBytes     = 0;
    AppendBegin(op, mReplicationPos, mPeerLocation);
}

AtomicRecordAppendManager::AtomicRecordAppendManager()
    : mAppenders(),
      mCleanUpSec(LEASE_INTERVAL_SECS + 60),
      mCloseEmptyWidStateSec(60),
      mFlushIntervalSec(60),
      mSendCommitAckTimeoutSec(2),
      mReplicationTimeoutSec(3 * 60),
      mMinMetaUptimeSec(8 * 60),
      mFlushLimit(1 << 20),
      mMaxAppenderBytes(0),
      mTotalBuffersBytes(0),
      mTotalPendingBytes(0),
      mActiveAppendersCount(0),
      mOpenAppendersCount(0),
      mAppendersWithWidCount(0),
      mBufferLimitRatio(0.6),
      mMaxWriteIdsPerChunk(16 << 10),
      mCloseOutOfSpaceThreshold(4),
      mCloseOutOfSpaceSec(5),
      mRecursionCount(0),
      mAppendDropLockMinSize((4 << 10) - 1),
      mCloseMinChunkSize(
        (chunkOff_t)CHUNKSIZE - (chunkOff_t)CHECKSUM_BLOCKSIZE),
      mMutexesCount(-1),
      mCurMutexIdx(0),
      mMutexes(0),
      mCurUpdateFlush(0),
      mCurUpdateLowBufFlush(0),
      mInstanceNum(0),
      mCounters()
{
    PendingFlushList::Init(mPendingFlushList);
    mCounters.Clear();
}

AtomicRecordAppendManager::~AtomicRecordAppendManager()
{
    assert(mAppenders.IsEmpty() && mRecursionCount == 0);
    delete [] mMutexes;
}

void
AtomicRecordAppendManager::SetParameters(const Properties& props)
{
    mCleanUpSec              = props.getValue(
        "chunkServer.recAppender.cleanupSec",          mCleanUpSec);
    mCloseEmptyWidStateSec   = props.getValue(
        "chunkServer.recAppender.closeEmptyWidStateSec",
        mCloseEmptyWidStateSec);
    mFlushIntervalSec        = props.getValue(
        "chunkServer.recAppender.flushIntervalSec",    mFlushIntervalSec),
    mSendCommitAckTimeoutSec = props.getValue(
        "chunkServer.recAppender.sendCommitAckTimeoutSec",
        mSendCommitAckTimeoutSec);
    mReplicationTimeoutSec = props.getValue(
        "chunkServer.recAppender.replicationTimeoutSec",
        mReplicationTimeoutSec);
    mMinMetaUptimeSec = props.getValue(
        "chunkServer.recAppender.minMetaUptimeSec",
        mMinMetaUptimeSec);
    mFlushLimit              = props.getValue(
        "chunkServer.recAppender.flushLimit",          mFlushLimit),
    mBufferLimitRatio        = props.getValue(
        "chunkServer.recAppender.bufferLimitRatio",    mBufferLimitRatio),
    mMaxWriteIdsPerChunk     = props.getValue(
        "chunkServer.recAppender.maxWriteIdsPerChunk", mMaxWriteIdsPerChunk);
    mCloseOutOfSpaceThreshold     = props.getValue(
        "chunkServer.recAppender.closeOutOfSpaceThreshold",
        mCloseOutOfSpaceThreshold);
    mCloseOutOfSpaceSec     = props.getValue(
        "chunkServer.recAppender.closeOutOfSpaceSec", mCloseOutOfSpaceSec);
    mAppendDropLockMinSize  = max(0, props.getValue(
        "chunkServer.recAppender.dropLockMinSize",    mAppendDropLockMinSize));
    mCloseMinChunkSize  = max((chunkOff_t)CHECKSUM_BLOCKSIZE, props.getValue(
        "chunkServer.recAppender.closeMinChunkSize",  mCloseMinChunkSize));
    mTotalBuffersBytes       = 0;
    if (! mAppenders.IsEmpty()) {
        UpdateAppenderFlushLimit();
    }
}

int
AtomicRecordAppendManager::GetFlushLimit(
    AtomicRecordAppender& /* appender */, int addBytes /* = 0 */)
{
    if (addBytes != 0) {
        assert(0 <= mTotalPendingBytes + addBytes);
        mTotalPendingBytes += addBytes;
        mCounters.mPendingByteCount += addBytes;
        UpdateAppenderFlushLimit();
    }
    return mMaxAppenderBytes;
}

void
AtomicRecordAppendManager::UpdateAppenderFlushLimit(
    const AtomicRecordAppender* appender /* = 0 */)
{
    if (! appender && 0 < mTotalBuffersBytes) {
        return;
    }
    if (mTotalBuffersBytes <= 0) {
        mTotalBuffersBytes = (int64_t)(
            DiskIo::GetBufferManager().GetTotalCount() *
            mBufferLimitRatio);
        if (mTotalBuffersBytes <= 0) {
            mTotalBuffersBytes = mFlushLimit;
        }
    }
    assert(0 <= mActiveAppendersCount);
    if (appender) {
        if (appender->IsChunkStable()) {
            assert(mActiveAppendersCount > 0);
            mActiveAppendersCount--;
        } else {
            assert((size_t)mActiveAppendersCount < mAppenders.GetSize());
            mActiveAppendersCount++;
        }
    }
    const int64_t prevLimit = mMaxAppenderBytes;
    mMaxAppenderBytes = min(
        int64_t(mFlushLimit),
        mTotalBuffersBytes / max(int64_t(1), mActiveAppendersCount)
    );
    if (mRecursionCount <= 1 && ! mCurUpdateFlush &&
            mMaxAppenderBytes < prevLimit * 15 / 16) {
        mRecursionCount++;
        mCurUpdateFlush = PendingFlushList::Front(mPendingFlushList);
        while (mCurUpdateFlush) {
            AtomicRecordAppender& cur = *mCurUpdateFlush;
            mCurUpdateFlush = &PendingFlushList::GetNext(cur);
            if (mCurUpdateFlush == PendingFlushList::Front(mPendingFlushList)) {
                mCurUpdateFlush = 0;
            }
            cur.UpdateFlushLimit(mMaxAppenderBytes);
        }
        mRecursionCount--;
        assert(0 <= mRecursionCount);
    }
}

void
AtomicRecordAppendManager::AllocateChunk(
    AllocChunkOp*          op,
    int                    replicationPos,
    const ServerLocation&  peerLoc,
    const DiskIo::FilePtr& chunkFileHandle)
{
    assert(op);
    bool insertedFlag = false;
    AtomicRecordAppender** const res = mAppenders.Insert(
        op->chunkId, (AtomicRecordAppender*)0, insertedFlag);
    assert(insertedFlag == (! *res));
    if (insertedFlag) {
        const ChunkInfo_t* info = 0;
        if (! chunkFileHandle ||
                ! chunkFileHandle->IsOpen() ||
                ! (info = gChunkManager.GetChunkInfo(op->chunkId)) ||
                (! info->chunkBlockChecksum && info->chunkSize != 0)) {
            op->statusMsg = "chunk manager closed this chunk";
            op->status    = AtomicRecordAppender::kErrParameters;
        } else if (op->chunkVersion != info->chunkVersion) {
            op->statusMsg = "invalid chunk version";
            op->status    = AtomicRecordAppender::kErrParameters;
        }
        WAPPEND_LOG_STREAM(op->status == 0 ?
                MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
            "allocate chunk: creating new appender: " <<
                op->statusMsg <<
            " status: "         << op->status  <<
            " appender count: " << mAppenders.GetSize() <<
            " chunk: "          << op->chunkId <<
            " checksums: "      <<
                (const void*)(info ? info->chunkBlockChecksum : 0) <<
            " size: "           << (info ? info->chunkSize : int64_t(-1)) <<
            " version: "        << (info ? info->chunkVersion : (int64_t)-1) <<
            " file handle: "    << (const void*)chunkFileHandle.get() <<
            " file open: "      << (chunkFileHandle->IsOpen() ? "yes" : "no") <<
            " " << op->Show() <<
        KFS_LOG_EOM;
        if (op->status != 0) {
            mAppenders.Erase(op->chunkId);
        } else {
            RemoteSyncSMPtr peer;
            if (uint32_t(replicationPos + 1) < op->numServers) {
                SRChunkServerAccess::Token token;
                SRChunkServerAccess::Token key;
                if (op->syncReplicationAccess.chunkServerAccess) {
                    const SRChunkServerAccess& csa =
                        *op->syncReplicationAccess.chunkServerAccess;
                    token = csa.token;
                    key   = csa.key;
                }
                const bool kConnectFlag              = false;
                const bool kForceUseClientThreadFlag = true;
                peer = RemoteSyncSM::Create(
                    peerLoc,
                    token.mPtr,
                    token.mLen,
                    key.mPtr,
                    key.mLen,
                    replicationPos == 0,
                    op->allowCSClearTextFlag,
                    op->status,
                    op->statusMsg,
                    kConnectFlag,
                    kForceUseClientThreadFlag
                );
                if (! peer && 0 <= op->status) {
                    op->status    = AtomicRecordAppender::kErrFailedState;
                    op->statusMsg = "failed to create forwarding peer";
                }
            }
            if (op->status != 0) {
                mAppenders.Erase(op->chunkId);
            } else {
                QCMutex* mutex = 0;
                if (mMutexesCount < 0) {
                    if (gClientManager.GetMutexPtr()) {
                        mMutexesCount = 1 << 8;
                        mMutexes = new QCMutex[mMutexesCount];
                    } else {
                        mMutexesCount = 0;
                        delete [] mMutexes;
                        mMutexes = 0;
                    }
                }
                if (mMutexes) {
                    if (mMutexesCount <= mCurMutexIdx) {
                        mCurMutexIdx = 0;
                    }
                    mutex = mMutexes + mCurMutexIdx;
                    ++mCurMutexIdx;
                }
                *res = new AtomicRecordAppender(
                    chunkFileHandle,
                    op->chunkId,
                    op->chunkVersion,
                    op->numServers,
                    op->servers,
                    peerLoc,
                    replicationPos,
                    info->chunkSize,
                    peer,
                    mutex
                );
                mOpenAppendersCount++;
                UpdateAppenderFlushLimit(*res);
            }
        }
    } else if ((*res)->IsOpen()) {
        op->status = (*res)->CheckParameters(
            op->chunkVersion, op->numServers,
            op->servers, replicationPos, peerLoc, chunkFileHandle, op->statusMsg);
        WAPPEND_LOG_STREAM(op->status == 0 ?
                MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
            "allocate chunk: appender exists: " <<
            " chunk: "          << op->chunkId <<
            " status: "         << op->status  <<
            " appender count: " << mAppenders.GetSize() <<
            " " << op->Show() <<
        KFS_LOG_EOM;
    } else {
        // This should not normally happen, but this could happen when meta
        // server restarts with partially (or completely) lost meta data, and
        // meta server re-uses the same chunk id.
        // Cleanup lingering appedner and retry.
        (*res)->Delete();
        AllocateChunk(op, replicationPos, peerLoc, chunkFileHandle);
        return; // Tail recursion.
    }
    if (replicationPos == 0) {
        mCounters.mAppenderAllocMasterCount++;
    }
    mCounters.mAppenderAllocCount++;
    if (op->status != 0) {
        mCounters.mAppenderAllocErrorCount++;
    }
}

void
AtomicRecordAppendManager::AllocateWriteId(
    WriteIdAllocOp*        op,
    int                    replicationPos,
    const ServerLocation&  peerLoc,
    const DiskIo::FilePtr& chunkFileHandle)
{
    assert(op);
    AtomicRecordAppender** const appender = mAppenders.Find(op->chunkId);
    if (! appender) {
        op->statusMsg = "not open for append; no appender";
        op->status    = AtomicRecordAppender::kErrParameters;
        mCounters.mWriteIdAllocNoAppenderCount++;
    } else {
        (*appender)->AllocateWriteId(
            op, replicationPos, peerLoc, chunkFileHandle);
    }
    mCounters.mWriteIdAllocCount++;
    if (op->status != 0) {
        mCounters.mWriteIdAllocErrorCount++;
    }
}

void
AtomicRecordAppendManager::Timeout()
{
    FlushIfLowOnBuffers();
}

void
AtomicRecordAppendManager::FlushIfLowOnBuffers()
{
    if (1 < mRecursionCount || mCurUpdateLowBufFlush) {
        return; // Prevent recursion.
    }
    if (! DiskIo::GetBufferManager().IsLowOnBuffers()) {
        return;
    }
    if (mRecursionCount <= 0) {
        mCounters.mLowOnBuffersFlushCount++;
    }
    mRecursionCount++;
    mCurUpdateLowBufFlush = PendingFlushList::Front(mPendingFlushList);
    while (mCurUpdateLowBufFlush) {
        AtomicRecordAppender& cur = *mCurUpdateLowBufFlush;
        mCurUpdateLowBufFlush = &PendingFlushList::GetNext(cur);
        if (mCurUpdateLowBufFlush ==
                PendingFlushList::Front(mPendingFlushList)) {
            mCurUpdateLowBufFlush = 0;
        }
        cur.LowOnBuffersFlush();
    }
    mRecursionCount--;
    assert(0 <= mRecursionCount);
}

bool
AtomicRecordAppendManager::IsChunkStable(kfsChunkId_t chunkId) const
{
    AtomicRecordAppender** const appender = mAppenders.Find(chunkId);
    return (! appender || (*appender)->IsChunkStable());
}

bool
AtomicRecordAppendManager::IsSpaceReservedInChunk(kfsChunkId_t chunkId)
{
    AtomicRecordAppender** const appender = mAppenders.Find(chunkId);
    return (appender && (*appender)->SpaceReserved() > 0);
}

int
AtomicRecordAppendManager::ChunkSpaceReserve(
    kfsChunkId_t chunkId, int64_t writeId, size_t nBytes, string* errMsg /* = 0 */)
{
    AtomicRecordAppender** const appender = mAppenders.Find(chunkId);
    int status;
    if (! appender) {
        if (errMsg) {
            (*errMsg) += "chunk does not exist or not open for append";
        }
        status = AtomicRecordAppender::kErrParameters;
    } else {
        status = (*appender)->ChangeChunkSpaceReservaton(
            writeId, nBytes, false, errMsg);
    }
    mCounters.mSpaceReserveCount++;
    if (status != 0) {
        mCounters.mSpaceReserveErrorCount++;
        if (status == AtomicRecordAppender::kErrOutOfSpace) {
            mCounters.mSpaceReserveDeniedCount++;
        }
    } else {
        mCounters.mSpaceReserveByteCount += nBytes;
    }
    return status;
}

int
AtomicRecordAppendManager::ChunkSpaceRelease(
    kfsChunkId_t chunkId, int64_t writeId, size_t nBytes, string* errMsg /* = 0 */)
{
    AtomicRecordAppender** const appender = mAppenders.Find(chunkId);
    if (! appender) {
        if (errMsg) {
            (*errMsg) += "chunk does not exist or not open for append";
        }
        return AtomicRecordAppender::kErrParameters;
    }
    return (*appender)->ChangeChunkSpaceReservaton(
        writeId, nBytes, true, errMsg);
}

int
AtomicRecordAppendManager::InvalidateWriteId(
    kfsChunkId_t chunkId, int64_t writeId, bool declareFailureFlag)
{
    AtomicRecordAppender** const appender = mAppenders.Find(chunkId);
    return (appender ?
        (*appender)->InvalidateWriteId(writeId, declareFailureFlag) : 0);
}

int
AtomicRecordAppendManager::GetAlignmentAndFwdFlag(kfsChunkId_t chunkId,
    bool& forwardFlag) const
{
    forwardFlag = false;
    AtomicRecordAppender** const appender = mAppenders.Find(chunkId);
    return (appender ?
        (*appender)->GetAlignmentAndFwdFlag(forwardFlag) : 0);
}

bool
AtomicRecordAppendManager::BeginMakeChunkStable(BeginMakeChunkStableOp* op)
{
    assert(op);
    AtomicRecordAppender** const appender = mAppenders.Find(op->chunkId);
    if (! appender) {
        op->statusMsg = "chunk does not exist or not open for append";
        op->status    = AtomicRecordAppender::kErrParameters;
        WAPPEND_LOG_STREAM_ERROR <<
            "begin make stable: no write appender"
            " chunk: "  << op->chunkId   <<
            " status: " << op->status    <<
            " msg: "    << op->statusMsg <<
            " " << op->Show() <<
        KFS_LOG_EOM;
        mCounters.mBeginMakeStableCount++;
        mCounters.mBeginMakeStableErrorCount++;
        return false; // Submit response now.
    }
    (*appender)->BeginChunkMakeStable(op);
    // Completion handler is already invoked or will be invoked later.
    return true;
}

bool
AtomicRecordAppendManager::CloseChunk(
    CloseOp* op, int64_t writeId, bool& forwardFlag)
{
    assert(op);
    AtomicRecordAppender** const appender = mAppenders.Find(op->chunkId);
    if (! appender) {
        return false; // let chunk manager handle it
    }
    (*appender)->CloseChunk(op, writeId, forwardFlag);
    return true;
}

bool
AtomicRecordAppendManager::MakeChunkStable(MakeChunkStableOp* op)
{
    assert(op);
    AtomicRecordAppender** const appender = mAppenders.Find(op->chunkId);
    if (appender) {
        (*appender)->MakeChunkStableEx(op);
        // Completion handler is already invoked or will be invoked later.
        return true;
    }
    int64_t  chunkSize     = -1;
    uint32_t chunkChecksum = 0;
    if (op->hasChecksum) {
        // The following is pretty much redundant now when write appender
        // created at the time of chunk allocation.
        if (AtomicRecordAppender::ComputeChecksum(
                op->chunkId, op->chunkVersion, chunkSize, chunkChecksum) &&
                chunkSize == op->chunkSize &&
                chunkChecksum == op->chunkChecksum) {
            op->status = 0;
        } else {
            op->statusMsg = "no write appender, checksum or size mismatch";
            op->status = AtomicRecordAppender::kErrFailedState;
            // Wait for meta sever to tell what to do with the chunk.
            // It is possible that this is stale make stable completion.
            // gChunkManager.ChunkIOFailed(op->chunkId, -EIO, 0);
            mCounters.mLostChunkCount++;
        }
    }
    WAPPEND_LOG_STREAM(op->status == 0 ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        "make stable: no write appender"
        " chunk: "    << op->chunkId   <<
        " status: "   << op->status    <<
        " msg: "      << op->statusMsg <<
        " size: "     << chunkSize     <<
        " checksum: " << chunkChecksum <<
        " " << op->Show() <<
    KFS_LOG_EOM;
    mCounters.mMakeStableCount++;
    if (op->status != 0) {
        mCounters.mMakeStableErrorCount++;
        if (op->hasChecksum) {
            if (chunkSize != op->chunkSize) {
                mCounters.mMakeStableLengthErrorCount++;
            }
            if (chunkChecksum != op->chunkChecksum) {
                mCounters.mMakeStableChecksumErrorCount++;
            }
        }
    } else {
        const bool appendFlag = false;
        const int res         = gChunkManager.MakeChunkStable(
            op->chunkId, op->chunkVersion, appendFlag, op, op->statusMsg);
        if (res >= 0) {
            return true;
        }
        op->status = res;
    }
    return false; // Submit response now.
}

void
AtomicRecordAppendManager::AppendBegin(
    RecordAppendOp*       op,
    int                   replicationPos,
    const ServerLocation& peerLoc)
{
    assert(op);
    AtomicRecordAppender** const appender = mAppenders.Find(op->chunkId);
    if (! appender) {
        op->status    = AtomicRecordAppender::kErrParameters;
        op->statusMsg = "chunk does not exist or not open for append";
        mCounters.mAppendCount++;
        mCounters.mAppendErrorCount++;
        SubmitOpResponse(op);
    } else {
        (*appender)->AppendChunkBegin(op, replicationPos, peerLoc);
    }
}

void
AtomicRecordAppendManager::GetOpStatus(GetRecordAppendOpStatus* op)
{
    assert(op);
    AtomicRecordAppender** const appender = mAppenders.Find(op->chunkId);
    if (! appender) {
        op->status    = AtomicRecordAppender::kErrParameters;
        op->statusMsg = "chunk does not exist or not open for append";
    } else {
        (*appender)->GetOpStatus(op);
    }
    mCounters.mGetOpStatusCount++;
    if (op->status != 0) {
        mCounters.mGetOpStatusErrorCount++;
    } else if (op->opStatus != AtomicRecordAppender::kErrStatusInProgress) {
        mCounters.mGetOpStatusKnownCount++;
    }
}

bool
AtomicRecordAppendManager::WantsToKeepLease(kfsChunkId_t chunkId) const
{
    AtomicRecordAppender** const appender = mAppenders.Find(chunkId);
    return (appender && (*appender)->WantsToKeepLease());
}

void
AtomicRecordAppendManager:: DeleteChunk(kfsChunkId_t chunkId)
{
    AtomicRecordAppender** const appender = mAppenders.Find(chunkId);
    if (appender) {
        (*appender)->DeleteChunk();
    }
}

void
AtomicRecordAppendManager::Shutdown()
{
    const ARAMapEntry* entry;
    mAppenders.First();
    while ((entry = mAppenders.Next())) {
        entry->GetVal()->Delete();
        mAppenders.First();
    }
}

AtomicRecordAppendManager gAtomicRecordAppendManager;

}
