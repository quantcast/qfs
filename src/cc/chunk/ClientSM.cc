//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/23
// Author: Sriram Rao
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
//
//----------------------------------------------------------------------------

#include "ClientSM.h"

#include "ChunkManager.h"
#include "ChunkServer.h"
#include "utils.h"
#include "KfsOps.h"
#include "AtomicRecordAppender.h"
#include "DiskIo.h"
#include "ClientManager.h"

#include "common/MsgLogger.h"
#include "common/time.h"
#include "kfsio/Globals.h"
#include "kfsio/ChunkAccessToken.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcdebug.h"

#include <algorithm>
#include <string>
#include <sstream>

#define CLIENT_SM_LOG_STREAM_PREFIX \
    << "I" << mInstanceNum << "I " << GetPeerName() << " "
#define CLIENT_SM_LOG_STREAM(pri)  \
    KFS_LOG_STREAM(pri)  CLIENT_SM_LOG_STREAM_PREFIX
#define CLIENT_SM_LOG_STREAM_DEBUG \
    KFS_LOG_STREAM_DEBUG CLIENT_SM_LOG_STREAM_PREFIX
#define CLIENT_SM_LOG_STREAM_WARN  \
    KFS_LOG_STREAM_WARN  CLIENT_SM_LOG_STREAM_PREFIX
#define CLIENT_SM_LOG_STREAM_INFO  \
    KFS_LOG_STREAM_INFO  CLIENT_SM_LOG_STREAM_PREFIX
#define CLIENT_SM_LOG_STREAM_ERROR \
    KFS_LOG_STREAM_ERROR CLIENT_SM_LOG_STREAM_PREFIX
#define CLIENT_SM_LOG_STREAM_FATAL \
    KFS_LOG_STREAM_FATAL CLIENT_SM_LOG_STREAM_PREFIX

namespace KFS
{
using std::string;
using std::max;
using std::make_pair;
using std::list;

// KFS client protocol state machine implementation.

int      ClientSM::sMaxCmdHeaderReadAhead    = 1 << 10;
bool     ClientSM::sTraceRequestResponseFlag = false;
bool     ClientSM::sEnforceMaxWaitFlag       = true;
int      ClientSM::sMaxReqSizeDiscard        = 256 << 10;
size_t   ClientSM::sMaxAppendRequestSize     = CHUNKSIZE;
uint64_t ClientSM::sInstanceNum              = 10000;

inline time_t
ClientSM::TimeNow() const
{
    return (mNetConnection ? mNetConnection->TimeNow() : time(0));
}

inline string
ClientSM::GetPeerName()
{
    return mNetConnection->GetPeerName();
}

inline /* static */ BufferManager&
ClientSM::GetBufferManager()
{
    return DiskIo::GetBufferManager();
}

inline /* static */ BufferManager*
ClientSM::FindDevBufferManager(KfsOp& op)
{
    const bool kFindFlag  = true;
    const bool kResetFlag = false;
    return op.GetDeviceBufferManager(kFindFlag, kResetFlag);
}

inline ClientSM::Client*
ClientSM::GetDevBufMgrClient(const BufferManager* bufMgr)
{
    if (! bufMgr) {
        return 0;
    }
    bool insertedFlag = false;
    DevBufferManagerClient** const cli =
        mDevBufMgrClients.Insert(bufMgr, 0, insertedFlag);
    if (! *cli) {
        *cli = new (mDevCliMgrAllocator.allocate(1))
            DevBufferManagerClient(*this);
    }
    return *cli;
}

inline void
ClientSM::PutAndResetDevBufferManager(KfsOp& op, ByteCount opBytes)
{
    const bool kFindFlag  = false;
    const bool kResetFlag = true;
    BufferManager* const devBufMgr =
        op.GetDeviceBufferManager(kFindFlag, kResetFlag);
    if (devBufMgr) {
        // Return everything back to the device buffer manager now, only count
        // pending response against global buffer manager.
        devBufMgr->Put(*GetDevBufMgrClient(devBufMgr), opBytes);
    }
}

inline void
ClientSM::SendResponse(KfsOp& op)
{
    ByteCount       respBytes = mNetConnection->GetNumBytesToWrite();
    const ByteCount opBytes   = op.bufferBytes.mCount;
    SendResponseSelf(op);
    respBytes = max(ByteCount(0),
        mNetConnection->GetNumBytesToWrite() - respBytes);
    mPrevNumToWrite = mNetConnection->GetNumBytesToWrite();
    PutAndResetDevBufferManager(op, opBytes);
    GetBufferManager().Put(*this, opBytes - respBytes);
}

inline static bool
IsDependingOpType(const KfsOp& op)
{
    const KfsOp_t type = op.op;
    return (
        (type == CMD_WRITE_PREPARE &&
            ! static_cast<const WritePrepareOp&>(op).replyRequestedFlag) ||
        (type == CMD_WRITE_PREPARE_FWD &&
            ! static_cast<const WritePrepareFwdOp&>(
                op).owner.replyRequestedFlag) ||
        type == CMD_WRITE
    );
}

/* static */ void
ClientSM::SetParameters(const Properties& prop)
{
    sTraceRequestResponseFlag = prop.getValue(
        "chunkServer.clientSM.traceRequestResponse",
        sTraceRequestResponseFlag ? 1 : 0) != 0;
    sEnforceMaxWaitFlag = prop.getValue(
        "chunkServer.clientSM.enforceMaxWait",
        sEnforceMaxWaitFlag ? 1 : 0) != 0;
    sMaxReqSizeDiscard = prop.getValue(
        "chunkServer.clientSM.maxReqSizeDiscard",
        sMaxReqSizeDiscard);
    sMaxAppendRequestSize = prop.getValue(
        "chunkServer.clientSM.maxAppendRequestSize",
        sMaxAppendRequestSize);
    sMaxCmdHeaderReadAhead = prop.getValue(
        "chunkServer.clientSM.maxCmdHeaderReadAhead",
        sMaxCmdHeaderReadAhead);
}

ClientSM::ClientSM(
    const NetConnectionPtr& conn,
    ClientThread*           thread)
    : KfsCallbackObj(),
      ClientThreadListEntry(thread),
      BufferManager::Client(),
      SslFilterServerPsk(),
      mNetConnection(conn),
      mCurOp(0),
      mOps(),
      mReservations(),
      mPendingOps(),
      mPendingSubmitQueue(),
      mRemoteSyncers(),
      mPrevNumToWrite(0),
      mRecursionCnt(0),
      mDiscardByteCnt(0),
      mInstanceNum(sInstanceNum++),
      mIStream(),
      mWOStream(),
      mDevBufMgrClients(),
      mDevBufMgr(0),
      mGrantedFlag(false),
      mInFlightOpCount(0),
      mDevCliMgrAllocator(),
      mDataReceivedFlag(false),
      mContentReceivedFlag(false),
      mDelegationToken(),
      mSessionKey(),
      mHandleTerminateFlag(false)
{
    if (! mNetConnection) {
        die("ClientSM: null connection");
        return;
    }
    if (IsClientThread()) {
        SET_HANDLER(this, &ClientSM::DispatchRequest);
    } else {
        SET_HANDLER(this, &ClientSM::HandleRequest);
    }
    mNetConnection->SetMaxReadAhead(sMaxCmdHeaderReadAhead);
    mNetConnection->SetInactivityTimeout(gClientManager.GetIdleTimeoutSec());
    SetReceiveOp();
}

ClientSM::~ClientSM()
{
    if (mRecursionCnt != 0) {
        die("~ClientSM: invalid recursion count");
        return;
    }
    if (mInstanceNum <= 0 || sInstanceNum < mInstanceNum) {
        die("~ClientSM: invalid instance");
        return;
    }
    if (mInFlightOpCount != 0 ||
            ! mOps.empty() ||
            ! mPendingOps.empty() ||
            ! mPendingSubmitQueue.empty()) {
        die("~ClientSM: ops queue(s) are not empty");
        return;
    }
    delete mCurOp;
    mCurOp = 0;
    mDevBufMgrClients.First();
    const DevBufMsrEntry* entry;
    while ((entry = mDevBufMgrClients.Next())) {
        DevBufferManagerClient* const ent = entry->GetVal();
        assert(ent);
        mDevCliMgrAllocator.destroy(ent);
        mDevCliMgrAllocator.deallocate(ent, 1);
    }
    gClientManager.Remove(this);
    // The following is to catch double delete, and use after free.
    mRecursionCnt = -1;
}

///
/// Send out the response to the client request.  The response is
/// generated by MetaRequest as per the protocol.
/// @param[in] op The request for which we finished execution.
///
void
ClientSM::SendResponseSelf(KfsOp& op)
{
    assert(mNetConnection);

    const int64_t timespent = max(int64_t(0),
        (int64_t)TimeNow() * 1000000 - op.startTime);
    const bool    tooLong   = timespent > 5 * 1000000;
    CLIENT_SM_LOG_STREAM(
            (op.status >= 0 ||
                (op.op == CMD_SPC_RESERVE && op.status == -ENOSPC)) ?
            (tooLong ? MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelDEBUG) :
            MsgLogger::kLogLevelERROR) <<
        "-seq: "       << op.seq <<
        " status: "    << op.status <<
        " buffers: "   << GetByteCount() <<
        " " << op.Show() <<
        (op.statusMsg.empty() ? "" : " msg: ") << op.statusMsg <<
        (tooLong ? " RPC too long " : " took: ") <<
            timespent << " usec." <<
    KFS_LOG_EOM;

    op.Response(mWOStream.Set(mNetConnection->GetOutBuffer()));
    mWOStream.Reset();

    IOBuffer* iobuf = 0;
    int       len   = 0;
    op.ResponseContent(iobuf, len);
    mNetConnection->Write(iobuf, len);
    gClientManager.RequestDone(timespent, op);
}

///
/// Generic event handler.  Decode the event that occurred and
/// appropriately extract out the data and deal with the event.
/// @param[in] code: The type of event that occurred
/// @param[in] data: Data being passed in relative to the event that
/// occurred.
/// @retval 0 to indicate successful event handling; -1 otherwise.
///
int
ClientSM::HandleRequest(int code, void* data)
{
    if (mHandleTerminateFlag) {
        return HandleTerminate(code, data);
    }

    if (mRecursionCnt < 0 || ! mNetConnection) {
        die("ClientSM: invalid recursion count or null connection");
        return -1;
    }
    mRecursionCnt++;

    switch (code) {
    case EVENT_NET_READ: {
        if (! mDataReceivedFlag && ! mNetConnection->GetInBuffer().IsEmpty()) {
            mDataReceivedFlag = true;
            if (mNetConnection->GetFilter()) {
                mSessionKey.clear(); // Not needed with encrypted connection.
            }
        }
        if (IsWaiting() || (mDevBufMgr && ! mGrantedFlag)) {
            CLIENT_SM_LOG_STREAM_DEBUG <<
                "spurious read:"
                " cur op: "     << KfsOp::ShowOp(mCurOp) <<
                " buffers: "    << GetByteCount() <<
                " waiting for " << (mDevBufMgr ? "dev. " : "") <<
                " io buffers "  <<
            KFS_LOG_EOM;
            break;
        }
        mGrantedFlag = false;
        // We read something from the network.  Run the RPC that
        // came in.
        int       cmdLen = 0;
        bool      gotCmd = false;
        IOBuffer& iobuf  = mNetConnection->GetInBuffer();
        assert(&iobuf == data);
        while ((mCurOp || GetReceivedOp() || IsMsgAvail(&iobuf, &cmdLen)) &&
                (gotCmd = HandleClientCmd(iobuf, cmdLen))) {
            cmdLen = 0;
            gotCmd = false;
        }
        if (! mCurOp) {
            int hdrsz;
            if (cmdLen > 0 && ! gotCmd) {
                if (mNetConnection->IsGood()) {
                    CLIENT_SM_LOG_STREAM_ERROR <<
                        " failed to parse request, closing connection;"
                        " header size: "    << cmdLen <<
                        " read available: " << iobuf.BytesConsumable() <<
                    KFS_LOG_EOM;
                    gClientManager.BadRequest();
                }
            } else if ((hdrsz = iobuf.BytesConsumable()) > MAX_RPC_HEADER_LEN) {
                CLIENT_SM_LOG_STREAM_ERROR <<
                    " exceeded max request header size: " << hdrsz <<
                    " limit: " << MAX_RPC_HEADER_LEN <<
                    ", closing connection" <<
                KFS_LOG_EOM;
                gClientManager.BadRequestHeader();
            } else {
                break;
            }
            iobuf.Clear();
            mNetConnection->Close();
        }
        break;
    }

    case EVENT_NET_WROTE: {
        const int rem = mNetConnection->GetNumBytesToWrite();
        GetBufferManager().Put(*this, mPrevNumToWrite - rem);
        mPrevNumToWrite = rem;
        break;
    }

    case EVENT_CMD_DONE: {
        if (! data || mInFlightOpCount <= 0) {
            die("invalid null op completion");
            return -1;
        }
        KfsOp* op = reinterpret_cast<KfsOp*>(data);
        gChunkServer.OpFinished();
        op->done = true;
        if (sTraceRequestResponseFlag) {
            IOBuffer::OStream os;
            op->Response(os);
            IOBuffer::IStream is(os);
            string line;
            while (getline(is, line)) {
                CLIENT_SM_LOG_STREAM_DEBUG <<
                    "response: " << line <<
                KFS_LOG_EOM;
            }
        }
        if (! IsDependingOpType(*op)) {
            SendResponse(*op);
            OpFinished(op);
            delete op;
            op = 0;
            break;
        }
        // "Depending" op finished execution. Send response back in FIFO
        while (! mOps.empty()) {
            KfsOp* const qop = mOps.front();
            if (! qop->done) {
                if (! op) {
                    break;
                }
                CLIENT_SM_LOG_STREAM_DEBUG <<
                    "previous op still pending: " <<
                    qop->Show() << "; deferring reply to: " <<
                    op->Show() <<
                KFS_LOG_EOM;
                break;
            }
            if (qop == op) {
                op = 0;
            }
            SendResponse(*qop);
            mOps.pop_front();
            OpFinished(qop);
            delete qop;
        }
        if (op) {
            // Waiting for other op. Disk io done -- put device buffers.
            PutAndResetDevBufferManager(*op, op->bufferBytes.mCount);
        }
        break;
    }

    case EVENT_NET_ERROR: {
        NetConnection::Filter* filter;
        if (mNetConnection->IsGood() &&
                (filter = mNetConnection->GetFilter()) &&
                filter->IsShutdownReceived()) {
            // Do not allow to shutdown filter with ops or data in flight.
            if (! mDataReceivedFlag &&
                    mInFlightOpCount <= 0 &&
                    mOps.empty() &&
                    mNetConnection->GetInBuffer().IsEmpty() &&
                    mNetConnection->GetOutBuffer().IsEmpty()) {
                // Ssl shutdown from the other side.
                if (mNetConnection->Shutdown() == 0) {
                    CLIENT_SM_LOG_STREAM_INFO <<
                        "filter shutdown"
                        " delegation: " << mDelegationToken.Show() <<
                        " filter: "     << (void*)mNetConnection->GetFilter() <<
                    KFS_LOG_EOM;
                    break;
                }
            } else if (0 < mNetConnection->GetNumBytesToRead() ||
                     0 < mNetConnection->GetNumBytesToWrite()) {
                CLIENT_SM_LOG_STREAM_ERROR <<
                    "invalid filter (ssl) shutdown: "
                    " error: "      << mNetConnection->GetErrorMsg() <<
                    " delegation: " << mDelegationToken.Show() <<
                    " pending"
                    " read: "       << mNetConnection->GetNumBytesToRead() <<
                    " write: "      << mNetConnection->GetNumBytesToWrite() <<
                    " ops: "        << (mOps.empty() ? "pending" : "none") <<
                KFS_LOG_EOM;
            }
        }
    }
    // Fall through.
    case EVENT_INACTIVITY_TIMEOUT:
        CLIENT_SM_LOG_STREAM_DEBUG <<
            "closing connection"
            " due to " << (code == EVENT_INACTIVITY_TIMEOUT ?
                "inactivity timeout" : "network error") <<
            " error: "        << mNetConnection->GetErrorMsg() <<
            " pending read: " << mNetConnection->GetNumBytesToRead() <<
            " write: "        << mNetConnection->GetNumBytesToWrite() <<
            " wants read: "   << mNetConnection->IsReadReady() <<
        KFS_LOG_EOM;
        mNetConnection->Close();
        assert(mNetConnection->GetNumBytesToWrite() <= 0);
        if (0 < mPrevNumToWrite) {
            GetBufferManager().Put(*this, mPrevNumToWrite);
            mPrevNumToWrite = 0;
        }
        if (mCurOp) {
            if (mDevBufMgr) {
                GetDevBufMgrClient(mDevBufMgr)->CancelRequest();
            } else {
                PutAndResetDevBufferManager(*mCurOp, GetWaitingForByteCount());
                CancelRequest();
            }
            delete mCurOp;
            mCurOp = 0;
        }
        break;

    default:
        die("unexpected event");
        break;
    }

    assert(mRecursionCnt > 0);
    if (mRecursionCnt == 1) {
        if (! IsClientThread()) {
            mNetConnection->StartFlush();
        }
        if (mNetConnection->IsGood()) {
            // Enforce 5 min timeout if connection has pending read and write.
            mNetConnection->SetInactivityTimeout(
                (mNetConnection->HasPendingRead() ||
                    mNetConnection->IsWriteReady()) ?
                gClientManager.GetIoTimeoutSec() :
                gClientManager.GetIdleTimeoutSec());
            if (IsWaiting() || mDevBufMgr) {
                mNetConnection->SetMaxReadAhead(0);
                ReceiveClear();
            } else if (! mCurOp || ! mNetConnection->IsReadReady()) {
                mNetConnection->SetMaxReadAhead(sMaxCmdHeaderReadAhead);
                SetReceiveOp();
            }
        } else {
            // get rid of the connection to all the peers in daisy chain;
            // if there were any outstanding ops, they will all come back
            // to this method as EVENT_CMD_DONE and we clean them up above.
            mRemoteSyncers.ReleaseAllServers();
            ReleaseChunkSpaceReservations();
            // if there are any disk ops, wait for the ops to finish
            mNetConnection->SetOwningKfsCallbackObj(0);
            mHandleTerminateFlag = true;
            assert(0 <= mInFlightOpCount);
            if (mInFlightOpCount <= 0) {
                mRecursionCnt--;
                delete this;
                return 1;
            }
        }
    }
    mRecursionCnt--;
    return 0;
}

///
/// Termination handler.  For the client state machine, we could have
/// ops queued at the logger.  So, for cleanup wait for all the
/// outstanding ops to finish and then delete this.  In this state,
/// the only event that gets raised is that an op finished; anything
/// else is bad.
///
int
ClientSM::HandleTerminate(int code, void* data)
{
    if (mRecursionCnt < 0 || ! mNetConnection) {
        die("ClientSM terminate: invalid recursion count or null connection");
        return -1;
    }
    mRecursionCnt++;

    switch (code) {

    case EVENT_CMD_DONE: {
        if (! data) {
            die("ClientSM terminate: invalid op");
            return -1;
        }
        if (mInFlightOpCount <= 0) {
            die("ClientSM terminate: spurious op completion");
            return -1;
        }
        KfsOp* op = reinterpret_cast<KfsOp*>(data);
        gChunkServer.OpFinished();
        // An op finished execution.
        op->done = true;
        if (! IsDependingOpType(*op)) {
            PutAndResetDevBufferManager(*op, op->bufferBytes.mCount);
            GetBufferManager().Put(*this, op->bufferBytes.mCount);
            OpFinished(op);
            delete op;
            break;
        }
        while (! mOps.empty()) {
            op = mOps.front();
            if (! op->done) {
                break;
            }
            PutAndResetDevBufferManager(*op, op->bufferBytes.mCount);
            GetBufferManager().Put(*this, op->bufferBytes.mCount);
            mOps.pop_front();
            OpFinished(op);
            delete op;
        }
        break;
    }

    default:
        die("unexpected event");
        break;
    }

    assert(0 < mRecursionCnt);
    mRecursionCnt--;
    if (mRecursionCnt <= 0 && mInFlightOpCount <= 0) {
        delete this;
        return 1;
    }
    return 0;
}

inline static BufferManager::ByteCount
IoRequestBytes(BufferManager::ByteCount numBytes, bool forwardFlag = false)
{
    BufferManager::ByteCount ret = IOBufferData::GetDefaultBufferSize();
    if (forwardFlag) {
        // ret += (numBytes + ret - 1) / ret * ret;
    }
    if (numBytes > 0) {
        ret += ((numBytes + CHECKSUM_BLOCKSIZE - 1) /
            CHECKSUM_BLOCKSIZE * CHECKSUM_BLOCKSIZE);
    }
    return ret;
}

inline BufferManager::ByteCount
GetQuota(const BufferManager& bufMgr, const BufferManager* devBufMgr)
{
    if (devBufMgr) {
        return min(bufMgr.GetMaxClientQuota(), devBufMgr->GetMaxClientQuota());
    }
    return bufMgr.GetMaxClientQuota();
}

bool
ClientSM::Discard(IOBuffer& iobuf)
{
    if (mDiscardByteCnt <= 0) {
        return true;
    }
    const int discardedCnt = iobuf.Consume(mDiscardByteCnt);
    gClientManager.Discarded(discardedCnt);
    mDiscardByteCnt -= discardedCnt;
    if (mDiscardByteCnt > 0) {
        mNetConnection->SetMaxReadAhead(
            (int)min((ByteCount)mDiscardByteCnt, max(ByteCount(1),
                min(GetBufferManager().GetRemainingByteCount(),
                ByteCount(8) * IOBufferData::GetDefaultBufferSize())))
        );
        return false;
    }
    return true;
}

bool
ClientSM::GetWriteOp(KfsOp& op, int align, int numBytes,
    IOBuffer& iobuf, IOBuffer& ioOpBuf, bool forwardFlag)
{
    const int nAvail = iobuf.BytesConsumable();
    if (! mCurOp && op.status < 0) {
        mCurOp          = &op;
        mDiscardByteCnt = numBytes;
        mDevBufMgr      = 0;
    } else if (! mCurOp || mDevBufMgr) {
        mDevBufMgr = mCurOp ? 0 : FindDevBufferManager(op);
        Client* const   mgrCli      = GetDevBufMgrClient(mDevBufMgr);
        const ByteCount bufferBytes = IoRequestBytes(numBytes, forwardFlag);
        BufferManager&  bufMgr      = GetBufferManager();
        ByteCount       quota       = -1;
        if (! mCurOp && (numBytes < 0 ||
                (op.op == CMD_RECORD_APPEND ? sMaxAppendRequestSize :
                    gChunkManager.GetMaxIORequestSize()) < (size_t)numBytes ||
                (quota = GetQuota(bufMgr, mDevBufMgr)) < bufferBytes)) {
            CLIENT_SM_LOG_STREAM_ERROR <<
                "invalid write request size"
                " seq: "          << op.seq <<
                " size: "         << numBytes <<
                " max allowed: "  <<
                    (op.op == CMD_RECORD_APPEND ? sMaxAppendRequestSize :
                        gChunkManager.GetMaxIORequestSize()) <<
                " buffer bytes: " << bufferBytes <<
                " max allowed: "  << quota <<
                " buffers: "      << GetByteCount() <<
                " closing connection"
                " op: "           << op.Show() <<
            KFS_LOG_EOM;
            delete &op;
            return false;
        }
        if (! mCurOp && nAvail <= numBytes) {
            // Move write data to the start of the buffers, to make it
            // aligned. Normally only one buffer will be created.
            const int off(align % IOBufferData::GetDefaultBufferSize());
            if (off > 0) {
                IOBuffer buf;
                buf.ReplaceKeepBuffersFull(&iobuf, off, nAvail);
                iobuf.Move(&buf);
                iobuf.Consume(off);
            } else {
                iobuf.MakeBuffersFull();
            }
        }
        mDiscardByteCnt = 0;
        mCurOp          = &op;
        if (mDevBufMgr && mDevBufMgr->GetForDiskIo(*mgrCli, bufferBytes)) {
            mDevBufMgr = 0;
        }
        if (mDevBufMgr || ! bufMgr.GetForDiskIo(*this, bufferBytes)) {
            const BufferManager& mgr      = mDevBufMgr ? *mDevBufMgr : bufMgr;
            const bool           failFlag =
                numBytes <= sMaxReqSizeDiscard + nAvail &&
                FailIfExceedsWait(bufMgr, 0);
            CLIENT_SM_LOG_STREAM_DEBUG <<
                " request for: " << bufferBytes << " bytes denied" <<
                (&mgr == &bufMgr ? "" : " by dev.") <<
                " seq: "   << op.seq <<
                " cur: "   << GetByteCount() <<
                " total: " << mgr.GetTotalByteCount() <<
                " used: "  << mgr.GetUsedByteCount() <<
                " bufs: "  << mgr.GetFreeBufferCount() <<
                (failFlag ? "exceeds max wait" : " waiting for buffers") <<
                " op: " << op.Show() <<
            KFS_LOG_EOM;
            if (failFlag) {
                mDiscardByteCnt = numBytes;
            } else {
                return false;
            }
        }
    }
    if (mDiscardByteCnt > 0) {
        if (! Discard(iobuf)) {
            return false;
        }
        if (op.status >= 0) {
            op.status = -ESERVERBUSY;
        }
        mDiscardByteCnt = 0;
        mCurOp          = 0;
        return true;
    }
    if (nAvail < numBytes) {
        mNetConnection->SetMaxReadAhead(numBytes - nAvail);
        SetReceiveContent(numBytes, op.op == CMD_WRITE_PREPARE);
        // we couldn't process the command...so, wait
        return false;
    }
    ioOpBuf.Clear();
    if (nAvail != numBytes) {
        assert(nAvail > numBytes);
        const int off(align % IOBufferData::GetDefaultBufferSize());
        ioOpBuf.ReplaceKeepBuffersFull(&iobuf, off, numBytes);
        if (off > 0) {
            ioOpBuf.Consume(off);
        }
    } else {
        ioOpBuf.Move(&iobuf);
    }
    mCurOp = 0;
    return true;
}

bool
ClientSM::FailIfExceedsWait(
    BufferManager&         bufMgr,
    BufferManager::Client* mgrCli)
{
    if (! sEnforceMaxWaitFlag || ! mCurOp || mCurOp->maxWaitMillisec <= 0) {
        return false;
    }
    const int64_t maxWait        = mCurOp->maxWaitMillisec * 1000;
    const bool    devMgrWaitFlag = mDevBufMgr != 0 && mgrCli != 0;
    const int64_t curWait        = bufMgr.GetWaitingAvgUsecs() +
        (devMgrWaitFlag ? mDevBufMgr->GetWaitingAvgUsecs() : int64_t(0));
    if (curWait <= maxWait ||
            microseconds() + curWait < mCurOp->startTime + maxWait) {
        return false;
    }
    CLIENT_SM_LOG_STREAM_DEBUG <<
        " exceeded wait:"
        " current: " << curWait <<
        " max: "     << maxWait <<
        " op: "      << mCurOp->Show() <<
    KFS_LOG_EOM;
    mCurOp->status    = -ESERVERBUSY;
    mCurOp->statusMsg = "exceeds max wait";
    if(devMgrWaitFlag) {
        mgrCli->CancelRequest();
        mDevBufMgr = 0;
    } else {
        PutAndResetDevBufferManager(*mCurOp, GetWaitingForByteCount());
        CancelRequest();
    }
    gClientManager.WaitTimeExceeded();
    return true;
}

///
/// We have a command in a buffer.  It is possible that we don't have
/// everything we need to execute it (for example, for a write we may
/// not have received all the data the client promised).  So, parse
/// out the command and if we have everything execute it.
///
bool
ClientSM::HandleClientCmd(IOBuffer& iobuf, int inCmdLen)
{
    KfsOp* op     = mCurOp;
    int    cmdLen = inCmdLen;
    assert(op ? cmdLen == 0 : (cmdLen > 0 || GetReceivedOp()));
    if (! op) {
        if ((op = GetReceivedOp())) {
            cmdLen = GetReceivedHeaderLen();
            ReceiveClear();
        }
        if (sTraceRequestResponseFlag) {
            istream& is = mIStream.Set(iobuf, cmdLen);
            string line;
            while (getline(is, line)) {
                CLIENT_SM_LOG_STREAM_DEBUG <<
                    "request: " << line <<
                KFS_LOG_EOM;
            }
            mIStream.Reset();
        }
        mContentReceivedFlag = false;
        if (! op && ParseClientCommand(iobuf, cmdLen, &op) != 0) {
            assert(! op);
            istream& is = mIStream.Set(iobuf, cmdLen);
            string line;
            int    maxLines = 64;
            while (--maxLines >= 0 && getline(is, line)) {
                CLIENT_SM_LOG_STREAM_ERROR <<
                    "invalid request: " << line <<
                KFS_LOG_EOM;
            }
            mIStream.Reset();
            iobuf.Consume(cmdLen);
            // got a bogus command
            return false;
        }
        CLIENT_SM_LOG_STREAM_DEBUG <<
            "+seq: " << op->seq << " " << op->Show() <<
        KFS_LOG_EOM;
        if (IsAccessEnforced() &&
                mDelegationToken.GetIssuedTime() +
                    mDelegationToken.GetValidForSec() <
                TimeNow()) {
            CLIENT_SM_LOG_STREAM_ERROR <<
                "delegation token has expired, closing connection " <<
                " request: " << op->seq <<
                " " << op->Show() <<
            KFS_LOG_EOM;
            delete op;
            iobuf.Consume(cmdLen);
            mNetConnection->Close();
            return false;
        }
        op->CheckAccess(*this);
    }
    iobuf.Consume(cmdLen);

    // Content length here might be just an initial part of the payload length,
    // the part that could be too large to fit into the normal rpc "header".
    // Append and write prepare ops has such format where the initial part of
    // the "content" is used to pass access tokens down to the synchronous
    // replication chain.
    const int contentLength = mContentReceivedFlag ? 0 : op->GetContentLength();
    if (0 < contentLength) {
        if (! mCurOp) {
            mCurOp = op;
            if (op->status < 0) {
                mDiscardByteCnt = contentLength;
            } else if (iobuf.BytesConsumable() < contentLength) {
                const ByteCount bufferBytes = contentLength;
                BufferManager&  bufMgr      = GetBufferManager();
                if (! bufMgr.Get(*this, bufferBytes)) {
                    const bool exceedsWaitFlag = FailIfExceedsWait(bufMgr, 0);
                    CLIENT_SM_LOG_STREAM_DEBUG <<
                        "request for: " << bufferBytes << " bytes denied" <<
                        " cur: "   << GetByteCount() <<
                        " total: " << bufMgr.GetTotalByteCount() <<
                        " used: "  << bufMgr.GetUsedByteCount() <<
                        " bufs: "  << bufMgr.GetFreeBufferCount() <<
                        " op: "    << op->Show() <<
                        (exceedsWaitFlag ?
                            "exceeds max wait" : " waiting for buffers") <<
                    KFS_LOG_EOM;
                    if (! exceedsWaitFlag) {
                        return false;
                    }
                }
            }
        }
        if (0 < mDiscardByteCnt) {
            if (! Discard(iobuf)) {
                return false;
            }
        } else if (iobuf.BytesConsumable() < contentLength) {
            mNetConnection->SetMaxReadAhead(
                iobuf.BytesConsumable() - contentLength);
            const bool kComputeChecksumFlag = false;
            SetReceiveContent(contentLength, kComputeChecksumFlag);
            return false;
        }
        mCurOp = 0;
        ReceiveClear();
        if (0 < contentLength && 0 <= op->status) {
            if (! op->ParseContent(mIStream.Set(iobuf, contentLength))) {
                if (0 < op->status) {
                    op->status    = -EINVAL;
                    op->statusMsg = "content parse error";
                }
            }
            mIStream.Reset();
            iobuf.Consume(contentLength);
        }
    }
    mContentReceivedFlag = true;

    ByteCount bufferBytes = -1;
    if (op->op == CMD_WRITE_PREPARE) {
        WritePrepareOp* const wop = static_cast<WritePrepareOp*>(op);
        const bool kForwardFlag = false; // The forward always share the buffers.
        if (! GetWriteOp(*wop, wop->offset, (int)wop->numBytes,
                iobuf, wop->dataBuf, kForwardFlag)) {
            return false;
        }
        bufferBytes = 0 <= op->status ? IoRequestBytes(wop->numBytes) : 0;
        if (GetReceiveByteCount() == (int)wop->numBytes) {
            wop->receivedChecksum = GetChecksum();
            wop->blocksChecksums.swap(GetBlockChecksums());
        }
        ReceiveClear();
    } else if (op->op == CMD_RECORD_APPEND) {
        RecordAppendOp* const waop = static_cast<RecordAppendOp*>(op);
        bool       forwardFlag = false;
        const int  align       = mCurOp ? 0 :
            gAtomicRecordAppendManager.GetAlignmentAndFwdFlag(
                waop->chunkId, forwardFlag);
        if (! GetWriteOp(
                *waop,
                align,
                (int)waop->numBytes,
                iobuf,
                waop->dataBuf,
                forwardFlag
            )) {
            return false;
        }
        bufferBytes = 0 <= op->status ? IoRequestBytes(waop->numBytes) : 0;
        ReceiveClear();
    }
    CLIENT_SM_LOG_STREAM_DEBUG <<
        "got:"
        " seq: "    << op->seq <<
        " status: " << op->status <<
        (op->statusMsg.empty() ? string() : " " + op->statusMsg) <<
        " " << op->Show() <<
    KFS_LOG_EOM;

    bool         submitResponseFlag = op->status < 0;
    kfsChunkId_t chunkId  = 0;
    int64_t      reqBytes = 0;
    if (! submitResponseFlag &&
            bufferBytes < 0 && op->IsChunkReadOp(reqBytes, chunkId)) {
        bufferBytes = reqBytes + IoRequestBytes(0); // 1 buffer for reply header
        if (! mCurOp || mDevBufMgr) {
            mDevBufMgr = mCurOp ? 0 : FindDevBufferManager(*op);
            Client* const  mgrCli = GetDevBufMgrClient(mDevBufMgr);
            BufferManager& bufMgr = GetBufferManager();
            ByteCount      quota  = -1;
            if (! mCurOp && (reqBytes < 0 ||
                    gChunkManager.GetMaxIORequestSize() < (size_t)reqBytes ||
                    (quota = GetQuota(bufMgr, mDevBufMgr)) < bufferBytes)) {
                CLIENT_SM_LOG_STREAM_ERROR <<
                    "invalid read request size"
                    " seq: "          << op->seq <<
                    " size: "         << reqBytes <<
                    " max allowed: "  << gChunkManager.GetMaxIORequestSize() <<
                    " buffer bytes: " << bufferBytes <<
                    " max allowed: "  << quota <<
                    " buffers: "      << GetByteCount() <<
                    " op : "          << op->Show() <<
                KFS_LOG_EOM;
                op->status         = -ENOMEM;
                op->statusMsg      = "exceeds max request size";
                submitResponseFlag = true;
            } else {
                if (mDevBufMgr &&
                        mDevBufMgr->GetForDiskIo(*mgrCli, bufferBytes)) {
                    mDevBufMgr = 0;
                }
                if (mDevBufMgr || ! bufMgr.GetForDiskIo(*this, bufferBytes)) {
                    mCurOp = op;
                    const BufferManager& mgr =
                        mDevBufMgr ? *mDevBufMgr : bufMgr;
                    submitResponseFlag = FailIfExceedsWait(bufMgr, mgrCli);
                    CLIENT_SM_LOG_STREAM_DEBUG <<
                        "request for: " << bufferBytes << " bytes denied" <<
                        (&mgr == &bufMgr ? "" : " by dev.") <<
                        " seq: "   << op->seq <<
                        " cur: "   << GetByteCount() <<
                        " total: " << mgr.GetTotalByteCount() <<
                        " used: "  << mgr.GetUsedByteCount() <<
                        " bufs: "  << mgr.GetFreeBufferCount() <<
                        (submitResponseFlag ?
                            "exceeds max wait" : " waiting for buffers") <<
                        " op: "    << op->Show() <<
                    KFS_LOG_EOM;
                    if (! submitResponseFlag) {
                        return false;
                    }
                    bufferBytes = 0; // Buffer accounting is already done.
                }
            }
        }
        mCurOp = 0;
    }

    if (bufferBytes < 0 && ! submitResponseFlag) {
        assert(
            op->op != CMD_WRITE_PREPARE &&
            op->op != CMD_RECORD_APPEND &&
            op->op != CMD_READ
        );
        // This is needed to account for large number of small responses to
        // prevent out of buffers in the case where the client queues requests
        // but doesn't read replies.
        // To speedup append status recovery give record append status inquiry a
        // "free pass", if there are no ops pending and connection input and
        // output buffers are empty. This should be the normal case as clients
        // create new connection to do status inquiry. There is virtually
        // no danger of running out of buffers: the reply size is small enough
        // to fit into the socket buffer, and free up the io buffer immediately.
        // Since the op is synchronous and doesn't involve disk io or forwarding
        // the same io buffer that was just freed by IOBuffer::Consume() the
        // the above should be re-used for send, and freed immediately as the
        // kernel's socket buffer is expected to have at least around 1K
        // available.
        bufferBytes = (op->op == CMD_GET_RECORD_APPEND_STATUS &&
                ! mCurOp &&
                mInFlightOpCount <= 0 &&
                GetByteCount() <= 0 &&
                ! IsWaiting() &&
                mNetConnection->GetOutBuffer().IsEmpty() &&
                mNetConnection->GetInBuffer().IsEmpty()
            ) ? ByteCount(0) : IoRequestBytes(0);
        if (! mCurOp) {
            BufferManager& bufMgr = GetBufferManager();
            if (! bufMgr.Get(*this, bufferBytes)) {
                mCurOp = op;
                submitResponseFlag = FailIfExceedsWait(bufMgr, 0);
                CLIENT_SM_LOG_STREAM_DEBUG <<
                    "request for: " << bufferBytes << " bytes denied" <<
                    " cur: "   << GetByteCount() <<
                    " total: " << bufMgr.GetTotalByteCount() <<
                    " used: "  << bufMgr.GetUsedByteCount() <<
                    " bufs: "  << bufMgr.GetFreeBufferCount() <<
                    " op: "    << op->Show() <<
                    (submitResponseFlag ?
                        "exceeds max wait" : " waiting for buffers") <<
                KFS_LOG_EOM;
                if (! submitResponseFlag) {
                    return false;
                }
            }
        }
        mCurOp = 0;
    }

    op->clientSMFlag       = true;
    op->clnt               = this;
    op->bufferBytes.mCount = bufferBytes;
    if (op->op == CMD_WRITE_SYNC) {
        // make the write sync depend on a previous write
        if (! mOps.empty()) {
            mPendingOps.push_back(OpPair(mOps.back(), op));
            CLIENT_SM_LOG_STREAM_DEBUG <<
                "keeping write-sync seq:" << op->seq <<
                " pending and depends on seq: " << mOps.back()->seq <<
            KFS_LOG_EOM;
            return true;
        }
        CLIENT_SM_LOG_STREAM_DEBUG <<
            "write-sync is being pushed down; no writes left, "
            << mInFlightOpCount << " ops left" <<
        KFS_LOG_EOM;
    }
    if (IsDependingOpType(*op)) {
        mOps.push_back(op);
    }
    mInFlightOpCount++;
    gChunkServer.OpInserted();
    if (submitResponseFlag) {
        HandleRequest(EVENT_CMD_DONE, op);
    } else {
        SubmitOp(op);
    }
    return true;
}

void
ClientSM::OpFinished(KfsOp* doneOp)
{
    assert(0 < mInFlightOpCount && doneOp);
    mInFlightOpCount--;
    if (! IsDependingOpType(*doneOp)) {
        return;
    }
    // Multiple ops could be waiting for a single op to finish.
    //
    // Do not run pending submit queue here, if it is not empty.
    // If pending submit is not empty here, then this is recursive call. Just
    // add the op to the pending submit queue and let the caller run the queue.
    // This is need to send responses in the request order, and to limit the
    // recursion depth.
    const bool runPendingSubmitQueueFlag = mPendingSubmitQueue.empty();
    while (! mPendingOps.empty()) {
        const OpPair& p = mPendingOps.front();
        if (p.op != doneOp) {
            break;
        }
        CLIENT_SM_LOG_STREAM_DEBUG <<
            "submitting write-sync (" << p.dependentOp->seq <<
            ") since " << p.op->seq << " finished" <<
        KFS_LOG_EOM;
        mPendingSubmitQueue.splice(mPendingSubmitQueue.end(),
            mPendingOps, mPendingOps.begin());
    }
    if (! runPendingSubmitQueueFlag) {
        return;
    }
    while (! mPendingSubmitQueue.empty()) {
        KfsOp* const op = mPendingSubmitQueue.front().dependentOp;
        mPendingSubmitQueue.pop_front();
        gChunkServer.OpInserted();
        mInFlightOpCount++;
        SubmitOp(op);
    }
}

void
ClientSM::ReleaseChunkSpaceReservations()
{
    mReservations.First();
    const SpaceResEntry* entry;
    while ((entry = mReservations.Next())) {
        gAtomicRecordAppendManager.ChunkSpaceRelease(
            entry->GetKey().chunkId,
            entry->GetKey().transactionId,
            entry->GetVal()
        );
    }
    mReservations.Clear();
}

RemoteSyncSMPtr
ClientSM::FindServer(
    const ServerLocation& location,
    bool                  connectFlag,
    const char*           sessionTokenPtr,
    int                   sessionTokenLen,
    const char*           sessionKeyPtr,
    int                   sessionKeyLen,
    bool                  writeMasterFlag,
    bool                  shutdownSslFlag,
    int&                  err,
    string&               errMsg)
{
    return RemoteSyncSM::FindServer(
        mRemoteSyncers,
        location,
        connectFlag,
        sessionTokenPtr,
        sessionTokenLen,
        sessionKeyPtr,
        sessionKeyLen,
        writeMasterFlag,
        writeMasterFlag ?
            shutdownSslFlag :
            mNetConnection && ! mNetConnection->GetFilter(),
        err,
        errMsg
    );
}

void
ClientSM::GrantedSelf(ClientSM::ByteCount byteCount, bool devBufManagerFlag)
{
    CLIENT_SM_LOG_STREAM_DEBUG <<
        "granted: " << (devBufManagerFlag ? "by dev. " : "") <<
        byteCount <<
        " seq: " << (mCurOp ? mCurOp->seq : -1) <<
        " op: " << KfsOp::ShowOp(mCurOp) <<
        " dev. mgr: " << (const void*)mDevBufMgr <<
    KFS_LOG_EOM;
    assert(devBufManagerFlag == (mDevBufMgr != 0));
    if (IsClientThread()) {
        DispatchGranted(*this);
    } else {
        HandleGranted();
    }
}

int
ClientSM::HandleGranted()
{
    if (! mNetConnection->IsGood()) {
        return 0;
    }
    mGrantedFlag = true;
    return HandleRequest(EVENT_NET_READ, &(mNetConnection->GetInBuffer()));
}

/* virtual */ unsigned long
ClientSM::GetPsk(
    const char*    inIdentityPtr,
    unsigned char* inPskBufferPtr,
    unsigned int   inPskBufferLen,
    string&        outAuthName)
{
    outAuthName.clear();
    string theErrMsg;
    const int theKeyLen = mDelegationToken.Process(
        inIdentityPtr,
        strlen(inIdentityPtr),
        (int64_t)TimeNow(),
        gChunkManager.GetCryptoKeys(),
        reinterpret_cast<char*>(inPskBufferPtr),
        (int)min(inPskBufferLen, 0x7FFFFu),
        &theErrMsg
    );
    if (theKeyLen > 0) {
        CLIENT_SM_LOG_STREAM_DEBUG <<
            "authentication succeeded:" <<
            " delegation: " << mDelegationToken.Show() <<
        KFS_LOG_EOM;
        mSessionKey.assign(
            reinterpret_cast<const char*>(inPskBufferPtr), theKeyLen);
        return theKeyLen;
    }
    CLIENT_SM_LOG_STREAM_ERROR <<
        "authentication failure: " << theErrMsg <<
        " delegation: "            << mDelegationToken.Show() <<
    KFS_LOG_EOM;
    mDelegationToken.Clear();
    return 0;
}

inline bool
ClientSM::IsAccessEnforced() const
{
    // Validate access if authentication is configured.
    // Presently with authentication configured ssl handshake must complete
    // successfully in order to get here, as handshake failure will result
    // in connection error.
    return (0 < mDelegationToken.GetValidForSec());
}

bool
ClientSM::CheckAccess(KfsOp& op)
{
    if (! IsAccessEnforced()) {
        return true;
    }
    if (mDelegationToken.GetValidForSec() <= 0) {
        op.statusMsg = "connection is not authenticated";
        op.status    = -EPERM;
        return false;
    }
    if (! mNetConnection || ! mNetConnection->GetFilter()) {
        op.statusMsg = "connection is not secure";
        op.status    = -EPERM;
        return false;
    }
    if (mDelegationToken.GetUid() != kKfsUserRoot) {
        op.statusMsg = "authenticated user is not root";
        op.status    = -EPERM;
        return false;
    }
    return true;
}

bool
ClientSM::CheckAccess(KfsClientChunkOp& op)
{
    if (! IsAccessEnforced()) {
        op.hasChunkAccessTokenFlag   = false;
        op.chunkAccessTokenValidFlag = false;
        return true;
    }

    if (! op.hasChunkAccessTokenFlag) {
        op.statusMsg = "chunk access: no chunk access token";
        op.status    = -EPERM;
        return false;
    }
    if (! op.chunkAccessTokenValidFlag) {
        if (op.statusMsg.empty()) {
            op.statusMsg = "chunk access: chunk access token is not valid";
        } else {
            op.statusMsg = "chunk access: " + op.statusMsg;
        }
        op.status = -EPERM;
        return false;
    }
    if (mDelegationToken.GetValidForSec() <= 0) {
        op.statusMsg = "chunk access: connection is not authenticated";
        op.status    = -EPERM;
        return false;
    }
    if (((op.chunkAccessFlags ^ mDelegationToken.GetFlags()) &
            DelegationToken::kChunkServerFlag) != 0) {
        op.statusMsg = "chunk access: chunk server flag mismatch";
        op.status    = -EPERM;
        return false;
    }
    if (op.chunkAccessUid != mDelegationToken.GetUid()) {
        op.statusMsg = "chunk access: chunk access uid mismatch";
        op.status    = -EPERM;
        return false;
    }
    if ((op.chunkAccessFlags & ChunkAccessToken::kAllowClearTextFlag) == 0 &&
            (! mNetConnection || ! mNetConnection->GetFilter())) {
        op.statusMsg = "chunk access: no clear text connection allowed";
        op.status    = -EPERM;
        return false;
    }
    switch (op.op) {
        case CMD_READ:
        case CMD_GET_CHUNK_METADATA:
            if ((op.chunkAccessFlags & ChunkAccessToken::kAllowReadFlag) == 0) {
                op.statusMsg = "chunk access: no read access";
                op.status    = -EPERM;
                return false;
            }
            break;
        case CMD_SIZE:
        case CMD_CLOSE:
            if ((op.chunkAccessFlags & (ChunkAccessToken::kAllowReadFlag |
                    ChunkAccessToken::kAllowWriteFlag)) == 0) {
                op.statusMsg = "chunk access: no rw access";
                op.status    = -EPERM;
                return false;
            }
            break;
        case CMD_GET_RECORD_APPEND_STATUS:
            if ((op.chunkAccessFlags &
                    (ChunkAccessToken::kAppendRecoveryFlag |
                    ChunkAccessToken::kAllowWriteFlag)) == 0) {
                op.statusMsg = "chunk access: invalid status recovery access";
                op.status    = -EPERM;
            }
            break;
        default:
            if ((op.chunkAccessFlags &
                    ChunkAccessToken::kAllowWriteFlag) == 0) {
                op.statusMsg = "chunk access: no write access";
                op.status    = -EPERM;
                return false;
            }
            break;
    }
    if ((op.chunkAccessFlags & ChunkAccessToken::kUsesLeaseIdFlag) != 0) {
        // Lease id isn't used yet.
        op.statusMsg = "chunk access: no lease id subject allowed";
        op.status    = -EPERM;
        return false;
    }
    if ((op.chunkAccessFlags & ChunkAccessToken::kUsesWriteIdFlag) != 0 &&
            ((op.chunkAccessFlags & ChunkAccessToken::kAllowReadFlag) != 0 ||
            (op.chunkAccessFlags & ChunkAccessToken::kAllowWriteFlag) == 0)) {
        // Write id only used with chunk write related rpcs.
        op.statusMsg = "chunk access: invalid write id subject access";
        op.status    = -EPERM;
        return false;
    }
    if ((op.chunkAccessFlags & ChunkAccessToken::kUsesWriteIdFlag) != 0 &&
            op.op != CMD_WRITE_PREPARE &&
            op.op != CMD_WRITE_SYNC    &&
            op.op != CMD_RECORD_APPEND &&
            op.op != CMD_SPC_RESERVE   &&
            op.op != CMD_SPC_RELEASE   &&
            op.op != CMD_GET_RECORD_APPEND_STATUS &&
            (op.op != CMD_CLOSE || (op.chunkAccessFlags &
                ChunkAccessToken::kAllowWriteFlag) == 0)) {
        op.statusMsg = "chunk access: no id subject allowed";
        op.status    = -EPERM;
        return false;
    }
    return true;
}

bool
ClientSM::CheckAccess(ChunkAccessRequestOp& op)
{
    KfsClientChunkOp& chunkOp = op;
    if (! CheckAccess(chunkOp)) {
        return false;
    }
    if (! op.createChunkAccessFlag && ! op.createChunkServerAccessFlag) {
        return true;
    }
    if (mDelegationToken.GetValidForSec() <= 0) {
        op.statusMsg = "invalid access request on non authenticated connection";
        op.status    = -EINVAL;
        return false;
    }
    if ((mDelegationToken.GetFlags() &
            DelegationToken::kChunkServerFlag) != 0) {
        op.statusMsg = "invalid access request from chunk server";
        op.status    = -EINVAL;
        return false;
    }
    return true;
}

}
