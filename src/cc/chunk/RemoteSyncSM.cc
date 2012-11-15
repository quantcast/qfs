//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/09/27
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

#include "RemoteSyncSM.h"
#include "utils.h"
#include "ChunkServer.h"
#include "kfsio/NetManager.h"
#include "kfsio/Globals.h"
#include "common/MsgLogger.h"
#include "common/Properties.h"
#include "common/kfserrno.h"

#include <cerrno>
#include <sstream>
#include <string>
#include <algorithm>
#include <utility>
#include <list>

namespace KFS
{

using std::for_each;
using std::list;
using std::istringstream;
using std::string;
using std::make_pair;

using namespace KFS::libkfsio;

const int kMaxCmdHeaderLength = 2 << 10;
bool RemoteSyncSM::sTraceRequestResponse = false;
int  RemoteSyncSM::sOpResponseTimeoutSec = 5 * 60; // 5 min op response timeout

static kfsSeq_t
NextSeq()
{
    static kfsSeq_t sSeqno = GetRandomSeq();
    return sSeqno++;
}

inline void
RemoteSyncSM::UpdateRecvTimeout()
{
    if (sOpResponseTimeoutSec < 0 || ! mNetConnection) {
        return;
    }
    const time_t now = globalNetManager().Now();
    const time_t end = mLastRecvTime + sOpResponseTimeoutSec;
    mNetConnection->SetInactivityTimeout(end > now ? end - now : 0);
}

// State machine for communication with other chunk servers.
RemoteSyncSM::RemoteSyncSM(const ServerLocation &location)
    : KfsCallbackObj(),
      mNetConnection(),
      mLocation(location),
      mSeqnum(NextSeq()),
      mDispatchedOps(),
      mReplySeqNum(-1),
      mReplyNumBytes(0),
      mRecursionCount(0),
      mLastRecvTime(0),
      mIStream(),
      mWOStream()
{
}

kfsSeq_t
RemoteSyncSM::NextSeqnum()
{
    mSeqnum = NextSeq();
    return mSeqnum;
}

RemoteSyncSM::~RemoteSyncSM()
{
    if (mNetConnection)
        mNetConnection->Close();
    assert(mDispatchedOps.size() == 0);
}

bool
RemoteSyncSM::Connect()
{
    assert(! mNetConnection);

    KFS_LOG_STREAM_DEBUG <<
        "trying to connect to: " << mLocation <<
    KFS_LOG_EOM;

    if (! globalNetManager().IsRunning()) {
        KFS_LOG_STREAM_DEBUG <<
            "net manager shutdown, failing connection attempt to: " <<
                mLocation <<
        KFS_LOG_EOM;
        return false;
    }
    TcpSocket* const sock = new TcpSocket();
    // do a non-blocking connect
    const int res = sock->Connect(mLocation, true);
    if ((res < 0) && (res != -EINPROGRESS)) {
        KFS_LOG_STREAM_INFO <<
            "connection to remote server " << mLocation <<
            " failed: status: " << res <<
        KFS_LOG_EOM;
        delete sock;
        return false;
    }

    KFS_LOG_STREAM_INFO <<
        "connection to remote server " << mLocation <<
        " succeeded, status: " << res <<
    KFS_LOG_EOM;

    SET_HANDLER(this, &RemoteSyncSM::HandleEvent);

    mNetConnection.reset(new NetConnection(sock, this));
    mNetConnection->SetDoingNonblockingConnect();
    mNetConnection->SetMaxReadAhead(kMaxCmdHeaderLength);
    mLastRecvTime = globalNetManager().Now();

    // If there is no activity on this socket, we want
    // to be notified, so that we can close connection.
    mNetConnection->SetInactivityTimeout(sOpResponseTimeoutSec);
    // Add this to the poll vector
    globalNetManager().AddConnection(mNetConnection);

    return true;
}

void
RemoteSyncSM::Enqueue(KfsOp* op)
{
    if (mNetConnection && ! mNetConnection->IsGood()) {
        KFS_LOG_STREAM_INFO <<
            "Lost connection to peer " << mLocation <<
            " failed; failing ops" <<
        KFS_LOG_EOM;
        mNetConnection->Close();
        mNetConnection.reset();
    }
    op->seq = NextSeqnum();
    if (! mNetConnection && ! Connect()) {
        KFS_LOG_STREAM_INFO <<
            "connection to peer " << mLocation <<
            " failed; failing ops" <<
        KFS_LOG_EOM;
        if (! mDispatchedOps.insert(make_pair(op->seq, op)).second) {
            die("duplicate seq. number");
        }
        FailAllOps();
        return;
    }
    if (mDispatchedOps.empty()) {
        mLastRecvTime = globalNetManager().Now();
    }
    KFS_LOG_STREAM_DEBUG <<
        "forwarding to " << mLocation <<
        " " << op->Show() <<
    KFS_LOG_EOM;
    IOBuffer& buf   = mNetConnection->GetOutBuffer();
    const int start = buf.BytesConsumable();
    op->Request(mWOStream.Set(buf), buf);
    mWOStream.Reset();
    if (sTraceRequestResponse) {
        IOBuffer::IStream is(buf, buf.BytesConsumable());
        is.ignore(start);
        string line;
        KFS_LOG_STREAM_DEBUG << reinterpret_cast<void*>(this) <<
            " send to: " << mLocation <<
        KFS_LOG_EOM;
        while (getline(is, line)) {
            KFS_LOG_STREAM_DEBUG << reinterpret_cast<void*>(this) <<
                " request: " << line <<
            KFS_LOG_EOM;
        }
    }
    if (op->op == CMD_CLOSE) {
        // fire'n'forget
        op->status = 0;
        SubmitOpResponse(op);
    } else if (op->op == CMD_WRITE_PREPARE_FWD) {
        // send the data as well
        WritePrepareFwdOp* const wpfo = static_cast<WritePrepareFwdOp*>(op);
        op->status = 0;
        mNetConnection->WriteCopy(wpfo->owner.dataBuf,
            wpfo->owner.dataBuf->BytesConsumable());
        if (wpfo->owner.replyRequestedFlag) {
            if (! mDispatchedOps.insert(make_pair(op->seq, op)).second) {
                die("duplicate seq. number");
            }
        } else {
            // fire'n'forget
            SubmitOpResponse(op);
        }
    } else {
        if (op->op == CMD_RECORD_APPEND) {
            // send the append over; we'll get an ack back
            RecordAppendOp* const ra = static_cast<RecordAppendOp*>(op);
            mNetConnection->Write(&ra->dataBuf, ra->numBytes);
        }
        if (! mDispatchedOps.insert(make_pair(op->seq, op)).second) {
            die("duplicate seq. number");
        }
    }
    UpdateRecvTimeout();
    if (mRecursionCount <= 0 && mNetConnection) {
        mNetConnection->StartFlush();
    }
}

int
RemoteSyncSM::HandleEvent(int code, void *data)
{
    IOBuffer *iobuf;
    int msgLen = 0;
    // take a ref to prevent the object from being deleted
    // while we are still in this function.
    RemoteSyncSMPtr self = shared_from_this();
    const char *reason = "error";

    mRecursionCount++;
    assert(mRecursionCount > 0);
    switch (code) {
    case EVENT_NET_READ:
        mLastRecvTime = globalNetManager().Now();
        // We read something from the network.  Run the RPC that
        // came in if we got all the data for the RPC
        iobuf = (IOBuffer *) data;
        while ((mReplyNumBytes > 0 || IsMsgAvail(iobuf, &msgLen)) &&
                HandleResponse(iobuf, msgLen) >= 0)
            {}
        UpdateRecvTimeout();
        break;

    case EVENT_NET_WROTE:
        // Something went out on the network.  For now, we don't
        // track it. Later, we may use it for tracking throttling
        // and such.
        UpdateRecvTimeout();
        break;


    case EVENT_INACTIVITY_TIMEOUT:
        reason = "inactivity timeout";
    case EVENT_NET_ERROR:
        // If there is an error or there is no activity on the socket
        // for N mins, we close the connection.
        KFS_LOG_STREAM_INFO << "Closing connection to peer: " <<
            mLocation << " due to " << reason <<
        KFS_LOG_EOM;
        if (mNetConnection) {
            mNetConnection->Close();
            mNetConnection.reset();
        }
        break;

    default:
        assert(!"Unknown event");
        break;
    }
    assert(mRecursionCount > 0);
    if (mRecursionCount <= 1) {
        const bool connectedFlag = mNetConnection && mNetConnection->IsGood();
        if (connectedFlag) {
            mNetConnection->StartFlush();
        }
        if (! connectedFlag || ! mNetConnection || ! mNetConnection->IsGood()) {
            // we are done...
            Finish();
        }
    }
    mRecursionCount--;
    return 0;

}

int
RemoteSyncSM::HandleResponse(IOBuffer *iobuf, int msgLen)
{
    DispatchedOps::iterator i = mDispatchedOps.end();
    int nAvail = iobuf->BytesConsumable();

    if (mReplyNumBytes <= 0) {
        assert(msgLen >= 0 && msgLen <= nAvail);
        if (sTraceRequestResponse) {
            IOBuffer::IStream is(*iobuf, msgLen);
            string            line;
            while (getline(is, line)) {
                KFS_LOG_STREAM_DEBUG << reinterpret_cast<void*>(this) <<
                    " " << mLocation << " response: " << line <<
                KFS_LOG_EOM;
            }
        }
        Properties prop;
        const char separator(':');
        prop.loadProperties(mIStream.Set(*iobuf, msgLen), separator, false);
        mIStream.Reset();
        iobuf->Consume(msgLen);
        mReplySeqNum = prop.getValue("Cseq", (kfsSeq_t) -1);
        if (mReplySeqNum < 0) {
            KFS_LOG_STREAM_ERROR <<
                "invalid or missing Cseq header: " << mReplySeqNum <<
                ", resetting connection" <<
            KFS_LOG_EOM;
            HandleEvent(EVENT_NET_ERROR, 0);
        }
        mReplyNumBytes = prop.getValue("Content-length", 0);
        nAvail -= msgLen;
        i = mDispatchedOps.find(mReplySeqNum);
        KfsOp* const op = i != mDispatchedOps.end() ? i->second : 0;
        if (op) {
            op->status = prop.getValue("Status", -1);
            if (op->status < 0) {
                op->status = -KfsToSysErrno(-op->status);
            }
            if (op->op == CMD_WRITE_ID_ALLOC) {
                WriteIdAllocOp *wiao = static_cast<WriteIdAllocOp *>(op);
                wiao->writeIdStr            = prop.getValue("Write-id", "");
                wiao->writePrepareReplyFlag =
                    prop.getValue("Write-prepare-reply", 0) != 0;
            } else if (op->op == CMD_READ) {
                ReadOp *rop = static_cast<ReadOp *> (op);
                const int checksumEntries = prop.getValue("Checksum-entries", 0);
                if (checksumEntries > 0) {
                    istringstream is(prop.getValue("Checksums", ""));
                    uint32_t cks;
                    for (int i = 0; i < checksumEntries; i++) {
                        is >> cks;
                        rop->checksum.push_back(cks);
                    }
                }
                const int off(rop->offset % IOBufferData::GetDefaultBufferSize());
                if (off > 0) {
                    IOBuffer buf;
                    buf.ReplaceKeepBuffersFull(iobuf, off, nAvail);
                    iobuf->Move(&buf);
                    iobuf->Consume(off);
                } else {
                    iobuf->MakeBuffersFull();
                }
            } else if (op->op == CMD_SIZE) {
                SizeOp *sop = static_cast<SizeOp *>(op);
                sop->size = prop.getValue("Size", 0);
            } else if (op->op == CMD_GET_CHUNK_METADATA) {
                GetChunkMetadataOp *gcm = static_cast<GetChunkMetadataOp *>(op);
                gcm->chunkVersion = prop.getValue("Chunk-version", 0);
                gcm->chunkSize = prop.getValue("Size", 0);
            }
        }
    }

    // if we don't have all the data for the write, hold on...
    if (nAvail < mReplyNumBytes) {
        // the data isn't here yet...wait...
        if (mNetConnection) {
            mNetConnection->SetMaxReadAhead(mReplyNumBytes - nAvail);
        }
        return -1;
    }

    // now, we got everything...
    if (mNetConnection) {
        mNetConnection->SetMaxReadAhead(kMaxCmdHeaderLength);
    }

    // find the matching op
    if (i == mDispatchedOps.end()) {
        i = mDispatchedOps.find(mReplySeqNum);
    }
    if (i != mDispatchedOps.end()) {
        KfsOp *const op = i->second;
        mDispatchedOps.erase(i);
        if (op->op == CMD_READ) {
            ReadOp *rop = static_cast<ReadOp *> (op);
            if (rop->dataBuf == NULL)
                rop->dataBuf = new IOBuffer();
            rop->dataBuf->Move(iobuf, mReplyNumBytes);
            rop->numBytesIO = mReplyNumBytes;
        } else if (op->op == CMD_GET_CHUNK_METADATA) {
            GetChunkMetadataOp *gcm = static_cast<GetChunkMetadataOp *>(op);
            if (gcm->dataBuf == NULL)
                gcm->dataBuf = new IOBuffer();
            gcm->dataBuf->Move(iobuf, mReplyNumBytes);
        }
        mReplyNumBytes = 0;
        // op->HandleEvent(EVENT_DONE, op);
        SubmitOpResponse(op);
    } else {
        KFS_LOG_STREAM_DEBUG <<
            "discarding a reply for unknown seq #: " << mReplySeqNum <<
        KFS_LOG_EOM;
        mReplyNumBytes = 0;
    }
    return 0;
}

// Helper functor that fails an op with an error code.
class OpFailer
{
    const int errCode;
public:
    OpFailer(int c)
        : errCode(c)
        {}
    template <typename T>
    void operator()(const T& val)
    {
        KfsOp* const op = val.second;
        op->status = errCode;
        SubmitOpResponse(op);
    }
};

void
RemoteSyncSM::FailAllOps()
{
    if (mDispatchedOps.empty()) {
        return;
    }
    // There is a potential recursive call: if a client owns this
    // object and the client got a network error, the client will call
    // here to fail the outstandnig ops; when an op the client is
    // notified and the client calls to close out this object.  We'll
    // be right back here trying  to fail an op and will core.  To
    // avoid, swap out the ops and try.
    DispatchedOps opsToFail;

    mDispatchedOps.swap(opsToFail);
    for_each(opsToFail.begin(), opsToFail.end(),
             OpFailer(-EHOSTUNREACH));
    opsToFail.clear();
}

void
RemoteSyncSM::Finish()
{
    FailAllOps();
    if (mNetConnection) {
        mNetConnection->Close();
        mNetConnection.reset();
    }
    // if the object was owned by the chunkserver, have it release the reference
    gChunkServer.RemoveServer(this);
}

//
// Utility functions to operate on a list of remotesync servers
//

class RemoteSyncSMMatcher {
    const ServerLocation myLoc;
public:
    RemoteSyncSMMatcher(const ServerLocation &loc)
        :  myLoc(loc)
        {}
    bool operator() (const RemoteSyncSMPtr& other)
    {
        return other->GetLocation() == myLoc;
    }
};

RemoteSyncSMPtr
FindServer(list<RemoteSyncSMPtr> &remoteSyncers, const ServerLocation &location,
                bool connect)
{
    RemoteSyncSMPtr peer;

    list<RemoteSyncSMPtr>::iterator const i = find_if(
        remoteSyncers.begin(), remoteSyncers.end(),
        RemoteSyncSMMatcher(location));
    if (i != remoteSyncers.end()) {
        peer = *i;
        return peer;
    }
    if (!connect) {
        return peer;
    }
    peer.reset(new RemoteSyncSM(location));
    if (peer->Connect()) {
        remoteSyncers.push_back(peer);
    } else {
        // we couldn't connect...so, force destruction
        peer.reset();
    }
    return peer;
}

void
RemoveServer(list<RemoteSyncSMPtr>& remoteSyncers, RemoteSyncSM* target)
{
    if (! target) {
        return;
    }
    list<RemoteSyncSMPtr>::iterator const i = find(
        remoteSyncers.begin(), remoteSyncers.end(), target->shared_from_this());
    if (i != remoteSyncers.end()) {
        remoteSyncers.erase(i);
    }
}

void
ReleaseAllServers(list<RemoteSyncSMPtr>& remoteSyncers)
{
    while (! remoteSyncers.empty()) {
        RemoteSyncSMPtr const r = remoteSyncers.front();
        remoteSyncers.pop_front();
        r->Finish();
    }
}

}
