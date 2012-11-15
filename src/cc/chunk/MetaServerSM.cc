//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/07
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
// \file MetaServerSM.cc
// \brief Handle interactions with the meta server.
//
//----------------------------------------------------------------------------

#include "common/MsgLogger.h"
#include "MetaServerSM.h"
#include "ChunkManager.h"
#include "ChunkServer.h"
#include "utils.h"
#include "LeaseClerk.h"
#include "Replicator.h"

#include "kfsio/NetManager.h"
#include "kfsio/Globals.h"
#include "qcdio/QCUtils.h"
#include "common/kfserrno.h"

#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <algorithm>
#include <sstream>
#include <utility>

namespace KFS
{
using std::ostringstream;
using std::istringstream;
using std::make_pair;
using std::string;
using KFS::libkfsio::globalNetManager;

MetaServerSM gMetaServerSM;

MetaServerSM::MetaServerSM()
    : KfsCallbackObj(),
      ITimeout(),
      mCmdSeq(GetRandomSeq()),
      mLocation(),
      mRackId(-1),
      mClusterKey(),
      mMD5Sum(),
      mChunkServerPort(-1),
      mChunkServerHostname(),
      mSentHello(false),
      mHelloOp(0),
      mPendingOps(),
      mDispatchedNoReplyOps(),
      mDispatchedOps(),
      mNetConnection(),
      mInactivityTimeout(65),
      mMaxReadAhead(4 << 10),
      mLastRecvCmdTime(0),
      mLastConnectTime(0),
      mConnectedTime(0),
      mReconnectFlag(false),
      mCounters(),
      mIStream(),
      mWOStream()
{
    // Force net manager construction here, to insure that net manager
    // destructor is called after gMetaServerSM destructor.
    globalNetManager();
    SET_HANDLER(this, &MetaServerSM::HandleRequest);
    mCounters.Clear();
}

MetaServerSM::~MetaServerSM()
{
    globalNetManager().UnRegisterTimeoutHandler(this);
    FailOps(true);
    delete mHelloOp;
}

void 
MetaServerSM::SetMetaInfo(const ServerLocation& metaLoc, const string& clusterKey, 
    int rackId, const string& md5sum, const Properties& prop)
{
    mLocation   = metaLoc;
    mClusterKey = clusterKey;
    mRackId     = rackId;
    mMD5Sum     = md5sum;
    SetParameters(prop);
}

void
MetaServerSM::SetParameters(const Properties& prop)
{
    mInactivityTimeout = prop.getValue(
        "chunkServer.meta.inactivityTimeout", mInactivityTimeout);
    mMaxReadAhead      = prop.getValue(
        "chunkServer.meta.maxReadAhead",      mMaxReadAhead);
}

void
MetaServerSM::Init()
{
    globalNetManager().RegisterTimeoutHandler(this);
}

void
MetaServerSM::Timeout()
{
    if (mReconnectFlag) {
        mReconnectFlag = false;
        KFS_LOG_STREAM_WARN <<
            "meta server reconnect requested" <<
        KFS_LOG_EOM;
        HandleRequest(EVENT_INACTIVITY_TIMEOUT, 0);
    }
    const time_t now = globalNetManager().Now();
    if (IsConnected() &&
            IsHandshakeDone() &&
            mLastRecvCmdTime + mInactivityTimeout < now) {
        KFS_LOG_STREAM_ERROR <<
            "meta server inactivity timeout, last request received: " <<
            (now - mLastRecvCmdTime) << " secs ago" <<
        KFS_LOG_EOM;
        HandleRequest(EVENT_INACTIVITY_TIMEOUT, 0);
    }
    if (! IsConnected()) {
        if (mHelloOp) {
            if (! mSentHello) {
                return; // Wait for hello to come back.
            }
            delete mHelloOp;
            mHelloOp   = 0;
            mSentHello = false;
        }
        if (mLastConnectTime + 1 < now) {
            mLastConnectTime = now;
            Connect();
        }
        return;
    }
    if (! IsHandshakeDone()) {
        return;
    }
    DispatchOps();
    mNetConnection->StartFlush();
}

time_t
MetaServerSM::ConnectionUptime() const
{
    return (IsUp() ? (globalNetManager().Now() - mLastConnectTime) : 0);
}

int
MetaServerSM::Connect()
{
    if (mHelloOp) {
        return 0;
    }
    mCounters.mConnectCount++;
    mSentHello = false;
    TcpSocket * const sock = new TcpSocket();
    const bool nonBlocking = true;
    const int  ret         = sock->Connect(mLocation, nonBlocking);
    if (ret < 0 && ret != -EINPROGRESS) {
        KFS_LOG_STREAM_ERROR <<
            "connection to meter server failed:"
            " error: " << QCUtils::SysError(-ret) <<
        KFS_LOG_EOM;
        delete sock;
        return -1;
    }
    KFS_LOG_STREAM_INFO <<
        (ret < 0 ? "connecting" : "connected") <<
            " to metaserver " << mLocation.ToString() <<
    KFS_LOG_EOM;
    mNetConnection.reset(new NetConnection(sock, this));
    if (ret != 0) {
        mNetConnection->SetDoingNonblockingConnect();
    }
    // when the system is overloaded, we still want to add this
    // connection to the poll vector for reads; this ensures that we
    // get the heartbeats and other RPCs from the metaserver
    mNetConnection->EnableReadIfOverloaded();
    mNetConnection->SetInactivityTimeout(mInactivityTimeout);
    mNetConnection->SetMaxReadAhead(mMaxReadAhead);
    // Add this to the poll vector
    globalNetManager().AddConnection(mNetConnection);
    if (ret == 0) {
        SendHello();
    }
    return 0;
}

inline void
ChunkServer::SetLocation(const ServerLocation& loc)
{
    mLocation = loc;
}

static inline int
IsIpHostedAndNotLoopBack(const char* ip)
{
    if (! ip) {
        return -EINVAL;
    }
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = 0; // any port
    if (! inet_aton(ip, &addr.sin_addr)) {
        return -EINVAL;
    }
    if (addr.sin_addr.s_addr == htonl(INADDR_LOOPBACK)) {
        return -EACCES;
    }
    const int fd = socket(PF_INET, SOCK_DGRAM, IPPROTO_IP);
    if (fd < 0) {
        return -errno;
    }
    const int ret = bind(fd, (struct sockaddr*)&addr, sizeof(addr)) == 0 ?
        0 : -errno;
    close(fd);
    return ret;
}

int
MetaServerSM::SendHello()
{
    if (mHelloOp) {
        return 0;
    }
    if (! IsConnected()) {
        KFS_LOG_STREAM_DEBUG <<
            "unable to connect to meta server" <<
        KFS_LOG_EOM;
        return -1;
    }
    if (gChunkServer.CanUpdateServerIp()) {
        // Advertise the same ip address to the clients, as used
        // for the meta connection.
        ServerLocation loc(gChunkServer.GetLocation());
        loc.hostname = mNetConnection->GetSockName();
        const size_t colonPos = loc.hostname.find(char(':'));
        if (colonPos == string::npos) {
            KFS_LOG_STREAM_ERROR <<
                "invalid socket name: " << loc.hostname <<
                " resetting meta server connection" <<
            KFS_LOG_EOM;
            mNetConnection->Close();
            return -1;
        }
        loc.hostname.erase(colonPos);
        // Paperover for cygwin / win 7 with no nics configured:
        // check if getsockname returns INADDR_ANY, and retry if it does.
        // Moving this logic into TcpSocket isn't appropriate: INADDR_ANY is
        // valid for unconnected socket bound to INADDR_ANY.
        const char* const kAddrAny = "0.0.0.0";
        if (! loc.IsValid() || loc.hostname == kAddrAny) {
            KFS_LOG_STREAM_ERROR <<
                "invalid chunk server location: " << loc <<
                " resetting meta server connection" <<
            KFS_LOG_EOM;
            mNetConnection->Close();
            return -1;
        }
        const string prevIp = gChunkServer.GetLocation().hostname;
        if (loc.hostname != prevIp) {
            if (prevIp.empty()) {
                KFS_LOG_STREAM_INFO <<
                    "setting chunk server ip to: " << loc.hostname <<
                KFS_LOG_EOM;
                gChunkServer.SetLocation(loc);
            } else {
                const int err = IsIpHostedAndNotLoopBack(prevIp.c_str());
                KFS_LOG_STREAM_WARN <<
                    "meta server connection local address: " << loc.hostname <<
                    " current chunk server ip: " << prevIp <<
                    (err == 0 ? string() :
                        " is no longer valid: " + QCUtils::SysError(-err)) <<
                KFS_LOG_EOM;
                if (err) {
                    gChunkServer.SetLocation(loc);
                }
            }
        }
    }
    mHelloOp = new HelloMetaOp(
        nextSeq(), gChunkServer.GetLocation(), mClusterKey, mMD5Sum, mRackId);
    mHelloOp->clnt = this;
    // Send the op and wait for the reply.
    SubmitOp(mHelloOp);
    return 0;
}

void
MetaServerSM::DispatchHello()
{
    if (! IsConnected()) {
        // don't have a connection...so, need to start the process again...
        delete mHelloOp;
        mHelloOp = 0;
        mSentHello = false;
        return;
    }
    mSentHello = true;
    IOBuffer& ioBuf = mNetConnection->GetOutBuffer();
    mHelloOp->Request(mWOStream.Set(ioBuf), ioBuf);
    mWOStream.Reset();
    KFS_LOG_STREAM_INFO <<
        "Sending hello to meta server: " << mHelloOp->Show() <<
    KFS_LOG_EOM;
    mNetConnection->StartFlush();
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
MetaServerSM::HandleRequest(int code, void* data)
{
    switch (code) {
    case EVENT_NET_READ: {
            // We read something from the network.  Run the RPC that
            // came in.
            IOBuffer * const iobuf = (IOBuffer *) data;
            bool hasMsg;
            int  cmdLen = 0;
            while ((hasMsg = IsMsgAvail(iobuf, &cmdLen))) {
                // if we don't have all the data for the command, bail
                if (! HandleMsg(iobuf, cmdLen)) {
                    break;
                }
            }
            int hdrsz;
            if (! hasMsg &&
                    (hdrsz = iobuf->BytesConsumable()) > MAX_RPC_HEADER_LEN) {
                KFS_LOG_STREAM_ERROR <<
                    "exceeded max request header size: " << hdrsz <<
                    ">" << MAX_RPC_HEADER_LEN <<
                    " closing connection: " << (IsConnected() ?
                        mNetConnection->GetPeerName() :
                        string("not connected")) <<
                KFS_LOG_EOM;
                iobuf->Clear();
                return HandleRequest(EVENT_NET_ERROR, 0);
            }
        }
        break;

    case EVENT_NET_WROTE:
        if (! mSentHello && ! mHelloOp) {
            SendHello();
        }
        // Something went out on the network.  For now, we don't
        // track it. Later, we may use it for tracking throttling
        // and such.
        break;

    case EVENT_CMD_DONE: {
            // An op finished execution.  Send a response back
            KfsOp* const op = reinterpret_cast<KfsOp*>(data);
            if (op->op == CMD_META_HELLO) {
                DispatchHello();
            } else {
                SendResponse(op);
                delete op;
            }
        }
        break;

    case EVENT_INACTIVITY_TIMEOUT:
    case EVENT_NET_ERROR:
        if (mNetConnection) {
            KFS_LOG_STREAM(globalNetManager().IsRunning() ?
                    MsgLogger::kLogLevelERROR :
                    MsgLogger::kLogLevelDEBUG) <<
                mLocation.ToString() <<
                " closing meta server connection due to " <<
                (code == EVENT_INACTIVITY_TIMEOUT ?
                    "inactivity timeout" : "network error") <<
            KFS_LOG_EOM;
            mNetConnection->Close();
            // Drop all leases.
            gLeaseClerk.UnregisterAllLeases();
            // Meta server will fail all replication requests on
            // disconnect anyway.
            Replicator::CancelAll();
            gChunkManager.MetaServerConnectionLost();
        }
        FailOps(! globalNetManager().IsRunning());
        break;

    default:
        assert(!"Unknown event");
        break;
    }
    return 0;
}

void
MetaServerSM::FailOps(bool shutdownFlag)
{
    // Fail all no retry ops, if any, or all ops on shutdown.
    OpsQueue   doneOps;
    for (DispatchedOps::iterator it = mDispatchedOps.begin();
            it != mDispatchedOps.end();
            ) {
        KfsOp* const op = it->second;
        if (op->noRetry || shutdownFlag) {
            mDispatchedOps.erase(it++);
            doneOps.push_back(op);
        } else {
            ++it;
        }
    }
    for (; ;) {
        for (OpsQueue::const_iterator it = doneOps.begin();
                it != doneOps.end();
                ++it) {
            KfsOp* const op = *it;
            op->status = -EHOSTUNREACH;
            SubmitOpResponse(op);
        }
        if (! shutdownFlag || mPendingOps.empty()) {
            break;
        }
        doneOps.clear();
        mPendingOps.swap(doneOps);
    }
}

bool
MetaServerSM::HandleMsg(IOBuffer *iobuf, int msgLen)
{
    char buf[3];
    if (iobuf->CopyOut(buf, 3) == 3 &&
            buf[0] == 'O' && buf[1] == 'K' && (buf[2] & 0xFF) <= ' ') {
        // This is a response to some op we sent earlier
        return HandleReply(iobuf, msgLen);
    } else {
        // is an RPC from the server
        return HandleCmd(iobuf, msgLen);
    }
}

bool
MetaServerSM::HandleReply(IOBuffer *iobuf, int msgLen)
{
    Properties prop;
    const char separator = ':';
    prop.loadProperties(mIStream.Set(*iobuf, msgLen), separator, false);
    mIStream.Reset();
    iobuf->Consume(msgLen);

    const kfsSeq_t seq    = prop.getValue("Cseq",  (kfsSeq_t)-1);
    int            status = prop.getValue("Status",          -1);
    if (status < 0) {
        status = -KfsToSysErrno(-status);
    }
    if (mHelloOp) {
        if (status == -EBADCLUSTERKEY) {
            KFS_LOG_STREAM_FATAL <<
                "exiting due to cluster key mismatch; our key: " << mClusterKey <<
            KFS_LOG_EOM;
            globalNetManager().Shutdown();
            return false;
        }
        mCounters.mHelloCount++;
        const bool err = seq != mHelloOp->seq || status != 0;
        if (err) {
            KFS_LOG_STREAM_ERROR <<
                " bad hello response:"
                " seq: "    << seq << "/" << mHelloOp->seq <<
                " status: " << status <<
            KFS_LOG_EOM;
            mCounters.mHelloErrorCount++;
        }
        HelloMetaOp::LostChunkDirs lostDirs;
        lostDirs.swap(mHelloOp->lostChunkDirs);
        delete mHelloOp;
        mHelloOp = 0;
        if (err) {
            HandleRequest(EVENT_NET_ERROR, 0);
            return false;
        }
        mConnectedTime = globalNetManager().Now();
        ResubmitOps();
        for (HelloMetaOp::LostChunkDirs::const_iterator it = lostDirs.begin();
                it != lostDirs.end();
                ++it) {
            EnqueueOp(new CorruptChunkOp(0, -1, -1, &(*it), false));
        }
        return true;
    }
    DispatchedOps::iterator const iter = mDispatchedOps.find(seq);
    if (iter == mDispatchedOps.end()) {
        string reply;
        prop.getList(reply, string(), string(" "));
        KFS_LOG_STREAM_DEBUG << "meta reply:"
            " no op found for: " << reply <<
        KFS_LOG_EOM;        
        return true;
    }
    KfsOp* const op = iter->second;
    mDispatchedOps.erase(iter);
    op->status = status;
    KFS_LOG_STREAM_DEBUG <<
        "recv meta reply:"
        " seq: "    << seq <<
        " status: " << status <<
        " "         << op->Show() <<
    KFS_LOG_EOM;        
    // The op will be gotten rid of by this call.
    SubmitOpResponse(op);
    return true;
}

///
/// We have a command in a buffer.  It is possible that we don't have
/// everything we need to execute it (for example, for a stale chunks
/// RPC, we may not have received all the chunkids).  So, parse
/// out the command and if we have everything execute it.
/// 

bool
MetaServerSM::HandleCmd(IOBuffer* iobuf, int cmdLen)
{
    KfsOp* op = 0;
    if (ParseCommand(*iobuf, cmdLen, &op) != 0) {
        IOBuffer::IStream is(*iobuf, cmdLen);
        const string peer = IsConnected() ?
            mNetConnection->GetPeerName() : string("not connected");
        string line;
        int numLines = 32;
        while (--numLines >= 0 && getline(is, line)) {
            KFS_LOG_STREAM_ERROR << peer <<
                " invalid meta request: " << line <<
            KFS_LOG_EOM;
        }
        iobuf->Clear();
        HandleRequest(EVENT_NET_ERROR, 0);
        // got a bogus command
        return false;
    }

    const int contentLength = op->GetContentLength();
    const int remLen = cmdLen + contentLength - iobuf->BytesConsumable();
    if (remLen > 0) {
        // if we don't have all the data wait...
        if (remLen > mMaxReadAhead && mNetConnection) {
            mNetConnection->SetMaxReadAhead(remLen);
        }
        delete op;
        return false;
    }
    if (mNetConnection) {
        mNetConnection->SetMaxReadAhead(mMaxReadAhead);
    }
    iobuf->Consume(cmdLen);
    if (contentLength > 0) {
        IOBuffer::IStream is(*iobuf, contentLength);
        if (! op->ParseContent(is)) {
            KFS_LOG_STREAM_ERROR <<
                (IsConnected() ?  mNetConnection->GetPeerName() : "") <<
                " invalid content: " << op->statusMsg <<
                " cmd: " << op->Show() <<
            KFS_LOG_EOM;
            delete op;
            HandleRequest(EVENT_NET_ERROR, 0);
            return false;
        }
        iobuf->Consume(contentLength);
    }
    mLastRecvCmdTime = globalNetManager().Now();
    op->clnt = this;
    KFS_LOG_STREAM_DEBUG <<
        "recv meta cmd:"
        " seq: " << op->seq <<
        " "      << op->Show() <<
    KFS_LOG_EOM;
    SubmitOp(op);
    return true;
}

void
MetaServerSM::EnqueueOp(KfsOp* op)
{
    op->seq = nextSeq();
    if (mPendingOps.empty() && IsUp()) {
        if (! op->noReply &&
                ! mDispatchedOps.insert(make_pair(op->seq, op)).second) {
            die("duplicate seq. number");
        }
        IOBuffer& ioBuf = mNetConnection->GetOutBuffer();
        op->Request(mWOStream.Set(ioBuf), ioBuf);
        mWOStream.Reset();
        op->status = 0;
        if (op->noReply) {
            SubmitOpResponse(op);
        }
    } else {
        if (globalNetManager().IsRunning()) {
            mPendingOps.push_back(op);
        } else {
            op->status = -EHOSTUNREACH;
            SubmitOpResponse(op);
        }
    }
    globalNetManager().Wakeup();
}

///
/// Queue the response to the meta server request.  The response is
/// generated by MetaRequest as per the protocol.
/// @param[in] op The request for which we finished execution.
///

void
MetaServerSM::SendResponse(KfsOp* op)
{
    if (! mSentHello || ! IsConnected()) {
        // Hello does full chunk inventory synchronization.
        // Meta server assumes undefined state for all requests that were in
        // in flight at the time of disconnect, and will discard the responses
        // anyway, as it will purge its pending response queue at the time of
        // disconnect.
        return;
    }
    // fire'n'forget.
    KFS_LOG_STREAM_DEBUG <<
        "send meta reply:"
        " seq: "     << op->seq <<
        (op->statusMsg.empty() ? "" : " msg: ") << op->statusMsg <<
        " status: "  << op->status <<
        " "          << op->Show() <<
    KFS_LOG_EOM;
    if (op->op == CMD_ALLOC_CHUNK) {
        mCounters.mAllocCount++;
        if (op->status < 0) {
            mCounters.mAllocErrorCount++;
        }
    }
    op->Response(mWOStream.Set(mNetConnection->GetOutBuffer()));
    mWOStream.Reset();
    globalNetManager().Wakeup();
}

void
MetaServerSM::DispatchOps()
{
    OpsQueue doneOps;
    while (! mPendingOps.empty() && IsHandshakeDone()) {
        if (! IsConnected()) {
            KFS_LOG_STREAM_INFO <<
                "meta handshake is not done, will dispatch later" <<
            KFS_LOG_EOM;
            return;
        }
        KfsOp* const op = mPendingOps.front();
        mPendingOps.pop_front();
        assert(op->op != CMD_META_HELLO);
        if (op->noReply) {
            mDispatchedNoReplyOps.push_back(op);
        } else if (! mDispatchedOps.insert(make_pair(op->seq, op)).second) {
            die("duplicate seq. number");
        }
        KFS_LOG_STREAM_DEBUG <<
            "send meta cmd:"
            " seq: " << op->seq <<
            " "      << op->Show() <<
        KFS_LOG_EOM;
        IOBuffer& ioBuf = mNetConnection->GetOutBuffer();
        op->Request(mWOStream.Set(ioBuf), ioBuf);
        mWOStream.Reset();
    }
    while (! mDispatchedNoReplyOps.empty()) {
        KfsOp* const op = mDispatchedNoReplyOps.front();
        mDispatchedNoReplyOps.pop_front();
        SubmitOpResponse(op);
    }
}

// After re-establishing connection to the server, resubmit the ops.
void
MetaServerSM::ResubmitOps()
{
    if (mDispatchedOps.empty()) {
        return;
    }
    IOBuffer& ioBuf = mNetConnection->GetOutBuffer();
    ostream&  os    = mWOStream.Set(ioBuf);
    for (DispatchedOps::const_iterator it = mDispatchedOps.begin();
            it != mDispatchedOps.end();
            ++it) {
        it->second->Request(os, ioBuf);
    }
    mWOStream.Reset();
}

} // namespace KFS
