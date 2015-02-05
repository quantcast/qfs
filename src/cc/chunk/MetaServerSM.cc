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

#include <algorithm>
#include <sstream>
#include <utility>

namespace KFS
{
using std::ostringstream;
using std::istringstream;
using std::make_pair;
using std::string;
using std::max;
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
      mAuthOp(0),
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
      mAuthContext(),
      mAuthRequestCtx(),
      mAuthType(
        kAuthenticationTypeKrb5 |
        kAuthenticationTypeX509 |
        kAuthenticationTypePSK),
      mAuthTypeStr("Krb5 X509 PSK"),
      mCurrentKeyId(),
      mUpdateCurrentKeyFlag(false),
      mNoFidsFlag(true),
      mOp(0),
      mRequestFlag(false),
      mContentLength(0),
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
    MetaServerSM::Shutdown();
}

void
MetaServerSM::CleanupOpInFlight()
{
    if (mRequestFlag) {
        delete mOp;
    }
    mOp = 0;
}

int
MetaServerSM::SetMetaInfo(
    const ServerLocation& metaLoc,
    const string&         clusterKey,
    int                   rackId,
    const string&         md5sum,
    const Properties&     prop)
{
    mLocation   = metaLoc;
    mClusterKey = clusterKey;
    mRackId     = rackId;
    mMD5Sum     = md5sum;
    return SetParameters(prop);
}

void
MetaServerSM::Shutdown()
{
    if (! mLocation.IsValid() && ! mNetConnection) {
        return;
    }
    if (mNetConnection) {
        mNetConnection->Close();
    }
    mNetConnection.reset();
    globalNetManager().UnRegisterTimeoutHandler(this);
    CleanupOpInFlight();
    DiscardPendingResponses();
    FailOps(true);
    delete mHelloOp;
    mHelloOp = 0;
    delete mAuthOp;
    mHelloOp = 0;
    mAuthContext.Clear();
    if (mLocation.IsValid()) {
        mLocation.port = -mLocation.port;
    }
}

int
MetaServerSM::SetParameters(const Properties& prop)
{
    mInactivityTimeout = prop.getValue(
        "chunkServer.meta.inactivityTimeout", mInactivityTimeout);
    mMaxReadAhead      = prop.getValue(
        "chunkServer.meta.maxReadAhead",      mMaxReadAhead);
    mNoFidsFlag        = prop.getValue(
        "chunkServer.meta.noFids",            mNoFidsFlag ? 1 : 0) != 0;
    const bool kVerifyFlag = true;
    int ret = mAuthContext.SetParameters(
        "chunkserver.meta.auth.", prop, 0, 0, kVerifyFlag);
    const char* const kAuthTypeParamName = "chunkserver.meta.auth.authType";
    mAuthTypeStr = prop.getValue(kAuthTypeParamName, mAuthTypeStr);
    istringstream is(mAuthTypeStr);
    string type;
    mAuthType = 0;
    while ((is >> type)) {
        if (type == "Krb5") {
            mAuthType |= kAuthenticationTypeKrb5;
        } else if (type == "X509") {
            mAuthType |= kAuthenticationTypeX509;
        } else if (type == "PSK") {
            mAuthType |= kAuthenticationTypePSK;
        }
    }
    if (mAuthContext.IsEnabled()) {
        string    errMsg;
        bool      authRequiredFlag = false;
        const int err = mAuthContext.CheckAuthType(
            mAuthType, authRequiredFlag, &errMsg);
        if (err) {
            if (ret == 0) {
                ret = err;
            }
            KFS_LOG_STREAM_ERROR <<
                "invalid " << kAuthTypeParamName <<
                " " << mAuthType <<
                " " << errMsg <<
            KFS_LOG_EOM;
        }
    }
    return ret;
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
    if (mAuthOp || ! IsHandshakeDone()) {
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
    delete mAuthOp;
    mAuthOp = 0;
    CleanupOpInFlight();
    DiscardPendingResponses();
    mContentLength = 0;
    mCounters.mConnectCount++;
    mSentHello = false;
    mUpdateCurrentKeyFlag = false;
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
            " to metaserver " << mLocation <<
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
    const bool kIpV6OnlyFlag = false;
    TcpSocket  socket;
    int ret = socket.Bind(
            ServerLocation(ip, 0), TcpSocket::kTypeIpV4, kIpV6OnlyFlag);
    ServerLocation loc;
    if (ret < 0 || (ret = socket.GetSockLocation(loc)) < 0) {
        return ret;
    }
    const string& name = loc.hostname;
    if (socket.GetType() == TcpSocket::kTypeIpV4) {
        return ((name == "127.0.0.1" || name == "0.0.0.0") ? -EACCES : 0);
    }
    return ((name == "::1" || name == "::") ?  -EACCES : 0);
}

int
MetaServerSM::SendHello()
{
    if (mHelloOp || mAuthOp) {
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
        ServerLocation loc;
        const int res = mNetConnection->GetSockLocation(loc);
        if (res < 0) {
            KFS_LOG_STREAM_ERROR <<
                "getsockname: " << QCUtils::SysError(-res) <<
            KFS_LOG_EOM;
            mNetConnection->Close();
            return -1;
        }
        // Paperover for cygwin / win 7 with no nics configured:
        // check if getsockname returns INADDR_ANY, and retry if it does.
        // Moving this logic into TcpSocket isn't appropriate: INADDR_ANY is
        // valid for unconnected socket bound to INADDR_ANY.
        const char* const kAddrAny = "0.0.0.0";
        if (loc.hostname == kAddrAny && mLocation.hostname == "127.0.0.1") {
            loc.hostname = mLocation.hostname;
        }
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
            loc.port = gChunkServer.GetLocation().port;
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
    if (! Authenticate()) {
        mHelloOp = new HelloMetaOp(
            nextSeq(), gChunkServer.GetLocation(),
            mClusterKey, mMD5Sum, mRackId);
        mHelloOp->noFidsFlag = mNoFidsFlag;
        mHelloOp->clnt       = this;
        // Send the op and wait for the reply.
        SubmitOp(mHelloOp);
    }
    return 0;
}

bool
MetaServerSM::Authenticate()
{
    if (! mAuthContext.IsEnabled()) {
        return false;
    }
    if (mAuthOp) {
        die("invalid authenticate invocation: auth is in flight");
        return true;
    }
    mAuthOp = new AuthenticateOp(nextSeq());
    string    errMsg;
    const int err = mAuthContext.Request(
        mAuthType,
        mAuthOp->requestedAuthType,
        mAuthOp->reqBuf,
        mAuthOp->contentLength,
        mAuthRequestCtx,
        &errMsg
    );
    if (err) {
        KFS_LOG_STREAM_ERROR <<
            "authentication request failure: " <<
            errMsg <<
        KFS_LOG_EOM;
        delete mAuthOp;
        mAuthOp = 0;
        HandleRequest(EVENT_NET_ERROR, 0);
        return true;
    }
    IOBuffer& ioBuf = mNetConnection->GetOutBuffer();
    mAuthOp->Request(mWOStream.Set(ioBuf), ioBuf);
    mWOStream.Reset();
    KFS_LOG_STREAM_INFO << "started: " << mAuthOp->Show() << KFS_LOG_EOM;
    return true;
}

void
MetaServerSM::DispatchHello()
{
    if (mSentHello || mAuthOp) {
        die("dispatch hello: invalid invocation");
        HandleRequest(EVENT_NET_ERROR, 0);
        return;
    }
    if (! IsConnected()) {
        // don't have a connection...so, need to start the process again...
        delete mAuthOp;
        mAuthOp = 0;
        delete mHelloOp;
        mHelloOp = 0;
        mSentHello = false;
        mUpdateCurrentKeyFlag = false;
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
            IOBuffer& iobuf = mNetConnection->GetInBuffer();
            assert(&iobuf == data);
            if ((mOp || mAuthOp) && iobuf.BytesConsumable() < mContentLength) {
                break;
            }
            if (mAuthOp) {
                if (mOp && ! IsHandshakeDone()) {
                    die("op and authentication in flight");
                }
                if (! mOp && 0 < mContentLength) {
                    HandleAuthResponse(iobuf);
                    break;
                }
            }
            if (mOp && ! (mRequestFlag ?
                    HandleCmd(iobuf, 0) :
                    HandleReply(iobuf, 0))) {
                break;
            }
            bool hasMsg;
            int  cmdLen = 0;
            while ((hasMsg = IsMsgAvail(&iobuf, &cmdLen))) {
                // if we don't have all the data for the command, bail
                if (! HandleMsg(iobuf, cmdLen)) {
                    break;
                }
            }
            int hdrsz;
            if (! hasMsg &&
                    (hdrsz = iobuf.BytesConsumable()) > MAX_RPC_HEADER_LEN) {
                KFS_LOG_STREAM_ERROR <<
                    "exceeded max request header size: " << hdrsz <<
                    ">" << MAX_RPC_HEADER_LEN <<
                    " closing connection: " << (IsConnected() ?
                        mNetConnection->GetPeerName() :
                        string("not connected")) <<
                KFS_LOG_EOM;
                iobuf.Clear();
                return HandleRequest(EVENT_NET_ERROR, 0);
            }
        }
        break;

    case EVENT_NET_WROTE:
        if (! mAuthOp && ! mSentHello && ! mHelloOp) {
            SendHello();
        }
        // Something went out on the network.  For now, we don't
        // track it. Later, we may use it for tracking throttling
        // and such.
        break;

    case EVENT_CMD_DONE: {
            // An op finished execution.  Send a response back
            if (! data) {
                die("invalid null op completion");
                break;
            }
            KfsOp* const op = reinterpret_cast<KfsOp*>(data);
            if (op == mAuthOp) {
                die("invalid authentication op completion");
                break;
            }
            if (op == mHelloOp) {
                DispatchHello();
                break;
            }
            if (mUpdateCurrentKeyFlag && op->op == CMD_HEARTBEAT) {
                assert(! mOp);
                HeartbeatOp& hb = *static_cast<HeartbeatOp*>(op);
                if ((hb.sendCurrentKeyFlag =
                        gChunkManager.GetCryptoKeys().GetCurrentKey(
                            hb.currentKeyId, hb.currentKey) &&
                        hb.currentKeyId != mCurrentKeyId)) {
                    mCurrentKeyId = hb.currentKeyId;
                }
            }
            if (SendResponse(op)) {
                delete op;
            }
        }
        break;

    case EVENT_INACTIVITY_TIMEOUT:
    case EVENT_NET_ERROR:
        if (mAuthOp && ! mOp && IsUp() && ! mNetConnection->GetFilter()) {
            HandleAuthResponse(mNetConnection->GetInBuffer());
            return 0;
        }
        delete mAuthOp;
        mAuthOp = 0;
        CleanupOpInFlight();
        DiscardPendingResponses();
        if (mNetConnection) {
            KFS_LOG_STREAM(globalNetManager().IsRunning() ?
                    MsgLogger::kLogLevelERROR :
                    MsgLogger::kLogLevelDEBUG) <<
                mLocation <<
                " closing meta server connection due to " <<
                (code == EVENT_INACTIVITY_TIMEOUT ?
                    "inactivity timeout" : "network error") <<
            KFS_LOG_EOM;
            mNetConnection->Close();
            mNetConnection->GetInBuffer().Clear();
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
MetaServerSM::HandleMsg(IOBuffer& iobuf, int msgLen)
{
    char buf[3];
    if (iobuf.CopyOut(buf, 3) == 3 &&
            buf[0] == 'O' && buf[1] == 'K' && (buf[2] & 0xFF) <= ' ') {
        // This is a response to some op we sent earlier
        return HandleReply(iobuf, msgLen);
    } else {
        // is an RPC from the server
        return HandleCmd(iobuf, msgLen);
    }
}

bool
MetaServerSM::HandleReply(IOBuffer& iobuf, int msgLen)
{
    DispatchedOps::iterator iter = mDispatchedOps.end();
    KfsOp* op = mOp;
    if (op) {
        mOp = 0;
    } else {
        Properties prop;
        const char separator = ':';
        prop.loadProperties(mIStream.Set(iobuf, msgLen), separator);
        mIStream.Reset();
        iobuf.Consume(msgLen);

        const kfsSeq_t seq    = prop.getValue("Cseq",  (kfsSeq_t)-1);
        int            status = prop.getValue("Status",          -1);
        string         statusMsg;
        if (status < 0) {
            status    = -KfsToSysErrno(-status);
            statusMsg = prop.getValue("Status-message", string());
        }
        mContentLength = prop.getValue("Content-length",  -1);
        if (mAuthOp && (! IsHandshakeDone() || seq == mAuthOp->seq)) {
            if (seq != mAuthOp->seq) {
                KFS_LOG_STREAM_ERROR <<
                    "authentication response seq number mismatch: " <<
                    seq << "/" << mAuthOp->seq <<
                    " " << mAuthOp->Show() <<
                KFS_LOG_EOM;
                HandleRequest(EVENT_NET_ERROR, 0);
                return false;
            }
            mAuthOp->status                = status;
            mAuthOp->responseContentLength = mContentLength;
            if (status < 0) {
                mAuthOp->statusMsg = statusMsg;
            }
            if (! mAuthOp->ParseResponse(prop, iobuf) && 0 <= status) {
                KFS_LOG_STREAM_ERROR <<
                    "invalid meta reply response:"
                    " seq: "         << op->seq <<
                    " "              << op->Show() <<
                KFS_LOG_EOM;
                HandleRequest(EVENT_NET_ERROR, 0);
                return false;
            }
            HandleAuthResponse(iobuf);
            return false;
        }
        if (mHelloOp) {
            if (status == -EBADCLUSTERKEY) {
                KFS_LOG_STREAM_FATAL <<
                    "exiting due to cluster key mismatch; our key: " <<
                    mClusterKey <<
                KFS_LOG_EOM;
                globalNetManager().Shutdown();
                return false;
            }
            mCounters.mHelloCount++;
            const bool err =
                seq != mHelloOp->seq || status != 0 || 0 < mContentLength;
            if (err) {
                KFS_LOG_STREAM_ERROR <<
                    "hello response error:"
                    " seq: "         << seq << " => " << mHelloOp->seq <<
                    " status: "      << status <<
                    " msg: "         << statusMsg <<
                    " content len: " << mContentLength <<
                KFS_LOG_EOM;
                mCounters.mHelloErrorCount++;
            } else {
                mHelloOp->metaFileSystemId =
                    prop.getValue("File-system-id", int64_t(-1));
                const int64_t deleteAllChunksId =
                    prop.getValue("Delete-all-chunks", (int64_t)-1);
                mHelloOp->deleteAllChunksFlag =
                    0 < mHelloOp->metaFileSystemId &&
                    deleteAllChunksId == mHelloOp->metaFileSystemId &&
                    0 < mHelloOp->fileSystemId &&
                    mHelloOp->fileSystemId != mHelloOp->metaFileSystemId;
                if (0 < mHelloOp->metaFileSystemId) {
                    gChunkManager.SetFileSystemId(
                        mHelloOp->metaFileSystemId,
                        mHelloOp->deleteAllChunksFlag);
                }
            }
            HelloMetaOp::LostChunkDirs lostDirs;
            lostDirs.swap(mHelloOp->lostChunkDirs);
            mUpdateCurrentKeyFlag = err == 0 && mHelloOp->sendCurrentKeyFlag;
            if (mUpdateCurrentKeyFlag) {
                mCurrentKeyId = mHelloOp->currentKeyId;
            }
            delete mHelloOp;
            mHelloOp = 0;
            if (err) {
                HandleRequest(EVENT_NET_ERROR, 0);
                return false;
            }
            mConnectedTime = globalNetManager().Now();
            ResubmitOps();
            for (HelloMetaOp::LostChunkDirs::const_iterator
                    it = lostDirs.begin();
                    it != lostDirs.end();
                    ++it) {
                EnqueueOp(new CorruptChunkOp(0, -1, -1, &(*it), false));
            }
            return true;
        }
        iter = mDispatchedOps.find(seq);
        if (iter == mDispatchedOps.end()) {
            string reply;
            prop.getList(reply, string(), string(" "));
            KFS_LOG_STREAM_ERROR << "meta reply:"
                " no op found for: " << reply <<
            KFS_LOG_EOM;
            HandleRequest(EVENT_NET_ERROR, 0);
            return false;
        }
        op = iter->second;
        op->status = status;
        if (status < 0 && op->statusMsg.empty()) {
            op->statusMsg.swap(statusMsg);
        }
        if (! op->ParseResponse(prop, iobuf) && 0 <= status) {
            KFS_LOG_STREAM_ERROR <<
                "invalid meta reply response:"
                " seq: "         << op->seq <<
                " "              << op->Show() <<
            KFS_LOG_EOM;
            HandleRequest(EVENT_NET_ERROR, 0);
            return false;
        }
    }
    if (0 < mContentLength) {
        const int rem = mContentLength - iobuf.BytesConsumable();
        if (0 < rem) {
            // if we don't have all the data wait...
            if (mNetConnection) {
                mNetConnection->SetMaxReadAhead(max(mMaxReadAhead, rem));
            }
            mRequestFlag = false;
            mOp          = op;
            return false;
        }
        const bool ok = op->ParseResponseContent(
            mIStream.Set(iobuf, mContentLength), mContentLength);
        mIStream.Reset();
        iobuf.Consume(mContentLength);
        const int len = mContentLength;
        mContentLength = 0;
        if (! ok) {
            KFS_LOG_STREAM_ERROR <<
                "invalid meta reply response content:"
                " seq: "         << op->seq <<
                " "              << op->Show() <<
                " content len: " << len <<
            KFS_LOG_EOM;
            HandleRequest(EVENT_NET_ERROR, 0);
            return false;
        }
    }
    if (iter != mDispatchedOps.end()) {
        mDispatchedOps.erase(iter);
    } else {
        mDispatchedOps.erase(op->seq);
    }
    KFS_LOG_STREAM_DEBUG <<
        "recv meta reply:"
        " seq: "    << op->seq <<
        " status: " << op->status <<
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
MetaServerSM::HandleCmd(IOBuffer& iobuf, int cmdLen)
{
    KfsOp* op = mOp;
    mOp = 0;
    if (! op && ParseMetaCommand(iobuf, cmdLen, &op) != 0) {
        IOBuffer::IStream is(iobuf, cmdLen);
        const string peer = IsConnected() ?
            mNetConnection->GetPeerName() : string("not connected");
        string line;
        int numLines = 32;
        while (--numLines >= 0 && getline(is, line)) {
            KFS_LOG_STREAM_ERROR << peer <<
                " invalid meta request: " << line <<
            KFS_LOG_EOM;
        }
        iobuf.Clear();
        HandleRequest(EVENT_NET_ERROR, 0);
        // got a bogus command
        return false;
    }
    iobuf.Consume(cmdLen);

    mContentLength = op->GetContentLength();
    const int rem = mContentLength - iobuf.BytesConsumable();
    if (0 < rem) {
        // if we don't have all the data wait...
        if (mNetConnection) {
            mNetConnection->SetMaxReadAhead(max(mMaxReadAhead, rem));
        }
        mRequestFlag = true;
        mOp          = op;
        return false;
    }
    if (mNetConnection) {
        mNetConnection->SetMaxReadAhead(mMaxReadAhead);
    }
    if (mContentLength > 0) {
        IOBuffer::IStream is(iobuf, mContentLength);
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
        iobuf.Consume(mContentLength);
        mContentLength = 0;
    }
    mLastRecvCmdTime = globalNetManager().Now();
    op->clnt = this;
    KFS_LOG_STREAM_DEBUG <<
        "recv meta cmd:"
        " seq: " << op->seq <<
        " "      << op->Show() <<
    KFS_LOG_EOM;
    if (! mAuthOp && op->op == CMD_HEARTBEAT &&
            static_cast<HeartbeatOp*>(op)->authenticateFlag) {
        if (Authenticate() && ! IsUp()) {
            delete op;
            return false;
        }
    }
    SubmitOp(op);
    return true;
}

void
MetaServerSM::EnqueueOp(KfsOp* op)
{
    op->seq = nextSeq();
    if (! mAuthOp && mPendingOps.empty() && IsUp()) {
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
        if (globalNetManager().IsRunning() && mLocation.IsValid()) {
            mPendingOps.push_back(op);
        } else {
            op->status = -EHOSTUNREACH;
            SubmitOpResponse(op);
            return;
        }
    }
    globalNetManager().Wakeup();
}

///
/// Queue the response to the meta server request.  The response is
/// generated by MetaRequest as per the protocol.
/// @param[in] op The request for which we finished execution.
///

bool
MetaServerSM::SendResponse(KfsOp* op)
{
    if (! mSentHello || ! IsConnected()) {
        // Hello does full chunk inventory synchronization.
        // Meta server assumes undefined state for all requests that were in
        // in flight at the time of disconnect, and will discard the responses
        // anyway, as it will purge its pending response queue at the time of
        // disconnect.
        return true;
    }
    if (mAuthOp) {
        mPendingResponses.push_back(op);
        return false;
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
    IOBuffer* iobuf = 0;
    int       len   = 0;
    op->ResponseContent(iobuf, len);
    mNetConnection->Write(iobuf, len);
    globalNetManager().Wakeup();
    return true;
}

void
MetaServerSM::DispatchOps()
{
    OpsQueue doneOps;
    while (! mPendingOps.empty() && ! mAuthOp && IsHandshakeDone()) {
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

void
MetaServerSM::HandleAuthResponse(IOBuffer& ioBuf)
{
    if (! mAuthOp || ! mNetConnection) {
        die("handle auth response: invalid invocation");
        delete mAuthOp;
        mAuthOp = 0;
        HandleRequest(EVENT_NET_ERROR, 0);
        return;
    }
    const int rem = mAuthOp->ReadResponseContent(ioBuf);
    if (0 < rem) {
        // Attempt to read more to detect protocol errors.
        mNetConnection->SetMaxReadAhead(rem + mMaxReadAhead);
        return;
    }
    if (! ioBuf.IsEmpty()) {
        KFS_LOG_STREAM_ERROR <<
            "authentication protocol failure:" <<
            " " << ioBuf.BytesConsumable() <<
            " bytes past authentication response" <<
            " filter: " <<
                reinterpret_cast<const void*>(mNetConnection->GetFilter()) <<
            " cmd: " << mAuthOp->Show() <<
        KFS_LOG_EOM;
        if (! mAuthOp->statusMsg.empty()) {
            mAuthOp->statusMsg += "; ";
        }
        mAuthOp->statusMsg += "invalid extraneous data received";
        mAuthOp->status    = -EINVAL;
    } else if (mAuthOp->status == 0) {
        if (mNetConnection->GetFilter()) {
            if (IsHandshakeDone()) {
                // Shutdown the current filter.
                mNetConnection->Shutdown();
                return;
            }
            if (! mAuthOp->statusMsg.empty()) {
                mAuthOp->statusMsg += "; ";
            }
            mAuthOp->statusMsg += "authentication protocol failure:"
                "  filter exists prior to handshake completion";
            mAuthOp->status = -EINVAL;
        } else {
            mAuthOp->status = mAuthContext.Response(
                mAuthOp->chosenAuthType,
                mAuthOp->useSslFlag,
                mAuthOp->responseBuf,
                mAuthOp->responseContentLength,
                *mNetConnection,
                mAuthRequestCtx,
                &mAuthOp->statusMsg
            );
        }
    }
    const bool okFlag = mAuthOp->status == 0;
    KFS_LOG_STREAM(okFlag ?
            MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
        "finished: " << mAuthOp->Show() <<
        " filter: "  <<
            reinterpret_cast<const void*>(mNetConnection->GetFilter()) <<
    KFS_LOG_EOM;
    delete mAuthOp;
    mAuthOp = 0;
    if (! okFlag) {
        HandleRequest(EVENT_NET_ERROR, 0);
        return;
    }
    if (IsHandshakeDone()) {
        while (! mPendingResponses.empty()) {
            KfsOp* const op = mPendingResponses.front();
            mPendingResponses.pop_front();
            if (! SendResponse(op)) {
                die("invalid send response completion");
                HandleRequest(EVENT_NET_ERROR, 0);
                return;
            }
            delete op;
        }
        if (! mPendingOps.empty()) {
            globalNetManager().Wakeup();
        }
        return;
    }
    if (mHelloOp) {
        die("hello op in flight prior to authentication completion");
        HandleRequest(EVENT_NET_ERROR, 0);
        return;
    }
    if (! mPendingResponses.empty()) {
        die("non empty pending responses");
        DiscardPendingResponses();
    }
    mHelloOp = new HelloMetaOp(
        nextSeq(), gChunkServer.GetLocation(), mClusterKey, mMD5Sum, mRackId);
    mHelloOp->sendCurrentKeyFlag = true;
    mHelloOp->noFidsFlag = mNoFidsFlag;
    mHelloOp->clnt       = this;
    // Send the op and wait for the reply.
    SubmitOp(mHelloOp);
}

void
MetaServerSM::DiscardPendingResponses()
{
    while (! mPendingResponses.empty()) {
        KfsOp* const op = mPendingResponses.front();
        mPendingResponses.pop_front();
        delete op;
    }
}

} // namespace KFS
