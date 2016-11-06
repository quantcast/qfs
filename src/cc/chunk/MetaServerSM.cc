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

#include "MetaServerSM.h"
#include "ChunkManager.h"
#include "ChunkServer.h"
#include "utils.h"
#include "LeaseClerk.h"
#include "Replicator.h"

#include "common/kfserrno.h"
#include "common/MsgLogger.h"
#include "common/StdAllocator.h"
#include "common/SingleLinkedQueue.h"

#include "kfsio/NetManager.h"
#include "kfsio/Globals.h"
#include "kfsio/KfsCallbackObj.h"
#include "kfsio/ITimeout.h"
#include "kfsio/NetConnection.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/ClientAuthContext.h"

#include "qcdio/QCUtils.h"

#include <map>
#include <algorithm>
#include <sstream>
#include <utility>

namespace KFS
{
using std::string;
using std::map;
using std::less;
using std::pair;
using std::ostringstream;
using std::istringstream;
using std::make_pair;
using std::string;
using std::max;
using KFS::libkfsio::globalNetManager;

class MetaServerSM::Impl : public KfsCallbackObj, private ITimeout
{
public:
    Impl(
        Counters& inCounters,
        OpsQueue& inPendingOps,
        Impl*&    inPrimary,
        bool      inUpdateServerIpFlag,
        int64_t   inChannelId);
    ~Impl();

    /// In each hello to the metaserver, we send an MD5 sum of the
    /// binaries.  This should be "acceptable" to the metaserver and
    /// only then is the chunkserver allowed in.  This provides a
    /// simple mechanism to identify nodes that didn't receive a
    /// binary update or are running versions that the metaserver
    /// doesn't know about and shouldn't be inlcuded in the system.
    int SetMetaInfo(const ServerLocation& metaLoc, const string& clusterKey,
        int rackId, const string& md5sum, const Properties& prop);

    void EnqueueOp(KfsOp* op);

    /// If the connection to the server breaks, periodically, retry to
    /// connect; also dispatch ops.
    virtual void Timeout();

    /// Return the meta server name/port information
    const ServerLocation& GetLocation() const {
        return mLocation;
    }

    kfsSeq_t nextSeq() {
        return mCmdSeq++;
    }

    time_t GetLastRecvCmdTime() const {
        return mLastRecvCmdTime;
    }

    bool IsConnected() const {
        return (mNetConnection && mNetConnection->IsGood());
    }

    bool IsHandshakeDone() const {
        return (mSentHello && ! mHelloOp);
    }

    bool IsUp() const {
        return (IsConnected() && IsHandshakeDone());
    }

    time_t ConnectionUptime() const;

    void Reconnect() {
        mReconnectFlag = true;
    }

    int SetParameters(const Properties& prop);

    bool IsAuthEnabled() const {
        return mAuthContext.IsEnabled();
    }

    void Shutdown();

    void ForceDown(const char* msg = 0);

    uint64_t GetGenerationCount() const {
        return mGenerationCount;
    }

    const KfsOp* FindInFlightOp(seq_t seq) const {
        DispatchedOps::const_iterator const it = mDispatchedOps.find(seq);
        return (mDispatchedOps.end() == it ? 0 : it->second);
    }
private:
    typedef MetaServerSM::OpsQueue OpsQueue;
    typedef OpsQueue               PendingResponses;
    typedef map<
        kfsSeq_t,
        KfsOp*,
        less<kfsSeq_t>,
        StdFastAllocator<
            pair<const kfsSeq_t, KfsOp*>
        >
    > DispatchedOps;

    kfsSeq_t mCmdSeq;
    /// where is the meta server located?
    ServerLocation mLocation;

    /// An id that specifies the rack on which the server is located;
    /// this is used to do rack-aware replication
    int mRackId;

    /// "shared secret" key for this cluster.  This is used to prevent
    /// config mishaps: When we connect to a metaserver, we have to
    /// agree on the cluster key.
    string mClusterKey;

    /// An MD5 sum computed over the binaries that we send to the metaserver.
    string mMD5Sum;

    /// the port that the metaserver tells the clients to connect to us at.
    int mChunkServerPort;

    /// the hostname to use for discovering our IP address
    /// (instead of using gethostname)
    string mChunkServerHostname;

    /// Track if we have sent a "HELLO" to metaserver
    bool mSentHello;

    HelloMetaOp*    mHelloOp;
    AuthenticateOp* mAuthOp;

    /// list of ops that need to be dispatched:
    OpsQueue& mPendingOps;

    /// ops that we have sent to metaserver and are waiting for reply.
    DispatchedOps mDispatchedOps;

    /// Our connection to the meta server.
    NetConnectionPtr mNetConnection;

    /// A timer to periodically check that the connection to the
    /// server is good; if the connection broke, reconnect and do the
    /// handshake again.  Also, we use the timeout to dispatch pending
    /// messages to the server.
    int                           mInactivityTimeout;
    int                           mMaxReadAhead;
    time_t                        mLastRecvCmdTime;
    time_t                        mLastConnectTime;
    time_t                        mConnectedTime;
    bool                          mReconnectFlag;
    ClientAuthContext             mAuthContext;
    ClientAuthContext::RequestCtx mAuthRequestCtx;
    int                           mAuthType;
    string                        mAuthTypeStr;
    kfsKeyId_t                    mCurrentKeyId;
    bool                          mUpdateCurrentKeyFlag;
    bool                          mNoFidsFlag;
    int                           mHelloResume;
    KfsOp*                        mOp;
    bool                          mRequestFlag;
    bool                          mTraceRequestResponseFlag;
    bool const                    mUpdateServerIpFlag;
    RpcFormat                     mRpcFormat;
    int                           mContentLength;
    int64_t const                 mChannelId;
    uint64_t                      mGenerationCount;
    size_t                        mMaxPendingOpsCount;
    PendingResponses              mPendingResponses;
    int                           mReconnectRetryInterval;
    Counters&                     mCounters;
    MetaServerSM::Impl*&          mPrimary;
    IOBuffer::IStream             mIStream;
    IOBuffer::WOStream            mWOStream;

    /// Generic event handler to handle RPC requests sent by the meta server.
    int HandleRequest(int code, void* data);

    bool HandleMsg(IOBuffer& iobuf, int msgLen);

    /// Send HELLO message.  This sends an op down to the event
    /// processor to get all the info.
    void SendHello();

    /// Connect to the meta server
    /// @retval 0 if connect was successful; -1 otherwise
    int Connect();

    /// Given a (possibly) complete op in a buffer, run it.
    bool HandleCmd(IOBuffer& iobuf, int cmdLen);
    /// Handle a reply to an RPC we previously sent.
    bool HandleReply(IOBuffer& iobuf, int msgLen);

    /// Op has finished execution.  Send a response to the meta
    /// server.
    bool SendResponse(KfsOp *op);

    /// This is special: we dispatch mHelloOp and get rid of it.
    void DispatchHello();

    /// Submit all the enqueued ops
    void DispatchOps();

    void Error(const char* msg);
    void FailOps(bool wasPrimaryFlag);
    void HandleAuthResponse(IOBuffer& ioBuf);
    void CleanupOpInFlight();
    bool Authenticate();
    void DiscardPendingResponses();
    void SubmitHello();
    void Request(KfsOp& op);
    template<typename T> inline void DetachAndDeleteOp(T*& op);
private:
    // No copy.
    Impl(const Impl&);
    Impl& operator=(const Impl&);
};

inline void
MetaServerSM::SetPrimary(
    MetaServerSM::Impl& primary)
{
    if (&primary == mPrimary) {
        return;
    }
    Impl* const prevPrimary = mPrimary;
    if (mPrimary) {
        mPrimary->ForceDown("no longer primary");
        if (mPrimary) {
            die("set meta server primary failure");
        }
    }
    mPrimary = &primary;
    for (int i = 0; i < mImplCount; i++) {
        if (mPrimary != mImpls + i && prevPrimary != mImpls + i) {
            mImpls[i].ForceDown("not primary");
        }
    }
}

template<typename T> inline void
MetaServerSM::Impl::DetachAndDeleteOp(T*& op)
{
    if (op == mOp) {
        mOp = 0;
    }
    T* const cur = op;
    op = 0;
    delete cur;
}

MetaServerSM::Impl::Impl(
    MetaServerSM::Counters& inCounters,
    MetaServerSM::OpsQueue& inPendingOps,
    MetaServerSM::Impl*&    inPrimary,
    bool                    inUpdateServerIpFlag,
    int64_t                 inChannelId)
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
      mPendingOps(inPendingOps),
      mDispatchedOps(),
      mNetConnection(),
      mInactivityTimeout(65),
      mMaxReadAhead(4 << 10),
      mLastRecvCmdTime(globalNetManager().Now() - 24 * 60 * 60),
      mLastConnectTime(mLastRecvCmdTime),
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
      mHelloResume(-1),
      mOp(0),
      mRequestFlag(false),
      mTraceRequestResponseFlag(false),
      mUpdateServerIpFlag(inUpdateServerIpFlag),
      mRpcFormat(kRpcFormatUndef),
      mContentLength(0),
      mChannelId(inChannelId),
      mGenerationCount(1),
      mMaxPendingOpsCount(96),
      mPendingResponses(),
      mReconnectRetryInterval(1),
      mCounters(inCounters),
      mPrimary(inPrimary),
      mIStream(),
      mWOStream()
{
    SetHandler(this, &MetaServerSM::Impl::HandleRequest);
}

MetaServerSM::Impl::~Impl()
{
    MetaServerSM::Impl::Shutdown();
}

void
MetaServerSM::Impl::CleanupOpInFlight()
{
    if (mRequestFlag && mOp != mHelloOp && mOp != mAuthOp) {
        delete mOp;
    }
    mOp = 0;
}

int
MetaServerSM::Impl::SetMetaInfo(
    const ServerLocation& metaLoc,
    const string&         clusterKey,
    int                   rackId,
    const string&         md5sum,
    const Properties&     prop)
{
    if (! metaLoc.IsValid()) {
        die("meta server location is not valid");
    }
    if (! mLocation.IsValid()) {
        globalNetManager().RegisterTimeoutHandler(this);
    }
    mLocation   = metaLoc;
    mClusterKey = clusterKey;
    mRackId     = rackId;
    mMD5Sum     = md5sum;
    return SetParameters(prop);
}

void
MetaServerSM::Impl::Shutdown()
{
    if (! mLocation.IsValid() && ! mNetConnection) {
        return;
    }
    const bool wasPrimaryFlag = this == mPrimary;
    if (wasPrimaryFlag) {
        mPrimary = 0;
    }
    if (mNetConnection) {
        mNetConnection->Close();
    }
    mGenerationCount++;
    mNetConnection.reset();
    globalNetManager().UnRegisterTimeoutHandler(this);
    if (mLocation.IsValid()) {
        mLocation.port = -mLocation.port;
    }
    CleanupOpInFlight();
    DiscardPendingResponses();
    FailOps(wasPrimaryFlag);
    mSentHello = false;
    DetachAndDeleteOp(mHelloOp);
    DetachAndDeleteOp(mAuthOp);
    mAuthContext.Clear();
}

void
MetaServerSM::Impl::ForceDown(
    const char* msg)
{
    if (mNetConnection) {
        Error(msg ? msg : "protocol error");
    }
}

int
MetaServerSM::Impl::SetParameters(const Properties& prop)
{
    mReconnectRetryInterval   = prop.getValue(
        "chunkServer.meta.reconnectRetryInterval", mReconnectRetryInterval);
    mInactivityTimeout        = prop.getValue(
        "chunkServer.meta.inactivityTimeout", mInactivityTimeout);
    mMaxReadAhead      = prop.getValue(
        "chunkServer.meta.maxReadAhead",      mMaxReadAhead);
    mNoFidsFlag               = prop.getValue(
        "chunkServer.meta.noFids",            mNoFidsFlag ? 1 : 0) != 0;
    mHelloResume              = prop.getValue(
        "chunkServer.meta.helloResume",       mHelloResume);
    mTraceRequestResponseFlag = prop.getValue(
        "chunkServer.meta.traceRequestResponseFlag",
        mTraceRequestResponseFlag ? 1 : 0) != 0;
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
MetaServerSM::Impl::Timeout()
{
    if (mPrimary && this != mPrimary) {
        if (IsConnected()) {
            Error("no longer primary");
        }
        return;
    }
    if (mReconnectFlag) {
        mReconnectFlag = false;
        const char* const msg = "meta server reconnect requested";
        KFS_LOG_STREAM_WARN << msg << KFS_LOG_EOM;
        Error(msg);
    }
    const time_t now = globalNetManager().Now();
    if (IsConnected() &&
            IsHandshakeDone() &&
            mLastRecvCmdTime + mInactivityTimeout < now) {
        KFS_LOG_STREAM_ERROR <<
            "meta server inactivity timeout, last request received: " <<
            (now - mLastRecvCmdTime) << " secs ago" <<
        KFS_LOG_EOM;
        Error("heartbeat request timeout");
    }
    if (! IsConnected()) {
        if (mHelloOp) {
            if (! mSentHello) {
                return; // Wait for hello to come back.
            }
            mSentHello = false;
            DetachAndDeleteOp(mHelloOp);
        }
        if (mLastConnectTime + mReconnectRetryInterval < now) {
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
MetaServerSM::Impl::ConnectionUptime() const
{
    return (IsUp() ? (globalNetManager().Now() - mLastConnectTime) : 0);
}

int
MetaServerSM::Impl::Connect()
{
    if (mHelloOp) {
        return 0;
    }
    CleanupOpInFlight();
    DetachAndDeleteOp(mAuthOp);
    DiscardPendingResponses();
    mContentLength = 0;
    mCounters.mConnectCount++;
    mGenerationCount++;
    mRpcFormat            = kRpcFormatUndef;
    mSentHello            = false;
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

void
MetaServerSM::Impl::SendHello()
{
    if (mHelloOp || mAuthOp) {
        return;
    }
    if (! IsConnected()) {
        KFS_LOG_STREAM_DEBUG <<
            "unable to connect to meta server" <<
        KFS_LOG_EOM;
        if (mNetConnection) {
            Error("network error");
        }
        return;
    }
    if (gChunkServer.CanUpdateServerIp() &&
            (! mPrimary || this == mPrimary) &&
            (mUpdateServerIpFlag ||
            ! gChunkServer.GetLocation().IsValid())) {
        // Advertise the same ip address to the clients, as used
        // for the meta connection.
        ServerLocation loc;
        const int res = mNetConnection->GetSockLocation(loc);
        if (res < 0) {
            KFS_LOG_STREAM_ERROR <<
                "getsockname: " << QCUtils::SysError(-res) <<
            KFS_LOG_EOM;
            Error("get socket name error");
            return;
        }
        // Paper over for cygwin / win 7 with no nics configured:
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
            Error("invalid socket address");
            return;
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
        SubmitHello();
    }
    return;
}

bool
MetaServerSM::Impl::Authenticate()
{
    if (! mAuthContext.IsEnabled()) {
        return false;
    }
    if (mAuthOp) {
        die("invalid authenticate invocation: auth is in flight");
        return true;
    }
    mAuthOp = new AuthenticateOp();
    mAuthOp->seq                = nextSeq();
    mAuthOp->reqShortRpcFmtFlag = kRpcFormatShort != mRpcFormat;
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
        DetachAndDeleteOp(mAuthOp);
        Error("authentication error");
        return true;
    }
    Request(*mAuthOp);
    KFS_LOG_STREAM_INFO << "started: " << mAuthOp->Show() << KFS_LOG_EOM;
    return true;
}

void
MetaServerSM::Impl::DispatchHello()
{
    if (mSentHello || mAuthOp) {
        die("dispatch hello: invalid invocation");
        Error("internal error");
        return;
    }
    if (! IsConnected()) {
        // don't have a connection...so, need to start the process again...
        mSentHello            = false;
        mUpdateCurrentKeyFlag = false;
        DetachAndDeleteOp(mAuthOp);
        DetachAndDeleteOp(mHelloOp);
        return;
    }
    mSentHello = true;
    Request(*mHelloOp);
    KFS_LOG_STREAM_INFO <<
        "sending hello to meta server: " << mHelloOp->Show() <<
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
MetaServerSM::Impl::HandleRequest(int code, void* data)
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
                Error("protocol parse error");
                break;
            }
            if (this == mPrimary && ! mPendingOps.IsEmpty()) {
                DispatchOps();
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

    case EVENT_NET_ERROR:
        if (mAuthOp && ! mOp && IsUp() && ! mNetConnection->GetFilter()) {
            HandleAuthResponse(mNetConnection->GetInBuffer());
            break;
        }
    // Fall through.
    case EVENT_INACTIVITY_TIMEOUT:
        Error(code == EVENT_INACTIVITY_TIMEOUT ?
            "inactivity timeout" : "network error");
        break;

    default:
        die("meta server state machine: unknown event");
        break;
    }
    return 0;
}

void
MetaServerSM::Impl::Error(const char* msg)
{
    const bool wasPrimaryFlag = this == mPrimary;
    if (wasPrimaryFlag) {
        mPrimary = 0;
    }
    mGenerationCount++;
    CleanupOpInFlight();
    DetachAndDeleteOp(mAuthOp);
    DiscardPendingResponses();
    if (mNetConnection) {
        KFS_LOG_STREAM((gMetaServerSM.IsRunning() && wasPrimaryFlag) ?
                MsgLogger::kLogLevelERROR :
                MsgLogger::kLogLevelDEBUG) <<
            mLocation <<
            " closing meta server connection due to " <<
            (msg ? msg : "error") <<
        KFS_LOG_EOM;
        mNetConnection->Close();
        mNetConnection->GetInBuffer().Clear();
        if (wasPrimaryFlag) {
            // Drop all leases.
            gLeaseClerk.UnregisterAllLeases();
            // Meta server will fail all replication requests on disconnect
            // anyway.
            Replicator::CancelAll();
            gChunkManager.MetaServerConnectionLost();
        }
    }
    FailOps(wasPrimaryFlag);
    mSentHello = false;
    DetachAndDeleteOp(mHelloOp);
}

void
MetaServerSM::Impl::FailOps(bool wasPrimaryFlag)
{
    // Fail all no retry ops, if any, or all ops on shutdown.
    OpsQueue doneOps;
    for (DispatchedOps::const_iterator it = mDispatchedOps.begin();
            it != mDispatchedOps.end();
            ++it) {
        doneOps.PushBack(*(it->second));
    }
    mDispatchedOps.clear();
    if (wasPrimaryFlag) {
        doneOps.PushBack(mPendingOps);
    }
    for (; ;) {
        KfsOp* op;
        while ((op = doneOps.PopFront())) {
            op->status = -EHOSTUNREACH;
            SubmitOpResponse(op);
        }
        if (mPendingOps.IsEmpty()) {
            break;
        }
        doneOps.PushBack(mPendingOps);
    }
}

bool
MetaServerSM::Impl::HandleMsg(IOBuffer& iobuf, int msgLen)
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
MetaServerSM::Impl::HandleReply(IOBuffer& iobuf, int msgLen)
{
    DispatchedOps::iterator iter = mDispatchedOps.end();
    KfsOp* op = mOp;
    if (op) {
        mOp = 0;
    } else {
        if (mTraceRequestResponseFlag) {
            IOBuffer::IStream is(iobuf, msgLen);
            string            line;
            while (getline(is, line)) {
                KFS_LOG_STREAM_DEBUG << reinterpret_cast<const void*>(this) <<
                    " " << mLocation << " meta response: " << line <<
                KFS_LOG_EOM;
            }
        }
        Properties prop(kRpcFormatShort == mRpcFormat ? 16 : 10);
        const char separator = ':';
        prop.loadProperties(mIStream.Set(iobuf, msgLen), separator);
        mIStream.Reset();
        iobuf.Consume(msgLen);
        if (kRpcFormatUndef == mRpcFormat && (
                (mHelloOp && mHelloOp->reqShortRpcFmtFlag) ||
                (mAuthOp && mAuthOp->reqShortRpcFmtFlag))) {
            if (! prop.getValue("Cseq") && prop.getValue("c")) {
                mRpcFormat = kRpcFormatShort;
                KfsOp& cur = *(mAuthOp ?
                    static_cast<KfsOp*>(mAuthOp) :
                    static_cast<KfsOp*>(mHelloOp));
                cur.initialShortRpcFormatFlag = true;
                cur.shortRpcFormatFlag        = true;
                prop.setIntBase(16);
            }
        }
        const kfsSeq_t seq    = prop.getValue(
            kRpcFormatShort == mRpcFormat ? "c" : "Cseq", kfsSeq_t(-1));
        int            status = prop.getValue(
            kRpcFormatShort == mRpcFormat ? "s" : "Status",    int(-1));
        string         statusMsg;
        if (status < 0) {
            status    = -KfsToSysErrno(-status);
            statusMsg = prop.getValue(
                kRpcFormatShort == mRpcFormat ? "m" : "Status-message",
                string());
        }
        mContentLength = prop.getValue(
            kRpcFormatShort == mRpcFormat ? "l" : "Content-length",  -1);
        if (mAuthOp && (! IsHandshakeDone() || seq == mAuthOp->seq)) {
            if (seq != mAuthOp->seq) {
                KFS_LOG_STREAM_ERROR <<
                    "authentication response seq number mismatch: " <<
                    seq << "/" << mAuthOp->seq <<
                    " " << mAuthOp->Show() <<
                KFS_LOG_EOM;
                Error("authentication protocol error");
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
                Error("invalid meta server response");
                return false;
            }
            HandleAuthResponse(iobuf);
            return false;
        }
        if (mHelloOp) {
            if (status == -EBADCLUSTERKEY) {
                KFS_LOG_STREAM_FATAL <<
                    (statusMsg.empty() ?
                        ErrorCodeToString(status) : statusMsg) <<
                    " cluster key: " << mClusterKey <<
                    " md5sum: "      << mMD5Sum <<
                KFS_LOG_EOM;
                globalNetManager().Shutdown();
                return false;
            }
            mCounters.mHelloCount++;
            const int resumeStep = status == 0 ?
                prop.getValue(kRpcFormatShort == mRpcFormat ? "R" : "Resume",
                    int(-1)) : -1;
            const bool errorFlag =
                seq != mHelloOp->seq ||
                (status != 0 && 0 < mContentLength) ||
                (mHelloOp->resumeStep != 0 && 0 < mContentLength) ||
                (mHelloOp->resumeStep < 0 && status != 0) ||
                (0 <= mHelloOp->resumeStep &&
                    (status != 0 && status != -EAGAIN)) ||
                (0 <= mHelloOp->resumeStep && status == 0 &&
                    resumeStep != mHelloOp->resumeStep);
            if (errorFlag) {
                KFS_LOG_STREAM_ERROR <<
                    "hello response error:"
                    " seq: "         << seq << " => " << mHelloOp->seq <<
                    " status: "      << status <<
                    " msg: "         << statusMsg <<
                    " resume: "      << mHelloOp->resumeStep <<
                    " / "            << resumeStep <<
                    " content len: " << mContentLength <<
                KFS_LOG_EOM;
                mCounters.mHelloErrorCount++;
            } else if (status == 0) {
                mHelloOp->metaFileSystemId      = prop.getValue(
                    kRpcFormatShort == mRpcFormat ? "FI" : "File-system-id",
                        int64_t(-1));
                const int64_t deleteAllChunksId = prop.getValue(
                    kRpcFormatShort == mRpcFormat ? "DA" : "Delete-all-chunks",
                        (int64_t)-1);
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
                mHelloOp->deletedCount  = prop.getValue(
                    kRpcFormatShort == mRpcFormat ? "D" : "Deleted",
                        uint64_t(0));
                mHelloOp->modifiedCount = prop.getValue(
                    kRpcFormatShort == mRpcFormat ? "M" : "Modified",
                        uint64_t(0));
                mHelloOp->chunkCount    = prop.getValue(
                    kRpcFormatShort == mRpcFormat ? "C" : "Chunks",
                        uint64_t(0));
                const Properties::String* const cs = prop.getValue(
                    kRpcFormatShort == mRpcFormat ? "K" : "Checksum");
                const char* csp = cs ? cs->GetPtr() : 0;
                if (! cs || ! (kRpcFormatShort == mRpcFormat ?
                        mHelloOp->checksum.Parse<HexIntParser>(
                            csp, cs->GetSize()) :
                        mHelloOp->checksum.Parse<DecIntParser>(
                            csp, cs->GetSize()))) {
                    mHelloOp->checksum.Clear();
                }
                mHelloOp->deletedReport = prop.getValue(
                    kRpcFormatShort == mRpcFormat ? "DR": "Deleted-report",
                        mHelloOp->deletedCount);
                mHelloOp->pendingNotifyFlag = prop.getValue(
                    kRpcFormatShort == mRpcFormat ? "PN" : "Pending-notify",
                    0) != 0;
                mMaxPendingOpsCount = (size_t)max(1, prop.getValue(
                    kRpcFormatShort == mRpcFormat ? "MP" : "Max-pending",
                    96));
            } else {
                mHelloOp->resumeStep = -1;
                mSentHello    = false;
                mHelloOp->seq = nextSeq(),
                SubmitOp(mHelloOp); // Re-submit hello.
                return true;
            }
            if (errorFlag || mHelloOp->resumeStep != 0) {
                mUpdateCurrentKeyFlag =
                    ! errorFlag && mHelloOp->sendCurrentKeyFlag;
                if (mUpdateCurrentKeyFlag) {
                    mCurrentKeyId = mHelloOp->currentKeyId;
                }
                if (errorFlag) {
                    Error("handshake error");
                    return false;
                }
                mConnectedTime = globalNetManager().Now();
                HelloMetaOp::LostChunkDirs lostDirs;
                lostDirs.swap(mHelloOp->lostChunkDirs);
                DetachAndDeleteOp(mHelloOp);
                if (IsUp()) {
                    gMetaServerSM.SetPrimary(*this);
                    mCounters.mHelloDoneCount++;
                    for (HelloMetaOp::LostChunkDirs::const_iterator
                            it = lostDirs.begin();
                            it != lostDirs.end() && IsConnected();
                            ++it) {
                        EnqueueOp(new CorruptChunkOp(-1, &(*it), false));
                    }
                    DispatchOps();
                }
                return true;
            }
            op = mHelloOp;
            assert(op);
        } else {
            iter = mDispatchedOps.find(seq);
            if (iter == mDispatchedOps.end()) {
                string reply;
                prop.getList(reply, string(), string(" "));
                KFS_LOG_STREAM_ERROR << "meta reply:"
                    " no op found for: " << reply <<
                KFS_LOG_EOM;
                Error("protocol invalid sequence");
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
                Error("meta response parse error");
                return false;
            }
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
                " msg: "         << op->statusMsg <<
                " "              << op->Show() <<
                " content len: " << len <<
            KFS_LOG_EOM;
            Error("response body parse error");
            return false;
        }
    }
    if (op == mHelloOp) {
        if (mHelloOp->resumeStep == 0) {
            mHelloOp->resumeStep = 1;
        }
        mSentHello    = false;
        mHelloOp->seq = nextSeq(),
        SubmitOp(mHelloOp); // Re-submit hello.
        return true;
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
MetaServerSM::Impl::HandleCmd(IOBuffer& iobuf, int cmdLen)
{
    KfsOp* op = mOp;
    mOp = 0;
    if (! op) {
        if (ParseMetaCommand(iobuf, cmdLen, &op, mRpcFormat) != 0) {
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
            Error("request parse error");
            // got a bogus command
            return false;
        }
        if (mTraceRequestResponseFlag) {
            IOBuffer::IStream is(iobuf, cmdLen);
            string            line;
            while (getline(is, line)) {
                KFS_LOG_STREAM_DEBUG << reinterpret_cast<const void*>(this) <<
                    " " << mLocation << " meta request: " << line <<
                KFS_LOG_EOM;
            }
        }
        iobuf.Consume(cmdLen);
        op->generation = mGenerationCount;
    }
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
    if (0 < mContentLength) {
        IOBuffer::IStream is(iobuf, mContentLength);
        if (! op->ParseContent(is)) {
            KFS_LOG_STREAM_ERROR <<
                (IsConnected() ?  mNetConnection->GetPeerName() : "") <<
                " invalid content: " << op->statusMsg <<
                " cmd: " << op->Show() <<
            KFS_LOG_EOM;
            delete op;
            Error("request body parse error");
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
    if (! mAuthOp && CMD_HEARTBEAT == op->op) {
        const HeartbeatOp& hb = *static_cast<HeartbeatOp*>(op);
        if (hb.authenticateFlag && Authenticate() && ! IsUp()) {
            delete op;
            return false;
        }
        mMaxPendingOpsCount = (size_t)max(1, hb.maxPendingOps);
    }
    SubmitOp(op);
    return true;
}

void
MetaServerSM::Impl::Request(KfsOp& op)
{
    op.shortRpcFormatFlag        = kRpcFormatShort == mRpcFormat;
    op.initialShortRpcFormatFlag = op.shortRpcFormatFlag;
    op.status                    = 0;
    KFS_LOG_STREAM_DEBUG <<
        "cs request:"
        " seq: " << op.seq <<
        " "      << op.Show() <<
    KFS_LOG_EOM;
    IOBuffer&  ioBuf = mNetConnection->GetOutBuffer();
    ReqOstream ros(mWOStream.Set(ioBuf));
    const int reqStart = ioBuf.BytesConsumable();
    op.Request(ros, ioBuf);
    mWOStream.Reset();
    if (mTraceRequestResponseFlag) {
        IOBuffer::IStream is(ioBuf, ioBuf.BytesConsumable());
        is.ignore(reqStart);
        const void* const self = this;
        string            line;
        KFS_LOG_STREAM_DEBUG << self <<
            " cs request: " << mLocation <<
        KFS_LOG_EOM;
        while (getline(is, line)) {
            KFS_LOG_STREAM_DEBUG << self <<
                " request: " << line <<
            KFS_LOG_EOM;
        }
    }
}

void
MetaServerSM::Impl::EnqueueOp(KfsOp* op)
{
    if (! mAuthOp && mPendingOps.IsEmpty() && IsUp() &&
            mDispatchedOps.size() < mMaxPendingOpsCount) {
        op->seq = nextSeq();
        if (! op->noReply &&
                ! mDispatchedOps.insert(make_pair(op->seq, op)).second) {
            die("duplicate seq. number");
        }
        Request(*op);
        if (op->noReply) {
            SubmitOpResponse(op);
        }
    } else {
        if (gMetaServerSM.IsRunning() && mLocation.IsValid()) {
            mPendingOps.PushBack(*op);
        } else {
            op->status = -EHOSTUNREACH;
            SubmitOpResponse(op);
            return;
        }
        globalNetManager().Wakeup();
    }
}

///
/// Queue the response to the meta server request.  The response is
/// generated by MetaRequest as per the protocol.
/// @param[in] op The request for which we finished execution.
///

bool
MetaServerSM::Impl::SendResponse(KfsOp* op)
{
    const bool discardFlag = ! mSentHello ||
        op->generation != mGenerationCount || ! IsConnected();
    KFS_LOG_STREAM_DEBUG <<
        (discardFlag ? "discard" : "send") <<
        " meta reply:"
        " seq: "     << op->seq <<
        (op->statusMsg.empty() ? "" : " msg: ") << op->statusMsg <<
        " status: "  << op->status <<
        " "          << op->Show() <<
    KFS_LOG_EOM;
    if (discardFlag) {
        // Hello does chunk inventory synchronization.
        // Meta server assumes undefined state for all requests that were in
        // in flight at the time of disconnect, and will discard the responses
        // anyway, as it will purge its pending response queue at the time of
        // disconnect.
        return true;
    }
    if (mAuthOp) {
        mPendingResponses.PushBack(*op);
        return false;
    }
    if (CMD_ALLOC_CHUNK == op->op) {
        mCounters.mAllocCount++;
        if (op->status < 0) {
            mCounters.mAllocErrorCount++;
        }
    }
    IOBuffer& buf    = mNetConnection->GetOutBuffer();
    const int reqPos = buf.BytesConsumable();
    ReqOstream ros(mWOStream.Set(buf));
    op->Response(ros);
    mWOStream.Reset();
    IOBuffer* iobuf = 0;
    int       len   = 0;
    op->ResponseContent(iobuf, len);
    mNetConnection->Write(iobuf, len);
    if (mTraceRequestResponseFlag) {
        IOBuffer::IStream is(buf, buf.BytesConsumable());
        is.ignore(reqPos);
        string line;
        while (getline(is, line)) {
            KFS_LOG_STREAM_DEBUG << reinterpret_cast<const void*>(this) <<
                " " << mLocation << " cs response: " << line <<
            KFS_LOG_EOM;
        }
    }
    return true;
}

void
MetaServerSM::Impl::DispatchOps()
{
    if (! IsUp() || mAuthOp || mPendingOps.IsEmpty() || this != mPrimary) {
        return;
    }
    OpsQueue doneOps;
    KfsOp*   op;
    size_t   cnt = mDispatchedOps.size();
    while (cnt < mMaxPendingOpsCount && (op = mPendingOps.PopFront())) {
        assert(CMD_META_HELLO != op->op);
        op->seq = nextSeq();
        if (op->noReply) {
            doneOps.PushBack(*op);
        } else if (! mDispatchedOps.insert(make_pair(op->seq, op)).second) {
            die("duplicate seq. number");
        }
        cnt++;
        Request(*op);
    }
    while ((op = doneOps.PopFront())) {
        SubmitOpResponse(op);
    }
}

void
MetaServerSM::Impl::HandleAuthResponse(IOBuffer& ioBuf)
{
    if (! mAuthOp || ! mNetConnection) {
        die("handle auth response: invalid invocation");
        DetachAndDeleteOp(mAuthOp);
        Error("internal error");
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
    DetachAndDeleteOp(mAuthOp);
    if (! okFlag) {
        Error("authentication protocol error");
        return;
    }
    if (IsHandshakeDone()) {
        KfsOp* op;
        while ((op = mPendingResponses.PopFront())) {
            if (! SendResponse(op)) {
                die("invalid send response completion");
                Error("internal error");
                return;
            }
            delete op;
        }
        if (! mPendingOps.IsEmpty()) {
            globalNetManager().Wakeup();
        }
        return;
    }
    if (mHelloOp) {
        die("hello op in flight prior to authentication completion");
        Error("internal error");
        return;
    }
    if (! mPendingResponses.IsEmpty()) {
        die("non empty pending responses");
        DiscardPendingResponses();
    }
    SubmitHello();
}

void
MetaServerSM::Impl::SubmitHello()
{
    if (mHelloOp) {
        die("invalid submit hello invocation");
        return;
    }
    mHelloOp = new HelloMetaOp(
        gChunkServer.GetLocation(), mClusterKey, mMD5Sum, mRackId, mChannelId);
    mHelloOp->seq                = nextSeq();
    mHelloOp->sendCurrentKeyFlag = true;
    mHelloOp->noFidsFlag         = mNoFidsFlag;
    mHelloOp->helloDoneCount     = mCounters.mHelloDoneCount;
    mHelloOp->resumeStep         = (mHelloResume < 0 ||
        (mHelloResume != 0 && 0 < mCounters.mHelloDoneCount)) ? 0 : -1;
    mHelloOp->clnt               = this;
    mHelloOp->shortRpcFormatFlag = kRpcFormatShort == mRpcFormat;
    mHelloOp->reqShortRpcFmtFlag = kRpcFormatShort != mRpcFormat;
    // Send the op and wait for the reply.
    SubmitOp(mHelloOp);
}

void
MetaServerSM::Impl::DiscardPendingResponses()
{
    KfsOp* op;
    while ((op = mPendingResponses.PopFront())) {
        delete op;
    }
}

MetaServerSM::MetaServerSM()
    : mCounters(),
      mRunningFlag(false),
      mAuthEnabledFlag(false),
      mAllowDuplicateLocationsFlag(false),
      mImplCount(0),
      mPendingOps(),
      mImpls(0),
      mPrimary(0),
      mLocations()
{
}

MetaServerSM::~MetaServerSM()
{
    MetaServerSM::Cleanup();
}

void
MetaServerSM::Cleanup()
{
    for (int i = 0; i < mImplCount; i++) {
        mImpls[i].~Impl();
    }
    mPrimary   = 0;
    mImplCount = 0;
    delete [] reinterpret_cast<char*>(mImpls);
    mImpls = 0;
}

bool
MetaServerSM::IsUp() const
{
    return (mPrimary && mPrimary->IsUp());
}

time_t
MetaServerSM::ConnectionUptime() const
{
    return (mPrimary ? mPrimary->ConnectionUptime() : time_t(0));
}

bool
MetaServerSM::IsInProgress(const KfsOp& op) const
{
    return (mPrimary && op.clnt == mPrimary &&
        op.generation == mPrimary->GetGenerationCount());
}

const KfsOp*
MetaServerSM::FindInFlightOp(seq_t seq) const
{
    return (mPrimary ? mPrimary->FindInFlightOp(seq) : 0);
}

ServerLocation
MetaServerSM::CetPrimaryLocation() const
{
    return (mPrimary ? mPrimary->GetLocation() : ServerLocation());
}

void
MetaServerSM::EnqueueOp(KfsOp* op)
{
    if (mPrimary) {
        mPrimary->EnqueueOp(op);
    } else {
        if (mRunningFlag) {
            mPendingOps.PushBack(*op);
            globalNetManager().Wakeup();
        } else {
            op->status = -EHOSTUNREACH;
            SubmitOpResponse(op);
        }
    }
}

int
MetaServerSM::SetParameters(const Properties& prop)
{
    int res = 0;
    for (int i = 0; i < mImplCount; i++) {
        const int ret = mImpls[i].SetParameters(prop);
        if (0 == res) {
            res = ret;
        }
    }
    mAllowDuplicateLocationsFlag = prop.getValue(
        "chunkServer.metaServer.allowDuplicateLocations",
        mAllowDuplicateLocationsFlag ? 1 : 0
    ) != 0;
    if (0 < mImplCount) {
        mAuthEnabledFlag = mImpls[0].IsAuthEnabled();
    }
    return res;
}

void
MetaServerSM::Reconnect()
{
    for (int i = 0; i < mImplCount; i++) {
        mImpls[i].Reconnect();
    }
}

void
MetaServerSM::Shutdown()
{
    mRunningFlag = false;
    for (int i = 0; i < mImplCount; i++) {
        mImpls[i].Shutdown();
    }
    OpsQueue doneOps;
    while (! mPendingOps.IsEmpty()) {
        doneOps.PushBack(mPendingOps);
        KfsOp* op;
        while ((op = doneOps.PopFront())) {
            op->status = -EHOSTUNREACH;
            SubmitOpResponse(op);
        }
    }
    Cleanup();
}

void
MetaServerSM::ForceDown()
{
    for (int i = 0; i < mImplCount; i++) {
        mImpls[i].ForceDown();
    }
}

int
MetaServerSM::SetMetaInfo(
    const string&     clusterKey,
    int               rackId,
    const string&     md5sum,
    const Properties& prop)
{
    if (0 < mImplCount || mRunningFlag) {
        KFS_LOG_STREAM_ERROR <<
            "meta server info already set" <<
        KFS_LOG_EOM;
        return -EINVAL;
    }
    SetParameters(prop);
    mLocations.clear();
    const char* const         kParamName = "chunkServer.metaServer.locations";
    const Properties::String* locs       = prop.getValue(kParamName);
    ServerLocation            loc;
    if (locs) {
        const char*       ptr = locs->data();
        const char* const end = ptr + locs->size();
        while (ptr < end) {
            const bool kHexFmtFlag = false;
            loc.Reset(0, -1);
            if (! loc.ParseString(ptr, end - ptr, kHexFmtFlag) ||
                    ! loc.IsValid()) {
                mLocations.clear();
                KFS_LOG_STREAM_FATAL <<
                    "invalid parameter: " << kParamName <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            if (mAllowDuplicateLocationsFlag ||
                    find(mLocations.begin(), mLocations.end(), loc) ==
                    mLocations.end()) {
                mLocations.push_back(loc);
            } else {
                mLocations.clear();
                KFS_LOG_STREAM_FATAL <<
                    "invalid parameter: "   << kParamName <<
                    " duplicate location: " << loc <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
        }
    } else {
        loc.hostname = prop.getValue(
            "chunkServer.metaServer.hostname", loc.hostname);
        loc.port     = prop.getValue(
            "chunkServer.metaServer.port",     loc.port);
        if (! loc.IsValid()) {
            KFS_LOG_STREAM_FATAL << "invalid meta server host or port: " <<
                loc.hostname << ':' << loc.port <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        mLocations.push_back(loc);
    }
    mPrimary   = 0;
    mImplCount = (int)mLocations.size();
    mImpls = reinterpret_cast<Impl*>(new char[sizeof(Impl) * mImplCount]);
    for (int i = 0; i < mImplCount; i++) {
        new (mImpls + i) Impl(
            mCounters, mPendingOps, mPrimary, mImplCount <= 1, i);
    }
    int res;
    for (int i = 0; i < mImplCount; i++) {
        res = mImpls[i].SetMetaInfo(
            mLocations[i], clusterKey, rackId, md5sum, prop);
        if (0 != res) {
            break;
        }
    }
    if (0 == res) {
        mRunningFlag     = true;
        mAuthEnabledFlag = mImpls[0].IsAuthEnabled();
    } else {
        Cleanup();
    }
    return res;
}

} // namespace KFS
