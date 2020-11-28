//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/07
// Author: Sriram Rao
//
// Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
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
#include "kfsio/Resolver.h"

#include "qcdio/QCUtils.h"
#include "qcdio/QCDLList.h"

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

const char* const kChunkServerAuthParamsPrefix = "chunkserver.meta.auth.";

class MetaServerSM::Impl : public KfsCallbackObj, private ITimeout
{
public:
    typedef QCDLList<Impl> List;

    Impl(
        Counters&     inCounters,
        OpsQueue&     inPendingOps,
        Impl*&        inPrimary,
        bool&         inUpdateServerIpFlag,
        int64_t       inChannelId,
        MetaServerSM* inOuter);

    void Delete() {
        if (mInFlightRequestCount <= 0) {
            delete this;
            return;
        }
        mDeleteFlag = true;
    }

    /// In each hello to the metaserver, we send an MD5 sum of the
    /// binaries.  This should be "acceptable" to the metaserver and
    /// only then is the chunkserver allowed in.  This provides a
    /// simple mechanism to identify nodes that didn't receive a
    /// binary update or are running versions that the metaserver
    /// doesn't know about and shouldn't be inlcuded in the system.
    int SetMetaInfo(const ServerLocation& metaLoc, const string& clusterKey,
        int inactivityTimeout, int rackId, const string& md5sum,
        const string& nodeId, const Properties& prop);

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

    int SetParameters(const Properties& prop, int inactivityTimeout);

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

    const ServerLocation& GetMyLocation() const {
        return mMyLocation;
    }
private:
    typedef OpsQueue PendingResponses;
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
    string mNodeId;

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
    int                           mReceiveTimeout;
    int                           mMaxReadAhead;
    bool                          mAbortOnRequestParseErrorFlag;
    time_t                        mLastRecvTime;
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
    bool                          mPendingDeleteFlag;
    bool                          mDeleteFlag;
    const bool&                   mUpdateServerIpFlag;
    RpcFormat                     mRpcFormat;
    int                           mContentLength;
    int64_t const                 mChannelId;
    uint64_t                      mGenerationCount;
    size_t                        mMaxPendingOpsCount;
    PendingResponses              mPendingResponses;
    int                           mInFlightRequestCount;
    int                           mRecursionCount;
    int                           mReconnectRetryInterval;
    int                           mHelloDelay;
    int64_t                       mRequestTimeoutUsec;
    bool                          mPendingHelloFlag;
    time_t                        mHelloSendTime;
    ServerLocation                mMyLocation;
    Counters&                     mCounters;
    MetaServerSM::Impl*&          mPrimary;
    IOBuffer::IStream             mIStream;
    IOBuffer::WOStream            mWOStream;
    MetaServerSM*                 mOuter;
    Impl*                         mPrevPtr[1];
    Impl*                         mNextPtr[1];

    ~Impl();
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
    void ScheduleSendHello()
    {
        if (mPendingHelloFlag) {
            return;
        }
        if (mAuthContext.IsEnabled() || mHelloDelay <= 0) {
            SendHello();
            return;
        }
        mPendingHelloFlag = true;
        mHelloSendTime    = globalNetManager().Now() + mHelloDelay;
    }

    friend class QCDLListOp<Impl>;
private:
    // No copy.
    Impl(const Impl&);
    Impl& operator=(const Impl&);
};

inline void
MetaServerSM::SetPrimary(
    MetaServerSM::Impl&   primary,
    const ServerLocation& loc)
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
    Impl::List::Iterator it(mImpls);
    Impl* ptr;
    while ((ptr = it.Next())) {
        if (mPrimary != ptr && prevPrimary != ptr) {
            if (mUpdateServerIpFlag) {
                const ServerLocation& myLoc = ptr->GetMyLocation();
                if (myLoc.IsValid() && loc != myLoc && ptr->IsConnected()) {
                    KFS_LOG_STREAM_INFO <<
                        "turning off chunk server ip update"
                        ": "  << myLoc <<
                        " / " << loc <<
                    KFS_LOG_EOM;
                    mUpdateServerIpFlag = false;
                }
            }
            ptr->ForceDown("not primary");
        }
    }
    if (loc != gChunkServer.GetLocation()) {
        gChunkServer.SetLocation(loc);
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
    bool&                   inUpdateServerIpFlag,
    int64_t                 inChannelId,
    MetaServerSM*           inOuter)
    : KfsCallbackObj(),
      ITimeout(),
      mCmdSeq(GetRandomSeq()),
      mLocation(),
      mRackId(-1),
      mClusterKey(),
      mMD5Sum(),
      mNodeId(),
      mChunkServerPort(-1),
      mChunkServerHostname(),
      mSentHello(false),
      mHelloOp(0),
      mAuthOp(0),
      mPendingOps(inPendingOps),
      mDispatchedOps(),
      mNetConnection(),
      mInactivityTimeout(40),
      mReceiveTimeout(24 * 60 * 60),
      mMaxReadAhead(4 << 10),
      mAbortOnRequestParseErrorFlag(false),
      mLastRecvTime(globalNetManager().Now() - 24 * 60 * 60),
      mLastConnectTime(mLastRecvTime),
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
      mPendingDeleteFlag(false),
      mDeleteFlag(false),
      mUpdateServerIpFlag(inUpdateServerIpFlag),
      mRpcFormat(kRpcFormatUndef),
      mContentLength(0),
      mChannelId(inChannelId),
      mGenerationCount(1),
      mMaxPendingOpsCount(96),
      mPendingResponses(),
      mInFlightRequestCount(0),
      mRecursionCount(0),
      mReconnectRetryInterval(1),
      mHelloDelay(1),
      mRequestTimeoutUsec(int64_t(6) * 60 * 1000 * 1000),
      mPendingHelloFlag(false),
      mHelloSendTime(0),
      mMyLocation(),
      mCounters(inCounters),
      mPrimary(inPrimary),
      mIStream(),
      mWOStream(),
      mOuter(inOuter)
{
    SetHandler(this, &MetaServerSM::Impl::HandleRequest);
    List::Init(*this);
    KFS_LOG_STREAM_DEBUG <<
        "MetaServerSM::Impl::Impl: " << (const void*)this <<
    KFS_LOG_EOM;
}

MetaServerSM::Impl::~Impl()
{
    KFS_LOG_STREAM_DEBUG <<
        "MetaServerSM::Impl::~Impl: " << (const void*)this <<
    KFS_LOG_EOM;
    MetaServerSM::Impl::Shutdown();
    if (0 != mRecursionCount || 0 != mInFlightRequestCount) {
        die("meta server state machine: invalid destructor invocation");
    }
    mRecursionCount = -1; // To catch double delete.
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
    int                   inactivityTimeout,
    int                   rackId,
    const string&         md5sum,
    const string&         nodeId,
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
    mNodeId     = nodeId;
    return SetParameters(prop, inactivityTimeout);
}

void
MetaServerSM::Impl::Shutdown()
{
    if (! mLocation.IsValid() && ! mNetConnection) {
        return;
    }
    KFS_LOG_STREAM_DEBUG <<
        "shutdown: " << mLocation <<
    KFS_LOG_EOM;
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
MetaServerSM::Impl::SetParameters(const Properties& prop, int inactivityTimeout)
{
    mInactivityTimeout            = inactivityTimeout;
    mReconnectRetryInterval       = prop.getValue(
        "chunkServer.meta.reconnectRetryInterval",
        mReconnectRetryInterval);
    mMaxReadAhead                 = prop.getValue(
        "chunkServer.meta.maxReadAhead",      mMaxReadAhead);
    mAbortOnRequestParseErrorFlag = prop.getValue(
        "chunkServer.meta.abortOnRequestParseError",
        mAbortOnRequestParseErrorFlag ? 1 : 0) != 0;
    mNoFidsFlag                   = prop.getValue(
        "chunkServer.meta.noFids",            mNoFidsFlag ? 1 : 0) != 0;
    mHelloResume                  = prop.getValue(
        "chunkServer.meta.helloResume",       mHelloResume);
    mTraceRequestResponseFlag = prop.getValue(
        "chunkServer.meta.traceRequestResponseFlag",
        mTraceRequestResponseFlag ? 1 : 0) != 0;
    mHelloDelay                   = prop.getValue(
        "chunkServer.meta.helloDelay",        mHelloDelay);
    mRequestTimeoutUsec           = max(int64_t(5), prop.getValue(
        "chunkServer.meta.requestTimeout",
        mRequestTimeoutUsec / (1000 * 1000))) * 1000 * 1000;
    const bool kVerifyFlag = true;
    int ret = mAuthContext.SetParameters(
        kChunkServerAuthParamsPrefix, prop, 0, 0, kVerifyFlag);
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
            KFS_LOG_STREAM_ERROR << mLocation <<
                " invalid " << kAuthTypeParamName <<
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
    } else if (mReconnectFlag && ! mPendingDeleteFlag) {
        mReconnectFlag = false;
        const char* const msg = "meta server reconnect requested";
        KFS_LOG_STREAM_WARN << mLocation << " " << msg << KFS_LOG_EOM;
        Error(msg);
    }
    const time_t now = globalNetManager().Now();
    if (IsConnected() &&
            IsHandshakeDone() &&
            mLastRecvTime +
                min(mReceiveTimeout, mInactivityTimeout) < now) {
        KFS_LOG_STREAM_ERROR << mLocation <<
            " meta server inactivity timeout, last request received: " <<
            (now - mLastRecvTime) << " secs ago" <<
            " timeout:"
            " inactivity: " << mInactivityTimeout <<
            " receive: "    << mReceiveTimeout <<
        KFS_LOG_EOM;
        Error("receive timeout");
    }
    if (! IsConnected()) {
        if (mHelloOp) {
            if (! mSentHello) {
                return; // Wait for hello to come back.
            }
            mSentHello = false;
            DetachAndDeleteOp(mHelloOp);
        }
        if (mPendingDeleteFlag) {
            if (mOuter) {
                mOuter->Error(*this);
            }
            Delete();
            return;
        }
        if ((! mPrimary || this == mPrimary) &&
                mLastConnectTime + mReconnectRetryInterval < now) {
            mLastConnectTime = now;
            Connect();
        }
        return;
    }
    if (mAuthOp) {
        return;
    }
    if (! IsHandshakeDone()) {
        if (mPendingHelloFlag && mHelloSendTime <= now &&
                ! mSentHello && ! mHelloOp) {
            mPendingHelloFlag = false;
            SendHello();
        }
        return;
    }
    DispatchOps();
    if (mRecursionCount <= 0) {
        mNetConnection->StartFlush();
    }
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
    if (mNetConnection) {
        mNetConnection->Close();
        mNetConnection.reset();
    }
    CleanupOpInFlight();
    DetachAndDeleteOp(mAuthOp);
    DiscardPendingResponses();
    mContentLength = 0;
    mCounters.mConnectCount++;
    mGenerationCount++;
    mMyLocation.Reset(0, -1);
    mRpcFormat             = kRpcFormatUndef;
    mSentHello             = false;
    mUpdateCurrentKeyFlag  = false;
    mPendingHelloFlag      = false;
    // when the system is overloaded, we still want to add this
    // connection to the poll vector for reads; this ensures that we
    // get the heartbeats and other RPCs from the metaserver
    const bool readIfOverloadedFlag = true;
    const NetConnectionPtr conn = NetConnection::Connect(
        globalNetManager(), mLocation, this, 0, readIfOverloadedFlag,
        mMaxReadAhead, mInactivityTimeout, mNetConnection);
    if (conn && conn->IsGood()) {
        const bool connectedFlag = conn->IsConnected() &&
            ! conn->IsConnectPending();
        KFS_LOG_STREAM_INFO <<
            (connectedFlag ? "connecting" : "connected") <<
                " to metaserver " << mLocation <<
        KFS_LOG_EOM;
        if (connectedFlag) {
            ScheduleSendHello();
        }
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
    if (! TcpSocket::IsValidConnectToIpAddress(name.c_str())) {
        return -EINVAL;
    }
    return (
        (socket.GetType() == TcpSocket::kTypeIpV4 ? "127.0.0.1" : "::1") == name
         ?  -EINVAL : 0
    );
}

void
MetaServerSM::Impl::SendHello()
{
    if (mHelloOp || mAuthOp) {
        return;
    }
    if (! IsConnected()) {
        KFS_LOG_STREAM_DEBUG <<
            "unable to connect to meta server " << mLocation <<
        KFS_LOG_EOM;
        if (mNetConnection) {
            Error("network error");
        }
        return;
    }
    mMyLocation = gChunkServer.GetLocation();
    if (gChunkServer.CanUpdateServerIp() &&
            (! mPrimary || this == mPrimary) &&
            (mUpdateServerIpFlag || ! mMyLocation.IsValid())) {
        // Advertise the same ip address to the clients, as used
        // for the meta connection.
        ServerLocation loc;
        const int res = mNetConnection->GetSockLocation(loc);
        if (res < 0) {
            KFS_LOG_STREAM_ERROR << mLocation <<
                " getsockname: " << QCUtils::SysError(-res) <<
            KFS_LOG_EOM;
            Error("get socket name error");
            return;
        }
        bool validConnectToIpFlag =
            TcpSocket::IsValidConnectToIpAddress(loc.hostname.c_str());
        if (! validConnectToIpFlag) {
            // Paper over for cygwin / win 7 with no NICs configured:
            // when etsockname returns INADDR_ANY, set ip to loopback address.
            if (mLocation.hostname == "127.0.0.1" ||
                        mLocation.hostname == "localhost" ||
                        mLocation.hostname == "::1") {
                loc.hostname = mLocation.hostname == "localhost" ?
                    string(loc.hostname.find(':') != string::npos ?
                        "::1" : "127.0.0.1") : mLocation.hostname;
                validConnectToIpFlag = true;
            }
        }
        if (! loc.IsValid() || ! validConnectToIpFlag) {
            KFS_LOG_STREAM_ERROR << mLocation <<
                " invalid chunk server location: " << loc <<
                " resetting meta server connection" <<
            KFS_LOG_EOM;
            Error("invalid socket address");
            return;
        }
        const string prevIp = mMyLocation.hostname;
        if (loc.hostname != prevIp) {
            loc.port = mMyLocation.port;
            if (prevIp.empty()) {
                KFS_LOG_STREAM_INFO << mLocation <<
                    " setting chunk server ip to: " << loc.hostname <<
                KFS_LOG_EOM;
                mMyLocation = loc;
            } else {
                const int err = IsIpHostedAndNotLoopBack(prevIp.c_str());
                KFS_LOG_STREAM_WARN << mLocation <<
                    " meta server connection local address: " << loc.hostname <<
                    " current chunk server ip: " << prevIp <<
                    (err == 0 ? string() :
                        " is no longer valid: " + QCUtils::SysError(-err)) <<
                KFS_LOG_EOM;
                if (err) {
                    mMyLocation = loc;
                }
            }
        }
    }
    if (! mMyLocation.IsValid()) {
        Error("chunk server location is not valid");
        return;
    }
    if (! Authenticate()) {
        SubmitHello();
    }
    return;
}

bool
MetaServerSM::Impl::Authenticate()
{
    if (! mNetConnection) {
        die("invalid authenticate invocation: no connection");
        return true;
    }
    if (mAuthOp) {
        die("invalid authenticate invocation: auth is in flight");
        return true;
    }
    if (! mAuthContext.IsEnabled()) {
        return false;
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
        KFS_LOG_STREAM_ERROR << mLocation <<
            " authentication request failure: " <<
            errMsg <<
        KFS_LOG_EOM;
        DetachAndDeleteOp(mAuthOp);
        Error("authentication error");
        return true;
    }
    Request(*mAuthOp);
    KFS_LOG_STREAM_INFO << mLocation <<
        " started: "   << mAuthOp->Show() <<
        " connected: " << IsConnected() <<
        " read:"
        " pending: "   << mNetConnection->GetNumBytesToRead() <<
        " ready: "     << mNetConnection->IsReadReady() <<
    KFS_LOG_EOM;
    if (mRecursionCount <= 0) {
        mNetConnection->StartFlush();
    }
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
    KFS_LOG_STREAM_INFO << mLocation <<
        " sending hello to meta server: " << mHelloOp->Show() <<
    KFS_LOG_EOM;
    if (mRecursionCount <= 0) {
        mNetConnection->StartFlush();
    }
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
    mRecursionCount++;
    switch (code) {
    case EVENT_NET_READ: {
            // We read something from the network.  Run the RPC that
            // came in.
            IOBuffer& iobuf = mNetConnection->GetInBuffer();
            assert(&iobuf == data);
            if (! iobuf.IsEmpty()) {
                mLastRecvTime = globalNetManager().Now();
            }
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
                KFS_LOG_STREAM_ERROR << mLocation <<
                    " exceeded max request header size: " << hdrsz <<
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
            ScheduleSendHello();
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
            if (mInFlightRequestCount <= 0) {
                die("invalid meta in flight request count");
            } else {
                mInFlightRequestCount--;
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
    if (mRecursionCount <= 0) {
        die("meta server state machine: invalid  recursion count");
    }
    if (mRecursionCount <= 1 && mNetConnection) {
        mNetConnection->StartFlush();
    }
    --mRecursionCount;
    if (mRecursionCount <= 0) {
        if (mDeleteFlag) {
            if (mInFlightRequestCount <= 0) {
                delete this;
            }
        } else if (mPendingDeleteFlag && ! globalNetManager().IsRunning()) {
            Delete();
        }
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
        const string conErrMsg = mNetConnection->GetErrorMsg();
        KFS_LOG_STREAM((gMetaServerSM.IsRunning() && wasPrimaryFlag) ?
                MsgLogger::kLogLevelERROR :
                MsgLogger::kLogLevelDEBUG) <<
            mLocation <<
            " closing meta server connection due to " <<
            (msg ? msg : (conErrMsg.empty() ? "error" : "")) <<
            (conErrMsg.empty() ? "" : "; ") << conErrMsg <<
        KFS_LOG_EOM;
        mNetConnection->Close();
        mNetConnection->GetInBuffer().Clear();
        mNetConnection.reset();
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
    mMyLocation.Reset(0, -1);
    if (mOuter) {
        mPendingDeleteFlag = true;
        MetaServerSM& outer = *mOuter;
        mOuter = 0;
        outer.Error(*this);
    }
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
    KfsOp* op;
    while ((op = doneOps.PopFront())) {
        op->status = -EHOSTUNREACH;
        KFS_LOG_STREAM_DEBUG <<
            "failing cs request: " << op->Show() <<
        KFS_LOG_EOM;
        SubmitOpResponse(op);
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
            istream& is = mIStream.Set(iobuf, msgLen);
            string   line;
            while (getline(is, line)) {
                KFS_LOG_STREAM_DEBUG << reinterpret_cast<const void*>(this) <<
                    " " << mLocation << " meta response: " << line <<
                KFS_LOG_EOM;
            }
            mIStream.Reset();
        }
        Properties prop(kRpcFormatShort == mRpcFormat ? 16 : 10);
        const char separator = ':';
        IOBuffer::iterator const it = iobuf.begin();
        if (it != iobuf.end() && msgLen <= it->BytesConsumable()) {
            prop.loadProperties(it->Consumer(), (size_t)msgLen, separator);
        } else {
            prop.loadProperties(mIStream.Set(iobuf, msgLen), separator);
            mIStream.Reset();
        }
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
                KFS_LOG_STREAM_ERROR << mLocation <<
                    " authentication response seq number mismatch: " <<
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
                KFS_LOG_STREAM_ERROR << mLocation <<
                    " invalid meta reply response: " << mAuthOp->Show() <<
                KFS_LOG_EOM;
                Error("invalid meta server response");
                return false;
            }
            HandleAuthResponse(iobuf);
            return false;
        }
        if (mHelloOp) {
            if (status == -EBADCLUSTERKEY) {
                KFS_LOG_STREAM_FATAL << mLocation << " " <<
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
                KFS_LOG_STREAM_ERROR << mLocation <<
                    " hello response error:"
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
                mHelloOp->pendingNotifyFlag = prop.getValue(
                    kRpcFormatShort == mRpcFormat ? "PN" : "Pending-notify",
                    0) != 0;
                mMaxPendingOpsCount = (size_t)max(1, prop.getValue(
                    kRpcFormatShort == mRpcFormat ? "MP" : "Max-pending",
                    96));
                const Properties::String* const metaChunkServerName =
                    prop.getValue(kRpcFormatShort == mRpcFormat ?
                        "SN" : "Chunk-server-name");
                if (metaChunkServerName && ! metaChunkServerName->empty() &&
                        *metaChunkServerName != mMyLocation.hostname) {
                    mMyLocation.hostname.assign(metaChunkServerName->data(),
                        metaChunkServerName->size());
                    KFS_LOG_STREAM_INFO << mLocation <<
                        " set chunk server location: " << mMyLocation <<
                    KFS_LOG_EOM;
                } else {
                    KFS_LOG_STREAM_DEBUG << mLocation <<
                        " got chunk server location: " <<
                            (metaChunkServerName ?
                             metaChunkServerName->data() : "NULL") <<
                    KFS_LOG_EOM;
                }
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
                    gMetaServerSM.SetPrimary(*this, mMyLocation);
                    if (! IsUp() || ! gMetaServerSM.IsUp()) {
                        const char* const msg = "meta server connection down"
                            " after set primary invocation";
                        die(msg);
                        Error(msg);
                        return false;
                    }
                    gChunkManager.HelloDone();
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
                KFS_LOG_STREAM_ERROR << mLocation <<
                    " meta reply no op found for: " << reply <<
                KFS_LOG_EOM;
                Error("no op with such sequence");
                return false;
            }
            op = iter->second;
            op->status = status;
            if (status < 0 && op->statusMsg.empty()) {
                op->statusMsg.swap(statusMsg);
            }
            if (! op->ParseResponse(prop, iobuf) && 0 <= status) {
                KFS_LOG_STREAM_ERROR << mLocation <<
                    " invalid meta reply response: " << op->Show() <<
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
            KFS_LOG_STREAM_ERROR << mLocation <<
                " peer: "    <<
                    (IsConnected() ?  mNetConnection->GetPeerName() : "") <<
                " invalid meta reply response content:"
                " length: "  << len <<
                " status: "  << op->status <<
                " "          << op->statusMsg <<
                " "          << op->Show() <<
                " buf size:" << iobuf.BytesConsumable() <<
                " data: "    << IOBuffer::DisplayData(iobuf) <<
            KFS_LOG_EOM;
            const char* const msg = "response body parse error";
            if (mAbortOnRequestParseErrorFlag) {
                die(msg);
            }
            Error(msg);
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
    KFS_LOG_STREAM_DEBUG << mLocation <<
        " cs reply:"
        " status: " << op->status <<
        " "         << op->statusMsg <<
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
            const string peer = IsConnected() ?
                mNetConnection->GetPeerName() : string("not connected");
            istream& is = mIStream.Set(iobuf, cmdLen);
            string   line;
            int numLines = 32;
            while (--numLines >= 0 && getline(is, line)) {
                KFS_LOG_STREAM_ERROR << mLocation << "/" << peer <<
                    " invalid meta request: " << line <<
                KFS_LOG_EOM;
            }
            mIStream.Reset();
            iobuf.Clear();
            const char* const msg = "request parse error";
            if (mAbortOnRequestParseErrorFlag) {
                die(msg);
            }
            Error(msg);
            // got a bogus command
            return false;
        }
        if (mTraceRequestResponseFlag) {
            istream& is = mIStream.Set(iobuf, cmdLen);
            string   line;
            while (getline(is, line)) {
                KFS_LOG_STREAM_DEBUG << reinterpret_cast<const void*>(this) <<
                    " " << mLocation << " meta request: " << line <<
                KFS_LOG_EOM;
            }
            mIStream.Reset();
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
        const bool okFlag = op->ParseContent(
            mIStream.Set(iobuf, mContentLength), iobuf);
        mIStream.Reset();
        if (! okFlag) {
            KFS_LOG_STREAM_ERROR << mLocation <<
                " / "            <<
                    (IsConnected() ?  mNetConnection->GetPeerName() : "") <<
                " parse error: " << op->statusMsg <<
                " "              << op->Show() <<
                " buf size: "    << iobuf.BytesConsumable() <<
                " data: "        << IOBuffer::DisplayData(iobuf) <<
            KFS_LOG_EOM;
            const char* const msg = "request body parse error";
            if (mAbortOnRequestParseErrorFlag) {
                die(msg);
            }
            delete op;
            Error(msg);
            return false;
        }
        iobuf.Consume(mContentLength);
        mContentLength = 0;
    }
    op->clnt = this;
    KFS_LOG_STREAM_DEBUG << mLocation <<
        " meta request: " << op->Show() <<
    KFS_LOG_EOM;
    if (! mAuthOp && CMD_HEARTBEAT == op->op && mNetConnection) {
        const HeartbeatOp& hb = *static_cast<HeartbeatOp*>(op);
        if (0 < hb.recvTimeout) {
            mReceiveTimeout = hb.recvTimeout;
        }
        DispatchedOps::const_iterator const it = mDispatchedOps.begin();
        if (mDispatchedOps.end() != it &&
                it->second->startTime + mRequestTimeoutUsec <
                globalNetManager().NowUsec()) {
            KFS_LOG_STREAM_ERROR << mLocation <<
                "meta request:"
                " seq: "     << it->first <<
                " "          << it->second->Show() <<
                " timed out"
                " started: " <<
                    (globalNetManager().NowUsec() - it->second->startTime) <<
                " uses. ago" <<
            KFS_LOG_EOM;
            delete op;
            Error("request timed out");
            return false;
        }
        if (hb.authenticateFlag && Authenticate() && ! IsUp()) {
            delete op;
            return false;
        }
        mMaxPendingOpsCount = (size_t)max(1, hb.maxPendingOps);
    }
    mInFlightRequestCount++;
    SubmitOp(op);
    return true;
}

void
MetaServerSM::Impl::Request(KfsOp& op)
{
    op.shortRpcFormatFlag        = kRpcFormatShort == mRpcFormat;
    op.initialShortRpcFormatFlag = op.shortRpcFormatFlag;
    op.status                    = 0;
    KFS_LOG_STREAM_DEBUG << mLocation <<
        " cs request: " << op.Show() <<
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
        KFS_LOG_STREAM_DEBUG << mLocation << " " << self <<
            " cs request: " << mLocation <<
        KFS_LOG_EOM;
        while (getline(is, line)) {
            KFS_LOG_STREAM_DEBUG << mLocation << " " << self <<
                " cs request: " << line <<
            KFS_LOG_EOM;
        }
    }
}

void
MetaServerSM::Impl::EnqueueOp(KfsOp* op)
{
    if (! mAuthOp && mPendingOps.IsEmpty() && IsUp() &&
            mDispatchedOps.size() < mMaxPendingOpsCount) {
        op->seq       = nextSeq();
        op->startTime = globalNetManager().NowUsec();
        const bool noReplyFlag = op->noReply;
        if (! noReplyFlag &&
                ! mDispatchedOps.insert(make_pair(op->seq, op)).second) {
            die("duplicate seq. number");
        }
        Request(*op);
        if (mRecursionCount <= 0) {
            mNetConnection->StartFlush();
        }
        if (noReplyFlag) {
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
    KFS_LOG_STREAM_DEBUG << mLocation <<
        (discardFlag ? " discard" : (mAuthOp ? " pending" : "")) <<
        " meta reply:"
        " status: "  << op->status <<
        " "          << op->statusMsg <<
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
    if (mRecursionCount <= 0) {
        mNetConnection->StartFlush();
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
        KFS_LOG_STREAM_ERROR << mLocation <<
            " authentication protocol failure:" <<
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
                const int res = mNetConnection->Shutdown();
                KFS_LOG_STREAM_DEBUG << mLocation <<
                    " re-authentication ssl filter shutdown:" <<
                    " status: " << res <<
                    " filter: " << (const void*)mNetConnection->GetFilter() <<
                KFS_LOG_EOM;
                if (0 != res && -EAGAIN != res) {
                    Error("ssl shutdown error");
                } else if (0 == res && ! mNetConnection->GetFilter()) {
                    HandleAuthResponse(ioBuf); // Tail recursion.
                }
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
        mLocation <<
        " finished: " << mAuthOp->Show() <<
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
        mMyLocation, mClusterKey, mMD5Sum, mRackId, mChannelId);
    mHelloOp->nodeId             = mNodeId;
    mHelloOp->seq                = nextSeq();
    mHelloOp->sendCurrentKeyFlag = true;
    mHelloOp->noFidsFlag         = mNoFidsFlag;
    mHelloOp->helloDoneCount     = mCounters.mHelloDoneCount;
    mHelloOp->resumeStep         = (mHelloResume < 0 ||
        (mHelloResume != 0 && 0 < mCounters.mHelloDoneCount)) ? 0 : -1;
    mHelloOp->clnt               = this;
    mHelloOp->shortRpcFormatFlag = kRpcFormatShort == mRpcFormat;
    mHelloOp->reqShortRpcFmtFlag = kRpcFormatShort != mRpcFormat;
    mHelloOp->supportsResumeFlag = true;
    // Send the op and wait for the reply.
    KFS_LOG_STREAM_DEBUG <<
        mLocation << ": submit hello" <<
    KFS_LOG_EOM;
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

class MetaServerSM::ResolverReq : public Resolver::Request
{
public:
    typedef QCDLList<ResolverReq>          List;
    typedef Resolver::Request::IpAddresses IpAddresses;

    ResolverReq(
        const string& inHostName,
        MetaServerSM& inOuter)
        : Resolver::Request(inHostName),
          mOuterPtr(&inOuter),
          mInFlightFlag(false)
        { List::Init(*this); }
    const string& GetHostName() const
        { return mHostName; }
    virtual void Done()
    {
        if (! mInFlightFlag) {
            die("resolver completion: request is not in flight");
            return;
        }
        if (! mOuterPtr) {
            delete this;
            return;
        }
        mInFlightFlag = false;
        mOuterPtr->Resolved(*this);
    }
    const IpAddresses& GetIps() const
        { return mIpAddresses; }
    int GetStatus() const
        { return mStatus; }
    const string& GetStatusMsg() const
        { return mStatusMsg; }
    void Delete(
        ResolverReq** inListPtr)
    {
        if (! mOuterPtr) {
            die("resolver request deleted");
            return;
        }
        List::Remove(inListPtr, *this);
        if (mInFlightFlag) {
            mOuterPtr = 0;
            return;
        }
        delete this;
    }
    bool IsInFlight() const
        { return mInFlightFlag; }
    void Enqueue(
        int inTimeoutSec)
    {
        if (mInFlightFlag) {
            die("resolver request is already in flight");
            return;
        }
        mInFlightFlag = true;
        const int theStatus = globalNetManager().Enqueue(*this, inTimeoutSec);
        if (0 != theStatus) {
            const bool runningFlag = globalNetManager().IsRunning();
            const char* const theMsgPtr = "failed to queue resolver request";
            KFS_LOG_STREAM(runningFlag ?
                    MsgLogger::kLogLevelFATAL :
                    MsgLogger::kLogLevelDEBUG) <<
                theMsgPtr <<
                " status: " << theStatus <<
                " "         << QCUtils::SysError(-theStatus) <<
            KFS_LOG_EOM;
            if (runningFlag) {
                die(theMsgPtr);
            }
            mStatusMsg = theMsgPtr;
            mStatus    = theStatus;
            Done();
        }
    }
private:
    MetaServerSM* mOuterPtr;
    bool          mInFlightFlag;
    ResolverReq*  mPrevPtr[1];
    ResolverReq*  mNextPtr[1];

    friend class QCDLListOp<ResolverReq>;

    virtual ~ResolverReq()
        {}
private:
    ResolverReq(
        const ResolverReq& inReq);
    ResolverReq& operator=(
        const ResolverReq& inReq);
};

MetaServerSM::MetaServerSM()
    : ITimeout(),
      mCounters(),
      mRunningFlag(false),
      mAuthEnabledFlag(false),
      mAllowDuplicateLocationsFlag(false),
      mUpdateServerIpFlag(true),
      mResolverStartTime(0),
      mResolverRetryInterval(5),
      mResolverInFlightCount(0),
      mResolvedInFlightCount(0),
      mInactivityTimeout(40),
      mPendingOps(),
      mPrimary(0),
      mLocations(),
      mChanId(1),
      mParameters(),
      mClusterKey(),
      mMd5sum(),
      mNodeId(),
      mRackId(-1)
{
    Impl::List::Init(mImpls);
    ResolverReq::List::Init(mResolverReqs);
}

MetaServerSM::~MetaServerSM()
{
    MetaServerSM::Shutdown();
}

void
MetaServerSM::Cleanup()
{
    Impl* ptr;
    while ((ptr = Impl::List::PopFront(mImpls))) {
        ptr->Delete();
    }
    mResolvedInFlightCount = 0;
    mPrimary = 0;
    ResolverReq* req;
    while ((req = ResolverReq::List::Front(mResolverReqs))) {
        if (req->IsInFlight() && 0 <= mResolverInFlightCount) {
            --mResolverInFlightCount;
        }
        req->Delete(mResolverReqs);
    }
    if (0 != mResolverInFlightCount) {
        die("cleanup: invalid resolver in flight count");
        mResolverInFlightCount = 0;
    }
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
    prop.copyWithPrefix("chunkServer.meta", mParameters);
    prop.copyWithPrefix(kChunkServerAuthParamsPrefix, mParameters);
    mInactivityTimeout = prop.getValue(
        "chunkServer.meta.inactivityTimeout", mInactivityTimeout);
    int res = 0;
    Impl::List::Iterator it(mImpls);
    Impl* ptr;
    while ((ptr = it.Next())) {
        const int ret = ptr->SetParameters(mParameters, mInactivityTimeout);
        if (0 == res) {
            res = ret;
        }
    }
    mResolverRetryInterval = max(1, mParameters.getValue(
        "chunkServer.meta.resolverRetryInterval",
        mResolverRetryInterval));
    mAllowDuplicateLocationsFlag = mParameters.getValue(
        "chunkServer.meta.allowDuplicateLocations",
        mAllowDuplicateLocationsFlag ? 1 : 0
    ) != 0;
    if (Impl::List::IsEmpty(mImpls)) {
        ClientAuthContext ctx;
        const bool kVerifyFlag = true;
        int ret;
        if ((ret = ctx.SetParameters(kChunkServerAuthParamsPrefix,
                mParameters, 0, 0, kVerifyFlag)) == 0) {
            mAuthEnabledFlag = ctx.IsEnabled();
        } else if (0 == res) {
            res = ret;
        }
        ctx.Clear();
    } else {
        mAuthEnabledFlag = Impl::List::Front(mImpls)->IsAuthEnabled();
    }
    return res;
}

void
MetaServerSM::Reconnect()
{
    Impl::List::Iterator it(mImpls);
    Impl* ptr;
    while ((ptr = it.Next())) {
        ptr->Reconnect();
    }
}

void
MetaServerSM::Shutdown()
{
    if (mRunningFlag) {
        globalNetManager().UnRegisterTimeoutHandler(this);
    }
    mRunningFlag = false;
    Impl::List::Iterator it(mImpls);
    Impl* ptr;
    while ((ptr = it.Next())) {
        ptr->Shutdown();
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
    Impl::List::Iterator it(mImpls);
    Impl* ptr;
    while ((ptr = it.Next())) {
        ptr->ForceDown();
    }
}

void
MetaServerSM::Timeout()
{
    if (mPrimary || mLocations.empty() || ! mRunningFlag ||
            0 < mResolverInFlightCount ||
            0 < mResolvedInFlightCount ||
            globalNetManager().Now() <=
                mResolverStartTime + mResolverRetryInterval) {
        return;
    }
    mResolverStartTime = globalNetManager().Now();
    ResolverReq::List::Iterator it(mResolverReqs);
    ResolverReq* ptr;
    while ((ptr = it.Next())) {
        if (! ptr->IsInFlight()) {
            mResolverInFlightCount++;
            ptr->Enqueue(mInactivityTimeout);
        }
    }
}

void
MetaServerSM::Resolved(MetaServerSM::ResolverReq& req)
{
    if (mResolverInFlightCount <= 0) {
        die("invalid resolver in flight count");
    } else {
        --mResolverInFlightCount;
    }
    if (mPrimary) {
        return;
    }
    if (0 != req.GetStatus()) {
        KFS_LOG_STREAM_ERROR <<
            "resolver: " << req.GetHostName() <<
            " status: "  << req.GetStatus() <<
            " "          << req.GetStatusMsg() <<
        KFS_LOG_EOM;
        return;
    }
    const string&                  host = req.GetHostName();
    const ResolverReq::IpAddresses ips  = req.GetIps();
    for (ResolverReq::IpAddresses::const_iterator ipit = ips.begin();
            ips.end() != ipit;
            ++ipit) {
        for (Locations::const_iterator it = mLocations.begin();
                mLocations.end() != it;
                ++it) {
            if (it->hostname != host) {
                continue;
            }
            ServerLocation const loc(*ipit, it->port);
            Impl::List::Iterator iit(mImpls);
            Impl* ptr;
            while ((ptr = iit.Next())) {
                if (ptr->GetLocation() == loc) {
                    break;
                }
            }
            if (ptr) {
                continue;
            }
            Impl& impl = *(new Impl(mCounters, mPendingOps,
                mPrimary, mUpdateServerIpFlag, mChanId++, this));
            const int res = impl.SetMetaInfo(loc, mClusterKey,
                mInactivityTimeout, mRackId, mMd5sum, mNodeId, mParameters);
            if (0 != res) {
                KFS_LOG_STREAM_ERROR <<
                    *it << ": " << QCUtils::SysError(-res) <<
                KFS_LOG_EOM;
                impl.Delete();
                continue;
            }
            mResolvedInFlightCount++;
            Impl::List::PushBack(mImpls, impl);
        }
    }
}

void
MetaServerSM::Error(MetaServerSM::Impl& impl)
{
    if (! Impl::List::IsInList(mImpls, impl)) {
        return;
    }
    if (mResolvedInFlightCount <= 0) {
        die("invalid resolved in flight count");
    }
    mResolvedInFlightCount--;
    Impl::List::Remove(mImpls, impl);
}

int
MetaServerSM::SetMetaInfo(
    const string&     clusterKey,
    int               rackId,
    const string&     md5sum,
    const string&     nodeId,
    const Properties& prop)
{
    if (! Impl::List::IsEmpty(mImpls) || ! mLocations.empty() || mRunningFlag) {
        KFS_LOG_STREAM_ERROR <<
            "meta server info already set" <<
        KFS_LOG_EOM;
        return -EINVAL;
    }
    mClusterKey = clusterKey;
    mRackId     = rackId;
    mMd5sum     = md5sum;
    mNodeId     = nodeId;
    int res = SetParameters(prop);
    if (0 != res) {
        return res;
    }
    mLocations.clear();
    Locations      ips;
    ServerLocation loc;
    loc.hostname = mParameters.getValue(
        "chunkServer.metaServer.hostname", loc.hostname);
    loc.port     = mParameters.getValue(
        "chunkServer.metaServer.port",     loc.port);
    if (loc.IsValid()) {
        if (TcpSocket::IsValidConnectToIpAddress(loc.hostname.c_str())) {
            ips.push_back(loc);
        } else {
            mLocations.push_back(loc);
        }
    }
    const char* const         kParamName = "chunkServer.meta.nodes";
    const Properties::String* locs       = mParameters.getValue(kParamName);
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
            Locations& locs =
                TcpSocket::IsValidConnectToIpAddress(loc.hostname.c_str()) ?
                ips : mLocations;
            if (mAllowDuplicateLocationsFlag ||
                    find(locs.begin(), locs.end(), loc) == locs.end()) {
                locs.push_back(loc);
            } else {
                mLocations.clear();
                KFS_LOG_STREAM_FATAL <<
                    "invalid parameter: "   << kParamName <<
                    " duplicate location: " << loc <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
        }
    }
    if (mLocations.empty() && ips.empty()) {
        KFS_LOG_STREAM_FATAL <<
            "no valid meta server address specified" <<
        KFS_LOG_EOM;
        return -EINVAL;
    }
    mPrimary = 0;
    for (Locations::const_iterator it = ips.begin(); ips.end() != it; ++it) {
        Impl& impl = *(new Impl(mCounters, mPendingOps, mPrimary,
            mUpdateServerIpFlag, mChanId++, 0));
        Impl::List::PushBack(mImpls, impl);
        if ((res = impl.SetMetaInfo(*it, mClusterKey, mInactivityTimeout,
                mRackId, mMd5sum, mNodeId, mParameters)) != 0) {
            break;
        }
    }
    if (0 == res) {
        for (Locations::const_iterator it = mLocations.begin();
                mLocations.end() != it;
                ++it) {
            Locations::const_iterator cit;
            for (cit = mLocations.begin();
                    cit != it && cit->hostname != it->hostname;
                    ++cit)
                {}
            if (cit == it) {
                ResolverReq::List::PushBack(
                    mResolverReqs, *(new ResolverReq(it->hostname, *this)));
            }
        }
        mResolverStartTime = globalNetManager().Now() -
            1 - mResolverRetryInterval;
        mRunningFlag = true;
        if (! Impl::List::IsEmpty(mImpls)) {
            mAuthEnabledFlag = Impl::List::Front(mImpls)->IsAuthEnabled();
        }
        globalNetManager().RegisterTimeoutHandler(this);
    } else {
        Cleanup();
    }
    return res;
}

} // namespace KFS
