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
// \file MetaServerSM.h
// \brief State machine that interfaces with the meta server and
// handles the RPCs sent by the meta server.
//
//----------------------------------------------------------------------------

#ifndef CHUNKSERVER_METASERVERSM_H
#define CHUNKSERVER_METASERVERSM_H

#include "KfsOps.h"

#include "kfsio/KfsCallbackObj.h"
#include "kfsio/ITimeout.h"
#include "kfsio/NetConnection.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/ClientAuthContext.h"
#include "common/StdAllocator.h"

#include <map>
#include <deque>
#include <string>
#include <inttypes.h>

namespace KFS
{
using std::string;
using std::deque;
using std::map;

class MetaServerSMTimeoutImpl;
class Properties;

class MetaServerSM : public KfsCallbackObj, private ITimeout {
public:
    struct Counters
    {
        typedef int64_t Counter;

        Counter mConnectCount;
        Counter mHelloCount;
        Counter mHelloErrorCount;
        Counter mAllocCount;
        Counter mAllocErrorCount;

        void Clear()
        {
            mConnectCount    = 0;
            mHelloCount      = 0;
            mHelloErrorCount = 0;
            mAllocCount      = 0;
            mAllocErrorCount = 0;
        }
    };

    MetaServerSM();
    ~MetaServerSM();

    /// In each hello to the metaserver, we send an MD5 sum of the
    /// binaries.  This should be "acceptable" to the metaserver and
    /// only then is the chunkserver allowed in.  This provides a
    /// simple mechanism to identify nodes that didn't receive a
    /// binary update or are running versions that the metaserver
    /// doesn't know about and shouldn't be inlcuded in the system.
    int SetMetaInfo(const ServerLocation &metaLoc, const string &clusterKey,
        int rackId, const string &md5sum, const Properties& prop);

    void Init();

    /// Send HELLO message.  This sends an op down to the event
    /// processor to get all the info.
    int SendHello();

    /// Generic event handler to handle RPC requests sent by the meta server.
    int HandleRequest(int code, void* data);

    bool HandleMsg(IOBuffer& iobuf, int msgLen);

    void EnqueueOp(KfsOp *op);

    /// If the connection to the server breaks, periodically, retry to
    /// connect; also dispatch ops.
    virtual void Timeout();

    /// Return the meta server name/port information
    ServerLocation GetLocation() const {
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

    void GetCounters(Counters& counters) {
        counters = mCounters;
    }

    void Reconnect() {
        mReconnectFlag = true;
    }

    int SetParameters(const Properties& prop);

    bool IsAuthEnabled() const {
        return mAuthContext.IsEnabled();
    }

    void Shutdown();
private:
    typedef deque<KfsOp*> OpsQueue;
    typedef OpsQueue      PendingResponses;
    typedef std::map<
        kfsSeq_t,
        KfsOp*,
        std::less<kfsSeq_t>,
        StdFastAllocator<
            std::pair<const kfsSeq_t, KfsOp*>
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

    /// a handle to the hello op.  The model: the network processor
    /// queues the hello op to the event processor; the event
    /// processor pulls the result and enqueues the op back to us; the
    /// network processor dispatches the op and gets rid of it.
    HelloMetaOp*    mHelloOp;
    AuthenticateOp* mAuthOp;

    /// list of ops that need to be dispatched: this is the queue that
    /// is shared between the event processor and the network
    /// dispatcher.  When the network dispatcher runs, it pulls ops
    /// from this queue and stashes them away in the dispatched list.
    OpsQueue mPendingOps;
    OpsQueue mDispatchedNoReplyOps;

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
    KfsOp*                        mOp;
    bool                          mRequestFlag;
    int                           mContentLength;
    PendingResponses              mPendingResponses;
    Counters                      mCounters;
    IOBuffer::IStream             mIStream;
    IOBuffer::WOStream            mWOStream;

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

    /// We reconnected to the metaserver; so, resend all the pending ops.
    void ResubmitOps();
    void FailOps(bool shutdownFlag);
    void HandleAuthResponse(IOBuffer& ioBuf);
    void CleanupOpInFlight();
    bool Authenticate();
    void DiscardPendingResponses();
private:
    // No copy.
    MetaServerSM(const MetaServerSM&);
    MetaServerSM& operator=(const MetaServerSM&);
};

extern MetaServerSM gMetaServerSM;

}

#endif // CHUNKSERVER_METASERVERSM_H
