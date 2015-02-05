//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/06
// Author: Sriram Rao, Mike Ovsiannikov
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
// Chunk server state machine implementation.
//
//----------------------------------------------------------------------------

#include "ChunkServer.h"
#include "LayoutManager.h"
#include "NetDispatch.h"
#include "kfstree.h"
#include "util.h"
#include "kfsio/Globals.h"
#include "kfsio/DelegationToken.h"
#include "kfsio/ChunkAccessToken.h"
#include "kfsio/CryptoKeys.h"
#include "common/MdStream.h"
#include "qcdio/QCUtils.h"
#include "common/MsgLogger.h"
#include "common/kfserrno.h"
#include "common/RequestParser.h"
#include "common/IntToString.h"

#include <boost/bind.hpp>

#include <cassert>
#include <string>
#include <sstream>
#include <iomanip>
#include <limits>

namespace KFS
{

using std::string;
using std::istream;
using std::max;
using std::make_pair;
using std::pair;
using std::hex;
using std::numeric_limits;
using std::sort;
using std::setprecision;
using std::scientific;
using std::fixed;
using libkfsio::globalNetManager;
using boost::bind;

static inline time_t TimeNow()
{
    return globalNetManager().Now();
}

class HelloBufferQueueRunner : public ITimeout
{
public:
    static void Schedule()
        { Instance().ScheduleSelf(); }
    virtual void Timeout()
    {
        mWokenFlag = false;
        if (ChunkServer::RunHelloBufferQueue() || ! mRegisteredFlag) {
            return;
        }
        mRegisteredFlag = false;
        globalNetManager().UnRegisterTimeoutHandler(this);
    }
private:
    bool mRegisteredFlag;
    bool mWokenFlag;

    HelloBufferQueueRunner()
        : ITimeout(),
          mRegisteredFlag(false),
          mWokenFlag(false)
        {}
    virtual ~HelloBufferQueueRunner()
    {
        if (! mRegisteredFlag) {
            return;
        }
        mRegisteredFlag = false;
        globalNetManager().UnRegisterTimeoutHandler(this);
    }
    void ScheduleSelf()
    {
        if (! mWokenFlag) {
            mWokenFlag = true;
            globalNetManager().Wakeup();
        }
        if (mRegisteredFlag) {
            return;
        }
        mRegisteredFlag = true;
        globalNetManager().RegisterTimeoutHandler(this);
    }
    static HelloBufferQueueRunner& Instance()
    {
        static HelloBufferQueueRunner sHelloBufferQueueRunner;
        return sHelloBufferQueueRunner;
    }
private:
    HelloBufferQueueRunner(const HelloBufferQueueRunner&);
    HelloBufferQueueRunner& operator=(const HelloBufferQueueRunner&);
};

static ostringstream&
GetTmpOStringStream(bool secondFlag = false)
{
    static ostringstream os[2];
    ostringstream& ret = os[secondFlag ? 1 : 0];
    ret.str(string());
    resetOStream(ret);
    return ret;
}

static ostringstream&
GetTmpOStringStream1()
{
    return GetTmpOStringStream(true);
}

static kfsUid_t
MakeAuthUid(const MetaHello& op, const string& authName)
{
    // Ensure that the chunk server "id" doesn't changes and very unlikely
    // collides with other chunk server "id".
    // Collisions with "real" user ids are OK, as the chunk server and chunk
    // access tokens that this id is used for have "chunk server" flag / bit
    // set.
    static MdStream sha1Stream(
        0,             // inStreamPtr
        true,          // inSyncFlag
        string("sha1"),
        0              // inBufSize
    );
    char port[2] = { (char)((op.port >> 8) & 0xFF) , (char)(op.port & 0xFF) };
    sha1Stream
        .Reset()
        .write(authName.data(),    authName.size())
        .write(op.hostname.data(), op.hostname.size())
        .write(port, sizeof(port))
    ;
    MdStream::MD md;
    const size_t len = sha1Stream.GetMdBin(md);
    if (! sha1Stream || len <= 0) {
        panic("failed to calculate sha1");
        return kKfsUserNone;
    }
    kfsUid_t authUid = 0;
    for (size_t i = len, k = 0; 0 < i; ) {
        if (sizeof(authUid) * 8 <= k) {
            k = 0;
        }
        authUid ^= kfsUid_t(md[--i]) << k;
        k += 8;
    }
    if (authUid == kKfsUserNone) {
        authUid &= ~kfsUid_t(1);
    }
    if (authUid == kKfsUserRoot) {
        authUid = 3333333;
    }
    return authUid;
}

const time_t kMaxSessionTimeoutSec = 10 * 365 * 24 * 60 * 60;

int ChunkServer::sHeartbeatTimeout     = 60;
int ChunkServer::sHeartbeatInterval    = 20;
int ChunkServer::sHeartbeatLogInterval = 1000;
int ChunkServer::sChunkAllocTimeout    = 40;
int ChunkServer::sChunkReallocTimeout  = 75;
int ChunkServer::sMakeStableTimeout    = 330;
int ChunkServer::sReplicationTimeout   = 510;
int ChunkServer::sRequestTimeout       = 600;
int ChunkServer::sMetaClientPort       = 0;
size_t ChunkServer::sMaxChunksToEvacuate  = 2 << 10; // Max queue size
// sHeartbeatInterval * sSrvLoadSamplerSampleCount -- boxcar FIR filter
// if sSrvLoadSamplerSampleCount > 0
int ChunkServer::sSrvLoadSamplerSampleCount = 0;
string ChunkServer::sSrvLoadPropName("Buffer-usec-wait-avg");
bool ChunkServer::sRestartCSOnInvalidClusterKeyFlag = false;
ChunkServer::ChunkOpsInFlight ChunkServer::sChunkOpsInFlight;
ChunkServer* ChunkServer::sChunkServersPtr[kChunkSrvListsCount] = { 0, 0 };
int ChunkServer::sChunkServerCount    = 0;
int ChunkServer::sMaxChunkServerCount = 0;
int ChunkServer::sPendingHelloCount    = 0;
int ChunkServer::sMinHelloWaitingBytes = 0;
int64_t ChunkServer::sHelloBytesCommitted = 0;
int64_t ChunkServer::sHelloBytesInFlight  = 0;
int64_t ChunkServer::sMaxHelloBufferBytes = 256 << 20;
int ChunkServer::sEvacuateRateUpdateInterval = 120;
size_t ChunkServer::sChunkDirsCount = 0;

const int kMaxReadAhead             = 4 << 10;
// Bigger than the default MAX_RPC_HEADER_LEN: max heartbeat size.
const int kMaxRequestResponseHeader = 64 << 10;

void ChunkServer::SetParameters(const Properties& prop, int clientPort)
{
    sHeartbeatTimeout  = prop.getValue(
        "metaServer.chunkServer.heartbeatTimeout",
        sHeartbeatTimeout);
    sHeartbeatInterval = max(3, prop.getValue(
        "metaServer.chunkServer.heartbeatInterval",
        sHeartbeatInterval));
    sHeartbeatLogInterval = prop.getValue(
        "metaServer.chunkServer.heartbeatLogInterval",
        sHeartbeatLogInterval);
    sChunkAllocTimeout = prop.getValue(
        "metaServer.chunkServer.chunkAllocTimeout",
        sChunkAllocTimeout);
    sChunkReallocTimeout = prop.getValue(
        "metaServer.chunkServer.chunkReallocTimeout",
        sChunkReallocTimeout);
    sMakeStableTimeout = prop.getValue(
        "metaServer.chunkServer.makeStableTimeout",
        sMakeStableTimeout);
    sReplicationTimeout = prop.getValue(
        "metaServer.chunkServer.replicationTimeout",
        sReplicationTimeout);
    sRequestTimeout = prop.getValue(
        "metaServer.chunkServer.requestTimeout",
        sRequestTimeout);
    sSrvLoadSamplerSampleCount = prop.getValue(
        "metaServer.chunkServer.srvLoadSampler.sampleCount",
        sSrvLoadSamplerSampleCount);
    sSrvLoadPropName = prop.getValue(
        "metaServer.chunkServer.srvLoadPropName",
        sSrvLoadPropName);
    sMaxChunksToEvacuate = max(size_t(1), prop.getValue(
        "metaServer.chunkServer.maxChunksToEvacuate",
        sMaxChunksToEvacuate));
    if (clientPort > 0) {
        sMetaClientPort = clientPort;
    }
    sRestartCSOnInvalidClusterKeyFlag = prop.getValue(
        "metaServer.chunkServer.restartOnInvalidClusterKey",
        sRestartCSOnInvalidClusterKeyFlag ? 1 : 0) != 0;
}

static seq_t RandomSeqNo()
{
    seq_t ret = 0;
    CryptoKeys::PseudoRand(&ret, sizeof(ret));
    return ((ret < 0 ? -ret : ret) >> 1);
}

inline void
ChunkServerRequest(MetaChunkRequest& req, ostream& os, IOBuffer& buf)
{
    req.request(os, buf);
}

inline void
ChunkServer::UpdateChunkWritesPerDrive(
    int  numChunkWrites,
    int  numWritableDrives)
{
    const int deltaChunkWrites    = numChunkWrites    - mNumChunkWrites;
    const int deltaWritableDrives = numWritableDrives - mNumWritableDrives;
    mNumChunkWrites    = numChunkWrites;
    mNumWritableDrives = numWritableDrives;
    gLayoutManager.UpdateChunkWritesPerDrive(
        *this,
        deltaChunkWrites,
        deltaWritableDrives,
        mStorageTiersInfoDelta
    );
}

inline void
ChunkServer::NewChunkInTier(kfsSTier_t tier)
{
    for (size_t i = 0; i < kKfsSTierCount; i++) {
        mStorageTiersInfoDelta[i].Clear();
    }
    if (kKfsSTierMin <= tier && tier <= kKfsSTierMax &&
            mStorageTiersInfo[tier].GetDeviceCount() > 0) {
        mStorageTiersInfoDelta[tier] = mStorageTiersInfo[tier];
        mStorageTiersInfo[tier].AddInFlightAlloc();
        mStorageTiersInfoDelta[tier].Delta(mStorageTiersInfo[tier]);
    }
    mAllocSpace += CHUNKSIZE;
    UpdateChunkWritesPerDrive(mNumChunkWrites + 1, mNumWritableDrives);
    gLayoutManager.UpdateSrvLoadAvg(*this, 0, mStorageTiersInfoDelta);
}

/* static */ KfsCallbackObj*
ChunkServer::Create(const NetConnectionPtr &conn)
{
    if (! conn || ! conn->IsGood()) {
        return 0;
    }
    if (sMaxChunkServerCount <= sChunkServerCount) {
        KFS_LOG_STREAM_ERROR << conn->GetPeerName() <<
            " chunk servers: "            << sChunkServerCount <<
            " over chunk servers limit: " << sMaxChunkServerCount <<
            " closing connection" <<
        KFS_LOG_EOM;
        return 0;
    }
    ChunkServer* const ret = new ChunkServer(conn, conn->GetPeerName());
    ret->mSelfPtr.reset(ret);
    return ret;
}

ChunkServer::ChunkServer(const NetConnectionPtr& conn, const string& peerName)
    : KfsCallbackObj(),
      CSMapServerInfo(),
      SslFilterVerifyPeer(),
      mSeqNo(RandomSeqNo()),
      mAuthPendingSeq(-1),
      mNetConnection(conn),
      mHelloDone(false),
      mDown(false),
      mHeartbeatSent(false),
      mHeartbeatSkipped(false),
      mLastHeartbeatSent(TimeNow()),
      mCanBeChunkMaster(false),
      mIsRetiring(false),
      mRetireStartTime(0),
      mLastHeard(),
      mChunksToMove(),
      mChunksToEvacuate(),
      mLocation(),
      mHostPortStr(),
      mRackId(-1),
      mNumCorruptChunks(0),
      mTotalSpace(0),
      mPrevTotalSpace(0),
      mTotalFsSpace(0),
      mPrevTotalFsSpace(0),
      mOneOverTotalSpace(0),
      mOneOverTotalFsSpace(0),
      mUsedSpace(0),
      mAllocSpace(0),
      mNumChunks(0),
      mNumDrives(0),
      mNumWritableDrives(0),
      mNumChunkWrites(0),
      mNumAppendsWithWid(0),
      mNumChunkWriteReplications(0),
      mNumChunkReadReplications(0),
      mDispatchedReqs(),
      mReqsTimeoutQueue(),
      mLostChunks(0),
      mUptime(0),
      mHeartbeatProperties(),
      mRestartScheduledFlag(false),
      mRestartQueuedFlag(false),
      mRestartScheduledTime(0),
      mLastHeartBeatLoggedTime(0),
      mDownReason(),
      mOstream(),
      mRecursionCount(0),
      mAuthUid(kKfsUserNone),
      mAuthName(),
      mAuthenticateOp(0),
      mAuthCtxUpdateCount(0),
      mSessionExpirationTime(TimeNow() + kMaxSessionTimeoutSec),
      mReAuthSentFlag(false),
      mHelloOp(0),
      mSelfPtr(),
      mSrvLoadSampler(sSrvLoadSamplerSampleCount, 0, TimeNow()),
      mLoadAvg(0),
      mCanBeCandidateServerFlag(false),
      mStaleChunksHexFormatFlag(false),
      mIStream(),
      mEvacuateCnt(0),
      mEvacuateBytes(0),
      mEvacuateDoneCnt(0),
      mEvacuateDoneBytes(0),
      mEvacuateInFlight(0),
      mPrevEvacuateDoneCnt(0),
      mPrevEvacuateDoneBytes(0),
      mEvacuateLastRateUpdateTime(TimeNow()),
      mEvacuateCntRate(0.),
      mEvacuateByteRate(0.),
      mLostChunkDirs(),
      mChunkDirInfos(),
      mMd5Sum(),
      mPeerName(peerName),
      mCryptoKeyValidFlag(false),
      mCryptoKeyId(),
      mCryptoKey(),
      mRecoveryMetaAccess(),
      mRecoveryMetaAccessEndTime(),
      mPendingResponseOpsHeadPtr(0),
      mPendingResponseOpsTailPtr(0),
      mStorageTiersInfo(),
      mStorageTiersInfoDelta()
{
    assert(mNetConnection);
    ChunkServersList::Init(*this);
    PendingHelloList::Init(*this);
    ChunkServersList::PushBack(sChunkServersPtr, *this);
    SET_HANDLER(this, &ChunkServer::HandleRequest);
    mNetConnection->SetInactivityTimeout(sHeartbeatInterval);
    mNetConnection->SetMaxReadAhead(kMaxReadAhead);
    sChunkServerCount++;
    for (size_t i = 0; i < kKfsSTierCount; i++) {
        mCanBeCandidateServerFlags[i] = false;
    }
    KFS_LOG_STREAM_INFO <<
        "new ChunkServer " << (const void*)this << " " <<
        GetPeerName() <<
        " total: " << sChunkServerCount <<
    KFS_LOG_EOM;
}

ChunkServer::~ChunkServer()
{
    assert(! mSelfPtr);
    KFS_LOG_STREAM_DEBUG << GetServerLocation() <<
        " ~ChunkServer " << (const void*)this <<
        " total: " << sChunkServerCount <<
    KFS_LOG_EOM;
    if (mNetConnection) {
        mNetConnection->Close();
    }
    RemoveFromPendingHelloList();
    delete mHelloOp;
    delete mAuthenticateOp;
    ReleasePendingResponses();
    ChunkServersList::Remove(sChunkServersPtr, *this);
    sChunkServerCount--;
}

void
ChunkServer::ReleasePendingResponses(bool sendResponseFlag /* = false */)
{
    MetaRequest* next = mPendingResponseOpsHeadPtr;
    mPendingResponseOpsTailPtr = 0;
    mPendingResponseOpsHeadPtr = 0;
    while (next) {
        MetaRequest* const op = next;
        next = op->next;
        op->next = 0;
        if (! sendResponseFlag || SendResponse(op)) {
            delete op;
        }
    }
}

void
ChunkServer::AddToPendingHelloList()
{
    if (PendingHelloList::IsInList(sChunkServersPtr, *this)) {
        return;
    }
    if (! mHelloOp || mHelloOp->contentLength <= 0) {
        sMinHelloWaitingBytes = 0;
    } else if (sPendingHelloCount <= 0 ||
            mHelloOp->contentLength < sMinHelloWaitingBytes) {
        sMinHelloWaitingBytes = mHelloOp->contentLength;
    }
    sPendingHelloCount++;
    assert(sPendingHelloCount > 0);
    PendingHelloList::PushBack(sChunkServersPtr, *this);
    HelloBufferQueueRunner::Schedule();
}

void
ChunkServer::RemoveFromPendingHelloList()
{
    if (! PendingHelloList::IsInList(sChunkServersPtr, *this)) {
        PutHelloBytes(mHelloOp);
        return;
    }
    assert(sPendingHelloCount > 0);
    sPendingHelloCount--;
    PendingHelloList::Remove(sChunkServersPtr, *this);
}

/* static */ bool
ChunkServer::RunHelloBufferQueue()
{
    if (PendingHelloList::IsEmpty(sChunkServersPtr)) {
        return false;
    }
    int maxScan = sPendingHelloCount;
    int minHelloSize = numeric_limits<int>::max();
    PendingHelloList::Iterator it(sChunkServersPtr);
    do {
        const int64_t bytesAvail = GetHelloBytes();
        if (bytesAvail < sMinHelloWaitingBytes) {
            break;
        }
        ChunkServer* ptr;
        while ((ptr = it.Next()) &&
                ptr->mNetConnection &&
                ptr->mNetConnection->IsGood() &&
                ptr->mHelloOp &&
                bytesAvail < ptr->mHelloOp->contentLength) {
            if (ptr->mHelloOp->contentLength < minHelloSize) {
                minHelloSize = ptr->mHelloOp->contentLength;
            }
            --maxScan;
        }
        if (! ptr) {
            sMinHelloWaitingBytes = minHelloSize;
            break;
        }
        ptr->RemoveFromPendingHelloList();
        if (ptr->mNetConnection) {
            if (GetHelloBytes(ptr->mHelloOp) < 0) {
                ptr->mNetConnection->SetMaxReadAhead(0);
                ptr->AddToPendingHelloList();
                break;
            }
            ptr->HandleRequest(
                EVENT_NET_READ,
                &ptr->mNetConnection->GetInBuffer()
            );
        } else {
            ptr->PutHelloBytes(ptr->mHelloOp);
        }
    } while (--maxScan > 0);
    return (! PendingHelloList::IsEmpty(sChunkServersPtr));
}

/* static */ int64_t
ChunkServer::GetHelloBytes(MetaHello* req /* = 0 */)
{
    const int64_t avail = min(
        sHelloBytesInFlight + gLayoutManager.GetFreeIoBufferByteCount(),
        sMaxHelloBufferBytes) - sHelloBytesCommitted;
    if (! req) {
        return avail;
    }
    if (req->contentLength <= 0) {
        return 0;
    }
    if (req->bytesReceived < 0) {
        req->bytesReceived = 0;
    }
    if (avail <= 0) {
        return avail;
    }
    if (avail >= req->contentLength) {
        sHelloBytesCommitted += req->contentLength;
    }
    return (avail - req->contentLength);
}

/* static */ void
ChunkServer::PutHelloBytes(MetaHello* req)
{
    if (! req || req->bytesReceived < 0) {
        return;
    }
    if (sHelloBytesCommitted < req->contentLength) {
        panic("invalid hello request byte counter");
        sHelloBytesCommitted = 0;
    } else {
        sHelloBytesCommitted -= req->contentLength;
    }
    if (sHelloBytesInFlight < req->bytesReceived) {
        panic("invalid hello received byte counter");
        sHelloBytesInFlight = 0;
    } else {
        sHelloBytesInFlight -= req->bytesReceived;
    }
    // For debugging store the number of bytes received, just change
    // the sign bit.
    req->bytesReceived = -(req->bytesReceived + 1);
    if (PendingHelloList::IsEmpty(sChunkServersPtr)) {
        return;
    }
    HelloBufferQueueRunner::Schedule();
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
ChunkServer::HandleRequest(int code, void *data)
{
    ChunkServerPtr const refSelf(mSelfPtr);

    mRecursionCount++;
    switch (code) {
    case EVENT_NET_READ: {
        // We read something from the network.  It is
        // either an RPC (such as hello) or a reply to
        // an RPC we sent earlier.
        assert(mNetConnection);
        IOBuffer& iobuf = mNetConnection->GetInBuffer();
        assert(&iobuf == data);
        if (mAuthenticateOp) {
            Authenticate(iobuf);
            if (mAuthenticateOp) {
                break;
            }
        }
        bool gotMsgHdr;
        int  msgLen = 0;
        while ((gotMsgHdr = mHelloOp || IsMsgAvail(&iobuf, &msgLen))) {
            const int retval = HandleMsg(&iobuf, msgLen);
            if (retval < 0) {
                iobuf.Clear();
                Error(mHelloDone ?
                    "request or response parse error" :
                    (mAuthenticateOp ?
                        (mAuthenticateOp->statusMsg.empty() ?
                            "invalid authenticate message" :
                            mAuthenticateOp->statusMsg.c_str()) :
                        "failed to parse hello message"));
                break;
            }
            if (retval > 0 || mAuthenticateOp) {
                break; // Need more data
            }
            msgLen = 0;
        }
        if (! mDown && ! gotMsgHdr &&
                iobuf.BytesConsumable() > kMaxRequestResponseHeader) {
            iobuf.Clear();
            Error(mHelloDone ?
                "request or response header length exceeds max allowed" :
                "hello message header length exceeds max allowed");
        }
        break;
    }

    case EVENT_CMD_DONE: {
        MetaRequest* const op = reinterpret_cast<MetaRequest*>(data);
        assert(data && (mHelloDone || mAuthenticateOp == op));
        const bool deleteOpFlag = op != mAuthenticateOp;
        // nothing left to be done...get rid of it
        if (SendResponse(op) && deleteOpFlag) {
            delete op;
        }
        break;
    }

    case EVENT_NET_WROTE:
        if (mAuthenticateOp) {
            if (! mNetConnection) {
                delete mAuthenticateOp;
                mAuthenticateOp = 0;
                break;
            }
            if (mNetConnection->IsWriteReady()) {
                break;
            }
            if (mAuthenticateOp->status != 0 ||
                    mNetConnection->HasPendingRead() ||
                    mNetConnection->IsReadPending()) {
                const string msg = mAuthenticateOp->statusMsg;
                Error(msg.empty() ?
                    (mNetConnection->HasPendingRead() ?
                        "out of order data received" :
                        "authentication error") :
                        msg.c_str()
                );
                break;
            }
            if (mNetConnection->GetFilter()) {
                if (! mAuthenticateOp->filter) {
                    Error("no clear text communication allowed");
                }
                // Wait for [ssl] shutdown with the current filter to complete.
                break;
            }
            if (mAuthenticateOp->filter) {
                NetConnection::Filter* const filter = mAuthenticateOp->filter;
                mAuthenticateOp->filter = 0;
                string errMsg;
                const int err = mNetConnection->SetFilter(filter, &errMsg);
                if (err) {
                    if (errMsg.empty()) {
                        errMsg = QCUtils::SysError(err < 0 ? -err : err);
                    }
                    Error(errMsg.c_str());
                    break;
                }
            }
            mSessionExpirationTime = mAuthenticateOp->sessionExpirationTime;
            delete mAuthenticateOp;
            mAuthenticateOp  = 0;
            KFS_LOG_STREAM_INFO << GetServerLocation() <<
                (mHelloDone ? " re-" : " ") <<
                "authentication complete:"
                " session expires in: " <<
                    (mSessionExpirationTime - TimeNow()) << " sec." <<
                " pending seq:"
                " requests: "  << mAuthPendingSeq <<
                    " +" << (mSeqNo - mAuthPendingSeq) <<
                " responses: " << (mPendingResponseOpsHeadPtr ?
                    mPendingResponseOpsHeadPtr->opSeqno : seq_t(-1)) <<
                " "            << (mPendingResponseOpsTailPtr ?
                        mPendingResponseOpsTailPtr->opSeqno : seq_t(-1)) <<
            KFS_LOG_EOM;
            if (mHelloDone && 0 <= mAuthPendingSeq) {
                // Enqueue rpcs that were waiting for authentication to finish.
                DispatchedReqs::const_iterator it =
                    mDispatchedReqs.lower_bound(mAuthPendingSeq);
                mAuthPendingSeq = -1;
                while (it != mDispatchedReqs.end()) {
                    MetaChunkRequest* const op = it->second.first->second;
                    ++it;
                    EnqueueSelf(op);
                }
            }
            const bool kSendResponseFlag = true;
            ReleasePendingResponses(kSendResponseFlag);
            break;
        }
        if (! mHelloDone &&
                mNetConnection && ! mNetConnection->IsWriteReady()) {
            Error(
                "hello authentication error, cluster key, or md5sum mismatch");
        }
        // Something went out on the network.
        break;

    case EVENT_INACTIVITY_TIMEOUT:
        // Check heartbeat timeout.
        if (mHelloDone || mLastHeartbeatSent + sHeartbeatTimeout >
                TimeNow()) {
            break;
        }
        Error("hello timeout");
        break;

    case EVENT_NET_ERROR: {
        NetConnection::Filter* filter;
        if (! mDown && mAuthenticateOp &&
                mNetConnection && mNetConnection->IsGood() &&
                (filter = mNetConnection->GetFilter()) &&
                filter->IsShutdownReceived()) {
            // Do not allow to shutdown filter with data in flight.
            if (mNetConnection->GetInBuffer().IsEmpty() &&
                    mNetConnection->GetOutBuffer().IsEmpty()) {
                // Ssl shutdown from the other side.
                if (mNetConnection->Shutdown() == 0) {
                    KFS_LOG_STREAM_DEBUG << GetPeerName() <<
                        " chunk server: " << GetServerLocation() <<
                        " shutdown filter: " <<
                            (void*)mNetConnection->GetFilter() <<
                    KFS_LOG_EOM;
                    if (! mNetConnection->GetFilter()) {
                        HandleRequest(
                            EVENT_NET_WROTE, &mNetConnection->GetOutBuffer());
                        break;
                    }
                }
            } else {
                KFS_LOG_STREAM_ERROR << GetPeerName() <<
                    " chunk server: " << GetServerLocation() <<
                    " invalid filter (ssl) shutdown: "
                    " error: " << mNetConnection->GetErrorMsg() <<
                    " read: "  << mNetConnection->GetNumBytesToRead() <<
                    " write: " << mNetConnection->GetNumBytesToWrite() <<
                KFS_LOG_EOM;
            }
        }
        Error("communication error");
        break;
    }

    default:
        assert(!"Unknown event");
        break;
    }
    if (mHelloDone) {
        if (mRecursionCount <= 1) {
            const int hbTimeout = Heartbeat();
            const int opTimeout = TimeoutOps();
            if (! mDown && mNetConnection) {
                mNetConnection->StartFlush();
            }
            if (! mDown && mNetConnection && mNetConnection->IsGood()) {
                mNetConnection->SetInactivityTimeout(
                    NetManager::Timer::MinTimeout(hbTimeout, opTimeout));
            }
        }
    } else {
        if (code != EVENT_INACTIVITY_TIMEOUT) {
            mLastHeartbeatSent = TimeNow();
        }
        if (mRecursionCount <= 1 && ! mDown && mNetConnection) {
            mNetConnection->StartFlush();
        }
    }
    assert(mRecursionCount > 0);
    mRecursionCount--;
    return 0;
}

void
ChunkServer::ForceDown()
{
    if (mDown) {
        return;
    }
    KFS_LOG_STREAM_WARN <<
        "forcing chunk server " << GetServerLocation() <<
        "/" << (mNetConnection ? GetPeerName() :
            string("not connected")) <<
        " down" <<
    KFS_LOG_EOM;
    if (mNetConnection) {
        mNetConnection->Close();
        mNetConnection->GetInBuffer().Clear();
        mNetConnection.reset();
    }
    RemoveFromPendingHelloList();
    delete mHelloOp;
    mHelloOp      = 0;
    mDown         = true;
    // Take out the server from write-allocation
    mTotalSpace   = 0;
    mTotalFsSpace = 0;
    mAllocSpace   = 0;
    mUsedSpace    = 0;
    const int64_t delta = -mLoadAvg;
    mLoadAvg      = 0;
    ClearStorageTiers();
    gLayoutManager.UpdateSrvLoadAvg(*this, delta, mStorageTiersInfoDelta);
    UpdateChunkWritesPerDrive(0, 0);
    FailDispatchedOps();
    assert(sChunkDirsCount >= mChunkDirInfos.size());
    sChunkDirsCount -= min(sChunkDirsCount, mChunkDirInfos.size());
    mChunkDirInfos.clear();
    mSelfPtr.reset(); // Unref / delete self
}

void
ChunkServer::SetCanBeChunkMaster(bool flag)
{
    if (mCanBeChunkMaster == flag) {
        return;
    }
    const int64_t delta = -mLoadAvg;
    mLoadAvg = 0;
    const bool kCanBeCandidateFlag = false;
    for (size_t i = 0; i < kKfsSTierCount; i++) {
        mStorageTiersInfoDelta[i].Clear();
    }
    gLayoutManager.UpdateSrvLoadAvg(
        *this, delta, mStorageTiersInfoDelta, kCanBeCandidateFlag);
    mCanBeChunkMaster = flag;
    mLoadAvg = -delta;
    gLayoutManager.UpdateSrvLoadAvg(*this, mLoadAvg, mStorageTiersInfoDelta);
}

void
ChunkServer::Error(const char* errorMsg)
{
    KFS_LOG_STREAM_ERROR <<
        "chunk server " << GetServerLocation() <<
        "/" << (mNetConnection ? GetPeerName() :
            string("not connected")) <<
        " down" <<
        (mRestartQueuedFlag ? " restart" : "") <<
        " reason: " << (errorMsg ? errorMsg : "unspecified") <<
        " socket error: " << (mNetConnection ?
            mNetConnection->GetErrorMsg() : string("none")) <<
    KFS_LOG_EOM;
    if (mNetConnection) {
        mNetConnection->Close();
        mNetConnection->GetInBuffer().Clear();
        mNetConnection.reset();
    }
    if (mDownReason.empty() && mRestartQueuedFlag) {
        mDownReason = "restart";
    }
    RemoveFromPendingHelloList();
    delete mAuthenticateOp;
    mAuthenticateOp = 0;
    delete mHelloOp;
    mHelloOp      = 0;
    mDown         = true;
    // Take out the server from write-allocation
    mTotalSpace   = 0;
    mTotalFsSpace = 0;
    mAllocSpace   = 0;
    mUsedSpace    = 0;
    const int64_t delta = -mLoadAvg;
    mLoadAvg      = 0;
    ClearStorageTiers();
    gLayoutManager.UpdateSrvLoadAvg(*this, delta, mStorageTiersInfoDelta);
    UpdateChunkWritesPerDrive(0, 0);
    FailDispatchedOps();
    assert(sChunkDirsCount >= mChunkDirInfos.size());
    sChunkDirsCount -= min(sChunkDirsCount, mChunkDirInfos.size());
    mChunkDirInfos.clear();
    if (mHelloDone) {
        // force the server down thru the main loop to avoid races
        MetaBye* const mb = new MetaBye(0, shared_from_this());
        mb->authUid = mAuthUid;
        mb->clnt    = this;
        submit_request(mb);
    }
    ReleasePendingResponses();
    mSelfPtr.reset(); // Unref / delete self
}

///
/// We have a message from the chunk server.  The message we got is one
/// of:
///  -  a HELLO message from the server
///  -  it is a response to some command we previously sent
///  -  is an RPC from the chunkserver
///
/// Of these, for the first and third case,create an op and
/// send that down the pike; in the second case, retrieve the op from
/// the pending list, attach the response, and push that down the pike.
///
/// @param[in] iobuf: Buffer containing the command
/// @param[in] msgLen: Length of the command in the buffer
/// @retval 0 if we handled the message properly; -1 on error;
///   1 if there is more data needed for this message and we haven't
///   yet received the data.
int
ChunkServer::HandleMsg(IOBuffer *iobuf, int msgLen)
{
    if (! mHelloDone) {
        return HandleHelloMsg(iobuf, msgLen);
    }
    char buf[3];
    if (iobuf->CopyOut(buf, 3) == 3 &&
            buf[0] == 'O' && buf[1] == 'K' && (buf[2] & 0xFF) <= ' ') {
        return HandleReply(iobuf, msgLen);
    }
    return HandleCmd(iobuf, msgLen);
}

void
ChunkServer::ShowLines(MsgLogger::LogLevel logLevel, const string& prefix,
    IOBuffer& iobuf, int len, int linesToShow /* = 64 */,
    const char* truncatePrefix /* = "CKey:" */)
{
    istream&     is       = mIStream.Set(iobuf, len);
    int          maxLines = linesToShow;
    string       line;
    size_t const prefLen  = truncatePrefix ? strlen(truncatePrefix) : 0; 
    while (--maxLines >= 0 && getline(is, line)) {
        string::iterator last = line.end();
        if (last != line.begin() && *--last == '\r') {
            line.erase(last);
        }
        if (truncatePrefix &&
                line.compare(0, prefLen, truncatePrefix, prefLen) == 0) {
            line.resize(prefLen);
            line += " xxx";
        }
        KFS_LOG_STREAM(logLevel) <<
            prefix << line <<
        KFS_LOG_EOM;
    }
    mIStream.Reset();
}

MetaRequest*
ChunkServer::GetOp(IOBuffer& iobuf, int msgLen, const char* errMsgPrefix)
{
    MetaRequest* op = 0;
    if (ParseCommand(iobuf, msgLen, &op) >= 0) {
        op->setChunkServer(shared_from_this());
        return op;
    }
    ShowLines(MsgLogger::kLogLevelERROR,
        GetHostPortStr() + "/" + GetPeerName() + " " +
            (errMsgPrefix ? errMsgPrefix : "") + ": ",
        iobuf,
        msgLen
    );
    iobuf.Consume(msgLen);
    return 0;
}

class HexChunkInfoParser
{
public:
    typedef MetaHello::ChunkInfo ChunkInfo;

    HexChunkInfoParser(const IOBuffer& buf, bool noFidsFlag)
        : mIt(buf),
          mCur(),
          mVal(0),
          mPrevSpaceFlag(true),
          mStartFieldIdx(noFidsFlag ? 1 : 0),
          mFieldIdx(mStartFieldIdx)
        {}
    const ChunkInfo* Next()
    {
        const unsigned char kInvalHex = 0xFF;
        // All ids are expected to be positive.
        const unsigned char* p;
        while ((p = reinterpret_cast<const unsigned char*>(
                mIt.Next()))) {
            if (*p <= ' ') {
                if (mPrevSpaceFlag) {
                    continue;
                }
                mPrevSpaceFlag = true;
                switch (mFieldIdx) {
                    case 0:                         ; break;
                    case 1: mCur.chunkId      = mVal; break;
                    case 2: mCur.chunkVersion = mVal;
                        mFieldIdx = mStartFieldIdx;
                        mVal      = 0;
                        return &mCur;
                    default: assert(! "invald field index");
                        return 0;
                }
                mFieldIdx++;
                mVal = 0;
                continue;
            }
            mPrevSpaceFlag = false;
            const unsigned char h = sC2HexTable[*p];
            if (h == kInvalHex) {
                return 0;
            }
            mVal = (mVal << 4) | h;
        }
        if (mFieldIdx == 2 && ! mPrevSpaceFlag) {
            mCur.chunkVersion = mVal;
            mFieldIdx = 0;
            mVal      = 0;
            return &mCur;
        }
        return 0;
    }
    bool IsError() const { return (mFieldIdx != 0); }
private:
    IOBuffer::ByteIterator mIt;
    ChunkInfo              mCur;
    int64_t                mVal;
    bool                   mPrevSpaceFlag;
    const int              mStartFieldIdx;
    int                    mFieldIdx;

    static const unsigned char* const sC2HexTable;
};
const unsigned char* const HexChunkInfoParser::sC2HexTable = char2HexTable();

int
ChunkServer::DeclareHelloError(
    int         status,
    const char* statusMsg)
{
    mHelloOp->status = status < 0 ? status : -EINVAL;
    if (statusMsg) {
        mHelloOp->statusMsg = statusMsg;
    }
    if (mHelloOp->statusMsg.empty()) {
        mHelloOp->statusMsg = "invalid chunk server hello";
    }
    KFS_LOG_STREAM_ERROR << GetPeerName() << " " <<
        mHelloOp->statusMsg <<
    KFS_LOG_EOM;
    mNetConnection->GetInBuffer().Clear();
    mNetConnection->SetMaxReadAhead(0);
    mNetConnection->SetInactivityTimeout(sRequestTimeout);
    if (SendResponse(mHelloOp)) {
        delete mHelloOp;
    }
    mHelloOp = 0;
    return 0;
}

/// Case #1: Handle Hello message from a chunkserver that
/// just connected to us.
int
ChunkServer::HandleHelloMsg(IOBuffer* iobuf, int msgLen)
{
    assert(!mHelloDone);

    const bool hasHelloOpFlag = mHelloOp != 0;
    if (! hasHelloOpFlag) {
        MetaRequest * const op = mAuthenticateOp ? mAuthenticateOp :
            GetOp(*iobuf, msgLen, "invalid hello");
        if (! op) {
            return -1;
        }
        if (! mAuthenticateOp && op->op == META_AUTHENTICATE) {
            mReAuthSentFlag = false;
            mAuthenticateOp = static_cast<MetaAuthenticate*>(op);
        }
        if (mAuthenticateOp) {
            iobuf->Consume(msgLen);
            return Authenticate(*iobuf);
        }
        if (op->op != META_HELLO) {
            ShowLines(MsgLogger::kLogLevelERROR,
                GetPeerName() + " ",
                *iobuf, msgLen);
        } else {
            ShowLines(MsgLogger::kLogLevelINFO,
                "new: " + GetPeerName() + " ",
                *iobuf, msgLen);
        }
        iobuf->Consume(msgLen);
        // We should only get a HELLO message here; anything else is
        // invalid.
        if (op->op != META_HELLO) {
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
                " unexpected request, expected hello" <<
            KFS_LOG_EOM;
            delete op;
            return -1;
        }
        NetConnection::Filter* filter;
        if (mAuthName.empty() && (filter = mNetConnection->GetFilter()) &&
                filter->GetAuthName().empty()) {
            // Only for PSK authentication with meta server empty key id.
            // If key id is used, then it is validated only once here, with
            // initial handshake, it is not validated in the case of
            // re-authentication.
            // User and group database is always ignored with PSK.
            mAuthName = filter->GetPeerId();
            if (! gLayoutManager.GetCSAuthContext().RemapAndValidate(
                    mAuthName)) {
                string msg("invalid chunk server peer id: ");
                msg +=  filter->GetPeerId();
                return DeclareHelloError(-EPERM, msg.c_str());
            }
        }
        mHelloOp           = static_cast<MetaHello*>(op);
        mHelloOp->authName = mAuthName;
        if (mAuthName.empty() &&
                gLayoutManager.GetCSAuthContext().IsAuthRequired()) {
            return DeclareHelloError(-EPERM, "authentication required");
        }
        if (! mAuthName.empty()) {
            if (! ParseCryptoKey(mHelloOp->cryptoKeyId, mHelloOp->cryptoKey)) {
                mHelloOp = 0;
                delete op;
                return -1;
            }
            mAuthCtxUpdateCount =
                gLayoutManager.GetCSAuthContext().GetUpdateCount();
        }
        const char* const kAddrAny = "0.0.0.0";
        if (mHelloOp->location.hostname == kAddrAny) {
            // Use peer name.
            mHelloOp->location.hostname.clear();
            const size_t pos = mPeerName.rfind(':');
            if (pos != string::npos) {
                mHelloOp->location.hostname.assign(mPeerName.data(), pos);
            }
        }
        if (! gLayoutManager.Validate(*mHelloOp)) {
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
                " hello"
                " location: " << mHelloOp->ToString() <<
                " error: "    << op->status <<
                " "           << op->statusMsg <<
            KFS_LOG_EOM;
            if (mHelloOp->status != -EBADCLUSTERKEY) {
                mHelloOp = 0;
                delete op;
                return -1;
            }
            if (! sRestartCSOnInvalidClusterKeyFlag) {
                return DeclareHelloError(mHelloOp->status, 0);
            }
        }
        if (! mHelloOp->location.IsValid()) {
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
                " hello: invalid server locaton: " << mHelloOp->location <<
            KFS_LOG_EOM;
            mHelloOp = 0;
            delete op;
            return -1;
        }
        if (mHelloOp->status == 0 &&
                sMaxHelloBufferBytes < mHelloOp->contentLength) {
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
                " hello content length: " << mHelloOp->contentLength <<
                " exceeds max. allowed: " << sMaxHelloBufferBytes <<
            KFS_LOG_EOM;
            mHelloOp = 0;
            delete op;
            return -1;
        }
        if (mHelloOp->fileSystemId <= 0 &&
                (0 < mHelloOp->numChunks ||
                    0 < mHelloOp->numNotStableAppendChunks ||
                    0 < mHelloOp->numNotStableChunks) &&
                gLayoutManager.IsFileSystemIdRequired()) {
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
                " hello: invalid file sytem id" <<
            KFS_LOG_EOM;
            mHelloOp = 0;
            delete op;
            return -1;
        }
        mHelloOp->metaFileSystemId = metatree.GetFsId();
        if (0 < mHelloOp->fileSystemId &&
                mHelloOp->fileSystemId != mHelloOp->metaFileSystemId) {
            if (gLayoutManager.IsDeleteChunkOnFsIdMismatch()) {
                mHelloOp->deleteAllChunksFlag      = true;
                mHelloOp->numChunks                = 0;
                mHelloOp->numNotStableAppendChunks = 0;
                mHelloOp->numNotStableChunks       = 0;
            } else {
                return DeclareHelloError(-EINVAL, "file system id mismatch");
            }
        }
        if (mHelloOp->status == 0 &&
                mHelloOp->contentLength > 0 &&
                iobuf->BytesConsumable() <
                    mHelloOp->contentLength &&
                GetHelloBytes(mHelloOp) < 0) {
            KFS_LOG_STREAM_INFO << GetPeerName() <<
                " hello content length: " <<
                    mHelloOp->contentLength <<
                " adding to pending list"
                " ops: "       << sPendingHelloCount <<
                " bytes:"
                " committed: " << sHelloBytesCommitted <<
                " received: "  << sHelloBytesInFlight <<
            KFS_LOG_EOM;
            mNetConnection->SetMaxReadAhead(0);
            AddToPendingHelloList();
            return 1;
        } else {
            // Hello didn't go through the buffer commit process,
            // mark it such that PutHelloBytes() if invoked has no
            // effect.
            mHelloOp->bytesReceived = -1;
        }
    }
    // make sure we have the chunk ids...
    if (mHelloOp->contentLength > 0) {
        const int nAvail = iobuf->BytesConsumable();
        if (nAvail < mHelloOp->contentLength) {
            // need to wait for data...
            if (mHelloOp->status != 0) {
                mHelloOp->contentLength -= nAvail;
                iobuf->Clear(); // Discard content.
            } else if (nAvail > mHelloOp->bytesReceived) {
                sHelloBytesInFlight +=
                    nAvail - mHelloOp->bytesReceived;
                mHelloOp->bytesReceived = nAvail;
            }
            mNetConnection->SetMaxReadAhead(max(kMaxReadAhead,
                mHelloOp->status == 0 ?
                    mHelloOp->contentLength - nAvail :
                kMaxRequestResponseHeader
            ));
            return 1;
        }
        const int contentLength = mHelloOp->contentLength;
        if (hasHelloOpFlag && mHelloOp->status == 0) {
            // Emit log message to have time stamp of when hello is
            // fully received, and parsing of chunk lists starts.
            KFS_LOG_STREAM_INFO << GetPeerName() <<
                " receiving hello: " <<
                    contentLength <<
                " bytes done" <<
            KFS_LOG_EOM;
            PutHelloBytes(mHelloOp);
        }
        mHelloOp->chunks.clear();
        mHelloOp->notStableChunks.clear();
        mHelloOp->notStableAppendChunks.clear();
        if (mHelloOp->status == 0) {
            const size_t numStable(max(0, mHelloOp->numChunks));
            mHelloOp->chunks.reserve(mHelloOp->numChunks);
            const size_t nonStableAppendNum(
                max(0, mHelloOp->numNotStableAppendChunks));
            mHelloOp->notStableAppendChunks.reserve(nonStableAppendNum);
            const size_t nonStableNum(max(0, mHelloOp->numNotStableChunks));
            mHelloOp->notStableChunks.reserve(nonStableNum);
            // get the chunkids
            istream& is = mIStream.Set(iobuf, contentLength);
            HexChunkInfoParser hexParser(*iobuf, mHelloOp->noFidsFlag);
            for (int j = 0; j < 3; ++j) {
                MetaHello::ChunkInfos& chunks = j == 0 ?
                    mHelloOp->chunks : (j == 1 ?
                    mHelloOp->notStableAppendChunks :
                    mHelloOp->notStableChunks);
                int i = j == 0 ?
                    mHelloOp->numChunks : (j == 1 ?
                    mHelloOp->numNotStableAppendChunks :
                    mHelloOp->numNotStableChunks);
                if (mHelloOp->contentIntBase == 16) {
                    const MetaHello::ChunkInfo* c;
                    while (i-- > 0 && (c = hexParser.Next())) {
                        chunks.push_back(*c);
                    }
                } else {
                    MetaHello::ChunkInfo c;
                    if (mHelloOp->noFidsFlag) {
                        while (i-- > 0) {
                            if (! (is >> c.chunkId >> c.chunkVersion)) {
                                break;
                            }
                            chunks.push_back(c);
                        }
                    } else {
                        fid_t allocFileId;
                        while (i-- > 0) {
                            if (! (is >> allocFileId
                                    >> c.chunkId
                                    >> c.chunkVersion)) {
                                break;
                            }
                            chunks.push_back(c);
                        }
                    }
                }
            }
            mIStream.Reset();
            iobuf->Consume(contentLength);
            if (mHelloOp->chunks.size() != numStable ||
                    mHelloOp->notStableAppendChunks.size() !=
                    nonStableAppendNum ||
                    mHelloOp->notStableChunks.size() !=
                    nonStableNum) {
                KFS_LOG_STREAM_ERROR << GetPeerName() <<
                    " invalid or short chunk list:"
                    " expected: " << mHelloOp->numChunks <<
                    "/"      << mHelloOp->numNotStableAppendChunks <<
                    "/"      << mHelloOp->numNotStableChunks <<
                    " got: " << mHelloOp->chunks.size() <<
                    "/"      <<
                        mHelloOp->notStableAppendChunks.size() <<
                    "/"      << mHelloOp->notStableChunks.size() <<
                    " last good chunk: " <<
                        (mHelloOp->chunks.empty() ? -1 :
                        mHelloOp->chunks.back().chunkId) <<
                    "/" << (mHelloOp->notStableAppendChunks.empty() ? -1 :
                        mHelloOp->notStableAppendChunks.back().chunkId) <<
                    "/" << (mHelloOp->notStableChunks.empty() ? -1 :
                        mHelloOp->notStableChunks.back().chunkId) <<
                    " content length: " << contentLength <<
                KFS_LOG_EOM;
                delete mHelloOp;
                mHelloOp = 0;
                return -1;
            }
        }
    }
    if (mHelloOp->status != 0) {
        iobuf->Clear();
        if (! mNetConnection) {
            delete mHelloOp;
            mHelloOp = 0;
            return -1;
        }
        KFS_LOG_STREAM_INFO <<
            mHelloOp->ToString() <<
            " "         << mHelloOp->Show() <<
            " status: " << mHelloOp->status <<
            " msg: "    << mHelloOp->statusMsg <<
            " initiating chunk server restart" <<
        KFS_LOG_EOM;
        // Tell him hello is OK in order to make the restart
        // work.
        mHelloOp->status = 0;
        IOBuffer& ioBuf = mNetConnection->GetOutBuffer();
        mOstream.Set(ioBuf);
        mHelloOp->response(mOstream);
        if (gLayoutManager.IsRetireOnCSRestart()) {
            MetaChunkRetire retire(
                NextSeq(), shared_from_this());
            ChunkServerRequest(retire, mOstream, ioBuf);
        } else {
            MetaChunkServerRestart restart(
                NextSeq(), shared_from_this());
            ChunkServerRequest(restart, mOstream, ioBuf);
        }
        mOstream.Reset();
        mNetConnection->SetMaxReadAhead(0);
        mNetConnection->SetInactivityTimeout(sRequestTimeout);
        delete mHelloOp;
        mHelloOp = 0;
        // Do not declare error, outbound data still pending.
        // Create response and set timeout, the chunk server
        // should disconnect when it restarts.
        return 0;
    }
    mNetConnection->SetMaxReadAhead(kMaxReadAhead);
    // Hello done.
    mHelloOp->peerName        = GetPeerName();
    mHelloOp->clnt            = this;
    mHelloOp->server          = shared_from_this();
    mHelloDone                = true;
    mLastHeard                = TimeNow();
    mUptime                   = mHelloOp->uptime;
    mNumAppendsWithWid        = mHelloOp->numAppendsWithWid;
    mStaleChunksHexFormatFlag = mHelloOp->staleChunksHexFormatFlag;
    for (size_t i = 0; i < kKfsSTierCount; i++) {
        mStorageTiersInfoDelta[i].Clear();
    }
    UpdateChunkWritesPerDrive((int)(mHelloOp->notStableAppendChunks.size() +
        mHelloOp->notStableChunks.size()), mNumWritableDrives);
    mLastHeartbeatSent = mLastHeard;
    mHeartbeatSent     = true;
    Enqueue(new MetaChunkHeartbeat(NextSeq(), shared_from_this(),
            IsRetiring() ? int64_t(1) :
                (int64_t)mChunksToEvacuate.Size()),
        2 * sHeartbeatTimeout);
    // Emit message to time parse.
    KFS_LOG_STREAM_INFO << GetPeerName() <<
        " submit hello" <<
    KFS_LOG_EOM;
    if (mAuthName.empty()) {
        mAuthUid = kKfsUserNone;
    } else {
        mAuthUid = MakeAuthUid(*mHelloOp, mAuthName);
    }
    SetServerLocation(mHelloOp->location);
    mMd5Sum = mHelloOp->md5sum;
    MetaRequest* const op = mHelloOp;
    mHelloOp = 0;
    op->authUid = mAuthUid;
    submit_request(op);
    return 0;
}

void
ChunkServer::SetServerLocation(const ServerLocation& loc)
{
    if (mLocation == loc) {
        return;
    }
    mLocation = loc;
    mHostPortStr = mLocation.hostname;
    mHostPortStr += ':';
    AppendDecIntToString(mHostPortStr, mLocation.port);
}

///
/// Case #2: Handle an RPC from a chunkserver.
///
int
ChunkServer::HandleCmd(IOBuffer* iobuf, int msgLen)
{
    if (! mHelloDone || mAuthenticateOp) {
        panic("chunk server state machine invalid state"
            " unfinished hello or authentication request");
        return -1;
    }

    MetaRequest* const op = GetOp(*iobuf, msgLen, "invalid request");
    if (! op) {
        return -1;
    }
    if (op->op == META_HELLO) {
        delete op;
        return -1;
    }
    // Message is ready to be pushed down.  So remove it.
    iobuf->Consume(msgLen);
    if (op->op == META_AUTHENTICATE) {
        mReAuthSentFlag = false;
        mAuthenticateOp = static_cast<MetaAuthenticate*>(op);
    }
    if (mAuthenticateOp) {
        return Authenticate(*iobuf);
    }
    op->fromClientSMFlag = false;
    op->clnt             = this;
    op->authUid          = mAuthUid;
    submit_request(op);
    return 0;
}

void
ChunkServer::UpdateSpace(MetaChunkEvacuate& op)
{
    if (op.totalSpace >= 0) {
        mTotalSpace = op.totalSpace;
    }
    if (op.totalFsSpace >= 0) {
        mTotalFsSpace = op.totalFsSpace;
    }
    if (op.usedSpace >= 0) {
        mUsedSpace = op.usedSpace;
    }
    int wrDrives = op.numWritableDrives;
    if (op.numDrives >= 0) {
        mNumDrives = op.numDrives;
        if (mNumDrives < mNumWritableDrives && wrDrives < 0) {
            wrDrives = mNumDrives;
        }
    }
    if (mNumDrives < wrDrives) {
        wrDrives = mNumDrives;
    }
    if (wrDrives >= 0) {
        UpdateStorageTiers(op.GetStorageTiersInfo(), wrDrives, mNumChunkWrites);
        UpdateChunkWritesPerDrive(mNumChunkWrites, wrDrives);
        gLayoutManager.UpdateSrvLoadAvg(*this, 0, mStorageTiersInfoDelta);
    }
    if (op.numEvacuateInFlight == 0) {
        mChunksToEvacuate.Clear();
        mEvacuateCnt = 0;
    } else if (op.numEvacuateInFlight > 0) {
        mEvacuateInFlight = op.numEvacuateInFlight;
    }
}

///
/// Case #3: Handle a reply from a chunkserver to an RPC we
/// previously sent.
///
int
ChunkServer::HandleReply(IOBuffer* iobuf, int msgLen)
{
    assert(mHelloDone);

    // We got a response for a command we previously
    // sent.  So, match the response to its request and
    // resume request processing.
    Properties prop;
    const bool ok = ParseResponse(mIStream.Set(iobuf, msgLen), prop);
    mIStream.Reset();
    if (! ok) {
        return -1;
    }
    // Message is ready to be pushed down.  So remove it.
    iobuf->Consume(msgLen);

    const seq_t             cseq = prop.getValue("Cseq", (seq_t) -1);
    MetaChunkRequest* const op   = FindMatchingRequest(cseq);
    if (! op) {
        // Most likely op was timed out, or chunk server sent response
        // after re-connect.
        KFS_LOG_STREAM_INFO << GetServerLocation() <<
            " unable to find command for response cseq: " << cseq <<
        KFS_LOG_EOM;
        return 0;
    }

    mLastHeard = TimeNow();
    op->statusMsg = prop.getValue("Status-message", "");
    op->status    = prop.getValue("Status",         -1);
    if (op->status < 0) {
        op->status = -KfsToSysErrno(-op->status);
    }
    op->handleReply(prop);
    if (op->op == META_CHUNK_HEARTBEAT) {
        mTotalSpace        = prop.getValue("Total-space",           int64_t(0));
        mTotalFsSpace      = prop.getValue("Total-fs-space",       int64_t(-1));
        mUsedSpace         = prop.getValue("Used-space",            int64_t(0));
        mNumChunks         = prop.getValue("Num-chunks",                     0);
        mNumDrives         = prop.getValue("Num-drives",                     0);
        mUptime            = prop.getValue("Uptime",                int64_t(0));
        mLostChunks        = prop.getValue("Chunk-lost",            int64_t(0));
        mNumCorruptChunks  = max(mNumCorruptChunks,
            prop.getValue("Chunk-corrupted", int64_t(0)));
        mNumAppendsWithWid = prop.getValue("Num-appends-with-wids", int64_t(0));
        mEvacuateCnt       = prop.getValue("Evacuate",              int64_t(-1));
        mEvacuateBytes     = prop.getValue("Evacuate-bytes",        int64_t(-1));
        mEvacuateDoneCnt   = prop.getValue("Evacuate-done",         int64_t(-1));
        mEvacuateDoneBytes = prop.getValue("Evacuate-done-bytes",   int64_t(-1));
        mEvacuateInFlight  = prop.getValue("Evacuate-in-flight",    int64_t(-1));
        const int numWrChunks = prop.getValue("Num-writable-chunks", 0);
        const int numWrDrives = prop.getValue("Num-wr-drives", mNumDrives);
        UpdateStorageTiers(prop.getValue("Storage-tiers"),
            numWrDrives, numWrChunks);
        UpdateChunkWritesPerDrive(numWrChunks, numWrDrives);
        const Properties::String* const cryptoKey = prop.getValue("CKey");
        if (cryptoKey) {
            const Properties::String* const keyId = prop.getValue("CKeyId");
            if (! keyId ||
                    ! ParseCryptoKey(*keyId, *cryptoKey) ||
                    ! mCryptoKeyValidFlag) {
                KFS_LOG_STREAM_ERROR << GetServerLocation() <<
                    " invalid heartbeat: invalid crypto key or id"
                    " seq:" << cseq <<
                KFS_LOG_EOM;
                mCryptoKeyValidFlag = false;
                return -1;
            }
            // Remove both crypto key and key ids from chunk server properties,
            // in order to prevent displaying these as counters.
            prop.remove("CKeyId");
            prop.remove("CKey");
        }
        if (mEvacuateInFlight == 0) {
            mChunksToEvacuate.Clear();
        }
        const time_t now = TimeNow();
        if (mEvacuateCnt > 0 && mEvacuateLastRateUpdateTime +
                sEvacuateRateUpdateInterval < now) {
            const time_t delta = now - mEvacuateLastRateUpdateTime;
            if (delta > 0 && delta <=
                    2 * sEvacuateRateUpdateInterval +
                    3 * sHeartbeatInterval) {
                mEvacuateCntRate = max(int64_t(0),
                    mEvacuateDoneCnt - mPrevEvacuateDoneCnt
                ) / double(delta);
                mEvacuateByteRate = max(int64_t(0),
                    mEvacuateDoneBytes - mPrevEvacuateDoneBytes
                ) / double(delta);
            } else {
                mEvacuateCntRate  = 0.;
                mEvacuateByteRate = 0.;
            }
            mEvacuateLastRateUpdateTime = now;
            mPrevEvacuateDoneCnt        = mEvacuateDoneCnt;
            mPrevEvacuateDoneBytes      = mEvacuateDoneBytes;
        }
        const int64_t srvLoad = prop.getValue(sSrvLoadPropName, int64_t(0));
        int64_t loadAvg;
        if (sSrvLoadSamplerSampleCount > 0) {
            if (mSrvLoadSampler.GetMaxSamples() != sSrvLoadSamplerSampleCount) {
                mSrvLoadSampler.SetMaxSamples(
                    sSrvLoadSamplerSampleCount, srvLoad, now);
            } else {
                mSrvLoadSampler.Put(srvLoad, now);
            }
            loadAvg = mSrvLoadSampler.GetLastFirstDiffByTime();
        } else {
            loadAvg = srvLoad;
        }
        if (loadAvg < 0) {
            KFS_LOG_STREAM_INFO <<
                " load average: " << loadAvg <<
                " resetting sampler" <<
            KFS_LOG_EOM;
            if (sSrvLoadSamplerSampleCount > 0) {
                mSrvLoadSampler.Reset(loadAvg, now);
            }
            loadAvg = 0;
        }
        mAllocSpace = mUsedSpace;
        if (mNumChunkWrites > 0) {
            // Overestimate allocated space to approximate the in flight
            // allocations that are not accounted for in this response.
            const int kInFlightEst = 16;
            mAllocSpace +=
                (mNumChunkWrites >= kInFlightEst * (kInFlightEst + 1)) ?
                mNumChunkWrites * ((int64_t)CHUNKSIZE / kInFlightEst) :
                min(kInFlightEst, max(mNumChunkWrites / 8, 1)) *
                    (int64_t)CHUNKSIZE;
        }
        mHeartbeatSent    = false;
        mHeartbeatSkipped = mLastHeartbeatSent + sHeartbeatInterval < now;
        mHeartbeatProperties.swap(prop);
        if (mTotalFsSpace < mTotalSpace) {
            mTotalFsSpace = mTotalSpace;
        }
        const int64_t delta = loadAvg - mLoadAvg;
        mLoadAvg = loadAvg;
        gLayoutManager.UpdateSrvLoadAvg(*this, delta, mStorageTiersInfoDelta);
        if (sHeartbeatLogInterval > 0 && mLastHeartBeatLoggedTime +
                sHeartbeatLogInterval <= mLastHeard) {
            mLastHeartBeatLoggedTime = mLastHeard;
            string hbp;
            mHeartbeatProperties.getList(hbp, " ", "");
            KFS_LOG_STREAM_INFO <<
                "===chunk=server: " << mLocation.hostname <<
                ":" << mLocation.port <<
                " responsive=" << IsResponsiveServer() <<
                " retiring="  << IsRetiring() <<
                " restarting=" << IsRestartScheduled() <<
                hbp <<
            KFS_LOG_EOM;
        }
    }
    op->resume();
    return 0;
}

///
/// The response sent by a chunkserver is of the form:
/// OK \r\n
/// Cseq: <seq #>\r\n
/// Status: <status> \r\n
/// {<other header/value pair>\r\n}*\r\n
///
/// @param[in] buf Buffer containing the response
/// @param[in] bufLen length of buf
/// @param[out] prop  Properties object with the response header/values
///
bool
ChunkServer::ParseResponse(istream& is, Properties &prop)
{
    string token;
    is >> token;
    // Response better start with OK
    if (token.compare("OK") != 0) {
        int maxLines = 32;
        do {
            KFS_LOG_STREAM_ERROR << GetServerLocation() <<
                " bad response header: " << token <<
            KFS_LOG_EOM;
        } while (--maxLines > 0 && getline(is, token));
        return false;
    }
    const char separator = ':';
    prop.loadProperties(is, separator);
    return true;
}

///
/// Request/responses are matched based on sequence #'s.
///
MetaChunkRequest*
ChunkServer::FindMatchingRequest(seq_t cseq)
{
    DispatchedReqs::iterator const it = mDispatchedReqs.find(cseq);
    if (it == mDispatchedReqs.end()) {
        return 0;
    }
    MetaChunkRequest* const op = it->second.first->second;
    mReqsTimeoutQueue.erase(it->second.first);
    sChunkOpsInFlight.erase(it->second.second);
    mDispatchedReqs.erase(it);
    return op;
}

int
ChunkServer::TimeSinceLastHeartbeat() const
{
    return (TimeNow() - mLastHeartbeatSent);
}

///
/// Queue an RPC request
///
void
ChunkServer::Enqueue(MetaChunkRequest* r, int timeout /* = -1 */)
{
    if (r->submitCount++ == 0) {
        r->submitTime = microseconds();
    }
    if (mDown || ! mNetConnection || ! mNetConnection->IsGood()) {
        r->status = -EIO;
        r->resume();
        return;
    }
    if (! mDispatchedReqs.insert(make_pair(
            r->opSeqno,
            make_pair(
                mReqsTimeoutQueue.insert(make_pair(
                    TimeNow() + (timeout < 0 ?
                        sRequestTimeout : timeout),
                    r
                )),
                sChunkOpsInFlight.insert(make_pair(
                    r->chunkId,
                    r
                ))
            ))).second) {
        panic("duplicate op sequence number");
    }
    if (r->op == META_CHUNK_REPLICATE) {
        KFS_LOG_STREAM_INFO << r->Show() << KFS_LOG_EOM;
    }
    EnqueueSelf(r);
}

void
ChunkServer::EnqueueSelf(MetaChunkRequest* r)
{
    if (mReAuthSentFlag || mAuthenticateOp) {
        if (mAuthPendingSeq < 0) {
            mAuthPendingSeq = r->opSeqno;
        }
        return;
    }
    IOBuffer& buf = mNetConnection->GetOutBuffer();
    ChunkServerRequest(*r, mOstream.Set(buf), buf);
    mOstream.Reset();
    if (mRecursionCount <= 0) {
        mNetConnection->StartFlush();
    }
}

int
ChunkServer::AllocateChunk(MetaAllocate* r, int64_t leaseId, kfsSTier_t tier)
{
    NewChunkInTier(tier);

    MetaChunkAllocate* const req = new MetaChunkAllocate(
        NextSeq(), r, shared_from_this(), leaseId, tier, r->maxSTier
    );
    size_t sz;
    if (0 <= leaseId && 0 < r->validForTime && 1 < (sz = r->servers.size())) {
        // Create synchronous replication chain access tokens. The write master
        // uses these tokens to setup synchronous replication chain.
        DelegationToken::TokenSeq tokenSeq =
            (DelegationToken::TokenSeq)gLayoutManager.GetRandom().Rand();
        const int16_t     kDelegationFlags = DelegationToken::kChunkServerFlag;
        ostringstream&    srvAccessOs      = GetTmpOStringStream();
        ostringstream&    chunkAccessOs    = GetTmpOStringStream1();
        CryptoKeys::KeyId keyId    = 0;
        CryptoKeys::Key   key;
        CryptoKeys::KeyId keyKeyId = 0;
        CryptoKeys::Key   keyKey;
        for (size_t i = 0; i < sz - 1; i++) {
            const kfsUid_t authUid = r->servers[i]->GetAuthUid();
            if (0 < i) {
                // Encrypt the session keys that will be passed via synchronous
                // replication chain.
                keyKeyId = keyId;
                keyKey   = key;
            }
            if (! r->servers[i + 1]->GetCryptoKey(keyId, key)) {
                req->status    = -EFAULT;
                req->statusMsg = "no valid crypto key";
                req->resume();
                return -EFAULT;
            }
            DelegationToken::WriteTokenAndSessionKey(
                srvAccessOs,
                authUid,
                tokenSeq,
                keyId,
                r->issuedTime,
                kDelegationFlags,
                r->validForTime,
                key.GetPtr(),
                key.GetSize(),
                0, // Subject pointer
                keyKeyId,
                0 < i ? keyKey.GetPtr()  : 0,
                0 < i ? keyKey.GetSize() : 0
            );
            srvAccessOs << "\n";
            ChunkAccessToken::WriteToken(
                chunkAccessOs,
                r->chunkId,
                authUid,
                tokenSeq,
                keyId,
                r->issuedTime,
                ChunkAccessToken::kAllowWriteFlag |
                    DelegationToken::kChunkServerFlag |
                    (r->clientCSAllowClearTextFlag ?
                        ChunkAccessToken::kAllowClearTextFlag : 0),
                LEASE_INTERVAL_SECS * 2,
                key.GetPtr(),
                key.GetSize()
            );
            chunkAccessOs << "\n";
            tokenSeq++;
        }
        req->chunkServerAccessStr = srvAccessOs.str();
        req->chunkAccessStr       = chunkAccessOs.str();
    }
    Enqueue(
        req,
        r->initialChunkVersion >= 0 ? sChunkReallocTimeout : sChunkAllocTimeout
    );
    return 0;
}

int
ChunkServer::DeleteChunk(chunkId_t chunkId)
{
    mChunksToEvacuate.Erase(chunkId);
    Enqueue(new MetaChunkDelete(NextSeq(), shared_from_this(), chunkId));
    return 0;
}

int
ChunkServer::GetChunkSize(fid_t fid, chunkId_t chunkId, seq_t chunkVersion,
    const string &pathname, bool retryFlag)
{
    Enqueue(new MetaChunkSize(NextSeq(), shared_from_this(),
        fid, chunkId, chunkVersion, pathname, retryFlag));
    return 0;
}

int
ChunkServer::BeginMakeChunkStable(fid_t fid, chunkId_t chunkId, seq_t chunkVersion)
{
    Enqueue(new MetaBeginMakeChunkStable(
        NextSeq(), shared_from_this(),
        mLocation, fid, chunkId, chunkVersion
    ), sMakeStableTimeout);
    return 0;
}

int
ChunkServer::MakeChunkStable(fid_t fid, chunkId_t chunkId, seq_t chunkVersion,
    chunkOff_t chunkSize, bool hasChunkChecksum, uint32_t chunkChecksum,
    bool addPending)
{
    Enqueue(new MetaChunkMakeStable(
        NextSeq(), shared_from_this(),
        fid, chunkId, chunkVersion,
        chunkSize, hasChunkChecksum, chunkChecksum, addPending
    ), sMakeStableTimeout);
    return 0;
}

int
ChunkServer::ReplicateChunk(fid_t fid, chunkId_t chunkId,
    const ChunkServerPtr& dataServer, const ChunkRecoveryInfo& recoveryInfo,
    kfsSTier_t minSTier, kfsSTier_t maxSTier,
    MetaChunkReplicate::FileRecoveryInFlightCount::iterator it)
{
    MetaChunkReplicate* const r = new MetaChunkReplicate(
        NextSeq(), shared_from_this(), fid, chunkId,
        dataServer->GetServerLocation(), dataServer, minSTier, maxSTier, it);
    if (! dataServer) {
        panic("invalid null replication source");
        r->status = -EINVAL;
        r->resume();
        return -EINVAL;
    }
    if (recoveryInfo.HasRecovery() && r->server == dataServer) {
        r->chunkVersion       = recoveryInfo.version;
        r->chunkOffset        = recoveryInfo.offset;
        r->striperType        = recoveryInfo.striperType;
        r->numStripes         = recoveryInfo.numStripes;
        r->numRecoveryStripes = recoveryInfo.numRecoveryStripes;
        r->stripeSize         = recoveryInfo.stripeSize;
        r->fileSize           = recoveryInfo.fileSize;
        r->dataServer.reset();
        r->srcLocation.hostname.clear();
        r->srcLocation.port = sMetaClientPort;
    }
    if (gLayoutManager.IsClientCSAuthRequired()) {
        r->issuedTime                 = TimeNow();
        r->validForTime               = (uint32_t)max(0,
            gLayoutManager.GetCSAccessValidForTime());
        r->clientCSAllowClearTextFlag =
            gLayoutManager.IsClientCSAllowClearText();
        if (mAuthUid == kKfsUserNone) {
            r->status    = -EFAULT;
            r->statusMsg = "destination server has invalid id";
            r->resume();
            return -EFAULT;
        }
        r->authUid  = mAuthUid;
        r->tokenSeq =
            (DelegationToken::TokenSeq)gLayoutManager.GetRandom().Rand();
        if (r->dataServer) {
            if (! r->dataServer->GetCryptoKey(r->keyId, r->key)) {
                r->status    = -EFAULT;
                r->statusMsg = "source has no valid crypto key";
                r->resume();
                return -EFAULT;
            }
        } else {
            const CryptoKeys* const keys = gNetDispatch.GetCryptoKeys();
            if (! keys || ! keys->GetCurrentKey(
                    r->keyId, r->key, r->validForTime)) {
                r->status    = -EFAULT;
                r->statusMsg = "has no current valid crypto key";
                r->resume();
                return -EFAULT;
            }
            if (mRecoveryMetaAccess.empty() || mRecoveryMetaAccessEndTime <
                    r->issuedTime + max(5 * 60, sReplicationTimeout * 2)) {
                ostringstream& os = GetTmpOStringStream();
                DelegationToken::WriteTokenAndSessionKey(
                    os,
                    r->authUid,
                    r->tokenSeq,
                    r->keyId,
                    r->issuedTime,
                    DelegationToken::kChunkServerFlag,
                    r->validForTime,
                    r->key.GetPtr(),
                    r->key.GetSize()
                );
                mRecoveryMetaAccessEndTime = r->issuedTime + r->validForTime;
                mRecoveryMetaAccess        = os.str();
            }
            r->metaServerAccess = mRecoveryMetaAccess;
        }
    } else {
        r->validForTime = 0;
    }
    mNumChunkWriteReplications++;
    NewChunkInTier(minSTier);
    Enqueue(r, sReplicationTimeout);
    return 0;
}

void
ChunkServer::NotifyStaleChunks(ChunkIdQueue& staleChunkIds,
    bool evacuatedFlag, bool clearStaleChunksFlag, const MetaChunkAvailable* ca)
{
    MetaChunkStaleNotify* const r = new MetaChunkStaleNotify(
        NextSeq(), shared_from_this(), evacuatedFlag,
        mStaleChunksHexFormatFlag, ca ? &ca->opSeqno : 0);
    if (clearStaleChunksFlag) {
        r->staleChunkIds.Swap(staleChunkIds);
    } else {
        r->staleChunkIds = staleChunkIds;
    }
    ChunkIdQueue::ConstIterator it(r->staleChunkIds);
    const chunkId_t*            id;
    while ((id = it.Next())) {
        mChunksToEvacuate.Erase(*id);
    }
    Enqueue(r);

}

void
ChunkServer::NotifyStaleChunk(chunkId_t staleChunkId, bool evacuatedFlag)
{
    MetaChunkStaleNotify * const r = new MetaChunkStaleNotify(
        NextSeq(), shared_from_this(), evacuatedFlag,
        mStaleChunksHexFormatFlag, 0);
    r->staleChunkIds.PushBack(staleChunkId);
    mChunksToEvacuate.Erase(staleChunkId);
    Enqueue(r);
}

void
ChunkServer::NotifyChunkVersChange(fid_t fid, chunkId_t chunkId, seq_t chunkVers,
    seq_t fromVersion, bool makeStableFlag, bool pendingAddFlag,
    MetaChunkReplicate* replicate)
{
    Enqueue(new MetaChunkVersChange(
        NextSeq(), shared_from_this(), fid, chunkId, chunkVers,
        fromVersion, makeStableFlag, pendingAddFlag, replicate),
        sMakeStableTimeout);
}

void
ChunkServer::SetRetiring()
{
    mIsRetiring = true;
    mRetireStartTime = TimeNow();
    mChunksToEvacuate.Clear();
    KFS_LOG_STREAM_INFO << GetServerLocation() <<
        " initiation of retire for " << mNumChunks << " chunks" <<
    KFS_LOG_EOM;
}

void
ChunkServer::Retire()
{
    Enqueue(new MetaChunkRetire(NextSeq(), shared_from_this()));
}

void
ChunkServer::SetProperties(const Properties& props)
{
    Enqueue(new MetaChunkSetProperties(NextSeq(), shared_from_this(), props));
}

void
ChunkServer::Restart(bool justExitFlag)
{
    mRestartQueuedFlag = true;
    if (justExitFlag) {
        Enqueue(new MetaChunkRetire(NextSeq(), shared_from_this()));
        return;
    }
    Enqueue(new MetaChunkServerRestart(NextSeq(), shared_from_this()));
}

int
ChunkServer::Heartbeat()
{
    if (! mHelloDone || mDown) {
        return -1;
    }
    assert(mNetConnection);
    const time_t now           = TimeNow();
    const int    timeSinceSent = (int)(now - mLastHeartbeatSent);
    if (mHeartbeatSent) {
        if (sHeartbeatTimeout >= 0 &&
                timeSinceSent >= sHeartbeatTimeout) {
            ostringstream& os = GetTmpOStringStream();
            os << "heartbeat timed out, sent: " <<
                timeSinceSent << " sec. ago";
            const string str = os.str();
            Error(str.c_str());
            return -1;
        }
        // If a request is outstanding, don't send one more
        if (! mHeartbeatSkipped &&
                mLastHeartbeatSent + sHeartbeatInterval < now) {
            mHeartbeatSkipped = true;
            KFS_LOG_STREAM_INFO << GetServerLocation() <<
                " skipping heartbeat send,"
                " last sent " << timeSinceSent << " sec. ago" <<
            KFS_LOG_EOM;
        }
        return(sHeartbeatTimeout < 0 ?
            sHeartbeatTimeout : sHeartbeatTimeout - timeSinceSent);
    }
    if (timeSinceSent >= sHeartbeatInterval) {
        KFS_LOG_STREAM_DEBUG << GetServerLocation() <<
            " sending heartbeat,"
            " last sent " << timeSinceSent << " sec. ago" <<
        KFS_LOG_EOM;
        mHeartbeatSent     = true;
        mLastHeartbeatSent = now;
        if (mReAuthSentFlag) {
            const time_t rem = mSessionExpirationTime + sRequestTimeout +
                    3 * (sHeartbeatTimeout + sHeartbeatInterval);
            if (rem < now) {
                Error("re-authentication timed out");
                return -1;
            }
            return (now - rem);
        }
        const AuthContext& authCtx = gLayoutManager.GetCSAuthContext();
        bool reAuthenticateFlag =
            (! mAuthenticateOp &&  mSessionExpirationTime <= now) ||
            mAuthCtxUpdateCount != authCtx.GetUpdateCount();
        if (reAuthenticateFlag && ! authCtx.IsAuthRequired()) {
            reAuthenticateFlag     = false;
            mSessionExpirationTime = now + kMaxSessionTimeoutSec;
            mAuthCtxUpdateCount    = authCtx.GetUpdateCount();
        }
        if (reAuthenticateFlag) {
            KFS_LOG_STREAM_INFO <<
                GetServerLocation() <<
                " requesting chunk server re-authentication:"
                " session expires in: " <<
                    (mSessionExpirationTime - now) << " sec." <<
                " auth ctx update count: " << mAuthCtxUpdateCount <<
                " vs: " << authCtx.GetUpdateCount() <<
            KFS_LOG_EOM;
        }
        Enqueue(new MetaChunkHeartbeat(
                NextSeq(),
                shared_from_this(),
                IsRetiring() ? int64_t(1) : (int64_t)mChunksToEvacuate.Size(),
                reAuthenticateFlag
            ),
            2 * sHeartbeatTimeout
        );
        mReAuthSentFlag = reAuthenticateFlag;
        return ((sHeartbeatTimeout >= 0 &&
                sHeartbeatTimeout < sHeartbeatInterval) ?
            sHeartbeatTimeout : sHeartbeatInterval
        );
    }
    return (sHeartbeatInterval - timeSinceSent);
}

int
ChunkServer::TimeoutOps()
{
    if (! mHelloDone || mDown) {
        return -1;
    }
    time_t const                     now = TimeNow();
    ReqsTimeoutQueue::iterator const end =
        mReqsTimeoutQueue.lower_bound(now);
    ReqsTimeoutQueue                 timedOut;
    for (ReqsTimeoutQueue::iterator it = mReqsTimeoutQueue.begin();
            it != end;
            ) {
        assert(it->second);
        DispatchedReqs::iterator const dri =
            mDispatchedReqs.find(it->second->opSeqno);
        if (dri == mDispatchedReqs.end()) {
            panic("invalid timeout queue entry");
        }
        sChunkOpsInFlight.erase(dri->second.second);
        mDispatchedReqs.erase(dri);
        timedOut.insert(*it);
        mReqsTimeoutQueue.erase(it++);
    }
    for (ReqsTimeoutQueue::iterator it = timedOut.begin();
            it != timedOut.end();
            ++it) {
        KFS_LOG_STREAM_INFO << GetServerLocation() <<
            " request timed out"
            " expired: "   << (now - it->first) <<
            " in flight: " << mDispatchedReqs.size() <<
            " total: "     << sChunkOpsInFlight.size() <<
            " "            << it->second->Show() <<
        KFS_LOG_EOM;
        it->second->status = -EIO;
        it->second->resume();
    }
    return (mReqsTimeoutQueue.empty() ?
        -1 : int(mReqsTimeoutQueue.begin()->first - now + 1));
}

void
ChunkServer::FailDispatchedOps()
{
    DispatchedReqs   reqs;
    ReqsTimeoutQueue reqTimeouts;
    mReqsTimeoutQueue.swap(reqTimeouts);
    mDispatchedReqs.swap(reqs);
    // Get all ops out of the in flight global queue first.
    for (DispatchedReqs::iterator it = reqs.begin();
            it != reqs.end();
            ++it) {
        sChunkOpsInFlight.erase(it->second.second);
    }
    // Fail in the same order as these were queued.
    for (DispatchedReqs::iterator it = reqs.begin();
            it != reqs.end();
            ++it) {
        MetaChunkRequest* const op = it->second.first->second;
        op->status = -EIO;
        op->resume();
    }
}

void
ChunkServer::GetRetiringStatus(ostream &os)
{
    if (! mIsRetiring) {
        return;
    }
    os <<
    "s="         << mLocation.hostname <<
    ", p="       << mLocation.port <<
    ", started=" << DisplayDateTime(
        int64_t(mRetireStartTime) * 1000 * 1000) <<
    ", numLeft=" << GetChunkCount() <<
    ", numDone=" << (mNumChunks - GetChunkCount()) <<
    "\t";
}

void
ChunkServer::GetEvacuateStatus(ostream &os)
{
    if (mIsRetiring || mEvacuateCnt <= 0) {
        return;
    }
    os <<
    "s="         << mLocation.hostname <<
    ", p="       << mLocation.port <<
    ", c="       << mEvacuateCnt <<
    ", b="       << mEvacuateBytes <<
    ", cDone="   << mEvacuateDoneCnt <<
    ", bDone="   << mEvacuateDoneBytes <<
    ", cFlight=" << mEvacuateInFlight <<
    ", cPend="   << mChunksToEvacuate.Size() <<
    ", cSec="    << mEvacuateCntRate <<
    ", bSec="    << mEvacuateByteRate <<
    ", eta="     << (mEvacuateByteRate > 0 ?
        mEvacuateBytes / mEvacuateByteRate : double(0)) <<
    "\t";
}

void
ChunkServer::Ping(ostream& os, bool useTotalFsSpaceFlag) const
{
    // for nodes taken out of write allocation, send the info back; this allows
    // the UI to color these nodes differently
    const double   utilisation  = GetSpaceUtilization(useTotalFsSpaceFlag);
    const bool     isOverloaded = utilisation >
        gLayoutManager.GetMaxSpaceUtilizationThreshold();
    const time_t  now           = TimeNow();
    const int64_t freeSpace     = max(int64_t(0), mTotalSpace - mUsedSpace);
    os << "s=" << mLocation.hostname << ", p=" << mLocation.port
        << ", rack=" << mRackId
        << ", used=" << mUsedSpace
        << ", free=" << freeSpace
        << ", total=" << mTotalFsSpace
        << ", util=" << setprecision(2) << fixed << utilisation * 1e2
        << ", nblocks=" << mNumChunks
        << ", lastheard=" << (now - mLastHeard)
        << ", ncorrupt=" << mNumCorruptChunks
        << ", nchunksToMove=" << mChunksToMove.Size()
        << ", numDrives=" << mNumDrives
        << ", numWritableDrives=" <<
            ((mIsRetiring || isOverloaded) ? 0 : mNumWritableDrives)
        << ", overloaded=" << (isOverloaded ? 1 : 0)
        << ", numReplications=" << GetNumChunkReplications()
        << ", numReadReplications=" << GetReplicationReadLoad()
        << ", good=" << (GetCanBeCandidateServerFlag() ? 1 : 0)
        << ", nevacuate=" << mEvacuateCnt
        << ", bytesevacuate=" << mEvacuateBytes
        << ", nlost=" << mLostChunks
        << ", nwrites=" << mNumChunkWrites
        << ", load=" << mLoadAvg
        << ", md5sum=" << mMd5Sum
        << ", tiers="
    ;
    const char* delim = "";
    for (size_t i = 0; i < kKfsSTierCount; i++) {
        const StorageTierInfo& info = mStorageTiersInfo[i];
        if (info.GetDeviceCount() <= 0) {
            continue;
        }
        os <<
            delim <<
            i <<
            ":" << info.GetDeviceCount() <<
            ":" << info.GetNotStableOpenCount() <<
            ":" << info.GetChunkCount() <<
            setprecision(2) << scientific <<
            ":" << (double)info.GetSpaceAvailable() <<
            ":" << (double)info.GetTotalSpace() <<
            fixed <<
            ":" << info.GetSpaceUtilization() * 1e2
        ;
        delim = ";";
    }
    os << ", lostChunkDirs=";
    LostChunkDirs::const_iterator it = mLostChunkDirs.begin();
    if (it != mLostChunkDirs.end()) {
        for (; ;) {
            os << *it;
            if (++it == mLostChunkDirs.end()) {
                break;
            }
            os << ";";
        }
    }
    os << "\t";
}

bool
ChunkServer::SendResponse(MetaRequest* op)
{
    if (! mNetConnection) {
        return true;
    }
    if (mAuthenticateOp && mAuthenticateOp != op) {
        op->next = 0;
        if (mPendingResponseOpsHeadPtr) {
            mPendingResponseOpsTailPtr->next = op;
        } else {
            mPendingResponseOpsHeadPtr = op;
        }
        mPendingResponseOpsTailPtr = op;
        return false;
    }
    IOBuffer& buf = mNetConnection->GetOutBuffer();
    op->response(mOstream.Set(buf), buf);
    mOstream.Reset();
    if (mRecursionCount <= 0) {
        mNetConnection->StartFlush();
    }
    return true;
}

bool
ChunkServer::ScheduleRestart(
    int64_t gracefulRestartTimeout, int64_t gracefulRestartAppendWithWidTimeout)
{
    if (mDown) {
        return true;
    }
    if (! mRestartScheduledFlag) {
        mRestartScheduledTime = TimeNow();
        mRestartScheduledFlag = true;
    }
    if ((mNumChunkWrites <= 0 &&
            mNumAppendsWithWid <= 0 &&
            mDispatchedReqs.empty()) ||
            mRestartScheduledTime +
                (mNumAppendsWithWid <= 0 ?
                    gracefulRestartTimeout :
                    max(gracefulRestartTimeout,
                        gracefulRestartAppendWithWidTimeout))
                < TimeNow()) {
        mDownReason = "restarting";
        Error("reconnect before restart");
        return true;
    }
    return false;
}

/* static */ string
ChunkServer::Escape(const char* buf, size_t len)
{
    const char* const kHexChars = "0123456789ABCDEF";
    string            ret;
    const char*       p = buf;
    const char* const e = p + len;
    ret.reserve(len);
    while (p < e) {
        const int c = *p++ & 0xFF;
        // For now do not escape '/' to make file names more readable.
        if (c <= ' ' || c >= 0xFF || strchr("!*'();:@&=+$,?#[]", c)) {
            ret.push_back('%');
            ret.push_back(kHexChars[(c >> 4) & 0xF]);
            ret.push_back(kHexChars[c & 0xF]);
        } else {
            ret.push_back(c);
        }
    }
    return ret;
}

void
ChunkServer::UpdateStorageTiersSelf(
    const char* buf,
    size_t      len,
    int         deviceCount,
    int         writableChunkCount)
{
    bool clearFlags[kKfsSTierCount];
    for (size_t i = 0; i < kKfsSTierCount; i++) {
        mStorageTiersInfoDelta[i] = mStorageTiersInfo[i];
        clearFlags[i]             = true;
    }
    if (buf) {
        kfsSTier_t tier;
        int32_t    deviceCount;
        int32_t    notStableOpenCount;
        int32_t    chunkCount;
        int64_t    spaceAvailable;
        int64_t    totalSpace;
        const char*       p = buf;
        const char* const e = p + len;
        while (p < e &&
                DecIntParser::Parse(p, e - p, tier) &&
                DecIntParser::Parse(p, e - p, deviceCount) &&
                DecIntParser::Parse(p, e - p, notStableOpenCount) &&
                DecIntParser::Parse(p, e - p, chunkCount) &&
                DecIntParser::Parse(p, e - p, spaceAvailable) &&
                DecIntParser::Parse(p, e - p, totalSpace)) {
            if (tier == kKfsSTierUndef) {
                continue;
            }
            if (tier < kKfsSTierMin) {
                tier = kKfsSTierMin;
            }
            if (tier > kKfsSTierMax) {
                tier = kKfsSTierMax;
            }
            mStorageTiersInfo[tier].Set(
                deviceCount,
                notStableOpenCount,
                chunkCount,
                min(totalSpace, spaceAvailable),
                totalSpace
            );
            clearFlags[tier] = false;
        }
    } else {
        // Backward compatibility: no storage tiers in the heartbeat.
        const kfsSTier_t tier         = kKfsSTierMax;
        const int64_t    totalFsSpace = max(mTotalSpace, mTotalFsSpace);
        mStorageTiersInfo[tier].Set(
            deviceCount,
            writableChunkCount,
            (int32_t)GetChunkCount(),
            min(GetFreeFsSpace(), totalFsSpace),
            totalFsSpace
        );
        clearFlags[tier] = false;
    }
    for (size_t i = 0; i < kKfsSTierCount; i++) {
        if (clearFlags[i]) {
            mStorageTiersInfo[i].Clear();
        }
        mStorageTiersInfoDelta[i].Delta(mStorageTiersInfo[i]);
    }
}

int
ChunkServer::Authenticate(IOBuffer& iobuf)
{
    if (! mAuthenticateOp) {
        return 0;
    }
    if (mAuthenticateOp->doneFlag) {
        if (mNetConnection->GetFilter()) {
            HandleRequest(EVENT_NET_WROTE, &mNetConnection->GetOutBuffer());
        }
        return 0;
    }
    if (mAuthenticateOp->contentBufPos <= 0) {
        gLayoutManager.GetCSAuthContext().Validate(*mAuthenticateOp);
    }
    const int rem = mAuthenticateOp->Read(iobuf);
    if (0 < rem) {
        mNetConnection->SetMaxReadAhead(rem);
        return rem;
    }
    if (! iobuf.IsEmpty() && mAuthenticateOp->status == 0) {
        mAuthenticateOp->status    = -EINVAL;
        mAuthenticateOp->statusMsg = "out of order data received";
    }
    gLayoutManager.GetCSAuthContext().Authenticate(*mAuthenticateOp, this, 0);
    if (mAuthenticateOp->status == 0) {
        if (mAuthName.empty()) {
            mAuthName = mAuthenticateOp->authName;
        } else if (! mAuthenticateOp->authName.empty() &&
                mAuthName != mAuthenticateOp->authName) {
            mAuthenticateOp->status    = -EINVAL;
            mAuthenticateOp->statusMsg = "authenticated name mismatch";
        } else if (! mAuthenticateOp->filter && mNetConnection->GetFilter()) {
            // An attempt to downgrade to clear text connection.
            mAuthenticateOp->status    = -EINVAL;
            mAuthenticateOp->statusMsg = "clear text communication not allowed";
        }
    }
    mAuthenticateOp->clnt     = this;
    mAuthenticateOp->doneFlag = true;
    mAuthCtxUpdateCount = gLayoutManager.GetCSAuthContext().GetUpdateCount();
    KFS_LOG_STREAM(mAuthenticateOp->status == 0 ?
        MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
        GetPeerName()           << " chunk server authentication"
        " type: "               << mAuthenticateOp->responseAuthType <<
        " name: "               << mAuthenticateOp->authName <<
        " filter: "             <<
            reinterpret_cast<const void*>(mAuthenticateOp->filter) <<
        " session expires in: " <<
            (mAuthenticateOp->sessionExpirationTime - TimeNow()) <<
        " response length: "    << mAuthenticateOp->responseContentLen <<
        " msg: "                << mAuthenticateOp->statusMsg <<
    KFS_LOG_EOM;
    HandleRequest(EVENT_CMD_DONE, mAuthenticateOp);
    return 0;
}

bool
ChunkServer::ParseCryptoKey(
    const Properties::String& keyId,
    const Properties::String& key)
{
    mCryptoKeyValidFlag = false;
    if (keyId.empty() && key.empty()) {
        // No key.
        return true;
    }
    if (keyId.empty() || key.empty()) {
        KFS_LOG_STREAM_ERROR << GetPeerName() << " " << GetServerLocation() <<
            " empty crypto key" << (keyId.empty() ? " id" : "") <<
        KFS_LOG_EOM;
        return false;
    }
    const char* p = keyId.GetPtr();
    if (! DecIntParser::Parse(p, keyId.GetSize(), mCryptoKeyId)) {
        KFS_LOG_STREAM_ERROR << GetPeerName() << " " << GetServerLocation() <<
            " failed to parse cryto key id: " << keyId <<
        KFS_LOG_EOM;
        return false;
    }
    if (! mCryptoKey.Parse(key.GetPtr(), (int)key.GetSize())) {
        KFS_LOG_STREAM_ERROR << GetPeerName() << " " << GetServerLocation() <<
            " invalid crypto key"
            " length: " << key.GetSize() <<
        KFS_LOG_EOM;
        return false;
    }
    mCryptoKeyValidFlag = true;
    return true;
}

    /* virtual */ bool
ChunkServer::Verify(
    string&       ioFilterAuthName,
    bool          inPreverifyOkFlag,
    int           inCurCertDepth,
    const string& inPeerName,
    int64_t       inEndTime,
    bool          inEndTimeValidFlag)
{
    KFS_LOG_STREAM_DEBUG << GetPeerName() <<
        " chunk server auth. verify:" <<
        " name: "           << inPeerName <<
        " prev: "           << ioFilterAuthName <<
        " preverify: "      << inPreverifyOkFlag <<
        " depth: "          << inCurCertDepth <<
        " end time: +"      << (inEndTime - time(0)) <<
        " end time valid: " << inEndTimeValidFlag <<
    KFS_LOG_EOM;
    // Do no allow to renegotiate and change the name.
    string authName = inPeerName;
    if (! inPreverifyOkFlag ||
            (inCurCertDepth == 0 &&
            ((gLayoutManager.GetCSAuthContext().HasUserAndGroup() ?
                gLayoutManager.GetCSAuthContext().GetUid(
                    authName) == kKfsUserNone :
                ! gLayoutManager.GetCSAuthContext().RemapAndValidate(
                    authName)) ||
            (! mAuthName.empty() && authName != mAuthName)))) {
        KFS_LOG_STREAM_ERROR << GetPeerName() <<
            " chunk server autentication failure:"
            " peer: "  << inPeerName <<
            " name: "  << authName <<
            " depth: " << inCurCertDepth <<
            " is not valid" <<
            (mAuthName.empty() ? "" : "prev name: ") << mAuthName <<
        KFS_LOG_EOM;
        mAuthName.clear();
        ioFilterAuthName.clear();
        return false;
    }
    if (inCurCertDepth == 0) {
        ioFilterAuthName = inPeerName;
        mAuthName        = authName;
        if (inEndTimeValidFlag && inEndTime < mSessionExpirationTime) {
            mSessionExpirationTime = inEndTime;
        }
    }
    return true;
}

} // namespace KFS
