//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/06
// Author: Sriram Rao, Mike Ovsiannikov
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
// Chunk server state machine implementation.
//
//----------------------------------------------------------------------------

#include "ChunkServer.h"
#include "CSMap.h"
#include "LayoutManager.h"
#include "NetDispatch.h"
#include "LogWriter.h"
#include "kfstree.h"
#include "util.h"

#include "qcdio/QCUtils.h"
#include "qcdio/qcdebug.h"
#include "qcdio/qcstutils.h"

#include "common/MdStream.h"
#include "common/MsgLogger.h"
#include "common/kfserrno.h"
#include "common/RequestParser.h"
#include "common/IntToString.h"

#include "kfsio/Globals.h"
#include "kfsio/DelegationToken.h"
#include "kfsio/ChunkAccessToken.h"
#include "kfsio/CryptoKeys.h"

#include <boost/static_assert.hpp>

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
using std::pair;
using libkfsio::globalNetManager;

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
        panic("chunk server: failed to calculate sha1");
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

template<typename T>
class ChunkIdInserter
{
public:
    ChunkIdInserter(
        T& dest)
        : mDest(dest)
        {}
    void operator()(chunkId_t id)
        { mDest.Insert(id); }
private:
    T& mDest;
};

template<typename T>
class ChunkIdRemover
{
public:
    ChunkIdRemover(
        T& dest)
        : mDest(dest),
          mRemovedFlag(false)
        {}
    void operator()(chunkId_t id)
    {
        if (0 < mDest.Erase(id)) {
            mRemovedFlag = true;
        }
    }
    bool GetRemovedFlag() const
        { return mRemovedFlag; }
private:
    T&   mDest;
    bool mRemovedFlag;
};

template<typename T>
inline static void
ProcessInFlightChunks(T& dest, const MetaChunkRequest& op)
{
    if (op.chunkVersion < 0) {
        return;
    }
    const MetaChunkRequest::ChunkIdSet* const ids = op.GetChunkIds();
    if (ids) {
        MetaChunkRequest::ChunkIdSet::ConstIterator it(*ids);
        const chunkId_t*            id;
        while ((id = it.Next())) {
            dest(*id);
        }
    } else if (0 <= op.chunkId) {
        dest(op.chunkId);
    }
}

template<typename T>
inline static void
AppendInFlightChunks(T& dest, const MetaChunkRequest& op)
{
    ChunkIdInserter<T> inserter(dest);
    ProcessInFlightChunks(inserter, op);
}

template<typename T>
inline static bool
RemoveInFlightChunks(T& dest, const MetaChunkRequest& op)
{
    if (dest.IsEmpty()) {
        return false;
    }
    ChunkIdRemover<T> remover(dest);
    ProcessInFlightChunks(remover, op);
    return remover.GetRemovedFlag();
}

static inline void
MarkSubmitted(MetaRequest& req)
{
    // Bump submit count to prevent it from going into transaction log.
    if (0 == req.submitCount) {
        req.submitTime  = microseconds();
        req.processTime = 0;
        req.submitCount = 1;
    }
}

const time_t kMaxSessionTimeoutSec = 10 * 365 * 24 * 60 * 60;

int ChunkServer::sHeartbeatTimeout         = 60;
int ChunkServer::sHeartbeatInterval        = 20;
int ChunkServer::sMinInactivityInterval    = 8;
int ChunkServer::sHeartbeatSkippedInterval = -1;
int ChunkServer::sHeartbeatLogInterval     = 1000;
int ChunkServer::sChunkAllocTimeout        = 40;
int ChunkServer::sChunkReallocTimeout      = 75;
int ChunkServer::sMakeStableTimeout        = 330;
int ChunkServer::sReplicationTimeout       = 510;
int ChunkServer::sRequestTimeout           = 600;
int ChunkServer::sMetaClientPort           = 0;
int ChunkServer::sTimedoutExpireTime       = 10;
size_t ChunkServer::sMaxChunksToEvacuate  = 2 << 10; // Max queue size
// sHeartbeatInterval * sSrvLoadSamplerSampleCount -- boxcar FIR filter
// if sSrvLoadSamplerSampleCount > 0
int ChunkServer::sSrvLoadSamplerSampleCount = 0;
bool ChunkServer::sRestartCSOnInvalidClusterKeyFlag = false;
ChunkServer* ChunkServer::sChunkServersPtr[kChunkSrvListsCount] = { 0, 0 };
int ChunkServer::sChunkServerCount    = 0;
int ChunkServer::sMaxChunkServerCount = 0;
int ChunkServer::sPendingHelloCount    = 0;
int ChunkServer::sMinHelloWaitingBytes = 0;
int64_t ChunkServer::sHelloBytesCommitted = 0;
int64_t ChunkServer::sHelloBytesInFlight  = 0;
int64_t ChunkServer::sMaxHelloBufferBytes = 256 << 20;
int64_t ChunkServer::sMaxPendingHelloLogByteCount = 4 << 20;
int64_t ChunkServer::sPendingHelloLogByteCount    = 0;
int ChunkServer::sMaxReadAhead = 4 << 10;
int ChunkServer::sMaxPendingOpsCount = 128;
int ChunkServer::sEvacuateRateUpdateInterval = 120;
size_t ChunkServer::sChunkDirsCount = 0;
MsgLogger::LogLevel ChunkServer::sHeartbeatLogLevel = MsgLogger::kLogLevelINFO;

// Bigger than the default MAX_RPC_HEADER_LEN: max heartbeat size.
const int kMaxRequestResponseHeader = 64 << 10;

/* static */ inline int
ChunkServer::GetMaxPendingHelloBytes()
{
    return (sMaxPendingHelloLogByteCount -
        MetaRequest::GetLogWriter().GetPendingAckBytesOverage());
}

inline ChunkServer::DispatchedReqs::iterator&
ChunkServer::GetDispatchedReqsIterator(ChunkServer::DispatchedReqsIterator& it)
{
    void* const storage = &it.mStorage; // Convert to void to eliminate warning.
    return *reinterpret_cast<DispatchedReqs::iterator*>(storage);
}

inline
ChunkServer::DispatchedReqsIterator::DispatchedReqsIterator()
{
    BOOST_STATIC_ASSERT(sizeof(DispatchedReqs::iterator) <= sizeof(mStorage));
    QCVERIFY(
        new (&mStorage) DispatchedReqs::iterator() ==
        &GetDispatchedReqsIterator(*this)
    );
}

template<typename T>
inline static void
Destroy(T& ptr) { ptr.~T(); }

inline
ChunkServer::DispatchedReqsIterator::~DispatchedReqsIterator()
{
    Destroy(GetDispatchedReqsIterator(*this));
}

void ChunkServer::SetParameters(const Properties& prop, int clientPort)
{
    sHeartbeatTimeout = max(4, prop.getValue(
        "metaServer.chunkServer.heartbeatTimeout",
        sHeartbeatTimeout));
    sMinInactivityInterval = max(2, prop.getValue(
        "metaServer.chunkServer.minInactivityInterval",
        sMinInactivityInterval));
    sHeartbeatInterval = max(3, prop.getValue(
        "metaServer.chunkServer.heartbeatInterval",
        sHeartbeatInterval));
    sHeartbeatSkippedInterval = max(3, prop.getValue(
        "metaServer.chunkServer.heartbeatSkippedInterval",
        sHeartbeatSkippedInterval));
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
    sTimedoutExpireTime = max(2, prop.getValue(
        "metaServer.chunkServer.timedoutExpireTime",
        sTimedoutExpireTime));
    sMaxReadAhead = max(1 << 10, prop.getValue(
        "metaServer.chunkServer.maxReadAhead",
        sMaxReadAhead));
    sMaxPendingOpsCount = max(8, prop.getValue(
        "metaServer.chunkServer.maxPendingOpsCount",
        sMaxPendingOpsCount));
    if (clientPort > 0) {
        sMetaClientPort = clientPort;
    }
    sRestartCSOnInvalidClusterKeyFlag = prop.getValue(
        "metaServer.chunkServer.restartOnInvalidClusterKey",
        sRestartCSOnInvalidClusterKeyFlag ? 1 : 0) != 0;
    const Properties::String* logLeveStr = prop.getValue(
        "metaServer.chunkServer.heartbeatLogLevel");
    if (logLeveStr) {
        const MsgLogger::LogLevel logLevel =
            MsgLogger::GetLogLevelId(logLeveStr->c_str());
        if (MsgLogger::kLogLevelUndef != logLevel) {
            sHeartbeatLogLevel = logLevel;
        }
    }
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
    ReqOstream ros(os);
    req.request(ros, buf);
}

inline void
ChunkServerResponse(MetaRequest& req, ostream& os, IOBuffer& buf)
{
    ReqOstream ros(os);
    req.response(ros, buf);
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
    if (! mNetConnection || ! mHelloDone) {
        return;
    }
    for (size_t i = 0; i < kKfsSTierCount; i++) {
        mStorageTiersInfoDelta[i].Clear();
    }
    if (IsValidSTier(tier) &&
            mStorageTiersInfo[tier].GetDeviceCount() > 0) {
        mStorageTiersInfoDelta[tier] = mStorageTiersInfo[tier];
        mStorageTiersInfo[tier].AddInFlightAlloc();
        mStorageTiersInfoDelta[tier].Delta(mStorageTiersInfo[tier]);
    }
    mAllocSpace += CHUNKSIZE;
    UpdateChunkWritesPerDrive(mNumChunkWrites + 1, mNumWritableDrives);
    gLayoutManager.UpdateSrvLoadAvg(*this, 0, mStorageTiersInfoDelta);
}

void
ChunkServer::HelloDone(const MetaHello* r)
{
    if (r ? (this != &*r->server || this != r->clnt) :
            ! mReplayFlag) {
        panic("chunk server: invalid hello done invocation");
    }
    if (mHelloDone) {
        return;
    }
    mHelloDone        = true;
    mHeartbeatSent    = true;
    mHelloProcessFlag = true;
    if (r) {
        mPendingHelloNotifyFlag = r->pendingNotifyFlag;
    }
    if (! mNetConnection) {
        gLayoutManager.Disconnected(*this);
    }
    if (mDown || mReplayFlag || ! mNetConnection) {
        return;
    }
    mLastHeartbeatSent = TimeNow();
    mHeartbeatSent     = true;
    const bool kReauthenticateFlag = false;
    Enqueue(*(new MetaChunkHeartbeat(mSelfPtr,
            mIsRetiring ? int64_t(1) : (int64_t)mChunksToEvacuate.Size(),
            kReauthenticateFlag, sMaxPendingOpsCount,
            min(sHeartbeatTimeout, 2 * sMinInactivityInterval))),
        2 * sHeartbeatTimeout
    );
}

/* static */ KfsCallbackObj*
ChunkServer::Create(const NetConnectionPtr& conn)
{
    if (! conn || ! conn->IsGood()) {
        return 0;
    }
    if (! gLayoutManager.IsPrimary()) {
        KFS_LOG_STREAM_DEBUG << conn->GetPeerName() <<
            " meta server node is not primary, closing connection" <<
        KFS_LOG_EOM;
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
    const int maxPendingByteCount = GetMaxPendingHelloBytes();
    if (maxPendingByteCount < sPendingHelloLogByteCount) {
        KFS_LOG_STREAM_ERROR << conn->GetPeerName() <<
            " chunk servers: "                << sChunkServerCount <<
            " over pending hello log limit: " << sPendingHelloLogByteCount <<
            " max: "                          << maxPendingByteCount <<
            " closing connection" <<
        KFS_LOG_EOM;
        return 0;
    }
    return CreateSelf(conn, ServerLocation());
}

/* static */ ChunkServerPtr
ChunkServer::Create(const NetConnectionPtr& conn, const ServerLocation& loc)
{
    ChunkServer* const ret = CreateSelf(conn, loc);
    return (ret ? ret->GetSharedPtr() : ChunkServerPtr());
}

/* static */ ChunkServer*
ChunkServer::CreateSelf(const NetConnectionPtr& conn, const ServerLocation& loc)
{
    if (! conn) {
        return 0;
    }
    ChunkServer& srv = *(new ChunkServer(
        conn,
        conn->IsGood() ?  conn->GetPeerName() : string("replay"),
        ! conn->IsGood()
    ));
    srv.mSelfPtr.reset(&srv);
    if (loc.IsValid()) {
        srv.SetServerLocation(loc);
    }
    return &srv;
}

inline ChunkServerPtr
ChunkServer::GetSelfPtr()
{
    return (mSelfPtr ? mSelfPtr : shared_from_this());
}

inline void
ChunkServer::Submit(MetaRequest& op)
{
    mPendingOpsCount++;
    submit_request(&op);
}

inline void
ChunkServer::RemoveInFlight(MetaChunkRequest& req)
{
    if (MetaChunkRequest::kNullIterator != req.inFlightIt) {
        if (req.chunkId < 0 && ! req.GetChunkIds()) {
            panic("chunk server invalid chunks in flight op");
            return;
        }
        sChunkOpsInFlight.erase(req.inFlightIt);
        req.inFlightIt = MetaChunkRequest::kNullIterator;
    }
}

ChunkServer::ChunkServer(
    const NetConnectionPtr& conn,
    const string&           peerName,
    bool                    replayFlag)
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
      mRetiredFlag(false),
      mHibernateDownTime(-1),
      mDisconnectReason(),
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
      mNumObjects(0),
      mNumWrObjects(0),
      mDispatchedReqs(),
      mLogCompletionInFlightReqs(),
      mReqsTimeoutQueue(),
      mDoneTimedoutChunks(),
      mDoneTimedoutList(),
      mTmpReqQueue(),
      mLogInFlightCount(0),
      mLostChunks(0),
      mUptime(0),
      mHeartbeatProperties(),
      mRestartScheduledFlag(false),
      mRestartQueuedFlag(false),
      mRestartScheduledTime(0),
      mLastHeartBeatLoggedTime(mLastHeartbeatSent - 240 * 60 * 60),
      mLastCountersUpdateTime(mLastHeartBeatLoggedTime),
      mLastSentTime(mLastHeartBeatLoggedTime),
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
      mPendingResponseOps(),
      mLastChunksInFlight(),
      mStaleChunkIdsInFlight(),
      mHelloChunkIds(),
      mHelloPendingStaleChunks(),
      mHelloProcessFlag(false),
      mHelloDoneCount(0),
      mHelloResumeCount(0),
      mHelloResumeFailedCount(0),
      mShortRpcFormatFlag(false),
      mHibernatedGeneration(0),
      mChannelId(-1),
      mPendingOpsCount(0),
      mPendingHelloNotifyFlag(false),
      mPendingByeFlag(false),
      mStoppedServicingFlag(false),
      mReplayFlag(replayFlag),
      mStorageTiersInfo(),
      mStorageTiersInfoDelta()
{
    assert(mNetConnection);
    ChunkServersList::Init(*this);
    PendingHelloList::Init(*this);
    LogInFlightReqs::Init(mLogCompletionInFlightReqs);
    DoneTimedoutList::Init(mDoneTimedoutList);
    SET_HANDLER(this, &ChunkServer::HandleRequest);
    mNetConnection->SetInactivityTimeout(sHeartbeatInterval);
    mNetConnection->SetMaxReadAhead(sMaxReadAhead);
    for (size_t i = 0; i < kKfsSTierCount; i++) {
        mCanBeCandidateServerFlags[i] = false;
    }
    QCStMutexLocker locker(gNetDispatch.GetClientManagerMutex());
    ChunkServersList::PushBack(sChunkServersPtr, *this);
    sChunkServerCount++;
    KFS_LOG_STREAM(mReplayFlag ?
            MsgLogger::kLogLevelDEBUG :
            MsgLogger::kLogLevelINFO)<<
        "new ChunkServer " << reinterpret_cast<const void*>(this) <<
        " "         << GetPeerName() <<
        " seq: "    << mSeqNo <<
        " replay: " << mReplayFlag <<
        " total: "  << sChunkServerCount <<
    KFS_LOG_EOM;
}

ChunkServer::~ChunkServer()
{
    if (0 != mRecursionCount || mSelfPtr || 0 != mPendingOpsCount ||
            ! LogInFlightReqs::IsEmpty(mLogCompletionInFlightReqs)) {
        panic("chunk server: invalid destructor invocation");
    }
    KFS_LOG_STREAM_DEBUG << GetServerLocation() <<
        " ~ChunkServer " << reinterpret_cast<const void*>(this) <<
        " "              << GetPeerName() <<
        " / "            << GetServerLocation() <<
        " seq: "         << mSeqNo <<
        " total: "       << sChunkServerCount <<
    KFS_LOG_EOM;
    if (mNetConnection) {
        mNetConnection->Close();
    }
    RemoveFromPendingHelloList();
    MetaRequest::Release(mHelloOp);
    MetaRequest::Release(mAuthenticateOp);
    ReleasePendingResponses();
    mRecursionCount = 0xF000DEAD; // To catch double delete.
    QCStMutexLocker locker(gNetDispatch.GetClientManagerMutex());
    if (sChunkServerCount <= 0 ||
            ! ChunkServersList::IsInList(sChunkServersPtr, *this)) {
        panic("chunk server:"
            " invalid chunk server list or destructor invocation");
    } else {
        ChunkServersList::Remove(sChunkServersPtr, *this);
        sChunkServerCount--;
    }
}

void
ChunkServer::ReleasePendingResponses(bool sendResponseFlag /* = false */)
{
    PendingResponseOps queue;
    queue.PushBack(mPendingResponseOps);
    MetaRequest* op;
    while ((op = queue.PopFront())) {
        if (! sendResponseFlag || SendResponse(op)) {
            MetaRequest::Release(op);
        }
    }
}

void
ChunkServer::AddToPendingHelloList()
{
    if (PendingHelloList::IsInList(sChunkServersPtr, *this)) {
        return;
    }
    if (! mHelloOp || mHelloOp->bufferBytes <= 0) {
        sMinHelloWaitingBytes = 0;
    } else if (sPendingHelloCount <= 0 ||
            mHelloOp->bufferBytes < sMinHelloWaitingBytes) {
        sMinHelloWaitingBytes = mHelloOp->bufferBytes;
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
                bytesAvail < ptr->mHelloOp->bufferBytes) {
            if (ptr->mHelloOp->bufferBytes < minHelloSize) {
                minHelloSize = ptr->mHelloOp->bufferBytes;
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
    if (req->bufferBytes <= 0) {
        return 0;
    }
    if (req->bytesReceived < 0) {
        req->bytesReceived = 0;
    }
    if (req->bufferBytes <= avail) {
        sHelloBytesCommitted += req->bufferBytes;
    }
    return (avail - req->bufferBytes);
}

/* static */ void
ChunkServer::PutHelloBytes(MetaHello* req)
{
    if (! req || req->bytesReceived < 0) {
        return;
    }
    if (sHelloBytesCommitted < req->bufferBytes) {
        panic("chunk server: invalid hello request byte counter");
        sHelloBytesCommitted = 0;
    } else {
        sHelloBytesCommitted -= req->bufferBytes;
    }
    if (sHelloBytesInFlight < req->bytesReceived) {
        panic("chunk server: invalid hello received byte counter");
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
    if (mRecursionCount < 0 || ! mSelfPtr) {
        panic("chunk server: invalid recursion count or self reference");
    }
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
                        "hello failure"));
                break;
            }
            if (0 < retval || mAuthenticateOp || ! mNetConnection) {
                break; // Need more data, or down
            }
            msgLen = 0;
        }
        if (mNetConnection && ! gotMsgHdr &&
                iobuf.BytesConsumable() > kMaxRequestResponseHeader) {
            iobuf.Clear();
            Error(mHelloDone ?
                "request or response header length exceeds max allowed" :
                "hello message header length exceeds max allowed");
        }
        break;
    }

    case EVENT_CMD_DONE: {
        assert(0 < mPendingOpsCount);
        mPendingOpsCount--;
        MetaRequest* const op = reinterpret_cast<MetaRequest*>(data);
        assert(data &&
            (mHelloDone || op == mAuthenticateOp ||
                op->op == META_CHUNK_SERVER_HELLO ||
                (META_CHUNK_SERVER_BYE == op->op && 0 == mPendingOpsCount &&
                ! mNetConnection)));
        if (META_CHUNK_SERVER_HELLO == op->op) {
            if (! mHelloDone && 0 != op->status &&
                    -EAGAIN != op->status && mDisconnectReason.empty()) {
                if (op->statusMsg.empty()) {
                    mDisconnectReason = "hello error";
                } else {
                    mDisconnectReason = op->statusMsg;
                }
            }
            MetaHello& helloOp = *static_cast<MetaHello*>(op);
            if (mDown || helloOp.replayFlag || this != &*helloOp.server ||
                    1 != sHelloInFlight.erase(helloOp.location)) {
                panic("chunk server: invalid hello completion");
            }
            if (0 < helloOp.bufferBytes && helloOp.supportsResumeFlag &&
                    ! mReplayFlag) {
                sPendingHelloLogByteCount -= helloOp.bufferBytes;
                if (sPendingHelloLogByteCount < 0) {
                    panic("chunk server: invalid pending hello log byte count");
                    sPendingHelloLogByteCount = 0;
                }
            }
            PutHelloBytes(&helloOp);
            if (! mNetConnection) {
                if (mHelloDone) {
                    SubmitMetaBye();
                } else {
                    ForceDown();
                }
            }
        } else if (META_CHUNK_SERVER_BYE == op->op) {
            if (! mPendingByeFlag || ! mHelloDone) {
                panic("chunk server: invalid bye completion");
            }
            mPendingByeFlag = false;
            // Layout manager forces down chunk server on successful bye
            // completion, otherwise new primary will re-schedule bye.
        }
        const bool deleteOpFlag = op != mAuthenticateOp;
        if (SendResponse(op) && deleteOpFlag) {
            MetaRequest::Release(op);
        }
        break;
    }

    case EVENT_NET_WROTE:
        if (mAuthenticateOp) {
            if (! mNetConnection) {
                MetaRequest::Release(mAuthenticateOp);
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
            MetaRequest::Release(mAuthenticateOp);
            mAuthenticateOp  = 0;
            KFS_LOG_STREAM_INFO << GetServerLocation() <<
                " / " << GetPeerName() <<
                (mHelloDone ? " re-" : " ") <<
                "authentication complete:"
                " session expires in: " <<
                    (mSessionExpirationTime - TimeNow()) << " sec." <<
                " pending seq:"
                " requests: "  << mAuthPendingSeq <<
                    " +" << (0 <= mAuthPendingSeq ?
                        mSeqNo - mAuthPendingSeq : seq_t(-1)) <<
                " responses: " << (mPendingResponseOps.IsEmpty() ? seq_t(-1) :
                    mPendingResponseOps.Front()->opSeqno) <<
                " "            << (mPendingResponseOps.IsEmpty() ? seq_t(-1) :
                        mPendingResponseOps.Back()->opSeqno) <<
            KFS_LOG_EOM;
            if (mHelloDone && 0 <= mAuthPendingSeq) {
                // Enqueue RPCs that were waiting for authentication to finish.
                DispatchedReqs::const_iterator it = mDispatchedReqs.lower_bound(
                    CseqToVrLogSeq(mAuthPendingSeq));
                mAuthPendingSeq = -1;
                while (it != mDispatchedReqs.end()) {
                    MetaChunkRequest* const op = it->second.second;
                    ++it;
                    EnqueueSelf(*op);
                }
            }
            const bool kSendResponseFlag = true;
            ReleasePendingResponses(kSendResponseFlag);
            break;
        }
        if (mNetConnection && ! mDisconnectReason.empty() &&
                ! mNetConnection->IsWriteReady()) {
            Error(mDisconnectReason.c_str());
        }
        mLastSentTime = TimeNow();
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
        if (mAuthenticateOp &&
                mNetConnection && mNetConnection->IsGood() &&
                (filter = mNetConnection->GetFilter()) &&
                filter->IsShutdownReceived()) {
            // Do not allow to shutdown filter with data in flight.
            if (mNetConnection->GetInBuffer().IsEmpty() &&
                    mNetConnection->GetOutBuffer().IsEmpty()) {
                // Ssl shutdown from the other side.
                if (mNetConnection->Shutdown() == 0) {
                    KFS_LOG_STREAM_DEBUG << GetServerLocation() <<
                        " / " << GetPeerName() <<
                        " shutdown filter: " <<
                            (const void*)mNetConnection->GetFilter() <<
                    KFS_LOG_EOM;
                    if (! mNetConnection->GetFilter()) {
                        HandleRequest(
                            EVENT_NET_WROTE, &mNetConnection->GetOutBuffer());
                        break;
                    }
                }
            } else {
                KFS_LOG_STREAM_ERROR << GetServerLocation() <<
                    " / " << GetServerLocation() <<
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
        if (mRecursionCount <= 1 && ! mReplayFlag) {
            const int hbTimeout = Heartbeat();
            const int opTimeout = TimeoutOps();
            if (mNetConnection) {
                mNetConnection->StartFlush();
            }
            if (mNetConnection && mNetConnection->IsGood()) {
                mNetConnection->SetInactivityTimeout(
                    NetManager::Timer::MinTimeout(hbTimeout, opTimeout));
            }
        }
    } else {
        if (code != EVENT_INACTIVITY_TIMEOUT) {
            mLastHeartbeatSent = TimeNow();
        }
        if (mRecursionCount <= 1 && mNetConnection) {
            mNetConnection->StartFlush();
        }
    }
    if (mRecursionCount <= 0) {
        panic("chunk server handle event: invalid recursion count");
    }
    mRecursionCount--;
    if (mRecursionCount <= 0 && mDown && mPendingOpsCount <= 0) {
        mSelfPtr.reset(); // Unref / delete self
    }
    return 0;
}

void
ChunkServer::RemoveFromWriteAllocation()
{
    if (! mHelloDone) {
        return;
    }
    // Take out the server from write allocation / placement.
    mTotalSpace   = 0;
    mTotalFsSpace = 0;
    mAllocSpace   = 0;
    mUsedSpace    = 0;
    const int64_t delta = -mLoadAvg;
    mLoadAvg      = 0;
    const int64_t objDelta = -mNumObjects;
    mNumObjects   = 0;
    const int64_t wrObjDelta = -mNumWrObjects;
    mNumWrObjects = 0;
    ClearStorageTiers();
    gLayoutManager.UpdateSrvLoadAvg(*this, delta, mStorageTiersInfoDelta);
    gLayoutManager.UpdateObjectsCount(*this, objDelta, wrObjDelta);
    UpdateChunkWritesPerDrive(0, 0);
    gLayoutManager.Disconnected(*this);
}

void
ChunkServer::ForceDown()
{
    if (mDown) {
        return;
    }
    KFS_LOG_STREAM((mNetConnection && ! mReplayFlag) ?
            MsgLogger::kLogLevelWARN : MsgLogger::kLogLevelDEBUG) <<
        GetServerLocation() <<
        " / " << GetPeerName() <<
        " forcing down chunk server" <<
    KFS_LOG_EOM;
    if (mNetConnection) {
        mNetConnection->Close();
        mNetConnection->GetInBuffer().Clear();
        mNetConnection.reset();
        RemoveFromPendingHelloList();
        RemoveFromWriteAllocation();
    }
    MetaRequest::Release(mHelloOp);
    mHelloOp = 0;
    mDown = true;
    FailDispatchedOps("chunk server down");
    assert(sChunkDirsCount >= mChunkDirInfos.size());
    sChunkDirsCount -= min(sChunkDirsCount, mChunkDirInfos.size());
    mChunkDirInfos.clear();
    ReleasePendingResponses();
    if (mRecursionCount <= 0 && mPendingOpsCount <= 0) {
        mSelfPtr.reset(); // Unref / delete self
    }
}

void
ChunkServer::SetCanBeChunkMaster(bool flag)
{
    if (mCanBeChunkMaster == flag) {
        return;
    }
    if (! mHelloDone) {
        mCanBeChunkMaster = flag;
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
ChunkServer::SubmitMetaBye()
{
    if (! mHelloDone) {
        panic("chunk server: submit bye while hello in flight");
        return;
    }
    KFS_LOG_STREAM_DEBUG << GetServerLocation() <<
        " / "        << GetPeerName() <<
        " "          << reinterpret_cast<const void*>(this) <<
        " bye:"
        " pending: " << mPendingByeFlag <<
        " chunks: "  << GetChunkCount() <<
        " index: "   << GetIndex() <<
    KFS_LOG_EOM;
    if (mPendingByeFlag) {
        return;
    }
    mPendingByeFlag = true;
    MetaBye& mb = *(new MetaBye(mSelfPtr));
    mb.chunkCount             = GetChunkCount();
    mb.cIdChecksum            = GetChecksum();
    mb.authUid                = mAuthUid;
    mb.logInFlightCount       = mLogInFlightCount;
    mb.completionInFlightFlag =
        ! LogInFlightReqs::IsEmpty(mLogCompletionInFlightReqs);
    mb.clnt                   = this;
    Submit(mb);
}

void
ChunkServer::Error(const char* errorMsg, bool ignoreReplayFlag)
{
    if (mDown || (mReplayFlag && ! ignoreReplayFlag)) {
        return;
    }
    if (! mSelfPtr) {
        panic("ChunkServer::Error: invalid null self reference");
        return;
    }
    if (mNetConnection) {
        KFS_LOG_STREAM(mReplayFlag ?
                MsgLogger::kLogLevelDEBUG :
                (ignoreReplayFlag ?
                    MsgLogger::kLogLevelINFO :
                    MsgLogger::kLogLevelERROR)) << GetServerLocation() <<
            " / " << GetPeerName() <<
            " chunk server down"
            " reason: " << (errorMsg ? errorMsg : "unspecified") <<
            " socket:"
            " good: "   << mNetConnection->IsGood() <<
            " status: " << mNetConnection->GetErrorMsg() <<
            " "         << mNetConnection->GetErrorCode() <<
        KFS_LOG_EOM;
        if (mDownReason.empty()) {
            if (mRestartQueuedFlag) {
                mDownReason = "restart";
            } else if (mHelloDone || 0 < mPendingOpsCount) {
                if (errorMsg) {
                    if (mReplayFlag) {
                        mDownReason = "replay: ";
                    }
                    mDownReason += errorMsg;
                }
                const size_t kMaxMsgSize = 128;
                if (! mReplayFlag && mDownReason.size() < kMaxMsgSize) {
                    const string msg = mNetConnection->GetErrorMsg();
                    if (! msg.empty() &&
                            mDownReason.size() + msg.size() < kMaxMsgSize) {
                        if (! mDownReason.empty()) {
                            mDownReason += "; ";
                        }
                        mDownReason += msg;
                    }
                }
            }
        }
        mNetConnection->Close();
        mNetConnection->GetInBuffer().Clear();
        mNetConnection.reset();
        RemoveFromPendingHelloList();
        RemoveFromWriteAllocation();
        MetaRequest::Release(mAuthenticateOp);
        mAuthenticateOp = 0;
    }
    if (mHelloDone) {
        // Ensure proper event ordering in the logger queue, such that down
        // event is executed after all RPCs in logger queue.
        SubmitMetaBye();
        return;
    }
    if (0 < mPendingOpsCount) {
        // If hello is already in flight, i.e. not done, and being logged.
        // In this case bye must be issued to ensure replay correctness.
        HelloInFlight::const_iterator const it = sHelloInFlight.find(
            GetServerLocation());
        if (sHelloInFlight.end() == it || this != &*it->second->server) {
            panic("chunk server: invalid pending ops count");
        }
        return;
    }
    ForceDown();
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
    if (0 <= ParseCommand(iobuf, msgLen, &op, 0, mShortRpcFormatFlag)) {
        if (! mSelfPtr) {
            KFS_LOG_STREAM_ERROR <<
                GetHostPortStr() + " / " + GetPeerName() <<
                " server down: " << op->Show() <<
            KFS_LOG_EOM;
            MetaRequest::Release(op);
            return 0;
        }
        op->setChunkServer(mSelfPtr);
        return op;
    }
    ShowLines(MsgLogger::kLogLevelERROR,
        GetHostPortStr() + " / " + GetPeerName() + " " +
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
          mFieldIdx(mStartFieldIdx),
          mIdOnlyFlag(false)
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
                    case 0: break;
                    case 1: mCur.chunkId = mVal;
                        if (mIdOnlyFlag) {
                            mFieldIdx = 1;
                            mVal      = 0;
                            return &mCur;
                        }
                        break;
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
            mFieldIdx = mStartFieldIdx;
            mVal      = 0;
            return &mCur;
        }
        return 0;
    }
    void SetIdOnly(bool flag)
    {
        mFieldIdx      = 1;
        mStartFieldIdx = 1;
        mIdOnlyFlag    = flag;
    }
    bool IsError() const { return (mFieldIdx != mStartFieldIdx); }
private:
    IOBuffer::ByteIterator mIt;
    ChunkInfo              mCur;
    int64_t                mVal;
    bool                   mPrevSpaceFlag;
    int                    mStartFieldIdx;
    int                    mFieldIdx;
    bool                   mIdOnlyFlag;

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
    KFS_LOG_STREAM_ERROR <<
        mHelloOp->location <<
        " / "              << GetPeerName() <<
        " hello error: "   << mHelloOp->status <<
        " "                << mHelloOp->statusMsg <<
        " seq: "           << mHelloOp->opSeqno <<
        " "                << mHelloOp->Show() <<
    KFS_LOG_EOM;
    PutHelloBytes(mHelloOp);
    mNetConnection->GetInBuffer().Clear();
    mNetConnection->SetMaxReadAhead(0);
    mNetConnection->SetInactivityTimeout(sRequestTimeout);
    if (mDisconnectReason.empty()) {
        mDisconnectReason = mHelloOp->statusMsg;
    }
    if (SendResponse(mHelloOp)) {
        MetaRequest::Release(mHelloOp);
    }
    mHelloOp = 0;
    return 0;
}

/// Case #1: Handle Hello message from a chunkserver that
/// just connected to us.
int
ChunkServer::HandleHelloMsg(IOBuffer* iobuf, int msgLen)
{
    assert(! mHelloDone);

    const bool hasHelloOpFlag = mHelloOp != 0;
    if (! hasHelloOpFlag) {
        if (0 < mPendingOpsCount) {
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
                " unexpected request while processing hello,"
                " pending ops: " << mPendingOpsCount <<
            KFS_LOG_EOM;
            return -1;
        }
        MetaRequest * const op = mAuthenticateOp ? mAuthenticateOp :
            GetOp(*iobuf, msgLen, "invalid hello");
        if (! op) {
            return -1;
        }
        if (! mAuthenticateOp && op->op == META_AUTHENTICATE) {
            mShortRpcFormatFlag = op->shortRpcFormatFlag;
            mReAuthSentFlag = false;
            mAuthenticateOp = static_cast<MetaAuthenticate*>(op);
        }
        if (mAuthenticateOp) {
            iobuf->Consume(msgLen);
            return Authenticate(*iobuf);
        }
        if (op->op != META_CHUNK_SERVER_HELLO) {
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
        if (op->op != META_CHUNK_SERVER_HELLO) {
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
                " unexpected request, expected hello" <<
            KFS_LOG_EOM;
            MetaRequest::Release(op);
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
                return DeclareHelloError(-EACCES, msg.c_str());
            }
        }
        mHelloOp = static_cast<MetaHello*>(op);
        mHelloOp->maxPendingOpsCount = sMaxPendingOpsCount;
        mHelloOp->authName           = mAuthName;
        // Mark hello by setting bytesReceived to a negative value, such that
        // PutHelloBytes(), if invoked, has no effect, in the case if hello does
        // not go through the buffer commit, i.e. GetHelloBytes() is not invoked.
        mHelloOp->bytesReceived      = -1;
        if (mAuthName.empty() &&
                gLayoutManager.GetCSAuthContext().IsAuthRequired()) {
            return DeclareHelloError(-EPERM, "authentication required");
        }
        if (! mAuthName.empty()) {
            if (! ParseCryptoKey(mHelloOp->cryptoKeyId, mHelloOp->cryptoKey,
                    mShortRpcFormatFlag)) {
                mHelloOp = 0;
                MetaRequest::Release(op);
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
        mHelloOp->bufferBytes = mHelloOp->contentLength;
        if (! gLayoutManager.Validate(*mHelloOp)) {
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
                " hello"
                " seq: "      << mHelloOp->opSeqno <<
                " location: " << mHelloOp->location <<
                " error: "    << op->status <<
                " "           << op->statusMsg <<
                " "           << mHelloOp->Show() <<
            KFS_LOG_EOM;
            if (mHelloOp->status != -EBADCLUSTERKEY) {
                mHelloOp = 0;
                MetaRequest::Release(op);
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
            MetaRequest::Release(op);
            return -1;
        }
        if (mHelloOp->status == 0 &&
                sMaxHelloBufferBytes < mHelloOp->bufferBytes) {
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
                " hello: "
                " location: "             << mHelloOp->location <<
                " buffer bytes: "         << mHelloOp->bufferBytes <<
                " content length: "       << mHelloOp->contentLength <<
                " exceeds max. allowed: " << sMaxHelloBufferBytes <<
            KFS_LOG_EOM;
            mHelloOp = 0;
            MetaRequest::Release(op);
            return -1;
        }
        if (mHelloOp->fileSystemId <= 0 &&
                (0 <= mHelloOp->resumeStep ||
                    0 < mHelloOp->numChunks ||
                    0 < mHelloOp->numNotStableAppendChunks ||
                    0 < mHelloOp->numNotStableChunks) &&
                gLayoutManager.IsFileSystemIdRequired()) {
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
                " hello:"
                " location: "               << mHelloOp->location <<
                " invalid file system id: " << mHelloOp->fileSystemId <<
            KFS_LOG_EOM;
            mHelloOp = 0;
            MetaRequest::Release(op);
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
                mHelloOp->resumeStep               = -1;
            } else {
                return DeclareHelloError(-EINVAL, "file system id mismatch");
            }
        }
        const int64_t kMinEntrySize = 4;
        if (mHelloOp->status == 0 && mHelloOp->contentLength + (1 << 10) <
                kMinEntrySize * (
                    (int64_t)max(0, mHelloOp->numChunks) +
                    (int64_t)max(0, mHelloOp->numNotStableAppendChunks) +
                    (int64_t)max(0, mHelloOp->numNotStableChunks) +
                    (int64_t)max(0, mHelloOp->numMissingChunks) +
                    (int64_t)max(0, mHelloOp->numPendingStaleChunks))) {
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
                " malformed hello:"
                " location: "             << mHelloOp->location <<
                " content length: "       << mHelloOp->contentLength <<
                " invalid chunk counts: " << mHelloOp->numChunks <<
                " + "                     <<
                    mHelloOp->numNotStableAppendChunks <<
                " + "                     << mHelloOp->numNotStableChunks <<
                " + "                     << mHelloOp->numMissingChunks <<
                " + "                     << mHelloOp->numPendingStaleChunks <<
            KFS_LOG_EOM;
            mHelloOp = 0;
            MetaRequest::Release(op);
            return -1;
        }
        if (0 == mHelloOp->status &&
                0 < mHelloOp->bufferBytes &&
                iobuf->BytesConsumable() < mHelloOp->bufferBytes &&
                GetHelloBytes(mHelloOp) < 0) {
            KFS_LOG_STREAM_INFO << GetPeerName() <<
                " location: "    << mHelloOp->location <<
                " hello:"
                " bytes:"
                " received: "    << mHelloOp->bytesReceived <<
                " buffered: "    << mHelloOp->bufferBytes <<
                " pending list:"
                " ops: "         << sPendingHelloCount <<
                " bytes:"
                " committed: "   << sHelloBytesCommitted <<
                " received: "    << sHelloBytesInFlight <<
                " min waiting: " << sMinHelloWaitingBytes <<
            KFS_LOG_EOM;
            mNetConnection->SetMaxReadAhead(0);
            AddToPendingHelloList();
            return 1;
        }
    }
    if (0 == mHelloOp->status && mHelloOp->supportsResumeFlag &&
            0 < mHelloOp->bufferBytes) {
        const int maxPendingByteCount = GetMaxPendingHelloBytes();
        if (maxPendingByteCount <
                mHelloOp->bufferBytes + sPendingHelloLogByteCount) {
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
                " chunk servers: "                << sChunkServerCount <<
                " over pending hello log limit: " <<
                    mHelloOp->bufferBytes + sPendingHelloLogByteCount <<
                " max: "                          << maxPendingByteCount <<
            KFS_LOG_EOM;
            return DeclareHelloError(
                -EBUSY, "over max hello log bytes in flight");
        }
    }
    // make sure we have the chunk ids...
    if (0 < mHelloOp->contentLength) {
        const int nAvail = iobuf->BytesConsumable();
        if (nAvail < mHelloOp->contentLength) {
            // need to wait for data...
            if (mHelloOp->status != 0) {
                mHelloOp->contentLength -= nAvail;
                iobuf->Clear(); // Discard content.
            } else if (nAvail > mHelloOp->bytesReceived) {
                sHelloBytesInFlight += nAvail - mHelloOp->bytesReceived;
                mHelloOp->bytesReceived = nAvail;
            }
            mNetConnection->SetMaxReadAhead(max(sMaxReadAhead,
                mHelloOp->status == 0 ?
                    mHelloOp->contentLength - nAvail :
                    kMaxRequestResponseHeader
            ));
            return 1;
        }
        const int contentLength = mHelloOp->contentLength;
        // Emit log message to have time stamp of when hello is
        // fully received, and parsing of chunk lists starts.
        KFS_LOG_STREAM(hasHelloOpFlag ?
                MsgLogger::kLogLevelINFO :
                MsgLogger::kLogLevelDEBUG) << GetPeerName() <<
            " location: "    << mHelloOp->location <<
            " hello:"
            " status: "      << mHelloOp->status <<
            " bytes:"
            " received: "    << mHelloOp->bytesReceived <<
            " buffered: "    << mHelloOp->bufferBytes <<
            " done: "        << contentLength <<
            " pending list:"
            " ops: "         << sPendingHelloCount <<
            " bytes:"
            " committed: "   << sHelloBytesCommitted <<
            " received: "    << sHelloBytesInFlight <<
            " min waiting: " << sMinHelloWaitingBytes <<
        KFS_LOG_EOM;
        mHelloOp->chunks.clear();
        mHelloOp->notStableChunks.clear();
        mHelloOp->notStableAppendChunks.clear();
        mHelloOp->missingChunks.clear();
        if (0 == mHelloOp->status) {
            const size_t numStable(max(0, mHelloOp->numChunks));
            mHelloOp->chunks.reserve(numStable);
            const size_t nonStableAppendNum(
                max(0, mHelloOp->numNotStableAppendChunks));
            mHelloOp->notStableAppendChunks.reserve(nonStableAppendNum);
            const size_t nonStableNum(max(0, mHelloOp->numNotStableChunks));
            mHelloOp->notStableChunks.reserve(nonStableNum);
            // get the chunkids
            istream& is = mIStream.Set(iobuf, contentLength);
            HexChunkInfoParser hexParser(*iobuf, mHelloOp->noFidsFlag);
            for (int j = 0; j < 3 && ! hexParser.IsError(); ++j) {
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
                            if (c.chunkId < 0 || c.chunkVersion < 0) {
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
                            if (c.chunkId < 0 || c.chunkVersion < 0) {
                                break;
                            }
                            chunks.push_back(c);
                        }
                    }
                }
            }
            for (int k = 0; k < 2 && ! hexParser.IsError(); k++) {
                int count = 0 == k ?
                    mHelloOp->numMissingChunks :
                    mHelloOp->numPendingStaleChunks;
                if (count <= 0) {
                    continue;
                }
                MetaHello::ChunkIdList& list = 0 == k ?
                    mHelloOp->missingChunks : mHelloOp->pendingStaleChunks;
                list.reserve((size_t)count);
                if (16 == mHelloOp->contentIntBase) {
                    hexParser.SetIdOnly(true);
                    const MetaHello::ChunkInfo* c;
                    while (count-- > 0 && (c = hexParser.Next()) &&
                            0 <= c->chunkId) {
                        list.push_back(c->chunkId);
                    }
                } else {
                    chunkId_t chunkId;
                    while (count-- > 0 && (is >> chunkId) && 0 <= chunkId) {
                        list.push_back(chunkId);
                    }
                }
            }
            mIStream.Reset();
            iobuf->Consume(contentLength);
            if (mHelloOp->chunks.size() != numStable ||
                    mHelloOp->notStableAppendChunks.size() !=
                        nonStableAppendNum ||
                    mHelloOp->notStableChunks.size() != nonStableNum ||
                    (size_t)max(0, mHelloOp->numMissingChunks) !=
                        mHelloOp->missingChunks.size() ||
                    (size_t)max(0, mHelloOp->numPendingStaleChunks) !=
                        mHelloOp->pendingStaleChunks.size() ||
                    hexParser.IsError()) {
                KFS_LOG_STREAM_ERROR << GetPeerName() <<
                    " location: " << mHelloOp->location <<
                    " invalid or short chunk list:"
                    " expected: " << mHelloOp->numChunks <<
                    "/"           << mHelloOp->numNotStableAppendChunks <<
                    "/"           << mHelloOp->numNotStableChunks <<
                    "/"           << mHelloOp->numMissingChunks <<
                    "/"           << mHelloOp->numPendingStaleChunks <<
                    " actual: "   << mHelloOp->chunks.size() <<
                    "/"           << mHelloOp->notStableAppendChunks.size() <<
                    "/"           << mHelloOp->notStableChunks.size() <<
                    "/"           << mHelloOp->missingChunks.size() <<
                    "/"           << mHelloOp->pendingStaleChunks.size() <<
                    " last good chunk: " <<
                        (mHelloOp->chunks.empty() ? -1 :
                        mHelloOp->chunks.back().chunkId) <<
                    "/" << (mHelloOp->notStableAppendChunks.empty() ? -1 :
                        mHelloOp->notStableAppendChunks.back().chunkId) <<
                    "/" << (mHelloOp->notStableChunks.empty() ? -1 :
                        mHelloOp->notStableChunks.back().chunkId) <<
                    "/" << (mHelloOp->missingChunks.empty() ? -1 :
                        mHelloOp->missingChunks.back()) <<
                    "/" << (mHelloOp->pendingStaleChunks.empty() ? -1 :
                        mHelloOp->pendingStaleChunks.back()) <<
                    " content length: " << contentLength <<
                KFS_LOG_EOM;
                PutHelloBytes(mHelloOp);
                MetaRequest::Release(mHelloOp);
                mHelloOp = 0;
                return -1;
            }
        } else {
            iobuf->Consume(contentLength);
        }
    }
    if (mHelloOp->status == -EBADCLUSTERKEY || mHelloOp->retireFlag) {
        iobuf->Clear();
        PutHelloBytes(mHelloOp);
        if (! mNetConnection) {
            MetaRequest::Release(mHelloOp);
            mHelloOp = 0;
            return -1;
        }
        KFS_LOG_STREAM_INFO << GetPeerName() <<
            " location: " << mHelloOp->location <<
            " "           << mHelloOp->Show() <<
            " status: "   << mHelloOp->status <<
            " msg: "      << mHelloOp->statusMsg <<
            " initiating chunk server restart" <<
        KFS_LOG_EOM;
        // Tell him hello is OK in order to make the restart work.
        mHelloOp->status = 0;
        IOBuffer& ioBuf = mNetConnection->GetOutBuffer();
        mOstream.Set(ioBuf);
        ChunkServerResponse(*mHelloOp, mOstream, ioBuf);
        if (mHelloOp->retireFlag) {
            MetaChunkRetire retire(mSelfPtr);
            ChunkServerRequest(retire, mOstream, ioBuf);
            mDisconnectReason = "retiring chunk server";
        } else {
            MetaChunkServerRestart restart(mSelfPtr);
            ChunkServerRequest(restart, mOstream, ioBuf);
            mDisconnectReason = "restarting chunk server";
        }
        mOstream.Reset();
        mNetConnection->SetMaxReadAhead(0);
        mNetConnection->SetInactivityTimeout(sRequestTimeout);
        MetaRequest::Release(mHelloOp);
        mHelloOp = 0;
        // Do not declare error, outbound data still pending.
        // Create response and set timeout, the chunk server
        // should disconnect when it restarts.
        return 0;
    }
    if (0 != mHelloOp->status) {
        PutHelloBytes(mHelloOp);
        mHelloOp->bufferBytes = 0;
    }
    if (! gLayoutManager.CanAddServer(sHelloInFlight.size())) {
        return DeclareHelloError(-EBUSY, "no slots available");
    }
    if (! sHelloInFlight.insert(
            make_pair(mHelloOp->location, mHelloOp)).second) {
        return DeclareHelloError(-EBUSY, "hello is in progress");
    }
    mNetConnection->SetMaxReadAhead(sMaxReadAhead);
    mHelloOp->peerName        = GetPeerName();
    mHelloOp->clnt            = this;
    mHelloOp->server          = mSelfPtr;
    mLastHeard                = TimeNow();
    mLastHeartbeatSent        = mLastHeard;
    mUptime                   = mHelloOp->uptime;
    mNumAppendsWithWid        = mHelloOp->numAppendsWithWid;
    mStaleChunksHexFormatFlag = mHelloOp->staleChunksHexFormatFlag;
    for (size_t i = 0; i < kKfsSTierCount; i++) {
        mStorageTiersInfoDelta[i].Clear();
    }
    UpdateChunkWritesPerDrive((int)(mHelloOp->notStableAppendChunks.size() +
        mHelloOp->notStableChunks.size()), mNumWritableDrives);
    // Emit message to time parse.
    KFS_LOG_STREAM_INFO << GetPeerName() <<
        " submit hello:"
        " seq: "    << mHelloOp->opSeqno <<
        " status: " << mHelloOp->status <<
        " "         << mHelloOp->statusMsg <<
        " "         << mHelloOp->Show() <<
    KFS_LOG_EOM;
    if (mAuthName.empty()) {
        mAuthUid = kKfsUserNone;
    } else {
        mAuthUid = MakeAuthUid(*mHelloOp, mAuthName);
    }
    SetServerLocation(mHelloOp->location);
    mHelloOp->authUid = mAuthUid;
    mNumChunks              = max(int64_t(0), mHelloOp->totalChunks);
    mMd5Sum                 = mHelloOp->md5sum;
    mHelloDoneCount         = mHelloOp->helloDoneCount;
    mHelloResumeCount       = mHelloOp->helloResumeCount;
    mHelloResumeFailedCount = mHelloOp->helloResumeFailedCount;
    mShortRpcFormatFlag     = mHelloOp->shortRpcFormatFlag;
    mChannelId              = mHelloOp->channelId;
    if (0 < mHelloOp->bufferBytes && mHelloOp->supportsResumeFlag) {
        sPendingHelloLogByteCount += mHelloOp->bufferBytes;
    }
    MetaHello& op = *mHelloOp;
    mHelloOp = 0;
    Submit(op);
    return 0;
}

void
ChunkServer::SetServerLocation(const ServerLocation& loc)
{
    if (mLocation == loc) {
        return;
    }
    if (mLocation.IsValid() || mHelloDone || ! loc.IsValid()) {
        panic("invalid attempt to chunk chunk server location");
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
    if (op->op == META_CHUNK_SERVER_HELLO) {
        KFS_LOG_STREAM_ERROR << GetServerLocation() <<
            " unexpected hello op: " << op->Show() <<
        KFS_LOG_EOM;
        MetaRequest::Release(op);
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
    op->fromClientSMFlag   = false;
    op->clnt               = this;
    op->authUid            = mAuthUid;
    op->shortRpcFormatFlag = mShortRpcFormatFlag;
    if (sMaxPendingOpsCount <= mPendingOpsCount) {
        KFS_LOG_STREAM_ERROR << GetServerLocation() <<
            " exceeded limit of " << sMaxPendingOpsCount <<
            " pending ops: "      << mPendingOpsCount <<
            " failing op: "       << op->Show() <<
        KFS_LOG_EOM;
        op->statusMsg = "exceeded pending ops limit of ";
        AppendDecIntToString(op->statusMsg, sMaxPendingOpsCount);
        op->status = -ESERVERBUSY;
        HandleRequest(EVENT_CMD_DONE, op);
    } else {
        KFS_LOG_STREAM_DEBUG << GetServerLocation() <<
            " +seq: " << op->opSeqno <<
            " "       << op->Show() <<
        KFS_LOG_EOM;
        Submit(*op);
    }
    return 0;
}

void
ChunkServer::UpdateSpace(const MetaChunkEvacuate& op)
{
    if (! mNetConnection || ! mHelloDone) {
        return;
    }
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
        UpdateStorageTiers(op.GetStorageTiersInfo(), wrDrives, mNumChunkWrites,
            mShortRpcFormatFlag);
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
    Properties prop(mShortRpcFormatFlag ? 16 : 10);
    const bool ok = ParseResponse(*iobuf, msgLen, prop);
    if (! ok) {
        KFS_LOG_STREAM_ERROR << GetServerLocation() <<
            " bad response header: " <<
            IOBuffer::DisplayData(*iobuf, msgLen) <<
        KFS_LOG_EOM;
        return -1;
    }
    // Message is ready to be pushed down.  So remove it.
    iobuf->Consume(msgLen);

    const seq_t             cseq = prop.getValue(
        mShortRpcFormatFlag ? "c" : "Cseq", (seq_t) -1);
    MetaChunkRequest* const op   = FindMatchingRequest(cseq);
    if (! op) {
        // Most likely op was timed out, or chunk server sent response
        // after re-connect.
        KFS_LOG_STREAM_INFO << GetServerLocation() <<
            " unable to find command for response seq: " << cseq <<
        KFS_LOG_EOM;
        return 0;
    }
    const time_t now = TimeNow();
    mLastHeard = now;
    op->statusMsg = prop.getValue(
        mShortRpcFormatFlag ? "m" : "Status-message", string());
    op->status    = prop.getValue(mShortRpcFormatFlag ? "s" : "Status", -1);
    if (op->status < 0) {
        op->status = -KfsToSysErrno(-op->status);
    }
    op->handleReply(prop);
    KFS_LOG_STREAM_DEBUG << GetServerLocation() <<
        " cs-reply:"
        " -seq: "   << op->opSeqno <<
        " log: "    << op->logCompletionSeq <<
        " status: " << op->status <<
        " "         << op->statusMsg <<
        " "         << op->Show() <<
    KFS_LOG_EOM;
    if (op->op != META_CHUNK_HEARTBEAT) {
        op->resume();
        return 0;
    }
    if (0 != op->status) {
        const string errMsg = "heartbeat error: " + op->statusMsg;
        Error(errMsg.c_str());
        op->resume();
        return 0;
    }
    mHeartbeatSent    = false;
    mHeartbeatSkipped = mLastHeartbeatSent +
        max(sHeartbeatInterval, sHeartbeatSkippedInterval) < now;
    if (static_cast<const MetaChunkHeartbeat*>(op)->omitCountersFlag ||
            0 != op->status) {
        op->resume();
        return 0;
    }
    // Heartbeat response uses decimal notation, except key id, to make
    // chunk server counters response backward compatible.
    prop.setIntBase(10);
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
    const int     numWrChunks = prop.getValue("Num-writable-chunks", 0);
    const int     numWrDrives = prop.getValue("Num-wr-drives", mNumDrives);
    const int64_t numObjs     = prop.getValue("Num-objs", int64_t(0));
    const int64_t numWrObjs   =
        min(numObjs, prop.getValue("Num-wr-objs", int64_t(0)));
    if (mNumWrObjects != numWrObjs || numObjs != mNumObjects) {
        gLayoutManager.UpdateObjectsCount(*this,
            numObjs - mNumObjects, numWrObjs - mNumWrObjects);
    }
    const bool kHexFormatFlag = false;
    UpdateStorageTiers(prop.getValue("Storage-tiers"),
        numWrDrives, numWrChunks, kHexFormatFlag);
    UpdateChunkWritesPerDrive(numWrChunks, numWrDrives);
    const Properties::String* const cryptoKey = prop.getValue("CKey");
    if (cryptoKey) {
        const Properties::String* const keyId = prop.getValue("CKeyId");
        if (! keyId ||
                ! ParseCryptoKey(*keyId, *cryptoKey, mShortRpcFormatFlag) ||
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
        KFS_LOG_STREAM_INFO << GetServerLocation() <<
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
        KFS_LOG_STREAM_START(sHeartbeatLogLevel, logStream);
            ostream& os = logStream.GetStream();
            os <<
                "===chunk=server: "
                ";location="   << mLocation <<
                ",time-usec="  << microseconds() <<
                ",responsive=" << IsResponsiveServer() <<
                ",retiring="   << mIsRetiring <<
                ",restarting=" << IsRestartScheduled()
            ;
            for (Properties::iterator it = mHeartbeatProperties.begin();
                    mHeartbeatProperties.end() != it;
                    ++it) {
                os << ',' << it->first << '=' << it->second;
            }
        KFS_LOG_STREAM_END;
    }
    mLastCountersUpdateTime = now;
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
ChunkServer::ParseResponse(IOBuffer& iobuf, int msgLen, Properties& prop)
{
    if (msgLen < 3) {
        return false;
    }
    const char separator = ':';
    IOBuffer::iterator const it = iobuf.begin();
    if (it != iobuf.end() && msgLen <= it->BytesConsumable()) {
        const char*       ptr = it->Consumer();
        const char* const end = ptr + msgLen;
        if ('O' != ptr[0] || 'K' != ptr[1] || ' ' < (ptr[2] & 0xFF)) {
            return false;
        }
        ptr += 3;
        prop.loadProperties(ptr, end - ptr, separator);
    } else {
        char buf[3];
        if (iobuf.CopyOut(buf, 3) < 3 || 'O' != buf[0] || 'K' != buf[1] ||
                ' ' < (buf[2] & 0xFF)) {
            return false;
        }
        istream& is = mIStream.Set(iobuf, msgLen);
        is.ignore(3);
        prop.loadProperties(is, separator);
        mIStream.Reset();
    }
    return true;
}

///
/// Request/responses are matched based on sequence #'s.
///
MetaChunkRequest*
ChunkServer::FindMatchingRequest(const MetaVrLogSeq& cseq)
{
    DispatchedReqs::iterator const it = mDispatchedReqs.find(cseq);
    if (it == mDispatchedReqs.end()) {
        return 0;
    }
    MetaChunkRequest* const op = it->second.second;
    mReqsTimeoutQueue.erase(it->second.first);
    it->second.first = ReqsTimeoutQueue::iterator();
    if (op->logCompletionSeq.IsValid()) {
        LogInFlightReqs::PushBack(mLogCompletionInFlightReqs, *op);
    } else {
        RemoveInFlight(*op);
    }
    mDispatchedReqs.erase(it);
    return op;
}

int
ChunkServer::TimeSinceLastHeartbeat() const
{
    return (TimeNow() - mLastHeartbeatSent);
}

bool
ChunkServer::ReplayValidate(MetaRequest& r) const
{
    if (! r.replayFlag || ! r.logseq.IsValid() || ! mReplayFlag) {
        panic("chunk server: invalid replay attempt");
        r.status = -EFAULT;
        submit_request(&r);
        return false;
    }
    return true;
}

void
ChunkServer::Handle(MetaChunkLogCompletion& req)
{
    if (mReplayFlag && ! ReplayValidate(req)) {
        return;
    }
    MetaChunkRequest* op;
    if (req.replayFlag) {
        op = FindMatchingRequest(req.doneLogSeq);
    } else {
        if (req.doneOp) {
            if (! LogInFlightReqs::IsInList(
                    mLogCompletionInFlightReqs, *req.doneOp)) {
                panic("chunk server: no matching log in flight op");
                op = 0;
            } else {
                op = req.doneOp;
                LogInFlightReqs::Remove(mLogCompletionInFlightReqs, *op);
                if (0 != req.status && ! mDown) {
                    if (mNetConnection) {
                        panic("chunk server: invalid log completion op status");
                        ScheduleDown(req.statusMsg.c_str());
                    }
                    // Add to last in flight, as bye has not arrived yet.
                    AppendInFlightChunks(mLastChunksInFlight, *op);
                }
            }
            RemoveInFlight(*req.doneOp);
        } else {
            panic("chunk server: invalid log in flight op");
            op = 0;
        }
    }
    if (req.staleChunkIdFlag) {
        if (req.chunkId < 0 || (req.doneOp && req.doneOp->chunkVersion < 0) ||
                ! mStaleChunkIdsInFlight.Erase(req.chunkId)) {
            if (mReplayFlag && 0 < req.chunkId) {
                if (op && ! op->hadPendingChunkOpFlag) {
                    req.status    = -EFAULT;
                    req.statusMsg = "had no pending chunk op in flight";
                }
                KFS_LOG_STREAM(0 == req.status ?
                        MsgLogger::kLogLevelDEBUG :
                        MsgLogger::kLogLevelERROR) << GetServerLocation() <<
                    " stale id"
                    " chunk: "  << req.chunkId <<
                    " status: " << req.status <<
                    " "         << req.statusMsg <<
                    " "         << MetaRequest::ShowReq(op) <<
                    " "         << req.Show() <<
                KFS_LOG_EOM;
            } else {
                panic(
                    "chunk server: invalid log in flight op stale chunk flag");
            }
        }
    }
    if (req.flushStaleQueueFlag) {
        mHelloPendingStaleChunks.clear();
    }
    if (op) {
        if (! req.doneOp) {
            if (! req.replayFlag || ! op->replayFlag) {
                panic("chunk server: invalid chunk op replay");
            }
            req.doneOp = op;
        } else if (op != req.doneOp) {
            req.status = -EFAULT;
            panic("chunk server: invalid chunk op completion");
        }
        if (mDown) {
            if (0 == req.status) {
                req.status = -ENOENT;
            }
        } else if (0 <= op->chunkVersion) {
            if (req.doneTimedOutFlag) {
                if (req.chunkId < 0 || req.chunkVersion < 0) {
                    panic(
                        "chunk server: invalid timed out chunk completion op");
                } else {
                    bool          insertedFlag = false;
                    TimeoutEntry* entry        = mDoneTimedoutChunks.Insert(
                        req.chunkId,  TimeoutEntry(TimeNow()), insertedFlag);
                    DoneTimedoutList::Insert(*entry,
                        DoneTimedoutList::GetPrev(mDoneTimedoutList));
                }
            } else {
                RemoveInFlightChunks(mDoneTimedoutChunks, *op);
            }
        }
        if (0 != req.status && 0 <= op->status) {
            op->status    = req.status;
            op->statusMsg = req.statusMsg;
        }
    } else {
        req.status = -ENOENT;
    }
}

void
ChunkServer::Replay(MetaChunkLogInFlight& req)
{
    if (! ReplayValidate(req)) {
        return;
    }
    if (! req.server) {
        const_cast<ChunkServerPtr&>(req.server) = GetSelfPtr();
    }
    MarkSubmitted(req);
    Enqueue(req, 365 * 24 * 60 * 60);
}

void
ChunkServer::Enqueue(MetaChunkLogInFlight& r)
{
    MetaChunkRequest* const req = r.request;
    r.request = 0;
    if (r.replayFlag || r.submitCount <= 0 ||
            (! r.logseq.IsValid() && 0 <= r.status) ||
            ! req || 0 != req->submitCount ||
                (MetaChunkRequest::kNullIterator != req->inFlightIt ?
                    req != req->inFlightIt->second : 0 <= req->chunkId)) {
        panic("chunk server: invalid submit attempt");
        r.status = -EFAULT;
    }
    if (mLogInFlightCount <= 0) {
        panic("chunk server: invalid log in flight count");
    } else {
        mLogInFlightCount--;
    }
    if (mDown) {
        if (req) {
            req->status = mDown ? -EIO : r.status;
            RemoveInFlight(*req);
            req->resume();
        }
        return;
    }
    if (0 == r.status) {
        req->logCompletionSeq = r.logseq;
    } else {
        if (req) {
            req->logCompletionSeq = MetaVrLogSeq();
            req->status           = r.status;
        }
        // In the case of failure must already be scheduled down.
        if (mNetConnection) {
            panic("chunk server: invalid log in flight op status");
            ScheduleDown(r.statusMsg.c_str());
        }
        if (! req) {
            return;
        }
    }
    const bool kLoggedFlag       = true;
    const bool kStaleChunkIdFlag = false;
    Enqueue(*req, r.maxWaitMillisec, kStaleChunkIdFlag, kLoggedFlag);
}

///
/// Queue an RPC request
///
void
ChunkServer::Enqueue(MetaChunkRequest& req,
    int timeout, bool staleChunkIdFlag, bool loggedFlag, bool removeReplicaFlag,
    chunkId_t addChunkIdInFlight)
{
    if (this != &*req.server || ! mHelloDone) {
        panic(mHelloDone ?
            "ChunkServer::Enqueue: invalid request" :
            "ChunkServer::Enqueue: invalid enqueue attempt"
        );
        req.status = -EFAULT;
        req.resume();
        return;
    }
    const bool restoreFlag = !! gLayoutManager.RestoreGetChunkServer();
    if (restoreFlag &&
            (! req.replayFlag ||
                this != &*gLayoutManager.RestoreGetChunkServer() || mDown)) {
        panic("chunk server: invalid restore attempt");
        return;
    }
    if (mHelloProcessFlag && MetaChunkLogInFlight::IsToBeLogged(req)) {
        AppendInFlightChunks(mHelloChunkIds, req);
    }
    if (staleChunkIdFlag && 0 <= req.chunkId && 0 <= req.chunkVersion) {
        if (req.replayFlag) {
            panic("chunk server: stale chunk id flag in replay");
            req.status = -EFAULT;
            return;
        }
        req.staleChunkIdFlag = mStaleChunkIdsInFlight.Insert(req.chunkId);
    }
    if (mReplayFlag && ! req.replayFlag) {
        MarkSubmitted(req);
        req.replayFlag = true;
        req.status     = -EIO;
        req.resume();
        return;
    }
    if (mDown) {
        req.status = -EIO;
        req.resume();
        return;
    }
    req.suspended = true;
    req.shortRpcFormatFlag = mShortRpcFormatFlag;
    if (! loggedFlag) {
        const chunkId_t chunkIdInFlight = 0 <= addChunkIdInFlight ?
            addChunkIdInFlight : req.chunkId;
        if (0 <= chunkIdInFlight) {
            req.inFlightIt = sChunkOpsInFlight.insert(
                make_pair(chunkIdInFlight, &req));
        }
        if (! req.replayFlag) {
            mLogInFlightCount++;
            if (MetaChunkLogInFlight::Log(req, timeout, removeReplicaFlag)) {
                return;
            }
            mLogInFlightCount--;
        }
    }
    if (! restoreFlag && 0 <= req.chunkVersion &&
            (req.logseq.IsValid() || req.logCompletionSeq.IsValid())) {
        if (RemoveInFlightChunks(mDoneTimedoutChunks, req)) {
            req.hadPendingChunkOpFlag = true;
        }
        if (RemoveInFlightChunks(mHelloChunkIds, req)) {
            req.hadPendingChunkOpFlag = true;
        }
    }
    if (0 == req.submitCount) {
        req.submitTime = microseconds();
    }
    ReqsTimeoutQueue::iterator const it = mReqsTimeoutQueue.insert(make_pair(
        TimeNow() + (timeout < 0 ? sRequestTimeout : timeout),
        DispatchedReqsIterator()
    ));
    if (! req.replayFlag) {
        req.opSeqno = NextSeq();
    }
    pair<DispatchedReqs::iterator, bool> const res =
        mDispatchedReqs.insert(make_pair(
            req.replayFlag ? req.logseq : CseqToVrLogSeq(req.opSeqno),
            make_pair(it, &req)
        ));
    if (res.second) {
        GetDispatchedReqsIterator(it->second) = res.first;
    } else {
        panic("chunk server: duplicate op sequence number");
        mReqsTimeoutQueue.erase(it);
        RemoveInFlight(req);
        req.status = -EFAULT;
        req.resume();
    }
    const bool sendFlag = mNetConnection && mNetConnection->IsGood();
    KFS_LOG_STREAM_DEBUG << GetServerLocation() <<
        " send: "   << sendFlag <<
        " +seq: "   << req.opSeqno <<
        " log: "    << req.logCompletionSeq <<
        " status: " << req.status <<
        " "         << req.Show() <<
    KFS_LOG_EOM;
    if (sendFlag) {
        if (req.status < 0 && ! mReplayFlag) {
            if (&req == FindMatchingRequest(req.opSeqno)) {
                req.resume();
            } else {
                panic("chunk server: invalid dispatch queue");
            }
        } else {
            EnqueueSelf(req);
        }
    }
}

void
ChunkServer::EnqueueSelf(MetaChunkRequest& req)
{
    if (mReAuthSentFlag || mAuthenticateOp) {
        if (mAuthPendingSeq < 0) {
            mAuthPendingSeq = req.opSeqno;
        }
        return;
    }
    IOBuffer& buf = mNetConnection->GetOutBuffer();
    if (buf.IsEmpty()) {
        mLastSentTime = TimeNow();
    }
    ChunkServerRequest(req, mOstream.Set(buf), buf);
    mOstream.Reset();
    if (mRecursionCount <= 0) {
        mNetConnection->StartFlush();
    }
}

int
ChunkServer::AllocateChunk(MetaAllocate& alc, int64_t leaseId, kfsSTier_t tier)
{
    if (0 < alc.numReplicas) {
        NewChunkInTier(tier);
    }
    MetaChunkAllocate& req = *(new MetaChunkAllocate(
        &alc, GetSelfPtr(), leaseId, tier, alc.maxSTier
    ));
    size_t sz;
    if (0 <= leaseId && 0 < alc.validForTime && 1 < (sz = alc.servers.size())) {
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
            const kfsUid_t authUid = alc.servers[i]->GetAuthUid();
            if (0 < i) {
                // Encrypt the session keys that will be passed via synchronous
                // replication chain.
                keyKeyId = keyId;
                keyKey   = key;
            }
            if (! alc.servers[i + 1]->GetCryptoKey(keyId, key)) {
                req.status    = -EFAULT;
                req.statusMsg = "no valid crypto key";
                req.resume();
                return -EFAULT;
            }
            DelegationToken::WriteTokenAndSessionKey(
                srvAccessOs,
                authUid,
                tokenSeq,
                keyId,
                alc.issuedTime,
                kDelegationFlags,
                alc.validForTime,
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
                alc.chunkId,
                authUid,
                tokenSeq,
                keyId,
                alc.issuedTime,
                ChunkAccessToken::kAllowWriteFlag |
                    DelegationToken::kChunkServerFlag |
                    (alc.clientCSAllowClearTextFlag ?
                        ChunkAccessToken::kAllowClearTextFlag : 0),
                LEASE_INTERVAL_SECS * 2,
                key.GetPtr(),
                key.GetSize()
            );
            chunkAccessOs << "\n";
            tokenSeq++;
        }
        req.chunkServerAccessStr = srvAccessOs.str();
        req.chunkAccessStr       = chunkAccessOs.str();
    }
    Enqueue(
        req,
        alc.initialChunkVersion >= 0 ? sChunkReallocTimeout : sChunkAllocTimeout
    );
    return 0;
}

int
ChunkServer::DeleteChunkVers(chunkId_t chunkId, seq_t chunkVersion,
    bool staleChunkIdFlag, bool forceDeleteFlag)
{
    if (0 <= chunkVersion) {
        mChunksToEvacuate.Erase(chunkId);
    }
    Enqueue(*(new MetaChunkDelete(
        GetSelfPtr(), chunkId, chunkVersion,
            staleChunkIdFlag || forceDeleteFlag)),
        -1, staleChunkIdFlag);
    return 0;
}

int
ChunkServer::GetChunkSize(fid_t fid, chunkId_t chunkId, seq_t chunkVersion,
    bool retryFlag)
{
    Enqueue(*(new MetaChunkSize(GetSelfPtr(),
        fid, chunkId, chunkVersion, retryFlag)));
    return 0;
}

int
ChunkServer::BeginMakeChunkStable(fid_t fid, chunkId_t chunkId, seq_t chunkVersion)
{
    Enqueue(*(new MetaBeginMakeChunkStable(
        GetSelfPtr(),
        mLocation, fid, chunkId, chunkVersion
    )), sMakeStableTimeout);
    return 0;
}

int
ChunkServer::MakeChunkStable(fid_t fid, chunkId_t chunkId, seq_t chunkVersion,
    chunkOff_t chunkSize, bool hasChunkChecksum, uint32_t chunkChecksum,
    bool addPending)
{
    Enqueue(*(new MetaChunkMakeStable(
        GetSelfPtr(),
        fid, chunkId, chunkVersion,
        chunkSize, hasChunkChecksum, chunkChecksum, addPending
    )), sMakeStableTimeout);
    return 0;
}

int
ChunkServer::ReplicateChunk(fid_t fid, chunkId_t chunkId,
    const ChunkServerPtr& dataServer, const ChunkRecoveryInfo& recoveryInfo,
    kfsSTier_t minSTier, kfsSTier_t maxSTier,
    MetaChunkReplicate::FileRecoveryInFlightCount::iterator it,
    bool removeReplicaFlag)
{
    mNumChunkWriteReplications++;
    MetaChunkReplicate& req = *(new MetaChunkReplicate(
        GetSelfPtr(), fid, chunkId,
        dataServer->GetServerLocation(), dataServer, minSTier, maxSTier, it));
    if (! dataServer) {
        panic("chunk server: invalid null replication source");
        req.status = -EINVAL;
        req.resume();
        return -EINVAL;
    }
    if (recoveryInfo.HasRecovery() && req.server == dataServer) {
        req.chunkVersion       = recoveryInfo.version;
        req.chunkOffset        = recoveryInfo.offset;
        req.striperType        = recoveryInfo.striperType;
        req.numStripes         = recoveryInfo.numStripes;
        req.numRecoveryStripes = recoveryInfo.numRecoveryStripes;
        req.stripeSize         = recoveryInfo.stripeSize;
        req.fileSize           = recoveryInfo.fileSize;
        req.dataServer.reset();
        req.srcLocation.hostname.clear();
        req.srcLocation.port   = sMetaClientPort;
        req.longRpcFormatFlag  = false;
        req.pendingAddFlag     = false; // Version change to add mapping.
    } else {
        req.longRpcFormatFlag = ! dataServer->IsShortRpcFormat();
        req.pendingAddFlag    = true; // Add mapping in replay.
    }
    if (gLayoutManager.IsClientCSAuthRequired()) {
        req.issuedTime                 = TimeNow();
        req.validForTime               = (uint32_t)max(0,
            gLayoutManager.GetCSAccessValidForTime());
        req.clientCSAllowClearTextFlag =
            gLayoutManager.IsClientCSAllowClearText();
        if (mAuthUid == kKfsUserNone) {
            req.status    = -EFAULT;
            req.statusMsg = "destination server has invalid id";
            req.resume();
            return -EFAULT;
        }
        req.authUid  = mAuthUid;
        req.tokenSeq =
            (DelegationToken::TokenSeq)gLayoutManager.GetRandom().Rand();
        if (req.dataServer) {
            if (! req.dataServer->GetCryptoKey(req.keyId, req.key)) {
                req.status    = -EFAULT;
                req.statusMsg = "source has no valid crypto key";
                req.resume();
                return -EFAULT;
            }
        } else {
            const CryptoKeys& keys = gNetDispatch.GetCryptoKeys();
            if (! keys.GetCurrentKey(
                    req.keyId, req.key, req.validForTime)) {
                req.status    = -EFAULT;
                req.statusMsg = "has no current valid crypto key";
                req.resume();
                return -EFAULT;
            }
            if (mRecoveryMetaAccess.empty() || mRecoveryMetaAccessEndTime <
                    req.issuedTime + max(5 * 60, sReplicationTimeout * 2)) {
                ostringstream& os = GetTmpOStringStream();
                DelegationToken::WriteTokenAndSessionKey(
                    os,
                    req.authUid,
                    req.tokenSeq,
                    req.keyId,
                    req.issuedTime,
                    DelegationToken::kChunkServerFlag,
                    req.validForTime,
                    req.key.GetPtr(),
                    req.key.GetSize()
                );
                mRecoveryMetaAccessEndTime = req.issuedTime + req.validForTime;
                mRecoveryMetaAccess        = os.str();
            }
            req.metaServerAccess = mRecoveryMetaAccess;
        }
    } else {
        req.validForTime = 0;
    }
    NewChunkInTier(minSTier);
    const bool kStaleChunkIdFlag = false;
    const bool kLoggedFlag       = false;
    Enqueue(req, sReplicationTimeout,
        kStaleChunkIdFlag, kLoggedFlag, removeReplicaFlag);
    return 0;
}

void
ChunkServer::NotifyStaleChunks(
    ChunkServer::InFlightChunks& staleChunkIds,
    bool                         evacuatedFlag,
    bool                         clearStaleChunksFlag,
    MetaChunkAvailable*          ca,
    MetaHello*                   hello)
{
    MetaChunkStaleNotify& req = *(new MetaChunkStaleNotify(
        GetSelfPtr(),
        evacuatedFlag,
        mStaleChunksHexFormatFlag,
        (ca && ! ca->replayFlag && ca->logseq.IsValid()) ? ca : 0
    ));
    if (clearStaleChunksFlag) {
        req.staleChunkIds.Swap(staleChunkIds);
    } else {
        req.staleChunkIds = staleChunkIds;
    }
    if (! mChunksToEvacuate.IsEmpty()) {
        InFlightChunks::ConstIterator it(req.staleChunkIds);
        const chunkId_t*              id;
        while ((id = it.Next())) {
            mChunksToEvacuate.Erase(*id);
        }
    }
    if (hello) {
        req.flushStaleQueueFlag = true;
        if (req.staleChunkIds.IsEmpty()) {
            // Force to create chunk log in flight by setting chunk id to 0.
            // 0 chunk id isn't used, but considered valid for the purpose of
            // logging, and the stale notify requests builder does not output
            // chunk id filed, only stale chunk ids list.
            req.chunkId = 0;
        }
        mHelloPendingStaleChunks.swap(hello->pendingStaleChunks);
    }
    Enqueue(req);
}

void
ChunkServer::NotifyStaleChunkSelf(chunkId_t staleChunkId, bool evacuatedFlag)
{
    MetaChunkStaleNotify& req = *(new MetaChunkStaleNotify(
        GetSelfPtr(), evacuatedFlag,
        mStaleChunksHexFormatFlag, 0));
    req.staleChunkIds.Insert(staleChunkId);
    mChunksToEvacuate.Erase(staleChunkId);
    const int  kTimeout           = -1;
    const bool kStaleChunkIdFlag  = false;
    const bool kLoggedFlag        = false;
    const bool kRemoveReplicaFlag = false;
    Enqueue(req, kTimeout, kStaleChunkIdFlag, kLoggedFlag, kRemoveReplicaFlag,
        staleChunkId);
}

void
ChunkServer::NotifyChunkVersChange(fid_t fid, chunkId_t chunkId, seq_t chunkVers,
    seq_t fromVersion, bool makeStableFlag, bool pendingAddFlag,
    MetaChunkReplicate* replicate, bool verifyStableFlag)
{
    Enqueue(*(new MetaChunkVersChange(
        GetSelfPtr(), fid, chunkId, chunkVers,
        fromVersion, makeStableFlag, pendingAddFlag, replicate,
        verifyStableFlag)),
        sMakeStableTimeout);
}

void
ChunkServer::SetRetiring(int64_t startTime, int64_t downTime)
{
    mIsRetiring        = downTime <= 0;
    mHibernateDownTime = mIsRetiring ? int64_t(-1) : downTime;
    mRetireStartTime   = (time_t)startTime;
    if (mIsRetiring) {
        mChunksToEvacuate.Clear();
    }
    KFS_LOG_STREAM(mReplayFlag ?
            MsgLogger::kLogLevelDEBUG :
            MsgLogger::kLogLevelINFO) <<
        GetServerLocation() <<
        " initiated " << (mIsRetiring ? "retire" : "hibernation") <<
        " chunks: " << GetChunkCount() <<
        " time: "
        " start: "  << mRetireStartTime <<
        " down: "   << mHibernateDownTime <<
    KFS_LOG_EOM;
}

void
ChunkServer::Retire()
{
    mRetiredFlag = true;
    Enqueue(*(new MetaChunkRetire(GetSelfPtr())));
}

void
ChunkServer::SetProperties(const Properties& props)
{
    Enqueue(*(new MetaChunkSetProperties(GetSelfPtr(), props)));
}

void
ChunkServer::Restart(bool justExitFlag)
{
    mRestartQueuedFlag = true;
    if (justExitFlag) {
        Enqueue(*(new MetaChunkRetire(GetSelfPtr())));
        return;
    }
    Enqueue(*(new MetaChunkServerRestart(GetSelfPtr())));
}

int
ChunkServer::Heartbeat()
{
    if (! mHelloDone || mDown || ! mNetConnection) {
        return -1;
    }
    const time_t now           = TimeNow();
    const int    timeSinceSent = (int)(now - mLastHeartbeatSent);
    if (mHeartbeatSent) {
        if (sHeartbeatTimeout <= timeSinceSent) {
            ostringstream& os = GetTmpOStringStream();
            os << "heartbeat timed out, sent: " <<
                timeSinceSent << " sec. ago";
            const string str = os.str();
            Error(str.c_str());
            return -1;
        }
        // If a request is outstanding, don't send one more
        if (! mHeartbeatSkipped &&
                mLastHeartbeatSent +
                    max(sHeartbeatInterval, sHeartbeatSkippedInterval) < now) {
            mHeartbeatSkipped = true;
            KFS_LOG_STREAM_INFO << GetServerLocation() <<
                " skipping heartbeat send,"
                " last sent " << timeSinceSent << " sec. ago" <<
            KFS_LOG_EOM;
        }
    } else if (sHeartbeatInterval <= timeSinceSent ||
            mLastSentTime + sMinInactivityInterval <= now) {
        KFS_LOG_STREAM_DEBUG << GetServerLocation() <<
            " sending heartbeat,"
            " last sent "    << timeSinceSent <<
            " / "            << (now - mLastSentTime) <<
            " ctrs update: " << (now - mLastCountersUpdateTime) <<
            " sec. ago" <<
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
            KFS_LOG_STREAM_INFO << GetServerLocation() <<
                " requesting chunk server re-authentication:"
                " session expires in: " <<
                    (mSessionExpirationTime - now) << " sec." <<
                " auth ctx update count: " << mAuthCtxUpdateCount <<
                " vs: " << authCtx.GetUpdateCount() <<
            KFS_LOG_EOM;
        }
        const int ctrsUpdateInterval = sHeartbeatInterval * 4 / 5;
        Enqueue(*(new MetaChunkHeartbeat(
                mSelfPtr,
                mIsRetiring ? int64_t(1) : (int64_t)mChunksToEvacuate.Size(),
                reAuthenticateFlag,
                sMaxPendingOpsCount,
                min(sHeartbeatTimeout, 2 * sMinInactivityInterval),
                sMinInactivityInterval < ctrsUpdateInterval &&
                    now < mLastCountersUpdateTime + ctrsUpdateInterval
            )),
            2 * sHeartbeatTimeout
        );
        mReAuthSentFlag = reAuthenticateFlag;
    }
    const time_t nextHbTime = mLastHeartbeatSent + sHeartbeatInterval;
    return max(time_t(1), (mHeartbeatSent ? nextHbTime :
        min(nextHbTime, mLastSentTime + sMinInactivityInterval)) - now);
}

int
ChunkServer::TimeoutOps()
{
    if (! mHelloDone || mDown || ! mNetConnection || mReplayFlag) {
        return -1;
    }
    time_t const                     now = TimeNow();
    ReqsTimeoutQueue::iterator const end =
        mReqsTimeoutQueue.lower_bound(now);
    for (ReqsTimeoutQueue::iterator it = mReqsTimeoutQueue.begin();
            it != end;
            ) {
        DispatchedReqs::iterator const dri =
            GetDispatchedReqsIterator(it->second);
        MetaChunkRequest* const        op  = dri->second.second;
        if (op->replayFlag || dri->second.first != it) {
            panic("chunk server: invalid timeout queue entry");
        }
        if (op->logCompletionSeq.IsValid()) {
            dri->second.first = ReqsTimeoutQueue::iterator();
            LogInFlightReqs::PushBack(mLogCompletionInFlightReqs, *op);
        } else {
            RemoveInFlight(*op);
        }
        mDispatchedReqs.erase(dri);
        mTmpReqQueue.push_back(op);
        KFS_LOG_STREAM_INFO << GetServerLocation() <<
            " request timed out"
            " expired: "   << (now - it->first) <<
            " in flight: " << mDispatchedReqs.size() <<
            " total: "     << sChunkOpsInFlight.size() <<
            " "            << op->Show() <<
        KFS_LOG_EOM;
        mReqsTimeoutQueue.erase(it++);
    }
    for (TmpReqQueue::iterator it = mTmpReqQueue.begin();
            it != mTmpReqQueue.end();
            ++it) {
        MetaChunkRequest* const op = *it;
        op->statusMsg    = "request timed out";
        op->status       = -EIO;
        op->timedOutFlag = true;
        op->resume();
    }
    mTmpReqQueue.clear();
    TimeoutEntry* entry   = &DoneTimedoutList::GetNext(mDoneTimedoutList);
    time_t const  expired = now - sTimedoutExpireTime;
    while (&mDoneTimedoutList != entry && entry->GetTime() < expired) {
        // Run chunk size RPC to check if chunk is present and
        // remove the entries in replay.
        TimeoutEntry& cur = *entry;
        if (cur.GetKey() < 0) {
            panic("chunk server: invalid timed out queue entry");
            entry = &DoneTimedoutList::GetNext(*entry);
            mDoneTimedoutChunks.Erase(cur.GetKey());
            continue;
        }
        MetaChunkSize& op = *(new MetaChunkSize(mSelfPtr));
        op.logAction      = MetaChunkSize::kLogNever;
        op.chunkId        = cur.GetKey();
        op.chunkVersion   = 0;
        op.checkChunkFlag = true;
        entry = &DoneTimedoutList::GetNext(cur);
        // Do not remove, just move it back, execution of size op should remove
        // it.
        cur.SetTime(now + 4 * 365 * 24 * 60 * 60);
        DoneTimedoutList::Insert(cur,
            DoneTimedoutList::GetPrev(mDoneTimedoutList));
        Enqueue(op);
    }
    return (mReqsTimeoutQueue.empty() ?
        -1 : int(mReqsTimeoutQueue.begin()->first - now + 1));
}

void
ChunkServer::FailDispatchedOps(const char* errMsg)
{
    if (mReplayFlag && ! LogInFlightReqs::IsEmpty(mLogCompletionInFlightReqs)) {
        panic("chunk server: replay: invalid non empty log in flight queue");
    }
    DispatchedReqs   reqs;
    ReqsTimeoutQueue reqTimeouts;
    mReqsTimeoutQueue.swap(reqTimeouts);
    mDispatchedReqs.swap(reqs);
    // Get all ops out of the in flight global queue first.
    // Remember all chunk ids that were in flight.
    for (DispatchedReqs::iterator it = reqs.begin();
            it != reqs.end();
            ++it) {
        MetaChunkRequest& op = *(it->second.second);
        KFS_LOG_STREAM_DEBUG << GetServerLocation() <<
            " failing op:"
            " -seq: " << op.opSeqno <<
            " "       << op.Show() <<
        KFS_LOG_EOM;
        if (! mHelloDone && op.logCompletionSeq.IsValid()) {
            panic("chunk server: op was queued prior to hello completion");
        }
        if (op.logCompletionSeq.IsValid() || op.replayFlag) {
            AppendInFlightChunks(mLastChunksInFlight, op);
        }
        RemoveInFlight(op);
    }
    // Add to the last in flight ops waiting for log completion, regardless
    // of whether or not those can change  (add) chunk mapping after the log
    // write completion, in order to match the replay case when the surrogate
    // ops (log in flight) are in the dispatched queue.
    LogInFlightReqs::Iterator it(mLogCompletionInFlightReqs);
    MetaChunkRequest*         op;
    while ((op = it.Next())) {
        if (! op->logCompletionSeq.IsValid()) {
            panic("chunk server: invalid log completion queue entry");
        }
        AppendInFlightChunks(mLastChunksInFlight, *op);
    }
    const chunkId_t* id;
    mStaleChunkIdsInFlight.First();
    while ((id = mStaleChunkIdsInFlight.Next())) {
        mLastChunksInFlight.Insert(*id);
    }
    const TimeoutEntry* entry = &mDoneTimedoutList;
    while (&mDoneTimedoutList != (entry = &DoneTimedoutList::GetNext(*entry))) {
        mLastChunksInFlight.Insert(entry->GetKey());
    }
    mDoneTimedoutChunks.Clear();
    assert(! DoneTimedoutList::IsInList(mDoneTimedoutList));
    // Fail in the same order as these were queued.
    for (DispatchedReqs::iterator it = reqs.begin();
            it != reqs.end();
            ++it) {
        MetaChunkRequest* const op = it->second.second;
        op->statusMsg        = errMsg ? errMsg : "chunk server disconnect";
        op->status           = -EIO;
        // Do not create (log) completion entry.
        op->logCompletionSeq = MetaVrLogSeq();
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
    const double  utilisation  = GetSpaceUtilization(useTotalFsSpaceFlag);
    const bool    isOverloaded = utilisation >
        gLayoutManager.GetMaxSpaceUtilizationThreshold();
    const time_t  now          = TimeNow();
    const int64_t freeSpace    = max(int64_t(0), mTotalSpace - mUsedSpace);
    os << "s=" << mLocation.hostname << ", p=" << mLocation.port
        << ", rack=" << mRackId
        << ", used=" << mUsedSpace
        << ", free=" << freeSpace
        << ", total=" << mTotalFsSpace
        << ", util=" << setprecision(2) << fixed << utilisation * 1e2
        << ", nblocks=" << mNumChunks
        << ", lastheard=" << (mReplayFlag ? time_t(-1) : now - mLastHeard)
        << ", ncorrupt=" << mNumCorruptChunks
        << ", nchunksToMove=" << mChunksToMove.Size()
        << ", numDrives=" << mNumDrives
        << ", numWritableDrives=" <<
            ((IsHibernatingOrRetiring() || isOverloaded) ?
                0 : mNumWritableDrives)
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
        << ", replay=" << (mReplayFlag ? 1 : 0)
        << ", connected=" << (IsConnected() ? 1 : 0)
        << ", stopped=" << (IsStoppedServicing() ? 1 : 0)
        << ", chunks=" << GetChunkCount()
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
    if (mAuthenticateOp && mAuthenticateOp != op) {
        op->next = 0;
        mPendingResponseOps.PushBack(*op);
        return false;
    }
    if (! mNetConnection || ! mNetConnection->IsGood()) {
        return true;
    }
    KFS_LOG_STREAM_DEBUG << GetServerLocation() <<
        " -seq: "   << op->opSeqno <<
        " status: " << op->status <<
        " "         << op->statusMsg <<
        " "         << op->Show() <<
    KFS_LOG_EOM;
    IOBuffer& buf = mNetConnection->GetOutBuffer();
    if (buf.IsEmpty()) {
        mLastSentTime = TimeNow();
    }
    ChunkServerResponse(*op, mOstream.Set(buf), buf);
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
    // For now do not escape '/' to make file names more readable.
    return escapeString(buf, len, '%', " !*'();:@&=+$,?#[]");
}

void
ChunkServer::UpdateStorageTiersSelf(
    const char* buf,
    size_t      len,
    int         deviceCount,
    int         writableChunkCount,
    bool        hexFormatFlag)
{
    bool clearFlags[kKfsSTierCount];
    for (size_t i = 0; i < kKfsSTierCount; i++) {
        mStorageTiersInfoDelta[i] = mStorageTiersInfo[i];
        clearFlags[i]             = true;
    }
    if (buf) {
        kfsSTier_t tier               = kKfsSTierUndef;
        int32_t    deviceCount        = -1;
        int32_t    notStableOpenCount = -1;
        int32_t    chunkCount         = -1;
        int64_t    spaceAvailable     = -1;
        int64_t    totalSpace         = -1;
        const char*       p = buf;
        const char* const e = p + len;
        while (p < e && (hexFormatFlag ? (
                HexIntParser::Parse(p, e - p, tier) &&
                HexIntParser::Parse(p, e - p, deviceCount) &&
                HexIntParser::Parse(p, e - p, notStableOpenCount) &&
                HexIntParser::Parse(p, e - p, chunkCount) &&
                HexIntParser::Parse(p, e - p, spaceAvailable) &&
                HexIntParser::Parse(p, e - p, totalSpace)
                ) : (
                DecIntParser::Parse(p, e - p, tier) &&
                DecIntParser::Parse(p, e - p, deviceCount) &&
                DecIntParser::Parse(p, e - p, notStableOpenCount) &&
                DecIntParser::Parse(p, e - p, chunkCount) &&
                DecIntParser::Parse(p, e - p, spaceAvailable) &&
                DecIntParser::Parse(p, e - p, totalSpace)))) {
            if (tier == kKfsSTierUndef) {
                continue;
            }
            if (kKfsSTierMax < tier) {
                tier = kKfsSTierMax;
            } else if (! IsValidSTier(tier)) {
                tier = kKfsSTierMin;
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
        " +seq: "               << mAuthenticateOp->opSeqno <<
        " type: "               << mAuthenticateOp->sendAuthType <<
        " name: "               << mAuthenticateOp->authName <<
        " filter: "             <<
            reinterpret_cast<const void*>(mAuthenticateOp->filter) <<
        " session expires in: " <<
            (mAuthenticateOp->sessionExpirationTime - TimeNow()) <<
        " response length: "    << mAuthenticateOp->sendContentLen <<
        " msg: "                << mAuthenticateOp->statusMsg <<
    KFS_LOG_EOM;
    mPendingOpsCount++;
    HandleRequest(EVENT_CMD_DONE, mAuthenticateOp);
    return 0;
}

bool
ChunkServer::ParseCryptoKey(
    const Properties::String& keyId,
    const Properties::String& key,
    bool                      hexFormatFlag)
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
    if (! (hexFormatFlag ?
            HexIntParser::Parse(p, keyId.GetSize(), mCryptoKeyId) :
            DecIntParser::Parse(p, keyId.GetSize(), mCryptoKeyId))) {
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
            " chunk server authentication failure:"
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

template <typename TS, typename TC>
inline static TS&
CpInsertChunkId(TS& os, const char* pref, TC& cnt, chunkId_t id)
{
    return (os << (0 == (cnt++ & 0x1F) ? pref : "/") << id);
}

bool
ChunkServer::Checkpoint(ostream& ost)
{
    if ((! mLastChunksInFlight.IsEmpty() && mNetConnection) ||
            mDown ||
            (mReplayFlag &&
                ! LogInFlightReqs::IsEmpty(mLogCompletionInFlightReqs))) {
        panic("chunk server: checkpoint: invalid state");
        return false;
    }
    ReqOstream os(ost);
    os << "cs"
        "/loc/"         << GetServerLocation() <<
        "/idx/"         << GetIndex() <<
        "/chunks/"      << GetChunkCount() <<
        "/chksum/"      << GetChecksum() <<
        "/retire/"      << (mIsRetiring ? 1 : 0) <<
        "/retirestart/" << mRetireStartTime <<
        "/retiredown/"  << mHibernateDownTime <<
        "/retired/"     << (mRetiredFlag ? 1 : 0) <<
        "/replay/"      << (mReplayFlag ? 1 : 0) <<
        "/rack/"        << mRackId <<
        "/pnotify/"     << (mPendingHelloNotifyFlag ? 1 : 0)
    ;
    size_t              cnt   = 0;
    const TimeoutEntry* entry = &mDoneTimedoutList;
    while (&mDoneTimedoutList != (entry = &DoneTimedoutList::GetNext(*entry))) {
        CpInsertChunkId(os, "\ncst/", cnt, entry->GetKey());
    }
    if (cnt != mDoneTimedoutChunks.GetSize()) {
        panic("chunk server: checkpoint: invalid timed out chunks list");
    }
    cnt = 0;
    const chunkId_t* id;
    mStaleChunkIdsInFlight.First();
    while ((id = mStaleChunkIdsInFlight.Next())) {
        CpInsertChunkId(os, "\ncss/", cnt, *id);
    }
    cnt = 0;
    ChunkIdSet::ConstIterator it(mHelloChunkIds);
    while ((id = it.Next())) {
        CpInsertChunkId(os, "\ncsr/", cnt, *id);
    }
    cnt = 0;
    for (ChunkIdList::const_iterator it = mHelloPendingStaleChunks.begin();
            it != mHelloPendingStaleChunks.end();
            ++it) {
        CpInsertChunkId(os, "\ncsp/", cnt, *it);
    }
    os << "\n";
    os.flush();
    if (! ost) {
        return false;
    }
    cnt = 0;
    for (DispatchedReqs::const_iterator it = mDispatchedReqs.begin();
            it != mDispatchedReqs.end() && ost;
            ++it) {
        if (MetaChunkLogInFlight::Checkpoint(ost, *(it->second.second))) {
            cnt++;
        }
        if (! ost) {
            return false;
        }
    }
    LogInFlightReqs::Iterator itc(mLogCompletionInFlightReqs);
    const MetaChunkRequest*   op;
    while ((op = itc.Next())) {
        if (! op->logCompletionSeq.IsValid() || op->replayFlag) {
            panic("chunk server: "
                "invalid log in flight op sequence or replay flag");
            continue;
        }
        if (MetaChunkLogInFlight::Checkpoint(ost, *op)) {
            cnt++;
        }
        if (! ost) {
            return false;
        }
    }
    os <<
        "cse/" << mDoneTimedoutChunks.GetSize() <<
        "/"    << cnt <<
        "/"    << mStaleChunkIdsInFlight.Size() <<
        "/"    << mHelloChunkIds.Size() <<
        "/"    << mHelloPendingStaleChunks.size() <<
        "\n";
    return (!! ost);
}

bool
ChunkServer::Restore(int type, size_t idx, int64_t n)
{
    if ('t' == type) {
        if (n < 0) {
            return false;
        }
        bool          insertedFlag = false;
        TimeoutEntry* entry        = mDoneTimedoutChunks.Insert(
            n,  TimeoutEntry(TimeNow()), insertedFlag);
        if (! insertedFlag) {
            return false;
        }
        DoneTimedoutList::Insert(*entry,
            DoneTimedoutList::GetPrev(mDoneTimedoutList));
        return  true;
    }
    if ('s' == type) {
        return (0 <= n && mStaleChunkIdsInFlight.Insert(n));
    }
    if ('r' == type) {
        return (0 <= n && mHelloChunkIds.Insert(n));
    }
    if ('p' == type) {
        if (n < 0) {
            return false;
        }
        mHelloPendingStaleChunks.push_back(n);
        return true;
    }
    return ('e' == type && 0 <= n && (4 < idx ||
        (0 == idx ? mDoneTimedoutChunks.GetSize() :
        (1 == idx ? mDispatchedReqs.size() :
        (2 == idx ? mStaleChunkIdsInFlight.Size() :
        (3 == idx ? mHelloChunkIds.Size() :
            mHelloPendingStaleChunks.size())))) ==
        (size_t)n));
}

inline void
ChunkServer::GetInFlightChunks(const CSMap& csMap,
    ChunkServer::InFlightChunks& chunks, CIdChecksum& chunksChecksum,
    ChunkServer::InFlightChunks& chunksDelete, uint64_t generation)
{
    KFS_LOG_STREAM_DEBUG <<
        " server: "        << GetServerLocation() <<
        " index: "         << GetIndex() <<
        " chunks: "        << GetChunkCount() <<
        " in flight: "     << mLastChunksInFlight.Size() <<
        " hello: "         << mHelloChunkIds.Size() <<
        " pending stale: " << mHelloPendingStaleChunks.size() <<
    KFS_LOG_EOM;
    // Treat pending stale as last in flight, requesting chunk server to report
    // status of these chunks back.
    for (ChunkIdList::const_iterator it = mHelloPendingStaleChunks.begin();
            it != mHelloPendingStaleChunks.end();
            ++it) {
        mLastChunksInFlight.Insert(*it);
    }
    ChunkServerPtr const srv = GetSelfPtr();
    for (int i = 0; i < 2; i++) {
        InFlightChunks& cur = i == 0 ?
            mLastChunksInFlight : mHelloChunkIds;
        const chunkId_t* id;
        ChunkIdSet::ConstIterator it(cur);
        while ((id = it.Next())) {
            if (0 == i) {
                mHelloChunkIds.Erase(*id);
            }
            const chunkId_t           chunkId = *id;
            const CSMap::Entry* const entry   = csMap.Find(chunkId);
            if (entry && csMap.HasServer(srv, *entry)) {
                chunks.Insert(chunkId);
                chunksChecksum.Add(chunkId, entry->GetChunkVersion());
            } else {
                chunksDelete.Insert(chunkId);
            }
            ChunkIdList::iterator const it = lower_bound(
                mHelloPendingStaleChunks.begin(),
                mHelloPendingStaleChunks.end(), chunkId);
            if (mHelloPendingStaleChunks.end() != it) {
                *it = -1;
            }
        }
        cur.Clear();
    }
    mHibernatedGeneration = generation;
}

HibernatedChunkServer::HibernatedChunkServer(
    ChunkServer& server,
    const CSMap& csMap)
    : CSMapServerInfo(),
      mLocation(server.GetServerLocation()),
      mDeletedChunks(),
      mModifiedChunks(),
      mListsSize(0),
      mGeneration(++sGeneration),
      mModifiedChecksum(),
      mPendingHelloNotifyFlag(server.IsHelloNotifyPending()),
      mReplayFlag(server.IsReplay())
{
    server.GetInFlightChunks(csMap, mModifiedChunks, mModifiedChecksum,
        mDeletedChunks, mGeneration);
    const size_t size = mModifiedChunks.Size() + mDeletedChunks.Size();
    mListsSize = 1 + size;
    sValidCount++;
    sChunkListsSize += size;
    KFS_LOG_STREAM(server.IsReplay() ?
            MsgLogger::kLogLevelDEBUG :
            MsgLogger::kLogLevelINFO) <<
        "hibernated: "  << mLocation <<
        " index: "      << server.GetIndex() <<
        " chunks: "     << server.GetChunkCount() <<
        " modified: "   << mModifiedChunks.Size() <<
        " delete: "     << mDeletedChunks.Size() <<
        " hibernated total:"
        " valid: "      << sValidCount <<
        " chunks: "     << sChunkListsSize <<
    KFS_LOG_EOM;
}

HibernatedChunkServer::HibernatedChunkServer(
    const ServerLocation& loc,
    const CIdChecksum&    modChksum,
    bool                  pendingHelloNotifyFlag)
    : CSMapServerInfo(),
      mLocation(loc),
      mDeletedChunks(),
      mModifiedChunks(),
      mListsSize(0),
      mGeneration(++sGeneration),
      mModifiedChecksum(modChksum),
      mPendingHelloNotifyFlag(pendingHelloNotifyFlag),
      mReplayFlag(true)
{}

void
HibernatedChunkServer::RemoveHosted(chunkId_t chunkId, seq_t vers, int index)
{
    if (0 < mListsSize) {
        if (mModifiedChunks.Erase(chunkId)) {
            mModifiedChecksum.Remove(chunkId, vers);
        } else {
            mListsSize++;
            sChunkListsSize++;
            Prune();
        }
        if (0 < mListsSize) {
            mDeletedChunks.Insert(chunkId);
        }
    }
    CSMapServerInfo::RemoveHosted(chunkId, vers, index);
    KFS_LOG_STREAM_DEBUG <<
        "hibernated: "  << mLocation <<
        " index: "      << GetIndex() <<
        " / "           << index <<
        " remove: "     << chunkId <<
        " lists size: " << mListsSize <<
    KFS_LOG_EOM;
}

void
HibernatedChunkServer::SetVersion(
    chunkId_t chunkId, seq_t curVers, seq_t vers, int index)
{
    Modified(chunkId, curVers, vers);
    CSMapServerInfo::SetVersion(chunkId, curVers, vers, index);
}

void
HibernatedChunkServer::Modified(chunkId_t chunkId, seq_t curVers, seq_t vers)
{
    if (0 < mListsSize) {
        if (mModifiedChunks.Insert(chunkId)) {
            mListsSize++;
            sChunkListsSize++;
            Prune();
            if (0 < mListsSize) {
                mModifiedChecksum.Add(chunkId, vers);
            }
        } else if (curVers != vers) {
            mModifiedChecksum.Remove(chunkId, curVers);
            mModifiedChecksum.Add(chunkId, vers);
        }
    }
    KFS_LOG_STREAM_DEBUG <<
        "hibernated: "  << mLocation <<
        " index: "      << GetIndex() <<
        " modified: "   << chunkId <<
        " version: "    << curVers <<
        " => "          << vers <<
        " lists size: " << mListsSize <<
    KFS_LOG_EOM;
}

void
HibernatedChunkServer::UpdateLastInFlight(const CSMap& csMap, chunkId_t chunkId)
{
    if (mListsSize <= 0) {
        return;
    }
    const CSMap::Entry* const ce =
        csMap.HasHibernatedServer(GetIndex(), chunkId);
    if (ce) {
        Modified(chunkId, ce->GetChunkVersion(), ce->GetChunkVersion());
        return;
    }
    if (mModifiedChunks.Find(chunkId)) {
        panic("hibernated server: invalid modified chunk entry");
        return;
    }
    mListsSize++;
    sChunkListsSize++;
    Prune();
    if (0 < mListsSize) {
        mDeletedChunks.Insert(chunkId);
    }
}

bool
HibernatedChunkServer::HelloResumeReply(
    MetaHello&                             req,
    const CSMap&                           csMap,
    HibernatedChunkServer::DeletedChunks&  staleChunkIds,
    HibernatedChunkServer::ModifiedChunks& modifiedChunks)
{
    if (! modifiedChunks.IsEmpty() || ! staleChunkIds.IsEmpty()) {
        panic("hibernated server: resume reply:"
            "invalid invocation: non empty chunks list");
        modifiedChunks.Clear();
        staleChunkIds.Clear();
    }
    if (! req.server) {
        panic("hibernated server: invalid hello, null server");
        req.status = -EFAULT;
        return false;
    }
    if (req.status != 0 || req.resumeStep < 0) {
        return false;
    }
    if (! CanBeResumed()) {
        req.statusMsg = "no valid hibernated server resume state exists";
        req.status    = -EAGAIN;
        return true;
    }
    if (0 < req.resumeStep) {
        // Ensure that next step has correct counts. The counts should
        // correspond to the counts sent on the previous step.
        if (! req.replayFlag && mDeletedChunks.Size() < req.deletedCount) {
            req.statusMsg = "invalid resume response";
            req.status    = -EINVAL;
        } else {
            req.statusMsg.clear();
        }
        KFS_LOG_STREAM(req.replayFlag ? MsgLogger::kLogLevelDEBUG :
                req.status == 0 ? MsgLogger::kLogLevelINFO :
                    MsgLogger::kLogLevelERROR) <<
            "hibernated: "  << mLocation <<
            " index: "      << GetIndex() <<
            " server: "     << req.server->GetServerLocation() <<
            " status: "     << req.status <<
            " msg: "        << req.statusMsg <<
            " resume: "     << req.resumeStep <<
            " chunks: "     << req.chunkCount <<
            " => "          << GetChunkCount() <<
            " deleted: "    << req.deletedCount <<
            " => "          << mDeletedChunks.Size() <<
            " modified: "   << req.modifiedCount <<
            " => "          << mModifiedChunks.Size() <<
        KFS_LOG_EOM;
        if (req.status != 0) {
            return true;
        }
        staleChunkIds.Swap(mDeletedChunks);
        modifiedChunks.Swap(mModifiedChunks);
        return false;
    }
    if (GetChunkCount() < mModifiedChunks.Size()) {
        panic("hibernated server: "
            "resume reply: invalid modified chunks list size");
        req.statusMsg = "cannot be resumed due to internal error";
        req.status    = -EAGAIN;
        return false;
    }
    req.pendingNotifyFlag  = mPendingHelloNotifyFlag;
    req.deletedCount       = 0;
    req.modifiedCount      = 0;
    req.chunkCount         = GetChunkCount() - mModifiedChunks.Size();
    req.checksum           = GetChecksum();
    req.checksum.Remove(mModifiedChecksum);
    // Chunk server assumes responsibility for ensuring that list has no
    // duplicate entries.
    for (MetaHello::ChunkIdList::const_iterator
            it = req.missingChunks.begin();
            it != req.missingChunks.end();
            ++it) {
        const chunkId_t     chunkId = *it;
        const CSMap::Entry* ce;
        if (mModifiedChunks.Find(chunkId) ||
                ! (ce = csMap.HasHibernatedServer(GetIndex(), chunkId))) {
            continue;
        }
        if (req.chunkCount <= 0) {
            req.statusMsg = "invalid missing chunk list:"
                " possible duplicate entries";
            req.status    = -EINVAL;
            return false;
        }
        req.chunkCount--;
        req.checksum.Remove(chunkId, ce->GetChunkVersion());
    }
    KFS_LOG_STREAM_INFO <<
        "hibernated: " << mLocation <<
        " index: "     << GetIndex() <<
        " server: "    << req.server->GetServerLocation() <<
        " resume: "    << req.resumeStep <<
        " chunks: "    << GetChunkCount() <<
        " => "         << req.chunkCount <<
        " checksum: "  << GetChecksum() <<
        " mod: "       << mModifiedChecksum <<
        " => "         << req.checksum <<
        " deleted: "   << mDeletedChunks.Size() <<
        " modified: "  << mModifiedChunks.Size() <<
    KFS_LOG_EOM;
    if (mListsSize <= 1) {
        if (! mModifiedChunks.IsEmpty() || ! mDeletedChunks.IsEmpty()) {
            panic("hibernated server: invalid lists size");
        }
        return true;
    }
    req.responseBuf.Clear();
    IOBufferWriter writer(req.responseBuf);
    const chunkId_t* id;
    char tbuf[sizeof(*id) * 8 / 4 + sizeof(char) * 4];
    char* const bend = tbuf + sizeof(tbuf) / sizeof(tbuf[0]) - 1;
    *bend = ' ';
    DeletedChunks::ConstIterator itd(mDeletedChunks);
    while ((id = itd.Next())) {
        const char* const ptr = IntToHexString(*id, bend);
        writer.Write(ptr, bend - ptr + 1);
        req.deletedCount++;
    }
    writer.Write("\n", 1);
    ModifiedChunks::ConstIterator itm(mModifiedChunks);
    while ((id = itm.Next())) {
        const char* const ptr = IntToHexString(*id, bend);
        writer.Write(ptr, bend - ptr + 1);
        req.modifiedCount++;
    }
    writer.Close();
    return true;
}

ostream&
HibernatedChunkServer::DisplaySelf(ostream& os, CSMap& csMap) const
{
    const int idx      = GetIndex();
    size_t    count    = 0;
    size_t    modCount = 0;
    for (csMap.First(); os;) {
        const CSMap::Entry* p = csMap.Next();
        if (! p) {
            break;
        }
        const chunkId_t chunkId = p->GetChunkId();
        if (! csMap.HasHibernatedServer(idx, chunkId)) {
            continue;
        }
        const bool modFlag = mModifiedChunks.Find(chunkId);
        const bool delFlag = mDeletedChunks.Find(chunkId);
        if (modFlag && delFlag) {
            os << "INVALID modified or deleted list entry: " << chunkId << "\n";
        }
        os << chunkId << " " << p->GetChunkInfo()->chunkVersion <<
            (modFlag ? " M" : (delFlag ? " D" : " S")) <<
        "\n";
        count++;
        if (modFlag) {
            modCount++;
        }
    }
    if (modCount != mModifiedChunks.Size()) {
        os <<
            "INVALID modififed set"
            "["    << mModifiedChunks.Size() <<
            " != " << modCount <<
            "]:";
        ModifiedChunks::ConstIterator it(mModifiedChunks);
        const chunkId_t* id;
        while (os && (id = it.Next())) {
            os << ' ' << *id;
        }
        os << "\n";
    }
    if (! mDeletedChunks.IsEmpty()) {
        os << "delted[" << mDeletedChunks.Size() << "]:\n";
        DeletedChunks::ConstIterator it(mDeletedChunks);
        const chunkId_t* id;
        while (os && (id = it.Next())) {
            if (! csMap.HasHibernatedServer(idx, *id)) {
                os << *id << "\n";
            }
        }
    }
    os <<
        "count: "     << GetChunkCount() <<
        " / "         << count <<
        " checksum: " << GetChecksum() <<
        " modified: " << modCount <<
    "\n";
    return os;
}

bool
HibernatedChunkServer::Checkpoint(ostream& ost, const ServerLocation& loc,
    uint64_t startTime, uint64_t endTime, bool retiredFlag)
{
    ReqOstream os(ost);
    os << "hcs"
        "/loc/"       << loc <<
        "/idx/"       << GetIndex() <<
        "/chunks/"    << GetChunkCount() <<
        "/chksum/"    << GetChecksum() <<
        "/start/"     << startTime <<
        "/end/"       << endTime <<
        "/retired/"   << retiredFlag <<
        "/replay/"    << (mReplayFlag ? 1 : 0)  <<
        "/modchksum/" << mModifiedChecksum <<
        "/pnotify/"   << (mPendingHelloNotifyFlag ? 1 : 0)
    ;
    const char*      pref = "\nhcsd/";
    unsigned int     cnt = 0;
    const chunkId_t* id;
    for (DeletedChunks::ConstIterator it(mDeletedChunks); (id = it.Next()); ) {
        CpInsertChunkId(os, pref, cnt, *id);
    }
    cnt  = 0;
    pref = "\nhcsm/";
    for (ModifiedChunks::ConstIterator it(mModifiedChunks); (id = it.Next());) {
        CpInsertChunkId(os, pref, cnt, *id);
    }
    os <<
        "\n"
        "hcse/" << mDeletedChunks.Size() <<
        "/"     << mModifiedChunks.Size() <<
        "\n";
    return (!! ost);
}

bool
HibernatedChunkServer::Restore(int type, size_t idx, int64_t n)
{
    if ('d' == type) {
        if (n < 0) {
            return false;
        }
        mDeletedChunks.Insert(n);
        return true;
    }
    if ('m' == type) {
        if (n < 0) {
            return false;
        }
        return mModifiedChunks.Insert(n);
    }
    if ('e' != type) {
        return false;
    }
    if (0 == idx) {
        return (mDeletedChunks.Size() == (size_t)n);
    }
    if (1 == idx) {
        if (mModifiedChunks.Size() != (size_t)n) {
            return false;
        }
        if (0 != mListsSize) {
            panic("hibernated server: restore: invalid lists size");
        }
        const size_t size = mModifiedChunks.Size() + mDeletedChunks.Size();
        mListsSize = 1 + size;
        sValidCount++;
        sChunkListsSize += size;
        return true;
    }
    return (1 < idx);
}

void
HibernatedChunkServer::Prune()
{
    if (mListsSize <= 0 || ! gLayoutManager.IsPrimary()) {
        return;
    }
    const size_t size = sMaxChunkListsSize + sPruneInFlightCount;
    if (sChunkListsSize <= size ||
            (uint64_t)sValidCount * (mListsSize - 1) < size) {
        return;
    }
    const size_t pruneSize = mListsSize - 1;
    sPruneInFlightCount += pruneSize;
    submit_request(new MetaHibernatedPrune(mLocation, pruneSize));
}

/* static */ void
HibernatedChunkServer::Handle(
    HibernatedChunkServer* server, MetaHibernatedPrune& req)
{
    sPruneInFlightCount = sPruneInFlightCount < req.listSize ?
        size_t(0) : sPruneInFlightCount - req.listSize;
    if (server) {
        server->Clear();
    }
}

/* static */ void
HibernatedChunkServer::SetParameters(const Properties& props)
{
    sMaxChunkListsSize = props.getValue(
        "metaServer.maxHibernatedChunkListSize",
        sMaxChunkListsSize * 2
    ) / 2;
}

size_t   HibernatedChunkServer::sValidCount(0);
size_t   HibernatedChunkServer::sChunkListsSize(0);
size_t   HibernatedChunkServer::sPruneInFlightCount(0);
uint64_t HibernatedChunkServer::sGeneration(0);
size_t   HibernatedChunkServer::sMaxChunkListsSize(
    size_t(sizeof(void*) < 8 ? 8 : 48) << 20);

} // namespace KFS
