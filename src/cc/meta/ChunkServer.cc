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
#include "util.h"
#include "kfsio/Globals.h"
#include "qcdio/QCUtils.h"
#include "common/MsgLogger.h"
#include "common/kfserrno.h"

#include <openssl/rand.h>

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

int ChunkServer::sHeartbeatTimeout     = 60;
int ChunkServer::sHeartbeatInterval    = 20;
int ChunkServer::sHeartbeatLogInterval = 1000;
int ChunkServer::sChunkAllocTimeout    = 40;
int ChunkServer::sChunkReallocTimeout  = 75;
int ChunkServer::sMakeStableTimeout    = 330;
int ChunkServer::sReplicationTimeout   = 330;
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
int ChunkServer::sChunkServerCount = 0;
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
    RAND_pseudo_bytes(
        reinterpret_cast<unsigned char*>(&ret), int(sizeof(ret)));
    return ((ret < 0 ? -ret : ret) >> 1);
}

inline void
ChunkServerRequest(MetaChunkRequest& req, ostream& os, IOBuffer& buf)
{
    req.request(os, buf);
}

inline void
ChunkServer::UpdateChunkWritesPerDrive(
    int numChunkWrites, int numWritableDrives)
{
    const int deltaChunkWrites    = numChunkWrites - mNumChunkWrites;
    const int deltaWritableDrives = numWritableDrives - mNumWritableDrives;
    mNumChunkWrites    = numChunkWrites;
    mNumWritableDrives = numWritableDrives;
    gLayoutManager.UpdateChunkWritesPerDrive(*this,
        deltaChunkWrites, deltaWritableDrives);

}

ChunkServer::ChunkServer(const NetConnectionPtr& conn, const string& peerName)
    : KfsCallbackObj(),
      CSMapServerInfo(),
      mSeqNo(RandomSeqNo()),
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
      mPeerName(peerName)
{
    assert(mNetConnection);
    ChunkServersList::Init(*this);
    PendingHelloList::Init(*this);
    ChunkServersList::PushBack(sChunkServersPtr, *this);
    SET_HANDLER(this, &ChunkServer::HandleRequest);
    mNetConnection->SetInactivityTimeout(sHeartbeatInterval);
    mNetConnection->SetMaxReadAhead(kMaxReadAhead);
    sChunkServerCount++;
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
    ChunkServersList::Remove(sChunkServersPtr, *this);
    sChunkServerCount--;
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
        panic("invalid hello request byte counter", false);
        sHelloBytesCommitted = 0;
    } else {
        sHelloBytesCommitted -= req->contentLength;
    }
    if (sHelloBytesInFlight < req->bytesReceived) {
        panic("invalid hello received byte counter", false);
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
        bool gotMsgHdr;
        int  msgLen = 0;
        while ((gotMsgHdr = mHelloOp || IsMsgAvail(&iobuf, &msgLen))) {
            const int retval = HandleMsg(&iobuf, msgLen);
            if (retval < 0) {
                iobuf.Clear();
                Error(mHelloDone ?
                    "request or response parse error" :
                    "failed to parse hello message");
                break;
            }
            if (retval > 0) {
                break; // Need more data
            }
        }
        if (! mDown && ! gotMsgHdr && iobuf.BytesConsumable() >
                kMaxRequestResponseHeader) {
            iobuf.Clear();
            Error(mHelloDone ?
                "request or response header length"
                    " exceeds max allowed" :
                "hello message header length"
                    " exceeds max allowed");
        }
        break;
    }

    case EVENT_CMD_DONE: {
        assert(mHelloDone && data);
        MetaRequest* const op = reinterpret_cast<MetaRequest*>(data);
        if (! mDown) {
            SendResponse(op);
        }
        // nothing left to be done...get rid of it
        delete op;
        break;
    }

    case EVENT_NET_WROTE:
        if (! mHelloDone &&
                mNetConnection &&
                ! mNetConnection->IsWriteReady()) {
            Error("hello error "
                "cluster key or md5sum mismatch");
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

    case EVENT_NET_ERROR:
        Error("communication error");
        break;

    default:
        assert(!"Unknown event");
        break;
    }
    if (mRecursionCount <= 1 && ! mDown && mNetConnection) {
        mNetConnection->StartFlush();
    }
    if (mHelloDone) {
        if (mRecursionCount <= 1) {
            const int hbTimeout = Heartbeat();
            const int opTimeout = TimeoutOps();
            if (! mDown &&
                    mNetConnection &&
                    mNetConnection->IsGood()) {
                mNetConnection->SetInactivityTimeout(
                    NetManager::Timer::MinTimeout(
                        hbTimeout, opTimeout));
            }

        }
    } else if (code != EVENT_INACTIVITY_TIMEOUT) {
        mLastHeartbeatSent = TimeNow();
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
    gLayoutManager.UpdateSrvLoadAvg(*this, delta);
    UpdateChunkWritesPerDrive(0, 0);
    FailDispatchedOps();
    mSelfPtr.reset(); // Unref / delete self
    assert(sChunkDirsCount >= mChunkDirInfos.size());
    sChunkDirsCount -= min(sChunkDirsCount, mChunkDirInfos.size());
    mChunkDirInfos.clear();
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
    gLayoutManager.UpdateSrvLoadAvg(*this, delta, kCanBeCandidateFlag);
    mCanBeChunkMaster = flag;
    mLoadAvg = -delta;
    gLayoutManager.UpdateSrvLoadAvg(*this, mLoadAvg);
}

void
ChunkServer::Error(const char* errorMsg)
{
    const int socketErr = (mNetConnection && mNetConnection->IsGood()) ?
        mNetConnection->GetSocketError() : 0;
    KFS_LOG_STREAM_ERROR <<
        "chunk server " << GetServerLocation() <<
        "/" << (mNetConnection ? GetPeerName() :
            string("not connected")) <<
        " down" <<
        (mRestartQueuedFlag ? " restart" : "") <<
        " reason: " << (errorMsg ? errorMsg : "unspecified") <<
        " socket error: " << QCUtils::SysError(socketErr) <<
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
    gLayoutManager.UpdateSrvLoadAvg(*this, delta);
    UpdateChunkWritesPerDrive(0, 0);
    FailDispatchedOps();
    assert(sChunkDirsCount >= mChunkDirInfos.size());
    sChunkDirsCount -= min(sChunkDirsCount, mChunkDirInfos.size());
    mChunkDirInfos.clear();
    if (mHelloDone) {
        // force the server down thru the main loop to avoid races
        MetaBye* const mb = new MetaBye(0, shared_from_this());
        mb->clnt = this;
        submit_request(mb);
    }
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
    IOBuffer& iobuf, int len, int linesToShow /* = 64 */)
{
    istream& is       = mIStream.Set(iobuf, len);
    int      maxLines = linesToShow;
    string   line;
    while (--maxLines >= 0 && getline(is, line)) {
        string::iterator last = line.end();
        if (last != line.begin() && *--last == '\r') {
            line.erase(last);
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

    HexChunkInfoParser(const IOBuffer& buf)
        : mIt(buf),
          mCur(),
          mVal(0),
          mPrevSpaceFlag(true),
          mFieldIdx(0)
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
                    case 0: mCur.allocFileId  = mVal; break;
                    case 1: mCur.chunkId      = mVal; break;
                    case 2: mCur.chunkVersion = mVal;
                        mFieldIdx = 0;
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
    int                    mFieldIdx;

    static const unsigned char* const sC2HexTable;
};
const unsigned char* const HexChunkInfoParser::sC2HexTable = char2HexTable();

/// Case #1: Handle Hello message from a chunkserver that
/// just connected to us.
int
ChunkServer::HandleHelloMsg(IOBuffer* iobuf, int msgLen)
{
    assert(!mHelloDone);

    const bool hasHelloOpFlag = mHelloOp != 0;
    if (! hasHelloOpFlag) {
        MetaRequest * const op = GetOp(*iobuf, msgLen, "invalid hello");
        if (! op) {
            return -1;
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
        mHelloOp = static_cast<MetaHello*>(op);
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
                iobuf->Clear();
                mNetConnection->SetMaxReadAhead(0);
                mNetConnection->SetInactivityTimeout(
                    sRequestTimeout);
                mOstream.Set(mNetConnection->GetOutBuffer());
                mHelloOp->response(mOstream);
                mOstream.Reset();
                delete mHelloOp;
                mHelloOp = 0;
                // Do not declare error, hello reply still
                // pending.
                return 0;
            }
        }
        if (mHelloOp->status == 0 &&
                sMaxHelloBufferBytes < mHelloOp->contentLength) {
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
                " hello content length: " <<
                    mHelloOp->contentLength <<
                " exceeds max. allowed: " <<
                    sMaxHelloBufferBytes <<
            KFS_LOG_EOM;
            mHelloOp = 0;
            delete op;
            return -1;
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
            HexChunkInfoParser hexParser(*iobuf);
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
                    while (i-- > 0) {
                        if (! (is >> c.allocFileId
                                >> c.chunkId
                                >> c.chunkVersion)) {
                            break;
                        }
                        chunks.push_back(c);
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
    MetaRequest* const op = mHelloOp;
    mHelloOp = 0;
    submit_request(op);
    return 0;
}

///
/// Case #2: Handle an RPC from a chunkserver.
///
int
ChunkServer::HandleCmd(IOBuffer *iobuf, int msgLen)
{
    assert(mHelloDone);

    MetaRequest * const op = GetOp(*iobuf, msgLen, "invalid request");
    if (! op) {
        return -1;
    }
    // Message is ready to be pushed down.  So remove it.
    iobuf->Consume(msgLen);
    op->clnt = this;
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
    if (op.numWritableDrives >= 0) {
        UpdateChunkWritesPerDrive(
            mNumChunkWrites, op.numWritableDrives);
    }
    if (op.numDrives >= 0) {
        mNumDrives = op.numDrives;
        UpdateChunkWritesPerDrive(
            mNumChunkWrites, min(mNumWritableDrives, mNumDrives));
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
        UpdateChunkWritesPerDrive(
            max(0, prop.getValue("Num-writable-chunks", 0)),
                   prop.getValue("Num-wr-drives",       mNumDrives)
        );
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
        const int64_t srvLoad =
            prop.getValue(sSrvLoadPropName, int64_t(0));
        int64_t loadAvg;
        if (sSrvLoadSamplerSampleCount > 0) {
            if (mSrvLoadSampler.GetMaxSamples() !=
                    sSrvLoadSamplerSampleCount) {
                mSrvLoadSampler.SetMaxSamples(
                    sSrvLoadSamplerSampleCount, srvLoad,
                    now);
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
        mAllocSpace       = mUsedSpace + mNumChunkWrites * CHUNKSIZE;
        mHeartbeatSent    = false;
        mHeartbeatSkipped =
            mLastHeartbeatSent + sHeartbeatInterval < now;
        mHeartbeatProperties.swap(prop);
        if (mTotalFsSpace < mTotalSpace) {
            mTotalFsSpace = mTotalSpace;
        }
        const int64_t delta = loadAvg - mLoadAvg;
        mLoadAvg = loadAvg;
        gLayoutManager.UpdateSrvLoadAvg(*this, delta);
        if (sHeartbeatLogInterval > 0 &&
            mLastHeartBeatLoggedTime +
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
    prop.loadProperties(is, separator, false);
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
        panic("duplicate op sequence number", false);
    }
    if (r->op == META_CHUNK_REPLICATE) {
        KFS_LOG_STREAM_INFO << r->Show() << KFS_LOG_EOM;
    }
    EnqueueSelf(r);
}

void
ChunkServer::EnqueueSelf(MetaChunkRequest* r)
{
    IOBuffer& buf = mNetConnection->GetOutBuffer();
    ChunkServerRequest(*r, mOstream.Set(buf), buf);
    mOstream.Reset();
    if (mRecursionCount <= 0) {
        mNetConnection->StartFlush();
    }
}

int
ChunkServer::AllocateChunk(MetaAllocate *r, int64_t leaseId)
{
    mAllocSpace += CHUNKSIZE;
    UpdateChunkWritesPerDrive(mNumChunkWrites + 1, mNumWritableDrives);
    Enqueue(new MetaChunkAllocate(
            NextSeq(), r, shared_from_this(), leaseId),
        r->initialChunkVersion >= 0 ?
            sChunkReallocTimeout : sChunkAllocTimeout);
    return 0;
}

int
ChunkServer::DeleteChunk(chunkId_t chunkId)
{
    mAllocSpace = max((int64_t)0, mAllocSpace - (int64_t)CHUNKSIZE);
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
    const ChunkServerPtr& dataServer, const ChunkRecoveryInfo& recoveryInfo)
{
    MetaChunkReplicate* const r = new MetaChunkReplicate(
        NextSeq(), shared_from_this(), fid, chunkId,
        dataServer->GetServerLocation(), dataServer);
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
    mNumChunkWriteReplications++;
    UpdateChunkWritesPerDrive(mNumChunkWrites + 1, mNumWritableDrives);
    mAllocSpace += CHUNKSIZE;
    Enqueue(r, sReplicationTimeout);
    return 0;
}

void
ChunkServer::NotifyStaleChunks(ChunkIdQueue& staleChunkIds,
    bool evacuatedFlag, bool clearStaleChunksFlag)
{
    mAllocSpace = max(int64_t(0),
        mAllocSpace - (int64_t)(CHUNKSIZE * staleChunkIds.GetSize()));
    MetaChunkStaleNotify * const r = new MetaChunkStaleNotify(
        NextSeq(), shared_from_this(), evacuatedFlag,
        mStaleChunksHexFormatFlag);
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
    mAllocSpace = max((int64_t)0, mAllocSpace - (int64_t)CHUNKSIZE);
    MetaChunkStaleNotify * const r = new MetaChunkStaleNotify(
        NextSeq(), shared_from_this(), evacuatedFlag,
        mStaleChunksHexFormatFlag);
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
            ostringstream os;
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
        Enqueue(new MetaChunkHeartbeat(NextSeq(), shared_from_this(),
                IsRetiring() ? int64_t(1) :
                    (int64_t)mChunksToEvacuate.Size()),
            2 * sHeartbeatTimeout);
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
            panic("invalid timeout queue entry", false);
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
        << ", util=" << utilisation * 100.0
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
        << ", lostChunkDirs="
    ;
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

void
ChunkServer::SendResponse(MetaRequest* op)
{
    if (! mNetConnection) {
        return;
    }
    op->response(mOstream.Set(mNetConnection->GetOutBuffer()));
    mOstream.Reset();
    if (mRecursionCount <= 0) {
        mNetConnection->StartFlush();
    }
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

} // namespace KFS
