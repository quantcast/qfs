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
#include "kfsio/SslFilter.h"
#include "common/MsgLogger.h"
#include "common/Properties.h"
#include "common/kfserrno.h"
#include "qcdio/QCUtils.h"

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

class RemoteSyncSM::Auth
{
public:
    Auth()
        : mSslCtxPtr(),
          mParams(),
          mEnabledFlag(false)
        {}
    ~Auth()
        {}
    bool SetParameters(
        const char*       inParamsPrefixPtr,
        const Properties& inParameters)
    {
        Properties::String theParamName;
        if (inParamsPrefixPtr) {
            theParamName.Append(inParamsPrefixPtr);
        }
        const size_t thePrefLen = theParamName.GetSize();
        Properties theParams(mParams);
        inParameters.copyWithPrefix(
            theParamName.GetPtr(), theParamName.GetSize(), theParams);
        const size_t theCurLen = theParamName.Append("psk.").GetSize();
        const bool theCreatSslPskFlag =
            theParams.getValue(
                theParamName.Truncate(theCurLen).Append(
                "disable"), 0) == 0;
        const bool thePskSslChangedFlag =
            (theCreatSslPskFlag != (mSslCtxPtr != 0)) ||
            theParams.getValue(
                theParamName.Truncate(theCurLen).Append(
                "forceReload"), 0) != 0 ||
            ! theParams.equalsWithPrefix(
                theParamName.Truncate(theCurLen).GetPtr(), theCurLen, mParams);
        SslCtxPtr theSslCtxPtr;
        if (thePskSslChangedFlag && theCreatSslPskFlag) {
            const bool kServerFlag  = false;
            const bool kPskOnlyFlag = true;
            string     theErrMsg;
            theSslCtxPtr = SslFilter::MakeCtxPtr(SslFilter::CreateCtx(
                kServerFlag,
                kPskOnlyFlag,
                theParamName.Truncate(theCurLen).GetPtr(),
                theParams,
                &theErrMsg
            ));
            if (! theSslCtxPtr) {
                KFS_LOG_STREAM_ERROR <<
                    theParamName.Truncate(theCurLen) <<
                    "* configuration error: " << theErrMsg <<
                KFS_LOG_EOM;
                return false;
            }
        }
        if (thePskSslChangedFlag) {
            mSslCtxPtr = theSslCtxPtr;
        }
        mParams.swap(theParams);
        mEnabledFlag = mSslCtxPtr && mParams.getValue(
            theParamName.Truncate(thePrefLen).Append(
            "disable"), 0) == 0;
        return true;
    }
    bool Setup(
        NetConnection&         inConn,
        const string&          inSessionId,
        const CryptoKeys::Key& inSessionKey)
    {
        if (! mEnabledFlag) {
            return true;
        }
        SslFilter::VerifyPeer* const kVerifyPeerPtr     = 0;
        SslFilter::ServerPsk* const  kServerPskPtr      = 0;
        const bool                   kDeleteOnCloseFlag = true;
        SslFilter* const theFilterPtr = new SslFilter(
            *mSslCtxPtr,
            inSessionKey.GetPtr(),
            (int)inSessionKey.GetSize(),
            inSessionId.c_str(),
            kServerPskPtr,
            kVerifyPeerPtr,
            kDeleteOnCloseFlag
        );
        const SslFilter::Error theErr = theFilterPtr->GetError();
        if (theErr) {
            KFS_LOG_STREAM_ERROR <<
                "ssl filter create error: " <<
                    SslFilter::GetErrorMsg(theErr) <<
                " status: " << theErr <<
            KFS_LOG_EOM;
            delete theFilterPtr;
            return false;
        }
        string theErrMsg;
        const int theStatus = inConn.SetFilter(theFilterPtr, &theErrMsg);
        if (theStatus == 0) {
            return true;
        }
        KFS_LOG_STREAM_ERROR <<
            "set ssl filter error: " <<
                QCUtils::SysError(-theStatus) <<
        KFS_LOG_EOM;
        return false;
    }
private:
    typedef SslFilter::CtxPtr SslCtxPtr;

    SslCtxPtr  mSslCtxPtr;
    Properties mParams;
    bool       mEnabledFlag;
private:
    Auth(
        const Auth& inAuth);
    Auth& operator=(
        const Auth& inAuth);
};

RemoteSyncSM::Auth* RemoteSyncSM::sAuthPtr              = 0;
bool                RemoteSyncSM::sTraceRequestResponse = false;
int                 RemoteSyncSM::sOpResponseTimeoutSec = 5 * 60;

const int kMaxCmdHeaderLength = 2 << 10;

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

bool
RemoteSyncSM::SetParameters(const char* prefix, const Properties& props)
{
    Properties::String name(prefix);
    const size_t       len(name.GetSize());
    sTraceRequestResponse = props.getValue(
        name.Truncate(len).Append(
            "traceRequestResponse"), sTraceRequestResponse ? 1 : 0) != 0;
    sOpResponseTimeoutSec = props.getValue(
        name.Truncate(len).Append(
            "traceRequestResponse"), sOpResponseTimeoutSec);
    if (! sAuthPtr) {
        sAuthPtr = new Auth();
    }
    return sAuthPtr->SetParameters(
        name.Truncate(len).Append("auth.").GetPtr(), props);
}

void
RemoteSyncSM::Shutdown()
{
    delete sAuthPtr;
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
      mSessionId(),
      mSessionKey(),
      mShutdownSslFlag(false),
      mSslShutdownInProgressFlag(false),
      mSessionExpirationTime(0),
      mIStream(),
      mWOStream(),
      mList(0),
      mListIt()
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
    if (res < 0 && res != -EINPROGRESS) {
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
    assert(sAuthPtr);
    mSslShutdownInProgressFlag = false;
    if (! mSessionId.empty()) {
        int  err          = 0;
        bool noFilterFlag = false;
        if (! sAuthPtr->Setup(*mNetConnection, mSessionId, mSessionKey) ||
                (noFilterFlag = ! mNetConnection->GetFilter()) ||
                (mShutdownSslFlag && (err = mNetConnection->Shutdown()) != 0)) {
            if (err) {
                KFS_LOG_STREAM_ERROR <<
                    mLocation << ": ssl shutdown failed status: " << err <<
                KFS_LOG_EOM;
            } else if (noFilterFlag) {
                KFS_LOG_STREAM_ERROR <<
                    mLocation << ": auth context configuration error" <<
                KFS_LOG_EOM;
            }
            mNetConnection.reset();
            return false;
        }
        mSslShutdownInProgressFlag = mShutdownSslFlag;
    }
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
        mNetConnection->WriteCopy(&wpfo->owner.dataBuf,
            wpfo->owner.dataBuf.BytesConsumable());
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
    const char *reason = "inactivity timeout";

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


    case EVENT_NET_ERROR:
        if (mSslShutdownInProgressFlag &&
                mNetConnection && mNetConnection->IsGood()) {
            KFS_LOG_STREAM(mNetConnection->GetFilter() ?
                    MsgLogger::kLogLevelERROR :
                    MsgLogger::kLogLevelDEBUG) << mLocation <<
                " ssl shutdown completion:"
                " filter: " << reinterpret_cast<const void*>(
                    mNetConnection->GetFilter()) <<
            KFS_LOG_EOM;
            mSslShutdownInProgressFlag = false;
            if (! mNetConnection->GetFilter()) {
                break;
            }
        }
        reason = "error";
    case EVENT_INACTIVITY_TIMEOUT:
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
                op->status    = -KfsToSysErrno(-op->status);
                op->statusMsg = prop.getValue("Status-message", string());
            }
            if (op->op == CMD_WRITE_ID_ALLOC) {
                WriteIdAllocOp* const wiao  = static_cast<WriteIdAllocOp*>(op);
                wiao->writeIdStr            = prop.getValue("Write-id", string());
                wiao->writePrepareReplyFlag =
                    prop.getValue("Write-prepare-reply", 0) != 0;
            } else if (op->op == CMD_READ) {
                ReadOp* const rop = static_cast<ReadOp*>(op);
                const int checksumEntries = prop.getValue("Checksum-entries", 0);
                rop->checksum.clear();
                if (checksumEntries > 0) {
                    istringstream is(prop.getValue("Checksums", string()));
                    uint32_t cks;
                    for (int i = 0; i < checksumEntries; i++) {
                        is >> cks;
                        rop->checksum.push_back(cks);
                    }
                }
                rop->skipVerifyDiskChecksumFlag =
                    rop->skipVerifyDiskChecksumFlag &&
                    prop.getValue("Skip-Disk-Chksum", 0) != 0;
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
                SizeOp* const sop = static_cast<SizeOp*>(op);
                sop->size = prop.getValue("Size", 0);
            } else if (op->op == CMD_GET_CHUNK_METADATA) {
                GetChunkMetadataOp* const gcm =
                    static_cast<GetChunkMetadataOp*>(op);
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
        KfsOp* const op = i->second;
        mDispatchedOps.erase(i);
        if (op->op == CMD_READ) {
            ReadOp* const rop = static_cast<ReadOp*>(op);
            rop->dataBuf.Move(iobuf, mReplyNumBytes);
            rop->numBytesIO = mReplyNumBytes;
        } else if (op->op == CMD_GET_CHUNK_METADATA) {
            GetChunkMetadataOp* const gcm =
                static_cast<GetChunkMetadataOp*>(op);
            gcm->dataBuf.Move(iobuf, mReplyNumBytes);
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
    RemoveFromList();
}

inline static time_t
GetExpirationTime(
    const char* sessionTokenPtr,
    int         sessionTokenLen,
    int&        err,
    string&     errMsg)
{
    DelegationToken token;
    if (! token.FromString(sessionTokenPtr, sessionTokenLen, 0, 0)) {
        err    = -EINVAL;
        errMsg = "invalid session token format";
        return 0;
    }
    return (token.GetIssuedTime() + token.GetValidForSec());
}

bool
RemoteSyncSM::UpdateSession(
    const char* sessionTokenPtr,
    int         sessionTokenLen,
    const char* sessionKeyPtr,
    int         sessionKeyLen,
    bool        writeMasterFlag,
    int&        err,
    string&     errMsg)
{
    if (sessionTokenLen <= 0 || sessionKeyLen <= 0) {
        err    = -EINVAL;
        errMsg = "invalid session and/or key length";
        return false;
    }
    if (mSessionId.empty() && mNetConnection && mNetConnection->IsGood()) {
        err    = -EINVAL;
        errMsg = "sync replication: no current session";
        return false;
    }
    err = 0;
    errMsg.clear();
    const time_t now = globalNetManager().Now();
    if (now + LEASE_INTERVAL_SECS <= mSessionExpirationTime) {
        return false;
    }
    const time_t expTime = GetExpirationTime(
        sessionTokenPtr, sessionTokenLen, err, errMsg);
    if (err || expTime < mSessionExpirationTime) {
        return false;
    }
    CryptoKeys::Key key;
    if (writeMasterFlag) {
        if (! key.Parse(sessionKeyPtr, sessionKeyLen)) {
            errMsg = "invalid session key format";
            err    = -EINVAL;
        }
    } else {
        err = DelegationToken::DecryptSessionKeyFromString(
            gChunkManager.GetCryptoKeys(),
            sessionKeyPtr,
            sessionKeyLen,
            key,
            &errMsg
        );
    }
    if (! err) {
        SetSessionKey(sessionTokenPtr, sessionTokenLen, key);
        mSessionExpirationTime = expTime;
    }
    return (err == 0);
}

RemoteSyncSM::SMPtr
RemoteSyncSM::Create(
    const ServerLocation& location,
    const char*           sessionTokenPtr,
    int                   sessionTokenLen,
    const char*           sessionKeyPtr,
    int                   sessionKeyLen,
    bool                  writeMasterFlag,
    bool                  shutdownSslFlag,
    int&                  err,
    string&               errMsg)
{
    err = 0;
    errMsg.clear();
    if (sessionKeyLen <= 0) {
        return SMPtr(new RemoteSyncSM(location));
    }
    if (sessionTokenLen <= 0) {
        err    = -EINVAL;
        errMsg = "invalid session token length";
    } else {
        const time_t expTime = GetExpirationTime(
            sessionTokenPtr, sessionTokenLen, err, errMsg);
        if (! err) {
            if (expTime < globalNetManager().Now()) {
                errMsg = "session token has expired";
                err    = -EINVAL;
            } else  {
                CryptoKeys::Key key;
                if (writeMasterFlag) {
                    if (! key.Parse(sessionKeyPtr, sessionKeyLen)) {
                        errMsg = "invalid session key format";
                        err    = -EINVAL;
                    }
                } else {
                    err = DelegationToken::DecryptSessionKeyFromString(
                        gChunkManager.GetCryptoKeys(),
                        sessionKeyPtr,
                        sessionKeyLen,
                        key,
                        &errMsg
                    );
                }
                if (! err) {
                    SMPtr peer(new RemoteSyncSM(location));
                    peer->SetSessionKey(sessionTokenPtr, sessionTokenLen, key);
                    peer->SetShutdownSslFlag(shutdownSslFlag);
                    peer->mSessionExpirationTime = expTime;
                    return peer;
                }
            }
        }
    }
    KFS_LOG_STREAM_ERROR <<
        "failed to forward: " << errMsg << " status: " << err <<
    KFS_LOG_EOM;
    return SMPtr();
}

//
// Utility functions to operate on a list of remotesync servers
//

class RemoteSyncSMMatcher
{
    const ServerLocation& mLocation;
    const bool            mAuthFlag;
    const bool            mShutdownSslFlag;
public:
    RemoteSyncSMMatcher(
        const ServerLocation& loc,
        bool                  authFlag,
        bool                  shutdownSslFlag)
        :  mLocation(loc),
           mAuthFlag(authFlag),
           mShutdownSslFlag(shutdownSslFlag)
        {}
    bool operator() (const RemoteSyncSMPtr& other) const
    {
        const RemoteSyncSM& sm = *other;
        return (
            sm.GetLocation()       == mLocation &&
            sm.HasAuthentication() == mAuthFlag &&
            (! mAuthFlag || sm.GetShutdownSslFlag() == mShutdownSslFlag)
        );
    }
};

RemoteSyncSMPtr
FindServer(
    RemoteSyncSMList&     remoteSyncers,
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
    err = 0;
    errMsg.clear();
    RemoteSyncSMPtr const res = remoteSyncers.Find(
        RemoteSyncSMMatcher(
            location,
            0 < sessionKeyLen,
            shutdownSslFlag
    ));
    if (res) {
        if (0 < sessionKeyLen) {
            if (sessionTokenLen <= 0) {
                err    = -EINVAL;
                errMsg = "invalid session token length";
                return RemoteSyncSMPtr();
            }
            if (! res->UpdateSession(
                    sessionTokenPtr,
                    sessionTokenLen,
                    sessionKeyPtr,
                    sessionKeyLen,
                    writeMasterFlag,
                    err,
                    errMsg) && err) {
                return RemoteSyncSMPtr();
            }
        }
        return res;
    }
    if (! connectFlag) {
        errMsg = "no remote peer found";
        err    = -ENOENT;
        return res;
    }
    RemoteSyncSMPtr const peer = RemoteSyncSM::Create(
        location,
        sessionTokenPtr,
        sessionTokenLen,
        sessionKeyPtr,
        sessionKeyLen,
        writeMasterFlag,
        shutdownSslFlag,
        err,
        errMsg
    );
    if (! peer) {
        return res;
    }
    if (peer->Connect()) {
        return remoteSyncers.PutInList(*peer);
    } else {
        errMsg = "connection failure";
        err    = -EHOSTUNREACH;
    }
    // Failed to connect, return 0
    return res;
}

}
