//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/05
// Author: Sriram Rao
//         Mike Ovsiannikov implement multiple outstanding request processing,
//         and "client threads".
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
// \file ClientSM.cc
// \brief Kfs client protocol state machine implementation.
//
//----------------------------------------------------------------------------

#include "ClientSM.h"
#include "ChunkServer.h"
#include "NetDispatch.h"
#include "util.h"
#include "common/kfstypes.h"
#include "kfsio/Globals.h"
#include "qcdio/qcstutils.h"
#include "qcdio/QCUtils.h"
#include "kfsio/IOBuffer.h"
#include "common/MsgLogger.h"
#include "common/Properties.h"
#include "AuditLog.h"
#include "AuthContext.h"

#include <string>
#include <sstream>
#include <algorithm>

namespace KFS
{

using std::max;
using std::string;
using std::ostringstream;


inline string
PeerName(const NetConnectionPtr& conn)
{
    return (conn ? conn->GetPeerName() : string("unknown"));
}

inline string
PeerIp(const NetConnectionPtr& conn)
{
    if (! conn) {
        return string();
    }
    const string peer = conn->GetPeerName();
    const size_t pos  = peer.rfind(':');
    if (pos == string::npos) {
        return peer;
    }
    return peer.substr(0, pos);
}

inline AuthContext&
ClientSM::GetAuthContext()
{
    return (ClientManager::GetAuthContext(mClientThread));
}

int  ClientSM::sMaxPendingOps             = 1;
int  ClientSM::sMaxPendingBytes           = 3 << 10;
int  ClientSM::sMaxReadAhead              = 3 << 10;
int  ClientSM::sInactivityTimeout         = 8 * 60;
int  ClientSM::sMaxWriteBehind            = 3 << 10;
int  ClientSM::sBufCompactionThreshold    = 1 << 10;
int  ClientSM::sOutBufCompactionThreshold = 8 << 10;
int  ClientSM::sClientCount               = 0;
bool ClientSM::sAuditLoggingFlag          = false;
ClientSM* ClientSM::sClientSMPtr[1]      = {0};
IOBuffer::WOStream ClientSM::sWOStream;

/* static */ void
ClientSM::SetParameters(const Properties& prop)
{
    const int maxPendingOps = prop.getValue(
        "metaServer.clientSM.maxPendingOps",
        -1);
    if (maxPendingOps > 0) {
        sMaxPendingOps = maxPendingOps;
    } else if (! gNetDispatch.IsRunning() &&
            prop.getValue("metaServer.clientThreadCount", -1) > 0) {
        sMaxPendingOps = 16;
    }
    sMaxPendingBytes = max(1, prop.getValue(
        "metaServer.clientSM.maxPendingBytes",
        sMaxPendingBytes));
    sMaxReadAhead = max(256, prop.getValue(
        "metaServer.clientSM.maxReadAhead",
        sMaxReadAhead));
    sInactivityTimeout = prop.getValue(
        "metaServer.clientSM.inactivityTimeout",
        sInactivityTimeout);
    sMaxWriteBehind = max(1, prop.getValue(
        "metaServer.clientSM.maxWriteBehind",
        sMaxWriteBehind));
    sBufCompactionThreshold = prop.getValue(
        "metaServer.clientSM.bufCompactionThreshold",
        sBufCompactionThreshold);
    sOutBufCompactionThreshold = prop.getValue(
        "metaServer.clientSM.outBufCompactionThreshold",
        sOutBufCompactionThreshold);
    sAuditLoggingFlag = prop.getValue(
        "metaServer.clientSM.auditLogging",
        sAuditLoggingFlag ? 1 : 0) != 0;
    AuditLog::SetParameters(prop);
}

ClientSM::ClientSM(
    const NetConnectionPtr&      conn,
    ClientManager::ClientThread* thread,
    IOBuffer::WOStream*          wostr,
    char*                        parseBuffer)
    : KfsCallbackObj(),
      SslFilterVerifyPeer(),
      mNetConnection(conn),
      mClientIp(PeerIp(conn)),
      mPendingOpsCount(0),
      mOstream(wostr ? *wostr : sWOStream),
      mParseBuffer(parseBuffer),
      mRecursionCnt(0),
      mClientProtoVers(KFS_CLIENT_PROTO_VERS),
      mDisconnectFlag(false),
      mLastReadLeft(0),
      mAuthenticateOp(0),
      mAuthName(),
      mClientThread(thread),
      mNext(0)
{
    assert(mNetConnection && mNetConnection->IsGood());

    ClientSMList::Init(*this);
    {
        QCStMutexLocker locker(gNetDispatch.GetClientManagerMutex());
        ClientSMList::PushBack(sClientSMPtr, *this);
        sClientCount++;
    }
    mNetConnection->SetInactivityTimeout(sInactivityTimeout);
    mNetConnection->SetMaxReadAhead(sMaxReadAhead);
    SET_HANDLER(this, &ClientSM::HandleRequest);
}

ClientSM::~ClientSM()
{
    delete mAuthenticateOp;
    QCStMutexLocker locker(gNetDispatch.GetClientManagerMutex());
    ClientSMList::Remove(sClientSMPtr, *this);
    sClientCount--;
}

///
/// Send out the response to the client request.  The response is
/// generated by MetaRequest as per the protocol.
/// @param[in] op The request for which we finished execution.
///
void
ClientSM::SendResponse(MetaRequest *op)
{
    if ((op->op == META_ALLOCATE && (op->status < 0 ||
            static_cast<const MetaAllocate*>(op)->logFlag)) ||
            MsgLogger::GetLogger()->IsLogLevelEnabled(
                MsgLogger::kLogLevelDEBUG)) {
        // for chunk allocations, for debugging purposes, need to know
        // where the chunk was placed.
        KFS_LOG_STREAM_INFO << PeerName(mNetConnection) <<
            " -seq: "   << op->opSeqno <<
            " status: " << op->status <<
            (op->statusMsg.empty() ?
                "" : " msg: ") << op->statusMsg <<
            " "         << op->Show() <<
        KFS_LOG_EOM;
    }
    if (! mNetConnection) {
        return;
    }
    if (op->op == META_DISCONNECT) {
        mDisconnectFlag = true;
    }
    op->response(
        mOstream.Set(mNetConnection->GetOutBuffer()),
        mNetConnection->GetOutBuffer());
    mOstream.Reset();
    if (mRecursionCnt <= 0) {
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
ClientSM::HandleRequest(int code, void *data)
{
    if (code == EVENT_CMD_DONE) {
        assert(data && mPendingOpsCount > 0);
        if (ClientManager::Enqueue(mClientThread,
                *reinterpret_cast<MetaRequest*>(data))) {
            return 0;
        }
    }
    return HandleRequestSelf(code, data);
}

int
ClientSM::HandleRequestSelf(int code, void *data)
{
    assert(mRecursionCnt >= 0 && (mNetConnection ||
        (code == EVENT_CMD_DONE && data && mPendingOpsCount > 0)));
    mRecursionCnt++;

    switch (code) {
    case EVENT_NET_READ: {
        // We read something from the network. Run the RPC that
        // came in.
        mLastReadLeft = 0;
        IOBuffer& iobuf = mNetConnection->GetInBuffer();
        if (mDisconnectFlag) {
            iobuf.Clear(); // Discard
        }
        assert(data == &iobuf);
        HandleAuthenticate(iobuf);
        if (mAuthenticateOp) {
            break;
        }
        // Do not start new op if response does not get unloaded by
        // the client to prevent out of buffers.
        bool overWriteBehindFlag = false;
        for (; ;) {
            while ((overWriteBehindFlag =
                    mNetConnection->GetNumBytesToWrite() >=
                        sMaxWriteBehind) &&
                    mRecursionCnt <= 1 &&
                    mNetConnection->CanStartFlush()) {
                mNetConnection->StartFlush();
            }
            int cmdLen;
            if (overWriteBehindFlag ||
                    IsOverPendingOpsLimit() ||
                    ! IsMsgAvail(&iobuf, &cmdLen)) {
                break;
            }
            HandleClientCmd(iobuf, cmdLen);
            if (mAuthenticateOp) {
                break;
            }
        }
        if (overWriteBehindFlag || mAuthenticateOp) {
            break;
        }
        if (! IsOverPendingOpsLimit() && ! mDisconnectFlag) {
            mLastReadLeft = iobuf.BytesConsumable();
            if (mLastReadLeft <= MAX_RPC_HEADER_LEN) {
                mNetConnection->SetMaxReadAhead(sMaxReadAhead);
                break;
            }
            KFS_LOG_STREAM_ERROR << PeerName(mNetConnection) <<
                " exceeded max request header size: " <<
                    mLastReadLeft <<
                " > " << MAX_RPC_HEADER_LEN <<
                " closing connection" <<
            KFS_LOG_EOM;
            mLastReadLeft = 0;
            iobuf.Clear();
            mNetConnection->Close();
            HandleRequest(EVENT_NET_ERROR, 0);
        }
        break;
    }

    case EVENT_CMD_DONE: {
        assert(data && mPendingOpsCount > 0);
        MetaRequest* const op = reinterpret_cast<MetaRequest*>(data);
        if (sAuditLoggingFlag && ! op->reqHeaders.IsEmpty()) {
            AuditLog::Log(*op);
        }
        const bool deleteOpFlag = op != mAuthenticateOp;
        SendResponse(op);
        if (deleteOpFlag) {
            delete op;
        }
        mPendingOpsCount--;
        if (! mNetConnection) {
            break;
        }
        if (mRecursionCnt <= 1 &&
                (mPendingOpsCount <= 0 ||
                ! ClientManager::Flush(mClientThread, *this))) {
            mNetConnection->StartFlush();
        }
    }
        // Fall through.
    case EVENT_NET_WROTE:
        // Something went out on the network.
        // Process next command.
        if (mAuthenticateOp && ! mNetConnection->IsWriteReady()) {
            if (mAuthenticateOp->status != 0 ||
                    mNetConnection->HasPendingRead()) {
                mDisconnectFlag = true;
                if (mAuthenticateOp->status != 0) {
                    KFS_LOG_STREAM_ERROR << PeerName(mNetConnection) <<
                        " authentication failure:" <<
                        " status: " << mAuthenticateOp->status <<
                        " " << mAuthenticateOp->statusMsg <<
                    KFS_LOG_EOM;
                } else {
                    KFS_LOG_STREAM_ERROR << PeerName(mNetConnection) <<
                        " authentication failure:" <<
                        " status: " << mAuthenticateOp->status <<
                        " out of order data received" <<
                    KFS_LOG_EOM;
                }
                delete mAuthenticateOp;
                mAuthenticateOp = 0;
            } else {
                mAuthName = mAuthenticateOp->authName;
                NetConnection::Filter* const filter = mAuthenticateOp->filter;
                mAuthenticateOp->filter = 0;
                delete mAuthenticateOp;
                mAuthenticateOp = 0;
                if (filter) {
                    string errMsg;
                    const int err = mNetConnection->SetFilter(filter, &errMsg);
                    if (err) {
                        if (errMsg.empty()) {
                            errMsg = QCUtils::SysError(err < 0 ? -err : err);
                        }
                        KFS_LOG_STREAM_ERROR << PeerName(mNetConnection) <<
                            " failed to set ssl filer:" <<
                            " status: " << err <<
                            " " << errMsg <<
                        KFS_LOG_EOM;
                        mDisconnectFlag = true;
                    }
                }
                if (! mDisconnectFlag) {
                    KFS_LOG_STREAM_DEBUG << PeerName(mNetConnection) <<
                        " auth reply sent:" <<
                        " ssl: "  << (filter ? 1 : 0) <<
                        " name: " << mAuthName <<
                    KFS_LOG_EOM;
                }
            }
        }
        if (! IsOverPendingOpsLimit() &&
                mRecursionCnt <= 1 &&
                ! mAuthenticateOp &&
                (code == EVENT_CMD_DONE || ! mNetConnection->IsReadReady()) &&
                mNetConnection->GetNumBytesToWrite() < sMaxWriteBehind) {
            if (mNetConnection->GetNumBytesToRead() > mLastReadLeft ||
                    mDisconnectFlag) {
                HandleRequest(EVENT_NET_READ, &mNetConnection->GetInBuffer());
            } else if (! mNetConnection->IsReadReady()) {
                mNetConnection->SetMaxReadAhead(sMaxReadAhead);
            }
        }
        break;

    case EVENT_NET_ERROR:
        if (mNetConnection->IsGood() &&
                (mPendingOpsCount > 0 ||
                mNetConnection->IsWriteReady())) {
            // Fin from the other side, flush and close connection.
            mDisconnectFlag = true;
            break;
        }
        // Fall through.
    case EVENT_INACTIVITY_TIMEOUT:
        KFS_LOG_STREAM_DEBUG << PeerName(mNetConnection) <<
            " closing connection " <<
            (code == EVENT_INACTIVITY_TIMEOUT ?
                string(" timed out") :
                (mNetConnection->IsGood() ?
                    string("EOF") : mNetConnection->GetErrorMsg())
            ) <<
        KFS_LOG_EOM;
        mNetConnection->Close();
        mNetConnection->GetInBuffer().Clear();
        break;

    default:
        assert(!"Unknown event");
        break;
    }

    if (mRecursionCnt <= 1) {
        bool goodFlag = mNetConnection && mNetConnection->IsGood();
        if (goodFlag && (mPendingOpsCount <= 0 ||
                ! ClientManager::Flush(mClientThread, *this))) {
            mNetConnection->StartFlush();
            goodFlag = mNetConnection && mNetConnection->IsGood();
        }
        if (goodFlag && mDisconnectFlag) {
            if (mPendingOpsCount <= 0 &&  ! mNetConnection->IsWriteReady()) {
                mNetConnection->Close();
                goodFlag = false;
            } else {
                mNetConnection->SetMaxReadAhead(0);
            }
        }
        if (goodFlag) {
            IOBuffer& inbuf = mNetConnection->GetInBuffer();
            int numBytes = inbuf.BytesConsumable();
            if (numBytes <= sBufCompactionThreshold && numBytes > 0) {
                inbuf.MakeBuffersFull();
            }
            IOBuffer& outbuf = mNetConnection->GetOutBuffer();
            numBytes = outbuf.BytesConsumable();
            if (numBytes <= sOutBufCompactionThreshold &&
                    numBytes > 0) {
                outbuf.MakeBuffersFull();
            }
            if (mNetConnection->IsReadReady() &&
                    (IsOverPendingOpsLimit() ||
                    mNetConnection->GetNumBytesToWrite() >= sMaxWriteBehind ||
                    mNetConnection->GetNumBytesToRead()  >= sMaxPendingBytes)) {
                mLastReadLeft = 0;
                mNetConnection->SetMaxReadAhead(0);
            }
        } else {
            if (mPendingOpsCount > 0) {
                mNetConnection.reset();
            } else {
                delete this;
                return 0;
            }
        }
    }
    assert(
        mRecursionCnt > 0 &&
        (mRecursionCnt > 1 || mPendingOpsCount > 0 ||
            (mNetConnection && mNetConnection->IsGood()))
    );
    mRecursionCnt--;
    return 0;
}

///
/// We have a command in a buffer. So, parse out the command and
/// execute it if possible.
/// @param[in] iobuf: Buffer containing the command
/// @param[in] cmdLen: Length of the command in the buffer
///
void
ClientSM::HandleClientCmd(IOBuffer& iobuf, int cmdLen)
{
    assert(! IsOverPendingOpsLimit() && mNetConnection);
    MetaRequest* op = 0;
    if (ParseCommand(iobuf, cmdLen, &op, mParseBuffer) != 0) {
        IOBuffer::IStream is(iobuf, cmdLen);
        char buf[128];
        int  maxLines = 16;
        while (maxLines-- > 0 && is.getline(buf, sizeof(buf))) {
            KFS_LOG_STREAM_ERROR << PeerName(mNetConnection) <<
                " invalid request: " << buf <<
            KFS_LOG_EOM;
        }
        iobuf.Clear();
        mNetConnection->Close();
        HandleRequest(EVENT_NET_ERROR, 0);
        return;
    }
    if (op->clientProtoVers < mClientProtoVers) {
        mClientProtoVers = op->clientProtoVers;
        KFS_LOG_STREAM_WARN << PeerName(mNetConnection) <<
            " command with old protocol version: " <<
            op->clientProtoVers << ' ' << op->Show() <<
        KFS_LOG_EOM;
    }
    // Command is ready to be pushed down.  So remove the cmd from the buffer.
    if (sAuditLoggingFlag) {
        op->reqHeaders.Move(&iobuf, cmdLen);
    } else {
        iobuf.Consume(cmdLen);
    }
    KFS_LOG_STREAM_DEBUG << PeerName(mNetConnection) <<
        " +seq: " << op->opSeqno <<
        " "       << op->Show() <<
        " pending:"
        " rd: "   << mNetConnection->GetNumBytesToRead() <<
        " wr: "   << mNetConnection->GetNumBytesToWrite() <<
    KFS_LOG_EOM;
    op->clientIp         = mClientIp;
    op->fromClientSMFlag = true;
    op->clnt             = this;
    op->authName         = mAuthName;
    mPendingOpsCount++;
    if (op->op == META_AUTHENTICATE) {
        assert(! mAuthenticateOp);
        mAuthenticateOp = static_cast<MetaAuthenticate*>(op);
        HandleAuthenticate(iobuf);
        return;
    }
    if (op->op == META_LOOKUP) {
        MetaLookup& lookupOp = *static_cast<MetaLookup*>(op);
        if (lookupOp.IsAuthNegotiation()) {
            lookupOp.authType = GetAuthContext().GetAuthTypes();
            if (mPendingOpsCount == 1) {
                HandleRequestSelf(EVENT_CMD_DONE, op);
            } else {
                HandleRequest(EVENT_CMD_DONE, op);
            }
            return;
        }
    }
    ClientManager::SubmitRequest(mClientThread, *op);
}

void
ClientSM::HandleAuthenticate(IOBuffer& iobuf)
{
    if (! mAuthenticateOp || mAuthenticateOp->doneFlag) {
        return;
    }
    if (mAuthenticateOp->contentBufPos <= 0) {
        mAuthName.clear();
        if (mNetConnection->GetFilter()) {
            // If filter already exits then do not allow authentication for now,
            // as this might require changing the filter / ssl on both sides.
            mAuthenticateOp->status    = -EINVAL;
            mAuthenticateOp->statusMsg = "re-authentication is not supported";
            delete [] mAuthenticateOp->contentBuf;
            mAuthenticateOp->contentBufPos = 0;
        } else {
            GetAuthContext().Validate(*mAuthenticateOp);
        }
    }
    const int rem = mAuthenticateOp->Read(iobuf);
    if (0 < rem) {
        // Try to read more, to detect protocol error, as the client
        // should not send anything else prior to receiving the response.Fi
        mNetConnection->SetMaxReadAhead(rem + sMaxReadAhead);
        return;
    }
    if (mAuthenticateOp->status == 0 &&
            (! iobuf.IsEmpty() || mPendingOpsCount != 1 ||
                mNetConnection->GetFilter())) {
        mAuthenticateOp->status    = -EINVAL;
        mAuthenticateOp->statusMsg = "out of order data received";
    } else {
        GetAuthContext().Authenticate(*mAuthenticateOp, this);
    }
    mDisconnectFlag = mDisconnectFlag || mAuthenticateOp->status != 0;
    mAuthenticateOp->doneFlag = true;
    if (mPendingOpsCount == 1) {
        HandleRequestSelf(EVENT_CMD_DONE, mAuthenticateOp);
    } else {
        HandleRequest(EVENT_CMD_DONE, mAuthenticateOp);
    }
    return;
}

    /* virtual */ bool
ClientSM::Verify(
    string&       ioFilterAuthName,
    bool          inPreverifyOkFlag,
    int           inCurCertDepth,
    const string& inPeerName)
{
    KFS_LOG_STREAM_DEBUG << PeerName(mNetConnection)  <<
        " auth. verify:" <<
        " name: "        << inPeerName <<
        " prev: "        << ioFilterAuthName <<
        " preverify: "   << inPreverifyOkFlag <<
        " depth: "       << inCurCertDepth <<
    KFS_LOG_EOM;
    // For now do no allow to renegotiate and change the name.
    string authName = inPeerName;
    if (! inPreverifyOkFlag ||
            (inCurCertDepth == 0 &&
            (! GetAuthContext().RemapAndValidate(authName) ||
            (! mAuthName.empty() && authName != mAuthName)))) {
        KFS_LOG_STREAM_ERROR << PeerName(mNetConnection) <<
            " autentication failure:"
            " peer: "  << inPeerName <<
            " name: "  << authName <<
            " depth: " << inCurCertDepth <<
            " is not valid" <<
            (mAuthName.empty() ? "" : " prev name: ") << mAuthName <<
        KFS_LOG_EOM;
        mAuthName.clear();
        ioFilterAuthName.clear();
        return false;
    }
    if (inCurCertDepth == 0) {
        ioFilterAuthName = inPeerName;
        mAuthName        = authName;
    }
    return true;
}

} // namespace KFS
