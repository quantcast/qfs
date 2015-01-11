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
#include "ChunkManager.h"
#include "ClientManager.h"
#include "ClientThread.h"
#include "utils.h"

#include "common/MsgLogger.h"
#include "common/Properties.h"
#include "common/kfserrno.h"

#include "kfsio/NetManager.h"
#include "kfsio/SslFilter.h"
#include "kfsio/Globals.h"
#include "kfsio/DelegationToken.h"

#include "qcdio/QCMutex.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"
#include "qcdio/qcdebug.h"

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
using libkfsio::globalNetManager;

class ClientThreadRemoteSyncListEntry::StMutexLocker :
    public ClientThread::StMutexLocker
{
public:
    StMutexLocker(
        ClientThreadRemoteSyncListEntry& inEntry)
        : ClientThread::StMutexLocker(inEntry.mClientThreadPtr)
        {}
};

inline NetManager&
ClientThreadRemoteSyncListEntry::GetNetManager()
{
    return (mClientThreadPtr ?
        mClientThreadPtr->GetNetManager() : globalNetManager());
}

inline const QCMutex*
RemoteSyncSM::GetMutexPtr()
{
    return gClientManager.GetMutexPtr();
}

inline bool IsMutexOwner(const QCMutex* mutex)
{
    return (! mutex || mutex->IsOwned());
}

inline static time_t
TimeNow()
{
    ClientThread* const curThread = gClientManager.GetCurrentClientThreadPtr();
    return (curThread ? curThread->GetNetManager() : globalNetManager()).Now();
}

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
        const Properties& inParameters,
        bool              inAuthEnabledFlag)
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
            if (! theSslCtxPtr && inAuthEnabledFlag) {
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
            "enabled"), inAuthEnabledFlag ? 1 : 0) != 0;
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
    void Clear()
    {
        mSslCtxPtr.reset();
        mParams.clear();
        mEnabledFlag = false;
    }
    bool IsEnabled() const
        { return mEnabledFlag; }
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

RemoteSyncSM::Auth* RemoteSyncSM::sAuthPtr                  = 0;
bool                RemoteSyncSM::sTraceRequestResponseFlag = false;
int                 RemoteSyncSM::sOpResponseTimeoutSec     = 5 * 60;
int                 RemoteSyncSM::sRemoteSyncCount          = 0;

const int kMaxCmdHeaderLength = 2 << 10;

inline void
RemoteSyncSM::UpdateRecvTimeout()
{
    if (mOpResponseTimeoutSec < 0 || ! mNetConnection) {
        return;
    }
    const time_t now = GetNetManager().Now();
    const time_t end = mLastRecvTime + mOpResponseTimeoutSec;
    mNetConnection->SetInactivityTimeout(end > now ? end - now : 0);
}

/* static */ bool
RemoteSyncSM::SetParameters(
    const char* prefix, const Properties& props, bool authEnabledFlag)
{
    QCASSERT(IsMutexOwner(GetMutexPtr()));

    Properties::String name(prefix);
    const size_t       len(name.GetSize());
    sTraceRequestResponseFlag = props.getValue(
        name.Truncate(len).Append(
            "traceRequestResponse"), sTraceRequestResponseFlag ? 1 : 0) != 0;
    sOpResponseTimeoutSec = props.getValue(
        name.Truncate(len).Append(
            "responseTimeoutSec"), sOpResponseTimeoutSec);
    if (! sAuthPtr) {
        sAuthPtr = new Auth();
    }
    return sAuthPtr->SetParameters(
        name.Truncate(len).Append("auth.").GetPtr(), props, authEnabledFlag);
}

/* static */ void
RemoteSyncSM::Shutdown()
{
    QCASSERT(IsMutexOwner(GetMutexPtr()));
    if (sRemoteSyncCount != 0) {
        die("remote sync shutdown: invalid instance count");
    }
    delete sAuthPtr;
    sAuthPtr = 0;
}

// State machine for communication with other chunk servers.
RemoteSyncSM::RemoteSyncSM(
    const ServerLocation& location,
    ClientThread*         thread)
    : KfsCallbackObj(),
      ClientThreadRemoteSyncListEntry(thread),
      mNetConnection(),
      mLocation(location),
      mSeqnum(GetRandomSeq()),
      mDispatchedOps(),
      mReplySeqNum(-1),
      mReplyNumBytes(0),
      mRecursionCount(0),
      mLastRecvTime(0),
      mSessionId(),
      mSessionKey(),
      mShutdownSslFlag(false),
      mSslShutdownInProgressFlag(false),
      mCurrentSessionExpirationTime(0),
      mSessionExpirationTime(0),
      mIStream(),
      mWOStream(),
      mList(0),
      mListIt(),
      mConnectCount(0),
      mDeleteFlag(false),
      mFinishRecursionCount(0),
      mDeletedFlagPtr(0),
      mOpResponseTimeoutSec(sOpResponseTimeoutSec),
      mTraceRequestResponseFlag(sTraceRequestResponseFlag)
{
    QCASSERT(IsMutexOwner(GetMutexPtr()));
    SET_HANDLER(this, &RemoteSyncSM::HandleEvent);
    sRemoteSyncCount++;
}

RemoteSyncSM::~RemoteSyncSM()
{
    if (! IsMutexOwner(GetMutexPtr()) ||
            sRemoteSyncCount <= 0 ||
            mRecursionCount < 0 ||
            (mRecursionCount != 0 && ! mDeletedFlagPtr) ||
            mFinishRecursionCount != 0 ||
            mNetConnection ||
            ! mDispatchedOps.empty() ||
            mList ||
            ! mDeleteFlag) {
        die("invalid remote sync destructor invocation");
    }
    if (mDeletedFlagPtr) {
        *mDeletedFlagPtr = true;
        mDeletedFlagPtr = 0;
    }
    sRemoteSyncCount--;
}

bool
RemoteSyncSM::Connect()
{
    QCASSERT(! mNetConnection && IsMutexOwner(GetMutexPtr()) && ! mDeleteFlag);

    mConnectCount++;
    KFS_LOG_STREAM_DEBUG <<
        "trying to connect to: " << mLocation <<
    KFS_LOG_EOM;

    if (! GetNetManager().IsRunning()) {
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

    mCurrentSessionExpirationTime = mSessionExpirationTime;
    mNetConnection.reset(new NetConnection(sock, this));
    mNetConnection->SetDoingNonblockingConnect();
    mNetConnection->SetMaxReadAhead(kMaxCmdHeaderLength);
    QCASSERT(sAuthPtr);
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
    mLastRecvTime = GetNetManager().Now();

    // If there is no activity on this socket, we want
    // to be notified, so that we can close connection.
    mNetConnection->SetInactivityTimeout(mOpResponseTimeoutSec);
    // Add this to the poll vector
    GetNetManager().AddConnection(mNetConnection);

    return true;
}

void
RemoteSyncSM::Enqueue(KfsOp* op)
{
    QCASSERT(op && ! mDeleteFlag);
    if (IsClientThread()) {
        DispatchEnqueue(*this, *op);
    } else {
        QCASSERT(! gClientManager.GetCurrentClientThreadPtr());
        EnqueueSelf(op);
    }
}

bool
RemoteSyncSM::EnqueueSelf(KfsOp* op)
{
    QCASSERT(IsMutexOwner(GetMutexPtr()) && ! mDeleteFlag);
    QCStDeleteNotifier const deleteNotifier(mDeletedFlagPtr);

    if (0 < mFinishRecursionCount) {
        op->status    = -EHOSTUNREACH;
        op->statusMsg = "remote sync finish in flight";
        SubmitOpResponse(op);
        return false;
    }
    if (mNetConnection && ! mNetConnection->IsGood()) {
        KFS_LOG_STREAM_INFO <<
            "lost connection to peer " << mLocation <<
            " failed; failing ops" <<
        KFS_LOG_EOM;
        mNetConnection->Close();
        mNetConnection.reset();
    }
    op->seq = NextSeqnum();
    const time_t now                             = GetNetManager().Now();
    const int64_t kSessionUpdateResolutionSec    = LEASE_INTERVAL_SECS / 2;
    const int64_t kSessionUpdateStartIntervalSec = 30 * 60;
    if (mNetConnection && ! mSessionId.empty()) {
        if (mDispatchedOps.empty() &&
                ((mCurrentSessionExpirationTime <
                    now + kSessionUpdateStartIntervalSec &&
                mCurrentSessionExpirationTime +
                    kSessionUpdateResolutionSec < mSessionExpirationTime) ||
                mCurrentSessionExpirationTime <= now) &&
                now < mSessionExpirationTime) {
            KFS_LOG_STREAM_INFO <<
                "peer: " << mLocation <<
                " session is about to expiere, forcing re-connect" <<
            KFS_LOG_EOM;
            mNetConnection->Close();
            mNetConnection.reset();
            mCurrentSessionExpirationTime = mSessionExpirationTime;
        }
        if (mCurrentSessionExpirationTime <= now) {
            KFS_LOG_STREAM_INFO <<
                "peer: " << mLocation <<
                " current session has expired" <<
                " ops in flight: " << mDispatchedOps.size() <<
            KFS_LOG_EOM;
            op->status    = -EPERM;
            op->statusMsg = "current session is no longer valid";
            SubmitOpResponse(op);
            return false;
        }
    }
    if (! mNetConnection && ! Connect()) {
        KFS_LOG_STREAM_INFO <<
            "connection to peer " << mLocation <<
            " failed; failing ops" <<
        KFS_LOG_EOM;
        if (! mDispatchedOps.insert(make_pair(op->seq, op)).second) {
            die("duplicate seq. number");
        }
        FailAllOps();
        return false;
    }
    if (mDispatchedOps.empty()) {
        mLastRecvTime = now;
    }
    KFS_LOG_STREAM_DEBUG <<
        "forwarding to " << mLocation <<
        " " << op->Show() <<
    KFS_LOG_EOM;
    IOBuffer& buf         = mNetConnection->GetOutBuffer();
    const int headerStart = buf.BytesConsumable();
    op->Request(mWOStream.Set(buf), buf);
    mWOStream.Reset();
    const int headerEnd   = buf.BytesConsumable();
    if (mTraceRequestResponseFlag) {
        IOBuffer::IStream is(buf, buf.BytesConsumable());
        is.ignore(headerStart);
        string line;
        KFS_LOG_STREAM_DEBUG << reinterpret_cast<const void*>(this) <<
            " send to: " << mLocation <<
        KFS_LOG_EOM;
        while (getline(is, line)) {
            KFS_LOG_STREAM_DEBUG << reinterpret_cast<const void*>(this) <<
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
    if (deleteNotifier.IsDeleted()) {
        return false;
    }
    UpdateRecvTimeout();
    if (mRecursionCount <= 0 && mNetConnection) {
        if (IsClientThread()) {
            if (headerStart <= 0 && 0 < headerEnd &&
                    headerEnd == buf.BytesConsumable()) {
                mNetConnection->Flush(); // Schedule write.
            }
        } else {
            mNetConnection->StartFlush();
        }
    }
    return (! deleteNotifier.IsDeleted() &&
        mNetConnection && mNetConnection->IsGood());
}

int
RemoteSyncSM::HandleEvent(int code, void *data)
{
    QCStDeleteNotifier const deleteNotifier(mDeletedFlagPtr);
    const char*              reason = "inactivity timeout";

    mRecursionCount++;
    QCASSERT(mRecursionCount > 0);
    switch (code) {
    case EVENT_NET_READ: {
        if (! mNetConnection) {
            break;
        }
        mLastRecvTime = GetNetManager().Now();
        // We read something from the network.  Run the RPC that
        // came in if we got all the data for the RPC
        IOBuffer& iobuf  = mNetConnection->GetInBuffer();
        int       msgLen = 0;
        QCASSERT(&iobuf == data);
        while ((mReplyNumBytes > 0 || IsMsgAvail(&iobuf, &msgLen)) &&
                HandleResponse(iobuf, msgLen)) {
            if (deleteNotifier.IsDeleted()) {
                // Unwind if deleted from SubmitResponse() invocation.
                return 0;
            }
        }
        UpdateRecvTimeout();
        break;
    }

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
        mReplyNumBytes = 0;
        mReplySeqNum   = -1;
        break;

    default:
        die("RemoteSyncSM: unexpected event");
        break;
    }
    QCASSERT(mRecursionCount > 0);
    if (mRecursionCount <= 1) {
        const bool connectedFlag = mNetConnection && mNetConnection->IsGood();
        if (connectedFlag) {
            mNetConnection->StartFlush();
            if (deleteNotifier.IsDeleted()) {
                return 0; // Unwind.
            }
        }
        if (! connectedFlag || ! mNetConnection || ! mNetConnection->IsGood()) {
            // we are done...
            mRecursionCount--;
            StMutexLocker lock(*this);
            Finish();
            return 0;
        }
    }
    mRecursionCount--;
    return 0;

}

void
RemoteSyncSM::ResetConnection()
{
    if (mNetConnection) {
        mNetConnection->Close();
        mNetConnection->GetInBuffer().Clear();
    }
    HandleEvent(EVENT_NET_ERROR, 0);
}

bool
RemoteSyncSM::HandleResponse(IOBuffer& iobuf, int msgLen)
{
    DispatchedOps::iterator it     = mDispatchedOps.end();
    int                     nAvail = iobuf.BytesConsumable();

    if (mReplyNumBytes <= 0) {
        QCASSERT(msgLen >= 0 && msgLen <= nAvail);
        if (mTraceRequestResponseFlag) {
            IOBuffer::IStream is(iobuf, msgLen);
            string            line;
            while (getline(is, line)) {
                KFS_LOG_STREAM_DEBUG << reinterpret_cast<void*>(this) <<
                    " " << mLocation << " response: " << line <<
                KFS_LOG_EOM;
            }
        }
        Properties prop;
        const char separator(':');
        prop.loadProperties(mIStream.Set(iobuf, msgLen), separator);
        mIStream.Reset();
        iobuf.Consume(msgLen);
        nAvail -= msgLen;
        mReplySeqNum = prop.getValue("Cseq", (kfsSeq_t)-1);
        if (mReplySeqNum < 0) {
            KFS_LOG_STREAM_ERROR <<
                "invalid or missing Cseq header: " << mReplySeqNum <<
                ", resetting connection" <<
            KFS_LOG_EOM;
            ResetConnection();
            return -1;
        }
        mReplyNumBytes = prop.getValue("Content-length", 0);
        it = mDispatchedOps.find(mReplySeqNum);
        KfsOp* const op = it != mDispatchedOps.end() ? it->second : 0;
        if (op) {
            const int status = prop.getValue("Status", -1);
            if (status < 0) {
                op->status    = -KfsToSysErrno(-status);
                op->statusMsg = prop.getValue("Status-message", string());
            } else {
                op->status = status;
            }
            if (! op->ParseResponse(prop, iobuf) && 0 <= status) {
                KFS_LOG_STREAM_ERROR <<
                    "invalid response:"
                    " seq: " << op->seq <<
                    " "      << op->Show() <<
                KFS_LOG_EOM;
                ResetConnection();
                return false;
            }
        }
    }
    // if we don't have all the data for the write, hold on...
    if (nAvail < mReplyNumBytes) {
        // the data isn't here yet...wait...
        if (mNetConnection) {
            mNetConnection->SetMaxReadAhead(mReplyNumBytes - nAvail);
        }
        return false;
    }
    // now, we got everything...
    if (mNetConnection) {
        mNetConnection->SetMaxReadAhead(kMaxCmdHeaderLength);
    }
    // find the matching op
    if (it == mDispatchedOps.end()) {
        it = mDispatchedOps.find(mReplySeqNum);
    }
    if (it != mDispatchedOps.end()) {
        KfsOp* const op = it->second;
        if (! op) {
            die("invalid null op");
            return false;
        }
        if (! op->GetResponseContent(iobuf, mReplyNumBytes)) {
            KFS_LOG_STREAM_ERROR <<
                "invalid response content:"
                " length: " << mReplySeqNum <<
                " " << op->Show() <<
            KFS_LOG_EOM;
            ResetConnection();
            return false;
        }
        mReplyNumBytes = 0;
        StMutexLocker lock(*this);
        // If finish is pending, then FinishSelf will fail the op.
        if (! IsFinishPending()) {
            mDispatchedOps.erase(it);
            SubmitOpResponse(op);
        }
    } else {
        KFS_LOG_STREAM_DEBUG <<
            "discarding a reply for unknown seq: " << mReplySeqNum <<
            " content length: "                    << mReplyNumBytes <<
        KFS_LOG_EOM;
        iobuf.Consume(mReplyNumBytes);
        mReplyNumBytes = 0;
    }
    return true;
}

// Helper functor that fails an op with an error code.
class OpFailer
{
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
private:
    const int errCode;
};

void
RemoteSyncSM::FailAllOps()
{
    QCASSERT(IsMutexOwner(GetMutexPtr()));

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
}

void
RemoteSyncSM::Finish()
{
    if (IsClientThread()) {
        DispatchFinish(*this);
    } else {
        QCASSERT(! gClientManager.GetCurrentClientThreadPtr());
        FinishSelf();
    }
}

void
RemoteSyncSM::FinishSelf()
{
    mFinishRecursionCount++;
    if (mNetConnection) {
        mNetConnection->Close();
        mNetConnection.reset();
    }
    FailAllOps();
    RemoveFromList();
    mFinishRecursionCount--;
    if (mDeleteFlag && mFinishRecursionCount <= 0) {
        delete this;
    }
}

bool
RemoteSyncSM::IsAuthEnabled()
{
    return (sAuthPtr && sAuthPtr->IsEnabled());
}

inline static int64_t
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
    QCASSERT(IsMutexOwner(GetMutexPtr()));

    if (sessionTokenLen <= 0 || sessionKeyLen <= 0) {
        err    = -EINVAL;
        errMsg = "invalid session and/or key length";
        return false;
    }
    if (mSessionId.empty() && 0 < mConnectCount) {
        err    = -EINVAL;
        errMsg = "sync replication: no current session";
        return false;
    }
    err = 0;
    errMsg.clear();
    const time_t now = TimeNow();
    if (now + LEASE_INTERVAL_SECS <= mSessionExpirationTime) {
        return false;
    }
    const int64_t expTime = GetExpirationTime(
        sessionTokenPtr, sessionTokenLen, err, errMsg);
    if (err) {
        return false;
    }
    if (expTime < mSessionExpirationTime) {
        return true;
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

void
RemoteSyncSM::ScheduleDelete()
{
    QCASSERT(IsMutexOwner(GetMutexPtr()) && ! mDeleteFlag);
    mDeleteFlag = true;
    Finish();
}

class RemoteSyncSMCleanupFunctor
{
public:
    void operator()(
        RemoteSyncSM* inSyncSMPtr)
        { inSyncSMPtr->ScheduleDelete(); }
};

inline static ClientThread*
GetClientThread(bool forceUseClientThreadFlag)
{
    ClientThread* const cliThread = gClientManager.GetCurrentClientThreadPtr();
    if (cliThread || ! forceUseClientThreadFlag) {
        return cliThread;
    }
    return gClientManager.GetNextClientThreadPtr();
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
    string&               errMsg,
    bool                  connectFlag,
    bool                  forceUseClientThreadFlag)
{
    QCASSERT(! connectFlag || ! forceUseClientThreadFlag);
    err = 0;
    errMsg.clear();
    SMPtr peer;
    if (sessionKeyLen <= 0) {
        peer.reset(new RemoteSyncSM(
            location, GetClientThread(forceUseClientThreadFlag)),
            RemoteSyncSMCleanupFunctor());
    } else if (sessionTokenLen <= 0) {
        err    = -EINVAL;
        errMsg = "invalid session token length";
    } else {
        const time_t expTime = GetExpirationTime(
            sessionTokenPtr, sessionTokenLen, err, errMsg);
        if (! err) {
            if (expTime < TimeNow()) {
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
                    peer.reset(new RemoteSyncSM(
                        location, GetClientThread(forceUseClientThreadFlag)),
                        RemoteSyncSMCleanupFunctor());
                    peer->SetSessionKey(sessionTokenPtr, sessionTokenLen, key);
                    peer->SetShutdownSslFlag(shutdownSslFlag);
                    peer->mSessionExpirationTime = expTime;
                }
            }
        }
    }
    if (connectFlag && peer && ! peer->Connect()) {
        errMsg = "connection failure";
        err    = -EHOSTUNREACH;
        peer.reset();
    }
    if (! peer) {
        KFS_LOG_STREAM_ERROR <<
            "failed to forward: " << errMsg << " status: " << err <<
        KFS_LOG_EOM;
    }
    return peer;
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

RemoteSyncSM::SMPtr
RemoteSyncSM::FindServer(
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
        errMsg,
        connectFlag
    );
    if (peer) {
        return remoteSyncers.PutInList(*peer);
    }
    return res;
}

}
