//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/04/21
// Author: Mike Ovsiannikov
//
// Copyright 2015 Quantcast Corp.
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
// Transaction log replication reciver.
//
//
//----------------------------------------------------------------------------

#include "AuthContext.h"
#include "MetaRequest.h"
#include "util.h"

#include "common/kfstypes.h"
#include "common/MsgLogger.h"
#include "common/RequestParser.h"

#include "kfsio/NetManager.h"
#include "kfsio/NetConnection.h"
#include "kfsio/SslFilter.h"
#include "kfsio/Acceptor.h"
#include "kfsio/checksum.h"

#include "qcdio/QCUtils.h"
#include "qcdio/qcdebug.h"

#include <string>
#include <algorithm>
#include <iomanip>

#include <time.h>

namespace KFS
{
using std::string;
using std::max;
using std::hex;

class LogReceiver
{
    class Impl;
};

class LogReceiver::Impl : public IAcceptorOwner
{
private:
    class Connection;
public:
    typedef QCDLList<Connection> List;

    Impl()
        : mReauthTimeout(20),
          mMaxReadAhead(MAX_RPC_HEADER_LEN),
          mTimeout(30),
          mConnectionCount(0),
          mAuthContext()
        { List::Init(mConnectionsHeadPtr); }
    virtual KfsCallbackObj* CreateKfsCallbackObj(
        NetConnectionPtr& inConnectionPtr);
    void SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters)
    {
    }
    void Shutdown();
    int GetReauthTimeout() const
        { return mReauthTimeout; }
    int GetMaxReadAhead() const
        { return mMaxReadAhead; }
    int GetTimeout() const
        { return mTimeout; }
    AuthContext& GetAuthContext()
        { return mAuthContext; }
    void New(
        Connection& inConnection);
    void Done(
        Connection& inConnection);
private:
    int         mReauthTimeout;
    int         mMaxReadAhead;
    int         mTimeout;
    int         mConnectionCount;
    AuthContext mAuthContext;
    Connection* mConnectionsHeadPtr[1];
};

class LogReceiver::Impl::Connection :
    public KfsCallbackObj,
    public SslFilterVerifyPeer
{
public:
    typedef Impl::List List;

    Connection(
        Impl&                   inImpl,
        const NetConnectionPtr& inConnectionPtr)
        : KfsCallbackObj(),
          SslFilterVerifyPeer(),
          mImpl(inImpl),
          mAuthName(),
          mSessionExpirationTime(0),
          mConnectionPtr(inConnectionPtr),
          mAuthenticateOpPtr(0),
          mAuthCount(0),
          mAuthCtxUpdateCount(0),
          mRecursionCount(0),
          mBlockLength(-1),
          mPendingOpsCount(0),
          mBlockChecksum(0),
          mBlockSeq(-1),
          mDownFlag(false),
          mAuthPendingResponsesHeadPtr(0),
          mAuthPendingResponsesTailPtr(0),
          mIStream(),
          mOstream()
    {
        List::Init(*this);
        SET_HANDLER(this, &Connection::HandleEvent);
        if (! mConnectionPtr || ! mConnectionPtr->IsGood()) {
            panic("LogReceiver::Impl::Connection; invalid connection poiner");
        }
        mSessionExpirationTime = TimeNow() - int64_t(60) * 60 * 24 * 365 * 10;
        mConnectionPtr->SetInactivityTimeout(mImpl.GetTimeout());
        mImpl.New(*this);
    }
    ~Connection()
    {
        if (mRecursionCount != 0 || mConnectionPtr->IsGood() ||
                mPendingOpsCount != 0) {
            panic("LogReceiver::~Impl::Connection invalid invocation");
        }
        MetaRequest* thePtr = mAuthPendingResponsesHeadPtr;
        mAuthPendingResponsesHeadPtr = 0;
        mAuthPendingResponsesTailPtr = 0;
        while (thePtr) {
            MetaRequest& theReq = *thePtr;
            thePtr = theReq.next;
            MetaRequest::Release(&theReq);
        }
        MetaRequest::Release(mAuthenticateOpPtr);
        mImpl.Done(*this);
        mRecursionCount  = 0xDEAD;
        mPendingOpsCount = 0xDEAD;
    }
    virtual bool Verify(
        string&       ioFilterAuthName,
        bool          inPreverifyOkFlag,
        int           inCurCertDepth,
        const string& inPeerName,
        int64_t       inEndTime,
        bool          inEndTimeValidFlag)
    {
        KFS_LOG_STREAM_DEBUG << GetPeerName() <<
            " log auth. verify:" <<
            " name: "           << inPeerName <<
            " prev: "           << ioFilterAuthName <<
            " preverify: "      << inPreverifyOkFlag <<
            " depth: "          << inCurCertDepth <<
            " end time: +"      << (inEndTime - time(0)) <<
            " end time valid: " << inEndTimeValidFlag <<
        KFS_LOG_EOM;
        // Do no allow to renegotiate and change the name.
        string theAuthName = inPeerName;
        if (! inPreverifyOkFlag ||
                (inCurCertDepth == 0 &&
                ((GetAuthContext().HasUserAndGroup() ?
                    GetAuthContext().GetUid(theAuthName) == kKfsUserNone :
                    ! GetAuthContext().RemapAndValidate(theAuthName)) ||
                (! mAuthName.empty() && theAuthName != mAuthName)))) {
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
                " log receiver authentication failure:"
                " peer: "  << inPeerName <<
                " name: "  << theAuthName <<
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
            mAuthName        = theAuthName;
            if (inEndTimeValidFlag && inEndTime < mSessionExpirationTime) {
                mSessionExpirationTime = inEndTime;
            }
        }
        return true;
    }
    int HandleEvent(
        int   inType,
        void* inDataPtr)
    {
        mRecursionCount++;
        QCASSERT(0 < mRecursionCount);
        switch (inType) {
            case EVENT_NET_READ:
                QCASSERT(&mConnectionPtr->GetInBuffer() == inDataPtr);
                HandleRead();
                break;
            case EVENT_NET_WROTE:
                if (mAuthenticateOpPtr) {
                    HandleAuthWrite();
                }
                break;
            case EVENT_CMD_DONE:
                if (! inDataPtr) {
                    panic("invalid null command completion");
                }
                HandleCmdDone(*reinterpret_cast<MetaRequest*>(inDataPtr));
                break;
            case EVENT_NET_ERROR:
                if (HandleSslShutdown()) {
                    break;
                }
                Error();
                break;
            case EVENT_TIMEOUT:
                Error("connection timed out");
                break;
            default:
                panic("LogReceiver: unexpected event");
                break;
        }
        mRecursionCount--;
        QCASSERT(0 <= mRecursionCount);
        if (mRecursionCount <= 0) {
            if (mConnectionPtr->IsGood()) {
                mConnectionPtr->StartFlush();
            } else if (! mDownFlag) {
                Error();
            }
            if (mDownFlag && mPendingOpsCount <= 0) {
                delete this;
            }
        }
        return 0;
    }
private:
    enum {
        kAckReauthFlag = 1
    };
    Impl&                  mImpl;
    string                 mAuthName;
    int64_t                mSessionExpirationTime;
    NetConnectionPtr const mConnectionPtr;
    MetaAuthenticate*      mAuthenticateOpPtr;
    int64_t                mAuthCount;
    uint64_t               mAuthCtxUpdateCount;
    int                    mRecursionCount;
    int                    mBlockLength;
    int                    mPendingOpsCount;
    int32_t                mBlockChecksum;
    int64_t                mBlockSeq;
    bool                   mDownFlag;
    MetaRequest*           mAuthPendingResponsesHeadPtr;
    MetaRequest*           mAuthPendingResponsesTailPtr;
    IOBuffer::IStream      mIStream;
    IOBuffer::WOStream     mOstream;
    Connection*            mPrevPtr[1];
    Connection*            mNextPtr[1];
    char                   mParseBuffer[MAX_RPC_HEADER_LEN];

    friend class QCDLListOp<Connection>;

    string GetPeerName()
        { return mConnectionPtr->GetPeerName(); }
    time_t TimeNow()
        { return mConnectionPtr->TimeNow(); }
    AuthContext& GetAuthContext()
        { return mImpl.GetAuthContext(); }
    int Authenticate(
        IOBuffer& inBuffer)
    {
        if (! mAuthenticateOpPtr) {
            return 0;
        }
        if (mAuthenticateOpPtr->doneFlag) {
            if (mConnectionPtr->GetFilter()) {
                HandleEvent(EVENT_NET_WROTE, &mConnectionPtr->GetOutBuffer());
            }
            return 0;
        }
        if (mAuthenticateOpPtr->contentBufPos <= 0) {
            GetAuthContext().Validate(*mAuthenticateOpPtr);
        }
        const int theRem = mAuthenticateOpPtr->Read(inBuffer);
        if (0 < theRem) {
            mConnectionPtr->SetMaxReadAhead(theRem);
            return theRem;
        }
        if (! inBuffer.IsEmpty() && mAuthenticateOpPtr->status == 0) {
            mAuthenticateOpPtr->status    = -EINVAL;
            mAuthenticateOpPtr->statusMsg = "out of order data received";
        }
        GetAuthContext().Authenticate(*mAuthenticateOpPtr, this, 0);
        if (mAuthenticateOpPtr->status == 0) {
            if (mAuthName.empty()) {
                mAuthName = mAuthenticateOpPtr->authName;
            } else if (! mAuthenticateOpPtr->authName.empty() &&
                    mAuthName != mAuthenticateOpPtr->authName) {
                mAuthenticateOpPtr->status    = -EINVAL;
                mAuthenticateOpPtr->statusMsg = "authenticated name mismatch";
            } else if (! mAuthenticateOpPtr->filter &&
                    mConnectionPtr->GetFilter()) {
                // An attempt to downgrade to clear text connection.
                mAuthenticateOpPtr->status    = -EINVAL;
                mAuthenticateOpPtr->statusMsg =
                    "clear text communication not allowed";
            }
        }
        mAuthenticateOpPtr->doneFlag = true;
        mAuthCtxUpdateCount = GetAuthContext().GetUpdateCount();
        KFS_LOG_STREAM(mAuthenticateOpPtr->status == 0 ?
            MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
            GetPeerName()           << " log receiver authentication"
            " type: "               << mAuthenticateOpPtr->responseAuthType <<
            " name: "               << mAuthenticateOpPtr->authName <<
            " filter: "             <<
                reinterpret_cast<const void*>(mAuthenticateOpPtr->filter) <<
            " session expires in: " <<
                (mAuthenticateOpPtr->sessionExpirationTime - TimeNow()) <<
            " response length: "    << mAuthenticateOpPtr->responseContentLen <<
            " msg: "                << mAuthenticateOpPtr->statusMsg <<
        KFS_LOG_EOM;
        mConnectionPtr->SetMaxReadAhead(mImpl.GetMaxReadAhead());
        SendResponse(*mAuthenticateOpPtr);
        return (mDownFlag ? -1 : 0);
    }
    void HandleAuthWrite()
    {
        if (! mAuthenticateOpPtr) {
            return;
        }
        if (mConnectionPtr->IsWriteReady()) {
            return;
        }
        if (mAuthenticateOpPtr->status != 0 ||
                mConnectionPtr->HasPendingRead() ||
                mConnectionPtr->IsReadPending()) {
            const string theMsg = mAuthenticateOpPtr->statusMsg;
            Error(theMsg.empty() ?
                (mConnectionPtr->HasPendingRead() ?
                    "out of order data received" :
                    "authentication error") :
                theMsg.c_str()
            );
            return;
        }
        if (mConnectionPtr->GetFilter()) {
            if (! mAuthenticateOpPtr->filter) {
                Error("no clear text communication allowed");
            }
            // Wait for [ssl] shutdown with the current filter to complete.
            return;
        }
        if (mAuthenticateOpPtr->filter) {
            NetConnection::Filter* const theFilterPtr =
                mAuthenticateOpPtr->filter;
            mAuthenticateOpPtr->filter = 0;
            string    theErrMsg;
            const int theErr = mConnectionPtr->SetFilter(
                theFilterPtr, &theErrMsg);
            if (theErr) {
                if (theErrMsg.empty()) {
                    theErrMsg = QCUtils::SysError(theErr < 0 ? -theErr : theErr);
                }
                Error(theErrMsg.c_str());
                return;
            }
        }
        mSessionExpirationTime = mAuthenticateOpPtr->sessionExpirationTime;
        MetaRequest::Release(mAuthenticateOpPtr);
        mAuthenticateOpPtr  = 0;
        KFS_LOG_STREAM_INFO << GetPeerName() <<
            (0 < mAuthCount ? " re-" : " ") <<
            "authentication complete:"
            " session expires in: " <<
                (mSessionExpirationTime - TimeNow()) << " sec." <<
        KFS_LOG_EOM;
        mAuthCount++;
        MetaRequest* thePtr = mAuthPendingResponsesHeadPtr;
        mAuthPendingResponsesHeadPtr = 0;
        mAuthPendingResponsesTailPtr = 0;
        while (thePtr && ! mDownFlag) {
            MetaRequest& theReq = *thePtr;
            thePtr = theReq.next;
            SendResponse(theReq);
            MetaRequest::Release(&theReq);
        }
    }
    bool HandleSslShutdown()
    {
        NetConnection::Filter* theFilterPtr;
        if (mDownFlag ||
                ! mAuthenticateOpPtr ||
                ! mConnectionPtr->IsGood() ||
                ! (theFilterPtr = mConnectionPtr->GetFilter()) ||
                ! theFilterPtr->IsShutdownReceived()) {
            return false;
        }
        // Do not allow to shutdown filter with data in flight.
        if (mConnectionPtr->GetInBuffer().IsEmpty() &&
                mConnectionPtr->GetOutBuffer().IsEmpty()) {
            // Ssl shutdown from the other side.
            if (mConnectionPtr->Shutdown() != 0) {
                return false;
            }
            KFS_LOG_STREAM_DEBUG << GetPeerName() <<
                " log receiver: shutdown filter: " <<
                    reinterpret_cast<const void*>(
                        mConnectionPtr->GetFilter()) <<
            KFS_LOG_EOM;
            if (mConnectionPtr->GetFilter()) {
                return false;
            }
            HandleAuthWrite();
            return (! mDownFlag);
        }
        KFS_LOG_STREAM_ERROR << GetPeerName() <<
            " log receiver: "
            " invalid filter (ssl) shutdown: "
            " error: " << mConnectionPtr->GetErrorMsg() <<
            " read: "  << mConnectionPtr->GetNumBytesToRead() <<
            " write: " << mConnectionPtr->GetNumBytesToWrite() <<
        KFS_LOG_EOM;
        return false;
    }
    void HandleRead()
    {
        IOBuffer& theBuf = mConnectionPtr->GetInBuffer();
        if (mAuthenticateOpPtr) {
            Authenticate(theBuf);
            if (mAuthenticateOpPtr) {
                return;
            }
        }
        bool theMsgAvailableFlag;
        int  theMsgLen = 0;
        while ((theMsgAvailableFlag = IsMsgAvail(&theBuf, &theMsgLen))) {
            const int theRet = HandleMsg(theBuf, theMsgLen);
            if (theRet < 0) {
                theBuf.Clear();
                Error(mAuthenticateOpPtr ?
                    (mAuthenticateOpPtr->statusMsg.empty() ?
                        "invalid authenticate message" :
                        mAuthenticateOpPtr->statusMsg.c_str()) :
                    "request parse error"
                );
                return;
            }
            if (0 < theRet || mAuthenticateOpPtr || mDownFlag) {
                return; // Need more data, or down
            }
            theMsgLen = 0;
        }
    }
    void HandleCmdDone(
        MetaRequest& inReq)
    {
        if (mPendingOpsCount <= 0) {
            panic("invalid outstanding ops count");
            return;
        }
        if (inReq.next) {
            panic("invalid request next field");
        }
        mPendingOpsCount--;
        if (mAuthenticateOpPtr && ! mDownFlag) {
            if (mAuthPendingResponsesTailPtr) {
                mAuthPendingResponsesTailPtr->next = &inReq;
            } else {
                mAuthPendingResponsesHeadPtr = &inReq;
            }
            mAuthPendingResponsesTailPtr = &inReq;
        }
        SendResponse(inReq);
        MetaRequest::Release(&inReq);
    }
    void SendResponse(
        MetaRequest& inReq)
    {
        if (mDownFlag) {
            return;
        }
        IOBuffer& theBuf = mConnectionPtr->GetOutBuffer();
        ReqOstream theStream(mOstream.Set(theBuf));
        inReq.response(theStream, theBuf);
        mOstream.Reset();
        if (mRecursionCount <= 0) {
            mConnectionPtr->StartFlush();
        }
    }
    int HandleMsg(
        IOBuffer& inBuffer,
        int       inMsgLen)
    {
        const int kSeparatorLen = 4;
        const int kPrefixLen    = 2;
        if (kSeparatorLen + kPrefixLen < inMsgLen) {
            if (IsAuthError()) {
                return -1;
            }
            int               theLen       = inMsgLen - kSeparatorLen;
            const char* const theHeaderPtr = inBuffer.CopyOutOrGetBufPtr(
                mParseBuffer, theLen);
            QCRTASSERT(inMsgLen - kSeparatorLen == theLen);
            const char*       thePtr    = theHeaderPtr;
            const char* const theEndPtr = thePtr + inMsgLen - kSeparatorLen;
            if ('l' == (*thePtr++ & 0xFF) && ':' == (*thePtr++ & 0xFF) &&
                    HexIntParser::Parse(
                        thePtr, theEndPtr - thePtr, mBlockLength) &&
                    HexIntParser::Parse(
                        thePtr, theEndPtr - thePtr, mBlockChecksum)) {
                if (mBlockLength < 0) {
                    Error("invalid negative block lenght");
                    return -1;
                }
                inBuffer.Consume(inMsgLen);
                return ReceiveBlock(inBuffer);
            }
        }
        MetaRequest* theReqPtr = 0;
        if (ParseLogRecvCommand(
                    inBuffer, inMsgLen, &theReqPtr, mParseBuffer) ||
                ! theReqPtr ||
                META_AUTHENTICATE != theReqPtr->op) {
            MetaRequest::Release(theReqPtr);
            const string thePrefix = GetPeerName() + " invalid request: ";
            MsgLogLines(
                MsgLogger::kLogLevelERROR,
                thePrefix.c_str(),
                inBuffer,
                inMsgLen
            );
            return -1;
        }
        inBuffer.Consume(inMsgLen);
        if (META_AUTHENTICATE == theReqPtr->op) {
            mAuthenticateOpPtr = static_cast<MetaAuthenticate*>(theReqPtr);
            return Authenticate(inBuffer);
        }
        if (IsAuthError()) {
            MetaRequest::Release(theReqPtr);
            return -1;
        }
        mPendingOpsCount++;
        submit_request(theReqPtr);
        return 0;
    }
    int ReceiveBlock(
        IOBuffer& inBuffer)
    {
        if (mBlockLength < 0) {
            return -1;
        }
        const int theRem = mBlockLength - inBuffer.BytesConsumable();
        if (0 < theRem) {
            mConnectionPtr->SetMaxReadAhead(
                max(theRem, mImpl.GetMaxReadAhead()));
            return theRem;
        }
        const uint32_t theChecksum = ComputeBlockChecksum(
            &inBuffer, mBlockLength, kKfsNullChecksum);
        if (theChecksum != mBlockChecksum) {
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
                " received block checksum: " << theChecksum <<
                " expected: "                << mBlockChecksum <<
                " length: "                  << mBlockLength <<
            KFS_LOG_EOM;
            Error("block checksum mimatch");
            return -1;
        }
        int         theMaxHdrLen    = (int)sizeof(seq_t) * 2 + 1 + 16;
        QCASSERT(theMaxHdrLen <= MAX_RPC_HEADER_LEN);
        const char* theStartPtr     = inBuffer.CopyOutOrGetBufPtr(
                mParseBuffer, theMaxHdrLen);
        const char* const theEndPtr = theStartPtr + theMaxHdrLen;
        int64_t     theBlockSeq = -1;
        const char* thePtr      = theStartPtr;
        if (! HexIntParser::Parse(
                thePtr, theEndPtr - thePtr, theBlockSeq) ||
                theBlockSeq < 0 ||
                (0 <= mBlockSeq && theBlockSeq != mBlockSeq + 1)) {
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
                " invalid block sequence: " << theBlockSeq <<
                " last    : "               << mBlockSeq <<
                " length: "                 << mBlockLength <<
            KFS_LOG_EOM;
            MsgLogLines(MsgLogger::kLogLevelERROR,
                "invalid block sequence: ", inBuffer, mBlockLength);
            Error("invalid block sequence");
            return -1;
        }
        while (thePtr < theEndPtr && (*thePtr & 0xFF) <= ' ') {
            thePtr++;
        }
        if (theEndPtr <= thePtr && theMaxHdrLen < mBlockLength) {
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
                " invalid block header:" << theBlockSeq  <<
                " length: "              << mBlockLength <<
            KFS_LOG_EOM;
            MsgLogLines(MsgLogger::kLogLevelERROR,
                "invalid block header: ", inBuffer, mBlockLength);
            Error("invalid block header");
            return -1;
        }
        inBuffer.Consume((int)(thePtr - theStartPtr));
        mBlockSeq = theBlockSeq;
        SendAck();
        if (mDownFlag) {
            return -1;
        }
        return ProcessBlock(inBuffer);
    }
    void Error(
        const char* inMsgPtr = 0)
    {
        if (mDownFlag) {
            return;
        }
        KFS_LOG_STREAM_ERROR << GetPeerName() <<
            " error:" << (inMsgPtr ? inMsgPtr : "")  <<
            " closing connection"
            " last block: "   << mBlockSeq <<
            " socket error: " << mConnectionPtr->GetErrorMsg() <<
        KFS_LOG_EOM;
        mConnectionPtr->Close();
        mDownFlag = true;
    }
    void SendAck()
    {
        if (mAuthenticateOpPtr) {
            return;
        }
        bool theReAuthFlag;
        if (GetAuthContext().IsAuthRequired()) {
            uint64_t const theUpdateCount = GetAuthContext().GetUpdateCount();
            theReAuthFlag = theUpdateCount != mAuthCtxUpdateCount ||
                mSessionExpirationTime < TimeNow() + mImpl.GetReauthTimeout();
            if (theReAuthFlag) {
                KFS_LOG_STREAM_INFO << GetPeerName() <<
                    " requesting re-authentication:"
                    " update count: " << theUpdateCount <<
                    " / "             << mAuthCtxUpdateCount <<
                    " expires in: "   << (mSessionExpirationTime - TimeNow()) <<
                KFS_LOG_EOM;
            }
        } else {
            theReAuthFlag = false;
        }
        uint64_t theAckFlags = 0;
        if (theReAuthFlag) {
            theAckFlags |= kAckReauthFlag;
        }
        ReqOstream theStream(mOstream.Set(mConnectionPtr->GetOutBuffer()));
        theStream << hex <<
            "A: " << mBlockSeq << " " << theAckFlags << "\n";
        theStream.flush();
        if (mRecursionCount <= 0) {
            mConnectionPtr->StartFlush();
        }
    }
    int ProcessBlock(
        IOBuffer& inBuffer)
    {
        if (mBlockLength < 0) {
            return -1;
        }
        inBuffer.Consume(mBlockLength);
        mConnectionPtr->SetMaxReadAhead(mImpl.GetMaxReadAhead());
        return 0;
    }
    void MsgLogLines(
        MsgLogger::LogLevel inLogLevel,
        const char*         inPrefixPtr,
        IOBuffer&           inBuffer,
        int                 inBufLen,
        int                 inMaxLines = 64)
    {
        const char* const thePrefixPtr = inPrefixPtr ? inPrefixPtr : "";
        istream&          theStream    = mIStream.Set(inBuffer, inBufLen);
        int               theRemCnt    = inMaxLines;
        string            theLine;
        while (--theRemCnt >= 0 && getline(theStream, theLine)) {
            string::iterator theIt = theLine.end();
            if (theIt != theLine.begin() && *--theIt <= ' ') {
                theLine.erase(theIt);
            }
            KFS_LOG_STREAM(inLogLevel) <<
                thePrefixPtr << theLine <<
            KFS_LOG_EOM;
        }
        mIStream.Reset();
    }
    bool IsAuthError()
    {
        if (! GetAuthContext().IsAuthRequired()) {
            return false;
        }
        if (mAuthName.empty()) {
            Error("autentication required");
            return true;
        }
        return false;
    }
private:
    Connection(
        const Connection& inConnection);
    Connection& operator=(
        const Connection& inConnection);
};

    /* virtual */ KfsCallbackObj*
LogReceiver::Impl::CreateKfsCallbackObj(
    NetConnectionPtr& inConnectionPtr)
{
    if (inConnectionPtr && inConnectionPtr->IsGood()) {
        return new Connection(*this, inConnectionPtr);
    }
    return 0;
}

    void
LogReceiver::Impl::New(
    Connection& inConnection)
{
    mConnectionCount++;
    List::PushBack(mConnectionsHeadPtr, inConnection);
    if (mConnectionCount <= 0) {
        panic("LogReceiver::Impl::New: invalid connections count");
    }
}

    void
LogReceiver::Impl::Done(
    Connection& inConnection)
{
    if (mConnectionCount <= 0) {
        panic("LogReceiver::Impl::Done: invalid connections count");
    }
    List::Remove(mConnectionsHeadPtr, inConnection);
    mConnectionCount--;
}

    void
LogReceiver::Impl::Shutdown()
{
    List::Iterator theIt(mConnectionsHeadPtr);
    Connection* thePtr;
    while (0 < mConnectionCount && (thePtr = theIt.Next())) {
        thePtr->HandleEvent(EVENT_NET_ERROR, 0);
    }
}

} // namespace KFS
