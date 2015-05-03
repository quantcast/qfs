//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/04/27
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
// Transaction log replication transmitter.
//
//
//----------------------------------------------------------------------------

#include "AuthContext.h"
#include "MetaRequest.h"
#include "util.h"

#include "common/kfstypes.h"
#include "common/MsgLogger.h"
#include "common/Properties.h"

#include "kfsio/KfsCallbackObj.h"
#include "kfsio/NetConnection.h"
#include "kfsio/NetManager.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/ClientAuthContext.h"
#include "kfsio/event.h"
#include "kfsio/Checksum.h"

#include "qcdio/QCUtils.h"
#include "qcdio/qcdebug.h"

#include <string>
#include <algorithm>
#include <set>

namespace KFS
{
using std::string;
using std::max;
using std::multiset;

class Properties;

class LogTransmitter
{
public:
    LogTransmitter();
    ~LogTransmitter();
    void SetParameters(
        const char*       inParamPrefixPtr,
        const Properties& inParameters);
    int TransmitBlocl(
        const char* inBlockPtr,
        size_t      inBlockSize);
private:
    class Impl;

    Impl& mImpl;
private:
    LogTransmitter(
        const LogTransmitter& inTransmitter);
    LogTransmitter& operator=(
        const LogTransmitter& inTransmitter);
};

class LogTransmitter::Impl
{
private:
    class Transmitter;
public:
    typedef QCDLList<Transmitter> List;

    Impl(
        NetManager& inNetManager)
        : mNetManager(inNetManager),
          mRetryInterval(2),
          mMaxReadAhead(MAX_RPC_HEADER_LEN),
          mHeartbeatInterval(5),
          mLastBlockSeq(-1),
          mServers()
        { List::Init(mTransmittersPtr); }
    ~Impl()
        {}
    int SetParameters(
        const char*       inParamPrefixPtr,
        const Properties& inParameters);
    int TransmitBlocl(
        const char* inBlockPtr,
        size_t      inBlockSize)
    {
        return 0;
    }
    static seq_t RandomSeq()
    {
        seq_t theReq = 0;
        CryptoKeys::PseudoRand(&theReq, sizeof(theReq));
        return ((theReq < 0 ? -theReq : theReq) >> 1);
    }
    char* GetParseBufferPtr()
        { return mParseBuffer; }
    NetManager& GetNetManager()
        { return mNetManager; }
    int GetRetryInterval() const
        { return mRetryInterval; }
    int GetMaxReadAhead() const
        { return mMaxReadAhead; }
    int GetHeartbeatInterval() const
        { return mHeartbeatInterval; }
    seq_t GetLastBlockSeq() const
        { return mLastBlockSeq; }
    void SetLastBlockSeq(
        seq_t inSeq)
        { mLastBlockSeq = inSeq; }
    void Add(
        Transmitter& inTransmitter);
    void Remove(
        Transmitter& inTransmitter);
private:
    typedef Properties::String       String;
    typedef multiset<ServerLocation> Locations;

    NetManager&  mNetManager;
    int          mRetryInterval;
    int          mMaxReadAhead;
    int          mHeartbeatInterval;
    seq_t        mLastBlockSeq;
    String       mServers;
    Transmitter* mTransmittersPtr[1];
    char         mParseBuffer[MAX_RPC_HEADER_LEN];

private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

class LogTransmitter::Impl::Transmitter :
    public KfsCallbackObj,
    public ITimeout
{
public:
    typedef Impl::List List;

    Transmitter(
        Impl&                 inImpl,
        const ServerLocation& inServer)
        : KfsCallbackObj(),
          mImpl(inImpl),
          mServer(inServer),
          mConnectionPtr(),
          mAuthenticateOpPtr(0),
          mNextSeq(mImpl.RandomSeq()),
          mRecursionCount(0),
          mAuthContext(),
          mAuthType(0),
          mAuthRequestCtx(),
          mLastSentBlockSeq(-1),
          mAckBlockSeq(-1),
          mAckBlockFlags(0),
          mReplyProps(),
          mIstream(),
          mOstream(),
          mSleepingFlag(false)
    {
        SET_HANDLER(this, &Transmitter::HandleEvent);
        List::Init(*this);
        mTmpBuf[kTmpBufSize] = 0;
        mSeqBuf[kTmpBufSize] = 0;
    }
    ~Transmitter()
    {
        Transmitter::Shutdown();
        MetaRequest::Release(mAuthenticateOpPtr);
        if (mSleepingFlag) {
            mImpl.GetNetManager().UnRegisterTimeoutHandler(this);
        }
        mImpl.Remove(*this);
    }
    int SetParameters(
        ClientAuthContext* inAuthCtxPtr,
        const char*        inParamPrefixPtr,
        const Properties&  inParameters,
        string&            outErrMsg)
    {
        const bool kVerifyFlag = true;
        return mAuthContext.SetParameters(
            inParamPrefixPtr,
            inParameters,
            inAuthCtxPtr,
            &outErrMsg,
            kVerifyFlag
        );
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
                break;
            case EVENT_CMD_DONE:
                if (! inDataPtr) {
                    panic("invalid null command completion");
                    break;
                }
                HandleCmdDone(*reinterpret_cast<MetaRequest*>(inDataPtr));
                break;
            case EVENT_NET_ERROR:
                if (HandleSslShutdown()) {
                    break;
                }
                Error("network error");
                break;
            case EVENT_TIMEOUT:
                if ( SendHeartbeat()) {
                    break;
                }
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
            } else if (mConnectionPtr) {
                Error();
            }
            if (mConnectionPtr && ! mAuthenticateOpPtr) {
                mConnectionPtr->SetMaxReadAhead(mImpl.GetMaxReadAhead());
                mConnectionPtr->SetInactivityTimeout(
                    mImpl.GetHeartbeatInterval());
            }
        }
        return 0;
    }
    void Shutdown()
    {
        mHeartbeatInFlighSeq = -1;
        if (mConnectionPtr) {
            mConnectionPtr->Close();
            mConnectionPtr.reset();
        }
        if (mSleepingFlag) {
            mSleepingFlag = false;
            mImpl.GetNetManager().UnRegisterTimeoutHandler(this);
        }
    }
    const ServerLocation& GetServerLocation() const
        { return mServer; }
    virtual void Timeout()
    {
        if (mSleepingFlag) {
            mSleepingFlag = false;
            mImpl.GetNetManager().UnRegisterTimeoutHandler(this);
        }
        Connect();
    }
    void SendBlock(
        seq_t       inBlockSeq,
        const char* inBlockPtr,
        size_t      inBlockLen)
    {
        SendBlock(inBlockSeq, inBlockPtr, inBlockLen,
            0, kKfsNullChecksum);
    }
    void SendBlock(
        seq_t       inBlockSeq,
        const char* inBlockPtr,
        size_t      inBlockLen,
        uint32_t    inChecksum,
        size_t      inChecksumStartPos)
    {
        int32_t theChecksum = inChecksum;
        if (inChecksumStartPos <= inBlockLen) {
            theChecksum = ComputeBlockChecksum(
                theChecksum,
                inBlockPtr + inChecksumStartPos,
                inBlockLen - inChecksumStartPos
            );
        }
        // Block sequence is at the end of the header, and is part of the
        // checksum.
        char* const    theSeqEndPtr = mSeqBuf + kTmpBufSize;
        char* thePtr = theSeqEndPtr - 1;
        *thePtr = '\n';
        thePtr = IntToHexString(inBlockSeq, thePtr);
        theChecksum = ChecksumBlocksCombine(
            ComputeBlockChecksum(
                kKfsNullChecksum, thePtr, theSeqEndPtr - thePtr),
            theChecksum, inBlockLen
        );
        const char* const theSeqPtr   = thePtr;
        const int         theBlockLen =
            (int)(theSeqEndPtr - theSeqPtr) + max(0, (int)inBlockLen);
        char* const theEndPtr = mTmpBuf + kTmpBufSize;
        *--thePtr = ' ';
        thePtr = IntToHexString(theBlockLen, theEndPtr);
        *--thePtr = ':';
        *--thePtr = 'l';
        IOBuffer& theBuf = mConnectionPtr->GetOutBuffer();
        theBuf.CopyIn(thePtr, (int)(theEndPtr - thePtr));
        thePtr = theEndPtr;
        *--thePtr = '\n';
        *--thePtr = '\r';
        *--thePtr = '\n';
        *--thePtr = '\r';
        thePtr = IntToHexString(theChecksum, thePtr);
        theBuf.CopyIn(thePtr, (int)(theEndPtr - thePtr));
        theBuf.CopyIn(theSeqPtr, (int)(theSeqEndPtr - theSeqPtr));
        theBuf.CopyIn(inBlockPtr, (int)inBlockLen);
        if (mRecursionCount <= 0) {
            mConnectionPtr->StartFlush();
        }
    }
    ClientAuthContext& GetAuthCtx()
        { return mAuthContext; }
private:
    typedef ClientAuthContext::RequestCtx RequestCtx;
    enum { kTmpBufSize = 2 + 1 + sizeof(long long) * 2 + 4 };

    Impl&              mImpl;
    ServerLocation     mServer;
    NetConnectionPtr   mConnectionPtr;
    MetaAuthenticate*  mAuthenticateOpPtr;
    seq_t              mNextSeq;
    int                mRecursionCount;
    ClientAuthContext  mAuthContext;
    int                mAuthType;
    RequestCtx         mAuthRequestCtx;
    seq_t              mLastSentBlockSeq;
    seq_t              mAckBlockSeq;
    uint64_t           mAckBlockFlags;
    Properties         mReplyProps;
    IOBuffer::IStream  mIstream;
    IOBuffer::WOStream mOstream;
    bool               mSleepingFlag;
    seq_t              mHeartbeatInFlighSeq;
    Transmitter*       mPrevPtr[1];
    Transmitter*       mNextPtr[1];
    char               mTmpBuf[kTmpBufSize + 1];
    char               mSeqBuf[kTmpBufSize + 1];

    friend class QCDLListOp<Transmitter>;

    void Connect()
    {
        Shutdown();
        if (! mImpl.GetNetManager().IsRunning()) {
            return;
        }
        if (! mServer.IsValid()) {
            return;
        }
        TcpSocket* theSocketPtr = new TcpSocket();
        mConnectionPtr.reset(new NetConnection(theSocketPtr, this));
        const bool kNonBlockingFlag = false;
        const int  theErr = theSocketPtr->Connect(mServer, kNonBlockingFlag);
        if (theErr != 0 && theErr != -EINPROGRESS) {
            Error("failed to connect");
            return;
        }
        mImpl.GetNetManager().AddConnection(mConnectionPtr);
        mConnectionPtr->EnableReadIfOverloaded();
        if (theErr != 0) {
            mConnectionPtr->SetDoingNonblockingConnect();
        }
        Authenticate();
    }
    bool Authenticate()
    {
        if (! mConnectionPtr || ! mAuthContext.IsEnabled()) {
            return false;
        }
        if (mAuthenticateOpPtr) {
            panic("invalid authenticate invocation: auth is in flight");
            return true;
        }
        mConnectionPtr->SetMaxReadAhead(mImpl.GetMaxReadAhead());
        mAuthenticateOpPtr = new MetaAuthenticate();
        mAuthenticateOpPtr->opSeqno            = GetNextSeq();
        mAuthenticateOpPtr->shortRpcFormatFlag = true;
        string    theErrMsg;
        const int theErr = mAuthContext.Request(
            mAuthType,
            mAuthenticateOpPtr->sendAuthType,
            mAuthenticateOpPtr->sendContentPtr,
            mAuthenticateOpPtr->sendContentLen,
            mAuthRequestCtx,
            &theErrMsg
        );
        if (theErr) {
            KFS_LOG_STREAM_ERROR <<
                mServer << ": "
                "authentication request failure: " <<
                theErrMsg <<
            KFS_LOG_EOM;
            MetaRequest::Release(mAuthenticateOpPtr);
            mAuthenticateOpPtr = 0;
            Error(theErrMsg.c_str());
            return true;
        }
        KFS_LOG_STREAM_INFO <<
            mServer << ": "
            "starting: " <<
            mAuthenticateOpPtr->Show() <<
        KFS_LOG_EOM;
        Request(*mAuthenticateOpPtr);
        return true;
    }
    void HandleRead()
    {
        IOBuffer& theBuf = mConnectionPtr->GetInBuffer();
        if (mAuthenticateOpPtr && 0 < mAuthenticateOpPtr->contentLength) {
            HandleAuthResponse(theBuf);
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
            if (0 < theRet || ! mConnectionPtr) {
                return; // Need more data, or down
            }
            theMsgLen = 0;
        }
        if (! mAuthenticateOpPtr &&
                MAX_RPC_HEADER_LEN < theBuf.BytesConsumable()) {
            Error("header size exceeds max allowed");
        }
    }
    void HandleAuthResponse(
        IOBuffer& inBuffer)
    {
        if (! mAuthenticateOpPtr || ! mConnectionPtr) {
            panic("handle auth response: invalid invocation");
            MetaRequest::Release(mAuthenticateOpPtr);
            mAuthenticateOpPtr = 0;
            Error();
            return;
        }
        if (! mAuthenticateOpPtr->contentBuf &&
                0 < mAuthenticateOpPtr->contentLength) {
            mAuthenticateOpPtr->contentBuf =
                new char [mAuthenticateOpPtr->contentLength];
        }
        const int theRem = mAuthenticateOpPtr->Read(inBuffer);
        if (0 < theRem) {
            // Request one byte more to detect extaneous data.
            mConnectionPtr->SetMaxReadAhead(theRem + 1);
            return;
        }
        if (! inBuffer.IsEmpty()) {
            KFS_LOG_STREAM_ERROR <<
                mServer << ": "
                "authentication protocol failure:" <<
                " " << inBuffer.BytesConsumable() <<
                " bytes past authentication response" <<
                " filter: " <<
                    reinterpret_cast<const void*>(mConnectionPtr->GetFilter()) <<
                " cmd: " << mAuthenticateOpPtr->Show() <<
            KFS_LOG_EOM;
            if (! mAuthenticateOpPtr->statusMsg.empty()) {
                mAuthenticateOpPtr->statusMsg += "; ";
            }
            mAuthenticateOpPtr->statusMsg += "invalid extraneous data received";
            mAuthenticateOpPtr->status    = -EINVAL;
        } else if (mAuthenticateOpPtr->status == 0) {
            if (mConnectionPtr->GetFilter()) {
                // Shutdown the current filter.
                mConnectionPtr->Shutdown();
                return;
            } else {
                mAuthenticateOpPtr->status = mAuthContext.Response(
                    mAuthenticateOpPtr->authType,
                    mAuthenticateOpPtr->useSslFlag,
                    mAuthenticateOpPtr->contentBuf,
                    mAuthenticateOpPtr->contentLength,
                    *mConnectionPtr,
                    mAuthRequestCtx,
                    &mAuthenticateOpPtr->statusMsg
                );
            }
        }
        const string theErrMsg = mAuthenticateOpPtr->statusMsg;
        const bool   theOkFlag = mAuthenticateOpPtr->status == 0;
        KFS_LOG_STREAM(theOkFlag ?
                MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
            "finished: " << mAuthenticateOpPtr->Show() <<
            " filter: "  <<
                reinterpret_cast<const void*>(mConnectionPtr->GetFilter()) <<
        KFS_LOG_EOM;
        MetaRequest::Release(mAuthenticateOpPtr);
        mAuthenticateOpPtr = 0;
        if (! theOkFlag) {
            Error(theErrMsg.c_str());
        }
    }
    bool HandleSslShutdown()
    {
        if (mAuthenticateOpPtr &&
                mConnectionPtr &&
                mConnectionPtr->IsGood() &&
                ! mConnectionPtr->GetFilter()) {
            HandleAuthResponse(mConnectionPtr->GetInBuffer());
            return (!! mConnectionPtr);
        }
        return false;
    }
    bool SendHeartbeat()
    {
        if (mAckBlockSeq < mLastSentBlockSeq ||
                0 <= mHeartbeatInFlighSeq) {
            return false;
        }
        mHeartbeatInFlighSeq = mLastSentBlockSeq;
        SendBlock(mHeartbeatInFlighSeq, "", 0);
        return true;
    }
    int HandleMsg(
        IOBuffer& inBuffer,
        int       inHeaderLen)
    {
        const char* const theHeaderPtr = inBuffer.CopyOutOrGetBufPtr(
            mImpl.GetParseBufferPtr(), inHeaderLen);
        if (2 <= inHeaderLen &&
                (theHeaderPtr[0] & 0xFF) == 'A' &&
                (theHeaderPtr[0] & 0xFF) <= ' ') {
            return HandleAck(theHeaderPtr, inHeaderLen, inBuffer);
        }
        if (3 <= inHeaderLen &&
                (theHeaderPtr[0] & 0xFF) == 'O' &&
                (theHeaderPtr[0] & 0xFF) == 'K' &&
                (theHeaderPtr[0] & 0xFF) <= ' ') {
            return HandleReply(theHeaderPtr, inHeaderLen, inBuffer);
        }
        return HanldeRequest(theHeaderPtr, inHeaderLen, inBuffer);
    }
    int HandleAck(
        const char* inHeaderPtr,
        int         inHeaderLen,
        IOBuffer&   inBuffer)
    {
        const char*       thePtr    = inHeaderPtr + 2;
        const char* const theEndPtr = thePtr + inHeaderLen;
        if (! HexIntParser::Parse(
                    thePtr, theEndPtr - thePtr, mAckBlockSeq) ||
                ! HexIntParser::Parse(
                    thePtr, theEndPtr - thePtr, mAckBlockFlags)) {
            MsgLogLines(MsgLogger::kLogLevelERROR,
                "malformed ack: ", inBuffer, inHeaderLen);
            Error("malformed ack");
            return -1;
        }
        if (mAckBlockSeq < 0 || mLastSentBlockSeq < mAckBlockSeq) {
            KFS_LOG_STREAM_ERROR <<
                mServer << ": "
                "invalid ack block sequence: " << mAckBlockSeq <<
                " last sent: "                 << mLastSentBlockSeq <<
            KFS_LOG_EOM;
            Error("invalid ack sequence");
            return -1;
        }
        inBuffer.Consume(inHeaderLen);
        if (! mAuthenticateOpPtr &&
                (mAckBlockFlags &
                    (uint64_t(1) << kLogBlockAckReAuthFlagBit)) != 0) {
            KFS_LOG_STREAM_DEBUG <<
                mServer << ": "
                "re-authentication requested" <<
            KFS_LOG_EOM;
            Authenticate();
        }
        return (mConnectionPtr ? 0 : -1);
    }
    int HandleReply(
        const char* inHeaderPtr,
        int         inHeaderLen,
        IOBuffer&   inBuffer)
    {
        mReplyProps.clear();
        if (mReplyProps.loadProperties(
                inHeaderPtr, inHeaderLen, (char)':') != 0) {
            MsgLogLines(MsgLogger::kLogLevelERROR,
                "malformed reply: ", inBuffer, inHeaderLen);
            Error("malformed reply");
            return -1;
        }
        // For now only handle authentication response.
        seq_t const theSeq = mReplyProps.getValue("c", -1);
        if (! mAuthenticateOpPtr || theSeq != mAuthenticateOpPtr->opSeqno) {
            KFS_LOG_STREAM_ERROR <<
                mServer << ": "
                "unexpected reply, authentication: " <<
                MetaRequest::ShowReq(mAuthenticateOpPtr) <<
            KFS_LOG_EOM;
            MsgLogLines(MsgLogger::kLogLevelERROR,
                "unexpected reply: ", inBuffer, inHeaderLen);
            Error("unexpected reply");
        }
        inBuffer.Consume(inHeaderLen);
        mAuthenticateOpPtr->contentLength         = mReplyProps.getValue("l", 0);
        mAuthenticateOpPtr->authType              =
            mReplyProps.getValue("A", int(kAuthenticationTypeUndef));
        mAuthenticateOpPtr->useSslFlag            =
            mReplyProps.getValue("US", 0) != 0;
        int64_t theCurrentTime                    =
            mReplyProps.getValue("CT", int64_t(-1));
        mAuthenticateOpPtr->sessionExpirationTime =
            mReplyProps.getValue("ET", int64_t(-1));
        KFS_LOG_STREAM_DEBUG <<
            mServer << ": "
            "authentication reply:"
            " cur time: "   << theCurrentTime <<
            " delta: "      << (TimeNow() - theCurrentTime) <<
            " expires in: " <<
                (mAuthenticateOpPtr->sessionExpirationTime - theCurrentTime) <<
        KFS_LOG_EOM;
        HandleAuthResponse(inBuffer);
        return (mConnectionPtr ? 0 : -1);
    }
    int HanldeRequest(
        const char* inHeaderPtr,
        int         inHeaderLen,
        IOBuffer&   inBuffer)
    {
        // No request handling for now.
        MsgLogLines(MsgLogger::kLogLevelERROR,
            "invalid response: ", inBuffer, inHeaderLen);
        Error("invalid response");
        return -1;
    }
    void HandleCmdDone(
        MetaRequest& inReq)
    {
        panic("LogTransmitter::Impl::Transmitter::HandleCmdDone "
            "unexpected invocation");
    }
    seq_t GetNextSeq()
        { return ++mNextSeq; }
    void Request(
        MetaRequest& inReq)
    {
        // For now authentication only.
        if (&inReq != mAuthenticateOpPtr) {
            panic("LogTransmitter::Impl::Transmitter: invalid request");
            return;
        }
        if (! mConnectionPtr) {
            return;
        }
        IOBuffer& theBuf = mConnectionPtr->GetOutBuffer();
        ReqOstream theStream(mOstream.Set(theBuf));
        mAuthenticateOpPtr->Request(theStream);
        mOstream.Reset();
        if (mRecursionCount <= 0) {
            mConnectionPtr->StartFlush();
        }
    }
    void Error(
        const char* inMsgPtr = 0)
    {
        if (! mConnectionPtr) {
            return;
        }
        KFS_LOG_STREAM_ERROR <<
            mServer << ": " <<
            (inMsgPtr ? inMsgPtr : "network error") <<
            " socket error: " << mConnectionPtr->GetErrorMsg() <<
        KFS_LOG_EOM;
        mConnectionPtr->Close();
        mConnectionPtr.reset();
        MetaRequest::Release(mAuthenticateOpPtr);
        mAuthenticateOpPtr = 0;
        mLastSentBlockSeq = -1;
        if (mSleepingFlag) {
            return;
        }
        mSleepingFlag = true;
        SetTimeoutInterval(mImpl.GetRetryInterval());
        mImpl.GetNetManager().RegisterTimeoutHandler(this);
    }
    void MsgLogLines(
        MsgLogger::LogLevel inLogLevel,
        const char*         inPrefixPtr,
        IOBuffer&           inBuffer,
        int                 inBufLen,
        int                 inMaxLines = 64)
    {
        const char* const thePrefixPtr = inPrefixPtr ? inPrefixPtr : "";
        istream&          theStream    = mIstream.Set(inBuffer, inBufLen);
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
        mIstream.Reset();
    }
    time_t TimeNow()
        { return mImpl.GetNetManager().Now(); }
private:
    Transmitter(
        const Transmitter& inTransmitter);
    Transmitter& operator=(
        const Transmitter& inTransmitter);
};

    void
LogTransmitter::Impl::Add(
    Transmitter& inTransmitter)
{
    List::PushBack(mTransmittersPtr, inTransmitter);
}

    void
LogTransmitter::Impl::Remove(
    Transmitter& inTransmitter)
{
    List::Remove(mTransmittersPtr, inTransmitter);
}

    int
LogTransmitter::Impl::SetParameters(
    const char*       inParamPrefixPtr,
    const Properties& inParameters)
{
    Properties::String theParamName;
    if (inParamPrefixPtr) {
        theParamName.Append(inParamPrefixPtr);
    }
    const size_t thePrefixLen = theParamName.GetSize();
    mRetryInterval = inParameters.getValue(
        theParamName.Truncate(thePrefixLen).Append(
        "retryInterval"), mRetryInterval);
    mMaxReadAhead = max(512, min(64 << 20, inParameters.getValue(
        theParamName.Truncate(thePrefixLen).Append(
        "maxReadAhead"), mMaxReadAhead)));
    mHeartbeatInterval = inParameters.getValue(
        theParamName.Truncate(thePrefixLen).Append(
        "heartbeatInterval"), mHeartbeatInterval);
    const String* const theServersPtr = inParameters.getValue(
        theParamName.Truncate(thePrefixLen).Append(
        "servers"));
    if (theServersPtr && *theServersPtr != mServers) {
        ServerLocation    theLocation;
        Locations         theLocations;
        const char*       thePtr      = theServersPtr->GetPtr();
        const char* const theEndPtr   = thePtr + theServersPtr->GetSize();
        const bool        kHexFmtFlag = false;
        while (thePtr < theEndPtr && theLocation.FromString(
                thePtr, theEndPtr - theEndPtr, kHexFmtFlag)) {
            theLocations.insert(theLocation);
        }
        List::Iterator theIt(mTransmittersPtr);
        Transmitter*   theTPtr;
        while ((theTPtr = theIt.Next())) {
            Locations::iterator const theIt =
                theLocations.find(theTPtr->GetServerLocation());
            if (theIt == theLocations.end()) {
                delete theTPtr;
            } else {
                theLocations.erase(theIt);
            }
        }
        Locations::iterator theLIt;
        while ((theLIt = theLocations.begin()) != theLocations.end()) {
            new Transmitter(*this, *theLIt);
            theLocations.erase(theLIt);
        }
        mServers = *theServersPtr;
    }
    const char* const  theAuthPrefixPtr =
        theParamName.Truncate(thePrefixLen).Append("auth.").c_str();
    ClientAuthContext* theAuthCtxPtr    =
        List::IsEmpty(mTransmittersPtr) ? 0 :
        &(List::Front(mTransmittersPtr)->GetAuthCtx());
    int                theRet           = 0;
    List::Iterator     theIt(mTransmittersPtr);
    Transmitter*       theTPtr;
    while ((theTPtr = theIt.Next())) {
        string    theErrMsg;
        const int theErr = theTPtr->SetParameters(
            theAuthCtxPtr, theAuthPrefixPtr, inParameters, theErrMsg);
        if (theErr) {
            if (theErrMsg.empty()) {
                theErrMsg = QCUtils::SysError(theErr,
                    "setting authentication parameters error");
            }
            KFS_LOG_STREAM_ERROR <<
                theTPtr->GetServerLocation() << ": " <<
                theErrMsg <<
            KFS_LOG_EOM;
            if (theRet == 0) {
                theRet = theErr;
            }
        }
        if (! theAuthCtxPtr) {
            theAuthCtxPtr = &theTPtr->GetAuthCtx();
        }
    }
    return theRet;
}

} // namespace KFS
