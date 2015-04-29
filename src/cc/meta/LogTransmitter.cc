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
#include "kfsio/IOBuffer.h"
#include "kfsio/ClientAuthContext.h"
#include "kfsio/event.h"

#include "qcdio/QCUtils.h"
#include "qcdio/qcdebug.h"

#include <string>

namespace KFS
{
using std::string;

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
public:
    Impl()
        {}
    ~Impl()
        {}
    void SetParameters(
        const char*       inParamPrefixPtr,
        const Properties& inParameters)
    {
    }
    static seq_t RandomSeq()
    {
        seq_t theReq = 0;
        CryptoKeys::PseudoRand(&theReq, sizeof(theReq));
        return ((theReq < 0 ? -theReq : theReq) >> 1);
    }
private:
    class Transmitter;

private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

class LogTransmitter::Impl::Transmitter : public KfsCallbackObj
{
public:
    Transmitter(
        Impl& inImpl)
        : KfsCallbackObj(),
          mImpl(inImpl),
          mConnectionPtr(),
          mAuthenticateOpPtr(0),
          mNextSeq(mImpl.RandomSeq()),
          mRecursionCount(0),
          mAuthContext(),
          mAuthType(0),
          mAuthRequestCtx(),
          mDownFlag(false)
        { SET_HANDLER(this, &Transmitter::HandleEvent); }
    ~Transmitter()
    {
        MetaRequest::Release(mAuthenticateOpPtr);
    }
    void SetParameters(
        const ServerLocation& inServerLocation,
        ClientAuthContext*    inAuthCtxPtr,
        const char*           inParamPrefixPtr,
        const Properties&     inParameters)
    {
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
        }
        return 0;
    }
private:
    typedef ClientAuthContext::RequestCtx RequestCtx;

    Impl&              mImpl;
    NetConnectionPtr   mConnectionPtr;
    MetaAuthenticate*  mAuthenticateOpPtr;
    seq_t              mNextSeq;
    int                mRecursionCount;
    ClientAuthContext  mAuthContext;
    int                mAuthType;
    RequestCtx         mAuthRequestCtx;
    bool               mDownFlag;

    bool Authenticate()
    {
        if (! mAuthContext.IsEnabled()) {
            return false;
        }
        if (mAuthenticateOpPtr) {
            panic("invalid authenticate invocation: auth is in flight");
            return true;
        }
        mAuthenticateOpPtr = new MetaAuthenticate();
        mAuthenticateOpPtr->opSeqno            = GetNextSeq();
        mAuthenticateOpPtr->shortRpcFormatFlag = true;
        string    theErrMsg;
        const int theErr = mAuthContext.Request(
            mAuthType,
            mAuthenticateOpPtr->authType,
            mAuthenticateOpPtr->responseContentPtr,
            mAuthenticateOpPtr->responseContentLen,
            mAuthRequestCtx,
            &theErrMsg
        );
        if (theErr) {
            KFS_LOG_STREAM_ERROR <<
                "authentication request failure: " <<
                theErrMsg <<
            KFS_LOG_EOM;
            MetaRequest::Release(mAuthenticateOpPtr);
            mAuthenticateOpPtr = 0;
            Error(theErrMsg.c_str());
            return true;
        }
        Request(*mAuthenticateOpPtr);
        KFS_LOG_STREAM_INFO << "started: " << mAuthenticateOpPtr->Show() <<
        KFS_LOG_EOM;
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
            if (0 < theRet || mDownFlag) {
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
            mConnectionPtr->SetMaxReadAhead(theRem);
            return;
        }
        if (! inBuffer.IsEmpty()) {
            KFS_LOG_STREAM_ERROR <<
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
                    mAuthenticateOpPtr->responseUseSslFlag,
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
                MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
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
            return (! mDownFlag);
        }
        return false;
    }
    int HandleMsg(
        IOBuffer& inBuffer,
        int       inMsgLen)
    {
        return 0;
    }
    void HandleCmdDone(
        MetaRequest& inReq)
    {
    }
    seq_t GetNextSeq()
        { return ++mNextSeq; }
    void Request(
        MetaRequest& inReq)
    {
    }
    void Error(
        const char* inMsgPtr = 0)
    {
    }
private:
    Transmitter(
        const Transmitter& inTransmitter);
    Transmitter& operator=(
        const Transmitter& inTransmitter);
};

} // namespace KFS
