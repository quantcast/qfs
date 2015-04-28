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
    class Connection;

private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

class LogTransmitter::Impl::Connection : public KfsCallbackObj
{
public:
    Connection(
        Impl& inImpl)
        : KfsCallbackObj(),
          mImpl(inImpl),
          mConnectionPtr(),
          mAuthenticateOpPtr(0),
          mNextSeq(mImpl.RandomSeq()),
          mAuthContext(),
          mAuthType(0),
          mAuthRequestCtx()
        {}
    ~Connection()
        {}
    void SetParameters(
        const ServerLocation& inServerLocation,
        ClientAuthContext*    inAuthCtxPtr,
        const char*           inParamPrefixPtr,
        const Properties&     inParameters)
    {
    }
private:
    typedef ClientAuthContext::RequestCtx RequestCtx;

    Impl&              mImpl;
    NetConnectionPtr   mConnectionPtr;
    MetaAuthenticate*  mAuthenticateOpPtr;
    seq_t              mNextSeq;
    ClientAuthContext  mAuthContext;
    int                mAuthType;
    RequestCtx         mAuthRequestCtx;

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
    seq_t GetNextSeq()
        { return ++mNextSeq; }
    void Request(
        MetaRequest& inReq)
    {
    }
    void Error(
        const char* inMsgPtr)
    {
    }
private:
    Connection(
        const Connection& inConnection);
    Connection& operator=(
        const Connection& inConnection);
};

} // namespace KFS
