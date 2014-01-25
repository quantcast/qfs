//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/28
// Author: Sriram Rao, Mike Ovsiannikov -- implement PSK authentication.
//
// Copyright 2008-2013 Quantcast Corp.
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

#include "common/Properties.h"
#include "common/MsgLogger.h"
#include "kfsio/SslFilter.h"
#include "kfsio/DelegationToken.h"
#include "qcdio/QCUtils.h"

#include "ClientManager.h"
#include "ClientSM.h"
#include "ChunkManager.h"

#include <string.h>

namespace KFS
{

class ClientManager::Auth
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
            const bool kServerFlag  = true;
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
            "enabled"), inAuthEnabledFlag ? 1 : 0) != 0;
        return true;
    }
    bool Setup(
        NetConnection&      inConn,
        SslFilterServerPsk& inServerPsk)
    {
        if (! mEnabledFlag) {
            return true;
        }
        SslFilter::VerifyPeer* const kVerifyPeerPtr        = 0;
        const char*                  kPskClientIdentityPtr = "";
        const bool                   kDeleteOnCloseFlag    = true;
        const char*                  kSessionKeyPtr        = 0;
        int                          kSessionKeyLen        = 0;
        SslFilter* const theFilterPtr = new SslFilter(
            *mSslCtxPtr,
            kSessionKeyPtr,
            kSessionKeyLen,
            kPskClientIdentityPtr,
            &inServerPsk,
            kVerifyPeerPtr,
            kDeleteOnCloseFlag
        );
        const SslFilter::Error theErr = theFilterPtr->GetError();
        if (! theErr) {
            string theErrMsg;
            const int theStatus = inConn.SetFilter(theFilterPtr, &theErrMsg);
            if (theStatus) {
                KFS_LOG_STREAM_ERROR <<
                    "set ssl filter error: " << QCUtils::SysError(
                            theStatus < 0 ? -theStatus : theStatus) <<
                    " " << theErrMsg <<
                KFS_LOG_EOM;
            }
            return (theStatus == 0);
        }
        KFS_LOG_STREAM_ERROR <<
            "ssl filter create error: " <<
                SslFilter::GetErrorMsg(theErr) <<
            " status: " << theErr <<
        KFS_LOG_EOM;
        delete theFilterPtr;
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

ClientManager gClientManager;

ClientManager::ClientManager()
    : mAcceptorPtr(0),
      mIoTimeoutSec(5 * 60),
      mIdleTimeoutSec(10 * 60),
      mCounters(),
      mAuth(*(new Auth))
{
    mCounters.Clear();
}

ClientManager::~ClientManager()
{
    assert(mCounters.mClientCount == 0);
    delete mAcceptorPtr;
    delete &mAuth;
}

    bool
ClientManager::BindAcceptor(
    int inPort)
{
    delete mAcceptorPtr;
    mAcceptorPtr = 0;
    const bool kBindOnlyFlag = true;
    mAcceptorPtr = new Acceptor(inPort, this, kBindOnlyFlag);
    return mAcceptorPtr->IsAcceptorStarted();
}

    bool
ClientManager::StartListening()
{
    if (! mAcceptorPtr) {
        return false;
    }
    mAcceptorPtr->StartListening();
    return mAcceptorPtr->IsAcceptorStarted();
}

    /* virtual */ KfsCallbackObj*
ClientManager::CreateKfsCallbackObj(
    NetConnectionPtr& inConnPtr)
{
    if (! inConnPtr) {
        return 0;
    }
    assert(mCounters.mClientCount >= 0);
    mCounters.mAcceptCount++;
    mCounters.mClientCount++;
    ClientSM* const theClientPtr = new ClientSM(inConnPtr);
    if (! mAuth.Setup(*inConnPtr, *theClientPtr)) {
        delete theClientPtr;
        return 0;
    }
    return theClientPtr;
}

    bool
ClientManager::SetParameters(
    const char*       inParamsPrefixPtr,
    const Properties& inProps,
    bool              inAuthEnabledFlag)
{
    Properties::String theParamName;
    if (inParamsPrefixPtr) {
        theParamName.Append(inParamsPrefixPtr);
    }
    const size_t thePrefLen = theParamName.GetSize();
    mIoTimeoutSec   = inProps.getValue(theParamName.Append(
        "ioTimeoutSec"), mIoTimeoutSec);
    mIdleTimeoutSec = inProps.getValue(theParamName.Truncate(thePrefLen).Append(
        "idleTimeoutSec"), mIdleTimeoutSec);
    return mAuth.SetParameters(
        theParamName.Truncate(thePrefLen).Append("auth.").GetPtr(),
        inProps,
        inAuthEnabledFlag
    );
}

}
