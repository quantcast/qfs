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

#include "ClientManager.h"
#include "ClientSM.h"
#include "ClientThread.h"

#include "common/Properties.h"
#include "common/MsgLogger.h"

#include "kfsio/SslFilter.h"
#include "kfsio/DelegationToken.h"
#include "kfsio/Globals.h"

#include "qcdio/QCUtils.h"
#include "qcdio/qcdebug.h"
#include "qcdio/qcstutils.h"

namespace KFS
{

using libkfsio::globalNetManager;

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
        const bool theEnabledFlag     = mParams.getValue(
            theParamName.Truncate(thePrefLen).Append(
            "enabled"), inAuthEnabledFlag ? 1 : 0) != 0;
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
            if (! theSslCtxPtr && theEnabledFlag) {
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
        mEnabledFlag = mSslCtxPtr && theEnabledFlag;
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
    void Clear()
    {
        mSslCtxPtr.reset();
        mParams.clear();
        mEnabledFlag = false;
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
      mAuth(*(new Auth)),
      mCurThreadIdx(0),
      mThreadCount(0),
      mThreadsPtr(0)
{
    mCounters.Clear();
}

ClientManager::~ClientManager()
{
    delete mAcceptorPtr;
    delete &mAuth;
    delete [] mThreadsPtr;
}

    bool
ClientManager::BindAcceptor(
    int inPort,
    int inThreadCount)
{
    delete mAcceptorPtr;
    delete [] mThreadsPtr;
    mAcceptorPtr = 0;
    mThreadsPtr  = 0;
    mThreadCount = 0;
    const bool kBindOnlyFlag = true;
    mAcceptorPtr = new Acceptor(inPort, this, kBindOnlyFlag);
    const bool theOkFlag = mAcceptorPtr->IsAcceptorStarted();
    if (theOkFlag && 0 < mThreadCount) {
        QCStMutexLocker theLocker(ClientThread::GetMutex());
        mThreadCount  = inThreadCount;
        mThreadsPtr   = new ClientThread[mThreadCount];
        mCurThreadIdx = 0;
        for (int i = 0; i < mThreadCount; i++) {
            mThreadsPtr[i].Start();
        }
    }
    return theOkFlag;
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

    void
ClientManager::Stop()
{
    for (int i = 0; i < mThreadCount; i++) {
        mThreadsPtr[i].Stop();
    }
}

    /* virtual */ KfsCallbackObj*
ClientManager::CreateKfsCallbackObj(
    NetConnectionPtr& inConnPtr)
{
    if (! inConnPtr) {
        return 0;
    }
    mCounters.mAcceptCount++;
    mCounters.mClientCount++;
    ClientThread* const theThreadPtr = GetNextClientThreadPtr();
    ClientSM*     const theClientPtr = new ClientSM(inConnPtr, theThreadPtr);
    if (! mAuth.Setup(*inConnPtr, *theClientPtr)) {
        delete theClientPtr;
        return 0;
    }
    if (theThreadPtr) {
        inConnPtr.reset(); // Thread takes ownership.
        theThreadPtr->Add(*theClientPtr);
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

    void
ClientManager::GetCounters(
    Counters& outCounters) const
{
    outCounters = mCounters;
}

    QCMutex*
ClientManager::GetMutexPtr() const
{
    return (0 < mThreadCount ? &ClientThread::GetMutex() : 0);
}

    void
ClientManager::Shutdown()
{
    for (int i = 0; i < mThreadCount; i++) {
        mThreadsPtr[i].Stop();
    }
    delete mAcceptorPtr;
    mAcceptorPtr = 0;
    mAuth.Clear();
}

    ClientThread*
ClientManager::GetCurrentClientThreadPtr()
{
    return (0 < mThreadCount ?
        ClientThread::GetCurrentClientThreadPtr() :
        0
    );
}

    ClientThread*
ClientManager::GetNextClientThreadPtr()
{
    if (mThreadCount <= 0) {
        return 0;
    }
    QCASSERT(0 <= mCurThreadIdx && mCurThreadIdx < mThreadCount);
    ClientThread* const theRetPtr = mThreadsPtr + mCurThreadIdx;
    mCurThreadIdx++;
    if (mThreadCount <= mCurThreadIdx) {
        mCurThreadIdx = 0;
    }
    return theRetPtr;
}

}
