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

#include "ClientManager.h"
#include "ClientSM.h"

namespace KFS
{

class ClientManager::Auth
{
public:
    Auth()
        {}
    ~Auth()
        {}
    bool SetParameters(
        const Properties& inProps)
    {
        return true;
    }
    bool Setup(
        NetConnection& inConn)
    {
        return true;
    }
private:
    Auth(
        const Auth& inAuth);
    Auth& operator=(
        const Auth& inAuth);
};

ClientManager gClientManager;

ClientManager::ClientManager()
    : mAcceptorPtr(0),
      mIoTimeoutSec(-1),
      mIdleTimeoutSec(-1),
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
    if (! inConnPtr || ! mAuth.Setup(*inConnPtr)) {
        return 0;
    }
    ClientSM* const clnt = new ClientSM(inConnPtr);
    assert(mCounters.mClientCount >= 0);
    mCounters.mAcceptCount++;
    mCounters.mClientCount++;
    return clnt;
}

    bool
ClientManager::SetParameters(
    const Properties& inProps)
{
    return mAuth.SetParameters(inProps);
}

}
