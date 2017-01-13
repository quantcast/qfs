//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2014/01/20
// Author: Mike Ovsiannikov
//
// Copyright 2014,2016 Quantcast Corporation. All rights reserved.
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
// \brief "Client" for monitoring and administering from meta and chunk servers.
//----------------------------------------------------------------------------

#include "MonClient.h"
#include "common/MsgLogger.h"
#include "common/Properties.h"
#include "kfsio/NetManager.h"
#include "kfsio/SslFilter.h"
#include "kfsio/Globals.h"
#include "kfsio/CryptoKeys.h"
#include "libclient/KfsClient.h"
#include "libclient/KfsNetClient.h"
#include "libclient/KfsOps.h"

#include <errno.h>
#include <stdlib.h>
#include <signal.h>

namespace KFS_MON {

using namespace KFS;
using namespace KFS::client;

class MonIinit
{
public:
    MonIinit()
        : mError(0)
    {
        signal(SIGPIPE, SIG_IGN);
        libkfsio::InitGlobals();
        mError = SslFilter::Initialize();
        if (mError != 0) {
            KFS_LOG_STREAM_ERROR <<
                "failed to initialize ssl status: " << mError <<
            KFS_LOG_EOM;
        }
    }
    ~MonIinit()
        { SslFilter::Cleanup(); }
    bool IsInited()
        { return (mError == 0); }
private:
    SslFilter::Error mError;
};

    static int
MonInit()
{
    static MonIinit sInit;
    return (sInit.IsInited() ? 0 : -EFAULT);
}

    static int64_t
InitialSeq()
{
    int64_t theRet = 0;
    CryptoKeys::PseudoRand(&theRet, sizeof(theRet));
    return ((theRet < 0 ? -theRet : theRet) >> 1);
}

    static int
CallMonInit()
{
    // kludge to call ssl init before auth context ctor
    MonInit();
    const int kNetManagerTimeoutMs = 1000;
    return kNetManagerTimeoutMs;
}

MonClient::MonClient()
    : NetManager(CallMonInit()),
      KfsNetClient(
        *this,
        string(),     // inHost
        0,            // inPort
        3,            // inMaxRetryCount
        10,           // inTimeSecBetweenRetries
        5  * 60,      // inOpTimeoutSec
        30 * 60,      // inIdleTimeoutSec
        InitialSeq()  // inInitialSeqNum,
      ), 
      KfsNetClient::OpOwner(),
      mAuthContext(),
      mNextIdempotentId(InitialSeq())
{
    KfsNetClient::SetAuthContext(&mAuthContext);
    KfsNetClient::SetMaxMetaLogWriteRetryCount(
        2 * KfsNetClient::GetMaxRetryCount());
}

MonClient::~MonClient()
{
    KfsNetClient::Cancel();
}

    int
MonClient::SetParameters(
    const ServerLocation& inMetaLocation,
    const char*           inConfigFileNamePtr,
    bool                  inSetMetaLocationsFlag)
{
    int theStatus = MonInit();
    if (theStatus != 0) {
        return theStatus;
    }

    Properties  theProperties;
    const char* theConfigPtr = inConfigFileNamePtr;
    if (inConfigFileNamePtr) {
        const char kDelimeter = '=';
        theStatus = theProperties.loadProperties(theConfigPtr, kDelimeter);
    } else {
        theStatus = KfsClient::LoadProperties(
            inMetaLocation.hostname.c_str(),
            inMetaLocation.port,
            0,
            theProperties,
            theConfigPtr
        );
    }
    if (theStatus == 0 && theConfigPtr) {
        const bool         kVerifyFlag  = true;
        ClientAuthContext* kOtherCtxPtr = 0;
        string*            kErrMsgPtr   = 0;
        theStatus = mAuthContext.SetParameters(
            KfsClient::GetClientAuthParamsPrefix(),
            theProperties,
            kOtherCtxPtr,
            kErrMsgPtr,
            kVerifyFlag
        );
    }
    if (0 == theStatus && inSetMetaLocationsFlag) {
        const string theMetaNodes         = theProperties.getValue(
            KfsClient::GetMetaServerNodesParamName(), string());
        const bool   kAllowDuplicatesFlag = true;
        const bool   kHexFormatFlag       = false;
        if (SetMetaServerLocations(
                inMetaLocation,
                theMetaNodes.data(),
                theMetaNodes.size(),
                kAllowDuplicatesFlag,
                kHexFormatFlag) <= 0) {
            theStatus = -EINVAL;
        }
    }
    return theStatus;
}

    void
MonClient::OpDone(
    KfsOp*    inOpPtr,
    bool      inCanceledFlag,
    IOBuffer* inBufferPtr)
{
    assert(inOpPtr && ! inBufferPtr);
    if (inCanceledFlag && inOpPtr->status == 0) {
        inOpPtr->status    = -ECANCELED;
        inOpPtr->statusMsg = "canceled";
    }
    KFS_LOG_STREAM_DEBUG <<
        (inCanceledFlag ? "op canceled: " : "op completed: ") <<
        inOpPtr->Show() << " status: " << inOpPtr->status <<
        (inOpPtr->status < 0 ?
            (" " + ErrorCodeToString(inOpPtr->status)) : string()) <<
        " msg: " << inOpPtr->statusMsg <<
    KFS_LOG_EOM;
    KfsNetClient::GetNetManager().Shutdown(); // Exit service loop.
}

    int
MonClient::Execute(
    KfsOp& inOp)
{
    int theStatus = MonInit();
    if (theStatus != 0) {
        return theStatus;
    }
    KfsNetClient::GetNetManager().UpdateTimeNow();
    if (! KfsNetClient::Enqueue(&inOp, this)) {
        KFS_LOG_STREAM_FATAL << "failed to enqueue op: " <<
            inOp.Show() <<
        KFS_LOG_EOM;
        MsgLogger::Stop();
        abort();
        return -EFAULT;
    }
    const bool     kWakeupAndCleanupFlag = false;
    QCMutex* const kNullMutexPtr         = 0;
    KfsNetClient::GetNetManager().MainLoop(
        kNullMutexPtr, kWakeupAndCleanupFlag);
    KfsNetClient::Cancel();
    return inOp.status;
}

}
