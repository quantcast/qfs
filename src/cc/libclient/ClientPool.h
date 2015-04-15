//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2009/05/20
// Author: Mike Ovsiannikov
//
// Copyright 2011-2012 Quantcast Corp.
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

#ifndef CLIENT_POOL_H
#define CLIENT_POOL_H

#include "common/kfsdecls.h"
#include "common/StdAllocator.h"
#include "KfsNetClient.h"

#include <map>
#include <utility>
#include <sstream>

namespace KFS
{
class ClientAuthContext;
namespace client
{
using std::pair;
using std::make_pair;
using std::map;
using std::less;
using std::ostringstream;
using std::string;

// Client connection (KfsNetClient) pool. Used to reduce number of chunk
// server connections. Presently used only with radix sort with write append
// with M clients each appending to N buckets.
class ClientPool
{
public:
    typedef KfsNetClient::Stats Stats;

    ClientPool(
        NetManager&        inNetManager,
        int                inMaxRetryCount                  = 0,
        int                inTimeSecBetweenRetries          = 10,
        int                inOpTimeoutSec                   = 5  * 60,
        int                inIdleTimeoutSec                 = 30 * 60,
        int64_t            inInitialSeqNum                  = 1,
        const char*        inLogPrefixPtr                   = 0,
        bool               inResetConnectionOnOpTimeoutFlag = true,
        bool               inRetryConnectOnlyFlag           = true,
        int                inMaxContentLength               = MAX_RPC_HEADER_LEN,
        bool               inFailAllOpsOnOpTimeoutFlag      = false,
        bool               inMaxOneOutstandingOpFlag        = false,
        ClientAuthContext* inAuthContextPtr                 = 0)
        : mClients(),
          mNetManager(inNetManager),
          mMaxRetryCount(inMaxRetryCount),
          mTimeSecBetweenRetries(inTimeSecBetweenRetries),
          mOpTimeoutSec(inOpTimeoutSec),
          mIdleTimeoutSec(inIdleTimeoutSec),
          mInitialSeqNum(inInitialSeqNum),
          mLogPrefix(inLogPrefixPtr ? inLogPrefixPtr : ""),
          mResetConnectionOnOpTimeoutFlag(inResetConnectionOnOpTimeoutFlag),
          mRetryConnectOnlyFlag(inRetryConnectOnlyFlag),
          mMaxContentLength(inMaxContentLength),
          mFailAllOpsOnOpTimeoutFlag(inFailAllOpsOnOpTimeoutFlag),
          mMaxOneOutstandingOpFlag(inMaxOneOutstandingOpFlag),
          mAuthContextPtr(inAuthContextPtr)
        {}
    ~ClientPool()
    {
        for (Clients::const_iterator it = mClients.begin();
                it != mClients.end();
                ++it) {
            delete it->second;
        }
    }
    KfsNetClient& Get(
        const ServerLocation& inLocation)
    {
        Clients::iterator it = mClients.find(inLocation);
        if (it == mClients.end()) {
            ostringstream theStream;
            theStream <<
                mLogPrefix << (mLogPrefix.empty() ? "" : ":") <<
                inLocation.hostname << ":" << inLocation.port;
            const string thePrefix = theStream.str();
            it = mClients.insert(make_pair(inLocation, new KfsNetClient(
                mNetManager,
                inLocation.hostname,
                inLocation.port,
                mMaxRetryCount,
                mTimeSecBetweenRetries,
                mOpTimeoutSec,
                mIdleTimeoutSec,
                mInitialSeqNum++,
                thePrefix.c_str(),
                mResetConnectionOnOpTimeoutFlag,
                mMaxContentLength,
                mFailAllOpsOnOpTimeoutFlag,
                mMaxOneOutstandingOpFlag,
                mAuthContextPtr))).first;
            it->second->SetRetryConnectOnly(mRetryConnectOnlyFlag);
        }
        return *(it->second);
    }
    void GetStats(
        Stats& outStats) const
    {
        outStats.Clear();
        Stats theStats;
        for (Clients::const_iterator it = mClients.begin();
                it != mClients.end();
                ++it) {
            it->second->GetStats(theStats);
            outStats.Add(theStats);
        }
    }
    void ClearMaxOneOutstandingOpFlag(
        bool inFailAllOpsOnOpTimeoutFlag)
    {
        if (! mMaxOneOutstandingOpFlag) {
            if (mFailAllOpsOnOpTimeoutFlag == inFailAllOpsOnOpTimeoutFlag) {
                return;
            }
            mFailAllOpsOnOpTimeoutFlag = inFailAllOpsOnOpTimeoutFlag;
            for (Clients::const_iterator theIt = mClients.begin();
                    theIt != mClients.end();
                    ++theIt) {
                theIt->second->SetFailAllOpsOnOpTimeoutFlag(
                    mFailAllOpsOnOpTimeoutFlag);
            }
            return;
        }
        mMaxOneOutstandingOpFlag   = false;
        mFailAllOpsOnOpTimeoutFlag = inFailAllOpsOnOpTimeoutFlag;
        for (Clients::const_iterator theIt = mClients.begin();
                theIt != mClients.end();
                ++theIt) {
            theIt->second->SetFailAllOpsOnOpTimeoutFlag(
                mFailAllOpsOnOpTimeoutFlag);
            theIt->second->ClearMaxOneOutstandingOpFlag();
        }
    }
    size_t GetSize() const
        { return mClients.size(); }
private:
    typedef map<
        ServerLocation,
        KfsNetClient*,
        less<ServerLocation>,
        StdFastAllocator<pair<const ServerLocation, KfsNetClient*> >
    > Clients;
    Clients            mClients;
    NetManager&        mNetManager;
    int                mMaxRetryCount;
    int                mTimeSecBetweenRetries;
    int                mOpTimeoutSec;
    int                mIdleTimeoutSec;
    int64_t            mInitialSeqNum;
    const string       mLogPrefix;
    bool               mResetConnectionOnOpTimeoutFlag;
    bool               mRetryConnectOnlyFlag;
    int                mMaxContentLength;
    bool               mFailAllOpsOnOpTimeoutFlag;
    bool               mMaxOneOutstandingOpFlag;
    ClientAuthContext* mAuthContextPtr;
private:
    ClientPool(
        const ClientPool& inPool);
    ClientPool& operator=(
        const ClientPool& inPool);
};
}
}

#endif /* CLIENT_POOL_H */
