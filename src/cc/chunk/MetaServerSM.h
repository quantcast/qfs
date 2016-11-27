//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/07
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
// \file MetaServerSM.h
// \brief State machine that interfaces with the meta server and
// handles the RPCs sent by the meta server.
//
//----------------------------------------------------------------------------

#ifndef CHUNKSERVER_METASERVERSM_H
#define CHUNKSERVER_METASERVERSM_H

#include "common/kfstypes.h"
#include "common/kfsdecls.h"
#include "common/SingleLinkedQueue.h"
#include "common/Properties.h"
#include "kfsio/ITimeout.h"

#include "KfsOps.h"

#include <string>
#include <vector>

namespace KFS
{
using std::string;
using std::vector;

class Resolver;

class MetaServerSM : public ITimeout
{
public:
    typedef vector<ServerLocation> Locations;

    struct Counters
    {
        typedef int64_t Counter;

        Counter mConnectCount;
        Counter mHelloCount;
        Counter mHelloErrorCount;
        Counter mHelloDoneCount;
        Counter mAllocCount;
        Counter mAllocErrorCount;

        void Clear()
        {
            mConnectCount    = 0;
            mHelloCount      = 0;
            mHelloErrorCount = 0;
            mHelloDoneCount  = 0;
            mAllocCount      = 0;
            mAllocErrorCount = 0;
        }
    };
    void GetCounters(Counters& counters) {
        counters = mCounters;
    }
    Counters::Counter GetHelloDoneCount() const {
        return mCounters.mHelloDoneCount;
    }
    bool IsRunning() const {
        return mRunningFlag;
    }
    bool IsAuthEnabled() const {
        return mAuthEnabledFlag;
    }
    bool HasPendingOps() const {
        return (! mPendingOps.IsEmpty());
    }
    bool IsUp() const;
    time_t ConnectionUptime() const;
    bool IsInProgress(const KfsOp& op) const;
    const KfsOp* FindInFlightOp(seq_t seq) const;
    ServerLocation CetPrimaryLocation() const;
    void EnqueueOp(KfsOp* op);
    int SetParameters(const Properties& prop);
    void Reconnect();
    void Shutdown();
    void ForceDown();
    int SetMetaInfo(
        const string&     clusterKey,
        int               rackId,
        const string&     md5sum,
        const Properties& prop
    );
    virtual void Timeout();
private:
    typedef SingleLinkedQueue<KfsOp, KfsOp::GetNext> OpsQueue;
    class Impl;
    class ResolverReq;

    Counters     mCounters;
    bool         mRunningFlag;
    bool         mAuthEnabledFlag;
    bool         mAllowDuplicateLocationsFlag;
    bool         mUpdateServerIpFlag;
    time_t       mResolverStartTime;
    int          mResolverRetryInterval;
    int          mResolverInFlightCount;
    int          mResolvedInFlightCount;
    OpsQueue     mPendingOps;
    Impl*        mPrimary;
    Locations    mLocations;
    int64_t      mChanId;
    Resolver*    mResolver;
    Properties   mParameters;
    string       mClusterKey;
    string       mMd5sum;
    int          mRackId;
    Impl*        mImpls[1];
    ResolverReq* mResolverReqs[1];

    MetaServerSM();
    ~MetaServerSM();
    void Cleanup();
    friend class ChunkServerGlobals;
public:
    inline void SetPrimary(Impl& primary, const ServerLocation& loc);
    void Resolved(ResolverReq& req);
    void Error(Impl& impl);
private:
    // No copy.
    MetaServerSM(const MetaServerSM&);
    MetaServerSM& operator=(const MetaServerSM&);
};

extern MetaServerSM& gMetaServerSM;

}

#endif // CHUNKSERVER_METASERVERSM_H
