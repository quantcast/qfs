//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/10/09
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
// \brief A lease clerk interacts with the metaserver for renewing
// leases.  There are two assumptions here:
// 1. The lease is for writes and only those need to be renewed
// 2. Prior to renewing a lease, the lease clerk checks with the
// ChunkManager to see if writes are outstanding on the chunk
// associated with the lease; only if writes are pending, is the lease
// renewed.
//----------------------------------------------------------------------------

#ifndef CHUNKSERVER_LEASECLERK_H
#define CHUNKSERVER_LEASECLERK_H

#include "kfsio/event.h"
#include "common/kfstypes.h"
#include "common/LinearHash.h"
#include "Chunk.h"
#include "KfsOps.h"

#include <vector>

namespace KFS
{

using std::vector;
struct LeaseRenewOp;
struct AllocChunkOp;

// mapping from a chunk id to its lease

class LeaseClerk : public KfsCallbackObj
{
public:

    LeaseClerk();
    ~LeaseClerk()
        {}
    /// Register a lease with the clerk.  The clerk will renew the
    /// lease with the server. 
    /// @param[in] chunkId The chunk associated with the lease.
    /// @param[in] leaseId  The lease id to be registered with the clerk
    /// @param[in] appendFlag True if chunk created in write append mode
    void RegisterLease(const AllocChunkOp& op);
    void UnRegisterLease(kfsChunkId_t chunkId);
    void InvalidateLease(kfsChunkId_t chunkId);

    /// Used for voluntarily giving up a write lease.
    ///
    void RelinquishLease(kfsChunkId_t chunkId, int64_t size = -1,
        bool hasChecksum = false, uint32_t checksum = 0);
    /// Record the occurence of a write.  This notifies the clerk to
    /// renew the lease prior to the end of the lease period.
    void DoingWrite(kfsChunkId_t chunkId);

    /// Check if lease is still valid.
    /// @param[in] chunkId  The chunk whose lease we are checking for validity.
    bool IsLeaseValid(
        kfsChunkId_t           chunkId,
        SyncReplicationAccess* syncReplicationAccess = 0,
        bool*                  allowCSClearTextFlag  = 0) const;

    // Lease renew op completion handler.
    int HandleEvent(int code, void *data);

    time_t GetLeaseExpireTime(kfsChunkId_t chunkId) const;
    void UnregisterAllLeases();

    void Timeout();
private:
    struct LeaseInfo_t
    {
        int64_t               leaseId;
        time_t                expires;
        time_t                lastWriteTime;
        bool                  leaseRenewSent:1;
        bool                  appendFlag:1;
        bool                  invalidFlag:1;
        bool                  allowCSClearTextFlag:1;
        time_t                syncReplicationExpirationTime;
        SyncReplicationAccess syncReplicationAccess;
    };
    typedef KVPair<kfsChunkId_t, LeaseInfo_t> LeaseMapEntry;
    typedef LinearHash<
        LeaseMapEntry,
        KeyCompare<kfsChunkId_t>,
        DynamicArray<
            SingleLinkedList<LeaseMapEntry>*,
            8 // start from 256 entries
        >,
        StdFastAllocator<LeaseMapEntry>
    > LeaseMap;

    /// All the leases registered with the clerk
    LeaseMap          mLeases;
    time_t            mLastLeaseCheckTime;
    vector<chunkId_t> mTmpExpireQueue;

    void LeaseRenewed(LeaseRenewOp& op);
    void LeaseExpired(kfsChunkId_t chunkId);

    inline static time_t Now();
private:
    LeaseClerk(const LeaseClerk&);
    LeaseClerk& operator=(const LeaseClerk&);
};

extern LeaseClerk gLeaseClerk;

}

#endif // CHUNKSERVER_LEASECLERK_H
