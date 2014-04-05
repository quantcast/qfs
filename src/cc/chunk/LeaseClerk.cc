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
// \brief Code for dealing with chunk write leases.
//
//----------------------------------------------------------------------------

#include "LeaseClerk.h"
#include "kfsio/Globals.h"
#include "kfsio/DelegationToken.h"

#include "ChunkManager.h"
#include "MetaServerSM.h"
#include "AtomicRecordAppender.h"

namespace KFS
{
using KFS::libkfsio::globalNetManager;

LeaseClerk gLeaseClerk;

static const string kWriteLease("WRITE_LEASE");

inline time_t
LeaseClerk::Now()
{
    return globalNetManager().Now();
}

LeaseClerk::LeaseClerk()
    : mLeases(),
      mLastLeaseCheckTime(Now() - LEASE_INTERVAL_SECS * 2),
      mTmpExpireQueue()
{
    SET_HANDLER(this, &LeaseClerk::HandleEvent);
    mTmpExpireQueue.reserve(4 << 10);
}

void
LeaseClerk::RegisterLease(const AllocChunkOp& op)
{
    // Get replace the old lease if there is one
    bool insertedFlag = false;
    LeaseInfo_t& lease = *mLeases.Insert(op.chunkId, LeaseInfo_t(), insertedFlag);
    lease.leaseId                       = op.leaseId;
    lease.lastWriteTime                 = Now();
    lease.expires                       = lease.lastWriteTime + LEASE_INTERVAL_SECS;
    lease.leaseRenewSent                = false;
    lease.invalidFlag                   = false;
    lease.allowCSClearTextFlag          = op.allowCSClearTextFlag;
    lease.appendFlag                    = op.appendFlag;
    lease.syncReplicationExpirationTime = -LEASE_INTERVAL_SECS;
    lease.syncReplicationAccess         = op.syncReplicationAccess;
    if (0 < op.chunkServerAccessValidForTime) {
        lease.syncReplicationExpirationTime += (time_t)(
            op.chunkServerAccessIssuedTime + op.chunkServerAccessValidForTime);
    } else if (lease.syncReplicationAccess.chunkServerAccess) {
        DelegationToken token;
        if (token.FromString(
                lease.syncReplicationAccess.chunkServerAccess->token.mPtr,
                lease.syncReplicationAccess.chunkServerAccess->token.mLen,
                0, 0)) {
            lease.syncReplicationExpirationTime +=
                token.GetIssuedTime() + token.GetValidForSec();
        } else {
            lease.syncReplicationExpirationTime += lease.lastWriteTime;
        }
    }
    KFS_LOG_STREAM_DEBUG <<
        "registered lease:"
        " chunk: " << op.chunkId <<
        " lease: " << lease.leaseId <<
    KFS_LOG_EOM;
}

void
LeaseClerk::UnRegisterLease(kfsChunkId_t chunkId)
{
    if (mLeases.Erase(chunkId) <= 0) {
        return;
    }
    KFS_LOG_STREAM_DEBUG <<
        "Lease for chunk: " << chunkId << " unregistered" <<
    KFS_LOG_EOM;
}

void
LeaseClerk::InvalidateLease(kfsChunkId_t chunkId)
{
    LeaseInfo_t* const lease = mLeases.Find(chunkId);
    if (! lease ||
            lease->invalidFlag ||
            lease->appendFlag ||
            lease->expires < Now()) {
        return;
    }
    // Keep meta server's lease valid to allow the client to re-allocate the
    // chunk.
    // Re-allocation will replace the lease.
    lease->lastWriteTime = Now();
    lease->invalidFlag = true;
    KFS_LOG_STREAM_DEBUG <<
        "Lease for chunk: " << chunkId << " invalidated" <<
    KFS_LOG_EOM;
}

void
LeaseClerk::UnregisterAllLeases()
{
    KFS_LOG_STREAM_DEBUG <<
        "Unregistered all " << mLeases.GetSize() << " leases" <<
    KFS_LOG_EOM;
    mLeases.Clear();
}

void
LeaseClerk::DoingWrite(kfsChunkId_t chunkId)
{
    LeaseInfo_t* const lease = mLeases.Find(chunkId);
    if (! lease) {
        return;
    }
    lease->lastWriteTime = Now();
}

bool
LeaseClerk::IsLeaseValid(
    kfsChunkId_t           chunkId,
    SyncReplicationAccess* syncReplicationAccess, /* = 0 */
    bool*                  allowCSClearTextFlag   /* = 0 */) const
{
    // now <= lease.expires ==> lease hasn't expired and is therefore
    // valid.
    LeaseInfo_t* const lease = mLeases.Find(chunkId);
    const bool validFlag =
        lease && ! lease->invalidFlag && Now() <= lease->expires;
    if (syncReplicationAccess) {
        if (validFlag) {
            *syncReplicationAccess = lease->syncReplicationAccess;
        } else {
            syncReplicationAccess->Clear();
        }
    }
    if (allowCSClearTextFlag) {
        *allowCSClearTextFlag = validFlag && lease->allowCSClearTextFlag;
    }
    return validFlag;
}

time_t
LeaseClerk::GetLeaseExpireTime(kfsChunkId_t chunkId) const
{
    LeaseInfo_t* const lease = mLeases.Find(chunkId);
    return ((! lease || lease->invalidFlag) ?
        Now() - 1 : lease->expires);
}

void
LeaseClerk::LeaseRenewed(LeaseRenewOp& op)
{
    LeaseInfo_t* const li = mLeases.Find(op.chunkId);
    if (! li) {
        return; // Ignore stale renew reply.
    }
    const time_t now   = Now();
    LeaseInfo_t& lease = *li;
    lease.expires               = now + LEASE_INTERVAL_SECS;
    lease.leaseRenewSent        = false;
    if (op.syncReplicationAccess.chunkServerAccess) {
        lease.syncReplicationAccess.chunkServerAccess.swap(
            op.syncReplicationAccess.chunkServerAccess);
        lease.syncReplicationExpirationTime = -LEASE_INTERVAL_SECS;
        if (0 < op.chunkServerAccessValidForTime) {
            lease.syncReplicationExpirationTime +=
                (time_t)(op.chunkServerAccessIssuedTime +
                op.chunkServerAccessValidForTime);
        } else {
            DelegationToken token;
            if (token.FromString(
                    lease.syncReplicationAccess.chunkServerAccess->token.mPtr,
                    lease.syncReplicationAccess.chunkServerAccess->token.mLen,
                    0, 0)) {
                lease.syncReplicationExpirationTime +=
                    token.GetIssuedTime() + token.GetValidForSec();
            } else {
                lease.syncReplicationExpirationTime += now;
            }
        }
    }
    lease.allowCSClearTextFlag = op.allowCSClearTextFlag;
    if (op.syncReplicationAccess.chunkAccess) {
        lease.syncReplicationAccess.chunkAccess.swap(
            op.syncReplicationAccess.chunkAccess);
    }
    KFS_LOG_STREAM_INFO <<
        "lease renewed for:"
        " chunk: " << op.chunkId <<
        " lease: " << lease.leaseId <<
    KFS_LOG_EOM;
}

int
LeaseClerk::HandleEvent(int code, void* data)
{
    switch(code) {
        case EVENT_CMD_DONE: {
            // we got a reply for a lease renewal
            KfsOp* const op = reinterpret_cast<KfsOp*>(data);
            if (! op) {
                break;
            }
            if (op->op == CMD_LEASE_RENEW) {
                LeaseRenewOp* const renewOp = static_cast<LeaseRenewOp*>(op);
                if (renewOp->status == 0) {
                    LeaseRenewed(*renewOp);
                } else {
                    KFS_LOG_STREAM_ERROR << renewOp->Show() <<
                        " status: " << renewOp->status <<
                        " msg: "    << renewOp->statusMsg <<
                    KFS_LOG_EOM;
                    UnRegisterLease(renewOp->chunkId);
                }
            } else if (op->op != CMD_LEASE_RELINQUISH) {
                // Relinquish op will get here with its default handler, but
                // no other op should,
                KFS_LOG_STREAM_ERROR << "unexpected op: " << op->op <<
                KFS_LOG_EOM;
            }
            delete op;
        }
        break;

        default:
            assert(!"Unknown event");
            break;
    }
    return 0;
}

void
LeaseClerk::Timeout()
{
    const time_t now = Now();
    if (mLastLeaseCheckTime + 1 >= now) {
        return;
    }
    mLastLeaseCheckTime = now;
    mTmpExpireQueue.clear();
    // once per second, check the state of the leases
    mLeases.First();
    const LeaseMapEntry* entry;
    while ((entry = mLeases.Next())) {
        // messages could be in-flight...so wait for a full
        // lease-interval before discarding dead leases
        const kfsChunkId_t chunkId = entry->GetKey();
        LeaseInfo_t&       lease   = const_cast<LeaseInfo_t&>(entry->GetVal());
        if (lease.expires + LEASE_INTERVAL_SECS < now) {
            KFS_LOG_STREAM_INFO <<
                "cleanup lease: " << lease.leaseId <<
                " chunk: "        << chunkId <<
            KFS_LOG_EOM;
            mTmpExpireQueue.push_back(chunkId);
            continue;
        }
        /// Before the lease expires at the server, we submit we a renew
        /// request, so that the lease remains valid.  So, back-off a few
        /// secs before the leases and submit the renew
        if (lease.leaseRenewSent ||
                now + LEASE_INTERVAL_SECS - 60 < lease.expires) {
            // If the lease is valid for a while or a lease renew is in flight,
            // move on
            continue;
        }
        // Renew the lease if a write is pending or a write
        // occured when we had a valid lease or if we are doing record
        // appends to the chunk and some client has space reserved or
        // there is some data buffered in the appender.
        if (lease.lastWriteTime + LEASE_INTERVAL_SECS < now &&
                ! (lease.appendFlag ?
                    gAtomicRecordAppendManager.WantsToKeepLease(chunkId) :
                    gChunkManager.IsWritePending(chunkId)
                )) {
            continue;
        }
        // The metaserverSM will fill seq#.
        LeaseRenewOp* const op = new LeaseRenewOp(
            -1, chunkId, lease.leaseId, kWriteLease,
            lease.syncReplicationAccess.chunkServerAccess &&
                lease.syncReplicationExpirationTime <= now
        );
        KFS_LOG_STREAM_INFO <<
            "sending lease renew for:"
            " chunk: "      << chunkId <<
            " lease: "      << lease.leaseId <<
            " expires in: " << (lease.expires - now) << " sec" <<
        KFS_LOG_EOM;
        op->noRetry = true;
        op->clnt    = this;
        lease.leaseRenewSent = true;
        gMetaServerSM.EnqueueOp(op);
    }
    for (vector<kfsChunkId_t>::const_iterator it = mTmpExpireQueue.begin();
            it != mTmpExpireQueue.end();
            ++it) {
        mLeases.Erase(*it);
    }
    mTmpExpireQueue.clear();
}

void
LeaseClerk::RelinquishLease(kfsChunkId_t chunkId, int64_t size,
    bool hasChecksum, uint32_t checksum)
{
    LeaseInfo_t* const it = mLeases.Find(chunkId);
    if (! it) {
        KFS_LOG_STREAM_DEBUG <<
            "lease relinquish: no lease exists for:"
            " chunk: "    << chunkId <<
            " size: "     << size    <<
            " checksum: " << (hasChecksum ? int64_t(checksum) : int64_t(-1)) <<
        KFS_LOG_EOM;
        return;
    }
    // Notify metaserver if the lease exists, even if lease expired or renew is
    // in flight, then delete the lease.
    const LeaseInfo_t& lease = *it;
    LeaseRelinquishOp* const op = new LeaseRelinquishOp(
        -1, chunkId, lease.leaseId, kWriteLease);
    KFS_LOG_STREAM_INFO <<
        "sending lease relinquish for:"
        " chunk: "      << chunkId <<
        " lease: "      << lease.leaseId <<
        " expires in: " << (lease.expires - Now()) << " sec" <<
        " size: "       << size <<
        " checksum: "   << (hasChecksum ? int64_t(checksum) : int64_t(-1)) <<
    KFS_LOG_EOM;
    op->noRetry       = true; // On disconnect meta server expires write leases.
    op->hasChecksum   = hasChecksum;
    op->chunkChecksum = checksum;
    op->chunkSize     = size;
    op->clnt          = this;
    mLeases.Erase(chunkId);
    gMetaServerSM.EnqueueOp(op);
}
} // namespace KFS
