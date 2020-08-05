//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/06
// Author: Sriram Rao
//         Mike Ovsiannikov
//
// Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
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
// \file LayoutManager.cc
// \brief Layout manager implementation.
//
//----------------------------------------------------------------------------

#include "LayoutManager.h"
#include "kfstree.h"
#include "ClientSM.h"
#include "NetDispatch.h"
#include "LogWriter.h"

#include "qcdio/QCIoBufferPool.h"
#include "qcdio/QCUtils.h"

#include "common/MsgLogger.h"
#include "common/Properties.h"
#include "common/time.h"
#include "common/Version.h"
#include "common/StdAllocator.h"
#include "common/rusage.h"

#include "kfsio/Globals.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/IOBufferWriter.h"

#include <algorithm>
#include <functional>
#include <sstream>
#include <iterator>
#include <fstream>
#include <limits>
#include <iomanip>
#include <boost/bind.hpp>

namespace KFS {

using std::for_each;
using std::find;
using std::sort;
using std::vector;
using std::min;
using std::max;
using std::istringstream;
using std::ostream;
using std::ostringstream;
using std::make_pair;
using std::pair;
using std::make_heap;
using std::pop_heap;
using std::ostream_iterator;
using std::copy;
using std::string;
using std::ofstream;
using std::ifstream;
using std::numeric_limits;
using std::iter_swap;
using std::setprecision;
using std::fixed;
using std::lower_bound;
using std::swap;
using std::ofstream;
using std::fstream;
using boost::bind;
using boost::ref;

using libkfsio::globalNetManager;
using libkfsio::globals;

const int64_t kSecs2MicroSecs = 1000 * 1000;

static inline time_t
TimeNow()
{
    return gLayoutManager.TimeNow();
}

static inline time_t
GetInitialWriteLeaseExpireTime()
{
    return (TimeNow() + 10 * 365 * 24 * 60 * 60);
}

inline static int
AsciiCharToLower(int c)
{
    return ((c >= 'A' && c <= 'Z') ? 'a' + (c - 'A') : c);
}

inline chunkOff_t
ChunkVersionToObjFileBlockPos(seq_t chunkVersion)
{
    return chunkStartOffset(-(chunkOff_t)chunkVersion - 1);
}

static inline void
UpdatePendingRecovery(CSMap& csmap, CSMap::Entry& ent)
{
    // Chunk wasn't previously available, check to see if recovery can
    // proceed now.
    // Schedule re-check of all pending recovery chunks that belong to the
    // file, and let the CanReplicateChunkNow decide if recovery can start
    // or not.
    if (csmap.GetState(ent) != CSMap::Entry::kStatePendingRecovery) {
        return;
    }
    const MetaFattr* const fa = ent.GetFattr();
    for (CSMap::Entry* prev = csmap.Prev(ent);
            prev && prev->GetFattr() == fa; ) {
        CSMap::Entry& entry = *prev;
        prev = csmap.Prev(entry);
        csmap.SetState(entry, CSMap::Entry::kStateCheckReplication);
    }
    for (CSMap::Entry* next = &ent; ;) {
        CSMap::Entry& entry = *next;
        next = csmap.Next(entry);
        csmap.SetState(entry, CSMap::Entry::kStateCheckReplication);
        if (! next || next->GetFattr() != fa) {
            break;
        }
    }
}

static inline void
UpdateReplicationState(CSMap& csmap, CSMap::Entry& entry)
{
    // Re-schedule replication check if needed.
    CSMap::Entry::State const curState = csmap.GetState(entry);
    if (curState == CSMap::Entry::kStatePendingRecovery) {
        UpdatePendingRecovery(csmap, entry);
    } else if (curState == CSMap::Entry::kStatePendingReplication) {
        csmap.SetState(entry, CSMap::Entry::kStateCheckReplication);
    }
}

static inline bool
IsObjectStoreBlock(fid_t fid, chunkOff_t pos)
{
    const MetaFattr* const fa = metatree.getFattr(fid);
    return (fa && KFS_FILE == fa->type && 0 == fa->numReplicas &&
        pos <= fa->nextChunkOffset());
}

static inline void
ResubmitRequest(MetaRequest& req)
{
    req.status       = 0;
    req.statusMsg.clear();
    req.submitCount  = 0;
    req.seqno        = -1;
    req.logAction    = MetaRequest::kLogIfOk;
    req.logseq       = MetaVrLogSeq();
    req.suspended    = false;
    submit_request(&req);
}

class ChunkIdMatcher
{
    const chunkId_t myid;
public:
    ChunkIdMatcher(chunkId_t c) : myid(c) { }
    bool operator() (MetaChunkInfo *c) const {
        return c->chunkId == myid;
    }
};

inline bool
LayoutManager::InRecoveryPeriod() const
{
    return (TimeNow() < mRecoveryStartTime + mRecoveryIntervalSec);
}

inline bool
LayoutManager::InRecovery() const
{
    return (
        GetConnectedServerCount() < mMinChunkserversToExitRecovery ||
        InRecoveryPeriod()
    );
}

inline bool
LayoutManager::IsChunkServerRestartAllowed() const
{
    return (
        mHibernatingServers.empty() &&
        ! InRecovery()
    );
}

inline bool
ARAChunkCache::Invalidate(fid_t fid)
{
    return (mMap.Erase(fid) > 0);
}

inline bool
ARAChunkCache::Invalidate(fid_t fid, chunkId_t chunkId)
{
    Entry* const it = mMap.Find(fid);
    if (! it || it->chunkId != chunkId) {
        return false;
    }
    mMap.Erase(fid);
    return true;
}

void
ARAChunkCache::RequestNew(MetaAllocate& req)
{
    if (req.offset < 0 || (req.offset % CHUNKSIZE) != 0 ||
            ! req.appendChunk) {
        panic("ARAChunkCache::RequestNew: invalid parameters");
        return;
    }
    // Find the end of the list, normally list should have only one element.
    MetaAllocate* last = &req;
    while (last->next) {
        last = last->next;
    }
    const Entry entry(
        req.chunkId,
        req.chunkVersion,
        req.offset,
        TimeNow(),
        last,
        req.permissions
    );
    bool insertedFlag = false;
    Entry* res = mMap.Insert(req.fid, entry, insertedFlag);
    if (! insertedFlag) {
        *res = entry;
    }
}

bool
ARAChunkCache::Entry::AddPending(MetaAllocate& req)
{
    assert(req.appendChunk);

    if (! lastPendingRequest || ! req.appendChunk) {
        if (req.appendChunk) {
            if (shortRpcFormatFlag == req.shortRpcFormatFlag) {
                req.responseStr = responseStr;
                if (req.authUid == authUid) {
                    req.responseAccessStr = responseAccessStr;
                    req.issuedTime        = issuedTime;
                }
            } else {
                req.servers = servers;
            }
        }
        return false;
    }
    assert(lastPendingRequest->suspended);
    MetaAllocate* last = &req;
    last->suspended = true;
    while (last->next) {
        last = last->next;
        last->suspended = true;
    }
    // Put request to the end of the queue.
    // Make sure that the last pointer is correct.
    while (lastPendingRequest->next) {
        lastPendingRequest = lastPendingRequest->next;
    }
    lastPendingRequest->next = last;
    lastPendingRequest = last;
    return true;
}

void
ARAChunkCache::RequestDone(const MetaAllocate& req)
{
    assert(req.appendChunk);

    Entry* const it = mMap.Find(req.fid);
    if (! it) {
        return;
    }
    Entry& entry = *it;
    if (entry.chunkId != req.chunkId) {
        return;
    }
    if (req.status != 0) {
        // Failure, invalidate the cache.
        mMap.Erase(req.fid);
        return;
    }
    entry.lastAccessedTime = TimeNow();
    entry.offset           = req.offset;
    if (entry.lastPendingRequest) {
        // Transition from pending to complete.
        // Cache the response. Restart decay timer.
        entry.shortRpcFormatFlag = req.shortRpcFormatFlag;
        entry.responseStr        = req.responseStr;
        entry.lastDecayTime      = entry.lastAccessedTime;
        entry.SetResponseAccess(req);
        entry.lastPendingRequest = 0;
    }
}

void
ARAChunkCache::Timeout(time_t minTime)
{
    mTmpClear.clear();
    mMap.First();
    const KVEntry* it;
    while ((it = mMap.Next())) {
        const Entry& entry = it->GetVal();
        if (entry.lastAccessedTime < minTime && ! entry.lastPendingRequest) {
            mTmpClear.push_back(it->GetKey());
        }
    }
    for (TmpClear::const_iterator
            it = mTmpClear.begin(); it != mTmpClear.end(); ++it) {
        mMap.Erase(*it);
    }
    mTmpClear.clear();
}

ChunkLeases::ChunkLeases(
    time_t now)
    : mReadLeases(),
      mWriteLeases(),
      mFileLeases(),
      mTimerRunningFlag(false),
      mReadLeaseTimer(now),
      mWriteLeaseTimer(now),
      mDumpsterCleanupTimer(now),
      mPendingDeleteList(),
      mWAllocationInFlightList(),
      mDumpsterCleanupDelaySec(LEASE_INTERVAL_SECS),
      mRemoveFromDumpsterInFlightCount(0)
{}

inline void
ChunkLeases::DecrementFileLease(
    ChunkLeases::FEntry& entry)
{
    if (--(entry.Get().mCount) <= 1) {
        if (entry.Get().mName.empty()) {
            mFileLeases.Erase(entry.GetKey());
        } else {
            mDumpsterCleanupTimer.Schedule(
                entry, TimeNow() + mDumpsterCleanupDelaySec);
        }
    }
}

inline void
ChunkLeases::DecrementFileLease(
    fid_t fid)
{
    if (fid < 0) {
        return;
    }
    FEntry* const entry = mFileLeases.Find(fid);
    if (! entry || entry->Get().mCount <= 1) {
        panic("internal error: invalid decrement file lease count");
        return;
    }
    DecrementFileLease(*entry);
}

inline bool
ChunkLeases::IncrementFileLease(
    ChunkLeases::FEntry& entry)
{
    if (++(entry.Get().mCount) <= 1) {
        panic("internal error: invalid increment file lease count");
        return false;
    }
    FEntry::List::Remove(entry);
    return true;
}

inline bool
ChunkLeases::IncrementFileLease(
    fid_t fid)
{
    if (fid < 0) {
        return false;
    }
    bool insertedFlag = false;
    FEntry* const entry = mFileLeases.Insert(fid, FEntry(), insertedFlag);
    if (! insertedFlag && entry->Get().mCount <= 0) {
        KFS_LOG_STREAM_DEBUG <<
            "delete pending: fid: " << fid <<
            " name: "               << entry->Get().mName <<
        KFS_LOG_EOM;
        return false;
    }
    IncrementFileLease(*entry);
    return true;
}

inline bool
ChunkLeases::IsDeletePending(
    fid_t fid) const
{
    const FEntry* const entry = mFileLeases.Find(fid);
    return (entry && entry->Get().mCount <= 0);
}

inline bool
ChunkLeases::IsDeleteScheduled(
    fid_t fid) const
{
    const FEntry* const entry = mFileLeases.Find(fid);
    return (entry && ! entry->Get().mName.empty());
}

inline void
ChunkLeases::Erase(
    ChunkLeases::WEntry& wl,
    fid_t                fid)
{
    DecrementFileLease(fid);
    if (mWriteLeases.Erase(wl.GetKey()) != 1) {
        panic("internal error: write lease delete failure");
    }
}

inline void
ChunkLeases::Erase(
    ChunkLeases::REntry& rl,
    fid_t                fid)
{
    DecrementFileLease(fid);
    const EntryKey key        = rl.GetKey();
    const bool     updateFlag =
        key.IsChunkEntry() && rl.Get().mScheduleReplicationCheckFlag;
    if (mReadLeases.Erase(key) != 1) {
        panic("internal error: read lease delete failure");
    }
    if (updateFlag) {
        gLayoutManager.ChangeChunkReplication(key.first);
    }
}

inline void
ChunkLeases::Erase(
    ChunkLeases::REntry& rl,
    CSMap&               csmap)
{
    const ChunkLeases::EntryKey& key = rl.GetKey();
    if (key.IsChunkEntry()) {
        const CSMap::Entry* const ci = csmap.Find(key.first);
        if (! ci) {
            panic("invalid stale expired read lease entry");
        }
        Erase(rl, ci ? ci->GetFileId() : -1);
    } else {
        Erase(rl, key.first);
    }
}

inline void
ChunkLeases::Renew(
    WEntry& we,
    time_t  now)
{
    WriteLease& wl = we;
    if (wl.expires < now + LEASE_INTERVAL_SECS) {
        wl.expires = now + LEASE_INTERVAL_SECS;
        PutInExpirationList(we);
    }
}

inline void
ChunkLeases::Expire(
    WEntry& we,
    time_t  now)
{
    WriteLease& wl = we;
    const time_t exp = now - 1;
    if (exp < wl.expires) {
        wl.expires = exp;
        PutInExpirationList(we);
    }
}

inline bool
ChunkLeases::IsReadLease(
    ChunkLeases::LeaseId leaseId)
{
    return ((leaseId & 0x1) == 0);
}

inline bool
ChunkLeases::IsWriteLease(
    ChunkLeases::LeaseId leaseId)
{
    return (! IsReadLease(leaseId));
}

static inline ChunkLeases::LeaseId
GenLeaseId()
{
    const ChunkLeases::LeaseId kMask =
        ~(ChunkLeases::LeaseId(1) << (sizeof(ChunkLeases::LeaseId) * 8 - 1));
    return ((ChunkLeases::LeaseId)gLayoutManager.GetRandom().Rand() & kMask);
}

inline ChunkLeases::LeaseId
ChunkLeases::NewReadLeaseId()
{
    const LeaseId kMask = ~LeaseId(1);
    const LeaseId id = GenLeaseId() & kMask;
    assert(IsReadLease(id));
    return id;
}

inline ChunkLeases::LeaseId
ChunkLeases::NewWriteLeaseId()
{
    const LeaseId id = GenLeaseId() | 1;
    assert(IsWriteLease(id));
    return id;
}

inline const ChunkLeases::WriteLease*
ChunkLeases::GetWriteLease(
    const ChunkLeases::EntryKey& key) const
{
    const WEntry* const wl = mWriteLeases.Find(key);
    return (wl ? &wl->Get() : 0);
}

inline const ChunkLeases::WriteLease*
ChunkLeases::GetChunkWriteLease(
    chunkId_t chunkId) const
{
    return GetWriteLease(EntryKey(chunkId));
}

inline const ChunkLeases::WriteLease*
ChunkLeases::GetValidWriteLease(
    const ChunkLeases::EntryKey& key) const
{
    WEntry* const we = mWriteLeases.Find(key);
    if (! we) {
        return 0;
    }
    const WriteLease& wl = *we;
    return (TimeNow() <= wl.expires ? &wl : 0);
}

inline const ChunkLeases::WriteLease*
ChunkLeases::RenewValidWriteLease(
    const ChunkLeases::EntryKey& key,
    const MetaAllocate&          req)
{
    WEntry* const we = mWriteLeases.Find(key);
    if (! we) {
        return 0;
    }
    WriteLease& wl = *we;
    const time_t now = TimeNow();
    if (wl.expires < now) {
        return 0;
    }
    if (! wl.allocInFlight) {
        if (req.authUid != kKfsUserNone) {
            if (wl.appendFlag) {
                // For now do not allow to append to the same chunk if the
                // delegation tokens are different, in order to make delegation
                // cancellation work.
                if ((0 < wl.delegationValidForTime) !=
                        req.validDelegationFlag ||
                    (req.validDelegationFlag && (
                        wl.delegationValidForTime !=
                            req.delegationValidForTime ||
                        wl.delegationFlags        != req.delegationFlags ||
                        wl.delegationIssuedTime   != req.delegationIssuedTime ||
                        wl.delegationUser         != req.authUid))) {
                    return 0;
                }
                wl.endTime = max(wl.endTime, req.sessionEndTime);
            } else {
                wl.delegationValidForTime = req.validDelegationFlag ?
                    req.delegationValidForTime : uint32_t(0);
                wl.delegationFlags      = req.delegationFlags;
                wl.delegationIssuedTime = req.delegationIssuedTime;
                wl.delegationUser       = req.authUid;
                wl.delegationSeq        = req.delegationSeq;
                wl.endTime              = req.sessionEndTime;
            }
        }
        Renew(*we, now);
    }
    return &wl;
}

inline bool
ChunkLeases::HasValidWriteLease(
    const ChunkLeases::EntryKey& key) const
{
    const WEntry* const we = mWriteLeases.Find(key);
    return (we && TimeNow() <= we->Get().expires);
}

inline bool
ChunkLeases::HasWriteLease(
    const ChunkLeases::EntryKey& key) const
{
    return (mWriteLeases.Find(key) != 0);
}

inline bool
ChunkLeases::HasValidLease(
    const ChunkLeases::EntryKey& key) const
{
    if (HasValidWriteLease(key)) {
        return true;
    }
    const REntry* const re = mReadLeases.Find(key);
    if (! re) {
        return false;
    }
    const ChunkReadLeasesHead& rl = *re;
    return (TimeNow() <= RLEntry::List::GetPrev(rl.mExpirationList).expires);
}

inline bool
ChunkLeases::HasLease(
    const ChunkLeases::EntryKey& key) const
{
    REntry* const rl = mReadLeases.Find(key);
    if (rl && ! rl->Get().mLeases.IsEmpty()) {
        return true;
    }
    return (mWriteLeases.Find(key) != 0);
}

inline bool
ChunkLeases::UpdateReadLeaseReplicationCheck(
    chunkId_t chunkId,
    bool      setScheduleReplicationCheckFlag)
{
    REntry* const rl = mReadLeases.Find(EntryKey(chunkId));
    if (rl  && ! rl->Get().mLeases.IsEmpty()) {
        if (setScheduleReplicationCheckFlag) {
            rl->Get().mScheduleReplicationCheckFlag = true;
        }
        return true;
    }
    return false;
}

inline int
ChunkLeases::ReplicaLost(
    chunkId_t          chunkId,
    const ChunkServer* chunkServer)
{
    WEntry* const wl = mWriteLeases.Find(EntryKey(chunkId));
    if (! wl) {
        return -EINVAL;
    }
    return ReplicaLost(*wl, chunkServer);
}

inline int
ChunkLeases::ReplicaLost(
    ChunkLeases::WEntry& we,
    const ChunkServer*   chunkServer)
{
    WriteLease& wl = we;
    if (chunkServer == wl.chunkServer.get() && ! wl.relinquishedFlag &&
            ! wl.allocInFlight) {
        const time_t now = TimeNow();
        if (wl.stripedFileFlag && now <= wl.expires) {
            // Keep the valid lease for striped files, instead, to
            // allow lease renewal when/if the next chunk allocation
            // comes in.
            Renew(we, now);
        } else {
            Expire(we, now);
        }
        wl.ownerWasDownFlag = wl.ownerWasDownFlag ||
            (chunkServer && chunkServer->IsDown());
        wl.ResetServer();
    }
    return 0;
}

inline void
ChunkLeases::ServerDown(
    const ChunkServerPtr& chunkServer,
    ARAChunkCache&        arac,
    CSMap&                csmap)
{
    if (chunkServer->IsStoppedServicing() || chunkServer->IsReplay()) {
        return;
    }
    mWriteLeases.First();
    const WEntry* entry;
    while ((entry = mWriteLeases.Next())) {
        chunkId_t const   chunkId = entry->GetKey().first;
        const WriteLease& wl      = *entry;
        CSMap::Entry*     ci      = 0;
        if (wl.appendFlag &&
                (ci = csmap.Find(chunkId)) &&
                csmap.HasServer(chunkServer, *ci)) {
            arac.Invalidate(ci->GetFileId(), chunkId);
        }
        ReplicaLost(*const_cast<WEntry*>(entry), &*chunkServer);
    }
}

inline bool
ChunkLeases::ExpiredCleanup(
    ChunkLeases::REntry& re,
    time_t               now,
    CSMap&               csmap)
{
    bool                 updateFlag = mTimerRunningFlag;
    ChunkReadLeasesHead& rl         = re.Get();
    for (RLEntry* n = &RLEntry::List::GetNext(rl.mExpirationList); ;) {
        RLEntry& c = *n;
        if (&c == &rl.mExpirationList || now <= c.GetExpiration()) {
            break;
        }
        n = &RLEntry::List::GetNext(c);
        rl.mLeases.Erase(c.leaseId);
        updateFlag = true;
    }
    if (rl.mLeases.IsEmpty()) {
        Erase(re, csmap);
        return true;
    }
    if (updateFlag) {
        mReadLeaseTimer.Schedule(re, rl.GetExpiration());
    }
    return false;
}

inline bool
ChunkLeases::ExpiredCleanup(
    ChunkLeases::WEntry& we,
    time_t               now,
    int                  ownerDownExpireDelay,
    ARAChunkCache&       arac,
    CSMap&               csmap)
{
    const WriteLease& wl = we;
    if (wl.allocInFlight) {
        if (mTimerRunningFlag) {
            panic("lease with allocation in flight has expired");
        }
        return false;
    }
    const EntryKey      key = we.GetKey();
    CSMap::Entry* const ci  = key.IsChunkEntry() ?
        csmap.Find(key.first) : 0;
    if (key.IsChunkEntry() && ! ci) {
        panic("invalid stale write lease");
        Erase(we, -1);
        return true;
    }
    const time_t exp = wl.expires +
        ((wl.ownerWasDownFlag && ownerDownExpireDelay > 0) ?
        ownerDownExpireDelay : 0);
    if (now <= exp) {
        if (mTimerRunningFlag) {
            mWriteLeaseTimer.Schedule(we, exp);
        }
        return false;
    }
    const ChunkServerPtr chunkServer      =
        ci ? ChunkServerPtr() : wl.chunkServer;
    const bool           relinquishedFlag = ci && wl.relinquishedFlag;
    const seq_t          chunkVersion     = wl.chunkVersion;
    const string         pathname         = wl.pathname;
    const bool           appendFlag       = wl.appendFlag;
    const bool           stripedFileFlag  = wl.stripedFileFlag;
    Erase(we, ci ? ci->GetFileId() : key.first);
    if (relinquishedFlag) {
        UpdateReplicationState(csmap, *ci);
        return true;
    }
    if (! ci) {
        if (key.IsChunkEntry() || appendFlag) {
            panic("invalid object store block write lease");
        }
        if (chunkServer && ! chunkServer->IsDown()) {
            const bool       kHasChunkChecksum = false;
            const bool       kPendingAddFlag   = false;
            const chunkOff_t kChunkSize        = -1;
            chunkServer->MakeChunkStable(
                key.first, key.first, -chunkVersion - 1,
                kChunkSize, kHasChunkChecksum, 0, kPendingAddFlag);
        }
        return true;
    }
    if (appendFlag) {
        arac.Invalidate(ci->GetFileId(), ci->GetChunkId());
    }
    const bool leaseRelinquishFlag = true;
    gLayoutManager.MakeChunkStableInit(
        *ci,
        chunkVersion,
        pathname,
        appendFlag,
        -1,
        false,
        0,
        stripedFileFlag,
        appendFlag,
        leaseRelinquishFlag
    );
    return true;
}

inline bool
ChunkLeases::ExpiredCleanup(
    const ChunkLeases::EntryKey& key,
    time_t                       now,
    int                          ownerDownExpireDelay,
    ARAChunkCache&               arac,
    CSMap&                       csmap)
{
    REntry* const rl = mReadLeases.Find(key);
    if (rl) {
        assert(! mWriteLeases.Find(key));
        const bool ret = ExpiredCleanup(*rl, now, csmap);
        if (! ret && (key.IsChunkEntry() ?
                ! csmap.Find(key.first) :
                ! IsObjectStoreBlock(key.first, key.second))) {
            Erase(*rl, -1);
            return true;
        }
        return ret;
    }
    WEntry* const wl = mWriteLeases.Find(key);
    return (! wl || ExpiredCleanup(
        *wl, now, ownerDownExpireDelay, arac, csmap));
}

inline const char*
ChunkLeases::FlushWriteLease(
    const ChunkLeases::EntryKey& key,
    ARAChunkCache&               arac,
    CSMap&                       csmap)
{
    WEntry* const we = mWriteLeases.Find(key);
    if (! we) {
        return "no write lease";
    }
    const WriteLease& wl = *we;
    if (! wl.appendFlag) {
        return "not append lease";
    }
    if (wl.allocInFlight) {
        return "allocation in flight";
    }
    if (wl.relinquishedFlag || wl.ownerWasDownFlag) {
        return "write lease expiration in flight";
    }
    const time_t now = TimeNow();
    Expire(*we, now);
    if (ExpiredCleanup(*we, now, 0, arac, csmap)) {
        return 0;
    }
    return "write lease expiration delayed";
}

inline int
ChunkLeases::LeaseRelinquish(
    const MetaLeaseRelinquish& req,
    ARAChunkCache&             arac,
    CSMap&                     csmap)
{
    EntryKey const key(req.chunkId, req.chunkPos);
    REntry* const  re = mReadLeases.Find(key);
    if (re) {
        assert(! mWriteLeases.Find(key));
        ChunkReadLeasesHead& rl     = *re;
        ChunkReadLeases&     leases = rl.mLeases;
        ReadLease* const     le     = leases.Find(req.leaseId);
        if (! le) {
            return -EINVAL;
        }
        const int    ret = le->expires < TimeNow() ? -ELEASEEXPIRED : 0;
        const time_t exp = rl.GetExpiration();
        leases.Erase(req.leaseId);
        if (leases.IsEmpty()) {
            Erase(*re, csmap);
        } else {
            const time_t cexp = rl.GetExpiration();
            if (exp != cexp) {
                mReadLeaseTimer.Schedule(*re, exp);
            }
        }
        return ret;
    }
    if (req.fromClientSMFlag) {
        // only chunk servers are allowed to relinquish write leases;
        return -EPERM;
    }

    WEntry* const we = mWriteLeases.Find(key);
    if (! we || we->Get().leaseId != req.leaseId) {
        return -EINVAL;
    }
    const CSMap::Entry* const ci =
        key.IsChunkEntry() ? csmap.Find(key.first) : 0;
    if (key.IsChunkEntry()) {
        if (! ci) {
            return -ELEASEEXPIRED;
        }
    } else {
        if (! IsObjectStoreBlock(key.first, key.second)) {
            return -ELEASEEXPIRED;
        }
    }
    WriteLease& wl = *we;
    if (wl.allocInFlight) {
        // If relinquish comes in before alloc completes, then
        // run completion when / if allocation finishes successfully.
        if (! wl.allocInFlight->pendingLeaseRelinquish) {
            const_cast<MetaAllocate*>(wl.allocInFlight
                    )->pendingLeaseRelinquish =
                new MetaLeaseRelinquish();
        }
        MetaLeaseRelinquish& lr =
            *(wl.allocInFlight->pendingLeaseRelinquish);
        lr.leaseType        = req.leaseType;
        lr.chunkId          = req.chunkId;
        lr.chunkPos         = req.chunkPos;
        lr.leaseId          = req.leaseId;
        lr.chunkSize        = req.chunkSize;
        lr.hasChunkChecksum = req.hasChunkChecksum;
        lr.chunkChecksum    = req.chunkChecksum;
        return 0;
    }
    const time_t         now          = TimeNow();
    const int            ret          = wl.expires < now ? -ELEASEEXPIRED : 0;
    const bool           hadLeaseFlag = ! wl.relinquishedFlag;
    const ChunkServerPtr chunkServer  = ci ? ChunkServerPtr() : wl.chunkServer;
    wl.ResetServer();
    wl.relinquishedFlag = true;
    // the owner of the lease is giving up the lease; update the expires so
    // that the normal lease cleanup will work out.
    Expire(*we, now);
    if (hadLeaseFlag) {
        if (ci) {
            // For write append lease checksum and size always have to be
            // specified for make chunk stable, otherwise run begin make
            // chunk stable.
            const CSMap::Entry& v = *ci;
            const bool beginMakeChunkStableFlag = wl.appendFlag &&
                (! req.hasChunkChecksum || req.chunkSize < 0);
            if (wl.appendFlag) {
                arac.Invalidate(v.GetFileId(), req.chunkId);
            }
            const bool leaseRelinquishFlag = true;
            gLayoutManager.MakeChunkStableInit(
                v,
                wl.chunkVersion,
                wl.pathname,
                beginMakeChunkStableFlag,
                req.chunkSize,
                req.hasChunkChecksum,
                req.chunkChecksum,
                wl.stripedFileFlag,
                wl.appendFlag,
                leaseRelinquishFlag
            );
        } else if (chunkServer && ! chunkServer->IsDown()) {
            const bool kHasChunkChecksum = false;
            const bool kPendingAddFlag   = false;
            chunkServer->MakeChunkStable(
                key.first, key.first, -wl.chunkVersion - 1,
                req.chunkSize, kHasChunkChecksum, 0, kPendingAddFlag);
        }
    }
    return ret;
}

class ChunkLeases::LeaseCleanup
{
public:
    LeaseCleanup(
        time_t         now,
        int            ownerDownExpireDelay,
        ARAChunkCache& arac,
        CSMap&         csmap,
        ChunkLeases&   leases,
        int            maxInFlightEntriesCount)
        : mNow(now),
          mOwnerDownExpireDelay(ownerDownExpireDelay),
          mArac(arac),
          mCsmap(csmap),
          mLeases(leases),
          mMaxInFlightEntriesCount(maxInFlightEntriesCount)
        {}
    void operator()(
        REntry& inEntry)
        { mLeases.ExpiredCleanup(inEntry, mNow, mCsmap); }
    void operator()(
        WEntry& inEntry)
    {
        mLeases.ExpiredCleanup(
            inEntry,
            mNow,
            mOwnerDownExpireDelay,
            mArac,
            mCsmap
        );
    }
    void operator()(
        FEntry& inEntry)
    {
        // The entry must be removed from the timer list.
        // Log write completion will re-schedule if needed
        if (! mLeases.ScheduleRemoveFromDumpster(
                inEntry, mMaxInFlightEntriesCount)) {
            inEntry.Get().mCount = 0;
            FEntry::List::Insert(inEntry,
                FEntry::List::GetPrev((mLeases.mPendingDeleteList)));
        }
    }
private:
    time_t const   mNow;
    int const      mOwnerDownExpireDelay;
    ARAChunkCache& mArac;
    CSMap&         mCsmap;
    ChunkLeases&   mLeases;
    int            mMaxInFlightEntriesCount;
private:
    LeaseCleanup(const LeaseCleanup&);
    LeaseCleanup& operator=(const LeaseCleanup&);
};

inline bool
ChunkLeases::ScheduleRemoveFromDumpster(
    FEntry& entry,
    int     maxInFlightEntriesCount)
{
    FileEntry& fentry = entry.Get();
    if (fentry.mName.empty() || 1 < fentry.mCount || ! fentry.mFa) {
        panic("invalid file lease entry");
        return false;
    }
    if (maxInFlightEntriesCount <= mRemoveFromDumpsterInFlightCount) {
        return false;
    }
    if (! fentry.mFa || KFS_FILE != fentry.mFa->type) {
        panic("invalid file lease entry: invalid file attribute");
        return false;
    }
    const int cnt = (int)min(fentry.mFa->chunkcount() + 1,
        (int64_t)maxInFlightEntriesCount -
            mRemoveFromDumpsterInFlightCount);
    mRemoveFromDumpsterInFlightCount += cnt;
    fentry.mCount = 0;
    FEntry::List::Remove(entry);
    submit_request(
        new MetaRemoveFromDumpster(fentry.mName, entry.GetKey(), cnt));
    return true;
}

inline void
ChunkLeases::ProcessPendingDelete(
    int maxInFlightEntriesCount)
{
    for (; ;) {
        FEntry& entry = FEntry::List::GetNext(mPendingDeleteList);
        if (&mPendingDeleteList == &entry ||
                ! ScheduleRemoveFromDumpster(entry, maxInFlightEntriesCount)) {
            break;
        }
    }
}

inline void
ChunkLeases::Timer(
    time_t         now,
    int            ownerDownExpireDelay,
    ARAChunkCache& arac,
    CSMap&         csmap,
    int            maxDelete)
{
    if (mTimerRunningFlag) {
        return; // Do not allow recursion.
    }
    ProcessPendingDelete(maxDelete);
    mTimerRunningFlag = true;
    LeaseCleanup cleanup(now, ownerDownExpireDelay, arac, csmap, *this,
        maxDelete);
    mReadLeaseTimer.Run(now, cleanup);
    mWriteLeaseTimer.Run(now, cleanup);
    mDumpsterCleanupTimer.Run(now, cleanup);
    mTimerRunningFlag = false;
}

inline void
ChunkLeases::StopServicing(
    ARAChunkCache& arac,
    CSMap&         csmap)
{
    if (mTimerRunningFlag) {
        panic("invalid lease stop servicing invocation");
    }
    while (! mReadLeases.IsEmpty()) {
        mReadLeases.First();
        const REntry* re;
        while ((re = mReadLeases.Next())) {
            Erase(*const_cast<REntry*>(re), csmap);
        }
    }
    while (! mWriteLeases.IsEmpty()) {
        mWriteLeases.First();
        const WEntry* wep;
        while ((wep = mWriteLeases.Next())) {
            WEntry& we = *const_cast<WEntry*>(wep);
            const WriteLease& wl = we;
            fid_t             fid;
            const EntryKey    key = we.GetKey();
            if (wl.allocInFlight) {
                MetaAllocate& alloc =
                    *const_cast<MetaAllocate*>(wl.allocInFlight);
                alloc.stoppedServicingFlag = true;
                fid = alloc.fid;
            } else {
                CSMap::Entry* const ci  = key.IsChunkEntry() ?
                    csmap.Find(key.first) : 0;
                if (! ci && key.IsChunkEntry()) {
                    panic("invalid stale write lease");
                    Erase(we, -1);
                    continue;
                }
                fid = ci ? ci->GetFileId() : key.first;
            }
            if (wl.appendFlag) {
                arac.Invalidate(fid, key.first);
            }
            Erase(we, fid);
        }
    }
    const time_t now = TimeNow();
    for (; ;) {
        FEntry& entry = FEntry::List::GetNext(mPendingDeleteList);
        if (&mPendingDeleteList == &entry) {
            break;
        }
        FEntry::List::Remove(entry);
        entry.Get().mCount = 1;
        mDumpsterCleanupTimer.Schedule(entry, now);
    }
}

inline void
ChunkLeases::SetTimerNextRunTime()
{
    if (mTimerRunningFlag) {
        panic("invalid lease set next run time invocation");
        return;
    }
    const time_t nextTime = TimeNow() + kLeaseTimerResolutionSec;
    mReadLeaseTimer.SetNextRunTime(nextTime);
    mWriteLeaseTimer.SetNextRunTime(nextTime);
    mDumpsterCleanupTimer.SetNextRunTime(nextTime);
}

inline bool
ChunkLeases::NewReadLease(
    fid_t                        fid,
    const ChunkLeases::EntryKey& key,
    time_t                       expires,
    ChunkLeases::LeaseId&        leaseId)
{
    if (mWriteLeases.Find(key)) {
        assert(! mReadLeases.Find(key));
        return false;
    }
    // Keep list sorted by expiration time.
    bool          insertedFlag = false;
    REntry&              re    = *mReadLeases.Insert(
        key, REntry(key, ChunkReadLeasesHead()), insertedFlag);
    ChunkReadLeasesHead& h      = re;
    ChunkReadLeases&     leases = h.mLeases;
    const time_t         exp    =
        insertedFlag ? expires + 1 : h.GetExpiration();
    if (insertedFlag) {
        if (! IncrementFileLease(fid)) {
            // Already in pending delete queue.
            mReadLeases.Erase(key);
            return false;
        }
    }
    leaseId = NewReadLeaseId();
    insertedFlag = false;
    RLEntry&             rl     = *leases.Insert(
        leaseId, RLEntry(ReadLease(leaseId, expires)), insertedFlag);
    if (! insertedFlag) {
        panic("duplicate read lease id");
    }
    h.PutInExpirationList(rl);
    if (expires < exp) {
        mReadLeaseTimer.Schedule(re, expires);
    }
    return true;
}

inline bool
ChunkLeases::NewWriteLease(
    MetaAllocate& req)
{
    EntryKey const key(
        0 < req.numReplicas ? req.chunkId    : req.fid,
        0 < req.numReplicas ? chunkOff_t(-1) : req.offset);
    if (mReadLeases.Find(key)) {
        assert(! mWriteLeases.Find(key));
        return false;
    }
    const LeaseId id = NewWriteLeaseId();
    WriteLease const wl(
        id,
        GetInitialWriteLeaseExpireTime(),
        req
    );
    bool insertedFlag = false;
    WEntry* const l = mWriteLeases.Insert(
        key, WEntry(key, wl), insertedFlag);
    req.leaseId       = l->Get().leaseId;
    req.leaseDuration = req.authUid != kKfsUserNone ?
        l->Get().endTime - TimeNow() : int64_t(-1);
    if (insertedFlag) {
        if (! IncrementFileLease(req.fid)) {
            mWriteLeases.Erase(key);
            return false;
        }
        PutInExpirationList(*l);
    }
    return insertedFlag;
}

inline int
ChunkLeases::Renew(
    fid_t                        fid,
    const ChunkLeases::EntryKey& key,
    ChunkLeases::LeaseId         leaseId,
    bool                         allocDoneFlag /* = false */,
    const MetaFattr*             fa            /* = 0 */,
    MetaLeaseRenew*              req           /* = 0 */)
{
    if (IsReadLease(leaseId)) {
        REntry* const rl = mReadLeases.Find(key);
        if (! rl) {
            return -EINVAL;
        }
        assert(! mWriteLeases.Find(key));
        ChunkReadLeasesHead& h      = *rl;
        ChunkReadLeases&     leases = h.mLeases;
        RLEntry* const       cl     = leases.Find(leaseId);
        if (! cl) {
            return -EINVAL;
        }
        const time_t now = TimeNow();
        if (cl->expires < now) {
            leases.Erase(leaseId);
            if (leases.IsEmpty()) {
                Erase(*rl, fid);
            }
            return -ELEASEEXPIRED;
        }
        const time_t exp = now + LEASE_INTERVAL_SECS;
        if (cl->expires != exp) {
            cl->expires = exp;
            if (1 < leases.GetSize()) {
                h.PutInExpirationList(*cl);
            }
            if (&RLEntry::List::GetNext(h.mExpirationList) == cl) {
                mReadLeaseTimer.Schedule(*rl, cl->expires);
            }
        }
        return 0;
    }
    WEntry* const we = mWriteLeases.Find(key);
    if (! we || we->Get().leaseId != leaseId) {
        return -EINVAL;
    }
    WriteLease& wl = *we;
    assert(! mReadLeases.Find(key));
    const time_t now = TimeNow();
    if (wl.expires < now && ! wl.allocInFlight) {
        // Don't renew expired leases, and let the timer to clean it up
        // to avoid posible recursion.
        return -ELEASEEXPIRED;
    }
    if (allocDoneFlag) {
        wl.allocInFlight = 0;
        wl.expires       = now - 1; // Force move into expiration list.
    }
    if (fa && ! fa->CanWrite(wl.euser, wl.egroup)) {
        Expire(*we, now);
        return -EPERM;
    }
    if (req && req->authUid != kKfsUserNone) {
        if (wl.endTime < now) {
            req->statusMsg = "authentication has expired";
            Expire(*we, now);
            return -EPERM;
        }
        if (0 < wl.delegationValidForTime &&
                gNetDispatch.IsCanceled(
                    wl.delegationIssuedTime + wl.delegationValidForTime,
                    wl.delegationIssuedTime,
                    wl.delegationUser,
                    wl.delegationSeq,
                    wl.delegationFlags)) {
            req->statusMsg = "delegation canceled";
            Expire(*we, now);
            return -EPERM;
        }
    }
    if (! wl.allocInFlight) {
        Renew(*we, now);
    }
    return 0;
}

inline bool
ChunkLeases::DeleteWriteLease(
    fid_t                        fid,
    const ChunkLeases::EntryKey& key,
    ChunkLeases::LeaseId         leaseId)
{
    WEntry* const we = mWriteLeases.Find(key);
    if (! we || we->Get().leaseId != leaseId) {
        return false;
    }
    Erase(*we, fid);
    return true;
}

inline bool
ChunkLeases::Delete(
    fid_t                        fid,
    const ChunkLeases::EntryKey& key)
{
    WEntry* const wl = mWriteLeases.Find(key);
    const bool hadWr = wl != 0;
    if (hadWr) {
        Erase(*wl, fid);
    }
    REntry* const rl = mReadLeases.Find(key);
    const bool hadRd = rl != 0;
    if (hadRd) {
        Erase(*rl, fid);
    }
    assert(! hadWr || ! hadRd);
    return (hadWr || hadRd);
}

inline void
ChunkLeases::ChangeFileId(
    chunkId_t chunkId,
    fid_t     fidFrom,
    fid_t     fidTo)
{
    const EntryKey      key(chunkId);
    const WEntry* const wl = mWriteLeases.Find(key);
    if (wl) {
        const MetaAllocate* const alloc = wl->Get().allocInFlight;
        if (alloc && alloc->initialChunkVersion < 0) {
            panic("internal error: attempt to change file id"
                " while initial chunk allocation is in flight");
        }
    }
    if (wl || mReadLeases.Find(key)) {
        DecrementFileLease(fidFrom);
        IncrementFileLease(fidTo);
    }
}

class ChunkLeases::OpenFileLister
{
public:
    OpenFileLister(
        MetaOpenFiles::ReadInfo&  openForRead,
        MetaOpenFiles::WriteInfo& openForWrite,
        const CSMap&              csmap,
        time_t                    now)
        : mOpenForRead(openForRead),
          mOpenForWrite(openForWrite),
          mCsmap(csmap),
          mNow(now)
        {}
    void operator()(
        const ChunkLeases::REntry& inEntry)
    {
        fid_t fid;
        if (inEntry.GetKey().IsChunkEntry()) {
            const CSMap::Entry* const ci = mCsmap.Find(inEntry.GetKey().first);
            if (! ci) {
                return;
            }
            fid = ci->GetFileId();
        } else {
            fid = inEntry.GetKey().first;
        }
        size_t         count = 0;
        const RLEntry& list  = inEntry.Get().mExpirationList;
        const RLEntry* rl    = &list;
        while ((rl = &RLEntry::List::GetPrev(*rl)) != &list &&
                mNow <= rl->expires) {
            count++;
        }
        if (0 < count) {
            mOpenForRead[fid].push_back(
                make_pair(inEntry.GetKey().IsChunkEntry() ?
                    inEntry.GetKey().first : -inEntry.GetKey().second, count));
        }
    }
    void operator()(
        const ChunkLeases::WEntry& inEntry)
    {
        if (inEntry.GetKey().IsChunkEntry()) {
            const CSMap::Entry* const ci = mCsmap.Find(inEntry.GetKey().first);
            if (ci) {
                mOpenForWrite[ci->GetFileId()
                    ].push_back(inEntry.GetKey().first);
            }
        } else {
            if (IsObjectStoreBlock(
                    inEntry.GetKey().first, inEntry.GetKey().second)) {
                mOpenForWrite[inEntry.GetKey().first
                    ].push_back(-inEntry.GetKey().second - 1);
            }
        }
    }
private:
    MetaOpenFiles::ReadInfo&  mOpenForRead;
    MetaOpenFiles::WriteInfo& mOpenForWrite;
    const CSMap&              mCsmap;
    const time_t              mNow;
private:
    OpenFileLister(const OpenFileLister&);
    OpenFileLister& operator=(const OpenFileLister&);
};

inline void
ChunkLeases::GetOpenFiles(
    MetaOpenFiles::ReadInfo&  openForRead,
    MetaOpenFiles::WriteInfo& openForWrite,
    const CSMap&              csmap) const
{
    OpenFileLister lister(openForRead, openForWrite, csmap, TimeNow());
    mReadLeaseTimer.Apply(lister);
    mWriteLeaseTimer.Apply(lister);
    const WEntry* we = &mWAllocationInFlightList;
    while((we = &WEntry::List::GetPrev(*we)) != &mWAllocationInFlightList) {
        if (we->GetKey().IsChunkEntry()) {
            const CSMap::Entry* const ci = csmap.Find(we->GetKey().first);
            if (ci) {
                openForWrite[ci->GetFileId()].push_back(we->GetKey().first);
            }
        } else {
            if (IsObjectStoreBlock(
                    we->GetKey().first, we->GetKey().second)) {
                openForWrite[we->GetKey().first
                    ].push_back(-we->GetKey().second);
            }
        }
    }
}

inline void
ChunkLeases::ScheduleDumpsterCleanup(
    const MetaFattr& inFa,
    const string&    inName)
{
    bool          insertedFlag = false;
    FEntry* const entry        = mFileLeases.Insert(
        inFa.id(), FEntry(inFa.id(), FileEntry(inName)), insertedFlag);
    if (! insertedFlag) {
        if (entry->Get().mName.empty()) {
            entry->Get().mName = inName;
        } else if (inName != entry->Get().mName) {
            panic("invalid file lease entry");
        }
        if (entry->Get().mFa && &inFa != entry->Get().mFa) {
            panic("invalid file lease entry attribute");
        }
        if (entry->Get().mCount <= 1) {
            panic("invalid file lease entry count");
        }
    }
    KFS_LOG_STREAM_DEBUG <<
        "scheduled dumpster cleanup:"
        " fid: "   << inFa.id() <<
        " name: "  << inName <<
        " count: " << entry->Get().mCount <<
    KFS_LOG_EOM;
    entry->Get().mFa = &inFa;
    if (entry->Get().mCount <= 1) {
        mDumpsterCleanupTimer.Schedule(
            *entry, TimeNow() + mDumpsterCleanupDelaySec);
    }
}

inline int
ChunkLeases::Handle(
    MetaRemoveFromDumpster& op,
    int                     maxInFlightEntriesCount)
{
    if (op.replayFlag) {
        if (0 != op.status) {
            panic("invalid remove from dumpster completion status in replay");
            return -1;
        }
    } else {
        if (mRemoveFromDumpsterInFlightCount < op.entriesCount) {
            panic("internal error: invalid remove from dumpster completion");
            return -1;
        }
        mRemoveFromDumpsterInFlightCount -= op.entriesCount;
    }
    FEntry* const entry = mFileLeases.Find(op.fid);
    if (! entry || 1 < entry->Get().mCount ||
            entry->Get().mName != op.name) {
        panic("invalid remove from dumpster completion");
        return -1;
    }
    KFS_LOG_STREAM_DEBUG <<
        "status: " << op.status <<
        " "        << op.Show() <<
    KFS_LOG_EOM;
    if (0 != op.status) {
        entry->Get().mCount = 1;
        mDumpsterCleanupTimer.Schedule(*entry, TimeNow());
    } else {
        if (op.cleanupDoneFlag) {
            mFileLeases.Erase(op.fid);
        } else if (! op.replayFlag &&
                ! ScheduleRemoveFromDumpster(*entry, maxInFlightEntriesCount)) {
            entry->Get().mCount = 0;
            FEntry::List::Insert(*entry,
                FEntry::List::GetPrev((mPendingDeleteList)));
        }
    }
    return ((! op.replayFlag && FEntry::List::IsInList(mPendingDeleteList)) ?
        mRemoveFromDumpsterInFlightCount : -1);
}

inline void
ChunkLeases::Start(MetaRename& req)
{
    if (0 != req.status || metatree.getDumpsterDirId() != req.dir) {
        return;
    }
    // The source must exist at the beginning of the RPC execution, i.e. the
    // file must already be in the dumpster. In other words, any possibly
    // already pending in the log queue RPC(s) (remove) that can potentially
    // successfully move the file into dumpster, are effectively ignored in
    // order to reduce complexity.
    MetaFattr* fa = 0;
    if (0 != (req.status = metatree.lookup(
            req.dir, req.oldname, req.euser, req.egroup, fa))) {
        return;
    }
    if (metatree.getChunkDeleteQueue() == fa || KFS_FILE != fa->type) {
        // Do not allow delete queue and sub directory removal, even though at
        // the moment of writing dumpster should have no sub directories.
        req.status = -EPERM;
        return;
    }
    FEntry* const entry = mFileLeases.Find(fa->id());
    if (! entry || req.oldname != entry->Get().mName ||
            entry->Get().mFa != fa) {
        const char* const msg = "internal error: invalid dumpster entry";
        panic(msg);
        req.status    = -EFAULT;
        req.statusMsg = msg;
        return;
    }
    if (entry->Get().mCount <= 0) {
        req.status    = -EPERM;
        req.statusMsg = "delete already in progress";
        return;
    }
    IncrementFileLease(*entry);
    req.leaseFileEntry = entry;
}

inline void
ChunkLeases::Done(MetaRename& req)
{
    if (! req.leaseFileEntry) {
        if (req.replayFlag && 0 == req.status &&
                metatree.getDumpsterDirId() == req.dir &&
                mFileLeases.Erase(req.srcFid) <= 0) {
            panic("internal error: invalid dumpster entry rename completion"
                " in replay");
        }
        return;
    }
    FEntry& entry = *reinterpret_cast<FEntry*>(req.leaseFileEntry);
    req.leaseFileEntry = 0;
    if (entry.Get().mCount <= 1) {
        const char* const msg =
            "internal error: invalid dumpster entry rename completion";
        panic(msg);
        req.status    = -EFAULT;
        req.statusMsg = msg;
        return;
    }
    if (0 == req.status) {
        // Moved out of the dumpster -- clear attribute and name.
        entry.Get().mFa = 0;
        entry.Get().mName.clear();
    }
    DecrementFileLease(entry);
}

inline LayoutManager::Servers::const_iterator
LayoutManager::FindServer(const ServerLocation& loc) const
{
    Servers::const_iterator const it = lower_bound(
        mChunkServers.begin(), mChunkServers.end(),
        loc, bind(&ChunkServer::GetServerLocation, _1) < loc
    );
    return ((it == mChunkServers.end() || (*it)->GetServerLocation() == loc) ?
        it : mChunkServers.end());
}

template<typename T>
inline LayoutManager::Servers::const_iterator
LayoutManager::FindServerByHost(const T& host) const
{
    const ServerLocation loc(host, -1);
    Servers::const_iterator const it = lower_bound(
        mChunkServers.begin(), mChunkServers.end(),
        boost::ref(loc),
        bind(&ChunkServer::GetServerLocation, _1) < boost::ref(loc)
    );
    return ((it == mChunkServers.end() ||
            (*it)->GetServerLocation().hostname == loc.hostname) ?
        it : mChunkServers.end());
}

inline LayoutManager::Servers::const_iterator
LayoutManager::FindServerForReq(const MetaRequest& req)
{
    LayoutManager::Servers::const_iterator it = req.clientIp.empty() ?
        mChunkServers.end() : FindServerByHost(req.clientIp);
    if (mChunkServers.end() == it && ! req.clientReportedIp.empty() &&
            req.clientIp != req.clientReportedIp) {
        it = FindServerByHost(req.clientReportedIp);
    }
    return it;
}

template <typename T> bool
LayoutManager::GetAccessProxyFromReq(T& req, LayoutManager::Servers& servers)
{
    ServerLocation loc;
    if (! loc.FromString(
            req.chunkServerName.data(), req.chunkServerName.size(),
            req.shortRpcFormatFlag)) {
        req.statusMsg = "invalid chunk server name";
        req.status    = -EINVAL;
        return false;
    }
    Servers::const_iterator const it = FindServer(loc);
    if (it == mChunkServers.end()) {
        req.statusMsg = "no proxy on host: " + loc.ToString();
        return false;
    }
    servers.push_back(*it);
    return true;
}

template<typename T> bool
LayoutManager::HandleReplay(T& req)
{
    if (! req.replayFlag) {
        return IsMetaLogWriteOrVrError(req.status);
    }
    if (req.server || ! req.location.IsValid() || 0 != req.status) {
        panic("invalid RPC in replay");
        req.status = -EFAULT;
        return true;
    }
    Servers::const_iterator const it = FindServer(req.location);
    if (mChunkServers.end() == it) {
        KFS_LOG_STREAM_DEBUG <<
            "no chunk server: " << req.location <<
            " " << req.Show() <<
        KFS_LOG_EOM;
        req.status = -ENOENT;
        return true;
    }
    req.server = *it;
    return false;
}

bool
LayoutManager::FindAccessProxy(MetaAllocate& req)
{
    req.servers.clear();
    if (req.chunkServerName.empty()) {
        Servers::const_iterator const it = mObjectStorePlacementTestFlag ?
            mChunkServers.end() : FindServerForReq(req);
        if (it == mChunkServers.end()) {
            if (mObjectStoreWriteCanUsePoxoyOnDifferentHostFlag) {
                if (FindAccessProxyFor(req, req.servers)) {
                    return true;
                }
                req.statusMsg = "no access proxy available";
                return false;
            } else  {
                req.statusMsg = "no access proxy on host: " + req.clientIp;
                return false;
            }
        }
        req.servers.push_back(*it);
    } else if (! GetAccessProxyFromReq(req, req.servers)) {
        return false;
    }
    return true;
}

inline CSMap::Entry&
LayoutManager::GetCsEntry(MetaChunkInfo& chunkInfo)
{
    return CSMap::Entry::GetCsEntry(chunkInfo);
}

inline CSMap::Entry*
LayoutManager::GetCsEntry(MetaChunkInfo* chunkInfo)
{
    return CSMap::Entry::GetCsEntry(chunkInfo);
}

inline void
LayoutManager::UpdatePendingRecovery(CSMap::Entry& ent)
{
    KFS::UpdatePendingRecovery(mChunkToServerMap, ent);
}

inline bool
LayoutManager::AddHosted(CSMap::Entry& entry, const ChunkServerPtr& c,
    size_t* srvCount)
{
    // Schedule replication even if the server went down, let recovery
    // logic decide what to do.
    UpdatePendingRecovery(entry);
    if (! c || c->IsDown()) {
        return false;
    }
    if (c->IsEvacuationScheduled(entry.GetChunkId())) {
        CheckReplication(entry);
    }
    const bool retFlag = mChunkToServerMap.AddServer(c, entry, srvCount);
    KFS_LOG_STREAM_DEBUG <<
        "+srv: "     << c->GetServerLocation() <<
        " chunk: "   << entry.GetChunkId() <<
        " version: " << entry.GetChunkVersion() <<
        (c->IsReplay() ? " replay" : "") <<
        " added: "   << retFlag <<
    KFS_LOG_EOM;
    return retFlag;
}

inline bool
LayoutManager::AddHosted(
    chunkId_t chunkId, CSMap::Entry& entry, const ChunkServerPtr& c)
{
    if (chunkId != entry.GetChunkId()) {
        panic("add hosted chunk id mismatch");
        return false;
    }
    return AddHosted(entry, c);
}

inline void
LayoutManager::UpdateReplicationState(CSMap::Entry& entry)
{
    KFS::UpdateReplicationState(mChunkToServerMap, entry);
}

inline void
LayoutManager::SetReplicationState(CSMap::Entry& entry,
    CSMap::Entry::State state)
{
    CSMap::Entry::State const curState = mChunkToServerMap.GetState(entry);
    if (curState == state) {
        return;
    }
    if (curState == CSMap::Entry::kStatePendingRecovery) {
        // Re-schedule replication check if needed.
        UpdatePendingRecovery(entry);
    }
    mChunkToServerMap.SetState(entry, state);
}

inline void
LayoutManager::CheckReplication(CSMap::Entry& entry)
{
    SetReplicationState(entry, CSMap::Entry::kStateCheckReplication);
}

inline seq_t
LayoutManager::GetChunkVersionRollBack(chunkId_t chunkId)
{
    seq_t* const it = mChunkVersionRollBack.Find(chunkId);
    if (it) {
        if (*it <= 0) {
            ostringstream& os = GetTempOstream();
            os <<
            "invalid chunk roll back entry:"
            " chunk: "             << chunkId <<
            " version increment: " << *it;
            const string msg = os.str();
            panic(msg.c_str());
            mChunkVersionRollBack.Erase(chunkId);
        } else {
            return *it;
        }
    }
    return 0;
}

inline seq_t
LayoutManager::IncrementChunkVersionRollBack(chunkId_t chunkId)
{
    bool insertedFlag = false;
    seq_t* const res = mChunkVersionRollBack.Insert(chunkId, 0, insertedFlag);
    if (! insertedFlag && *res <= 0) {
        ostringstream& os = GetTempOstream();
        os <<
        "invalid chunk roll back entry:"
        " chunk: "             << chunkId <<
        " version increment: " << *res;
        const string msg = os.str();
        panic(msg.c_str());
        *res = 0;
    }
    ++(*res);
    return *res;
}

template<typename T>
inline void
LayoutManager::UpdateATimeSelf(int64_t updateResolutionUsec,
    const MetaFattr* fa, T& req)
{
    if (req.fromChunkServerFlag || updateResolutionUsec < 0 ||
            req.submitTime <= fa->atime + updateResolutionUsec) {
        return;
    }
    req.atimeInFlightFlag = true;
    req.suspended = true;
    bool insertedFlag = false;
    MetaSetATime** const op =
        mSetATimeInFlight.Insert(fa->id(), 0, insertedFlag);
    if (insertedFlag) {
        *op = new MetaSetATime(fa->id(), req.submitTime, &req);
        submit_request(*op);
    } else {
        (*op)->waitQueue.PushBack(req);
    }
}

int64_t
LayoutManager::Rand(int64_t interval)
{
    return (int64_t)(mRandom.Rand() % interval);
}

LayoutManager::ChunkPlacement::ChunkPlacement(LayoutManager* layoutManager)
    : Super(layoutManager ? *layoutManager : gLayoutManager)
{
    Reserve(512);
}

const bool kClientAuthAllowPskFlag      = true;
// By default allow web ui access from local host without authentication.
const int kClientDefaultNoAuthMetaOps[] = {
    META_PING,
    META_GET_CHUNK_SERVERS_COUNTERS,
    META_GET_CHUNK_SERVER_DIRS_COUNTERS,
    META_GET_REQUEST_COUNTERS,
    META_DISCONNECT,
    META_VR_GET_STATUS,
    META_NUM_OPS_COUNT // Sentinel
};
const char* const kClientDefaultNoAuthMetaOpsHosts[] = {
    "127.0.0.1",
    0 // Sentinel
};
const bool kCSAuthenticationUsesServerPskFlag = false;

LayoutManager::LayoutManager()
    : mNetManager(globalNetManager()),
      mNumOngoingReplications(0),
      mIsRebalancingEnabled(true),
      mMaxRebalanceSpaceUtilThreshold(0.85),
      mMinRebalanceSpaceUtilThreshold(0.75),
      mIsExecutingRebalancePlan(false),
      mRecoveryStartTime(0),
      mStartTime(time(0)),
      mRecoveryIntervalSec(25),
      mLeaseCleanerOtherIntervalSec(60),
      mLeaseCleanerOtherNextRunTime(TimeNow()),
      mLeaseCleaner(ChunkLeases::kLeaseTimerResolutionSec * 1000),
      mChunkReplicator(5 * 1000),
      mCheckpoint(5 * 1000),
      mMinChunkserversToExitRecovery(1),
      mChunkServers(),
      mHibernatingServers(),
      mDownServers(),
      mRacks(),
      mChunkToServerMap(),
      mStripedFilesAllocationsInFlight(),
      mChunkLeases(TimeNow()),
      mARAChunkCache(),
      mNonStableChunks(),
      mPendingBeginMakeStable(),
      mPendingMakeStable(),
      mChunkVersionRollBack(),
      mSetATimeInFlight(),
      mMastersCount(0),
      mSlavesCount(0),
      mAssignMasterByIpFlag(false),
      mLeaseOwnerDownExpireDelay(30),
      mMaxDumpsterCleanupInFlight(256),
      mMaxTruncateChunksDeleteCount(36),
      mMaxTruncateChunksQueueCount(1 << 20),
      mMaxTruncatedChunkDeletesInFlight(256),
      mTruncatedChunkDeletesInFlight(0),
      mWasServicingFlag(false),
      mMaxReservationSize(4 << 20),
      mReservationDecayStep(4), // decrease by factor of 2 every 4 sec
      mChunkReservationThreshold(CHUNKSIZE),
      mAllocAppendReuseInFlightTimeoutSec(25),
      mMinAppendersPerChunk(96),
      mMaxAppendersPerChunk(4 << 10),
      mReservationOvercommitFactor(1.0),
      mServerDownReplicationDelay(MetaBye::kDefaultReplicationDelay),
      mMaxDownServersHistorySize(4 << 10),
      mChunkServersProps(),
      mCSToRestartCount(0),
      mMastersToRestartCount(0),
      mMaxCSRestarting(0),
      mRetireOnCSRestartFlag(true),
      mMaxCSUptime(int64_t(24) * 60 * 60 * 36500),
      mCSRestartTime(TimeNow() + int64_t(24) * 60 * 60 * 36500),
      mCSGracefulRestartTimeout(15 * 60),
      mCSGracefulRestartAppendWithWidTimeout(40 * 60),
      mLastReplicationCheckTime(numeric_limits<int64_t>::min()), // check all
      mLastRecomputeDirsizeTime(TimeNow()),
      mRecomputeDirSizesIntervalSec(0),
      mMaxConcurrentWriteReplicationsPerNode(5),
      mMaxConcurrentReadReplicationsPerNode(10),
      mUseEvacuationRecoveryFlag(true),
      // Replication check 30ms/.20-30ms = 120 -- 20% cpu when idle
      mMaxTimeForChunkReplicationCheck(30 * 1000),
      mMinChunkReplicationCheckInterval(120 * 1000),
      mLastReplicationCheckRunEndTime(microseconds()),
      mReplicationCheckTimeouts(0),
      mNoServersAvailableForReplicationCount(0),
      mFullReplicationCheckInterval(
          int64_t(7) * 24 * 60 * 60 * kSecs2MicroSecs),
      mCheckAllChunksInProgressFlag(false),
      mConcurrentWritesPerNodeWatermark(10),
      mMaxSpaceUtilizationThreshold(0.95),
      mUseFsTotalSpaceFlag(true),
      mChunkAllocMinAvailSpace(2 * (int64_t)CHUNKSIZE),
      mCompleteReplicationCheckInterval(30 * kSecs2MicroSecs),
      mCompleteReplicationCheckTime(
          microseconds() - mCompleteReplicationCheckInterval),
      mPastEofRecoveryDelay(int64_t(60) * 6 * 60 * kSecs2MicroSecs),
      mMaxServerCleanupScan(2 << 10),
      mMaxRebalanceScan(1024),
      mRebalanceReplicationsThreshold(0.5),
      mRebalanceReplicationsThresholdCount(0),
      mMaxRebalanceRunTime(int64_t(30) * 1000),
      mLastRebalanceRunTime(microseconds()),
      mRebalanceRunInterval(int64_t(512) * 1024),
      mMaxRebalancePlanRead(2048),
      mRebalancePlanFileName(),
      mRebalanceCtrs(),
      mRebalancePlan(),
      mCleanupScheduledFlag(false),
      mPrimaryFlag(true),
      mCSCountersUpdateInterval(2),
      mCSCountersUpdateTime(0),
      mCSCountersResponse(),
      mCSDirCountersUpdateInterval(10),
      mCSDirCountersUpdateTime(0),
      mCSDirCountersResponse(),
      mPingUpdateInterval(2),
      mPingUpdateTime(0),
      mPingResponse(),
      mWOstream(),
      mBufferPool(0),
      mMightHaveRetiringServersFlag(false),
      mRackPrefixUsePortFlag(false),
      mUseCSRackAssignmentFlag(false),
      mReplaySetRackFlag(false),
      mRackPrefixes(),
      mRackWeights(),
      mChunkServerMd5sums(),
      mClusterKey(),
      mDelayedRecoveryUpdateMaxScanCount(32),
      mForceDelayedRecoveryUpdateFlag(false),
      mSortCandidatesBySpaceUtilizationFlag(false),
      mSortCandidatesByLoadAvgFlag(false),
      mMaxFsckFiles(128 << 10),
      mFsckAbandonedFileTimeout(int64_t(1000) * kSecs2MicroSecs),
      mMaxFsckTime(int64_t(19) * 60 * kSecs2MicroSecs),
      mFullFsckFlag(true),
      mDirATimeUpdateResolution(-1),
      mATimeUpdateResolution(-1),
      mMTimeUpdateResolution(kSecs2MicroSecs),
      mMaxPendingRecoveryMsgLogInfo(1 << 10),
      mAllowLocalPlacementFlag(true),
      mAllowLocalPlacementForAppendFlag(false),
      mInRackPlacementForAppendFlag(false),
      mInRackPlacementFlag(false),
      mAppendPlacementIgnoreMasterSlaveFlag(true),
      mAllocateDebugVerifyFlag(false),
      mChunkEntryToChange(0),
      mFattrToChangeTo(0),
      mCSLoadAvgSum(0),
      mCSMasterLoadAvgSum(0),
      mCSSlaveLoadAvgSum(0),
      mCSTotalLoadAvgSum(0),
      mCSOpenObjectCount(0),
      mCSWritableObjectCount(0),
      mCSTotalPossibleCandidateCount(0),
      mCSMasterPossibleCandidateCount(0),
      mCSSlavePossibleCandidateCount(0),
      mUpdateCSLoadAvgFlag(false),
      mUpdatePlacementScaleFlag(false),
      mCSMaxGoodCandidateLoadAvg(0),
      mCSMaxGoodMasterCandidateLoadAvg(0),
      mCSMaxGoodSlaveCandidateLoadAvg(0),
      mCSMaxGoodCandidateLoadRatio(4),
      mCSMaxGoodMasterLoadRatio(4),
      mCSMaxGoodSlaveLoadRatio(4),
      mSlavePlacementScale(int64_t(1) << kSlaveScaleFracBits),
      mMaxSlavePlacementRange(
          (int64_t)(1.8 * (int64_t(1) << kSlaveScaleFracBits))),
      mMaxReplicasPerFile(MAX_REPLICAS_PER_FILE),
      mMaxReplicasPerRSFile(MAX_REPLICAS_PER_FILE),
      mGetAllocOrderServersByLoadFlag(true),
      mMinChunkAllocClientProtoVersion(-1),
      mMaxResponseSize(256 << 20),
      mMinIoBufferBytesToProcessRequest(mMaxResponseSize + (10 << 20)),
      mReadDirLimit(8 << 10),
      mAllowChunkServerRetireFlag(false),
      mPanicOnInvalidChunkFlag(false),
      mPanicOnRemoveFromPlacementFlag(false),
      mAppendCacheCleanupInterval(-1),
      mTotalChunkWrites(0),
      mTotalWritableDrives(0),
      mMinWritesPerDrive(10),
      mMaxWritesPerDrive(4 << 10),
      mMaxWritesPerDriveThreshold(mMinWritesPerDrive),
      mMaxWritesPerDriveRatio(1.5),
      mMaxLocalPlacementWeight(1.0),
      mTotalWritableDrivesMult(0.),
      mConfig(),
      mConfigParameters(),
      mDefaultUser(kKfsUserNone),      // Request defaults
      mDefaultGroup(kKfsGroupNone),
      mDefaultFileMode(0644),
      mDefaultDirMode(0755),
      mDefaultLoadUser(kKfsUserRoot),  // Checkpoint load and replay defaults
      mDefaultLoadGroup(kKfsGroupRoot),
      mDefaultLoadFileMode(0666),
      mDefaultLoadDirMode(0777),
      mForceEUserToRootFlag(false),
      mVerifyAllOpsPermissionsParamFlag(false),
      mVerifyAllOpsPermissionsFlag(false),
      mRootHosts(),
      mHostUserGroupRemap(),
      mLastUidGidRemap(),
      mIoBufPending(0),
      mAuthCtxUpdateCount(0),
      mClientAuthContext(
          kClientAuthAllowPskFlag,
          kClientDefaultNoAuthMetaOps,
          kClientDefaultNoAuthMetaOpsHosts),
      mCSAuthContext(kCSAuthenticationUsesServerPskFlag),
      mUserAndGroup(),
      mClientCSAuthRequiredFlag(false),
      mClientCSAllowClearTextFlag(false),
      mCSAccessValidForTimeSec(2 * 60 * 60),
      mMinWriteLeaseTimeSec(LEASE_INTERVAL_SECS),
      mFileSystemIdRequiredFlag(false),
      mDeleteChunkOnFsIdMismatchFlag(false),
      mChunkAvailableUseReplicationOrRecoveryThreshold(-1),
      mObjectStoreEnabledFlag(false),
      mObjectStoreReadCanUsePoxoyOnDifferentHostFlag(false),
      mObjectStoreWriteCanUsePoxoyOnDifferentHostFlag(false),
      mObjectStorePlacementTestFlag(false),
      mCreateFileTypeExclude(),
      mMaxDataStripeCount(KFS_MAX_DATA_STRIPE_COUNT),
      mMaxRecoveryStripeCount(min(32, KFS_MAX_RECOVERY_STRIPE_COUNT)),
      mMaxRSDataStripeCount(min(64, KFS_MAX_DATA_STRIPE_COUNT)),
      mDebugSimulateDenyHelloResumeInterval(0),
      mDebugPanicOnHelloResumeFailureCount(-1),
      mHelloResumeFailureTraceFileName(),
      mFileRecoveryInFlightCount(),
      mIdempotentRequestTracker(),
      mResubmitQueue(),
      mObjStoreDeleteMaxSchedulePerRun(16 << 10),
      mObjStoreMaxDeletesPerServer(128),
      mObjStoreDeleteDelay(2 * LEASE_INTERVAL_SECS),
      mResubmitClearObjectStoreDeleteFlag(false),
      mObjStoreDeleteSrvIdx(0),
      mObjStoreFilesDeleteQueue(),
      mObjBlocksDeleteRequeue(),
      mObjBlocksDeleteInFlight(),
      mRestoreChunkServerPtr(),
      mRestoreHibernatedCSPtr(),
      mReplayServerCount(0),
      mDisconnectedCount(0),
      mServiceStartTime(TimeNow() - 10 * 24 * 60 * 60),
      mCleanupFlag(false),
      mChunkInfosTmp(),
      mChunkInfos2Tmp(),
      mServersTmp(),
      mServers2Tmp(),
      mServers3Tmp(),
      mServers4Tmp(),
      mPlacementTiersTmp(),
      mChunkPlacementTmp(this),
      mRandom(),
      mTempOstream()
{
    mReplicationTodoStats    = new Counter("Num Replications Todo");
    mOngoingReplicationStats = new Counter("Num Ongoing Replications");
    mTotalReplicationStats   = new Counter("Total Num Replications");
    mFailedReplicationStats  = new Counter("Num Failed Replications");
    mStaleChunkCount         = new Counter("Num Stale Chunks");
    // how much to be done before we are done
    globals().counterManager.AddCounter(mReplicationTodoStats);
    // how many chunks are "endangered"
    // how much are we doing right now
    globals().counterManager.AddCounter(mOngoingReplicationStats);
    globals().counterManager.AddCounter(mTotalReplicationStats);
    globals().counterManager.AddCounter(mFailedReplicationStats);
    globals().counterManager.AddCounter(mStaleChunkCount);
    for (size_t i = 0; i < kKfsSTierCount; i++) {
        mTierSpaceUtilizationThreshold[i]   = 2.;
        mTiersMaxWritesPerDriveThreshold[i] = mMinWritesPerDrive;
        mTiersMaxWritesPerDrive[i]          = mMaxWritesPerDrive;
        mTiersTotalWritableDrivesMult[i]    = 0.;
        mTierCandidatesCount[i]             = 0;
    }
}

LayoutManager::~LayoutManager()
{
    mCleanupFlag = true;
    // Cleanup chunk servers to prevent queue ops due to chunk entries delete
    // in remveSubTree();
    CleanupChunkServers();
    mRacks.clear();
    mChunkServers.clear();
    // Cleanup meta tree prior to destroying chunk hash table, as hash table,
    // owns and allocates chunk info nodes.
    metatree.removeSubTree(ROOTFID, 0);
    globals().counterManager.RemoveCounter(mOngoingReplicationStats);
    globals().counterManager.RemoveCounter(mTotalReplicationStats);
    globals().counterManager.RemoveCounter(mFailedReplicationStats);
    globals().counterManager.RemoveCounter(mStaleChunkCount);
    delete mReplicationTodoStats;
    delete mOngoingReplicationStats;
    delete mTotalReplicationStats;
    delete mFailedReplicationStats;
    delete mStaleChunkCount;
    if (mCleanupScheduledFlag) {
        mNetManager.UnRegisterTimeoutHandler(this);
    }
}

void
LayoutManager::CleanupChunkServers()
{
    for (Servers::iterator it = mChunkServers.begin();
            mChunkServers.end() != it;
            ++it) {
        (*it)->ForceDown();
        mChunkToServerMap.RemoveServer(*it);
    }
}

template<typename T, typename OT> void
LayoutManager::LoadIdRemap(istream& fs, T OT::* map)
{
    string        line;
    string        prefix;
    istringstream is;
    HostPrefix    hp;
    line.reserve(4 << 10);
    prefix.reserve(256);
    while(getline(fs, line)) {
        is.str(line);
        is.clear();
        if (! (is >> prefix)) {
            continue;
        }
        if (hp.Parse(prefix) <= 0) {
            continue;
        }
        HostUserGroupRemap::iterator it = find_if(
            mHostUserGroupRemap.begin(),
            mHostUserGroupRemap.end(),
            bind(&HostUserGroupRemap::value_type::mHostPrefix, _1)
            == hp
        );
        if (it == mHostUserGroupRemap.end()) {
            it = mHostUserGroupRemap.insert(
                mHostUserGroupRemap.end(),
                HostUserGroupRemap::value_type()
            );
            it->mHostPrefix = hp;
        }
        typename T::key_type    key;
        typename T::mapped_type val;
        T& m = (*it).*map;
        while ((is >> key >> val)) {
            m.insert(make_pair(key, val));
        }
        // de-reference "line", to avoid re-allocation [hopefully]
        is.str(string());
    }
}

struct RackPrefixValidator
{
    bool operator()(const string& pref, LayoutManager::RackId& id) const
    {
        if (id < 0) {
            id = -1;
            return false;
        }
        if (id > ChunkPlacement<LayoutManager>::kMaxRackId) {
            KFS_LOG_STREAM_ERROR <<
                "invalid rack id: " <<
                pref << " " << id <<
            KFS_LOG_EOM;
            id = -1;
            return false;
        }
        KFS_LOG_STREAM_INFO <<
            "rack:"
            " prefix: " << pref <<
            " id: "     << id   <<
        KFS_LOG_EOM;
        return true;
    }
};

bool
LayoutManager::SetParameters(const Properties& props, int clientPort)
{
    if (MsgLogger::GetLogger()) {
        MsgLogger::GetLogger()->SetParameters(
            props, "metaServer.msgLogWriter.");
    }
    ChunkServer::SetParameters(props, clientPort);
    MetaRequest::SetParameters(props);

    mMaxConcurrentReadReplicationsPerNode = props.getValue(
        "metaServer.maxConcurrentReadReplicationsPerNode",
        mMaxConcurrentReadReplicationsPerNode);
    mMaxConcurrentWriteReplicationsPerNode = props.getValue(
        "metaServer.maxConcurrentWriteReplicationsPerNode",
        mMaxConcurrentWriteReplicationsPerNode);
    mUseEvacuationRecoveryFlag = props.getValue(
        "metaServer.useEvacuationRecoveryFlag",
        mUseEvacuationRecoveryFlag ? 1 : 0) != 0;
    mFullReplicationCheckInterval = (int64_t)(props.getValue(
        "metaServer.fullReplicationCheckInterval",
        mFullReplicationCheckInterval * 1e-6) * 1e6);
    mMaxTimeForChunkReplicationCheck = (int64_t)(props.getValue(
        "metaServer.maxTimeForChunkReplicationCheck",
        mMaxTimeForChunkReplicationCheck * 1e-6) * 1e6);
    mMinChunkReplicationCheckInterval = (int64_t)(props.getValue(
        "metaServer.minChunkReplicationCheckInterval",
        mMinChunkReplicationCheckInterval * 1e-6) * 1e6);
    mConcurrentWritesPerNodeWatermark = props.getValue(
        "metaServer.concurrentWritesPerNodeWatermark",
        mConcurrentWritesPerNodeWatermark);
    mMaxSpaceUtilizationThreshold = props.getValue(
        "metaServer.maxSpaceUtilizationThreshold",
        mMaxSpaceUtilizationThreshold);
    mUseFsTotalSpaceFlag = props.getValue(
        "metaServer.useFsTotalSpace",
        mUseFsTotalSpaceFlag ? 1 : 0) != 0;
    mChunkAllocMinAvailSpace = props.getValue(
        "metaServer.chunkAllocMinAvailSpace",
        mChunkAllocMinAvailSpace);
    mCompleteReplicationCheckInterval = (int64_t)(props.getValue(
        "metaServer.completeReplicationCheckInterval",
        mCompleteReplicationCheckInterval * 1e-6) * 1e6);
    mPastEofRecoveryDelay = (int64_t)(props.getValue(
        "metaServer.pastEofRecoveryDelay",
        mPastEofRecoveryDelay * 1e-6) * 1e6);
    mMaxServerCleanupScan = max(0, props.getValue(
        "metaServer.maxServerCleanupScan",
        (int)mMaxServerCleanupScan));

    mMaxRebalanceScan = max(0, props.getValue(
        "metaServer.maxRebalanceScan",
        mMaxRebalanceScan));
    mRebalanceReplicationsThreshold = props.getValue(
        "metaServer.rebalanceReplicationsThreshold",
        mRebalanceReplicationsThreshold);
    mRebalanceReplicationsThreshold = (int64_t)(props.getValue(
        "metaServer.maxRebalanceRunTime",
        double(mMaxRebalanceRunTime) * 1e-6) * 1e6);
    mRebalanceRunInterval = (int64_t)(props.getValue(
        "metaServer.rebalanceRunInterval",
        double(mRebalanceRunInterval) * 1e-6) * 1e6);
    mIsRebalancingEnabled = props.getValue(
        "metaServer.rebalancingEnabled",
        mIsRebalancingEnabled ? 1 : 0) != 0;
    mMaxRebalanceSpaceUtilThreshold = props.getValue(
        "metaServer.maxRebalanceSpaceUtilThreshold",
        mMaxRebalanceSpaceUtilThreshold);
    mMinRebalanceSpaceUtilThreshold = props.getValue(
        "metaServer.minRebalanceSpaceUtilThreshold",
        mMinRebalanceSpaceUtilThreshold);
    mMaxRebalancePlanRead = props.getValue(
        "metaServer.maxRebalancePlanRead",
        mMaxRebalancePlanRead);
    LoadRebalancePlan(props.getValue(
        "metaServer.rebalancePlanFileName",
        mRebalancePlanFileName));

    mAssignMasterByIpFlag = props.getValue(
        "metaServer.assignMasterByIp",
        mAssignMasterByIpFlag ? 1 : 0) != 0;
    mLeaseOwnerDownExpireDelay = max(0, props.getValue(
        "metaServer.leaseOwnerDownExpireDelay",
        mLeaseOwnerDownExpireDelay));
    mMaxDumpsterCleanupInFlight = max(2, props.getValue(
        "metaServer.maxDumpsterCleanupInFlight",
        mMaxDumpsterCleanupInFlight));
    mMaxTruncateChunksDeleteCount = max(1, props.getValue(
        "metaServer.maxTruncateChunksDeleteCount",
        mMaxTruncateChunksDeleteCount));
    mMaxTruncateChunksQueueCount = max(1, props.getValue(
        "metaServer.maxTruncateChunksQueueCount",
        mMaxTruncateChunksQueueCount));
    mMaxTruncatedChunkDeletesInFlight = max(2, props.getValue(
        "metaServer.maxTruncatedChunkDeletesInFlight",
        mMaxTruncatedChunkDeletesInFlight));
    mMaxReservationSize = max(0, props.getValue(
        "metaServer.wappend.maxReservationSize",
        mMaxReservationSize));
    mReservationDecayStep = props.getValue(
        "metaServer.reservationDecayStep",
        mReservationDecayStep);
    mChunkReservationThreshold = props.getValue(
        "metaServer.reservationThreshold",
        mChunkReservationThreshold);
    mAllocAppendReuseInFlightTimeoutSec = props.getValue(
        "metaServer.wappend.reuseInFlightTimeoutSec",
        mAllocAppendReuseInFlightTimeoutSec);
    mMinAppendersPerChunk = props.getValue(
        "metaserver.wappend.minAppendersPerChunk",
        mMinAppendersPerChunk);
    mMaxAppendersPerChunk = props.getValue(
        "metaserver.wappend.maxAppendersPerChunk",
        mMaxAppendersPerChunk);
    mReservationOvercommitFactor = max(0., props.getValue(
        "metaServer.wappend.reservationOvercommitFactor",
        mReservationOvercommitFactor));

    mLeaseCleanerOtherIntervalSec = (int)props.getValue(
        "metaServer.leaseCleanupInterval",
        (double)mLeaseCleanerOtherIntervalSec);
    mLeaseCleanerOtherNextRunTime = min(mLeaseCleanerOtherNextRunTime,
        TimeNow() + mLeaseCleanerOtherIntervalSec);
    mChunkReplicator.SetTimeoutInterval((int)(props.getValue(
        "metaServer.replicationCheckInterval",
        mChunkReplicator.GetTimeoutInterval() * 1e-3) * 1e3));

    mCheckpoint.GetOp().SetParameters(props);

    mCSCountersUpdateInterval = props.getValue(
        "metaServer.CSCountersUpdateInterval",
        mCSCountersUpdateInterval);
    mCSDirCountersUpdateInterval = props.getValue(
        "metaServer.CSDirCountersUpdateInterval",
        mCSCountersUpdateInterval);
    mPingUpdateInterval = props.getValue(
        "metaServer.pingUpdateInterval",
        mPingUpdateInterval);

    /// On startup, the # of secs to wait before we are open for reads/writes
    mRecoveryIntervalSec = props.getValue(
        "metaServer.recoveryInterval", mRecoveryIntervalSec);
    mServerDownReplicationDelay = (int)props.getValue(
        "metaServer.serverDownReplicationDelay",
         double(mServerDownReplicationDelay));
    mMaxDownServersHistorySize = props.getValue(
        "metaServer.maxDownServersHistorySize",
         mMaxDownServersHistorySize);

    mMaxCSRestarting = props.getValue(
        "metaServer.maxCSRestarting",
         mMaxCSRestarting);
    mRetireOnCSRestartFlag = props.getValue(
        "metaServer.retireOnCSRestart",
         mRetireOnCSRestartFlag ? 1 : 0) != 0;
    mMaxCSUptime = props.getValue(
        "metaServer.maxCSUptime",
         mMaxCSUptime);
    mCSGracefulRestartTimeout = max((int64_t)0, props.getValue(
        "metaServer.CSGracefulRestartTimeout",
         mCSGracefulRestartTimeout));
    mCSGracefulRestartAppendWithWidTimeout = max((int64_t)0, props.getValue(
        "metaServer.CSGracefulRestartAppendWithWidTimeout",
        mCSGracefulRestartAppendWithWidTimeout));

    mRecomputeDirSizesIntervalSec = max(0, props.getValue(
        "metaServer.recomputeDirSizesIntervalSec",
        mRecomputeDirSizesIntervalSec));

    mDelayedRecoveryUpdateMaxScanCount = props.getValue(
        "metaServer.delayedRecoveryUpdateMaxScanCount",
        mDelayedRecoveryUpdateMaxScanCount);
    mForceDelayedRecoveryUpdateFlag = props.getValue(
        "metaServer.forceDelayedRecoveryUpdate",
        mForceDelayedRecoveryUpdateFlag ? 1 : 0) != 0;

    mSortCandidatesBySpaceUtilizationFlag = props.getValue(
        "metaServer.sortCandidatesBySpaceUtilization",
        mSortCandidatesBySpaceUtilizationFlag ? 1 : 0) != 0;
    mSortCandidatesByLoadAvgFlag = props.getValue(
        "metaServer.sortCandidatesByLoadAvg",
        mSortCandidatesByLoadAvgFlag ? 1 : 0) != 0;

    // The following two parameter names are for backward compatibility.
    mMaxFsckFiles = props.getValue(
        "metaServer.maxFsckChunks",
        mMaxFsckFiles);
    mMaxFsckTime = (int64_t)(props.getValue(
        "metaServer.mMaxFsckTime",
        mMaxFsckTime * 1e-6) * 1e6);
    mMaxFsckFiles = props.getValue(
        "metaServer.maxFsckFiles",
        mMaxFsckFiles);
    mFsckAbandonedFileTimeout = (int64_t)(props.getValue(
        "metaServer.fsckAbandonedFileTimeout",
        mFsckAbandonedFileTimeout * 1e-6) * 1e6);
    mMaxFsckTime = (int64_t)(props.getValue(
        "metaServer.maxFsckTime",
        mMaxFsckTime * 1e-6) * 1e6);
    mFullFsckFlag = props.getValue(
        "metaServer.fullFsck",
        mFullFsckFlag ? 1 : 0) != 0;

    mDirATimeUpdateResolution = (int64_t)(props.getValue(
        "metaServer.dirATimeUpdateResolution",
        mDirATimeUpdateResolution * 1e-6) * 1e6);
    mATimeUpdateResolution = (int64_t)(props.getValue(
        "metaServer.ATimeUpdateResolution",
        mATimeUpdateResolution * 1e-6) * 1e6);
    mMTimeUpdateResolution = (int64_t)(props.getValue(
        "metaServer.MTimeUpdateResolution",
        mMTimeUpdateResolution * 1e-6) * 1e6);

    mMaxPendingRecoveryMsgLogInfo = props.getValue(
        "metaServer.maxPendingRecoveryMsgLogInfo",
        mMaxPendingRecoveryMsgLogInfo);

    mAllowLocalPlacementFlag = props.getValue(
        "metaServer.allowLocalPlacement",
        mAllowLocalPlacementFlag ? 1 : 0) != 0;
    mAllowLocalPlacementForAppendFlag = props.getValue(
        "metaServer.allowLocalPlacementForAppend",
        mAllowLocalPlacementForAppendFlag ? 1 : 0) != 0;
    mInRackPlacementForAppendFlag = props.getValue(
        "metaServer.inRackPlacementForAppend",
        mInRackPlacementForAppendFlag ? 1 : 0) != 0;
    mInRackPlacementFlag = props.getValue(
        "metaServer.inRackPlacement",
        mInRackPlacementFlag ? 1 : 0) != 0;
    mAppendPlacementIgnoreMasterSlaveFlag = props.getValue(
        "metaServer.appendPlacementIgnoreMasterSlave",
        mAppendPlacementIgnoreMasterSlaveFlag ? 1 : 0) != 0;
    mAllocateDebugVerifyFlag = props.getValue(
        "metaServer.allocateDebugVerify",
        mAllocateDebugVerifyFlag ? 1 : 0) != 0;
    mGetAllocOrderServersByLoadFlag = props.getValue(
        "metaServer.getAllocOrderServersByLoad",
        mGetAllocOrderServersByLoadFlag ? 1 : 0) != 0;
    mMinChunkAllocClientProtoVersion = props.getValue(
        "metaServer.minChunkAllocClientProtoVersion",
        mMinChunkAllocClientProtoVersion);
    mRackPrefixUsePortFlag = props.getValue(
        "metaServer.rackPrefixUsePort",
        mRackPrefixUsePortFlag ? 1 : 0) != 0;
    mUseCSRackAssignmentFlag = props.getValue(
        "metaServer.useCSRackAssignment",
        mUseCSRackAssignmentFlag ? 1 : 0) != 0;

    HibernatedChunkServer::SetParameters(props);

    mRackPrefixes.clear();
    {
        istringstream is(props.getValue("metaServer.rackPrefixes", ""));
        RackPrefixValidator validator;
        mRackPrefixes.Load(is, &validator, -1);
    }
    mRackWeights.clear();
    {
        istringstream is(props.getValue("metaServer.rackWeights", ""));
        RackId        id     = -1;
        double        weight = -1;
        while ((is >> id >> weight)) {
            if (id >= 0 && weight >= 0) {
                mRackWeights[id] = weight;
                KFS_LOG_STREAM_INFO <<
                    "rack: "    << id <<
                    " weight: " << weight <<
                KFS_LOG_EOM;
            }
            id     = -1;
            weight = -1;
        }
        for (RackInfos::iterator it = mRacks.begin();
                it != mRacks.end();
                ++it) {
            RackWeights::const_iterator const wi =
                mRackWeights.find(it->id());
            it->setWeight(wi == mRackWeights.end() ?
                double(1) : wi->second);
        }
    }

    mCSMaxGoodCandidateLoadRatio = props.getValue(
        "metaServer.maxGoodCandidateLoadRatio",
        mCSMaxGoodCandidateLoadRatio);
    mCSMaxGoodMasterLoadRatio = props.getValue(
        "metaServer.maxGoodMasterLoadRatio",
        mCSMaxGoodMasterLoadRatio);
    mCSMaxGoodSlaveLoadRatio = props.getValue(
        "metaServer.maxGoodSlaveLoadRatio",
        mCSMaxGoodSlaveLoadRatio);
    const double k1Frac = (double)(int64_t(1) << kSlaveScaleFracBits);
    mMaxSlavePlacementRange = (int64_t)(props.getValue(
        "metaServer.maxSlavePlacementRange",
        (double)mMaxSlavePlacementRange / k1Frac) * k1Frac);

    const int kMaxReplication = (1 << 14) - 1; // 14 bit field in file attribute
    mMaxReplicasPerFile = (int16_t)min(kMaxReplication, props.getValue(
        "metaServer.maxReplicasPerFile", int(mMaxReplicasPerFile)));
    mMaxReplicasPerRSFile = (int16_t)min(kMaxReplication, props.getValue(
        "metaServer.maxReplicasPerRSFile", int(mMaxReplicasPerRSFile)));

    mChunkServerMd5sums.clear();
    {
        istringstream is(props.getValue(
            "metaServer.chunkServerMd5sums", ""));
        string md5sum;
        while ((is >> md5sum)) {
            mChunkServerMd5sums.push_back(md5sum);
            md5sum.clear();
        }
    }
    const string clusterKey =
        props.getValue(kMetaClusterKeyParamNamePtr, mClusterKey);
    const bool validClusterKeyFlag = isValidClusterKey(clusterKey.c_str());
    if (validClusterKeyFlag) {
        mClusterKey = clusterKey;
    }
    mMaxResponseSize = props.getValue(
        "metaServer.maxResponseSize",
        mMaxResponseSize);
    mMinIoBufferBytesToProcessRequest = props.getValue(
        "metaServer.minIoBufferBytesToProcessRequest",
        mMinIoBufferBytesToProcessRequest);

    int64_t totalIoBytes = mBufferPool ?
        (int64_t)mBufferPool->GetTotalBufferCount() *
        mBufferPool->GetBufferSize()  : int64_t(-1);
    if (totalIoBytes > 0) {
        const int64_t minReserve =
            (16 << 10) * mBufferPool->GetBufferSize();
        if (totalIoBytes > minReserve * 3) {
            totalIoBytes -= minReserve;
        } else {
            totalIoBytes = totalIoBytes * 2 / 3;
        }
        if (mMaxResponseSize > totalIoBytes) {
            mMaxResponseSize = (int)min(totalIoBytes,
                (int64_t)numeric_limits<int>::max());
        }
        ChunkServer::SetMaxHelloBufferBytes(
            min(totalIoBytes, props.getValue(
                "chunkServer.maxHelloBufferBytes",
                ChunkServer::GetMaxHelloBufferBytes()))
        );
    } else {
        ChunkServer::SetMaxHelloBufferBytes(props.getValue(
            "chunkServer.maxHelloBufferBytes",
            ChunkServer::GetMaxHelloBufferBytes())
        );
    }
    if (mMinIoBufferBytesToProcessRequest > totalIoBytes) {
        mMinIoBufferBytesToProcessRequest = totalIoBytes;
    }
    KFS_LOG_STREAM_INFO <<
        "max. response size: " <<
            mMaxResponseSize <<
        " minIoBufferBytesToProcessRequest: " <<
            mMinIoBufferBytesToProcessRequest <<
    KFS_LOG_EOM;
    mReadDirLimit = props.getValue(
        "metaServer.readDirLimit", mReadDirLimit);

    ClientSM::SetParameters(props);
    const int netDispatchErr = gNetDispatch.SetParameters(props);
    if (mDownServers.size() > mMaxDownServersHistorySize) {
        mDownServers.erase(mDownServers.begin(), mDownServers.begin() +
            mDownServers.size() - mMaxDownServersHistorySize);
    }
    MetaFsck::SetParameters(props);
    SetRequestParameters(props);
    CSMapUnitTest(props);
    mChunkToServerMap.SetDebugValidate(props.getValue(
        "metaServer.chunkToServerMap.debugValidate", 0) != 0);
    mAllowChunkServerRetireFlag = props.getValue(
        "metaServer.allowChunkServerRetire",
        mAllowChunkServerRetireFlag ? 1 : 0) != 0;
    mPanicOnInvalidChunkFlag = props.getValue(
        "metaServer.panicOnInvalidChunk",
        mPanicOnInvalidChunkFlag ? 1 : 0) != 0;
    mPanicOnRemoveFromPlacementFlag = props.getValue(
        "metaServer.panicOnRemoveFromPlacement",
        mPanicOnRemoveFromPlacementFlag ? 1 : 0) != 0;
    mAppendCacheCleanupInterval = (int)props.getValue(
        "metaServer.appendCacheCleanupInterval",
        double(mAppendCacheCleanupInterval));
    UpdateReplicationsThreshold();
    mMaxWritesPerDrive = props.getValue(
        "metaServer.maxWritesPerDrive",
        mMaxWritesPerDrive);
    mMaxWritesPerDriveRatio = props.getValue(
        "metaServer.maxWritesPerDriveRatio",
        mMaxWritesPerDriveRatio);
    mMaxLocalPlacementWeight = props.getValue(
        "metaServer.maxLocalPlacementWeight",
        mMaxLocalPlacementWeight);
    mMinWritesPerDrive = max(1, props.getValue(
        "metaServer.minWritesPerDrive",
        mMinWritesPerDrive));
    mMaxWritesPerDriveThreshold =
        max(mMinWritesPerDrive, mMaxWritesPerDriveThreshold);
    mDefaultUser = props.getValue(
        "metaServer.defaultUser",
        mDefaultUser);
    mDefaultGroup = props.getValue(
        "metaServer.defaultGroup",
        mDefaultGroup);
    mDefaultFileMode = props.getValue(
        "metaServer.defaultFileMode",
        mDefaultFileMode);
    mDefaultDirMode = props.getValue(
        "metaServer.defaultDirMode",
        mDefaultDirMode);
    mDefaultLoadUser = props.getValue(
        "metaServer.defaultLoadUser",
        mDefaultLoadUser);
    mDefaultLoadGroup = props.getValue(
        "metaServer.defaultLoadGroup",
        mDefaultLoadGroup);
    mDefaultLoadFileMode = props.getValue(
        "metaServer.defaultLoadFileMode",
        mDefaultLoadFileMode);
    mDefaultLoadDirMode = props.getValue(
        "metaServer.defaultLoadDirMode",
        mDefaultLoadDirMode);
    mForceEUserToRootFlag = props.getValue(
        "metaServer.forceEUserToRoot",
        mForceEUserToRootFlag ? 1 : 0) != 0;
    mVerifyAllOpsPermissionsParamFlag = props.getValue(
        "metaServer.verifyAllOpsPermissions",
        mVerifyAllOpsPermissionsParamFlag ? 1 : 0) != 0;
    mObjStoreDeleteMaxSchedulePerRun = max(1, props.getValue(
        "metaServer.objectStoreDeleteMaxSchedulePerRun",
        mObjStoreDeleteMaxSchedulePerRun));
    mObjStoreMaxDeletesPerServer = max(1, props.getValue(
        "metaServer.objectStoreMaxDeletesPerServer",
        mObjStoreMaxDeletesPerServer));
    mObjStoreDeleteDelay = props.getValue(
        "metaServer.objectStoreDeleteDelay",
        mObjStoreDeleteDelay);
    mRootHosts.clear();
    {
        istringstream is(props.getValue("metaServer.rootHosts", ""));
        string host;
        while ((is >> host)) {
            mRootHosts.insert(host);
        }
    }
    mHostUserGroupRemap.clear();
    mLastUidGidRemap.mIp.clear();
    for (int i = 0; i < 2; i++) {
        const string idRemapFileName = props.getValue(
            i == 0 ?
                "metaServer.hostUserRemap" :
                "metaServer.hostGroupRemap",
            string()
        );
        if (idRemapFileName.empty()) {
            continue;
        }
        ifstream fs(idRemapFileName.c_str());
        if (! fs) {
            KFS_LOG_STREAM_ERROR << "failed to open: " <<
                idRemapFileName <<
            KFS_LOG_EOM;
            continue;
        }
        if (i == 0) {
            LoadIdRemap(fs,
                &HostUserGroupRemap::value_type::mUserMap);
        } else {
            LoadIdRemap(fs,
                &HostUserGroupRemap::value_type::mGroupMap);
        }
    }
    {
        for (size_t i = 0; i < kKfsSTierCount; i++) {
            mTiersMaxWritesPerDrive[i] = mMaxWritesPerDrive;
        }
        istringstream is(props.getValue(
            "metaServer.tiersMaxWrtiesPerDrive", string()));
        int tier;
        int maxWr;
        while ((is >> tier >> maxWr)) {
            if (tier >= 0 && tier < (int)kKfsSTierCount) {
                mTiersMaxWritesPerDrive[tier] = maxWr;
            }
        }
    }
    {
        for (size_t i = 0; i < kKfsSTierCount; i++) {
            mTierSpaceUtilizationThreshold[i] = 2.;
        }
        istringstream is(props.getValue(
            "metaServer.tiersMaxSpaceUtilization", string()));
        int    tier;
        double util;
        while ((is >> tier >> util)) {
            if (tier >= 0 && tier < (int)kKfsSTierCount) {
                mTierSpaceUtilizationThreshold[tier] = util;
            }
        }
    }

    const int userAndGroupErr = mUserAndGroup.SetParameters(
        "metaServer.userAndGroup.", props);

    mConfigParameters = props;
    const bool csOkFlag = mCSAuthContext.SetParameters(
        "metaServer.CSAuthentication.", mConfigParameters);
    const bool curCSAuthUseUserAndGroupFlag = mCSAuthContext.HasUserAndGroup();
    const bool newCSAuthUseUserAndGroupFlag = props.getValue(
        "metaServer.CSAuthentication.useUserAndGrupDb",
        curCSAuthUseUserAndGroupFlag ? 1 : 0) != 0;
    if (newCSAuthUseUserAndGroupFlag != curCSAuthUseUserAndGroupFlag) {
        if (newCSAuthUseUserAndGroupFlag) {
            mCSAuthContext.SetUserAndGroup(mUserAndGroup);
        } else {
            mCSAuthContext.DontUseUserAndGroup();
        }
    }
    const bool cliOkFlag = UpdateClientAuth(mClientAuthContext);
    mAuthCtxUpdateCount++;

    mClientCSAuthRequiredFlag = props.getValue(
        "metaServer.clientCSAuthRequired",
        mCSAuthContext.IsAuthRequired() ? 1 : 0) != 0;
    mClientCSAllowClearTextFlag = props.getValue(
        "metaServer.clientCSAllowClearText",
        mClientCSAllowClearTextFlag ? 1 : 0) != 0;
    mCSAccessValidForTimeSec = max(LEASE_INTERVAL_SECS, props.getValue(
        "metaServer.CSAccessValidForTimeSec", mCSAccessValidForTimeSec));
    mMinWriteLeaseTimeSec = props.getValue(
        "metaServer.minWriteLeaseTimeSec", mMinWriteLeaseTimeSec);
    mFileSystemIdRequiredFlag = props.getValue(
        "metaServer.fileSystemIdRequired",
        mFileSystemIdRequiredFlag ? 1 : 0) != 0;
    mDeleteChunkOnFsIdMismatchFlag = props.getValue(
        "metaServer.deleteChunkOnFsIdMismatch",
        mDeleteChunkOnFsIdMismatchFlag ? 1 : 0) != 0;
    mChunkAvailableUseReplicationOrRecoveryThreshold = props.getValue(
        "metaServer.chunkAvailableUseReplicationOrRecoveryThreshold",
        mChunkAvailableUseReplicationOrRecoveryThreshold);
    {
        istringstream is(props.getValue("metaServer.createFileTypeExclude", ""));
        mCreateFileTypeExclude.clear();
        int val;
        while ((is >> val)) {
            mCreateFileTypeExclude.insert(val);
        }
    }
    mMaxDataStripeCount = min(KFS_MAX_DATA_STRIPE_COUNT, props.getValue(
        "metaServer.maxDataStripeCount",     mMaxDataStripeCount));
    mMaxRecoveryStripeCount = min(KFS_MAX_RECOVERY_STRIPE_COUNT, props.getValue(
        "metaServer.maxRecoveryStripeCount", mMaxRecoveryStripeCount));
    mMaxRSDataStripeCount = min(KFS_MAX_DATA_STRIPE_COUNT, props.getValue(
        "metaServer.maxRSDataStripeCount", mMaxRSDataStripeCount));
    mDebugSimulateDenyHelloResumeInterval = props.getValue(
        "metaServer.debugSimulateDenyHelloResumeInterval",
        mDebugSimulateDenyHelloResumeInterval);
    mDebugPanicOnHelloResumeFailureCount = props.getValue(
        "metaServer.debugPanicOnHelloResumeFailureCount",
        mDebugPanicOnHelloResumeFailureCount);
    mHelloResumeFailureTraceFileName = props.getValue(
        "metaServer.helloResumeFailureTraceFileName",
        mHelloResumeFailureTraceFileName);
    mObjectStoreEnabledFlag = props.getValue(
        "metaServer.objectStoreEnabled", mObjectStoreEnabledFlag ? 1 : 0) != 0;
    mObjectStoreReadCanUsePoxoyOnDifferentHostFlag = props.getValue(
        "metaServer.objectStoreReadCanUsePoxoyOnDifferentHost",
        mObjectStoreReadCanUsePoxoyOnDifferentHostFlag ? 1 : 0) != 0;
    mObjectStoreWriteCanUsePoxoyOnDifferentHostFlag = props.getValue(
        "metaServer.objectStoreWriteCanUsePoxoyOnDifferentHost",
        mObjectStoreWriteCanUsePoxoyOnDifferentHostFlag ? 1 : 0) != 0;
    mObjectStorePlacementTestFlag = props.getValue(
        "metaServer.objectStorePlacementTest",
        mObjectStorePlacementTestFlag ? 1 : 0) != 0;

    mIdempotentRequestTracker.SetParameters(
        "metaServer.idempotentRequest.", props);
    mConfig.clear();
    mConfig.reserve(10 << 10);
    StBufferT<PropertiesTokenizer::Token, 4> configFilter;
    for (const char* ptr = props.getValue(
                "metaServer.pingDoNotShow",
                    " metaServer.clientAuthentication."
                    " metaServer.CSAuthentication."
                ); ;) {
        while ((*ptr & 0xFF) <= ' ' && *ptr) {
            ++ptr;
        }
        const char* const s = ptr;
        while (' ' < (*ptr & 0xFF)) {
            ++ptr;
        }
        if (ptr <= s) {
            break;
        }
        configFilter.Append(PropertiesTokenizer::Token(s, ptr - s));
    }
    for (Properties::iterator it = props.begin(); it != props.end(); ++it) {
        const PropertiesTokenizer::Token*       pref = configFilter.GetPtr();
        const PropertiesTokenizer::Token* const pend =
            pref + configFilter.GetSize();
        for (; pref < pend; ++pref) {
            if (pref->mLen <= it->first.size()) {
                const char*       pp  = pref->mPtr;
                const char*       ptr = it->first.data();
                const char* const end = ptr + pref->mLen;
                while (ptr < end &&
                        AsciiCharToLower(*ptr & 0xFF) ==
                        AsciiCharToLower(*pp  & 0xFF)) {
                    ++pp;
                    ++ptr;
                }
                if (end <= ptr) {
                    break;
                }
            }
        }
        const char  kEscapePrefix = '%';
        const char* kEscapeList   = "=;";
        mConfig.append(escapeString(it->first.data(), it->first.size(),
            kEscapePrefix, kEscapeList));
        mConfig += '=';
        if (pref < pend) {
            mConfig += 'x';
        } else {
            mConfig.append(escapeString(it->second.data(), it->second.size(),
                kEscapePrefix, kEscapeList));
        }
        mConfig += ';';
    }
    mChunkLeases.SetDumpsterCleanupDelaySec(props.getValue(
        "metaServer.dumpsterCleanupDelaySec",
        mChunkLeases.GetDumpsterCleanupDelaySec())
    );
    mVerifyAllOpsPermissionsFlag =
        mVerifyAllOpsPermissionsParamFlag ||
        mClientAuthContext.IsAuthRequired();
    SetChunkServersProperties(props);
    return (csOkFlag && cliOkFlag && validClusterKeyFlag &&
        0 == userAndGroupErr && 0 == netDispatchErr);
}

bool
LayoutManager::UpdateClientAuth(AuthContext& ctx)
{
    const bool ret = ctx.SetParameters(
        "metaServer.clientAuthentication.", mConfigParameters,
        &ctx == &mClientAuthContext ? 0 : &mClientAuthContext
    );
    ctx.SetUserAndGroup(mUserAndGroup);
    return ret;
}

void
LayoutManager::UpdateReplicationsThreshold()
{
    const int64_t srvCnt = (int64_t)GetConnectedServerCount();
    mRebalanceReplicationsThresholdCount = max(min(srvCnt, int64_t(1)),
    (int64_t)(
        mRebalanceReplicationsThreshold *
        mMaxConcurrentWriteReplicationsPerNode *
        srvCnt
    ));
}

/*!
 * \brief Validate the cluster key, and that md5 sent by a chunk server
 *  matches one of the acceptable md5's.
 */
bool
LayoutManager::Validate(MetaHello& r)
{
    if (r.replayFlag) {
        panic("invalid chunk server hello replay attempt");
        r.status = -EFAULT;
        return false;
    }
    if (! r.location.IsValid()) {
        r.statusMsg = "invalid chunk server location: " + r.location.ToString();
        r.status    = -EINVAL;
        return false;
    }
    if (r.clusterKey != mClusterKey) {
        r.statusMsg = "cluster key mismatch:"
            " expect: "   + mClusterKey +
            " received: " + r.clusterKey;
        r.status     = -EBADCLUSTERKEY;
        r.retireFlag = mRetireOnCSRestartFlag;
        return false;
    }
    if (! mChunkServerMd5sums.empty() && find(
                mChunkServerMd5sums.begin(),
                mChunkServerMd5sums.end(),
                r.md5sum) == mChunkServerMd5sums.end()) {
        r.statusMsg  = "MD5sum mismatch: received: " + r.md5sum;
        r.status     = -EBADCLUSTERKEY;
        r.retireFlag = mRetireOnCSRestartFlag;
        return false;
    }
    if (! mPrimaryFlag) {
        r.statusMsg  = "meta server node is not primary";
        r.status     = -ELOGFAILED;
        return false;
    }
    if (0 <= mDebugPanicOnHelloResumeFailureCount &&
            mDebugPanicOnHelloResumeFailureCount < r.helloResumeFailedCount) {
        const HibernatedChunkServer* const cs = FindHibernatingCS(r.location);
        if (cs && cs->CanBeResumed()) {
            ofstream* traceTee = 0;
            if (! mHelloResumeFailureTraceFileName.empty()) {
                static ofstream sTraceTee;
                sTraceTee.open(mHelloResumeFailureTraceFileName.c_str(),
                    ofstream::app | ofstream::out);
                if (sTraceTee.is_open()) {
                    traceTee = &sTraceTee;
                }
            }
            KFS_LOG_STREAM_START_TEE(
                    MsgLogger::kLogLevelFATAL, logStream, traceTee);
                logStream.GetStream() <<
                    "server: " << r.location << "\n" <<
                    HibernatedChunkServer::Display(*cs, mChunkToServerMap);
            KFS_LOG_STREAM_END;
            if (traceTee) {
                *traceTee << "\n";
                traceTee->close();
            }
        } else {
            KFS_LOG_STREAM_FATAL <<
                (cs ? "no valid hibernated state" : "no hibernated state") <<
            KFS_LOG_EOM;
        }
        panic("hello resume failure detected: " + r.location.ToString());
    }
    const HibernatingServerInfo* const hsi = FindHibernatingCSInfo(r.location);
    if (hsi && hsi->retiredFlag && TimeNow() < hsi->startTime + r.uptime) {
        // Chunk server must restart when it gets retire request. Uptime is used
        // to detect if retire is lost due to communication error, and
        // hibernation can be achieved by re-issuing retire.
        r.statusMsg  = "retire retry";
        r.status     = -EINVAL;
        r.retireFlag = true;
        return true;
    }
    if (0 <= r.resumeStep) {
        const HibernatedChunkServer* const cs = (hsi && hsi->IsHibernated()) ?
            mChunkToServerMap.GetHiberantedServer(hsi->csmapIdx) : 0;
        if (cs) {
            if (cs->CanBeResumed()) {
                const size_t kChunkIdReplayBytes = 17; // Hex encoded.
                r.bufferBytes = max(r.bufferBytes,
                    (int)(cs->GetChunkListsSize() * kChunkIdReplayBytes));
            } else {
                r.statusMsg = "resume not possible"
                    ", no valid hibernated info exists";
                r.status    = -EAGAIN;
            }
        } else {
            Servers::const_iterator const it = FindServer(r.location);
            if (it == mChunkServers.end()) {
                r.statusMsg = "resume not possible, no hibernated info exists";
                r.status    = -EAGAIN;
            }
            // Otherwise hello start will schedule server down,
            // see Start(MetaHello&) below.
        }
    }
    return true;
}

bool
LayoutManager::CanAddServer(size_t pendingAddSize)
{
    return (mChunkToServerMap.GetAvailableServerSlotCount()
        > pendingAddSize);
}

void
LayoutManager::Start(MetaHello& r)
{
    if (0 != r.status) {
        return;
    }
    if (! r.server || r.server->GetServerLocation() != r.location ||
            r.server->IsReplay() || r.replayFlag) {
        panic("invalid chunk server hello");
        r.status = -EFAULT;
        return;
    }
    if (! mPrimaryFlag) {
        r.statusMsg  = "meta server node is not primary";
        r.status     = -ELOGFAILED;
        return;
    }
    Servers::const_iterator const it = FindServer(r.location);
    if (mChunkServers.end() != it) {
        if (*it == r.server) {
            panic("invalid duplicate chunk server hello");
            r.status = -EFAULT;
            return;
        }
        if ((*it)->IsDuplicateChannel(r.channelId)) {
            r.statusMsg  = "ignoring duplicate channel";
            r.status     = -EEXIST;
            return;
        }
        (*it)->ScheduleDown("chunk server re-connect");
    }
    if (! mUseCSRackAssignmentFlag || r.rackId < 0) {
        RackId const rackId = GetRackId(r.location);
        if (0 <= rackId) {
            r.rackId = rackId;
        }
    }
    if (-1 != r.rackId && (r.rackId < 0 ||
            ChunkPlacement::kMaxRackId < r.rackId)) {
        KFS_LOG_STREAM_ERROR <<
            "chunk server: " << r.location <<
            " rack id: " << r.rackId <<
            " is out of supported range: [0:" << ChunkPlacement::kMaxRackId <<
            "] -- treated as undefined" <<
        KFS_LOG_EOM;
        r.rackId = -1;
    }
}

bool
LayoutManager::Validate(MetaCreate& createOp) const
{
    if (mCreateFileTypeExclude.end() !=
            mCreateFileTypeExclude.find(createOp.striperType)) {
        createOp.status    = -EPERM;
        createOp.statusMsg = "file type is not allowed";
        return false;
    }
    if (mMaxDataStripeCount < createOp.numStripes) {
        createOp.status    = -EPERM;
        createOp.statusMsg = "stripe count exceeds max allowed";
        return false;
    }
    if (mMaxRecoveryStripeCount < createOp.numRecoveryStripes) {
        createOp.status    = -EPERM;
        createOp.statusMsg = "recovery stripe count exceeds max allowed";
        return false;
    }
    if (0 < createOp.numRecoveryStripes &&
            mMaxRSDataStripeCount < createOp.numStripes) {
        createOp.status    = -EPERM;
        createOp.statusMsg =
            "data stripe count exceeds max allowed for files with recovery";
        return false;
    }
    if (0 == createOp.numReplicas && ! mObjectStoreEnabledFlag) {
        createOp.statusMsg = "object store is not enabled";
        createOp.status    = -EINVAL;
        return false;
    }
    return true;
}

LayoutManager::RackId
LayoutManager::GetRackId(const ServerLocation& loc) const
{
    return mRackPrefixes.GetId(loc, -1, mRackPrefixUsePortFlag);
}

LayoutManager::RackId
LayoutManager::GetRackId(const string& name) const
{
    return mRackPrefixes.GetId(name, -1);
}

LayoutManager::RackId
LayoutManager::GetRackId(const MetaRequest& req) const
{
    if (0 <= req.clientRackId) {
        return req.clientRackId;
    }
    const RackId rackId = mRackPrefixes.GetId(req.clientIp, -1);
    if (0 <= rackId) {
        return rackId;
    }
    return mRackPrefixes.GetId(req.clientReportedIp, -1);
}

void
LayoutManager::SetChunkServersProperties(const Properties& props)
{
    if (props.empty()) {
        return;
    }
    const char* prefix = "chunkServer.";
    props.copyWithPrefix(prefix, mChunkServersProps);
    if (0 < mCSAccessValidForTimeSec && mClientAuthContext.IsAuthRequired()) {
        const int         kBufSize = 32;
        char              buf[kBufSize];
        char*             end      = buf + kBufSize;
        const char* const ptr      =
            IntToDecString(mCSAccessValidForTimeSec, end);
        Properties::String name(prefix);
        mChunkServersProps.setValue(
            name.Append("cryptoKeys.minKeyValidTimeSec"),
            Properties::String(ptr, end - ptr)
        );
    }
    if (mChunkServersProps.empty()) {
        return;
    }
    string display;
    mChunkServersProps.getList(display, "", ";");
    KFS_LOG_STREAM_INFO << "setting properties for " <<
        mChunkServers.size() << " chunk servers: " << display <<
    KFS_LOG_EOM;
    Servers const chunkServers(mChunkServers);
    for (Servers::const_iterator i = chunkServers.begin();
            i != chunkServers.end();
            ++i) {
        (*i)->SetProperties(mChunkServersProps);
    }
}

void
LayoutManager::Shutdown()
{
    mCleanupFlag = true;
    CancelRequestsWaitingForBuffers();
    // Return io buffers back into the pool.
    mCSCountersResponse.Clear();
    mCSDirCountersResponse.Clear();
    mPingResponse.Clear();
    mUserAndGroup.Shutdown();
    mClientAuthContext.Clear();
    mCSAuthContext.Clear();
    mIdempotentRequestTracker.Clear();
    RequestQueue queue;
    queue.PushBack(mResubmitQueue);
    MetaRequest* req;
    while ((req = queue.PopFront())) {
        submit_request(req);
    }
    CleanupChunkServers();
    mCleanupFlag = false;
}

template<
    typename Writer,
    typename Iterator,
    typename GetCounters,
    typename WriteHeader,
    typename WriteExtra
>
void
WriteCounters(
    Writer&        writer,
    Iterator       first,
    Iterator       last,
    GetCounters&   getCounters,
    WriteHeader&   writeHeader,
    WriteExtra&    writeExtra)
{
    static const Properties::String kColumnDelim(",");
    static const Properties::String kRowDelim("\n");
    static const Properties::String kRemove[] = {
        "Cseq", "status", "s", "c", "m",
        "" /* sentinel */ };

    if (first == last) {
        return;
    }
    Properties columns;
    bool firstFlag = true;
    for (int pass = 0; pass < 2; ++pass) {
        if (! firstFlag) {
            writeHeader(writer, columns, kColumnDelim);
            writer.Write(kRowDelim);
        }
        bool writeFlag = true;
        for (Iterator it = first; it != last; ++it) {
            typename GetCounters::iterator ctrFirst;
            typename GetCounters::iterator ctrLast;
            getCounters(*it, ctrFirst, ctrLast);
            if (firstFlag && ctrFirst != ctrLast) {
                firstFlag = false;
                columns = getCounters(ctrFirst);
                for (const Properties::String* p = kRemove; ! p->empty(); ++p) {
                    columns.remove(*p);
                }
                writeHeader(writer, columns, kColumnDelim);
                writer.Write(kRowDelim);
            }
            for ( ; ctrFirst != ctrLast; ++ctrFirst) {
                const Properties&    props = getCounters(ctrFirst);
                Properties::iterator ci    = columns.begin();
                for (Properties::iterator pi = props.begin();
                        pi != props.end();
                        ++pi) {
                    const Properties::String* rp = kRemove;
                    for (; ! rp->empty() && pi->first != *rp; ++rp)
                        {}
                    if (! rp->empty()) {
                        continue;
                    }
                    while (ci != columns.end() && ci->first < pi->first) {
                        if (writeFlag) {
                            writer.Write(kColumnDelim);
                        }
                        ++ci;
                    }
                    if (ci == columns.end() || ci->first != pi->first) {
                        assert(pass < 1);
                        columns.setValue(pi->first, Properties::String());
                        writeFlag = false;
                        continue;
                    }
                    if (writeFlag) {
                        writer.Write(pi->second);
                        writer.Write(kColumnDelim);
                    }
                    ++ci;
                }
                if (writeFlag) {
                    writeExtra(writer, *it, ctrFirst, kColumnDelim);
                    writer.Write(kRowDelim);
                }
            }
        }
        if (writeFlag) {
            break;
        }
        writer.Clear();
    }
}

class CtrWriteExtra
{
protected:
    CtrWriteExtra()
        : mBufEnd(mBuf + kBufSize)
        {}
    template<typename T>
    void Write(IOBufferWriter& writer, T val)
    {
        const char* const b = IntToDecString(val, mBufEnd);
        writer.Write(b, mBufEnd - b);
    }
    void Write(IOBufferWriter& writer, bool val)
    {
        mBuf[0] = val ? '1' : '0';
        writer.Write(mBuf, 1);
    }

    enum { kBufSize = 32 };

    char        mBuf[kBufSize];
    char* const mBufEnd;
};

class GetHeartbeatCounters
{
public:
    typedef const Properties* iterator;

    void operator()(
        const ChunkServerPtr& srv,
        iterator&             first,
        iterator&             last) const
    {
        first = &(srv->HeartBeatProperties());
        last  = first + 1;
    }
    const Properties& operator()(const iterator& it)
        { return *it; }
};

const Properties::String kCSExtraHeaders[] = {
    "md5sum",
    "Hello-done",
    "Hello-resume",
    "Hello-resume-fail",
    "XMeta-location",
    "XMeta-retiring",
    "XMeta-restarting",
    "XMeta-responsive",
    "XMeta-space-avail",
    "XMeta-heartbeat-time",
    "XMeta-replication-read",
    "XMeta-replication-write",
    "XMeta-rack",
    "XMeta-rack-placement-weight",
    "XMeta-load-avg",
    "XMeta-to-evacuate-cnt",
    "XMeta-replay",
    "XMeta-connected",
    "XMeta-stopped",
    "XMeta-chunks"
};

class CSWriteExtra : public CtrWriteExtra
{
public:
    typedef LayoutManager::RackInfos RackInfos;

    CSWriteExtra(const RackInfos& ri)
        : CtrWriteExtra(),
          mRacks(ri),
          mNow(TimeNow())
        {}
    void operator()(
        IOBufferWriter&                       writer,
        const ChunkServerPtr&                 cs,
        const GetHeartbeatCounters::iterator& /* it */,
        const Properties::String&             columnDelim)
    {
        const ChunkServer& srv = *cs;
        writer.Write(srv.GetMd5Sum());
        writer.Write(columnDelim);
        Write(writer, srv.GetHelloDoneCount());
        writer.Write(columnDelim);
        Write(writer, srv.GetHelloResumeCount());
        writer.Write(columnDelim);
        Write(writer, srv.GetHelloResumeFailedCount());
        writer.Write(columnDelim);
        writer.Write(srv.GetHostPortStr());
        writer.Write(columnDelim);
        Write(writer, srv.IsRetiring());
        writer.Write(columnDelim);
        Write(writer, srv.IsRestartScheduled());
        writer.Write(columnDelim);
        Write(writer, srv.IsResponsiveServer());
        writer.Write(columnDelim);
        Write(writer, srv.GetAvailSpace());
        writer.Write(columnDelim);
        Write(writer, mNow - srv.TimeSinceLastHeartbeat());
        writer.Write(columnDelim);
        Write(writer, srv.GetReplicationReadLoad());
        writer.Write(columnDelim);
        Write(writer, srv.GetNumChunkReplications());
        writer.Write(columnDelim);
        const RackInfo::RackId rid = srv.GetRack();
        Write(writer, rid);
        writer.Write(columnDelim);
        RackInfos::const_iterator const rackIter = rid >= 0 ? find_if(
            mRacks.begin(), mRacks.end(),
            bind(&RackInfo::id, _1) == rid
        ) : mRacks.end();
        Write(writer, rackIter != mRacks.end() ?
            rackIter->getWeightedPossibleCandidatesCount() : int64_t(-1));
        writer.Write(columnDelim);
        Write(writer, srv.GetLoadAvg());
        writer.Write(columnDelim);
        Write(writer, srv.GetChunksToEvacuateCount());
        writer.Write(columnDelim);
        Write(writer, srv.IsReplay());
        writer.Write(columnDelim);
        Write(writer, srv.IsConnected());
        writer.Write(columnDelim);
        Write(writer, srv.IsStoppedServicing());
        writer.Write(columnDelim);
        Write(writer, srv.GetChunkCount());
    }
private:
    const RackInfos& mRacks;
    const time_t     mNow;
};

class GetChunkDirCounters
{
public:
    typedef ChunkServer::ChunkDirInfos::const_iterator iterator;

    void operator()(
        const ChunkServerPtr& srv,
        iterator&             first,
        iterator&             last) const
    {
        const ChunkServer::ChunkDirInfos& infos = srv->GetChunkDirInfos();
        first = infos.begin();
        last  = infos.end();
    }
    const Properties& operator()(const iterator& it) const
        { return it->second.first; }
};

const Properties::String kCSDirExtraHeaders[] = {
    "Chunk-server",
    "Chunk-dir",
    "Chunk-server-dir" // row id for the web ui samples collector
};

class CSDirWriteExtra : public CtrWriteExtra
{
public:
    void operator()(
        IOBufferWriter&                      writer,
        const ChunkServerPtr&                cs,
        const GetChunkDirCounters::iterator& it,
        const Properties::String&            columnDelim)
    {
        const ChunkServer& srv = *cs;
        writer.Write(srv.GetHostPortStr());
        writer.Write(columnDelim);
        writer.Write(it->second.second);
        writer.Write(columnDelim);
        writer.Write(srv.GetHostPortStr());
        writer.Write("/", 1);
        writer.Write(it->second.second);
    }
};

class CSWriteHeader
{
public:
    CSWriteHeader(
        const Properties::String* first,
        const Properties::String* last)
        : mExtraFirst(first),
          mExtraLast(last)
        {}
    void operator()(
        IOBufferWriter&           writer,
        const Properties&         columns,
        const Properties::String& columnDelim)
    {
        bool nextFlag = false;
        for (Properties::iterator it = columns.begin();
                it != columns.end();
                ++it) {
            if (nextFlag) {
                writer.Write(columnDelim);
            }
            writer.Write(it->first);
            nextFlag = true;
        }
        for (const Properties::String* it = mExtraFirst;
                it != mExtraLast;
                ++it) {
            if (nextFlag) {
                writer.Write(columnDelim);
            }
            writer.Write(*it);
            nextFlag = true;
        }
    }
private:
    const Properties::String* const mExtraFirst;
    const Properties::String* const mExtraLast;
};

void
LayoutManager::GetChunkServerCounters(IOBuffer& buf)
{
    if (! mCSCountersResponse.IsEmpty() &&
            TimeNow() < mCSCountersUpdateTime + mCSCountersUpdateInterval) {
        buf.Copy(&mCSCountersResponse,
            mCSCountersResponse.BytesConsumable());
        return;
    }
    mCSCountersResponse.Clear();
    IOBufferWriter       writer(mCSCountersResponse);
    GetHeartbeatCounters getCtrs;
    CSWriteHeader        writeHeader(kCSExtraHeaders, kCSExtraHeaders +
        sizeof(kCSExtraHeaders) / sizeof(kCSExtraHeaders[0]));
    CSWriteExtra         writeExtra(mRacks);
    WriteCounters(
        writer,
        mChunkServers.begin(),
        mChunkServers.end(),
        getCtrs,
        writeHeader,
        writeExtra
    );
    writer.Close();
    mCSCountersUpdateTime = TimeNow();
    buf.Copy(&mCSCountersResponse, mCSCountersResponse.BytesConsumable());
}

void
LayoutManager::GetChunkServerDirCounters(IOBuffer& buf)
{
    if (! mCSDirCountersResponse.IsEmpty() &&
            TimeNow() < mCSDirCountersUpdateTime +
                mCSDirCountersUpdateInterval) {
        buf.Copy(&mCSDirCountersResponse,
            mCSDirCountersResponse.BytesConsumable());
        return;
    }
    mCSDirCountersResponse.Clear();
    IOBufferWriter      writer(mCSDirCountersResponse);
    GetChunkDirCounters getCtrs;
    CSWriteHeader       writeHeader(kCSDirExtraHeaders, kCSDirExtraHeaders +
        sizeof(kCSDirExtraHeaders) / sizeof(kCSDirExtraHeaders[0]));
    CSDirWriteExtra     writeExtra;
    WriteCounters(
        writer,
        mChunkServers.begin(),
        mChunkServers.end(),
        getCtrs,
        writeHeader,
        writeExtra
    );
    writer.Close();
    mCSDirCountersUpdateTime = TimeNow();
    buf.Copy(&mCSDirCountersResponse, mCSDirCountersResponse.BytesConsumable());
}

void
LayoutManager::UpdateDelayedRecovery(const MetaFattr& fa,
        bool forceUpdateFlag /* = false */)
{
    // See comment in CanReplicateChunkNow()
    size_t const count = mChunkToServerMap.GetCount(
        CSMap::Entry::kStateDelayedRecovery);
    if (count <= 0) {
        return;
    }
    if (! fa.HasRecovery() || fa.chunkcount() <= 0 || fa.filesize <= 0) {
        return;
    }
    const bool forceFlag =
        forceUpdateFlag || mForceDelayedRecoveryUpdateFlag;
    if ((int64_t)count <= max(forceFlag ? 2 * fa.chunkcount() : 0,
            mDelayedRecoveryUpdateMaxScanCount)) {
        mChunkToServerMap.First(CSMap::Entry::kStateDelayedRecovery);
        CSMap::Entry* p;
        while ((p = mChunkToServerMap.Next(
                CSMap::Entry::kStateDelayedRecovery))) {
            if (p->GetFattr() == &fa) {
                mChunkToServerMap.SetState(*p,
                    CSMap::Entry::kStateCheckReplication);
            }
        }
        return;
    }
    if (! forceFlag) {
        // Rely on low priority replication check to eventurally
        // update the state.
        return;
    }
    StTmp<vector<MetaChunkInfo*> > cinfoTmp(mChunkInfosTmp);
    vector<MetaChunkInfo*>&        chunks = cinfoTmp.Get();
    if (metatree.getalloc(fa.id(), chunks) != 0) {
        return;
    }
    for (vector<MetaChunkInfo*>::const_iterator it = chunks.begin();
            it != chunks.end();
            ++it) {
        CSMap::Entry& entry = GetCsEntry(**it);
        if (mChunkToServerMap.GetState(entry) !=
                CSMap::Entry::kStateDelayedRecovery) {
            continue;
        }
        mChunkToServerMap.SetState(
            entry, CSMap::Entry::kStateCheckReplication);
    }
}

bool
LayoutManager::HasWriteAppendLease(chunkId_t chunkId) const
{
    const ChunkLeases::WriteLease* const wl =
        mChunkLeases.GetChunkWriteLease(chunkId);
    return (wl && wl->appendFlag);
}

bool
LayoutManager::AddServerWithStableReplica(
    CSMap::Entry& c, const ChunkServerPtr& server)
{
    size_t     srvCount = 0;
    const bool res      = AddHosted(c, server, &srvCount);
    if (! res) {
        return res;
    }
    const MetaFattr&     fa = *(c.GetFattr());
    const MetaChunkInfo& ci = *(c.GetChunkInfo());
    if (fa.filesize < 0 && ! fa.IsStriped() &&
            server->IsConnected() && ! server->IsReplay() &&
            ci.offset + (chunkOff_t)CHUNKSIZE >= fa.nextChunkOffset() &&
            ! mChunkLeases.GetValidWriteLease(ci.chunkId)) {
        KFS_LOG_STREAM_DEBUG << server->GetServerLocation() <<
            " chunk size: <" << fa.id() << "," << ci.chunkId << ">" <<
        KFS_LOG_EOM;
        server->GetChunkSize(fa.id(), ci.chunkId, ci.chunkVersion);
    }
    if (! server->IsDown()) {
        if (fa.numReplicas <= srvCount) {
            CancelPendingMakeStable(fa.id(), ci.chunkId);
        }
        if (fa.numReplicas != (0 < mChunkToServerMap.GetHibernatedCount() ?
                mChunkToServerMap.ServerCount(c) : srvCount)) {
            CheckReplication(c);
        }
    }
    return true;
}

template<typename T> const ChunkServerPtr*
LayoutManager::ReplayFindServer(const ServerLocation& loc, T& req)
{
    if (! req.replayFlag || ! loc.IsValid()) {
        panic("invalid chunk server log completion replay");
        req.status = -EFAULT;
        return 0;
    }
    Servers::const_iterator const it = FindServer(loc);
    if (mChunkServers.end() == it) {
        return 0;
    }
    return &*it;
}

inline bool
LayoutManager::RemoveServer(
    const ChunkServerPtr& server, bool replayFlag, chunkId_t chunkId)
{
    CSMap::Entry* const entry = mChunkToServerMap.Find(chunkId);
    const bool retFlag = entry &&
        mChunkToServerMap.RemoveServer(server, *entry);
    if (retFlag && replayFlag && mChunkToServerMap.ServerCount(*entry) !=
            entry->GetFattr()->numReplicas) {
        CheckReplication(*entry);
    }
    KFS_LOG_STREAM_DEBUG <<
        "-srv: "     << server->GetServerLocation() <<
        " chunk: "   << chunkId <<
        " version: " << (entry ? entry->GetChunkVersion() : seq_t(-1)) <<
        (server->IsReplay() ? " replay" : "") <<
        " removed: " << retFlag <<
        (entry ? "" : " no such chunk") <<
    KFS_LOG_EOM;
    return retFlag;
}

inline bool
LayoutManager::IsAllocationInFlight(chunkId_t chunkId)
{
    const ChunkLeases::WriteLease* const wl =
        mChunkLeases.GetChunkWriteLease(chunkId);
    return (wl && wl->allocInFlight && 0 == wl->allocInFlight->status &&
        0 < wl->allocInFlight->numReplicas);
}

void
LayoutManager::Handle(MetaChunkLogInFlight& req)
{
    if (req.replayFlag ? 0 != req.request : (! req.request || ! req.server)) {
        panic("invalid chunk log in flight");
        req.status = -EFAULT;
        return;
    }
    KFS_LOG_STREAM_DEBUG <<
        "CLIF done:"
        " status: " << req.status <<
        " "         << req.statusMsg <<
        " down: "   << (req.server && req.server->IsDown()) <<
        " "         << req.Show() <<
    KFS_LOG_EOM;
    if (0 != req.status) {
        if (req.replayFlag) {
            if (! req.server || ! req.server->IsDown()) {
                panic(
                    "invalid chunk log in flight log failed status in replay");
                req.status = -EFAULT;
            }
        } else {
            // Schedule down, as re-trying might re-order ops.
            // Doing so have no effect on primary switch over, as chunk server
            // has to re-connect to the new primary once the primary is elected.
            req.server->ScheduleDown(req.statusMsg.c_str());
            req.server->Enqueue(req);
        }
        return;
    }
    if (req.replayFlag && ! req.server) {
        const ChunkServerPtr* const cs = ReplayFindServer(req.location, req);
        if (cs) {
            (*cs)->Replay(req);
        }
    }
    int count = 0;
    if (req.server && ! req.server->IsDown()) {
        if (mChunkToServerMap.Validate(req.server)) {
            if (req.removeServerFlag) {
                if (0 <= req.chunkId && RemoveServer(
                        req.server, req.replayFlag, req.chunkId)) {
                    count++;
                }
                const MetaChunkRequest::ChunkIdSet* const ids =
                    req.GetChunkIds();
                if (ids) {
                    MetaChunkRequest::ChunkIdSet::ConstIterator it(*ids);
                    const chunkId_t*                            id;
                    while ((id = it.Next())) {
                        if (RemoveServer(req.server, req.replayFlag, *id)) {
                            count++;
                        }
                    }
                }
            } else if (! req.replayFlag && req.request &&
                    META_CHUNK_REPLICATE == req.request->op &&
                    0 <= req.request->status) {
                // Check if chunk replica has appeared while the request was
                // being logged. If it has, then chunk available was in the log
                // queue when replication was scheduled. Cancel replication if
                // chunk replica already exists.
                const CSMap::Entry* const entry =
                    mChunkToServerMap.Find(req.chunkId);
                if (entry && entry->HasServer(mChunkToServerMap, req.server)) {
                    req.request->status    = -EEXIST;
                    req.request->statusMsg = "CLIF: chunk replica exists";
                    KFS_LOG_STREAM_DEBUG <<
                        "CLIF: canceled: " << req.Show() <<
                    KFS_LOG_EOM;
                }
            }
        } else {
            panic("invalid chunk log in flight up server");
            req.status = -EFAULT;
        }
    } else {
        if (req.server && req.location != req.server->GetServerLocation()) {
            panic("invalid chunk log in flight down server location");
            req.status = -EFAULT;
        } else {
            HibernatedChunkServer* const hs = FindHibernatingCS(req.location);
            if (hs) {
                if (req.server && hs->GetGeneration() !=
                        req.server->GetHibernatedGeneration()) {
                    panic("invalid chunk log in flight down server"
                        " hibernated generation");
                    req.status = -EFAULT;
                } else {
                    if (0 <= req.chunkId) {
                        hs->UpdateLastInFlight(mChunkToServerMap, req.chunkId);
                        count++;
                    }
                    const MetaChunkRequest::ChunkIdSet* const ids =
                        req.GetChunkIds();
                    if (ids) {
                        MetaChunkRequest::ChunkIdSet::ConstIterator it(*ids);
                        const chunkId_t*                            id;
                        while ((id = it.Next())) {
                            hs->UpdateLastInFlight(mChunkToServerMap, *id);
                            count++;
                        }
                    }
                    req.status = -EIO;
                }
            } else {
                req.status = -ENOENT;
            }
        }
    }
    if (META_CHUNK_STALENOTIFY == req.reqType) {
        mStaleChunkCount->Update(count);
    }
    KFS_LOG_STREAM_DEBUG <<
        "CLIF done:"
        " status: "  << req.status <<
        " "          << req.statusMsg <<
        " server: "  << reinterpret_cast<const void*>(req.server.get()) <<
        " down: "    << (req.server && req.server->IsDown()) <<
        " updated: " << count <<
        " "          << req.Show() <<
    KFS_LOG_EOM;
    if (! req.replayFlag) {
        req.server->Enqueue(req);
    }
}

void
LayoutManager::Handle(MetaChunkLogCompletion& req)
{
    if (! req.doneOp != req.replayFlag || (req.replayFlag && 0 != req.status)) {
        panic("invalid chunk log completion");
        req.status = -EFAULT;
        return;
    }
    ChunkServerPtr server;
    if (req.doneOp) {
        server = req.doneOp->server;
        if (0 != req.status) {
            // The logic in MetaChunkLogInFlight handling applies here as well.
            server->ScheduleDown(req.statusMsg.c_str());
        }
    } else {
        const ChunkServerPtr* const cs =
            ReplayFindServer(req.doneLocation, req);
        if (cs) {
            server = *cs;
        } else {
            req.status = -ENOENT;
        }
    }
    if (server) {
        server->Handle(req);
    }
    const char* updatedMsg = "not updated";
    bool        staleFlag  = false;
    if (0 <= req.chunkId && 0 == req.status) {
        if (MetaChunkLogCompletion::kChunkOpTypeAdd == req.chunkOpType) {
            if (server && ! server->IsDown()) {
                if (! mChunkToServerMap.Validate(server)) {
                    panic("invalid chunk log completion op server");
                    req.status = -EFAULT;
                }
                if (0 == req.doneStatus) {
                    CSMap::Entry* const entry =
                        mChunkToServerMap.Find(req.chunkId);
                    if (entry && entry->GetChunkInfo()->chunkVersion ==
                            req.chunkVersion) {
                        size_t srvCount = 0;
                        const bool addedFlag =
                            AddHosted(*entry, server, &srvCount);
                        updatedMsg = addedFlag ? "added" : "not added";
                        if (req.replayFlag && addedFlag &&
                                srvCount != entry->GetFattr()->numReplicas) {
                            CheckReplication(*entry);
                        }
                    } else {
                        // Treat chunk as chunk with stale id even in the case
                        // version mismatch in order to handle possible
                        // transaction log failure, by inserting id into in
                        // flight pending stale set.
                        staleFlag = true;
                    }
                } else {
                    // Replica wasn't added to the mapping even though it
                    // possibly exists but has a different version, or stable
                    // state. Issue delete with stale id flag to add chunk id to
                    // the in flight stale id set in order to handle possible
                    // future "chunk log in flight" transaction log write
                    // failure.
                    // Replication is a special case, as on failure chunk server
                    // guarantees that it has no replica, except in the case
                    // where replica existed prior to replication which is
                    // handled by replication completion.
                    staleFlag = req.doneOp && META_CHUNK_REPLICATE !=
                        (META_CHUNK_OP_LOG_IN_FLIGHT == req.doneOp->op ?
                            static_cast<const MetaChunkLogInFlight*>(
                                req.doneOp)->reqType : req.doneOp->op);
                }
                if (staleFlag && (staleFlag =
                        0 <= req.chunkId && 0 <= req.chunkVersion)) {
                    const bool kStaleChunkIdFlag = true;
                    server->DeleteChunk(req.chunkId, kStaleChunkIdFlag);
                    if (req.replayFlag) {
                        if (0 != req.doneStatus) {
                            updatedMsg = RemoveServer(
                                server, req.replayFlag, req.chunkId) ?
                                "removed" : "not removed";
                        }
                    } else if (req.doneOp) {
                        // Mark it for completion as already deleted.
                        if (0 == req.doneOp->status) {
                            req.doneOp->status = -EINVAL;
                        }
                        req.doneOp->staleChunkIdFlag = true;
                    }
                }
            } else {
                HibernatedChunkServer* const hs =
                    FindHibernatingCS(req.doneLocation);
                if (hs) {
                    hs->UpdateLastInFlight(mChunkToServerMap, req.chunkId);
                    updatedMsg = "hibernated updated";
                }
            }
        }
    }
    if (req.replayFlag && 0 <= req.chunkId && req.doneOp &&
            META_CHUNK_OP_LOG_IN_FLIGHT == req.doneOp->op) {
        const MetaChunkLogInFlight& op =
            *static_cast<const MetaChunkLogInFlight*>(req.doneOp);
        if (META_CHUNK_MAKE_STABLE == op.reqType &&
                mPendingMakeStable.Find(req.chunkId)) {
            // Schedule replication check when this node becomes primary, in
            // order to cleanup pending make stable entry, in the case when
            // pending make stable done RPC was lost due to log write error, and
            // / or VR primary transition.
            ChangeChunkReplication(req.chunkId);
        }
    }
    KFS_LOG_STREAM_DEBUG <<
        "CLC done:"
        " status: "  << req.status <<
        " "          << req.statusMsg <<
        " server: "  << reinterpret_cast<const void*>(server.get()) <<
        " down: "    << (server && server->IsDown()) <<
        " mapping: " << updatedMsg <<
        " stale: "   << staleFlag <<
        " "          << req.Show() <<
    KFS_LOG_EOM;
    if (req.doneOp) {
        MetaChunkRequest& op = *req.doneOp;
        req.doneOp = 0;
        if (! op.suspended ||
                (! op.logCompletionSeq.IsValid() && ! op.replayFlag)) {
            panic("MetaChunkLogCompletion: invalid log sequence");
        }
        op.logCompletionSeq = MetaVrLogSeq();
        if (op.replayFlag) {
            MetaRequest::Release(&op);
        } else {
            op.resume();
        }
    }
}

void
LayoutManager::Handle(MetaHibernatedPrune& req)
{
    if (IsMetaLogWriteOrVrError(req.status)) {
        if (req.replayFlag) {
            panic("invalid meta hibernate prune");
        } else {
            ScheduleResubmitOrCancel(req);
        }
        return;
    }
    HibernatedChunkServer* hs;
    if (0 == req.status) {
        hs = FindHibernatingCS(req.location);
        if (! hs) {
            req.status = -ENOENT;
        }
    } else {
        hs = 0;
    }
    HibernatedChunkServer::Handle(hs, req);
}

void
LayoutManager::Handle(MetaHibernatedRemove& req)
{
    if (0 != req.status) {
        if (req.replayFlag) {
            panic("invalid meta hibernate remove");
            return;
        }
        if (! IsMetaLogWriteOrVrError(req.status)) {
            panic("invalid meta hibernate remove status");
            return;
        }
    }
    HibernatedServerInfos::iterator it;
    if (! FindHibernatingCSInfo(req.location, &it)) {
        if (0 == req.status) {
            req.status = -ENOENT;
        }
        return;
    }
    if (0 != req.status) {
        if (&req == it->removeOp) {
            // Handle transaction log write failure, by resetting pointer, and
            // relying on the timer cleanup to re-issue remove again.
            it->removeOp = 0;
        }
        return;
    }
    if (! req.replayFlag && it->removeOp != &req) {
        panic("invalid hibernated server remove completion");
    }
    if (it->IsHibernated()) {
        if (mChunkToServerMap.RemoveHibernatedServer(it->csmapIdx)) {
            ScheduleCleanup();
        } else {
            panic("failed to remove hibernated server");
        }
    }
    mHibernatingServers.erase(it);
}

bool
LayoutManager::RestoreChunkServer(
    const ServerLocation& loc,
    size_t                idx,
    size_t                chunks,
    const CIdChecksum&    chksum,
    bool                  retiringFlag,
    int64_t               retStart,
    int64_t               retDown,
    bool                  retiredFlag,
    LayoutManager::RackId rackId,
    bool                  pendingHelloNotifyFlag)
{
    if (mRestoreChunkServerPtr) {
        return false;
    }
    if (! mChunkToServerMap.SetDebugValidate(false)) {
        KFS_LOG_STREAM_ERROR <<
            "chunk server map debug validate cannot be set with replay" <<
        KFS_LOG_EOM;
        return false;
    }
    Servers::iterator const it = lower_bound(
        mChunkServers.begin(), mChunkServers.end(),
        loc, bind(&ChunkServer::GetServerLocation, _1) < loc
    );
    if ( mChunkServers.end() != it && (*it)->GetServerLocation() == loc) {
        KFS_LOG_STREAM_ERROR <<
            "duplicate chunk server: " << loc <<
        KFS_LOG_EOM;
        return false;
    }
    ChunkServerPtr const server = ChunkServer::Create(
        NetConnectionPtr(new NetConnection(new TcpSocket(), 0)),
        loc
    );
    if (! mChunkToServerMap.RestoreServer(server, idx, chunks, chksum)) {
        return false;
    }
    if (! server->IsReplay() || mReplaySetRackFlag) {
        SetRack(server, rackId);
    } else {
        server->SetRack(rackId);
    }
    // Chunk server in checkpoint can only appear as a result of hello
    // processing -- set chunk server state machine state to the end of hello
    // chunk inventory processing.
    server->HelloDone(0);
    server->HelloEnd();
    server->SetPendingHelloNotify(pendingHelloNotifyFlag);
    if (retiringFlag || 0 <= retDown) {
        server->SetRetiring(retStart, retDown);
    }
    if (server->IsHibernating()) {
        HibernatedServerInfos::iterator it;
        if (FindHibernatingCSInfo(loc, &it)) {
            KFS_LOG_STREAM_ERROR <<
                "duplicate retiring server: " << loc <<
            KFS_LOG_EOM;
            return false;
        }
        mHibernatingServers.insert(it, HibernatingServerInfo(
            loc, retStart, max(int64_t(0), retDown), server->IsReplay())
        )->retiredFlag = retiredFlag;
    }
    if (server->IsReplay()) {
        mReplayServerCount++;
    }
    mChunkServers.insert(it, server);
    mRestoreChunkServerPtr = server;
    return true;
}

bool
LayoutManager::RestoreHibernatedCS(
    const ServerLocation& loc,
    size_t                idx,
    size_t                chunks,
    const CIdChecksum&    chksum,
    const CIdChecksum&    modChksum,
    int64_t               startTime,
    int64_t               endTime,
    bool                  retiredFlag,
    bool                  pendingHelloNotifyFlag)
{
    if (mRestoreHibernatedCSPtr) {
        return false;
    }
    HibernatedServerInfos::iterator it;
    if (FindHibernatingCSInfo(loc, &it)) {
        KFS_LOG_STREAM_ERROR <<
            "duplicate hibernated chunk server: " << loc <<
        KFS_LOG_EOM;
        return false;
    }
    HibernatedChunkServerPtr const server(new HibernatedChunkServer(
        loc, modChksum, pendingHelloNotifyFlag));
    if (! mChunkToServerMap.RestoreHibernatedServer(
            server, idx, chunks, chksum)) {
        return false;
    }
    mHibernatingServers.insert(it, HibernatingServerInfo(
        loc, startTime, endTime - startTime, server->IsReplay(), idx,
        retiredFlag));
    mRestoreHibernatedCSPtr = server;
    return true;
}

void
LayoutManager::Replay(MetaHello& req)
{
    if (req.server) {
        return;
    }
    if (! req.location.IsValid() || 0 != req.status) {
        panic("invalid chunk server hello replay");
        req.status = -EFAULT;
        return;
    }
    req.server = ChunkServer::Create(
        NetConnectionPtr(new NetConnection(new TcpSocket(), 0)),
        req.location);
    if (! req.server) {
        req.status = -EFAULT;
    } else {
        req.clnt = &*req.server;
        if (req.peerName.empty()) {
            req.peerName = "replay";
        }
    }
}

void
LayoutManager::SetRack(const ChunkServerPtr& server,
    LayoutManager::RackId rackId)
{
    server->SetRack(rackId);
    if (0 <= rackId) {
        RackInfos::iterator const rackIter = lower_bound(
            mRacks.begin(), mRacks.end(),
            rackId, bind(&RackInfo::id, _1) < rackId
        );
        if (rackIter != mRacks.end() && rackIter->id() == rackId) {
            rackIter->addServer(server);
        } else {
            RackWeights::const_iterator const it =
                mRackWeights.find(rackId);
            mRacks.insert(rackIter, RackInfo(
                rackId,
                it != mRackWeights.end() ? it->second : double(1),
                server
            ));
        }
    } else {
        KFS_LOG_STREAM_DEBUG << server->GetServerLocation() <<
            ": no rack specified: " << rackId <<
        KFS_LOG_EOM;
    }
}

bool
LayoutManager::AddToInFlightChunkAllocation(
    const MetaAllocate& req, const ChunkServerPtr& server)
{
    // Add server to in flight allocation / versions change if chunk
    // allocation was already issued to chunk servers, but not all chunk
    // servers responded yet.
    seq_t versionRollBack;
    if (0 == req.status &&
            req.suspended &&
            0 < req.numReplicas &&
            ! req.invalidateAllFlag &&
            0 < (versionRollBack = GetChunkVersionRollBack(req.chunkId)) &&
            req.initialChunkVersion + versionRollBack == req.chunkVersion &&
            req.numServerReplies < req.servers.size() &&
            req.servers.end() == find_if(
                req.servers.begin(), req.servers.end(),
                bind(&ChunkServer::GetServerLocation, _1) ==
                    server->GetServerLocation())) {
        KFS_LOG_STREAM_DEBUG <<
            "adding server: " << server->GetServerLocation() <<
            " to " << req.Show() <<
        KFS_LOG_EOM;
        MetaAllocate& alloc = const_cast<MetaAllocate&>(req);
        alloc.servers.push_back(server);
        server->AllocateChunk(alloc, -1, alloc.minSTier);
        return true;
    }
    return false;
}

/// Add the newly joined server to the list of servers we have.  Also,
/// update our state to include the chunks hosted on this server.
void
LayoutManager::AddNewServer(MetaHello& req)
{
    if (req.replayFlag) {
        Replay(req);
    }
    if (0 != req.status) {
        return;
    }
    ChunkServer& srv = *(req.server);
    if (srv.IsDown()) {
        panic("add new server: invalid down state");
        req.status = -EFAULT;
        return;
    }
    const ServerLocation& srvId = srv.GetServerLocation();
    if (srvId != req.location || ! srvId.IsValid()) {
        panic("add new server: invalid server location");
        req.status = -EFAULT;
        return;
    }
    Servers::iterator const existing = lower_bound(
        mChunkServers.begin(), mChunkServers.end(),
        srvId, bind(&ChunkServer::GetServerLocation, _1) < srvId);
    if (existing != mChunkServers.end() &&
            (*existing)->GetServerLocation() == srvId) {
        KFS_LOG_STREAM_DEBUG <<
            "duplicate server: " << srvId <<
            " possible reconnect:"
            " existing: " << (const void*)&*existing <<
            " new: "      << (const void*)&srv <<
            " resume: "   << req.resumeStep <<
        KFS_LOG_EOM;
        if (*existing == req.server) {
            panic("invalid duplicate attempt to add chunk server");
            req.status = -EFAULT;
            return;
        }
        if ((*existing)->IsDown()) {
            req.statusMsg = "down server exists";
        } else {
            req.statusMsg = "up server exists";
        }
        req.statusMsg += ", retry resume later";
        req.status = -EEXIST;
        if (! req.replayFlag && mPrimaryFlag &&
                ! (*existing)->IsDuplicateChannel(req.channelId)) {
            (*existing)->ScheduleDown("reconnect");
        }
        return;
    }

    HibernatedChunkServer::DeletedChunks  staleChunkIds;
    HibernatedChunkServer::ModifiedChunks modififedChunks;
    if (0 <= req.resumeStep) {
        HibernatedServerInfos::iterator it;
        HibernatedChunkServer* const    cs =
            FindHibernatingCS(req.location, &it);
        if (! cs) {
            req.statusMsg = "resume not possible, no hibernated info exists";
            req.status    = -EAGAIN;
            return;
        }
        if (0 == req.resumeStep && req.logseq.IsValid()) {
            // Step 0 is not written into transaction log.
            const char* const msg =
                "invalid hello resume step logged / replayed";
            panic(msg);
            req.statusMsg = msg;
            req.status    = -EFAULT;
            return;
        }
        if (cs->HelloResumeReply(
                req, mChunkToServerMap, staleChunkIds, modififedChunks)) {
            return;
        }
        if ((int)it->csmapIdx != cs->GetIndex() ||
                ! mChunkToServerMap.ReplaceHibernatedServer(
                    req.server, it->csmapIdx)) {
            const char* const msg = "failed to replace hibernated server";
            panic(msg);
            req.statusMsg = msg;
            req.status    = -EFAULT;
            mHibernatingServers.erase(it);
            return;
        }
        mHibernatingServers.erase(it);
    } else {
        HibernatedServerInfos::iterator it;
        HibernatingServerInfo* const    hs =
            FindHibernatingCSInfo(req.location, &it);
        if (hs) {
            if (it->IsHibernated()) {
                KFS_LOG_STREAM(req.replayFlag ?
                        MsgLogger::kLogLevelDEBUG :
                        MsgLogger::kLogLevelINFO) <<
                    "hibernated server: " << it->location  <<
                    " is back as promised" <<
                KFS_LOG_EOM;
                if (! mChunkToServerMap.RemoveHibernatedServer(it->csmapIdx)) {
                    panic("add new server: failed to remove hibernated server");
                }
            }
            mHibernatingServers.erase(it);
        }
        // Add server first, then add chunks, otherwise if/when the server goes
        // down in the process of adding chunks, taking out server from chunk
        // info will not work in ServerDown().
        bool addedFlag;
        if (! (addedFlag = mChunkToServerMap.AddServer(req.server))) {
            if (req.replayFlag) {
                mChunkToServerMap.RemoveServerCleanup(0);
                addedFlag = mChunkToServerMap.AddServer(req.server);
            }
            // Hello start method must ensure that sufficient number of slots
            // is available, therefore adding server must not fail here.
            if (! addedFlag) {
                KFS_LOG_STREAM_FATAL <<
                    "failed to add server: " << srvId <<
                    " no slots available "
                    " servers: " << mChunkToServerMap.GetServerCount() <<
                    " / " << mChunkServers.size() <<
                    " hibernated: " << mChunkToServerMap.GetHibernatedCount() <<
                    " slots: " <<
                        mChunkToServerMap.GetAvailableServerSlotCount() <<
                KFS_LOG_EOM;
                panic("add new server: out of chunk servers slots");
                req.statusMsg = "out of chunk server slots, try again later";
                req.status    = -EBUSY;
                return;
            }
        }
    }
    if (srv.IsReplay()) {
        mReplayServerCount++;
    }
    mChunkServers.insert(existing, req.server);

    const uint64_t allocSpace = req.chunks.size() * CHUNKSIZE;
    srv.SetSpace(req.totalSpace, req.usedSpace, allocSpace);
    // Ensure that rack exists before invoking UpdateSrvLoadAvg(), as it
    // can update rack possible allocation candidates count.
    if (! srv.IsReplay() || mReplaySetRackFlag) {
        SetRack(req.server, req.rackId);
    } else {
        srv.SetRack(req.rackId);
    }
    if (! srv.IsReplay()) {
        if (mAssignMasterByIpFlag) {
            // if the server node # is odd, it is master; else slave
            const string&           ipaddr    = req.location.hostname;
            string::size_type const len       = ipaddr.length();
            int                     lastDigit = -1;
            if (0 < len) {
                const int sym = ipaddr[len - 1] & 0xFF;
                if ('0' <= sym && sym <= '9') {
                    lastDigit = sym - '0'; // ipv4 or ipv6 last digit
                } else if (sym == ':' ) {
                    lastDigit = 0; // ipv6 16 bit 0
                } else if ('a' <= sym && sym <= 'f') {
                    lastDigit = sym - 'a' + 10; // ipv6 last digit
                } else if ('A' <= sym && sym <= 'F') {
                    lastDigit = sym - 'A' + 10; // ipv6 last digit
                }
            }
            if (lastDigit < 0) {
                srv.SetCanBeChunkMaster(mMastersCount <= mSlavesCount);
            } else {
                srv.SetCanBeChunkMaster((lastDigit & 0x1) != 0);
            }
        } else {
            srv.SetCanBeChunkMaster(mMastersCount <= mSlavesCount);
        }
        if (srv.CanBeChunkMaster()) {
            mMastersCount++;
        } else {
            mSlavesCount++;
        }
    }
    srv.HelloDone(&req);
    UpdateSrvLoadAvg(srv, 0, 0);
    if (! mChunkServersProps.empty()) {
        srv.SetProperties(mChunkServersProps);
    }
    int maxLogInfoCnt = 32;
    for (MetaHello::ChunkInfos::const_iterator it = req.chunks.begin();
            it != req.chunks.end();
            ++it) {
        const chunkId_t     chunkId      = it->chunkId;
        const char*         staleReason  = 0;
        CSMap::Entry* const cmi          = mChunkToServerMap.Find(chunkId);
        seq_t               chunkVersion = -1;
        if (cmi) {
            CSMap::Entry& c = *cmi;
            if (0 < req.resumeStep) {
                staleChunkIds.Erase(chunkId);
                const bool removedFlag =
                    c.Remove(mChunkToServerMap, req.server);
                if (modififedChunks.Erase(chunkId)) {
                    if (! removedFlag ) {
                        panic("stable: invalid modified chunk list");
                    }
                }
            }
            const fid_t          fileId = c.GetFileId();
            const ChunkServerPtr cs     = c.GetServer(
                mChunkToServerMap, srv.GetServerLocation());
            if (cs) {
                const bool duplicateEntryFlag = &*cs == &srv;
                KFS_LOG_STREAM_ERROR << srvId <<
                    " stable chunk: <"     << fileId << "," << chunkId << ">" <<
                    " already hosted on: " << (const void*)&*cs <<
                    " / "                  << (const void*)&srv <<
                    " location: "          << srv.GetServerLocation() <<
                    (duplicateEntryFlag ?
                        " duplicate chunk entry" :
                        " stale chunk to server mapping") <<
                KFS_LOG_EOM;
                if (duplicateEntryFlag) {
                    // Ignore duplicate chunk inventory entries.
                    continue;
                }
                panic("invalid duplicate chunk to server mapping");
            }
            const MetaChunkInfo& ci = *(cmi->GetChunkInfo());
            chunkVersion = ci.chunkVersion;
            if (chunkVersion != it->chunkVersion) {
                if (it->chunkVersion < chunkVersion) {
                    staleReason = "lower chunk version";
                } else if (chunkVersion + GetChunkVersionRollBack(chunkId) <
                        it->chunkVersion) {
                    staleReason = "higher chunk version";
                } else {
                    // Even if allocation is in flight let version change
                    // completion decide the outcome. If allocation successfully
                    // finishes then completion will issue replica delete,
                    // or otherwise add replica.
                    const bool kMakeStableFlag = false;
                    const bool kPendingAddFlag = true;
                    srv.NotifyChunkVersChange(
                        fileId,
                        chunkId,
                        chunkVersion,     // to
                        it->chunkVersion, // from
                        kMakeStableFlag,
                        kPendingAddFlag
                    );
                }
            } else {
                const ChunkLeases::WriteLease* wl;
                if (! req.replayFlag &&
                        (wl = mChunkLeases.GetChunkWriteLease(chunkId)) &&
                        wl->allocInFlight &&
                        wl->allocInFlight->initialChunkVersion ==
                            chunkVersion) {
                    AddToInFlightChunkAllocation(
                        *wl->allocInFlight, req.server);
                }
                // This chunk is non-stale. Check replication,
                // and update file size if this is the last
                // chunk and update required.
                AddServerWithStableReplica(c, req.server);
            }
        } else {
            if (0 < req.resumeStep && modififedChunks.Find(chunkId)) {
                panic("stable: invalid modified chunk list");
            }
            staleReason = "no chunk mapping exists";
        }
        if (staleReason) {
            maxLogInfoCnt--;
            KFS_LOG_STREAM((0 < maxLogInfoCnt && ! req.replayFlag) ?
                    MsgLogger::kLogLevelINFO :
                    MsgLogger::kLogLevelDEBUG) <<
                srvId <<
                " stable chunk: " << chunkId <<
                " version: "      << it->chunkVersion <<
                " / "             << chunkVersion <<
                " "               << staleReason <<
                " => stale" <<
            KFS_LOG_EOM;
            staleChunkIds.Insert(chunkId);
        }
    }
    for (int i = 0; i < 2; i++) {
        const MetaHello::ChunkInfos& chunks = i == 0 ?
            req.notStableAppendChunks : req.notStableChunks;
        int maxLogInfoCnt = 64;
        for (MetaHello::ChunkInfos::const_iterator it = chunks.begin();
                it != chunks.end();
                ++it) {
            const char* staleReason = 0;
            if (0 < req.resumeStep) {
                CSMap::Entry* const cmi = mChunkToServerMap.Find(it->chunkId);
                if (cmi) {
                    const bool removedFlag =
                        cmi->Remove(mChunkToServerMap, req.server);
                    if (modififedChunks.Erase(it->chunkId)) {
                        if (! removedFlag) {
                            panic(string("not stable") +
                                (i == 0 ? "append" : "") +
                                ": invalid modified chunk list"
                            );
                        }
                    }
                    staleChunkIds.Erase(it->chunkId);
                } else {
                    if (modififedChunks.Find(it->chunkId)) {
                        panic(string("not stable") +
                            (i == 0 ? "append" : "") +
                            ": invalid modified chunk list"
                        );
                    }
                    staleReason = "no chunk mapping exists";
                }
            }
            if (! staleReason) {
                staleReason = AddNotStableChunk(
                    req.server,
                    it->chunkId,
                    it->chunkVersion,
                    i == 0,
                    srvId
                );
            }
            maxLogInfoCnt--;
            KFS_LOG_STREAM((0 < maxLogInfoCnt && ! req.replayFlag) ?
                    MsgLogger::kLogLevelINFO :
                    MsgLogger::kLogLevelDEBUG) <<
                srvId <<
                " not stable" << (i == 0 ? " append" : "") <<
                " chunk: "    << it->chunkId <<
                " version: "  << it->chunkVersion <<
                " " << (staleReason ? staleReason : "") <<
                (staleReason ? " => stale" : "added back") <<
            KFS_LOG_EOM;
            if (staleReason) {
                staleChunkIds.Insert(it->chunkId);
            }
            // MakeChunkStableDone will process pending recovery.
        }
    }
    if (0 < req.resumeStep) {
        for (MetaHello::ChunkIdList::const_iterator
                it = req.missingChunks.begin();
                it != req.missingChunks.end();
                ++it) {
            const chunkId_t chunkId = *it;
            staleChunkIds.Erase(chunkId);
            const bool      modFlag = modififedChunks.Erase(chunkId);
            CSMap::Entry* const cmi = mChunkToServerMap.Find(chunkId);
            if (! cmi || ! cmi->Remove(mChunkToServerMap, req.server)) {
                if (modFlag) {
                    panic("missing chunks: invalid modified chunk list");
                }
                continue;
            }
            KFS_LOG_STREAM_DEBUG <<
                "-srv: "   << srvId <<
                " chunk: " << chunkId <<
                " missing" <<
            KFS_LOG_EOM;
            const MetaFattr& fa = *(cmi->GetFattr());
            if (fa.numReplicas != mChunkToServerMap.AllServerCount(*cmi)) {
                CheckReplication(*cmi);
            }
        }
        for (; ;) {
            HibernatedChunkServer::ModifiedChunks::ConstIterator
                it(modififedChunks);
            const chunkId_t* id = it.Next();
            if (! id) {
                break;
            }
            const chunkId_t chunkId = *id;
            modififedChunks.Erase(chunkId);
            CSMap::Entry* const cmi = mChunkToServerMap.Find(chunkId);
            if (! cmi || ! cmi->Remove(mChunkToServerMap, req.server)) {
                panic("invalid modified chunk list");
                staleChunkIds.Erase(chunkId);
                continue;
            }
            staleChunkIds.Erase(chunkId);
            seq_t const chunkVersion = cmi->GetChunkInfo()->chunkVersion;
            KFS_LOG_STREAM_DEBUG <<
                "-srv: "     << srvId <<
                " chunk: "   << chunkId <<
                " change stable modified"
                " version: " <<
                    chunkVersion + GetChunkVersionRollBack(chunkId) <<
                " -> "       << chunkVersion <<
            KFS_LOG_EOM;
            const bool                kMakeStableFlag   = false;
            const bool                kPendingAddFlag   = true;
            const bool                kVerifyStableFlag = true;
            MetaChunkReplicate* const kReplicateOp      = 0;
            srv.NotifyChunkVersChange(
                cmi->GetFileId(),
                chunkId,
                chunkVersion, // to.
                chunkVersion + GetChunkVersionRollBack(chunkId),
                kMakeStableFlag,
                kPendingAddFlag,
                kReplicateOp,
                kVerifyStableFlag
            );
        }
    }
    const size_t staleCnt = staleChunkIds.Size();
    if (0 < staleCnt || ! req.pendingStaleChunks.empty()) {
        // Even with no stale chunks, if hello has pending stale chunks wait
        // for chunk server's stale queue to advance past the point of where it
        // is at this moment in order to ensure that that the chunks in the
        // stale queue are deleted.
        srv.NotifyStaleChunks(staleChunkIds, req);
    }
    // All ops are queued at this point, make sure that the server is still up.
    // Chunk server cannot possibly go down here, as with hello already in
    // flight it can only transition down as a result of "bye" internal RPC
    // execution, and the later must be successfully written into the
    // transaction log prior to the execution.
    if (srv.IsDown()) {
        KFS_LOG_STREAM_FATAL << srvId <<
            ": went down in the process of adding it" <<
        KFS_LOG_EOM;
        panic("add new server: invalid state transition to down");
        return;
    }
    // End of hello chunk inventory processing.
    srv.HelloEnd();
    // Update the list since a new server appeared.
    if (! req.replayFlag) {
        CheckHibernatingServersStatus();
    }
    const char* msg = "added";
    if (! req.replayFlag &&
            IsChunkServerRestartAllowed() &&
            mCSToRestartCount < mMaxCSRestarting) {
        if (srv.Uptime() >= GetMaxCSUptime() &&
                ! srv.IsRestartScheduled()) {
            mCSToRestartCount++;
            if (srv.CanBeChunkMaster()) {
                mMastersToRestartCount++;
            }
            if (srv.GetNumChunkWrites() <= 0 &&
                    srv.GetNumAppendsWithWid() <= 0) {
                srv.Restart(mRetireOnCSRestartFlag);
                msg = "restarted";
            } else {
                srv.ScheduleRestart(
                    mCSGracefulRestartTimeout,
                    mCSGracefulRestartAppendWithWidTimeout);
            }
        } else {
            ScheduleChunkServersRestart();
        }
    }
    UpdateReplicationsThreshold();
    KFS_LOG_STREAM(req.replayFlag ?
            MsgLogger::kLogLevelDEBUG :
            MsgLogger::kLogLevelINFO) <<
        msg <<
        " chunk server: "       << srv.GetServerLocation() <<
        " / "                   << req.peerName <<
        " "                     << reinterpret_cast<const void*>(&srv) <<
        (srv.CanBeChunkMaster() ? " master" : " slave") <<
        " logseq: "             << req.logseq <<
        " resume: "             << req.resumeStep <<
        " rack: "               << req.rackId <<
        " checksum: "           << srv.GetChecksum() <<
        " chunks: "             << srv.GetChunkCount() <<
        " stable: "             << req.chunks.size() <<
        " not stable: "         << req.notStableChunks.size() <<
        " append: "             << req.notStableAppendChunks.size() <<
        " +wid: "               << req.numAppendsWithWid <<
        " writes: "             << srv.GetNumChunkWrites() <<
        " +wid: "               << srv.GetNumAppendsWithWid() <<
        " stale: "              << staleCnt <<
        " masters: "            << mMastersCount <<
        " slaves: "             << mSlavesCount <<
        " total: "              << mChunkServers.size() <<
        " uptime: "             << srv.Uptime() <<
        " restart: "            << srv.IsRestartScheduled() <<
        " to restart: "         << mCSToRestartCount <<
        " to restart masters: " << mMastersToRestartCount <<
    KFS_LOG_EOM;
}

const char*
LayoutManager::AddNotStableChunk(
    const ChunkServerPtr& server,
    chunkId_t             chunkId,
    seq_t                 chunkVersion,
    bool                  appendFlag,
    const ServerLocation& logPrefix)
{
    CSMap::Entry* const cmi = mChunkToServerMap.Find(chunkId);
    if (! cmi) {
        return "no chunk mapping exists";
    }
    CSMap::Entry&        pinfo  = *cmi;
    const fid_t          fileId = pinfo.GetFileId();
    const ChunkServerPtr cs     = pinfo.GetServer(
        mChunkToServerMap, server->GetServerLocation());
    if (cs) {
        KFS_LOG_STREAM_ERROR << logPrefix <<
            " not stable chunk:" <<
                (appendFlag ? " append " : "") <<
            " <"                   << fileId <<
            ","                    << chunkId << ">" <<
            " already hosted on: " << (const void*)&*cs <<
            " new server: "        << (const void*)&*server <<
            (&*cs == &*server ?
            " duplicate chunk entry" :
            " possible stale chunk to server mapping entry") <<
        KFS_LOG_EOM;
        return 0;
    }
    if (server->IsReplay()) {
        // In hello replay add mapping if chunk exists, regardless of the
        // version, effectively deferring decision to the primary. The primary
        // will eventually issue stale chunk, make stable, and/or version
        // change which will be written into the transaction log. The chunk
        // server cannot make chunk stable on its own, it always must be
        // instructed by the meta server to do so. The chunk server must
        // report all non stable chunk in meta hello, and remove non stable
        // chunks from chunk inventory checksum, it follows that even if the
        // primary fails to make or convey its decision. the chunk will be in
        // the hello on chunk server reconnect -- no need to add chunk to the
        // set of chunk ids "manipulated" as a result of processing hello.
        AddHosted(chunkId, pinfo, server);
        return 0;
    }
    const char* staleReason = 0;
    if (AddServerToMakeStable(pinfo, server,
            chunkId, chunkVersion, staleReason) || staleReason) {
        return staleReason;
    }
    // At this point it is known that no make chunk stable is in progress:
    // AddServerToMakeStable() invoked already.
    // Delete the replica if sufficient number of replicas already exists.
    const MetaFattr * const fa = pinfo.GetFattr();
    if (fa &&
            fa->numReplicas <= mChunkToServerMap.ConnectedServerCount(pinfo)) {
        CancelPendingMakeStable(fileId, chunkId);
        return "sufficient number of replicas exists";
    }
    // See if it is possible to add the chunk back before "[Begin] Make
    // Chunk Stable" ([B]MCS) starts. Expired lease cleanup lags behind. If
    // expired lease exists, and [B]MCS is not in progress, then add the
    // chunk back.
    const ChunkLeases::WriteLease* const wl =
        mChunkLeases.GetChunkWriteLease(chunkId);
    if (wl && appendFlag != wl->appendFlag) {
        return (appendFlag ? "not append lease" : "append lease");
    }
    if ((! wl || wl->relinquishedFlag) && appendFlag) {
        const PendingMakeStableEntry* const msi =
            mPendingMakeStable.Find(chunkId);
        if (! msi) {
            return "no make stable info";
        }
        if (chunkVersion != msi->mChunkVersion) {
            return "pending make stable chunk version mismatch";
        }
        const bool beginMakeStableFlag = msi->mSize < 0;
        if (beginMakeStableFlag) {
            AddHosted(chunkId, pinfo, server);
            if (InRecoveryPeriod() || ! mPendingBeginMakeStable.IsEmpty()) {
                // Allow chunk servers to connect back.
                bool insertedFlag = false;
                mPendingBeginMakeStable.Insert(
                    chunkId, chunkVersion, insertedFlag);
                return 0;
            }
            assert(! wl || ! wl->stripedFileFlag);
            const bool kStripedFileFlag    = false;
            const bool leaseRelinquishFlag = false;
            MakeChunkStableInit(
                pinfo,
                chunkVersion,
                string(),
                beginMakeStableFlag,
                -1,
                false,
                0,
                kStripedFileFlag,
                appendFlag,
                leaseRelinquishFlag
            );
            return 0;
        }
        const bool kPendingAddFlag = true;
        server->MakeChunkStable(
            fileId, chunkId, chunkVersion,
            msi->mSize,
            msi->mHasChecksum,
            msi->mChecksum,
            kPendingAddFlag
        );
        return 0;
    }
    if (! wl && ! appendFlag && mPendingMakeStable.Find(chunkId)) {
        return "chunk was open for append";
    }
    const seq_t curChunkVersion = pinfo.GetChunkInfo()->chunkVersion;
    if (chunkVersion < curChunkVersion) {
        return "lower chunk version";
    }
    if (chunkVersion > curChunkVersion + GetChunkVersionRollBack(chunkId)) {
        // This indicates that part of meta server log or checkpoint
        // was lost, or rolled back to the previous state.
        return "higher chunk version";
    }
    if (curChunkVersion != chunkVersion &&
            (appendFlag || ! wl || ! wl->allocInFlight)) {
        // Make stable below will be invoked to make the chunk stable
        const bool kMakeStableFlag = false;
        server->NotifyChunkVersChange(
            fileId, chunkId, curChunkVersion, chunkVersion,
            kMakeStableFlag);
        if (server->IsDown()) {
            // Went down while sending notification.
            // Op completion invokes required error handling.
            return 0;
        }
        // AddHosted below completes before the the notification op
        // completion.
    }
    // Adding server back can change replication chain (order) -- invalidate
    // record appender cache to prevent futher appends to this chunk.
    if (wl) {
        if (appendFlag) {
            mARAChunkCache.Invalidate(fileId, chunkId);
        } else if (wl->allocInFlight) {
            if (0 == wl->allocInFlight->status) {
                if (wl->allocInFlight->initialChunkVersion != chunkVersion ||
                        ! AddToInFlightChunkAllocation(
                            *wl->allocInFlight, server)) {
                    return "re-allocation in flight";
                }
            } else if (wl->allocInFlight->chunkVersion != chunkVersion) {
                // Allocation / version change has already
                // failed, but the allocation op still pending
                // waiting remaining replies.
                // Set allocation version to ensure that the
                // version roll back won't fail when the final
                // reply comes in.
                const bool kMakeStableFlag = false;
                server->NotifyChunkVersChange(
                    fileId,
                    chunkId,
                    wl->allocInFlight->chunkVersion, // to
                    chunkVersion,                    // from
                    kMakeStableFlag);
                if (server->IsDown()) {
                    return 0; // Went down do not add chunk.
                }
            }
        }
        AddHosted(chunkId, pinfo, server);
    } else if (! appendFlag) {
        const bool kPendingAddFlag = true;
        server->MakeChunkStable(
            fileId, chunkId, curChunkVersion,
            -1, false, 0, kPendingAddFlag
        );
    }
    return 0;
}

void
LayoutManager::Done(MetaChunkVersChange& req)
{
    if (req.replayFlag) {
        return;
    }
    if (req.replicate) {
        assert(req.replicate->versChange = &req);
        Handle(*req.replicate);
        MetaChunkReplicate* const repl = req.replicate;
        req.replicate = 0;
        if (repl && ! repl->suspended) {
            submit_request(repl);
        }
        return;
    }
    CSMap::Entry* const cmi = mChunkToServerMap.Find(req.chunkId);
    if (! cmi) {
        KFS_LOG_STREAM_INFO << req.Show() <<
            " chunk no longer esists,"
            " declaring stale replica" <<
        KFS_LOG_EOM;
        if (0 == req.status || ! req.staleChunkIdFlag) {
            req.server->ForceDeleteChunk(req.chunkId);
        }
        return;
    }
    UpdateReplicationState(*cmi);
    if (0 != req.status) {
        KFS_LOG_STREAM_ERROR << req.Show() <<
            " status: "     << req.status <<
            " msg: "        << req.statusMsg <<
            " pendingAdd: " << req.pendingAddFlag <<
        KFS_LOG_EOM;
        if (req.pendingAddFlag) {
            // In case of pending add log completion must have already issued
            // chunk delete.
            if (! req.staleChunkIdFlag && ! req.server->IsDown()) {
                req.server->NotifyStaleChunk(req.chunkId);
            }
        } else {
            ChunkCorrupt(req.chunkId, req.server);
        }
        return;
    }
    if (! req.pendingAddFlag || req.server->IsDown()) {
        return;
    }
    // For now do not start recursive version change.
    // If this was the last replica, then another round of version change
    // won't start. If another replica existed, and version change has
    // failed, then such sequence might result in loosing all replicas.
    //
    // This replica is assumed to be stable.
    // Non stable replica add path, uses another code path, which ends
    // with make chunk stable.
    // Ensure that no write lease exists.
    if (GetChunkVersionRollBack(req.chunkId) <= 0 ||
            mChunkLeases.GetChunkWriteLease(req.chunkId)) {
        KFS_LOG_STREAM_INFO << req.Show() <<
            " chunk allocation in flight, declaring stale replica" <<
        KFS_LOG_EOM;
        req.server->NotifyStaleChunk(req.chunkId);
        return;
    }
    // Coalesce block can change file id while version change
    // was in flight. Use fid from the chunk mappings.
    const MetaChunkInfo* const ci = cmi->GetChunkInfo();
    if (ci->chunkVersion != req.chunkVersion) {
        KFS_LOG_STREAM_INFO << req.Show() <<
            " chunk version mismatch,"
            " declaring replica stale" <<
        KFS_LOG_EOM;
        req.server->NotifyStaleChunk(req.chunkId);
        return;
    }
    // The chunk should have been added by MetaChunkLogCompletion.
    if (! mChunkToServerMap.HasServer(req.server, *cmi)) {
        KFS_LOG_STREAM_ERROR << req.Show() <<
            " no such server, or mappings update failed" <<
        KFS_LOG_EOM;
        return;
    }
    KFS_LOG_STREAM_INFO << req.Show() <<
        " replica added: " << req.server->GetServerLocation() <<
    KFS_LOG_EOM;
    const MetaFattr* const fa     = cmi->GetFattr();
    const fid_t            fileId = fa->id();
    if (! fa->IsStriped() &&
            ci->offset + (chunkOff_t)CHUNKSIZE >= fa->nextChunkOffset() &&
            fa->filesize < 0) {
        KFS_LOG_STREAM_DEBUG <<
            " get chunk size: <" << fileId << "," << req.chunkId << ">" <<
        KFS_LOG_EOM;
        req.server->GetChunkSize(fileId, req.chunkId, req.chunkVersion);
    }
    if (req.server->IsDown()) {
        // Went down in GetChunkSize().
        return;
    }
    // Cancel pending make stable not counting hibernated servers.
    const size_t srvCount = mChunkToServerMap.ConnectedServerCount(*cmi);
    if (fa->numReplicas <= srvCount) {
        CancelPendingMakeStable(fileId, req.chunkId);
    }
    if (fa->numReplicas != srvCount) {
        CheckReplication(*cmi);
    }
}

void
LayoutManager::ProcessPendingBeginMakeStable()
{
    if (mPendingBeginMakeStable.IsEmpty()) {
        return;
    }
    PendingBeginMakeStable pendingBeginMakeStable;
    pendingBeginMakeStable.Swap(mPendingBeginMakeStable);
    const bool kBeginMakeStableFlag = true;
    const bool kStripedFileFlag     = false;
    const PendingBeginMakeStableKVEntry* it;
    pendingBeginMakeStable.First();
    while ((it = pendingBeginMakeStable.Next())) {
        CSMap::Entry* const cmi = mChunkToServerMap.Find(it->GetKey());
        if (! cmi) {
            continue;
        }
        const bool    appendFlag          = true;
        const bool    leaseRelinquishFlag = false;
        MakeChunkStableInit(
            *cmi,
            it->GetVal(),
            string(),
            kBeginMakeStableFlag,
            -1,
            false,
            0,
            kStripedFileFlag,
            appendFlag,
            leaseRelinquishFlag
        );
    }
}

class PrintChunkServerInfo
{
    ostream&   ofs;
    const bool useFsTotalSpaceFlag;
public:
    PrintChunkServerInfo(ostream& o, bool f)
        : ofs(o),
          useFsTotalSpaceFlag(f)
        {}
    void operator()(const ChunkServerPtr& c) const {
        ofs <<
            c->GetServerLocation() <<
            ' ' << c->GetRack() <<
            ' ' << c->GetTotalSpace(useFsTotalSpaceFlag) <<
            ' ' << (useFsTotalSpaceFlag ?
                c->GetFsUsedSpace() : c->GetUsedSpace()) <<
        "\n";
    }
};

//
// Dump out the chunk block map to a file.  The output can be used in emulation
// modes where we setup the block map and experiment.
//
void
LayoutManager::DumpChunkToServerMap(const string& dirToUse)
{
    //
    // to make offline rebalancing/re-replication easier, dump out where the
    // servers are and how much space each has.
    //
    string fn = dirToUse + "/network.def";
    ofstream ofs(fn.c_str(), ofstream::out | ofstream::trunc);
    if (ofs) {
        for_each(mChunkServers.begin(), mChunkServers.end(),
            PrintChunkServerInfo(ofs, mUseFsTotalSpaceFlag));
        ofs.close();
    }
    if (! ofs) {
        unlink(fn.c_str());
        return;
    }

    fn = dirToUse + "/chunkmap.txt";
    ofs.open(fn.c_str(), ofstream::out | ofstream::trunc);
    if (ofs) {
        DumpChunkToServerMap(ofs);
        ofs.close();
    }
    if (! ofs) {
        unlink(fn.c_str());
        return;
    }
}

void
LayoutManager::Handle(MetaDumpChunkReplicationCandidates& req)
{
    int64_t total   = 0;
    int64_t outLeft = mMaxFsckFiles;
    req.resp.Clear();
    ostream& os = mWOstream.Set(req.resp, mMaxResponseSize);
    for (int state = CSMap::Entry::kStateCheckReplication;
            state < CSMap::Entry::kStateCount;
            ++state) {
        for (const CSMap::Entry* entry = mChunkToServerMap.Front(
                    CSMap::Entry::State(state));
                entry != 0 && outLeft-- >= 0;
                entry = mChunkToServerMap.Next(*entry)) {
            if (! (os << entry->GetChunkId() << ' ')) {
                break;
            }
        }
        if ( !(os << '\n')) {
            break;
        }
        total += mChunkToServerMap.GetCount(CSMap::Entry::State(state));

    }
    os.flush();
    if (! os) {
        req.status     = -ENOMEM;
        req.statusMsg  = "response exceeds max. size";
    }
    mWOstream.Reset();
    if (req.status == 0) {
        req.numReplication     = total;
        req.numPendingRecovery = mChunkToServerMap.GetCount(
            CSMap::Entry::kStatePendingRecovery);
    } else {
        req.numReplication     = 0;
        req.numPendingRecovery = 0;
    }
}

bool
LayoutManager::CanBeRecovered(
    const CSMap::Entry&     entry,
    bool&                   incompleteChunkBlockFlag,
    bool*                   incompleteChunkBlockWriteHasLeaseFlag,
    vector<MetaChunkInfo*>& cblk,
    int*                    outGoodCnt /* = 0 */) const
{
    cblk.clear();
    incompleteChunkBlockFlag = false;
    if (incompleteChunkBlockWriteHasLeaseFlag) {
        *incompleteChunkBlockWriteHasLeaseFlag = false;
    }
    const MetaFattr* const fa = entry.GetFattr();
    if (! fa) {
        panic("chunk mapping null file attribute");
        return false;
    }
    if (mChunkToServerMap.HasServers(entry)) {
        return true;
    }
    if (! fa->HasRecovery()) {
        return false;
    }
    MetaFattr*     mfa    = 0;
    MetaChunkInfo* mci    = 0;
    chunkOff_t     start  = -1;
    chunkOff_t     offset = entry.GetChunkInfo()->offset;
    cblk.reserve(fa->numStripes + fa->numRecoveryStripes);
    if (metatree.getalloc(fa->id(),
            offset, mfa, mci, &cblk,
            &start) != 0 ||
            mfa != fa ||
            mci != entry.GetChunkInfo()) {
        panic("chunk mapping / getalloc mismatch");
        return false;
    }
    vector<MetaChunkInfo*>::const_iterator it = cblk.begin();
    unsigned int     stripeIdx = 0;
    int              localCnt;
    int&             goodCnt   = outGoodCnt ? *outGoodCnt : localCnt;
    chunkOff_t const end       = start + fa->ChunkBlkSize();
    goodCnt = 0;
    for (chunkOff_t pos = start;
            pos < end;
            pos += (chunkOff_t)CHUNKSIZE, stripeIdx++) {
        if (it == cblk.end()) {
             // Incomplete chunk block.
            incompleteChunkBlockFlag = true;
            break;
        }
        assert((*it)->offset % CHUNKSIZE == 0);
        if (pos < (*it)->offset) {
            if (fa->numStripes <= stripeIdx) {
                // No recovery: incomplete chunk block.
                incompleteChunkBlockFlag = true;
                break;
            }
            goodCnt++;
            continue; // no chunk -- hole.
        }
        if (mChunkToServerMap.HasServers(GetCsEntry(**it))) {
            goodCnt++;
        }
        ++it;
    }
    if (incompleteChunkBlockFlag && incompleteChunkBlockWriteHasLeaseFlag) {
        for (it = cblk.begin(); it != cblk.end(); ++it) {
            if (mChunkLeases.GetChunkWriteLease((*it)->chunkId)) {
                *incompleteChunkBlockWriteHasLeaseFlag = true;
                break;
            }
        }
    }
    return ((int)fa->numStripes <= goodCnt);
}

typedef KeyOnly<const MetaFattr*> KeyOnlyFattrPtr;
typedef LinearHash<
    KeyOnlyFattrPtr,
    KeyCompare<KeyOnlyFattrPtr::Key>,
    DynamicArray<SingleLinkedList<KeyOnlyFattrPtr>*>
    // Use straight new to reduce the chances to encounter locked by
    // the parent process mutex. Glibs malloc can properly deal with
    // forking multi threaded processes.
    // StdFastAllocator<KeyOnlyFattrPtr>
> MetaFattrSet;

bool
LayoutManager::Fsck(ostream& os, bool reportAbandonedFilesFlag)
{
    // Do full scan, for added safety: the replication lists don't have to
    // be correct / up to date.
    const int kLostSet       = 0;
    const int kEndangeredSet = 1;
    const int kAbandonedSet  = 2;
    const int kSetCount      = 3;
    const char* const setNames[kSetCount] = {
        "Lost",
        "Endangered",
        "Abandoned"
    };
    MetaFattrSet  files[kSetCount];
    int64_t       maxFilesToReport       = mMaxFsckFiles;
    const int64_t startTime              = microseconds();
    const int64_t pastEofRecoveryEndTime = startTime -
        mPastEofRecoveryDelay;
    const int64_t abandonedFileEndTime   = startTime -
        mFsckAbandonedFileTimeout;
    const int64_t maxEndTime             = startTime + mMaxFsckTime;
    unsigned int  timeCheckCnt           = 0;
    bool          timedOutFlag           = false;
    vector<MetaChunkInfo*> cblk;
    mChunkToServerMap.First();
    for (const CSMap::Entry* p;
            (p = mChunkToServerMap.Next()) &&
            maxFilesToReport > 0; ) {
        const size_t serverCnt = mChunkToServerMap.ServerCount(*p);
        if (serverCnt <= 0 ||
                (reportAbandonedFilesFlag &&
                    p->GetFattr()->IsStriped())) {
            const MetaFattr* const fa = p->GetFattr();
            // For now treat all striped files as bein written into,
            // regardless if the file has recovery stripes or not,
            // if chunk logical position is past logical EOF.
            if (fa->IsStriped()) {
                if (files[kLostSet].Find(fa)) {
                    continue; // Already has missing chunks.
                }
                if (fa->mtime < pastEofRecoveryEndTime &&
                        fa->filesize <=
                        fa->ChunkPosToChunkBlkFileStartPos(
                            p->GetChunkInfo()->offset)) {
                    bool insertedFlag = false;
                    if (fa->mtime < abandonedFileEndTime &&
                            files[kAbandonedSet].Insert(
                                fa, fa,
                                insertedFlag) &&
                            insertedFlag &&
                            files[kEndangeredSet].Erase(fa) == 0) {
                        maxFilesToReport--;
                    }
                    continue;
                }
                if (serverCnt > 0) {
                    continue;
                }
                bool incompleteCBFlag              = false;
                bool incompleteCBWriteHasLeaseFlag = false;
                if (CanBeRecovered(*p, incompleteCBFlag,
                        &incompleteCBWriteHasLeaseFlag,
                        cblk)) {
                    continue;
                }
                if (incompleteCBFlag) {
                    if (incompleteCBWriteHasLeaseFlag ||
                            fa->mtime >=
                            abandonedFileEndTime) {
                        continue;
                    }
                    bool insertedFlag = false;
                    if (files[kAbandonedSet].Insert(
                            fa, fa, insertedFlag) &&
                            insertedFlag &&
                            files[kEndangeredSet].Erase(fa) == 0) {
                        maxFilesToReport--;
                    }
                    continue;
                }
            }
            bool insertedFlag = false;
            if (files[kLostSet].Insert(fa, fa, insertedFlag) &&
                    insertedFlag &&
                    files[kEndangeredSet].Erase(fa) == 0 &&
                    files[kAbandonedSet].Erase(fa) == 0) {
                maxFilesToReport--;
            }
        } else if (serverCnt == 1 && p->GetFattr()->numReplicas > 1) {
            const MetaFattr* const fa = p->GetFattr();
            bool insertedFlag = false;
            if (! files[kLostSet].Find(fa) &&
                    ! files[kAbandonedSet].Find(fa) &&
                    files[kEndangeredSet].Insert(
                        fa, fa, insertedFlag) &&
                    insertedFlag) {
                maxFilesToReport--;
            }
        }
        if ((++timeCheckCnt & 0x3FFF) == 0 &&
                maxEndTime < microseconds()) {
            timedOutFlag = true;
            break;
        }
    }
    for (int i = 0; i < kSetCount; i++) {
        os << setNames[i] << " files total: " <<
            files[i].GetSize() << "\n";
    }
    if (maxFilesToReport <= 0) {
        os << "Warning: report limited to first: " <<
            mMaxFsckFiles << " files\n";
    }
    if (timedOutFlag) {
        os << "Warning: report limited due to reaching time"
            " limit of: " << (mMaxFsckTime * 1e-6) << " seconds\n";
    }
    os << "Fsck run time: " <<
        ((microseconds() - startTime) * 1e-6) << " sec.\n";
    for (int i = 0; i < kSetCount; i++) {
        if (files[i].GetSize() <= 0) {
            continue;
        }
        os << setNames[i] << " files [path size mtime]:\n";
        files[i].First();
        for (const KeyOnlyFattrPtr* p; (p = files[i].Next()); ) {
            const MetaFattr* const fa = p->GetKey();
            const string path = metatree.getPathname(fa);
            if (path.empty()) {
                continue;
            }
            os << path << "\t" << fa->filesize << "\t" <<
                DisplayIsoDateTime(fa->mtime) <<
            "\n";
        }
    }
    return files[kLostSet].IsEmpty();
}

class LayoutManager::FilesChecker
{
public:
    typedef LayoutManager::ChunkPlacement ChunkPlacement;

    enum Status
    {
        kStateNone = 0,
        kLost,
        kLostIfServerDown,
        kLostIfRackDown,
        kAbandoned,
        kOk,
        kStateCount
    };
    static int GetStreamsCount(bool reportAbandonedFilesFlag)
    {
        return (kStateCount - (reportAbandonedFilesFlag ? 1 : 2));
    }

    FilesChecker(
        LayoutManager&  layoutManager,
        int64_t         maxFilesToReport,
        ostream**       os)
        : mLayoutManager(layoutManager),
          mPlacement(&layoutManager),
          mStartTime(microseconds()),
          mMaxToReportFileCount(maxFilesToReport),
          mPath(),
          mDepth(0),
          mStopFlag(false),
          mStopReason(),
          mDirCount(0),
          mFileCount(0),
          mMaxDirDepth(0),
          mOverReplicatedCount(0),
          mUnderReplicatedCount(0),
          mChunkLostCount(0),
          mNoRackCount(0),
          mRecoveryBlock(0),
          mPartialRecoveryBlock(0),
          mReplicaCount(0),
          mMaxReplicaCount(0),
          mToReportFileCount(0),
          mMaxChunkCount(0),
          mTotalChunkCount(0),
          mMaxFileSize(0),
          mTotalFilesSize(0),
          mStripedFilesCount(0),
          mFilesWithRecoveryCount(0),
          mMaxReplication(0),
          mObjectStoreFileCount(0),
          mObjectStoreBlockCount(0),
          mMaxObjectStoreBlockCount(0)
    {
        mPath.reserve(8 << 10);
        for (int i = 0, k = 0; i < kStateCount; i++) {
            if ((mOs[i] = os ? os[k] : 0)) {
                k++;
            }
            mFileCounts[i] = 0;
        }
    }
    bool operator()(
            const MetaDentry& de,
            const MetaFattr&  fa,
            size_t            depth)
    {
        if (mStopFlag) {
            return false;
        }
        if (depth < mDepth) {
            mDepth = depth;
            mPath.resize(mDepth);
        }
        assert(depth == mDepth);
        if (fa.type == KFS_DIR) {
            mPath.resize(mDepth);
            mPath.push_back(&de);
            mMaxDirDepth = max(mMaxDirDepth, mDepth);
            mDirCount++;
            mDepth++;
            return true;
        }
        const chunkOff_t fsize = metatree.getFileSize(fa);
        mMaxFileSize     = max(mMaxFileSize, fsize);
        mTotalFilesSize += fsize;
        mMaxChunkCount   = max(mMaxChunkCount, fa.chunkcount());
        mFileCount++;
        if (fa.HasRecovery()) {
            mFilesWithRecoveryCount++;
        } else if (fa.IsStriped()) {
            mStripedFilesCount++;
        }
        if (0 == fa.numReplicas) {
            const int64_t theBlkCnt =
                (int64_t)(fa.nextChunkOffset() / (chunkOff_t)CHUNKSIZE);
            mObjectStoreFileCount++;
            mMaxObjectStoreBlockCount =
                max(mMaxObjectStoreBlockCount, theBlkCnt);
            mObjectStoreBlockCount += theBlkCnt;
            // Continue to validate that object store file has no chunks.
        }
        mMaxReplication  = max(mMaxReplication, (int)fa.numReplicas);
        mLayoutManager.CheckFile(*this, de, fa);
        return (! mStopFlag);
    }
    void Report(
        Status            status,
        const MetaDentry& de,
        const MetaFattr&  fa)
    {
        mFileCounts[status]++;
        if (mOs[status] == 0 ||
                mToReportFileCount++ >
                mMaxToReportFileCount) {
            return;
        }
        ostream& os = *(mOs[status]);
        os <<
        status <<
        " " << metatree.getFileSize(fa) <<
        " " << fa.numReplicas <<
        " " << fa.striperType <<
        " " << fa.numStripes <<
        " " << fa.numRecoveryStripes <<
        " " << fa.stripeSize <<
        " " << (0 == fa.numReplicas ?
            (int64_t)(fa.nextChunkOffset() / (chunkOff_t)CHUNKSIZE) :
            fa.chunkcount()) <<
        " " << DisplayIsoDateTime(fa.mtime) <<
        " ";
        DisplayPath(os) << "/" << de.getName() << "\n";
    }
    void OverReplicated()       { mOverReplicatedCount++; }
    void UnderReplicated()      { mUnderReplicatedCount++; }
    void NoRack()               { mNoRackCount++; }
    void RecoveryBlock()        { mRecoveryBlock++; }
    void PartialRecoveryBlock() { mPartialRecoveryBlock++; }
    void Chunk()                { mTotalChunkCount++; }
    void ChunkLost()            { mChunkLostCount++; }
    void ChunkReplicas(size_t cnt)
    {
        mReplicaCount += cnt;
        mMaxReplicaCount = max(mMaxReplicaCount, cnt);
    }
    ChunkPlacement& GetPlacement() { return mPlacement; }
    int64_t StartTime() const      { return mStartTime; }
    int64_t GetFileCount() const   { return mFileCount; }
    int64_t ItemsCount() const
        { return (mTotalChunkCount + (int64_t)mFileCount); }
    void Stop(const string& reason = string())
    {
        mStopReason = reason;
        mStopFlag   = true;
    }
    const string& GetStopReason() const { return mStopReason; }
    bool Report(size_t chunkCount)
    {
        if (! mOs[kStateNone]) {
            return (0 == mFileCounts[kLost]);
        }
        const char* const kStateNames[kStateCount] = {
            "none",
            "lost",
            "lost if server down",
            "lost if rack down",
            "abandoned",
            "ok"
        };
        ostream& os = *(mOs[kStateNone]);
        if (! mStopFlag) {
            // For backward compatibility with kfsfsck client the
            // first line must be the following:
            os << "Lost files total: " <<
                mFileCounts[kLost] << "\n";
        }
        const char* const suff = mStopFlag ? "checked" : "reachable";
        if (mStopFlag) {
            os <<
            "WARNING: fsck report is incomplete: " <<
            mStopReason << "\n";
        } else if (mMaxToReportFileCount < mToReportFileCount) {
            os <<
            "WARNING: fsck report is incomplete: " <<
            " exceeded max number files to report: " <<
            mMaxToReportFileCount <<
            " total: " << mToReportFileCount << "\n";
        }
        const int64_t dirCnt  = GetNumDirs();
        const int64_t fileCnt = GetNumFiles();
        os <<
        "Directories: " << dirCnt << "\n"
        "Directories " << suff << ": " << (mDirCount + 1) <<
            " " << ((mDirCount + 1) * 1e2 /
                max(dirCnt, int64_t(1))) << "%\n"
        "Directory " << suff << " max depth: " <<
            (mMaxDirDepth + 1) << "\n"
        "Files: " << fileCnt << "\n"
        "Files " << suff << ": " << mFileCount <<
            " " << (mFileCount * 1e2 /
                max(fileCnt, int64_t(1))) << "%\n";
        const double fpct = 1e2 / max(mFileCount, size_t(1));
        os <<
        "Files " << suff << " with recovery: " <<
            mFilesWithRecoveryCount <<
            " " << (mFilesWithRecoveryCount * fpct) << "%\n"
        "Files " << suff << " striped: " <<
            mStripedFilesCount <<
            " " << (mStripedFilesCount * fpct) << "%\n"
        "Files "  << suff << " object store: " <<
            mObjectStoreFileCount <<
            " " << (mObjectStoreFileCount * fpct) << "%\n"
        "Files " << suff << " sum of logical sizes: " <<
            mTotalFilesSize << "\n";
        for (int i = kStateNone + 1; i < kStateCount; i++) {
            os <<
            i << "  Files " <<
                suff << " " << kStateNames[i] << ": "  <<
            mFileCounts[i] << " " <<
                (mFileCounts[i] * fpct) << "%\n";
        }
        const double cpct = 1e2 / max(mTotalChunkCount, int64_t(1));
        os <<
        "File " << suff << " max size: " << mMaxFileSize << "\n"
        "File " << suff << " max chunks: " << mMaxChunkCount << "\n"
        "File " << suff << " max replication: " <<
            mMaxReplication << "\n"
        "File " << suff << " max object store blocks: " <<
            mMaxObjectStoreBlockCount << "\n"
        "Chunks: " << chunkCount << "\n"
        "Chunks " << suff << ": " << mTotalChunkCount <<
            " " << (mTotalChunkCount * 1e2 /
                max(chunkCount, size_t(1))) << "%\n"
        "Chunks " << suff << " lost: " <<
            mChunkLostCount <<
            " " << (mChunkLostCount * cpct) << "%\n"
        "Chunks " << suff << " no rack assigned: " <<
            mNoRackCount <<
            " " << (mNoRackCount * cpct) << "%\n"
        "Chunks " << suff << " over replicated: "  <<
            mOverReplicatedCount <<
            " " << (mOverReplicatedCount *  cpct) << "%\n"
        "Chunks " << suff << " under replicated: " <<
            mUnderReplicatedCount <<
            " " << (mUnderReplicatedCount * cpct) << "%\n"
        "Chunks " << suff << " replicas: " << mReplicaCount <<
            " " << (mReplicaCount * cpct) << "%\n"
        "Chunk " << suff << " max replicas: " <<
            mMaxReplicaCount << "\n"
        "Recovery blocks " << suff << ": " << mRecoveryBlock << "\n"
        "Recovery blocks " << suff << " partial: " <<
            mPartialRecoveryBlock <<
            " " << (mPartialRecoveryBlock * 1e2 /
                max(mRecoveryBlock, size_t(1))) << "%\n"
        "Object store blocks " << suff << ": " << mObjectStoreBlockCount << "\n"
        "Fsck run time: "  <<
            (microseconds() - mStartTime) * 1e-6 << " sec.\n"
        "Files: [fsck_state size replication type stripes"
            " recovery_stripes stripe_size chunk_count mtime"
            " path]\n"
        ;
        return (0 == mFileCounts[kLost]);
    }
private:
    typedef vector<const MetaDentry*> Path;

    LayoutManager& mLayoutManager;
    ChunkPlacement mPlacement;
    ostream*       mOs[kStateCount];
    size_t         mFileCounts[kStateCount];
    const int64_t  mStartTime;
    const int64_t  mMaxToReportFileCount;
    Path           mPath;
    size_t         mDepth;
    bool           mStopFlag;
    string         mStopReason;
    size_t         mDirCount;
    size_t         mFileCount;
    size_t         mMaxDirDepth;
    size_t         mOverReplicatedCount;
    size_t         mUnderReplicatedCount;
    size_t         mChunkLostCount;
    size_t         mNoRackCount;
    size_t         mRecoveryBlock;
    size_t         mPartialRecoveryBlock;
    size_t         mReplicaCount;
    size_t         mMaxReplicaCount;
    int64_t        mToReportFileCount;
    int64_t        mMaxChunkCount;
    int64_t        mTotalChunkCount;
    chunkOff_t     mMaxFileSize;
    int64_t        mTotalFilesSize;
    size_t         mStripedFilesCount;
    size_t         mFilesWithRecoveryCount;
    int            mMaxReplication;
    size_t         mObjectStoreFileCount;
    int64_t        mObjectStoreBlockCount;
    int64_t        mMaxObjectStoreBlockCount;

    ostream& DisplayPath(ostream& os) const
    {
        for (Path::const_iterator it = mPath.begin();
                it != mPath.end();
                ++it) {
            os << "/" << (*it)->getName();
        }
        return os;
    }
private:
    FilesChecker(const FilesChecker&);
    FilesChecker& operator=(const FilesChecker&);
};

void
LayoutManager::CheckFile(
    FilesChecker&     fsck,
    const MetaDentry& de,
    const MetaFattr&  fa)
{
    const int64_t         kScanCheckMask    = ((int64_t(1) << 16) - 1);
    bool                  stopFlag          = false;
    FilesChecker::Status  status            = FilesChecker::kOk;
    const size_t          recoveryStripeCnt = (size_t)(fa.HasRecovery() ?
        fa.numRecoveryStripes : 0);
    const chunkOff_t      recoveryPos       = (chunkOff_t)CHUNKSIZE *
        (recoveryStripeCnt > 0 ? fa.numStripes : 1);
    const chunkOff_t      chunkBlockSize    = (chunkOff_t)CHUNKSIZE *
        (recoveryStripeCnt > 0 ?
            (fa.numStripes + fa.numRecoveryStripes) : 1);
    ChunkIterator         it                = metatree.getAlloc(fa.id());
    StTmp<Servers>        serversTmp(mServersTmp);
    StTmp<ChunkPlacement> placementTmp(mChunkPlacementTmp);
    ChunkPlacement&       placement         = placementTmp.Get();
    ChunkPlacement&       chunkPlacement    = fsck.GetPlacement();
    chunkOff_t            chunkBlockEnd     = -1;
    chunkOff_t            chunkBlockCount   = 0;
    bool                  invalidBlkFlag    = false;
    for (const MetaChunkInfo* p = it.next(); p; ) {
        if (p->offset != chunkBlockEnd) {
            chunkBlockEnd = p->offset;
            if (recoveryStripeCnt > 0 && chunkBlockEnd != 0) {
                const chunkOff_t blockHead =
                    chunkBlockEnd % chunkBlockSize;
                chunkBlockEnd -= blockHead;
                if (blockHead != 0) {
                    fsck.PartialRecoveryBlock();
                    invalidBlkFlag = true;
                }
            }
        }
        const chunkOff_t recoveryStartPos = chunkBlockEnd + recoveryPos;
        chunkBlockEnd += chunkBlockSize;
        placement.clear();
        size_t missingCnt             = 0;
        size_t blockRecoveryStripeCnt = 0;
        if (recoveryStripeCnt > 0) {
            fsck.RecoveryBlock();
        }
        for ( ; p; p = it.next()) {
            const MetaChunkInfo& ci = *p;
            if (chunkBlockEnd <= ci.offset) {
                break;
            }
            fsck.Chunk();
            if ((fsck.ItemsCount() & kScanCheckMask) == 0 &&
                    fsck.StartTime() + mMaxFsckTime <
                    microseconds()) {
                stopFlag = true;
                break;
            }
            if (recoveryStartPos <= ci.offset) {
                blockRecoveryStripeCnt++;
            }
            const CSMap::Entry& entry =
                CSMap::Entry::GetCsEntry(ci);
            Servers&            srvs  = serversTmp.Get();
            mChunkToServerMap.GetServers(entry, srvs);
            chunkPlacement.clear();
            // Count here chunks that are being evacuated, and the
            // servers that are being retired.
            chunkPlacement.ExcludeServerAndRack(srvs);
            const size_t srvsCnt =
                chunkPlacement.GetExcludedServersCount();
            if (srvsCnt == 0) {
                fsck.ChunkLost();
                missingCnt++;
                continue;
            }
            fsck.ChunkReplicas(srvsCnt);
            const size_t rackCnt =
                chunkPlacement.GetExcludedRacksCount();
            if (rackCnt <= 1) {
                if (chunkPlacement.GetExcludedServersCount() <=
                        1) {
                    placement.ExcludeServer(srvs);
                }
                if (rackCnt > 0) {
                    placement.ExcludeRack(srvs);
                } else {
                    fsck.NoRack();
                }
            }
            if (srvsCnt < (size_t)fa.numReplicas) {
                fsck.UnderReplicated();
            } else if (srvsCnt > (size_t)fa.numReplicas) {
                fsck.OverReplicated();
            }
        }
        if (stopFlag) {
            break;
        }
        chunkBlockCount++;
        if (blockRecoveryStripeCnt < recoveryStripeCnt) {
            fsck.PartialRecoveryBlock();
            invalidBlkFlag = true;
        }
        if (recoveryStripeCnt < missingCnt) {
            status = FilesChecker::kLost;
        }
        if (status == FilesChecker::kLost) {
            continue;
        }
        if (recoveryStripeCnt < missingCnt +
                placement.GetExcludedServersMaxCount()) {
            status = FilesChecker::kLostIfServerDown;
        } else if (status == FilesChecker::kOk &&
                recoveryStripeCnt < missingCnt +
                    placement.GetExcludedRacksMaxCount()) {
            status = FilesChecker::kLostIfRackDown;
        }
    }
    if (! stopFlag) {
        if (0 == fa.numReplicas && fa.filesize <= 0 &&
                0 < fa.nextChunkOffset() && FilesChecker::kLost != status &&
                fa.mtime + mFsckAbandonedFileTimeout < fsck.StartTime()) {
            status = FilesChecker::kAbandoned;
        } else if (0 < recoveryStripeCnt && 0 < chunkBlockCount &&
                (status != FilesChecker::kLost ||
                    fa.filesize <= 0 ||
                    invalidBlkFlag) &&
                fa.mtime + mPastEofRecoveryDelay <
                    fsck.StartTime() &&
                fa.filesize <= (chunkBlockCount - 1) *
                    fa.numStripes * (chunkOff_t)CHUNKSIZE &&
                fa.mtime + mFsckAbandonedFileTimeout < fsck.StartTime()) {
            status = FilesChecker::kAbandoned;
        }
        fsck.Report(status, de, fa);
    } else if (chunkBlockCount <= 0) {
        stopFlag = (fsck.ItemsCount() & kScanCheckMask) == 0 &&
            fsck.StartTime() + mMaxFsckTime < microseconds();
    }
    if (stopFlag) {
        ostringstream os;
        os << "exceeded fsck run time limit of " <<
            (mMaxFsckTime * 1e-6) << " sec.";
        fsck.Stop(os.str());
    }
}

int
LayoutManager::FsckStreamCount(bool reportAbandonedFilesFlag) const
{
    return (mFullFsckFlag ?
        FilesChecker::GetStreamsCount(reportAbandonedFilesFlag) : 1);
}

bool
LayoutManager::Fsck(ostream** os, bool reportAbandonedFilesFlag)
{
    if (mFullFsckFlag) {
        FilesChecker fsck(*this, mMaxFsckFiles, os);
        metatree.iterateDentries(fsck);
        return fsck.Report(mChunkToServerMap.Size());
    }
    if (os && os[0]) {
        return Fsck(*(os[0]), reportAbandonedFilesFlag);
    }
    return false;
}

void
LayoutManager::DumpChunkToServerMap(ostream& os)
{
    mChunkToServerMap.First();
    StTmp<Servers> serversTmp(mServersTmp);
    for (const CSMap::Entry* p; (p = mChunkToServerMap.Next()); ) {
        Servers& cs = serversTmp.Get();
        mChunkToServerMap.GetServers(*p, cs);
        os << p->GetChunkId() <<
            " " << p->GetFileId() <<
            " " << cs.size();
        for (Servers::const_iterator it = cs.begin();
                it != cs.end();
                ++it) {
            os <<
                " " << (*it)->GetServerLocation() <<
                " " << (*it)->GetRack();
        }
        os << "\n";
    }
}

void
LayoutManager::Start(MetaBye& req)
{
    if (0 != req.status) {
        return;
    }
    if (req.location != req.server->GetServerLocation()) {
        panic("start bye: invalid server location");
        req.status = -EFAULT;
        return;
    }
    if (! mChunkToServerMap.Validate(req.server) && ! req.server->IsDown()) {
        panic("start bye: no such server");
        req.status = -EFAULT;
        return;
    }
    Servers::const_iterator const it = FindServer(req.location);
    if (it == mChunkServers.end() || *it != req.server) {
        if (req.server->IsDown()) {
            req.status = -EINVAL;
        } else {
            panic("start bye: invalid stale server entry");
            req.status = -EFAULT;
        }
        return;
    }
    if (req.server->IsDown()) {
        panic("start bye: server is already down");
        req.status = -EFAULT;
        return;
    }
    const bool simulateResumeDenyFlag =
        0 < mDebugSimulateDenyHelloResumeInterval &&
        0 == Rand(mDebugSimulateDenyHelloResumeInterval);
    const int kMinReplicationDelay    = 20;
    req.replicationDelay = simulateResumeDenyFlag ? -kMinReplicationDelay :
        max(kMinReplicationDelay, mServerDownReplicationDelay -
            req.server->TimeSinceLastHeartbeat());
}

void
LayoutManager::Handle(MetaBye& req)
{
    if (req.server) {
        if (req.replayFlag) {
            panic("invalid meta bye in replay");
            return;
        }
        if (IsMetaLogWriteOrVrError(req.status)) {
            ScheduleResubmitOrCancel(req);
            return;
        }
        if (0 != req.status) {
            return;
        }
    } else {
        const ChunkServerPtr* const cs = ReplayFindServer(req.location, req);
        if (! cs) {
            panic("replay bye: no such server");
            req.status = -EFAULT;
            return;
        }
        req.server = *cs;
    }
    const ChunkServerPtr& server = req.server;
    if (req.location != server->GetServerLocation()) {
        panic("handle bye: invalid server location");
        req.status = -EFAULT;
        return;
    }
    if (! mChunkToServerMap.Validate(server) && ! req.server->IsDown()) {
        panic("handle bye: no such server");
        req.status = -EFAULT;
        return;
    }
    Servers::const_iterator const sit = FindServer(req.location);
    if (sit == mChunkServers.end() || *sit != server) {
        if (req.server->IsDown()) {
            req.status = -EINVAL;
        } else {
            panic("handle bye: invalid stale server entry");
            req.status = -EFAULT;
        }
        return;
    }
    if (req.server->IsDown()) {
        panic("handle bye: server is already down");
        req.status = -EFAULT;
        return;
    }
    server->ForceDown();
    if (! (server->GetChunkCount() == req.chunkCount &&
            server->GetChecksum() == req.cIdChecksum)) {
        // In flight "log in flight", or "log completion" at the time when bye
        // was submitted might result in mismatch.
        // Due to non stable chunks replay handling the secondaries has chunk
        // super set of the primary.
        KFS_LOG_STREAM_DEBUG << "bye:"
            " server: "      << server->GetServerLocation() <<
            " chunk count: " << server->GetChunkCount() <<
            " expected: "    << req.chunkCount <<
            " checksum: "    << server->GetChecksum() <<
            " expected: "    << req.cIdChecksum <<
            " "              << req.Show() <<
        KFS_LOG_EOM;
    }
    RackInfos::iterator const rackIter = FindRack(server->GetRack());
    if (rackIter != mRacks.end() &&
            rackIter->removeServer(server) &&
            rackIter->getServers().empty()) {
        // The entire rack of servers is gone take the rack out.
        KFS_LOG_STREAM(req.replayFlag ?
                MsgLogger::kLogLevelDEBUG :
                MsgLogger::kLogLevelINFO) <<
            "all servers in rack " << server->GetRack() <<
            " are down; taking out the rack" <<
        KFS_LOG_EOM;
        mRacks.erase(rackIter);
    }
    // Schedule to expire write leases, and invalidate record append cache.
    mChunkLeases.ServerDown(server, mARAChunkCache, mChunkToServerMap);
    const bool           canBeMaster = server->CanBeChunkMaster();
    const ServerLocation loc         = server->GetServerLocation();
    const size_t         blockCount  = server->GetChunkCount();
    const string         downReason  = server->DownReason();
    const char*          reason      = downReason.c_str();

    KFS_LOG_STREAM(server->IsReplay() ?
            MsgLogger::kLogLevelDEBUG :
            MsgLogger::kLogLevelINFO) <<
        "server down: "  << loc <<
        " block count: " << blockCount <<
        " master: "      << canBeMaster <<
        " replay: "      << server->IsReplay() <<
        (*reason ? " reason: " : "") << reason <<
        " "              << req.Show() <<
    KFS_LOG_EOM;

    // check if this server was sent to hibernation
    HibernatedServerInfos::iterator it;
    HibernatingServerInfo* const     hs = FindHibernatingCSInfo(loc, &it);
    if (hs) {
        HibernatingServerInfo& hsi               = *hs;
        const bool             wasHibernatedFlag = hsi.IsHibernated();
        const size_t           prevIdx           = hsi.csmapIdx;
        if (mChunkToServerMap.SetHibernated(server, hsi.csmapIdx)) {
            if (wasHibernatedFlag) {
                if (prevIdx == hsi.csmapIdx ||
                        ! mChunkToServerMap.RemoveHibernatedServer(prevIdx)) {
                    panic("failed to update hibernated server index");
                }
                KFS_LOG_STREAM_ERROR <<
                    "hibernated server reconnect failure:"
                    " location: " << loc <<
                    " index: "    << prevIdx <<
                    " -> "        << hsi.csmapIdx <<
                    " blocks: "   << blockCount <<
                KFS_LOG_EOM;
                reason = "Reconnect failed";
            } else {
                reason = "Hibernated";
            }
        } else {
            reason = "Hibernated";
        }
    }

    if (! hs && server->IsRetiring()) {
        reason = "Retired";
    } else if (! *reason) {
        reason = "Unreachable";
    }
    // for reporting purposes, record when it went down
    ostringstream& os = GetTempOstream();
    os <<
        "s="        << loc.hostname <<
        ", p="      << loc.port <<
        ", down="   << DisplayDateTime(req.timeUsec) <<
        ", reason="
    ;
    if (downReason.c_str() == reason) {
        for (const char* p = reason, *const e = p + downReason.size();
                p < e;
                ++p) {
            const int  sym       = *p & 0xFF;
            const char kSpace    = ' ';
            const int  kSymSpace = kSpace & 0xFF;
            switch (sym) {
                case ',':  os << ';';    break;
                case '=':  os << ':';    break;
                case '\t': os << kSpace; break;
                default:
                    os << (sym < kSymSpace ? kSpace : *p);
            }
        }
    } else {
        os << reason;
    }
    os << "\t";
    mDownServers.push_back(os.str());
    if (mDownServers.size() > mMaxDownServersHistorySize) {
        mDownServers.erase(mDownServers.begin(), mDownServers.begin() +
            mDownServers.size() - mMaxDownServersHistorySize);
    }
    if (! hs) {
        // Delay replication by marking server as hibernated, to allow the
        // server to reconnect back. Save chunk chunk server inventory,
        // including in flight chunk ids. Always transition into hibernated
        // state, in order to make replay consistent, even if in flight and
        // chunk count diverge between primary and backups due to differences in
        // handling of non stable chunks on primary and backups. The primary
        // must explicitly issue remove hibernated chunk server RPC, and the RPC
        // must be successfully logged.
        size_t idx = ~size_t(0);
        if (! mChunkToServerMap.SetHibernated(server, idx)) {
            panic("failed to initiate hibernation");
        }
        mHibernatingServers.insert(it, HibernatingServerInfo(
            loc, (time_t)(req.timeUsec / 1000000), req.replicationDelay,
            req.replayFlag, idx));
    }
    if (! server->IsReplay() && ! server->IsStoppedServicing()) {
        if (canBeMaster) {
            if (mMastersCount > 0) {
                mMastersCount--;
            }
        } else if (mSlavesCount > 0) {
            mSlavesCount--;
        }
        if (server->IsRestartScheduled()) {
            if (mCSToRestartCount > 0) {
                mCSToRestartCount--;
            }
            if (mMastersToRestartCount > 0 && server->CanBeChunkMaster()) {
                mMastersToRestartCount--;
            }
        }
    }
    if (server->IsReplay()) {
        if (mReplayServerCount <= 0) {
            panic("invalid replay server count");
        }
        mReplayServerCount--;
    } else {
        if (mDisconnectedCount <= 0) {
            panic("invalid disconnected server count");
        }
        mDisconnectedCount--;
    }
    // Convert const_iterator to iterator below to make erase() compile.
    mChunkServers.erase(mChunkServers.begin() + (sit - mChunkServers.begin()));
    if (mPrimaryFlag && ! mAssignMasterByIpFlag &&
            0 < mSlavesCount && mMastersCount == 0 &&
            ! server->IsReplay() && ! server->IsStoppedServicing() &&
            0 < GetConnectedServerCount()) {
        for (Servers::const_iterator it = mChunkServers.begin();
                mChunkServers.end() != it;
                ++it) {
            ChunkServer& srv = **it;
            if (srv.IsConnected()) {
                mSlavesCount--;
                mMastersCount++;
                srv.SetCanBeChunkMaster(true);
            }
        }
    }
    UpdateReplicationsThreshold();
    ScheduleCleanup();
    if (! req.replayFlag && req.replicationDelay <= 0) {
        CheckHibernatingServersStatus();
    }
}

LayoutManager::HibernatingServerInfo*
LayoutManager::FindHibernatingCSInfo(const ServerLocation& loc,
    LayoutManager::HibernatedServerInfos::iterator* outIt)
{
    HibernatedServerInfos::iterator const it = lower_bound(
        mHibernatingServers.begin(), mHibernatingServers.end(),
        loc, bind(&HibernatingServerInfo::location, _1) < loc
    );
    if (outIt) {
        *outIt = it;
    }
    return ((it != mHibernatingServers.end() && loc == it->location) ?
        &(*it) : 0);
}

void
LayoutManager::SetPrimary(bool flag)
{
    if (mPrimaryFlag == flag) {
        return;
    }
    mPrimaryFlag = flag;
    KFS_LOG_STREAM_DEBUG <<
        "set primary: " << mPrimaryFlag <<
    KFS_LOG_EOM;
    mIdempotentRequestTracker.SetDisableTimerFlag(! mPrimaryFlag);
    if (mPrimaryFlag) {
        mChunkLeases.SetTimerNextRunTime();
        const time_t now = TimeNow();
        mLeaseCleanerOtherNextRunTime = now;
        if (gNetDispatch.IsRunning()) {
            mLeaseCleaner.ScheduleNext();
            const int delay       = mChunkLeases.GetDumpsterCleanupDelaySec();
            const int kDelayRatio = 3;
            if (kDelayRatio < delay &&
                    mServiceStartTime + delay - delay / kDelayRatio < now) {
                const int rescheduleDelay =
                    (delay + kDelayRatio - 1) / kDelayRatio;
                KFS_LOG_STREAM_DEBUG <<
                    "rescheduling dumpster cleanup to: +" <<
                        rescheduleDelay << " sec." <<
                KFS_LOG_EOM;
                mChunkLeases.RescheduleDumpsterCleanup(
                    TimeNow() + rescheduleDelay);
            }
        }
    }
}

HibernatedChunkServer*
LayoutManager::FindHibernatingCS(const ServerLocation& loc,
    LayoutManager::HibernatedServerInfos::iterator* outIt)
{
    const HibernatingServerInfo* const hs = FindHibernatingCSInfo(loc, outIt);
    if (! hs || ! hs->IsHibernated()) {
        return 0;
    }
    HibernatedChunkServer* const ret =
        mChunkToServerMap.GetHiberantedServer(hs->csmapIdx);
    if (! ret) {
        panic("invalid hibernated server info index");
    }
    return ret;
}

bool
LayoutManager::Validate(MetaRetireChunkserver& req)
{
    if (0 != req.status) {
        return false;
    }
    if (mAllowChunkServerRetireFlag || 0 < req.nSecsDown) {
        return true;
    }
    req.status    = -EPERM;
    req.statusMsg = "chunk server retire is deprecated,"
        " please use chunk directory evacuation ";
    KFS_LOG_STREAM_INFO << req.statusMsg <<
    KFS_LOG_EOM;
    return false;
}

void
LayoutManager::RetireServer(MetaRetireChunkserver& req)
{
    if (0 != req.status) {
        return;
    }
    Servers::const_iterator const si = FindServer(req.location);
    if (si == mChunkServers.end() || (*si)->IsDown()) {
        // Update down time, and let hibernation status check to
        // take appropriate action.
        HibernatingServerInfo* const hs = FindHibernatingCSInfo(req.location);
        if (hs) {
            hs->startTime    = req.startTime;
            hs->sleepEndTime = req.startTime + max(0, req.nSecsDown);
            hs->replayFlag   = req.replayFlag;
            return;
        }
        req.status    = -ENOENT;
        req.statusMsg = "no such server";
        return;
    }
    ChunkServerPtr const server(*si);
    server->SetRetiring(req.startTime, req.nSecsDown);
    if (server->IsHibernating()) {
        HibernatedServerInfos::iterator it;
        HibernatingServerInfo*          hs =
            FindHibernatingCSInfo(req.location, &it);
        if (hs) {
            hs->sleepEndTime = req.startTime + req.nSecsDown;
        } else {
            it = mHibernatingServers.insert(it, HibernatingServerInfo(
                req.location, req.startTime, req.nSecsDown, req.replayFlag));
            hs = &*it;
        }
        KFS_LOG_STREAM(req.replayFlag ?
                MsgLogger::kLogLevelDEBUG :
                MsgLogger::kLogLevelINFO) <<
            "hibernating server: " << req.location <<
            " down time: " << req.nSecsDown <<
        KFS_LOG_EOM;
        hs->retiredFlag = true;
        server->Retire(); // Remove when connection will go down.
        return;
    }
    mMightHaveRetiringServersFlag = true;
    if (server->GetChunkCount() <= 0) {
        server->Retire();
        return;
    }
    InitCheckAllChunks();
    return;
}

int64_t
LayoutManager::GetSlavePlacementScale()
{
    if (! mUpdatePlacementScaleFlag) {
        return mSlavePlacementScale;
    }
    mUpdatePlacementScaleFlag = false;
    // Make slaves comparable to masters for the purpose of RS placement
    // replication 1 or non append placement.
    mSlavePlacementScale = max(int64_t(1),
        ((mCSMasterLoadAvgSum * mCSSlavePossibleCandidateCount) <<
            kSlaveScaleFracBits / 2) /
        max(int64_t(1), (mCSSlaveLoadAvgSum *
            mCSMasterPossibleCandidateCount) >>
            (kSlaveScaleFracBits - kSlaveScaleFracBits / 2)));
    if (mSlavePlacementScale > mMaxSlavePlacementRange) {
        mSlavePlacementScale -= mMaxSlavePlacementRange;
    } else {
        mSlavePlacementScale = int64_t(1) << kSlaveScaleFracBits;
    }
    return mSlavePlacementScale;
}

void
LayoutManager::UpdateGoodCandidateLoadAvg()
{
    if (! mUpdateCSLoadAvgFlag) {
        return;
    }
    mUpdateCSLoadAvgFlag = false;
    mCSMaxGoodCandidateLoadAvg = (int64_t)(mCSLoadAvgSum *
        mCSMaxGoodCandidateLoadRatio /
        max(mCSTotalPossibleCandidateCount, 1));
    mCSMaxGoodMasterCandidateLoadAvg = (int64_t)(mCSMasterLoadAvgSum *
        mCSMaxGoodMasterLoadRatio /
        max(mCSMasterPossibleCandidateCount, 1));
    mCSMaxGoodSlaveCandidateLoadAvg = (int64_t)(mCSSlaveLoadAvgSum *
        mCSMaxGoodSlaveLoadRatio /
        max(mCSSlavePossibleCandidateCount, 1));
}

bool
LayoutManager::IsCandidateServer(
    const ChunkServer& c,
    kfsSTier_t         tier /* = kKfsSTierUndef */,
    double             writableChunksThresholdRatio /* = 1 */)
{
    UpdateGoodCandidateLoadAvg();
    return (
        c.GetCanBeCandidateServerFlag() &&
        (tier == kKfsSTierUndef || c.GetCanBeCandidateServerFlag(tier)) &&
        (c.GetLoadAvg() <= (c.CanBeChunkMaster() ?
            mCSMaxGoodMasterCandidateLoadAvg :
            mCSMaxGoodSlaveCandidateLoadAvg)) &&
        (tier == kKfsSTierUndef ?
            c.GetNumChunkWrites() < c.GetNumWritableDrives() *
                mMaxWritesPerDriveThreshold * writableChunksThresholdRatio
            :
            c.GetNotStableOpenCount(tier) < c.GetDeviceCount(tier) *
                mTiersMaxWritesPerDriveThreshold[tier] *
                writableChunksThresholdRatio
        )
    );
}

void
LayoutManager::UpdateSrvLoadAvg(ChunkServer& srv, int64_t delta,
    const LayoutManager::StorageTierInfo* tiersDelta,
    bool canBeCandidateFlag)
{
    if (! mChunkToServerMap.Validate(srv)) {
        panic("update load average: invalid server");
        return;
    }
    mUpdateCSLoadAvgFlag      = true;
    mUpdatePlacementScaleFlag = true;
    const bool wasPossibleCandidate = srv.GetCanBeCandidateServerFlag();
    mCSTotalLoadAvgSum += delta;
    if (wasPossibleCandidate && delta != 0) {
        mCSLoadAvgSum += delta;
        if (srv.CanBeChunkMaster()) {
            mCSMasterLoadAvgSum += delta;
        } else {
            mCSSlaveLoadAvgSum += delta;
        }
        assert(mCSLoadAvgSum >= 0 &&
            mCSMasterLoadAvgSum >= 0 && mCSSlaveLoadAvgSum >= 0);
    }
    bool isPossibleCandidate = canBeCandidateFlag &&
        srv.GetAvailSpace() >= mChunkAllocMinAvailSpace &&
        srv.IsResponsiveServer() &&
        ! srv.IsHibernatingOrRetiring() &&
        ! srv.IsRestartScheduled();
    int candidateTiersCount = 0;
    int racksCandidatesDelta[kKfsSTierCount];
    for (size_t i = 0; i < kKfsSTierCount; i++) {
        const bool flag = isPossibleCandidate &&
            srv.GetDeviceCount(i) > 0 &&
            srv.GetStorageTierAvailSpace(i) >= mChunkAllocMinAvailSpace &&
            srv.GetStorageTierSpaceUtilization(i) <=
                GetMaxTierSpaceUtilization(i)
        ;
        if (flag) {
            candidateTiersCount++;
        }
        if (flag == srv.GetCanBeCandidateServerFlag(i)) {
            racksCandidatesDelta[i] = 0;
            continue;
        }
        srv.SetCanBeCandidateServerFlag(i, flag);
        if (flag) {
            mTierCandidatesCount[i]++;
        } else if (mTierCandidatesCount[i] > 0) {
            mTierCandidatesCount[i]--;
            KFS_LOG_STREAM_DEBUG <<
                " not for placement:"
                " srv: "        << srv.GetServerLocation() <<
                " tier: "       << i <<
                " candidate: "  << isPossibleCandidate <<
                " can: "        << canBeCandidateFlag <<
                " was: "        << wasPossibleCandidate <<
                " count: "      << mTierCandidatesCount[i] <<
                " devs: "       << srv.GetDeviceCount(i) <<
                " space:"
                " total: "      << srv.GetTotalSpace(false) <<
                " avail: "      << srv.GetAvailSpace() <<
                " min: "        << mChunkAllocMinAvailSpace <<
                " tier: "       << srv.GetStorageTierAvailSpace(i) <<
                " util: "       << srv.GetStorageTierSpaceUtilization(i) <<
                " max: "        << GetMaxTierSpaceUtilization(i) <<
                " writes: "     << srv.GetNumChunkWrites() <<
                " responsive: " << srv.IsResponsiveServer() <<
                " restart: "    << srv.IsRestartScheduled() <<
                " retiring: "   << srv.IsHibernatingOrRetiring() <<
            KFS_LOG_EOM;
            if (mPanicOnRemoveFromPlacementFlag && canBeCandidateFlag &&
                    srv.IsConnected()) {
                panic("connected removed from placement");
            }
        }
        racksCandidatesDelta[i] = flag ? 1 : -1;
    }
    if (isPossibleCandidate && candidateTiersCount <= 0) {
        isPossibleCandidate = false;
    }
    const int inc = wasPossibleCandidate == isPossibleCandidate ? 0 :
        (isPossibleCandidate ? 1 : -1);
    if (inc == 0 && ! tiersDelta) {
        return;
    }
    if (! srv.IsReplay() || mReplaySetRackFlag) {
        RackInfos::iterator const rackIter = FindRack(srv.GetRack());
        if (rackIter != mRacks.end()) {
            rackIter->updatePossibleCandidatesCount(
                inc, tiersDelta, racksCandidatesDelta);
        }
    }
    if (inc == 0) {
        return;
    }
    mCSTotalPossibleCandidateCount += inc;
    if (srv.CanBeChunkMaster()) {
        mCSMasterPossibleCandidateCount += inc;
    } else {
        mCSSlavePossibleCandidateCount += inc;
    }
    assert(
        mCSTotalPossibleCandidateCount >= 0 &&
        mCSMasterPossibleCandidateCount >= 0 &&
        mCSSlavePossibleCandidateCount >= 0
    );
    const int64_t davg = isPossibleCandidate ?
        srv.GetLoadAvg() : -srv.GetLoadAvg();
    mCSLoadAvgSum += davg;
    if (srv.CanBeChunkMaster()) {
        mCSMasterLoadAvgSum += davg;
    } else {
        mCSSlaveLoadAvgSum += davg;
    }
    assert(mCSLoadAvgSum >= 0 &&
        mCSMasterLoadAvgSum >= 0 && mCSSlaveLoadAvgSum >= 0);
    srv.SetCanBeCandidateServerFlag(isPossibleCandidate);
}

void
LayoutManager::UpdateObjectsCount(
    ChunkServer& srv, int64_t delta, int64_t writableDelta)
{
    if (0 == delta && 0 == writableDelta) {
        return;
    }
    mCSOpenObjectCount     += delta;
    mCSWritableObjectCount += writableDelta;
    assert(0 <= mCSOpenObjectCount && 0 <= mCSWritableObjectCount &&
        mCSWritableObjectCount <= mCSOpenObjectCount);
}

void
LayoutManager::UpdateChunkWritesPerDrive(
    ChunkServer&                          srv,
    int                                   deltaNumChunkWrites,
    int                                   deltaNumWritableDrives,
    const LayoutManager::StorageTierInfo* tiersDelta)
{
    mTotalChunkWrites    += deltaNumChunkWrites;
    mTotalWritableDrives += deltaNumWritableDrives;
    if (deltaNumWritableDrives != 0) {
        mTotalWritableDrivesMult = mTotalWritableDrives > 0 ?
            mMaxWritesPerDriveRatio / mTotalWritableDrives : 0.;
    }
    if (mTotalChunkWrites <= 0) {
        mTotalChunkWrites = 0;
    }
    if (mTotalWritableDrives <= 0) {
        mTotalWritableDrives = 0;
        mMaxWritesPerDriveThreshold = mMinWritesPerDrive;
    } else if (mTotalChunkWrites <= mTotalWritableDrives) {
        mMaxWritesPerDriveThreshold = mMinWritesPerDrive;
    } else {
        mMaxWritesPerDriveThreshold = min(mMaxWritesPerDrive,
            max(mMinWritesPerDrive, (int)(mTotalChunkWrites *
            mTotalWritableDrivesMult)));
    }
    assert(tiersDelta);
    if (! tiersDelta) {
        return;
    }
    for (size_t i = 0; i < kKfsSTierCount; i++) {
        StorageTierInfo& info = mStorageTierInfo[i];
        info += tiersDelta[i];
        if (tiersDelta[i].GetDeviceCount() != 0) {
            mTiersTotalWritableDrivesMult[i] = info.GetDeviceCount() > 0 ?
                mMaxWritesPerDriveRatio / info.GetDeviceCount() : 0.;
        }
        if (info.GetDeviceCount() <= 0) {
            mTiersMaxWritesPerDriveThreshold[i] = mMinWritesPerDrive;
            info.Clear();
        } else if (info.GetNotStableOpenCount() <= info.GetDeviceCount()) {
            mTiersMaxWritesPerDriveThreshold[i] = mMinWritesPerDrive;
        } else {
            mTiersMaxWritesPerDriveThreshold[i] = min(mTiersMaxWritesPerDrive[i],
                max(mMinWritesPerDrive, (int)(info.GetNotStableOpenCount() *
                    mTiersTotalWritableDrivesMult[i])));
        }
    }
}

void
LayoutManager::Disconnected(const ChunkServer& srv)
{
    if (srv.IsReplay()) {
        return;
    }
    mDisconnectedCount++;
}

bool
LayoutManager::FindStorageTiersRange(kfsSTier_t& minTier, kfsSTier_t& maxTier)
{
    if (kKfsSTierMax < minTier) {
        minTier = kKfsSTierMax;
    } else if (! IsValidSTier(minTier)) {
        minTier = kKfsSTierMin;
    }
    if (kKfsSTierMax < maxTier) {
        maxTier = kKfsSTierMax;
    } else if (! IsValidSTier(maxTier)) {
        maxTier = kKfsSTierMin;
    }
    while (minTier <= maxTier && mTierCandidatesCount[minTier] <= 0) {
        ++minTier;
    }
    while (minTier <= maxTier && mTierCandidatesCount[maxTier] <= 0) {
        --maxTier;
    }
    return (minTier <= maxTier);
}

inline double
GetRackWeight(
    const LayoutManager::RackInfos& racks,
    LayoutManager::RackId           rack,
    double                          maxWeight)
{
    if (rack < 0) {
        return maxWeight;
    }
    LayoutManager::RackInfos::const_iterator const it =
        LayoutManager::FindRackT(racks.begin(), racks.end(), rack);
    return (it == racks.end() ?
        maxWeight : min(maxWeight, it->getWeight()));
}

int
LayoutManager::AllocateChunk(
    MetaAllocate& req, const vector<MetaChunkInfo*>& chunkBlock)
{
    // req.offset is a multiple of CHUNKSIZE
    assert(req.offset >= 0 && (req.offset % CHUNKSIZE) == 0);

    req.servers.clear();
    if (0 == req.numReplicas) {
        if (! FindAccessProxy(req)) {
            req.servers.clear();
            return (req.status == 0 ? -ENOSPC : req.status);
        }
        if (! mChunkLeases.NewWriteLease(req)) {
            req.statusMsg = "failed to get write lease for a new chunk";
            req.servers.clear();
            return -EBUSY;
        }
        req.allChunkServersShortRpcFlag =
            req.servers.front()->IsShortRpcFormat();
        req.servers.front()->AllocateChunk(req, req.leaseId, req.minSTier);
        return 0;
    }
    if (req.numReplicas <= 0) {
        KFS_LOG_STREAM_DEBUG <<
            "allocate chunk reaplicas: " << req.numReplicas <<
            " request: " << req.Show() <<
        KFS_LOG_EOM;
        req.statusMsg = "0 replicas";
        return -EINVAL;
    }
    kfsSTier_t minTier = req.minSTier;
    kfsSTier_t maxTier = req.maxSTier;
    if (! FindStorageTiersRange(minTier, maxTier)) {
        KFS_LOG_STREAM_DEBUG <<
            "allocate chunk no space: tiers: [" <<
                (int)req.minSTier << "," << (int)req.maxSTier << "] => [" <<
                (int)minTier << "," << (int)minTier << "]" <<
                " servers: " << mChunkServers.size() <<
        KFS_LOG_EOM;
        req.statusMsg = "no space available";
        return -ENOSPC;
    }
    StTmp<ChunkPlacement> placementTmp(mChunkPlacementTmp);
    ChunkPlacement&       placement = placementTmp.Get();
    if (req.stripedFileFlag) {
        // For replication greater than one do the same placement, but
        // only take into the account write masters, or the chunk server
        // hosting the first replica.
        for (StripedFilesAllocationsInFlight::const_iterator it =
                mStripedFilesAllocationsInFlight.lower_bound(
                    make_pair(make_pair(
                    req.fid, req.chunkBlockStart), 0));
                it != mStripedFilesAllocationsInFlight.end() &&
                it->first.first == req.fid;
                ++it) {
            if (it->first.second == req.chunkBlockStart) {
                const ChunkLeases::WriteLease* const lease =
                    mChunkLeases.GetChunkWriteLease(it->second);
                if (! lease || ! lease->chunkServer) {
                    continue;
                }
                if (lease->allocInFlight &&
                        lease->allocInFlight->status == 0) {
                    placement.ExcludeServerAndRack(
                        lease->allocInFlight->servers,
                        it->second);
                } else {
                    placement.ExcludeServerAndRack(
                        lease->chunkServer, it->second);
                }
            }
        }
        StTmp<Servers> serversTmp(mServersTmp);
        for (vector<MetaChunkInfo*>::const_iterator it =
                chunkBlock.begin();
                it != chunkBlock.end();
                ++it) {
            Servers& srvs = serversTmp.Get();
            mChunkToServerMap.GetServers(GetCsEntry(**it), srvs);
            placement.ExcludeServerAndRack(srvs, (*it)->chunkId);
        }
    }
    req.servers.reserve(req.numReplicas);
    StTmp<vector<kfsSTier_t> > tiersTmp(mPlacementTiersTmp);
    vector<kfsSTier_t>&        tiers = tiersTmp.Get();

    // For non-record append case, take the server local to the machine on
    // which the client is on make that the master; this avoids a network
    // transfer. For the record append case, to avoid deadlocks when writing
    // out large records, we are doing hierarchical allocation: a chunkserver
    // that is a chunk master is never made a slave.
    ChunkServerPtr localserver;
    int            replicaCnt = 0;
    Servers::const_iterator const li = (req.appendChunk ?
        (mAllowLocalPlacementForAppendFlag && ! mInRackPlacementForAppendFlag) :
        mAllowLocalPlacementFlag) ?
        FindServerForReq(req) :
        mChunkServers.end();
    if (li != mChunkServers.end() &&
            (mAppendPlacementIgnoreMasterSlaveFlag ||
                ! req.appendChunk || (*li)->CanBeChunkMaster()) &&
            IsCandidateServer(**li, minTier, GetRackWeight(
                mRacks, (*li)->GetRack(), mMaxLocalPlacementWeight)) &&
            ! placement.IsExcluded(*li)) {
        replicaCnt++;
        localserver = *li;
        placement.ExcludeServer(localserver);
    }
    RackId rackIdToUse = -1;
    if ((req.appendChunk ?
            mInRackPlacementForAppendFlag :
            mInRackPlacementFlag) &&
            ! mRacks.empty() && ! req.clientIp.empty()) {
        if (li != mChunkServers.end()) {
            rackIdToUse = (*li)->GetRack();
        }
        if (rackIdToUse < 0) {
            rackIdToUse = GetRackId(req);
        }
        if (rackIdToUse < 0 && li == mChunkServers.end()) {
            Servers::const_iterator const it = FindServerForReq(req);
            if (it != mChunkServers.end()) {
                rackIdToUse = (*it)->GetRack();
            }
        }
    }
    placement.FindCandidatesInRack(minTier, maxTier, rackIdToUse);
    size_t numServersPerRack(1);
    if (req.numReplicas > 1) {
        numServersPerRack = placement.GetCandidateRackCount();
        if (req.appendChunk ?
                mInRackPlacementForAppendFlag :
                mInRackPlacementFlag) {
            numServersPerRack = req.numReplicas;
        } else if (numServersPerRack <= 1 ||
                (numServersPerRack < (size_t)req.numReplicas &&
                placement.GetExcludedRacksCount() > 0)) {
            // Place first replica, then re-calculate.
            numServersPerRack = 1;
        } else {
            numServersPerRack = ((size_t)req.numReplicas +
                numServersPerRack - 1) / numServersPerRack;
        }
    }
    // For append always reserve the first slot -- write master.
    if ((req.appendChunk && ! mAppendPlacementIgnoreMasterSlaveFlag) ||
            localserver) {
        req.servers.push_back(localserver);
        tiers.push_back(minTier);
    }
    int    mastersSkipped = 0;
    int    slavesSkipped  = 0;
    size_t numCandidates  = 0;
    for (; ;) {
        // take as many as we can from this rack
        const size_t psz    = req.servers.size();
        const RackId rackId = placement.GetRackId();
        for (size_t n = (localserver &&
                rackId == localserver->GetRack()) ? 1 : 0;
                (n < numServersPerRack || rackId < 0) &&
                    replicaCnt < req.numReplicas;
                ) {
            const ChunkServerPtr cs = placement.GetNext(req.stripedFileFlag);
            if (! cs) {
                break;
            }
            if (placement.IsUsingServerExcludes() &&
                    find(req.servers.begin(), req.servers.end(), cs) !=
                    req.servers.end()) {
                continue;
            }
            numCandidates++;
            if (req.appendChunk && ! mAppendPlacementIgnoreMasterSlaveFlag) {
                // for record appends, to avoid deadlocks for
                // buffer allocation during atomic record
                // appends, use hierarchical chunkserver
                // selection
                if (cs->CanBeChunkMaster()) {
                    if (req.servers.front()) {
                        mastersSkipped++;
                        continue;
                    }
                    req.servers.front() = cs;
                    tiers.front() = placement.GetStorageTier();
                } else {
                    if (req.servers.size() >=
                            (size_t)req.numReplicas) {
                        slavesSkipped++;
                        continue;
                    }
                    if (mAllocateDebugVerifyFlag &&
                            find(req.servers.begin(), req.servers.end(), cs) !=
                                req.servers.end()) {
                        panic("allocate: duplicate slave");
                        continue;
                    }
                    req.servers.push_back(cs);
                    tiers.push_back(placement.GetStorageTier());
                }
            } else {
                if (mAllocateDebugVerifyFlag &&
                        find(req.servers.begin(), req.servers.end(), cs) !=
                            req.servers.end()) {
                    panic("allocate: duplicate server");
                    continue;
                }
                req.servers.push_back(cs);
                tiers.push_back(placement.GetStorageTier());
            }
            n++;
            replicaCnt++;
        }
        if (req.numReplicas <= replicaCnt || placement.IsLastAttempt()) {
            break;
        }
        if (req.appendChunk && mInRackPlacementForAppendFlag && rackId >= 0 &&
                (req.numReplicas + 1) * placement.GetCandidateRackCount() <
                    mChunkServers.size()) {
            // Reset, try to find another rack where both replicas
            // can be placed.
            // This assumes that the racks are reasonably
            // "balanced".
            replicaCnt = 0;
            req.servers.clear();
            localserver.reset();
            tiers.clear();
            if (! mAppendPlacementIgnoreMasterSlaveFlag) {
                req.servers.push_back(localserver);
                tiers.push_back(minTier);
            }
        } else if (req.stripedFileFlag && req.numReplicas > 1 &&
                numServersPerRack == 1 &&
                psz == 0 && req.servers.size() == size_t(1)) {
            // Striped file placement: attempt to place the first
            // chunk replica on a different rack / server than other
            // chunks in the stripe.
            // Attempt to place all subsequent replicas on different
            // racks.
            placement.clear();
            placement.ExcludeServerAndRack(req.servers);
            placement.FindCandidates(minTier, maxTier);
            numServersPerRack = placement.GetCandidateRackCount();
            numServersPerRack = numServersPerRack <= 1 ?
                (size_t)(req.numReplicas - replicaCnt) :
                ((size_t)(req.numReplicas - replicaCnt) +
                numServersPerRack - 1) / numServersPerRack;
        } else {
            placement.ExcludeServer(
                req.servers.begin() + psz, req.servers.end());
        }
        if (! placement.NextRack()) {
            break;
        }
    }
    bool noMaster = false;
    if (req.servers.empty() || (noMaster = ! req.servers.front())) {
        req.statusMsg = noMaster ? "no master" : "no servers";
        int dontLikeCount[2]      = { 0, 0 };
        int outOfSpaceCount[2]    = { 0, 0 };
        int notResponsiveCount[2] = { 0, 0 };
        int retiringCount[2]      = { 0, 0 };
        int restartingCount[2]    = { 0, 0 };
        for (Servers::const_iterator it = mChunkServers.begin();
                it != mChunkServers.end();
                ++it) {
            const ChunkServer& cs = **it;
            const int i = cs.CanBeChunkMaster() ? 0 : 1;
            if (! IsCandidateServer(cs)) {
                dontLikeCount[i]++;
            }
            if (cs.GetAvailSpace() < mChunkAllocMinAvailSpace ||
                    cs.GetSpaceUtilization(
                        mUseFsTotalSpaceFlag) >
                    mMaxSpaceUtilizationThreshold) {
                outOfSpaceCount[i]++;
            }
            if (! cs.IsResponsiveServer()) {
                notResponsiveCount[i]++;
            }
            if (cs.IsHibernatingOrRetiring()) {
                retiringCount[i]++;
            }
            if (cs.IsRestartScheduled()) {
                restartingCount[i]++;
            }
            KFS_LOG_STREAM_DEBUG <<
                "allocate: "          << req.statusMsg <<
                " fid: "              << req.fid <<
                " offset: "           << req.offset <<
                " chunk: "            << req.chunkId <<
                " append: "           << req.appendChunk <<
                " server: "           << cs.GetHostPortStr() <<
                " master: "           << cs.CanBeChunkMaster() <<
                " wr-drives: "        << cs.GetNumWritableDrives() <<
                " candidate: "        << cs.GetCanBeCandidateServerFlag() <<
                " writes: "           << cs.GetNumChunkWrites() <<
                " max-wr-per-drive: " << mMaxWritesPerDriveThreshold <<
                " load-avg: "         << cs.GetLoadAvg() <<
                " max-load: "         << mCSMaxGoodMasterCandidateLoadAvg <<
                " / "                 << mCSMaxGoodSlaveCandidateLoadAvg <<
                " space:"
                " avail: "            << cs.GetAvailSpace() <<
                " util: "             <<
                    cs.GetSpaceUtilization(mUseFsTotalSpaceFlag) <<
                " max util: "         << mMaxSpaceUtilizationThreshold <<
                " retire: "           << cs.IsHibernatingOrRetiring() <<
                " responsive: "       << cs.IsResponsiveServer() <<
            KFS_LOG_EOM;
        }
        const size_t numFound = req.servers.size();
        req.servers.clear();
        KFS_LOG_STREAM_INFO << "allocate: " <<
            req.statusMsg <<
            " fid: "        << req.fid <<
            " offset: "     << req.offset <<
            " chunk: "      << req.chunkId <<
            " append: "     << req.appendChunk <<
            " repl: "       << req.numReplicas <<
            "/"             << replicaCnt <<
            " servers: "    << numFound <<
            "/"             << mChunkServers.size() <<
            " dont like: "  << dontLikeCount[0] <<
            "/"             << dontLikeCount[1] <<
            " no space: "   << outOfSpaceCount[0] <<
            "/"             << outOfSpaceCount[1] <<
            " slow: "       << notResponsiveCount[0] <<
            "/"             << notResponsiveCount[1] <<
            " retire: "     << retiringCount[0] <<
            "/"             << retiringCount[1] <<
            " restart: "    << restartingCount[0] <<
            "/"             << restartingCount[1] <<
            " racks: "      << placement.GetCandidateRackCount() <<
            " candidates: " << numCandidates <<
            " masters: "    << mastersSkipped <<
            "/"             << mMastersCount <<
            " slaves: "     << slavesSkipped <<
            "/"             << mSlavesCount <<
            " to restart: " << mCSToRestartCount <<
            "/"             << mMastersToRestartCount <<
            " request: "    << req.Show() <<
        KFS_LOG_EOM;
        return -ENOSPC;
    }
    assert(
        ! req.servers.empty() && req.status == 0 &&
        req.servers.size() <= (size_t)req.numReplicas &&
        req.servers.size() == tiers.size()
    );

    if (! mChunkLeases.NewWriteLease(req)) {
        panic("failed to get write lease for a new chunk");
        req.servers.clear();
        return -EFAULT;
    }

    if (req.stripedFileFlag) {
        if (! mStripedFilesAllocationsInFlight.insert(make_pair(make_pair(
                req.fid, req.chunkBlockStart), req.chunkId)).second) {
            panic("duplicate in striped file allocation entry");
        }
    }
    if (mClientCSAuthRequiredFlag) {
        req.clientCSAllowClearTextFlag = mClientCSAllowClearTextFlag;
        req.issuedTime                 = TimeNow();
        req.validForTime               = mCSAccessValidForTimeSec;
    } else {
        req.validForTime = 0;
    }
    req.allChunkServersShortRpcFlag = true;
    for (Servers::const_iterator it = req.servers.begin();
            it != req.servers.end();
            ++it) {
        if (! (*it)->IsShortRpcFormat()) {
            req.allChunkServersShortRpcFlag = false;
            break;
        }
    }
    if (req.appendChunk) {
        mARAChunkCache.RequestNew(req);
    }
    for (size_t i = req.servers.size(); i-- > 0; ) {
        req.servers[i]->AllocateChunk(req, i == 0 ? req.leaseId : -1, tiers[i]);
    }
    return 0;
}

int
LayoutManager::ProcessBeginChangeChunkVersion(
    fid_t     fid,
    chunkId_t chunkId,
    seq_t     chunkVersion,
    string*   statusMsg,
    bool      panicOnInvaliVersionFlag)
{
    const char*               msg  = "OK";
    int                       ret  = 0;
    const CSMap::Entry* const cs   = mChunkToServerMap.Find(chunkId);
    seq_t                     vers;
    if (cs) {
        vers = cs->GetChunkInfo()->chunkVersion;
        if (chunkVersion <= vers) {
            msg = "invalid version transition";
            ret = -EINVAL;
        } else {
            bool         insertedFlag = false;
            const seq_t  entryVal     = chunkVersion - vers;
            seq_t* const res          = mChunkVersionRollBack.Insert(
                chunkId, entryVal, insertedFlag);
            if (! insertedFlag) {
                if (chunkVersion <= vers + *res) {
                    msg = "version roll back entry invalid version transition";
                    ret = -EINVAL;
                } else {
                    *res = entryVal;
                }
            }
        }
    } else {
        vers = -1;
        msg  = "no such chunk";
        ret  = -ENOENT;
    }
    KFS_LOG_STREAM(0 != ret ?
        MsgLogger::kLogLevelERROR :
        MsgLogger::kLogLevelDEBUG) <<
        "begin chunk version change:"
        " fid: "     << fid <<
        " chunk: "   << chunkId <<
        " version: " << vers <<
        " => "       << chunkVersion <<
        " "          << msg <<
    KFS_LOG_EOM;
    if (ret == -EINVAL && panicOnInvaliVersionFlag) {
        panic(msg);
    }
    if (0 != ret && statusMsg) {
        *statusMsg = msg;
    }
    return ret;
}

int
LayoutManager::WritePendingChunkVersionChange(ostream& os)
{
    ReqOstream ros(os);
    const ChunkVersionRollBackEntry* it;
    mChunkVersionRollBack.First();
    while ((it = mChunkVersionRollBack.Next()) && os) {
        if (it->GetVal() <= 0) {
            KFS_LOG_STREAM_ERROR <<
                "version change invalid chunk roll back entry:"
                " chunk: "             << it->GetKey() <<
                " version increment: " << it->GetVal() <<
            KFS_LOG_EOM;
            continue;
        }
        const CSMap::Entry* const ci = mChunkToServerMap.Find(it->GetKey());
        if (! ci) {
            // Stale mapping.
            KFS_LOG_STREAM_ERROR <<
                "version change failed to get chunk mapping:"
                " chunk: "             << it->GetKey() <<
                " version increment: " << it->GetVal() <<
            KFS_LOG_EOM;
            continue;
        }
        const seq_t vers = ci->GetChunkInfo()->chunkVersion;
        ros << "beginchunkversionchange"
            "/file/"         << ci->GetFileId() <<
            "/chunkId/"      << it->GetKey() <<
            "/chunkVersion/" << (vers + it->GetVal()) <<
        "\n";
    }
    ros.flush();
    return (os ? 0 : -EIO);
}

int
LayoutManager::GetInFlightChunkOpsCount(chunkId_t chunkId, MetaOp opType,
    chunkOff_t objStoreBlockPos /* = -1 */) const
{
    const MetaOp types[] = { opType,  META_NUM_OPS_COUNT };
    return GetInFlightChunkOpsCount(chunkId, types, objStoreBlockPos);
}

int
LayoutManager::GetInFlightChunkModificationOpCount(
    chunkId_t               chunkId,
    chunkOff_t              objStoreBlockPos /* = -1 */,
    LayoutManager::Servers* srvs             /* = 0 */) const
{
    MetaOp const types[] = {
        META_CHUNK_REPLICATE,        // Recovery or replication.
        META_CHUNK_VERSCHANGE,       // Always runs after recovery.
        META_CHUNK_MAKE_STABLE,
        META_CHUNK_OP_LOG_IN_FLIGHT, // Queued in replay.
        META_NUM_OPS_COUNT           // Sentinel
    };
    return GetInFlightChunkOpsCount(chunkId, types, objStoreBlockPos, srvs);
}

int
LayoutManager::GetInFlightChunkOpsCount(
    chunkId_t               chunkId,
    const MetaOp*           opTypes,
    chunkOff_t              objStoreBlockPos /* = -1 */,
    LayoutManager::Servers* srvs              /* = 0 */) const
{
    int                                  ret = 0;
    const ChunkServer::ChunkOpsInFlight& ops =
        ChunkServer::GetChunkOpsInFlight();
    pair<
        ChunkServer::ChunkOpsInFlight::const_iterator,
        ChunkServer::ChunkOpsInFlight::const_iterator
    > const range = ops.equal_range(chunkId);
    for (ChunkServer::ChunkOpsInFlight::const_iterator it = range.first;
            it != range.second;
            ++it) {
        for (const MetaOp* op = opTypes;
                *op != META_NUM_OPS_COUNT;
                op++) {
            if (it->second->op != *op) {
                continue;
            }
            if (objStoreBlockPos < 0) {
                if (it->second->chunkVersion < 0) {
                    continue;
                }
            } else {
                if (0 <= it->second->chunkVersion) {
                    continue;
                }
                if (ChunkVersionToObjFileBlockPos(it->second->chunkVersion) !=
                        objStoreBlockPos) {
                    continue;
                }
            }
            ret++;
            if (srvs && find(srvs->begin(), srvs->end(),
                    it->second->server) == srvs->end()) {
                srvs->push_back(it->second->server);
            }
        }
    }
    return ret;
}

void
LayoutManager::GetChunkWriteLease(MetaAllocate& req)
{
    if (InRecovery()) {
        KFS_LOG_STREAM_INFO <<
            "GetChunkWriteLease: InRecovery() => EBUSY" <<
        KFS_LOG_EOM;
        req.statusMsg = "meta server in recovery mode";
        req.status    = -EBUSY;
        return;
    }
    if (0 == req.numReplicas) {
        if (! FindAccessProxy(req)) {
            if (0 == req.status) {
                req.status = -ENOSPC;
            }
            return;
        }
        ChunkLeases::EntryKey const leaseKey(req.fid, req.offset);
        const ChunkLeases::WriteLease* const l =
            mChunkLeases.RenewValidWriteLease(leaseKey, req);
        if (l) {
            if (l->allocInFlight) {
                req.statusMsg = "allocation is in progress";
                req.status    = -EBUSY;
                KFS_LOG_STREAM_INFO << "write lease denied"
                    " <" << req.fid << "@" << req.offset << "> " <<
                    req.statusMsg <<
                KFS_LOG_EOM;
            } else if (l->chunkServer && (req.servers.empty() ||
                    l->chunkServer->GetServerLocation() !=
                        req.servers.front()->GetServerLocation())) {
                req.statusMsg = "other access proxy owns write lease: " +
                    l->chunkServer->GetServerLocation().ToString();
                req.status    = -EBUSY;
                KFS_LOG_STREAM_INFO << "write lease denied"
                    " ip: " << req.clientIp <<
                    " <" << req.fid << "@" << req.offset << "> " <<
                    req.statusMsg <<
                KFS_LOG_EOM;
            } else if (GetInFlightChunkOpsCount(
                    req.fid, META_CHUNK_MAKE_STABLE, req.offset)) {
                req.statusMsg = "make block stable in progress";
                req.status    = -EBUSY;
                KFS_LOG_STREAM_INFO << "write lease denied"
                    " ip: " << req.clientIp <<
                    " <" << req.fid << "@" << req.offset << "> " <<
                    req.statusMsg <<
                KFS_LOG_EOM;
            }
            if (0 != req.status) {
                req.servers.clear();
                return;
            }
        }
        // Create new lease even though no version change done.
        mChunkLeases.Delete(leaseKey.first, leaseKey);
        if (! mChunkLeases.NewWriteLease(req)) {
            panic("failed to get write lease for a chunk");
            req.status = -EFAULT;
        }
        return;
    }
    if (GetInFlightChunkModificationOpCount(req.chunkId) > 0) {
        // Wait for re-replication to finish.
        KFS_LOG_STREAM_INFO << "write lease: " << req.chunkId <<
            " is being re-replicated => EBUSY" <<
        KFS_LOG_EOM;
        req.statusMsg = "replication is in progress";
        req.status    = -EBUSY;
        return;
    }
    const CSMap::Entry* const ci = mChunkToServerMap.Find(req.chunkId);
    if (! ci || metatree.getChunkDeleteQueue() == ci->GetFattr()) {
        req.statusMsg = "no such chunk";
        req.status    = -EINVAL;
        return;
    }
    req.servers.clear();
    size_t hibernatedCount   = 0;
    size_t disconnectedCount = 0;
    size_t cnt = mChunkToServerMap.GetConnectedServers(
        *ci, req.servers, hibernatedCount, disconnectedCount);
    req.servers.clear();
    int ret = 0;
    if (cnt <= disconnectedCount) {
        req.statusMsg = "no replicas available";
        ret = -EDATAUNAVAIL;
        if (! req.stripedFileFlag) {
            req.status = ret;
            return;
        }
        // Renew write lease with striped files, even if no
        // replica available to ensure that the chunk block can not
        // change, and recovery can not be started.
        // Chunk invalidation and normal chunk close (in the case when
        // replica re-appears) will expire the lease.
    } else if (0 < disconnectedCount) {
        req.statusMsg = "server down in flight";
        ret = -EBUSY;
    }

    ChunkLeases::EntryKey const leaseKey(req.chunkId);
    const ChunkLeases::WriteLease* const l =
        mChunkLeases.RenewValidWriteLease(leaseKey, req);
    if (l) {
        if (l->allocInFlight) {
            req.statusMsg =
                "allocation or version change is in progress";
            req.status    = -EBUSY;
            KFS_LOG_STREAM_INFO << "write lease denied"
                " chunk " << req.chunkId << " " << req.statusMsg <<
            KFS_LOG_EOM;
            return;
        }
        if (l->appendFlag) {
            req.statusMsg = "valid write append lease exists";
            req.status    = -EBUSY;
            KFS_LOG_STREAM_INFO << "write lease denied"
                " chunk " << req.chunkId << " " << req.statusMsg <<
            KFS_LOG_EOM;
            return;
        }
        // valid write lease; so, tell the client where to go
        KFS_LOG_STREAM_INFO <<
            "valid write lease:"
            " chunk: "      << req.chunkId <<
            " expires in: " << (l->expires - TimeNow()) << " sec."
            " replicas: "   << mChunkToServerMap.ServerCount(*ci) <<
            " status: "     << ret <<
        KFS_LOG_EOM;
        if (ret < 0) {
            req.leaseId       = l->leaseId;
            req.leaseDuration = req.authUid != kKfsUserNone ?
                l->endTime - TimeNow() : int64_t(-1);
            req.status = ret;
            return;
        }
        // Delete the lease to force version number bump.
        // Assume that the client encountered a write error, or other client
        // is writing into the same file.
        mChunkLeases.Delete(ci->GetFileId(), leaseKey);
    }
    if (ret < 0) {
        req.status = ret;
        return;
    }
    // there is no valid write lease; to issue a new write lease, we
    // need to do a version # bump.  do that only if we haven't yet
    // handed out valid read leases
    const int ownerDownExpireDelay = 0;
    if (! mChunkLeases.ExpiredCleanup(
            leaseKey, TimeNow(), ownerDownExpireDelay,
            mARAChunkCache, mChunkToServerMap)) {
        req.statusMsg = "valid read lease";
        req.status    = -EBUSY;
        KFS_LOG_STREAM_DEBUG << "write lease denied"
            " chunk " << req.chunkId << " " << req.statusMsg <<
        KFS_LOG_EOM;
        return;
    }
    // Check if make stable is in progress.
    // It is crucial to check the after invoking ExpiredCleanup()
    // Expired lease cleanup the above can start make chunk stable.
    if (! IsChunkStable(req.chunkId)) {
        req.statusMsg = "chunk is not stable";
        req.status    = -EBUSY;
        KFS_LOG_STREAM_DEBUG << "write lease denied"
            " chunk " << req.chunkId << " " << req.statusMsg <<
        KFS_LOG_EOM;
        return;
    }
    // Check if servers vector has changed:
    // chunk servers can go down in ExpiredCleanup()
    req.servers.clear();
    hibernatedCount   = 0;
    disconnectedCount = 0;
    cnt = mChunkToServerMap.GetConnectedServers(
        *ci, req.servers, hibernatedCount, disconnectedCount);
    if (cnt <= disconnectedCount) {
        // all the associated servers are dead...so, fail
        // the allocation request.
        req.statusMsg = "no replicas available";
        req.status    = -EDATAUNAVAIL;
    } else if (0 < disconnectedCount) {
        req.statusMsg = "server down in progress";
        req.status    = -EBUSY;
    }
    if (0 == req.status) {
        // Need space on the servers..otherwise, fail it
        for (Servers::const_iterator it = req.servers.begin();
                it != req.servers.end();
                ++it) {
            if ((*it)->GetAvailSpace() < mChunkAllocMinAvailSpace) {
                req.statusMsg = (*it)->GetServerLocation().ToString();
                req.statusMsg += " no space available";
                req.status = -ENOSPC;
                break;
            }
        }
    }
    req.servers.clear();
    if (0 != req.status) {
        return;
    }
    assert(req.chunkVersion == req.initialChunkVersion);
    // When issuing a new lease, increment the version, skipping over
    // the failed version increment attemtps.
    req.chunkVersion += GetChunkVersionRollBack(req.chunkId) + 1;
    if (! mChunkLeases.NewWriteLease(req)) {
        panic("failed to get write lease for a chunk");
        req.status = -EFAULT;
        return;
    }
    KFS_LOG_STREAM_INFO <<
        "new write"
        " lease: "   << req.leaseId <<
        " chunk: "   << req.chunkId <<
        " version: " << req.chunkVersion <<
    KFS_LOG_EOM;
    if (mClientCSAuthRequiredFlag) {
        req.clientCSAllowClearTextFlag = mClientCSAllowClearTextFlag;
        req.issuedTime                 = TimeNow();
        req.validForTime               = mCSAccessValidForTimeSec;
    } else {
        req.validForTime = 0;
    }
    return;
}

bool
LayoutManager::IsAllocationAllowed(MetaAllocate& req)
{
    if (req.clientProtoVers < mMinChunkAllocClientProtoVersion) {
        req.status    = -EPERM;
        req.statusMsg = "client upgrade required";
        return false;
    }
    if (InRecovery() && ! req.invalidateAllFlag) {
        req.statusMsg = "meta server in recovery mode";
        req.status    = -EBUSY;
        return false;
    }
    if (req.authUid != kKfsUserNone) {
        req.sessionEndTime = max(
            req.sessionEndTime, (int64_t)TimeNow() + mMinWriteLeaseTimeSec);
    }
    return true;
}

/*
 * \brief During atomic record appends, a client tries to allocate a block.
 * Since the client doesn't know the file size, the client notifies the
 * metaserver it is trying to append.  Simply allocating a new chunk for each
 * such request will cause too many chunks.  Instead, the metaserver picks one
 * of the existing chunks of the file which has a valid write lease (presumably,
 * that chunk is not full), and returns that info.  When the client gets the
 * info, it is possible that the chunk became full. In such a scenario, the
 * client may have to try  multiple times until it finds a chunk that it can
 * write to.
 */
int
LayoutManager::AllocateChunkForAppend(MetaAllocate& req)
{
    ARAChunkCache::Entry* const entry = mARAChunkCache.Get(req.fid);
    if (! entry) {
        return -1;
    }

    KFS_LOG_STREAM_DEBUG << "append"
        " file: "       << req.fid <<
        " pos: "        << req.offset <<
        " entry pos:  " << entry->offset <<
        (entry->IsAllocationPending() ? " allocation in progress" : "") <<
        " appenders: "  << entry->numAppendersInChunk <<
    KFS_LOG_EOM;

    if (entry->offset < 0 || (entry->offset % CHUNKSIZE) != 0 ||
            entry->servers.empty()) {
        panic("invalid write append cache entry");
        mARAChunkCache.Invalidate(req.fid);
        return -1;
    }
    if (mVerifyAllOpsPermissionsFlag &&
            ! entry->permissions->CanWrite(req.euser, req.egroup)) {
        req.status = -EPERM;
        return -1;
    }
    // The client is providing an offset hint in the case when it needs a
    // new chunk: space allocation failed because chunk is full, or it can
    // not talk to the chunk server.
    //
    // If allocation has already finished, then cache entry offset is valid,
    // otherwise the offset is equal to EOF at the time the initial request
    // has started. The client specifies offset just to indicate that it
    // wants a new chunk, and when the allocation finishes it will get the
    // new chunk.
    if (entry->offset < req.offset && ! entry->IsAllocationPending()) {
        mARAChunkCache.Invalidate(req.fid);
        return -1;
    }
    // Ensure that master is still good.
    if (entry->numAppendersInChunk > mMinAppendersPerChunk) {
        UpdateGoodCandidateLoadAvg();
        if (entry->servers.front()->GetLoadAvg() >
                mCSMaxGoodMasterCandidateLoadAvg) {
            KFS_LOG_STREAM_INFO <<
                "invalidating append cache entry: " <<
                req.fid <<
                " " << entry->servers.front()->GetServerLocation() <<
                " load: " << entry->servers.front()->GetLoadAvg() <<
                " exceeds: " <<
                    mCSMaxGoodMasterCandidateLoadAvg <<
            KFS_LOG_EOM;
            mARAChunkCache.Invalidate(req.fid);
            return -1;
        }
    }
    // Since there is no un-reservation mechanism, decay reservation by
    // factor of 2 every mReservationDecayStep sec.
    // The goal is primarily to decrease # or rtt and meta server cpu
    // consumption due to chunk space reservation contention between
    // multiple concurrent appenders, while keeping chunk size as large as
    // possible.
    // Start decay only after allocation completes.
    // Enforce timeout on pending allocation, in order not to re-queue the
    // timed out client back to the same allocation group.
    const time_t now = TimeNow();
    if (entry->IsAllocationPending()) {
        if (entry->lastDecayTime + mAllocAppendReuseInFlightTimeoutSec < now) {
            mARAChunkCache.Invalidate(req.fid);
            return -1;
        }
    } else if (mReservationDecayStep > 0 &&
            entry->lastDecayTime +
            mReservationDecayStep <= now) {
        const size_t exp = (now - entry->lastDecayTime) /
            mReservationDecayStep;
        if (exp >= sizeof(entry->spaceReservationSize) * 8) {
            entry->spaceReservationSize = 0;
        } else {
            entry->spaceReservationSize >>= exp;
        }
        entry->lastDecayTime = now;
    }
    const int reservationSize = (int)(min(double(mMaxReservationSize),
        mReservationOvercommitFactor *
        max(1, req.spaceReservationSize)));
    if (entry->spaceReservationSize + reservationSize >
            mChunkReservationThreshold) {
        return -1;
    }
    const ChunkLeases::WriteLease* const wl = mChunkLeases.RenewValidWriteLease(
            ChunkLeases::EntryKey(entry->chunkId), req);
    if (! wl) {
        mARAChunkCache.Invalidate(req.fid);
        return -1;
    }
    // valid write lease; so, tell the client where to go
    req.chunkId      = entry->chunkId;
    req.offset       = entry->offset;
    req.chunkVersion = entry->chunkVersion;
    entry->numAppendersInChunk++;
    entry->lastAccessedTime = now;
    entry->spaceReservationSize += reservationSize;
    const bool pending = entry->AddPending(req);
    if (! pending && req.responseStr.empty() && req.servers.empty()) {
        // The cached response will have or already has all the info.
        // Presently it should never get here.
        KFS_LOG_STREAM_WARN <<
            "invalid write append cache entry:"
            " no cached response"  <<
            " file: "   << req.fid <<
            " chunk: "  << entry->chunkId <<
            " offset: " << entry->offset <<
        KFS_LOG_EOM;
        mARAChunkCache.Invalidate(req.fid);
        return -1;
    }
    if (! req.responseAccessStr.empty()) {
        req.validForTime = mCSAccessValidForTimeSec;
    }
    bool accessExpiredFlag = false;
    if (! pending && mClientCSAuthRequiredFlag &&
            (req.responseAccessStr.empty() ||
            (accessExpiredFlag =
                req.issuedTime +
                    min(mCSAccessValidForTimeSec / 3,
                        LEASE_INTERVAL_SECS * 2 / 3) < now))) {
        // 2/3 of the lease time implicitly assumes that the chunk access token
        // life time is double of more of the lease time.
        if (! entry->servers.empty() &&
                (req.writeMasterKeyValidFlag =
                    entry->servers.front()->GetCryptoKey(
                        req.writeMasterKeyId, req.writeMasterKey))) {
            req.clientCSAllowClearTextFlag = mClientCSAllowClearTextFlag;
            req.issuedTime                 = now;
            req.validForTime               = mCSAccessValidForTimeSec;
            if (accessExpiredFlag &&
                    entry->numAppendersInChunk < mMaxAppendersPerChunk) {
                req.responseAccessStr.clear();
                req.tokenSeq = (MetaAllocate::TokenSeq)mRandom.Rand();
                ostringstream& os = GetTempOstream();
                ReqOstream ros(os);
                req.writeChunkAccess(ros);
                req.responseAccessStr = os.str();
                entry->SetResponseAccess(req);
            }
        } else {
            KFS_LOG_STREAM_WARN <<
                "invalid write append cache entry:"
                " no master or master has no valid crypto key"  <<
                " file: "   << req.fid <<
                " chunk: "  << entry->chunkId <<
                " offset: " << entry->offset <<
            KFS_LOG_EOM;
            req.responseAccessStr.clear();
            mARAChunkCache.Invalidate(req.fid);
            return -1;
        }
    }
    KFS_LOG_STREAM_DEBUG <<
        "valid write lease exists for chunk " << req.chunkId <<
        " expires in " << (wl->expires - TimeNow()) << " sec" <<
        " space: " << entry->spaceReservationSize <<
        " (+" << reservationSize <<
        "," << req.spaceReservationSize << ")" <<
        " appenders: " << entry->numAppendersInChunk <<
        (pending ? " allocation in progress" : "") <<
        " access: size: " << req.responseAccessStr.size() <<
    KFS_LOG_EOM;
    if (mMaxAppendersPerChunk <= entry->numAppendersInChunk) {
        mARAChunkCache.Invalidate(req.fid);
    }
    return 0;
}

/*
 * The chunk files are named <fid, chunkid, version>. The fid is now ignored by
 * the meta server.
*/
int
LayoutManager::ChangeChunkFid(MetaFattr* srcFattr, MetaFattr* dstFattr,
    MetaChunkInfo* chunk)
{
    if (mChunkEntryToChange || mFattrToChangeTo) {
        panic("coalesce blocks:  invalid invocation:"
            " previous change pending");
        return -EFAULT;
    }
    if (! chunk) {
        if (! srcFattr) {
            if (dstFattr) {
                panic("coalesce blocks: invalid invocation:"
                    " src fattr is not null");
                return -EFAULT;
            }
            return 0;
        }
        if (! dstFattr) {
            panic("coalesce blocks: invalid invocation: dst fattr is null");
            return -EFAULT;
        }
        if (mChunkLeases.IsDeleteScheduled(srcFattr->id()) ||
                mChunkLeases.IsDeleteScheduled(dstFattr->id())) {
            return -EPERM;
        }
        // Invalidate fid cache.
        mARAChunkCache.Invalidate(srcFattr->id());
        return 0;
    }
    if (! dstFattr) {
        panic("coalesce blocks:  invalid invocation:"
            " null destination fattr");
        return -EFAULT;
    }

    CSMap::Entry& entry = GetCsEntry(*chunk);
    if (entry.GetFattr() != srcFattr) {
        ostringstream& os = GetTempOstream();
        os <<
            "coalesce blocks: chunk: " << chunk->chunkId <<
            " undexpected file attr: " << (void*)entry.GetFattr() <<
            " id: "                    << entry.GetFileId() <<
            " expect: "                << (void*)srcFattr <<
            " id: "                    << srcFattr->id()
        ;
        const string msg = os.str();
        panic(msg.c_str());
        return -EFAULT;
    }
    mChunkEntryToChange = &entry;
    mFattrToChangeTo    = dstFattr;
    return 0;
}

void
LayoutManager::GetChunkReadLeases(MetaLeaseAcquire& req)
{
    req.responseBuf.Clear();
    if (req.chunkIds.empty()) {
        return;
    }
    if (req.appendRecoveryFlag) {
        req.statusMsg = "no chunk list allowed with append recovery";
        req.status    = -EINVAL;
        return;
    }
    StTmp<Servers>    serversTmp(mServers3Tmp);
    Servers&          servers = serversTmp.Get();
    const bool        recoveryFlag = InRecovery() && ! req.replayFlag;
    const char*       p            = req.chunkIds.GetPtr();
    const char*       e            = p + req.chunkIds.GetSize();
    IntIOBufferWriter writer(req.responseBuf);
    const bool        emitCAFlag   = req.authUid != kKfsUserNone &&
        0 < req.leaseTimeout && mClientCSAuthRequiredFlag;
    while (p < e) {
        chunkId_t chunkId = -1;
        if (! req.ParseInt(p, e - p, chunkId)) {
            while (p < e && *p <= ' ') {
                p++;
            }
            if (p != e) {
                req.status    = -EINVAL;
                req.statusMsg = "chunk id list parse error";
            }
            break;
        }
        ChunkLeases::LeaseId leaseId = 0;
        const CSMap::Entry*  cs      = 0;
        servers.clear();
        if ((recoveryFlag && ! req.fromChunkServerFlag) ||
                ! IsChunkStable(chunkId)) {
            leaseId = -EBUSY;
        } else if (! (cs = mChunkToServerMap.Find(chunkId)) ||
                metatree.getChunkDeleteQueue() == cs->GetFattr()) {
            leaseId = -EINVAL;
        } else if (mVerifyAllOpsPermissionsFlag &&
                ((0 < req.leaseTimeout &&
                ! cs->GetFattr()->CanRead(req.euser, req.egroup)) ||
                (req.flushFlag &&
                ! cs->GetFattr()->CanWrite(req.euser, req.egroup)))) {
            leaseId = -EACCES;
        } else if (mChunkToServerMap.GetServers(*cs, servers) <= 0) {
            // Cannot obtain lease if no replicas exist.
            leaseId = -EAGAIN;
        } else if ((req.leaseTimeout <= 0 ?
                mChunkLeases.HasWriteLease(ChunkLeases::EntryKey(chunkId)) :
                ! mChunkLeases.NewReadLease(
                    cs->GetFileId(),
                    ChunkLeases::EntryKey(chunkId),
                    TimeNow() + req.leaseTimeout,
                    leaseId))) {
            leaseId = -EBUSY;
            if (req.flushFlag) {
                mChunkLeases.FlushWriteLease(ChunkLeases::EntryKey(chunkId),
                    mARAChunkCache, mChunkToServerMap);
            }
        } else if (0 < req.leaseTimeout) {
            UpdateATimeSelf(mATimeUpdateResolution, cs->GetFattr(), req);
        }
        writer.Write(" ", 1);
        if (req.getChunkLocationsFlag) {
            writer.WriteHexInt(leaseId);
            writer.Write(" ", 1);
            writer.WriteHexInt(servers.size());
            for (Servers::const_iterator it = servers.begin();
                    it != servers.end();
                    ++it) {
                const ServerLocation& loc = (*it)->GetServerLocation();
                if (loc.IsValid() &&
                        (! emitCAFlag || (*it)->IsCryptoKeyValid())) {
                    writer.Write(" ", 1);
                    writer.Write(loc.hostname);
                    writer.Write(" ", 1);
                    writer.WriteHexInt(loc.port);
                    if (emitCAFlag) {
                        MetaLeaseAcquire::ChunkAccessInfo info(
                            loc, chunkId, req.authUid);
                        if ((*it)->GetCryptoKey(info.keyId, info.key)) {
                            req.chunkAccess.Append(info);
                        }
                    }
                } else {
                    writer.Write(" ? -1", 5);
                }
            }
        } else {
            writer.WriteInt(leaseId);
        }
    }
    if (0 == req.status) {
        writer.Close();
    } else {
        writer.Clear();
    }
}

void
LayoutManager::MakeChunkAccess(
    const CSMap::Entry&            cs,
    kfsUid_t                       authUid,
    MetaLeaseAcquire::ChunkAccess& chunkAccess,
    const ChunkServer*             writeMaster)
{
    StTmp<Servers> serversTmp(mServers3Tmp);
    Servers&       servers = serversTmp.Get();
    mChunkToServerMap.GetServers(cs, servers);
    if (writeMaster && (servers.empty() ||
            writeMaster != &*servers.front())) {
        KFS_LOG_STREAM_ERROR <<
            (servers.empty() ? "empty" : "") <<
            "invalid replication chain with write lease renew"
            " possibly due missing lease invalidation" <<
        KFS_LOG_EOM;
        chunkAccess.Clear();
        return;
    }
    MakeChunkAccess(
        cs.GetChunkId(), servers, authUid, chunkAccess, writeMaster);
}

void
LayoutManager::MakeChunkAccess(
    chunkId_t                      chunkId,
    const LayoutManager::Servers&  servers,
    kfsUid_t                       authUid,
    MetaLeaseAcquire::ChunkAccess& chunkAccess,
    const ChunkServer*             writeMaster)
{
    MetaLeaseAcquire::ChunkAccessInfo info(ServerLocation(), chunkId, authUid);
    for (Servers::const_iterator it = servers.begin(); it != servers.end(); ) {
        if (writeMaster) {
            info.authUid = (*it)->GetAuthUid();
            if (++it == servers.end()) {
                break;
            }
        } else {
            // Location is implicit for write leases.
            info.serverLocation = (*it)->GetServerLocation();
        }
        if ((writeMaster || info.serverLocation.IsValid()) &&
                (*it)->GetCryptoKey(info.keyId, info.key)) {
            chunkAccess.Append(info);
        }
        if (! writeMaster) {
            ++it;
        }
    }
}

/*
 * \brief Process a reqeuest for a READ lease.
*/
void
LayoutManager::Handle(MetaLeaseAcquire& req)
{
    if (req.suspended || req.next) {
        panic("invalid read lease request");
        req.status = -EFAULT;
        return;
    }
    req.chunkAccess.Clear();
    if (LEASE_INTERVAL_SECS < req.leaseTimeout) {
        req.leaseTimeout = LEASE_INTERVAL_SECS;
    }
    req.clientCSAllowClearTextFlag = mClientCSAuthRequiredFlag &&
        mClientCSAllowClearTextFlag;
    if (mClientCSAuthRequiredFlag) {
        req.issuedTime   = TimeNow();
        req.validForTime = mCSAccessValidForTimeSec;
    }
    if (0 <= req.chunkPos) {
        if (req.chunkId < 0) {
            req.statusMsg = "invalid file id";
            req.status    = -EINVAL;
            return;
        }
        const MetaFattr* const fa = metatree.getFattr(req.chunkId);
        if (! fa) {
            req.statusMsg = "no such file";
            req.status    = -ENOENT;
            return;
        }
        if (KFS_FILE != fa->type) {
            req.statusMsg = "not a file";
            req.status    = -EISDIR;
            return;
        }
        if (fa->IsSymLink()) {
            req.statusMsg = "symbolic link";
            req.status    = -ENXIO;
            return;
        }
        if (0 != fa->numReplicas) {
            req.statusMsg = "not an object store file";
            req.status    = -EINVAL;
            return;
        }
        if (req.appendRecoveryFlag) {
            req.statusMsg = "append is not supported with object store file";
            req.status    = -EINVAL;
            return;
        }
        if (mClientCSAuthRequiredFlag && req.authUid != kKfsUserNone) {
            StTmp<Servers> serversTmp(mServers3Tmp);
            Servers&       servers = serversTmp.Get();
            if (req.chunkServerName.empty()) {
                GetAccessProxy(req, servers);
            } else if (! GetAccessProxyFromReq(req, servers)) {
                if (0 == req.status) {
                    req.status = -EINVAL;
                }
                return;
            }
            if (servers.empty()) {
                req.statusMsg =
                    "no access proxy available on " + req.clientIp;
                req.status    = -EAGAIN;
                return;
            }
            MakeChunkAccess(
                req.chunkId, servers, req.authUid, req.chunkAccess, 0);
            if (req.chunkAccess.IsEmpty()) {
                req.statusMsg = "no access proxy key available";
                req.status    = -EAGAIN;
                return;
            }
        }
        if (mChunkLeases.NewReadLease(
                fa->id(),
                ChunkLeases::EntryKey(req.chunkId, req.chunkPos),
                TimeNow() + req.leaseTimeout,
                req.leaseId)) {
            UpdateATimeSelf(mATimeUpdateResolution, fa, req);
        } else {
            req.statusMsg = "write lease exists";
            req.status    = -EBUSY;
            return;
        }
        return;
    }
    GetChunkReadLeases(req);
    if (0 != req.status || req.chunkId < 0) {
        return;
    }
    if ((! req.fromChunkServerFlag && ! req.appendRecoveryFlag &&
            ! req.replayFlag) && InRecovery()) {
        req.statusMsg = "recovery is in progress";
        req.status    = -EBUSY;
        KFS_LOG_STREAM_INFO << "chunk " << req.chunkId <<
            " " << req.statusMsg << " => EBUSY" <<
        KFS_LOG_EOM;
        return;
    }
    const CSMap::Entry* const cs = mChunkToServerMap.Find(req.chunkId);
    if ((! cs || ! mChunkToServerMap.HasServers(*cs)) &&
            (req.fromChunkServerFlag || ! req.appendRecoveryFlag ||
                req.appendRecoveryLocations.empty())) {
        req.statusMsg = cs ? "no replica available" : "no such chunk";
        req.status    = cs ? -EAGAIN : -EINVAL;
        return;
    }
    if (metatree.getChunkDeleteQueue() == cs->GetFattr()) {
        req.statusMsg = "no such chunk";
        req.status    = -EINVAL;
        return;
    }
    if (req.fromChunkServerFlag) {
        if (mClientCSAuthRequiredFlag && mFileRecoveryInFlightCount.find(
                make_pair(req.authUid, cs->GetFileId())) ==
                mFileRecoveryInFlightCount.end()) {
            req.statusMsg = "no chunk recovery is in flight for this file";
            req.status    = -EACCES;
            return;
        }
    } else if (mVerifyAllOpsPermissionsFlag && cs &&
            ((0 <= req.leaseTimeout && ! req.appendRecoveryFlag &&
                ! cs->GetFattr()->CanRead(req.euser, req.egroup)) ||
            ((req.flushFlag || req.appendRecoveryFlag) &&
                ! cs->GetFattr()->CanWrite(req.euser, req.egroup)))) {
        req.status = -EACCES;
        return;
    }
    if (req.appendRecoveryFlag) {
        if (! mClientCSAuthRequiredFlag || req.authUid == kKfsUserNone) {
            return;
        }
        // Leases are irrelevant, the client just needs to talk to the write
        // slaves to recover the its last append rpc status.
        // To avoid lease lookup, and to handle the case where no write lease
        // exists return access tokens to all servers. It is possible that one
        // of the server is or was the write master -- without the corresponding
        // write lease, or the client explicitly telling this, it is not
        // possible to determine which one was the master.
        if (req.appendRecoveryLocations.empty()) {
            assert(cs);
            MakeChunkAccess(*cs, req.authUid, req.chunkAccess, 0);
            if (req.chunkAccess.IsEmpty()) {
                req.statusMsg = "no chunk server keys available";
                req.status    = -EAGAIN;
                return;
            }
        } else {
            // Ensure that the client isn't trying to get recovery for in flight
            // allocations.
            // Chunk might already be deleted. The chunk server keeps append
            // status information after the chunk deletion.
            // Give the client access to the chunk server it requests with
            // the chunk access tokens that only permit append status recovery.
            if (! cs) {
                if (chunkID.getseed() < req.chunkId) {
                    req.statusMsg = "invalid chunk id";
                    req.status    = -EINVAL;
                    return;
                }
                if (0 < GetInFlightChunkOpsCount(
                        req.chunkId, META_CHUNK_ALLOCATE)) {
                    req.statusMsg = "chunk allocation in flight";
                    req.status    = -EINVAL;
                    return;
                }
            }
            MetaLeaseAcquire::ChunkAccessInfo info(
                ServerLocation(), req.chunkId, req.authUid);
            req.chunkAccess.Clear();
            const char*       ptr = req.appendRecoveryLocations.data();
            const char* const end = ptr + req.appendRecoveryLocations.size();
            while (info.serverLocation.ParseString(
                    ptr, end - ptr, req.shortRpcFormatFlag)) {
                Servers::const_iterator const it =
                    FindServer(info.serverLocation);
                if (it != mChunkServers.end()) {
                    if ((*it)->GetCryptoKey(info.keyId, info.key)) {
                        req.chunkAccess.Append(info);
                    }
                }
            }
            if (req.chunkAccess.IsEmpty()) {
                // For now retry even in the case of parse errors.
                req.statusMsg = "no chunk servers available";
                req.status    = -EAGAIN;
                return;
            }
        }
        return;
    }
    //
    // Even if there is no write lease, wait until the chunk is stable
    // before the client can read the data.  We could optimize by letting
    // the client read from servers where the data is stable, but that
    // requires more book-keeping; so, we'll defer for now.
    //
    MakeChunkStableInfo* const it = mNonStableChunks.Find(req.chunkId);
    if (it) {
        if (req.fromChunkServerFlag || req.leaseTimeout <= 0) {
            req.statusMsg = "not yet stable";
            req.status    = -EBUSY;
            return;
        }
        req.suspended = true;
        if (it->pendingReqRingTail) {
            req.next = it->pendingReqRingTail->next;
            it->pendingReqRingTail->next = &req;
        } else {
            req.next = &req;
        }
        it->pendingReqRingTail = &req;
        KFS_LOG_STREAM_INFO << "chunk: " << req.chunkId <<
            " " << "not yet stable suspending read lease acquire request" <<
        KFS_LOG_EOM;
        return;
    }
    if ((req.leaseTimeout <= 0 ?
            ! mChunkLeases.HasWriteLease(ChunkLeases::EntryKey(req.chunkId)) :
            mChunkLeases.NewReadLease(
                cs->GetFileId(),
                ChunkLeases::EntryKey(req.chunkId),
                TimeNow() + req.leaseTimeout,
                req.leaseId))) {
        if (mClientCSAuthRequiredFlag && req.authUid != kKfsUserNone) {
            MakeChunkAccess(*cs, req.authUid, req.chunkAccess, 0);
            if (req.chunkAccess.IsEmpty()) {
                req.statusMsg = "no chunk server keys available";
                req.status    = -EAGAIN;
            }
        }
        if (0 < req.leaseTimeout && 0 == req.status) {
            UpdateATimeSelf(mATimeUpdateResolution, cs->GetFattr(), req);
        }
        return;
    }
    req.statusMsg = "has write lease";
    if (req.flushFlag) {
        const char* const errMsg = mChunkLeases.FlushWriteLease(
             ChunkLeases::EntryKey(req.chunkId),
             mARAChunkCache, mChunkToServerMap);
        req.statusMsg += "; ";
        req.statusMsg += errMsg ? errMsg :
            "initiated write lease relinquish";
    }
    req.status = -EBUSY;
    KFS_LOG_STREAM_INFO << "Chunk " << req.chunkId <<
        " " << req.statusMsg << " => EBUSY" <<
    KFS_LOG_EOM;
}

class ValidLeaseIssued
{
    const ChunkLeases& leases;
public:
    ValidLeaseIssued(const ChunkLeases& cl)
        : leases(cl) {}
    bool operator() (MetaChunkInfo *c) const {
        return leases.HasValidLease(ChunkLeases::EntryKey(c->chunkId));
    }
};

void
LayoutManager::ScheduleDumpsterCleanup(
    const MetaFattr& fa,
    const string&    name)
{
    mChunkLeases.ScheduleDumpsterCleanup(fa, name);
}

void
LayoutManager::Handle(MetaRemoveFromDumpster& op)
{
    const int cnt = mChunkLeases.Handle(
        op, mPrimaryFlag ? mMaxDumpsterCleanupInFlight : 0);
    if (0 <= cnt && mPrimaryFlag && cnt < mMaxDumpsterCleanupInFlight) {
        mLeaseCleaner.ScheduleNext();
    }
}

void
LayoutManager::Handle(MetaLeaseRenew& req)
{
    const CSMap::Entry* const cs = 0 <= req.chunkPos ? 0 :
        mChunkToServerMap.Find(req.chunkId);
    const MetaFattr* fa;
    if (0 <= req.chunkPos) {
        if (! (fa = metatree.getFattr(req.chunkId))) {
            req.statusMsg = "no such file";
            req.status    = -ENOENT;
            return;
        }
        if (KFS_FILE != fa->type) {
            req.statusMsg = "not a file";
            req.status    = -EISDIR;
            return;
        }
        if (fa->IsSymLink()) {
            req.statusMsg = "symbolic link";
            req.status    = -ENXIO;
            return;
        }
        if (0 != fa->numReplicas) {
            req.statusMsg = "not an object store file";
            req.status    = -EINVAL;
            return;
        }
    } else {
        if (! cs) {
            req.status = -EINVAL;
            return;
        }
        fa = cs->GetFattr();
    }
    const bool readLeaseFlag = mChunkLeases.IsReadLease(req.leaseId);
    if (readLeaseFlag != (req.leaseType == READ_LEASE)) {
        req.statusMsg = "invalid lease type";
        req.status    = -EINVAL;
        return;
    }
    if (! readLeaseFlag && (req.fromClientSMFlag || ! req.chunkServer)) {
        req.statusMsg = "only chunk servers are allowed to renew write leases";
        req.status    = -EPERM;
        return;
    }
    if (mVerifyAllOpsPermissionsFlag && readLeaseFlag &&
                ! fa->CanRead(req.euser, req.egroup)) {
        req.statusMsg = "access denied";
        req.status    = -EACCES;
        return;
    }
    ChunkLeases::EntryKey const key(req.chunkId, req.chunkPos);
    const bool                  kAllocDoneFlag = false;
    const int                   ret            = mChunkLeases.Renew(
        fa->id(),
        key,
        req.leaseId,
        kAllocDoneFlag,
        (mVerifyAllOpsPermissionsFlag && ! readLeaseFlag) ? fa : 0,
        mClientCSAuthRequiredFlag ? &req : 0
    );
    if (ret == 0 && mClientCSAuthRequiredFlag && req.authUid != kKfsUserNone &&
            (! readLeaseFlag || cs || 0 <= req.chunkPos)) {
        req.issuedTime                 = TimeNow();
        req.clientCSAllowClearTextFlag = mClientCSAllowClearTextFlag;
        if (req.emitCSAccessFlag) {
            req.validForTime = mCSAccessValidForTimeSec;
        }
        req.tokenSeq = (MetaAllocate::TokenSeq)mRandom.Rand();
        if (cs) {
            MakeChunkAccess(
                *cs, req.authUid, req.chunkAccess, req.chunkServer);
        } else if (! req.chunkServer) {
            StTmp<Servers> serversTmp(mServers3Tmp);
            Servers&       servers = serversTmp.Get();
            if (req.chunkServerName.empty()) {
                GetAccessProxy(req, servers);
            } else if (! GetAccessProxyFromReq(req, servers)) {
                if (0 == req.status) {
                    req.status = -EINVAL;
                }
                return;
            }
            MakeChunkAccess(
                req.chunkId, servers, req.authUid, req.chunkAccess, 0);
        }
    }
    req.status = ret;
    if (0 == ret && req.leaseType == READ_LEASE) {
        UpdateATimeSelf(mATimeUpdateResolution, fa, req);
    }
}

///
/// Handling a corrupted chunk involves removing the mapping
/// from chunk id->chunkserver that we know has it.
///
void
LayoutManager::Handle(MetaChunkCorrupt& req)
{
    if (HandleReplay(req) || 0 != req.status) {
        return;
    }
    const char* p = req.chunkIdsStr.GetPtr();
    const char* e = p + req.chunkIdsStr.GetSize();
    for (int i = -1; i < 0 || i < req.chunkCount; i++) {
        chunkId_t chunkId = i < 0 ? req.chunkId : chunkId_t(-1);
        if (i < 0) {
            if (chunkId < 0) {
                continue;
            }
        } else if (! req.ParseInt(p, e - p, chunkId)) {
            req.status    = -EINVAL;
            req.statusMsg = "chunk id list parse error";
            KFS_LOG_STREAM_ERROR <<  req.Show() << " : " <<
                req.statusMsg <<
            KFS_LOG_EOM;
            break;
        }
        if (! req.isChunkLost) {
            req.server->IncCorruptChunks();
        }
        KFS_LOG_STREAM(req.replayFlag ?
                MsgLogger::kLogLevelDEBUG :
                MsgLogger::kLogLevelINFO) <<
            "server " << req.server->GetServerLocation() <<
            " claims chunk: <" <<
            req.fid << "," << chunkId <<
            "> to be " << (req.isChunkLost ? "lost" : "corrupt") <<
        KFS_LOG_EOM;
        const bool kNotifyStaleFlag = false;
        ChunkCorrupt(chunkId, req.server, kNotifyStaleFlag);
    }
}

void
LayoutManager::ChunkCorrupt(chunkId_t chunkId, const ChunkServerPtr& server,
        bool notifyStale /* = true */)
{
    CSMap::Entry* const ci = mChunkToServerMap.Find(chunkId);
    if (! ci) {
        if (notifyStale && ! server->IsDown()) {
            server->ForceDeleteChunk(chunkId);
        }
        return;
    }
    const bool existedFlag = notifyStale ?
        ci->HasServer(mChunkToServerMap, server) :
        ci->Remove(mChunkToServerMap, server);
    mChunkLeases.ReplicaLost(chunkId, &*server);
    mARAChunkCache.Invalidate(ci->GetFileId(), chunkId);
    if (existedFlag) {
        // check the replication state when the replicaiton checker gets to it
        CheckReplication(*ci);
    }
    const MetaFattr* const fa = ci->GetFattr();
    KFS_LOG_STREAM(server->IsReplay() ?
            MsgLogger::kLogLevelDEBUG :
            MsgLogger::kLogLevelINFO) <<
        "server " << server->GetServerLocation() <<
        " declaring: <" <<
        ci->GetFileId() << "," << chunkId <<
        "> lost" <<
        " servers: " << mChunkToServerMap.ServerCount(*ci) <<
        (existedFlag ? " -1" : " -0") <<
        " notify: "      << notifyStale <<
        " down: "        << server->IsDown() <<
        " replication: " << fa->numReplicas <<
        " recovery: "    << fa->numRecoveryStripes <<
        " down: "        << server->IsDown() <<
    KFS_LOG_EOM;
    if (! notifyStale || server->IsDown()) {
        return;
    }
    server->NotifyStaleChunk(chunkId);
}

void
LayoutManager::Handle(MetaChunkEvacuate& req)
{
    if (! req.server->IsConnected() ||
            ! mChunkToServerMap.Validate(req.server)) {
        req.status    = -EAGAIN;
        req.statusMsg = "stale request ignored";
        return;
    }
    if (req.server->IsHibernatingOrRetiring()) {
        req.status    = -EINVAL;
        req.statusMsg = "chunk server is scheduled to hibernate or retire";
        return;
    }
    req.server->UpdateSpace(req);
    MetaChunkRequest::ChunkIdSet deletedChunks;
    MetaChunkRequest::ChunkIdSet evacuatedChunks;
    const MetaAllocate*          alloc = 0;
    const char*                  p     = req.chunkIds.GetPtr();
    const char*                  e     = p + req.chunkIds.GetSize();
    while (p < e) {
        chunkId_t chunkId = -1;
        if (! req.ParseInt(p, e - p, chunkId)) {
            while (p < e && *p <= ' ') {
                p++;
            }
            if (p != e) {
                req.status    = -EINVAL;
                req.statusMsg = "chunk id list parse error";
                KFS_LOG_STREAM_ERROR <<  req.Show() << " : " <<
                    req.statusMsg <<
                KFS_LOG_EOM;
            }
            break;
        }
        CSMap::Entry* const ci = mChunkToServerMap.Find(chunkId);
        if (! ci) {
            const ChunkLeases::WriteLease* const lease =
                mChunkLeases.GetChunkWriteLease(chunkId);
            if (! lease || ! (alloc = lease->allocInFlight) ||
                    find(alloc->servers.begin(),
                        alloc->servers.end(),
                        req.server) ==
                        alloc->servers.end()) {
                deletedChunks.Insert(chunkId);
                alloc = 0;
                continue;
            }
        } else if (! ci->HasServer(mChunkToServerMap, req.server)) {
            evacuatedChunks.Insert(chunkId);
            continue;
        }
        const int status = req.server->Evacuate(chunkId);
        if (-EEXIST == status) {
            continue; // Already scheduled.
        }
        if (status != 0) {
            req.status = status;
            if (status == -EAGAIN) {
                req.statusMsg = "exceeded evacuate queue limit";
            }
            break;
        }
        if (! ci) {
            assert(alloc);
            alloc = 0;
            continue;
        }
        CheckReplication(*ci);
    }
    if (! deletedChunks.IsEmpty()) {
        req.server->NotifyStaleChunks(deletedChunks);
    }
    if (! evacuatedChunks.IsEmpty()) {
        const bool kEvacuatedFlag = true;
        req.server->NotifyStaleChunks(evacuatedChunks, kEvacuatedFlag);
    }
}

// Chunk available used with partial hello, where chunk server hello has only
// non stable chunks inventory, and all the stable chunk inventory is
// communicated with chunk available, and in the case if previously unavailable
// chunk directory becomes available again.
// Chunk replicas became available again: the disk / directory came back.
// Use these replicas only if absolutely must -- no other replicas exists,
// and / or the chunk replica cannot be recovered, or there is only one replica
// left, etc.
// If disk / chunk directory goes off line it is better not not use it as it is
// unreliable, except for the case where there is some kind of "DoS attack"
// affecting multiple nodes at the same time.

void
LayoutManager::Start(MetaChunkAvailable& req)
{
    if (0 != req.status) {
        return;
    }
    req.useThreshold = req.helloFlag ? -1 :
        mChunkAvailableUseReplicationOrRecoveryThreshold;
}

void
LayoutManager::Handle(MetaChunkAvailable& req)
{
    if (HandleReplay(req) || 0 != req.status || req.server->IsDown()) {
        return;
    }
    vector<MetaChunkInfo*> cblk;
    MetaChunkRequest::ChunkIdSet staleChunks;
    const ServerLocation&        loc = req.server->GetServerLocation();
    const char*                  p   = req.chunkIdAndVers.GetPtr();
    const char*                  e   = p + req.chunkIdAndVers.GetSize();
    int                          cnt = 0;
    while (p < e) {
        chunkId_t chunkId        = -1;
        seq_t     chunkVersion   = -1;
        bool      gotVersionFlag = true;
        if (! HexIntParser::Parse(p, e - p, chunkId) ||
                ! (gotVersionFlag =
                    HexIntParser::Parse(p, e - p, chunkVersion))) {
            while (p < e && *p <= ' ') {
                p++;
            }
            if (! gotVersionFlag || p != e ||
                    (0 <= req.numChunks && cnt != req.numChunks)) {
                req.status    = -EINVAL;
                req.statusMsg = "chunk id list parse error";
                KFS_LOG_STREAM_ERROR << req.Show() << " : " <<
                    req.statusMsg <<
                KFS_LOG_EOM;
            }
            break;
        }
        cnt++;
        CSMap::Entry* const cmi = mChunkToServerMap.Find(chunkId);
        if (! cmi) {
            KFS_LOG_STREAM_DEBUG <<
                loc <<
                " available chunk: " << chunkId <<
                " version: "         << chunkVersion <<
                " hello: "           << req.helloFlag <<
                " does not exist" <<
            KFS_LOG_EOM;
            staleChunks.Insert(chunkId);
            continue;
        }
        const MetaChunkInfo& ci = *(cmi->GetChunkInfo());
        if (cmi->HasServer(mChunkToServerMap, req.server)) {
            KFS_LOG_STREAM_ERROR <<
                loc <<
                " available chunk: " << chunkId <<
                " version: "         << chunkVersion <<
                " hosted version: "  << ci.chunkVersion <<
                " replica is already hosted"
                " hello: "           << req.helloFlag <<
            KFS_LOG_EOM;
            // This is likely the result of the replication or recovery, or
            // previous chunk available rpc that the chunk server considered
            // timed out, and retried.
            continue;
        }
        if (ci.chunkVersion != chunkVersion &&
                (chunkVersion < ci.chunkVersion ||
                    ci.chunkVersion + GetChunkVersionRollBack(chunkId) <
                    chunkVersion)) {
            KFS_LOG_STREAM_DEBUG <<
                loc <<
                " available chunk: "    << chunkId <<
                " version: "            << chunkVersion <<
                " mismatch, expected: " << ci.chunkVersion <<
                " hello: "              << req.helloFlag <<
            KFS_LOG_EOM;
            staleChunks.Insert(chunkId);
            continue;
        }
        // Add chunk replicas in replay, as leases don't exist in replay, and
        // chunk replicas might be off due to lag of the updates from the
        // primary. Rely on the primary to delete chunks, if necessary, by
        // issuing stale chunks notification. Stale chunks notification replayed
        // prior to being issued to the chunk server, as it required to be
        // logged successfully before it can be issued to the chunk server. In
        // the case of the log failure or chunk disconnect the chunk server will
        // not receive chunk available completion, and therefore will mark the
        // corresponding chunks as in flight, and must add these to hello
        // resume.
        if (! req.replayFlag) {
            if (IsAllocationInFlight(chunkId)) {
                KFS_LOG_STREAM_DEBUG <<
                    loc <<
                    " available chunk: "  << chunkId <<
                    " version: "          << chunkVersion <<
                    " hello: "            << req.helloFlag <<
                    " write lease exists" <<
                KFS_LOG_EOM;
                staleChunks.Insert(chunkId);
                continue;
            }
            if (! IsChunkStable(chunkId)) {
                KFS_LOG_STREAM_INFO <<
                    loc <<
                    " available chunk: " << chunkId <<
                    " version: "         << chunkVersion <<
                    " hello: "           << req.helloFlag <<
                    " not stable" <<
                KFS_LOG_EOM;
                // Available chunks are always stable. If the version matches
                // then it is likely that the replica became stable but make
                // stable reply was "lost" due chunk server disconnect or
                // chunk directory transition into "lost" state.
            }
            const MetaFattr& fa     = *(cmi->GetFattr());
            const size_t     srvCnt = mChunkToServerMap.ServerCount(*cmi);
            if (! req.replayFlag && 0 < srvCnt && (fa.numReplicas <= srvCnt ||
                    (0 <= req.useThreshold &&
                        fa.numReplicas <= srvCnt + req.useThreshold))) {
                KFS_LOG_STREAM_DEBUG <<
                    loc <<
                    " available chunk: "     << chunkId <<
                    " version: "             << chunkVersion <<
                    " hello: "               << req.helloFlag <<
                    " sufficient replicas: " << srvCnt <<
                    " replay: "              << req.replayFlag <<
                KFS_LOG_EOM;
                staleChunks.Insert(chunkId);
                continue;
            }
            bool incompleteChunkBlockFlag              = false;
            bool incompleteChunkBlockWriteHasLeaseFlag = false;
            int  goodCnt                               = 0;
            if (0 <= req.useThreshold &&
                    0 < fa.numRecoveryStripes &&
                    CanBeRecovered(
                        *cmi,
                        incompleteChunkBlockFlag,
                        &incompleteChunkBlockWriteHasLeaseFlag,
                        cblk,
                        &goodCnt) &&
                    (int)fa.numStripes + req.useThreshold <= goodCnt) {
                KFS_LOG_STREAM_DEBUG <<
                    loc <<
                    " available chunk: "  << chunkId <<
                    " version: "          << chunkVersion <<
                    " hello: "            << req.helloFlag <<
                    " can be recovered: "
                    " good: "             << goodCnt <<
                    " data stripes: "     << fa.numStripes <<
                KFS_LOG_EOM;
                staleChunks.Insert(chunkId);
                continue;
            }
            if (incompleteChunkBlockWriteHasLeaseFlag) {
                KFS_LOG_STREAM_DEBUG <<
                    loc <<
                    " available chunk: "  << chunkId <<
                    " version: "          << chunkVersion <<
                    " hello: "            << req.helloFlag <<
                    " partial chunk block has write lease" <<
                KFS_LOG_EOM;
                staleChunks.Insert(chunkId);
                continue;
            }
        }
        if (ci.chunkVersion != chunkVersion) {
            KFS_LOG_STREAM_DEBUG <<
                loc <<
                " available chunk: " << chunkId <<
                " version: "         << chunkVersion <<
                " hello: "           << req.helloFlag <<
                " change version "   << ci.chunkVersion <<
            KFS_LOG_EOM;
            bool kMakeStableFlag = false;
            bool kPendingAddFlag = true;
            req.server->NotifyChunkVersChange(
                cmi->GetFattr()->id(),
                chunkId,
                ci.chunkVersion, // to
                chunkVersion,    // from
                kMakeStableFlag,
                kPendingAddFlag
            );
            continue;
        }
        const bool addedFlag = AddServerWithStableReplica(*cmi, req.server);
        KFS_LOG_STREAM_DEBUG <<
            loc <<
            " available chunk: " << chunkId <<
            " version: "         << chunkVersion <<
            " hello: "           << req.helloFlag <<
            " added "            << addedFlag <<
        KFS_LOG_EOM;
    }
    // Chunk's server logic requires stale chunk's RPC arrival prior to chunk
    // available RPC response. The order and chunk available RPC sequence
    // number is used by chunk server to disambiguate between "regular" stale
    // chunk notifications, and stale chunk notifications issued as a result
    // of processing of chunk available notification.
    if (! staleChunks.IsEmpty()) {
        req.server->NotifyStaleChunks(staleChunks, req);
    }
    if (req.helloFlag) {
        req.server->SetPendingHelloNotify(! req.endOfNotifyFlag);
    }
}

void
CSMap::Entry::destroySelf()
{
    gLayoutManager.DeleteChunk(*this);
}

void
LayoutManager::DeleteChunk(CSMap::Entry& entry)
{
    if (mChunkEntryToChange == &entry) {
        // The entry is deleted from the b+tree, it should be inserted
        // back shortly with different file attribute.
        MetaFattr* const fa = mFattrToChangeTo;
        if (! fa) {
            panic("chunk change file id: invalid null file attribute");
            return;
        }
        const bool deleteQueueFlag = metatree.getChunkDeleteQueue() == fa;
        const bool checkReplicationFlag = ! deleteQueueFlag && (
            ! entry.GetFattr() ||
            fa->numReplicas != entry.GetFattr()->numReplicas);
        mChunkEntryToChange = 0;
        mFattrToChangeTo    = 0;
        if (deleteQueueFlag) {
            StTmp<Servers>  serversTmp(mServers3Tmp);
            Servers&        servers = serversTmp.Get();
            const bool kStaleChunkIdFlag = false;
            DeleteChunk(entry.GetFileId(), entry.GetChunkId(), servers,
                kStaleChunkIdFlag);
            SetReplicationState(entry, CSMap::Entry::kStateNone);
        } else {
            mChunkLeases.ChangeFileId(
                entry.GetChunkId(), entry.GetFileId(), fa->id());
        }
        entry.SetFattr(fa);
        if (checkReplicationFlag) {
            CheckReplication(entry);
        }
        return;
    }

    const fid_t     fid     = entry.GetFileId();
    const chunkId_t chunkId = entry.GetChunkInfo()->chunkId;
    StTmp<Servers>  serversTmp(mServers3Tmp);
    Servers&        servers = serversTmp.Get();
    mChunkToServerMap.GetServers(entry, servers);
    // remove the mapping
    KFS_LOG_STREAM_DEBUG <<
        "delete chunk: <" << fid << "," << chunkId << ">"
        " servers: " << MetaRequest::InsertServers(servers) <<
    KFS_LOG_EOM;
    mChunkToServerMap.Erase(chunkId);
    const bool kStaleChunkIdFlag = true;
    DeleteChunk(fid, chunkId, servers, kStaleChunkIdFlag);
}

void
LayoutManager::DeleteChunk(fid_t fid, chunkId_t chunkId,
    const LayoutManager::Servers& servers, bool staleChunkIdFlag /* = false */)
{
    // Make a copy to deal with possible recursion.
    Servers const cs(servers);

    for (StripedFilesAllocationsInFlight::iterator it =
            mStripedFilesAllocationsInFlight.lower_bound(
                make_pair(make_pair(fid, 0), 0));
            it != mStripedFilesAllocationsInFlight.end() &&
            it->first.first == fid;
            ++it) {
        if (it->second == chunkId) {
            mStripedFilesAllocationsInFlight.erase(it);
            break;
        }
    }
    mARAChunkCache.Invalidate(fid, chunkId);
    mPendingBeginMakeStable.Erase(chunkId);
    mPendingMakeStable.Erase(chunkId);
    mChunkLeases.Delete(fid, ChunkLeases::EntryKey(chunkId));
    mChunkVersionRollBack.Erase(chunkId);

    // submit an RPC request
    for_each(cs.begin(), cs.end(),
        bind(&ChunkServer::DeleteChunk, _1, chunkId, staleChunkIdFlag));
}

void
LayoutManager::DeleteChunk(MetaAllocate& req)
{
    if (0 == req.numReplicas) {
        if (mChunkLeases.DeleteWriteLease(
                req.fid,
                ChunkLeases::EntryKey(req.fid, req.offset),
                req.leaseId) &&
                ! req.servers.empty() &&
                ! req.servers.front()->IsDown()) {
            const bool       kHasChunkChecksum = false;
            const bool       kPendingAddFlag   = false;
            const chunkOff_t kChunkSize        = -1;
            req.servers.front()->MakeChunkStable(
                req.fid, req.fid, -req.chunkVersion - 1,
                kChunkSize, kHasChunkChecksum, 0, kPendingAddFlag);
        }
        return;
    }
    if (mChunkToServerMap.Find(req.chunkId)) {
        if (mPrimaryFlag) {
            for (Servers::const_iterator it = req.servers.begin();
                    req.servers.end() != it;
                    ++it) {
                if (! (*it)->IsStoppedServicing()) {
                    panic("chunk allocation"
                        " attempts to delete existing chunk mapping");
                }
            }
        }
        return;
    }
    DeleteChunk(req.fid, req.chunkId, req.servers);
}

bool
LayoutManager::InvalidateAllChunkReplicas(
    fid_t fid, chunkOff_t offset, chunkId_t chunkId, seq_t& chunkVersion)
{
    CSMap::Entry* const ci = mChunkToServerMap.Find(chunkId);
    if (! ci || ci->GetFileId() != fid) {
        return false;
    }
    MetaChunkInfo* const mci = ci->GetChunkInfo();
    if (mci->offset != offset) {
        return false;
    }
    StTmp<Servers> serversTmp(mServers3Tmp);
    Servers&       c = serversTmp.Get();
    mChunkToServerMap.GetServers(*ci, c);
    ci->RemoveAllServers(mChunkToServerMap);
    mci->chunkVersion += IncrementChunkVersionRollBack(chunkId);
    chunkVersion = mci->chunkVersion;
    mARAChunkCache.Invalidate(ci->GetFileId(), chunkId);
    mPendingBeginMakeStable.Erase(chunkId);
    mPendingMakeStable.Erase(chunkId);
    mChunkLeases.Delete(fid, ChunkLeases::EntryKey(chunkId));
    mChunkVersionRollBack.Erase(chunkId);
    // Add chunks to the list of stale chunk ids in flight, to handle the case
    // of log chunk in flight log failure.
    const bool kStaleChunkIdFlag = true;
    for_each(c.begin(), c.end(),
        bind(&ChunkServer::DeleteChunk, _1, chunkId, kStaleChunkIdFlag));
    return true;
}

MetaChunkInfo*
LayoutManager::AddChunkToServerMapping(MetaFattr* fattr,
    chunkOff_t offset, chunkId_t chunkId, seq_t chunkVersion,
    bool& newEntryFlag)
{
    if (! fattr) {
        panic("AddChunkToServerMapping: fattr == null");
        return 0;
    }
    CSMap::Entry* const ret = mChunkToServerMap.Insert(fattr,
            offset, chunkId, chunkVersion, newEntryFlag);
    if (! ret) {
        panic("failed to create chunk map entry");
        return 0;
    }
    // Chunk allocation log or checkpoint entry resets chunk version roll
    // back.
    mChunkVersionRollBack.Erase(chunkId);
    return ret->GetChunkInfo();
}

bool
LayoutManager::GetChunkFileId(chunkId_t chunkId, fid_t& fileId,
    const MetaChunkInfo** chunkInfo, const MetaFattr** fa,
    LayoutManager::Servers* srvs)
{
    const CSMap::Entry* const entry = mChunkToServerMap.Find(chunkId);
    if (! entry) {
        return false;
    }
    fileId = entry->GetFileId();
    if (fa) {
        *fa = entry->GetFattr();
    }
    if (chunkInfo) {
        *chunkInfo = entry->GetChunkInfo();
    }
    if (srvs) {
        mChunkToServerMap.GetServers(*entry, *srvs);
    }
    return true;
}

int
LayoutManager::GetChunkToServerMapping(
    MetaChunkInfo&          chunkInfo,
    LayoutManager::Servers& c,
    MetaFattr*&             fa,
    bool*                   orderReplicasFlag /* = 0 */)
{
    const CSMap::Entry& entry = GetCsEntry(chunkInfo);
    fa = entry.GetFattr();
    c.clear();
    const size_t cnt = mChunkToServerMap.GetServers(entry, c);
    if (cnt <= 0) {
        return ((0 < fa->numRecoveryStripes && fa->IsStriped() &&
            InRecovery()) ? -EBUSY : -EAGAIN);
    }
    if (cnt <= 1 || ! orderReplicasFlag ||
            ! mGetAllocOrderServersByLoadFlag) {
        return 0;
    }
    // Random shuffle hosting servers, such that the servers with
    // smaller load go before the servers with larger load.
    int64_t       loadAvgSum    = 0;
    const int64_t kLoadAvgFloor = 1;
    for (Servers::const_iterator it = c.begin();
            it != c.end();
            ++it) {
        loadAvgSum += (*it)->GetLoadAvg() + kLoadAvgFloor;
    }
    *orderReplicasFlag = true;
    for (size_t i = c.size(); 2 <= i; ) {
        assert(loadAvgSum > 0);
        int64_t rnd = Rand(loadAvgSum);
        size_t  ri  = i--;
        int64_t load;
        do {
            --ri;
            load = c[ri]->GetLoadAvg() + kLoadAvgFloor;
            rnd -= load;
        } while (rnd >= 0 && ri > 0);
        iter_swap(c.begin() + i, c.begin() + ri);
        loadAvgSum -= load;
    }
    return 0;
}

int64_t
LayoutManager::GetFreeIoBufferByteCount() const
{
    // This has to be re-entrant. Racy check is OK though.
    return (
        mBufferPool ?
        (int64_t)mBufferPool->GetFreeBufferCount() *
        mBufferPool->GetBufferSize()  : int64_t(-1)
    );
}

class Pinger
{
    ostream&   os;
    const bool useFsTotalSpaceFlag;
public:
    uint64_t               totalSpace;
    uint64_t               usedSpace;
    uint64_t               freeFsSpace;
    uint64_t               goodMasters;
    uint64_t               goodSlaves;
    uint64_t               writableDrives;
    uint64_t               totalDrives;
    LayoutManager::Servers retiring;
    LayoutManager::Servers evacuating;

    Pinger(ostream& s, bool f)
        : os(s),
          useFsTotalSpaceFlag(f),
          totalSpace(0),
          usedSpace(0),
          freeFsSpace(0),
          goodMasters(0),
          goodSlaves(0),
          writableDrives(0),
          totalDrives(0),
          retiring(),
          evacuating()
        {}
    void Process(const ChunkServerPtr& c)
    {
        ChunkServer& cs = *c;
        cs.Ping(os, useFsTotalSpaceFlag);
        totalSpace  += cs.GetTotalSpace(useFsTotalSpaceFlag);
        usedSpace   += cs.GetUsedSpace();
        freeFsSpace += cs.GetFreeFsSpace();
        if (gLayoutManager.IsCandidateServer(cs)) {
            if (cs.CanBeChunkMaster()) {
                goodMasters++;
            } else {
                goodSlaves++;
            }
            writableDrives += max(0, cs.GetNumWritableDrives());
        }
        totalDrives += max(0, cs.GetNumDrives());
        if (cs.IsRetiring()) {
            retiring.push_back(c);
        } else if (cs.GetEvacuateCount() > 0) {
            evacuating.push_back(c);
        }
    }
};

inline static void
ShowTiersInfo(
    ostream&                         os,
    const RackInfo*                  rack,
    const RackInfo::StorageTierInfo* tiersInfo,
    const int*                       candidatesCnt)
{
    for (size_t i = 0; i < kKfsSTierCount; i++) {
        const RackInfo::StorageTierInfo& info = tiersInfo[i];
        if (info.GetDeviceCount() <= 0) {
            continue;
        }
        if (rack) {
            os << "\t" << rack->id();
        } else {
            os << "\tall";
        }
        os <<
            "\t" << i <<
            "\t" << info.GetDeviceCount() <<
            "\t" << info.GetNotStableOpenCount() <<
            "\t" << info.GetChunkCount() <<
            "\t" << info.GetSpaceAvailable() <<
            "\t" << info.GetTotalSpace() <<
            "\t" << setprecision(2) << fixed <<
                info.GetSpaceUtilization() * 1e2 <<
            "\t" << (candidatesCnt ? candidatesCnt[i] :
                rack->getPossibleCandidatesCount(i))
        ;
    }
}

inline static void
ShowWatchdogCounters(ostream& os, int idx, const Watchdog::Counters& cntrs)
{
    os <<
    "wd." << idx << ".name=" <<
        cntrs.mName << ";"
    "wd." << idx << ".polls=" <<
        cntrs.mPollCount << ";"
    "wd." << idx << ".timeouts=" <<
        cntrs.mTimeoutCount << ";"
    "wd." << idx << ".totalTimeouts=" <<
        cntrs.mTotalTimeoutCount << ";"
    "wd." << idx << ".changedAgoUsec=" <<
        cntrs.mLastChangedTimeAgoUsec << ";"
    ;
}

void
LayoutManager::Handle(MetaPing& inReq, bool wormModeFlag)
{
    if (IsPingResponseUpToDate()) {
        inReq.resp.Clear();
        inReq.resp.Copy(&mPingResponse, mPingResponse.BytesConsumable());
        return;
    }
    UpdateGoodCandidateLoadAvg();
    mPingResponse.Clear();
    IOBuffer tmpbuf;
    mWOstream.Set(tmpbuf);
    mWOstream <<
        "\r\n"
        "Servers: ";
    Pinger pinger(mWOstream, mUseFsTotalSpaceFlag);
    for_each(mChunkServers.begin(), mChunkServers.end(),
        bind(&Pinger::Process, ref(pinger), _1));
    mWOstream <<
        "\r\n"
        "Retiring Servers: ";
    for_each(pinger.retiring.begin(), pinger.retiring.end(),
        bind(&ChunkServer::GetRetiringStatus, _1, boost::ref(mWOstream)));
    mWOstream <<
        "\r\n"
        "Evacuating Servers: ";
    for_each(pinger.evacuating.begin(), pinger.evacuating.end(),
        bind(&ChunkServer::GetEvacuateStatus, _1, boost::ref(mWOstream)));
    mWOstream <<
        "\r\n"
        "Down Servers: ";
    copy(mDownServers.begin(), mDownServers.end(),
        ostream_iterator<string>(mWOstream));
    mWOstream <<
        "\r\n"
        "Rebalance status: ";
    mRebalanceCtrs.Show(mWOstream, "= ", "\t");
    mWOstream <<
        "\r\n"
        "Config: " << mConfig;
    mWOstream <<
        "\r\n"
        "VR Status: ";
    mWOstream.flush();
    tmpbuf.Move(&inReq.resp);
    const bool kRusageSelfFlag = true;
    mWOstream <<
        "\r\n"
        "Rusage self: ";
    showrusage(mWOstream, "= ", "\t", kRusageSelfFlag);
    mWOstream <<
        "\r\n"
        "Rusage children: ";
    showrusage(mWOstream, "= ", "\t", ! kRusageSelfFlag);
    Watchdog& watchdog = gNetDispatch.GetWatchdog();
    mWOstream <<
        "\r\n"
        "Watchdog: "
        "wd.polls=" << watchdog.GetPollCount() << ";"
        "wd.timeouts=" << watchdog.GetTimeoutCount() << ";"
        "wd.timerOverruns=" << watchdog.GetTimerOverrunCount() << ";"
        "wd.timerOverrunsUsecs=" << watchdog.GetTimerOverrunUsecCount() << ";";
    Watchdog::Counters wdCntrs;
    int                idx;
    for (idx = 0; watchdog.GetCounters(idx, wdCntrs); ++idx) {
        ShowWatchdogCounters(mWOstream, idx, wdCntrs);
    }
    mWOstream <<
        "\r\n"
        "Storage tiers info names: "
        "rack\ttier\tdevices\twr-chunks\tchunks\tspace-available"
            "\ttotal-space\t%util.\tcandidates"
        "\r\n"
        "Storage tiers info: "
    ;
    ShowTiersInfo(mWOstream, 0, mStorageTierInfo, mTierCandidatesCount);
    for (RackInfos::const_iterator it = mRacks.begin();
            it != mRacks.end();
            ++it) {
        ShowTiersInfo(mWOstream, &*it, it->getStorageTiersInfo(), 0);
    }
    mWOstream << "\r\n\r\n"; // End of headers.
    mWOstream.flush();
    // Initial headers.
    mWOstream.Set(mPingResponse);
    mPingUpdateTime = TimeNow();
    LogWriter::Counters logCtrs;
    MetaRequest::GetLogWriter().GetCounters(logCtrs);
    const MetaFattr* const fa = metatree.getFattr(ROOTFID);
    mWOstream <<
        "Build-version: "       << KFS_BUILD_VERSION_STRING << "\r\n"
        "Source-version: "      << KFS_SOURCE_REVISION_STRING << "\r\n"
        "WORM: "                << (wormModeFlag ? "1" : "0") << "\r\n"
        "System Info: "
        "Up since= "            << DisplayDateTime(kSecs2MicroSecs * mStartTime) << "\t"
        "Total space= "         << pinger.totalSpace << "\t"
        "Used space= "          << pinger.usedSpace << "\t"
        "Replications= "        << mNumOngoingReplications << "\t"
        "Replications check= "  << mChunkToServerMap.GetCount(
            CSMap::Entry::kStateCheckReplication) << "\t"
        "Pending recovery= "    << mChunkToServerMap.GetCount(
            CSMap::Entry::kStatePendingRecovery) << "\t"
        "Repl check timeouts= " << mReplicationCheckTimeouts << "\t"
        "Open Files= "          << mChunkLeases.GetFileLeasesCount() << "\t"
        "Update time= "         << DisplayDateTime(kSecs2MicroSecs * mPingUpdateTime) << "\t"
        "Uptime= "              << (mPingUpdateTime - mStartTime) << "\t"
        "Buffers= "             <<
            (mBufferPool ? mBufferPool->GetUsedBufferCount() : 0) << "\t"
        "Clients= "             << ClientSM::GetClientCount() << "\t"
        "Chunk srvs= "          << ChunkServer::GetChunkServerCount() << "\t"
        "Requests= "            << MetaRequest::GetRequestCount() << "\t"
        "Sockets= "             << globals().ctrOpenNetFds.GetValue() << "\t"
        "Chunks= "              << mChunkToServerMap.Size() << "\t"
        "Pending replication= " << mChunkToServerMap.GetCount(
            CSMap::Entry::kStatePendingReplication) << "\t"
        "Internal nodes= "      <<
            MetaNode::getPoolAllocator<Node>().GetInUseCount() << "\t"
        "Internal node size= "  <<
            MetaNode::getPoolAllocator<Node>().GetItemSize() << "\t"
        "Internal nodes storage= "  <<
            MetaNode::getPoolAllocator<Node>().GetStorageSize() << "\t"
        "Dentry nodes= "      <<
            MetaNode::getPoolAllocator<MetaDentry>().GetInUseCount() << "\t"
        "Dentry node size= "  <<
            MetaNode::getPoolAllocator<MetaDentry>().GetItemSize() << "\t"
        "Dentry nodes storage= "  <<
            MetaNode::getPoolAllocator<MetaDentry>().GetStorageSize() << "\t"
        "Fattr nodes= "      <<
            MetaNode::getPoolAllocator<MetaFattr>().GetInUseCount() << "\t"
        "Fattr node size= "  <<
            MetaNode::getPoolAllocator<MetaFattr>().GetItemSize() << "\t"
        "Fattr nodes storage= "  <<
            MetaNode::getPoolAllocator<MetaFattr>().GetStorageSize() << "\t"
        "ChunkInfo nodes= "      <<
            CSMap::Entry::GetAllocBlockCount() << "\t"
        "ChunkInfo node size= "  <<
            sizeof(MetaChunkInfo) << "\t"
        "ChunkInfo nodes storage= "  <<
            0 << "\t"
        "CSmap nodes= "  <<
            mChunkToServerMap.GetAllocator().GetInUseCount() << "\t"
        "CSmap node size= "  <<
            mChunkToServerMap.GetAllocator().GetItemSize() << "\t"
        "CSmap nodes storage= "  <<
            mChunkToServerMap.GetAllocator().GetStorageSize() << "\t"
        "CSmap entry nodes= "  <<
            CSMap::Entry::GetAllocBlockCount() << "\t"
        "CSmap entry bytes= "  <<
            CSMap::Entry::GetAllocByteCount() << "\t"
        "Delayed recovery= " << mChunkToServerMap.GetCount(
            CSMap::Entry::kStateDelayedRecovery) << "\t"
        "Replication backlog= " << mChunkToServerMap.GetCount(
            CSMap::Entry::kStateNoDestination) << "\t"
        "In recovery= " << (InRecovery() ? 1 : 0) << "\t"
        "To restart= "         << mCSToRestartCount << "\t"
        "To restart masters= " << mMastersToRestartCount << "\t" <<
        "CS Max Good Load Avg= "        <<
            mCSMaxGoodCandidateLoadAvg << "\t" <<
        "CS Max Good Master Load Avg= " <<
            mCSMaxGoodMasterCandidateLoadAvg << "\t" <<
        "CS Max Good Slave Load Avg= "   <<
            mCSMaxGoodSlaveCandidateLoadAvg << "\t" <<
        "Hibernated servers= " <<
            mChunkToServerMap.GetHibernatedCount() << "\t"
        "Free space= "        << pinger.freeFsSpace << "\t"
        "Good masters= "      << pinger.goodMasters << "\t"
        "Good slaves= "       << pinger.goodSlaves  << "\t"
        "Total drives= "      << pinger.totalDrives << "\t"
        "Writable drives= "   << pinger.writableDrives << "\t"
        "Append cache size= " << mARAChunkCache.GetSize() << "\t"
        "Max clients= "       << gNetDispatch.GetMaxClientCount() << "\t"
        "Max chunk srvs= "    << ChunkServer::GetMaxChunkServerCount() << "\t"
        "Buffers total= "     <<
            (mBufferPool ? mBufferPool->GetTotalBufferCount() : 0) << "\t"
        "Object store enabled= " << mObjectStoreEnabledFlag << "\t"
        "Object store deletes= " << mObjStoreFilesDeleteQueue.GetSize() << "\t"
        "Object store in flight deletes= " <<
            mObjBlocksDeleteInFlight.GetSize() << "\t"
        "Object store block retry deletes= " <<
            mObjBlocksDeleteRequeue.GetSize() << "\t"
        "Object store first delete time= " <<
            (mObjStoreFilesDeleteQueue.IsEmpty() ? time_t(0) :
                TimeNow() - mObjStoreFilesDeleteQueue.Front()->mTime) << "\t"
        "File count= "            << GetNumFiles() << "\t"
        "Dir count= "             << GetNumDirs() << "\t"
        "Logical Size= "          << (fa ? fa->filesize : chunkOff_t(-1)) << "\t"
        "FS ID= "                 << metatree.GetFsId() << "\t"
        "Primary= "               << mPrimaryFlag << "\t"
        "VR Node= "               << inReq.vrNodeId << "\t"
        "VR Primary= "            << inReq.vrPrimaryNodeId << "\t"
        "VR Active= "             << inReq.vrActiveFlag << "\t"
        "Log Time Usec= "         << logCtrs.mLogTimeUsec << "\t"
        "Log Time Ops Count= "    << logCtrs.mLogTimeOpsCount << "\t"
        "Log Pending Ops Count= " << logCtrs.mPendingOpsCount << "\t"
        "Log 5 Sec Avg Usec= "    << logCtrs.mLog5SecAvgUsec << "\t"
        "Log 10 Sec Avg Usec= "   << logCtrs.mLog10SecAvgUsec << "\t"
        "Log 15 Sec Avg Usec= "   << logCtrs.mLog15SecAvgUsec << "\t"
        "Log 5 Sec Avg Rate= "    << logCtrs.mLog5SecAvgReqRate << "\t"
        "Log 10 Sec Avg Rate= "   << logCtrs.mLog10SecAvgReqRate << "\t"
        "Log 15 Sec Avg Rate= "   << logCtrs.mLog15SecAvgReqRate << "\t"
        "Log Avg Rate Div="       <<
            (int64_t(1) << LogWriter::Counters::kRateFracBits) << "\t"
        "B Tree Height= "         << metatree.height() << "\t"
        "Log Disk Write Time Usec= "  << logCtrs.mDiskWriteTimeUsec << "\t"
        "Log Disk Write Byte Count= " << logCtrs.mDiskWriteByteCount << "\t"
        "Log Disk Write Count= "      << logCtrs.mDiskWriteCount << "\t"
        "Log Disk Write Op 5 sec Avg Usec= " <<
            logCtrs.mLogOpWrite5SecAvgUsec << "\t"
        "Log Disk Write Op 10 sec Avg Usec= " <<
            logCtrs.mLogOpWrite10SecAvgUsec << "\t"
        "Log Disk Write Op 15 sec Avg Usec= " <<
            logCtrs.mLogOpWrite15SecAvgUsec << "\t"
        "Log Exceeded Queue Depth Failure Count= " <<
            logCtrs.mExceedLogQueueDepthFailureCount << "\t"
        "Log Pending Ack Byte Count= " <<
            logCtrs.mPendingByteCount << "\t"
        "Log Total Request Count= " <<
            logCtrs.mTotalRequestCount << "\t"
        "Log Exceeded Queue Depth Failure Count 300 sec. Avg= " <<
            logCtrs.mExceedLogQueueDepthFailureCount300SecAvg << "\t"
        "WD Polls= "                << watchdog.GetPollCount() << "\t"
        "WD Timeouts= "             << watchdog.GetTimeoutCount() << "\t"
        "WD Timer Overruns= "       << watchdog.GetTimerOverrunCount() << "\t"
        "WD Timer Overruns Usecs= " << watchdog.GetTimerOverrunUsecCount()
    ;
    mWOstream.flush();
    mWOstream.Reset();
    mPingResponse.Move(&tmpbuf);
    inReq.resp.Copy(&mPingResponse, mPingResponse.BytesConsumable());
}

class UpServersList
{
    ostream& os;
public:
    UpServersList(ostream& s) : os(s) {}
    void operator () (const ChunkServerPtr& c) {
        if (c->IsConnected()) {
            os << c->GetServerLocation() << "\n";
        }
    }
};

void
LayoutManager::UpServers(ostream &os)
{
    for_each(mChunkServers.begin(), mChunkServers.end(), UpServersList(os));
}

// Periodically, check the replication level of ALL chunks in the system.
void
LayoutManager::InitCheckAllChunks()
{
    // HandoutChunkReplicationWork() iterates trough this list when
    // replication check is exhausted.
    mChunkToServerMap.First(CSMap::Entry::kStateNone);
    mCheckAllChunksInProgressFlag = true;
    mChunkReplicator.ScheduleNext();
}

bool
LayoutManager::ExpiredLeaseCleanup(int64_t now, chunkId_t chunkId)
{
    const int ownerDownExpireDelay = 0;
    return mChunkLeases.ExpiredCleanup(
        ChunkLeases::EntryKey(chunkId), TimeNow(), ownerDownExpireDelay,
        mARAChunkCache, mChunkToServerMap
    );
}

void
LayoutManager::LeaseCleanup(
    int64_t startTime)
{
    if (! mPrimaryFlag) {
        return;
    }
    const time_t now = (time_t)startTime;
    mChunkLeases.Timer(now, mLeaseOwnerDownExpireDelay,
        mARAChunkCache, mChunkToServerMap, mMaxDumpsterCleanupInFlight);
    if (! mWasServicingFlag) {
        mWasServicingFlag = true;
        ScheduleTruncatedChunksDelete();
    }
    if (now < mLeaseCleanerOtherNextRunTime) {
        return;
    }
    mLeaseCleanerOtherNextRunTime = now + mLeaseCleanerOtherIntervalSec;
    if (mAppendCacheCleanupInterval >= 0) {
        // Timing out the cache entries should now be redundant,
        // and is disabled by default, as the cache should not have
        // any stale entries. The lease cleanup, allocation
        // completion in the case of failure, and chunk deletion
        // should cleanup the cache.
        mARAChunkCache.Timeout(now - mAppendCacheCleanupInterval);
    }
    if (metatree.getUpdatePathSpaceUsageFlag() &&
            0 < mRecomputeDirSizesIntervalSec &&
            mLastRecomputeDirsizeTime + mRecomputeDirSizesIntervalSec < now) {
        KFS_LOG_STREAM_INFO << "Doing a recompute dir size..." <<
        KFS_LOG_EOM;
        metatree.recomputeDirSize();
        mLastRecomputeDirsizeTime = now;
        KFS_LOG_STREAM_INFO << "Recompute dir size is done..." <<
        KFS_LOG_EOM;
    }
    if (mResubmitClearObjectStoreDeleteFlag) {
        mResubmitClearObjectStoreDeleteFlag = false;
        submit_request(new MetaLogClearObjStoreDelete());
    }
    RequestQueue queue;
    queue.PushBack(mResubmitQueue);
    MetaRequest* req;
    while ((req = queue.PopFront())) {
        KFS_LOG_STREAM_DEBUG <<
            "resubmitting: " << req->Show() <<
        KFS_LOG_EOM;
        ResubmitRequest(*req);
    }
    ScheduleChunkServersRestart();
}

void
LayoutManager::ScheduleRestartChunkServers()
{
    mCSRestartTime = TimeNow();
    if (mMaxCSRestarting <= 0) {
        mMaxCSRestarting = 2;
    }
    KFS_LOG_STREAM_INFO <<
        "scheduling chunk servers restart:"
        " servers: "            << mChunkServers.size() <<
        " masters: "            << mMastersCount <<
        " restarting: "         << mCSToRestartCount <<
        " masters restarting: " << mMastersToRestartCount <<
        " max restarting: "     << mMaxCSRestarting <<
    KFS_LOG_EOM;
}

int64_t
LayoutManager::GetMaxCSUptime() const
{
    const time_t now = TimeNow();
    return (mCSRestartTime <= now ?
        min(mMaxCSUptime, now - mCSRestartTime) : mMaxCSUptime);
}

void
LayoutManager::ScheduleChunkServersRestart()
{
    if (mMaxCSRestarting <= 0 || ! IsChunkServerRestartAllowed()) {
        return;
    }
    Servers servers;
    servers.reserve(mChunkServers.size());
    for (Servers::const_iterator it = mChunkServers.begin();
            mChunkServers.end() != it;
            ++it) {
        if (! (*it)->IsReplay() || ! (*it)->IsStoppedServicing()) {
            servers.push_back(*it);
        }
    }
    make_heap(servers.begin(), servers.end(),
        bind(&ChunkServer::Uptime, _1) <
        bind(&ChunkServer::Uptime, _2));
    const int64_t maxCSUptime  = GetMaxCSUptime();
    const size_t  minMastersUp = max(size_t(1), mSlavesCount / 3 * 2);
    while (! servers.empty()) {
        ChunkServer& srv = *servers.front();
        if (srv.Uptime() < maxCSUptime) {
            break;
        }
        bool restartFlag = srv.IsRestartScheduled();
        if (! restartFlag && mCSToRestartCount < mMaxCSRestarting) {
            // Make sure that there are enough masters.
            restartFlag = ! srv.CanBeChunkMaster() ||
                mMastersCount >
                mMastersToRestartCount + minMastersUp;
            if (! restartFlag && ! mAssignMasterByIpFlag) {
                for (Servers::const_iterator
                        it = servers.begin();
                        it != servers.end();
                        ++it) {
                    ChunkServer& cs = **it;
                    if (! cs.CanBeChunkMaster() &&
                            ! cs.IsRestartScheduled() &&
                            IsCandidateServer(cs)) {
                        cs.SetCanBeChunkMaster(true);
                        srv.SetCanBeChunkMaster(false);
                        restartFlag = true;
                        break;
                    }
                }
            }
            if (restartFlag) {
                mCSToRestartCount++;
                if (srv.CanBeChunkMaster()) {
                    mMastersToRestartCount++;
                }
            }
        }
        if (restartFlag &&
                srv.ScheduleRestart(
                    mCSGracefulRestartTimeout,
                    mCSGracefulRestartAppendWithWidTimeout)) {
            KFS_LOG_STREAM_INFO <<
                "initiated restart sequence for: " <<
                servers.front()->GetServerLocation() <<
            KFS_LOG_EOM;
            break;
        }
        pop_heap(servers.begin(), servers.end(),
            bind(&ChunkServer::Uptime, _1) <
            bind(&ChunkServer::Uptime, _2));
        servers.pop_back();
    }
}

bool
LayoutManager::Validate(MetaAllocate& req)
{
    const ChunkLeases::WriteLease* const lease =
        req.numReplicas == 0 ?
            mChunkLeases.GetWriteLease(
                ChunkLeases::EntryKey(req.fid, req.offset)) :
            mChunkLeases.GetChunkWriteLease(req.chunkId);
    if (lease && &req == lease->allocInFlight &&
            lease->leaseId == req.leaseId &&
            lease->chunkVersion == req.chunkVersion) {
        return true;
    }
    KFS_LOG_STREAM_DEBUG <<
        "stale allocation: " << reinterpret_cast<const void*>(&req) <<
        " seq: "      << req.opSeqno <<
        " status: "   << req.status <<
        " msg: "      << req.statusMsg <<
        " chunk: "    << req.chunkId <<
        " version:"
        " initial: "  << req.initialChunkVersion <<
        " current: "  << req.chunkVersion <<
        " lease:"
        " id: "       << (lease ? lease->leaseId : ChunkLeases::LeaseId(-1)) <<
        " inflight: " << (lease ?
            (const void*)lease->allocInFlight : (const void*)0) <<
        " version: "  << (lease ? lease->chunkVersion : seq_t(-1)) <<
        " expires: "  << (lease ? lease->expires - TimeNow() : time_t(-1)) <<
    KFS_LOG_EOM;
    if (0 <= req.status) {
        req.status    = -EALLOCFAILED;
        req.statusMsg = lease ? "invalid write lease" : "no write lease";
    }
    return false;
}

void
LayoutManager::CommitOrRollBackChunkVersion(MetaLogChunkAllocate& req)
{
    if (0 == req.status && 0 <= req.initialChunkVersion &&
            ! req.objectStoreFileFlag && ! req.invalidateAllFlag) {
        // Schedule delete chunk replicas that were added (re-appeared), if any,
        // while chunk allocation (version change completion) was being logged.
        CSMap::Entry* const ci = mChunkToServerMap.Find(req.chunkId);
        if (! ci) {
            panic("missing chunk mapping");
            req.status = -EFAULT;
            return;
        }
        StTmp<Servers> serversTmp(mServers3Tmp);
        Servers&       srvs = serversTmp.Get();
        mChunkToServerMap.GetServers(*ci, srvs);
        ServerLocations::const_iterator       rit         = req.servers.begin();
        ServerLocations::const_iterator const rend        = req.servers.end();
        bool                                  removedFlag = false;
        for (Servers::const_iterator
                it = srvs.begin(); it != srvs.end(); ++it) {
            const ServerLocation& loc = (*it)->GetServerLocation();
            if (rit != rend && loc == *rit) {
                ++rit;
                continue;
            }
            if (rit == rend || find(rit, rend, loc) == rend) {
                KFS_LOG_STREAM_DEBUG <<
                    "-srv: "   << loc <<
                    " chunk: " << req.chunkId <<
                    " deleting replica"
                    " " << req.Show() <<
                KFS_LOG_EOM;
                const bool kStaleChunkIdFlag = true;
                (*it)->DeleteChunk(req.chunkId, kStaleChunkIdFlag);
                mChunkToServerMap.RemoveServer(*it, *ci);
                removedFlag = true;
            }
        }
        if (removedFlag) {
            CheckChunkReplication(*ci);
        }
        const bool kNofifyHibernatedOnlyFlag = true;
        mChunkToServerMap.SetVersion(
            *ci, req.chunkVersion, kNofifyHibernatedOnlyFlag);
    }
    if (req.alloc) {
        if (0 != req.status && req.initialChunkVersion < 0) {
            DeleteChunk(*(req.alloc));
        } else {
            CommitOrRollBackChunkVersion(*req.alloc);
        }
        return;
    }
    // Replay.
    if (0 != req.status || req.objectStoreFileFlag) {
        return;
    }
    if (req.initialChunkVersion < 0) {
        CSMap::Entry* const ci = mChunkToServerMap.Find(req.chunkId);
        if (! ci) {
            panic("missing chunk mapping");
            req.status = -EFAULT;
            return;
        }
        for (ServerLocations::const_iterator
                it = req.servers.begin();
                it != req.servers.end();
                ++it) {
            Servers::const_iterator const sit = FindServer(*it);
            if (sit == mChunkServers.end()) {
                KFS_LOG_STREAM_DEBUG <<
                    "no chunk server: " << *it <<
                    " " << req.Show() <<
                KFS_LOG_EOM;
                continue;
            }
            AddHosted(*ci, *sit);
        }
    }
    if (req.appendChunk) {
        // Create pending make stable in case of replay.
        ReplayPendingMakeStable(
            req.chunkId, req.chunkVersion, -1, false, 0, true);
    }
    mChunkVersionRollBack.Erase(req.chunkId);
}

void
LayoutManager::CommitOrRollBackChunkVersion(MetaAllocate& req)
{
    if (req.stripedFileFlag && req.initialChunkVersion < 0 &&
            0 != req.numReplicas) {
        if (mStripedFilesAllocationsInFlight.erase(make_pair(make_pair(
                req.fid, req.chunkBlockStart), req.chunkId)) != 1 &&
                0 == req.status && mPrimaryFlag) {
            panic("no striped file allocation entry");
        }
    }
    const int status = req.status;
    if (req.stoppedServicingFlag && 0 == req.status) {
        req.status    = -EVRNOTPRIMARY;
        req.statusMsg = "no longer primary node";
    }
    if (mClientCSAuthRequiredFlag && 0 <= req.status) {
        req.clientCSAllowClearTextFlag = mClientCSAllowClearTextFlag;
        if ((req.writeMasterKeyValidFlag = ! req.servers.empty() &&
                req.servers.front()->GetCryptoKey(
                    req.writeMasterKeyId, req.writeMasterKey))) {
            req.issuedTime   = TimeNow();
            req.validForTime = mCSAccessValidForTimeSec;
            req.tokenSeq     = (MetaAllocate::TokenSeq)mRandom.Rand();
        } else {
            req.status    = -EALLOCFAILED;
            req.statusMsg = "no write master crypto key";
        }
    }
    ChunkLeases::EntryKey const leaseKey(
        0 == req.numReplicas ? req.fid    : req.chunkId,
        0 == req.numReplicas ? req.offset : chunkOff_t(-1));
    if (0 <= status) {
        // Tree::assignChunkId() succeeded.
        // File and chunk ids are valid and in sync with meta tree.
        const bool kAllocDoneFlag = true;
        const int  ret            = mChunkLeases.Renew(
            req.fid, leaseKey, req.leaseId, kAllocDoneFlag);
        if (ret < 0 && ! req.stoppedServicingFlag) {
            panic("failed to renew allocation write lease");
            req.status = ret;
            return;
        }
        if (0 == req.numReplicas) {
            return;
        }
        // AddChunkToServerMapping() should delete version roll back for
        // new chunks.
        if (mChunkVersionRollBack.Erase(req.chunkId) > 0 &&
                req.initialChunkVersion < 0) {
            panic("chunk version roll back still exists");
            req.status = -EFAULT;
            return;
        }
        CSMap::Entry* const ci = mChunkToServerMap.Find(req.chunkId);
        if (! ci) {
            panic("missing chunk mapping");
            req.status = -EFAULT;
            return;
        }
        if (req.initialChunkVersion < 0) {
            // New valid chunk -- set servers.
            // req.offset is a multiple of CHUNKSIZE
            assert(req.offset >= 0 && (req.offset % CHUNKSIZE) == 0);
            if (req.fid != ci->GetFileId() ||
                    mChunkToServerMap.HasServers(*ci)) {
                panic("invalid chunk mapping");
                req.status = -EFAULT;
                return;
            }
            for (Servers::const_iterator
                    it = req.servers.begin();
                    it != req.servers.end();
                    ++it) {
                AddHosted(*ci, *it);
            }
            // Schedule replication check if needed.
            if (req.servers.size() != (size_t)req.numReplicas) {
                CheckReplication(*ci);
            }
        }
        if (req.appendChunk) {
            // Insert pending make stable entry here, to ensure that
            // it gets into the checkpoint.
            // With checkpoints from forked copy enabled checkpoint
            // can start *before* the corresponding make stable
            // starts
            const chunkOff_t kSize            = -1;
            const bool       kHasChecksumFlag = false;
            const uint32_t   kChecksum        = 0;
            bool             insertedFlag     = false;
            mPendingMakeStable.Insert(req.chunkId, PendingMakeStableEntry(
                    kSize, kHasChecksumFlag, kChecksum, req.chunkVersion),
                insertedFlag
            );
        }
        if (mClientCSAuthRequiredFlag && 0 <= req.status) {
            req.clientCSAllowClearTextFlag = mClientCSAllowClearTextFlag;
            if ((req.writeMasterKeyValidFlag = req.servers.front()->GetCryptoKey(
                    req.writeMasterKeyId, req.writeMasterKey))) {
                req.issuedTime   = TimeNow();
                req.validForTime = mCSAccessValidForTimeSec;
                req.tokenSeq     = (MetaAllocate::TokenSeq)mRandom.Rand();
            } else {
                // Fail the allocation only for the client, by telling him to
                // retry, but keep the chunk and lease,
                // This is required because the write ahead log replay does not
                // (and cannot possibly) go through this code path, therefore
                // changing the meta data state by deleting chunk or rolling
                // back chunk version might cause replay and checkpoint states
                // to diverge.
                req.status    = -EALLOCFAILED;
                req.statusMsg = "no write master crypto key";
            }
        }
        return;
    }
    // Delete write lease, it wasn't ever handed to the client, and
    // version change will make chunk stable, thus there is no need to
    // go trough the normal lease cleanup procedure.
    if (! mChunkLeases.DeleteWriteLease(req.fid, leaseKey, req.leaseId)) {
        if (0 == req.numReplicas) {
            return;
        }
        if (! mChunkToServerMap.Find(req.chunkId)) {
            // Chunk does not exist, deleted.
            mChunkVersionRollBack.Erase(req.chunkId);
            for_each(req.servers.begin(), req.servers.end(),
                bind(&ChunkServer::DeleteChunk, _1, req.chunkId));
            return;
        }
        if (req.stoppedServicingFlag) {
            return;
        }
        panic("chunk version roll back failed to delete write lease");
    }
    if (0 == req.numReplicas) {
        if (! req.servers.empty() && req.servers.front() &&
                ! req.servers.front()->IsDown()) {
            const bool       kHasChunkChecksum = false;
            const bool       kPendingAddFlag   = false;
            const chunkOff_t kChunkSize        = -1;
            req.servers.front()->MakeChunkStable(
                req.fid, req.fid, -req.chunkVersion - 1,
                kChunkSize, kHasChunkChecksum, 0, kPendingAddFlag);
        }
        return;
    }
    if (req.initialChunkVersion < 0 || req.logChunkVersionChangeFailedFlag) {
        return;
    }
    if (req.initialChunkVersion >= req.chunkVersion) {
        panic("invalid chunk version transition");
    }
    CSMap::Entry* const ci = mChunkToServerMap.Find(req.chunkId);
    if (! ci) {
        mChunkVersionRollBack.Erase(req.chunkId);
        for_each(req.servers.begin(), req.servers.end(),
            bind(&ChunkServer::DeleteChunk, _1, req.chunkId));
        return;
    }
    if (req.initialChunkVersion + GetChunkVersionRollBack(req.chunkId) !=
            req.chunkVersion) {
        ostringstream& os = GetTempOstream();
        os <<
        "invalid chunk version transition:" <<
        " "    << req.initialChunkVersion <<
        "+"    << GetChunkVersionRollBack(req.chunkId) <<
        " => " << req.chunkVersion;
        const string msg = os.str();
        panic(msg.c_str());
        return;
    }
    // Roll back to the initial chunk version, and make chunk stable.
    StTmp<Servers> serversTmp(mServers3Tmp);
    Servers&       srvs = serversTmp.Get();
    mChunkToServerMap.GetServers(*ci, srvs);
    const bool kMakeStableFlag = true;
    for (Servers::const_iterator it = srvs.begin(); it != srvs.end(); ++it) {
        if ((*it)->IsDown() ||
                find(req.servers.begin(), req.servers.end(), *it) ==
                req.servers.end()) {
            // Roll back only with servers that are not down, and were in
            // instructed to "re-allocate" / advance chunk versions.
            continue;
        }
        (*it)->NotifyChunkVersChange(
            req.fid,
            req.chunkId,
            req.initialChunkVersion, // to
            req.chunkVersion,        // from
            kMakeStableFlag
        );
    }
}

void
LayoutManager::ChangeChunkVersion(chunkId_t chunkId, seq_t version,
    MetaAllocate* req)
{
    CSMap::Entry* const ci = mChunkToServerMap.Find(chunkId);
    if (! ci) {
        if (req) {
            req->statusMsg = "no such chunk";
            req->status    = -EINVAL;
        }
        return;
    }
    if (req) {
        // Need space on the servers..otherwise, fail it
        req->servers.clear();
        mChunkToServerMap.GetServers(*ci, req->servers);
        if (req->servers.empty()) {
            // all the associated servers are dead...so, fail
            // the allocation request.
            req->statusMsg = "no replicas available";
            req->status    = -EDATAUNAVAIL;
        } else {
            req->allChunkServersShortRpcFlag = true;
            for (Servers::const_iterator it = req->servers.begin();
                    it != req->servers.end();
                    ++it) {
                req->allChunkServersShortRpcFlag =
                    req->allChunkServersShortRpcFlag &&
                    (*it)->IsShortRpcFormat();
                if ((*it)->GetAvailSpace() < mChunkAllocMinAvailSpace) {
                    req->status = -ENOSPC;
                    break;
                }
                if (! (*it)->IsConnected()) {
                    req->status    = -EALLOCFAILED;
                    req->statusMsg =
                        (*it)->GetServerLocation().ToString() + " went down";
                    break;
                }
            }
        }
        if (req->status < 0) {
            req->servers.clear();
        }
    }
    const bool kNofifyHibernatedOnlyFlag = true;
    mChunkToServerMap.SetVersion(*ci, version, kNofifyHibernatedOnlyFlag);
}

void
LayoutManager::SetChunkVersion(MetaChunkInfo& chunkInfo, seq_t version)
{
    const bool kNofifyHibernatedOnlyFlag = false;
    mChunkToServerMap.SetVersion(GetCsEntry(chunkInfo), version,
        kNofifyHibernatedOnlyFlag);
}

void
LayoutManager::Handle(MetaLeaseRelinquish& req)
{
    req.status = mChunkLeases.LeaseRelinquish(
        req, mARAChunkCache, mChunkToServerMap);
}

void
LayoutManager::CheckAllLeases()
{
    if (! mPrimaryFlag) {
        return;
    }
    mChunkLeases.Timer(TimeNow(), mLeaseOwnerDownExpireDelay,
        mARAChunkCache, mChunkToServerMap, mMaxDumpsterCleanupInFlight);
}

/*

Make chunk stable protocol description.

The protocol is mainly designed for write append, though it is also partially
used for random write.

The protocol is needed to solve consensus problem, i.e. make all chunk replicas
identical. This also allows replication participants (chunk servers) to
determine the status of a particular write append operation, and, if requested,
to convey this status to the write append client(s).

The fundamental idea is that the meta server always makes final irrevocable
decision what stable chunk replicas should be: chunk size and chunk checksum.
The meta server selects exactly one variant of the replica, and broadcast this
information to all replication participants (the hosting chunk servers). More
over, the meta server maintains this information until sufficient number or
replicas become "stable" as a result of "make chunk stable" operation, or as a
result of re-replication from already "stable" replica(s).

The meta server receives chunk size and chunk checksum from the chunk servers.
There are two ways meta server can get this information:
1. Normally chunk size, and checksum are conveyed by the write master in the
write lease release request.
2. The meta server declares chunk master nonoperational, and broadcasts "begin
make chunk stable" request to all remaining operational replication participants
(slaves). The slaves reply with chunk size and chunk checksum. The meta server
always selects one reply that has the smallest chunk size, in the hope that
other participants can converge their replicas to this chunk size, and checksum
by simple truncation. Begin make chunk stable repeated until the meta server
gets at least one valid reply.

The meta server writes this decision: chunk version, size, and checksum into the
log before broadcasting make chunk stable request with these parameters to all
operational replication participants. This guarantees that the decision is
final: it can never change as long as the log write is persistent. Once log
write completes successfully the meta server broadcasts make chunk stable
request to all operational
replication participants.

The chunk server maintains the information about chunk state: stable -- read
only, or not stable -- writable, and if the chunk was open for write append or
for random write. This information conveyed (back) to the meta server in the
chunk server hello message. The hello message contains 3 chunk lists: stable,
not stable write append, and not stable random write. This is needed to make
appropriate decision when chunk server establishes communication with the meta
server.

The chunk server can never transition not stable chunk replica into stable
replica, unless it receives make chunk stable request from the meta server.
The chunk server discards all non stable replicas on startup (restart).

For stable chunks the server is added to the list of the servers hosting the
chunk replica, as long as the corresponding chunk meta data exists, and version
of the replica matches the meta data version.

In case of a failure, the meta server declares chunk replica stale and conveys
this decision to the chunk server, then the chunk server discards the stale
replica.

For not stable random write chunk replicas the same checks are performed, plus
additional step: make chunk stable request is issued, The request in this case
does not specify the chunk size, and checksum. When make chunk stable completes
successfully the server added to the list of servers hosting the chunk replica.

With random writes version number is used to detect missing writes, and the task
of making chunk replicas consistent left entirely up to the writer (client).
Write lease mechanism is used to control write concurrency: for random write
only one concurrent writer per chunk is allowed.

Not stable write append chunk handling is more involved, because multiple
concurrent write appenders are allowed to append to the same chunk.

First, the same checks for chunk meta data existence, and the version match are
applied.  If successful, then the check for existing write lease is performed.
If the write (possibly expired) lease exists the server is added to the list of
servers hosting the replica. If the write lease exists, and begin make chunk
stable or make chunk stable operation for the corresponding chunk is in
progress, the chunk server is added to the operation.

If no lease exists, and begin make chunk stable was never successfully completed
(no valid pending make chunk stable info exists), then the meta server issues
begin make chunk stable request.

Once begin make chunks stable successfully completes the meta server writes
"mkstable" log record with the chunk version, size, and checksum into the log,
and adds this information to in-memory pending make chunk stable table. Make
chunk stable request is issued after log write successfully completes.

The make chunk stable info is kept in memory, and in the checkpoint file until
sufficient number of stable replicas created, or chunk ceases to exist. Once
sufficient number of replicas is created, the make chunk stable info is purged
from memory, and the "mkstabledone" record written to the log. If chunk ceases
to exist then only in-memory information purged, but no log write performed.
"Mkstabledone" log records effectively cancels "mkstable" record.

Chunk allocation log records have an additional "append" attribute set to 1. Log
replay process creates in-memory make chunk stable entry with chunk size
attribute set to -1 for every chunk allocation record with the append attribute
set to 1. In memory entries with size set to -1 mark not stable chunks for which
chunk size and chunk checksum are not known. For such chunks begin make stable
has to be issued first. The "mkstable" records are used to update in-memory
pending make stable info with the corresponding chunk size and checksum. The
"mkstabledone" records are used to delete the corresponding in-memory pending
make stable info. Chunk delete log records also purge the corresponding
in-memory pending make stable info.

In memory pending delete info is written into the checkpoint file, after the
meta (tree) information. One "mkstable" entry for every chunk that is not
stable, or does not have sufficient number of replicas.

During "recovery" period, begin make chunk stable is not issued, instead these
are delayed until recovery period ends, in the hope that begin make stable with
more servers has higher chances of succeeding, and can potentially produce more
stable replicas.

*/

void
LayoutManager::MakeChunkStableInit(
    const CSMap::Entry& entry,
    seq_t               chunkVersion,
    string              pathname,
    bool                beginMakeStableFlag,
    chunkOff_t          chunkSize,
    bool                hasChunkChecksum,
    uint32_t            chunkChecksum,
    bool                stripedFileFlag,
    bool                appendFlag,
    bool                leaseRelinquishFlag)
{
    const char* const logPrefix  = beginMakeStableFlag ? "BMCS:" : "MCS:";
    const chunkId_t   chunkId    = entry.GetChunkId();
    const fid_t       fid        = entry.GetFileId();
    StTmp<Servers>    serversTmp(mServers3Tmp);
    Servers&          srvs       = serversTmp.Get();
    const int         serversCnt =
        (int)mChunkToServerMap.GetServers(entry, srvs);
    const Servers&    servers    = srvs;
    if (serversCnt <= 0) {
        if (leaseRelinquishFlag) {
            // Update file modification time.
            MetaFattr* const fa  = entry.GetFattr();
            const int64_t    now = microseconds();
            if (! fa->IsStriped() &&
                    fa->mtime + mMTimeUpdateResolution < now) {
                submit_request(new MetaSetMtime(fid, now));
            }
        }
        if (beginMakeStableFlag) {
            // Ensure that there is at least pending begin make
            // stable.
            // Append allocations are marked as such, and log replay
            // adds begin make stable entries if necessary.
            bool insertedFlag = false;
            PendingMakeStableEntry* const entry = mPendingMakeStable.Insert(
                    chunkId, PendingMakeStableEntry(), insertedFlag);
            if (insertedFlag) {
                entry->mChunkVersion = chunkVersion;
            }
        }
        // If no servers, MCS with append (checksum and size) still
        // needs to be logged and the corresponding pending make stable
        // entry has to be created.
        if (beginMakeStableFlag || ! appendFlag) {
            const MetaFattr* const fa = entry.GetFattr();
            KFS_LOG_STREAM_INFO << logPrefix <<
                " <" << fid << "," << chunkId << ">"
                " name: "     << pathname <<
                " servers: 0" <<
                " replication: " << fa->numReplicas <<
                " recovery: "    << fa->numRecoveryStripes <<
            KFS_LOG_EOM;
            // Update replication state.
            ChangeChunkReplication(chunkId);
            return;
        }
    }
    bool nonStableInsertedFlag = false;
    MakeChunkStableInfo* const ret = mNonStableChunks.Insert(chunkId,
            MakeChunkStableInfo(
                serversCnt,
                beginMakeStableFlag,
                pathname,
                chunkVersion,
                stripedFileFlag,
                leaseRelinquishFlag && serversCnt > 0 &&
                    ! entry.GetFattr()->IsStriped()
        ), nonStableInsertedFlag);
    if (! nonStableInsertedFlag) {
        KFS_LOG_STREAM_INFO << logPrefix <<
            " <" << fid << "," << chunkId << ">"
            " name: " << pathname <<
            " already in progress" <<
        KFS_LOG_EOM;
        return;
    }
    KFS_LOG_STREAM_INFO << logPrefix <<
        " <" << fid << "," << chunkId << ">"
        " name: "     << pathname <<
        " version: "  << chunkVersion <<
        " servers: "  << serversCnt <<
        " size: "     << chunkSize <<
        " checksum: " << (hasChunkChecksum ?
            (int64_t)chunkChecksum : (int64_t)-1) <<
        " append: "   << appendFlag <<
    KFS_LOG_EOM;
    if (beginMakeStableFlag) {
        for_each(servers.begin(), servers.end(),
            bind(&ChunkServer::BeginMakeChunkStable, _1,
                fid, chunkId, chunkVersion));
    } else if (appendFlag) {
        // Remember chunk check sum and size.
        PendingMakeStableEntry const pmse(
            chunkSize,
            hasChunkChecksum,
            chunkChecksum,
            chunkVersion
        );
        bool insertedFlag = false;
        PendingMakeStableEntry* const entry =
            mPendingMakeStable.Insert(chunkId, pmse, insertedFlag);
        if (! insertedFlag) {
            KFS_LOG_STREAM((entry->mSize >= 0 || entry->mHasChecksum) ?
                    MsgLogger::kLogLevelWARN :
                    MsgLogger::kLogLevelDEBUG) <<
                logPrefix <<
                " <" << fid << "," << chunkId << ">"
                " updating existing pending MCS: " <<
                " chunk: "    << chunkId <<
                " version: "  << entry->mChunkVersion <<
                "=>"          << pmse.mChunkVersion <<
                " size: "     << entry->mSize <<
                "=>"          << pmse.mSize <<
                " checksum: " << (entry->mHasChecksum ?
                    int64_t(entry->mChecksum) :
                    int64_t(-1)) <<
                "=>"          << (pmse.mHasChecksum ?
                    int64_t(pmse.mChecksum) :
                    int64_t(-1)) <<
            KFS_LOG_EOM;
            *entry = pmse;
        }
        ret->logMakeChunkStableFlag = true;
        submit_request(new MetaLogMakeChunkStable(
            fid, chunkId, chunkVersion,
            chunkSize, hasChunkChecksum, chunkChecksum, chunkId
        ));
    } else {
        const bool kPendingAddFlag = false;
        for_each(servers.begin(), servers.end(), bind(
            &ChunkServer::MakeChunkStable, _1,
            fid, chunkId, chunkVersion,
            chunkSize, hasChunkChecksum, chunkChecksum,
            kPendingAddFlag
        ));
    }
}

bool
LayoutManager::AddServerToMakeStable(
    CSMap::Entry&  placementInfo,
    ChunkServerPtr server,
    chunkId_t      chunkId,
    seq_t          chunkVersion,
    const char*&   errMsg)
{
    errMsg = 0;
    MakeChunkStableInfo* const it = mNonStableChunks.Find(chunkId);
    if (! it) {
        return false; // Not in progress
    }
    MakeChunkStableInfo& info = *it;
    if (info.chunkVersion != chunkVersion) {
        errMsg = "version mismatch";
        return false;
    }
    StTmp<Servers> serversTmp(mServers3Tmp);
    Servers&       servers = serversTmp.Get();
    mChunkToServerMap.GetServers(placementInfo, servers);
    if (find_if(servers.begin(), servers.end(),
            bind(&ChunkServer::GetServerLocation, _1) ==
                server->GetServerLocation()
            ) != servers.end()) {
        // Already there, duplicate chunk? Same as in progress.
        return true;
    }
    KFS_LOG_STREAM_DEBUG <<
        (info.beginMakeStableFlag ? "B" :
            info.logMakeChunkStableFlag ? "L" : "") <<
        "MCS:"
        " <" << placementInfo.GetFileId() << "," << chunkId << ">"
        " adding server: " << server->GetServerLocation() <<
        " name: "          << info.pathname <<
        " servers: "       << info.numAckMsg <<
        "/"                << info.numServers <<
        "/"                << servers.size() <<
        " size: "          << info.chunkSize <<
        " checksum: "      << info.chunkChecksum <<
        " added: "         << info.serverAddedFlag <<
    KFS_LOG_EOM;
    AddHosted(chunkId, placementInfo, server);
    info.numServers++;
    info.serverAddedFlag = true;
    if (info.beginMakeStableFlag) {
        server->BeginMakeChunkStable(
            placementInfo.GetFileId(), chunkId, info.chunkVersion);
    } else if (! info.logMakeChunkStableFlag) {
        const bool kPendingAddFlag = false;
        server->MakeChunkStable(
            placementInfo.GetFileId(),
            chunkId,
            info.chunkVersion,
            info.chunkSize,
            info.chunkSize >= 0,
            info.chunkChecksum,
            kPendingAddFlag
        );
    }
    // If log make stable is in progress, then make stable or begin make
    // stable will be started when logging is done.
    return true;
}

void
LayoutManager::ScheduleResubmitOrCancel(MetaRequest& req)
{
    if (req.next || 0 <= req.status || req.logseq.IsValid() ||
            req.submitCount <= 0) {
        panic("invalid resubmit request attempt");
    }
    if (! mPrimaryFlag || mCleanupFlag) {
        KFS_LOG_STREAM_DEBUG <<
            "not primary, ignoring resubmit: " <<
            " status: " << req.status <<
            " "         << req.statusMsg <<
            " "         << req.Show() <<
        KFS_LOG_EOM;
        return;
    }
    req.next      = 0;
    req.suspended = true;
    mResubmitQueue.PushBack(req);
}

void
LayoutManager::BeginMakeChunkStableDone(const MetaBeginMakeChunkStable& req)
{
    const char* const          logPrefix = "BMCS: done";
    MakeChunkStableInfo* const it        = mNonStableChunks.Find(req.chunkId);
    if (! it || ! it->beginMakeStableFlag) {
        KFS_LOG_STREAM_DEBUG << logPrefix <<
            " <" << req.fid << "," << req.chunkId << ">"
            " " << req.Show() <<
            " ignored: " <<
            (it ? "MCS in progress" : "not in progress") <<
        KFS_LOG_EOM;
        return;
    }
    MakeChunkStableInfo& info = *it;
    KFS_LOG_STREAM_DEBUG << logPrefix <<
        " <" << req.fid << "," << req.chunkId << ">"
        " name: "     << info.pathname <<
        " servers: "  << info.numAckMsg <<
        "/"           << info.numServers <<
        " size: "     << info.chunkSize <<
        " checksum: " << info.chunkChecksum <<
        " " << req.Show() <<
    KFS_LOG_EOM;
    CSMap::Entry* ci              = 0;
    bool          noSuchChunkFlag = false;
    if (req.status != 0 || req.chunkSize < 0) {
        if (req.status == 0 && req.chunkSize < 0) {
            KFS_LOG_STREAM_ERROR << logPrefix <<
                " <" << req.fid << "," << req.chunkId  << ">"
                " invalid chunk size: " << req.chunkSize <<
                " declaring chunk replica corrupt" <<
                " " << req.Show() <<
            KFS_LOG_EOM;
        }
        ci = mChunkToServerMap.Find(req.chunkId);
        if (ci) {
            const ChunkServerPtr server = ci->GetServer(
                mChunkToServerMap, req.serverLoc);
            if (server && ! server->IsDown()) {
                ChunkCorrupt(req.chunkId, server);
            }
        } else {
            noSuchChunkFlag = true;
        }
    } else if (req.chunkSize < info.chunkSize || info.chunkSize < 0) {
        // Pick the smallest good chunk.
        info.chunkSize     = req.chunkSize;
        info.chunkChecksum = req.chunkChecksum;
    }
    if (++info.numAckMsg < info.numServers) {
        return;
    }
    if (! noSuchChunkFlag && ! ci) {
        ci = mChunkToServerMap.Find(req.chunkId);
        noSuchChunkFlag = ! ci;
    }
    if (noSuchChunkFlag) {
        KFS_LOG_STREAM_DEBUG << logPrefix <<
            " <" << req.fid << "," << req.chunkId  << ">"
            " no such chunk, cleaning up" <<
        KFS_LOG_EOM;
        DeleteNonStableEntry(req.chunkId, it, -EINVAL, "no such chunk");
        mPendingMakeStable.Erase(req.chunkId);
        return;
    }
    info.beginMakeStableFlag    = false;
    info.logMakeChunkStableFlag = true;
    info.serverAddedFlag        = false;
    // Remember chunk check sum and size.
    PendingMakeStableEntry const pmse(
        info.chunkSize,
        info.chunkSize >= 0,
        info.chunkChecksum,
        req.chunkVersion
    );
    bool insertedFlag = false;
    PendingMakeStableEntry* const entry =
        mPendingMakeStable.Insert(req.chunkId, pmse, insertedFlag);
    assert(
        insertedFlag ||
        (entry->mSize < 0 && entry->mChunkVersion == pmse.mChunkVersion)
    );
    if (! insertedFlag && pmse.mSize >= 0) {
        *entry = pmse;
    }
    if (entry->mSize < 0) {
        int numUpServers = 0;
        StTmp<Servers> serversTmp(mServers3Tmp);
        Servers&       servers = serversTmp.Get();
        mChunkToServerMap.GetServers(*ci, servers);
        for (Servers::const_iterator
                si = servers.begin();
                si != servers.end();
                ++si) {
            if (! (*si)->IsDown()) {
                numUpServers++;
            }
        }
        if (numUpServers <= 0) {
            const MetaFattr* const fa = ci->GetFattr();
            KFS_LOG_STREAM_DEBUG << logPrefix <<
                " <" << req.fid << "," << req.chunkId  << ">"
                " no servers up, retry later" <<
                " servers: 0" <<
                " replication: " << fa->numReplicas <<
                " recovery: "    << fa->numRecoveryStripes <<
            KFS_LOG_EOM;
        } else {
            // Shouldn't get here.
            KFS_LOG_STREAM_WARN << logPrefix <<
                " <" << req.fid << "," << req.chunkId  << ">"
                " internal error:"
                " up servers: "         << numUpServers <<
                " invalid chunk size: " <<
                    entry->mSize <<
            KFS_LOG_EOM;
        }
        // Try again later.
        DeleteNonStableEntry(req.chunkId, it, -EAGAIN, "no replicas available");
        UpdateReplicationState(*ci);
        return;
    }
    submit_request(new MetaLogMakeChunkStable(
        req.fid, req.chunkId, req.chunkVersion,
        info.chunkSize, info.chunkSize >= 0, info.chunkChecksum,
        req.opSeqno
    ));
}

void
LayoutManager::LogMakeChunkStableDone(MetaLogMakeChunkStable& req)
{
    const char* const          logPrefix = "LMCS: done";
    MakeChunkStableInfo* const it        = mNonStableChunks.Find(req.chunkId);
    if (! it) {
        KFS_LOG_STREAM_DEBUG << logPrefix <<
            " <" << req.fid << "," << req.chunkId  << ">"
            " " << req.Show() <<
            " ignored: not in progress" <<
        KFS_LOG_EOM;
        // Update replication state.
        ChangeChunkReplication(req.chunkId);
        return;
    }
    if (! it->logMakeChunkStableFlag) {
        KFS_LOG_STREAM_ERROR << logPrefix <<
            " <" << req.fid << "," << req.chunkId  << ">"
            " " << req.Show() <<
            " ignored: " <<
                (it->beginMakeStableFlag ? "B" : "") <<
                "MCS in progress" <<
        KFS_LOG_EOM;
        return;
    }
    MakeChunkStableInfo& info = *it;
    CSMap::Entry* const  ci   = mChunkToServerMap.Find(req.chunkId);
    if (! ci || ! mChunkToServerMap.HasServers(*ci)) {
        KFS_LOG_STREAM_INFO << logPrefix <<
            " <" << req.fid << "," << req.chunkId  << ">" <<
            " name: " << info.pathname <<
            (! ci ?
                " does not exist, cleaning up" :
                " no servers, run MCS later") <<
        KFS_LOG_EOM;
        if (ci) {
            UpdateReplicationState(*ci);
        } else {
            // If chunk was deleted, do not emit mkstabledone log
            // entry. Only ensure that no stale pending make stable
            // entry exists.
            mPendingMakeStable.Erase(req.chunkId);
        }
        DeleteNonStableEntry(
            req.chunkId, it,
            ci ? -EINVAL         : -EAGAIN,
            ci ? "no such chunk" : "no replicas available"
        );
        return;
    }
    if (req.status < 0) {
        // Log failure.
        KFS_LOG_STREAM_ERROR <<
            req.Show() <<
            " status: " << req.status <<
            " "         << req.statusMsg <<
        KFS_LOG_EOM;
        // Make stable needs to be re-submitted once the logger starts working,
        // again, or canceled if the other meta server becomes primary.
        // In the later case the primary will re-run make stable state machine
        // as a result of the non-stable chunks inventory synchronization.
        // Possible log failures of make stable done, should be resolved by
        // full chunk replication check, that should be initiated by the primary
        // once logger starts working again.
        if (mPrimaryFlag) {
            ScheduleResubmitOrCancel(req);
        } else {
            DeleteNonStableEntry(
                req.chunkId, it, req.status, req.statusMsg.c_str());
        }
        return;
    }
    const bool     serverWasAddedFlag = info.serverAddedFlag;
    const int      prevNumServer      = info.numServers;
    StTmp<Servers> serversTmp(mServers3Tmp);
    Servers&       servers            = serversTmp.Get();
    info.numServers                   =
        (int)mChunkToServerMap.GetServers(*ci, servers);
    info.numAckMsg                    = 0;
    info.beginMakeStableFlag          = false;
    info.logMakeChunkStableFlag       = false;
    info.serverAddedFlag              = false;
    info.chunkSize                    = req.chunkSize;
    info.chunkChecksum                = req.chunkChecksum;
    KFS_LOG_STREAM_INFO << logPrefix <<
        " <" << req.fid << "," << req.chunkId  << ">"
        " starting MCS"
        " version: "  << req.chunkVersion  <<
        " name: "     << info.pathname <<
        " size: "     << info.chunkSize     <<
        " checksum: " << info.chunkChecksum <<
        " servers: "  << prevNumServer << "->" << info.numServers <<
        " "           << (serverWasAddedFlag ? "new servers" : "") <<
    KFS_LOG_EOM;
    if (serverWasAddedFlag && info.chunkSize < 0) {
        // Retry make chunk stable with newly added servers.
        info.beginMakeStableFlag = true;
        for_each(servers.begin(), servers.end(), bind(
            &ChunkServer::BeginMakeChunkStable, _1,
            ci->GetFileId(), req.chunkId, info.chunkVersion
        ));
        return;
    }
    const bool kPendingAddFlag = false;
    for_each(servers.begin(), servers.end(), bind(
        &ChunkServer::MakeChunkStable, _1,
        req.fid, req.chunkId, req.chunkVersion,
        req.chunkSize, req.hasChunkChecksum, req.chunkChecksum,
        kPendingAddFlag
    ));
}

void
LayoutManager::MakeChunkStableDone(const MetaChunkMakeStable& req)
{
    if (req.chunkVersion < 0) {
        // Client is responsible for updating logical EOF for object store
        // files.
        if (0 != req.status) {
            return;
        }
        ChunkLeases::EntryKey const key(req.chunkId,
            ChunkVersionToObjFileBlockPos(req.chunkVersion));
        const ChunkLeases::WriteLease* const lease =
            mChunkLeases.GetWriteLease(key);
        if (lease && lease->relinquishedFlag) {
            mChunkLeases.DeleteWriteLease(req.fid, key, lease->leaseId);
        }
        return;
    }
    if (req.replayFlag) {
        return;
    }
    const char* const          logPrefix       = "MCS: done";
    string                     pathname;
    CSMap::Entry*              pinfo           = 0;
    bool                       updateSizeFlag  = false;
    bool                       updateMTimeFlag = false;
    MakeChunkStableInfo* const it              =
        mNonStableChunks.Find(req.chunkId);
    if (req.pendingAddFlag) {
        // Make chunk stable started in AddNotStableChunk() is now
        // complete. Sever can be added if nothing has changed since
        // the op was started.
        // It is also crucial to ensure to the server with the
        // identical location is not already present in the list of
        // servers hosting the chunk before declaring chunk stale.
        bool                           notifyStaleFlag = true;
        const char*                    res             = 0;
        PendingMakeStableEntry*        msi             = 0;
        ChunkLeases::WriteLease const* li              = 0;
        if (it) {
            res = "not stable again";
        } else {
            msi = mPendingMakeStable.Find(req.chunkId);
        }
        if (res) {
            // Has already failed.
        } else if (req.chunkSize >= 0 || req.hasChunkChecksum) {
            if (! msi) {
                // Chunk went away, or already sufficiently
                // replicated.
                res = "no pending make stable info";
            } else if (msi->mChunkVersion != req.chunkVersion ||
                    msi->mSize            != req.chunkSize ||
                    msi->mHasChecksum     != req.hasChunkChecksum ||
                    msi->mChecksum        != req.chunkChecksum) {
                // Stale request.
                res = "pending make stable info has changed";
            }
        } else if (msi) {
            res = "pending make stable info now exists";
        }
        if (req.server->IsDown()) {
            res = "server down";
            notifyStaleFlag = false;
        } else if (0 != req.status) {
            res = "request failed";
        } else {
            CSMap::Entry* const ci = mChunkToServerMap.Find(req.chunkId);
            if (! ci) {
                res = "no such chunk";
            } else if (! ci->HasServer(mChunkToServerMap, req.server)) {
                res = "chunk log completion failure to add server";
            } else if ((li = mChunkLeases.GetChunkWriteLease(req.chunkId)) &&
                    (((! li->relinquishedFlag && li->expires >= TimeNow()) ||
                    li->chunkVersion != req.chunkVersion))) {
                // No write lease existed when this was started.
                res = "new write lease exists";
            } else if (req.chunkVersion != ci->GetChunkInfo()->chunkVersion) {
                res = "chunk version has changed";
            } else {
                pinfo           = ci;
                updateSizeFlag  = 1 == mChunkToServerMap.ServerCount(*pinfo);
                notifyStaleFlag = false;
            }
        }
        if (res) {
            KFS_LOG_STREAM_INFO << logPrefix <<
                " <" << req.fid << "," << req.chunkId  << ">"
                " "  << req.server->GetServerLocation() <<
                " not added: " << res <<
                (notifyStaleFlag ? " => stale" : "") <<
                "; " << req.Show() <<
            KFS_LOG_EOM;
            if (notifyStaleFlag &&
                    (0 == req.status || ! req.staleChunkIdFlag)) {
                req.server->NotifyStaleChunk(req.chunkId);
            }
            // List of servers hosting the chunk remains unchanged.
            return;
        }
    } else {
        if (! it ||
                it->beginMakeStableFlag ||
                it->logMakeChunkStableFlag) {
            KFS_LOG_STREAM_ERROR << "MCS"
                " " << req.Show() <<
                " ignored: BMCS in progress" <<
            KFS_LOG_EOM;
            return;
        }
        MakeChunkStableInfo& info = *it;
        KFS_LOG_STREAM_DEBUG << logPrefix <<
            " <" << req.fid << "," << req.chunkId  << ">"
            " name: "     << info.pathname <<
            " servers: "  << info.numAckMsg <<
            "/"           << info.numServers <<
            " size: "     << req.chunkSize <<
            "/"           << info.chunkSize <<
            " checksum: " << req.chunkChecksum <<
            "/"           << info.chunkChecksum <<
            " "           << req.Show() <<
        KFS_LOG_EOM;
        if (req.status != 0 && ! req.server->IsDown()) {
            ChunkCorrupt(req.chunkId, req.server);
        }
        if (++info.numAckMsg < info.numServers) {
            return;
        }
        // Cleanup mNonStableChunks, after the lease cleanup, for extra
        // safety: this will prevent make chunk stable from restarting
        // recursively, in the case if there are double or stale
        // write lease.
        ExpiredLeaseCleanup(TimeNow(), req.chunkId);
        pathname = info.pathname;
        updateSizeFlag  = ! info.stripedFileFlag;
        updateMTimeFlag = info.updateMTimeFlag;
        DeleteNonStableEntry(req.chunkId, it);
        // "&info" is invalid at this point.
    }
    if (! pinfo) {
        CSMap::Entry* const ci = mChunkToServerMap.Find(req.chunkId);
        if (! ci) {
            KFS_LOG_STREAM_INFO << logPrefix <<
                " <"      << req.fid <<
                ","       << req.chunkId  << ">" <<
                " name: " << pathname <<
                " does not exist, skipping size update" <<
            KFS_LOG_EOM;
            return;
        }
        pinfo = ci;
    }
    UpdateReplicationState(*pinfo);
    unsigned int   numServers     = 0;
    int            numDownServers = 0;
    ChunkServerPtr goodServer;
    StTmp<Servers> serversTmp(mServers3Tmp);
    Servers&       servers = serversTmp.Get();
    mChunkToServerMap.GetServers(*pinfo, servers);
    for (Servers::const_iterator csi = servers.begin();
            csi != servers.end();
            ++csi) {
        if ((*csi)->IsConnected()) {
            numServers++;
            if (! goodServer) {
                goodServer = *csi;
            }
        } else {
            numDownServers++;
        }
    }
    MetaFattr* const fa     = pinfo->GetFattr();
    const fid_t      fileId = pinfo->GetFileId();
    if (updateMTimeFlag || mChunkToServerMap.GetState(*pinfo) !=
            CSMap::Entry::kStateCheckReplication) {
        if (fa->IsStriped()) {
            updateSizeFlag = false;
        }
        if (updateMTimeFlag) {
            const int64_t now = microseconds();
            if (fa->mtime + mMTimeUpdateResolution < now) {
                submit_request(new MetaSetMtime(fileId, now));
            }
        }
        if (fa->numReplicas != numServers) {
            CheckReplication(*pinfo);
        } else {
            CancelPendingMakeStable(fileId, req.chunkId);
        }
    }
    KFS_LOG_STREAM_INFO << logPrefix <<
        " <" << req.fid << "," << req.chunkId  << ">"
        " fid: "              << fileId <<
        " version: "          << req.chunkVersion  <<
        " name: "             << pathname <<
        " size: "             << req.chunkSize <<
        " checksum: "         << req.chunkChecksum <<
        " replicas: "         << fa->numReplicas <<
        " is now stable on: " << numServers <<
        " down: "             << numDownServers <<
        " server(s)" <<
    KFS_LOG_EOM;
    if (! updateSizeFlag ||
            numServers <= 0 ||
            fa->filesize >= 0 ||
            fa->IsStriped() ||
            pinfo->GetChunkInfo()->offset +
                (chunkOff_t)CHUNKSIZE < fa->nextChunkOffset()) {
         // if no servers, or not the last chunk can not update size.
        return;
    }
    if (req.chunkSize >= 0) {
        // Already know the size, update it.
        // The following will invoke Handle(MetaChunkSize),
        // and update the log.
        MetaChunkSize& op = *(new MetaChunkSize(
            req.server, // chunk server
            fileId, req.chunkId, req.chunkVersion,
            false
        ));
        op.chunkSize = req.chunkSize;
        submit_request(&op);
    } else {
        // Get the chunk's size from one of the servers.
        goodServer->GetChunkSize(fileId, req.chunkId, req.chunkVersion);
    }
}

void
LayoutManager::Handle(MetaLogMakeChunkStableDone& req)
{
    if (0 != req.status) {
        if (req.replayFlag) {
            panic("invalid log make chunk stable done");
        } else if (IsMetaLogWriteOrVrError(req.status)) {
            ScheduleResubmitOrCancel(req);
        }
        // Schedule replication check to remove pending make chunk stable entry
        // if needed.
        ChangeChunkReplication(req.chunkId);
        return;
    }
    const bool kAddFlag = false;
    ReplayPendingMakeStable(req.chunkId, req.chunkVersion, req.chunkSize,
        req.hasChunkChecksum, req.chunkChecksum, kAddFlag);
}

void
LayoutManager::ReplayPendingMakeStable(
    chunkId_t  chunkId,
    seq_t      chunkVersion,
    chunkOff_t chunkSize,
    bool       hasChunkChecksum,
    uint32_t   chunkChecksum,
    bool       addFlag)
{
    const char*               res             = 0;
    seq_t                     curChunkVersion = -1;
    const CSMap::Entry* const ci              = mChunkToServerMap.Find(chunkId);
    MsgLogger::LogLevel       logLevel        = MsgLogger::kLogLevelDEBUG;
    if (! ci) {
        res = "no such chunk";
    } else if ((curChunkVersion = ci->GetChunkInfo()->chunkVersion) !=
            chunkVersion) {
        res      = "chunk version mismatch";
        logLevel = MsgLogger::kLogLevelERROR;
    } else if (addFlag) {
        const PendingMakeStableEntry entry(
            chunkSize,
            hasChunkChecksum,
            chunkChecksum,
            chunkVersion
        );
        bool insertedFlag = false;
        PendingMakeStableEntry* const cur =
            mPendingMakeStable.Insert(chunkId, entry, insertedFlag);
        if (! insertedFlag) {
            KFS_LOG_STREAM(((cur->mHasChecksum || cur->mSize >= 0) &&
                        *cur != entry) ?
                    MsgLogger::kLogLevelWARN :
                    MsgLogger::kLogLevelDEBUG) <<
                "replay MCS add:" <<
                " update:"
                " chunk: "    << chunkId <<
                " version: "  << cur->mChunkVersion <<
                "=>"          << entry.mChunkVersion <<
                " size: "     << cur->mSize <<
                "=>"          << entry.mSize <<
                " checksum: " << (cur->mHasChecksum ?
                    int64_t(cur->mChecksum) :
                    int64_t(-1)) <<
                "=>"          << (entry.mHasChecksum ?
                    int64_t(entry.mChecksum) :
                    int64_t(-1)) <<
            KFS_LOG_EOM;
            *cur = entry;
        }
    } else {
        PendingMakeStableEntry* const it = mPendingMakeStable.Find(chunkId);
        if (! it) {
            res      = "no such entry";
            logLevel = MsgLogger::kLogLevelDEBUG;
        } else {
            const bool warn =
                it->mChunkVersion != chunkVersion ||
                (it->mSize >= 0 && (
                    it->mSize != chunkSize ||
                    it->mHasChecksum != hasChunkChecksum ||
                    (hasChunkChecksum && it->mChecksum != chunkChecksum)
                ));
            KFS_LOG_STREAM(warn ?
                    MsgLogger::kLogLevelWARN :
                    MsgLogger::kLogLevelDEBUG) <<
                "replay MCS remove:"
                " chunk: "    << chunkId <<
                " version: "  << it->mChunkVersion <<
                "=>"          << chunkVersion <<
                " size: "     << it->mSize <<
                "=>"          << chunkSize <<
                " checksum: " << (it->mHasChecksum ?
                    int64_t(it->mChecksum) :
                    int64_t(-1)) <<
                "=>"          << (hasChunkChecksum ?
                    int64_t(chunkChecksum) : int64_t(-1)) <<
            KFS_LOG_EOM;
            mPendingMakeStable.Erase(chunkId);
            mPendingBeginMakeStable.Erase(chunkId);
        }
    }
    KFS_LOG_STREAM(logLevel) <<
        "replay MCS: " <<
        (addFlag ? "add" : "remove") <<
        " "           << (res ? res : "ok") <<
        " total: "    << mPendingMakeStable.GetSize() <<
        " chunk: "    << chunkId <<
        " version: "  << chunkVersion <<
        " cur vers: " << curChunkVersion <<
        " size: "     << chunkSize <<
        " checksum: " << (hasChunkChecksum ?
            int64_t(chunkChecksum) : int64_t(-1)) <<
    KFS_LOG_EOM;
}

void
LayoutManager::DeleteNonStableEntry(
    chunkId_t                                 chunkId,
    const LayoutManager::MakeChunkStableInfo* it,
    int                                       status    /* = 0 */,
    const char*                               statusMsg /* = 0 */)
{
    MetaRequest* next = it->pendingReqRingTail;
    mNonStableChunks.Erase(chunkId);
    if (! next) {
        return;
    }
    // Submit requests in the same order they came in.
    next = next->next;
    const MetaRequest* const head = next;
    do {
        MetaRequest& req = *next;
        next = req.next;
        req.next = 0;
        req.suspended = false;
        req.status = status;
        if (statusMsg) {
            req.statusMsg = statusMsg;
        }
        submit_request(&req);
    } while (head != next);
}

int
LayoutManager::WritePendingMakeStable(ostream& os)
{
    ReqOstream ros(os);
    // Write all entries in restore_makestable() format.
    const PendingMakeStableKVEntry* it;
    mPendingMakeStable.First();
    while ((it = mPendingMakeStable.Next()) && os) {
        ros <<
            "mkstable"
            "/chunkId/"      << it->GetKey() <<
            "/chunkVersion/" << it->GetVal().mChunkVersion  <<
            "/size/"         << it->GetVal().mSize <<
            "/checksum/"     << it->GetVal().mChecksum <<
            "/hasChecksum/"  << (it->GetVal().mHasChecksum ? 1 : 0) <<
        "\n";
    }
    ros.flush();
    return (os ? 0 : -EIO);
}

void
LayoutManager::CancelPendingMakeStable(fid_t fid, chunkId_t chunkId)
{
    if (! mPrimaryFlag) {
        return;
    }
    PendingMakeStableEntry* const it = mPendingMakeStable.Find(chunkId);
    if (! it) {
        return;
    }
    MakeChunkStableInfo* const nsi = mNonStableChunks.Find(chunkId);
    if (nsi) {
        KFS_LOG_STREAM_ERROR <<
            "delete pending MCS:"
            " <" << fid << "," << chunkId << ">" <<
            " attempt to delete while " <<
            (nsi->beginMakeStableFlag ? "B" :
                (nsi->logMakeChunkStableFlag ? "L" : "")) <<
            "MCS is in progress denied" <<
        KFS_LOG_EOM;
        return;
    }
    // Emit done log record -- this "cancels" "mkstable" log record.
    // Do not write if begin make stable wasn't started before the
    // chunk got deleted.
    MetaLogMakeChunkStableDone* const op = new MetaLogMakeChunkStableDone(
        fid, chunkId, it->mChunkVersion,
        it->mSize, it->mHasChecksum,
        it->mChecksum, chunkId
    );
    KFS_LOG_STREAM_DEBUG <<
        "delete pending MCS:"
        " <" << fid << "," << chunkId << ">" <<
        " total: " << mPendingMakeStable.GetSize() <<
        " " << MetaRequest::ShowReq(op) <<
    KFS_LOG_EOM;
    submit_request(op);
}

bool
LayoutManager::Start(MetaChunkSize& req)
{
    if (req.status < 0) {
        return false;
    }
    if (req.chunkVersion < 0 || // object store block.
            req.chunkSize < 0 ||
            ! IsChunkStable(req.chunkId)) {
        req.status = -EINVAL;
        return false;
    }
    const CSMap::Entry* const ci = mChunkToServerMap.Find(req.chunkId);
    if (! ci) {
        req.status = -ENOENT;
        return false;
    }
    MetaFattr* const           fa    = ci->GetFattr();
    const MetaChunkInfo* const chunk = ci->GetChunkInfo();
    if (req.chunkVersion == chunk->chunkVersion &&
            ! fa->IsStriped() && fa->filesize < 0 && KFS_FILE == fa->type &&
            ! fa->IsSymLink() &&
            fa->nextChunkOffset() <= chunk->offset + (chunkOff_t)CHUNKSIZE &&
            0 != fa->numReplicas &&
            metatree.getChunkDeleteQueue() != fa) {
        return true;
    }
    req.status = -EINVAL;
    return false;
}

void
LayoutManager::Handle(MetaChunkSize& req)
{
    if (req.chunkVersion < 0) {
        req.status = -ENOENT; // Object store block.
        return;
    }
    if (req.checkChunkFlag) {
        if (req.logseq.IsValid()) {
            panic("chunk size: check chunk only: invalid log sequence");
            req.status = -EFAULT;
            return;
        }
        if (0 == req.status && req.server && 0 < req.chunkId) {
            MetaOp const types[] = {
                META_CHUNK_ALLOCATE,
                META_CHUNK_DELETE,
                META_CHUNK_REPLICATE,
                META_CHUNK_VERSCHANGE,
                META_BEGIN_MAKE_CHUNK_STABLE,
                META_CHUNK_MAKE_STABLE,
                META_NUM_OPS_COUNT     // Sentinel
            };
            // If chunk server has the replica that isn't in the map, or
            // chunk version doesn't match, then delete it.
            // Do not consider version roll back, for now, to keep it simple.
            const CSMap::Entry* const ci = mChunkToServerMap.Find(req.chunkId);
            if ((! ci || ! mChunkToServerMap.HasServer(req.server, *ci) ||
                    req.replyChunkVersion !=
                        ci->GetChunkInfo()->chunkVersion) &&
                     ! req.server->IsDown()) {
                if (ci) {
                    Servers          srvs;
                    const chunkOff_t kObjStoreBlockPos = -1;
                    if (0 < GetInFlightChunkOpsCount(
                                req.chunkId, types, kObjStoreBlockPos, &srvs) &&
                            find(srvs.begin(), srvs.end(), req.server) ==
                                srvs.end()) {
                        req.server->NotifyStaleChunk(req.chunkId);
                    }
                } else {
                    req.server->ForceDeleteChunk(req.chunkId);
                }
            }
        }
        return;
    }
    if (0 <= req.status && ! req.logseq.IsValid()) {
        panic("chunk size: invalid log sequence");
        req.status = -EFAULT;
        return;
    }
    if (! req.retryFlag && req.status < 0) {
        return;
    }
    const CSMap::Entry* const ci = mChunkToServerMap.Find(req.chunkId);
    if (! ci) {
        req.status = -ENOENT; // No such chunk, fail.
        return;
    }
    MetaFattr* const           fa    = ci->GetFattr();
    const MetaChunkInfo* const chunk = ci->GetChunkInfo();
    if (fa->IsStriped() || 0 <= fa->filesize || KFS_FILE != fa->type ||
            fa->IsSymLink() ||
            metatree.getChunkDeleteQueue() == fa ||
            chunk->offset + (chunkOff_t)CHUNKSIZE < fa->nextChunkOffset() ||
            0 == fa->numReplicas) {
        KFS_LOG_STREAM_DEBUG <<
            " file: "       << fa->id()              <<
            " size: "       << fa->filesize          <<
            " next pos: "   << fa->nextChunkOffset() <<
            " replicas: "   << fa->numReplicas       <<
            " type: "       << (int)fa->type         <<
            " striped: "    << fa->IsStriped()       <<
            " chunk: "      << chunk->chunkId        <<
            " version: "    << chunk->chunkVersion   <<
            " ignoring:"
            " log: "        << req.logseq           <<
            " "             << req.Show()           <<
        KFS_LOG_EOM;
        req.status = -EINVAL; // No update needed.
        return;
    }
    if (0 <= req.status && req.chunkVersion != chunk->chunkVersion) {
        KFS_LOG_STREAM_DEBUG <<
            "last chunk: "  << chunk->chunkId      <<
            " version: "    << chunk->chunkVersion <<
            " ignoring:"
            " log: "        << req.logseq         <<
            " "             << req.Show()         <<
            " status: "     << req.status         <<
            " msg: "        << req.statusMsg      <<
        KFS_LOG_EOM;
        req.status = -EBADVERS;
    }
    if (req.status < 0) {
        // Do not pay attention to lease, as lease might be discarded, if
        // allocation fails.
        const bool retryFlag = ! req.replayFlag &&
            req.retryFlag && IsChunkStable(req.chunkId);
        KFS_LOG_STREAM(req.replayFlag ?
                MsgLogger::kLogLevelDEBUG :
                MsgLogger::kLogLevelERROR) <<
            "log: "     << req.logseq <<
            " "         << req.Show() <<
            " status: " << req.status <<
            " msg: "    << req.statusMsg <<
            " retry: "  << retryFlag <<
        KFS_LOG_EOM;
        if (! retryFlag) {
            return;
        }
        // Retry the size request with all servers.
        StTmp<Servers> serversTmp(mServers3Tmp);
        Servers&       srvs = serversTmp.Get();
        mChunkToServerMap.GetServers(*ci, srvs);
        for (Servers::const_iterator it = srvs.begin();
                it != srvs.end();
                ++it) {
            if ((*it)->IsDown()) {
                continue;
            }
            const bool retryFlag = false;
            (*it)->GetChunkSize(
                fa->id(), chunk->chunkId, chunk->chunkVersion, retryFlag);
        }
        return;
    }
    chunkOff_t const offset = chunk->offset;
    metatree.setFileSize(fa, offset + req.chunkSize);
    KFS_LOG_STREAM_DEBUG <<
        "file: "            << fa->id() <<
        " chunk: "          << req.chunkId <<
        " version: "        << req.chunkVersion <<
        " size: "           << req.chunkSize <<
        " log: "            << req.logseq <<
        " submitted: "      << req.submitCount <<
        " commit pending: " << req.commitPendingFlag <<
        " status: "         << req.status <<
        " filesize: "       << fa->filesize <<
    KFS_LOG_EOM;
    return;
}

bool
LayoutManager::IsChunkStable(chunkId_t chunkId) const
{
    return (! mNonStableChunks.Find(chunkId));
}

int
LayoutManager::ReplicateChunk(
    CSMap::Entry&                  clli,
    int                            extraReplicas,
    LayoutManager::ChunkPlacement& placement,
    const ChunkRecoveryInfo&       recoveryInfo)
{
    if (extraReplicas <= 0) {
        return 0;
    }
    const MetaFattr* const fa       = clli.GetFattr();
    kfsSTier_t             minSTier = fa->minSTier;
    kfsSTier_t             maxSTier = fa->maxSTier;
    if (! FindStorageTiersRange(minSTier, maxSTier)) {
        return 0;
    }
    bool useServerExcludesFlag = clli.GetFattr()->IsStriped();
    if (! recoveryInfo.HasRecovery()) {
        GetPlacementExcludes(clli, placement);
        if (useServerExcludesFlag &&
                clli.GetFattr()->numReplicas > 1 &&
                placement.GetExcludedRacksCount() +
                extraReplicas > mRacks.size()) {
            // Do not pay attention to other stripes, with
            // replication higher than 1 and insufficient number of
            // racks.
            StTmp<Servers> serversTmp(mServers3Tmp);
            Servers&       servers = serversTmp.Get();
            mChunkToServerMap.GetServers(clli, servers);
            placement.clear();
            placement.ExcludeServerAndRack(
                servers, clli.GetChunkId());
            useServerExcludesFlag = false;
        }
    }
    placement.FindCandidatesForReplication(minSTier, maxSTier);
    const size_t numRacks = placement.GetCandidateRackCount();
    const size_t numServersPerRack = numRacks <= 1 ?
        (size_t)extraReplicas :
        ((size_t)extraReplicas + numRacks - 1) / numRacks;
    // Find candidates other than those that are already hosting the chunk.
    StTmp<Servers>             serversTmp(mServersTmp);
    Servers&                   candidates = serversTmp.Get();
    StTmp<vector<kfsSTier_t> > tiersTmp(mPlacementTiersTmp);
    vector<kfsSTier_t>&        tiers = tiersTmp.Get();
    for (int rem = extraReplicas; ;) {
        const size_t psz = candidates.size();
        for (size_t i = 0; ; ) {
            const ChunkServerPtr cs = placement.GetNext(useServerExcludesFlag);
            if (! cs) {
                break;
            }
            if (placement.IsUsingServerExcludes() && (
                    find(candidates.begin(), candidates.end(), cs) !=
                        candidates.end() ||
                    mChunkToServerMap.HasServer(cs, clli))) {
                continue;
            }
            candidates.push_back(cs);
            tiers.push_back(placement.GetStorageTier());
            if (--rem <= 0 || ++i >= numServersPerRack) {
                break;
            }
        }
        if (rem <= 0 || placement.IsLastAttempt()) {
            break;
        }
        placement.ExcludeServer(candidates.begin() + psz, candidates.end());
        if (! placement.NextRack()) {
            break;
        }
    }
    if (candidates.empty()) {
        KFS_LOG_STREAM_WARN <<
            "can not find replication destination for: <" <<
            clli.GetFileId() << "," << clli.GetChunkId() <<
            "> replicas: " << mChunkToServerMap.ServerCount(clli) <<
            " extra: " << extraReplicas <<
        KFS_LOG_EOM;
        return 0;
    }
    return ReplicateChunk(clli, extraReplicas, candidates, recoveryInfo,
        tiers, maxSTier);
}

int
LayoutManager::ReplicateChunk(
    CSMap::Entry&                 clli,
    int                           extraReplicas,
    const LayoutManager::Servers& candidates,
    const ChunkRecoveryInfo&      recoveryInfo,
    const vector<kfsSTier_t>&     tiers,
    kfsSTier_t                    maxSTier,
    const char*                   reasonMsg,
    bool                          removeReplicaFlag)
{
    // prefer a server that is being retired to the other nodes as
    // the source of the chunk replication
    StTmp<Servers> serversTmp(mServers3Tmp);
    Servers&       servers = serversTmp.Get();
    mChunkToServerMap.GetServers(clli, servers);
    Servers::const_iterator const iter = find_if(
        servers.begin(), servers.end(),
        bind(&ChunkServer::IsEvacuationScheduled, _1, clli.GetChunkId())
    );
    vector<kfsSTier_t>::const_iterator ti = tiers.begin();
    int numDone = 0;
    for (Servers::const_iterator it = candidates.begin();
            numDone < extraReplicas && it != candidates.end();
            ++it) {
        const ChunkServerPtr& c    = *it;
        ChunkServer&          cs   = *c;
        const kfsSTier_t      tier = ti != tiers.end() ? *ti : kKfsSTierUndef;
        // verify that we got good candidates
        if (! removeReplicaFlag &&
                find(servers.begin(), servers.end(), c) != servers.end()) {
            panic("invalid replication candidate");
        }
        if (cs.IsDown() || ! cs.IsConnected()) {
            continue;
        }
        const char*    reason = "none";
        ChunkServerPtr dataServer;
        if (iter != servers.end()) {
            ChunkServer& ds = **iter;
            reason = "evacuation";
            if (recoveryInfo.HasRecovery()) {
                reason     = "evacuation recovery";
                dataServer = c;
            } else if (ds.GetReplicationReadLoad() <
                    mMaxConcurrentReadReplicationsPerNode &&
                    (ds.IsResponsiveServer() ||
                        servers.size() <= 1)) {
                dataServer = *iter;
            }
        } else if (recoveryInfo.HasRecovery()) {
            reason     = "recovery";
            dataServer = c;
        } else {
            reason = "re-replication";
        }
        // if we can't find a retiring server, pick a server that
        // has read b/w available
        for (Servers::const_iterator si = servers.begin();
                ! dataServer && si != servers.end();
                ++si) {
            ChunkServer& ss = **si;
            if (ss.GetReplicationReadLoad() >=
                    mMaxConcurrentReadReplicationsPerNode ||
                    ! ss.IsResponsiveServer()) {
                continue;
            }
            dataServer = *si;
        }
        if (! dataServer) {
            continue;
        }
        KFS_LOG_STREAM_INFO <<
            "starting re-replication:"
            " chunk: "  << clli.GetChunkId() <<
            " from: "   <<
                dataServer->GetServerLocation() <<
            " to: "     <<
                cs.GetServerLocation() <<
            " reason: " << reason <<
            ((reasonMsg && reasonMsg[0]) ? " " : "") <<
                (reasonMsg ? reasonMsg : "") <<
        KFS_LOG_EOM;
        // Do not increment replication read load when starting chunk recovery.
        // Chunk server side recovery logic decides from where to read.
        // With recovery dataServer == &cs here, and the source location in the
        // request will only have meta server port, and empty host name.
        FileRecoveryInFlightCount::iterator recovIt =
            mFileRecoveryInFlightCount.end();
        if (recoveryInfo.HasRecovery() && dataServer == c) {
            if (mClientCSAuthRequiredFlag && cs.GetAuthUid() != kKfsUserNone) {
                recovIt = mFileRecoveryInFlightCount.insert(
                    make_pair(make_pair(cs.GetAuthUid(), clli.GetFileId()), 0)
                ).first;
                recovIt->second++;
            }
        } else {
            dataServer->UpdateReplicationReadLoad(1);
        }
        assert(mNumOngoingReplications >= 0);
        // Bump counters here, completion can be invoked
        // immediately, for example when send fails.
        mNumOngoingReplications++;
        mOngoingReplicationStats->Update(1);
        mTotalReplicationStats->Update(1);
        const CSMap::Entry::State replicationState =
            mChunkToServerMap.GetState(clli);
        if (replicationState == CSMap::Entry::kStateNone ||
                replicationState ==
                CSMap::Entry::kStateCheckReplication) {
            SetReplicationState(clli,
                CSMap::Entry::kStatePendingReplication);
        }
        // Do not count synchronous failures.
        if (cs.ReplicateChunk(clli.GetFileId(), clli.GetChunkId(),
                dataServer, recoveryInfo, tier, maxSTier, recovIt,
                removeReplicaFlag) == 0 && ! cs.IsDown()) {
            numDone++;
        }
    }
    return numDone;
}

bool
LayoutManager::GetPlacementExcludes(
    const CSMap::Entry&            entry,
    LayoutManager::ChunkPlacement& placement,
    bool                           includeThisChunkFlag /* = true */,
    bool                           stopIfHasAnyReplicationsInFlight /* = false */,
    vector<MetaChunkInfo*>*        chunkBlock /* = 0 */)
{
    const MetaFattr* const fa = entry.GetFattr();
    if (! fa->IsStriped()) {
        if (includeThisChunkFlag) {
            StTmp<Servers> serversTmp(mServers3Tmp);
            Servers&       servers = serversTmp.Get();
            mChunkToServerMap.GetServers(entry, servers);
            placement.ExcludeServerAndRack(
                servers, entry.GetChunkId());
        }
        return true;
    }
    StTmp<vector<MetaChunkInfo*> > cinfoTmp(mChunkInfosTmp);
    const MetaChunkInfo* const     chunk  = entry.GetChunkInfo();
    chunkOff_t                     start  = -1;
    MetaFattr*                     mfa    = 0;
    MetaChunkInfo*                 mci    = 0;
    chunkOff_t                     offset = chunk->offset;
    vector<MetaChunkInfo*>&        cblk   =
        chunkBlock ? *chunkBlock : cinfoTmp.Get();
    cblk.reserve(fa->numStripes + fa->numRecoveryStripes);
    if (cblk.empty() &&
            (metatree.getalloc(fa->id(), offset,
                mfa, mci, &cblk, &start) != 0 ||
            mfa != fa || mci != chunk)) {
        panic("chunk mapping / getalloc mismatch");
        return false;
    }
    StTmp<Servers> serversTmp(mServers3Tmp);
    for (vector<MetaChunkInfo*>::const_iterator it = cblk.begin();
            it != cblk.end();
            ++it) {
        if (! includeThisChunkFlag && chunk == *it) {
            continue;
        }
        Servers&            servers = serversTmp.Get();
        const CSMap::Entry& ce      = GetCsEntry(**it);
        mChunkToServerMap.GetServers(ce, servers);
        if (chunk != *it) {
            if (stopIfHasAnyReplicationsInFlight &&
                    mChunkToServerMap.GetState(ce) !=
                        CSMap::Entry::kStateNone) {
                return false;
            }
            const chunkOff_t kObjStoreBlockPos = -1;
            if (GetInFlightChunkModificationOpCount(
                    (*it)->chunkId,
                    kObjStoreBlockPos,
                    stopIfHasAnyReplicationsInFlight ?
                        0 : &servers
                    ) > 0 &&
                    stopIfHasAnyReplicationsInFlight) {
                return false; // Early termination -- ignore the rest
            }
        }
        placement.ExcludeServerAndRack(servers, ce.GetChunkId());
    }
    return true;
}

bool
LayoutManager::CanReplicateChunkNow(
    CSMap::Entry&                  c,
    int&                           extraReplicas,
    LayoutManager::ChunkPlacement& placement,
    int*                           hibernatedReplicaCount /* = 0 */,
    ChunkRecoveryInfo*             recoveryInfo           /* = 0 */,
    bool                           forceRecoveryFlag      /* = false */)
{
    extraReplicas = 0;
    if (hibernatedReplicaCount) {
        *hibernatedReplicaCount = 0;
    }
    if (recoveryInfo) {
        recoveryInfo->Clear();
    }
    if (! mPrimaryFlag) {
        if (recoveryInfo) {
            SetReplicationState(c, CSMap::Entry::kStatePendingReplication);
        }
        return false;
    }
    const MetaFattr* const fa      = c.GetFattr();
    const chunkId_t        chunkId = c.GetChunkId();
    // Don't replicate chunks for which a write lease has been
    // issued.
    const ChunkLeases::WriteLease* const wl =
        mChunkLeases.GetChunkWriteLease(chunkId);
    if (wl) {
        KFS_LOG_STREAM_DEBUG <<
            "re-replication delayed chunk:"
            " <" << c.GetFileId() << "," << chunkId << ">"
            " " << (TimeNow() <= wl->expires ?
                "valid" : "expired") <<
            " write lease exists" <<
        KFS_LOG_EOM;
        if (recoveryInfo) {
            SetReplicationState(c, CSMap::Entry::kStatePendingReplication);
        }
        return false;
    }
    if (! IsChunkStable(chunkId)) {
        KFS_LOG_STREAM_DEBUG <<
            "re-replication delayed chunk:"
            " <" << c.GetFileId() << "," << chunkId << ">"
            " is not stable yet" <<
        KFS_LOG_EOM;
        if (recoveryInfo) {
            SetReplicationState(c, CSMap::Entry::kStatePendingReplication);
        }
        return false;
    }
    if (mChunkLeases.IsDeletePending(c.GetFileId())) {
        KFS_LOG_STREAM_DEBUG <<
            "re-replication delayed chunk:"
            " <" << c.GetFileId() << "," << chunkId << ">"
            " file delete pending" <<
        KFS_LOG_EOM;
        return false;
    }
    if (metatree.getChunkDeleteQueue() == c.GetFattr()) {
        KFS_LOG_STREAM_DEBUG <<
            "re-replication delayed chunk:"
            " <" << c.GetFileId() << "," << chunkId << ">"
            " chunk is being deleted" <<
        KFS_LOG_EOM;
        return false;
    }
    const MetaChunkInfo* const chunk             = c.GetChunkInfo();
    size_t                     hibernatedCount   = 0;
    size_t                     disconnectedCount = 0;
    StTmp<Servers>             serversTmp(mServers3Tmp);
    Servers&                   servers = serversTmp.Get();
    mChunkToServerMap.GetConnectedServers(
        c, servers, hibernatedCount, disconnectedCount);
    if (hibernatedReplicaCount) {
        *hibernatedReplicaCount = (int)hibernatedCount;
    }
    if (forceRecoveryFlag ||
            (servers.empty() &&
                (! recoveryInfo || hibernatedCount <= 0)) ||
            (mUseEvacuationRecoveryFlag &&
                recoveryInfo &&
                servers.size() == 1 &&
                servers.front()->GetReplicationReadLoad() >=
                    mMaxConcurrentReadReplicationsPerNode &&
                servers.front()->IsEvacuationScheduled(chunkId) &&
                fa->numReplicas == 1 &&
                fa->HasRecovery())) {
        if (! recoveryInfo || ! fa->HasRecovery()) {
            if (recoveryInfo) {
                KFS_LOG_STREAM_DEBUG <<
                    "can not re-replicate chunk:"
                    " <" << c.GetFileId() << "," << chunkId << ">"
                    " no copies left,"
                    " canceling re-replication" <<
                KFS_LOG_EOM;
                SetReplicationState(c, CSMap::Entry::kStatePendingReplication);
            }
            return false;
        }
        StTmp<vector<MetaChunkInfo*> > cinfoTmp(mChunkInfosTmp);
        vector<MetaChunkInfo*>& cblk = cinfoTmp.Get();
        cblk.reserve(fa->numStripes + fa->numRecoveryStripes);
        chunkOff_t     start  = -1;
        MetaFattr*     mfa    = 0;
        MetaChunkInfo* mci    = 0;
        chunkOff_t     offset = chunk->offset;
        if (metatree.getalloc(fa->id(), offset,
                    mfa, mci, &cblk, &start) != 0 ||
                mfa != fa || mci != chunk) {
            panic("chunk mapping / getalloc mismatch");
            return false;
        }
        const chunkOff_t end       = start + fa->ChunkBlkSize();
        int              good      = 0;
        int              notStable = 0;
        unsigned int     stripeIdx = 0;
        bool             holeFlag  = false;
        vector<MetaChunkInfo*>::const_iterator it = cblk.begin();
        StTmp<Servers> serversTmp(mServers4Tmp);
        for (chunkOff_t pos = start;
                pos < end;
                pos += (chunkOff_t)CHUNKSIZE, stripeIdx++) {
            if (it == cblk.end()) {
                notStable = -1;
                break; // incomplete chunk block.
            }
            assert((*it)->offset % CHUNKSIZE == 0);
            if (pos < (*it)->offset) {
                if (fa->numStripes <= stripeIdx) {
                    // No recovery: incomplete chunk block.
                    notStable = -1;
                    break;
                }
                good++;
                holeFlag = true;
                continue; // no chunk -- hole.
            }
            if (holeFlag && stripeIdx < fa->numStripes) {
                // No prior stripes, incomplete chunk block.
                notStable = -1;
                break;
            }
            const chunkId_t curChunkId = (*it)->chunkId;
            if (mChunkLeases.GetChunkWriteLease(curChunkId) ||
                    ! IsChunkStable(curChunkId)) {
                notStable++;
                break;
                // MakeChunkStableDone will restart
                // re-replication.
            }
            Servers&            srvs = serversTmp.Get();
            const CSMap::Entry& ce   = GetCsEntry(**it);
            if (mChunkToServerMap.GetConnectedServers(ce, srvs) > 0) {
                good++;
            }
            if (chunkId != curChunkId) {
                const chunkOff_t kObjStoreBlockPos = -1;
                GetInFlightChunkModificationOpCount(
                    curChunkId, kObjStoreBlockPos, &srvs);
            }
            placement.ExcludeServerAndRack(srvs, curChunkId);
            ++it;
        }
        if (notStable > 0 ||
                (notStable == 0 && good < (int)fa->numStripes)) {
            if (! servers.empty()) {
                // Can not use recovery instead of replication.
                SetReplicationState(c, CSMap::Entry::kStateNoDestination);
                return false;
            }
            // Ensure that all pending recovery chunks in this block
            // are adjacent in the the recovery list.
            // UpdatePendingRecovery() depends on this.
            int       pendingCnt   = 0;
            chunkId_t firstPending = -1;
            for (it = cblk.begin(); it != cblk.end(); ++it) {
                chunkId_t const chunkId = (*it)->chunkId;
                CSMap::Entry&   ci      = GetCsEntry(**it);
                if (! mChunkToServerMap.HasServers(ci) ||
                        mChunkLeases.GetChunkWriteLease(chunkId) ||
                        ! IsChunkStable(chunkId)) {
                    mChunkToServerMap.SetState(ci,
                        CSMap::Entry::kStatePendingRecovery);
                    pendingCnt++;
                    if (&ci != &c && firstPending <= 0) {
                        firstPending = chunkId;
                    }
                }
            }
            const int64_t totalCnt = mChunkToServerMap.GetCount(
                CSMap::Entry::kStatePendingRecovery);
            KFS_LOG_STREAM(
                totalCnt < mMaxPendingRecoveryMsgLogInfo ?
                    MsgLogger::kLogLevelINFO :
                    MsgLogger::kLogLevelDEBUG) <<
                "recovery pending:"
                " <" << fa->id() << "," << chunkId << ">"
                " chunks: available: "  << good <<
                " stripe: "             << stripeIdx <<
                " required: "           << fa->numStripes <<
                " added: "              << pendingCnt <<
                " total: "              << totalCnt <<
                " not stable: "         << notStable <<
                " other chunk: "        << firstPending <<
                " block:"
                " chunks: "             << cblk.size() <<
                " pos: "                << start <<
                " size: "               << (end - start) <<
            KFS_LOG_EOM;
            return false;
        }
        // Temporary paper over: presently the client lib doesn't
        // re-invalidate chunks when all leases expire withing the chunk
        // group it writes into.
        // For example: no write activity for more than 5 min,
        // or client doesn't get scheduled on cpu because os thrashing,
        // or there is connectivity problem.
        // For now delay recovery of the chunk in the blocks past of
        // logical EOF, until the client updates EOF. This is needed to
        // prevent recovery from starting "too soon" and creating a
        // potentially bogus chunk.
        // Obviously this has no effect with re-write, when chunk block
        // position is less than logical EOF, but re-write isn't fully
        // supported with striped files.
        const int64_t timeMicrosec = (int64_t)TimeNow() * kSecs2MicroSecs;
        if (notStable != 0 || (fa->filesize <=
                fa->ChunkPosToChunkBlkFileStartPos(start) &&
                timeMicrosec < fa->mtime +
                    mPastEofRecoveryDelay)) {
            if (! servers.empty()) {
                // Cannot use recovery instead of replication.
                SetReplicationState(c, CSMap::Entry::kStateNoDestination);
                return false;
            }
            KFS_LOG_STREAM_INFO <<
                "recovery:"
                " <" << fa->id() << "," << chunkId << ">"
                " file size: "   << fa->filesize <<
                " fblk pos: "    <<
                    fa->ChunkPosToChunkBlkFileStartPos(start) <<
                " chunk off: "   << start <<
                " not stable: "  << notStable <<
                " mtime: "       <<
                    ((timeMicrosec - fa->mtime) * 1e-6) <<
                    " sec. ago"
                " pending recovery: "  <<
                    mChunkToServerMap.GetCount(
                        CSMap::Entry::kStateDelayedRecovery) <<
                " delaying recovery by: " <<
                    ((fa->mtime + mPastEofRecoveryDelay -
                        timeMicrosec) * 1e-6) <<
                    " sec." <<
            KFS_LOG_EOM;
            SetReplicationState(c, CSMap::Entry::kStateDelayedRecovery);
            return false;
        }
        recoveryInfo->offset             = chunk->offset;
        recoveryInfo->version            = chunk->chunkVersion;
        recoveryInfo->striperType        = fa->striperType;
        recoveryInfo->numStripes         = fa->numStripes;
        recoveryInfo->numRecoveryStripes = fa->numRecoveryStripes;
        recoveryInfo->stripeSize         = fa->stripeSize;
        recoveryInfo->fileSize           = fa->filesize;
    }
    // if any of the chunkservers are retiring, we need to make copies
    // so, first determine how many copies we need because one of the
    // servers hosting the chunk is going down
    // May need to re-replicate this chunk:
    //    - extraReplicas > 0 means make extra copies;
    //    - extraReplicas == 0, take out this chunkid from the candidate set
    //    - extraReplicas < 0, means we got too many copies; delete some
    const int numRetiringServers = (int)count_if(
        servers.begin(), servers.end(),
        bind(&ChunkServer::IsEvacuationScheduled, _1, chunkId));
    // now, determine if we have sufficient copies
    // we need to make this many copies: # of servers that are
    // retiring plus the # this chunk is under-replicated
    extraReplicas = fa->numReplicas + numRetiringServers -
        (int)servers.size();
    // Do not delete evacuated / retired replicas until there is sufficient
    // number of replicas, then delete all extra copies at once.
    // Take into the account hibernated servers, delay the re-replication /
    // recovery until the hibernated servers are removed.
    // For now count hibernated server only in the case if
    if (extraReplicas <= 0) {
        extraReplicas -= numRetiringServers;
    } else if (recoveryInfo && (int)hibernatedCount <= extraReplicas) {
        extraReplicas -= hibernatedCount;
    }
    //
    // If additional copies need to be deleted, check if there is a valid
    // (read) lease issued on the chunk. In case if lease exists leave the
    // chunk alone for now; we'll look at deleting it when the lease has
    // expired.  This is for safety: if a client was reading from the copy
    // of the chunk that we are trying to delete, the client will see the
    // deletion and will have to failover; avoid unnecessary failovers
    //
    const bool readLeaseWaitFlag = recoveryInfo && extraReplicas < 0 &&
        mChunkLeases.UpdateReadLeaseReplicationCheck(chunkId, true);
    // Chunk to server map is updated after stale notify was successfully
    // appended to the transaction log. Handle possible out of date map entry by
    // delaying / rescheduling extra replicas removal if / when any stale notify
    // for this chunk is in flight.
    // In theory delete is redundant in the pending op list below at the moment,
    // as delete should not be issued to remove extra replicas. Is is in the list
    // for additional safety / in case if this changes in the future.
    MetaOp const kPendingOpTypes[] = {
        META_CHUNK_STALENOTIFY,
        META_CHUNK_DELETE,
        META_NUM_OPS_COUNT // Sentinel
    };
    const bool delExtraReplWaitFlag = readLeaseWaitFlag || (extraReplicas < 0 &&
        0 < GetInFlightChunkOpsCount(chunkId, kPendingOpTypes));
    KFS_LOG_STREAM_DEBUG <<
        "re-replicate: chunk:"
        " <" << c.GetFileId() << "," << chunkId << ">"
        " version: "    << chunk->chunkVersion <<
        " offset: "     << chunk->offset <<
        " eof: "        << fa->filesize <<
        " replicas: "   << servers.size() <<
        " retiring: "   << numRetiringServers <<
        " target: "     << fa->numReplicas <<
        " rlease: "     << readLeaseWaitFlag <<
        " wait: "       << delExtraReplWaitFlag <<
        " hibernated: " << hibernatedCount <<
        " needed: "     << extraReplicas <<
    KFS_LOG_EOM;
    if (delExtraReplWaitFlag) {
        SetReplicationState(c, CSMap::Entry::kStatePendingReplication);
        return false;
    }
    return true;
}

void
LayoutManager::CheckHibernatingServersStatus()
{
    if (! mPrimaryFlag) {
        return;
    }
    const int    recoveryFlag = InRecovery();
    const time_t now          = TimeNow();
    RequestQueue queue;
    for (HibernatedServerInfos::iterator iter = mHibernatingServers.begin();
            iter != mHibernatingServers.end();
            ++iter) {
        if (iter->removeOp) {
            continue;
        }
        Servers::const_iterator const it = FindServer(iter->location);
        if (it == mChunkServers.end() && now < iter->sleepEndTime) {
            // Within the time window.
            continue;
        }
        if (it != mChunkServers.end()) {
            if (iter->IsHibernated()) {
                // Must be removed by hello processing in AddNewServer().
                panic("invalid stale hibernated server info");
            } else if ((*it)->IsConnected()) {
                if (now < iter->sleepEndTime + 5 * 60) {
                    continue;
                }
                KFS_LOG_STREAM_INFO <<
                    "hibernated server: " << iter->location  <<
                    " still connected, canceling hibernation" <<
                KFS_LOG_EOM;
            } else {
                KFS_LOG_STREAM_DEBUG <<
                    "hibernated server: " << iter->location  <<
                    " is not connected, skipping" <<
                KFS_LOG_EOM;
                // Wait for bye to go trough the log.
                continue;
            }
        } else {
            // Server hasn't come back as promised, initiate
            // re-replication check for the blocks that were on that node.
            KFS_LOG_STREAM(recoveryFlag ?
                    MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
                "hibernated server: " << iter->location <<
                " is NOT back as promised" <<
            KFS_LOG_EOM;
            if (recoveryFlag) {
                continue;
            }
        }
        iter->removeOp = new MetaHibernatedRemove(iter->location);
        queue.PushBack(*iter->removeOp);
    }
    MetaRequest* req;
    while ((req = queue.PopFront())) {
        submit_request(req);
    }
}

int
LayoutManager::CountServersAvailForReReplication(
    size_t& pendingHelloCnt, size_t& connectedCnt) const
{
    pendingHelloCnt = 0;
    connectedCnt = GetConnectedServerCount();
    if (connectedCnt <= 0) {
        return 0;
    }
    int anyAvail = 0;
    for (Servers::const_iterator it = mChunkServers.begin();
            mChunkServers.end() != it;
            ++it) {
        const ChunkServer& cs = **it;
        if (! cs.IsConnected()) {
            continue;
        }
        if (cs.IsHelloNotifyPending()) {
            pendingHelloCnt++;
        }
        if (mMaxSpaceUtilizationThreshold <
                cs.GetSpaceUtilization(mUseFsTotalSpaceFlag)) {
            continue;
        }
        if (mMaxConcurrentWriteReplicationsPerNode <=
                cs.GetNumChunkReplications()) {
            continue;
        }
        anyAvail++;
    }
    return anyAvail;
}

bool
LayoutManager::HandoutChunkReplicationWork()
{
    // There is a set of chunks that are affected: their server went down
    // or there is a change in their degree of replication.  in either
    // case, walk this set of chunkid's and work on their replication amount.

    // List of in flight ops to transition chunk to pending list.
    // Completion of any op in this list must transition chunk from this
    // list (usually by invoking UpdateReplicationState()) as the list is
    // not scanned by the timer.
    MetaOp const kPendingOpTypes[] = {
        META_CHUNK_REPLICATE,
        META_CHUNK_VERSCHANGE,
        META_CHUNK_MAKE_STABLE,
        // Possible extra replicas delete:
        META_CHUNK_DELETE, META_CHUNK_STALENOTIFY,
        META_NUM_OPS_COUNT // Sentinel
    };

    int64_t   now          = microseconds();
    int64_t   endTime      = now;
    const int kCheckTime   = 32;
    int       pass         = kCheckTime;
    int64_t   start        = now;
    int64_t   count        = 0;
    int64_t   doneCount    = 0;
    int64_t   loopCount    = 0;
    int       avail        = 0;
    bool      timedOutFlag = false;
    if (now <= mLastReplicationCheckRunEndTime +
            mMinChunkReplicationCheckInterval) {
        int64_t kMinInterval = 500;
        if (mLastReplicationCheckRunEndTime + kMinInterval < now) {
            endTime += kMinInterval;
        } else {
            pass = 4;
        }
    } else {
        endTime += mMaxTimeForChunkReplicationCheck;
    }
    ChunkRecoveryInfo     recoveryInfo;
    StTmp<ChunkPlacement> placementTmp(mChunkPlacementTmp);
    bool nextRunLowPriorityFlag = false;
    mChunkToServerMap.First(CSMap::Entry::kStateCheckReplication);
    for (; mPrimaryFlag; loopCount++) {
        if (--pass <= 0) {
            now  = microseconds();
            pass = kCheckTime;
            if (endTime <= now) {
                mReplicationCheckTimeouts++;
                if (nextRunLowPriorityFlag) {
                    break;
                }
                timedOutFlag = true;
                mChunkReplicator.ScheduleNext();
                const int64_t kMsgInterval =
                    2 * kSecs2MicroSecs;
                if (now < mLastReplicationCheckRunEndTime +
                        kMsgInterval) {
                    break;
                }
                KFS_LOG_STREAM_INFO <<
                    "exiting replication check:"
                     " time spent: " << (now - start) <<
                        " microsec" <<
                     " timeouts: "   <<
                        mReplicationCheckTimeouts <<
                     " candidates: " <<
                        mChunkToServerMap.GetCount(
                    CSMap::Entry::kStateCheckReplication) <<
                     " initiated: "  << count <<
                     " done: "       << doneCount <<
                     " loop: "       << loopCount <<
                KFS_LOG_EOM;
                break;
            }
        }
        size_t pendingHelloCnt = 0;
        size_t connectedCnt    = 0;
        if (avail <= 0 && ((avail = CountServersAvailForReReplication(
                    pendingHelloCnt, connectedCnt)) <= 0 ||
                connectedCnt <
                        mMinChunkserversToExitRecovery + pendingHelloCnt)) {
            if (count <= 0) {
                mNoServersAvailableForReplicationCount++;
            }
            KFS_LOG_STREAM(count <= 0 ?
                    MsgLogger::kLogLevelERROR : MsgLogger::kLogLevelINFO) <<
                "exiting replication check:"
                " no servers available for replication:"
                " "                << mNoServersAvailableForReplicationCount <<
                " replications"
                " started: "       << count <<
                " servers:"
                " total: "         << mChunkServers.size() <<
                " available: "     << avail <<
                " pending hello: " << pendingHelloCnt <<
                " connected: "     << connectedCnt <<
                " replay: "        << mReplayServerCount <<
                " disconnected: "  << mDisconnectedCount <<
                " min servers: "   << mMinChunkserversToExitRecovery <<
            KFS_LOG_EOM;
            break;
        }
        CSMap::Entry* cur = mChunkToServerMap.Next(
            CSMap::Entry::kStateCheckReplication);
        if (! cur) {
            // See if all chunks check was requested.
            if (! (cur = mChunkToServerMap.Next(CSMap::Entry::kStateNone))) {
                mCheckAllChunksInProgressFlag = false;
                nextRunLowPriorityFlag = true;
                if (! (cur = mChunkToServerMap.Next(
                            CSMap::Entry::kStateNoDestination)) &&
                        ! (cur = mChunkToServerMap.Next(
                            CSMap::Entry::kStateDelayedRecovery))) {
                    mChunkToServerMap.First(CSMap::Entry::kStateNoDestination);
                    mChunkToServerMap.First(
                        CSMap::Entry::kStateDelayedRecovery);
                    break; // Done.
                }
            }
            // Move to the replication list.
            mChunkToServerMap.SetState(
                *cur, CSMap::Entry::kStateCheckReplication);
        }
        CSMap::Entry& entry = *cur;

        if (0 < GetInFlightChunkOpsCount(entry.GetChunkId(), kPendingOpTypes)) {
            // This chunk is being re-replicated, or in transition.
            // Replication check will get scheduled again when the
            // corresponding op completes.
            SetReplicationState(entry,
                CSMap::Entry::kStatePendingReplication);
            continue;
        }
        int extraReplicas          = 0;
        int hibernatedReplicaCount = 0;
        recoveryInfo.Clear();
        ChunkPlacement& placement = placementTmp.Get();
        if (! CanReplicateChunkNow(
                entry,
                extraReplicas,
                placement,
                &hibernatedReplicaCount,
                &recoveryInfo)) {
            continue;
        }
        if (extraReplicas > 0) {
            const int numStarted = ReplicateChunk(
                entry,
                extraReplicas,
                placement,
                recoveryInfo);
            if (numStarted <= 0) {
                SetReplicationState(entry, CSMap::Entry::kStateNoDestination);
            }
            count += numStarted;
            avail -= numStarted;
        } else {
            if (extraReplicas < 0) {
                DeleteAddlChunkReplicas(entry, -extraReplicas, placement);
            }
            if (hibernatedReplicaCount <= 0) {
                // Sufficient replicas, now no need to make
                // chunk stable.
                CancelPendingMakeStable(
                    entry.GetFileId(), entry.GetChunkId());
            }
            SetReplicationState(entry, CSMap::Entry::kStateNone);
            doneCount++;
        }
        if (mNumOngoingReplications > (int64_t)mChunkServers.size() *
                mMaxConcurrentWriteReplicationsPerNode) {
            // throttle...we are handing out
            break;
        }
    }
    mLastReplicationCheckRunEndTime = pass == kCheckTime ? now : microseconds();
    return timedOutFlag;
}

void LayoutManager::Timeout()
{
    ScheduleCleanup(mMaxServerCleanupScan);
}

void LayoutManager::ScheduleCleanup(size_t maxScanCount /* = 1 */)
{
    if (! gNetDispatch.IsRunning()) {
        // Perform full cleanup in replay.
        mChunkToServerMap.RemoveServerCleanup(0);
        mCleanupScheduledFlag = false;
        return;
    }
    if (mChunkToServerMap.RemoveServerCleanup(maxScanCount)) {
        if (! mCleanupScheduledFlag) {
            mCleanupScheduledFlag = true;
            mNetManager.RegisterTimeoutHandler(this);
        }
        mNetManager.Wakeup();
    } else {
        if (mCleanupScheduledFlag) {
            mCleanupScheduledFlag = false;
            mNetManager.UnRegisterTimeoutHandler(this);
        }
    }
}

struct EvacuateChunkChecker
{
    bool& mRetiringServersFlag;
    EvacuateChunkChecker(bool& flag)
            : mRetiringServersFlag(flag) {
        mRetiringServersFlag = false;
    }
    void operator()(const ChunkServerPtr& c) const {
        if (! c->IsRetiring()) {
            return;
        }
        // Until the server disconnects, even if it has no chunks,
        // set the flag.
        mRetiringServersFlag = true;
        if (c->GetChunkCount() > 0) {
            return;
        }
        c->Retire();
    }
};

void
LayoutManager::ChunkReplicationChecker()
{
    if (! mPrimaryFlag) {
        ScheduleCleanup(mMaxServerCleanupScan);
        return;
    }
    if (! mPendingBeginMakeStable.IsEmpty() && ! InRecoveryPeriod()) {
        ProcessPendingBeginMakeStable();
    }
    const bool    recoveryFlag  = InRecovery();
    const int64_t now           = microseconds();
    const bool    fullCheckFlag = mCompleteReplicationCheckTime +
        mCompleteReplicationCheckInterval <= now;
    if (fullCheckFlag) {
        mCompleteReplicationCheckTime = now;
        CheckHibernatingServersStatus();
    }
    if (mLastReplicationCheckTime + mFullReplicationCheckInterval <= now) {
        KFS_LOG_STREAM_INFO <<
            "Initiating a replication check of all chunks" <<
        KFS_LOG_EOM;
        InitCheckAllChunks();
        mLastReplicationCheckTime = now;
    }
    const bool runRebalanceFlag =
        ! recoveryFlag &&
        ! HandoutChunkReplicationWork() &&
        ! mCheckAllChunksInProgressFlag;
    if (fullCheckFlag) {
        if (mMightHaveRetiringServersFlag) {
            // Chunk deletion does not initiate retiring, and retire
            // isn't completely reliable -- notification only.
            // Tell servers to retire if they are still here.
            for_each(mChunkServers.begin(), mChunkServers.end(),
                EvacuateChunkChecker(mMightHaveRetiringServersFlag));
        }
    }
    if (! RunObjectBlockDeleteQueue() && runRebalanceFlag &&
            (mIsRebalancingEnabled || mIsExecutingRebalancePlan) &&
            mLastRebalanceRunTime + mRebalanceRunInterval <= now) {
        mLastRebalanceRunTime = now;
        RebalanceServers();
    }
    mReplicationTodoStats->Set(mChunkToServerMap.GetCount(
        CSMap::Entry::kStateCheckReplication));
    ScheduleCleanup(mMaxServerCleanupScan);
}

void
LayoutManager::Handle(MetaChunkReplicate& req)
{
    const bool versChangeDoneFlag = req.versChange != 0;
    assert(! req.suspended || versChangeDoneFlag);
    if (versChangeDoneFlag) {
        if (! req.suspended) {
            req.versChange = 0;
            return;
        }
        assert(! req.versChange->clnt);
        req.suspended = false;
        req.status    = req.versChange->status;
        req.statusMsg = req.versChange->statusMsg;
    }
    if (req.recovIt != mFileRecoveryInFlightCount.end()) {
        assert(0 < req.recovIt->second);
        if (--(req.recovIt->second) <= 0) {
            mFileRecoveryInFlightCount.erase(req.recovIt);
        }
        req.recovIt = mFileRecoveryInFlightCount.end();
    }

    // In the recovery case the source location's host name is empty.
    const bool replicationFlag = req.srcLocation.IsValid();
    KFS_LOG_STREAM_INFO <<
        (versChangeDoneFlag ? "version change" :
            (replicationFlag ? "replication" : "recovery")) <<
            " done:"
        " chunk: "      << req.chunkId <<
        " version: "    << req.chunkVersion <<
        " status: "     << req.status <<
        (req.statusMsg.empty() ? "" : " ") << req.statusMsg <<
        " server: " << req.server->GetServerLocation() <<
        " " << (req.server->IsDown() ? "down" : "OK") <<
        " replications in flight: " << mNumOngoingReplications <<
    KFS_LOG_EOM;

    if (! versChangeDoneFlag) {
        mOngoingReplicationStats->Update(-1);
        assert(0 < mNumOngoingReplications);
        mNumOngoingReplications--;
        req.server->ReplicateChunkDone(req.chunkId);
        if (replicationFlag && req.dataServer) {
            req.dataServer->UpdateReplicationReadLoad(-1);
        }
        req.dataServer.reset();
    }

    // Since this server is now free,
    // schedule chunk replication scheduler to run.
    if ((((int64_t)mChunkToServerMap.GetCount(
                CSMap::Entry::kStateCheckReplication) > 0 ||
            (int64_t)mChunkToServerMap.GetCount(
                CSMap::Entry::kStateNoDestination) >
            (int64_t)mChunkServers.size() *
                mMaxConcurrentWriteReplicationsPerNode) &&
            (int64_t)mNumOngoingReplications * 5 / 4 <
            (int64_t)mChunkServers.size() *
                mMaxConcurrentWriteReplicationsPerNode) ||
            (req.server->GetNumChunkReplications() * 5 / 4 <
                mMaxConcurrentWriteReplicationsPerNode &&
            ! req.server->IsHibernatingOrRetiring() &&
            ! req.server->IsDown())) {
        mChunkReplicator.ScheduleNext();
    }

    CSMap::Entry* const ci = mChunkToServerMap.Find(req.chunkId);
    if (! ci) {
        KFS_LOG_STREAM_INFO <<
            "chunk " << req.chunkId <<
            " mapping no longer exists" <<
        KFS_LOG_EOM;
        if (0 == req.status || ! req.staleChunkIdFlag) {
            req.server->ForceDeleteChunk(req.chunkId);
        }
        return;
    }
    if (0 != req.status || req.server->IsDown()) {
        // Replication failed...we will try again later
        const fid_t fid = ci->GetFileId();
        KFS_LOG_STREAM_INFO <<
            req.server->GetServerLocation() <<
            ": re-replication failed"
            " chunk: "           << req.chunkId <<
            " fid: "             << req.fid <<
            "/"                  << fid <<
            " status: "          << req.status <<
            " in flight: "       <<
                GetInFlightChunkOpsCount(
                    req.chunkId, META_CHUNK_REPLICATE) <<
            " invalid stripes: " << req.invalidStripes.size() <<
        KFS_LOG_EOM;
        mFailedReplicationStats->Update(1);
        UpdateReplicationState(*ci);
        // Send stale chunk notification if setver is still up, and
        // status isn't EEXIST. The EEXIST is used by the chunk server to
        // for chunks that exist but aren't communicated to the meta server
        // yet.
        if (-EEXIST == req.status || req.server->IsDown()) {
            return;
        }
        if (! versChangeDoneFlag && fid == req.fid) {
            ProcessInvalidStripes(req);
        }
        if (! req.staleChunkIdFlag) {
            // Send stale chunk notification, unless it was already scheduled by
            // chunk log completion.
            req.server->NotifyStaleChunk(req.chunkId);
        }
        if (! replicationFlag || req.server->IsDown() || versChangeDoneFlag) {
            return;
        }
        const MetaFattr* const fa = ci->GetFattr();
        if (fa->HasRecovery() && mChunkToServerMap.ServerCount(*ci) == 1) {
            KFS_LOG_STREAM_INFO <<
                "chunk: " << req.chunkId <<
                " fid: "  << req.fid <<
                " / "     << fid <<
                " attempting to use recovery"
                " instead of replication" <<
            KFS_LOG_EOM;
            const bool        kForceRecoveryFlag     = true;
            int               extraReplicas          = 0;
            int               hibernatedReplicaCount = 0;
            ChunkRecoveryInfo recoveryInfo;
            recoveryInfo.Clear();
            StTmp<ChunkPlacement> placementTmp(mChunkPlacementTmp);
            ChunkPlacement&       placement = placementTmp.Get();
            if (GetInFlightChunkModificationOpCount(
                        req.chunkId) <= 0 &&
                    CanReplicateChunkNow(
                        *ci,
                        extraReplicas,
                        placement,
                        &hibernatedReplicaCount,
                        &recoveryInfo,
                        kForceRecoveryFlag) &&
                    extraReplicas > 0 &&
                    ReplicateChunk(
                        *ci,
                        extraReplicas,
                        placement,
                        recoveryInfo) <= 0) {
                SetReplicationState(*ci, CSMap::Entry::kStateNoDestination);
            }
        }
        return;
    }
    // replication succeeded: book-keeping
    // validate that the server got the latest copy of the chunk
    const MetaChunkInfo* const chunk = ci->GetChunkInfo();
    if (chunk->chunkVersion != req.chunkVersion) {
        // Version that we replicated has changed...so, stale
        KFS_LOG_STREAM_INFO <<
            req.server->GetServerLocation() <<
            " re-replicate: chunk " << req.chunkId <<
            " version changed was: " << req.chunkVersion <<
            " now " << chunk->chunkVersion << " => stale" <<
        KFS_LOG_EOM;
        mFailedReplicationStats->Update(1);
        UpdateReplicationState(*ci);
        req.server->NotifyStaleChunk(req.chunkId);
        return;
    }
    if (! replicationFlag && ! versChangeDoneFlag) {
        const fid_t fid = ci->GetFileId();
        if (fid != req.fid) {
            KFS_LOG_STREAM_INFO <<
                req.server->GetServerLocation() <<
                " recover: chunk " << req.chunkId <<
                " file id changed:"
                " was: "  << req.fid <<
                " now: "  << fid << " => stale" <<
            KFS_LOG_EOM;
            UpdateReplicationState(*ci);
            req.server->NotifyStaleChunk(req.chunkId);
            return;
        }
        req.suspended = true;
        const bool kMakeStableFlag = true;
        const bool kPendingAddFlag = true; // Tell replay to add chunk.
        req.server->NotifyChunkVersChange(
            req.fid,
            req.chunkId,
            req.chunkVersion, // to
            0,                // from
            kMakeStableFlag,
            kPendingAddFlag,
            &req
        );
        return;
    }
    UpdateReplicationState(*ci);
    if (! mChunkToServerMap.HasServer(req.server, *ci)) {
        KFS_LOG_STREAM_ERROR <<
            req.server->GetServerLocation() <<
            " chunk: " << req.chunkId <<
            (replicationFlag ? " re-replication" : " recovery") <<
            " chunk log completion failure to add server" <<
        KFS_LOG_EOM;
        return;
    }
    // Yaeee...all good...
    KFS_LOG_STREAM_DEBUG <<
        req.server->GetServerLocation() <<
        " chunk: " << req.chunkId <<
        (replicationFlag ? " re-replication" : " recovery") <<
        " done" <<
    KFS_LOG_EOM;
    req.server->MovingChunkDone(req.chunkId);
    StTmp<Servers> serversTmp(mServersTmp);
    Servers&       servers = serversTmp.Get();
    mChunkToServerMap.GetServers(*ci, servers);
    // if any of the hosting servers were being "retired", notify them that
    // re-replication of any chunks hosted on them is finished
    // Replication check is already scheduled by UpdateReplicationState the
    // above. Let the normal path figure out if the any further actions are
    // needed.
    RemoveRetiring(*ci, servers, ci->GetFattr()->numReplicas);
}

void
LayoutManager::RemoveRetiring(
    CSMap::Entry&           ci,
    LayoutManager::Servers& servers,
    int                     numReplicas,
    bool                    deleteRetiringFlag /* = false */)
{
    const chunkId_t chunkId = ci.GetChunkId();
    int             cnt     = (int)servers.size();
    for (int i = 0; numReplicas < cnt && i < cnt; ) {
        const ChunkServerPtr& server = servers[i];
        if (! server->IsEvacuationScheduled(chunkId)) {
            i++;
            continue;
        }
        if (server->IsDown() || ! server->IsRetiring()) {
            // Queue RPC to log and remove entry, and replica.
            const bool kEvacuateChunkFlag = true;
            server->NotifyStaleChunk(chunkId, kEvacuateChunkFlag);
        } else {
            if (deleteRetiringFlag) {
                server->DeleteChunk(chunkId);
            } else {
                const bool retFlag = mChunkToServerMap.RemoveServer(server, ci);
                if (! retFlag) {
                    panic("failed to remove server");
                }
                KFS_LOG_STREAM_DEBUG <<
                    "-srv: "      << server->GetServerLocation() <<
                    " chunk: "    << ci.GetChunkId() <<
                    (server->IsReplay() ? " replay" : "") <<
                    " removed: "  << retFlag <<
                    " retiring: " << server->IsRetiring() <<
                    " chunks: "   << server->GetChunkCount() <<
                KFS_LOG_EOM;
            }
            if (server->GetChunkCount() <= 0) {
                server->Retire();
            }
        }
        servers.erase(servers.begin() + i);
        cnt--;
    }
}

struct InvalidChunkInfo
{
    InvalidChunkInfo(const MetaChunkInfo& ci)
        : offset(ci.offset),
          chunkId(ci.chunkId),
          chunkVersion(ci.chunkVersion)
        {}
    chunkOff_t offset;
    chunkId_t  chunkId;
    seq_t      chunkVersion;
};
typedef vector<InvalidChunkInfo, StdAllocator<InvalidChunkInfo> > InvalidChunks;

void
LayoutManager::ProcessInvalidStripes(MetaChunkReplicate& req)
{
    if (req.invalidStripes.empty()) {
        return;
    }
    CSMap::Entry* const ci = mChunkToServerMap.Find(req.chunkId);
    if (! ci) {
        return;
    }
    const MetaFattr* const fa = ci->GetFattr();
    if (! fa->HasRecovery() ||
            fa->striperType != req.striperType ||
            (int)fa->stripeSize != req.stripeSize ||
            (int)fa->numStripes != req.numStripes ||
            (int)fa->numRecoveryStripes != req.numRecoveryStripes) {
        return;
    }
    const MetaChunkInfo* const chunk = ci->GetChunkInfo();
    chunkOff_t             start = -1;
    vector<MetaChunkInfo*> cblk;
    cblk.reserve(fa->numStripes + fa->numRecoveryStripes);
    MetaFattr*     mfa    = 0;
    MetaChunkInfo* mci    = 0;
    chunkOff_t     offset = chunk->offset;
    if (metatree.getalloc(fa->id(), offset,
                mfa, mci, &cblk, &start) != 0 ||
            mfa != fa || mci != chunk) {
        panic("chunk mapping / getalloc mismatch");
        return;
    }
    const chunkOff_t end = start + fa->ChunkBlkSize();
    int              idx = 0;
    InvalidChunks    invalidChunks;
    vector<MetaChunkInfo*>::const_iterator it = cblk.begin();
    for (chunkOff_t pos = start;
            pos < end;
            pos += (chunkOff_t)CHUNKSIZE, idx++) {
        if (it == cblk.end() || pos < (*it)->offset) {
            if (req.invalidStripes.find(idx) !=
                    req.invalidStripes.end()) {
                KFS_LOG_STREAM_ERROR << "invalid stripes:"
                    " index: "  << idx <<
                    " chunk: "  <<
                        (it == cblk.end() ?
                            (*it)->chunkId :
                            chunkId_t(-1)) <<
                    " chunk offset: " <<
                        (it == cblk.end() ?
                            (*it)->offset :
                            chunkOff_t(-1)) <<
                    " offset: " << pos <<
                    " error: no chunk" <<
                KFS_LOG_EOM;
                invalidChunks.clear();
                break;
            }
            continue; // no chunk -- hole.
        }
        assert(pos == (*it)->offset);
        if (mChunkLeases.GetChunkWriteLease((*it)->chunkId) ||
                ! IsChunkStable((*it)->chunkId)) {
            KFS_LOG_STREAM_ERROR << "invalid stripes:"
                " index: "  << idx <<
                " chunk: "  << (*it)->chunkId <<
                " offset: " << pos <<
                " error: chunk is not readable" <<
            KFS_LOG_EOM;
            invalidChunks.clear();
            break;
        }
        MetaChunkReplicate::InvalidStripes::const_iterator const isi =
            req.invalidStripes.find(idx);
        if (isi != req.invalidStripes.end()) {
            if (isi->second.first != (*it)->chunkId ||
                    isi->second.second != (*it)->chunkVersion) {
                KFS_LOG_STREAM_ERROR << "invalid stripes:"
                    " index: "    << idx <<
                    " chunk: "    << (*it)->chunkId <<
                    " expected: " << isi->second.first <<
                    " version: "  << (*it)->chunkVersion <<
                    " expected: " << isi->second.second <<
                    " offset: "   << pos <<
                    " error: chunk id or version mismatch" <<
                KFS_LOG_EOM;
                invalidChunks.clear();
                break;
            }
            assert(mChunkToServerMap.Find((*it)->chunkId));
            // It is likely that more than one recovery was
            // scheduled at the same time for this chunk group, and
            // at least one has finished and reported this chunk
            // as invalid.
            // Do not invalidate chunks, wait for in-flight recovery
            // to finish.
            MetaOp const opTypes[] = {
                META_CHUNK_ALLOCATE,
                META_CHUNK_REPLICATE,
                META_CHUNK_MAKE_STABLE,
                META_CHUNK_VERSCHANGE,
                META_NUM_OPS_COUNT
            };
            if (GetInFlightChunkOpsCount(
                    (*it)->chunkId, opTypes) > 0) {
                KFS_LOG_STREAM_ERROR << "invalid stripes:"
                    " index: "  << idx <<
                    " chunk: "  << (*it)->chunkId <<
                    " offset: " << pos <<
                    " error: chunk is being replicated" <<
                KFS_LOG_EOM;
                invalidChunks.clear();
                break;
            }
            invalidChunks.push_back(InvalidChunkInfo(**it));
        }
        ++it;
    }
    if (invalidChunks.empty()) {
        return;
    }
    if (req.invalidStripes.size() != invalidChunks.size()) {
        KFS_LOG_STREAM_ERROR << "invalid stripes:"
            " failed to find all chunks:"
            " expected: " << req.invalidStripes.size() <<
            " found: "    << invalidChunks.size() <<
        KFS_LOG_EOM;
        return;
    }
    MetaChunkReplicate::InvalidStripes::const_iterator sit =
        req.invalidStripes.begin();
    for (InvalidChunks::const_iterator cit = invalidChunks.begin();
            cit != invalidChunks.end();
            ++cit, ++sit) {
        KFS_LOG_STREAM(fa->filesize <= 0 ?
                MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
            "invalidating:"
            " <"         << req.fid <<
            ","          << cit->chunkId << ">"
            " version: " << cit->chunkVersion <<
            " offset: "  << cit->offset <<
            " stripe: "  << sit->first <<
            " eof: "     << fa->filesize <<
        KFS_LOG_EOM;
        if (mPanicOnInvalidChunkFlag && 0 < fa->filesize) {
            ostringstream& os = GetTempOstream();
            os <<
            "invalid chunk detected:"
            " <"         << req.fid <<
            ","          << cit->chunkId << ">"
            " version: " << cit->chunkVersion <<
            " offset: "  << cit->offset <<
            " stripe: "  << sit->first <<
            " eof: "     << fa->filesize;
            panic(os.str());
        }
        MetaAllocate& alloc = *(new MetaAllocate(
            sit->first, req.fid, cit->offset));
        alloc.invalidateAllFlag = true;
        alloc.clientProtoVers   = max(KFS_CLIENT_PROTO_VERS,
            mMinChunkAllocClientProtoVersion + 1);
        // To pass worm mode check assign name with tmp suffix.
        const char* const kWormFakeName = "InvalidateChunk.tmp";
        alloc.pathname.Copy(kWormFakeName, strlen(kWormFakeName));
        submit_request(&alloc);
    }
}

//
// To delete additional copies of a chunk, find the servers that have the least
// amount of space and delete the chunk from there.  In addition, also pay
// attention to rack-awareness: if two copies are on the same rack, then we pick
// the server that is the most loaded and delete it there
//
void
LayoutManager::DeleteAddlChunkReplicas(
    CSMap::Entry&                  entry,
    int                            extraReplicas,
    LayoutManager::ChunkPlacement& placement)
{
    if (extraReplicas <= 0) {
        return;
    }
    StTmp<Servers> serversTmp(mServersTmp);
    Servers&       servers = serversTmp.Get();
    mChunkToServerMap.GetServers(entry, servers);
    size_t cnt = servers.size();
    if (cnt <= (size_t)extraReplicas) {
        return;
    }
    // Remove retiring / evacuating first, regardless of the placement
    // constraints to make retirement / evacuation work in the case where
    // not enough racks or disk space is available.
    const size_t numReplicas         = cnt - extraReplicas;
    const bool   kDeleteRetiringFlag = true;
    RemoveRetiring(entry, servers, (int)numReplicas, kDeleteRetiringFlag);
    if (servers.size() <= numReplicas) {
        return;
    }
    placement.clear();
    const MetaFattr* const fa                    = entry.GetFattr();
    const int              fileNumReplicas       = fa->numReplicas;
    bool                   useOtherSrvsRacksFlag = fa->IsStriped();
    if (useOtherSrvsRacksFlag && fileNumReplicas > 1) {
        // If more than one replicas are on the same rack, then do not
        // take into the account placement of other chunks in the stripe
        // block.
        placement.ExcludeServerAndRack(servers);
        useOtherSrvsRacksFlag = placement.GetExcludedRacksCount() >=
            servers.size();
        placement.clear();
    }
    size_t otherRacksEx = 0;
    if (useOtherSrvsRacksFlag) {
        const bool kIncludeThisChunkFlag = false;
        GetPlacementExcludes(entry, placement, kIncludeThisChunkFlag);
        otherRacksEx = placement.GetExcludedRacksCount();
        if (fileNumReplicas > 1 &&
                otherRacksEx + fileNumReplicas >
                    mRacks.size()) {
            // Do not pay attention to other stripes, with
            // replication higher than 1 and insufficient number of
            // racks.
            placement.clear();
            otherRacksEx = 0;
        }
    }

    StTmp<Servers> copiesToDiscardTmp(mServers2Tmp);
    Servers&       copiesToDiscard = copiesToDiscardTmp.Get();
    // Sort server by space utilization in ascending order: the delete
    // candidates with the least free space will be at the end.
    sort(servers.begin(), servers.end(),
        bind(&ChunkServer::GetSpaceUtilization, _1,
            mUseFsTotalSpaceFlag) <
        bind(&ChunkServer::GetSpaceUtilization, _2,
            mUseFsTotalSpaceFlag)
    );
    const size_t otherSrvEx = placement.GetExcludedServersCount();
    if (otherSrvEx > 0) {
        for (Servers::iterator it = servers.end();
                numReplicas < cnt &&
                it != servers.begin(); ) {
            ChunkServerPtr& server = *--it;
            if (! placement.IsServerExcluded(server)) {
                continue;
            }
            // Delete redundant replica on the server with
            // other chunks / replicas from the same
            // stripe block.
            copiesToDiscard.insert(copiesToDiscard.end(),
                ChunkServerPtr())->swap(server);
            cnt--;
        }
    }
    const chunkId_t chunkId = entry.GetChunkId();
    if (numReplicas < cnt) {
        // Try to keep as many copies as racks.
        // For striped files placement keep the copies that are on
        // different racks than the chunks in stripe / rs block.
        StBufferT<size_t, 16> canDiscardIdx;
        for (Servers::const_iterator it = servers.begin();
                it != servers.end();
                ++it) {
            const ChunkServerPtr& server = *it;
            if (! server || placement.ExcludeServerAndRack(
                    server, chunkId)) {
                continue;
            }
            // Delete redundant replica on this rack.
            canDiscardIdx.Append(it - servers.begin());
        }
        for (const size_t* first = canDiscardIdx.GetPtr(),
                    * cur = first + canDiscardIdx.GetSize();
                first < cur && numReplicas < cnt;
                ) {
            --cur;
            ChunkServerPtr& server = servers[*cur];
            copiesToDiscard.insert(copiesToDiscard.end(),
                ChunkServerPtr())->swap(server);
            cnt--;
        }
        // Drop the tail if needed.
        for (Servers::iterator it = servers.end();
                numReplicas < cnt &&
                it != servers.begin(); ) {
            ChunkServerPtr& server = *--it;
            if (! server) {
                continue;
            }
            copiesToDiscard.insert(copiesToDiscard.end(),
                ChunkServerPtr())->swap(server);
            cnt--;
        }
    }

    KFS_LOG_STREAM_START(MsgLogger::kLogLevelINFO, logStream);
        ostream& os = logStream.GetStream();
        os <<
        "<" << entry.GetFileId() << "," << chunkId  << ">"
        " excludes:"
        " srv: "
        " other: " << otherSrvEx <<
        " all: "   << placement.GetExcludedServersCount() <<
        " rack:"
        " other: " << otherRacksEx <<
        " all: "   << placement.GetExcludedRacksCount() <<
        " keeping:";
        const char* prefix = " ";
        for (Servers::const_iterator it = servers.begin();
                it != servers.end();
                ++it) {
            const ChunkServerPtr& server = *it;
            if (! server) {
                continue;
            }
            const ChunkServer& srv = *server;
            os << prefix <<
                srv.GetServerLocation() <<
                " " << srv.GetRack() <<
                " " << srv.GetSpaceUtilization(
                        mUseFsTotalSpaceFlag);
            prefix = ",";
        }
        os << " discarding:";
        prefix = " ";
        for (Servers::const_iterator it = copiesToDiscard.begin();
                it != copiesToDiscard.end();
                ++it) {
            const ChunkServer& srv = **it;
            os << prefix <<
                srv.GetServerLocation() <<
                " " << srv.GetRack() <<
                " " << srv.GetSpaceUtilization(
                        mUseFsTotalSpaceFlag);
            prefix = ",";
        }
    KFS_LOG_STREAM_END;

    for (Servers::const_iterator it = copiesToDiscard.begin();
            it != copiesToDiscard.end();
            ++it) {
        const ChunkServerPtr& server = *it;
        server->DeleteChunk(chunkId);
    }
}

void
LayoutManager::ChangeChunkReplication(chunkId_t chunkId)
{
    CSMap::Entry* const entry = mChunkToServerMap.Find(chunkId);
    if (entry) {
        CheckReplication(*entry);
    }
}

static inline void
MoveChunkBlockBack(
    vector<MetaChunkInfo*>& cblk,
    CSMap&                  csmap)
{
    if (cblk.size() <= 1) {
        return; // No point of moving it.
    }
    // Move all chunks in the block to the back of the list.
    // The order in "state none" list presently only has effect on
    // re-balance, and nothing else. The logic depend on the relative order
    // in the other chunk lists, on particular delayed recovery.
    for (vector<MetaChunkInfo*>::const_iterator
            it = cblk.begin(); it != cblk.end(); ++it) {
        CSMap::Entry& ce = CSMap::Entry::GetCsEntry(**it);
        if (csmap.GetState(ce) == CSMap::Entry::kStateNone) {
            csmap.SetState(ce, CSMap::Entry::kStateNone);
        }
    }
}

//
// Periodically, if we find that some chunkservers have LOT (> 80% free) of space
// and if others are loaded (i.e., < 30% free space), move chunks around.  This
// helps with keeping better disk space utilization (and maybe load).
//
void
LayoutManager::RebalanceServers()
{
    if (InRecovery() || mChunkToServerMap.Size() <= 0) {
        return;
    }
    if (mRebalanceReplicationsThresholdCount <= mNumOngoingReplications) {
        return;
    }
    // if we are doing rebalancing based on a plan, execute as
    // much of the plan as there is room.
    ExecuteRebalancePlan();

    if (! mIsRebalancingEnabled || mIsExecutingRebalancePlan) {
        return;
    }

    // Use backward cursor, check all chunk replication uses forward cursor,
    // see InitCheckAllChunks()
    const ChunkRecoveryInfo        recoveryInfo;
    StTmp<Servers>                 serversTmp(mServersTmp);
    StTmp<ChunkPlacement>          placementTmp(mChunkPlacementTmp);
    StTmp<vector<MetaChunkInfo*> > cblkTmp(mChunkInfos2Tmp);
    StTmp<vector<kfsSTier_t> >     tiersTmp(mPlacementTiersTmp);
    vector<MetaChunkInfo*>&        cblk           = cblkTmp.Get();
    bool                           rescheduleFlag = true;
    int64_t                        maxTime        =
        microseconds() + mMaxRebalanceRunTime;
    const size_t            maxScan        = min(mChunkToServerMap.Size(),
        (size_t)max(mMaxRebalanceScan, 0));
    for (size_t i = 0; i < maxScan; i++) {
        if (((i + 1) & 0x1F) == 0) {
            const int64_t now = microseconds();
            if (maxTime < now) {
                mRebalanceCtrs.ScanTimeout();
                break;
            }
        }
        CSMap::Entry* p = mChunkToServerMap.Prev(
            CSMap::Entry::kStateNone);
        if (! p) {
            rescheduleFlag = false;
            mRebalanceCtrs.NextRound();
            // Restart backward scan.
            mChunkToServerMap.Last(CSMap::Entry::kStateNone);
            if (! (p = mChunkToServerMap.Prev(
                    CSMap::Entry::kStateNone))) {
                break;
            }
        }
        mRebalanceCtrs.Scanned();
        CSMap::Entry&   entry         = *p;
        const chunkId_t cid           = entry.GetChunkId();
        int             extraReplicas = 0;
        ChunkPlacement& placement     = placementTmp.Get();
        if (GetInFlightChunkModificationOpCount(cid) > 0 ||
                ! CanReplicateChunkNow(
                    entry, extraReplicas, placement) ||
                extraReplicas != 0) {
            mRebalanceCtrs.Busy();
            continue;
        }
        // Cache chunk block.
        if (find(cblk.begin(), cblk.end(), entry.GetChunkInfo()) ==
                cblk.end()) {
            cblk.clear();
        }
        placement.clear();
        const bool kIncludeThisChunkFlag             = false;
        const bool kStopIfHasAnyReplicationsInFlight = true;
        const bool busyFlag = ! GetPlacementExcludes(
            entry,
            placement,
            kIncludeThisChunkFlag,
            kStopIfHasAnyReplicationsInFlight,
            &cblk
        );
        const int numReplicas = entry.GetFattr()->numReplicas;
        if (numReplicas > 1 &&
                placement.GetExcludedRacksCount() +
                numReplicas >= mRacks.size()) {
            // Do not pay attention to other stripes, with
            // replication higher than 1 and insufficient number of
            // racks.
            placement.clear();
        } else if (busyFlag) {
            mRebalanceCtrs.BusyOther();
            cblk.clear();
            // Move all chunks in the blok to the back of
            // the list, in order to skip them on this
            // re-balance pass.
            MoveChunkBlockBack(cblk, mChunkToServerMap);
            continue;
        }
        Servers& srvs = serversTmp.Get();
        mChunkToServerMap.GetServers(entry, srvs);
        double maxUtil = -1;
        int    srcCnt  = 0;
        int    srvPos  = -1;
        int    rackPos = -1;
        int    loadPos = -1;
        for (Servers::const_iterator it = srvs.begin();
                it != srvs.end();
                ++it) {
            ChunkServer& srv = **it;
            if (srv.GetReplicationReadLoad() <
                    mMaxConcurrentReadReplicationsPerNode &&
                    srv.IsResponsiveServer()) {
                srcCnt++;
            }
            if (srvPos < 0 &&
                    (placement.IsServerExcluded(srv) &&
                    placement.GetExcludedServersCount() <
                    mChunkServers.size())) {
                srvPos = (int)(it - srvs.begin());
            }
            if (! placement.ExcludeServerAndRack(srv, cid) &&
                        rackPos < 0 &&
                        placement.SearchCandidateRacks()
                        ) {
                rackPos = (int)(it - srvs.begin());
            }
            if (srvPos >= 0 || rackPos >= 0) {
                continue;
            }
            const double util =
                srv.GetSpaceUtilization(mUseFsTotalSpaceFlag);
            if (util > max(maxUtil,
                    mMaxRebalanceSpaceUtilThreshold)) {
                loadPos = (int)(it - srvs.begin());
                maxUtil = util;
            }
        }
        if (srcCnt <= 0) {
            if (srvPos >= 0 || rackPos >= 0 || loadPos >= 0) {
                mRebalanceCtrs.NoSource();
            } else {
                mRebalanceCtrs.ServerOk();
            }
            continue;
        }
        const char*            reason   = 0;
        const MetaFattr* const fa       = entry.GetFattr();
        vector<kfsSTier_t>&    tiers    = tiersTmp.Get();
        kfsSTier_t             maxSTier = kKfsSTierUndef;
        if (srvPos >= 0 || rackPos >= 0) {
            srvs.clear();
            tiers.clear();
            double     maxUtilization = mMinRebalanceSpaceUtilThreshold;
            kfsSTier_t minSTier       = kKfsSTierUndef;
            for (int i = 0; ; i++) {
                if (i > 0) {
                    if (mMaxSpaceUtilizationThreshold <= maxUtilization) {
                        break;
                    }
                    maxUtilization = mMaxSpaceUtilizationThreshold;
                }
                if (minSTier == kKfsSTierUndef) {
                    minSTier = fa->minSTier;
                    maxSTier = fa->maxSTier;
                    if (! FindStorageTiersRange(minSTier, maxSTier)) {
                        break;
                    }
                }
                placement.FindRebalanceCandidates(
                    minSTier, maxSTier, maxUtilization);
                if (srvPos < 0) {
                    if (placement.IsUsingRackExcludes()) {
                        continue;
                    }
                    const RackId rackId = placement.GetRackId();
                    if (rackId < 0 || rackId == srvs[rackPos]->GetRack()) {
                        continue;
                    }
                }
                const bool kCanIgnoreServerExcludesFlag = false;
                const ChunkServerPtr srv = placement.GetNext(
                    kCanIgnoreServerExcludesFlag);
                if (! srv) {
                    continue;
                }
                srvs.push_back(srv);
                tiers.push_back(placement.GetStorageTier());
                reason = srvPos >= 0 ?
                    "re-balance server placement" :
                    "re-balance rack placement";
                break;
            }
        } else if (loadPos >= 0) {
            const RackId   rackId   = srvs[loadPos]->GetRack();
            kfsSTier_t     minSTier = fa->minSTier;
            maxSTier = fa->maxSTier;
            ChunkServerPtr srv;
            if (FindStorageTiersRange(minSTier, maxSTier)) {
                placement.FindRebalanceCandidates(
                    minSTier,
                    maxSTier,
                    mMinRebalanceSpaceUtilThreshold,
                    rackId
                );
                const bool kCanIgnoreServerExcludesFlag = false;
                srv = placement.GetNext(kCanIgnoreServerExcludesFlag);
            }
            srvs.clear();
            tiers.clear();
            if (srv && (srv->GetRack() >= 0 || rackId < 0) &&
                    ((placement.GetRackId() >= 0 &&
                        ! placement.IsUsingRackExcludes()
                    ) ||
                    (placement.GetCandidateRackCount() <=
                        0 &&
                    placement.GetExcludedRacksCount() +
                    numReplicas >= mRacks.size()))) {
                srvs.push_back(srv);
                tiers.push_back(placement.GetStorageTier());
                reason = "re-balance utilization";
            }
        } else {
            mRebalanceCtrs.ServerOk();
            continue;
        }
        const bool noCandidatesFlag = srvs.empty();
        if (srvPos >= 0) {
            mRebalanceCtrs.ServerNeeded();
            if (noCandidatesFlag) {
                mRebalanceCtrs.NoServerFound();
            }
        } else if (rackPos >= 0) {
            mRebalanceCtrs.RackNeeded();
            if (noCandidatesFlag) {
                mRebalanceCtrs.NoRackFound();
            }
        } else {
            mRebalanceCtrs.NonLoadedServerNeeded();
            if (noCandidatesFlag) {
                mRebalanceCtrs.NoNonLoadedServerFound();
            }
        }
        if (noCandidatesFlag) {
            continue;
        }
        if (ReplicateChunk(entry, 1, srvs, recoveryInfo, tiers, maxSTier,
                reason) > 0) {
            mRebalanceCtrs.ReplicationStarted();
            MoveChunkBlockBack(cblk, mChunkToServerMap);
            cblk.clear();
            if (mRebalanceReplicationsThresholdCount <=
                    mNumOngoingReplications) {
                break;
            }
        } else {
            mRebalanceCtrs.NoReplicationStarted();
        }
    }
    if (rescheduleFlag) {
        mChunkReplicator.ScheduleNext(mRebalanceRunInterval / 1024);
    }
}

int
LayoutManager::LoadRebalancePlan(const string& planFn)
{
    if (mRebalancePlanFileName == planFn &&
            mRebalancePlan.is_open()) {
        return 0;
    }
    mRebalancePlan.close();
    mRebalancePlanFileName = planFn;
    if (mRebalancePlanFileName.empty()) {
        return 0;
    }
    mRebalancePlan.open(mRebalancePlanFileName.c_str(), istream::in);
    if (! mRebalancePlan) {
        int err = errno;
        KFS_LOG_STREAM_ERROR << "re-balance plan: " <<
            mRebalancePlanFileName <<
            " error: " << QCUtils::SysError(err) <<
        KFS_LOG_EOM;
        return (err > 0 ? -err : -EINVAL);
    }
    mRebalancePlan.setf(istream::hex);
    mIsExecutingRebalancePlan = true;
    mRebalanceCtrs.StartPlan();
    KFS_LOG_STREAM_INFO <<
        "start executing re-balance plan: " <<
        mRebalancePlanFileName <<
    KFS_LOG_EOM;
    return 0;
}

bool
LayoutManager::ReadRebalancePlan(size_t nread)
{
    if (! mRebalancePlan.is_open()) {
        return false;
    }
    chunkId_t      chunkId;
    ServerLocation loc;
    bool           addedFlag = false;
    size_t         i;
    for (i = 0; i < nread && ! mRebalancePlan.eof(); i++) {
        if (! (mRebalancePlan >> chunkId >> loc)) {
            break;
        }
        mRebalanceCtrs.PlanLine();
        Servers::const_iterator const it = FindServer(loc);
        if (it == mChunkServers.end()) {
            mRebalanceCtrs.PlanNoServer();
            continue;
        }
        (*it)->AddToChunksToMove(chunkId);
        mRebalanceCtrs.PlanAdded();
        addedFlag = true;
    }
    if (nread <= i) {
        return true;
    }
    if (mRebalancePlan.eof()) {
        KFS_LOG_STREAM_INFO <<
            "finished loading re-balance plan" <<
        KFS_LOG_EOM;
    } else {
        KFS_LOG_STREAM_ERROR <<
            "invalid re-balance plan line: " <<
            mRebalanceCtrs.GetPlanLine() <<
            " terminating plan loading" <<
        KFS_LOG_EOM;
    }
    mRebalancePlan.close();
    return addedFlag;
}

void
LayoutManager::ExecuteRebalancePlan()
{
    if (! mIsExecutingRebalancePlan ||
            mRebalanceReplicationsThresholdCount <=
            mNumOngoingReplications) {
        return;
    }
    size_t  rem           = 0;
    int     maxScan       = mMaxRebalanceScan;
    int     nextTimeCheck = maxScan - 32;
    int64_t maxTime       = microseconds() + mMaxRebalanceRunTime;
    for (Servers::const_iterator it = mChunkServers.begin();
            maxScan > 0 && it != mChunkServers.end();
            ++it) {
        bool serverDownFlag = true;
        rem += ExecuteRebalancePlan(
            *it,
            serverDownFlag,
            maxScan,
            maxTime,
            nextTimeCheck);
        if (serverDownFlag ||
                mRebalanceReplicationsThresholdCount <=
                mNumOngoingReplications) {
            maxScan = -1;
            break;
        }
    }
    if (maxScan <= 0) {
        mChunkReplicator.ScheduleNext(mRebalanceRunInterval / 1024);
        return;
    }
    if (mMaxRebalancePlanRead <= 0 && mRebalancePlan) {
        KFS_LOG_STREAM_INFO <<
            "terminating loading re-balance plan " <<
            mRebalancePlanFileName <<
        KFS_LOG_EOM;
        mRebalancePlan.close();
    }
    if (rem >= mMaxRebalancePlanRead) {
        return;
    }
    if (ReadRebalancePlan(mMaxRebalancePlanRead - rem)) {
        return;
    }
    if (rem <= 0) {
        KFS_LOG_STREAM_INFO <<
            "finished execution of rebalance plan: " <<
            mRebalancePlanFileName <<
        KFS_LOG_EOM;
        mIsExecutingRebalancePlan = false;
    }
}

size_t
LayoutManager::ExecuteRebalancePlan(
    const ChunkServerPtr& c, bool& serverDownFlag, int& maxScan,
    int64_t maxTime, int& nextTimeCheck)
{
    serverDownFlag = false;
    if (! mIsExecutingRebalancePlan || c->IsHibernatingOrRetiring() ||
           ! c->IsConnected() ||
            c->GetAvailSpace() < mChunkAllocMinAvailSpace) {
        c->ClearChunksToMove();
        return 0;
    }
    ChunkServer::ChunkIdSet& chunksToMove =
        const_cast<ChunkServer::ChunkIdSet&>(c->GetChunksToMove());
    if (c->GetSpaceUtilization(mUseFsTotalSpaceFlag) >
            mMaxSpaceUtilizationThreshold) {
        KFS_LOG_STREAM_INFO <<
            "terminating re-balance plan execution for"
            " overloaded server " << c->GetServerLocation() <<
            " chunks left: " << c->GetChunksToMove().Size() <<
        KFS_LOG_EOM;
        c->ClearChunksToMove();
        return 0;
    }
    if (chunksToMove.IsEmpty() || ! IsCandidateServer(*c)) {
        return chunksToMove.Size();
    }

    StTmp<ChunkPlacement>      placementTmp(mChunkPlacementTmp);
    StTmp<Servers>             serversTmp(mServersTmp);
    StTmp<Servers>             candidatesTmp(mServers2Tmp);
    Servers&                   candidates = candidatesTmp.Get();
    StTmp<vector<kfsSTier_t> > tiersTmp(mPlacementTiersTmp);
    vector<kfsSTier_t>&        tiers = tiersTmp.Get();
    candidates.push_back(c);
    tiers.push_back(kKfsSTierMax);

    const ChunkRecoveryInfo recoveryInfo;
    size_t                  curScan = chunksToMove.Size();
    while (maxScan > 0 && curScan > 0) {
        if (c->GetNumChunkReplications() >=
                mMaxConcurrentWriteReplicationsPerNode) {
            mRebalanceCtrs.PlanNoDest();
            break;
        }
        if (maxScan <= nextTimeCheck) {
            if (maxTime < microseconds()) {
                maxScan = -1;
                mRebalanceCtrs.PlanTimeout();
                break;
            }
            nextTimeCheck = maxScan - 32;
        }
        const chunkId_t* it = chunksToMove.Next();
        if (! it) {
            const size_t sz = chunksToMove.Size();
            if (sz <= 0) {
                break;
            }
            curScan = min(curScan, sz);
            chunksToMove.First();
            continue;
        }
        curScan--;
        maxScan--;
        mRebalanceCtrs.PlanScanned();
        chunkId_t const     cid = *it;
        CSMap::Entry* const ci  = mChunkToServerMap.Find(cid);
        if (! ci) {
            // Chunk got deleted from the time the plan was created.
            c->MovingChunkDone(cid);
            mRebalanceCtrs.PlanNoChunk();
            continue;
        }
        const MetaFattr* const fa = ci->GetFattr();
        kfsSTier_t i;
        for (i = fa->minSTier;
                i <= fa->maxSTier && ! IsCandidateServer(*c, i);
                i++)
            {}
        if (fa->maxSTier < i) {
            KFS_LOG_STREAM_INFO <<
                "cannot move"
                " chunk: "    << cid <<
                " to: "       << c->GetServerLocation() <<
                " tiers: ["   << (int)fa->minSTier <<
                ","           << (int)fa->maxSTier << "]"
                " no storage tiers available" <<
            KFS_LOG_EOM;
            continue;
        }
        tiers.front() = i;
        Servers& srvs = serversTmp.Get();
        mChunkToServerMap.GetServers(*ci, srvs);
        bool foundFlag = false;
        int  srcCnt    = 0;
        for (Servers::const_iterator ci = srvs.begin();
                ci != srvs.end();
                ++ci) {
            if (*ci == c) {
                foundFlag = true;
                break;
            }
            if ((*ci)->GetReplicationReadLoad() <
                    mMaxConcurrentReadReplicationsPerNode &&
                    (*ci)->IsResponsiveServer()) {
                srcCnt++;
            }
        }
        if (foundFlag) {
            c->MovingChunkDone(cid); // Already there.
            continue;
        }
        if (srcCnt <= 0) {
            mRebalanceCtrs.PlanNoSrc();
            continue;
        }
        int extraReplicas = 0;
        ChunkPlacement& placement = placementTmp.Get();
        if (mChunkToServerMap.GetState(*ci) !=
                    CSMap::Entry::kStateNone ||
                GetInFlightChunkModificationOpCount(cid) > 0 ||
                ! CanReplicateChunkNow(
                    *ci, extraReplicas, placement)) {
            mRebalanceCtrs.PlanBusy();
            continue;
        }
        placement.clear();
        const bool kIncludeThisChunkFlag             = false;
        const bool kStopIfHasAnyReplicationsInFlight = true;
        if (ci->GetFattr()->numReplicas <= 1 &&
                ! GetPlacementExcludes(
                    *ci,
                    placement,
                    kIncludeThisChunkFlag,
                    kStopIfHasAnyReplicationsInFlight)) {
            mRebalanceCtrs.PlanBusyOther();
            continue;
        }
        if ((placement.IsServerExcluded(c) &&
                placement.GetExcludedServersCount() <
                    mChunkServers.size()) ||
                (placement.IsRackExcluded(c) &&
                    placement.HasCandidateRacks())) {
            // Chunk cannot be moved due to rack aware placement
            // constraints.
            c->MovingChunkDone(cid);
            KFS_LOG_STREAM_INFO <<
                "cannot move"
                " chunk: "    << cid <<
                " to: "       << c->GetServerLocation() <<
                " excluded: " <<
                " servers: "  <<
                    placement.GetExcludedServersCount() <<
                " racks: "    <<
                    placement.GetExcludedRacksCount() <<
            KFS_LOG_EOM;
            mRebalanceCtrs.PlanCannotMove();
            continue;
        }
        if (ReplicateChunk(*ci, 1, candidates, recoveryInfo,
                tiers, fa->maxSTier, "re-balance plan") > 0) {
            mRebalanceCtrs.PlanReplicationStarted();
            if (mRebalanceReplicationsThresholdCount <=
                    mNumOngoingReplications) {
                break;
            }
        } else {
            mRebalanceCtrs.PlanNoReplicationStarted();
        }
        // Always use smart pointer copy here, instead of a reference,
        // as reference might become invalid if the chunk server goes
        // down as result of queuing replication op.
        if (candidates.front()->IsDown()) {
            serverDownFlag = true;
            return 0;
        }
    }
    return chunksToMove.Size();
}

void
LayoutManager::GetOpenFiles(
    MetaOpenFiles::ReadInfo&  openForRead,
    MetaOpenFiles::WriteInfo& openForWrite)
{
    mChunkLeases.GetOpenFiles(openForRead, openForWrite, mChunkToServerMap);
}

bool
LayoutManager::HasEnoughFreeBuffers(MetaRequest* /* req = 0*/)
{
    // This has to be re-entrant with req == 0. Racy check is OK though.
    return (GetFreeIoBufferByteCount() >
        SyncAddAndFetch(mIoBufPending, int64_t(0)) +
            mMinIoBufferBytesToProcessRequest);
}

void
LayoutManager::SetUserAndGroupSelf(const MetaRequest& req,
    kfsUid_t& user, kfsGid_t& group)
{
    const string& ip = req.clientIp;
    if (ip.empty()) {
        return;
    }
    if (ip == mLastUidGidRemap.mIp &&
            mLastUidGidRemap.mUser == user &&
            mLastUidGidRemap.mGroup == group) {
        if (user != kKfsUserNone) {
            user = mLastUidGidRemap.mToUser;
        }
        if (group != kKfsGroupNone) {
            group = mLastUidGidRemap.mToGroup;
        }
        return;
    }
    mLastUidGidRemap.mIp    = ip;
    mLastUidGidRemap.mUser  = user;
    mLastUidGidRemap.mGroup = group;
    for (HostUserGroupRemap::const_iterator
            it = mHostUserGroupRemap.begin();
            it != mHostUserGroupRemap.end();
            ++it) {
        if (! it->mHostPrefix.Match(ip)) {
            continue;
        }
        if (user != kKfsUserNone) {
            HostUserGroupMapEntry::UserMap::const_iterator
                const ui = it->mUserMap.find(user);
            if (ui != it->mUserMap.end()) {
                user = ui->second;
            }
        }
        if (group != kKfsGroupNone) {
            HostUserGroupMapEntry::GroupMap::const_iterator
                const gi = it->mGroupMap.find(user);
            if (gi != it->mGroupMap.end()) {
                group = gi->second;
            }
        }
        break;
    }
    mLastUidGidRemap.mToUser  = user;
    mLastUidGidRemap.mToGroup = group;
}

void
LayoutManager::CSMapUnitTest(const Properties& props)
{
    const char* const kUniteTestPropName = "metaServer.csmap.unittest";
    const int unitTestPropVal = props.getValue(kUniteTestPropName, 0);
    if (unitTestPropVal == 0) {
        return;
    }

    if (mChunkToServerMap.Size() > 0 ||
            mChunkToServerMap.GetServerCount() > 0) {
        KFS_LOG_STREAM_INFO << "not running CSMap unit test:"
            " chunks: "  << mChunkToServerMap.Size() <<
            " servers: " << mChunkToServerMap.GetServerCount() <<
        KFS_LOG_EOM;
        return;
    }
    KFS_LOG_STREAM_WARN << "running CSMap unit test: " <<
        kUniteTestPropName << " = " << unitTestPropVal <<
    KFS_LOG_EOM;

    const chunkId_t kChunks  = 1000;
    const int       kServers = 100;

    mChunkToServerMap.SetDebugValidate(true);
    MetaFattr* const fattr = MetaFattr::create(KFS_FILE, 1, 1,
        kKfsUserRoot, kKfsGroupRoot, 0644, microseconds());
    chunkId_t        cid;
    for (cid = 1; cid <= kChunks; cid++) {
        bool newEntryFlag = false;
        if (! mChunkToServerMap.Insert(
                fattr, (chunkOff_t)cid * CHUNKSIZE, cid, 1,
                newEntryFlag) || ! newEntryFlag) {
            panic("duplicate chunk id");
            break;
        }
    }
    for (int i = 0; i < kServers; i++) {
        mChunkServers.push_back(
            ChunkServer::Create(
                NetConnectionPtr(new NetConnection(new TcpSocket(), 0)),
                ServerLocation("test", i + 1)
        ));
        if (! mChunkToServerMap.AddServer(mChunkServers.back())) {
            panic("failed to add server");
        }
    }
    if (! mChunkToServerMap.RemoveServer(mChunkServers.front())) {
        panic("failed to remove server");
    }
    if (! mChunkToServerMap.RemoveServer(mChunkServers.back())) {
        panic("failed to remove server");
    }
    if (! mChunkToServerMap.AddServer(mChunkServers.front())) {
        panic("failed to add server");
    }
    if (! mChunkToServerMap.AddServer(mChunkServers.back())) {
        panic("failed to add server");
    }
    if (mChunkToServerMap.GetServerCount() != mChunkServers.size()) {
        panic("server count don't match");
    }
    Servers expected;
    for (int i = 0; i < 4; i++) {
        expected.push_back(mChunkServers[i]);
    }
    for (cid = 1; cid <= kChunks; cid++) {
        CSMap::Entry* const cur = mChunkToServerMap.Find(cid);
        if (! cur) {
            panic("missing chunk entry");
            break;
        }
        CSMap::Entry& entry = *cur;
        if (! mChunkToServerMap.AddServer(mChunkServers[5], entry)) {
            panic("failed to add server to entry");
            break;
        }
        for (int i = 0; i < 5; i++) {
            if (! mChunkToServerMap.AddServer(
                    mChunkServers[i], entry)) {
                panic("failed to add server to entry");
                break;
            }
        }
        if (! mChunkToServerMap.RemoveServer(mChunkServers[4], entry)) {
            panic("failed to remove server to entry");
            break;
        }
        if (! mChunkToServerMap.RemoveServer(mChunkServers[5], entry)) {
            panic("failed to remove server to entry");
            break;
        }
        if (mChunkToServerMap.GetServers(entry) != expected) {
            panic("servers don't match");
            break;
        }
        mChunkToServerMap.SetServers(mChunkServers, entry);
        if (mChunkToServerMap.GetServers(entry) != mChunkServers) {
            panic("servers don't match");
            break;
        }
        if (! mChunkToServerMap.RemoveServer(
                mChunkServers[10], entry)) {
            panic("failed to remove server to entry");
            break;
        }
        if (cid % 3 == 0) {
            continue;
        }
        for (int i = 0; i < kServers; i++) {
            if (i == 10) {
                continue;
            }
            if (! mChunkToServerMap.RemoveServer(
                    mChunkServers[i], entry)) {
                panic("failed to remove server");
            }
        }
        if (mChunkToServerMap.ServerCount(entry) != 0) {
            panic("invalid server count");
        }
    }
    cid = 1;
    for (int i = 11; i < kServers && i < 30; i++) {
        if (i == 10) {
            continue;
        }
        if (! mChunkToServerMap.RemoveServer(mChunkServers[i])) {
            panic("failed to remove server");
        }
        if (mChunkToServerMap.RemoveServerCleanup(1)) {
            KFS_LOG_STREAM_DEBUG <<
                "more cleanup " << i <<
            KFS_LOG_EOM;
        }
        if (! mChunkToServerMap.SetState(cid++,
                CSMap::Entry::kStatePendingReplication)) {
            panic("failed to move to pending replication");
        }
    }
    cid = 1000000;
    vector<size_t> idxs;
    for (int i = 30; i < kServers && i < 60; i++) {
        if (i == 10) {
            continue;
        }
        size_t idx = 0;
        if (! mChunkToServerMap.SetHibernated(mChunkServers[i], idx)) {
            panic("failed to hibernate server");
        }
        idxs.push_back(idx);
        if (mChunkToServerMap.RemoveServerCleanup(1)) {
            KFS_LOG_STREAM_DEBUG <<
                "hibernate more cleanup: " << i <<
                " server: " << idx <<
            KFS_LOG_EOM;
        }
        bool newEntryFlag = false;
        cid++;
        if (! mChunkToServerMap.Insert(
                fattr, (chunkOff_t)cid * CHUNKSIZE, cid, 1,
                newEntryFlag) ||
                ! newEntryFlag) {
            panic("duplicate chunk id");
        }
        if (! mChunkToServerMap.SetState(cid,
                CSMap::Entry::kStateCheckReplication)) {
            panic("failed to move into check replication");
        }
    }
    expected = mChunkToServerMap.GetServers(3);
    for (chunkId_t cid = 1; cid <= kChunks; cid++) {
        if (cid % 3 == 0) {
            if (mChunkToServerMap.GetServers(cid) != expected) {
                panic("invalid servers");
            }
        } else {
            if (mChunkToServerMap.HasServers(cid)) {
                panic("invalid server count");
            }
        }
    }
    expected.clear();
    if (mChunkToServerMap.GetHibernatedCount() != idxs.size()) {
        panic("invalid hibernated servers count");
    }
    for (size_t i = 0; i < idxs.size(); i++) {
        if (! mChunkToServerMap.RemoveHibernatedServer(idxs[i])) {
            panic("failed to remove hibernated server");
        }
    }
    if (mChunkToServerMap.GetHibernatedCount() != 0) {
        panic("invalid hibernated servers count");
    }
    while (mChunkToServerMap.RemoveServerCleanup(3)) {
        KFS_LOG_STREAM_DEBUG << "final cleanup" << KFS_LOG_EOM;
    }
    KFS_LOG_STREAM_DEBUG <<
        "servers: " << mChunkToServerMap.GetServerCount() <<
        " replication: " << mChunkToServerMap.GetCount(
            CSMap::Entry::kStateCheckReplication) <<
        " pending: " << mChunkToServerMap.GetCount(
            CSMap::Entry::kStatePendingReplication) <<
    KFS_LOG_EOM;
    mChunkToServerMap.RemoveServerCleanup(0);
    mChunkToServerMap.Clear();
    for (int i = 0; i < kServers; i++) {
        if (mChunkServers[i]->GetIndex() < 0) {
            mChunkServers[i]->ForceDown();
            continue;
        }
        if (! mChunkToServerMap.RemoveServer(mChunkServers[i])) {
            panic("failed to remove server");
        }
        mChunkServers[i]->ForceDown();
    }
    if (mChunkToServerMap.GetServerCount() != 0) {
        panic("failed to remove all servers");
    }
    if (CSMap::Entry::GetAllocBlockCount() != 0) {
        panic("server list allocation leak");
    }
    if (CSMap::Entry::GetAllocByteCount() != 0) {
        panic("server list allocation byte count mismatch");
    }
    mChunkServers.clear();
    fattr->destroy();

    KFS_LOG_STREAM_WARN << "passed CSMap unit test" <<
    KFS_LOG_EOM;
}

bool
LayoutManager::AddReplica(CSMap::Entry& ci, const ChunkServerPtr& s)
{
    return AddHosted(ci, s);
}

void
LayoutManager::CheckChunkReplication(CSMap::Entry& entry)
{
    return CheckReplication(entry);
}

ostream&
LayoutManager::RebalanceCtrs::Show(
    ostream& os, const char* prefix, const char* suffix)
{
    const char* const pref = prefix ? prefix : " ";
    const char* const suf  = suffix ? suffix : " ";
    os <<
    "RoundCount"                    << pref << mRoundCount << suf <<
    "NoSource"                      << pref << mNoSource << suf <<
    "ServerNeeded"                  << pref << mServerNeeded << suf <<
    "NoServerFound"                 << pref << mNoServerFound << suf <<
    "RackNeeded"                    << pref << mRackNeeded << suf <<
    "NoRackFound"                   << pref << mNoRackFound << suf <<
    "NonLoadedServerNeeded"         << pref << mNonLoadedServerNeeded << suf <<
    "NoNonLoadedServerFound"        << pref << mNoNonLoadedServerFound << suf <<
    "Ok"                            << pref << mOk << suf <<
    "Scanned"                       << pref << mScanned << suf <<
    "Busy"                          << pref << mBusy << suf <<
    "BusyOther"                     << pref << mBusyOther << suf <<
    "ReplicationStarted"            << pref << mReplicationStarted << suf <<
    "NoReplicationStarted"          << pref << mNoReplicationStarted << suf <<
    "ScanTimeout"                   << pref << mScanTimeout << suf <<
    "TotalNoSource"                 << pref << mTotalNoSource << suf <<
    "TotalServerNeeded"             << pref << mTotalServerNeeded << suf <<
    "TotalNoServerFound"            << pref << mTotalNoServerFound << suf <<
    "TotalRackNeeded"               << pref << mTotalRackNeeded << suf <<
    "TotalNoRackFound"              << pref << mTotalNoRackFound << suf <<
    "TotalNonLoadedServerNeeded"    << pref << mTotalNonLoadedServerNeeded << suf <<
    "TotalNoNonLoadedServerFound"   << pref << mTotalNoNonLoadedServerFound << suf <<
    "TotalOk"                       << pref << mTotalOk << suf <<
    "TotalScanned"                  << pref << mTotalScanned << suf <<
    "TotalBusy"                     << pref << mTotalBusy << suf <<
    "TotalBusyOther"                << pref << mTotalBusyOther << suf <<
    "TotalReplicationStarted"       << pref << mTotalReplicationStarted << suf <<
    "TotalNoReplicationStarted"     << pref << mTotalNoReplicationStarted << suf <<
    "TotalScanTimeout"              << pref << mTotalScanTimeout << suf <<
    "Plan"                          << pref << mPlan << suf <<
    "PlanNoDest"                    << pref << mPlanNoDest << suf <<
    "PlanTimeout"                   << pref << mPlanTimeout << suf <<
    "PlanScanned"                   << pref << mPlanScanned << suf <<
    "PlanNoChunk"                   << pref << mPlanNoChunk << suf <<
    "PlanNoSrc"                     << pref << mPlanNoSrc << suf <<
    "PlanBusy"                      << pref << mPlanBusy << suf <<
    "PlanBusyOther"                 << pref << mPlanBusyOther << suf <<
    "PlanCannotMove"                << pref << mPlanCannotMove << suf <<
    "PlanReplicationStarted"        << pref << mPlanReplicationStarted << suf <<
    "PlanNoReplicationStarted"      << pref << mPlanNoReplicationStarted << suf <<
    "PlanLine"                      << pref << mPlanLine << suf <<
    "PlanNoServer"                  << pref << mPlanNoServer << suf <<
    "PlanAdded"                     << pref << mPlanAdded << suf <<
    "TotalPlanNoDest"               << pref << mTotalPlanNoDest << suf <<
    "TotalPlanTimeout"              << pref << mTotalPlanTimeout << suf <<
    "TotalPlanScanned"              << pref << mTotalPlanScanned << suf <<
    "TotalPlanNoChunk"              << pref << mTotalPlanNoChunk << suf <<
    "TotalPlanNoSrc"                << pref << mTotalPlanNoSrc << suf <<
    "TotalPlanBusy"                 << pref << mTotalPlanBusy << suf <<
    "TotalPlanBusyOther"            << pref << mTotalPlanBusyOther << suf <<
    "TotalPlanCannotMove"           << pref << mTotalPlanCannotMove << suf <<
    "TotalPlanReplicationStarted"   << pref << mTotalPlanReplicationStarted << suf <<
    "TotalPlanNoReplicationStarted" << pref << mTotalPlanNoReplicationStarted << suf <<
    "TotalPlanLine"                 << pref << mTotalPlanLine << suf <<
    "TotalPlanNoServer"             << pref << mTotalPlanNoServer << suf <<
    "TotalPlanAdded"                << pref << mTotalPlanAdded << suf
    ;
    return os;
}

void
LayoutManager::Handle(MetaForceChunkReplication& op)
{
    // The following is intended for debug and testing purposes only.
    if (op.chunkId < 0) {
        op.status    = -EINVAL;
        op.statusMsg = "invalid chunk id";
        return;
    }
    CSMap::Entry* const entry = mChunkToServerMap.Find(op.chunkId);
    if (! entry) {
        op.status    = -ENOENT;
        op.statusMsg = "no such chunk";
        return;
    }
    const MetaFattr* const fa = entry->GetFattr();
    if (fa->numRecoveryStripes <= 0 && op.recoveryFlag) {
        op.status    = -EINVAL;
        op.statusMsg = "file is not created with recovery";
        return;
    }
    if (op.recoveryFlag && ! op.forcePastEofRecoveryFlag && fa->filesize <=
            fa->ChunkPosToChunkBlkFileStartPos(entry->GetChunkInfo()->offset)) {
        op.status    = -EINVAL;
        op.statusMsg = "chunk block past logical end of file";
        return;
    }
    if (mChunkLeases.GetChunkWriteLease(op.chunkId)) {
        // This check isn't sufficient with recovery, typically the size check
        // the above would be sifficient, as logical end of file set and the end
        // of write. If remove isn't set, then the can replicate chunk now will
        // catch the theoretically possible corner case.
        op.status    = -EBUSY;
        op.statusMsg = "write lease exists";
        return;
    }
    bool                  removeDstFlag = false;
    const ServerLocation& dst           = op;
    Servers::const_iterator dstIt;
    if (dst.IsValid()) {
        dstIt = FindServer(dst);
        if (dstIt == mChunkServers.end()) {
            op.status    = -EINVAL;
            op.statusMsg = "no such chunk server";
            return;
        }
        StTmp<Servers> serversTmp(mServers3Tmp);
        Servers&       servers = serversTmp.Get();
        mChunkToServerMap.GetServers(*entry, servers);
        if (find(servers.begin(), servers.end(), *dstIt) != servers.end()) {
            if (! op.removeFlag ||
                    (servers.size() <= size_t(1) && ! op.recoveryFlag)) {
                op.status    = -EINVAL;
                op.statusMsg = "chunk exists already on " + dst.ToString();
                return;
            }
            removeDstFlag = true;
        }
    } else {
        dstIt = mChunkServers.end();
    }
    int extraReplicas = 0;
    ChunkRecoveryInfo recoveryInfo;
    StTmp<ChunkPlacement> placementTmp(mChunkPlacementTmp);
    ChunkPlacement& placement = placementTmp.Get();
    int hibernatedReplicaCount = 0;
    if (! CanReplicateChunkNow(
            *entry,
            extraReplicas,
            placement,
            &hibernatedReplicaCount,
            &recoveryInfo,
            op.recoveryFlag) &&
                (! op.removeFlag || dstIt == mChunkServers.end())) {
        op.status    = -EBUSY;
        op.statusMsg = "cannot be started at the moment";
        return;
    }
    KFS_LOG_STREAM_INFO <<
        "starting: "
        " seq: " << op.opSeqno <<
        " "      << op.Show() <<
    KFS_LOG_EOM;
    if (dstIt != mChunkServers.end()) {
        const ChunkServerPtr srv = *dstIt;
        if (removeDstFlag) {
            srv->NotifyStaleChunk(op.chunkId);
            if (srv->IsDown()) {
                op.status    = -EFAULT;
                op.statusMsg = "server went down";
                return;
            }
        }
        StTmp<vector<kfsSTier_t> > tiersTmp(mPlacementTiersTmp);
        vector<kfsSTier_t>&        tiers = tiersTmp.Get();
        StTmp<Servers>             candidatesTmp(mServers2Tmp);
        Servers&                   candidates = candidatesTmp.Get();
        tiers.push_back(fa->minSTier);
        candidates.push_back(srv);
        extraReplicas = 1;
        if (ReplicateChunk(
                *entry,
                extraReplicas,
                candidates,
                recoveryInfo,
                tiers,
                fa->maxSTier,
                "admin forced",
                mChunkToServerMap.HasServer(srv, *entry)) <= 0) {
            op.status    = -EAGAIN;
            op.statusMsg = "failed to start replication";
        }
    } else {
        extraReplicas = max(1, extraReplicas + 1);
        if (ReplicateChunk(
                *entry, extraReplicas, placement, recoveryInfo) <= 0) {
            op.status    = -EAGAIN;
            op.statusMsg = "no replication candidates";
            return;
        }
    }
}

bool
LayoutManager::FindAccessProxyFor(
    const MetaRequest&      req,
    LayoutManager::Servers& srvs)
{
    for (int pass = 0; pass < 2; pass++) {
        const RackId                    rackId =
            pass == 0 ? GetRackId(req) : RackId(-1);
        RackInfos::const_iterator const it     =
            rackId < 0 ? mRacks.end() : FindRack(rackId);
        if (it == mRacks.end() && 0 == pass) {
            pass++;
        }
        const Servers& servers =
            0 == pass ? it->getServers() : mChunkServers;
        const size_t   size    = servers.size();
        if (0 < size) {
            // Find one with below twice average load or writable object count.
            // Start from random place. Limit scan depth.
            const int64_t kWritableFloor = 6;
            const int64_t kOpenFloor     = 32;
            const size_t  kMaxScan       = 64;
            const int64_t                 mult = mChunkServers.size() / 2;
            Servers::const_iterator       it   = servers.begin() +
                (1 < size ? (size_t)Rand(size) : size_t(0));
            Servers::const_iterator const sit  = it;
            for (size_t i = 0; i < min(kMaxScan, size); i++) {
                if ((*it)->GetLoadAvg() * mult <= mCSTotalLoadAvgSum &&
                        ((*it)->GetWritableObjectCount() <= kWritableFloor ||
                            (*it)->GetWritableObjectCount() * mult <=
                            mCSWritableObjectCount) &&
                        ((*it)->GetOpenObjectCount() <= kOpenFloor ||
                            (*it)->GetOpenObjectCount() * mult <=
                            mCSOpenObjectCount)) {
                    srvs.push_back(*it);
                    return true;
                }
                if (servers.end() == ++it) {
                    it = servers.begin();
                }
            }
            if (0 != pass) {
                // No good one, use the fist randomly chosen one.
                srvs.push_back(*sit);
                return true;
            }
        }
    }
    return false;
}

bool
LayoutManager::GetAccessProxy(
    const MetaRequest&      req,
    LayoutManager::Servers& servers)
{
    Servers::const_iterator it = mObjectStorePlacementTestFlag ?
        mChunkServers.end() : FindServerForReq(req);
    servers.clear();
    if (it != mChunkServers.end()) {
        servers.push_back(*it);
        return true;
    }
    return (mObjectStoreReadCanUsePoxoyOnDifferentHostFlag &&
        FindAccessProxyFor(req, servers));
}

bool
LayoutManager::RunObjectBlockDeleteQueue()
{
    if (GetConnectedServerCount() <= 0) {
        return false;
    }
    int          rem         = mObjStoreDeleteMaxSchedulePerRun;
    const size_t kMaxRandCnt = 4;
    size_t       randCnt     = 0;
    while (! mObjBlocksDeleteRequeue.IsEmpty() && 0 < rem) {
        const ObjBlockDeleteQueueEntry entry    = mObjBlocksDeleteRequeue.Back();
        const size_t                   prevSize =
            mObjBlocksDeleteRequeue.PopBack();
        if (DeleteFileBlocks(entry.first, entry.second, entry.second, rem) ==
                entry.second) {
            mObjBlocksDeleteRequeue.PushBack(entry);
        }
        const size_t size = mObjBlocksDeleteRequeue.GetSize();
        if (prevSize <= size) {
            if (rem <= 0 || size < 2 || min(size - 1, kMaxRandCnt) <= randCnt) {
                break;
            }
            randCnt++;
            // Delete cannot be scheduled due to allocation or make stable in
            // flight. Randomly choose next item to schedule.
            swap(mObjBlocksDeleteRequeue[
                    size < 3 ? int64_t(0) : Rand(size - 1)],
                mObjBlocksDeleteRequeue.Back());
        }
    }
    const time_t                     expire = TimeNow() - mObjStoreDeleteDelay;
    ObjStoreFilesDeleteQueue::Entry* entry;
    while (0 < rem &&
            (entry = mObjStoreFilesDeleteQueue.Front()) &&
            entry->mTime < expire &&
            (entry->mLast = DeleteFileBlocks(
                entry->mFid, 0, entry->mLast, rem)) < 0) {
        mObjStoreFilesDeleteQueue.Remove();
    }
    return (rem < mObjStoreDeleteMaxSchedulePerRun);
}

void
LayoutManager::DeleteFile(const MetaFattr& fa)
{
    if (0 != fa.numReplicas || KFS_FILE != fa.type || mCleanupFlag) {
        return;
    }
    // Queue one past the last block, to handle possible in flight allocation.
    chunkOff_t last = fa.nextChunkOffset() + fa.maxSTier;
    int        rem  = mObjStoreDeleteMaxSchedulePerRun;
    if (mObjStoreDeleteDelay <= 0 &&
            (last = DeleteFileBlocks(fa.id(), 0, last, rem)) < 0) {
        return;
    }
    mObjStoreFilesDeleteQueue.Add(TimeNow(), fa.id(), last);
}

chunkOff_t
LayoutManager::DeleteFileBlocks(fid_t fid, chunkOff_t first, chunkOff_t last,
        int& remScanCnt)
{
    MetaOp const types[] = {
        META_CHUNK_DELETE,
        META_CHUNK_ALLOCATE,
        META_CHUNK_MAKE_STABLE,
        META_NUM_OPS_COUNT // Sentinel
    };
    ObjBlocksDeleteInFlightEntry::Val const entryVal(fid, 0);
    ObjBlocksDeleteInFlightEntry            entry(entryVal, entryVal);
    chunkOff_t                              pos;
    for (pos = last; first <= pos && 0 < remScanCnt; pos -= CHUNKSIZE) {
         // Update size as chunk servers might go down due to DeleteChunkVers()
         // invocation.
        remScanCnt--;
        const size_t size        = mChunkServers.size();
        const size_t maxInFlight = size * mObjStoreMaxDeletesPerServer;
        if (maxInFlight <= mObjBlocksDeleteInFlight.GetSize()) {
            remScanCnt = 0;
            return pos;
        }
        if (size <= mObjStoreDeleteSrvIdx) {
            mObjStoreDeleteSrvIdx = 0;
            if (size <= 0) {
                remScanCnt = 0;
                return pos;
            }
        }
        const seq_t chunkVersion = -(seq_t)pos - 1;
        if (0 < GetInFlightChunkOpsCount(fid, types,
                ChunkVersionToObjFileBlockPos(chunkVersion))) {
            if (maxInFlight <= mObjBlocksDeleteRequeue.GetSize()) {
                return pos;
            }
            mObjBlocksDeleteRequeue.PushBack(make_pair(fid, pos));
        } else {
            entry.GetVal().second = chunkVersion;
            bool insertedFlag = false;
            mObjBlocksDeleteInFlight.Insert(
                entry.GetKey(), entry.GetVal(), insertedFlag);
            if (insertedFlag) {
                const bool kStaleChunkIdFlag = false;
                const bool kForceDeleteFlag  = true;
                mChunkServers[mObjStoreDeleteSrvIdx++]->DeleteChunkVers(
                    fid, chunkVersion, kStaleChunkIdFlag, kForceDeleteFlag);
            }
        }
    }
    return pos;
}

void
LayoutManager::Done(MetaChunkDelete& req)
{
    if (0 <= req.chunkVersion) {
        CSMap::Entry* const ci = mChunkToServerMap.Find(req.chunkId);
        if (ci) {
            UpdateReplicationState(*ci);
        }
        return;
    }
    if (mObjBlocksDeleteInFlight.Erase(ObjBlocksDeleteInFlightEntry::Key(
                req.chunkId, req.chunkVersion)) <= 0) {
        return;
    }
    if (0 != req.status && -ENOENT != req.status) {
        mObjBlocksDeleteRequeue.PushBack(
            make_pair(req.chunkId, -req.chunkVersion - 1));
        return; // Do not re-queue it immediately.
    }
    if (mObjBlocksDeleteInFlight.IsEmpty() &&
            mObjBlocksDeleteRequeue.IsEmpty() &&
                mObjStoreFilesDeleteQueue.IsEmpty()) {
        // Drained the queues, log this event.
        mResubmitClearObjectStoreDeleteFlag = false;
        submit_request(new MetaLogClearObjStoreDelete());
        return;
    }
    if (mObjBlocksDeleteInFlight.GetSize() + mObjStoreMaxDeletesPerServer / 2 <
            mObjStoreMaxDeletesPerServer * mChunkServers.size()) {
        RunObjectBlockDeleteQueue();
    }
}

void
LayoutManager::ClearObjStoreDelete()
{
    mObjBlocksDeleteInFlight.Clear();
    mObjBlocksDeleteRequeue.Clear();
    mObjStoreFilesDeleteQueue.Clear();
}

bool
LayoutManager::IsObjectStoreDeleteEmpty() const
{
    return (
        mObjBlocksDeleteInFlight.IsEmpty() &&
        mObjBlocksDeleteRequeue.IsEmpty()  &&
        mObjStoreFilesDeleteQueue.IsEmpty()
    );
}

void
LayoutManager::Handle(MetaLogClearObjStoreDelete& req)
{
    mResubmitClearObjectStoreDeleteFlag = ! req.replayFlag &&
        0 != req.status && IsObjectStoreDeleteEmpty();
    if (req.replayFlag && 0 == req.status) {
        ClearObjStoreDelete();
    }
}

bool
LayoutManager::AddPendingObjStoreDelete(
    chunkId_t chunkId, chunkOff_t first, chunkOff_t last)
{
    if (chunkId <= 0 || last < 0 || (0 != first && first != last)) {
        return false;
    }
    if (first == last) {
        mObjBlocksDeleteRequeue.PushBack(make_pair(chunkId, last));
    } else {
        mObjStoreFilesDeleteQueue.Add(TimeNow(), chunkId, last);
    }
    return true;
}

class ObjStoreDeleteWriter
{
public:
    ObjStoreDeleteWriter(
        ostream& os)
        : mOs(os)
        {}
    template<typename IDT, typename PT>
    void operator()(IDT id, PT pos, bool blockFlag)
    {
        mOs << (blockFlag ? "osd/" : "osx/") << id << "/" << pos << "\n";
    }
    void Flush()
        { mOs.flush(); }
private:
    ReqOstream mOs;
};

int
LayoutManager::WritePendingObjStoreDelete(ostream& os)
{
    ObjStoreDeleteWriter writer(os);
    GetPendingObjStoreDelete(writer);
    writer.Flush();
    return (os ? 0 : -EIO);
}

bool
LayoutManager::RestoreStart()
{
    if (! mChunkServers.empty() ||
            ! mHibernatingServers.empty() ||
            0 < mChunkToServerMap.Size() ||
            0 < mChunkToServerMap.GetServerCount() ||
            0 < mChunkToServerMap.GetHibernatedCount() ||
            mChunkToServerMap.RemoveServerCleanup(0)) {
        KFS_LOG_STREAM_FATAL <<
            "restore: invalid initial state" <<
        KFS_LOG_EOM;
        return false;
    }
    return true;
}

int
LayoutManager::WriteChunkServers(ostream& os) const
{
    for (Servers::const_iterator it = mChunkServers.begin();
            os && mChunkServers.end() != it;
            ++it) {
        if (! (*it)->Checkpoint(os)) {
            return -EIO;
        }
    }
    for (HibernatedServerInfos::const_iterator it = mHibernatingServers.begin();
            os && mHibernatingServers.end() != it;
            ++it) {
        if (it->IsHibernated()) {
            HibernatedChunkServer* const srv =
                mChunkToServerMap.GetHiberantedServer(it->csmapIdx);
            if (srv) {
                if (! srv->Checkpoint(os, it->location,
                        it->startTime, it->sleepEndTime, it->retiredFlag)) {
                    return -EIO;
                }
            }
        }
    }
    return (os ? 0 : -EIO);
}

void
LayoutManager::StartServicing()
{
    KFS_LOG_STREAM(gNetDispatch.IsRunning() ?
            MsgLogger::kLogLevelINFO :
            MsgLogger::kLogLevelDEBUG) <<
        "start servicing,"
        " primary: "      << mPrimaryFlag <<
        " servers: "      << mChunkServers.size() <<
        " replay: "       << mReplayServerCount <<
        " disconnected: " << mDisconnectedCount <<
    KFS_LOG_EOM;
    if (! mPrimaryFlag) {
        return;
    }
    if (! gNetDispatch.IsRunning()) {
        return;
    }
    for (Servers::const_iterator it = mChunkServers.begin();
            mChunkServers.end() != it;
            ++it) {
        ChunkServer& srv = **it;
        srv.ScheduleDown("start servicing");
    }
    for (HibernatedServerInfos::iterator it = mHibernatingServers.begin();
            mHibernatingServers.end() != it;
            ++it) {
        // Extend hibernated interval to the of the recovery interval, in order
        // to attempt partial chunk inventory synchronization.
        it->sleepEndTime = max(it->sleepEndTime, TimeNow() +
            max(2 * mServerDownReplicationDelay, mRecoveryIntervalSec));
        it->replayFlag   = false;
    }
    mServiceStartTime = TimeNow();
    mWasServicingFlag = false;
}

void
LayoutManager::StopServicing()
{
    KFS_LOG_STREAM(gNetDispatch.IsRunning() ?
            MsgLogger::kLogLevelINFO :
            MsgLogger::kLogLevelDEBUG) <<
        "stop servicing,"
        " primary: "      << mPrimaryFlag <<
        " servers: "      << mChunkServers.size() <<
        " replay: "       << mReplayServerCount <<
        " disconnected: " << mDisconnectedCount <<
    KFS_LOG_EOM;
    if (mReplayServerCount < mChunkServers.size()) {
        for (Servers::const_iterator it = mChunkServers.begin();
                mChunkServers.end() != it;
                ++it) {
            ChunkServer& srv = **it;
            if (! srv.IsReplay()) {
                srv.StopServicing();
            }
        }
    }
    mSlavesCount  = 0;
    mMastersCount = 0;
    mChunkLeases.StopServicing(mARAChunkCache, mChunkToServerMap);
    while (! mNonStableChunks.IsEmpty()) {
        mNonStableChunks.First();
        const NonStableChunkKVEntry* entry;
        while ((entry = mNonStableChunks.Next())) {
            DeleteNonStableEntry(
                entry->GetKey(),
                &(entry->GetVal()),
                -EVRNOTPRIMARY,
                "no longer primary node"
            );
        }
    }
    mStripedFilesAllocationsInFlight.clear();
    mPendingBeginMakeStable.Clear();
    if (mPrimaryFlag) {
        return;
    }
    RequestQueue queue;
    queue.PushBack(mResubmitQueue);
    MetaRequest* req;
    while ((req = queue.PopFront())) {
        KFS_LOG_STREAM_DEBUG <<
            "canceling: " << req->Show() <<
        KFS_LOG_EOM;
        req->suspended = false;
        submit_request(req);
    }
}

ostream&
LayoutManager::Checkpoint(ostream& os, const MetaChunkInfo& info) const
{
    return mChunkToServerMap.Checkpoint(os, CSMap::Entry::GetCsEntry(info));
}

bool
LayoutManager::Restore(MetaChunkInfo& info,
    const char* restoreIdxs, size_t restoreIdxsLen, bool hexFmtFlag)
{
    return (hexFmtFlag ?
        mChunkToServerMap.Restore<HexIntParser>(CSMap::Entry::GetCsEntry(info),
            restoreIdxs, restoreIdxsLen) :
        mChunkToServerMap.Restore<DecIntParser>(CSMap::Entry::GetCsEntry(info),
            restoreIdxs, restoreIdxsLen)
    );
}

int
LayoutManager::RunFsck(
    const string& tmpPrefix, bool reportAbandonedFilesFlag, ostream& os)
{
    const int cnt = FsckStreamCount(reportAbandonedFilesFlag);
    if (cnt <= 0) {
        KFS_LOG_STREAM_ERROR << "internal error" << KFS_LOG_EOM;
        return -EINVAL;
    }
    const char* const    suffix    = ".XXXXXX";
    const size_t         suffixLen = strlen(suffix);
    StBufferT<char, 128> buf;
    fstream*  const      streams   = new fstream[cnt];
    ostream** const      ostreams  = new ostream*[cnt + 1];
    int                  status    = 0;
    ostreams[cnt] = 0;
    for (int i = 0; i < cnt; i++) {
        char* const ptr = buf.Resize(tmpPrefix.length() + suffixLen + 1);
        memcpy(ptr, tmpPrefix.data(), tmpPrefix.size());
        strcpy(ptr + tmpPrefix.size(), suffix);
        const int tfd = mkstemp(ptr);
        if (tfd < 0) {
            status = errno > 0 ? -errno : -EINVAL;
            KFS_LOG_STREAM_ERROR <<
                "failed to create temporary file: " << ptr <<
                QCUtils::SysError(-status) <<
            KFS_LOG_EOM;
            close(tfd);
            break;
        }
        streams[i].open(ptr, fstream::in | fstream::out);
        close(tfd);
        unlink(ptr);
        if (! streams[i]) {
            status = errno > 0 ? -errno : -EINVAL;
            KFS_LOG_STREAM_ERROR <<
                "failed to open temporary file: " << ptr <<
                QCUtils::SysError(-status) <<
            KFS_LOG_EOM;
            break;
        }
        ostreams[i] = streams + i;
    }
    if (0 == status) {
        status = Fsck(ostreams, reportAbandonedFilesFlag) ? 0 : -EINVAL;
        char* const  ptr = buf.Resize(128 << 10);
        const size_t len = buf.GetSize();
        for (int i = 0; i < cnt; i++) {
            streams[i].flush();
            streams[i].seekp(0);
            while (os && streams[i]) {
                streams[i].read(ptr, len);
                os.write(ptr, streams[i].gcount());
            }
            if (! streams[i].eof()) {
                status = errno > 0 ? -errno : -EINVAL;
                KFS_LOG_STREAM_ERROR <<
                    "io error: " << QCUtils::SysError(-status) <<
                KFS_LOG_EOM;
                while (i < cnt) {
                    streams[i].close();
                }
                break;
            }
            streams[i].close();
        }
    }
    delete [] streams;
    delete [] ostreams;
    return status;
}

int
LayoutManager::RunFsck(const string& fileName, bool reportAbandonedFilesFlag)
{
    ofstream os(fileName.c_str(), ofstream::out | ofstream::trunc);
    if (! os) {
        const int err = errno;
        KFS_LOG_STREAM_ERROR << fileName << ": " <<
            QCUtils::SysError(err) <<
        KFS_LOG_EOM;
        return (0 < err ? -err : (0 == err ? -EIO : err));
    }
    int ret = RunFsck(fileName, reportAbandonedFilesFlag, os);
    os.close();
    if (! os) {
        const int err = errno;
        KFS_LOG_STREAM_ERROR << fileName << ": " <<
            QCUtils::SysError(err) <<
        KFS_LOG_EOM;
        if (0 == ret) {
            ret = 0 < err ? -err : (0 == err ? -EIO : err);
        }
    }
    return ret;
}

void
LayoutManager::Start(MetaTruncate& req)
{
    if (req.chunksCleanupFlag || req.replayFlag || 0 != req.status) {
        return;
    }
    req.maxDeleteCount = mMaxTruncateChunksDeleteCount;
    req.maxQueueCount  = mMaxTruncateChunksQueueCount;
}

void
LayoutManager::ScheduleTruncatedChunksDelete()
{
    if (! mPrimaryFlag) {
        return;
    }
    if (mMaxTruncatedChunkDeletesInFlight <= mTruncatedChunkDeletesInFlight) {
        return;
    }
    const MetaFattr* const fa = metatree.getChunkDeleteQueue();
    if (! fa || fa->chunkcount() <= 0) {
        return;
    }
    MetaTruncate& req = *(new MetaTruncate());
    req.chunksCleanupFlag = true;
    req.setEofHintFlag    = false;
    req.maxDeleteCount    = (int)min(
        (int64_t)mMaxTruncatedChunkDeletesInFlight -
            mTruncatedChunkDeletesInFlight,
        fa->chunkcount()
    );
    KFS_LOG_STREAM_DEBUG <<
        "scheduling truncated chunks cleanup:"
        " remaining: " << fa->chunkcount() <<
        " to delete: " << req.maxDeleteCount <<
        " in flight: " << mTruncatedChunkDeletesInFlight <<
        " max: "       << mMaxTruncatedChunkDeletesInFlight <<
        " "            << req.Show() <<
    KFS_LOG_EOM;
    mTruncatedChunkDeletesInFlight += req.maxDeleteCount;
    submit_request(&req);
    return;
}

void
LayoutManager::Handle(MetaTruncate& req)
{
    if (req.replayFlag) {
        return;
    }
    if (req.chunksCleanupFlag) {
        if (mTruncatedChunkDeletesInFlight < req.maxDeleteCount) {
            panic("invalid truncated chunks delete in flight count");
            mTruncatedChunkDeletesInFlight = 0;
        } else {
            mTruncatedChunkDeletesInFlight -= req.maxDeleteCount;
        }
    }
    ScheduleTruncatedChunksDelete();
}

void
LayoutManager::Handle(MetaSetATime& req)
{
    if (! req.replayFlag && 1 != mSetATimeInFlight.Erase(req.fid)) {
        panic("invalid set atime op");
    }
}

void
LayoutManager::UpdateATime(const MetaFattr* fa, MetaReaddir& req)
{
    UpdateATimeSelf(mDirATimeUpdateResolution, fa, req);
}

void
LayoutManager::UpdateATime(const MetaFattr* fa, MetaReaddirPlus& req)
{
    UpdateATimeSelf(mDirATimeUpdateResolution, fa, req);
}

void
LayoutManager::Start(MetaRename& req)
{
    mChunkLeases.Start(req);
}

void
LayoutManager::Done(MetaRename& req)
{
    mChunkLeases.Done(req);
}

} // namespace KFS
