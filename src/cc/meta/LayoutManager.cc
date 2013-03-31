//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/06
// Author: Sriram Rao
//         Mike Ovsiannikov
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
// \file LayoutManager.cc
// \brief Layout manager implementation.
//
//----------------------------------------------------------------------------

#include "LayoutManager.h"
#include "kfstree.h"
#include "ClientSM.h"
#include "NetDispatch.h"

#include "kfsio/Globals.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/IOBufferWriter.h"
#include "qcdio/QCIoBufferPool.h"
#include "qcdio/QCUtils.h"
#include "common/MsgLogger.h"
#include "common/Properties.h"
#include "common/time.h"
#include "common/Version.h"
#include "common/RequestParser.h"
#include "common/StdAllocator.h"
#include "common/rusage.h"

#include <algorithm>
#include <functional>
#include <sstream>
#include <iterator>
#include <fstream>
#include <limits>
#include <iomanip>
#include <boost/mem_fn.hpp>
#include <boost/bind.hpp>
#include <openssl/rand.h>

namespace KFS {

using std::for_each;
using std::find;
using std::sort;
using std::unique;
using std::random_shuffle;
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
using std::setw;
using std::setfill;
using std::hex;
using boost::mem_fn;
using boost::bind;
using boost::ref;

using libkfsio::globalNetManager;
using libkfsio::globals;

const int64_t kSecs2MicroSecs = 1000 * 1000;

//LayoutManager gLayoutManager;

class LayoutManager::RandGen
{
public:
    RandGen(LayoutManager& m)
        : mLm(m)
        {}
    size_t operator()(size_t interval) {
        return (size_t)mLm.Rand((int64_t)interval);
    }
private:
    LayoutManager& mLm;
};

static inline time_t
TimeNow()
{
    return globalNetManager().Now();
}

static inline time_t
GetInitialWriteLeaseExpireTime() {
    return (TimeNow() + 10 * 365 * 24 * 60 * 60);
}

static inline seq_t
RandomSeqNo()
{
    seq_t ret = 0;
    RAND_pseudo_bytes(
        reinterpret_cast<unsigned char*>(&ret), int(sizeof(ret)));
    return ((ret < 0 ? -ret : ret) >> 1);
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
        mChunkServers.size() < mMinChunkserversToExitRecovery ||
        InRecoveryPeriod()
    );
}

inline bool
LayoutManager::IsChunkServerRestartAllowed() const
{
    return (
        ! InRecovery() &&
        mChunkServers.size() > mMinChunkserversToExitRecovery &&
        mHibernatingServers.empty()
    );
}

inline bool
ARAChunkCache::Invalidate(iterator it)
{
    assert(it != mMap.end() && ! mMap.empty());
    mMap.erase(it);
    return true;
}

inline bool
ARAChunkCache::Invalidate(fid_t fid)
{
    iterator const it = mMap.find(fid);
    if (it == mMap.end()) {
        return false;
    }
    mMap.erase(it);
    return true;
}

inline bool
ARAChunkCache::Invalidate(fid_t fid, chunkId_t chunkId)
{
    iterator const it = mMap.find(fid);
    if (it == mMap.end() || it->second.chunkId != chunkId) {
        return false;
    }
    mMap.erase(it);
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
    mMap[req.fid] = Entry(
        req.chunkId,
        req.chunkVersion,
        req.offset,
        TimeNow(),
        last,
        req.permissions
    );
}

bool
ARAChunkCache::Entry::AddPending(MetaAllocate& req)
{
    assert(req.appendChunk);

    if (! lastPendingRequest || ! req.appendChunk) {
        if (req.appendChunk) {
            req.responseStr = responseStr;
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

    iterator const it = mMap.find(req.fid);
    if (it == mMap.end()) {
        return;
    }
    Entry& entry = it->second;
    if (entry.chunkId != req.chunkId) {
        return;
    }
    if (req.status != 0) {
        // Failure, invalidate the cache.
        mMap.erase(it);
        return;
    }
    entry.lastAccessedTime = TimeNow();
    entry.offset           = req.offset;
    if (entry.lastPendingRequest) {
        // Transition from pending to complete.
        // Cache the response. Restart decay timer.
        entry.responseStr        = req.responseStr;
        entry.lastDecayTime      = entry.lastAccessedTime;
        entry.lastPendingRequest = 0;
    }
}

void
ARAChunkCache::Timeout(time_t minTime)
{
    for (iterator it = mMap.begin(); it != mMap.end(); ) {
        const Entry& entry = it->second;
        if (entry.lastAccessedTime >= minTime ||
                entry.lastPendingRequest) {
            ++it; // valid entry; keep going
        } else {
            mMap.erase(it++);
        }
    }
}

ChunkLeases::ChunkLeases()
    : mLeaseId(RandomSeqNo()),
      mReadLeases(),
      mWriteLeases(),
      mCurWrIt(mWriteLeases.end()),
      mTimerRunningFlag(false)
{}

inline void
ChunkLeases::Erase(
    WriteLeases::iterator it)
{
    assert(it != mWriteLeases.end());
    if (mTimerRunningFlag && it == mCurWrIt) {
        ++mCurWrIt;
    }
    mWriteLeases.erase(it);
}

inline void
ChunkLeases::Erase(
    ReadLeases::iterator it)
{
    assert(it != mReadLeases.end());
    const bool      updateFlag = it->second.mScheduleReplicationCheckFlag;
    const chunkId_t chunkId    = it->first;
    mReadLeases.erase(it);
    if (updateFlag) {
        gLayoutManager.ChangeChunkReplication(chunkId);
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

inline ChunkLeases::LeaseId
ChunkLeases::NewReadLeaseId()
{
    const LeaseId id = IsReadLease(mLeaseId) ? mLeaseId : (mLeaseId + 1);
    assert(IsReadLease(id));
    return id;
}

inline ChunkLeases::LeaseId
ChunkLeases::NewWriteLeaseId()
{
    const LeaseId id = IsWriteLease(mLeaseId) ? mLeaseId : (mLeaseId + 1);
    assert(IsWriteLease(id));
    return id;
}

inline const ChunkLeases::WriteLease*
ChunkLeases::GetWriteLease(
    chunkId_t chunkId) const
{
    WriteLeases::const_iterator const wi = mWriteLeases.find(chunkId);
    return (wi != mWriteLeases.end() ? &wi->second : 0);
}

inline const ChunkLeases::WriteLease*
ChunkLeases::GetValidWriteLease(
    chunkId_t chunkId) const
{
    WriteLeases::const_iterator const wi = mWriteLeases.find(chunkId);
    return ((wi != mWriteLeases.end() && TimeNow() <= wi->second.expires) ?
        &wi->second : 0);
}

inline const ChunkLeases::WriteLease*
ChunkLeases::RenewValidWriteLease(
    chunkId_t chunkId)
{
    WriteLeases::iterator const wi = mWriteLeases.find(chunkId);
    if (wi == mWriteLeases.end()) {
        return 0;
    }
    const time_t now = TimeNow();
    if (wi->second.expires < now) {
        return 0;
    }
    if (! wi->second.allocInFlight) {
        wi->second.expires =
            max(wi->second.expires, now + LEASE_INTERVAL_SECS);
    }
    return (&wi->second);
}

inline bool
ChunkLeases::HasValidWriteLease(
    chunkId_t chunkId) const
{
    WriteLeases::const_iterator const wi = mWriteLeases.find(chunkId);
    return (wi != mWriteLeases.end() && TimeNow() <= wi->second.expires);
}

inline bool
ChunkLeases::HasWriteLease(
    chunkId_t chunkId) const
{
    return (mWriteLeases.find(chunkId) != mWriteLeases.end());
}

inline bool
ChunkLeases::HasValidLease(
    chunkId_t chunkId) const
{
    if (HasValidWriteLease(chunkId)) {
        return true;
    }
    ReadLeases::const_iterator const ri = mReadLeases.find(chunkId);
    if (ri == mReadLeases.end()) {
        return false;
    }
    const time_t now = TimeNow();
    for (ChunkReadLeases::const_iterator it = ri->second.mLeases.begin();
            it != ri->second.mLeases.end(); ++it) {
        if (now <= it->expires) {
            return true;
        }
    }
    return false;
}

inline bool
ChunkLeases::HasLease(
    chunkId_t chunkId) const
{
    ReadLeases::const_iterator const ri = mReadLeases.find(chunkId);
    if (ri != mReadLeases.end() && ! ri->second.mLeases.empty()) {
        return true;
    }
    return (mWriteLeases.find(chunkId) != mWriteLeases.end());
}

inline bool
ChunkLeases::UpdateReadLeaseReplicationCheck(
    chunkId_t chunkId,
    bool      setScheduleReplicationCheckFlag)
{
    ReadLeases::iterator const ri = mReadLeases.find(chunkId);
    if (ri != mReadLeases.end() && ! ri->second.mLeases.empty()) {
        if (setScheduleReplicationCheckFlag) {
            ri->second.mScheduleReplicationCheckFlag = true;
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
    WriteLeases::iterator it = mWriteLeases.find(chunkId);
    if (it == mWriteLeases.end()) {
        return -EINVAL;
    }
    return ReplicaLost(it->second, chunkServer);
}

inline int
ChunkLeases::ReplicaLost(
    ChunkLeases::WriteLease& wl,
    const ChunkServer*       chunkServer)
{
    if (wl.chunkServer.get() == chunkServer && ! wl.relinquishedFlag &&
            ! wl.allocInFlight) {
        const time_t now = TimeNow();
        if (wl.stripedFileFlag && now <= wl.expires) {
            // Keep the valid lease for striped files, instead, to
            // allow lease renewal when/if the next chunk allocation
            // comes in.
            wl.expires = max(wl.expires, now + LEASE_INTERVAL_SECS);
        } else {
            wl.expires = now - 1;
        }
        wl.ownerWasDownFlag = wl.ownerWasDownFlag ||
            (chunkServer && chunkServer->IsDown());
        WriteLease::Mutable(wl.chunkServer).reset();
    }
    return 0;
}

inline void
ChunkLeases::ServerDown(
    const ChunkServerPtr& chunkServer,
    ARAChunkCache&        arac,
    CSMap&                csmap)
{
    for (WriteLeases::iterator it = mWriteLeases.begin();
            it != mWriteLeases.end();
            ) {
        chunkId_t const chunkId = it->first;
        WriteLease&     wl      = it->second;
        CSMap::Entry*   ci      = 0;
        ++it;
        if (wl.appendFlag &&
                (ci = csmap.Find(chunkId)) &&
                csmap.HasServer(chunkServer, *ci)) {
            arac.Invalidate(ci->GetFileId(), chunkId);
        }
        ReplicaLost(wl, chunkServer.get());
    }
}

inline bool
ChunkLeases::ExpiredCleanup(
    ChunkLeases::ReadLeases::iterator ri,
    time_t                            now)
{
    const time_t maxLeaseEndTime = now + LEASE_INTERVAL_SECS;
    for (ChunkReadLeases::iterator it = ri->second.mLeases.begin();
            it != ri->second.mLeases.end(); ) {
        if (it->expires < now) {
            it = ri->second.mLeases.erase(it);
            continue;
        }
        if (it->expires <= maxLeaseEndTime) {
            // List is ordered by expiration time.
            break;
        }
        // Reset to the max allowed.
        it->expires = maxLeaseEndTime;
        ++it;
    }
    if (ri->second.mLeases.empty()) {
        Erase(ri);
        return true;
    }
    return false;
}

inline bool
ChunkLeases::ExpiredCleanup(
    ChunkLeases::WriteLeases::iterator it,
    time_t                             now,
    int                                ownerDownExpireDelay,
    ARAChunkCache&                     arac,
    CSMap&                             csmap)
{
    WriteLease& wl = it->second;
    if (wl.allocInFlight) {
        return false;
    }
    const chunkId_t     chunkId = it->first;
    CSMap::Entry* const ci      = csmap.Find(chunkId);
    if (! ci) {
        Erase(it);
        return true;
    }
    if (now <= wl.expires +
            ((wl.ownerWasDownFlag && ownerDownExpireDelay > 0) ?
                ownerDownExpireDelay : 0)) {
        return false;
    }
    const bool   relinquishedFlag = wl.relinquishedFlag;
    const seq_t  chunkVersion     = wl.chunkVersion;
    const string pathname         = wl.pathname;
    const bool   appendFlag       = wl.appendFlag;
    const bool   stripedFileFlag  = wl.stripedFileFlag;
    Erase(it);
    if (relinquishedFlag) {
        UpdateReplicationState(csmap, *ci);
        return true;
    }
    if (appendFlag) {
        arac.Invalidate(ci->GetFileId(), chunkId);
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
    chunkId_t      chunkId,
    time_t         now,
    int            ownerDownExpireDelay,
    ARAChunkCache& arac,
    CSMap&         csmap)
{
    ReadLeases::iterator const ri = mReadLeases.find(chunkId);
    if (ri != mReadLeases.end()) {
        assert(mWriteLeases.find(chunkId) == mWriteLeases.end());
        const bool ret = ExpiredCleanup(ri, now);
        if (! ret && ! csmap.Find(chunkId)) {
            Erase(ri);
            return true;
        }
        return ret;
    }
    WriteLeases::iterator const wi = mWriteLeases.find(chunkId);
    return (wi == mWriteLeases.end() || ExpiredCleanup(
            wi, now, ownerDownExpireDelay, arac, csmap));
}

inline const char*
ChunkLeases::FlushWriteLease(
    chunkId_t      chunkId,
    ARAChunkCache& arac,
    CSMap&         csmap)
{
    WriteLeases::iterator const wi = mWriteLeases.find(chunkId);
    if (wi == mWriteLeases.end()) {
        return "no write lease";
    }
    WriteLease& wl = wi->second;
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
    wi->second.expires = min(wi->second.expires, now - 1);
    if (ExpiredCleanup(wi, now, 0, arac, csmap)) {
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
    ReadLeases::iterator const ri = mReadLeases.find(req.chunkId);
    if (ri != mReadLeases.end()) {
        assert(mWriteLeases.find(req.chunkId) == mWriteLeases.end());
        for (ChunkReadLeases::iterator it = ri->second.mLeases.begin();
                it != ri->second.mLeases.end(); ++it) {
            if (it->leaseId == req.leaseId) {
                const time_t now = TimeNow();
                const int ret = it->expires < now ?
                    -ELEASEEXPIRED : 0;
                ri->second.mLeases.erase(it);
                if (ri->second.mLeases.empty()) {
                    Erase(ri);
                }
                return ret;
            }
        }
        return -EINVAL;
    }

    WriteLeases::iterator const wi = mWriteLeases.find(req.chunkId);
    if (wi == mWriteLeases.end() || wi->second.leaseId != req.leaseId) {
        return -EINVAL;
    }
    const CSMap::Entry* const ci = csmap.Find(req.chunkId);
    if (! ci) {
        return -ELEASEEXPIRED;
    }
    WriteLease& wl = wi->second;
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
        lr.leaseId          = req.leaseId;
        lr.chunkSize        = req.chunkSize;
        lr.hasChunkChecksum = req.hasChunkChecksum;
        lr.chunkChecksum    = req.chunkChecksum;
        return 0;
    }
    const time_t now = TimeNow();
    const int    ret = wl.expires < now ? -ELEASEEXPIRED : 0;
    const bool   hadLeaseFlag = ! wl.relinquishedFlag;
    WriteLease::Mutable(wl.chunkServer).reset();
    wl.relinquishedFlag = true;
    // the owner of the lease is giving up the lease; update the expires so
    // that the normal lease cleanup will work out.
    wl.expires = min(time_t(0), now - 100 * LEASE_INTERVAL_SECS);
    if (hadLeaseFlag) {
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
    }
    return ret;
}

inline bool
ChunkLeases::Timer(
    time_t         now,
    int            ownerDownExpireDelay,
    ARAChunkCache& arac,
    CSMap&         csmap)
{
    if (mTimerRunningFlag) {
        return false; // Do not allow recursion.
    }
    mTimerRunningFlag = true;
    bool cleanedFlag = false;
    for (ReadLeases::iterator ri = mReadLeases.begin();
            ri != mReadLeases.end(); ) {
        ReadLeases::iterator const it = ri++;
        if (ExpiredCleanup(it, now)) {
            cleanedFlag = true;
        }
    }
    for (mCurWrIt = mWriteLeases.begin();
            mCurWrIt != mWriteLeases.end(); ) {
        WriteLeases::iterator const it = mCurWrIt++;
        if (ExpiredCleanup(
                it,
                now,
                ownerDownExpireDelay,
                arac,
                csmap)) {
            cleanedFlag = true;
        }
    }
    mTimerRunningFlag = false;
    return cleanedFlag;
}

inline bool
ChunkLeases::NewReadLease(
    chunkId_t              chunkId,
    time_t                 expires,
    ChunkLeases::LeaseId&  leaseId)
{
    if (mWriteLeases.find(chunkId) != mWriteLeases.end()) {
        assert(mReadLeases.find(chunkId) == mReadLeases.end());
        return false;
    }
    // Keep list sorted by expiration time.
    const LeaseId id = NewReadLeaseId();
    ChunkReadLeases& rl = mReadLeases[chunkId].mLeases;
    ChunkReadLeases::iterator it = rl.end();
    while (it != rl.begin()) {
        --it;
        if (it->expires <= expires) {
            ++it;
            break;
        }
    }
    rl.insert(it, ReadLease(id, expires));
    leaseId = id;
    mLeaseId = id + 1;
    return true;
}

inline bool
ChunkLeases::NewWriteLease(
    chunkId_t             chunkId,
    seq_t                 chunkVersion,
    time_t                expires,
    const ChunkServerPtr& server,
    const string&         path,
    bool                  append,
    bool                  stripedFileFlag,
    const MetaAllocate*   allocInFlight,
    ChunkLeases::LeaseId& leaseId)
{
    if (mReadLeases.find(chunkId) != mReadLeases.end()) {
        assert(mWriteLeases.find(chunkId) == mWriteLeases.end());
        return false;
    }
    const LeaseId id = NewWriteLeaseId();
    WriteLease const wl(
        id,
        chunkVersion,
        server,
        path,
        append,
        stripedFileFlag,
        allocInFlight,
        expires
    );
    pair<WriteLeases::iterator,bool> const res =
        mWriteLeases.insert(make_pair(chunkId, wl));
    leaseId = res.first->second.leaseId;
    if (res.second) {
        mLeaseId = id + 1;
    }
    return res.second;
}

inline int
ChunkLeases::Renew(
    chunkId_t            chunkId,
    ChunkLeases::LeaseId leaseId,
    bool                 allocDoneFlag /* = false */)
{
    if (IsReadLease(leaseId)) {
        ReadLeases::iterator const ri = mReadLeases.find(chunkId);
        if (ri == mReadLeases.end()) {
            return -EINVAL;
        }
        assert(mWriteLeases.find(chunkId) == mWriteLeases.end());
        for (ChunkReadLeases::iterator it = ri->second.mLeases.begin();
                it != ri->second.mLeases.end(); ++it) {
            if (it->leaseId == leaseId) {
                const time_t now = TimeNow();
                if (it->expires < now) {
                    // Don't renew expired leases.
                    ri->second.mLeases.erase(it);
                    if (ri->second.mLeases.empty()) {
                        Erase(ri);
                    }
                    return -ELEASEEXPIRED;
                }
                it->expires = now + LEASE_INTERVAL_SECS;
                // Keep the list sorted by expiration time.
                // Max expiration time is
                // now + LEASE_INTERVAL_SECS
                ri->second.mLeases.splice(
                    ri->second.mLeases.end(),
                    ri->second.mLeases, it);
                return 0;
            }
        }
        return -EINVAL;
    }
    WriteLeases::iterator const wi = mWriteLeases.find(chunkId);
    if (wi == mWriteLeases.end() || wi->second.leaseId != leaseId) {
        return -EINVAL;
    }
    assert(mReadLeases.find(chunkId) == mReadLeases.end());
    const time_t now = TimeNow();
    if (wi->second.expires < now && ! wi->second.allocInFlight) {
        // Don't renew expired leases, and let the timer to clean it up
        // to avoid posible recursion.
        return -ELEASEEXPIRED;
    }
    if (allocDoneFlag) {
        wi->second.allocInFlight = 0;
    }
    if (! wi->second.allocInFlight) {
        wi->second.expires = now + LEASE_INTERVAL_SECS;
    }
    return 0;
}

inline bool
ChunkLeases::DeleteWriteLease(
    chunkId_t            chunkId,
    ChunkLeases::LeaseId leaseId)
{
    WriteLeases::iterator const wi = mWriteLeases.find(chunkId);
    if (wi == mWriteLeases.end() || wi->second.leaseId != leaseId) {
        return false;
    }
    Erase(wi);
    return true;
}

inline void
ChunkLeases::SetMaxLeaseId(
    ChunkLeases::LeaseId id)
{
    if (id > mLeaseId) {
        mLeaseId = id;
    }
}

inline bool
ChunkLeases::Delete(
    chunkId_t chunkId)
{
    WriteLeases::iterator const wi = mWriteLeases.find(chunkId);
    const bool hadWr = wi != mWriteLeases.end();
    if (hadWr) {
        Erase(wi);
    }
    ReadLeases::iterator ri = mReadLeases.find(chunkId);
    const bool hadRd = ri != mReadLeases.end();
    if (hadRd) {
        Erase(ri);
    }
    assert(! hadWr || ! hadRd);
    return (hadWr || hadRd);
}

inline void
ChunkLeases::GetOpenFiles(
    MetaOpenFiles::ReadInfo&  openForRead,
    MetaOpenFiles::WriteInfo& openForWrite,
    const CSMap&              csmap) const
{
    const time_t now = TimeNow();
    for (ReadLeases::const_iterator ri = mReadLeases.begin();
            ri != mReadLeases.end(); ++ri) {
        const CSMap::Entry* const ci = csmap.Find(ri->first);
        if (! ci) {
            continue;
        }
        size_t count = 0;
        for (ChunkReadLeases::const_iterator
                it = ri->second.mLeases.begin();
                it != ri->second.mLeases.end();
                ++it) {
            if (now <= it->expires) {
                count++;
            }
        }
        if (count > 0) {
            openForRead[ci->GetFileId()]
                .push_back(make_pair(ri->first, count));
        }
    }
    for (WriteLeases::const_iterator wi = mWriteLeases.begin();
            wi != mWriteLeases.end(); ++wi) {
        if (now <= wi->second.expires) {
            const CSMap::Entry* const ci = csmap.Find(wi->first);
            if (ci) {
                openForWrite[ci->GetFileId()]
                    .push_back(wi->first);
            }
        }
    }
}

class MatchingServer
{
    const ServerLocation loc;
public:
    MatchingServer(const ServerLocation& l) : loc(l) {}
    bool operator() (const ChunkServerPtr &s) const {
        return s->MatchingServer(loc);
    }
};

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
LayoutManager::AddHosted(CSMap::Entry& entry, const ChunkServerPtr& c)
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
    return mChunkToServerMap.AddServer(c, entry);
}

inline bool
LayoutManager::AddHosted(chunkId_t chunkId, CSMap::Entry& entry, const ChunkServerPtr& c)
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
    ChunkVersionRollBack::iterator const it =
        mChunkVersionRollBack.find(chunkId);
    if (it != mChunkVersionRollBack.end()) {
        if (it->second <= 0) {
            ostringstream os;
            os <<
            "invalid chunk roll back entry:"
            " chunk: "             << it->first <<
            " version increment: " << it->second;
            const string msg = os.str();
            panic(msg.c_str());
            mChunkVersionRollBack.erase(it);
        } else {
            return it->second;
        }
    }
    return 0;
}

inline seq_t
LayoutManager::IncrementChunkVersionRollBack(chunkId_t chunkId)
{
    pair<ChunkVersionRollBack::iterator, bool> const res =
        mChunkVersionRollBack.insert(make_pair(chunkId, 0));
    if (! res.second && res.first->second <= 0) {
        ostringstream os;
        os <<
        "invalid chunk roll back entry:"
        " chunk: "             << res.first->first <<
        " version increment: " << res.first->second;
        const string msg = os.str();
        panic(msg.c_str());
        res.first->second = 0;
    }
    ++(res.first->second);
    return res.first->second;
}

LayoutManager::Random::result_type
LayoutManager::RandSeed()
{
    Random::result_type theRet = 1;
    RAND_pseudo_bytes(
        reinterpret_cast<unsigned char*>(&theRet),
        int(sizeof(theRet))
    );
    return theRet;
}

int64_t
LayoutManager::Rand(int64_t interval)
{
    // Use this simpler and hopefully faster version instead of
    // variate_generator<mt19937, uniform_int<int> >
    // Scaling up and down 32 bit random number should be more than
    // adequate for now, even though the result will at most 32
    // random bits.

    // Don't use modulo, low order bits might be "less random".
    // Though this shouldn't be a problem with Mersenne twister.
    return min((int64_t)((uint64_t)(mRandom() - mRandMin) * interval /
        mRandInterval), interval - 1);
}

LayoutManager::ChunkPlacement::ChunkPlacement()
    : Super(gLayoutManager)
{
    Reserve(512);
}

LayoutManager::LayoutManager() :
    mNumOngoingReplications(0),
    mIsRebalancingEnabled(true),
    mMaxRebalanceSpaceUtilThreshold(0.85),
    mMinRebalanceSpaceUtilThreshold(0.75),
    mIsExecutingRebalancePlan(false),
    mRecoveryStartTime(0),
    mStartTime(time(0)),
    mRecoveryIntervalSec(LEASE_INTERVAL_SECS),
    mLeaseCleaner(60 * 1000),
    mChunkReplicator(5 * 1000),
    mCheckpoint(5 * 1000),
    mMinChunkserversToExitRecovery(1),
    mMastersCount(0),
    mSlavesCount(0),
    mAssignMasterByIpFlag(false),
    mLeaseOwnerDownExpireDelay(30),
    mMaxReservationSize(4 << 20),
    mReservationDecayStep(4), // decrease by factor of 2 every 4 sec
    mChunkReservationThreshold(CHUNKSIZE),
    mAllocAppendReuseInFlightTimeoutSec(25),
    mMinAppendersPerChunk(96),
    mMaxAppendersPerChunk(4 << 10),
    mReservationOvercommitFactor(1.0),
    mServerDownReplicationDelay(2 * 60),
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
    mRecomputeDirSizesIntervalSec(60 * 60 * 24 * 3650),
    mMaxConcurrentWriteReplicationsPerNode(5),
    mMaxConcurrentReadReplicationsPerNode(10),
    mUseEvacuationRecoveryFlag(true),
    mReplicationFindWorkTimeouts(0),
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
    mMTimeUpdateResolution(kSecs2MicroSecs),
    mMaxPendingRecoveryMsgLogInfo(1 << 10),
    mAllowLocalPlacementFlag(true),
    mAllowLocalPlacementForAppendFlag(false),
    mInRackPlacementForAppendFlag(false),
    mInRackPlacementFlag(false),
    mAllocateDebugVerifyFlag(false),
    mChunkEntryToChange(0),
    mFattrToChangeTo(0),
    mCSLoadAvgSum(0),
    mCSMasterLoadAvgSum(0),
    mCSSlaveLoadAvgSum(0),
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
    mAppendCacheCleanupInterval(-1),
    mTotalChunkWrites(0),
    mTotalWritableDrives(0),
    mMinWritesPerDrive(10),
    mMaxWritesPerDriveThreshold(mMinWritesPerDrive),
    mMaxWritesPerDriveRatio(1.5),
    mMaxLocalPlacementWeight(1.0),
    mTotalWritableDrivesMult(0.),
    mConfig(),
    mDefaultUser(kKfsUserNone),      // Request defaults
    mDefaultGroup(kKfsGroupNone),
    mDefaultFileMode(0644),
    mDefaultDirMode(0755),
    mDefaultLoadUser(kKfsUserRoot),  // Checkpoint load and replay defaults
    mDefaultLoadGroup(kKfsGroupRoot),
    mDefaultLoadFileMode(0666),
    mDefaultLoadDirMode(0777),
    mForceEUserToRootFlag(false),
    mVerifyAllOpsPermissionsFlag(false),
    mRootHosts(),
    mHostUserGroupRemap(),
    mLastUidGidRemap(),
    mIoBufPending(0),
    mChunkInfosTmp(),
    mChunkInfos2Tmp(),
    mServersTmp(),
    mServers2Tmp(),
    mServers3Tmp(),
    mServers4Tmp(),
    mChunkPlacementTmp(),
    mRandom(RandSeed()),
    mRandMin(mRandom.min()),
    mRandInterval(mRandom.max() - mRandMin)
{
    globals();
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
}

LayoutManager::~LayoutManager()
{
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
        globalNetManager().UnRegisterTimeoutHandler(this);
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

void
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

    mLeaseCleaner.SetTimeoutInterval((int)(props.getValue(
        "metaServer.leaseCleanupInterval",
        mLeaseCleaner.GetTimeoutInterval() * 1e-3) * 1e3));
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

    mMaxFsckFiles = props.getValue(
        "metaServer.maxFsckChunks",
        mMaxFsckFiles);
    mFsckAbandonedFileTimeout = (int64_t)(props.getValue(
        "metaServer.fsckAbandonedFileTimeout",
        mFsckAbandonedFileTimeout * 1e-6) * 1e6);
    mMaxFsckTime = (int64_t)(props.getValue(
        "metaServer.mMaxFsckTime",
        mMaxFsckTime * 1e-6) * 1e6);
    mFullFsckFlag = props.getValue(
        "metaServer.fullFsck",
        mFullFsckFlag ? 1 : 0) != 0;

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
    mClusterKey = props.getValue("metaServer.clusterKey", string());

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

    SetChunkServersProperties(props);
    ClientSM::SetParameters(props);
    gNetDispatch.SetParameters(props);
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
    mAppendCacheCleanupInterval = (int)props.getValue(
        "metaServer.appendCacheCleanupInterval",
        double(mAppendCacheCleanupInterval));
    UpdateReplicationsThreshold();
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
    mVerifyAllOpsPermissionsFlag = props.getValue(
        "metaServer.verifyAllOpsPermissions",
        mVerifyAllOpsPermissionsFlag ? 1 : 0) != 0;
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
    mConfig.clear();
    mConfig.reserve(10 << 10);
    props.getList(mConfig, string(), string(";"));
}

void
LayoutManager::UpdateReplicationsThreshold()
{
    const int64_t srvCnt = (int64_t)mChunkServers.size();
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
LayoutManager::Validate(MetaHello& r) const
{
    if (r.clusterKey != mClusterKey) {
        r.statusMsg = "cluster key mismatch:"
            " expect: "   + mClusterKey +
            " recieved: " + r.clusterKey;
        r.status = -EBADCLUSTERKEY;
        return false;
    }
    if (mChunkServerMd5sums.empty() || find(
                mChunkServerMd5sums.begin(),
                mChunkServerMd5sums.end(),
                r.md5sum) != mChunkServerMd5sums.end()) {
        return true;
    }
    r.statusMsg = "MD5sum mismatch: recieved: " + r.md5sum;
    r.status    = -EBADCLUSTERKEY;
    return false;
}

LayoutManager::RackId
LayoutManager::GetRackId(const ServerLocation& loc)
{
    return mRackPrefixes.GetId(loc, -1, mRackPrefixUsePortFlag);
}

LayoutManager::RackId
LayoutManager::GetRackId(const string& name)
{
    return mRackPrefixes.GetId(name, -1);
}

void
LayoutManager::SetChunkServersProperties(const Properties& props)
{
    if (props.empty()) {
        return;
    }
    props.copyWithPrefix("chunkServer.", mChunkServersProps);
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
    // Return io buffers back into the pool.
    mCSCountersResponse.Clear();
    mCSDirCountersResponse.Clear();
    mPingResponse.Clear();
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
    static const Properties::String kCseq("Cseq");

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
                columns.remove(kCseq);
                writeHeader(writer, columns, kColumnDelim);
                writer.Write(kRowDelim);
            }
            for ( ; ctrFirst != ctrLast; ++ctrFirst) {
                const Properties&    props = getCounters(ctrFirst);
                Properties::iterator ci    = columns.begin();
                for (Properties::iterator pi = props.begin();
                        pi != props.end();
                        ++pi) {
                    if (pi->first == kCseq) {
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

struct CtrWriteExtra
{
protected:
    CtrWriteExtra()
        : mBufEnd(mBuf + kBufSize)
        {}
    const Properties::String& BoolToString(bool flag) const
    {
        return (flag ? kTrueStr : kFalseStr);
    }
    template<typename T>
    void Write(IOBufferWriter& writer, T val)
    {
        const char* const b = IntToDecString(val, mBufEnd);
        writer.Write(b, mBufEnd - b);
    }

    enum { kBufSize = 32 };

    char        mBuf[kBufSize];
    char* const mBufEnd;

    static const Properties::String kFalseStr;
    static const Properties::String kTrueStr;
};

const Properties::String CtrWriteExtra::kFalseStr("0");
const Properties::String CtrWriteExtra::kTrueStr ("1");

struct GetHeartbeatCounters
{
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
    "XMeta-to-evacuate-cnt"
};

struct CSWriteExtra : public CtrWriteExtra
{
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
        writer.Write(srv.GetHostPortStr());
        writer.Write(columnDelim);
        writer.Write(BoolToString(srv.IsRetiring()));
        writer.Write(columnDelim);
        writer.Write(BoolToString(srv.IsRestartScheduled()));
        writer.Write(columnDelim);
        writer.Write(BoolToString(srv.IsResponsiveServer()));
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
    }
private:
    const RackInfos& mRacks;
    const time_t     mNow;
};

struct GetChunkDirCounters
{
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

struct CSDirWriteExtra : public CtrWriteExtra
{
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

struct CSWriteHeader
{
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

//
// Try to match servers by hostname: for write allocation, we'd like to place
// one copy of the block on the same host on which the client is running.
//
template <typename T>
class HostNameEqualsTo
{
    const T& host;
public:
    HostNameEqualsTo(const T &h) : host(h) {}
    bool operator()(const ChunkServerPtr &s) const {
        return s->GetServerLocation().hostname == host;
    }
};
template <typename T> HostNameEqualsTo<T>
MatchServerByHost(const T& host) { return HostNameEqualsTo<T>(host); }

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
        mChunkLeases.GetWriteLease(chunkId);
    return (wl && wl->appendFlag);
}

/// Add the newly joined server to the list of servers we have.  Also,
/// update our state to include the chunks hosted on this server.
void
LayoutManager::AddNewServer(MetaHello *r)
{
    if (r->server->IsDown()) {
        return;
    }
    ChunkServer& srv = *r->server.get();
    srv.SetServerLocation(r->location);

    const string srvId = r->location.ToString();
    Servers::iterator const existing = find_if(
        mChunkServers.begin(), mChunkServers.end(),
        MatchingServer(r->location));
    if (existing != mChunkServers.end()) {
        KFS_LOG_STREAM_DEBUG << "duplicate server: " << srvId <<
            " possible reconnect, taking: " <<
                (const void*)existing->get() << " down " <<
            " replacing with: " << (const void*)&srv <<
        KFS_LOG_EOM;
        ServerDown(*existing);
        if (srv.IsDown()) {
            return;
        }
    }

    // Add server first, then add chunks, otherwise if/when the server goes
    // down in the process of adding chunks, taking out server from chunk
    // info will not work in ServerDown().
    if ( ! mChunkToServerMap.AddServer(r->server)) {
        KFS_LOG_STREAM_WARN <<
            "failed to add server: " << srvId <<
            " no slots available "
            " servers: " << mChunkToServerMap.GetServerCount() <<
            " / " << mChunkServers.size() <<
        KFS_LOG_EOM;
        srv.ForceDown();
        return;
    }
    mChunkServers.push_back(r->server);

    const uint64_t allocSpace = r->chunks.size() * CHUNKSIZE;
    srv.SetSpace(r->totalSpace, r->usedSpace, allocSpace);
    RackId rackId = GetRackId(r->location);
    if (rackId < 0 && r->rackId >= 0) {
        rackId = r->rackId;
    }
    srv.SetRack(rackId);
    // Ensure that rack exists before invoking UpdateSrvLoadAvg(), as it
    // can update rack possible allocation candidates count.
    if (rackId >= 0) {
        RackInfos::iterator const rackIter = find_if(
            mRacks.begin(), mRacks.end(),
            bind(&RackInfo::id, _1) == rackId);
        if (rackIter != mRacks.end()) {
            rackIter->addServer(r->server);
        } else {
            RackWeights::const_iterator const
                it = mRackWeights.find(rackId);
            mRacks.push_back(RackInfo(
                rackId,
                it != mRackWeights.end() ?
                    it->second : double(1),
                r->server
            ));
        }
    } else {
        KFS_LOG_STREAM_INFO << srvId <<
            ": no rack specified: " << rackId <<
        KFS_LOG_EOM;
    }
    UpdateSrvLoadAvg(srv, 0);

    if (mAssignMasterByIpFlag) {
        // if the server node # is odd, it is master; else slave
        string ipaddr = r->peerName;
        string::size_type delimPos = ipaddr.rfind(':');
        if (delimPos != string::npos) {
            ipaddr.erase(delimPos);
        }
        delimPos = ipaddr.rfind('.');
        if (delimPos == string::npos) {
            srv.SetCanBeChunkMaster(Rand(2) != 0);
        } else {
            string nodeNumStr = ipaddr.substr(delimPos + 1);
            srv.SetCanBeChunkMaster((toNumber(nodeNumStr) % 2) != 0);
        }
    } else {
        srv.SetCanBeChunkMaster(mSlavesCount >= mMastersCount);
    }
    if (srv.CanBeChunkMaster()) {
        mMastersCount++;
    } else {
        mSlavesCount++;
    }

    int maxLogInfoCnt = 32;
    ChunkIdQueue staleChunkIds;
    for (MetaHello::ChunkInfos::const_iterator it = r->chunks.begin();
            it != r->chunks.end() && ! srv.IsDown();
            ++it) {
        const chunkId_t     chunkId      = it->chunkId;
        const char*         staleReason  = 0;
        CSMap::Entry* const cmi          = mChunkToServerMap.Find(chunkId);
        seq_t               chunkVersion = -1;
        if (cmi) {
            CSMap::Entry&        c      = *cmi;
            const fid_t          fileId = c.GetFileId();
            const ChunkServerPtr cs     = c.GetServer(
                mChunkToServerMap, srv.GetServerLocation());
            if (cs) {
                KFS_LOG_STREAM_ERROR << srvId <<
                    " stable chunk: <" <<
                        fileId << "/" <<
                        it->allocFileId << "," <<
                        chunkId << ">" <<
                    " already hosted on: " <<
                        (const void*)cs.get() <<
                    " new server: " <<
                        (const void*)&srv <<
                    " has the same location: " <<
                        srv.GetServerLocation() <<
                    (cs.get() == &srv ?
                        " duplicate chunk entry" :
                        " possible stale chunk to"
                        " server mapping entry"
                    ) <<
                KFS_LOG_EOM;
                if (cs.get() == &srv) {
                    // Ignore duplicate chunk inventory entries.
                    continue;
                }
            }
            const MetaFattr&     fa = *(cmi->GetFattr());
            const MetaChunkInfo& ci = *(cmi->GetChunkInfo());
            chunkVersion = ci.chunkVersion;
            if (chunkVersion > it->chunkVersion) {
                staleReason = "lower chunk version";
            } else if (chunkVersion +
                    GetChunkVersionRollBack(chunkId) <
                    it->chunkVersion) {
                staleReason = "higher chunk version";
            } else {
                if (chunkVersion != it->chunkVersion) {
                    bool kMakeStableFlag = false;
                    bool kPendingAddFlag = true;
                    srv.NotifyChunkVersChange(
                        fileId,
                        chunkId,
                        chunkVersion,
                        it->chunkVersion,
                        kMakeStableFlag,
                        kPendingAddFlag
                    );
                    continue;
                }
                const ChunkLeases::WriteLease* const wl =
                    mChunkLeases.GetWriteLease(chunkId);
                if (wl && wl->allocInFlight &&
                        wl->allocInFlight->status == 0) {
                    staleReason = "chunk allocation in flight";
                } else {
                    // This chunk is non-stale. Check replication,
                    // and update file size if this is the last
                    // chunk and update required.
                    const int res = AddHosted(c, r->server);
                    assert(res >= 0);
                    if (! fa.IsStriped() && fa.filesize < 0 &&
                            ci.offset +
                                (chunkOff_t)CHUNKSIZE >=
                            fa.nextChunkOffset()) {
                        KFS_LOG_STREAM_DEBUG << srvId <<
                            " chunk size: <" << fileId <<
                            "," << chunkId << ">" <<
                        KFS_LOG_EOM;
                        srv.GetChunkSize(fileId, chunkId, chunkVersion, "");
                    }
                    if (! srv.IsDown()) {
                        const int srvCount =
                            (int)mChunkToServerMap.ServerCount(c);
                        if (fa.numReplicas <= srvCount) {
                            CancelPendingMakeStable(fileId, chunkId);
                        }
                        if (fa.numReplicas != srvCount) {
                            CheckReplication(c);
                        }
                    }
                }
            }
        } else {
            staleReason = "no chunk mapping exists";
        }
        if (staleReason) {
            maxLogInfoCnt--;
            KFS_LOG_STREAM((maxLogInfoCnt > 0) ?
                    MsgLogger::kLogLevelINFO :
                    MsgLogger::kLogLevelDEBUG) <<
                srvId <<
                " stable chunk: <" <<
                it->allocFileId << "," << chunkId << ">"
                " version: " << it->chunkVersion <<
                "/" << chunkVersion <<
                " " << staleReason <<
                " => stale" <<
            KFS_LOG_EOM;
            staleChunkIds.PushBack(it->chunkId);
            mStaleChunkCount->Update(1);
        }
    }

    for (int i = 0; i < 2; i++) {
        const MetaHello::ChunkInfos& chunks = i == 0 ?
            r->notStableAppendChunks : r->notStableChunks;
        int maxLogInfoCnt = 64;
        for (MetaHello::ChunkInfos::const_iterator it = chunks.begin();
                it != chunks.end() && ! srv.IsDown();
                ++it) {
            const char* const staleReason = AddNotStableChunk(
                r->server,
                it->allocFileId,
                it->chunkId,
                it->chunkVersion,
                i == 0,
                srvId
            );
            maxLogInfoCnt--;
            KFS_LOG_STREAM((maxLogInfoCnt > 0) ?
                    MsgLogger::kLogLevelINFO :
                    MsgLogger::kLogLevelDEBUG) <<
                srvId <<
                " not stable chunk:" <<
                (i == 0 ? " append" : "") <<
                " <" <<
                    it->allocFileId << "," << it->chunkId << ">"
                " version: " << it->chunkVersion <<
                " " << (staleReason ? staleReason : "") <<
                (staleReason ? " => stale" : "added back") <<
            KFS_LOG_EOM;
            if (staleReason) {
                staleChunkIds.PushBack(it->chunkId);
                mStaleChunkCount->Update(1);
            }
            // MakeChunkStableDone will process pending recovery.
        }
    }
    const size_t staleCnt = staleChunkIds.GetSize();
    if (! staleChunkIds.IsEmpty() && ! srv.IsDown()) {
        srv.NotifyStaleChunks(staleChunkIds);
    }
    if (! mChunkServersProps.empty() && ! srv.IsDown()) {
        srv.SetProperties(mChunkServersProps);
    }
    // All ops are queued at this point, make sure that the server is still up.
    if (srv.IsDown()) {
        KFS_LOG_STREAM_ERROR << srvId <<
            ": went down in the process of adding it" <<
        KFS_LOG_EOM;
        return;
    }

    // Update the list since a new server is in
    CheckHibernatingServersStatus();

    const char* msg = "added";
    if (IsChunkServerRestartAllowed() &&
            mCSToRestartCount < mMaxCSRestarting) {
        if (srv.Uptime() >= GetMaxCSUptime() &&
                ! srv.IsDown() &&
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
    KFS_LOG_STREAM_INFO <<
        msg << " chunk server: " << r->peerName << "/" <<
            srv.GetServerLocation() <<
        (srv.CanBeChunkMaster() ? " master" : " slave") <<
        " rack: "            << r->rackId << " => " << rackId <<
        " chunks: stable: "  << r->chunks.size() <<
        " not stable: "      << r->notStableChunks.size() <<
        " append: "          << r->notStableAppendChunks.size() <<
        " +wid: "            << r->numAppendsWithWid <<
        " writes: "          << srv.GetNumChunkWrites() <<
        " +wid: "            << srv.GetNumAppendsWithWid() <<
        " stale: "           << staleCnt <<
        " masters: "         << mMastersCount <<
        " slaves: "          << mSlavesCount <<
        " total: "           << mChunkServers.size() <<
        " uptime: "          << srv.Uptime() <<
        " restart: "         << srv.IsRestartScheduled() <<
        " 2restart: "        << mCSToRestartCount <<
        " 2restartmasters: " << mMastersToRestartCount <<
    KFS_LOG_EOM;
}

const char*
LayoutManager::AddNotStableChunk(
    const ChunkServerPtr& server,
    fid_t                 allocFileId,
    chunkId_t             chunkId,
    seq_t                 chunkVersion,
    bool                  appendFlag,
    const string&         logPrefix)
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
            "/"                    << allocFileId <<
            ","                    << chunkId << ">" <<
            " already hosted on: " << (const void*)cs.get() <<
            " new server: "        << (const void*)server.get() <<
            (cs.get() == server.get() ?
            " duplicate chunk entry" :
            " possible stale chunk to server mapping entry") <<
        KFS_LOG_EOM;
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
    if (fa && fa->numReplicas <= (int)mChunkToServerMap.ServerCount(pinfo)) {
        CancelPendingMakeStable(fileId, chunkId);
        return "sufficient number of replicas exists";
    }
    // See if it is possible to add the chunk back before "[Begin] Make
    // Chunk Stable" ([B]MCS) starts. Expired lease cleanup lags behind. If
    // expired lease exists, and [B]MCS is not in progress, then add the
    // chunk back.
    const ChunkLeases::WriteLease* const wl =
        mChunkLeases.GetWriteLease(chunkId);
    if (wl && appendFlag != wl->appendFlag) {
        return (appendFlag ? "not append lease" : "append lease");
    }
    if ((! wl || wl->relinquishedFlag) && appendFlag) {
        PendingMakeStableMap::iterator const msi =
                mPendingMakeStable.find(chunkId);
        if (msi == mPendingMakeStable.end()) {
            return "no make stable info";
        }
        if (chunkVersion != msi->second.mChunkVersion) {
            return "pending make stable chunk version mismatch";
        }
        const bool beginMakeStableFlag = msi->second.mSize < 0;
        if (beginMakeStableFlag) {
            AddHosted(chunkId, pinfo, server);
            if (InRecoveryPeriod() ||
                    ! mPendingBeginMakeStable.empty()) {
                // Allow chunk servers to connect back.
                mPendingBeginMakeStable.insert(
                    make_pair(chunkId, chunkVersion));
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
            msi->second.mSize,
            msi->second.mHasChecksum,
            msi->second.mChecksum,
            kPendingAddFlag
        );
        return 0;
    }
    if (! wl && ! appendFlag &&
            mPendingMakeStable.find(chunkId) !=
            mPendingMakeStable.end()) {
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
            if (wl->allocInFlight->CheckStatus() == 0) {
                return "re-allocation in flight";
            }
            if (wl->allocInFlight->chunkVersion != chunkVersion) {
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
    if (req.replicate) {
        assert(req.replicate->versChange = &req);
        ChunkReplicationDone(req.replicate);
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
        req.server->NotifyStaleChunk(req.chunkId);
        return;
    }
    UpdateReplicationState(*cmi);

    if (req.status != 0) {
        KFS_LOG_STREAM_ERROR    << req.Show() <<
            " status: "     << req.status <<
            " msg: "        << req.statusMsg <<
            " pendingAdd: " << req.pendingAddFlag <<
        KFS_LOG_EOM;
        if (! req.pendingAddFlag) {
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
            mChunkLeases.GetWriteLease(req.chunkId)) {
        KFS_LOG_STREAM_INFO << req.Show() <<
            " no version roll back or write lese exists,"
            " declaring stale replica" <<
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
    if (! AddHosted(*cmi, req.server)) {
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
            ci->offset + (chunkOff_t)CHUNKSIZE >=
                fa->nextChunkOffset() &&
            fa->filesize < 0) {
        KFS_LOG_STREAM_DEBUG <<
            " get chunk size: <" << fileId <<
            "," << req.chunkId << ">" <<
        KFS_LOG_EOM;
        req.server->GetChunkSize(
            fileId, req.chunkId, req.chunkVersion, string());
    }
    if (req.server->IsDown()) {
        // Went down in GetChunkSize().
        return;
    }
    const int srvCount = (int)mChunkToServerMap.ServerCount(*cmi);
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
    if (mPendingBeginMakeStable.empty()) {
        return;
    }
    PendingBeginMakeStable pendingBeginMakeStable;
    pendingBeginMakeStable.swap(mPendingBeginMakeStable);
    const bool kBeginMakeStableFlag = true;
    const bool kStripedFileFlag     = false;
    for (PendingBeginMakeStable::const_iterator
            it = pendingBeginMakeStable.begin();
            it != pendingBeginMakeStable.end();
            ++it) {
        CSMap::Entry* const cmi = mChunkToServerMap.Find(it->first);
        if (! cmi) {
            continue;
        }
        const bool    appendFlag          = true;
        const bool    leaseRelinquishFlag = false;
        MakeChunkStableInit(
            *cmi,
            it->second,
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
LayoutManager::DumpChunkReplicationCandidates(MetaDumpChunkReplicationCandidates* op)
{
    int64_t total   = 0;
    int64_t outLeft = mMaxFsckFiles;
    op->resp.Clear();
    ostream& os = mWOstream.Set(op->resp, mMaxResponseSize);
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
        op->status     = -ENOMEM;
        op->statusMsg  = "response exceeds max. size";
    }
    mWOstream.Reset();
    if (op->status == 0) {
        op->numReplication     = total;
        op->numPendingRecovery = mChunkToServerMap.GetCount(
            CSMap::Entry::kStatePendingRecovery);
    } else {
        op->numReplication     = 0;
        op->numPendingRecovery = 0;
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
    int              stripeIdx = 0;
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
            if (mChunkLeases.GetWriteLease((*it)->chunkId)) {
                *incompleteChunkBlockWriteHasLeaseFlag = true;
                break;
            }
        }
    }
    return (fa->numStripes <= goodCnt);
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

void
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
          mPlacement(),
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
          mMaxReplication(0)
    {
        mPath.reserve(8 << 10);
        for (int i = 0, k = 0; i < kStateCount; i++) {
            if ((mOs[i] = os ? os[k] : 0)) {
                k++;
            }
            mFileCounts[i] = 0;
        }
    }
    bool operator()(const MetaDentry& de,
            const MetaFattr&  fa,
            size_t            depth)
    {
        if (mStopFlag) {
            return false;
        }
        if (depth < mDepth) {
            mDepth = depth;
            mPath.resize(mDepth);
        } else if (depth > mDepth) {
            assert(depth == mDepth + 1);
            mDepth = depth;
        }
        if (fa.type == KFS_DIR) {
            mPath.resize(mDepth);
            mPath.push_back(&de);
            mMaxDirDepth = max(mMaxDirDepth, mDepth);
            mDirCount++;
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
        mMaxReplication  = max(mMaxReplication, fa.numReplicas);
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
        " " << fa.chunkcount() <<
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
    void Report(size_t chunkCount)
    {
        if (! mOs[kStateNone]) {
            return;
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
        "Fsck run time: "  <<
            (microseconds() - mStartTime) * 1e-6 << " sec.\n"
        "Files: [fsck_state size replication type stripes"
            " recovery_stripes stripe_size chunk_count mtime"
            " path]\n"
        ;
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
        if (recoveryStripeCnt > 0 && chunkBlockCount > 0 &&
                (status != FilesChecker::kLost ||
                    fa.filesize <= 0 ||
                    invalidBlkFlag) &&
                fa.mtime + mPastEofRecoveryDelay <
                    fsck.StartTime() &&
                fa.filesize <= (chunkBlockCount - 1) *
                    fa.numStripes * (chunkOff_t)CHUNKSIZE &&
                fa.mtime + mFsckAbandonedFileTimeout <
                    fsck.StartTime()) {
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

void
LayoutManager::Fsck(ostream** os, bool reportAbandonedFilesFlag)
{
    if (mFullFsckFlag) {
        FilesChecker fsck(*this, mMaxFsckFiles, os);
        metatree.iterateDentries(fsck);
        fsck.Report(mChunkToServerMap.Size());
    } else if (os && os[0]) {
        Fsck(*(os[0]), reportAbandonedFilesFlag);
    }
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
LayoutManager::ServerDown(const ChunkServerPtr& server)
{
    if (! server->IsDown()) {
        server->ForceDown();
    }
    const bool validFlag = mChunkToServerMap.Validate(server);
    Servers::iterator const i = find(
        mChunkServers.begin(), mChunkServers.end(), server);
    if (validFlag != (i != mChunkServers.end())) {
        panic("stale server");
        return;
    }
    if (! validFlag) {
        return;
    }
    RackInfos::iterator const rackIter = find_if(
        mRacks.begin(), mRacks.end(),
        bind(&RackInfo::id, _1) == server->GetRack());
    if (rackIter != mRacks.end()) {
        rackIter->removeServer(server);
        if (rackIter->getServers().empty()) {
            // the entire rack of servers is gone
            // so, take the rack out
            KFS_LOG_STREAM_INFO << "All servers in rack " <<
                server->GetRack() << " are down; taking out the rack" <<
            KFS_LOG_EOM;
            mRacks.erase(rackIter);
        }
    }

    // Schedule to expire write leases, and invalidate record append cache.
    mChunkLeases.ServerDown(server, mARAChunkCache, mChunkToServerMap);

    const bool           canBeMaster = server->CanBeChunkMaster();
    const time_t         now         = TimeNow();
    const ServerLocation loc         = server->GetServerLocation();
    const size_t         blockCount  = server->GetChunkCount();
    string               reason      = server->DownReason();

    KFS_LOG_STREAM_INFO <<
        "server down: "  << loc <<
        " block count: " << blockCount <<
        " master: "      << canBeMaster <<
        (reason.empty() ? "" : " reason: " ) << reason <<
    KFS_LOG_EOM;

    // check if this server was sent to hibernation
    HibernatingServerInfo_t* const hs = FindHibernatingServer(loc);
    bool isHibernating = hs != 0;
    if (isHibernating) {
        HibernatingServerInfo_t& hsi = *hs;
        const bool   wasHibernatedFlag = hsi.IsHibernated();
        const size_t prevIdx           = hsi.csmapIdx;
        if (mChunkToServerMap.SetHibernated(server, hsi.csmapIdx)) {
            if (! wasHibernatedFlag) {
                reason = "Hibernated";
            } else {
                if (! mChunkToServerMap.RemoveHibernatedServer(
                        prevIdx)) {
                    panic("failed to update hibernated"
                        " server index");
                }
                KFS_LOG_STREAM_ERROR <<
                    "hibernated server reconnect "
                    " failure " <<
                    " location: " << loc <<
                    " index: "    << prevIdx <<
                    " -> "        << hsi.csmapIdx <<
                    " blocks: "   << blockCount <<
                KFS_LOG_EOM;
                reason = "Reconnect failed";
            }
        } else {
            reason = "Hibernated";
        }
    }

    if (! isHibernating && server->IsRetiring()) {
        reason = "Retired";
    } else if (reason.empty()) {
        reason = "Unreachable";
    }
    // for reporting purposes, record when it went down
    ostringstream os;
    os <<
        "s="        << loc.hostname <<
        ", p="      << loc.port <<
        ", down="   <<
            DisplayDateTime(int64_t(now) * kSecs2MicroSecs) <<
        ", reason=" << reason <<
    "\t";
    mDownServers.push_back(os.str());
    if (mDownServers.size() > mMaxDownServersHistorySize) {
        mDownServers.erase(mDownServers.begin(), mDownServers.begin() +
            mDownServers.size() - mMaxDownServersHistorySize);
    }

    if (! isHibernating && server->GetChunkCount() > 0) {
        const int kMinReplicationDelay = 15;
        const int replicationDelay     = mServerDownReplicationDelay -
            server->TimeSinceLastHeartbeat();
        if (replicationDelay > kMinReplicationDelay) {
            // Delay replication by marking server as hibernated,
            // to allow the server to reconnect back.
            mHibernatingServers.push_back(
                HibernatingServerInfo_t());
            HibernatingServerInfo_t& hsi =
                mHibernatingServers.back();
            hsi.location     = loc;
            hsi.sleepEndTime = TimeNow() + replicationDelay;
            if (! mChunkToServerMap.SetHibernated(
                    server, hsi.csmapIdx)) {
                panic("failed to initiate hibernation");
            }
            isHibernating = true;
        }
    }

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
    if (! isHibernating) {
        if (! mChunkToServerMap.RemoveServer(server)) {
            panic("remove server failure");
        }
    }
    mChunkServers.erase(i);
    if (! mAssignMasterByIpFlag &&
            mMastersCount == 0 && ! mChunkServers.empty()) {
        assert(mSlavesCount > 0 &&
            ! mChunkServers.front()->CanBeChunkMaster());
        mSlavesCount--;
        mMastersCount++;
        mChunkServers.front()->SetCanBeChunkMaster(true);
    }
    UpdateReplicationsThreshold();
    ScheduleCleanup();
}

HibernatingServerInfo_t*
LayoutManager::FindHibernatingServer(const ServerLocation& loc)
{
    HibernatedServerInfos::iterator const it = find_if(
        mHibernatingServers.begin(), mHibernatingServers.end(),
        bind(&HibernatingServerInfo_t::location, _1) == loc
    );
    return (it != mHibernatingServers.end() ? &(*it) : 0);
}

int
LayoutManager::RetireServer(const ServerLocation &loc, int downtime)
{
    if (! mAllowChunkServerRetireFlag && downtime <= 0) {
        KFS_LOG_STREAM_INFO << "chunk server retire is not enabled" <<
        KFS_LOG_EOM;
        return -EPERM;
    }
    Servers::iterator const si = find_if(
        mChunkServers.begin(), mChunkServers.end(),
        MatchingServer(loc)
    );
    if (si == mChunkServers.end() || (*si)->IsDown()) {
        // Update down time, and let hibernation status check to
        // take appropriate action.
        HibernatingServerInfo_t* const hs = FindHibernatingServer(loc);
        if (hs) {
            hs->sleepEndTime = TimeNow() + max(0, downtime);
            return 0;
        }
        return -ENOENT;
    }

    mMightHaveRetiringServersFlag = true;
    ChunkServerPtr const server(*si);
    if (server->IsRetiring()) {
        KFS_LOG_STREAM_INFO << "server: " << loc <<
            " has already retiring status" <<
            " down time: " << downtime <<
        KFS_LOG_EOM;
        if (downtime <= 0) {
            // The server is already retiring.
            return 0;
        }
        // Change from retiring to hibernating state.
    }

    server->SetRetiring();
    if (downtime > 0) {
        HibernatingServerInfo_t* const hs = FindHibernatingServer(loc);
        if (hs) {
            hs->sleepEndTime = TimeNow() + downtime;
        } else {
            mHibernatingServers.push_back(HibernatingServerInfo_t());
            HibernatingServerInfo_t& hsi = mHibernatingServers.back();
            hsi.location     = loc;
            hsi.sleepEndTime = TimeNow() + downtime;
        }
        KFS_LOG_STREAM_INFO << "hibernating server: " << loc <<
            " down time: " << downtime <<
        KFS_LOG_EOM;
        server->Retire(); // Remove when connection will go down.
        return 0;
    }

    if (server->GetChunkCount() <= 0) {
        server->Retire();
        return 0;
    }

    InitCheckAllChunks();
    return 0;
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
LayoutManager::CanBeCandidateServer(const ChunkServer& c) const
{
    return (
        c.GetAvailSpace() >= mChunkAllocMinAvailSpace &&
        c.IsResponsiveServer() &&
        ! c.IsRetiring() &&
        ! c.IsRestartScheduled() &&
        c.GetSpaceUtilization(mUseFsTotalSpaceFlag) <=
            mMaxSpaceUtilizationThreshold
    );
}

bool
LayoutManager::IsCandidateServer(const ChunkServer& c,
    double writableChunksThresholdRatio /* = 1 */)
{
    UpdateGoodCandidateLoadAvg();
    return (
        c.GetCanBeCandidateServerFlag() &&
        c.GetNumChunkWrites() < c.GetNumWritableDrives() *
            mMaxWritesPerDriveThreshold *
            writableChunksThresholdRatio &&
        (c.GetLoadAvg() <= (c.CanBeChunkMaster() ?
            mCSMaxGoodMasterCandidateLoadAvg :
            mCSMaxGoodSlaveCandidateLoadAvg))
    );
}

void
LayoutManager::UpdateSrvLoadAvg(ChunkServer& srv, int64_t delta,
    bool canBeCandidateFlag /* = true */)
{
    mUpdateCSLoadAvgFlag      = true;
    mUpdatePlacementScaleFlag = true;
    const bool wasPossibleCandidate = srv.GetCanBeCandidateServerFlag();
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
    const bool isPossibleCandidate =
        canBeCandidateFlag && CanBeCandidateServer(srv);
    if (wasPossibleCandidate == isPossibleCandidate) {
        return;
    }
    const int inc = isPossibleCandidate ? 1 : -1;
    RackInfos::iterator const rackIter = find_if(
        mRacks.begin(), mRacks.end(),
        bind(&RackInfo::id, _1) == srv.GetRack());
    if (rackIter != mRacks.end()) {
        rackIter->updatePossibleCandidatesCount(inc);
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
LayoutManager::UpdateChunkWritesPerDrive(ChunkServer& /* srv */,
    int deltaNumChunkWrites, int deltaNumWritableDrives)
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
        mMaxWritesPerDriveThreshold = max(mMinWritesPerDrive,
            (int)(mTotalChunkWrites * mTotalWritableDrivesMult));
    }
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
    LayoutManager::RackInfos::const_iterator const it = find_if(
        racks.begin(), racks.end(), bind(&RackInfo::id, _1) == rack);
    return (it == racks.end() ?
        maxWeight : min(maxWeight, it->getWeight()));
}

int
LayoutManager::AllocateChunk(
    MetaAllocate* r, const vector<MetaChunkInfo*>& chunkBlock)
{
    // r->offset is a multiple of CHUNKSIZE
    assert(r->offset >= 0 && (r->offset % CHUNKSIZE) == 0);

    r->servers.clear();
    if (r->numReplicas <= 0) {
        // huh? allocate a chunk with 0 replicas???
        KFS_LOG_STREAM_DEBUG <<
            "allocate chunk reaplicas: " << r->numReplicas <<
            " request: " << r->Show() <<
        KFS_LOG_EOM;
        r->statusMsg = "0 replicas";
        return -EINVAL;
    }
    StTmp<ChunkPlacement> placementTmp(mChunkPlacementTmp);
    ChunkPlacement&       placement = placementTmp.Get();
    if (r->stripedFileFlag) {
        // For replication greater than one do the same placement, but
        // only take into the account write masters, or the chunk server
        // hosting the first replica.
        for (StripedFilesAllocationsInFlight::const_iterator it =
                mStripedFilesAllocationsInFlight.lower_bound(
                    make_pair(make_pair(
                    r->fid, r->chunkBlockStart), 0));
                it != mStripedFilesAllocationsInFlight.end() &&
                it->first.first == r->fid;
                ++it) {
            if (it->first.second == r->chunkBlockStart) {
                const ChunkLeases::WriteLease* const lease =
                    mChunkLeases.GetWriteLease(it->second);
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
    r->servers.reserve(r->numReplicas);

    // for non-record append case, take the server local to the machine on
    // which the client is on make that the master; this avoids a network transfer.
    // For the record append case, to avoid deadlocks when writing out large
    // records, we are doing hierarchical allocation: a chunkserver that is
    // a chunk master is never made a slave.
    ChunkServerPtr localserver;
    int            replicaCnt = 0;
    Servers::iterator const li = (! (r->appendChunk ?
        mAllowLocalPlacementForAppendFlag &&
            ! mInRackPlacementForAppendFlag :
        mAllowLocalPlacementFlag) ||
        r->clientIp.empty()) ?
        mChunkServers.end() :
        find_if(mChunkServers.begin(), mChunkServers.end(),
            MatchServerByHost(r->clientIp));
    if (li != mChunkServers.end() &&
            (! r->appendChunk || (*li)->CanBeChunkMaster()) &&
            IsCandidateServer(
                **li,
                GetRackWeight(mRacks, (*li)->GetRack(),
                    mMaxLocalPlacementWeight)
            ) &&
            placement.CanBeUsed(*li)) {
        replicaCnt++;
        localserver = *li;
        placement.ExcludeServer(localserver);
    }
    RackId rackIdToUse = -1;
    if ((r->appendChunk ?
            mInRackPlacementForAppendFlag :
            mInRackPlacementFlag) &&
            ! mRacks.empty() && ! r->clientIp.empty()) {
        if (li != mChunkServers.end()) {
            rackIdToUse = (*li)->GetRack();
        }
        if (rackIdToUse < 0) {
            rackIdToUse = GetRackId(r->clientIp);
        }
        if (rackIdToUse < 0 && li == mChunkServers.end()) {
            Servers::iterator const it = find_if(
                mChunkServers.begin(),
                mChunkServers.end(),
                MatchServerByHost(r->clientIp));
            if (it != mChunkServers.end()) {
                rackIdToUse = (*it)->GetRack();
            }
        }
    }
    const bool kForReplicationFlag = false;
    placement.FindCandidates(kForReplicationFlag, rackIdToUse);
    size_t numServersPerRack(1);
    if (r->numReplicas > 1) {
        numServersPerRack = placement.GetCandidateRackCount();
        if (r->appendChunk ?
                mInRackPlacementForAppendFlag :
                mInRackPlacementFlag) {
            numServersPerRack = r->numReplicas;
        } else if (numServersPerRack <= 1 ||
                (numServersPerRack < (size_t)r->numReplicas &&
                placement.GetExcludedRacksCount() > 0)) {
            // Place first replica, then re-calculate.
            numServersPerRack = 1;
        } else {
            numServersPerRack = ((size_t)r->numReplicas +
                numServersPerRack - 1) / numServersPerRack;
        }
    }
    // For append always reserve the first slot -- write master.
    if (r->appendChunk || localserver) {
        r->servers.push_back(localserver);
    }
    int    mastersSkipped = 0;
    int    slavesSkipped  = 0;
    size_t numCandidates  = 0;
    for (; ;) {
        // take as many as we can from this rack
        const size_t psz    = r->servers.size();
        const RackId rackId = placement.GetRackId();
        for (size_t n = (localserver &&
                rackId == localserver->GetRack()) ? 1 : 0;
                (n < numServersPerRack || rackId < 0) &&
                    replicaCnt < r->numReplicas;
                ) {
            const ChunkServerPtr cs =
                placement.GetNext(r->stripedFileFlag);
            if (! cs) {
                break;
            }
            if (placement.IsUsingServerExcludes() &&
                    find(r->servers.begin(),
                        r->servers.end(), cs) !=
                    r->servers.end()) {
                continue;
            }
            numCandidates++;
            if (r->appendChunk) {
                // for record appends, to avoid deadlocks for
                // buffer allocation during atomic record
                // appends, use hierarchical chunkserver
                // selection
                if (cs->CanBeChunkMaster()) {
                    if (r->servers.front()) {
                        mastersSkipped++;
                        continue;
                    }
                    r->servers.front() = cs;
                } else {
                    if (r->servers.size() >=
                            (size_t)r->numReplicas) {
                        slavesSkipped++;
                        continue;
                    }
                    if (mAllocateDebugVerifyFlag &&
                            find(r->servers.begin(), r->servers.end(), cs) !=
                                r->servers.end()) {
                        panic("allocate: duplicate slave");
                        continue;
                    }
                    r->servers.push_back(cs);
                }
            } else {
                if (mAllocateDebugVerifyFlag &&
                        find(r->servers.begin(), r->servers.end(), cs) !=
                            r->servers.end()) {
                    panic("allocate: duplicate server");
                    continue;
                }
                r->servers.push_back(cs);
            }
            n++;
            replicaCnt++;
        }
        if (r->numReplicas <= replicaCnt || placement.IsLastRack()) {
            break;
        }
        if (r->appendChunk && mInRackPlacementForAppendFlag &&
                rackId >= 0 &&
                (r->numReplicas + 1) *
                    placement.GetCandidateRackCount() <
                mChunkServers.size()) {
            // Reset, try to find another rack where both replicas
            // can be placed.
            // This assumes that the racks are reasonably
            // "balanced".
            replicaCnt = 0;
            r->servers.clear();
            r->servers.push_back(ChunkServerPtr());
            localserver.reset();
        } else if (r->stripedFileFlag && r->numReplicas > 1 &&
                numServersPerRack == 1 &&
                psz == 0 && r->servers.size() == size_t(1)) {
            // Striped file placement: attempt to place the first
            // chunk replica on a different rack / server than other
            // chunks in the stripe.
            // Attempt to place all subsequent replicas on different
            // racks.
            placement.clear();
            placement.ExcludeServerAndRack(r->servers);
            placement.FindCandidates(kForReplicationFlag);
            numServersPerRack = placement.GetCandidateRackCount();
            numServersPerRack = numServersPerRack <= 1 ?
                (size_t)(r->numReplicas - replicaCnt) :
                ((size_t)(r->numReplicas - replicaCnt) +
                numServersPerRack - 1) / numServersPerRack;
        } else {
            placement.ExcludeServer(
                r->servers.begin() + psz, r->servers.end());
        }
        if (! placement.NextRack()) {
            break;
        }
    }
    bool noMaster = false;
    if (r->servers.empty() || (noMaster = ! r->servers.front())) {
        r->statusMsg = noMaster ? "no master" : "no servers";
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
            if (cs.IsRetiring()) {
                retiringCount[i]++;
            }
            if (cs.IsRestartScheduled()) {
                restartingCount[i]++;
            }
            KFS_LOG_STREAM_DEBUG <<
                "allocate: "          << r->statusMsg <<
                " fid: "              << r->fid <<
                " offset: "           << r->offset <<
                " chunkId: "          << r->chunkId <<
                " append: "           << r->appendChunk <<
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
                " retire: "           << cs.IsRetiring() <<
                " responsive: "       << cs.IsResponsiveServer() <<
            KFS_LOG_EOM;
        }
        const size_t numFound = r->servers.size();
        r->servers.clear();
        KFS_LOG_STREAM_INFO << "allocate: " <<
            r->statusMsg <<
            " fid: "        << r->fid <<
            " offset: "     << r->offset <<
            " chunkId: "    << r->chunkId <<
            " append: "     << r->appendChunk <<
            " repl: "       << r->numReplicas <<
                "/" << replicaCnt <<
            " servers: "    << numFound <<
                "/" << mChunkServers.size() <<
            " dont like: "  << dontLikeCount[0] <<
                "/" << dontLikeCount[1] <<
            " no space: "   << outOfSpaceCount[0] <<
                "/" << outOfSpaceCount[1] <<
            " slow: "   <<  notResponsiveCount[0] <<
                "/" << notResponsiveCount[1] <<
            " retire: "     << retiringCount[0] <<
                "/" << retiringCount[1] <<
            " restart: "    << restartingCount[0] <<
                "/" << restartingCount[1] <<
            " racks: "      << placement.GetCandidateRackCount() <<
            " candidates: " << numCandidates <<
            " masters: "    << mastersSkipped <<
                "/" << mMastersCount <<
            " slaves: "     << slavesSkipped <<
                "/" << mSlavesCount <<
            " to restart: " << mCSToRestartCount <<
                "/"    << mMastersToRestartCount <<
            " request: "    << r->Show() <<
        KFS_LOG_EOM;
        return -ENOSPC;
    }
    assert(r->servers.size() <= (size_t)r->numReplicas);
    r->master = r->servers[0];

    if (! mChunkLeases.NewWriteLease(
            r->chunkId,
            r->chunkVersion,
            GetInitialWriteLeaseExpireTime(),
            r->servers[0],
            r->pathname.GetStr(),
            r->appendChunk,
            r->stripedFileFlag,
            r,
            r->leaseId)) {
        panic("failed to get write lease for a new chunk");
    }

    if (r->stripedFileFlag) {
        if (! mStripedFilesAllocationsInFlight.insert(make_pair(make_pair(
                r->fid, r->chunkBlockStart), r->chunkId)).second) {
            panic("duplicate in striped file allocation entry");
        }
    }
    for (size_t i = r->servers.size(); i-- > 0; ) {
        r->servers[i]->AllocateChunk(r, i == 0 ? r->leaseId : -1);
    }
    // Handle possible recursion ensure that request still valid.
    if (! r->servers.empty() && r->appendChunk && r->status >= 0) {
        mARAChunkCache.RequestNew(*r);
    }
    return 0;
}

struct MetaLogChunkVersionChange : public MetaRequest, public KfsCallbackObj
{
    MetaAllocate& alloc;
    MetaLogChunkVersionChange(MetaAllocate& alloc)
        : MetaRequest(
            META_LOG_CHUNK_VERSION_CHANGE, true, alloc.opSeqno),
          KfsCallbackObj(),
          alloc(alloc)
    {
        SET_HANDLER(this, &MetaLogChunkVersionChange::logDone);
        clnt = this;
    }
    virtual void handle()
        { status = 0; }
    virtual string Show() const
    {
        return string("log-chunk-version-change: ") + alloc.Show();
    }
    virtual int log(ostream &file) const
    {
        file << "beginchunkversionchange"
            "/file/"         << alloc.fid <<
            "/chunkId/"      << alloc.chunkId <<
            "/chunkVersion/" << alloc.chunkVersion <<
        "\n";
        return file.fail() ? -EIO : 0;
    }
    int logDone(int code, void* data)
    {
        assert(code == EVENT_CMD_DONE && data == this);
        MetaAllocate& r = alloc;
        delete this;
        for (size_t i = r.servers.size(); i-- > 0; ) {
            r.servers[i]->AllocateChunk(&r, i == 0 ? r.leaseId : -1);
        }
        return 0;
    }
};

bool
LayoutManager::ReplayBeginChangeChunkVersion(
    fid_t     fid,
    chunkId_t chunkId,
    seq_t     chunkVersion)
{
    const char* err = 0;
    const CSMap::Entry* const cs = mChunkToServerMap.Find(chunkId);
    if (! cs) {
        err = "no such chunk";
    }
    const seq_t vers = err ? -1 : cs->GetChunkInfo()->chunkVersion;
    if (! err && vers >= chunkVersion) {
        err = "invalid version transition";
    }
    if (! err) {
        mChunkVersionRollBack[chunkId] = chunkVersion - vers;
    }
    KFS_LOG_STREAM(err ?
        MsgLogger::kLogLevelWARN :
        MsgLogger::kLogLevelDEBUG) <<
        "replay beginchunkversionchange"
        " fid: "     << fid <<
        " chunkId: " << chunkId <<
        " version: " << vers << "=>" << chunkVersion <<
        " "          << (err ? err : "OK") <<
    KFS_LOG_EOM;
    return (! err);
}

int
LayoutManager::WritePendingChunkVersionChange(ostream& os) const
{
    for (ChunkVersionRollBack::const_iterator
            it = mChunkVersionRollBack.begin();
            it != mChunkVersionRollBack.end() && os;
            ++it) {
        if (it->second <= 0) {
            KFS_LOG_STREAM_ERROR <<
                "version change invalid chunk roll back entry:"
                " chunk: "             << it->first <<
                " version increment: " << it->second <<
            KFS_LOG_EOM;
            continue;
        }
        const CSMap::Entry* const ci = mChunkToServerMap.Find(it->first);
        if (! ci) {
            // Stale mapping.
            KFS_LOG_STREAM_ERROR <<
                "version change failed to get chunk mapping:"
                " chunk: "             << it->first <<
                " version increment: " << it->second <<
            KFS_LOG_EOM;
            continue;
        }
        const seq_t vers = ci->GetChunkInfo()->chunkVersion;
        os << "beginchunkversionchange"
            "/file/"         << ci->GetFileId() <<
            "/chunkId/"      << it->first <<
            "/chunkVersion/" << (vers + it->second) <<
        "\n";
    }
    return (os ? 0 : -EIO);
}

int
LayoutManager::GetInFlightChunkOpsCount(chunkId_t chunkId, MetaOp opType) const
{
    const MetaOp types[] = { opType,  META_NUM_OPS_COUNT };
    return GetInFlightChunkOpsCount(chunkId, types);
}

int
LayoutManager::GetInFlightChunkModificationOpCount(
    chunkId_t               chunkId,
    LayoutManager::Servers* srvs /* = 0 */) const
{
    MetaOp const types[] = {
        META_CHUNK_REPLICATE,  // Recovery or replication.
        META_CHUNK_VERSCHANGE, // Always runs after recovery.
        META_CHUNK_MAKE_STABLE,
        META_NUM_OPS_COUNT     // Sentinel
    };
    return GetInFlightChunkOpsCount(chunkId, types, srvs);
}

int
LayoutManager::GetInFlightChunkOpsCount(
    chunkId_t               chunkId,
    const MetaOp*           opTypes,
    LayoutManager::Servers* srvs /* = 0 */) const
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
            if (it->second->op == *op) {
                ret++;
            }
            if (srvs && find(srvs->begin(), srvs->end(),
                    it->second->server) == srvs->end()) {
                srvs->push_back(it->second->server);
            }
        }
    }
    return ret;
}

int
LayoutManager::GetChunkWriteLease(MetaAllocate *r, bool &isNewLease)
{
    if (InRecovery()) {
        KFS_LOG_STREAM_INFO <<
            "GetChunkWriteLease: InRecovery() => EBUSY" <<
        KFS_LOG_EOM;
        r->statusMsg = "meta server in recovery mode";
        return -EBUSY;
    }
    if (GetInFlightChunkModificationOpCount(r->chunkId) > 0) {
        // Wait for re-replication to finish.
        KFS_LOG_STREAM_INFO << "Write lease: " << r->chunkId <<
            " is being re-replicated => EBUSY" <<
        KFS_LOG_EOM;
        r->statusMsg = "replication is in progress";
        return -EBUSY;
    }
    const CSMap::Entry* const ci = mChunkToServerMap.Find(r->chunkId);
    if (! ci) {
        r->statusMsg = "no such chunk";
        return -EINVAL;
    }
    int ret = 0;
    if (! mChunkToServerMap.HasServers(*ci)) {
        r->statusMsg = "no replicas available";
        ret = -EDATAUNAVAIL;
        if (! r->stripedFileFlag) {
            return ret;
        }
        // Renew write lease with striped files, even if no
        // replica available to ensure that the chunk block can not
        // change, and recovery can not be started.
        // Chunk invalidation and normal chunk close (in the case when
        // replica re-appears) will expire the lease.
    }

    const ChunkLeases::WriteLease* const l =
        mChunkLeases.RenewValidWriteLease(r->chunkId);
    if (l) {
        if (l->allocInFlight) {
            r->statusMsg =
                "allocation or version change is in progress";
            KFS_LOG_STREAM_INFO << "write lease denied"
                " chunk " << r->chunkId << " " << r->statusMsg <<
            KFS_LOG_EOM;
            return -EBUSY;
        }
        if (l->appendFlag) {
            r->statusMsg = "valid write append lease exists";
            KFS_LOG_STREAM_INFO << "write lease denied"
                " chunk " << r->chunkId << " " << r->statusMsg <<
            KFS_LOG_EOM;
            return -EBUSY;
        }
        // valid write lease; so, tell the client where to go
        KFS_LOG_STREAM_INFO <<
            "valid write lease:"
            " chunk: "      << r->chunkId <<
            " expires in: " << (l->expires - TimeNow()) << " sec."
            " replicas: "   << mChunkToServerMap.ServerCount(*ci) <<
            " status: "     << ret <<
        KFS_LOG_EOM;
        if (ret < 0) {
            isNewLease = false;
            r->servers.clear();
            mChunkToServerMap.GetServers(*ci, r->servers);
            r->master = l->chunkServer;
            return ret;
        }
        // Delete the lease to force version number bump.
        // Assume that the client encountered a write error.
        mChunkLeases.Delete(r->chunkId);
    }
    if (ret < 0) {
        return ret;
    }
    // there is no valid write lease; to issue a new write lease, we
    // need to do a version # bump.  do that only if we haven't yet
    // handed out valid read leases
    if (! ExpiredLeaseCleanup(r->chunkId)) {
        r->statusMsg = "valid read lease";
        KFS_LOG_STREAM_DEBUG << "write lease denied"
            " chunk " << r->chunkId << " " << r->statusMsg <<
        KFS_LOG_EOM;
        return -EBUSY;
    }
    // Check if make stable is in progress.
    // It is crucial to check the after invoking ExpiredLeaseCleanup()
    // Expired lease cleanup the above can start make chunk stable.
    if (! IsChunkStable(r->chunkId)) {
        r->statusMsg = "chunk is not stable";
        KFS_LOG_STREAM_DEBUG << "write lease denied"
            " chunk " << r->chunkId << " " << r->statusMsg <<
        KFS_LOG_EOM;
        return -EBUSY;
    }
    // Check if servers vector has changed:
    // chunk servers can go down in ExpiredLeaseCleanup()
    r->servers.clear();
    mChunkToServerMap.GetServers(*ci, r->servers);
    if (r->servers.empty()) {
        // all the associated servers are dead...so, fail
        // the allocation request.
        r->statusMsg = "no replicas available";
        return -EDATAUNAVAIL;
    }
    // Need space on the servers..otherwise, fail it
    Servers::size_type i;
    for (i = 0; i < r->servers.size(); i++) {
        if (r->servers[i]->GetAvailSpace() < mChunkAllocMinAvailSpace) {
            return -ENOSPC;
        }
    }
    isNewLease = true;
    assert(r->chunkVersion == r->initialChunkVersion);
    // When issuing a new lease, increment the version, skipping over
    // the failed version increment attemtps.
    r->chunkVersion += IncrementChunkVersionRollBack(r->chunkId);
    if (! mChunkLeases.NewWriteLease(
            r->chunkId,
            r->chunkVersion,
            GetInitialWriteLeaseExpireTime(),
            r->servers[0],
            r->pathname.GetStr(),
            r->appendChunk,
            r->stripedFileFlag,
            r,
            r->leaseId)) {
        panic("failed to get write lease for a new chunk");
    }

    r->master = r->servers[0];
    KFS_LOG_STREAM_INFO <<
        "new write"
        " lease:"    << r->leaseId <<
        " chunk: "   << r->chunkId <<
        " version: " << r->chunkVersion <<
    KFS_LOG_EOM;
    submit_request(new MetaLogChunkVersionChange(*r));
    return 0;
}

bool
LayoutManager::IsAllocationAllowed(MetaAllocate* req)
{
    if (req->clientProtoVers < mMinChunkAllocClientProtoVersion) {
        req->status    = -EPERM;
        req->statusMsg = "client upgrade required";
        return false;
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
LayoutManager::AllocateChunkForAppend(MetaAllocate* req)
{
    ARAChunkCache::iterator const it    = mARAChunkCache.Find(req->fid);
    ARAChunkCache::Entry* const   entry = mARAChunkCache.Get(it);
    if (! entry) {
        return -1;
    }

    KFS_LOG_STREAM_DEBUG << "Append on file " << req->fid <<
        " with offset " << req->offset <<
        " max offset  " << entry->offset <<
        (entry->IsAllocationPending() ?
            " allocation in progress" : "") <<
        " appenders: "  << entry->numAppendersInChunk <<
    KFS_LOG_EOM;

    if (entry->offset < 0 || (entry->offset % CHUNKSIZE) != 0 ||
            ! entry->master) {
        panic("invalid write append cache entry");
        mARAChunkCache.Invalidate(req->fid);
        return -1;
    }
    if (mVerifyAllOpsPermissionsFlag &&
            ! entry->permissions->CanWrite(req->euser, req->egroup)) {
        req->status = -EPERM;
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
    if (entry->offset < req->offset && ! entry->IsAllocationPending()) {
        mARAChunkCache.Invalidate(it);
        return -1;
    }
    // Ensure that master is still good.
    if (entry->numAppendersInChunk > mMinAppendersPerChunk) {
        UpdateGoodCandidateLoadAvg();
        if (entry->master->GetLoadAvg() >
                mCSMaxGoodMasterCandidateLoadAvg) {
            KFS_LOG_STREAM_INFO <<
                "invalidating append cache entry: " <<
                req->fid <<
                " " << entry->master->GetServerLocation() <<
                " load: " << entry->master->GetLoadAvg() <<
                " exceeds: " <<
                    mCSMaxGoodMasterCandidateLoadAvg <<
            KFS_LOG_EOM;
            mARAChunkCache.Invalidate(it);
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
            mARAChunkCache.Invalidate(it);
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
        max(1, req->spaceReservationSize)));
    if (entry->spaceReservationSize + reservationSize >
            mChunkReservationThreshold) {
        return -1;
    }
    const ChunkLeases::WriteLease* const wl =
        mChunkLeases.RenewValidWriteLease(entry->chunkId);
    if (! wl) {
        mARAChunkCache.Invalidate(it);
        return -1;
    }
    // valid write lease; so, tell the client where to go
    req->chunkId      = entry->chunkId;
    req->offset       = entry->offset;
    req->chunkVersion = entry->chunkVersion;
    entry->numAppendersInChunk++;
    entry->lastAccessedTime = now;
    entry->spaceReservationSize += reservationSize;
    const bool pending = entry->AddPending(*req);
    if (! pending && req->responseStr.empty()) {
        // The cached response will have or already has all the info.
        // Presently it should never get here.
        KFS_LOG_STREAM_WARN <<
            "invalid write append cache entry:"
            " no cached response"  <<
            " file: "   << req->fid <<
            " chunk: "  << entry->chunkId <<
            " offset: " << entry->offset <<
        KFS_LOG_EOM;
        mARAChunkCache.Invalidate(it);
        return -1;
    }
    KFS_LOG_STREAM_DEBUG <<
        "Valid write lease exists for " << req->chunkId <<
        " expires in " << (wl->expires - TimeNow()) << " sec" <<
        " space: " << entry->spaceReservationSize <<
        " (+" << reservationSize <<
        "," << req->spaceReservationSize << ")" <<
        " num appenders: " << entry->numAppendersInChunk <<
        (pending ? " allocation in progress" : "") <<
    KFS_LOG_EOM;
    if (entry->numAppendersInChunk >= mMaxAppendersPerChunk) {
        mARAChunkCache.Invalidate(it);
    }
    return 0;
}

/*
 * The chunk files are named <fid, chunkid, version>. The fid is now ignored by
 * the meta server.
*/
void
LayoutManager::ChangeChunkFid(MetaFattr* srcFattr, MetaFattr* dstFattr,
    MetaChunkInfo* chunk)
{
    if (mChunkEntryToChange || mFattrToChangeTo) {
        panic("coalesce blocks:  invalid invocation:"
            " previous change pending");
        return;
    }
    if (! chunk) {
        if (! srcFattr) {
            if (dstFattr) {
                panic("coalesce blocks: invalid invocation:"
                    " src fattr is not null");
            }
            return;
        }
        // Invalidate fid cache.
        mARAChunkCache.Invalidate(srcFattr->id());
        return;
    }
    if (! dstFattr) {
        panic("coalesce blocks:  invalid invocation:"
            " null destination fattr");
        return;
    }

    CSMap::Entry& entry = GetCsEntry(*chunk);
    if (entry.GetFattr() != srcFattr) {
        ostringstream os;
        os <<
            "coalesce blocks: chunk: " << chunk->chunkId <<
            " undexpected file attr: " << (void*)entry.GetFattr() <<
            " id: "                    << entry.GetFileId() <<
            " expect: "                << (void*)srcFattr <<
            " id: "                    << srcFattr->id()
        ;
        const string msg = os.str();
        panic(msg.c_str());
        return;
    }
    mChunkEntryToChange = &entry;
    mFattrToChangeTo    = dstFattr;
}

int
LayoutManager::GetChunkReadLeases(MetaLeaseAcquire& req)
{
    req.responseBuf.Clear();
    if (req.chunkIds.empty()) {
        return 0;
    }
    StTmp<Servers>    serversTmp(mServers3Tmp);
    Servers&          servers = serversTmp.Get();
    const bool        recoveryFlag = InRecovery();
    const char*       p            = req.chunkIds.GetPtr();
    const char*       e            = p + req.chunkIds.GetSize();
    int               ret          = 0;
    IntIOBufferWriter writer(req.responseBuf);
    while (p < e) {
        chunkId_t chunkId;
        if (! ValueParser::ParseInt(p, e - p, chunkId)) {
            while (p < e && *p <= ' ') {
                p++;
            }
            if (p != e) {
                req.status    = -EINVAL;
                req.statusMsg = "chunk id list parse error";
                ret = req.status;
            }
            break;
        }
        ChunkLeases::LeaseId leaseId = 0;
        const CSMap::Entry*  cs      = 0;
        servers.clear();
        if ((recoveryFlag && ! req.fromChunkServerFlag) ||
                ! IsChunkStable(chunkId)) {
            leaseId = -EBUSY;
        } else if ((req.leaseTimeout <= 0 ?
                mChunkLeases.HasWriteLease(chunkId) :
                ! ((cs = mChunkToServerMap.Find(chunkId)) &&
                mChunkToServerMap.GetServers(*cs, servers) > 0 &&
                mChunkLeases.NewReadLease(
                    chunkId,
                    TimeNow() + min(req.leaseTimeout, LEASE_INTERVAL_SECS),
                    leaseId)))) {
            leaseId = -EBUSY;
            if (req.flushFlag) {
                mChunkLeases.FlushWriteLease(
                    chunkId, mARAChunkCache, mChunkToServerMap);
            }
        } else if (! ((cs = mChunkToServerMap.Find(chunkId)) &&
                mChunkToServerMap.GetServers(*cs, servers) > 0)) {
            // Cannot obtain lease if no replicas exist.
            leaseId = cs ? -EAGAIN : -EINVAL;
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
                if (loc.IsValid()) {
                    writer.Write(" ", 1);
                    writer.Write(loc.hostname);
                    writer.Write(" ", 1);
                    writer.WriteHexInt(loc.port);
                } else {
                    writer.Write(" ? -1", 5);
                }
            }
        } else {
            writer.WriteInt(leaseId);
        }
    }
    if (ret == 0) {
        writer.Close();
    } else {
        writer.Clear();
    }
    return ret;
}

/*
 * \brief Process a reqeuest for a READ lease.
*/
int
LayoutManager::GetChunkReadLease(MetaLeaseAcquire* req)
{
    const int ret = GetChunkReadLeases(*req);
    if (ret != 0 || req->chunkId < 0) {
        return ret;
    }
    if (InRecovery() && ! req->fromChunkServerFlag) {
        req->statusMsg = "recovery is in progress";
        KFS_LOG_STREAM_INFO << "chunk " << req->chunkId <<
            " " << req->statusMsg << " => EBUSY" <<
        KFS_LOG_EOM;
        return -EBUSY;
    }
    const CSMap::Entry* const cs = mChunkToServerMap.Find(req->chunkId);
    if (! cs || ! mChunkToServerMap.HasServers(*cs)) {
        req->statusMsg = cs ? "no replica available" : "no such chunk";
        return (cs ? -EAGAIN : -EINVAL);
    }
    if (! req->fromChunkServerFlag && mVerifyAllOpsPermissionsFlag &&
            ! cs->GetFattr()->CanRead(req->euser, req->egroup)) {
        return -EACCES;
    }
    //
    // Even if there is no write lease, wait until the chunk is stable
    // before the client can read the data.  We could optimize by letting
    // the client read from servers where the data is stable, but that
    // requires more book-keeping; so, we'll defer for now.
    //
    if (! IsChunkStable(req->chunkId)) {
        req->statusMsg = "is not yet stable";
        KFS_LOG_STREAM_INFO << "Chunk " << req->chunkId <<
            " " << req->statusMsg << " => EBUSY" <<
        KFS_LOG_EOM;
        return -EBUSY;
    }
    if ((req->leaseTimeout <= 0 ?
            ! mChunkLeases.HasWriteLease(req->chunkId) :
            mChunkLeases.NewReadLease(
                req->chunkId,
                TimeNow() + min(req->leaseTimeout,
                    LEASE_INTERVAL_SECS),
                req->leaseId))) {
        return 0;
    }
    req->statusMsg = "has write lease";
    if (req->flushFlag) {
        const char* errMsg = mChunkLeases.FlushWriteLease(
            req->chunkId, mARAChunkCache, mChunkToServerMap);
        req->statusMsg += "; ";
        req->statusMsg += errMsg ? errMsg :
            "initiated write lease relinquish";
    }
    KFS_LOG_STREAM_INFO << "Chunk " << req->chunkId <<
        " " << req->statusMsg << " => EBUSY" <<
    KFS_LOG_EOM;
    return -EBUSY;
}

class ValidLeaseIssued
{
    const ChunkLeases& leases;
public:
    ValidLeaseIssued(const ChunkLeases& cl)
        : leases(cl) {}
    bool operator() (MetaChunkInfo *c) const {
        return leases.HasValidLease(c->chunkId);
    }
};

bool
LayoutManager::IsValidLeaseIssued(const vector<MetaChunkInfo*>& c)
{
    vector<MetaChunkInfo*>::const_iterator const i = find_if(
        c.begin(), c.end(),
        ValidLeaseIssued(mChunkLeases)
    );
    if (i == c.end()) {
        return false;
    }
    KFS_LOG_STREAM_DEBUG << "Valid lease issued on chunk: " <<
            (*i)->chunkId << KFS_LOG_EOM;
    return true;
}

int
LayoutManager::LeaseRenew(MetaLeaseRenew *req)
{
    if (! mChunkToServerMap.Find(req->chunkId)) {
        if (InRecovery()) {
            mChunkLeases.SetMaxLeaseId(req->leaseId + 1);
        }
        return -EINVAL;
    }
    return mChunkLeases.Renew(req->chunkId, req->leaseId);
}

///
/// Handling a corrupted chunk involves removing the mapping
/// from chunk id->chunkserver that we know has it.
///
void
LayoutManager::ChunkCorrupt(MetaChunkCorrupt *r)
{
    if (! r->isChunkLost) {
        r->server->IncCorruptChunks();
    }
    KFS_LOG_STREAM_INFO <<
        "server " << r->server->GetServerLocation() <<
        " claims chunk: <" <<
        r->fid << "," << r->chunkId <<
        "> to be " << (r->isChunkLost ? "lost" : "corrupt") <<
    KFS_LOG_EOM;
    ChunkCorrupt(r->chunkId, r->server, false);
}

void
LayoutManager::ChunkCorrupt(chunkId_t chunkId, const ChunkServerPtr& server,
        bool notifyStale)
{
    CSMap::Entry* const ci = mChunkToServerMap.Find(chunkId);
    if (! ci) {
        return;
    }
    const bool removedFlag = ci->Remove(mChunkToServerMap, server);
    mChunkLeases.ReplicaLost(chunkId, server.get());
    // Invalidate cache.
    mARAChunkCache.Invalidate(ci->GetFileId(), chunkId);
    if (removedFlag) {
        // check the replication state when the replicaiton checker gets to it
        CheckReplication(*ci);
    }
    KFS_LOG_STREAM_INFO << "server " << server->GetServerLocation() <<
        " declaring: <" <<
        ci->GetFileId() << "," << chunkId <<
        "> lost" <<
        " servers: " << mChunkToServerMap.ServerCount(*ci) <<
        (removedFlag ? " -1" : " -0") <<
    KFS_LOG_EOM;
    if (! notifyStale || server->IsDown()) {
        return;
    }
    server->NotifyStaleChunk(chunkId);
}

void
LayoutManager::ChunkEvacuate(MetaChunkEvacuate* r)
{
    if (r->server->IsDown() || r->server->IsRetiring()) {
        return;
    }
    r->server->UpdateSpace(*r);
    ChunkIdQueue        deletedChunks;
    ChunkIdQueue        evacuatedChunks;
    const MetaAllocate* alloc = 0;
    const char*         p     = r->chunkIds.GetPtr();
    const char*         e     = p + r->chunkIds.GetSize();
    while (p < e) {
        chunkId_t chunkId;
        if (! ValueParser::ParseInt(p, e - p, chunkId)) {
            while (p < e && *p <= ' ') {
                p++;
            }
            if (p != e) {
                r->status    = -EINVAL;
                r->statusMsg = "chunk id list parse error";
                KFS_LOG_STREAM_ERROR <<  r->Show() << " : " <<
                    r->statusMsg <<
                KFS_LOG_EOM;
            }
            break;
        }
        CSMap::Entry* const ci = mChunkToServerMap.Find(chunkId);
        if (! ci) {
            const ChunkLeases::WriteLease* const lease =
                mChunkLeases.GetWriteLease(chunkId);
            if (! lease || ! (alloc = lease->allocInFlight) ||
                    find(alloc->servers.begin(),
                        alloc->servers.end(),
                        r->server) ==
                        alloc->servers.end()) {
                deletedChunks.PushBack(chunkId);
                alloc = 0;
                continue;
            }
        } else if (! ci->HasServer(mChunkToServerMap, r->server)) {
            evacuatedChunks.PushBack(chunkId);
            continue;
        }
        const int status = r->server->Evacuate(chunkId);
        if (status == -EEXIST) {
            continue; // Already scheduled.
        }
        if (status != 0) {
            r->status = status;
            if (status == -EAGAIN) {
                r->statusMsg = "exceeded evacuate queue limit";
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
        r->server->NotifyStaleChunks(deletedChunks);
    }
    if (! evacuatedChunks.IsEmpty() && ! r->server->IsDown()) {
        const bool kEvacuatedFlag = true;
        r->server->NotifyStaleChunks(evacuatedChunks, kEvacuatedFlag);
    }
}

// Chunk replicas became available again: the disk / directory came back.
// Use these replicas only if absolutely must -- no other replicas exists,
// and / or the chunk replica cannot be recovered, or there is only one replica
// left, etc.
// If disk / chunk directory goes off line it is better not not use it as it is
// unreliable, except for the case where there is some kind of "DoS attack"
// affecting multiple nodes at the same time.
void
LayoutManager::ChunkAvailable(MetaChunkAvailable* r)
{
    if (r->server->IsDown()) {
        return;
    }
    vector<MetaChunkInfo*> cblk;
    ChunkIdQueue           staleChunks;
    const char*            p = r->chunkIdAndVers.GetPtr();
    const char*            e = p + r->chunkIdAndVers.GetSize();
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
            if (! gotVersionFlag || p != e) {
                r->status    = -EINVAL;
                r->statusMsg = "chunk id list parse error";
                KFS_LOG_STREAM_ERROR << r->Show() << " : " <<
                    r->statusMsg <<
                KFS_LOG_EOM;
            }
            break;
        }
        CSMap::Entry* const cmi = mChunkToServerMap.Find(chunkId);
        if (! cmi) {
            KFS_LOG_STREAM_DEBUG <<
                "available chunk: " << chunkId <<
                " version: "        << chunkVersion <<
                " does not exist" <<
            KFS_LOG_EOM;
            staleChunks.PushBack(chunkId);
            continue;
        }
        const MetaChunkInfo& ci = *(cmi->GetChunkInfo());
        if (cmi->HasServer(mChunkToServerMap, r->server)) {
            KFS_LOG_STREAM_ERROR <<
                "available chunk: " << chunkId <<
                " version: "        << chunkVersion <<
                " hosted version: " << ci.chunkVersion <<
                " replica is already hosted" <<
            KFS_LOG_EOM;
            // For now just let it be.
            continue;
        }
        if (ci.chunkVersion != chunkVersion) {
            KFS_LOG_STREAM_DEBUG <<
                "available chunk: "     << chunkId <<
                " version: "            << chunkVersion <<
                " mismatch, expected: " << ci.chunkVersion <<
            KFS_LOG_EOM;
            staleChunks.PushBack(chunkId);
            continue;
        }
        const ChunkLeases::WriteLease* const lease =
            mChunkLeases.GetWriteLease(chunkId);
        if (lease) {
            KFS_LOG_STREAM_DEBUG <<
                "available chunk: " << chunkId <<
                " version: "        << chunkVersion <<
                " write lease exists" <<
            KFS_LOG_EOM;
            staleChunks.PushBack(chunkId);
            continue;
        }
        if (! IsChunkStable(chunkId)) {
            KFS_LOG_STREAM_DEBUG <<
                "available chunk: " << chunkId <<
                " version: "        << chunkVersion <<
                " not stable" <<
            KFS_LOG_EOM;
            staleChunks.PushBack(chunkId);
            continue;
        }
        const MetaFattr& fa     = *(cmi->GetFattr());
        const int        srvCnt = (int)mChunkToServerMap.ServerCount(*cmi);
        if (srvCnt > 1 || fa.numReplicas <= srvCnt) {
            KFS_LOG_STREAM_DEBUG <<
                "available chunk: "      << chunkId <<
                " version: "             << chunkVersion <<
                " sufficinet replicas: " << srvCnt <<
            KFS_LOG_EOM;
            staleChunks.PushBack(chunkId);
            continue;
        }
        bool incompleteChunkBlockFlag              = false;
        bool incompleteChunkBlockWriteHasLeaseFlag = false;
        int  goodCnt                               = 0;
        if (srvCnt <= 0 && fa.numRecoveryStripes > 0 &&
                CanBeRecovered(
                    *cmi, 
                    incompleteChunkBlockFlag,
                    &incompleteChunkBlockWriteHasLeaseFlag,
                    cblk,
                    &goodCnt) &&
                goodCnt > fa.numStripes) {
            KFS_LOG_STREAM_DEBUG <<
                "available chunk: "   << chunkId <<
                " version: "          << chunkVersion <<
                " can be recovered: "
                " good: "             << goodCnt <<
                " data stripes: "     << fa.numStripes <<
            KFS_LOG_EOM;
            staleChunks.PushBack(chunkId);
            continue;
        }
        if (incompleteChunkBlockWriteHasLeaseFlag) {
            KFS_LOG_STREAM_DEBUG <<
                "available chunk: "   << chunkId <<
                " version: "          << chunkVersion <<
                " partial chunk block has write lease" <<
            KFS_LOG_EOM;
            staleChunks.PushBack(chunkId);
            continue;
        }
        if (! AddHosted(*cmi, r->server)) {
            panic("available chunk: failed to add replica");
        }
        if (srvCnt + 1 != fa.numReplicas) {
            CheckReplication(*cmi);
        }
    }
    if (! staleChunks.IsEmpty()) {
        r->server->NotifyStaleChunks(staleChunks);
    }
}

void
CSMap::Entry::destroy()
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
        const bool checkReplicationFlag = ! fa || ! entry.GetFattr() ||
            fa->numReplicas != entry.GetFattr()->numReplicas;
        mChunkEntryToChange = 0;
        mFattrToChangeTo    = 0;
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
    mChunkToServerMap.Erase(chunkId);
    DeleteChunk(fid, chunkId, servers);
}

void
LayoutManager::DeleteChunk(fid_t fid, chunkId_t chunkId,
    const LayoutManager::Servers& servers)
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
    mPendingBeginMakeStable.erase(chunkId);
    mPendingMakeStable.erase(chunkId);
    mChunkLeases.Delete(chunkId);
    mChunkVersionRollBack.erase(chunkId);

    // submit an RPC request
    for_each(cs.begin(), cs.end(),
        bind(&ChunkServer::DeleteChunk, _1, chunkId));
}

void
LayoutManager::DeleteChunk(MetaAllocate *req)
{
    if (mChunkToServerMap.Find(req->chunkId)) {
        panic("allocation attempts to delete existing chunk mapping");
        return;
    }
    DeleteChunk(req->fid, req->chunkId, req->servers);
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
    mci->chunkVersion += IncrementChunkVersionRollBack(chunkId);
    chunkVersion = mci->chunkVersion;
    StTmp<Servers> serversTmp(mServers3Tmp);
    Servers&       c = serversTmp.Get();
    mChunkToServerMap.GetServers(*ci, c);
    ci->RemoveAllServers(mChunkToServerMap);
    mARAChunkCache.Invalidate(ci->GetFileId(), chunkId);
    mPendingBeginMakeStable.erase(chunkId);
    mPendingMakeStable.erase(chunkId);
    mChunkLeases.Delete(chunkId);
    mChunkVersionRollBack.erase(chunkId);
    const bool kEvacuateChunkFlag = false;
    for_each(c.begin(), c.end(), bind(&ChunkServer::NotifyStaleChunk,
        _1, chunkId, kEvacuateChunkFlag));
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
    mChunkVersionRollBack.erase(chunkId);
    return ret->GetChunkInfo();
}

int
LayoutManager::UpdateChunkToServerMapping(chunkId_t chunkId, const ChunkServerPtr& s)
{
    // If the chunkid isn't present in the mapping table, it could be a
    // stale chunk
    CSMap::Entry* const ci = mChunkToServerMap.Find(chunkId);
    if (! ci) {
        return -1;
    }
    AddHosted(chunkId, *ci, s);
    return 0;
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
LayoutManager::GetChunkToServerMapping(MetaChunkInfo& chunkInfo,
    LayoutManager::Servers& c, MetaFattr*& fa, bool* orderReplicasFlag /* = 0 */)
{
    const CSMap::Entry& entry = GetCsEntry(chunkInfo);
    fa = entry.GetFattr();
    c.clear();
    const size_t cnt = mChunkToServerMap.GetServers(entry, c);
    if (cnt <= 0) {
        return -1;
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
    for (size_t i = c.size(); i >= 2; ) {
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
    uint64_t           freeFsSpace;
    uint64_t           goodMasters;
    uint64_t           goodSlaves;
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

void
LayoutManager::Ping(IOBuffer& buf, bool wormModeFlag)
{
    if (! mPingResponse.IsEmpty() &&
            TimeNow() < mPingUpdateTime + mPingUpdateInterval) {
        buf.Copy(&mPingResponse, mPingResponse.BytesConsumable());
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
        bind(&ChunkServer::GetRetiringStatus, _1, ref(mWOstream)));
    mWOstream <<
        "\r\n"
        "Evacuating Servers: ";
    for_each(pinger.evacuating.begin(), pinger.evacuating.end(),
        bind(&ChunkServer::GetEvacuateStatus, _1, ref(mWOstream)));
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
    const bool kRusageSelfFlag = true;
    mWOstream <<
        "\r\n"
        "Rusage self: ";
    showrusage(mWOstream, "= ", "\t", kRusageSelfFlag);
    mWOstream <<
        "\r\n"
        "Rusage children: ";
    showrusage(mWOstream, "= ", "\t", ! kRusageSelfFlag);
    mWOstream << "\r\n\r\n"; // End of headers.
    mWOstream.flush();
    // Initial headers.
    mWOstream.Set(mPingResponse);
    mPingUpdateTime = TimeNow();
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
        "Find repl timemoust= " << mReplicationFindWorkTimeouts << "\t"
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
        "Append cache size= " << mARAChunkCache.GetSize()
    ;
    mWOstream.flush();
    mWOstream.Reset();
    mPingResponse.Move(&tmpbuf);
    buf.Copy(&mPingResponse, mPingResponse.BytesConsumable());
}

class UpServersList
{
    ostream& os;
public:
    UpServersList(ostream& s) : os(s) {}
    void operator () (const ChunkServerPtr& c) {
        os << c->GetServerLocation() << "\n";
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
LayoutManager::ExpiredLeaseCleanup(chunkId_t chunkId)
{
    const int ownerDownExpireDelay = 0;
    return mChunkLeases.ExpiredCleanup(
        chunkId, TimeNow(), ownerDownExpireDelay,
        mARAChunkCache, mChunkToServerMap
    );
}

void
LayoutManager::LeaseCleanup()
{
    const time_t now = TimeNow();

    mChunkLeases.Timer(now, mLeaseOwnerDownExpireDelay,
        mARAChunkCache, mChunkToServerMap);
    if (mAppendCacheCleanupInterval >= 0) {
        // Timing out the cache entries should now be redundant,
        // and is disabled by default, as the cache should not have
        // any stale entries. The lease cleanup, allocation
        // completion in the case of failure, and chunk deletion
        // should cleanup the cache.
        mARAChunkCache.Timeout(now - mAppendCacheCleanupInterval);
    }
    if (metatree.getUpdatePathSpaceUsageFlag() &&
            mLastRecomputeDirsizeTime +
            mRecomputeDirSizesIntervalSec < now) {
        KFS_LOG_STREAM_INFO << "Doing a recompute dir size..." <<
        KFS_LOG_EOM;
        metatree.recomputeDirSize();
        mLastRecomputeDirsizeTime = now;
        KFS_LOG_STREAM_INFO << "Recompute dir size is done..." <<
        KFS_LOG_EOM;
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
    Servers servers(mChunkServers);
    make_heap(servers.begin(), servers.end(),
        bind(&ChunkServer::Uptime, _1) <
        bind(&ChunkServer::Uptime, _2));
    const int64_t maxCSUptime  = GetMaxCSUptime();
    const size_t  minMastersUp = max(size_t(1), mSlavesCount / 3 * 2);
    while (! servers.empty()) {
        ChunkServer& srv = *servers.front().get();
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
                for (Servers::iterator
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
LayoutManager::Validate(MetaAllocate* r)
{
    const ChunkLeases::WriteLease* const lease =
        mChunkLeases.GetWriteLease(r->chunkId);
    if (lease && lease->allocInFlight &&
            lease->leaseId == r->leaseId &&
            lease->chunkVersion == r->chunkVersion) {
        return true;
    }
    if (r->status >= 0) {
        r->status = -EALLOCFAILED;
    }
    return false;
}

void
LayoutManager::CommitOrRollBackChunkVersion(MetaAllocate* r)
{
    if (r->stripedFileFlag && r->initialChunkVersion < 0) {
        if (mStripedFilesAllocationsInFlight.erase(make_pair(make_pair(
                r->fid, r->chunkBlockStart), r->chunkId)) != 1 &&
                r->status >= 0) {
            panic("no striped file allocation entry");
        }
    }
    if (r->status >= 0) {
        // Tree::assignChunkId() succeeded.
        // File and chunk ids are valid and in sync with meta tree.
        const int ret = mChunkLeases.Renew(r->chunkId, r->leaseId, true);
        if (ret < 0) {
            panic("failed to renew allocation write lease");
            r->status = ret;
            return;
        }
        // AddChunkToServerMapping() should delete version roll back for
        // new chunks.
        if (mChunkVersionRollBack.erase(r->chunkId) > 0 &&
                r->initialChunkVersion < 0) {
            panic("chunk version roll back still exists");
            r->statusMsg = "internal error:"
                " chunk version roll back still exists";
            r->status = -EINVAL;
            return;
        }
        CSMap::Entry* const ci = mChunkToServerMap.Find(r->chunkId);
        if (! ci) {
            panic("missing chunk mapping");
            r->statusMsg = "internal error:"
                " missing chunk mapping";
            r->status = -EINVAL;
            return;
        }
        if (r->initialChunkVersion < 0) {
            // New valid chunk -- set servers.
            // r->offset is a multiple of CHUNKSIZE
            assert(r->offset >= 0 && (r->offset % CHUNKSIZE) == 0);
            if (r->fid != ci->GetFileId() ||
                    mChunkToServerMap.HasServers(*ci)) {
                panic("invalid chunk mapping");
                r->statusMsg = "internal error:"
                    " invalid chunk mapping";
                r->status = -EINVAL;
                return;
            }
            for (Servers::const_iterator
                    it = r->servers.begin();
                    it != r->servers.end();
                    ++it) {
                AddHosted(*ci, *it);
            }
            // Schedule replication check if needed.
            if (r->servers.size() != (size_t)r->numReplicas) {
                CheckReplication(*ci);
            }
        }
        if (r->appendChunk) {
            // Insert pending make stable entry here, to ensure that
            // it gets into the checkpoint.
            // With checkpoints from forked copy enabled checkpoint
            // can start *before* the corresponding make stable
            // starts
            pair<PendingMakeStableMap::iterator, bool> const res =
                mPendingMakeStable.insert(make_pair(
                    r->chunkId, PendingMakeStableEntry()));
            if (res.second) {
                res.first->second.mChunkVersion =
                    r->chunkVersion;
            }
        }
        return;
    }
    // Delete write lease, it wasn't ever handed to the client, and
    // version change will make chunk stable, thus there is no need to
    // go trough the normal lease cleanup procedure.
    if (! mChunkLeases.DeleteWriteLease(r->chunkId, r->leaseId)) {
        if (! mChunkToServerMap.Find(r->chunkId)) {
            // Chunk does not exist, deleted.
            mChunkVersionRollBack.erase(r->chunkId);
            return;
        }
        panic("chunk version roll back failed to delete write lease");
    }
    if (r->initialChunkVersion < 0) {
        return;
    }
    if (r->initialChunkVersion >= r->chunkVersion) {
        panic("invalid chunk version transition");
    }
    CSMap::Entry* const ci = mChunkToServerMap.Find(r->chunkId);
    if (! ci) {
        mChunkVersionRollBack.erase(r->chunkId);
        return;
    }
    if (r->initialChunkVersion + GetChunkVersionRollBack(r->chunkId) !=
            r->chunkVersion) {
        ostringstream os;
        os <<
        "invalid chunk version transition:" <<
        " "    << r->initialChunkVersion <<
        "+"    << GetChunkVersionRollBack(r->chunkId) <<
        " => " << r->chunkVersion;
        const string msg = os.str();
        panic(msg.c_str());
        return;
    }
    // Roll back to the initial chunk version, and make chunk stable.
    StTmp<Servers> serversTmp(mServers3Tmp);
    Servers&       srvs = serversTmp.Get();
    mChunkToServerMap.GetServers(*ci, srvs);
    const bool kMakeStableFlag = true;
    for (Servers::const_iterator
            it = srvs.begin(); it != srvs.end(); ++it) {
        (*it)->NotifyChunkVersChange(
            r->fid,
            r->chunkId,
            r->initialChunkVersion, // to
            r->chunkVersion,        // from
            kMakeStableFlag
        );
    }
}

int
LayoutManager::LeaseRelinquish(MetaLeaseRelinquish *req)
{
    return mChunkLeases.LeaseRelinquish(
        *req, mARAChunkCache, mChunkToServerMap);
}

// Periodically, check the status of all the leases
// This is an expensive call...use sparingly....
void
LayoutManager::CheckAllLeases()
{
    mChunkLeases.Timer(TimeNow(), mLeaseOwnerDownExpireDelay,
        mARAChunkCache, mChunkToServerMap);
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
            if (fa->mtime + mMTimeUpdateResolution < now) {
                fa->mtime = now;
                submit_request(new MetaSetMtime(fid, fa->mtime));
            }
        }
        if (beginMakeStableFlag) {
            // Ensure that there is at least pending begin make
            // stable.
            // Append allocations are marked as such, and log replay
            // adds begin make stable entries if necessary.
            pair<PendingMakeStableMap::iterator, bool> const res =
                mPendingMakeStable.insert(make_pair(
                    chunkId, PendingMakeStableEntry()));
            if (res.second) {
                res.first->second.mChunkVersion = chunkVersion;
            }
        }
        // If no servers, MCS with append (checksum and size) still
        // needs to be logged and the corresponding pending make stable
        // entry has to be created.
        if (beginMakeStableFlag || ! appendFlag) {
            KFS_LOG_STREAM_INFO << logPrefix <<
                " <" << fid << "," << chunkId << ">"
                " name: "     << pathname <<
                " no servers" <<
            KFS_LOG_EOM;
            // Update replication state.
            ChangeChunkReplication(chunkId);
            return;
        }
    }
    pair<NonStableChunksMap::iterator, bool> const ret =
        mNonStableChunks.insert(make_pair(chunkId,
            MakeChunkStableInfo(
                serversCnt,
                beginMakeStableFlag,
                pathname,
                chunkVersion,
                stripedFileFlag,
                leaseRelinquishFlag && serversCnt > 0
        )));
    if (! ret.second) {
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
        pair<PendingMakeStableMap::iterator, bool> const res =
            mPendingMakeStable.insert(make_pair(chunkId, pmse));
        if (! res.second) {
            KFS_LOG_STREAM((res.first->second.mSize >= 0 ||
                    res.first->second.mHasChecksum) ?
                    MsgLogger::kLogLevelWARN :
                    MsgLogger::kLogLevelDEBUG) <<
                logPrefix <<
                " <" << fid << "," << chunkId << ">"
                " updating existing pending MCS: " <<
                " chunkId: "  << chunkId <<
                " version: "  <<
                    res.first->second.mChunkVersion <<
                "=>"          << pmse.mChunkVersion <<
                " size: "     << res.first->second.mSize <<
                "=>"          << pmse.mSize <<
                " checksum: " <<
                    (res.first->second.mHasChecksum ?
                    int64_t(res.first->second.mChecksum) :
                    int64_t(-1)) <<
                "=>"          << (pmse.mHasChecksum ?
                    int64_t(pmse.mChecksum) :
                    int64_t(-1)) <<
            KFS_LOG_EOM;
            res.first->second = pmse;
        }
        ret.first->second.logMakeChunkStableFlag = true;
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
    NonStableChunksMap::iterator const it = mNonStableChunks.find(chunkId);
    if (it == mNonStableChunks.end()) {
        return false; // Not in progress
    }
    MakeChunkStableInfo& info = it->second;
    if (info.chunkVersion != chunkVersion) {
        errMsg = "version mismatch";
        return false;
    }
    StTmp<Servers> serversTmp(mServers3Tmp);
    Servers&       servers = serversTmp.Get();
    mChunkToServerMap.GetServers(placementInfo, servers);
    if (find_if(servers.begin(), servers.end(),
            MatchingServer(server->GetServerLocation())
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
LayoutManager::BeginMakeChunkStableDone(const MetaBeginMakeChunkStable* req)
{
    const char* const                  logPrefix = "BMCS: done";
    NonStableChunksMap::iterator const it        =
        mNonStableChunks.find(req->chunkId);
    if (it == mNonStableChunks.end() || ! it->second.beginMakeStableFlag) {
        KFS_LOG_STREAM_DEBUG << logPrefix <<
            " <" << req->fid << "," << req->chunkId << ">"
            " " << req->Show() <<
            " ignored: " <<
            (it == mNonStableChunks.end() ?
                "not in progress" : "MCS in progress") <<
        KFS_LOG_EOM;
        return;
    }
    MakeChunkStableInfo& info = it->second;
    KFS_LOG_STREAM_DEBUG << logPrefix <<
        " <" << req->fid << "," << req->chunkId << ">"
        " name: "     << info.pathname <<
        " servers: "  << info.numAckMsg << "/" << info.numServers <<
        " size: "     << info.chunkSize <<
        " checksum: " << info.chunkChecksum <<
        " " << req->Show() <<
    KFS_LOG_EOM;
    CSMap::Entry* ci              = 0;
    bool          noSuchChunkFlag = false;
    if (req->status != 0 || req->chunkSize < 0) {
        if (req->status == 0 && req->chunkSize < 0) {
            KFS_LOG_STREAM_ERROR << logPrefix <<
                " <" << req->fid << "," << req->chunkId  << ">"
                " invalid chunk size: " << req->chunkSize <<
                " declaring chunk replica corrupt" <<
                " " << req->Show() <<
            KFS_LOG_EOM;
        }
        ci = mChunkToServerMap.Find(req->chunkId);
        if (ci) {
            const ChunkServerPtr server = ci->GetServer(
                mChunkToServerMap, req->serverLoc);
            if (server && ! server->IsDown()) {
                ChunkCorrupt(req->chunkId, server);
            }
        } else {
            noSuchChunkFlag = true;
        }
    } else if (req->chunkSize < info.chunkSize || info.chunkSize < 0) {
        // Pick the smallest good chunk.
        info.chunkSize     = req->chunkSize;
        info.chunkChecksum = req->chunkChecksum;
    }
    if (++info.numAckMsg < info.numServers) {
        return;
    }
    if (! noSuchChunkFlag && ! ci) {
        ci = mChunkToServerMap.Find(req->chunkId);
        noSuchChunkFlag = ! ci;
    }
    if (noSuchChunkFlag) {
        KFS_LOG_STREAM_DEBUG << logPrefix <<
            " <" << req->fid << "," << req->chunkId  << ">"
            " no such chunk, cleaning up" <<
        KFS_LOG_EOM;
        mNonStableChunks.erase(it);
        mPendingMakeStable.erase(req->chunkId);
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
        req->chunkVersion
    );
    pair<PendingMakeStableMap::iterator, bool> const res =
        mPendingMakeStable.insert(make_pair(req->chunkId, pmse));
    assert(
        res.second ||
        (res.first->second.mSize < 0 &&
        res.first->second.mChunkVersion == pmse.mChunkVersion)
    );
    if (! res.second && pmse.mSize >= 0) {
        res.first->second = pmse;
    }
    if (res.first->second.mSize < 0) {
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
            KFS_LOG_STREAM_DEBUG << logPrefix <<
                " <" << req->fid << "," << req->chunkId  << ">"
                " no servers up, retry later" <<
            KFS_LOG_EOM;
        } else {
            // Shouldn't get here.
            KFS_LOG_STREAM_WARN << logPrefix <<
                " <" << req->fid << "," << req->chunkId  << ">"
                " internal error:"
                " up servers: "         << numUpServers <<
                " invalid chunk size: " <<
                    res.first->second.mSize <<
            KFS_LOG_EOM;
        }
        // Try again later.
        mNonStableChunks.erase(it);
        UpdateReplicationState(*ci);
        return;
    }
    submit_request(new MetaLogMakeChunkStable(
        req->fid, req->chunkId, req->chunkVersion,
        info.chunkSize, info.chunkSize >= 0, info.chunkChecksum,
        req->opSeqno
    ));
}

void
LayoutManager::LogMakeChunkStableDone(const MetaLogMakeChunkStable* req)
{
    const char* const                  logPrefix = "LMCS: done";
    NonStableChunksMap::iterator const it        =
        mNonStableChunks.find(req->chunkId);
    if (it == mNonStableChunks.end()) {
        KFS_LOG_STREAM_DEBUG << logPrefix <<
            " <" << req->fid << "," << req->chunkId  << ">"
            " " << req->Show() <<
            " ignored: not in progress" <<
        KFS_LOG_EOM;
        // Update replication state.
        ChangeChunkReplication(req->chunkId);
        return;
    }
    if (! it->second.logMakeChunkStableFlag) {
        KFS_LOG_STREAM_ERROR << logPrefix <<
            " <" << req->fid << "," << req->chunkId  << ">"
            " " << req->Show() <<
            " ignored: " <<
                (it->second.beginMakeStableFlag ? "B" : "") <<
                "MCS in progress" <<
        KFS_LOG_EOM;
        return;
    }
    MakeChunkStableInfo& info = it->second;
    CSMap::Entry* const  ci   = mChunkToServerMap.Find(req->chunkId);
    if (! ci || ! mChunkToServerMap.HasServers(*ci)) {
        KFS_LOG_STREAM_INFO << logPrefix <<
            " <" << req->fid << "," << req->chunkId  << ">" <<
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
            mPendingMakeStable.erase(req->chunkId);
        }
        mNonStableChunks.erase(it);
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
    info.chunkSize                    = req->chunkSize;
    info.chunkChecksum                = req->chunkChecksum;
    KFS_LOG_STREAM_INFO << logPrefix <<
        " <" << req->fid << "," << req->chunkId  << ">"
        " starting MCS"
        " version: "  << req->chunkVersion  <<
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
            ci->GetFileId(), req->chunkId, info.chunkVersion
        ));
        return;
    }
    const bool kPendingAddFlag = false;
    for_each(servers.begin(), servers.end(), bind(
        &ChunkServer::MakeChunkStable, _1,
        req->fid, req->chunkId, req->chunkVersion,
        req->chunkSize, req->hasChunkChecksum, req->chunkChecksum,
        kPendingAddFlag
    ));
}

void
LayoutManager::MakeChunkStableDone(const MetaChunkMakeStable* req)
{
    const char* const                  logPrefix       = "MCS: done";
    string                             pathname;
    CSMap::Entry*                      pinfo           = 0;
    bool                               updateSizeFlag  = false;
    bool                               updateMTimeFlag = false;
    NonStableChunksMap::iterator const it              =
        mNonStableChunks.find(req->chunkId);
    if (req->addPending) {
        // Make chunk stable started in AddNotStableChunk() is now
        // complete. Sever can be added if nothing has changed since
        // the op was started.
        // It is also crucial to ensure to the server with the
        // identical location is not already present in the list of
        // servers hosting the chunk before declaring chunk stale.
        bool                           notifyStaleFlag = true;
        const char*                    res             = 0;
        ChunkLeases::WriteLease const* li              = 0;
        PendingMakeStableMap::iterator msi;
        if (it != mNonStableChunks.end()) {
            res = "not stable again";
        } else {
            msi = mPendingMakeStable.find(req->chunkId);
        }
        if (res) {
            // Has already failed.
        } else if (req->chunkSize >= 0 || req->hasChunkChecksum) {
            if (msi == mPendingMakeStable.end()) {
                // Chunk went away, or already sufficiently
                // replicated.
                res = "no pending make stable info";
            } else if (msi->second.mChunkVersion !=
                        req->chunkVersion ||
                    msi->second.mSize != req->chunkSize ||
                    msi->second.mHasChecksum !=
                        req->hasChunkChecksum ||
                    msi->second.mChecksum !=
                        req->chunkChecksum) {
                // Stale request.
                res = "pending make stable info has changed";
            }
        } else if (msi != mPendingMakeStable.end()) {
            res = "pending make stable info now exists";
        }
        if (req->server->IsDown()) {
            res = "server down";
            notifyStaleFlag = false;
        } else if (req->status != 0) {
            res = "request failed";
            notifyStaleFlag = false;
        } else {
            CSMap::Entry* const ci = mChunkToServerMap.Find(req->chunkId);
            if (! ci) {
                res = "no such chunk";
            } else if (ci->HasServer(mChunkToServerMap,
                    req->server->GetServerLocation())) {
                res = "already added";
                notifyStaleFlag = false;
            } else if ((li = mChunkLeases.GetWriteLease(req->chunkId)) &&
                (((! li->relinquishedFlag &&
                    li->expires >= TimeNow()) ||
                    li->chunkVersion !=
                    req->chunkVersion))) {
                // No write lease existed when this was started.
                res = "new write lease exists";
            } else if (req->chunkVersion !=
                    ci->GetChunkInfo()->chunkVersion) {
                res = "chunk version has changed";
            } else {
                pinfo          = ci;
                updateSizeFlag =
                    ! mChunkToServerMap.HasServers(*pinfo);
                AddHosted(*ci, req->server);
                notifyStaleFlag = false;
            }
        }
        if (res) {
            KFS_LOG_STREAM_INFO << logPrefix <<
                " <" << req->fid << "," << req->chunkId  << ">"
                " "  << req->server->GetServerLocation() <<
                " not added: " << res <<
                (notifyStaleFlag ? " => stale" : "") <<
                "; " << req->Show() <<
            KFS_LOG_EOM;
            if (notifyStaleFlag) {
                req->server->NotifyStaleChunk(req->chunkId);
            }
            // List of servers hosting the chunk remains unchanged.
            return;
        }
    } else {
        if (it == mNonStableChunks.end() ||
                it->second.beginMakeStableFlag ||
                it->second.logMakeChunkStableFlag) {
            KFS_LOG_STREAM_ERROR << "MCS"
                " " << req->Show() <<
                " ignored: BMCS in progress" <<
            KFS_LOG_EOM;
            return;
        }
        MakeChunkStableInfo& info = it->second;
        KFS_LOG_STREAM_DEBUG << logPrefix <<
            " <" << req->fid << "," << req->chunkId  << ">"
            " name: "     << info.pathname <<
            " servers: "  << info.numAckMsg <<
                "/" << info.numServers <<
            " size: "     << req->chunkSize <<
                "/" << info.chunkSize <<
            " checksum: " << req->chunkChecksum <<
                "/" << info.chunkChecksum <<
            " " << req->Show() <<
        KFS_LOG_EOM;
        if (req->status != 0 && ! req->server->IsDown()) {
            ChunkCorrupt(req->chunkId, req->server);
        }
        if (++info.numAckMsg < info.numServers) {
            return;
        }
        // Cleanup mNonStableChunks, after the lease cleanup, for extra
        // safety: this will prevent make chunk stable from restarting
        // recursively, in the case if there are double or stale
        // write lease.
        ExpiredLeaseCleanup(req->chunkId);
        pathname = info.pathname;
        updateSizeFlag  = ! info.stripedFileFlag;
        updateMTimeFlag = info.updateMTimeFlag;
        mNonStableChunks.erase(it);
        // "&info" is invalid at this point.
    }
    if (! pinfo) {
        CSMap::Entry* const ci = mChunkToServerMap.Find(req->chunkId);
        if (! ci) {
            KFS_LOG_STREAM_INFO << logPrefix <<
                " <"      << req->fid <<
                ","       << req->chunkId  << ">" <<
                " name: " << pathname <<
                " does not exist, skipping size update" <<
            KFS_LOG_EOM;
            return;
        }
        pinfo = ci;
    }
    UpdateReplicationState(*pinfo);
    int            numServers     = 0;
    int            numDownServers = 0;
    ChunkServerPtr goodServer;
    StTmp<Servers> serversTmp(mServers3Tmp);
    Servers&       servers = serversTmp.Get();
    mChunkToServerMap.GetServers(*pinfo, servers);
    for (Servers::const_iterator csi = servers.begin();
            csi != servers.end();
            ++csi) {
        if ((*csi)->IsDown()) {
            numDownServers++;
        } else {
            numServers++;
            if (! goodServer) {
                goodServer = *csi;
            }
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
                fa->mtime = now;
                submit_request(
                    new MetaSetMtime(fileId, fa->mtime));
            }
        }
        if (fa->numReplicas != numServers) {
            CheckReplication(*pinfo);
        } else {
            CancelPendingMakeStable(fileId, req->chunkId);
        }
    }
    KFS_LOG_STREAM_INFO << logPrefix <<
        " <" << req->fid << "," << req->chunkId  << ">"
        " fid: "              << fileId <<
        " version: "          << req->chunkVersion  <<
        " name: "             << pathname <<
        " size: "             << req->chunkSize <<
        " checksum: "         << req->chunkChecksum <<
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
    if (req->chunkSize >= 0) {
        // Already know the size, update it.
        // The following will invoke GetChunkSizeDone(),
        // and update the log.
        MetaChunkSize* const op = new MetaChunkSize(
            0, // seq #
            req->server, // chunk server
            fileId, req->chunkId, req->chunkVersion, pathname,
            false
        );
        op->chunkSize = req->chunkSize;
        submit_request(op);
    } else {
        // Get the chunk's size from one of the servers.
        goodServer->GetChunkSize(
            fileId, req->chunkId, req->chunkVersion, pathname);
    }
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
    MsgLogger::LogLevel logLevel = MsgLogger::kLogLevelDEBUG;
    if (! ci) {
        res = "no such chunk";
    } else if ((curChunkVersion = ci->GetChunkInfo()->chunkVersion) !=
            chunkVersion) {
        res      = "chunk version mismatch";
        logLevel = MsgLogger::kLogLevelERROR;
    }
    if (res) {
        // Failure.
    } else if (addFlag) {
        const PendingMakeStableEntry entry(
            chunkSize,
            hasChunkChecksum,
            chunkChecksum,
            chunkVersion
        );
        pair<PendingMakeStableMap::iterator, bool> const res =
            mPendingMakeStable.insert(make_pair(chunkId, entry));
        if (! res.second) {
            KFS_LOG_STREAM((res.first->second.mHasChecksum ||
                    res.first->second.mSize >= 0) ?
                    MsgLogger::kLogLevelWARN :
                    MsgLogger::kLogLevelDEBUG) <<
                "replay MCS add:" <<
                " update:"
                " chunkId: "  << chunkId <<
                " version: "  <<
                    res.first->second.mChunkVersion <<
                "=>"          << entry.mChunkVersion <<
                " size: "     << res.first->second.mSize <<
                "=>"          << entry.mSize <<
                " checksum: " <<
                    (res.first->second.mHasChecksum ?
                    int64_t(res.first->second.mChecksum) :
                    int64_t(-1)) <<
                "=>"          << (entry.mHasChecksum ?
                    int64_t(entry.mChecksum) :
                    int64_t(-1)) <<
            KFS_LOG_EOM;
            res.first->second = entry;
        }
    } else {
        PendingMakeStableMap::iterator const it =
            mPendingMakeStable.find(chunkId);
        if (it == mPendingMakeStable.end()) {
            res      = "no such entry";
            logLevel = MsgLogger::kLogLevelERROR;
        } else {
            const bool warn =
                it->second.mChunkVersion != chunkVersion ||
                (it->second.mSize >= 0 && (
                    it->second.mSize != chunkSize ||
                    it->second.mHasChecksum !=
                        hasChunkChecksum ||
                    (hasChunkChecksum &&
                    it->second.mChecksum != chunkChecksum
                )));
            KFS_LOG_STREAM(warn ?
                    MsgLogger::kLogLevelWARN :
                    MsgLogger::kLogLevelDEBUG) <<
                "replay MCS remove:"
                " chunkId: "  << chunkId <<
                " version: "  << it->second.mChunkVersion <<
                "=>"          << chunkVersion <<
                " size: "     << it->second.mSize <<
                "=>"          << chunkSize <<
                " checksum: " << (it->second.mHasChecksum ?
                    int64_t(it->second.mChecksum) :
                    int64_t(-1)) <<
                "=>"          << (hasChunkChecksum ?
                    int64_t(chunkChecksum) : int64_t(-1)) <<
            KFS_LOG_EOM;
            mPendingMakeStable.erase(it);
        }
    }
    KFS_LOG_STREAM(logLevel) <<
        "replay MCS: " <<
        (addFlag ? "add" : "remove") <<
        " "           << (res ? res : "ok") <<
        " total: "    << mPendingMakeStable.size() <<
        " chunkId: "  << chunkId <<
        " version: "  << chunkVersion <<
        " cur vers: " << curChunkVersion <<
        " size: "     << chunkSize <<
        " checksum: " << (hasChunkChecksum ?
            int64_t(chunkChecksum) : int64_t(-1)) <<
    KFS_LOG_EOM;
}

int
LayoutManager::WritePendingMakeStable(ostream& os) const
{
    // Write all entries in restore_makestable() format.
    for (PendingMakeStableMap::const_iterator it =
                mPendingMakeStable.begin();
            it != mPendingMakeStable.end() && os;
            ++it) {
        os <<
            "mkstable"
            "/chunkId/"      << it->first <<
            "/chunkVersion/" << it->second.mChunkVersion  <<
            "/size/"         << it->second.mSize <<
            "/checksum/"     << it->second.mChecksum <<
            "/hasChecksum/"  << (it->second.mHasChecksum ? 1 : 0) <<
        "\n";
    }
    return (os ? 0 : -EIO);
}

void
LayoutManager::CancelPendingMakeStable(fid_t fid, chunkId_t chunkId)
{
    PendingMakeStableMap::iterator const it =
        mPendingMakeStable.find(chunkId);
    if (it == mPendingMakeStable.end()) {
        return;
    }
    NonStableChunksMap::iterator const nsi = mNonStableChunks.find(chunkId);
    if (nsi != mNonStableChunks.end()) {
        KFS_LOG_STREAM_ERROR <<
            "delete pending MCS:"
            " <" << fid << "," << chunkId << ">" <<
            " attempt to delete while " <<
            (nsi->second.beginMakeStableFlag ? "B" :
                (nsi->second.logMakeChunkStableFlag ? "L" : "")) <<
            "MCS is in progress denied" <<
        KFS_LOG_EOM;
        return;
    }
    // Emit done log record -- this "cancels" "mkstable" log record.
    // Do not write if begin make stable wasn't started before the
    // chunk got deleted.
    MetaLogMakeChunkStableDone* const op =
        (it->second.mSize < 0 || it->second.mChunkVersion < 0) ? 0 :
        new MetaLogMakeChunkStableDone(
            fid, chunkId, it->second.mChunkVersion,
            it->second.mSize, it->second.mHasChecksum,
            it->second.mChecksum, chunkId
        );
    mPendingMakeStable.erase(it);
    mPendingBeginMakeStable.erase(chunkId);
    KFS_LOG_STREAM_DEBUG <<
        "delete pending MCS:"
        " <" << fid << "," << chunkId << ">" <<
        " total: " << mPendingMakeStable.size() <<
        " " << (op ? op->Show() : string("size < 0")) <<
    KFS_LOG_EOM;
    if (op) {
        submit_request(op);
    }
}

int
LayoutManager::GetChunkSizeDone(MetaChunkSize* req)
{
    if (! req->retryFlag && (req->chunkSize < 0 || req->status < 0)) {
        return -1;
    }
    if (! IsChunkStable(req->chunkId) ||
            mChunkLeases.HasWriteLease(req->chunkId)) {
        return -1; // Chunk isn't stable yet, or being written again.
    }
    const CSMap::Entry* const ci = mChunkToServerMap.Find(req->chunkId);
    if (! ci) {
        return -1; // No such chunk, do not log.
    }
    MetaFattr* const           fa    = ci->GetFattr();
    const MetaChunkInfo* const chunk = ci->GetChunkInfo();
    // Coalesce can change file id while request is in flight.
    if (req->fid != fa->id()) {
        req->fid = fa->id();
        req->pathname.clear(); // Path name is no longer valid.
    }
    if (fa->IsStriped() || fa->filesize >= 0 || fa->type != KFS_FILE ||
            chunk->offset + (chunkOff_t)CHUNKSIZE <
                fa->nextChunkOffset()) {
        return -1; // No update needed, do not write log entry.
    }
    if (req->chunkVersion != chunk->chunkVersion) {
        KFS_LOG_STREAM_DEBUG <<
            " last chunk: " << chunk->chunkId      <<
            " version: "    << chunk->chunkVersion <<
            " ignoring: "   << req->Show()         <<
            " status: "     << req->status         <<
            " msg: "        << req->statusMsg      <<
        KFS_LOG_EOM;
        return -1;
    }
    if (req->chunkSize < 0 || req->status < 0) {
        KFS_LOG_STREAM_ERROR <<
            req->Show() <<
            " status: " << req->status <<
            " msg: "    << req->statusMsg <<
        KFS_LOG_EOM;
        if (! req->retryFlag) {
            return -1;
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
                req->fid, req->chunkId, req->chunkVersion,
                req->pathname, retryFlag);
        }
        return -1;
    }
    metatree.setFileSize(fa, chunk->offset + req->chunkSize);
    KFS_LOG_STREAM_INFO <<
        "file: "      << req->fid <<
        " chunk: "    << req->chunkId <<
        " size: "     << req->chunkSize <<
        " filesize: " << fa->filesize <<
    KFS_LOG_EOM;
    return 0;
}

bool
LayoutManager::IsChunkStable(chunkId_t chunkId)
{
    return (mNonStableChunks.find(chunkId) == mNonStableChunks.end());
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
    placement.FindCandidatesForReplication();
    const size_t numRacks = placement.GetCandidateRackCount();
    const size_t numServersPerRack = numRacks <= 1 ?
        (size_t)extraReplicas :
        ((size_t)extraReplicas + numRacks - 1) / numRacks;
    // Find candidates other than those that are already hosting the chunk.
    StTmp<Servers> serversTmp(mServersTmp);
    Servers& candidates = serversTmp.Get();
    for (int rem = extraReplicas; ;) {
        const size_t psz = candidates.size();
        for (size_t i = 0; ; ) {
            const ChunkServerPtr cs =
                placement.GetNext(useServerExcludesFlag);
            if (! cs) {
                break;
            }
            if (placement.IsUsingServerExcludes() && (
                    find(candidates.begin(),
                        candidates.end(), cs) !=
                    candidates.end() ||
                    mChunkToServerMap.HasServer(cs, clli))) {
                continue;
            }
            candidates.push_back(cs);
            if (--rem <= 0 || ++i >= numServersPerRack) {
                break;
            }
        }
        if (rem <= 0 || placement.IsLastRack()) {
            break;
        }
        placement.ExcludeServer(
            candidates.begin() + psz, candidates.end());
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
    return ReplicateChunk(clli, extraReplicas, candidates, recoveryInfo);
}

int
LayoutManager::ReplicateChunk(
    CSMap::Entry&                 clli,
    int                           extraReplicas,
    const LayoutManager::Servers& candidates,
    const ChunkRecoveryInfo&      recoveryInfo,
    const char*                   reasonMsg)
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
    int numDone = 0;
    for (Servers::const_iterator it = candidates.begin();
            numDone < extraReplicas && it != candidates.end();
            ++it) {
        const ChunkServerPtr& c  = *it;
        ChunkServer&          cs = *c;
        // verify that we got good candidates
        if (find(servers.begin(), servers.end(), c) != servers.end()) {
            panic("invalid replication candidate");
        }
        if (cs.IsDown()) {
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
        // Do not increment replication read load when starting
        // chunk recovery.
        // Recovery decides from where to read.
        // With recovery dataServer == &cs here, and the source
        // location in the request will only have meta server
        // port, and empty host name.
        if (! recoveryInfo.HasRecovery() || dataServer != c) {
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
        cs.ReplicateChunk(clli.GetFileId(), clli.GetChunkId(),
            dataServer, recoveryInfo);
        // Do not count synchronous failures.
        if (! cs.IsDown()) {
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
            if (GetInFlightChunkModificationOpCount(
                    (*it)->chunkId,
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

    const MetaFattr* const fa      = c.GetFattr();
    const chunkId_t        chunkId = c.GetChunkId();
    // Don't replicate chunks for which a write lease has been
    // issued.
    const ChunkLeases::WriteLease* const wl =
        mChunkLeases.GetWriteLease(chunkId);
    if (wl) {
        KFS_LOG_STREAM_DEBUG <<
            "re-replication delayed chunk:"
            " <" << c.GetFileId() << "," << chunkId << ">"
            " " << (TimeNow() <= wl->expires ?
                "valid" : "expired") <<
            " write lease exists" <<
        KFS_LOG_EOM;
        if (recoveryInfo) {
            SetReplicationState(c,
                CSMap::Entry::kStatePendingReplication);
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
            SetReplicationState(c,
                CSMap::Entry::kStatePendingReplication);
        }
        return false;
    }
    const MetaChunkInfo* const chunk           = c.GetChunkInfo();
    size_t                     hibernatedCount = 0;
    StTmp<Servers>             serversTmp(mServers3Tmp);
    Servers&                   servers = serversTmp.Get();
    mChunkToServerMap.GetServers(c, servers, hibernatedCount);
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
                SetReplicationState(c,
                    CSMap::Entry::kStatePendingReplication);
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
        int              stripeIdx = 0;
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
            if (mChunkLeases.GetWriteLease(curChunkId) ||
                    ! IsChunkStable(curChunkId)) {
                notStable++;
                break;
                // MakeChunkStableDone will restart
                // re-replication.
            }
            Servers&            srvs = serversTmp.Get();
            const CSMap::Entry& ce   = GetCsEntry(**it);
            if (mChunkToServerMap.GetServers(ce, srvs) > 0) {
                good++;
            }
            if (chunkId != curChunkId) {
                GetInFlightChunkModificationOpCount(
                    curChunkId, &srvs);
            }
            placement.ExcludeServerAndRack(srvs, curChunkId);
            ++it;
        }
        if (notStable > 0 ||
                (notStable == 0 && good < fa->numStripes)) {
            if (! servers.empty()) {
                // Can not use recovery instead of replication.
                SetReplicationState(c,
                    CSMap::Entry::kStateNoDestination);
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
                        mChunkLeases.GetWriteLease(
                            chunkId) ||
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
        const int64_t timeMicrosec =
            (int64_t)TimeNow() * kSecs2MicroSecs;
        if (notStable != 0 || (fa->filesize <=
                fa->ChunkPosToChunkBlkFileStartPos(start) &&
                timeMicrosec < fa->mtime +
                    mPastEofRecoveryDelay)) {
            if (! servers.empty()) {
                // Cannot use recovery instead of replication.
                SetReplicationState(c,
                    CSMap::Entry::kStateNoDestination);
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
            SetReplicationState(c,
                CSMap::Entry::kStateDelayedRecovery);
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
        " hibernated: " << hibernatedCount <<
        " needed: "     << extraReplicas <<
    KFS_LOG_EOM;
    if (readLeaseWaitFlag) {
        SetReplicationState(c, CSMap::Entry::kStatePendingReplication);
        return false;
    }
    return true;
}

void
LayoutManager::CheckHibernatingServersStatus()
{
    const time_t now = TimeNow();

    for (HibernatedServerInfos::iterator
            iter = mHibernatingServers.begin();
            iter != mHibernatingServers.end();
            ) {
        Servers::const_iterator const i = find_if(
            mChunkServers.begin(), mChunkServers.end(),
            MatchingServer(iter->location));
        if (i == mChunkServers.end() && now < iter->sleepEndTime) {
            // within the time window where the server is sleeping
            // so, move on
            iter++;
            continue;
        }
        if (i != mChunkServers.end()) {
            if (! iter->IsHibernated()) {
                if (iter->sleepEndTime + 10 * 60 < now) {
                    KFS_LOG_STREAM_INFO <<
                        "hibernated server: " <<
                            iter->location  <<
                        " still connected, canceling"
                        " hibernation" <<
                    KFS_LOG_EOM;
                    iter = mHibernatingServers.erase(iter);
                }
                continue;
            }
            KFS_LOG_STREAM_INFO <<
                "hibernated server: " << iter->location  <<
                " is back as promised" <<
            KFS_LOG_EOM;
        } else {
            // server hasn't come back as promised...so, check
            // re-replication for the blocks that were on that node
            KFS_LOG_STREAM_INFO <<
                "hibernated server: " << iter->location <<
                " is NOT back as promised" <<
            KFS_LOG_EOM;
        }
        if (! mChunkToServerMap.RemoveHibernatedServer(iter->csmapIdx)) {
            panic("failed to remove hibernated server");
        }
        iter = mHibernatingServers.erase(iter);
    }
}

int
LayoutManager::CountServersAvailForReReplication() const
{
    int anyAvail = 0;
    for (uint32_t i = 0; i < mChunkServers.size(); i++) {
        const ChunkServer& cs = *mChunkServers[i].get();
        if (cs.GetSpaceUtilization(mUseFsTotalSpaceFlag) >
                mMaxSpaceUtilizationThreshold) {
            continue;
        }
        if (cs.GetNumChunkReplications() >=
                mMaxConcurrentWriteReplicationsPerNode) {
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
    MetaOp const makePendingOpTypes[] = {
        META_CHUNK_REPLICATE,
        META_CHUNK_VERSCHANGE,
        META_CHUNK_MAKE_STABLE,
        META_NUM_OPS_COUNT     // Sentinel
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
    for (; ; loopCount++) {
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
        if (avail <= 0 && (avail =
                CountServersAvailForReReplication()) <= 0) {
            if (count <= 0) {
                mNoServersAvailableForReplicationCount++;
                KFS_LOG_STREAM_INFO <<
                    "exiting replication check:"
                    " no servers available for"
                    " replication: " <<
                    mNoServersAvailableForReplicationCount <<
                KFS_LOG_EOM;
            }
            break;
        }
        CSMap::Entry* cur = mChunkToServerMap.Next(
            CSMap::Entry::kStateCheckReplication);
        if (! cur) {
            // See if all chunks check was requested.
            if (! (cur = mChunkToServerMap.Next(
                    CSMap::Entry::kStateNone))) {
                mCheckAllChunksInProgressFlag = false;
                nextRunLowPriorityFlag = true;
                if (! (cur = mChunkToServerMap.Next(
                            CSMap::Entry::kStateNoDestination)) &&
                        ! (cur = mChunkToServerMap.Next(
                            CSMap::Entry::kStateDelayedRecovery))) {
                    mChunkToServerMap.First(
                        CSMap::Entry::kStateNoDestination);
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

        if (GetInFlightChunkOpsCount(entry.GetChunkId(),
                makePendingOpTypes) > 0) {
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
                SetReplicationState(entry,
                    CSMap::Entry::kStateNoDestination);
            }
            count += numStarted;
            avail -= numStarted;
        } else {
            if (extraReplicas < 0) {
                DeleteAddlChunkReplicas(
                    entry, -extraReplicas, placement);
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
    mLastReplicationCheckRunEndTime =
        pass == kCheckTime ? now : microseconds();
    return timedOutFlag;
}

void LayoutManager::Timeout()
{
    ScheduleCleanup(mMaxServerCleanupScan);
}

void LayoutManager::ScheduleCleanup(size_t maxScanCount /* = 1 */)
{
    if (mChunkToServerMap.RemoveServerCleanup(maxScanCount)) {
        if (! mCleanupScheduledFlag) {
            mCleanupScheduledFlag = true;
            globalNetManager().RegisterTimeoutHandler(this);
        }
        globalNetManager().Wakeup();
    } else {
        if (mCleanupScheduledFlag) {
            mCleanupScheduledFlag = false;
            globalNetManager().UnRegisterTimeoutHandler(this);
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
    if (! mPendingBeginMakeStable.empty() && ! InRecoveryPeriod()) {
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
                EvacuateChunkChecker(
                    mMightHaveRetiringServersFlag));
        }
    }
    if (runRebalanceFlag &&
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
LayoutManager::ChunkReplicationDone(MetaChunkReplicate* req)
{
    const bool versChangeDoneFlag = req->versChange != 0;
    assert(! req->suspended || versChangeDoneFlag);
    if (versChangeDoneFlag) {
        if (! req->suspended) {
            req->versChange = 0;
            return;
        }
        assert(! req->versChange->clnt);
        req->suspended = false;
        req->status    = req->versChange->status;
        req->statusMsg = req->versChange->statusMsg;
    }

    // In the recovery case the source location's host name is empty.
    const bool replicationFlag = req->srcLocation.IsValid();
    KFS_LOG_STREAM_INFO <<
        (versChangeDoneFlag ? "version change" :
            (replicationFlag ? "replication" : "recovery")) <<
            " done:"
        " chunk: "      << req->chunkId <<
        " version: "    << req->chunkVersion <<
        " status: "     << req->status <<
        (req->statusMsg.empty() ? "" : " ") << req->statusMsg <<
        " server: " << req->server->GetServerLocation() <<
        " " << (req->server->IsDown() ? "down" : "OK") <<
        " replications in flight: " << mNumOngoingReplications <<
    KFS_LOG_EOM;

    if (! versChangeDoneFlag) {
        mOngoingReplicationStats->Update(-1);
        assert(mNumOngoingReplications > 0);
        mNumOngoingReplications--;
        req->server->ReplicateChunkDone(req->chunkId);
        if (replicationFlag && req->dataServer) {
            req->dataServer->UpdateReplicationReadLoad(-1);
        }
        req->dataServer.reset();
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
            (req->server->GetNumChunkReplications() * 5 / 4 <
                mMaxConcurrentWriteReplicationsPerNode &&
            ! req->server->IsRetiring() &&
            ! req->server->IsDown())) {
        mChunkReplicator.ScheduleNext();
    }

    CSMap::Entry* const ci = mChunkToServerMap.Find(req->chunkId);
    if (! ci) {
        KFS_LOG_STREAM_INFO <<
            "chunk " << req->chunkId <<
            " mapping no longer exists" <<
        KFS_LOG_EOM;
        req->server->NotifyStaleChunk(req->chunkId);
        return;
    }
    if (req->status != 0 || req->server->IsDown()) {
        // Replication failed...we will try again later
        const fid_t fid = ci->GetFileId();
        KFS_LOG_STREAM_INFO <<
            req->server->GetServerLocation() <<
            ": re-replication failed"
            " chunk: "           << req->chunkId <<
            " fid: "             << req->fid << "/" << fid <<
            " status: "          << req->status <<
            " in flight: "       <<
                GetInFlightChunkOpsCount(
                    req->chunkId, META_CHUNK_REPLICATE) <<
            " invalid stripes: " << req->invalidStripes.size() <<
        KFS_LOG_EOM;
        mFailedReplicationStats->Update(1);
        UpdateReplicationState(*ci);
        // Aways send stale chunk notification properly handle op time
        // outs by the meta server. Theoretically this could be
        // conditional on the op status code, if it is guaranteed that
        // the chunk server never sends the op timed out status.
        if (req->server->IsDown() ||
                ci->HasServer(mChunkToServerMap, req->server)) {
            return;
        }
        if (! versChangeDoneFlag && fid == req->fid) {
            ProcessInvalidStripes(*req);
        }
        req->server->NotifyStaleChunk(req->chunkId);
        if (! replicationFlag || req->server->IsDown() ||
                versChangeDoneFlag) {
            return;
        }
        const MetaFattr* const fa = ci->GetFattr();
        if (fa->HasRecovery() &&
                mChunkToServerMap.ServerCount(*ci) == 1) {
            KFS_LOG_STREAM_INFO <<
                "chunk: " << req->chunkId <<
                " fid: "  << req->fid << "/" << fid <<
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
                        req->chunkId) <= 0 &&
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
                SetReplicationState(*ci,
                    CSMap::Entry::kStateNoDestination);
            }
        }
        return;
    }
    // replication succeeded: book-keeping
    // validate that the server got the latest copy of the chunk
    const MetaChunkInfo* const chunk = ci->GetChunkInfo();
    if (chunk->chunkVersion != req->chunkVersion) {
        // Version that we replicated has changed...so, stale
        KFS_LOG_STREAM_INFO <<
            req->server->GetServerLocation() <<
            " re-replicate: chunk " << req->chunkId <<
            " version changed was: " << req->chunkVersion <<
            " now " << chunk->chunkVersion << " => stale" <<
        KFS_LOG_EOM;
        mFailedReplicationStats->Update(1);
        UpdateReplicationState(*ci);
        req->server->NotifyStaleChunk(req->chunkId);
        return;
    }
    if (! replicationFlag && ! versChangeDoneFlag) {
        const fid_t fid = ci->GetFileId();
        if (fid != req->fid) {
            KFS_LOG_STREAM_INFO <<
                req->server->GetServerLocation() <<
                " recover: chunk " << req->chunkId <<
                " file id changed:"
                " was: "  << req->fid <<
                " now: "  << fid << " => stale" <<
            KFS_LOG_EOM;
            UpdateReplicationState(*ci);
            req->server->NotifyStaleChunk(req->chunkId);
            return;
        }
        req->suspended = true;
        const bool kMakeStableFlag = true;
        const bool kPendingAddFlag = false;
        req->server->NotifyChunkVersChange(
            req->fid,
            req->chunkId,
            req->chunkVersion, // to
            0,                 // from
            kMakeStableFlag,
            kPendingAddFlag,
            req
        );
        return;
    }
    UpdateReplicationState(*ci);
    // Yaeee...all good...
    KFS_LOG_STREAM_DEBUG <<
        req->server->GetServerLocation() <<
        " chunk: " << req->chunkId <<
        (replicationFlag ? " re-replication" : " recovery") <<
        " done" <<
    KFS_LOG_EOM;
    AddHosted(*ci, req->server);
    req->server->MovingChunkDone(req->chunkId);
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
        if (! mChunkToServerMap.RemoveServer(server, ci)) {
            panic("failed to remove server");
        }
        if (! server->IsDown()) {
            if (server->IsRetiring()) {
                if (server->GetChunkCount() <= 0) {
                    server->Retire();
                } else if (deleteRetiringFlag) {
                    server->DeleteChunk(chunkId);
                }
            } else {
                const bool kEvacuateChunkFlag = true;
                server->NotifyStaleChunk(
                    chunkId, kEvacuateChunkFlag);
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
            fa->stripeSize != req.stripeSize ||
            fa->numStripes != req.numStripes ||
            fa->numRecoveryStripes != req.numRecoveryStripes) {
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
        if (mChunkLeases.GetWriteLease((*it)->chunkId) ||
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
        KFS_LOG_STREAM_INFO << "invalidating:"
            " <"         << req.fid <<
            ","          << cit->chunkId << ">"
            " version: " << cit->chunkVersion <<
            " offset: "  << cit->offset <<
            " stripe: "  << sit->first <<
        KFS_LOG_EOM;
        if (mPanicOnInvalidChunkFlag) {
            ostringstream os;
            os <<
            "invalid chunk detected:"
            " <"         << req.fid <<
            ","          << cit->chunkId << ">"
            " version: " << cit->chunkVersion <<
            " offset: "  << cit->offset <<
            " stripe: "  << sit->first;
            panic(os.str());
        }
        MetaAllocate& alloc = *(new MetaAllocate(
            sit->first, req.fid, cit->offset));
        alloc.invalidateAllFlag = true;
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
        for (Servers::iterator it = servers.begin();
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
        entry.Remove(mChunkToServerMap, server);
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
    if (InRecovery() ||
            mChunkServers.empty() ||
            mChunkToServerMap.Size() <= 0) {
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
        const char* reason = 0;
        if (srvPos >= 0 || rackPos >= 0) {
            srvs.clear();
            double maxUtilization = mMinRebalanceSpaceUtilThreshold;
            for (int i = 0; ; i++) {
                if (i > 0) {
                    if (mMaxSpaceUtilizationThreshold <=
                            maxUtilization) {
                        break;
                    }
                    maxUtilization =
                        mMaxSpaceUtilizationThreshold;
                }
                placement.FindRebalanceCandidates(
                    maxUtilization);
                if (srvPos < 0) {
                    if (placement.IsUsingRackExcludes()) {
                        continue;
                    }
                    const RackId rackId =
                        placement.GetRackId();
                    if (rackId < 0 || rackId == srvs[
                            rackPos]->GetRack()) {
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
                reason = srvPos >= 0 ?
                    "re-balance server placement" :
                    "re-balance rack placement";
                break;
            }
        } else if (loadPos >= 0) {
            const RackId rackId = srvs[loadPos]->GetRack();
            placement.FindRebalanceCandidates(
                mMinRebalanceSpaceUtilThreshold,
                rackId
            );
            const bool kCanIgnoreServerExcludesFlag = false;
            const ChunkServerPtr srv = placement.GetNext(
                kCanIgnoreServerExcludesFlag);
            srvs.clear();
            if (srv && (srv->GetRack() >= 0 || rackId < 0) &&
                    ((placement.GetRackId() >= 0 &&
                        ! placement.IsUsingRackExcludes()
                    ) ||
                    (placement.GetCandidateRackCount() <=
                        0 &&
                    placement.GetExcludedRacksCount() +
                    numReplicas >= mRacks.size()))) {
                srvs.push_back(srv);
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
        if (ReplicateChunk(entry, 1, srvs, recoveryInfo, reason) > 0) {
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
        Servers::const_iterator const it = find_if(
            mChunkServers.begin(), mChunkServers.end(),
            MatchingServer(loc)
        );
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
    if (! mIsExecutingRebalancePlan || c->IsRetiring() || c->IsDown()) {
        c->ClearChunksToMove();
        return 0;
    }
    ChunkIdSet& chunksToMove = const_cast<ChunkIdSet&>(c->GetChunksToMove());
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

    StTmp<ChunkPlacement> placementTmp(mChunkPlacementTmp);
    StTmp<Servers>        serversTmp(mServersTmp);
    StTmp<Servers>        candidatesTmp(mServers2Tmp);
    Servers&              candidates = candidatesTmp.Get();
    candidates.push_back(c);

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
                "re-balance plan") > 0) {
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
        kKfsUserRoot, kKfsGroupRoot, 0644);
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
        mChunkServers.push_back(ChunkServerPtr(
            new ChunkServer(NetConnectionPtr(
            new NetConnection(
            new TcpSocket(), 0)))));
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
    "PlanLine"              << pref << mPlanLine << suf <<
    "PlanNoServer"              << pref << mPlanNoServer << suf <<
    "PlanAdded"             << pref << mPlanAdded << suf <<
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

} // namespace KFS
