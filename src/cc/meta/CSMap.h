//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2011/02/03
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
// \file CSMap.h
// \brief Chunk ids to chunk server, file id, and replication state table.
//
//----------------------------------------------------------------------------

#ifndef CS_MAP_H
#define CS_MAP_H

#include "qcdio/QCDLList.h"
#include "common/LinearHash.h"
#include "common/PoolAllocator.h"
#include "common/StdAllocator.h"
#include "kfstypes.h"
#include "meta.h"
#include "util.h"
#include "MetaRequest.h"
#include "ChunkServer.h"

#include <stdint.h>
#include <assert.h>
#include <string.h>
#include <boost/static_assert.hpp>

namespace KFS
{
using std::vector;

// chunkid to server(s) map
class CSMap
{
public:
    class Entry;
private:
    typedef QCDLListOp<Entry, 0> EList;
public:
    typedef MetaRequest::Servers Servers;

    class Entry : private MetaChunkInfo
    {
    public:
        enum State
        {
            // Order is important: the lists are scanned in this
            // order.
            kStateNone               = 0,
            kStateCheckReplication   = 1,
            kStatePendingReplication = 2,
            kStateNoDestination      = 3,
            kStatePendingRecovery    = 4,
            kStateDelayedRecovery    = 5,
            kStateCount
        };

        explicit Entry(MetaFattr* fattr = 0, chunkOff_t offset = 0,
                chunkId_t chunkId = 0, seq_t chunkVersion = 0)
            : MetaChunkInfo(fattr, offset, chunkId, chunkVersion),
              mIdxData(0)
        {
            EList::Init(*this);
            BOOST_STATIC_ASSERT(sizeof(void*) <= sizeof(mIdxData));
        }
        explicit Entry(chunkId_t chunkId, const Entry& entry)
            : MetaChunkInfo(entry.fattr,
                entry.offset, entry.chunkId, entry.chunkVersion),
              mIdxData(0)
        {
            assert(chunkId == this->chunkId && entry.mIdxData == 0);
            EList::Init(*this);
        }
        ~Entry()
        {
            EList::Remove(*this);
            if (IsAddr()) {
                AddrClear();
            }
        }
        const chunkId_t& GetChunkId() const { return chunkId; }
        fid_t GetFileId() const { return id(); }
        MetaFattr* GetFattr() const { return fattr; }
        void SetFattr(MetaFattr* fa) {
            if (! fa) {
                panic("SetFattr: null argument", false);
                return;
            }
            fattr = fa;
        }
        void destroySelf();
        MetaChunkInfo* GetChunkInfo() const
            { return const_cast<Entry*>(this); }
        size_t ServerCount(const CSMap& map) const {
            return map.ServerCount(*this);
        }
        ChunkServerPtr GetServer(const CSMap& map) const {
            return map.GetServer(*this);
        }
        Servers GetServers(const CSMap& map) const {
            return map.GetServers(*this);
        }
        void SetServers(CSMap& map, const Servers& servers) {
            return map.SetServers(servers, *this);
        }
        bool Add(const CSMap& map, const ChunkServerPtr& server) {
            return map.AddServer(server, *this);
        }
        bool Remove(const CSMap& map, const ChunkServerPtr& server) {
            return map.RemoveServer(server, *this);
        }
        void RemoveAllServers(const CSMap& map) {
            map.RemoveHosted(*this);
            ClearServers();
        }
        bool HasServer(const CSMap& map,
                const ServerLocation& loc) const {
            return map.HasServer(loc, *this);
        }
        bool HasServer(const CSMap& map,
                const ChunkServerPtr& srv) const {
            return map.HasServer(srv, *this);
        }
        ChunkServerPtr GetServer(const CSMap& map,
                const ServerLocation& loc) const {
            return map.GetServer(loc, *this);
        }
        static size_t GetAllocBlockCount() {
            return GetAllocator().GetBlockCount();
        }
        static size_t GetAllocByteCount() {
            return GetAllocator().GetByteCount();
        }
        static const Entry& GetCsEntry(const MetaChunkInfo& chunkInfo) {
            return static_cast<const Entry&>(chunkInfo);
        }
        static Entry& GetCsEntry(MetaChunkInfo& chunkInfo) {
            return static_cast<Entry&>(chunkInfo);
        }
        static Entry* GetCsEntry(MetaChunkInfo* chunkInfo) {
            return static_cast<Entry*>(chunkInfo);
        }
        static Entry* GetCsEntry(MetaNode* node) {
            return static_cast<Entry*>(node);
        }
        static const Entry* GetCsEntry(const MetaNode* node) {
            return static_cast<const Entry*>(node);
        }
    private:
        typedef uint64_t IdxData;
        typedef uint16_t AllocIdx;
        enum
        {
            kNumStateBits    = 3,
            kIdxBits         = 15,
            kIdxMask         = (1 << kIdxBits) - 1,
            kFirstIdxShift   = sizeof(IdxData) * 8 - kIdxBits,
            kMaxNonAllocSrvs = sizeof(IdxData) * 8 / kIdxBits,
            kMaxServers      = kIdxMask, // sentinel is 0
            kSentinel        = 0,        // sentinel entry
            kStateMask       = (1 << kNumStateBits) - 1,
            kAllocated       = 1 << kNumStateBits,
            kOtherBitsMask   = kStateMask | kAllocated,
            kAddrAlign       = kOtherBitsMask + 1,
            // kNumStateBits + 1 low order bits are used, address
            // must be aligned accordingly.
            // Allocated array positions.
            kAddrCapacityPos = 0,
            kAddrSizePos     = 1,
            kAddrIdxPos      = 2
        };
        BOOST_STATIC_ASSERT(
            kStateCount <= kStateMask + 1 &&
            kIdxBits * kMaxNonAllocSrvs + kNumStateBits <
                sizeof(IdxData) * 8 &&
            kAddrAlign % sizeof(AllocIdx) == 0
        );
        class Allocator
        {
        public:
            Allocator()
                : mBlockCount(0),
                  mByteCount(0)
                {}
            char* Allocate(size_t n) {
                assert(n > 0);
                char* const ret = mAlloc.allocate(n);
                if (! ret) {
                    panic("allocation failure", true);
                    return 0;
                }
                mBlockCount++;
                mByteCount += n;
                return ret;
            }
            void Deallocate(char* ptr, size_t n) {
                if (! ptr) {
                    return;
                }
                assert(
                    n > 0 &&
                    mByteCount >= n && mBlockCount > 0
                );
                mAlloc.deallocate(ptr, n);
                mByteCount -= n;
                mBlockCount--;
            }
            size_t GetBlockCount() const {
                return mBlockCount;
            }
            size_t GetByteCount() const {
                return mByteCount;
            }
        private:
            StdAllocator<char> mAlloc;
            size_t             mBlockCount;
            size_t             mByteCount;
        };

        IdxData mIdxData;
        Entry*  mPrevPtr[1];
        Entry*  mNextPtr[1];

        static Allocator& GetAllocator() {
            static Allocator alloc;
            return alloc;
        }
        size_t ServerCount() const {
            if (IsAddr()) {
                return AddrCount();
            }
            size_t  count = 0;
            IdxData mask  = IdxData(kIdxMask) << kFirstIdxShift;
            while ((mask & mIdxData) != 0) {
                count++;
                mask >>= kIdxBits;
                mask &= ~IdxData(kOtherBitsMask);
            }
            return count;
        }
        bool HasServers() const {
            return (IsAddr() || (mIdxData &
                (IdxData(kIdxMask) << kFirstIdxShift)) != 0);
        }
        State GetState() const {
            return (State)(mIdxData & kStateMask);
        }
        void SetState(State state) {
            mIdxData &= ~IdxData(kStateMask);
            mIdxData |= IdxData(kStateMask & state);
        }
        bool IsAddr() const {
            return ((mIdxData & kAllocated) != 0);
        }
        void ClearServers()
        {
            if (IsAddr()) {
                AddrClear();
            } else {
                mIdxData &= IdxData(kStateMask);
            }
        }
        bool HasIndex(size_t idx) const {
            if (idx >= kMaxServers) {
                return false;
            }
            if (IsAddr()) {
                return AddrHasIndex((AllocIdx)(idx + 1));
            }
            for (IdxData id = IdxData(idx + 1) <<
                        kFirstIdxShift,
                    data = mIdxData &
                        ~IdxData(kOtherBitsMask),
                    mask = IdxData(kIdxMask) <<
                        kFirstIdxShift;
                    (data & mask) != 0;
                    id   >>= kIdxBits,
                    mask >>= kIdxBits) {
                if ((data & mask) == id) {
                    return true;
                }
            }
            return false;
        }
        bool AddIndex(size_t idx) {
            if (idx >= kMaxServers) {
                return false;
            }
            const AllocIdx index = (AllocIdx)(idx + 1);
            if (IsAddr()) {
                return AddrAddIndex(index);
            }
            for (IdxData id = IdxData(index) <<
                        kFirstIdxShift,
                    mask = IdxData(kIdxMask) <<
                        kFirstIdxShift;
                    mask >= IdxData(kIdxMask);
                    id   >>= kIdxBits,
                    mask >>= kIdxBits) {
                if ((mIdxData & mask) == 0) {
                    mIdxData |= id;
                    return true;
                }
                if ((mIdxData & mask) == id) {
                    return false;
                }
            }
            AllocateIndexes(index, kMaxNonAllocSrvs + 1);
            return true;
        }
        bool RemoveIndex(size_t idx) {
            if (idx >= kMaxServers) {
                return false;
            }
            const AllocIdx index = (AllocIdx)(idx + 1);
            if (IsAddr()) {
                return AddrRemoveIndex(index);
            }
            int shift = kFirstIdxShift;
            for (IdxData id = IdxData(index) <<
                        kFirstIdxShift,
                    data = mIdxData &
                        ~IdxData(kOtherBitsMask),
                    mask = IdxData(kIdxMask) <<
                        kFirstIdxShift;
                    (data & mask) != 0;
                    id   >>= kIdxBits,
                    mask >>= kIdxBits,
                    shift -= kIdxBits) {
                if ((data & mask) == id) {
                    const IdxData lm =
                        (IdxData(1) << shift) - 1;
                    mIdxData =
                        (data & ~(mask | lm)) |
                        ((data & lm) << kIdxBits) |
                        (mIdxData & kOtherBitsMask);
                    return true;
                }
            }
            return false;
        }
        size_t IndexAt(size_t idx) const {
            if (IsAddr()) {
                return AddrIndexAt(idx);
            }
            return ((size_t)((mIdxData >>
                (kFirstIdxShift - idx * kIdxBits)) & kIdxMask)
                - 1);
        }
        size_t AddrIndexAt(size_t idx) const {
            return (AddrGetArray()[idx + kAddrIdxPos] - 1);
        }
        AllocIdx* AddrGetIdxPtr() const {
            return reinterpret_cast<AllocIdx*>((char*)0 +
                (mIdxData & ~IdxData(kOtherBitsMask)));
        }
        void AddrSet(AllocIdx* addr) {
            const IdxData idxAddr = (IdxData)
                (reinterpret_cast<char*>(addr) - (char*)0);
            assert((idxAddr & kOtherBitsMask) == 0);
            mIdxData &= IdxData(kStateMask);
            mIdxData |= idxAddr;
            mIdxData |= kAllocated;
        }
        AllocIdx* AddrGetArray() const {
            AllocIdx* p = AddrGetIdxPtr();
            if ((p[0] & ~AllocIdx(kIdxMask)) != 0) {
                p++; // First slot is alignment
            }
            return p;
        }
        size_t AddrCount() const {
            return (AddrGetArray()[kAddrSizePos] - kAddrIdxPos);
        }
        void AddrClear() {
            AllocIdx* p = AddrGetIdxPtr();
            size_t size;
            const AllocIdx kMask(kIdxMask);
            if ((p[0] & ~kMask) != 0) {
                const size_t off = p[0] & kMask;
                size = kAddrAlign + (p[1] + 1) * sizeof(p[1]);
                p = reinterpret_cast<AllocIdx*>(
                    reinterpret_cast<char*>(p) - off);
            } else {
                size = p[0] * sizeof(p[0]);
            }
            GetAllocator().Deallocate(
                reinterpret_cast<char*>(p), size);
            mIdxData &= IdxData(kStateMask);
        }
        void AllocateIndexes(AllocIdx idx, size_t capacity) {
            const size_t kQuantum = 4;
            size_t       alloccap = (capacity + 3 + kQuantum - 1) /
                kQuantum * kQuantum;
            // Glibc malloc is expected to return 2 * sizeof(size_t)
            // aligned address.
            // The low order bits are used, ensure that the address
            // aligned accordingly.
            char* const  alloc = GetAllocator().Allocate(
                kAddrAlign + alloccap * sizeof(AllocIdx));
            size_t const align = (AllocIdx)((alloc - (char*)0) &
                size_t(kAddrAlign - 1));
            AllocIdx const off = (AllocIdx) (align > 0 ?
                (kAddrAlign - align) : 0);
            AllocIdx* const idxAddr = reinterpret_cast<AllocIdx*>(
                alloc + off);
            AllocIdx* indexes = idxAddr;
            if (off != 0) {
                *indexes++ = off | (AllocIdx(1) << kIdxBits);
                alloccap--;
            } else {
                alloccap += kAddrAlign / sizeof(AllocIdx);
            }
            indexes[kAddrCapacityPos] = alloccap;
            AllocIdx& size = indexes[kAddrSizePos];
            size = kAddrIdxPos;
            if (IsAddr()) {
                AllocIdx* const prev = AddrGetArray();
                assert(prev[kAddrSizePos] <= alloccap);
                memcpy(indexes + kAddrSizePos,
                    prev + kAddrSizePos,
                    (prev[kAddrSizePos] - 1) *
                    sizeof(prev[0]));
                if (idx != kSentinel) {
                    assert(size < alloccap);
                    indexes[size++] = idx;
                }
                AddrClear();
                AddrSet(idxAddr);
                return;
            }
            assert(alloccap > kMaxNonAllocSrvs + kAddrIdxPos &&
                idx != kSentinel);
            for (int shift = kFirstIdxShift;
                    shift >= 0;
                    shift -= kIdxBits) {
                const AllocIdx id = (AllocIdx)
                    ((mIdxData >> shift) & kIdxMask);
                assert(id > 0);
                indexes[size++] = id;
            }
            indexes[size++] = idx;
            AddrSet(idxAddr);
        }
        bool AddrHasIndex(AllocIdx idx) const {
            AllocIdx const* const indexes = AddrGetArray();
            AllocIdx size = indexes[kAddrSizePos];
            for (size_t i = kAddrIdxPos; i < size; i++) {
                if (idx == indexes[i]) {
                    return true;
                }
            }
            return false;
        }
        bool AddrAddIndex(AllocIdx idx) {
            if (AddrHasIndex(idx)) {
                return false;
            }
            AllocIdx* const indexes = AddrGetArray();
            AllocIdx& capacity = indexes[kAddrCapacityPos];
            AllocIdx& size     = indexes[kAddrSizePos];
            if (capacity > size) {
                indexes[size++] = idx;
            } else {
                AllocateIndexes(idx, capacity + 1);
            }
            return true;
        }
        bool AddrRemoveIndex(AllocIdx idx) {
            AllocIdx* const indexes = AddrGetArray();
            AllocIdx& capacity = indexes[kAddrCapacityPos];
            AllocIdx& size     = indexes[kAddrSizePos];
            size_t i;
            for (i = kAddrIdxPos; i < size && idx != indexes[i]; i++)
                {}
            if (i >= indexes[1]) {
                return false;
            }
            for (size_t k = i + 1; k < size; k++, i++) {
                indexes[i] = indexes[k];
            }
            size--;
            if (size <= kAddrIdxPos) {
                // 0 indexes
                AddrClear();
                return true;
            }
            if (size <= kMaxNonAllocSrvs + kAddrIdxPos) {
                // indexes fit into mIdxData
                IdxData data  = 0;
                int     shift = kFirstIdxShift;
                for (i = kAddrIdxPos; i < size; i++) {
                    data |= IdxData(indexes[i]) << shift;
                    shift -= kIdxBits;
                }
                AddrClear();
                mIdxData |= data;
                return true;
            }
            if (capacity / 2 >= size) {
                // Reallocate.
                AllocateIndexes(kSentinel, capacity / 2);
            }
            return true;
        }
        friend class QCDLListOp<Entry, 0>;
        friend class CSMap;
    private:
        Entry(const Entry& entry);
        Entry& operator=(const Entry& entry);
    };

    CSMap()
        : mMap(),
          mServers(),
          mPendingRemove(),
          mNullSlots(),
          mServerCount(0),
          mHibernatedCount(0),
          mRemoveServerScanPtr(0),
          mCachedEntry(0),
          mCachedChunkId(-1),
          mDebugValidateFlag(false)
    {
        for (int i = 0; i < Entry::kStateCount; i++) {
            mCounts[i]  = 0;
            mPrevPtr[i] = 0;
            mNextPtr[i] = 0;
            mLists[i].SetState(Entry::State(i));
            mNextEnd[i].SetState(Entry::State(i));
            EList::Insert(mLists[i+1], mLists[i]);
        }
        mMap.SetDeleteObserver(this);
        memset(mHibernatedIndexes, 0, sizeof(mHibernatedIndexes));
    }
    ~CSMap()
    {
        mMap.SetDeleteObserver(0);
    }
    bool SetDebugValidate(bool flag) {
        if (GetServerCount() > 0) {
            return (mDebugValidateFlag == flag);
        }
        mDebugValidateFlag = flag;
        return true;
    }
    bool Validate(const ChunkServerPtr& server) const {
        if (! server) {
            return false;
        }
        const int idx = server->GetIndex();
        return (idx >= 0 && idx < (int)mServers.size() &&
            mServers[idx] == server);
    }
    bool AddServer(const ChunkServerPtr& server) {
        if (! server || Validate(server)) {
            return false;
        }
        if (mServerCount + mPendingRemove.size() >=
                Entry::kMaxServers) {
            return false;
        }
        if (mNullSlots.empty()) {
            server->SetIndex(mServers.size(), mDebugValidateFlag);
            mServers.push_back(server);
        } else {
            Entry::AllocIdx const idx = mNullSlots.back();
            mNullSlots.pop_back();
            if (idx >= mServers.size() || mServers[idx]) {
                InternalError("invalid null slots");
                return false;
            }
            mServers[idx] = server;
            server->SetIndex(idx, mDebugValidateFlag);
        }
        server->ClearHosted();
        mServerCount++;
        Validate();
        return true;
    }
    bool RemoveServer(const ChunkServerPtr& server) {
        if (! server || ! Validate(server)) {
            return false;
        }
        Validate();
        mServers[server->GetIndex()].reset();
        mPendingRemove.push_back(server->GetIndex());
        server->SetIndex(-1, mDebugValidateFlag);
        mServerCount--;
        server->ClearHosted();
        // Start or restart full scan.
        RemoveServerScanFirst();
        return true;
    }
    bool SetHibernated(const ChunkServerPtr& server, size_t& idx) {
        if (! server || ! Validate(server) ||
                ! SetHibernated(server->GetIndex())) {
            return false;
        }
        mServers[server->GetIndex()].reset();
        idx = server->GetIndex();
        server->SetIndex(-1, mDebugValidateFlag);
        Validate();
        return true;
    }
    bool RemoveHibernatedServer(size_t idx) {
        if (/* idx < 0 ||*/ idx >= Entry::kMaxServers) {
            return false;
        }
        Validate();
        if (! ClearHibernated(idx)) {
            return false;
        }
        assert(! mServers[idx] && mServerCount > 0);
        mPendingRemove.push_back(idx);
        mServerCount--;
        // Start or restart full scan.
        RemoveServerScanFirst();
        return true;
    }
    size_t GetServerCount() const {
        return mServerCount;
    }
    size_t GetHibernatedCount() const {
        return mHibernatedCount;
    }
    size_t ServerCount(const Entry& entry) const {
        if (mRemoveServerScanPtr) {
            return CleanupStaleServers(entry);
        }
        ValidateHosted(entry);
        if (mHibernatedCount > 0) {
            size_t ret = 0;
            for (size_t i = 0, e = entry.ServerCount();
                    i < e;
                    i++) {
                if (IsHibernated(entry.IndexAt(i))) {
                    continue;
                }
                ret++;
            }
            return ret;
        }
        return entry.ServerCount();
    }
    bool HasServers(chunkId_t chunkId) const {
        const Entry* const entry = Find(chunkId);
        return (entry && HasServers(*entry));
    }
    bool HasServers(const Entry& entry) const {
        if (mRemoveServerScanPtr) {
            return (CleanupStaleServers(entry) > 0);
        }
        if (mHibernatedCount > 0) {
            return (ServerCount(entry) > 0);
        }
        ValidateHosted(entry);
        return entry.HasServers();
    }
    Servers GetServers(chunkId_t chunkId) const {
        const Entry* const entry = Find(chunkId);
        return (entry ? GetServers(*entry) : Servers());
    }
    Servers GetServers(const Entry& entry) const {
        Servers servers;
        GetServers(entry, servers);
        return servers;
    }
    Servers GetServers(const Entry& entry, size_t& hibernatedCount) const {
        Servers servers;
        GetServers(entry, servers, hibernatedCount);
        return servers;
    }
    size_t GetServers(const Entry& entry, Servers& servers) const {
        size_t hibernatedCount = 0;
        return GetServers(entry, servers, hibernatedCount);
    }
    size_t GetServers(const Entry& entry, Servers& servers,
            size_t& hibernatedCount) const {
        hibernatedCount = 0;
        if (mRemoveServerScanPtr) {
            return CleanupStaleServers(entry, &servers,
                hibernatedCount);
        }
        ValidateHosted(entry);
        size_t count = 0;
        for (size_t i = 0, e = entry.ServerCount(); i < e; i++) {
            const ChunkServerPtr& srv = mServers[entry.IndexAt(i)];
            if (srv) {
                servers.push_back(srv);
                count++;
            } else {
                if (mHibernatedCount <= 0) {
                    panic("invalid server index", false);
                }
                hibernatedCount++;
            }
        }
        return count;
    }
    ChunkServerPtr GetServer(const Entry& entry) const {
        if (mRemoveServerScanPtr) {
            Servers srvs;
            return (CleanupStaleServers(entry, &srvs) > 0 ?
                srvs[0] : ChunkServerPtr());
        }
        ValidateHosted(entry);
        if (mHibernatedCount > 0) {
            for (size_t i = 0, e = entry.ServerCount();
                    i < e;
                    i++) {
                const ChunkServerPtr& srv =
                    mServers[entry.IndexAt(i)];
                if (srv) {
                    return srv;
                }
            }
            return ChunkServerPtr();
        }
        return (entry.HasServers() ?
            mServers[entry.IndexAt(0)] : ChunkServerPtr());
    }
    void SetServers(const Servers& servers, Entry& entry) {
        ValidateHosted(entry);
        entry.RemoveAllServers(*this);
        for (Servers::const_iterator it = servers.begin();
                it != servers.end();
                ++it) {
            AddServer(*it, entry);
        }
    }
    bool AddServer(const ChunkServerPtr& server, Entry& entry) const {
        if (! Validate(server)) {
            return false;
        }
        if (mRemoveServerScanPtr) {
            CleanupStaleServers(entry);
        }
        ValidateHosted(entry);
        if (! entry.AddIndex(server->GetIndex())) {
            return false;
        }
        AddHosted(server, entry);
        ValidateServers(entry);
        return true;
    }
    bool RemoveServer(const ChunkServerPtr& server, Entry& entry) const {
        if (! Validate(server)) {
            return false;
        }
        ValidateHosted(entry);
        if (! entry.RemoveIndex(server->GetIndex())) {
            return false;
        }
        RemoveHosted(server, entry);
        ValidateHosted(entry);
        return true;
    }
    bool HasServer(const ChunkServerPtr& server, const Entry& entry) const {
        if (! Validate(server)) {
            return false;
        }
        if (mRemoveServerScanPtr) {
            CleanupStaleServers(entry);
        } else {
            ValidateHosted(entry);
        }
        return entry.HasIndex(server->GetIndex());
    }
    ChunkServerPtr GetServer(const ServerLocation& loc,
            const Entry& entry) const {
        ValidateHosted(entry);
        for (size_t i = 0, e = entry.ServerCount(); i < e; i++) {
            const ChunkServerPtr& srv = mServers[entry.IndexAt(i)];
            if (srv && loc == srv->GetServerLocation()) {
                return srv;
            }
        }
        return ChunkServerPtr();
    }
    bool HasServer(const ServerLocation& loc, const Entry& entry) const {
        return !!GetServer(loc, entry);
    }
    Entry::State GetState(const Entry& entry) const {
        Validate(entry);
        return entry.GetState();
    }
    bool SetState(chunkId_t chunkId, Entry::State state) {
        Entry* const entry = Find(chunkId);
        return (entry && SetState(*entry, state));
    }
    bool SetState(Entry& entry, Entry::State state) {
        if (! Validate(entry) || ! Validate(state)) {
            return false;
        }
        SetStateSelf(entry, state);
        if (mRemoveServerScanPtr) {
            // The entry can potentially be missed by the
            // lazy full scan due to its list position change.
            // Do cleanup here.
            CleanupStaleServers(entry);
        } else {
            ValidateHosted(entry);
        }
        return true;
    }
    Entry* Next(Entry& entry) const {
        Entry* ret = &EList::GetNext(entry);
        if (IsNextEnd(*ret)) {
            ret = &EList::GetNext(*ret);
        }
        return ((ret == &entry || IsHead(*ret)) ? 0 : ret);
    }
    const Entry* Next(const Entry& entry) const {
        const Entry* ret = &EList::GetNext(entry);
        if (IsNextEnd(*ret)) {
            ret = &EList::GetNext(*ret);
        }
        return ((ret == &entry || IsHead(*ret)) ? 0 : ret);
    }
    Entry* Prev(Entry& entry) const {
        Entry* ret = &EList::GetPrev(entry);
        if (IsNextEnd(*ret)) {
            ret = &EList::GetPrev(*ret);
        }
        return ((ret == &entry || IsHead(*ret)) ? 0 : ret);
    }
    const Entry* Prev(const Entry& entry) const {
        const Entry* ret = &EList::GetPrev(entry);
        if (IsNextEnd(*ret)) {
            ret = &EList::GetPrev(*ret);
        }
        return ((ret == &entry || IsHead(*ret)) ? 0 : ret);
    }
    Entry* Find(chunkId_t chunkId) {
        if (mCachedChunkId == chunkId && mCachedEntry) {
            return mCachedEntry;
        }
        Entry* const entry = mMap.Find(chunkId);
        if (entry) {
            mCachedEntry   = entry;
            mCachedChunkId = chunkId;
        }
        return entry;
    }
    const Entry* Find(chunkId_t chunkId) const {
        return const_cast<CSMap*>(this)->Find(chunkId);
    }
    size_t Erase(chunkId_t chunkId) {
        return mMap.Erase(chunkId);
    }
    Entry* Insert(MetaFattr* fattr, chunkOff_t offset, chunkId_t chunkId,
            seq_t chunkVersion, bool& newEntryFlag) {
        newEntryFlag = false;
        Entry* const entry = mMap.Insert(
            chunkId,
            Entry(fattr, offset, chunkId, chunkVersion),
            newEntryFlag);
        if (newEntryFlag) {
            const Entry::State state = entry->GetState();
            mCounts[state]++;
            assert(mCounts[state] > 0);
            EList::Insert(*entry,
                EList::GetPrev(mLists[state + 1]));
        } else if (entry) {
            entry->offset       = offset;
            entry->chunkVersion = chunkVersion;
            entry->SetFattr(fattr);
        }
        if (entry) {
            mCachedEntry   = entry;
            mCachedChunkId = chunkId;
        }
        return entry;
    }
    void First() {
        mMap.First();
    }
    const Entry* Next() {
        return mMap.Next();
    }
    size_t Size() const {
        return mMap.GetSize();
    }
    void Clear() {
        mMap.Clear();
        RemoveServerCleanup(0);
    }
    bool CanAddServer(const ChunkServerPtr& server) const {
        return (mServerCount + mPendingRemove.size() <
                Entry::kMaxServers &&
            server && ! Validate(server)
        );
    }
    bool Validate(const Entry& entry) const {
        const char* const reason = ValidateSelf(entry);
        if (reason) {
            InternalError(reason);
        }
        return (! reason);
    }
    bool Validate(Entry::State state) const {
        if (state < 0 || state >= Entry::kStateCount) {
            InternalError("invalid state");
            return false;
        }
        return true;
    }
    void First(Entry::State state) {
        if (Validate(state)) {
            mNextPtr[state] = Next(mLists[state]);
            // Insert or move iteration delimiter at the present
            // list end.
            // Set state inserts items before mLists[state + 1],
            // this prevents iterating over newly inserted entries,
            // and the endless loops with the reordering withing the
            // same list.
            EList::Insert(mNextEnd[state],
                EList::GetPrev(mLists[state + 1]));
        }
    }
    Entry* Next(Entry::State state) {
        if (! Validate(state)) {
            return 0;
        }
        Entry* const ret = mNextPtr[state];
        if (ret) {
            SetNextPtr(mNextPtr[state]);
        }
        return ret;
    }
    void Last(Entry::State state) {
        if (Validate(state)) {
            mPrevPtr[state] = Prev(mLists[state + 1]);
        }
    }
    Entry* Prev(Entry::State state) {
        if (! Validate(state)) {
            return 0;
        }
        Entry* const ret = mPrevPtr[state];
        if (ret) {
            mPrevPtr[state] = Prev(*ret);
        }
        return ret;
    }
    Entry* Front(Entry::State state) {
        if (! Validate(state)) {
            return 0;
        }
        return Next(mLists[state]);
    }
    const Entry* Front(Entry::State state) const {
        if (! Validate(state)) {
            return 0;
        }
        return Next(mLists[state]);
    }
    bool RemoveServerCleanup(size_t maxScanCount) {
        RemoveServerScanCur();
        for (size_t i = 0;
                mRemoveServerScanPtr &&
                    (i < maxScanCount || maxScanCount <= 0);
                i++) {
            Entry& entry = *mRemoveServerScanPtr;
            mRemoveServerScanPtr = &EList::GetPrev(entry);
            CleanupStaleServers(entry);
            RemoveServerScanCur();
        }
        return (mRemoveServerScanPtr != 0);
    }
    size_t GetCount(Entry::State state) const {
        return (Validate(state) ? mCounts[state] : size_t(0));
    }
private:
    struct KeyVal : public Entry
    {
        typedef chunkId_t Key;
        typedef Entry     Val;

        KeyVal(const Key& key, const Val& val)
            : Entry(key, val)
            {}
        KeyVal(const KeyVal& kv)
            : Entry(kv.GetKey(), kv.GetVal())
            {}
        const Key& GetKey() const { return GetChunkId(); }
        const Val& GetVal() const { return *this; }
        Val& GetVal()             { return *this; }
    private:
        KeyVal& operator=(const KeyVal&);
    };
public:
    // Public only to avoid declaring LinearHash as a friend.
    void operator()(KeyVal& keyVal) {
        Erasing(keyVal.GetVal());
    }
private:
    template<typename T>
    class Allocator
    {
    public:
        T* allocate(size_t n) {
            if (n != 1) {
                panic("alloc n != 1 not implemented", false);
                return 0;
            }
            return reinterpret_cast<T*>(GetAllocator().Allocate());
        }
        void deallocate(T* ptr, size_t n) {
            if (n != 1) {
                panic("dealloc n != 1 not implemented", false);
                return;
            }
            GetAllocator().Deallocate(ptr);
        }
        static void construct(T* ptr, const T& other) {
            new (ptr) T(other);
        }
        static void destroy(T* ptr) {
            ptr->~T();
        }
        template <typename TOther>
        struct rebind {
            typedef Allocator<TOther> other;
        };
        typedef PoolAllocator<
            sizeof(T),         // size_t TItemSize,
            size_t(8)   << 20, // size_t TMinStorageAlloc,
            size_t(128) << 20, // size_t TMaxStorageAlloc,
            false              // bool   TForceCleanupFlag
        > Alloc;
        const Alloc& GetAllocator() const {
            return alloc;
        }
    private:
        Alloc alloc;
        Alloc& GetAllocator() {
            return alloc;
        }
    };
    typedef LinearHash<
        KeyVal,
        KeyCompare<chunkId_t>,
        DynamicArray<
            SingleLinkedList<KeyVal>*,
            24 // 2^24 * sizeof(void*) => 128 MB
        >,
        Allocator<KeyVal>,
        CSMap
    > Map;
public:
    typedef Map::Allocator::Alloc PAllocator;
    const PAllocator& GetAllocator() const {
        return mMap.GetAllocator().GetAllocator();
    }
private:
    typedef vector<Entry::AllocIdx> SlotIndexes;
    typedef uint8_t                 HibernatedBits;
    enum
    {
        kHibernatedBitShift = 3,
        kHibernatedBitMask  = (1 << kHibernatedBitShift) - 1
    };

    Map            mMap;
    Servers        mServers;
    SlotIndexes    mPendingRemove;
    SlotIndexes    mNullSlots;
    size_t         mServerCount;
    size_t         mHibernatedCount;
    Entry*         mRemoveServerScanPtr;
    Entry*         mCachedEntry;
    chunkId_t      mCachedChunkId;
    bool           mDebugValidateFlag;
    Entry*         mPrevPtr[Entry::kStateCount];
    Entry*         mNextPtr[Entry::kStateCount];
    size_t         mCounts[Entry::kStateCount];
    Entry          mLists[Entry::kStateCount + 1];
    Entry          mNextEnd[Entry::kStateCount];
    HibernatedBits mHibernatedIndexes[
        (Entry::kMaxServers + kHibernatedBitMask) /
        (1 << kHibernatedBitShift)];

    void Erasing(Entry& entry) {
        if (EList::IsInList(entry)) {
            const Entry::State state = entry.GetState();
            assert(mCounts[state] > 0);
            mCounts[state]--;
            if (&entry == mNextPtr[state]) {
                SetNextPtr(mNextPtr[state]);
            }
            if (&entry == mPrevPtr[state]) {
                mPrevPtr[state] = Prev(entry);
            }
            if (&entry == mRemoveServerScanPtr) {
                // Do not call RemoveServerScanNext()
                // Validate() will fail since the size of the
                // list won't match the size of hash table, as
                // the entry has already been removed.
                mRemoveServerScanPtr = &EList::GetPrev(entry);
            }
            RemoveHosted(entry);
            if (mCachedEntry == &entry) {
                mCachedEntry   = 0;
                mCachedChunkId = -1;
            }
            // Only update counters here.
            // Entry dtor removes it from the list.
        }
    }
    bool IsHead(const Entry& entry) const {
        return (&entry == &mLists[entry.GetState()] ||
            &entry == &mLists[Entry::kStateCount]);
    }
    bool IsNextEnd(const Entry& entry) const {
        return (&entry == &mNextEnd[entry.GetState()]);
    }
    void SetNextPtr(Entry*& next) {
        next = &EList::GetNext(*next);
        if (IsHead(*next) || IsNextEnd(*next)) {
            next = 0;
        }
    }
    const char* ValidateSelf(const Entry& entry) const {
        if (! EList::IsInList(entry)) {
            return "not in list";
        }
        const int state = entry.GetState();
        if (state < 0 || state >= Entry::kStateCount) {
            return "invalid state";
        }
        if (IsHead(entry)) {
            return "list head";
        }
        if (IsNextEnd(entry)) {
            return "next end";
        }
        return 0;
    }
    bool ValidateServers(const Entry& entry,
            bool ignoreScanFlag = false) const {
        if (! mDebugValidateFlag) {
            return true;
        }
        for (size_t i = 0, e = entry.ServerCount(); i < e; i++) {
            const size_t          idx = entry.IndexAt(i);
            const ChunkServerPtr& srv = mServers[idx];
            if (srv) {
                const int* const cur = srv->HostedIdx(
                    entry.GetChunkId());
                if (! cur || *cur != (int)idx) {
                    InternalError("hosted mismatch");
                    return false;
                }
            } else {
                if (idx >= mServers.size()) {
                    InternalError("invalid index");
                    return false;
                }
                if ((ignoreScanFlag ||
                        ! mRemoveServerScanPtr) &&
                        ! IsHibernated(idx)) {
                    InternalError("no server");
                    return false;
                }
            }
        }
        return true;
    }
    bool ValidateServersNoScan(const Entry& entry) {
        return ValidateServers(entry, true);
    }
    void RemoveHosted(Entry& entry) const {
        for (size_t i = 0, e = entry.ServerCount(); i < e; i++) {
            const size_t          idx = entry.IndexAt(i);
            const ChunkServerPtr& srv = mServers[idx];
            if (srv) {
                if (mDebugValidateFlag) {
                    srv->RemoveHosted(
                        entry.GetChunkId(), idx);
                } else {
                    srv->RemoveHosted();
                }
            }
        }
    }
    void AddHosted(const ChunkServerPtr& server, const Entry& entry) const {
        if (mDebugValidateFlag) {
            server->AddHosted(
                entry.GetChunkId(), server->GetIndex());
        } else {
            server->AddHosted();
        }
    }
    void RemoveHosted(const ChunkServerPtr& server,
            const Entry& entry) const {
        if (mDebugValidateFlag) {
            server->RemoveHosted(
                entry.GetChunkId(), server->GetIndex());
        } else {
            server->RemoveHosted();
        }
    }
    bool Validate() const {
        if (! mDebugValidateFlag) {
            return true;
        }
        size_t cnt = 0;
        for (const Entry* entry = &mLists[Entry::kStateNone]; ; ) {
            entry = &EList::GetNext(*entry);
            if (entry == &mLists[Entry::kStateNone]) {
                break;
            }
            if (! IsNextEnd(*entry) && ! IsHead(*entry)) {
                cnt++;
                if (! ValidateServers(*entry)) {
                    return false;
                }
            }
        }
        if (cnt != mMap.GetSize()) {
            InternalError("invalid entry count");
            return false;
        }
        return true;
    }
    bool ValidateHosted(const Entry& entry) const {
        return (! mDebugValidateFlag ||
            (Validate(entry) && ValidateServers(entry)));
    }
    size_t CleanupStaleServers(const Entry& entry,
            Servers* servers = 0) const {
        size_t hibernatedCount = 0;
        return CleanupStaleServers(entry, servers, hibernatedCount);
    }
    size_t CleanupStaleServers(const Entry& entry,
            Servers* servers, size_t& hibernatedCount) const {
        return const_cast<CSMap*>(this)->CleanupStaleServers(
            const_cast<Entry&>(entry), servers, hibernatedCount);
    }
    size_t CleanupStaleServers(Entry& entry, Servers* servers = 0) {
        size_t hibernatedCount = 0;
        return CleanupStaleServers(entry, servers, hibernatedCount);
    }
    size_t CleanupStaleServers(Entry& entry, Servers* servers,
            size_t& hibernatedCount) {
        ValidateHosted(entry);
        size_t       cnt  = entry.ServerCount();
        const size_t prev = cnt;
        size_t       ret  = 0;
        for (size_t i = 0; i < cnt; ) {
            const size_t          idx    = entry.IndexAt(i);
            const ChunkServerPtr& server = mServers[idx];
            if (server) {
                if (servers) {
                    servers->push_back(server);
                }
                ret++;
            } else if (IsHibernated(idx)) {
                hibernatedCount++;
            } else {
                entry.RemoveIndex(idx);
                cnt--;
                continue;
            }
            i++;
        }
        ValidateServersNoScan(entry);
        // Enqueue replication check if servers were removed.
        if (prev != cnt && entry.GetState() == Entry::kStateNone) {
            SetStateSelf(entry, Entry::kStateCheckReplication);
        }
        return ret;
    }
    void RemoveServerScanFirst() {
        // Scan backwards to avoid scanning the newly added entries,
        // or entries that have been moved.
        mRemoveServerScanPtr = &mLists[Entry::kStateCount];
        RemoveServerScanNext();
    }
    void RemoveServerScanNext() {
        for (; ;) {
            if (&mLists[Entry::kStateNone] ==
                    mRemoveServerScanPtr) {
                mRemoveServerScanPtr = 0;
                Validate();
                if (mNullSlots.empty()) {
                    mNullSlots.swap(mPendingRemove);
                } else {
                    mNullSlots.insert(
                        mNullSlots.end(),
                        mPendingRemove.begin(),
                        mPendingRemove.end());
                    mPendingRemove.clear();
                }
                return;
            }
            mRemoveServerScanPtr = &EList::GetPrev(
                *mRemoveServerScanPtr);
            if (! IsHead(*mRemoveServerScanPtr) &&
                    ! IsNextEnd(*mRemoveServerScanPtr)) {
                break;
            }
        }
    }
    void RemoveServerScanCur() {
        if (mRemoveServerScanPtr &&
                (IsHead(*mRemoveServerScanPtr) ||
                IsNextEnd(*mRemoveServerScanPtr))) {
            RemoveServerScanNext();
        }
    }
    void SetStateSelf(Entry& entry, Entry::State state) {
        const Entry::State prev = entry.GetState();
        assert(mCounts[prev] > 0);
        mCounts[prev]--;
        if (&entry == mNextPtr[prev]) {
            SetNextPtr(mNextPtr[prev]);
        }
        if (&entry == mPrevPtr[prev]) {
            mPrevPtr[prev] = Prev(entry);
        }
        if (&entry == mRemoveServerScanPtr) {
            // Do not call RemoveServerScanNext(), SetState()
            // cleans the entry only if mRemoveServerScanPtr != 0
            mRemoveServerScanPtr = &EList::GetPrev(entry);
        }
        entry.SetState(state);
        mCounts[state]++;
        assert(mCounts[state] > 0);
        EList::Insert(entry, EList::GetPrev(mLists[state + 1]));
    }
    bool IsHibernated(size_t idx) const {
        return (mHibernatedIndexes[idx >> kHibernatedBitShift] &
            (HibernatedBits(1) << (idx & kHibernatedBitMask))) != 0;
    }
    bool SetHibernated(size_t idx) {
        HibernatedBits&      bits = mHibernatedIndexes[
            idx >> kHibernatedBitShift];
        const HibernatedBits bit  = HibernatedBits(1) <<
            (idx & kHibernatedBitMask);
        if ((bits & bit) != 0) {
            return false;
        }
        bits |= bit;
        mHibernatedCount++;
        assert(mHibernatedCount > 0);
        return true;
    }
    bool ClearHibernated(size_t idx) {
        HibernatedBits&      bits = mHibernatedIndexes[
            idx >> kHibernatedBitShift];
        const HibernatedBits bit  = HibernatedBits(1) <<
            (idx & kHibernatedBitMask);
        if ((bits & bit) == 0) {
            return false;
        }
        bits &= ~bit;
        assert(mHibernatedCount > 0);
        mHibernatedCount--;
        return true;
    }
    static void InternalError(const char* errMsg) {
        panic(errMsg ? errMsg : "internal error", false);
    }
private:
    CSMap(const CSMap&);
    CSMap& operator=(const CSMap&);
};

} // namespace KFS

#endif /* CS_MAP_H */
