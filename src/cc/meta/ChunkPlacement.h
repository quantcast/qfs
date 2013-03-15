//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/02/06
// Author: Mike Ovsiannikov
//
// Copyright 2012 Quantcast Corp.
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
// \file ChunkPlacement.h
//
//----------------------------------------------------------------------------

#ifndef CHUNK_PLACEMENT_H
#define CHUNK_PLACEMENT_H

#include "common/StdAllocator.h"
#include "common/MsgLogger.h"

#include <vector>
#include <set>
#include <deque>
#include <sstream>
#include <algorithm>
#include <cstddef>

#include <inttypes.h>

#include <boost/bind.hpp>

namespace KFS
{
using std::vector;
using std::pair;
using std::make_pair;
using std::find_if;
using std::iter_swap;
using std::sort;
using boost::bind;

/*
 * Rack aware chunk placement.
 *
 * The long term placement goal is to keep at most 1 replica of a chunk per
 * rack, or all chunks from Reed-Solomon recovery block with replication 1 on
 * different racks.
 *
 * The likelihood of a particular rack to be selected for placement is
 * proportional to the number of chunk servers that can be used in each rack
 * multiplied by the rack weight. The rack weight can be configured, otherwise
 * it assumed to be 1.0.
 *
 * The likelihood of a particular server to be chosen from a rack is
 * proportional to the to the reciprocal of the "write load" or available space.
 * For more details look at GetLoad() and LayoutManager::IsCandidateServer().
 *
 * The available space is used for space re-balancing, and write load is used
 * for the initial chunk placement, unless using available space is forced by
 * the meta server configuration for initial chunk placement.
 *
 * To minimize network transfers between the rack the re-replication and
 * re-balancing attempts to choose re-replication source and destination withing
 * the same rack. If not enough different racks available, put chunk replicas
 * into the rack that has the least chunk replicas or lest chunks for a given rs
 * block. When more racks become available later, the re-balancing will move
 * the replicas accordingly.
 *
 */

template<typename LayoutManager>
class ChunkPlacement
{
public:
    typedef typename LayoutManager::Servers       Servers;
    typedef typename Servers::value_type          ChunkServerPtr;
    typedef typename ChunkServerPtr::element_type ChunkServer;
    typedef typename LayoutManager::RackInfos     RackInfos;
    typedef typename RackInfos::value_type        RackInfo;
    typedef typename RackInfo::RackId             RackId;
    typedef          std::size_t                  size_t;

    ChunkPlacement(
        LayoutManager& layoutManager)
        : mLayoutManager(layoutManager),
          mRacks(mLayoutManager.GetRacks()),
          mRackExcludes(),
          mServerExcludes(),
          mCandidateRacks(),
          mCandidates(),
          mLoadAvgSum(0),
          mRackPos(0),
          mCandidatePos(0),
          mCurRackId(-1),
          mCandidatesInRacksCount(0),
          mMaxReplicationsPerNode(0),
          mMaxSpaceUtilizationThreshold(0),
          mCurSTierMaxSpaceUtilizationThreshold(0),
          mForReplicationFlag(false),
          mUsingRackExcludesFlag(false),
          mUsingServerExcludesFlag(false),
          mSortBySpaceUtilizationFlag(false),
          mSortCandidatesByLoadAvgFlag(false),
          mLastAttemptFlag(false),
          mMinSTier(kKfsSTierMax),
          mMaxSTier(kKfsSTierMax),
          mCurSTier(mMinSTier)
        {}
    void Reset()
    {
        mLoadAvgSum              = 0;
        mRackPos                 = 0;
        mCandidatePos            = 0;
        mCurRackId               = -1;
        mCurSTier                = mMinSTier;
        mUsingRackExcludesFlag   = false;
        mUsingServerExcludesFlag = false;
        mLastAttemptFlag         = false;
        mCandidateRacks.clear();
        mCandidates.clear();
    }
    void clear()
    {
        Reset();
        mRackExcludes.Clear();
        mServerExcludes.Clear();
    }
    void FindCandidates(
        kfsSTier_t minSTier,
        kfsSTier_t maxSTier,
        bool       forReplicationFlag = false,
        RackId     rackIdToUse        = -1)
    {
        assert(
            kKfsSTierMin <= minSTier && minSTier <= kKfsSTierMax &&
            kKfsSTierMin <= maxSTier && maxSTier <= kKfsSTierMax &&
            minSTier <= maxSTier
        );
        Reset();
        mMinSTier                     = minSTier;
        mMaxSTier                     = maxSTier;
        mCurSTier                     = mMinSTier;
        mForReplicationFlag           = forReplicationFlag;
        mSortBySpaceUtilizationFlag   =
            mLayoutManager.GetSortCandidatesBySpaceUtilizationFlag();
        mSortCandidatesByLoadAvgFlag  =
            mLayoutManager.GetSortCandidatesByLoadAvgFlag();
        mMaxSpaceUtilizationThreshold =
            mLayoutManager.GetMaxSpaceUtilizationThreshold();
        mMaxReplicationsPerNode       =
            mLayoutManager.GetMaxConcurrentWriteReplicationsPerNode();
        FindCandidatesSelf(rackIdToUse);
    }
    void FindCandidatesInRack(
        kfsSTier_t minSTier,
        kfsSTier_t maxSTier,
        RackId     rackIdToUse)
        { FindCandidates(minSTier, maxSTier, false, rackIdToUse); }

    void FindCandidatesForReplication(
        kfsSTier_t minSTier,
        kfsSTier_t maxSTier)
        { FindCandidates(minSTier, maxSTier, true); }

    void FindRebalanceCandidates(
        kfsSTier_t minSTier,
        kfsSTier_t maxSTier,
        double     maxUtilization,
        RackId     rackIdToUse = -1)
    {
        assert(
            kKfsSTierMin <= minSTier && minSTier <= kKfsSTierMax &&
            kKfsSTierMin <= maxSTier && maxSTier <= kKfsSTierMax &&
            minSTier <= maxSTier
        );
        Reset();
        mMinSTier                     = minSTier;
        mMaxSTier                     = maxSTier;
        mCurSTier                     = mMinSTier;
        mForReplicationFlag           = true;
        mSortBySpaceUtilizationFlag   = true;
        mSortCandidatesByLoadAvgFlag  = false;
        mMaxSpaceUtilizationThreshold = min(maxUtilization,
            mLayoutManager.GetMaxSpaceUtilizationThreshold());
        mMaxReplicationsPerNode       =
            mLayoutManager.GetMaxConcurrentWriteReplicationsPerNode();
        FindCandidatesSelf(rackIdToUse);
    }

    size_t GetCandidateRackCount() const
        { return mCandidateRacks.size(); }

    bool HasCandidateRacks()
    {
        bool anyAvailableFlag = false;
        FindCandidateRacks(&anyAvailableFlag);
        return anyAvailableFlag;
    }

    bool SearchCandidateRacks()
    {
        Reset();
        bool anyAvailableFlag = false;
        FindCandidateRacks(&anyAvailableFlag);
        return anyAvailableFlag;
    }

    ChunkServerPtr GetNext(
        bool canIgnoreServerExcludesFlag)
    {
        if (mUsingServerExcludesFlag) {
            if (! canIgnoreServerExcludesFlag) {
                return ChunkServerPtr();
            }
            while (mCandidatePos < mServerExcludes.Size()) {
                ChunkServer& srv = *(mServerExcludes.Get(mCandidatePos++));
                if (IsCandidateServer(srv)) {
                    return srv.shared_from_this();
                }
            }
            if (mMaxSTier <= mCurSTier) {
                return ChunkServerPtr();
            }
            NextTier();
            mCandidatePos = 0;
            return GetNext(canIgnoreServerExcludesFlag); // Tail recursion.
        }
        if (mCandidatePos <= 0) {
            if (! canIgnoreServerExcludesFlag ||
                    ! mUsingRackExcludesFlag ||
                    mRackPos <= mRackExcludes.Size()) {
                return ChunkServerPtr();
            }
            mServerExcludes.SortByCount();
            mCandidatePos            = 0;
            mUsingServerExcludesFlag = true;
            // Tail recursion.
            return GetNext(canIgnoreServerExcludesFlag);
        }
        // Random shuffle chosen servers, such that the servers with
        // smaller "load" go before the servers with larger load.
        if (mCandidatePos == 1) {
            return mCandidates[--mCandidatePos].second->shared_from_this();
        }
        assert(mLoadAvgSum > 0);
        int64_t rnd = Rand(mLoadAvgSum);
        size_t  ri  = mCandidatePos--;
        int64_t load;
        do {
            --ri;
            load = mCandidates[ri].first;
            rnd -= load;
        } while (rnd >= 0 && ri > 0);
        iter_swap(mCandidates.begin() + mCandidatePos,
            mCandidates.begin() + ri);
        mLoadAvgSum -= load;
        return mCandidates[mCandidatePos].second->shared_from_this();
    }

    bool IsUsingServerExcludes() const
        { return mUsingServerExcludesFlag; }

    bool IsUsingRackExcludes() const
        { return mUsingRackExcludesFlag; }

    bool NextRack()
    {
        for ( ; ; ) {
            if (mUsingRackExcludesFlag) {
                mCurRackId = -1;
                const size_t size = mRackExcludes.Size();
                if (size < mRackPos) {
                    return false;
                }
                if (size == mRackPos) {
                    if (mLastAttemptFlag) {
                        if (mMaxSTier <= mCurSTier) {
                            mRackPos++;
                            mCurSTier = mMinSTier;
                            return false;
                        } else {
                            NextTier();
                        }
                    } else {
                        if (mCurSTier < mMaxSTier && mRackPos > 0) {
                            NextTier();
                            mRackPos = 0;
                        } else {
                            mLastAttemptFlag = true;
                            mCurSTier        = mMinSTier;
                        }
                    }
                    if (mLastAttemptFlag) {
                        // The last attempt -- put it somewhere.
                        FindCandidateServers(
                            mLayoutManager.GetChunkServers(),
                            mLayoutManager.GetTierCandidatesCount(mCurSTier)
                        );
                        if (mCandidates.empty()) {
                            continue;
                        }
                        break;
                    }
                }
                const RackId rackId = mRackExcludes.Get(mRackPos++);
                typename RackInfos::const_iterator const it =
                    LayoutManager::FindRackT(
                        mRacks.begin(), mRacks.end(), rackId);
                if (it == mRacks.end()) {
                    KFS_LOG_STREAM_ERROR <<
                        " invalid rack id: " << rackId <<
                    KFS_LOG_EOM;
                    continue;
                }
                FindCandidateServers(*it);
                if (mCandidates.empty()) {
                    continue;
                }
                mCurRackId = rackId;
                break;
            }
            if (mRackPos >= mCandidateRacks.size()) {
                if (mRackPos == 0 || mMaxSTier <= mCurSTier) {
                    // Use rack excludes.
                    // Put the racks with least chunk replicas
                    // first, to put the same number of replicas on
                    // each rack.
                    mRackExcludes.SortByCount();
                    mCurSTier              = mMinSTier;
                    mRackPos               = 0;
                    mUsingRackExcludesFlag = true;
                    continue;
                }
                mRackPos = 0;
                // Try to use the next tier. Satisfying rack placement
                // constraints has higher priority then satisfying storage
                // tier constrain.
                NextTier();
            }
            // Random shuffle chosen racks, such that the racks with
            // larger number of possible allocation candidates have
            // higher probability to go before the racks with lesser
            // number of candidates.
            if (mCurSTier == mMinSTier &&
                    mRackPos + 1 < mCandidateRacks.size()) {
                assert(mCandidatesInRacksCount > 0);
                int64_t rnd = Rand(mCandidatesInRacksCount);
                size_t  ri  = mRackPos;
                for (; ;) {
                    const int64_t sz = mCandidateRacks[ri].first;
                    if ((rnd -= sz) < 0) {
                        mCandidatesInRacksCount -= sz;
                        break;
                    }
                    ri++;
                }
                iter_swap(mCandidateRacks.begin() + mRackPos,
                    mCandidateRacks.begin() + ri);
            }
            const RackInfo& rack = *(mCandidateRacks[mRackPos++].second);
            if (mCurSTier != mMinSTier && mRackExcludes.Find(rack.id())) {
                // Skip rack if it is now in rack excludes.
                FindCandidateServers(rack.getServers(), 0);
            } else {
                FindCandidateServers(rack);
            }
            if (! mCandidates.empty()) {
                mCurRackId = rack.id();
                break;
            }
        }
        return (! mCandidates.empty());
    }

    bool ExcludeServer(
        ChunkServer& srv)
    {
        return mServerExcludes.Insert(&srv);
    }

    bool ExcludeServer(
        const ChunkServerPtr& srv)
    {
        return ExcludeServer(*srv);
    }

    template<typename IT>
    bool ExcludeServer(
        IT start,
        IT end)
    {
        bool ret = false;
        while (start != end) {
            ret = ExcludeServer(*start++) || ret;
        }
        return ret;
    }

    bool ExcludeServer(
        const Servers& servers)
    {
        return ExcludeServer(servers.begin(), servers.end());
    }

    bool ExcludeRack(
        RackId rackId)
    {
        return mRackExcludes.Insert(rackId);
    }

    bool ExcludeRack(
        ChunkServer& srv,
        chunkId_t    chunkId = -1)
    {
        const RackId rackId = srv.GetRack();
        if (chunkId >= 0 && srv.IsEvacuationScheduled(chunkId)) {
            return (! mRackExcludes.Find(rackId));
        }
        return ExcludeRack(rackId);
    }

    bool ExcludeRack(
        const ChunkServerPtr& srv,
        chunkId_t             chunkId = -1)
    {
        return ExcludeRack(*srv, chunkId);
    }

    template<typename IT>
    bool ExcludeRack(
        IT        start,
        IT        end,
        chunkId_t chunkId = -1)
    {
        bool ret = false;
        while (start != end) {
            ret = ExcludeRack(*start++, chunkId) || ret;
        }
        return ret;
    }

    bool ExcludeRack(
        const Servers& servers,
        chunkId_t      chunkId = -1)
    {
        return ExcludeRack(servers.begin(), servers.end(), chunkId);
    }

    bool ExcludeServerAndRack(
        ChunkServer& srv,
        chunkId_t    chunkId = -1)
    {
        mServerExcludes.Insert(&srv);
        return ExcludeRack(srv, chunkId);
    }

    bool ExcludeServerAndRack(
        const ChunkServerPtr& srv,
        chunkId_t             chunkId = -1)
    {
        return ExcludeServerAndRack(*srv, chunkId);
    }

    template<typename IT>
    bool ExcludeServerAndRack(
        IT        start,
        IT        end,
        chunkId_t chunkId = -1)
    {
        bool ret = false;
        while (start != end) {
            ret = ExcludeServerAndRack(*start++, chunkId) || ret;
        }
        return ret;
    }

    bool ExcludeServerAndRack(
        const Servers& servers,
        chunkId_t      chunkId = -1)
    {
        return ExcludeServerAndRack(
            servers.begin(), servers.end(), chunkId);
    }

    bool IsServerExcluded(
        const ChunkServer& srv) const
    {
        return mServerExcludes.Find(&srv);
    }

    bool IsServerExcluded(
        const ChunkServerPtr& srv) const
    {
        return IsServerExcluded(*srv);
    }

    bool IsRackExcluded(
        const ChunkServer& srv) const
    {
        return mRackExcludes.Find(srv.GetRack());
    }

    bool IsRackExcluded(
        const ChunkServerPtr& srv) const
    {
        return IsRackExcluded(*srv);
    }

    bool IsExcluded(
        const ChunkServer& srv) const
    {
        return (
            IsRackExcluded(srv) ||
            IsServerExcluded(srv)
        );
    }

    bool IsExcluded(
        const ChunkServerPtr& srv) const
    {
        return IsExcluded(*srv);
    }

    size_t GetExcludedServersCount() const
        { return mServerExcludes.Size(); }

    size_t GetExcludedRacksCount() const
        { return mRackExcludes.Size(); }

    size_t GetTotalExcludedServersCount() const
        { return mServerExcludes.GetTotal(); }

    size_t GetTotalExcludedRacksCount() const
        { return mRackExcludes.GetTotal(); }

    size_t GetExcludedServersMaxCount() const
        { return mServerExcludes.GetMaxCount(); }

    size_t GetExcludedRacksMaxCount() const
        { return mRackExcludes.GetMaxCount(); }

    RackId GetRackId() const
        { return mCurRackId; }

    kfsSTier_t GetStorageTier() const
        { return mCurSTier; }

    bool IsLastAttempt() const
    {
        return (
            mLastAttemptFlag &&
            mRackExcludes.Size() < mRackPos
        );
    }

    void Reserve(
        size_t count)
    {
        mServerExcludes.Reserve(count);
        mRackExcludes.Reserve(count);
    }

    RackId GetMostUsedRackId() const
        { return mRackExcludes.GetMaxId(); }

private:
    template <typename IdT, typename CountT, typename IdConstT = IdT>
    class IdSet
    {
    public:
        IdSet()
            : mIds(),
              mMaxId(),
              mTotal(0),
              mMaxCount(0)
            {}
        bool Find(
            const IdConstT& id) const
        {
            for (typename Ids::const_iterator it = mIds.begin();
                    it != mIds.end();
                    ++it) {
                if (it->first == id) {
                    return true;
                }
            }
            return false;
        }
        bool Insert(
            const IdT& id)
        {
            mTotal++;
            for (typename Ids::iterator it = mIds.begin();
                    it != mIds.end();
                    ++it) {
                if (it->first == id) {
                    if (++(it->second) > mMaxCount) {
                        mMaxCount = it->second;
                        mMaxId    = id;
                    }
                    return false;
                }
            }
            if (mMaxCount <= 0) {
                mMaxCount = size_t(1);
                mMaxId    = id;
            }
            mIds.push_back(make_pair(id, CountT(1)));
            return true;
        }
        void SortByCount()
        {
            sort(mIds.begin(), mIds.end(),
                bind(&Ids::value_type::second, _1) <
                bind(&Ids::value_type::second, _2)
            );
        }
        size_t GetMaxCount() const
            { return mMaxCount; }
        const IdT& Get(size_t pos) const
            { return mIds[pos].first; }
        size_t Size() const
            { return mIds.size(); }
        bool IsEmpty() const
            { return mIds.empty(); }
        void Clear()
        {
            mIds.clear();
            mTotal    = 0;
            mMaxCount = 0;
            mMaxId    = IdT();
        }
        size_t GetTotal() const
            { return mTotal; }
        void Reserve(
            size_t count)
            { mIds.reserve(count); }
        const IdT& GetMaxId() const
            { return mMaxId; }
    private:
        typedef vector<
            pair<IdT, CountT>,
            StdFastAllocator<pair<IdT, CountT> >
        > Ids;
        Ids    mIds;
        IdT    mMaxId;
        size_t mTotal;
        size_t mMaxCount;
    private:
        IdSet(const IdSet&);
        IdSet& operator=(const IdSet&);
    };
    class RackIdSet
    {
    public:
        enum { kMaxId = (RackId(1) << 16) - 1 };

        bool Find(
            RackId id) const
        {
            if (id < 0 || id > kMaxId) {
                return false;
            }
            return mIds.Find((int16_t)id);
        }
        bool Insert(
            RackId id)
        {
            if (id < 0 || id > kMaxId) {
                return false;
            }
            return mIds.Insert((int16_t)id);
        }
        void SortByCount()
            { mIds.SortByCount(); }
        size_t GetMaxCount() const
            { return mIds.GetMaxCount(); }
        RackId Get(size_t pos) const
            { return RackId(mIds.Get(pos)); }
        size_t Size() const
            { return mIds.Size(); }
        bool IsEmpty() const
            { return mIds.IsEmpty(); }
        void Clear()
            { mIds.Clear(); }
        void Reserve(
            size_t count)
            { mIds.Reserve(count); }
        RackId GetMaxId() const
        {
            return (mIds.GetMaxCount() <= 0 ?
                RackId(-1) : mIds.GetMaxId());
        }
    private:
        typedef IdSet<uint16_t, uint16_t> RackIds;
        RackIds mIds;
    };
    typedef IdSet<ChunkServer*, size_t, const ChunkServer*> ServerExcludes;
    typedef vector<
            pair<int64_t, const RackInfo*>,
            StdAllocator<pair<int64_t, const RackInfo*> >
        > CandidateRacks;
    typedef vector<
            pair<int64_t, ChunkServer*>,
            StdAllocator<pair<int64_t, ChunkServer*> >
        > Candidates;
    typedef Servers Sources;
    enum { kSlaveScaleFracBits = LayoutManager::kSlaveScaleFracBits };

    LayoutManager&   mLayoutManager;
    const RackInfos& mRacks;
    RackIdSet        mRackExcludes;
    ServerExcludes   mServerExcludes;
    CandidateRacks   mCandidateRacks;
    Candidates       mCandidates;
    int64_t          mLoadAvgSum;
    size_t           mRackPos;
    size_t           mCandidatePos;
    RackId           mCurRackId;
    int64_t          mCandidatesInRacksCount;
    int              mMaxReplicationsPerNode;
    double           mMaxSpaceUtilizationThreshold;
    double           mCurSTierMaxSpaceUtilizationThreshold;
    bool             mForReplicationFlag;
    bool             mUsingRackExcludesFlag;
    bool             mUsingServerExcludesFlag;
    bool             mSortBySpaceUtilizationFlag;
    bool             mSortCandidatesByLoadAvgFlag;
    bool             mLastAttemptFlag;
    kfsSTier_t       mMinSTier;
    kfsSTier_t       mMaxSTier;
    kfsSTier_t       mCurSTier;

    int64_t Rand(
        int64_t interval)
        { return mLayoutManager.Rand(interval); }
    void FindCandidatesSelf(
        RackId rackIdToUse = -1)
    {
        FindCandidateRacks(0, rackIdToUse);
        if (rackIdToUse >= 0 && mCandidateRacks.size() > 1) {
            for (size_t ri = 0; ri < mCandidateRacks.size(); ri++) {
                if (mCandidateRacks[ri].second->id() != rackIdToUse) {
                    continue;
                }
                mCandidatesInRacksCount -= mCandidateRacks[ri].first;
                iter_swap(mCandidateRacks.begin() + mRackPos,
                    mCandidateRacks.begin() + ri);
                const RackInfo& rack = *(mCandidateRacks[mRackPos++].second);
                FindCandidateServers(rack);
                if (! mCandidates.empty()) {
                    mCurRackId = rack.id();
                    return;
                }
                break;
            }
        }
        NextRack();
    }
    void FindCandidateRacks(
        bool*  anyAvailableFlag = 0,
        RackId rackIdToUse      = -1)
    {
        mCurSTierMaxSpaceUtilizationThreshold = min(
            mLayoutManager.GetMaxTierSpaceUtilization(mCurSTier),
            mMaxSpaceUtilizationThreshold
        );
        if (anyAvailableFlag) {
            if ((*anyAvailableFlag = ! mCandidateRacks.empty())) {
                return;
            }
        } else {
            mCandidateRacks.clear();
        }
        if (mRacks.empty() || mRacks.size() <= mRackExcludes.Size()) {
            return;
        }
        mCandidatesInRacksCount = 0;
        for (typename RackInfos::const_iterator it = mRacks.begin();
                it != mRacks.end();
                ++it) {
            const RackInfo& rack = *it;
            const RackId    id   = rack.id();
            if (id < 0 || id > RackIdSet::kMaxId) {
                continue; // Invalid rack id.
            }
            if (id != rackIdToUse && mRackExcludes.Find(id)) {
                continue;
            }
            // If only one tier is requested, then get the candidates count in
            // the requested tier.
            const int64_t cnt = mCurSTier == mMaxSTier ?
                rack.getWeightedPossibleCandidatesCount(mCurSTier) :
                rack.getWeightedPossibleCandidatesCount();
            if (cnt <= 0) {
                continue;
            }
            if (anyAvailableFlag) {
                *anyAvailableFlag = true;
                break;
            }
            mCandidatesInRacksCount += cnt;
            mCandidateRacks.push_back(make_pair(cnt, &rack));
        }
    }
    int64_t GetLoad(
        const ChunkServer& srv) const
    {
        const int64_t kLoadAvgFloor = 1;
        if (mSortBySpaceUtilizationFlag) {
            return ((int64_t)(srv.GetStorageTierSpaceUtilization(mCurSTier) *
                (int64_t(1) << (10 + kSlaveScaleFracBits))) + kLoadAvgFloor);
        }
        if (mSortCandidatesByLoadAvgFlag) {
            int64_t load = srv.GetLoadAvg();
            if (! srv.CanBeChunkMaster()) {
                load = (load * mLayoutManager.GetSlavePlacementScale()
                ) >> kSlaveScaleFracBits;
            }
            return (load + kLoadAvgFloor);
        }
        return kLoadAvgFloor;
    }
    bool IsCandidateServer(
        const ChunkServer& srv) const
    {
        return (
            mLayoutManager.IsCandidateServer(srv, mCurSTier) &&
            srv.GetStorageTierSpaceUtilization(mCurSTier) <=
                mCurSTierMaxSpaceUtilizationThreshold &&
            (! mForReplicationFlag ||
                srv.GetNumChunkReplications() < mMaxReplicationsPerNode)
        );
    }
    void FindCandidateServers(
        const RackInfo& rack)
    {
        FindCandidateServers(
            rack.getServers(), rack.getPossibleCandidatesCount(mCurSTier));
    }
    void FindCandidateServers(
        const Sources& sources,
        int            candidatesCount)
    {
        mLoadAvgSum   = 0;
        mCandidatePos = 0;
        mCandidates.clear();
        int cnt = 0;
        for (typename Servers::const_iterator it = sources.begin();
                cnt < candidatesCount && it != sources.end();
                ++it) {
            ChunkServer& srv = **it;
            if (! IsCandidateServer(srv)) {
                continue;
            }
            cnt++;
            if (mServerExcludes.Find(&srv)) {
                continue;
            }
            const int64_t load = GetLoad(srv);
            assert(mLoadAvgSum < mLoadAvgSum + load);
            mLoadAvgSum += load;
            mCandidates.push_back(make_pair(load, &srv));
        }
        mCandidatePos = mCandidates.size();
    }
    void NextTier()
    {
        while (mCurSTier < mMaxSTier &&
                mLayoutManager.GetTierCandidatesCount(++mCurSTier) <= 0)
            {}
        mCurSTierMaxSpaceUtilizationThreshold = min(
            mLayoutManager.GetMaxTierSpaceUtilization(mCurSTier),
            mMaxSpaceUtilizationThreshold
        );
    }
private:
    ChunkPlacement(const ChunkPlacement&);
    ChunkPlacement& operator=(const ChunkPlacement&);
public:
    enum { kMaxRackId = RackIdSet::kMaxId };
};

}
#endif /* CHUNK_PLACEMENT_H */
