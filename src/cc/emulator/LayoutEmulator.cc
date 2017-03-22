//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/08/27
//
// Author: Sriram Rao
//
// Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
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
// \brief Emulator for the layout manager: read in a chunk->location
// map; we can then migrate blocks around to experiment with placement algorithms.
//
//----------------------------------------------------------------------------

#include "LayoutEmulator.h"
#include "ChunkServerEmulator.h"
#include "common/kfstypes.h"
#include "common/MsgLogger.h"
#include "common/BufferInputStream.h"
#include "common/StBuffer.h"
#include "meta/kfstree.h"
#include "meta/util.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <algorithm>
#include <cerrno>
#include <fstream>
#include <iostream>

#include <boost/bind.hpp>

namespace KFS
{

using std::string;
using std::ifstream;
using std::for_each;
using std::ofstream;
using std::cout;
using boost::bind;

static inline ChunkServerEmulator&
GetCSEmulator(ChunkServer& server)
{
    return static_cast<ChunkServerEmulator&>(server);
}

class MetaChunkEvacuateOp : public MetaChunkEvacuate
{
public:
    MetaChunkEvacuateOp()
        : MetaChunkEvacuate()
    {
        numWritableDrives   = 1;
        numDrives           = 1;
        numEvacuateInFlight = 0;
    }
};

class MetaHelloOp : public MetaHello
{
public:
    MetaHelloOp()
        : MetaHello()
        {}
};

static const
MetaChunkEvacuate& GetMetaChunkEvacuate()
{
    static const MetaChunkEvacuateOp sOp;
    return sOp;
}


void
LayoutEmulator::UseForPlacement(const ChunkServerPtr& srv)
{
    if (0 <= srv->GetRack()) {
        SetRack(srv, srv->GetRack());
    }
    srv->SetCanBeChunkMaster(mMastersCount <= mSlavesCount);
    if (srv->CanBeChunkMaster()) {
        mMastersCount++;
    } else {
        mSlavesCount++;
    }
    MetaHelloOp hello;
    hello.server = srv;
    hello.clnt   = srv.get();
    srv->HelloDone(&hello);
    UpdateSrvLoadAvg(*srv, 0, 0);
    srv->HelloEnd();
    srv->UpdateSpace(GetMetaChunkEvacuate());
    UpdateReplicationsThreshold();
}

int
LayoutEmulator::LoadChunkmap(
    const string& chunkLocationFn, bool addChunksToReplicationChecker)
{
    ifstream file(chunkLocationFn.c_str());
    if (! file) {
        const int err = errno;
        KFS_LOG_STREAM_INFO << chunkLocationFn << ": " << strerror(err) <<
        KFS_LOG_EOM;
        return (err > 0 ? -err : -1);
    }
    const size_t       kMaxLineSize = 256 << 10;
    StBufferT<char, 1> buf;
    char* const        line         = buf.Resize(kMaxLineSize);
    size_t             lineno       = 1;
    size_t             len          = 0;
    ServerLocation     loc;
    line[0] = 0;
    while (file.getline(line, kMaxLineSize) &&
            (len = file.gcount()) < kMaxLineSize - 1 &&
            Parse(line, len, addChunksToReplicationChecker, loc)) {
        lineno++;
    }
    const bool badFlag = file.bad();
    if (! badFlag && file.eof()) {
        return 0;
    }
    const int err = badFlag ? errno : EINVAL;
    KFS_LOG_STREAM_ERROR << chunkLocationFn << ":" << lineno <<
        (badFlag ? " " : " malformed: ") <<
        (badFlag ? strerror(err) : line ) <<
    KFS_LOG_EOM;
    return (err > 0 ? -err : -1);
}

bool
LayoutEmulator::Parse(
    const char*       line,
    size_t            size,
    bool              addChunksToReplicationChecker,
    ServerLocation&   loc)
{
    // format of the file:
    // <chunkid> <fileid> <# of servers> [server location]
    // \n
    // where, <server location>: replica-size name port rack#
    // and we have as many server locations as # of servers

    kfsChunkId_t       cid;
    fid_t              fid;
    int                numServers;
    const char*        p   = line;
    const char* const  end = p + size;
    if (! DecIntParser::Parse(p, end - p, cid) ||
            ! DecIntParser::Parse(p, end - p, fid) ||
            ! DecIntParser::Parse(p, end - p, numServers)) {
        return false;
    }
    CSMap::Entry* const ci = mChunkToServerMap.Find(cid);
    if (! ci) {
        KFS_LOG_STREAM_ERROR << "no such chunk: " << cid << KFS_LOG_EOM;
        return true;
    }
    for (int i = 0; i < numServers; i++) {
        while (p < end && (*p & 0xFF) <= ' ') {
            p++;
        }
        if (p >= end) {
            return false;
        }
        const char* const host = p;
        while (p < end && (*p & 0xFF) > ' ') {
            p++;
        }
        if (p >= end) {
            return false;
        }
        loc.hostname.assign(host, p - host);
        if (! DecIntParser::Parse(p, end - p, loc.port)) {
            return false;
        }
        while (p < end && (*p & 0xFF) <= ' ') {
            p++;
        }
        if (p >= end) {
            return false;
        }
        // const char* const rack = p;
        while (p < end && (*p & 0xFF) > ' ') {
            p++;
        }
        Servers::const_iterator const it = lower_bound(
            mChunkServers.begin(), mChunkServers.end(),
            loc, bind(&ChunkServer::GetServerLocation, _1) < loc
        );
        if (it == mChunkServers.end() || (*it)->GetServerLocation() != loc) {
            KFS_LOG_STREAM_ERROR <<
                "chunk: " << cid <<
                " no such server: "  << loc <<
            KFS_LOG_EOM;
            continue;
        }
        if (! AddReplica(*ci, *it)) {
            KFS_LOG_STREAM_ERROR <<
                "chunk: "        << cid <<
                " add server: "  << loc <<
                " failed" <<
            KFS_LOG_EOM;
            continue;
        }
        GetCSEmulator(**it).HostingChunk(cid, GetChunkSize(*ci));
    }
    if (addChunksToReplicationChecker) {
        CheckChunkReplication(*ci);
    }
    return true;
}

// override what is in the layout manager (only for the emulator code)
bool
LayoutEmulator::Handle(MetaChunkReplicate& req)
{
    mOngoingReplicationStats->Update(-1);
    // Book-keeping....
    if (mNumOngoingReplications > 0) {
        mNumOngoingReplications--;
    }
    req.server->ReplicateChunkDone(req.chunkId);
    if (req.srcLocation.IsValid() && req.dataServer) {
        req.dataServer->UpdateReplicationReadLoad(-1);
    }
    req.dataServer.reset();
    if (0 != req.status) {
        // Replication failed...we will try again later
        KFS_LOG_STREAM_ERROR <<
            "replication failed"
            " chunk: "  << req.chunkId <<
            " status: " << req.status <<
            " "         << req.statusMsg <<
            " server: " << req.server->GetServerLocation() <<
            " "         << req.Show() <<
        KFS_LOG_EOM;
        mFailedReplicationStats->Update(1);
        return false;
    }
    mNumBlksRebalanced++;
    // replication succeeded: book-keeping
    CSMap::Entry* const ci = mChunkToServerMap.Find(req.chunkId);
    if (! ci) {
        KFS_LOG_STREAM_ERROR <<
            "replication completion: no such"
            " chunk: " << req.chunkId <<
            " "        << req.Show() <<
        KFS_LOG_EOM;
        return false;
    }
    const bool addedFlag = AddReplica(*ci, req.server);
    if (addedFlag) {
        GetCSEmulator(*(req.server)).HostingChunk(
            req.chunkId, GetChunkSize(*ci));
    } else {
        KFS_LOG_STREAM_ERROR <<
            "chunk: "        << req.chunkId <<
            " add server: "  << req.server->GetServerLocation() <<
            " failed" <<
        KFS_LOG_EOM;
    }
    CheckChunkReplication(*ci);
    return addedFlag;
}

// override what is in the layout manager (only for the emulator code)
bool
LayoutEmulator::Handle(MetaChunkDelete& req)
{
    if (0 != req.status) {
        // Replication failed...we will try again later
        KFS_LOG_STREAM_ERROR <<
            "delete failed"
            " chunk: "  << req.chunkId <<
            " status: " << req.status <<
            " "         << req.statusMsg <<
            " server: " << req.server->GetServerLocation() <<
            " "         << req.Show() <<
        KFS_LOG_EOM;
        return false;
    }
    CSMap::Entry* const ci = mChunkToServerMap.Find(req.chunkId);
    if (! ci) {
        KFS_LOG_STREAM_ERROR <<
            "delete completion: no such"
            " chunk: " << req.chunkId <<
            " "        << req.Show() <<
        KFS_LOG_EOM;
        return false;
    }
    const bool removedFlag = mChunkToServerMap.RemoveServer(req.server, *ci);
    if (! removedFlag) {
        KFS_LOG_STREAM_ERROR <<
            "chunk: "        << req.chunkId <<
            " remove server: "  << req.server->GetServerLocation() <<
            " failed" <<
        KFS_LOG_EOM;
    }
    CheckChunkReplication(*ci);
    return removedFlag;
}

seq_t
LayoutEmulator::GetChunkversion(chunkId_t cid) const
{
    const CSMap::Entry* const ci = mChunkToServerMap.Find(cid);
    return (ci ? ci->GetChunkInfo()->chunkVersion : -1);
}

size_t
LayoutEmulator::GetChunkSize(chunkId_t cid) const
{
    const CSMap::Entry* const ci = mChunkToServerMap.Find(cid);
    if (! ci) {
        return 0;
    }
    return GetChunkSize(*ci);
}

size_t
LayoutEmulator::GetChunkSize(const CSMap::Entry& ci) const
{
    // Assume that the file isn't sparse.
    const MetaFattr* const fa = ci.GetFattr();
    if (fa->chunkcount() <= 0) {
        return 0;
    }
    const chunkOff_t pos   = ci.GetChunkInfo()->offset;
    const chunkOff_t fsize = metatree.getFileSize(fa);
    if (! fa->IsStriped()) {
        if (fsize < pos) {
            return CHUNKSIZE;
        }
        return min(CHUNKSIZE, size_t(fsize - pos));
    }

    const chunkOff_t blkSize = fa->numStripes * (chunkOff_t)CHUNKSIZE;
    const chunkOff_t blkPos  = fa->ChunkPosToChunkBlkFileStartPos(pos);
    const chunkOff_t size    = fsize - blkPos;
    if (size <= 0 || size >= blkSize) {
        return CHUNKSIZE;
    }
    const int        chunkStripeIdx = (int)(pos / (chunkOff_t)CHUNKSIZE %
        (fa->numStripes + fa->numRecoveryStripes));
    const chunkOff_t strideSize     = fa->stripeSize * fa->numStripes;
    const chunkOff_t strideCount    = size / strideSize;
    const chunkOff_t strideHead     = size % strideSize;
    const chunkOff_t stripeIdx      = strideHead / fa->stripeSize;
    const int        idx            =
            chunkStripeIdx < (int)fa->numStripes ? chunkStripeIdx : 0;
    chunkOff_t       chunkSize      = strideCount * fa->stripeSize;
    if (idx < stripeIdx) {
        chunkSize += fa->stripeSize;
    } else if (idx == stripeIdx) {
        chunkSize += strideHead % fa->stripeSize;
    }
    return (size_t)chunkSize;
}

void
LayoutEmulator::AddServer(
    const ServerLocation& loc,
    int                   rack,
    uint64_t              totalSpace,
    uint64_t              usedSpace)
{
    ChunkServerPtr c;
    ChunkServerEmulator& srv =
        *(new ChunkServerEmulator(loc, rack, *this));
    c.reset(&srv);
    srv.Init(totalSpace, usedSpace, mUseFsTotalSpaceFlag);

    mChunkToServerMap.AddServer(c);
    Servers::iterator const its = lower_bound(
        mChunkServers.begin(), mChunkServers.end(), loc,
        bind(&ChunkServer::GetServerLocation, _1) < loc
    );
    mChunkServers.insert(its, c);
    UseForPlacement(c);
    KFS_LOG_STREAM_INFO <<
        "added:"
        " server: "      << srv.GetServerLocation() <<
        " rack: "        << srv.GetRack() <<
        " space:"
        " total: "       << srv.GetTotalSpace(mUseFsTotalSpaceFlag) <<
        " utilization: " << srv.GetSpaceUtilization(mUseFsTotalSpaceFlag) <<
    KFS_LOG_EOM;
}

int
LayoutEmulator::SetRebalancePlanOutFile(const string& rebalancePlanFn)
{
    mPlanFile.close();
    mPlanFile.open(rebalancePlanFn.c_str(), ofstream::out);
    mPlanFile.setf(istream::hex);
    if (! mPlanFile) {
        const int err = errno;
        KFS_LOG_STREAM_ERROR << rebalancePlanFn << ": " << strerror(err) <<
        KFS_LOG_EOM;
        return -1;
    }
    for (Servers::iterator i = mChunkServers.begin();
            i != mChunkServers.end();
            i++) {
        GetCSEmulator(**i).SetRebalancePlanOutFd(&mPlanFile);
    }
    return 0;
}

size_t
LayoutEmulator::RunChunkserverOps()
{
    size_t opsCount = 0;
    const size_t size = mChunkServers.size();
    for (size_t i = 0; i < size; i++) {
        ChunkServer& srv = *mChunkServers[i];
        opsCount += GetCSEmulator(srv).Dispatch();
        if (mChunkServers.size() != size) {
            panic("chunk servers vector size change detected");
            break;
        }
        if (&*mChunkServers[i] == &srv) {
            UpdateSrvLoadAvg(srv, 0, 0);
        }
    }
    return opsCount;
}

void
LayoutEmulator::CalculateRebalaceThresholds()
{
    double avgSpaceUtil = 0;
    for (Servers::iterator it = mChunkServers.begin();
            it != mChunkServers.end();
            it++) {
        avgSpaceUtil += (*it)->GetSpaceUtilization(mUseFsTotalSpaceFlag);
    }
    const size_t cnt = mChunkServers.size();
    if (cnt > 0) {
        avgSpaceUtil /= cnt;
    }
    if (mVariationFromMean > 0) {
        // Take the average utilizaiton in the cluster; any node that has
        // utilizaiton outside the average is candidate for rebalancing
        mMinRebalanceSpaceUtilThreshold =
            max(0.0, avgSpaceUtil - mVariationFromMean * 0.5);
        mMaxRebalanceSpaceUtilThreshold =
            min(1.0, avgSpaceUtil + mVariationFromMean * 0.5);
    }
    KFS_LOG_STREAM_INFO <<
        "chunk servers: " << cnt <<
        " racks: "        << mRacks.size() <<
        " rebalance thresholds:"
        " average: "      << avgSpaceUtil <<
        " min: "          << mMinRebalanceSpaceUtilThreshold <<
        " max: "          << mMaxRebalanceSpaceUtilThreshold <<
    KFS_LOG_EOM;
}

void
LayoutEmulator::PrepareRebalance(bool enableRebalanceFlag)
{
    mRecoveryStartTime             = TimeNow() - 10 * mRecoveryIntervalSec;
    mMinChunkserversToExitRecovery = 0;
    mClientCSAuthRequiredFlag      = false;
    SetPrimary(true);

    ToggleRebalancing(enableRebalanceFlag);

    if (enableRebalanceFlag) {
        CalculateRebalaceThresholds();
    }
    const int64_t kMicroseconds = int64_t(1000) * 1000;
    const int64_t kMaxRunTime   = kMicroseconds * 15;
    mRebalanceRunInterval             = -1;
    mMinChunkReplicationCheckInterval = -1;
    mMaxRebalanceRunTime             = max(mMaxRebalanceRunTime, kMaxRunTime);
    mMaxTimeForChunkReplicationCheck =
        max(mMaxTimeForChunkReplicationCheck, kMaxRunTime);

    if (mMaxConcurrentWriteReplicationsPerNode < 1) {
        mMaxConcurrentWriteReplicationsPerNode = 1;
        UpdateReplicationsThreshold();
    }
    if (mMaxConcurrentReadReplicationsPerNode < 1) {
        mMaxConcurrentReadReplicationsPerNode = 1;
    }
}

void
LayoutEmulator::ScheduleReplication()
{
    if (mCleanupScheduledFlag) {
        ScheduleCleanup();
    }
    if (HandoutChunkReplicationWork()) {
        return;
    }
    RebalanceServers();
}
void
LayoutEmulator::BuildRebalancePlan()
{
    PrepareRebalance(true);

    RebalanceCtrs::Counter round       = mRebalanceCtrs.GetRoundCount() + 1;
    RebalanceCtrs::Counter nextScanned = 0;
    const size_t kThreshUpdateInterval = 4 << 10;
    size_t       updateThreshOpsCnt    = kThreshUpdateInterval;
    for (int prev = mNumBlksRebalanced; ;) {
        ScheduleReplication();
        const size_t opsCount = RunChunkserverOps();
        if (prev != mNumBlksRebalanced) {
            prev  = mNumBlksRebalanced;
            round = mRebalanceCtrs.GetRoundCount() + 1;
        }
        const bool doneFlag =
            mStopFlag ||
            mChunkServers.empty() ||
            mCSTotalPossibleCandidateCount <= 0 ||
            (opsCount <= 0 &&
            ! mCleanupScheduledFlag &&
            mRebalanceCtrs.GetRoundCount() > round &&
            ! mChunkToServerMap.Front(CSMap::Entry::kStateCheckReplication));
        RebalanceCtrs::Counter const scanned = mRebalanceCtrs.GetTotalScanned();
        if (doneFlag || nextScanned < scanned) {
            KFS_LOG_STREAM_START(MsgLogger::kLogLevelINFO, logStream);
                ostream& os = logStream.GetStream();
                os << "=== rebalance counters: ";
                mRebalanceCtrs.Show(os, ": ", " ");
            KFS_LOG_STREAM_END;
            nextScanned = scanned + 1000 * 1000;
        }
        if (doneFlag) {
            break;
        }
        if (mVariationFromMean > 0) {
            if (updateThreshOpsCnt <= opsCount) {
                CalculateRebalaceThresholds();
                updateThreshOpsCnt = kThreshUpdateInterval;
            } else {
                updateThreshOpsCnt -= opsCount;
            }
        }
    }
}

void
LayoutEmulator::ExecuteRebalancePlan()
{
    // the plan has already been worked out; we just execute
    PrepareRebalance(false);

    RebalanceCtrs::Counter nextScanned = 0;
    for (; ;) {
        ScheduleReplication();
        const bool doneFlag =
            mStopFlag ||
            mChunkServers.empty() ||
            mCSTotalPossibleCandidateCount <= 0 ||
            (RunChunkserverOps() <= 0 &&
            ! mChunkToServerMap.Front(CSMap::Entry::kStateCheckReplication) &&
            // ! mIsExecutingRebalancePlan &&
            ! mCleanupScheduledFlag);
        RebalanceCtrs::Counter const scanned = mRebalanceCtrs.GetTotalScanned();
        if (doneFlag || nextScanned < scanned) {
            KFS_LOG_STREAM_START(MsgLogger::kLogLevelINFO, logStream);
                ostream& os = logStream.GetStream();
                os << "=== rebalance counters: ";
                mRebalanceCtrs.Show(os, ": ", " ");
            KFS_LOG_STREAM_END;
            nextScanned = scanned + 1000 * 1000;
        }
        if (doneFlag) {
            break;
        }
    }
}

class PrintBlockCount
{
    ostream&   mOs;
    const bool mUseFsTotalSpaceFlag;
public:
    PrintBlockCount(ostream& os, bool f)
        : mOs(os),
          mUseFsTotalSpaceFlag(f)
        {}
    void operator()(const ChunkServerPtr &c) const
    {
        ChunkServer& cse = *c;
        mOs << cse.GetServerLocation() <<
            ' ' << cse.GetChunkCount() <<
            ' ' << cse.GetUsedSpace() <<
            ' ' << cse.GetSpaceUtilization(mUseFsTotalSpaceFlag) <<
        '\n';
    }
};

void
LayoutEmulator::PrintChunkserverBlockCount(ostream& os) const
{
    for_each(mChunkServers.begin(), mChunkServers.end(),
        PrintBlockCount(os, mUseFsTotalSpaceFlag));
}

int
LayoutEmulator::InitUseCurrentState(
    int64_t chunkServerTotalSpace)
{
    KFS_LOG_STREAM_DEBUG <<
        "servers: " << ChunkServer::GetChunkServerCount() <<
    KFS_LOG_EOM;
    int64_t csTotalSpace = chunkServerTotalSpace;
    if (chunkServerTotalSpace < 0) {
        csTotalSpace = 0;
        for (Servers::const_iterator it = mChunkServers.begin();
                it != mChunkServers.end();
                ++it) {
            const ChunkServer& srv = **it;
            csTotalSpace += srv.GetChunkCount() * (int64_t)CHUNKSIZE;
        }
        for (HibernatedServerInfos::const_iterator
                it = mHibernatingServers.begin();
                mHibernatingServers.end() != it;
                ++it) {
            if (it->IsHibernated() && 0 <= GetRackId(it->location)) {
                HibernatedChunkServer* const hsrv =
                    mChunkToServerMap.GetHiberantedServer(it->csmapIdx);
                if (! hsrv) {
                    panic("invalid hibernated server index");
                    return -EFAULT;
                }
                csTotalSpace += hsrv->GetChunkCount() * (int64_t)CHUNKSIZE;
            }
        }
        csTotalSpace += csTotalSpace * 20 / 100;
        const size_t size = mChunkServers.size();
        if (0 < size) {
            csTotalSpace /= size;
        }
        KFS_LOG_STREAM_INFO <<
            "chunk server space: " << csTotalSpace << " x " << size <<
        KFS_LOG_EOM;
    }
    for (Servers::iterator it = mChunkServers.begin();
            it != mChunkServers.end();
            ++it) {
        ChunkServerPtr& srv = *it;
        ChunkServerEmulator& cse = *(new ChunkServerEmulator(
            srv->GetServerLocation(), srv->GetRack(), *this));
        ChunkServerPtr ep;
        ep.reset(&cse);
        cse.Init(csTotalSpace, 0, mUseFsTotalSpaceFlag);
        srv->ForceDown();
        if (! mChunkToServerMap.ReplaceServerWith(srv, ep)) {
            panic("failed to replace server");
            return -EFAULT;
        }
        if (srv->IsReplay()) {
            if (mReplayServerCount <= 0) {
                panic("invalid replay server count");
            }
            mReplayServerCount--;
        } else if (! srv->IsConnected()) {
            if (mDisconnectedCount <= 0) {
                panic("invalid disconnected server count");
            }
            mDisconnectedCount--;
        }
        if (srv->IsHelloNotifyPending()) {
            KFS_LOG_STREAM_INFO <<
                srv->GetServerLocation() << " pending hello in progress" <<
            KFS_LOG_EOM;
        }
        RackInfos::iterator const rackIter = FindRack(srv->GetRack());
        if (rackIter != mRacks.end() &&
                rackIter->removeServer(srv) &&
                rackIter->getServers().empty()) {
            mRacks.erase(rackIter);
        }
        srv.swap(ep);
        UseForPlacement(srv);
        KFS_LOG_STREAM_INFO <<
            "added:"
            " server: "      << srv->GetServerLocation() <<
            " rack: "        << srv->GetRack() <<
            " space:"
            " total: "       << srv->GetTotalSpace(mUseFsTotalSpaceFlag) <<
            " used: "        << srv->GetUsedSpace() <<
            " utilization: " <<
                srv->GetSpaceUtilization(mUseFsTotalSpaceFlag) <<
        KFS_LOG_EOM;
    }
    for (HibernatedServerInfos::const_iterator it = mHibernatingServers.begin();
            mHibernatingServers.end() != it; ) {
        if (it->IsHibernated()) {
            HibernatedChunkServer* const hsrv =
                mChunkToServerMap.GetHiberantedServer(it->csmapIdx);
            if (! hsrv) {
                panic("invalid hibernated server index");
                return -EFAULT;
            }
            const RackId rackId = GetRackId(it->location);
            KFS_LOG_STREAM_INFO <<
                "hibernated server: " << it->location <<
                " rack: "             << rackId <<
                " chunks: "           << hsrv->GetChunkCount() <<
                (0 <= rackId ? " replacing with up server emulator" : "") <<
            KFS_LOG_EOM;
            if (rackId < 0) {
                ++it;
                continue;
            }
            ChunkServerEmulator& cse = *(new ChunkServerEmulator(
                it->location, rackId, *this));
            ChunkServerPtr srv;
            srv.reset(&cse);
            cse.Init(csTotalSpace, 0, mUseFsTotalSpaceFlag);
            if (! mChunkToServerMap.ReplaceHibernatedServer(
                    srv, it->csmapIdx)) {
                panic("failed to replace hibernated server");
                return -EFAULT;
            }
            Servers::iterator const its = lower_bound(
                mChunkServers.begin(), mChunkServers.end(), it->location,
                bind(&ChunkServer::GetServerLocation, _1) < it->location
            );
            if (mChunkServers.end() != its &&
                    (*its)->GetServerLocation() == it->location) {
                panic("invalid duplicate hibernated server location");
                return -EFAULT;
            }
            mChunkServers.insert(its, srv);
            // Convert to non const iterator for pre c++11.
            it = mHibernatingServers.erase(mHibernatingServers.begin() +
                (it - mHibernatingServers.begin()));
            UseForPlacement(srv);
        } else {
            ++it;
        }
    }
    KFS_LOG_STREAM_DEBUG <<
        "servers: " << ChunkServer::GetChunkServerCount() <<
    KFS_LOG_EOM;
    mChunkToServerMap.RemoveServerCleanup(0);
    mChunkToServerMap.First();
    Servers srvs;
    for (const CSMap::Entry* ci; (ci = mChunkToServerMap.Next()); ) {
        srvs.clear();
        mChunkToServerMap.GetServers(*ci, srvs);
        for (Servers::const_iterator
                it = srvs.begin(); it != srvs.end(); ++it) {
            GetCSEmulator(**it).HostingChunk(
                ci->GetChunkId(), GetChunkSize(*ci));
        }
    }
    return 0;
}

int
LayoutEmulator::ReadNetworkDefn(const string& networkFn)
{
    // Clear checkpoint / replay chunk server and chunk map.
    for (Servers::const_iterator it = mChunkServers.begin();
            it != mChunkServers.end();
            ++it) {
        const ChunkServerPtr& srv = *it;
        srv->ForceDown();
        if (! mChunkToServerMap.RemoveServer(srv)) {
            panic("failed to remove server");
            return -EFAULT;
        }
    }
    mReplayServerCount = 0;
    mDisconnectedCount = 0;
    mChunkServers.clear();
    for (HibernatedServerInfos::const_iterator it = mHibernatingServers.begin();
            mHibernatingServers.end() != it;
            ++it) {
        if (it->IsHibernated()) {
            if (! mChunkToServerMap.RemoveHibernatedServer(it->csmapIdx)) {
                panic("failed to remove hibernated server");
                return -EFAULT;
            }
        }
    }
    mHibernatingServers.clear();
    mChunkToServerMap.RemoveServerCleanup(0);
    mRacks.clear();

    ifstream file(networkFn.c_str());
    if (! file) {
        const int err = errno;
        KFS_LOG_STREAM_ERROR << networkFn << ": " << strerror(err) <<
        KFS_LOG_EOM;
        return (err > 0 ? -err : -1);
    }

    ServerLocation    loc;
    BufferInputStream bis;
    const size_t      kMaxLineSize = 4 << 10;
    char              line[kMaxLineSize];
    size_t            lineno = 1;
    size_t            len;
    line[0] = 0;
    while (file.getline(line, kMaxLineSize) &&
            (len = file.gcount()) < kMaxLineSize - 1) {
        istream& is = bis.Set(line, len);
        int      rack;
        uint64_t totalSpace;
        uint64_t usedSpace;
        if (! (is >> loc.hostname >> loc.port >> rack >>
                totalSpace >> usedSpace)) {
            KFS_LOG_STREAM_ERROR << networkFn << ":" << lineno <<
                " malformed: " << line <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        lineno++;
        AddServer(loc, rack, totalSpace, usedSpace);
    }
    const bool badFlag = file.bad();
    if (badFlag || ! file.eof()) {
        const int err = badFlag ? errno : EINVAL;
        KFS_LOG_STREAM_ERROR << networkFn << ":" << lineno <<
            (badFlag ? " " : " malformed: ") <<
            (badFlag ? strerror(err) : line ) <<
        KFS_LOG_EOM;
        return (err > 0 ? -err : -1);
    }

    const size_t cnt = mChunkServers.size();
    KFS_LOG_STREAM_INFO <<
        "chunk servers: " << cnt <<
        " racks: "        << mRacks.size() <<
    KFS_LOG_EOM;
    for (RackInfos::iterator it = mRacks.begin();
            it != mRacks.end();
            ++it) {
        KFS_LOG_STREAM_INFO <<
            "rack: "                 << it->id() <<
            " weight: "              << it->getWeight() <<
            " servers: "             << it->getServers().size() <<
            " candidates: "          << it->getPossibleCandidatesCount() <<
            " weighted candidates: " <<
                it->getWeightedPossibleCandidatesCount() <<
        KFS_LOG_EOM;
    }

    return 0;
}

class LayoutEmulator::PlacementVerifier
{
public:
    int           sameRack;
    int           underReplicated;
    int           overReplicated;
    int           missing;
    int           sameNode;
    int           stripeSameNode;
    const int64_t startTime;

    PlacementVerifier()
        : sameRack(0),
          underReplicated(0),
          overReplicated(0),
          missing(0),
          sameNode(0),
          stripeSameNode(0),
          startTime(microseconds())
        {}
    ostream& report(ostream& os, size_t chunkCount)
    {
        os <<
        "************************************************\n"
        " Total chunks : "                  << chunkCount         << "\n"
        " Total chunks missing : "          << missing            << "\n"
        " Total chunks on same rack : "     << sameRack           << "\n"
        " Total chunks under replicated : " << underReplicated    << "\n"
        " Total chunks over replicated : "  << overReplicated     << "\n"
        " Total stripes on same node : "    << stripeSameNode     << "\n"
        " Run time : " << ((microseconds() - startTime) * 1e-6)   << "\n"
        "************************************************\n"
        ;
        return os;
    }
    bool IsHealthy() const
        { return (missing <= 0); }
};

inline const string&
GetFileName(const MetaFattr* fa, string& fileName)
{
    if (fileName.empty()) {
        fileName = metatree.getPathname(fa);
    }
    return fileName;
}

class DisplayFileType
{
public:
    DisplayFileType(const MetaFattr* a)
        : fa(a)
        {}
    ostream& Display(ostream& os) const
    {
        if (! fa->IsStriped()) {
            return (os << "r " << fa->numReplicas);
        }
        return (os << "rs " << fa->numReplicas << "," <<
            fa->numStripes << "+" << fa->numRecoveryStripes);
    }
private:
    const MetaFattr* const fa;
};

ostream&
operator<<(ostream& os, const DisplayFileType& d) {
    return d.Display(os);
}

void
LayoutEmulator::ShowPlacementError(
    ostream&            os,
    const CSMap::Entry& c,
    const ChunkServer*  srv,
    string&             fileName,
    size_t              replicas,
    const char*         reason)
{
    const MetaFattr* const fa  = c.GetFattr();
    const chunkOff_t       pos = c.GetChunkInfo()->offset;
    os <<
        reason <<
        " chunk: "    << c.GetChunkId() <<
        " pos: "      << pos <<
        " size: "     << GetChunkSize(c) <<
        " block: "    << fa->ChunkPosToChunkBlkIndex(pos) <<
        " file: "     << c.GetFileId() <<
        " type: "     << DisplayFileType(fa) <<
        " node: "     << (srv ? srv->GetServerLocation() : ServerLocation()) <<
        " rack: "     << (srv ? srv->GetRack() : -1) <<
        " replicas: " << fa->numReplicas <<
        " actual: "   << replicas <<
        " "           << GetFileName(fa, fileName) <<
    "\n";
}

void
LayoutEmulator::VerifyPlacement(
    const CSMap::Entry&                c,
    const LayoutEmulator::Servers&     servers,
    const vector<MetaChunkInfo*>&      cblk,
    LayoutEmulator::ChunkPlacement&    placement,
    ostream&                           os,
    bool                               verboseFlag,
    bool                               reportAllFlag,
    LayoutEmulator::PlacementVerifier& verifier)
{
    const MetaFattr* const fa  = c.GetFattr();
    string                 fileName;

    if (servers.empty()) {
        if (verboseFlag) {
            ShowPlacementError(os, c, 0, fileName, servers.size(),
                "no replicas");
        }
        verifier.missing++;
        return;
    }
    for (Servers::const_iterator it = servers.begin();
            it != servers.end();
            ++it) {
        ChunkServer& srv = **it;
        if (placement.IsServerExcluded(srv)) {
            if (! fa->IsStriped() ||
                    find(it + 1, servers.end(), *it) !=
                        servers.end()) {
                verifier.sameNode++;
                ShowPlacementError(os, c, &srv, fileName, servers.size(),
                    "duplicate server");
            } else if (reportAllFlag ||
                    placement.GetExcludedServersCount() <
                    mChunkServers.size()) {
                verifier.stripeSameNode++;
                if (verboseFlag) {
                    ShowPlacementError(os, c, &srv, fileName, servers.size(),
                        "same node");
                }
            }
            placement.ExcludeServerAndRack(srv, c.GetChunkId());
        } else if (! placement.ExcludeServerAndRack(srv, c.GetChunkId()) &&
                (reportAllFlag || placement.HasCandidateRacks())) {
            verifier.sameRack++;
            if (verboseFlag) {
                ShowPlacementError(os, c, &srv, fileName, servers.size(),
                    "same rack");
            }
        }
    }
    if (! servers.empty() &&
            servers.size() != (size_t)fa->numReplicas) {
        const bool underReplicatedFlag =
            servers.size() < (size_t)fa->numReplicas;
        if (underReplicatedFlag) {
            verifier.underReplicated++;
        } else {
            verifier.overReplicated++;
        }
        if (verboseFlag) {
            ShowPlacementError(os, c, 0, fileName, servers.size(),
                (underReplicatedFlag ? " under replicated" :
                    " over replicated"));
        }
    }
}

int
LayoutEmulator::VerifyRackAwareReplication(
    bool reportAllFlag, bool verboseFlag, ostream& os)
{
    os <<
    "************************************************\n"
    " KFS Replica Checker\n"
    "************************************************\n"
    ;
    StTmp<Servers>                 serversTmp(mServersTmp);
    StTmp<ChunkPlacement>          placementTmp(mChunkPlacementTmp);
    StTmp<vector<MetaChunkInfo*> > cinfoTmp(mChunkInfosTmp);
    PlacementVerifier     verifier;
    const bool kIncludeThisChunkFlag             = false;
    const bool kStopIfHasAnyReplicationsInFlight = false;
    mChunkToServerMap.First();
    for (const CSMap::Entry* p; (p = mChunkToServerMap.Next()); ) {
        ChunkPlacement& placement = placementTmp.Get();
        Servers& servers = serversTmp.Get();
        mChunkToServerMap.GetServers(*p, servers);
        vector<MetaChunkInfo*>& cblk = cinfoTmp.Get();
        if (! servers.empty()) {
            GetPlacementExcludes(*p, placement, kIncludeThisChunkFlag,
                kStopIfHasAnyReplicationsInFlight, &cblk);
        }
        VerifyPlacement(*p, servers, cblk, placement,
            os, verboseFlag, reportAllFlag, verifier);
    }
    verifier.report(os, mChunkToServerMap.Size());
    return (verifier.IsHealthy() ? 0 : 1);
}

int
LayoutEmulator::RunFsck(const string& fileName)
{
    const bool kReportAbandonedFilesFlag = true;
    if (fileName.empty() || fileName == "-") {
        return LayoutManager::RunFsck("tmp.", kReportAbandonedFilesFlag, cout);
    }
    return LayoutManager::RunFsck(fileName, kReportAbandonedFilesFlag);
}

LayoutEmulator::~LayoutEmulator()
{
    mPlanFile.close();
    for (Servers::iterator it = mChunkServers.begin();
            it != mChunkServers.end();
            ++it) {
        ChunkServerPtr& srv = *it;
        srv->ForceDown();
        GetCSEmulator(*srv).Dispatch();
    }
}

/* static */ LayoutEmulator&
LayoutEmulator::Instance()
{
    static LayoutEmulator sLayoutEmulator;
    return sLayoutEmulator;
}

/* static */ LayoutManager&
LayoutManager::Instance()
{
    return LayoutEmulator::Instance();
}

} // namespace KFS
