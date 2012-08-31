//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/08/08
//
// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
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
// map; we can then migrate blocks around to test, debug chunk placement and
// re-balancing, and experiment with chunk placement algorithms.
//
//----------------------------------------------------------------------------

#ifndef EMULATOR_LAYOUTEMULATOR_H
#define EMULATOR_LAYOUTEMULATOR_H

#include <string>
#include <map>
#include <vector>
#include <fstream>
#include "meta/LayoutManager.h"

namespace KFS
{
using std::ofstream;
using std::map;
using std::vector;
using std::string;

class LayoutEmulator : public LayoutManager
{
public:
    LayoutEmulator()
        : mVariationFromMean(0),
          mNumBlksRebalanced(0),
          mStopFlag(false),
          mPlanFile(),
          mLoc2Server()
    {
        SetMinChunkserversToExitRecovery(0);
        ToggleRebalancing(true);
    }
    ~LayoutEmulator()
    {
        mPlanFile.close();
    }
    // Given a chunk->location data in a file, rebuild the chunk->location map.
    //
    int LoadChunkmap(const string& chunkLocationFn,
        bool addChunksToReplicationChecker = false);
    void AddServer(const ServerLocation& loc,
        int rack, uint64_t totalSpace, uint64_t usedSpace);
    void SetupForRebalancePlanning(
        double utilizationPercentVariationFromMean)
    {
        mVariationFromMean =
            min(1., max(0., utilizationPercentVariationFromMean * 1e-2));
    }
    int SetRebalancePlanOutFile(const string& rebalancePlanFn);
    void BuildRebalancePlan();
    bool ChunkReplicationDone(MetaChunkReplicate* req);
    void ExecuteRebalancePlan();
    void PrintChunkserverBlockCount(ostream& os) const;
    int ReadNetworkDefn(const string& networkFn);
    int VerifyRackAwareReplication(
        bool reportAllFlag, bool verbose, ostream& os);
    seq_t  GetChunkversion(chunkId_t cid) const;
    size_t GetChunkSize(chunkId_t cid) const;
    void MarkServerDown(const ServerLocation& loc);
    int GetNumBlksRebalanced() const
    {
        return mNumBlksRebalanced;
    }
    void Stop()
    {
        mStopFlag = true;
    }
    int RunFsck(const string& fileName);
private:
    typedef map<ServerLocation, ChunkServerPtr> Loc2Server;
    class PlacementVerifier;

    size_t RunChunkserverOps();
    void CalculateRebalaceThresholds();
    void PrepareRebalance(bool enableRebalanceFlag);
    bool Parse(const char* line, size_t size,
        bool addChunksToReplicationChecker, ServerLocation& loc);
    void ShowPlacementError(
        ostream&            os,
        const CSMap::Entry& c,
        const ChunkServer*  srv,
        string&             fileName,
        size_t              replicas,
        const char*         reason);
    void VerifyPlacement(
        const CSMap::Entry&           c,
        const Servers&                servers,
        const vector<MetaChunkInfo*>& cblk,
        ChunkPlacement&               placement,
        ostream&                      os,
        bool                          verboseFlag,
        bool                          reportAllFlag,
        PlacementVerifier&            verifier);
    size_t GetChunkSize(const CSMap::Entry& ci) const;

    // for the purposes of rebalancing, we compute the cluster
    // wide average space utilization; then we take into the
    // desired variation from mean to compute thresholds that determine
    // which nodes are candidates for migration.
    double     mVariationFromMean;
    int        mNumBlksRebalanced;
    bool       mStopFlag;
    ofstream   mPlanFile;
    Loc2Server mLoc2Server;
private:
    // No copy.
    LayoutEmulator(const LayoutEmulator&);
    LayoutEmulator& operator=(const LayoutEmulator&);
};

extern LayoutEmulator gLayoutEmulator;
}

#endif // EMULATOR_LAYOUTEMULATOR_H
