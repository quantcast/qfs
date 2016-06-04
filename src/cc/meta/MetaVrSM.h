//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2016/05/11
// Author: Mike Ovsiannikov
//
// Copyright 2016 Quantcast Corp.
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
// Veiwstamped replications state machine..
//
// http://pmg.csail.mit.edu/papers/vr-revisited.pdf
//
//----------------------------------------------------------------------------

#ifndef META_VRSM_H
#define META_VRSM_H

#include "common/kfstypes.h"
#include "common/kfsdecls.h"
#include "common/StdAllocator.h"

#include <time.h>

#include <vector>
#include <map>
#include <utility>

namespace KFS
{
using std::vector;
using std::map;
using std::pair;
using std::less;
using std::make_pair;

struct MetaRequest;
class MetaVrStartViewChange;
class MetaVrDoViewChange;
class MetaVrStartView;
class MetaVrReconfiguration;
class MetaVrStartEpoch;
class Properties;
class LogTransmitter;

const char* const kMetaVrNodeIdParameterNamePtr = "metaServer.Vr.id";

class MetaVrSM
{
public:
    class Config
    {
    public:
        typedef int64_t                NodeId;
        typedef uint64_t               Flags;
        typedef vector<ServerLocation> Locations;
        enum
        {
            kFlagsNone   = 0,
            kFlagWitness = 0x1,
            kFlagActive  = 0x2
        };
        class Node
        {
        public:
            Node(
                Flags            inFlags        = kFlagsNone,
                int              inPrimaryOrder = 0,
                const Locations& inLocations    = Locations())
                : mFlags(inFlags),
                  mPrimaryOrder(inPrimaryOrder),
                  mLocations(inLocations)
                {}
            template<typename ST>
            ST& Insert(
                ST&         inStream,
                const char* inDelimPtr = " ") const
            {
                inStream <<
                    mLocations.size() <<
                    inDelimPtr << mFlags <<
                    inDelimPtr << mPrimaryOrder;
                for (Locations::const_iterator theIt = mLocations.begin();
                        mLocations.end() != theIt;
                        ++theIt) {
                    inStream << inDelimPtr << *theIt;
                }
                return inStream;
            }
            template<typename ST>
            ST& Extract(
                ST& inStream)
            {
                Clear();
                size_t theSize;
                if (! (inStream >> theSize) || ! (inStream >> mFlags) ||
                        ! (inStream >> mPrimaryOrder)) {
                    return inStream;
                }
                mLocations.reserve(theSize);
                while (mLocations.size() < theSize) {
                    ServerLocation theLocation;
                    if (! (inStream >> theLocation) ||
                            ! theLocation.IsValid()) {
                        break;
                    }
                    mLocations.push_back(theLocation);
                }
                if (mLocations.size() != theSize) {
                    inStream.setstate(ST::failbit);
                    Clear();
                }
                return inStream;
            }
            void Clear()
            {
                mFlags        = kFlagsNone;
                mPrimaryOrder = 0;
                mLocations.clear();
            }
            const Locations& GetLocations() const
                { return mLocations; }
            Flags GetFlags() const
                { return mFlags; }
            void SetFlags(
                Flags inFlags)
                { mFlags = inFlags; }
        int GetPrimaryOrder() const
            { return mPrimaryOrder; }
        private:
            Flags     mFlags;
            int       mPrimaryOrder;
            Locations mLocations;
        };
        typedef map<
            NodeId,
            Node,
            less<NodeId>,
            StdFastAllocator<pair<const NodeId, Node> >
        > Nodes;

        Config()
            : mNodes()
            {}
        template<typename ST>
        ST& Insert(
            ST&         inStream,
            const char* inDelimPtr     = " ",
            const char* inNodeDelimPtr = " ") const
        {
            inStream << mNodes.size();
            for (Nodes::const_iterator theIt = mNodes.begin();
                    mNodes.end() != theIt;
                    ++theIt) {
                inStream  << inNodeDelimPtr << theIt->first << inDelimPtr;
                theIt->second.Insert(inStream, inDelimPtr);
            }
        }
        template<typename ST>
        ST& Extract(
            ST& inStream)
        {
            mNodes.clear();
            size_t theSize;
            if (! (inStream >> theSize)) {
                return inStream;
            }
            while (mNodes.size() < theSize) {
                Node theNode;
                NodeId theId = -1;
                if (! (inStream >> theId) ||
                        theId < 0 ||
                        ! theNode.Extract(inStream)) {
                    mNodes.clear();
                    break;
                }
                pair<Nodes::iterator, bool> const theRes =
                    mNodes.insert(make_pair(theId, theNode));
                if (! theRes.second &&
                        theNode.GetPrimaryOrder() <
                            theRes.first->second.GetPrimaryOrder()) {
                    mNodes[theId] = theNode;
                }
            }
            if (mNodes.size() != theSize) {
                inStream.setstate(ST::failbit);
            }
            return inStream;
        }
        bool IsEmpty() const
            { return mNodes.empty(); }
        const Nodes& GetNodes() const
            { return mNodes; }
        Nodes& GetNodes()
            { return mNodes; }
        bool Validate() const;
        bool AddNode(
            NodeId      inId,
            const Node& inNode)
            { return mNodes.insert(make_pair(inId, inNode)).second; }
        bool RemoveNode(
            NodeId inId)
            { return (0 < mNodes.erase(inId)); }
    private:
        Nodes mNodes;
    };

    typedef Config::NodeId NodeId;

    MetaVrSM(
        LogTransmitter& inLogTransmitter);
    ~MetaVrSM();
    int HandleLogBlock(
        seq_t  inLogSeq,
        seq_t  inBlockLenSeq,
        seq_t  inCommitSeq,
        seq_t& outEpochSeq,
        seq_t& outViewSeq);
    bool Handle(
        MetaRequest& inReq);
    void HandleReply(
        MetaVrStartViewChange& inReq,
        seq_t                  inSeq,
        const Properties&      inProps,
        NodeId                 inNodeId);
    void HandleReply(
        MetaVrDoViewChange& inReq,
        seq_t               inSeq,
        const Properties&   inProps,
        NodeId              inNodeId);
    void HandleReply(
        MetaVrStartView&  inReq,
        seq_t             inSeq,
        const Properties& inProps,
        NodeId            inNodeId);
    void HandleReply(
        MetaVrReconfiguration& inReq,
        seq_t                  inSeq,
        const Properties&      inProps,
        NodeId                 inNodeId);
    void HandleReply(
        MetaVrStartEpoch& inReq,
        seq_t             inSeq,
        const Properties& inProps,
        NodeId            inNodeId);
    void SetLastLogReceivedTime(
        time_t inTime);
    void Process(
        time_t inTimeNow);
    int SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters);
    void Commit(
        seq_t inLogSeq);
    void Start();
    void Shutdown();
    const Config& GetConfig() const;
    int GetQuorum() const;
    bool IsPrimary() const;
private:
    class Impl;
    Impl& mImpl;
private:
    MetaVrSM(
        const MetaVrSM& inSm);
    MetaVrSM& operator=(
        const MetaVrSM& inSm);
};

template<typename ST>
    static inline ST&
operator<<(
    ST&                          inStream,
    const MetaVrSM::Config& inConfig)
{
    return inConfig.Insert(inStream);
}

template<typename ST>
    static inline ST&
operator>>(
    ST&               inStream,
    MetaVrSM::Config& inConfig)
{
    return inConfig.Extract(inStream);
}

} // namespace KFS

#endif /* META_VRSM_H */
