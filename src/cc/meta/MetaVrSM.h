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

class MetaVrStartViewChange;
class MetaVrDoViewChange;
class MetaVrStartView;
class MetaVrReconfiguration;
class MetaVrStartEpoch;
class Properties;

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
            Node()
                : mFlags(kFlagsNone),
                  mLocations()
                {}
            template<typename ST>
            ST& Insert(
                ST& inStream) const
            {
                inStream << mLocations.size() << " " << mFlags;
                for (Locations::const_iterator theIt = mLocations.begin();
                        mLocations.end() != theIt;
                        ++theIt) {
                    inStream << " " << *theIt;
                }
                return inStream;
            }
            template<typename ST>
            ST& Extract(
                ST& inStream)
            {
                Clear();
                size_t theSize;
                if (! (inStream >> theSize) || ! (inStream >> mFlags)) {
                    return inStream;
                }
                mLocations.reserve(theSize);
                while (mLocations.size() < theSize) {
                    ServerLocation theLocation;
                    if (! (inStream >> theLocation)) {
                        Clear();
                        break;
                    }
                    mLocations.push_back(theLocation);
                }
                return inStream;
            }
            void Clear()
            {
                mFlags = kFlagsNone;
                mLocations.clear();
            }
            const Locations& GetLocations() const
                { return mLocations; }
            Flags GetFlags() const
                { return mFlags; }
        private:
            Flags     mFlags;
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
            ST& inStream) const
        {
            inStream << mNodes.size();
            for (Nodes::const_iterator theIt = mNodes.begin();
                    mNodes.end() != theIt;
                    ++theIt) {
                inStream  << " " << theIt->first << " ";
                theIt->second.Insert(inStream);
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
                    if (theId < 0 && inStream) {
                        inStream.setstate(ST::failbit);
                    }
                    break;
                }
                mNodes.insert(make_pair(theId, theNode));
            }
            return inStream;
        }
        const Nodes& GetNodes() const
            { return mNodes; }
        bool Validate() const ;
    private:
        Nodes mNodes;
    };

    MetaVrSM();
    ~MetaVrSM();
    int HandleLogBlock(
        seq_t inEpochSeq,
        seq_t inViewSeq,
        seq_t inLogSeq,
        seq_t inBlockLenSeq,
        seq_t inCommitSeq);
    void Handle(
        MetaVrStartViewChange& inReq);
    void Handle(
        MetaVrDoViewChange& inReq);
    void Handle(
        MetaVrStartView& inReq);
    void Handle(
        MetaVrReconfiguration& inReq);
    void Handle(
        MetaVrStartEpoch& inReq);
    void HandleReply(
        MetaVrStartViewChange& inReq,
        seq_t                  inSeq,
        const Properties&      inProps);
    void HandleReply(
        MetaVrDoViewChange& inReq,
        seq_t               inSeq,
        const Properties&   inProps);
    void HandleReply(
        MetaVrStartView&  inReq,
        seq_t             inSeq,
        const Properties& inProps);
    void HandleReply(
        MetaVrReconfiguration& inReq,
        seq_t                  inSeq,
        const Properties&      inProps);
    void HandleReply(
        MetaVrStartEpoch& inReq,
        seq_t             inSeq,
        const Properties& inProps);
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
