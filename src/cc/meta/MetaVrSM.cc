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

#include "MetaVrSM.h"
#include "MetaVrOps.h"
#include "util.h"
#include "LogTransmitter.h"
#include "NetDispatch.h"

#include "qcdio/QCMutex.h"
#include "qcdio/qcstutils.h"

#include "common/IntToString.h"

#include <algorithm>

namespace KFS
{
using std::lower_bound;
using std::find;
using std::sort;
using std::unique;

class MetaVrSM::Impl
{
public:
    enum State
    {
        kStateNone            = 0,
        kStateReconfiguration = 1,
        kStatePrimary         = 2,
        kStateBackup          = 3
    };

    Impl(
        LogTransmitter& inLogTransmitter,
        MetaVrSM&       inMetaVrSM)
        : mLogTransmitter(inLogTransmitter),
          mMetaVrSM(inMetaVrSM),
          mMutexPtr(0),
          mState(kStateNone),
          mNodeId(-1),
          mReconfigureReqPtr(0),
          mPendingLocations(),
          mPendingNodeIds(),
          mConfig(),
          mLocations(),
          mQuorum(0)
        {}
    ~Impl()
        {}
    int HandleLogBlock(
        seq_t  inEpochSeq,
        seq_t  inViewSeq,
        seq_t  inLogSeq,
        seq_t& outEpochSeq,
        seq_t& outViewSeq)
    {
        outEpochSeq = -1;
        outViewSeq  = -1;
        return ((mConfig.IsEmpty() && mNodeId <= 0) ? 0 : -EVRNOTPRIMARY);
    }
    bool Handle(
        MetaRequest& inReq)
    {
        switch (inReq.op) {
            case META_VR_START_VIEW_CHANGE:
                return Handle(static_cast<MetaVrStartViewChange&>(inReq));
            case META_VR_DO_VIEW_CHANGE:
                return Handle(static_cast<MetaVrDoViewChange&>(inReq));
            case META_VR_START_VIEW:
                return Handle(static_cast<MetaVrStartView&>(inReq));
            case META_VR_RECONFIGURATION:
                return Handle(static_cast<MetaVrReconfiguration&>(inReq));
            case META_VR_START_EPOCH:
                return Handle(static_cast<MetaVrStartEpoch&>(inReq));
            default:
                break;
        }
        return false;
    }
    void HandleReply(
        MetaVrStartViewChange& inReq,
        seq_t                  inSeq,
        const Properties&      inProps,
        NodeId                 inNodeId)
    {
    }
    void HandleReply(
        MetaVrDoViewChange& inReq,
        seq_t               inSeq,
        const Properties&   inProps,
        NodeId              inNodeId)
    {
    }
    void HandleReply(
        MetaVrStartView&  inReq,
        seq_t             inSeq,
        const Properties& inProps,
        NodeId            inNodeId)
    {
    }
    void HandleReply(
        MetaVrReconfiguration& inReq,
        seq_t                  inSeq,
        const Properties&      inProps,
        NodeId                 inNodeId)
    {
        const char* const theMsgPtr =
            "VR: invalid reconfiguration reply handling attempt";
        KFS_LOG_STREAM_FATAL <<
            theMsgPtr <<
            " seq: "   << inSeq <<
            " props: " << inProps.size() <<
            " node: "  << inNodeId <<
            " "        << inReq <<
        KFS_LOG_EOM;
        panic(theMsgPtr);
    }
    void HandleReply(
        MetaVrStartEpoch& inReq,
        seq_t             inSeq,
        const Properties& inProps,
        NodeId            inNodeId)
    {
    }
    void SetLastLogReceivedTime(
        time_t inTime)
    {
    }
    void Process(
        time_t inTimeNow)
    {
    }
    void Start()
    {
    }
    void Shutdown()
    {
    }
    int SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters)
    {
        mNodeId = inParameters.getValue(kMetaVrNodeIdParameterNamePtr, -1);
        return 0;
    }
    void Commit(
        seq_t inLogSeq)
    {
        if (! mReconfigureReqPtr || inLogSeq < mReconfigureReqPtr->logseq) {
            return;
        }
        const MetaVrReconfiguration& theReq = *mReconfigureReqPtr;
        mReconfigureReqPtr = 0;
        Commit(theReq);
    }
    const Config& GetConfig() const
        { return mConfig; }
    int GetQuorum() const
        { return mQuorum; }
    bool IsPrimary() const
        { return mPrimaryFlag; }
    static const char* GetStateName(
        State inState)
    {
        switch (inState)
        {
            case kStateNone:
                return "none";
            case kStateReconfiguration:
                return "reconfiguration";
            case kStatePrimary:
                return "primary";
            case kStateBackup:
                return "backup";
            default: break;
        }
        return "invalid";
    }
private:
    typedef Config::Locations Locations;
    typedef vector<NodeId>    NodeIds;

    class NopFunc
    {
    public:
        bool operator()(
            int&                 /* ioStatus */,
            string&              /* ioErr */,
            NodeId               /* inId */,
            const Config::Nodes& /* inNodes */,
            const Config::Node&  /* inNode */) const
            { return true; }
    };

    class RmoveFunc
    {
    public:
        RmoveFunc(
            Impl& inImpl)
            : mImpl(inImpl)
            {}
        bool operator()(
            int&                 /* ioStatus */,
            string&              /* ioErr */,
            NodeId               inId,
            Config::Nodes&       inNodes,
            const Config::Node&  inNode) const
        {
            for (Locations::const_iterator
                    theIt = inNode.GetLocations().begin();
                    inNode.GetLocations().end() != theIt;
                    ++theIt) {
                mImpl.RemoveLocation(*theIt);
            }
            if (inNodes.erase(inId) <= 0) {
                panic("VR: remove node failure");
            }
            return true;
        }
    private:
        Impl& mImpl;
    };
    friend class RmoveFunc;

    class ChangeActiveFunc
    {
    public:
        ChangeActiveFunc(
            const bool inActivateFlag)
            : mActivateFlag(inActivateFlag)
            {}
        bool operator()(
            int&                 /* ioStatus */,
            string&              /* ioErr */,
            NodeId               /* inId */,
            const Config::Nodes& /* inNodes */,
            Config::Node&        inNode) const
        {
            if (mActivateFlag) {
                inNode.SetFlags(inNode.GetFlags() | Config::kFlagActive);
            } else {
                inNode.SetFlags(inNode.GetFlags() &
                    ~Config::Flags(Config::kFlagActive));
            }
            return true;
        }
    private:
        const bool mActivateFlag;
    };

    LogTransmitter&              mLogTransmitter;
    MetaVrSM&                    mMetaVrSM;
    QCMutex*                     mMutexPtr;
    State                        mState;
    NodeId                       mNodeId;
    const MetaVrReconfiguration* mReconfigureReqPtr;
    Locations                    mPendingLocations;
    NodeIds                      mPendingNodeIds;
    Config                       mConfig;
    Locations                    mLocations;
    int                          mQuorum;
    bool                         mPrimaryFlag;

    bool Handle(
        MetaVrStartViewChange& inReq)
    {
        return true;
    }
    bool Handle(
        MetaVrDoViewChange& inReq)
    {
        return true;
    }
    bool Handle(
        MetaVrStartView& inReq)
    {
        return true;
    }
    bool Handle(
        MetaVrReconfiguration& inReq)
    {
        if (0 != inReq.status) {
            if (inReq.replayFlag) {
                panic("VR: invalid reconfiguration request replay");
            }
            return true;
        }
        if (inReq.logseq <= 0) {
            if (inReq.replayFlag) {
                panic("VR: invalid reconfiguration request");
                inReq.status    = -EFAULT;
                inReq.statusMsg = "internal error";
                return true;
            }
            if (MetaRequest::kLogIfOk != inReq.logAction &&
                    MetaRequest::kLogAlways != inReq.logAction) {
                return false;
            }
            StartReconfiguration(inReq);
        } else {
            if (inReq.replayFlag) {
                StartReconfiguration(inReq);
                Commit(inReq);
            }
            // If not replay, then prepare and commit are handled by the log
            // writer.
        }
        return (0 != inReq.status);
    }
    void StartReconfiguration(
        MetaVrReconfiguration& inReq)
    {
        if (kStatePrimary != mState && kStateBackup != mState) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "reconfiguration is not possible, state: ";
            inReq.statusMsg += GetStateName(mState);
            return;
        }
        if (inReq.mListSize <= 0) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "add node: no listeners specified";
            return;
        }
        if (mReconfigureReqPtr) {
            inReq.status    = -EAGAIN;
            inReq.statusMsg = "reconfiguration is in progress";
            return;
        }
        switch (inReq.mOpType)
        {
            case MetaVrReconfiguration::kOpTypeAddNode:
                AddNode(inReq);
                break;
            case MetaVrReconfiguration::kOpTypeRemoveNodes:
                RemoveNodes(inReq);
                break;
            case MetaVrReconfiguration::kOpTypeActivateNodes:
                ModifyActiveStatus(inReq, true);
                break;
            case MetaVrReconfiguration::kOpTypeInactivateNodes:
                ModifyActiveStatus(inReq, false);
                break;
            default:
                inReq.status    = -EINVAL;
                inReq.statusMsg = "invalid operation type";
                break;
        }
        if (0 == inReq.status) {
            mReconfigureReqPtr = &inReq;
        }
    }
    void AddNode(
        MetaVrReconfiguration& inReq)
    {
        const Config::Nodes& theNodes = mConfig.GetNodes();
        if (theNodes.find(inReq.mNodeId) != theNodes.end()) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "add node: node already exists";
            return;
        }
        if (0 != (inReq.mNodeFlags & Config::kFlagActive)) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "add node: node active flag must not set";
            return;
        }
        mPendingLocations.clear();
        const char*       thePtr    = inReq.mListStr.GetPtr();
        const char* const theEndPtr = thePtr + inReq.mListStr.GetSize();
        ServerLocation    theLocation;
        while (thePtr < theEndPtr) {
            if (theLocation.ParseString(
                    thePtr, theEndPtr - thePtr, inReq.shortRpcFormatFlag)) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "add node: node active flag must not set";
                mPendingLocations.clear();
                return;
            }
            if (HasLocation(theLocation)) {
                inReq.status = -EINVAL;
                inReq.statusMsg = "add node: litener: ";
                inReq.statusMsg += theLocation.ToString();
                inReq.statusMsg += " already assigned to ";
                AppendDecIntToString(
                    inReq.statusMsg, FindNodeByLocation(theLocation));
                mPendingLocations.clear();
                return;
            }
            mPendingLocations.push_back(theLocation);
        }
        if ((int)mPendingLocations.size() != inReq.mListSize) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "add node: listeners list parse failure";
            mPendingLocations.clear();
            return;
        }
    }
    void RemoveNodes(
        MetaVrReconfiguration& inReq)
    {
        if (inReq.mListSize <= 0) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "change active status: empty node id list";
            return;
        }
        if (! ParseNodeIdList(inReq)) {
            return;
        }
        const bool kActiveFlag = false;
        ApplyT(inReq, kActiveFlag, mConfig.GetNodes(), NopFunc());
    }
    void ModifyActiveStatus(
        MetaVrReconfiguration& inReq,
        bool                   inActivateFlag)
    {
        if (inReq.mListSize <= 0) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "change active status: empty node id list";
            return;
        }
        if (! ParseNodeIdList(inReq)) {
            return;
        }
        ApplyT(inReq, ! inActivateFlag, mConfig.GetNodes(), NopFunc());
    }
    template<typename NT, typename T>
    void ApplyT(
        int&     outStatus,
        string&  outErr,
        bool     inActiveFlag,
        NT&      inNodes,
        const T& inFunc)
    {
        for (NodeIds::const_iterator theIt = mPendingNodeIds.begin();
                mPendingNodeIds.end() != theIt;
                ++theIt) {
            typename NT::iterator const theNodeIt = inNodes.find(*theIt);
            if (inNodes.end() == theNodeIt) {
                outStatus = -EINVAL;
                outErr    = "change active status: no such node: ";
                AppendDecIntToString(outErr, *theIt);
                return;
            }
            Config::Node& theNode = theNodeIt->second;
            if ((0 != (theNode.GetFlags() & Config::kFlagActive)) !=
                    inActiveFlag) {
                outStatus = -EINVAL;
                outErr    = inActiveFlag ?
                    "node not active: " : "node active: ";
                AppendDecIntToString(outErr, *theIt);
                return;
            }
            if (! inFunc(outStatus, outErr, *theIt, inNodes, theNode)) {
                return;
            }
        }
    }
    template<typename RT, typename NT, typename T>
    void ApplyT(
        RT&      inReq,
        bool     inActiveFlag,
        NT&      inNodes,
        const T& inFunc)
    {
        ApplyT(inReq.status, inReq.statusMsg, inActiveFlag, inNodes, inFunc);
    }
    template<typename NT, typename T>
    void ApplyT(
        bool     inActiveFlag,
        NT&      inNodes,
        const T& inFunc)
    {
        int    status;
        string statusMsg;
        ApplyT(status, statusMsg, inActiveFlag, inNodes, inFunc);
        if (0 != status) {
            panic("VR: " + statusMsg);
        }
    }
    bool ParseNodeIdList(
        MetaVrReconfiguration& inReq)
    {
        mPendingNodeIds.clear();
        const char*       thePtr    = inReq.mListStr.GetPtr();
        const char* const theEndPtr = thePtr + inReq.mListStr.GetSize();
        while (thePtr < theEndPtr) {
            NodeId theNodeId = -1;
            if (! inReq.ParseInt(thePtr, theEndPtr - thePtr, theNodeId) ||
                    theNodeId < 0) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "invalid node id";
                mPendingNodeIds.clear();
                return false;
            }
            mPendingNodeIds.push_back(theNodeId);
        }
        if ((int)mPendingNodeIds.size() != inReq.mListSize) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "node id list size mismatch";
            mPendingNodeIds.clear();
            return false;
        }
        sort(mPendingNodeIds.begin(), mPendingNodeIds.end());
        if (mPendingNodeIds.end() !=
                unique(mPendingNodeIds.begin(), mPendingNodeIds.end())) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "invalid duplicate node id";
            mPendingNodeIds.clear();
            return false;
        }
        return true;
    }
    bool Handle(
        MetaVrStartEpoch& inReq)
    {
        return false;
    }
    void Commit(
        const MetaVrReconfiguration& inReq)
    {
        if (0 != inReq.status) {
            return;
        }
        switch (inReq.mOpType) {
            case MetaVrReconfiguration::kOpTypeAddNode:
                CommitAddNode(inReq);
                break;
            case MetaVrReconfiguration::kOpTypeRemoveNodes:
                CommitRemoveNodes(inReq);
                break;
            case MetaVrReconfiguration::kOpTypeActivateNodes:
                CommitModifyActiveStatus(inReq, true);
                break;
            case MetaVrReconfiguration::kOpTypeInactivateNodes:
                CommitModifyActiveStatus(inReq, false);
                break;
            default:
                panic("VR: invalid reconfiguration commit attempt");
                break;
        }
        if (0 != inReq.status) {
            panic("VR: reconfiguration commit failure");
        }
        mLogTransmitter.Update(mMetaVrSM);
    }
    void CommitAddNode(
        const MetaVrReconfiguration& inReq)
    {
        if ((int)mPendingLocations.size() != inReq.mListSize) {
            panic("VR: commit add node: invalid locations");
            return;
        }
        if (! mConfig.AddNode(inReq.mNodeId, Config::Node(
                inReq.mNodeFlags, inReq.mPrimaryOrder, mPendingLocations))) {
            panic("VR: commit add node: duplicate node id");
            return;
        }
        for (Locations::const_iterator theIt = mPendingLocations.begin();
                mPendingLocations.end() != theIt;
                ++theIt) {
            AddLocation(*theIt);
        }
    }
    void CommitRemoveNodes(
        const MetaVrReconfiguration& inReq)
    {
        const bool kActiveFlag = false;
        ApplyT(kActiveFlag, mConfig.GetNodes(), RmoveFunc(*this));
    }
    void CommitModifyActiveStatus(
        const MetaVrReconfiguration& inReq,
        bool                         inActivateFlag)
    {
        ApplyT(! inActivateFlag, mConfig.GetNodes(),
            ChangeActiveFunc(inActivateFlag));
    }
    bool HasLocation(
        const ServerLocation& inLocation) const
    {
        Locations::const_iterator const theIt = lower_bound(
            mLocations.begin(), mLocations.end(), inLocation);
        return (theIt != mLocations.end() && inLocation == *theIt);
    }
    void AddLocation(
        const ServerLocation& inLocation)
    {
        mLocations.insert(lower_bound(
            mLocations.begin(), mLocations.end(), inLocation), inLocation);
    }
    bool RemoveLocation(
        const ServerLocation& inLocation)
    {
        Locations::iterator const theIt = lower_bound(
            mLocations.begin(), mLocations.end(), inLocation);
        if (mLocations.end() == theIt || inLocation != *theIt) {
            return false;
        }
        mLocations.erase(theIt);
        return true;
    }
    NodeId FindNodeByLocation(
        const ServerLocation& inLocation)
    {
        const Config::Nodes& theNodes = mConfig.GetNodes();
        for (Config::Nodes::const_iterator theIt = theNodes.begin();
                theNodes.end() != theIt;
                ++theIt) {
            const Config::Locations& theLocations =
                theIt->second.GetLocations();
            if (find(theLocations.begin(), theLocations.end(), inLocation) !=
                    theLocations.end()) {
                return theIt->first;
            }
        }
        return -1;
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

MetaVrSM::MetaVrSM(
    LogTransmitter& inLogTransmitter)
    : mImpl(*(new Impl(inLogTransmitter, *this)))
{
}

MetaVrSM::~MetaVrSM()
{
    delete &mImpl;
}

    int 
MetaVrSM::HandleLogBlock(
    seq_t  inLogSeq,
    seq_t  inBlockLenSeq,
    seq_t  inCommitSeq,
    seq_t& outEpochSeq,
    seq_t& outViewSeq)
{
    return mImpl.HandleLogBlock(
        inLogSeq, inBlockLenSeq, inCommitSeq, outEpochSeq, outViewSeq);
}

    bool
MetaVrSM::Handle(
    MetaRequest& inReq)
{
    return mImpl.Handle(inReq);
}

    void
MetaVrSM::HandleReply(
    MetaVrStartViewChange& inReq,
    seq_t                  inSeq,
    const Properties&      inProps,
    MetaVrSM::NodeId       inNodeId)
{
    mImpl.HandleReply(inReq, inSeq, inProps, inNodeId);
}

    void
MetaVrSM::HandleReply(
    MetaVrDoViewChange& inReq,
    seq_t               inSeq,
    const Properties&   inProps,
    MetaVrSM::NodeId    inNodeId)
{
    mImpl.HandleReply(inReq, inSeq, inProps, inNodeId);
}

    void
MetaVrSM::HandleReply(
    MetaVrStartView&  inReq,
    seq_t             inSeq,
    const Properties& inProps,
    MetaVrSM::NodeId  inNodeId)
{
    mImpl.HandleReply(inReq, inSeq, inProps, inNodeId);
}

    void
MetaVrSM::HandleReply(
    MetaVrReconfiguration& inReq,
    seq_t                  inSeq,
    const Properties&      inProps,
    MetaVrSM::NodeId       inNodeId)
{
    mImpl.HandleReply(inReq, inSeq, inProps, inNodeId);
}

    void
MetaVrSM::HandleReply(
    MetaVrStartEpoch& inReq,
    seq_t             inSeq,
    const Properties& inProps,
    MetaVrSM::NodeId  inNodeId)
{
    mImpl.HandleReply(inReq, inSeq, inProps, inNodeId);
}

    void
MetaVrSM::SetLastLogReceivedTime(
    time_t inTime)
{
    mImpl.SetLastLogReceivedTime(inTime);
}

    void
MetaVrSM::Process(
    time_t inTimeNow)
{
    mImpl.Process(inTimeNow);
}

    void
MetaVrSM::Start()
{
    mImpl.Start();
}

    void
MetaVrSM::Shutdown()
{
    mImpl.Shutdown();
}

    void
MetaVrSM::Commit(
    seq_t inLogSeq)
{
    mImpl.Commit(inLogSeq);
}

    int
MetaVrSM::SetParameters(
    const char*       inPrefixPtr,
    const Properties& inParameters)
{
    return mImpl.SetParameters(inPrefixPtr, inParameters);
}

    const MetaVrSM::Config&
MetaVrSM::GetConfig() const
{
    return mImpl.GetConfig();
}

    int
MetaVrSM::GetQuorum() const
{
    return mImpl.GetQuorum();
}

    bool
MetaVrSM::IsPrimary() const
{
    return mImpl.IsPrimary();
}

} // namespace KFS
