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

    LogTransmitter&              mLogTransmitter;
    MetaVrSM&                    mMetaVrSM;
    QCMutex*                     mMutexPtr;
    State                        mState;
    NodeId                       mNodeId;
    const MetaVrReconfiguration* mReconfigureReqPtr;
    Locations                    mPendingLocations;
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
        if (inReq.mLocationsCount <= 0) {
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
            case MetaVrReconfiguration::kOpTypeRemoveNode:
                RemoveNode(inReq);
                break;
            case MetaVrReconfiguration::kOpTypeModifyNode:
                ModifyNode(inReq);
                break;
            default:
                inReq.status    = -EINVAL;
                inReq.statusMsg = "invalid operation type";
                break;
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
        const char*       thePtr    = inReq.mLocationsStr.GetPtr();
        const char* const theEndPtr = thePtr + inReq.mLocationsStr.GetSize();
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
        if ((int)mPendingLocations.size() != inReq.mLocationsCount) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "add node: listeners list parse failure";
            mPendingLocations.clear();
            return;
        }
        mReconfigureReqPtr = &inReq;
    }
    void RemoveNode(
        MetaVrReconfiguration& inReq)
    {
    }
    void ModifyNode(
        MetaVrReconfiguration& inReq)
    {
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
        switch (inReq.mOpType)
        {
            case MetaVrReconfiguration::kOpTypeAddNode:
                CommitAddNode(inReq);
                break;
            case MetaVrReconfiguration::kOpTypeRemoveNode:
                CommitRemoveNode(inReq);
                break;
            case MetaVrReconfiguration::kOpTypeModifyNode:
                CommitModifyNode(inReq);
                break;
            default:
                panic("VR: invalid reconfiguration commit attempt");
                break;
        }
        mLogTransmitter.Update(mMetaVrSM);
    }
    void CommitAddNode(
        const MetaVrReconfiguration& inReq)
    {
        if ((int)mPendingLocations.size() != inReq.mLocationsCount) {
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
    void CommitRemoveNode(
        const MetaVrReconfiguration& inReq)
    {
    }
    void CommitModifyNode(
        const MetaVrReconfiguration& inReq)
    {
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
