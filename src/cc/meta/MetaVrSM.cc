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

#include "NetDispatch.h"

#include "qcdio/QCMutex.h"
#include "qcdio/qcstutils.h"

namespace KFS
{

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
          mConfig(),
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
    LogTransmitter& mLogTransmitter;
    MetaVrSM&       mMetaVrSM;
    QCMutex*        mMutexPtr;
    State           mState;
    NodeId          mNodeId;
    Config          mConfig;
    int             mQuorum;
    bool            mPrimaryFlag;

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
                Commit(inReq.logseq);
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
    }
    bool Handle(
        MetaVrStartEpoch& inReq)
    {
        return false;
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
