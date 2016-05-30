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

namespace KFS
{

class MetaVrSM::Impl
{
public:
    Impl(
        LogTransmitter& inLogTransmitter)
        : mLogTransmitter(inLogTransmitter),
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
        return -EVRNOTPRIMARY;
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
    void Shutdown()
    {
    }
    const Config& GetConfig() const
        { return mConfig; }
    int GetQuorum() const
        { return mQuorum; }
    bool IsPrimary() const
        { return mPrimaryFlag; }
private:
    LogTransmitter& mLogTransmitter;
    Config          mConfig;
    int             mQuorum;
    bool            mPrimaryFlag;

    Impl(
        const Impl& inImpl);
    Impl operator=(
        const Impl& inImpl);
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
        return true;
    }
    bool Handle(
        MetaVrStartEpoch& inReq)
    {
        return true;
    }
};

MetaVrSM::MetaVrSM(
    LogTransmitter& inLogTransmitter)
    : mImpl(*(new Impl(inLogTransmitter)))
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
MetaVrSM::Shutdown()
{
    mImpl.Shutdown();
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
