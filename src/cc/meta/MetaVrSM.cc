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

namespace KFS
{

class MetaVrSM::Impl
{
public:
    Impl()
        {}
    ~Impl()
        {}
    int HandleLogBlock(
        seq_t inEpochSeq,
        seq_t inViewSeq,
        seq_t inLogSeq,
        seq_t inBlockLenSeq,
        seq_t inCommitSeq)
    {
        return 0;
    }
    void Handle(
        MetaVrStartViewChange& inReq)
    {
    }
    void Handle(
        MetaVrDoViewChange& inReq)
    {
    }
    void Handle(
        MetaVrStartView& inReq)
    {
    }
    void Handle(
        MetaVrReconfiguration& inReq)
    {
    }
    void Handle(
        MetaVrStartEpoch& inReq)
    {
    }
    void HandleReply(
        MetaVrStartViewChange& inReq,
        seq_t                  inSeq,
        const Properties&      inProps)
    {
    }
    void HandleReply(
        MetaVrDoViewChange& inReq,
        seq_t               inSeq,
        const Properties&   inProps)
    {
    }
    void HandleReply(
        MetaVrStartView&  inReq,
        seq_t             inSeq,
        const Properties& inProps)
    {
    }
    void HandleReply(
        MetaVrReconfiguration& inReq,
        seq_t                  inSeq,
        const Properties&      inProps)
    {
    }
    void HandleReply(
        MetaVrStartEpoch&  inReq,
        seq_t              inSeq,
        const Properties&  inProps)
    {
    }
private:
    Impl(
        const Impl& inImpl);
    Impl operator=(
        const Impl& inImpl);
};

MetaVrSM::MetaVrSM()
    : mImpl(*(new Impl()))
{
}

MetaVrSM::~MetaVrSM()
{
    delete &mImpl;
}

    int 
MetaVrSM::HandleLogBlock(
    seq_t inEpochSeq,
    seq_t inViewSeq,
    seq_t inLogSeq,
    seq_t inBlockLenSeq,
    seq_t inCommitSeq)
{
    return mImpl.HandleLogBlock(
        inEpochSeq, inViewSeq, inLogSeq, inBlockLenSeq, inCommitSeq);
}

    void
MetaVrSM::Handle(
    MetaVrStartViewChange& inReq)
{
    mImpl.Handle(inReq);
}

    void
MetaVrSM::Handle(
    MetaVrDoViewChange& inReq)
{
    mImpl.Handle(inReq);
}

    void
MetaVrSM::Handle(
    MetaVrStartView& inReq)
{
    mImpl.Handle(inReq);
}

    void
MetaVrSM::Handle(
    MetaVrReconfiguration& inReq)
{
    mImpl.Handle(inReq);
}

    void
MetaVrSM::Handle(
    MetaVrStartEpoch& inReq)
{
    mImpl.Handle(inReq);
}

    void
MetaVrSM::HandleReply(
    MetaVrStartViewChange& inReq,
    seq_t                  inSeq,
    const Properties&      inProps)
{
    mImpl.HandleReply(inReq, inSeq, inProps);
}

    void
MetaVrSM::HandleReply(
    MetaVrDoViewChange& inReq,
    seq_t               inSeq,
    const Properties&   inProps)
{
    mImpl.HandleReply(inReq, inSeq, inProps);
}

    void
MetaVrSM::HandleReply(
    MetaVrStartView&  inReq,
    seq_t             inSeq,
    const Properties& inProps)
{
    mImpl.HandleReply(inReq, inSeq, inProps);
}

    void
MetaVrSM::HandleReply(
    MetaVrReconfiguration& inReq,
    seq_t                  inSeq,
    const Properties&      inProps)
{
    mImpl.HandleReply(inReq, inSeq, inProps);
}

    void
MetaVrSM::HandleReply(
    MetaVrStartEpoch& inReq,
    seq_t             inSeq,
    const Properties& inProps)
{
    mImpl.HandleReply(inReq, inSeq, inProps);
}

} // namespace KFS
