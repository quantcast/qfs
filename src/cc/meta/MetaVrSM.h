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

namespace KFS
{

class MetaVrStartViewChange;
class MetaVrDoViewChange;
class MetaVrStartView;
class MetaVrReconfiguration;
class MetaVrStartEpoch;
class Properties;

class MetaVrSM
{
public:
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

} // namespace KFS

#endif /* META_VRSM_H */
