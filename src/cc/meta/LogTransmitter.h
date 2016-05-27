//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/05/05
// Author: Mike Ovsiannikov
//
// Copyright 2015 Quantcast Corp.
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
// Transaction log replication transmitter.
//
//
//----------------------------------------------------------------------------


#ifndef KFS_META_LOG_TRANSMITTER_H
#define KFS_META_LOG_TRANSMITTER_H

#include "common/kfstypes.h"

namespace KFS {

class Properties;
class NetManager;
class MetaVrRequest;
class MetaVrSM;

class LogTransmitter
{
public:
    class CommitObserver
    {
    public:
        virtual void Notify(
            seq_t inSeq) = 0;
    protected:
        CommitObserver()
            {}
        virtual ~CommitObserver()
            {}
    };
    LogTransmitter(
        NetManager&     inNetManager,
        CommitObserver& inCommitObserver);
    ~LogTransmitter();
    int SetParameters(
        const char*       inParamPrefixPtr,
        const Properties& inParameters);
    int TransmitBlock(
        seq_t       inViewSeq,
        seq_t       inBlockSeq,
        int         inBlockSeqLen,
        const char* inBlockPtr,
        size_t      inBlockLen,
        uint32_t    inChecksum,
        size_t      inChecksumStartPos);
    bool IsUp();
    void QueueVrRequest(
        MetaVrRequest& inVrReq);
    void Update(
        MetaVrSM& inMetaVrSM);
private:
    class Impl;

    Impl& mImpl;
private:
    LogTransmitter(
        const LogTransmitter& inTransmitter);
    LogTransmitter& operator=(
        const LogTransmitter& inTransmitter);
};

} // namespace KFS

#endif /* KFS_META_LOG_TRANSMITTER_H */
