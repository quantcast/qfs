//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2016/1/11
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
// Transaction log and checkpoint retrieval.
//
//
//----------------------------------------------------------------------------

#ifndef KFS_META_DATA_SYNC_H
#define KFS_META_DATA_SYNC_H

#include "LogReceiver.h"

#include "common/kfsdecls.h"

#include <vector>

namespace KFS
{
using std::vector;

class Properties;
class NetManager;
class MetaVrLogSeq;

class MetaDataSync
{
public:
    typedef vector<ServerLocation> Servers;

    MetaDataSync(
        NetManager& inNetManager);
    ~MetaDataSync();

    int SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters);
    int Start(
        const char* inCheckpointDirPtr,
        const char* inLogDirPtr);
    void Shutdown();
    void StartLogSync(
        const MetaVrLogSeq&    inLogSeq,
        LogReceiver::Replayer& inReplayer);
    void ScheduleLogSync(
        const Servers&      inServers,
        const MetaVrLogSeq& inLogStartSeq,
        const MetaVrLogSeq& inLogEndSeq);
private:
    class Impl;
    Impl& mImpl;
private:
    MetaDataSync(
        const MetaDataSync& inMetaDataSync);
    MetaDataSync& operator=(
        const MetaDataSync& inMetaDataSync);
};

}

#endif /* KFS_META_DATA_SYNC_H */
