//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/05/10
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
// Transaction log replication reciver.
//
//
//----------------------------------------------------------------------------


#ifndef KFS_META_LOG_RECEIVER_H
#define KFS_META_LOG_RECEIVER_H

#include "kfstypes.h"

class QCMutex;

namespace KFS
{

class Properties;
class NetManager;

class LogReceiver
{
public:
    class Replayer
    {
    public:
        virtual seq_t Apply(
            const char* inLinePtr,
            int         inLen) = 0;
    };
    LogReceiver();
    ~LogReceiver();
    bool SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters);
    void SetNextLogSeq(
        seq_t inSeq);
    int Start(
        NetManager& inNetManager,
        Replayer&   inReplayer,
        QCMutex*    inMutexPtr);
    void Shutdown();
private:
    class Impl;

    Impl& mImpl;
private:
    LogReceiver(
        const LogReceiver& inReceiver);
    LogReceiver& operator=(
        const LogReceiver& inReceiver);
};

} // namespace KFS

#endif /* KFS_META_LOG_RECEIVER_H */
