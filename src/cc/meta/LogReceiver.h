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

#include "common/kfstypes.h"

#include <time.h>

namespace KFS
{

class Properties;
class NetManager;
class IOBuffer;
class MetaVrLogSeq;
struct ServerLocation;
struct MetaLogWriterControl;

class LogReceiver
{
public:
    class Waker
    {
    public:
        virtual void Wakeup() = 0;
    protected:
        Waker()
            {}
        virtual ~Waker()
            {}
        Waker(
            const Waker& /* inWaker */)
            {}
        Waker& operator=(
            const Waker& /* inWaker */)
            { return *this; }
    };
    LogReceiver();
    ~LogReceiver();
    bool Dispatch();
    bool SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters);
    int Start(
        NetManager&         inNetManager,
        Waker&              inWaker,
        const MetaVrLogSeq& inCommittedLogSeq,
        const MetaVrLogSeq& inLastLogSeq,
        int64_t             inFileSystemId,
        vrNodeId_t          inNodeId);
    void Shutdown();
    ServerLocation GetListenerAddress() const;
    static int ParseBlockLines(
        const IOBuffer&       inBuffer,
        int                   inLength,
        MetaLogWriterControl& inOp,
        int                   inLastSym);
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
