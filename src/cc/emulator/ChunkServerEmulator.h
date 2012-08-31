//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/08/27
//
// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
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
// \brief An emulator for a chunk server, that only "emulates" chunk replication
// and recovery to make layout emulator work.
//
//----------------------------------------------------------------------------

#ifndef EMULATOR_CHUNKSERVEREMULATOR_H
#define EMULATOR_CHUNKSERVEREMULATOR_H

#include <string>
#include <vector>

#include "meta/ChunkServer.h"
#include "kfsio/TcpSocket.h"

namespace KFS
{
using std::vector;
using std::ostream;

class ChunkServerEmulator : private TcpSocket, public ChunkServer
{
public:
    ChunkServerEmulator(
        const ServerLocation& loc, int rack, const string& peerName);
    virtual ~ChunkServerEmulator();

    size_t Dispatch();
    // when this emulated server goes down, fail the pending ops
    // that were destined to this node
    void FailPendingOps();
    void HostingChunk(kfsChunkId_t /* chunkId */, size_t chunksize)
    {
        mNumChunks++;
        mUsedSpace += chunksize;
        mAllocSpace = mUsedSpace;
    }
    void SetRebalancePlanOutFd(ostream* os)
    {
        mOut = os;
    }
    void InitSpace(int64_t totalSpace, int64_t usedSpace,
            bool useFsTotalSpaceFlag)
    {
        if (useFsTotalSpaceFlag) {
            mTotalFsSpace = totalSpace;
            if (totalSpace > usedSpace) {
                mTotalSpace = totalSpace - usedSpace;
            } else {
                mTotalSpace = 0;
            }
        } else {
            mTotalFsSpace = totalSpace;
            mTotalSpace   = totalSpace;
        }
        mAllocSpace = 0;
        mUsedSpace  = 0;
    }

protected:
    virtual void EnqueueSelf(MetaChunkRequest* r);

private:
    typedef vector<MetaChunkRequest*> PendingReqs;
    PendingReqs mPendingReqs;
    ostream*    mOut;
private:
    ChunkServerEmulator(const ChunkServerEmulator&);
    ChunkServerEmulator& operator=(const ChunkServerEmulator&);
};

}

#endif // EMULATOR_CHUNKSERVEREMULATOR_H
