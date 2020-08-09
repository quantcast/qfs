//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/08/27
//
// Author: Sriram Rao
//
// Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
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
// Chunk server "emulator" / stub implementation.
//
//----------------------------------------------------------------------------

#include "ChunkServerEmulator.h"
#include "LayoutEmulator.h"

#include "common/MsgLogger.h"

#include "meta/MetaRequest.h"
#include "meta/util.h"

namespace KFS
{
using std::numeric_limits;

ChunkServerEmulator::ChunkServerEmulator(
    const ServerLocation& loc,
    int                   rack,
    LayoutEmulator&       emulator)
    : TcpSocket(numeric_limits<int>::max()), // Fake fd, for IsGood()
      ChunkServer(NetConnectionPtr(
        new NetConnection(this, this, false, false)), "emulator"),
      mPendingReqs(),
      mOut(0),
      mLayoutEmulator(emulator),
      mDispatchRecursionCount(0)
{
    SetServerLocation(loc);
    SetRack(rack);
}

ChunkServerEmulator::~ChunkServerEmulator()
{
    if (0 != mDispatchRecursionCount) {
        panic("invalid recursion count");
    }
    if (mNetConnection) {
        mNetConnection->Close();
    }
    DetachFd(); // Reset socket's fake fd.
    for (PendingReqs::const_iterator it = mPendingReqs.begin();
            mPendingReqs.end() != it;
            ++it) {
        MetaRequest::Release(*it);
    }
    mDispatchRecursionCount--; // To catch double delete.
}

void
ChunkServerEmulator::Init(int64_t totalSpace, int64_t usedSpace,
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
    mNumChunks  = GetChunkCount();
    mUsedSpace  = mNumChunks * (int64_t)CHUNKSIZE;
    mAllocSpace = mUsedSpace;
    if (! mSelfPtr) {
        mSelfPtr = shared_from_this();
    }
}

void
ChunkServerEmulator::Enqueue(MetaChunkRequest& req,
    int timeout, bool staleChunkIdFlag, bool loggedFlag, bool removeReplicaFlag,
    chunkId_t addChunkIdInFlight)
{
    if (0 != mDispatchRecursionCount) {
        panic("dispatch recursion");
    }
    KFS_LOG_STREAM_DEBUG <<
        "enqueue:"
        " timeout: "    << timeout <<
        " staleChunk: " << staleChunkIdFlag <<
        " logged: "     << loggedFlag <<
        " remove: "     << removeReplicaFlag <<
        " up: "         << (mNetConnection ? 1 : 0) <<
        " "             << req.Show() <<
    KFS_LOG_EOM;
    if (mNetConnection && META_CHUNK_HEARTBEAT != req.op) {
        mPendingReqs.push_back(&req);
    } else {
        MetaRequest::Release(&req);
    }
}

size_t
ChunkServerEmulator::Dispatch()
{
    mDispatchRecursionCount++;
    for (PendingReqs::const_iterator it = mPendingReqs.begin();
            mPendingReqs.end() != it;
            ++it) {
        MetaRequest& req = **it;
        if (! mNetConnection) {
            MetaRequest::Release(&req);
            continue;
        }
        if (req.op == META_CHUNK_REPLICATE) {
            MetaChunkReplicate& mcr = static_cast<MetaChunkReplicate&>(req);
            if (mLayoutEmulator.Handle(mcr)) {
                KFS_LOG_STREAM_DEBUG <<
                    "moved chunk: " << mcr.chunkId <<
                    " to " << mcr.server->GetServerLocation() <<
                KFS_LOG_EOM;
                if (mOut) {
                    (*mOut) <<
                        mcr.chunkId << " " << mcr.server->GetServerLocation() <<
                    "\n";
                }
            }
        } else if (req.op == META_CHUNK_DELETE) {
            MetaChunkDelete& mcd = static_cast<MetaChunkDelete&>(req);
            if (mLayoutEmulator.Handle(mcd) && 0 < mNumChunks) {
                mNumChunks--;
                mUsedSpace -= mLayoutEmulator.GetChunkSize(mcd.chunkId);
                if (mUsedSpace < 0 || mNumChunks <= 0) {
                    mUsedSpace = 0;
                }
                mAllocSpace = mUsedSpace;
            }
        } else {
            KFS_LOG_STREAM_FATAL <<
                "unexpected op: " << req.Show() <<
            KFS_LOG_EOM;
            panic("unexpected op");
        }
        MetaRequest::Release(&req);
    }
    const size_t cnt = mPendingReqs.size();
    mPendingReqs.clear();
    mDispatchRecursionCount--;
    return cnt;
}

} // namespace KFS
