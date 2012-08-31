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
// Chunk server "emulator" / stub implementation.
//
//----------------------------------------------------------------------------

#include "common/MsgLogger.h"
#include "common/kfsdecls.h"
#include "meta/MetaRequest.h"
#include "meta/util.h"
#include "ChunkServerEmulator.h"
#include "LayoutEmulator.h"

namespace KFS
{
using std::numeric_limits;

ChunkServerEmulator::ChunkServerEmulator(
    const ServerLocation& loc, int rack, const string& peerName)
    : TcpSocket(numeric_limits<int>::max()), // Fake fd, for IsGood()
      ChunkServer(NetConnectionPtr(
        new NetConnection(this, this, false, false)), peerName),
      mPendingReqs(),
      mOut(0)
{
    SetServerLocation(loc);
    SetRack(rack);
}

ChunkServerEmulator::~ChunkServerEmulator()
{
    if (mNetConnection) {
        mNetConnection->Close();
    }
    TcpSocket& sock = *this;
    sock = TcpSocket(); // Reset socket's fake fd.
    ChunkServerEmulator::FailPendingOps();
}

void
ChunkServerEmulator::EnqueueSelf(MetaChunkRequest* r)
{
    mPendingReqs.push_back(r);
}

size_t
ChunkServerEmulator::Dispatch()
{
    // Use index instead of iterator in order handle correctly Enqueue()
    // while iterating though the queue (though this isn't needed at the
    // time of writing).
    size_t i;
    for (i = 0; i < mPendingReqs.size(); i++) {
        MetaRequest*      const r  = mPendingReqs[i];
        MetaChunkRequest* const op = FindMatchingRequest(r->opSeqno);
        if (op != r) {
            panic("invalid request: not in the queue");
        }
        if (r->op == META_CHUNK_REPLICATE) {
            MetaChunkReplicate* const mcr = static_cast<MetaChunkReplicate*>(r);
            if (gLayoutEmulator.ChunkReplicationDone(mcr)) {
                KFS_LOG_STREAM_DEBUG <<
                    "moved chunk: " << mcr->chunkId <<
                    " to " << mcr->server->GetServerLocation() <<
                KFS_LOG_EOM;
                if (mOut) {
                    (*mOut) <<
                        mcr->chunkId << " " << mcr->server->GetServerLocation() <<
                    "\n";
                }
            }
        } else if (r->op == META_CHUNK_DELETE) {
            MetaChunkDelete* const mcd = static_cast<MetaChunkDelete*>(r);
            if (mNumChunks > 0) {
                mNumChunks--;
                mUsedSpace -= gLayoutEmulator.GetChunkSize(mcd->chunkId);
                if (mUsedSpace < 0 || mNumChunks <= 0) {
                    mUsedSpace = 0;
                }
                mAllocSpace = mUsedSpace;
            }
        } else {
            KFS_LOG_STREAM_ERROR << "unexpected op: " << r->Show() <<
            KFS_LOG_EOM;
        }
        delete r;
    }
    mPendingReqs.clear();
    return i;
}

void
ChunkServerEmulator::FailPendingOps()
{
    for (size_t i = 0; i < mPendingReqs.size(); i++) {
        MetaRequest*      const r  = mPendingReqs[i];
        MetaChunkRequest* const op = FindMatchingRequest(r->opSeqno);
        if (op != r) {
            panic("invalid request: not in the queue");
        }
        if (r->op == META_CHUNK_REPLICATE) {
            MetaChunkReplicate* const mcr = static_cast<MetaChunkReplicate*>(r);
            mcr->status = -EIO;
            gLayoutEmulator.ChunkReplicationDone(mcr);
        }
        delete r;
    }
    mPendingReqs.clear();
}

} // namespace KFS
