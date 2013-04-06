//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/22
// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
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
// 
//----------------------------------------------------------------------------

#ifndef _CLIENTSM_H
#define _CLIENTSM_H

#include <deque>
#include <list>
#include <map>
#include <algorithm>
#include <boost/functional/hash.hpp>
#include <tr1/unordered_map>

#include "kfsio/KfsCallbackObj.h"
#include "kfsio/NetConnection.h"
#include "kfsio/IOBuffer.h"
#include "common/StdAllocator.h"
#include "Chunk.h"
#include "RemoteSyncSM.h"
#include "KfsOps.h"
#include "BufferManager.h"

namespace KFS
{

// There is a dependency in waiting for a write-op to finish
// before we can execute a write-sync op. Use this struct to track
// such dependencies.
struct OpPair
{
    // once op is finished, we can then execute dependent op.
    OpPair(KfsOp* o, KfsOp* d)
        : op(0),
          dependentOp(d)
        {}
    KfsOp* op;
    KfsOp* dependentOp;
};

// For record appends when client reserves space within a chunk,
// we use a hash table to track the various reservation requests
// for a single client.  The hash table is keyed by <chunkId, transactionId>
struct ChunkSpaceReservationKey_t {
    ChunkSpaceReservationKey_t(kfsChunkId_t c, int64_t t) : 
        chunkId(c), transactionId(t) { }
    kfsChunkId_t chunkId;
    int64_t transactionId; // unique for each chunkserver
    bool operator==(const ChunkSpaceReservationKey_t &other) const {
        return chunkId == other.chunkId && transactionId == other.transactionId;
    }
};
static inline std::size_t hash_value(ChunkSpaceReservationKey_t const &csr) {
    boost::hash<int> h;
    return h(csr.transactionId);
}

typedef std::tr1::unordered_map<
    ChunkSpaceReservationKey_t, size_t, boost::hash<ChunkSpaceReservationKey_t>
> ChunkSpaceResMap;

class Properties;

// KFS client protocol state machine.
class ClientSM :
    public KfsCallbackObj,
    private BufferManager::Client
{
public:
    static void SetParameters(const Properties& prop);

    ClientSM(NetConnectionPtr &conn);
    ~ClientSM(); 

    //
    // Sequence:
    //  Client connects.
    //   - A new client sm is born
    //   - reads a request out of the connection
    //   - client says READ chunkid
    //   - request handler calls the disk manager to get the size
    //   -- the request handler then runs in a loop:
    //       -- in READ START: schedule a read for 4k; transition to READ DONE
    //       -- in READ DONE: data that was read arrives; 
    //            schedule that data to be sent out and transition back to READ START
    //       
    int HandleRequest(int code, void *data);

    // This is a terminal state handler.  In this state, we wait for
    // all outstanding ops to finish and then destroy this.
    int HandleTerminate(int code, void *data);

    // For daisy-chain writes, retrieve the server object for the
    // chunkserver running at the specified location.
    //
    RemoteSyncSMPtr FindServer(const ServerLocation &loc, bool connect = true);

    void ChunkSpaceReserve(kfsChunkId_t chunkId, int64_t writeId, int nbytes);

    void ReleaseChunkSpaceReservations();

    void ReleaseReservedSpace(kfsChunkId_t chunkId, int64_t writeId) {
        mReservations.erase(ChunkSpaceReservationKey_t(chunkId, writeId));
    }

    size_t UseReservedSpace(kfsChunkId_t chunkId, int64_t writeId, size_t nbytes) {
        ChunkSpaceResMap::iterator const iter = mReservations.find(
            ChunkSpaceReservationKey_t(chunkId, writeId));
        size_t ret = 0;
        if (iter != mReservations.end()) {
            ret = std::min(iter->second, nbytes);
            iter->second -= ret;
        }
        return ret;
    }
    size_t GetReservedSpace(kfsChunkId_t chunkId, int64_t writeId) const {
        // Cast until mac std::tr1::unordered_map gets "find() const"
        ChunkSpaceResMap::const_iterator const iter =
            const_cast<ChunkSpaceResMap&>(mReservations).find(
            ChunkSpaceReservationKey_t(chunkId, writeId));
        return (iter == mReservations.end() ? 0 : iter->second);
    }
    void ChunkSpaceReserve(kfsChunkId_t chunkId, int64_t writeId, size_t nbytes) {
        mReservations.insert(
            std::make_pair(ChunkSpaceReservationKey_t(chunkId, writeId), 0)
        ).first->second += nbytes;
    }
    virtual void Granted(ByteCount byteCount)
        { GrantedSelf(byteCount, false); }
private:
    typedef std::deque<std::pair<KfsOp*, ByteCount> > OpsQueue;
    typedef std::list<OpPair,
        StdFastAllocator<OpPair> > PendingOpsList;

    class DevBufferManagerClient : public BufferManager::Client
    {
    public:
        DevBufferManagerClient(ClientSM& client)
            : BufferManager::Client(),
              mClient(client)
            {}
        virtual ~DevBufferManagerClient()
            {}
        virtual void Granted(ByteCount byteCount)
            { mClient.GrantedSelf(byteCount, true); }
    private:
        ClientSM& mClient;
    private:
        DevBufferManagerClient(const DevBufferManagerClient&);
        DevBufferManagerClient& operator=(const DevBufferManagerClient&);
    };
    friend class DevBufferManagerClient;
    typedef std::map<
        const BufferManager*,
        DevBufferManagerClient*,
        std::less<const BufferManager*>,
        StdFastAllocator<pair<
            const BufferManager* const,
            DevBufferManagerClient*
        > >
    > DevBufferManagerClients;
    typedef StdFastAllocator<DevBufferManagerClient> DevClientMgrAllocator;

    NetConnectionPtr           mNetConnection;
    KfsOp*                     mCurOp;
    /// Queue of outstanding ops from the client.  We reply to ops in FIFO
    OpsQueue                   mOps;

    /// chunks for which the client has space reserved
    ChunkSpaceResMap           mReservations;

    /// Queue of pending ops: ops that depend on other ops to finish before we can execute them.
    PendingOpsList            mPendingOps;
    PendingOpsList            mPendingSubmitQueue;

    /// for writes, we daisy-chain the chunkservers in the forwarding path.  this list
    /// maintains the set of servers to which we have a connection.
    std::list<RemoteSyncSMPtr> mRemoteSyncers;
    ByteCount                  mPrevNumToWrite;
    int                        mRecursionCnt;
    int                        mDiscardByteCnt;
    const uint64_t             mInstanceNum;
    IOBuffer::WOStream         mWOStream;
    DevBufferManagerClients    mDevBufMgrClients;
    BufferManager*             mDevBufMgr;
    DevClientMgrAllocator      mDevCliMgrAllocator;

    static bool                sTraceRequestResponseFlag;
    static bool                sEnforceMaxWaitFlag;
    static bool                sCloseWriteOnPendingOverQuotaFlag;
    static int                 sMaxReqSizeDiscard;
    static uint64_t            sInstanceNum;

    /// Given a (possibly) complete op in a buffer, run it.
    /// @retval True if the command was handled (i.e., we have all the
    /// data and we could execute it); false otherwise.
    bool HandleClientCmd(IOBuffer *iobuf, int cmdLen);

    /// Op has finished execution.  Send a response to the client.
    void SendResponse(KfsOp *op);

    /// Submit ops that have been held waiting for doneOp to finish.
    void OpFinished(KfsOp *doneOp);
    bool GetWriteOp(KfsOp* wop, int align, int numBytes, IOBuffer* iobuf,
        IOBuffer*& ioOpBuf, bool forwardFlag);
    std::string GetPeerName();
    inline void SendResponse(KfsOp* op, ByteCount opBytes);
    inline static BufferManager& GetBufferManager();
    inline static BufferManager* FindDevBufferManager(KfsOp& op);
    inline Client* GetDevBufMgrClient(const BufferManager* bufMgr);
    inline void PutAndResetDevBufferManager(KfsOp& op, ByteCount opBytes);
    bool FailIfExceedsWait(
        BufferManager&         bufMgr,
        BufferManager::Client* mgrCli);
    void GrantedSelf(ByteCount byteCount, bool devBufManagerFlag);
private:
    // No copy.
    ClientSM(const ClientSM&);
    ClientSM& operator=(const ClientSM&);
};

}

#endif // _CLIENTSM_H
