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

#include "common/LinearHash.h"
#include "kfsio/KfsCallbackObj.h"
#include "kfsio/NetConnection.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/SslFilter.h"
#include "kfsio/DelegationToken.h"
#include "common/StdAllocator.h"
#include "Chunk.h"
#include "RemoteSyncSM.h"
#include "KfsOps.h"
#include "BufferManager.h"

namespace KFS
{

using std::min;
using std::deque;
using std::pair;
using std::list;
using std::string;

class Properties;

// KFS client protocol state machine.
class ClientSM :
    public KfsCallbackObj,
    private BufferManager::Client,
    public SslFilterServerPsk
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
    RemoteSyncSMPtr FindServer(
        const ServerLocation& location,
        bool                  connectFlag,
        const char*           sessionTokenPtr,
        int                   sessionTokenLen,
        const char*           sessionKeyPtr,
        int                   sessionKeyLen,
        bool                  writeMasterFlag,
        bool                  shutdownSslFlag,
        int&                  err,
        string&               errMsg);

    void ReleaseReservedSpace(kfsChunkId_t chunkId, int64_t writeId)
        { mReservations.Erase(SpaceResKey(chunkId, writeId)); }
    size_t UseReservedSpace(kfsChunkId_t chunkId, int64_t writeId, size_t nbytes)
    {
        size_t* const val = mReservations.Find(SpaceResKey(chunkId, writeId));
        size_t        ret = 0;
        if (val) {
            ret = min(*val, nbytes);
            *val -= ret;
        }
        return ret;
    }
    size_t GetReservedSpace(kfsChunkId_t chunkId, int64_t writeId) const
    {
        size_t* const val = mReservations.Find(SpaceResKey(chunkId, writeId));
        return (val ? *val : size_t(0));
    }
    void ChunkSpaceReserve(kfsChunkId_t chunkId, int64_t writeId, size_t nbytes)
    {
        bool insertedFlag = false;
        *mReservations.Insert(
            SpaceResKey(chunkId, writeId), size_t(0), insertedFlag
        ) += nbytes;
    }
    virtual void Granted(ByteCount byteCount)
        { GrantedSelf(byteCount, false); }
    bool CheckAccess(KfsOp& op);
    bool CheckAccess(KfsClientChunkOp& op);
private:
    typedef deque<KfsOp*> OpsQueue;
    // There is a dependency in waiting for a write-op to finish
    // before we can execute a write-sync op. Use this struct to track
    // such dependencies.
    struct OpPair
    {
        // once op is finished, we can then execute dependent op.
        OpPair(KfsOp* o, KfsOp* d)
            : op(o),
              dependentOp(d)
            {}
        KfsOp* op;
        KfsOp* dependentOp;
    };
    typedef list<
        OpPair,
        StdFastAllocator<OpPair>
    > PendingOpsList;

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
    // For record appends when client reserves space within a chunk,
    // we use a hash table to track the various reservation requests
    // for a single client.  The hash table is keyed by <chunkId, transactionId>
    struct SpaceResKey
    {
        SpaceResKey(kfsChunkId_t c, int64_t t)
            : chunkId(c),
              transactionId(t)
            {}
        kfsChunkId_t chunkId;
        int64_t      transactionId; // unique for each chunkserver
    };
    struct SpaceResKeyCompare
    {
        static bool Equals(
            const SpaceResKey& inLhs,
            const SpaceResKey& inRhs)
        {
            return (inLhs.chunkId == inRhs.chunkId &&
                inLhs.transactionId == inRhs.transactionId);
        }
        static bool Less(
            const SpaceResKey& inLhs,
            const SpaceResKey& inRhs)
        {
            return (
                inLhs.chunkId < inRhs.chunkId || (
                    inLhs.chunkId == inRhs.chunkId &&
                    inLhs.transactionId < inRhs.transactionId)
            );
        }
        static size_t Hash(
            const SpaceResKey& inVal)
            { return (size_t)inVal.transactionId; }
    };
    typedef KVPair<SpaceResKey, size_t> SpaceResEntry;
    typedef LinearHash<
        SpaceResEntry,
        SpaceResKeyCompare,
        DynamicArray<
            SingleLinkedList<SpaceResEntry>*,
            5 // 32 entries to start with
        >,
        StdFastAllocator<SpaceResEntry>
    > ChunkSpaceResMap;
    friend class DevBufferManagerClient;
    typedef KVPair<
        const BufferManager*,
        DevBufferManagerClient*
    > DevBufMsrEntry;
    typedef LinearHash<
        DevBufMsrEntry,
        KeyCompare<const BufferManager*>,
        DynamicArray<
            SingleLinkedList<DevBufMsrEntry>*,
            5 // 32 entries to start with
        >,
        StdFastAllocator<DevBufMsrEntry>
    > DevBufferManagerClients;
    typedef StdFastAllocator<DevBufferManagerClient> DevClientMgrAllocator;

    NetConnectionPtr const     mNetConnection;
    KfsOp*                     mCurOp;
    /// Queue of outstanding ops "depending" (no reply) from the client.
    OpsQueue                   mOps;

    /// chunks for which the client has space reserved
    ChunkSpaceResMap           mReservations;

    /// Queue of pending ops: ops that depend on other ops to finish before we can execute them.
    PendingOpsList             mPendingOps;
    PendingOpsList             mPendingSubmitQueue;

    /// for writes, we daisy-chain the chunkservers in the forwarding path.  this list
    /// maintains the set of servers to which we have a connection.
    RemoteSyncSMList           mRemoteSyncers;
    ByteCount                  mPrevNumToWrite;
    int                        mRecursionCnt;
    int                        mDiscardByteCnt;
    const uint64_t             mInstanceNum;
    IOBuffer::IStream          mIStream;
    IOBuffer::WOStream         mWOStream;
    DevBufferManagerClients    mDevBufMgrClients;
    BufferManager*             mDevBufMgr;
    bool                       mGrantedFlag;
    int                        mInFlightOpCount;
    DevClientMgrAllocator      mDevCliMgrAllocator;
    bool                       mDataReceivedFlag;
    DelegationToken            mDelegationToken;

    static bool                sTraceRequestResponseFlag;
    static bool                sEnforceMaxWaitFlag;
    static bool                sSslPskEnabledFlag;
    static int                 sMaxReqSizeDiscard;
    static size_t              sMaxAppendRequestSize;
    static uint64_t            sInstanceNum;

    void ReleaseChunkSpaceReservations();
    /// Given a (possibly) complete op in a buffer, run it.
    /// @retval True if the command was handled (i.e., we have all the
    /// data and we could execute it); false otherwise.
    bool HandleClientCmd(IOBuffer& iobuf, int cmdLen);

    /// Op has finished execution.  Send a response to the client.
    void SendResponseSelf(KfsOp& op);

    /// Submit ops that have been held waiting for doneOp to finish.
    void OpFinished(KfsOp* doneOp);
    bool Discard(IOBuffer& iobuf);
    bool GetWriteOp(KfsOp& op, int align, int numBytes, IOBuffer& iobuf,
        IOBuffer& ioOpBuf, bool forwardFlag);
    string GetPeerName();
    inline void SendResponse(KfsOp& op);
    inline static BufferManager& GetBufferManager();
    inline static BufferManager* FindDevBufferManager(KfsOp& op);
    inline Client* GetDevBufMgrClient(const BufferManager* bufMgr);
    inline void PutAndResetDevBufferManager(KfsOp& op, ByteCount opBytes);
    bool FailIfExceedsWait(
        BufferManager&         bufMgr,
        BufferManager::Client* mgrCli);
    void GrantedSelf(ByteCount byteCount, bool devBufManagerFlag);
    virtual unsigned long GetPsk(
        const char*    inIdentityPtr,
	unsigned char* inPskBufferPtr,
        unsigned int   inPskBufferLen,
        string&        outAuthName);
private:
    // No copy.
    ClientSM(const ClientSM&);
    ClientSM& operator=(const ClientSM&);
};

}

#endif // _CLIENTSM_H
