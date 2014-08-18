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

#ifndef CHUNK_CLIENTSM_H
#define CHUNK_CLIENTSM_H

#include "Chunk.h"
#include "RemoteSyncSM.h"
#include "KfsOps.h"
#include "BufferManager.h"

#include "qcdio/QCDLList.h"

#include "common/LinearHash.h"
#include "common/StdAllocator.h"

#include "kfsio/KfsCallbackObj.h"
#include "kfsio/NetConnection.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/SslFilter.h"
#include "kfsio/DelegationToken.h"

#include <deque>
#include <list>
#include <map>
#include <algorithm>
#include <vector>

namespace KFS
{

using std::min;
using std::deque;
using std::pair;
using std::list;
using std::string;
using std::vector;

class ClientSM;
class Properties;
class ClientThread;
class ClientThreadListEntry
{
private:
    enum {
        kDispatchQueueIdx   = 0,
        kDispatchQueueCount = 1
    };
    typedef QCDLList<ClientThreadListEntry, kDispatchQueueIdx> DispatchQueue;
protected:
    ClientThreadListEntry(
        ClientThread* inClientThreadPtr)
        : mClientThreadPtr(inClientThreadPtr),
          mOpsHeadPtr(0),
          mOpsTailPtr(0),
          mReceivedOpPtr(0),
          mBlocksChecksums(),
          mChecksum(0),
          mFirstChecksumBlockLen(CHECKSUM_BLOCKSIZE),
          mReceiveByteCount(-1),
          mReceivedHeaderLen(0),
          mGrantedFlag(false),
          mReceiveOpFlag(false),
          mComputeChecksumFlag(false)
        { DispatchQueue::Init(*this); }
    ~ClientThreadListEntry();
    void ReceiveClear()
    {
        if (! mClientThreadPtr) {
            return;
        }
        mFirstChecksumBlockLen = CHECKSUM_BLOCKSIZE;
        mReceiveByteCount      = -1;
        mReceivedHeaderLen     = 0;
        mReceiveOpFlag         = false;
        mComputeChecksumFlag   = false;
        mReceivedOpPtr         = 0;
        mChecksum              = 0;
        mBlocksChecksums.clear();
    }
    void SetReceiveOp()
    {
        if (! mClientThreadPtr) {
            return;
        }
        ReceiveClear();
        mReceiveOpFlag = true;
    }
    void SetReceiveContent(
        int     inLength,
        bool    inComputeChecksumFlag,
        int32_t inFirstCheckSumBlockLen = CHECKSUM_BLOCKSIZE)
    {
        if (! mClientThreadPtr) {
            return;
        }
        ReceiveClear();
        mReceiveByteCount      = inLength;
        mFirstChecksumBlockLen = inFirstCheckSumBlockLen;
        mComputeChecksumFlag   =
            0 <= mReceiveByteCount && inComputeChecksumFlag;
    }
    KfsOp* GetReceivedOp() const
        { return mReceivedOpPtr; }
    vector<uint32_t>& GetBlockChecksums()
        { return mBlocksChecksums; }
    uint32_t GetChecksum() const
        { return mChecksum; }
    int GetReceivedHeaderLen() const
        { return mReceivedHeaderLen; }
    int GetReceiveByteCount() const
        { return mReceiveByteCount; }
    bool IsClientThread() const
        { return (mClientThreadPtr != 0); }
    int DispatchEvent(
        ClientSM& inClient,
        int       inCode,
        void*     inDataPtr);
    void DispatchGranted(
        ClientSM& inClient);
private:
    ClientThread* const    mClientThreadPtr;
    KfsOp*                 mOpsHeadPtr;
    KfsOp*                 mOpsTailPtr;
    KfsOp*                 mReceivedOpPtr;
    vector<uint32_t>       mBlocksChecksums;
    uint32_t               mChecksum;
    uint32_t               mFirstChecksumBlockLen;
    int                    mReceiveByteCount;
    int                    mReceivedHeaderLen;
    bool                   mGrantedFlag:1;
    bool                   mReceiveOpFlag:1;
    bool                   mComputeChecksumFlag:1;
    ClientThreadListEntry* mPrevPtr[kDispatchQueueCount];
    ClientThreadListEntry* mNextPtr[kDispatchQueueCount];

    friend class QCDLListOp<ClientThreadListEntry, kDispatchQueueIdx>;
    friend class ClientThreadImpl;

    inline static int HandleRequest(
        ClientSM& inClient,
        int       inCode,
        void*     inDataPtr,
        bool&     outRecursionFlag);
    inline static int HandleGranted(
        ClientSM& inClient);
    inline static const NetConnectionPtr& GetConnection(
        const ClientSM& inClient);
    inline ClientSM& GetClient();
private:
    ClientThreadListEntry(
        const ClientThreadListEntry& inEntry);
    ClientThreadListEntry& operator=(
        const ClientThreadListEntry& inEntry);
};

// KFS client protocol state machine.
class ClientSM :
    public  KfsCallbackObj,
    public  ClientThreadListEntry,
    private BufferManager::Client,
    public  SslFilterServerPsk
{
public:
    static void SetParameters(const Properties& prop);

    ClientSM(
        const NetConnectionPtr& conn,
        ClientThread*           thread);
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
    bool CheckAccess(ChunkAccessRequestOp& op);
    const DelegationToken& GetDelegationToken() const
        { return mDelegationToken; }
    const string& GetSessionKey() const
        { return mSessionKey; }
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
    bool                       mContentReceivedFlag;
    DelegationToken            mDelegationToken;
    string                     mSessionKey;
    bool                       mHandleTerminateFlag;

    static int                 sMaxCmdHeaderReadAhead;
    static bool                sTraceRequestResponseFlag;
    static bool                sEnforceMaxWaitFlag;
    static bool                sSslPskEnabledFlag;
    static int                 sMaxReqSizeDiscard;
    static size_t              sMaxAppendRequestSize;
    static uint64_t            sInstanceNum;

    int HandleRequest(int code, void *data);

    // This is a terminal state handler.  In this state, we wait for
    // all outstanding ops to finish and then destroy this.
    int HandleTerminate(int code, void *data);
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
    int HandleRequestSelf(int code, void* data);
    int HandleGranted();
    inline time_t TimeNow() const;
    inline void SendResponse(KfsOp& op);
    inline static BufferManager& GetBufferManager();
    inline static BufferManager* FindDevBufferManager(KfsOp& op);
    inline Client* GetDevBufMgrClient(const BufferManager* bufMgr);
    inline void PutAndResetDevBufferManager(KfsOp& op, ByteCount opBytes);
    inline bool IsAccessEnforced() const;
    bool FailIfExceedsWait(
        BufferManager&         bufMgr,
        BufferManager::Client* mgrCli);
    void GrantedSelf(ByteCount byteCount, bool devBufManagerFlag);
    virtual unsigned long GetPsk(
        const char*    inIdentityPtr,
	unsigned char* inPskBufferPtr,
        unsigned int   inPskBufferLen,
        string&        outAuthName);
    int DispatchRequest(int code, void* data)
        { return DispatchEvent(*this, code, data); }
private:
    // No copy.
    ClientSM(const ClientSM&);
    ClientSM& operator=(const ClientSM&);
    friend class ClientThreadListEntry;
};

}

#endif // CHUNK_CLIENTSM_H
