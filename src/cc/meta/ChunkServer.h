//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/05
// Author: Sriram Rao, Mike Ovsiannikov
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
// \file ChunkServer.h
// \brief Object that handles the communication with an individual
// chunk server. Model here is the following:
//  - For write-allocation, layout manager asks the ChunkServer object
// to send an RPC to the chunk server.
//  - The ChunkServer object sends the RPC and holds on to the request
// that triggered the RPC.
//  - Eventually, when the RPC reply is received, the request is
// re-activated (alongwith the response) and is sent back down the pike.
//
//----------------------------------------------------------------------------

#ifndef META_CHUNKSERVER_H
#define META_CHUNKSERVER_H


#include "common/LinearHash.h"
#include "kfsio/KfsCallbackObj.h"
#include "kfsio/NetConnection.h"
#include "kfsio/SslFilter.h"
#include "kfsio/CryptoKeys.h"
#include "qcdio/QCDLList.h"
#include "common/kfstypes.h"
#include "common/Properties.h"
#include "common/ValueSampler.h"
#include "common/StdAllocator.h"
#include "common/MsgLogger.h"
#include "MetaRequest.h"

#include <string>
#include <ostream>
#include <istream>
#include <map>
#include <set>

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>

#include <time.h>

namespace KFS
{
using std::string;
using std::ostream;
using std::istream;
using std::map;
using std::multimap;
using std::pair;
using std::less;
using std::set;

/// Chunk server connects to the meta server, sends a HELLO
/// message to configure its state with the meta server,  and
/// from then onwards, the meta server then drives the RPCs.
/// Types of messages:
///   Meta server --> Chunk server: Allocate, Free, Heartbeat
///
struct ChunkRecoveryInfo;
struct MetaHello;

class CSMapServerInfo
{
public:
    CSMapServerInfo()
        : mIndex(-1),
          mChunkCount(0),
          mSet(0)
        {}
    ~CSMapServerInfo() {
        delete mSet;
    }
    int GetIndex() const { return mIndex; }
    size_t GetChunkCount() const { return mChunkCount; }
private:
    int    mIndex;
    size_t mChunkCount;

    void AddHosted() {
        mChunkCount++;
        assert(mChunkCount > 0);
    }
    void RemoveHosted() {
        if (mChunkCount <= 0) {
            panic("no hosted chunks", false);
            return;
        }
        mChunkCount--;
    }
    void ClearHosted() {
        mChunkCount = 0;
        if (mSet) {
            mSet->Clear();
        }
    }
    void SetIndex(int idx, bool debugTrackChunkIdFlag) {
        mIndex = idx;
        if (debugTrackChunkIdFlag) {
             if (! mSet) {
                mSet = new Set();
            }
        } else {
            delete mSet;
            mSet = 0;
        }
    }

private:
    // The set here is for CSMap debugging only, see
    // CSMap::SetDebugValidate()
    typedef KeyOnly<chunkId_t> KeyVal;
    typedef LinearHash<
        KeyVal,
        KeyCompare<chunkId_t>,
        DynamicArray<
            SingleLinkedList<KeyVal>*,
            8 // 2^8 * sizeof(void*) => 2048
        >,
        StdFastAllocator<KeyVal>
    > Set;
    Set* mSet;

    void AddHosted(chunkId_t chunkId, int index) {
        bool newEntryFlag = false;
        if (mIndex < 0 || index != mIndex) {
            panic("invalid index", false);
        }
        if (mSet && (! mSet->Insert(chunkId, chunkId, newEntryFlag) ||
                ! newEntryFlag)) {
            panic("duplicate chunk id", false);
        }
        AddHosted();
    }
    void RemoveHosted(chunkId_t chunkId, int index) {
        if (mIndex < 0 || index != mIndex) {
            panic("invalid index", false);
        }
        if (mSet && mSet->Erase(chunkId) <= 0) {
            panic("no such chunk", false);
        }
        RemoveHosted();
    }
    const int* HostedIdx(chunkId_t chunkId) const {
        return ((mSet && mSet->Find(chunkId)) ? &mIndex : 0);
    }
    friend class CSMap;
private:
    CSMapServerInfo(const CSMapServerInfo&);
    CSMapServerInfo& operator=(const CSMapServerInfo&);
};

class ChunkServer :
    public KfsCallbackObj,
    public boost::enable_shared_from_this<ChunkServer>,
    public CSMapServerInfo,
    private SslFilterVerifyPeer {
public:
    typedef int RackId;
    class ChunkIdSet
    {
    public:
        ChunkIdSet()
            : mSet()
            {}
        ~ChunkIdSet()
            {}
        const chunkId_t* Find(chunkId_t chunkId) const {
            return mSet.Find(chunkId);
        }
        bool Erase(chunkId_t chunkId) {
            return (mSet.Erase(chunkId) != 0);
        }
        void First() {
            mSet.First();
        }
        const chunkId_t* Next() {
            const KeyVal* const ret = mSet.Next();
            return (ret ? &ret->GetKey() : 0);
        }
        bool Insert(chunkId_t chunkId) {
            bool inserted = false;
            mSet.Insert(chunkId, chunkId, inserted);
            return inserted;
        }
        void Clear() {
            mSet.Clear();
        }
        size_t Size() const {
            return mSet.GetSize();
        }
        bool IsEmpty() const {
            return mSet.IsEmpty();
        }
        void Swap(ChunkIdSet& other) {
            mSet.Swap(other.mSet);
        }
    private:
        typedef KeyOnly<chunkId_t> KeyVal;
        typedef LinearHash<
            KeyVal,
            KeyCompare<chunkId_t>,
            DynamicArray<
                SingleLinkedList<KeyVal>*,
                5 // 2^5 * sizeof(void*) => 256
            >,
            StdFastAllocator<KeyVal>
        > Set;

        Set mSet;
    private:
        ChunkIdSet(const ChunkIdSet&);
        ChunkIdSet& operator=(const ChunkIdSet&);
    };

    class StorageTierInfo
    {
    public:
        StorageTierInfo()
            : mDeviceCount(0),
              mNotStableOpenCount(0),
              mChunkCount(0),
              mSpaceAvailable(0),
              mTotalSpace(0),
              mSpaceUtilization(1.0),
              mOneOverTotalSpace(-1)
            {}
        void Set(
            int32_t deviceCount,
            int32_t notStableOpenCount,
            int32_t chunkCount,
            int64_t spaceAvailable,
            int64_t totalSpace)
        {
            if (mSpaceAvailable != spaceAvailable) {
                mSpaceUtilization = -1;
            }
            if (mTotalSpace != totalSpace) {
                mSpaceUtilization  = -1;
                mOneOverTotalSpace = -1;
            }
            mDeviceCount        = deviceCount;
            mNotStableOpenCount = notStableOpenCount;
            mChunkCount         = chunkCount;
            mSpaceAvailable     = spaceAvailable;
            mTotalSpace         = totalSpace;
        }
        int32_t GetDeviceCount() const {
            return mDeviceCount;
        }
        int32_t GetNotStableOpenCount() const {
            return mNotStableOpenCount;
        }
        int32_t GetChunkCount() const {
            return mChunkCount;
        }
        int64_t GetSpaceAvailable() const {
            return mSpaceAvailable;
        }
        int64_t GetTotalSpace() const {
            return mTotalSpace;
        }
        void Clear()
            { Set(0, 0, 0, 0, 0); }
        StorageTierInfo& operator-=(const StorageTierInfo& info) {
            mDeviceCount        -= info.mDeviceCount;
            mNotStableOpenCount -= info.mNotStableOpenCount;
            mChunkCount         -= info.mChunkCount;
            mSpaceAvailable     -= info.mSpaceAvailable;
            mTotalSpace         -= info.mTotalSpace;
            mSpaceUtilization   = -1;
            mOneOverTotalSpace  = -1;
            return *this;
        }
        StorageTierInfo& operator+=(const StorageTierInfo& info) {
            mDeviceCount        += info.mDeviceCount;
            mNotStableOpenCount += info.mNotStableOpenCount;
            mChunkCount         += info.mChunkCount;
            mSpaceAvailable     += info.mSpaceAvailable;
            mTotalSpace         += info.mTotalSpace;
            mSpaceUtilization   = -1;
            mOneOverTotalSpace  = -1;
           return *this;
        }
        StorageTierInfo& Delta(const StorageTierInfo& cur) {
            mDeviceCount        = cur.mDeviceCount        - mDeviceCount;
            mNotStableOpenCount = cur.mNotStableOpenCount - mNotStableOpenCount;
            mChunkCount         = cur.mChunkCount         - mChunkCount;
            mSpaceAvailable     = cur.mSpaceAvailable     - mSpaceAvailable;
            mTotalSpace         = cur.mTotalSpace         - mTotalSpace;
            mSpaceUtilization   = -1;
            mOneOverTotalSpace  = -1;
            return *this;
        }
        double GetSpaceUtilization() const {
            if (mSpaceUtilization >= 0) {
                return mSpaceUtilization;
            }
            const int64_t used = mTotalSpace - mSpaceAvailable;
            if (used < 0 || mTotalSpace <= 0) {
                Mutable(*this).mSpaceUtilization = mTotalSpace <= 0 ? 1. : 0.;
                return mSpaceUtilization;
            }
            if (mOneOverTotalSpace < 0) {
                Mutable(mOneOverTotalSpace) = double(1) / (double)mTotalSpace;
            }
            Mutable(mSpaceUtilization) = (double)used * mOneOverTotalSpace;
            return mSpaceUtilization;
        }
        void AddInFlightAlloc() {
            mSpaceAvailable -= (int64_t)CHUNKSIZE;
            if (mSpaceAvailable < 0) {
                mSpaceAvailable = 0;
            }
            mSpaceUtilization = -1;
            mNotStableOpenCount++;
            mChunkCount++;
        }
    private:
        int32_t mDeviceCount;
        int32_t mNotStableOpenCount;
        int32_t mChunkCount;
        int64_t mSpaceAvailable;
        int64_t mTotalSpace;
        double  mSpaceUtilization;
        double  mOneOverTotalSpace;

        template <typename T> static T& Mutable(const T& v) {
            return const_cast<T&>(v);
        }
    };

    typedef multimap <
        chunkId_t,
        const MetaChunkRequest*,
        less<chunkId_t>,
        StdFastAllocator<
            pair<const chunkId_t, const MetaChunkRequest*>
        >
    > ChunkOpsInFlight;

    static KfsCallbackObj* Create(const NetConnectionPtr &conn);
    ///
    /// Sequence:
    ///  Chunk server connects.
    ///   - A new chunkserver sm is born
    ///   - chunkserver sends a HELLO with config info
    ///   - send/recv messages with that chunkserver.
    ///
    ChunkServer(const NetConnectionPtr& conn, const string& peerName);
    ~ChunkServer();

    bool CanBeChunkMaster() const {
        return mCanBeChunkMaster;
    }
    void SetCanBeChunkMaster(bool flag);

    /// Generic event handler to handle network
    /// events. This method gets from the net manager when
    /// it sees some data is available on the socket.
    int HandleRequest(int code, void *data);

    /// Send an RPC to allocate a chunk on this server.
    /// An RPC request is enqueued and the call returns.
    /// When the server replies to the RPC, the request
    /// processing resumes.
    /// @param[in] r the request associated with the RPC call.
    /// @param[in] leaseId the id associated with the write lease.
    /// @retval 0 on success; -1 on failure
    ///
    int AllocateChunk(MetaAllocate* r, int64_t leaseId, kfsSTier_t tier);

    /// Send an RPC to delete a chunk on this server.
    /// An RPC request is enqueued and the call returns.
    /// When the server replies to the RPC, the request
    /// processing resumes.
    /// @param[in] chunkId name of the chunk that is being
    ///  deleted.
    /// @retval 0 on success; -1 on failure
    ///
    int DeleteChunk(chunkId_t chunkId);

    ///
    /// Send a message to the server asking it to go down.
    ///
    void Retire();

    void Restart(bool justExitFlag);

    /// Method to get the size of a chunk from a chunkserver.
    int GetChunkSize(fid_t fid, chunkId_t chunkId,
        seq_t chunkVersion, const string &pathname, bool retryFlag = true);

    /// Methods to handle (re) replication of a chunk.  If there are
    /// insufficient copies of a chunk, we replicate it.
    int ReplicateChunk(fid_t fid, chunkId_t chunkId,
        const ChunkServerPtr&    dataServer,
        const ChunkRecoveryInfo& recoveryInfo,
        kfsSTier_t minSTier, kfsSTier_t maxSTier,
        MetaChunkReplicate::FileRecoveryInFlightCount::iterator it);
    /// Start write append recovery when chunk master is non operational.
    int BeginMakeChunkStable(fid_t fid, chunkId_t chunkId, seq_t chunkVersion);
    /// Notify a chunkserver that the writes to a chunk are done;
    /// the chunkserver in turn should flush dirty data and make the
    /// chunk "stable".
    int MakeChunkStable(fid_t fid, chunkId_t chunkId, seq_t chunkVersion,
        chunkOff_t chunkSize, bool hasChunkChecksum, uint32_t chunkChecksum,
        bool addPending);

    /// Replication of a chunk finished.  Update statistics
    void ReplicateChunkDone(chunkId_t chunkId) {
        mNumChunkWriteReplications--;
        assert(mNumChunkWriteReplications >= 0);
        if (mNumChunkWriteReplications < 0)
            mNumChunkWriteReplications = 0;
        MovingChunkDone(chunkId);
    }


    /// Accessor method to get # of replications that are being
    /// handled by this server.
    int GetNumChunkReplications() const {
        return mNumChunkWriteReplications;
    }

    /// During re-replication, we want to track how much b/w is
    /// being spent read requests for replication by the server.  This
    /// is to prevent a server being overloaded and becoming
    /// unresponsive as we try to increase the # of replicas.
    int GetReplicationReadLoad() const {
        return mNumChunkReadReplications;
    }

    void UpdateReplicationReadLoad(int count) {
        mNumChunkReadReplications += count;
        if (mNumChunkReadReplications < 0)
            mNumChunkReadReplications = 0;
    }

    /// If a chunkserver isn't responding, don't send any
    /// write load towards it.  We detect loaded servers to be
    /// those that don't respond to heartbeat messages.
    bool IsResponsiveServer() const {
        return (! mDown && ! mHeartbeatSkipped);
    }

    /// To support scheduled down-time and allow maintenance to be
    /// done on the server node, we could "retire" a server; when the
    /// server is being retired, we evacuate the blocks on that server
    /// and re-replicate them elsewhere (on non-retiring nodes).
    /// During the stage where the server is being retired, we don't
    /// want to send any new write traffic to the server.
    ///
    void SetRetiring();

    bool IsRetiring() const {
        return mIsRetiring;
    }

    void IncCorruptChunks() {
        mNumCorruptChunks++;
    }

    /// Provide some stats...useful for ops
    void GetRetiringStatus(ostream &os);
    void GetEvacuateStatus(ostream &os);

    /// When the plan is read in, the set of chunks that
    /// need to be moved to this node is updated.
    bool AddToChunksToMove(chunkId_t chunkId) {
        return mChunksToMove.Insert(chunkId);
    }

    const ChunkIdSet& GetChunksToMove() {
        return mChunksToMove;
    }

    void ClearChunksToMove() {
        mChunksToMove.Clear();
    }

    /// Whenever this node re-replicates a chunk that was targeted
    /// for rebalancing, update the set.
    bool MovingChunkDone(chunkId_t chunkId) {
        return (mChunksToMove.Erase(chunkId) > 0);
    }

    /// Whenever the layout manager determines that this
    /// server has stale chunks, it queues an RPC to
    /// notify the chunk server of the stale data.
    void NotifyStaleChunks(ChunkIdQueue& staleChunks,
        bool evacuatedFlag = false, bool clearStaleChunksFlag = true,
        const MetaChunkAvailable* ca = 0);
    void NotifyStaleChunks(
        ChunkIdQueue&             staleChunks,
        const MetaChunkAvailable& ca)
        { NotifyStaleChunks(staleChunks, false, true, &ca); }
    void NotifyStaleChunk(chunkId_t staleChunk, bool evacuatedFlag = false);

    /// There is a difference between the version # as stored
    /// at the chunkserver and what is on the metaserver.  By sending
    /// this message, the metaserver is asking the chunkserver to change
    /// the version # to what is passed in.
    void NotifyChunkVersChange(fid_t fid, chunkId_t chunkId, seq_t chunkVers,
        seq_t fromVersion, bool makeStableFlag, bool pendingAddFlag = false,
        MetaChunkReplicate* replicate = 0);

    /// Accessor method to get the host name/port
    const ServerLocation& GetServerLocation() const {
        return mLocation;
    }

    /// Check if the hostname/port matches what is passed in
    /// @param[in] name  name to match
    /// @param[in] port  port # to match
    /// @retval true  if a match occurs; false otherwise
    bool MatchingServer(const ServerLocation& loc) const {
        return mLocation == loc;
    }

    /// Setter method to set space
    void SetSpace(int64_t total, int64_t used, int64_t alloc) {
        mTotalSpace = total;
        mUsedSpace = used;
        mAllocSpace = alloc;
    }

    const char* GetServerName() {
        return mLocation.hostname.c_str();
    }

    void SetRack(RackId rackId) {
        mRackId = rackId;
    }
    /// Return the unique identifier for the rack on which the
    /// server is located.
    RackId GetRack() const {
        return mRackId;
    }

    /// Available space is defined as the difference
    /// between the total storage space available
    /// on the server and the amount of space that
    /// has been parceled out for outstanding writes
    /// by the meta server.  THat is, alloc space is tied
    /// to the chunks that have been write-leased.  This
    /// has the effect of keeping alloc space tied closely
    /// to used space.
    int64_t GetAvailSpace() const {
        return max(int64_t(0), mTotalSpace - mAllocSpace);
    }

    /// Accessor to that returns an estimate of the # of
    /// concurrent writes that are being handled by this server
    int GetNumChunkWrites() const {
        return mNumChunkWrites;

    }

    int64_t GetNumAppendsWithWid() const {
        return mNumAppendsWithWid;
    }

    int64_t GetTotalSpace(bool useFsTotalSpaceFlag) const {
        return (useFsTotalSpaceFlag ? mTotalFsSpace : mTotalSpace);
    }

    int64_t GetTotalFsSpace() const {
        return mTotalFsSpace;
    }

    int64_t GetUsedSpace() const {
        return mUsedSpace;
    }

    int GetNumChunks() const {
        return mNumChunks;
    }

    int64_t GetFreeFsSpace() const {
        return GetAvailSpace();
    }

    int64_t GetFsUsedSpace() const {
        return max(mUsedSpace, mTotalFsSpace - GetFreeFsSpace());
    }

    int GetDeviceCount(kfsSTier_t tier) const {
        return mStorageTiersInfo[tier].GetDeviceCount();
    }
    int GetNotStableOpenCount(kfsSTier_t tier) const {
        return mStorageTiersInfo[tier].GetNotStableOpenCount();
    }
    int64_t GetStorageTierAvailSpace(kfsSTier_t tier) const {
        return mStorageTiersInfo[tier].GetSpaceAvailable();
    }
    double GetStorageTierSpaceUtilization(kfsSTier_t tier) const {
        return mStorageTiersInfo[tier].GetSpaceUtilization();
    }

    /// Return an estimate of disk space utilization on this server.
    /// The estimate is between [0..1]
    double GetSpaceUtilization(bool useFsTotalSpaceFlag) const {
        return (useFsTotalSpaceFlag ?
            GetFsSpaceUtilization() :
            GetTotalSpaceUtilization()
        );
    }
    double GetTotalSpaceUtilization() const {
        if (mTotalSpace <= 0) {
            return 1;
        }
        if (mPrevTotalSpace != mTotalSpace) {
            Mutable(mPrevTotalSpace)    = mTotalSpace;
            Mutable(mOneOverTotalSpace) = double(1) / mTotalSpace;
        }
        return (mUsedSpace * mOneOverTotalSpace);
    }
    double GetFsSpaceUtilization() const {
        if (mTotalFsSpace <= 0) {
            return 1;
        }
        if (mPrevTotalFsSpace != mTotalFsSpace) {
            Mutable(mPrevTotalFsSpace)    = mTotalFsSpace;
            Mutable(mOneOverTotalFsSpace) =
                double(1) / mTotalFsSpace;
        }
        return (GetFsUsedSpace() * mOneOverTotalFsSpace);
    }
    bool IsDown() const {
        return mDown;
    }

    ///
    /// The chunk server went down.  So, fail all the
    /// outstanding ops.
    ///
    void FailPendingOps();

    /// For monitoring purposes, dump out state as a string.
    /// @param [out] result   The state of this server
    ///
    void Ping(ostream& os, bool useTotalFsSpaceFlag) const;

    seq_t NextSeq() { return mSeqNo++; }
    int TimeSinceLastHeartbeat() const;
    void ForceDown();
    static void SetParameters(const Properties& prop, int clientPort);
    void SetProperties(const Properties& props);
    int64_t Uptime() const { return mUptime; }
    bool ScheduleRestart(int64_t gracefulRestartTimeout, int64_t gracefulRestartAppendWithWidTimeout);
    bool IsRestartScheduled() const {
        return (mRestartScheduledFlag || mRestartQueuedFlag);
    }
    const string& DownReason() const {
        return mDownReason;
    }
    const Properties& HeartBeatProperties() const {
        return mHeartbeatProperties;
    }
    int64_t GetLoadAvg() const {
        return mLoadAvg;
    }
    int Evacuate(chunkId_t chunkId) {
        if (mIsRetiring) {
            return -EEXIST;
        }
        if (mChunksToEvacuate.Size() >= sMaxChunksToEvacuate) {
            return -EAGAIN;
        }
        return (mChunksToEvacuate.Insert(chunkId) ? 0 : -EEXIST);
    }
    bool IsEvacuationScheduled(chunkId_t chunkId) const {
        return (mIsRetiring || mChunksToEvacuate.Find(chunkId));
    }
    static const ChunkOpsInFlight& GetChunkOpsInFlight() {
        return sChunkOpsInFlight;
    }
    static int GetChunkServerCount() {
        return sChunkServerCount;
    }
    void UpdateSpace(MetaChunkEvacuate& op);
    size_t GetChunksToEvacuateCount() const {
        return mChunksToEvacuate.Size();
    }
    bool GetCanBeCandidateServerFlag() const {
        return mCanBeCandidateServerFlag;
    }
    bool GetCanBeCandidateServerFlag(kfsSTier_t tier) const {
        return mCanBeCandidateServerFlags[tier];
    }
    void SetCanBeCandidateServerFlag(kfsSTier_t tier, bool flag) {
        mCanBeCandidateServerFlags[tier] = flag;
    }
    void SetCanBeCandidateServerFlag(bool flag) {
        mCanBeCandidateServerFlag = flag;
    }
    int64_t GetEvacuateCount() const {
        return mEvacuateCnt;
    }
    int64_t GetEvacuateBytes() const {
        return mEvacuateBytes;
    }
    int GetNumDrives() const {
        return mNumDrives;
    }
    int GetNumWritableDrives() const {
        return mNumWritableDrives;
    }
    void SetChunkDirStatus(const string& dir, bool dirOkFlag) {
        if (dirOkFlag) {
            mLostChunkDirs.erase(Escape(dir));
        } else {
            mLostChunkDirs.insert(Escape(dir));
        }
    }
    const string& GetHostPortStr() const
        { return mHostPortStr; }
    typedef MetaChunkDirInfo::DirName DirName;
    typedef map <
        DirName,
        pair<Properties, string>,
        less<DirName>,
        StdFastAllocator<
            pair<const DirName, pair<Properties, string> > >
    > ChunkDirInfos;
    void SetChunkDirInfo(const DirName& dirName, Properties& props) {
        if (IsDown() || dirName.empty()) {
            return;
        }
        // Find should succeed, except initial load. Use find to avoid
        // key (dirName) copy in the insertion pair constructor.
        ChunkDirInfos::iterator const it = mChunkDirInfos.find(dirName);
        if (it != mChunkDirInfos.end()) {
            it->second.first.swap(props);
            return;
        }
        ChunkDirInfos::mapped_type& di = mChunkDirInfos[dirName];
        di.first.swap(props);
        di.second = Escape(dirName);
        sChunkDirsCount++;
    }
    const ChunkDirInfos& GetChunkDirInfos() const {
        return mChunkDirInfos;
    }
    static void SetMaxHelloBufferBytes(int64_t maxBytes) {
        sMaxHelloBufferBytes = maxBytes;
    }
    static int64_t GetMaxHelloBufferBytes() {
        return sMaxHelloBufferBytes;
    }
    static bool RunHelloBufferQueue();
    static size_t GetChunkDirsCount() {
        return sChunkDirsCount;
    }
    virtual bool Verify(
	string&       ioFilterAuthName,
        bool          inPreverifyOkFlag,
        int           inCurCertDepth,
        const string& inPeerName,
        int64_t       inEndTime,
        bool          inEndTimeValidFlag);
    bool GetCryptoKey(
        CryptoKeys::KeyId&  outKeyId,
        CryptoKeys::Key&    outKey) const
    {
        if (mCryptoKeyValidFlag) {
            outKeyId = mCryptoKeyId;
            outKey   = mCryptoKey;
        }
        return mCryptoKeyValidFlag;
    }
    bool IsCryptoKeyValid() const
        { return mCryptoKeyValidFlag; }
    kfsUid_t GetAuthUid() const
        { return mAuthUid; }
    const string& GetMd5Sum() const
        { return mMd5Sum; }
    static void SetMaxChunkServerCount(int count)
        { sMaxChunkServerCount = count; }
    static int GetMaxChunkServerCount()
        { return sMaxChunkServerCount; }

protected:
    /// Enqueue a request to be dispatched to this server
    /// @param[in] r  the request to be enqueued.
    /// allow override in layout emulator.
    virtual void EnqueueSelf(MetaChunkRequest* r);
    void Enqueue(MetaChunkRequest* r, int timeout = -1);
    void SetServerLocation(const ServerLocation& loc);

    /// A sequence # associated with each RPC we send to
    /// chunk server.  This variable tracks the seq # that
    /// we should use in the next RPC.
    seq_t mSeqNo;
    seq_t mAuthPendingSeq;
    /// A handle to the network connection
    NetConnectionPtr mNetConnection;

    /// Are we thru with processing HELLO message
    bool mHelloDone;

    /// Boolean that tracks whether this server is down
    bool mDown;

    /// Is there a heartbeat message for which we haven't
    /// recieved a reply yet?  If yes, dont' send one more
    bool   mHeartbeatSent;

    /// did we skip the sending of a heartbeat message?
    bool mHeartbeatSkipped;

    time_t mLastHeartbeatSent;

    static int    sHeartbeatTimeout;
    static int    sHeartbeatInterval;
    static int    sHeartbeatLogInterval;
    static int    sChunkAllocTimeout;
    static int    sChunkReallocTimeout;
    static int    sMakeStableTimeout;
    static int    sReplicationTimeout;
    static int    sRequestTimeout;
    static int    sMetaClientPort;
    static bool   sRestartCSOnInvalidClusterKeyFlag;
    static int    sSrvLoadSamplerSampleCount;
    static string sSrvLoadPropName;
    static size_t sMaxChunksToEvacuate;

    /// For record append's, can this node be a chunk master
    bool mCanBeChunkMaster;

    /// is the server being retired
    bool mIsRetiring;
    /// when we did we get the retire request
    time_t mRetireStartTime;

    /// when did we get the last heartbeat reply
    time_t mLastHeard;

    /// Set of chunks that need to be moved to this server.
    /// This set was previously computed by the rebalance planner.
    ChunkIdSet mChunksToMove;

    ChunkIdSet mChunksToEvacuate;

    /// Location of the server at which clients can
    /// connect to
    ServerLocation mLocation;
    string         mHostPortStr;

    /// A unique id to denote the rack on which the server is located.
    /// -1 signifies that we don't what rack the server is on and by
    /// implication, all servers are on same rack
    RackId mRackId;

    /// Keep a count of how many corrupt chunks we are seeing on
    /// this node; an indicator of the node in trouble?
    int64_t mNumCorruptChunks;

    /// total space available on this server
    int64_t mTotalSpace;
    int64_t mPrevTotalSpace;
    int64_t mTotalFsSpace;
    int64_t mPrevTotalFsSpace;
    double  mOneOverTotalSpace;
    double  mOneOverTotalFsSpace;
    /// space that has been used by chunks on this server
    int64_t mUsedSpace;

    /// space that has been allocated for chunks: this
    /// corresponds to the allocations that have been
    /// made, but not all of the allocated space is used.
    /// For instance, when we have partially filled
    /// chunks, there is space is allocated for a chunk
    /// but that space hasn't been fully used up.
    int64_t mAllocSpace;

    /// # of chunks hosted on this server; useful for
    /// reporting purposes
    long mNumChunks;

    /// An estimate of the CPU load average as reported by the
    /// chunkserver.  When selecting nodes for block allocation, we
    /// can use this info to weed out the most heavily loaded N% of
    /// the nodes.
    double mCpuLoadAvg;

    /// Chunkserver returns the # of drives on the node in a
    /// heartbeat response; we can then show this value on the UI
    int mNumDrives;
    int mNumWritableDrives;

    /// An estimate of the # of writes that are being handled
    /// by this server.  We use this value to update mAllocSpace
    /// The problem we have is that, we can end up with lots of
    /// partial chunks and over time such drift can significantly
    /// reduce the available space on the server (space is held
    /// down for by the partial chunks that may never be written to).
    /// Since writes can occur only when someone gets a valid write lease,
    /// we track the # of write leases that are issued and where the
    /// writes are occurring.  So, whenever we get a heartbeat, we
    /// can update alloc space as a sum of the used space and the # of
    /// writes that are currently being handled by this server.
    int     mNumChunkWrites;
    int64_t mNumAppendsWithWid;

    /// Track the # of chunk replications (write/read) that are going on this server
    int mNumChunkWriteReplications;
    int mNumChunkReadReplications;

    typedef multimap <
        time_t,
        MetaChunkRequest*,
        less<time_t>,
        StdFastAllocator<
            pair<const time_t,  MetaChunkRequest*>
        >
    > ReqsTimeoutQueue;
    typedef map <
        seq_t,
        pair<
            ReqsTimeoutQueue::iterator,
            ChunkOpsInFlight::iterator
        >,
        less<seq_t>,
        StdFastAllocator<
            pair<const seq_t,  ReqsTimeoutQueue::iterator>
        >
    > DispatchedReqs;
    typedef set <
        string,
        less<string>,
        StdFastAllocator<string>
    > LostChunkDirs;

    enum { kChunkSrvListsCount = 2 };
    /// RPCs that we have sent to this chunk server.
    DispatchedReqs     mDispatchedReqs;
    ReqsTimeoutQueue   mReqsTimeoutQueue;
    int64_t            mLostChunks;
    int64_t            mUptime;
    Properties         mHeartbeatProperties;
    bool               mRestartScheduledFlag;
    bool               mRestartQueuedFlag;
    time_t             mRestartScheduledTime;
    time_t             mLastHeartBeatLoggedTime;
    string             mDownReason;
    IOBuffer::WOStream mOstream;
    int                mRecursionCount;
    kfsUid_t           mAuthUid;
    string             mAuthName;
    MetaAuthenticate*  mAuthenticateOp;
    uint64_t           mAuthCtxUpdateCount;
    time_t             mSessionExpirationTime;
    bool               mReAuthSentFlag;
    MetaHello*         mHelloOp;
    ChunkServerPtr     mSelfPtr;
    ValueSampler       mSrvLoadSampler;
    int64_t            mLoadAvg;
    bool               mCanBeCandidateServerFlag;
    bool               mStaleChunksHexFormatFlag;
    IOBuffer::IStream  mIStream;
    int64_t            mEvacuateCnt;
    int64_t            mEvacuateBytes;
    int64_t            mEvacuateDoneCnt;
    int64_t            mEvacuateDoneBytes;
    int64_t            mEvacuateInFlight;
    int64_t            mPrevEvacuateDoneCnt;
    int64_t            mPrevEvacuateDoneBytes;
    time_t             mEvacuateLastRateUpdateTime;
    double             mEvacuateCntRate;
    double             mEvacuateByteRate;
    LostChunkDirs      mLostChunkDirs;
    ChunkDirInfos      mChunkDirInfos;
    string             mMd5Sum;
    const string       mPeerName;
    bool               mCryptoKeyValidFlag;
    CryptoKeys::KeyId  mCryptoKeyId;
    CryptoKeys::Key    mCryptoKey;
    string             mRecoveryMetaAccess;
    time_t             mRecoveryMetaAccessEndTime;
    MetaRequest*       mPendingResponseOpsHeadPtr;
    MetaRequest*       mPendingResponseOpsTailPtr;
    bool               mCanBeCandidateServerFlags[kKfsSTierCount];
    StorageTierInfo    mStorageTiersInfo[kKfsSTierCount];
    StorageTierInfo    mStorageTiersInfoDelta[kKfsSTierCount];
    ChunkServer*       mPrevPtr[kChunkSrvListsCount];
    ChunkServer*       mNextPtr[kChunkSrvListsCount];

    static ChunkOpsInFlight sChunkOpsInFlight;
    static ChunkServer*     sChunkServersPtr[kChunkSrvListsCount];
    static int              sChunkServerCount;
    static int              sMaxChunkServerCount;
    static int              sPendingHelloCount;
    static int              sMinHelloWaitingBytes;
    static int64_t          sMaxHelloBufferBytes;
    static int64_t          sHelloBytesCommitted;
    static int64_t          sHelloBytesInFlight;
    static int              sEvacuateRateUpdateInterval;
    static size_t           sChunkDirsCount;

    friend class QCDLListOp<ChunkServer, 0>;
    friend class QCDLListOp<ChunkServer, 1>;
    typedef QCDLList<ChunkServer, 0> ChunkServersList;
    typedef QCDLList<ChunkServer, 1> PendingHelloList;

    void AddToPendingHelloList();
    void RemoveFromPendingHelloList();
    static int64_t GetHelloBytes(MetaHello* req = 0);
    static void PutHelloBytes(MetaHello* req);

    ///
    /// We have received a message from the chunk
    /// server. Do something with it.
    /// @param[in] iobuf  An IO buffer stream with message
    /// received from the chunk server.
    /// @param[in] msgLen  Length in bytes of the message.
    /// @retval 0 if message was processed successfully;
    /// -1 if there was an error
    ///
    int HandleMsg(IOBuffer *iobuf, int msgLen);

    /// Handlers for the 3 types of messages we could get:
    /// 1. Hello message from a chunkserver
    /// 2. An RPC from a chunkserver
    /// 3. A reply to an RPC that we have sent previously.

    int HandleHelloMsg(IOBuffer *iobuf, int msgLen);
    int HandleCmd(IOBuffer *iobuf, int msgLen);
    int HandleReply(IOBuffer *iobuf, int msgLen);

    /// Send a response message to the MetaRequest we got.
    bool SendResponse(MetaRequest *op);

    ///
    /// Given a response from a chunkserver, find the
    /// associated request that we previously sent.
    /// Request/responses are matched based on sequence
    /// numbers in the messages.
    ///
    /// @param[in] cseq The sequence # of the op we are
    /// looking for.
    /// @retval The matching request if one exists; NULL
    /// otherwise
    ///
    MetaChunkRequest *FindMatchingRequest(seq_t cseq);

    MetaRequest* GetOp(IOBuffer& iobuf, int msgLen, const char* errMsgPrefix);

    ///
    /// The response sent by a chunkserver is of the form:
    /// OK \r\n
    /// Cseq: <seq #>\r\n
    /// Status: <status> \r\n\r\n
    /// Extract out Cseq, Status
    ///
    /// @param[in] buf Buffer containing the response
    /// @param[in] bufLen length of buf
    /// @param[out] prop  Properties object with the response header/values
    ///
    bool ParseResponse(istream& is, Properties &prop);
    ///
    /// The chunk server went down.  So, stop the network timer event;
    /// also, fail all the dispatched ops.
    ///
    void Error(const char* errorMsg);
    void FailDispatchedOps();
    /// Periodically, send a heartbeat message to the chunk server.
    int Heartbeat();
    int TimeoutOps();
    inline void UpdateChunkWritesPerDrive(
        int  numChunkWrites,
        int  numWritableDrives);
    inline void NewChunkInTier(kfsSTier_t tier);
    void ShowLines(MsgLogger::LogLevel logLevel, const string& prefix,
        IOBuffer& iobuf, int len, int linesToShow = 64,
        const char* truncatePrefix = "CKey:");
    const string& GetPeerName() const {
        return  mPeerName;
    }
    template<typename T>
    static string Escape(const T& str)
        { return Escape(str.data(), str.size()); }
    static string Escape(const char* buf, size_t len);
    template <typename T> static T& Mutable(const T& v) {
        return const_cast<T&>(v);
    }
    template<typename T>
    void UpdateStorageTiers(
        const T* tiers,
        int      deviceCount,
        int      writableChunkCount)
    {
        UpdateStorageTiersSelf(
            tiers ? tiers->GetPtr()  : 0,
            tiers ? tiers->GetSize() : size_t(0),
            deviceCount,
            writableChunkCount
        );
    }
    void ClearStorageTiers()
        { UpdateStorageTiersSelf("", 0, 0, 0); }
    void UpdateStorageTiersSelf(const char* buf, size_t len,
        int deviceCount, int writableChunkCount);
    int Authenticate(IOBuffer& iobuf);
    bool ParseCryptoKey(
        const Properties::String& keyId,
        const Properties::String& key);
    int DeclareHelloError(
        int         status,
        const char* statusMsg);
    void ReleasePendingResponses(bool sendResponseFlag = false);
};

} // namespace KFS

#endif // META_CHUNKSERVER_H
