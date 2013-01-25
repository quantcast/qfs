//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/28
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
// \file ChunkManager.h
// \brief Handles all chunk related ops.
//
//----------------------------------------------------------------------------

#ifndef _CHUNKMANAGER_H
#define _CHUNKMANAGER_H

#include "Chunk.h"
#include "KfsOps.h"
#include "DiskIo.h"
#include "DirChecker.h"

#include "kfsio/ITimeout.h"
#include "common/LinearHash.h"
#include "common/StdAllocator.h"

#include <vector>
#include <string>
#include <set>
#include <map>
#include <boost/static_assert.hpp>

namespace KFS
{

using std::string;
using std::vector;
using std::ostream;
using std::list;
using std::map;
using std::pair;
using std::make_pair;
using std::less;

class ChunkInfoHandle;

class Properties;

/// The chunk manager writes out chunks as individual files on disk.
/// The location of the chunk directory is defined by chunkBaseDir.
/// The file names of chunks is a string representation of the chunk
/// id. The chunk manager performs disk I/O asynchronously -- it never blocks.
/// All disk io related requests, including host file system meta operations
/// (create, delete, stat etc) added to disk io queues. The specified request
/// completion handler invoked upon completion of the request.
///
class ChunkManager : private ITimeout {
public:
    struct Counters
    {
        typedef int64_t Counter;

        Counter mBadChunkHeaderErrorCount;
        Counter mReadChecksumErrorCount;
        Counter mReadErrorCount;
        Counter mWriteErrorCount;
        Counter mOpenErrorCount;
        Counter mCorruptedChunksCount;
        Counter mLostChunksCount;
        Counter mDirLostChunkCount;
        Counter mChunkDirLostCount;

        void Clear()
        {
            mBadChunkHeaderErrorCount = 0;
            mReadChecksumErrorCount   = 0;
            mReadErrorCount           = 0;
            mWriteErrorCount          = 0;
            mOpenErrorCount           = 0;
            mCorruptedChunksCount     = 0;
            mLostChunksCount          = 0;
            mDirLostChunkCount        = 0;
            mChunkDirLostCount        = 0;
        }
    };

    ChunkManager();
    ~ChunkManager();

    void SetParameters(const Properties& prop);
    /// Init function to configure the chunk manager object.
    bool Init(const vector<string>& chunkDirs, const Properties& prop);

    /// Allocate a file to hold a chunk on disk.  The filename is the
    /// chunk id itself.
    /// @param[in] fileId  id of the file that has chunk chunkId
    /// @param[in] chunkId id of the chunk being allocated.
    /// @param[in] chunkVersion  the version assigned by the metaserver to this chunk
    /// @param[in] isBeingReplicated is the allocation for replicating a chunk?
    /// @retval status code
    int AllocChunk(kfsFileId_t fileId, kfsChunkId_t chunkId, 
                           int64_t chunkVersion,
                           bool isBeingReplicated = false,
                           ChunkInfoHandle **cih = 0,
                           bool mustExistFlag = false);
    void AllocChunkForAppend(
        AllocChunkOp*         op,
        int                   replicationPos,
        const ServerLocation& peerLoc);
    /// Delete a previously allocated chunk file.
    /// @param[in] chunkId id of the chunk being deleted.
    /// @retval status code
    int DeleteChunk(kfsChunkId_t chunkId);

    /// Dump chunk map with information about chunkID and chunkSize    
    void DumpChunkMap();

    /// Dump chunk map with information about chunkID and chunkSize
    /// to a string stream
    void DumpChunkMap(ostream& ofs);

    /// A previously created dirty chunk should now be made "stable".
    /// Move that chunk out of the dirty dir.
    int MakeChunkStable(kfsChunkId_t chunkId, kfsSeq_t chunkVersion,
            bool appendFlag, KfsCallbackObj* cb, string& statusMsg);
    bool IsChunkStable(kfsChunkId_t chunkId) const;
    bool IsChunkReadable(kfsChunkId_t chunkId) const;
    bool IsChunkStable(MakeChunkStableOp* op);

    /// A previously created chunk is stale; move it to stale chunks
    /// dir only if we want to preserve it; otherwise, delete
    ///
    /// @param[in] chunkId id of the chunk being moved
    /// @retval status code
    int StaleChunk(kfsChunkId_t chunkId,
        bool forceDeleteFlag = false, bool evacuatedFlag = false);

    /// Truncate a chunk to the specified size
    /// @param[in] chunkId id of the chunk being truncated.
    /// @param[in] chunkSize  size to which chunk should be truncated.
    /// @retval status code
    int TruncateChunk(kfsChunkId_t chunkId, int64_t chunkSize);

    /// Change a chunk's version # to what the server says it should be.
    /// @param[in] fileId  id of the file that has chunk chunkId
    /// @param[in] chunkId id of the chunk being allocated.
    /// @param[in] chunkVersion  the version assigned by the metaserver to this chunk
    /// @retval status code
    int ChangeChunkVers(kfsChunkId_t chunkId, 
                           int64_t chunkVersion, bool stableFlag, KfsCallbackObj* cb);
    int ChangeChunkVers(ChunkInfoHandle *cih,
                           int64_t chunkVersion, bool stableFlag, KfsCallbackObj* cb);
    int ChangeChunkVers(ChangeChunkVersOp* op);

    /// Open a chunk for I/O.
    /// @param[in] chunkId id of the chunk being opened.
    /// @param[in] openFlags  O_RDONLY, O_WRONLY
    /// @retval status code
    int OpenChunk(kfsChunkId_t chunkId, int openFlags);

    /// Close a previously opened chunk and release resources.
    /// @param[in] chunkId id of the chunk being closed.
    /// @retval 0 if the close was accepted; -1 otherwise
    int  CloseChunk(kfsChunkId_t chunkId);
    int  CloseChunk(ChunkInfoHandle* cih);
    bool CloseChunkIfReadable(kfsChunkId_t chunkId);

    /// Utility function that returns a pointer to mChunkTable[chunkId].
    /// @param[in] chunkId  the chunk id for which we want info
    /// @param[out] cih  the resulting pointer from mChunkTable[chunkId]
    /// @retval  0 on success; -EBADF if we can't find mChunkTable[chunkId]
    int GetChunkInfoHandle(kfsChunkId_t chunkId, ChunkInfoHandle **cih) const;

    /// Given a byte range, return the checksums for that range.
    vector<uint32_t> GetChecksums(kfsChunkId_t chunkId, int64_t offset, size_t numBytes);

    /// For telemetry purposes, provide the driveName where the chunk
    /// is stored and pass that back to the client. 
    string GetDirName(chunkId_t chunkId) const;

    /// Schedule a read on a chunk.
    /// @param[in] op  The read operation being scheduled.
    /// @retval 0 if op was successfully scheduled; -1 otherwise
    int ReadChunk(ReadOp *op);

    /// Schedule a write on a chunk.
    /// @param[in] op  The write operation being scheduled.
    /// @retval 0 if op was successfully scheduled; -1 otherwise
    int WriteChunk(WriteOp *op);

    /// Write/read out/in the chunk meta-data and notify the cb when the op
    /// is done.
    /// @retval 0 if op was successfully scheduled; -errno otherwise
    int WriteChunkMetadata(kfsChunkId_t chunkId, KfsCallbackObj *cb, bool forceFlag = false);
    int ReadChunkMetadata(kfsChunkId_t chunkId, KfsOp *cb);
    
    /// Notification that read is finished
    void ReadChunkMetadataDone(ReadChunkMetaOp* op, IOBuffer* dataBuf);
    bool IsChunkMetadataLoaded(kfsChunkId_t chunkId);

    /// A previously scheduled write op just finished.  Update chunk
    /// size and the amount of used space.
    /// @param[in] op  The write op that just finished
    ///
    bool ReadChunkDone(ReadOp *op);
    void ReplicationDone(kfsChunkId_t chunkId, int status);
    /// Determine the size of a chunk.
    /// @param[in] chunkId  The chunk whose size is needed
    /// @param[out] fid     Return the file-id that owns the chunk
    /// @param[out] chunkSize  The size of the chunk
    /// @retval status code
    void ChunkSize(SizeOp* op);

    /// Register a timeout handler with the net manager for taking
    /// checkpoints.  Also, get the logger going
    void Start();
    
    /// Read the chunk table from disk following a restart.  See
    /// comments in the method for issues relating to validation (such
    /// as, checkpoint contains a chunk name, but the associated file
    /// is not there on disk, etc.).
    int Restart();

    /// Retrieve the chunks hosted on this chunk server.
    typedef pair<int64_t*, ostream*> HostedChunkList;
    void GetHostedChunks(
        const HostedChunkList& stable,
        const HostedChunkList& notStableAppend,
        const HostedChunkList& notStable);

    /// Return the total space that is exported by this server.  If
    /// chunks are stored in a single directory, we use statvfs to
    /// determine the total space avail; we report the min of statvfs
    /// value and the configured mTotalSpace.
    int64_t GetTotalSpace(int64_t& totalFsSpace, int& chunkDirs,
        int& evacuateInFlightCount, int& writableDirs,
        int& evacuateChunks, int64_t& evacuteByteCount,
        int* evacuateDoneChunkCount = 0, int64_t* evacuateDoneByteCount = 0,
        HelloMetaOp::LostChunkDirs* lostChunkDirs = 0);
    int64_t GetUsedSpace() const { return mUsedSpace; };
    long GetNumChunks() const { return mChunkTable.GetSize(); };
    long GetNumWritableChunks() const;

    /// For a write, the client is defining a write operation.  The op
    /// is queued and the client pushes data for it subsequently.
    /// @param[in] wi  The op that defines the write
    /// @retval status code
    int AllocateWriteId(
        WriteIdAllocOp*       wi,
        int                   replicationPos,
        const ServerLocation& peerLoc);

    /// Check if a write is pending to a chunk.
    /// @param[in] chunkId  The chunkid for which we are checking for
    /// pending write(s). 
    /// @retval True if a write is pending; false otherwise
    bool IsWritePending(kfsChunkId_t chunkId) const {
        return mPendingWrites.HasChunkId(chunkId);
    }

    /// Given a chunk id, return its version
    int64_t GetChunkVersion(kfsChunkId_t c);

    /// Retrieve the write op given a write id.
    /// @param[in] writeId  The id corresponding to a previously
    /// enqueued write.
    /// @retval WriteOp if one exists; NULL otherwise
    WriteOp *GetWriteOp(int64_t writeId);

    /// The model with writes: allocate a write id (this causes a
    /// write-op to be created); then, push data for writes (which
    /// retrieves the write-op and then sends writes down to disk).
    /// The "clone" method makes a copy of a previously created
    /// write-op.
    /// @param[in] writeId the write id that was previously assigned
    /// @retval WriteOp if one exists; NULL otherwise
    WriteOp *CloneWriteOp(int64_t writeId);

    /// Set the status for a given write id
    void SetWriteStatus(int64_t writeId, int status);
    int  GetWriteStatus(int64_t writeId);
    
    /// Is the write id a valid one
    bool IsValidWriteId(int64_t writeId) {
        return mPendingWrites.find(writeId);
    }

    virtual void Timeout();

    ChunkInfo_t* GetChunkInfo(kfsChunkId_t chunkId);

    void ChunkIOFailed(kfsChunkId_t chunkId, int err, const DiskIo::File* file);
    void ChunkIOFailed(kfsChunkId_t chunkId, int err, const DiskIo* diskIo);
    void ChunkIOFailed(ChunkInfoHandle* cih, int err);
    void ReportIOFailure(ChunkInfoHandle* cih, int err);
    size_t GetMaxIORequestSize() const {
        return mMaxIORequestSize;
    }
    void Shutdown();
    bool IsWriteAppenderOwns(kfsChunkId_t chunkId) const;

    inline void LruUpdate(ChunkInfoHandle& cih);
    inline bool IsInLru(const ChunkInfoHandle& cih) const;
    inline void UpdateStale(ChunkInfoHandle& cih);

    void GetCounters(Counters& counters)
        { counters = mCounters; }

    /// Utility function that sets up a disk connection for an
    /// I/O operation on a chunk.
    /// @param[in] cih  chunk handle on which we are doing I/O
    /// @param[in] op   The KfsCallbackObj that is being on the chunk
    /// @retval A disk connection pointer allocated via a call to new;
    /// it is the caller's responsibility to free the memory
    DiskIo *SetupDiskIo(ChunkInfoHandle *cih, KfsCallbackObj *op);
    /// Notify the metaserver that chunk chunkId is corrupted; the
    /// metaserver will re-replicate this chunk and for now, won't
    /// send us traffic for this chunk.
    void NotifyMetaCorruptedChunk(ChunkInfoHandle *cih, int err);
    int  StaleChunk(ChunkInfoHandle *cih,
        bool forceDeleteFlag = false, bool evacuatedFlag = false);
    /// Utility function that given a chunkId, returns the full path
    /// to the chunk filename.
    string MakeChunkPathname(ChunkInfoHandle *cih);
    string MakeChunkPathname(ChunkInfoHandle *cih, bool stableFlag, kfsSeq_t targetVersion);
    void WriteDone(WriteOp* op);
    int GetMaxDirCheckDiskTimeouts() const
        { return mMaxDirCheckDiskTimeouts; }
    void MetaServerConnectionLost();
    void SetChunkSize(ChunkInfo_t& ci, int64_t chunkSize)
    {
        if (ci.chunkSize > 0) {
            mUsedSpace = mUsedSpace >= ci.chunkSize ?
                mUsedSpace - ci.chunkSize : 0;
        }
        ci.chunkSize = chunkSize > 0 ? chunkSize : 0;
        mUsedSpace += ci.chunkSize;
    }

    enum { kChunkInfoHandleListCount = 1 };
    enum ChunkListType
    {
        kChunkLruList = 0,
        kChunkStaleList = 1,
        kChunkPendingStaleList = 2,
        kChunkInfoListCount
    };
    typedef ChunkInfoHandle* ChunkLists[kChunkInfoHandleListCount];
    struct ChunkDirInfo;

    const string& GetEvacuateFileName() const
        { return mEvacuateFileName; }
    const string& GetEvacuateDoneFileName() const
        { return mEvacuateDoneFileName; }
    int UpdateCountFsSpaceAvailableFlags();
    void MetaHeartbeat(HeartbeatOp& op);
    int GetMaxEvacuateIoErrors() const
        { return mMaxEvacuateIoErrors; }
    int GetAvailableChunksRetryInterval() const
        { return mAvailableChunksRetryInterval; }
    bool IsSyncChunkHeader() const
        { return mSyncChunkHeaderFlag; }
    // The following are "internal/private" -- to be used only withing
    // ChunkManager.cpp
    inline ChunkInfoHandle* AddMapping(ChunkInfoHandle* cih);
    inline void MakeStale(ChunkInfoHandle& cih,
        bool forceDeleteFlag, bool evacuatedFlag);
    inline void DeleteSelf(ChunkInfoHandle& cih);
    inline bool Remove(ChunkInfoHandle& cih);

private:
    class PendingWrites
    {
    public:
        PendingWrites()
           : mWriteIds(), mChunkIds(), mLru(), mKeyOp(0, 0)
            {}
        bool empty() const
            { return (mWriteIds.empty()); }
        bool push_front(WriteOp* op)
            { return Insert(op, true); }
        bool push_back(WriteOp* op)
            { return Insert(op, false); }
        bool pop_front()
            { return Remove(true); }
        bool pop_back()
            { return Remove(false); }
        size_t size() const
            { return mWriteIds.size(); }
        WriteOp* front() const
            { return mLru.front().mWriteIdIt->mOp; }
        WriteOp* back() const
            { return mLru.back().mWriteIdIt->mOp; }
        WriteOp* find(int64_t writeId) const
        {
            WriteOp& op = GetKeyOp();
            op.writeId = writeId;
            WriteIdSet::const_iterator const i =
                mWriteIds.find(WriteIdEntry(&op));
            return (i == mWriteIds.end() ? 0 : i->mOp);
        }
        bool HasChunkId(kfsChunkId_t chunkId) const
            { return (mChunkIds.find(chunkId) != mChunkIds.end()); }
        bool erase(WriteOp* op)
        {
            const WriteIdSet::iterator i = mWriteIds.find(WriteIdEntry(op));
            return (i != mWriteIds.end() && op == i->mOp && Erase(i));
        }
        bool erase(int64_t writeId)
        {
            WriteOp& op = GetKeyOp();
            op.writeId = writeId;
            WriteIdSet::const_iterator const i =
                mWriteIds.find(WriteIdEntry(&op));
            return (i != mWriteIds.end() && Erase(i));
        }
        bool Delete(kfsChunkId_t chunkId, kfsSeq_t chunkVersion)
        {
            ChunkIdMap::iterator i = mChunkIds.find(chunkId);
            if (i == mChunkIds.end()) {
                return true;
            }
            ChunkWrites& wr = i->second;
            for (ChunkWrites::iterator w = wr.begin(); w != wr.end(); ) {
                Lru::iterator const c = w->GetLruIterator();
                if (c->mWriteIdIt->mOp->chunkVersion == chunkVersion) {
                    WriteOp* const op = c->mWriteIdIt->mOp;
                    mWriteIds.erase(c->mWriteIdIt);
                    mLru.erase(c);
                    w = wr.erase(w);
                    delete op;
                } else {
                    ++w;
                }
            }
            if (wr.empty()) {
                mChunkIds.erase(i);
                return true;
            }
            return false;
        }
        WriteOp* FindAndMoveBack(int64_t writeId)
        {
            mKeyOp.writeId = writeId;
            const WriteIdSet::iterator i =
                mWriteIds.find(WriteIdEntry(&mKeyOp));
            if (i == mWriteIds.end()) {
                return 0;
            }
            // splice: "All iterators remain valid including iterators that
            // point to elements of x." x == mLru
            mLru.splice(mLru.end(), mLru, i->GetLruIterator());
            return i->mOp;
        }
        size_t GetChunkIdCount() const
            { return mChunkIds.size(); }
    private:
        class LruIterator;
        class OpListEntry
        {
            private:
                struct { // Make it struct aligned.
                    char  mArray[sizeof(list<void*>::iterator)];
                } mLruIteratorStorage;
            public:
                inline OpListEntry();
                inline ~OpListEntry();
                // Set iterator prohibit node mutation, because the node is the
                // key, and changing the key can potentially change the order.
                // In this particular case order only depends on mOp->writeId.
                // The following hack is also needed to get around type dependency
                // cycle with Lru::iterator, and WriteIdEntry.
                LruIterator& GetLruIterator() const
                {
                    return *reinterpret_cast<LruIterator*>(
                        &const_cast<OpListEntry*>(this)->mLruIteratorStorage);
                }
        };
        struct WriteIdEntry : public OpListEntry
        {
        public:
            inline WriteIdEntry(WriteOp* op = 0);
            WriteOp* mOp;
        };
        struct WriteIdCmp
        {
            bool operator()(const WriteIdEntry& x, const WriteIdEntry& y) const
                { return (x.mOp->writeId < y.mOp->writeId); }
        };
        typedef set<WriteIdEntry, WriteIdCmp,
            StdFastAllocator<WriteIdEntry>
        > WriteIdSet;
        typedef list<OpListEntry,
            StdFastAllocator<OpListEntry> > ChunkWrites;
        typedef map<kfsChunkId_t, ChunkWrites, less<kfsChunkId_t>,
            StdFastAllocator<
                pair<const kfsChunkId_t, ChunkWrites> >
        > ChunkIdMap;
        struct LruEntry
        {
            LruEntry()
                : mWriteIdIt(), mChunkIdIt(), mChunkWritesIt()
                {}
            LruEntry(
                WriteIdSet::iterator  writeIdIt,
                ChunkIdMap::iterator  chunkIdIt,
                ChunkWrites::iterator chunkWritesIt)
                : mWriteIdIt(writeIdIt),
                  mChunkIdIt(chunkIdIt),
                  mChunkWritesIt(chunkWritesIt)
                {}
            WriteIdSet::iterator  mWriteIdIt;
            ChunkIdMap::iterator  mChunkIdIt;
            ChunkWrites::iterator mChunkWritesIt;
        };
        typedef list<LruEntry, StdFastAllocator<LruEntry> > Lru;
        class LruIterator : public Lru::iterator
        {
        public:
            LruIterator& operator=(const Lru::iterator& it)
            {
                Lru::iterator::operator=(it);
                return *this;
            }
        };

        WriteIdSet mWriteIds;
        ChunkIdMap mChunkIds;
        Lru        mLru;
        WriteOp    mKeyOp;

        bool Insert(WriteOp* op, bool front)
        {
            if (! op) {
                return false;
            }
            pair<WriteIdSet::iterator, bool> const w =
                mWriteIds.insert(WriteIdEntry(op));
            if (! w.second) {
                return false;
            }
            ChunkIdMap::iterator const c = mChunkIds.insert(
                make_pair(op->chunkId, ChunkWrites())).first;
            ChunkWrites::iterator const cw =
                c->second.insert(c->second.end(), OpListEntry());
            w.first->GetLruIterator() = mLru.insert(
                front ? mLru.begin() : mLru.end(),
                LruEntry(w.first, c, cw));
            cw->GetLruIterator() = w.first->GetLruIterator();
            return true;
        }
        bool Remove(bool front)
        {
            if (mLru.empty()) {
                return false;
            }
            LruEntry& c = front ? mLru.front() : mLru.back();
            mWriteIds.erase(c.mWriteIdIt);
            c.mChunkIdIt->second.erase(c.mChunkWritesIt);
            if (c.mChunkIdIt->second.empty()) {
                mChunkIds.erase(c.mChunkIdIt);
            }
            if (front) {
                mLru.pop_front();
            } else {
                mLru.pop_back();
            }
            return true;
        }
        bool Erase(WriteIdSet::iterator i)
        {
            const Lru::iterator c = i->GetLruIterator();
            c->mChunkIdIt->second.erase(c->mChunkWritesIt);
            if (c->mChunkIdIt->second.empty()) {
                mChunkIds.erase(c->mChunkIdIt);
            }
            mLru.erase(c);
            mWriteIds.erase(i);
            return true;
        }
        WriteOp& GetKeyOp() const
            { return *const_cast<WriteOp*>(&mKeyOp); }
    private:
        PendingWrites(const PendingWrites&);
        PendingWrites& operator=(const PendingWrites&);
    };

    class ChunkDirs
    {
    public:
        typedef ChunkDirInfo* iterator;
        typedef const ChunkDirInfo* const_iterator;
        ChunkDirs()
            : mChunkDirs(0),
              mSize(0)
            {}
        inline ~ChunkDirs();
        inline ChunkDirInfo& operator[](size_t i);
        inline const ChunkDirInfo& operator[](size_t i) const;
        inline iterator begin() { return mChunkDirs; }
        inline iterator end();
        inline const_iterator begin() const { return mChunkDirs; };
        inline const_iterator end()   const;
        void Allocate(size_t size);
        size_t size() const { return mSize; }
    private:
        ChunkDirInfo* mChunkDirs;
        size_t        mSize;

        ChunkDirs(const ChunkDirs&);
        ChunkDirs& operator=(const ChunkDirs&);
    };

    struct StaleChunkCompletion : public KfsCallbackObj
    {
        StaleChunkCompletion(
            ChunkManager& m)
            : KfsCallbackObj(),
              mMgr(m)
            { SET_HANDLER(this, &StaleChunkCompletion::Done); }
        int Done(int /* code */, void* /* data */) {
            const bool completionFlag = true;
            mMgr.RunStaleChunksQueue(completionFlag);
            return 0;
        }
        ChunkManager& mMgr;
    };

    bool StartDiskIo();

    /// Map from a chunk id to a chunk handle
    ///
    typedef KVPair<kfsChunkId_t, ChunkInfoHandle*> CMapEntry;
    typedef LinearHash<
        CMapEntry,
        KeyCompare<kfsChunkId_t>,
        DynamicArray<
            SingleLinkedList<CMapEntry>*,
            20 // 2 * sizeof(size_t) = 16 MB initial
        >,
        StdFastAllocator<CMapEntry>
    > CMap;

    /// How long should a pending write be held in LRU
    int mMaxPendingWriteLruSecs;
    /// take a checkpoint once every 2 mins
    int mCheckpointIntervalSecs;

    /// space available for allocation 
    int64_t mTotalSpace;
    /// how much is used up by chunks
    int64_t mUsedSpace;
    int64_t mMinFsAvailableSpace;
    double  mMaxSpaceUtilizationThreshold;

    time_t mNextCheckpointTime;
    int    mMaxOpenChunkFiles;
    int    mMaxOpenFds;
    int    mFdsPerChunk;
    
    /// directories for storing the chunks
    ChunkDirs mChunkDirs;

    /// See the comments in KfsOps.cc near WritePrepareOp related to write handling
    int64_t mWriteId;
    PendingWrites mPendingWrites;

    /// table that maps chunkIds to their associated state
    CMap   mChunkTable;
    size_t mMaxIORequestSize;
    /// Chunk lru, and stale chunks list heads.
    ChunkLists mChunkInfoLists[kChunkInfoListCount];

    /// Periodically do an IO and check the chunk dirs and identify failed drives
    time_t mNextChunkDirsCheckTime;
    int    mChunkDirsCheckIntervalSecs;
    time_t mNextGetFsSpaceAvailableTime;
    int    mGetFsSpaceAvailableIntervalSecs;
    time_t mNextSendChunDirInfoTime;
    int    mSendChunDirInfoIntervalSecs;

    // Cleanup fds on which no I/O has been done for the past N secs
    int    mInactiveFdsCleanupIntervalSecs;
    time_t mNextInactiveFdCleanupTime;
    int    mInactiveFdFullScanIntervalSecs;
    time_t mNextInactiveFdFullScanTime;

    int mReadChecksumMismatchMaxRetryCount;
    bool mAbortOnChecksumMismatchFlag; // For debugging
    bool mRequireChunkHeaderChecksumFlag;
    bool mForceDeleteStaleChunksFlag;
    bool mKeepEvacuatedChunksFlag;
    StaleChunkCompletion mStaleChunkCompletion;
    int mStaleChunkOpsInFlight;
    int mMaxStaleChunkOpsInFlight;
    int mMaxDirCheckDiskTimeouts;
    double mChunkPlacementPendingReadWeight;
    double mChunkPlacementPendingWriteWeight;
    double mMaxPlacementSpaceRatio;
    int64_t mMinPendingIoThreshold;
    bool mAllowSparseChunksFlag;
    bool mBufferedIoFlag;
    bool mSyncChunkHeaderFlag;

    uint32_t mNullBlockChecksum;

    Counters   mCounters;
    DirChecker mDirChecker;
    bool       mCleanupChunkDirsFlag;
    string     mStaleChunksDir;
    string     mDirtyChunksDir;
    string     mEvacuateFileName;
    string     mEvacuateDoneFileName;
    string     mChunkDirLockName;
    int        mEvacuationInactivityTimeout;
    time_t     mMetaHeartbeatTime;
    int64_t    mMetaEvacuateCount;
    int        mMaxEvacuateIoErrors;
    int        mAvailableChunksRetryInterval;

    ChunkHeaderBuffer mChunkHeaderBuffer;

    inline void Delete(ChunkInfoHandle& cih);
    inline void Release(ChunkInfoHandle& cih);

    /// When a checkpoint file is read, update the mChunkTable[] to
    /// include a mapping for cih->chunkInfo.chunkId.
    void AddMapping(ChunkDirInfo& dir, kfsFileId_t fileId, chunkId_t chunkId,
        kfsSeq_t chunkVers, int64_t chunkSize);

    /// Of the various directories this chunkserver is configured with, find the directory to store a chunk file.  
    /// This method does a "directory allocation".
    ChunkDirInfo* GetDirForChunk();

    void CheckChunkDirs();
    void GetFsSpaceAvailable();

    string MakeChunkPathname(const string &chunkdir, kfsFileId_t fid, kfsChunkId_t chunkId, kfsSeq_t chunkVersion);

    /// Utility function that given a chunkId, returns the full path
    /// to the chunk filename in the "stalechunks" dir
    string MakeStaleChunkPathname(ChunkInfoHandle *cih);

    /// update the used space in the directory where the chunk resides by nbytes.
    void UpdateDirSpace(ChunkInfoHandle *cih, int64_t nbytes);

    /// Checksums are computed on 64K blocks.  To verify checksums on
    /// reads, reads are aligned at 64K boundaries and data is read in
    /// 64K blocks.  So, for reads that are un-aligned/read less data,
    /// adjust appropriately.
    void AdjustDataRead(ReadOp *op);

    /// Pad the buffer with sufficient 0's so that checksumming works
    /// out.
    /// @param[in/out] buffer  The buffer to be padded with 0's
    void ZeroPad(IOBuffer *buffer);

    /// Given a chunkId and offset, return the checksum of corresponding
    /// "checksum block"---i.e., the 64K block that contains offset.
    uint32_t GetChecksum(kfsChunkId_t chunkId, int64_t offset);

    /// For any writes that have been held for more than 2 mins,
    /// scavenge them and reclaim memory.
    void ScavengePendingWrites(time_t now);

    /// If we have too many open fd's close out whatever we can.  When
    /// periodic is set, we do a scan and clean up.
    bool CleanupInactiveFds(time_t now, bool forceFlag = false);

    /// For some reason, dirname is not accessable (for instance, the
    /// drive may have failed); in this case, notify metaserver that
    /// all the blocks on that dir are lost and the metaserver can
    /// then re-replicate.
    void NotifyMetaChunksLost(ChunkDirInfo& dir);

    /// Helper function to move a chunk to the stale dir
    int MarkChunkStale(ChunkInfoHandle *cih, KfsCallbackObj* cb);

    /// On a restart, nuke out all the dirty chunks
    void RemoveDirtyChunks();

    /// Scan the chunk dirs and rebuild the list of chunks that are hosted on this server
    void Restore();
    /// Restore the chunk meta-data from the specified file name.
    void RestoreChunkMeta(const string &chunkMetaFn);
    
    /// Update the checksums in the chunk metadata based on the op.
    void UpdateChecksums(ChunkInfoHandle *cih, WriteOp *op);
    bool IsChunkStable(const ChunkInfoHandle* cih) const;
    void RunStaleChunksQueue(bool completionFlag = false);
    int OpenChunk(ChunkInfoHandle* cih, int openFlags);
    void SendChunkDirInfo();
private:
    // No copy.
    ChunkManager(const ChunkManager&);
    ChunkManager& operator=(const ChunkManager&);
};

inline ChunkManager::PendingWrites::OpListEntry::OpListEntry()
{
    BOOST_STATIC_ASSERT(sizeof(mLruIteratorStorage) >= sizeof(LruIterator));
    LruIterator* const i =
        ::new (static_cast<void*>(&mLruIteratorStorage)) LruIterator();
    assert(i == &GetLruIterator());
    (void)i;
}

inline ChunkManager::PendingWrites::OpListEntry::~OpListEntry()
{  GetLruIterator().~LruIterator(); }

inline ChunkManager::PendingWrites::WriteIdEntry::WriteIdEntry(WriteOp* op)
    : OpListEntry(), mOp(op)
{}

extern ChunkManager gChunkManager;

}

#endif // _CHUNKMANAGER_H
