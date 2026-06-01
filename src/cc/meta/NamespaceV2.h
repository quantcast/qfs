//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Memory-native namespace scaffolding for RFC-0001.
//
// Copyright 2026 Quantcast Corporation. All rights reserved.
//
// This file is part of Kosmos File System (KFS).
//
// Licensed under the Apache License, Version 2.0.
//
//----------------------------------------------------------------------------

#ifndef META_NAMESPACE_V2_H
#define META_NAMESPACE_V2_H

#include "common/kfsdecls.h"

#include <stdint.h>

#include <atomic>
#include <cstddef>
#include <iosfwd>

#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include <utility>

class QCMutex;

namespace KFS
{

class Properties;

namespace NamespaceV2
{

typedef uint64_t TxnId;

enum InodeType
{
    kInodeTypeFile,
    kInodeTypeDir,
    kInodeTypeSymlink
};

struct Config
{
    bool enabledFlag;
    bool rpcEnabledFlag;
    int  dirLargeThreshold;
    int  dirPromoteMaxWallMs;
    int  dirShardCount;

    Config();
    static Config FromProperties(const Properties& props);
};

void SetParameters(const Properties& props);
const Config& GetConfig();

struct NameKey
{
    uint64_t    hash;
    std::string name;

    NameKey();
    explicit NameKey(const std::string& name);
    NameKey(uint64_t hash, const std::string& name);

    bool operator<(const NameKey& other) const;
    bool operator==(const NameKey& other) const;
};

struct NameKeyHash
{
    size_t operator()(const NameKey& key) const;
};

struct VersionedDirEntry
{
    fid_t  childFid;
    TxnId  createTxn;
    TxnId  deleteTxn;
    bool   pendingFlag;

    VersionedDirEntry();
    VersionedDirEntry(fid_t childFid, TxnId createTxn);

    bool IsVisible(TxnId committedTxn) const;
};

struct InodeRecord
{
    fid_t     fid;
    fid_t     parentFid;
    InodeType type;
    TxnId     createTxn;
    TxnId     deleteTxn;
    bool      pendingFlag;
    uint64_t  generation;
    kfsUid_t  user;
    kfsGid_t  group;
    kfsMode_t mode;
    int16_t   numReplicas;
    int64_t   mtime;
    int64_t   ctime;
    int64_t   atime;

    InodeRecord();
    InodeRecord(fid_t fid, fid_t parentFid, InodeType type, TxnId createTxn,
        kfsUid_t user = kKfsUserRoot, kfsGid_t group = kKfsGroupRoot,
        kfsMode_t mode = 0, int16_t numReplicas = 1, int64_t mtime = 0);

    bool IsVisible(TxnId committedTxn) const;
};

class InodeTable
{
public:
    InodeTable();
    bool Insert(const InodeRecord& record);
    InodeRecord* Find(fid_t fid);
    const InodeRecord* Find(fid_t fid) const;
    const InodeRecord* FindCommitted(fid_t fid, TxnId committedTxn) const;
    bool MarkDeleted(fid_t fid, TxnId deleteTxn);
    bool Move(fid_t fid, fid_t parentFid);
    void GetCommitted(TxnId committedTxn,
        std::vector<InodeRecord>& records) const;
    void CommitThrough(TxnId committedTxn);
    size_t Size() const;
private:
    typedef std::unordered_map<fid_t, InodeRecord> Table;
    typedef std::vector<Table> Tables;
    Tables mTables;
};

enum DirState
{
    kDirStateSmall,
    kDirStatePromoting,
    kDirStateLarge
};

struct ReaddirCookie
{
    uint64_t generation;
    DirState layout;
    bool     hasLastKeyFlag;
    NameKey  lastKey;

    ReaddirCookie();
};

struct ReaddirResult
{
    struct Entry
    {
        NameKey key;
        fid_t   childFid;

        Entry(const NameKey& key, fid_t childFid);
    };

    std::vector<Entry> entries;
    bool               moreEntriesFlag;
    ReaddirCookie      nextCookie;

    ReaddirResult();
};

struct CheckpointDirEntry
{
    fid_t   parentFid;
    NameKey key;
    fid_t   childFid;

    CheckpointDirEntry(fid_t parentFid, const NameKey& key,
        fid_t childFid);
};

class DirNode
{
public:
    explicit DirNode(
        int largeThreshold = Config().dirLargeThreshold,
        int promoteMaxWallMs = Config().dirPromoteMaxWallMs);

    DirState GetState() const;
    uint64_t GetGeneration() const;
    void SetGeneration(uint64_t generation);
    size_t GetChildCount() const;
    bool IsLarge() const;

    bool HasVisibleOrPendingName(const NameKey& key,
        TxnId committedTxn) const;
    bool HasVisibleOrPendingName(const std::string& name,
        TxnId committedTxn) const;
    int InsertPending(const NameKey& key, fid_t childFid, TxnId txnId,
        TxnId committedTxn = 0, bool replaceDeletedFlag = false);
    int InsertPending(const std::string& name, fid_t childFid, TxnId txnId,
        TxnId committedTxn = 0, bool replaceDeletedFlag = false);
    int InsertCommitted(const std::string& name, fid_t childFid);
    void GetCommittedEntries(TxnId committedTxn,
        std::vector<ReaddirResult::Entry>& entries) const;
    int DeletePending(const std::string& name, TxnId txnId,
        fid_t* childFidPtr = 0);
    const VersionedDirEntry* LookupCommitted(
        const std::string& name, TxnId committedTxn) const;
    int ReaddirCommitted(TxnId committedTxn, const ReaddirCookie* cookiePtr,
        size_t maxEntries, ReaddirResult& result) const;
    void CommitThrough(TxnId committedTxn);

private:
    typedef std::unordered_map<NameKey, VersionedDirEntry, NameKeyHash>
        SmallEntries;
    typedef std::unordered_map<NameKey, VersionedDirEntry, NameKeyHash>
        LargeEntries;

    DirState GetCookieLayout() const;
    int Promote();
    VersionedDirEntry* FindMutable(const NameKey& key);
    const VersionedDirEntry* Find(const NameKey& key) const;
    void IncrementSmallGeneration();

    DirState     mState;
    uint64_t     mGeneration;
    int          mLargeThreshold;
    int          mPromoteMaxWallMs;
    size_t       mChildCount;
    SmallEntries mSmall;
    LargeEntries mLarge;
};

class DirTable
{
public:
    DirTable();
    bool Insert(fid_t dirFid, const DirNode& dir);
    DirNode* Find(fid_t dirFid);
    const DirNode* Find(fid_t dirFid) const;
    void CommitThrough(TxnId committedTxn);
    size_t Size() const;
    void GetCommittedEntries(TxnId committedTxn,
        std::vector<CheckpointDirEntry>& entries) const;
    void GetDirGenerations(
        std::vector<std::pair<fid_t, uint64_t> >& generations) const;

private:
    typedef std::unordered_map<fid_t, DirNode> Table;
    typedef std::vector<Table> Tables;
    Tables mTables;
};

struct LookupResult
{
    fid_t     fid;
    InodeType type;
    uint64_t  parentGeneration;
    kfsUid_t  user;
    kfsGid_t  group;
    kfsMode_t mode;
    int16_t   numReplicas;
    int64_t   mtime;
    int64_t   ctime;
    int64_t   atime;
    int64_t   fileCount;
    int64_t   dirCount;

    LookupResult();
};

struct CreateResult
{
    fid_t    fid;
    TxnId    txnId;
    uint64_t parentGeneration;

    CreateResult();
};

struct EditLogRecord
{
    enum Type
    {
        kInvalid,
        kCreate,
        kRemove,
        kRename
    };

    Type        type;
    TxnId       txnId;
    fid_t       parentFid;
    std::string name;
    fid_t       fid;
    InodeType   inodeType;
    kfsUid_t    user;
    kfsGid_t    group;
    kfsMode_t   mode;
    int16_t     numReplicas;
    int64_t     mtime;
    std::string newPath;
    bool        overwriteFlag;

    EditLogRecord();
};

int WriteEditLog(std::ostream& os, const EditLogRecord& record);
int ReadEditLog(const std::string& line, EditLogRecord& record);

class NamespaceStore
{
public:
    explicit NamespaceStore(
        const Config& config = GetConfig(), fid_t rootFid = ROOTFID);

    fid_t GetRootFid() const;
    TxnId GetCommittedTxn() const;
    TxnId GetLastTxn() const;
    size_t GetInodeCount() const;
    size_t GetDirCount() const;
    void ReserveCreateIds(fid_t& childFid, TxnId& txnId);
    void ReserveCreateIdsRange(
        size_t count, fid_t& firstFid, TxnId& firstTxn);

    int Create(fid_t parentFid, const std::string& name, InodeType type,
        CreateResult* resultPtr = 0, kfsUid_t user = kKfsUserRoot,
        kfsGid_t group = kKfsGroupRoot, kfsMode_t mode = 0,
        int16_t numReplicas = 1, int64_t mtime = 0);
    // Test/bench helper: ReserveCreateIds + ApplyCreate (pending until CommitThrough).
    int ApplyCreatePending(fid_t parentFid, const std::string& name,
        InodeType type, CreateResult* resultPtr = 0,
        kfsUid_t user = kKfsUserRoot, kfsGid_t group = kKfsGroupRoot,
        kfsMode_t mode = 0, int16_t numReplicas = 1, int64_t mtime = 0);
    int Lookup(fid_t parentFid, const std::string& name,
        LookupResult& result) const;
    int LookupPath(fid_t rootFid, const std::string& path,
        LookupResult& result) const;
    int GetAttr(fid_t fid, LookupResult& result) const;
    int Readdir(fid_t dirFid, const ReaddirCookie* cookiePtr,
        size_t maxEntries, ReaddirResult& result) const;
    int ReaddirFromName(fid_t dirFid, const std::string& name,
        size_t maxEntries, ReaddirResult& result) const;
    int Remove(fid_t parentFid, const std::string& name,
        TxnId* txnIdPtr = 0);
    int RemoveFile(fid_t parentFid, const std::string& name,
        TxnId* txnIdPtr = 0);
    int Rmdir(fid_t parentFid, const std::string& name,
        TxnId* txnIdPtr = 0);
    int Rename(fid_t parentFid, const std::string& oldName,
        const std::string& newPath, bool overwriteFlag,
        TxnId* txnIdPtr = 0, fid_t* srcFidPtr = 0);
    int ApplyEditLog(const EditLogRecord& record, bool commitFlag = true);
    int ApplyEditLog(std::istream& is);
    int ApplyCreate(fid_t parentFid, const std::string& name, InodeType type,
        fid_t childFid, TxnId txnId, kfsUid_t user = kKfsUserRoot,
        kfsGid_t group = kKfsGroupRoot, kfsMode_t mode = 0,
        int16_t numReplicas = 1, int64_t mtime = 0,
        bool commitFlag = true, bool advanceSeedsFlag = false);
    // Trusted apply fast path: assumes WAL / RPC start already validated
    // parent/name/type semantics. Replay must not use this.
    int ApplyCreateTrusted(fid_t parentFid, const std::string& name,
        InodeType type, fid_t childFid, TxnId txnId,
        kfsUid_t user = kKfsUserRoot, kfsGid_t group = kKfsGroupRoot,
        kfsMode_t mode = 0, int16_t numReplicas = 1, int64_t mtime = 0,
        bool commitFlag = true, bool advanceSeedsFlag = false);
    int SaveCheckpoint(std::ostream& os) const;
    int SaveCheckpointDiskEntry(std::ostream& os) const;
    int LoadCheckpoint(std::istream& is);
    void CommitThrough(TxnId committedTxn);
    void CommitThroughRange(TxnId firstTxn, TxnId lastTxn);

private:
    TxnId GetCommittedTxnSnapshot() const;
    TxnId GetLastTxnSnapshot() const;
    void AllocateCreateIds(fid_t& childFid, TxnId& txnId);
    TxnId AllocateTxnId();
    void AdvanceSeeds(fid_t fid, TxnId txnId);
    const InodeRecord* FindCommittedDir(
        fid_t dirFid, TxnId committedTxn) const;
    int ResolveCreateParentDir(
        fid_t parentFid,
        TxnId committedTxn,
        DirNode*& dirPtr);
    int CheckCreateParentName(
        fid_t              parentFid,
        const NameKey&     key,
        TxnId              committedTxn);
    int CreateSelf(fid_t parentFid, const NameKey& key, InodeType type,
        fid_t childFid, TxnId txnId, CreateResult* resultPtr,
        kfsUid_t user, kfsGid_t group, kfsMode_t mode,
        int16_t numReplicas, int64_t mtime,
        TxnId committedTxn);
    int CreateSelfTrusted(fid_t parentFid, const NameKey& key, InodeType type,
        fid_t childFid, TxnId txnId, CreateResult* resultPtr,
        kfsUid_t user, kfsGid_t group, kfsMode_t mode,
        int16_t numReplicas, int64_t mtime,
        TxnId committedTxn);
    int CreateSelf(fid_t parentFid, const std::string& name, InodeType type,
        fid_t childFid, TxnId txnId, CreateResult* resultPtr,
        kfsUid_t user, kfsGid_t group, kfsMode_t mode,
        int16_t numReplicas, int64_t mtime,
        TxnId committedTxn);
    int ResolveRenameTarget(fid_t baseDirFid, const std::string& newPath,
        fid_t& dstParentFid, std::string& dstName) const;
    bool IsDescendant(fid_t ancestorFid, fid_t dirFid,
        TxnId committedTxn) const;
    int RemoveSelf(fid_t parentFid, const std::string& name,
        InodeType type, bool requireEmptyFlag, TxnId txnId,
        TxnId* txnIdPtr);
    int RenameSelf(fid_t parentFid, const std::string& oldName,
        const std::string& newPath, bool overwriteFlag, TxnId txnId,
        TxnId* txnIdPtr, fid_t* srcFidPtr);
    int FillLookupResult(const InodeRecord& inode,
        uint64_t parentGeneration, LookupResult& result) const;

    Config     mConfig;
    fid_t      mRootFid;
    fid_t      mNextFid;
    TxnId      mNextTxn;
    std::atomic<TxnId> mCommittedTxn;
    std::set<TxnId> mPendingCommittedTxns;
    InodeTable mInodes;
    DirTable   mDirs;
};

NamespaceStore& GetStore();

struct ResourceLockKey
{
    enum Class
    {
        kSnapshot = 1,
        kDir      = 2,
        kInode    = 3,
        kBlockMap = 4,
        kEditLog  = 5
    };

    Class    resourceClass;
    uint64_t major;
    uint64_t minor;

    ResourceLockKey(Class resourceClass, uint64_t major, uint64_t minor = 0);

    bool operator<(const ResourceLockKey& other) const;
};

} // namespace NamespaceV2
} // namespace KFS

#endif // META_NAMESPACE_V2_H
