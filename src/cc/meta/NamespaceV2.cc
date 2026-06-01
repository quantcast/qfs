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

#include "NamespaceV2.h"

#include "common/Properties.h"
#include "common/hsieh_hash.h"
#include "common/time.h"
#include "qcdio/QCMutex.h"

#include <algorithm>
#include <errno.h>
#include <functional>
#include <utility>
#include <limits>
#include <istream>
#include <ostream>
#include <sstream>

namespace KFS
{
namespace NamespaceV2
{

namespace
{
    const TxnId kNoTxn = 0;
    Config      sConfig;
    enum { kNamespaceV2ShardCount = 1024 };

    size_t
    GetLockShard(
        fid_t fid)
    {
        return (size_t)((uint64_t)fid * 11400714819323198485ULL) %
            kNamespaceV2ShardCount;
    }

    QCMutex&
    GetTxnMutex()
    {
        static QCMutex sMutex;
        return sMutex;
    }

    QCMutex*
    GetDirShardMutexes()
    {
        static QCMutex sLocks[kNamespaceV2ShardCount];
        return sLocks;
    }

    QCMutex*
    GetInodeShardMutexes()
    {
        static QCMutex sLocks[kNamespaceV2ShardCount];
        return sLocks;
    }

    QCMutex&
    GetDirShardMutex(
        fid_t fid)
    {
        return GetDirShardMutexes()[GetLockShard(fid)];
    }

    QCMutex&
    GetInodeShardMutex(
        fid_t fid)
    {
        return GetInodeShardMutexes()[GetLockShard(fid)];
    }

    void
    AddMutex(
        std::vector<QCMutex*>& locks,
        QCMutex& mutex)
    {
        locks.push_back(&mutex);
    }

    void
    AddDirShardMutex(
        std::vector<QCMutex*>& locks,
        fid_t fid)
    {
        AddMutex(locks, GetDirShardMutex(fid));
    }

    void
    AddInodeShardMutex(
        std::vector<QCMutex*>& locks,
        fid_t fid)
    {
        AddMutex(locks, GetInodeShardMutex(fid));
    }

    void
    AddAllDirShardMutexes(
        std::vector<QCMutex*>& locks)
    {
        QCMutex* const mutexes = GetDirShardMutexes();
        for (size_t i = 0; i < kNamespaceV2ShardCount; ++i) {
            AddMutex(locks, mutexes[i]);
        }
    }

    void
    AddAllInodeShardMutexes(
        std::vector<QCMutex*>& locks)
    {
        QCMutex* const mutexes = GetInodeShardMutexes();
        for (size_t i = 0; i < kNamespaceV2ShardCount; ++i) {
            AddMutex(locks, mutexes[i]);
        }
    }

    class ScopedMutex
    {
    public:
        explicit ScopedMutex(
            QCMutex& mutex)
            : mMutex(mutex)
        {
            mMutex.Lock();
        }
        ~ScopedMutex()
        {
            mMutex.Unlock();
        }
    private:
        QCMutex& mMutex;

        ScopedMutex(const ScopedMutex&);
        ScopedMutex& operator=(const ScopedMutex&);
    };

    class ScopedMutexGroup
    {
    public:
        explicit ScopedMutexGroup(
            std::vector<QCMutex*> locks)
            : mLocks(locks)
        {
            std::sort(mLocks.begin(), mLocks.end());
            mLocks.erase(std::unique(mLocks.begin(), mLocks.end()),
                mLocks.end());
            for (std::vector<QCMutex*>::iterator it = mLocks.begin();
                    it != mLocks.end();
                    ++it) {
                (*it)->Lock();
            }
        }
        ~ScopedMutexGroup()
        {
            for (std::vector<QCMutex*>::reverse_iterator it =
                        mLocks.rbegin();
                    it != mLocks.rend();
                    ++it) {
                (*it)->Unlock();
            }
        }
    private:
        std::vector<QCMutex*> mLocks;

        ScopedMutexGroup(const ScopedMutexGroup&);
        ScopedMutexGroup& operator=(const ScopedMutexGroup&);
    };

    static void
    SortUniqueMutexPtrs(
        QCMutex** locks,
        size_t&   count)
    {
        if (count <= 1) {
            return;
        }
        if (count == 2) {
            if (locks[0] > locks[1]) {
                QCMutex* const tmp = locks[0];
                locks[0] = locks[1];
                locks[1] = tmp;
            }
            return;
        }
        if (count == 3) {
            if (locks[0] > locks[1]) {
                QCMutex* const tmp = locks[0];
                locks[0] = locks[1];
                locks[1] = tmp;
            }
            if (locks[1] > locks[2]) {
                QCMutex* const tmp = locks[1];
                locks[1] = locks[2];
                locks[2] = tmp;
            }
            if (locks[0] > locks[1]) {
                QCMutex* const tmp = locks[0];
                locks[0] = locks[1];
                locks[1] = tmp;
            }
            return;
        }
        if (count == 4) {
            for (size_t i = 1; i < count; ++i) {
                QCMutex* const key = locks[i];
                size_t j = i;
                while (j > 0 && locks[j - 1] > key) {
                    locks[j] = locks[j - 1];
                    --j;
                }
                locks[j] = key;
            }
            return;
        }
        std::sort(locks, locks + count);
    }

    class ScopedSmallMutexGroup
    {
    public:
        ScopedSmallMutexGroup(
            QCMutex* lock0,
            QCMutex* lock1,
            QCMutex* lock2 = 0,
            QCMutex* lock3 = 0)
            : mCount(0)
        {
            Add(lock0);
            Add(lock1);
            Add(lock2);
            Add(lock3);
            SortUniqueMutexPtrs(mLocks, mCount);
            mCount = DedupeMutexPtrs(mLocks, mCount);
            for (size_t i = 0; i < mCount; ++i) {
                mLocks[i]->Lock();
            }
        }
        ~ScopedSmallMutexGroup()
        {
            while (mCount > 0) {
                mLocks[--mCount]->Unlock();
            }
        }
    private:
        static size_t
        DedupeMutexPtrs(
            QCMutex** locks,
            size_t    count)
        {
            if (count <= 1) {
                return count;
            }
            size_t out = 1;
            for (size_t i = 1; i < count; ++i) {
                if (locks[i] != locks[out - 1]) {
                    locks[out++] = locks[i];
                }
            }
            return out;
        }
        void Add(QCMutex* mutex)
        {
            if (mutex) {
                mLocks[mCount++] = mutex;
            }
        }

        QCMutex* mLocks[4];
        size_t   mCount;

        ScopedSmallMutexGroup(const ScopedSmallMutexGroup&);
        ScopedSmallMutexGroup& operator=(const ScopedSmallMutexGroup&);
    };


    uint64_t
    HashName(
        const std::string& name)
    {
        Hsieh_hash_fcn hash;
        return (uint64_t(hash(name)) << 4);
    }

    bool
    IsDeletedAt(
        const VersionedDirEntry& entry,
        TxnId                    committedTxn)
    {
        return entry.deleteTxn != kNoTxn && entry.deleteTxn <= committedTxn;
    }

    bool
    IsLegalName(
        const std::string& name)
    {
        return ! name.empty() && name.size() <= MAX_FILE_NAME_LENGTH &&
            name.find_first_of("/\n") == std::string::npos;
    }

    bool
    IsSupportedInodeType(
        InodeType type)
    {
        return type == kInodeTypeFile || type == kInodeTypeDir ||
            type == kInodeTypeSymlink;
    }

    bool
    IsVisibleOrPendingEntry(
        const VersionedDirEntry* entry,
        TxnId                    committedTxn)
    {
        if (! entry) {
            return false;
        }
        if (entry->deleteTxn == kNoTxn) {
            return true;
        }
        if (! entry->pendingFlag) {
            return false;
        }
        return committedTxn == kNoTxn || committedTxn < entry->deleteTxn;
    }

    int
    ValidateCreateRequest(
        const std::string& name,
        InodeType          type)
    {
        if (! IsLegalName(name) || ! IsSupportedInodeType(type)) {
            return -EINVAL;
        }
        return 0;
    }

    int
    ValidateCreateRequest(
        const std::string& name,
        InodeType          type,
        fid_t              childFid,
        TxnId              txnId,
        TxnId              committedTxn)
    {
        const int status = ValidateCreateRequest(name, type);
        if (status != 0) {
            return status;
        }
        if (childFid < 0 || txnId == kNoTxn || txnId <= committedTxn) {
            return -EINVAL;
        }
        return 0;
    }

    char
    HexDigit(
        int value)
    {
        return (char)(value < 10 ? 48 + value : 97 + value - 10);
    }

    int
    HexValue(
        char value)
    {
        return 48 <= value && value <= 57 ? value - 48 :
            (97 <= value && value <= 102 ? value - 97 + 10 :
            (65 <= value && value <= 70 ? value - 65 + 10 : -1));
    }

    std::string
    EncodeString(
        const std::string& value)
    {
        std::string result;
        result.reserve(value.size() * 2);
        for (size_t i = 0; i < value.size(); ++i) {
            const int byte = (unsigned char)value[i];
            result.push_back(HexDigit((byte >> 4) & 0xf));
            result.push_back(HexDigit(byte & 0xf));
        }
        return result;
    }

    std::string
    EncodeName(
        const std::string& name)
    {
        return EncodeString(name);
    }

    bool
    DecodeHexString(
        const std::string& encoded,
        std::string&       value)
    {
        if (encoded.size() % 2 != 0) {
            return false;
        }
        std::string result;
        result.reserve(encoded.size() / 2);
        for (size_t i = 0; i < encoded.size(); i += 2) {
            const int hi = HexValue(encoded[i]);
            const int lo = HexValue(encoded[i + 1]);
            if (hi < 0 || lo < 0) {
                return false;
            }
            result.push_back((char)((hi << 4) | lo));
        }
        value.swap(result);
        return true;
    }

    bool
    DecodeName(
        const std::string& encoded,
        std::string&       name)
    {
        std::string result;
        if (! DecodeHexString(encoded, result) || ! IsLegalName(result)) {
            return false;
        }
        name.swap(result);
        return true;
    }

    bool
    DecodePath(
        const std::string& encoded,
        std::string&       path)
    {
        std::string result;
        if (! DecodeHexString(encoded, result) || result.empty() ||
                result.find(char(10)) != std::string::npos) {
            return false;
        }
        path.swap(result);
        return true;
    }

    int
    InodeTypeToInt(
        InodeType type)
    {
        return type == kInodeTypeDir ? 1 :
            (type == kInodeTypeSymlink ? 2 : 0);
    }

    bool
    IntToInodeType(
        int        value,
        InodeType& type)
    {
        if (value == 0) {
            type = kInodeTypeFile;
            return true;
        }
        if (value == 1) {
            type = kInodeTypeDir;
            return true;
        }
        if (value == 2) {
            type = kInodeTypeSymlink;
            return true;
        }
        return false;
    }

    const char*
    EditLogRecordTypeName(
        EditLogRecord::Type type)
    {
        return type == EditLogRecord::kCreate ? "create" :
            (type == EditLogRecord::kRemove ? "remove" :
            (type == EditLogRecord::kRename ? "rename" : "invalid"));
    }

    int
    ValidateEditLogRecord(
        const EditLogRecord& record)
    {
        if (record.txnId == kNoTxn || record.parentFid < 0) {
            return -EINVAL;
        }
        if (record.type == EditLogRecord::kCreate) {
            if (record.fid < 0 || ! IsLegalName(record.name) ||
                    (record.inodeType != kInodeTypeFile &&
                    record.inodeType != kInodeTypeDir &&
                    record.inodeType != kInodeTypeSymlink)) {
                return -EINVAL;
            }
            return 0;
        }
        if (record.type == EditLogRecord::kRemove) {
            return IsLegalName(record.name) ? 0 : -EINVAL;
        }
        if (record.type == EditLogRecord::kRename) {
            if (record.fid < 0 || ! IsLegalName(record.name) ||
                    record.newPath.empty() ||
                    record.newPath.find(char(10)) != std::string::npos) {
                return -EINVAL;
            }
            return 0;
        }
        return -EINVAL;
    }
}

Config::Config()
    : enabledFlag(false),
      rpcEnabledFlag(false),
      dirLargeThreshold(4096),
      dirPromoteMaxWallMs(1000),
      dirShardCount(128)
    {}

    Config
Config::FromProperties(
    const Properties& props)
{
    Config cfg;
    cfg.enabledFlag = props.getValue(
        "metaServer.namespaceV2.enabled", cfg.enabledFlag ? 1 : 0) != 0;
    cfg.rpcEnabledFlag = props.getValue(
        "metaServer.namespaceV2.rpcEnabled",
        cfg.rpcEnabledFlag ? 1 : 0) != 0;
    cfg.dirLargeThreshold = std::max(1, props.getValue(
        "metaServer.dir.largeThreshold", cfg.dirLargeThreshold));
    cfg.dirPromoteMaxWallMs = std::max(1, props.getValue(
        "metaServer.dir.promoteMaxWallMs", cfg.dirPromoteMaxWallMs));
    cfg.dirShardCount = std::max(1, props.getValue(
        "metaServer.namespaceV2.dirShardCount", cfg.dirShardCount));
    return cfg;
}

    void
SetParameters(
    const Properties& props)
{
    sConfig = Config::FromProperties(props);
}

    const Config&
GetConfig()
{
    return sConfig;
}


    NamespaceStore&
GetStore()
{
    static NamespaceStore* sStorePtr = 0;
    if (! sStorePtr) {
        sStorePtr = new NamespaceStore(GetConfig());
    }
    return *sStorePtr;
}

NameKey::NameKey()
    : hash(0),
      name()
    {}

NameKey::NameKey(
    const std::string& inName)
    : hash(HashName(inName)),
      name(inName)
    {}

NameKey::NameKey(
    uint64_t           inHash,
    const std::string& inName)
    : hash(inHash),
      name(inName)
    {}

    bool
NameKey::operator<(
    const NameKey& other) const
{
    return hash < other.hash || (hash == other.hash && name < other.name);
}

    bool
NameKey::operator==(
    const NameKey& other) const
{
    return hash == other.hash && name == other.name;
}

    size_t
NameKeyHash::operator()(
    const NameKey& key) const
{
    return size_t(key.hash ^ (key.hash >> 33)) ^
        (std::hash<std::string>()(key.name) << 1);
}

VersionedDirEntry::VersionedDirEntry()
    : childFid(-1),
      createTxn(kNoTxn),
      deleteTxn(kNoTxn),
      pendingFlag(false)
    {}

VersionedDirEntry::VersionedDirEntry(
    fid_t childFid,
    TxnId createTxn)
    : childFid(childFid),
      createTxn(createTxn),
      deleteTxn(kNoTxn),
      pendingFlag(true)
    {}

    bool
VersionedDirEntry::IsVisible(
    TxnId committedTxn) const
{
    return createTxn <= committedTxn && ! IsDeletedAt(*this, committedTxn);
}

InodeRecord::InodeRecord()
    : fid(-1),
      parentFid(-1),
      type(kInodeTypeFile),
      createTxn(kNoTxn),
      deleteTxn(kNoTxn),
      pendingFlag(false),
      generation(0),
      user(kKfsUserRoot),
      group(kKfsGroupRoot),
      mode(0),
      numReplicas(1),
      mtime(0),
      ctime(0),
      atime(0)
    {}

InodeRecord::InodeRecord(
    fid_t     inFid,
    fid_t     inParentFid,
    InodeType inType,
    TxnId     inCreateTxn,
    kfsUid_t  inUser,
    kfsGid_t  inGroup,
    kfsMode_t inMode,
    int16_t   inNumReplicas,
    int64_t   inMtime)
    : fid(inFid),
      parentFid(inParentFid),
      type(inType),
      createTxn(inCreateTxn),
      deleteTxn(kNoTxn),
      pendingFlag(true),
      generation(0),
      user(inUser),
      group(inGroup),
      mode(inMode),
      numReplicas(inNumReplicas),
      mtime(inMtime),
      ctime(inMtime),
      atime(inMtime)
    {}

    bool
InodeRecord::IsVisible(
    TxnId committedTxn) const
{
    return createTxn <= committedTxn &&
        (deleteTxn == kNoTxn || committedTxn < deleteTxn);
}

    InodeTable::InodeTable()
    : mTables(kNamespaceV2ShardCount)
    {}

    bool
InodeTable::Insert(
    const InodeRecord& record)
{
    Table& table = mTables[GetLockShard(record.fid)];
    return table.insert(std::make_pair(record.fid, record)).second;
}

    InodeRecord*
InodeTable::Find(
    fid_t fid)
{
    Table& table = mTables[GetLockShard(fid)];
    Table::iterator const it = table.find(fid);
    return it == table.end() ? 0 : &it->second;
}

    const InodeRecord*
InodeTable::Find(
    fid_t fid) const
{
    const Table& table = mTables[GetLockShard(fid)];
    Table::const_iterator const it = table.find(fid);
    return it == table.end() ? 0 : &it->second;
}

    const InodeRecord*
InodeTable::FindCommitted(
    fid_t fid,
    TxnId committedTxn) const
{
    const InodeRecord* const record = Find(fid);
    return record && record->IsVisible(committedTxn) ? record : 0;
}

    bool
InodeTable::MarkDeleted(
    fid_t fid,
    TxnId deleteTxn)
{
    InodeRecord* const record = Find(fid);
    if (! record || record->deleteTxn != kNoTxn) {
        return false;
    }
    record->deleteTxn   = deleteTxn;
    record->pendingFlag = true;
    record->generation++;
    return true;
}

    bool
InodeTable::Move(
    fid_t fid,
    fid_t parentFid)
{
    InodeRecord* const record = Find(fid);
    if (! record || record->deleteTxn != kNoTxn) {
        return false;
    }
    record->parentFid = parentFid;
    record->generation++;
    return true;
}

    void
InodeTable::GetCommitted(
    TxnId committedTxn,
    std::vector<InodeRecord>& records) const
{
    records.clear();
    size_t size = 0;
    for (Tables::const_iterator tableIt = mTables.begin();
            tableIt != mTables.end();
            ++tableIt) {
        size += tableIt->size();
    }
    records.reserve(size);
    for (Tables::const_iterator tableIt = mTables.begin();
            tableIt != mTables.end();
            ++tableIt) {
        for (Table::const_iterator it = tableIt->begin();
                it != tableIt->end();
                ++it) {
            if (it->second.IsVisible(committedTxn)) {
                records.push_back(it->second);
            }
        }
    }
    std::sort(records.begin(), records.end(),
        [](const InodeRecord& lhs, const InodeRecord& rhs) {
            return lhs.fid < rhs.fid;
        });
}

    void
InodeTable::CommitThrough(
    TxnId committedTxn)
{
    for (Tables::iterator tableIt = mTables.begin();
            tableIt != mTables.end();
            ++tableIt) {
        for (Table::iterator it = tableIt->begin();
                it != tableIt->end();
                ++it) {
            if (it->second.createTxn <= committedTxn &&
                    (it->second.deleteTxn == kNoTxn ||
                        it->second.deleteTxn <= committedTxn)) {
                it->second.pendingFlag = false;
            }
        }
    }
}

    size_t
InodeTable::Size() const
{
    size_t ret = 0;
    for (Tables::const_iterator it = mTables.begin();
            it != mTables.end();
            ++it) {
        ret += it->size();
    }
    return ret;
}

ReaddirCookie::ReaddirCookie()
    : generation(0),
      layout(kDirStateSmall),
      hasLastKeyFlag(false),
      lastKey()
    {}

ReaddirResult::Entry::Entry(
    const NameKey& inKey,
    fid_t          inChildFid)
    : key(inKey),
      childFid(inChildFid)
    {}

ReaddirResult::ReaddirResult()
    : entries(),
      moreEntriesFlag(false),
      nextCookie()
    {}

CheckpointDirEntry::CheckpointDirEntry(
    fid_t          inParentFid,
    const NameKey& inKey,
    fid_t          inChildFid)
    : parentFid(inParentFid),
      key(inKey),
      childFid(inChildFid)
    {}

DirNode::DirNode(
    int largeThreshold,
    int promoteMaxWallMs)
    : mState(kDirStateSmall),
      mGeneration(0),
      mLargeThreshold(std::max(1, largeThreshold)),
      mPromoteMaxWallMs(std::max(1, promoteMaxWallMs)),
      mChildCount(0),
      mSmall(),
      mLarge()
    {}

    DirState
DirNode::GetCookieLayout() const
{
    return mState == kDirStateLarge ? kDirStateLarge : kDirStateSmall;
}

    DirState
DirNode::GetState() const
{
    return mState;
}

    uint64_t
DirNode::GetGeneration() const
{
    return mGeneration;
}

    void
DirNode::SetGeneration(
    uint64_t generation)
{
    mGeneration = generation;
}

    size_t
DirNode::GetChildCount() const
{
    return mChildCount;
}

    bool
DirNode::IsLarge() const
{
    return mState == kDirStateLarge;
}

    bool
DirNode::HasVisibleOrPendingName(
    const NameKey& key,
    TxnId          committedTxn) const
{
    return IsVisibleOrPendingEntry(Find(key), committedTxn);
}

    bool
DirNode::HasVisibleOrPendingName(
    const std::string& name,
    TxnId              committedTxn) const
{
    return HasVisibleOrPendingName(NameKey(name), committedTxn);
}

    int
DirNode::InsertPending(
    const std::string& name,
    fid_t              childFid,
    TxnId              txnId,
    TxnId              committedTxn,
    bool               replaceDeletedFlag)
{
    if (! IsLegalName(name)) {
        return -EINVAL;
    }
    return InsertPending(NameKey(name), childFid, txnId, committedTxn,
        replaceDeletedFlag);
}

    int
DirNode::InsertPending(
    const NameKey& key,
    fid_t          childFid,
    TxnId          txnId,
    TxnId          committedTxn,
    bool           replaceDeletedFlag)
{
    if (txnId == kNoTxn || childFid < 0) {
        return -EINVAL;
    }
    if (mState == kDirStatePromoting) {
        return -EBUSY;
    }
    VersionedDirEntry* const oldEntry = FindMutable(key);
    const bool replaceFlag = replaceDeletedFlag && oldEntry &&
        oldEntry->deleteTxn == txnId;
    if (! replaceFlag &&
            IsVisibleOrPendingEntry(oldEntry, committedTxn)) {
        return -EEXIST;
    }
    if (mState == kDirStateSmall && ! replaceFlag &&
            (int)(mSmall.size() + 1) > mLargeThreshold) {
        const int status = Promote();
        if (status != 0) {
            return status;
        }
    }
    VersionedDirEntry entry(childFid, txnId);
    if (mState == kDirStateLarge) {
        mLarge[key] = entry;
    } else {
        mSmall[key] = entry;
        IncrementSmallGeneration();
    }
    mChildCount++;
    return 0;
}

    int
DirNode::InsertCommitted(
    const std::string& name,
    fid_t              childFid)
{
    if (childFid < 0 || ! IsLegalName(name)) {
        return -EINVAL;
    }
    const NameKey key(name);
    if (Find(key)) {
        return -EEXIST;
    }
    if (mState == kDirStateSmall &&
            (int)(mSmall.size() + 1) > mLargeThreshold) {
        const int status = Promote();
        if (status != 0) {
            return status;
        }
    }
    VersionedDirEntry entry(childFid, kNoTxn);
    entry.pendingFlag = false;
    if (mState == kDirStateLarge) {
        mLarge[key] = entry;
    } else {
        mSmall[key] = entry;
        IncrementSmallGeneration();
    }
    mChildCount++;
    return 0;
}

    void
DirNode::GetCommittedEntries(
    TxnId committedTxn,
    std::vector<ReaddirResult::Entry>& entries) const
{
    if (mState == kDirStateLarge) {
        for (LargeEntries::const_iterator it = mLarge.begin();
                it != mLarge.end();
                ++it) {
            if (it->second.IsVisible(committedTxn)) {
                entries.push_back(ReaddirResult::Entry(
                    it->first, it->second.childFid));
            }
        }
    } else {
        std::vector<ReaddirResult::Entry> sorted;
        sorted.reserve(mSmall.size());
        for (SmallEntries::const_iterator it = mSmall.begin();
                it != mSmall.end();
                ++it) {
            if (it->second.IsVisible(committedTxn)) {
                sorted.push_back(ReaddirResult::Entry(
                    it->first, it->second.childFid));
            }
        }
        std::sort(sorted.begin(), sorted.end(),
            [](const ReaddirResult::Entry& lhs,
                    const ReaddirResult::Entry& rhs) {
                return lhs.key < rhs.key;
            });
        entries.insert(entries.end(), sorted.begin(), sorted.end());
    }
}

    int
DirNode::DeletePending(
    const std::string& name,
    TxnId              txnId,
    fid_t*             childFidPtr)
{
    if (txnId == kNoTxn) {
        return -EINVAL;
    }
    VersionedDirEntry* const entry = FindMutable(NameKey(name));
    if (! entry || entry->deleteTxn != kNoTxn) {
        return -ENOENT;
    }
    entry->deleteTxn   = txnId;
    entry->pendingFlag = true;
    if (childFidPtr) {
        *childFidPtr = entry->childFid;
    }
    if (mChildCount > 0) {
        mChildCount--;
    }
    if (mState != kDirStateLarge) {
        IncrementSmallGeneration();
    }
    return 0;
}

    const VersionedDirEntry*
DirNode::LookupCommitted(
    const std::string& name,
    TxnId              committedTxn) const
{
    const VersionedDirEntry* const entry = Find(NameKey(name));
    return entry && entry->IsVisible(committedTxn) ? entry : 0;
}

    int
DirNode::ReaddirCommitted(
    TxnId                 committedTxn,
    const ReaddirCookie*  cookiePtr,
    size_t                maxEntries,
    ReaddirResult&        result) const
{
    result = ReaddirResult();
    if (maxEntries == 0) {
        return 0;
    }
    const DirState cookieLayout = GetCookieLayout();
    if (cookiePtr) {
        if (cookiePtr->generation != mGeneration ||
                cookiePtr->layout != cookieLayout) {
            return -EINVAL;
        }
    }
    result.nextCookie.generation = mGeneration;
    result.nextCookie.layout     = cookieLayout;
    const bool hasLastKeyFlag = cookiePtr && cookiePtr->hasLastKeyFlag;
    const NameKey lastKey = hasLastKeyFlag ? cookiePtr->lastKey : NameKey();
    const SmallEntries* const entriesMap =
        mState == kDirStateLarge ? 0 : &mSmall;
    const LargeEntries* const largeMap =
        mState == kDirStateLarge ? &mLarge : 0;
    std::vector<std::pair<NameKey, VersionedDirEntry> > entries;
    if (largeMap) {
        entries.reserve(largeMap->size());
        for (LargeEntries::const_iterator it = largeMap->begin();
                it != largeMap->end();
                ++it) {
            if (it->second.IsVisible(committedTxn) &&
                    (! hasLastKeyFlag || lastKey < it->first)) {
                entries.push_back(*it);
            }
        }
    } else if (entriesMap) {
        entries.reserve(entriesMap->size());
        for (SmallEntries::const_iterator it = entriesMap->begin();
                it != entriesMap->end();
                ++it) {
            if (it->second.IsVisible(committedTxn) &&
                    (! hasLastKeyFlag || lastKey < it->first)) {
                entries.push_back(*it);
            }
        }
    }
    std::sort(entries.begin(), entries.end(),
        [](const std::pair<NameKey, VersionedDirEntry>& lhs,
                const std::pair<NameKey, VersionedDirEntry>& rhs) {
            return lhs.first < rhs.first;
        });
    for (std::vector<std::pair<NameKey, VersionedDirEntry> >::const_iterator
            it = entries.begin();
            it != entries.end();
            ++it) {
        if (result.entries.size() >= maxEntries) {
            result.moreEntriesFlag = true;
            break;
        }
        result.entries.push_back(ReaddirResult::Entry(
            it->first, it->second.childFid));
    }
    if (! result.entries.empty()) {
        result.nextCookie.hasLastKeyFlag = true;
        result.nextCookie.lastKey = result.entries.back().key;
    }
    return 0;
}

    void
DirNode::CommitThrough(
    TxnId committedTxn)
{
    if (mState == kDirStateLarge) {
        for (LargeEntries::iterator it = mLarge.begin();
                it != mLarge.end();
                ++it) {
            if (it->second.createTxn <= committedTxn &&
                    (it->second.deleteTxn == kNoTxn ||
                        it->second.deleteTxn <= committedTxn)) {
                it->second.pendingFlag = false;
            }
        }
    } else {
        for (SmallEntries::iterator it = mSmall.begin();
                it != mSmall.end();
                ++it) {
            if (it->second.createTxn <= committedTxn &&
                    (it->second.deleteTxn == kNoTxn ||
                        it->second.deleteTxn <= committedTxn)) {
                it->second.pendingFlag = false;
            }
        }
    }
}

    int
DirNode::Promote()
{
    if (mState == kDirStateLarge) {
        return 0;
    }
    if (mState != kDirStateSmall) {
        return -EINVAL;
    }
    enum { kPromoteBatchSize = 512 };
    mState = kDirStatePromoting;
    const int64_t deadlineUsec = microseconds() +
        (int64_t)mPromoteMaxWallMs * 1000;
    LargeEntries staging;
    staging.reserve(mSmall.size());
    SmallEntries::const_iterator it = mSmall.begin();
    while (it != mSmall.end()) {
        for (size_t batch = 0;
                batch < kPromoteBatchSize && it != mSmall.end();
                ++batch, ++it) {
            staging.insert(*it);
        }
        if (it != mSmall.end() && microseconds() > deadlineUsec) {
            mState = kDirStateSmall;
            return -EBUSY;
        }
    }
    mLarge.swap(staging);
    mSmall.clear();
    mState = kDirStateLarge;
    mGeneration++;
    return 0;
}

    VersionedDirEntry*
DirNode::FindMutable(
    const NameKey& key)
{
    if (mState == kDirStateLarge) {
        LargeEntries::iterator const it = mLarge.find(key);
        return it == mLarge.end() ? 0 : &it->second;
    }
    SmallEntries::iterator const it = mSmall.find(key);
    return it == mSmall.end() ? 0 : &it->second;
}

    const VersionedDirEntry*
DirNode::Find(
    const NameKey& key) const
{
    if (mState == kDirStateLarge) {
        LargeEntries::const_iterator const it = mLarge.find(key);
        return it == mLarge.end() ? 0 : &it->second;
    }
    SmallEntries::const_iterator const it = mSmall.find(key);
    return it == mSmall.end() ? 0 : &it->second;
}

    void
DirNode::IncrementSmallGeneration()
{
    if (mState != kDirStateLarge) {
        mGeneration++;
    }
}

    DirTable::DirTable()
    : mTables(kNamespaceV2ShardCount)
    {}

    bool
DirTable::Insert(
    fid_t          dirFid,
    const DirNode& dir)
{
    Table& table = mTables[GetLockShard(dirFid)];
    return table.insert(std::make_pair(dirFid, dir)).second;
}

    DirNode*
DirTable::Find(
    fid_t dirFid)
{
    Table& table = mTables[GetLockShard(dirFid)];
    Table::iterator const it = table.find(dirFid);
    return it == table.end() ? 0 : &it->second;
}

    const DirNode*
DirTable::Find(
    fid_t dirFid) const
{
    const Table& table = mTables[GetLockShard(dirFid)];
    Table::const_iterator const it = table.find(dirFid);
    return it == table.end() ? 0 : &it->second;
}

    void
DirTable::CommitThrough(
    TxnId committedTxn)
{
    for (Tables::iterator tableIt = mTables.begin();
            tableIt != mTables.end();
            ++tableIt) {
        for (Table::iterator it = tableIt->begin();
                it != tableIt->end();
                ++it) {
            it->second.CommitThrough(committedTxn);
        }
    }
}

    size_t
DirTable::Size() const
{
    size_t ret = 0;
    for (Tables::const_iterator it = mTables.begin();
            it != mTables.end();
            ++it) {
        ret += it->size();
    }
    return ret;
}

    void
DirTable::GetCommittedEntries(
    TxnId committedTxn,
    std::vector<CheckpointDirEntry>& entries) const
{
    entries.clear();
    std::vector<fid_t> dirFids;
    dirFids.reserve(Size());
    for (Tables::const_iterator tableIt = mTables.begin();
            tableIt != mTables.end();
            ++tableIt) {
        for (Table::const_iterator it = tableIt->begin();
                it != tableIt->end();
                ++it) {
            dirFids.push_back(it->first);
        }
    }
    std::sort(dirFids.begin(), dirFids.end());
    for (std::vector<fid_t>::const_iterator it = dirFids.begin();
            it != dirFids.end();
            ++it) {
        const DirNode* const dir = Find(*it);
        if (! dir) {
            continue;
        }
        std::vector<ReaddirResult::Entry> dirEntries;
        dir->GetCommittedEntries(committedTxn, dirEntries);
        for (std::vector<ReaddirResult::Entry>::const_iterator entryIt =
                    dirEntries.begin();
                entryIt != dirEntries.end();
                ++entryIt) {
            entries.push_back(CheckpointDirEntry(
                *it, entryIt->key, entryIt->childFid));
        }
    }
}

    void
DirTable::GetDirGenerations(
    std::vector<std::pair<fid_t, uint64_t> >& generations) const
{
    generations.clear();
    generations.reserve(Size());
    for (Tables::const_iterator tableIt = mTables.begin();
            tableIt != mTables.end();
            ++tableIt) {
        for (Table::const_iterator it = tableIt->begin();
                it != tableIt->end();
                ++it) {
            generations.push_back(std::make_pair(
                it->first, it->second.GetGeneration()));
        }
    }
    std::sort(generations.begin(), generations.end(),
        [](const std::pair<fid_t, uint64_t>& lhs,
                const std::pair<fid_t, uint64_t>& rhs) {
            return lhs.first < rhs.first;
        });
}

LookupResult::LookupResult()
    : fid(-1),
      type(kInodeTypeFile),
      parentGeneration(0),
      user(kKfsUserRoot),
      group(kKfsGroupRoot),
      mode(0),
      numReplicas(1),
      mtime(0),
      ctime(0),
      atime(0),
      fileCount(0),
      dirCount(0)
    {}

CreateResult::CreateResult()
    : fid(-1),
      txnId(kNoTxn),
      parentGeneration(0)
    {}

EditLogRecord::EditLogRecord()
    : type(kInvalid),
      txnId(kNoTxn),
      parentFid(-1),
      name(),
      fid(-1),
      inodeType(kInodeTypeFile),
      user(kKfsUserRoot),
      group(kKfsGroupRoot),
      mode(0),
      numReplicas(1),
      mtime(0),
      newPath(),
      overwriteFlag(false)
    {}

    int
WriteEditLog(
    std::ostream&       os,
    const EditLogRecord& record)
{
    const int status = ValidateEditLogRecord(record);
    if (status != 0) {
        return status;
    }
    os << "namespacev2_edit 1 " << EditLogRecordTypeName(record.type) << " ";
    if (record.type == EditLogRecord::kCreate) {
        os << record.txnId << " " << record.parentFid << " " <<
            record.fid << " " << InodeTypeToInt(record.inodeType) <<
            " " << record.user << " " << record.group << " " <<
            record.mode << " " << record.numReplicas << " " <<
            record.mtime << " " << EncodeName(record.name);
    } else if (record.type == EditLogRecord::kRemove) {
        os << record.txnId << " " << record.parentFid << " " <<
            InodeTypeToInt(record.inodeType) << " " <<
            EncodeName(record.name);
    } else if (record.type == EditLogRecord::kRename) {
        os << record.txnId << " " << record.parentFid << " " <<
            record.fid << " " << (record.overwriteFlag ? 1 : 0) <<
            " " << EncodeName(record.name) << " " <<
            EncodeString(record.newPath);
    }
    os << char(10);
    return os.good() ? 0 : -EIO;
}

    int
ReadEditLog(
    const std::string& line,
    EditLogRecord&    record)
{
    std::istringstream is(line);
    std::string magic;
    int version = 0;
    std::string op;
    if (! (is >> magic >> version >> op) ||
            magic != "namespacev2_edit" || version != 1) {
        return -EINVAL;
    }
    EditLogRecord tmp;
    if (op == "create") {
        int typeValue = -1;
        std::string encodedName;
        if (! (is >> tmp.txnId >> tmp.parentFid >> tmp.fid >>
                typeValue >> tmp.user >> tmp.group >> tmp.mode >>
                tmp.numReplicas >> tmp.mtime >> encodedName) ||
                ! IntToInodeType(typeValue, tmp.inodeType) ||
                ! DecodeName(encodedName, tmp.name)) {
            return -EINVAL;
        }
        tmp.type = EditLogRecord::kCreate;
    } else if (op == "remove") {
        int typeValue = -1;
        std::string encodedName;
        if (! (is >> tmp.txnId >> tmp.parentFid >> typeValue >>
                encodedName) ||
                ! IntToInodeType(typeValue, tmp.inodeType) ||
                ! DecodeName(encodedName, tmp.name)) {
            return -EINVAL;
        }
        tmp.type = EditLogRecord::kRemove;
    } else if (op == "rename") {
        int overwrite = 0;
        std::string encodedName;
        std::string encodedPath;
        if (! (is >> tmp.txnId >> tmp.parentFid >> tmp.fid >>
                overwrite >> encodedName >> encodedPath) ||
                ! DecodeName(encodedName, tmp.name) ||
                ! DecodePath(encodedPath, tmp.newPath)) {
            return -EINVAL;
        }
        tmp.type = EditLogRecord::kRename;
        tmp.overwriteFlag = overwrite != 0;
    } else {
        return -EINVAL;
    }
    std::string extra;
    if (is >> extra) {
        return -EINVAL;
    }
    const int status = ValidateEditLogRecord(tmp);
    if (status != 0) {
        return status;
    }
    record = tmp;
    return 0;
}

NamespaceStore::NamespaceStore(
    const Config& inConfig,
    fid_t         rootFid)
    : mConfig(inConfig),
      mRootFid(rootFid),
      mNextFid(rootFid + 1),
      mNextTxn(kNoTxn),
      mCommittedTxn(kNoTxn),
      mInodes(),
      mDirs()
{
    InodeRecord root;
    root.fid         = rootFid;
    root.parentFid   = rootFid;
    root.type        = kInodeTypeDir;
    root.createTxn   = kNoTxn;
    root.deleteTxn   = kNoTxn;
    root.pendingFlag = false;
    root.mode        = 0777;
    mInodes.Insert(root);
    mDirs.Insert(rootFid, DirNode(
        mConfig.dirLargeThreshold, mConfig.dirPromoteMaxWallMs));
}

    fid_t
NamespaceStore::GetRootFid() const
{
    return mRootFid;
}

    TxnId
NamespaceStore::GetCommittedTxn() const
{
    return GetCommittedTxnSnapshot();
}

    TxnId
NamespaceStore::GetLastTxn() const
{
    return GetLastTxnSnapshot();
}

    size_t
NamespaceStore::GetInodeCount() const
{
    std::vector<QCMutex*> locks;
    AddAllInodeShardMutexes(locks);
    ScopedMutexGroup locker(locks);
    return mInodes.Size();
}

    size_t
NamespaceStore::GetDirCount() const
{
    std::vector<QCMutex*> locks;
    AddAllDirShardMutexes(locks);
    ScopedMutexGroup locker(locks);
    return mDirs.Size();
}

    TxnId
NamespaceStore::GetCommittedTxnSnapshot() const
{
    return mCommittedTxn.load(std::memory_order_acquire);
}

    TxnId
NamespaceStore::GetLastTxnSnapshot() const
{
    ScopedMutex locker(GetTxnMutex());
    return mNextTxn;
}

    void
NamespaceStore::ReserveCreateIds(
    fid_t& childFid,
    TxnId& txnId)
{
    AllocateCreateIds(childFid, txnId);
}

    void
NamespaceStore::ReserveCreateIdsRange(
    size_t count,
    fid_t& firstFid,
    TxnId& firstTxn)
{
    if (count == 0) {
        firstFid = -1;
        firstTxn = 0;
        return;
    }
    ScopedMutex locker(GetTxnMutex());
    firstFid = mNextFid;
    firstTxn = mNextTxn + 1;
    mNextFid = firstFid + (fid_t)count;
    mNextTxn = firstTxn + (TxnId)count - 1;
}

    void
NamespaceStore::AllocateCreateIds(
    fid_t& childFid,
    TxnId& txnId)
{
    ScopedMutex locker(GetTxnMutex());
    childFid = mNextFid;
    txnId = mNextTxn + 1;
    mNextFid = childFid + 1;
    mNextTxn = txnId;
}

    TxnId
NamespaceStore::AllocateTxnId()
{
    ScopedMutex locker(GetTxnMutex());
    const TxnId txnId = mNextTxn + 1;
    mNextTxn = txnId;
    return txnId;
}

    void
NamespaceStore::AdvanceSeeds(
    fid_t fid,
    TxnId txnId)
{
    ScopedMutex locker(GetTxnMutex());
    if (fid >= 0) {
        mNextFid = std::max(mNextFid, fid + 1);
    }
    mNextTxn = std::max(mNextTxn, txnId);
}

    int
NamespaceStore::Create(
    fid_t              parentFid,
    const std::string& name,
    InodeType          type,
    CreateResult*      resultPtr,
    kfsUid_t           user,
    kfsGid_t           group,
    kfsMode_t          mode,
    int16_t            numReplicas,
    int64_t            mtime)
{
    return ApplyCreatePending(parentFid, name, type, resultPtr,
        user, group, mode, numReplicas, mtime);
}

    int
NamespaceStore::ApplyCreatePending(
    fid_t              parentFid,
    const std::string& name,
    InodeType          type,
    CreateResult*      resultPtr,
    kfsUid_t           user,
    kfsGid_t           group,
    kfsMode_t          mode,
    int16_t            numReplicas,
    int64_t            mtime)
{
    int status = ValidateCreateRequest(name, type);
    if (status != 0) {
        return status;
    }
    const NameKey key(name);
    {
        ScopedSmallMutexGroup parentLocker(
            &GetDirShardMutex(parentFid),
            &GetInodeShardMutex(parentFid));
        status = CheckCreateParentName(
            parentFid, key, GetCommittedTxnSnapshot());
        if (status != 0) {
            return status;
        }
    }
    fid_t childFid = -1;
    TxnId txnId = kNoTxn;
    ReserveCreateIds(childFid, txnId);
    status = ApplyCreate(parentFid, name, type, childFid, txnId,
        user, group, mode, numReplicas, mtime, false, true);
    if (status == 0 && resultPtr) {
        resultPtr->fid = childFid;
        resultPtr->txnId = txnId;
        const DirNode* const dir = mDirs.Find(parentFid);
        resultPtr->parentGeneration = dir ? dir->GetGeneration() : 0;
    }
    return status;
}


    int
NamespaceStore::CreateSelf(
    fid_t              parentFid,
    const std::string& name,
    InodeType          type,
    fid_t              childFid,
    TxnId              txnId,
    CreateResult*      resultPtr,
    kfsUid_t           user,
    kfsGid_t           group,
    kfsMode_t          mode,
    int16_t            numReplicas,
    int64_t            mtime,
    TxnId              committedTxn)
{
    int status = ValidateCreateRequest(
        name, type, childFid, txnId, committedTxn);
    if (status != 0) {
        return status;
    }
    return CreateSelf(parentFid, NameKey(name), type, childFid, txnId,
        resultPtr, user, group, mode, numReplicas, mtime, committedTxn);
}

    int
NamespaceStore::CreateSelf(
    fid_t              parentFid,
    const NameKey&     key,
    InodeType          type,
    fid_t              childFid,
    TxnId              txnId,
    CreateResult*      resultPtr,
    kfsUid_t           user,
    kfsGid_t           group,
    kfsMode_t          mode,
    int16_t            numReplicas,
    int64_t            mtime,
    TxnId              committedTxn)
{
    if (childFid < 0 || txnId == kNoTxn || txnId <= committedTxn ||
            ! IsSupportedInodeType(type)) {
        return -EINVAL;
    }
    if (mInodes.Find(childFid)) {
        return -EEXIST;
    }
    DirNode* dir = 0;
    int status = ResolveCreateParentDir(parentFid, committedTxn, dir);
    if (status != 0) {
        return status;
    }
    status = dir->InsertPending(key, childFid, txnId, committedTxn);
    if (status != 0) {
        return status;
    }
    if (! mInodes.Insert(InodeRecord(childFid, parentFid, type, txnId,
            user, group, mode, numReplicas, mtime))) {
        return -EEXIST;
    }
    if (type == kInodeTypeDir &&
            ! mDirs.Insert(childFid, DirNode(
                mConfig.dirLargeThreshold, mConfig.dirPromoteMaxWallMs))) {
        return -EEXIST;
    }
    if (resultPtr) {
        resultPtr->fid = childFid;
        resultPtr->txnId = txnId;
        resultPtr->parentGeneration = dir->GetGeneration();
    }
    return 0;
}

    int
NamespaceStore::CreateSelfTrusted(
    fid_t              parentFid,
    const NameKey&     key,
    InodeType          type,
    fid_t              childFid,
    TxnId              txnId,
    CreateResult*      resultPtr,
    kfsUid_t           user,
    kfsGid_t           group,
    kfsMode_t          mode,
    int16_t            numReplicas,
    int64_t            mtime,
    TxnId              committedTxn)
{
    if (childFid < 0 || txnId == kNoTxn || txnId <= committedTxn ||
            ! IsSupportedInodeType(type)) {
        return -EINVAL;
    }
    if (mInodes.Find(childFid)) {
        return -EEXIST;
    }
    DirNode* const dir = mDirs.Find(parentFid);
    if (! dir) {
        return -ENOENT;
    }
    int status = dir->InsertPending(key, childFid, txnId, committedTxn);
    if (status != 0) {
        return status;
    }
    if (! mInodes.Insert(InodeRecord(childFid, parentFid, type, txnId,
            user, group, mode, numReplicas, mtime))) {
        return -EEXIST;
    }
    if (type == kInodeTypeDir &&
            ! mDirs.Insert(childFid, DirNode(
                mConfig.dirLargeThreshold, mConfig.dirPromoteMaxWallMs))) {
        return -EEXIST;
    }
    if (resultPtr) {
        resultPtr->fid = childFid;
        resultPtr->txnId = txnId;
        resultPtr->parentGeneration = dir->GetGeneration();
    }
    return 0;
}

    int
NamespaceStore::Lookup(
    fid_t              parentFid,
    const std::string& name,
    LookupResult&      result) const
{
    result = LookupResult();
    if (parentFid == mRootFid && name == "/") {
        return GetAttr(mRootFid, result);
    }

    for (int retry = 0; retry < 4; ++retry) {
        fid_t childFid = -1;
        {
            ScopedSmallMutexGroup parentLocker(
                &GetDirShardMutex(parentFid),
                &GetInodeShardMutex(parentFid));
            const TxnId committedTxn = GetCommittedTxnSnapshot();
            if (! FindCommittedDir(parentFid, committedTxn)) {
                return -ENOENT;
            }
            const DirNode* const dir = mDirs.Find(parentFid);
            if (! dir) {
                return -ENOENT;
            }
            const VersionedDirEntry* const entry =
                dir->LookupCommitted(name, committedTxn);
            if (! entry) {
                return -ENOENT;
            }
            childFid = entry->childFid;
        }

        ScopedSmallMutexGroup locker(
            &GetDirShardMutex(parentFid),
            &GetInodeShardMutex(parentFid),
            &GetInodeShardMutex(childFid),
            &GetDirShardMutex(childFid));
        const TxnId committedTxn = GetCommittedTxnSnapshot();
        if (! FindCommittedDir(parentFid, committedTxn)) {
            return -ENOENT;
        }
        const DirNode* const dir = mDirs.Find(parentFid);
        if (! dir) {
            return -ENOENT;
        }
        const VersionedDirEntry* const entry =
            dir->LookupCommitted(name, committedTxn);
        if (! entry) {
            return -ENOENT;
        }
        if (entry->childFid != childFid) {
            continue;
        }
        const InodeRecord* const inode =
            mInodes.FindCommitted(childFid, committedTxn);
        if (! inode) {
            return -ENOENT;
        }
        return FillLookupResult(*inode, dir->GetGeneration(), result);
    }
    return -EAGAIN;
}

    int
NamespaceStore::LookupPath(
    fid_t              rootFid,
    const std::string& path,
    LookupResult&      result) const
{
    result = LookupResult();
    if (path.empty()) {
        return -EINVAL;
    }
    fid_t curFid = (! path.empty() && path[0] == '/') ? mRootFid : rootFid;
    int status = GetAttr(curFid, result);
    if (status != 0) {
        return status;
    }
    size_t pos = 0;
    while (pos < path.size()) {
        while (pos < path.size() && path[pos] == '/') {
            ++pos;
        }
        if (pos >= path.size()) {
            break;
        }
        const size_t start = pos;
        while (pos < path.size() && path[pos] != '/') {
            ++pos;
        }
        const std::string name = path.substr(start, pos - start);
        if (name == ".") {
            continue;
        }
        if (result.type != kInodeTypeDir) {
            return -ENOTDIR;
        }
        status = Lookup(curFid, name, result);
        if (status != 0) {
            return status;
        }
        curFid = result.fid;
    }
    return 0;
}

    int
NamespaceStore::GetAttr(
    fid_t         fid,
    LookupResult& result) const
{
    result = LookupResult();
    for (int retry = 0; retry < 4; ++retry) {
        fid_t parentFid = -1;
        {
            std::vector<QCMutex*> inodeLocks;
            AddInodeShardMutex(inodeLocks, fid);
            ScopedMutexGroup inodeLocker(inodeLocks);
            const TxnId committedTxn = GetCommittedTxnSnapshot();
            const InodeRecord* const inode =
                mInodes.FindCommitted(fid, committedTxn);
            if (! inode) {
                return -ENOENT;
            }
            parentFid = inode->parentFid;
        }

        std::vector<QCMutex*> locks;
        AddInodeShardMutex(locks, fid);
        AddDirShardMutex(locks, fid);
        AddDirShardMutex(locks, parentFid);
        ScopedMutexGroup locker(locks);
        const TxnId committedTxn = GetCommittedTxnSnapshot();
        const InodeRecord* const inode =
            mInodes.FindCommitted(fid, committedTxn);
        if (! inode) {
            return -ENOENT;
        }
        if (inode->parentFid != parentFid) {
            continue;
        }
        uint64_t parentGeneration = 0;
        const DirNode* const parentDir = mDirs.Find(parentFid);
        if (parentDir) {
            parentGeneration = parentDir->GetGeneration();
        }
        return FillLookupResult(*inode, parentGeneration, result);
    }
    return -EAGAIN;
}

    int
NamespaceStore::Readdir(
    fid_t                 dirFid,
    const ReaddirCookie*  cookiePtr,
    size_t                maxEntries,
    ReaddirResult&        result) const
{
    std::vector<QCMutex*> locks;
    AddDirShardMutex(locks, dirFid);
    AddInodeShardMutex(locks, dirFid);
    ScopedMutexGroup locker(locks);
    const TxnId committedTxn = GetCommittedTxnSnapshot();
    if (! FindCommittedDir(dirFid, committedTxn)) {
        return -ENOENT;
    }
    const DirNode* const dir = mDirs.Find(dirFid);
    return dir ? dir->ReaddirCommitted(committedTxn, cookiePtr,
        maxEntries, result) : -ENOENT;
}

    int
NamespaceStore::ReaddirFromName(
    fid_t              dirFid,
    const std::string& name,
    size_t             maxEntries,
    ReaddirResult&     result) const
{
    std::vector<QCMutex*> locks;
    AddDirShardMutex(locks, dirFid);
    AddInodeShardMutex(locks, dirFid);
    ScopedMutexGroup locker(locks);
    const TxnId committedTxn = GetCommittedTxnSnapshot();
    if (! FindCommittedDir(dirFid, committedTxn)) {
        return -ENOENT;
    }
    const DirNode* const dir = mDirs.Find(dirFid);
    if (! dir) {
        return -ENOENT;
    }
    ReaddirCookie cookie;
    cookie.generation = dir->GetGeneration();
    cookie.layout = dir->GetState();
    cookie.hasLastKeyFlag = ! name.empty();
    cookie.lastKey = NameKey(name);
    return dir->ReaddirCommitted(committedTxn,
        cookie.hasLastKeyFlag ? &cookie : 0, maxEntries, result);
}

    int
NamespaceStore::Remove(
    fid_t              parentFid,
    const std::string& name,
    TxnId*             txnIdPtr)
{
    return RemoveSelf(parentFid, name, kInodeTypeFile, false, kNoTxn,
        txnIdPtr);
}

    int
NamespaceStore::RemoveFile(
    fid_t              parentFid,
    const std::string& name,
    TxnId*             txnIdPtr)
{
    return RemoveSelf(parentFid, name, kInodeTypeFile, false, kNoTxn,
        txnIdPtr);
}

    int
NamespaceStore::Rmdir(
    fid_t              parentFid,
    const std::string& name,
    TxnId*             txnIdPtr)
{
    return RemoveSelf(parentFid, name, kInodeTypeDir, true, kNoTxn,
        txnIdPtr);
}

    int
NamespaceStore::ResolveRenameTarget(
    fid_t              baseDirFid,
    const std::string& newPath,
    fid_t&             dstParentFid,
    std::string&       dstName) const
{
    dstParentFid = -1;
    dstName.clear();
    if (newPath.empty() || newPath[newPath.size() - 1] == '/') {
        return -EINVAL;
    }
    const std::string::size_type rslash = newPath.rfind('/');
    if (rslash == std::string::npos) {
        dstParentFid = baseDirFid;
        dstName = newPath;
        return 0;
    }
    LookupResult parent;
    const int status = LookupPath(baseDirFid,
        newPath.substr(0, std::max(size_t(1), rslash)), parent);
    if (status != 0) {
        return status;
    }
    if (parent.type != kInodeTypeDir) {
        return -ENOTDIR;
    }
    dstParentFid = parent.fid;
    dstName = newPath.substr(rslash + 1);
    return dstName.empty() ? -EINVAL : 0;
}

    bool
NamespaceStore::IsDescendant(
    fid_t ancestorFid,
    fid_t dirFid,
    TxnId committedTxn) const
{
    fid_t curFid = dirFid;
    for (size_t guard = 0; guard <= mInodes.Size(); ++guard) {
        if (curFid == ancestorFid) {
            return true;
        }
        if (curFid == mRootFid) {
            return false;
        }
        const InodeRecord* const inode =
            mInodes.FindCommitted(curFid, committedTxn);
        if (! inode || inode->parentFid == curFid) {
            return false;
        }
        curFid = inode->parentFid;
    }
    return false;
}

    int
NamespaceStore::Rename(
    fid_t              parentFid,
    const std::string& oldName,
    const std::string& newPath,
    bool               overwriteFlag,
    TxnId*             txnIdPtr,
    fid_t*             srcFidPtr)
{
    return RenameSelf(parentFid, oldName, newPath, overwriteFlag,
        kNoTxn, txnIdPtr, srcFidPtr);
}

    int
NamespaceStore::RenameSelf(
    fid_t              parentFid,
    const std::string& oldName,
    const std::string& newPath,
    bool               overwriteFlag,
    TxnId              txnId,
    TxnId*             txnIdPtr,
    fid_t*             srcFidPtr)
{
    if (txnIdPtr) {
        *txnIdPtr = kNoTxn;
    }
    if (srcFidPtr) {
        *srcFidPtr = -1;
    }
    if (! IsLegalName(oldName)) {
        return -EINVAL;
    }

    fid_t dstParentFid = -1;
    std::string dstName;
    int status = ResolveRenameTarget(parentFid, newPath,
        dstParentFid, dstName);
    if (status != 0) {
        return status;
    }
    if (! IsLegalName(dstName)) {
        return -EINVAL;
    }

    for (int retry = 0; retry < 4; ++retry) {
        fid_t phaseDstFid = -1;
        InodeType phaseDstType = kInodeTypeFile;
        bool phaseDstExistsFlag = false;
        {
            std::vector<QCMutex*> phaseLocks;
            AddAllInodeShardMutexes(phaseLocks);
            AddDirShardMutex(phaseLocks, parentFid);
            AddDirShardMutex(phaseLocks, dstParentFid);
            ScopedMutexGroup phaseLocker(phaseLocks);
            const TxnId committedTxn = GetCommittedTxnSnapshot();
            if (txnId != kNoTxn && txnId <= committedTxn) {
                return -EINVAL;
            }
            if (! FindCommittedDir(parentFid, committedTxn)) {
                return -ENOENT;
            }
            DirNode* const srcDir = mDirs.Find(parentFid);
            if (! srcDir) {
                return -ENOENT;
            }
            const VersionedDirEntry* const srcEntry =
                srcDir->LookupCommitted(oldName, committedTxn);
            if (! srcEntry) {
                return -ENOENT;
            }
            const InodeRecord* const srcInode =
                mInodes.FindCommitted(srcEntry->childFid, committedTxn);
            if (! srcInode) {
                return -ENOENT;
            }
            if (srcFidPtr) {
                *srcFidPtr = srcInode->fid;
            }
            if (parentFid == dstParentFid && oldName == dstName) {
                return 0;
            }
            if (! FindCommittedDir(dstParentFid, committedTxn)) {
                return -ENOENT;
            }
            DirNode* const dstDir = mDirs.Find(dstParentFid);
            if (! dstDir) {
                return -ENOENT;
            }
            const VersionedDirEntry* const dstEntry =
                dstDir->LookupCommitted(dstName, committedTxn);
            if (dstEntry) {
                const InodeRecord* const dstInode =
                    mInodes.FindCommitted(dstEntry->childFid, committedTxn);
                if (! dstInode) {
                    return -ENOENT;
                }
                phaseDstExistsFlag = true;
                phaseDstFid = dstInode->fid;
                phaseDstType = dstInode->type;
            }
        }

        std::vector<QCMutex*> locks;
        AddAllInodeShardMutexes(locks);
        AddDirShardMutex(locks, parentFid);
        AddDirShardMutex(locks, dstParentFid);
        if (phaseDstExistsFlag && phaseDstType == kInodeTypeDir) {
            AddDirShardMutex(locks, phaseDstFid);
        }
        ScopedMutexGroup locker(locks);
        const TxnId committedTxn = GetCommittedTxnSnapshot();
        if (txnId != kNoTxn && txnId <= committedTxn) {
            return -EINVAL;
        }
        if (! FindCommittedDir(parentFid, committedTxn)) {
            return -ENOENT;
        }
        DirNode* const srcDir = mDirs.Find(parentFid);
        if (! srcDir) {
            return -ENOENT;
        }
        const VersionedDirEntry* const srcEntry =
            srcDir->LookupCommitted(oldName, committedTxn);
        if (! srcEntry) {
            return -ENOENT;
        }
        InodeRecord* const srcInode = mInodes.Find(srcEntry->childFid);
        if (! srcInode || ! srcInode->IsVisible(committedTxn)) {
            return -ENOENT;
        }
        if (srcFidPtr) {
            *srcFidPtr = srcInode->fid;
        }
        if (parentFid == dstParentFid && oldName == dstName) {
            return 0;
        }
        if (! FindCommittedDir(dstParentFid, committedTxn)) {
            return -ENOENT;
        }
        if (srcInode->type == kInodeTypeDir &&
                IsDescendant(srcInode->fid, dstParentFid, committedTxn)) {
            return -EINVAL;
        }
        DirNode* const dstDir = mDirs.Find(dstParentFid);
        if (! dstDir) {
            return -ENOENT;
        }
        const VersionedDirEntry* const dstEntry =
            dstDir->LookupCommitted(dstName, committedTxn);
        InodeRecord* dstInode = 0;
        if (dstEntry) {
            dstInode = mInodes.Find(dstEntry->childFid);
            if (! dstInode || ! dstInode->IsVisible(committedTxn)) {
                return -ENOENT;
            }
            if (! phaseDstExistsFlag || phaseDstFid != dstInode->fid ||
                    phaseDstType != dstInode->type) {
                continue;
            }
            if (! overwriteFlag) {
                return -EEXIST;
            }
            if (srcInode->type != dstInode->type) {
                return srcInode->type == kInodeTypeDir ? -ENOTDIR : -EISDIR;
            }
            if (dstInode->type == kInodeTypeDir) {
                const DirNode* const childDir = mDirs.Find(dstInode->fid);
                if (! childDir) {
                    return -ENOENT;
                }
                if (childDir->GetChildCount() != 0) {
                    return -ENOTEMPTY;
                }
            }
        } else if (phaseDstExistsFlag) {
            continue;
        }

        const TxnId opTxnId = txnId == kNoTxn ? AllocateTxnId() : txnId;
        if (dstEntry) {
            fid_t deletedFid = -1;
            status = dstDir->DeletePending(dstName, opTxnId, &deletedFid);
            if (status != 0) {
                return status;
            }
            if (! dstInode || deletedFid != dstInode->fid ||
                    ! mInodes.MarkDeleted(deletedFid, opTxnId)) {
                return -EIO;
            }
        }
        fid_t movedFid = -1;
        status = srcDir->DeletePending(oldName, opTxnId, &movedFid);
        if (status != 0) {
            return status;
        }
        if (movedFid != srcInode->fid) {
            return -EIO;
        }
        status = dstDir->InsertPending(dstName, srcInode->fid, opTxnId,
            committedTxn, dstEntry != 0);
        if (status != 0) {
            return status;
        }
        if (parentFid != dstParentFid &&
                ! mInodes.Move(srcInode->fid, dstParentFid)) {
            return -EIO;
        }
        if (txnId != kNoTxn) {
            AdvanceSeeds(-1, opTxnId);
        }
        if (txnIdPtr) {
            *txnIdPtr = opTxnId;
        }
        return 0;
    }
    return -EAGAIN;
}

    int
NamespaceStore::RemoveSelf(
    fid_t              parentFid,
    const std::string& name,
    InodeType          type,
    bool               requireEmptyFlag,
    TxnId              txnId,
    TxnId*             txnIdPtr)
{
    if (txnIdPtr) {
        *txnIdPtr = kNoTxn;
    }
    if (! IsLegalName(name)) {
        return -EINVAL;
    }

    fid_t childFid = -1;
    {
        std::vector<QCMutex*> parentLocks;
        AddDirShardMutex(parentLocks, parentFid);
        AddInodeShardMutex(parentLocks, parentFid);
        ScopedMutexGroup parentLocker(parentLocks);
        const TxnId committedTxn = GetCommittedTxnSnapshot();
        if (txnId != kNoTxn && txnId <= committedTxn) {
            return -EINVAL;
        }
        if (! FindCommittedDir(parentFid, committedTxn)) {
            return -ENOENT;
        }
        DirNode* const dir = mDirs.Find(parentFid);
        if (! dir) {
            return -ENOENT;
        }
        const VersionedDirEntry* const committedEntry =
            dir->LookupCommitted(name, committedTxn);
        if (! committedEntry) {
            return -ENOENT;
        }
        childFid = committedEntry->childFid;
    }

    std::vector<QCMutex*> locks;
    AddDirShardMutex(locks, parentFid);
    AddInodeShardMutex(locks, parentFid);
    AddInodeShardMutex(locks, childFid);
    if (requireEmptyFlag) {
        AddDirShardMutex(locks, childFid);
    }
    ScopedMutexGroup locker(locks);
    const TxnId committedTxn = GetCommittedTxnSnapshot();
    if (txnId != kNoTxn && txnId <= committedTxn) {
        return -EINVAL;
    }
    if (! FindCommittedDir(parentFid, committedTxn)) {
        return -ENOENT;
    }
    DirNode* const dir = mDirs.Find(parentFid);
    if (! dir) {
        return -ENOENT;
    }
    const VersionedDirEntry* const committedEntry =
        dir->LookupCommitted(name, committedTxn);
    if (! committedEntry || committedEntry->childFid != childFid) {
        return -ENOENT;
    }
    const InodeRecord* const inode =
        mInodes.FindCommitted(childFid, committedTxn);
    if (! inode) {
        return -ENOENT;
    }
    if (inode->type != type) {
        return type == kInodeTypeDir ? -ENOTDIR : -EISDIR;
    }
    if (requireEmptyFlag) {
        const DirNode* const childDir = mDirs.Find(inode->fid);
        if (! childDir) {
            return -ENOENT;
        }
        if (childDir->GetChildCount() != 0) {
            return -ENOTEMPTY;
        }
    }
    const TxnId opTxnId = txnId == kNoTxn ? AllocateTxnId() : txnId;
    fid_t deletedFid = -1;
    const int status = dir->DeletePending(name, opTxnId, &deletedFid);
    if (status != 0) {
        return status;
    }
    if (deletedFid != childFid || ! mInodes.MarkDeleted(childFid, opTxnId)) {
        return -EIO;
    }
    AdvanceSeeds(-1, opTxnId);
    if (txnIdPtr) {
        *txnIdPtr = opTxnId;
    }
    return 0;
}

    int
NamespaceStore::ApplyCreate(
    fid_t              parentFid,
    const std::string& name,
    InodeType          type,
    fid_t              childFid,
    TxnId              txnId,
    kfsUid_t           user,
    kfsGid_t           group,
    kfsMode_t          mode,
    int16_t            numReplicas,
    int64_t            mtime,
    bool               commitFlag,
    bool               advanceSeedsFlag)
{
    if (parentFid < 0 || childFid < 0 || txnId == kNoTxn) {
        return -EINVAL;
    }
    int status = 0;
    {
        ScopedSmallMutexGroup locker(
            &GetDirShardMutex(parentFid),
            &GetInodeShardMutex(parentFid),
            &GetInodeShardMutex(childFid),
            type == kInodeTypeDir ? &GetDirShardMutex(childFid) : 0);
        const TxnId committedTxn = commitFlag ?
            GetCommittedTxnSnapshot() : txnId - 1;
        status = CreateSelf(parentFid, NameKey(name), type, childFid, txnId, 0,
            user, group, mode, numReplicas, mtime, committedTxn);
    }
    if (advanceSeedsFlag) {
        AdvanceSeeds(childFid, txnId);
    }
    if (commitFlag) {
        CommitThrough(txnId);
    }
    return status;
}

    int
NamespaceStore::ApplyCreateTrusted(
    fid_t              parentFid,
    const std::string& name,
    InodeType          type,
    fid_t              childFid,
    TxnId              txnId,
    kfsUid_t           user,
    kfsGid_t           group,
    kfsMode_t          mode,
    int16_t            numReplicas,
    int64_t            mtime,
    bool               commitFlag,
    bool               advanceSeedsFlag)
{
    if (parentFid < 0 || childFid < 0 || txnId == kNoTxn) {
        return -EINVAL;
    }
    int status = 0;
    {
        ScopedSmallMutexGroup locker(
            &GetDirShardMutex(parentFid),
            &GetInodeShardMutex(parentFid),
            &GetInodeShardMutex(childFid),
            type == kInodeTypeDir ? &GetDirShardMutex(childFid) : 0);
        const TxnId committedTxn = commitFlag ?
            GetCommittedTxnSnapshot() : txnId - 1;
        status = CreateSelfTrusted(parentFid, NameKey(name), type,
            childFid, txnId, 0, user, group, mode, numReplicas, mtime,
            committedTxn);
    }
    if (advanceSeedsFlag) {
        AdvanceSeeds(childFid, txnId);
    }
    if (commitFlag) {
        CommitThrough(txnId);
    }
    return status;
}


    int
NamespaceStore::ApplyEditLog(
    const EditLogRecord& record,
    bool                 commitFlag)
{
    const int validStatus = ValidateEditLogRecord(record);
    if (validStatus != 0) {
        return validStatus;
    }
    if (record.txnId <= GetCommittedTxnSnapshot()) {
        return -EINVAL;
    }
    int status = 0;
    if (record.type == EditLogRecord::kCreate) {
        return ApplyCreate(record.parentFid, record.name, record.inodeType,
            record.fid, record.txnId, record.user, record.group,
            record.mode, record.numReplicas, record.mtime, commitFlag, true);
    }
    if (record.type == EditLogRecord::kRemove) {
        TxnId txnId = kNoTxn;
        status = RemoveSelf(record.parentFid, record.name, record.inodeType,
            record.inodeType == kInodeTypeDir, record.txnId, &txnId);
        if (status == 0) {
            if (txnId != record.txnId) {
                return -EINVAL;
            }
            CommitThrough(record.txnId);
        }
        return status;
    }
    if (record.type == EditLogRecord::kRename) {
        LookupResult lookup;
        status = Lookup(record.parentFid, record.name, lookup);
        if (status != 0) {
            return status;
        }
        if (lookup.fid != record.fid) {
            return -EINVAL;
        }
        TxnId txnId = kNoTxn;
        fid_t srcFid = -1;
        status = RenameSelf(record.parentFid, record.name, record.newPath,
            record.overwriteFlag, record.txnId, &txnId, &srcFid);
        if (status == 0) {
            if (txnId != record.txnId || srcFid != record.fid) {
                return -EINVAL;
            }
            CommitThrough(record.txnId);
        }
        return status;
    }
    return -EINVAL;
}

    int
NamespaceStore::ApplyEditLog(
    std::istream& is)
{
    std::string line;
    while (std::getline(is, line)) {
        if (line.empty()) {
            continue;
        }
        EditLogRecord record;
        int status = ReadEditLog(line, record);
        if (status != 0) {
            return status;
        }
        status = ApplyEditLog(record);
        if (status != 0) {
            return status;
        }
    }
    return is.bad() ? -EIO : 0;
}

    int
NamespaceStore::SaveCheckpoint(
    std::ostream& os) const
{
    if (! os.good()) {
        return -EIO;
    }
    std::vector<QCMutex*> locks;
    AddAllDirShardMutexes(locks);
    AddAllInodeShardMutexes(locks);
    ScopedMutexGroup locker(locks);
    fid_t nextFid = -1;
    TxnId nextTxn = kNoTxn;
    TxnId committedTxn = kNoTxn;
    {
        ScopedMutex txnLocker(GetTxnMutex());
        nextFid = mNextFid;
        nextTxn = mNextTxn;
        committedTxn = mCommittedTxn.load(std::memory_order_relaxed);
    }
    os << "namespacev2_checkpoint 1\n" <<
        "state " << mRootFid << " " << nextFid << " " <<
            nextTxn << " " << committedTxn << " " <<
            mConfig.dirLargeThreshold << "\n";

    std::vector<InodeRecord> inodes;
    mInodes.GetCommitted(committedTxn, inodes);
    for (std::vector<InodeRecord>::const_iterator it = inodes.begin();
            it != inodes.end();
            ++it) {
        const InodeRecord& inode = *it;
        os << "inode " << inode.fid << " " << inode.parentFid << " " <<
            InodeTypeToInt(inode.type) << " " << inode.generation << " " <<
            inode.user << " " << inode.group << " " << inode.mode << " " <<
            inode.numReplicas << " " << inode.mtime << " " <<
            inode.ctime << " " << inode.atime << "\n";
    }

    std::vector<std::pair<fid_t, uint64_t> > dirGenerations;
    mDirs.GetDirGenerations(dirGenerations);
    for (std::vector<std::pair<fid_t, uint64_t> >::const_iterator it =
                dirGenerations.begin();
            it != dirGenerations.end();
            ++it) {
        if (mInodes.FindCommitted(it->first, committedTxn)) {
            os << "dirgen " << it->first << " " << it->second << "\n";
        }
    }

    std::vector<CheckpointDirEntry> entries;
    mDirs.GetCommittedEntries(committedTxn, entries);
    for (std::vector<CheckpointDirEntry>::const_iterator it = entries.begin();
            it != entries.end();
            ++it) {
        if (! mInodes.FindCommitted(it->parentFid, committedTxn) ||
                ! mInodes.FindCommitted(it->childFid, committedTxn)) {
            continue;
        }
        os << "dentry " << it->parentFid << " " << it->childFid << " " <<
            EncodeName(it->key.name) << "\n";
    }
    os << "end\n";
    return os.good() ? 0 : -EIO;
}

    int
NamespaceStore::SaveCheckpointDiskEntry(
    std::ostream& os) const
{
    if (! os.good()) {
        return -EIO;
    }
    std::vector<QCMutex*> locks;
    AddAllDirShardMutexes(locks);
    AddAllInodeShardMutexes(locks);
    ScopedMutexGroup locker(locks);
    fid_t nextFid = -1;
    TxnId nextTxn = kNoTxn;
    TxnId committedTxn = kNoTxn;
    {
        ScopedMutex txnLocker(GetTxnMutex());
        nextFid = mNextFid;
        nextTxn = mNextTxn;
        committedTxn = mCommittedTxn.load(std::memory_order_relaxed);
    }
    os << "nv2/state/" << mRootFid << "/" << nextFid << "/" <<
        nextTxn << "/" << committedTxn << "/" <<
        mConfig.dirLargeThreshold << "\n";

    std::vector<InodeRecord> inodes;
    mInodes.GetCommitted(committedTxn, inodes);
    for (std::vector<InodeRecord>::const_iterator it = inodes.begin();
            it != inodes.end();
            ++it) {
        const InodeRecord& inode = *it;
        os << "nv2/inode/" << inode.fid << "/" << inode.parentFid <<
            "/" << InodeTypeToInt(inode.type) << "/" <<
            inode.generation << "/" << inode.user << "/" <<
            inode.group << "/" << inode.mode << "/" <<
            inode.numReplicas << "/" << inode.mtime << "/" <<
            inode.ctime << "/" << inode.atime << "\n";
    }

    std::vector<std::pair<fid_t, uint64_t> > dirGenerations;
    mDirs.GetDirGenerations(dirGenerations);
    for (std::vector<std::pair<fid_t, uint64_t> >::const_iterator it =
                dirGenerations.begin();
            it != dirGenerations.end();
            ++it) {
        if (mInodes.FindCommitted(it->first, committedTxn)) {
            os << "nv2/dirgen/" << it->first << "/" <<
                it->second << "\n";
        }
    }

    std::vector<CheckpointDirEntry> entries;
    mDirs.GetCommittedEntries(committedTxn, entries);
    for (std::vector<CheckpointDirEntry>::const_iterator it = entries.begin();
            it != entries.end();
            ++it) {
        if (! mInodes.FindCommitted(it->parentFid, committedTxn) ||
                ! mInodes.FindCommitted(it->childFid, committedTxn)) {
            continue;
        }
        os << "nv2/dentry/" << it->parentFid << "/" <<
            it->childFid << "/" << EncodeName(it->key.name) << "\n";
    }
    os << "nv2/end\n";
    return os.good() ? 0 : -EIO;
}

    int
NamespaceStore::LoadCheckpoint(
    std::istream& is)
{
    std::string magic;
    int version = 0;
    if (! (is >> magic >> version) || magic != "namespacev2_checkpoint" ||
            version != 1) {
        return -EINVAL;
    }
    std::string stateTag;
    fid_t rootFid = -1;
    fid_t nextFid = -1;
    TxnId nextTxn = 0;
    TxnId committedTxn = 0;
    int largeThreshold = 0;
    if (! (is >> stateTag >> rootFid >> nextFid >> nextTxn >>
            committedTxn >> largeThreshold) || stateTag != "state" ||
            rootFid < 0 || nextFid <= rootFid || largeThreshold <= 0 ||
            committedTxn > nextTxn) {
        return -EINVAL;
    }

    NamespaceStore tmp(mConfig, rootFid);
    tmp.mConfig.dirLargeThreshold = largeThreshold;
    tmp.mRootFid      = rootFid;
    tmp.mNextFid      = nextFid;
    tmp.mNextTxn      = nextTxn;
    tmp.mCommittedTxn.store(committedTxn, std::memory_order_relaxed);
    tmp.mInodes       = InodeTable();
    tmp.mDirs         = DirTable();

    std::vector<CheckpointDirEntry> dentries;
    std::vector<std::pair<fid_t, uint64_t> > dirGenerations;
    std::string tag;
    bool endFlag = false;
    while (is >> tag) {
        if (tag == "end") {
            endFlag = true;
            break;
        }
        if (tag == "inode") {
            fid_t fid = -1;
            fid_t parentFid = -1;
            int typeValue = -1;
            uint64_t generation = 0;
            kfsUid_t user = kKfsUserRoot;
            kfsGid_t group = kKfsGroupRoot;
            kfsMode_t mode = 0;
            int16_t numReplicas = 1;
            int64_t mtime = 0;
            int64_t ctime = 0;
            int64_t atime = 0;
            InodeType type = kInodeTypeFile;
            if (! (is >> fid >> parentFid >> typeValue >> generation >>
                    user >> group >> mode >> numReplicas >> mtime >>
                    ctime >> atime) || fid < 0 || parentFid < 0 ||
                    ! IntToInodeType(typeValue, type)) {
                return -EINVAL;
            }
            InodeRecord inode(fid, parentFid, type, kNoTxn,
                user, group, mode, numReplicas, mtime);
            inode.generation  = generation;
            inode.pendingFlag = false;
            inode.ctime       = ctime;
            inode.atime       = atime;
            if (! tmp.mInodes.Insert(inode)) {
                return -EINVAL;
            }
            if (type == kInodeTypeDir && ! tmp.mDirs.Insert(
                    fid, DirNode(tmp.mConfig.dirLargeThreshold,
                        tmp.mConfig.dirPromoteMaxWallMs))) {
                return -EINVAL;
            }
        } else if (tag == "dirgen") {
            fid_t dirFid = -1;
            uint64_t generation = 0;
            if (! (is >> dirFid >> generation) || dirFid < 0) {
                return -EINVAL;
            }
            dirGenerations.push_back(std::make_pair(dirFid, generation));
        } else if (tag == "dentry") {
            fid_t parentFid = -1;
            fid_t childFid = -1;
            std::string encodedName;
            std::string name;
            if (! (is >> parentFid >> childFid >> encodedName) ||
                    ! DecodeName(encodedName, name)) {
                return -EINVAL;
            }
            dentries.push_back(CheckpointDirEntry(
                parentFid, NameKey(name), childFid));
        } else {
            return -EINVAL;
        }
    }
    if (! endFlag) {
        return -EINVAL;
    }
    const InodeRecord* const root =
        tmp.mInodes.FindCommitted(rootFid, committedTxn);
    if (! root || root->type != kInodeTypeDir || ! tmp.mDirs.Find(rootFid)) {
        return -EINVAL;
    }
    for (std::vector<CheckpointDirEntry>::const_iterator it = dentries.begin();
            it != dentries.end();
            ++it) {
        const InodeRecord* const parent =
            tmp.mInodes.FindCommitted(it->parentFid, committedTxn);
        const InodeRecord* const child =
            tmp.mInodes.FindCommitted(it->childFid, committedTxn);
        DirNode* const dir = tmp.mDirs.Find(it->parentFid);
        if (! parent || parent->type != kInodeTypeDir || ! child || ! dir) {
            return -EINVAL;
        }
        const int status = dir->InsertCommitted(it->key.name, it->childFid);
        if (status != 0) {
            return status;
        }
    }
    for (std::vector<std::pair<fid_t, uint64_t> >::const_iterator it =
                dirGenerations.begin();
            it != dirGenerations.end();
            ++it) {
        DirNode* const dir = tmp.mDirs.Find(it->first);
        if (! dir) {
            return -EINVAL;
        }
        dir->SetGeneration(it->second);
    }
    {
        std::vector<QCMutex*> locks;
        AddAllDirShardMutexes(locks);
        AddAllInodeShardMutexes(locks);
        ScopedMutexGroup locker(locks);
        ScopedMutex txnLocker(GetTxnMutex());
        mConfig = tmp.mConfig;
        mRootFid = tmp.mRootFid;
        mNextFid = tmp.mNextFid;
        mNextTxn = tmp.mNextTxn;
        mCommittedTxn.store(
            tmp.mCommittedTxn.load(std::memory_order_relaxed),
            std::memory_order_release);
        mPendingCommittedTxns = tmp.mPendingCommittedTxns;
        mInodes = tmp.mInodes;
        mDirs = tmp.mDirs;
    }
    return 0;
}

    void
NamespaceStore::CommitThrough(
    TxnId committedTxn)
{
    ScopedMutex locker(GetTxnMutex());
    TxnId current = mCommittedTxn.load(std::memory_order_relaxed);
    if (committedTxn <= current) {
        return;
    }
    mPendingCommittedTxns.insert(committedTxn);
    for (;;) {
        std::set<TxnId>::iterator const it =
            mPendingCommittedTxns.find(current + 1);
        if (it == mPendingCommittedTxns.end()) {
            break;
        }
        mPendingCommittedTxns.erase(it);
        ++current;
    }
    mCommittedTxn.store(current, std::memory_order_release);
}

    void
NamespaceStore::CommitThroughRange(
    TxnId firstTxn,
    TxnId lastTxn)
{
    if (lastTxn < firstTxn) {
        return;
    }
    ScopedMutex locker(GetTxnMutex());
    TxnId current = mCommittedTxn.load(std::memory_order_relaxed);
    if (lastTxn <= current) {
        return;
    }
    if (firstTxn <= current + 1) {
        current = lastTxn;
    } else {
        for (TxnId txnId = firstTxn; txnId <= lastTxn; ++txnId) {
            mPendingCommittedTxns.insert(txnId);
            if (txnId == lastTxn) {
                break;
            }
        }
    }
    for (;;) {
        std::set<TxnId>::iterator const it =
            mPendingCommittedTxns.find(current + 1);
        if (it == mPendingCommittedTxns.end()) {
            break;
        }
        mPendingCommittedTxns.erase(it);
        ++current;
    }
    mCommittedTxn.store(current, std::memory_order_release);
}

    const InodeRecord*
NamespaceStore::FindCommittedDir(
    fid_t dirFid,
    TxnId committedTxn) const
{
    const InodeRecord* const inode =
        mInodes.FindCommitted(dirFid, committedTxn);
    return inode && inode->type == kInodeTypeDir ? inode : 0;
}

    int
NamespaceStore::ResolveCreateParentDir(
    fid_t   parentFid,
    TxnId   committedTxn,
    DirNode*& dirPtr)
{
    dirPtr = 0;
    if (! FindCommittedDir(parentFid, committedTxn)) {
        return -ENOENT;
    }
    dirPtr = mDirs.Find(parentFid);
    if (! dirPtr) {
        return -ENOENT;
    }
    return 0;
}

    int
NamespaceStore::CheckCreateParentName(
    fid_t              parentFid,
    const NameKey&     key,
    TxnId              committedTxn)
{
    DirNode* dir = 0;
    const int status = ResolveCreateParentDir(parentFid, committedTxn, dir);
    if (status != 0) {
        return status;
    }
    if (dir->HasVisibleOrPendingName(key, committedTxn)) {
        return -EEXIST;
    }
    return 0;
}

    int
NamespaceStore::FillLookupResult(
    const InodeRecord& inode,
    uint64_t           parentGeneration,
    LookupResult&      result) const
{
    result.fid              = inode.fid;
    result.type             = inode.type;
    result.parentGeneration = parentGeneration;
    result.user             = inode.user;
    result.group            = inode.group;
    result.mode             = inode.mode;
    result.numReplicas      = inode.numReplicas;
    result.mtime            = inode.mtime;
    result.ctime            = inode.ctime;
    result.atime            = inode.atime;
    if (inode.type == kInodeTypeDir) {
        const DirNode* const dir = mDirs.Find(inode.fid);
        result.fileCount = dir ? (int64_t)dir->GetChildCount() : 0;
        result.dirCount = 0;
    }
    return 0;
}

ResourceLockKey::ResourceLockKey(
    Class    inResourceClass,
    uint64_t inMajor,
    uint64_t inMinor)
    : resourceClass(inResourceClass),
      major(inMajor),
      minor(inMinor)
    {}

    bool
ResourceLockKey::operator<(
    const ResourceLockKey& other) const
{
    if (resourceClass != other.resourceClass) {
        return resourceClass < other.resourceClass;
    }
    if (major != other.major) {
        return major < other.major;
    }
    return minor < other.minor;
}

} // namespace NamespaceV2
} // namespace KFS
