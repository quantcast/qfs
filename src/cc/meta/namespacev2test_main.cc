//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Unit tests for RFC-0001 NamespaceV2 scaffolding.
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

#include <algorithm>
#include <atomic>
#include <errno.h>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace KFS;
using namespace KFS::NamespaceV2;
using std::cout;
using std::string;
using std::vector;

namespace
{

int gErrorCount = 0;

    void
Check(
    bool        okFlag,
    const char* msgPtr)
{
    if (! okFlag) {
        cout << "FAILED: " << msgPtr << "\n";
        gErrorCount++;
    }
}

    string
MakeName(
    const char* prefixPtr,
    int         first,
    int         second = -1)
{
    std::ostringstream os;
    os << prefixPtr << first;
    if (second >= 0) {
        os << "_" << second;
    }
    return os.str();
}

    void
TestConfig()
{
    Properties props;
    props.setValue("metaServer.namespaceV2.enabled", "1");
    props.setValue("metaServer.namespaceV2.rpcEnabled", "1");
    props.setValue("metaServer.dir.largeThreshold", "8");
    props.setValue("metaServer.dir.promoteMaxWallMs", "77");
    props.setValue("metaServer.namespaceV2.dirShardCount", "16");
    SetParameters(props);
    const Config& cfg = GetConfig();
    Check(cfg.enabledFlag, "namespace v2 enabled flag");
    Check(cfg.rpcEnabledFlag, "namespace v2 rpc enabled flag");
    Check(cfg.dirLargeThreshold == 8, "large threshold");
    Check(cfg.dirPromoteMaxWallMs == 77, "promotion wall limit");
    Check(cfg.dirShardCount == 16, "dir shard count");
}

    void
TestNameKey()
{
    const NameKey a("a");
    const NameKey b("b");
    Check(a == NameKey(a.hash, "a"), "name key equality");
    Check((a < b) || (b < a), "name key strict ordering");
}

    void
TestPendingCommittedVisibility()
{
    DirNode dir(4);
    Check(dir.InsertPending("f", 100, 10) == 0, "insert pending");
    Check(dir.LookupCommitted("f", 9) == 0, "pending create invisible");
    dir.CommitThrough(10);
    const VersionedDirEntry* entry = dir.LookupCommitted("f", 10);
    Check(entry && entry->childFid == 100, "committed create visible");
    fid_t deletedFid = -1;
    Check(dir.DeletePending("f", 20, &deletedFid) == 0, "delete pending");
    Check(deletedFid == 100, "delete returns child fid");
    Check(dir.LookupCommitted("f", 19) != 0, "pending delete still visible");
    Check(dir.InsertPending("f", 101, 21) == -EEXIST,
        "recreate rejected while delete pending");
    dir.CommitThrough(20);
    Check(dir.LookupCommitted("f", 20) == 0, "committed delete invisible");
    Check(dir.InsertPending("f", 101, 30) == 0,
        "recreate after delete commit");
    Check(dir.LookupCommitted("f", 29) == 0, "recreate pending invisible");
    dir.CommitThrough(30);
    entry = dir.LookupCommitted("f", 30);
    Check(entry && entry->childFid == 101, "recreate committed visible");
}

    void
TestSmallCookieInvalidation()
{
    DirNode dir(10);
    Check(dir.InsertPending("b", 2, 1) == 0, "insert b");
    Check(dir.InsertPending("a", 1, 2) == 0, "insert a");
    dir.CommitThrough(2);
    ReaddirResult res;
    Check(dir.ReaddirCommitted(2, 0, 1, res) == 0, "small readdir first page");
    Check(res.entries.size() == 1, "small readdir page size");
    const ReaddirCookie oldCookie = res.nextCookie;
    Check(dir.InsertPending("c", 3, 3) == 0, "insert c invalidates small cookie");
    dir.CommitThrough(3);
    Check(dir.ReaddirCommitted(3, &oldCookie, 1, res) == -EINVAL,
        "old small cookie rejected");
}

    void
TestPromotion()
{
    DirNode dir(2);
    Check(dir.InsertPending("a", 1, 1) == 0, "promotion insert a");
    Check(dir.InsertPending("b", 2, 2) == 0, "promotion insert b");
    Check(! dir.IsLarge(), "at threshold still small");
    const uint64_t genBefore = dir.GetGeneration();
    Check(dir.InsertPending("c", 3, 3) == 0, "promotion insert c");
    Check(dir.IsLarge(), "promoted to large");
    Check(dir.GetGeneration() > genBefore, "promotion increments generation");
    dir.CommitThrough(3);
    Check(dir.LookupCommitted("a", 3) != 0, "large lookup a");
    Check(dir.LookupCommitted("b", 3) != 0, "large lookup b");
    Check(dir.LookupCommitted("c", 3) != 0, "large lookup c");
}

    void
TestLargeCookieStableAcrossInsert()
{
    DirNode dir(1);
    Check(dir.InsertPending("a", 1, 1) == 0, "large insert a");
    Check(dir.InsertPending("b", 2, 2) == 0, "large insert b");
    Check(dir.InsertPending("d", 4, 3) == 0, "large insert d");
    dir.CommitThrough(3);
    Check(dir.IsLarge(), "large state");
    ReaddirResult res;
    Check(dir.ReaddirCommitted(3, 0, 1, res) == 0, "large first page");
    Check(res.entries.size() == 1, "large page size");
    const ReaddirCookie cookie = res.nextCookie;
    const uint64_t gen = dir.GetGeneration();
    Check(dir.InsertPending("c", 3, 4) == 0, "large insert keeps cookie valid");
    dir.CommitThrough(4);
    Check(dir.GetGeneration() == gen, "large create does not bump generation");
    Check(dir.ReaddirCommitted(4, &cookie, 10, res) == 0,
        "old large NameKey cookie remains valid");
    Check(! res.entries.empty(), "large resume returns entries");
    for (size_t i = 0; i < res.entries.size(); i++) {
        Check(cookie.lastKey < res.entries[i].key,
            "large resume returns entries after last key");
    }
}

    void
TestInodeTable()
{
    InodeTable table;
    Check(table.Insert(InodeRecord(10, 1, kInodeTypeFile, 5)),
        "inode insert");
    Check(! table.Insert(InodeRecord(10, 1, kInodeTypeFile, 6)),
        "duplicate inode insert rejected");
    Check(table.FindCommitted(10, 4) == 0, "pending inode invisible");
    Check(table.FindCommitted(10, 5) != 0, "committed inode visible");
    Check(table.MarkDeleted(10, 8), "inode mark deleted");
    Check(table.FindCommitted(10, 7) != 0, "pending inode delete visible");
    Check(table.FindCommitted(10, 8) == 0, "committed inode delete invisible");
}

    void
TestNamespaceStoreBasic()
{
    Config cfg;
    cfg.dirLargeThreshold = 2;
    NamespaceStore store(cfg);
    LookupResult lookup;
    Check(store.GetRootFid() == ROOTFID, "namespace root fid");
    Check(store.GetInodeCount() == 1, "namespace root inode");
    Check(store.GetDirCount() == 1, "namespace root dir");
    Check(store.Lookup(ROOTFID, "f", lookup) == -ENOENT,
        "namespace missing lookup");

    CreateResult create;
    Check(store.Create(ROOTFID, "f", kInodeTypeFile, &create) == 0,
        "namespace create file");
    Check(create.fid > ROOTFID, "namespace create fid assigned");
    Check(store.GetLastTxn() == create.txnId, "namespace txn assigned");
    Check(store.Lookup(ROOTFID, "f", lookup) == -ENOENT,
        "namespace pending create invisible");
    Check(store.Create(ROOTFID, "f", kInodeTypeFile, 0) == -EEXIST,
        "namespace duplicate pending create rejected");
    Check(store.GetLastTxn() == create.txnId,
        "namespace failed create does not consume txn");

    store.CommitThrough(create.txnId);
    Check(store.Lookup(ROOTFID, "f", lookup) == 0 &&
            lookup.fid == create.fid && lookup.type == kInodeTypeFile,
        "namespace committed create visible");
    ReaddirResult readdir;
    Check(store.Readdir(ROOTFID, 0, 10, readdir) == 0 &&
            readdir.entries.size() == 1 &&
            readdir.entries[0].childFid == create.fid,
        "namespace readdir committed create");

    TxnId deleteTxn = 0;
    Check(store.Remove(ROOTFID, "f", &deleteTxn) == 0,
        "namespace remove file");
    Check(deleteTxn > create.txnId, "namespace remove txn assigned");
    Check(store.Lookup(ROOTFID, "f", lookup) == 0,
        "namespace pending remove still visible");
    Check(store.Create(ROOTFID, "f", kInodeTypeFile, 0) == -EEXIST,
        "namespace recreate rejected while remove pending");
    Check(store.GetLastTxn() == deleteTxn,
        "namespace failed recreate does not consume txn");
    store.CommitThrough(deleteTxn);
    Check(store.Lookup(ROOTFID, "f", lookup) == -ENOENT,
        "namespace committed remove invisible");
    Check(store.Create(ROOTFID, "f", kInodeTypeFile, 0) == 0,
        "namespace recreate after committed remove");
}

    void
TestNamespaceStoreDirectory()
{
    Config cfg;
    cfg.dirLargeThreshold = 1;
    NamespaceStore store(cfg);
    CreateResult dir;
    Check(store.Create(ROOTFID, "d", kInodeTypeDir, &dir) == 0,
        "namespace mkdir");
    Check(store.GetDirCount() == 2, "namespace dir table insert pending dir");
    Check(store.Create(dir.fid, "before_commit", kInodeTypeFile, 0) == -ENOENT,
        "namespace pending dir not usable");
    Check(store.GetLastTxn() == dir.txnId,
        "namespace failed child create does not consume txn");
    store.CommitThrough(dir.txnId);

    CreateResult first;
    Check(store.Create(dir.fid, "a", kInodeTypeFile, &first) == 0,
        "namespace child create");
    store.CommitThrough(first.txnId);
    LookupResult lookup;
    Check(store.Lookup(dir.fid, "a", lookup) == 0 && lookup.fid == first.fid,
        "namespace child lookup");

    CreateResult second;
    Check(store.Create(dir.fid, "b", kInodeTypeFile, &second) == 0,
        "namespace child create promotes dir");
    store.CommitThrough(second.txnId);
    ReaddirResult readdir;
    Check(store.Readdir(dir.fid, 0, 10, readdir) == 0 &&
            readdir.entries.size() == 2,
        "namespace child readdir after promotion");
}

    void
TestNamespaceStorePathAndRmdir()
{
    Config cfg;
    NamespaceStore store(cfg);
    CreateResult dir;
    Check(store.Create(ROOTFID, "d", kInodeTypeDir, &dir,
            10, 20, 0755, 0, 100) == 0,
        "namespace path mkdir d");
    store.CommitThrough(dir.txnId);

    CreateResult child;
    Check(store.Create(dir.fid, "c", kInodeTypeDir, &child,
            11, 21, 0750, 0, 101) == 0,
        "namespace path mkdir child");
    store.CommitThrough(child.txnId);

    CreateResult file;
    Check(store.Create(child.fid, "f", kInodeTypeFile, &file,
            12, 22, 0644, 2, 102) == 0,
        "namespace path create file");
    store.CommitThrough(file.txnId);

    LookupResult lookup;
    Check(store.LookupPath(ROOTFID, "/d/c/f", lookup) == 0 &&
            lookup.fid == file.fid && lookup.user == 12 &&
            lookup.group == 22 && lookup.mode == 0644 &&
            lookup.numReplicas == 2,
        "namespace lookup path attrs");
    Check(store.LookupPath(ROOTFID, "/d/c/f/x", lookup) == -ENOTDIR,
        "namespace lookup below file rejected");
    Check(store.RemoveFile(dir.fid, "c", 0) == -EISDIR,
        "namespace remove dir as file rejected");
    Check(store.Rmdir(child.fid, "f", 0) == -ENOTDIR,
        "namespace rmdir file rejected");
    Check(store.Rmdir(dir.fid, "c", 0) == -ENOTEMPTY,
        "namespace rmdir non-empty rejected");

    TxnId txnId = 0;
    Check(store.RemoveFile(child.fid, "f", &txnId) == 0,
        "namespace remove child file");
    store.CommitThrough(txnId);
    Check(store.Rmdir(dir.fid, "c", &txnId) == 0,
        "namespace rmdir empty child");
    store.CommitThrough(txnId);
    Check(store.LookupPath(ROOTFID, "/d/c", lookup) == -ENOENT,
        "namespace rmdir committed invisible");
}


    void
TestNamespaceStoreRename()
{
    Config cfg;
    cfg.dirLargeThreshold = 2;
    NamespaceStore store(cfg);

    CreateResult d;
    Check(store.Create(ROOTFID, "d", kInodeTypeDir, &d) == 0,
        "rename mkdir d");
    store.CommitThrough(d.txnId);
    CreateResult e;
    Check(store.Create(ROOTFID, "e", kInodeTypeDir, &e) == 0,
        "rename mkdir e");
    store.CommitThrough(e.txnId);

    CreateResult file;
    Check(store.Create(d.fid, "f", kInodeTypeFile, &file) == 0,
        "rename create source file");
    store.CommitThrough(file.txnId);

    TxnId txnId = 0;
    fid_t srcFid = -1;
    Check(store.Rename(d.fid, "f", "g", false, &txnId, &srcFid) == 0 &&
            srcFid == file.fid,
        "rename same dir file");
    store.CommitThrough(txnId);
    LookupResult lookup;
    Check(store.Lookup(d.fid, "f", lookup) == -ENOENT,
        "rename source removed");
    Check(store.Lookup(d.fid, "g", lookup) == 0 && lookup.fid == file.fid,
        "rename target visible");

    Check(store.Rename(d.fid, "g", "/e/h", false, &txnId, &srcFid) == 0,
        "rename cross dir absolute target");
    store.CommitThrough(txnId);
    Check(store.Lookup(d.fid, "g", lookup) == -ENOENT,
        "cross dir source removed");
    Check(store.Lookup(e.fid, "h", lookup) == 0 && lookup.fid == file.fid,
        "cross dir target visible");

    CreateResult other;
    Check(store.Create(e.fid, "z", kInodeTypeFile, &other) == 0,
        "rename create overwrite target");
    store.CommitThrough(other.txnId);
    Check(store.Rename(e.fid, "h", "z", false, 0, 0) == -EEXIST,
        "rename overwrite disabled");
    Check(store.Rename(e.fid, "h", "z", true, &txnId, &srcFid) == 0,
        "rename overwrite file");
    store.CommitThrough(txnId);
    Check(store.Lookup(e.fid, "h", lookup) == -ENOENT,
        "overwrite source removed");
    Check(store.Lookup(e.fid, "z", lookup) == 0 && lookup.fid == file.fid,
        "overwrite target replaced");

    CreateResult x;
    Check(store.Create(ROOTFID, "x", kInodeTypeFile, &x) == 0,
        "rename create type mismatch file");
    store.CommitThrough(x.txnId);
    CreateResult y;
    Check(store.Create(ROOTFID, "y", kInodeTypeDir, &y) == 0,
        "rename create type mismatch dir");
    store.CommitThrough(y.txnId);
    Check(store.Rename(ROOTFID, "x", "y", true, 0, 0) == -EISDIR,
        "rename file over dir rejected");

    CreateResult a;
    Check(store.Create(ROOTFID, "a", kInodeTypeDir, &a) == 0,
        "rename create ancestor dir");
    store.CommitThrough(a.txnId);
    CreateResult b;
    Check(store.Create(a.fid, "b", kInodeTypeDir, &b) == 0,
        "rename create descendant dir");
    store.CommitThrough(b.txnId);
    Check(store.Rename(ROOTFID, "a", "/a/b/c", false, 0, 0) == -EINVAL,
        "rename dir into descendant rejected");

    CreateResult p;
    Check(store.Create(ROOTFID, "p", kInodeTypeDir, &p) == 0,
        "rename create source dir");
    store.CommitThrough(p.txnId);
    CreateResult q;
    Check(store.Create(ROOTFID, "q", kInodeTypeDir, &q) == 0,
        "rename create non-empty target dir");
    store.CommitThrough(q.txnId);
    CreateResult qChild;
    Check(store.Create(q.fid, "child", kInodeTypeFile, &qChild) == 0,
        "rename create target child");
    store.CommitThrough(qChild.txnId);
    Check(store.Rename(ROOTFID, "p", "q", true, 0, 0) == -ENOTEMPTY,
        "rename over non-empty dir rejected");
}


    void
TestNamespaceStoreCheckpoint()
{
    Config cfg;
    cfg.dirLargeThreshold = 1;
    NamespaceStore store(cfg);

    CreateResult d;
    Check(store.Create(ROOTFID, "d", kInodeTypeDir, &d,
            10, 20, 0755, 0, 100) == 0,
        "checkpoint mkdir d");
    store.CommitThrough(d.txnId);
    CreateResult e;
    Check(store.Create(ROOTFID, "e", kInodeTypeDir, &e,
            11, 21, 0750, 0, 101) == 0,
        "checkpoint mkdir e");
    store.CommitThrough(e.txnId);

    CreateResult removed;
    Check(store.Create(d.fid, "removed", kInodeTypeFile, &removed) == 0,
        "checkpoint create removed file");
    store.CommitThrough(removed.txnId);
    TxnId txnId = 0;
    Check(store.RemoveFile(d.fid, "removed", &txnId) == 0,
        "checkpoint remove file");
    store.CommitThrough(txnId);

    CreateResult file;
    Check(store.Create(d.fid, "f", kInodeTypeFile, &file,
            12, 22, 0644, 3, 102) == 0,
        "checkpoint create file");
    store.CommitThrough(file.txnId);
    Check(store.Rename(d.fid, "f", "/e/g", false, &txnId, 0) == 0,
        "checkpoint rename file");
    store.CommitThrough(txnId);

    LookupResult originalMoved;
    Check(store.LookupPath(ROOTFID, "/e/g", originalMoved) == 0,
        "checkpoint original moved lookup");

    std::stringstream image;
    Check(store.SaveCheckpoint(image) == 0,
        "checkpoint save");

    NamespaceStore restored(cfg);
    Check(restored.LoadCheckpoint(image) == 0,
        "checkpoint load");
    Check(restored.GetCommittedTxn() == store.GetCommittedTxn(),
        "checkpoint committed txn restored");
    Check(restored.GetLastTxn() == store.GetLastTxn(),
        "checkpoint last txn restored");

    LookupResult lookup;
    Check(restored.LookupPath(ROOTFID, "/e/g", lookup) == 0 &&
            lookup.fid == file.fid && lookup.user == 12 &&
            lookup.group == 22 && lookup.mode == 0644 &&
            lookup.numReplicas == 3 &&
            lookup.parentGeneration == originalMoved.parentGeneration,
        "checkpoint restored moved file attrs");
    Check(restored.LookupPath(ROOTFID, "/d/f", lookup) == -ENOENT,
        "checkpoint old rename source absent");
    Check(restored.LookupPath(ROOTFID, "/d/removed", lookup) == -ENOENT,
        "checkpoint removed file absent");

    ReaddirResult readdir;
    Check(restored.Readdir(e.fid, 0, 10, readdir) == 0 &&
            readdir.entries.size() == 1 &&
            readdir.entries[0].childFid == file.fid,
        "checkpoint restored dir entries");

    const TxnId lastTxn = restored.GetLastTxn();
    CreateResult after;
    Check(restored.Create(ROOTFID, "after", kInodeTypeFile, &after) == 0,
        "checkpoint create after restore");
    Check(after.txnId == lastTxn + 1 && after.fid > file.fid,
        "checkpoint seeds continue after restore");
}

    void
TestNamespaceStoreEditLog()
{
    Config cfg;
    cfg.dirLargeThreshold = 1;
    NamespaceStore store(cfg);

    CreateResult d;
    Check(store.Create(ROOTFID, "d", kInodeTypeDir, &d,
            10, 20, 0755, 0, 100) == 0,
        "edit log mkdir d");
    store.CommitThrough(d.txnId);
    CreateResult e;
    Check(store.Create(ROOTFID, "e", kInodeTypeDir, &e,
            11, 21, 0750, 0, 101) == 0,
        "edit log mkdir e");
    store.CommitThrough(e.txnId);

    std::stringstream checkpoint;
    Check(store.SaveCheckpoint(checkpoint) == 0,
        "edit log checkpoint save");

    std::stringstream logs;
    CreateResult tmp;
    Check(store.Create(d.fid, "tmp", kInodeTypeFile, &tmp,
            30, 40, 0600, 1, 200) == 0,
        "edit log create tmp");
    EditLogRecord tmpCreate;
    tmpCreate.type = EditLogRecord::kCreate;
    tmpCreate.txnId = tmp.txnId;
    tmpCreate.parentFid = d.fid;
    tmpCreate.name = "tmp";
    tmpCreate.fid = tmp.fid;
    tmpCreate.inodeType = kInodeTypeFile;
    tmpCreate.user = 30;
    tmpCreate.group = 40;
    tmpCreate.mode = 0600;
    tmpCreate.numReplicas = 1;
    tmpCreate.mtime = 200;
    Check(WriteEditLog(logs, tmpCreate) == 0,
        "edit log write tmp create");
    store.CommitThrough(tmp.txnId);

    TxnId removeTxn = 0;
    Check(store.RemoveFile(d.fid, "tmp", &removeTxn) == 0,
        "edit log remove tmp");
    EditLogRecord tmpRemove;
    tmpRemove.type = EditLogRecord::kRemove;
    tmpRemove.txnId = removeTxn;
    tmpRemove.parentFid = d.fid;
    tmpRemove.name = "tmp";
    tmpRemove.inodeType = kInodeTypeFile;
    Check(WriteEditLog(logs, tmpRemove) == 0,
        "edit log write tmp remove");
    store.CommitThrough(removeTxn);

    CreateResult file;
    Check(store.Create(d.fid, "f", kInodeTypeFile, &file,
            31, 41, 0644, 3, 201) == 0,
        "edit log create file");
    EditLogRecord fileCreate;
    fileCreate.type = EditLogRecord::kCreate;
    fileCreate.txnId = file.txnId;
    fileCreate.parentFid = d.fid;
    fileCreate.name = "f";
    fileCreate.fid = file.fid;
    fileCreate.inodeType = kInodeTypeFile;
    fileCreate.user = 31;
    fileCreate.group = 41;
    fileCreate.mode = 0644;
    fileCreate.numReplicas = 3;
    fileCreate.mtime = 201;
    Check(WriteEditLog(logs, fileCreate) == 0,
        "edit log write file create");
    store.CommitThrough(file.txnId);

    TxnId renameTxn = 0;
    fid_t srcFid = -1;
    Check(store.Rename(d.fid, "f", "/e/g", false,
            &renameTxn, &srcFid) == 0 && srcFid == file.fid,
        "edit log rename file");
    EditLogRecord rename;
    rename.type = EditLogRecord::kRename;
    rename.txnId = renameTxn;
    rename.parentFid = d.fid;
    rename.name = "f";
    rename.fid = srcFid;
    rename.newPath = "/e/g";
    rename.overwriteFlag = false;
    Check(WriteEditLog(logs, rename) == 0,
        "edit log write rename");
    store.CommitThrough(renameTxn);

    NamespaceStore restored(cfg);
    Check(restored.LoadCheckpoint(checkpoint) == 0,
        "edit log checkpoint load");
    Check(restored.ApplyEditLog(logs) == 0,
        "edit log replay stream");

    LookupResult lookup;
    Check(restored.LookupPath(ROOTFID, "/e/g", lookup) == 0 &&
            lookup.fid == file.fid && lookup.user == 31 &&
            lookup.group == 41 && lookup.mode == 0644 &&
            lookup.numReplicas == 3,
        "edit log replay moved file attrs");
    Check(restored.LookupPath(ROOTFID, "/d/f", lookup) == -ENOENT,
        "edit log replay old rename source absent");
    Check(restored.LookupPath(ROOTFID, "/d/tmp", lookup) == -ENOENT,
        "edit log replay removed file absent");
    Check(restored.GetCommittedTxn() == store.GetCommittedTxn(),
        "edit log committed txn restored");
    Check(restored.GetLastTxn() == store.GetLastTxn(),
        "edit log last txn restored");

    CreateResult after;
    Check(restored.Create(ROOTFID, "after_log", kInodeTypeFile, &after) == 0,
        "edit log create after replay");
    Check(after.txnId == store.GetLastTxn() + 1 && after.fid > file.fid,
        "edit log seeds continue after replay");

    EditLogRecord parsed;
    Check(ReadEditLog("namespacev2_edit 1 create 0 3 4 0 1 1 0 1 1 61",
            parsed) == -EINVAL,
        "edit log rejects zero txn");
}

    void
TestNamespaceStoreEditLogFailedCreateNoop()
{
    Config cfg;
    NamespaceStore store(cfg);

    fid_t fid = -1;
    TxnId txnId = 0;
    store.ReserveCreateIds(fid, txnId);
    EditLogRecord first;
    first.type = EditLogRecord::kCreate;
    first.txnId = txnId;
    first.parentFid = ROOTFID;
    first.name = "dup";
    first.fid = fid;
    first.inodeType = kInodeTypeFile;
    first.mode = 0644;
    Check(store.ApplyEditLog(first) == 0,
        "edit log first create succeeds");

    fid_t failedFid = -1;
    TxnId failedTxnId = 0;
    store.ReserveCreateIds(failedFid, failedTxnId);
    EditLogRecord duplicate(first);
    duplicate.txnId = failedTxnId;
    duplicate.fid = failedFid;
    Check(store.ApplyEditLog(duplicate) == -EEXIST,
        "edit log duplicate create fails");
    Check(store.GetCommittedTxn() == failedTxnId,
        "edit log failed create commits no-op txn");
    Check(store.GetLastTxn() == failedTxnId,
        "edit log failed create advances txn seed");

    LookupResult lookup;
    Check(store.Lookup(ROOTFID, "dup", lookup) == 0 && lookup.fid == fid,
        "edit log failed create keeps original entry");

    CreateResult after;
    Check(store.Create(ROOTFID, "after_failed", kInodeTypeFile, &after) == 0,
        "edit log create after failed no-op");
    Check(after.txnId == failedTxnId + 1 && after.fid > failedFid,
        "edit log create continues after failed no-op");
}


    void
TestNamespaceStoreEditLogBatchCreateCommit()
{
    Config cfg;
    NamespaceStore store(cfg);

    fid_t parentFid = -1;
    TxnId parentTxn = 0;
    store.ReserveCreateIds(parentFid, parentTxn);
    EditLogRecord parent;
    parent.type = EditLogRecord::kCreate;
    parent.txnId = parentTxn;
    parent.parentFid = ROOTFID;
    parent.name = "batch_parent";
    parent.fid = parentFid;
    parent.inodeType = kInodeTypeDir;
    parent.mode = 0755;
    Check(store.ApplyCreate(parent.parentFid, parent.name, parent.inodeType,
            parent.fid, parent.txnId, kKfsUserRoot, kKfsGroupRoot,
            parent.mode, 0, 0, false, false) == 0,
        "batch parent create succeeds");
    Check(store.GetCommittedTxn() < parentTxn,
        "edit log batch parent not globally committed yet");

    fid_t childFid = -1;
    TxnId childTxn = 0;
    store.ReserveCreateIds(childFid, childTxn);
    EditLogRecord child;
    child.type = EditLogRecord::kCreate;
    child.txnId = childTxn;
    child.parentFid = parentFid;
    child.name = "child";
    child.fid = childFid;
    child.inodeType = kInodeTypeDir;
    child.mode = 0755;
    Check(store.ApplyCreate(child.parentFid, child.name, child.inodeType,
            child.fid, child.txnId, kKfsUserRoot, kKfsGroupRoot,
            child.mode, 0, 0, false, false) == 0,
        "batch child sees prior parent create");

    store.CommitThroughRange(parentTxn, childTxn);
    LookupResult lookup;
    Check(store.Lookup(parentFid, "child", lookup) == 0 &&
            lookup.fid == childFid,
        "edit log batch committed child lookup");
    Check(store.GetCommittedTxn() == childTxn,
        "edit log batch commit through last txn");
}


    void
TestNamespaceStoreConcurrentShardLocks()
{
    Config cfg;
    cfg.dirLargeThreshold = 8;
    NamespaceStore store(cfg);

    const int kThreads = 8;
    const int kFilesPerThread = 100;
    vector<fid_t> parents;
    parents.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        CreateResult dir;
        Check(store.Create(ROOTFID, MakeName("cd", t),
                kInodeTypeDir, &dir) == 0,
            "concurrent mkdir parent");
        store.CommitThrough(dir.txnId);
        parents.push_back(dir.fid);
    }

    std::atomic<int> failures(0);
    vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.push_back(std::thread([&store, &parents, &failures, t]() {
            for (int i = 0; i < kFilesPerThread; ++i) {
                CreateResult create;
                const int status = store.Create(parents[t],
                    MakeName("f", t, i), kInodeTypeFile, &create);
                if (status != 0) {
                    ++failures;
                    continue;
                }
                store.CommitThrough(create.txnId);
            }
        }));
    }
    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }
    Check(failures.load() == 0, "concurrent create threads");

    LookupResult lookup;
    for (int t = 0; t < kThreads; ++t) {
        for (int i = 0; i < kFilesPerThread; ++i) {
            Check(store.Lookup(parents[t], MakeName("f", t, i),
                    lookup) == 0,
                "concurrent create lookup");
        }
    }

    failures = 0;
    threads.clear();
    for (int t = 0; t < kThreads; ++t) {
        threads.push_back(std::thread([&store, &parents, &failures, t]() {
            for (int i = 0; i < kFilesPerThread; ++i) {
                TxnId txnId = 0;
                const int status = store.Rename(parents[t],
                    MakeName("f", t, i), MakeName("g", t, i),
                    false, &txnId, 0);
                if (status != 0) {
                    ++failures;
                    continue;
                }
                store.CommitThrough(txnId);
            }
        }));
    }
    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }
    Check(failures.load() == 0, "concurrent rename threads");
    for (int t = 0; t < kThreads; ++t) {
        for (int i = 0; i < kFilesPerThread; ++i) {
            Check(store.Lookup(parents[t], MakeName("f", t, i),
                    lookup) == -ENOENT,
                "concurrent rename old missing");
            Check(store.Lookup(parents[t], MakeName("g", t, i),
                    lookup) == 0,
                "concurrent rename new visible");
        }
    }

    failures = 0;
    threads.clear();
    for (int t = 0; t < kThreads; ++t) {
        threads.push_back(std::thread([&store, &parents, &failures, t]() {
            for (int i = 0; i < kFilesPerThread; ++i) {
                TxnId txnId = 0;
                const int status = store.RemoveFile(parents[t],
                    MakeName("g", t, i), &txnId);
                if (status != 0) {
                    ++failures;
                    continue;
                }
                store.CommitThrough(txnId);
            }
        }));
    }
    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }
    Check(failures.load() == 0, "concurrent remove threads");
    for (int t = 0; t < kThreads; ++t) {
        for (int i = 0; i < kFilesPerThread; ++i) {
            Check(store.Lookup(parents[t], MakeName("g", t, i),
                    lookup) == -ENOENT,
                "concurrent remove missing");
        }
    }
}


    void
TestResourceLockOrdering()
{
    vector<ResourceLockKey> locks;
    locks.push_back(ResourceLockKey(ResourceLockKey::kInode, 10));
    locks.push_back(ResourceLockKey(ResourceLockKey::kDir, 3, 20));
    locks.push_back(ResourceLockKey(ResourceLockKey::kSnapshot, 0));
    locks.push_back(ResourceLockKey(ResourceLockKey::kDir, 2, 30));
    locks.push_back(ResourceLockKey(ResourceLockKey::kEditLog, 0));
    std::sort(locks.begin(), locks.end());
    Check(locks[0].resourceClass == ResourceLockKey::kSnapshot,
        "snapshot lock first");
    Check(locks[1].resourceClass == ResourceLockKey::kDir &&
            locks[1].major == 2,
        "dir locks sorted by shard");
    Check(locks[2].resourceClass == ResourceLockKey::kDir &&
            locks[2].major == 3,
        "dir locks sorted by shard second");
    Check(locks[3].resourceClass == ResourceLockKey::kInode,
        "inode after dir");
    Check(locks[4].resourceClass == ResourceLockKey::kEditLog,
        "edit log last");
}

} // namespace

    int
main(
    int    /* argc */,
    char** /* argv */)
{
    TestConfig();
    TestNameKey();
    TestPendingCommittedVisibility();
    TestSmallCookieInvalidation();
    TestPromotion();
    TestLargeCookieStableAcrossInsert();
    TestInodeTable();
    TestNamespaceStoreBasic();
    TestNamespaceStoreDirectory();
    TestNamespaceStorePathAndRmdir();
    TestNamespaceStoreRename();
    TestNamespaceStoreCheckpoint();
    TestNamespaceStoreEditLog();
    TestNamespaceStoreEditLogFailedCreateNoop();
    TestNamespaceStoreEditLogBatchCreateCommit();
    TestNamespaceStoreConcurrentShardLocks();
    TestResourceLockOrdering();
    if (gErrorCount != 0) {
        cout << gErrorCount << " NamespaceV2 tests failed\n";
        return 1;
    }
    cout << "NamespaceV2 tests passed\n";
    return 0;
}
