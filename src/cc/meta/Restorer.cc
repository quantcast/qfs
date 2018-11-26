/*
 * $Id$
 *
 * \file restore.cc
 * \brief rebuild metatree from saved checkpoint
 * \author Blake Lewis (Kosmix Corp.)
 *         Mike Ovsiannikov
 *
 * Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
 * Copyright 2006-2008 Kosmix Corp.
 *
 * This file is part of Kosmos File System (KFS).
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include "Restorer.h"
#include "util.h"
#include "meta.h"
#include "kfstree.h"
#include "Replay.h"
#include "Restorer.h"
#include "DiskEntry.h"
#include "Checkpoint.h"
#include "LayoutManager.h"
#include "NetDispatch.h"
#include "LogWriter.h"
#include "MetaVrSM.h"

#include "common/MdStream.h"
#include "common/MsgLogger.h"
#include "common/StringIo.h"

#include "qcdio/QCUtils.h"

#include <fcntl.h>
#include <cerrno>
#include <cstring>
#include <fstream>

namespace KFS
{
using std::cerr;
using std::string;
using std::ifstream;

static int16_t sMinReplicasPerFile     = 0;
static bool    sHasVrSequenceFlag      = false;
static bool    sVrSequenceRequiredFlag = false;

static bool
checkpoint_seq(DETokenizer& c)
{
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    const seq_t highest = (seq_t)c.toNumber();
    if (highest < 0 || ! c.isLastOk()) {
        return false;
    }
    c.pop_front();
    if (! c.empty()) {
        const int64_t chksum = c.toNumber();
        if (! c.isLastOk()) {
            return false;
        }
        c.pop_front();
        replayer.setErrChksum(chksum);
    }
    seq_t epoch = 0;
    seq_t view  = 0;
    if (! c.empty()) {
        epoch = c.toNumber();
        if (! c.isLastOk() || epoch < 0) {
            return false;
        }
        c.pop_front();
        if (c.empty()) {
            return false;
        }
        view = c.toNumber();
        if (! c.isLastOk() || view < 0 ) {
            return false;
        }
        sHasVrSequenceFlag = true;
    } else if (sVrSequenceRequiredFlag) {
        KFS_LOG_STREAM_ERROR <<
            "checkpoint has no VR sequence" <<
        KFS_LOG_EOM;
        return false;
    }
    replayer.setCommitted(MetaVrLogSeq(epoch, view, highest));
    return true;
}

static bool
checkpoint_fid(DETokenizer& c)
{
    c.pop_front();
    if (c.empty())
        return false;
    fid_t highest = (fid_t)c.toNumber();
    fileID.setseed(highest);
    return (highest > 0);
}

static bool
checkpoint_chunkId(DETokenizer& c)
{
    c.pop_front();
    if (c.empty())
        return false;
    fid_t highest = (fid_t)c.toNumber();
    chunkID.setseed(highest);
    return (highest > 0);
}

static bool
checkpoint_version(DETokenizer& c)
{
    c.pop_front();
    if (c.empty())
        return false;
    int version = (int)c.toNumber();
    return version == Checkpoint::VERSION;
}

static bool
checkpoint_time(DETokenizer& c)
{
    c.pop_front();
    KFS_LOG_STREAM_INFO << "restoring from checkpoint of " << c.front() <<
    KFS_LOG_EOM;
    return true;
}

static bool
checkpoint_log(DETokenizer& c)
{
    c.pop_front();
    string s;
    while (!c.empty()) {
        s += c.front();
        c.pop_front();
        if (!c.empty())
            s += "/";
    }
    return (replayer.openlog(s) == 0 &&
        replayer.verifyLogSegmentsPresent());
}

bool
restore_chunkVersionInc(DETokenizer& c)
{
    c.pop_front();
    // No longer used. For backward compatibility only.
    return (! c.empty() && c.toNumber() >= 1);
}

static bool sShortNamesFlag = false;

static bool
restore_short_names(DETokenizer& c)
{
    if (2 != c.size()) {
        return false;
    }
    c.pop_front();
    const int64_t n = c.toNumber();
    if (! c.isLastOk()) {
        return false;
    }
    sShortNamesFlag = 0 != n;
    return true;
}

static bool
restore_dentry(DETokenizer& c)
{
    string name;
    fid_t id, parent;
    c.pop_front();
    bool ok = pop_name(name, sShortNamesFlag ? "n" : "name",   c, true);
    ok = pop_fid(id,         sShortNamesFlag ? "i" : "id",     c, ok);
    ok = pop_fid(parent,     sShortNamesFlag ? "p" : "parent", c, ok);
    if (!ok)
        return false;

    MetaDentry* const d = MetaDentry::create(parent, name, id, 0);
    return (metatree.insert(d) == 0);
}

static bool
restore_striped_file_params(DETokenizer& c, MetaFattr& f)
{
    chunkOff_t t = 0, n = 0, nr = 0, ss = 0;
    if (! pop_offset(t, sShortNamesFlag ? "s" : "striperType", c, true)) {
        f.striperType        = KFS_STRIPED_FILE_TYPE_NONE;
        f.numStripes         = 0;
        f.numRecoveryStripes = 0;
        f.stripeSize         = 0;
        return true;
    }
    return (
        pop_offset(n,  sShortNamesFlag ? "N" : "numStripes",         c, true) &&
        pop_offset(nr, sShortNamesFlag ? "R" : "numRecoveryStripes", c, true) &&
        pop_offset(ss, sShortNamesFlag ? "S" : "stripeSize",         c, true) &&
        f.SetStriped((int32_t)t, n, nr, ss) &&
        f.filesize >= 0
    );
}

static bool
restore_fattr(DETokenizer& c)
{
    FileType type;
    fid_t fid;
    fid_t chunkcount;
    chunkOff_t filesize = -1;
    int64_t mtime, ctime, atime;
    int16_t numReplicas;

    bool ok = pop_type(type,    sShortNamesFlag ? "a" : "fattr",       c, true);
    ok = pop_fid(fid,           sShortNamesFlag ? "i" : "id",          c, ok);
    ok = pop_fid(chunkcount,    sShortNamesFlag ? "c" : "chunkcount",  c, ok);
    ok = pop_short(numReplicas, sShortNamesFlag ? "r" : "numReplicas", c, ok);
    ok = pop_time(mtime,        sShortNamesFlag ? "m" : "mtime",       c, ok);
    ok = pop_time(ctime,        sShortNamesFlag ? "c" : "ctime",       c, ok);
    ok = pop_time(atime,        sShortNamesFlag ? "C" : "crtime",      c, ok);
    if (!ok) {
        return false;
    }
    // filesize is optional; if it isn't there, we can re-compute
    // by asking the chunkservers
    const bool gotfilesize = pop_offset(
        filesize, sShortNamesFlag ? "e" : "filesize", c, true) &&
        (filesize >= 0 || 0 == numReplicas);
    if (0 != numReplicas && numReplicas < sMinReplicasPerFile) {
        numReplicas = sMinReplicasPerFile;
    }
    // chunkcount is an estimate; recompute it as we add chunks to the file.
    // reason for it being estimate: if a CP is in progress while the
    // metatree is updated, we have cases where the chunkcount is off by 1
    // and the checkpoint contains the newly added chunk.
    MetaFattr* const f = MetaFattr::create(type, fid, mtime, ctime, atime,
        0, numReplicas, kKfsUserNone, kKfsGroupNone, kKfsModeUndef);
    if (type != KFS_DIR) {
        f->filesize = gotfilesize ? filesize : chunkOff_t(-1);
        if (! restore_striped_file_params(c, *f)) {
            f->destroy();
            return false;
        }
    }
    int64_t n = f->user;
    const bool gotperms = ! c.empty();
    if (gotperms) {
        if (! pop_num(n, sShortNamesFlag ? "u" : "user", c, true)) {
            f->destroy();
            return false;
        }
        f->user = (kfsUid_t)n;
        n = f->group;
        if (! pop_num(n, sShortNamesFlag ? "g" : "group", c, true)) {
            f->destroy();
            return false;
        }
        f->group = (kfsGid_t)n;
        n = f->mode;
        if (! pop_num(n, sShortNamesFlag ? "M" : "mode", c, true)) {
            f->destroy();
            return false;
        }
        f->mode = (kfsMode_t)n;
        if ((type == KFS_FILE || type == KFS_DIR) && ! c.empty() &&
                pop_num(n, sShortNamesFlag ? "t" : "minTier", c, ok)) {
            f->minSTier = (kfsSTier_t)n;
            if (! pop_num(n, sShortNamesFlag ? "T" : "maxTier", c, ok)) {
                f->destroy();
                return false;
            }
            f->maxSTier = (kfsSTier_t)n;
            if (f->maxSTier < f->minSTier ||
                    ! IsValidSTier(f->minSTier) ||
                    ! IsValidSTier(f->maxSTier)) {
                f->destroy();
                return false;
            }
        }
        if (c.isLastOk() && ! c.empty() &&
                pop_num(n, sShortNamesFlag ? "o" : "nextChunkOffset", c, ok)) {
            if (n < 0 || n % CHUNKSIZE != 0) {
                f->destroy();
                return false;
            }
            if (0 == numReplicas) {
                f->nextChunkOffset() = (chunkOff_t)n;
            }
        }
        if (c.isLastOk() && ! c.empty() && pop_num(n, "a", c, ok) &&
                kFileAttrExtTypeNone != n) {
            string str;
            if (! c.isLastOk() || c.empty() || n < kFileAttrExtTypeNone ||
                    kFileAttrExtTypeEnd <= n ||
                    ! StringIo::Unescape(c.front().ptr, c.front().len, str)) {
                f->destroy();
                return false;
            }
            c.pop_front();
            f->SetExtAttributes(FileAttrExtTypes(n), str);
        }
        if (! c.isLastOk()) {
            f->destroy();
            return false;
        }
    } else {
        f->user  = gLayoutManager.GetDefaultLoadUser();
        f->group = gLayoutManager.GetDefaultLoadGroup();
        f->mode  = type == KFS_DIR ?
            gLayoutManager.GetDefaultLoadDirMode() :
            gLayoutManager.GetDefaultLoadFileMode();
    }
    if (f->user == kKfsUserNone || f->group == kKfsGroupNone ||
            f->mode == kKfsModeUndef) {
        f->destroy();
        return false;
    }
    if (metatree.insert(f) != 0) {
        return false;
    }
    if (type == KFS_DIR) {
        UpdateNumDirs(1);
    } else {
        UpdateNumFiles(1);
    }
    return true;
}

static bool
restore_chunkinfo(DETokenizer& c)
{
    fid_t fid;
    chunkId_t cid;
    chunkOff_t offset;
    seq_t chunkVersion;

    c.pop_front();
    bool ok = pop_fid(fid,     sShortNamesFlag ? "i" : "fid",          c, true);
    ok = pop_fid(cid,          sShortNamesFlag ? "c" : "chunkid",      c, ok);
    ok = pop_offset(offset,    sShortNamesFlag ? "o" : "offset",       c, ok);
    ok = pop_fid(chunkVersion, sShortNamesFlag ? "v" : "chunkVersion", c, ok);
    if (!ok) {
        return false;
    }

    // The chunks of a file are stored next to each other in the tree and
    // are written out contigously.  Use this property when restoring the
    // chunkinfo: stash the fileattr for the the file we are currently
    // working on; as long as this doesn't change, we avoid tree lookups.
    static MetaFattr* sCurrFa = 0;
    MetaFattr* fa = sCurrFa;
    if (! fa || fa->id() != fid) {
        fa = metatree.getFattr(fid);
        sCurrFa = fa;
    }
    if (! fa) {
        return false;
    }
    const char* idxs;
    size_t      idxsLen;
    if (! c.empty() && (sShortNamesFlag ? "s" : "si") == c.front()) {
        c.pop_front();
        if (c.empty()) {
            return false;
        }
        const DETokenizer::Token tok = c.front();
        idxs    = tok.ptr;
        idxsLen = tok.len;
        if (idxsLen <= 0) {
            return false;
        }
    } else {
        idxs    = 0;
        idxsLen = 0;
    }
    const chunkOff_t boundary = chunkStartOffset(offset);
    bool newEntryFlag = false;
    MetaChunkInfo* const ch = gLayoutManager.AddChunkToServerMapping(
        fa, boundary, cid, chunkVersion, newEntryFlag);
    if (! ch || ! newEntryFlag) {
        return false;
    }
    if (0 < idxsLen) {
        if (! gLayoutManager.Restore(
                *ch, idxs, idxsLen, 16 == c.getIntBase())) {
            return false;
        }
        c.pop_front();
    }
    if (metatree.insert(ch) != 0) {
        return false;
    }
    if (boundary >= fa->nextChunkOffset()) {
        fa->nextChunkOffset() = boundary + CHUNKSIZE;
    }
    fa->chunkcount()++;
    UpdateNumChunks(1);
    return true;
}

static bool
restore_makestable(DETokenizer& c)
{
    chunkId_t  chunkId;
    seq_t      chunkVersion;
    chunkOff_t chunkSize;
    int64_t    tmp;
    uint32_t   checksum;
    bool       hasChecksum;

    c.pop_front();
    bool ok = pop_fid(chunkId, "chunkId", c, true);
    ok = pop_fid(chunkVersion, "chunkVersion", c, ok);
    ok = pop_num(tmp, "size", c, ok);
    chunkSize = (chunkOff_t)tmp;
    ok = pop_num(tmp, "checksum", c, ok);
    checksum = (uint32_t)tmp;
    ok = pop_num(tmp, "hasChecksum", c, ok);
    hasChecksum = tmp != 0;
    if (ok) {
        gLayoutManager.ReplayPendingMakeStable(
            chunkId, chunkVersion, chunkSize,
            hasChecksum, checksum, true);
    }
    return ok;
}

static bool
restore_beginchunkversionchange(DETokenizer& c)
{
    fid_t     fid;
    chunkId_t chunkId;
    seq_t     chunkVersion;

    c.pop_front();
    bool ok = pop_fid(fid,          "file",         c, true);
    ok = pop_fid     (chunkId,      "chunkId",      c, ok);
    ok = pop_fid     (chunkVersion, "chunkVersion", c, ok);

    return (ok && gLayoutManager.RestoreBeginChangeChunkVersion(
            fid, chunkId, chunkVersion));
}

bool
restore_setintbase(DETokenizer& c)
{
    c.pop_front();
    if (c.empty())
        return false;
    const int base = (int)c.toNumber();
    if (base != 16 && base != 10)
        return false;
    c.setIntBase(base);
    return true;
}

string restoreChecksum;
bool   lastLineChecksumFlag = false;

bool
restore_checksum(DETokenizer& c)
{
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    const string val = c.front();
    if (val.empty()) {
        return false;
    }
    if (val == "last-line") {
        lastLineChecksumFlag = true;
    } else {
        restoreChecksum = val;
    }
    return true;
}

bool
restore_delegate_cancel(DETokenizer& c)
{
    c.pop_front();
    bool    ok     = true;
    int64_t exp    = -1;
    int64_t issued = -1;
    int64_t uid    = 0;
    int64_t seq    = 0;
    int64_t flags  = 0;
    ok = pop_num(exp,    "exp",    c, ok);
    ok = pop_num(issued, "issued", c, ok);
    ok = pop_num(uid,    "uid",    c, ok);
    ok = pop_num(seq,    "seq",    c, ok);
    ok = pop_num(flags,  "flags",  c, ok);
    if (! ok || exp <= issued || uid == kKfsUserNone) {
        return false;
    }
    gNetDispatch.CancelToken(
        exp,
        issued,
        (kfsUid_t)uid,
        seq,
        (uint16_t)flags
    );
    return true;
}

bool
restore_filesystem_info(DETokenizer& c)
{
    c.pop_front();
    int64_t fsid   = -1;
    int64_t crtime = 0;
    bool ok = pop_num(fsid,      "fsid", c, true);
    ok =      pop_time(crtime, "crtime", c, ok);
    if (ok) {
        metatree.SetFsInfo(fsid, crtime);
    }
    return (ok && 0 <= fsid);
}

bool
restore_idempotent_request(DETokenizer& c)
{
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    const DETokenizer::Token& token = c.front();
    return gLayoutManager.GetIdempotentRequestTracker().Read(
        token.ptr, token.len) == 0;
}

static bool
restore_group_users_reset(DETokenizer& c)
{
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    gLayoutManager.GetUserAndGroup().ClearGroups();
    return true;
}

static const DETokenizer::Token kGUContinue("guc");

static bool
restore_group_users(DETokenizer& c)
{
    const bool appendFlag = c.front() == kGUContinue;
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    const DETokenizer::Token& token = c.front();
    return gLayoutManager.GetUserAndGroup().ReadGroup(
        token.ptr, token.len, appendFlag, c.getIntBase() == 16) == 0;
}

static bool
restore_objstore_delete(DETokenizer& c)
{
    const bool osdFlag = c.front() == DETokenizer::Token("osd", 3);
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    const chunkId_t chunkId = c.toNumber();
    if (! c.isLastOk()) {
        return false;
    }
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    const chunkOff_t last = c.toNumber();
    if (! c.isLastOk()) {
        return false;
    }
    return gLayoutManager.AddPendingObjStoreDelete(
        chunkId, osdFlag ? last : chunkOff_t(0), last);
}

static bool
restore_chunk_server_start(DETokenizer& c)
{
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    if (c.front() != "loc") {
        return false;
    }
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    const DETokenizer::Token& ltok = c.front();
    ServerLocation loc;
    if (! loc.FromString(ltok.ptr, ltok.len, c.getIntBase() == 16)) {
        return false;
    }
    c.pop_front();
    int64_t idx;
    if (! pop_num(idx, "idx", c, true)) {
        return false;
    }
    int64_t chunks;
    if (! pop_num(chunks, "chunks", c, true) || chunks < 0) {
        return false;
    }
    if (c.empty() || c.front() != "chksum") {
        return false;
    }
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    CIdChecksum chksum;
    const DETokenizer::Token& ctk = c.front();
    const char* ptr = ctk.ptr;
    if (! (c.getIntBase() == 16 ?
            chksum.Parse<HexIntParser>(ptr, ctk.len) :
            chksum.Parse<DecIntParser>(ptr, ctk.len))) {
        return false;
    }
    c.pop_front();
    int64_t n = 0;
    if (! pop_num(n, "retire", c, true)) {
        return false;
    }
    const bool retiringFlag = 0 != n;
    int64_t retstart = 0;
    if (! pop_num(retstart, "retirestart", c, true)) {
        return false;
    }
    int64_t retdown = 0;
    if (! pop_num(retdown, "retiredown", c, true)) {
        return false;
    }
    if (! pop_num(n, "retired", c, true)) {
        return false;
    }
    const bool retiredFlag = 0 != n;
    if (! pop_num(n, "replay", c, true) && 1 != n && 0 != n) {
        return false;
    }
    n = -1;
    if (! pop_num(n, "rack", c, true)) {
        return false;
    }
    const LayoutManager::RackId rack = (LayoutManager::RackId)n;
    n = -1;
    if (! pop_num(n, "pnotify", c, true)) {
        return false;
    }
    const bool pendingHelloNotifyFlag = 0 != n;
    return gLayoutManager.RestoreChunkServer(
        loc, (size_t)idx, (size_t)chunks, chksum, retiringFlag,
        retstart, retdown, retiredFlag, rack, pendingHelloNotifyFlag);
}

static bool
restore_chunk_server(DETokenizer& c)
{
    if (c.empty() || 3 != c.front().len) {
        return false;
    }
    const int type = c.front().ptr[2] & 0xFF;
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    ChunkServerPtr const server = gLayoutManager.RestoreGetChunkServer();
    if (! server) {
        return false;
    }
    size_t idx = 0;
    while (! c.empty()) {
        const int64_t n = c.toNumber();
        if (! c.isLastOk() || ! server->Restore(type, idx, n)) {
            return false;
        }
        c.pop_front();
        idx++;
    }
    if ('e' == type) {
        gLayoutManager.RestoreClearChunkServer();
        return restore_chunk_server_end(c);
    }
    return true;
}

static bool
restore_hibernated_cs_start(DETokenizer& c)
{
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    if (c.front() != "loc") {
        return false;
    }
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    const DETokenizer::Token& ltok = c.front();
    ServerLocation loc;
    if (! loc.FromString(ltok.ptr, ltok.len, c.getIntBase() == 16)) {
        return false;
    }
    c.pop_front();
    int64_t idx;
    if (! pop_num(idx, "idx", c, true)) {
        return false;
    }
    int64_t chunks;
    if (! pop_num(chunks, "chunks", c, true) || chunks < 0) {
        return false;
    }
    if (c.empty() || c.front() != "chksum") {
        return false;
    }
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    CIdChecksum chksum;
    const DETokenizer::Token& ctk = c.front();
    const char* ptr = ctk.ptr;
    if (! (c.getIntBase() == 16 ?
            chksum.Parse<HexIntParser>(ptr, ctk.len) :
            chksum.Parse<DecIntParser>(ptr, ctk.len))) {
        return false;
    }
    c.pop_front();
    int64_t start = 0;
    if (! pop_num(start, "start", c, true)) {
        return false;
    }
    int64_t end = 0;
    if (! pop_num(start, "end", c, true)) {
        return false;
    }
    int64_t n = 0;
    if (! pop_num(n, "retired", c, true)) {
        return false;
    }
    const bool retiredFlag = 0 != n;
    if (! pop_num(n, "replay", c, true)) {
        return false;
    }
    int64_t delReport = 0;
    if (c.front() == "dreport" &&
            (! pop_num(delReport, "dreport", c, true) || delReport < 0)) {
        return false;
    }
    if (c.empty() || c.front() != "modchksum") {
        return false;
    }
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    CIdChecksum modChksum;
    const DETokenizer::Token& mtk = c.front();
    ptr = mtk.ptr;
    if (! (c.getIntBase() == 16 ?
            modChksum.Parse<HexIntParser>(ptr, mtk.len) :
            modChksum.Parse<DecIntParser>(ptr, mtk.len))) {
        return false;
    }
    c.pop_front();
    const bool pendingHelloNotifyFlag =
        pop_num(n, "pnotify", c, true) && 0 != n;
    return gLayoutManager.RestoreHibernatedCS(
        loc, (size_t)idx, (size_t)chunks, chksum, modChksum,
        start, end, retiredFlag, pendingHelloNotifyFlag);
}

static bool
restore_hibernated_cs(DETokenizer& c)
{
    if (c.empty() || 4 != c.front().len) {
        return false;
    }
    const int type = c.front().ptr[3] & 0xFF;
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    HibernatedChunkServerPtr const server =
        gLayoutManager.RestoreGetHibernatedCS();
    if (! server) {
        return false;
    }
    size_t idx = 0;
    while (! c.empty()) {
        const int64_t n = c.toNumber();
        if (! c.isLastOk() || ! server->Restore(type, idx, n)) {
            return false;
        }
        c.pop_front();
        idx++;
    }
    if ('e' == type) {
        gLayoutManager.RestoreClearHibernatedCS();
    }
    return true;
}

static bool
restore_viewstamped_config(DETokenizer& c)
{
    if (c.empty() || 4 != c.front().len) {
        return false;
    }
    const int type = c.front().ptr[3] & 0xFF;
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    const DETokenizer::Token& tok = c.front();
    return MetaRequest::GetLogWriter().GetMetaVrSM().Restore(
        16 == c.getIntBase(), type, tok.ptr, tok.len);
}

static bool
restore_worm_mode(DETokenizer& c)
{
    if (c.size() < 2) {
        return false;
    }
    c.pop_front();
    const int64_t n = c.toNumber();
    if (! c.isLastOk()) {
        return false;
    }
    setWORMMode(0 != n);
    return true;
}

static bool
restore_crypto_key(DETokenizer& c)
{
    if (c.size() < 4) {
        return false;
    }
    c.pop_front();
    const int64_t time = c.toNumber();
    if (! c.isLastOk()) {
        return false;
    }
    c.pop_front();
    const int64_t id = c.toNumber();
    if (! c.isLastOk()) {
        return false;
    }
    c.pop_front();
    CryptoKeys::Key key;
    const bool kUrlSafeFmtFlag = true;
    if (! key.Parse(c.front().ptr, c.front().len, kUrlSafeFmtFlag)) {
        return false;
    }
    return gNetDispatch.Restore(id, key, time);
}

static const DiskEntry&
get_entry_map()
{
    static bool initied = false;
    static DiskEntry e;
    if (initied) {
        return e;
    }
    e.add_parser("setintbase",              &restore_setintbase);
    e.add_parser("checkpoint",              &checkpoint_seq);
    e.add_parser("version",                 &checkpoint_version);
    e.add_parser("fid",                     &checkpoint_fid);
    e.add_parser("chunkId",                 &checkpoint_chunkId);
    e.add_parser("time",                    &checkpoint_time);
    e.add_parser("log",                     &checkpoint_log);
    e.add_parser("chunkVersionInc",         &restore_chunkVersionInc);
    e.add_parser("dentry",                  &restore_dentry);
    e.add_parser("d",                       &restore_dentry);
    e.add_parser("fattr",                   &restore_fattr);
    e.add_parser("a",                       &restore_fattr);
    e.add_parser("chunkinfo",               &restore_chunkinfo);
    e.add_parser("c",                       &restore_chunkinfo);
    e.add_parser("mkstable",                &restore_makestable);
    e.add_parser("beginchunkversionchange", &restore_beginchunkversionchange);
    e.add_parser("checksum",                &restore_checksum);
    e.add_parser("delegatecancel",          &restore_delegate_cancel);
    e.add_parser("filesysteminfo",          &restore_filesystem_info);
    e.add_parser("idr",                     &restore_idempotent_request);
    e.add_parser("gu",                      &restore_group_users);
    e.add_parser("guc",                     &restore_group_users);
    e.add_parser("gur",                     &restore_group_users_reset);
    e.add_parser("osx",                     &restore_objstore_delete);
    e.add_parser("osd",                     &restore_objstore_delete);
    e.add_parser("cs",                      &restore_chunk_server_start);
    e.add_parser("cst",                     &restore_chunk_server);
    e.add_parser("css",                     &restore_chunk_server);
    e.add_parser("csr",                     &restore_chunk_server);
    e.add_parser("cse",                     &restore_chunk_server);
    e.add_parser("csp",                     &restore_chunk_server);
    e.add_parser("hcs",                     &restore_hibernated_cs_start);
    e.add_parser("hcsd",                    &restore_hibernated_cs);
    e.add_parser("hcsm",                    &restore_hibernated_cs);
    e.add_parser("hcse",                    &restore_hibernated_cs);
    e.add_parser("vrcn",                    &restore_viewstamped_config);
    e.add_parser("vrce",                    &restore_viewstamped_config);
    e.add_parser("worm",                    &restore_worm_mode);
    e.add_parser("ckey",                    &restore_crypto_key);
    e.add_parser("shortnames",              &restore_short_names);
    Replay::AddRestotreEntries(e);
    initied = true;
    return e;
}

inline static MetaFattr*
lookupFattr(fid_t dir, const string& name)
{
    MetaFattr* fa = 0;
    if (metatree.lookup(dir, name, kKfsUserRoot, kKfsGroupRoot, fa) == 0) {
        return fa;
    }
    return 0;
}

/*!
 * \brief rebuild metadata tree from CP file cpname
 * \param[in] cpname    the CP file
 * \param[in] minReplicas  the desired # of replicas for each chunk of a file;
 *   if the values in the checkpoint file are below this threshold, then
 *   bump replication.
 * \return      true if successful
 */
bool
Restorer::rebuild(const string& cpname, int16_t minReplicas)
{
    if (metatree.getFattr(ROOTFID)) {
        KFS_LOG_STREAM_FATAL <<
            cpname << ": initial fs / meta tree is not empty" <<
        KFS_LOG_EOM;
        return false;
    }
    if (! gLayoutManager.RestoreStart()) {
        return false;
    }
    sMinReplicasPerFile     = minReplicas;
    sVrSequenceRequiredFlag = mVrSequenceRequiredFlag;
    ifstream file;
    file.open(cpname.c_str(), ifstream::binary | ifstream::in);
    if (file.fail()) {
        const int err = errno;
        KFS_LOG_STREAM_FATAL <<
            cpname << ": " << QCUtils::SysError(err) <<
        KFS_LOG_EOM;
        return false;
    }

    const DiskEntry&  entrymap = get_entry_map();
    Replay::Tokenizer replayTokenizer(file, 0, 0);
    DETokenizer&      tokenizer = replayTokenizer.Get();

    restoreChecksum.clear();
    lastLineChecksumFlag = false;
    MdStream mds(0, false, string(), 0);
    bool is_ok = true;
    while (tokenizer.next(&mds)) {
        if (! entrymap.parse(tokenizer)) {
            KFS_LOG_STREAM_FATAL <<
                cpname << ":" << tokenizer.getEntryCount() <<
                ":" << tokenizer.getEntry() <<
            KFS_LOG_EOM;
            is_ok = false;
            break;
        }
        if (! restoreChecksum.empty()) {
            if (tokenizer.next()) {
                KFS_LOG_STREAM_FATAL <<
                    cpname << ": entry after checksum" <<
                KFS_LOG_EOM;
                is_ok = false;
            }
            break;
        }
    }
    if (is_ok && ! file.eof()) {
        KFS_LOG_STREAM_FATAL <<
            "error " << cpname << ":" << tokenizer.getEntryCount() <<
            ":" << tokenizer.getEntry() <<
        KFS_LOG_EOM;
        is_ok = false;
    }
    file.close();
    if (is_ok && lastLineChecksumFlag) {
        const string md = mds.GetMd();
        if (restoreChecksum != md) {
            KFS_LOG_STREAM_FATAL <<
                cpname <<
                ": checksum mismatch:"
                " expected:"  << restoreChecksum <<
                " computed: " << md <<
            KFS_LOG_EOM;
            is_ok = false;
        }
    }
    if (gLayoutManager.RestoreGetChunkServer() ||
            gLayoutManager.RestoreGetHibernatedCS()) {
        KFS_LOG_STREAM_FATAL <<
            cpname <<
            ": invalid incomplete chunk server entry" <<
        KFS_LOG_EOM;
        is_ok = false;
    }
    const MetaFattr* fa;
    if (is_ok && ! (
            (fa = metatree.getFattr(ROOTFID)) &&
            lookupFattr(ROOTFID, "/") == fa &&
            lookupFattr(ROOTFID, ".") == fa &&
            lookupFattr(ROOTFID, "..") == fa)) {
        KFS_LOG_STREAM_FATAL <<
            cpname <<
            ": invalid or missing root directory" <<
        KFS_LOG_EOM;
        is_ok = false;
    }
    if (is_ok) {
        if (sHasVrSequenceFlag) {
            const int err = metatree.checkDumpsterExists();
            if (err) {
                KFS_LOG_STREAM_FATAL <<
                    cpname << ": invalid or missing dumpster directory: " <<
                    QCUtils::SysError(-err) <<
                KFS_LOG_EOM;
                is_ok = false;
            }
        }
        if (is_ok) {
            metatree.setEnforceDumpsterRules(sHasVrSequenceFlag);
        }
    }
    if (is_ok) {
        // Set up back pointers, required for replay.
        metatree.setUpdatePathSpaceUsage(true);
        metatree.cleanupDumpster();
    }
    return is_ok;
}

int
try_to_acquire_lockfile(const string& lockfn)
{
    const int fd = open(lockfn.c_str(), O_APPEND|O_CREAT|O_RDWR, 0644);
    if (fd < 0) {
        return (errno > 0 ? -errno : -1);
    }
    if (fcntl(fd, F_SETFD, FD_CLOEXEC)) {
        const int err = errno;
        KFS_LOG_STREAM_ERROR <<
            "lock file: " << lockfn << " set close on exec failure: " <<
            QCUtils::SysError(err) <<
        KFS_LOG_EOM;
    }
    struct flock fl;
    memset(&fl, 0, sizeof(fl));
    fl.l_type   = F_WRLCK;
    fl.l_whence = SEEK_SET;
    if (fcntl(fd, F_SETLK, &fl)) {
        const int err = errno;
        close(fd);
        return (0 < err ? -err : (err < 0 ? err : -EIO));
    }
    return fd;
}

int
acquire_lockfile(const string& lockfn, int ntries)
{
    int ret = -EINVAL;
    for (int i = 0; i < ntries; i++) {
        if (0 <= (ret = try_to_acquire_lockfile(lockfn))) {
            return ret;
        }
        if (-EACCES != ret && -EAGAIN != ret) {
            KFS_LOG_STREAM_ERROR <<
                "failed to open lock file: " << lockfn <<
                ": " << strerror(-ret) <<
            KFS_LOG_EOM;
            break;
        }
        KFS_LOG_STREAM_INFO <<
            "lock file: " << lockfn << " is busy; will retry in 60 seconds" <<
        KFS_LOG_EOM;
        sleep(60);
    }
    KFS_LOG_STREAM_ERROR <<
        "failed to acquire lock file: " << lockfn <<
        " after " << ntries << " attempts" <<
    KFS_LOG_EOM;
    return ret;
}

int
restore_checkpoint(const string& lockfn, bool allowEmptyCheckpointFlag)
{
    if (! lockfn.empty()) {
        const int ret = acquire_lockfile(lockfn, 10);
        if (ret < 0) {
            return ret;
        }
    }
    if (! allowEmptyCheckpointFlag || file_exists(LASTCP)) {
        Restorer r;
        return (r.rebuild(LASTCP) ? 0 : -EIO);
    } else {
        return metatree.new_tree();
    }
}

}
