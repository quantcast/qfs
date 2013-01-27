/*
 * $Id$
 *
 * \file restore.cc
 * \brief rebuild metatree from saved checkpoint
 * \author Blake Lewis (Kosmix Corp.)
 *         Mike Ovsiannikov
 *
 * Copyright 2008-2012 Quantcast Corp.
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

#include <fcntl.h>
#include <cerrno>
#include <cstring>
#include "Restorer.h"
#include "util.h"
#include "Logger.h"
#include "meta.h"
#include "kfstree.h"
#include "Replay.h"
#include "Restorer.h"
#include "DiskEntry.h"
#include "Checkpoint.h"
#include "LayoutManager.h"
#include "common/MdStream.h"
#include "common/MsgLogger.h"
#include "qcdio/QCUtils.h"

namespace KFS
{
using std::cerr;
using std::string;

static int16_t minReplicasPerFile = 0;

static bool
checkpoint_seq(DETokenizer& c)
{
    c.pop_front();
    if (c.empty())
        return false;
    seq_t highest = (seq_t)c.toNumber();
    oplog.set_seqno(highest);
    return (highest >= 0);
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
    return (replayer.openlog(s) == 0);
}

bool
restore_chunkVersionInc(DETokenizer& c)
{
    c.pop_front();
    // No longer used. For backward compatibility only.
    return (! c.empty() && c.toNumber() >= 1);
}

static bool
restore_dentry(DETokenizer& c)
{
    string name;
    fid_t id, parent;
    c.pop_front();
    bool ok = pop_name(name, "name", c, true);
    ok = pop_fid(id, "id", c, ok);
    ok = pop_fid(parent, "parent", c, ok);
    if (!ok)
        return false;

    MetaDentry* const d = MetaDentry::create(parent, name, id, 0);
    return (metatree.insert(d) == 0);
}

static bool
restore_striped_file_params(DETokenizer& c, MetaFattr& f)
{
    chunkOff_t t = 0, n = 0, nr = 0, ss = 0;
    if (! pop_offset(t, "striperType", c, true)) {
        f.striperType        = KFS_STRIPED_FILE_TYPE_NONE;
        f.numStripes         = 0;
        f.numRecoveryStripes = 0;
        f.stripeSize         = 0;
        return true;
    }
    return (
        pop_offset(n,  "numStripes",         c, true) &&
        pop_offset(nr, "numRecoveryStripes", c, true) &&
        pop_offset(ss, "stripeSize",         c, true) &&
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
    int64_t mtime, ctime, crtime;
    int16_t numReplicas;

    bool ok = pop_type(type, "fattr", c, true);
    ok = pop_fid(fid, "id", c, ok);
    ok = pop_fid(chunkcount, "chunkcount", c, ok);
    ok = pop_short(numReplicas, "numReplicas", c, ok);
    ok = pop_time(mtime, "mtime", c, ok);
    ok = pop_time(ctime, "ctime", c, ok);
    ok = pop_time(crtime, "crtime", c, ok);
    if (!ok) {
        return false;
    }
    // filesize is optional; if it isn't there, we can re-compute
    // by asking the chunkservers
    const bool gotfilesize = pop_offset(filesize, "filesize", c, true) &&
        filesize >= 0;
    if (numReplicas < minReplicasPerFile) {
        numReplicas = minReplicasPerFile;
    }
    // chunkcount is an estimate; recompute it as we add chunks to the file.
    // reason for it being estimate: if a CP is in progress while the
    // metatree is updated, we have cases where the chunkcount is off by 1
    // and the checkpoint contains the newly added chunk.
    MetaFattr* const f = MetaFattr::create(type, fid, mtime, ctime, crtime,
        0, numReplicas, kKfsUserNone, kKfsGroupNone, kKfsModeUndef);
    if (type != KFS_DIR) {
        f->filesize = gotfilesize ? filesize : chunkOff_t(-1);
        if (! restore_striped_file_params(c, *f)) {
            f->destroy();
            return false;
        }
    }
    int64_t n = f->user;
    const bool gotperms = pop_num(n, "user", c, true);
    if (gotperms) {
        f->user = (kfsUid_t)n;
        n = f->group;
        if (! pop_num(n, "group", c, true)) {
            f->destroy();
            return false;
        }
        f->group = (kfsGid_t)n;
        n = f->mode;
        if (! pop_num(n, "mode", c, true)) {
            f->destroy();
            return false;
        }
        f->mode = (kfsMode_t)n;
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
    bool ok = pop_fid(fid, "fid", c, true);
    ok = pop_fid(cid, "chunkid", c, ok);
    ok = pop_offset(offset, "offset", c, ok);
    ok = pop_fid(chunkVersion, "chunkVersion", c, ok);
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
        const chunkOff_t boundary = chunkStartOffset(offset);
    bool newEntryFlag = false;
    MetaChunkInfo* const ch = gLayoutManager.AddChunkToServerMapping(
        fa, boundary, cid, chunkVersion, newEntryFlag);
    if (! ch || ! newEntryFlag) {
        return false;
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

    return (ok && gLayoutManager.ReplayBeginChangeChunkVersion(
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

static DiskEntry&
get_entry_map()
{
    static bool initied = false;
    static DiskEntry e;
    if (initied) {
        return e;
    }
    e.add_parser("setintbase", restore_setintbase);
    e.add_parser("checkpoint", checkpoint_seq);
    e.add_parser("version", checkpoint_version);
    e.add_parser("fid", checkpoint_fid);
    e.add_parser("chunkId", checkpoint_chunkId);
    e.add_parser("time", checkpoint_time);
    e.add_parser("log", checkpoint_log);
    e.add_parser("chunkVersionInc", restore_chunkVersionInc);
    e.add_parser("dentry", restore_dentry);
    e.add_parser("fattr", restore_fattr);
    e.add_parser("chunkinfo", restore_chunkinfo);
    e.add_parser("mkstable", restore_makestable);
    e.add_parser("beginchunkversionchange", restore_beginchunkversionchange);
    e.add_parser("checksum", restore_checksum);
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
Restorer::rebuild(const string cpname, int16_t minReplicas)
{
    if (metatree.getFattr(ROOTFID)) {
        KFS_LOG_STREAM_FATAL <<
            cpname <<
                        ": initial fs / meta tree is not empty" <<
        KFS_LOG_EOM;
        return false;
    }
    minReplicasPerFile = minReplicas;
    file.open(cpname.c_str(), ofstream::binary | ofstream::in);
    if (file.fail()) {
        const int err = errno;
        KFS_LOG_STREAM_FATAL <<
            cpname <<
                        ": " << QCUtils::SysError(err) <<
        KFS_LOG_EOM;
        return false;
    }

    DiskEntry& entrymap = get_entry_map();
        DETokenizer tokenizer(file);

    restoreChecksum.clear();
    lastLineChecksumFlag = false;
    MdStream mds(0, false, string(), 0);
    bool is_ok = true;
    while (tokenizer.next(&mds)) {
        if (! entrymap.parse(tokenizer)) {
            KFS_LOG_STREAM_FATAL <<
                cpname <<
                                ":" << tokenizer.getEntryCount() <<
                ":" << tokenizer.getEntry() <<
            KFS_LOG_EOM;
                        is_ok = false;
                        break;
        }
        if (! restoreChecksum.empty()) {
            if (tokenizer.next()) {
                KFS_LOG_STREAM_FATAL <<
                    cpname <<
                    ": entry after checksum" <<
                KFS_LOG_EOM;
                is_ok = false;
            }
            break;
        }
    }
    if (is_ok && ! file.eof()) {
        KFS_LOG_STREAM_FATAL <<
            "error " << cpname <<
                        ":" << tokenizer.getEntryCount() <<
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
    return is_ok;
}

int
try_to_acquire_lockfile(const string &lockfn)
{
    const int fd = open(lockfn.c_str(), O_APPEND|O_CREAT|O_RDWR, 0644);
    if (fd < 0) {
        return (errno > 0 ? -errno : -1);
    }
    struct flock fl;
    memset(&fl, 0, sizeof(fl));
    fl.l_type = F_WRLCK;
    fl.l_whence = SEEK_SET;
    if (fcntl(fd, F_SETLK, &fl)) {
        const int err = errno;
        close(fd);
        return (err > 0 ? -err : -1);
    }
    return fd;
}

void
acquire_lockfile(const string &lockfn, int ntries)
{
    for (int i = 0; i < ntries; i++) {
        const int ret = try_to_acquire_lockfile(lockfn);
        if (ret >= 0) {
            return;
        }
        if (ret != -EACCES && ret != -EAGAIN) {
            cerr << "failed to open lock file: " << lockfn <<
                ": " << strerror(-ret) << " exiting.\n";
            exit(-1);
        }
        cerr << "lock file: " << lockfn << " is busy; waiting...\n";
        sleep(60);
    }
    cerr << "failed to open lock file: " << lockfn << " after "
        << ntries << " exiting.\n";
    exit(-1);
}

}
