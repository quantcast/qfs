/*!
 * $Id$
 *
 * \file Replay.cc
 * \brief transaction log replay
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

#include "Logger.h"
#include "Replay.h"
#include "Restorer.h"
#include "util.h"
#include "DiskEntry.h"
#include "kfstree.h"
#include "LayoutManager.h"
#include "common/MdStream.h"
#include "common/MsgLogger.h"
#include "qcdio/QCUtils.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <sstream>

namespace KFS
{
using std::ostringstream;
using std::atoi;

Replay replayer;

/*!
 * \brief open saved log file for replay
 * \param[in] p a path in the form "<logdir>/log.<number>"
 */
int
Replay::openlog(const string &p)
{
    if (file.is_open()) {
        file.close();
    }
    KFS_LOG_STREAM_INFO <<
        "open log file: " << p.c_str() <<
    KFS_LOG_EOM;
    int                     num = -1;
    const string::size_type dot = p.rfind('.');
    if (dot != string::npos) {
        const char* const ptr = p.c_str() + dot + 1;
        if (*ptr != 0) {
            char* end = 0;
            const long val = strtol(ptr, &end, 10);
            num = (int)val;
            if (val != num || *end != 0) {
                num = -1;
            }
        }
    }
    if (num < 0) {
        KFS_LOG_STREAM_FATAL <<
            p << ": invalid log file name" <<
        KFS_LOG_EOM;
        return -EINVAL;
    }
    file.open(p.c_str());
    if (file.fail()) {
        const int err = errno;
        KFS_LOG_STREAM_FATAL <<
            p << ": " << QCUtils::SysError(err) <<
        KFS_LOG_EOM;
        return (err > 0 ? -err : (err == 0 ? -1 : err));
    }
    number = num;
    path   = p;
    return 0;
}

/*!
 * \brief check log version
 * format: version/<number>
 */
static bool
replay_version(DETokenizer& c)
{
    fid_t vers;
    bool ok = pop_fid(vers, "version", c, true);
    return (ok && vers == Logger::VERSION);
}

/*!
 * \brief handle common prefix for all log records
 */
static bool
pop_parent(fid_t &id, DETokenizer& c)
{
    c.pop_front();      // get rid of record type
    return pop_fid(id, "dir", c, true);
}

/*!
 * \brief update the seed of a UniqueID with what is passed in.
 * Since this function is called in the context of log replay, it
 * better be the case that the seed passed in is higher than
 * the id's seed (which was set from a checkpoint file).
*/
static void
updateSeed(UniqueID &id, seqid_t seed)
{
    if (seed < id.getseed()) {
        ostringstream os;
        os << "seed from log: " << seed <<
            " < id's seed: " << id.getseed();
        panic(os.str(), false);
    }
    id.setseed(seed);
}

/*!
 * \brief replay a file create
 * format: create/dir/<parentID>/name/<name>/id/<myID>{/ctime/<time>}
 */
static bool
replay_create(DETokenizer& c)
{
    fid_t parent, me;
    string myname;
    int status = 0;
    int16_t numReplicas;
    int64_t ctime;

    bool ok = pop_parent(parent, c);
    ok = pop_name(myname, "name", c, ok);
    ok = pop_fid(me, "id", c, ok);
    ok = pop_short(numReplicas, "numReplicas", c, ok);
    // if the log has the ctime, pass it thru
    const bool gottime = pop_time(ctime, "ctime", c, ok);
    chunkOff_t t = KFS_STRIPED_FILE_TYPE_NONE, n = 0, nr = 0, ss = 0;
    if (ok && gottime && pop_offset(t, "striperType", c, true)) {
        ok =    pop_offset(n,  "numStripes",         c, true) &&
            pop_offset(nr, "numRecoveryStripes", c, true) &&
            pop_offset(ss, "stripeSize",         c, true);
    }
    if (! ok) {
        return false;
    }
    fid_t todumpster = -1;
    if (! pop_fid(todumpster, "todumpster", c, ok)) {
        todumpster = -1;
    }
    kfsUid_t  user  = kKfsUserNone;
    kfsUid_t  group = kKfsGroupNone;
    kfsMode_t mode  = 0;
    int64_t   k     = user;
    if (pop_num(k, "user", c, ok)) {
        user = (kfsUid_t)k;
        if (user == kKfsUserNone) {
            return false;
        }
        k = group;
        if (! pop_num(k, "group", c, ok)) {
            return false;
        }
        group = (kfsGid_t)k;
        if (group == kKfsGroupNone) {
            return false;
        }
        k = mode;
        if (! pop_num(k, "mode", c, ok)) {
            return false;
        }
        mode = (kfsMode_t)k;
    } else {
        user  = gLayoutManager.GetDefaultLoadUser();
        group = gLayoutManager.GetDefaultLoadGroup();
        mode  = gLayoutManager.GetDefaultLoadFileMode();
    }
    if (user == kKfsUserNone || group == kKfsGroupNone ||
            mode == kKfsModeUndef) {
        return false;
    }
    // for all creates that were successful during normal operation,
    // when we replay it should work; so, exclusive = false
    MetaFattr* fa = 0;
    status = metatree.create(parent, myname, &me, numReplicas, false,
        t, n, nr, ss, todumpster, user, group, mode,
        kKfsUserRoot, kKfsGroupRoot, &fa);
    if (status == 0) {
        assert(fa);
        updateSeed(fileID, me);
        if (gottime) {
            fa->mtime = fa->ctime = fa->crtime = ctime;
            if (fa->IsStriped()) {
                fa->filesize = 0;
            }
        }
    }
    KFS_LOG_STREAM_DEBUG << "replay create:"
        " name: " << myname <<
        " id: "   << me <<
    KFS_LOG_EOM;
    return (status == 0);
}

/*!
 * \brief replay mkdir
 * format: mkdir/dir/<parentID>/name/<name>/id/<myID>{/ctime/<time>}
 */
static bool
replay_mkdir(DETokenizer& c)
{
    fid_t parent, me;
    string myname;
    int status = 0;
    int64_t ctime;

    bool ok = pop_parent(parent, c);
    ok = pop_name(myname, "name", c, ok);
    ok = pop_fid(me, "id", c, ok);
    if (! ok) {
        return false;
    }
    // if the log has the ctime, pass it thru
    const bool gottime = pop_time(ctime, "ctime", c, ok);
    kfsUid_t  user  = kKfsUserNone;
    kfsUid_t  group = kKfsGroupNone;
    kfsMode_t mode  = 0;
    int64_t   k     = user;
    if (pop_num(k, "user", c, ok)) {
        user = (kfsUid_t)k;
        if (user == kKfsUserNone) {
            return false;
        }
        k = group;
        if (! pop_num(k, "group", c, ok)) {
            return false;
        }
        group = (kfsGid_t)k;
        if (group == kKfsGroupNone) {
            return false;
        }
        k = mode;
        if (! pop_num(k, "mode", c, ok)) {
            return false;
        }
        mode = (kfsMode_t)k;
    } else {
        user  = gLayoutManager.GetDefaultLoadUser();
        group = gLayoutManager.GetDefaultLoadGroup();
        mode  = gLayoutManager.GetDefaultLoadFileMode();
    }
    if (user == kKfsUserNone || group == kKfsGroupNone ||
            mode == kKfsModeUndef) {
        return false;
    }
    MetaFattr* fa = 0;
    status = metatree.mkdir(parent, myname, user, group, mode,
        kKfsUserRoot, kKfsGroupRoot, &me, &fa);
    if (status == 0) {
        assert(fa);
        updateSeed(fileID, me);
        if (gottime) {
            fa->mtime = fa->ctime = fa->crtime = ctime;
        }
    }
    KFS_LOG_STREAM_DEBUG << "replay mkdir: "
        " name: " << myname <<
        " id: "   << me <<
    KFS_LOG_EOM;
    return (ok && status == 0);
}

/*!
 * \brief replay remove
 * format: remove/dir/<parentID>/name/<name>
 */
static bool
replay_remove(DETokenizer& c)
{
    fid_t parent;
    string myname;
    int status = 0;
    bool ok = pop_parent(parent, c);
    ok = pop_name(myname, "name", c, ok);
    fid_t todumpster = -1;
    if (! pop_fid(todumpster, "todumpster", c, ok))
        todumpster = -1;
    if (ok)
        status = metatree.remove(parent, myname, "", todumpster,
        kKfsUserRoot, kKfsGroupRoot);

    return (ok && status == 0);
}

/*!
 * \brief replay rmdir
 * format: rmdir/dir/<parentID>/name/<name>
 */
static bool
replay_rmdir(DETokenizer& c)
{
    fid_t parent;
    string myname;
    int status = 0;
    bool ok = pop_parent(parent, c);
    ok = pop_name(myname, "name", c, ok);
    if (ok)
        status = metatree.rmdir(parent, myname, "",
            kKfsUserRoot, kKfsGroupRoot);
    return (ok && status == 0);
}

/*!
 * \brief replay rename
 * format: rename/dir/<parentID>/old/<oldname>/new/<newpath>
 * NOTE: <oldname> is the name of file/dir in parent.  This
 * will never contain any slashes.
 * <newpath> is the full path of file/dir. This may contain slashes.
 * Since it is the last component, everything after new is <newpath>.
 * So, unlike <oldname> which just requires taking one element out,
 * we need to take everything after "new" for the <newpath>.
 *
 */
static bool
replay_rename(DETokenizer& c)
{
    fid_t parent;
    string oldname, newpath;
    int status = 0;
    bool ok = pop_parent(parent, c);
    ok = pop_name(oldname, "old", c, ok);
    ok = pop_path(newpath, "new", c, ok);
    fid_t todumpster = -1;
    if (! pop_fid(todumpster, "todumpster", c, ok))
        todumpster = -1;
    if (ok) {
        string oldpath;
        status = metatree.rename(parent, oldname, newpath, oldpath,
            true, todumpster, kKfsUserRoot, kKfsGroupRoot);
    }
    return (ok && status == 0);
}

/*!
 * \brief replay allocate
 * format: allocate/file/<fileID>/offset/<offset>/chunkId/<chunkID>/
 * chunkVersion/<chunkVersion>/{mtime/<time>}{/append/<1|0>}
 */
static bool
replay_allocate(DETokenizer& c)
{
    fid_t fid;
    chunkId_t cid, logChunkId;
    chunkOff_t offset, tmp = 0;
    seq_t chunkVersion, logChunkVersion;
    int status = 0;
    int64_t mtime;

    c.pop_front();
    bool ok = pop_fid(fid, "file", c, true);
    ok = pop_fid(offset, "offset", c, ok);
    ok = pop_fid(logChunkId, "chunkId", c, ok);
    ok = pop_fid(logChunkVersion, "chunkVersion", c, ok);
    // if the log has the mtime, pass it thru
    const bool gottime = pop_time(mtime, "mtime", c, ok);
    const bool append = pop_fid(tmp, "append", c, ok) && tmp != 0;

    // during normal operation, if a file that has a valid
    // lease is removed, we move the file to the dumpster and log it.
    // a subsequent allocation on that file will succeed.
    // the remove/allocation is recorded in the logs in that order.
    // during replay, we do the remove first and then we try to
    // replay allocation; for the allocation, we won't find
    // the file attributes.  we move on...when the chunkservers
    // that has the associated chunks for the file contacts us, we won't
    // find the fid and so those chunks will get nuked as stale.
    MetaFattr* const fa = metatree.getFattr(fid);
    if (! fa) {
        return ok;
    }
    if (ok) {
        // if the log has the mtime, set it up in the FA
        if (gottime) {
            fa->mtime = mtime;
        }
        cid = logChunkId;
        bool stripedFile = false;
        status = metatree.allocateChunkId(fid, offset, &cid,
                        &chunkVersion, NULL, &stripedFile);
        if (stripedFile && append) {
            return false; // append is not supported with striped files
        }
        const bool chunkExists = status == -EEXIST;
        if (chunkExists) {
            // allocates are particularly nasty: we can have
            // allocate requests that retrieve the info for an
            // existing chunk; since there is no tree mutation,
            // there is no way to turn off logging for the request
            // (the mutation field of a request is const).  so, if
            // we end up in a situation where what we get from the
            // log matches what is in the tree, ignore it and move
            // on
            if (cid != logChunkId) {
                return false;
            }
            if (chunkVersion == logChunkVersion) {
                return ok;
            }
            status = 0;
        }

        if (status == 0) {
            assert(cid == logChunkId);
            chunkVersion = logChunkVersion;
            status = metatree.assignChunkId(fid, offset,
                            cid, chunkVersion, 0, 0, append);
            if (status == 0) {
                fid_t cfid = 0;
                if (chunkExists &&
                        (! gLayoutManager.GetChunkFileId(
                            cid, cfid) ||
                        fid != cfid)) {
                    panic("missing chunk mapping", false);
                }
                // In case of append create begin make chunk stable entry,
                // if it doesn't already exist.
                if (append) {
                    gLayoutManager.ReplayPendingMakeStable(
                        cid, chunkVersion, -1, false, 0, true);
                }
                if (cid > chunkID.getseed()) {
                    // chunkID are handled by a two-stage
                    // allocation: the seed is updated in
                    // the first part of the allocation and
                    // the chunk is attached to the file
                    // after the chunkservers have ack'ed
                    // the allocation.  We can have a run
                    // where: (1) the seed is updated, (2)
                    // a checkpoint is taken, (3) allocation
                    // is done and written to log file.  If
                    // we crash, then the cid in log < seed in ckpt.
                    updateSeed(chunkID, cid);
                }
            }
            // assign updates the mtime; so, set it to what is in
            // the log
            if (gottime) {
                fa->mtime = mtime;
            }
        }
    }
    return (ok && status == 0);
}

/*!
 * \brief replay coalesce (do the cleanup/accounting actions)
 * format: coalesce/old/<srcFid>/new/<dstFid>/count/<# of blocks coalesced>
 */
static bool
replay_coalesce(DETokenizer& c)
{
    fid_t srcFid, dstFid;
    size_t count;
    int64_t mtime;

    c.pop_front();
    bool ok = pop_fid(srcFid, "old", c, true);
    ok = pop_fid(dstFid, "new", c, ok);
    ok = pop_size(count, "count", c, ok);
    const bool gottime = pop_time(mtime, "mtime", c, ok);
    fid_t      retSrcFid      = -1;
    fid_t      retDstFid      = -1;
    chunkOff_t dstStartOffset = -1;
    size_t     numChunksMoved = 0;
    ok = ok && metatree.coalesceBlocks(
        metatree.getFattr(srcFid), metatree.getFattr(dstFid),
        retSrcFid, retDstFid, dstStartOffset,
        gottime ? &mtime : 0, numChunksMoved,
        kKfsUserRoot, kKfsGroupRoot) == 0;
    return (
        ok &&
        retSrcFid == srcFid && retDstFid == dstFid &&
        numChunksMoved == count
    );
}


/*!
 * \brief replay truncate
 * format: truncate/file/<fileID>/offset/<offset>{/mtime/<time>}
 */
static bool
replay_truncate(DETokenizer& c)
{
    fid_t fid;
    chunkOff_t offset;
    chunkOff_t endOffset;
    int status = 0;
    int64_t mtime;

    c.pop_front();
    bool ok = pop_fid(fid, "file", c, true);
    ok = pop_offset(offset, "offset", c, ok);
    // if the log has the mtime, pass it thru
    const bool gottime = pop_time(mtime, "mtime", c, ok);
    if (! gottime || ! pop_offset(endOffset, "endoff", c, ok)) {
        endOffset = -1;
    }
    if (ok) {
        const bool kSetEofHintFlag = true;
        status = metatree.truncate(fid, offset,
            gottime ? &mtime : 0,
            kKfsUserRoot, kKfsGroupRoot, endOffset, kSetEofHintFlag);
    }
    return (ok && status == 0);
}

/*!
 * \brief replay prune blks from head of file
 * format: pruneFromHead/file/<fileID>/offset/<offset>{/mtime/<time>}
 */
static bool
replay_pruneFromHead(DETokenizer& c)
{
    fid_t fid;
    chunkOff_t offset;
    int status = 0;
    int64_t mtime;

    c.pop_front();
    bool ok = pop_fid(fid, "file", c, true);
    ok = pop_fid(offset, "offset", c, ok);
    // if the log has the mtime, pass it thru
    bool gottime = pop_time(mtime, "mtime", c, ok);
    if (ok) {
        status = metatree.pruneFromHead(fid, offset,
            gottime ? &mtime : 0);
    }
    return (ok && status == 0);
}

/*!
 * \brief replay size
 * format: size/file/<fileID>/filesize/<filesize>
 */
static bool
replay_size(DETokenizer& c)
{
    fid_t fid;
    chunkOff_t filesize;

    c.pop_front();
    bool ok = pop_fid(fid, "file", c, true);
    ok = pop_offset(filesize, "filesize", c, ok);
    if (ok) {
        MetaFattr* const fa = metatree.getFattr(fid);
        if (fa) {
            if (filesize >= 0) {
                metatree.setFileSize(fa, filesize);
            } else {
                metatree.setFileSize(fa, 0);
                metatree.invalidateFileSize(fa);
            }
        }
    }
    return true;
}

/*!
 * Replay a change file replication RPC.
 * format: setrep/file/<fid>/replicas/<#>
 */

static bool
replay_setrep(DETokenizer& c)
{
    fid_t fid;
    int16_t numReplicas;

    c.pop_front();
    bool ok = pop_fid(fid, "file", c, true);
    ok = pop_short(numReplicas, "replicas", c, ok);
    if (ok) {
        metatree.changePathReplication(fid, numReplicas);
    }
    return ok;
}

/*!
 * \brief replay setmtime
 * format: setmtime/file/<fileID>/mtime/<time>
 */
static bool
replay_setmtime(DETokenizer& c)
{
    fid_t fid;
    int64_t mtime;

    c.pop_front();
    bool ok = pop_fid(fid, "file", c, true);
    ok = pop_time(mtime, "mtime", c, ok);
    if (ok) {
        MetaFattr *fa = metatree.getFattr(fid);
        // If the fa isn't there that isn't fatal.
        if (fa != NULL)
            fa->mtime = mtime;
    }
    return ok;
}

static int sRestoreTimeCount = 0;
/*!
 * \brief restore time
 * format: time/<time>
 */
static bool
restore_time(DETokenizer& c)
{
    c.pop_front();
    KFS_LOG_STREAM_INFO << "log time: " << c.front() << KFS_LOG_EOM;
    sRestoreTimeCount++;
    return true;
}

/*!
 * \brief restore make chunk stable
 * format:
 * "mkstable{done}/fileId/" << fid <<
 * "/chunkId/"        << chunkId <<
 * "/chunkVersion/"   << chunkVersion  <<
 * "/size/"           << chunkSize <<
 * "/checksum/"       << chunkChecksum <<
 * "/hasChecksum/"    << (hasChunkChecksum ? 1 : 0)
 */
static bool
restore_makechunkstable(DETokenizer& c, bool addFlag)
{
    fid_t      fid;
    chunkId_t  chunkId;
    seq_t      chunkVersion;
    chunkOff_t chunkSize;
    string     str;
    fid_t      tmp;
    uint32_t   checksum;
    bool       hasChecksum;

    c.pop_front();
    bool ok = pop_fid(fid, "fileId", c, true);
    ok = pop_fid(chunkId, "chunkId", c, ok);
    ok = pop_fid(chunkVersion, "chunkVersion", c, ok);
    int64_t num = -1;
    ok = pop_num(num, "size", c, ok);
    chunkSize = chunkOff_t(num);
    ok = pop_fid(tmp, "checksum", c, ok);
    checksum = (uint32_t)tmp;
    ok = pop_fid(tmp, "hasChecksum", c, ok);
    hasChecksum = tmp != 0;
    if (!ok) {
        KFS_LOG_STREAM_ERROR << "ignore log line for mkstable <"
            << fid << ',' << chunkId << ',' << chunkVersion
            << ">" <<
        KFS_LOG_EOM;
        return true;
    }
    if (ok) {
        gLayoutManager.ReplayPendingMakeStable(
            chunkId, chunkVersion, chunkSize,
            hasChecksum, checksum, addFlag);
    }
    return ok;
}

static bool
restore_mkstable(DETokenizer& c)
{
    return restore_makechunkstable(c, true);
}

static bool
restore_mkstabledone(DETokenizer& c)
{
    return restore_makechunkstable(c, false);
}


static bool
replay_beginchunkversionchange(DETokenizer& c)
{
    fid_t     fid;
    chunkId_t chunkId;
    seq_t     chunkVersion;

    c.pop_front();
    bool ok = pop_fid(fid,          "file",         c, true);
    ok = pop_fid     (chunkId,      "chunkId",      c, ok);
    ok = pop_fid     (chunkVersion, "chunkVersion", c, ok);

    if (! ok) {
        return false;
    }
    if (! metatree.getFattr(fid)) {
        // Ignore files that no longer exists.
        // The same logic as in chunk allocation replay applies.
        return true;
    }
    return gLayoutManager.ReplayBeginChangeChunkVersion(
        fid, chunkId, chunkVersion);
}

/*
  Roll file id and chunk id seeds after meta data loss. This entry must be the
  last entry in the current log segment, the log segment that was open for
  append. If no such segment exists it can be created by copying the first 4
  lines of any other log segment.
  Usually the log segments have the following entry:
  setintbase/16
  therefore the number should be in hex, the default -- 0 should be reasonable
  for the most occasions.
  Example:
  [460]mtv1% ls -ltr kfslog/ | tail -n 2
  -rw-r--r-- 2 mike users     227 Jan 22 22:11 last
  -rw-r--r-- 1 mike users      88 Jan 25 16:52 log.660
  [461]mtv1% tail kfslog/log.660
  version/1
  checksum/last-line
  setintbase/16
  time/2012-01-23T06:11:03.683655Z
  [462]mtv1% echo rollseeds/0 >> kfslog/log.660
*/
static bool
restore_rollseeds(DETokenizer& c)
{
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    int64_t roll = c.toNumber();
    if (roll == 0) {
        roll = 2000000; // Default 2M
    }
    if (roll < 0 ||
            chunkID.getseed() + roll < chunkID.getseed() ||
            fileID.getseed() + roll < fileID.getseed()) {
        KFS_LOG_STREAM_ERROR <<
            "invalid seed roll value: " << roll <<
        KFS_LOG_EOM;
        return false;
    }
    chunkID.setseed(chunkID.getseed() + roll);
    fileID.setseed(fileID.getseed() + roll);
    return true;
}

static bool
replay_chmod(DETokenizer& c)
{
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    fid_t fid = -1;
    if (! pop_fid(fid, "file", c, true)) {
        return false;
    }
    int64_t n = 0;
    if (! pop_num(n, "mode", c, true)){
        return false;
    }
    const kfsMode_t mode = (kfsMode_t)n;
    MetaFattr* const fa = metatree.getFattr(fid);
    if (! fa) {
        return false;
    }
    fa->mode = mode;
    return true;
}

static bool
replay_chown(DETokenizer& c)
{
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    fid_t fid = -1;
    if (! pop_fid(fid, "file", c, true)) {
        return false;
    }
    int64_t n = kKfsUserNone;
    if (! pop_num(n, "user", c, true)) {
        return false;
    }
    const kfsUid_t user = (kfsUid_t)n;
    n = kKfsGroupNone;
    if (! pop_num(n, "group", c, true)) {
        return false;
    }
    const kfsGid_t group = (kfsGid_t)n;
    if (user == kKfsUserNone && group == kKfsGroupNone) {
        return false;
    }
    MetaFattr* const fa = metatree.getFattr(fid);
    if (! fa) {
        return false;
    }
    if (user != kKfsUserNone) {
        fa->user = user;
    }
    if (group != kKfsGroupNone) {
        fa->user = group;
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
    e.add_parser("version", replay_version);
    e.add_parser("create", replay_create);
    e.add_parser("mkdir", replay_mkdir);
    e.add_parser("remove", replay_remove);
    e.add_parser("rmdir", replay_rmdir);
    e.add_parser("rename", replay_rename);
    e.add_parser("allocate", replay_allocate);
    e.add_parser("truncate", replay_truncate);
    e.add_parser("coalesce", replay_coalesce);
    e.add_parser("pruneFromHead", replay_pruneFromHead);
    e.add_parser("setrep", replay_setrep);
    e.add_parser("size", replay_size);
    e.add_parser("setmtime", replay_setmtime);
    e.add_parser("chunkVersionInc", restore_chunkVersionInc);
    e.add_parser("time", restore_time);
    e.add_parser("mkstable", restore_mkstable);
    e.add_parser("mkstabledone", restore_mkstabledone);
    e.add_parser("beginchunkversionchange", replay_beginchunkversionchange);
    e.add_parser("checksum", restore_checksum);
    e.add_parser("rollseeds", restore_rollseeds);
    e.add_parser("chmod", replay_chmod);
    e.add_parser("chown", replay_chown);
    initied = true;
    return e;
}

/*!
 * \brief replay contents of log file
 * \return  zero if replay successful, negative otherwise
 */
int
Replay::playlog(bool& lastEntryChecksumFlag)
{
    restoreChecksum.clear();
    lastLineChecksumFlag = false;
    lastEntryChecksumFlag = false;
    MdStream& mds = oplog.getMdStream();
    mds.Reset();
    mds.SetWriteTrough(true);

    if (! file.is_open()) {
        //!< no log...so, reset the # to 0.
        number = 0;
        return 0;
    }

    DiskEntry& entrymap = get_entry_map();
    DETokenizer tokenizer(file);

    seq_t opcount = oplog.checkpointed();
    int status = 0;
    while (tokenizer.next(&mds)) {
        if (! entrymap.parse(tokenizer)) {
            KFS_LOG_STREAM_FATAL <<
                "error " << path <<
                ":" << tokenizer.getEntryCount() <<
                ":" << tokenizer.getEntry() <<
            KFS_LOG_EOM;
            status = -EINVAL;
            break;
        }
        lastEntryChecksumFlag = ! restoreChecksum.empty();
        if (lastEntryChecksumFlag) {
            const string md = mds.GetMd();
            if (md != restoreChecksum) {
                KFS_LOG_STREAM_FATAL <<
                    "error " << path <<
                    ":" << tokenizer.getEntryCount() <<
                    ":" << tokenizer.getEntry() <<
                    ": checksum mismatch:"
                    " expectd:" << restoreChecksum <<
                    " computed: " << md <<
                KFS_LOG_EOM;
                status = -EINVAL;
                break;
            }
            restoreChecksum.clear();
        }
    }
    opcount += tokenizer.getEntryCount();
    oplog.set_seqno(opcount);
    if (status == 0 && ! file.eof()) {
        KFS_LOG_STREAM_FATAL <<
            "error " << path <<
            ":" << tokenizer.getEntryCount() <<
            ":" << tokenizer.getEntry() <<
        KFS_LOG_EOM;
        status = -EIO;
    }
    if (status == 0) {
        lastLogIntBase = tokenizer.getIntBase();
    }
    file.close();
    return status;
}

/*!
 * \brief replay contents of all log files since CP
 * \return  zero if replay successful, negative otherwise
 */
int
Replay::playLogs(bool includeLastLogFlag)
{
    if (number < 0) {
        //!< no log...so, reset the # to 0.
        number = 0;
        appendToLastLogFlag = false;
        oplog.setLog(number);
        return 0;
    }
    int       last   = -1;
    const int status = getLastLog(last);
    return (status == 0 ? playLogs(last, includeLastLogFlag) : status);
}

int
Replay::playLogs(int last, bool includeLastLogFlag)
{
    int status = 0;
    appendToLastLogFlag        = false;
    lastLineChecksumFlag       = false;
    lastLogIntBase             = -1;
    bool lastEntryChecksumFlag = false;
    int i;
    for (i = number; ; i++) {
        if (! includeLastLogFlag && last < i) {
            break;
        }
        const string logfn = oplog.logfile(i);
        if (last < i && ! file_exists(logfn)) {
            if (! appendToLastLogFlag && number < i) {
                number = i;
            }
            break;
        }
        sRestoreTimeCount = 0;
        if ((status = openlog(logfn)) != 0 ||
                (status = playlog(lastEntryChecksumFlag)) != 0) {
            break;
        }
        if (sRestoreTimeCount <= 0) {
            // "time/" is the last line of the header.
            // Each valid log even last partial must have
            // complete header.
            KFS_LOG_STREAM_FATAL <<
                logfn <<
                ": missing \"time\" line" <<
            KFS_LOG_EOM;
            status = -EINVAL;
            break;
        }
        if (lastLineChecksumFlag &&
                (! lastEntryChecksumFlag && (i <= last ||
                file_exists(oplog.logfile(i + 1))))) {
            KFS_LOG_STREAM_FATAL <<
                logfn <<
                ": missing last line checksum" <<
            KFS_LOG_EOM;
            status = -EINVAL;
            break;
        }
        if (last < i && ! lastEntryChecksumFlag) {
            appendToLastLogFlag = true;
        }
    }
    if (status == 0) {
        oplog.setLog(i);
        } else {
        appendToLastLogFlag = false;
    }
    return status;
}

int
Replay::getLastLog(int& last)
{
    last = number;
    if (last < 0) {
        // no logs, to replay.
        return 0;
    }
    // Get last complete log number. All log files before and including this
    // won't ever be written to again.
    // Get the inode # for the last file
    struct stat lastst = {0};
    if (stat(LASTLOG.c_str(), &lastst)) {
        const int err = errno;
        if (last == 0 && ! file_exists(oplog.logfile(last + 1)) &&
                ! file_exists(oplog.logfile(last + 2))) {
            last = -1;
            return 0; // Initial empty checkpoint and log.
        }
        KFS_LOG_STREAM_FATAL <<
            LASTLOG <<
            ": " << QCUtils::SysError(err) <<
        KFS_LOG_EOM;
        return (err > 0 ? -err : (err == 0 ? -1 : err));
    }
    if (last > 0 && file_exists(oplog.logfile(last - 1))) {
        // Start search from the previous, as checkpoint might
        // point to the current one.
        last--;
    }
    for ( ; ; last++) {
        const string logfn = oplog.logfile(last);
        struct stat st = {0};
        if (stat(logfn.c_str(), &st)) {
            const int err = errno;
            KFS_LOG_STREAM_FATAL <<
                logfn <<
                ": " << QCUtils::SysError(err) <<
            KFS_LOG_EOM;
            return (err > 0 ? -err : (err == 0 ? -1 : err));
        }
        if (st.st_ino == lastst.st_ino) {
            break;
        }
    }
    return 0;
}

} // namespace KFS
