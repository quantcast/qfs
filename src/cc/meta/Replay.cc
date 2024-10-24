/*!
 * $Id$
 *
 * \file Replay.cc
 * \brief transaction log replay
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

#include "Replay.h"
#include "LogWriter.h"
#include "Restorer.h"
#include "util.h"
#include "DiskEntry.h"
#include "kfstree.h"
#include "LayoutManager.h"
#include "MetaVrSM.h"
#include "MetaVrOps.h"
#include "MetaDataStore.h"

#include "common/MdStream.h"
#include "common/MsgLogger.h"
#include "common/StdAllocator.h"
#include "common/kfserrno.h"
#include "common/RequestParser.h"
#include "common/juliantime.h"
#include "common/StBuffer.h"

#include "kfsio/checksum.h"

#include "qcdio/QCUtils.h"

#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

#include <cassert>
#include <cstdlib>
#include <sstream>
#include <iomanip>
#include <deque>
#include <set>

namespace KFS
{
using std::ostringstream;
using std::deque;
using std::set;
using std::less;
using std::hex;
using std::dec;
using std::streamoff;

inline void
Replay::setRollSeeds(int64_t roll)
{
    rollSeeds = roll;
}

class ReplayState
{
public:
    // Commit entry other than "op" are intended to debugging error checksum
    // mismatches, after series of ops are committed.
    class CommitQueueEntry
    {
    public:
        CommitQueueEntry(
            const MetaVrLogSeq& ls = MetaVrLogSeq(),
            int                 s  = 0,
            fid_t               fs = -1,
            int64_t             ek = 0,
            MetaRequest*        o  = 0)
            : logSeq(ls),
              seed(fs),
              errChecksum(ek),
              status(s),
              op(o)
            {}
        MetaVrLogSeq logSeq;
        fid_t        seed;
        int64_t      errChecksum;
        int          status;
        MetaRequest* op;
    };
    typedef deque<
        CommitQueueEntry
    > CommitQueue;
    class EnterAndLeave;
    friend class EnterAndLeave;

    ReplayState(
        Replay* replay,
        bool*   enqueueFlagPtr)
        : mCommitQueue(),
          mCheckpointCommitted(0, 0, 0),
          mLastNonLogCommit(),
          mCheckpointFileIdSeed(0),
          mCheckpointErrChksum(0),
          mLastCommittedSeq(0, 0, 0),
          mLastBlockCommittedSeq(0, 0, 0),
          mViewStartSeq(0, 0, 0),
          mBlockStartLogSeq(0, 0, 0),
          mLastBlockSeq(-1),
          mLastLogAheadSeq(0, 0, 0),
          mLastNonEmptyViewEndSeq(0, 0, 0),
          mLogAheadErrChksum(0),
          mSubEntryCount(0),
          mLogSegmentTimeUsec(0),
          mLastBlockSeed(0),
          mLastBlockStatus(0),
          mLastBlockErrChecksum(0),
          mLastCommittedStatus(0),
          mRestoreTimeCount(0),
          mUpdateLogWriterFlag(false),
          mReplayer(replay),
          mEnqueueFlagFlagPtr(enqueueFlagPtr),
          mCurOp(0),
          mRecursionCount(0)
    {
        if (! mEnqueueFlagFlagPtr && mReplayer) {
            panic("invalid replay stae arguments");
        }
    }
    ~ReplayState()
    {
        if (mCurOp) {
            mCurOp->replayFlag = false;
            MetaRequest::Release(mCurOp);
        }
        for (CommitQueue::iterator it = mCommitQueue.begin();
                mCommitQueue.end() != it;
                ++it) {
            if (it->op) {
                it->op->replayFlag = false;
                MetaRequest::Release(it->op);
                it->op = 0;
            }
        }
    }
    bool runCommitQueue(
        const MetaVrLogSeq& logSeq,
        seq_t               seed,
        int64_t             status,
        int64_t             errChecksum);
    bool incSeq()
    {
        if (0 != mSubEntryCount) {
            return false;
        }
        curOpDone();
        mLastLogAheadSeq.mLogSeq++;
        if (mLastLogAheadSeq.IsPastViewStart()) {
            mLastNonEmptyViewEndSeq = mLastLogAheadSeq;
        }
        return true;
    }
    bool subEntry()
    {
        if (--mSubEntryCount <= 0) {
            return incSeq();
        }
        return true;
    }
    inline static ReplayState& get(const DETokenizer& c);
    bool handle()
    {
        if (! mCurOp) {
            panic("replay: no current op");
            return false;
        }
        if (mReplayer) {
            if (META_VR_LOG_START_VIEW == mCurOp->op) {
                handleOp(*mCurOp);
                if (mCurOp) {
                    const bool ok = 0 == mCurOp->status;
                    curOpDone();
                    return ok;
                }
            } else {
                mCommitQueue.push_back(ReplayState::CommitQueueEntry(
                    mCurOp->logseq.IsValid() ?
                        mCurOp->logseq : mLastLogAheadSeq, 0, 0, 0, mCurOp));
                mCurOp = 0;
                *mEnqueueFlagFlagPtr = true;
            }
        } else {
            handleOp(*mCurOp);
        }
        return true;
    }
    bool isCurOpLogSeqValid() const
    {
        if (! mReplayer || ! mCurOp) {
            return true;
        }
        if (META_VR_LOG_START_VIEW == mCurOp->op) {
            return true;
        }
        MetaVrLogSeq next = mLastLogAheadSeq;
        next.mLogSeq++;
        if (next == mCurOp->logseq) {
            return true;
        }
        KFS_LOG_STREAM_ERROR <<
            "replay logseq mismatch:"
            " expected: "  << next <<
            " actual: "    << mCurOp->logseq <<
            " "            << mCurOp->Show() <<
        KFS_LOG_EOM;
        return false;
    }
    void replayCurOp()
    {
        if (! mCurOp || 1 != mSubEntryCount) {
            panic("invalid replay current op invocation");
            return;
        }
        if (! isCurOpLogSeqValid()) {
            panic("invalid current op log sequence");
            return;
        }
        handle();
    }
    void commit(CommitQueueEntry& entry)
    {
        if (! entry.op) {
            return;
        }
        MetaRequest& op                  = *entry.op;
        bool         updateCommittedFlag = op.logseq.IsValid();
        if (! updateCommittedFlag && op.replayFlag) {
            panic("replay: commit invalid op log sequence");
            return;
        }
        if (! op.replayFlag) {
            update();
        }
        if (updateCommittedFlag && op.logseq <= mViewStartSeq) {
            // Set failure status for the remaining ops from the previous view.
            // The op is effectively canceled.
            updateCommittedFlag = false;
            op.status = -EVRNOTPRIMARY;
            if (op.replayFlag) {
                op.statusMsg = "primary node might have changed";
            }
            // No effect on error checksum, set etnry status to 0.
            entry.status = 0;
            // Invalidate the log sequence, as the start view has effectively
            // deleted the op from the log.
            op.logseq = MetaVrLogSeq();
            if (! op.replayFlag) {
                handleOp(op);
            }
        } else {
            handleOp(op);
        }
        if (updateCommittedFlag && META_VR_LOG_START_VIEW != op.op) {
            const int status = op.status < 0 ? SysToKfsErrno(-op.status) : 0;
            mLastCommittedSeq    = op.logseq;
            mLastCommittedStatus = status;
            mLogAheadErrChksum  += status;
            entry.status = status;
        }
        entry.logSeq      = op.logseq;
        entry.seed        = fileID.getseed();
        entry.errChecksum = mLogAheadErrChksum;
        entry.op          = 0;
        if (&op != mCurOp && (! op.suspended || ! op.replayFlag)) {
            opDone(op);
        }
    }
    void handleOp(MetaRequest& op)
    {
        if (op.replayFlag) {
            op.seqno = MetaRequest::GetLogWriter().GetNextSeq();
            op.handle();
        } else {
            if (! op.SubmitBegin()) {
                panic("replay: invalid submit begin status");
            }
        }
        KFS_LOG_STREAM_DEBUG <<
            (mReplayer ? (op.replayFlag ? "replay:" : "commit") : "handle:") <<
            " logseq: " << op.logseq <<
            " x "       << hex << op.logseq << dec <<
            " status: " << op.status <<
            " "         << op.statusMsg <<
            " "         << op.Show() <<
        KFS_LOG_EOM;
        if (op.suspended && &op == mCurOp) {
            mCurOp = 0;
        }
    }
    bool commitAll()
    {
        if (0 != mSubEntryCount || ! mReplayer) {
            return false;
        }
        if (mCommitQueue.empty()) {
            return true;
        }
        for (CommitQueue::iterator it = mCommitQueue.begin();
                mCommitQueue.end() != it;
                ++it) {
            commit(*it);
        }
        mCommitQueue.clear();
        mBlockStartLogSeq = mLastCommittedSeq;
        return true;
    }
    void getReplayCommitQueue(
       Replay::CommitQueue& queue) const
    {
        queue.clear();
        queue.reserve(mCommitQueue.size());
        for (CommitQueue::const_iterator it = mCommitQueue.begin();
                mCommitQueue.end() != it;
                ++it) {
            if (! it->op || ! it->op->logseq.IsValid() || ! it->op->replayFlag ||
                    0 != it->op->status) {
                continue;
            }
            queue.push_back(it->op);
        }
    }
    void curOpDone()
    {
        if (! mCurOp) {
            return;
        }
        MetaRequest& op = *mCurOp;
        mCurOp = 0;
        opDone(op);
    }
    void opDone(MetaRequest& op)
    {
        if (op.replayFlag) {
            op.replayFlag = false;
            MetaRequest::Release(&op);
        } else {
            op.SubmitEnd();
        }
    }
    void handleStartView(MetaVrLogStartView& op)
    {
        if (! mReplayer) {
            panic("replay: invalid start view invocation");
            return;
        }
        if (0 != op.status) {
            KFS_LOG_STREAM_ERROR <<
                " status: " << op.status <<
                " "         << op.statusMsg <<
                " logseq: " << op.logseq <<
                " "         << op.Show() <<
            KFS_LOG_EOM;
            return;
        }
        if (! op.Validate()) {
            op.status    = -EINVAL;
            op.statusMsg = "invalid log start view op";
            return;
        }
        CommitQueue::iterator it = mCommitQueue.begin();
        while (mCommitQueue.end() != it && it->logSeq <= op.mCommittedSeq) {
            commit(*it);
            ++it;
        }
        mCommitQueue.erase(mCommitQueue.begin(), it);
        if (op.mCommittedSeq != mLastCommittedSeq) {
            op.status    = -EINVAL;
            op.statusMsg = "invalid committed sequence";
            return;
        }
        mViewStartSeq      = op.mNewLogSeq;
        mLastLogAheadSeq   = op.mNewLogSeq;
        mBlockStartLogSeq  = op.mNewLogSeq;
        mBlockStartLogSeq.mLogSeq--;
        if (! runCommitQueue(
                op.mCommittedSeq,
                fileID.getseed(),
                mLastCommittedStatus,
                mLogAheadErrChksum) ||
                    op.mCommittedSeq != mLastCommittedSeq ||
                    ! mCommitQueue.empty()) {
            op.status    = -EINVAL;
            op.statusMsg = "run commit queue failure";
            return;
        }
    }
    void setReplayState(
        const MetaVrLogSeq& committed,
        const MetaVrLogSeq& viewStartSeq,
        seq_t               seed,
        int                 status,
        int64_t             errChecksum,
        MetaRequest*        commitQueue,
        const MetaVrLogSeq& lastBlockCommitted,
        fid_t               lastBlockSeed,
        int                 lastBlockStatus,
        int64_t             lastBlockErrChecksum,
        const MetaVrLogSeq& lastNonEmptyViewEndSeq)
    {
        if (mCurOp || ! mReplayer || ! committed.IsValid() ||
                (commitQueue && commitQueue->logseq.IsValid() &&
                    commitQueue->logseq <= committed)) {
            panic("replay: set replay state invalid arguments");
            return;
        }
        if (! mCommitQueue.empty() && ! runCommitQueue(
                committed,
                seed,
                status,
                errChecksum)) {
            panic("replay: set replay state run commit queue failure");
            return;
        }
        if (mBlockStartLogSeq < committed) {
            mBlockStartLogSeq = committed;
        }
        if (mLastLogAheadSeq < committed) {
            mLastLogAheadSeq = committed;
        }
        if (mViewStartSeq < viewStartSeq) {
            mViewStartSeq = viewStartSeq;
        }
        mLastBlockCommittedSeq  = lastBlockCommitted;
        mLastBlockSeed          = lastBlockSeed;
        mLastBlockStatus        = lastBlockStatus,
        mLastBlockErrChecksum   = lastBlockErrChecksum;
        mLastCommittedSeq       = committed;
        mLastCommittedStatus    = status;
        mLastNonEmptyViewEndSeq = lastNonEmptyViewEndSeq;
        mLogAheadErrChksum      = errChecksum;
        mSubEntryCount          = 0;
        MetaRequest* next = commitQueue;
        while (next) {
            MetaRequest& op = *next;
            next = op.next;
            op.next = 0;
            if (! enqueueSelf(op)) {
                submit_request(&op);
            }
        }
        if (! runCommitQueue(
                mLastCommittedSeq,
                fileID.getseed(),
                mLastCommittedStatus,
                mLogAheadErrChksum)) {
            panic("replay: set replay state run commit queue failure");
        }
    }
    bool enqueue(MetaRequest& req)
    {
        if (! mReplayer) {
            panic("replay: invalid enqueue attempt");
            return false;
        }
        if (mCommitQueue.empty()) {
            return false;
        }
        return enqueueSelf(req);
    }
    bool enqueueSelf(MetaRequest& req)
    {
        if (mCurOp) {
            panic("replay: invalid enqueue attempt: has pending op");
            return false;
        }
        if (req.replayBypassFlag || IsMetaLogWriteOrVrError(req.status)) {
            return false;
        }
        mCurOp = &req;
        MetaVrLogSeq const nextSeq = mCurOp->logseq;
        if (nextSeq.IsValid() && ! isCurOpLogSeqValid()) {
            panic("replay: invalid enqueue log sequence");
        }
        if (handle()) {
            if (mLastLogAheadSeq < nextSeq) {
                mLastLogAheadSeq = nextSeq;
            }
        } else {
            panic("replay: enqueue: invalid handle completion");
            return false;
        }
        return true;
    }
    void update()
    {
        if (mUpdateLogWriterFlag) {
            *mEnqueueFlagFlagPtr = ! mCommitQueue.empty();
            MetaRequest::GetLogWriter().SetCommitted(
                mLastCommittedSeq,
                mLogAheadErrChksum,
                fileID.getseed(),
                mLastCommittedStatus,
                mLastLogAheadSeq,
                mViewStartSeq
            );
        }
    }

    CommitQueue   mCommitQueue;
    MetaVrLogSeq  mCheckpointCommitted;
    MetaVrLogSeq  mLastNonLogCommit;
    fid_t         mCheckpointFileIdSeed;
    int64_t       mCheckpointErrChksum;
    MetaVrLogSeq  mLastCommittedSeq;
    MetaVrLogSeq  mLastBlockCommittedSeq;
    MetaVrLogSeq  mViewStartSeq;
    MetaVrLogSeq  mBlockStartLogSeq;
    seq_t         mLastBlockSeq;
    MetaVrLogSeq  mLastLogAheadSeq;
    MetaVrLogSeq  mLastNonEmptyViewEndSeq;
    int64_t       mLogAheadErrChksum;
    int64_t       mSubEntryCount;
    int64_t       mLogSegmentTimeUsec;
    fid_t         mLastBlockSeed;
    int           mLastBlockStatus;
    int64_t       mLastBlockErrChecksum;
    int           mLastCommittedStatus;
    int           mRestoreTimeCount;
    bool          mUpdateLogWriterFlag;
    Replay* const mReplayer;
    bool* const   mEnqueueFlagFlagPtr;
    MetaRequest*  mCurOp;
private:
    int           mRecursionCount;
private:
    ReplayState(const ReplayState&);
    ReplayState& operator=(const ReplayState&);
};

class ReplayState::EnterAndLeave
{
public:
    EnterAndLeave(ReplayState& state, int targetCnt = 0)
        : mState(state),
          mTargetCount(targetCnt)
    {
        if (mTargetCount != mState.mRecursionCount) {
            panic("replay: invalid recursion");
        }
        mState.mRecursionCount++;
    }
    ~EnterAndLeave()
    {
        if (mTargetCount + 1 != mState.mRecursionCount) {
            panic("replay: invalid recursion count");
        }
        mState.mRecursionCount--;
        mState.update();
    }
private:
    ReplayState& mState;
    const int    mTargetCount;
private:
    EnterAndLeave(const EnterAndLeave&);
    EnterAndLeave& operator=(const EnterAndLeave&);
};

class Replay::State : public ReplayState
{
public:
    State(
        Replay* replay,
        bool*   flag)
        : ReplayState(replay, flag)
        {}
    static State& get(const DETokenizer& c)
        { return *reinterpret_cast<State*>(c.getUserData()); }
};

    /* static */ inline
ReplayState& ReplayState::get(const DETokenizer& c)
    { return Replay::State::get(c); }

static bool
parse_vr_log_seq(DETokenizer& c, MetaVrLogSeq& outSeq)
{
    if (c.empty()) {
        return false;
    }
    const char* cur = c.front().ptr;
    return (
        16 == c.getIntBase() ?
        outSeq.Parse<HexIntParser>(cur, c.front().len) :
        outSeq.Parse<DecIntParser>(cur, c.front().len)
    );
}

/*!
 * \brief open saved log file for replay
 * \param[in] p a path in the form "<logdir>/log.<number>"
 */
int
Replay::openlog(const string& name)
{
    if (file.is_open()) {
        file.close();
    }
    string::size_type pos = name.rfind('/');
    if (string::npos == pos) {
        pos = 0;
    } else {
        pos++;
    }
    if (logdir.empty()) {
        tmplogname.assign(name.data(), name.size());
    } else {
        tmplogname.assign(logdir.data(), logdir.size());
        tmplogname += name.data() + pos;
        pos = logdir.size();
    }
    KFS_LOG_STREAM_INFO <<
        "open log file: " << name << " => " << tmplogname <<
    KFS_LOG_EOM;
    int64_t                 num = -1;
    const string::size_type dot = tmplogname.rfind('.');
    if (string::npos == dot || dot < pos ||
            (num = toNumber(tmplogname.c_str() + dot + 1)) < 0) {
        KFS_LOG_STREAM_FATAL <<
            tmplogname << ": invalid log file name" <<
        KFS_LOG_EOM;
        tmplogname.clear();
        return -EINVAL;
    }
    file.open(tmplogname.c_str(), ifstream::in | ifstream::binary);
    if (file.fail()) {
        const int err = errno;
        KFS_LOG_STREAM_FATAL <<
            tmplogname << ": " << QCUtils::SysError(err) <<
        KFS_LOG_EOM;
        tmplogname.clear();
        return (err > 0 ? -err : (err == 0 ? -1 : err));
    }
    number = num;
    path.assign(tmplogname.data(), tmplogname.size());
    tmplogname.clear();
    return 0;
}

void
Replay::setLogDir(const char* dir)
{
    if (dir && *dir) {
        logdir = dir;
        if ('/' != *logdir.rbegin()) {
            logdir += '/';
        }
    } else {
        logdir.clear();
    }
}

const string&
Replay::logfile(seq_t num)
{
    if (tmplogname.empty()) {
        string::size_type name = path.rfind('/');
        if (string::npos == name) {
            name = 0;
        } else {
            name++;
        }
        const string::size_type dot = path.find('.', name);
        if (dot == string::npos) {
            tmplogname = path + ".";
        } else {
            tmplogname = path.substr(0, dot + 1);
        }
        tmplogprefixlen = tmplogname.length();
    }
    tmplogname.erase(tmplogprefixlen);
    if (logSegmentHasLogSeq(num)) {
        AppendDecIntToString(tmplogname, lastLogSeq.mEpochSeq);
        tmplogname += '.';
        AppendDecIntToString(tmplogname, lastLogSeq.mViewSeq);
        tmplogname += '.';
        AppendDecIntToString(tmplogname, lastLogSeq.mLogSeq);
        tmplogname += '.';
    }
    AppendDecIntToString(tmplogname, num);
    return tmplogname;
}

string
Replay::getLastLog()
{
    const char* kLast = MetaDataStore::GetLogSegmentLastFileNamePtr();
    const string::size_type pos = path.rfind('/');
    if (string::npos != pos) {
        return path.substr(0, pos + 1) + kLast;
    }
    return kLast;
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
    return (ok && vers == LogWriter::VERSION);
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
    kfsUid_t   user     = kKfsUserNone;
    kfsUid_t   group    = kKfsGroupNone;
    kfsMode_t  mode     = 0;
    int64_t    k        = user;
    kfsSTier_t minSTier = kKfsSTierMax;
    kfsSTier_t maxSTier = kKfsSTierMax;
    if (! c.empty()) {
        if (! pop_num(k, "user", c, ok)) {
            return false;
        }
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
        if (! c.empty()) {
            if (! pop_num(k, "minTier", c, ok)) {
                return false;
            }
            minSTier = (kfsSTier_t)k;
            if (! pop_num(k, "maxTier", c, ok)) {
                return false;
            }
            maxSTier = (kfsSTier_t)k;
        }
    } else {
        user  = gLayoutManager.GetDefaultLoadUser();
        group = gLayoutManager.GetDefaultLoadGroup();
        mode  = gLayoutManager.GetDefaultLoadFileMode();
    }
    if (user == kKfsUserNone || group == kKfsGroupNone ||
            mode == kKfsModeUndef) {
        return false;
    }
    if (maxSTier < minSTier ||
            ! IsValidSTier(minSTier) ||
            ! IsValidSTier(maxSTier)) {
        return false;
    }
    // for all creates that were successful during normal operation,
    // when we replay it should work; so, exclusive = false
    MetaFattr* fa = 0;
    status = metatree.create(parent, myname, &me, numReplicas, false,
        t, n, nr, ss, user, group, mode,
        kKfsUserRoot, kKfsGroupRoot, &fa,
        gottime ? ctime : ReplayState::get(c).mLogSegmentTimeUsec,
        0 < todumpster);
    if (0 == status) {
        assert(fa);
        updateSeed(fileID, me);
        if (gottime) {
            fa->mtime = fa->ctime = fa->atime = ctime;
            if (fa->IsStriped()) {
                fa->filesize = 0;
            }
        }
        if (minSTier < kKfsSTierMax) {
            fa->minSTier = minSTier;
            fa->maxSTier = maxSTier;
        }
    }
    KFS_LOG_STREAM_DEBUG << "replay create:"
        " name: " << myname <<
        " id: "   << me <<
    KFS_LOG_EOM;
    return (0 == status);
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
    int64_t mtime;
    if (! pop_time(mtime, "mtime", c, ok)) {
        mtime = ReplayState::get(c).mLogSegmentTimeUsec;
    }
    MetaFattr* fa = 0;
    status = metatree.mkdir(parent, myname, user, group, mode,
        kKfsUserRoot, kKfsGroupRoot, &me, &fa, mtime);
    if (0 == status) {
        assert(fa);
        updateSeed(fileID, me);
        if (gottime) {
            fa->mtime = fa->ctime = fa->atime = ctime;
        }
    }
    KFS_LOG_STREAM_DEBUG << "replay mkdir: "
        " name: " << myname <<
        " id: "   << me <<
    KFS_LOG_EOM;
    return (ok && 0 == status);
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
    if (! pop_fid(todumpster, "todumpster", c, ok)) {
        todumpster = -1;
    }
    if (ok) {
        int64_t mtime;
        if (! pop_time(mtime, "mtime", c, ok)) {
            mtime = ReplayState::get(c).mLogSegmentTimeUsec;
        }
        status = metatree.remove(parent, myname, "",
            kKfsUserRoot, kKfsGroupRoot, mtime, 0 < todumpster);
    }
    return (ok && 0 == status);
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
    if (ok) {
        int64_t mtime;
        if (! pop_time(mtime, "mtime", c, ok)) {
            mtime = ReplayState::get(c).mLogSegmentTimeUsec;
        }
        status = metatree.rmdir(parent, myname, "",
            kKfsUserRoot, kKfsGroupRoot, mtime);
    }
    return (ok && 0 == status);
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
        int64_t mtime;
        if (! pop_time(mtime, "mtime", c, ok)) {
            mtime = ReplayState::get(c).mLogSegmentTimeUsec;
        }
        string oldpath;
        status = metatree.rename(parent, oldname, newpath, oldpath,
            true, kKfsUserRoot, kKfsGroupRoot, mtime, 0, 0 < todumpster);
    }
    return (ok && 0 == status);
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
    seq_t chunkVersion = -1, logChunkVersion;
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
            fa->mtime = max(fa->mtime, mtime);
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
            if (cid != logChunkId) {
                return false;
            }
            if (chunkVersion == logChunkVersion) {
                return true;
            }
            status = 0;
        }
        if (0 == status) {
            assert(cid == logChunkId);
            status = metatree.assignChunkId(fid, offset,
                cid, logChunkVersion, fa->mtime, 0, 0, append);
            if (0 == status) {
                fid_t cfid = 0;
                if (chunkExists &&
                        (! gLayoutManager.GetChunkFileId(
                            cid, cfid) ||
                        fid != cfid)) {
                    panic("missing chunk mapping", false);
                }
                MetaLogChunkAllocate logAlloc;
                logAlloc.replayFlag          = true;
                logAlloc.status              = 0;
                logAlloc.fid                 = fid;
                logAlloc.offset              = offset;
                logAlloc.chunkId             = logChunkId;
                logAlloc.chunkVersion        = logChunkVersion;
                logAlloc.appendChunk         = append;
                logAlloc.invalidateAllFlag   = false;
                logAlloc.objectStoreFileFlag = 0 == fa->numReplicas;
                logAlloc.initialChunkVersion = chunkVersion;
                logAlloc.mtime               = gottime ? mtime : fa->mtime;
                gLayoutManager.CommitOrRollBackChunkVersion(logAlloc);
                status = logAlloc.status;
                // assign updates the mtime; so, set it to what is in the log.
                if (0 == status && gottime) {
                    fa->mtime = mtime;
                }
                if (chunkID.getseed() < cid) {
                    // Update here should only be executed for old style write
                    // behind log. Write append id update should be executed by
                    // log chunk in flight op handler.
                    chunkID.setseed(cid);
                }
            }
        }
    }
    return (ok && 0 == status);
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
        gottime ? mtime : ReplayState::get(c).mLogSegmentTimeUsec,
        numChunksMoved,
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
            gottime ? mtime : ReplayState::get(c).mLogSegmentTimeUsec,
            kKfsUserRoot, kKfsGroupRoot, endOffset, kSetEofHintFlag, -1, -1, 0);
    }
    return (ok && 0 == status);
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
            gottime ? mtime : ReplayState::get(c).mLogSegmentTimeUsec,
            kKfsUserRoot, kKfsGroupRoot, -1, -1, 0);
    }
    return (ok && 0 == status);
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
    c.pop_front();
    fid_t fid;
    bool ok = pop_fid(fid, "file", c, true);
    int16_t numReplicas;
    ok = pop_short(numReplicas, "replicas", c, ok);
    kfsSTier_t minSTier = kKfsSTierUndef;
    kfsSTier_t maxSTier = kKfsSTierUndef;
    if (! c.empty()) {
        int64_t k;
        if (! pop_num(k, "minTier", c, ok)) {
            return false;
        }
        minSTier = (kfsSTier_t)k;
        if (! pop_num(k, "maxTier", c, ok)) {
            return false;
        }
        maxSTier = (kfsSTier_t)k;
    }
    if (! ok) {
        return ok;
    }
    MetaFattr* const fa = metatree.getFattr(fid);
    return (fa && metatree.changeFileReplication(
        fa, numReplicas, minSTier, maxSTier) == 0);
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

/*!
 * \brief restore time
 * format: time/<time>
 */
static bool
replay_time(DETokenizer& c)
{
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    // 2016-02-06T04:11:44.429777Z
    const char* ptr    = c.front().ptr;
    int         year   = 0;
    int         mon    = 0;
    int         mday   = 0;
    int         hour   = 0;
    int         minute = 0;
    int         sec    = 0;
    int64_t     usec   = 0;
    if (27 == c.front().len &&
            DecIntParser::Parse(ptr, 4, year) &&
            '-' == *ptr &&
            DecIntParser::Parse(++ptr, 2, mon) &&
            '-' == *ptr &&
            1 <= mon && mon <= 12 &&
            DecIntParser::Parse(++ptr, 2, mday) &&
            1 <= mday && mday <= 31 &&
            'T' == *ptr &&
            DecIntParser::Parse(++ptr, 2, hour) &&
            0 <= hour && hour <= 23 &&
            ':' == *ptr &&
            DecIntParser::Parse(++ptr, 2, minute) &&
            0 <= minute && minute <= 59 &&
            ':' == *ptr &&
            DecIntParser::Parse(++ptr, 2, sec) &&
            0 <= sec && sec <= 59 &&
            '.' == *ptr &&
            DecIntParser::Parse(++ptr, 6, usec) &&
            0 <= usec && usec <= 999999 &&
            'Z' == *ptr) {
        ReplayState::get(c).mLogSegmentTimeUsec =
            ToUnixTime(year, mon, mday, hour, minute, sec) * 1000000 + usec;
    } else {
        ReplayState::get(c).mLogSegmentTimeUsec = microseconds();
    }
    KFS_LOG_STREAM_INFO << "log time: " << c.front() << KFS_LOG_EOM;
    ReplayState::get(c).mRestoreTimeCount++;
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
replay_makechunkstable(DETokenizer& c, bool addFlag)
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
replay_mkstable(DETokenizer& c)
{
    return replay_makechunkstable(c, true);
}

static bool
replay_mkstabledone(DETokenizer& c)
{
    return replay_makechunkstable(c, false);
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
    const bool    kPanicOnInvalidVersionFlag = false;
    string* const kStatusMsg                 = 0;
    const int ret = gLayoutManager.ProcessBeginChangeChunkVersion(
        fid, chunkId, chunkVersion, kStatusMsg, kPanicOnInvalidVersionFlag);
    return (0 == ret || -ENOENT == ret);
}

/*
  DEPRECATED, DO NOT USE.
  Meta server now maintains chunk server inventory, and stores in flight
  chunk (ID) allocations in checkpoint and transaction log, therefore highest
  used chunk ID is now maintained the same way as file ID.
*/
static bool
replay_rollseeds(DETokenizer& c)
{
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    ReplayState& state = ReplayState::get(c);
    if (! state.mReplayer || state.mReplayer->logSegmentHasLogSeq()) {
        KFS_LOG_STREAM_ERROR <<
            "DEPRECATED: rollseeds is no longer required and cannot be used"
            " with the current transaction log format" <<
        KFS_LOG_EOM;
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
    state.mReplayer->setRollSeeds(roll);
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

static bool
replay_inc_seq(DETokenizer& c)
{
    return ReplayState::get(c).incSeq();
}

static bool
replay_sub_entry(DETokenizer& c)
{
    return ReplayState::get(c).subEntry();
}

bool
ReplayState::runCommitQueue(
    const MetaVrLogSeq& logSeq,
    seq_t               seed,
    int64_t             status,
    int64_t             errChecksum)
{
    if (logSeq <= mCheckpointCommitted) {
        // Checkpoint has no info about the last op status.
        if ((logSeq == mCheckpointCommitted ?
                (errChecksum == mCheckpointErrChksum &&
                    mCheckpointFileIdSeed == seed) :
                (mCommitQueue.empty() ||
                    logSeq < mCommitQueue.front().logSeq))) {
            if (mViewStartSeq < logSeq) {
                return true;
            }
        } else {
            KFS_LOG_STREAM_ERROR <<
                "commit"
                " sequence: "       << logSeq <<
                " checkpoint: "     << mCheckpointCommitted <<
                " error checksum:"
                " log: "            << errChecksum <<
                " actual: "         << mCheckpointErrChksum <<
                " seed:"
                " log: "            << seed <<
                " actual: "         << mCheckpointFileIdSeed <<
                " view start: "     << mViewStartSeq <<
                " queue:"
                " size: "           << mCommitQueue.size() <<
                " front: "          << (mCommitQueue.empty() ?
                    MetaVrLogSeq() : mCommitQueue.front().logSeq) <<
            KFS_LOG_EOM;
            return false;
        }
    }
    if (logSeq < mLastNonLogCommit) {
        return true;
    }
    const MetaVrLogSeq    endSeq    = max(logSeq, mViewStartSeq);
    CommitQueue::iterator it        = mCommitQueue.begin();
    bool                  foundFlag = false;
    while (mCommitQueue.end() != it) {
        CommitQueueEntry& f = *it;
        if (endSeq < f.logSeq) {
            break;
        }
        commit(f);
        if (logSeq == f.logSeq) {
            foundFlag = true;
            if (mViewStartSeq <= logSeq &&
                    (f.status != status ||
                    f.seed != seed ||
                    f.errChecksum != errChecksum)) {
                for (CommitQueue::const_iterator cit = mCommitQueue.begin();
                        ; ++cit) {
                    KFS_LOG_STREAM_ERROR <<
                        "commit"
                        " sequence: "       << cit->logSeq <<
                        " x "               << hex << cit->logSeq << dec <<
                        " seed: "           << cit->seed <<
                        " error checksum: " << cit->errChecksum <<
                        " status: "         << cit->status <<
                        " [" << (0 == cit->status ? string("OK") :
                            ErrorCodeToString(-KfsToSysErrno(cit->status))) <<
                        "]" <<
                    KFS_LOG_EOM;
                    if (cit == it) {
                        break;
                    }
                }
                KFS_LOG_STREAM_ERROR <<
                    "log commit:"
                    " sequence: " << logSeq <<
                    " x "         << hex << logSeq << dec <<
                    " status mismatch"
                    " log: "      << status <<
                    " [" << ErrorCodeToString(-KfsToSysErrno(status)) << "]"
                    " actual: "   << f.status <<
                    " [" << ErrorCodeToString(-KfsToSysErrno(f.status)) <<
                        "]" <<
                    " seed:"
                    " log: "      << seed <<
                    " actual: "   << f.seed <<
                    " error checksum:"
                    " log: "      << errChecksum <<
                    " actual: "   << f.errChecksum <<
                KFS_LOG_EOM;
                return false;
            }
        }
        ++it;
    }
    mCommitQueue.erase(mCommitQueue.begin(), it);
    // Commit sequence must always be at the log block end.
    if (! foundFlag && mViewStartSeq <= logSeq &&
            (fileID.getseed() != seed ||
            mLastCommittedStatus != status ||
            mLogAheadErrChksum != errChecksum)) {
        KFS_LOG_STREAM_ERROR <<
            "invliad log commit:"
            " sequence: "       << logSeq <<
            " x "               << hex << logSeq << dec <<
            " / "               << (mCommitQueue.empty() ?
                MetaVrLogSeq() : mCommitQueue.front().logSeq) <<
            " status: "         << status <<
            " [" << ErrorCodeToString(-KfsToSysErrno(status)) << "]" <<
            " expected: "       << mLastCommittedStatus <<
            " [" << ErrorCodeToString(
                    -KfsToSysErrno(mLastCommittedStatus)) << "]" <<
            " seed:"
            " log: "            << seed <<
            " actual: "         << fileID.getseed() <<
            " error checksum:"
            " log: "            << errChecksum <<
            " actual: "         << mLogAheadErrChksum <<
            " commit queue: "   << mCommitQueue.size() <<
        KFS_LOG_EOM;
        return false;
    }
    return true;
}

static bool
replay_log_ahead_entry(DETokenizer& c)
{
    ReplayState& state = ReplayState::get(c);
    if (0 != state.mSubEntryCount || state.mCurOp) {
        KFS_LOG_STREAM_ERROR <<
            "invalid replay state:"
            " sub entry count: " << state.mSubEntryCount <<
            " cur op: "          << MetaRequest::ShowReq(state.mCurOp) <<
        KFS_LOG_EOM;
        return false;
    }
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    const DETokenizer::Token& token = c.front();
    state.mCurOp = MetaRequest::ReadReplay(token.ptr, token.len);
    if (! state.mCurOp) {
        KFS_LOG_STREAM_ERROR <<
            "replay parse failure:"
            " logseq: " << state.mLastLogAheadSeq <<
            " "         << token <<
        KFS_LOG_EOM;
        return false;
    }
    if (! state.isCurOpLogSeqValid()) {
        return false;
    }
    state.mCurOp->replayFlag = true;
    return (state.handle() && state.incSeq());
}

static bool
replay_log_commit_entry(DETokenizer& c, Replay::BlockChecksum& blockChecksum)
{
    if (c.size() < 9) {
        return false;
    }
    ReplayState& state = ReplayState::get(c);
    if (state.mCurOp) {
        KFS_LOG_STREAM_ERROR <<
            "replay invalid state:"
            " non null current op at the end of log block: " <<
            state.mCurOp->Show() <<
        KFS_LOG_EOM;
        return false;
    }
    const char* const ptr   = c.front().ptr;
    const size_t      len   = c.back().ptr - ptr;
    const size_t      skip  = len + c.back().len;
    blockChecksum.write(ptr, len);
    c.pop_front();
    MetaVrLogSeq commitSeq;
    if (! parse_vr_log_seq(c, commitSeq) || ! commitSeq.IsValid()) {
        return false;
    }
    c.pop_front();
    const int64_t seed = c.toNumber();
    if (! c.isLastOk()) {
        return false;
    }
    c.pop_front();
    const int64_t errchksum = c.toNumber();
    if (! c.isLastOk()) {
        return false;
    }
    c.pop_front();
    const int64_t status = c.toNumber();
    if (! c.isLastOk() || status < 0) {
        return false;
    }
    c.pop_front();
    MetaVrLogSeq logSeq;
    if (! parse_vr_log_seq(c, logSeq) || logSeq != state.mLastLogAheadSeq) {
        return false;
    }
    c.pop_front();
    const int64_t blockLen = c.toNumber();
    if (! c.isLastOk() || blockLen < 0 ||
            state.mBlockStartLogSeq.mLogSeq + blockLen != logSeq.mLogSeq) {
        return false;
    }
    c.pop_front();
    const int64_t blockSeq = c.toNumber();
    if (! c.isLastOk() || blockSeq != state.mLastBlockSeq + 1) {
        return false;
    }
    c.pop_front();
    const int64_t checksum = c.toNumber();
    if (! c.isLastOk() || checksum < 0) {
        return false;
    }
    const uint32_t expectedChecksum = blockChecksum.blockEnd(skip);
    if ((int64_t)expectedChecksum != checksum) {
        KFS_LOG_STREAM_ERROR <<
            "record block checksum mismatch:"
            " expected: " << expectedChecksum <<
            " actual: "   << checksum <<
        KFS_LOG_EOM;
        return false;
    }
    if (commitSeq < state.mLastBlockCommittedSeq ||
            (commitSeq < state.mLastCommittedSeq &&
                state.mCheckpointCommitted <= commitSeq &&
                state.mLastNonLogCommit <= commitSeq &&
                state.mViewStartSeq < commitSeq) ||
            state.mLastLogAheadSeq < commitSeq ||
                (state.mViewStartSeq == commitSeq &&
                    state.mViewStartSeq != MetaVrLogSeq(0, 0, 0))) {
        KFS_LOG_STREAM_ERROR <<
            "committed:"
            " expected range: [" << state.mLastCommittedSeq <<
            ","                  << state.mLastLogAheadSeq << "]"
            " view start: "      << state.mViewStartSeq <<
            " actual: "          << commitSeq <<
            " previous: "        << state.mLastBlockCommittedSeq <<
        KFS_LOG_EOM;
        return false;
    }
    if (! state.runCommitQueue(commitSeq, seed, status, errchksum)) {
        return false;
    }
    if (0 != state.mSubEntryCount) {
        return false;
    }
    state.mBlockStartLogSeq      = logSeq;
    state.mLastBlockSeq          = blockSeq;
    state.mLastBlockCommittedSeq = commitSeq;
    state.mLastBlockSeed         = seed;
    state.mLastBlockStatus       = status;
    state.mLastBlockErrChecksum  = errchksum;
    if (state.mLastCommittedSeq < commitSeq) {
        state.mLastCommittedSeq = commitSeq;
    }
    return true;
}

static bool
replay_group_users_reset(DETokenizer& c)
{
    ReplayState& state = ReplayState::get(c);
    if (c.empty() || state.mCurOp || ! state.mLastLogAheadSeq.IsValid()) {
        return false;
    }
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    const int64_t n = c.toNumber();
    if (! c.isLastOk() || n < 0) {
        return false;
    }
    c.pop_front();
    state.mSubEntryCount = n;
    state.mCurOp = new MetaSetGroupUsers(16 == c.getIntBase());
    state.mCurOp->logseq = state.mLastLogAheadSeq;
    state.mCurOp->logseq.mLogSeq++;
    state.mCurOp->replayFlag = true;
    if (state.mSubEntryCount <= 0) {
        state.mSubEntryCount = 1;
        state.replayCurOp();
        return replay_sub_entry(c);
    }
    return true;
}

static bool
replay_group_users(DETokenizer& c)
{
    ReplayState& state = ReplayState::get(c);
    if (c.empty() || ! state.mCurOp ||
            META_SET_GROUP_USERS != state.mCurOp->op) {
        return false;
    }
    const bool appendFlag = 3 == c.front().len;
    c.pop_front();
    if (c.empty()) {
        return false;
    }
    const DETokenizer::Token& token = c.front();
    MetaSetGroupUsers& op = *static_cast<MetaSetGroupUsers*>(state.mCurOp);
    op.AddEntry(appendFlag, token.ptr, token.len);
    if (1 == state.mSubEntryCount) {
        state.replayCurOp();
    }
    return replay_sub_entry(c);
}

static bool
replay_clear_obj_store_delete(DETokenizer& c)
{
    c.pop_front();
    gLayoutManager.ClearObjStoreDelete();
    return true;
}

static bool
replay_cs_hello(DETokenizer& c)
{
    const DETokenizer::Token& verb  = c.front();
    ReplayState&              state = ReplayState::get(c);
    MetaHello*                op;
    if (3 == verb.len) {
        if (0 != state.mSubEntryCount) {
            return false;
        }
        c.pop_front();
        int64_t n;
        if (! pop_num(n, "e", c, true) || n <= 0) {
            return false;
        }
        if ("l" != c.front()) {
            return false;
        }
        c.pop_front();
        state.mSubEntryCount = n;
        op = new MetaHello();
        state.mCurOp = op;
        op->replayFlag = true;
        const DETokenizer::Token& loc = c.front();
        if (! op->location.FromString(loc.ptr, loc.len, 16 == c.getIntBase())) {
            return false;
        }
        c.pop_front();
        if (! pop_num(n, "s", c, true) || n < 0) {
            return false;
        }
        op->numChunks = (int)n;
        if (! pop_num(n, "n", c, true) || n < 0) {
            return false;
        }
        op->numNotStableChunks = (int)n;
        if (! pop_num(n, "a", c, true) || n < 0) {
            return false;
        }
        op->numNotStableAppendChunks = (int)n;
        if (! pop_num(n, "m", c, true) || n < 0) {
            return false;
        }
        op->numMissingChunks = (int)n;
        if (pop_num(n, "p", c, true)) {
            if (n < 0) {
                return false;
            }
            op->numPendingStaleChunks = (int)n;
        } else {
            op->numPendingStaleChunks = 0;
        }
        if (! pop_num(n, "d", c, true) || n < 0) {
            return false;
        }
        op->deletedCount = (size_t)n;
        if (! pop_num(n, "r", c, true)) {
            return false;
        }
        op->resumeStep = (int)n;
        if (! pop_num(n, "t", c, true)) {
            return false;
        }
        op->timeUsec = n;
        if (! pop_num(n, "r", c, true)) {
            return false;
        }
        op->rackId = (int)n;
        if (! pop_num(n, "P", c, true)) {
            return false;
        }
        op->pendingNotifyFlag = 0 != n;
        n = 0;
        if (! pop_num(n, "R", c, true)) {
            return false;
        }
        op->supportsResumeFlag = 0 != n;
        if (c.empty() || 1 != c.front().len || 'z' != c.front().ptr[0]) {
            return false;
        }
        c.pop_front();
        if (! parse_vr_log_seq(c, op->logseq) || ! op->logseq.IsValid()) {
            return false;
        }
        c.pop_front();
        if (state.mReplayer) {
            MetaVrLogSeq next = state.mLastLogAheadSeq;
            next.mLogSeq++;
            if (op->logseq != next) {
                return false;
            }
        }
        op->chunks.reserve(op->numChunks);
        op->notStableChunks.reserve(op->numNotStableChunks);
        op->notStableAppendChunks.reserve(op->numNotStableAppendChunks);
        op->missingChunks.reserve(op->numMissingChunks);
    } else {
        if (4 != verb.len || ! state.mCurOp) {
            return false;
        }
        op = static_cast<MetaHello*>(state.mCurOp);
        if ('c' == verb.ptr[3]) {
            c.pop_front();
            if (c.empty()) {
                return false;
            }
            MetaHello::ChunkInfo info;
            while (! c.empty()) {
                info.chunkId = c.toNumber();
                if (! c.isLastOk() || info.chunkId < 0) {
                    return false;
                }
                c.pop_front();
                if (c.empty()) {
                    return false;
                }
                info.chunkVersion = c.toNumber();
                if (! c.isLastOk() || info.chunkVersion < 0) {
                    return false;
                }
                c.pop_front();
                if (op->chunks.size() < (size_t)op->numChunks) {
                    op->chunks.push_back(info);
                } else if (op->notStableChunks.size() <
                        (size_t)op->numNotStableChunks) {
                    op->notStableChunks.push_back(info);
                } else if (op->notStableAppendChunks.size() <
                        (size_t)op->numNotStableAppendChunks) {
                    op->notStableAppendChunks.push_back(info);
                } else {
                    return false;
                }
            }
        } else {
            const int type = verb.ptr[3] & 0xFF;
            if ('m' != type && 'p' != type) {
                return false;
            }
            c.pop_front();
            if (c.empty()) {
                return false;
            }
            MetaHello::ChunkIdList& list = 'm' == type ?
                op->missingChunks : op->pendingStaleChunks;
            const size_t            cnt  = max(0, 'm' == type ?
                op->numMissingChunks : op->numPendingStaleChunks);
            while (! c.empty()) {
                const int64_t n = c.toNumber();
                if (! c.isLastOk() || n < 0) {
                    return false;
                }
                c.pop_front();
                if (cnt <= list.size()) {
                    return false;
                }
                list.push_back(n);
            }
        }
    }
    if (1 == state.mSubEntryCount) {
        if (op->chunks.size() != (size_t)op->numChunks ||
                op->notStableChunks.size() != (size_t)op->numNotStableChunks ||
                op->notStableAppendChunks.size() !=
                    (size_t)op->numNotStableAppendChunks ||
                op->missingChunks.size() != (size_t)op->numMissingChunks ||
                op->pendingStaleChunks.size() !=
                    (size_t)op->numPendingStaleChunks) {
            return false;
        }
        state.replayCurOp();
    }
    return replay_sub_entry(c);
}

static bool
replay_cs_inflight(DETokenizer& c)
{
    const DETokenizer::Token& verb  = c.front();
    ReplayState&              state = ReplayState::get(c);
    MetaChunkLogInFlight*     op;
    if (3 == verb.len && 's' == verb.ptr[2]) {
        op = static_cast<MetaChunkLogInFlight*>(state.mCurOp);
        if (! op) {
            return false;
        }
        c.pop_front();
        while (! c.empty()) {
            const int64_t n = c.toNumber();
            if (! c.isLastOk() || n < 0) {
                return false;
            }
            c.pop_front();
            if ((size_t)op->idCount <= op->chunkIds.Size()) {
                return false;
            }
            op->chunkIds.Insert(n);
        }
        if (1 == state.mSubEntryCount &&
                (size_t)op->idCount != op->chunkIds.Size()) {
            return false;
        }
    } else {
        if (0 != state.mSubEntryCount || state.mCurOp) {
            return false;
        }
        c.pop_front();
        int64_t n;
        if (! pop_num(n, "e", c, true) || n <= 0) {
            return false;
        }
        if ("l" != c.front()) {
            return false;
        }
        c.pop_front();
        state.mSubEntryCount = n;
        op = new MetaChunkLogInFlight();
        state.mCurOp = op;
        op->replayFlag = true;
        const DETokenizer::Token& loc = c.front();
        if (! op->location.FromString(loc.ptr, loc.len, 16 == c.getIntBase())) {
            return false;
        }
        c.pop_front();
        if (! pop_num(n, "s", c, true) || n < 0) {
            return false;
        }
        op->idCount = n;
        if (! pop_num(n, "c", c, true) || (0 < op->idCount && 0 <= n)) {
            return false;
        }
        op->chunkId = n;
        if (! pop_num(n, "x", c, true) || n < 0) {
            return false;
        }
        op->removeServerFlag = 0 != n;
        if ("r" != c.front()) {
            return false;
        }
        c.pop_front();
        if (c.empty()) {
            return false;
        }
        // Original request type, presently used for debugging.
        const DETokenizer::Token& rtype = c.front();
        op->reqType = MetaChunkLogInFlight::GetReqId(rtype.ptr, rtype.len);
        c.pop_front();
        if (! c.empty() && 'p' ==  c.front().ptr[0]) {
            op->hadPendingChunkOpFlag = true;
            c.pop_front();
        }
        if (! c.empty() && 'n' ==  c.front().ptr[0]) {
            c.pop_front();
        } else {
            op->processPendingDownFlag = true;
        }
        if (1 != c.front().len || 'z' != c.front().ptr[0]) {
            return false;
        }
        c.pop_front();
        if (! parse_vr_log_seq(c, op->logseq) || ! op->logseq.IsValid()) {
            return false;
        }
        c.pop_front();
        if (state.mReplayer) {
            MetaVrLogSeq next = state.mLastLogAheadSeq;
            next.mLogSeq++;
            if (next != op->logseq) {
                return false;
            }
        }
    }
    if (1 == state.mSubEntryCount) {
        state.replayCurOp();
    }
    return replay_sub_entry(c);
}

bool
restore_chunk_server_end(DETokenizer& c)
{
    return replay_inc_seq(c);
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
    e.add_parser("version",                 &replay_version);
    e.add_parser("create",                  &replay_create);
    e.add_parser("mkdir",                   &replay_mkdir);
    e.add_parser("remove",                  &replay_remove);
    e.add_parser("rmdir",                   &replay_rmdir);
    e.add_parser("rename",                  &replay_rename);
    e.add_parser("allocate",                &replay_allocate);
    e.add_parser("truncate",                &replay_truncate);
    e.add_parser("coalesce",                &replay_coalesce);
    e.add_parser("pruneFromHead",           &replay_pruneFromHead);
    e.add_parser("setrep",                  &replay_setrep);
    e.add_parser("size",                    &replay_size);
    e.add_parser("setmtime",                &replay_setmtime);
    e.add_parser("chunkVersionInc",         &restore_chunkVersionInc);
    e.add_parser("time",                    &replay_time);
    e.add_parser("mkstable",                &replay_mkstable);
    e.add_parser("mkstabledone",            &replay_mkstabledone);
    e.add_parser("beginchunkversionchange", &replay_beginchunkversionchange);
    e.add_parser("checksum",                &restore_checksum);
    e.add_parser("rollseeds",               &replay_rollseeds);
    e.add_parser("chmod",                   &replay_chmod);
    e.add_parser("chown",                   &replay_chown);
    e.add_parser("delegatecancel",          &restore_delegate_cancel);
    e.add_parser("filesysteminfo",          &restore_filesystem_info);
    e.add_parser("clearobjstoredelete",     &replay_clear_obj_store_delete);
    // Write ahead log entries.
    e.add_parser("gur",                     &replay_group_users_reset);
    e.add_parser("gu",                      &replay_group_users);
    e.add_parser("guc",                     &replay_group_users);
    e.add_parser("csh",                     &replay_cs_hello);
    e.add_parser("cshc",                    &replay_cs_hello);
    e.add_parser("cshm",                    &replay_cs_hello);
    e.add_parser("cshp",                    &replay_cs_hello);
    e.add_parser("cif",                     &replay_cs_inflight);
    e.add_parser("cis",                     &replay_cs_inflight);
    initied = true;
    return e;
}

/* static */ void
Replay::AddRestotreEntries(DiskEntry& e)
{
    e.add_parser("cif", &replay_cs_inflight);
    e.add_parser("cis", &replay_cs_inflight);
}

Replay::BlockChecksum::BlockChecksum()
    : skip(0),
      checksum(kKfsNullChecksum)
{}

uint32_t
Replay::BlockChecksum::blockEnd(size_t s)
{
    skip = s;
    const uint32_t ret = checksum;
    checksum = kKfsNullChecksum;
    return ret;
}

bool
Replay::BlockChecksum::write(const char* buf, size_t len)
{
    if (len <= skip) {
        skip -= len;
    } else {
        checksum = ComputeBlockChecksum(checksum, buf + skip, len - skip);
        skip = 0;
    }
    return true;
}

Replay::Tokenizer::Tokenizer(
    istream& file, Replay* replay, bool* enqueueFlag)
     : state(*(new Replay::State(replay, enqueueFlag))),
       tokenizer(*(new DETokenizer(file, &state)))
{}

Replay::Tokenizer::~Tokenizer()
{
    delete &tokenizer;
    delete &state;
}

const DETokenizer::Token kAheadLogEntry ("a", 1);
const DETokenizer::Token kCommitLogEntry("c", 1);

Replay::Replay()
    : file(),
      path(),
      number(-1),
      lastLogNum(-1),
      lastLogIntBase(-1),
      appendToLastLogFlag(false),
      verifyAllLogSegmentsPresetFlag(false),
      enqueueFlag(false),
      replayTokenizer(file, this, &enqueueFlag),
      checkpointCommitted(replayTokenizer.GetState().mCheckpointCommitted),
      committed(replayTokenizer.GetState().mLastCommittedSeq),
      lastLogStart(checkpointCommitted),
      lastLogSeq(replayTokenizer.GetState().mLastLogAheadSeq),
      viewStartSeq(replayTokenizer.GetState().mViewStartSeq),
      lastBlockSeq(replayTokenizer.GetState().mLastBlockSeq),
      errChecksum(replayTokenizer.GetState().mLogAheadErrChksum),
      lastCommittedStatus(replayTokenizer.GetState().mLastCommittedStatus),
      rollSeeds(0),
      tmplogprefixlen(0),
      tmplogname(),
      logdir(),
      mds(),
      entrymap(get_entry_map()),
      blockChecksum(),
      maxLogNum(-1),
      logSeqStartNum(-1),
      primaryNodeId(-1),
      buffer()
{
    buffer.Reserve(16 << 10);
}

Replay::~Replay()
{}

int
Replay::playLine(const char* line, int len, seq_t blockSeq)
{
    if (len <= 0) {
        return 0;
    }
    DETokenizer&               tokenizer = replayTokenizer.Get();
    ReplayState&               state     = replayTokenizer.GetState();
    ReplayState::EnterAndLeave enterAndLeave(state);
    tokenizer.setIntBase(16);
    if (0 <= blockSeq) {
        state.mLastBlockSeq = blockSeq - 1;
    }
    int status = 0;
    if (! tokenizer.next(line, len)) {
        status = -EINVAL;
    }
    if (0 == status && ! tokenizer.empty()) {
        if (! (kAheadLogEntry == tokenizer.front() ?
                replay_log_ahead_entry(tokenizer) :
                (kCommitLogEntry == tokenizer.front() ?
                    replay_log_commit_entry(tokenizer, blockChecksum) :
                    entrymap.parse(tokenizer)))) {
            KFS_LOG_STREAM_ERROR <<
                "error block seq: " << blockSeq <<
                ":" << tokenizer.getEntryCount() <<
                ":" << tokenizer.getEntry() <<
            KFS_LOG_EOM;
            status = -EINVAL;
        }
    }
    if (0 <= blockSeq || 0 != status) {
        blockChecksum.blockEnd(0);
        blockChecksum.write("\n", 1);
        if (state.mSubEntryCount != 0 && 0 == status) {
            KFS_LOG_STREAM_ERROR <<
                "invalid block commit:"
                " sub entry count: " << state.mSubEntryCount <<
            KFS_LOG_EOM;
            state.mSubEntryCount = 0;
            status = -EINVAL;
            // Next block implicitly includes leading new line.
            tokenizer.resetEntryCount();
        }
    } else {
        blockChecksum.write(line, len);
    }
    return status;
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
    blockChecksum.blockEnd(0);
    mds.Reset(&blockChecksum);
    mds.SetWriteTrough(true);

    if (! file.is_open()) {
        //!< no log...so, reset the # to 0.
        number = 0;
        return 0;
    }

    ReplayState& state     = replayTokenizer.GetState();
    lastLogStart           = state.mLastLogAheadSeq;
    state.mLastBlockSeq    = -1;
    state.mSubEntryCount   = 0;
    int          status    = 0;
    DETokenizer& tokenizer = replayTokenizer.Get();
    tokenizer.reset();
    while (tokenizer.next(&mds)) {
        if (tokenizer.empty()) {
            continue;
        }
        if (! (kAheadLogEntry == tokenizer.front() ?
                replay_log_ahead_entry(tokenizer) :
                (kCommitLogEntry == tokenizer.front() ?
                    replay_log_commit_entry(tokenizer, blockChecksum) :
                    entrymap.parse(tokenizer)))) {
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
    if (0 == status && 0 != state.mSubEntryCount) {
        KFS_LOG_STREAM_FATAL <<
            "error " << path <<
            " invalid sub entry count: " << state.mSubEntryCount <<
        KFS_LOG_EOM;
        status = -EIO;
    }
    if (0 == status && ! file.eof()) {
        KFS_LOG_STREAM_FATAL <<
            "error " << path <<
            ":" << tokenizer.getEntryCount() <<
            ":" << tokenizer.getEntry() <<
        KFS_LOG_EOM;
        status = -EIO;
    }
    if (0 == status) {
        lastLogIntBase = tokenizer.getIntBase();
        mds.SetStream(0);
    }
    file.close();
    blockChecksum.blockEnd(0);
    blockChecksum.write("\n", 1);
    tokenizer.resetEntryCount();
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
        return 0;
    }
    gLayoutManager.SetPrimary(false);
    gLayoutManager.StopServicing();
    const int status = getLastLogNum();
    return (0 == status ?
        playLogs(lastLogNum, includeLastLogFlag) : status);
}

int
Replay::playLogs(seq_t last, bool includeLastLogFlag)
{
    ReplayState&               state = replayTokenizer.GetState();
    ReplayState::EnterAndLeave enterAndLeave(state);

    appendToLastLogFlag  = false;
    lastLineChecksumFlag = false;
    lastLogIntBase       = -1;
    bool         lastEntryChecksumFlag = false;
    bool         completeSegmentFlag   = true;
    int          status                = 0;
    state.mCheckpointFileIdSeed   = fileID.getseed();
    state.mCheckpointErrChksum    = errChecksum;
    state.mBlockStartLogSeq       = checkpointCommitted;
    state.mLastNonEmptyViewEndSeq = checkpointCommitted;
    state.mUpdateLogWriterFlag = false; // Turn off updates in initial replay.
    for (seq_t i = number; ; i++) {
        if (! includeLastLogFlag && last < i) {
            break;
        }
        // Check if the next log segment exists prior to loading current log
        // segment in order to allow fsck to load all segments while meta server
        // is running. The meta server might close the current segment, and
        // create the new segment after reading / loading tail of the current
        // segment, in which case the last read might not have the last checksum
        // line.
        if (last < i && maxLogNum <= i) {
            completeSegmentFlag = ! logSegmentHasLogSeq(i + 1) &&
                file_exists(logfile(i + 1));
            if (! completeSegmentFlag && maxLogNum < i &&
                    ! file_exists(logfile(i))) {
                break;
            }
        }
        state.mRestoreTimeCount = 0;
        const string logfn = logfile(i);
        if ((status = openlog(logfn)) != 0 ||
                (status = playlog(lastEntryChecksumFlag)) != 0) {
            break;
        }
        if (state.mRestoreTimeCount <= 0) {
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
                (! lastEntryChecksumFlag && completeSegmentFlag)) {
            KFS_LOG_STREAM_FATAL <<
                logfn <<
                ": missing last line checksum" <<
            KFS_LOG_EOM;
            status = -EINVAL;
            break;
        }
        number = i;
        if (last < i && ! lastEntryChecksumFlag) {
            appendToLastLogFlag = true;
            break;
        }
    }
    // Enable updates, and reset primary node id at the end of replay.
    state.mUpdateLogWriterFlag = true;
    primaryNodeId = -1;
    if (0 == status) {
        if (state.mCommitQueue.empty() &&
                state.mLastBlockCommittedSeq == MetaVrLogSeq(0, 0, 0) &&
                state.mLastLogAheadSeq == state.mLastCommittedSeq &&
                state.mLastBlockCommittedSeq <= state.mLastCommittedSeq &&
                0 == state.mLastBlockSeed &&
                0 == state.mLastBlockErrChecksum &&
                0 == state.mLastBlockStatus) {
            // Set commit state, when converting from prior log version.
            state.mLastBlockSeed         = fileID.getseed();
            state.mLastBlockCommittedSeq = state.mLastCommittedSeq;
            appendToLastLogFlag          = false;
        }
    } else {
        appendToLastLogFlag = false;
    }
    return status;
}

static int
ValidateLogSegmentTrailer(
    const char* name,
    bool        completeSegmentFlag)
{
    int             ret       = 0;
    streamoff const kTailSize = 1 << 10;
    streamoff       pos       = -1;
    ifstream fs(name, ifstream::in | ifstream::binary);
    if (fs && fs.seekg(0, ifstream::end) && 0 <= (pos = fs.tellg())) {
        streamoff sz;
        if (kTailSize < pos) {
            sz = kTailSize;
            pos -= kTailSize;
        } else {
            sz  = pos;
            pos = 0;
        }
        StBufferT<char, 1> buf;
        char* const        ptr = buf.Resize(sz + streamoff(1));
        if (fs.seekg(pos, ifstream::beg) && fs.read(ptr, sz)) {
            if (sz != fs.gcount()) {
                KFS_LOG_STREAM_FATAL <<
                    name << ": "
                    "invalid read size:"
                    " actual: "   << fs.gcount() <<
                    " expected: " << sz <<
                KFS_LOG_EOM;
                ret = -EIO;
            } else {
                ptr[sz] = 0;
                const char*       p = ptr + sz - 1;
                const char* const b = ptr;
                if (p <= b || '\n' != *p) {
                    ret = -EINVAL;
                    KFS_LOG_STREAM_FATAL <<
                        name << ": no trailing new line: " <<
                        ptr <<
                    KFS_LOG_EOM;
                } else {
                    // Last line must start with log block trailer
                    // if segment is not complete / closed or checksum line
                    // otherwise.
                    --p;
                    while (b < p && '\n' != *p) {
                        --p;
                    }
                    if (p <= b ||
                            ((completeSegmentFlag || ptr + sz < p + 3 ||
                                    0 != memcmp("c/", p + 1, 2)) &&
                            (ptr + sz < p + 10 ||
                                0 != memcmp("checksum/", p + 1, 9)))) {
                        KFS_LOG_STREAM_FATAL <<
                            name << ": invalid log segment trailer: " <<
                            (p + 1) <<
                        KFS_LOG_EOM;
                        ret = -EINVAL;
                    }
                }
            }
        }
    }
    if (0 == ret && (! fs || pos < 0)) {
        const int err = errno;
        KFS_LOG_STREAM_FATAL <<
            name << ": " << QCUtils::SysError(err) <<
        KFS_LOG_EOM;
        ret = 0 < err ? -err : (err == 0 ? -EIO : err);
    }
    fs.close();
    return ret;
}

typedef map<
    seq_t,
    string,
    less<seq_t>,
    StdFastAllocator<pair<const seq_t, string> >
> LogSegmentNumbers;

int
Replay::getLastLogNum()
{
    if (0 <= lastLogNum) {
        return 0;
    }
    lastLogNum     = number;
    maxLogNum      = -1;
    logSeqStartNum = -1;
    if (lastLogNum < 0) {
        // no logs, to replay.
        return 0;
    }
    // Get last complete log number. All log files before and including this
    // won't ever be written to again.
    // Get the inode # for the last file
    const string lastlog = getLastLog();
    struct stat lastst = {0};
    if (stat(lastlog.c_str(), &lastst)) {
        const int err = errno;
        if (ENOENT == err) {
            lastLogNum = -1; // Checkpoint with single log segment.
        } else {
            KFS_LOG_STREAM_FATAL <<
                lastlog <<
                ": " << QCUtils::SysError(err) <<
            KFS_LOG_EOM;
            return (0 < err ? -err : (err == 0 ? -EIO : err));
        }
    }
    if (0 <= lastLogNum && lastst.st_nlink != 2) {
        KFS_LOG_STREAM_FATAL <<
            lastlog <<
            ": invalid link count: " << lastst.st_nlink <<
            " this must be \"hard\" link to the last complete log"
            " segment (usually the last log segment with last line starting"
            " with \"checksum/\" prefix), and therefore must have link"
            " count 2" <<
        KFS_LOG_EOM;
        return -EINVAL;
    }
    string            dirName  = lastlog;
    string::size_type pos      = dirName.rfind('/');
    const char*       lastName = lastlog.c_str();
    if (string::npos != pos) {
        lastName += pos + 1;
        if (pos <= 0) {
            dirName = "/";
        } else {
            dirName.erase(pos);
        }
    } else {
        dirName = ".";
    }
    DIR* const dir = opendir(dirName.c_str());
    if (! dir) {
        const int err = errno;
        KFS_LOG_STREAM_FATAL <<
            dirName << ": " << QCUtils::SysError(err) <<
        KFS_LOG_EOM;
        return (err > 0 ? -err : (err == 0 ? -1 : err));
    }
    bool                 useDirIno = false;
    string               pathName = dirName + '/';
    const size_t         pathNameLen = pathName.length();
    int                  ret = 0;
    LogSegmentNumbers    logNums;
    const struct dirent* ent;
    while ((ent = readdir(dir))) {
        if (strcmp(ent->d_name, lastName) == 0) {
            continue;
        }
        const char* const p   = strrchr(ent->d_name, '.');
        const int64_t     num = p ? toNumber(p + 1) : int64_t(-1);
        if (0 <= lastLogNum) {
            bool lastLogFlag;
            // Check if d_ino field can be used by comparing its value with
            // st_ino. If these match for the first entry, then use d_ino,
            // otherwise use stat to get i-node number.
            if (useDirIno) {
                lastLogFlag = lastst.st_ino == ent->d_ino;
            } else {
                pathName.erase(pathNameLen);
                pathName += ent->d_name;
                struct stat cur;
                if (stat(pathName.c_str(), &cur)) {
                    const int err = errno;
                    KFS_LOG_STREAM_ERROR <<
                        "stat: " << pathName <<
                        ": " << QCUtils::SysError(err) <<
                    KFS_LOG_EOM;
                    ret = 0 < err ? -err : -EINVAL;
                    break;
                }
                lastLogFlag = lastst.st_ino == cur.st_ino;
                useDirIno = cur.st_ino == ent->d_ino;
            }
            if (lastLogFlag) {
                lastLogNum = num;
                if (num < 0) {
                    KFS_LOG_STREAM_FATAL <<
                        "invalid log segment name: " <<
                            dirName << "/" << ent->d_name <<
                    KFS_LOG_EOM;
                    ret = -EINVAL;
                    break;
                }
            }
        }
        if (num < 0) {
            continue;
        }
        // Find first, if any, log segment number in the form
        // log.<log sequence>.<log number>
        const char* s = p;
        while (ent->d_name <= --s) {
            const int sym = *s & 0xFF;
            if ('.' == sym) {
                if (s + 1 < p && p <= s + 22) {
                    logSeqStartNum = logSeqStartNum < 0 ? num :
                        min(num, logSeqStartNum);
                }
                break;
            }
            if (sym < '0' || '9' < sym) {
                break;
            }
        }
        if (lastLogNum < 0 && number < num) {
            KFS_LOG_STREAM_FATAL <<
                "no link to last complete log segment: " << lastlog <<
            KFS_LOG_EOM;
            ret = -EINVAL;
            break;
        }
        if ((verifyAllLogSegmentsPresetFlag || number <= num) &&
                ! logNums.insert(make_pair(num, ent->d_name)).second) {
            KFS_LOG_STREAM_FATAL <<
                "duplicate log segment number: " << num <<
                " " << dirName << "/" << ent->d_name <<
            KFS_LOG_EOM;
            ret = -EINVAL;
            break;
        }
        if (maxLogNum < num) {
            maxLogNum = num;
        }
    }
    closedir(dir);
    if (0 == ret && maxLogNum < 0) {
        KFS_LOG_STREAM_FATAL <<
            "no log segments found: " << dirName <<
        KFS_LOG_EOM;
        ret = -EINVAL;
    }
    LogSegmentNumbers::const_iterator it = logNums.begin();
    if (logNums.end() == it || (verifyAllLogSegmentsPresetFlag ?
            logNums.find(number) == logNums.end() : it->first != number)) {
        KFS_LOG_STREAM_FATAL <<
            "missing log segmnet: " << number <<
        KFS_LOG_EOM;
        ret = -EINVAL;
    } else {
        seq_t n = it->first;
        while (logNums.end() != ++it) {
            if (++n != it->first) {
                KFS_LOG_STREAM_FATAL <<
                    "missing log segmnets:"
                    " from: " << n  <<
                    " to: "   << it->first <<
                KFS_LOG_EOM;
                n = it->first;
                ret = -EINVAL;
            }
        }
    }
    if (0 == ret && 0 <= logSeqStartNum) {
        it = logNums.find(logSeqStartNum);
        string name;
        while (logNums.end() != it) {
            name = dirName + "/" + it->second;
            ++it;
            if (0 != (ret = ValidateLogSegmentTrailer(
                    name.c_str(), logNums.end() != it))) {
                break;
            }
        }
    }
    return ret;
}

void
Replay::handle(MetaVrLogStartView& op)
{
    if (op.mHandledFlag) {
        return;
    }
    op.mHandledFlag = true;
    if (0 != op.status) {
        if (op.replayFlag) {
            return;
        }
        if (! IsMetaLogWriteOrVrError(op.status)) {
            panic("replay: invalid start view op status");
        }
        return;
    }
    ReplayState&               state = replayTokenizer.GetState();
    ReplayState::EnterAndLeave enterAndLeave(state,
        (op.replayFlag || enqueueFlag) ? 1 : 0);
    state.handleStartView(op);
    if (op.replayFlag) {
        if (0 == op.status) {
            gLayoutManager.SetPrimary(false);
            gLayoutManager.StopServicing();
            primaryNodeId = op.mNodeId;
            if (state.mLastLogAheadSeq == op.mNewLogSeq) {
                // Decrement to account incSeq() at the end of state handle
                // method
                state.mLastLogAheadSeq.mLogSeq--;
            }
        }
    } else {
        if (0 != op.status || ! op.Validate() ||
                state.mViewStartSeq != op.mNewLogSeq ||
                ! state.mCommitQueue.empty()) {
            panic("replay: invalid start view op completion");
            return;
        }
        gLayoutManager.SetPrimary(true);
        gLayoutManager.StartServicing();
        primaryNodeId = -1;
    }
}

void
Replay::setReplayState(
    const MetaVrLogSeq& committed,
    const MetaVrLogSeq& viewStartSeq,
    seq_t               seed,
    int                 status,
    int64_t             errChecksum,
    MetaRequest*        commitQueue,
    const MetaVrLogSeq& lastBlockCommitted,
    fid_t               lastBlockSeed,
    int                 lastBlockStatus,
    int64_t             lastBlockErrChecksum,
    const MetaVrLogSeq& lastNonEmptyViewEndSeq)
{
    ReplayState&               state = replayTokenizer.GetState();
    ReplayState::EnterAndLeave enterAndLeave(state);
    // Enqeue all new ops into replay.
    // Log start view recursion counter now must be 1 when entering
    // handle(MetaVrLogStartView&)
    enqueueFlag = true;
    gLayoutManager.SetPrimary(false);
    gLayoutManager.StopServicing();
    state.setReplayState(
        committed,
        viewStartSeq,
        seed,
        status,
        errChecksum,
        commitQueue,
        lastBlockCommitted,
        lastBlockSeed,
        lastBlockStatus,
        lastBlockErrChecksum,
        lastNonEmptyViewEndSeq
    );
}

bool
Replay::runCommitQueue(
    const MetaVrLogSeq& committed,
    seq_t               seed,
    int64_t             status,
    int64_t             errChecksum)
{
    ReplayState&               state = replayTokenizer.GetState();
    ReplayState::EnterAndLeave enterAndLeave(state);
    const bool okFlag = state.runCommitQueue(
        committed, seed, status, errChecksum);
    if (okFlag) {
        state.mLastNonLogCommit = state.mLastCommittedSeq;
    }
    return okFlag;
}

bool
Replay::commitAll()
{
    ReplayState&               state = replayTokenizer.GetState();
    ReplayState::EnterAndLeave enterAndLeave(state);
    gLayoutManager.SetPrimary(true);
    return replayTokenizer.GetState().commitAll();
}

bool
Replay::enqueue(MetaRequest& req)
{
    ReplayState&               state = replayTokenizer.GetState();
    ReplayState::EnterAndLeave enterAndLeave(state);
    return state.enqueue(req);
}

void
Replay::handle(MetaLogWriterControl& op)
{
    if (0 != op.status || MetaLogWriterControl::kWriteBlock != op.type) {
        return;
    }
    KFS_LOG_STREAM_DEBUG <<
        "replaying: " << op.Show() <<
    KFS_LOG_EOM;
    const int*       lenPtr     = op.blockLines.GetPtr();
    const int* const lendEndPtr = lenPtr + op.blockLines.GetSize();
    while (lenPtr < lendEndPtr) {
        int              lineLen = *lenPtr++;
        IOBuffer::BufPos len     = lineLen;
        const char*      linePtr;
        int              trLen;
        if (lendEndPtr == lenPtr && 0 < (trLen = op.blockTrailer[0] & 0xFF)) {
            if (sizeof(op.blockTrailer) <= (size_t)trLen) {
                const char* const kErrMsg =
                    "replay: invalid write op trailer length";
                panic(kErrMsg);
                op.status    = -EFAULT;
                op.statusMsg = kErrMsg;
                break;
            }
            if (lineLen <= 0) {
                linePtr = op.blockTrailer + 1;
                len     = trLen;
                lineLen = trLen;
            } else {
                lineLen += trLen;
                char* const ptr = buffer.Reserve(lineLen);
                len = op.blockData.CopyOut(ptr, len);
                memcpy(ptr + len, op.blockTrailer + 1, trLen);
                len += trLen;
                linePtr = ptr;
            }
        } else {
            if (len <= 0) {
                continue;
            }
            trLen = 0;
            linePtr = op.blockData.CopyOutOrGetBufPtr(
                buffer.Reserve(lineLen), len);
        }
        if (len != lineLen) {
            const char* const kErrMsg = "replay: invalid write op line length";
            panic(kErrMsg);
            op.status    = -EFAULT;
            op.statusMsg = kErrMsg;
            break;
        }
        const int status = playLine(
            linePtr,
            len,
            lenPtr < lendEndPtr ? seq_t(-1) : op.blockSeq
        );
        if (status != 0) {
            char* const trailerPtr = buffer.Reserve(trLen + 1);
            memcpy(trailerPtr, op.blockTrailer + 1, trLen);
            trailerPtr[trLen] = 0;
            const char* const kErrMsg = "replay: log block apply failure";
            KFS_LOG_STREAM_FATAL <<
                kErrMsg << ":"
                " "         << op.Show() <<
                " commit: " << op.blockCommitted <<
                " status: " << status <<
                " line: "   <<
                    IOBuffer::DisplayData(op.blockData, len - trLen) <<
                    trailerPtr <<
            KFS_LOG_EOM;
            panic(kErrMsg);
            op.status    = -EFAULT;
            op.statusMsg = kErrMsg;
        }
        op.blockData.Consume(len);
    }
}

void
Replay::getLastLogBlockCommitted(
    MetaVrLogSeq& outCommitted,
    fid_t&        outSeed,
    int&          outStatus,
    int64_t&      outErrChecksum) const
{
    const ReplayState& state = replayTokenizer.GetState();
    outCommitted   = state.mLastBlockCommittedSeq;
    outSeed        = state.mLastBlockSeed;
    outStatus      = state.mLastBlockStatus;
    outErrChecksum = state.mLastBlockErrChecksum;
}

MetaVrLogSeq
Replay::getLastNonEmptyViewEndSeq() const
{
    const ReplayState& state = replayTokenizer.GetState();
    return state.mLastNonEmptyViewEndSeq;
}

void
Replay::getReplayCommitQueue(Replay::CommitQueue& queue) const
{
    const ReplayState& state = replayTokenizer.GetState();
    return state.getReplayCommitQueue(queue);
}

void
Replay::updateLastBlockSeed()
{
    ReplayState& state   = replayTokenizer.GetState();
    state.mLastBlockSeed = fileID.getseed();
}

} // namespace KFS
