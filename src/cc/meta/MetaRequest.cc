/*!
 * $Id$
 *
 * \file MetaRequest.cc
 * \brief Meta server request handlers.
 * \author Blake Lewis and Sriram Rao
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

#include "kfstree.h"
#include "MetaRequest.h"
#include "LogWriter.h"
#include "Checkpoint.h"
#include "util.h"
#include "LayoutManager.h"
#include "ChildProcessTracker.h"
#include "NetDispatch.h"
#include "Restorer.h"
#include "AuditLog.h"
#include "ClientSM.h"
#include "Replay.h"
#include "MetaVrOps.h"

#include "kfsio/Globals.h"
#include "kfsio/checksum.h"
#include "kfsio/IOBufferWriter.h"
#include "kfsio/DelegationToken.h"
#include "kfsio/ChunkAccessToken.h"

#include "common/MsgLogger.h"
#include "common/RequestParser.h"
#include "common/IntToString.h"
#include "common/SingleLinkedQueue.h"
#include "common/time.h"
#include "common/kfserrno.h"
#include "common/StringIo.h"

#include "qcdio/QCThread.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <signal.h>
#include <unistd.h>
#include <string.h>

#include <map>
#include <iomanip>
#include <sstream>
#include <limits>
#include <fstream>

namespace KFS {

using std::map;
using std::string;
using std::istringstream;
using std::ifstream;
using std::min;
using std::max;
using std::make_pair;
using std::numeric_limits;
using std::hex;
using std::ofstream;
using KFS::libkfsio::globals;

static bool    gWormMode = false;
static string  gChunkmapDumpDir(".");
static const char* const ftypes[] = { "empty", "file", "dir" };

class StIdempotentRequestHandler
{
public:
    StIdempotentRequestHandler(
        MetaIdempotentRequest& req)
        : mRequestPtr(&req)
    {
        if (0 <= mRequestPtr->reqId &&
                gLayoutManager.GetIdempotentRequestTracker().Handle(
                    *mRequestPtr)) {
            MetaIdempotentRequest* const r = req.GetReq();
            KFS_LOG_STREAM_DEBUG <<
                "idempotent request was already handled:"
                " seq: " << req.opSeqno <<
                " id: "  << req.reqId <<
                " "      << req.Show() <<
                " => "   << MetaRequest::ShowReq(r) <<
            KFS_LOG_EOM;
            mRequestPtr = 0;
            if (! r) {
                panic("invalid idempotent request handling");
                return;
            }
            // If the original request is in the log queue, then this request
            // must go though the log queue to ensure original request
            // completed before submitting response.
            req.logAction = r->commitPendingFlag ?
                MetaRequest::kLogQueue : MetaRequest::kLogNever;
        }
    }
    ~StIdempotentRequestHandler()
    {
        // Remove request and cleanup user id table in order to reduce effect
        // of possible DoS, generating very large number of user ids.
        // For extra safety remove malformed requests.
        if (mRequestPtr &&
                0 <= mRequestPtr->reqId &&
                (-EINVAL == mRequestPtr->status ||
                (kKfsUserNone == mRequestPtr->authUid &&
                (-EACCES == mRequestPtr->status ||
                 -EPERM == mRequestPtr->status)))) {
            gLayoutManager.GetIdempotentRequestTracker().Remove(*mRequestPtr);
        }
    }
    bool IsDone() const { return (! mRequestPtr); }
private:
    MetaIdempotentRequest* mRequestPtr;
};

static ostringstream&
GetTmpOStringStream()
{
    static ostringstream ret;
    ret.str(string());
    resetOStream(ret);
    return ret;
}

static bool
CanAccessFile(const MetaFattr* fa, MetaRequest& op)
{
    if (! fa) {
        op.status    = -ENOENT;
        op.statusMsg = "no such file";
        return false;
    }
    if (fa->IsStriped() && op.clientProtoVers <
            KFS_CLIENT_MIN_STRIPED_FILE_SUPPORT_PROTO_VERS) {
        op.status    = -EINVAL;
        op.statusMsg = "striped file, client upgrade required";
        return false;
    }
    return true;
}

/*!
 * Specially named files (such as, those that end with ".tmp") can be
 * mutated by remove/rename.  Otherwise, in WORM no deletes/renames are allowed.
 */
static inline bool
IsWormMutationAllowed(const string &pathname)
{
    return (pathname.length() >= 4 &&
        pathname.compare(pathname.length() - 4, 4, ".tmp") == 0);
}

static inline bool
startsWith(const string& str, const string& prefix)
{
    const size_t len = prefix.length();
    return (str.length() >= len && str.compare(0, len, prefix) == 0);
}

/*
 * Set WORM mode. In WORM mode, deletes are disabled.
 */
void
setWORMMode(bool value)
{
    gWormMode = value;
}

bool
getWORMMode()
{
    return gWormMode;
}

void
setChunkmapDumpDir(string d)
{
    gChunkmapDumpDir = d;
}

inline static bool
OkHeader(const MetaRequest* op, ReqOstream &os, bool checkStatus = true)
{
    if (op->shortRpcFormatFlag) {
        os << hex;
    }
    os <<
        (op->shortRpcFormatFlag ?
            "OK\r\n"
            "c:" :
            "OK\r\n"
            "Cseq: ") << op->opSeqno
    ;
    if (op->status == 0 && op->statusMsg.empty()) {
        os << (op->shortRpcFormatFlag ?
            "\r\n"
            "s:0\r\n" :
            "\r\n"
            "Status: 0\r\n"
        );
        return true;
    }
    os <<
        (op->shortRpcFormatFlag ?
        "\r\n"
        "s:" :
        "\r\n"
        "Status: "
        ) << (op->status >= 0 ? op->status :
            -SysToKfsErrno(-op->status)) << "\r\n"
    ;
    if (! op->statusMsg.empty()) {
        if (op->statusMsg.find('\r') != string::npos ||
                op->statusMsg.find('\n') != string::npos) {
            panic("invalid status message RPC field: " + op->statusMsg);
        } else {
            os << (op->shortRpcFormatFlag ? "m:" : "Status-message: ") <<
                op->statusMsg <<
            "\r\n";
        }
    }
    if (checkStatus && op->status < 0) {
        os << "\r\n";
    }
    return (op->status >= 0);
}

inline static ReqOstream&
PutHeader(const MetaRequest* op, ReqOstream &os)
{
    OkHeader(op, os, false);
    return os;
}

inline static bool
IsValidUser(kfsUid_t user)
{
    return (user != kKfsUserNone);
}

inline static bool
IsValidGroup(kfsGid_t group)
{
    return (group != kKfsGroupNone);
}

inline static bool
IsValidMode(kfsMode_t mode, bool dirFlag)
{
    return (mode != kKfsModeUndef &&
        (mode & ~(dirFlag ?
            kfsMode_t(Permissions::kDirModeMask) :
            kfsMode_t(Permissions::kFileModeMask))) == 0
    );
}

inline static bool
IsGroupMember(kfsUid_t user, kfsGid_t group)
{
    return IsValidGroup(group); // The client lib does group validation
}

inline static void
SetEUserAndEGroup(MetaRequest& req)
{
    gLayoutManager.SetEUserAndEGroup(req);
}

inline static const UserAndGroupNames*
GetUserAndGroupNames(const MetaRequest& req)
{
    return (
        (req.authUid != kKfsUserNone && req.fromClientSMFlag && req.clnt) ?
        &static_cast<const ClientSM*>(req.clnt
            )->GetAuthContext().GetUserAndGroupNames() : 0
    );
}

inline static void
SetPermissionDeniedStatus(MetaRequest& op)
{
    if (0 <= op.status) {
        op.status   = -EPERM;
        if (op.statusMsg.empty()) {
            op.statusMsg = "permission denied";
        }
    }
}

inline static bool
HasMetaServerAdminAccess(MetaRequest& op)
{
    if (! gLayoutManager.HasMetaServerAdminAccess(op)) {
        SetPermissionDeniedStatus(op);
        return false;
    }
    return true;
}

inline static bool
HasMetaServerStatsAccess(MetaRequest& op)
{
    if (! gLayoutManager.HasMetaServerStatsAccess(op)) {
        SetPermissionDeniedStatus(op);
        return false;
    }
    return true;
}

template<typename T> inline bool
SetUserAndGroup(T& req)
{
    if (req.authUid != kKfsUserNone) {
        if (! req.ownerName.empty()) {
            req.user = gLayoutManager.GetUserAndGroup().GetUserId(
                req.ownerName);
            if (req.user == kKfsUserNone) {
                req.status    = -EINVAL;
                req.statusMsg = "no such user";
                return false;
            }
        }
        if (! req.groupName.empty()) {
            if (! gLayoutManager.GetUserAndGroup().GetGroupId(
                    req.groupName, req.group)) {
                req.status    = -EINVAL;
                req.statusMsg = "no such group";
                return false;
            }
        }
    }
    SetEUserAndEGroup(req);
    if (req.user != kKfsUserNone || req.group != kKfsGroupNone) {
        gLayoutManager.SetUserAndGroup(req, req.user, req.group);
    }
    return true;
}

inline bool
IsAccessOk(const MetaFattr& fa, MetaRequest& req)
{
    if (ROOTFID == fa.id()) {
        return true;
    }
    if (! fa.parent) {
        panic("invalid null parent file attribute");
        req.status = -EFAULT;
        return false;
    }
    if (! fa.parent->CanSearch(req.euser, req.egroup)) {
        req.status = -EPERM;
        return false;
    }
    if (KFS_FILE == fa.type &&
            0 < fa.numReplicas &&
            0 == fa.filesize &&
            0 == (fa.mode & MetaFattr::kFileModeMask) &&
            fa.parent->id() == metatree.getDumpsterDirId()) {
        req.status    = -EPERM;
        req.statusMsg = "file is being deleted,"
            " access modification is not permitted";
        return false;
    }
    return true;
}

inline static void
FattrReply(const MetaFattr* fa, MFattr& ofa)
{
    if (! fa) {
        return;
    }
    ofa = *fa;
    // Keep the following to handle purely theoretical case: "re-open" file
    // for append. Normal append now does not attempt to calculate "the actual"
    // file size as append files are sparse anyway, and always sets file size
    // equal to the begining of the "next" chunk.
    if (fa->filesize < 0 &&
            KFS_FILE == fa->type &&
            fa->chunkcount() > 0 &&
            fa->nextChunkOffset() >= (chunkOff_t)CHUNKSIZE &&
            ! fa->IsStriped()) {
        MetaChunkInfo* ci = 0;
        if (metatree.getalloc(fa->id(),
                fa->nextChunkOffset() - CHUNKSIZE, &ci) == 0 &&
                ci &&
                gLayoutManager.HasWriteAppendLease(ci->chunkId)) {
            // Reduce getlayout calls, return the same value to the
            // client as in the case of getlayout followed by
            // getsize on unstable write append chunk.
            // Chunk servers always return CHUNKSIZE in such case
            // to allow reading file opened for append while the
            // file is being written into.
            ofa.filesize = fa->nextChunkOffset();
        }
    }
}

inline static ReqOstream&
UserAndGroupNamesReply(
    ReqOstream&              os,
    const UserAndGroupNames* ugn,
    kfsUid_t                 user,
    kfsGid_t                 group,
    bool                     shortRpcFmtFlag,
    const char* const        prefix = "")
{
    if (ugn) {
        const string* name = ugn->GetUserName(user);
        os << prefix << (shortRpcFmtFlag ? "UN:" : "UName: ");
        if (name && ! name->empty()) {
            os << *name;
        } else {
            os << user;
        }
        os << "\r\n"
            << prefix << (shortRpcFmtFlag ? "GN:" : "GName: ");
        name = ugn->GetGroupName(group);
        if (name && ! name->empty()) {
            os << *name;
        } else {
            os << group;
        }
        os << "\r\n";
    }
    return os;
}

inline static ReqOstream&
FattrReply(ReqOstream& os, const MFattr& fa, const UserAndGroupNames* ugn,
    bool shortRpcFmtFlag)
{
    os <<
    (shortRpcFmtFlag ? "P:" : "File-handle: ") << fa.id()         << "\r\n" <<
    (shortRpcFmtFlag ? "T:" : "Type: ")        << ftypes[fa.type] << "\r\n" <<
    (shortRpcFmtFlag ? "S:" : "File-size: ")   << fa.filesize     << "\r\n" <<
    (shortRpcFmtFlag ? "R:" : "Replication: ") << fa.numReplicas  << "\r\n";
    if (KFS_FILE == fa.type) {
        os << (shortRpcFmtFlag ? "C:" : "Chunk-count: ") <<
            fa.chunkcount() << "\r\n";
        if (0 == fa.numReplicas) {
            os << (shortRpcFmtFlag ? "NC:" : "Next-chunk-pos: ") <<
                fa.nextChunkOffset() << "\r\n";
        }
    } else if (fa.type == KFS_DIR) {
        os <<
        (shortRpcFmtFlag ? "FC:" : "File-count: ") <<
            fa.fileCount() << "\r\n" <<
        (shortRpcFmtFlag ? "DC:" : "Dir-count: ")  <<
            fa.dirCount()  << "\r\n";
    }
    sendtime(os, (shortRpcFmtFlag ? "MT:" : "M-Time: "),  fa.mtime, "\r\n");
    sendtime(os, (shortRpcFmtFlag ? "CT:" : "C-Time: "),  fa.ctime, "\r\n");
    sendtime(os, (shortRpcFmtFlag ? "CR:" : "CR-Time: "), fa.atime, "\r\n");
    if (fa.IsStriped()) {
        os <<
        (shortRpcFmtFlag ? "ST:" : "Striper-type: ") <<
            int32_t(fa.striperType) << "\r\n" <<
        (shortRpcFmtFlag ? "SN:" : "Num-stripes: ") <<
            fa.numStripes << "\r\n" <<
        (shortRpcFmtFlag ? "SR:" : "Num-recovery-stripes: ") <<
            fa.numRecoveryStripes << "\r\n" <<
        (shortRpcFmtFlag ? "SS:" : "Stripe-size: ") <<
            fa.stripeSize << "\r\n";
    }
    os <<
    (shortRpcFmtFlag ? "U:" : "User: ")  << fa.user  << "\r\n" <<
    (shortRpcFmtFlag ? "G:" : "Group: ") << fa.group << "\r\n" <<
    (shortRpcFmtFlag ? "M:" : "Mode: ")  << fa.mode  << "\r\n";
    if (fa.minSTier < kKfsSTierMax) {
        os <<
        (shortRpcFmtFlag ? "TL:" : "Min-tier: ") <<
            (int)fa.minSTier << "\r\n" <<
        (shortRpcFmtFlag ? "TH:" : "Max-tier: ") <<
            (int)fa.maxSTier << "\r\n";
    }
    if (fa.HasExtAttrs()) {
        os << (shortRpcFmtFlag ? "ET:" : "Ext-attrs-types: ") <<
            fa.GetExtTypes() << "\r\n";
        if (! fa.extAttributes.empty()) {
            os << (shortRpcFmtFlag ? "EA:" : "Ext-attrs: ");
            os.Get() << MakeEscapedStringInserter(fa.extAttributes) << "\r\n";
        }
    }
    return UserAndGroupNamesReply(os, ugn, fa.user, fa.group, shortRpcFmtFlag);
}

template<typename CondT>
class RequestWaitQueue : public ITimeout
{
public:
    RequestWaitQueue(
        NetManager& netManager,
        CondT&      cond,
        bool        ownsCondFlag = false)
        : ITimeout(),
          mCond(cond),
          mQueue(),
          mCur(0),
          mDeletedFlag(0),
          mNetManager(netManager),
          mOwnsCondFlag(ownsCondFlag),
          mMaxOpsPerLoop(2)
        {}
    virtual ~RequestWaitQueue()
    {
        if (! mQueue.IsEmpty()) {
            mNetManager.UnRegisterTimeoutHandler(this);
            // leave requests suspended
        }
        if (mOwnsCondFlag) {
            delete &mCond;
        }
        if (mDeletedFlag) {
            *mDeletedFlag = true;
        }
    }
    virtual void Timeout()
    {
        int  opsCount    = 0;
        bool deletedFlag = false;
        mDeletedFlag = &deletedFlag;
        MetaRequest* req;
        while ((req = mQueue.Front()) && mCond(*req)) {
            mCur = mQueue.PopFront();
            mCur->suspended = false;
            submit_request(mCur);
            if (deletedFlag) {
                return;
            }
            mCur = 0;
            if (mMaxOpsPerLoop <= ++opsCount) {
                if ((req = mQueue.Front()) && mCond(*req)) {
                    mNetManager.Wakeup();
                }
                break;
            }
        }
        mDeletedFlag = 0;
        if (mQueue.IsEmpty()) {
            mNetManager.UnRegisterTimeoutHandler(this);
        }
    }
    void CancelAll()
    {
        MetaRequest* req;
        Queue        queue;
        queue.PushBack(mQueue);
        while ((req = queue.PopFront())) {
            req->suspended = false;
            req->status    = -ECANCELED;
            submit_request(req);
        }
    }
    bool SuspendIfNeeded(MetaRequest& req)
    {
        if (mCur == &req || (! HasPendingRequests() && mCond(req))) {
            return false;
        }
        Add(&req);
        return true;
    }
    void Add(MetaRequest* req)
    {
        if (! req || req->next ||
                mQueue.Front() == req || mQueue.Back() == req) {
            panic("request is null "
                "or already in this or another queue", false);
            return;
        }
        req->suspended = true;
        const bool wasEmptyFlag = mQueue.IsEmpty();
        mQueue.PushBack(*req);
        if (wasEmptyFlag) {
            mNetManager.RegisterTimeoutHandler(this);
        }
    }
    bool HasPendingRequests() const
        { return (! mQueue.IsEmpty()); }
    void Wakeup()
    {
        if (! HasPendingRequests()) {
            return;
        }
        mNetManager.Wakeup();
    }
    void SetParameters(const Properties& props, const char* prefix)
    {
        mMaxOpsPerLoop = props.getValue(
            (prefix ? prefix : "") + string("maxOpsPerLoop"),
            mMaxOpsPerLoop
        );
    }
private:
    typedef SingleLinkedQueue<MetaRequest, MetaRequest::GetNext> Queue;

    CondT&       mCond;
    Queue        mQueue;
    MetaRequest* mCur;
    bool*        mDeletedFlag;
    NetManager&  mNetManager;
    bool         mOwnsCondFlag;
    int          mMaxOpsPerLoop;

    RequestWaitQueue(const RequestWaitQueue&);
    RequestWaitQueue& operator=(const RequestWaitQueue&);
};

class EnoughBuffersCond
{
public:
    bool operator() (MetaRequest& req) const
    {
        return gLayoutManager.HasEnoughFreeBuffers(&req);
    }
};

typedef RequestWaitQueue<EnoughBuffersCond> BuffersWaitQueue;

static BuffersWaitQueue&
MakeBufferWaitQueue()
{
    NetManager& netManager = globalNetManager();
    static EnoughBuffersCond sEnoughBuffersCond;
    static BuffersWaitQueue  sBuffersWaitQueue(netManager, sEnoughBuffersCond);
    return sBuffersWaitQueue;
}

static BuffersWaitQueue& sBuffersWaitQueue = MakeBufferWaitQueue();

void
CheckIfIoBuffersAvailable()
{
    if (sBuffersWaitQueue.HasPendingRequests() &&
            gLayoutManager.HasEnoughFreeBuffers()) {
        sBuffersWaitQueue.Wakeup();
    }
}

void
CancelRequestsWaitingForBuffers()
{
    sBuffersWaitQueue.CancelAll();
}

void
SetRequestParameters(const Properties& props)
{
    sBuffersWaitQueue.SetParameters(props, "metaServer.buffersWaitQueue.");
}

static bool
HasEnoughIoBuffersForResponse(MetaRequest& req)
{
    return (! sBuffersWaitQueue.SuspendIfNeeded(req));
}

class ResponseWOStream : private IOBuffer::WOStream
{
public:
    ResponseWOStream()
        : IOBuffer::WOStream()
        {}
    ostream& Set(IOBuffer& buf)
    {
        return IOBuffer::WOStream::Set(
            buf, gLayoutManager.GetMaxResponseSize());
    }
    void Reset()
        { IOBuffer::WOStream::Reset(); }
};
static ResponseWOStream sWOStream;

/* virtual */ void
MetaLookup::handle()
{
    if (status < 0) {
        return;
    }
    authType = kAuthenticationTypeUndef; // always reset if op gets here.
    SetEUserAndEGroup(*this);
    MetaFattr* fa = 0;
    if ((status = metatree.lookup(dir, name, euser, egroup, fa)) == 0) {
        FattrReply(fa, fattr);
    }
}

/* virtual */ bool
MetaLookup::dispatch(ClientSM& sm)
{
    return sm.Handle(*this);
}

/* virtual */ void
MetaLookupPath::handle()
{
    if (status < 0) {
        return;
    }
    SetEUserAndEGroup(*this);
    MetaFattr* fa = 0;
    if ((status = metatree.lookupPath(
            root, path, euser, egroup, fa)) == 0) {
        FattrReply(fa, fattr);
    }
}

template<typename T> inline static bool
CheckUserAndGroup(T& req)
{
    if (! IsValidUser(req.user)) {
        req.status    = -EINVAL;
        req.statusMsg = "invalid user";
        return false;
    }
    if (! IsValidGroup(req.group)) {
        req.status    = -EINVAL;
        req.statusMsg = "invalid group";
        return false;
    }
    if (req.user != req.euser && req.euser != kKfsUserRoot) {
        req.status    = -EPERM;
        req.statusMsg = "user different from effective user";
        return false;
    }
    if (req.euser != kKfsUserRoot &&
            ! (req.authUid != kKfsUserNone ?
            gLayoutManager.GetUserAndGroup().IsGroupMember(req.user, req.group) :
            IsGroupMember(req.user, req.group))) {
        req.status    = -EPERM;
        req.statusMsg = "user is not a member of the group";
        return false;
    }
    return true;
}

template<typename T> inline static bool
CheckCreatePerms(T& req, bool dirFlag)
{
    if (req.authUid == kKfsUserNone) {
        if (req.euser == kKfsUserNone) {
            req.euser = gLayoutManager.GetDefaultUser();
        }
        if (req.egroup == kKfsGroupNone) {
            req.egroup = gLayoutManager.GetDefaultGroup();
        }
        if (req.user == kKfsUserNone) {
            req.user = req.euser;
        }
        if (req.group == kKfsGroupNone) {
            req.group = req.egroup;
        }
    } else {
        if (req.user == kKfsUserNone) {
            req.user = req.authUid;
        }
        if (req.group == kKfsGroupNone) {
            req.group = req.authGid;
        }
    }
    if (req.mode == kKfsModeUndef) {
        req.mode = req.op == META_MKDIR ?
            gLayoutManager.GetDefaultDirMode() :
            gLayoutManager.GetDefaultFileMode();
    }
    if (! IsValidMode(req.mode, dirFlag)) {
        req.status    = -EINVAL;
        req.statusMsg = "invalid mode";
        return false;
    }
    return CheckUserAndGroup(req);
}

class MetaRequestNull : public MetaRequest
{
public:
    static const MetaRequest& Get()
    {
        static MetaRequestNull sNullReq;
        return sNullReq;
    }
protected:
    MetaRequestNull()
        : MetaRequest(META_NUM_OPS_COUNT, kLogNever)
        {}
    virtual ostream& ShowSelf(ostream& os) const
        { return os << "null"; }
};

/* static */ const MetaRequest&
MetaRequest::GetNullReq()
{
    return MetaRequestNull::Get();
}

/* static */ bool
MetaRequest::Initialize()
{
    const bool initedFlag = false;
    if (initedFlag) {
        return true;
    }
    if (0 != sMetaRequestCount) {
        panic("invalid meta request initialize attempt");
        return false;
    }
    MetaRequestsList::Init(sMetaRequestsPtr);
    GetNullReq();
    MetaChunkRequest::MakeNullIterator();
    return true;
}

inline bool
MetaIdempotentRequest::IsHandled()
{
    if (replayFlag) {
        if (req) {
            panic("in-flight idempotent in replay");
            return true;
        }
        // Set submit time to make expiration of idempotent tracker entries work.
        submitTime = globalNetManager().NowUsec();
        if (gLayoutManager.GetIdempotentRequestTracker().Handle(*this)) {
            panic("handled/duplicate idempotent in replay");
            return true;
        }
    } else {
        if (req && req != this) {
            if (IsMetaLogWriteOrVrError(status)) {
                // Detach from pending request, the client must retry in order
                // to determine the status, after this or other node becomes
                // primary.
                SetReq(0);
                ackId = -1;
                return true;
            }
            if (req->seqno < 0 || req->commitPendingFlag) {
                panic("invalid in-flight idempotent request");
            }
            return true;
        }
        if (! logseq.IsValid() || IsMetaLogWriteOrVrError(status)) {
            // Remove RPC from the tracker, as otherwise ACK reply will
            // fail in replay due to missing log record, but succeeds at
            // run time (now).
            gLayoutManager.GetIdempotentRequestTracker().Remove(*this);
            ackId = -1; // Do not request client to ACK.
        }
    }
    return (0 != status);
}

const string kInvalidChunksPath("/proc/invalid_chunks");
const string kInvalidChunksPrefix(kInvalidChunksPath + "/");

/* virtual */ bool
MetaCreate::start()
{
    if (! SetUserAndGroup(*this)) {
        return false;
    }
    StIdempotentRequestHandler handler(*this);
    if (handler.IsDone()) {
        return true;
    }
    if (0 != status) {
        return false;
    }
    const bool invalChunkFlag = dir == ROOTFID &&
        startsWith(name, kInvalidChunksPrefix);
    if (invalChunkFlag) {
        name = name.substr(kInvalidChunksPrefix.length());
        const char* chunk = name.c_str();
        for (const char* p = chunk; ; p++) {
            const int sym = *p & 0xff;
            if ((sym < '0' || sym > '9') && sym != '.') {
                if (sym == 0 && p > chunk) {
                    break;
                }
                statusMsg = "invalid chunk id: " + name;
                status    = -EINVAL;
                return false;
            }
        }
        const char* ptr     = name.data();
        const char* end     = ptr + name.size();
        chunkId_t   chunkId = -1;
        if (! DecIntParser::Parse(ptr, end - ptr, chunkId) || ptr != end) {
            statusMsg = "invalid chunk id: " + name;
            status    = -EINVAL;
            return false;
        }
        fid_t            fid = -1;
        const MetaFattr* cfa = 0;
        if (! gLayoutManager.GetChunkFileId(chunkId, fid, 0, &cfa, 0) ||
                ! cfa) {
            status    = -ENOENT;
            statusMsg = "no such chunk";
            return false;
        }
        string msg("detected invalid chunk: " + name);
        msg += " file ";
        AppendDecIntToString(msg, fid);
        msg += " eof: ";
        AppendDecIntToString(msg, cfa->filesize);
        KFS_LOG_STREAM_ERROR << msg << KFS_LOG_EOM;
        if (cfa->filesize <= 0) {
            // Ignore null size files for now, most likely it is abandoned file.
            return false;
        }
        if (gLayoutManager.GetPanicOnInvalidChunkFlag()) {
            panic(msg);
        }
        MetaFattr* fa = 0;
        if ((status = metatree.lookupPath(ROOTFID, kInvalidChunksPath,
                    kKfsUserRoot, kKfsGroupRoot, fa)) != 0
                || ! fa || fa->type != KFS_DIR) {
            if (status == 0) {
                status    = -ENOENT;
                statusMsg = kInvalidChunksPath +
                    ": no such directory";
            }
            return false;
        }
        dir = fa->id();
        if (user == kKfsUserNone && authUid == kKfsUserNone) {
            user  = euser  != kKfsUserNone  ? euser  : kKfsUserRoot;
            group = egroup != kKfsGroupNone ? egroup : kKfsGroupRoot;
        }
        if (authUid == kKfsUserNone) {
            euser = kKfsUserRoot;
        }
        mode     = 0;
        minSTier = kKfsSTierMax;
        maxSTier = kKfsSTierMax;
    } else {
        const bool kDirFlag = false;
        if (! CheckCreatePerms(*this, kDirFlag)) {
            return false;
        }
    }

    if (! invalChunkFlag && gWormMode && ! IsWormMutationAllowed(name)) {
        // Do not create a file that we can not write into.
        statusMsg = "worm mode";
        status    = -EPERM;
        return false;
    }
    fid = 0;
    const bool wasNotObjectStoreFileFlag = 0 < numReplicas;
    if (striperType != KFS_STRIPED_FILE_TYPE_NONE && 0 < numRecoveryStripes) {
        numReplicas = min(numReplicas,
            gLayoutManager.GetMaxReplicasPerRSFile());
    } else {
        numReplicas = min(numReplicas, gLayoutManager.GetMaxReplicasPerFile());
    }
    if (0 == numReplicas && wasNotObjectStoreFileFlag &&
           gLayoutManager.IsObjectStoreEnabled()) {
        // Convert to object store file if meta server is configured to do
        // so.
        striperType        = KFS_STRIPED_FILE_TYPE_NONE;
        numRecoveryStripes = 0;
        numStripes         = 0;
        stripeSize         = 0;
        if (minSTier < kKfsSTierMax) {
            maxSTier = minSTier; // No storage tier range.
        }
    }
    if (maxSTier < minSTier ||
            ! IsValidSTier(minSTier) ||
            ! IsValidSTier(maxSTier)) {
        status    = -EINVAL;
        statusMsg = "invalid storage tier range";
        return false;
    }
    if (minSTier < kKfsSTierMax && 0 == numReplicas && minSTier != maxSTier) {
        status    = -EINVAL;
        statusMsg =
            "storage tier range is not supported with object store files";
        return false;
    }
    if (! gLayoutManager.Validate(*this)) {
        if (0 <= status) {
            status = -EINVAL;
        }
        return false;
    }
    if (0 == status) {
        mtime = microseconds();
    }
    return (0 == status);
}

/* virtual */ void
MetaCreate::handle()
{
    if (IsHandled()) {
        return;
    }
    fid = 0;
    MetaFattr* fa = 0;
    bool const kToDumpsterFlag = true;
    status = metatree.create(
        dir,
        name,
        &fid,
        numReplicas,
        exclusive,
        striperType,
        numStripes,
        numRecoveryStripes,
        stripeSize,
        user,
        group,
        mode,
        euser,
        egroup,
        &fa,
        mtime,
        kToDumpsterFlag
    );
    if (status == 0) {
        if (! fa || fa->id() <= 0) {
            panic("invalid file attributes");
            status = -EFAULT;
            return;
        }
        if (minSTier < kKfsSTierMax) {
            fa->minSTier = minSTier;
            fa->maxSTier = maxSTier;
        } else {
            minSTier = fa->minSTier;
            maxSTier = fa->maxSTier;
        }
    }
}

/* virtual */ bool
MetaMkdir::start()
{
    if (! SetUserAndGroup(*this)) {
        return false;
    }
    StIdempotentRequestHandler handler(*this);
    if (handler.IsDone()) {
        return true;
    }
    if (0 != status) {
        return false;
    }
    const bool kDirFlag = true;
    if (! CheckCreatePerms(*this, kDirFlag)) {
        return false;
    }
    if (0 == status) {
        mtime = microseconds();
    }
    return (0 == status);
}

/* virtual */ void
MetaMkdir::handle()
{
    if (IsHandled()) {
        return;
    }
    fid = 0;
    MetaFattr* fa = 0;
    status = metatree.mkdir(
        dir, name, user, group, mode, euser, egroup, &fid, &fa, mtime);
    if (status == 0 && fa) {
        minSTier = fa->minSTier;
        maxSTier = fa->maxSTier;
    }
}

static int
LookupAbsPath(fid_t& dir, string& name, kfsUid_t euser, kfsGid_t egroup)
{
    if (dir != ROOTFID || name.empty() ||
            *name.begin() != '/' || *name.rbegin() == '/') {
        return 0;
    }
    const size_t nameStart = name.rfind('/');
    size_t pos = nameStart;
    while (pos > 0 && name[pos-1] == '/') {
        pos--;
    }
    if (pos == 0) {
        name = name.substr(nameStart + 1);
    } else {
        const string parentDir = name.substr(0, pos);
        MetaFattr*   fa        = 0;
        const int status = metatree.lookupPath(ROOTFID, parentDir,
            euser, egroup, fa);
        if (status != 0) {
            return status;
        }
        if (fa && fa->type == KFS_DIR) {
            dir  = fa->id();
            name = name.substr(nameStart + 1);
        }
    }
    return 0;
}

/*!
 * \brief Remove a file in a directory.  Also, remove the chunks
 * associated with the file.  For removing chunks, we send off
 * RPCs to the appropriate chunkservers.
 */

/* virtual */ bool
MetaRemove::start()
{
    if (gWormMode && ! IsWormMutationAllowed(name)) {
        // deletes are disabled in WORM mode except for specially named
        // files
        statusMsg = "worm mode";
        status    = -EPERM;
        return false;
    }
    SetEUserAndEGroup(*this);
    StIdempotentRequestHandler handler(*this);
    if (handler.IsDone()) {
        return true;
    }
    if (0 == status) {
        mtime = microseconds();
    }
    return (0 == status);
}

/* virtual */ void
MetaRemove::handle()
{
    if (IsHandled()) {
        return;
    }
    if ((status = LookupAbsPath(dir, name, euser, egroup)) != 0) {
        return;
    }
    bool const kToDumpsterFlag = true;
    status = metatree.remove(dir, name, pathname, euser, egroup, mtime,
        kToDumpsterFlag);
}

/* virtual */ bool
MetaRmdir::start()
{
    if (gWormMode && ! IsWormMutationAllowed(name)) {
        // deletes are disabled in WORM mode
        statusMsg = "worm mode";
        status    = -EPERM;
        return false;
    }
    SetEUserAndEGroup(*this);
    StIdempotentRequestHandler handler(*this);
    if (handler.IsDone()) {
        return true;
    }
    if (0 == status) {
        mtime = microseconds();
    }
    return (0 == status);
}

/* virtual */ void
MetaRmdir::handle()
{
    if (IsHandled()) {
        return;
    }
    if ((status = LookupAbsPath(dir, name, euser, egroup)) != 0) {
        return;
    }
    status = metatree.rmdir(dir, name, pathname, euser, egroup, mtime);
}

static vector<MetaDentry*>&
GetReadDirTmpVec()
{
    static vector<MetaDentry*> sReaddirRes;
    sReaddirRes.clear();
    sReaddirRes.reserve(1024);
    return sReaddirRes;
}

inline const MetaFattr*
GetDirAttr(fid_t dir, const vector<MetaDentry*>& v)
{
    const MetaFattr* fa = v.empty() ? 0 : (v.front()->getName() == ".." ?
        v.back()->getFattr() : v.front()->getFattr());
    if (fa && fa->id() != dir) {
        fa = fa->parent;
        if (fa && fa->id() != dir) {
            fa = 0;
        }
    }
    return (fa ? fa : metatree.getFattr(dir));
}

/* virtual */ void
MetaReaddir::handle()
{
    if (atimeInFlightFlag) {
        atimeInFlightFlag = false;
        return;
    }
    if (status < 0) {
        return;
    }
    if (! HasEnoughIoBuffersForResponse(*this)) {
        return;
    }
    const bool oldFormatFlag = numEntries < 0;
    int maxEntries = gLayoutManager.GetReadDirLimit();
    if (numEntries > 0 &&
            (maxEntries <= 0 || numEntries < maxEntries)) {
        maxEntries = numEntries;
    }
    numEntries = 0;
    resp.Clear();
    vector<MetaDentry*>& v = GetReadDirTmpVec();
    if ((status = fnameStart.empty() ?
            metatree.readdir(dir, v,
                maxEntries, &hasMoreEntriesFlag) :
            metatree.readdir(dir, fnameStart, v,
                maxEntries, hasMoreEntriesFlag)
            ) != 0) {
        if (status == -ENOENT) {
            const MetaFattr* const fa = metatree.getFattr(dir);
            if (fa && fa->type != KFS_DIR) {
                status = -ENOTDIR;
            }
        }
        return;
    }
    SetEUserAndEGroup(*this);
    const MetaFattr* const fa = GetDirAttr(dir, v);
    if (! fa || ! fa->CanRead(euser, egroup)) {
        status = -EACCES;
        return;
    }
    if (oldFormatFlag && hasMoreEntriesFlag) {
        status     = -ENOMEM;
        statusMsg  = "response exceeds max. allowed number of entries"
            " consider updating kfs client lib";
        return;
    }
    const int extSize = IOBufferData::GetDefaultBufferSize() +
        int(MAX_FILE_NAME_LENGTH);
    int       maxSize = gLayoutManager.GetMaxResponseSize();
    if (! oldFormatFlag && extSize * 2 < maxSize) {
        maxSize -= extSize;
    }
    IOBufferWriter writer(resp);
    vector<MetaDentry*>::const_iterator it;
    for (it = v.begin();
            it != v.end() && writer.GetSize() <= maxSize;
            ++it) {
        const string& name = (*it)->getName();
        // Supress "/" dentry for "/".
        if (dir == ROOTFID && name == "/") {
            continue;
        }
        writer.Write(name);
        writer.Write("\n", 1);
        ++numEntries;
    }
    writer.Close();
    if (resp.BytesConsumable() > maxSize) {
        if (oldFormatFlag) {
            resp.Clear();
            numEntries = 0;
            status     = -ENOMEM;
            statusMsg  = "response exceeds max. size";
        } else {
            if (it != v.end()) {
                hasMoreEntriesFlag = true;
            }
        }
    }
    if (0 == status) {
        gLayoutManager.UpdateATime(fa, *this);
    }
}

class EnumerateLocations
{
    ServerLocations& v;
public:
    EnumerateLocations(ServerLocations& result)
        : v(result)
        {}
    void operator()(const ChunkServerPtr& c) const
    {
        v.push_back(c->GetServerLocation());
    }
};

class EnumerateLocationsShortRpc
{
    ServerLocations& v;
    bool&            shortRpcFlag;
public:
    EnumerateLocationsShortRpc(ServerLocations& result, bool& flag)
        : v(result),
          shortRpcFlag(flag)
        {}
    void operator()(const ChunkServerPtr& c) const
    {
        v.push_back(c->GetServerLocation());
        shortRpcFlag = shortRpcFlag && c->IsShortRpcFormat();
    }
};


class ListServerLocations
{
    ReqOstream& os;
public:
    ListServerLocations(ReqOstream &out)
        : os(out)
        {}
    void operator()(const ServerLocation& s) const
    {
        os << " " << s;
    }
};

template<bool ShortFormatFlag>
class ReaddirPlusWriter
{
public:
    typedef MetaReaddirPlus::DEntry   DEntry;
    typedef MetaReaddirPlus::DEntries DEntries;
    typedef MetaReaddirPlus::CInfos   CInfos;

    void write(const char* data, size_t len)
    {
        Write(data, len);
    }
private:
    enum { kNumBufSize = 64 };

    typedef LayoutManager::Servers     Servers;
    typedef PropertiesTokenizer::Token Token;

    class PropName : public Token
    {
    public:
        PropName(const char* shortName, const char* longName)
            : Token(ShortFormatFlag ? shortName : longName)
            {}
    };

    typedef LinearHash<
        KeyOnly<uint64_t>,
        KeyCompare<uint64_t>,
        DynamicArray<SingleLinkedList<KeyOnly<uint64_t> >*, 10>,
        StdFastAllocator<KeyOnly<uint64_t> >
    > IdSet;

    IOBufferWriter writer;
    const int      maxSize;
    const bool     getLastChunkInfoOnlyIfSizeUnknown;
    const bool     omitLastChunkInfoFlag;
    const bool     fileIdAndTypeOnlyFlag;
    char* const    nBufEnd;
    kfsUid_t       prevUid;
    kfsGid_t       prevGid;
    bool           firstEntryFlag;
    bool           insertPrevGidFlag;
    bool           insertPrevUidFlag;
    IdSet          ugids;
    char           nBuf[kNumBufSize];

    static const PropName kMtime;
    static const PropName kCtime;
    static const PropName kCrtime;
    static const PropName kBeginEntry;
    static const PropName kName;
    static const PropName kId;
    static const PropName kType;
    static const PropName kNL;
    static const PropName kSType;
    static const PropName kSCount;
    static const PropName kSRecov;
    static const PropName kSSize;
    static const PropName kCCnt;
    static const PropName kFSize;
    static const PropName kRepl;
    static const PropName kUser;
    static const PropName kGroup;
    static const PropName kMode;
    static const PropName kLOff;
    static const PropName kLId;
    static const PropName kLVers;
    static const PropName kLRCnt;
    static const PropName kLRepl;
    static const PropName kFileCount;
    static const PropName kDirCount;
    static const PropName kMinTier;
    static const PropName kMaxTier;
    static const PropName kSpace;
    static const PropName kUserName;
    static const PropName kGroupName;
    static const PropName kExtAttrsType;
    static const PropName kExtAttrs;
    static const Token    kFileType[];

    template<typename T> static
    char* ToString(T val, char* bufEnd)
    {
        return (ShortFormatFlag ?
            IntToHexString(val, bufEnd) :
            IntToDecString(val, bufEnd)
        );
    }
    void Write(const Token& name)
    {
        Write(name.mPtr, name.mLen);
    }
    template<typename T>
    void WriteInt(T val)
    {
        const char* const b = ToString(val, nBufEnd);
        Write(b, nBufEnd - b);
    }
    void WriteTime(int64_t t)
    {
        if (ShortFormatFlag) {
            WriteInt(t);
            return;
        }
        const int64_t kMicroseconds = 1000 * 1000;
        char* p = ToString(t % kMicroseconds, nBufEnd);
        *--p = ' ';
        p = ToString(t / kMicroseconds, p);
        Write(p, nBufEnd - p);
    }
    void Write(const char* data, size_t len)
    {
        writer.Write(data, len);
    }
    void Write(const string& str)
    {
        writer.Write(str);
    }
    bool IsNewGroup(kfsGid_t gid)
    {
        if (prevGid == gid) {
            return false;
        }
        const uint64_t kGroupBit = uint64_t(1) << sizeof(kfsGid_t) * 8;
        bool           ret       = false;
        if (insertPrevGidFlag) {
            const uint64_t id = prevGid | kGroupBit;
            ugids.Insert(id, id, ret);
            ret = false;
            insertPrevGidFlag = false;
        }
        prevGid = gid;
        const uint64_t id = gid | kGroupBit;
        ugids.Insert(id, id, ret);
        return ret;
    }
    bool IsNewUser(kfsUid_t uid)
    {
        if (prevUid == uid) {
            return false;
        }
        bool ret = false;
        if (insertPrevUidFlag) {
            ugids.Insert(prevUid, prevUid, ret);
            ret = false;
            insertPrevUidFlag = false;
        }
        prevUid = uid;
        ugids.Insert(uid, uid, ret);
        return ret;
    }
    void Write(const DEntry& entry, CInfos::const_iterator& lci,
            bool noAttrsFlag, const UserAndGroupNames* ugn)
    {
        Write(kBeginEntry);
        Write(kName);
        Write(entry.name);
        if (noAttrsFlag) {
            Write(kNL);
            return;
        }
        Write(kId);
        WriteInt(entry.id());
        Write(kType);
        Write(kFileType[entry.type]);
        // Always write symbolic link info as it is implicit file type.
        if (entry.HasExtAttrs()) {
            Write(kExtAttrsType);
            WriteInt(entry.GetExtTypes());
            if (! entry.extAttributes.empty()) {
                Write(kExtAttrs);
                StringIo::Escape(entry.extAttributes.data(),
                    entry.extAttributes.size(), *this);
            }
        }
        if (fileIdAndTypeOnlyFlag) {
            Write(kNL);
            return;
        }
        Write(kMtime);
        WriteTime(entry.mtime);
        if (! ShortFormatFlag || entry.ctime != entry.atime) {
            Write(kCtime);
            WriteTime(entry.ctime);
        }
        Write(kCrtime);
        WriteTime(entry.atime);
        Write(kUser);
        WriteInt(entry.user);
        Write(kGroup);
        WriteInt(entry.group);
        Write(kMode);
        WriteInt(entry.mode);
        if (ugn) {
            if (firstEntryFlag || IsNewUser(entry.user)) {
                const string* const name = ugn->GetUserName(entry.user);
                if (name) {
                    Write(kUserName);
                    Write(*name);
                }
            }
            if (firstEntryFlag || IsNewGroup(entry.group)) {
                const string* const name = ugn->GetGroupName(entry.group);
                if (name) {
                    Write(kGroupName);
                    Write(*name);
                }
            }
            if (firstEntryFlag) {
                firstEntryFlag = false;
                prevUid = entry.user;
                prevGid = entry.group;
                insertPrevUidFlag = true;
                insertPrevGidFlag = true;
            }
        }
        if (entry.type == KFS_DIR) {
            if (entry.filesize >= 0) {
                Write(kFSize);
                WriteInt(entry.filesize);
            }
            Write(kFileCount);
            WriteInt(entry.fileCount());
            Write(kDirCount);
            WriteInt(entry.dirCount());
            Write(kNL);
            return;
        }
        if (entry.IsStriped()) {
            Write(kSType);
            WriteInt(int(entry.striperType));
            Write(kSCount);
            WriteInt(entry.numStripes);
            Write(kSRecov);
            WriteInt(entry.numRecoveryStripes);
            Write(kSSize);
            WriteInt(entry.stripeSize);
        }
        Write(kCCnt);
        WriteInt(entry.chunkcount());
        Write(kFSize);
        WriteInt(entry.filesize);
        Write(kRepl);
        WriteInt(entry.numReplicas);
        if (entry.minSTier < kKfsSTierMax &&
                (KFS_FILE == entry.type || KFS_DIR == entry.type)) {
            Write(kMinTier);
            WriteInt(entry.minSTier);
            Write(kMaxTier);
            WriteInt(entry.maxSTier);
        }
        if (omitLastChunkInfoFlag ||
                entry.type == KFS_DIR || entry.IsStriped() ||
                (getLastChunkInfoOnlyIfSizeUnknown &&
                entry.filesize >= 0)) {
            Write(kNL);
            return;
        }
        const ChunkLayoutInfo& lc = *lci++;
        if (lc.chunkId <= 0) {
            Write(kNL);
            return;
        }
        // Tell the client the info about the last chunk of the
        // file. If the file size is not known then, client can use this
        // info to calculate the size. The most common case is when the
        // chunk is being written into, and the file is not striped.
        Write(kLOff);
        WriteInt(lc.offset);
        Write(kLId);
        WriteInt(lc.chunkId);
        Write(kLVers);
        WriteInt(lc.chunkVersion);
        Write(kLRCnt);
        WriteInt(lc.locations.size());
        Write(kLRepl);
        for (ServerLocations::const_iterator it = lc.locations.begin();
                it != lc.locations.end();
                ++it) {
            Write(kSpace);
            Write(it->hostname);
            Write(kSpace);
            WriteInt(it->port);
        }
        Write(kNL);
    }
    ReaddirPlusWriter(const ReaddirPlusWriter&);
    ReaddirPlusWriter& operator=(const ReaddirPlusWriter&);
public:
    ReaddirPlusWriter(IOBuffer& b, int ms, bool f, bool olcif, bool fidtof)
        : writer(b),
          maxSize(ms),
          getLastChunkInfoOnlyIfSizeUnknown(f),
          omitLastChunkInfoFlag(olcif),
          fileIdAndTypeOnlyFlag(fidtof),
          nBufEnd(nBuf + kNumBufSize - 1),
          prevUid(kKfsUserNone),
          prevGid(kKfsGroupNone),
          firstEntryFlag(true),
          insertPrevGidFlag(false),
          insertPrevUidFlag(false),
          ugids()
        {}
    size_t Write(const DEntries& entries, const CInfos& cinfos,
            bool noAttrsFlag, const UserAndGroupNames* ugn)
    {
        ugids.Clear();
        firstEntryFlag = true;
        CInfos::const_iterator   cit = cinfos.begin();
        DEntries::const_iterator it;
        for (it = entries.begin();
                it != entries.end() &&
                    writer.GetSize() <= maxSize;
                ++it) {
            Write(*it, cit, noAttrsFlag, ugn);
        }
        if (cinfos.end() < cit) {
            panic("dentry and last chunk info mismatch", false);
        }
        writer.Close();
        return (it - entries.begin());
    }
};

template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kMtime(
    "\nM:"  , "\r\nM-Time: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kCtime(
    "\nC:"  , "\r\nC-Time: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kCrtime(
    "\nCR:" , "\r\nCR-Time: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kBeginEntry(
    "B"  , "Begin-entry");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kName(
    "\nN:" , "\r\nName: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kId(
    "\nH:" , "\r\nFile-handle: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kType(
    "\nT:" , "\r\nType: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kNL(
    "\n" , "\r\n");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kSType(
    "\nST:" , "\r\nStriper-type: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kSCount(
    "\nSC:" , "\r\nNum-stripes: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kSRecov(
    "\nSR:" , "\r\nNum-recovery-stripes: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kSSize(
    "\nSS:" , "\r\nStripe-size: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kCCnt(
    "\nCC:" , "\r\nChunk-count: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kFSize(
    "\nS:"  , "\r\nFile-size: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kRepl(
    "\nR:"  , "\r\nReplication: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kUser(
    "\nU:"  , "\r\nUser: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kGroup(
    "\nG:"  , "\r\nGroup: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kMode(
    "\nA:"  , "\r\nMode: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kLOff(
    "\nLO:" , "\r\nChunk-offset: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kLId(
    "\nLH:" , "\r\nChunk-handle: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kLVers(
    "\nLV:" , "\r\nChunk-version: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kLRCnt(
    "\nLN:" , "\r\nNum-replicas: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kLRepl(
    "\nLR:" , "\r\nReplicas:");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kFileCount(
    "\nFC:" , "\r\nFile-count: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kDirCount(
    "\nDC:" , "\r\nDir-count: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kMinTier(
    "\nFT:" , "\r\nMin-tier: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kMaxTier(
    "\nLT:" , "\r\nMax-tier: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kSpace(
    " " , " ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kUserName(
    "\nUN:" , "\r\nUser-name: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kGroupName(
    "\nGN:" , "\r\nGroup-name: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kExtAttrsType(
    "\nET:" , "\r\nExt-attrs-types: ");
template<bool F> const typename ReaddirPlusWriter<F>::PropName
    ReaddirPlusWriter<F>::kExtAttrs(
    "\nEA:" , "\r\nExt-attrs: ");
template<bool F> const typename ReaddirPlusWriter<F>::Token
    ReaddirPlusWriter<F>::kFileType[] = { "empty", "file", "dir" };

MetaReaddirPlus::~MetaReaddirPlus()
{
    if (ioBufPending > 0) {
        gLayoutManager.ChangeIoBufPending(-ioBufPending);
    }
}

/* virtual */ void
MetaReaddirPlus::handle()
{
    if (atimeInFlightFlag) {
        atimeInFlightFlag = false;
        return;
    }
    if (status < 0) {
        return;
    }
    if (! HasEnoughIoBuffersForResponse(*this)) {
        return;
    }
    dentries.clear();
    lastChunkInfos.clear();
    if (ioBufPending > 0) {
        gLayoutManager.ChangeIoBufPending(-ioBufPending);
    }
    ioBufPending = 0;
    int maxEntries = gLayoutManager.GetReadDirLimit();
    if (numEntries > 0 &&
            (maxEntries <= 0 || numEntries < maxEntries)) {
        maxEntries = numEntries;
    }
    vector<MetaDentry*>& res = GetReadDirTmpVec();
    if ((status = fnameStart.empty() ?
            metatree.readdir(dir, res,
                maxEntries, &hasMoreEntriesFlag) :
            metatree.readdir(dir, fnameStart, res,
                maxEntries, hasMoreEntriesFlag)
            ) != 0) {
        if (status == -ENOENT) {
            MetaFattr * const fa = metatree.getFattr(dir);
            if (fa && fa->type != KFS_DIR) {
                status = -ENOTDIR;
            }
        }
        return;
    }
    const MetaFattr* const fa = GetDirAttr(dir, res);
    SetEUserAndEGroup(*this);
    if (! fa || ! fa->CanRead(euser, egroup)) {
        status = -EACCES;
        return;
    }
    noAttrsFlag      = ! fa->CanSearch(euser, egroup);
    if (numEntries < 0 && hasMoreEntriesFlag) {
        status     = -ENOMEM;
        statusMsg  = "response exceeds max. allowed number of entries"
            " consider updating kfs client lib";
        return;
    }
    maxRespSize = max(0, gLayoutManager.GetMaxResponseSize());
    const int    extSize = IOBufferData::GetDefaultBufferSize() +
        int(MAX_FILE_NAME_LENGTH);
    const size_t maxSize =
        (size_t)((numEntries >= 0 && extSize * 2 < maxRespSize) ?
        maxRespSize - extSize : maxRespSize);
    dentries.reserve(res.size());
    const size_t avgEntrySz[] = {
        148, 272, 64,
         64, 128, 24,
         36, 36,  64,
         28, 28,  24,
    };
    int idx = (fileIdAndTypeOnlyFlag ? 6 : 0) + (numEntries < 0 ? 0 : 3);
    const size_t avgDirExtraSize  = avgEntrySz[idx++];
    const size_t avgFileExtraSize = avgEntrySz[idx++];
    const size_t avgChunkInfoSize = avgEntrySz[idx++];
    const size_t avgLocationSize  = 22;
    size_t responseSize = 0;
    vector<MetaDentry*>::const_iterator it;
    for (it = res.begin();
            it != res.end() && responseSize <= maxSize;
            ++it) {
        const MetaDentry* const entry = *it;
        const MetaFattr*  const fa    = metatree.getFattr(entry);
        if (! fa) {
            continue;
        }
        // Supress "/" dentry for "/".
        const string& name = entry->getName();
        if (fa->id() == ROOTFID && name == "/") {
            continue;
        }
        responseSize += name.length() + (fa->type == KFS_DIR ?
            avgDirExtraSize : avgFileExtraSize);
        dentries.push_back(DEntry(*fa, name));
        if (omitLastChunkInfoFlag || fileIdAndTypeOnlyFlag ||
                noAttrsFlag || fa->type == KFS_DIR || fa->IsStriped() ||
                (getLastChunkInfoOnlyIfSizeUnknown &&
                fa->filesize >= 0)) {
            continue;
        }
        responseSize += avgChunkInfoSize;
        lastChunkInfos.push_back(ChunkLayoutInfo());
        ChunkLayoutInfo& lc = lastChunkInfos.back();
        // for a file, get the layout and provide location of last chunk
        // so that the client can compute filesize
        MetaChunkInfo* lastChunk = 0;
        MetaFattr*     cfa       = 0;
        if (metatree.getLastChunkInfo(fa->id(), cfa, lastChunk) != 0 ||
                ! lastChunk) {
            lc.offset       = -1;
            lc.chunkId      = -1;
            lc.chunkVersion = -1;
            continue;
        }
        if (fa != cfa) {
            panic("readdirplus: file attribute mismatch", false);
        }
        lc.offset       = lastChunk->offset;
        lc.chunkId      = lastChunk->chunkId;
        lc.chunkVersion = lastChunk->chunkVersion;
        Servers    c;
        MetaFattr* lfa = 0;
        if (gLayoutManager.GetChunkToServerMapping(
                *lastChunk, c, lfa) != 0) {
            // All the servers hosting the chunk are down.
            continue;
        }
        lc.locations.reserve(c.size());
        for_each(c.begin(), c.end(), EnumerateLocations(lc.locations));
        responseSize += lc.locations.size() * avgLocationSize;
    }
    if (maxSize < responseSize) {
        if (numEntries < 0) {
            status    = -ENOMEM;
            statusMsg = "response exceeds max. size";
            lastChunkInfos.clear();
            dentries.clear();
            responseSize = 0;
        } else if (it != res.end()) {
            hasMoreEntriesFlag = true;
        }
    }
    ioBufPending = (int64_t)responseSize;
    if (ioBufPending > 0) {
        gLayoutManager.ChangeIoBufPending(ioBufPending);
        maxRespSize = (int)max((int64_t)maxRespSize, ioBufPending +
            IOBufferData::GetDefaultBufferSize());
    }
    if (0 == status) {
        gLayoutManager.UpdateATime(fa, *this);
    }
}

/*!
 * \brief Get the allocation information for a specific chunk in a file.
 */
/* virtual */ void
MetaGetalloc::handle()
{
    if (status < 0) {
        return;
    }
    if (offset < 0) {
        status    = -EINVAL;
        statusMsg = "negative offset";
        return;
    }
    MetaFattr* fa  = 0;
    int        err = 0;
    Servers    c;
    replicasOrderedFlag = false;
    if (objectStoreFlag) {
        if (! (fa = metatree.getFattr(fid))) {
            status    = -ENOENT;
            statusMsg = "no such file";
            return;
        }
        if (KFS_FILE != fa->type) {
            status    = -EISDIR;
            statusMsg = "not a file";
            return;
        }
        if (0 != fa->numReplicas) {
            status    = -EINVAL;
            statusMsg = "not an object store file";
            return;
        }
        if (fa->nextChunkOffset() <= offset) {
            status    = -EINVAL;
            statusMsg = "past end of file position";
            return;
        }
        gLayoutManager.GetAccessProxy(*this, c);
        if (c.empty()) {
            status    = -EAGAIN;
            statusMsg = "no access proxy available on host: " + clientIp;
            return;
        }
        chunkId      = fid;
        chunkVersion =
            -(seq_t)(chunkStartOffset(offset) + fa->maxSTier) - 1;
    } else {
        MetaChunkInfo* chunkInfo = 0;
        status = metatree.getalloc(fid, offset, &chunkInfo);
        if (status != 0) {
            KFS_LOG_STREAM_DEBUG <<
                "handle_getalloc(" << fid << "," << offset <<
                ") = " << status << ": kfsop failed" <<
            KFS_LOG_EOM;
            return;
        }
        chunkId      = chunkInfo->chunkId;
        chunkVersion = chunkInfo->chunkVersion;
        err = gLayoutManager.GetChunkToServerMapping(
            *chunkInfo, c, fa, &replicasOrderedFlag);
        if (! fa || fa->IsSymLink()) {
            panic("invalid chunk to server map", false);
        }
    }
    if (! CanAccessFile(fa, *this)) {
        return;
    }
    if (! fromChunkServerFlag && gLayoutManager.VerifyAllOpsPermissions()) {
        SetEUserAndEGroup(*this);
        if (! fa->CanRead(euser, egroup)) {
            status = -EACCES;
            return;
        }
    }
    if (-EBUSY == err) {
        status    = err;
        statusMsg = "meta server is in recovery mode";
        return;
    }
    if (err) {
        status    = -EAGAIN;
        statusMsg = "no replicas available chunk: ";
        AppendDecIntToString(statusMsg, chunkId);
        KFS_LOG_STREAM_ERROR <<
            "getalloc "
            "<" << fid << "," << chunkId << "," << offset << ">"
            " " << statusMsg <<
        KFS_LOG_EOM;
        return;
    }
    locations.reserve(c.size());
    allChunkServersShortRpcFlag = shortRpcFormatFlag;
    for_each(c.begin(), c.end(), EnumerateLocationsShortRpc(locations,
        allChunkServersShortRpcFlag));
    status = 0;
}

/*!
 * \brief Get the allocation information for a file.  Determine
 * how many chunks there and where they are located.
 */
/* virtual */ void
MetaGetlayout::handle()
{
    if (status < 0) {
        return;
    }
    if (! HasEnoughIoBuffersForResponse(*this)) {
        return;
    }
    vector<MetaChunkInfo*> chunkInfo;
    MetaFattr*             fa = 0;
    if (lastChunkInfoOnlyFlag) {
        MetaChunkInfo* ci = 0;
        status = metatree.getLastChunkInfo(
            fid, fa, ci);
        if (status == 0 && ci) {
            chunkInfo.push_back(ci);
        }
    } else if (startOffset <= 0) {
        status = metatree.getalloc(fid, fa, chunkInfo,
            maxResCnt > 0 ? maxResCnt + 1 : maxResCnt);
    } else {
        status = metatree.getalloc(fid, startOffset, chunkInfo,
            maxResCnt > 0 ? maxResCnt + 1 : maxResCnt);
        if (status == -ENOENT && (fa = metatree.getFattr(fid))) {
            status = 0;
        }
    }
    if (status != 0) {
        return;
    }
    if (! fa) {
        if (chunkInfo.empty()) {
            panic("MetaGetlayout::handle -- getalloc no chunks");
            status = -EFAULT;
            return;
        }
        fa = chunkInfo.front()->getFattr();
        if (! fa) {
            panic("MetaGetlayout::handle -- invalid chunk entry");
            status = -EFAULT;
            return;
        }
    }
    if (! CanAccessFile(fa, *this)) {
        return;
    }
    if (gLayoutManager.VerifyAllOpsPermissions()) {
        SetEUserAndEGroup(*this);
        if (! fa->CanRead(euser, egroup)) {
            status = -EACCES;
            return;
        }
    }
    fileSize  = fa->filesize;
    numChunks = (int)chunkInfo.size();
    if ((hasMoreChunksFlag = maxResCnt > 0 && maxResCnt < numChunks)) {
        numChunks = maxResCnt;
    }
    ostream& os = sWOStream.Set(resp);
    if (shortRpcFormatFlag) {
        os << hex;
    }
    const char*     prefix = "";
    Servers         c;
    ChunkLayoutInfo l;
    allChunkServersShortRpcFlag = shortRpcFormatFlag;
    for (int i = 0; i < numChunks; i++) {
        l.locations.clear();
        l.offset       = chunkInfo[i]->offset;
        l.chunkId      = chunkInfo[i]->chunkId;
        l.chunkVersion = chunkInfo[i]->chunkVersion;
        if (! omitLocationsFlag) {
            MetaFattr* cfa = 0;
            const int  err = gLayoutManager.GetChunkToServerMapping(
                *(chunkInfo[i]), c, cfa);
            assert(! fa || cfa == fa);
            if (err && ! continueIfNoReplicasFlag) {
                resp.Clear();
                status    = -EHOSTUNREACH;
                statusMsg = "no replicas available chunk: ";
                AppendDecIntToString(statusMsg, l.chunkId);
                break;
            }
            for_each(c.begin(), c.end(),
                EnumerateLocationsShortRpc(l.locations,
                    allChunkServersShortRpcFlag));
        }
        if (! (os << prefix << l)) {
            break;
        }
        prefix = " ";
    }
    os.flush();
    if (status == 0 && ! os) {
        resp.Clear();
        status    = -ENOMEM;
        statusMsg = "response exceeds max. size";
    }
    sWOStream.Reset();
}

/* virtual */ bool
MetaAllocate::dispatch(ClientSM& sm)
{
    return sm.Handle(*this);
}

/*!
 * \brief handle an allocation request for a chunk in a file.
 * \param[in] r write allocation request
 *
 * Write allocation proceeds as follows:
 *  1. The client has sent a write allocation request which has been
 * parsed and turned into an RPC request (which is handled here).
 *  2. We first get a unique chunk identifier (after validating the
 * fileid).
 *  3. We send the request to the layout manager to pick a location
 * for the chunk.
 *  4. The layout manager picks a location and sends an RPC to the
 * corresponding chunk server to create the chunk.
 *  5. When the RPC is going on, processing for this request is
 * suspended.
 *  6. When the RPC reply is received, this request gets re-activated
 * and we come back to this function.
 *  7. Assuming that the chunk server returned a success,  we update
 * the metatree to link the chunkId with the fileid (from this
 * request).
 *  8. Processing for this request is now complete; it is logged and
 * a reply is sent back to the client.
 *
 * Versioning/Leases introduces a few wrinkles to the above steps:
 * In step #2, the metatree could return -EEXIST if an allocation
 * has been done for the <fid, offset>.  In such a case, we need to
 * check with the layout manager to see if a new lease is required.
 * If a new lease is required, the layout manager bumps up the version
 * # for the chunk and notifies the chunkservers.  The message has to
 * be suspended until the chunkservers ack.  After the message is
 * restarted, we need to update the metatree to reflect the newer
 * version # before notifying the client.
 *
 * On the other hand, if a new lease isn't required, then the layout
 * manager tells us where the data has been placed; the process for
 * the request is therefore complete.
 */

/* virtual */ void
MetaAllocate::handle()
{
    assert(! MetaRequest::next);
    suspended = false;
    if (startedFlag) {
        return;
    }
    startedFlag = true;
    if (status < 0) {
        return;
    }
    KFS_LOG_STREAM_DEBUG <<
        "starting layout: " << Show() <<
    KFS_LOG_EOM;
    if (gWormMode && ! IsWormMutationAllowed(pathname.GetStr())) {
        statusMsg = "worm mode";
        status    = -EPERM;
        return;
    }
    if (! gLayoutManager.IsAllocationAllowed(*this)) {
        if (0 <= status) {
            statusMsg = "allocation not allowed";
            status    = -EPERM;
        }
        return;
    }
    // Do not check permissions for chunk invalidation internally generated by
    // the chunk recovery.
    if (gLayoutManager.VerifyAllOpsPermissions() &&
            (clnt || ! invalidateAllFlag || fromChunkServerFlag ||
                fromClientSMFlag || authUid != kKfsUserNone)) {
        SetEUserAndEGroup(*this);
    } else {
        euser = kKfsUserRoot; // Don't check permissions.
    }
    if (appendChunk && invalidateAllFlag) {
        statusMsg = "chunk invalidation is not supported with append";
        status    = -EINVAL;
        return;
    }
    if (appendChunk) {
        // pick a chunk for which a write lease exists
        status = 0;
        if (gLayoutManager.AllocateChunkForAppend(*this) == 0) {
            // all good
            KFS_LOG_STREAM_DEBUG <<
                " req: " << opSeqno <<
                " append re-using chunk " << chunkId <<
                (suspended ? "; allocation in progress" : "") <<
                " status : " << status <<
            KFS_LOG_EOM;
            // No logging or serialization is required, LayoutDone method will
            // resume / re-submit request if suspended.
            return;
        }
        if (0 != status) {
            return;
        }
        offset = -1; // Allocate a new chunk past eof.
    }
    // force an allocation
    chunkId             = 0;
    initialChunkVersion = -1;
    vector<MetaChunkInfo*> chunkBlock;
    MetaFattr*             fa = 0;
    // start at step #2 above.
    status = metatree.allocateChunkId(
        fid,
        offset,
        &chunkId,
        &chunkVersion,
        &numReplicas,
        &stripedFileFlag,
        &chunkBlock,
        &chunkBlockStart,
        euser,
        egroup,
        &fa
    );
    if (0 == numReplicas) {
        if (clientProtoVers <
                KFS_CLIENT_MIN_OBJECT_STORE_FILE_SUPPORT_PROTO_VERS) {
            status    = -EPERM;
            statusMsg = "client upgrade required to write object store file";
            return;
        }
        if (appendChunk) {
            status    = -EINVAL;
            statusMsg = "append is not supported with object store files";
            return;
        }
        if (invalidateAllFlag) {
            status    = -EINVAL;
            statusMsg = "chunk invalidation is not supported"
                " with object store files";
            return;
        }
    }
    if (0 != status && (-EEXIST != status || appendChunk)) {
        return; // Access denied or invalid request.
    }
    if (stripedFileFlag && appendChunk) {
        status    = -EINVAL;
        statusMsg = "append is not supported with striped files";
        return;
    }
    if (invalidateAllFlag) {
        if (! stripedFileFlag) {
            status    = -EINVAL;
            statusMsg = "chunk invalidation"
                " is not supported with non striped files";
            return;
        }
        if (-EEXIST == status) {
            initialChunkVersion = chunkVersion;
            status = 0;
        }
        if (0 == status) {
            // Invalidate after log completion.
            suspended = true;
            submit_request(new MetaLogChunkAllocate(this));
        }
        return;
    }
    permissions = fa;
    minSTier    = fa->minSTier;
    maxSTier    = fa->maxSTier;
    if (status == -EEXIST) {
        initialChunkVersion = chunkVersion;
        // Attempt to obtain a new lease.
        status = 0;
        gLayoutManager.GetChunkWriteLease(*this);
        if (suspended) {
            panic("chunk allocation suspended after lease acquistion");
        }
        if (0 == status) {
            suspended = true;
            if (0 == numReplicas) {
                if (1 != servers.size() || ! servers.front()) {
                    panic("chunk allocation suspended after lease acquistion");
                    status    = -EFAULT;
                    suspended = false;
                } else {
                    servers.front()->AllocateChunk(*this, leaseId, minSTier);
                }
            } else {
                submit_request(new MetaLogChunkVersionChange(this));
            }
        }
        return;
    }
    suspended = true;
    const int ret = gLayoutManager.AllocateChunk(*this, chunkBlock);
    if (0 == ret) {
        return;
    }
    // Failure: set status and resume.
    status    = ret;
    suspended = false;
}

void
MetaAllocate::LayoutDone(int64_t chunkAllocProcessTime)
{
    suspended = false;
    if (0 == status) {
        // Check if all servers are still up, and didn't go down
        CheckAllServersUp();
    }
    KFS_LOG_STREAM_DEBUG <<
        "layout is done for"
        " req: "     << opSeqno   <<
        " status: "  << status    <<
        " "          << statusMsg <<
        " servers: " << InsertServers(servers) <<
    KFS_LOG_EOM;
    if (-ENOENT == status) {
        // Change status to generic failure, to distingush from no such file.
        status = -EALLOCFAILED;
    }
    // Ensure that the op isn't stale.
    // Invalidate all replicas might make it stale if it comes while this op
    // is in flight. Do not do any cleanup if the op is invalid: all required
    // cleanup has already been done.
    if (gLayoutManager.Validate(*this) && 0 != status) {
        // we have a problem: it is possible that the server
        // went down.  ask the client to retry....
        if (0 <= status) {
            status = -EALLOCFAILED;
        }
        if (0 <= initialChunkVersion) {
            gLayoutManager.CommitOrRollBackChunkVersion(*this);
        } else {
            // this is the first time the chunk was allocated.
            // since the allocation failed, remove existence of this chunk
            // on the metaserver.
            gLayoutManager.DeleteChunk(*this);
        }
    } else if (0 != status && initialChunkVersion < 0) {
        // Cleanup stale chunk in the case when client has already invalidated
        // the chunk, and the chunk didn't exist. In this case a new chunk id
        // was created.
        gLayoutManager.DeleteChunk(*this);
    }
    if (0 == status) {
        assert(! MetaRequest::next);
        suspended = true;
        submit_request(new MetaLogChunkAllocate(this));
        return;
    }
    const bool kCountAllocTimeFlag = true;
    Done(kCountAllocTimeFlag, chunkAllocProcessTime);
}

bool
MetaLogChunkAllocate::start()
{
    if (alloc) {
        if (! alloc->suspended) {
            panic("log chunk alloction:"
                " chunk allocation was not suspended");
            return false;
        }
        fid                 = alloc->fid;
        offset              = alloc->offset;
        chunkId             = alloc->chunkId;
        chunkVersion        = alloc->chunkVersion;
        appendChunk         = alloc->appendChunk;
        invalidateAllFlag   = alloc->invalidateAllFlag;
        objectStoreFileFlag = 0 == alloc->numReplicas;
        initialChunkVersion = alloc->initialChunkVersion;
        mtime               = microseconds();
        if (! invalidateAllFlag) {
            if (0 != alloc->status || alloc->servers.empty()) {
                panic("invalid meta log chunk allocate: no servers");
            }
            servers.reserve(alloc->servers.size());
            for (Servers::const_iterator it = alloc->servers.begin();
                    it != alloc->servers.end();
                    ++it) {
                servers.push_back((*it)->GetServerLocation());
            }
        }
    }
    return (0 == status);
}

void
MetaLogChunkAllocate::handle()
{
    if (replayFlag != (0 == alloc)) {
        panic("invalid meta log chunk allocate");
        return;
    }
    if (alloc && 0 != alloc->status) {
        panic("chunk allocate status changed while log allocate was in flight");
    }
    if (0 != status) {
        KFS_LOG_STREAM_ERROR <<
            "status: " << status <<
            " "        << statusMsg <<
            " "        << Show() <<
        KFS_LOG_EOM;
    } else if (invalidateAllFlag) {
        if (0 <= initialChunkVersion) {
            if (gLayoutManager.InvalidateAllChunkReplicas(
                    fid, offset, chunkId, chunkVersion)) {
                // Add the chunk to the recovery queue.
                gLayoutManager.ChangeChunkReplication(chunkId);
            } else {
                // Chunk might be deleted or re-assigned to a different i-node
                // by coalesce block -- the op in the log queue in front of this
                // one.
                status    = -ENOENT;
                statusMsg = "invalidate replicas no such file or chunk";
            }
        } else {
            // Writer can deside to invalidate chunk that does not exists in the
            // case of continious allocation failures (out of space for
            // example).
            // Allocate the chunk to trigger the recovery later.
            status = metatree.assignChunkId(
                fid, offset, chunkId, chunkVersion, mtime);
            if (0 == status) {
                // Add the chunk to the recovery queue.
                gLayoutManager.ChangeChunkReplication(chunkId);
            }
        }
    } else {
        if (0 <= initialChunkVersion && ! objectStoreFileFlag) {
            fid_t   curFid = -1;
            Servers curServers;
            if (! gLayoutManager.GetChunkFileId(chunkId, curFid)) {
                // Chunk was deleted (by truncate), fail the allocation.
                status    = -ENOENT;
                statusMsg = "no such chunk";
                KFS_LOG_STREAM_DEBUG <<
                    statusMsg << " " << Show() <<
                KFS_LOG_EOM;
            } else if (fid != curFid) {
                // fid = curFid;
                status    = -ENOENT;
                statusMsg = "file id has changed";
                KFS_LOG_STREAM_DEBUG <<
                    statusMsg << " to: " << curFid <<
                    " " << Show() <<
                KFS_LOG_EOM;
            }
        }
        if (0 == status) {
            // layout is complete (step #6)
            // update the tree (step #7) and since we turned off the
            // suspend flag, the request will be logged and go on its
            // merry way.
            //
            // There could be more than one append allocation request set
            // (each set one or more request with the same chunk) in flight.
            // The append request sets can finish in any order.
            // The append request sets can potentially all have the same
            // offset: the eof at the time the first request in each set
            // started.
            // For append requests assignChunkId assigns past eof offset,
            // if it succeeds, and returns the value in appendOffset.
            chunkOff_t       appendOffset      = offset;
            chunkId_t        curChunkId        = chunkId;
            const bool       kAppendReplayFlag = false;
            const MetaFattr* fa                = 0;
            status = metatree.assignChunkId(fid, offset, chunkId, chunkVersion,
                mtime,
                appendChunk ? &appendOffset : 0, &curChunkId,
                kAppendReplayFlag, &fa);
            if (status == 0) {
                // Offset can change in the case of append.
                offset = appendOffset;
                if (objectStoreFileFlag != (0 == fa->numReplicas)) {
                    panic("object store flag and replication count mismatch");
                }
                if (! objectStoreFileFlag && 0 <= initialChunkVersion) {
                    gLayoutManager.CancelPendingMakeStable(fid, chunkId);
                }
            } else {
                KFS_LOG_STREAM((appendChunk && status == -EEXIST) ?
                        MsgLogger::kLogLevelFATAL :
                        MsgLogger::kLogLevelDEBUG) <<
                    "chunk assignment failed for:"
                    " chunk: "    << chunkId <<
                    " fid: "      << fid <<
                    " pos: "      << offset <<
                    " status: "   << status <<
                    " curchunk: " << curChunkId <<
                KFS_LOG_EOM;
                if (appendChunk && status == -EEXIST) {
                    panic("append chunk allocation internal error");
                } else if (status == -ENOENT ||
                        (status == -EEXIST && curChunkId != chunkId)) {
                    if (alloc) {
                        gLayoutManager.DeleteChunk(*alloc);
                        alloc->servers.clear();
                    }
                }
            }
        }
    }
    if (alloc) {
        alloc->status    = status;
        alloc->statusMsg = statusMsg;
    }
    if (! invalidateAllFlag) {
        gLayoutManager.CommitOrRollBackChunkVersion(*this);
    }
    if (alloc) {
        const bool kCountAllocTimeFlag = false;
        alloc->Done(kCountAllocTimeFlag, 0);
    } else {
        // Replay.
        chunkID.setseed(max(chunkID.getseed(), chunkId));
    }
}

void
MetaAllocate::Done(bool countAllocTimeFlag, int64_t chunkAllocProcessTime)
{
    suspended = false;
    if (appendChunk) {
        if (0 <= status) {
            if (responseStr.empty()) {
                ostringstream& os = GetTmpOStringStream();
                ReqOstream ros(os);
                responseSelf(ros);
                responseStr = os.str();
            }
            if (writeMasterKeyValidFlag && responseAccessStr.empty()) {
                tokenSeq = (TokenSeq)gLayoutManager.GetRandom().Rand();
                ostringstream& os = GetTmpOStringStream();
                ReqOstream ros(os);
                writeChunkAccess(ros);
                responseAccessStr = os.str();
            }
        }
        gLayoutManager.AllocateChunkForAppendDone(*this);
    }
    if (0 <= status && pendingLeaseRelinquish) {
        // Relinquish the lease upon completion.
        if (pendingLeaseRelinquish->clnt) {
            panic("allocate invalid pending lease relinquish target");
        } else {
            pendingLeaseRelinquish->clnt = clnt;
            clnt = this;
        }
    }
    if (0 < GetRecursionCount()) {
        // Don't need need to resume, if it wasn't suspended: this
        // method is [indirectly] invoked from handle().
        // Presently the only way to get here is from synchronous chunk
        // server allocation failure. The request cannot possibly have
        // non empty request list in this case, as it wasn't ever
        // suspened.
        if (next) {
            panic("non empty allocation queue,"
                "for request that was not suspended");
        }
        return;
    }
    // Currently the ops queue only used for append allocations.
    assert(appendChunk || ! next);
    // Update the process time, charged from MetaChunkAllocate.
    if (countAllocTimeFlag) {
        processTime += microseconds() - chunkAllocProcessTime;
    }
    if (! next) {
        submit_request(this);
        return;
    }
    // Clone status for all ops in the queue.
    // Submit the replies in the same order as requests.
    // "this" might get deleted after submit_request()
    MetaAllocate*         n           = this;
    const string          ra          = responseAccessStr;
    const kfsUid_t        rauid       = authUid;
    const CryptoKeys::Key wmkey       = writeMasterKey;
    const bool            shorFmtFlag = shortRpcFormatFlag;
    const string          respStr     = responseStr;
    const Servers         srvs        = servers;
    do {
        MetaAllocate& c = *n;
        n = c.next;
        c.next = 0;
        if (n) {
            MetaAllocate& q = *n;
            assert(q.fid == c.fid);
            q.status           = c.status;
            q.statusMsg        = c.statusMsg;
            q.suspended        = false;
            q.fid              = c.fid;
            q.offset           = c.offset;
            q.chunkId          = c.chunkId;
            q.chunkVersion     = c.chunkVersion;
            q.pathname         = c.pathname;
            q.numReplicas      = c.numReplicas;
            q.appendChunk      = c.appendChunk;
            q.stripedFileFlag  = c.stripedFileFlag;
            q.numServerReplies = c.numServerReplies;
            if (shorFmtFlag == q.shortRpcFormatFlag) {
                q.responseStr = respStr;
            }
            if (q.responseStr.empty()) {
                q.servers = srvs;
            }
            q.writeMasterKeyValidFlag = c.writeMasterKeyValidFlag;
            if (q.writeMasterKeyValidFlag) {
                q.tokenSeq                   = c.tokenSeq;
                q.clientCSAllowClearTextFlag = c.clientCSAllowClearTextFlag;
                q.issuedTime                 = c.issuedTime;
                q.validForTime               = c.validForTime;
                q.writeMasterKeyId           = c.writeMasterKeyId;
                if (ra.empty() || rauid != c.authUid ||
                        shorFmtFlag != q.shortRpcFormatFlag) {
                    q.writeMasterKey = wmkey;
                } else {
                    q.responseAccessStr = ra;
                }
            }
        }
        submit_request(&c);
    } while (n);
}

int
MetaAllocate::PendingLeaseRelinquish(int code, void* data)
{
    if (EVENT_CMD_DONE != code ||
            (this != data && pendingLeaseRelinquish != data)) {
        panic("MetaChunkAllocate::logDone invalid invocation");
        return 1;
    }
    if (this == data) {
        clnt = pendingLeaseRelinquish->clnt;
        if (0 == status) {
            pendingLeaseRelinquish->clnt = this;
            submit_request(pendingLeaseRelinquish);
            return 0;
        }
    }
    pendingLeaseRelinquish->clnt = 0;
    MetaRequest::Release(pendingLeaseRelinquish);
    pendingLeaseRelinquish = 0;
    submit_request(this);
    return 0;
}

bool
MetaAllocate::CheckAllServersUp()
{
    for (Servers::const_iterator it = servers.begin();
            servers.end() != it;
            ++it) {
        const ChunkServer& srv = **it;
        if (! srv.IsConnected()) {
            if (0 <= status) {
                status    = -EALLOCFAILED;
                statusMsg += "server ";
                statusMsg += srv.GetServerLocation().ToString();
                statusMsg += " went down";
            }
            return false;
        }
    }
    return true;
}

bool
MetaAllocate::ChunkAllocDone(const MetaChunkAllocate& chunkAlloc)
{
    // if there is a non-zero status, don't throw it away
    if (0 != chunkAlloc.status) {
        if (0 == status && firstFailedServerIdx < 0) {
            status = chunkAlloc.status;
            // In the case of version change failure take the first failed
            // server out, otherwise allocation might never succeed.
            if (0 <= initialChunkVersion && 1 < servers.size()) {
                const ChunkServer* const cs = chunkAlloc.server.get();
                for (Servers::const_iterator it = servers.begin();
                        servers.end() != it;
                        ++it) {
                    if (it->get() == cs && (**it).IsConnected()) {
                        firstFailedServerIdx = (int)(servers.begin() - it);
                        break;
                    }
                }
            }
        }
        // Collect and pass back to the client chunk server allocation failure
        // messages.
        if (chunkAlloc.server) {
            if (statusMsg.length() < 384) {
                if (! statusMsg.empty()) {
                    statusMsg += " ";
                }
                statusMsg += chunkAlloc.server->GetServerLocation().ToString();
                if (! chunkAlloc.statusMsg.empty()) {
                    statusMsg += " ";
                    statusMsg += chunkAlloc.statusMsg;
                }
            }
        } else if (statusMsg.empty()) {
            statusMsg = chunkAlloc.statusMsg;
        }
    }
    numServerReplies++;
    // wait until we get replies from all servers
    if (numServerReplies != servers.size()) {
        return false;
    }
    // The op is no longer suspended.
    const bool discardFlag = 0 <= firstFailedServerIdx && 0 != status;
    if (discardFlag && CheckAllServersUp()) {
        gLayoutManager.ChunkCorrupt(chunkId, servers[firstFailedServerIdx]);
    }
    LayoutDone(chunkAlloc.processTime);
    return true;
}

/* virtual */ void
MetaChunkAllocate::handle()
{
    if (! req || META_ALLOCATE != req->op) {
        panic("invalid meta chunk allocation op");
        return;
    }
    if (req->ChunkAllocDone(*this)) {
        // The time was charged to alloc.
        processTime = microseconds();
    }
    // Detach allocate op, as it might be resumed and deleted.
    const_cast<MetaAllocate*&>(req) = 0;
}

ostream&
MetaAllocate::ShowSelf(ostream& os) const
{
    os << "allocate:"
        " seq: "        << opSeqno             <<
        " status: "     << status              <<
        " "             << statusMsg           <<
        " path: "       << pathname            <<
        " fid: "        << fid                 <<
        " chunk: "      << chunkId             <<
        " offset: "     << offset              <<
        " client: "     << clientHost          <<
        " / "           << clientIp            <<
        " replicas: "   << numReplicas         <<
        " append: "     << appendChunk         <<
        " version: "    << chunkVersion        <<
        " / "           << initialChunkVersion <<
        " invalidate: " << invalidateAllFlag   <<
        " valid for: "  << validForTime
    ;
    for (Servers::const_iterator i = servers.begin();
            i != servers.end();
            ++i) {
        os << " " << (*i)->GetServerLocation();
    }
    return os;
}

bool
MetaLogChunkVersionChange::start()
{
    if (! alloc || ! alloc->suspended || 0 != alloc->status) {
        panic("log version change:"
            " allocation null, failed, or was not suspended");
        return false;
    }
    fid          = alloc->fid;
    chunkId      = alloc->chunkId;
    chunkVersion = alloc->chunkVersion;
    return (0 == status);
}

void
MetaLogChunkVersionChange::handle()
{
    if ((0 == alloc) != replayFlag) {
        panic("invalid log version change: null allocation or repaly flag");
    }
    if (0 == status) {
        const bool panicOnInvalidVersion = 0 != alloc;
        status = gLayoutManager.ProcessBeginChangeChunkVersion(
            fid, chunkId, chunkVersion, &statusMsg, panicOnInvalidVersion);
    }
    if (alloc && (! alloc->suspended || 0 != alloc->status)) {
        panic("version change: invalid allocate");
    }
    if (0 == status) {
        gLayoutManager.ChangeChunkVersion(chunkId, chunkVersion, alloc);
    }
    if (! alloc) {
        return;
    }
    if (0 != status) {
        alloc->logChunkVersionChangeFailedFlag = true;
        alloc->status                          = status;
        alloc->statusMsg                       = statusMsg;
        alloc->LayoutDone(processTime);
        processTime = microseconds(); // Time charged to allocate.
        return;
    }
    if (alloc->status < 0) {
        alloc->LayoutDone(processTime);
        return;
    }
    if (alloc->servers.empty()) {
        panic("version change: no servers");
        alloc->status = -EFAULT;
        alloc->LayoutDone(processTime);
    }
    for (size_t i = alloc->servers.size(); i-- > 0; ) {
        alloc->servers[i]->AllocateChunk(
            *alloc, i == 0 ? alloc->leaseId : -1, alloc->minSTier);
    }
}

/* virtual */ void
MetaChunkVersChange::handle()
{
    gLayoutManager.Done(*this);
}

/* virtual */ bool
MetaTruncate::start()
{
    if (chunksCleanupFlag) {
        if (pruneBlksFromHead || ! pathname.empty() || maxDeleteCount < 0) {
            status = -EINVAL;
        }
    } else {
        if (gWormMode && ! IsWormMutationAllowed(pathname.GetStr())) {
            statusMsg = "worm mode";
            status    = -EPERM;
            return false;
        }
        if (checkPermsFlag || gLayoutManager.VerifyAllOpsPermissions()) {
            SetEUserAndEGroup(*this);
        } else {
            euser = kKfsUserRoot;
        }
        if (! pruneBlksFromHead && (endOffset >= 0 && endOffset < offset)) {
            status    = -EINVAL;
            statusMsg = "end offset less than offset";
            return false;
        }
    }
    if (0 == status) {
        mtime = microseconds();
    }
    gLayoutManager.Start(*this);
    return (0 == status);
}

/* virtual */ void
MetaTruncate::handle()
{
    if (0 == status) {
        if (chunksCleanupFlag) {
            status = metatree.cleanupChunks(maxDeleteCount, mtime);
        } else if (pruneBlksFromHead) {
            status = metatree.pruneFromHead(
                fid, offset, mtime, euser, egroup, maxDeleteCount,
                maxQueueCount, &statusMsg);
        } else {
            status = metatree.truncate(fid, offset, mtime, euser, egroup,
                endOffset, setEofHintFlag, maxDeleteCount,
                maxQueueCount, &statusMsg);
        }
    }
    gLayoutManager.Handle(*this);
}

/* virtual */ bool
MetaRename::start()
{
    SetEUserAndEGroup(*this);
    StIdempotentRequestHandler handler(*this);
    if (handler.IsDone()) {
        return true;
    }
    if (0 != status) {
        return false;
    }
    wormModeFlag = gWormMode;
    if (wormModeFlag && ! IsWormMutationAllowed(oldname)) {
        statusMsg = "worm mode";
        status    = -EPERM;
    }
    if (0 == status && metatree.getDumpsterDirId() == dir) {
        gLayoutManager.Start(*this);
    }
    if (0 == status) {
        mtime = microseconds();
    }
    return (0 == status);
}

/* virtual */ void
MetaRename::handle()
{
    if (IsHandled()) {
        return;
    }
    if (0 == status) {
        // renames are disabled in WORM mode: otherwise, we
        // ocould overwrite an existing file
        srcFid = -1;
        bool const kToDumpsterFlag = true;
        status = metatree.rename(dir, oldname, newname,
            oldpath, overwrite && ! wormModeFlag, euser, egroup,
            mtime, &srcFid, kToDumpsterFlag);
        if (wormModeFlag && -EEXIST == status) {
            statusMsg = "worm mode";
            status    = -EPERM;
        }
    }
    if (leaseFileEntry ||
            (replayFlag && metatree.getDumpsterDirId() == dir)) {
        gLayoutManager.Done(*this);
    }
}

bool
MetaLink::Validate()
{
    string orig;
    orig.swap(targetPath);

    return (0 <= dir && ! name.empty() &&
        StringIo::Unescape(orig.data(), orig.size(), targetPath) &&
        ! targetPath.empty());
}

/* virtual */ bool
MetaLink::start()
{
    if (! SetUserAndGroup(*this)) {
        return false;
    }
    StIdempotentRequestHandler handler(*this);
    if (handler.IsDone()) {
        return true;
    }
    if (targetPath.empty()) {
        status    = -EINVAL;
        statusMsg = "empty target path";
        return false;
    }
    if (MAX_PATH_NAME_LENGTH < targetPath.length()) {
        status    = -ENAMETOOLONG;
        statusMsg = "target path is too long";
        return false;
    }
    const bool kDirFlag = false;
    if (! CheckCreatePerms(*this, kDirFlag)) {
        return false;
    }
    if (0 == status) {
        wormModeFlag = gWormMode;
        mtime = microseconds();
    }
    return (0 == status);
}

/* virtual */ void
MetaLink::handle()
{
    if (IsHandled()) {
        return;
    }
    MetaFattr*    fa                  = 0;
    bool    const kToDumpsterFlag     = true;
    bool    const exclusiveFlag       = ! overwriteFlag || wormModeFlag;
    bool    const numReplicas         = 1;
    int32_t const numStripes          = 0;
    int32_t const numRecoveryStripes  = 0;
    int32_t const stripeSize          = 0;
    fid = 0; // Set to 0 in order to generate new ID.
    status = metatree.create(
        dir,
        name,
        &fid,
        numReplicas,
        exclusiveFlag,
        KFS_STRIPED_FILE_TYPE_NONE,
        numStripes,
        numRecoveryStripes,
        stripeSize,
        user,
        group,
        mode,
        euser,
        egroup,
        &fa,
        mtime,
        kToDumpsterFlag
    );
    if (0 == status) {
        if (! fa || fa->id() <= 0 || 0 != fa->chunkcount() ||
                0 != fa->filesize || 1 != fa->numReplicas) {
            panic("invalid symbolic link attributes");
            status = -EFAULT;
            return;
        }
        // Do not inherit directory tiers, reset them to
        // keep all symbolic links consistent.
        fa->minSTier = kKfsSTierMax;
        fa->maxSTier = kKfsSTierMax;
        fa->SetExtAttributes(kFileAttrExtTypeSymLink, targetPath);
    }
}

/* virtual */ bool
MetaSetMtime::start()
{
    if (fid < 0) {
        SetEUserAndEGroup(*this);
    }
    return (0 == status);
}

/* virtual */ void
MetaSetMtime::handle()
{
    if (0 != status) {
        return;
    }
    MetaFattr* fa = 0;
    if (0 <= fid) {
        fa = metatree.getFattr(fid);
    } else {
        status = metatree.lookupPath(dir, pathname, euser, egroup, fa);
    }
    if (0 == status && ! fa) {
        status = -ENOENT;
    }
    if (0 != status) {
        return;
    }
    if (fid < 0 && ! fa->CanWrite(euser, egroup)) {
        status = -EACCES;
        return;
    }
    if (IsAccessOk(*fa, *this)) {
        fa->mtime = mtime;
        if (kSetTimeTimeNotValid != atime) {
            fa->atime = atime;
        }
        if (kSetTimeTimeNotValid != ctime) {
            fa->ctime = ctime;
        }
    }
}

/* virtual */ void
MetaSetATime::handle()
{
    gLayoutManager.Handle(*this);
    WaitQueue queue;
    queue.PushBack(waitQueue);
    MetaRequest* req;
    while ((req = queue.PopFront())) {
        req->suspended = false;
        if (0 != status && 0 == req->status) {
            req->status = status;
        }
        submit_request(req);
    }
    if (0 != status) {
        return;
    }
    MetaFattr* const fa = 0 <= fid ? metatree.getFattr(fid) : 0;
    if (fa) {
        fa->atime = atime;
    } else {
        status = -ENOENT;
    }
}

/* virtual */ bool
MetaChangeFileReplication::start()
{
    SetEUserAndEGroup(*this);
    if (0 == status) {
        maxRSFileReplicas = gLayoutManager.GetMaxReplicasPerRSFile();
        maxFileReplicas   = gLayoutManager.GetMaxReplicasPerFile();
    }
    return (0 == status);
}

/* virtual */ void
MetaChangeFileReplication::handle()
{
    if (0 != status) {
        return;
    }
    MetaFattr* const fa = metatree.getFattr(fid);
    if (! CanAccessFile(fa, *this)) {
        return;
    }
    if (! fa->CanWrite(euser, egroup)) {
        status = -EACCES;
        return;
    }
    if (fa->type == KFS_DIR) {
        if (euser != kKfsUserRoot) {
            status    = -EACCES;
            statusMsg = "root privileges are"
                " required to change directory storage tiers";
            return;
        }
    } else {
        if (0 == fa->numReplicas) {
            status    = -EINVAL;
            statusMsg =
                "modification of object store file parameters is not supported";
            return;
        }
        numReplicas = min(numReplicas,
            max(int16_t(fa->numReplicas),
                (fa->striperType != KFS_STRIPED_FILE_TYPE_NONE &&
                        fa->numRecoveryStripes > 0) ?
                maxRSFileReplicas :
                maxFileReplicas
        ));
    }
    status = metatree.changeFileReplication(
        fa, numReplicas, minSTier, maxSTier);
    if (status == 0) {
        numReplicas = fa->type == KFS_DIR ?
            (int16_t)0 : (int16_t)fa->numReplicas;
    }
}

/*
 * Move chunks from src file into the end chunk boundary of the dst file.
 */
/* virtual */ bool
MetaCoalesceBlocks::start()
{
    dstStartOffset = -1;
    SetEUserAndEGroup(*this);
    if (0 == status) {
        mtime = microseconds();
    }
    if (0 <= srcFid) {
        srcPath.clear();
    }
    if (0 <= dstFid) {
        dstPath.clear();
    }
    return (0 == status);
}

/* virtual */ void
MetaCoalesceBlocks::handle()
{
    if (0 != status) {
        return;
    }
    status = metatree.coalesceBlocks(
        srcPath, dstPath, srcFid, dstFid,
        dstStartOffset, mtime, numChunksMoved,
        euser, egroup);
    KFS_LOG_STREAM(replayFlag ? MsgLogger::kLogLevelDEBUG :
            (0 == status ? MsgLogger::kLogLevelINFO :
                MsgLogger::kLogLevelERROR)) <<
        "coalesce blocks " << srcPath << "->" << dstPath <<
        " " << srcFid << "->" << dstFid <<
        " status: "       << status <<
        " offset: "       << dstStartOffset <<
        " chunks moved: " << numChunksMoved <<
    KFS_LOG_EOM;
}

/* virtual */ bool
MetaRetireChunkserver::start()
{
    startTime = globalNetManager().Now();
    return (
        HasMetaServerAdminAccess(*this) &&
        gLayoutManager.Validate(*this) &&
        0 == status
    );
}

/* virtual */ void
MetaRetireChunkserver::handle()
{
    gLayoutManager.RetireServer(*this);
}

/* virtual */ bool
MetaToggleWORM::start()
{
    return HasMetaServerAdminAccess(*this);
}

/* virtual */ void
MetaToggleWORM::handle()
{
    if (status < 0) {
        return;
    }
    KFS_LOG_STREAM_INFO << "Toggle WORM: " << value << KFS_LOG_EOM;
    setWORMMode(value);
}

/* virtual */ bool
MetaHello::start()
{
    if (! server && 0 == status) {
        status = -EINVAL;
        // This is likely coming from the ClientSM.
        KFS_LOG_STREAM_DEBUG << "no server invalid cmd: " << Show() <<
        KFS_LOG_EOM;
    }
    if (0 == status) {
        timeUsec = submitTime;
        gLayoutManager.Start(*this);
        if (0 != resumeStep && 0 == status) {
            logAction = kLogIfOk;
        }
    }
    return (0 == status);
}

/* virtual */ void
MetaHello::handle()
{
    gLayoutManager.AddNewServer(*this);
    if (replayFlag && 0 != status) {
        server->ScheduleDown(statusMsg.c_str());
    }
}

/* virtual */ bool
MetaHello::log(ostream& os) const
{
    const size_t kEntrySizeLog2 = 6;
    const size_t kMask          = (size_t(1) << kEntrySizeLog2) - 1;
    const size_t stableCount    =
        supportsResumeFlag ? chunks.size() : size_t(0);
    size_t       subEntryCnt    =
        1 +
        ((stableCount +
        notStableChunks.size() +
        notStableAppendChunks.size() + kMask) >> kEntrySizeLog2) +
        ((missingChunks.size() + kMask) >> kEntrySizeLog2) +
        ((pendingStaleChunks.size() + kMask) >> kEntrySizeLog2);
    ReqOstream ros(os);
    ros <<
        "csh"
        "/e/" << subEntryCnt <<
        "/l/" << location <<
        "/s/" << stableCount <<
        "/n/" << notStableChunks.size() <<
        "/a/" << notStableAppendChunks.size() <<
        "/m/" << missingChunks.size() <<
        "/p/" << pendingStaleChunks.size() <<
        "/d/" << deletedCount <<
        "/r/" << resumeStep <<
        "/t/" << timeUsec <<
        "/r/" << rackId <<
        "/P/" << (pendingNotifyFlag ? 1 : 0) <<
        "/R/" << (supportsResumeFlag ? 1 : 0) <<
        "/z/" << logseq
    ;
    const ChunkInfos* const infos[] =
        { &chunks, &notStableChunks, &notStableAppendChunks, 0 };
    size_t cnt = 0;
    for (const ChunkInfos*const* info = infos; *info; ++info) {
        if (*info == &chunks && stableCount <= 0) {
            continue;
        }
        for (ChunkInfos::const_iterator it = (*info)->begin();
                (*info)->end() != it;
                ++it) {
            if ((cnt++ & kMask) == 0) {
                ros << "\ncshc";
                subEntryCnt--;
            }
            ros << "/" << it->chunkId << "/" << it->chunkVersion;
        }
    }
    for (int k = 0; k < 2; k++) {
        cnt = 0;
        const ChunkIdList& list = 0 == k ? missingChunks : pendingStaleChunks;
        const char* const  pref = 0 == k ? "\ncshm" : "\ncshp";
        for (ChunkIdList::const_iterator it = list.begin();
                list.end() != it;
                ++it) {
            if ((cnt++ & kMask) == 0) {
                ros << pref;
                subEntryCnt--;
            }
            ros << "/" << *it;
        }
    }
    if (1 != subEntryCnt) {
        panic("invalid sub entry count");
    }
    ros << "\n";
    return true;
}

/* virtual */ bool
MetaBye::start()
{
    location = server->GetServerLocation();
    timeUsec = submitTime;
    gLayoutManager.Start(*this);
    return (0 == status);
}

/* virtual */ void
MetaBye::handle()
{
    gLayoutManager.Handle(*this);
}

/* virtual */ void
MetaLeaseAcquire::handle()
{
    if (atimeInFlightFlag) {
        atimeInFlightFlag = false;
        return;
    }
    if (status < 0) {
        return;
    }
    if (handleCount <= 0 && gLayoutManager.VerifyAllOpsPermissions()) {
        SetEUserAndEGroup(*this);
    }
    if (0 < handleCount++) {
        // Minimize spurious read lease acquisitions, when client timed out, and
        // closed connection. The connection check is racy, but should suffice
        // for the purpose at hands.
        // Presently the request can only come from the client. fromClientSMFlag
        // check here isn't strictly required, well unless something pretends to
        // be chunkserver, sends hello, then this request.
        const int64_t kMicroseconds = 1000 * 1000;
        const int64_t now           = globalNetManager().NowUsec();
        if ((submitTime + leaseTimeout * kMicroseconds < now) ||
                (0 < maxWaitMillisec &&
                    submitTime + maxWaitMillisec * 1000 < now)) {
            statusMsg = "lease wait timed out";
            status    = -EAGAIN;
            return;
        }
        if (fromClientSMFlag &&
                ! static_cast<const ClientSM*>(clnt)->GetConnection()) {
            status = -EAGAIN;
            return;
        }
    }
    gLayoutManager.Handle(*this);
}

/* virtual */ void
MetaLeaseRenew::handle()
{
    if (atimeInFlightFlag) {
        atimeInFlightFlag = false;
        return;
    }
    if (status < 0) {
        return;
    }
    if (gLayoutManager.VerifyAllOpsPermissions()) {
        SetEUserAndEGroup(*this);
    }
    if (status < 0) {
        return;
    }
    gLayoutManager.Handle(*this);
}

/* virtual */ void
MetaLeaseRelinquish::handle()
{
    if (status < 0) {
        return;
    }
    gLayoutManager.Handle(*this);
    KFS_LOG_STREAM(status == 0 ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        Show() << " status: " << status <<
    KFS_LOG_EOM;
}

/* virtual */ void
MetaLeaseCleanup::handle()
{
    if (0 <= status) {
        const time_t now = globalNetManager().Now();
        gLayoutManager.LeaseCleanup(now);
        metatree.cleanupPathToFidCache(now);
    }
    status = 0;
    statusMsg.clear();
}

/* virtual */ void
MetaGetPathName::handle()
{
    if (status < 0) {
        return;
    }
    ostringstream& oss = GetTmpOStringStream();
    ReqOstream os(oss);
    if (shortRpcFormatFlag) {
        os << hex;
    }
    const MetaFattr* fa = 0;
    if (fid < 0) {
        const MetaChunkInfo*   chunkInfo = 0;
        LayoutManager::Servers srvs;
        if (! gLayoutManager.GetChunkFileId(
                chunkId, fid, &chunkInfo, &fa, &srvs)) {
            status    = -ENOENT;
            statusMsg = "no such chunk";
            return;
        }
        if (chunkInfo) {
            os <<
            (shortRpcFormatFlag ? "O:" : "Chunk-offset: ") <<
                chunkInfo->offset << "\r\n" <<
            (shortRpcFormatFlag ? "V:" : "Chunk-version: ") <<
                chunkInfo->chunkVersion << "\r\n"
            ;
        }
        os << (shortRpcFormatFlag ? "NR:" : "Num-replicas: ") <<
            srvs.size() << "\r\n";
        if (! srvs.empty()) {
            os << (shortRpcFormatFlag ? "SL:" : "Replicas: ") <<
                InsertServers(srvs) << "\r\n";
        }
    } else {
        fa = metatree.getFattr(fid);
        if (! fa) {
            status    = -ENOENT;
            statusMsg = "no such file";
            return;
        }
    }
    if (fa) {
        if (gLayoutManager.VerifyAllOpsPermissions()) {
            SetEUserAndEGroup(*this);
            if (! fa->CanRead(euser, egroup)) {
                status = -EACCES;
                return;
            }
        }
        os << (shortRpcFormatFlag ? "N:" : "Path-name: ") <<
            metatree.getPathname(fa) << "\r\n";
        FattrReply(fa, fattr);
    }
    result = oss.str();
}

/* virtual */ bool
MetaChmod::start()
{
    SetEUserAndEGroup(*this);
    if (0 == status) {
        ctime = microseconds();
    }
    return (0 == status);
}

/* virtual */ void
MetaChmod::handle()
{
    if (0 != status) {
        return;
    }
    MetaFattr* const fa = metatree.getFattr(fid);
    if (! fa) {
        status = -ENOENT;
        return;
    }
    if (! IsValidMode(mode, fa->type == KFS_DIR)) {
        status = -EINVAL;
        return;
    }
    if (fa->user != euser && euser != kKfsUserRoot) {
        status = -EACCES;
        return;
    }
    if (IsAccessOk(*fa, *this)) {
        status = 0;
        fa->mode  = mode;
        if (fa->ctime < ctime) {
            fa->ctime = ctime;
        }
    }
}

/* virtual */ bool
MetaChown::start()
{
    if (0 != status || ! SetUserAndGroup(*this)) {
        return false;
    }
    ctime = microseconds();
    return (0 == status);
}

/* virtual */ void
MetaChown::handle()
{
    if (0 != status) {
        return;
    }
    MetaFattr* const fa = metatree.getFattr(fid);
    if (! fa) {
        status = -ENOENT;
        return;
    }
    if (fa->user != euser && euser != kKfsUserRoot) {
        status = -EACCES;
        return;
    }
    if (user != kKfsUserNone && euser != user && euser != kKfsUserRoot) {
        status = -EACCES;
        return;
    }
    if (group != kKfsGroupNone && euser != kKfsUserRoot) {
        kfsUid_t const owner = user == kKfsUserNone ? euser : user;
        if (! (authUid != kKfsUserNone ?
                gLayoutManager.GetUserAndGroup().IsGroupMember(owner, group) :
                IsGroupMember(owner, group))) {
            statusMsg = "user not a member of a group";
            status    = -EACCES;
            return;
        }
    }
    status = 0;
    if (user != kKfsUserNone) {
        fa->user = user;
    }
    if (group != kKfsGroupNone) {
        fa->group = group;
    }
    if (fa->ctime < ctime) {
        fa->ctime = ctime;
    }
}

/* virtual */ bool
MetaChunkCorrupt::start()
{
    if (server) {
        location = server->GetServerLocation();
        if (location.IsValid()) {
            if (chunkId < 0 && chunkCount <= 0) {
                // Do not log chunk directories updates.
                logAction = kLogQueue;
            }
        } else {
            panic("chunk corrupt: invalid server location");
            status = -EFAULT;
        }
    } else {
        // This is likely coming from the ClientSM.
        KFS_LOG_STREAM_DEBUG << "no server invalid cmd: " << Show() <<
        KFS_LOG_EOM;
        status = -EINVAL;
    }
    return (0 == status);
}

/* virtual */ void
MetaChunkCorrupt::handle()
{
    if (! chunkDir.empty() && 0 == status) {
        server->SetChunkDirStatus(chunkDir, dirOkFlag);
    }
    if (0 <= chunkId || 0 < chunkCount || 0 != status) {
        gLayoutManager.Handle(*this);
    }
}

/* virtual */ void
MetaChunkEvacuate::handle()
{
    if (server) {
        gLayoutManager.Handle(*this);
    } else {
        // This is likely coming from the ClientSM.
        KFS_LOG_STREAM_DEBUG << "no server invalid cmd: " << Show() <<
        KFS_LOG_EOM;
        status = -EINVAL;
    }
}

/* virtual */ bool
MetaChunkAvailable::start()
{
    if (0 == status && ! server) {
        status = -EINVAL;
        KFS_LOG_STREAM_DEBUG << "no server invalid cmd: " << Show() <<
        KFS_LOG_EOM;
    } else {
        location = server->GetServerLocation();
        if (location.IsValid()) {
            gLayoutManager.Start(*this);
        } else {
            panic("chunks available: invalid server location");
            status = -EFAULT;
        }
    }
    return (0 == status);
}

/* virtual */ void
MetaChunkAvailable::handle()
{
    gLayoutManager.Handle(*this);
}

/* virtual */ void
MetaChunkDirInfo::handle()
{
    if (server) {
        server->SetChunkDirInfo(dirName, props);
    } else {
        // This is likely coming from the ClientSM.
        KFS_LOG_STREAM_DEBUG << "no server invalid cmd: " << Show() <<
        KFS_LOG_EOM;
        status = -EINVAL;
    }
}

/* virtual */ void
MetaChunkReplicationCheck::handle()
{
    gLayoutManager.ChunkReplicationChecker();
    status = 0;
}

/* virtual */ void
MetaBeginMakeChunkStable::handle()
{
    gLayoutManager.BeginMakeChunkStableDone(*this);
    status = 0;
}

void
MetaLogMakeChunkStable::handle()
{
    if (replayFlag) {
        if (0 != status) {
            panic("invalid log make chunk stable");
            return;
        }
        const bool kAddFlag = true;
        gLayoutManager.ReplayPendingMakeStable(
            chunkId, chunkVersion, chunkSize, hasChunkChecksum, chunkChecksum,
            kAddFlag);
    } else {
        gLayoutManager.LogMakeChunkStableDone(*this);
    }
}

void
MetaLogMakeChunkStableDone::handle()
{
    gLayoutManager.Handle(*this);
}

/* virtual */ void
MetaChunkMakeStable::handle()
{
    gLayoutManager.MakeChunkStableDone(*this);
    status = 0;
}

/* virtual */ ostream&
MetaChunkMakeStable::ShowSelf(ostream& os) const
{
    return os <<
        "make-chunk-stable:"
        " server: "        << server->GetServerLocation() <<
        " seq: "           << opSeqno <<
        " status: "        << status <<
            (statusMsg.empty() ? "" : " ") << statusMsg <<
        " fileid: "        << fid <<
        " chunk: "         << chunkId <<
        " chunkvers: "     << chunkVersion <<
        " chunkSize: "     << chunkSize <<
        " chunkChecksum: " << chunkChecksum
    ;
}

/* virtual */ bool
MetaChunkSize::start()
{
    if (0 == status && gLayoutManager.Start(*this)) {
        return (0 == status);
    }
    return false;
}

/* virtual */ void
MetaChunkSize::handle()
{
    // Invoke regardless of status, in order to schedule retry.
    gLayoutManager.Handle(*this);
}

/* virtual */ void
MetaChunkReplicate::handle()
{
    gLayoutManager.Handle(*this);
}

/* virtual */ ostream&
MetaChunkReplicate::ShowSelf(ostream& os) const
{
    return os <<
        (numRecoveryStripes > 0 ? "recover" : "replicate") <<
        " chunk: "        << chunkId <<
        " version: "      << chunkVersion <<
        " file: "         << fid <<
        " fileSize: "     << fileSize <<
        " path: "         << pathname <<
        " recovStripes: " << numRecoveryStripes <<
        " seq: "          << opSeqno <<
        " from: "         << (dataServer ?
            dataServer->GetServerLocation() : ServerLocation()) <<
        " to: "           << (server ?
            server->GetServerLocation() : ServerLocation())
    ;
}

/* virtual */ bool
MetaPing::start()
{
    if (! HasMetaServerStatsAccess(*this)) {
        return false;
    }
    updateFlag = ! gLayoutManager.IsPingResponseUpToDate();
    if (! updateFlag) {
        logAction = kLogNever;
        gLayoutManager.Handle(*this, gWormMode);
    }
    return updateFlag;
}

/* virtual */ void
MetaPing::handle()
{
    if (0 == status && updateFlag) {
        gLayoutManager.Handle(*this, gWormMode);
    }
}

/* virtual */ void
MetaUpServers::handle()
{
    if (status < 0) {
        return;
    }
    if (! HasEnoughIoBuffersForResponse(*this)) {
        return;
    }
    if (! HasMetaServerStatsAccess(*this)) {
        return;
    }
    ostream& os = sWOStream.Set(resp);
    gLayoutManager.UpServers(os);
    os.flush();
    sWOStream.Reset();
    if (! os) {
        resp.Clear();
        status    = -ENOMEM;
        statusMsg = "response exceeds max. size";
    }
}

/* virtual */ void
MetaRecomputeDirsize::handle()
{
    if (! HasMetaServerAdminAccess(*this)) {
        return;
    }
    status = 0;
    KFS_LOG_STREAM_INFO << "processing a recompute dir size..." << KFS_LOG_EOM;
    metatree.recomputeDirSize();
}

static void
SigHupHandler(int /* sinum */)
{
    _exit(1);
}

static void
SigAlarmHandler(int /* sinum */)
{
    _exit(2);
}

static void
ChildAtFork(int childTimeLimit, const char* childName)
{
    signal(SIGHUP, &SigHupHandler);
    signal(SIGALRM, &SigAlarmHandler);
    if (childName) {
        QCThread::SetName(childName);
    }
    if (childTimeLimit > 0) {
        alarm(childTimeLimit);
    }
    if (MsgLogger::GetLogger()) {
        MsgLogger::GetLogger()->ChildAtFork();
    }
    AuditLog::ChildAtFork();
    globalNetManager().ChildAtFork();
    MetaRequest::GetLogWriter().ChildAtFork();
    gNetDispatch.ChildAtFork();
}

static int
DoFork(int childTimeLimit, const char* childName)
{
    gNetDispatch.PrepareCurrentThreadToFork();
    MetaRequest::GetLogWriter().PrepareToFork();
    gLayoutManager.GetUserAndGroup().PrepareToFork();
    AuditLog::PrepareToFork();
    MsgLogger* const logger = MsgLogger::GetLogger();
    if (logger) {
        logger->PrepareToFork();
    }
    int ret = fork();
    if (ret == 0) {
        ChildAtFork(childTimeLimit, childName);
    } else {
        if (ret < 0) {
            ret = -errno;
            if (0 == ret) {
                ret = -EFAULT;
            }
            if (0 < ret) {
                ret = -ret;
            }
        }
        if (logger) {
            logger->ForkDone();
        }
        AuditLog::ForkDone();
        gLayoutManager.GetUserAndGroup().ForkDone();
        MetaRequest::GetLogWriter().ForkDone();
        gNetDispatch.CurrentThreadForkDone();
    }
    return ret;
}

/* virtual */ void
MetaDumpChunkToServerMap::handle()
{
    suspended = false;
    if (pid > 0) {
        pid = -1;
        return; // Child finished.
    }
    if (! HasMetaServerAdminAccess(*this)) {
        return;
    }
    if (gChildProcessTracker.GetProcessCount() > 0) {
        statusMsg = "another child process running";
        status    = -EAGAIN;
        return;
    }
    if ((pid = DoFork(20 * 60, "meta-dump-map")) == 0) {
        // let the child write out the map; if the map is large, this'll
        // take several seconds.  we get the benefits of writing out the
        // map in the background while the metaserver continues to
        // process other RPCs
        gLayoutManager.DumpChunkToServerMap(gChunkmapDumpDir);
        _exit(0); // Child does not do graceful exit.
    }
    // if fork() failed, let the sender know
    if (pid < 0) {
        status = (int)pid;
        KFS_LOG_STREAM_ERROR << "failed to start chunk server map writer: " <<
            QCUtils::SysError(-status) <<
        KFS_LOG_EOM;
        return;
    }
    KFS_LOG_STREAM_INFO << "chunk to server map writer pid: " << pid <<
    KFS_LOG_EOM;
    // hold on to the request until the child  finishes
    ostringstream& os = GetTmpOStringStream();
    os << gChunkmapDumpDir << "/chunkmap.txt";
    chunkmapFile = os.str();
    suspended = true;
    gChildProcessTracker.Track(pid, this);
}

/* virtual */ void
MetaDumpChunkReplicationCandidates::handle()
{
    if (status < 0) {
        return;
    }
    if (! HasEnoughIoBuffersForResponse(*this)) {
        return;
    }
    if (! HasMetaServerAdminAccess(*this)) {
        return;
    }
    gLayoutManager.Handle(*this);
}

/* virtual */ void
MetaFsck::handle()
{
    suspended = false;
    resp.Clear();
    if (0 < pid) {
        if (! HasEnoughIoBuffersForResponse(*this)) {
            return;
        }
        // Child finished.
        pid = -1;
        if (status == 0) {
            int maxReadSize = min(
                gLayoutManager.GetMaxResponseSize(),
                sMaxFsckResponseSize);
            if (maxReadSize <= 0) {
                statusMsg = "out of io buffers";
                status    = -ENOMEM;
            } else {
                if (fd.empty()) {
                    statusMsg = "internal error";
                    status    = -EINVAL;
                    return;
                }
                for (Fds::const_iterator it = fd.begin();
                        maxReadSize > 0 &&
                            it != fd.end();
                        ++it) {
                    struct stat st = {0};
                    if (fstat(*it, &st) < 0) {
                        status    = -errno;
                        statusMsg = QCUtils::SysError(
                            -status);
                        if (status >= 0) {
                            status = -EIO;
                        }
                        break;
                    }
                    if (st.st_size <= 0 &&
                            it == fd.begin()) {
                        status = -EIO;
                        break;
                    }
                    const int nRead =
                        resp.Read(*it, maxReadSize);
                    if (st.st_size > maxReadSize &&
                            maxReadSize == nRead) {
                        ostream& os =
                            sWOStream.Set(resp);
                        os  <<
                            "\nWARNING: output"
                            " truncated to " <<
                            maxReadSize << " bytes"
                        "\n";
                        os.flush();
                        if (! os) {
                            resp.Clear();
                            statusMsg ="out of io "
                                "buffers";
                            status    = -ENOMEM;
                        }
                        sWOStream.Reset();
                        break;
                    }
                    if (nRead < st.st_size) {
                        statusMsg = "short read";
                        status    = -EIO;
                        break;
                    }
                    maxReadSize -= nRead;
                }
            }
        } else if (status > 0) {
            status = -status;
        }
        for (Fds::const_iterator it = fd.begin();
                it != fd.end();
                ++it) {
            close(*it);
        }
        fd.clear();
        if (status != 0) {
            status = status < 0 ? status : -EIO;
            if (statusMsg.empty()) {
                statusMsg = "fsck io failure";
            }
            resp.Clear();
        }
        return;
    }
    if (status < 0) {
        return;
    }
    if (! HasMetaServerAdminAccess(*this)) {
        return;
    }
    if (gChildProcessTracker.GetProcessCount() > 0) {
        statusMsg = "another child process running";
        status    = -EAGAIN;
        return;
    }
    const int cnt = gLayoutManager.FsckStreamCount(
        reportAbandonedFilesFlag);
    if (cnt <= 0) {
        statusMsg = "internal error";
        status    = -EINVAL;
        return;
    }
    const char* const    suffix    = ".XXXXXX";
    const size_t         suffixLen = strlen(suffix);
    StBufferT<char, 128> buf;
    vector<string>       names;
    names.reserve(cnt);
    fd.reserve(cnt);
    for (int i = 0; i < cnt; i++) {
        char* const ptr = buf.Reserve(sTmpName.length() + suffixLen + 1);
        memcpy(ptr, sTmpName.data(), sTmpName.size());
        strcpy(ptr + sTmpName.size(), suffix);
        const int tfd = mkstemp(ptr);
        if (tfd < 0) {
            status    = errno > 0 ? -errno : -EINVAL;
            statusMsg = "failed to create temporary file";
            while (--i >= 0) {
                close(fd[i]);
                unlink(names[i].c_str());
            }
            return;
        }
        fd.push_back(tfd);
        names.push_back(string(ptr));
    }
    if ((pid = DoFork((int)(gLayoutManager.GetMaxFsckTime() /
                    (1000 * 1000)), "meta-fsck")) == 0) {
        StBufferT<ostream*, 8> streamsPtrBuf;
        ostream** const ptr        = streamsPtrBuf.Resize(cnt + 1);
        ofstream* const streams    = new ofstream[cnt];
        bool            failedFlag = false;
        for (int i = 0; i < cnt; i++) {
            const char* const name = names[i].c_str();
            if (! failedFlag) {
                streams[i].open(name);
                ptr[i] = streams + i;
            }
            close(fd[i]);
            unlink(name);
            failedFlag = failedFlag || ! streams[i];
        }
        if (! failedFlag) {
            ptr[cnt] = 0;
            gLayoutManager.Fsck(ptr, reportAbandonedFilesFlag);
            failedFlag = false;
            for (int i = 0; i < cnt; i++) {
                streams[i].flush();
                streams[i].close();
                if (! streams[i]) {
                    failedFlag = true;
                    break;
                }
            }
            for (int i = 0; i < cnt; i++) {
                if (failedFlag) {
                    // Zero length file means error.
                    if (ftruncate(fd[i], 0) < 0) {
                        QCUtils::SetLastIgnoredError(errno);
                    }
                }
            }
        }
        delete [] streams;
        _exit(failedFlag ? 3 : 0); // Child does not attemp graceful exit.
    }
    if (pid < 0) {
        status    = (int)pid;
        statusMsg = QCUtils::SysError(-status, "fork failure");
        for (int i = 0; i < cnt; i++) {
            close(fd[i]);
            unlink(names[i].c_str());
        }
        return;
    }
    KFS_LOG_STREAM_INFO << "fsck pid: " << pid <<
    KFS_LOG_EOM;
    suspended = true;
    gChildProcessTracker.Track(pid, this);
}

void
MetaFsck::SetParameters(const Properties& props)
{
    sTmpName = props.getValue(
        "metaServer.fsck.tmpfile",             sTmpName);
    sMaxFsckResponseSize = props.getValue(
        "metaServer.fsck.maxFsckResponseSize", sMaxFsckResponseSize);
}

string MetaFsck::sTmpName("/tmp/kfsfsck.tmp");
int    MetaFsck::sMaxFsckResponseSize(20 << 20);

/* virtual */ void
MetaCheckLeases::handle()
{
    if (status < 0) {
        return;
    }
    if (! HasMetaServerAdminAccess(*this)) {
        return;
    }
    gLayoutManager.CheckAllLeases();
}

/* virtual */ void
MetaStats::handle()
{
    if (! HasMetaServerStatsAccess(*this)) {
        return;
    }
    ostringstream& os = GetTmpOStringStream();
    status = 0;
    globals().counterManager.Show(os);
    stats = os.str();
}

/* virtual */ void
MetaOpenFiles::handle()
{
    if (0 != status) {
        return;
    }
    if (! HasEnoughIoBuffersForResponse(*this)) {
        return;
    }
    if (! HasMetaServerAdminAccess(*this)) {
        return;
    }
    ReadInfo  openForRead;
    WriteInfo openForWrite;
    gLayoutManager.GetOpenFiles(openForRead, openForWrite);
    status = 0;
    ostream& os = sWOStream.Set(resp);
    for (ReadInfo::const_iterator it = openForRead.begin();
            it != openForRead.end();
            ++it) {
        os << it->first;
        for (std::vector<std::pair<chunkId_t, size_t> >::const_iterator
                i = it->second.begin();
                i != it->second.end();
                ++i) {
            if (! (os << "  " << i->first << " " << i->second)) {
                break;
            }
        }
        os << "\n";
    }
    os << "\n";
    for (WriteInfo::const_iterator it = openForWrite.begin();
            it != openForWrite.end();
            ++it) {
        os << it->first;
        for (std::vector<chunkId_t>::const_iterator
                i = it->second.begin();
                i != it->second.end();
                ++i) {
            if (! (os << " " << *i)) {
                break;
            }
        }
        os << "\n";
    }
    os.flush();
    if (! os) {
        resp.Clear();
        status    = -ENOMEM;
        statusMsg = "response exceeds max. size";
    } else {
        openForReadCnt  = openForRead.size();
        openForWriteCnt = openForWrite.size();
    }
    sWOStream.Reset();
}

/* virtual */ void
MetaSetChunkServersProperties::handle()
{
    if (0 != status) {
        return;
    }
    if (! HasMetaServerAdminAccess(*this)) {
        return;
    }
    status = (int)properties.size();
    gLayoutManager.SetChunkServersProperties(properties);
}

/* virtual */ void
MetaGetChunkServersCounters::handle()
{
    if (! HasEnoughIoBuffersForResponse(*this)) {
        return;
    }
    if (! HasMetaServerStatsAccess(*this)) {
        return;
    }
    status = 0;
    gLayoutManager.GetChunkServerCounters(resp);
}

/* virtual */ void
MetaGetChunkServerDirsCounters::handle()
{
    if (! HasEnoughIoBuffersForResponse(*this)) {
        return;
    }
    if (! HasMetaServerStatsAccess(*this)) {
        return;
    }
    status = 0;
    gLayoutManager.GetChunkServerDirCounters(resp);
}

/* virtual */ void
MetaGetRequestCounters::handle()
{
    if (! HasEnoughIoBuffersForResponse(*this)) {
        return;
    }
    if (! HasMetaServerStatsAccess(*this)) {
        return;
    }
    status = 0;
    gNetDispatch.GetStatsCsv(resp);
    userCpuMicroSec   = gNetDispatch.GetUserCpuMicroSec();
    systemCpuMicroSec = gNetDispatch.GetSystemCpuMicroSec();
}

/* virtual */ void
MetaCheckpoint::handle()
{
    const time_t now = globalNetManager().Now();
    suspended = false;
    if (0 < pid) {
        // Child finished.
        KFS_LOG_STREAM(status == 0 ?
                MsgLogger::kLogLevelINFO :
                MsgLogger::kLogLevelERROR) <<
            "checkpoint: "    << runningCheckpointId <<
            " pid: "          << pid <<
            " done; status: " << status <<
            " failures: "     << failedCount <<
        KFS_LOG_EOM;
        if (status < 0) {
            failedCount++;
        } else {
            failedCount = 0;
            lastCheckpointId = runningCheckpointId;
            const string fileName = cp.cpfile(lastCheckpointId);
            gNetDispatch.GetMetaDataStore().RegisterCheckpoint(
                fileName.c_str(),
                lastCheckpointId,
                runningCheckpointLogSegmentNum
            );
        }
        if (0 <= lockFd) {
            close(lockFd);
            lockFd = -1;
        }
        if (failedCount > maxFailedCount) {
            panic("checkpoint failures", false);
        }
        lastRunDoneTime = now;
        runningCheckpointId = MetaVrLogSeq();
        runningCheckpointLogSegmentNum = -(runningCheckpointLogSegmentNum + 1);
        pid = -1;
        return;
    }
    status = 0;
    statusMsg.clear();
    if (intervalSec <= 0) {
        return; // Disabled.
    }
    if (! lastCheckpointId.IsValid()) {
        // First call -- init.
        lastCheckpointId = replayer.getCheckpointCommitted();
        if (! lastCheckpointId.IsValid()) {
            lastCheckpointId = GetLogWriter().GetCommittedLogSeq();
        }
        lastRun = now;
        lastRunDoneTime = now;
        return;
    }
    if (! finishLog) {
        const MetaVrLogSeq committed = GetLogWriter().GetCommittedLogSeq();
        MetaVrLogSeq last;
        if (0 < flushNewViewDelaySec &&
                0 == GetLogWriter().GetVrStatus() &&
                committed < (last = replayer.getLastLogSeq()) &&
                flushViewLogSeq < last &&
                gLayoutManager.GetServiceStartTime() + 5 < now) {
            KFS_LOG_STREAM_DEBUG <<
                "flushing new view: "  << last <<
                " priror view flush: " << flushViewLogSeq <<
            KFS_LOG_EOM;
            flushViewLogSeq = last;
            submit_request(new MetaNoop());
        }
        if (now < lastRun + intervalSec || committed <= lastCheckpointId) {
            return;
        }
        if (0 <= lockFd) {
            close(lockFd);
            lockFd = -1;
        }
        if (! lockFileName.empty() &&
                (lockFd = try_to_acquire_lockfile(lockFileName)) < 0) {
            KFS_LOG_STREAM_INFO << "checkpoint: " <<
                " failed to acquire lock: " << lockFileName <<
                " " << QCUtils::SysError(-lockFd) <<
            KFS_LOG_EOM;
            return; // Retry later.
        }
        finishLog = new MetaLogWriterControl(
            MetaLogWriterControl::kCheckpointNewLog, this);
        suspended = true;
        submit_request(finishLog);
        return;
    }
    if (finishLog->status != 0) {
        KFS_LOG_STREAM_ERROR <<
            "failed to finish log:"
            " "        << finishLog->logName <<
            " status:" << finishLog->status <<
            " "        << finishLog->statusMsg <<
        KFS_LOG_EOM;
        if (0 <= lockFd) {
            close(lockFd);
            lockFd = -1;
        }
        finishLog = 0;
        return;
    }
    if (finishLog->logName.empty()) {
        panic("invalid empty next log segment name");
    }
    const MetaVrLogSeq committedSeq = GetLogWriter().GetCommittedLogSeq();
    if (committedSeq != finishLog->lastLogSeq) {
        KFS_LOG_STREAM_INFO <<
            "re-scheduling checkpoint due to pending view change:"
            " finish log: " << finishLog->lastLogSeq <<
            " committed: "  << committedSeq <<
        KFS_LOG_EOM;
        if (0 <= lockFd) {
            close(lockFd);
            lockFd = -1;
        }
        finishLog = 0;
        return;
    }
    if (committedSeq < finishLog->committed) {
        panic("invalid finish log committed sequence");
    }
    runningCheckpointId            = committedSeq;
    lastRun                        = now;
    runningCheckpointLogSegmentNum = finishLog->logSegmentNum;
    // DoFork() / PrepareCurrentThreadToFork() releases and re-acquires the
    // global mutex by waiting on condition with this mutex, but must ensure
    // that no other RPC gets processed. If log commit sequence has changed
    // after DoFork() invocation, then there is a bug with the prepare to
    // fork logic, and checkpoint will not be valid. In such case do not write
    // the checkpoint in the child, and "panic" the parent.
    if ((pid = DoFork(checkpointWriteTimeoutSec, "meta-checkpoint")) == 0) {
        MetaVrLogSeq logSeq;
        int64_t      errChecksum = -1;
        fid_t        fidSeed     = -1;
        int          commStatus  = -1;
        GetLogWriter().GetCommitted(logSeq, errChecksum, fidSeed, commStatus);
        if (runningCheckpointId != logSeq || fidSeed != fileID.getseed()) {
            status = -EINVAL;
        } else {
            metatree.disableFidToPathname();
            metatree.setUpdatePathSpaceUsage(true);
            cp.setWriteSyncFlag(checkpointWriteSyncFlag);
            cp.setWriteBufferSize(checkpointWriteBufferSize);
            status = cp.write(
                finishLog->logName,
                runningCheckpointId,
                errChecksum
            );
        }
        // Child does not attempt graceful exit.
        _exit(status == 0 ? 0 : 1);
    }
    if (GetLogWriter().GetCommittedLogSeq() != runningCheckpointId) {
        panic("checkpoint: meta data changed after prepare to fork");
    }
    finishLog = 0;
    if (pid < 0) {
        status = (int)pid;
        KFS_LOG_STREAM_ERROR <<
            "checkpoint: " << runningCheckpointId <<
            " fork failure: " << QCUtils::SysError(-status) <<
        KFS_LOG_EOM;
        return;
    }
    KFS_LOG_STREAM_INFO <<
        "checkpoint: " << lastCheckpointId <<
        " => "         << runningCheckpointId <<
        " pid: "       << pid <<
    KFS_LOG_EOM;
    suspended = true;
    gChildProcessTracker.Track(pid, this);
}

void
MetaCheckpoint::ScheduleNow()
{
    lastRun = globalNetManager().Now() - intervalSec - 1;
}

void
MetaCheckpoint::SetParameters(const Properties& props)
{
    intervalSec = props.getValue(
        "metaServer.checkpoint.interval",       intervalSec);
    lockFileName = props.getValue(
        "metaServer.checkpoint.lockFileName",   lockFileName);
    maxFailedCount = max(0, props.getValue(
        "metaServer.checkpoint.maxFailedCount", maxFailedCount));
    checkpointWriteTimeoutSec = max(0, (int)props.getValue(
        "metaServer.checkpoint.writeTimeoutSec",
        (double)checkpointWriteTimeoutSec));
    checkpointWriteSyncFlag = props.getValue(
        "metaServer.checkpoint.writeSync",
        checkpointWriteSyncFlag ? 0 : 1) != 0;
    checkpointWriteBufferSize = props.getValue(
        "metaServer.checkpoint.writeBufferSize",
        checkpointWriteBufferSize);
    flushNewViewDelaySec = props.getValue(
        "metaServer.checkpoint.flushNewViewDelaySec",
        flushNewViewDelaySec);
}

int*
MetaRequest::GetLogQueueCounter() const
{
    return ((fromClientSMFlag && clnt) ?
        &(static_cast<ClientSM*>(clnt)->GetLogQueueCounter()) : (int*)0
    );
}

void
MetaRequest::Submit(int64_t nowUsec)
{
    if (SubmitBegin(nowUsec)) {
        SubmitEnd();
    }
}

bool
MetaRequest::SubmitBegin(int64_t nowUsec)
{
    const int64_t tstart = nowUsec;
    if (++recursionCount <= 0) {
        panic("submit: invalid request recursion count");
    }
    if (1 != recursionCount) {
        KFS_LOG_STREAM_DEBUG <<
            "submit request recursion: " << recursionCount <<
            " logseq: " << seqno <<
            " opseq: "  << opSeqno <<
            " "         << Show() <<
        KFS_LOG_EOM;
    }
    if (0 == submitCount++) {
        submitTime  = tstart;
        processTime = tstart;
        if (GetLogWriter().Enqueue(*this)) {
            processTime = microseconds() - processTime;
            if (--recursionCount != 0) {
                panic("submit: invalid request recursion count");
            }
            return false;
        }
    } else {
        // accumulate processing time.
        processTime = tstart - processTime;
    }
    handle();
    return true;
}

void
MetaRequest::SubmitEnd()
{
    if (--recursionCount < 0) {
        panic("submit: invalid request recursion count");
    }
    if (0 < recursionCount) {
        KFS_LOG_STREAM_DEBUG <<
            "submit request end recursion: " << recursionCount <<
            " logseq: " << seqno <<
            " opseq: "  << opSeqno <<
            " "         << Show() <<
        KFS_LOG_EOM;
        return;
    }
    if (commitPendingFlag) {
        GetLogWriter().Committed(*this, fileID.getseed());
    }
    if (suspended) {
        processTime = microseconds() - processTime;
    } else {
        gNetDispatch.Dispatch(this);
    }
}

bool
MetaRequest::log(ostream& /* file */) const
{
    panic("invalid empty log method");
    return false;
}

void
MetaRequest::Init()
{
    MetaRequestsList::Init(*this);
    QCStMutexLocker locker(gNetDispatch.GetClientManagerMutex());
    MetaRequestsList::PushBack(sMetaRequestsPtr, *this);
    sMetaRequestCount++;
}

/* virtual */
MetaRequest::~MetaRequest()
{
    if (recursionCount != 0) {
        panic("invalid non 0 recursion count, in meta request destructor");
    }
    recursionCount = 0xF000DEAD;
    QCStMutexLocker locker(gNetDispatch.GetClientManagerMutex());
    MetaRequestsList::Remove(sMetaRequestsPtr, *this);
    sMetaRequestCount--;
}

/* static */ MetaChunkRequest::ChunkOpsInFlight::iterator
MetaChunkRequest::MakeNullIterator()
{
    static ChunkOpsInFlight sNull;
    return sNull.end();
}
const MetaChunkRequest::ChunkOpsInFlight::iterator
    MetaChunkRequest::kNullIterator(MetaChunkRequest::MakeNullIterator());

/* virtual */
MetaChunkRequest::~MetaChunkRequest()
{
    if (kNullIterator != inFlightIt) {
        panic("invalid in flight iterator in meta chunk request destructor");
    }
}

/* virtual */ void
MetaRequest::handle()
{
    status = -ENOSYS;  // Not implemented
}

/* static */ void
MetaRequest::SetParameters(const Properties& props)
{
    sRequireHeaderChecksumFlag = props.getValue(
        "metaServer.request.requireHeaderChecksum", 0) != 0;
    sVerifyHeaderChecksumFlag = props.getValue(
        "metaServer.request.verifyHeaderChecksum", 1) != 0;
}

/* static */ uint32_t
MetaRequest::Checksum(
    const char* name,
    size_t      nameLen,
    const char* header,
    size_t      headerLen)
{
    return ComputeBlockChecksum(
        ComputeBlockChecksum(name, nameLen), header, headerLen);
}

bool MetaRequest::sRequireHeaderChecksumFlag  = false;
bool MetaRequest::sVerifyHeaderChecksumFlag   = true;
int  MetaRequest::sMetaRequestCount           = 0;
MetaRequest* MetaRequest::sMetaRequestsPtr[1] = {0};

bool
MetaCreate::Validate()
{
    return (dir >= 0 && ! name.empty() && 0 <= numReplicas);
}

/*!
 * \brief Generate response (a string) for various requests that
 * describes the result of the request execution.  The generated
 * response string is based on the KFS protocol.  All follow the same
 * model:
 * @param[out] os: A string stream that contains the response.
 */
void
MetaLookup::response(ReqOstream& os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    if (authType != kAuthenticationTypeUndef) {
        os <<
        (shortRpcFormatFlag ? "VRP:" : "Vr-primary:") <<
            (primaryFlag ? 1 : 0) << "\r\n"  <<
        (shortRpcFormatFlag ? "A:" : "Auth-type: ") <<
            authType << "\r\n\r\n";
        return;
    }
    os <<
        (shortRpcFormatFlag ? "EU:" : "EUserId: ")  << euser  << "\r\n" <<
        (shortRpcFormatFlag ? "EG:" : "EGroupId: ") << egroup << "\r\n"
    ;
    if (authInfoOnlyFlag) {
        os <<
            (shortRpcFormatFlag ? "U:" : "User: ")  << authUid  << "\r\n" <<
            (shortRpcFormatFlag ? "G:" : "Group: ") << authGid << "\r\n"
        ;
        const UserAndGroupNames* const ugn = GetUserAndGroupNames(*this);
        UserAndGroupNamesReply(os, ugn, euser,   egroup, shortRpcFormatFlag,
            "E");
        UserAndGroupNamesReply(os, ugn, authUid, authGid, shortRpcFormatFlag) <<
            "\r\n";
    } else {
        FattrReply(os, fattr, GetUserAndGroupNames(*this),
            shortRpcFormatFlag) << "\r\n";
    }
}

void
MetaLookupPath::response(ReqOstream &os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os <<
        (shortRpcFormatFlag ? "Eu:" : "EUserId: ") << euser  << "\r\n" <<
        (shortRpcFormatFlag ? "Eg:" : "EGroupId: ") << egroup << "\r\n"
    ;
    FattrReply(os, fattr, GetUserAndGroupNames(*this),
        shortRpcFormatFlag) << "\r\n";
}

void
MetaCreate::response(ReqOstream &os)
{
    if (! IdempotentAck(os)) {
        return;
    }
    if (GetReq() && GetReq() != this) {
        // Copy fields, as GetUserAndGroupNames() won't work if requests are
        // handled by different client network threads.
        const MetaCreate& r = *static_cast<const MetaCreate*>(GetReq());
        fid         = r.fid;
        striperType = r.striperType;
        user        = r.user;
        group       = r.group;
        mode        = r.mode;
        minSTier    = r.minSTier;
        maxSTier    = r.maxSTier;
    }
    os << (shortRpcFormatFlag ? "P:" : "File-handle: ")  << fid << "\r\n";
    if (striperType != KFS_STRIPED_FILE_TYPE_NONE) {
        os << (shortRpcFormatFlag ? "ST:" : "Striper-type: ") <<
            striperType << "\r\n";
    }
    os <<
    (shortRpcFormatFlag ? "R:" : "Num-replicas: ") << numReplicas <<  "\r\n" <<
    (shortRpcFormatFlag ? "u:" : "User: ")         << user        <<  "\r\n" <<
    (shortRpcFormatFlag ? "g:" : "Group: ")        << group       <<  "\r\n" <<
    (shortRpcFormatFlag ? "M:" : "Mode: ")         << mode        <<  "\r\n"
    ;
    if (minSTier < kKfsSTierMax) {
        os <<
        (shortRpcFormatFlag ? "TL:" : "Min-tier: ") << (int)minSTier <<
            "\r\n" <<
        (shortRpcFormatFlag ? "TH:" : "Max-tier: ") << (int)maxSTier << "\r\n";
    }
    UserAndGroupNamesReply(os, GetUserAndGroupNames(*this), user, group,
        shortRpcFormatFlag) << "\r\n";
}

void
MetaRemove::response(ReqOstream &os)
{
    if (! IdempotentAck(os)) {
        return;
    }
    os << "\r\n";
}

void
MetaMkdir::response(ReqOstream &os)
{
    if (! IdempotentAck(os)) {
        return;
    }
    if (GetReq() && GetReq() != this) {
        // Copy fields, as GetUserAndGroupNames() won't work if requests are
        // handled by different client network threads.
        const MetaMkdir& r = *static_cast<const MetaMkdir*>(GetReq());
        fid      = r.fid;
        user     = r.user;
        group    = r.group;
        mode     = r.mode;
        minSTier = r.minSTier;
        maxSTier = r.maxSTier;
    }
    os <<
    (shortRpcFormatFlag ? "P:" : "File-handle: ") << fid   << "\r\n" <<
    (shortRpcFormatFlag ? "u:" : "User: ")        << user  << "\r\n" <<
    (shortRpcFormatFlag ? "g:" : "Group: ")       << group << "\r\n" <<
    (shortRpcFormatFlag ? "M:" : "Mode: ")        << mode  << "\r\n"
    ;
    if (minSTier < kKfsSTierMax) {
        os <<
        (shortRpcFormatFlag ? "TL:" : "Min-tier: ") << (int)minSTier <<
            "\r\n" <<
        (shortRpcFormatFlag ? "TH:" : "Max-tier: ") << (int)maxSTier << "\r\n";
    }
    UserAndGroupNamesReply(os, GetUserAndGroupNames(*this), user, group,
        shortRpcFormatFlag) << "\r\n";
}

void
MetaRmdir::response(ReqOstream &os)
{
    if (! IdempotentAck(os)) {
        return;
    }
    os << "\r\n";
}

void
MetaReaddir::response(ReqOstream& os, IOBuffer& buf)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os <<
        (shortRpcFormatFlag ? "EC:" : "Num-Entries: ") << numEntries <<
            "\r\n" <<
        (shortRpcFormatFlag ? "EM:" : "Has-more-entries: ") <<
            (hasMoreEntriesFlag ? 1 : 0) << "\r\n" <<
        (shortRpcFormatFlag ? "l:" : "Content-length: ")
            << resp.BytesConsumable() << "\r\n"
    "\r\n";
    os.flush();
    buf.Move(&resp);
}

void
MetaReaddirPlus::response(ReqOstream& os, IOBuffer& buf)
{
    if (! OkHeader(this, os)) {
        return;
    }
    size_t   entryCount;
    IOBuffer resp;
    if (numEntries >= 0) {
        ReaddirPlusWriter<true> writer(
            resp,
            maxRespSize,
            getLastChunkInfoOnlyIfSizeUnknown,
            omitLastChunkInfoFlag,
            fileIdAndTypeOnlyFlag);
        entryCount = writer.Write(dentries, lastChunkInfos,
            noAttrsFlag, GetUserAndGroupNames(*this));
    } else {
        ReaddirPlusWriter<false> writer(
            resp,
            maxRespSize,
            getLastChunkInfoOnlyIfSizeUnknown,
            omitLastChunkInfoFlag,
            fileIdAndTypeOnlyFlag);
        entryCount = writer.Write(dentries, lastChunkInfos,
            noAttrsFlag, GetUserAndGroupNames(*this));
    }
    hasMoreEntriesFlag = hasMoreEntriesFlag || entryCount < dentries.size();
    dentries.clear();
    lastChunkInfos.clear();
    if (ioBufPending > 0) {
        gLayoutManager.ChangeIoBufPending(-ioBufPending);
        ioBufPending = 0;
    }
    if (hasMoreEntriesFlag && numEntries < 0) {
        resp.Clear();
        status    = -ENOMEM;
        statusMsg = "response exceeds max. size";
        OkHeader(this, os);
        return;
    }
    os <<
        (shortRpcFormatFlag ? "EC:" : "Num-Entries: ")
            << entryCount << "\r\n" <<
        (shortRpcFormatFlag ? "EM:" : "Has-more-entries: ") <<
            (hasMoreEntriesFlag ? 1 : 0) << "\r\n" <<
        (shortRpcFormatFlag ? "l:" : "Content-length: ")
            << resp.BytesConsumable() << "\r\n"
    "\r\n";
    os.flush();
    buf.Move(&resp);
}

void
MetaRename::response(ReqOstream &os)
{
    if (! IdempotentAck(os)) {
        return;
    }
    os << "\r\n";
}

void
MetaLink::response(ReqOstream &os)
{
    if (! IdempotentAck(os)) {
        return;
    }
    if (GetReq() && GetReq() != this) {
        // Copy fields, as GetUserAndGroupNames() won't work if requests are
        // handled by different client network threads.
        const MetaLink& r = *static_cast<const MetaLink*>(GetReq());
        fid   = r.fid;
        user  = r.user;
        group = r.group;
        mode  = r.mode;
    }
    os <<
    (shortRpcFormatFlag ? "P:" : "File-handle: ") << fid   << "\r\n" <<
    (shortRpcFormatFlag ? "u:" : "User: ")        << user  << "\r\n" <<
    (shortRpcFormatFlag ? "g:" : "Group: ")       << group << "\r\n" <<
    (shortRpcFormatFlag ? "M:" : "Mode: ")        << mode  << "\r\n"
    ;
    UserAndGroupNamesReply(os, GetUserAndGroupNames(*this), user, group,
        shortRpcFormatFlag) << "\r\n";
}

void
MetaSetMtime::response(ReqOstream &os)
{
    PutHeader(this, os) << "\r\n";
}

void
MetaGetalloc::response(ReqOstream& os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os <<
        (shortRpcFormatFlag ? "H:" : "Chunk-handle: ") << chunkId << "\r\n" <<
        (shortRpcFormatFlag ? "V:" : "Chunk-version: ") << chunkVersion <<
        "\r\n";
    if (replicasOrderedFlag) {
        os << (shortRpcFormatFlag ? "O: 1\r\n" : "Replicas-ordered: 1\r\n");
    }
    os << (shortRpcFormatFlag ? "R:" : "Num-replicas: ") <<
        locations.size() << "\r\n";
    if (shortRpcFormatFlag && allChunkServersShortRpcFlag) {
        os << "SS:1\r\n";
    }
    assert(locations.size() > 0);
    os << (shortRpcFormatFlag ? "S:" : "Replicas:");
    for_each(locations.begin(), locations.end(), ListServerLocations(os));
    os << "\r\n\r\n";
}

void
MetaGetlayout::response(ReqOstream& os, IOBuffer& buf)
{
    if (! OkHeader(this, os)) {
        return;
    }
    if (hasMoreChunksFlag) {
        os << (shortRpcFormatFlag ? "MC:1\r\n" : "Has-more-chunks:  1\r\n");
    }
    if (0 <= fileSize) {
        os << (shortRpcFormatFlag ? "S:" : "File-size: ") << fileSize << "\r\n";
    }
    if (shortRpcFormatFlag && allChunkServersShortRpcFlag) {
        os << "SS:1\r\n";
    }
    os <<
        (shortRpcFormatFlag ? "C:" : "Num-chunks: ") << numChunks << "\r\n" <<
        (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
            resp.BytesConsumable() << "\r\n"
    "\r\n";
    os.flush();
    buf.Move(&resp);
}

void
MetaAllocate::response(ReqOstream& os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    writeChunkAccess(os);
    responseSelf(os);
}

void
MetaAllocate::writeChunkAccess(ReqOstream& os)
{
    if (authUid == kKfsUserNone) {
        return;
    }
    if (! responseAccessStr.empty()) {
        os.write(responseAccessStr.data(), responseAccessStr.size());
        return;
    }
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os << (shortRpcFormatFlag ? "LD:" : "Lease-duration: ") <<
        leaseDuration << "\r\n";
    if (validForTime <= 0) {
        return;
    }
    if (clientCSAllowClearTextFlag) {
        os << (shortRpcFormatFlag ? "CT:1\r\n" : "CS-clear-text: 1\r\n");
    }
    os <<
        (shortRpcFormatFlag ? "SI:" : "CS-acess-issued: ") <<
            issuedTime   << "\r\n" <<
        (shortRpcFormatFlag ? "ST:" : "CS-acess-time: ") <<
            validForTime << "\r\n" <<
        (shortRpcFormatFlag ? "SA:" : "CS-access: ");
    const int16_t kDelegationFlags = 0;
    DelegationToken::WriteTokenAndSessionKey(
        os.Get(),
        authUid,
        tokenSeq,
        writeMasterKeyId,
        issuedTime,
        kDelegationFlags,
        validForTime,
        writeMasterKey.GetPtr(),
        writeMasterKey.GetSize()
    );
    os <<
        "\r\n" <<
        (shortRpcFormatFlag ? "C:" : "C-access: ");
    ChunkAccessToken::WriteToken(
        os.Get(),
        0 == numReplicas ? fid : chunkId,
        authUid,
        tokenSeq,
        writeMasterKeyId,
        issuedTime,
        ChunkAccessToken::kAllowWriteFlag |
            (clientCSAllowClearTextFlag ?
                ChunkAccessToken::kAllowClearTextFlag : 0) |
            (0 == numReplicas ?
                ChunkAccessToken::kObjectStoreFlag : 0),
        LEASE_INTERVAL_SECS * 2,
        writeMasterKey.GetPtr(),
        writeMasterKey.GetSize()
    );
    os << "\r\n";
}

void
MetaAllocate::responseSelf(ReqOstream& os)
{
    if (status < 0) {
        return;
    }
    if (shortRpcFormatFlag) {
        os << hex;
    }
    if (! responseStr.empty()) {
        os.write(responseStr.data(), responseStr.size());
        return;
    }
    os <<
        (shortRpcFormatFlag ? "H:" : "Chunk-handle: ")  << chunkId << "\r\n" <<
        (shortRpcFormatFlag ? "V:" : "Chunk-version: ") << (0 == numReplicas ?
            -chunkVersion - 1 : chunkVersion) << "\r\n";
    if (appendChunk) {
        os << (shortRpcFormatFlag ? "O:" : "Chunk-offset: ") <<
            offset << "\r\n";
    }
    assert(! servers.empty() || invalidateAllFlag);
    if (! shortRpcFormatFlag && ! servers.empty()) {
        os << "Master: " << servers.front()->GetServerLocation() << "\r\n";
    }
    if (shortRpcFormatFlag && allChunkServersShortRpcFlag) {
        os << "SS:1\r\n";
    }
    os << (shortRpcFormatFlag ? "R:" : "Num-replicas: ") <<
        servers.size() << "\r\n";
    if (! servers.empty()) {
        if (shortRpcFormatFlag && ! allChunkServersShortRpcFlag) {
            os << "LF:1\r\n";
        }
        os << (shortRpcFormatFlag ? "S:" : "Replicas: ") <<
            InsertServers(servers) << "\r\n";
    }
    os << "\r\n";
}

void
MetaLeaseAcquire::response(ReqOstream& os, IOBuffer& buf)
{
    DelegationToken::TokenSeq tokenSeq =
        (DelegationToken::TokenSeq)(leaseId >> 1);
    const size_t              count    = chunkAccess.GetSize();
    if (status == 0 && 0 < count && leaseId <= 0 &&
            ! CryptoKeys::PseudoRand(&tokenSeq, sizeof(tokenSeq))) {
        status    = -EFAULT;
        statusMsg = "pseudo random generator failure";
    }
    if (! OkHeader(this, os)) {
        return;
    }
    if (leaseId >= 0) {
        os << (shortRpcFormatFlag ? "L:" : "Lease-id: ") << leaseId << "\r\n";
    }
    if (clientCSAllowClearTextFlag) {
        os << (shortRpcFormatFlag ? "CT:1\r\n" : "CS-clear-text: 1\r\n");
    }
    if (0 < count) {
        os << (shortRpcFormatFlag ? "SA:" : "CS-access: ") << count << "\r\n";
    }
    if (0 < validForTime) {
        os <<
            (shortRpcFormatFlag ? "SI:" : "CS-acess-issued: ") <<
                issuedTime   << "\r\n" <<
            (shortRpcFormatFlag ? "ST:" : "CS-acess-time: ")   <<
                validForTime << "\r\n";
    }
    if (! getChunkLocationsFlag && ! responseBuf.IsEmpty()) {
        os << (shortRpcFormatFlag ? "LS:" : "Lease-ids:");
        os.flush();
        responseBuf.CopyIn("\r\n", 2);
        buf.Move(&responseBuf);
    }
    if (0 < count) {
        IntIOBufferWriter            writer(responseBuf);
        const ChunkAccessInfo*       ptr = chunkAccess.GetPtr();
        const ChunkAccessInfo* const end = ptr + count;
        if (! responseBuf.IsEmpty() && ptr < end) {
            writer.Write("\n", 1);
        }
        while (ptr < end) {
            writer.WriteHexInt(ptr->chunkId);
            writer.Write(" ", 1);
            writer.Write(
                ptr->serverLocation.hostname.data(),
                ptr->serverLocation.hostname.size());
            writer.Write(" ", 1);
            writer.WriteHexInt(ptr->serverLocation.port);
            writer.Write(" ", 1);
            const int16_t kDelegationFlags = 0;
            if (ptr->authUid == kKfsUserNone) {
                writer.Write("? ? ?", 5);
            } else {
                DelegationToken::WriteTokenAndSessionKey(
                    writer,
                    ptr->authUid,
                    tokenSeq,
                    ptr->keyId,
                    issuedTime,
                    kDelegationFlags,
                    validForTime,
                    ptr->key.GetPtr(),
                    ptr->key.GetSize()
                );
                writer.Write(" ", 1);
                ChunkAccessToken::WriteToken(
                    writer,
                    ptr->chunkId,
                    ptr->authUid,
                    tokenSeq ^ (uint32_t)leaseId,
                    ptr->keyId,
                    issuedTime,
                    (appendRecoveryFlag ?
                        ChunkAccessToken::kAppendRecoveryFlag :
                        ChunkAccessToken::kAllowReadFlag) |
                        (clientCSAllowClearTextFlag ?
                            ChunkAccessToken::kAllowClearTextFlag : 0) |
                        (0 <= chunkPos ?
                            ChunkAccessToken::kObjectStoreFlag : 0),
                    leaseTimeout * 2,
                    ptr->key.GetPtr(),
                    ptr->key.GetSize()
                );
            }
            tokenSeq++;
            writer.Write("\n", 1);
            ptr++;
        }
        writer.Close();
    }
    const int len = responseBuf.BytesConsumable();
    if (len <= 0) {
        buf.CopyIn("\r\n", 2);
        return;
    }
    char tbuf[48];
    char* const end = tbuf + sizeof(tbuf);
    char* const pe  = end - 4;
    memcpy(pe, "\r\n\r\n", end - pe);
    char* const pn = shortRpcFormatFlag ?
        IntToHexString(len, pe) : IntToDecString(len, pe);
    char* const p  = pn - (shortRpcFormatFlag ? 2 : 16);
    memcpy(p, shortRpcFormatFlag ? "l:" : "Content-length: ", pn - p);
    buf.CopyIn(p, end - p);
    buf.Move(&responseBuf);
    return;
}

void
MetaLeaseRenew::response(ReqOstream& os, IOBuffer& buf)
{
    if (! OkHeader(this, os)) {
        return;
    }
    if (clientCSAllowClearTextFlag) {
        os << (shortRpcFormatFlag ? "CT:1\r\n" : "CS-clear-text: 1\r\n");
    }
    const size_t count = chunkAccess.GetSize();
    if (count <= 0) {
        os << "\r\n";
        return;
    }
    if (0 < validForTime) {
        os <<
            (shortRpcFormatFlag ? "SI:" : "CS-acess-issued: ") <<
                issuedTime   << "\r\n" <<
            (shortRpcFormatFlag ? "ST:" : "CS-acess-time: ")   <<
                validForTime << "\r\n";
    }
    IOBuffer                     iobuf;
    IntIOBufferWriter            writer(iobuf);
    const ChunkAccessInfo*       ptr = chunkAccess.GetPtr();
    const ChunkAccessInfo* const end = ptr + count;
    while (ptr < end) {
        if (leaseType != WRITE_LEASE) {
            writer.Write(
                ptr->serverLocation.hostname.data(),
                ptr->serverLocation.hostname.size());
            writer.Write(" ", 1);
            writer.WriteHexInt(ptr->serverLocation.port);
            writer.Write(" ", 1);
            if (0 < validForTime) {
                const int16_t kDelegationFlags = 0;
                DelegationToken::WriteTokenAndSessionKey(
                    writer,
                    ptr->authUid,
                    tokenSeq,
                    ptr->keyId,
                    issuedTime,
                    kDelegationFlags,
                    validForTime,
                    ptr->key.GetPtr(),
                    ptr->key.GetSize()
                );
                writer.Write(" ", 1);
            }
        }
        ChunkAccessToken::WriteToken(
            writer,
            ptr->chunkId,
            ptr->authUid,
            tokenSeq,
            ptr->keyId,
            issuedTime,
            (leaseType == WRITE_LEASE ?
                (DelegationToken::kChunkServerFlag |
                    ChunkAccessToken::kAllowWriteFlag) :
                ChunkAccessToken::kAllowReadFlag) |
                (clientCSAllowClearTextFlag ?
                    ChunkAccessToken::kAllowClearTextFlag : 0) |
                (0 <= chunkPos ? ChunkAccessToken::kObjectStoreFlag : 0),
            LEASE_INTERVAL_SECS * 2,
            ptr->key.GetPtr(),
            ptr->key.GetSize()
        );
        tokenSeq++;
        writer.Write("\n", 1);
        ptr++;
    }
    if (leaseType == WRITE_LEASE && 0 < validForTime &&
            (ptr = chunkAccess.GetPtr()) < end) {
        os << (shortRpcFormatFlag ? "AL:" : "C-access-length: ") <<
            writer.GetTotalSize() << "\r\n";
        DelegationToken::Subject* const kNoSubjectPtr = 0;
        const ChunkAccessInfo* prev = 0;
        do {
            DelegationToken::WriteTokenAndSessionKey(
                writer,
                ptr->authUid,
                tokenSeq,
                ptr->keyId,
                issuedTime,
                DelegationToken::kChunkServerFlag,
                validForTime,
                ptr->key.GetPtr(),
                ptr->key.GetSize(),
                kNoSubjectPtr,
                prev ? prev->keyId         : kfsKeyId_t(),
                prev ? prev->key.GetPtr()  : 0,
                prev ? prev->key.GetSize() : 0
            );
            writer.Write("\n", 1);
            prev = ptr++;
            tokenSeq++;
        } while (ptr < end);
    }
    writer.Close();
    os <<
        (shortRpcFormatFlag ? "C:" : "C-access: ") << count << "\r\n" <<
        (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
            iobuf.BytesConsumable() << "\r\n"
    "\r\n";
    os.flush();
    buf.Move(&iobuf);
}

void
MetaLeaseRelinquish::response(ReqOstream &os)
{
    PutHeader(this, os) << "\r\n";
}

void
MetaCoalesceBlocks::response(ReqOstream &os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os <<
        (shortRpcFormatFlag ? "O:" : "Dst-start-offset: ") <<
            dstStartOffset  << "\r\n" <<
        (shortRpcFormatFlag ? "MT:" : "M-Time: ") <<
            ShowTime(mtime) << "\r\n"
    "\r\n";
}

void
MetaHello::response(ReqOstream& os, IOBuffer& buf)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os << (shortRpcFormatFlag ? "SN:" : "Chunk-server-name: ") <<
        location.hostname << "\r\n";
    if (0 <= metaFileSystemId) {
        os << (shortRpcFormatFlag ? "FI:" : "File-system-id: ") <<
            metaFileSystemId << "\r\n";
        if (deleteAllChunksFlag) {
            os << (shortRpcFormatFlag ? "DA:" : "Delete-all-chunks: ") <<
                metaFileSystemId << "\r\n";
        }
    }
    if (0 <= resumeStep) {
        os << (shortRpcFormatFlag ? "R:" : "Resume: ") <<
            resumeStep   << "\r\n";
    }
    if (pendingNotifyFlag) {
        os << (shortRpcFormatFlag ? "PN:1" : "Pending-notify: 1") << "\r\n";
    }
    os << (shortRpcFormatFlag ? "MP:" : "Max-pending: ") <<
        maxPendingOpsCount << "\r\n";
    if (0 == resumeStep) {
        os <<
            (shortRpcFormatFlag ? "D:" : "Deleted: ") <<
                deletedCount << "\r\n" <<
            (shortRpcFormatFlag ? "M:" : "Modified: ") <<
                modifiedCount << "\r\n" <<
            (shortRpcFormatFlag ? "C:" : "Chunks: ") <<
                chunkCount << "\r\n" <<
            (shortRpcFormatFlag ? "K:" : "Checksum: ") <<
                checksum << "\r\n" <<
            (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
                responseBuf.BytesConsumable() << "\r\n"
            "\r\n"
        ;
        os.flush();
        buf.Move(&responseBuf);
    } else {
        os << "\r\n";
    }
}

void
MetaChunkCorrupt::response(ReqOstream &os)
{
    if (noReplyFlag) {
        return;
    }
    PutHeader(this, os) << "\r\n";
}

void
MetaChunkEvacuate::response(ReqOstream& os)
{
    PutHeader(this, os) << "\r\n";
}

void
MetaChunkAvailable::response(ReqOstream& os)
{
    // If stale notify set, then it will send response.
    if (staleNotify) {
        KFS_LOG_STREAM_DEBUG <<
            "scheduling reply:"
            " seq: "    << opSeqno <<
            " status: " << status <<
            " "         <<  Show() <<
            " after:"
            " "         << staleNotify->Show() <<
        KFS_LOG_EOM;
    } else {
        responseSelf(os);
    }
}

void
MetaChunkAvailable::responseSelf(ReqOstream& os)
{
    PutHeader(this, os) << "\r\n";
}

void
MetaChunkDirInfo::response(ReqOstream& os)
{
    if (noReplyFlag) {
        return;
    }
    PutHeader(this, os) << "\r\n";
}

void
MetaTruncate::response(ReqOstream &os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    if (endOffset >= 0) {
        os << (shortRpcFormatFlag ? "O:" : "End-offset: ") <<
            endOffset << "\r\n";
    }
    os << "\r\n";
}

void
MetaChangeFileReplication::response(ReqOstream &os)
{
    PutHeader(this, os) <<
        (shortRpcFormatFlag ? "R:" : "Num-replicas: ") <<
        numReplicas << "\r\n\r\n";
}

void
MetaRetireChunkserver::response(ReqOstream &os)
{
    PutHeader(this, os) << "\r\n";
}

void
MetaToggleWORM::response(ReqOstream &os)
{
    PutHeader(this, os) << "\r\n";
}

void
MetaPing::response(ReqOstream &os, IOBuffer& buf)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os.flush();
    buf.Move(&resp);
}

void
MetaUpServers::response(ReqOstream& os, IOBuffer& buf)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os << (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
        resp.BytesConsumable() << "\r\n\r\n";
    os.flush();
    buf.Move(&resp);
}

void
MetaStats::response(ReqOstream &os)
{
    PutHeader(this, os) << stats << "\r\n";
}

void
MetaCheckLeases::response(ReqOstream &os)
{
    PutHeader(this, os) << "\r\n";
}

void
MetaRecomputeDirsize::response(ReqOstream &os)
{
    PutHeader(this, os) << "\r\n";
}

void
MetaDumpChunkToServerMap::response(ReqOstream &os)
{
    PutHeader(this, os) << "Filename: " << chunkmapFile << "\r\n\r\n";
}

void
MetaDumpChunkReplicationCandidates::response(ReqOstream &os, IOBuffer& buf)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os <<
        "Num-replication: "       << numReplication         << "\r\n"
        "Num-pending-recovery: "  << numPendingRecovery     << "\r\n" <<
        (shortRpcFormatFlag ? "l:" : "Content-length: ")
            << resp.BytesConsumable() << "\r\n"
    "\r\n";
    os.flush();
    buf.Move(&resp);
}

void
MetaFsck::response(ReqOstream &os, IOBuffer& buf)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os  << (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
        resp.BytesConsumable() << "\r\n\r\n";
    os.flush();
    buf.Move(&resp);
}

void
MetaOpenFiles::response(ReqOstream& os, IOBuffer& buf)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os <<
        "Read: "  << openForReadCnt  << "\r\n"
        "Write: " << openForWriteCnt << "\r\n" <<
        (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
            resp.BytesConsumable() << "\r\n"
    "\r\n";
    os.flush();
    buf.Move(&resp);
}

void
MetaSetChunkServersProperties::response(ReqOstream &os)
{
    PutHeader(this, os) << "\r\n";
}

void
MetaGetChunkServersCounters::response(ReqOstream &os, IOBuffer& buf)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os << (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
        resp.BytesConsumable() << "\r\n\r\n";
    os.flush();
    buf.Move(&resp);
}

void
MetaGetChunkServerDirsCounters::response(ReqOstream &os, IOBuffer& buf)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os << (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
        resp.BytesConsumable() << "\r\n\r\n";
    os.flush();
    buf.Move(&resp);
}

void
MetaGetRequestCounters::response(ReqOstream &os, IOBuffer& buf)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os <<
        (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
            resp.BytesConsumable()  << "\r\n"
        "User-cpu-micro-sec: "  << userCpuMicroSec    << "\r\n"
        "System-cpu-mcro-sec: " << systemCpuMicroSec  << "\r\n"
    "\r\n";
    os.flush();
    buf.Move(&resp);
}

void
MetaGetPathName::response(ReqOstream& os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os << result;
    FattrReply(os, fattr, GetUserAndGroupNames(*this),
        shortRpcFormatFlag) << "\r\n";
}

void
MetaChmod::response(ReqOstream& os)
{
    PutHeader(this, os) << "\r\n";
}

void
MetaChown::response(ReqOstream& os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os <<
    (shortRpcFormatFlag ? "U:" : "User: ")  << user  << "\r\n" <<
    (shortRpcFormatFlag ? "G:" : "Group: ") << group << "\r\n"
    ;
    UserAndGroupNamesReply(os, GetUserAndGroupNames(*this), user, group,
        shortRpcFormatFlag) << "\r\n";
}

/* virtual */ bool
MetaAuthenticate::dispatch(ClientSM& sm)
{
    return sm.Handle(*this);
}

void
MetaAuthenticate::response(ReqOstream& os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os <<
        (shortRpcFormatFlag ? "A:" : "Auth-type: ") <<
            sendAuthType << "\r\n" <<
        (shortRpcFormatFlag ? "US:" : "Use-ssl: ") <<
            (filter ? 1 : 0) << "\r\n" <<
        (shortRpcFormatFlag ? "CT:" : "Curtime: ") <<
            (int64_t)time(0) << "\r\n" <<
        (shortRpcFormatFlag ? "ET:" : "Endtime: ") <<
            (int64_t)sessionExpirationTime << "\r\n"
    ;
    if (sendContentLen <= 0) {
        os << "\r\n";
        return;
    }
    os << (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
        sendContentLen << "\r\n"
    "\r\n";
    os.write(sendContentPtr, sendContentLen);
}

void
MetaAuthenticate::Request(ReqOstream& os) const
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
        "AUTHENTICATE\r\n" <<
        (shortRpcFormatFlag ? "c:" : "Cseq: ")      << opSeqno      << "\r\n" <<
        (shortRpcFormatFlag ? "A:" : "Auth-type: ") << sendAuthType << "\r\n"
    ;
    if (0 < sendContentLen) {
        os << (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
            sendContentLen << "\r\n";
    }
    os << "\r\n";
    os.write(sendContentPtr, sendContentLen);
}

/* virtual */ bool
MetaDelegate::dispatch(ClientSM& sm)
{
    return sm.Handle(*this);
}

void
MetaDelegate::response(ReqOstream& os)
{
    if (status == 0 && validForTime <= 0) {
        status    = -EINVAL;
        statusMsg = "invalid 0 delegation expiration time";
    }
    if (status == 0 && (! fromClientSMFlag || ! clnt)) {
        status    = -EPERM;
        statusMsg = "no client authentication";
    }
    if (status == 0 && authUid == kKfsUserNone) {
        status    = -EPERM;
        statusMsg = "not authenticated";
    }
    const CryptoKeys& keys  = gNetDispatch.GetCryptoKeys();
    CryptoKeys::KeyId keyId = 0;
    CryptoKeys::Key   key;
    uint32_t          keyValidForSec = 0;
    if (status == 0 && ! keys.GetCurrentKey(keyId, key, keyValidForSec)) {
        status    = -EAGAIN;
        statusMsg = "no valid key exists";
    }
    if (! OkHeader(this, os)) {
        return;
    }
    os <<
        (shortRpcFormatFlag ? "TI:" : "Issued-time: ") <<
            issuedTime << "\r\n" <<
        (shortRpcFormatFlag ? "TV:" : "Valid-for-time: ") <<
            validForTime << "\r\n";
    if (keyValidForSec < validForTime) {
        os << (shortRpcFormatFlag ? "TT:" : "Token-valid-for-time: ") <<
            keyValidForSec << "\r\n";
    }
    os << (shortRpcFormatFlag ? "A:" : "Access: ");
    DelegationToken::WriteTokenAndSessionKey(
        os.Get(),
        renewTokenStr.empty() ? authUid : renewToken.GetUid(),
        tokenSeq,
        keyId,
        issuedTime,
        delegationFlags,
        validForTime,
        key.GetPtr(),
        key.GetSize()
    );
    os <<
        "\r\n"
        "\r\n"
    ;
}

/*!
 * \brief Generate request (a string) that should be sent to the chunk
 * server.  The generated request string is based on the KFS
 * protocol.  All follow the same model:
 * @param[out] os: A string stream that contains the response.
 */
void
MetaChunkAllocate::request(ReqOstream& os)
{
    if (! req || META_ALLOCATE != req->op || req->servers.empty()) {
        panic("request: invalid meta chunk allocate op");
        return;
    }
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os << "ALLOCATE \r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << opSeqno << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: KFS/1.0\r\n";
    }
    os <<
        (shortRpcFormatFlag ? "P:" : "File-handle: ") <<
            req->fid << "\r\n" <<
        (shortRpcFormatFlag ? "H:" : "Chunk-handle: ") <<
            req->chunkId << "\r\n" <<
        (shortRpcFormatFlag ? "V:" : "Chunk-version: ") <<
            (0 == req->numReplicas ?
                -req->chunkVersion - 1 :
                req->chunkVersion) << "\r\n" <<
        (shortRpcFormatFlag ? "TL:" : "Min-tier: ") <<
            (int)minSTier << "\r\n" <<
        (shortRpcFormatFlag ? "TH:" : "Max-tier: ") <<
            (int)maxSTier     << "\r\n"
    ;
    if (0 <= leaseId) {
        os << (shortRpcFormatFlag ? "L:" : "Lease-id: ") << leaseId << "\r\n";
        if (req->clientCSAllowClearTextFlag) {
            os << (shortRpcFormatFlag ? "CT:1\r\n" : "CS-clear-text: 1\r\n");
        }
        // Update lease / "re-allocate" object store block only in the case when
        // client indicates access proxy (chunk server), otherwise create a new
        // object store block. This is needed to handle the case where an empty
        // block is created by the reply never reaches the client.
        if (0 == req->numReplicas && 0 < req->initialChunkVersion &&
                ! req->chunkServerName.empty()) {
            os << (shortRpcFormatFlag ? "E:1\r\n" : "Exists: 1\r\n");
        }
    }
    if (shortRpcFormatFlag && ! req->allChunkServersShortRpcFlag) {
        os << "LF:1\r\n";
    }
    os <<
        (shortRpcFormatFlag ? "CA:" : "Chunk-append: ") <<
            (req->appendChunk ? 1 : 0) << "\r\n" <<
        (shortRpcFormatFlag ? "R:" : "Num-servers: ") <<
            req->servers.size() << "\r\n" <<
        (shortRpcFormatFlag ? "S:" : "Servers: ") <<
            InsertServers(req->servers);
    ;
    const size_t cAccessLen = chunkAccessStr.size();
    const size_t len        = cAccessLen + chunkServerAccessStr.size();
    if (len <= 0) {
        os << "\r\n\r\n";
        return;
    }
    os <<
        "\r\n" <<
        (shortRpcFormatFlag ? "l:" : "Content-length: ") << len << "\r\n";
    if (cAccessLen < len) {
        os << (shortRpcFormatFlag ? "AL:" : "C-access-length: ") <<
            cAccessLen << "\r\n";
        if (0 < req->validForTime) {
            os <<
                (shortRpcFormatFlag ? "SI:" : "CS-acess-issued: ") <<
                    req->issuedTime   << "\r\n" <<
                (shortRpcFormatFlag ? "ST:" : "CS-acess-time: ") <<
                    req->validForTime << "\r\n";
        }
    }
    os << "\r\n";
    os.write(chunkAccessStr.data(),       cAccessLen);
    os.write(chunkServerAccessStr.data(), chunkServerAccessStr.size());
}

void
MetaChunkDelete::request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os << "DELETE \r\n";
    os << (shortRpcFormatFlag ? "c:" : "Cseq: ") << opSeqno << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: KFS/1.0\r\n";
    }
    os << (shortRpcFormatFlag ? "H:" : "Chunk-handle: ") <<
        chunkId << "\r\n";
    if (0 != chunkVersion) {
        os << (shortRpcFormatFlag ? "V:" : "Chunk-version: ") <<
            chunkVersion << "\r\n";
    }
    if (deleteStaleChunkIdFlag) {
        os << (shortRpcFormatFlag ? "S:1" : "Chunk-id-stale: 1") << "\r\n";
    }
    os << "\r\n";
}

void
MetaChunkHeartbeat::request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "HEARTBEAT \r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << opSeqno << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: KFS/1.0\r\n";
    }
    os << (shortRpcFormatFlag ? "E:" : "Num-evacuate: ") <<
        evacuateCount << "\r\n";
    os << (shortRpcFormatFlag ? "MP:" : "Max-pending: ") <<
        maxPendingOpsCount << "\r\n";
    if (reAuthenticateFlag) {
        os << (shortRpcFormatFlag ? "A:1\r\n" : "Authenticate: 1\r\n");
    }
    if (0 < recvTimeout) {
        os << (shortRpcFormatFlag ? "T:" : "Receive-timeout: ") <<
            recvTimeout << "\r\n";
    }
    if (omitCountersFlag) {
        os << (omitCountersFlag ? "O:1\r\n" : "Omit-counters: 1\r\n");
    }
    os << "\r\n";
}

static inline char*
ChunkIdToString(chunkId_t id, bool hexFormatFlag, char* end)
{
    return (hexFormatFlag ? IntToHexString(id, end) : IntToDecString(id, end));
}

MetaChunkStaleNotify::MetaChunkStaleNotify(const ChunkServerPtr& s,
    bool evacFlag, bool hexFmtFlag, MetaChunkAvailable* req)
    : MetaChunkRequest(META_CHUNK_STALENOTIFY, kLogNever, s, -1),
      staleChunkIds(),
      evacuatedFlag(evacFlag),
      hexFormatFlag(hexFmtFlag),
      flushStaleQueueFlag(false),
      chunkAvailableReq(0)
{
    if (req && ! req->staleNotify && server && req->clnt == &*server) {
        req->staleNotify  = this;
        chunkAvailableReq = req;
        if (req->ref <= 0) {
            panic("invalid chunk available request");
        }
        req->ref++;
    }
}

/* virtual */ ostream&
MetaChunkStaleNotify::ShowSelf(ostream& os) const
{
    os << "chunk-stale-notify:"
        " sseq: " << (chunkAvailableReq ? chunkAvailableReq->opSeqno : -1) <<
        " size: " << staleChunkIds.Size() <<
        " ids:"
    ;
    ChunkIdSet::ConstIterator it(staleChunkIds);
    const chunkId_t*          id;
    while (os && (id = it.Next())) {
        os << " " << *id;
    }
    return os;
}

/* virtual */ void
MetaChunkStaleNotify::request(ReqOstream& os, IOBuffer& buf)
{
    size_t const count = staleChunkIds.Size();
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
        "STALE_CHUNKS \r\n" <<
        (shortRpcFormatFlag ? "c:" : "Cseq: ") << opSeqno << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: KFS/1.0\r\n";
    }
    os << (shortRpcFormatFlag ? "C:" : "Num-chunks: ") << count << "\r\n";
    if (evacuatedFlag) {
        os << (shortRpcFormatFlag ? "E:1\r\n" : "Evacuated: 1\r\n");
    }
    if (hexFormatFlag) {
        os << (shortRpcFormatFlag ? "HF:1\r\n" : "HexFormat: 1\r\n");
    }
    if (chunkAvailableReq) {
        os << (shortRpcFormatFlag ? "AC:" : "AvailChunksSeq: ") <<
            chunkAvailableReq->opSeqno << "\r\n";
    }
    if (flushStaleQueueFlag) {
        os << (shortRpcFormatFlag ? "FQ:1" : "Flush-queue: 1") << "\r\n";
    }
    const int                 kBufEnd = 30;
    char                      tmpBuf[kBufEnd + 1];
    char* const               end = tmpBuf + kBufEnd;
    ChunkIdSet::ConstIterator it(staleChunkIds);
    if (count <= 1) {
        char* const p   = count < 1 ? end :
            ChunkIdToString(*it.Next(), hexFormatFlag, end);
        size_t      len = end - p;
        os << (shortRpcFormatFlag ? "K:" : "Content-chksum: ") <<
            ComputeBlockChecksum(p, len) << "\r\n";
        os << (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
            len << "\r\n\r\n";
        os.write(p, len);
    } else {
        const chunkId_t* id;
        IOBuffer         ioBuf;
        IOBufferWriter   writer(ioBuf);
        tmpBuf[kBufEnd] = (char)' ';
        while ((id = it.Next())) {
            char* const p = ChunkIdToString(*id, hexFormatFlag, end);
            writer.Write(p, (int)(end - p + 1));
        }
        writer.Close();
        const IOBuffer::BufPos len = ioBuf.BytesConsumable();
        os << (shortRpcFormatFlag ? "K:" : "Content-chksum: ") <<
            ComputeBlockChecksum(&ioBuf, max(IOBuffer::BufPos(0), len)) <<
            "\r\n";
        os << (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
            len << "\r\n\r\n";
        IOBuffer::iterator const bi    = ioBuf.begin();
        const IOBuffer::BufPos   defsz = IOBufferData::GetDefaultBufferSize();
        if (len < defsz - defsz / 4 &&
                bi != ioBuf.end() && len == bi->BytesConsumable()) {
            os.write(bi->Consumer(), len);
        } else {
            os.flush();
            buf.Move(&ioBuf);
        }
    }

    // Send available chunk reply immediately after.
    if (chunkAvailableReq) {
        if (this != chunkAvailableReq->staleNotify ||
                &*server != chunkAvailableReq->clnt) {
            panic("invalid stale notify op");
        }
        os.flush();
        chunkAvailableReq->responseSelf(os);
        os.flush();
    }
}

/* virtual */ void
MetaChunkStaleNotify::ReleaseSelf()
{
    if (chunkAvailableReq) {
        if (this != chunkAvailableReq->staleNotify ||
                &*server != chunkAvailableReq->clnt) {
            panic("invalid stale notify op release");
        }
        chunkAvailableReq->staleNotify = 0;
        MetaChunkAvailable* const aop = chunkAvailableReq;
        chunkAvailableReq = 0;
        Release(aop);
    }
    MetaChunkRequest::ReleaseSelf();
}

/* virtual */ void
MetaChunkAvailable::ReleaseSelf()
{
    if (--ref <= 0) {
        if (ref < 0 || staleNotify) {
            panic("chunk available: invalid ref. count");
        }
        MetaRequest::ReleaseSelf();
    }
}

void
MetaChunkRetire::request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os << "RETIRE \r\n";
    os << (shortRpcFormatFlag ? "c:" : "Cseq: ") << opSeqno << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: KFS/1.0\r\n";
    }
    os << "\r\n";
}

void
MetaChunkVersChange::request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "CHUNK_VERS_CHANGE \r\n" <<
        (shortRpcFormatFlag ? "c:" : "Cseq: ") << opSeqno << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: KFS/1.0\r\n";
    }
    os <<
        (shortRpcFormatFlag ? "P:" : "File-handle: ") <<
            fid << "\r\n" <<
        (shortRpcFormatFlag ? "H:" : "Chunk-handle: ")  <<
            chunkId << "\r\n" <<
        (shortRpcFormatFlag ? "VF:" : "From-chunk-version: ") <<
            fromVersion  << "\r\n" <<
        (shortRpcFormatFlag ? "V:" : "Chunk-version: ") <<
            chunkVersion << "\r\n";
    if (makeStableFlag) {
        os << (shortRpcFormatFlag ? "MS:1\r\n" : "Make-stable: 1\r\n");
    }
    if (verifyStableFlag) {
        os << (shortRpcFormatFlag ? "VV:1\r\n" : "Verify: 1\r\n");
    }
    os << "\r\n";
}

void
MetaBeginMakeChunkStable::request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os << "BEGIN_MAKE_CHUNK_STABLE\r\n" <<
        (shortRpcFormatFlag ? "c:" : "Cseq: ") << opSeqno << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: KFS/1.0\r\n";
    }
    os <<
        (shortRpcFormatFlag ? "P:" : "File-handle: ")   << fid << "\r\n" <<
        (shortRpcFormatFlag ? "H:" : "Chunk-handle: ")  << chunkId << "\r\n" <<
        (shortRpcFormatFlag ? "V:" : "Chunk-version: ") <<
            chunkVersion << "\r\n"
    "\r\n";
}

void
MetaChunkMakeStable::request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os << "MAKE_CHUNK_STABLE \r\n" <<
        (shortRpcFormatFlag ? "c:" : "Cseq: ") << opSeqno << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: KFS/1.0\r\n";
    }
    os <<
        (shortRpcFormatFlag ? "P:" : "File-handle: ")   << fid << "\r\n" <<
        (shortRpcFormatFlag ? "H:" : "Chunk-handle: ")  << chunkId << "\r\n" <<
        (shortRpcFormatFlag ? "V:" : "Chunk-version: ") <<
            chunkVersion << "\r\n" <<
        (shortRpcFormatFlag ? "S:" : "Chunk-size: ") << chunkSize << "\r\n";
    if (hasChunkChecksum) {
        os << (shortRpcFormatFlag ? "K:" : "Chunk-checksum: ") <<
            chunkChecksum << "\r\n";
    }
    os << "\r\n";
}

static const string sReplicateCmdName("REPLICATE");

void
MetaChunkReplicate::request(ReqOstream& os)
{
    // OK to use global here as chunk server state machine runs in the main
    // thread.
    ostringstream& rs = GetTmpOStringStream();
    if (shortRpcFormatFlag) {
        rs << hex;
    }
    rs <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << opSeqno << "\r\n";
    if (! shortRpcFormatFlag) {
        rs << "Version: KFS/1.0\r\n";
    }
    rs <<
    (shortRpcFormatFlag ? "P:"  : "File-handle: ")  << fid << "\r\n" <<
    (shortRpcFormatFlag ? "H:"  : "Chunk-handle: ") << chunkId << "\r\n" <<
    (shortRpcFormatFlag ? "TL:" : "Min-tier: ") << (int)minSTier << "\r\n" <<
    (shortRpcFormatFlag ? "TH:" : "Max-tier: ") << (int)maxSTier << "\r\n"
    ;
    if (numRecoveryStripes > 0) {
        rs <<
        (shortRpcFormatFlag ? "V:0\r\n" : "Chunk-version: 0\r\n") <<
        (shortRpcFormatFlag ? "O:" : "Chunk-offset: ") <<
            chunkOffset << "\r\n" <<
        (shortRpcFormatFlag ? "ST:" : "Striper-type: ") <<
            striperType << "\r\n" <<
        (shortRpcFormatFlag ? "SN:" : "Num-stripes: " ) <<
            numStripes << "\r\n" <<
        (shortRpcFormatFlag ? "SR:" : "Num-recovery-stripes: ") <<
            numRecoveryStripes   << "\r\n" <<
        (shortRpcFormatFlag ? "SS:" : "Stripe-size: ") <<
            stripeSize << "\r\n" <<
        (shortRpcFormatFlag ? "MP:" : "Meta-port: ") <<
            srcLocation.port << "\r\n" <<
        (shortRpcFormatFlag ? "VT:" : "Target-version: ") <<
            chunkVersion << "\r\n"
        ;
        if (0 < fileSize) {
            rs << (shortRpcFormatFlag ? "S:" : "File-size: ") <<
                fileSize << "\r\n";
        }
    } else {
        rs << (shortRpcFormatFlag ? "SC:" : "Chunk-location: ") <<
            srcLocation << "\r\n";
    }
    if (shortRpcFormatFlag && longRpcFormatFlag) {
        rs << "LF:1\r\n";
    }
    if (0 < validForTime) {
        if (clientCSAllowClearTextFlag) {
            rs << (shortRpcFormatFlag ? "CT:1\r\n" : "CS-clear-text: 1\r\n");
        }
        if (dataServer) {
            rs << (shortRpcFormatFlag ? "SA:" : "CS-access: ");
            DelegationToken::WriteTokenAndSessionKey(
                rs,
                authUid,
                tokenSeq,
                keyId,
                issuedTime,
                DelegationToken::kChunkServerFlag,
                validForTime,
                key.GetPtr(),
                key.GetSize()
            );
            rs << (shortRpcFormatFlag ?
                "\r\n"
                "C:" :
                "\r\n"
                "C-access: ");
            ChunkAccessToken::WriteToken(
                rs,
                chunkId,
                authUid,
                tokenSeq,
                keyId,
                issuedTime,
                ChunkAccessToken::kAllowReadFlag |
                    DelegationToken::kChunkServerFlag |
                    (clientCSAllowClearTextFlag ?
                        ChunkAccessToken::kAllowClearTextFlag : 0),
                LEASE_INTERVAL_SECS * 2,
                key.GetPtr(),
                key.GetSize()
            );
        } else {
            rs << (shortRpcFormatFlag ? "SA:" : "CS-access: ");
            if (metaServerAccess.empty()) {
                DelegationToken::WriteTokenAndSessionKey(
                    rs,
                    authUid,
                    tokenSeq,
                    keyId,
                    issuedTime,
                    DelegationToken::kChunkServerFlag,
                    validForTime,
                    key.GetPtr(),
                    key.GetSize()
                );
            } else {
                rs.write(metaServerAccess.data(), metaServerAccess.size());
            }
        }
        rs << "\r\n";
    }
    rs << "\r\n";
    const string req = rs.str();
    os << sReplicateCmdName << " " << Checksum(
        sReplicateCmdName.data(),
        sReplicateCmdName.size(),
        req.data(),
        req.size()) <<
    "\r\n";
    os.write(req.data(), req.size());
}

void
MetaChunkReplicate::handleReply(const Properties& prop)
{
    if (status == 0) {
        const seq_t cVers = prop.getValue(
            shortRpcFormatFlag ? "V" : "Chunk-version", seq_t(0));
        if (numRecoveryStripes <= 0) {
            chunkVersion = cVers;
        } else if (cVers != 0) {
            status    = -EINVAL;
            statusMsg = "invalid chunk version in reply";
            return;
        }
    }
    fid = prop.getValue((shortRpcFormatFlag ? "P" : "File-handle"), fid_t(0));
    invalidStripes.clear();
    const int sc = numStripes + numRecoveryStripes;
    if (status == 0 || sc <= 0) {
        return;
    }
    const string idxStr(prop.getValue(
        shortRpcFormatFlag ? "IS" : "Invalid-stripes", string()));
    if (idxStr.empty()) {
        return;
    }
    istringstream is(idxStr);
    is >> std::ws;
    while (! is.eof()) {
        int       idx       = -1;
        chunkId_t chunkId   = -1;
        seq_t     chunkVers = -1;
        if (! (is >> idx >> chunkId >> chunkVers >> std::ws) ||
                idx < 0 || sc <= idx ||
                (int)invalidStripes.size() >= sc - 1) {
            KFS_LOG_STREAM_ERROR << "replicate reply: parse error:"
                " pos: " << invalidStripes.size() <<
                " Invalid-stripes: " << idxStr <<
            KFS_LOG_EOM;
            invalidStripes.clear();
            break;
        }
        invalidStripes.insert(
            make_pair(idx, make_pair(chunkId, chunkVers)));
    }
}

void
MetaChunkDelete::handle()
{
    gLayoutManager.Done(*this);
}

void
MetaChunkSize::request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "SIZE \r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << opSeqno << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: KFS/1.0\r\n";
    }
    if (checkChunkFlag) {
        os << (shortRpcFormatFlag ? "K:1" : "Chunk-check: 1") << "\r\n";
    }
    os <<
    (shortRpcFormatFlag ? "P:" : "File-handle: ")   << fid          << "\r\n" <<
    (shortRpcFormatFlag ? "V:" : "Chunk-version: ") << chunkVersion << "\r\n" <<
    (shortRpcFormatFlag ? "H:" : "Chunk-handle: ")  << chunkId      << "\r\n"
    "\r\n";
}

void
MetaChunkSetProperties::request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "CMD_SET_PROPERTIES\r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << opSeqno << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: KFS/1.0\r\n";
    }
    os << (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
        serverProps.length() << "\r\n\r\n" <<
    serverProps
    ;
}

void
MetaChunkServerRestart::request(ReqOstream& os)
{
    os <<
    "RESTART_CHUNK_SERVER\r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << opSeqno << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: KFS/1.0\r\n";
    }
    os << "\r\n";
}

int
MetaAuthenticate::Read(IOBuffer& iobuf)
{
    int rem = contentLength - contentBufPos;
    if (contentBuf) {
        rem = iobuf.CopyOut(contentBuf + contentBufPos, rem);
    }
    contentBufPos += iobuf.Consume(rem);
    return (contentLength - contentBufPos);
}

/* virtual */ bool
MetaDelegateCancel::dispatch(ClientSM& sm)
{
    return sm.Handle(*this);
}

/* virtual */ void
MetaDelegateCancel::handle()
{
    if (status == 0  && ! gNetDispatch.CancelToken(
            tExp,
            tIssued,
            tUid,
            tSeq,
            tFlags)) {
        // status = -EEXIST;
        KFS_LOG_STREAM_DEBUG <<
            "token: " << tSeq << " already canceled" <<
        KFS_LOG_EOM;
    }
}

bool
MetaDelegateCancel::Validate()
{
    const CryptoKeys& keys = gNetDispatch.GetCryptoKeys();
    CryptoKeys::Key   key;
    if (! key.Parse(tokenKeyStr.GetPtr(), tokenKeyStr.GetSize())) {
        status    = -EINVAL;
        statusMsg = "invalid key format";
        return true;
    }
    char keyBuf[CryptoKeys::Key::kLength];
    const int len = token.Process(
        tokenStr.GetPtr(),
        (int)tokenStr.GetSize(),
        time(0),
        keys,
        keyBuf,
        CryptoKeys::Key::kLength,
        &statusMsg
    );
    if (len < 0) {
        status = len;
        return true;
    }
    if (len != key.GetSize() || memcmp(key.GetPtr(), keyBuf, len) != 0) {
        status    = -EINVAL;
        statusMsg = "invalid key";
    }
    return true;
}

void
MetaDelegateCancel::response(ReqOstream& os)
{
    PutHeader(this, os) << "\r\n";
}

void
MetaForceChunkReplication::handle()
{
    if (0 != status) {
        return;
    }
    if (! HasMetaServerAdminAccess(*this)) {
        return;
    }
    gLayoutManager.Handle(*this);
}

void
MetaForceChunkReplication::response(ReqOstream& os)
{
    PutHeader(this, os) << "\r\n";
}

MetaIdempotentRequest::~MetaIdempotentRequest()
{
    if (ref != 0 || this == req) {
        panic("MetaIdempotentRequest: invalid ref count");
    }
    if (req) {
        req->UnRef();
    }
    ref -= 1000; // To catch double delete.
}

bool
MetaIdempotentRequest::IdempotentAck(ReqOstream& os)
{
    if (req && req != this) {
        status    = req->status;
        statusMsg = req->statusMsg;
        ackId     = req->ackId;
    }
    PutHeader(this, os);
    if (0 <= ackId) {
        os << (shortRpcFormatFlag ? "a:" : "Ack: ");
        if (! shortRpcFormatFlag) {
            os << hex;
        }
        os << ackId;
        if (! shortRpcFormatFlag) {
            os << dec;
        }
        const TokenValue name = GetName(op);
        if (0 < name.mLen) {
            os << ' ';
            os.write(name.mPtr, name.mLen);
        }
        os << "\r\n";
    }
    if (0 <= status) {
        return true;
    }
    os << "\r\n";
    return false;
}

bool
MetaAck::start()
{
    // Do not set user and group for the ACK generated internally by the
    // idempotent tracker expiration logic.
    if (fromClientSMFlag || fromChunkServerFlag) {
        SetEUserAndEGroup(*this);
    }
    return (0 == status);
}

void
MetaAck::handle()
{
    // Always invoke request tracker, even in case of failure (status != 0)
    // in order to ensure that the acks generated by the request tracker timer
    // make back to the request tracker.
    gLayoutManager.GetIdempotentRequestTracker().Handle(*this);
}

bool
MetaRemoveFromDumpster::start()
{
    mtime = microseconds();
    return true;
}

void
MetaRemoveFromDumpster::handle()
{
    if (status == 0) {
        status = metatree.removeFromDumpster(
            fid, name, mtime, entriesCount, cleanupDoneFlag);
        if (0 != status) {
            panic("dumpster entry delete failure");
        }
    }
    gLayoutManager.Handle(*this);
}

bool
MetaLogClearObjStoreDelete::start()
{
    if (! gLayoutManager.IsObjectStoreDeleteEmpty()) {
        status = -EINVAL; // Re-scheduled with non empty queue.
    }
    return (0 == status);
}

void
MetaLogClearObjStoreDelete::handle()
{
    gLayoutManager.Handle(*this);
}

void
MetaReadMetaData::handle()
{
    if (handledFlag) {
        suspended = false;
        return;
    }
    if (status < 0) {
        if ((! allowNotPrimaryFlag && -EVRBACKUP != status) ||
                ! IsMetaLogWriteOrVrError(status)) {
            return;
        }
        status = 0;
    }
    if ((0 == submitCount && ! HasMetaServerAdminAccess(*this))) {
        return;
    }
    if (! HasEnoughIoBuffersForResponse(*this)) {
        return;
    }
    const int64_t fsId = metatree.GetFsId();
    if (fsId != fileSystemId) {
        if (0 <= fileSystemId) {
            status    = -EBADVERS;
            statusMsg = "file system id mismatch, expected: ";
            AppendDecIntToString(statusMsg, fsId);
        } else {
            fileSystemId = fsId;
        }
    }
    MetaVrLogSeq seq;
    if (startLogSeq.IsValid() && readPos <= 0 &&
            (seq = max(replayer.getLastLogSeq(),
                GetLogWriter().GetCommittedLogSeq())) < startLogSeq) {
        status    = -ERANGE;
        statusMsg = "requested log sequence higher than last and committed: ";
        AppendDecIntToString(statusMsg, seq.mEpochSeq);
        statusMsg += " ";
        AppendDecIntToString(statusMsg, seq.mViewSeq);
        statusMsg += " ";
        AppendDecIntToString(statusMsg, seq.mLogSeq);
    }
    handledFlag = true;
    gNetDispatch.GetMetaDataStore().Handle(*this);
}

void
MetaReadMetaData::response(ReqOstream& os, IOBuffer& buf)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os <<
    (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
        data.BytesConsumable() << "\r\n" <<
    (shortRpcFormatFlag ? "L:"  : "Start-log: ")   << startLogSeq << "\r\n" <<
    (shortRpcFormatFlag ? "FI:" : "FsId: ")        << fileSystemId << "\r\n" <<
    (shortRpcFormatFlag ? "M:" :  "Max-rd-size: ") << maxReadSize << "\r\n" <<
    (shortRpcFormatFlag ? "K:"  : "Crc32: ")       << checksum    << "\r\n";
    if (endLogSeq.IsValid()) {
        os << (shortRpcFormatFlag ? "E:" : "End-log: ") << endLogSeq << "\r\n";
    }
    if (0 <= fileSize) {
        os << (shortRpcFormatFlag ? "S:" : "Size: ") << fileSize << "\r\n";
    }
    if (! filename.empty()) {
        os << (shortRpcFormatFlag ? "N:" : "Name: ") << filename << "\r\n";
    }
    if (! clusterKey.empty()) {
        os << (shortRpcFormatFlag ? "CK:" : "Cluster-key: ") <<
            clusterKey << "\r\n";
    }
    if (! metaMd.empty()) {
        os << (shortRpcFormatFlag ? "MM:" : "Meta-md: ") <<
            metaMd << "\r\n";
    }
    os << "\r\n";
    os.flush();
    buf.Move(&data);
}

MetaChunkLogCompletion::MetaChunkLogCompletion(
    MetaChunkRequest* op)
    : MetaRequest(META_CHUNK_OP_LOG_COMPLETION, kLogIfOk),
      doneLocation(op ? op->server->GetServerLocation() : ServerLocation()),
      doneLogSeq(op ? op->logCompletionSeq : MetaVrLogSeq()),
      doneStatus(op ? op->status : 0),
      doneKfsStatus(doneStatus < 0 ? SysToKfsErrno(-doneStatus) : 0),
      doneTimedOutFlag(
        op &&
        op->timedOutFlag &&
        0 <= op->chunkId &&
        0 <= op->chunkVersion &&
        META_CHUNK_DELETE      != op->op &&
        META_CHUNK_STALENOTIFY != op->op &&
        META_CHUNK_SIZE        != op->op),
      doneOp(op),
      chunkId(op ? op->chunkId : chunkId_t(-1)),
      chunkVersion(doneTimedOutFlag ? op->chunkVersion : seq_t(-1)),
      chunkOpType(MetaChunkLogCompletion::kChunkOpTypeNone),
      staleChunkIdFlag(op && op->staleChunkIdFlag),
      flushStaleQueueFlag(op && META_CHUNK_STALENOTIFY == op->op &&
        static_cast<const MetaChunkStaleNotify*>(op)->flushStaleQueueFlag)
{
    if (op && op->pendingAddFlag &&
            0 <= op->chunkId && 0 <= op->chunkVersion) {
        chunkId      = op->chunkId;
        chunkVersion = op->chunkVersion;
        chunkOpType  = kChunkOpTypeAdd;
    }
}

/* virtual */ ostream&
MetaChunkLogCompletion::ShowSelf(ostream& os) const
{
    os <<
        "log-chunk-completion:"
        " "           << doneLocation <<
        " logseq: "   << logseq <<
        " doneseq: "  << doneLogSeq <<
        " status: "   << doneStatus <<
        " map op: "   << chunkOpType <<
        " chunk: "    << chunkId <<
        " version: "  << chunkVersion <<
        " timedout: " << doneTimedOutFlag
    ;
    if (doneOp) {
        os <<
            " seq: "    << doneOp->opSeqno <<
            " status: " << doneOp->status
        ;
        if (0 != doneOp->status && ! doneOp->statusMsg.empty()) {
            os << " msg: " << doneOp->statusMsg;
        }
        os << " " << doneOp->Show();
    }
    return os;
}

/* virtual */ bool
MetaChunkLogCompletion::start()
{
    if (! doneOp || ! doneOp->server || doneOp->server->IsDown()) {
        // Handle re-submit after server down event.
        status = -EINVAL;
    }
    return (0 == status);
}

void
MetaChunkLogCompletion::handle()
{
    if (0 != doneKfsStatus) {
        doneStatus = -KfsToSysErrno(doneKfsStatus);
    }
    gLayoutManager.Handle(*this);
}

MetaChunkLogInFlight::MetaChunkLogInFlight(
    MetaChunkRequest* req,
    int               timeout,
    bool              removeFlag)
    : MetaChunkRequest(
        META_CHUNK_OP_LOG_IN_FLIGHT,
        kLogIfOk,
        req ? req->server  : ChunkServerPtr(),
        req ? req->chunkId : chunkId_t(-1)),
      location(req ? req->server->GetServerLocation() : ServerLocation()),
      chunkIds(),
      idCount(-1),
      removeServerFlag(removeFlag),
      processPendingDownFlag(false),
      request(req),
      reqType(req ? req->op : -1)
{
    if (request) {
        if (! request->server) {
            panic("invalid log in flight request: no server");
            status = -EFAULT;
        }
        if (request->server->IsDownOrPendingDown()) {
            panic("invalid log in flight request: server down or pending down");
            status = -EFAULT;
        }
    }
    maxWaitMillisec = timeout;
}

/* virtual */ bool
MetaChunkLogInFlight::start()
{
    if (! server || server->IsDown()) {
        // Handle re-submit after server down event, primarily to make it
        // work with transaction log write error simulation.
        status = -EINVAL;
    }
    if (0 == status && server->IsDownOrPendingDown()) {
        panic("invalid log in flight request start: server down or pending down");
        status = -EFAULT;
    }
    return (0 == status);
}

/* static */ bool
MetaChunkLogInFlight::Log(MetaChunkRequest& req, int timeout,
    bool removeServerFlag)
{
    if (! IsToBeLogged(req)) {
        return false;
    }
    submit_request(new MetaChunkLogInFlight(&req, timeout,
        ! req.staleChunkIdFlag &&
            (META_CHUNK_DELETE == req.op || META_CHUNK_STALENOTIFY == req.op ||
            removeServerFlag)
    ));
    return true;
}

/* static */ bool
MetaChunkLogInFlight::Checkpoint(ostream& os, const MetaChunkRequest& req)
{
    if (META_CHUNK_OP_LOG_IN_FLIGHT != req.op &&
            ! req.logCompletionSeq.IsValid()) {
        return false;
    }
    const bool kRemoveFlag = false; // Removal is already done at this point.
    const int  kTimeout    = -1;
    MetaChunkLogInFlight lreq(
        const_cast<MetaChunkRequest*>(&req), kTimeout, kRemoveFlag);
    lreq.hadPendingChunkOpFlag = req.hadPendingChunkOpFlag;
    if (META_CHUNK_OP_LOG_IN_FLIGHT == req.op) {
        const MetaChunkLogInFlight& cur = static_cast<const MetaChunkLogInFlight&>(req);
        lreq.logseq                 = cur.logseq;
        lreq.reqType                = cur.reqType;
        lreq.processPendingDownFlag = cur.processPendingDownFlag;
    } else {
        lreq.logseq = req.logCompletionSeq;
    }
    if (! lreq.logseq.IsValid()) {
        panic("checkpoint chunk in flight: invalid log sequence");
        return false;
    }
    const bool kOmitDefaultsFlag = true;
    return (lreq.start() && lreq.WriteLog(os, kOmitDefaultsFlag) && os);
}

/* virtual */ ostream&
MetaChunkLogInFlight::ShowSelf(ostream& os) const
{
    os << "log-chunk-in-flight:"
        " "          << location <<
        " logseq: "  << logseq <<
        " type: "    << GetReqName(reqType) <<
        " chunk: "   << chunkId <<
        " version: " << chunkVersion <<
        " remove: "  << removeServerFlag <<
        (hadPendingChunkOpFlag ? " had-pending-op" : "") <<
        (processPendingDownFlag ? " process-pending-down" : "")
    ;
    if (! chunkIds.IsEmpty()) {
        os << " chunks: size: " << chunkIds.Size() << " ids:";
        ChunkIdSet::ConstIterator it(chunkIds);
        const chunkId_t*          id;
        while (os && (id = it.Next())) {
            os << " " << *id;
        }
    }
    if (request) {
        os << " " << request->Show();
    }
    return os;
}

void
MetaChunkLogInFlight::handle()
{
    if (replayFlag && 0 <= chunkId && 0 <= chunkVersion) {
        // Advance seed if needed,
        chunkID.setseed(max(chunkID.getseed(), chunkId));
    }
    gLayoutManager.Handle(*this);
}

inline static chunkId_t
GetFirtChunkId(const MetaChunkRequest::ChunkIdSet& ids, chunkId_t chunkId)
{
    MetaChunkRequest::ChunkIdSet::ConstIterator it(ids);
    const chunkId_t* const id = it.Next();
    return (id ? *id : chunkId);
}

bool
MetaChunkLogInFlight::log(ostream& os) const
{
    const ChunkIdSet*             ids = 0;
    const MetaChunkRequest* const req =
        request ? request : (replayFlag ? this : 0);
    if (! req || (req->chunkId < 0 && ! (ids = req->GetChunkIds()) &&
            ! req->logCompletionSeq.IsValid())) {
        panic("invalid MetaChunkLogInFlight log attempt");
        return false;
    }
    ReqOstream ros(os);
    size_t subEntryCnt = 1;
    const char* const name = GetReqName(reqType);
    if (ids && 1 != ids->Size()) {
        const size_t kEntrySizeLog2 = 6;
        const size_t kMask          = (size_t(1) << kEntrySizeLog2) - 1;
        subEntryCnt += (ids->Size() + kMask) >> kEntrySizeLog2;
        ros <<
            "cif"
            "/e/" << subEntryCnt <<
            "/l/" << location <<
            "/s/" << ids->Size() <<
            "/c/" << chunkId_t(-1) <<
            "/x/" << (removeServerFlag ? 1 : 0) <<
            "/r/" << name <<
            (hadPendingChunkOpFlag ? "/p" : "") <<
            (processPendingDownFlag ? "" : "/n" ) <<
            "/z/" << logseq
        ;
        size_t                    cnt = 0;
        ChunkIdSet::ConstIterator it(*ids);
        const chunkId_t*          id;
        while ((id = it.Next())) {
            if ((cnt++ & kMask) == 0) {
                ros << "\ncis";
                subEntryCnt--;
            }
            ros << "/" << *id;
        }
        if (1 != subEntryCnt) {
            panic("MetaChunkLogInFlight: internal error");
        }
    } else {
        ros <<
            "cif"
            "/e/" << subEntryCnt <<
            "/l/" << location <<
            "/s/" << size_t(0) <<
            "/c/" << (ids ? GetFirtChunkId(*ids, req->chunkId) : req->chunkId) <<
            "/x/" << (removeServerFlag ? 1 : 0) <<
            "/r/" << name <<
            (hadPendingChunkOpFlag ? "/p" : "") <<
            (processPendingDownFlag ? "" : "/n" ) <<
            "/z/" << logseq
        ;
    }
    ros << "\n";
    return true;
}

MetaChunkLogInFlight::NameTable const MetaChunkLogInFlight::sNameTable[] = {
    // Only the chunk ops that are logged using "log in flight".
    // Presently intended for debugging.
    { META_CHUNK_ALLOCATE,          { "ALC"  }},
    { META_CHUNK_DELETE,            { "DEL"  }},
    { META_CHUNK_STALENOTIFY,       { "STL"  }},
    { META_BEGIN_MAKE_CHUNK_STABLE, { "BMCS" }},
    { META_CHUNK_MAKE_STABLE,       { "MCS"  }},
    { META_CHUNK_VERSCHANGE,        { "VC"   }},
    { META_CHUNK_REPLICATE,         { "REPL" }},
    { META_CHUNK_SIZE,              { "SZ"   }},
    { META_CHUNK_OP_LOG_IN_FLIGHT,  { "LIF"  }},
    { -1, { "" }} // Sentinel
};

/* static */ const char*
MetaChunkLogInFlight::GetReqName(int type)
{
    for (const NameTable* ptr = sNameTable; *ptr->name; ++ptr) {
        if (ptr->id == type) {
            return ptr->name;
        }
    }
    return "";
}

/* static */ int
MetaChunkLogInFlight::GetReqId(const char* name, size_t len)
{
    for (const NameTable* ptr = sNameTable; *ptr->name; ++ptr) {
        if (strncmp(ptr->name, name, len) == 0) {
            return ptr->id;
        }
    }
    return -1;
}

/* virtual */ void
MetaSetGroupUsers::handle()
{
    if (0 == status) {
        gLayoutManager.GetUserAndGroup().Handle(*this);
    }
}

/* virtual */ void
MetaHibernatedPrune::handle()
{
    gLayoutManager.Handle(*this);
}

/* virtual */ void
MetaHibernatedRemove::handle()
{
    gLayoutManager.Handle(*this);
}

bool
MetaVrRequest::ResponseHeader(ReqOstream& os)
{
    const bool kCheckStatusFlag = false;
    return OkHeader(this, os, kCheckStatusFlag);
}

/* virtual */ void
MetaVrRequest::handle()
{
    // No status check, as commit scheduling does no depend on status.
    if (! mScheduleCommitFlag) {
        return;
    }
    mScheduleCommitFlag = false;
    if (mCommittedSeq.IsValid() &&
            mCommittedSeq <= replayer.getLastLogSeq() &&
            replayer.getCommitted() < mCommittedSeq &&
            ! replayer.runCommitQueue(
                mCommittedSeq,
                mCommittedFidSeed,
                mCommittedStatus,
                mCommittedErrChecksum)) {
        panic("invalid VR request commit");
    }
}

/* virtual */ void
MetaLogWriterControl::handle()
{
    if (completion) {
        if (! completion->suspended) {
            panic("writer control: invalid completion");
        }
        submit_request(completion);
        return;
    }
    if (kWriteBlock == type && 0 == status) {
        replayer.handle(*this);
    }
}

/* virtual */ bool
MetaVrReconfiguration::start()
{
    if (0 == status && ! Validate()) {
        status = -EINVAL;
        return false;
    }
    if (! HasMetaServerAdminAccess(*this)) {
        return false;
    }
    StIdempotentRequestHandler handler(*this);
    if (handler.IsDone()) {
        return true;
    }
    if (0 != status) {
        return false;
    }
    if (! GetLogWriter().GetMetaVrSM().HasValidNodeId()) {
        status    = -ENOENT;
        statusMsg = "no valid VR node id assigned by configuration";
        return false;
    }
    return true;
}

/* virtual */ void
MetaVrReconfiguration::handle()
{
    if (IsHandled()) {
        return;
    }
    GetLogWriter().GetMetaVrSM().Handle(*this, MetaVrLogSeq());
}

/* virtual */ void
MetaVrReconfiguration::response(ReqOstream& os, IOBuffer& buf)
{
    if (! IdempotentAck(os)) {
        return;
    }
    const int len = mResponse.BytesConsumable();
    if (0 < len) {
        os << (shortRpcFormatFlag ? "l:" : "Content-length: ") << len << "\r\n"
        "\r\n";
        os.flush();
        buf.Copy(&mResponse, len);
    } else {
        os << "\r\n";
    }
}

/* virtual */ void
MetaVrLogStartView::handle()
{
    replayer.handle(*this);
}

/* virtual */ bool
MetaVrGetStatus::start()
{
    return HasMetaServerAdminAccess(*this);
}

/* virtual */ void
MetaVrGetStatus::response(ReqOstream& os, IOBuffer& buf)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os << (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
            mResponse.BytesConsumable() << "\r\n"
    "\r\n";
    os.flush();
    buf.Move(&mResponse);
}

MetaProcessRestart::RestartPtr MetaProcessRestart::sRestartPtr = 0;

} /* namespace KFS */
