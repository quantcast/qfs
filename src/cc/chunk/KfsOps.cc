//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/05/26
// Author: Sriram Rao
//
// Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
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
// Code for parsing commands sent to the Chunkserver and generating
// responses that summarize the result of their execution.
//
//
//----------------------------------------------------------------------------

#include "KfsOps.h"

#include "ChunkManager.h"
#include "ChunkServer.h"
#include "LeaseClerk.h"
#include "Replicator.h"
#include "AtomicRecordAppender.h"
#include "ClientSM.h"
#include "utils.h"
#include "MetaServerSM.h"
#include "ClientManager.h"

#include "common/Version.h"
#include "common/kfstypes.h"
#include "common/time.h"
#include "common/RequestParser.h"
#include "common/kfserrno.h"
#include "common/IntToString.h"

#include "kfsio/Globals.h"
#include "kfsio/checksum.h"
#include "kfsio/CryptoKeys.h"
#include "kfsio/ChunkAccessToken.h"

#include "qcdio/qcstutils.h"
#include "qcdio/QCUtils.h"

#include <algorithm>
#include <iomanip>
#include <stdlib.h>

#ifdef KFS_OS_NAME_SUNOS
#include <sys/loadavg.h>
#endif

namespace KFS {

using std::map;
using std::string;
using std::ostringstream;
using std::istream;
using std::ostream;
using std::for_each;
using std::vector;
using std::min;
using std::make_pair;
using std::hex;
using std::dec;
using std::max;
using std::streamsize;
using namespace KFS::libkfsio;

// Counters for the various ops
struct OpCounters : private map<KfsOp_t, Counter *>
{
    static void Update(KfsOp_t opName, int64_t startTime)
    {
        Counter* const c = GetCounter(opName);
        if (! c) {
            return;
        }
        c->Update(1);
        c->UpdateTime(microseconds() - startTime);
    }
    static void WriteMaster()
    {
        if (! sInstance) {
            return;
        }
        sInstance->mWriteMaster.Update(1);
    }
    static void WriteDuration(int64_t time)
    {
        if (! sInstance) {
            return;
        }
        sInstance->mWriteDuration.Update(1);
        sInstance->mWriteDuration.UpdateTime(time);
    }
    static void Init()
    {
        if (! sInstance) {
            sInstance = MakeInstance();
        }
    }
private:
    Counter mWriteMaster;
    Counter mWriteDuration;
    static OpCounters* sInstance;

    OpCounters()
        : map<KfsOp_t, Counter *>(),
          mWriteMaster("Write Master"),
          mWriteDuration("Write Duration")
      {}
    ~OpCounters()
    {
        for (iterator i = begin(); i != end(); ++i) {
            if (sInstance == this) {
                globals().counterManager.RemoveCounter(i->second);
            }
            delete i->second;
        }
        if (sInstance == this) {
            globals().counterManager.RemoveCounter(&mWriteMaster);
            globals().counterManager.RemoveCounter(&mWriteDuration);
            sInstance = 0;
        }
    }
    void AddCounter(const char *name, KfsOp_t opName)
    {
        Counter* const c = new Counter(name);
        if (! insert(make_pair(opName, c)).second) {
            delete c;
            return;
        }
        globals().counterManager.AddCounter(c);
    }
    static Counter* GetCounter(KfsOp_t opName)
    {
        if (! sInstance) {
            return 0;
        }
        OpCounters::iterator iter = sInstance->find(opName);
        if (iter == sInstance->end()) {
            return 0;
        }
        return iter->second;
    }
    static OpCounters* MakeInstance()
    {
        // ensure that globals constructed first
        globals();
        static OpCounters instance;
        instance.AddCounter("Read", CMD_READ);
        instance.AddCounter("Write Prepare", CMD_WRITE_PREPARE);
        instance.AddCounter("Write Sync", CMD_WRITE_SYNC);
        instance.AddCounter("Write (AIO)", CMD_WRITE);
        instance.AddCounter("Size", CMD_SIZE);
        instance.AddCounter("Record append", CMD_RECORD_APPEND);
        instance.AddCounter("Space reserve", CMD_SPC_RESERVE);
        instance.AddCounter("Space release", CMD_SPC_RELEASE);
        instance.AddCounter("Get Chunk Metadata", CMD_GET_CHUNK_METADATA);
        instance.AddCounter("Alloc", CMD_ALLOC_CHUNK);
        instance.AddCounter("Delete", CMD_DELETE_CHUNK);
        instance.AddCounter("Truncate", CMD_TRUNCATE_CHUNK);
        instance.AddCounter("Replicate", CMD_REPLICATE_CHUNK);
        instance.AddCounter("Heartbeat", CMD_HEARTBEAT);
        instance.AddCounter("Change Chunk Vers", CMD_CHANGE_CHUNK_VERS);
        instance.AddCounter("Make Chunk Stable", CMD_MAKE_CHUNK_STABLE);
        globals().counterManager.AddCounter(&instance.mWriteMaster);
        globals().counterManager.AddCounter(&instance.mWriteDuration);
        return &instance;
    }
}* OpCounters::sInstance(0);

template <typename T, typename VP> inline static bool
needToForwardToPeer(
    const VP*       /* parserType */,
    T&              serverInfo,
    uint32_t        numServers,
    int&            myPos,
    ServerLocation& peerLoc,
    bool            writeIdPresentFlag,
    int64_t&        writeId)
{
    const char*       ptr        = serverInfo.data();
    const char* const end        = ptr + serverInfo.size();
    ServerLocation    loc;
    int64_t           id         = -1;
    bool              foundLocal = false;

    // the list of servers is ordered: we forward to the next one
    // in the list.
    for (uint32_t i = 0; i < numServers; i++) {
        while (ptr < end && (*ptr & 0xFF) <= ' ') {
            ptr++;
        }
        const char* const s = ptr;
        while (ptr < end && ' ' < (*ptr & 0xFF)) {
            ptr++;
        }
        if (ptr <= s) {
            break;
        }
        loc.hostname.assign(s, ptr - s);
        if (! VP::Parse(ptr, end - ptr, loc.port) ||
                (writeIdPresentFlag && ! VP::Parse(ptr, end - ptr, id))) {
            break;
        }
        // forward if we are not the last in the list
        if (foundLocal) {
            peerLoc = loc;
            return true;
        }
        if (gChunkServer.IsLocalServer(loc)) {
            // return the position of where this server is present in the list
            myPos = i;
            if (writeIdPresentFlag) {
                writeId = id;
            }
            foundLocal = true;
        }
    }
    return false;
}

template <typename T>
inline static bool
needToForwardToPeer(
    bool            shortRpcFormatFlag,
    T&              serverInfo,
    uint32_t        numServers,
    int&            myPos,
    ServerLocation& peerLoc,
    bool            writeIdPresentFlag,
    int64_t&        writeId)
{
    return (shortRpcFormatFlag ?
        needToForwardToPeer(
            static_cast<const HexIntParser*>(0),
            serverInfo,
            numServers,
            myPos,
            peerLoc,
            writeIdPresentFlag,
            writeId) :
        needToForwardToPeer(
            static_cast<const DecIntParser*>(0),
            serverInfo,
            numServers,
            myPos,
            peerLoc,
            writeIdPresentFlag,
            writeId));
}

template<typename VP, typename ST, typename CT>
inline static void
ConvertServersSelf(const VP* /* parserType */,
    ST& os, const char* str, size_t len, CT numServers, bool writeIdPresentFlag)
{
    // Port numbers and write id radix conversion.
    const char*       ptr = str;
    const char* const end = ptr + len;
    for (CT i = 0; i < numServers; i++) {
        const char* const s = ptr;
        while (ptr < end && (*ptr & 0xFF) <= ' ') {
            ptr++;
        }
        while (ptr < end && ' ' < (*ptr & 0xFF)) {
            ptr++;
        }
        while (ptr < end && (*ptr & 0xFF) <= ' ') {
            ptr++;
        }
        if (ptr <= s) {
            break;
        }
        const char* const e    = ptr;
        int               port = -1;
        int64_t           id   = -1;
        if (! VP::Parse(ptr, end - ptr, port) ||
                (writeIdPresentFlag && ! VP::Parse(ptr, end - ptr, id))) {
            break;
        }
        os.write(s, e - s);
        os << port;
        if (writeIdPresentFlag) {
            os.write(" ", 1);
            os << id;
        }
    }
}

template<typename ST, typename CT>
inline static void
ConvertServers(bool initialShortRpcFormatFlag,
    ST& os, const char* str, size_t len, CT numServers, bool writeIdPresentFlag)
{
    if (initialShortRpcFormatFlag) {
        ConvertServersSelf(static_cast<const HexIntParser*>(0),
            os, str, len, numServers, writeIdPresentFlag);
    } else {
        ConvertServersSelf(static_cast<const DecIntParser*>(0),
            os, str, len, numServers, writeIdPresentFlag);
    }
}

template<typename T, int TRadix>
class StringAppender
{
public:
    StringAppender(T& str)
        : mString(str)
        {}
    template<typename TI>
    StringAppender<T, TRadix>& operator<<(TI val)
    {
        IntToString<TRadix>::Append(mString, val);
        return *this;
    }
    StringAppender<T, TRadix>& write(const char* ptr, size_t len)
    {
        mString.append(ptr, len);
        return *this;
    }
private:
    T& mString;
};

template<typename TD, typename TS, typename SC>
inline static TD&
AppendServers(TD& dst, const TS& src, SC numServers,
    bool dstShortFmtFlag, bool srcShortFmtFlag, bool writeIdPresentFlag = true)
{
    if (dstShortFmtFlag == srcShortFmtFlag) {
        dst.append(src.data(), src.size());
    } else if (dstShortFmtFlag) {
        StringAppender<TD, 16> os(dst);
        ConvertServers(srcShortFmtFlag,
            os, src.data(), src.size(), numServers, writeIdPresentFlag);
    } else {
        StringAppender<TD, 10> os(dst);
        ConvertServers(srcShortFmtFlag,
            os, src.data(), src.size(), numServers, writeIdPresentFlag);
    }
    return dst;
}

template<typename ST, typename T>
inline static ST&
WriteServers(ST& os, const T& op,
    bool shortRpcFormatFlag, bool writeIdPresentFlag)
{
    os << (shortRpcFormatFlag ? "R:" : "Num-servers: ") <<
        op.numServers << "\r\n";
    if (op.numServers <= 0) {
        return os;
    }
    os << (shortRpcFormatFlag ? "S:" : "Servers: ");
    if (shortRpcFormatFlag == op.initialShortRpcFormatFlag) {
        os << op.servers;
    } else {
        ConvertServers(op.initialShortRpcFormatFlag,
            os, op.servers.data(), op.servers.size(), op.numServers,
            writeIdPresentFlag);
    }
    return (os << "\r\n");
}

template<typename T>
inline static ReqOstream&
WriteServers(ReqOstream& os, const T& op, bool writeIdPresentFlag = true)
{
    return WriteServers(os, op, op.shortRpcFormatFlag, writeIdPresentFlag);
}

template<typename T>
static inline RemoteSyncSMPtr
FindPeer(
    T&                    op,
    const ServerLocation& loc,
    bool                  writeMasterFlag,
    bool                  allowCSClearTextFlag)
{
    ClientSM* const csm = op.GetClientSM();
    if (! csm) {
        return RemoteSyncSMPtr();
    }
    SRChunkServerAccess::Token token;
    SRChunkServerAccess::Token key;
    if (op.syncReplicationAccess.chunkServerAccess) {
        const SRChunkServerAccess& csa =
            *op.syncReplicationAccess.chunkServerAccess;
        token = csa.token;
        key   = csa.key;
    }
    const bool kConnectFlag = true;
    return csm->FindServer(
        loc,
        kConnectFlag,
        token.mPtr,
        token.mLen,
        key.mPtr,
        key.mLen,
        writeMasterFlag,
        allowCSClearTextFlag,
        op.shortRpcFormatFlag,
        op.status,
        op.statusMsg
    );
}

void
SubmitOp(KfsOp* op)
{
    op->type = OP_REQUEST;
    op->Execute();
}

void
SubmitOpResponse(KfsOp* op)
{
    op->type = OP_RESPONSE;
    op->HandleEvent(EVENT_CMD_DONE, op);
}

inline static void
WriteSyncReplicationAccess(
    const SyncReplicationAccess& sra,
    ReqOstream&                  os,
    bool                         shortFmtFlag,
    const char*                  contentLengthHeader)
{
    const SRChunkAccess::Token* cFwd = 0;
    if (sra.chunkAccess) {
        const SRChunkAccess& ra = *sra.chunkAccess;
        os << (shortFmtFlag ? "C:" : "C-access: ");
            os.write(ra.token.mPtr, ra.token.mLen) << "\r\n";
        cFwd = &ra.fwd;
    }
    const SRChunkServerAccess::Token* csFwd = sra.chunkServerAccess ?
        &sra.chunkServerAccess->fwd : 0;
    int         len = (csFwd && 0 < csFwd->mLen) ? csFwd->mLen : 0;
    const char* sep = 0;
    if (cFwd && 0 < cFwd->mLen) {
        os << (shortFmtFlag ? "AL:" : "C-access-length: ") <<
            cFwd->mLen << "\r\n";
        if (0 < len &&
                ' ' < (cFwd->mPtr[cFwd->mLen] & 0xFF) &&
                ' ' < (csFwd->mPtr[0] & 0xFF)) {
            sep = "\n";
            len++;
        }
        len += cFwd->mLen;
    }
    if (0 < len) {
        os << contentLengthHeader << len << "\r\n";
    }
    os << "\r\n";
    if (cFwd && 0 < cFwd->mLen) {
        os.write(cFwd->mPtr, cFwd->mLen);
        if (sep) {
            os << sep;
        }
    }
    if (csFwd && 0 < csFwd->mLen) {
        os.write(csFwd->mPtr, csFwd->mLen);
    }
}

template<typename T>
class FwdAccessParser
{
public:
    static inline T* Parse(istream& is, int len)
    {
        if (len <= 0) {
            return 0;
        }
        char* const tokensStr = new char[len + 1];
        is.read(tokensStr, (streamsize)len);
        if (is.gcount() != (streamsize)len) {
            delete [] tokensStr;
            return 0;
        }
        tokensStr[len] = 0;
        typename T::Token tokens[T::kTokenCount];
        const char* p = tokensStr;
        for (int i = 0; ; i++) {
            while (*p && (*p & 0xFF) <= ' ') {
                ++p;
            }
            if (T::kTokenCount <= i + 1) {
                tokens[i] = typename T::Token(p, tokensStr + len - p);
                break;
            }
            const char* const b = p;
            while (' ' < (*p & 0xFF)) {
                ++p;
            }
            tokens[i] = typename T::Token(b, p - b);
        }
        if (tokens[max(0, T::kTokenCount - 2)].mLen <= 0) {
            delete [] tokensStr;
            return 0;
        }
        return new T(tokens, tokensStr);
    }
};

SRChunkAccess*
SRChunkAccess::Parse(istream& is, int len)
{
    return FwdAccessParser<SRChunkAccess>::Parse(is, len);
}

SRChunkServerAccess*
SRChunkServerAccess::Parse(istream& is, int len)
{
    return FwdAccessParser<SRChunkServerAccess>::Parse(is, len);
}

bool
SyncReplicationAccess::Parse(istream& is, int chunkAccessLength, int len)
{
    if (len <= 0) {
        return (chunkAccessLength <= 0);
    }
    if (len < chunkAccessLength) {
        return false;
    }
    const int caLen = 0 < chunkAccessLength ? chunkAccessLength : len;
    chunkAccess.reset(SRChunkAccess::Parse(is, caLen));
    if (! chunkAccess) {
        return false;
    }
    if (len <= caLen) {
        return true;
    }
    chunkServerAccess.reset(SRChunkServerAccess::Parse(is, len - caLen));
    return !!chunkServerAccess;
}

class KfsOp::NullOp : public KfsOp
{
protected:
    NullOp()
        : KfsOp(CMD_NULL)
        {}
    virtual void Execute()
        {}
    virtual ostream& ShowSelf(ostream& os) const
        { return os << "null"; }
    static const KfsOp& sNullOp;
    friend struct KfsOp;
};

/* static */ const KfsOp&
KfsOp::GetNullOp()
{
    static const NullOp sNullOp;
    return sNullOp;
}

class KfsOp::CleanupChecker
{
public:
    CleanupChecker()
    {
        if (0 != KfsOp::GetOpsCount()) {
            abort();
        }
    }
    ~CleanupChecker()
    {
        const int64_t cnt = KfsOp::GetOpsCount();
        if (0 == cnt && 0 == globals().ctrOpenNetFds.GetValue() &&
                0 == globals().ctrOpenDiskFds.GetValue()) {
            return;
        }
        char buffer[] = { "error: ops / sockets / disk fds at exit"
                          "                "
                          "                "
                          "                \n" };
        const size_t sz = sizeof(buffer) / sizeof(buffer[0]);
        IntToDecString(cnt,
            IntToDecString(globals().ctrOpenNetFds.GetValue(),
                IntToDecString(globals().ctrOpenDiskFds.GetValue(),
                    buffer + sz - 1) - 1) - 1
        );
        if (write(2, buffer, sizeof(buffer))) {
            QCUtils::SetLastIgnoredError(errno);
        }
        if (KfsOp::GetExitDebugCheckFlag()) {
            abort();
        }
    }
};

/* static */ bool
KfsOp::Init()
{
    static bool doneFlag = false;
    if (! doneFlag) {
        doneFlag = true;
        OpCounters::Init();
        if (0 != KfsOp::GetOpsCount()) {
            const char* const msg = "invalid kfs op init invocation\n";
            if (write(2, msg, strlen(msg))) {
                QCUtils::SetLastIgnoredError(errno);
            }
            abort();
        }
        OpsList::Init(sOpsList);
        static CleanupChecker sChecker;
        GetNullOp();
    }
    return doneFlag;
}

inline void
KfsOp::UpdateStatus(int code, const void* data)
{
    if (code != EVENT_DISK_ERROR || status < 0) {
        return;
    }
    status = data ? *reinterpret_cast<const int*>(data) : -EIO;
    if (0 <= status) {
        status = -EIO;
    }
    if (statusMsg.empty()) {
        statusMsg = status != -ETIMEDOUT ? "IO error" : "IO timed out";
    }
}

inline int
KfsOp::Submit()
{
    if (! clnt) {
        die("KfsOp null client");
        return -1;
    }
    return clnt->HandleEvent(EVENT_CMD_DONE, this);
}

inline int
KfsOp::Submit(int ret)
{
    if (0 <= ret) {
        // Completion might be already invoked; this might not be valid.
        return 0;
    }
    if (0 <= status) {
        status = ret;
    }
    return Submit();
}

QCMutex* KfsOp::sMutex              = 0;
int64_t  KfsOp::sOpsCount           = 0;
KfsOp*   KfsOp::sOpsList[1]         = {0};
bool     KfsOp::sExitDebugCheckFlag = false;

KfsOp::KfsOp(KfsOp_t o)
    : KfsCallbackObj(),
      op(o),
      type(OP_REQUEST),
      seq(-1),
      status(0),
      cancelled(false),
      done(false),
      noReply(false),
      clientSMFlag(false),
      shortRpcFormatFlag(false),
      initialShortRpcFormatFlag(false),
      maxWaitMillisec(-1),
      statusMsg(),
      clnt(0),
      generation(0),
      startTime(microseconds()),
      bufferBytes(),
      next(0),
      nextOp()
{
    OpsList::Init(*this);
    SET_HANDLER(this, &KfsOp::HandleDone);
    QCStMutexLocker theLocker(sMutex);
    sOpsCount++;
    OpsList::PushBack(sOpsList, *this);
}

KfsOp::~KfsOp()
{
    QCStMutexLocker theLocker(sMutex);
    if (sOpsCount <= 0 || op <= CMD_UNKNOWN || CMD_NCMDS <= op) {
        die("~KfsOp: invalid instance");
        return;
    }
    OpCounters::Update(op, startTime);
    sOpsCount--;
    OpsList::Remove(sOpsList, *this);
    clnt = 0;
    const_cast<KfsOp_t&>(op) = CMD_UNKNOWN; // To catch double delete.
}

/* static */ uint32_t
KfsOp::Checksum(
    const char* name,
    size_t      nameLen,
    const char* header,
    size_t      headerLen)
{
    return ComputeBlockChecksum(
        ComputeBlockChecksum(name, nameLen), header, headerLen);
}

/* virtual */ bool
KfsOp::CheckAccess(ClientSM& sm)
{
    return sm.CheckAccess(*this);
}

/* static */ BufferManager*
KfsOp::FindDeviceBufferManager(kfsChunkId_t chunkId, int64_t chunkVersion)
{
    return gChunkManager.FindDeviceBufferManager(chunkId, chunkVersion);
}

bool
KfsClientChunkOp::Validate()
{
    if (! KfsOp::Validate()) {
        return false;
    }
    if ((hasChunkAccessTokenFlag = ! chunkAccessVal.empty())) {
        ChunkAccessToken token;
        if ((chunkAccessTokenValidFlag = token.Process(
                chunkId,
                chunkAccessVal.mPtr,
                chunkAccessVal.mLen,
                globalNetManager().Now(),
                gChunkManager.GetCryptoKeys(),
                &statusMsg,
                subjectId))) {
            chunkAccessUid   = token.Get().GetUid();
            chunkAccessFlags = token.Get().GetFlags();
        }
        chunkAccessVal.clear();
    }
    return true;
}

/* virtual */ bool
KfsClientChunkOp::CheckAccess(ClientSM& sm)
{
    return sm.CheckAccess(*this);
}

/* virtual */ bool
ChunkAccessRequestOp::CheckAccess(ClientSM& sm)
{
    return sm.CheckAccess(*this);
}

void
ChunkAccessRequestOp::WriteChunkAccessResponse(
    ReqOstream& os, int64_t subjectId, int accessTokenFlags)
{
    if (status < 0 ||
            ! hasChunkAccessTokenFlag ||
            ! chunkAccessTokenValidFlag) {
        return;
    }
    const ClientSM* const csm = GetClientSM();
    if (! csm) {
        return;
    }
    const DelegationToken& token = csm->GetDelegationToken();
    if (token.GetValidForSec() <= 0) {
        return;
    }
    DelegationToken::TokenSeq tokenSeq = 0;
    if (! CryptoKeys::PseudoRand(&tokenSeq, sizeof(tokenSeq))) {
        return;
    }
    CryptoKeys::KeyId keyId       = -1;;
    CryptoKeys::Key   key;
    uint32_t          validForSec = 0;
    if (! gChunkManager.GetCryptoKeys().GetCurrentKey(
            keyId, key, validForSec) || validForSec <= 0) {
        return;
    }
    const time_t now = globalNetManager().Now();
    os <<
        (shortRpcFormatFlag ? "SI:" : "Acess-issued: ") << now << "\r\n" <<
        (shortRpcFormatFlag ? "ST:" : "Acess-time: ")   <<
            validForSec << "\r\n";
    if (createChunkAccessFlag) {
        os << (shortRpcFormatFlag ? "C:" : "C-access: ");
        ChunkAccessToken::WriteToken(
            os.Get(),
            chunkId,
            token.GetUid(),
            tokenSeq++,
            keyId,
            now,
            (chunkAccessFlags & (
                DelegationToken::kChunkServerFlag |
                ChunkAccessToken::kAllowReadFlag |
                ChunkAccessToken::kAllowWriteFlag |
                ChunkAccessToken::kAllowClearTextFlag |
                ChunkAccessToken::kObjectStoreFlag)) |
                accessTokenFlags,
            validForSec,
            key.GetPtr(),
            key.GetSize(),
            subjectId
        );
        os << "\r\n";
    }
    if (createChunkServerAccessFlag) {
        os << (shortRpcFormatFlag ? "SA:" : "CS-access: ");
        // Session key must not be empty if communication is in clear text, in
        // order to encrypt the newly issued session key.
        // The ClientSM saves session key only for clear text sessions.
        const string& sessionKey = csm->GetSessionKey();
        DelegationToken::Subject* kSubjectPtr   = 0;
        kfsKeyId_t                kSessionKeyId = 0;
        DelegationToken::WriteTokenAndSessionKey(
            os.Get(),
            token.GetUid(),
            tokenSeq,
            keyId,
            now,
            (token.GetFlags() & DelegationToken::kChunkServerFlag),
            validForSec,
            key.GetPtr(),
            key.GetSize(),
            kSubjectPtr,
            kSessionKeyId,
            sessionKey.data(),
            sessionKey.size()
        );
        os << "\r\n";
    }
}

ClientSM*
KfsOp::GetClientSM()
{
    return (clientSMFlag ? static_cast<ClientSM*>(clnt) : 0);
}

bool
WriteSyncOp::Validate()
{
    if (checksumsCnt <= 0) {
        checksumsVal.clear();
        return ChunkAccessRequestOp::Validate();
    }
    const char*       ptr = checksumsVal.mPtr;
    const char* const end = ptr + checksumsVal.mLen;
    checksums.clear();
    checksums.reserve(checksumsCnt);
    for (int i = 0; i < checksumsCnt; i++) {
        uint32_t cksum = 0;
        if (! (initialShortRpcFormatFlag ?
                ValueParserT<HexIntParser>::ParseInt(ptr, end - ptr, cksum) :
                ValueParserT<DecIntParser>::ParseInt(ptr, end - ptr, cksum))) {
            return false;
        }
        checksums.push_back(cksum);
        while (ptr < end && (*ptr & 0xFF) > ' ') {
            ++ptr;
        }
    }
    checksumsVal.clear();
    return ChunkAccessRequestOp::Validate();
}

bool MakeChunkStableOp::Validate()
{
    hasChecksum = 0 < checksumVal.mLen;
    if (hasChecksum) {
        if (initialShortRpcFormatFlag) {
            ValueParserT<HexIntParser>::SetValue(
                checksumVal.mPtr,
                checksumVal.mLen,
                uint32_t(0),
                chunkChecksum
            );
        } else {
            ValueParserT<DecIntParser>::SetValue(
                checksumVal.mPtr,
                checksumVal.mLen,
                uint32_t(0),
                chunkChecksum
            );
        }
        checksumVal.clear();
    }
    return KfsOp::Validate();
}

///
/// Generic event handler for tracking completion of an event
/// execution.  Push the op to the logger and the net thread will pick
/// it up and dispatch it.
///
int
KfsOp::HandleDone(int code, void* data)
{
    Submit();
    return 0;
}

///
/// A read op finished.  Set the status and the # of bytes read
/// alongwith the data and notify the client.
///
int
ReadOp::HandleDone(int code, void* data)
{
    if (code == EVENT_DISK_ERROR) {
        UpdateStatus(code, data);
        KFS_LOG_STREAM_ERROR <<
            "disk error:"
            " status: "   << status <<
            " chunk: "    << chunkId <<
            " version: "  << chunkVersion <<
        KFS_LOG_EOM;
        if (status != -ETIMEDOUT) {
            gChunkManager.ChunkIOFailed(
                chunkId, chunkVersion, status, diskIo.get());
        }
    } else if (code == EVENT_DISK_READ) {
        if (data) {
            IOBuffer* const b = reinterpret_cast<IOBuffer*>(data);
            // Order matters...when we append b, we take the data from b
            // and put it into our buffer.
            dataBuf.Move(b);
            // verify checksum
            if (! gChunkManager.ReadChunkDone(this)) {
                return 0; // Retry.
            }
            numBytesIO = dataBuf.BytesConsumable();
            if (status == 0) {
                // checksum verified
                status = numBytesIO;
            }
        } else {
            die("read: invalid write event data");
            status = -EFAULT;
        }
    }

    if (status >= 0) {
        assert(
            numBytesIO >= 0 && (numBytesIO == 0 ||
            (size_t)((offset + numBytesIO - 1) / CHECKSUM_BLOCKSIZE + 1 -
                offset / CHECKSUM_BLOCKSIZE) == checksum.size())
        );
        if (numBytesIO <= 0) {
            checksum.clear();
        } else if (! skipVerifyDiskChecksumFlag) {
            if (offset % CHECKSUM_BLOCKSIZE != 0) {
                checksum = ComputeChecksums(&dataBuf, numBytesIO);
            } else {
                const int len = (int)(numBytesIO % CHECKSUM_BLOCKSIZE);
                if (len > 0) {
                    checksum.back() = ComputeBlockChecksumAt(
                        &dataBuf, numBytesIO - len, (size_t)len);
                }
            }
            assert((size_t)((numBytesIO + CHECKSUM_BLOCKSIZE - 1) /
                CHECKSUM_BLOCKSIZE) == checksum.size());
        }
    }

    if (wop) {
        // if the read was triggered by a write, then resume execution of write
        wop->Execute();
        return 0;
    }

    const ChunkInfo_t* ci = gChunkManager.GetChunkInfo(chunkId, chunkVersion);
    if (ci && ci->chunkSize > 0 && offset + numBytesIO >= ci->chunkSize &&
            ! gLeaseClerk.IsLeaseValid(chunkId, chunkVersion)) {
        // If we have read the full chunk, close out the fd.  The
        // observation is that reads are sequential and when we
        // finished a chunk, the client will move to the next one.
        //
        // Release disk io first for CloseChunk to have effect: normally
        // this method is invoked from io completion routine, and diskIo has a
        // reference to file dataFH.
        // DiskIo completion path doesn't expect diskIo pointer to remain valid
        // upon return.
        diskIo.reset();
        KFS_LOG_STREAM_INFO << "closing"
            " chunk: "   << chunkId <<
            " version: " << chunkVersion <<
        KFS_LOG_EOM;
        gChunkManager.CloseChunk(chunkId, ci->chunkVersion);
    }

    Submit();
    return 0;
}

void
ReadOp::VerifyReply()
{
    if (0 <= status && (ssize_t)numBytes < numBytesIO) {
        status    = -EINVAL;
        statusMsg = "invalid read: return size exceeds requited";
    }
    if (status >= 0) {
        assert(numBytesIO == dataBuf.BytesConsumable());
        vector<uint32_t> datacksums = ComputeChecksums(
            &dataBuf, numBytesIO);
        if (datacksums.size() > checksum.size()) {
            KFS_LOG_STREAM_INFO <<
                "checksum number of entries mismatch in re-replication: "
                " expect: " << datacksums.size() <<
                " got: " << checksum.size() <<
            KFS_LOG_EOM;
            status = -EBADCKSUM;
        } else {
            const size_t sz = datacksums.size();
            for (size_t i = 0; i < sz; i++) {
                if (datacksums[i] != checksum[i]) {
                    KFS_LOG_STREAM_INFO <<
                        "Checksum mismatch in re-replication: "
                        " expect: " << datacksums[i] <<
                        " got: " << checksum[i] <<
                    KFS_LOG_EOM;
                    status = -EBADCKSUM;
                    break;
                }
            }
            if (sz != checksum.size()) {
                checksum.swap(datacksums);
            }
        }
    }
}

int
ReadOp::HandleReplicatorDone(int code, void* data)
{
    // notify the replicator object that the read it had submitted to
    // the peer has finished.
    return clnt->HandleEvent(code, data);
}

int
WriteOp::HandleRecordAppendDone(int code, void* data)
{
    UpdateStatus(code, data);
    gChunkManager.WriteDone(this);
    if (EVENT_DISK_ERROR == code) {
        // eat up everything that was sent
        dataBuf.Consume(numBytes);
        KFS_LOG_STREAM_ERROR <<
            "disk error:"
            " status: "   << status <<
            " chunk: "    << chunkId <<
        KFS_LOG_EOM;
    } else if (EVENT_DISK_WROTE == code && data) {
        status = *reinterpret_cast<const int*>(data);
        numBytesIO = status;
        dataBuf.Consume(numBytesIO);
    } else {
        die("write: unexpected event code or data");
        status = -EFAULT;
    }
    return Submit();
}

int
ReadOp::HandleScrubReadDone(int code, void* data)
{
    return scrubOp->HandleScrubReadDone(code, data);
}

bool
ReadOp::IsChunkReadOp(int64_t& outNumBytes, kfsChunkId_t& outChunkId)
{
    outChunkId = chunkId;
    if (numBytes > 0) {
        outNumBytes = (int64_t)((numBytes + CHECKSUM_BLOCKSIZE - 1) /
            CHECKSUM_BLOCKSIZE * CHECKSUM_BLOCKSIZE);
    } else {
        outNumBytes = numBytes;
    }
    return true;
}

int
WriteOp::HandleWriteDone(int code, void* data)
{
    UpdateStatus(code, data);
    gChunkManager.WriteDone(this);
    if (isFromReReplication) {
        if (code == EVENT_DISK_WROTE) {
            if (data) {
                status = min(
                    *reinterpret_cast<const int*>(data), int(numBytes));
                numBytesIO = status;
            } else {
                die("write: invalid write event data");
                status = -EFAULT;
            }
        }
        return clnt->HandleEvent(code, this);
    }
    if (! wpop) {
        die("write: invaid null write prepare op");
        status = -EFAULT;
        return 0;
    }
    if (EVENT_DISK_ERROR == code) {
        // eat up everything that was sent
        dataBuf.Consume(max(int(numBytesIO), int(numBytes)));
        KFS_LOG_STREAM_ERROR <<
            "disk error:"
            " status: "  << status <<
            " chunk: "   << chunkId <<
            " version: " << chunkVersion <<
        KFS_LOG_EOM;
        gChunkManager.ChunkIOFailed(
            chunkId, chunkVersion, status, diskIo.get());
        if (wpop->status >= 0) {
            wpop->status = status;
        }
        wpop->HandleEvent(EVENT_CMD_DONE, this);
        return 0;
    }
    if (EVENT_DISK_WROTE == code) {
        if (data) {
            status = *reinterpret_cast<const int*>(data);
        } else {
            die("write: invalid write event data");
            status = -EFAULT;
        }
        if (numBytesIO != status || status < (int)numBytes) {
            // write didn't do everything that was asked; we need to retry
            KFS_LOG_STREAM_INFO <<
                "write on chunk did less: asked: " <<
                numBytes << "/" << numBytesIO <<
                " did: " << status << "; asking clnt to retry" <<
            KFS_LOG_EOM;
            status = -EAGAIN;
        } else {
            status = numBytes; // reply back the same # of bytes as in request.
        }
        if (numBytesIO > ssize_t(numBytes)) {
            const int off(offset % IOBufferData::GetDefaultBufferSize());
            KFS_LOG_STREAM_DEBUG <<
                "chunk write: asked " << numBytes << "/" << numBytesIO <<
                " actual, buf offset: " << off <<
            KFS_LOG_EOM;
            // restore original data in the buffer.
            assert(ssize_t(numBytes) <= numBytesIO - off);
            dataBuf.Consume(off);
            dataBuf.Trim(int(numBytes));
        }
        numBytesIO = numBytes;
        // eat up everything that was sent
        dataBuf.Consume(numBytes);
        wpop->HandleEvent(EVENT_CMD_DONE, this);
    }
    return 0;
}

void
CloseOp::Execute()
{
    KFS_LOG_STREAM_INFO <<
        "closing"
        " chunk: "   << chunkId <<
        " version: " << chunkVersion <<
        " and might give up lease" <<
    KFS_LOG_EOM;

    ServerLocation peerLoc;
    int            myPos         = -1;
    int64_t        writeId       = -1;
    bool           needToForward = needToForwardToPeer(shortRpcFormatFlag,
        servers, numServers, myPos, peerLoc, hasWriteId, writeId);
    if (chunkVersion < 0 && needToForward && hasWriteId) {
        status    = -EINVAL;
        statusMsg = "invalid object store file block close";
    } else if (chunkAccessTokenValidFlag &&
            (chunkAccessFlags & ChunkAccessToken::kUsesWriteIdFlag) != 0 &&
            (subjectId != writeId || ! hasWriteId)) {
        status    = -EPERM;
        statusMsg = "access token invalid subject";
    } else {
        bool allowCSClearTextFlag = chunkAccessTokenValidFlag &&
            (chunkAccessFlags & ChunkAccessToken::kAllowClearTextFlag) != 0;
        if (chunkAccessTokenValidFlag) {
            if (myPos == 0 || numServers <= 0) {
                const bool hasValidLeaseFlag = gLeaseClerk.IsLeaseValid(
                    chunkId, chunkVersion,
                    &syncReplicationAccess, &allowCSClearTextFlag);
                if (hasValidLeaseFlag) {
                    if ((chunkAccessFlags &
                            ChunkAccessToken::kAllowWriteFlag) == 0) {
                        status    = -EPERM;
                        statusMsg = "valid write lease exists";
                    }
                } else if ((hasWriteId || (chunkAccessFlags &
                        ChunkAccessToken::kAllowReadFlag) == 0) &&
                        0 <= chunkVersion) {
                    status    = -EPERM;
                    statusMsg = "no valid write lease exists";
                }
            } else if ((chunkAccessFlags &
                    DelegationToken::kChunkServerFlag) == 0) {
                status    = -EPERM;
                statusMsg = "no chunk server access flag set";
            }
        }
        if (status < 0) {
            needToForward = false;
        } else if (! gAtomicRecordAppendManager.CloseChunk(
                this, writeId, needToForward)) {
            // forward the close only if it was accepted by the chunk
            // manager. The chunk manager can reject a close if the
            // chunk is being written to by multiple record appenders
            if (hasWriteId) {
                const bool waitReadableFlag = clnt && chunkVersion < 0;
                if (waitReadableFlag) {
                    SET_HANDLER(this, &CloseOp::HandleDone);
                }
                const int ret = gChunkManager.CloseChunkWrite(
                        chunkId,
                        chunkVersion,
                        writeId,
                        waitReadableFlag ? this          : 0,
                        waitReadableFlag ? &readMetaFlag : 0
                );
                if (ret < 0) {
                    status    = ret;
                    statusMsg = "invalid write or chunk id";
                }
                if (waitReadableFlag && 0 <= ret) {
                    return;
                }
            } else {
                needToForward = gChunkManager.CloseChunk(
                    chunkId, chunkVersion) == 0 && needToForward;
                status        = 0;
            }
        }
        if (needToForward) {
            ForwardToPeer(peerLoc, myPos == 0, allowCSClearTextFlag);
        }
    }
    Submit();
}

void
CloseOp::ForwardToPeer(
    const ServerLocation& loc,
    bool                  writeMasterFlag,
    bool                  allowCSClearTextFlag)
{
    RemoteSyncSMPtr const peer = FindPeer(
        *this, loc, writeMasterFlag, allowCSClearTextFlag);
    if (! peer) {
        KFS_LOG_STREAM_DEBUG <<
            "unable to forward to peer: " << loc <<
            " cmd: " << Show() <<
        KFS_LOG_EOM;
        return;
    }
    CloseOp* const fwdedOp = new CloseOp(*this);
    // don't need an ack back
    fwdedOp->needAck = false;
    // this op goes to the remote-sync SM and after it is sent, comes right back
    // to be deleted.
    fwdedOp->clnt    = fwdedOp;
    SET_HANDLER(fwdedOp, &CloseOp::HandlePeerReply);
    peer->Enqueue(fwdedOp);
}

int
CloseOp::HandlePeerReply(int code, void* data)
{
    delete this;
    return 0;
}

int
CloseOp::HandleDone(int code, void* data)
{
    UpdateStatus(code, data);
    if (0 <= status && readMetaFlag && chunkVersion < 0) {
        readMetaFlag = false;
        const int ret = gChunkManager.CloseChunkWrite(
            chunkId, chunkVersion, -1, this, &readMetaFlag);
        if (0 <= ret) {
            return 0;
        }
        if (0 <= status) {
            status = ret;
        }
        if (statusMsg.empty()) {
            statusMsg = "invalid write or chunk id";
        }
    }
    Submit();
    return 0;
}

void
AllocChunkOp::Execute()
{
    int            myPos   = -1;
    int64_t        writeId = -1;
    ServerLocation peerLoc;
    needToForwardToPeer(shortRpcFormatFlag,
        servers, numServers, myPos, peerLoc, false, writeId);
    if (myPos < 0) {
        statusMsg = "invalid or missing Servers: field";
        status    = -EINVAL;
        Submit();
        return;
    }
    if (chunkVersion < 0 &&
            (status = gChunkManager.GetObjectStoreStatus(&statusMsg)) != 0) {
        Submit();
        return;
    }
    // Allocation implicitly invalidates all previously existed write leases.
    gLeaseClerk.UnRegisterLease(chunkId, chunkVersion);
    mustExistFlag = mustExistFlag || 1 < chunkVersion;
    const bool deleteIfExistsFlag = ! mustExistFlag && 0 <= chunkVersion;
    if (deleteIfExistsFlag) {
        const bool kStaleChunkIdFlag = false;
        const int  ret               = gChunkManager.DeleteChunk(
            chunkId, chunkVersion, kStaleChunkIdFlag, 0);
        if (ret != -EBADF) {
            KFS_LOG_STREAM_WARN <<
                "allocate: delete existing"
                " chunk: "  << chunkId <<
                " status: " << ret <<
            KFS_LOG_EOM;
        }
    }
    // Check if chunk exists, if it does then load chunk meta data.
    SET_HANDLER(this, &AllocChunkOp::HandleChunkMetaReadDone);
    const bool addObjectBlockMappingFlag = mustExistFlag;
    int res = gChunkManager.ReadChunkMetadata(chunkId, chunkVersion, this,
        addObjectBlockMappingFlag);
    if (0 <= res) {
        // The completion handler will be or already invoked: "this" might not
        // be valid at this point, as it might have been deleted already:
        // do not attempt to access.
        if (deleteIfExistsFlag) {
            die("chunk deletion failed");
        }
        return; // The completion handler will be or have already been invoked.
    }
    if (! mustExistFlag && -EBADF == res) {
        // Allocate new chunk.
        statusMsg.clear();
        HandleChunkAllocDone(EVENT_CMD_DONE, this);
        return;
    }
    KFS_LOG_STREAM_ERROR <<
        "allocate: read chunk metadata:"
        " chunk: "   << chunkId <<
        " version: " << chunkVersion <<
        " error: "   << res <<
    KFS_LOG_EOM;
    status = res;
    Submit();
}

int
AllocChunkOp::HandleChunkMetaReadDone(int code, void* data)
{
    UpdateStatus(code, data);
    if (status < 0) {
        Submit();
        return 0;
    }
    if (! gMetaServerSM.IsInProgress(*this)) {
        status = -EAGAIN;
        Submit();
        return 0;
    }
    SET_HANDLER(this, &AllocChunkOp::HandleChunkAllocDone);
    // When version change is done the chunk must exist.
    // This is needed to detect chunk deletion while version version change is
    // in progress.
    // AllocChunk() does chunk version verification and other necessary checks
    // in the case if chunk exists.
    mustExistFlag = true;
    const bool stableFlag = false;
    Submit(gChunkManager.ChangeChunkVers(
        chunkId, chunkVersion, stableFlag, this));
    return 0;
}

int
AllocChunkOp::HandleChunkAllocDone(int code, void* data)
{
    UpdateStatus(code, data);
    if (0 <= status) {
        if (0 <= leaseId) {
            OpCounters::WriteMaster();
        }
        if (! diskIo) {
            SET_HANDLER(this, &AllocChunkOp::HandleChunkAllocDone);
            if (appendFlag) {
                int            myPos   = -1;
                int64_t        writeId = -1;
                ServerLocation peerLoc;
                needToForwardToPeer(shortRpcFormatFlag,
                    servers, numServers, myPos, peerLoc, false, writeId);
                assert(myPos >= 0);
                gChunkManager.AllocChunkForAppend(this, myPos, peerLoc);
            } else {
                bool kBeingReplicatedFlag = false;
                status = gChunkManager.AllocChunk(
                    fileId,
                    chunkId,
                    chunkVersion,
                    minStorageTier,
                    maxStorageTier,
                    kBeingReplicatedFlag,
                    0,
                    mustExistFlag,
                    this
                );
            }
            if (diskIo) {
                // File create is in progress. This method will be called again
                // when create / open completes.
                assert(status == 0);
                return 0;
            }
        }
        if (0 <= status && 0 <= leaseId) {
            gLeaseClerk.RegisterLease(*this);
        }
    }
    diskIo.reset();
    Submit();
    return 0;
}

void
DeleteChunkOp::Execute()
{
    if (chunkVersion < 0 &&
            (status = gChunkManager.GetObjectStoreStatus(&statusMsg)) != 0) {
        Submit();
        return;
    }
    SET_HANDLER(this, &DeleteChunkOp::Done);
    Submit(gChunkManager.DeleteChunk(
        chunkId, chunkVersion, staleChunkIdFlag, this));
}

int
DeleteChunkOp::Done(int code, void* data)
{
    UpdateStatus(code, data);
    Submit();
    return 0;
}

void
TruncateChunkOp::Execute()
{
    SET_HANDLER(this, &TruncateChunkOp::HandleChunkMetaReadDone);
    const bool kAddObjectBlockMappingFlag = false;
    Submit(gChunkManager.ReadChunkMetadata(chunkId, 0, this,
            kAddObjectBlockMappingFlag));
}

int
TruncateChunkOp::HandleChunkMetaReadDone(int code, void* data)
{
    UpdateStatus(code, data);
    if (status < 0) {
        Submit();
        return 0;
    }
    status = gChunkManager.TruncateChunk(chunkId, chunkSize);
    if (status < 0) {
        Submit();
        return 0;
    }
    SET_HANDLER(this, &TruncateChunkOp::HandleChunkMetaWriteDone);
    Submit(gChunkManager.WriteChunkMetadata(chunkId, 0, this));
    return 0;
}

int
TruncateChunkOp::HandleChunkMetaWriteDone(int code, void* data)
{
    UpdateStatus(code, data);
    Submit();
    return 0;
}

void
ReplicateChunkOp::Execute()
{
    Replicator::Run(this);
}

void
BeginMakeChunkStableOp::Execute()
{
    status = 0;
    if (gAtomicRecordAppendManager.BeginMakeChunkStable(this)) {
        return;
    }
    Submit();
}

void
MakeChunkStableOp::Execute()
{
    status = 0;
    if (gChunkManager.IsChunkStable(this)) {
        Submit();
        return;
    }
    SET_HANDLER(this, &MakeChunkStableOp::HandleChunkMetaReadDone);
    const bool kAddObjectBlockMappingFlag = false;
    Submit(gChunkManager.ReadChunkMetadata(
        chunkId, chunkVersion, this, kAddObjectBlockMappingFlag));
}

int
MakeChunkStableOp::HandleChunkMetaReadDone(int code, void* data)
{
    UpdateStatus(code, data);
    if (0 <= status && ! gMetaServerSM.IsInProgress(*this)) {
        status = -EAGAIN;
    }
    if (status < 0) {
        Submit();
        return 0;
    }
    SET_HANDLER(this, &MakeChunkStableOp::HandleMakeStableDone);
    if (gAtomicRecordAppendManager.MakeChunkStable(this)) {
        return 0;
    }
    HandleMakeStableDone(EVENT_CMD_DONE, this);
    return 0;
}

int
MakeChunkStableOp::HandleMakeStableDone(int code, void* data)
{
    UpdateStatus(code, data);
    if (0 <= status && 0 <= chunkSize && chunkVersion < 0) {
        const ChunkInfo_t* const ci =
            gChunkManager.GetChunkInfo(chunkId, chunkVersion);
        // Verify that the object store block size matches.
        if (! ci || ! ci->AreChecksumsLoaded()) {
            statusMsg = "checksums not unloaded";
            status    = -EAGAIN;
        } else if (ci->chunkSize != chunkSize) {
            statusMsg = "object block size do not match";
            status    = -EINVAL;
        }
    }
    // If size is undefined, do not close object block yet, as meta server might
    // query size in order to determine logical EOF.
    if (0 <= status &&
            (0 <= chunkVersion || 0 <= chunkSize) &&
            ! gLeaseClerk.IsLeaseValid(chunkId, chunkVersion) &&
            gChunkManager.CloseChunkIfReadable(chunkId, chunkVersion)) {
        KFS_LOG_STREAM_DEBUG <<
            Show() << " done, chunk closed" <<
        KFS_LOG_EOM;
    }
    Submit();
    return 0;
}

void
ChangeChunkVersOp::Execute()
{
    if (verifyStableFlag && 0 <= chunkVersion) {
        // Bypass meta data load, if only chunk version check is required.
        const ChunkInfo_t* const ci =
            gChunkManager.GetChunkInfo(chunkId, chunkVersion);
        if (! ci || gChunkManager.IsChunkReadable(chunkId, chunkVersion)) {
            if (! ci) {
                statusMsg = "no such chunk";
                status    = -ENOENT;
            } else if ((fromChunkVersion < chunkVersion ?
                    (ci->chunkVersion < fromChunkVersion ||
                        chunkVersion < ci->chunkVersion) :
                    (ci->chunkVersion < chunkVersion ||
                        fromChunkVersion < ci->chunkVersion))) {
                statusMsg = "version out of range";
                status    = -EINVAL;
            } else {
                status = 0;
            }
            if (0 != status || chunkVersion == ci->chunkVersion) {
                Submit();
                return;
            }
        }
    }
    SET_HANDLER(this, &ChangeChunkVersOp::HandleChunkMetaReadDone);
    const bool kAddObjectBlockMappingFlag = false;
    Submit(gChunkManager.ReadChunkMetadata(
        chunkId, chunkVersion, this, kAddObjectBlockMappingFlag));
}

int
ChangeChunkVersOp::HandleChunkMetaReadDone(int code, void* data)
{
    UpdateStatus(code, data);
    if (0 <= status && ! gMetaServerSM.IsInProgress(*this)) {
        status = -EAGAIN;
    }
    if (status < 0) {
        Submit();
        return 0;
    }
    SET_HANDLER(this, &ChangeChunkVersOp::HandleChunkMetaWriteDone);
    Submit(gChunkManager.ChangeChunkVers(this));
    return 0;
}

int
ChangeChunkVersOp::HandleChunkMetaWriteDone(int code, void* data)
{
    UpdateStatus(code, data);
    Submit();
    return 0;
}


static inline ostream&
HBAppendKey(ostream& os, const char* keyPrefix, int idx, const char* key)
{
    if (keyPrefix) {
        os << keyPrefix;
    }
    os << key;
    if (0 <= idx) {
        os << idx;
    }
    return os;
}

template<typename T>
inline static void
HBAppend(ostream** os, const char* key, const T& val,
    const char* keyPrefix = 0, int idx = -1)
{
    if (os[0]) {
        HBAppendKey(*os[0], keyPrefix, idx, key) << ": " << val << "\r\n";
    }
    if (os[1]) {
        HBAppendKey(*os[1]  << ",", keyPrefix, idx, key) << "=" << val;
    }
}

inline static void
HBAppend(ostream** os, int idx, const Watchdog::Counters& counters)
{
    HBAppend(os, counters.mName, counters.mTimeoutCount,
        "WD-timeouts-", idx);
    HBAppend(os, counters.mName, counters.mPollCount,
        "WD-polls-",    idx);
    HBAppend(os, counters.mName, counters.mTotalTimeoutCount,
        "WD-tot-timeouts-", idx);
    HBAppend(os, counters.mName, counters.mLastChangedTimeAgoUsec,
        "WD-changed-usec-ago-", idx);
}

template<typename T>
inline static void
AppendStorageTiersInfo(const char* prefix, T& os,
    const ChunkManager::StorageTiersInfo& tiersInfo, const char* suffix = "\r\n")
{
    os << prefix;
    for (ChunkManager::StorageTiersInfo::const_iterator it = tiersInfo.begin();
            it != tiersInfo.end();
            ++it) {
        os <<
            " " << (unsigned int)it->first <<
            " " << it->second.mDeviceCount <<
            " " << it->second.mNotStableOpenCount <<
            " " << it->second.mChunkCount <<
            " " << it->second.mSpaceAvailable <<
            " " << it->second.mTotalSpace
        ;
    }
    os << suffix;
}

static void
HBAppendCounters(ostream* hbos)
{
    ostream* os[2];
    os[0] = hbos;
    MsgLogger::LogLevel const logLevel =
        gChunkManager.GetHeartbeatCtrsLogLevel();
    if (MsgLogger::kLogLevelUndef != logLevel &&
            MsgLogger::GetLogger() &&
            MsgLogger::GetLogger()->IsLogLevelEnabled(logLevel)) {
        os[1] =
            &MsgLogger::GetLogger()->GetStream(logLevel, 0);
        *os[1] << KFS_LOG_STREAM_SRC_PREFIX <<
            "===counters: "
            ";time-usec="              << microseconds() <<
            ",location="               << gChunkServer.GetLocation() <<
            ",primary="                << gMetaServerSM.CetPrimaryLocation() <<
            ",meta-connection-uptime=" <<
                (gMetaServerSM.IsUp() ?
                    gMetaServerSM.ConnectionUptime() : time_t(-1));
    } else {
        if (! hbos) {
            return;
        }
        os[1] = 0;
    }

    double loadavg[3] = {-1, -1, -1};
#ifndef KFS_OS_NAME_CYGWIN
    getloadavg(loadavg, 3);
#endif
    const int64_t writeCount       = gChunkManager.GetNumWritableChunks();
    const int64_t writeAppendCount =
        gAtomicRecordAppendManager.GetOpenAppendersCount();
    const int64_t replicationCount = Replicator::GetNumReplications();
    int64_t utime, stime;

    if (cputime(&utime, &stime) < 0) {
        utime = stime = -1;
    }
    int64_t totalFsSpace           = 0;
    int     chunkDirs              = 0;
    int     evacuateInFlightCount  = 0;
    int     writableDirs           = 0;
    int     evacuateChunks         = 0;
    int64_t evacuateByteCount      = 0;
    int     evacuateDoneChunkCount = 0;
    int64_t evacuateDoneByteCount  = 0;
    int64_t devWaitAvgUsec         = 0;
    ChunkManager::StorageTiersInfo tiersInfo;

    HBAppend(os, "Total-space", gChunkManager.GetTotalSpace(
        totalFsSpace, chunkDirs, evacuateInFlightCount, writableDirs,
        evacuateChunks, evacuateByteCount,
        &evacuateDoneChunkCount, &evacuateDoneByteCount, 0, &tiersInfo,
        &devWaitAvgUsec));
    HBAppend(os, "Total-fs-space",totalFsSpace);
    HBAppend(os, "Used-space",    gChunkManager.GetUsedSpace());
    HBAppend(os, "Num-drives",    chunkDirs);
    HBAppend(os, "Num-wr-drives", writableDirs);
    HBAppend(os, "Num-chunks",    gChunkManager.GetNumChunks());
    HBAppend(os, "Num-writable-chunks",
        writeCount + writeAppendCount + replicationCount);
    HBAppend(os, "Num-wr-objs", gChunkManager.GetNumWritableObjects());
    HBAppend(os, "Num-objs",    gChunkManager.GetNumOpenObjects());
    HBAppend(os, "Evacuate",
        max(evacuateChunks, evacuateInFlightCount));
    HBAppend(os, "Evacuate-bytes",      evacuateByteCount);
    HBAppend(os, "Evacuate-done",       evacuateDoneChunkCount);
    HBAppend(os, "Evacuate-done-bytes", evacuateDoneByteCount);
    HBAppend(os, "Evacuate-in-flight",  evacuateInFlightCount);
    if (os[0]) {
        AppendStorageTiersInfo("Storage-tiers:", *os[0], tiersInfo);
    }
    if (os[1]) {
        AppendStorageTiersInfo(",Storage-tiers=", *os[1], tiersInfo, "");
    }
    HBAppend(os, "Num-random-writes",   writeCount);
    HBAppend(os, "Num-appends",         writeAppendCount);
    HBAppend(os, "Num-re-replications", replicationCount);
    HBAppend(os, "Num-appends-with-wids",
        gAtomicRecordAppendManager.GetAppendersWithWidCount());
    HBAppend(os, "Uptime",              globalNetManager().UpTime());

    HBAppend(os, "CPU-user",     utime);
    HBAppend(os, "CPU-sys",      stime);
    HBAppend(os, "CPU-load-avg", loadavg[0]);

    ChunkManager::Counters cm;
    gChunkManager.GetCounters(cm);
    HBAppend(os, "Chunk-corrupted",           cm.mCorruptedChunksCount);
    HBAppend(os, "Chunk-lost",                cm.mLostChunksCount);
    HBAppend(os, "Chunk-header-errors",       cm.mBadChunkHeaderErrorCount);
    HBAppend(os, "Chunk-chksum-errors",       cm.mReadChecksumErrorCount);
    HBAppend(os, "Chunk-read-errors",         cm.mReadErrorCount);
    HBAppend(os, "Chunk-write-errors",        cm.mWriteErrorCount);
    HBAppend(os, "Chunk-open-errors",         cm.mOpenErrorCount);
    HBAppend(os, "Dir-chunk-lost",            cm.mDirLostChunkCount);
    HBAppend(os, "Chunk-dir-lost",            cm.mChunkDirLostCount);
    HBAppend(os, "Read-chksum",               cm.mReadChecksumCount);
    HBAppend(os, "Read-chksum-bytes",         cm.mReadChecksumByteCount);
    HBAppend(os, "Read-chksum-skip",          cm.mReadSkipDiskVerifyCount);
    HBAppend(os, "Read-chksum-skip-err",      cm.mReadSkipDiskVerifyErrorCount);
    HBAppend(os, "Read-chksum-skip-bytes",    cm.mReadSkipDiskVerifyByteCount);
    HBAppend(os, "Read-chksum-skip-cs-bytes",
        cm.mReadSkipDiskVerifyChecksumByteCount);

    MetaServerSM::Counters mc;
    gMetaServerSM.GetCounters(mc);
    HBAppend(os, "Meta-connect",      mc.mConnectCount);
    HBAppend(os, "Meta-hello-count",  mc.mHelloCount);
    HBAppend(os, "Meta-hello-errors", mc.mHelloErrorCount);
    HBAppend(os, "Meta-alloc-count",  mc.mAllocCount);
    HBAppend(os, "Meta-alloc-errors", mc.mAllocErrorCount);

    ClientManager::Counters cli;
    gClientManager.GetCounters(cli);
    HBAppend(os, "Client-accept",             cli.mAcceptCount);
    HBAppend(os, "Client-active",             cli.mClientCount);
    HBAppend(os, "Client-req-invalid",        cli.mBadRequestCount);
    HBAppend(os, "Client-req-invalid-header", cli.mBadRequestHeaderCount);
    HBAppend(os, "Client-req-invalid-length", cli.mRequestLengthExceededCount);
    HBAppend(os, "Client-discarded-bytes",    cli.mDiscardedBytesCount);
    HBAppend(os, "Client-wait-exceed",        cli.mWaitTimeExceededCount);
    HBAppend(os, "Client-read-count",         cli.mReadRequestCount);
    HBAppend(os, "Client-read-bytes",         cli.mReadRequestBytes);
    HBAppend(os, "Client-read-micro-sec",     cli.mReadRequestTimeMicroSecs);
    HBAppend(os, "Client-read-errors",        cli.mReadRequestErrors);
    HBAppend(os, "Client-write-count",        cli.mWriteRequestCount);
    HBAppend(os, "Client-write-bytes",        cli.mWriteRequestBytes);
    HBAppend(os, "Client-write-micro-sec",    cli.mWriteRequestTimeMicroSecs);
    HBAppend(os, "Client-write-errors",       cli.mWriteRequestErrors);
    HBAppend(os, "Client-append-count",       cli.mAppendRequestCount);
    HBAppend(os, "Client-append-bytes"    ,   cli.mAppendRequestBytes);
    HBAppend(os, "Client-append-micro-sec",   cli.mAppendRequestTimeMicroSecs);
    HBAppend(os, "Client-append-errors",      cli.mAppendRequestErrors);
    HBAppend(os, "Client-other-count",        cli.mOtherRequestCount);
    HBAppend(os, "Client-other-micro-sec",    cli.mOtherRequestTimeMicroSecs);
    HBAppend(os, "Client-other-errors",       cli.mOtherRequestErrors);
    HBAppend(os, "Client-over-limit",         cli.mOverClientLimitCount);
    HBAppend(os, "Client-max-count",
        gClientManager.GetMaxClientCount());

    HBAppend(os, "Timer-overrun-count",
        globalNetManager().GetTimerOverrunCount());
    HBAppend(os, "Timer-overrun-sec",
        globalNetManager().GetTimerOverrunSec());

    HBAppend(os, "Write-appenders",
        gAtomicRecordAppendManager.GetAppendersCount());
    AtomicRecordAppendManager::Counters wa;
    gAtomicRecordAppendManager.GetCounters(wa);
    HBAppend(os, "WAppend-count",                wa.mAppendCount);
    HBAppend(os, "WAppend-bytes",                wa.mAppendByteCount);
    HBAppend(os, "WAppend-errors",               wa.mAppendErrorCount);
    HBAppend(os, "WAppend-replication-errors",   wa.mReplicationErrorCount);
    HBAppend(os, "WAppend-replication-tiemouts", wa.mReplicationTimeoutCount);
    HBAppend(os, "WAppend-alloc-count",          wa.mAppenderAllocCount);
    HBAppend(os, "WAppend-alloc-master-count",   wa.mAppenderAllocMasterCount);
    HBAppend(os, "WAppend-alloc-errors",         wa.mAppenderAllocErrorCount);
    HBAppend(os, "WAppend-wid-alloc-count",      wa.mWriteIdAllocCount);
    HBAppend(os, "WAppend-wid-alloc-errors",     wa.mWriteIdAllocErrorCount);
    HBAppend(os, "WAppend-wid-alloc-no-appender",
        wa.mWriteIdAllocNoAppenderCount);
    HBAppend(os, "WAppend-sreserve-count",       wa.mSpaceReserveCount);
    HBAppend(os, "WAppend-sreserve-bytes",       wa.mSpaceReserveByteCount);
    HBAppend(os, "WAppend-sreserve-errors",      wa.mSpaceReserveErrorCount);
    HBAppend(os, "WAppend-sreserve-denied",      wa.mSpaceReserveDeniedCount);
    HBAppend(os, "WAppend-bmcs-count",           wa.mBeginMakeStableCount);
    HBAppend(os, "WAppend-bmcs-errors",          wa.mBeginMakeStableErrorCount);
    HBAppend(os, "WAppend-mcs-count",            wa.mMakeStableCount);
    HBAppend(os, "WAppend-mcs-errors",           wa.mMakeStableErrorCount);
    HBAppend(os, "WAppend-mcs-length-errors",
        wa.mMakeStableLengthErrorCount);
    HBAppend(os, "WAppend-mcs-chksum-errors",
        wa.mMakeStableChecksumErrorCount);
    HBAppend(os, "WAppend-get-op-status-count",  wa.mGetOpStatusCount);
    HBAppend(os, "WAppend-get-op-status-errors", wa.mGetOpStatusErrorCount);
    HBAppend(os, "WAppend-get-op-status-known",  wa.mGetOpStatusKnownCount);
    HBAppend(os, "WAppend-chksum-erros",         wa.mChecksumErrorCount);
    HBAppend(os, "WAppend-read-erros",           wa.mReadErrorCount);
    HBAppend(os, "WAppend-write-errors",         wa.mWriteErrorCount);
    HBAppend(os, "WAppend-lease-ex-errors",      wa.mLeaseExpiredCount);
    HBAppend(os, "WAppend-lost-timeouts",        wa.mTimeoutLostCount);
    HBAppend(os, "WAppend-lost-chunks",          wa.mLostChunkCount);
    HBAppend(os, "WAppend-pending-bytes",        wa.mPendingByteCount);
    HBAppend(os, "WAppend-low-buf-flush",        wa.mLowOnBuffersFlushCount);
    HBAppend(os, "WAppend-chksum-unlock",        wa.mChecksumUnlockedCount);
    HBAppend(os, "WAppend-chksum-unlock-fail",   wa.mChecksumUnlockFailCount);

    const BufferManager& bufMgr = DiskIo::GetBufferManager();
    HBAppend(os, "Buffer-bytes-total",      bufMgr.GetTotalByteCount());
    HBAppend(os, "Buffer-bytes-wait",       bufMgr.GetWaitingByteCount());
    HBAppend(os, "Buffer-bytes-wait-avg",   bufMgr.GetWaitingAvgBytes());
    HBAppend(os, "Buffer-s-usec-wait-avg",  bufMgr.GetWaitingAvgUsecs());
    HBAppend(os, "Buffer-usec-wait-avg",
        bufMgr.GetWaitingAvgUsecs() + devWaitAvgUsec);
    HBAppend(os, "Buffer-clients-wait-avg", bufMgr.GetWaitingAvgCount());
    HBAppend(os, "Buffer-total-count",      bufMgr.GetTotalBufferCount());
    HBAppend(os, "Buffer-min-count",        bufMgr.GetMinBufferCount());
    HBAppend(os, "Buffer-free-count",       bufMgr.GetFreeBufferCount());
    HBAppend(os, "Buffer-clients",
        bufMgr.GetClientsWihtBuffersCount());
    HBAppend(os, "Buffer-clients-wait",     bufMgr.GetWaitingCount());
    HBAppend(os, "Buffer-quota-clients-wait",
        bufMgr.GetOverQuotaWaitingCount());

    BufferManager::Counters bmCnts;
    bufMgr.GetCounters(bmCnts);
    HBAppend(os, "Buffer-req-total",         bmCnts.mRequestCount);
    HBAppend(os, "Buffer-req-bytes",         bmCnts.mRequestByteCount);
    HBAppend(os, "Buffer-req-denied-total",  bmCnts.mRequestDeniedCount);
    HBAppend(os, "Buffer-req-denied-bytes",  bmCnts.mRequestDeniedByteCount);
    HBAppend(os, "Buffer-req-granted-total", bmCnts.mRequestGrantedCount);
    HBAppend(os, "Buffer-req-granted-bytes", bmCnts.mRequestGrantedByteCount);
    HBAppend(os, "Buffer-req-wait-usec",     bmCnts.mRequestWaitUsecs);
    HBAppend(os, "Buffer-req-denied-quota",
        bmCnts.mOverQuotaRequestDeniedCount);
    HBAppend(os, "Buffer-req-denied-quota-bytes",
        bmCnts.mOverQuotaRequestDeniedByteCount);

    DiskIo::Counters dio;
    DiskIo::GetCounters(dio);
    HBAppend(os, "Disk-read-count",           dio.mReadCount);
    HBAppend(os, "Disk-read-bytes",           dio.mReadByteCount);
    HBAppend(os, "Disk-read-errors",          dio.mReadErrorCount);
    HBAppend(os, "Disk-write-count",          dio.mWriteCount);
    HBAppend(os, "Disk-write-bytes",          dio.mWriteByteCount);
    HBAppend(os, "Disk-write-errors",         dio.mWriteErrorCount);
    HBAppend(os, "Disk-sync-count",           dio.mSyncCount);
    HBAppend(os, "Disk-sync-errors",          dio.mSyncErrorCount);
    HBAppend(os, "Disk-delete-count",         dio.mDeleteCount);
    HBAppend(os, "Disk-delete-errors",        dio.mDeleteErrorCount);
    HBAppend(os, "Disk-rename-count",         dio.mRenameCount);
    HBAppend(os, "Disk-rename-errors",        dio.mRenameErrorCount);
    HBAppend(os, "Disk-fs-get-free-count",    dio.mGetFsSpaceAvailableCount);
    HBAppend(os, "Disk-fs-get-free-errors",
        dio.mGetFsSpaceAvailableErrorCount);
    HBAppend(os, "Disk-dir-readable-count",   dio.mCheckDirReadableCount);
    HBAppend(os, "Disk-dir-readable-errors",  dio.mCheckDirReadableErrorCount);
    HBAppend(os, "Disk-dir-writable-count",   dio.mCheckDirWritableCount);
    HBAppend(os, "Disk-dir-writable-errors",  dio.mCheckDirWritableErrorCount);
    HBAppend(os, "Disk-timedout-count",       dio.mTimedOutErrorCount);
    HBAppend(os, "Disk-timedout-read-bytes",  dio.mTimedOutErrorReadByteCount);
    HBAppend(os, "Disk-timedout-write-bytes", dio.mTimedOutErrorWriteByteCount);
    HBAppend(os, "Disk-open-files",           dio.mOpenFilesCount);

    MsgLogger::Counters msgLogCntrs;
    MsgLogger::GetLogger()->GetCounters(msgLogCntrs);
    HBAppend(os, "Msg-log-level",
        MsgLogger::GetLogger()->GetLogLevel());
    HBAppend(os, "Msg-log-count",            msgLogCntrs.mAppendCount);
    HBAppend(os, "Msg-log-drop",             msgLogCntrs.mDroppedCount);
    HBAppend(os, "Msg-log-write-errors",     msgLogCntrs.mWriteErrorCount);
    HBAppend(os, "Msg-log-wait",             msgLogCntrs.mAppendWaitCount);
    HBAppend(os, "Msg-log-waited-micro-sec", msgLogCntrs.mAppendWaitMicroSecs);

    Replicator::Counters replCntrs;
    Replicator::GetCounters(replCntrs);
    HBAppend(os, "Replication-count",      replCntrs.mReplicationCount);
    HBAppend(os, "Replication-errors",     replCntrs.mReplicationErrorCount);
    HBAppend(os, "Replication-cancel",     replCntrs.mReplicationCanceledCount);
    HBAppend(os, "Replicator-count",       replCntrs.mReplicatorCount);
    HBAppend(os, "Recovery-count",         replCntrs.mRecoveryCount);
    HBAppend(os, "Recovery-errors",        replCntrs.mRecoveryErrorCount);
    HBAppend(os, "Recovery-cancel",        replCntrs.mRecoveryCanceledCount);
    HBAppend(os, "Replicator-reads",       replCntrs.mReadCount);
    HBAppend(os, "Replicator-read-bytes",  replCntrs.mReadByteCount);
    HBAppend(os, "Replicator-writes",      replCntrs.mWriteCount);
    HBAppend(os, "Replicator-write-bytes", replCntrs.mWriteByteCount);

    HBAppend(os, "Ops-in-flight-count", gChunkServer.GetNumOps());
    HBAppend(os, "Socket-count",        globals().ctrOpenNetFds.GetValue());
    HBAppend(os, "Disk-fd-count",       globals().ctrOpenDiskFds.GetValue());
    HBAppend(os, "Net-bytes-read",      globals().ctrNetBytesRead.GetValue());
    HBAppend(os, "Net-bytes-write",
        globals().ctrNetBytesWritten.GetValue());
    HBAppend(os, "Disk-bytes-read",     globals().ctrDiskBytesRead.GetValue());
    HBAppend(os, "Disk-bytes-write",
        globals().ctrDiskBytesWritten.GetValue());
    HBAppend(os, "Dns-resolved",
        globals().ctrNetDnsResolvedCtr.GetValue());
    HBAppend(os, "Dns-resolved-usec",
        globals().ctrNetDnsResolvedCtr.GetTimeSpent());
    HBAppend(os, "Dns-errors",          globals().ctrNetDnsErrors.GetValue());
    HBAppend(os, "Dns-errors-usec",
        globals().ctrNetDnsErrors.GetTimeSpent());
    HBAppend(os, "Total-ops-count",     KfsOp::GetOpsCount());
    HBAppend(os, "Auth-clnt",           gClientManager.IsAuthEnabled() ? 1 : 0);
    HBAppend(os, "Auth-rsync",          RemoteSyncSM::IsAuthEnabled()  ? 1 : 0);
    HBAppend(os, "Auth-meta",           gMetaServerSM.IsAuthEnabled()  ? 1 : 0);

    Watchdog& watchdog = gChunkServer.GetWatchdog();
    HBAppend(os, "WD-timeouts",          watchdog.GetTimeoutCount());
    HBAppend(os, "WD-polls",             watchdog.GetPollCount());
    HBAppend(os, "WD-tm-overruns",       watchdog.GetTimerOverrunCount());
    HBAppend(os, "WD-tm-oversuns-usecs", watchdog.GetTimerOverrunUsecCount());
    Watchdog::Counters wdCntrs;
    Watchdog::Counters wdCntrsSum;
    int                idx;
    const int          kMaxWdCountersToReport = 8;
    for (idx = 0; watchdog.GetCounters(idx, wdCntrs); ++idx) {
        if (kMaxWdCountersToReport <= idx) {
            wdCntrsSum.Add(wdCntrs);
        } else {
            HBAppend(os, idx, wdCntrs);
        }
    }
    if (kMaxWdCountersToReport < idx) {
        --idx;
        HBAppend(os, idx, kMaxWdCountersToReport == idx ? wdCntrs : wdCntrsSum);
    }
    if (os[0]) {
        *os[0] << "\r\n";
        os[0]->flush();
    }
    if (os[1]) {
        os[1]->flush();
        MsgLogger::GetLogger()->PutStream(*os[1]);
    }
}

void
LogChunkServerCounters()
{
    HBAppendCounters(0);
}

// This is the heartbeat sent by the meta server
void
HeartbeatOp::Execute()
{
    gChunkManager.MetaHeartbeat(*this);
    if (! omitCountersFlag) {
        static IOBuffer::WOStream sWOs;
        HBAppendCounters(&sWOs.Set(response));
        sWOs.Reset();
    }
    status = 0;
    Submit();
}

void
RetireOp::Execute()
{
    // we are told to retire...so, bow out
    KFS_LOG_STREAM_WARN << "we have been asked to retire, bye" << KFS_LOG_EOM;
    globalNetManager().Shutdown();
    Submit();
}

bool
StaleChunksOp::ParseContent(istream& is, const IOBuffer& buf)
{
    if (0 != status) {
        return false;
    }
    uint32_t chksum;
    if (0 <= contentChecksum && (uint32_t)contentChecksum !=
                (chksum = ComputeBlockChecksum(&buf, max(0, contentLength)))) {
        status    = -EINVAL;
        statusMsg = "content checksum mismatch";
        KFS_LOG_STREAM_ERROR <<
            statusMsg <<
            " expected: " << contentChecksum <<
            " actual: "   << chksum <<
            " "           << Show() <<
            " content: "  << IOBuffer::DisplayData(buf, contentLength) <<
        KFS_LOG_EOM;
        return false;
    }
    if (numStaleChunks <= 0) {
        if (0 < contentLength) {
            statusMsg = "invalid stale chunks count";
            status = -EINVAL;
        }
        return (0 == status);
    }
    staleChunkIds.reserve(numStaleChunks);
    const istream::fmtflags isFlags = is.flags();
    if (hexFormatFlag) {
        is >> hex;
    }
    kfsChunkId_t c = -1;
    for(int i = 0; i < numStaleChunks; ++i) {
        if (! (is >> c) || c < 0) {
            break;
        }
        staleChunkIds.push_back(c);
    }
    if (staleChunkIds.size() != (size_t)numStaleChunks) {
        statusMsg = "failed to parse stale chunks request: expected: ";
        AppendDecIntToString(statusMsg, numStaleChunks)
            .append(" got: ");
        AppendDecIntToString(statusMsg, staleChunkIds.size())
            .append(" last chunk: ");
        AppendDecIntToString(statusMsg, c);
        status = -EINVAL;
    }
    is.flags(isFlags);
    return (0 == status);
}

void
StaleChunksOp::Execute()
{
    if (0 != pendingCount) {
        die("delete stale chunks: invalid pending count in execute");
    }
    status = 0;
    // Set pending to 1 in order to protect against recursion, due to
    // immediate / "synchronous" completion, by effectively self referencing.
    pendingCount++;
    const bool forceDeleteFlag = true;
    for (StaleChunkIds::const_iterator it = staleChunkIds.begin();
            it != staleChunkIds.end();
            ++it) {
        pendingCount++;
        const int ret = gChunkManager.StaleChunk(
                *it, forceDeleteFlag, evacuatedFlag, availChunksSeq, this);
        if (ret < 0) {
            pendingCount--;
            KFS_LOG_STREAM_DEBUG <<
                "stale-chunk:"
                " seq: "     << seq <<
                " chunk: "   << *it <<
                " status: "  << ret <<
                " pending: " << pendingCount <<
            KFS_LOG_EOM;
            if (0 == status) {
                status = ret;
            }
        }
    }
    if (flushStaleQueueFlag) {
        pendingCount++;
        const int ret = gChunkManager.FlushStaleQueue(*this);
        if (ret < 0) {
            if (0 == status) {
                status = ret;
            }
            pendingCount--;
        }
    }
    pendingCount--;
    if (pendingCount <= 0) {
        Submit();
    }
}

int
StaleChunksOp::Done(int code, void* data)
{
    UpdateStatus(code, data);
    if (pendingCount <= 0) {
        die("delete stale chunks: invalid pending count in completion");
    } else {
        pendingCount--;
    }
    if (pendingCount <= 0) {
        Submit();
    }
    return 0;
}

void
ReadOp::Execute()
{
    if (numBytes > CHUNKSIZE) {
        KFS_LOG_STREAM_DEBUG <<
            "read request size exceeds chunk size: " << numBytes <<
        KFS_LOG_EOM;
        status = -EINVAL;
    } else if (clientSMFlag &&
            ! gChunkManager.IsChunkReadable(chunkId, chunkVersion)) {
        // Do not allow dirty reads.
        statusMsg = "chunk not readable";
        status    = -EAGAIN;
        KFS_LOG_STREAM_ERROR <<
            " read request for"
            " chunk: "   << chunkId <<
            " version: " << chunkVersion <<
            " denied: "  << statusMsg <<
        KFS_LOG_EOM;
    }
    if (status < 0) {
        Submit();
        return;
    }
    if (chunkVersion < 0 &&
            (status = gChunkManager.GetObjectStoreStatus(&statusMsg)) != 0) {
        Submit();
        return;
    }
    SET_HANDLER(this, &ReadOp::HandleChunkMetaReadDone);
    const bool kAddObjectBlockMappingFlag = true;
    const int res = gChunkManager.ReadChunkMetadata(
        chunkId, chunkVersion, this, kAddObjectBlockMappingFlag);
    if (res < 0) {
        KFS_LOG_STREAM_ERROR <<
            "failed read meta data:"
            " chunk: "   << chunkId <<
            " version: " << chunkVersion <<
            " status: "  << res <<
            " "          << statusMsg <<
        KFS_LOG_EOM;
        if (0 <= status) {
            status = res;
        }
        Submit();
    }
}

int
ReadOp::HandleChunkMetaReadDone(int code, void* data)
{
    UpdateStatus(code, data);
    if (0 <= status) {
        SET_HANDLER(this, &ReadOp::HandleDone);
        const int ret = gChunkManager.ReadChunk(this);
        if (0 <= ret) {
            return 0;
        }
        if (0 <= status) {
            status = ret;
        }
    }
    if (wop) {
        // resume execution of write
        wop->Execute();
    } else {
        Submit();
    }
    return 0;
}

/* virtual */ bool
ReadOp::ParseResponse(const Properties& props, IOBuffer& iobuf)
{
    const int checksumEntries = props.getValue(
        shortRpcFormatFlag ? "KC" : "Checksum-entries", 0);
    checksum.clear();
    if (0 < checksumEntries) {
        const Properties::String* const cks = props.getValue(
            shortRpcFormatFlag ? "K" : "Checksums");
        if (! cks) {
            return false;
        }
        const char*       ptr = cks->GetPtr();
        const char* const end = ptr + cks->GetSize();
        for (int i = 0; i < checksumEntries; i++) {
            if (end <= ptr) {
                return false;
            }
            uint32_t cs = 0;
            if (!(shortRpcFormatFlag ?
                    HexIntParser::Parse(ptr, end - ptr, cs) :
                    DecIntParser::Parse(ptr, end - ptr, cs))) {
                return false;
            }
            checksum.push_back(cs);
        }
    }
    skipVerifyDiskChecksumFlag = skipVerifyDiskChecksumFlag &&
        props.getValue(shortRpcFormatFlag ? "KS" : "Skip-Disk-Chksum", 0) != 0;
    const int off = (int)(offset % IOBufferData::GetDefaultBufferSize());
    if (0 < off) {
        IOBuffer buf;
        buf.ReplaceKeepBuffersFull(&iobuf, off, iobuf.BytesConsumable());
        iobuf.Move(&buf);
        iobuf.Consume(off);
    } else {
        iobuf.MakeBuffersFull();
    }
    return true;
}

//
// Handling of writes is done in multiple steps:
// 1. The client allocates a chunk from the metaserver; the metaserver
// picks a set of hosting chunkservers and nominates one of the
// server's as the "master" for the transaction.
// 2. The client pushes data for a write via a WritePrepareOp to each
// of the hosting chunkservers (in any order).
// 3. The chunkserver in turn enqueues the write with the ChunkManager
// object.  The ChunkManager assigns an id to the write.   NOTE:
// nothing is written out to disk at this point.
// 4. After the client has pushed out data to replica chunk-servers
// and gotten write-id's, the client does a WriteSync to the master.
// 5. The master retrieves the write corresponding to the write-id and
// commits the write to disk.
// 6. The master then sends out a WriteCommit to each of the replica
// chunkservers asking them to commit the write; this commit message
// is sent concurrently to all the replicas.
// 7. After the replicas reply, the master replies to the client with
// status from individual servers and how much got written on each.
//

void
WriteIdAllocOp::Execute()
{
    if (chunkAccessTokenValidFlag &&
            (chunkAccessFlags & ChunkAccessToken::kUsesWriteIdFlag) != 0) {
        status    = -EPERM;
        statusMsg = "no write id subject allowed";
        Submit();
        return;
    }
    // check if we need to forward anywhere
    writeId = -1;
    int64_t        dummyWriteId  = -1;
    int            myPos         = -1;
    ServerLocation peerLoc;
    const bool     needToForward = needToForwardToPeer(shortRpcFormatFlag,
        servers, numServers, myPos, peerLoc, false, dummyWriteId);
    if (myPos < 0) {
        statusMsg = "invalid or missing Servers: field";
        status    = -EINVAL;
        Submit();
        return;
    }
    const bool writeMaster          = myPos == 0;
    bool       allowCSClearTextFlag = chunkAccessTokenValidFlag &&
        (chunkAccessFlags & ChunkAccessToken::kAllowClearTextFlag) != 0;
    if (writeMaster && ! gLeaseClerk.IsLeaseValid(
            chunkId, chunkVersion,
            &syncReplicationAccess, &allowCSClearTextFlag)) {
        status    = -ELEASEEXPIRED;
        statusMsg = "no valid write lease exists";
        Done(EVENT_CMD_DONE, this);
        return;
    }
    const int res = gChunkManager.AllocateWriteId(this, myPos, peerLoc);
    if (res < 0 && 0 <= status) {
        status = res;
    }
    if (0 != status) {
        Done(EVENT_CMD_DONE, this);
        return;
    }
    if (writeMaster) {
        // Notify the lease clerk that we are doing write.  This is to
        // signal the lease clerk to renew the lease for the chunk when
        // appropriate.
        gLeaseClerk.DoingWrite(chunkId, chunkVersion);
    }
    const ServerLocation& loc = gChunkServer.GetLocation();
    writeIdStr.Copy(loc.hostname.data(), loc.hostname.size()).Append((char)' ');
    if (initialShortRpcFormatFlag) {
        AppendHexIntToString(writeIdStr, loc.port).Append((char)' ');
        AppendHexIntToString(writeIdStr, writeId);
    } else {
        AppendDecIntToString(writeIdStr, loc.port).Append((char)' ');
        AppendDecIntToString(writeIdStr, writeId);
    }
    if (needToForward) {
        ForwardToPeer(peerLoc, writeMaster, allowCSClearTextFlag);
    } else {
        ReadChunkMetadata();
    }
}

void
WriteIdAllocOp::ForwardToPeer(
    const ServerLocation& loc,
    bool                  writeMasterFlag,
    bool                  allowCSClearTextFlag)
{
    assert(! fwdedOp && status == 0 && (clnt || isForRecordAppend));

    RemoteSyncSMPtr const peer = isForRecordAppend ?
        appendPeer :
        FindPeer(*this, loc, writeMasterFlag, allowCSClearTextFlag);
    if (! peer) {
        if (0 <= status) {
            status    = -EHOSTUNREACH;
            statusMsg = "unable to find peer " + loc.ToString();
        }
        Done(EVENT_CMD_DONE, this);
        return;
    }
    fwdedOp = new WriteIdAllocOp(*this);
    // set by the next one in the chain.
    fwdedOp->writePrepareReplyFlag = false;
    // When forwarded op completes, call this op HandlePeerReply.
    fwdedOp->clnt                  = this;
    SET_HANDLER(this, &WriteIdAllocOp::HandlePeerReply);
    peerShortRpcFormatFlag = peer->IsShortRpcFormat();
    peer->Enqueue(fwdedOp);
}

int
WriteIdAllocOp::HandlePeerReply(int code, void* data)
{
    assert(code == EVENT_CMD_DONE && data == fwdedOp);

    if (status == 0 && fwdedOp->status < 0) {
        status    = fwdedOp->status;
        statusMsg = fwdedOp->statusMsg.empty() ?
            string("forwarding failed") : fwdedOp->statusMsg;
    }
    if (0 != status) {
        return Done(EVENT_CMD_DONE, this);
    }
    writeIdStr.Append(' ');
    AppendServers(writeIdStr, fwdedOp->writeIdStr, fwdedOp->numServers,
        initialShortRpcFormatFlag, peerShortRpcFormatFlag);
    writePrepareReplyFlag =
        writePrepareReplyFlag && fwdedOp->writePrepareReplyFlag;
    ReadChunkMetadata();
    return 0;
}

void
WriteIdAllocOp::ReadChunkMetadata()
{
    assert(status == 0);
    // Now, we are all done pending metadata read
    // page in the chunk meta-data if needed
    // if the read was successful, the call to read will callback handle-done
    SET_HANDLER(this, &WriteIdAllocOp::Done);
    const bool kAddObjectBlockMappingFlag = false;
    const int ret = gChunkManager.ReadChunkMetadata(chunkId, chunkVersion, this,
        kAddObjectBlockMappingFlag);
    if (0 <= ret) {
        return;
    }
    if (0 <= status) {
        status = ret;
    }
    Done(EVENT_CMD_DONE, this);
}

int
WriteIdAllocOp::Done(int code, void* data)
{
    UpdateStatus(code, data);
    if (0 != status) {
        if (statusMsg.empty()) {
            statusMsg = "chunk meta data read failed";
        }
        if (isForRecordAppend) {
            if (! writeIdStr.empty()) {
                gAtomicRecordAppendManager.InvalidateWriteIdDeclareFailure(
                    chunkId, writeId);
            }
        } else {
            gChunkManager.SetWriteStatus(writeId, status);
            // The write id alloc has failed; we don't want to renew the lease.
            // Now, when the client forces a re-allocation, the
            // metaserver will do a version bump; when the node that
            // was dead comes back, we can detect it has missed a write
            gLeaseClerk.InvalidateLease(chunkId, chunkVersion);
        }
    }
    KFS_LOG_STREAM(
        status == 0 ? MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
        (status == 0 ? "done: " : "failed: ") << Show() <<
    KFS_LOG_EOM;
    Submit();
    return 0;
}

void
WritePrepareOp::Execute()
{
    SET_HANDLER(this, &WritePrepareOp::Done);

    // check if we need to forward anywhere
    ServerLocation peerLoc;
    int            myPos         = -1;
    const bool     needToForward = needToForwardToPeer(shortRpcFormatFlag,
        servers, numServers, myPos, peerLoc, true, writeId);
    if (myPos < 0) {
        statusMsg = "invalid or missing Servers: field";
        status = -EINVAL;
        Submit();
        return;
    }
    if (chunkAccessTokenValidFlag &&
            (chunkAccessFlags & ChunkAccessToken::kUsesWriteIdFlag) != 0 &&
            subjectId != writeId) {
        status    = -EPERM;
        statusMsg = "access token write access mismatch";
        Submit();
        return;
    }

    const bool writeMaster = (myPos == 0);
    if (! gChunkManager.IsValidWriteId(writeId)) {
        statusMsg = "invalid write id";
        status = -EINVAL;
        Submit();
        return;
    }

    if (!gChunkManager.IsChunkMetadataLoaded(chunkId, chunkVersion)) {
        statusMsg = "checksums are not loaded";
        status = -ELEASEEXPIRED;
        Done(EVENT_CMD_DONE, this);
        return;
    }
    bool allowCSClearTextFlag = chunkAccessTokenValidFlag &&
        (chunkAccessFlags & ChunkAccessToken::kAllowClearTextFlag) != 0;
    if (writeMaster) {
        // if we are the master, check the lease...
        if (! gLeaseClerk.IsLeaseValid(
                chunkId, chunkVersion,
                &syncReplicationAccess, &allowCSClearTextFlag)) {
            KFS_LOG_STREAM_ERROR <<
                "Write prepare failed, lease expired for " << chunkId <<
            KFS_LOG_EOM;
            statusMsg = "no valid write lease exists";
            gLeaseClerk.InvalidateLease(chunkId, chunkVersion);
            status = -ELEASEEXPIRED;
            Done(EVENT_CMD_DONE, this);
            return;
        }
        // Notify the lease clerk that we are doing write.  This is to
        // signal the lease clerk to renew the lease for the chunk when appropriate.
        gLeaseClerk.DoingWrite(chunkId, chunkVersion);
    }

    if (blocksChecksums.empty()) {
        blocksChecksums = ComputeChecksums(&dataBuf, numBytes, &receivedChecksum);
    }
    if (receivedChecksum != checksum) {
        statusMsg = "checksum mismatch";
        KFS_LOG_STREAM_ERROR <<
            "checksum mismatch: sent: " << checksum <<
            ", computed: " << receivedChecksum << " for " << Show() <<
        KFS_LOG_EOM;
        status = -EBADCKSUM;
        Done(EVENT_CMD_DONE, this);
        return;
    }

    // will clone only when the op is good
    writeOp = gChunkManager.CloneWriteOp(writeId);

    if (! writeOp) {
        // the write has previously failed; so fail this op and move on
        status = gChunkManager.GetWriteStatus(writeId);
        if (status >= 0) {
            status = -EINVAL;
        }
        Done(EVENT_CMD_DONE, this);
        return;
    }

    if (needToForward) {
        ForwardToPeer(peerLoc, writeMaster, allowCSClearTextFlag);
        if (status < 0) {
            // can't forward to peer...so fail the write
            Done(EVENT_CMD_DONE, this);
            return;
        }
    }

    writeOp->offset = offset;
    writeOp->numBytes = numBytes;
    writeOp->dataBuf.Move(&dataBuf);
    writeOp->wpop = this;
    writeOp->checksums.swap(blocksChecksums);

    writeOp->enqueueTime = globalNetManager().Now();

    KFS_LOG_STREAM_DEBUG <<
        "writing to"
        " chunk: "    << chunkId <<
        " version: "  << chunkVersion <<
        " @offset: "  << offset <<
        " nbytes: "   << numBytes <<
        " checksum: " << checksum <<
    KFS_LOG_EOM;

    const int ret = gChunkManager.WriteChunk(writeOp);
    if (0 <= ret) {
        return;
    }
    if (0 <= status) {
        status = ret;
        if (writeOp) {
            statusMsg = writeOp->statusMsg;
        }
    }
    Done(EVENT_CMD_DONE, this);
}

void
WritePrepareOp::ForwardToPeer(
    const ServerLocation& loc,
    bool                  writeMasterFlag,
    bool                  allowCSClearTextFlag)
{
    assert(clnt);
    RemoteSyncSMPtr const peer = FindPeer(
        *this, loc, writeMasterFlag, allowCSClearTextFlag);
    if (! peer) {
        if (0 <= status) {
            statusMsg = "no such peer " + loc.ToString();
            status    = -EHOSTUNREACH;
        }
        return;
    }
    writeFwdOp = new WritePrepareFwdOp(*this);
    writeFwdOp->clnt = this;
    peer->Enqueue(writeFwdOp);
}

int
WritePrepareOp::Done(int code, void* data)
{
    if (0 <= status && writeFwdOp && writeFwdOp->status < 0) {
        status    = writeFwdOp->status;
        statusMsg = writeFwdOp->statusMsg;
    }
    if (status < 0) {
        // so that the error goes out on a sync
        gChunkManager.SetWriteStatus(writeId, status);
        // The write has failed; we don't want to renew the lease.
        // Now, when the client forces a re-allocation, the
        // metaserver will do a version bump; when the node that
        // was dead comes back, we can detect it has missed a write
        gLeaseClerk.InvalidateLease(chunkId, chunkVersion);
    }
    numDone++;
    if (writeFwdOp && numDone < 2) {
        return 0;
    }
    KFS_LOG_STREAM(
        status >= 0 ? MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
        (status >= 0 ? "done: " : "failed: ") << Show() <<
        " status: " << status <<
        (statusMsg.empty() ? "" : " msg: ") << statusMsg <<
    KFS_LOG_EOM;
    Submit();
    return 0;
}

void
WriteSyncOp::Execute()
{
    KFS_LOG_STREAM_DEBUG << "executing: " << Show() << KFS_LOG_EOM;
    if (status < 0) {
        Submit();
        return;
    }
    ServerLocation peerLoc;
    int            myPos = -1;
    // check if we need to forward anywhere
    const bool needToForward = needToForwardToPeer(shortRpcFormatFlag,
        servers, numServers, myPos, peerLoc, true, writeId);
    if (myPos < 0) {
        statusMsg = "invalid or missing Servers: field";
        status = -EINVAL;
        Submit();
        return;
    }
    if (chunkAccessTokenValidFlag &&
            (chunkAccessFlags & ChunkAccessToken::kUsesWriteIdFlag) != 0 &&
            subjectId != writeId) {
        status    = -EPERM;
        statusMsg = "access token write access mismatch";
        Submit();
        return;
    }

    writeMaster = myPos == 0;
    writeOp = gChunkManager.CloneWriteOp(writeId);
    if (! writeOp) {
        status    = -EINVAL;
        statusMsg = "no such write id";
        KFS_LOG_STREAM_ERROR <<
            "failed: " << statusMsg << " " << Show() <<
        KFS_LOG_EOM;
        Submit();
        return;
    }

    writeOp->enqueueTime = globalNetManager().Now();

    if (writeOp->status < 0) {
        // due to failures with data forwarding/checksum errors and such
        status    = writeOp->status;
        statusMsg = "write error";
        Submit();
        return;
    }

    if (! gChunkManager.IsChunkMetadataLoaded(chunkId, chunkVersion)) {
        // This should not normally happen, as valid write id would keep chunk
        // loaded / writable.
        status    = -ELEASEEXPIRED;
        statusMsg = "meta data unloaded";
        KFS_LOG_STREAM_ERROR <<
            "failed: " << statusMsg << " " << Show() <<
        KFS_LOG_EOM;
        gChunkManager.SetWriteStatus(writeId, status);
        Submit();
        return;
    }

    bool allowCSClearTextFlag = chunkAccessTokenValidFlag &&
        (chunkAccessFlags & ChunkAccessToken::kAllowClearTextFlag) != 0;
    if (writeMaster) {
        // if we are the master, check the lease...
        if (! gLeaseClerk.IsLeaseValid(
                chunkId, chunkVersion,
                &syncReplicationAccess, &allowCSClearTextFlag)) {
            statusMsg = "no valid write lease exists";
            status    = -ELEASEEXPIRED;
            KFS_LOG_STREAM_ERROR <<
                "failed: " << statusMsg << " " << Show() <<
            KFS_LOG_EOM;
            gChunkManager.SetWriteStatus(writeId, status);
            Submit();
            return;
        }
        // Notify the lease clerk that we are doing write.  This is to
        // signal the lease clerk to renew the lease for the chunk when
        // appropriate.
        gLeaseClerk.DoingWrite(chunkId, chunkVersion);
    }

    SET_HANDLER(this, &WriteSyncOp::Done);

    if (needToForward) {
        ForwardToPeer(peerLoc, writeMaster, allowCSClearTextFlag);
        if (status < 0) {
            // can't forward to peer...so fail the write
            Done(EVENT_CMD_DONE, this);
            return;
        }
    }

    // When write is not aligned, we can't validate the checksums handed by
    // the client. In such cases, make sure that the chunk servers agree on
    // the checksum.
    // In the write slave case, the checksums should match the write master
    // write checksum.
    bool                   mismatch    = false;
    const vector<uint32_t> myChecksums =
        gChunkManager.GetChecksums(chunkId, chunkVersion, offset, numBytes);
    if ((writeMaster && (
            (offset % CHECKSUM_BLOCKSIZE) != 0 ||
            (numBytes % CHECKSUM_BLOCKSIZE) != 0)) || checksums.empty()) {
        // Either we can't validate checksums due to alignment OR the
        // client didn't give us checksums.  In either case:
        // The sync covers a certain region for which the client
        // sent data.  The value for that region should be non-zero
        for (uint32_t i = 0; i < myChecksums.size() && ! mismatch; i++) {
            if (myChecksums[i] == 0) {
                KFS_LOG_STREAM_ERROR <<
                    "sync failed due to 0 checksum:" <<
                    " chunk: "   << chunkId <<
                    " version: " << chunkVersion <<
                    " offset: "  << offset <<
                    " size: "    << numBytes <<
                    " index: "   << i <<
                KFS_LOG_EOM;
                mismatch = true;
            }
        }
        if (! mismatch) {
            KFS_LOG_STREAM_DEBUG <<
                "validated checksums are non-zero for"
                " chunk: "   << chunkId <<
                " version: " << chunkVersion <<
                " offset: "  << offset <<
                " size: "    << numBytes <<
            KFS_LOG_EOM;
        }
    } else {
        if (myChecksums.size() != checksums.size()) {
            KFS_LOG_STREAM_ERROR <<
                "sync checksum mismatch: number of entries"
                " expected: " << myChecksums.size() <<
                " received: " << checksums.size() <<
            KFS_LOG_EOM;
            mismatch = true;
        }
        for (uint32_t i = 0; i < myChecksums.size() && ! mismatch; i++) {
            if (myChecksums[i] != checksums[i]) {
                KFS_LOG_STREAM_ERROR <<
                    "sync failed due to checksum mismatch:" <<
                    " expected: " << myChecksums[i] <<
                    " received: " << checksums[i] <<
                    KFS_LOG_EOM;
                mismatch = true;
                break;
            }
        }
        if (! mismatch) {
            KFS_LOG_STREAM_DEBUG <<
                "sync checksum verified for"
                " chunk: "    << chunkId <<
                " version: "  << chunkVersion <<
                " offset: "   << offset <<
                " checksum entries:"
                " expected: " << myChecksums.size() <<
                " received: " << checksums.size() <<
            KFS_LOG_EOM;
        }
    }
    if (mismatch) {
        status = -EAGAIN;
        statusMsg = "checksum mismatch";
        Done(EVENT_CMD_DONE, this);
        return;
    }
    assert(status >= 0);
    Done(EVENT_CMD_DONE, this);
}

void
WriteSyncOp::ForwardToPeer(
    const ServerLocation& loc,
    bool                  writeMasterFlag,
    bool                  allowCSClearTextFlag)
{
    assert(clnt);
    RemoteSyncSMPtr const peer = FindPeer(
        *this, loc, writeMasterFlag, allowCSClearTextFlag);
    if (! peer) {
        if (0 <= status) {
            statusMsg = "no such peer " + loc.ToString();
            status    = -EHOSTUNREACH;
        }
        return;
    }
    fwdedOp = new WriteSyncOp(chunkId, chunkVersion, offset, numBytes);
    fwdedOp->numServers                = numServers;
    fwdedOp->servers                   = servers;
    fwdedOp->clnt                      = this;
    fwdedOp->syncReplicationAccess     = syncReplicationAccess;
    fwdedOp->shortRpcFormatFlag        = shortRpcFormatFlag;
    fwdedOp->initialShortRpcFormatFlag = initialShortRpcFormatFlag;
    SET_HANDLER(fwdedOp, &KfsOp::HandleDone);

    if (writeMaster) {
        fwdedOp->checksums =
            gChunkManager.GetChecksums(chunkId, chunkVersion, offset, numBytes);
    } else {
        fwdedOp->checksums = checksums;
    }
    peer->Enqueue(fwdedOp);
}

int
WriteSyncOp::Done(int code, void* data)
{
    if (status >= 0 && fwdedOp && fwdedOp->status < 0) {
        status    = fwdedOp->status;
        statusMsg = fwdedOp->statusMsg;
        KFS_LOG_STREAM_ERROR <<
            "Peer: " << fwdedOp->Show() << " returned: " << fwdedOp->status <<
        KFS_LOG_EOM;
    }
    if (status < 0) {
        gChunkManager.SetWriteStatus(writeId, status);
        // The write has failed; we don't want to renew the lease.
        // Now, when the client forces a re-allocation, the
        // metaserver will do a version bump; when the node that
        // was dead comes back, we can detect it has missed a write
        gLeaseClerk.InvalidateLease(chunkId, chunkVersion);
    }
    numDone++;
    if (fwdedOp && numDone < 2) {
        return 0;
    }
    KFS_LOG_STREAM(
        status >= 0 ? MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
        (status >= 0 ? "done: " : "failed: ") << Show() <<
        " status: " << status <<
        (statusMsg.empty() ? "" : " msg: ") << statusMsg <<
    KFS_LOG_EOM;
    Submit();
    return 0;
}

void
WriteOp::Execute()
{
    const int ret = gChunkManager.WriteChunk(this);
    if (0 <= ret) {
        return;
    }
    if (0 <= status) {
        status = ret;
    }
    if (isFromRecordAppend) {
        HandleEvent(EVENT_CMD_DONE, this);
        return;
    }
    if (! wpop) {
        die("invalid null write prepare op");
        Submit();
        return;
    }
    wpop->HandleEvent(EVENT_CMD_DONE, this);
}

void
RecordAppendOp::Execute()
{
    ServerLocation peerLoc;
    int            myPos = -1;
    needToForwardToPeer(shortRpcFormatFlag,
        servers, numServers, myPos, peerLoc, true, writeId);
    if (chunkAccessTokenValidFlag &&
            (chunkAccessFlags & ChunkAccessToken::kUsesWriteIdFlag) != 0 &&
            subjectId != writeId) {
        status    = -EPERM;
        statusMsg = "access token write access mismatch";
        Submit();
        return;
    }
    gAtomicRecordAppendManager.AppendBegin(this, myPos, peerLoc);
}

void
GetRecordAppendOpStatus::Execute()
{
    if (chunkAccessTokenValidFlag &&
            (chunkAccessFlags & ChunkAccessToken::kUsesWriteIdFlag) != 0 &&
            subjectId != writeId) {
        status    = -EPERM;
        statusMsg = "access token write access mismatch";
    } else {
        gAtomicRecordAppendManager.GetOpStatus(this);
    }
    Submit();
}

void
SizeOp::Execute()
{
    int  res                        = 0;
    bool kAddObjectBlockMappingFlag = true;
    if (gChunkManager.ChunkSize(this) ||
            (res = gChunkManager.ReadChunkMetadata(
                chunkId, chunkVersion, this, kAddObjectBlockMappingFlag)) < 0) {
        if (0 <= status && res < 0) {
            status = res;
        }
        Submit();
    }
}

int
SizeOp::HandleChunkMetaReadDone(int code, void* data)
{
    UpdateStatus(code, data);
    if (0 <= status && ! gChunkManager.ChunkSize(this)) {
        statusMsg = "chunk header is not loaded";
        status    = -EAGAIN;
    }
    Submit();
    return 0;
}

void
ChunkSpaceReserveOp::Execute()
{
    ServerLocation peerLoc;
    int myPos = -1;

    needToForwardToPeer(shortRpcFormatFlag,
        servers, numServers, myPos, peerLoc, true, writeId);
    if (chunkAccessTokenValidFlag &&
            (chunkAccessFlags & ChunkAccessToken::kUsesWriteIdFlag) != 0 &&
            subjectId != writeId) {
        status    = -EPERM;
        statusMsg = "access token write access mismatch";
        Submit();
        return;
    }
    if (myPos == 0) {
        status = gAtomicRecordAppendManager.ChunkSpaceReserve(
                chunkId, writeId, nbytes, &statusMsg);
    } else {
        status    = -EINVAL;
        statusMsg = "invalid or missing Servers: field";
    }
    if (status == 0) {
        // Only master keeps track of space reservations.
        assert(myPos == 0);
        ClientSM* const client = GetClientSM();
        assert((client != 0) == (clnt != 0));
        if (client) {
            client->ChunkSpaceReserve(chunkId, writeId, nbytes);
        }
    }
    KFS_LOG_STREAM((status >= 0 || status == -ENOSPC) ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        "space reserve: "
        " chunk: "   << chunkId <<
        " version: " << chunkVersion <<
        " writeId: " << writeId <<
        " bytes: "   << nbytes  <<
        " status: "  << status  <<
    KFS_LOG_EOM;
    Submit();
}

void
ChunkSpaceReleaseOp::Execute()
{
    ServerLocation peerLoc;
    int myPos = -1;

    needToForwardToPeer(shortRpcFormatFlag,
        servers, numServers, myPos, peerLoc, true, writeId);
    if (chunkAccessTokenValidFlag &&
            (chunkAccessFlags & ChunkAccessToken::kUsesWriteIdFlag) != 0 &&
            subjectId != writeId) {
        status    = -EPERM;
        statusMsg = "access token write access mismatch";
        Submit();
        return;
    }
    size_t rsvd = 0;
    if (myPos == 0) {
        ClientSM* const client = GetClientSM();
        assert((client != 0) == (clnt != 0));
        rsvd = client ?
            min(client->GetReservedSpace(chunkId, writeId), nbytes) : nbytes;
        status = gAtomicRecordAppendManager.ChunkSpaceRelease(
            chunkId, writeId, rsvd, &statusMsg);
        if (status == 0 && client) {
            client->UseReservedSpace(chunkId, writeId, rsvd);
        }
    } else {
        status    = -EINVAL;
        statusMsg = "invalid or missing Servers: field";
    }
    KFS_LOG_STREAM(status >= 0 ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        "space release: "
        " chunk: "     << chunkId <<
        " version: "   << chunkVersion <<
        " writeId: "   << writeId <<
        " requested: " << nbytes  <<
        " reserved: "  << rsvd    <<
        " status: "    << status  <<
    KFS_LOG_EOM;
    Submit();
}

void
GetChunkMetadataOp::Execute()
{
    SET_HANDLER(this, &GetChunkMetadataOp::HandleChunkMetaReadDone);
    bool const kAddObjectBlockMappingFlag = true;
    Submit(gChunkManager.ReadChunkMetadata(chunkId, 0, this,
            kAddObjectBlockMappingFlag));
}

int
GetChunkMetadataOp::HandleChunkMetaReadDone(int code, void* data)
{
    UpdateStatus(code, data);
    if (status < 0) {
        Submit();
        return 0;
    }
    const ChunkInfo_t * const info = gChunkManager.GetChunkInfo(chunkId, 0);
    if (info) {
        if (info->chunkBlockChecksum || info->chunkSize == 0) {
            chunkVersion = info->chunkVersion;
            chunkSize    = info->chunkSize;
            if (info->chunkBlockChecksum) {
                dataBuf.CopyIn((const char *)info->chunkBlockChecksum,
                    MAX_CHUNK_CHECKSUM_BLOCKS * sizeof(uint32_t));
                numBytesIO = dataBuf.BytesConsumable();
            }
        } else {
            assert(! "no checksums");
            status = -EIO;
        }
    } else {
        status = -EBADF;
    }

    if (status < 0 || ! readVerifyFlag) {
        Submit();
        return 0;
    }

    numBytesScrubbed = 0;
    readOp.chunkId = chunkId;
    readOp.chunkVersion = chunkVersion;
    readOp.offset = 0;
    readOp.numBytes = min((int64_t) 1 << 20, chunkSize);

    readOp.SetScrubOp(this);
    SET_HANDLER(this, &GetChunkMetadataOp::HandleScrubReadDone);
    Submit(gChunkManager.ReadChunk(&readOp));
    return 0;
}

int
GetChunkMetadataOp::HandleScrubReadDone(int code, void* data)
{
    if (code == EVENT_DISK_ERROR) {
        UpdateStatus(code, data);
        KFS_LOG_STREAM_ERROR <<
            "disk error:"
            " chunk: "   << chunkId <<
            " version: " << chunkVersion <<
            " status: "  << status <<
        KFS_LOG_EOM;
        gChunkManager.ChunkIOFailed(
            chunkId, chunkVersion, status, readOp.diskIo.get());
        Submit();
        return 0;
    }
    if (code != EVENT_DISK_READ || ! data) {
        die("scrub read: unexpected event or event data");
        status = -EFAULT;
        Submit();
        return 0;
    }
    IOBuffer* const b = reinterpret_cast<IOBuffer*>(data);
    readOp.dataBuf.Move(b);
    if ((size_t)chunkSize <
            (size_t)(readOp.offset + readOp.dataBuf.BytesConsumable()) &&
            (size_t)readOp.numBytes <
                (size_t)readOp.dataBuf.BytesConsumable()) {
        // trim the extra stuff off the end.
        readOp.dataBuf.Trim(readOp.numBytes);
    }
    // verify checksum
    gChunkManager.ReadChunkDone(&readOp);
    status = readOp.status;
    if (0 <= status) {
        KFS_LOG_STREAM_DEBUG <<
            "scrub read succeeded"
            " chunk: "   << chunkId <<
            " version: " << chunkVersion <<
            " offset: "  << readOp.offset <<
        KFS_LOG_EOM;
        // checksum verified; setup the next read
        numBytesScrubbed += readOp.dataBuf.BytesConsumable();
        readOp.offset += readOp.dataBuf.BytesConsumable();
        readOp.numBytes = min((int64_t)kChunkReadSize,
            chunkSize - numBytesScrubbed);
        // throw away the data
        readOp.dataBuf.Clear();
        if (chunkSize <= numBytesScrubbed) {
            KFS_LOG_STREAM_DEBUG <<
                "scrub succeeded"
                " chunk: "      << chunkId <<
                " version: "    << chunkVersion <<
                " bytes read: " << numBytesScrubbed <<
            KFS_LOG_EOM;
            Submit();
            return 0;
        }
        const int ret = gChunkManager.ReadChunk(&readOp);
        if (0 <= ret) {
            return 0;
        }
        if (0 <= status) {
            status = ret;
        }
    }
    if (status < 0) {
        readOp.dataBuf.Clear();
        KFS_LOG_STREAM_ERROR <<
            "scrub read failed:"
            " chunk: "   << chunkId <<
            " version: " << chunkVersion <<
            " status: "  << status <<
        KFS_LOG_EOM;
        Submit();
    }
    return 0;
}

void
PingOp::Execute()
{
    int     chunkDirs         = 0;
    int     writableDirs      = 0;
    int     evacuateChunks    = 0;
    int64_t evacuateByteCount = 0;
    totalFsSpace = 0;
    totalSpace = gChunkManager.GetTotalSpace(totalFsSpace, chunkDirs,
        evacuateInFlightCount, writableDirs, evacuateChunks, evacuateByteCount);
    usedSpace = gChunkManager.GetUsedSpace();
    if (usedSpace < 0) {
        usedSpace = 0;
    }
    status = 0;
    Submit();
}

void
DumpChunkMapOp::Execute()
{
    response.Clear();
    IOBuffer::WOStream wos;
    ostream& os = wos.Set(response);
    gChunkManager.DumpChunkMap(os);
    status = os ? 0 : -EIO;
    wos.Reset();
    Submit();
}

void
StatsOp::Execute()
{
    ostringstream os;

    os << "Num aios: " << 0 << "\r\n";
    os << "Num ops: " << gChunkServer.GetNumOps() << "\r\n";
    globals().counterManager.Show(os);
    stats = os.str();
    status = 0;
    Submit();
}

inline static bool
OkHeader(const KfsOp* op, ReqOstream& os, bool checkStatus = true)
{
    if (op->shortRpcFormatFlag) {
        os << hex;
    }
    os << "OK\r\n";
    os << (op->shortRpcFormatFlag ? "c:" : "Cseq: ") << op->seq << "\r\n";
    os << (op->shortRpcFormatFlag ? "s:" : "Status: ") <<
        (op->status >= 0 ? op->status : -SysToKfsErrno(-op->status)) << "\r\n";
    if (! op->statusMsg.empty()) {
        if (op->statusMsg.find('\r') != string::npos ||
                op->statusMsg.find('\n') != string::npos) {
            die("invalid RPC status message");
        } else {
            os << (op->shortRpcFormatFlag ? "m:" : "Status-message: ") <<
                op->statusMsg << "\r\n";
        }
    }
    if (checkStatus && op->status < 0) {
        os << "\r\n";
    }
    return (op->status >= 0);
}

inline static ReqOstream&
PutHeader(const KfsOp* op, ReqOstream &os)
{
    OkHeader(op, os, false);
    return os;
}

///
/// Generate response for an op based on the KFS protocol.
///
void
KfsOp::Response(ReqOstream& os)
{
    PutHeader(this, os) << "\r\n";
}

void
ChunkAccessRequestOp::Response(ReqOstream& os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    WriteChunkAccessResponse(os, writeId, ChunkAccessToken::kUsesWriteIdFlag);
    os << "\r\n";
}

void
SizeOp::Response(ReqOstream& os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    if (checkFlag && 0 <= chunkVersion) {
        os << (shortRpcFormatFlag ? "V:"  : "Chunk-version: ") <<
            chunkVersion << "\r\n";
        os << (shortRpcFormatFlag ? "SC:" : "Stable-flag: ") <<
            (stableFlag ? 1 : 0) << "\r\n";
    }
    os << (shortRpcFormatFlag ? "S:" : "Size: ") << size << "\r\n\r\n";
}

void
GetChunkMetadataOp::Response(ReqOstream& os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os <<
    (shortRpcFormatFlag ? "H:" : "Chunk-handle: ")  << chunkId      << "\r\n" <<
    (shortRpcFormatFlag ? "V:" : "Chunk-version: ") << chunkVersion << "\r\n" <<
    (shortRpcFormatFlag ? "S:" : "Size: ")          << chunkSize    << "\r\n" <<
    (shortRpcFormatFlag ? "l:" : "Content-length: ") << numBytesIO  << "\r\n"
    "\r\n";
}

void
ReadOp::Response(ReqOstream& os)
{
    PutHeader(this, os);
    if (status < 0) {
        os << "\r\n";
        return;
    }
    if (shortRpcFormatFlag) {
        os << "D:" << diskIOTime << "\r\n";
    } else {
        os << "DiskIOtime: " << (diskIOTime * 1e-6) << "\r\n";
    }
    os << (shortRpcFormatFlag ? "KC:" : "Checksum-entries: ") <<
        checksum.size() << "\r\n";
    if (skipVerifyDiskChecksumFlag) {
        os << (shortRpcFormatFlag ? "KS:1\r\n" : "Skip-Disk-Chksum: 1\r\n");
    }
    if (checksum.empty()) {
        os << (shortRpcFormatFlag ? "K:0\r\n" : "Checksums: 0\r\n");
    } else {
        os << (shortRpcFormatFlag ? "K:" : "Checksums:");
        for (size_t i = 0; i < checksum.size(); i++) {
            os << ' ' << checksum[i];
        }
        os << "\r\n";
    }
    os << (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
        numBytesIO << "\r\n\r\n";
}

void
WriteIdAllocOp::Response(ReqOstream& os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    if (writePrepareReplyFlag) {
        os << (shortRpcFormatFlag ? "WR:1\r\n" : "Write-prepare-reply: 1\r\n");
    }
    WriteChunkAccessResponse(os, writeId, ChunkAccessToken::kUsesWriteIdFlag);
    os << (shortRpcFormatFlag ? "W:" : "Write-id: ") << writeIdStr <<  "\r\n"
    "\r\n";
}

void
WritePrepareOp::Response(ReqOstream& os)
{
    if (! replyRequestedFlag) {
        // no reply for a prepare...the reply is covered by sync
        return;
    }
    ChunkAccessRequestOp::Response(os);
}

void
RecordAppendOp::Response(ReqOstream& os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    WriteChunkAccessResponse(os, writeId, ChunkAccessToken::kUsesWriteIdFlag);
    os << (shortRpcFormatFlag ? "FO:" : "File-offset: ") << fileOffset <<
    "\r\n\r\n";
}

void
RecordAppendOp::Request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "RECORD_APPEND \r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << seq << "\r\n"
    ;
    if (! shortRpcFormatFlag) {
        os << "Version: " << KFS_VERSION_STR << "\r\n";
    }
    os <<
    (shortRpcFormatFlag ? "H:"  : "Chunk-handle: ") << chunkId      << "\r\n" <<
    (shortRpcFormatFlag ? "V:"  : "Chunk-version: ")<< chunkVersion << "\r\n";
    if (0 <= offset || ! shortRpcFormatFlag) {
        os << (shortRpcFormatFlag ? "O:"  : "Offset: ") << offset << "\r\n";
    }
    os <<
    (shortRpcFormatFlag ? "F:"  : "File-offset: ")  << fileOffset   << "\r\n" <<
    (shortRpcFormatFlag ? "B:"  : "Num-bytes: ")    << numBytes     << "\r\n" <<
    (shortRpcFormatFlag ? "K:"  : "Checksum: ")     << checksum     << "\r\n" <<
    (shortRpcFormatFlag ? "Cc:" : "Client-cseq: ")  << clientSeq    << "\r\n" <<
    (shortRpcFormatFlag ? "M:"  : "Master-committed: ") <<
        masterCommittedOffset << "\r\n"
    ;
    WriteServers(os, *this);
    WriteSyncReplicationAccess(syncReplicationAccess, os, shortRpcFormatFlag,
        shortRpcFormatFlag ? "AF:" : "Access-fwd-length: ");
}

void
GetRecordAppendOpStatus::Request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "GET_RECORD_APPEND_OP_STATUS \r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ")          << seq     << "\r\n" <<
    (shortRpcFormatFlag ? "H:" : "Chunk-handle: ")  << chunkId << "\r\n" <<
    (shortRpcFormatFlag ? "W:" : "Write-id: ")      << writeId << "\r\n"
    "\r\n";
}

void
GetRecordAppendOpStatus::Response(ReqOstream& os)
{
    PutHeader(this, os);
    os <<
        (shortRpcFormatFlag ? "V:"  : "Chunk-version: ")         <<
            chunkVersion << "\r\n" <<
        (shortRpcFormatFlag ? "Oc:" : "Op-seq: ")
            << opSeq << "\r\n" <<
        (shortRpcFormatFlag ? "Os:" : "Op-status: ") <<
            (opStatus < 0 ? -SysToKfsErrno(-opStatus) : opStatus) << "\r\n" <<
        (shortRpcFormatFlag ? "OO:" : "Op-offset: ")             <<
            opOffset << "\r\n" <<
        (shortRpcFormatFlag ? "OL:" : "Op-length: ")             <<
            opLength << "\r\n" <<
        (shortRpcFormatFlag ? "AC:" : "Wid-append-count: ")      <<
            widAppendCount << "\r\n" <<
        (shortRpcFormatFlag ? "RW:" : "Wid-bytes-reserved: ")    <<
            widBytesReserved << "\r\n" <<
        (shortRpcFormatFlag ? "RB:" : "Chunk-bytes-reserved: ")  <<
            chunkBytesReserved << "\r\n" <<
        (shortRpcFormatFlag ? "LR:" : "Remaining-lease-time: ")  <<
            remainingLeaseTime << "\r\n" <<
        (shortRpcFormatFlag ? "CO:" : "Master-commit-offset: ")  <<
            masterCommitOffset << "\r\n" <<
        (shortRpcFormatFlag ? "CN:" : "Next-commit-offset: ")    <<
            nextCommitOffset << "\r\n" <<
        (shortRpcFormatFlag ? "WR:" : "Wid-read-only: ")         <<
            (widReadOnlyFlag ? 1 : 0) << "\r\n" <<
        (shortRpcFormatFlag ? "WP:" : "Wid-was-read-only: ")     <<
            (widWasReadOnlyFlag ? 1 : 0) << "\r\n" <<
        (shortRpcFormatFlag ? "MC:" : "Chunk-master: ")          <<
            (masterFlag ? 1 : 0) << "\r\n" <<
        (shortRpcFormatFlag ? "SC:" : "Stable-flag: ")           <<
            (stableFlag ? 1 : 0) << "\r\n" <<
        (shortRpcFormatFlag ? "AO:" : "Open-for-append-flag: ")  <<
            (openForAppendFlag  ? 1 : 0) << "\r\n" <<
        (shortRpcFormatFlag ? "AS:" : "Appender-state: ")        <<
            appenderState << "\r\n" <<
        (shortRpcFormatFlag ? "As:" : "Appender-state-string: ") <<
            appenderStateStr << "\r\n"
    "\r\n";
}

void
CloseOp::Request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "CLOSE \r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << seq << "\r\n"
    ;
    if (! shortRpcFormatFlag) {
        os << "Version: " << KFS_VERSION_STR << "\r\n";
    }
    os <<
    (shortRpcFormatFlag ? "A:" : "Need-ack: ") <<
        (needAck ? 1 : 0) << "\r\n" <<
    (shortRpcFormatFlag ? "V:" : "Chunk-version: ") <<
        chunkVersion    << "\r\n"
    ;
    if (0 < numServers) {
        WriteServers(os, *this, hasWriteId);
    }
    os << (shortRpcFormatFlag ? "H:" : "Chunk-handle: ") << chunkId << "\r\n";
    if (hasWriteId) {
        os << (shortRpcFormatFlag ? "W:1\r\n" : "Has-write-id: 1\r\n");
    }
    if (masterCommitted >= 0) {
        os  << (shortRpcFormatFlag ? "M:" : "Master-committed: ") <<
            masterCommitted << "\r\n";
    }
    WriteSyncReplicationAccess(syncReplicationAccess, os, shortRpcFormatFlag,
        shortRpcFormatFlag ? "l:" : "Content-length: ");
}

void
SizeOp::Request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "SIZE\r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << seq << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: "       << KFS_VERSION_STR << "\r\n";
    }
    if (checkFlag) {
        os << (shortRpcFormatFlag ? "K:1" : "Chunk-check: 1") << "\r\n";
    }
    os <<
    (shortRpcFormatFlag ? "H:" : "Chunk-handle: ")  << chunkId << "\r\n" <<
    (shortRpcFormatFlag ? "V:" : "Chunk-version: ") << chunkVersion    << "\r\n"
    "\r\n";
}

void
GetChunkMetadataOp::Request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "GET_CHUNK_METADATA\r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << seq << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: "       << KFS_VERSION_STR << "\r\n";
    }
    os <<
    (shortRpcFormatFlag ? "H:"  : "Chunk-handle: ") << chunkId << "\r\n" <<
    (shortRpcFormatFlag ? "RV:" : "Read-verify: ")  <<
        (readVerifyFlag ? 1 : 0) << "\r\n"
    ;
    if (requestChunkAccess) {
        os << (shortRpcFormatFlag ? "C:" : "C-access: ") <<
            requestChunkAccess << "\r\n";
    }
    os << "\r\n";
}

void
ReadOp::Request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "READ\r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << seq << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: "       << KFS_VERSION_STR << "\r\n";
    }
    os <<
    (shortRpcFormatFlag ? "H:" : "Chunk-handle: ")  << chunkId      << "\r\n" <<
    (shortRpcFormatFlag ? "V:" : "Chunk-version: ") << chunkVersion << "\r\n" <<
    (shortRpcFormatFlag ? "O:" : "Offset: ")        << offset       << "\r\n" <<
    (shortRpcFormatFlag ? "B:" : "Num-bytes: ")     << numBytes     << "\r\n"
    ;
    if (skipVerifyDiskChecksumFlag) {
        os << (shortRpcFormatFlag ? "KS:1\r\n" : "Skip-Disk-Chksum: 1\r\n");
    }
    if (requestChunkAccess) {
        os << (shortRpcFormatFlag ? "C:" : "C-access: ") <<
            requestChunkAccess << "\r\n";
    }
    os << "\r\n";
}

void
WriteIdAllocOp::Request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "WRITE_ID_ALLOC\r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << seq << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: "       << KFS_VERSION_STR << "\r\n";
    }
    os <<
    (shortRpcFormatFlag ? "H:" : "Chunk-handle: ")  << chunkId      << "\r\n" <<
    (shortRpcFormatFlag ? "V:" : "Chunk-version: ") << chunkVersion << "\r\n" <<
    (shortRpcFormatFlag ? "O:" : "Offset: ")        << offset       << "\r\n" <<
    (shortRpcFormatFlag ? "B:" : "Num-bytes: ")     << numBytes     << "\r\n" <<
    (shortRpcFormatFlag ? "A:" : "For-record-append: ") <<
        (isForRecordAppend ? 1 : 0) << "\r\n" <<
    (shortRpcFormatFlag ? "Cc:" : "Client-cseq: ")  << clientSeq    << "\r\n"
    ;
    const bool kHasWriteId = false;
    WriteServers(os, *this, kHasWriteId);
    WriteSyncReplicationAccess(syncReplicationAccess, os, shortRpcFormatFlag,
        shortRpcFormatFlag ? "l:" : "Content-length: ");
}

void
WritePrepareFwdOp::Request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "WRITE_PREPARE\r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << seq << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: "       << KFS_VERSION_STR << "\r\n";
    }
    os <<
    (shortRpcFormatFlag ? "H:"  : "Chunk-handle: ")  <<
        owner.chunkId      << "\r\n" <<
    (shortRpcFormatFlag ? "V:"  : "Chunk-version: ") <<
        owner.chunkVersion << "\r\n" <<
    (shortRpcFormatFlag ? "O:"  : "Offset: ")        <<
        owner.offset       << "\r\n" <<
    (shortRpcFormatFlag ? "B:"  : "Num-bytes: ")     <<
        owner.numBytes     << "\r\n" <<
    (shortRpcFormatFlag ? "K:" : "Checksum: ")      <<
        owner.checksum     << "\r\n" <<
    (shortRpcFormatFlag ? "RR:" : "Reply: ")         <<
        (owner.replyRequestedFlag ? 1 : 0) << "\r\n"
    ;
    const bool kHasWriteIdFlag = true;
    WriteServers(os, owner, shortRpcFormatFlag, kHasWriteIdFlag);
    WriteSyncReplicationAccess(owner.syncReplicationAccess, os,
        shortRpcFormatFlag,
        shortRpcFormatFlag ? "AF:" : "Access-fwd-length: ");
}

void
WriteSyncOp::Request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "WRITE_SYNC\r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << seq << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: "       << KFS_VERSION_STR << "\r\n";
    }
    os <<
    (shortRpcFormatFlag ? "H:"  : "Chunk-handle: ") << chunkId      << "\r\n" <<
    (shortRpcFormatFlag ? "V:"  : "Chunk-version: ")<< chunkVersion << "\r\n" <<
    (shortRpcFormatFlag ? "O:"  : "Offset: ")       << offset       << "\r\n" <<
    (shortRpcFormatFlag ? "B:"  : "Num-bytes: ")    << numBytes     << "\r\n" <<
    (shortRpcFormatFlag ? "KC:" : "Checksum-entries: ") <<
        checksums.size() << "\r\n";
    if (checksums.empty()) {
        os << (shortRpcFormatFlag ? "K:0\r\n" : "Checksums: 0\r\n");
    } else {
        os << (shortRpcFormatFlag ? "K:" : "Checksums:");
        for (size_t i = 0; i < checksums.size(); i++) {
            os << ' ' << checksums[i];
        }
        os << "\r\n";
    }
    WriteServers(os, *this);
    WriteSyncReplicationAccess(syncReplicationAccess, os, shortRpcFormatFlag,
        shortRpcFormatFlag ? "l:" : "Content-length: ");
}

static void
SendCryptoKey(ReqOstream& os, CryptoKeys::KeyId keyId, const CryptoKeys::Key& key,
    bool shortRpcFormatFlag)
{
    os <<
        (shortRpcFormatFlag ? "KI:"   : "CKeyId: ") << keyId << "\r\n" <<
        (shortRpcFormatFlag ? "CKey:" : "CKey: ")   << key   << "\r\n"
    ;
}

void
HeartbeatOp::Response(ReqOstream& os)
{
    OkHeader(this, os);
    if (sendCurrentKeyFlag) {
        const bool kShortRpcFormatFlag = false;
        SendCryptoKey(os, currentKeyId, currentKey, kShortRpcFormatFlag);
    }
    if (response.IsEmpty()) {
        os << "\r\n"; // No counters, empty response, mark end of headers.
    }
}

void
ReplicateChunkOp::Response(ReqOstream& os)
{
    PutHeader(this, os) <<
    (shortRpcFormatFlag ? "P:" : "File-handle: ")   << fid          << "\r\n" <<
    (shortRpcFormatFlag ? "V:" : "Chunk-version: ") << chunkVersion << "\r\n"
    ;
    if (! invalidStripeIdx.empty()) {
        os << (shortRpcFormatFlag ? "IS:" : "Invalid-stripes: ") <<
            invalidStripeIdx << "\r\n";
    }
    os << "\r\n";
}

void
PingOp::Response(ReqOstream& os)
{
    ServerLocation loc = gMetaServerSM.CetPrimaryLocation();

    PutHeader(this, os);
    os <<
        "Meta-server-host: " << loc.hostname          << "\r\n"
        "Meta-server-port: " << loc.port              << "\r\n"
        "Total-space: "      << totalSpace            << "\r\n"
        "Total-fs-space: "   << totalFsSpace          << "\r\n"
        "Used-space: "       << usedSpace             << "\r\n"
        "Num-evacuate: "     << evacuateInFlightCount << "\r\n"
    "\r\n";
}

void
BeginMakeChunkStableOp::Response(ReqOstream& os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os <<
    (shortRpcFormatFlag ? "S:" : "Chunk-size: ")     << chunkSize << "\r\n" <<
    (shortRpcFormatFlag ? "K:" : "Chunk-checksum: ") << chunkChecksum << "\r\n"
    "\r\n";
}

void
DumpChunkMapOp::Response(ReqOstream& os)
{
    PutHeader(this, os) <<
    (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
        response.BytesConsumable() << "\r\n"
    "\r\n";
}

void
StatsOp::Response(ReqOstream& os)
{
    PutHeader(this, os) << stats << "\r\n";
}

////////////////////////////////////////////////
// Now the handle done's....
////////////////////////////////////////////////

int
SizeOp::HandleDone(int code, void* data)
{
    // notify the owning object that the op finished
    Submit();
    return 0;
}

int
GetChunkMetadataOp::HandleDone(int code, void* data)
{
    // notify the owning object that the op finished
    Submit();
    return 0;
}

class ReadChunkMetaNotifier
{
public:
    ReadChunkMetaNotifier(int r)
        : res(r)
        {}
    void operator()(KfsOp* op)
    {
        if (0 == res) {
            op->HandleEvent(EVENT_CMD_DONE, op);
        } else {
            int r = res;
            op->HandleEvent(EVENT_DISK_ERROR, &r);
        }
    }
private:
    const int res;
};

int
ReadChunkMetaOp::HandleDone(int code, void* data)
{
    IOBuffer* dataBuf = 0;
    if (code == EVENT_DISK_ERROR) {
        UpdateStatus(code, data);
        KFS_LOG_STREAM_ERROR <<
            " read meta disk error:"
            " chunk: "   << chunkId <<
            " version: " << chunkVersion <<
            " status: "  << status <<
        KFS_LOG_EOM;
    } else if (code == EVENT_DISK_READ && data) {
        dataBuf = reinterpret_cast<IOBuffer*>(data);
    } else {
        status = -EINVAL;
        ostringstream os;
        os  << "read chunk meta data unexpected event: "
            " code: "    << code <<
            " data: "    << data <<
            " chunk: "   << chunkId <<
            " version: " << chunkVersion
        ;
        die(os.str());
    }
    gChunkManager.ReadChunkMetadataDone(this, dataBuf);
    if (clnt) {
        if (0 == status) {
            clnt->HandleEvent(EVENT_CMD_DONE, this);
        } else {
            int res = status;
            clnt->HandleEvent(EVENT_DISK_ERROR, &res);
        }
    }
    for_each(waiters.begin(), waiters.end(), ReadChunkMetaNotifier(status));

    delete this;
    return 0;
}

WriteOp::~WriteOp()
{
    if (isWriteIdHolder) {
        // track how long it took for the write to finish up:
        // enqueueTime tracks when the last write was done to this
        // writeid
        const int64_t kMicroSecs = 1000 * 1000;
        const int64_t timeSpent = int64_t(enqueueTime) * kMicroSecs - startTime;
        // we don't want write id's to pollute stats
        startTime = microseconds();
        OpCounters::WriteDuration(timeSpent);
    }
    if (rop) {
        rop->wop = 0;
        // rop->dataBuf can be non null when read completes but WriteChunk
        // fails, and returns before using this buff.
        // Read op destructor deletes dataBuf.
        delete rop;
    }
}

WriteIdAllocOp::~WriteIdAllocOp()
{
    delete fwdedOp;
}

WritePrepareOp::~WritePrepareOp()
{
    delete writeFwdOp;
    delete writeOp;
}

WriteSyncOp::~WriteSyncOp()
{
    delete fwdedOp;
    delete writeOp;
}

void
LeaseRenewOp::Request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "LEASE_RENEW\r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << seq << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: " << KFS_VERSION_STR << "\r\n";
    }
    os <<
    (shortRpcFormatFlag ? "H:" : "Chunk-handle: ") << chunkId   << "\r\n" <<
    (shortRpcFormatFlag ? "L:" : "Lease-id: ")     << leaseId   << "\r\n" <<
    (shortRpcFormatFlag ? "T:" : "Lease-type: ")   << leaseType << "\r\n"
    ;
    if (emitCSAceessFlag) {
        os << (shortRpcFormatFlag ? "A:1\r\n" : "CS-access: 1\r\n");
    }
    if (chunkVersion < 0) {
        os << (shortRpcFormatFlag ? "O:" : "Chunk-pos: ") <<
            (-(int64_t)chunkVersion - 1) << "\r\n";
    }
    os << "\r\n";
}

int
LeaseRenewOp::HandleDone(int code, void* data)
{
    assert(data == this && clnt);
    return Submit();
}

void
LeaseRelinquishOp::Request(ReqOstream& os)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "LEASE_RELINQUISH\r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << seq << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: " << KFS_VERSION_STR << "\r\n";
    }
    os <<
    (shortRpcFormatFlag ? "H:" : "Chunk-handle: ") << chunkId   << "\r\n" <<
    (shortRpcFormatFlag ? "L:" : "Lease-id: ")     << leaseId   << "\r\n" <<
    (shortRpcFormatFlag ? "T:" : "Lease-type: ")  << leaseType << "\r\n"
    ;
    if (0 <= chunkSize) {
        os << (shortRpcFormatFlag ? "S:" : "Chunk-size: ") <<
            chunkSize << "\r\n";
    }
    if (hasChecksum) {
        os << (shortRpcFormatFlag ? "K:" : "Chunk-checksum: ") <<
            chunkChecksum << "\r\n";
    }
    if (chunkVersion < 0) {
        os << (shortRpcFormatFlag ? "O:" : "Chunk-pos: ") <<
            (-(int64_t)chunkVersion - 1) << "\r\n";
    }
    os << "\r\n";
}

int
LeaseRelinquishOp::HandleDone(int code, void* data)
{
    if (code != EVENT_CMD_DONE || this != data) {
        die("LeaseRelinquishOp: invalid completion");
    }
    delete this;
    return 0;
}

void
CorruptChunkOp::Request(ReqOstream& os)
{
    if (kMaxChunkIds < chunkCount) {
        die("invalid corrupt chunk RPC");
    }
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "CORRUPT_CHUNK\r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << seq << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: " << KFS_VERSION_STR << "\r\n";
    }
    os << (shortRpcFormatFlag ? "L:" : "Is-chunk-lost: ") <<
        (isChunkLost ? 1 : 0) << "\r\n";
    if (noReply) {
        os << (shortRpcFormatFlag ? "N:1\r\n" : "No-reply: 1\r\n");
    }
    if (! chunkDir.empty()) {
        os <<
        (shortRpcFormatFlag ? "D:" : "Chunk-dir: ") << chunkDir << "\r\n" <<
        (shortRpcFormatFlag ? "O:" : "Dir-ok: ") <<
            (dirOkFlag ? 1 : 0) << "\r\n"
        ;
    }
    if (0 < chunkCount) {
        os <<
        (shortRpcFormatFlag ? "C:" : "Num-chunks: ") << chunkCount << "\r\n" <<
        (shortRpcFormatFlag ? "I:" : "Ids:");
        for (int i = 0; i < chunkCount; i++) {
            os << ' ' << chunkIds[i];
        }
        os << "\r\n";
    }
    os << "\r\n";
}

int
CorruptChunkOp::HandleDone(int code, void* data)
{
    if (code != EVENT_CMD_DONE || this != data) {
        die("CorruptChunkOp: invalid completion");
    }
    if (notifyChunkManagerFlag) {
        gChunkManager.NotifyStaleChunkDone(*this);
    } else {
        delete this;
    }
    return 0;
}

void
EvacuateChunksOp::Request(ReqOstream& os)
{
    assert(numChunks <= kMaxChunkIds);

    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "EVACUATE_CHUNK\r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << seq << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: " << KFS_VERSION_STR << "\r\n";
    }
    if (totalSpace >= 0) {
        os << (shortRpcFormatFlag ? "T:" : "Total-space: ") <<
            totalSpace << "\r\n";
    }
    if (usedSpace >= 0) {
        os << (shortRpcFormatFlag ? "U:" : "Used-space: ") <<
            usedSpace << "\r\n";
    }
    if (chunkDirs >= 0) {
        os << (shortRpcFormatFlag ? "D:" : "Num-drives: ") <<
            chunkDirs << "\r\n";
    }
    if (writableChunkDirs >= 0) {
        os << (shortRpcFormatFlag ? "W:" : "Num-wr-drives: ") <<
            writableChunkDirs << "\r\n";
        AppendStorageTiersInfo(
            shortRpcFormatFlag ? "S" : "Storage-tiers:", os, tiersInfo);
    }
    if (evacuateInFlightCount >= 0) {
        os << (shortRpcFormatFlag ? "E:" : "Num-evacuate: ") <<
            evacuateInFlightCount << "\r\n";
    }
    os << (shortRpcFormatFlag ? "I:" : "Chunk-ids:");
    for (int i = 0; i < numChunks; i++) {
        os << " " << chunkIds[i];
    }
    os << "\r\n\r\n";
}

void
AvailableChunksOp::Request(ReqOstream& os)
{
    if (numChunks <= 0 && noReply) {
        return;
    }
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "AVAILABLE_CHUNK\r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << seq << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: " << KFS_VERSION_STR << "\r\n";
    }
    if (helloFlag) {
        os << (shortRpcFormatFlag ? "H:1" : "Hello: 1") << "\r\n";
    }
    if (endOfNotifyFlag) {
        os << (shortRpcFormatFlag ? "E:1" : "End-notify: 1") << "\r\n";
    }
    os << (shortRpcFormatFlag ? "N:" : "Num-chunks:") << numChunks << "\r\n";
    os << (shortRpcFormatFlag ? "I:" : "Chunk-ids-vers:");
    os << hex;
    for (int i = 0; i < numChunks; i++) {
        os << ' ' << chunks[i].first << ' ' << chunks[i].second;
    }
    os << "\r\n\r\n";
}

void
HelloMetaOp::Request(ReqOstream& os, IOBuffer& buf)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os << "HELLO \r\n";
    if (shortRpcFormatFlag) {
        os << "c:" << seq << "\r\n";
    } else {
        os <<
        "Version: " << KFS_VERSION_STR << "\r\n"
        "Cseq: "    << seq             << "\r\n";
    }
    os <<
    (shortRpcFormatFlag ? "SN:" : "Chunk-server-name: ") <<
        myLocation.hostname << "\r\n" <<
    (shortRpcFormatFlag ? "SP:" : "Chunk-server-port: ") <<
        myLocation.port << "\r\n" <<
    (shortRpcFormatFlag ? "CK:" : "Cluster-key: ") <<
        clusterKey << "\r\n" <<
    (shortRpcFormatFlag ? "5:"  : "MD5Sum: ") <<
        md5sum  << "\r\n";
    if (! nodeId.empty()) {
        os <<
        (shortRpcFormatFlag ? "ND:"  : "Node-id: ") <<
            nodeId  << "\r\n";
    }
    os <<
    (shortRpcFormatFlag ? "RI:" : "Rack-id: ") <<
        rackId << "\r\n" <<
    (shortRpcFormatFlag ? "T:"  : "Total-space: ") <<
        totalSpace << "\r\n" <<
    (shortRpcFormatFlag ? "TF:" : "Total-fs-space: ") <<
        totalFsSpace << "\r\n" <<
    (shortRpcFormatFlag ? "US:" : "Used-space: ") <<
        usedSpace << "\r\n" <<
    (shortRpcFormatFlag ? "UP:" : "Uptime: ") <<
        globalNetManager().UpTime() << "\r\n" <<
    (shortRpcFormatFlag ? "NC:" : "Num-chunks: ") <<
        chunkLists[kStableChunkList].count << "\r\n" <<
    (shortRpcFormatFlag ? "NA:" : "Num-not-stable-append-chunks: ") <<
        chunkLists[kNotStableAppendChunkList].count << "\r\n" <<
    (shortRpcFormatFlag ? "NS:" : "Num-not-stable-chunks: ") <<
        chunkLists[kNotStableChunkList].count << "\r\n" <<
    (shortRpcFormatFlag ? "AW:" : "Num-appends-with-wids: ") <<
        gAtomicRecordAppendManager.GetAppendersWithWidCount() << "\r\n" <<
    (shortRpcFormatFlag ? "RR:" : "Num-re-replications: ") <<
        Replicator::GetNumReplications() << "\r\n" <<
    (shortRpcFormatFlag ? "SX:1\r\n" : "Stale-chunks-hex-format: 1\r\n") <<
    (shortRpcFormatFlag ? "HD:" : "Num-hello-done: ") <<
        helloDoneCount << "\r\n" <<
    (shortRpcFormatFlag ? "NR:" : "Num-resume: ") <<
        helloResumeCount << "\r\n" <<
    (shortRpcFormatFlag ? "RF:" : "Num-resume-fail: ") <<
        helloResumeFailedCount << "\r\n" <<
    (shortRpcFormatFlag ? "IB:" : "Content-int-base: ") << 16 << "\r\n"
    ;
    if (pendingNotifyFlag) {
        os << (shortRpcFormatFlag ? "PN:1" : "Pending-notify: 1") << "\r\n";
    }
    if (reqShortRpcFmtFlag || shortRpcFormatFlag) {
        os << (shortRpcFormatFlag ? "f:1\r\n" : "Short-rpc-fmt: 1\r\n");
    }
    if (0 < chunkLists[kMissingList].count) {
        os << (shortRpcFormatFlag ? "CM:" : "Num-missing: ") <<
            chunkLists[kMissingList].count << "\r\n";
    }
    if (0 < chunkLists[kPendingStaleList].count) {
        os << (shortRpcFormatFlag ? "PS:" : "Num-stale: ") <<
            chunkLists[kPendingStaleList].count << "\r\n";
    }
    if (noFidsFlag) {
        os << (shortRpcFormatFlag ? "NF:1\r\n" : "NoFids: 1\r\n");
    }
    if (0 < fileSystemId) {
        os << (shortRpcFormatFlag ? "FI:" : "FsId: ") << fileSystemId << "\r\n";
    }
    if (sendCurrentKeyFlag) {
        SendCryptoKey(os, currentKeyId, currentKey, shortRpcFormatFlag);
    }
    if (0 <= resumeStep) {
        os << (shortRpcFormatFlag ? "R:" : "Resume: ") << resumeStep << "\r\n";
    }
    if (1 == resumeStep) {
        os <<
        (shortRpcFormatFlag ? "D:" : "Deleted: ")  << deletedCount  << "\r\n" <<
        (shortRpcFormatFlag ? "M:" : "Modified: ") << modifiedCount << "\r\n" <<
        (shortRpcFormatFlag ? "C:" : "Chunks: ")   << chunkCount    << "\r\n" <<
        (shortRpcFormatFlag ? "K:" : "Checksum: ") << checksum      << "\r\n"
        ;
    }
    if (0 <= channelId) {
        os << (shortRpcFormatFlag ? "CID:" : "ChannelId: ") <<
            channelId << "\r\n";
    }
    if (supportsResumeFlag) {
        os << (shortRpcFormatFlag ?  "SR:1" : "SupportsResume: 1") << "\r\n";
    }
    os << (shortRpcFormatFlag ? "TC:" : "Total-chunks: ") <<
        totalChunks << "\r\n";
    int64_t contentLength = 1;
    for (int i = 0; i < kChunkListCount; i++) {
        contentLength += chunkLists[i].ioBuf.BytesConsumable();
    }
    os << (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
        contentLength << "\r\n"
    "\r\n";
    os.flush();
    // Order matters. The meta server expects the lists to be in this order.
    const int kChunkListsOrder[kChunkListCount] = {
        kStableChunkList,
        kNotStableAppendChunkList,
        kNotStableChunkList,
        kMissingList,
        kPendingStaleList
    };
    for (int i = 0; i < kChunkListCount; i++) {
        buf.Move(&chunkLists[kChunkListsOrder[i]].ioBuf);
    }
    buf.CopyIn("\n", 1);
}

bool
HelloMetaOp::ParseResponseContent(istream& is, int len)
{
    resumeModified.clear();
    resumeDeleted.clear();
    if (status < 0) {
        return false;
    }
    if (len <= 0) {
        return (deletedCount <= 0 && modifiedCount <= 0);
    }
    const uint64_t kMinEntrySize = 2;
    if ((uint64_t)len / kMinEntrySize + (1 << 10) <
            deletedCount + modifiedCount) {
        statusMsg = "parse response: invalid chunk counts";
        status    = -EINVAL;
        return false;
    }
    kfsChunkId_t chunkId = -1;
    uint64_t     i;
    resumeDeleted.reserve(deletedCount);
    is >> hex;
    for (i = 0; i < deletedCount && (is >> chunkId) && 0 <= chunkId; i++) {
        resumeDeleted.push_back(chunkId);
    }
    if (i < deletedCount) {
        statusMsg = "parse response: invalid deleted count";
        status    = -EINVAL;
        resumeDeleted.clear();
        return false;
    }
    resumeModified.reserve(modifiedCount);
    for (i = 0; i < modifiedCount && (is >> chunkId) && 0 <= chunkId; i++) {
        resumeModified.push_back(chunkId);
    }
    if (i < modifiedCount) {
        statusMsg = "parse response: invalid modified count";
        status    = -EINVAL;
        resumeDeleted.clear();
        resumeModified.clear();
        return false;
    }
    return true;
}

void
SetProperties::Request(ReqOstream& os)
{
    string content;
    properties.getList(content, string());
    contentLength = content.length();
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os << "CMD_SET_PROPERTIES \r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << seq << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: " << KFS_VERSION_STR << "\r\n";
    }
    os << (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
        contentLength << "\r\n\r\n";
    os << content;
}

bool
SetProperties::ParseContent(istream& is)
{
    properties.clear();
    status = min(0, properties.loadProperties(is, '='));
    if (status != 0) {
        statusMsg = "failed to parse properties";
    }
    return (status == 0);
}

void
SetProperties::Execute()
{
    if (status == 0) {
        if (! MsgLogger::GetLogger()) {
            status    = -ENOENT;
            statusMsg = "no logger";
        } else {
            MsgLogger::GetLogger()->SetParameters(
                properties, "chunkServer.msgLogWriter.");
            gChunkServer.SetParameters(properties);
            gMetaServerSM.SetParameters(properties);
            gChunkManager.SetParameters(properties);
        }
    }
    Submit();
}

string RestartChunkServer();

void
RestartChunkServerOp::Execute()
{
    statusMsg = RestartChunkServer();
    status = statusMsg.empty() ? 0 : -1;
    Submit();
}

void
HelloMetaOp::Execute()
{
    if (0 <= resumeStep && ! gChunkManager.CanBeResumed(*this)) {
        resumeStep = -1;
    }
    status       = 0;
    totalFsSpace = 0;
    statusMsg.clear();
    lostChunkDirs.clear();
    IOBuffer::WOStream            streams[kChunkListCount];
    ChunkManager::HostedChunkList lists[kChunkListCount];
    for (int i = 0; i < kChunkListCount; i++) {
        chunkLists[i].count = 0;
        chunkLists[i].ioBuf.Clear();
        lists[i].first  = &(chunkLists[i].count);
        lists[i].second = &(streams[i].Set(chunkLists[i].ioBuf) << hex);
    }
    if (resumeStep < 0) {
        gChunkManager.GetHostedChunks(
            *this,
            lists[kStableChunkList],
            lists[kNotStableAppendChunkList],
            lists[kNotStableChunkList],
            noFidsFlag
        );
    } else {
        gChunkManager.GetHostedChunksResume(
            *this,
            lists[kStableChunkList],
            lists[kNotStableAppendChunkList],
            lists[kNotStableChunkList],
            lists[kMissingList],
            lists[kPendingStaleList],
            noFidsFlag
        );
        if (resumeStep < 0) {
            HelloMetaOp::Execute(); // Tail recursion.
            return;
        }
    }
    for (int i = 0; i < kChunkListCount; i++) {
        lists[i].second->flush();
        streams[i].Reset();
        if (chunkLists[i].count <= 0) {
            chunkLists[i].ioBuf.Clear();
        }
    }
    int     chunkDirs            = 0;
    int     numEvacuateInFlight  = 0;
    int     numWritableChunkDirs = 0;
    int     evacuateChunks       = 0;
    int64_t evacuateByteCount    = 0;
    totalSpace = gChunkManager.GetTotalSpace(
        totalFsSpace, chunkDirs, numEvacuateInFlight, numWritableChunkDirs,
        evacuateChunks, evacuateByteCount, 0, 0, &lostChunkDirs);
    usedSpace = gChunkManager.GetUsedSpace();
    ChunkManager::Counters cm;
    gChunkManager.GetCounters(cm);
    helloResumeCount       = cm.mHelloResumeCount;
    helloResumeFailedCount = cm.mHelloResumeFailedCount;
    sendCurrentKeyFlag = sendCurrentKeyFlag &&
        gChunkManager.GetCryptoKeys().GetCurrentKey(currentKeyId, currentKey);
    fileSystemId = gChunkManager.GetFileSystemId();
    totalChunks  = gChunkManager.GetNumChunks();
    Submit();
}

void
AuthenticateOp::Request(ReqOstream& os, IOBuffer& buf)
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os <<
    "AUTHENTICATE\r\n" <<
    (shortRpcFormatFlag ? "c:" : "Cseq: ") << seq << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: " << KFS_VERSION_STR << "\r\n";
        if (reqShortRpcFmtFlag) {
            os << "Short-rpc-fmt: 1\r\n";
        }
    }
    os << (shortRpcFormatFlag ? "A:" : "Auth-type: ") <<
        requestedAuthType << "\r\n"
    ;
    if (shortRpcFormatFlag && reqShortRpcFmtFlag) {
        os << "f:1\r\n";
    }
    if (0 < contentLength) {
        os << (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
            contentLength << "\r\n";
    }
    os << "\r\n";
    os.flush();
    if (0 < contentLength) {
        buf.CopyIn(reqBuf, contentLength);
    }
}

int
AuthenticateOp::ReadResponseContent(IOBuffer& iobuf)
{
    if (responseContentLength <= 0) {
        return 0;
    }
    if (! responseBuf) {
        responseBuf    = new char[responseContentLength];
        responseBufPos = 0;
    }
    const int len = iobuf.CopyOut(responseBuf + responseBufPos,
        responseContentLength - responseBufPos);
    if (0 < len) {
        iobuf.Consume(len);
        responseBufPos += len;
    }
    return (responseContentLength - responseBufPos);
}

}
