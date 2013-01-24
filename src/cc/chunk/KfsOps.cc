//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/05/26
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
// Code for parsing commands sent to the Chunkserver and generating
// responses that summarize the result of their execution.
//
//
//----------------------------------------------------------------------------

#include "KfsOps.h"
#include "common/Version.h"
#include "common/kfstypes.h"
#include "common/time.h"
#include "common/RequestParser.h"
#include "common/kfserrno.h"
#include "kfsio/Globals.h"
#include "kfsio/checksum.h"

#include "ChunkManager.h"
#include "Logger.h"
#include "ChunkServer.h"
#include "LeaseClerk.h"
#include "Replicator.h"
#include "AtomicRecordAppender.h"
#include "utils.h"

#include <algorithm>
#include <iomanip>
#include <iterator>
#include <stdlib.h>

#ifdef KFS_OS_NAME_SUNOS
#include <sys/loadavg.h>
#endif

namespace KFS {

using std::map;
using std::string;
using std::ofstream;
using std::ifstream;
using std::istringstream;
using std::ostringstream;
using std::istream;
using std::ostream;
using std::for_each;
using std::vector;
using std::min;
using std::make_pair;
using std::ostream_iterator;
using std::copy;
using std::hex;
using std::dec;
using std::max;
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
        instance.AddCounter("Open", CMD_OPEN);
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
}* OpCounters::sInstance(OpCounters::MakeInstance());

static bool
needToForwardToPeer(string &serverInfo, uint32_t numServers, int &myPos,
                    ServerLocation &peerLoc,
                    bool isWriteIdPresent, int64_t &writeId);

static inline RemoteSyncSMPtr
FindPeer(KfsOp& op, const ServerLocation& loc)
{
    ClientSM* const csm = op.GetClientSM();
    return (csm ? csm->FindServer(loc) : RemoteSyncSMPtr());
}

void
SubmitOp(KfsOp *op)
{
    op->type = OP_REQUEST;
    op->Execute();
}

void
SubmitOpResponse(KfsOp *op)
{
    op->type = OP_RESPONSE;
    op->HandleEvent(EVENT_CMD_DONE, op);
}

int64_t KfsOp::sOpsCount = 0;

KfsOp::~KfsOp()
{
    OpCounters::Update(op, startTime);
    assert(sOpsCount > 0);
    sOpsCount--;
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

typedef RequestHandler<KfsOp> ChunkRequestHandler;
static const ChunkRequestHandler&
MakeRequestHandler()
{
    static ChunkRequestHandler sHandler;
    return sHandler
    .MakeParser<OpenOp                  >("OPEN")
    .MakeParser<CloseOp                 >("CLOSE")
    .MakeParser<ReadOp                  >("READ")
    .MakeParser<WriteIdAllocOp          >("WRITE_ID_ALLOC")
    .MakeParser<WritePrepareOp          >("WRITE_PREPARE")
    .MakeParser<WriteSyncOp             >("WRITE_SYNC")
    .MakeParser<SizeOp                  >("SIZE")
    .MakeParser<RecordAppendOp          >("RECORD_APPEND")
    .MakeParser<GetRecordAppendOpStatus >("GET_RECORD_APPEND_OP_STATUS")
    .MakeParser<ChunkSpaceReserveOp     >("CHUNK_SPACE_RESERVE")
    .MakeParser<ChunkSpaceReleaseOp     >("CHUNK_SPACE_RELEASE")
    .MakeParser<GetChunkMetadataOp      >("GET_CHUNK_METADATA")
    .MakeParser<AllocChunkOp            >("ALLOCATE")
    .MakeParser<DeleteChunkOp           >("DELETE")
    .MakeParser<TruncateChunkOp         >("TRUNCATE")
    .MakeParser<ReplicateChunkOp        >("REPLICATE")
    .MakeParser<HeartbeatOp             >("HEARTBEAT")
    .MakeParser<StaleChunksOp           >("STALE_CHUNKS")
    .MakeParser<ChangeChunkVersOp       >("CHUNK_VERS_CHANGE")
    .MakeParser<BeginMakeChunkStableOp  >("BEGIN_MAKE_CHUNK_STABLE")
    .MakeParser<MakeChunkStableOp       >("MAKE_CHUNK_STABLE")
    .MakeParser<RetireOp                >("RETIRE")
    .MakeParser<PingOp                  >("PING")
    .MakeParser<DumpChunkMapOp          >("DUMP_CHUNKMAP")
    .MakeParser<StatsOp                 >("STATS")
    .MakeParser<SetProperties           >("CMD_SET_PROPERTIES")
    .MakeParser<RestartChunkServerOp    >("RESTART_CHUNK_SERVER")
    ;
}
static const ChunkRequestHandler& sRequestHandler = MakeRequestHandler();

///
/// Given a command in a buffer, parse it out and build a "Command"
/// structure which can then be executed.  For parsing, we take the
/// string representation of a command and build a Properties object
/// out of it; we can then pull the various headers in whatever order
/// we choose.
/// Commands are of the form:
/// <COMMAND NAME> \r\n
/// {header: value \r\n}+\r\n
///
///  The general model in parsing the client command:
/// 1. Each command has its own parser
/// 2. Extract out the command name and find the parser for that
/// command
/// 3. Dump the header/value pairs into a properties object, so that we
/// can extract the header/value fields in any order.
/// 4. Finally, call the parser for the command sent by the client.
///
/// @param[in] cmdBuf: buffer containing the request sent by the client
/// @param[in] cmdLen: length of cmdBuf
/// @param[out] res: A piece of memory allocated by calling new that
/// contains the data for the request.  It is the caller's
/// responsibility to delete the memory returned in res.
/// @retval 0 on success;  -1 if there is an error
///
int
ParseCommand(const IOBuffer& ioBuf, int len, KfsOp** res)
{
    // Main thread's buffer
    static char tempBuf[MAX_RPC_HEADER_LEN];

    *res = 0;
    if (len <= 0 || len > MAX_RPC_HEADER_LEN) {
        return -1;
    }
    // Copy if request header spans two or more buffers.
    // Requests on average are over a magnitude shorter than single
    // io buffer (4K page), thus the copy should be infrequent, and
    // small enough. With modern cpu the copy should be take less
    // cpu cycles than buffer boundary handling logic (or one symbol
    // per call processing), besides the request headers are small
    // enough to fit into cpu cache.
    int               reqLen = len;
    const char* const buf    = ioBuf.CopyOutOrGetBufPtr(tempBuf, reqLen);
    assert(reqLen == len);
    *res = reqLen == len ? sRequestHandler.Handle(buf, reqLen) : 0;
    return (*res ? 0 : -1);
}

ClientSM*
KfsOp::GetClientSM()
{
    return (clientSMFlag ? static_cast<ClientSM*>(clnt) : 0);
}

bool
WriteIdAllocOp::Validate()
{
    ValueParser::SetValue(
        clientSeqStr.GetPtr(),
        clientSeqStr.GetSize(),
        seq,
        clientSeq
    );
    return true;
}

bool
RecordAppendOp::Validate()
{
    ValueParser::SetValue(
        clientSeqStr.GetPtr(),
        clientSeqStr.GetSize(),
        seq,
        clientSeq
    );
    return true;
}

bool
WriteSyncOp::Validate()
{
    if (checksumsCnt <= 0) {
        return true;
    }
    const char*       ptr = checksumsStr.GetPtr();
    const char* const end = ptr + checksumsStr.GetSize();
    checksums.clear();
    checksums.reserve(checksumsCnt);
    for (int i = 0; i < checksumsCnt; i++) {
        uint32_t cksum = 0;
        if (! ValueParser::ParseInt(ptr, end - ptr, cksum)) {
            return false;
        }
        checksums.push_back(cksum);
        while (ptr < end && (*ptr & 0xFF) > ' ') {
            ++ptr;
        }
    }
    return true;
}

bool MakeChunkStableOp::Validate()
{
    hasChecksum = ! checksumStr.empty();
    if (hasChecksum) {
        ValueParser::SetValue(
            checksumStr.GetPtr(),
            checksumStr.GetSize(),
            uint32_t(0),
            chunkChecksum
        );
    }
    return true;
}

///
/// Generic event handler for tracking completion of an event
/// execution.  Push the op to the logger and the net thread will pick
/// it up and dispatch it.
///
int
KfsOp::HandleDone(int code, void *data)
{
    gLogger.Submit(this);
    return 0;
}

///
/// A read op finished.  Set the status and the # of bytes read
/// alongwith the data and notify the client.
///
int
ReadOp::HandleDone(int code, void *data)
{
    if (code == EVENT_DISK_ERROR) {
        status = -1;
        if (data) {
            status = *reinterpret_cast<const int*>(data);
            KFS_LOG_STREAM_INFO <<
                "disk error: errno: " << status << " chunkid: " << chunkId <<
            KFS_LOG_EOM;
        }
        if (status != -ETIMEDOUT) {
            gChunkManager.ChunkIOFailed(chunkId, status, diskIo.get());
        }
    } else if (code == EVENT_DISK_READ) {
        if (! dataBuf) {
            dataBuf = new IOBuffer();
        }
        IOBuffer* const b = reinterpret_cast<IOBuffer*>(data);
        // Order matters...when we append b, we take the data from b
        // and put it into our buffer.
        dataBuf->Append(b);
        // verify checksum
        if (! gChunkManager.ReadChunkDone(this)) {
            return 0; // Retry.
        }
        numBytesIO = dataBuf->BytesConsumable();
        if (status == 0) {
            // checksum verified
            status = numBytesIO;
        }
    }

    if (status >= 0) {
        assert(numBytesIO >= 0);
        if (offset % CHECKSUM_BLOCKSIZE != 0 ||
                numBytesIO % CHECKSUM_BLOCKSIZE != 0) {
            checksum = ComputeChecksums(dataBuf, numBytesIO);
        }
        assert(size_t((numBytesIO + CHECKSUM_BLOCKSIZE - 1) / CHECKSUM_BLOCKSIZE) ==
            checksum.size());
    }

    if (wop) {
        // if the read was triggered by a write, then resume execution of write
        wop->Execute();
        return 0;
    }

    const ChunkInfo_t* ci = gChunkManager.GetChunkInfo(chunkId);
    if (ci && ci->chunkSize > 0 && offset + numBytesIO >= ci->chunkSize &&
            ! gLeaseClerk.IsLeaseValid(chunkId)) {
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
        KFS_LOG_STREAM_INFO << "closing chunk: " << chunkId << KFS_LOG_EOM;
        gChunkManager.CloseChunk(chunkId);
    }

    gLogger.Submit(this);
    return 0;
}

int
ReadOp::HandleReplicatorDone(int code, void *data)
{
    if (status >= 0 && ! checksum.empty()) {
        const vector<uint32_t> datacksums = ComputeChecksums(dataBuf, numBytesIO);
        if (datacksums.size() > checksum.size()) {
                    KFS_LOG_STREAM_INFO <<
                        "Checksum number of entries mismatch in re-replication: "
                        " expect: " << datacksums.size() <<
                        " got: " << checksum.size() <<
                    KFS_LOG_EOM;
                    status = -EBADCKSUM;
        } else {
            for (uint32_t i = 0; i < datacksums.size(); i++) {
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
        }
    }
    // notify the replicator object that the read it had submitted to
    // the peer has finished.
    return clnt->HandleEvent(code, data);
}

int
WriteOp::HandleRecordAppendDone(int code, void *data)
{
    gChunkManager.WriteDone(this);
    if (code == EVENT_DISK_ERROR) {
        // eat up everything that was sent
        dataBuf->Consume(numBytes);
        status = -1;
        if (data) {
            status = *(int *) data;
            KFS_LOG_STREAM_INFO <<
                "Disk error: errno: " << status << " chunkid: " << chunkId <<
            KFS_LOG_EOM;
        }
    } else if (code == EVENT_DISK_WROTE) {
        status = *(int *) data;
        numBytesIO = status;
        dataBuf->Consume(numBytesIO);
    } else {
        die("unexpected event code");
    }
    return clnt->HandleEvent(EVENT_CMD_DONE, this);
}

int
ReadOp::HandleScrubReadDone(int code, void *data)
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
WriteOp::HandleWriteDone(int code, void *data)
{
    // DecrementCounter(CMD_WRITE);

    gChunkManager.WriteDone(this);
    if (isFromReReplication) {
        if (code == EVENT_DISK_WROTE) {
            status = min(*(int *) data, int(numBytes));
            numBytesIO = status;
        }
        else {
            status = -1;
        }
        return clnt->HandleEvent(code, this);
    }
    assert(wpop);

    if (code == EVENT_DISK_ERROR) {
        // eat up everything that was sent
        dataBuf->Consume(max(int(numBytesIO), int(numBytes)));
        status = -1;
        if (data) {
            status = *(int *) data;
            KFS_LOG_STREAM_INFO <<
                "Disk error: errno: " << status << " chunkid: " << chunkId <<
            KFS_LOG_EOM;
        }
        gChunkManager.ChunkIOFailed(chunkId, status, diskIo.get());

        if (wpop->status >= 0) {
            wpop->status = status;
        }
        wpop->HandleEvent(EVENT_CMD_DONE, this);
        return 0;
    }
    else if (code == EVENT_DISK_WROTE) {
        status = *(int *) data;
        if (numBytesIO != status || status < (int)numBytes) {
            // write didn't do everything that was asked; we need to retry
            KFS_LOG_STREAM_INFO <<
                "Write on chunk did less: asked: " << numBytes << "/" << numBytesIO <<
                " did: " << status << "; asking clnt to retry" <<
            KFS_LOG_EOM;
            status = -EAGAIN;
        } else {
            status = numBytes; // reply back the same # of bytes as in request.
        }
        if (numBytesIO > ssize_t(numBytes) && dataBuf) {
            const int off(offset % IOBufferData::GetDefaultBufferSize());
            KFS_LOG_STREAM_DEBUG <<
                "chunk write: asked " << numBytes << "/" << numBytesIO <<
                " actual, buf offset: " << off <<
            KFS_LOG_EOM;
            // restore original data in the buffer.
            assert(ssize_t(numBytes) <= numBytesIO - off);
            dataBuf->Consume(off);
            dataBuf->Trim(int(numBytes));
        }
        numBytesIO = numBytes;
        if (dataBuf) {
            // eat up everything that was sent
            dataBuf->Consume(numBytes);
        }
        if (status >= 0) {
            SET_HANDLER(this, &WriteOp::HandleLoggingDone);
            gLogger.Submit(this);
        } else {
            wpop->HandleEvent(EVENT_CMD_DONE, this);
        }
    }
    return 0;
}

int
WriteOp::HandleLoggingDone(int code, void *data)
{
    assert(wpop);
    return wpop->HandleEvent(EVENT_CMD_DONE, this);
}

///
/// Handlers for executing the various ops.  If the op execution is
/// "in-line", that is the op doesn't block, then when the execution
/// is finished, the op is handed off to the logger; the net thread
/// will drain the logger and then notify the client.  Otherwise, the op is queued
/// for execution and the client gets notified whenever the op
/// finishes execution.
///
void
OpenOp::Execute()
{
    status = gChunkManager.OpenChunk(chunkId, openFlags);
    gLogger.Submit(this);
}

void
CloseOp::Execute()
{
    KFS_LOG_STREAM_INFO <<
        "Closing chunk: " << chunkId << " and might give up lease" <<
    KFS_LOG_EOM;

    int            myPos   = -1;
    int64_t        writeId = -1;
    ServerLocation peerLoc;
    bool needToForward = needToForwardToPeer(
        servers, numServers, myPos, peerLoc, hasWriteId, writeId);
    if (! gAtomicRecordAppendManager.CloseChunk(
            this, writeId, needToForward)) {
        // forward the close only if it was accepted by the chunk
        // manager.  the chunk manager can reject a close if the
        // chunk is being written to by multiple record appenders
        needToForward = gChunkManager.CloseChunk(chunkId) == 0 && needToForward;
        status = 0;
    }
    if (needToForward) {
        ForwardToPeer(peerLoc);
    }
    gLogger.Submit(this);
}

void
CloseOp::ForwardToPeer(const ServerLocation& loc)
{
    RemoteSyncSMPtr const peer = FindPeer(*this, loc);
    if (! peer) {
        KFS_LOG_STREAM_DEBUG <<
            "unable to forward to peer: " << loc.ToString() <<
            " cmd: " << Show() <<
        KFS_LOG_EOM;
        return;
    }
    CloseOp* const fwdedOp = new CloseOp(0, this);
    // don't need an ack back
    fwdedOp->needAck = false;
    // this op goes to the remote-sync SM and after it is sent, comes right back to be nuked
    // when this op comes, just nuke it
    fwdedOp->clnt = fwdedOp;

    SET_HANDLER(fwdedOp, &CloseOp::HandlePeerReply);
    peer->Enqueue(fwdedOp);
}

int
CloseOp::HandlePeerReply(int code, void *data)
{
    delete this;
    return 0;
}

void
AllocChunkOp::Execute()
{
    int            myPos   = -1;
    int64_t        writeId = -1;
    ServerLocation peerLoc;
    needToForwardToPeer(servers, numServers, myPos, peerLoc, false, writeId);
    if (myPos < 0) {
        statusMsg = "invalid or missing Servers: field";
        status    = -EINVAL;
        gLogger.Submit(this);
        return;
    }

    // Allocation implicitly invalidates all previously existed write leases.
    gLeaseClerk.UnRegisterLease(chunkId);
    mustExistFlag = chunkVersion > 1;
    if (! mustExistFlag) {
        const int ret = gChunkManager.DeleteChunk(chunkId);
        if (ret != -EBADF) {
            KFS_LOG_STREAM_WARN <<
                "allocate: delete existing"
                " chunk: "  << chunkId <<
                " status: " << ret <<
            KFS_LOG_EOM;
        }
    }
    const bool failIfExistsFlag = ! mustExistFlag;
    // Check if chunk exists, if it does then load chunk meta data.
    SET_HANDLER(this, &AllocChunkOp::HandleChunkMetaReadDone);
    int res = gChunkManager.ReadChunkMetadata(chunkId, this);
    if (res == 0) {
        if (failIfExistsFlag) {
            die("chunk deletion failed");
        }
        return; // The completion handler will be or already invoked.
    }
    if (! mustExistFlag && res == -EBADF) {
        // Allocate new chunk.
        res = 0;
        HandleChunkAllocDone(EVENT_CMD_DONE, &res);
        return;
    }
    KFS_LOG_STREAM_ERROR <<
        "allocate: read chunk metadata:"
        " chunk: " << chunkId <<
        " error: " << res <<
    KFS_LOG_EOM;
    status = res;
    gLogger.Submit(this);
}

int
AllocChunkOp::HandleChunkMetaReadDone(int code, void* data)
{
    if (status < 0) {
        gLogger.Submit(this);
        return 0;
    } else if (data) {
        status = *reinterpret_cast<const int*>(data);
    }
    SET_HANDLER(this, &AllocChunkOp::HandleChunkAllocDone);
    // When version change is done the chunk must exist.
    // This is needed to detect chunk deletion while version version change is
    // in progress.
    // AllocChunk() does chunk version verification and other necessary checks
    // in the case if chunk exists.
    mustExistFlag = true;
    const bool stableFlag = false;
    const int ret = gChunkManager.ChangeChunkVers(
        chunkId, chunkVersion, stableFlag, this);
    if (ret < 0) {
        statusMsg = "change version failure";
        status = ret;
        gLogger.Submit(this);
    }
    return 0;
}

int
AllocChunkOp::HandleChunkAllocDone(int code, void *data)
{
    if (status >= 0 && code == EVENT_DISK_ERROR) {
        status = data ? *reinterpret_cast<const int*>(data) : -1;
    }
    if (status >= 0) {
        if (leaseId >= 0) {
            OpCounters::WriteMaster();
        }
        if (appendFlag) {
            int            myPos   = -1;
            int64_t        writeId = -1;
            ServerLocation peerLoc;
            needToForwardToPeer(servers, numServers, myPos, peerLoc, false, writeId);
            assert(myPos >= 0);
            gChunkManager.AllocChunkForAppend(this, myPos, peerLoc);
        } else {
            bool beingReplicatedFlag = false;
            status = gChunkManager.AllocChunk(fileId, chunkId, chunkVersion,
                beingReplicatedFlag, 0, mustExistFlag);
        }
        if (status >= 0 && leaseId >= 0) {
            gLeaseClerk.RegisterLease(chunkId, leaseId, appendFlag);
        }
    }
    gLogger.Submit(this);
    return 0;
}

void
DeleteChunkOp::Execute()
{
    status = gChunkManager.DeleteChunk(chunkId);
    gLogger.Submit(this);
}

void
TruncateChunkOp::Execute()
{
    SET_HANDLER(this, &TruncateChunkOp::HandleChunkMetaReadDone);
    if (gChunkManager.ReadChunkMetadata(chunkId, this) < 0) {
        status = -EINVAL;
        gLogger.Submit(this);
    }
}

int
TruncateChunkOp::HandleChunkMetaReadDone(int code, void *data)
{
    if (status >= 0 && data) {
        status = *(int *) data;
    }
    if (status < 0) {
        gLogger.Submit(this);
        return 0;
    }

    status = gChunkManager.TruncateChunk(chunkId, chunkSize);
    if (status < 0) {
        gLogger.Submit(this);
        return 0;
    }
    SET_HANDLER(this, &TruncateChunkOp::HandleChunkMetaWriteDone);
    const int ret = gChunkManager.WriteChunkMetadata(chunkId, this);
    if (ret != 0) {
        status = ret;
        gLogger.Submit(this);
    }
    return 0;
}

int
TruncateChunkOp::HandleChunkMetaWriteDone(int code, void* data)
{
    int res = data ? *reinterpret_cast<const int*>(data) : -1;
    if (res < 0) {
        status = res;
    }
    gLogger.Submit(this);
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
    gLogger.Submit(this);
}

void
MakeChunkStableOp::Execute()
{
    status = 0;
    if (gChunkManager.IsChunkStable(this)) {
        gLogger.Submit(this);
        return;
    }
    SET_HANDLER(this, &MakeChunkStableOp::HandleChunkMetaReadDone);
    const int ret = gChunkManager.ReadChunkMetadata(chunkId, this);
    if (ret < 0) {
        status = ret;
        gLogger.Submit(this);
    }
}

int
MakeChunkStableOp::HandleChunkMetaReadDone(int code, void *data)
{
    if (status >= 0 && data) {
        status = *reinterpret_cast<const int*>(data);
    }
    if (status < 0) {
        gLogger.Submit(this);
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
MakeChunkStableOp::HandleMakeStableDone(int code, void *data)
{
    if (code == EVENT_DISK_ERROR && status == 0) {
        const int res = data ? *reinterpret_cast<const int*>(data) : -1;
        status = res < 0 ? res : -1;
    }
    if (status >= 0 &&
            ! gLeaseClerk.IsLeaseValid(chunkId) &&
            gChunkManager.CloseChunkIfReadable(chunkId)) {
        KFS_LOG_STREAM_DEBUG <<
            Show() << " done, chunk closed" <<
        KFS_LOG_EOM;
    }
    gLogger.Submit(this);
    return 0;
}

void
ChangeChunkVersOp::Execute()
{
    SET_HANDLER(this, &ChangeChunkVersOp::HandleChunkMetaReadDone);
    const int ret = gChunkManager.ReadChunkMetadata(chunkId, this);
    if (ret < 0) {
        status = -EINVAL;
        gLogger.Submit(this);
    }
}

int
ChangeChunkVersOp::HandleChunkMetaReadDone(int code, void *data)
{
    if (status >= 0 && data) {
        status = *(int *) data;
    }
    if (status < 0) {
        gLogger.Submit(this);
        return 0;
    }
    SET_HANDLER(this, &ChangeChunkVersOp::HandleChunkMetaWriteDone);
    if (gChunkManager.ChangeChunkVers(this) < 0) {
        gLogger.Submit(this);
    }
    return 0;
}

int
ChangeChunkVersOp::HandleChunkMetaWriteDone(int code, void* data)
{
    const int res = data ? *reinterpret_cast<const int*>(data) : -1;
    if (res < 0) {
        status = res;
    }
    gLogger.Submit(this);
    return 0;
}

template<typename T> void
HeartbeatOp::Append(const char* key1, const char* key2, T val)
{
    if (key1 && *key1) {
        response << key1 << ": " << val << "\r\n";
    }
    if (key2 && *key2) {
        cmdShow  << " " << key2 << ": " << val;
    }
}

// This is the heartbeat sent by the meta server
void
HeartbeatOp::Execute()
{
    double loadavg[3] = {-1, -1, -1};
#ifndef KFS_OS_NAME_CYGWIN
    getloadavg(loadavg, 3);
#endif
    gChunkManager.MetaHeartbeat(*this);

    const int64_t writeCount       = gChunkManager.GetNumWritableChunks();
    const int64_t writeAppendCount = gAtomicRecordAppendManager.GetOpenAppendersCount();
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
    cmdShow << " space:";
    Append("Total-space",    "total",  gChunkManager.GetTotalSpace(
        totalFsSpace, chunkDirs, evacuateInFlightCount, writableDirs,
        evacuateChunks, evacuateByteCount,
        &evacuateDoneChunkCount, &evacuateDoneByteCount));
    Append("Total-fs-space", "tfs",      totalFsSpace);
    Append("Used-space",     "used",     gChunkManager.GetUsedSpace());
    Append("Num-drives",     "drives",   chunkDirs);
    Append("Num-wr-drives",  "wr-drv",   writableDirs);
    Append("Num-chunks",     "chunks",   gChunkManager.GetNumChunks());
    Append("Num-writable-chunks", "wrchunks",
        writeCount + writeAppendCount + replicationCount
    );
    Append("Evacuate",              "evacuate",
        max(evacuateChunks, evacuateInFlightCount));
    Append("Evacuate-bytes",        "evac-b",   evacuateByteCount);
    Append("Evacuate-done",         "evac-d",   evacuateDoneChunkCount);
    Append("Evacuate-done-bytes",   "evac-d-b", evacuateDoneByteCount);
    Append("Evacuate-in-flight",    "evac-fl",  evacuateInFlightCount);
    Append("Num-random-writes",     "rwr",  writeCount);
    Append("Num-appends",           "awr",  writeAppendCount);
    Append("Num-re-replications",   "rep",  replicationCount);
    Append("Num-appends-with-wids", "awid",
        gAtomicRecordAppendManager.GetAppendersWithWidCount());
    Append("Uptime", "up", globalNetManager().UpTime());

    Append("CPU-user", "ucpu", utime);
    Append("CPU-sys",  "scpu", stime);
    Append("CPU-load-avg",             "load", loadavg[0]);

    ChunkManager::Counters cm;
    gChunkManager.GetCounters(cm);
    cmdShow << " chunk: err:";
    Append("Chunk-corrupted",     "cor",  cm.mCorruptedChunksCount);
    Append("Chunk-lost",          "lost", cm.mLostChunksCount);
    Append("Chunk-header-errors", "hdr",  cm.mBadChunkHeaderErrorCount);
    Append("Chunk-chksum-errors", "csum", cm.mReadChecksumErrorCount);
    Append("Chunk-read-errors",   "rd",   cm.mReadErrorCount);
    Append("Chunk-write-errors",  "wr",   cm.mWriteErrorCount);
    Append("Chunk-open-errors",   "open", cm.mOpenErrorCount);
    Append("Dir-chunk-lost",      "dce",  cm.mDirLostChunkCount);
    Append("Chunk-dir-lost",      "cdl",  cm.mChunkDirLostCount);

    MetaServerSM::Counters mc;
    gMetaServerSM.GetCounters(mc);
    cmdShow << " meta:";
    Append("Meta-connect",     "conn", mc.mConnectCount);
    cmdShow << " hello:";
    Append("Meta-hello-count",  "cnt", mc.mHelloCount);
    Append("Meta-hello-errors", "err", mc.mHelloErrorCount);
    cmdShow << " alloc:";
    Append("Meta-alloc-count",  "cnt", mc.mAllocCount);
    Append("Meta-alloc-errors", "err", mc.mAllocErrorCount);

    ClientManager::Counters cli;
    gClientManager.GetCounters(cli);
    cmdShow << " cli:";
    Append("Client-accept",  "accept", cli.mAcceptCount);
    Append("Client-active",  "cur",    cli.mClientCount);
    cmdShow << " req: err:";
    Append("Client-req-invalid",        "inval", cli.mBadRequestCount);
    Append("Client-req-invalid-header", "hdr",   cli.mBadRequestHeaderCount);
    Append("Client-req-invalid-length", "len",
        cli.mRequestLengthExceededCount);
    cmdShow << " read:";
    Append("Client-read-count",     "cnt",   cli.mReadRequestCount);
    Append("Client-read-bytes",     "bytes", cli.mReadRequestBytes);
    Append("Client-read-micro-sec", "tm",    cli.mReadRequestTimeMicroSecs);
    Append("Client-read-errors",    "err",   cli.mReadRequestErrors);
    cmdShow << " write:";
    Append("Client-write-count",     "cnt",   cli.mWriteRequestCount);
    Append("Client-write-bytes",     "bytes", cli.mWriteRequestBytes);
    Append("Client-write-micro-sec", "tm",    cli.mWriteRequestTimeMicroSecs);
    Append("Client-write-errors",    "err",   cli.mWriteRequestErrors);
    cmdShow << " append:";
    Append("Client-append-count",     "cnt",   cli.mAppendRequestCount);
    Append("Client-append-bytes",     "bytes", cli.mAppendRequestBytes);
    Append("Client-append-micro-sec", "tm",    cli.mAppendRequestTimeMicroSecs);
    Append("Client-append-errors",    "err",   cli.mAppendRequestErrors);
    cmdShow << " other:";
    Append("Client-other-count",     "cnt",   cli.mOtherRequestCount);
    Append("Client-other-micro-sec", "tm",    cli.mOtherRequestTimeMicroSecs);
    Append("Client-other-errors",    "err",   cli.mOtherRequestErrors);

    cmdShow << " timer: ovr:";
    Append("Timer-overrun-count", "cnt",
        globalNetManager().GetTimerOverrunCount());
    Append("Timer-overrun-sec",   "sec",
        globalNetManager().GetTimerOverrunSec());

    cmdShow << " wappend:";
    Append("Write-appenders", "cur",
        gAtomicRecordAppendManager.GetAppendersCount());
    AtomicRecordAppendManager::Counters wa;
    gAtomicRecordAppendManager.GetCounters(wa);
    Append("WAppend-count", "cnt",   wa.mAppendCount);
    Append("WAppend-bytes", "bytes", wa.mAppendByteCount);
    Append("WAppend-errors","err",   wa.mAppendErrorCount);
    cmdShow << " repl:";
    Append("WAppend-replication-errors",   "err", wa.mReplicationErrorCount);
    Append("WAppend-replication-tiemouts", "tmo", wa.mReplicationTimeoutCount);
    cmdShow << " alloc:";
    Append("WAppend-alloc-count",        "cnt", wa.mAppenderAllocCount);
    Append("WAppend-alloc-master-count", "mas", wa.mAppenderAllocMasterCount);
    Append("WAppend-alloc-errors",       "err", wa.mAppenderAllocErrorCount);
    cmdShow << " wid:";
    Append("WAppend-wid-alloc-count",      "cnt", wa.mWriteIdAllocCount);
    Append("WAppend-wid-alloc-errors",     "err", wa.mWriteIdAllocErrorCount);
    Append("WAppend-wid-alloc-no-appender","nae", wa.mWriteIdAllocNoAppenderCount);
    cmdShow << " srsrv:";
    Append("WAppend-sreserve-count",  "cnt",   wa.mSpaceReserveCount);
    Append("WAppend-sreserve-bytes",  "bytes", wa.mSpaceReserveByteCount);
    Append("WAppend-sreserve-errors", "err",   wa.mSpaceReserveErrorCount);
    Append("WAppend-sreserve-denied", "den",   wa.mSpaceReserveDeniedCount);
    cmdShow << " bmcs:";
    Append("WAppend-bmcs-count",  "cnt", wa.mBeginMakeStableCount);
    Append("WAppend-bmcs-errors", "err", wa.mBeginMakeStableErrorCount);
    cmdShow << " mcs:";
    Append("WAppend-mcs-count",         "cnt", wa.mMakeStableCount);
    Append("WAppend-mcs-errors",        "err", wa.mMakeStableErrorCount);
    Append("WAppend-mcs-length-errors", "eln", wa.mMakeStableLengthErrorCount);
    Append("WAppend-mcs-chksum-errors", "ecs", wa.mMakeStableChecksumErrorCount);
    cmdShow << " gos:";
    Append("WAppend-get-op-status-count", "cnt", wa.mGetOpStatusCount);
    Append("WAppend-get-op-status-errors","err", wa.mGetOpStatusErrorCount);
    Append("WAppend-get-op-status-known", "knw", wa.mGetOpStatusKnownCount);
    cmdShow << " err:";
    Append("WAppend-chksum-erros",    "csum",  wa.mChecksumErrorCount);
    Append("WAppend-read-erros",      "rd",    wa.mReadErrorCount);
    Append("WAppend-write-errors",    "wr",    wa.mWriteErrorCount);
    Append("WAppend-lease-ex-errors", "lease", wa.mLeaseExpiredCount);
    cmdShow << " lost:";
    Append("WAppend-lost-timeouts", "tm",   wa.mTimeoutLostCount);
    Append("WAppend-lost-chunks",   "csum", wa.mLostChunkCount);

    const BufferManager&  bufMgr = DiskIo::GetBufferManager();
    cmdShow <<  " buffers: bytes:";
    Append("Buffer-bytes-total",      "total", bufMgr.GetTotalByteCount());
    Append("Buffer-bytes-wait",       "wait",  bufMgr.GetWaitingByteCount());
    Append("Buffer-bytes-wait-avg",   "wavg",  bufMgr.GetWaitingAvgBytes());
    Append("Buffer-usec-wait-avg",    "uavg",  bufMgr.GetWaitingAvgUsecs());
    Append("Buffer-clients-wait-avg", "cavg",  bufMgr.GetWaitingAvgCount());
    cmdShow << " cnt:";
    Append("Buffer-total-count", "total", bufMgr.GetTotalBufferCount());
    Append("Buffer-min-count",   "min",   bufMgr.GetMinBufferCount());
    Append("Buffer-free-count",  "free",  bufMgr.GetFreeBufferCount());
    cmdShow << " req:";
    Append("Buffer-clients",      "cbuf",  bufMgr.GetClientsWihtBuffersCount());
    Append("Buffer-clients-wait", "cwait", bufMgr.GetWaitingCount());
    BufferManager::Counters bmCnts;
    bufMgr.GetCounters(bmCnts);
    Append("Buffer-req-total",         "cnt",   bmCnts.mRequestCount);
    Append("Buffer-req-bytes",         "bytes", bmCnts.mRequestByteCount);
    Append("Buffer-req-denied-total",  "den",   bmCnts.mRequestDeniedCount);
    Append("Buffer-req-denied-bytes",  "denb",  bmCnts.mRequestDeniedByteCount);
    Append("Buffer-req-granted-total", "grn",   bmCnts.mRequestGrantedCount);
    Append("Buffer-req-granted-bytes", "grnb",  bmCnts.mRequestGrantedByteCount);
    Append("Buffer-req-wait-usec",     "rwu",   bmCnts.mRequestWaitUsecs);

    DiskIo::Counters dio;
    DiskIo::GetCounters(dio);
    cmdShow <<  " disk: read:";
    Append("Disk-read-count", "cnt",   dio.mReadCount);
    Append("Disk-read-bytes", "bytes", dio.mReadByteCount);
    Append("Disk-read-errors","err",   dio.mReadErrorCount);
    cmdShow <<  " write:";
    Append("Disk-write-count", "cnt",   dio.mWriteCount);
    Append("Disk-write-bytes", "bytes", dio.mWriteByteCount);
    Append("Disk-write-errors","err",   dio.mWriteErrorCount);
    cmdShow <<  " sync:";
    Append("Disk-sync-count", "cnt",   dio.mSyncCount);
    Append("Disk-sync-errors","err",   dio.mSyncErrorCount);
    cmdShow <<  " del:";
    Append("Disk-delete-count", "cnt",   dio.mDeleteCount);
    Append("Disk-delete-errors","err",   dio.mDeleteErrorCount);
    cmdShow <<  " rnm:";
    Append("Disk-rename-count", "cnt",   dio.mRenameCount);
    Append("Disk-rename-errors","err",   dio.mRenameErrorCount);
    cmdShow <<  " fsavl:";
    Append("Disk-fs-get-free-count", "cnt",  dio.mGetFsSpaceAvailableCount);
    Append("Disk-fs-get-free-errors","err",  dio.mGetFsSpaceAvailableErrorCount);
    cmdShow <<  " dirchk:";
    Append("Disk-dir-readable-count", "cnt", dio.mCheckDirReadableCount);
    Append("Disk-dir-readable-errors","err", dio.mCheckDirReadableErrorCount);
    cmdShow <<  " timedout:";
    Append("Disk-timedout-count",      "cnt",    dio.mTimedOutErrorCount);
    Append("Disk-timedout-read-bytes", "rbytes", dio.mTimedOutErrorReadByteCount);
    Append("Disk-timedout-write-bytes","wbytes", dio.mTimedOutErrorWriteByteCount);
    Append("Disk-open-files",          "fopen",  dio.mOpenFilesCount);

    cmdShow << " msglog:";
    MsgLogger::Counters msgLogCntrs;
    MsgLogger::GetLogger()->GetCounters(msgLogCntrs);
    Append("Msg-log-level",            "level",  MsgLogger::GetLogger()->GetLogLevel());
    Append("Msg-log-count",            "cnt",    msgLogCntrs.mAppendCount);
    Append("Msg-log-drop",             "drop",   msgLogCntrs.mDroppedCount);
    Append("Msg-log-write-errors",     "werr",   msgLogCntrs.mWriteErrorCount);
    Append("Msg-log-wait",             "wait",   msgLogCntrs.mAppendWaitCount);
    Append("Msg-log-waited-micro-sec", "waittm", msgLogCntrs.mAppendWaitMicroSecs);

    cmdShow << " repl:";
    Replicator::Counters replCntrs;
    Replicator::GetCounters(replCntrs);
    Append("Replication-count",  "cnt",    replCntrs.mReplicationCount);
    Append("Replication-errors", "err",    replCntrs.mReplicationErrorCount);
    Append("Replication-cancel", "cancel", replCntrs.mReplicationCanceledCount);
    Append("Replicator-count",   "obj",    replCntrs.mReplicatorCount);
    cmdShow << " recov:";
    Append("Recovery-count",  "cnt",    replCntrs.mRecoveryCount);
    Append("Recovery-errors", "err",    replCntrs.mRecoveryErrorCount);
    Append("Recovery-cancel", "cancel", replCntrs.mRecoveryCanceledCount);

    Append("Ops-in-flight-count", "opsf", gChunkServer.GetNumOps());
    cmdShow << " gcntrs:";
    Append("Socket-count",    "socks", globals().ctrOpenNetFds.GetValue());
    Append("Disk-fd-count",   "dfds",  globals().ctrOpenDiskFds.GetValue());
    Append("Net-bytes-read",   "nrd",  globals().ctrNetBytesRead.GetValue());
    Append("Net-bytes-write",  "nwr",  globals().ctrNetBytesWritten.GetValue());
    Append("Disk-bytes-read",  "drd",  globals().ctrDiskBytesRead.GetValue());
    Append("Disk-bytes-write", "dwr",  globals().ctrDiskBytesWritten.GetValue());
    Append("Total-ops-count",  "ops",  KfsOp::GetOpsCount());

    status = 0;
    gLogger.Submit(this);
}

void
RetireOp::Execute()
{
    // we are told to retire...so, bow out
    KFS_LOG_STREAM_INFO << "we have been asked to retire, bye" << KFS_LOG_EOM;
    globalNetManager().Shutdown();
}

bool
StaleChunksOp::ParseContent(istream& is)
{
    if (status != 0) {
        return false;
    }
    kfsChunkId_t c;
    staleChunkIds.reserve(numStaleChunks);
    const istream::fmtflags isFlags = is.flags();
    if (hexFormatFlag) {
        is >> hex;
    }
    for(int i = 0; i < numStaleChunks; ++i) {
        if (! (is >> c)) {
            ostringstream os;
            os <<
                "failed to parse stale chunks request:"
                " expected: "   << numStaleChunks <<
                " got: "        << i <<
                " last chunk: " << c
            ;
            statusMsg = os.str();
            status = -EINVAL;
            break;
        }
        staleChunkIds.push_back(c);
    }
    is.flags(isFlags);
    return (status == 0);
}

void
StaleChunksOp::Execute()
{
    status = 0;
    const bool forceDeleteFlag = true;
    for (StaleChunkIds::const_iterator it = staleChunkIds.begin();
            it != staleChunkIds.end();
            ++it) {
        gChunkManager.StaleChunk(*it, forceDeleteFlag, evacuatedFlag);
    }
    KFS_LOG_STREAM_INFO << "stale chunks: " <<
        (staleChunkIds.empty() ? kfsChunkId_t(-1) : staleChunkIds.front()) <<
        " count: " << staleChunkIds.size() <<
    KFS_LOG_EOM;
    gLogger.Submit(this);
}

void
ReadOp::Execute()
{
    if (numBytes > CHUNKSIZE) {
        KFS_LOG_STREAM_DEBUG <<
            "read request size exceeds chunk size: " << numBytes <<
        KFS_LOG_EOM;
        status = -EINVAL;
        gLogger.Submit(this);
        return;
    }

    SET_HANDLER(this, &ReadOp::HandleChunkMetaReadDone);
    const int res = gChunkManager.ReadChunkMetadata(chunkId, this);
    if (res < 0) {
        KFS_LOG_STREAM_ERROR <<
            "failed read chunk meta data, status: " << res <<
        KFS_LOG_EOM;
        status = res;
        gLogger.Submit(this);
    }
}

int
ReadOp::HandleChunkMetaReadDone(int code, void *data)
{
    if (status >= 0 && data) {
        status = *(int *) data;
    }
    if (status < 0) {
        gLogger.Submit(this);
        return 0;
    }

    SET_HANDLER(this, &ReadOp::HandleDone);
    status = gChunkManager.ReadChunk(this);

    if (status < 0) {
        // clnt->HandleEvent(EVENT_CMD_DONE, this);
        if (! wop) {
            // we are done with this op; this needs draining
            gLogger.Submit(this);
        } else {
            // resume execution of write
            wop->Execute();
        }
    }
    return 0;
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

static bool
needToForwardToPeer(string &serverInfo, uint32_t numServers, int &myPos,
                    ServerLocation &peerLoc,
                    bool isWriteIdPresent, int64_t &writeId)
{
    istringstream ist(serverInfo);
    ServerLocation loc;
    bool foundLocal = false;
    int64_t id;
    bool needToForward = false;

    // the list of servers is ordered: we forward to the next one
    // in the list.
    for (uint32_t i = 0; i < numServers; i++) {
        ist >> loc.hostname;
        ist >> loc.port;
        if (isWriteIdPresent)
            ist >> id;

        if (gChunkServer.IsLocalServer(loc)) {
            // return the position of where this server is present in the list
            myPos = i;
            foundLocal = true;
            if (isWriteIdPresent)
                writeId = id;
            continue;
        }
        // forward if we are not the last in the list
        if (foundLocal) {
            needToForward = true;
            break;
        }
    }
    peerLoc = loc;
    return needToForward;
}

void
WriteIdAllocOp::Execute()
{
    // check if we need to forward anywhere
    writeId = -1;
    int64_t        dummyWriteId  = -1;
    int            myPos         = -1;
    ServerLocation peerLoc;
    const bool     needToForward = needToForwardToPeer(
        servers, numServers, myPos, peerLoc, false, dummyWriteId);
    if (myPos < 0) {
        statusMsg = "invalid or missing Servers: field";
        status    = -EINVAL;
        gLogger.Submit(this);
        return;
    }
    const bool writeMaster = myPos == 0;
    if (writeMaster && ! gLeaseClerk.IsLeaseValid(chunkId)) {
        status    = -ELEASEEXPIRED;
        statusMsg = "no valid write lease exists";
        Done(EVENT_CMD_DONE, &status);
        return;
    }
    const int res = gChunkManager.AllocateWriteId(this, myPos, peerLoc);
    if (res != 0 && status == 0) {
        status = res < 0 ? res : -res;
    }
    if (status != 0) {
        Done(EVENT_CMD_DONE, &status);
        return;
    }
    if (writeMaster) {
        // Notify the lease clerk that we are doing write.  This is to
        // signal the lease clerk to renew the lease for the chunk when appropriate.
        gLeaseClerk.DoingWrite(chunkId);
    }
    ostringstream os;
    os << gChunkServer.GetMyLocation() << " " << writeId;
    writeIdStr = os.str();
    if (needToForward) {
        ForwardToPeer(peerLoc);
    } else {
        ReadChunkMetadata();
    }
}

int
WriteIdAllocOp::ForwardToPeer(const ServerLocation& loc)
{
    assert(! fwdedOp && status == 0 && (clnt || isForRecordAppend));

    RemoteSyncSMPtr const peer = isForRecordAppend ?
        appendPeer : FindPeer(*this, loc);
    if (! peer) {
        status    = -EHOSTUNREACH;
        statusMsg = "unable to find peer " + loc.ToString();
        return Done(EVENT_CMD_DONE, &status);
    }
    fwdedOp = new WriteIdAllocOp(0, *this);
    fwdedOp->writePrepareReplyFlag = false; // set by the next one in the chain.
    // When forwarded op completes, call this op HandlePeerReply.
    fwdedOp->clnt = this;
    SET_HANDLER(this, &WriteIdAllocOp::HandlePeerReply);

    peer->Enqueue(fwdedOp);
    return 0;
}

int
WriteIdAllocOp::HandlePeerReply(int code, void *data)
{
    assert(code == EVENT_CMD_DONE && data == fwdedOp);

    if (status == 0 && fwdedOp->status < 0) {
        status    = fwdedOp->status;
        statusMsg = fwdedOp->statusMsg.empty() ?
            string("forwarding failed") : fwdedOp->statusMsg;
    }
    if (status != 0) {
        return Done(EVENT_CMD_DONE, &status);
    }
    writeIdStr += " " + fwdedOp->writeIdStr;
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
    int res = gChunkManager.ReadChunkMetadata(chunkId, this);
    if (res < 0) {
        Done(EVENT_CMD_DONE, &res);
    }
}

int
WriteIdAllocOp::Done(int code, void *data)
{
    if (status == 0) {
        status = (code == EVENT_CMD_DONE && data) ?
            *reinterpret_cast<const int*>(data) : -1;
        if (status != 0) {
            statusMsg = "chunk meta data read failed";
        }
    }
    if (status != 0) {
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
            gLeaseClerk.InvalidateLease(chunkId);
        }
    }
    KFS_LOG_STREAM(
        status == 0 ? MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
        (status == 0 ? "done: " : "failed: ") << Show() <<
    KFS_LOG_EOM;
    gLogger.Submit(this);
    return 0;
}

void
WritePrepareOp::Execute()
{
    ServerLocation peerLoc;
    int myPos = -1;

    SET_HANDLER(this, &WritePrepareOp::Done);

    // check if we need to forward anywhere
    bool needToForward = false, writeMaster;

    needToForward = needToForwardToPeer(servers, numServers, myPos, peerLoc, true, writeId);
    if (myPos < 0) {
        statusMsg = "invalid or missing Servers: field";
        status = -EINVAL;
        gLogger.Submit(this);
        return;
    }
    writeMaster = (myPos == 0);

    if (! gChunkManager.IsValidWriteId(writeId)) {
        statusMsg = "invalid write id";
        status = -EINVAL;
        gLogger.Submit(this);
        return;
    }

    if (!gChunkManager.IsChunkMetadataLoaded(chunkId)) {
        statusMsg = "checksums are not loaded";
        status = -ELEASEEXPIRED;
        Done(EVENT_CMD_DONE, this);
        return;
    }

    if (writeMaster) {
        // if we are the master, check the lease...
        if (! gLeaseClerk.IsLeaseValid(chunkId)) {
            KFS_LOG_STREAM_ERROR <<
                "Write prepare failed, lease expired for " << chunkId <<
            KFS_LOG_EOM;
            statusMsg = "no valid write lease exists";
            gLeaseClerk.InvalidateLease(chunkId);
            status = -ELEASEEXPIRED;
            Done(EVENT_CMD_DONE, this);
            return;
        }
        // Notify the lease clerk that we are doing write.  This is to
        // signal the lease clerk to renew the lease for the chunk when appropriate.
        gLeaseClerk.DoingWrite(chunkId);
    }

    uint32_t         val       = 0;
    vector<uint32_t> checksums = ComputeChecksums(dataBuf, numBytes, &val);
    if (val != checksum) {
        statusMsg = "checksum mismatch";
        KFS_LOG_STREAM_ERROR <<
            "checksum mismatch: sent: " << checksum <<
            ", computed: " << val << " for " << Show() <<
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
        status = ForwardToPeer(peerLoc);
        if (status < 0) {
            // can't forward to peer...so fail the write
            Done(EVENT_CMD_DONE, this);
            return;
        }
    }

    writeOp->offset = offset;
    writeOp->numBytes = numBytes;
    writeOp->dataBuf = dataBuf;
    writeOp->wpop = this;
    writeOp->checksums.swap(checksums);
    dataBuf = 0;

    writeOp->enqueueTime = globalNetManager().Now();

    KFS_LOG_STREAM_DEBUG <<
        "Writing to chunk: " << chunkId <<
        " @offset: " << offset <<
        " nbytes: " << numBytes <<
        " checksum: " << checksum <<
    KFS_LOG_EOM;

    status = gChunkManager.WriteChunk(writeOp);
    if (status < 0) {
        Done(EVENT_CMD_DONE, this);
    }
}

int
WritePrepareOp::ForwardToPeer(const ServerLocation& loc)
{
    assert(clnt);
    RemoteSyncSMPtr const peer = FindPeer(*this, loc);
    if (!peer) {
        statusMsg = "no such peer " + loc.ToString();
        return -EHOSTUNREACH;
    }
    writeFwdOp = new WritePrepareFwdOp(*this);
    writeFwdOp->clnt = this;
    peer->Enqueue(writeFwdOp);
    return 0;
}

int
WritePrepareOp::Done(int code, void *data)
{
    if (status >= 0 && writeFwdOp && writeFwdOp->status < 0) {
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
        gLeaseClerk.InvalidateLease(chunkId);
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
    gLogger.Submit(this);
    return 0;
}

void
WriteSyncOp::Execute()
{
    ServerLocation peerLoc;
    int            myPos = -1;

    KFS_LOG_STREAM_DEBUG << "executing: " << Show() << KFS_LOG_EOM;
    // check if we need to forward anywhere
    const bool needToForward = needToForwardToPeer(servers, numServers, myPos, peerLoc, true, writeId);
    if (myPos < 0) {
        statusMsg = "invalid or missing Servers: field";
        status = -EINVAL;
        gLogger.Submit(this);
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
        gLogger.Submit(this);
        return;
    }

    writeOp->enqueueTime = globalNetManager().Now();

    if (writeOp->status < 0) {
        // due to failures with data forwarding/checksum errors and such
        status    = writeOp->status;
        statusMsg = "write error";
        gLogger.Submit(this);
        return;
    }

    if (! gChunkManager.IsChunkMetadataLoaded(chunkId)) {
        // This should not normally happen, as valid write id would keep chunk
        // loaded / writable.
        status    = -ELEASEEXPIRED;
        statusMsg = "meta data unloaded";
        KFS_LOG_STREAM_ERROR <<
            "failed: " << statusMsg << " " << Show() <<
        KFS_LOG_EOM;
        gChunkManager.SetWriteStatus(writeId, status);
        gLogger.Submit(this);
        return;
    }

    if (writeMaster) {
        // if we are the master, check the lease...
        if (! gLeaseClerk.IsLeaseValid(chunkId)) {
            statusMsg = "no valid write lease exists";
            status    = -ELEASEEXPIRED;
            KFS_LOG_STREAM_ERROR <<
                "failed: " << statusMsg << " " << Show() <<
            KFS_LOG_EOM;
            gChunkManager.SetWriteStatus(writeId, status);
            gLogger.Submit(this);
            return;
        }
        // Notify the lease clerk that we are doing write.  This is to
        // signal the lease clerk to renew the lease for the chunk when appropriate.
        gLeaseClerk.DoingWrite(chunkId);
    }

    SET_HANDLER(this, &WriteSyncOp::Done);

    if (needToForward) {
        status = ForwardToPeer(peerLoc);
        if (status < 0) {
            // can't forward to peer...so fail the write
            Done(EVENT_CMD_DONE, this);
            return;
        }
    }

    // when things aren't aligned, we can't validate the checksums
    // handed by the client.  In such cases, make sure that the
    // chunkservers agree on the checksum
    bool validateChecksums = true;
    bool mismatch = false;
    if (writeMaster &&
        (((offset % CHECKSUM_BLOCKSIZE) != 0) || ((numBytes % CHECKSUM_BLOCKSIZE) != 0))) {
            validateChecksums = false;
    }
    // in the non-writemaster case, our checksums should match what
    // the write master sent us.

    vector<uint32_t> myChecksums = gChunkManager.GetChecksums(chunkId, offset, numBytes);
    if ((!validateChecksums) || (checksums.size() == 0)) {
        // Either we can't validate checksums due to alignment OR the
        // client didn't give us checksums.  In either case:
        // The sync covers a certain region for which the client
        // sent data.  The value for that region should be non-zero
        for (uint32_t i = 0; (i < myChecksums.size()) && !mismatch; i++) {
            if (myChecksums[i] == 0) {
                KFS_LOG_STREAM_ERROR <<
                    "Sync failed due to checksum mismatch: we have 0 in the range " <<
                    offset << "->" << offset+numBytes << " ; but should be non-zero" << KFS_LOG_EOM;
                mismatch = true;
            }
        }
        if (!mismatch)
            KFS_LOG_STREAM_DEBUG << "Validated checksums are non-zero for chunk = " << chunkId
                                 << " offset = " << offset << " numbytes = " << numBytes << KFS_LOG_EOM;
    } else {
        if (myChecksums.size() != checksums.size()) {
            KFS_LOG_STREAM_ERROR <<
                "Checksum mismatch: # of entries we have: " << myChecksums.size() <<
                " # of entries client sent: " << checksums.size() << KFS_LOG_EOM;
            mismatch = true;
        }
        for (uint32_t i = 0; (i < myChecksums.size()) && !mismatch; i++) {
            if (myChecksums[i] != checksums[i]) {
                KFS_LOG_STREAM_ERROR <<
                    "Sync failed due to checksum mismatch: we have = " <<
                    myChecksums[i] << " but the value should be: " << checksums[i] <<
                    KFS_LOG_EOM;
                mismatch = true;
                break;
            }
            // KFS_LOG_STREAM_DEBUG << "Got = " << checksums[i] << " and ours: " << myChecksums[i] << KFS_LOG_EOM;
        }
        // bit of testing code
        // if ((rand() % 20) == 0) {
        // if ((offset == 33554432) && (chunkVersion == 1)) {
        // if ((2097152 <= offset) && (offset <= 4194304) && (chunkVersion == 1)) {
        // KFS_LOG_STREAM_DEBUG << "Intentionally failing verify for chunk = " << chunkId << " offset = " << offset
        // << KFS_LOG_EOM;
        // mismatch = true;
        //}

        if (!mismatch)
            KFS_LOG_STREAM_DEBUG << "Checksum verified for chunk = " << chunkId << " offset = " << offset
                                 << ": " << myChecksums.size() << " and got: " << checksums.size() << KFS_LOG_EOM;
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

int
WriteSyncOp::ForwardToPeer(const ServerLocation& loc)
{
    assert(clnt);
    RemoteSyncSMPtr const peer = FindPeer(*this, loc);
    if (! peer) {
        statusMsg = "no such peer " + loc.ToString();
        return -EHOSTUNREACH;
    }
    fwdedOp = new WriteSyncOp(0, chunkId, chunkVersion, offset, numBytes);
    fwdedOp->numServers = numServers;
    fwdedOp->servers = servers;
    fwdedOp->clnt = this;
    SET_HANDLER(fwdedOp, &KfsOp::HandleDone);

    if (writeMaster) {
        fwdedOp->checksums = gChunkManager.GetChecksums(chunkId, offset, numBytes);
    } else {
        fwdedOp->checksums = this->checksums;
    }
    peer->Enqueue(fwdedOp);
    return 0;
}

int
WriteSyncOp::Done(int code, void *data)
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
        gLeaseClerk.InvalidateLease(chunkId);
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
    gLogger.Submit(this);
    return 0;
}

void
WriteOp::Execute()
{
    status = gChunkManager.WriteChunk(this);

    if (status < 0) {
        if (isFromRecordAppend) {
            HandleEvent(EVENT_CMD_DONE, this);
            return;
        } else {
            assert(wpop);
            wpop->HandleEvent(EVENT_CMD_DONE, this);
        }
    }
}

void
RecordAppendOp::Execute()
{
    ServerLocation peerLoc;
    int myPos = -1;

    needToForwardToPeer(servers, numServers, myPos, peerLoc, true, writeId);
    gAtomicRecordAppendManager.AppendBegin(this, myPos, peerLoc);
}

void
GetRecordAppendOpStatus::Execute()
{
    gAtomicRecordAppendManager.GetOpStatus(this);
    gLogger.Submit(this);
}

void
SizeOp::Execute()
{
    gChunkManager.ChunkSize(this);
    gLogger.Submit(this);
}

void
ChunkSpaceReserveOp::Execute()
{
    ServerLocation peerLoc;
    int myPos = -1;

    needToForwardToPeer(servers, numServers, myPos, peerLoc, true, writeId);
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
        " writeId: " << writeId <<
        " bytes: "   << nbytes  <<
        " status: "  << status  <<
    KFS_LOG_EOM;
    gLogger.Submit(this);
}

void
ChunkSpaceReleaseOp::Execute()
{
    ServerLocation peerLoc;
    int myPos = -1;

    needToForwardToPeer(servers, numServers, myPos, peerLoc, true, writeId);
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
        " writeId: "   << writeId <<
        " requested: " << nbytes  <<
        " reserved: "  << rsvd    <<
        " status: "    << status  <<
    KFS_LOG_EOM;
    gLogger.Submit(this);
}

void
GetChunkMetadataOp::Execute()
{
    SET_HANDLER(this, &GetChunkMetadataOp::HandleChunkMetaReadDone);
    if (gChunkManager.ReadChunkMetadata(chunkId, this) < 0) {
        status = -EINVAL;
        gLogger.Submit(this);
    }
}

int
GetChunkMetadataOp::HandleChunkMetaReadDone(int code, void *data)
{
    if (status >= 0 && data) {
        status = *(int *) data;
    }
    if (status < 0) {
        gLogger.Submit(this);
        return 0;
    }
    const ChunkInfo_t * const info = gChunkManager.GetChunkInfo(chunkId);
    if (info) {
        if (info->chunkBlockChecksum || info->chunkSize == 0) {
            chunkVersion = info->chunkVersion;
            chunkSize    = info->chunkSize;
            if (info->chunkBlockChecksum) {
                dataBuf = new IOBuffer();
                dataBuf->CopyIn((const char *)info->chunkBlockChecksum,
                    MAX_CHUNK_CHECKSUM_BLOCKS * sizeof(uint32_t));
                numBytesIO = dataBuf->BytesConsumable();
            }
        } else {
            assert(! "no checksums");
            status = -EIO;
        }
    } else {
        status = -EBADF;
    }

    if (status < 0 || ! readVerifyFlag) {
        gLogger.Submit(this);
        return 0;
    }

    numBytesScrubbed = 0;
    readOp.chunkId = chunkId;
    readOp.chunkVersion = chunkVersion;
    readOp.offset = 0;
    readOp.numBytes = min((int64_t) 1 << 20, chunkSize);

    readOp.SetScrubOp(this);
    SET_HANDLER(this, &GetChunkMetadataOp::HandleScrubReadDone);
    status = gChunkManager.ReadChunk(&readOp);
    if (status < 0) {
        gLogger.Submit(this);
        return 0;
    }
    return 0;
}

int
GetChunkMetadataOp::HandleScrubReadDone(int code, void *data)
{
    if (code == EVENT_DISK_ERROR) {
        status = -1;
        if (data) {
            status = *(int *) data;
            KFS_LOG_STREAM_ERROR << "disk error:"
                " chunkid: " << chunkId <<
                " status: "  << status <<
            KFS_LOG_EOM;
        }
        gChunkManager.ChunkIOFailed(chunkId, status, readOp.diskIo.get());
        gLogger.Submit(this);
        return 0;
    } else if (code == EVENT_DISK_READ) {
        if (! readOp.dataBuf) {
            readOp.dataBuf = new IOBuffer();
        }
        IOBuffer *b = (IOBuffer *) data;
        // Order matters...when we append b, we take the data from b
        // and put it into our buffer.
        readOp.dataBuf->Append(b);
        if (((size_t) (readOp.offset + readOp.dataBuf->BytesConsumable()) > (size_t) chunkSize) &&
            ((size_t) readOp.dataBuf->BytesConsumable() > (size_t) readOp.numBytes)) {
            // trim the extra stuff off the end.
            readOp.dataBuf->Trim(readOp.numBytes);
        }
        // verify checksum
        gChunkManager.ReadChunkDone(&readOp);
        status = readOp.status;
        if (status == 0) {
            KFS_LOG_STREAM_DEBUG << "scrub read succeeded"
                " chunk: "  << chunkId <<
                " offset: " << readOp.offset <<
            KFS_LOG_EOM;
            // checksum verified; setup the next read
            numBytesScrubbed += readOp.dataBuf->BytesConsumable();
            readOp.offset += readOp.dataBuf->BytesConsumable();
            readOp.numBytes = min((int64_t)kChunkReadSize, chunkSize - numBytesScrubbed);
            // throw away the data
            readOp.dataBuf->Consume(readOp.dataBuf->BytesConsumable());
            if (numBytesScrubbed >= chunkSize) {
                KFS_LOG_STREAM_DEBUG << "scrub succeeded"
                    " chunk: "      << chunkId <<
                    " bytes read: " << numBytesScrubbed <<
                KFS_LOG_EOM;
                gLogger.Submit(this);
                return 0;
            }
            status = gChunkManager.ReadChunk(&readOp);
        }
    }
    if (status < 0) {
        KFS_LOG_STREAM_INFO << "scrub read failed: "
            " chunk: "  << chunkId <<
            " status: " << status <<
        KFS_LOG_EOM;
        gLogger.Submit(this);
        return 0;
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
    if (usedSpace < 0)
        usedSpace = 0;
    status = 0;
    // clnt->HandleEvent(EVENT_CMD_DONE, this);
    gLogger.Submit(this);
}

void
DumpChunkMapOp::Execute()
{
   // Dump chunk map
   gChunkManager.DumpChunkMap();
   status = 0;
   gLogger.Submit(this);
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
    // clnt->HandleEvent(EVENT_CMD_DONE, this);
    gLogger.Submit(this);
}

inline static bool
OkHeader(const KfsOp* op, ostream &os, bool checkStatus = true)
{
    os << "OK\r\n";
    os << "Cseq: "   << op->seq << "\r\n";
    os << "Status: " << (op->status >= 0 ? op->status :
        -SysToKfsErrno(-op->status)) << "\r\n";
    if (! op->statusMsg.empty()) {
        const size_t p = op->statusMsg.find('\r');
        assert(string::npos == p && op->statusMsg.find('\n') == string::npos);
        os << "Status-message: " <<
            (p == string::npos ? op->statusMsg : op->statusMsg.substr(0, p)) <<
        "\r\n";
    }
    if (checkStatus && op->status < 0) {
        os << "\r\n";
    }
    return (op->status >= 0);
}

inline static ostream&
PutHeader(const KfsOp* op, ostream &os)
{
    OkHeader(op, os, false);
    return os;
}

///
/// Generate response for an op based on the KFS protocol.
///
void
KfsOp::Response(ostream &os)
{
    PutHeader(this, os) << "\r\n";
}

void
SizeOp::Response(ostream &os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os << "Size: " << size << "\r\n\r\n";
}

void
GetChunkMetadataOp::Response(ostream &os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n";
    os << "Size: " << chunkSize << "\r\n";
    os << "Content-length: " << numBytesIO << "\r\n\r\n";
}

void
ReadOp::Response(ostream &os)
{
    PutHeader(this, os);
    if (status < 0) {
        os << "\r\n";
        return;
    }

    os << "DiskIOtime: " << (diskIOTime * 1e-6) << "\r\n";
    os << "Checksum-entries: " << checksum.size() << "\r\n";
    if (checksum.size() == 0) {
        os << "Checksums: " << 0 << "\r\n";
    } else {
        os << "Checksums: ";
        for (uint32_t i = 0; i < checksum.size(); i++)
            os << checksum[i] << ' ';
        os << "\r\n";
    }
    os << "Content-length: " << numBytesIO << "\r\n\r\n";
}

void
WriteIdAllocOp::Response(ostream &os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    if (writePrepareReplyFlag) {
        os << "Write-prepare-reply: 1\r\n";
    }
    os << "Write-id: " << writeIdStr <<  "\r\n"
    "\r\n";
}

void
WritePrepareOp::Response(ostream &os)
{
    if (! replyRequestedFlag) {
        // no reply for a prepare...the reply is covered by sync
        return;
    }
    PutHeader(this, os) << "\r\n";
}

void
RecordAppendOp::Response(ostream &os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os << "File-offset: " << fileOffset << "\r\n\r\n";
}

void
RecordAppendOp::Request(ostream &os)
{
    os <<
        "RECORD_APPEND \r\n"
        "Cseq: "             << seq                   << "\r\n"
        "Version: "          << KFS_VERSION_STR       << "\r\n"
        "Chunk-handle: "     << chunkId               << "\r\n"
        "Chunk-version: "    << chunkVersion          << "\r\n"
        "Offset: "           << offset                << "\r\n"
        "File-offset: "      << fileOffset            << "\r\n"
        "Num-bytes: "        << numBytes              << "\r\n"
        "Checksum: "         << checksum              << "\r\n"
        "Num-servers: "      << numServers            << "\r\n"
        "Client-cseq: "      << clientSeq             << "\r\n"
        "Servers: "          << servers               << "\r\n"
        "Master-committed: " << masterCommittedOffset << "\r\n"
    "\r\n";
}

void
GetRecordAppendOpStatus::Request(ostream &os)
{
    os <<
        "GET_RECORD_APPEND_OP_STATUS \r\n"
        "Cseq: "          << seq     << "\r\n"
        "Chunk-handle: "  << chunkId << "\r\n"
        "Write-id: "      << writeId << "\r\n"
    "\r\n";
}

void
GetRecordAppendOpStatus::Response(ostream &os)
{
    PutHeader(this, os);
    os <<
        "Chunk-version: "         << chunkVersion       << "\r\n"
        "Op-seq: "                << opSeq              << "\r\n"
        "Op-status: "             <<
            (opStatus < 0 ? -SysToKfsErrno(-opStatus) : opStatus) << "\r\n"
        "Op-offset: "             << opOffset           << "\r\n"
        "Op-length: "             << opLength           << "\r\n"
        "Wid-append-count: "      << widAppendCount     << "\r\n"
        "Wid-bytes-reserved: "    << widBytesReserved   << "\r\n"
        "Chunk-bytes-reserved: "  << chunkBytesReserved << "\r\n"
        "Remaining-lease-time: "  << remainingLeaseTime << "\r\n"
        "Master-commit-offset: "  << masterCommitOffset << "\r\n"
        "Next-commit-offset: "    << nextCommitOffset   << "\r\n"
        "Wid-read-only: "         << (widReadOnlyFlag    ? 1 : 0) << "\r\n"
        "Wid-was-read-only: "     << (widWasReadOnlyFlag ? 1 : 0) << "\r\n"
        "Chunk-master: "          << (masterFlag         ? 1 : 0) << "\r\n"
        "Stable-flag: "           << (stableFlag         ? 1 : 0) << "\r\n"
        "Open-for-append-flag: "  << (openForAppendFlag  ? 1 : 0) << "\r\n"
        "Appender-state: "        << appenderState      << "\r\n"
        "Appender-state-string: " << appenderStateStr   << "\r\n"
    "\r\n";
}

void
CloseOp::Request(ostream &os)
{
    os <<
        "CLOSE \r\n"
        "Cseq: "     << seq               << "\r\n"
        "Version: "  << KFS_VERSION_STR   << "\r\n"
        "Need-ack: " << (needAck ? 1 : 0) << "\r\n"
    ;
    if (numServers > 0) {
        os <<
            "Num-servers: " << numServers << "\r\n"
            "Servers: "     << servers    << "\r\n"
        ;
    }
    os << "Chunk-handle: " << chunkId << "\r\n";
    if (hasWriteId) {
        os << "Has-write-id: " << 1 << "\r\n";
    }
    if (masterCommitted >= 0) {
        os  << "Master-committed: " << masterCommitted << "\r\n";
    }
    os << "\r\n";
}

void
SizeOp::Request(ostream &os)
{
    os << "SIZE \r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n\r\n";
}

void
GetChunkMetadataOp::Request(ostream &os)
{
    os << "GET_CHUNK_METADATA \r\n"
        "Cseq: " << seq << "\r\n"
        "Version: " << KFS_VERSION_STR << "\r\n"
        "Chunk-handle: " << chunkId << "\r\n"
        "Read-verify: " << (readVerifyFlag ? 1 : 0) << "\r\n"
    "\r\n";
}

void
ReadOp::Request(ostream &os)
{
    os << "READ \r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n";
    os << "Offset: " << offset << "\r\n";
    os << "Num-bytes: " << numBytes << "\r\n\r\n";
}

void
WriteIdAllocOp::Request(ostream &os)
{
    os <<
        "WRITE_ID_ALLOC\r\n"
        "Version: "           << KFS_VERSION_STR             << "\r\n"
        "Cseq: "              << seq                         << "\r\n"
        "Chunk-handle: "      << chunkId                     << "\r\n"
        "Chunk-version: "     << chunkVersion                << "\r\n"
        "Offset: "            << offset                      << "\r\n"
        "Num-bytes: "         << numBytes                    << "\r\n"
        "For-record-append: " << (isForRecordAppend ? 1 : 0) << "\r\n"
        "Client-cseq: "       << clientSeq                   << "\r\n"
        "Num-servers: "       << numServers                  << "\r\n"
        "Servers: "           << servers                     << "\r\n"
    "\r\n";
}

void
WritePrepareFwdOp::Request(ostream &os)
{
    os <<
    "WRITE_PREPARE\r\n"
    "Version: "       << KFS_VERSION_STR << "\r\n"
    "Cseq: "          << seq << "\r\n"
    "Chunk-handle: "  << owner.chunkId << "\r\n"
    "Chunk-version: " << owner.chunkVersion << "\r\n"
    "Offset: "        << owner.offset << "\r\n"
    "Num-bytes: "     << owner.numBytes << "\r\n"
    "Checksum: "      << owner.checksum << "\r\n"
    "Num-servers: "   << owner.numServers << "\r\n"
    "Reply: "         << (owner.replyRequestedFlag ? 1 : 0) << "\r\n"
    "Servers: "       << owner.servers << "\r\n"
    "\r\n";
}

void
WriteSyncOp::Request(ostream &os)
{
    os << "WRITE_SYNC\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n";
    os << "Offset: " << offset << "\r\n";
    os << "Num-bytes: " << numBytes << "\r\n";
    os << "Checksum-entries: " << checksums.size() << "\r\n";
    if (checksums.size() == 0) {
        os << "Checksums: " << 0 << "\r\n";
    } else {
        os << "Checksums: ";
        for (uint32_t i = 0; i < checksums.size(); i++)
            os << checksums[i] << ' ';
        os << "\r\n";
    }
    os << "Num-servers: " << numServers << "\r\n";
    os << "Servers: " << servers << "\r\n\r\n";
}

void
HeartbeatOp::Response(ostream &os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os << response.str() << "\r\n";
}

void
ReplicateChunkOp::Response(ostream &os)
{
    PutHeader(this, os) <<
        "File-handle: "   << fid          << "\r\n"
        "Chunk-version: " << chunkVersion << "\r\n"
    ;
    if (! invalidStripeIdx.empty()) {
        os << "Invalid-stripes: " << invalidStripeIdx << "\r\n";
    }
    os << "\r\n";
}

void
PingOp::Response(ostream &os)
{
    ServerLocation loc = gMetaServerSM.GetLocation();

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
BeginMakeChunkStableOp::Response(ostream& os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os <<
        "Chunk-size: "     << chunkSize     << "\r\n"
        "Chunk-checksum: " << chunkChecksum << "\r\n"
    "\r\n";
}

void
DumpChunkMapOp::Response(ostream &os)
{
    ostringstream v;
    gChunkManager.DumpChunkMap(v);
    PutHeader(this, os) <<
        "Content-length: " << v.str().length() << "\r\n\r\n";
    if (v.str().length() > 0) {
       os << v.str();
    }
}

void
StatsOp::Response(ostream &os)
{
    PutHeader(this, os) << stats << "\r\n";
}

////////////////////////////////////////////////
// Now the handle done's....
////////////////////////////////////////////////

int
SizeOp::HandleDone(int code, void *data)
{
    // notify the owning object that the op finished
    clnt->HandleEvent(EVENT_CMD_DONE, this);
    return 0;
}

int
GetChunkMetadataOp::HandleDone(int code, void *data)
{
    // notify the owning object that the op finished
    clnt->HandleEvent(EVENT_CMD_DONE, this);
    return 0;
}

class ReadChunkMetaNotifier {
    const int res;
public:
    ReadChunkMetaNotifier(int r) : res(r) { }
    void operator()(KfsOp *op) {
        int r = res;
        op->HandleEvent(EVENT_CMD_DONE, &r);
    }
};

int
ReadChunkMetaOp::HandleDone(int code, void *data)
{
    IOBuffer* dataBuf = 0;
    if (code == EVENT_DISK_ERROR) {
        status = data ? *reinterpret_cast<const int*>(data) : -EIO;
        KFS_LOG_STREAM_ERROR <<
            "chunk: " << chunkId <<
            " read meta disk error: " << status <<
        KFS_LOG_EOM;
    } else if (code == EVENT_DISK_READ) {
        dataBuf = reinterpret_cast<IOBuffer*>(data);
    } else {
        status = -EINVAL;
        ostringstream os;
        os  << "read chunk meta data unexpected event: "
            " code: " <<  code << " data: " << data;
        die(os.str());
    }
    gChunkManager.ReadChunkMetadataDone(this, dataBuf);
    int res = status;
    if (clnt) {
        clnt->HandleEvent(EVENT_CMD_DONE, &res);
    }
    for_each(waiters.begin(), waiters.end(), ReadChunkMetaNotifier(res));

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
        if (timeSpent > 5 * kMicroSecs) {
            gChunkServer.SendTelemetryReport(CMD_WRITE, timeSpent);
        }
        // we don't want write id's to pollute stats
        startTime = microseconds();
        OpCounters::WriteDuration(timeSpent);
    }

    delete dataBuf;
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
    // on a successful prepare, dataBuf should be moved to a write op.
    assert(status != 0 || ! dataBuf);

    delete dataBuf;
    delete writeFwdOp;
    delete writeOp;
}

WriteSyncOp::~WriteSyncOp()
{
    delete fwdedOp;
    delete writeOp;
}

void
LeaseRenewOp::Request(ostream &os)
{
    os << "LEASE_RENEW\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Lease-id: " << leaseId << "\r\n";
    os << "Lease-type: " << leaseType << "\r\n\r\n";
}

int
LeaseRenewOp::HandleDone(int code, void *data)
{
    assert(data == this && clnt);
    return clnt->HandleEvent(EVENT_CMD_DONE, data);
}

void
LeaseRelinquishOp::Request(ostream &os)
{
    os << "LEASE_RELINQUISH\r\n"
        "Version: "        << KFS_VERSION_STR << "\r\n"
        "Cseq: "           << seq             << "\r\n"
        "Chunk-handle: "   << chunkId         << "\r\n"
        "Lease-id: "       << leaseId         << "\r\n"
        "Lease-type: "     << leaseType       << "\r\n"
    ;
    if (chunkSize >= 0) {
        os << "Chunk-size: " << chunkSize << "\r\n";
    }
    if (hasChecksum) {
        os << "Chunk-checksum: " << chunkChecksum << "\r\n";
    }
    os << "\r\n";
}

int
LeaseRelinquishOp::HandleDone(int code, void *data)
{
    if (code != EVENT_CMD_DONE || data != this) {
        die("LeaseRelinquishOp: invalid completion");
    }
    delete this;
    return 0;
}

void
CorruptChunkOp::Request(ostream &os)
{
    os <<
    "CORRUPT_CHUNK\r\n"
    "Version: " << KFS_VERSION_STR << "\r\n"
    "Cseq: " << seq << "\r\n"
    "File-handle: " << fid << "\r\n"
    "Chunk-handle: " << chunkId << "\r\n"
    "Is-chunk-lost: " << (isChunkLost ? 1 : 0) << "\r\n"
    ;
    if (noReply) {
        os << "No-reply: 1\r\n";
    }
    if (! chunkDir.empty()) {
        os <<
        "Chunk-dir: " << chunkDir            << "\r\n"
        "Dir-ok: "    << (dirOkFlag ? 1 : 0) << "\r\n"
        ;
    }
    os << "\r\n";
}

int
CorruptChunkOp::HandleDone(int code, void* data)
{
    if (code != EVENT_CMD_DONE || data != this) {
        die("CorruptChunkOp: invalid completion");
    }
    UnRef();
    return 0;
}

void
EvacuateChunksOp::Request(ostream &os)
{
    assert(numChunks <= kMaxChunkIds);

    os <<
    "EVACUATE_CHUNK\r\n"
    "Version: " << KFS_VERSION_STR << "\r\n"
    "Cseq: "    << seq             << "\r\n"
    ;
    if (totalSpace >= 0) {
        os << "Total-space: " << totalSpace << "\r\n";
    }
    if (usedSpace >= 0) {
        os << "Used-space: " << usedSpace << "\r\n";
    }
    if (chunkDirs >= 0) {
        os << "Num-drives: " << chunkDirs << "\r\n";
    }
    if (writableChunkDirs >= 0) {
        os << "Num-wr-drives: " << writableChunkDirs << "\r\n";
    }
    if (evacuateInFlightCount >= 0) {
        os << "Num-evacuate: " << evacuateInFlightCount << "\r\n";
    }
    os << "Chunk-ids:";
    for (int i = 0; i < numChunks; i++) {
        os << " " << chunkIds[i];
    }
    os << "\r\n\r\n";
}

void
AvailableChunksOp::Request(ostream& os)
{
    if (numChunks <= 0 && noReply) {
        return;
    }
    os <<
    "AVAILABLE_CHUNK\r\n"
    "Version: " << KFS_VERSION_STR << "\r\n"
    "Cseq: "    << seq             << "\r\n"
    ;
    os << "Chunk-ids-vers:";
    os << hex;
    for (int i = 0; i < numChunks; i++) {
        os << ' ' << chunkIds[i] << ' ' << chunkVersions[i];
    }
    os << dec;
    os << "\r\n\r\n";
}

void
HelloMetaOp::Request(ostream& os, IOBuffer& buf)
{
    os <<
        "HELLO \r\n"
        "Version: " << KFS_VERSION_STR << "\r\n"
        "Cseq: " << seq << "\r\n"
        "Chunk-server-name: " << myLocation.hostname << "\r\n"
        "Chunk-server-port: " << myLocation.port << "\r\n"
        "Cluster-key: " << clusterKey << "\r\n"
        "MD5Sum: " << md5sum << "\r\n"
        "Rack-id: " << rackId << "\r\n"
        "Total-space: " << totalSpace << "\r\n"
        "Total-fs-space: " << totalFsSpace << "\r\n"
        "Used-space: " << usedSpace << "\r\n"
        "Uptime: " << globalNetManager().UpTime() << "\r\n"
        "Num-chunks: " <<
            chunkLists[kStableChunkList].count << "\r\n"
        "Num-not-stable-append-chunks: " <<
            chunkLists[kNotStableAppendChunkList].count << "\r\n"
        "Num-not-stable-chunks: " <<
            chunkLists[kNotStableChunkList].count << "\r\n"
        "Num-appends-with-wids: " <<
            gAtomicRecordAppendManager.GetAppendersWithWidCount() << "\r\n"
        "Num-re-replications: " << Replicator::GetNumReplications() << "\r\n"
        "Stale-chunks-hex-format: 1\r\n"
        "Content-int-base: 16\r\n"
    ;
    int64_t contentLength = 0;
    for (int i = 0; i < kChunkListCount; i++) {
        contentLength += chunkLists[i].ioBuf.BytesConsumable();
    }
    os << "Content-length: " << contentLength << "\r\n\r\n";
    os.flush();
    // Order matters. The meta server expects the lists to be in this order.
    const int kChunkListsOrder[kChunkListCount] = {
        kStableChunkList,
        kNotStableAppendChunkList,
        kNotStableChunkList
    };
    for (int i = 0; i < kChunkListCount; i++) {
        buf.Move(&chunkLists[kChunkListsOrder[i]].ioBuf);
    }
}

void
SetProperties::Request(ostream &os)
{
    string content;
    properties.getList(content, "");
    contentLength = content.length();
    os << "CMD_SET_PROPERTIES \r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Content-length: " << contentLength << "\r\n\r\n";
    os << content;
}

bool
SetProperties::ParseContent(istream& is)
{
    properties.clear();
    status = min(0, properties.loadProperties(is, '=', false));
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
            gMetaServerSM.SetParameters(properties);
            gChunkManager.SetParameters(properties);
        }
    }
    gLogger.Submit(this);
}

string RestartChunkServer();

void
RestartChunkServerOp::Execute()
{
    statusMsg = RestartChunkServer();
    status = statusMsg.empty() ? 0 : -1;
    gLogger.Submit(this);
}

void
HelloMetaOp::Execute()
{
    int     chunkDirs            = 0;
    int     numEvacuateInFlight  = 0;
    int     numWritableChunkDirs = 0;
    int     evacuateChunks       = 0;
    int64_t evacuateByteCount    = 0;
    totalFsSpace = 0;
    totalSpace = gChunkManager.GetTotalSpace(
        totalFsSpace, chunkDirs, numEvacuateInFlight, numWritableChunkDirs,
        evacuateChunks, evacuateByteCount, 0, 0, &lostChunkDirs);
    usedSpace = gChunkManager.GetUsedSpace();
    IOBuffer::WOStream            streams[kChunkListCount];
    ChunkManager::HostedChunkList lists[kChunkListCount];
    for (int i = 0; i < kChunkListCount; i++) {
        lists[i].first  = &(chunkLists[i].count);
        lists[i].second = &(streams[i].Set(chunkLists[i].ioBuf) << hex);
    }
    gChunkManager.GetHostedChunks(
        lists[kStableChunkList],
        lists[kNotStableAppendChunkList],
        lists[kNotStableChunkList]
    );
    for (int i = 0; i < kChunkListCount; i++) {
        lists[i].second->flush();
        streams[i].Reset();
    }
    status = 0;
    gLogger.Submit(this);
}

}
