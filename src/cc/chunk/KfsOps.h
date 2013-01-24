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
// Declarations for the various Chunkserver ops and RPCs.
//
//
//----------------------------------------------------------------------------

#ifndef _CHUNKSERVER_KFSOPS_H
#define _CHUNKSERVER_KFSOPS_H

#include "kfsio/KfsCallbackObj.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/event.h"

#include "common/Properties.h"
#include "common/kfsdecls.h"
#include "common/time.h"
#include "common/StBuffer.h"
#include "Chunk.h"
#include "DiskIo.h"
#include "RemoteSyncSM.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <string>
#include <istream>
#include <sstream>
#include <vector>
#include <set>
#include <list>

namespace KFS
{

using std::string;
using std::vector;
using std::set;
using std::list;
using std::ostream;
using std::istream;
using std::ostringstream;

enum KfsOp_t {
    CMD_UNKNOWN,
    // Meta server->Chunk server ops
    CMD_ALLOC_CHUNK,
    CMD_DELETE_CHUNK,
    CMD_TRUNCATE_CHUNK,
    CMD_REPLICATE_CHUNK,
    CMD_CHANGE_CHUNK_VERS,
    CMD_BEGIN_MAKE_CHUNK_STABLE,
    CMD_MAKE_CHUNK_STABLE,
    CMD_COALESCE_BLOCK,
    CMD_HEARTBEAT,
    CMD_STALE_CHUNKS,
    CMD_RETIRE,
    // Chunk server->Meta server ops
    CMD_META_HELLO,
    CMD_CORRUPT_CHUNK,
    CMD_LEASE_RENEW,
    CMD_LEASE_RELINQUISH,

    // Client -> Chunkserver ops
    CMD_SYNC,
    CMD_OPEN,
    CMD_CLOSE,
    CMD_READ,
    CMD_WRITE_ID_ALLOC,
    CMD_WRITE_PREPARE,
    CMD_WRITE_PREPARE_FWD,
    CMD_WRITE_SYNC,
    CMD_SIZE,
    // RPCs support for record append: client reserves space and sends
    // us records; the client can also free reserved space
    CMD_RECORD_APPEND,
    CMD_SPC_RESERVE,
    CMD_SPC_RELEASE,
    CMD_GET_RECORD_APPEND_STATUS,
    // when data is loaded KFS, we need a way to verify that what was
    // copied in matches the source.  analogous to md5 model, client
    // can issue this RPC and get the checksums stored for a chunk;
    // the client can comptue checksum on input data and verify that
    // they both match
    CMD_GET_CHUNK_METADATA,
    // Monitoring ops
    CMD_PING,
    CMD_STATS,
    CMD_DUMP_CHUNKMAP,
    // Internally generated ops
    CMD_CHECKPOINT,
    CMD_WRITE,
    CMD_WRITE_CHUNKMETA, // write out the chunk meta-data
    CMD_READ_CHUNKMETA, // read out the chunk meta-data
    // op sent by the network thread to event thread to kill a
    // "RemoteSyncSM".
    CMD_KILL_REMOTE_SYNC,
    // this op is to periodically "kick" the event processor thread
    CMD_TIMEOUT,
    // op to signal the disk manager that some disk I/O has finished
    CMD_DISKIO_COMPLETION,
    CMD_SET_PROPERTIES,
    CMD_RESTART_CHUNK_SERVER,
    CMD_EVACUATE_CHUNKS,
    CMD_AVAILABLE_CHUNKS,
    CMD_CHUNKDIR_INFO,
    CMD_NCMDS
};

enum OpType_t {
    OP_REQUEST,
    OP_RESPONSE
};

const char* const KFS_VERSION_STR = "KFS/1.0";

class ClientSM;

struct KfsOp : public KfsCallbackObj {
    const KfsOp_t   op;
    OpType_t        type;
    kfsSeq_t        seq;
    int32_t         status;
    bool            cancelled:1;
    bool            done:1;
    bool            noReply:1;
    bool            noRetry:1;
    bool            clientSMFlag:1;
    string          statusMsg; // output, optional, mostly for debugging
    KfsCallbackObj* clnt;
    // keep statistics
    int64_t         startTime;

    KfsOp(KfsOp_t o, kfsSeq_t s, KfsCallbackObj *c = 0)
        : op(o),
          type(OP_REQUEST),
          seq(s),
          status(0),
          cancelled(false),
          done(false),
          noReply(false),
          noRetry(false),
          clientSMFlag(false),
          statusMsg(),
          clnt(c),
          startTime(microseconds())
    {
        SET_HANDLER(this, &KfsOp::HandleDone);
        sOpsCount++;
    }
    void Cancel() {
        cancelled = true;
    }
    // to allow dynamic-type-casting, make the destructor virtual
    virtual ~KfsOp();
    virtual void Request(ostream& os, IOBuffer& /* buf */) {
        Request(os);
    }
    // After an op finishes execution, this method generates the
    // response that should be sent back to the client.  The response
    // string that is generated is based on the KFS protocol.
    virtual void Response(ostream& os);
    virtual void ResponseContent(IOBuffer*& buf, int& size) {
        buf  = 0;
        size = 0;
    }
    virtual void Execute() = 0;
    // Return info. about op for debugging
    virtual string Show() const = 0;
    // If the execution of an op suspends and then resumes and
    // finishes, this method should be invoked to signify completion.
    virtual int HandleDone(int code, void *data);
    virtual int GetContentLength() const { return 0; }
    virtual bool ParseContent(istream& is) { return true; }
    virtual bool IsChunkReadOp(
        int64_t& /* numBytes */, kfsChunkId_t& /* chunkId */) { return false; }
    static int64_t GetOpsCount() { return sOpsCount; }
    bool ValidateRequestHeader(
        const char* name,
        size_t      nameLen,
        const char* header,
        size_t      headerLen,
        bool        hasChecksum,
        uint32_t    checksum)
    {
        return (! hasChecksum ||
                Checksum(name, nameLen, header, headerLen) == checksum);
    }
    bool Validate() { return true; }
    ClientSM* GetClientSM();
    static uint32_t Checksum(
        const char* name,
        size_t      nameLen,
        const char* header,
        size_t      headerLen);
    bool HandleUnknownField(
        const char* /* key */, size_t /* keyLen */,
        const char* /* val */, size_t /* valLen */)
        { return true; }
    template<typename T> static T& ParserDef(T& parser)
    {
        return parser
        .Def("Cseq", &KfsOp::seq, kfsSeq_t(-1))
        ;
    }
protected:
    virtual void Request(ostream& /* os */) {
        // fill this method if the op requires a message to be sent to a server.
    };
private:
    static int64_t sOpsCount;
};

//
// Model used in all the c'tor's of the ops: we do minimal
// initialization and primarily init the fields that are used for
// output.  The fields that are "input" are set when they are parsed
// from the input stream.
//
struct AllocChunkOp : public KfsOp {
    kfsFileId_t  fileId;       // input
    kfsChunkId_t chunkId;      // input
    int64_t      chunkVersion; // input
    int64_t      leaseId;      // input
    bool         appendFlag;   // input
    string       servers;      // input
    uint32_t     numServers;
    bool         mustExistFlag;
    AllocChunkOp(kfsSeq_t s = 0)
        : KfsOp(CMD_ALLOC_CHUNK, s),
          fileId(-1),
          chunkId(-1),
          chunkVersion(-1),
          leaseId(-1),
          appendFlag(false),
          servers(),
          numServers(0),
          mustExistFlag(false)
    {
        // All inputs will be parsed in
    }
    void Execute();
    // handlers for reading/writing out the chunk meta-data
    int HandleChunkMetaReadDone(int code, void *data);
    int HandleChunkAllocDone(int code, void *data);
    string Show() const {
        ostringstream os;
        os <<
            "alloc-chunk:"
            " seq: "       << seq <<
            " fileid: "    << fileId <<
            " chunkid: "   << chunkId <<
            " chunkvers: " << chunkVersion <<
            " leaseid: "   << leaseId <<
            " append: "    << (appendFlag ? 1 : 0)
        ;
        return os.str();
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("File-handle",   &AllocChunkOp::fileId,       kfsFileId_t(-1))
        .Def("Chunk-handle",  &AllocChunkOp::chunkId,      kfsChunkId_t(-1))
        .Def("Chunk-version", &AllocChunkOp::chunkVersion, int64_t(-1))
        .Def("Lease-id",      &AllocChunkOp::leaseId,      int64_t(-1))
        .Def("Chunk-append",  &AllocChunkOp::appendFlag,   false)
        .Def("Num-servers",   &AllocChunkOp::numServers)
        .Def("Servers",       &AllocChunkOp::servers)
        ;
    }
};

struct BeginMakeChunkStableOp : public KfsOp {
    kfsFileId_t             fileId;        // input
    kfsChunkId_t            chunkId;       // input
    int64_t                 chunkVersion;  // input
    int64_t                 chunkSize;     // output
    uint32_t                chunkChecksum; // output
    BeginMakeChunkStableOp* next;
    BeginMakeChunkStableOp(kfsSeq_t s = 0)
        : KfsOp(CMD_BEGIN_MAKE_CHUNK_STABLE, s),
            fileId(-1),
            chunkId(-1),
            chunkVersion(-1),
            chunkSize(-1),
            chunkChecksum(0),
            next(0)
        {}
    void Execute();
    void Response(ostream &os);
    string Show() const {
        ostringstream os;
        os << "begin-make-chunk-stable:"
            " seq: "       << seq <<
            " fileid: "    << fileId <<
            " chunkid: "   << chunkId <<
            " chunkvers: " << chunkVersion <<
            " size: "      << chunkSize <<
            " checksum: "  << chunkChecksum
        ;
        return os.str();
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("File-handle",   &BeginMakeChunkStableOp::fileId,       kfsFileId_t(-1))
        .Def("Chunk-handle",  &BeginMakeChunkStableOp::chunkId,      kfsChunkId_t(-1))
        .Def("Chunk-version", &BeginMakeChunkStableOp::chunkVersion, int64_t(-1))
        ;
    }
};

struct MakeChunkStableOp : public KfsOp {
    kfsFileId_t        fileId;        // input
    kfsChunkId_t       chunkId;       // input
    int64_t            chunkVersion;  // input
    int64_t            chunkSize;     // input
    uint32_t           chunkChecksum; // input
    bool               hasChecksum;
    StringBufT<32>     checksumStr;
    MakeChunkStableOp* next;
    MakeChunkStableOp(kfsSeq_t s = 0)
        : KfsOp(CMD_MAKE_CHUNK_STABLE, s),
          fileId(-1),
          chunkId(-1),
          chunkVersion(-1),
          chunkSize(-1),
          chunkChecksum(0),
          hasChecksum(false),
          checksumStr(),
          next(0)
        {}
    void Execute();
    int HandleChunkMetaReadDone(int code, void *data);
    int HandleMakeStableDone(int code, void *data);
    string Show() const {
        ostringstream os;

        os << "make-chunk-stable:"
            " seq: "          << seq <<
            " fileid: "       << fileId <<
            " chunkid: "      << chunkId <<
            " chunkvers: "    << chunkVersion <<
            " chunksize: "    << chunkSize <<
            " checksum: "     << chunkChecksum <<
            " has-checksum: " << (hasChecksum ? "yes" : "no")
        ;
        return os.str();
    }
    // generic response from KfsOp works..
    bool Validate();
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("File-handle",    &MakeChunkStableOp::fileId,        kfsFileId_t(-1))
        .Def("Chunk-handle",   &MakeChunkStableOp::chunkId,       kfsChunkId_t(-1))
        .Def("Chunk-version",  &MakeChunkStableOp::chunkVersion,  int64_t(-1))
        .Def("Chunk-size",     &MakeChunkStableOp::chunkSize,     int64_t(-1))
        .Def("Chunk-checksum", &MakeChunkStableOp::checksumStr)
        ;
    }
};

struct ChangeChunkVersOp : public KfsOp {
    kfsFileId_t  fileId;           // input
    kfsChunkId_t chunkId;          // input
    int64_t      chunkVersion;     // input
    int64_t      fromChunkVersion; // input
    bool         makeStableFlag;
    ChangeChunkVersOp(kfsSeq_t s = 0)
        : KfsOp(CMD_CHANGE_CHUNK_VERS, s),
          fileId(-1),
          chunkId(-1),
          chunkVersion(-1),
          fromChunkVersion(-1),
          makeStableFlag(false)
        {}
    void Execute();
    // handler for reading in the chunk meta-data
    int HandleChunkMetaReadDone(int code, void *data);
    int HandleChunkMetaWriteDone(int code, void *data);
    string Show() const {
        ostringstream os;

        os << "change-chunk-vers:"
            " fileid: "      << fileId <<
            " chunkid: "     << chunkId <<
            " chunkvers: "   << chunkVersion <<
            " make stable: " << makeStableFlag
        ;
        return os.str();
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("File-handle",        &ChangeChunkVersOp::fileId,           kfsFileId_t(-1))
        .Def("Chunk-handle",       &ChangeChunkVersOp::chunkId,          kfsChunkId_t(-1))
        .Def("Chunk-version",      &ChangeChunkVersOp::chunkVersion,     int64_t(-1))
        .Def("From-chunk-version", &ChangeChunkVersOp::fromChunkVersion, int64_t(-1))
        .Def("Make-stable",        &ChangeChunkVersOp::makeStableFlag,   false)
        ;
    }
};

struct DeleteChunkOp : public KfsOp {
    kfsChunkId_t chunkId; // input
    DeleteChunkOp(kfsSeq_t s = 0)
       : KfsOp(CMD_DELETE_CHUNK, s),
         chunkId(-1)
        {}
    void Execute();
    string Show() const {
        ostringstream os;

        os << "delete-chunk: chunkid: " << chunkId;
        return os.str();
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Chunk-handle", &DeleteChunkOp::chunkId, kfsChunkId_t(-1))
        ;
    }
};

struct TruncateChunkOp : public KfsOp {
    kfsChunkId_t chunkId;  // input
    size_t       chunkSize; // size to which file should be truncated to
    TruncateChunkOp(kfsSeq_t s = 0)
        : KfsOp(CMD_TRUNCATE_CHUNK, s),
          chunkId(-1),
          chunkSize(0)
        {}
    void Execute();
    // handler for reading in the chunk meta-data
    int HandleChunkMetaReadDone(int code, void *data);
    int HandleChunkMetaWriteDone(int code, void *data);
    string Show() const {
        ostringstream os;

        os << "truncate-chunk:"
            " chunkid: "   << chunkId <<
            " chunksize: " << chunkSize
        ;
        return os.str();
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Chunk-handle", &TruncateChunkOp::chunkId,   kfsChunkId_t(-1))
        .Def("Chunk-size",   &TruncateChunkOp::chunkSize, size_t(0))
        ;
    }
};

// Op for replicating the chunk.  The metaserver is asking this
// chunkserver to create a copy of a chunk.  We replicate the chunk
// and then notify the server upon completion.
//
struct ReplicateChunkOp : public KfsOp {
    kfsFileId_t    fid;          // input
    kfsChunkId_t   chunkId;      // input
    ServerLocation location;     // input: where to get the chunk from
    int64_t        chunkVersion; // io: we tell the metaserver what we replicated
    int64_t        fileSize;
    int64_t        chunkOffset;
    int16_t        striperType;
    int16_t        numStripes;
    int16_t        numRecoveryStripes;
    int32_t        stripeSize;
    string         pathName;
    string         invalidStripeIdx;
    int            metaPort;
    StringBufT<64> locationStr;
    ReplicateChunkOp(kfsSeq_t s = 0) :
        KfsOp(CMD_REPLICATE_CHUNK, s),
        fid(-1),
        chunkId(-1),
        location(),
        chunkVersion(-1),
        fileSize(-1),
        chunkOffset(-1),
        striperType(KFS_STRIPED_FILE_TYPE_NONE),
        numStripes(0),
        numRecoveryStripes(0),
        stripeSize(0),
        pathName(),
        invalidStripeIdx(),
        metaPort(-1),
        locationStr()
        {}
    void Execute();
    void Response(ostream &os);
    string Show() const {
        ostringstream os;

        os << "replicate-chunk:" <<
            " fid: "      << fid <<
            " chunkid: "  << chunkId <<
            " version: "  << chunkVersion <<
            " offset: "   << chunkOffset <<
            " stiper: "   << striperType <<
            " dstripes: " << numStripes <<
            " rstripes: " << numRecoveryStripes <<
            " ssize: "    << stripeSize <<
            " fsize: "    << fileSize <<
            " fname: "    << pathName <<
            " invals: "   << invalidStripeIdx
        ;
        return os.str();
    }
    bool Validate()
    {
        if (locationStr.empty()) {
            location.port = metaPort;
        } else {
            location.FromString(locationStr.GetStr());
        }
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("File-handle",          &ReplicateChunkOp::fid,          kfsFileId_t(-1))
        .Def("Chunk-handle",         &ReplicateChunkOp::chunkId,      kfsChunkId_t(-1))
        .Def("Chunk-version",        &ReplicateChunkOp::chunkVersion, int64_t(-1))
        .Def("Chunk-location",       &ReplicateChunkOp::locationStr)
        .Def("Meta-port",            &ReplicateChunkOp::metaPort,     int(-1))
        .Def("Chunk-offset",         &ReplicateChunkOp::chunkOffset,  int64_t(-1))
        .Def("Striper-type",         &ReplicateChunkOp::striperType,  int16_t(KFS_STRIPED_FILE_TYPE_NONE))
        .Def("Num-stripes",          &ReplicateChunkOp::numStripes)
        .Def("Num-recovery-stripes", &ReplicateChunkOp::numRecoveryStripes)
        .Def("Stripe-size",          &ReplicateChunkOp::stripeSize)
        .Def("Pathname",             &ReplicateChunkOp::pathName)
        .Def("File-size",            &ReplicateChunkOp::fileSize,     int64_t(-1))
        ;
    }
};

struct HeartbeatOp : public KfsOp {
    int64_t       metaEvacuateCount; // input
    ostringstream response;
    ostringstream cmdShow;
    HeartbeatOp(kfsSeq_t s = 0)
        : KfsOp(CMD_HEARTBEAT, s),
          metaEvacuateCount(-1),
          response(),
          cmdShow()
        { cmdShow << "meta-heartbeat:"; }
    void Execute();
    void Response(ostream &os);
    string Show() const {
        return cmdShow.str();
    }
    template<typename T> void Append(const char* key1, const char* key2, T val);
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Num-evacuate", &HeartbeatOp::metaEvacuateCount, int64_t(-1))
        ;
    }
};

struct StaleChunksOp : public KfsOp {
    typedef vector<kfsChunkId_t> StaleChunkIds;
    int           contentLength; /* length of data that identifies the stale chunks */
    int           numStaleChunks; /* what the server tells us */
    bool          evacuatedFlag;
    bool          hexFormatFlag;
    StaleChunkIds staleChunkIds; /* data we parse out */
    StaleChunksOp(kfsSeq_t s = 0)
        : KfsOp(CMD_STALE_CHUNKS, s),
          contentLength(0),
          numStaleChunks(0),
          evacuatedFlag(false),
          hexFormatFlag(false),
          staleChunkIds()
        {}
    void Execute();
    string Show() const {
        ostringstream os;

        os << "stale chunks: count: " << numStaleChunks <<
            " evacuated: " << evacuatedFlag;
        return os.str();
    }
    virtual int GetContentLength() const { return contentLength; }
    virtual bool ParseContent(istream& is);
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Content-length", &StaleChunksOp::contentLength)
        .Def("Num-chunks",     &StaleChunksOp::numStaleChunks)
        .Def("Evacuated",      &StaleChunksOp::evacuatedFlag, false)
        .Def("HexFormat",      &StaleChunksOp::hexFormatFlag, false)
        ;
    }
};

struct RetireOp : public KfsOp {
    RetireOp(kfsSeq_t s = 0)
        : KfsOp(CMD_RETIRE, s)
        {}
    void Execute();
    string Show() const {
        return "meta-server is telling us to retire";
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        ;
    }
};

struct OpenOp : public KfsOp {
    kfsChunkId_t   chunkId;  // input
    int            openFlags;  // either O_RDONLY, O_WRONLY
    StringBufT<64> intentStr;
    OpenOp(kfsSeq_t s = 0)
        : KfsOp(CMD_OPEN, s),
          chunkId(-1),
          openFlags(0),
          intentStr()
        {}
    void Execute();
    string Show() const {
        ostringstream os;

        os << "open: chunkId: " << chunkId;
        return os.str();
    }
    bool Valudate()
    {
        openFlags = O_RDWR;
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Chunk-handle", &OpenOp::chunkId, kfsChunkId_t(-1))
        .Def("Intent",       &OpenOp::intentStr)
        ;
    }
};

struct CloseOp : public KfsOp {
    kfsChunkId_t chunkId;         // input
    uint32_t     numServers;      // input
    bool         needAck;         // input: when set, this RPC is ack'ed
    bool         hasWriteId;      // input
    int64_t      masterCommitted; // input
    string       servers;         // input: set of servers on which to chunk is to be closed
    CloseOp(kfsSeq_t s = 0, const CloseOp* op = 0)
        : KfsOp(CMD_CLOSE, s),
          chunkId        (op ? op->chunkId         : (kfsChunkId_t)-1),
          numServers     (op ? op->numServers      : 0u),
          needAck        (op ? op->needAck         : true),
          hasWriteId     (op ? op->hasWriteId      : false),
          masterCommitted(op ? op->masterCommitted : (int64_t)-1),
          servers        (op ? op->servers         : string())
        {}
    void Execute();
    string Show() const {
        ostringstream os;
        os <<
            "close:"
            " chunkId: "         << chunkId <<
            " num-servers: "     << numServers <<
            " servers: "         << servers <<
            " need-ack: "        << needAck <<
            " has-write-id: "    << hasWriteId <<
            " mater-committed: " << masterCommitted
        ;
        return os.str();
    }
    // if there was a daisy chain for this chunk, forward the close down the chain
    void Request(ostream &os);
    void Response(ostream &os) {
        if (needAck) {
            KfsOp::Response(os);
        }
    }
    void ForwardToPeer(const ServerLocation &loc);
    int HandlePeerReply(int code, void *data);
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Chunk-handle",     &CloseOp::chunkId,         kfsChunkId_t(-1))
        .Def("Num-servers",      &CloseOp::numServers)
        .Def("Servers",          &CloseOp::servers)
        .Def("Need-ack",         &CloseOp::needAck,         true)
        .Def("Has-write-id",     &CloseOp::hasWriteId,      false)
        .Def("Master-committed", &CloseOp::masterCommitted, int64_t(-1))
        ;
    }
};

struct ReadOp;
struct WriteOp;
struct WriteSyncOp;
struct WritePrepareFwdOp;

// support for record appends
struct RecordAppendOp : public KfsOp {
    kfsSeq_t       clientSeq;             /* input */
    kfsChunkId_t   chunkId;               /* input */
    int64_t        chunkVersion;          /* input */
    size_t         numBytes;              /* input */
    int64_t        writeId;               /* value for the local parsed out of servers string */
    int64_t        offset;                /* input: offset as far as the transaction is concerned */
    int64_t        fileOffset;            /* value set by the head of the daisy chain */
    uint32_t       numServers;            /* input */
    uint32_t       checksum;              /* input: as computed by the sender; 0 means sender didn't send */
    string         servers;               /* input: set of servers on which to write */
    int64_t        masterCommittedOffset; /* input piggy back master's ack to slave */
    StringBufT<32> clientSeqStr;
    IOBuffer       dataBuf;               /* buffer with the data to be written */
    /*
     * when a record append is to be fwd'ed along a daisy chain,
     * this field stores the original op client.
     */
    KfsCallbackObj* origClnt;
    kfsSeq_t        origSeq;
    time_t          replicationStartTime;
    RecordAppendOp* mPrevPtr[1];
    RecordAppendOp* mNextPtr[1];

    RecordAppendOp(kfsSeq_t s = 0);
    virtual ~RecordAppendOp();

    void Request(ostream &os);
    void Response(ostream &os);
    void Execute();
    string Show() const;
    bool Validate();
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Chunk-handle",     &RecordAppendOp::chunkId,               kfsChunkId_t(-1))
        .Def("Chunk-version",    &RecordAppendOp::chunkVersion,          int64_t(-1))
        .Def("Offset",           &RecordAppendOp::offset,                int64_t(-1))
        .Def("File-offset",      &RecordAppendOp::fileOffset,            int64_t(-1))
        .Def("Num-bytes",        &RecordAppendOp::numBytes)
        .Def("Num-servers",      &RecordAppendOp::numServers)
        .Def("Servers",          &RecordAppendOp::servers)
        .Def("Checksum",         &RecordAppendOp::checksum)
        .Def("Client-cseq",      &RecordAppendOp::clientSeqStr)
        .Def("Master-committed", &RecordAppendOp::masterCommittedOffset, int64_t(-1))
        ;
    }
};

struct GetRecordAppendOpStatus : public KfsOp
{
    kfsChunkId_t chunkId;          // input
    int64_t      writeId;          // input
    kfsSeq_t     opSeq;            // output
    int64_t      chunkVersion;
    int64_t      opOffset;
    size_t       opLength;
    int          opStatus;
    size_t       widAppendCount;
    size_t       widBytesReserved;
    size_t       chunkBytesReserved;
    int64_t      remainingLeaseTime;
    int64_t      masterCommitOffset;
    int64_t      nextCommitOffset;
    int          appenderState;
    const char*  appenderStateStr;
    bool         masterFlag;
    bool         stableFlag;
    bool         openForAppendFlag;
    bool         widWasReadOnlyFlag;
    bool         widReadOnlyFlag;

    GetRecordAppendOpStatus(kfsSeq_t s = 0)
        : KfsOp(CMD_GET_RECORD_APPEND_STATUS, s),
          chunkId(-1),
          writeId(-1),
          opSeq(-1),
          chunkVersion(-1),
          opOffset(-1),
          opLength(0),
          opStatus(-1),
          widAppendCount(0),
          widBytesReserved(0),
          chunkBytesReserved(0),
          remainingLeaseTime(0),
          masterCommitOffset(-1),
          nextCommitOffset(-1),
          appenderState(0),
          appenderStateStr(""),
          masterFlag(false),
          stableFlag(false),
          openForAppendFlag(false),
          widWasReadOnlyFlag(false),
          widReadOnlyFlag(false)
    {}
    void Request(ostream &os);
    void Response(ostream &os);
    void Execute();
    string Show() const
    {
        ostringstream os;
        os << "get-record-append-op-status:"
            " seq: "          << seq <<
            " chunkId: "      << chunkId <<
            " writeId: "      << writeId <<
            " status: "       << status  <<
            " op-seq: "       << opSeq <<
            " op-status: "    << opStatus <<
            " wid: "          << (widReadOnlyFlag ? "ro" : "w")
        ;
        return os.str();
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Chunk-handle",  &GetRecordAppendOpStatus::chunkId,      kfsChunkId_t(-1))
        .Def("Chunk-version", &GetRecordAppendOpStatus::chunkVersion, int64_t(-1))
        .Def("Write-id",      &GetRecordAppendOpStatus::writeId,      int64_t(-1))
        ;
    }
};

struct WriteIdAllocOp : public KfsOp {
    kfsSeq_t        clientSeq;         /* input */
    kfsChunkId_t    chunkId;
    int64_t         chunkVersion;
    int64_t         offset;            /* input */
    size_t          numBytes;          /* input */
    int64_t         writeId;           /* output */
    string          writeIdStr;        /* output */
    uint32_t        numServers;        /* input */
    string          servers;           /* input: set of servers on which to write */
    WriteIdAllocOp* fwdedOp;           /* if we did any fwd'ing, this is the op that tracks it */
    bool            isForRecordAppend; /* set if the write-id-alloc is for a record append that will follow */
    bool            writePrepareReplyFlag; /* write prepare reply supported */
    StringBufT<32>  clientSeqStr;
    RemoteSyncSMPtr appendPeer;

    WriteIdAllocOp(kfsSeq_t s = 0)
        : KfsOp(CMD_WRITE_ID_ALLOC, s),
          clientSeq(-1),
          chunkId(-1),
          chunkVersion(-1),
          offset(0),
          numBytes(0),
          writeId(-1),
          writeIdStr(),
          numServers(0),
          servers(),
          fwdedOp(0),
          isForRecordAppend(false),
          writePrepareReplyFlag(true),
          clientSeqStr(),
          appendPeer()
    {
        SET_HANDLER(this, &WriteIdAllocOp::Done);
    }
    WriteIdAllocOp(kfsSeq_t s, const WriteIdAllocOp& other)
        : KfsOp(CMD_WRITE_ID_ALLOC, s),
          clientSeq(other.clientSeq),
          chunkId(other.chunkId),
          chunkVersion(other.chunkVersion),
          offset(other.offset),
          numBytes(other.numBytes),
          writeId(-1),
          numServers(other.numServers),
          servers(other.servers),
          fwdedOp(0),
          isForRecordAppend(other.isForRecordAppend),
          writePrepareReplyFlag(other.writePrepareReplyFlag),
          clientSeqStr(),
          appendPeer()
        {}
    ~WriteIdAllocOp();

    void Request(ostream &os);
    void Response(ostream &os);
    void Execute();
    // should the chunk metadata get paged out, then we use the
    // write-id alloc op as a hint to page the data back in---writes
    // are coming.
    void ReadChunkMetadata();

    int ForwardToPeer(const ServerLocation &peer);
    int HandlePeerReply(int code, void *data);
    int Done(int code, void *data);

    string Show() const {
        ostringstream os;

        os << "write-id-alloc:"
            " seq: "          << seq <<
            " client-seq: "   << clientSeq <<
            " chunkId: "      << chunkId <<
            " chunkversion: " << chunkVersion <<
            " servers: "      << servers <<
            " status: "       << status <<
            " msg: "          << statusMsg
        ;
        return os.str();
    }
    bool Validate();
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Chunk-handle",        &WriteIdAllocOp::chunkId,           kfsChunkId_t(-1))
        .Def("Chunk-version",       &WriteIdAllocOp::chunkVersion,      int64_t(-1))
        .Def("Offset",              &WriteIdAllocOp::offset)
        .Def("Num-bytes",           &WriteIdAllocOp::numBytes)
        .Def("Num-servers",         &WriteIdAllocOp::numServers)
        .Def("Servers",             &WriteIdAllocOp::servers)
        .Def("For-record-append",   &WriteIdAllocOp::isForRecordAppend, false)
        .Def("Client-cseq",         &WriteIdAllocOp::clientSeqStr)
        .Def("Write-prepare-reply", &WriteIdAllocOp::writePrepareReplyFlag)
        ;
    }
};

struct WritePrepareOp : public KfsOp {
    kfsChunkId_t       chunkId;
    int64_t            chunkVersion;
    int64_t            offset;   /* input */
    size_t             numBytes; /* input */
    int64_t            writeId; /* value for the local server */
    uint32_t           numServers; /* input */
    uint32_t           checksum; /* input: as computed by the sender; 0 means sender didn't send */
    string             servers; /* input: set of servers on which to write */
    bool               replyRequestedFlag;
    IOBuffer*          dataBuf; /* buffer with the data to be written */
    WritePrepareFwdOp* writeFwdOp; /* op that tracks the data we fwd'ed to a peer */
    WriteOp*           writeOp; /* the underlying write that is queued up locally */
    uint32_t           numDone; // if we did forwarding, we wait for
                                // local/remote to be done; otherwise, we only
                                // wait for local to be done
    WritePrepareOp(kfsSeq_t s = 0)
        : KfsOp(CMD_WRITE_PREPARE, s),
          chunkId(-1),
          chunkVersion(-1),
          offset(0),
          numBytes(0),
          writeId(-1),
          numServers(0),
          checksum(0),
          servers(),
          replyRequestedFlag(false),
          dataBuf(0),
          writeFwdOp(0),
          writeOp(0),
          numDone(0)
        { SET_HANDLER(this, &WritePrepareOp::Done); }
    ~WritePrepareOp();

    void Response(ostream &os);
    void Execute();

    int ForwardToPeer(const ServerLocation& peer);
    int Done(int code, void *data);

    string Show() const {
        ostringstream os;

        os << "write-prepare:"
            " seq: "          << seq <<
            " chunkId: "      << chunkId <<
            " chunkversion: " << chunkVersion <<
            " offset: "       << offset <<
            " numBytes: "     << numBytes
        ;
        return os.str();
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Chunk-handle",  &WritePrepareOp::chunkId,      kfsChunkId_t(-1))
        .Def("Chunk-version", &WritePrepareOp::chunkVersion, int64_t(-1))
        .Def("Offset",        &WritePrepareOp::offset)
        .Def("Num-bytes",     &WritePrepareOp::numBytes)
        .Def("Num-servers",   &WritePrepareOp::numServers)
        .Def("Servers",       &WritePrepareOp::servers)
        .Def("Checksum",      &WritePrepareOp::checksum)
        .Def("Reply",         &WritePrepareOp::replyRequestedFlag)
        ;
    }
};

struct WritePrepareFwdOp : public KfsOp {
    const WritePrepareOp& owner;
    WritePrepareFwdOp(WritePrepareOp& o)
        : KfsOp(CMD_WRITE_PREPARE_FWD, 0),
          owner(o)
        {}
    void Request(ostream &os);
    // nothing to do...we send the data to peer and wait. have a
    // decl. to keep compiler happy
    void Execute() {}

    string Show() const {
        return ("write-prepare-fwd: " + owner.Show());
    }
};

struct WriteOp : public KfsOp {
    kfsChunkId_t     chunkId;
    int64_t          chunkVersion;
    int64_t          offset;   /* input */
    size_t           numBytes; /* input */
    ssize_t          numBytesIO; /* output: # of bytes actually written */
    DiskIoPtr        diskIo; /* disk connection used for writing data */
    IOBuffer*        dataBuf; /* buffer with the data to be written */
    int64_t          diskIOTime;
    vector<uint32_t> checksums; /* store the checksum for logging purposes */
    /*
     * for writes that are smaller than a checksum block, we need to
     * read the whole block in, compute the new checksum and then write
     * out data.  This buffer holds the data read in from disk.
    */
    ReadOp *rop;
    /*
     * The owning write prepare op
     */
    WritePrepareOp *wpop;
    /* Set if the write was triggered due to re-replication */
    bool isFromReReplication;
    // Set if the write is from a record append
    bool isFromRecordAppend;
    // for statistics purposes, have a "holder" op that tracks how long it took a write to finish.
    bool isWriteIdHolder;
    int64_t      writeId;
    // time at which the write was enqueued at the ChunkManager
    time_t       enqueueTime;

    WriteOp(kfsChunkId_t c, int64_t v)
        : KfsOp(CMD_WRITE, 0),
          chunkId(c),
          chunkVersion(v),
          offset(0),
          numBytes(0),
          numBytesIO(0),
          dataBuf(0),
          diskIOTime(0),
          rop(0),
          wpop(0),
          isFromReReplication(false),
          isFromRecordAppend(false),
          isWriteIdHolder(false)
    {
        SET_HANDLER(this, &WriteOp::HandleWriteDone);
    }
    WriteOp(kfsSeq_t s, kfsChunkId_t c, int64_t v, int64_t o, size_t n,
            IOBuffer* b, int64_t id)
        : KfsOp(CMD_WRITE, s),
          chunkId(c),
          chunkVersion(v),
          offset(o),
          numBytes(n),
          numBytesIO(0),
          dataBuf(b),
          diskIOTime(0),
          rop(0),
          wpop(0),
          isFromReReplication(false),
          isFromRecordAppend(false),
          isWriteIdHolder(false),
          writeId(id)
    {
        SET_HANDLER(this, &WriteOp::HandleWriteDone);
    }
    ~WriteOp();

    void InitForRecordAppend() {
        SET_HANDLER(this, &WriteOp::HandleRecordAppendDone);
        dataBuf = new IOBuffer();
        isFromRecordAppend = true;
    }

    void Reset() {
        status = numBytesIO = 0;
        SET_HANDLER(this, &WriteOp::HandleWriteDone);
    }
    void Response(ostream &os) {}
    void Execute();

    // for record appends, this handler will be called back; on the
    // callback, notify the atomic record appender of
    // completion status
    int HandleRecordAppendDone(int code, void *data);
    int HandleWriteDone(int code, void *data);
    int HandleLoggingDone(int code, void *data);

    string Show() const {
        ostringstream os;

        os << "write:"
            " chunkId: "       << chunkId <<
            " chunkversion: "  << chunkVersion <<
            " offset: "        << offset <<
            " numBytes: "      << numBytes
        ;
        return os.str();
    }
};

// sent by the client to force data to disk
struct WriteSyncOp : public KfsOp {
    kfsChunkId_t     chunkId;
    int64_t          chunkVersion;
    // what is the range of data we are sync'ing
    int64_t          offset; /* input */
    size_t           numBytes; /* input */
    // sent by the chunkmaster to downstream replicas; if there is a
    // mismatch, the sync will fail and the client will retry the write
    vector<uint32_t> checksums;
    int64_t          writeId; /* corresponds to the local write */
    uint32_t         numServers;
    string           servers;
    WriteSyncOp*     fwdedOp;
    WriteOp*         writeOp; // the underlying write that needs to be pushed to disk
    uint32_t         numDone; // if we did forwarding, we wait for
                      // local/remote to be done; otherwise, we only
                      // wait for local to be done
    bool             writeMaster; // infer from the server list if we are the "master" for doing the writes
    int              checksumsCnt;
    StringBufT<256>  checksumsStr;

    WriteSyncOp(kfsSeq_t s = 0, kfsChunkId_t c = -1,
            int64_t v = -1, int64_t o = 0, size_t n = 0)
        : KfsOp(CMD_WRITE_SYNC, s),
          chunkId(c),
          chunkVersion(v),
          offset(o),
          numBytes(n),
          writeId(-1),
          numServers(0),
          fwdedOp(0),
          writeOp(0),
          numDone(0),
          writeMaster(false),
          checksumsCnt(0),
          checksumsStr()
        { SET_HANDLER(this, &WriteSyncOp::Done); }
    ~WriteSyncOp();

    void Request(ostream &os);
    void Execute();

    int ForwardToPeer(const ServerLocation &peer);
    int Done(int code, void *data);

    string Show() const {
        ostringstream os;

        os << "write-sync:"
            " seq: "          << seq <<
            " chunkId: "      << chunkId <<
            " chunkversion: " << chunkVersion <<
            " offset: "       << offset <<
            " numBytes: "     << numBytes <<
            " write-ids: "    << servers;
        return os.str();
    }
    bool Validate();
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Chunk-handle",     &WriteSyncOp::chunkId,      kfsChunkId_t(-1))
        .Def("Chunk-version",    &WriteSyncOp::chunkVersion, int64_t(-1))
        .Def("Offset",           &WriteSyncOp::offset)
        .Def("Num-bytes",        &WriteSyncOp::numBytes)
        .Def("Num-servers",      &WriteSyncOp::numServers)
        .Def("Servers",          &WriteSyncOp::servers)
        .Def("Checksum-entries", &WriteSyncOp::checksumsCnt)
        .Def("Checksums",        &WriteSyncOp::checksumsStr)
        ;
    }
};

struct ReadChunkMetaOp : public KfsOp {
    kfsChunkId_t chunkId;
    DiskIoPtr    diskIo; /* disk connection used for reading data */

    // others ops that are also waiting for this particular meta-data
    // read to finish; they'll get notified when the read is done
    list<KfsOp *> waiters;
    ReadChunkMetaOp(kfsChunkId_t c, KfsCallbackObj *o)
        : KfsOp(CMD_READ_CHUNKMETA, 0, o),
          chunkId(c),
          diskIo(),
          waiters()
    {
        SET_HANDLER(this, &ReadChunkMetaOp::HandleDone);
    }

    void Execute() {}
    string Show() const {
        ostringstream os;

        os << "read-chunk-meta: chunkid: " << chunkId;
        return os.str();
    }

    void AddWaiter(KfsOp *op) {
        waiters.push_back(op);
    }
    // Update internal data structures and then notify the waiting op
    // that read of meta-data is done.
    int HandleDone(int code, void *data);
};

struct GetChunkMetadataOp;

struct ReadOp : public KfsOp {
    kfsChunkId_t     chunkId;
    int64_t          chunkVersion;
    int64_t          offset;   /* input */
    size_t           numBytes; /* input */
    ssize_t          numBytesIO; /* output: # of bytes actually read */
    DiskIoPtr        diskIo; /* disk connection used for reading data */
    IOBuffer*        dataBuf; /* buffer with the data read */
    vector<uint32_t> checksum; /* checksum over the data that is sent back to client */
    int64_t          diskIOTime; /* how long did the AIOs take */
    int              retryCnt;
    /*
     * for writes that require the associated checksum block to be
     * read in, store the pointer to the associated write op.
    */
    WriteOp*            wop;
    // for getting chunk metadata, we do a data scrub.
    GetChunkMetadataOp* scrubOp;
    ReadOp(kfsSeq_t s = 0)
        : KfsOp(CMD_READ, s),
          chunkId(-1),
          chunkVersion(-1),
          offset(0),
          numBytes(0),
          numBytesIO(0),
          diskIo(),
          dataBuf(0),
          checksum(),
          diskIOTime(0),
          retryCnt(0),
          wop(0),
          scrubOp(0)
        { SET_HANDLER(this, &ReadOp::HandleDone); }
    ReadOp(WriteOp* w, int64_t o, size_t n)
        : KfsOp(CMD_READ, w->seq),
          chunkId(w->chunkId),
          chunkVersion(w->chunkVersion),
          offset(o),
          numBytes(n),
          numBytesIO(0),
          diskIo(),
          dataBuf(0),
          checksum(),
          diskIOTime(0),
          retryCnt(0),
          wop(w),
          scrubOp(0)
    {
        clnt = w;
        SET_HANDLER(this, &ReadOp::HandleDone);
    }
    ~ReadOp() {
        assert(! wop);
        delete dataBuf;
    }

    void SetScrubOp(GetChunkMetadataOp *sop) {
        scrubOp = sop;
        SET_HANDLER(this, &ReadOp::HandleScrubReadDone);
    }
    void Request(ostream &os);
    void Response(ostream &os);
    void ResponseContent(IOBuffer*& buf, int& size) {
        buf  = status >= 0 ? dataBuf : 0;
        size = buf ? numBytesIO : 0;
    }
    void Execute();
    int HandleDone(int code, void *data);
    // handler for reading in the chunk meta-data
    int HandleChunkMetaReadDone(int code, void *data);
    // handler for dealing with re-replication events
    int HandleReplicatorDone(int code, void *data);
    int HandleScrubReadDone(int code, void *data);
    string Show() const {
        ostringstream os;

        os << "read:"
            " chunkId: "      << chunkId <<
            " chunkversion: " << chunkVersion <<
            " offset: "       << offset <<
            " numBytes: "     << numBytes
        ;
        return os.str();
    }
    virtual bool IsChunkReadOp(int64_t& outNumBytes, kfsChunkId_t& outChunkId);
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Chunk-handle",     &ReadOp::chunkId,      kfsChunkId_t(-1))
        .Def("Chunk-version",    &ReadOp::chunkVersion, int64_t(-1))
        .Def("Offset",           &ReadOp::offset)
        .Def("Num-bytes",        &ReadOp::numBytes)
        ;
    }
};

// used for retrieving a chunk's size
struct SizeOp : public KfsOp {
    kfsFileId_t  fileId; // optional
    kfsChunkId_t chunkId;
    int64_t      chunkVersion;
    int64_t      size; /* result */
    SizeOp(
        kfsSeq_t     s   = 0,
        kfsFileId_t  fid = -1,
        kfsChunkId_t c   = -1,
        int64_t      v   = -1)
        : KfsOp(CMD_SIZE, s),
          fileId(fid),
          chunkId(c),
          chunkVersion(v),
          size(-1)
        {}

    void Request(ostream &os);
    void Response(ostream &os);
    void Execute();
    string Show() const {
        ostringstream os;

        os << "size:"
        " chunkId: "      << chunkId <<
        " chunkversion: " << chunkVersion <<
        " size: "         << size;
        return os.str();
    }
    int HandleDone(int code, void *data);
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("File-handle",   &SizeOp::fileId,       kfsFileId_t(-1))
        .Def("Chunk-handle",  &SizeOp::chunkId,      kfsChunkId_t(-1))
        .Def("Chunk-version", &SizeOp::chunkVersion, int64_t(-1))
        ;
    }
};

// used for reserving space in a chunk
struct ChunkSpaceReserveOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t      writeId; /* value for the local server */
    string       servers; /* input: set of servers on which to write */
    uint32_t     numServers; /* input */
    // client to provide transaction id (in the daisy chain, the
    // upstream node is a proxy for the client; since the data fwding
    // for a record append is all serialized over a single TCP
    // connection, we need to pass the transaction id so that the
    // receivers in the daisy chain can update state
    //
    size_t        nbytes;
    ChunkSpaceReserveOp(kfsSeq_t s = 0)
        : KfsOp(CMD_SPC_RESERVE, s),
          chunkId(-1),
          writeId(-1),
          servers(),
          numServers(0),
          nbytes(0)
        {}
    ChunkSpaceReserveOp(kfsSeq_t s, kfsChunkId_t c, size_t n)
        : KfsOp(CMD_SPC_RESERVE, s),
          chunkId(c),
          writeId(-1),
          servers(),
          numServers(0),
          nbytes(n)
        {}

    void Execute();
    string Show() const {
        ostringstream os;

        os << "space reserve: chunkId: " << chunkId << " nbytes: " << nbytes;
        return os.str();
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Chunk-handle", &ChunkSpaceReserveOp::chunkId, kfsChunkId_t(-1))
        .Def("Num-bytes",    &ChunkSpaceReserveOp::nbytes)
        .Def("Num-servers",  &ChunkSpaceReserveOp::numServers)
        .Def("Servers",      &ChunkSpaceReserveOp::servers)
        ;
    }
};

// used for releasing previously reserved chunk space reservation
struct ChunkSpaceReleaseOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t      writeId; /* value for the local server */
    string       servers; /* input: set of servers on which to write */
    uint32_t     numServers; /* input */
    size_t       nbytes;
    ChunkSpaceReleaseOp(kfsSeq_t s = 0)
        : KfsOp(CMD_SPC_RELEASE, s),
          chunkId(-1),
          writeId(-1),
          servers(),
          numServers(0),
          nbytes(0)
        {}
    ChunkSpaceReleaseOp(kfsSeq_t s, kfsChunkId_t c, int n)
        : KfsOp(CMD_SPC_RELEASE, s),
          chunkId(c),
          writeId(-1),
          servers(),
          numServers(0),
          nbytes(n)
        {}

    void Execute();
    string Show() const {
        ostringstream os;

        os << "space release: chunkId: " << chunkId << " nbytes: " << nbytes;
        return os.str();
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Chunk-handle", &ChunkSpaceReleaseOp::chunkId, kfsChunkId_t(-1))
        .Def("Num-bytes",    &ChunkSpaceReleaseOp::nbytes)
        .Def("Num-servers",  &ChunkSpaceReleaseOp::numServers)
        .Def("Servers",      &ChunkSpaceReleaseOp::servers)
        ;
    }
};

struct GetChunkMetadataOp : public KfsOp {
    kfsChunkId_t chunkId;  // input
    int64_t      chunkVersion; // output
    bool         readVerifyFlag;
    int64_t      chunkSize; // output
    IOBuffer*    dataBuf; // buffer with the checksum info
    size_t       numBytesIO;
    ReadOp       readOp; // internally generated
    int64_t      numBytesScrubbed;
    enum { kChunkReadSize = 1 << 20, kChunkMetaReadSize = 16 << 10 };

    GetChunkMetadataOp(kfsSeq_t s = 0)
        : KfsOp(CMD_GET_CHUNK_METADATA, s),
          chunkId(-1),
          chunkVersion(0),
          readVerifyFlag(false),
          chunkSize(0),
          dataBuf(0),
          numBytesIO(0),
          readOp(0),
          numBytesScrubbed(0)
        {}
    ~GetChunkMetadataOp()
        { delete dataBuf; }
    void Execute();
    // handler for reading in the chunk meta-data
    int HandleChunkMetaReadDone(int code, void *data);

    // We scrub the chunk 1MB at a time and validate checksums; once
    // the chunk is fully scrubbed and checksums are good, we return
    // the values to the client
    int HandleScrubReadDone(int code, void *data);

    void Request(ostream &os);
    void Response(ostream &os);
    void ResponseContent(IOBuffer*& buf, int& size) {
        buf  = status >= 0 ? dataBuf : 0;
        size = buf ? numBytesIO : 0;
    }
    string Show() const {
        ostringstream os;

        os << "get-chunk-metadata:"
            " chunkid: "      << chunkId <<
            " chunkversion: " << chunkVersion
        ;
        return os.str();
    }
    int HandleDone(int code, void *data);
    virtual bool IsChunkReadOp(int64_t& outNumBytes, kfsChunkId_t& outChunkId) {
        outChunkId  = chunkId;
        outNumBytes = readVerifyFlag ? kChunkReadSize : kChunkMetaReadSize;
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Chunk-handle", &GetChunkMetadataOp::chunkId, kfsChunkId_t(-1))
        .Def("Read-verify",  &GetChunkMetadataOp::readVerifyFlag)
        ;
    }
};

// used for pinging the server and checking liveness
struct PingOp : public KfsOp {
    int64_t totalSpace;
    int64_t usedSpace;
    int64_t totalFsSpace;
    int     evacuateInFlightCount;
    PingOp(kfsSeq_t s = 0)
        : KfsOp(CMD_PING, s),
          totalSpace(-1),
          usedSpace(-1),
          totalFsSpace(-1),
          evacuateInFlightCount(-1)
        {}
    void Response(ostream &os);
    void Execute();
    string Show() const {
        return "monitoring ping";
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        ;
    }
};

// used to dump chunk map
struct DumpChunkMapOp : public KfsOp {
    DumpChunkMapOp(kfsSeq_t s = 0)
       : KfsOp(CMD_DUMP_CHUNKMAP, s)
       {}
    void Response(ostream &os);
    void Execute();
    string Show() const {
       return "dumping chunk map";
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        ;
    }
};

// used to extract out all the counters we have
struct StatsOp : public KfsOp {
    string stats; // result
    StatsOp(kfsSeq_t s = 0)
        : KfsOp(CMD_STATS, s),
          stats()
        {}
    void Response(ostream &os);
    void Execute();
    string Show() const {
        return "monitoring stats";
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        ;
    }
};

struct LeaseRenewOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t      leaseId;
    string       leaseType;
    LeaseRenewOp(kfsSeq_t s, kfsChunkId_t c, int64_t l, string t)
        : KfsOp(CMD_LEASE_RENEW, s),
          chunkId(c),
          leaseId(l),
          leaseType(t)
    {
        SET_HANDLER(this, &LeaseRenewOp::HandleDone);
    }
    void Request(ostream &os);
    // To be called whenever we get a reply from the server
    int HandleDone(int code, void *data);
    void Execute() {}
    string Show() const {
        ostringstream os;

        os << "lease-renew:"
            " chunkid: " << chunkId <<
            " leaseId: " << leaseId <<
            " type: "    << leaseType
        ;
        return os.str();
    }
};

// Whenever we want to give up a lease early, we notify the metaserver
// using this op.
struct LeaseRelinquishOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t      leaseId;
    string       leaseType;
    int64_t      chunkSize;
    uint32_t     chunkChecksum;
    bool         hasChecksum;
    LeaseRelinquishOp(kfsSeq_t s, kfsChunkId_t c, int64_t l, string t)
        : KfsOp(CMD_LEASE_RELINQUISH, s),
          chunkId(c),
          leaseId(l),
          leaseType(t),
          chunkSize(-1),
          chunkChecksum(0),
          hasChecksum(false)
    {
        SET_HANDLER(this, &LeaseRelinquishOp::HandleDone);
    }
    void Request(ostream &os);
    // To be called whenever we get a reply from the server
    int HandleDone(int code, void *data);
    void Execute() {}
    string Show() const {
        ostringstream os;

        os << "lease-relinquish:"
            " chunkid: "  << chunkId <<
            " leaseId: "  << leaseId <<
            " type: "     << leaseType <<
            " size: "     << chunkSize <<
            " checksum: " << chunkChecksum
        ;
        return os.str();
    }
};

// This is just a helper op for building a hello request to the metaserver.
struct HelloMetaOp : public KfsOp {
    typedef vector<string> LostChunkDirs;
    struct ChunkList
    {
        int64_t  count;
        IOBuffer ioBuf;
        ChunkList()
            : count(0),
              ioBuf()
            {}
    };
    enum
    {
        kStableChunkList          = 0,
        kNotStableAppendChunkList = 1,
        kNotStableChunkList       = 2,
        kChunkListCount           = 3
    };

    ServerLocation myLocation;
    string         clusterKey;
    string         md5sum;
    int            rackId;
    int64_t        totalSpace;
    int64_t        totalFsSpace;
    int64_t        usedSpace;
    LostChunkDirs  lostChunkDirs;
    ChunkList      chunkLists[kChunkListCount];
    HelloMetaOp(kfsSeq_t s, const ServerLocation& l,
            const string& k, const string& m, int r)
        : KfsOp(CMD_META_HELLO, s),
          myLocation(l),
          clusterKey(k),
          md5sum(m),
          rackId(r),
          totalSpace(0),
          totalFsSpace(0),
          usedSpace(0),
          lostChunkDirs(),
          chunkLists()
        {}
    void Execute();
    void Request(ostream& os, IOBuffer& buf);
    string Show() const {
        ostringstream os;

        os << "meta-hello:"
            " seq: "         << seq <<
            " mylocation: "  << myLocation.ToString() <<
            " cluster-key: " << clusterKey <<
            " md5sum: "      << md5sum <<
            " rackId: "      << rackId <<
            " space: "       << totalSpace <<
            " used: "        << usedSpace <<
            " chunks: "      << chunkLists[kStableChunkList].count <<
            " not-stable: "  << chunkLists[kNotStableChunkList].count <<
            " append: "      << chunkLists[kNotStableAppendChunkList].count
        ;
        return os.str();
    }
};

struct CorruptChunkOp : public KfsOp {
    kfsFileId_t  fid;     // input: fid whose chunk is bad
    kfsChunkId_t chunkId; // input: chunkid of the corrupted chunk
    // input: set if chunk was lost---happens when we disconnect from metaserver and miss messages
    bool         isChunkLost;
    bool         dirOkFlag;
    string       chunkDir;

    CorruptChunkOp(kfsSeq_t s, kfsFileId_t f, kfsChunkId_t c,
            const string* cDir = 0, bool dOkFlag = false)
        : KfsOp(CMD_CORRUPT_CHUNK, s),
          fid(f),
          chunkId(c),
          isChunkLost(false),
          dirOkFlag(dOkFlag),
          chunkDir(cDir ? *cDir : string()),
          refCount(1)
    {
        noReply = true;
        noRetry = true;
        SET_HANDLER(this, &CorruptChunkOp::HandleDone);
    }
    int Ref() { return refCount++; }
    void UnRef() {
        if (--refCount <= 0) {
            delete this;
        }
    }
    int GetRef() const { return refCount; }
    void Request(ostream &os);
    // To be called whenever we get a reply from the server
    int HandleDone(int code, void *data);
    void Execute() {}
    string Show() const {
        ostringstream os;
        os << "corrupt chunk:"
            " fileid: "  << fid <<
            " chunkid: " << chunkId
        ;
        return os.str();
    }
private:
    int refCount;
};

struct EvacuateChunksOp : public KfsOp {
    enum { kMaxChunkIds = 32 };
    kfsChunkId_t chunkIds[kMaxChunkIds]; // input
    int          numChunks;
    int          chunkDirs;
    int          writableChunkDirs;
    int          evacuateInFlightCount;
    int          evacuateChunks;
    int64_t      totalSpace;
    int64_t      totalFsSpace;
    int64_t      usedSpace;
    int64_t      evacuateByteCount;

    EvacuateChunksOp(kfsSeq_t s = 0, KfsCallbackObj* c = 0)
        : KfsOp(CMD_EVACUATE_CHUNKS, s, c),
          numChunks(0),
          chunkDirs(-1),
          writableChunkDirs(-1),
          evacuateInFlightCount(-1),
          evacuateChunks(0),
          totalSpace(-1),
          totalFsSpace(-1),
          usedSpace(-1),
          evacuateByteCount(-1)
    {
        SET_HANDLER(this, &EvacuateChunksOp::HandleDone);
    }
    void Request(ostream &os);
    // To be called whenever we get a reply from the server
    int HandleDone(int code, void *data) {
        if (clnt) {
            return KfsOp::HandleDone(code, data);
        }
        delete this;
        return 0;
    }
    void Execute() {}
    string Show() const {
        ostringstream os;
        os << "evacuate chunks:";
        for (int i = 0; i < numChunks; i++) {
            os << " " << chunkIds[i];
        }
        return os.str();
    }
};

struct AvailableChunksOp : public KfsOp {
    enum { kMaxChunkIds = 64 };
    kfsChunkId_t chunkIds[kMaxChunkIds];      // input
    kfsChunkId_t chunkVersions[kMaxChunkIds]; // input
    int          numChunks;

    AvailableChunksOp(kfsSeq_t s = 0, KfsCallbackObj* c = 0)
        : KfsOp(CMD_AVAILABLE_CHUNKS, s, c),
          numChunks(0)
    {
        SET_HANDLER(this, &AvailableChunksOp::HandleDone);
    }
    void Request(ostream &os);
    // To be called whenever we get a reply from the server
    int HandleDone(int code, void *data) {
        if (clnt) {
            return KfsOp::HandleDone(code, data);
        }
        delete this;
        return 0;
    }
    void Execute() {}
    string Show() const {
        ostringstream os;
        os << "available chunks:";
        for (int i = 0; i < numChunks; i++) {
            os << " " << chunkIds[i] << "." << chunkVersions[i];
        }
        return os.str();
    }
};

struct SetProperties : public KfsOp {
    int        contentLength;
    Properties properties; // input
    SetProperties(kfsSeq_t seq = 0)
        : KfsOp(CMD_SET_PROPERTIES, seq),
          contentLength(0),
          properties()
        {}
    virtual void Request(ostream &os);
    virtual void Execute();
    virtual string Show() const {
        string ret("set-properties: " );
        properties.getList(ret, "", ";");
        return ret;
    }
    virtual int GetContentLength() const { return contentLength; }
    virtual bool ParseContent(istream& is);
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Content-length", &SetProperties::contentLength)
        ;
    }
};

struct RestartChunkServerOp : public KfsOp {
    RestartChunkServerOp(kfsSeq_t seq = 0)
        : KfsOp(CMD_RESTART_CHUNK_SERVER, seq)
        {}
    virtual void Execute();
    virtual string Show() const {
        return string("restart");
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        ;
    }
};

extern int ParseCommand(const IOBuffer& ioBuf, int len, KfsOp** res);
extern void SubmitOp(KfsOp *op);
extern void SubmitOpResponse(KfsOp *op);

}

#endif // CHUNKSERVER_KFSOPS_H
