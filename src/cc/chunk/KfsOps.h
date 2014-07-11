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
#include "kfsio/CryptoKeys.h"

#include "common/Properties.h"
#include "common/kfsdecls.h"
#include "common/time.h"
#include "common/StBuffer.h"
#include "common/RequestParser.h"

#include "qcdio/QCDLList.h"

#include "Chunk.h"
#include "DiskIo.h"
#include "RemoteSyncSM.h"
#include "utils.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <string>
#include <istream>
#include <sstream>
#include <vector>
#include <list>

#include <boost/shared_ptr.hpp>

class QCMutex;

namespace KFS
{

using std::string;
using std::vector;
using std::list;
using std::ostream;
using std::istream;
using std::ostringstream;
using std::map;
using std::pair;
using boost::shared_ptr;

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
    CMD_AUTHENTICATE,
    CMD_NULL,
    CMD_NCMDS
};

enum OpType_t {
    OP_REQUEST,
    OP_RESPONSE
};

const char* const KFS_VERSION_STR = "KFS/1.0";

class ClientSM;
class BufferManager;

class BufferBytes
{
public:
    BufferBytes()
        : mCount(0)
        {}
private:
    int64_t mCount;
    friend class ClientSM;
private:
    BufferBytes(const BufferBytes&);
    BufferBytes& operator=(const BufferBytes&);
};

struct KfsOp;
class NextOp
{
public:
    NextOp()
        : mNextPtr(0)
        {}
private:
    KfsOp* mNextPtr;
    friend class ClientThreadImpl;
private:
    NextOp(const NextOp&);
    NextOp& operator=(const NextOp&);
};

template<typename> class FwdAccessParser;

class SRChunkAccess
{
public:
    enum { kTokenCount = 2 };
    typedef PropertiesTokenizer::Token Token;

    static SRChunkAccess* Parse(istream& is, int len);
    ~SRChunkAccess()
        { delete [] accessStr; }
    const Token token;
    const Token fwd;
private:
    const char* const accessStr;

    SRChunkAccess(
        const Token tokens[kTokenCount],
        const char* str)
        : token    (tokens[0]),
          fwd      (tokens[1]),
          accessStr(str)
        {}
    SRChunkAccess(const SRChunkAccess&);
    SRChunkAccess& operator=(const SRChunkAccess&);
    friend class FwdAccessParser<SRChunkAccess>;
};
typedef boost::shared_ptr<SRChunkAccess> SRChunkAccessPtr;

class SRChunkServerAccess
{
public:
    enum { kTokenCount = 3 };
    typedef PropertiesTokenizer::Token Token;

    static SRChunkServerAccess* Parse(istream& is, int len);
    ~SRChunkServerAccess()
        { delete [] accessStr; }
    const Token token;
    const Token key;
    const Token fwd;
private:
    const char* const accessStr;

    SRChunkServerAccess(
        const Token tokens[kTokenCount],
        const char* str)
        : token    (tokens[0]),
          key      (tokens[1]),
          fwd      (tokens[2]),
          accessStr(str)
        {}
    SRChunkServerAccess(const SRChunkServerAccess&);
    SRChunkServerAccess& operator=(const SRChunkServerAccess&);
    friend class FwdAccessParser<SRChunkServerAccess>;
};
typedef boost::shared_ptr<SRChunkServerAccess> SRChunkServerAccessPtr;

class SyncReplicationAccess
{
public:
    SRChunkAccessPtr       chunkAccess;
    SRChunkServerAccessPtr chunkServerAccess;

    void Clear()
    {
        chunkAccess.reset();
        chunkServerAccess.reset();
    }
    bool Parse(istream& is, int chunkAccessLength, int len);
};

struct KfsOp : public KfsCallbackObj
{
    class Display
    {
    public:
        Display(const KfsOp& op)
            : mOp(op)
            {}
        Display(const Display& other)
            : mOp(other.mOp)
            {}
        ostream& Show(ostream& os) const
            { return mOp.ShowSelf(os); }
    private:
        const KfsOp& mOp;
    };

    const KfsOp_t   op;
    OpType_t        type;
    kfsSeq_t        seq;
    int32_t         status;
    bool            cancelled:1;
    bool            done:1;
    bool            noReply:1;
    bool            noRetry:1;
    bool            clientSMFlag:1;
    int64_t         maxWaitMillisec;
    string          statusMsg; // output, optional, mostly for debugging
    KfsCallbackObj* clnt;
    // keep statistics
    int64_t         startTime;
    BufferBytes     bufferBytes;
    NextOp          nextOp;

    KfsOp(KfsOp_t o, kfsSeq_t s, KfsCallbackObj *c = 0);
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
    Display Show() const { return Display(*this); }
    // If the execution of an op suspends and then resumes and
    // finishes, this method should be invoked to signify completion.
    virtual int HandleDone(int code, void *data);
    virtual int GetContentLength() const { return 0; }
    virtual bool ParseContent(istream& is) { return true; }
    virtual bool ParseResponse(
        const Properties& /* props */, IOBuffer& /* iobuf */) { return true; }
    virtual bool ParseResponseContent(istream& /* is */, int /* len */)
        { return false; }
    virtual bool GetResponseContent(IOBuffer& /* iobuf */, int len)
        { return (len <= 0); }
    virtual bool IsChunkReadOp(
        int64_t& /* numBytes */, kfsChunkId_t& /* chunkId */) { return false; }
    virtual BufferManager* GetDeviceBufferManager(
        bool /* findFlag */, bool /* resetFlag */) { return 0; }
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
        .Def("Cseq",        &KfsOp::seq,            kfsSeq_t(-1))
        .Def("Max-wait-ms", &KfsOp::maxWaitMillisec, int64_t(-1))
        ;
    }
    static inline BufferManager* GetDeviceBufferMangerSelf(
        bool            findFlag,
        bool            resetFlag,
        kfsChunkId_t    chunkId,
        BufferManager*& devBufMgr)
    {
        if (findFlag && ! devBufMgr) {
            devBufMgr = FindDeviceBufferManager(chunkId);
        }
        BufferManager* const ret = devBufMgr;
        if (resetFlag) {
            devBufMgr = 0;
        }
        return ret;
    }
    static void SetMutex(QCMutex* mutex)
        { sMutex = mutex; }
    static BufferManager* FindDeviceBufferManager(kfsChunkId_t chunkId);
    inline static Display ShowOp(const KfsOp* op)
        { return (op ? Display(*op) : Display(GetNullOp())); }
    virtual bool CheckAccess(ClientSM& sm);
protected:
    virtual void Request(ostream& /* os */) {
        // fill this method if the op requires a message to be sent to a server.
    };
    virtual ostream& ShowSelf(ostream& os) const = 0;
    static const KfsOp& GetNullOp();
    class NullOp;
    friend class NullOp;
private:
    typedef QCDLList<KfsOp> OpsList;
    KfsOp* mPrevPtr[1];
    KfsOp* mNextPtr[1];

    static KfsOp*   sOpsList[1];
    static int64_t  sOpsCount;
    static QCMutex* sMutex;
    class CleanupChecker
    {
    public:
        CleanupChecker()
            { assert(sOpsCount == 0); }
        ~CleanupChecker();
    };
    friend class QCDLListOp<KfsOp>;
private:
    KfsOp(const KfsOp&);
    KfsOp& operator=(const KfsOp&);
};
inline static ostream& operator<<(ostream& os, const KfsOp::Display& disp)
{ return disp.Show(os); }

struct KfsClientChunkOp : public KfsOp
{
    kfsChunkId_t chunkId;
    int64_t      subjectId;
    bool         hasChunkAccessTokenFlag:1;
    bool         chunkAccessTokenValidFlag:1;
    uint16_t     chunkAccessFlags;
    kfsUid_t     chunkAccessUid;

    KfsClientChunkOp(KfsOp_t o, kfsSeq_t s, KfsCallbackObj* c = 0)
        : KfsOp(o, s, c),
          chunkId(-1),
          subjectId(-1),
          hasChunkAccessTokenFlag(false),
          chunkAccessTokenValidFlag(false),
          chunkAccessFlags(0),
          chunkAccessUid(kKfsUserNone),
          chunkAccessVal()
        {}
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Chunk-handle", &KfsClientChunkOp::chunkId, kfsChunkId_t(-1))
        .Def("C-access",     &KfsClientChunkOp::chunkAccessVal)
        .Def("Subject-id",   &KfsClientChunkOp::subjectId, int64_t(-1))
        ;
    }
    inline bool Validate();
    virtual bool CheckAccess(ClientSM& sm);
private:
    TokenValue chunkAccessVal;
};

struct ChunkAccessRequestOp : public KfsClientChunkOp
{
    bool    createChunkAccessFlag;
    bool    createChunkServerAccessFlag;
    int64_t writeId;

    ChunkAccessRequestOp(KfsOp_t o, kfsSeq_t s, KfsCallbackObj* c = 0)
        : KfsClientChunkOp(o, s, c),
          createChunkAccessFlag(false),
          createChunkServerAccessFlag(false),
          writeId(-1)
          {}
    void WriteChunkAccessResponse(
        ostream& os, int64_t subjectId, int accessTokenFlags);
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsClientChunkOp::ParserDef(parser)
        .Def("C-access-req",  &ChunkAccessRequestOp::createChunkAccessFlag)
        .Def("CS-access-req", &ChunkAccessRequestOp::createChunkServerAccessFlag)
        ;
    }
    virtual bool CheckAccess(ClientSM& sm);
    virtual void Response(ostream &os);
};

//
// Model used in all the c'tor's of the ops: we do minimal
// initialization and primarily init the fields that are used for
// output.  The fields that are "input" are set when they are parsed
// from the input stream.
//
struct AllocChunkOp : public KfsOp {
    kfsFileId_t           fileId;
    kfsChunkId_t          chunkId;
    int64_t               chunkVersion;
    int64_t               leaseId;
    bool                  appendFlag;
    StringBufT<256>       servers;
    uint32_t              numServers;
    bool                  mustExistFlag;
    bool                  allowCSClearTextFlag;
    kfsSTier_t            minStorageTier;
    kfsSTier_t            maxStorageTier;
    int64_t               chunkServerAccessValidForTime;
    int64_t               chunkServerAccessIssuedTime;
    int                   contentLength;
    int                   chunkAccessLength;
    SyncReplicationAccess syncReplicationAccess;
    DiskIoPtr             diskIo;

    AllocChunkOp(kfsSeq_t s = 0)
        : KfsOp(CMD_ALLOC_CHUNK, s),
          fileId(-1),
          chunkId(-1),
          chunkVersion(-1),
          leaseId(-1),
          appendFlag(false),
          servers(),
          numServers(0),
          mustExistFlag(false),
          allowCSClearTextFlag(false),
          minStorageTier(kKfsSTierUndef),
          maxStorageTier(kKfsSTierUndef),
          chunkServerAccessValidForTime(0),
          chunkServerAccessIssuedTime(0),
          contentLength(0),
          chunkAccessLength(0),
          syncReplicationAccess(),
          diskIo()
        {}
    void Execute();
    // handlers for reading/writing out the chunk meta-data
    int HandleChunkMetaReadDone(int code, void *data);
    int HandleChunkAllocDone(int code, void *data);
    virtual int GetContentLength() const { return contentLength; }
    virtual bool ParseContent(istream& is)
    {
        return syncReplicationAccess.Parse(
            is, chunkAccessLength, contentLength);
    }
    virtual ostream& ShowSelf(ostream& os) const {
        return os <<
            "alloc-chunk:"
            " seq: "       << seq <<
            " fileid: "    << fileId <<
            " chunkid: "   << chunkId <<
            " chunkvers: " << chunkVersion <<
            " leaseid: "   << leaseId <<
            " append: "    << (appendFlag ? 1 : 0) <<
            " cleartext: " << (allowCSClearTextFlag ? 1 : 0)
        ;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("File-handle",     &AllocChunkOp::fileId,         kfsFileId_t(-1))
        .Def("Chunk-handle",    &AllocChunkOp::chunkId,        kfsChunkId_t(-1))
        .Def("Chunk-version",   &AllocChunkOp::chunkVersion,   int64_t(-1))
        .Def("Lease-id",        &AllocChunkOp::leaseId,        int64_t(-1))
        .Def("Chunk-append",    &AllocChunkOp::appendFlag,     false)
        .Def("Num-servers",     &AllocChunkOp::numServers)
        .Def("Servers",         &AllocChunkOp::servers)
        .Def("Min-tier",        &AllocChunkOp::minStorageTier, kKfsSTierUndef)
        .Def("Max-tier",        &AllocChunkOp::maxStorageTier, kKfsSTierUndef)
        .Def("Content-length",  &AllocChunkOp::contentLength,  0)
        .Def("CS-clear-text",   &AllocChunkOp::allowCSClearTextFlag)
        .Def("C-access-length", &AllocChunkOp::chunkAccessLength)
        .Def("CS-acess-time",   &AllocChunkOp::chunkServerAccessValidForTime)
        .Def("CS-acess-issued", &AllocChunkOp::chunkServerAccessIssuedTime)
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
    virtual ostream& ShowSelf(ostream& os) const {
        return os <<
            "begin-make-chunk-stable:"
            " seq: "       << seq <<
            " fileid: "    << fileId <<
            " chunkid: "   << chunkId <<
            " chunkvers: " << chunkVersion <<
            " size: "      << chunkSize <<
            " checksum: "  << chunkChecksum
        ;
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
    virtual ostream& ShowSelf(ostream& os) const {
        return os <<
            "make-chunk-stable:"
            " seq: "          << seq <<
            " fileid: "       << fileId <<
            " chunkid: "      << chunkId <<
            " chunkvers: "    << chunkVersion <<
            " chunksize: "    << chunkSize <<
            " checksum: "     << chunkChecksum <<
            " has-checksum: " << (hasChecksum ? "yes" : "no")
        ;
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
    virtual ostream& ShowSelf(ostream& os) const {
        return os <<
            "change-chunk-vers:"
            " seq: "         << seq <<
            " fileid: "      << fileId <<
            " chunkid: "     << chunkId <<
            " chunkvers: "   << chunkVersion <<
            " make stable: " << makeStableFlag
        ;
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
    virtual ostream& ShowSelf(ostream& os) const {
        return os <<
            "delete-chunk:"
            " seq: "     << seq <<
            " chunkid: " << chunkId
        ;
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
    virtual ostream& ShowSelf(ostream& os) const {
        return os <<
            "truncate-chunk:"
            " seq: "       << seq <<
            " chunkid: "   << chunkId <<
            " chunksize: " << chunkSize
        ;
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
    kfsFileId_t     fid;          // input
    kfsChunkId_t    chunkId;      // input
    ServerLocation  location;     // input: where to get the chunk from
    int64_t         chunkVersion; // io: we tell the metaserver what we replicated
    int64_t         targetVersion;
    int64_t         fileSize;
    int64_t         chunkOffset;
    int16_t         striperType;
    int16_t         numStripes;
    int16_t         numRecoveryStripes;
    int32_t         stripeSize;
    kfsSTier_t      minStorageTier;
    kfsSTier_t      maxStorageTier;
    string          pathName;
    string          invalidStripeIdx;
    int             metaPort;
    bool            allowCSClearTextFlag;
    StringBufT<64>  locationStr;
    StringBufT<148> chunkServerAccess;
    StringBufT<64>  chunkAccess;

    ReplicateChunkOp(kfsSeq_t s = 0) :
        KfsOp(CMD_REPLICATE_CHUNK, s),
        fid(-1),
        chunkId(-1),
        location(),
        chunkVersion(-1),
        targetVersion(-1),
        fileSize(-1),
        chunkOffset(-1),
        striperType(KFS_STRIPED_FILE_TYPE_NONE),
        numStripes(0),
        numRecoveryStripes(0),
        stripeSize(0),
        minStorageTier(kKfsSTierUndef),
        maxStorageTier(kKfsSTierUndef),
        pathName(),
        invalidStripeIdx(),
        metaPort(-1),
        allowCSClearTextFlag(false),
        locationStr(),
        chunkServerAccess(),
        chunkAccess()
        {}
    void Execute();
    void Response(ostream &os);
    virtual ostream& ShowSelf(ostream& os) const {
        return os <<
            "replicate-chunk:" <<
            " seq: "        << seq <<
            " fid: "        << fid <<
            " chunkid: "    << chunkId <<
            " version: "    << chunkVersion <<
            " targetVers: " << targetVersion <<
            " offset: "     << chunkOffset <<
            " stiper: "     << striperType <<
            " dstripes: "   << numStripes <<
            " rstripes: "   << numRecoveryStripes <<
            " ssize: "      << stripeSize <<
            " fsize: "      << fileSize <<
            " fname: "      << pathName <<
            " invals: "     << invalidStripeIdx
        ;
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
        .Def("File-handle",          &ReplicateChunkOp::fid,            kfsFileId_t(-1))
        .Def("Chunk-handle",         &ReplicateChunkOp::chunkId,        kfsChunkId_t(-1))
        .Def("Chunk-version",        &ReplicateChunkOp::chunkVersion,   int64_t(-1))
        .Def("Target-version",       &ReplicateChunkOp::targetVersion,  int64_t(-1))
        .Def("Chunk-location",       &ReplicateChunkOp::locationStr)
        .Def("Meta-port",            &ReplicateChunkOp::metaPort,       int(-1))
        .Def("Chunk-offset",         &ReplicateChunkOp::chunkOffset,    int64_t(-1))
        .Def("Striper-type",         &ReplicateChunkOp::striperType,    int16_t(KFS_STRIPED_FILE_TYPE_NONE))
        .Def("Num-stripes",          &ReplicateChunkOp::numStripes)
        .Def("Num-recovery-stripes", &ReplicateChunkOp::numRecoveryStripes)
        .Def("Stripe-size",          &ReplicateChunkOp::stripeSize)
        .Def("Pathname",             &ReplicateChunkOp::pathName)
        .Def("File-size",            &ReplicateChunkOp::fileSize,       int64_t(-1))
        .Def("Min-tier",             &ReplicateChunkOp::minStorageTier, kKfsSTierUndef)
        .Def("Max-tier",             &ReplicateChunkOp::maxStorageTier, kKfsSTierUndef)
        .Def("C-access",             &ReplicateChunkOp::chunkAccess)
        .Def("CS-access",            &ReplicateChunkOp::chunkServerAccess)
        .Def("CS-clear-text",        &ReplicateChunkOp::allowCSClearTextFlag)
        ;
    }
};

struct HeartbeatOp : public KfsOp {
    int64_t           metaEvacuateCount; // input
    bool              authenticateFlag;
    IOBuffer          response;
    string            cmdShow;
    bool              sendCurrentKeyFlag;
    CryptoKeys::KeyId currentKeyId;
    CryptoKeys::Key   currentKey;

    HeartbeatOp(kfsSeq_t s = 0)
        : KfsOp(CMD_HEARTBEAT, s),
          metaEvacuateCount(-1),
          authenticateFlag(false),
          response(),
          cmdShow(),
          sendCurrentKeyFlag(false),
          currentKeyId(),
          currentKey()
        {}
    void Execute();
    void Response(ostream &os);
    virtual ostream& ShowSelf(ostream& os) const {
        if (cmdShow.empty()) {
            return os << "heartbeat";
        }
        return os << cmdShow;
    }
    virtual void ResponseContent(IOBuffer*& buf, int& size) {
        buf  = status >= 0 ? &response : 0;
        size = buf ? response.BytesConsumable() : 0;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Num-evacuate", &HeartbeatOp::metaEvacuateCount, int64_t(-1))
        .Def("Authenticate", &HeartbeatOp::authenticateFlag, false)
        ;
    }
};

struct StaleChunksOp : public KfsOp {
    typedef vector<kfsChunkId_t> StaleChunkIds;
    int           contentLength; /* length of data that identifies the stale chunks */
    int           numStaleChunks; /* what the server tells us */
    bool          evacuatedFlag;
    bool          hexFormatFlag;
    kfsSeq_t      availChunksSeq;
    StaleChunkIds staleChunkIds; /* data we parse out */

    StaleChunksOp(kfsSeq_t s = 0)
        : KfsOp(CMD_STALE_CHUNKS, s),
          contentLength(0),
          numStaleChunks(0),
          evacuatedFlag(false),
          hexFormatFlag(false),
          availChunksSeq(-1),
          staleChunkIds()
        {}
    void Execute();
    virtual ostream& ShowSelf(ostream& os) const {
        return os <<
            "stale chunks:"
            " seq: "       << seq <<
            " count: "     << numStaleChunks <<
            " evacuated: " << evacuatedFlag
        ;
    }
    virtual int GetContentLength() const { return contentLength; }
    virtual bool ParseContent(istream& is);
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        .Def("Content-length", &StaleChunksOp::contentLength)
        .Def("Num-chunks",     &StaleChunksOp::numStaleChunks)
        .Def("Evacuated",      &StaleChunksOp::evacuatedFlag,  false)
        .Def("HexFormat",      &StaleChunksOp::hexFormatFlag,  false)
        .Def("AvailChunksSeq", &StaleChunksOp::availChunksSeq, kfsSeq_t(-1))
        ;
    }
};

struct RetireOp : public KfsOp {
    RetireOp(kfsSeq_t s = 0)
        : KfsOp(CMD_RETIRE, s)
        {}
    void Execute();
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "meta-server is telling us to retire";
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        ;
    }
};

struct CloseOp : public KfsClientChunkOp {
    uint32_t              numServers;      // input
    bool                  needAck;         // input: when set, this RPC is ack'ed
    bool                  hasWriteId;      // input
    int64_t               masterCommitted; // input
    StringBufT<256>       servers;         // input: set of servers on which to chunk is to be closed
    int                   chunkAccessLength;
    int                   contentLength;
    SyncReplicationAccess syncReplicationAccess;

    CloseOp(kfsSeq_t s = 0)
        : KfsClientChunkOp(CMD_CLOSE, s),
          numServers           (0u),
          needAck              (true),
          hasWriteId           (false),
          masterCommitted      ((int64_t)-1),
          servers              (),
          chunkAccessLength    (0),
          contentLength        (0),
          syncReplicationAccess()
        {}
    CloseOp(kfsSeq_t s, const CloseOp& op)
        : KfsClientChunkOp(CMD_CLOSE, s),
          numServers           (op.numServers),
          needAck              (op.needAck),
          hasWriteId           (op.hasWriteId),
          masterCommitted      (op.masterCommitted),
          servers              (op.servers),
          chunkAccessLength    (op.chunkAccessLength),
          contentLength        (op.contentLength),
          syncReplicationAccess(op.syncReplicationAccess)
        { chunkId = op.chunkId; }
    virtual int GetContentLength() const { return contentLength; }
    virtual bool ParseContent(istream& is)
    {
        return syncReplicationAccess.Parse(
            is, chunkAccessLength, contentLength);
    }
    void Execute();
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "close:"
            " seq: "             << seq <<
            " chunkId: "         << chunkId <<
            " num-servers: "     << numServers <<
            " servers: "         << servers <<
            " need-ack: "        << needAck <<
            " has-write-id: "    << hasWriteId <<
            " mater-committed: " << masterCommitted
        ;
    }
    // if there was a daisy chain for this chunk, forward the close down the chain
    void Request(ostream &os);
    void Response(ostream &os) {
        if (needAck) {
            KfsOp::Response(os);
        }
    }
    void ForwardToPeer(
        const ServerLocation& loc,
        bool                  wrtieMasterFlag,
        bool                  allowCSClearTextFlag);
    int HandlePeerReply(int code, void *data);
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsClientChunkOp::ParserDef(parser)
        .Def("Num-servers",      &CloseOp::numServers)
        .Def("Servers",          &CloseOp::servers)
        .Def("Need-ack",         &CloseOp::needAck,         true)
        .Def("Has-write-id",     &CloseOp::hasWriteId,      false)
        .Def("Master-committed", &CloseOp::masterCommitted, int64_t(-1))
        .Def("C-access-length",  &CloseOp::chunkAccessLength)
        .Def("Content-length",   &CloseOp::contentLength)
        ;
    }
};

struct ReadOp;
struct WriteOp;
struct WriteSyncOp;
struct WritePrepareFwdOp;

// support for record appends
struct RecordAppendOp : public ChunkAccessRequestOp {
    kfsSeq_t              clientSeq;             /* input */
    int64_t               chunkVersion;          /* input */
    size_t                numBytes;              /* input */
    int64_t               offset;                /* input: offset as far as the transaction is concerned */
    int64_t               fileOffset;            /* value set by the head of the daisy chain */
    uint32_t              numServers;            /* input */
    uint32_t              checksum;              /* input: as computed by the sender; 0 means sender didn't send */
    StringBufT<256>       servers;               /* input: set of servers on which to write */
    int64_t               masterCommittedOffset; /* input piggy back master's ack to slave */
    IOBuffer              dataBuf;               /* buffer with the data to be written */
    int                   chunkAccessLength;
    int                   accessFwdLength;
    SyncReplicationAccess syncReplicationAccess;
    /*
     * when a record append is to be fwd'ed along a daisy chain,
     * this field stores the original op client.
     */
    KfsCallbackObj* origClnt;
    kfsSeq_t        origSeq;
    time_t          replicationStartTime;
    BufferManager*  devBufMgr;
    RecordAppendOp* mPrevPtr[1];
    RecordAppendOp* mNextPtr[1];

    RecordAppendOp(kfsSeq_t s = 0);
    virtual ~RecordAppendOp();

    virtual int GetContentLength() const { return accessFwdLength; }
    virtual bool ParseContent(istream& is)
    {
        return syncReplicationAccess.Parse(
            is, chunkAccessLength, accessFwdLength);
    }
    void Request(ostream &os);
    void Response(ostream &os);
    void Execute();
    virtual ostream& ShowSelf(ostream& os) const;
    bool Validate();
    virtual BufferManager* GetDeviceBufferManager(
        bool findFlag, bool resetFlag)
    {
        return GetDeviceBufferMangerSelf(
            findFlag, resetFlag, chunkId, devBufMgr);
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return ChunkAccessRequestOp::ParserDef(parser)
        .Def("Chunk-version",     &RecordAppendOp::chunkVersion,          int64_t(-1))
        .Def("Offset",            &RecordAppendOp::offset,                int64_t(-1))
        .Def("File-offset",       &RecordAppendOp::fileOffset,            int64_t(-1))
        .Def("Num-bytes",         &RecordAppendOp::numBytes)
        .Def("Num-servers",       &RecordAppendOp::numServers)
        .Def("Servers",           &RecordAppendOp::servers)
        .Def("Checksum",          &RecordAppendOp::checksum)
        .Def("Client-cseq",       &RecordAppendOp::clientSeqVal)
        .Def("Master-committed",  &RecordAppendOp::masterCommittedOffset, int64_t(-1))
        .Def("Access-fwd-length", &RecordAppendOp::accessFwdLength, 0)
        .Def("C-access-length",   &RecordAppendOp::chunkAccessLength)
        ;
    }
private:
    TokenValue clientSeqVal;
};

struct GetRecordAppendOpStatus : public KfsClientChunkOp
{
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
        : KfsClientChunkOp(CMD_GET_RECORD_APPEND_STATUS, s),
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
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "get-record-append-op-status:"
            " seq: "          << seq <<
            " chunkId: "      << chunkId <<
            " writeId: "      << writeId <<
            " status: "       << status  <<
            " op-seq: "       << opSeq <<
            " op-status: "    << opStatus <<
            " wid: "          << (widReadOnlyFlag ? "ro" : "w")
        ;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsClientChunkOp::ParserDef(parser)
        .Def("Chunk-version", &GetRecordAppendOpStatus::chunkVersion, int64_t(-1))
        .Def("Write-id",      &GetRecordAppendOpStatus::writeId,      int64_t(-1))
        ;
    }
};

struct WriteIdAllocOp : public ChunkAccessRequestOp {
    kfsSeq_t              clientSeq;         /* input */
    int64_t               chunkVersion;
    int64_t               offset;            /* input */
    size_t                numBytes;          /* input */
    StringBufT<256>       writeIdStr;        /* output */
    uint32_t              numServers;        /* input */
    StringBufT<256>       servers;           /* input: set of servers on which to write */
    WriteIdAllocOp*       fwdedOp;           /* if we did any fwd'ing, this is the op that tracks it */
    bool                  isForRecordAppend; /* set if the write-id-alloc is for a record append that will follow */
    bool                  writePrepareReplyFlag; /* write prepare reply supported */
    int                   contentLength;
    int                   chunkAccessLength;
    SyncReplicationAccess syncReplicationAccess;
    RemoteSyncSMPtr       appendPeer;

    WriteIdAllocOp(kfsSeq_t s = 0)
        : ChunkAccessRequestOp(CMD_WRITE_ID_ALLOC, s),
          clientSeq(-1),
          chunkVersion(-1),
          offset(0),
          numBytes(0),
          writeIdStr(),
          numServers(0),
          servers(),
          fwdedOp(0),
          isForRecordAppend(false),
          writePrepareReplyFlag(true),
          contentLength(0),
          chunkAccessLength(0),
          syncReplicationAccess(),
          appendPeer(),
          clientSeqVal()
        { SET_HANDLER(this, &WriteIdAllocOp::Done); }
    WriteIdAllocOp(kfsSeq_t s, const WriteIdAllocOp& other)
        : ChunkAccessRequestOp(CMD_WRITE_ID_ALLOC, s),
          clientSeq(other.clientSeq),
          chunkVersion(other.chunkVersion),
          offset(other.offset),
          numBytes(other.numBytes),
          numServers(other.numServers),
          servers(other.servers),
          fwdedOp(0),
          isForRecordAppend(other.isForRecordAppend),
          writePrepareReplyFlag(other.writePrepareReplyFlag),
          contentLength(other.contentLength),
          chunkAccessLength(other.chunkAccessLength),
          syncReplicationAccess(other.syncReplicationAccess),
          appendPeer(),
          clientSeqVal()
        { chunkId = other.chunkId; }
    ~WriteIdAllocOp();

    virtual int GetContentLength() const { return contentLength; }
    virtual bool ParseContent(istream& is)
    {
        return syncReplicationAccess.Parse(
            is, chunkAccessLength, contentLength);
    }
    void Request(ostream &os);
    void Response(ostream &os);
    void Execute();
    // should the chunk metadata get paged out, then we use the
    // write-id alloc op as a hint to page the data back in---writes
    // are coming.
    void ReadChunkMetadata();

    void ForwardToPeer(
        const ServerLocation& loc,
        bool                  wrtieMasterFlag,
        bool                  allowCSClearTextFlag);
    int HandlePeerReply(int code, void *data);
    int Done(int code, void *data);
    virtual bool ParseResponse(const Properties& props, IOBuffer& /* iobuf */)
    {
        const Properties::String* const wids = props.getValue("Write-id");
        if (wids) {
            writeIdStr = *wids;
        } else {
            writeIdStr.clear();
        }
        writePrepareReplyFlag = props.getValue("Write-prepare-reply", 0) != 0;
        return (! writeIdStr.empty());
    }
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "write-id-alloc:"
            " seq: "          << seq <<
            " client-seq: "   << clientSeq <<
            " chunkId: "      << chunkId <<
            " chunkversion: " << chunkVersion <<
            " servers: "      << servers <<
            " status: "       << status <<
            " msg: "          << statusMsg
        ;
    }
    bool Validate();
    template<typename T> static T& ParserDef(T& parser)
    {
        return ChunkAccessRequestOp::ParserDef(parser)
        .Def("Chunk-version",       &WriteIdAllocOp::chunkVersion,      int64_t(-1))
        .Def("Offset",              &WriteIdAllocOp::offset)
        .Def("Num-bytes",           &WriteIdAllocOp::numBytes)
        .Def("Num-servers",         &WriteIdAllocOp::numServers)
        .Def("Servers",             &WriteIdAllocOp::servers)
        .Def("For-record-append",   &WriteIdAllocOp::isForRecordAppend, false)
        .Def("Client-cseq",         &WriteIdAllocOp::clientSeqVal)
        .Def("Write-prepare-reply", &WriteIdAllocOp::writePrepareReplyFlag)
        .Def("Content-length",      &WriteIdAllocOp::contentLength, 0)
        .Def("C-access-length",     &WriteIdAllocOp::chunkAccessLength)
        ;
    }
private:
    TokenValue clientSeqVal;
};

struct WritePrepareOp : public ChunkAccessRequestOp {
    int64_t               chunkVersion;
    int64_t               offset;     /* input */
    size_t                numBytes;   /* input */
    uint32_t              numServers; /* input */
    uint32_t              checksum;   /* input: as computed by the sender; 0 means sender didn't send */
    StringBufT<256>       servers;    /* input: set of servers on which to write */
    bool                  replyRequestedFlag;
    int                   accessFwdLength;
    int                   chunkAccessLength;
    SyncReplicationAccess syncReplicationAccess;
    IOBuffer              dataBuf;    /* buffer with the data to be written */
    WritePrepareFwdOp*    writeFwdOp; /* op that tracks the data we fwd'ed to a peer */
    WriteOp*              writeOp;    /* the underlying write that is queued up locally */
    uint32_t              numDone;    // sub/forwarding ops count
    BufferManager*        devBufMgr;
    uint32_t              receivedChecksum;
    vector<uint32_t>      blocksChecksums;

    WritePrepareOp(kfsSeq_t s = 0)
        : ChunkAccessRequestOp(CMD_WRITE_PREPARE, s),
          chunkVersion(-1),
          offset(0),
          numBytes(0),
          numServers(0),
          checksum(0),
          servers(),
          replyRequestedFlag(false),
          accessFwdLength(0),
          chunkAccessLength(0),
          syncReplicationAccess(),
          dataBuf(),
          writeFwdOp(0),
          writeOp(0),
          numDone(0),
          devBufMgr(0),
          receivedChecksum(0),
          blocksChecksums()
        { SET_HANDLER(this, &WritePrepareOp::Done); }
    ~WritePrepareOp();

    virtual int GetContentLength() const { return accessFwdLength; }
    virtual bool ParseContent(istream& is)
    {
        return syncReplicationAccess.Parse(
            is, chunkAccessLength, accessFwdLength);
    }
    void Response(ostream &os);
    void Execute();
    void ForwardToPeer(
        const ServerLocation& loc,
        bool                  wrtieMasterFlag,
        bool                  allowCSClearTextFlag);
    int Done(int code, void *data);
    virtual BufferManager* GetDeviceBufferManager(
        bool findFlag, bool resetFlag)
    {
        return GetDeviceBufferMangerSelf(
            findFlag, resetFlag, chunkId, devBufMgr);
    }
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "write-prepare:"
            " seq: "          << seq <<
            " chunkId: "      << chunkId <<
            " chunkversion: " << chunkVersion <<
            " offset: "       << offset <<
            " numBytes: "     << numBytes
        ;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return ChunkAccessRequestOp::ParserDef(parser)
        .Def("Chunk-version",     &WritePrepareOp::chunkVersion, int64_t(-1))
        .Def("Offset",            &WritePrepareOp::offset)
        .Def("Num-bytes",         &WritePrepareOp::numBytes)
        .Def("Num-servers",       &WritePrepareOp::numServers)
        .Def("Servers",           &WritePrepareOp::servers)
        .Def("Checksum",          &WritePrepareOp::checksum)
        .Def("Reply",             &WritePrepareOp::replyRequestedFlag)
        .Def("Access-fwd-length", &WritePrepareOp::accessFwdLength, 0)
        .Def("C-access-length",   &WritePrepareOp::chunkAccessLength)
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

    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "write-prepare-fwd: "
            "seq: " << seq <<
            " " << owner.Show()
        ;
    }
};

struct WriteOp : public KfsOp {
    kfsChunkId_t     chunkId;
    int64_t          chunkVersion;
    int64_t          offset;   /* input */
    size_t           numBytes; /* input */
    ssize_t          numBytesIO; /* output: # of bytes actually written */
    DiskIoPtr        diskIo; /* disk connection used for writing data */
    IOBuffer         dataBuf; /* buffer with the data to be written */
    int64_t          diskIOTime;
    vector<uint32_t> checksums; /* store the checksum for logging purposes */
    /*
     * for writes that are smaller than a checksum block, we need to
     * read the whole block in, compute the new checksum and then write
     * out data.  This buffer holds the data read in from disk.
    */
    ReadOp*           rop;
    /*
     * The owning write prepare op
     */
    WritePrepareOp*   wpop;
    /* Set if the write was triggered due to re-replication */
    bool isFromReReplication;
    // Set if the write is from a record append
    bool             isFromRecordAppend;
    // for statistics purposes, have a "holder" op that tracks how long it took a write to finish.
    bool             isWriteIdHolder;
    int64_t          writeId;
    // time at which the write was enqueued at the ChunkManager
    time_t           enqueueTime;

    WriteOp(kfsChunkId_t c, int64_t v)
        : KfsOp(CMD_WRITE, 0),
          chunkId(c),
          chunkVersion(v),
          offset(0),
          numBytes(0),
          numBytesIO(0),
          diskIo(),
          dataBuf(),
          diskIOTime(0),
          checksums(),
          rop(0),
          wpop(0),
          isFromReReplication(false),
          isFromRecordAppend(false),
          isWriteIdHolder(false),
          writeId(-1),
          enqueueTime()
        { SET_HANDLER(this, &WriteOp::HandleWriteDone); }
    WriteOp(kfsSeq_t s, kfsChunkId_t c, int64_t v, int64_t o, size_t n,
            int64_t id)
        : KfsOp(CMD_WRITE, s),
          chunkId(c),
          chunkVersion(v),
          offset(o),
          numBytes(n),
          numBytesIO(0),
          diskIo(),
          dataBuf(),
          diskIOTime(0),
          checksums(),
          rop(0),
          wpop(0),
          isFromReReplication(false),
          isFromRecordAppend(false),
          isWriteIdHolder(false),
          writeId(id),
          enqueueTime()
        { SET_HANDLER(this, &WriteOp::HandleWriteDone); }
    ~WriteOp();
    void InitForRecordAppend()
    {
        SET_HANDLER(this, &WriteOp::HandleRecordAppendDone);
        isFromRecordAppend = true;
    }
    void Reset()
    {
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

    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "write:"
            " chunkId: "       << chunkId <<
            " chunkversion: "  << chunkVersion <<
            " offset: "        << offset <<
            " numBytes: "      << numBytes
        ;
    }
};

// sent by the client to force data to disk
struct WriteSyncOp : public ChunkAccessRequestOp {
    int64_t                   chunkVersion;
    // what is the range of data we are sync'ing
    int64_t                   offset; /* input */
    size_t                    numBytes; /* input */
    // sent by the chunkmaster to downstream replicas; if there is a
    // mismatch, the sync will fail and the client will retry the write
    vector<uint32_t>          checksums;
    uint32_t                  numServers;
    StringBufT<256>           servers;
    WriteSyncOp*              fwdedOp;
    WriteOp*                  writeOp; // the underlying write that needs to be pushed to disk
    uint32_t                  numDone; // if we did forwarding, we wait for
                                       // local/remote to be done; otherwise, we only
                                       // wait for local to be done
    bool                  writeMaster; // infer from the server list if we are the "master" for doing the writes
    int                   checksumsCnt;
    StringBufT<256>       checksumsStr;
    int                   contentLength;
    int                   chunkAccessLength;
    SyncReplicationAccess syncReplicationAccess;

    WriteSyncOp(kfsSeq_t s = 0, kfsChunkId_t c = -1,
            int64_t v = -1, int64_t o = 0, size_t n = 0)
        : ChunkAccessRequestOp(CMD_WRITE_SYNC, s),
          chunkVersion(v),
          offset(o),
          numBytes(n),
          checksums(),
          numServers(0),
          servers(),
          fwdedOp(0),
          writeOp(0),
          numDone(0),
          writeMaster(false),
          checksumsCnt(0),
          checksumsStr(),
          contentLength(0),
          chunkAccessLength(0),
          syncReplicationAccess()
    {
        chunkId = c;
        SET_HANDLER(this, &WriteSyncOp::Done);
    }
    ~WriteSyncOp();
    virtual int GetContentLength() const { return contentLength; }
    virtual bool ParseContent(istream& is)
    {
        return syncReplicationAccess.Parse(
            is, chunkAccessLength, contentLength);
    }
    void Request(ostream &os);
    void Execute();
    void ForwardToPeer(
        const ServerLocation& loc,
        bool                  wrtieMasterFlag,
        bool                  allowCSClearTextFlag);
    int Done(int code, void *data);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "write-sync:"
            " seq: "          << seq <<
            " chunkId: "      << chunkId <<
            " chunkversion: " << chunkVersion <<
            " offset: "       << offset <<
            " numBytes: "     << numBytes <<
            " write-ids: "    << servers;
    }
    bool Validate();
    template<typename T> static T& ParserDef(T& parser)
    {
        return ChunkAccessRequestOp::ParserDef(parser)
        .Def("Chunk-version",    &WriteSyncOp::chunkVersion, int64_t(-1))
        .Def("Offset",           &WriteSyncOp::offset)
        .Def("Num-bytes",        &WriteSyncOp::numBytes)
        .Def("Num-servers",      &WriteSyncOp::numServers)
        .Def("Servers",          &WriteSyncOp::servers)
        .Def("Checksum-entries", &WriteSyncOp::checksumsCnt)
        .Def("Checksums",        &WriteSyncOp::checksumsStr)
        .Def("Content-length",   &WriteSyncOp::contentLength, 0)
        .Def("C-access-length",  &WriteSyncOp::chunkAccessLength)
        ;
    }
};

struct ReadChunkMetaOp : public KfsOp {
    kfsChunkId_t chunkId;
    DiskIoPtr    diskIo; /* disk connection used for reading data */
    // others ops that are also waiting for this particular meta-data
    // read to finish; they'll get notified when the read is done
    list<KfsOp*, StdFastAllocator<KfsOp*> > waiters;

    ReadChunkMetaOp(kfsChunkId_t c, KfsCallbackObj *o)
        : KfsOp(CMD_READ_CHUNKMETA, 0, o),
          chunkId(c),
          diskIo(),
          waiters()
    {
        SET_HANDLER(this, &ReadChunkMetaOp::HandleDone);
    }

    void Execute() {}
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "read-chunk-meta:"
            " chunkid: " << chunkId
        ;
    }

    void AddWaiter(KfsOp *op) {
        waiters.push_back(op);
    }
    // Update internal data structures and then notify the waiting op
    // that read of meta-data is done.
    int HandleDone(int code, void *data);
};

struct GetChunkMetadataOp;

struct ReadOp : public KfsClientChunkOp {
    int64_t          chunkVersion;
    int64_t          offset;     /* input */
    size_t           numBytes;   /* input */
    ssize_t          numBytesIO; /* output: # of bytes actually read */
    DiskIoPtr        diskIo;     /* disk connection used for reading data */
    IOBuffer         dataBuf;    /* buffer with the data read */
    vector<uint32_t> checksum;   /* checksum over the data that is sent back to client */
    int64_t          diskIOTime; /* how long did the AIOs take */
    int              retryCnt;
    bool             skipVerifyDiskChecksumFlag;
    const char*      requestChunkAccess;
    /*
     * for writes that require the associated checksum block to be
     * read in, store the pointer to the associated write op.
    */
    WriteOp*            wop;
    // for getting chunk metadata, we do a data scrub.
    GetChunkMetadataOp* scrubOp;
    BufferManager*      devBufMgr;

    ReadOp(kfsSeq_t s = 0)
        : KfsClientChunkOp(CMD_READ, s),
          chunkVersion(-1),
          offset(0),
          numBytes(0),
          numBytesIO(0),
          diskIo(),
          dataBuf(),
          checksum(),
          diskIOTime(0),
          retryCnt(0),
          skipVerifyDiskChecksumFlag(false),
          requestChunkAccess(0),
          wop(0),
          scrubOp(0),
          devBufMgr(0)
        { SET_HANDLER(this, &ReadOp::HandleDone); }
    ReadOp(WriteOp* w, int64_t o, size_t n)
        : KfsClientChunkOp(CMD_READ, w->seq),
          chunkVersion(w->chunkVersion),
          offset(o),
          numBytes(n),
          numBytesIO(0),
          diskIo(),
          dataBuf(),
          checksum(),
          diskIOTime(0),
          retryCnt(0),
          skipVerifyDiskChecksumFlag(false),
          requestChunkAccess(0),
          wop(w),
          scrubOp(0),
          devBufMgr(0)
    {
        clnt    = w;
        chunkId = w->chunkId;
        SET_HANDLER(this, &ReadOp::HandleDone);
    }
    ~ReadOp() {
        assert(! wop);
    }

    void SetScrubOp(GetChunkMetadataOp *sop) {
        scrubOp = sop;
        SET_HANDLER(this, &ReadOp::HandleScrubReadDone);
    }
    void Request(ostream &os);
    void Response(ostream &os);
    void ResponseContent(IOBuffer*& buf, int& size) {
        buf  = status >= 0 ? &dataBuf : 0;
        size = buf ? numBytesIO : 0;
    }
    void Execute();
    int HandleDone(int code, void *data);
    // handler for reading in the chunk meta-data
    int HandleChunkMetaReadDone(int code, void *data);
    // handler for dealing with re-replication events
    void VerifyReply();
    int HandleReplicatorDone(int code, void *data);
    int HandleScrubReadDone(int code, void *data);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "read:"
            " chunkId: "      << chunkId <<
            " chunkversion: " << chunkVersion <<
            " offset: "       << offset <<
            " numBytes: "     << numBytes <<
            (skipVerifyDiskChecksumFlag ? " skip-disk-chksum" : "")
        ;
    }
    virtual bool IsChunkReadOp(int64_t& outNumBytes, kfsChunkId_t& outChunkId);
    virtual BufferManager* GetDeviceBufferManager(
        bool findFlag, bool resetFlag)
    {
        return GetDeviceBufferMangerSelf(
            findFlag, resetFlag, chunkId, devBufMgr);
    }
    virtual bool ParseResponse(const Properties& props, IOBuffer& iobuf);
    virtual bool GetResponseContent(IOBuffer& iobuf, int len)
    {
        const int nmv = dataBuf.Move(&iobuf, len);
        if (0 <= len && nmv != len) {
            return false;
        }
        numBytesIO = len;
        VerifyReply();
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsClientChunkOp::ParserDef(parser)
        .Def("Chunk-version",    &ReadOp::chunkVersion, int64_t(-1))
        .Def("Offset",           &ReadOp::offset)
        .Def("Num-bytes",        &ReadOp::numBytes)
        .Def("Skip-Disk-Chksum", &ReadOp::skipVerifyDiskChecksumFlag, false)
        ;
    }
};

// used for retrieving a chunk's size
struct SizeOp : public KfsClientChunkOp {
    kfsFileId_t fileId; // optional
    int64_t     chunkVersion;
    int64_t     size; /* result */
    SizeOp(
        kfsSeq_t     s   = 0,
        kfsFileId_t  fid = -1,
        kfsChunkId_t c   = -1,
        int64_t      v   = -1)
        : KfsClientChunkOp(CMD_SIZE, s),
          fileId(fid),
          chunkVersion(v),
          size(-1)
    {
        chunkId = c;
        SET_HANDLER(this, &SizeOp::HandleChunkMetaReadDone);
    }

    void Request(ostream &os);
    void Response(ostream &os);
    void Execute();
    int HandleChunkMetaReadDone(int code, void* data);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "size:"
            " seq: "          << seq <<
            " chunkId: "      << chunkId <<
            " chunkversion: " << chunkVersion <<
            " size: "         << size
        ;
    }
    int HandleDone(int code, void *data);
    virtual bool ParseResponse(const Properties& props, IOBuffer& /* iobuf */)
    {
        size = props.getValue("Size", int64_t(-1));
        return (0 <= size);
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsClientChunkOp::ParserDef(parser)
        .Def("File-handle",   &SizeOp::fileId,       kfsFileId_t(-1))
        .Def("Chunk-version", &SizeOp::chunkVersion, int64_t(-1))
        ;
    }
};

// used for reserving space in a chunk
struct ChunkSpaceReserveOp : public KfsClientChunkOp {
    int64_t         writeId; /* value for the local server */
    StringBufT<256> servers; /* input: set of servers on which to write */
    uint32_t        numServers; /* input */
    // client to provide transaction id (in the daisy chain, the
    // upstream node is a proxy for the client; since the data fwding
    // for a record append is all serialized over a single TCP
    // connection, we need to pass the transaction id so that the
    // receivers in the daisy chain can update state
    //
    size_t        nbytes;

    ChunkSpaceReserveOp(kfsSeq_t s = 0)
        : KfsClientChunkOp(CMD_SPC_RESERVE, s),
          writeId(-1),
          servers(),
          numServers(0),
          nbytes(0)
        {}
    ChunkSpaceReserveOp(kfsSeq_t s, kfsChunkId_t c, size_t n)
        : KfsClientChunkOp(CMD_SPC_RESERVE, s),
          writeId(-1),
          servers(),
          numServers(0),
          nbytes(n)
        { chunkId = c; }

    void Execute();
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "space reserve:"
            " seq: "     << seq <<
            " chunkId: " << chunkId <<
            " nbytes: "  << nbytes
        ;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsClientChunkOp::ParserDef(parser)
        .Def("Num-bytes",    &ChunkSpaceReserveOp::nbytes)
        .Def("Num-servers",  &ChunkSpaceReserveOp::numServers)
        .Def("Servers",      &ChunkSpaceReserveOp::servers)
        ;
    }
};

// used for releasing previously reserved chunk space reservation
struct ChunkSpaceReleaseOp : public KfsClientChunkOp {
    int64_t         writeId; /* value for the local server */
    StringBufT<256> servers; /* input: set of servers on which to write */
    uint32_t        numServers; /* input */
    size_t          nbytes;

    ChunkSpaceReleaseOp(kfsSeq_t s = 0)
        : KfsClientChunkOp(CMD_SPC_RELEASE, s),
          writeId(-1),
          servers(),
          numServers(0),
          nbytes(0)
        {}
    ChunkSpaceReleaseOp(kfsSeq_t s, kfsChunkId_t c, int n)
        : KfsClientChunkOp(CMD_SPC_RELEASE, s),
          writeId(-1),
          servers(),
          numServers(0),
          nbytes(n)
        { chunkId = c; }

    void Execute();
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "space release:"
            " seq: "     << seq <<
            " chunkId: " << chunkId <<
            " nbytes: "  << nbytes
        ;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsClientChunkOp::ParserDef(parser)
        .Def("Num-bytes",    &ChunkSpaceReleaseOp::nbytes)
        .Def("Num-servers",  &ChunkSpaceReleaseOp::numServers)
        .Def("Servers",      &ChunkSpaceReleaseOp::servers)
        ;
    }
};

struct GetChunkMetadataOp : public KfsClientChunkOp {
    int64_t      chunkVersion; // output
    bool         readVerifyFlag;
    int64_t      chunkSize; // output
    IOBuffer     dataBuf; // buffer with the checksum info
    size_t       numBytesIO;
    ReadOp       readOp; // internally generated
    int64_t      numBytesScrubbed;
    const char*  requestChunkAccess;
    enum { kChunkReadSize = 1 << 20, kChunkMetaReadSize = 16 << 10 };

    GetChunkMetadataOp(kfsSeq_t s = 0)
        : KfsClientChunkOp(CMD_GET_CHUNK_METADATA, s),
          chunkVersion(0),
          readVerifyFlag(false),
          chunkSize(0),
          dataBuf(),
          numBytesIO(0),
          readOp(0),
          numBytesScrubbed(0),
          requestChunkAccess(0)
        {}
    ~GetChunkMetadataOp()
        {}
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
        buf  = status >= 0 ? &dataBuf : 0;
        size = buf ? numBytesIO : 0;
    }
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "get-chunk-metadata:"
            " seq: "          << seq <<
            " chunkid: "      << chunkId <<
            " chunkversion: " << chunkVersion
        ;
    }
    int HandleDone(int code, void *data);
    virtual bool IsChunkReadOp(int64_t& outNumBytes, kfsChunkId_t& outChunkId) {
        outChunkId  = chunkId;
        outNumBytes = readVerifyFlag ? kChunkReadSize : kChunkMetaReadSize;
        return true;
    }
    virtual bool ParseResponse(const Properties& props, IOBuffer& /* iobuf */)
    {
        chunkVersion = props.getValue("Chunk-version", int64_t(-1));
        chunkSize    = props.getValue("Size",          int64_t(-1));
        return (0 <= chunkVersion && 0 <= chunkSize);
    }
    virtual bool GetResponseContent(IOBuffer& iobuf, int len)
    {
        return (len < 0 || dataBuf.Move(&iobuf, len) == len);
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsClientChunkOp::ParserDef(parser)
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
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "monitoring ping:"
            " seq: " << seq
        ;
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
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "dump chunk map:"
            "seq: " << seq
        ;
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
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "monitoring stats:"
            " seq: " << seq
        ;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        ;
    }
};

struct LeaseRenewOp : public KfsOp {
    kfsChunkId_t          chunkId;
    int64_t               leaseId;
    const string          leaseType;
    bool                  emitCSAceessFlag;
    bool                  allowCSClearTextFlag;
    int64_t               chunkServerAccessValidForTime;
    int64_t               chunkServerAccessIssuedTime;
    int                   chunkAccessLength;
    SyncReplicationAccess syncReplicationAccess;

    LeaseRenewOp(kfsSeq_t s, kfsChunkId_t c, int64_t l, const string& t, bool a)
        : KfsOp(CMD_LEASE_RENEW, s),
          chunkId(c),
          leaseId(l),
          leaseType(t),
          emitCSAceessFlag(a),
          allowCSClearTextFlag(false),
          chunkServerAccessValidForTime(0),
          chunkServerAccessIssuedTime(0),
          chunkAccessLength(0),
          syncReplicationAccess()
        { SET_HANDLER(this, &LeaseRenewOp::HandleDone); }
    virtual bool ParseResponse(const Properties& props, IOBuffer& /* iobuf */)
    {
        chunkAccessLength             = props.getValue("C-access-length", 0);
        chunkServerAccessValidForTime = props.getValue("CS-acess-time",   0);
        chunkServerAccessIssuedTime   = props.getValue("CS-acess-issued", 0);
        allowCSClearTextFlag          = props.getValue("CS-clear-text", 0) != 0;
        return true;
    }
    virtual bool ParseResponseContent(istream& is, int len)
    {
        return syncReplicationAccess.Parse(
            is, chunkAccessLength, len);
    }
    void Request(ostream &os);
    // To be called whenever we get a reply from the server
    int HandleDone(int code, void *data);
    void Execute() {}
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "lease-renew:"
            " seq: "     << seq <<
            " chunkid: " << chunkId <<
            " leaseId: " << leaseId <<
            " type: "    << leaseType
        ;
    }
};

// Whenever we want to give up a lease early, we notify the metaserver
// using this op.
struct LeaseRelinquishOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t      leaseId;
    const string leaseType;
    int64_t      chunkSize;
    uint32_t     chunkChecksum;
    bool         hasChecksum;

    LeaseRelinquishOp(kfsSeq_t s, kfsChunkId_t c, int64_t l, const string& t)
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
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "lease-relinquish:"
            " seq: "      << seq <<
            " chunkid: "  << chunkId <<
            " leaseId: "  << leaseId <<
            " type: "     << leaseType <<
            " size: "     << chunkSize <<
            " checksum: " << chunkChecksum
        ;
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

    ServerLocation    myLocation;
    string            clusterKey;
    string            md5sum;
    int               rackId;
    int64_t           totalSpace;
    int64_t           totalFsSpace;
    int64_t           usedSpace;
    LostChunkDirs     lostChunkDirs;
    ChunkList         chunkLists[kChunkListCount];
    bool              sendCurrentKeyFlag;
    CryptoKeys::KeyId currentKeyId;
    CryptoKeys::Key   currentKey;
    int64_t           fileSystemId;
    int64_t           metaFileSystemId;
    bool              deleteAllChunksFlag;
    bool              noFidsFlag;

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
          chunkLists(),
          sendCurrentKeyFlag(false),
          currentKeyId(),
          currentKey(),
          fileSystemId(-1),
          metaFileSystemId(-1),
          deleteAllChunksFlag(false),
          noFidsFlag(false)
        {}
    void Execute();
    void Request(ostream& os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "meta-hello:"
            " seq: "         << seq <<
            " mylocation: "  << myLocation <<
            " cluster-key: " << clusterKey <<
            " md5sum: "      << md5sum <<
            " rackId: "      << rackId <<
            " space: "       << totalSpace <<
            " used: "        << usedSpace <<
            " chunks: "      << chunkLists[kStableChunkList].count <<
            " not-stable: "  << chunkLists[kNotStableChunkList].count <<
            " append: "      << chunkLists[kNotStableAppendChunkList].count <<
            " fsid: "        << fileSystemId <<
            " metafsid: "    << metaFileSystemId <<
            " delete flag: " << deleteAllChunksFlag
        ;
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
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "corrupt chunk:"
            " seq: "     << seq <<
            " fileid: "  << fid <<
            " chunkid: " << chunkId
        ;
    }
private:
    int refCount;
};

struct EvacuateChunksOp : public KfsOp {
    class StorageTierInfo
    {
    public:
        StorageTierInfo()
            : mDeviceCount(0),
              mNotStableOpenCount(0),
              mChunkCount(0),
              mSpaceAvailable(0),
              mTotalSpace(0)
            {}
        int32_t mDeviceCount;
        int32_t mNotStableOpenCount;
        int32_t mChunkCount;
        int64_t mSpaceAvailable;
        int64_t mTotalSpace;
    };
    typedef map<
        kfsSTier_t,
        StorageTierInfo,
        less<kfsSTier_t>,
        StdFastAllocator<pair<const kfsSTier_t, StorageTierInfo> >
    > StorageTiersInfo;
    enum { kMaxChunkIds = 32 };
    kfsChunkId_t     chunkIds[kMaxChunkIds]; // input
    int              numChunks;
    int              chunkDirs;
    int              writableChunkDirs;
    int              evacuateInFlightCount;
    int              evacuateChunks;
    int64_t          totalSpace;
    int64_t          totalFsSpace;
    int64_t          usedSpace;
    int64_t          evacuateByteCount;
    StorageTiersInfo tiersInfo;

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
          evacuateByteCount(-1),
          tiersInfo()
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
    virtual ostream& ShowSelf(ostream& os) const
    {
        os << "evacuate chunks: seq: " << seq;
        for (int i = 0; i < numChunks; i++) {
            os << " " << chunkIds[i];
        }
        return os;
    }
};

struct AvailableChunksOp : public KfsOp {
    enum { kMaxChunkIds = 64 };
    typedef pair<kfsChunkId_t, int64_t> Chunks;
    Chunks chunks[kMaxChunkIds];
    int    numChunks;

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
    virtual ostream& ShowSelf(ostream& os) const
    {
        os << "available chunks: seq: " << seq;
        for (int i = 0; i < numChunks; i++) {
            os << " " << chunks[i].first << "." << chunks[i].second;
        }
        return os;
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
    virtual ostream& ShowSelf(ostream& os) const
    {
        string list;
        properties.getList(list, "", ";");
        return os <<
            "set-properties: " <<
            " seq: " << seq <<
            list
        ;
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
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "restart";
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return KfsOp::ParserDef(parser)
        ;
    }
};

struct AuthenticateOp : public KfsOp {
    int         requestedAuthType;
    int         chosenAuthType;
    bool        useSslFlag;
    int         contentLength;
    int         responseContentLength;
    const char* reqBuf;
    char*       responseBuf;

    AuthenticateOp(kfsSeq_t s = 0, int authType = kAuthenticationTypeUndef)
        : KfsOp (CMD_AUTHENTICATE, s),
          requestedAuthType(authType),
          chosenAuthType(kAuthenticationTypeUndef),
          useSslFlag(false),
          contentLength(0),
          responseContentLength(-1),
          reqBuf(0),
          responseBuf(0),
          responseBufPos(0)
        {}
    virtual ~AuthenticateOp()
        { delete [] responseBuf; }
    virtual void Execute() {
        die("unexpected invocation");
    }
    virtual bool ParseResponse(const Properties& props, IOBuffer& /* iobuf */)
    {
        chosenAuthType = props.getValue("Auth-type",
            int(kAuthenticationTypeUndef));
        useSslFlag     = props.getValue("Use-ssl", 0) != 0;
        return true;
    }
    virtual void Request(ostream& os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const {
        return os << "authenticate:"
            " requested: " << requestedAuthType <<
            " chosen: "    << chosenAuthType <<
            " ssl: "       << (useSslFlag ? 1 : 0) <<
            " status: "    << status <<
            " msg: "       << statusMsg
        ;
    }
    int ReadResponseContent(IOBuffer& buf);
private:
    int responseBufPos;
};

extern int ParseMetaCommand(const IOBuffer& ioBuf, int len, KfsOp** res);
extern int ParseClientCommand(const IOBuffer& ioBuf, int len, KfsOp** res,
    char* tmpBuf = 0);
extern void SubmitOp(KfsOp *op);
extern void SubmitOpResponse(KfsOp *op);

}

#endif // CHUNKSERVER_KFSOPS_H
