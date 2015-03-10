/*!
 * $Id$
 *
 * \file MetaRequest.h
 * \brief protocol requests to KFS metadata server
 * \author Blake Lewis (Kosmix Corp.)
 *         Mike Ovsiannikov
 *
 * The model is that various receiver threads handle network
 * connections and extract RPC parameters, then queue a request
 * of the appropriate type for the metadata server to process.
 * When the operation is finished, the server calls back to the
 * receiver with status and any results.
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
#if !defined(KFS_REQUEST_H)
#define KFS_REQUEST_H

#include "common/kfsdecls.h"
#include "kfstypes.h"
#include "meta.h"
#include "util.h"

#include "kfsio/KfsCallbackObj.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/NetConnection.h"
#include "kfsio/CryptoKeys.h"
#include "kfsio/DelegationToken.h"
#include "common/Properties.h"
#include "common/StBuffer.h"
#include "common/StdAllocator.h"
#include "common/DynamicArray.h"
#include "common/ReqOstream.h"
#include "common/RequestParser.h"
#include "common/kfsatomic.h"
#include "qcdio/QCDLList.h"

#include <string.h>

#include <sstream>
#include <vector>
#include <map>
#include <iomanip>

namespace KFS {

using std::ostream;
using std::vector;
using std::map;
using std::pair;
using std::ostringstream;
using std::dec;
using std::oct;
using std::less;

/*!
 * \brief Metadata server operations
 */
#define KfsForEachMetaOpId(f) \
    /* Client -> Metadata server ops */ \
    f(LOOKUP) \
    f(LOOKUP_PATH) \
    f(CREATE) \
    f(MKDIR) \
    f(REMOVE) \
    f(RMDIR) \
    f(READDIR) \
    f(READDIRPLUS) \
    f(GETALLOC) \
    f(GETLAYOUT) \
    f(ALLOCATE) \
    f(TRUNCATE) \
    f(RENAME) \
    f(SETMTIME) /* Set the mtime on a specific file to support cp -p */ \
    f(CHANGE_FILE_REPLICATION) /* Client is asking for a change in file's replication factor */ \
    f(COALESCE_BLOCKS) /* Client is asking for blocks from one file to be coalesced with another */ \
    /* Admin is notifying us to retire a chunkserver */ \
    f(RETIRE_CHUNKSERVER) \
    f(TOGGLE_WORM) /* Toggle metaserver's WORM mode */ \
    /* Metadata server <-> Chunk server ops */ \
    f(HELLO) /* Hello RPC sent by chunkserver on startup */ \
    f(BYE)  /* Internally generated op whenever a chunkserver goes down */ \
    f(CHUNK_HEARTBEAT) /* Periodic heartbeat from meta->chunk */ \
    f(CHUNK_ALLOCATE) /* Allocate chunk RPC from meta->chunk */ \
    f(CHUNK_DELETE)  /* Delete chunk RPC from meta->chunk */ \
    f(CHUNK_STALENOTIFY) /* Stale chunk notification RPC from meta->chunk */ \
    f(BEGIN_MAKE_CHUNK_STABLE) \
    f(CHUNK_MAKE_STABLE) /* Notify a chunkserver to make a chunk stable */ \
    f(CHUNK_COALESCE_BLOCK) /* Notify a chunkserver to coalesce a chunk from file to another */ \
    f(CHUNK_VERSCHANGE) /* Notify chunkserver of version # change from meta->chunk */ \
    f(CHUNK_REPLICATE) /* Ask chunkserver to replicate a chunk */ \
    f(CHUNK_SIZE) /* Ask chunkserver for the size of a chunk */ \
    f(CHUNK_REPLICATION_CHECK) /* Internally generated */ \
    f(CHUNK_CORRUPT) /*  Chunkserver is notifying us that a chunk is corrupt */ \
    /* All the blocks on the retiring server have been evacuated and the */ \
    /* server can safely go down.  We are asking the server to take a graceful bow */ \
    f(CHUNK_RETIRE) \
    /* Lease related messages */ \
    f(LEASE_ACQUIRE) \
    f(LEASE_RENEW) \
    f(LEASE_RELINQUISH) \
    /* Internally generated to cleanup leases */ \
    f(LEASE_CLEANUP) \
    /* Metadata server monitoring */ \
    f(PING) /*  Print out chunkserves and their configs */ \
    f(STATS) /*  Print out whatever statistics/counters we have */ \
    f(RECOMPUTE_DIRSIZE) /* Do a top-down size update */ \
    f(DUMP_CHUNKTOSERVERMAP) /* Dump out the chunk -> location map */ \
    f(DUMP_CHUNKREPLICATIONCANDIDATES) /* Dump out the list of chunks being re-replicated */ \
    f(FSCK) /*  Check all blocks and report files that have missing blocks */ \
    f(CHECK_LEASES) /* Check all the leases and clear out expired ones */ \
    f(OPEN_FILES) /* Print out open files---for which there is a valid read/write lease */ \
    f(UPSERVERS) /* Print out live chunk servers */ \
    f(LOG_MAKE_CHUNK_STABLE) /*  Emit log record with chunk length and checksum */ \
    f(LOG_MAKE_CHUNK_STABLE_DONE) /*  Emit log record with successful completion of make chunk stable. */ \
    f(SET_CHUNK_SERVERS_PROPERTIES) \
    f(CHUNK_SERVER_RESTART) \
    f(CHUNK_SET_PROPERTIES) \
    f(GET_CHUNK_SERVERS_COUNTERS) \
    f(LOG_CHUNK_VERSION_CHANGE) \
    f(GET_REQUEST_COUNTERS) \
    f(CHECKPOINT) \
    f(DISCONNECT) \
    f(GETPATHNAME) \
    f(CHUNK_EVACUATE) \
    f(CHMOD) \
    f(CHOWN) \
    f(CHUNK_AVAILABLE) \
    f(CHUNKDIR_INFO) \
    f(GET_CHUNK_SERVER_DIRS_COUNTERS) \
    f(AUTHENTICATE) \
    f(DELEGATE) \
    f(DELEGATE_CANCEL) \
    f(SET_FILE_SYSTEM_INFO) \
    f(FORCE_CHUNK_REPLICATION) \
    f(LOG_GROUP_USERS) \
    f(LOG_CONFIG) \
    f(ACK)

enum MetaOp {
#define KfsMakeMetaOpEnumEntry(name) META_##name,
    KfsForEachMetaOpId(KfsMakeMetaOpEnumEntry)
#undef KfsMakeMetaOpEnumEntry
    META_NUM_OPS_COUNT // must be the last one
};


class ChunkServer;
class ClientSM;
typedef boost::shared_ptr<ChunkServer> ChunkServerPtr;
typedef DynamicArray<chunkId_t, 8> ChunkIdQueue;
typedef ReqOstreamT<ostream> ReqOstream;

/*!
 * \brief Meta request base class
 */
struct MetaRequest {
    typedef vector<
        ChunkServerPtr,
        StdAllocator<ChunkServerPtr>
    > Servers;

    class Display
    {
    public:
        Display(const MetaRequest& req)
            : mReq(req)
            {}
        Display(const Display& other)
            : mReq(other.mReq)
            {}
        ostream& Show(ostream& os) const
            { return mReq.ShowSelf(os); }
    private:
        const MetaRequest& mReq;
    };

    enum LogAction
    {
        kLogNever,
        kLogIfOk,
        kLogAlways
    };
    const MetaOp    op;              //!< type of request
    int             status;          //!< returned status
    int             clientProtoVers; //!< protocol version # sent by client
    int             submitCount;     //!< for time tracking.
    int64_t         submitTime;      //!< to time requests, optional.
    int64_t         processTime;     //!< same as previous
    string          statusMsg;       //!< optional human readable status message
    seq_t           opSeqno;         //!< command sequence # sent by the client
    seq_t           seqno;           //!< sequence no. global ordering
    seq_t           logseq;          //!< sequence no. in log
    LogAction       logAction;       //!< mutates metatree
    bool            suspended;       //!< is this request suspended somewhere
    bool            fromChunkServerFlag;
    bool            validDelegationFlag;
    bool            fromClientSMFlag;
    bool            shortRpcFormatFlag;
    bool            replayFlag;
    bool            commitPendingFlag;
    string          clientIp;
    IOBuffer        reqHeaders;
    kfsUid_t        authUid;
    kfsGid_t        authGid;
    kfsUid_t        euser;
    kfsGid_t        egroup;
    int64_t         maxWaitMillisec;
    int64_t         sessionEndTime;
    MetaRequest*    next;
    KfsCallbackObj* clnt;            //!< a handle to the client that generated this request.
    MetaRequest(MetaOp o, bool mu, seq_t opSeq = -1)
        : op(o),
          status(0),
          clientProtoVers(0),
          submitCount(0),
          submitTime(0),
          processTime(0),
          statusMsg(),
          opSeqno(opSeq),
          seqno(0),
          logseq(-1),
          logAction(mu ? kLogIfOk : kLogNever),
          suspended(false),
          fromChunkServerFlag(false),
          validDelegationFlag(false),
          fromClientSMFlag(false),
          shortRpcFormatFlag(false),
          replayFlag(false),
          commitPendingFlag(false),
          clientIp(),
          reqHeaders(),
          authUid(kKfsUserNone),
          authGid(kKfsGroupNone),
          euser(kKfsUserNone),
          egroup(kKfsGroupNone),
          maxWaitMillisec(-1),
          sessionEndTime(),
          next(0),
          clnt(0)
        { MetaRequest::Init(); }
    static void Release(MetaRequest* req)
    {
        if (req) {
            req->ReleaseSelf();
        }
    }
    virtual bool start() { return false; }
    virtual void handle();
    //!< when an op finishes execution, we send a response back to
    //!< the client.  This function should generate the appropriate
    //!< response to be sent back as per the KFS protocol.
    virtual void response(ReqOstream& os, IOBuffer& /* buf */) { response(os); }
    virtual int log(ostream &file) const { return 0; }
    Display Show() const { return Display(*this); }
    virtual void setChunkServer(const ChunkServerPtr& /* cs */) {};
    bool ValidateRequestHeader(
        const char* name,
        size_t      nameLen,
        const char* header,
        size_t      headerLen,
        bool        hasChecksum,
        uint32_t    checksum,
        bool        shortFieldNamesFlag)
    {
        shortRpcFormatFlag = shortFieldNamesFlag;
        return (
            hasChecksum ?
            (! sVerifyHeaderChecksumFlag ||
            Checksum(name, nameLen, header, headerLen) == checksum) :
            (! sRequireHeaderChecksumFlag || kLogNever == logAction)
        );
    }
    bool HandleUnknownField(
        const char* /* key */, size_t /* keyLen */,
        const char* /* val */, size_t /* valLen */)
        { return true; }
    template<typename T> static T& ParserDef(T& parser)
    {
        return parser
        .Def2("Cseq",                    "c", &MetaRequest::opSeqno,           seq_t(-1))
        .Def2("Client-Protocol-Version", "p", &MetaRequest::clientProtoVers,      int(0))
        .Def2("From-chunk-server",       "s", &MetaRequest::fromChunkServerFlag,   false)
        .Def2("UserId",                  "u", &MetaRequest::euser,          kKfsUserNone)
        .Def2("GroupId",                 "g", &MetaRequest::egroup,        kKfsGroupNone)
        .Def2("Max-wait-ms",             "w", &MetaRequest::maxWaitMillisec, int64_t(-1))
        ;
    }
    template<typename T> static T& IoParserDef(T& parser)
    {
        return parser
        .Def("u", &MetaRequest::euser,     kKfsUserNone)
        .Def("a", &MetaRequest::authUid,   kKfsUserNone)
        .Def("s", &MetaRequest::status,    0)
        .Def("m", &MetaRequest::statusMsg, string())
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return parser
        .Def("u", &MetaRequest::euser,               kKfsUserNone)
        .Def("g", &MetaRequest::egroup,              kKfsGroupNone)
        .Def("a", &MetaRequest::authUid,             kKfsUserNone)
        .Def("s", &MetaRequest::fromChunkServerFlag, false)
        ;
    }
    virtual ostream& ShowSelf(ostream& os) const = 0;
    static void SetParameters(const Properties& props);
    static uint32_t Checksum(
        const char* name,
        size_t      nameLen,
        const char* header,
        size_t      headerLen);
    static int GetRequestCount()
        { return sMetaRequestCount; }
    static Display ShowReq(const MetaRequest* req)
    {
        return (req ? *req : GetNullReq()).Show();
    }
    virtual bool dispatch(ClientSM& /* sm */)
        { return false; }
    template<typename T>
    bool ParseInt(
        const char*& ioPtr,
        size_t       inLen,
        T&           outValue)
    {
        return (shortRpcFormatFlag ?
            HexIntParser::Parse(ioPtr, inLen, outValue) :
            DecIntParser::Parse(ioPtr, inLen, outValue));
    }
    bool Write(ostream& os, bool omitDefaultsFlag = false) const;
    bool WriteLog(ostream& os, bool omitDefaultsFlag) const;
    static bool Replay(const char* buf, size_t len, int& status);
    static MetaRequest* Read(const char* buf, size_t len);
    static int GetId(const TokenValue& name);
    static TokenValue GetName(int id);
protected:
    virtual void response(ReqOstream& /* os */) {}
    virtual ~MetaRequest();
    virtual void ReleaseSelf()
        { delete this; }
private:
    MetaRequest* mPrevPtr[1];
    MetaRequest* mNextPtr[1];

    static bool         sRequireHeaderChecksumFlag;
    static bool         sVerifyHeaderChecksumFlag;
    static int          sMetaRequestCount;
    static MetaRequest* sMetaRequestsPtr[1];

    friend class QCDLListOp<MetaRequest, 0>;
    typedef QCDLList<MetaRequest, 0> MetaRequestsList;
    void Init();
    static const MetaRequest& GetNullReq();
};
inline static ostream& operator<<(ostream& os, const MetaRequest::Display& disp)
{ return disp.Show(os); }

void submit_request(MetaRequest *r);

struct MetaIdempotentRequest : public MetaRequest {
    // Derived classes' request() method must be const, i.e. not modify "this".
    seq_t reqId;
    seq_t ackId;
    MetaIdempotentRequest(MetaOp o, bool mu, seq_t opSeq = -1, int rc = 1)
        : MetaRequest(o, mu, opSeq),
          reqId(-1),
          ackId(-1),
          ref(rc),
          req(0)
        { logAction = kLogAlways; }
    void SetReq(MetaIdempotentRequest* r)
    {
        if (r) {
            r->Ref();
        }
        MetaIdempotentRequest* const pr = req;
        req = r;
        if (pr) {
            pr->UnRef();
        }
    }
    MetaIdempotentRequest* GetReq() const
        { return req; }
    bool WriteLog(ostream& os) const;
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Rid", "r", &MetaIdempotentRequest::reqId, seq_t(-1))
        ;
    }
    template<typename T> static T& IoParserDef(T& parser)
    {
        return MetaRequest::IoParserDef(parser)
        .Def("r", &MetaIdempotentRequest::reqId, seq_t(-1))
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("r", &MetaIdempotentRequest::reqId, seq_t(-1))
        ;
    }
protected:
    bool IdempotentAck(ReqOstream& os);
    void Ref() { SyncAddAndFetch(ref, 1); }
    void UnRef()
    {
        if (SyncAddAndFetch(ref, -1) <= 0) {
            delete this;
        }
    }
    virtual ~MetaIdempotentRequest();
    virtual void ReleaseSelf() { UnRef(); }
    inline bool IsHandled();
    inline bool IsLogNeeded() const;
private:
    volatile int           ref;
    MetaIdempotentRequest* req;
};

/*!
 * \brief look up a file name
 */
struct MetaLookup: public MetaRequest {
    fid_t  dir;      //!< parent directory fid
    string name;     //!< name to look up
    int    authType; //!< io auth type
    bool   authInfoOnlyFlag;
    MFattr fattr;
    MetaLookup()
        : MetaRequest(META_LOOKUP, false),
          dir(-1),
          name(),
          authType(kAuthenticationTypeUndef),
          authInfoOnlyFlag(false),
          fattr()
        {}
    virtual void handle();
    virtual void response(ReqOstream& os);
    virtual bool dispatch(ClientSM& sm);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "lookup:"
            " name: "   << name <<
            " parent: " << dir
        ;
    }
    bool Validate()
    {
        return (dir >= 0 && ! name.empty());
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Short-rpc-fmt",      "f", &MetaRequest::shortRpcFormatFlag)
        .Def2("Parent File-handle", "P", &MetaLookup::dir,              fid_t(-1))
        .Def2("Filename",           "N", &MetaLookup::name              )
        .Def2("Auth-type",          "A", &MetaLookup::authType,         int(kAuthenticationTypeUndef))
        .Def2("Auth-info-only",     "I", &MetaLookup::authInfoOnlyFlag, false)
        ;
    }
    bool IsAuthNegotiation() const
    {
        return (authType != kAuthenticationTypeUndef &&
            dir == ROOTFID && name == "/");
    }
};

/*!
 * \brief look up a complete path
 */
struct MetaLookupPath: public MetaRequest {
    fid_t  root;   //!< fid of starting directory
    string path;   //!< path to look up
    MFattr fattr;
    MetaLookupPath()
        : MetaRequest(META_LOOKUP_PATH, false),
          root(-1),
          path(),
          fattr()
        {}
    virtual void handle();
    virtual void response(ReqOstream& os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "lookup path:"
            " path: " << path <<
            " root: " << root
        ;
    }
    bool Validate()
    {
        return (root >= 0 && ! path.empty());
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Root File-handle", "P", &MetaLookupPath::root, fid_t(-1))
        .Def2("Pathname",         "N", &MetaLookupPath::path           )
        ;
    }
};

/*!
 * \brief create a file
 */
struct MetaCreate: public MetaIdempotentRequest {
    fid_t      dir;                 //!< parent directory fid
    fid_t      fid;                 //!< file ID of new file
    int16_t    numReplicas;         //!< desired degree of replication
    int32_t    striperType;
    int32_t    numStripes;
    int32_t    numRecoveryStripes;
    int32_t    stripeSize;
    bool       exclusive;           //!< model the O_EXCL flag
    fid_t      todumpster;          //!< moved existing to dumpster
    kfsUid_t   user;
    kfsGid_t   group;
    kfsMode_t  mode;
    kfsSTier_t minSTier;
    kfsSTier_t maxSTier;
    string     name;                //!< name to create
    string     ownerName;
    string     groupName;
    MetaCreate()
        : MetaIdempotentRequest(META_CREATE, true),
          dir(-1),
          fid(-1),
          numReplicas(1),
          striperType(KFS_STRIPED_FILE_TYPE_NONE),
          numStripes(0),
          numRecoveryStripes(0),
          stripeSize(0),
          exclusive(false),
          todumpster(-1),
          user(kKfsUserNone),
          group(kKfsGroupNone),
          mode(kKfsModeUndef),
          minSTier(kKfsSTierMax),
          maxSTier(kKfsSTierMax),
          name(),
          ownerName(),
          groupName()
        {}
    virtual bool start();
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "create:"
            " name: "        << name <<
            " parent: "      << dir <<
            " replication: " << numReplicas <<
            " striper: "     << striperType <<
            " stripes: "     << numStripes <<
            " recovery: "    << numRecoveryStripes <<
            " stripe-size: " << stripeSize <<
            " todumpster: "  << todumpster <<
            " user: "        << user <<
            " group: "       << group <<
            " mode: "        << oct << mode << dec
        ;
    }
    bool Validate();
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaIdempotentRequest::ParserDef(parser)
        .Def2("Parent File-handle",   "P",  &MetaCreate::dir,                fid_t(-1))
        .Def2("Num-replicas",         "R",  &MetaCreate::numReplicas,        int16_t( 1))
        .Def2("Striper-type",         "ST", &MetaCreate::striperType,        int32_t(KFS_STRIPED_FILE_TYPE_NONE))
        .Def2("Num-stripes",          "SN", &MetaCreate::numStripes,         int32_t(0))
        .Def2("Num-recovery-stripes", "SR", &MetaCreate::numRecoveryStripes, int32_t(0))
        .Def2("Stripe-size",          "SS", &MetaCreate::stripeSize,         int32_t(0))
        .Def2("Exclusive",            "E",  &MetaCreate::exclusive,          false)
        .Def2("Filename",             "N",  &MetaCreate::name                     )
        .Def2("Owner",                "O",  &MetaCreate::user,               kKfsUserNone)
        .Def2("Group",                "G",  &MetaCreate::group,              kKfsGroupNone)
        .Def2("Mode",                 "M",  &MetaCreate::mode,               kKfsModeUndef)
        .Def2("Min-tier",             "TL", &MetaCreate::minSTier,           kKfsSTierMax)
        .Def2("Max-tier",             "TH", &MetaCreate::maxSTier,           kKfsSTierMax)
        .Def2("OName",                "ON", &MetaCreate::ownerName)
        .Def2("GName",                "GN", &MetaCreate::groupName)
        ;
    }
    template<typename T> static T& IoParserDef(T& parser)
    {
        // Make every response field persistent.
        // Keep parent directory, replication, and name for debugging.
        return MetaIdempotentRequest::IoParserDef(parser)
        .Def("P",  &MetaCreate::dir,         fid_t(-1))
        .Def("R",  &MetaCreate::numReplicas, int16_t( 1))
        .Def("N",  &MetaCreate::name)
        .Def("H",  &MetaCreate::fid,         fid_t(-1))
        .Def("ST", &MetaCreate::striperType, int32_t(KFS_STRIPED_FILE_TYPE_NONE))
        .Def("U",  &MetaCreate::user,        kKfsUserNone)
        .Def("G",  &MetaCreate::group,       kKfsUserNone)
        .Def("M",  &MetaCreate::mode,        kKfsModeUndef)
        .Def("TL", &MetaCreate::minSTier,    kKfsSTierMax)
        .Def("TH", &MetaCreate::maxSTier,    kKfsSTierMax)
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaIdempotentRequest::LogIoDef(parser)
        .Def("P",  &MetaCreate::dir,                fid_t(-1))
        .Def("R",  &MetaCreate::numReplicas,        int16_t( 1))
        .Def("ST", &MetaCreate::striperType,        int32_t(KFS_STRIPED_FILE_TYPE_NONE))
        .Def("SN", &MetaCreate::numStripes,         int32_t(0))
        .Def("SR", &MetaCreate::numRecoveryStripes, int32_t(0))
        .Def("SS", &MetaCreate::stripeSize,         int32_t(0))
        .Def("E",  &MetaCreate::exclusive,          false)
        .Def("N",  &MetaCreate::name                     )
        .Def("O",  &MetaCreate::user,               kKfsUserNone)
        .Def("G",  &MetaCreate::group,              kKfsGroupNone)
        .Def("M",  &MetaCreate::mode,               kKfsModeUndef)
        .Def("TL", &MetaCreate::minSTier,           kKfsSTierMax)
        .Def("TH", &MetaCreate::maxSTier,           kKfsSTierMax)
        ;
    }
};

/*!
 * \brief create a directory
 */
struct MetaMkdir: public MetaIdempotentRequest {
    fid_t      dir;  //!< parent directory fid
    fid_t      fid;  //!< file ID of new directory
    kfsUid_t   user;
    kfsGid_t   group;
    kfsMode_t  mode;
    kfsSTier_t minSTier;
    kfsSTier_t maxSTier;
    string     name; //!< name to create
    string     ownerName;
    string     groupName;
    MetaMkdir()
        : MetaIdempotentRequest(META_MKDIR, true),
          dir(-1),
          fid(-1),
          user(kKfsUserNone),
          group(kKfsGroupNone),
          mode(kKfsModeUndef),
          minSTier(kKfsSTierMax),
          maxSTier(kKfsSTierMax),
          name(),
          ownerName(),
          groupName()
        {}
    virtual bool start();
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "mkdir:"
            " name: "   << name <<
            " parent: " << dir  <<
            " user: "   << user <<
            " group: "  << group <<
            " mode: "   << oct << mode << dec <<
            " euser: "  << euser <<
            " egroup: " << egroup
        ;
    }
    bool Validate()
    {
        return (dir >= 0 && ! name.empty());
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaIdempotentRequest::ParserDef(parser)
        .Def2("Parent File-handle", "P",  &MetaMkdir::dir, fid_t(-1))
        .Def2("Directory",          "N",  &MetaMkdir::name          )
        .Def2("Owner",              "O",  &MetaMkdir::user,   kKfsUserNone)
        .Def2("Group",              "G",  &MetaMkdir::group,  kKfsGroupNone)
        .Def2("Mode",               "M",  &MetaMkdir::mode,   kKfsModeUndef)
        .Def2("OName",              "ON", &MetaMkdir::ownerName)
        .Def2("GName",              "GN", &MetaMkdir::groupName)
        ;
    }
    template<typename T> static T& IoParserDef(T& parser)
    {
        // Make every response field persistent.
        // Keep parent directory and name for debugging.
        return MetaIdempotentRequest::IoParserDef(parser)
        .Def("P",  &MetaMkdir::dir,      fid_t(-1))
        .Def("N",  &MetaMkdir::name)
        .Def("H",  &MetaMkdir::fid,      fid_t(-1))
        .Def("U",  &MetaMkdir::user,     kKfsUserNone)
        .Def("G",  &MetaMkdir::group,    kKfsUserNone)
        .Def("M",  &MetaMkdir::mode,     kKfsModeUndef)
        .Def("TL", &MetaMkdir::minSTier, kKfsSTierMax)
        .Def("TH", &MetaMkdir::maxSTier, kKfsSTierMax)
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaIdempotentRequest::LogIoDef(parser)
        .Def("P",  &MetaMkdir::dir,      fid_t(-1))
        .Def("N",  &MetaMkdir::name)
        .Def("U",  &MetaMkdir::user,     kKfsUserNone)
        .Def("G",  &MetaMkdir::group,    kKfsUserNone)
        .Def("M",  &MetaMkdir::mode,     kKfsModeUndef)
        .Def("TL", &MetaMkdir::minSTier, kKfsSTierMax)
        .Def("TH", &MetaMkdir::maxSTier, kKfsSTierMax)
        ;
    }
};

/*!
 * \brief remove a file
 */
struct MetaRemove: public MetaIdempotentRequest {
    fid_t    dir;      //!< parent directory fid
    string   name;     //!< name to remove
    string   pathname; //!< full pathname to remove
    fid_t    todumpster;
    MetaRemove()
        : MetaIdempotentRequest(META_REMOVE, true),
          dir(-1),
          name(),
          pathname(),
          todumpster(-1)
        {}
    virtual bool start();
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "remove:"
            " path: "       << pathname <<
            " name: "       << name <<
            " dir: "        << dir <<
            " todumpster: " << todumpster
        ;
    }
    bool Validate()
    {
        return (dir >= 0 && ! name.empty());
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaIdempotentRequest::ParserDef(parser)
        .Def2("Parent File-handle", "P",  &MetaRemove::dir, fid_t(-1))
        .Def2("Filename",           "N",  &MetaRemove::name          )
        .Def2("Pathname",           "PN", &MetaRemove::pathname      )
        ;
    }
    template<typename T> static T& IoParserDef(T& parser)
    {
        // Keep parent directory and name for debugging.
        return MetaIdempotentRequest::IoParserDef(parser)
        .Def("P", &MetaRemove::dir, fid_t(-1))
        .Def("N", &MetaRemove::name)
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaIdempotentRequest::LogIoDef(parser)
        .Def("P", &MetaRemove::dir, fid_t(-1))
        .Def("N", &MetaRemove::name)
        ;
    }
};

/*!
 * \brief remove a directory
 */
struct MetaRmdir: public MetaIdempotentRequest {
    fid_t  dir;      //!< parent directory fid
    string name;     //!< name to remove
    string pathname; //!< full pathname to remove
    MetaRmdir()
        : MetaIdempotentRequest(META_RMDIR, true),
          dir(-1),
          name(),
          pathname()
        {}
    virtual bool start();
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "rmdir:"
            " path: "   << pathname <<
            " name: "   << name <<
            " parent: " << dir
        ;
    }
    bool Validate()
    {
        return (dir >= 0 && ! name.empty());
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaIdempotentRequest::ParserDef(parser)
        .Def2("Parent File-handle", "P",  &MetaRmdir::dir, fid_t(-1))
        .Def2("Directory",          "N",  &MetaRmdir::name          )
        .Def2("Pathname",           "PN", &MetaRmdir::pathname      )
        ;
    }
    template<typename T> static T& IoParserDef(T& parser)
    {
        // Keep parent directory and name for debugging.
        return MetaIdempotentRequest::IoParserDef(parser)
        .Def("P", &MetaRmdir::dir, fid_t(-1))
        .Def("N", &MetaRmdir::name)
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaIdempotentRequest::LogIoDef(parser)
        .Def("P", &MetaRmdir::dir, fid_t(-1))
        .Def("N", &MetaRmdir::name)
        ;
    }
};

/*!
 * \brief read directory contents
 */
struct MetaReaddir: public MetaRequest {
    fid_t    dir; //!< directory to read
    IOBuffer resp;
    int      numEntries;
    bool     hasMoreEntriesFlag;
    string   fnameStart;
    MetaReaddir()
        : MetaRequest(META_READDIR, false),
          dir(-1),
          resp(),
          numEntries(-1),
          hasMoreEntriesFlag(false),
          fnameStart()
        {}
    virtual void handle();
    virtual void response(ReqOstream& os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "readdir: dir: " << dir;
    }
    bool Validate()
    {
        return (dir >= 0);
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Directory File-handle", "P", &MetaReaddir::dir,       fid_t(-1))
        .Def2("Max-entries",           "M", &MetaReaddir::numEntries,        0)
        .Def2("Fname-start",           "S", &MetaReaddir::fnameStart)
        ;
    }
};

typedef vector<
    ServerLocation,
    StdAllocator<ServerLocation>
> ServerLocations;

/*!
 * \brief layout information for a chunk
 */
struct ChunkLayoutInfo {
    chunkOff_t      offset;       //!< offset of chunk within file
    chunkId_t       chunkId;      //!< Id of the chunk corresponding to offset
    seq_t           chunkVersion; //!< version # assigned to this chunk
    ServerLocations locations;    //!< where the copies of the chunks are
    ostream& show(ostream& os) const
    {
        os << offset <<
            " " << chunkId <<
            " " << chunkVersion <<
            " " << locations.size();
        for (ServerLocations::size_type i = 0;
                i < locations.size();
                ++i) {
            os <<
                " " << locations[i].hostname <<
                " " << locations[i].port;
        }
        return os;
    }
};

inline static ostream& operator<<(ostream& os, const ChunkLayoutInfo& li) {
    return li.show(os);
}

/*!
 * \brief read directory contents and get file attributes
 */
struct MetaReaddirPlus: public MetaRequest {
    struct DEntry : public MFattr
    {
        DEntry()
            : MFattr(),
              name()
            {}
        DEntry(const MFattr& fa, const string& n)
            : MFattr(fa),
              name(n)
            {}
        string name;
    };
    typedef vector<DEntry,          StdAllocator<DEntry>          > DEntries;
    typedef vector<ChunkLayoutInfo, StdAllocator<ChunkLayoutInfo> > CInfos;

    fid_t    dir;        //!< directory to read
    int      numEntries; //!< max number of entres to return
    int      maxRespSize;
    bool     getLastChunkInfoOnlyIfSizeUnknown;
    bool     omitLastChunkInfoFlag;
    bool     fileIdAndTypeOnlyFlag;
    bool     hasMoreEntriesFlag;
    bool     noAttrsFlag;
    int64_t  ioBufPending;
    string   fnameStart;
    DEntries dentries;
    CInfos   lastChunkInfos;

    MetaReaddirPlus()
        : MetaRequest(META_READDIRPLUS, false),
          dir(-1),
          numEntries(-1),
          maxRespSize(-1),
          getLastChunkInfoOnlyIfSizeUnknown(false),
          omitLastChunkInfoFlag(false),
          fileIdAndTypeOnlyFlag(false),
          hasMoreEntriesFlag(false),
          noAttrsFlag(false),
          ioBufPending(0),
          fnameStart(),
          dentries(),
          lastChunkInfos()
        {}
    ~MetaReaddirPlus();
    virtual void handle();
    virtual void response(ReqOstream& os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "readdir plus: dir: " << dir;
    }
    bool Validate()
    {
        return (dir >= 0);
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Directory File-handle", "P", &MetaReaddirPlus::dir, fid_t(-1))
        .Def2("GetLastChunkInfoOnlyIfSizeUnknown", "LC",
            &MetaReaddirPlus::getLastChunkInfoOnlyIfSizeUnknown, false)
        .Def2("Max-entries", "M", &MetaReaddirPlus::numEntries,  0)
        .Def2("Fname-start", "S", &MetaReaddirPlus::fnameStart)
        .Def2("Omit-lci", "O",
            &MetaReaddirPlus::omitLastChunkInfoFlag, false)
        .Def2("FidT-only", "F",
            &MetaReaddirPlus::fileIdAndTypeOnlyFlag, false)
        ;
    }
};

/*!
 * \brief get allocation info. a chunk for a file
 */
struct MetaGetalloc: public MetaRequest {
    fid_t           fid;          //!< file for alloc info is needed
    chunkOff_t      offset;       //!< offset of chunk within file
    chunkId_t       chunkId;      //!< Id of the chunk corresponding to offset
    seq_t           chunkVersion; //!< version # assigned to this chunk
    ServerLocations locations;    //!< where the copies of the chunks are
    StringBufT<256> pathname;     //!< pathname of the file (useful to print in debug msgs)
    bool            replicasOrderedFlag;
    bool            allChunkServersShortRpcFlag;
    MetaGetalloc()
        : MetaRequest(META_GETALLOC, false),
          fid(-1),
          offset(-1),
          chunkId(-1),
          chunkVersion(-1),
          locations(),
          pathname(),
          replicasOrderedFlag(false),
          allChunkServersShortRpcFlag(false)
        {}
    virtual void handle();
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "getalloc:"
            " fid: "    << fid <<
            " offset: " << offset <<
            " path: "   << pathname
        ;
    }
    bool Validate()
    {
        return (fid >= 0 && offset >= 0);
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("File-handle",  "P", &MetaGetalloc::fid,          fid_t(-1))
        .Def2("Chunk-offset", "O", &MetaGetalloc::offset,  chunkOff_t(-1))
        .Def2("Pathname",     "N", &MetaGetalloc::pathname               )
        ;
    }
};

/*!
 * \brief get allocation info. for all chunks of a file
 */
struct MetaGetlayout: public MetaRequest {
    fid_t      fid; //!< file for layout info is needed
    chunkOff_t startOffset;
    bool       omitLocationsFlag;
    bool       lastChunkInfoOnlyFlag;
    bool       continueIfNoReplicasFlag;
    int        maxResCnt;
    int        numChunks;
    bool       hasMoreChunksFlag;
    bool       allChunkServersShortRpcFlag;
    chunkOff_t fileSize;
    IOBuffer   resp;   //!< result
    MetaGetlayout()
        : MetaRequest(META_GETLAYOUT, false),
          fid(-1),
          startOffset(0),
          omitLocationsFlag(false),
          lastChunkInfoOnlyFlag(false),
          continueIfNoReplicasFlag(false),
          maxResCnt(-1),
          numChunks(-1),
          hasMoreChunksFlag(false),
          allChunkServersShortRpcFlag(false),
          fileSize(-1),
          resp()
        {}
    virtual void handle();
    virtual void response(ReqOstream &os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "getlayout: fid: " << fid;
    }
    bool Validate()
    {
        return (fid >= 0);
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("File-handle",             "P", &MetaGetlayout::fid,                      fid_t(-1))
        .Def2("Start-offset",            "S", &MetaGetlayout::startOffset,              chunkOff_t(0))
        .Def2("Omit-locations",          "O", &MetaGetlayout::omitLocationsFlag,        false)
        .Def2("Last-chunk-only",         "L", &MetaGetlayout::lastChunkInfoOnlyFlag,    false)
        .Def2("Max-chunks",              "M", &MetaGetlayout::maxResCnt,                -1)
        .Def2("Continue-if-no-replicas", "R", &MetaGetlayout::continueIfNoReplicasFlag, false)
        ;
    }
};

/*!
 * \brief Op for relinquishing a lease on a chunk of a file.
 */
struct MetaLeaseRelinquish: public MetaRequest {
    LeaseType  leaseType; //!< input
    chunkId_t  chunkId;   //!< input
    int64_t    leaseId;   //!< input
    chunkOff_t chunkSize;
    bool       hasChunkChecksum;
    uint32_t   chunkChecksum;
    MetaLeaseRelinquish()
        : MetaRequest(META_LEASE_RELINQUISH, false),
          leaseType(READ_LEASE),
          chunkId(-1),
          leaseId(-1),
          chunkSize(-1),
          hasChunkChecksum(false),
          chunkChecksum(0),
          chunkChecksumHdr(-1),
          leaseTypeStr()
         {}
    virtual void handle();
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
        "relinquish " <<
            (leaseType == READ_LEASE ? "read" : "write") <<
        " lease: "     << leaseId <<
        " chunk: "     << chunkId <<
        " chunkSize: " << chunkSize <<
        " checksum: "  << (hasChunkChecksum ?
            int64_t(chunkChecksum) : int64_t(-1))
        ;
    }
    bool Validate()
    {
        leaseType = (leaseTypeStr == "WRITE_LEASE") ? WRITE_LEASE : READ_LEASE;
        if (leaseType == READ_LEASE && leaseTypeStr != "READ_LEASE") {
            return false;
        }
        hasChunkChecksum = chunkChecksumHdr >= 0;
        chunkChecksum    = hasChunkChecksum ?
            (uint32_t)chunkChecksumHdr : (uint32_t)0;
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Lease-type",     "T", &MetaLeaseRelinquish::leaseTypeStr                 )
        .Def2("Chunk-handle",   "H", &MetaLeaseRelinquish::chunkId,        chunkId_t(-1))
        .Def2("Lease-id",       "L", &MetaLeaseRelinquish::leaseId,          int64_t(-1))
        .Def2("Chunk-size",     "S", &MetaLeaseRelinquish::chunkSize,     chunkOff_t(-1))
        .Def2("Chunk-checksum", "K", &MetaLeaseRelinquish::chunkChecksumHdr, int64_t(-1))
        ;
    }
private:
    int64_t        chunkChecksumHdr;
    StringBufT<32> leaseTypeStr;
};

struct MetaChunkAllocate;

/*!
 * \brief allocate a chunk for a file
 */
struct MetaAllocate: public MetaRequest, public  KfsCallbackObj {
    typedef DelegationToken::TokenSeq TokenSeq;

    fid_t                fid;          //!< file for which space has to be allocated
    chunkOff_t           offset;       //!< offset of chunk within file
    chunkId_t            chunkId;      //!< Id of the chunk that was allocated
    seq_t                chunkVersion; //!< version # assigned to this chunk
    seq_t                initialChunkVersion;
    int16_t              numReplicas;  //!< inherited from file's fattr
    bool                 stripedFileFlag;
    bool                 layoutDone;   //!< Has layout of chunk been done
    //!< when set, the allocation request is asking the metaserver to append
    //!< a chunk to the file and let the client know the offset at which it was
    //!< appended.
    bool                 appendChunk;
    //!< Write append only: the space reservation size that will follow the
    //!< chunk allocation.
    int                  spaceReservationSize;
    //!< Suggested max # of concurrent appenders per chunk
    int                  maxAppendersPerChunk;
    //!< Server(s) on which this chunk has been placed
    Servers              servers;
    uint32_t             numServerReplies;
    int                  firstFailedServerIdx;
    bool                 invalidateAllFlag;
    const FAPermissions* permissions;
    MetaAllocate*        next;
    int64_t              leaseId;
    chunkOff_t           chunkBlockStart;
    MetaLeaseRelinquish* pendingLeaseRelinquish;
    kfsSTier_t           minSTier;
    kfsSTier_t           maxSTier;
    string               responseStr; // Cached response
    string               responseAccessStr;
    bool                 writeMasterKeyValidFlag;
    bool                 clientCSAllowClearTextFlag;
    bool                 allChunkServersShortRpcFlag;
    TokenSeq             tokenSeq;
    time_t               issuedTime;
    int                  validForTime;
    CryptoKeys::KeyId    writeMasterKeyId;
    CryptoKeys::Key      writeMasterKey;
    TokenSeq             delegationSeq;
    uint32_t             delegationValidForTime;
    uint16_t             delegationFlags;
    int64_t              delegationIssuedTime;
    // With StringBufT instead of string the append allocation (presently
    // the most frequent allocation type) saves malloc() calls.
    StringBufT<64>       clientHost;   //!< the host from which request was received
    StringBufT<256>      pathname;     //!< full pathname that corresponds to fid
    MetaAllocate(seq_t s = -1, fid_t f = -1, chunkOff_t o = -1)
        : MetaRequest(META_ALLOCATE, true, s),
          KfsCallbackObj(),
          fid(f),
          offset(o),
          chunkId(-1),
          chunkVersion(-1),
          initialChunkVersion(-1),
          numReplicas(0),
          stripedFileFlag(false),
          layoutDone(false),
          appendChunk(false),
          spaceReservationSize(1 << 20),
          maxAppendersPerChunk(64),
          servers(),
          numServerReplies(0),
          firstFailedServerIdx(-1),
          invalidateAllFlag(false),
          permissions(0),
          next(0),
          leaseId(-1),
          chunkBlockStart(-1),
          pendingLeaseRelinquish(0),
          minSTier(kKfsSTierMax),
          maxSTier(kKfsSTierMax),
          responseStr(),
          responseAccessStr(),
          writeMasterKeyValidFlag(false),
          clientCSAllowClearTextFlag(false),
          allChunkServersShortRpcFlag(false),
          tokenSeq(),
          issuedTime(),
          validForTime(0),
          writeMasterKeyId(),
          writeMasterKey(),
          delegationSeq(-1),
          delegationValidForTime(0),
          delegationFlags(0),
          delegationIssuedTime(0),
          clientHost(),
          pathname(),
          origClnt(0)
    {
        SET_HANDLER(this, &MetaAllocate::logOrLeaseRelinquishDone);
    }
    virtual ~MetaAllocate()
        { delete pendingLeaseRelinquish; }
    virtual bool start();
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const;
    void responseSelf(ReqOstream &os);
    void LayoutDone(int64_t chunkAllocProcessTime);
    int logOrLeaseRelinquishDone(int code, void *data);
    int CheckStatus(bool forceFlag = false) const;
    bool ChunkAllocDone(const MetaChunkAllocate& chunkAlloc);
    void writeChunkAccess(ReqOstream& os);
    virtual bool dispatch(ClientSM& sm);
    bool Validate()
    {
        return (fid >= 0 && (offset >= 0 || appendChunk));
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("File-handle",    "P", &MetaAllocate::fid,                   fid_t(-1))
        .Def2("Chunk-append",   "A", &MetaAllocate::appendChunk,               false)
        .Def2("Chunk-offset",   "S", &MetaAllocate::offset,           chunkOff_t(-1))
        .Def2("Pathname",       "N", &MetaAllocate::pathname                        )
        .Def2("Client-host",    "H", &MetaAllocate::clientHost                      )
        .Def2("Space-reserve",  "R", &MetaAllocate::spaceReservationSize, int(1<<20))
        .Def2("Max-appenders",  "M", &MetaAllocate::maxAppendersPerChunk,    int(64))
        .Def2("Invalidate-all", "I", &MetaAllocate::invalidateAllFlag,         false)
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("P", &MetaAllocate::fid,                   fid_t(-1))
        .Def("A", &MetaAllocate::appendChunk,               false)
        .Def("S", &MetaAllocate::offset,           chunkOff_t(-1))
        .Def("I", &MetaAllocate::invalidateAllFlag,         false)
        ;
    }
private:
    KfsCallbackObj* origClnt;
    void LogDone(
        bool resumeFlag, bool countAllocTimeFlag, int64_t chunkAllocProcessTime);
};

/*!
 * \brief truncate a file
 */
struct MetaTruncate: public MetaRequest {
    fid_t           fid;      //!< file for which space has to be allocated
    chunkOff_t      offset;   //!< offset to truncate the file to
    chunkOff_t      endOffset;
    bool            setEofHintFlag; //!< set eof is the most frequently used
    //!< set if the blks from the beginning of the file to the offset have
    //!< to be deleted.
    bool            pruneBlksFromHead;
    bool            checkPermsFlag;
    StringBufT<256> pathname; //!< full pathname for file being truncated
    int64_t         mtime;
    MetaTruncate()
        : MetaRequest(META_TRUNCATE, true),
          fid(-1),
          offset(-1),
          endOffset(-1),
          setEofHintFlag(true),
          pruneBlksFromHead(false),
          checkPermsFlag(false),
          pathname(),
          mtime()
        {}
    virtual bool start();
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            (pruneBlksFromHead ?
                "prune from head:" : "truncate:") <<
            " path: "   << pathname <<
            " fid: "    << fid <<
            " offset: " << offset
        ;
    }
    bool Validate()
    {
        return (fid >= 0 && offset >= 0);
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("File-handle",     "P", &MetaTruncate::fid,                  fid_t(-1))
        .Def2("Offset",          "S", &MetaTruncate::offset,          chunkOff_t(-1))
        .Def2("Pathname",        "N", &MetaTruncate::pathname                       )
        .Def2("Prune-from-head", "H", &MetaTruncate::pruneBlksFromHead,        false)
        .Def2("End-offset",      "E", &MetaTruncate::endOffset,       chunkOff_t(-1))
        .Def2("Set-eof",         "O", &MetaTruncate::setEofHintFlag,            true)
        .Def2("Check-perms",     "M", &MetaTruncate::checkPermsFlag,           false)
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("P", &MetaTruncate::fid,                  fid_t(-1))
        .Def("S", &MetaTruncate::offset,          chunkOff_t(-1))
        .Def("H", &MetaTruncate::pruneBlksFromHead,        false)
        .Def("E", &MetaTruncate::endOffset,       chunkOff_t(-1))
        .Def("O", &MetaTruncate::setEofHintFlag,            true)
        .Def("M", &MetaTruncate::checkPermsFlag,           false)
        ;
    }
};

/*!
 * \brief rename a file or directory
 */
struct MetaRename: public MetaIdempotentRequest {
    fid_t  dir;        //!< parent directory
    string oldname;    //!< old file name
    string newname;    //!< new file name
    string oldpath;    //!< fully-qualified old pathname
    bool   overwrite;  //!< overwrite newname if it exists
    fid_t  todumpster; //!< moved original to dumpster
    MetaRename()
        : MetaIdempotentRequest(META_RENAME, true),
          dir(-1),
          oldname(),
          newname(),
          oldpath(),
          overwrite(false),
          todumpster(-1)
        {}
    virtual bool start();
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "rename:"
            " dir: "        << dir     <<
            " from: "       << oldpath <<
            " to: "         << newname <<
            " todumpster: " << todumpster
        ;
    }
    bool Validate()
    {
        return (dir >= 0 && ! oldname.empty() && ! newname.empty());
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaIdempotentRequest::ParserDef(parser)
        .Def2("Parent File-handle", "P", &MetaRename::dir,   fid_t(-1))
        .Def2("Old-name",           "O", &MetaRename::oldname         )
        .Def2("New-path",           "N", &MetaRename::newname         )
        .Def2("Old-path",           "F", &MetaRename::oldpath         )
        .Def2("Overwrite",          "W", &MetaRename::overwrite, false)
        ;
    }
    template<typename T> static T& IoParserDef(T& parser)
    {
        // Keep parent directory for debugging.
        return MetaIdempotentRequest::IoParserDef(parser)
        .Def("P", &MetaRename::dir, fid_t(-1))
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaIdempotentRequest::LogIoDef(parser)
        .Def("P", &MetaRename::dir,   fid_t(-1))
        .Def("O", &MetaRename::oldname         )
        .Def("N", &MetaRename::newname         )
        .Def("F", &MetaRename::oldpath         )
        .Def("W", &MetaRename::overwrite, false)
        ;
    }
};

/*!
 * \brief set the mtime for a file or directory
 */
struct MetaSetMtime: public MetaRequest {
    fid_t   fid;      //!< stash the fid for logging
    string  pathname; //!< absolute path for which we want to set the mtime
    int64_t mtime;
    MetaSetMtime(fid_t id = -1, int64_t mtime = 0)
        : MetaRequest(META_SETMTIME, true),
          fid(id),
          pathname(),
          mtime(mtime),
          sec(0),
          usec(0)
        {}
    virtual bool start();
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "setmtime:"
            " path: "  << pathname <<
            " mtime: " << ShowTime(mtime)
        ;
    }
    bool Validate()
    {
        mtime = sec * 1000 * 1000 + usec;
        return (! pathname.empty());
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Mtime-sec",  "S", &MetaSetMtime::sec     )
        .Def2("Mtime-usec", "U", &MetaSetMtime::usec    )
        .Def2("Pathname",   "N", &MetaSetMtime::pathname)
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("N", &MetaSetMtime::pathname)
        .Def("S", &MetaSetMtime::sec)
        .Def("U", &MetaSetMtime::usec)
        ;
    }
private:
    int64_t sec;
    int64_t usec;
};

/*!
 * \brief change a file's replication factor, and storage tiers.
 */
struct MetaChangeFileReplication: public MetaRequest {
    fid_t      fid;         //!< fid whose replication has to be changed
    int16_t    numReplicas; //!< desired degree of replication
    kfsSTier_t minSTier;
    kfsSTier_t maxSTier;
    bool       logFlag;
    MetaChangeFileReplication()
        : MetaRequest(META_CHANGE_FILE_REPLICATION, true),
          fid(-1),
          numReplicas(1),
          minSTier(kKfsSTierUndef),
          maxSTier(kKfsSTierUndef),
          logFlag(false),
          fa(0)
        {}
    virtual bool start();
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "change-file-replication:"
            " fid: "      << fid <<
            " replicas: " << numReplicas <<
            " min-tier: " << (int)minSTier <<
            " max-tier: " << (int)maxSTier
        ;
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("File-handle",  "P",  &MetaChangeFileReplication::fid,         fid_t(-1))
        .Def2("Num-replicas", "R",  &MetaChangeFileReplication::numReplicas, int16_t(1))
        .Def2("Min-tier",     "TL", &MetaChangeFileReplication::minSTier,    kKfsSTierUndef)
        .Def2("Max-tier",     "TH", &MetaChangeFileReplication::maxSTier,    kKfsSTierUndef)
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("P",  &MetaChangeFileReplication::fid,         fid_t(-1))
        .Def("R",  &MetaChangeFileReplication::numReplicas, int16_t(1))
        .Def("TL", &MetaChangeFileReplication::minSTier,    kKfsSTierUndef)
        .Def("TH", &MetaChangeFileReplication::maxSTier,    kKfsSTierUndef)
        ;
    }
private:
    MetaFattr* fa;
};

/*!
 * \brief coalesce blocks of one file with another by appending the blocks from
 * src->dest.  After the coalesce is done, src will be of size 0.
 */
struct MetaCoalesceBlocks: public MetaRequest {
    string     srcPath; //!< fully-qualified pathname
    string     dstPath; //!< fully-qualified pathname
    fid_t      srcFid;
    fid_t      dstFid;
    //!< output: the offset in dst at which the first
    //!< block of src was moved to.
    chunkOff_t dstStartOffset;
    size_t     numChunksMoved;
    int64_t    mtime;
    MetaCoalesceBlocks()
        : MetaRequest(META_COALESCE_BLOCKS, true),
          srcPath(),
          dstPath(),
          srcFid(-1),
          dstFid(-1),
          dstStartOffset(-1),
          numChunksMoved(0),
          mtime(0)
        {}
    virtual bool start();
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "coalesce blocks:"
            " src: "  << srcPath <<
            " dst = " << dstPath
        ;
    }
    bool Validate()
    {
        return (! srcPath.empty() && ! dstPath.empty());
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Src-path",  "S", &MetaCoalesceBlocks::srcPath)
        .Def2("Dest-path", "D", &MetaCoalesceBlocks::dstPath)
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("S", &MetaCoalesceBlocks::srcPath)
        .Def("D", &MetaCoalesceBlocks::dstPath)
        ;
    }
};

/*!
 * \brief Notification to hibernate/retire a chunkserver:
 * Hibernation: when the server is put
 * in hibernation mode, the server is taken down temporarily with a promise that
 * it will come back N secs later; if the server doesnt' come up as promised
 * then re-replication starts.
 *
 * Retirement: is extended downtime.  The server is taken down and we don't know
 * if it will ever come back.  In this case, we use this server (preferably)
 * to evacuate/re-replicate all the blocks off it before we take it down.
 */

struct MetaRetireChunkserver : public MetaRequest, public ServerLocation {
    ServerLocation& location;  //<! Location of this server
    int             nSecsDown; //<! set to -1, we retire; otherwise, # of secs of down time
    MetaRetireChunkserver()
        : MetaRequest(META_RETIRE_CHUNKSERVER, false),
          ServerLocation(),
          location(*this),
          nSecsDown(-1)
        {}
    virtual void handle();
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            (nSecsDown > 0 ? "hibernating server: " : "retiring server: ") <<
            location <<
            "down time: " << nSecsDown
        ;
    }
    bool Validate()
    {
        return (location.IsValid());
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Chunk-server-name", "H", &ServerLocation::hostname            )
        .Def2("Chunk-server-port", "P", &ServerLocation::port,             -1)
        .Def2("Downtime",          "D", &MetaRetireChunkserver::nSecsDown, -1)
        ;
    }
};

/*!
 * \brief hello RPC from a chunk server on startup
 */
struct MetaHello : public MetaRequest, public ServerLocation {
    struct ChunkInfo {
        chunkId_t chunkId;
        seq_t     chunkVersion;
    };
    typedef vector<ChunkInfo, StdAllocator<ChunkInfo> > ChunkInfos;
    typedef vector<chunkId_t>                           MissingChunks;

    ChunkServerPtr     server;                   //!< The chunkserver that sent the hello message
    ServerLocation&    location;                 //<! Location of this server
    string             peerName;
    string             clusterKey;
    string             md5sum;
    string             authName;
    Properties::String cryptoKey;
    Properties::String cryptoKeyId;
    int64_t            totalSpace;               //!< How much storage space does the server have (bytes)
    int64_t            totalFsSpace;
    int64_t            usedSpace;                //!< How much storage space is used up (in bytes)
    int64_t            uptime;                   //!< Chunk server uptime.
    int                rackId;                   //!< the rack on which the server is located
    int                numChunks;                //!< # of chunks hosted on this server
    int                numNotStableAppendChunks; //!< # of not stable append chunks hosted on this server
    int                numNotStableChunks;       //!< # of not stable chunks hosted on this server
    int                numMissingChunks;
    int                contentLength;            //!< Length of the message body
    int64_t            numAppendsWithWid;
    int                contentIntBase;
    ChunkInfos         chunks;                   //!< Chunks  hosted on this server
    ChunkInfos         notStableChunks;
    ChunkInfos         notStableAppendChunks;
    MissingChunks      missingChunks;
    int                bytesReceived;
    bool               staleChunksHexFormatFlag;
    bool               deleteAllChunksFlag;
    int64_t            fileSystemId;
    int64_t            metaFileSystemId;
    int64_t            helloDoneCount;
    int64_t            helloResumeCount;
    int64_t            helloResumeFailedCount;
    int64_t            deletedReportCount;
    bool               noFidsFlag;
    int                resumeStep;
    int                bufferBytes;
    size_t             deletedCount;
    size_t             modifiedCount;
    size_t             chunkCount;
    CIdChecksum_t      checksum;
    IOBuffer           responseBuf;

    MetaHello()
        : MetaRequest(META_HELLO, false),
          ServerLocation(),
          server(),
          location(*this),
          peerName(),
          clusterKey(),
          md5sum(),
          authName(),
          cryptoKey(),
          cryptoKeyId(),
          totalSpace(0),
          totalFsSpace(0),
          usedSpace(0),
          uptime(0),
          rackId(-1),
          numChunks(0),
          numNotStableAppendChunks(0),
          numNotStableChunks(0),
          numMissingChunks(0),
          contentLength(0),
          numAppendsWithWid(0),
          contentIntBase(10),
          chunks(),
          notStableChunks(),
          notStableAppendChunks(),
          missingChunks(),
          bytesReceived(0),
          staleChunksHexFormatFlag(false),
          deleteAllChunksFlag(false),
          fileSystemId(-1),
          metaFileSystemId(-1),
          helloDoneCount(0),
          helloResumeCount(0),
          helloResumeFailedCount(0),
          deletedReportCount(0),
          noFidsFlag(false),
          resumeStep(-1),
          bufferBytes(0),
          deletedCount(0),
          modifiedCount(0),
          chunkCount(0),
          checksum(0),
          responseBuf()
        {}
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ReqOstream& os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "Chunkserver hello";
    }
    bool Validate()
    {
        return (ServerLocation::IsValid() &&
            (contentIntBase == 10 || contentIntBase == 16));
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Short-rpc-fmt",                &MetaRequest::shortRpcFormatFlag            )
        .Def("Chunk-server-name",            &ServerLocation::hostname                   )
        .Def("Chunk-server-port",            &ServerLocation::port,               int(-1))
        .Def("Cluster-key",                  &MetaHello::clusterKey                      )
        .Def("MD5Sum",                       &MetaHello::md5sum                          )
        .Def("Total-space",                  &MetaHello::totalSpace,           int64_t(0))
        .Def("Total-fs-space",               &MetaHello::totalFsSpace,         int64_t(0))
        .Def("Used-space",                   &MetaHello::usedSpace,            int64_t(0))
        .Def("Rack-id",                      &MetaHello::rackId,                  int(-1))
        .Def("Uptime",                       &MetaHello::uptime,               int64_t(0))
        .Def("Num-chunks",                   &MetaHello::numChunks,                int(0))
        .Def("Num-not-stable-append-chunks", &MetaHello::numNotStableAppendChunks, int(0))
        .Def("Num-not-stable-chunks",        &MetaHello::numNotStableChunks,       int(0))
        .Def("Num-appends-with-wids",        &MetaHello::numAppendsWithWid,    int64_t(0))
        .Def("Content-length",               &MetaHello::contentLength,            int(0))
        .Def("Content-int-base",             &MetaHello::contentIntBase,          int(10))
        .Def("Stale-chunks-hex-format",      &MetaHello::staleChunksHexFormatFlag,  false)
        .Def("CKeyId",                       &MetaHello::cryptoKeyId)
        .Def("CKey",                         &MetaHello::cryptoKey)
        .Def("FsId",                         &MetaHello::fileSystemId,        int64_t(-1))
        .Def("NoFids",                       &MetaHello::noFidsFlag,                false)
        .Def("Resume",                       &MetaHello::resumeStep,              int(-1))
        .Def("Deleted",                      &MetaHello::deletedCount                    )
        .Def("Modified",                     &MetaHello::modifiedCount                   )
        .Def("Chunks",                       &MetaHello::chunkCount                      )
        .Def("Checksum",                     &MetaHello::checksum                        )
        .Def("Num-missing",                  &MetaHello::numMissingChunks                )
        .Def("Num-hello-done",               &MetaHello::helloDoneCount                  )
        .Def("Num-resume",                   &MetaHello::helloResumeCount                )
        .Def("Num-resume-fail",              &MetaHello::helloResumeFailedCount          )
        ;
    }
};

/*!
 * \brief whenever a chunk server goes down, this message is used to clean up state.
 */
struct MetaBye: public MetaRequest {
    ChunkServerPtr server; //!< The chunkserver that went down
    MetaBye(seq_t s, const ChunkServerPtr& c)
        : MetaRequest(META_BYE, false, s),
          server(c) { }
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "Chunkserver bye";
    }
};

struct MetaGetPathName: public MetaRequest {
    fid_t     fid;
    chunkId_t chunkId;
    MFattr    fattr;
    string    result;
    MetaGetPathName()
        : MetaRequest(META_GETPATHNAME, false),
          fid(-1),
          chunkId(-1),
          fattr(),
          result()
        {}
    virtual void handle();
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "get pathname:"
            " fid: "     << fid <<
            " chunkId: " << chunkId <<
            " status: "  << status
        ;
    }
    bool Validate()
        { return (fid >= 0 || chunkId >= 0); }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("File-handle",  "P", &MetaGetPathName::fid,     fid_t(-1))
        .Def2("Chunk-handle", "H", &MetaGetPathName::chunkId, chunkId_t(-1))
        ;
    }
};

struct MetaChmod: public MetaRequest {
    fid_t     fid;
    kfsMode_t mode;
    MetaChmod()
        : MetaRequest(META_CHMOD, true),
          fid(-1),
          mode(kKfsModeUndef)
        {}
    virtual bool start();
    virtual void handle();
    virtual void response(ReqOstream &os);
    virtual int log(ostream& file) const;
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "chmod:"
            " fid: "    << fid <<
            " mode: "   << oct << mode << dec <<
            " status: " << status
        ;
    }
    bool Validate()
        { return (fid >= 0 && mode != kKfsModeUndef); }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("File-handle", "P", &MetaChmod::fid,  fid_t(-1))
        .Def2("Mode",        "M", &MetaChmod::mode, kKfsModeUndef)
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("P", &MetaChmod::fid,  fid_t(-1))
        .Def("M", &MetaChmod::mode, kKfsModeUndef)
        ;
    }
};

struct MetaChown: public MetaRequest {
    fid_t    fid;
    kfsUid_t user;
    kfsGid_t group;
    string   ownerName;
    string   groupName;
    MetaChown()
        : MetaRequest(META_CHOWN, true),
          fid(-1),
          user(kKfsUserNone),
          group(kKfsGroupNone),
          ownerName(),
          groupName()
        {}
    virtual bool start();
    virtual void handle();
    virtual void response(ReqOstream &os);
    virtual int log(ostream& file) const;
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "chown:"
            " euser: "  << euser <<
            " egroup: " << egroup <<
            " fid: "    << fid <<
            " user: "   << user <<
            " group: "  << group <<
            " status: " << status
        ;
    }
    bool Validate()
        { return (fid >= 0); }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("File-handle", "P",  &MetaChown::fid,   fid_t(-1))
        .Def2("Owner",       "O",  &MetaChown::user,  kKfsUserNone)
        .Def2("Group",       "G",  &MetaChown::group, kKfsGroupNone)
        .Def2("OName",       "ON", &MetaChown::ownerName)
        .Def2("GName",       "GN", &MetaChown::groupName)
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("P",  &MetaChown::fid,   fid_t(-1))
        .Def("O",  &MetaChown::user,  kKfsUserNone)
        .Def("G",  &MetaChown::group, kKfsGroupNone)
        ;
    }
};

/*!
 * \brief RPCs that go from meta server->chunk server are
 * MetaRequest's that define a method to generate the RPC
 * request.
 */
struct MetaChunkRequest: public MetaRequest {
    const chunkId_t      chunkId;
    const ChunkServerPtr server; // The "owner".
    MetaChunkRequest(MetaOp o, seq_t s, bool mu,
            const ChunkServerPtr& c, chunkId_t cid)
        : MetaRequest(o, mu, s),
          chunkId(cid),
          server(c)
        {}
    //!< generate a request message (in string format) as per the
    //!< KFS protocol.
    virtual void request(ReqOstream& os, IOBuffer& /* buf */) { request(os); }
    virtual void handleReply(const Properties& prop) {}
    virtual void handle() {}
    void resume()
    {
        submit_request(this);
    }
protected:
    virtual void request(ReqOstream& /* os */) {}
};

/*!
 * \brief Allocate RPC from meta server to chunk server
 */
struct MetaChunkAllocate : public MetaChunkRequest {
    const int64_t       leaseId;
    kfsSTier_t          minSTier;
    kfsSTier_t          maxSTier;
    string              chunkServerAccessStr;
    string              chunkAccessStr;
    MetaAllocate* const req;
    MetaChunkAllocate(seq_t n, MetaAllocate *r,
            const ChunkServerPtr& s, int64_t l, kfsSTier_t minTier,
            kfsSTier_t maxTier)
        : MetaChunkRequest(META_CHUNK_ALLOCATE, n, false, s, r->chunkId),
          leaseId(l),
          minSTier(minTier),
          maxSTier(maxTier),
          chunkServerAccessStr(),
          chunkAccessStr(),
          req(r)
          {}
    virtual void handle();
    virtual void request(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        os << "meta->chunk allocate:";
        if (req) {
            os << req->Show();
        }
        return os;
    }
};

/*!
 * \brief Delete RPC from meta server to chunk server
 */
struct MetaChunkDelete: public MetaChunkRequest {
    MetaChunkDelete(seq_t n, const ChunkServerPtr& s, chunkId_t c)
        : MetaChunkRequest(META_CHUNK_DELETE, n, false, s, c)
        {}
    virtual void request(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "meta->chunk delete: chunkId: " << chunkId;
    }
};

struct MetaChunkVersChange;

/*!
 * \brief Replicate RPC from meta server to chunk server.  This
 * message is sent to a "destination" chunk server---that is, a chunk
 * server is told to create a copy of chunk from some source that is
 * already hosting the chunk.  This model allows the destination to
 * replicate the chunk at its convenieance.
 */
struct MetaChunkReplicate: public MetaChunkRequest {
    typedef DelegationToken::TokenSeq TokenSeq;
    typedef map<
        int,
        pair<chunkId_t, seq_t>,
        less<int>,
        StdFastAllocator<pair<const int, pair<chunkId_t, seq_t> > >
    > InvalidStripes;
    typedef map<
        pair<kfsUid_t, fid_t>,
        unsigned int,
        less<pair<kfsUid_t, fid_t> >,
        StdFastAllocator<pair<const pair<kfsUid_t, fid_t>, unsigned int> >
    > FileRecoveryInFlightCount;

    fid_t                               fid;          //!< input: we tell the chunkserver what it is
    seq_t                               chunkVersion; //!< io: the chunkservers tells us what it did
    chunkOff_t                          chunkOffset;  //!< input: chunk recovery parameters
    int16_t                             striperType;
    int16_t                             numStripes;
    int16_t                             numRecoveryStripes;
    int32_t                             stripeSize;
    ChunkServerPtr                      dataServer;  //!< where to get a copy from
    ServerLocation                      srcLocation;
    string                              pathname;
    int64_t                             fileSize;
    InvalidStripes                      invalidStripes;
    kfsSTier_t                          minSTier;
    kfsSTier_t                          maxSTier;
    TokenSeq                            tokenSeq;
    bool                                clientCSAllowClearTextFlag;
    bool                                longRpcFormatFlag;
    time_t                              issuedTime;
    uint32_t                            validForTime;
    CryptoKeys::KeyId                   keyId;
    CryptoKeys::Key                     key;
    MetaChunkVersChange*                versChange;
    FileRecoveryInFlightCount::iterator recovIt;
    string                              metaServerAccess;
    MetaChunkReplicate(seq_t n, const ChunkServerPtr& s,
            fid_t f, chunkId_t c, const ServerLocation& loc,
            const ChunkServerPtr& src, kfsSTier_t minTier, kfsSTier_t maxTier,
            FileRecoveryInFlightCount::iterator it)
        : MetaChunkRequest(META_CHUNK_REPLICATE, n, false, s, c),
          fid(f),
          chunkVersion(-1),
          chunkOffset(-1),
          striperType(KFS_STRIPED_FILE_TYPE_NONE),
          numStripes(0),
          numRecoveryStripes(0),
          stripeSize(0),
          dataServer(src),
          srcLocation(loc),
          pathname(),
          fileSize(-1),
          invalidStripes(),
          minSTier(minTier),
          maxSTier(maxTier),
          tokenSeq(),
          clientCSAllowClearTextFlag(false),
          longRpcFormatFlag(false),
          issuedTime(),
          validForTime(0),
          keyId(),
          key(),
          versChange(0),
          recovIt(it),
          metaServerAccess()
        {}
    virtual ~MetaChunkReplicate() { assert(! versChange); }
    virtual void handle();
    virtual void request(ReqOstream &os);
    virtual void handleReply(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const;
};

/*!
 * \brief Chunk version # change RPC from meta server to chunk server
 */
struct MetaChunkVersChange: public MetaChunkRequest {
    fid_t               fid;
    seq_t               chunkVersion; //!< version # assigned to this chunk
    seq_t               fromVersion;
    bool                makeStableFlag;
    bool                pendingAddFlag;
    bool                verifyStableFlag;
    MetaChunkReplicate* replicate;

    MetaChunkVersChange(
        seq_t                 n,
        const ChunkServerPtr& s,
        fid_t                 f,
        chunkId_t             c,
        seq_t                 v,
        seq_t                 fromVers,
        bool                  mkStableFlag,
        bool                  pendAddFlag,
        MetaChunkReplicate*   repl,
        bool                  verifyStblFlag)
        : MetaChunkRequest(META_CHUNK_VERSCHANGE, n, false, s, c),
          fid(f),
          chunkVersion(v),
          fromVersion(fromVers),
          makeStableFlag(mkStableFlag),
          pendingAddFlag(pendAddFlag),
          verifyStableFlag(verifyStblFlag),
          replicate(repl)
    {
        if (replicate) {
            assert(! replicate->versChange);
            replicate->versChange = this;
        }
    }
    virtual ~MetaChunkVersChange()  { assert(! replicate); }
    virtual void handle();
    virtual void request(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "meta->chunk vers change:"
            " fid: "        << fid <<
            " chunkId: "        << chunkId <<
            " version: from: "  << fromVersion <<
            " => to: "          << chunkVersion <<
            " make stable: "    << makeStableFlag
        ;
    }
};

/*!
 * \brief As a chunkserver for the size of a particular chunk.  We use this RPC
 * to compute the filesize: whenever the lease on the last chunk of the file
 * expires, we get the chunk's size and then determine the filesize.
 */
struct MetaChunkSize: public MetaChunkRequest {
    fid_t      fid;     //!< input: we use the tuple <fileid, chunkid> to
                //!< find the entry we need.
    seq_t      chunkVersion;
    chunkOff_t chunkSize; //!< output: the chunk size
    chunkOff_t filesize;  //!< for logging purposes: the size of the file
    /// input: given the pathname, we can update space usage for the path
    /// hierarchy corresponding to pathname; this will enable us to make "du"
    /// instantaneous.
    string     pathname;
    bool       retryFlag;
    MetaChunkSize(seq_t n, const ChunkServerPtr& s, fid_t f,
            chunkId_t c, seq_t v, const string &p, bool retry)
        : MetaChunkRequest(META_CHUNK_SIZE, n, true, s, c),
          fid(f),
          chunkVersion(v),
          chunkSize(-1),
          filesize(-1),
          pathname(p),
          retryFlag(retry)
        {}
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void request(ReqOstream &os);
    virtual void handleReply(const Properties& prop)
    {
        chunkSize = prop.getValue(
            shortRpcFormatFlag ? "S" : "Size", (chunkOff_t) -1);
    }
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "meta->chunk size: " << pathname <<
            " fid: "             << fid <<
            " chunkId: "         << chunkId <<
            " chunkVersion: "    << chunkVersion <<
            " size: "            << chunkSize
        ;
    }
};

/*!
 * \brief Heartbeat RPC from meta server to chunk server.  We can
 * ask the chunk server for lots of stuff; for now, we ask it
 * how much is available/used up.
 */
struct MetaChunkHeartbeat: public MetaChunkRequest {
    int64_t evacuateCount;
    bool    reAuthenticateFlag;
    MetaChunkHeartbeat(seq_t n, const ChunkServerPtr& s,
            int64_t evacuateCnt, bool reAuthFlag = false)
        : MetaChunkRequest(META_CHUNK_HEARTBEAT, n, false, s, -1),
          evacuateCount(evacuateCnt),
          reAuthenticateFlag(reAuthFlag)
        {}
    virtual void request(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "meta->chunk heartbeat";
    }
};

/*!
 * \brief Stale chunk notification message from meta->chunk.  This
 * tells the chunk servers the id's of stale chunks, which the chunk
 * server should get rid of.
 */
struct MetaChunkStaleNotify: public MetaChunkRequest {
    ChunkIdQueue staleChunkIds; //!< chunk ids that are stale
    bool         evacuatedFlag;
    bool         hexFormatFlag;
    bool         hasAvailChunksSeqFlag;
    seq_t        availChunksSeq;
    size_t       skipFront;
    MetaChunkStaleNotify(seq_t n, const ChunkServerPtr& s,
            bool evacFlag, bool hexFmtFlag, const seq_t* acSeq)
        : MetaChunkRequest(META_CHUNK_STALENOTIFY, n, false, s, -1),
          staleChunkIds(),
          evacuatedFlag(evacFlag),
          hexFormatFlag(hexFmtFlag),
          hasAvailChunksSeqFlag(acSeq != 0),
          availChunksSeq(acSeq ? *acSeq : -1),
          skipFront(0)
        {}
    virtual void request(ReqOstream& os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "meta->chunk stale notify";
    }
};

struct MetaBeginMakeChunkStable : public MetaChunkRequest {
    const fid_t          fid;           // input
    const seq_t          chunkVersion;  // input
    const ServerLocation serverLoc;     // processing this cmd
    int64_t              chunkSize;     // output
    uint32_t             chunkChecksum; // output
    MetaBeginMakeChunkStable(seq_t n, const ChunkServerPtr& s,
            const ServerLocation& l, fid_t f, chunkId_t c, seq_t v) :
        MetaChunkRequest(META_BEGIN_MAKE_CHUNK_STABLE, n, false, s, c),
        fid(f),
        chunkVersion(v),
        serverLoc(l),
        chunkSize(-1),
        chunkChecksum(0)
        {}
    virtual void handle();
    virtual void request(ReqOstream &os);
    virtual void handleReply(const Properties& prop)
    {
        chunkSize     =           prop.getValue(
            shortRpcFormatFlag ? "S" : "Chunk-size",     (int64_t) -1);
        chunkChecksum = (uint32_t)prop.getValue(
            shortRpcFormatFlag ? "K" : "Chunk-checksum", (uint64_t)0);
    }
    virtual ostream& ShowSelf(ostream& os) const {
        return os <<
            "begin-make-chunk-stable:"
            " server: "        << serverLoc <<
            " seq: "           << opSeqno <<
            " status: "        << status <<
            (statusMsg.empty() ? "" : " ") << statusMsg <<
            " fileid: "        << fid <<
            " chunkid: "       << chunkId <<
            " chunkvers: "     << chunkVersion <<
            " chunkSize: "     << chunkSize <<
            " chunkChecksum: " << chunkChecksum
        ;
    }
};

struct MetaLogMakeChunkStable : public MetaRequest, public  KfsCallbackObj {
    const fid_t     fid;              // input
    const chunkId_t chunkId;          // input
    const seq_t     chunkVersion;     // input
    const int64_t   chunkSize;        // input
    const uint32_t  chunkChecksum;    // input
    const bool      hasChunkChecksum; // input
    MetaLogMakeChunkStable(fid_t fileId, chunkId_t id, seq_t version,
        int64_t size, bool hasChecksum, uint32_t checksum, seq_t seqNum,
        bool logDoneTypeFlag = false)
        : MetaRequest(logDoneTypeFlag ?
            META_LOG_MAKE_CHUNK_STABLE_DONE :
            META_LOG_MAKE_CHUNK_STABLE, true, seqNum),
          KfsCallbackObj(),
          fid(fileId),
          chunkId(id),
          chunkVersion(version),
          chunkSize(size),
          chunkChecksum(checksum),
          hasChunkChecksum(hasChecksum)
    {
        SET_HANDLER(this, &MetaLogMakeChunkStable::logDone);
        clnt = this;
    }
    virtual void handle() { status = 0; }
    virtual ostream& ShowSelf(ostream& os) const {
        return os <<
            (op == META_LOG_MAKE_CHUNK_STABLE ?
                "log-make-chunk-stable:" :
                "log-make-chunk-stable-done:") <<
            " fleid: "         << fid <<
            " chunkid: "       << chunkId <<
            " chunkvers: "     << chunkVersion <<
            " chunkSize: "     << chunkSize <<
            " chunkChecksum: " << (hasChunkChecksum ?
                int64_t(chunkChecksum) : int64_t(-1))
        ;
    }
    virtual int log(ostream &file) const;
    int logDone(int code, void *data);
};

struct MetaLogMakeChunkStableDone : public MetaLogMakeChunkStable {
    MetaLogMakeChunkStableDone(fid_t fileId, chunkId_t id, seq_t version,
        int64_t size, bool hasChecksum, uint32_t checksum, seq_t seqNum)
        : MetaLogMakeChunkStable(fileId, id, version, size, hasChecksum,
            checksum, seqNum, true)
        {}
};

/*!
 * \brief Notification message from meta->chunk asking the server to make a
 * chunk.  This tells the chunk server that the writes to a chunk are done and
 * that the chunkserver should flush any dirty data.
 */
struct MetaChunkMakeStable: public MetaChunkRequest {
    const fid_t      fid;   //!< input: we tell the chunkserver what it is
    const seq_t      chunkVersion; //!< The version tha the chunk should be in
    const chunkOff_t chunkSize;
    const bool       hasChunkChecksum:1;
    const bool       addPending:1;
    const uint32_t   chunkChecksum;
    MetaChunkMakeStable(
        seq_t                 inSeqNo,
        const ChunkServerPtr& inServer,
        fid_t                 inFileId,
        chunkId_t             inChunkId,
        seq_t                 inChunkVersion,
        chunkOff_t            inChunkSize,
        bool                  inHasChunkChecksum,
        uint32_t              inChunkChecksum,
        bool                  inAddPending)
        : MetaChunkRequest(META_CHUNK_MAKE_STABLE,
                inSeqNo, false, inServer, inChunkId),
          fid(inFileId),
          chunkVersion(inChunkVersion),
          chunkSize(inChunkSize),
          hasChunkChecksum(inHasChunkChecksum),
          addPending(inAddPending),
          chunkChecksum(inChunkChecksum)
        {}
    virtual void handle();
    virtual void request(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const;
};


/*!
 * For scheduled downtime, we evacaute all the chunks on a server; when
 * we know that the evacuation is finished, we tell the chunkserver to retire.
 */
struct MetaChunkRetire: public MetaChunkRequest {
    MetaChunkRetire(seq_t n, const ChunkServerPtr& s):
        MetaChunkRequest(META_CHUNK_RETIRE, n, false, s, -1) { }
    virtual void request(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "chunkserver retire";
    }
};

struct MetaChunkSetProperties: public MetaChunkRequest {
    const string serverProps;
    MetaChunkSetProperties(seq_t n, const ChunkServerPtr& s,
            const Properties& props)
        : MetaChunkRequest(META_CHUNK_SET_PROPERTIES, n, false, s, -1),
          serverProps(Properties2Str(props))
        {}
    virtual void request(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "chunkserver set properties";
    }
    static string Properties2Str(const Properties& props)
    {
        string ret;
        props.getList(ret, "");
        return ret;
    }
};

struct MetaChunkServerRestart : public MetaChunkRequest {
    MetaChunkServerRestart(seq_t n, const ChunkServerPtr& s)
        : MetaChunkRequest(META_CHUNK_SERVER_RESTART, n, false, s, -1)
        {}
    virtual void request(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "chunkserver restart";
    }
};

/*!
 * \brief For monitoring purposes, a client/tool can send a PING
 * request.  In response, the server replies with the list of all
 * connected chunk servers and their locations as well as some state
 * about each of those servers.
 */
struct MetaPing : public MetaRequest {
    IOBuffer resp;
    MetaPing()
        : MetaRequest(META_PING, false),
          resp()
    {
        // Suppress warning with requests with no version filed.
        clientProtoVers = KFS_CLIENT_PROTO_VERS;
    }
    virtual void handle();
    virtual void response(ReqOstream &os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "ping";
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        ;
    }
};

/*!
 * \brief For monitoring purposes, a client/tool can request metaserver
 * to provide a list of live chunkservers.
 */
struct MetaUpServers: public MetaRequest {
    IOBuffer resp;
    MetaUpServers()
        : MetaRequest(META_UPSERVERS, false),
          resp()
        {}
    virtual void handle();
    virtual void response(ReqOstream& os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "upservers";
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        ;
    }
};

/*!
 * \brief To toggle WORM mode of metaserver a client/tool can send a
 * TOGGLE_WORM request. In response, the server changes its WORM state.
 */
struct MetaToggleWORM: public MetaRequest {
    bool value; // !< Enable/disable WORM
    MetaToggleWORM()
        : MetaRequest(META_TOGGLE_WORM, false),
          value(false)
        {}
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            (value ? "Toggle WORM: Enabled" : "Toggle WORM: Disabled");
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Toggle-WORM", "T", &MetaToggleWORM::value, false)
        ;
    }
};

/*!
 * \brief For monitoring purposes, a client/tool can send a STATS
 * request.  In response, the server replies with the list of all
 * counters it keeps.
 */
struct MetaStats: public MetaRequest {
    string stats; //!< result
    MetaStats()
        : MetaRequest(META_STATS, false),
          stats()
        {}
    virtual void handle();
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "stats";
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        ;
    }
};

/*!
 * \brief For debugging purposes, recompute the size of the dir tree
 */
struct MetaRecomputeDirsize: public MetaRequest {
    MetaRecomputeDirsize()
        : MetaRequest(META_RECOMPUTE_DIRSIZE, false)
        {}
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "recompute dir size";
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        ;
    }
};

/*!
 * \brief For debugging purposes, dump out the chunk->location map
 * to a file.
 */
struct MetaDumpChunkToServerMap: public MetaRequest {
    string chunkmapFile; //!< file to which the chunk map was written to
    int    pid;
    MetaDumpChunkToServerMap()
        : MetaRequest(META_DUMP_CHUNKTOSERVERMAP, false),
          chunkmapFile(),
          pid(-1)
        {}
    virtual void handle();
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "dump chunk2server map";
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        ;
    }
};

/*!
 * \brief For debugging purposes, check the status of all the leases
 */
struct MetaCheckLeases: public MetaRequest {
    MetaCheckLeases()
        : MetaRequest(META_CHECK_LEASES, false)
        {}
    virtual void handle();
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "checking all leases";
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        ;
    }
};

/*!
 * \brief For debugging purposes, dump out the set of blocks that are currently
 * being re-replicated.
 */
struct MetaDumpChunkReplicationCandidates: public MetaRequest {
    // list of blocks that are being re-replicated
    size_t   numReplication;
    size_t   numPendingRecovery;
    IOBuffer resp;
    MetaDumpChunkReplicationCandidates()
        : MetaRequest(META_DUMP_CHUNKREPLICATIONCANDIDATES, false),
          numReplication(0),
          numPendingRecovery(0),
          resp()
        {}
    virtual void handle();
    virtual void response(ReqOstream &os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "dump chunk replication candidates";
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        ;
    }
};

/*!
 * \brief Check the replication level of all blocks in the system.  Return back
 * a list of files that have blocks missing.
*/
struct MetaFsck: public MetaRequest {
    MetaFsck()
        : MetaRequest(META_FSCK, false),
          reportAbandonedFilesFlag(true),
          pid(-1),
          fd(),
          resp()
        {}
    virtual void handle();
    virtual void response(ReqOstream &os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "fsck";
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Report-Abandoned-Files", "A", &MetaFsck::reportAbandonedFilesFlag)
        ;
    }
    static void SetParameters(const Properties& props);
private:
    typedef vector<int> Fds;

    bool          reportAbandonedFilesFlag;
    int           pid;
    Fds           fd;
    IOBuffer      resp;
    static string sTmpName;
    static int    sMaxFsckResponseSize;
};

/*!
 * \brief For monitoring purposes, a client/tool can send a OPEN FILES
 * request.  In response, the server replies with the list of all
 * open files---files for which there is a valid lease
 */
struct MetaOpenFiles: public MetaRequest {
    typedef map<
        fid_t,
        vector<pair<chunkId_t, size_t> >,
        less<fid_t>,
        StdFastAllocator<pair<const fid_t, vector<pair<chunkId_t, size_t> > > >
    > ReadInfo;
    typedef map<
        fid_t,
        vector<chunkId_t>,
        less<fid_t>,
        StdFastAllocator<pair<const fid_t, vector<chunkId_t> > >
    > WriteInfo;
    size_t   openForReadCnt;  //!< result
    size_t   openForWriteCnt; //!< result
    IOBuffer resp;
    MetaOpenFiles()
        : MetaRequest(META_OPEN_FILES, false),
          openForReadCnt(0),
          openForWriteCnt(0),
          resp()
        {}
    virtual void handle();
    virtual void response(ReqOstream& os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "open files";
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        ;
    }
};

struct MetaSetChunkServersProperties : public MetaRequest {
    Properties properties; // input
    MetaSetChunkServersProperties()
        : MetaRequest(META_SET_CHUNK_SERVERS_PROPERTIES, false),
          properties()
        {}
    virtual void handle();
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        string ret("set chunk servers properties ");
        properties.getList(ret, "", ";");
        return os << ret;
    }
    bool HandleUnknownField(
        const char* key, size_t keyLen,
        const char* val, size_t valLen)
    {
        const size_t      kPrefLen = 12;
        const char* const kPref    = "chunkServer.";
        if (keyLen >= kPrefLen || memcmp(kPref, key, kPrefLen) == 0) {
            properties.setValue(
                Properties::String(key, keyLen),
                Properties::String(val, valLen)
            );
        }
        return true;
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        ;
    }
};

struct MetaGetChunkServersCounters : public MetaRequest {
    MetaGetChunkServersCounters()
        : MetaRequest(META_GET_CHUNK_SERVERS_COUNTERS, false),
          resp()
    {
        // Suppress warning with requests with no version filed.
        clientProtoVers = KFS_CLIENT_PROTO_VERS;
    }
    virtual void handle();
    virtual void response(ReqOstream &os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "get chunk servers counters";
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        ;
    }
private:
    IOBuffer resp;
};

struct MetaGetChunkServerDirsCounters : public MetaRequest {
    MetaGetChunkServerDirsCounters()
        : MetaRequest(META_GET_CHUNK_SERVER_DIRS_COUNTERS, false),
          resp()
    {
        // Suppress warning with requests with no version filed.
        clientProtoVers = KFS_CLIENT_PROTO_VERS;
    }
    virtual void handle();
    virtual void response(ReqOstream &os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "get chunk servers dir counters";
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        ;
    }
private:
    IOBuffer resp;
};

struct MetaGetRequestCounters : public MetaRequest {
    MetaGetRequestCounters()
        : MetaRequest(META_GET_REQUEST_COUNTERS, false),
          resp(),
          userCpuMicroSec(0),
          systemCpuMicroSec(0)
        {}
    virtual void handle();
    virtual void response(ReqOstream &os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "get request counters ";
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        ;
    }
private:
    IOBuffer resp;
    int64_t  userCpuMicroSec;
    int64_t  systemCpuMicroSec;
};

struct MetaCheckpoint : public MetaRequest {
    MetaCheckpoint(seq_t s, KfsCallbackObj* c)
        : MetaRequest(META_CHECKPOINT, false, s),
          lockFileName(),
          lockFd(-1),
          intervalSec(60 * 60),
          pid(-1),
          failedCount(0),
          maxFailedCount(2),
          checkpointWriteTimeoutSec(60 * 60),
          checkpointWriteSyncFlag(true),
          checkpointWriteBufferSize(16 << 20),
          lastCheckpointId(-1),
          runningCheckpointId(-1),
          lastRun(0)
        { clnt = c; }
    virtual void handle();
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "checkpoint";
    }
    void SetParameters(const Properties& props);
    void ScheduleNow();
private:
    string lockFileName;
    int    lockFd;
    int    intervalSec;
    int    pid;
    int    failedCount;
    int    maxFailedCount;
    int    checkpointWriteTimeoutSec;
    bool   checkpointWriteSyncFlag;
    size_t checkpointWriteBufferSize;
    seq_t  lastCheckpointId;
    seq_t  runningCheckpointId;
    time_t lastRun;
};

/*!
 * \brief Op to initiate connection close by the meta server. To use with netcat
 * and such.
 */
struct MetaDisconnect : public MetaRequest {
    MetaDisconnect()
        : MetaRequest(META_DISCONNECT, false)
    {
        // Suppress warning with requests with no version filed.
        clientProtoVers = KFS_CLIENT_PROTO_VERS;
    }
    virtual void handle()                {}
    virtual ostream& ShowSelf(ostream& os) const
        { return os << "disconnect"; }
    bool Validate()                      { return true; }
};

struct MetaAuthenticate : public MetaRequest {
    int                    authType;
    int                    contentLength;
    char*                  contentBuf;
    int                    contentBufPos;
    int                    responseAuthType;
    const char*            responseContentPtr;
    int                    responseContentLen;
    bool                   doneFlag;
    int64_t                credExpirationTime;
    int64_t                sessionExpirationTime;
    string                 authName;
    NetConnection::Filter* filter;

    MetaAuthenticate()
        : MetaRequest(META_AUTHENTICATE, false),
          authType(kAuthenticationTypeUndef),
          contentLength(0),
          contentBuf(0),
          contentBufPos(0),
          responseAuthType(kAuthenticationTypeUndef),
          responseContentPtr(0),
          responseContentLen(0),
          doneFlag(false),
          credExpirationTime(0),
          sessionExpirationTime(0),
          authName(),
          filter(0)
          {}
    virtual ~MetaAuthenticate()
    {
        delete [] contentBuf;
        delete filter;
    }
    virtual void handle() {}
    virtual bool dispatch(ClientSM& sm);
    virtual ostream& ShowSelf(ostream& os) const
        { return os << "authenticate"; }
    virtual void response(ReqOstream& os);
    bool Validate()                            { return true; }
    int Read(IOBuffer& iobuf);
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Short-rpc-fmt",   "f", &MetaRequest::shortRpcFormatFlag)
        .Def2("Auth-type",       "A", &MetaAuthenticate::authType,      int(kAuthenticationTypeUndef))
        .Def2("Content-length",  "l", &MetaAuthenticate::contentLength, int(0))
        ;
    }
};

struct MetaDelegate : public MetaRequest {
    uint16_t                  delegationFlags;
    uint32_t                  validForTime;
    uint64_t                  issuedTime;
    DelegationToken::TokenSeq tokenSeq;
    bool                      allowDelegationFlag;
    StringBufT<64>            renewTokenStr;
    StringBufT<64>            renewKeyStr;
    DelegationToken           renewToken;

    MetaDelegate()
        : MetaRequest(META_DELEGATE, false),
          delegationFlags(0),
          validForTime(0),
          issuedTime(0),
          tokenSeq(0),
          allowDelegationFlag(false),
          renewTokenStr(),
          renewKeyStr(),
          renewToken()
          {}
    virtual void handle() {}
    virtual bool dispatch(ClientSM& sm);
    virtual ostream& ShowSelf(ostream& os) const
        { return (os << "delegate: " << " uid: " << authUid); }
    virtual void response(ReqOstream& os);
    bool Validate()                            { return true; }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Valid-for-time",   "V", &MetaDelegate::validForTime, uint32_t(0))
        .Def2("Allow-delegation", "D", &MetaDelegate::allowDelegationFlag, false)
        .Def2("Renew-token",      "T", &MetaDelegate::renewTokenStr)
        .Def2("Renew-key",        "K", &MetaDelegate::renewKeyStr)
        ;
    }
};

struct MetaDelegateCancel : public MetaRequest {
    DelegationToken token;
    StringBufT<64>  tokenStr;
    StringBufT<64>  tokenKeyStr;

    MetaDelegateCancel()
        : MetaRequest(META_DELEGATE_CANCEL, true),
          token(),
          tokenStr(),
          tokenKeyStr()
          {}
    virtual bool dispatch(ClientSM& sm);
    virtual void handle();
    virtual ostream& ShowSelf(ostream& os) const
        { return (os << "delegate cancel " <<  token.Show()); }
    virtual void response(ReqOstream& os);
    virtual int log(ostream& /* file */) const;
    bool Validate();
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Token", "T", &MetaDelegateCancel::tokenStr)
        .Def2("Key",   "K", &MetaDelegateCancel::tokenKeyStr)
        ;
    }
};

/*!
 * \brief Op for handling a notify of a corrupt chunk
 */
struct MetaChunkCorrupt: public MetaRequest {
    typedef StringBufT<256> ChunkIds;

    fid_t          fid;         //!< input
    chunkId_t      chunkId;     //!< input
    ChunkIds       chunkIdsStr; //!< input
    bool           isChunkLost; //!< input
    bool           noReplyFlag; //!< input
    bool           dirOkFlag;   //!< input
    int            chunkCount;  //!< input
    string         chunkDir;    //!< input
    ChunkServerPtr server;      //!< The chunkserver that sent us this message
    MetaChunkCorrupt(seq_t s = -1, fid_t f = -1, chunkId_t c = -1)
        : MetaRequest(META_CHUNK_CORRUPT, false, s),
          fid(f),
          chunkId(c),
          chunkIdsStr(),
          isChunkLost(false),
          noReplyFlag(false),
          dirOkFlag(false),
          chunkCount(0),
          chunkDir(),
          server()
        {}
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            (isChunkLost ? "lost" : "corrupt") <<
            " fid: "   << fid <<
            " chunk: " << chunkId
        ;
    }
    virtual void setChunkServer(const ChunkServerPtr& cs) { server = cs; }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("File-handle",   "P", &MetaChunkCorrupt::fid,             fid_t(-1))
        .Def2("Chunk-handle",  "H", &MetaChunkCorrupt::chunkId,     chunkId_t(-1))
        .Def2("Is-chunk-lost", "L", &MetaChunkCorrupt::isChunkLost,         false)
        .Def2("No-reply",      "N", &MetaChunkCorrupt::noReplyFlag,         false)
        .Def2("Chunk-dir",     "D", &MetaChunkCorrupt::chunkDir)
        .Def2("Dir-ok",        "O", &MetaChunkCorrupt::dirOkFlag,           false)
        .Def2("Num-chunks",    "C", &MetaChunkCorrupt::chunkCount,              0)
        .Def2("Ids",           "I", &MetaChunkCorrupt::chunkIdsStr)
        ;
    }
};

/*!
 * \brief chunk server chunks evacuate request
 */
struct MetaChunkEvacuate: public MetaRequest {
    typedef StringBufT<256> StorageStr;

    int64_t             totalSpace;
    int64_t             totalFsSpace;
    int64_t             usedSpace;
    int                 numDrives;
    int                 numWritableDrives;
    int                 numEvacuateInFlight;
    StorageStr          storageTiersInfo;
    StringBufT<21 * 32> chunkIds; //!< input
    ChunkServerPtr      server;
    MetaChunkEvacuate(seq_t s = -1)
        : MetaRequest(META_CHUNK_EVACUATE, false, s),
          totalSpace(-1),
          totalFsSpace(-1),
          usedSpace(-1),
          numDrives(-1),
          numWritableDrives(-1),
          numEvacuateInFlight(-1),
          storageTiersInfo("X"),
          chunkIds(),
          server()
        {}
    virtual void handle();
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        os << "evacuate: ";
        os.write(chunkIds.GetPtr(), chunkIds.GetSize());
        return os;
    }
    virtual void setChunkServer(const ChunkServerPtr& cs) { server = cs; }
    bool Validate()
    {
        return true;
    }
    const StorageStr* GetStorageTiersInfo() const
    {
        if (storageTiersInfo == "X") {
            return 0;
        }
        return &storageTiersInfo;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Chunk-ids",      "I", &MetaChunkEvacuate::chunkIds)
        .Def2("Total-space",    "T", &MetaChunkEvacuate::totalSpace,      int64_t(-1))
        .Def2("Total-fs-space", "F", &MetaChunkEvacuate::totalFsSpace,    int64_t(-1))
        .Def2("Used-space",     "U", &MetaChunkEvacuate::usedSpace,       int64_t(-1))
        .Def2("Num-drives",     "D", &MetaChunkEvacuate::numDrives,           int(-1))
        .Def2("Num-wr-drives",  "W", &MetaChunkEvacuate::numWritableDrives,   int(-1))
        .Def2("Num-evacuate",   "E", &MetaChunkEvacuate::numEvacuateInFlight, int(-1))
        .Def2("Storage-tiers:", "S", &MetaChunkEvacuate::storageTiersInfo)
        ;
    }
};

struct MetaChunkAvailable : public MetaRequest {
    StringBufT<16 * 64 * 2> chunkIdAndVers; //!< input
    ChunkServerPtr          server;
    MetaChunkAvailable(seq_t s = -1)
        : MetaRequest(META_CHUNK_AVAILABLE, false, s),
          chunkIdAndVers(),
          server()
        {}
    virtual void handle();
    virtual void response(ReqOstream& os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        os << "chunk available: ";
        os.write(chunkIdAndVers.GetPtr(), chunkIdAndVers.GetSize());
        return os;
    }
    virtual void setChunkServer(const ChunkServerPtr& cs) { server = cs; }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Chunk-ids-vers", "I", &MetaChunkAvailable::chunkIdAndVers)
        ;
    }
};

struct MetaChunkDirInfo : public MetaRequest {
    typedef StringBufT<256> DirName;

    ChunkServerPtr server;
    bool           noReplyFlag;
    DirName        dirName;
    StringBufT<32> kfsVersion;
    Properties     props;

    MetaChunkDirInfo(seq_t s = -1)
        : MetaRequest(META_CHUNKDIR_INFO, false, s),
          server(),
          noReplyFlag(false),
          dirName(),
          kfsVersion(),
          props()
        {}
    virtual void handle();
    virtual void response(ReqOstream& os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "chunk dir info";
    }
    virtual void setChunkServer(const ChunkServerPtr& cs) { server = cs; }
    bool Validate()
    {
        return true;
    }
    // RequestParser::Parse creates object of this type, overload / hiding the
    // parent's method is sufficient, i.e. HandleUnknownField does not have to
    // be "virtual".
    bool HandleUnknownField(
        const char* key, size_t keyLen,
        const char* val, size_t valLen)
    {
        props.setValue(
            Properties::String(key, keyLen),
            Properties::String(val, valLen)
        );
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        // Make sure that all "unwanted" fields that aren't counters are added
        // to the parser.
        return MetaRequest::ParserDef(parser)
        .Def("No-reply", &MetaChunkDirInfo::noReplyFlag)
        .Def("Dir-name", &MetaChunkDirInfo::dirName)
        .Def("Version",  &MetaChunkDirInfo::kfsVersion)
        ;
    }
};

/*!
 * \brief Op for acquiring a lease on a chunk of a file.
 */
struct MetaLeaseAcquire: public MetaRequest {
    struct ChunkAccessInfo
    {
        ServerLocation    serverLocation;
        chunkId_t         chunkId;
        kfsUid_t          authUid;
        CryptoKeys::KeyId keyId;
        CryptoKeys::Key   key;
        ChunkAccessInfo(
            const ServerLocation& loc = ServerLocation(),
            chunkId_t             id  = -1,
            kfsUid_t              uid = kKfsUserNone)
            : serverLocation(loc),
              chunkId(id),
              authUid(uid),
              keyId(),
              key()
        {}
    };
    typedef StBufferT<ChunkAccessInfo, 3> ChunkAccess;

    StringBufT<256>    pathname; // Optional for debugging.
    chunkId_t          chunkId;
    bool               flushFlag;
    int                leaseTimeout;
    int64_t            leaseId;
    bool               clientCSAllowClearTextFlag;
    time_t             issuedTime;
    int                validForTime;
    StringBufT<21 * 8> chunkIds; // This and the following used by sort master.
    bool               getChunkLocationsFlag;
    bool               appendRecoveryFlag;
    string             appendRecoveryLocations;
    IOBuffer           responseBuf;
    ChunkAccess        chunkAccess;
    MetaLeaseAcquire()
        : MetaRequest(META_LEASE_ACQUIRE, false),
          pathname(),
          chunkId(-1),
          flushFlag(false),
          leaseTimeout(LEASE_INTERVAL_SECS),
          leaseId(-1),
          clientCSAllowClearTextFlag(false),
          issuedTime(0),
          validForTime(0),
          chunkIds(),
          getChunkLocationsFlag(false),
          appendRecoveryFlag(false),
          appendRecoveryLocations(),
          responseBuf(),
          chunkAccess(),
          handleCount(0)
          {}
    virtual void handle();
    virtual void response(ReqOstream& os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            " lease acquire:"
            " chunkId: " << chunkId <<
            (flushFlag ? " flush" : "") <<
            " caccess: " << chunkAccess.GetSize() <<
            " "          << pathname
        ;
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Pathname",            "N", &MetaLeaseAcquire::pathname                         )
        .Def2("Chunk-handle",        "H", &MetaLeaseAcquire::chunkId,      chunkId_t(-1)      )
        .Def2("Flush-write-lease",   "F", &MetaLeaseAcquire::flushFlag,    false              )
        .Def2("Lease-timeout",       "T", &MetaLeaseAcquire::leaseTimeout, LEASE_INTERVAL_SECS)
        .Def2("Chunk-ids",           "I", &MetaLeaseAcquire::chunkIds)
        .Def2("Get-locations",       "L", &MetaLeaseAcquire::getChunkLocationsFlag,      false)
        .Def2("Append-recovery",     "A", &MetaLeaseAcquire::appendRecoveryFlag,         false)
        .Def2("Append-recovery-loc", "R", &MetaLeaseAcquire::leaseId,              int64_t(-1))
        ;
    }
protected:
    int handleCount;
};

/*!
 * \brief Op for renewing a lease on a chunk of a file.
 */
struct MetaLeaseRenew: public MetaRequest {
    typedef DelegationToken::TokenSeq         TokenSeq;
    typedef MetaLeaseAcquire::ChunkAccessInfo ChunkAccessInfo;
    typedef MetaLeaseAcquire::ChunkAccess     ChunkAccess;

    LeaseType          leaseType;
    StringBufT<256>    pathname; // Optional for debugging;
    chunkId_t          chunkId;
    int64_t            leaseId;
    bool               emitCSAccessFlag;
    bool               clientCSAllowClearTextFlag;
    time_t             issuedTime;
    ChunkAccess        chunkAccess;
    const ChunkServer* chunkServer;
    int                validForTime;
    TokenSeq           tokenSeq;
    MetaLeaseRenew()
        : MetaRequest(META_LEASE_RENEW, false),
          leaseType(READ_LEASE),
          pathname(),
          chunkId(-1),
          leaseId(-1),
          emitCSAccessFlag(false),
          clientCSAllowClearTextFlag(false),
          issuedTime(),
          chunkAccess(),
          chunkServer(0),
          validForTime(0),
          tokenSeq(0),
          leaseTypeStr()
        {}
    virtual void handle();
    virtual void response(ReqOstream& os, IOBuffer& buf);
    virtual void setChunkServer(const ChunkServerPtr& cs)
        { chunkServer = cs.get(); }
    virtual ostream& ShowSelf(ostream& os) const
    {
        return (os <<
            (leaseType == READ_LEASE ? "read" : "write") <<
            " lease renew"
            " chunkId: " << chunkId <<
            " " << pathname
        );
    }
    bool Validate()
    {
        leaseType = (leaseTypeStr == "WRITE_LEASE") ? WRITE_LEASE : READ_LEASE;
        if (leaseType == READ_LEASE && leaseTypeStr != "READ_LEASE") {
            return false;
        }
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Lease-type",   "T", &MetaLeaseRenew::leaseTypeStr          )
        .Def2("Lease-id",     "L", &MetaLeaseRenew::leaseId,   int64_t(-1))
        .Def2("Chunk-handle", "H", &MetaLeaseRenew::chunkId, chunkId_t(-1))
        .Def2("CS-access",    "A", &MetaLeaseRenew::emitCSAccessFlag)
        ;
    }
private:
    StringBufT<32> leaseTypeStr;
};

/*!
 * \brief An internally generated op to force the cleanup of
 * dead leases thru the main event processing loop.
 */
struct MetaLeaseCleanup: public MetaRequest {
    MetaLeaseCleanup(seq_t s = -1, KfsCallbackObj *c = 0)
        : MetaRequest(META_LEASE_CLEANUP, false, s)
            { clnt = c; }

    virtual void handle();
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "lease cleanup";
    }
    bool Validate() { return true; }
};

/*!
 * \brief An internally generated op to check that the degree
 * of replication for each chunk is satisfactory.  This op goes
 * thru the main event processing loop.
 */
struct MetaChunkReplicationCheck : public MetaRequest {
    MetaChunkReplicationCheck(seq_t s, KfsCallbackObj *c)
        : MetaRequest(META_CHUNK_REPLICATION_CHECK, false, s)
            { clnt = c; }

    virtual void handle();
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "chunk replication check";
    }
};

struct MetaForceChunkReplication : public ServerLocation, public MetaRequest {
    chunkId_t chunkId;
    bool      recoveryFlag;
    bool      removeFlag;
    bool      forcePastEofRecoveryFlag;

    MetaForceChunkReplication()
        : ServerLocation(),
          MetaRequest(META_FORCE_CHUNK_REPLICATION, false),
          chunkId(-1),
          recoveryFlag(false),
          removeFlag(false),
          forcePastEofRecoveryFlag(false)
        {}
    virtual void handle();
    virtual void response(ReqOstream& os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        const ServerLocation& dst = *this;
        return os << "force replication:"
            " chunk: "             << chunkId <<
            " recovery: "          << recoveryFlag <<
            " remove: "            << removeFlag <<
            " forcePastEofRecov: " << forcePastEofRecoveryFlag <<
            " dst: "               << dst
        ;
    }
    bool Validate()
        { return true; }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Host",                 "H", &ServerLocation::hostname)
        .Def2("Port",                 "P", &ServerLocation::port,                              int(-1))
        .Def2("Chunk",                "C", &MetaForceChunkReplication::chunkId,            int64_t(-1))
        .Def2("Recovery",             "R", &MetaForceChunkReplication::recoveryFlag,             false)
        .Def2("Remove",               "X", &MetaForceChunkReplication::removeFlag,               false)
        .Def2("ForcePastEofRecovery", "E", &MetaForceChunkReplication::forcePastEofRecoveryFlag, false)
        ;
    }
};

struct MetaAck : public MetaRequest {
    StringBufT<32> ack;
    MetaAck()
        : MetaRequest(META_ACK, true),
          ack()
        {}
    bool Validate()
        { return (! ack.empty()); }
    virtual bool start();
    virtual void handle();
    virtual void response(ReqOstream& /* os */)
        { /* No response; */ }
    virtual int log(ostream& file) const;
    virtual ostream& ShowSelf(ostream& os) const
        { return (os << "ack: " << ack); }
    template<typename T> static T& ParserDef(T& parser)
    {
        return  MetaRequest::ParserDef(parser)
        .Def2("Ack", "a", &MetaAck::ack)
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("A", &MetaAck::ack)
        ;
    }
};

struct MetaLogConfig : public MetaRequest {
    const bool verifyAllOpsPermissionsFlag;
    MetaLogConfig(
        bool flag)
        : MetaRequest(META_LOG_CONFIG, true),
          verifyAllOpsPermissionsFlag(flag)
        {}
    virtual bool start()  { return true; }
    virtual void handle() { status = 0; }
    virtual void response(ReqOstream& /* os */)
        { /* No response; */ }
    virtual int log(ostream& file) const;
    virtual ostream& ShowSelf(ostream& os) const
    {
        return (os <<
            "log config: verify permissions: " << verifyAllOpsPermissionsFlag);
    }
};

int ParseCommand(const IOBuffer& buf, int len, MetaRequest **res,
    char* threadParseBuffer, bool shortRpcFmtFlag);
int ParseFirstCommand(const IOBuffer& ioBuf, int len, MetaRequest **res,
    char* threadParseBuffer, bool& shortRpcFmtFlag);

void printleaves();

void setClusterKey(const char *key);
void setMD5SumFn(const char *md5sumFn);
void setWORMMode(bool value);
void setMaxReplicasPerFile(int16_t value);
void setChunkmapDumpDir(string dir);
void CheckIfIoBuffersAvailable();
void SetRequestParameters(const Properties& props);

/* update counters for # of files/dirs/chunks in the system */
void UpdateNumDirs(int count);
void UpdateNumFiles(int count);
void UpdateNumChunks(int count);
void UpdatePathToFidCacheMiss(int count);
void UpdatePathToFidCacheHit(int count);
int64_t GetNumFiles();
int64_t GetNumDirs();

}
#endif /* !defined(KFS_REQUEST_H) */
