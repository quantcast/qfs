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
#include "MetaVrLogSeq.h"

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
#include "common/CIdChecksum.h"
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
using std::multimap;
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
    f(SET_GROUP_USERS) \
    f(ACK) \
    f(REMOVE_FROM_DUMPSTER) \
    f(LOG_CHUNK_ALLOCATE) \
    f(LOG_WRITER_CONTROL) \
    f(LOG_CLEAR_OBJ_STORE_DELETE) \
    f(READ_META_DATA) \
    f(CHUNK_OP_LOG_COMPLETION) \
    f(CHUNK_OP_LOG_IN_FLIGHT) \
    f(HIBERNATED_PRUNE) \
    f(HIBERNATED_REMOVE) \
    f(RESTART_PROCESS) \
    f(NOOP) \
    f(VR_HELLO) \
    f(VR_START_VIEW_CHANGE) \
    f(VR_DO_VIEW_CHANGE) \
    f(VR_START_VIEW) \
    f(VR_RECONFIGURATION) \
    f(VR_LOG_START_VIEW) \
    f(VR_GET_STATUS)

enum MetaOp {
#define KfsMakeMetaOpEnumEntry(name) META_##name,
    KfsForEachMetaOpId(KfsMakeMetaOpEnumEntry)
#undef KfsMakeMetaOpEnumEntry
    META_NUM_OPS_COUNT // must be the last one
};

class ChunkServer;
class ClientSM;
class LogWriter;
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
    template<typename T>
    class InsertServersT
    {
    public:
        InsertServersT(const T& s, char d = ' ')
            : srvs(s),
              sep(d)
            {}
        template<typename ST>
        friend ST& operator<<(ST& os, const InsertServersT& ics)
            { return Insert(ics.srvs.begin(), ics.srvs.end(), os, ics.sep); }
        friend ReqOstream& operator<<(ReqOstream& os, const InsertServersT& ics)
            { return Insert(ics.srvs.begin(), ics.srvs.end(), os, ics.sep); }
    private:
        const T&   srvs;
        const char sep;

        template<typename IT, typename ST>
        static ST& Insert(IT it, const IT& end, ST& os, char sep)
        {
            if (it != end) {
                os << (*it)->GetServerLocation();
                while (++it != end) {
                    os << sep << (*it)->GetServerLocation();
                }
            }
            return os;
        }
    };
    typedef InsertServersT<Servers> InsertServers;
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
    class GetNext
    {
    public:
        static MetaRequest*& Next(
            MetaRequest& inReq)
            { return inReq.next; }
    };
    enum LogAction
    {
        kLogNever,
        kLogQueue,
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
    MetaVrLogSeq    logseq;          //!< sequence no. in log
    LogAction       logAction;       //!< mutates metatree
    bool            suspended;       //!< is this request suspended somewhere
    bool            fromChunkServerFlag;
    bool            validDelegationFlag;
    bool            fromClientSMFlag;
    bool            shortRpcFormatFlag;
    bool            replayFlag;
    bool            commitPendingFlag;
    bool            replayBypassFlag;
    string          clientIp;
    IOBuffer        reqHeaders;
    kfsUid_t        authUid;
    kfsGid_t        authGid;
    kfsUid_t        euser;
    kfsGid_t        egroup;
    int64_t         maxWaitMillisec;
    int64_t         sessionEndTime;
    MetaRequest*    next;
    KfsCallbackObj* clnt;            //!< completion handler.

    MetaRequest(MetaOp o, LogAction la, seq_t opSeq = -1)
        : op(o),
          status(0),
          clientProtoVers(0),
          submitCount(0),
          submitTime(0),
          processTime(0),
          statusMsg(),
          opSeqno(opSeq),
          seqno(-1),
          logseq(),
          logAction(la),
          suspended(false),
          fromChunkServerFlag(false),
          validDelegationFlag(false),
          fromClientSMFlag(false),
          shortRpcFormatFlag(true),
          replayFlag(false),
          commitPendingFlag(false),
          replayBypassFlag(false),
          clientIp(),
          reqHeaders(),
          authUid(kKfsUserNone),
          authGid(kKfsGroupNone),
          euser(kKfsUserNone),
          egroup(kKfsGroupNone),
          maxWaitMillisec(-1),
          sessionEndTime(0),
          next(0),
          clnt(0),
          recursionCount(0)
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
    virtual bool log(ostream& file) const;
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
        .Def("p", &MetaRequest::clientProtoVers,     int(0))
        .Def("s", &MetaRequest::fromChunkServerFlag, false)
        .Def("u", &MetaRequest::euser,               kKfsUserNone)
        .Def("g", &MetaRequest::egroup,              kKfsGroupNone)
        .Def("a", &MetaRequest::authUid,             kKfsUserNone)
        .Def("z", &MetaRequest::logseq)
        .Def("x", &MetaRequest::shortRpcFormatFlag,  true)
        ;
        // Keep log sequence at the end of the line by using "z" key to
        // detect possibly truncated lines in the last log log segment.
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
        { return (req ? *req : GetNullReq()).Show(); }
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
    int* GetLogQueueCounter() const;
    const int GetRecursionCount() const
        { return recursionCount; }
    bool Write(ostream& os, bool omitDefaultsFlag = false) const;
    bool WriteLog(ostream& os, bool omitDefaultsFlag) const;
    void Submit();
    bool SubmitBegin();
    void SubmitEnd();
    static MetaRequest* ReadReplay(const char* buf, size_t len);
    static MetaRequest* Read(const char* buf, size_t len);
    static int GetId(const TokenValue& name);
    static TokenValue GetName(int id);
    static LogWriter& GetLogWriter()
        { return sLogWriter; }
    static bool Initialize();
protected:
    virtual void response(ReqOstream& /* os */) {}
    virtual ~MetaRequest();
    virtual void ReleaseSelf()
        { delete this; }
    void ResetSelf()
    {
        status              = 0;
        clientProtoVers     = 0;
        submitCount         = 0;
        submitTime          = 0;
        processTime         = 0;
        statusMsg           = string();
        opSeqno             = -1;
        seqno               = -1;
        logseq              = MetaVrLogSeq();
        logAction           = kLogNever;
        suspended           = false;
        fromChunkServerFlag = false;
        validDelegationFlag = false;
        fromClientSMFlag    = false;
        shortRpcFormatFlag  = false;
        replayFlag          = false;
        commitPendingFlag   = false;
        replayBypassFlag    = false;
        clientIp = string();
        reqHeaders.Clear();
        authUid             = kKfsUserNone;
        authGid             = kKfsGroupNone;
        euser               = kKfsUserNone;
        egroup              = kKfsGroupNone;
        maxWaitMillisec     = -1;
        sessionEndTime      = 0;
        next                = 0;
        clnt                = 0;
        recursionCount      = 0;
    }
private:
    int          recursionCount;
    MetaRequest* mPrevPtr[1];
    MetaRequest* mNextPtr[1];

    static LogWriter&         sLogWriter;
    static bool               sRequireHeaderChecksumFlag;
    static bool               sVerifyHeaderChecksumFlag;
    static int                sMetaRequestCount;
    static MetaRequest*       sMetaRequestsPtr[1];

    friend class QCDLListOp<MetaRequest, 0>;
    typedef QCDLList<MetaRequest, 0> MetaRequestsList;
    void Init();
    static const MetaRequest& GetNullReq();
protected:
    MetaRequest(const MetaRequest&);
    MetaRequest& operator=(const MetaRequest&);
};
inline static ostream& operator<<(ostream& os, const MetaRequest::Display& disp)
{ return disp.Show(os); }

inline static void submit_request(MetaRequest* r)
{ r->Submit(); }

struct MetaIdempotentRequest : public MetaRequest {
    // Derived classes' request() method must be const, i.e. not modify "this".
    seq_t reqId;
    seq_t ackId;
    MetaIdempotentRequest(MetaOp o, LogAction la, int rc = 1)
        : MetaRequest(o, la),
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
private:
    volatile int           ref;
    MetaIdempotentRequest* req;
};

struct MetaNoop : public MetaRequest {
    MetaNoop()
        : MetaRequest(META_NOOP, kLogIfOk)
        {}
    bool Validate()       { return true; }
    virtual bool start()  { return (0 == status); }
    virtual void handle() {}
    virtual ostream& ShowSelf(ostream& os) const
        { return (os << "no-op-request logseq: " << logseq); }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser);
    }
protected:
    virtual ~MetaNoop() {}
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
        : MetaRequest(META_LOOKUP, kLogNever),
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
        : MetaRequest(META_LOOKUP_PATH, kLogNever),
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
    int64_t    mtime;
    MetaCreate()
        : MetaIdempotentRequest(META_CREATE, kLogIfOk),
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
          groupName(),
          mtime()
        {}
    virtual bool start();
    virtual void handle();
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
        .Def("N",  &MetaCreate::name)
        .Def("O",  &MetaCreate::user,               kKfsUserNone)
        .Def("G",  &MetaCreate::group,              kKfsGroupNone)
        .Def("M",  &MetaCreate::mode,               kKfsModeUndef)
        .Def("TL", &MetaCreate::minSTier,           kKfsSTierMax)
        .Def("TH", &MetaCreate::maxSTier,           kKfsSTierMax)
        .Def("T",  &MetaCreate::mtime)
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
    int64_t    mtime;
    MetaMkdir()
        : MetaIdempotentRequest(META_MKDIR, kLogIfOk),
          dir(-1),
          fid(-1),
          user(kKfsUserNone),
          group(kKfsGroupNone),
          mode(kKfsModeUndef),
          minSTier(kKfsSTierMax),
          maxSTier(kKfsSTierMax),
          name(),
          ownerName(),
          groupName(),
          mtime()
        {}
    virtual bool start();
    virtual void handle();
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
        .Def("T",  &MetaMkdir::mtime)
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
    int64_t  mtime;
    MetaRemove()
        : MetaIdempotentRequest(META_REMOVE, kLogIfOk),
          dir(-1),
          name(),
          pathname(),
          todumpster(-1),
          mtime()
        {}
    virtual bool start();
    virtual void handle();
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
        .Def("T", &MetaRemove::mtime)
        ;
    }
};

/*!
 * \brief remove a directory
 */
struct MetaRmdir: public MetaIdempotentRequest {
    fid_t   dir;      //!< parent directory fid
    string  name;     //!< name to remove
    string  pathname; //!< full pathname to remove
    int64_t mtime;
    MetaRmdir()
        : MetaIdempotentRequest(META_RMDIR, kLogIfOk),
          dir(-1),
          name(),
          pathname(),
          mtime()
        {}
    virtual bool start();
    virtual void handle();
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
        .Def("T", &MetaRmdir::mtime)
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
        : MetaRequest(META_READDIR, kLogNever),
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
        : MetaRequest(META_READDIRPLUS, kLogNever),
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
    bool            objectStoreFlag;
    chunkId_t       chunkId;      //!< Id of the chunk corresponding to offset
    seq_t           chunkVersion; //!< version # assigned to this chunk
    ServerLocations locations;    //!< where the copies of the chunks are
    StringBufT<256> pathname;     //!< pathname of the file (useful to print in debug msgs)
    bool            replicasOrderedFlag;
    bool            allChunkServersShortRpcFlag;
    MetaGetalloc()
        : MetaRequest(META_GETALLOC, kLogNever),
          fid(-1),
          offset(-1),
          objectStoreFlag(false),
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
        .Def2("Obj-store",    "S", &MetaGetalloc::objectStoreFlag,  false)
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
        : MetaRequest(META_GETLAYOUT, kLogNever),
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
    chunkOff_t chunkPos;
    int64_t    leaseId;   //!< input
    chunkOff_t chunkSize;
    bool       hasChunkChecksum;
    uint32_t   chunkChecksum;
    MetaLeaseRelinquish()
        : MetaRequest(META_LEASE_RELINQUISH, kLogNever),
          leaseType(READ_LEASE),
          chunkId(-1),
          chunkPos(-1),
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
        .Def2("Chunk-pos",      "O", &MetaLeaseRelinquish::chunkPos,      chunkOff_t(-1))
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
    bool                 logChunkVersionChangeFailedFlag;
    TokenSeq             tokenSeq;
    time_t               issuedTime;
    int                  validForTime;
    CryptoKeys::KeyId    writeMasterKeyId;
    CryptoKeys::Key      writeMasterKey;
    TokenSeq             delegationSeq;
    uint32_t             delegationValidForTime;
    uint16_t             delegationFlags;
    int64_t              delegationIssuedTime;
    int64_t              leaseDuration;
    // With StringBufT instead of string the append allocation (presently
    // the most frequent allocation type) saves malloc() calls.
    StringBufT<64>       chunkServerName;
    StringBufT<64>       clientHost;   //!< the host from which request was received
    StringBufT<256>      pathname;     //!< full pathname that corresponds to fid
    MetaAllocate(seq_t s = -1, fid_t f = -1, chunkOff_t o = -1)
        : MetaRequest(META_ALLOCATE, kLogNever, s),
          KfsCallbackObj(),
          fid(f),
          offset(o),
          chunkId(-1),
          chunkVersion(-1),
          initialChunkVersion(-1),
          numReplicas(0),
          stripedFileFlag(false),
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
          logChunkVersionChangeFailedFlag(false),
          tokenSeq(),
          issuedTime(),
          validForTime(0),
          writeMasterKeyId(),
          writeMasterKey(),
          delegationSeq(-1),
          delegationValidForTime(0),
          delegationFlags(0),
          delegationIssuedTime(0),
          leaseDuration(-1),
          chunkServerName(),
          clientHost(),
          pathname(),
          startedFlag(false)
    {
        SET_HANDLER(this, &MetaAllocate::PendingLeaseRelinquish);
    }
    virtual ~MetaAllocate()
        { delete pendingLeaseRelinquish; }
    virtual void handle();
    virtual void response(ReqOstream& os);
    virtual ostream& ShowSelf(ostream& os) const;
    void responseSelf(ReqOstream& os);
    void LayoutDone(int64_t chunkAllocProcessTime);
    int PendingLeaseRelinquish(int code, void* data);
    bool ChunkAllocDone(const MetaChunkAllocate& chunkAlloc);
    void writeChunkAccess(ReqOstream& os);
    virtual bool dispatch(ClientSM& sm);
    void Done(bool countAllocTimeFlag, int64_t chunkAllocProcessTime);
    bool Validate()
    {
        return (fid >= 0 && (0 <= offset || appendChunk));
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
        .Def2("Chunk-master",   "C", &MetaAllocate::chunkServerName                 )
        ;
    }
private:
    bool startedFlag;
    bool CheckAllServersUp();
};

struct MetaLogChunkAllocate : public MetaRequest {
    MetaAllocate* const alloc;
    fid_t               fid;
    chunkOff_t          offset;
    chunkId_t           chunkId;
    seq_t               initialChunkVersion;
    seq_t               chunkVersion;
    int64_t             mtime;
    bool                appendChunk;
    bool                invalidateAllFlag;
    bool                objectStoreFileFlag;
    ServerLocations     servers;

    MetaLogChunkAllocate(
        MetaAllocate* a = 0)
        : MetaRequest(META_LOG_CHUNK_ALLOCATE, kLogIfOk),
          alloc(a),
          fid(-1),
          offset(-1),
          chunkId(-1),
          initialChunkVersion(-1),
          chunkVersion(1),
          mtime(0),
          appendChunk(false),
          invalidateAllFlag(false),
          objectStoreFileFlag(false),
          servers()
        {}
    bool Validate() { return true; }
    virtual bool start();
    virtual void handle();
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "log allocate: " << ShowReq(alloc);
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        // Set chunk version default to  1 to reduce log space, by omitting the
        // most common value.
        return MetaRequest::LogIoDef(parser)
        .Def("P", &MetaLogChunkAllocate::fid,                 fid_t(-1))
        .Def("O", &MetaLogChunkAllocate::offset,              chunkOff_t(-1))
        .Def("C", &MetaLogChunkAllocate::chunkId,             chunkId_t(-1))
        .Def("B", &MetaLogChunkAllocate::initialChunkVersion, seq_t(-1))
        .Def("V", &MetaLogChunkAllocate::chunkVersion,        seq_t(1))
        .Def("M", &MetaLogChunkAllocate::mtime,               int64_t(0))
        .Def("A", &MetaLogChunkAllocate::appendChunk,         false)
        .Def("I", &MetaLogChunkAllocate::invalidateAllFlag,   false)
        .Def("X", &MetaLogChunkAllocate::objectStoreFileFlag, false)
        .Def("S", &MetaLogChunkAllocate::servers)
        ;
    }
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
        : MetaRequest(META_TRUNCATE, kLogIfOk),
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
        .Def("T", &MetaTruncate::mtime)
        ;
    }
};

/*!
 * \brief rename a file or directory
 */
struct MetaRename: public MetaIdempotentRequest {
    fid_t   dir;        //!< parent directory
    string  oldname;    //!< old file name
    string  newname;    //!< new file name
    string  oldpath;    //!< fully-qualified old pathname
    bool    overwrite;  //!< overwrite newname if it exists
    bool    wormModeFlag;
    fid_t   todumpster; //!< moved original to dumpster
    int64_t mtime;
    MetaRename()
        : MetaIdempotentRequest(META_RENAME, kLogIfOk),
          dir(-1),
          oldname(),
          newname(),
          oldpath(),
          overwrite(false),
          wormModeFlag(false),
          todumpster(-1),
          mtime()
        {}
    virtual bool start();
    virtual void handle();
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
        .Def("P", &MetaRename::dir,      fid_t(-1))
        .Def("O", &MetaRename::oldname            )
        .Def("N", &MetaRename::newname            )
        .Def("F", &MetaRename::oldpath            )
        .Def("W", &MetaRename::overwrite,    false)
        .Def("M", &MetaRename::wormModeFlag, false)
        .Def("T", &MetaRename::mtime)
        ;
    }
};

/*!
 * \brief set the mtime for a file or directory
 */
struct MetaSetMtime: public MetaRequest {
    fid_t      dir;
    fid_t      fid;      //!< stash the fid for logging
    string     pathname; //!< absolute path for which we want to set the mtime
    int64_t    mtime;
    MetaSetMtime(fid_t id = -1, int64_t mtime = 0)
        : MetaRequest(META_SETMTIME, kLogIfOk),
          dir(ROOTFID),
          fid(id),
          pathname(),
          mtime(mtime),
          sec(0),
          usec(0)
        {}
    virtual bool start();
    virtual void handle();
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
        return (! pathname.empty() && 0 <= dir);
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Mtime-sec",  "S", &MetaSetMtime::sec     )
        .Def2("Mtime-usec", "U", &MetaSetMtime::usec    )
        .Def2("Pathname",   "N", &MetaSetMtime::pathname)
        .Def2("Parent-dir", "D", &MetaSetMtime::dir,   fid_t(-1))
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("P", &MetaSetMtime::fid, fid_t(-1))
        .Def("N", &MetaSetMtime::pathname)
        .Def("M", &MetaSetMtime::mtime)
        .Def("D", &MetaSetMtime::dir, ROOTFID)
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
    int16_t    maxRSFileReplicas;
    int16_t    maxFileReplicas;
    MetaChangeFileReplication()
        : MetaRequest(META_CHANGE_FILE_REPLICATION, kLogIfOk),
          fid(-1),
          numReplicas(1),
          minSTier(kKfsSTierUndef),
          maxSTier(kKfsSTierUndef),
          maxRSFileReplicas(-1),
          maxFileReplicas(-1)
        {}
    virtual bool start();
    virtual void handle();
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
        .Def("P",  &MetaChangeFileReplication::fid,               fid_t(-1))
        .Def("R",  &MetaChangeFileReplication::numReplicas,       int16_t(1))
        .Def("TL", &MetaChangeFileReplication::minSTier,          kKfsSTierUndef)
        .Def("TH", &MetaChangeFileReplication::maxSTier,          kKfsSTierUndef)
        .Def("RR", &MetaChangeFileReplication::maxRSFileReplicas, int16_t(-1))
        .Def("RF", &MetaChangeFileReplication::maxFileReplicas,   int16_t(-1))
        ;
    }
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
        : MetaRequest(META_COALESCE_BLOCKS, kLogIfOk),
          srcPath(),
          dstPath(),
          srcFid(-1),
          dstFid(-1),
          dstStartOffset(-1),
          numChunksMoved(0),
          mtime()
        {}
    virtual bool start();
    virtual void handle();
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
        .Def("M", &MetaCoalesceBlocks::mtime)
        ;
    }
};

/*!
 * \brief Notification to hibernate/retire a chunkserver:
 * Hibernation: when the server is put
 * in hibernation mode, the server is taken down temporarily with a promise that
 * it will come back N secs later; if the server doesn't come up as promised
 * then re-replication starts.
 *
 * Retirement: is extended downtime.  The server is taken down and we don't know
 * if it will ever come back.  In this case, we use this server (preferably)
 * to evacuate/re-replicate all the blocks off it before we take it down.
 */

struct MetaRetireChunkserver : public MetaRequest, public ServerLocation {
    ServerLocation& location;  //<! Location of this server
    int             nSecsDown; //<! -1 to retire; otherwise secs of down time.
    int64_t         startTime;
    MetaRetireChunkserver()
        : MetaRequest(META_RETIRE_CHUNKSERVER, kLogIfOk),
          ServerLocation(),
          location(*this),
          nSecsDown(-1),
          startTime(0)
        {}
    virtual bool start();
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
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("H", &MetaRetireChunkserver::hostname     )
        .Def("P", &MetaRetireChunkserver::port,      -1)
        .Def("D", &MetaRetireChunkserver::nSecsDown, -1)
        .Def("S", &MetaRetireChunkserver::startTime, int64_t(0))
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
    typedef vector<chunkId_t>                           ChunkIdList;

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
    int                numPendingStaleChunks;
    int                contentLength;            //!< Length of the message body
    int64_t            numAppendsWithWid;
    int                contentIntBase;
    ChunkInfos         chunks;                   //!< Chunks  hosted on this server
    ChunkInfos         notStableChunks;
    ChunkInfos         notStableAppendChunks;
    ChunkIdList        missingChunks;
    ChunkIdList        pendingStaleChunks;
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
    bool               pendingNotifyFlag;
    int                resumeStep;
    int                bufferBytes;
    int64_t            totalChunks;
    size_t             deletedCount;
    size_t             modifiedCount;
    size_t             chunkCount;
    int64_t            reReplicationCount;
    CIdChecksum        checksum;
    int64_t            timeUsec;
    bool               retireFlag;
    int                maxPendingOpsCount;
    IOBuffer           responseBuf;

    MetaHello()
        : MetaRequest(META_HELLO, kLogQueue),
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
          numPendingStaleChunks(0),
          contentLength(0),
          numAppendsWithWid(0),
          contentIntBase(10),
          chunks(),
          notStableChunks(),
          notStableAppendChunks(),
          missingChunks(),
          pendingStaleChunks(),
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
          pendingNotifyFlag(false),
          resumeStep(-1),
          bufferBytes(0),
          totalChunks(0),
          deletedCount(0),
          modifiedCount(0),
          chunkCount(0),
          reReplicationCount(0),
          checksum(),
          timeUsec(-1),
          retireFlag(false),
          maxPendingOpsCount(128),
          responseBuf()
        {}
    virtual bool start();
    virtual void handle();
    virtual void response(ReqOstream& os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return (os <<
            "chunk server hello: " << location <<
            " logseq: "    << logseq <<
            " resume: "    << resumeStep <<
            " chunks: "
            " stable: "    << chunks.size() <<
            " not stable " << notStableAppendChunks.size() <<
            " missing: "   << missingChunks.size() <<
            " count: "     << chunkCount <<
            " checksum: "  << checksum
        );
    }
    virtual bool log(ostream& os) const;
    bool Validate()
    {
        return (ServerLocation::IsValid() &&
            (contentIntBase == 10 || contentIntBase == 16));
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Short-rpc-fmt",                "f",  &MetaRequest::shortRpcFormatFlag            )
        .Def2("Chunk-server-name",            "SN", &ServerLocation::hostname                   )
        .Def2("Chunk-server-port",            "SP", &ServerLocation::port,               int(-1))
        .Def2("Cluster-key",                  "CK", &MetaHello::clusterKey                      )
        .Def2("MD5Sum",                       "5",  &MetaHello::md5sum                          )
        .Def2("Total-space",                  "T",  &MetaHello::totalSpace,           int64_t(0))
        .Def2("Total-fs-space",               "TF", &MetaHello::totalFsSpace,         int64_t(0))
        .Def2("Used-space",                   "US", &MetaHello::usedSpace,            int64_t(0))
        .Def2("Rack-id",                      "RI", &MetaHello::rackId,                  int(-1))
        .Def2("Uptime",                       "UP", &MetaHello::uptime,               int64_t(0))
        .Def2("Num-chunks",                   "NC", &MetaHello::numChunks,                int(0))
        .Def2("Num-not-stable-append-chunks", "NA", &MetaHello::numNotStableAppendChunks, int(0))
        .Def2("Num-not-stable-chunks",        "NS", &MetaHello::numNotStableChunks,       int(0))
        .Def2("Num-appends-with-wids",        "AW", &MetaHello::numAppendsWithWid,    int64_t(0))
        .Def2("Content-length",               "l",  &MetaHello::contentLength,            int(0))
        .Def2("Content-int-base",             "IB", &MetaHello::contentIntBase,          int(10))
        .Def2("Stale-chunks-hex-format",      "SX", &MetaHello::staleChunksHexFormatFlag,  false)
        .Def2("CKeyId",                       "KI", &MetaHello::cryptoKeyId)
        .Def2("CKey",                       "CKey", &MetaHello::cryptoKey)
        .Def2("FsId",                         "FI", &MetaHello::fileSystemId,        int64_t(-1))
        .Def2("NoFids",                       "NF", &MetaHello::noFidsFlag,                false)
        .Def2("Resume",                       "R",  &MetaHello::resumeStep,              int(-1))
        .Def2("Deleted",                      "D",  &MetaHello::deletedCount                    )
        .Def2("Modified",                     "M",  &MetaHello::modifiedCount                   )
        .Def2("Chunks",                       "C",  &MetaHello::chunkCount                      )
        .Def2("Checksum",                     "K",  &MetaHello::checksum                        )
        .Def2("Num-missing",                  "CM", &MetaHello::numMissingChunks                )
        .Def2("Num-stale",                    "PS", &MetaHello::numPendingStaleChunks           )
        .Def2("Num-hello-done",               "HD", &MetaHello::helloDoneCount                  )
        .Def2("Num-resume",                   "NR", &MetaHello::helloResumeCount                )
        .Def2("Num-resume-fail",              "RF", &MetaHello::helloResumeFailedCount          )
        .Def2("Num-re-replications",          "RR", &MetaHello::reReplicationCount              )
        .Def2("Pending-notify",               "PN", &MetaHello::pendingNotifyFlag,         false)
        .Def2("Total-chunks",                 "TC", &MetaHello::totalChunks,          int64_t(0))
        ;
    }
};

/*!
 * \brief whenever a chunk server goes down, this message is used to clean up state.
 */
struct MetaBye: public MetaRequest {
    ChunkServerPtr server; //!< The chunkserver that went down
    ServerLocation location;
    size_t         chunkCount;
    CIdChecksum    cIdChecksum;
    int64_t        timeUsec;  // current / start time.
    int64_t        logInFlightCount;
    int            replicationDelay;
    bool           completionInFlightFlag;

    enum { kDefaultReplicationDelay = 60 };

    MetaBye(const ChunkServerPtr& c = ChunkServerPtr())
        : MetaRequest(META_BYE, kLogIfOk, 0),
          server(c),
          location(),
          chunkCount(0),
          cIdChecksum(),
          timeUsec(-1),
          logInFlightCount(0),
          replicationDelay(kDefaultReplicationDelay),
          completionInFlightFlag(false)
        {}
    virtual bool start();
    virtual void handle();
    virtual ostream& ShowSelf(ostream& os) const
    {
        return (os <<
            "chunk server bye: " << location <<
            " logseq: "          << logseq <<
            " chunks: "          << chunkCount <<
            " checksum: "        << cIdChecksum <<
            " log:"
            " in flight: "       << logInFlightCount <<
            " repl delay: "      << replicationDelay <<
            " completion: "      << (completionInFlightFlag ? "yes" : "no")
        );
    }
    bool Validate()
    {
        return location.IsValid();
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("C", &MetaBye::location)
        .Def("K", &MetaBye::cIdChecksum)
        .Def("S", &MetaBye::chunkCount,             size_t(0))
        .Def("T", &MetaBye::timeUsec,               int64_t(-1))
        .Def("L", &MetaBye::logInFlightCount,       int64_t(0))
        .Def("I", &MetaBye::completionInFlightFlag, false)
        .Def("D", &MetaBye::replicationDelay,       int(kDefaultReplicationDelay))
        ;
    }
};

struct MetaGetPathName: public MetaRequest {
    fid_t     fid;
    chunkId_t chunkId;
    MFattr    fattr;
    string    result;
    MetaGetPathName()
        : MetaRequest(META_GETPATHNAME, kLogNever),
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
        : MetaRequest(META_CHMOD, kLogIfOk),
          fid(-1),
          mode(kKfsModeUndef)
        {}
    virtual bool start();
    virtual void handle();
    virtual void response(ReqOstream &os);
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
        : MetaRequest(META_CHOWN, kLogIfOk),
          fid(-1),
          user(kKfsUserNone),
          group(kKfsGroupNone),
          ownerName(),
          groupName()
        {}
    virtual bool start();
    virtual void handle();
    virtual void response(ReqOstream &os);
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
    bool Validate() { return (0 <= fid); }
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

struct MetaChunkRequest;

struct MetaChunkLogCompletion : public MetaRequest {
    enum ChunkOpType
    {
        kChunkOpTypeNone = 0,
        kChunkOpTypeAdd  = 1
    };

    ServerLocation    doneLocation;
    MetaVrLogSeq      doneLogSeq;
    int               doneStatus;
    int               doneKfsStatus;
    bool              doneTimedOutFlag;
    MetaChunkRequest* doneOp;
    chunkId_t         chunkId;
    seq_t             chunkVersion;
    int               chunkOpType;
    bool              staleChunkIdFlag;
    bool              flushStaleQueueFlag;

    MetaChunkLogCompletion(MetaChunkRequest* op = 0);
    virtual bool start();
    virtual void handle();
    virtual ostream& ShowSelf(ostream& os) const;
    bool Validate()
    {
        return (
            doneLogSeq.IsValid() &&
            doneLocation.IsValid() &&
            kChunkOpTypeNone <= chunkOpType &&
            chunkOpType <= kChunkOpTypeAdd
        );
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("S", &MetaChunkLogCompletion::doneLocation)
        .Def("L", &MetaChunkLogCompletion::doneLogSeq)
        .Def("R", &MetaChunkLogCompletion::doneKfsStatus,       0)
        .Def("T", &MetaChunkLogCompletion::doneTimedOutFlag,    false)
        .Def("C", &MetaChunkLogCompletion::chunkId,             chunkId_t(-1))
        .Def("V", &MetaChunkLogCompletion::chunkVersion,        seq_t(-1))
        .Def("O", &MetaChunkLogCompletion::chunkOpType,         int(kChunkOpTypeNone))
        .Def("X", &MetaChunkLogCompletion::staleChunkIdFlag,    false)
        .Def("F", &MetaChunkLogCompletion::flushStaleQueueFlag, false)
        ;
    }
};

/*!
 * \brief RPCs that go from meta server->chunk server are
 * MetaRequest's that define a method to generate the RPC
 * request.
 */
struct MetaChunkRequest : public MetaRequest {
    chunkId_t            chunkId;
    const ChunkServerPtr server; // The "owner".
    seq_t                chunkVersion;
    MetaVrLogSeq         logCompletionSeq;
    bool                 pendingAddFlag;
    bool                 timedOutFlag;
    bool                 staleChunkIdFlag;
private:
    typedef multimap <
        chunkId_t,
        const MetaChunkRequest*,
        less<chunkId_t>,
        StdFastAllocator<
            pair<const chunkId_t, const MetaChunkRequest*>
        >
    > ChunkOpsInFlight;
    typedef QCDLList<MetaChunkRequest, 0> List;

    ChunkOpsInFlight::iterator inFlightIt;
    MetaChunkRequest*          mPrevPtr[1];
    MetaChunkRequest*          mNextPtr[1];
    friend class QCDLListOp<MetaChunkRequest, 0>;
    friend class ChunkServer;
    friend bool MetaRequest::Initialize();

    static ChunkOpsInFlight::iterator MakeNullIterator();
protected:
    MetaChunkRequest(MetaOp o, seq_t s, LogAction la,
            const ChunkServerPtr& c, chunkId_t cid)
        : MetaRequest(o, la, s),
          chunkId(cid),
          server(c),
          chunkVersion(0),
          logCompletionSeq(),
          pendingAddFlag(false),
          timedOutFlag(false),
          staleChunkIdFlag(false),
          inFlightIt(kNullIterator)
        { List::Init(*this); }
public:
    static const ChunkOpsInFlight::iterator kNullIterator;
    virtual ~MetaChunkRequest();
    //!< generate a request message (in string format) as per the
    //!< KFS protocol.
    virtual void request(ReqOstream& os, IOBuffer& /* buf */) { request(os); }
    virtual void handleReply(const Properties& prop) {}
    virtual void handle() {}
    void resume()
    {
        if (logCompletionSeq.IsValid()) {
            submit_request(new MetaChunkLogCompletion(this));
        } else {
            suspended = false;
            submit_request(this);
        }
    }
    virtual const ChunkIdQueue* GetChunkIds() const { return 0; }
protected:
    virtual void request(ReqOstream& /* os */) {}
};

struct MetaChunkLogInFlight : public MetaChunkRequest {
    ServerLocation    location;
    ChunkIdQueue      chunkIds;
    int64_t           idCount;
    bool              removeServerFlag;
    MetaChunkRequest* request;
    int               reqType;

    static bool IsToBeLogged(const MetaChunkRequest& req) {
        return  (
            ! req.replayFlag &&
            0 == req.status &&
            kLogIfOk != req.logAction &&
            kLogAlways != req.logAction &&
            0 <= req.chunkVersion && // do not log object store RPCs
            (0 <= req.chunkId || req.GetChunkIds())
        );
    }
    static bool Log(MetaChunkRequest& req, int timeout, bool removeServerFlag);
    static bool Checkpoint(ostream& os, const MetaChunkRequest& req);
    MetaChunkLogInFlight(
        MetaChunkRequest* req        = 0,
        int               tmeout     = -1,
        bool              removeFlag = false);
    virtual bool start();
    virtual void handle();
    virtual bool log(ostream& os) const;
    virtual ostream& ShowSelf(ostream& os) const;
    virtual const ChunkIdQueue* GetChunkIds() const
    {
        return (request ? request->GetChunkIds() :
            (chunkIds.IsEmpty() ? 0 : &chunkIds));
    }
    static const char* GetReqName(int id);
    static int GetReqId(const char* name, size_t len);
private:
    struct NameTable
    {
        int        id;
        const char name[8];
    };
    static NameTable const sNameTable[];
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
        : MetaChunkRequest(META_CHUNK_ALLOCATE, n, kLogNever, s, r->chunkId),
          leaseId(l),
          minSTier(minTier),
          maxSTier(maxTier),
          chunkServerAccessStr(),
          chunkAccessStr(),
          req(r)
          { chunkVersion = req->chunkVersion; }
    virtual void handle();
    virtual void request(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "meta->chunk allocate: " << ShowReq(req);
    }
};

/*!
 * \brief Delete RPC from meta server to chunk server
 */
struct MetaChunkDelete: public MetaChunkRequest {
    bool deleteStaleChunkIdFlag;
    MetaChunkDelete(
        seq_t                 n,
        const ChunkServerPtr& s,
        chunkId_t             c,
        seq_t                 v,
        bool                  staleIdFlag)
        : MetaChunkRequest(META_CHUNK_DELETE, n, kLogNever, s, c),
          deleteStaleChunkIdFlag(staleIdFlag)
        { chunkVersion = v; }
    virtual void handle();
    virtual void request(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return (os << "meta->chunk delete:"
            " chunkId: " << chunkId <<
            " version: " << chunkVersion <<
            " staleId: " << deleteStaleChunkIdFlag);
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
        : MetaChunkRequest(META_CHUNK_REPLICATE, n, kLogNever, s, c),
          fid(f),
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
    seq_t               fromVersion;
    bool                makeStableFlag;
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
        : MetaChunkRequest(META_CHUNK_VERSCHANGE, n, kLogNever, s, c),
          fid(f),
          fromVersion(fromVers),
          makeStableFlag(mkStableFlag),
          verifyStableFlag(verifyStblFlag),
          replicate(repl)
    {
        pendingAddFlag = pendAddFlag;
        chunkVersion   = v;
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
            " make stable: "    << makeStableFlag <<
            " verify stable: "  << verifyStableFlag
        ;
    }
};

/*!
 * \brief As a chunkserver for the size of a particular chunk.  We use this RPC
 * to compute the filesize: whenever the lease on the last chunk of the file
 * expires, we get the chunk's size and then determine the filesize.
 */
struct MetaChunkSize: public MetaChunkRequest {
    fid_t      fid; // redundant, for debug purposes only.
    chunkOff_t chunkSize; //!< output: the chunk size
    bool       retryFlag;
    bool       checkChunkFlag;
    seq_t      replyChunkVersion;
    bool       stableFlag;
    MetaChunkSize(
            seq_t                 n = 0,
            const ChunkServerPtr& s = ChunkServerPtr(),
            fid_t                 f = -1,
            chunkId_t             c = -1,
            seq_t                 v = -1,
            bool                  retry = false)
        : MetaChunkRequest(META_CHUNK_SIZE, n, kLogIfOk, s, c),
          fid(f),
          chunkSize(-1),
          retryFlag(retry),
          checkChunkFlag(false),
          replyChunkVersion(-1),
          stableFlag(false)
        { chunkVersion = v; }
    bool Validate() { return true; }
    virtual bool start();
    virtual void handle();
    virtual void request(ReqOstream &os);
    virtual void handleReply(const Properties& prop)
    {
        chunkSize = prop.getValue(
            shortRpcFormatFlag ? "S"  : "Size",          chunkOff_t(-1));
        replyChunkVersion = prop.getValue(
            shortRpcFormatFlag ? "V"  : "Chunk-version", seq_t(-1));
        stableFlag = prop.getValue(
            shortRpcFormatFlag ? "SC" : "Stable-flag",   false);
    }
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            " get size:"
            " fid: "           << fid <<
            " chunkId: "       << chunkId <<
            " chunkVersion: "  << chunkVersion <<
            " size: "          << chunkSize <<
            " retry: "         << retryFlag <<
            " check: "         << checkChunkFlag <<
            " stable: "        << stableFlag <<
            " targetVersion: " << replyChunkVersion
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        //.Def("P", &MetaChunkSize::fid,          fid_t(-1))
        .Def("V", &MetaChunkSize::chunkVersion, seq_t(-1))
        .Def("C", &MetaChunkSize::chunkId,      chunkId_t(-1))
        .Def("S", &MetaChunkSize::chunkSize,    chunkOff_t(-1))
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
    int     maxPendingOpsCount;
    MetaChunkHeartbeat(seq_t n, const ChunkServerPtr& s,
            int64_t evacuateCnt, bool reAuthFlag, int maxPendingOpsCnt)
        : MetaChunkRequest(META_CHUNK_HEARTBEAT, n, kLogNever, s, -1),
          evacuateCount(evacuateCnt),
          reAuthenticateFlag(reAuthFlag),
          maxPendingOpsCount(maxPendingOpsCnt)
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
struct MetaChunkAvailable;
struct MetaChunkStaleNotify: public MetaChunkRequest {
    ChunkIdQueue staleChunkIds; //!< chunk ids that are stale
    bool         evacuatedFlag;
    bool         hexFormatFlag;
    bool         flushStaleQueueFlag;
    MetaChunkStaleNotify(seq_t n, const ChunkServerPtr& s,
            bool evacFlag, bool hexFmtFlag, MetaChunkAvailable* req);
    virtual void request(ReqOstream& os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const;
    virtual const ChunkIdQueue* GetChunkIds() const { return &staleChunkIds; }
protected:
    virtual void ReleaseSelf();
    MetaChunkAvailable* chunkAvailableReq;
};

struct MetaBeginMakeChunkStable : public MetaChunkRequest {
    const fid_t          fid;           // input
    const ServerLocation serverLoc;     // processing this cmd
    int64_t              chunkSize;     // output
    uint32_t             chunkChecksum; // output
    MetaBeginMakeChunkStable(seq_t n, const ChunkServerPtr& s,
            const ServerLocation& l, fid_t f, chunkId_t c, seq_t v) :
        MetaChunkRequest(META_BEGIN_MAKE_CHUNK_STABLE, n, kLogNever, s, c),
        fid(f),
        serverLoc(l),
        chunkSize(-1),
        chunkChecksum(0)
        { chunkVersion = v; }
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

struct MetaLogMakeChunkStable : public MetaRequest {
    fid_t     fid;              // input
    chunkId_t chunkId;          // input
    seq_t     chunkVersion;     // input
    int64_t   chunkSize;        // input
    uint32_t  chunkChecksum;    // input
    bool      hasChunkChecksum; // input
    MetaLogMakeChunkStable(
        fid_t     fileId          = -1,
        chunkId_t id              = -1,
        seq_t     version         = 1,
        int64_t   size            = -1,
        bool      hasChecksum     = false,
        uint32_t  checksum        = 0,
        seq_t     seqNum          = -1,
        MetaOp    op              = META_LOG_MAKE_CHUNK_STABLE)
        : MetaRequest(op, kLogIfOk, seqNum),
          fid(fileId),
          chunkId(id),
          chunkVersion(version),
          chunkSize(size),
          chunkChecksum(checksum),
          hasChunkChecksum(hasChecksum)
        {}
    bool Validate() { return true; }
    virtual bool start()
    {
        if (chunkVersion < 0) {
            status = -EINVAL;
        }
        return (0 == status);
    }
    virtual void handle();
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
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("P", &MetaLogMakeChunkStable::fid,              fid_t(-1))
        .Def("C", &MetaLogMakeChunkStable::chunkId,          chunkId_t(-1))
        .Def("V", &MetaLogMakeChunkStable::chunkVersion,     seq_t(1))
        .Def("S", &MetaLogMakeChunkStable::chunkSize,        int64_t(-1))
        .Def("K", &MetaLogMakeChunkStable::chunkChecksum,    uint32_t(0))
        .Def("H", &MetaLogMakeChunkStable::hasChunkChecksum, false)
        ;
    }
};

struct MetaLogMakeChunkStableDone : public MetaLogMakeChunkStable {
    MetaLogMakeChunkStableDone(
        fid_t     fileId          = -1,
        chunkId_t id              = -1,
        seq_t     version         = 1,
        int64_t   size            = -1,
        bool      hasChecksum     = false,
        uint32_t  checksum        = 0,
        seq_t     seqNum          = -1)
        : MetaLogMakeChunkStable(fileId, id, version, size, hasChecksum,
            checksum, seqNum, META_LOG_MAKE_CHUNK_STABLE_DONE)
        {}
    virtual void handle();
};

struct MetaLogChunkVersionChange : public MetaRequest {
    MetaAllocate* const alloc;
    fid_t               fid;
    chunkId_t           chunkId;
    seq_t               chunkVersion;
    MetaLogChunkVersionChange(MetaAllocate* a = 0)
        : MetaRequest(
            META_LOG_CHUNK_VERSION_CHANGE, kLogIfOk,
                a ? a->opSeqno : seq_t(-1)),
          alloc(a),
          fid(-1),
          chunkId(-1),
          chunkVersion(-1)
        {}
    bool Validate()
        { return true; }
    virtual bool start();
    virtual void handle();
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "log-chunk-version-change: " <<
            ShowReq(alloc)
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("P", &MetaLogChunkVersionChange::fid,          fid_t(-1))
        .Def("C", &MetaLogChunkVersionChange::chunkId,      chunkId_t(-1))
        .Def("V", &MetaLogChunkVersionChange::chunkVersion, seq_t(-1))
        ;
    }
};

/*!
 * \brief Notification message from meta->chunk asking the server to make a
 * chunk.  This tells the chunk server that the writes to a chunk are done and
 * that the chunkserver should flush any dirty data.
 */
struct MetaChunkMakeStable: public MetaChunkRequest {
    const fid_t      fid;   //!< input: we tell the chunkserver what it is
    const chunkOff_t chunkSize;
    const bool       hasChunkChecksum;
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
                inSeqNo, kLogNever, inServer, inChunkId),
          fid(inFileId),
          chunkSize(inChunkSize),
          hasChunkChecksum(inHasChunkChecksum),
          chunkChecksum(inChunkChecksum)
    {
        pendingAddFlag = inAddPending;
        chunkVersion   = inChunkVersion;
    }
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
        MetaChunkRequest(META_CHUNK_RETIRE, n, kLogQueue, s, -1) { }
    virtual void request(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "chunkserver-retire";
    }
};

struct MetaChunkSetProperties: public MetaChunkRequest {
    const string serverProps;
    MetaChunkSetProperties(seq_t n, const ChunkServerPtr& s,
            const Properties& props)
        : MetaChunkRequest(META_CHUNK_SET_PROPERTIES, n, kLogQueue, s, -1),
          serverProps(Properties2Str(props))
        {}
    virtual void request(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "chunkserver-set-properties";
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
        : MetaChunkRequest(META_CHUNK_SERVER_RESTART, n, kLogQueue, s, -1)
        {}
    virtual void request(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "chunkserver-restart";
    }
};

/*!
 * \brief For monitoring purposes, a client/tool can send a PING
 * request.  In response, the server replies with the list of all
 * connected chunk servers and their locations as well as some state
 * about each of those servers.
 */
struct MetaPing : public MetaRequest {
    // Run through the log writer to get VR status.
    IOBuffer   resp;
    bool       updateFlag;
    vrNodeId_t vrNodeId;
    vrNodeId_t vrPrimaryNodeId;
    bool       vrActiveFlag;
    MetaPing()
        : MetaRequest(META_PING, kLogIfOk),
          resp(),
          updateFlag(false),
          vrNodeId(-1),
          vrPrimaryNodeId(-1),
          vrActiveFlag(false)
    {
        // Suppress warning with requests with no version filed.
        clientProtoVers  = KFS_CLIENT_PROTO_VERS;
        replayBypassFlag = true;
    }
    virtual bool start();
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
        : MetaRequest(META_UPSERVERS, kLogNever),
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
        : MetaRequest(META_TOGGLE_WORM, kLogIfOk),
          value(false)
        {}
    virtual bool start();
    virtual void handle();
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            (value ? "toggle-WORM-on" : "toggle-WORM-off");
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
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("T", &MetaToggleWORM::value, false)
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
        : MetaRequest(META_STATS, kLogNever),
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
        : MetaRequest(META_RECOMPUTE_DIRSIZE, kLogNever)
        {}
    virtual void handle();
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
        : MetaRequest(META_DUMP_CHUNKTOSERVERMAP, kLogNever),
          chunkmapFile(),
          pid(-1)
        {}
    virtual void handle();
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "dump-chunk2server-map";
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
        : MetaRequest(META_CHECK_LEASES, kLogNever)
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
        : MetaRequest(META_DUMP_CHUNKREPLICATIONCANDIDATES, kLogNever),
          numReplication(0),
          numPendingRecovery(0),
          resp()
        {}
    virtual void handle();
    virtual void response(ReqOstream &os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "dump-chunk-replication-candidates";
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
        : MetaRequest(META_FSCK, kLogNever),
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
        : MetaRequest(META_OPEN_FILES, kLogNever),
          openForReadCnt(0),
          openForWriteCnt(0),
          resp()
        {}
    virtual void handle();
    virtual void response(ReqOstream& os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "open-files";
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
        : MetaRequest(META_SET_CHUNK_SERVERS_PROPERTIES, kLogNever),
          properties()
        {}
    virtual void handle();
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        string ret("set-chunk-servers-properties ");
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
        : MetaRequest(META_GET_CHUNK_SERVERS_COUNTERS, kLogNever),
          resp()
    {
        // Suppress warning with requests with no version filed.
        clientProtoVers = KFS_CLIENT_PROTO_VERS;
    }
    virtual void handle();
    virtual void response(ReqOstream &os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "get-chunk-servers-counters";
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
        : MetaRequest(META_GET_CHUNK_SERVER_DIRS_COUNTERS, kLogNever),
          resp()
    {
        // Suppress warning with requests with no version filed.
        clientProtoVers = KFS_CLIENT_PROTO_VERS;
    }
    virtual void handle();
    virtual void response(ReqOstream &os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "get-chunk-servers-dir-counters";
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
        : MetaRequest(META_GET_REQUEST_COUNTERS, kLogNever),
          resp(),
          userCpuMicroSec(0),
          systemCpuMicroSec(0)
        {}
    virtual void handle();
    virtual void response(ReqOstream &os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "get-request-counters";
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

struct MetaLogWriterControl;

struct MetaCheckpoint : public MetaRequest {
    MetaCheckpoint(seq_t s, KfsCallbackObj* c)
        : MetaRequest(META_CHECKPOINT, kLogNever, s),
          lockFileName(),
          lockFd(-1),
          intervalSec(60 * 60),
          pid(-1),
          failedCount(0),
          maxFailedCount(2),
          checkpointWriteTimeoutSec(60 * 60),
          flushNewViewDelaySec(10),
          checkpointWriteSyncFlag(true),
          checkpointWriteBufferSize(16 << 20),
          lastCheckpointId(),
          runningCheckpointId(),
          runningCheckpointLogSegmentNum(-1),
          lastRun(0),
          finishLog(0),
          flushViewLogSeq()
        { clnt = c; }
    virtual void handle();
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "checkpoint";
    }
    void SetParameters(const Properties& props);
    void ScheduleNow();
private:
    string                lockFileName;
    int                   lockFd;
    int                   intervalSec;
    int                   pid;
    int                   failedCount;
    int                   maxFailedCount;
    int                   checkpointWriteTimeoutSec;
    int                   flushNewViewDelaySec;
    bool                  checkpointWriteSyncFlag;
    size_t                checkpointWriteBufferSize;
    MetaVrLogSeq          lastCheckpointId;
    MetaVrLogSeq          runningCheckpointId;
    seq_t                 runningCheckpointLogSegmentNum;
    time_t                lastRun;
    MetaLogWriterControl* finishLog;
    MetaVrLogSeq          flushViewLogSeq;
};

/*!
 * \brief Op to initiate connection close by the meta server. To use with netcat
 * and such.
 */
struct MetaDisconnect : public MetaRequest {
    MetaDisconnect()
        : MetaRequest(META_DISCONNECT, kLogNever)
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
    int                    sendAuthType;
    const char*            sendContentPtr;
    int                    sendContentLen;
    bool                   doneFlag;
    bool                   useSslFlag;
    int64_t                credExpirationTime;
    int64_t                sessionExpirationTime;
    string                 authName;
    NetConnection::Filter* filter;

    MetaAuthenticate()
        : MetaRequest(META_AUTHENTICATE, kLogNever),
          authType(kAuthenticationTypeUndef),
          contentLength(0),
          contentBuf(0),
          contentBufPos(0),
          sendAuthType(kAuthenticationTypeUndef),
          sendContentPtr(0),
          sendContentLen(0),
          doneFlag(false),
          useSslFlag(false),
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
    void Request(ReqOstream& os) const;
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
        : MetaRequest(META_DELEGATE, kLogNever),
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
    DelegationToken           token;
    StringBufT<64>            tokenStr;
    StringBufT<64>            tokenKeyStr;
    int64_t                   tExp;
    int64_t                   tIssued;
    kfsUid_t                  tUid;
    DelegationToken::TokenSeq tSeq;
    uint16_t                  tFlags;

    MetaDelegateCancel()
        : MetaRequest(META_DELEGATE_CANCEL, kLogIfOk),
          token(),
          tokenStr(),
          tokenKeyStr(),
          tExp(0),
          tIssued(0),
          tUid(kKfsUserNone),
          tSeq(-1),
          tFlags(0)
          {}
    virtual bool dispatch(ClientSM& sm);
    virtual bool start()
    {
        if (0 == status) {
            tExp    = token.GetIssuedTime() + token.GetValidForSec();
            tIssued = token.GetIssuedTime();
            tUid    = token.GetUid();
            tSeq    = token.GetSeq();
            tFlags  = token.GetFlags();
        }
        return (0 == status);
    }
    virtual void handle();
    virtual ostream& ShowSelf(ostream& os) const
        { return (os << "delegate-cancel: " << token.Show()); }
    virtual void response(ReqOstream& os);
    bool Validate();
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Token", "T", &MetaDelegateCancel::tokenStr)
        .Def2("Key",   "K", &MetaDelegateCancel::tokenKeyStr)
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        // No MetaRequest::LogIoDef(parser) -- log seq below sufficient.
        return parser
        .Def("z", &MetaDelegateCancel::logseq)
        .Def("E", &MetaDelegateCancel::tExp,   int64_t(0))
        .Def("I", &MetaDelegateCancel::tIssued,int64_t(0))
        .Def("U", &MetaDelegateCancel::tUid,   kKfsUserNone)
        .Def("S", &MetaDelegateCancel::tSeq,   DelegationToken::TokenSeq(-1))
        .Def("F", &MetaDelegateCancel::tFlags, uint16_t(0))
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
    ServerLocation location;
    ChunkServerPtr server;      //!< The chunkserver that sent us this message
    MetaChunkCorrupt(seq_t s = -1, fid_t f = -1, chunkId_t c = -1)
        : MetaRequest(META_CHUNK_CORRUPT, kLogIfOk, s),
          fid(f),
          chunkId(c),
          chunkIdsStr(),
          isChunkLost(false),
          noReplyFlag(false),
          dirOkFlag(false),
          chunkCount(0),
          chunkDir(),
          location(),
          server()
        {}
    virtual bool start();
    virtual void handle();
    virtual void response(ReqOstream &os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "chunk-currupt:"
            " fid: "   << fid <<
            " chunk: " << chunkId <<
            " count: " << chunkCount <<
            " lost: "  << isChunkLost <<
            " dir: "   << chunkDir <<
            " ok: "    << dirOkFlag <<
            " ids: "   << chunkIdsStr
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
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("S", &MetaChunkCorrupt::location)
        .Def("P", &MetaChunkCorrupt::fid,             fid_t(-1))
        .Def("H", &MetaChunkCorrupt::chunkId,     chunkId_t(-1))
        .Def("L", &MetaChunkCorrupt::isChunkLost,         false)
        .Def("C", &MetaChunkCorrupt::chunkCount,              0)
        .Def("I", &MetaChunkCorrupt::chunkIdsStr)
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
        : MetaRequest(META_CHUNK_EVACUATE, kLogQueue, s),
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
        return os <<
            "evacuate: "
            " space:"
            " total: "      << totalSpace <<
            " fs: "         << totalFsSpace <<
            " used: "       << usedSpace <<
            " drives: "     << numDrives <<
            " in flight: "  << numEvacuateInFlight <<
            " ids: "        << chunkIds;
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
    StringBufT<(16 + 3) * 256> chunkIdAndVers; //!< input
    int                        numChunks;
    bool                       helloFlag;
    bool                       endOfNotifyFlag;
    int                        useThreshold;
    ServerLocation             location;
    ChunkServerPtr             server;
    MetaChunkStaleNotify*      staleNotify;
    MetaChunkAvailable(seq_t s = -1)
        : MetaRequest(META_CHUNK_AVAILABLE, kLogIfOk, s),
          chunkIdAndVers(),
          numChunks(-1),
          helloFlag(false),
          endOfNotifyFlag(false),
          useThreshold(-1),
          location(),
          server(),
          staleNotify(0)
        {}
    virtual bool start();
    virtual void handle();
    virtual void response(ReqOstream& os);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "chunk-available:"
            " hello: " << helloFlag <<
            " count: " << numChunks <<
            " ids: "   << chunkIdAndVers
        ;
    }
    virtual void setChunkServer(const ChunkServerPtr& cs) { server = cs; }
    void responseSelf(ReqOstream& os);
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def2("Chunk-ids-vers", "I", &MetaChunkAvailable::chunkIdAndVers        )
        .Def2("Num-chunks",     "N", &MetaChunkAvailable::numChunks,          -1)
        .Def2("Hello",          "H", &MetaChunkAvailable::helloFlag,       false)
        .Def2("End-notify",     "E", &MetaChunkAvailable::endOfNotifyFlag, false)
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("S", &MetaChunkAvailable::location)
        .Def("C", &MetaChunkAvailable::chunkIdAndVers)
        .Def("N", &MetaChunkAvailable::numChunks,          -1)
        .Def("T", &MetaChunkAvailable::useThreshold,       -1)
        .Def("E", &MetaChunkAvailable::endOfNotifyFlag, false)
        ;
    }
protected:
    virtual void ReleaseSelf();
};

struct MetaChunkDirInfo : public MetaRequest {
    typedef StringBufT<256> DirName;

    ChunkServerPtr server;
    bool           noReplyFlag;
    DirName        dirName;
    StringBufT<32> kfsVersion;
    Properties     props;

    MetaChunkDirInfo(seq_t s = -1)
        : MetaRequest(META_CHUNKDIR_INFO, kLogNever, s),
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
        return os << "chunk-dir-info: " << dirName;
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
        .Def2("No-reply", "N", &MetaChunkDirInfo::noReplyFlag, false)
        .Def2("Dir-name", "D", &MetaChunkDirInfo::dirName)
        .Def2("Version",  "V", &MetaChunkDirInfo::kfsVersion)
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
    StringBufT<64>     chunkServerName;
    chunkId_t          chunkId;
    chunkOff_t         chunkPos;
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
        : MetaRequest(META_LEASE_ACQUIRE, kLogNever),
          pathname(),
          chunkServerName(),
          chunkId(-1),
          chunkPos(-1),
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
            " lease-acquire:"
            " chunkId: " << chunkId <<
            " flush: "   << flushFlag <<
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
        .Def2("Chunk-pos",           "O", &MetaLeaseAcquire::chunkPos,           chunkId_t(-1))
        .Def2("Flush-write-lease",   "F", &MetaLeaseAcquire::flushFlag,    false              )
        .Def2("Lease-timeout",       "T", &MetaLeaseAcquire::leaseTimeout, LEASE_INTERVAL_SECS)
        .Def2("Chunk-ids",           "I", &MetaLeaseAcquire::chunkIds)
        .Def2("Get-locations",       "L", &MetaLeaseAcquire::getChunkLocationsFlag,      false)
        .Def2("Append-recovery",     "A", &MetaLeaseAcquire::appendRecoveryFlag,         false)
        .Def2("Append-recovery-loc", "R", &MetaLeaseAcquire::appendRecoveryLocations          )
        .Def2("Chunk-server",        "C", &MetaLeaseAcquire::chunkServerName                  )
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
    StringBufT<64>     chunkServerName;
    chunkId_t          chunkId;
    chunkOff_t         chunkPos;
    int64_t            leaseId;
    bool               emitCSAccessFlag;
    bool               clientCSAllowClearTextFlag;
    time_t             issuedTime;
    ChunkAccess        chunkAccess;
    const ChunkServer* chunkServer;
    int                validForTime;
    TokenSeq           tokenSeq;
    MetaLeaseRenew()
        : MetaRequest(META_LEASE_RENEW, kLogNever),
          leaseType(READ_LEASE),
          pathname(),
          chunkServerName(),
          chunkId(-1),
          chunkPos(-1),
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
        return os <<
            "lease-renew: "
            " type: "    << (leaseType == READ_LEASE ? "read" : "write") <<
            " chunkId: " << chunkId <<
            " "          << pathname
        ;
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
        .Def2("Chunk-pos",    "O", &MetaLeaseRenew::chunkPos, chunkId_t(-1))
        .Def2("CS-access",    "A", &MetaLeaseRenew::emitCSAccessFlag)
        .Def2("Chunk-server", "C", &MetaLeaseRenew::chunkServerName)
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
        : MetaRequest(META_LEASE_CLEANUP, kLogNever, s)
            { clnt = c; }

    virtual void handle();
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "lease-cleanup";
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
        : MetaRequest(META_CHUNK_REPLICATION_CHECK, kLogNever, s)
            { clnt = c; }

    virtual void handle();
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "chunk-replication-check";
    }
};

struct MetaForceChunkReplication : public ServerLocation, public MetaRequest {
    chunkId_t chunkId;
    bool      recoveryFlag;
    bool      removeFlag;
    bool      forcePastEofRecoveryFlag;

    MetaForceChunkReplication()
        : ServerLocation(),
          MetaRequest(META_FORCE_CHUNK_REPLICATION, kLogNever),
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
        return os << "force-replication:"
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
        : MetaRequest(META_ACK, kLogIfOk),
          ack()
        {}
    bool Validate()
        { return (! ack.empty()); }
    virtual bool start();
    virtual void handle();
    virtual void response(ReqOstream& /* os */)
        { /* No response; */ }
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

struct MetaRemoveFromDumpster : public MetaRequest {
    string  name;
    fid_t   fid;
    int64_t mtime;

    MetaRemoveFromDumpster(
        const string& nm = string(),
        fid_t         id = -1)
        : MetaRequest(META_REMOVE_FROM_DUMPSTER, kLogIfOk),
          name(nm),
          fid(id),
          mtime()
        {}
    bool Validate()
        { return (0 <= fid && ! name.empty()); }
    virtual bool start();
    virtual void handle();
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os <<
            "remove-from-dumpster: " << name <<
            " fid: "                 << fid;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("N", &MetaRemoveFromDumpster::name)
        .Def("P", &MetaRemoveFromDumpster::fid, fid_t(-1))
        .Def("T", &MetaRemoveFromDumpster::mtime)
        ;
    }
};

struct MetaLogWriterControl : public MetaRequest {
    enum Type
    {
        kNop,
        kNewLog,
        kCheckpointNewLog,
        kSetParameters,
        kWriteBlock,
        kLogFetchDone
    };
    typedef StBufferT<int, 64> Lines;

    Type               type;
    MetaVrLogSeq       committed;
    MetaVrLogSeq       lastLogSeq;
    vrNodeId_t         primaryNodeId;
    seq_t              logSegmentNum;
    Properties         params;
    string             paramsPrefix;
    string             logName;
    MetaRequest* const completion;
    vrNodeId_t         transmitterId;
    uint32_t           blockChecksum;
    seq_t              blockSeq;
    MetaVrLogSeq       blockStartSeq;
    MetaVrLogSeq       blockEndSeq;
    MetaVrLogSeq       blockCommitted;
    Lines              blockLines;
    IOBuffer           blockData;

    MetaLogWriterControl(
        Type         t = kNop,
        MetaRequest* c = 0)
        : MetaRequest(META_LOG_WRITER_CONTROL, kLogAlways),
          type(t),
          committed(),
          lastLogSeq(),
          primaryNodeId(-1),
          logSegmentNum(-1),
          params(),
          paramsPrefix(),
          logName(),
          completion(c),
          transmitterId(-1),
          blockChecksum(0),
          blockSeq(-1),
          blockStartSeq(),
          blockEndSeq(),
          blockCommitted(),
          blockLines(),
          blockData()
        { replayBypassFlag = kCheckpointNewLog != type; }
    virtual bool start()  { return true; }
    virtual void handle();
    virtual ostream& ShowSelf(ostream& os) const
    {
        if (kWriteBlock == type) {
            os <<
                "log-write"
                " block: ["          << blockStartSeq <<
                ":"                  << blockEndSeq <<
                "] length: "         << blockData.BytesConsumable() <<
                " lines: "           << blockLines.GetSize() <<
                " block committed: " << blockCommitted <<
                " block seq: "       << blockSeq
            ;
        } else {
            os << "log-control: ";
            switch (type) {
                case kNop:              os << "nop";            break;
                case kNewLog:           os << "new-log";        break;
                case kCheckpointNewLog: os << "cpt-new-log";    break;
                case kSetParameters:    os << "set-parameters"; break;
                case kLogFetchDone:     os << "log-fetch-done"; break;
                default:                os << "invalid";        break;
            }
        }
        os <<
            " last-log: " << lastLogSeq <<
            " status: "   << status <<
            (statusMsg.empty() ? "" : " ") << statusMsg;
        return os;
    }
    void Reset(Type t)
    {
        ResetSelf();
        logAction        = kLogAlways;
        replayBypassFlag = kCheckpointNewLog != t;
        type             = t;
        committed        = MetaVrLogSeq();
        lastLogSeq       = MetaVrLogSeq();
        primaryNodeId    = -1;
        logSegmentNum    = -1;
        params.clear();
        paramsPrefix     = string();
        logName          = string();
        transmitterId    = -1;
        blockChecksum    = 0;
        blockSeq         = -1;
        blockStartSeq    = MetaVrLogSeq();
        blockEndSeq      = MetaVrLogSeq();
        blockCommitted   = MetaVrLogSeq();
        blockLines.Clear();
        blockData.Clear();
    }
};

struct MetaSetFsInfo : public MetaRequest {
    MetaSetFsInfo(
        int64_t fsid   = -1,
        int64_t crTime = -1)
        : MetaRequest(META_SET_FILE_SYSTEM_INFO, kLogIfOk),
          fileSystemId(fsid),
          createTime(crTime)
        {}
    virtual bool start();
    virtual void handle();
    bool Validate()
        { return (0 <= fileSystemId); }
    virtual ostream& ShowSelf(ostream& os) const
    {
        return (os << "setfsinfo "
            "fsid: "  << fileSystemId <<
            " time: " << createTime
        );
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("I", &MetaSetFsInfo::fileSystemId, int64_t(-1))
        .Def("C", &MetaSetFsInfo::createTime,   int64_t(-1))
        ;
    }
private:
    int64_t fileSystemId;
    int64_t createTime;
};

struct MetaSetGroupUsers : public MetaRequest {
    const bool hexFlag;
    MetaSetGroupUsers(bool f)
        : MetaRequest(META_SET_GROUP_USERS, kLogNever),
          hexFlag(f),
          buf()
        {}
    virtual bool start()
    {
        if (0 == status && buf.empty()) {
            status = -EINVAL;
        }
        return (0 == status);
    }
    virtual void handle();
    void AddEntry(bool appendFlag, const char* ptr, size_t len)
    {
        if (len <= 0) {
            return;
        }
        const int64_t alen = appendFlag ? -(int64_t)len :  (int64_t)len;
        buf.append(reinterpret_cast<const char*>(&alen), sizeof(alen));
        buf.append(ptr, len);
    }
    bool Next(const char*& ptr, size_t& len, bool& appendFlag) const
    {
        int64_t alen;
        if (ptr) {
            ptr += len;
            if (buf.data() + buf.size() < ptr + sizeof(alen)) {
                len = 0;
                return false;
            }
        } else {
            ptr = buf.data();
        }
        char* dst = reinterpret_cast<char*>(&alen);
        for (const char* end = ptr + sizeof(alen); ptr < end; ptr++) {
            *dst++ = *ptr;
        }
        if (alen < 0) {
            appendFlag = true;
            len = (size_t)-alen;
        } else {
            len = (size_t)alen;
        }
        return true;
    }
    virtual ostream& ShowSelf(ostream& os) const
    {
        return (os << "set-group_users");
    }
private:
    string buf;
};

struct MetaLogClearObjStoreDelete : public MetaRequest {
    MetaLogClearObjStoreDelete()
        : MetaRequest(META_LOG_CLEAR_OBJ_STORE_DELETE, kLogIfOk)
        {}
    bool Validate()
        { return true; }
    virtual bool start();
    virtual void handle();
    virtual void response(ReqOstream& /* os */)
        { /* No response; */ }
    virtual ostream& ShowSelf(ostream& os) const
        { return (os << "clear-object-store-delete"); }
    template<typename T> static T& ParserDef(T& parser)
    {
        return  MetaRequest::ParserDef(parser)
        ;
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        ;
    }
};

struct MetaReadMetaData : public MetaRequest {
    int64_t      fileSystemId;
    MetaVrLogSeq startLogSeq;
    MetaVrLogSeq endLogSeq;
    int64_t      readPos;
    bool         checkpointFlag;
    bool         allowNotPrimaryFlag;
    int          readSize;
    int          maxReadSize;
    uint32_t     checksum;
    int64_t      fileSize;
    bool         handledFlag;
    string       filename;
    string       clusterKey;
    string       metaMd;
    IOBuffer     data;

    MetaReadMetaData()
        : MetaRequest(META_READ_META_DATA, kLogNever),
          fileSystemId(-1),
          startLogSeq(),
          endLogSeq(),
          readPos(-1),
          checkpointFlag(false),
          allowNotPrimaryFlag(false),
          readSize(-1),
          maxReadSize(-1),
          checksum(0),
          fileSize(-1),
          handledFlag(false),
          filename(),
          clusterKey(),
          metaMd(),
          data()
        { replayBypassFlag = true; }
    bool Validate()
        { return true; }
    virtual void handle();
    virtual void response(ReqOstream& os, IOBuffer& buf);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return (os <<
            "read-" << (checkpointFlag ? "checkpoint" : "log") <<
            " start seq: " << startLogSeq <<
            " pos: "       << readPos <<
            " size: "      << readSize
        );
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return  MetaRequest::ParserDef(parser)
        .Def2("FsId",       "FI", &MetaReadMetaData::fileSystemId,  int64_t(-1))
        .Def2("Start-log",   "L", &MetaReadMetaData::startLogSeq               )
        .Def2("Checkpoint",  "C", &MetaReadMetaData::checkpointFlag,      false)
        .Def2("Read-size",   "S", &MetaReadMetaData::readSize,               -1)
        .Def2("Read-pos",    "O", &MetaReadMetaData::readPos,       int64_t(-1))
        .Def2("Not-prm-ok", "NP", &MetaReadMetaData::allowNotPrimaryFlag, false)
        ;
    }
};

struct MetaHibernatedPrune : public MetaRequest {
    ServerLocation location;
    size_t         listSize;
    MetaHibernatedPrune(
        const ServerLocation& loc = ServerLocation(),
        size_t                ls  = 0)
        : MetaRequest(META_HIBERNATED_PRUNE, kLogIfOk),
          location(loc),
          listSize(ls)
        {}
    bool Validate()
        { return location.IsValid(); }
    virtual bool start()
    {
        if (! Validate()) {
            status = -EINVAL;
        }
        return (0 == status);
    }
    virtual void handle();
    virtual ostream& ShowSelf(ostream& os) const
    {
        return (os <<
            "prune-hibernated-server: " << location <<
            " size: " << listSize
        );
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("S", &MetaHibernatedPrune::location)
        ;
    }
};

struct MetaHibernatedRemove : public MetaRequest {
    ServerLocation location;
    MetaHibernatedRemove(
        const ServerLocation& loc = ServerLocation())
        : MetaRequest(META_HIBERNATED_REMOVE, kLogIfOk),
          location(loc)
        {}
    bool Validate()
        { return location.IsValid(); }
    virtual bool start()
    {
        if (! Validate()) {
            status = -EINVAL;
        }
        return (0 == status);
    }
    virtual void handle();
    virtual ostream& ShowSelf(ostream& os) const
    {
        return (os <<
            "remove-hibernated-server: " << location
        );
    }
    template<typename T> static T& LogIoDef(T& parser)
    {
        return MetaRequest::LogIoDef(parser)
        .Def("S", &MetaHibernatedRemove::location)
        ;
    }
};

struct MetaProcessRestart : public MetaRequest {
    typedef void (*RestartPtr)(void);

    MetaProcessRestart()
        : MetaRequest(META_RESTART_PROCESS, kLogNever)
        { replayBypassFlag = true; }
    virtual void handle()
    {
        if (sRestartPtr) {
            (*sRestartPtr)();
        }
    }
    virtual ostream& ShowSelf(ostream& os) const
        { return (os << "restart-meta-server"); }
    static void SetRestartPtr(RestartPtr restartPtr)
        { sRestartPtr = restartPtr; }
private:
    static RestartPtr sRestartPtr;
};

const char* const kMetaClusterKeyParamNamePtr    = "metaServer.clusterKey";
const char* const kMetaserverMetaMdsParamNamePtr = "metaServer.metaMds";
const char* const kLogWriteAheadPrefixPtr        = "a/";
const char* const kLogVrStatViewNamePtr          = "VRSV";
const char        kMetaIoPropertiesSeparator     = '=';

enum LogBlockAckFlags
{
    kLogBlockAckReAuthFlagBit  = 0,
    kLogBlockAckHasServerIdBit = 1
};

int ParseCommand(const IOBuffer& buf, int len, MetaRequest **res,
    char* threadParseBuffer, bool shortRpcFmtFlag);
int ParseFirstCommand(const IOBuffer& ioBuf, int len, MetaRequest **res,
    char* threadParseBuffer, bool& shortRpcFmtFlag);
int ParseLogRecvCommand(const IOBuffer& ioBuf, int len, MetaRequest **res,
    char* threadParseBuffer);

void setWORMMode(bool value);
bool getWORMMode();
void setChunkmapDumpDir(string dir);
void CheckIfIoBuffersAvailable();
void CancelRequestsWaitingForBuffers();
void SetRequestParameters(const Properties& props);

/* update counters for # of files/dirs/chunks in the system */
void UpdateNumDirs(int count);
void UpdateNumFiles(int count);
void UpdateNumChunks(int count);
void UpdatePathToFidCacheMiss(int count);
void UpdatePathToFidCacheHit(int count);
int64_t GetNumFiles();
int64_t GetNumDirs();
bool ValidateMetaReplayIoHandler(ostream& inErrStream);

}
#endif /* !defined(KFS_REQUEST_H) */
