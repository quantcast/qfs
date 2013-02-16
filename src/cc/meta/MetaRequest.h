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
#include "common/Properties.h"
#include "common/StBuffer.h"
#include "common/StdAllocator.h"
#include "common/DynamicArray.h"
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
    f(GET_CHUNK_SERVER_DIRS_COUNTERS)

enum MetaOp {
#define KfsMakeMetaOpEnumEntry(name) META_##name,
    KfsForEachMetaOpId(KfsMakeMetaOpEnumEntry)
#undef KfsMakeMetaOpEnumEntry
    META_NUM_OPS_COUNT // must be the last one
};


class ChunkServer;
typedef boost::shared_ptr<ChunkServer> ChunkServerPtr;
typedef DynamicArray<chunkId_t, 8> ChunkIdQueue;

/*!
 * \brief Meta request base class
 */
struct MetaRequest {
    typedef vector<
        ChunkServerPtr,
        StdAllocator<ChunkServerPtr>
    > Servers;

    const MetaOp    op;              //!< type of request
    int             status;          //!< returned status
    int             clientProtoVers; //!< protocol version # sent by client
    int             submitCount;     //!< for time tracking.
    int64_t         submitTime;      //!< to time requests, optional.
    int64_t         processTime;     //!< same as previous
    string          statusMsg;       //!< optional human readable status message
    seq_t           opSeqno;         //!< command sequence # sent by the client
    seq_t           seqno;           //!< sequence no. in log
    const bool      mutation;        //!< mutates metatree
    bool            suspended;       //!< is this request suspended somewhere
    bool            fromChunkServerFlag;
    string          clientIp;
    IOBuffer        reqHeaders;
    kfsUid_t        euser;
    kfsGid_t        egroup;
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
          mutation(mu),
          suspended(false),
          fromChunkServerFlag(false),
          clientIp(),
                  reqHeaders(),
          euser(kKfsUserNone),
          egroup(kKfsGroupNone),
          next(0),
          clnt(0)
        { MetaRequest::Init(); }
    virtual ~MetaRequest();
    virtual void handle();
    //!< when an op finishes execution, we send a response back to
    //!< the client.  This function should generate the appropriate
    //!< response to be sent back as per the KFS protocol.
    virtual void response(ostream& /* os */) {}
    virtual void response(ostream& os, IOBuffer& /* buf */) { response(os); }
    virtual int log(ostream &file) const = 0; //!< write request to log
    virtual string Show() const { return string(); }
    virtual void setChunkServer(const ChunkServerPtr& /* cs */) {};
    bool ValidateRequestHeader(
        const char* name,
        size_t      nameLen,
        const char* header,
        size_t      headerLen,
        bool        hasChecksum,
        uint32_t    checksum)
    {
        return (
            hasChecksum ?
            (! sVerifyHeaderChecksumFlag ||
            Checksum(name, nameLen, header, headerLen) == checksum) :
            (! sRequireHeaderChecksumFlag || ! mutation)
        );
    }
    bool HandleUnknownField(
        const char* /* key */, size_t /* keyLen */,
        const char* /* val */, size_t /* valLen */)
        { return true; }
    template<typename T> static T& ParserDef(T& parser)
    {
        return parser
        .Def("Cseq",                    &MetaRequest::opSeqno,         seq_t(-1))
        .Def("Client-Protocol-Version", &MetaRequest::clientProtoVers,    int(0))
        .Def("From-chunk-server",       &MetaRequest::fromChunkServerFlag, false)
        .Def("UserId",                  &MetaRequest::euser,  kKfsUserNone)
        .Def("GroupId",                 &MetaRequest::egroup, kKfsGroupNone)
        ;
    }
    static void SetParameters(const Properties& props);
    static uint32_t Checksum(
        const char* name,
        size_t      nameLen,
        const char* header,
        size_t      headerLen);
    static int GetRequestCount()
        { return sMetaRequestCount; }
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
};

void submit_request(MetaRequest *r);

/*!
 * \brief look up a file name
 */
struct MetaLookup: public MetaRequest {
    fid_t  dir;    //!< parent directory fid
    string name;   //!< name to look up
    MFattr fattr;
    MetaLookup()
        : MetaRequest(META_LOOKUP, false),
          dir(-1),
          name(),
          fattr()
        {}
    virtual void handle();
    virtual int log(ostream& file) const;
    virtual void response(ostream& os);
    virtual string Show() const
    {
        ostringstream os;

        os << "lookup: name = " << name;
        os << " (parent fid = " << dir << ")";
        return os.str();
    }
    bool Validate()
    {
        return (dir >= 0 && ! name.empty());
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Parent File-handle", &MetaLookup::dir, fid_t(-1))
        .Def("Filename",           &MetaLookup::name          )
        ;
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
    virtual int log(ostream& file) const;
    virtual void response(ostream& os);
    virtual string Show() const
    {
        ostringstream os;

        os << "lookup_path: path = " << path;
        os << " (root fid = " << root << ")";
        return os.str();
    }
    bool Validate()
    {
        return (root >= 0 && ! path.empty());
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Root File-handle", &MetaLookupPath::root, fid_t(-1))
        .Def("Pathname",         &MetaLookupPath::path           )
        ;
    }
};

/*!
 * \brief create a file
 */
struct MetaCreate: public MetaRequest {
    fid_t     dir;               //!< parent directory fid
    fid_t     fid;               //!< file ID of new file
    int16_t   numReplicas;         //!< desired degree of replication
    int32_t   striperType;
    int32_t   numStripes;
    int32_t   numRecoveryStripes;
    int32_t   stripeSize;
    bool      exclusive;           //!< model the O_EXCL flag
    fid_t     todumpster;          //!< moved existing to dumpster
    kfsUid_t  user;
    kfsGid_t  group;
    kfsMode_t mode;
    seq_t     reqId;
    string    name;              //!< name to create
    MetaCreate()
        : MetaRequest(META_CREATE, true),
          dir(-1),
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
          reqId(-1),
          name()
        {}
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const
    {
        ostringstream os;
        os << "create:"
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
        return os.str();
    }
    bool Validate();
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Parent File-handle",   &MetaCreate::dir,                fid_t(-1))
        .Def("Num-replicas",         &MetaCreate::numReplicas,        int16_t( 1))
        .Def("Striper-type",         &MetaCreate::striperType,        int32_t(KFS_STRIPED_FILE_TYPE_NONE))
        .Def("Num-stripes",          &MetaCreate::numStripes,         int32_t(0))
        .Def("Num-recovery-stripes", &MetaCreate::numRecoveryStripes, int32_t(0))
        .Def("Stripe-size",          &MetaCreate::stripeSize,         int32_t(0))
        .Def("Exclusive",            &MetaCreate::exclusive,          false)
        .Def("Filename",             &MetaCreate::name                     )
        .Def("Owner",                &MetaCreate::user,               kKfsUserNone)
        .Def("Group",                &MetaCreate::group,              kKfsGroupNone)
        .Def("Mode",                 &MetaCreate::mode,               kKfsModeUndef)
        .Def("ReqId",                &MetaCreate::reqId,              seq_t(-1))
        ;
    }
};

/*!
 * \brief create a directory
 */
struct MetaMkdir: public MetaRequest {
    fid_t     dir;  //!< parent directory fid
    fid_t     fid;  //!< file ID of new directory
    kfsUid_t  user;
    kfsGid_t  group;
    kfsMode_t mode;
    seq_t     reqId;
    string    name; //!< name to create
    MetaMkdir()
        : MetaRequest(META_MKDIR, true),
          dir(-1),
          fid(-1),
          user(kKfsUserNone),
          group(kKfsGroupNone),
          mode(kKfsModeUndef),
          reqId(-1),
          name()
        {}
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const
    {
        ostringstream os;

        os << "mkdir:"
            " name: "   << name <<
            " parent: " << dir  <<
            " user: "   << user <<
            " group: "  << group <<
            " mode: "   << oct << mode << dec <<
            " euser: "  << euser <<
            " egroup: " << egroup
        ;
        return os.str();
    }
    bool Validate()
    {
        return (dir >= 0 && ! name.empty());
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Parent File-handle", &MetaMkdir::dir, fid_t(-1))
        .Def("Directory",          &MetaMkdir::name          )
        .Def("Owner",              &MetaMkdir::user,   kKfsUserNone)
        .Def("Group",              &MetaMkdir::group,  kKfsGroupNone)
        .Def("Mode",               &MetaMkdir::mode,   kKfsModeUndef)
        .Def("ReqId",              &MetaMkdir::reqId,  seq_t(-1))
        ;
    }
};

/*!
 * \brief remove a file
 */
struct MetaRemove: public MetaRequest {
    fid_t    dir;      //!< parent directory fid
    string   name;     //!< name to remove
    string   pathname; //!< full pathname to remove
    fid_t    todumpster;
    MetaRemove()
        : MetaRequest(META_REMOVE, true),
          dir(-1),
          name(),
          pathname(),
          todumpster(-1)
        {}
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const
    {
        ostringstream os;

        os << "remove:"
            " path: "       << pathname <<
            " name: "       << name <<
            " dir: "        << dir <<
            " todumpster: " << todumpster
        ;
        return os.str();
    }
    bool Validate()
    {
        return (dir >= 0 && ! name.empty());
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Parent File-handle", &MetaRemove::dir, fid_t(-1))
        .Def("Filename",           &MetaRemove::name          )
        .Def("Pathname",           &MetaRemove::pathname      )
        ;
    }
};

/*!
 * \brief remove a directory
 */
struct MetaRmdir: public MetaRequest {
    fid_t  dir; //!< parent directory fid
    string name;    //!< name to remove
    string pathname; //!< full pathname to remove
    MetaRmdir()
        : MetaRequest(META_RMDIR, true),
          dir(-1),
          name(),
          pathname()
        {}
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const
    {
        ostringstream os;

        os << "rmdir: path = " << pathname << " (name = " << name << ")";
        os << " (parent fid = " << dir << ")";
        return os.str();
    }
    bool Validate()
    {
        return (dir >= 0 && ! name.empty());
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Parent File-handle", &MetaRmdir::dir, fid_t(-1))
        .Def("Directory",          &MetaRmdir::name          )
        .Def("Pathname",           &MetaRmdir::pathname      )
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
    virtual int log(ostream& file) const;
    virtual void response(ostream& os, IOBuffer& buf);
    virtual string Show() const
    {
        ostringstream os;
        os << "readdir: dir fid = " << dir;
        return os.str();
    }
    bool Validate()
    {
        return (dir >= 0);
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Directory File-handle", &MetaReaddir::dir,       fid_t(-1))
        .Def("Max-entries",           &MetaReaddir::numEntries,        0)
        .Def("Fname-start",           &MetaReaddir::fnameStart)
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
          hasMoreEntriesFlag(false),
          noAttrsFlag(false),
          ioBufPending(0),
          fnameStart(),
          dentries(),
          lastChunkInfos()
        {}
    ~MetaReaddirPlus();
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ostream& os, IOBuffer& buf);
    virtual string Show() const
    {
        ostringstream os;

        os << "readdir plus: dir fid = " << dir;
        return os.str();
    }
    bool Validate()
    {
        return (dir >= 0);
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Directory File-handle", &MetaReaddirPlus::dir, fid_t(-1))
        .Def("GetLastChunkInfoOnlyIfSizeUnknown",
            &MetaReaddirPlus::getLastChunkInfoOnlyIfSizeUnknown, false)
        .Def("Max-entries",           &MetaReaddirPlus::numEntries,  0)
        .Def("Fname-start",           &MetaReaddirPlus::fnameStart)
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
    MetaGetalloc()
        : MetaRequest(META_GETALLOC, false),
          fid(-1),
          offset(-1),
          chunkId(-1),
          chunkVersion(-1),
          locations(),
          pathname(),
          replicasOrderedFlag(false)
    {}
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const
    {
        ostringstream os;

        os << "getalloc: " << pathname << " (fid = " << fid << ")";
        os << " offset = " << offset;
        return os.str();
    }
    bool Validate()
    {
        return (fid >= 0 && offset >= 0);
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("File-handle",  &MetaGetalloc::fid,          fid_t(-1))
        .Def("Chunk-offset", &MetaGetalloc::offset,  chunkOff_t(-1))
        .Def("Pathname",     &MetaGetalloc::pathname               )
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
    int        maxResCnt;
    int        numChunks;
    bool       hasMoreChunksFlag;
    IOBuffer   resp;   //!< result
    MetaGetlayout()
        : MetaRequest(META_GETLAYOUT, false),
          fid(-1),
          startOffset(0),
          omitLocationsFlag(false),
          lastChunkInfoOnlyFlag(false),
          maxResCnt(-1),
          numChunks(-1),
          hasMoreChunksFlag(false),
          resp()
        {}
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ostream &os, IOBuffer& buf);
    virtual string Show() const
    {
        ostringstream os;

        os << "getlayout: fid = " << fid;
        return os.str();
    }
    bool Validate()
    {
        return (fid >= 0);
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("File-handle",     &MetaGetlayout::fid,                   fid_t(-1))
        .Def("Start-offset",    &MetaGetlayout::startOffset,           chunkOff_t(0))
        .Def("Omit-locations",  &MetaGetlayout::omitLocationsFlag,     false)
        .Def("Last-chunk-only", &MetaGetlayout::lastChunkInfoOnlyFlag, false)
        .Def("Max-chunks",      &MetaGetlayout::maxResCnt,             -1)
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
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const
    {
        ostringstream os;
        os <<
        "relinquish " <<
            (leaseType == READ_LEASE ? "read" : "write") <<
        " lease: "     << leaseId <<
        " chunk: "     << chunkId <<
        " chunkSize: " << chunkSize <<
        " checksum: "  << (hasChunkChecksum ?
            int64_t(chunkChecksum) : int64_t(-1))
        ;
        return os.str();
    }
    bool Validate()
    {
        leaseType = (leaseTypeStr == "WRITE_LEASE") ?
            WRITE_LEASE : READ_LEASE;
        hasChunkChecksum = chunkChecksumHdr >= 0;
        chunkChecksum    = hasChunkChecksum ?
            (uint32_t)chunkChecksumHdr : (uint32_t)0;
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Lease-type",     &MetaLeaseRelinquish::leaseTypeStr                 )
        .Def("Chunk-handle",   &MetaLeaseRelinquish::chunkId,        chunkId_t(-1))
        .Def("Lease-id",       &MetaLeaseRelinquish::leaseId,          int64_t(-1))
        .Def("Chunk-size",     &MetaLeaseRelinquish::chunkSize,     chunkOff_t(-1))
        .Def("Chunk-checksum", &MetaLeaseRelinquish::chunkChecksumHdr, int64_t(-1))
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
    //!< For replication, the master that runs the transaction
    //!< for completing the write.
    ChunkServerPtr       master;
    uint32_t             numServerReplies;
    int                  firstFailedServerIdx;
    bool                 logFlag;
    bool                 invalidateAllFlag;
    const Permissions*   permissions;
    MetaAllocate*        next;
    int64_t              leaseId;
    chunkOff_t           chunkBlockStart;
    MetaLeaseRelinquish* pendingLeaseRelinquish;
    string               responseStr; // Cached response
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
          master(),
          numServerReplies(0),
          firstFailedServerIdx(-1),
          logFlag(true),
          invalidateAllFlag(false),
          permissions(0),
          next(0),
          leaseId(-1),
          chunkBlockStart(-1),
          pendingLeaseRelinquish(0),
          responseStr(),
          clientHost(),
          pathname()
    {
        SET_HANDLER(this, &MetaAllocate::logOrLeaseRelinquishDone);
    }
    virtual ~MetaAllocate()
        { delete pendingLeaseRelinquish; }
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const;
    void responseSelf(ostream &os);
    void LayoutDone(int64_t chunkAllocProcessTime);
    int logOrLeaseRelinquishDone(int code, void *data);
    int CheckStatus(bool forceFlag = false) const;
    bool ChunkAllocDone(const MetaChunkAllocate& chunkAlloc);
    bool Validate()
    {
        return (fid >= 0 && (offset >= 0 || appendChunk));
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("File-handle",              &MetaAllocate::fid,                   fid_t(-1))
        .Def("Chunk-append",             &MetaAllocate::appendChunk,               false)
        .Def("Chunk-offset",             &MetaAllocate::offset,           chunkOff_t(-1))
        .Def("Pathname",                 &MetaAllocate::pathname                        )
        .Def("Client-host",              &MetaAllocate::clientHost                      )
        .Def("Space-reserve",            &MetaAllocate::spaceReservationSize, int(1<<20))
        .Def("Max-appenders",            &MetaAllocate::maxAppendersPerChunk,    int(64))
        .Def("Invalidate-all",           &MetaAllocate::invalidateAllFlag,         false)
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
    StringBufT<256> pathname; //!< full pathname for file being truncated
    int64_t         mtime;
    MetaTruncate()
        : MetaRequest(META_TRUNCATE, true),
          fid(-1),
          offset(-1),
          endOffset(-1),
          setEofHintFlag(true),
          pruneBlksFromHead(false),
          pathname(),
          mtime()
        {}
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const
    {
        ostringstream os;
        os <<
            (pruneBlksFromHead ?
                "prune from head:" : "truncate:") <<
            " path: "   << pathname <<
            " fid: "    << fid <<
            " offset: " << offset
        ;
        return os.str();
    }
    bool Validate()
    {
        return (fid >= 0 && offset >= 0);
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("File-handle",     &MetaTruncate::fid,                  fid_t(-1))
        .Def("Offset",          &MetaTruncate::offset,          chunkOff_t(-1))
        .Def("Pathname",        &MetaTruncate::pathname                       )
        .Def("Prune-from-head", &MetaTruncate::pruneBlksFromHead,        false)
        .Def("End-offset",      &MetaTruncate::endOffset,       chunkOff_t(-1))
        .Def("Set-eof",         &MetaTruncate::setEofHintFlag,            true)
        ;
    }
};

/*!
 * \brief rename a file or directory
 */
struct MetaRename: public MetaRequest {
    fid_t  dir;        //!< parent directory
    string oldname;    //!< old file name
    string newname;    //!< new file name
    string oldpath;    //!< fully-qualified old pathname
    bool   overwrite;  //!< overwrite newname if it exists
    fid_t  todumpster; //!< moved original to dumpster
    MetaRename()
        : MetaRequest(META_RENAME, true),
          dir(-1),
          oldname(),
          newname(),
          oldpath(),
          overwrite(false),
          todumpster(-1)
        {}
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const
    {
        ostringstream os;

        os << "rename:"
        " dir: "        << dir     <<
        " from: "       << oldpath <<
        " to: "         << newname <<
        " todumpster: " << todumpster
        ;
        return os.str();
    }
    bool Validate()
    {
        return (dir >= 0 && ! oldname.empty() && ! newname.empty());
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Parent File-handle", &MetaRename::dir,   fid_t(-1))
        .Def("Old-name",           &MetaRename::oldname         )
        .Def("New-path",           &MetaRename::newname         )
        .Def("Old-path",           &MetaRename::oldpath         )
        .Def("Overwrite",          &MetaRename::overwrite, false)
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
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const
    {
        ostringstream os;
        os << "setmtime:"
            " path: "  << pathname <<
            " mtime: " << ShowTime(mtime)
        ;
        return os.str();
    }
    bool Validate()
    {
        mtime = sec * 1000 * 1000 + usec;
        return (! pathname.empty());
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Mtime-sec",  &MetaSetMtime::sec     )
        .Def("Mtime-usec", &MetaSetMtime::usec    )
        .Def("Pathname",   &MetaSetMtime::pathname)
        ;
    }
private:
    int64_t sec;
    int64_t usec;
};

/*!
 * \brief change a file's replication factor
 */
struct MetaChangeFileReplication: public MetaRequest {
    fid_t   fid;         //!< fid whose replication has to be changed
    int16_t numReplicas; //!< desired degree of replication
    MetaChangeFileReplication()
        : MetaRequest(META_CHANGE_FILE_REPLICATION, true),
          fid(-1),
          numReplicas(1)
        {}
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const
    {
        ostringstream os;

        os << "change-file-replication: fid = " << fid;
        os << " new # of replicas: " << numReplicas << ' ';
        return os.str();
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("File-handle",  &MetaChangeFileReplication::fid,         fid_t(-1))
        .Def("Num-replicas", &MetaChangeFileReplication::numReplicas, int16_t(1))
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
        : MetaRequest(META_COALESCE_BLOCKS, true),
          srcPath(),
          dstPath(),
          srcFid(-1),
          dstFid(-1),
          dstStartOffset(-1),
          numChunksMoved(0),
          mtime(0)
        {}
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const
    {
        ostringstream os;

        os << "coalesce blocks: src = " << srcPath;
        os << " dst = " << dstPath;
        return os.str();
    }
    bool Validate()
    {
        return (! srcPath.empty() && ! dstPath.empty());
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Src-path",  &MetaCoalesceBlocks::srcPath)
        .Def("Dest-path", &MetaCoalesceBlocks::dstPath)
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
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    string Show() const
    {
        return ((nSecsDown > 0 ?
            "hibernating server: " : "retiring server: "
        ) + location.ToString());
    }
    bool Validate()
    {
        return (location.IsValid());
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Chunk-server-name", &ServerLocation::hostname            )
        .Def("Chunk-server-port", &ServerLocation::port,             -1)
        .Def("Downtime",          &MetaRetireChunkserver::nSecsDown, -1)
        ;
    }
};

/*!
 * \brief hello RPC from a chunk server on startup
 */
struct MetaHello : public MetaRequest, public ServerLocation {
    struct ChunkInfo {
        fid_t     allocFileId; // file id when chunk was allocated
        chunkId_t chunkId;
        seq_t     chunkVersion;
    };
    typedef vector<ChunkInfo, StdAllocator<ChunkInfo> > ChunkInfos;

    ChunkServerPtr  server;                   //!< The chunkserver that sent the hello message
    ServerLocation& location;                 //<! Location of this server
    string          peerName;
    string          clusterKey;
    string          md5sum;
    int64_t         totalSpace;               //!< How much storage space does the server have (bytes)
    int64_t         totalFsSpace;
    int64_t         usedSpace;                //!< How much storage space is used up (in bytes)
    int64_t         uptime;                   //!< Chunk server uptime.
    int             rackId;                   //!< the rack on which the server is located
    int             numChunks;                //!< # of chunks hosted on this server
    int             numNotStableAppendChunks; //!< # of not stable append chunks hosted on this server
    int             numNotStableChunks;       //!< # of not stable chunks hosted on this server
    int             contentLength;            //!< Length of the message body
    int64_t         numAppendsWithWid;
    int             contentIntBase;
    ChunkInfos      chunks;                   //!< Chunks  hosted on this server
    ChunkInfos      notStableChunks;
    ChunkInfos      notStableAppendChunks;
    int             bytesReceived;
    bool            staleChunksHexFormatFlag;
    MetaHello()
        : MetaRequest(META_HELLO, false),
          ServerLocation(),
          server(),
          location(*this),
          peerName(),
          clusterKey(),
          md5sum(),
          totalSpace(0),
          usedSpace(0),
          uptime(0),
          rackId(-1),
          numChunks(0),
          numNotStableAppendChunks(0),
          numNotStableChunks(0),
          contentLength(0),
          numAppendsWithWid(0),
          contentIntBase(10),
          chunks(),
          notStableChunks(),
          notStableAppendChunks(),
          bytesReceived(0),
          staleChunksHexFormatFlag(false)
        {}
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const
    {
        return "Chunkserver Hello";
    }
    bool Validate()
    {
        return (ServerLocation::IsValid() &&
            (contentIntBase == 10 || contentIntBase == 16));
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
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
        .Def("Stale-chunks-hex-format",      &MetaHello::staleChunksHexFormatFlag, false)
        ;
    }
};

/*!
 * \brief whenever a chunk server goes down, this message is used to clean up state.
 */
struct MetaBye: public MetaRequest {
    ChunkServerPtr server; //!< The chunkserver that went down
    MetaBye(seq_t s, const ChunkServerPtr& c):
        MetaRequest(META_BYE, false, s), server(c) { }
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual string Show() const
    {
        return "Chunkserver Bye";
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
    virtual void response(ostream &os);
    virtual int log(ostream& /* file */) const { return 0; }
    virtual string Show() const
    {
        ostringstream os;
        os <<
            "get pathname:"
            " fid: "     << fid <<
            " chunkId: " << chunkId <<
            " status: "  << status
        ;
        return os.str();
    }
    bool Validate()
        { return (fid >= 0 || chunkId >= 0); }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("File-handle",  &MetaGetPathName::fid,     fid_t(-1))
        .Def("Chunk-handle", &MetaGetPathName::chunkId, chunkId_t(-1))
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
    virtual void handle();
    virtual void response(ostream &os);
    virtual int log(ostream& file) const;
    virtual string Show() const
    {
        ostringstream os;
        os <<
            "chmod:"
            " fid: "    << fid <<
            " mode: "   << oct << mode << dec <<
            " status: " << status
        ;
        return os.str();
    }
    bool Validate()
        { return (fid >= 0 && mode != kKfsModeUndef); }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("File-handle",  &MetaChmod::fid,    fid_t(-1))
        .Def("Mode",         &MetaChmod::mode,   kKfsModeUndef)
        ;
    }
};

struct MetaChown: public MetaRequest {
    fid_t    fid;
    kfsUid_t user;
    kfsGid_t group;
    MetaChown()
        : MetaRequest(META_CHOWN, true),
          fid(-1),
          user(kKfsUserNone),
          group(kKfsGroupNone)
        {}
    virtual void handle();
    virtual void response(ostream &os);
    virtual int log(ostream& file) const;
    virtual string Show() const
    {
        ostringstream os;
        os <<
            "chown:"
            " euser: "  << euser <<
            " egroup: " << egroup <<
            " fid: "    << fid <<
            " user: "   << user <<
            " group: "  << group <<
            " status: " << status
        ;
        return os.str();
    }
    bool Validate()
        { return (fid >= 0); }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("File-handle", &MetaChown::fid,    fid_t(-1))
        .Def("Owner",       &MetaChown::user,   kKfsUserNone)
        .Def("Group",       &MetaChown::group,  kKfsGroupNone)
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
    virtual int  log(ostream& /* file */) const { return 0; }
    virtual void request(ostream& os, IOBuffer& /* buf */) { request(os); }
    virtual void handleReply(const Properties& prop) {}
    virtual void handle() {}
    void resume()
    {
        submit_request(this);
    }
protected:
    virtual void request(ostream& /* os */) {}
};

/*!
 * \brief Allocate RPC from meta server to chunk server
 */
struct MetaChunkAllocate : public MetaChunkRequest {
    const int64_t       leaseId;
    MetaAllocate* const req;
    MetaChunkAllocate(seq_t n, MetaAllocate *r,
            const ChunkServerPtr& s, int64_t l)
        : MetaChunkRequest(META_CHUNK_ALLOCATE, n, false, s, r->chunkId),
          leaseId(l),
          req(r)
          {}
    virtual void handle();
    virtual void request(ostream &os);
    virtual string Show() const
    {
        return ("meta->chunk allocate:" + (req ? req->Show() : string()));
    }
};

/*!
 * \brief Delete RPC from meta server to chunk server
 */
struct MetaChunkDelete: public MetaChunkRequest {
    MetaChunkDelete(seq_t n, const ChunkServerPtr& s, chunkId_t c)
        : MetaChunkRequest(META_CHUNK_DELETE, n, false, s, c)
        {}
    virtual void request(ostream &os);
    virtual string Show() const
    {
        ostringstream os;

        os << "meta->chunk delete: chunkId: " << chunkId;
        return os.str();
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
    typedef map<int, pair<chunkId_t, seq_t> > InvalidStripes;

    fid_t                fid;          //!< input: we tell the chunkserver what it is
    seq_t                chunkVersion; //!< io: the chunkservers tells us what it did
    chunkOff_t           chunkOffset;  //!< input: chunk recovery parameters
    int16_t              striperType;
    int16_t              numStripes;
    int16_t              numRecoveryStripes;
    int32_t              stripeSize;
    ChunkServerPtr       dataServer;  //!< where to get a copy from
    ServerLocation       srcLocation;
    string               pathname;
    int64_t              fileSize;
    InvalidStripes       invalidStripes;
    MetaChunkVersChange* versChange;
    MetaChunkReplicate(seq_t n, const ChunkServerPtr& s,
            fid_t f, chunkId_t c, const ServerLocation& loc,
            const ChunkServerPtr& src)
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
          versChange(0)
        {}
    virtual ~MetaChunkReplicate() { assert(! versChange); }
    virtual void handle();
    virtual void request(ostream &os);
    virtual void handleReply(const Properties& prop);
    virtual string Show() const;
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
        MetaChunkReplicate*   repl = 0)
        : MetaChunkRequest(META_CHUNK_VERSCHANGE, n, false, s, c),
          fid(f),
          chunkVersion(v),
          fromVersion(fromVers),
          makeStableFlag(mkStableFlag),
          pendingAddFlag(pendAddFlag),
          replicate(repl)
    {
        if (replicate) {
            assert(! replicate->versChange);
            replicate->versChange = this;
        }
    }
    virtual ~MetaChunkVersChange()  { assert(! replicate); }
    virtual void handle();
    virtual void request(ostream &os);
    virtual string Show() const
    {
        ostringstream os;

        os <<
            "meta->chunk vers change:"
            " fid: "        << fid <<
            " chunkId: "        << chunkId <<
            " version: from: "  << fromVersion <<
            " => to: "          << chunkVersion <<
            " make stable: "    << makeStableFlag
        ;
        return os.str();
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
    virtual void request(ostream &os);
    virtual void handleReply(const Properties& prop)
    {
        chunkSize = prop.getValue("Size", (chunkOff_t) -1);
    }
    virtual string Show() const
    {
        ostringstream os;
        os <<
        "meta->chunk size: " << pathname <<
        " fid: "             << fid <<
        " chunkId: "         << chunkId <<
        " chunkVersion: "    << chunkVersion <<
        " size: "            << chunkSize;
        return os.str();
    }
};

/*!
 * \brief Heartbeat RPC from meta server to chunk server.  We can
 * ask the chunk server for lots of stuff; for now, we ask it
 * how much is available/used up.
 */
struct MetaChunkHeartbeat: public MetaChunkRequest {
    int64_t evacuateCount;
    MetaChunkHeartbeat(seq_t n, const ChunkServerPtr& s,
            int64_t evacuateCnt)
        : MetaChunkRequest(META_CHUNK_HEARTBEAT, n, false, s, -1),
          evacuateCount(evacuateCnt)
        {}
    virtual void request(ostream &os);
    virtual string Show() const
    {
        return "meta->chunk heartbeat";
    }
};

/*!
 * \brief Stale chunk notification message from meta->chunk.  This
 * tells the chunk servers the id's of stale chunks, which the chunk
 * server should get rid of.
 */
struct MetaChunkStaleNotify: public MetaChunkRequest {
    MetaChunkStaleNotify(seq_t n, const ChunkServerPtr& s,
            bool evacFlag, bool hexFmtFlag)
        : MetaChunkRequest(META_CHUNK_STALENOTIFY, n, false, s, -1),
          staleChunkIds(),
          evacuatedFlag(evacFlag),
          hexFormatFlag(hexFmtFlag)
        {}
    ChunkIdQueue staleChunkIds; //!< chunk ids that are stale
    bool         evacuatedFlag;
    bool         hexFormatFlag;
    virtual void request(ostream& os, IOBuffer& buf);
    virtual string Show() const
    {
        return "meta->chunk stale notify";
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
        fid(f), chunkVersion(v), serverLoc(l),
        chunkSize(-1), chunkChecksum(0)
        {}
    virtual void handle();
    virtual void request(ostream &os);
    virtual void handleReply(const Properties& prop)
    {
        chunkSize     =           prop.getValue("Chunk-size",     (int64_t) -1);
        chunkChecksum = (uint32_t)prop.getValue("Chunk-checksum", (uint64_t)0);
    }
    virtual string Show() const {
        ostringstream os;
        os << "begin-make-chunk-stable:"
        " server: "        << serverLoc <<
        " seq: "           << opSeqno <<
        " status: "        << status <<
        (statusMsg.empty() ? "" : " ") << statusMsg <<
        " fileid: "        << fid <<
        " chunkid: "       << chunkId <<
        " chunkvers: "     << chunkVersion <<
        " chunkSize: "     << chunkSize <<
        " chunkChecksum: " << chunkChecksum;
        return os.str();
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
    virtual string Show() const {
        ostringstream os;
        os << (op == META_LOG_MAKE_CHUNK_STABLE ?
            "log-make-chunk-stable:" :
            "log-make-chunk-stable-done:") <<
        " fleid: "         << fid <<
        " chunkid: "       << chunkId <<
        " chunkvers: "     << chunkVersion <<
        " chunkSize: "     << chunkSize <<
        " chunkChecksum: " << (hasChunkChecksum ?
            int64_t(chunkChecksum) : int64_t(-1));
        return os.str();
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
    const fid_t      fid;   //!< input: we tell the chunkserver what it is
    const seq_t      chunkVersion; //!< The version tha the chunk should be in
    const chunkOff_t chunkSize;
    const bool       hasChunkChecksum:1;
    const bool       addPending:1;
    const uint32_t   chunkChecksum;
    virtual void handle();
    virtual void request(ostream &os);
    virtual string Show() const;
};


/*!
 * For scheduled downtime, we evacaute all the chunks on a server; when
 * we know that the evacuation is finished, we tell the chunkserver to retire.
 */
struct MetaChunkRetire: public MetaChunkRequest {
    MetaChunkRetire(seq_t n, const ChunkServerPtr& s):
        MetaChunkRequest(META_CHUNK_RETIRE, n, false, s, -1) { }
    virtual void request(ostream &os);
    virtual string Show() const
    {
        return "chunkserver retire";
    }
};

struct MetaChunkSetProperties: public MetaChunkRequest {
    const string serverProps;
    MetaChunkSetProperties(seq_t n, const ChunkServerPtr& s,
            const Properties& props)
        : MetaChunkRequest(META_CHUNK_SET_PROPERTIES, n, false, s, -1),
          serverProps(Properties2Str(props))
    {}
    virtual void request(ostream &os);
    virtual string Show() const
    {
        return "chunkserver set properties";
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
    virtual void request(ostream &os);
    virtual string Show() const
    {
        return "chunkserver restart";
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
    virtual int log(ostream &file) const;
    virtual void response(ostream &os, IOBuffer& buf);
    virtual string Show() const
    {
        return "ping";
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
    virtual int log(ostream& file) const;
    virtual void response(ostream& os, IOBuffer& buf);
    virtual string Show() const
    {
        return "upservers";
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
    virtual void response(ostream &os);
    virtual string Show() const
    {
        return (value ? "Toggle WORM: Enabled" : "Toggle WORM: Disabled");
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Toggle-WORM", &MetaToggleWORM::value, false)
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
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const
    {
        return "stats";
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
    virtual void response(ostream &os);
    virtual string Show() const
    {
        return "recompute dir size";
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
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const
    {
        return "dump chunk2server map";
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
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const
    {
        return "checking all leases";
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
    MetaDumpChunkReplicationCandidates()
        : MetaRequest(META_DUMP_CHUNKREPLICATIONCANDIDATES, false),
          numReplication(0),
          numPendingRecovery(0),
          resp()
        {}
    // list of blocks that are being re-replicated
    size_t   numReplication;
    size_t   numPendingRecovery;
    IOBuffer resp;
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ostream &os, IOBuffer& buf);
    virtual string Show() const
    {
        return "dump chunk replication candidates";
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
    virtual int log(ostream &file) const;
    virtual void response(ostream &os, IOBuffer& buf);
    virtual string Show() const
    {
        return "fsck";
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Report-Abandoned-Files", &MetaFsck::reportAbandonedFilesFlag)
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
        vector<pair<chunkId_t, size_t> >
    > ReadInfo;
    typedef map<
        fid_t,
        vector<chunkId_t>
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
    virtual int log(ostream &file) const;
    virtual void response(ostream& os, IOBuffer& buf);
    virtual string Show() const
    {
        return "open files";
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
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const
    {
        string ret("set chunk servers properties ");
        properties.getList(ret, "", ";");
        return ret;
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
    virtual int log(ostream &file) const;
    virtual void response(ostream &os, IOBuffer& buf);
    virtual string Show() const
    {
        return string("get chunk servers counters");
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
    virtual int log(ostream &file) const;
    virtual void response(ostream &os, IOBuffer& buf);
    virtual string Show() const
    {
        return string("get chunk servers dir counters");
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
    virtual int log(ostream &file) const
    {
        return 0;
    }
    virtual void response(ostream &os, IOBuffer& buf);
    virtual string Show() const
    {
        return string("get request counters ");
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
    virtual int log(ostream &file) const
    {
        return 0;
    }
    virtual string Show() const
    {
        return string("checkpoint");
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
    virtual string Show() const          { return string("disconnect"); }
    virtual int log(ostream &file) const { return 0; }
    bool Validate()                      { return true; }
};

/*!
 * \brief Op for handling a notify of a corrupt chunk
 */
struct MetaChunkCorrupt: public MetaRequest {
    fid_t          fid;         //!< input
    chunkId_t      chunkId;     //!< input
    bool           isChunkLost; //!< input
    bool           noReplyFlag; //!< input
    bool           dirOkFlag;   //!< input
    string         chunkDir;    //!< input
    ChunkServerPtr server;      //!< The chunkserver that sent us this message
    MetaChunkCorrupt(seq_t s = -1, fid_t f = -1, chunkId_t c = -1)
        : MetaRequest(META_CHUNK_CORRUPT, false, s),
          fid(f),
          chunkId(c),
          isChunkLost(false),
          noReplyFlag(false),
          dirOkFlag(false),
          chunkDir(),
          server()
        {}
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const
    {
        ostringstream os;

        os << (isChunkLost ? "lost" : "corrupt") << " chunk: fid = " << fid << " chunkid = " << chunkId;
        return os.str();
    }
    virtual void setChunkServer(const ChunkServerPtr& cs) { server = cs; }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("File-handle",   &MetaChunkCorrupt::fid,             fid_t(-1))
        .Def("Chunk-handle",  &MetaChunkCorrupt::chunkId,     chunkId_t(-1))
        .Def("Is-chunk-lost", &MetaChunkCorrupt::isChunkLost,         false)
        .Def("No-reply",      &MetaChunkCorrupt::noReplyFlag,         false)
        .Def("Chunk-dir",     &MetaChunkCorrupt::chunkDir)
        .Def("Dir-ok",        &MetaChunkCorrupt::dirOkFlag,           false)
        ;
    }
};

/*!
 * \brief chunk server chunks evacuate request
 */
struct MetaChunkEvacuate: public MetaRequest {
    int64_t             totalSpace;
    int64_t             totalFsSpace;
    int64_t             usedSpace;
    int                 numDrives;
    int                 numWritableDrives;
    int                 numEvacuateInFlight;
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
          chunkIds(),
          server()
        {}
    virtual void handle();
    virtual int log(ostream &file) const
    {
        return 0;
    }
    virtual void response(ostream &os);
    virtual string Show() const
    {
        return ("evacuate: " + chunkIds.GetStr());
    }
    virtual void setChunkServer(const ChunkServerPtr& cs) { server = cs; }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Chunk-ids",      &MetaChunkEvacuate::chunkIds)
        .Def("Total-space",    &MetaChunkEvacuate::totalSpace,      int64_t(-1))
        .Def("Total-fs-space", &MetaChunkEvacuate::totalFsSpace,    int64_t(-1))
        .Def("Used-space",     &MetaChunkEvacuate::usedSpace,       int64_t(-1))
        .Def("Num-drives",     &MetaChunkEvacuate::numDrives,           int(-1))
        .Def("Num-wr-drives",  &MetaChunkEvacuate::numWritableDrives,   int(-1))
        .Def("Num-evacuate",   &MetaChunkEvacuate::numEvacuateInFlight, int(-1))
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
    virtual int log(ostream &file) const
    {
        return 0;
    }
    virtual void response(ostream& os);
    virtual string Show() const
    {
        return ("chunk available: " + chunkIdAndVers.GetStr());
    }
    virtual void setChunkServer(const ChunkServerPtr& cs) { server = cs; }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Chunk-ids-vers", &MetaChunkAvailable::chunkIdAndVers)
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
    virtual int log(ostream &file) const
    {
        return 0;
    }
    virtual void response(ostream& os);
    virtual string Show() const
    {
        return ("chunk dir info");
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
    const LeaseType    leaseType;
    StringBufT<128>    pathname; // Optional for debugging.
    chunkId_t          chunkId;
    bool               flushFlag;
    int                leaseTimeout;
    int64_t            leaseId;
    StringBufT<21 * 8> chunkIds; // This and the following used by sort master.
    bool               getChunkLocationsFlag; 
    IOBuffer           responseBuf;
    MetaLeaseAcquire()
        : MetaRequest(META_LEASE_ACQUIRE, false),
          leaseType(READ_LEASE),
          pathname(),
          chunkId(-1),
          flushFlag(false),
          leaseTimeout(LEASE_INTERVAL_SECS),
          leaseId(-1),
          chunkIds(),
          getChunkLocationsFlag(false),
          responseBuf()
          {}
    virtual void handle();
    virtual int log(ostream& file) const;
    virtual void response(ostream& os, IOBuffer& buf);
    virtual string Show() const
    {
        ostringstream os;

        os <<
        "lease acquire:"
        " chunkId: " << chunkId <<
        " " << pathname <<
        (leaseType == READ_LEASE ? " read lease " : " write lease ") <<
        (flushFlag ? " flush" : "")
        ;
        return os.str();
    }
    bool Validate()
    {
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Pathname",          &MetaLeaseAcquire::pathname                         )
        .Def("Chunk-handle",      &MetaLeaseAcquire::chunkId,      chunkId_t(-1)      )
        .Def("Flush-write-lease", &MetaLeaseAcquire::flushFlag,    false              )
        .Def("Lease-timeout",     &MetaLeaseAcquire::leaseTimeout, LEASE_INTERVAL_SECS)
        .Def("Chunk-ids",         &MetaLeaseAcquire::chunkIds)
        .Def("Get-locations",     &MetaLeaseAcquire::getChunkLocationsFlag,      false)
        ;
    }
};

/*!
 * \brief Op for renewing a lease on a chunk of a file.
 */
struct MetaLeaseRenew: public MetaRequest {
    LeaseType leaseType; //!< input
    string    pathname;  //!< full pathname of the file that owns chunk
    chunkId_t chunkId;   //!< input
    int64_t   leaseId;   //!< input
    MetaLeaseRenew()
        : MetaRequest(META_LEASE_RENEW, false),
          leaseType(READ_LEASE),
          pathname(),
          chunkId(-1),
          leaseId(-1),
          leaseTypeStr()
        {}
    virtual void handle();
    virtual int log(ostream &file) const;
    virtual void response(ostream &os);
    virtual string Show() const
    {
        ostringstream os;

        os << "lease renew: " << pathname << " ";
        if (leaseType == READ_LEASE)
            os << "read lease ";
        else
            os << "write lease ";

        os << " chunkId = " << chunkId;
        return os.str();
    }
    bool Validate()
    {
        leaseType = (leaseTypeStr == "WRITE_LEASE") ?
            WRITE_LEASE : READ_LEASE;
        return true;
    }
    template<typename T> static T& ParserDef(T& parser)
    {
        return MetaRequest::ParserDef(parser)
        .Def("Lease-type",   &MetaLeaseRenew::leaseTypeStr          )
        .Def("Lease-id",     &MetaLeaseRenew::leaseId,   int64_t(-1))
        .Def("Chunk-handle", &MetaLeaseRenew::chunkId, chunkId_t(-1))
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
    MetaLeaseCleanup(seq_t s, KfsCallbackObj *c):
        MetaRequest(META_LEASE_CLEANUP, false, s) { clnt = c; }

    virtual void handle();
    virtual int log(ostream &file) const;
    virtual string Show() const
    {
        return "lease cleanup";
    }
};

/*!
 * \brief An internally generated op to check that the degree
 * of replication for each chunk is satisfactory.  This op goes
 * thru the main event processing loop.
 */
struct MetaChunkReplicationCheck : public MetaRequest {
    MetaChunkReplicationCheck(seq_t s, KfsCallbackObj *c):
        MetaRequest(META_CHUNK_REPLICATION_CHECK, false, s) { clnt = c; }

    virtual void handle();
    virtual int log(ostream &file) const;
    virtual string Show() const
    {
        return "chunk replication check";
    }
};

int ParseCommand(const IOBuffer& buf, int len, MetaRequest **res,
    char* threadParseBuffer = 0);

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
