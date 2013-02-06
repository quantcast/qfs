//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/05/24
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
//
//----------------------------------------------------------------------------

#ifndef _LIBKFSCLIENT_KFSOPS_H
#define _LIBKFSCLIENT_KFSOPS_H

#include "common/kfstypes.h"
#include "common/Properties.h"
#include "KfsAttr.h"

#include <algorithm>
#include <string>
#include <iostream>
#include <sstream>
#include <vector>
#include <iomanip>

#include <boost/static_assert.hpp>

namespace KFS {
namespace client {
using std::string;
using std::ostringstream;
using std::ostream;
using std::istream;
using std::oct;
using std::dec;

// KFS client library RPCs.
enum KfsOp_t {
    CMD_UNKNOWN,
    // Meta-data server RPCs
    CMD_GETALLOC,
    CMD_GETLAYOUT,
    CMD_ALLOCATE,
    CMD_TRUNCATE,
    CMD_LOOKUP,
    CMD_MKDIR,
    CMD_RMDIR,
    CMD_READDIR,
    CMD_READDIRPLUS,
    CMD_GETDIRSUMMARY,
    CMD_CREATE,
    CMD_REMOVE,
    CMD_RENAME,
    CMD_SETMTIME,
    CMD_LEASE_ACQUIRE,
    CMD_LEASE_RENEW,
    CMD_LEASE_RELINQUISH,
    CMD_COALESCE_BLOCKS,
    CMD_CHUNK_SPACE_RESERVE,
    CMD_CHUNK_SPACE_RELEASE,
    CMD_RECORD_APPEND,
    CMD_GET_RECORD_APPEND_STATUS,
    CMD_CHANGE_FILE_REPLICATION,
    CMD_DUMP_CHUNKTOSERVERMAP,
    CMD_UPSERVERS,
    // Chunkserver RPCs
    CMD_OPEN,
    CMD_CLOSE,
    CMD_READ,
    CMD_WRITE_ID_ALLOC,
    CMD_WRITE_PREPARE,
    CMD_WRITE_SYNC,
    CMD_SIZE,
    CMD_GET_CHUNK_METADATA,
    CMD_DUMP_CHUNKMAP,
    CMD_WRITE,
    CMD_GETPATHNAME,
    CMD_CHMOD,
    CMD_CHOWN,

    CMD_NCMDS,
};

struct KfsOp {
    KfsOp_t  op;
    kfsSeq_t seq;
    int32_t  status;
    uint32_t checksum; // a checksum over the data
    size_t   contentLength;
    size_t   contentBufLen;
    char*    contentBuf;
    string   statusMsg; // optional, mostly for debugging

    KfsOp (KfsOp_t o, kfsSeq_t s)
        : op(o), seq(s), status(0), checksum(0), contentLength(0),
          contentBufLen(0), contentBuf(0), statusMsg()
        {}
    // to allow dynamic-type-casting, make the destructor virtual
    virtual ~KfsOp() {
        delete [] contentBuf;
    }
    void AttachContentBuf(const char *buf, size_t len) {
        AttachContentBuf((char *) buf, len);
    }
    void EnsureCapacity(size_t len) {
        if (contentBufLen >= len) {
            return;
        }
        DeallocContentBuf();
        AllocContentBuf(len);
    }
    void AllocContentBuf(size_t len) {
        contentBuf      = new char[len + 1];
        contentBuf[len] = 0;
        contentBufLen   = len;
    }
    void DeallocContentBuf() {
        delete [] contentBuf;
        ReleaseContentBuf();
    }
    void AttachContentBuf(char *buf, size_t len) {
        contentBuf = buf;
        contentBufLen = len;
    }
    void ReleaseContentBuf() {
        contentBuf = 0;
        contentBufLen = 0;
    }
    // Build a request RPC that can be sent to the server
    virtual void Request(ostream &os) = 0;
    virtual bool NextRequest(kfsSeq_t /* seq */, ostream& /* os */)
        { return false; }

    // Common parsing code: parse the response from string and fill
    // that into a properties structure.
    void ParseResponseHeader(istream& is);
    // Parse a response header from the server: This does the
    // default parsing of OK/Cseq/Status/Content-length.
    void ParseResponseHeader(const Properties& prop);

    // Return information about op that can printed out for debugging.
    virtual string Show() const = 0;
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    // Global setting use only at startup, not re-entrant.
    // The string added to the headers section as is.
    // The headers must be properly formatted: each header line must end with
    // \r\n
    static void SetExtraRequestHeaders(const string& headers) {
        sExtraHeaders = headers;
    }
    static void AddExtraRequestHeaders(const string& headers) {
        sExtraHeaders += headers;
    }
    static void AddDefaultRequestHeaders(
        kfsUid_t euser = kKfsUserNone, kfsGid_t egroup = kKfsGroupNone);
    class ReqHeaders;
    friend class OpsHeaders;
private:
    static string sExtraHeaders;
};

struct CreateOp : public KfsOp {
    kfsFileId_t parentFid; // input parent file-id
    const char *filename;
    kfsFileId_t fileId; // result
    int numReplicas; // desired degree of replication
    bool exclusive; // O_EXCL flag
    int striperType;
    int numStripes;
    int numRecoveryStripes;
    int stripeSize;
    int metaStriperType;
    Permissions permissions;
    kfsSeq_t reqId;
    CreateOp(kfsSeq_t s, kfsFileId_t p, const char *f, int n, bool e,
            const Permissions& perms = Permissions(),
            kfsSeq_t id = -1) :
        KfsOp(CMD_CREATE, s),
        parentFid(p),
        filename(f),
        numReplicas(n),
        exclusive(e),
        striperType(KFS_STRIPED_FILE_TYPE_NONE),
        numStripes(0),
        numRecoveryStripes(0),
        stripeSize(0),
        metaStriperType(KFS_STRIPED_FILE_TYPE_UNKNOWN),
        permissions(perms),
        reqId(id)
    {}
    void Request(ostream &os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    string Show() const {
        ostringstream os;

        os << "create: " << filename << " parent: " << parentFid;
        return os.str();
    }
};

struct RemoveOp : public KfsOp {
    kfsFileId_t parentFid; // input parent file-id
    const char *filename;
    const char *pathname;
    RemoveOp(kfsSeq_t s, kfsFileId_t p, const char *f, const char *pn) :
        KfsOp(CMD_REMOVE, s), parentFid(p), filename(f), pathname(pn)
    {

    }
    void Request(ostream &os);
    string Show() const {
        ostringstream os;

        os << "remove: " << filename << " (parentfid = " << parentFid << ")";
        return os.str();
    }
};

struct MkdirOp : public KfsOp {
    kfsFileId_t parentFid; // input parent file-id
    const char *dirname;
    Permissions permissions;
    kfsSeq_t    reqId;
    kfsFileId_t fileId; // result
    MkdirOp(kfsSeq_t s, kfsFileId_t p, const char *d,
            const Permissions& perms = Permissions(), kfsSeq_t id = -1)
        : KfsOp(CMD_MKDIR, s),
          parentFid(p),
          dirname(d),
          permissions(perms),
          reqId(id),
          fileId(-1)
        {}
    void Request(ostream &os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    string Show() const {
        ostringstream os;

        os << "mkdir: " << dirname << " parent: " << parentFid;
        return os.str();
    }
};

struct RmdirOp : public KfsOp {
    kfsFileId_t parentFid; // input parent file-id
    const char *dirname;
    const char *pathname; // input: full pathname
    RmdirOp(kfsSeq_t s, kfsFileId_t p, const char *d, const char *pn) :
        KfsOp(CMD_RMDIR, s), parentFid(p), dirname(d), pathname(pn)
    {

    }
    void Request(ostream &os);
    // default parsing of OK/Cseq/Status/Content-length will suffice.

    string Show() const {
        ostringstream os;

        os << "rmdir: " << dirname << " (parentfid = " << parentFid << ")";
        return os.str();
    }
};

struct RenameOp : public KfsOp {
    kfsFileId_t parentFid; // input parent file-id
    const char *oldname;  // old file name/dir
    const char *newpath;  // new path to be renamed to
    const char *oldpath;  // old path (starting from /)
    bool overwrite; // set if the rename can overwrite newpath
    RenameOp(kfsSeq_t s, kfsFileId_t p, const char *o,
             const char *n, const char *op, bool c) :
        KfsOp(CMD_RENAME, s), parentFid(p), oldname(o),
        newpath(n), oldpath(op), overwrite(c)
    {

    }
    void Request(ostream &os);

    // default parsing of OK/Cseq/Status/Content-length will suffice.

    string Show() const {
        ostringstream os;

        if (overwrite)
            os << "rename_overwrite: ";
        else
            os << "rename: ";
        os << " old: " << oldname << " (parentfid: " << parentFid << ")";
        os << " new: " << newpath;
        return os.str();
    }
};

struct ReaddirOp : public KfsOp {
    kfsFileId_t fid;        // fid of the directory
    int         numEntries; // # of entries in the directory
    bool        hasMoreEntriesFlag;
    string      fnameStart;
    ReaddirOp(kfsSeq_t s, kfsFileId_t f)
        : KfsOp(CMD_READDIR, s),
          fid(f),
          numEntries(0),
          hasMoreEntriesFlag(false),
          fnameStart()
        {}
    void Request(ostream &os);
    // This will only extract out the default+num-entries.  The actual
    // dir. entries are in the content-length portion of things
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    string Show() const {
        ostringstream os;
        os <<
            "readdir:"
            " fid: "     << fid <<
            " start: "   << fnameStart <<
            " entries: " << numEntries <<
            " hasmore: " << hasMoreEntriesFlag;
        return os.str();
    }
};

struct SetMtimeOp : public KfsOp {
    const char *pathname;
    struct timeval mtime;
    SetMtimeOp(kfsSeq_t s, const char *p, const struct timeval &m):
        KfsOp(CMD_SETMTIME, s), pathname(p), mtime(m)
    {
    }
    void Request(ostream &os);
    string Show() const {
        ostringstream os;
        os << "setmtime: " << pathname <<
            " mtime: " << mtime.tv_sec << ':' << mtime.tv_usec;
        return os.str();
    }
};

struct DumpChunkServerMapOp : public KfsOp {
        DumpChunkServerMapOp(kfsSeq_t s):
            KfsOp(CMD_DUMP_CHUNKTOSERVERMAP, s)
        {
        }
        void Request(ostream &os);
        virtual void ParseResponseHeaderSelf(const Properties& prop);
        string Show() const {
            ostringstream os;
            os << "dumpchunktoservermap";
            return os.str();
        }
};

struct UpServersOp : public KfsOp {
    UpServersOp(kfsSeq_t s):
        KfsOp(CMD_UPSERVERS, s)
    {
    }
    void Request(ostream &os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    string Show() const {
        ostringstream os;
        os << "upservers";
        return os.str();
    }
};

struct DumpChunkMapOp : public KfsOp {
        DumpChunkMapOp(kfsSeq_t s):
            KfsOp(CMD_DUMP_CHUNKMAP, s)
        {
        }
        void Request(ostream &os);
        virtual void ParseResponseHeaderSelf(const Properties& prop);
        string Show() const {
            ostringstream os;
            os << "dumpchunkmap";
            return os.str();
        }
};

struct ReaddirPlusOp : public KfsOp {
    kfsFileId_t fid;         // fid of the directory
    bool        getLastChunkInfoOnlyIfSizeUnknown;
    bool        hasMoreEntriesFlag;
    int         numEntries; // # of entries in the directory
    string      fnameStart;
    ReaddirPlusOp(kfsSeq_t s, kfsFileId_t f, bool cif)
        : KfsOp(CMD_READDIRPLUS, s),
          fid(f),
          getLastChunkInfoOnlyIfSizeUnknown(cif),
          hasMoreEntriesFlag(false),
          numEntries(0),
          fnameStart()
        {}
    void Request(ostream &os);
    // This will only extract out the default+num-entries.  The actual
    // dir. entries are in the content-length portion of things
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    string Show() const {
        ostringstream os;
        os <<
            "readdirplus:"
            " fid: "     << fid <<
            " start: "   << fnameStart <<
            " entries: " << numEntries <<
            " hasmore: " << hasMoreEntriesFlag;
        return os.str();
    }
};

// Lookup the attributes of a file in a directory
struct LookupOp : public KfsOp {
    kfsFileId_t parentFid; // fid of the parent dir
    const char *filename; // file in the dir
    FileAttr fattr; // result
    LookupOp(kfsSeq_t s, kfsFileId_t p, const char *f) :
        KfsOp(CMD_LOOKUP, s), parentFid(p), filename(f)
    {

    }
    void Request(ostream &os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);

    string Show() const {
        ostringstream os;

        os << "lookup: " << filename << " parent: " << parentFid;
        return os.str();
    }
};

// Lookup the attributes of a file relative to a root dir.
struct LookupPathOp : public KfsOp {
    kfsFileId_t rootFid; // fid of the root dir
    const char *filename; // path relative to root
    FileAttr fattr; // result
    LookupPathOp(kfsSeq_t s, kfsFileId_t r, const char *f) :
        KfsOp(CMD_LOOKUP, s), rootFid(r), filename(f)
    {

    }
    void Request(ostream &os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);

    string Show() const {
        ostringstream os;

        os << "lookup_path: " << filename << " (rootFid = " << rootFid << ")";
        return os.str();
    }
};

/// Coalesce blocks from src->dst by appending the blocks of src to
/// dst.  If the op is successful, src will end up with 0 blocks.
struct CoalesceBlocksOp: public KfsOp {
    string     srcPath; // input
    string     dstPath; // input
    chunkOff_t dstStartOffset; // output
    CoalesceBlocksOp(kfsSeq_t s, const string& o, const string& n) :
        KfsOp(CMD_COALESCE_BLOCKS, s), srcPath(o), dstPath(n)
    {

    }
    void Request(ostream &os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    string Show() const {
        ostringstream os;

        os << "coalesce blocks: " << srcPath << "<-" << dstPath;
        return os.str();
    }
};

/// Get the allocation information for a chunk in a file.
struct GetAllocOp: public KfsOp {
    kfsFileId_t  fid;
    chunkOff_t   fileOffset;
    kfsChunkId_t chunkId; // result
    int64_t      chunkVersion; // result
    bool         serversOrderedFlag;  // result: meta server ordered the servers list
    // by its preference / load -- try the servers in this order.
    // result: where the chunk is hosted name/port
    vector<ServerLocation> chunkServers;
    string filename; // input
    GetAllocOp(kfsSeq_t s, kfsFileId_t f, chunkOff_t o)
        : KfsOp(CMD_GETALLOC, s),
          fid(f),
          fileOffset(o),
          chunkId(-1),
          chunkVersion(-1),
          serversOrderedFlag(false),
          chunkServers(),
          filename()
        {}
    void Request(ostream &os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    string Show() const {
        ostringstream os;

        os <<
            "getalloc:"
            " fid: "     << fid <<
            " offset: "  << fileOffset <<
            " chunkId: " << chunkId <<
            " version: " << chunkVersion
        ;
        return os.str();
    }
};

struct ChunkLayoutInfo {
    ChunkLayoutInfo()
        : fileOffset(-1),
          chunkId(-1),
          chunkVersion(-1),
          chunkServers()
        {}
    chunkOff_t             fileOffset;
    kfsChunkId_t           chunkId;      // result
    int64_t                chunkVersion; // result
    vector<ServerLocation> chunkServers; // where the chunk lives
    istream& Parse(istream& is);
};

inline static istream& operator>>(istream& is, ChunkLayoutInfo& li) {
    return li.Parse(is);
}

/// Get the layout information for all chunks in a file.
struct GetLayoutOp: public KfsOp {
    kfsFileId_t             fid;
    chunkOff_t              startOffset;
    bool                    omitLocationsFlag;
    bool                    lastChunkOnlyFlag;
    int                     numChunks;
    int                     maxChunks;
    bool                    hasMoreChunksFlag;
    vector<ChunkLayoutInfo> chunks;
    GetLayoutOp(kfsSeq_t s, kfsFileId_t f)
        : KfsOp(CMD_GETLAYOUT, s),
          fid(f),
          startOffset(0),
          omitLocationsFlag(false),
          lastChunkOnlyFlag(false),
          numChunks(0),
          maxChunks(-1),
          hasMoreChunksFlag(false),
          chunks()
        {}
    void Request(ostream &os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    int ParseLayoutInfo();
    string Show() const {
        ostringstream os;

        os << "getlayout: fid: " << fid;
        return os.str();
    }
};

// Get the chunk metadata (aka checksums) stored on the chunkservers
struct GetChunkMetadataOp: public KfsOp {
    kfsChunkId_t chunkId;
    bool readVerifyFlag;
    GetChunkMetadataOp(kfsSeq_t s, kfsChunkId_t c, bool verifyFlag) :
        KfsOp(CMD_GET_CHUNK_METADATA, s), chunkId(c), readVerifyFlag(verifyFlag) { }
    void Request(ostream &os);
    string Show() const {
        ostringstream os;

        os << "get chunk metadata: chunkId: " << chunkId;
        return os.str();
    }
};

struct AllocateOp : public KfsOp {
    kfsFileId_t fid;
    chunkOff_t  fileOffset;
    string      pathname; // input: the full pathname corresponding to fid
    kfsChunkId_t chunkId; // result
    int64_t chunkVersion; // result---version # for the chunk
    // where is the chunk hosted name/port
    ServerLocation masterServer; // master for running the write transaction
    vector<ServerLocation> chunkServers;
    // if this is set, then the metaserver will pick the offset in the
    // file at which the chunk was allocated.
    bool append;
    // the space reservation size that will follow the allocation.
    int spaceReservationSize;
    // suggested max. # of concurrent appenders per chunk
    int maxAppendersPerChunk;
    bool invalidateAllFlag;
    AllocateOp(kfsSeq_t s, kfsFileId_t f, const string &p) :
        KfsOp(CMD_ALLOCATE, s),
        fid(f),
        fileOffset(0),
        pathname(p),
        chunkId(-1),
        chunkVersion(-1),
        masterServer(),
        chunkServers(),
        append(false),
        spaceReservationSize(1 << 20),
        maxAppendersPerChunk(64),
        invalidateAllFlag(false)
        {}
    void Request(ostream &os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    string Show() const {
        ostringstream os;
        os << "allocate:"
            " fid: "    << fid <<
            " offset: " << fileOffset <<
            (invalidateAllFlag ? " invalidate" : "");
        const size_t sz = chunkServers.size();
        if (sz > 0) {
            os <<
                " chunkId: " << chunkId <<
                " version: " << chunkVersion <<
                " servers: "
            ;
            for (size_t i = 0; i < sz; i++) {
                os << " " << chunkServers[i];
            } 
        }
        return os.str();
    }
};

struct TruncateOp : public KfsOp {
    const char* pathname;
    kfsFileId_t fid;
    chunkOff_t  fileOffset;
    chunkOff_t  endOffset;
    bool        pruneBlksFromHead;
    bool        setEofHintFlag;
    chunkOff_t  respEndOffset;
    TruncateOp(kfsSeq_t s, const char *p, kfsFileId_t f, chunkOff_t o)
        : KfsOp(CMD_TRUNCATE, s),
          pathname(p),
          fid(f),
          fileOffset(o),
          endOffset(-1),
          pruneBlksFromHead(false),
          setEofHintFlag(true),
          respEndOffset(-1)
        {}
    virtual void Request(ostream &os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual string Show() const {
        ostringstream os;
        os <<
            "truncate:"
            " fid: "    << fid <<
            " offset: " << fileOffset <<
            (pruneBlksFromHead ? " prune from head" : "")
        ;
        if (endOffset >= 0) {
            os << " end: " << endOffset;
        }
        return os.str();
    }
};

struct OpenOp : public KfsOp {
    kfsChunkId_t chunkId;
    int openFlags;  // either O_RDONLY, O_WRONLY or O_RDWR
    OpenOp(kfsSeq_t s, kfsChunkId_t c) :
        KfsOp(CMD_OPEN, s), chunkId(c)
    {

    }
    void Request(ostream &os);
    string Show() const {
        ostringstream os;

        os << "open: chunkid: " << chunkId;
        return os.str();
    }
};

struct WriteInfo {
    ServerLocation serverLoc;
    int64_t        writeId;
    WriteInfo() : writeId(-1) { }
    WriteInfo(ServerLocation loc, int64_t w) :
        serverLoc(loc), writeId(w) { }
    WriteInfo & operator = (const WriteInfo &other) {
        serverLoc = other.serverLoc;
        writeId = other.writeId;
        return *this;
    }
    string Show() const {
        ostringstream os;

        os << " location: " << serverLoc.ToString() << " writeId: " << writeId;
        return os.str();
    }
};

class ShowWriteInfo {
    ostringstream &os;
public:
    ShowWriteInfo(ostringstream &o) : os(o) { }
    void operator() (WriteInfo w) {
        os << w.Show() << ' ';
    }
};

struct CloseOp : public KfsOp {
    kfsChunkId_t chunkId;
    vector<ServerLocation> chunkServerLoc;
    vector<WriteInfo> writeInfo;
    CloseOp(kfsSeq_t s, kfsChunkId_t c) :
        KfsOp(CMD_CLOSE, s), chunkId(c), writeInfo()
    {}
    CloseOp(kfsSeq_t s, kfsChunkId_t c, const vector<WriteInfo>& wi) :
        KfsOp(CMD_CLOSE, s), chunkId(c), writeInfo(wi)
    {}
    void Request(ostream &os);
    string Show() const {
        ostringstream os;

        os << "close: chunkid: " << chunkId;
        return os.str();
    }
};

// used for retrieving a chunk's size
struct SizeOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t      chunkVersion;
    chunkOff_t   size; /* result */
    SizeOp(kfsSeq_t s, kfsChunkId_t c, int64_t v) :
        KfsOp(CMD_SIZE, s), chunkId(c), chunkVersion(v), size(-1)
    {

    }
    void Request(ostream &os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    string Show() const {
        ostringstream os;

        os <<
            "size:"
            " chunkid: " << chunkId <<
            " version: " << chunkVersion <<
            " size: "    << size
        ;
        return os.str();
    }
};


struct ReadOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t      chunkVersion; /* input */
    chunkOff_t   offset;   /* input */
    size_t       numBytes; /* input */
    struct timeval submitTime; /* when the client sent the request to the server */
    vector<uint32_t> checksums; /* checksum for each 64KB block */
    float   diskIOTime; /* as reported by the server */
    float   elapsedTime; /* as measured by the client */

    ReadOp(kfsSeq_t s, kfsChunkId_t c, int64_t v) :
        KfsOp(CMD_READ, s), chunkId(c), chunkVersion(v),
        offset(0), numBytes(0), diskIOTime(0.0), elapsedTime(0.0)
    {

    }
    void Request(ostream &os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);

    string Show() const {
        ostringstream os;

        os << "read:"
            " chunkid: "  << chunkId <<
            " version: "  << chunkVersion <<
            " offset: "   << offset <<
            " numBytes: " << numBytes <<
            " iotm: "     << diskIOTime
        ;
        return os.str();
    }
};

// op that defines the write that is going to happen
struct WriteIdAllocOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t      chunkVersion; /* input */
    chunkOff_t   offset;   /* input */
    size_t       numBytes; /* input */
    bool         isForRecordAppend; /* set if this is for a record append that is coming */
    bool         writePrepReplySupportedFlag;
    string       writeIdStr;  /* output */
    vector<ServerLocation> chunkServerLoc;
    WriteIdAllocOp(kfsSeq_t s, kfsChunkId_t c, int64_t v, chunkOff_t o, size_t n)
        : KfsOp(CMD_WRITE_ID_ALLOC, s),
          chunkId(c),
          chunkVersion(v),
          offset(o),
          numBytes(n),
          isForRecordAppend(false),
          writePrepReplySupportedFlag(false)
    {

    }
    void Request(ostream &os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    string Show() const {
        ostringstream os;

        os << "write-id-alloc: chunkid: " << chunkId <<
            " version: " << chunkVersion;
        return os.str();
    }
};

struct WritePrepareOp : public KfsOp {
    kfsChunkId_t      chunkId;
    int64_t           chunkVersion; /* input */
    chunkOff_t        offset;       /* input */
    size_t            numBytes;     /* input */
    bool              replyRequestedFlag;
    vector<uint32_t>  checksums;    /* checksum for each 64KB block */
    vector<WriteInfo> writeInfo;    /* input */
    WritePrepareOp(kfsSeq_t s, kfsChunkId_t c, int64_t v)
        : KfsOp(CMD_WRITE_PREPARE, s),
          chunkId(c),
          chunkVersion(v),
          offset(0),
          numBytes(0),
          replyRequestedFlag(false),
          checksums(),
          writeInfo()
        {}
    void Request(ostream &os);
    string Show() const {
        ostringstream os;

        os << "write-prepare:"
            " chunkid: "  << chunkId <<
            " version: "  << chunkVersion <<
            " offset: "   << offset <<
            " numBytes: " << numBytes <<
            " checksum: " << checksum;
        for_each(writeInfo.begin(), writeInfo.end(), ShowWriteInfo(os));
        return os.str();
    }
};

struct WriteSyncOp : public KfsOp {
    kfsChunkId_t      chunkId;
    int64_t           chunkVersion;
    // The range of data we are sync'ing
    chunkOff_t        offset; /* input */
    size_t            numBytes; /* input */
    vector<WriteInfo> writeInfo;
    // The checksums that cover the region.
    vector<uint32_t>  checksums;
    WriteSyncOp()
        : KfsOp(CMD_WRITE_SYNC, 0),
          chunkId(0),
          chunkVersion(0),
          offset(0),
          numBytes(0),
          writeInfo()
        {}
    void Request(ostream &os);
    string Show() const {
        ostringstream os;

        os << "write-sync:"
            " chunkid: "  << chunkId <<
            " version: "  << chunkVersion <<
            " offset: "   << offset <<
            " numBytes: " << numBytes;
        for_each(writeInfo.begin(), writeInfo.end(), ShowWriteInfo(os));
        return os.str();
    }
};

struct ChunkLeaseInfo {
    ChunkLeaseInfo()
        : leaseId(-1),
          chunkServers()
        {}
    int64_t                leaseId;
    vector<ServerLocation> chunkServers;

    istream& Parse(istream& is) {
        chunkServers.clear();
        int numServers = 0;
        if (! (is >> leaseId >> numServers)) {
            return is;
        }
        ServerLocation loc;
        for (int i = 0; i < numServers && (is >> loc); ++i) {
            chunkServers.push_back(loc);
        }
        return is;
    }
};

inline static istream& operator>>(istream& is, ChunkLeaseInfo& li) {
    return li.Parse(is);
}

struct LeaseAcquireOp : public KfsOp {
    enum { kMaxChunkIds = 256 };
    BOOST_STATIC_ASSERT(kMaxChunkIds * 21 + (1<<10) < MAX_RPC_HEADER_LEN);

    kfsChunkId_t  chunkId;      // input
    const char*   pathname;     // input
    bool          flushFlag;    // input
    int           leaseTimeout; // input
    int64_t       leaseId;      // output
    kfsChunkId_t* chunkIds;
    int64_t*      leaseIds;
    bool          getChunkLocationsFlag;

    LeaseAcquireOp(kfsSeq_t s, kfsChunkId_t c, const char *p)
        : KfsOp(CMD_LEASE_ACQUIRE, s),
          chunkId(c),
          pathname(p),
          flushFlag(false),
          leaseTimeout(-1),
          leaseId(-1),
          chunkIds(0),
          leaseIds(0),
          getChunkLocationsFlag(false)
        {}
    void Request(ostream &os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    string Show() const {
        ostringstream os;
        os << "lease-acquire:"
            " chunkid: " << chunkId <<
            " leaseid: " << leaseId
        ;
        return os.str();
    }
};

struct LeaseRenewOp : public KfsOp {
    kfsChunkId_t chunkId; // input
    int64_t leaseId; // input
    const char *pathname; // input
    LeaseRenewOp(kfsSeq_t s, kfsChunkId_t c, int64_t l, const char *p) :
        KfsOp(CMD_LEASE_RENEW, s), chunkId(c), leaseId(l), pathname(p)
    {

    }

    void Request(ostream &os);
    // default parsing of status is sufficient

    string Show() const {
        ostringstream os;
        os << "lease-renew: chunkid: " << chunkId << " leaseId: " << leaseId;
        return os.str();
    }
};

// Whenever we want to give up a lease early, we notify the metaserver
// using this op.
struct LeaseRelinquishOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t leaseId;
    string leaseType;
    LeaseRelinquishOp(kfsSeq_t s, kfsChunkId_t c, int64_t l) :
        KfsOp(CMD_LEASE_RELINQUISH, s), chunkId(c), leaseId(l)
    {

    }
    void Request(ostream &os);
    // defaut parsing of status is sufficient
    string Show() const {
        ostringstream os;

        os << "lease-relinquish: chunkid: " << chunkId <<
            " leaseId: " << leaseId << " type: " << leaseType;
        return os.str();
    }
};

/// add in ops for space reserve/release/record-append
struct ChunkSpaceReserveOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t      chunkVersion; /* input */
    size_t       numBytes; /* input */
    vector<WriteInfo> writeInfo; /* input */
    ChunkSpaceReserveOp(kfsSeq_t s, kfsChunkId_t c, int64_t v, vector<WriteInfo> &w, size_t n) :
        KfsOp(CMD_CHUNK_SPACE_RESERVE, s), chunkId(c), chunkVersion(v),
        numBytes(n), writeInfo(w)
    {

    }
    void Request(ostream &os);
    string Show() const {
        ostringstream os;

        os << "chunk-space-reserve: chunkid: " << chunkId <<
            " version: " << chunkVersion << " num-bytes: " << numBytes;
        return os.str();
    }
};

struct ChunkSpaceReleaseOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t      chunkVersion; /* input */
    size_t       numBytes; /* input */
    vector<WriteInfo> writeInfo; /* input */
    ChunkSpaceReleaseOp(kfsSeq_t s, kfsChunkId_t c, int64_t v, vector<WriteInfo> &w, size_t n) :
        KfsOp(CMD_CHUNK_SPACE_RELEASE, s), chunkId(c), chunkVersion(v),
        numBytes(n), writeInfo(w)
    {

    }
    void Request(ostream &os);
    string Show() const {
        ostringstream os;

        os << "chunk-space-release: chunkid: " << chunkId <<
            " version: " << chunkVersion << " num-bytes: " << numBytes;
        return os.str();
    }
};

struct RecordAppendOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t      chunkVersion; /* input */
    chunkOff_t   offset; /* input: this client's view of where it is writing in the file */
    vector<WriteInfo> writeInfo; /* input */
    RecordAppendOp(kfsSeq_t s, kfsChunkId_t c, int64_t v, chunkOff_t o, vector<WriteInfo> &w) :
        KfsOp(CMD_RECORD_APPEND, s), chunkId(c), chunkVersion(v), offset(o), writeInfo(w)
    {

    }
    void Request(ostream &os);
    string Show() const {
        ostringstream os;

        os << "record-append: chunkid: " << chunkId <<
            " version: " << chunkVersion <<
            " num-bytes: " << contentLength;
        return os.str();
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
    string       appenderStateStr;
    bool         masterFlag;
    bool         stableFlag;
    bool         openForAppendFlag;
    bool         widWasReadOnlyFlag;
    bool         widReadOnlyFlag;

    GetRecordAppendOpStatus(kfsSeq_t seq, kfsChunkId_t c, int64_t w) :
        KfsOp(CMD_GET_RECORD_APPEND_STATUS, seq),
        chunkId(c),
        writeId(w),
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
        appenderStateStr(),
        masterFlag(false),
        stableFlag(false),
        openForAppendFlag(false),
        widWasReadOnlyFlag(false),
        widReadOnlyFlag(false)
    {}
    void Request(ostream &os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    string Show() const
    {
        ostringstream os;
        os << "get-record-append-op-status:"
            " seq: "                   << seq                <<
            " chunkId: "               << chunkId            <<
            " writeId: "               << writeId            <<
            " chunk-version: "         << chunkVersion       <<
            " op-seq: "                << opSeq              <<
            " op-status: "             << opStatus           <<
            " op-offset: "             << opOffset           <<
            " op-length: "             << opLength           <<
            " wid-read-only: "         << widReadOnlyFlag    <<
            " master-commit: "         << masterCommitOffset <<
            " next-commit: "           << nextCommitOffset   <<
            " wid-append-count: "      << widAppendCount     <<
            " wid-bytes-reserved: "    << widBytesReserved   <<
            " chunk-bytes-reserved: "  << chunkBytesReserved <<
            " remaining-lease-time: "  << remainingLeaseTime <<
            " wid-was-read-only: "     << widWasReadOnlyFlag <<
            " chunk-master: "          << masterFlag         <<
            " stable-flag: "           << stableFlag         <<
            " open-for-append-flag: "  << openForAppendFlag  <<
            " appender-state: "        << appenderState      <<
            " appender-state-string: " << appenderStateStr
        ;
        return os.str();
    }
};

struct ChangeFileReplicationOp : public KfsOp {
    kfsFileId_t fid; // input
    int16_t numReplicas; // desired replication
    ChangeFileReplicationOp(kfsSeq_t s, kfsFileId_t f, int16_t r) :
        KfsOp(CMD_CHANGE_FILE_REPLICATION, s), fid(f), numReplicas(r)
    {

    }

    void Request(ostream &os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);

    string Show() const {
        ostringstream os;
        os << "change-file-replication: fid: " << fid
           << " # of replicas: " << numReplicas;
        return os.str();
    }
};

struct GetPathNameOp : public KfsOp {
    kfsFileId_t            fid;
    kfsChunkId_t           chunkId;
    chunkOff_t             offset;
    int64_t                chunkVersion;
    vector<ServerLocation> servers;
    FileAttr               fattr;
    string                 pathname;
    GetPathNameOp(kfsSeq_t s, kfsFileId_t f, kfsChunkId_t c)
        : KfsOp(CMD_GETPATHNAME, s),
          fid(f),
          chunkId(c),
          offset(-1),
          chunkVersion(-1),
          servers(),
          fattr(),
          pathname()
        {}
    void Request(ostream &os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    string Show() const {
        ostringstream os;
        os << "getpathname:"
            " fid: "    << fid <<
            " cid: "    << chunkId <<
            " status: " << status
        ;
        return os.str();
    }
};

struct ChmodOp : public KfsOp {
    kfsFileId_t fid;
    kfsMode_t   mode;
    ChmodOp(kfsSeq_t s, kfsFileId_t f, kfsMode_t m)
        : KfsOp(CMD_CHMOD, s),
          fid(f),
          mode(m)
        {}
    void Request(ostream &os);
    string Show() const {
        ostringstream os;
        os << "chmod:"
            " fid: "    << fid <<
            " mode: "   << oct << mode << dec <<
            " status: " << status
        ;
        return os.str();
    }
};

struct ChownOp : public KfsOp {
    kfsFileId_t fid;
    kfsUid_t    user;
    kfsGid_t    group;
    ChownOp(kfsSeq_t s, kfsFileId_t f, kfsUid_t u, kfsGid_t g)
        : KfsOp(CMD_CHOWN, s),
          fid(f),
          user(u),
          group(g)
        {}
    void Request(ostream &os);
    string Show() const {
        ostringstream os;
        os << "chown:"
            " fid: "    << fid <<
            " user: "   << user <<
            " group: "  << group <<
            " status: " << status
        ;
        return os.str();
    }
};

} //namespace client
} //namespace KFS

#endif // _LIBKFSCLIENT_KFSOPS_H
