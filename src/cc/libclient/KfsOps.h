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
#include "common/StdAllocator.h"
#include "common/RequestParser.h"
#include "kfsio/NetConnection.h"
#include "kfsio/CryptoKeys.h"
#include "KfsAttr.h"

#include <algorithm>
#include <string>
#include <iostream>
#include <sstream>
#include <vector>
#include <iomanip>
#include <map>

#include <boost/static_assert.hpp>

namespace KFS {
namespace client {
using std::string;
using std::ostringstream;
using std::ostream;
using std::istream;
using std::oct;
using std::dec;
using std::pair;
using std::make_pair;

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
    // Chunkserver RPCs
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
    CMD_AUTHENTICATE,
    CMD_DELEGATE,
    CMD_DELEGATE_CANCEL,
    // Stats and admin ops.
    CMD_CHUNK_PING,
    CMD_CHUNK_STATS,
    CMD_META_PING,
    CMD_META_STATS,
    CMD_META_TOGGLE_WORM,
    CMD_META_RETIRE_CHUNKSERVER,
    CMD_META_FSCK,
    // Meta server maintenance and debugging.
    CMD_META_CHECK_LEASES,
    CMD_META_RECOMPUTE_DIRSIZE,
    CMD_META_DUMP_CHUNKREPLICATIONCANDIDATES,
    CMD_META_OPEN_FILES,
    CMD_META_GET_CHUNK_SERVERS_COUNTERS,
    CMD_META_GET_CHUNK_SERVER_DIRS_COUNTERS,
    CMD_META_SET_CHUNK_SERVERS_PROPERTIES,
    CMD_META_GET_REQUEST_COUNTERS,
    CMD_META_DISCONNECT,
    CMD_META_FORCE_REPLICATION,
    CMD_META_DUMP_CHUNKTOSERVERMAP,
    CMD_META_UPSERVERS,

    CMD_NCMDS
};

struct KfsOp {
    class Display
    {
    public:
        Display(const KfsOp& op)
            : mOp(op)
            {}
        ostream& Show(ostream& os) const
            { return mOp.ShowSelf(os); }
    private:
        const KfsOp& mOp;
    };

    KfsOp_t  op;
    kfsSeq_t seq;
    int32_t  status;
    uint32_t checksum; // a checksum over the data
    int64_t  maxWaitMillisec;
    size_t   contentLength;
    size_t   contentBufLen;
    char*    contentBuf;
    string   statusMsg; // optional, mostly for debugging

    KfsOp (KfsOp_t o, kfsSeq_t s)
        : op(o),
          seq(s),
          status(0),
          checksum(0),
          maxWaitMillisec(-1),
          contentLength(0),
          contentBufLen(0),
          contentBuf(0),
          statusMsg(),
          contentBufOwnerFlag(true)
        {}
    // to allow dynamic-type-casting, make the destructor virtual
    virtual ~KfsOp() {
        KfsOp::DeallocContentBuf();
    }
    void AttachContentBuf(const char* buf, size_t len,
            bool ownsBufferFlag = true) {
        AttachContentBuf(const_cast<char*>(buf), len,
            ownsBufferFlag);
    }
    void EnsureCapacity(size_t len) {
        if (contentBufLen >= len && contentBufOwnerFlag) {
            return;
        }
        DeallocContentBuf();
        AllocContentBuf(len);
    }
    void AllocContentBuf(size_t len) {
        contentBuf          = new char[len + 1];
        contentBuf[len]     = 0;
        contentBufLen       = len;
        contentBufOwnerFlag = true;
    }
    void DeallocContentBuf() {
        if (contentBufOwnerFlag) {
            delete [] contentBuf;
        }
        ReleaseContentBuf();
    }
    void AttachContentBuf(char* buf, size_t len,
            bool ownsBufferFlag = true) {
        DeallocContentBuf();
        contentBuf          = buf;
        contentBufLen       = len;
        contentBufOwnerFlag = ownsBufferFlag;
    }
    void ReleaseContentBuf() {
        contentBuf    = 0;
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
    virtual Display Show() const
        { return Display(*this); }
    virtual ostream& ShowSelf(ostream& os) const = 0;
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
    bool          contentBufOwnerFlag;
    static string sExtraHeaders;
};

struct KfsNullOp : public KfsOp
{
    KfsNullOp()
        : KfsOp(CMD_UNKNOWN, 0)
        {}
    virtual void Request(ostream& /* os */)
        {}
    virtual ostream& ShowSelf(ostream& os) const
        { return (os << "NULL op"); }
};
static const KfsNullOp kKfsNullOp;

inline static ostream&
operator<<(ostream& os, const KfsOp::Display& display)
{ return display.Show(os); }

struct CreateOp : public KfsOp {
    kfsFileId_t parentFid; // input parent file-id
    const char* filename;
    kfsFileId_t fileId; // result
    int         numReplicas; // desired degree of replication
    bool        exclusive; // O_EXCL flag
    int         striperType;
    int         numStripes;
    int         numRecoveryStripes;
    int         stripeSize;
    int         metaStriperType;
    Permissions permissions;
    kfsSeq_t    reqId;
    kfsSTier_t  minSTier;
    kfsSTier_t  maxSTier;
    string      userName;
    string      groupName;
    CreateOp(kfsSeq_t s,
            kfsFileId_t        p,
            const char*        f,
            int                n,
            bool               e,
            const Permissions& perms   = Permissions(),
            kfsSeq_t           id      = -1,
            kfsSTier_t         minTier = kKfsSTierMax,
            kfsSTier_t         maxTier = kKfsSTierMax)
        : KfsOp(CMD_CREATE, s),
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
          reqId(id),
          minSTier(minTier),
          maxSTier(maxTier),
          userName(),
          groupName()
        {}
    void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "create: " << filename << " parent: " << parentFid;
        return os;
    }
};

struct RemoveOp : public KfsOp {
    kfsFileId_t parentFid; // input parent file-id
    const char *filename;
    const char *pathname;
    RemoveOp(kfsSeq_t s, kfsFileId_t p, const char* f, const char* pn)
        : KfsOp(CMD_REMOVE, s), parentFid(p), filename(f), pathname(pn)
        {}
    void Request(ostream& os);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "remove: " << filename << " (parentfid = " << parentFid << ")";
        return os;
    }
};

struct MkdirOp : public KfsOp {
    kfsFileId_t parentFid; // input parent file-id
    const char* dirname;
    Permissions permissions;
    kfsSeq_t    reqId;
    kfsFileId_t fileId; // result
    kfsSTier_t  minSTier;
    kfsSTier_t  maxSTier;
    string      userName;
    string      groupName;
    MkdirOp(kfsSeq_t s, kfsFileId_t p, const char* d,
            const Permissions& perms = Permissions(), kfsSeq_t id = -1)
        : KfsOp(CMD_MKDIR, s),
          parentFid(p),
          dirname(d),
          permissions(perms),
          reqId(id),
          fileId(-1),
          minSTier(kKfsSTierMax),
          maxSTier(kKfsSTierMax),
          userName(),
          groupName()
        {}
    void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "mkdir: " << dirname << " parent: " << parentFid;
        return os;
    }
};

struct RmdirOp : public KfsOp {
    kfsFileId_t parentFid; // input parent file-id
    const char* dirname;
    const char* pathname; // input: full pathname
    RmdirOp(kfsSeq_t s, kfsFileId_t p, const char* d, const char* pn)
        : KfsOp(CMD_RMDIR, s), parentFid(p), dirname(d), pathname(pn)
        {}
    void Request(ostream& os);
    // default parsing of OK/Cseq/Status/Content-length will suffice.

    virtual ostream& ShowSelf(ostream& os) const {
        os << "rmdir: " << dirname << " (parentfid = " << parentFid << ")";
        return os;
    }
};

struct RenameOp : public KfsOp {
    kfsFileId_t parentFid; // input parent file-id
    const char *oldname;  // old file name/dir
    const char *newpath;  // new path to be renamed to
    const char *oldpath;  // old path (starting from /)
    bool overwrite; // set if the rename can overwrite newpath
    RenameOp(kfsSeq_t s, kfsFileId_t p, const char *o,
             const char* n, const char* op, bool c) :
        KfsOp(CMD_RENAME, s), parentFid(p), oldname(o),
        newpath(n), oldpath(op), overwrite(c)
        {}
    void Request(ostream& os);

    // default parsing of OK/Cseq/Status/Content-length will suffice.

    virtual ostream& ShowSelf(ostream& os) const {
        if (overwrite) {
            os << "rename_overwrite: ";
        } else {
            os << "rename: ";
        }
        os << " old: " << oldname << " (parentfid: " << parentFid << ")";
        os << " new: " << newpath;
        return os;
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
    void Request(ostream& os);
    // This will only extract out the default+num-entries.  The actual
    // dir. entries are in the content-length portion of things
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os <<
            "readdir:"
            " fid: "     << fid <<
            " start: "   << fnameStart <<
            " entries: " << numEntries <<
            " hasmore: " << hasMoreEntriesFlag;
        return os;
    }
};

struct SetMtimeOp : public KfsOp {
    const char*    pathname;
    struct timeval mtime;
    SetMtimeOp(kfsSeq_t s, const char* p, const struct timeval& m)
        : KfsOp(CMD_SETMTIME, s), pathname(p), mtime(m)
        {}
    void Request(ostream& os);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "setmtime: " << pathname <<
            " mtime: " << mtime.tv_sec << ':' << mtime.tv_usec;
        return os;
    }
};

struct DumpChunkServerMapOp : public KfsOp {
        DumpChunkServerMapOp(kfsSeq_t s)
            : KfsOp(CMD_META_DUMP_CHUNKTOSERVERMAP, s)
            {}
        void Request(ostream& os);
        virtual void ParseResponseHeaderSelf(const Properties& prop);
        virtual ostream& ShowSelf(ostream& os) const {
            os << "dumpchunktoservermap";
            return os;
        }
};

struct UpServersOp : public KfsOp {
    UpServersOp(kfsSeq_t s)
        : KfsOp(CMD_META_UPSERVERS, s)
        {}
    void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "upservers";
        return os;
    }
};

struct DumpChunkMapOp : public KfsOp {
        DumpChunkMapOp(kfsSeq_t s)
            : KfsOp(CMD_META_DUMP_CHUNKTOSERVERMAP, s)
            {}
        void Request(ostream& os);
        virtual void ParseResponseHeaderSelf(const Properties& prop);
        virtual ostream& ShowSelf(ostream& os) const {
            os << "dumpchunkmap";
            return os;
        }
};

struct ReaddirPlusOp : public KfsOp {
    kfsFileId_t fid;         // fid of the directory
    bool        getLastChunkInfoOnlyIfSizeUnknown;
    bool        omitLastChunkInfoFlag;
    bool        fileIdAndTypeOnlyFlag;
    bool        hasMoreEntriesFlag;
    int         numEntries; // # of entries in the directory
    string      fnameStart;
    ReaddirPlusOp(kfsSeq_t s, kfsFileId_t f, bool cif, bool olcif, bool fidtof)
        : KfsOp(CMD_READDIRPLUS, s),
          fid(f),
          getLastChunkInfoOnlyIfSizeUnknown(cif),
          omitLastChunkInfoFlag(olcif),
          fileIdAndTypeOnlyFlag(fidtof),
          hasMoreEntriesFlag(false),
          numEntries(0),
          fnameStart()
        {}
    void Request(ostream& os);
    // This will only extract out the default+num-entries.  The actual
    // dir. entries are in the content-length portion of things
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os <<
            "readdirplus:"
            " fid: "     << fid <<
            " start: "   << fnameStart <<
            " entries: " << numEntries <<
            " hasmore: " << hasMoreEntriesFlag;
        return os;
    }
};

// Lookup the attributes of a file in a directory
struct LookupOp : public KfsOp {
    kfsFileId_t parentFid; // fid of the parent dir
    const char* filename;  // file in the dir
    FileAttr    fattr;     // result
    kfsUid_t    euser;     // result -- effective user set by the meta server
    kfsGid_t    egroup;    // result -- effective group set by the meta server
    int         authType;  // in / out auth type.
    bool        getAuthInfoOnlyFlag; // if set retrieve authentication info only
    string      userName;
    string      groupName;
    string      euserName;
    string      egroupName;
    LookupOp(kfsSeq_t s, kfsFileId_t p, const char* f,
        kfsUid_t eu = kKfsUserNone, kfsGid_t eg = kKfsGroupNone)
        : KfsOp(CMD_LOOKUP, s),
          parentFid(p),
          filename(f),
          euser(eu),
          egroup(eg),
          authType(kAuthenticationTypeUndef),
          getAuthInfoOnlyFlag(false),
          userName(),
          groupName(),
          euserName(),
          egroupName()
        {}
    void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);

    virtual ostream& ShowSelf(ostream& os) const {
        os << "lookup: " << filename << " parent: " << parentFid;
        return os;
    }
};

// Lookup the attributes of a file relative to a root dir.
struct LookupPathOp : public KfsOp {
    kfsFileId_t rootFid; // fid of the root dir
    const char* filename; // path relative to root
    FileAttr    fattr; // result
    kfsUid_t    euser;     // result -- effective user set by the meta server
    kfsGid_t    egroup;    // result -- effective group set by the meta server
    string      userName;
    string      groupName;
    LookupPathOp(kfsSeq_t s, kfsFileId_t r, const char* f,
        kfsUid_t eu = kKfsUserNone, kfsGid_t eg = kKfsGroupNone)
        : KfsOp(CMD_LOOKUP, s),
          rootFid(r),
          filename(f),
          euser(eu),
          egroup(eg),
          userName(),
          groupName()
        {}
    void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);

    virtual ostream& ShowSelf(ostream& os) const {
        os << "lookup_path: " << filename << " (rootFid = " << rootFid << ")";
        return os;
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
        {}
    void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "coalesce blocks: " << srcPath << "<-" << dstPath;
        return os;
    }
};

/// Get the allocation information for a chunk in a file.
struct GetAllocOp: public KfsOp {
    kfsFileId_t            fid;
    chunkOff_t             fileOffset;
    kfsChunkId_t           chunkId;      // result
    int64_t                chunkVersion; // result
    bool                   serversOrderedFlag; // result: meta server ordered the servers list
                                               // by its preference / load -- try the servers in this order.
    vector<ServerLocation> chunkServers; // result: where the chunk is hosted name/port
    string                 filename;     // input

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
    void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os <<
            "getalloc:"
            " fid: "     << fid <<
            " offset: "  << fileOffset <<
            " chunkId: " << chunkId <<
            " version: " << chunkVersion <<
            " ordered: " << serversOrderedFlag <<
            " servers: " << chunkServers.size()
        ;
        for (vector<ServerLocation>::const_iterator it = chunkServers.begin();
                it != chunkServers.end();
                ++it) {
            os << " " << *it;
        }
        return os;
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
    bool                    continueIfNoReplicasFlag;
    int                     numChunks;
    int                     maxChunks;
    bool                    hasMoreChunksFlag;
    chunkOff_t              fileSize;
    vector<ChunkLayoutInfo> chunks;
    GetLayoutOp(kfsSeq_t s, kfsFileId_t f)
        : KfsOp(CMD_GETLAYOUT, s),
          fid(f),
          startOffset(0),
          omitLocationsFlag(false),
          lastChunkOnlyFlag(false),
          continueIfNoReplicasFlag(false),
          numChunks(0),
          maxChunks(-1),
          hasMoreChunksFlag(false),
          fileSize(-1),
          chunks()
        {}
    void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    int ParseLayoutInfo(bool clearFlag = true);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "getlayout: fid: " << fid;
        return os;
    }
};

class ChunkServerAccess
{
public:
    typedef PropertiesTokenizer::Token Token;
    ChunkServerAccess()
        : mAccess(),
          mAccessBuf(0),
          mOwnsBufferFlag(false)
        {}
    ~ChunkServerAccess()
        { ChunkServerAccess::Clear(); }
    int Parse(
        int          count,
        bool         hasChunkServerAccessFlag,
        kfsChunkId_t chunkId,
        const char*  buf,
        int          bufPos,
        int          bufLen,
        bool         ownsBufferFlag);
    struct Entry
    {
        Token chunkServerAccessId;
        Token chunkServerKey;
        Token chunkAccess;

        Entry()
            : chunkServerAccessId(),
              chunkServerKey(),
              chunkAccess()
            {}
    };
    bool IsEmpty() const
        { return mAccess.empty(); }
    string GetChunkAccess(
        const ServerLocation& location,
        kfsChunkId_t          chunkId) const
    {
        Access::const_iterator const it = mAccess.find(SCLocation(
            make_pair(Token(location.hostname.data(), location.hostname.size()),
                location.port), chunkId
        ));
        return (it == mAccess.end() ? string() :
            string(
                it->second.chunkAccess.mPtr,
                (size_t)it->second.chunkAccess.mLen
        ));
    }
    const Entry* Get(
        const ServerLocation& location,
        kfsChunkId_t          chunkId,
        CryptoKeys::Key&      outKey) const
    {
        Access::const_iterator const it = mAccess.find(SCLocation(
            make_pair(Token(location.hostname.data(), location.hostname.size()),
                location.port), chunkId
        ));
        if (it == mAccess.end()) {
            return 0;
        }
        const Entry& entry = it->second;
        if (! outKey.Parse(
                entry.chunkServerKey.mPtr,
                entry.chunkServerKey.mLen)) {
            return 0;
        }
        return &entry;
    }
    const Entry* Get(
        size_t           i,
        ServerLocation&  location,
        kfsChunkId_t     chunkId,
        CryptoKeys::Key& outKey) const
    {
        Access::const_iterator it = mAccess.begin();
        for (size_t k = 0; it != mAccess.end() && k < i; k++) {
            ++it;
        }
        if (it == mAccess.end()) {
            return 0;
        }
        location.hostname.assign(
            it->first.first.first.mPtr, it->first.first.first.mLen);
        location.port = it->first.first.second;
        const Entry& entry = it->second;
        if (! outKey.Parse(
                entry.chunkServerKey.mPtr,
                entry.chunkServerKey.mLen)) {
            return 0;
        }
        return &entry;
    }
    void Clear()
    {
        mAccess.clear();
        if (mOwnsBufferFlag) {
            delete [] mAccessBuf;
        }
        mAccessBuf = 0;
    }
private:
    typedef pair<pair<Token, int>, kfsChunkId_t> SCLocation;
    typedef map<
        SCLocation,
        Entry,
        less<SCLocation>,
        StdFastAllocator<pair<
            const SCLocation,
            Entry
        > >
    > Access;

    Access      mAccess;
    const char* mAccessBuf;
    bool        mOwnsBufferFlag;

private:
    ChunkServerAccess(const ChunkServerAccess&);
    ChunkServerAccess& operator=(const ChunkServerAccess&);
};

struct ChunkAccessOp: public KfsOp {
    class AccessReq
    {
    public:
        AccessReq(const ChunkAccessOp& op)
            : mOp(op)
            {}
        ostream& Write(ostream& os) const
            { return mOp.WriteReq(os); }
    private:
        const ChunkAccessOp& mOp;
    };

    kfsChunkId_t    chunkId;
    string          access;
    bool            createChunkAccessFlag:1;
    bool            createChunkServerAccessFlag:1;
    bool            hasSubjectIdFlag:1;
    int64_t         subjectId;
    int64_t         accessResponseValidForSec;
    int64_t         accessResponseIssued;
    string          chunkAccessResponse;
    string          chunkServerAccessId;
    CryptoKeys::Key chunkServerAccessKey;
    const string*   decryptKey;

    ChunkAccessOp(KfsOp_t o, kfsSeq_t s, kfsChunkId_t c)
        : KfsOp(o, s),
          chunkId(c),
          access(),
          createChunkAccessFlag(false),
          createChunkServerAccessFlag(false),
          hasSubjectIdFlag(false),
          subjectId(-1),
          accessResponseValidForSec(0),
          accessResponseIssued(0),
          chunkAccessResponse(),
          chunkServerAccessId(),
          chunkServerAccessKey(),
          decryptKey(0)
        {}
    AccessReq Access() const
        { return AccessReq(*this); }
    ostream& WriteReq(ostream& os) const
    {
        if (access.empty()) {
            return os;
        }
        if (hasSubjectIdFlag) {
            os << "Subject-id: " << subjectId << "\r\n";
        }
        return (
            (os << "C-access: ").write(access.data(), access.size()) << "\r\n" <<
            (createChunkServerAccessFlag ? "CS-access-req: 1\r\n" : "") <<
            (createChunkAccessFlag       ? "C-access-req: 1\r\n"  : "")
        );
    }
    virtual void ParseResponseHeaderSelf(const Properties& prop);
};

inline static ostream&
operator<<(ostream& os, const ChunkAccessOp::AccessReq& req)
{ return req.Write(os); }

// Get the chunk metadata (aka checksums) stored on the chunkservers
struct GetChunkMetadataOp: public ChunkAccessOp {
    bool readVerifyFlag;
    GetChunkMetadataOp(kfsSeq_t s, kfsChunkId_t c, bool verifyFlag)
        : ChunkAccessOp(CMD_GET_CHUNK_METADATA, s, c),
          readVerifyFlag(verifyFlag)
        {}
    void Request(ostream& os);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "get chunk metadata: chunkId: " << chunkId;
        return os;
    }
};

struct AllocateOp : public KfsOp {
    kfsFileId_t            fid;
    chunkOff_t             fileOffset;
    string                 pathname; // input: the full pathname corresponding to fid
    kfsChunkId_t           chunkId; // result
    int64_t                chunkVersion; // result---version # for the chunk
    // where is the chunk hosted name/port
    ServerLocation         masterServer; // master for running the write transaction
    vector<ServerLocation> chunkServers;
    // if this is set, then the metaserver will pick the offset in the
    // file at which the chunk was allocated.
    bool                   append;
    // the space reservation size that will follow the allocation.
    int                    spaceReservationSize;
    // suggested max. # of concurrent appenders per chunk
    int                    maxAppendersPerChunk;
    bool                   invalidateAllFlag;
    bool                   allowCSClearTextFlag;
    int64_t                chunkServerAccessValidForTime;
    int64_t                chunkServerAccessIssuedTime;
    string                 chunkAccess;
    string                 chunkServerAccessToken;
    CryptoKeys::Key        chunkServerAccessKey;
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
        invalidateAllFlag(false),
        allowCSClearTextFlag(false),
        chunkServerAccessValidForTime(0),
        chunkServerAccessIssuedTime(0),
        chunkAccess(),
        chunkServerAccessToken(),
        chunkServerAccessKey()
        {}
    void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
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
        os <<
            " access:" <<
            " s: " << chunkServerAccessToken <<
            " c: " << chunkAccess;
        return os;
    }
};

struct TruncateOp : public KfsOp {
    const char* pathname;
    kfsFileId_t fid;
    chunkOff_t  fileOffset;
    chunkOff_t  endOffset;
    bool        pruneBlksFromHead;
    bool        setEofHintFlag;
    bool        checkPermsFlag;
    chunkOff_t  respEndOffset;
    TruncateOp(kfsSeq_t s, const char *p, kfsFileId_t f, chunkOff_t o)
        : KfsOp(CMD_TRUNCATE, s),
          pathname(p),
          fid(f),
          fileOffset(o),
          endOffset(-1),
          pruneBlksFromHead(false),
          setEofHintFlag(true),
          checkPermsFlag(false),
          respEndOffset(-1)
        {}
    virtual void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os <<
            "truncate:"
            " fid: "    << fid <<
            " offset: " << fileOffset <<
            (pruneBlksFromHead ? " prune from head" : "")
        ;
        if (endOffset >= 0) {
            os << " end: " << endOffset;
        }
        return os;
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
    ostream& Show(ostream& os) const {
        os << " location: " << serverLoc << " writeId: " << writeId;
        return os;
    }
};

class ShowWriteInfo {
    ostream& os;
public:
    ShowWriteInfo(ostream& o) : os(o) { }
    void operator() (WriteInfo w) {
        w.Show(os) << ' ';
    }
};

struct CloseOp : public ChunkAccessOp {
    vector<ServerLocation> chunkServerLoc;
    vector<WriteInfo>      writeInfo;

    CloseOp(kfsSeq_t s, kfsChunkId_t c)
        : ChunkAccessOp(CMD_CLOSE, s, c),
          writeInfo()
        {}
    CloseOp(kfsSeq_t s, kfsChunkId_t c, const vector<WriteInfo>& wi)
        : ChunkAccessOp(CMD_CLOSE, s, c),
          writeInfo(wi)
        {}
    void Request(ostream& os);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "close: chunkid: " << chunkId;
        return os;
    }
};

// used for retrieving a chunk's size
struct SizeOp : public ChunkAccessOp {
    int64_t    chunkVersion;
    chunkOff_t size; /* result */

    SizeOp(kfsSeq_t s, kfsChunkId_t c, int64_t v)
        : ChunkAccessOp(CMD_SIZE, s, c),
          chunkVersion(v),
          size(-1)
        {}
    void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os <<
            "size:"
            " chunkid: " << chunkId <<
            " version: " << chunkVersion <<
            " size: "    << size
        ;
        return os;
    }
};


struct ReadOp : public ChunkAccessOp {
    int64_t          chunkVersion; /* input */
    chunkOff_t       offset;       /* input */
    size_t           numBytes;     /* input */
    bool             skipVerifyDiskChecksumFlag;
    struct timeval   submitTime;   /* when the client sent the request to the server */
    vector<uint32_t> checksums;    /* checksum for each 64KB block */
    float            diskIOTime;   /* as reported by the server */
    float            elapsedTime ; /* as measured by the client */

    ReadOp(kfsSeq_t s, kfsChunkId_t c, int64_t v)
        : ChunkAccessOp(CMD_READ, s, c),
          chunkVersion(v),
          offset(0),
          numBytes(0),
          skipVerifyDiskChecksumFlag(false),
          diskIOTime(0.0),
          elapsedTime(0.0)
        {}
    void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "read:"
            " chunkid: "  << chunkId <<
            " version: "  << chunkVersion <<
            " offset: "   << offset <<
            " numBytes: " << numBytes <<
            " iotm: "     << diskIOTime <<
            (skipVerifyDiskChecksumFlag ? " skip-disk-chksum" : "")
        ;
        return os;
    }
};

// op that defines the write that is going to happen
struct WriteIdAllocOp : public ChunkAccessOp {
    int64_t      chunkVersion; /* input */
    chunkOff_t   offset;       /* input */
    size_t       numBytes;     /* input */
    bool         isForRecordAppend; /* set if this is for a record append that is coming */
    bool         writePrepReplySupportedFlag;
    string       writeIdStr;   /* output */
    vector<ServerLocation> chunkServerLoc;

    WriteIdAllocOp(kfsSeq_t s, kfsChunkId_t c, int64_t v, chunkOff_t o, size_t n)
        : ChunkAccessOp(CMD_WRITE_ID_ALLOC, s, c),
          chunkVersion(v),
          offset(o),
          numBytes(n),
          isForRecordAppend(false),
          writePrepReplySupportedFlag(false)
    {

    }
    void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "write-id-alloc: chunkid: " << chunkId <<
            " version: " << chunkVersion;
        return os;
    }
};

struct WritePrepareOp : public ChunkAccessOp {
    kfsChunkId_t      chunkId;
    int64_t           chunkVersion; /* input */
    chunkOff_t        offset;       /* input */
    size_t            numBytes;     /* input */
    bool              replyRequestedFlag;
    vector<uint32_t>  checksums;    /* checksum for each 64KB block */
    vector<WriteInfo> writeInfo;    /* input */

    WritePrepareOp(kfsSeq_t s, kfsChunkId_t c, int64_t v)
        : ChunkAccessOp(CMD_WRITE_PREPARE, s, c),
          chunkVersion(v),
          offset(0),
          numBytes(0),
          replyRequestedFlag(false),
          checksums(),
          writeInfo()
        {}
    void Request(ostream& os);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "write-prepare:"
            " chunkid: "  << chunkId <<
            " version: "  << chunkVersion <<
            " offset: "   << offset <<
            " numBytes: " << numBytes <<
            " checksum: " << checksum;
        for_each(writeInfo.begin(), writeInfo.end(), ShowWriteInfo(os));
        return os;
    }
};

struct WriteSyncOp : public ChunkAccessOp {
    int64_t           chunkVersion;
    // The range of data we are sync'ing
    chunkOff_t        offset; /* input */
    size_t            numBytes; /* input */
    vector<WriteInfo> writeInfo;
    // The checksums that cover the region.
    vector<uint32_t>  checksums;

    WriteSyncOp()
        : ChunkAccessOp(CMD_WRITE_SYNC, 0, 0),
          chunkVersion(0),
          offset(0),
          numBytes(0),
          writeInfo()
        {}
    void Request(ostream& os);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "write-sync:"
            " chunkid: "  << chunkId <<
            " version: "  << chunkVersion <<
            " offset: "   << offset <<
            " numBytes: " << numBytes;
        for_each(writeInfo.begin(), writeInfo.end(), ShowWriteInfo(os));
        return os;
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

    kfsChunkId_t           chunkId;      // input
    const char*            pathname;     // input
    bool                   flushFlag;    // input
    int                    leaseTimeout; // input
    int64_t                leaseId;      // output
    int                    chunkAccessCount;
    int64_t                chunkServerAccessValidForTime;
    int64_t                chunkServerAccessIssuedTime;
    bool                   allowCSClearTextFlag;
    bool                   appendRecoveryFlag;
    vector<ServerLocation> appendRecoveryLocations;
    kfsChunkId_t*          chunkIds;
    int64_t*               leaseIds;
    bool                   getChunkLocationsFlag;

    LeaseAcquireOp(kfsSeq_t s, kfsChunkId_t c, const char* p)
        : KfsOp(CMD_LEASE_ACQUIRE, s),
          chunkId(c),
          pathname(p),
          flushFlag(false),
          leaseTimeout(-1),
          leaseId(-1),
          chunkAccessCount(0),
          chunkServerAccessValidForTime(0),
          chunkServerAccessIssuedTime(0),
          allowCSClearTextFlag(false),
          appendRecoveryFlag(false),
          appendRecoveryLocations(),
          chunkIds(0),
          leaseIds(0),
          getChunkLocationsFlag(false)
        {}
    void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "lease-acquire:"
            " chunkid: " << chunkId <<
            " leaseid: " << leaseId
        ;
        return os;
    }
};

struct LeaseRenewOp : public KfsOp {
    kfsChunkId_t chunkId;  // input
    int64_t      leaseId;  // input
    const char*  pathname; // input
    bool         getCSAccessFlag;
    int          chunkAccessCount;
    int64_t      chunkServerAccessValidForTime;
    int64_t      chunkServerAccessIssuedTime;
    bool         allowCSClearTextFlag;

    LeaseRenewOp(kfsSeq_t s, kfsChunkId_t c, int64_t l, const char* p)
        : KfsOp(CMD_LEASE_RENEW, s),
          chunkId(c),
          leaseId(l),
          pathname(p),
          getCSAccessFlag(false),
          chunkAccessCount(0),
          chunkServerAccessValidForTime(0),
          chunkServerAccessIssuedTime(0),
          allowCSClearTextFlag(false)
        {}
    void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    // default parsing of status is sufficient
    virtual ostream& ShowSelf(ostream& os) const {
        os <<
            "lease-renew:"
            " chunkid: " << chunkId <<
            " leaseId: " << leaseId;
        return os;
    }
};

// Whenever we want to give up a lease early, we notify the metaserver
// using this op.
struct LeaseRelinquishOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t      leaseId;
    string       leaseType;

    LeaseRelinquishOp(kfsSeq_t s, kfsChunkId_t c, int64_t l)
        : KfsOp(CMD_LEASE_RELINQUISH, s),
          chunkId(c),
          leaseId(l)
        {}
    void Request(ostream& os);
    // defaut parsing of status is sufficient
    virtual ostream& ShowSelf(ostream& os) const {
        os << "lease-relinquish: chunkid: " << chunkId <<
            " leaseId: " << leaseId << " type: " << leaseType;
        return os;
    }
};

/// add in ops for space reserve/release/record-append
struct ChunkSpaceReserveOp : public ChunkAccessOp {
    int64_t          chunkVersion; /* input */
    size_t            numBytes;    /* input */
    vector<WriteInfo> writeInfo;   /* input */

    ChunkSpaceReserveOp(kfsSeq_t s, kfsChunkId_t c, int64_t v,
        vector<WriteInfo> &w, size_t n)
        : ChunkAccessOp(CMD_CHUNK_SPACE_RESERVE, s, c),
          chunkVersion(v),
          numBytes(n),
          writeInfo(w)
        {}
    void Request(ostream& os);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "chunk-space-reserve: chunkid: " << chunkId <<
            " version: " << chunkVersion << " num-bytes: " << numBytes;
        return os;
    }
};

struct ChunkSpaceReleaseOp : public ChunkAccessOp {
    int64_t           chunkVersion; /* input */
    size_t            numBytes;     /* input */
    vector<WriteInfo> writeInfo;    /* input */

    ChunkSpaceReleaseOp(kfsSeq_t s, kfsChunkId_t c, int64_t v,
            const vector<WriteInfo>& w, size_t n)
        : ChunkAccessOp(CMD_CHUNK_SPACE_RELEASE, s, c),
          chunkVersion(v),
          numBytes(n),
          writeInfo(w)
        {}
    void Request(ostream& os);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "chunk-space-release: chunkid: " << chunkId <<
            " version: " << chunkVersion << " num-bytes: " << numBytes;
        return os;
    }
};

struct RecordAppendOp : public ChunkAccessOp {
    int64_t      chunkVersion;   /* input */
    chunkOff_t   offset;         /* input: this client's view of where it is writing in the file */
    vector<WriteInfo> writeInfo; /* input */

    RecordAppendOp(kfsSeq_t s, kfsChunkId_t c, int64_t v, chunkOff_t o,
        const vector<WriteInfo>& w)
        : ChunkAccessOp(CMD_RECORD_APPEND, s, c),
          chunkVersion(v),
          offset(o),
          writeInfo(w)
        {}
    void Request(ostream& os);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "record-append: chunkid: " << chunkId <<
            " version: " << chunkVersion <<
            " num-bytes: " << contentLength;
        return os;
    }
};

struct GetRecordAppendOpStatus : public ChunkAccessOp
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
    string       appenderStateStr;
    bool         masterFlag;
    bool         stableFlag;
    bool         openForAppendFlag;
    bool         widWasReadOnlyFlag;
    bool         widReadOnlyFlag;

    GetRecordAppendOpStatus(kfsSeq_t seq, kfsChunkId_t c, int64_t w) :
        ChunkAccessOp(CMD_GET_RECORD_APPEND_STATUS, seq, c),
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
    void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const
    {
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
        return os;
    }
};

struct ChangeFileReplicationOp : public KfsOp {
    kfsFileId_t fid; // input
    int16_t     numReplicas; // desired replication
    kfsSTier_t  minSTier;
    kfsSTier_t  maxSTier;
    ChangeFileReplicationOp(kfsSeq_t s, kfsFileId_t f, int16_t r)
        : KfsOp(CMD_CHANGE_FILE_REPLICATION, s),
          fid(f),
          numReplicas(r),
          minSTier(kKfsSTierUndef),
          maxSTier(kKfsSTierUndef)
        {}

    void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);

    virtual ostream& ShowSelf(ostream& os) const {
        os << "change-file-replication: fid: " << fid
           << " # of replicas: " << numReplicas;
        return os;
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
    string                 userName;
    string                 groupName;
    GetPathNameOp(kfsSeq_t s, kfsFileId_t f, kfsChunkId_t c)
        : KfsOp(CMD_GETPATHNAME, s),
          fid(f),
          chunkId(c),
          offset(-1),
          chunkVersion(-1),
          servers(),
          fattr(),
          pathname(),
          userName(),
          groupName()
        {}
    void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "getpathname:"
            " fid: "    << fid <<
            " cid: "    << chunkId <<
            " status: " << status
        ;
        return os;
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
    void Request(ostream& os);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "chmod:"
            " fid: "    << fid <<
            " mode: "   << oct << mode << dec <<
            " status: " << status
        ;
        return os;
    }
};

struct ChownOp : public KfsOp {
    kfsFileId_t fid;
    kfsUid_t    user;
    kfsGid_t    group;
    string      userName;
    string      groupName;
    ChownOp(kfsSeq_t s, kfsFileId_t f, kfsUid_t u, kfsGid_t g)
        : KfsOp(CMD_CHOWN, s),
          fid(f),
          user(u),
          group(g),
          userName(),
          groupName()
        {}
    virtual void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "chown:"
            " fid: "    << fid <<
            " uid: "    << user <<
            " gid: "    << group <<
            " user: "   << userName <<
            " group: "  << groupName <<
            " status: " << status
        ;
        return os;
    }
};

struct AuthenticateOp : public KfsOp {
    int     requestedAuthType;
    int     chosenAuthType;
    bool    useSslFlag;
    int64_t currentTime;
    int64_t sessionEndTime;

    AuthenticateOp(kfsSeq_t s, int authType)
        : KfsOp (CMD_AUTHENTICATE, s),
          requestedAuthType(authType),
          chosenAuthType(kAuthenticationTypeUndef),
          useSslFlag(false),
          currentTime(-1),
          sessionEndTime(-1)
        {}
    virtual void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "authenticate:"
            " requested: " << requestedAuthType <<
            " chosen: "    << chosenAuthType <<
            " ssl: "       << (useSslFlag ? 1 : 0) <<
            " time:"
            " cur: "       << currentTime <<
            " end: +"      << (sessionEndTime - currentTime) <<
            " status: "    << status <<
            " msg: "       << statusMsg
        ;
        return os;
    }
};

struct DelegateOp : public KfsOp {
    bool     allowDelegationFlag;
    uint32_t requestedValidForTime;
    uint32_t validForTime;
    uint32_t tokenValidForTime;
    uint64_t issuedTime;
    string   renewTokenStr;
    string   renewKeyStr;
    string   access;

    DelegateOp(kfsSeq_t s)
        : KfsOp (CMD_DELEGATE, s),
          allowDelegationFlag(false),
          requestedValidForTime(0),
          validForTime(0),
          tokenValidForTime(0),
          issuedTime(0),
          renewTokenStr(),
          renewKeyStr(),
          access()
        {}
    virtual void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "delegate:"
            " delegation bit: " << allowDelegationFlag <<
            " time: "           << requestedValidForTime <<
            " / "               << validForTime <<
            " renew: "          << renewTokenStr <<
            " status: "         << status
        ;
        return os;
    }
};

struct DelegateCancelOp : public KfsOp {
    string tokenStr;
    string keyStr;

    DelegateCancelOp(kfsSeq_t s)
        : KfsOp (CMD_DELEGATE_CANCEL, s),
          tokenStr(),
          keyStr()
        {}
    virtual void Request(ostream& os);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "delegate cancel:"
            " token: "  << tokenStr <<
            " status: " << status
        ;
        return os;
    }
};

typedef KfsOp KfsMonOp;

struct MetaPingOp : public KfsMonOp {
    vector<string> upServers; /// result
    vector<string> downServers; /// result
    MetaPingOp(kfsSeq_t s)
        : KfsMonOp(CMD_META_PING, s),
          upServers(),
          downServers()
        {}
    virtual void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "meta ping:"
            " status: " << status
        ;
        return os;
    }
};

struct MetaToggleWORMOp : public KfsMonOp {
    int value;
    MetaToggleWORMOp(kfsSeq_t s, int v)
        : KfsMonOp(CMD_META_TOGGLE_WORM, s),
          value(v)
        {}
    virtual void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "toggle worm:"
            " value: "  << value <<
            " status: " << status
        ;
        return os;
    }
};

struct ChunkPingOp : public KfsMonOp {
    ServerLocation location;
    int64_t        totalSpace;
    int64_t        usedSpace;
    ChunkPingOp(kfsSeq_t s)
        : KfsMonOp(CMD_CHUNK_PING, s),
          location(),
          totalSpace(-1),
          usedSpace(-1)
        {}
    virtual void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "chunk server ping:"
            " "         << location <<
            " status: " << status
        ;
        return os;
    }
};

struct MetaStatsOp : public KfsMonOp {
    Properties stats; // result
    MetaStatsOp(kfsSeq_t s)
        : KfsMonOp(CMD_META_STATS, s),
          stats()
        {}
    virtual void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "meta stats:"
            " status: " << status
        ;
        return os;
    }
};

struct ChunkStatsOp : public KfsMonOp {
    Properties stats; // result
    ChunkStatsOp(kfsSeq_t s)
        : KfsMonOp(CMD_CHUNK_STATS, s),
          stats()
        {}
    virtual void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "chunk stats:"
            " status: " << status
        ;
        return os;
    }
};

struct RetireChunkserverOp : public KfsMonOp {
    ServerLocation chunkLoc;
    int            downtime; // # of seconds of downtime
    RetireChunkserverOp(kfsSeq_t s, const ServerLocation &c, int d)
        : KfsMonOp(CMD_META_RETIRE_CHUNKSERVER, s),
          chunkLoc(c),
          downtime(d)
        {}
    virtual void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "retire chunk server:"
            " " << chunkLoc <<
            " down time: "  << downtime <<
            " status: "     << status
        ;
        return os;
    }
};

struct FsckOp : public KfsMonOp {
    bool reportAbandonedFilesFlag;
    FsckOp(kfsSeq_t inSeq, bool inReportAbandonedFilesFlag)
        : KfsMonOp(CMD_META_FSCK, inSeq),
          reportAbandonedFilesFlag(inReportAbandonedFilesFlag)
        {}
    virtual void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        os << "fsck:"
            " report abandoned files: "  << reportAbandonedFilesFlag <<
            " status: "                  << status
        ;
        return os;
    }
};

struct MetaMonOp : public KfsMonOp {
    Properties requestProps;
    Properties responseProps;
    MetaMonOp(KfsOp_t op, const char* inVerb, kfsSeq_t seq = 0)
        : KfsMonOp(op, seq),
          verb(inVerb)
        {}
    virtual void Request(ostream& os);
    virtual void ParseResponseHeaderSelf(const Properties& prop);
    virtual ostream& ShowSelf(ostream& os) const {
        return (os << verb << " status: " << status);
    }
private:
    const char* verb;
};

} //namespace client
} //namespace KFS

#endif // _LIBKFSCLIENT_KFSOPS_H
