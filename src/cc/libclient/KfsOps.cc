//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/05/24
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
// Client side RPCs implementation.
//
//----------------------------------------------------------------------------

#include "KfsOps.h"
#include "utils.h"

#include "kfsio/checksum.h"
#include "kfsio/DelegationToken.h"
#include "common/BufferInputStream.h"
#include "common/kfserrno.h"
#include "common/IntToString.h"

#include <cassert>
#include <iostream>
#include <algorithm>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace KFS
{
namespace client
{
using std::istream;
using std::string;
using std::min;
using std::max;
using std::hex;

static const char* InitHostName()
{
    const int   maxLen = 1024;
    static char sHostName[maxLen + 1];
    sHostName[gethostname(sHostName, maxLen) ? 0 : min(64, maxLen)] = 0;
    return sHostName;
}
static const char* const sHostName(InitHostName());

inline ReqOstream&
KfsOp::ParentHeaders(ReqOstream& os) const
{
    if (shortRpcFormatFlag) {
        os << hex;
    }
    os << (shortRpcFormatFlag ? "c:" : "Cseq: ") << seq << "\r\n";
    if (! shortRpcFormatFlag) {
        os << "Version: " "KFS/1.0" "\r\n";
    }
    os << (shortRpcFormatFlag ? "p:" : "Client-Protocol-Version: ") <<
        KFS_CLIENT_PROTO_VERS << "\r\n";
    if (extraHeaders) {
        os << *(extraHeaders);
    }
    if (0 < maxWaitMillisec) {
        os << (shortRpcFormatFlag ? "w:" : "Max-wait-ms: ") <<
            maxWaitMillisec << "\r\n";
    }
    return os;
}

inline ReqOstream&
KfsIdempotentOp::ParentHeaders(ReqOstream& os) const
{
    KfsOp::ParentHeaders(os);
    if (0 <= reqId) {
        os << (shortRpcFormatFlag ? "r:" : "Rid: ") << reqId << "\r\n";
    }
    return os;
}

template<typename T>
class KfsOp::ReqHeadersT
{
public:
    ReqHeadersT(const T& o)
        : op(o)
        {}
    ReqOstream& Insert(ReqOstream& os) const
        { return op.ParentHeaders(os); }
private:
    const T& op;
};

template<typename T>
inline static ReqOstream& operator<<(
    ReqOstream&                 os,
    const KfsOp::ReqHeadersT<T>& hdrs) {
    return hdrs.Insert(os);
}

template<typename T>
inline KfsOp::ReqHeadersT<T>
KfsOp::ReqHeaders(const T& op)
{
    return ReqHeadersT<T>(op);
}

inline ReqOstream&
PutPermissions(bool shortRpcFormatFlag,
    ReqOstream& os, const Permissions& permissions)
{
    if (permissions.user != kKfsUserNone) {
        os << (shortRpcFormatFlag ? "O:" : "Owner: ") <<
            permissions.user << "\r\n";
    }
    if (permissions.group != kKfsGroupNone) {
        os << (shortRpcFormatFlag ? "G:" : "Group: ") <<
            permissions.group << "\r\n";
    }
    if (permissions.mode != kKfsModeUndef) {
        os << (shortRpcFormatFlag ? "M:" : "Mode: ")  <<
            permissions.mode << "\r\n";
    }
    return os;
}

    int
ChunkServerAccess::Parse(
    int          count,
    bool         hasChunkServerAccessFlag,
    kfsChunkId_t chunkId,
    const char*  buf,
    int          bufPos,
    int          bufLen,
    bool         ownsBufferFlag)
{
    Clear();
    mAccessBuf      = buf;
    mOwnsBufferFlag = ownsBufferFlag;
    const char*       p           = buf + bufPos;
    const char* const e           = buf + bufLen;
    const int         tokenCount  = 0 <= chunkId ?
        (hasChunkServerAccessFlag ? 5 : 3) : 6;
    Token             tokens[6];
    for (int i = 0; i < count; i++) {
        for (int k = 0; k < tokenCount; k++) {
            while (p < e && (*p & 0xFF) <= ' ') {
                p++;
            }
            const char* const s = p;
            while (p < e && ' ' < (*p & 0xFF)) {
                p++;
            }
            tokens[k] = Token(s, p);
        }
        if (tokens[tokenCount - 1].mLen <= 0) {
            Clear();
            return -EINVAL;
        }
        int          n = 0;
        kfsChunkId_t cid = chunkId;
        const char*  ptr;
        if (cid < 0) {
            ptr = tokens[n].mPtr;
            if (! HexIntParser::Parse(ptr, tokens[n].mLen, cid) || cid < 0) {
                Clear();
                return -EINVAL;
            }
            ++n;
        }
        ++n;
        ptr = tokens[n].mPtr;
        int port = -1;
        if (! HexIntParser::Parse(ptr, tokens[n].mLen, port) ||
                port <= 0) {
            Clear();
            return -EINVAL;
        }
        Entry& entry = mAccess[
            SCLocation(make_pair(tokens[n - 1], port), cid)];
        if (0 < entry.chunkAccess.mLen) {
            // Duplicate entry.
            Clear();
            return -EINVAL;
        }
        if (hasChunkServerAccessFlag) {
            entry.chunkServerAccessId = tokens[++n];
            entry.chunkServerKey      = tokens[++n];
        }
        entry.chunkAccess = tokens[++n];
    }
    while (p < e && (*p & 0xFF) <= ' ') {
        p++;
    }
    return (int)(p - (buf + bufPos));
}

///
/// All Request() methods build a request RPC based on the KFS
/// protocol and output the request into a ostream.
/// @param[out] os which contains the request RPC.
///
void
CreateOp::Request(ReqOstream& os)
{
    os <<
        "CREATE \r\n"               << ReqHeaders(*this) <<
        (shortRpcFormatFlag ? "P:" : "Parent File-handle: ")
            << parentFid << "\r\n" <<
        (shortRpcFormatFlag ? "N:" : "Filename: ") <<
            filename << "\r\n" <<
        (shortRpcFormatFlag ? "R:" : "Num-replicas: ") <<
            numReplicas << "\r\n" <<
        (shortRpcFormatFlag ? "E:" : "Exclusive: ") <<
            (exclusive ? 1 : 0)   << "\r\n"
    ;
    if (striperType != KFS_STRIPED_FILE_TYPE_NONE &&
            striperType != KFS_STRIPED_FILE_TYPE_UNKNOWN) {
        os <<
            (shortRpcFormatFlag ? "ST:" : "Striper-type: ") <<
                striperType << "\r\n" <<
            (shortRpcFormatFlag ? "SN:" : "Num-stripes: ") <<
                numStripes << "\r\n" <<
            (shortRpcFormatFlag ? "SR:" : "Num-recovery-stripes: ") <<
                numRecoveryStripes << "\r\n" <<
            (shortRpcFormatFlag ? "SS:" : "Stripe-size: ") <<
                stripeSize << "\r\n"
        ;
    }
    PutPermissions(shortRpcFormatFlag, os, permissions);
    if (minSTier < kKfsSTierMax) {
        os <<
            (shortRpcFormatFlag ? "TL:" : "Min-tier: ") <<
                (int)minSTier << "\r\n" <<
            (shortRpcFormatFlag ? "TH:" : "Max-tier: ") <<
                (int)maxSTier << "\r\n";
    }
    os << "\r\n";
}

void
MkdirOp::Request(ReqOstream& os)
{
    os <<
        "MKDIR \r\n"           << ReqHeaders(*this) <<
        (shortRpcFormatFlag ? "P:" : "Parent File-handle: ") <<
            parentFid << "\r\n" <<
        (shortRpcFormatFlag ? "N:" : "Directory: ") <<
            dirname << "\r\n"
    ;
    PutPermissions(shortRpcFormatFlag, os, permissions);
    os << "\r\n";
}

void
RmdirOp::Request(ReqOstream& os)
{
    os <<
        "RMDIR \r\n"           << ReqHeaders(*this) <<
        (shortRpcFormatFlag ? "P:" : "Parent File-handle: ") <<
            parentFid << "\r\n" <<
        (shortRpcFormatFlag ? "PN:" : "Pathname: ") << pathname << "\r\n" <<
        (shortRpcFormatFlag ? "N:"  : "Directory: ") << dirname << "\r\n"
    "\r\n";
}

void
RenameOp::Request(ReqOstream& os)
{
    os <<
        "RENAME \r\n"          << ReqHeaders(*this)   <<
        (shortRpcFormatFlag ? "P:" : "Parent File-handle: ") <<
            parentFid << "\r\n" <<
        (shortRpcFormatFlag ? "O:" : "Old-name: ") <<
            oldname << "\r\n" <<
        (shortRpcFormatFlag ? "N:" : "New-path: ") <<
            newpath << "\r\n" <<
        (shortRpcFormatFlag ? "F:" : "Old-path: ") <<
            oldpath << "\r\n"
    ;
    if (overwrite) {
        os << (shortRpcFormatFlag ? "W:1\r\n" : "Overwrite: 1\r\n");
    }
    os << "\r\n";
}

void
ReaddirOp::Request(ReqOstream& os)
{
    os <<
        "READDIR \r\n"            << ReqHeaders(*this) <<
        (shortRpcFormatFlag ? "P:" : "Directory File-handle: ") <<
            fid << "\r\n" <<
        (shortRpcFormatFlag ? "M:" : "Max-entries: ") <<
            numEntries << "\r\n"
    ;
    if (! fnameStart.empty()) {
        os << (shortRpcFormatFlag ? "S:" : "Fname-start: ") <<
            fnameStart << "\r\n";
    }
    os << "\r\n";
}

void
SetMtimeOp::Request(ReqOstream& os)
{
    os <<
        "SET_MTIME\r\n"  << ReqHeaders(*this) <<
        (shortRpcFormatFlag ? "N:" : "Pathname: ") << pathname << "\r\n" <<
        (shortRpcFormatFlag ? "S:" : "Mtime-sec: ") <<
            mtime.tv_sec      << "\r\n" <<
        (shortRpcFormatFlag ? "U:" : "Mtime-usec: ") <<
            mtime.tv_usec << "\r\n"
    ;
    if (kSetTimeTimeNotValid != atime) {
        os << (shortRpcFormatFlag ? "A:" : "Atime: ") <<
            atime << "\r\n";
    }
    if (kSetTimeTimeNotValid != ctime) {
        os << (shortRpcFormatFlag ? "C:" : "Ctime: ") <<
            ctime << "\r\n";
    }
    os << "\r\n";
}

void
DumpChunkServerMapOp::Request(ReqOstream& os)
{
    os <<
        "DUMP_CHUNKTOSERVERMAP\r\n" << ReqHeaders(*this) <<
    "\r\n";
}

void
DumpChunkMapOp::Request(ReqOstream& os)
{
    os <<
        "DUMP_CHUNKMAP\r\n" << ReqHeaders(*this) <<
    "\r\n";
}

void
UpServersOp::Request(ReqOstream& os)
{
    os <<
        "UPSERVERS\r\n" << ReqHeaders(*this) <<
    "\r\n";
}

void
ReaddirPlusOp::Request(ReqOstream& os)
{
    os <<
        "READDIRPLUS\r\n"         << ReqHeaders(*this)  <<
        (shortRpcFormatFlag ? "P:" : "Directory File-handle: ") <<
            fid << "\r\n" <<
        (shortRpcFormatFlag ? "LC:" : "GetLastChunkInfoOnlyIfSizeUnknown: ") <<
            (getLastChunkInfoOnlyIfSizeUnknown ? 1 : 0) << "\r\n" <<
        (shortRpcFormatFlag ? "M:"  : "Max-entries: ") << numEntries << "\r\n"
    ;
    if (fileIdAndTypeOnlyFlag) {
        os << (shortRpcFormatFlag ? "F:1\r\n" : "FidT-only: 1\r\n");
    } else if (omitLastChunkInfoFlag) {
        os << (shortRpcFormatFlag ? "O:1\r\n" : "Omit-lci: 1\r\n");
    }
    if (! fnameStart.empty()) {
        os << (shortRpcFormatFlag ? "S:" : "Fname-start: ") <<
            fnameStart << "\r\n";
    }
    os << "\r\n";
}

void
RemoveOp::Request(ReqOstream& os)
{
    os <<
        "REMOVE\r\n"           << ReqHeaders(*this) <<
        (shortRpcFormatFlag ? "PN:" : "Pathname: ") <<
            pathname << "\r\n" <<
        (shortRpcFormatFlag ? "P:" : "Parent File-handle: ") <<
            parentFid << "\r\n" <<
        (shortRpcFormatFlag ? "N:" : "Filename: ") << filename << "\r\n"
    "\r\n";
}

void
LookupOp::Request(ReqOstream& os)
{
    os <<
        "LOOKUP\r\n"           << ReqHeaders(*this) <<
        (shortRpcFormatFlag ? "P:" : "Parent File-handle: ") <<
            parentFid << "\r\n" <<
        (shortRpcFormatFlag ? "N:" : "Filename: ") << filename << "\r\n"
    ;
    if (authType != kAuthenticationTypeUndef) {
        os << (shortRpcFormatFlag ? "A:" : "Auth-type: ") << authType << "\r\n";
    }
    if (getAuthInfoOnlyFlag) {
        os << (shortRpcFormatFlag ? "I:1\r\n" : "Auth-info-only: 1\r\n");
    }
    if (reqShortRpcFormatFlag && ! shortRpcFormatFlag) {
        os << "Short-rpc-fmt: 1\r\n";
    }
    if (0 <= rackId) {
        os << (shortRpcFormatFlag ? "R:" : "Rack-id: ") << rackId << "\r\n";
    }
    if (! clientLocation.hostname.empty()) {
        os << (shortRpcFormatFlag ? "C:" : "Client-ip: ") <<
            clientLocation.hostname << "\r\n";
    }
    if (0 <= clientLocation.port) {
        os << (shortRpcFormatFlag ? "CP:" : "Client-port: ") <<
            clientLocation.port << "\r\n";
    }
    os << "\r\n";
}

void
LookupPathOp::Request(ReqOstream& os)
{
    os <<
        "LOOKUP_PATH\r\n"    << ReqHeaders(*this) <<
        (shortRpcFormatFlag ? "P:" : "Root File-handle: ") <<
            rootFid << "\r\n" <<
        (shortRpcFormatFlag ? "N:" : "Pathname: ") <<
            filename << "\r\n"
    "\r\n";
}

void
GetAllocOp::Request(ReqOstream& os)
{
    assert(fileOffset >= 0);

    os <<
        "GETALLOC\r\n"   << ReqHeaders(*this);
    if (! filename.empty()) {
        os << (shortRpcFormatFlag ? "N:" : "Pathname: ") << filename << "\r\n";
    }
    os <<
        (shortRpcFormatFlag ? "P:" : "File-handle: ") << fid << "\r\n" <<
        (shortRpcFormatFlag ? "O:" : "Chunk-offset: ") << fileOffset << "\r\n"
    ;
    if (objectStoreFlag) {
        os << (shortRpcFormatFlag ? "S:1\r\n" : "Obj-store: 1\r\n");
    }
    os << "\r\n";
}

void
GetLayoutOp::Request(ReqOstream& os)
{
    os <<
        "GETLAYOUT\r\n" << ReqHeaders(*this) <<
        (shortRpcFormatFlag ? "P:" : "File-handle: ") << fid << "\r\n"
    ;
    if (startOffset > 0) {
        os << (shortRpcFormatFlag ? "S:" : "Start-offset: ") <<
            startOffset << "\r\n";
    }
    if (omitLocationsFlag) {
        os << (shortRpcFormatFlag ? "O:1\r\n" : "Omit-locations: 1\r\n");
    }
    if (lastChunkOnlyFlag) {
        os << (shortRpcFormatFlag ? "L:1\r\n" : "Last-chunk-only: 1\r\n");
    }
    if (continueIfNoReplicasFlag) {
        os << (shortRpcFormatFlag ?
            "R:1\r\n" : "Continue-if-no-replicas: 1\r\n");
    }
    if (maxChunks > 0) {
        os << (shortRpcFormatFlag ? "M:" : "Max-chunks : ") <<
            maxChunks << "\r\n";
    }
    os << "\r\n";
}

void
CoalesceBlocksOp::Request(ReqOstream& os)
{
    os << "COALESCE_BLOCKS\r\n" << ReqHeaders(*this);
    if (! srcPath.empty()) {
        os << (shortRpcFormatFlag ? "S:" : "Src-path: ") << srcPath << "\r\n";
    }
    if (! dstPath.empty()) {
        os << (shortRpcFormatFlag ? "D:" : "Dest-path: ") << dstPath << "\r\n";
    }
    if (0 <= srcFid) {
        os << (shortRpcFormatFlag ? "SI:" : "Src-fid: ") << srcFid << "\r\n";
    }
    if (0 <= dstFid) {
        os << (shortRpcFormatFlag ? "DI:" : "Dst-fid: ") << dstFid << "\r\n";
    }
    os << "\r\n";
}

void
GetChunkMetadataOp::Request(ReqOstream& os)
{
    os <<
        "GET_CHUNK_METADATA\r\n" << ReqHeaders(*this) <<
        (shortRpcFormatFlag ? "H:"  : "Chunk-handle: ") << chunkId << "\r\n" <<
        (shortRpcFormatFlag ? "RV:" : "Read-verify: ")  <<
            (readVerifyFlag ? 1 : 0) << "\r\n" <<
        Access() <<
    "\r\n";
}

void
AllocateOp::Request(ReqOstream& os)
{
    os <<
        "ALLOCATE\r\n"   << ReqHeaders(*this) <<
        (shortRpcFormatFlag ? "H:" : "Client-host: ") <<
            sHostName << "\r\n";
    if (! pathname.empty()) {
        os << (shortRpcFormatFlag ? "N:" : "Pathname: ") <<
            pathname << "\r\n";
    }
    os <<
        (shortRpcFormatFlag ? "P:" : "File-handle: ")  << fid << "\r\n" <<
        (shortRpcFormatFlag ? "S:" : "Chunk-offset: ") << fileOffset << "\r\n"
    ;
    if (invalidateAllFlag) {
        os << (shortRpcFormatFlag ? "I:1\r\n" : "Invalidate-all: 1\r\n");
    }
    if (masterServer.IsValid()) {
        os << (shortRpcFormatFlag ? "C:" : "Chunk-master: ") <<
            masterServer << "\r\n";
    }
    if (append) {
        os <<
            (shortRpcFormatFlag ? "A:1\r\n" : "Chunk-append: 1\r\n") <<
            (shortRpcFormatFlag ? "R:" : "Space-reserve: ") <<
                spaceReservationSize << "\r\n" <<
            (shortRpcFormatFlag ? "M:" : "Max-appenders: ") <<
                maxAppendersPerChunk << "\r\n"
        ;
    }
    os << "\r\n";
}

void
TruncateOp::Request(ReqOstream& os)
{
    os <<
        "TRUNCATE\r\n"  << ReqHeaders(*this) <<
        (shortRpcFormatFlag ? "N:" : "Pathname: ")    << pathname   << "\r\n" <<
        (shortRpcFormatFlag ? "P:" : "File-handle: ") << fid        << "\r\n" <<
        (shortRpcFormatFlag ? "S:" : "Offset: ")      << fileOffset << "\r\n"
    ;
    if (pruneBlksFromHead) {
        os << (shortRpcFormatFlag ? "H:1\r\n" : "Prune-from-head: 1\r\n");
    }
    if (! setEofHintFlag) {
        // Default is true
        os << (shortRpcFormatFlag ? "O:0\r\n" : "Set-eof: 0\r\n");
    }
    if (checkPermsFlag) {
        os << (shortRpcFormatFlag ? "M:1\r\n" : "Check-perms: 1\r\n");
    }
    if (endOffset >= 0) {
        os << (shortRpcFormatFlag ? "E:" : "End-offset: ") << endOffset << "\r\n";
    }
    os << "\r\n";
}

void
CloseOp::Request(ReqOstream& os)
{
    os <<
        "CLOSE\r\n"      << ReqHeaders(*this) <<
        (shortRpcFormatFlag ? "H:" : "Chunk-handle: ") << chunkId << "\r\n" <<
        (shortRpcFormatFlag ? "V:" : "Chunk-version: ") <<
            chunkVersion << "\r\n"
        << Access()
    ;
    if (! writeInfo.empty()) {
        os <<
            (shortRpcFormatFlag ? "W:1\r\n" : "Has-write-id: 1\r\n") <<
            (shortRpcFormatFlag ? "R:" : "Num-servers: ") <<
                writeInfo.size() << "\r\n" <<
            (shortRpcFormatFlag ? "S:" : "Servers:")
        ;
        for (vector<WriteInfo>::const_iterator i = writeInfo.begin();
                i < writeInfo.end(); ++i) {
            os << " " << i->serverLoc << " " << i->writeId;
        }
        os << "\r\n";
    } else if (chunkServerLoc.size() > 1) {
        os <<
            (shortRpcFormatFlag ? "R:" : "Num-servers: ") <<
                chunkServerLoc.size() << "\r\n" <<
            (shortRpcFormatFlag ? "S:" : "Servers:")
        ;
        for (vector<ServerLocation>::const_iterator i = chunkServerLoc.begin();
                i != chunkServerLoc.end(); ++i) {
            os << " " << *i;
        }
        os << "\r\n";
    }
    os << "\r\n";
}

void
ReadOp::Request(ReqOstream& os)
{
    os <<
    "READ\r\n"        << ReqHeaders(*this) <<
    (shortRpcFormatFlag ? "H:" : "Chunk-handle: ")  << chunkId      << "\r\n" <<
    (shortRpcFormatFlag ? "V:" : "Chunk-version: ") << chunkVersion << "\r\n" <<
    (shortRpcFormatFlag ? "O:" : "Offset: ")        << offset       << "\r\n" <<
    (shortRpcFormatFlag ? "B:" : "Num-bytes: ")     << numBytes     << "\r\n" <<
    Access()
    ;
    if (skipVerifyDiskChecksumFlag) {
        os << (shortRpcFormatFlag ? "KS:1\r\n" : "Skip-Disk-Chksum: 1\r\n");
    }
    os << "\r\n";
}

void
WriteIdAllocOp::Request(ReqOstream& os)
{
    os <<
    "WRITE_ID_ALLOC\r\n"  << ReqHeaders(*this)           <<
    (shortRpcFormatFlag ? "H:" : "Chunk-handle: ")  << chunkId      << "\r\n" <<
    (shortRpcFormatFlag ? "V:" : "Chunk-version: ") << chunkVersion << "\r\n" <<
    (shortRpcFormatFlag ? "O:" : "Offset: ")        << offset       << "\r\n" <<
    (shortRpcFormatFlag ? "B:" : "Num-bytes: ")     << numBytes     << "\r\n" <<
    (shortRpcFormatFlag ? "A:" : "For-record-append: ") <<
        (isForRecordAppend ? 1 : 0) << "\r\n" <<
    (shortRpcFormatFlag ? "R:" : "Num-servers: ") <<
        chunkServerLoc.size() << "\r\n" <<
    Access() <<
    (shortRpcFormatFlag ? "S:" : "Servers:")
    ;
    for (vector<ServerLocation>::const_iterator it = chunkServerLoc.begin();
            it != chunkServerLoc.end();
            ++it) {
        os << ' ' << *it;
    }
    os << "\r\n\r\n";
}

void
ChunkSpaceReserveOp::Request(ReqOstream& os)
{
    os <<
    "CHUNK_SPACE_RESERVE\r\n" << ReqHeaders(*this) <<
    (shortRpcFormatFlag ? "H:" : "Chunk-handle: ")  << chunkId      << "\r\n" <<
    (shortRpcFormatFlag ? "V:" : "Chunk-version: ") << chunkVersion << "\r\n" <<
    (shortRpcFormatFlag ? "B:" : "Num-bytes: ")     << numBytes     << "\r\n" <<
    (shortRpcFormatFlag ? "R:" : "Num-servers: ") <<
        writeInfo.size() << "\r\n" <<
    Access() <<
    (shortRpcFormatFlag ? "S:" : "Servers:")
    ;
    for (vector<WriteInfo>::const_iterator it = writeInfo.begin();
            it != writeInfo.end();
            ++it) {
        os << ' ' << it->serverLoc << ' ' << it->writeId;
    }
    os << "\r\n\r\n";
}

void
ChunkSpaceReleaseOp::Request(ReqOstream& os)
{
    os <<
    "CHUNK_SPACE_RELEASE\r\n" << ReqHeaders(*this) <<
    (shortRpcFormatFlag ? "H:" : "Chunk-handle: ")  << chunkId      << "\r\n" <<
    (shortRpcFormatFlag ? "V:" : "Chunk-version: ") << chunkVersion << "\r\n" <<
    (shortRpcFormatFlag ? "B:" : "Num-bytes: ")     << numBytes     << "\r\n" <<
    (shortRpcFormatFlag ? "R:" : "Num-servers: ") <<
        writeInfo.size() << "\r\n" <<
    Access() <<
    (shortRpcFormatFlag ? "S:" : "Servers:")
    ;
    for (vector<WriteInfo>::const_iterator it = writeInfo.begin();
            it != writeInfo.end();
            ++it) {
        os << ' ' << it->serverLoc << ' ' << it->writeId;
    }
    os << "\r\n\r\n";
}

void
WritePrepareOp::Request(ReqOstream& os)
{
    // one checksum over the whole data plus one checksum per 64K block
    os <<
    "WRITE_PREPARE\r\n"  << ReqHeaders(*this) <<
    (shortRpcFormatFlag ? "H:"  : "Chunk-handle: ") << chunkId      << "\r\n" <<
    (shortRpcFormatFlag ? "V:"  : "Chunk-version: ")<< chunkVersion << "\r\n" <<
    (shortRpcFormatFlag ? "O:"  : "Offset: ")       << offset       << "\r\n" <<
    (shortRpcFormatFlag ? "B:"  : "Num-bytes: ")    << numBytes     << "\r\n" <<
    (shortRpcFormatFlag ? "K:"  : "Checksum: ")     << checksum     << "\r\n" <<
    Access()
    ;
    if (! checksums.empty()) {
        os << (shortRpcFormatFlag ? "KC:" : "Checksum-entries: ") <<
            checksums.size()  << "\r\n" <<
            (shortRpcFormatFlag ? "Ks:" : "Checksums: ");
        for (size_t i = 0; i < checksums.size(); i++) {
            os << checksums[i] << ' ';
        }
        os << "\r\n";
    }
    if (replyRequestedFlag) {
        os << (shortRpcFormatFlag ? "RR:1\r\n" : "Reply: 1\r\n");
    }
    os <<
        (shortRpcFormatFlag ? "R:" : "Num-servers: ") <<
            writeInfo.size() << "\r\n" <<
        (shortRpcFormatFlag ? "S:" : "Servers:")
    ;
    for (vector<WriteInfo>::const_iterator it = writeInfo.begin();
            it != writeInfo.end();
            ++it) {
        os << ' ' << it->serverLoc << ' ' << it->writeId;
    }
    os << "\r\n\r\n";
}

void
WriteSyncOp::Request(ReqOstream& os)
{
    os <<
    "WRITE_SYNC\r\n"     << ReqHeaders(*this) <<
    (shortRpcFormatFlag ? "H:" : "Chunk-handle: ")  << chunkId      << "\r\n" <<
    (shortRpcFormatFlag ? "V:" : "Chunk-version: ") << chunkVersion << "\r\n" <<
    (shortRpcFormatFlag ? "O:" : "Offset: ")        << offset       << "\r\n" <<
    (shortRpcFormatFlag ? "B:" : "Num-bytes: ")     << numBytes     << "\r\n" <<
    (shortRpcFormatFlag ? "KC:" : "Checksum-entries: ") <<
        checksums.size() << "\r\n" <<
    Access()
    ;
    if (! checksums.empty()) {
        os << (shortRpcFormatFlag ? "K:" : "Checksums: ");
        for (size_t i = 0; i < checksums.size(); i++) {
            os << checksums[i] << ' ';
        }
        os << "\r\n";
    }
    os <<
    (shortRpcFormatFlag ? "R:" : "Num-servers: ") << writeInfo.size() << "\r\n" <<
    (shortRpcFormatFlag ? "S:" : "Servers:")
    ;
    for (vector<WriteInfo>::const_iterator it = writeInfo.begin();
            it != writeInfo.end();
            ++it) {
        os << ' ' << it->serverLoc << ' ' << it->writeId;
    }
    os << "\r\n\r\n";
}

void
SizeOp::Request(ReqOstream& os)
{
    os <<
    "SIZE\r\n"        << ReqHeaders(*this) <<
    (shortRpcFormatFlag ? "H:" : "Chunk-handle: ")  << chunkId      << "\r\n" <<
    (shortRpcFormatFlag ? "V:" : "Chunk-version: ") << chunkVersion << "\r\n" <<
    Access() <<
    "\r\n";
}

void
LeaseAcquireOp::Request(ReqOstream& os)
{
    os << "LEASE_ACQUIRE\r\n" << ReqHeaders(*this);
    if (pathname && pathname[0]) {
        os << (shortRpcFormatFlag ? "N:" : "Pathname: ") << pathname << "\r\n";
    }
    if (chunkId >= 0) {
        os << (shortRpcFormatFlag ? "H:" : "Chunk-handle: ") <<
            chunkId << "\r\n";
    }
    if (0 <= chunkPos) {
        os << (shortRpcFormatFlag ? "O:" : "Chunk-pos: ") << chunkPos << "\r\n";
    }
    if (flushFlag) {
        os << (shortRpcFormatFlag ? "F:1\r\n" : "Flush-write-lease: 1\r\n");
    }
    if (leaseTimeout >= 0) {
        os << (shortRpcFormatFlag ? "T:" : "Lease-timeout: ") <<
            leaseTimeout << "\r\n";
    }
    if (appendRecoveryFlag) {
        os << (shortRpcFormatFlag ? "A:1\r\n" : "Append-recovery: 1\r\n");
        const size_t cnt = appendRecoveryLocations.size();
        if (0 < cnt) {
            os << (shortRpcFormatFlag ? "R:" : "Append-recovery-loc:");
            for (size_t i = 0; i < cnt; i++) {
                os << " " << appendRecoveryLocations[i];
            }
            os << "\r\n";
        }
    }
    if (chunkServer.IsValid()) {
        os << (shortRpcFormatFlag ? "C:" : "Chunk-server: ") <<
            chunkServer << "\r\n";
    }
    if (chunkIds && (leaseIds || getChunkLocationsFlag) && chunkIds[0] >= 0) {
        os << (shortRpcFormatFlag ? "I:" : "Chunk-ids:");
        for (int i = 0; i < kMaxChunkIds && 0 <= chunkIds[i]; i++) {
            os << " " << chunkIds[i];
        }
        os << "\r\n";
        if (getChunkLocationsFlag) {
            os << (shortRpcFormatFlag ? "L:1\r\n" : "Get-locations: 1\r\n");
        }
    }
    os << "\r\n";
}

void
LeaseRenewOp::Request(ReqOstream& os)
{
    os <<
    "LEASE_RENEW\r\n" << ReqHeaders(*this);
    if (pathname && *pathname) {
        os << (shortRpcFormatFlag ? "N:" : "Pathname: ") << pathname << "\r\n";
    }
    os <<
    (shortRpcFormatFlag ? "H:" : "Chunk-handle: ")  << chunkId      << "\r\n" <<
    (shortRpcFormatFlag ? "L:" : "Lease-id: ")      << leaseId      << "\r\n" <<
    (shortRpcFormatFlag ? "T:" : "Lease-type: ")    << "READ_LEASE"    "\r\n"
    ;
    if (0 <= chunkPos) {
        os << (shortRpcFormatFlag ? "O:" : "Chunk-pos: ") << chunkPos << "\r\n";
    }
    if (chunkServer.IsValid()) {
        os << (shortRpcFormatFlag ? "C:" : "Chunk-server: ") <<
            chunkServer << "\r\n";
    }
    if (getCSAccessFlag) {
        os << (shortRpcFormatFlag ? "A:1\r\n" : "CS-access: 1\r\n");
    }
    os << "\r\n";
}

void
LeaseRelinquishOp::Request(ReqOstream& os)
{
    os <<
    "LEASE_RELINQUISH\r\n" << ReqHeaders(*this) <<
    (shortRpcFormatFlag ? "H:" : "Chunk-handle:") << chunkId << "\r\n" <<
    (shortRpcFormatFlag ? "L:" : "Lease-id: ")    << leaseId << "\r\n" <<
    (shortRpcFormatFlag ? "T:" : "Lease-type: ")  << "READ_LEASE" "\r\n"
    ;
    if (0 <= chunkPos) {
        os << (shortRpcFormatFlag ? "O:" : "Chunk-pos: ") << chunkPos << "\r\n";
    }
    os << "\r\n";
}

void
RecordAppendOp::Request(ReqOstream& os)
{
    os <<
    "RECORD_APPEND\r\n" << ReqHeaders(*this) <<
    (shortRpcFormatFlag ? "H:" : "Chunk-handle: ")  << chunkId      << "\r\n" <<
    (shortRpcFormatFlag ? "V:" : "Chunk-version: ") << chunkVersion << "\r\n" <<
    (shortRpcFormatFlag ? "B:" : "Num-bytes: ")     <<
        contentLength << "\r\n" <<
    (shortRpcFormatFlag ? "K:" : "Checksum: ")      << checksum     << "\r\n" <<
    (shortRpcFormatFlag ? "O:" : "Offset: ")        << offset       << "\r\n"
    ;
    if (! shortRpcFormatFlag) {
        os << "File-offset: " "-1" "\r\n";
    }
    os << (shortRpcFormatFlag ? "R:" : "Num-servers: ") <<
        writeInfo.size()  << "\r\n" <<
    Access() <<
    (shortRpcFormatFlag ? "S:" : "Servers:")
    ;
    for (vector<WriteInfo>::const_iterator it = writeInfo.begin();
            it != writeInfo.end();
            ++it) {
        os << ' ' << it->serverLoc << ' ' << it->writeId;
    }
    os << "\r\n\r\n";
}

void
GetRecordAppendOpStatus::Request(ReqOstream& os)
{
    os <<
    "GET_RECORD_APPEND_OP_STATUS\r\n" << ReqHeaders(*this) <<
    (shortRpcFormatFlag ? "H:" : "Chunk-handle: ") << chunkId << "\r\n" <<
    (shortRpcFormatFlag ? "W:" : "Write-id: ")     << writeId << "\r\n" <<
    Access() <<
    "\r\n";
}

void
ChangeFileReplicationOp::Request(ReqOstream& os)
{
    os <<
    "CHANGE_FILE_REPLICATION\r\n" << ReqHeaders(*this) <<
    (shortRpcFormatFlag ? "P:" : "File-handle: ")  << fid         << "\r\n" <<
    (shortRpcFormatFlag ? "R:" : "Num-replicas: ") << numReplicas << "\r\n"
    ;
    if (minSTier != kKfsSTierUndef) {
        os << (shortRpcFormatFlag ? "TL:" : "Min-tier: ") <<
            (int)minSTier << "\r\n";
    }
    if (maxSTier != kKfsSTierUndef) {
        os << (shortRpcFormatFlag ? "TH:" : "Max-tier: ") <<
            (int)maxSTier << "\r\n";
    }
    os << "\r\n";
}

void
GetRecordAppendOpStatus::ParseResponseHeaderSelf(const Properties& prop)
{
    chunkVersion        = prop.getValue(
        shortRpcFormatFlag ? "V" : "Chunk-version", (int64_t)-1);
    opSeq               = prop.getValue(
        shortRpcFormatFlag ? "Oc" : "Op-seq", (int64_t)-1);
    opStatus            = prop.getValue(
        shortRpcFormatFlag ? "Os" : "Op-status", -1);
    if (opStatus < 0) {
        opStatus = -KfsToSysErrno(-opStatus);
    }
    opOffset            = prop.getValue(
        shortRpcFormatFlag ? "OO" : "Op-offset", (int64_t)-1);
    opLength            = (size_t)prop.getValue(
        shortRpcFormatFlag ? "OL" : "Op-length", (uint64_t)0);
    widAppendCount      = (size_t)prop.getValue(
        shortRpcFormatFlag ? "AC" : "Wid-append-count", (uint64_t)0);
    widBytesReserved    = (size_t)prop.getValue(
        shortRpcFormatFlag ? "RW" : "Wid-bytes-reserved", (uint64_t)0);
    chunkBytesReserved  = (size_t)prop.getValue(
        shortRpcFormatFlag ? "RB" : "Chunk-bytes-reserved",(uint64_t)0);
    remainingLeaseTime  = prop.getValue(
        shortRpcFormatFlag ? "LR" : "Remaining-lease-time", (int64_t)-1);
    widWasReadOnlyFlag  = prop.getValue(
        shortRpcFormatFlag ? "WP" : "Wid-was-read-only", 0) != 0;
    masterFlag          = prop.getValue(
        shortRpcFormatFlag ? "MC" : "Chunk-master", 0) != 0;
    stableFlag          = prop.getValue(
        shortRpcFormatFlag ? "SC" : "Stable-flag", 0) != 0;
    openForAppendFlag   = prop.getValue(
        shortRpcFormatFlag ? "AO" : "Open-for-append-flag", 0) != 0;
    appenderState       = prop.getValue(
        shortRpcFormatFlag ? "AS" : "Appender-state", -1);
    appenderStateStr    = prop.getValue(
        shortRpcFormatFlag ? "As" : "Appender-state-string", "");
    masterCommitOffset  = prop.getValue(
        shortRpcFormatFlag ? "CO" : "Master-commit-offset", (int64_t)-1);
    nextCommitOffset    = prop.getValue(
        shortRpcFormatFlag ? "CN" :  "Next-commit-offset", (int64_t)-1);
    widReadOnlyFlag     = prop.getValue(
        shortRpcFormatFlag ? "WR" : "Wid-read-only", 0) != 0;
}

///
/// Handlers to parse a response sent by the server.  The model is
/// similar to what is done by ChunkServer/metaserver: put the
/// response into a properties object and then extract out the values.
///
/// \brief Parse the response common to all RPC requests.
/// @param[in] resp: a string consisting of header/value pairs in
/// which header/value is separated by a ':' character.
/// @param[out] prop: a properties object that contains the result of
/// parsing.
///
void
KfsOp::ParseResponseHeader(istream& is)
{
    const char separator = ':';
    Properties prop;
    prop.loadProperties(is, separator);
    ParseResponseHeader(prop);
}

void
KfsOp::ParseResponseHeader(const Properties& prop)
{
    // kfsSeq_t resSeq = prop.getValue("Cseq", (kfsSeq_t) -1);
    status = prop.getValue(shortRpcFormatFlag ? "s" : "Status", -1);
    if (status < 0) {
        status = -KfsToSysErrno(-status);
    }
    contentLength = prop.getValue(
        shortRpcFormatFlag ? "l" : "Content-length", 0);
    statusMsg = prop.getValue(
        shortRpcFormatFlag ? "m" : "Status-message", string());
    ParseResponseHeaderSelf(prop);
}

///
/// Default parse response handler.
/// @param[in] buf: buffer containing the response
/// @param[in] len: str-len of the buffer.
void
KfsOp::ParseResponseHeaderSelf(const Properties& prop)
{
}

/* static */ void
KfsOp::AddDefaultRequestHeaders(
        bool     shortRpcFormatFlag,
        string&  headers,
        kfsUid_t euser  /* = kKfsUserNone */,
        kfsGid_t egroup /* = kKfsGroupNone */)
{
    headers += shortRpcFormatFlag ? "u:" : "UserId: ";
    if (euser == kKfsUserNone) {
        if (shortRpcFormatFlag) {
            AppendHexIntToString(headers, geteuid());
        } else {
            AppendDecIntToString(headers, geteuid());
        }
    } else {
        if (shortRpcFormatFlag) {
            AppendHexIntToString(headers, euser);
        } else {
            AppendDecIntToString(headers, euser);
        }
    }
    headers += "\r\n";
    headers += shortRpcFormatFlag ? "g:" : "GroupId: ";
    if (egroup == kKfsGroupNone) {
        if (shortRpcFormatFlag) {
            AppendHexIntToString(headers, getegid());
        } else {
            AppendDecIntToString(headers, getegid());
        }
    } else {
        if (shortRpcFormatFlag) {
            AppendHexIntToString(headers, egroup);
        } else {
            AppendDecIntToString(headers, egroup);
        }
    }
    headers += "\r\n";
}

///
/// Specific response parsing handlers.
///
void
CreateOp::ParseResponseHeaderSelf(const Properties& prop)
{
    fileId            = prop.getValue(
        shortRpcFormatFlag ? "P" : "File-handle", (kfsFileId_t) -1);
    metaStriperType   = prop.getValue(
        shortRpcFormatFlag ? "ST" : "Striper-type",
        int(KFS_STRIPED_FILE_TYPE_NONE));
    metaNumReplicas   = prop.getValue(
        shortRpcFormatFlag ? "R" : "Num-replicas", numReplicas);
    if (0 <= status) {
        permissions.user  = prop.getValue(
            shortRpcFormatFlag ? "u" : "User", permissions.user);
        permissions.group = prop.getValue(
            shortRpcFormatFlag ? "g" : "Group", permissions.group);
        permissions.mode  = prop.getValue(
            shortRpcFormatFlag ? "M" : "Mode", permissions.mode);
        userName          = prop.getValue(
            shortRpcFormatFlag ? "UN" : "UName", string());
        groupName         = prop.getValue(
            shortRpcFormatFlag ? "GN" : "GName",    string());
        minSTier          = prop.getValue(
            shortRpcFormatFlag ? "TL" : "Min-tier", minSTier);
        maxSTier          = prop.getValue(
            shortRpcFormatFlag ? "TH" : "Max-tier", maxSTier);
    }
}

void
ReaddirOp::ParseResponseHeaderSelf(const Properties& prop)
{
    numEntries         = prop.getValue(
        shortRpcFormatFlag ? "EC" : "Num-Entries", 0);
    hasMoreEntriesFlag = prop.getValue(
        shortRpcFormatFlag ? "EM" : "Has-more-entries", 0) != 0;
}

void
DumpChunkServerMapOp::ParseResponseHeaderSelf(const Properties& prop)
{
}

void
DumpChunkMapOp::ParseResponseHeaderSelf(const Properties& prop)
{
}

void
UpServersOp::ParseResponseHeaderSelf(const Properties& prop)
{
}

void
ReaddirPlusOp::ParseResponseHeaderSelf(const Properties& prop)
{
    numEntries         = prop.getValue(
        shortRpcFormatFlag ? "EC" : "Num-Entries", 0);
    hasMoreEntriesFlag = prop.getValue(
        shortRpcFormatFlag ? "EM" : "Has-more-entries", 0) != 0;
}

void
MkdirOp::ParseResponseHeaderSelf(const Properties& prop)
{
    fileId = prop.getValue(
        shortRpcFormatFlag ? "P" : "File-handle", (kfsFileId_t) -1);
    if (0 <= status) {
        permissions.user  = prop.getValue(
            shortRpcFormatFlag ? "u" : "User",      permissions.user);
        permissions.group = prop.getValue(
            shortRpcFormatFlag ? "g" : "Group",     permissions.group);
        permissions.mode  = prop.getValue(
            shortRpcFormatFlag ? "M" : "Mode",      permissions.mode);
        userName          = prop.getValue(
            shortRpcFormatFlag ? "UN" : "UName",    string());
        groupName         = prop.getValue(
            shortRpcFormatFlag ? "GN" : "GName",    string());
        minSTier          = prop.getValue(
            shortRpcFormatFlag ? "TL" : "Min-tier", minSTier);
        maxSTier          = prop.getValue(
            shortRpcFormatFlag ? "TH" : "Max-tier", maxSTier);
    }
}

static void
ParseFileAttribute(bool shortRpcFormatFlag, const Properties& prop,
    FileAttr& fattr, string& outUserName, string& outGroupName)
{
    const string estr;

    fattr.fileId      =          prop.getValue(
        shortRpcFormatFlag ? "P" : "File-handle", kfsFileId_t(-1));
    fattr.isDirectory =          prop.getValue(
        shortRpcFormatFlag ? "T" : "Type", estr) == "dir";
    if (fattr.isDirectory) {
        fattr.subCount1 = prop.getValue(
            shortRpcFormatFlag ? "FC" : "File-count", int64_t(-1));
        fattr.subCount2 = prop.getValue(
            shortRpcFormatFlag ? "DC" : "Dir-count",  int64_t(-1));
    } else {
        fattr.subCount1 = prop.getValue(
            shortRpcFormatFlag ? "C"  : "Chunk-count",    int64_t(0));
        fattr.subCount2 = prop.getValue(
            shortRpcFormatFlag ? "NC" : "Next-chunk-pos", int64_t(-1));
    }
    fattr.fileSize    =          prop.getValue(
        shortRpcFormatFlag ? "S" : "File-size",   chunkOff_t(-1));
    fattr.numReplicas = (int16_t)prop.getValue(
        shortRpcFormatFlag ? "R" : "Replication", 1);

    GetTimeval(prop.getValue(
        shortRpcFormatFlag ? "MT" : "M-Time",  ""), fattr.mtime,
        shortRpcFormatFlag);
    GetTimeval(prop.getValue(
        shortRpcFormatFlag ? "CT" : "C-Time",  ""), fattr.ctime,
        shortRpcFormatFlag);
    GetTimeval(prop.getValue(
        shortRpcFormatFlag ? "CR" : "CR-Time", ""), fattr.crtime,
        shortRpcFormatFlag);

    const int type = prop.getValue(
        shortRpcFormatFlag ? "ST" : "Striper-type", int(KFS_STRIPED_FILE_TYPE_NONE));
    if (KFS_STRIPED_FILE_TYPE_NONE <= type && type < KFS_STRIPED_FILE_TYPE_COUNT) {
        fattr.striperType = StripedFileType(type);
    } else {
        fattr.striperType = KFS_STRIPED_FILE_TYPE_UNKNOWN;
    }

    fattr.numStripes         = (int16_t)prop.getValue(
        shortRpcFormatFlag ? "SN" : "Num-stripes",          0);
    fattr.numRecoveryStripes = (int16_t)prop.getValue(
        shortRpcFormatFlag ? "SR" : "Num-recovery-stripes", 0);
    fattr.stripeSize         =          prop.getValue(
        shortRpcFormatFlag ? "SS" : "Stripe-size",          int32_t(0));
    fattr.user               =          prop.getValue(
        shortRpcFormatFlag ? "U"  : "User",                 kKfsUserNone);
    fattr.group              =          prop.getValue(
        shortRpcFormatFlag ? "G"  : "Group",                kKfsGroupNone);
    fattr.mode               =          prop.getValue(
        shortRpcFormatFlag ? "M"  : "Mode",                 kKfsModeUndef);
    fattr.minSTier           =          prop.getValue(
        shortRpcFormatFlag ? "TL" : "Min-tier",             kKfsSTierMax);
    fattr.maxSTier           =          prop.getValue(
        shortRpcFormatFlag ? "TH" : "Max-tier",             kKfsSTierMax);

    outUserName  = prop.getValue(
        shortRpcFormatFlag ? "UN" : "UName", string());
    outGroupName = prop.getValue(
        shortRpcFormatFlag ? "GN" : "GName", string());
}

void
LookupOp::ParseResponseHeaderSelf(const Properties& prop)
{
    euser         = prop.getValue(
        shortRpcFormatFlag ? "EU"  : "EUserId",   euser);
    egroup        = prop.getValue(
        shortRpcFormatFlag ? "EG"  : "EGroupId",  kKfsGroupNone);
    authType      = prop.getValue(
        shortRpcFormatFlag ? "A"   : "Auth-type", int(kAuthenticationTypeUndef));
    euserName     = prop.getValue(
        shortRpcFormatFlag ? "EUN" : "EUName", string());
    egroupName    = prop.getValue(
        shortRpcFormatFlag ? "EGN" : "EGName", string());
    const char* vrPrimaryKey = shortRpcFormatFlag ? "VRP" : "Vr-primary";
    vrPrimaryFlag = prop.getValue(vrPrimaryKey, 0) != 0;
    responseHasVrPrimaryKeyFlag = vrPrimaryFlag ||
        0 != prop.getValue(vrPrimaryKey);
    ParseFileAttribute(shortRpcFormatFlag, prop, fattr, userName, groupName);
}

void
LookupPathOp::ParseResponseHeaderSelf(const Properties& prop)
{
    euser  = prop.getValue(
        shortRpcFormatFlag ? "EU" : "EUserId",  euser);
    egroup = prop.getValue(
        shortRpcFormatFlag ? "EG" : "EGroupId", kKfsGroupNone);
    ParseFileAttribute(shortRpcFormatFlag, prop, fattr, userName, groupName);
}

static inline bool
ParseChunkServerAccess(
    KfsOp&                    inOp,
    const Properties::String* csAccess,
    string&                   chunkServerAccessToken,
    CryptoKeys::Key&          chunkServerAccessKey,
    const char*               decryptKeyPtr = 0,
    int                       decryptKeyLen = 0)
{
    if (inOp.status < 0 || ! csAccess) {
        return false;
    }
    const char*       cur = csAccess->GetPtr();
    const char* const end = cur + csAccess->GetSize();
    while (cur < end && (*cur & 0xFF) <= ' ') {
        ++cur;
    }
    const char* const id = cur;
    while (cur < end && ' ' < (*cur & 0xFF)) {
        ++cur;
    }
    chunkServerAccessToken.assign(id, cur - id);
    if (chunkServerAccessToken.empty()) {
        inOp.statusMsg = "invalid chunk server access id";
        inOp.status    = -EINVAL;
        return false;
    }
    while (cur < end && (*cur & 0xFF) <= ' ') {
        ++cur;
    }
    const char* const key = cur;
    while (cur < end && ' ' < (*cur & 0xFF)) {
        ++cur;
    }
    if (decryptKeyPtr &&  0 < decryptKeyLen) {
        const int err = DelegationToken::DecryptSessionKeyFromString(
            decryptKeyPtr,
            decryptKeyLen,
            key,
            (int)(cur - key),
            chunkServerAccessKey,
            &inOp.statusMsg
        );
        if (err) {
            inOp.status = err < 0 ? err : -EINVAL;
            if (inOp.statusMsg.empty()) {
                inOp.statusMsg = "failed to decrypt key";
            }
            chunkServerAccessToken.clear();
        }
        return (err == 0);
    }
    if (chunkServerAccessKey.Parse(key, (int)(cur - key))) {
        return true;
    }
    chunkServerAccessToken.clear();
    inOp.statusMsg = "invalid chunk server access key";
    inOp.status    = -EINVAL;
    return false;
}

void
AllocateOp::ParseResponseHeaderSelf(const Properties& prop)
{
    chunkId      = prop.getValue(
        shortRpcFormatFlag ? "H" : "Chunk-handle",  (kfsFileId_t) -1);
    chunkVersion = prop.getValue(
        shortRpcFormatFlag ? "V" : "Chunk-version", (int64_t)-1);
    if (append) {
        fileOffset = prop.getValue(
            shortRpcFormatFlag ? "O" : "Chunk-offset", (chunkOff_t) 0);
    }
    chunkServers.clear();
    if (! shortRpcFormatFlag) {
        const Properties::String* master = prop.getValue("Master");
        ServerLocation loc;
        if (master && loc.FromString(
                master->data(), master->size(), shortRpcFormatFlag) &&
                loc.IsValid()) {
            chunkServers.push_back(loc);
        }
    }
    allCSShortRpcFlag = shortRpcFormatFlag && prop.getValue("SS", 0) != 0;
    const int numReplicas = prop.getValue(
        shortRpcFormatFlag ? "R" : "Num-replicas", 0);
    if (0 < numReplicas) {
        const Properties::String* const replicas = prop.getValue(
            shortRpcFormatFlag ? "S" : "Replicas");
        const bool noMasterFlag = chunkServers.empty();
        if (replicas) {
            chunkServers.reserve(numReplicas);
            const char*       ptr = replicas->data();
            const char* const end = ptr + replicas->size();
            for (int i = 0; i < numReplicas; ++i) {
                ServerLocation loc;
                if (! loc.ParseString(ptr, end - ptr, shortRpcFormatFlag) ||
                        ! loc.IsValid()) {
                    status    = -EINVAL;
                    statusMsg = "response replica location parse error: ";
                    statusMsg.append(replicas->data(), replicas->size());
                    break;
                }
                if (noMasterFlag || chunkServers.front() != loc) {
                    chunkServers.push_back(loc);
                }
            }
        }
    }
    chunkServerAccessValidForTime = 0;
    chunkServerAccessIssuedTime   = 0;
    allowCSClearTextFlag          = false;
    chunkServerAccessToken.clear();
    chunkAccess.clear();
    if (status < 0) {
        return;
    }
    chunkLeaseDuration = prop.getValue(
        shortRpcFormatFlag ? "LD" : "Lease-duration", int64_t(-1));
    if (ParseChunkServerAccess(*this, prop.getValue(
            shortRpcFormatFlag ? "SA" : "CS-access"),
            chunkServerAccessToken, chunkServerAccessKey)) {
        chunkServerAccessValidForTime = prop.getValue(
            shortRpcFormatFlag ? "ST" : "CS-acess-time",   int64_t(0));
        chunkServerAccessIssuedTime   = prop.getValue(
            shortRpcFormatFlag ? "SI" : "CS-acess-issued", int64_t(0));
        allowCSClearTextFlag = prop.getValue(
            shortRpcFormatFlag ? "CT" : "CS-clear-text", 0) != 0;
    }
    chunkAccess = prop.getValue(
        shortRpcFormatFlag ? "C" : "C-access", string());
}

void
GetAllocOp::ParseResponseHeaderSelf(const Properties& prop)
{
    chunkId = prop.getValue(
        shortRpcFormatFlag ? "H" : "Chunk-handle", (kfsFileId_t) -1);
    chunkVersion = prop.getValue(
        shortRpcFormatFlag ? "V" : "Chunk-version", (int64_t) -1);
    serversOrderedFlag = prop.getValue(
        shortRpcFormatFlag ? "O" : "Replicas-ordered", 0) != 0;
    const int numReplicas = prop.getValue(
        shortRpcFormatFlag ? "R" : "Num-replicas", 0);
    allCSShortRpcFlag = shortRpcFormatFlag && prop.getValue("SS", 0) != 0;
    chunkServers.clear();
    if (0 < numReplicas) {
        const Properties::String* const replicas = prop.getValue(
            shortRpcFormatFlag ? "S" : "Replicas");
        if (replicas) {
            chunkServers.reserve(numReplicas);
            const char*       ptr = replicas->data();
            const char* const end = ptr + replicas->size();
            for (int i = 0; i < numReplicas; ++i) {
                ServerLocation loc;
                if (! loc.ParseString(ptr, end - ptr, shortRpcFormatFlag) ||
                        ! loc.IsValid()) {
                    status    = -EINVAL;
                    statusMsg = "response replica location parse error: ";
                    statusMsg.append(replicas->data(), replicas->size());
                    break;
                }
                chunkServers.push_back(loc);
            }
        }
    }
}

void
ChunkAccessOp::ParseResponseHeaderSelf(const Properties& prop)
{
    accessResponseIssued      = prop.getValue(
        shortRpcFormatFlag ? "SI" : "Acess-issued", int64_t(0));
    accessResponseValidForSec = prop.getValue(
        shortRpcFormatFlag ? "ST" : "Acess-time",   int64_t(0));
    chunkAccessResponse       = prop.getValue(
        shortRpcFormatFlag ? "C"  : "C-access",     string());
    chunkServerAccessId.clear();
    ParseChunkServerAccess(
        *this,
        prop.getValue(shortRpcFormatFlag ? "SA" : "CS-access"),
        chunkServerAccessId,
        chunkServerAccessKey,
        decryptKey ? decryptKey->data()      : 0,
        decryptKey ? (int)decryptKey->size() : 0
    );
}

void
CoalesceBlocksOp::ParseResponseHeaderSelf(const Properties& prop)
{
    dstStartOffset = prop.getValue(
        shortRpcFormatFlag ? "O" : "Dst-start-offset", (chunkOff_t) 0);
}

void
GetLayoutOp::ParseResponseHeaderSelf(const Properties& prop)
{
    numChunks         = prop.getValue(
        shortRpcFormatFlag ? "C"  : "Num-chunks", 0);
    hasMoreChunksFlag = prop.getValue(
        shortRpcFormatFlag ? "MC" : "Has-more-chunks", 0) != 0;
    fileSize          = prop.getValue(
        shortRpcFormatFlag ? "S"  : "File-size", chunkOff_t(-1));
    allCSShortRpcFlag = shortRpcFormatFlag && prop.getValue("SS", 0) != 0;
}

int
GetLayoutOp::ParseLayoutInfo(bool clearFlag)
{
    if (clearFlag) {
        chunks.clear();
        chunks.reserve(numChunks);
    }
    if (numChunks <= 0 || ! contentBuf) {
        return 0;
    }
    BufferInputStream is(contentBuf, contentLength);
    if (shortRpcFormatFlag) {
        is >> hex;
    }
    for (int i = 0; i < numChunks; ++i) {
        chunks.push_back(ChunkLayoutInfo());
        if (! (is >> chunks.back())) {
            chunks.clear();
            return -EINVAL;
        }
    }
    return 0;
}

istream&
ChunkLayoutInfo::Parse(istream& is)
{
    chunkServers.clear();
    if (! (is >> fileOffset >> chunkId >> chunkVersion)) {
        return is;
    }
    int numServers = 0;
    if (! (is >> numServers)) {
        return is;
    }
    chunkServers.reserve(max(0, numServers));
    for (int j = 0; j < numServers; j++) {
        chunkServers.push_back(ServerLocation());
        ServerLocation& s = chunkServers.back();
        if (! (is >> s.hostname >> s.port)) {
            return is;
        }
    }
    return is;
}

void
SizeOp::ParseResponseHeaderSelf(const Properties& prop)
{
    size = prop.getValue(shortRpcFormatFlag ? "S" : "Size", (long long) 0);
}

void
ReadOp::ParseResponseHeaderSelf(const Properties& prop)
{
    const int nentries = prop.getValue(
        shortRpcFormatFlag ? "KC" : "Checksum-entries", 0);
    if (shortRpcFormatFlag) {
        diskIOTime = prop.getValue("D", int64_t(0)) * 1e-6;
    } else {
        diskIOTime = prop.getValue("DiskIOtime", 0.0);
    }
    skipVerifyDiskChecksumFlag =
        skipVerifyDiskChecksumFlag && prop.getValue(
            shortRpcFormatFlag ? "KS" : "Skip-Disk-Chksum", 0) != 0;
    checksums.clear();
    if (0 < nentries) {
        const Properties::String* const checksumStr = prop.getValue(
            shortRpcFormatFlag ? "K" : "Checksums");
        const char*       ptr = checksumStr->data();
        const char* const end = ptr + checksumStr->size();
        if (checksumStr) {
            checksums.reserve(nentries);
            for (int i = 0; i < nentries; i++) {
                uint32_t cksum = 0;
                if (! (shortRpcFormatFlag ?
                        HexIntParser::Parse(ptr, end - ptr, cksum) :
                        DecIntParser::Parse(ptr, end - ptr, cksum))) {
                    status    = -EINVAL;
                    statusMsg = "response checksum parse error";
                    break;
                }
                checksums.push_back(cksum);
            }
        }
    }
}

void
WriteIdAllocOp::ParseResponseHeaderSelf(const Properties& prop)
{
    ChunkAccessOp::ParseResponseHeaderSelf(prop);
    writeIdStr                  = prop.getValue(
        shortRpcFormatFlag ? "W" : "Write-id", string());
    writePrepReplySupportedFlag = prop.getValue(
        shortRpcFormatFlag ? "WR" : "Write-prepare-reply", 0) != 0;
}

void
LeaseAcquireOp::ParseResponseHeaderSelf(const Properties& prop)
{
    leaseId = prop.getValue(
        shortRpcFormatFlag ? "L" : "Lease-id", int64_t(-1));
    if (leaseIds) {
        leaseIds[0] = -1;
    }
    chunkAccessCount              = prop.getValue(
        shortRpcFormatFlag ? "SA" : "CS-access",       0);
    chunkServerAccessValidForTime = prop.getValue(
        shortRpcFormatFlag ? "ST" : "CS-acess-time",   0);
    chunkServerAccessIssuedTime   = prop.getValue(
        shortRpcFormatFlag ? "SI" : "CS-acess-issued", 0);
    const Properties::String* const v = prop.getValue(
        shortRpcFormatFlag ? "LS" : "Lease-ids");
    allowCSClearTextFlag          = prop.getValue(
        shortRpcFormatFlag ? "CT" : "CS-clear-text",
        (0 <= status && (0 <= leaseId || (v && ! v->empty())) &&
            chunkAccessCount <= 0 && chunkServerAccessValidForTime <= 0
        ) ? 1 : 0
    ) != 0;
    if (! chunkIds || ! leaseIds) {
        return;
    }
    if (! v) {
        return;
    }
    const char*       p = v->GetPtr();
    const char* const e = p + v->GetSize();
    for (int i = 0; p < e && i < kMaxChunkIds && chunkIds[i] >= 0; i++) {
        if (! (shortRpcFormatFlag ?
                HexIntParser::Parse(p, e - p, leaseIds[i]) :
                DecIntParser::Parse(p, e - p, leaseIds[i]))) {
            status      = -EINVAL;
            statusMsg   = "response parse error";
            leaseIds[0] = -1;
            return;
        }
    }
}

void
LeaseRenewOp::ParseResponseHeaderSelf(const Properties& prop)
{
    chunkAccessCount              = prop.getValue(
        shortRpcFormatFlag ? "C"  : "C-access", 0);
    chunkServerAccessValidForTime = prop.getValue(
        shortRpcFormatFlag ? "ST" : "CS-acess-time",   0);
    chunkServerAccessIssuedTime   = prop.getValue(
        shortRpcFormatFlag ? "SI" : "CS-acess-issued", 0);
    allowCSClearTextFlag          = prop.getValue(
        shortRpcFormatFlag ? "CT" : "CS-clear-text", 0) != 0;
}

void
ChangeFileReplicationOp::ParseResponseHeaderSelf(const Properties& prop)
{
    numReplicas = prop.getValue(
        shortRpcFormatFlag ? "R" : "Num-replicas", 1);
}

void
GetPathNameOp::Request(ReqOstream& os)
{
    os << "GETPATHNAME\r\n" << ReqHeaders(*this);
    if (fid > 0) {
        os << (shortRpcFormatFlag ? "P:" : "File-handle: ") << fid << "\r\n";
    }
    if (chunkId > 0) {
        os << (shortRpcFormatFlag ? "H:" : "Chunk-handle: ") <<
            chunkId << "\r\n";
    }
    os << "\r\n";
}

void
GetPathNameOp::ParseResponseHeaderSelf(const Properties& prop)
{
    ParseFileAttribute(shortRpcFormatFlag, prop, fattr, userName, groupName);
    pathname     = prop.getValue(
        shortRpcFormatFlag ? "N" :  "Path-name", string());
    offset       = prop.getValue(
        shortRpcFormatFlag ? "O" :  "Chunk-offset", chunkOff_t(-1));
    chunkVersion = prop.getValue(
        shortRpcFormatFlag ? "V" :  "Chunk-version", int64_t(-1));
    const int numReplicas = prop.getValue(
        shortRpcFormatFlag ? "NR" : "Num-replicas", 0);
    servers.clear();
    if (0 < numReplicas) {
        const Properties::String* const replicas = prop.getValue(
            shortRpcFormatFlag ? "SL" : "Replicas");
        if (replicas) {
            const char*       ptr = replicas->data();
            const char* const end = ptr + replicas->size();
            for (int i = 0; i < numReplicas; ++i) {
                ServerLocation loc;
                if (! loc.ParseString(ptr, end - ptr, shortRpcFormatFlag) ||
                        ! loc.IsValid()) {
                    status    = -EINVAL;
                    statusMsg = "response replica location parse error: ";
                    statusMsg.append(replicas->data(), replicas->size());
                    break;
                }
                servers.push_back(loc);
            }
        }
    }
}

void
ChmodOp::Request(ReqOstream& os)
{
    os <<
    "CHMOD\r\n" << ReqHeaders(*this) <<
    (shortRpcFormatFlag ? "P:" : "File-handle: ") << fid  << "\r\n" <<
    (shortRpcFormatFlag ? "M:" : "Mode: ")        << mode << "\r\n"
    "\r\n";
}

void
ChownOp::Request(ReqOstream& os)
{
    os <<
        "CHOWN\r\n" << ReqHeaders(*this) <<
        (shortRpcFormatFlag ? "P:" : "File-handle: ") << fid << "\r\n";
    if (user != kKfsUserNone) {
        os << (shortRpcFormatFlag ? "O:" : "Owner: ") << user << "\r\n";
    }
    if (group != kKfsGroupNone) {
        os << (shortRpcFormatFlag ? "G:" : "Group: ") << group << "\r\n";
    }
    if (! userName.empty()) {
        os << (shortRpcFormatFlag ? "ON:" : "OName: ") << userName << "\r\n";
    }
    if (! groupName.empty()) {
        os << (shortRpcFormatFlag ? "GN:" : "GName: ") << groupName << "\r\n";
    }
    os << "\r\n";
}

void
ChownOp::ParseResponseHeaderSelf(const Properties& prop)
{
    if (0 <= status) {
        user      = prop.getValue(
            shortRpcFormatFlag ? "U" : "User",  user);
        group     = prop.getValue(
            shortRpcFormatFlag ? "G" : "Group", group);
        userName  = prop.getValue(
            shortRpcFormatFlag ? "UN" : "UName", userName);
        groupName = prop.getValue(
            shortRpcFormatFlag ? "GN" : "GName", groupName);
    }
}

void
TruncateOp::ParseResponseHeaderSelf(const Properties& prop)
{
    respEndOffset = prop.getValue(
        shortRpcFormatFlag ? "O" : "End-offset", int64_t(-1));
    if (status == 0 && ! pruneBlksFromHead &&
            endOffset >= 0 && respEndOffset != endOffset) {
        status    = -EFAULT;
        statusMsg = "range truncate is not supported";
    }
}

void
AuthenticateOp::Request(ReqOstream& os)
{
    os <<
        "AUTHENTICATE\r\n" << ReqHeaders(*this) <<
        (shortRpcFormatFlag ? "A:" : "Auth-type: ") << requestedAuthType << "\r\n"
    ;
    if (0 < contentLength) {
        os << (shortRpcFormatFlag ? "l:" : "Content-length: ") <<
            contentLength << "\r\n";
    }
    if (reqShortRpcFormatFlag && ! shortRpcFormatFlag) {
        os << "Short-rpc-fmt: 1\r\n";
    }
    if (0 <= rackId) {
        os << (shortRpcFormatFlag ? "R:" : "Rack-id: ") << rackId << "\r\n";
    }
    if (! clientLocation.hostname.empty()) {
        os << (shortRpcFormatFlag ? "C:" : "Client-ip: ") <<
            clientLocation.hostname << "\r\n";
    }
    if (0 <= clientLocation.port) {
        os << (shortRpcFormatFlag ? "CP:" : "Client-port: ") <<
            clientLocation.port << "\r\n";
    }
    os << "\r\n";
}

void
AuthenticateOp::ParseResponseHeaderSelf(const Properties& prop)
{
    chosenAuthType = prop.getValue(
        shortRpcFormatFlag ? "A" : "Auth-type", int(kAuthenticationTypeUndef));
    useSslFlag     = prop.getValue(
        shortRpcFormatFlag ? "US" : "Use-ssl", 0) != 0;
    currentTime    = prop.getValue(
        shortRpcFormatFlag ? "CT" : "Curtime", int64_t(-1));
    sessionEndTime = prop.getValue(
        shortRpcFormatFlag? "ET" : "Endtime", int64_t(-1));
}

void
DelegateOp::Request(ReqOstream& os)
{
    os <<
        "DELEGATE\r\n" << ReqHeaders(*this);
    if (0 < requestedValidForTime) {
        os << (shortRpcFormatFlag ? "V:" : "Valid-for-time: ") <<
            requestedValidForTime << "\r\n";
    }
    if (allowDelegationFlag) {
        os << (shortRpcFormatFlag ? "D:1\r\n" : "Allow-delegation: 1\r\n");
    }
    if (! renewTokenStr.empty()) {
        os << (shortRpcFormatFlag ? "T:" : "Renew-token: ") <<
            renewTokenStr << "\r\n";
    }
    if (! renewKeyStr.empty()) {
        os << (shortRpcFormatFlag ? "K:" : "Renew-key: ") <<
            renewKeyStr << "\r\n";
    }
    os << "\r\n";
}

void
DelegateOp::ParseResponseHeaderSelf(const Properties& prop)
{
    issuedTime        = prop.getValue(
        shortRpcFormatFlag ? "TI" : "Issued-time",          int64_t(0));
    validForTime      = prop.getValue(
        shortRpcFormatFlag ? "TV" : "Valid-for-time",       uint32_t(0));
    tokenValidForTime = prop.getValue(
        shortRpcFormatFlag ? "TT" : "Token-valid-for-time", validForTime);
    access            = prop.getValue(
        shortRpcFormatFlag ? "A"  : "Access",               string());
}

void
DelegateCancelOp::Request(ReqOstream& os)
{
    os <<
        "DELEGATE_CANCEL\r\n" << ReqHeaders(*this);
    if (! tokenStr.empty()) {
        os << (shortRpcFormatFlag ? "T:" : "Token: ") << tokenStr << "\r\n";
    }
    if (! keyStr.empty()) {
        os << (shortRpcFormatFlag ? "K:" : "Key: ") << keyStr << "\r\n";
    }
    os << "\r\n";
}

void
MetaPingOp::Request(ReqOstream& os)
{
    os <<
    "PING\r\n" << ReqHeaders(*this) <<
    "\r\n"
    ;
}

void
MetaToggleWORMOp::Request(ReqOstream& os)
{
    os <<
    "TOGGLE_WORM\r\n" << ReqHeaders(*this) <<
    (shortRpcFormatFlag ? "T:" : "Toggle-WORM: ") << value << "\r\n"
    "\r\n"
    ;
}

void
MetaToggleWORMOp::ParseResponseHeaderSelf(const Properties&)
{
}

void
MetaPingOp::ParseResponseHeaderSelf(const Properties& prop)
{
    const char delim = '\t';
    string serv  = prop.getValue("Servers", "");
    size_t start = serv.find_first_of("s=");
    if (start == string::npos) {
        return;
    }

    string serverInfo;
    size_t end;
    while (start != string::npos) {
        end = serv.find_first_of(delim, start);
        if (end != string::npos) {
            serverInfo.assign(serv, start, end - start);
        } else {
            serverInfo.assign(serv, start, serv.size() - start);
        }
        upServers.push_back(serverInfo);
        start = serv.find_first_of("s=", end);
    }
    serv = prop.getValue("Down Servers", "");
    start = serv.find_first_of("s=");
    if (start == string::npos) {
        return;
    }
    while (start != string::npos) {
        end = serv.find_first_of(delim, start);
        if (end != string::npos) {
            serverInfo.assign(serv, start, end - start);
        } else {
            serverInfo.assign(serv, start, serv.size() - start);
        }
        downServers.push_back(serverInfo);
        start = serv.find_first_of("s=", end);
    }
}

void
ChunkPingOp::Request(ReqOstream& os)
{
    os <<
    "PING\r\n" << ReqHeaders(*this) <<
    "\r\n"
    ;
}

void
ChunkPingOp::ParseResponseHeaderSelf(const Properties& prop)
{
    location.hostname = prop.getValue("Meta-server-host",   string());
    location.port     = prop.getValue("Meta-server-port",          0);
    totalSpace        = prop.getValue("Total-space",      int64_t(0));
    usedSpace         = prop.getValue("Used-space",       int64_t(0));
}

void
MetaStatsOp::Request(ReqOstream& os)
{
    os <<
    "STATS\r\n" << ReqHeaders(*this) <<
    "\r\n"
    ;
}

void
ChunkStatsOp::Request(ReqOstream& os)
{
    os <<
    "STATS\r\n" << ReqHeaders(*this) <<
    "\r\n"
    ;
}

void
MetaStatsOp::ParseResponseHeaderSelf(const Properties& prop)
{
    stats = prop;
    stats.setIntBase(10);
}

void
ChunkStatsOp::ParseResponseHeaderSelf(const Properties& prop)
{
    stats = prop;
    stats.setIntBase(10);
}

void
RetireChunkserverOp::Request(ReqOstream& os)
{
    os <<
    "RETIRE_CHUNKSERVER\r\n" << ReqHeaders(*this) <<
    (shortRpcFormatFlag ? "D:" : "Downtime: ")
        << downtime << "\r\n" <<
    (shortRpcFormatFlag ? "H:" : "Chunk-server-name: ")
        << chunkLoc.hostname << "\r\n" <<
    (shortRpcFormatFlag ? "P:" : "Chunk-server-port: ")
        << chunkLoc.port     << "\r\n"
    "\r\n"
    ;
}

void
RetireChunkserverOp::ParseResponseHeaderSelf(const Properties& /* prop */)
{
}

void
FsckOp::Request(ReqOstream& os)
{
    os <<
    "FSCK\r\n" << ReqHeaders(*this) <<
    (shortRpcFormatFlag ? "A:" : "Report-Abandoned-Files: ") <<
        (reportAbandonedFilesFlag ? 1 : 0) << "\r\n"
    "\r\n"
    ;
}

void
FsckOp::ParseResponseHeaderSelf(const Properties& /* prop */)
{
}

void
MetaReadMetaData::Request(ReqOstream& os)
{
    os <<
    "READ_META_DATA\r\n" << ReqHeaders(*this) <<
    (shortRpcFormatFlag ? "FI:" : "FsId: ")       << fileSystemId << "\r\n" <<
    (shortRpcFormatFlag ? "L:"  : "Start-log: ")  << startLogSeq << "\r\n" <<
    (shortRpcFormatFlag ? "C:"  : "Checkpoint: ") <<
        (checkpointFlag ? 1 : 0) << "\r\n" <<
    (shortRpcFormatFlag ? "S:"  : "Read-size: ")  << readSize << "\r\n" <<
    (shortRpcFormatFlag ? "O:"  : "Read-pos: ")   << readPos << "\r\n" <<
    (shortRpcFormatFlag ? "NP:" : "Not-prm-ok: ") <<
        (allowNotPrimaryFlag ? 1 : 0) << "\r\n"
    "\r\n"
    ;
}

void
MetaReadMetaData::ParseResponseHeaderSelf(const Properties& prop)
{
    startLogSeq  = prop.parseValue(
        shortRpcFormatFlag ? "L" :  "Start-log", MetaVrLogSeq());
    checksum     = prop.getValue(
        shortRpcFormatFlag ? "K" :  "Crc32",     (uint32_t)0);
    fileSystemId = prop.getValue(
        shortRpcFormatFlag ? "FI" : "FsId",      (int64_t)-1);
    fileSize     = prop.getValue(
        shortRpcFormatFlag ?  "S" : "Size",      (int64_t)-1);
    fileName     = prop.getValue(
        shortRpcFormatFlag ?  "N" : "Name",      string());
    endLogSeq    = prop.parseValue(
        shortRpcFormatFlag ?  "E" : "End-log",   MetaVrLogSeq());
    maxReadSize  = prop.getValue(
        shortRpcFormatFlag ? "M" :  "Max-rd-size",  -1);
    clusterKey   = prop.getValue(
        shortRpcFormatFlag ? "CK" : "Cluster-key", string());
    metaMd       = prop.getValue(
        shortRpcFormatFlag ? "MM" : "Meta-md",     string());
}

void
MetaMonOp::Request(ReqOstream& os)
{
    os << verb << "\r\n" << ReqHeaders(*this);
    for (Properties::iterator it = requestProps.begin();
            it != requestProps.end();
            ++it) {
        os << it->first << ": " << it->second << "\r\n";
    }
    os << "\r\n";
}

void
MetaMonOp::ParseResponseHeaderSelf(const Properties& prop)
{
    responseProps = prop;
}

} //namespace client
} //namespace KFS
