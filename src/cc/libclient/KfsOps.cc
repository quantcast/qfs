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
// Client side RPCs implementation.
//
//----------------------------------------------------------------------------

#include "KfsOps.h"

#include <cassert>
#include <iostream>
#include <algorithm>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "kfsio/checksum.h"
#include "common/RequestParser.h"
#include "common/kfserrno.h"
#include "utils.h"

namespace KFS
{
namespace client
{
using std::istringstream;
using std::ostream;
using std::istream;
using std::string;
using std::min;
using std::max;

static const char* InitHostName()
{
    const int   maxLen = 1024;
    static char sHostName[maxLen + 1];
    sHostName[gethostname(sHostName, maxLen) ? 0 : min(64, maxLen)] = 0;
    return sHostName;
}
static const char* const sHostName(InitHostName());

string KfsOp::sExtraHeaders;

class KfsOp::ReqHeaders
{
public:
    ReqHeaders(const KfsOp& o)
        : op(o)
        {}
    ostream& Insert(ostream& os) const
    {
        return (os <<
            "Cseq: "                    << op.seq                << "\r\n"
            "Version: "                    "KFS/1.0"                "\r\n"
            "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n"
            << KfsOp::sExtraHeaders
        );
    }
private:
    const KfsOp& op;
};

inline ostream& operator<<(ostream& os, const KfsOp::ReqHeaders& hdrs) {
    return hdrs.Insert(os);
}

inline ostream& PutPermissions(ostream& os, const Permissions& permissions)
{
    if (permissions.user != kKfsUserNone) {
        os << "Owner: " << permissions.user << "\r\n";
    }
    if (permissions.group != kKfsGroupNone) {
        os << "Group: " << permissions.group << "\r\n";
    }
    if (permissions.mode != kKfsModeUndef) {
        os << "Mode: "  << permissions.mode << "\r\n";
    }
    return os;
}

///
/// All Request() methods build a request RPC based on the KFS
/// protocol and output the request into a ostream.
/// @param[out] os which contains the request RPC.
///
void
CreateOp::Request(ostream &os)
{
    os <<
        "CREATE \r\n"               << ReqHeaders(*this)     <<
        "Parent File-handle: "      << parentFid             << "\r\n"
        "Filename: "                << filename              << "\r\n"
        "Num-replicas: "            << numReplicas           << "\r\n"
        "Exclusive: "               << (exclusive ? 1 : 0)   << "\r\n"
    ;
    if (striperType != KFS_STRIPED_FILE_TYPE_NONE &&
            striperType != KFS_STRIPED_FILE_TYPE_UNKNOWN) {
        os <<
            "Striper-type: "         << striperType        << "\r\n"
            "Num-stripes: "          << numStripes         << "\r\n"
            "Num-recovery-stripes: " << numRecoveryStripes << "\r\n"
            "Stripe-size: "          << stripeSize         << "\r\n"
        ;
    }
    PutPermissions(os, permissions);
    if (reqId >= 0) {
        os << "ReqId: " << reqId << "\r\n";
    }
    os << "\r\n";
}

void
MkdirOp::Request(ostream &os)
{
    os <<
        "MKDIR \r\n"           << ReqHeaders(*this) <<
        "Parent File-handle: " << parentFid         << "\r\n"
        "Directory: "          << dirname           << "\r\n"
    ;
    PutPermissions(os, permissions);
    if (reqId >= 0) {
        os << "ReqId: " << reqId << "\r\n";
    }
    os << "\r\n";
}

void
RmdirOp::Request(ostream &os)
{
    os <<
        "RMDIR \r\n"           << ReqHeaders(*this) <<
        "Parent File-handle: " << parentFid         << "\r\n"
        "Pathname: "           << pathname          << "\r\n"
        "Directory: "          << dirname           << "\r\n"
    "\r\n";
}

void
RenameOp::Request(ostream &os)
{
    os <<
        "RENAME \r\n"          << ReqHeaders(*this)   <<
        "Parent File-handle: " << parentFid           << "\r\n"
        "Old-name: "           << oldname             << "\r\n"
        "New-path: "           << newpath             << "\r\n"
        "Old-path: "           << oldpath             << "\r\n"
        "Overwrite: "          << (overwrite ? 1 : 0) << "\r\n"
    "\r\n";
}

void
ReaddirOp::Request(ostream &os)
{
    os <<
        "READDIR \r\n"            << ReqHeaders(*this) <<
        "Directory File-handle: " << fid               << "\r\n"
        "Max-entries: "           << numEntries        << "\r\n"
    ;
    if (! fnameStart.empty()) {
        os << "Fname-start: " << fnameStart << "\r\n";
    }
    os << "\r\n";
}

void
SetMtimeOp::Request(ostream &os)
{
    os <<
        "SET_MTIME\r\n"  << ReqHeaders(*this) <<
        "Pathname: "     << pathname          << "\r\n"
        "Mtime-sec: "    << mtime.tv_sec      << "\r\n"
        "Mtime-usec: "   << mtime.tv_usec     << "\r\n"
    "\r\n";
}

void
DumpChunkServerMapOp::Request(ostream &os)
{
    os <<
        "DUMP_CHUNKTOSERVERMAP\r\n" << ReqHeaders(*this) <<
    "\r\n";
}

void
DumpChunkMapOp::Request(ostream &os)
{
    os <<
        "DUMP_CHUNKMAP\r\n" << ReqHeaders(*this) <<
    "\r\n";
}

void
UpServersOp::Request(ostream &os)
{
    os <<
        "UPSERVERS\r\n" << ReqHeaders(*this) <<
    "\r\n";
}

void
ReaddirPlusOp::Request(ostream &os)
{
    os <<
        "READDIRPLUS \r\n"        << ReqHeaders(*this)  <<
        "Directory File-handle: " << fid                << "\r\n"
        "GetLastChunkInfoOnlyIfSizeUnknown: " <<
            (getLastChunkInfoOnlyIfSizeUnknown ? 1 : 0) << "\r\n"
        "Max-entries: " << numEntries << "\r\n"
    ;
    if (! fnameStart.empty()) {
        os << "Fname-start: " << fnameStart << "\r\n";
    }
    os << "\r\n";
}

void
RemoveOp::Request(ostream &os)
{
    os <<
        "REMOVE \r\n"          << ReqHeaders(*this) <<
        "Pathname: "           << pathname          << "\r\n"
        "Parent File-handle: " << parentFid         << "\r\n"
        "Filename: "           << filename          << "\r\n"
    "\r\n";
}

void
LookupOp::Request(ostream &os)
{
    os <<
        "LOOKUP\r\n"           << ReqHeaders(*this) <<
        "Parent File-handle: " << parentFid         << "\r\n"
        "Filename: "           << filename          << "\r\n"
    "\r\n";
}

void
LookupPathOp::Request(ostream &os)
{
    os <<
        "LOOKUP_PATH\r\n"    << ReqHeaders(*this) <<
        "Root File-handle: " << rootFid           << "\r\n"
        "Pathname: "         << filename          << "\r\n"
    "\r\n";
}

void
GetAllocOp::Request(ostream &os)
{
    assert(fileOffset >= 0);

    os <<
        "GETALLOC\r\n"   << ReqHeaders(*this) <<
        "Pathname: "     << filename          << "\r\n"
        "File-handle: "  << fid               << "\r\n"
        "Chunk-offset: " << fileOffset        << "\r\n"
    "\r\n";
}

void
GetLayoutOp::Request(ostream &os)
{
    os <<
        "GETLAYOUT\r\n" << ReqHeaders(*this) <<
        "File-handle: " << fid               << "\r\n"
    ;
    if (startOffset > 0) {
        os << "Start-offset: " << startOffset << "\r\n";
    }
    if (omitLocationsFlag) {
        os << "Omit-locations: 1\r\n";
    }
    if (lastChunkOnlyFlag) {
        os << "Last-chunk-only: 1\r\n";
    }
    if (maxChunks > 0) {
        os << "Max-chunks : " << maxChunks << "\r\n";
    }
    os << "\r\n";
}

void
CoalesceBlocksOp::Request(ostream &os)
{
    os <<
        "COALESCE_BLOCKS\r\n" << ReqHeaders(*this) <<
        "Src-path: "          << srcPath << "\r\n"
        "Dest-path: "         << dstPath << "\r\n"
    "\r\n";
}

void
GetChunkMetadataOp::Request(ostream &os)
{
    os <<
        "GET_CHUNK_METADATA\r\n" << ReqHeaders(*this)        <<
        "Chunk-handle: "         << chunkId                  << "\r\n"
        "Read-verify: "          << (readVerifyFlag ? 1 : 0) << "\r\n"
    "\r\n";
}

void
AllocateOp::Request(ostream &os)
{
    os <<
        "ALLOCATE\r\n"   << ReqHeaders(*this) <<
        "Client-host: "  << sHostName         << "\r\n"
        "Pathname: "     << pathname          << "\r\n"
        "File-handle: "  << fid               << "\r\n"
        "Chunk-offset: " << fileOffset        << "\r\n"
    ;
    if (invalidateAllFlag) {
        os << "Invalidate-all: 1\r\n";
    }
    if (append) {
        os <<
            "Chunk-append: 1\r\n"
            "Space-reserve: " << spaceReservationSize << "\r\n"
            "Max-appenders: " << maxAppendersPerChunk << "\r\n"
        ;
    }
    os << "\r\n";
}

void
TruncateOp::Request(ostream &os)
{
    os <<
        "TRUNCATE\r\n"  << ReqHeaders(*this) <<
        "Pathname: "    << pathname          << "\r\n"
        "File-handle: " << fid               << "\r\n"
        "Offset: "      << fileOffset        << "\r\n"
    ;
    if (pruneBlksFromHead) {
        os << "Prune-from-head: 1\r\n";
    }
    if (! setEofHintFlag) {
        // Default is true
        os << "Set-eof: 0\r\n";
    }
    if (endOffset >= 0) {
        os << "End-offset: " << endOffset << "\r\n";
    }
    os << "\r\n";
}

void
OpenOp::Request(ostream &os)
{
    os <<
        "OPEN\r\n"       << ReqHeaders(*this) <<
        "Chunk-handle: " << chunkId << "\r\n"
        "Intent: "       << (openFlags == O_RDONLY ? "READ" : "WRITE") << "\r\n"
    "\r\n";
}

void
CloseOp::Request(ostream &os)
{
    os <<
        "CLOSE\r\n"      << ReqHeaders(*this) <<
        "Chunk-handle: " << chunkId           << "\r\n"
    ;
    if (! writeInfo.empty()) {
        os <<
            "Has-write-id: 1\r\n"
            "Num-servers: "  << writeInfo.size() << "\r\n"
            "Servers:"
        ;
        for (vector<WriteInfo>::const_iterator i = writeInfo.begin();
                i < writeInfo.end(); ++i) {
            os << " " << i->serverLoc.ToString() << " " << i->writeId;
        }
        os << "\r\n";
    } else if (chunkServerLoc.size() > 1) {
        os <<
            "Num-servers: " << chunkServerLoc.size() << "\r\n"
            "Servers:"
        ;
        for (vector<ServerLocation>::const_iterator i = chunkServerLoc.begin();
                i != chunkServerLoc.end(); ++i) {
            os << " " << i->ToString();
        }
        os << "\r\n";
    }
    os << "\r\n";
}

void
ReadOp::Request(ostream &os)
{
    os <<
        "READ\r\n"        << ReqHeaders(*this) <<
        "Chunk-handle: "  << chunkId           << "\r\n"
        "Chunk-version: " << chunkVersion      << "\r\n"
        "Offset: "        << offset            << "\r\n"
        "Num-bytes: "     << numBytes          << "\r\n"
    "\r\n";
}

void
WriteIdAllocOp::Request(ostream &os)
{
    os <<
        "WRITE_ID_ALLOC\r\n"  << ReqHeaders(*this)           <<
        "Chunk-handle: "      << chunkId                     << "\r\n"
        "Chunk-version: "     << chunkVersion                << "\r\n"
        "Offset: "            << offset                      << "\r\n"
        "Num-bytes: "         << numBytes                    << "\r\n"
        "For-record-append: " << (isForRecordAppend ? 1 : 0) << "\r\n"
        "Num-servers: "       << chunkServerLoc.size()       << "\r\n"
        "Servers:"
    ;
    for (vector<ServerLocation>::size_type i = 0; i < chunkServerLoc.size(); ++i) {
        os << chunkServerLoc[i].ToString() << ' ';
    }
    os << "\r\n\r\n";
}

void
ChunkSpaceReserveOp::Request(ostream &os)
{
    os <<
        "CHUNK_SPACE_RESERVE\r\n" << ReqHeaders(*this) <<
        "Chunk-handle: "          << chunkId           << "\r\n"
        "Chunk-version: "         << chunkVersion      << "\r\n"
        "Num-bytes: "             << numBytes          << "\r\n"
        "Num-servers: "           << writeInfo.size()  << "\r\n"
        "Servers:"
    ;
    for (vector<WriteInfo>::size_type i = 0; i < writeInfo.size(); ++i) {
        os << writeInfo[i].serverLoc.ToString() <<
            ' ' << writeInfo[i].writeId << ' ';
    }
    os << "\r\n\r\n";
}

void
ChunkSpaceReleaseOp::Request(ostream &os)
{
    os <<
        "CHUNK_SPACE_RELEASE\r\n" << ReqHeaders(*this) <<
        "Chunk-handle: "          << chunkId           << "\r\n"
        "Chunk-version: "         << chunkVersion      << "\r\n"
        "Num-bytes: "             << numBytes          << "\r\n"
        "Num-servers: "           << writeInfo.size()  << "\r\n"
        "Servers:"
    ;
    for (vector<WriteInfo>::size_type i = 0; i < writeInfo.size(); ++i) {
        os << writeInfo[i].serverLoc.ToString() <<
            ' ' << writeInfo[i].writeId << ' ';
    }
    os << "\r\n\r\n";
}

void
WritePrepareOp::Request(ostream &os)
{
    // one checksum over the whole data plus one checksum per 64K block
    os <<
        "WRITE_PREPARE\r\n"  << ReqHeaders(*this) <<
        "Chunk-handle: "     << chunkId           << "\r\n"
        "Chunk-version: "    << chunkVersion      << "\r\n"
        "Offset: "           << offset            << "\r\n"
        "Num-bytes: "        << numBytes          << "\r\n"
        "Checksum: "         << checksum          << "\r\n"
        "Checksum-entries: " << checksums.size()  << "\r\n"
    ;
    if (checksums.size() > 0) {
        os << "Checksums: ";
        for (uint32_t i = 0; i < checksums.size(); i++) {
            os << checksums[i] << ' ';
        }
        os << "\r\n";
    }
    if (replyRequestedFlag) {
        os << "Reply: 1\r\n";
    }
    os <<
        "Num-servers: " << writeInfo.size() << "\r\n"
        "Servers:"
    ;
    for (vector<WriteInfo>::size_type i = 0; i < writeInfo.size(); ++i) {
        os << writeInfo[i].serverLoc.ToString() <<
            ' ' << writeInfo[i].writeId << ' ';
    }
    os << "\r\n\r\n";
}

void
WriteSyncOp::Request(ostream &os)
{
    os <<
        "WRITE_SYNC\r\n"     << ReqHeaders(*this) <<
        "Chunk-handle: "     << chunkId           << "\r\n"
        "Chunk-version: "    << chunkVersion      << "\r\n"
        "Offset: "           << offset            << "\r\n"
        "Num-bytes: "        << numBytes          << "\r\n"
        "Checksum-entries: " << checksums.size()  << "\r\n"
    ;
    if (checksums.size() > 0) {
        os << "Checksums: ";
        for (uint32_t i = 0; i < checksums.size(); i++) {
            os << checksums[i] << ' ';
        }
        os << "\r\n";
    }
    os <<
        "Num-servers: " << writeInfo.size() << "\r\n"
        "Servers:"
    ;
    for (vector<WriteInfo>::size_type i = 0; i < writeInfo.size(); ++i) {
        os << writeInfo[i].serverLoc.ToString() <<
            ' ' << writeInfo[i].writeId << ' ';
    }
    os << "\r\n\r\n";
}

void
SizeOp::Request(ostream &os)
{
    os <<
        "SIZE\r\n"        << ReqHeaders(*this) <<
        "Chunk-handle: "  << chunkId           << "\r\n"
        "Chunk-version: " << chunkVersion      << "\r\n"
    "\r\n";
}

void
LeaseAcquireOp::Request(ostream &os)
{
    os << "LEASE_ACQUIRE\r\n" << ReqHeaders(*this);
    if (pathname && pathname[0]) {
        os << "Pathname: " << pathname << "\r\n";
    }
    if (chunkId >= 0) {
        os << "Chunk-handle: " << chunkId << "\r\n";
    }
    if (flushFlag) {
        os << "Flush-write-lease: 1\r\n";
    }
    if (leaseTimeout >= 0) {
        os << "Lease-timeout: " << leaseTimeout << "\r\n";
    }
    if (chunkIds && (leaseIds || getChunkLocationsFlag) && chunkIds[0] >= 0) {
        os << "Chunk-ids:";
        for (int i = 0; i < kMaxChunkIds && chunkIds[i] >= 0; i++) {
            os << " " << chunkIds[i];
        }
        os << "\r\n";
        if (getChunkLocationsFlag) {
            os << "Get-locations: 1\r\n";
        }
    }
    os << "\r\n";
}

void
LeaseRenewOp::Request(ostream &os)
{
    os <<
        "LEASE_RENEW\r\n" << ReqHeaders(*this) <<
        "Pathname: "      << pathname          << "\r\n"
        "Chunk-handle: "  << chunkId           << "\r\n"
        "Lease-id: "      << leaseId           << "\r\n"
        "Lease-type: "       "READ_LEASE"         "\r\n"
    "\r\n";
}

void
LeaseRelinquishOp::Request(ostream &os)
{
    os <<
        "LEASE_RELINQUISH\r\n" << ReqHeaders(*this) <<
        "Chunk-handle:"        << chunkId           << "\r\n"
        "Lease-id: "           << leaseId           << "\r\n"
        "Lease-type: "            "READ_LEASE"         "\r\n"
    "\r\n";
}

void
RecordAppendOp::Request(ostream &os)
{
    os <<
        "RECORD_APPEND\r\n" << ReqHeaders(*this) <<
        "Chunk-handle: "    << chunkId           << "\r\n"
        "Chunk-version: "   << chunkVersion      << "\r\n"
        "Num-bytes: "       << contentLength     << "\r\n"
        "Checksum: "        << checksum          << "\r\n"
        "Offset: "          << offset            << "\r\n"
        "File-offset: "       "-1"                  "\r\n"
        "Num-servers: "     << writeInfo.size()  << "\r\n"
        "Servers:"
    ;
    for (vector<WriteInfo>::size_type i = 0; i < writeInfo.size(); ++i) {
        os << writeInfo[i].serverLoc.ToString() <<
            ' ' << writeInfo[i].writeId << ' ';
    }
    os << "\r\n\r\n";
}

void
GetRecordAppendOpStatus::Request(ostream &os)
{
    os <<
        "GET_RECORD_APPEND_OP_STATUS\r\n" << ReqHeaders(*this) <<
        "Chunk-handle: "                  << chunkId << "\r\n"
        "Write-id: "                      << writeId << "\r\n"
    "\r\n";
}

void
ChangeFileReplicationOp::Request(ostream &os)
{
    os <<
        "CHANGE_FILE_REPLICATION\r\n" << ReqHeaders(*this) <<
        "File-handle: "               << fid         << "\r\n"
        "Num-replicas: "              << numReplicas << "\r\n"
    "\r\n";
}

void
GetRecordAppendOpStatus::ParseResponseHeaderSelf(const Properties &prop)
{
    chunkVersion        = prop.getValue("Chunk-version",               (int64_t)-1);
    opSeq               = prop.getValue("Op-seq",                      (int64_t)-1);
    opStatus            = prop.getValue("Op-status",                   -1);
    if (opStatus < 0) {
        opStatus = -KfsToSysErrno(-opStatus);
    }
    opOffset            = prop.getValue("Op-offset",                   (int64_t)-1);
    opLength            = (size_t)prop.getValue("Op-length",           (uint64_t)0);
    widAppendCount      = (size_t)prop.getValue("Wid-append-count",    (uint64_t)0);
    widBytesReserved    = (size_t)prop.getValue("Wid-bytes-reserved",  (uint64_t)0);
    chunkBytesReserved  = (size_t)prop.getValue("Chunk-bytes-reserved",(uint64_t)0);
    remainingLeaseTime  = prop.getValue("Remaining-lease-time",        (int64_t)-1);
    widWasReadOnlyFlag  = prop.getValue("Wid-was-read-only",            0) != 0;
    masterFlag          = prop.getValue("Chunk-master",                 0) != 0;
    stableFlag          = prop.getValue("Stable-flag",                  0) != 0;
    openForAppendFlag   = prop.getValue("Open-for-append-flag",         0) != 0;
    appenderState       = prop.getValue("Appender-state",               -1);
    appenderStateStr    = prop.getValue("Appender-state-string",        "");
    masterCommitOffset  = prop.getValue("Master-commit-offset",         (int64_t)-1);
    nextCommitOffset    = prop.getValue("Next-commit-offset",           (int64_t)-1);
    widReadOnlyFlag     = prop.getValue("Wid-read-only",                 0) != 0;
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
    prop.loadProperties(is, separator, false);
    ParseResponseHeader(prop);
}

void
KfsOp::ParseResponseHeader(const Properties& prop)
{
    // kfsSeq_t resSeq = prop.getValue("Cseq", (kfsSeq_t) -1);
    status = prop.getValue("Status", -1);
    if (status < 0) {
        status = -KfsToSysErrno(-status);
    }
    contentLength = prop.getValue("Content-length", 0);
    statusMsg = prop.getValue("Status-message", string());
    ParseResponseHeaderSelf(prop);
}

///
/// Default parse response handler.
/// @param[in] buf: buffer containing the response
/// @param[in] len: str-len of the buffer.
void
KfsOp::ParseResponseHeaderSelf(const Properties &prop)
{
}

/* static */ void
KfsOp::AddDefaultRequestHeaders(
    kfsUid_t euser /* = kKfsUserNone */, kfsGid_t egroup /* = kKfsGroupNone */)
{
    ostringstream os;
    os << "UserId: ";
    if (euser == kKfsUserNone) {
        os << geteuid();
    } else {
        os << euser;
    }
    os << "\r\n"
    "GroupId: ";
    if (egroup == kKfsGroupNone) {
        os << getegid();
    } else {
        os << egroup;
    }
    os << "\r\n";
    KfsOp::AddExtraRequestHeaders(os.str());
}

///
/// Specific response parsing handlers.
///
void
CreateOp::ParseResponseHeaderSelf(const Properties &prop)
{
    fileId            = prop.getValue("File-handle", (kfsFileId_t) -1);
    metaStriperType   = prop.getValue("Striper-type",
        int(KFS_STRIPED_FILE_TYPE_NONE));
    permissions.user  = prop.getValue("User",  permissions.user);
    permissions.group = prop.getValue("Group", permissions.group);
    permissions.mode  = prop.getValue("Mode",  permissions.mode);
}

void
ReaddirOp::ParseResponseHeaderSelf(const Properties &prop)
{
    numEntries         = prop.getValue("Num-Entries", 0);
    hasMoreEntriesFlag = prop.getValue("Has-more-entries", 0) != 0;
}

void
DumpChunkServerMapOp::ParseResponseHeaderSelf(const Properties &prop)
{
}

void
DumpChunkMapOp::ParseResponseHeaderSelf(const Properties &prop)
{
}

void
UpServersOp::ParseResponseHeaderSelf(const Properties &prop)
{
}

void
ReaddirPlusOp::ParseResponseHeaderSelf(const Properties &prop)
{
    numEntries         = prop.getValue("Num-Entries", 0);
    hasMoreEntriesFlag = prop.getValue("Has-more-entries", 0) != 0;
}

void
MkdirOp::ParseResponseHeaderSelf(const Properties &prop)
{
    fileId            = prop.getValue("File-handle", (kfsFileId_t) -1);
    permissions.user  = prop.getValue("User",  permissions.user);
    permissions.group = prop.getValue("Group", permissions.group);
    permissions.mode  = prop.getValue("Mode",  permissions.mode);
}

static void
ParseFileAttribute(const Properties &prop, FileAttr &fattr)
{
    const string estr;

    fattr.fileId      =          prop.getValue("File-handle", kfsFileId_t(-1));
    fattr.isDirectory =          prop.getValue("Type", estr) == "dir";
    if (fattr.isDirectory) {
        fattr.subCount1 = prop.getValue("File-count", int64_t(-1));
        fattr.subCount2 = prop.getValue("Dir-count",  int64_t(-1));
    } else {
        fattr.subCount1 = prop.getValue("Chunk-count", int64_t(0));
    }
    fattr.fileSize    =          prop.getValue("File-size",   chunkOff_t(-1));
    fattr.numReplicas = (int16_t)prop.getValue("Replication", 1);

    GetTimeval(prop.getValue("M-Time",  ""), fattr.mtime);
    GetTimeval(prop.getValue("C-Time",  ""), fattr.ctime);
    GetTimeval(prop.getValue("CR-Time", ""), fattr.crtime);

    switch (prop.getValue("Striper-type", int(KFS_STRIPED_FILE_TYPE_NONE))) {
        case KFS_STRIPED_FILE_TYPE_NONE:
            fattr.striperType = KFS_STRIPED_FILE_TYPE_NONE;
        break;
        case KFS_STRIPED_FILE_TYPE_RS:
            fattr.striperType = KFS_STRIPED_FILE_TYPE_RS;
        break;
        default:
            fattr.striperType = KFS_STRIPED_FILE_TYPE_UNKNOWN;
        break;
    }

    fattr.numStripes         = (int16_t)prop.getValue("Num-stripes",          0);
    fattr.numRecoveryStripes = (int16_t)prop.getValue("Num-recovery-stripes", 0);
    fattr.stripeSize         =          prop.getValue("Stripe-size",          int32_t(0));
    fattr.user               =          prop.getValue("User",                 kKfsUserNone);
    fattr.group              =          prop.getValue("Group",                kKfsGroupNone);
    fattr.mode               =          prop.getValue("Mode",                 kKfsModeUndef);
}

void
LookupOp::ParseResponseHeaderSelf(const Properties &prop)
{
    ParseFileAttribute(prop, fattr);
}

void
LookupPathOp::ParseResponseHeaderSelf(const Properties &prop)
{
    ParseFileAttribute(prop, fattr);
}

void
AllocateOp::ParseResponseHeaderSelf(const Properties &prop)
{
    chunkId = prop.getValue("Chunk-handle", (kfsFileId_t) -1);
    chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);
    if (append)
        fileOffset = prop.getValue("Chunk-offset", (chunkOff_t) 0);

    string master = prop.getValue("Master", "");
    if (master != "") {
        istringstream ist(master);

        ist >> masterServer.hostname;
        ist >> masterServer.port;
        // put the master the first in the list
        chunkServers.push_back(masterServer);
    }

    int numReplicas = prop.getValue("Num-replicas", 0);
    string replicas = prop.getValue("Replicas", "");

    if (replicas != "") {
        istringstream ser(replicas);
        ServerLocation loc;

        for (int i = 0; i < numReplicas; ++i) {
            ser >> loc.hostname;
            ser >> loc.port;
            if (loc != masterServer)
                chunkServers.push_back(loc);
        }
    }
}

void
GetAllocOp::ParseResponseHeaderSelf(const Properties &prop)
{
    chunkId = prop.getValue("Chunk-handle", (kfsFileId_t) -1);
    chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);
    serversOrderedFlag = prop.getValue("Replicas-ordered", 0) != 0;
    int numReplicas = prop.getValue("Num-replicas", 0);
    string replicas = prop.getValue("Replicas", "");
    if (replicas != "") {
        istringstream ser(replicas);
        ServerLocation loc;

        for (int i = 0; i < numReplicas; ++i) {
            ser >> loc.hostname;
            ser >> loc.port;
            chunkServers.push_back(loc);
        }
    }
}

void
CoalesceBlocksOp::ParseResponseHeaderSelf(const Properties &prop)
{
    dstStartOffset = prop.getValue("Dst-start-offset", (chunkOff_t) 0);
}

void
GetLayoutOp::ParseResponseHeaderSelf(const Properties &prop)
{
    numChunks         = prop.getValue("Num-chunks", 0);
    hasMoreChunksFlag = prop.getValue("Has-more-chunks", 0) != 0;
}

int
GetLayoutOp::ParseLayoutInfo()
{
    if (numChunks <= 0 || contentBuf == NULL) {
        return 0;
    }
    BufferInputStream is(contentBuf, contentLength);
    chunks.clear();
    chunks.reserve(numChunks);
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
SizeOp::ParseResponseHeaderSelf(const Properties &prop)
{
    size = prop.getValue("Size", (long long) 0);
}

void
ReadOp::ParseResponseHeaderSelf(const Properties &prop)
{
    string checksumStr;
    uint32_t nentries;

    nentries = prop.getValue("Checksum-entries", 0);
    checksumStr = prop.getValue("Checksums", "");
    diskIOTime = prop.getValue("DiskIOtime", 0.0);
    istringstream ist(checksumStr);
    checksums.clear();
    for (uint32_t i = 0; i < nentries; i++) {
        uint32_t cksum;
        ist >> cksum;
        checksums.push_back(cksum);
    }
}

void
WriteIdAllocOp::ParseResponseHeaderSelf(const Properties &prop)
{
    writeIdStr                  = prop.getValue("Write-id", string());
    writePrepReplySupportedFlag = prop.getValue("Write-prepare-reply", 0) != 0;
}

void
LeaseAcquireOp::ParseResponseHeaderSelf(const Properties& prop)
{
    leaseId = prop.getValue("Lease-id", int64_t(-1));
    if (leaseIds) {
        leaseIds[0] = -1;
    }
    if (! chunkIds || ! leaseIds) {
        return;
    }
    const Properties::String* const v = prop.getValue("Lease-ids");
    if (! v) {
        return;
    }
    const char*       p = v->GetPtr();
    const char* const e = p + v->GetSize();
    for (int i = 0; p < e && i < kMaxChunkIds && chunkIds[i] >= 0; i++) {
        if (! ValueParser::ParseInt(p, e - p, leaseIds[i])) {
            status      = -EINVAL;
            statusMsg   = "response parse error";
            leaseIds[0] = -1;
            return;
        }
    }
}

void
ChangeFileReplicationOp::ParseResponseHeaderSelf(const Properties &prop)
{
    numReplicas = prop.getValue("Num-replicas", 1);
}

void
GetPathNameOp::Request(ostream& os)
{
    os <<
        "GETPATHNAME\r\n" << ReqHeaders(*this);
    if (fid > 0) {
        os << "File-handle: " << fid << "\r\n";
    }
    if (chunkId > 0) {
        os << "Chunk-handle: " << chunkId << "\r\n";
    }
    os << "\r\n";
}

void
GetPathNameOp::ParseResponseHeaderSelf(const Properties& prop)
{
    ParseFileAttribute(prop, fattr);
    pathname = prop.getValue("Path-name", string());

    offset       = prop.getValue("Chunk-offset", chunkOff_t(-1));
    chunkVersion = prop.getValue("Chunk-version", int64_t(-1));

    const int    numReplicas = prop.getValue("Num-replicas", 0);
    const string replicas    = prop.getValue("Replicas", string());
    if (! replicas.empty()) {
        istringstream ser(replicas);
        for (int i = 0; i < numReplicas; ++i) {
            ServerLocation loc;
            ser >> loc.hostname;
            ser >> loc.port;
            servers.push_back(loc);
        }
    }
}

void
ChmodOp::Request(ostream &os)
{
    os <<
        "CHMOD\r\n" << ReqHeaders(*this) <<
        "File-handle: " << fid << "\r\n"
        "Mode: "        << mode << "\r\n"
        "\r\n";
    ;
}

void
ChownOp::Request(ostream &os)
{
    os <<
        "CHOWN\r\n" << ReqHeaders(*this) <<
        "File-handle: " << fid << "\r\n";
    if (user != kKfsUserNone) {
        os << "Owner: " << user << "\r\n";
    }
    if (group != kKfsGroupNone) {
        os << "Group: " << group << "\r\n";
    }
    os << "\r\n";
}

void
TruncateOp::ParseResponseHeaderSelf(const Properties& prop)
{
    respEndOffset = prop.getValue("End-offset", int64_t(-1));
    if (status == 0 && ! pruneBlksFromHead &&
            endOffset >= 0 && respEndOffset != endOffset) {
        status    = -EFAULT;
        statusMsg = "range truncate is not supported";
    }
}

} //namespace client
} //namespace KFS
