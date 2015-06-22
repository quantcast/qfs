//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/04/18
// Author: Sriram Rao
//         Mike Ovsiannikov
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
// \file KfsClient.cc
// \brief Kfs Client-library code.
//
//----------------------------------------------------------------------------

#include "KfsClient.h"
#include "KfsClientInt.h"
#include "Path.h"
#include "utils.h"
#include "KfsProtocolWorker.h"
#include "RSStriper.h"

#include "common/Properties.h"
#include "common/MsgLogger.h"
#include "common/hsieh_hash.h"
#include "common/kfsatomic.h"
#include "common/MdStream.h"
#include "common/StdAllocator.h"
#include "common/IntToString.h"

#include "qcdio/qcstutils.h"
#include "qcdio/QCUtils.h"

#include "kfsio/checksum.h"
#include "kfsio/Globals.h"
#include "kfsio/SslFilter.h"
#include "kfsio/DelegationToken.h"

#include <signal.h>
#include <stdlib.h>

#include <cstdio>
#include <cstdlib>
#include <limits>
#include <cerrno>
#include <iostream>
#include <sstream>
#include <string>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <pwd.h>
#include <grp.h>
#include <fcntl.h>

#include <boost/scoped_array.hpp>
#include <boost/bind.hpp>

namespace KFS
{
using std::string;
using std::ostream;
using std::istream;
using std::min;
using std::max;
using std::map;
using std::vector;
using std::sort;
using std::transform;
using std::numeric_limits;
using std::unique;
using std::find;
using std::ostringstream;
using std::cerr;

using boost::scoped_array;
using boost::bind;

using client::RSStriperValidate;
using client::KfsNetClient;

// Set read dir limit larger than meta server's default 8K, to allow to
// make it larger, if required, by changing the meta server configuration.
const int kMaxReaddirEntries = 16 << 10;
const int kMaxReadDirRetries = 16;

KfsClient*
Connect(const char* propFile)
{
    ostream* const verbose =
#ifdef DEBUG
    &cerr;
#else
    0;
#endif
    Properties p;
    if (p.loadProperties(propFile, '=', verbose) != 0) {
        return 0;
    }
    return Connect(p.getValue("metaServer.name", ""),
                   p.getValue("metaServer.port", -1),
                   &p);
}

KfsClient*
Connect(const string& metaServerHost, int metaServerPort,
    const Properties* props)
{
    KfsClient* const clnt = new KfsClient();
    clnt->Init(metaServerHost, metaServerPort, props);
    if (clnt->IsInitialized()) {
        return clnt;
    }
    delete clnt;
    return 0;
}

KfsClient*
KfsClient::Connect(
    const string& metaServerHost, int metaServerPort, const char* configFileName)
{
    Properties props;
    const char* const kUseDefaultEnvVarNamePrefix = 0;
    const char*       cfgName                     = 0;
    if (KfsClient::LoadProperties(
            metaServerHost.c_str(),
            metaServerPort,
            kUseDefaultEnvVarNamePrefix,
            props,
            cfgName)) {
        return 0;
    }
    if (configFileName &&
            props.loadProperties(configFileName, '=') != 0) {
        return 0;
    }
    KfsClient* const clnt = new KfsClient();
    clnt->Init(metaServerHost, metaServerPort, &props);
    if (clnt->IsInitialized()) {
        return clnt;
    }
    delete clnt;
    return 0;
}

string
ErrorCodeToStr(int status)
{
    switch (-status) {
        case EBADVERS:        return "version mismatch";
        case ELEASEEXPIRED:   return "lease has expired";
        case EBADCKSUM:       return "checksum mismatch";
        case EDATAUNAVAIL:    return "data not available";
        case ESERVERBUSY:     return "server busy";
        case EALLOCFAILED:    return "chunk allocation failed";
        case EBADCLUSTERKEY:  return "cluster key mismatch";
        case EINVALCHUNKSIZE: return "invalid chunk size";
        case 0:               return "";
        default:              break;
    }
    return QCUtils::SysError(-status);
}

static inline kfsSeq_t
RandomSeqNo()
{
    kfsSeq_t ret = 0;
    if (! CryptoKeys::PseudoRand(&ret, sizeof(ret))) {
        KFS_LOG_STREAM_WARN << "PseudoRand failure" << KFS_LOG_EOM;
        size_t kMaxNameLen = 1024;
        char name[kMaxNameLen + 1];
        gethostname(name, kMaxNameLen);
        name[kMaxNameLen] = 0;
        Hsieh_hash_fcn hf;
        static int64_t cnt = 0;
        ret = microseconds() + getpid() + hf(name, strlen(name)) +
            SyncAddAndFetch(cnt, int64_t(1000000));
    }
    return ((ret < 0 ? -ret : ret) >> 1);
}

static int
ValidateCreateParams(
    int numReplicas, int numStripes, int numRecoveryStripes,
    int stripeSize, int stripedType, kfsSTier_t minSTier, kfsSTier_t maxSTier)
{
    return (
        (numReplicas <= 0 ||
        (stripedType != KFS_STRIPED_FILE_TYPE_NONE &&
            ! RSStriperValidate(
                stripedType, numStripes, numRecoveryStripes, stripeSize, 0)) ||
        (minSTier > maxSTier ||
            maxSTier > kKfsSTierMax ||
            minSTier > kKfsSTierMax ||
            maxSTier < kKfsSTierMin ||
            minSTier < kKfsSTierMin)
        ) ? -EINVAL : 0
    );
}

KfsClient::KfsClient(KfsNetClient* metaServer)
    : mImpl(new KfsClientImpl(metaServer))
{
}

KfsClient::~KfsClient()
{
    delete mImpl;
}

void
KfsClient::SetLogLevel(const string& logLevel)
{
    if (MsgLogger::GetLogger()) {
        MsgLogger::GetLogger()->SetLogLevel(logLevel.c_str());
    }
}

int
KfsClient::Init(const string &metaServerHost, int metaServerPort,
    const Properties* props)
{
    if (IsInitialized()) {
        return -EINVAL;
    }
    return mImpl->Init(metaServerHost, metaServerPort, props);
}

bool
KfsClient::IsInitialized()
{
    return mImpl->IsInitialized();
}

int
KfsClient::Cd(const char *pathname)
{
    return mImpl->Cd(pathname);
}

int
KfsClient::SetCwd(const char* pathname)
{
    return mImpl->SetCwd(pathname);
}

string
KfsClient::GetCwd()
{
    return mImpl->GetCwd();
}

int
KfsClient::Mkdirs(const char *pathname, kfsMode_t mode)
{
    return mImpl->Mkdirs(pathname, mode);
}

int
KfsClient::Mkdir(const char *pathname, kfsMode_t mode)
{
    return mImpl->Mkdir(pathname, mode);
}

int
KfsClient::Rmdir(const char *pathname)
{
    return mImpl->Rmdir(pathname);
}

int
KfsClient::Rmdirs(const char *pathname,
    KfsClient::ErrorHandler* errHandler)
{
    return mImpl->Rmdirs(pathname, errHandler);
}

int
KfsClient::RmdirsFast(const char *pathname,
    KfsClient::ErrorHandler* errHandler)
{
    return mImpl->RmdirsFast(pathname, errHandler);
}

int
KfsClient::Readdir(const char *pathname, vector<string> &result)
{
    return mImpl->Readdir(pathname, result);
}

int
KfsClient::ReaddirPlus(const char *pathname, vector<KfsFileAttr> &result,
    bool computeFilesize, bool updateClientCache, bool fileIdAndTypeOnly)
{
    return mImpl->ReaddirPlus(pathname, result,
        computeFilesize, updateClientCache, fileIdAndTypeOnly);
}

int
KfsClient::OpenDirectory(const char *pathname)
{
    return mImpl->OpenDirectory(pathname);
}

int
KfsClient::Stat(const char *pathname, KfsFileAttr& result, bool computeFilesize)
{
    return mImpl->Stat(pathname, result, computeFilesize);
}

int
KfsClient::Stat(int fd, KfsFileAttr& result)
{
    return mImpl->Stat(fd, result);
}

int
KfsClient::GetNumChunks(const char *pathname)
{
    return mImpl->GetNumChunks(pathname);
}

int
KfsClient::UpdateFilesize(int fd)
{
    return mImpl->UpdateFilesize(fd);
}

bool
KfsClient::Exists(const char *pathname)
{
    return mImpl->Exists(pathname);
}

bool
KfsClient::IsFile(const char *pathname)
{
    return mImpl->IsFile(pathname);
}

bool
KfsClient::IsDirectory(const char *pathname)
{
    return mImpl->IsDirectory(pathname);
}

int
KfsClient::EnumerateBlocks(
    const char* pathname, KfsClient::BlockInfos& res, bool getChunkSizesFlag)
{
    return mImpl->EnumerateBlocks(pathname, res, getChunkSizesFlag);
}

int
KfsClient::GetReplication(const char* pathname,
    KfsFileAttr& attr, int& minChunkReplication, int& maxChunkReplication)
{
    return mImpl->GetReplication(pathname, attr,
        minChunkReplication, maxChunkReplication);
}

int
KfsClient::CreateDelegationToken(
    bool      allowDelegationFlag,
    uint32_t  maxValidForSec,
    bool&     outDelegationAllowedFlag,
    uint64_t& outIssuedTime,
    uint32_t& outTokenValidForSec,
    uint32_t& outDelegationValidForSec,
    string&   outToken,
    string&   outKey,
    string*   outErrMsg)
{
    return mImpl->CreateDelegationToken(
        allowDelegationFlag,
        maxValidForSec,
        outDelegationAllowedFlag,
        outIssuedTime,
        outTokenValidForSec,
        outDelegationValidForSec,
        outToken,
        outKey,
        outErrMsg
    );
}

int
KfsClient::RenewDelegation(
    string&   ioToken,
    string&   ioKey,
    bool&     outDelegationAllowedFlag,
    uint64_t& outIssuedTime,
    uint32_t& outTokenValidForSec,
    uint32_t& outDelegationValidForSec,
    string*   outErrMsg)
{
    return mImpl->RenewDelegation(ioToken, ioKey, outDelegationAllowedFlag,
        outIssuedTime, outTokenValidForSec, outDelegationValidForSec,
        outErrMsg);
}

int
KfsClient::CancelDelegation(
    const string& token,
    const string& key,
    string*       outErrMsg)
{
    return mImpl->CancelDelegation(token, key, outErrMsg);
}

int
KfsClient::GetDelegationTokenInfo(
    const char* inTokenStrPtr,
    kfsUid_t&   outUid,
    uint32_t&   outSeq,
    kfsKeyId_t& outKeyId,
    int16_t&    outFlags,
    uint64_t&   outIssuedTime,
    uint32_t&   outValidForSec)
{
    return mImpl->GetDelegationTokenInfo(
        inTokenStrPtr,
        outUid,
        outSeq,
        outKeyId,
        outFlags,
        outIssuedTime,
        outValidForSec
    );
}

int
KfsClient::CompareChunkReplicas(const char *pathname, string &md5sum)
{
    return mImpl->CompareChunkReplicas(pathname, md5sum);
}

int
KfsClient::VerifyDataChecksums(const char *pathname)
{
    return mImpl->VerifyDataChecksums(pathname);
}

int
KfsClient::VerifyDataChecksums(int fd)
{
    return mImpl->VerifyDataChecksums(fd);
}

/* static */ int
KfsClient::ParseCreateParams(const char* params,
    int& numReplicas, int& numStripes, int& numRecoveryStripes,
    int& stripeSize, int& stripedType,
    kfsSTier_t& minSTier, kfsSTier_t& maxSTier)
{
    numReplicas        = 2;
    numStripes         = 0;
    numRecoveryStripes = 0;
    stripeSize         = 0;
    stripedType        = KFS_STRIPED_FILE_TYPE_NONE;
    minSTier           = kKfsSTierMax;
    maxSTier           = kKfsSTierMax;
    if (! params || ! *params) {
        return 0;
    }
    if (params[0] == 'S' && (params[1] == 0 || params[1] == ',')) {
        numReplicas        = 1;
        numStripes         = 6;
        numRecoveryStripes = 3;
        stripeSize         = 64 << 10,
        stripedType        = KFS_STRIPED_FILE_TYPE_RS;
        if (params[1] == ',') {
            char* p = 0;
            minSTier = (kfsSTier_t)strtol(params + 2, &p, 10);
            if (*p == ',') {
                maxSTier = (kfsSTier_t)strtol(p + 1, &p, 10);
            }
        }
    } else {
        char* p = 0;
        numReplicas = (int)strtol(params, &p, 10);
        if (numReplicas <= 0) {
            return -EINVAL;
        }
        if (*p == ',') numStripes         = (int)strtol(p + 1, &p, 10);
        if (*p == ',') numRecoveryStripes = (int)strtol(p + 1, &p, 10);
        if (*p == ',') stripeSize         = (int)strtol(p + 1, &p, 10);
        if (*p == ',') stripedType        = (int)strtol(p + 1, &p, 10);
        if (*p == ',') minSTier           = (kfsSTier_t)strtol(p + 1, &p, 10);
        if (*p == ',') maxSTier           = (kfsSTier_t)strtol(p + 1, &p, 10);
        if (stripedType == KFS_STRIPED_FILE_TYPE_NONE) {
            numStripes         = 0;
            numRecoveryStripes = 0;
            stripeSize         = 0;
        }
    }
    return ValidateCreateParams(numReplicas, numStripes, numRecoveryStripes,
        stripeSize, stripedType, minSTier, maxSTier);
}

int
KfsClient::Create(const char *pathname, int numReplicas, bool exclusive,
    int numStripes, int numRecoveryStripes, int stripeSize, int stripedType,
    bool forceTypeFlag, kfsMode_t mode, kfsSTier_t minSTier, kfsSTier_t maxSTier)
{
    return mImpl->Create(pathname, numReplicas, exclusive,
        numStripes, numRecoveryStripes, stripeSize, stripedType, forceTypeFlag,
        mode, minSTier, maxSTier);
}


int
KfsClient::Create(const char *pathname, bool exclusive, const char *params)
{
    int        numReplicas;
    int        numStripes;
    int        numRecoveryStripes;
    int        stripeSize;
    int        stripedType;
    kfsSTier_t minSTier;
    kfsSTier_t maxSTier;
    const int ret = ParseCreateParams(
        params, numReplicas, numStripes, numRecoveryStripes,
        stripeSize, stripedType, minSTier, maxSTier);
    if (ret) {
        return ret;
    }
    return mImpl->Create(pathname, numReplicas, exclusive,
        numStripes, numRecoveryStripes, stripeSize, stripedType, true,
        0666, maxSTier, minSTier);
}

int
KfsClient::Remove(const char *pathname)
{
    return mImpl->Remove(pathname);
}

int
KfsClient::Rename(const char *oldpath, const char *newpath, bool overwrite)
{
    return mImpl->Rename(oldpath, newpath, overwrite);
}

int
KfsClient::CoalesceBlocks(const char *srcPath, const char *dstPath, chunkOff_t *dstStartOffset)
{
    return mImpl->CoalesceBlocks(srcPath, dstPath, dstStartOffset);
}

int
KfsClient::SetMtime(const char *pathname, const struct timeval &mtime)
{
    return mImpl->SetMtime(pathname, mtime);
}

int
KfsClient::Open(const char *pathname, int openFlags, int numReplicas,
    int numStripes, int numRecoveryStripes, int stripeSize, int stripedType,
    kfsMode_t mode, kfsSTier_t minSTier, kfsSTier_t maxSTier)
{
    return mImpl->Open(pathname, openFlags, numReplicas,
        numStripes, numRecoveryStripes, stripeSize, stripedType, mode,
        minSTier, maxSTier);
}

int
KfsClient::Open(const char *pathname, int openFlags, const char *params,
    kfsMode_t mode)
{
    int        numReplicas;
    int        numStripes;
    int        numRecoveryStripes;
    int        stripeSize;
    int        stripedType;
    kfsSTier_t minSTier;
    kfsSTier_t maxSTier;
    const int ret = ParseCreateParams(
        params, numReplicas, numStripes, numRecoveryStripes,
        stripeSize, stripedType, minSTier, maxSTier);
    if (ret) {
        return ret;
    }
    return mImpl->Open(pathname, openFlags, numReplicas,
        numStripes, numRecoveryStripes, stripeSize, stripedType, mode,
        minSTier, maxSTier);
}

int
KfsClient::Close(int fd)
{
    return mImpl->Close(fd);
}

int
KfsClient::RecordAppend(int fd, const char *buf, int reclen)
{
    return mImpl->RecordAppend(fd, buf, reclen);
}

int
KfsClient::AtomicRecordAppend(int fd, const char *buf, int reclen)
{
    return mImpl->AtomicRecordAppend(fd, buf, reclen);
}

void
KfsClient::EnableAsyncRW()
{
}

void
KfsClient::DisableAsyncRW()
{
}

int
KfsClient::ReadPrefetch(int fd, char *buf, size_t numBytes)
{
    return mImpl->ReadPrefetch(fd, buf, numBytes);
}

ssize_t
KfsClient::PRead(int fd, chunkOff_t pos, char *buf, size_t numBytes)
{
    chunkOff_t cpos = pos;
    return mImpl->Read(fd, buf, numBytes, &cpos);
}

ssize_t
KfsClient::PWrite(int fd, chunkOff_t pos, const char *buf, size_t numBytes)
{
    chunkOff_t cpos = pos;
    return mImpl->Write(fd, buf, numBytes, &cpos);
}

ssize_t
KfsClient::Read(int fd, char *buf, size_t numBytes)
{
    return mImpl->Read(fd, buf, numBytes);
}

ssize_t
KfsClient::Write(int fd, const char *buf, size_t numBytes)
{
    return mImpl->Write(fd, buf, numBytes);
}

int
KfsClient::WriteAsync(int fd, const char *buf, size_t numBytes)
{
    return mImpl->WriteAsync(fd, buf, numBytes);
}

int
KfsClient::WriteAsyncCompletionHandler(int fd)
{
    return mImpl->WriteAsyncCompletionHandler(fd);
}

void
KfsClient::SkipHolesInFile(int fd)
{
    mImpl->SkipHolesInFile(fd);
}

int
KfsClient::Sync(int fd)
{
    return mImpl->Sync(fd);
}

chunkOff_t
KfsClient::Seek(int fd, chunkOff_t offset, int whence)
{
    return mImpl->Seek(fd, offset, whence);
}

chunkOff_t
KfsClient::Seek(int fd, chunkOff_t offset)
{
    return mImpl->Seek(fd, offset, SEEK_SET);
}

chunkOff_t
KfsClient::Tell(int fd)
{
    return mImpl->Tell(fd);
}

int
KfsClient::Truncate(const char* pathname, chunkOff_t offset)
{
    return mImpl->Truncate(pathname, offset);
}

int
KfsClient::Truncate(int fd, chunkOff_t offset)
{
    return mImpl->Truncate(fd, offset);
}

int
KfsClient::PruneFromHead(int fd, chunkOff_t offset)
{
    return mImpl->PruneFromHead(fd, offset);
}

int
KfsClient::GetDataLocation(const char *pathname, chunkOff_t start, chunkOff_t len,
        vector< vector <string> >& locations)
{
    return mImpl->GetDataLocation(pathname, start, len, locations, 0);
}

int
KfsClient::GetDataLocation(const char *pathname, chunkOff_t start, chunkOff_t len,
        vector< vector <string> >& locations, chunkOff_t* outBlkSize)
{
    return mImpl->GetDataLocation(pathname, start, len, locations, outBlkSize);
}

int
KfsClient::GetDataLocation(int fd, chunkOff_t start, chunkOff_t len,
    vector< vector <string> >& locations)
{
    return mImpl->GetDataLocation(fd, start, len, locations, 0);
}

int
KfsClient::GetDataLocation(int fd, chunkOff_t start, chunkOff_t len,
    vector< vector <string> >& locations, chunkOff_t* outBlkSize)
{
    return mImpl->GetDataLocation(fd, start, len, locations, outBlkSize);
}

int
KfsClient::GetReplicationFactor(const char *pathname)
{
    return mImpl->GetReplicationFactor(pathname);
}

int
KfsClient::SetReplicationFactor(const char *pathname, int16_t numReplicas)
{
    return mImpl->SetReplicationFactor(pathname, numReplicas);
}

int
KfsClient::SetReplicationFactorR(const char *pathname, int16_t numReplicas,
    ErrorHandler* errHandler)
{
    return mImpl->SetReplicationFactorR(pathname, numReplicas, errHandler);
}

int
KfsClient::SetStorageTierRange(
    const char *pathname, kfsSTier_t minSTier, kfsSTier_t maxSTier)
{
    return mImpl->SetStorageTierRange(pathname, minSTier, maxSTier);
}

ServerLocation
KfsClient::GetMetaserverLocation() const
{
    return mImpl->GetMetaserverLocation();
}

void
KfsClient::SetDefaultIOTimeout(int nsecs)
{
    mImpl->SetDefaultIOTimeout(nsecs);
}

int
KfsClient::GetDefaultIOTimeout() const
{
    return mImpl->GetDefaultIOTimeout();
}


void
KfsClient::SetDefaultMetaOpTimeout(int nsecs)
{
    mImpl->SetDefaultMetaOpTimeout(nsecs);
}

int
KfsClient::GetDefaultMetaOpTimeout() const
{
    return mImpl->GetDefaultMetaOpTimeout();
}

void
KfsClient::SetRetryDelay(int nsecs)
{
    mImpl->SetRetryDelay(nsecs);
}

int
KfsClient::GetRetryDelay() const
{
    return mImpl->GetRetryDelay();
}

void
KfsClient::SetMaxRetryPerOp(int retryCount)
{
    mImpl->SetMaxRetryPerOp(retryCount);
}

int
KfsClient::GetMaxRetryPerOp() const
{
    return mImpl->GetMaxRetryPerOp();
}

ssize_t
KfsClient::SetDefaultIoBufferSize(size_t size)
{
    return mImpl->SetDefaultIoBufferSize(size);
}

ssize_t
KfsClient::GetDefaultIoBufferSize() const
{
    return mImpl->GetDefaultIoBufferSize();
}

ssize_t
KfsClient::SetIoBufferSize(int fd, size_t size)
{
    return mImpl->SetIoBufferSize(fd, size);
}

ssize_t
KfsClient::GetIoBufferSize(int fd) const
{
    return mImpl->GetIoBufferSize(fd);
}

ssize_t
KfsClient::SetDefaultReadAheadSize(size_t size)
{
    return mImpl->SetDefaultReadAheadSize(size);
}

ssize_t
KfsClient::GetDefaultReadAheadSize() const
{
    return mImpl->GetDefaultReadAheadSize();
}

ssize_t
KfsClient::SetReadAheadSize(int fd, size_t size)
{
    return mImpl->SetReadAheadSize(fd, size);
}

ssize_t
KfsClient::GetReadAheadSize(int fd) const
{
    return mImpl->GetReadAheadSize(fd);
}

void
KfsClient::SetEOFMark(int fd, chunkOff_t offset)
{
    mImpl->SetEOFMark(fd, offset);
}

int
KfsClient::GetFileOrChunkInfo(kfsFileId_t fileId, kfsChunkId_t chunkId,
    KfsFileAttr& fattr, chunkOff_t& offset, int64_t& chunkVersion,
    vector<ServerLocation>& servers)
{
    return mImpl->GetFileOrChunkInfo(
        fileId, chunkId, fattr, offset, chunkVersion, servers);
}

void
KfsClient::SetDefaultFullSparseFileSupport(bool flag)
{
    mImpl->SetDefaultFullSparseFileSupport(flag);
}

int
KfsClient::SetFullSparseFileSupport(int fd, bool flag)
{
    return mImpl->SetFullSparseFileSupport(fd, flag);
}

void
KfsClient::SetFileAttributeRevalidateTime(int secs)
{
    mImpl->SetFileAttributeRevalidateTime(secs);
}

int
KfsClient::Chmod(int fd, kfsMode_t mode)
{
    return mImpl->Chmod(fd, mode);
}

int
KfsClient::Chmod(const char* pathname, kfsMode_t mode)
{
    return mImpl->Chmod(pathname, mode);
}

int
KfsClient::Chown(int fd, kfsUid_t user, kfsGid_t group)
{
    return mImpl->Chown(fd, user, group);
}

int
KfsClient::Chown(int fd, const char* user, const char* group)
{
    return mImpl->Chown(fd, user, group);
}

int
KfsClient::ChmodR(const char* pathname, kfsMode_t mode,
    KfsClient::ErrorHandler* errHandler)
{
    return mImpl->ChmodR(pathname, mode, errHandler);
}

int
KfsClient::ChownR(const char* pathname, kfsUid_t user, kfsGid_t group,
    KfsClient::ErrorHandler* errHandler)
{
    return mImpl->ChownR(pathname, user, group, errHandler);
}

int
KfsClient::ChownR(const char* pathname, const char* user, const char* group,
    KfsClient::ErrorHandler* errHandler)
{
    return mImpl->ChownR(pathname, user, group, errHandler);
}

void
KfsClient::SetUMask(kfsMode_t mask)
{
    mImpl->SetUMask(mask);
}

kfsMode_t
KfsClient::GetUMask() const
{
    return mImpl->GetUMask();
}

int
KfsClient::Chown(const char* pathname, kfsUid_t user, kfsGid_t group)
{
    return mImpl->Chown(pathname, user, group);
}

int
KfsClient::Chown(const char* pathname, const char* user, const char* group)
{
    return mImpl->Chown(pathname, user, group);
}

int
KfsClient::SetEUserAndEGroup(kfsUid_t user, kfsGid_t group,
    kfsGid_t* groups, int groupsCnt)
{
    return mImpl->SetEUserAndEGroup(user, group, groups, groupsCnt);
}

int
KfsClient::GetUserAndGroupNames(kfsUid_t user, kfsGid_t group,
    string& uname, string& gname)
{
    return mImpl->GetUserAndGroupNames(user, group, uname, gname);
}

int
KfsClient::GetUserAndGroupIds(const char* user, const char* group,
    kfsUid_t& uid, kfsGid_t& gid)
{
    return mImpl->GetUserAndGroupIds(user, group, uid, gid);
}

kfsUid_t
KfsClient::GetUserId()
{
    return mImpl->GetUserId();
}

Properties*
KfsClient::GetStats()
{
    return mImpl->GetStats();
}

static int
LoadConfig(const char* configEnvName, const char* cfg, Properties& props)
{
    const char* const pref  = "FILE:";
    const size_t      len   = strlen(pref);
    char              delim = (char)'=';
    if (strncmp(cfg, pref, len) == 0) {
        if (props.loadProperties(cfg + len, delim) != 0) {
            KFS_LOG_STREAM_INFO <<
                "failed to load configuration from file: " << cfg <<
                " set by environment varialbe:" << configEnvName <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        KFS_LOG_STREAM_INFO <<
            "using configuration: " << cfg <<
            " set by environment varialbe:" << configEnvName <<
        KFS_LOG_EOM;
    } else {
        const char* const pref = "DELIM:";
        const size_t      len  = strlen(pref);
        int               sep  = 0;
        if (strncmp(cfg, pref, len) == 0 &&
                cfg[len] != 0 && cfg[len + 1] != 0) {
            delim = cfg[len];
            sep   = cfg[len + 1] & 0xFF;
        }
        string val;
        for (const char* p = cfg; *p; ++p) {
            int cur = *p & 0xFF;
            if ((sep == 0 ? (cur <= ' ' || cur == ';') : cur == sep)) {
                cur = '\n';
            }
            val.push_back((char)cur);
        }
        if (props.loadProperties(val.data(), val.size(), delim) != 0) {
            KFS_LOG_STREAM_ERROR <<
                "failed to load configuration"
                " set by environment varialbe: " << configEnvName <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        KFS_LOG_STREAM_INFO <<
            "using configuration"
            " set by environment varialbe: " << configEnvName <<
            " length: " << val.size() <<
        KFS_LOG_EOM;
    }
    return 0;
}

/* static */ int
KfsClient::LoadProperties(
    const char*  metaServerHost,
    int          metaServerPort,
    const char*  evnVarNamePrefix,
    Properties&  properties,
    const char*& cfg)
{
    cfg = 0;
    const char* prefix =
        evnVarNamePrefix ? evnVarNamePrefix : "QFS_CLIENT_CONFIG";
    if (metaServerHost && 0 < metaServerPort) {
        ostringstream os;
        os << prefix << "_" << metaServerHost << "_" << metaServerPort;
        string configEnvName = os.str();
        for (size_t i = 0; i < configEnvName.size(); i++) {
            if (strchr("=.+-", configEnvName[i])) {
                configEnvName[i] = '_';
            }
        }
        cfg = getenv(configEnvName.c_str());
        if (cfg && *cfg) {
            return LoadConfig(configEnvName.c_str(), cfg, properties);
        }
    }
    const char* const envName = prefix;
    cfg = getenv(envName);
    return ((cfg && *cfg) ? LoadConfig(envName, cfg, properties) : 0);
}

/* static */ Properties*
KfsClient::CreateProperties()
{
    return new Properties();
}

/* static */ void
KfsClient::DisposeProperties(Properties* props)
{
    delete props;
}

class KfsClient::PropertiesIterator::Iterator : public Properties::iterator
{
public:
    Properties::iterator& Get() { return *this; }
};

KfsClient::PropertiesIterator::PropertiesIterator(
    const Properties* props, bool autoDisposeFlag)
    : mAutoDisposeFlag(autoDisposeFlag),
      mPropertiesPtr(props),
      mKeyPtr(0),
      mValuePtr(0),
      mIteratorPtr(0)
{}

KfsClient::PropertiesIterator::~PropertiesIterator()
{
    delete mIteratorPtr;
    if (mAutoDisposeFlag) {
        KfsClient::DisposeProperties(const_cast<Properties*>(mPropertiesPtr));
    }
}

bool
KfsClient::PropertiesIterator::Next()
{
    if (! mIteratorPtr) {
        if (! mPropertiesPtr || mPropertiesPtr->empty()) {
            return false;
        }
        mIteratorPtr        = new Iterator();
        mIteratorPtr->Get() = mPropertiesPtr->begin();
    }
    if (mPropertiesPtr->end() == mIteratorPtr->Get()) {
        return false;
    }
    mKeyPtr   = mIteratorPtr->Get()->first.c_str();
    mValuePtr = mIteratorPtr->Get()->second.c_str();
    mIteratorPtr->Get()++;
    return true;
}

void
KfsClient::PropertiesIterator::Reset()
{
    if (mIteratorPtr) {
        mIteratorPtr->Get() = mPropertiesPtr->begin();
    }
}

size_t
KfsClient::PropertiesIterator::Size() const
{
    return (mPropertiesPtr ? mPropertiesPtr->size() : size_t(0));
}

namespace client
{

class MatchingServer {
    ServerLocation loc;
public:
    MatchingServer(const ServerLocation &l) : loc(l) { }
    bool operator()(KfsClient * &clnt) const {
        return clnt->GetMetaserverLocation() == loc;
    }
    bool operator()(const ServerLocation &other) const {
        return other == loc;
    }
};

class DefaultErrHandler : public KfsClient::ErrorHandler
{
public:
    DefaultErrHandler()
        : mStatus(0)
        {}
    virtual int operator()(const string& path, int status)
    {
        // Report error and continue.
        KFS_LOG_STREAM_ERROR <<
            path << ": " << ErrorCodeToStr(status) <<
        KFS_LOG_EOM;
        mStatus = status;
        return 0;
    }
    int GetStatus() const
        { return mStatus; }
private:
    int mStatus;
};

class KfsClientImpl::ClientsList
{
public:
    static void Insert(KfsClientImpl& client)
        { Instance().InsertSelf(client); }
    static void Remove(KfsClientImpl& client)
        { Instance().RemoveSelf(client); }
    static void Init(KfsClientImpl& client)
        { Instance().InitSelf(client); }
    static int SetEUserAndEGroup(kfsUid_t user, kfsGid_t group,
            kfsGid_t* groups, int groupsCnt)
    {
        return Instance().SetEUserAndEGroupSelf(user, group, groups, groupsCnt);
    }
private:
    typedef QCDLList<KfsClientImpl, 0> List;
    QCMutex        mMutex;
    KfsClientImpl* mList[1];

    static ClientsList* sInstance;

    class Globals
    {
    public:
        kfsMode_t        mUMask;
        kfsUid_t         mEUser;
        kfsGid_t         mEGroup;
        vector<kfsGid_t> mGroups;
        int              mDefaultFileAttributeRevalidateTime;
        unsigned int     mDefaultFileAttributeRevalidateScan;

        static const Globals& Get()
            { return GetInstance(); }
        static int SetEUserAndEGroup(kfsUid_t user, kfsGid_t group,
            kfsGid_t* groups, int groupsCnt)
        {
            return GetInstance().SetEUserAndEGroupSelf(
                user, group, groups, groupsCnt);
        }
    private:
        Globals()
            : mUMask(0),
              mEUser(geteuid()),
              mEGroup(getegid()),
              mGroups(),
              mDefaultFileAttributeRevalidateTime(30),
              mDefaultFileAttributeRevalidateScan(64)
        {
            signal(SIGPIPE, SIG_IGN);
            libkfsio::InitGlobals();
            SslFilter::Error const sslErr = SslFilter::Initialize();
            if (sslErr) {
                KFS_LOG_STREAM_FATAL << "ssl intialization error: " << sslErr <<
                    " " << SslFilter::GetErrorMsg(sslErr) <<
                KFS_LOG_EOM;
                MsgLogger::Stop();
                abort();
            }
            int const maxGroups = min((int)sysconf(_SC_NGROUPS_MAX), 1 << 16);
            if (maxGroups > 0) {
                gid_t* const grs = new gid_t[maxGroups];
                const int    cnt = getgroups(maxGroups, grs);
                if (cnt > 0) {
                    mGroups.reserve(cnt + 1);
                    for (int i = 0; i < cnt; i++) {
                        mGroups.push_back((kfsGid_t)grs[i]);
                    }
                }
                delete [] grs;
            }
            if (find(mGroups.begin(), mGroups.end(), mEGroup) == mGroups.end()) {
                mGroups.push_back(mEGroup);
            }
            KfsOp::AddDefaultRequestHeaders(mEUser, mEGroup);
            AddUserHeader((uid_t)mEUser);
            const mode_t mask = umask(0);
            umask(mask);
            mUMask = mask & Permissions::kAccessModeMask;
            const char* p = getenv("QFS_CLIENT_DEFAULT_FATTR_REVALIDATE_TIME");
            if (! p) {
                p = getenv("KFS_CLIENT_DEFAULT_FATTR_REVALIDATE_TIME");
            }
            if (p) {
                char* e = 0;
                long  v = strtol(p, &e, 10);
                if (p < e && ((*e & 0xFF) <= ' ' || (*e & 0xFF) == ':')) {
                    mDefaultFileAttributeRevalidateTime = (int)v;
                    if ((*e & 0xFF) == ':') {
                        p = e + 1;
                        v = strtol(p, &e, 10);
                        if (0 <= v && p < e && (*e & 0xFF) <= ' ') {
                            mDefaultFileAttributeRevalidateScan = (unsigned int)v;
                        }
                    }
                }
            }
        }
        void AddUserHeader(uid_t uid)
        {
            struct passwd      pwebuf = {0};
            struct passwd*     pwe    = 0;
            StBufferT<char, 1> buf;
            buf.Resize((size_t)
                max(long(8) << 10, sysconf(_SC_GETPW_R_SIZE_MAX)));
            const int err = getpwuid_r(uid, &pwebuf,
                buf.GetPtr(), buf.GetSize(), &pwe);
            if (! err && (pwe && pwe->pw_name)) {
                string hdr("User: ");
                for (const char* p = pwe->pw_name; *p != 0; p++) {
                    const int c = *p & 0xFF;
                    if (c > ' ' && c != '%') {
                        hdr.push_back((char)c);
                    } else {
                        const char* const kHexDigits = "0123456789ABCDEF";
                        hdr.push_back((char)'%');
                        hdr.push_back(kHexDigits[(c >> 4) & 0xF]);
                        hdr.push_back(kHexDigits[c & 0xF]);
                    }
                }
                hdr += "\r\n";
                KfsOp::AddExtraRequestHeaders(hdr);
            }
        }
        ~Globals()
            { Instance().Shutdown(); }
        int SetEUserAndEGroupSelf(kfsUid_t user, kfsGid_t group,
            kfsGid_t* groups, int groupsCnt)
        {
            mGroups.clear();
            if (groupsCnt > 0) {
                mGroups.reserve(groupsCnt + 1);
                for (int i = 0; i < groupsCnt; i++) {
                    mGroups.push_back(groups[i]);
                }
            }
            if (group != kKfsGroupNone &&
                    find(mGroups.begin(), mGroups.end(), group) ==
                        mGroups.end()) {
                mGroups.push_back(group);
            }
            mEUser  = user  == kKfsUserNone  ? geteuid() : user;
            mEGroup = group == kKfsGroupNone ? getegid() : group;
            KfsOp::SetExtraRequestHeaders(string());
            KfsOp::AddDefaultRequestHeaders(mEUser, mEGroup);
            AddUserHeader((uid_t)mEUser);
            return 0;
        }
        static Globals& GetInstance()
        {
            static Globals globals;
            return globals;
        }
    };
    friend class Globals;

    ClientsList()
        : mMutex()
        { List::Init(mList); }
    ~ClientsList()
        { assert(! "unexpected invocation"); }
    void InsertSelf(KfsClientImpl& client)
    {
        QCStMutexLocker locker(mMutex);
        List::Init(client);
        List::PushBack(mList, client);
        const Globals& globals = Globals::Get();
        client.mEUser  = globals.mEUser;
        client.mEGroup = globals.mEGroup;
        client.mGroups = globals.mGroups;
        client.mUMask  = globals.mUMask;
        client.mFileAttributeRevalidateTime =
            globals.mDefaultFileAttributeRevalidateTime;
        client.mFileAttributeRevalidateScan =
            globals.mDefaultFileAttributeRevalidateScan;
    }
    void RemoveSelf(KfsClientImpl& client)
    {
        QCStMutexLocker locker(mMutex);
        List::Remove(mList, client);
    }
    void Shutdown()
    {
        KfsClientImpl* list[1];
        {
            QCStMutexLocker locker(mMutex);
            list[0] = mList[0];
            List::Init(mList);
        }
        while (! List::IsEmpty(list)) {
            List::PopFront(list)->Shutdown();
        }
        {
            QCStMutexLocker locker(mMutex);
            KfsOp::SetExtraRequestHeaders(string());
        }
        SslFilter::Cleanup();
    }
    void InitSelf(KfsClientImpl& /* client */)
    {
        QCStMutexLocker locker(mMutex);
        if (! MsgLogger::IsLoggerInited()) {
            const char* p = getenv("QFS_CLIENT_LOG_LEVEL");
            if (! p) {
                p = getenv("KFS_CLIENT_LOG_LEVEL");
            }
            MsgLogger::Init(0, MsgLogger::kLogLevelINFO, 0, 0, p);
        }
    }
    int SetEUserAndEGroupSelf(kfsUid_t user, kfsGid_t group,
        kfsGid_t* groups, int groupCnt)
    {
        QCStMutexLocker locker(mMutex);
        if (List::IsEmpty(mList)) {
            return Globals::SetEUserAndEGroup(
                user, group, groups, groupCnt);
        }
        if (List::Front(mList) != List::Back(mList)) {
            KFS_LOG_STREAM_ERROR <<
                "cannot change user and group -- more than one"
                " KFS client instance already exist" <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        KfsClientImpl& client = *List::Front(mList);
        QCStMutexLocker clientLock(client.mMutex);
        if (! client.mFileTable.empty()) {
            KFS_LOG_STREAM_ERROR <<
                "setting user and group must be performed immediately"
                " after KFS client created" <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        const int ret = Globals::SetEUserAndEGroup(
            user, group, groups, groupCnt);
        if (ret != 0) {
            return ret;
        }
        const Globals& globals = Globals::Get();
        client.mEUser  = globals.mEUser;
        client.mEGroup = globals.mEGroup;
        client.mGroups = globals.mGroups;
        return 0;
    }
    static ClientsList& Instance();
};

KfsClientImpl::ClientsList&
KfsClientImpl::ClientsList::Instance()
{
    static bool sOnce = true;
    if (sOnce) {
        sOnce = false;
        static struct { char alloc[sizeof(ClientsList)]; } sStorage;
        sInstance = new (&sStorage) ClientsList();
    }
    return *sInstance;
}
KfsClientImpl::ClientsList* KfsClientImpl::ClientsList::sInstance =
    &KfsClientImpl::ClientsList::Instance();

inline void
KfsClientImpl::Validate(
    const KfsClientImpl::FAttr* fa) const
{
    if (fa && (
            ! FAttrLru::IsInList(mFAttrLru, *fa) ||
            fa != fa->fidNameIt->second ||
            (fa->nameIt != mPathCacheNone && fa != fa->nameIt->second))) {
        KFS_LOG_STREAM_FATAL << "invalid FAttr:" <<
            " "               << (const void*)fa <<
            " prev: "         << (const void*)&FAttrLru::GetPrev(*fa) <<
            " next: "         << (const void*)&FAttrLru::GetNext(*fa) <<
            " fileId: "       << fa->fileId <<
            " gen: "          << fa->generation <<
            " validateTime: " << fa->validatedTime <<
        KFS_LOG_EOM;
        MsgLogger::Stop();
        abort();
    }
}

//
// Now, the real work is done by the impl object....
//

KfsClientImpl::KfsClientImpl(
    KfsNetClient* metaServer)
    : mMutex(),
      mReadCompletionMutex(),
      mIsInitialized(metaServer != 0),
      mMetaServerLoc(),
      mNetManager(),
      mChunkServer(mNetManager),
      mCwd("/"),
      mFileTable(),
      mFidNameToFAttrMap(),
      mPathCache(),
      mPathCacheNone(mPathCache.insert(
        make_pair(string(), static_cast<KfsClientImpl::FAttr*>(0))).first),
      mFAttrPool(),
      mDeleteClearFattr(0),
      mFreeFileTableEntires(),
      mFattrCacheSkipValidateCnt(0),
      mFileAttributeRevalidateTime(30),
      mFileAttributeRevalidateScan(64),
      mFAttrCacheGeneration(1),
      mTmpPath(),
      mTmpAbsPathStr(),
      mTmpAbsPath(),
      mTmpCurPath(),
      mTmpDirName(),
      mSlash("/"),
      mDefaultIoBufferSize(min(CHUNKSIZE, size_t(1) << 20)),
      mDefaultReadAheadSize(min(mDefaultIoBufferSize, size_t(1) << 20)),
      mFailShortReadsFlag(true),
      mFileInstance(0),
      mProtocolWorker(0),
      mMaxNumRetriesPerOp(DEFAULT_NUM_RETRIES_PER_OP),
      mRetryDelaySec(RETRY_DELAY_SECS),
      mDefaultOpTimeout(30),
      mDefaultMetaOpTimeout(120),
      mFreeCondVarsHead(0),
      mEUser(kKfsUserNone),
      mEGroup(kKfsGroupNone),
      mUMask(0),
      mGroups(),
      mCreateId(RandomSeqNo()),
      mUseOsUserAndGroupFlag(true),
      mInitLookupRootFlag(true),
      mUserNames(),
      mGroupNames(),
      mUserIds(),
      mGroupIds(),
      mUidIt(mUserIds.end()),
      mGidIt(mGroupIds.end()),
      mUserNamesIt(mUserNames.end()),
      mGroupNamesIt(mGroupNames.end()),
      mUpdateUidIt(mUserIds.end()),
      mUpdateGidIt(mGroupIds.end()),
      mUpdateUserNameIt(mUserNames.end()),
      mUpdateGroupNameIt(mGroupNames.end()),
      mTmpInputStream(),
      mTmpOutputStream(),
      mNameBufSize((size_t)max(max(
        sysconf(_SC_GETPW_R_SIZE_MAX),
        sysconf(_SC_GETGR_R_SIZE_MAX)),
        long(8) << 10)
      ),
      mNameBuf(new char[mNameBufSize]),
      mAuthCtx(),
      mProtocolWorkerAuthCtx(),
      mTargetDiskIoSize(1 << 20),
      mConfig(),
      mMetaServer(metaServer)
{
    if (mMetaServer) {
        mMetaServerLoc = mMetaServer->GetServerLocation();
    } else {
        ClientsList::Insert(*this);
    }

    QCStMutexLocker l(mMutex);

    FAttrLru::Init(mFAttrLru);
    mTmpPath.reserve(32);
    mTmpAbsPathStr.reserve(MAX_PATH_NAME_LENGTH);
    mTmpBuffer[kTmpBufferSize] = 0;
    mChunkServer.SetMaxContentLength(64 << 20);
    mChunkServer.SetAuthContext(&mAuthCtx);
}

KfsClientImpl::~KfsClientImpl()
{
    if (! mMetaServer) {
        ClientsList::Remove(*this);
    }
    QCStMutexLocker l(mMutex);
    KfsClientImpl::ShutdownSelf();
    FAttr* p;
    while ((p = FAttrLru::Front(mFAttrLru))) {
        Delete(p);
    }
    delete mProtocolWorker;
    KfsClientImpl::CleanupPendingRead();
    vector <FileTableEntry *>::iterator it = mFileTable.begin();
    while (it != mFileTable.end()) {
        delete *it++;
    }
    delete [] mNameBuf;
}

void
KfsClientImpl::Shutdown()
{
    QCStMutexLocker l(mMutex);
    ShutdownSelf();
}

void
KfsClientImpl::ShutdownSelf()
{
    assert(mMutex.IsOwned());
    if (mProtocolWorker) {
        QCStMutexUnlocker unlock(mMutex);
        mProtocolWorker->Stop();
    }
    mAuthCtx.Clear();
    mProtocolWorkerAuthCtx.Clear();
}

int KfsClientImpl::Init(const string& metaServerHost, int metaServerPort,
    const Properties* props)
{
    if (mMetaServer) {
        KFS_LOG_STREAM_ERROR <<
            "invalid invocation: meta server already set: " <<
                mMetaServer->GetServerLocation() <<
        KFS_LOG_EOM;
        return -EINVAL;
    }
    if (metaServerHost.empty() || metaServerPort <= 0) {
        KFS_LOG_STREAM_ERROR <<
            "invalid metaserver location: " <<
            metaServerHost << ":" << metaServerPort <<
        KFS_LOG_EOM;
        return -EINVAL;
    }
    ClientsList::Init(*this);

    QCStMutexLocker l(mMutex);
    if (mIsInitialized) {
        KFS_LOG_STREAM_ERROR <<
            "extraneous init invocation with: " <<
                metaServerHost << " " << metaServerPort <<
            " current: " << mMetaServerLoc <<
        KFS_LOG_EOM;
        return -EINVAL;
    }

    mMetaServerLoc.hostname = metaServerHost;
    mMetaServerLoc.port     = metaServerPort;
    const Properties* properties = props;
    Properties envProps;
    if (! properties) {
        const char* cfg = 0;
        const int   ret = KfsClient::LoadProperties(
            metaServerHost.c_str(), metaServerPort, 0, envProps, cfg);
        if (ret < 0) {
            return ret;
        }
        if (cfg) {
            properties = &envProps;
        }
    }
    const char* const kAuthParamPrefix = "client.auth.";
    if (properties) {
        string errMsg;
        int    err;
        const bool kVerifyFlag = true;
        if ((err = mAuthCtx.SetParameters(
                kAuthParamPrefix, *properties, 0, &errMsg,
                kVerifyFlag)) != 0 ||
                (err = mProtocolWorkerAuthCtx.SetParameters(
                    kAuthParamPrefix, *properties, &mAuthCtx, &errMsg,
                    kVerifyFlag)) != 0) {
            KFS_LOG_STREAM_ERROR <<
                "authentication context initialization error: " <<
                errMsg <<
            KFS_LOG_EOM;
            return err;
        }
        mFailShortReadsFlag = properties->getValue(
            "client.fullSparseFileSupport",
            mFailShortReadsFlag ? 0 : 1) == 0;
        // Target io and buffer size defaults.
        const int targetDiskIoSize = properties->getValue(
            "client.targetDiskIoSize", -1);
        if ((int)CHECKSUM_BLOCKSIZE <= targetDiskIoSize) {
            mTargetDiskIoSize = (targetDiskIoSize +
                (int)CHECKSUM_BLOCKSIZE - 1) / (int)CHECKSUM_BLOCKSIZE *
                (int)CHECKSUM_BLOCKSIZE;
        }
        const int defaultIoBufferSize = properties->getValue(
            "client.defaultIoBufferSize", -1);
        if ((int)CHECKSUM_BLOCKSIZE <= defaultIoBufferSize) {
            mDefaultIoBufferSize = ((size_t)defaultIoBufferSize +
                CHECKSUM_BLOCKSIZE  - 1) /
                CHECKSUM_BLOCKSIZE * CHECKSUM_BLOCKSIZE;
        } else {
            mDefaultIoBufferSize = max(mDefaultIoBufferSize,
                (size_t)mTargetDiskIoSize);
        }
        const int defaultReadAheadSize = properties->getValue(
            "client.defaultReadAheadSize", -1);
        if (0 <= defaultReadAheadSize) {
            mDefaultReadAheadSize = ((size_t)defaultReadAheadSize +
                CHECKSUM_BLOCKSIZE - 1) /
                CHECKSUM_BLOCKSIZE * CHECKSUM_BLOCKSIZE;
        } else if ((int)CHECKSUM_BLOCKSIZE <= defaultIoBufferSize) {
            mDefaultReadAheadSize = mDefaultIoBufferSize;
        }
        mConfig.clear();
        properties->copyWithPrefix("client.", mConfig);
    }
    KFS_LOG_STREAM_DEBUG <<
        "will use metaserver at: " <<
        metaServerHost << ":" << metaServerPort <<
    KFS_LOG_EOM;
    mIsInitialized = mMetaServerLoc.IsValid();
    if (! mIsInitialized) {
        KFS_LOG_STREAM_ERROR <<
            "invalid metaserver location: " <<
            metaServerHost << ":" << metaServerPort <<
        KFS_LOG_EOM;
        return -EINVAL;
    }
    const int ret = InitUserAndGroupMode();
    mIsInitialized = ret == 0;
    if (! mIsInitialized) {
        mInitLookupRootFlag = true;
        ShutdownSelf();
    }
    return ret;
}

/// A notion of "cwd" in KFS.
///
int
KfsClientImpl::Cd(const char *pathname)
{
    QCStMutexLocker l(mMutex);

    KfsFileAttr attr;
    string      path;
    const int   status = StatSelf(pathname, attr, false, &path);
    if (status < 0) {
        KFS_LOG_STREAM_DEBUG << "Non-existent file: " <<
            (pathname ? pathname : "null:")  <<
        KFS_LOG_EOM;
        return status;
    }
    if (! attr.isDirectory) {
        KFS_LOG_STREAM_DEBUG << "Non-existent dir: " <<
            (pathname ? pathname : "null:")  <<
        KFS_LOG_EOM;
        return -ENOTDIR;
    }
    mCwd = path;
    return 0;
}

int
KfsClientImpl::SetCwd(const char *pathname)
{
    if (! pathname) {
        return -EFAULT;
    }

    QCStMutexLocker l(mMutex);

    size_t       len = strlen(pathname);
    const char*  ptr = GetTmpAbsPath(pathname, len);
    if (! mTmpAbsPath.Set(ptr, len)) {
        return -EINVAL;
    }
    const size_t      sz       = mTmpAbsPath.size();
    const Path::Token kRootDir = Path::Token("/", 1);
    if (sz < 1 || mTmpAbsPath[0] != kRootDir) {
        return -EINVAL;
    }
    const Path::Token kThisDir(".",    1);
    const Path::Token kParentDir("..", 2);
    for (size_t i = 1; i < sz; i++) {
        const Path::Token& cname = mTmpAbsPath[i];
        if (cname == kThisDir || cname.mLen <= 0 || cname == kParentDir) {
            continue;
        }
        mTmpDirName.assign(cname.mPtr, cname.mLen);
        int res;
        if ((res = ValidateName(mTmpDirName)) != 0) {
            return res;
        }
    }
    mCwd = mTmpAbsPath.NormPath();
    return 0;
}

///
/// To allow tools to get at "pwd"
///
string
KfsClientImpl::GetCwd()
{
    return mCwd;
}

const char*
KfsClientImpl::GetTmpAbsPath(const char* pathname, size_t& ioLen)
{
    if (mTmpAbsPathStr.data() == pathname) {
        return pathname;
    }
    if (ioLen <= 0) {
        ioLen = mCwd.length();
        return mCwd.c_str();
    }
    if (*pathname == '/') {
        return pathname;
    }
    mTmpAbsPathStr.assign(mCwd.data(), mCwd.length());
    mTmpAbsPathStr.append("/", 1);
    mTmpAbsPathStr.append(pathname, ioLen);
    ioLen = mTmpAbsPathStr.length();
    return mTmpAbsPathStr.c_str();
}

///
/// Make a directory hierarchy in KFS.
///
int
KfsClientImpl::Mkdirs(const char *pathname, kfsMode_t mode)
{
    if (! pathname) {
        return -EFAULT;
    }
    if (! *pathname) {
        return -EINVAL;
    }

    QCStMutexLocker l(mMutex);

    KFS_LOG_STREAM_DEBUG <<
        "mkdirs: " << pathname <<
        " mode: "  << oct << mode <<
    KFS_LOG_EOM;
    //
    // Walk from the root down to the last part of the path making the
    // directory hierarchy along the way.  If any of the components of
    // the path is a file, error out.
    //
    size_t       len = strlen(pathname);
    const char*  ptr = GetTmpAbsPath(pathname, len);
    if (! mTmpAbsPath.Set(ptr, len)) {
        return -EINVAL;
    }
    const size_t      sz       = mTmpAbsPath.size();
    const Path::Token kRootDir = Path::Token("/", 1);
    if (sz < 1 || mTmpAbsPath[0] != kRootDir) {
        return -EINVAL;
    }
    const bool        kComputeFileSize = false;
    const time_t      now              = time(0);
    bool              createdFlag      = false;
    int               res              = 0;
    const Path::Token kThisDir(".",    1);
    const Path::Token kParentDir("..", 2);
    mTmpCurPath.clear();
    mTmpCurPath.reserve(MAX_PATH_NAME_LENGTH);
    mTmpPath.clear();
    mTmpPath.reserve(sz);
    mTmpPath.push_back(make_pair(ROOTFID, 0));
    for (size_t i = 1; i < sz; i++) {
        const Path::Token& cname = mTmpAbsPath[i];
        if (cname == kThisDir || cname.mLen <= 0) {
            continue;
        }
        if (cname == kParentDir) {
            const size_t pl = mTmpPath.size() - 1;
            if (pl > 0) {
                mTmpCurPath.erase(mTmpCurPath.length() -
                    mTmpAbsPath[mTmpPath.back().second].mLen - 1);
                mTmpPath.pop_back();
            }
            continue;
        }
        mTmpDirName.assign(cname.mPtr, cname.mLen);
        if ((res = ValidateName(mTmpDirName)) != 0) {
            break;
        }
        mTmpCurPath += mSlash;
        mTmpCurPath += mTmpDirName;
        if (mTmpCurPath.length() > MAX_PATH_NAME_LENGTH) {
            res = -ENAMETOOLONG;
            break;
        }
        FAttr* fa = LookupFAttr(mTmpPath.back().first, mTmpDirName);
        res = (fa && IsValid(*fa, now)) ? 0 :
            LookupAttr(mTmpPath.back().first, mTmpDirName,
                fa, kComputeFileSize, mTmpCurPath);
        if (res == 0) {
            assert(fa);
            if (! fa->isDirectory) {
                res = -ENOTDIR;
                break;
            }
            // Invalidate the counts, assuming that in most cases case a new sub
            // directory will be created.
            fa->staleSubCountsFlag = true;
            mTmpPath.push_back(make_pair(fa->fileId, i));
            continue;
        }
        if (res != -ENOENT) {
            break;
        }
        MkdirOp op(0, mTmpPath.back().first, mTmpDirName.c_str(),
            Permissions(
                mUseOsUserAndGroupFlag ? mEUser  : kKfsUserNone,
                mUseOsUserAndGroupFlag ? mEGroup : kKfsGroupNone,
                mode != kKfsModeUndef ? (mode & ~mUMask) : mode
            ),
            NextCreateId()
        );
        DoMetaOpWithRetry(&op);
        if ((res = op.status) == 0) {
            mTmpPath.push_back(make_pair(op.fileId, i));
            if (! createdFlag && (fa = LookupFAttr(ROOTFID, mSlash))) {
                fa->staleSubCountsFlag = true;
            }
            createdFlag = true;
            if (i + 1 == sz) {
                if (! op.userName.empty()) {
                    UpdateUserId(op.userName, op.permissions.user, now);
                }
                if (! op.groupName.empty()) {
                    UpdateGroupId(op.groupName, op.permissions.group, now);
                }
            }
            continue;
        }
        if (res != -EEXIST) {
            break;
        }
        fa = 0;
        if ((res = LookupAttr(mTmpPath.back().first, mTmpDirName,
                fa, kComputeFileSize, mTmpCurPath)) != 0) {
            break;
        }
        assert(fa);
        if (! fa->isDirectory) {
            res = -ENOTDIR;
            break;
        }
        fa->staleSubCountsFlag = true;
        mTmpPath.push_back(make_pair(fa->fileId, i));
    }
    mTmpAbsPath.Clear();
    return res;
}

///
/// Make a directory in KFS.
/// @param[in] pathname         The full pathname such as /.../dir
/// @retval 0 if mkdir is successful; -errno otherwise
int
KfsClientImpl::Mkdir(const char *pathname, kfsMode_t mode)
{
    QCStMutexLocker l(mMutex);

    kfsFileId_t parentFid;
    string      dirname;
    string      path;
    const bool  kEnforceLastDirFlag      = false;
    const bool  kInvalidateSubCountsFlag = true;
    int         res                      = GetPathComponents(
        pathname, &parentFid, dirname, &path,
        kInvalidateSubCountsFlag, kEnforceLastDirFlag);
    if (res < 0) {
        return res;
    }
    MkdirOp op(0, parentFid, dirname.c_str(),
        Permissions(
            mUseOsUserAndGroupFlag ? mEUser  : kKfsUserNone,
            mUseOsUserAndGroupFlag ? mEGroup : kKfsGroupNone,
            mode != kKfsModeUndef  ? (mode & ~mUMask) : mode
        ),
        NextCreateId()
    );
    DoMetaOpWithRetry(&op);
    if (op.status < 0) {
        return op.status;
    }
    time_t now = 0; // assign to suppress compiler warning.
    if (! op.userName.empty()) {
        now = time(0);
        UpdateUserId(op.userName, op.permissions.user, now);
    }
    if (! op.groupName.empty()) {
        if (op.userName.empty()) {
            now = time(0);
        }
        UpdateGroupId(op.groupName, op.permissions.group, now);
    }
    return 0;
}

///
/// Remove a directory in KFS.
/// @param[in] pathname         The full pathname such as /.../dir
/// @retval 0 if rmdir is successful; -errno otherwise
int
KfsClientImpl::Rmdir(const char *pathname)
{
    QCStMutexLocker l(mMutex);

    string      dirname;
    string      path;
    kfsFileId_t parentFid;
    const bool  kInvalidateSubCountsFlag = true;
    int res = GetPathComponents(pathname, &parentFid, dirname, &path,
        kInvalidateSubCountsFlag);
    if (res < 0) {
        return res;
    }
    RmdirOp op(0, parentFid, dirname.c_str(), path.c_str());
    DoMetaOpWithRetry(&op);
    Delete(LookupFAttr(parentFid, dirname));
    return op.status;
}

///
/// Remove a directory hierarchy in KFS.
/// @param[in] pathname         The full pathname such as /.../dir
/// @retval 0 if rmdir is successful; -errno otherwise
int
KfsClientImpl::Rmdirs(const char *pathname,
    KfsClientImpl::ErrorHandler* errHandler)
{
    return RmdirsFast(pathname, errHandler);
}

///
/// Remove a directory hierarchy in KFS.
/// @param[in] pathname         The full pathname such as /.../dir
/// @retval 0 if rmdir is successful; -errno otherwise
int
KfsClientImpl::RmdirsFast(const char *pathname,
    KfsClientImpl::ErrorHandler* errHandler)
{
    QCStMutexLocker l(mMutex);

    if (! pathname || ! *pathname) {
        return -EINVAL;
    }
    string      dirname;
    kfsFileId_t parentFid = -1;
    string      path;
    int ret = GetPathComponents(pathname, &parentFid, dirname, &path);
    if (ret < 0) {
        return ret;
    }
    FAttr* fa = 0;
    ret = LookupAttr(parentFid, dirname, fa, false, path);
    if (ret < 0) {
        return ret;
    }
    if (! fa->isDirectory) {
        return -ENOTDIR;
    }
    const size_t pos = path.rfind('/');
    if (pos == string::npos) {
        assert(! "internal error: invalid path name");
        return -EFAULT;
    }
    DefaultErrHandler errorHandler;
    ret = RmdirsSelf(
        path.substr(0, pos),
        (pos == 0 && dirname == "/") ? string() : dirname,
        parentFid,
        fa->fileId,
        errHandler ? *errHandler : errorHandler
    );
    // Invalidate cached attributes.
    InvalidateAllCachedAttrs();
    return (errHandler ? ret : (ret != 0 ? ret : errorHandler.GetStatus()));
}

void
KfsClientImpl::InvalidateAllCachedAttrs()
{
    // Invalidate cached attributes.
    mFAttrCacheGeneration++;
}

int
KfsClientImpl::RmdirsSelf(const string& path, const string& dirname,
    kfsFileId_t parentFid, kfsFileId_t dirFid,
    KfsClientImpl::ErrorHandler& errHandler)
{
    const string p = path + (path == "/" ? "" : "/") + dirname;
    // Don't compute file sizes and don't update i-node attribute cache.
    vector<KfsFileAttr> entries;
    int res = ReaddirPlus(p, dirFid, entries, false, false, true);
    if (res < 0) {
        if ((res = errHandler(p, res)) != 0) {
            return res;
        }
        entries.clear();
    }
    for (vector<KfsFileAttr>::const_iterator it = entries.begin();
            it != entries.end();
            ++it) {
        if (it->filename == "." || it->filename == "..") {
            continue;
        }
        if (it->isDirectory) {
            if (it->fileId == ROOTFID) {
                continue;
            }
            res = RmdirsSelf(p, it->filename, dirFid, it->fileId, errHandler);
            if (res != 0) {
                break;
            }
        } else {
            res = Remove(p, dirFid, it->filename);
            if (res < 0 && (res = errHandler(p, res)) != 0) {
                break;
            }
        }
    }
    if (res != 0) {
        return res;
    }
    if (dirname.empty() || (parentFid == ROOTFID && dirname == "/")) {
        return 0;
    }
    RmdirOp op(0, parentFid, dirname.c_str(), p.c_str());
    DoMetaOpWithRetry(&op);
    return (op.status < 0 ? errHandler(p, op.status) : 0);
}

int
KfsClientImpl::Remove(const string& dirname, kfsFileId_t dirFid,
    const string& filename)
{
    string pathname = dirname + "/" + filename;
    RemoveOp op(0, dirFid, filename.c_str(), pathname.c_str());
    DoMetaOpWithRetry(&op);
    return op.status;
}

class ReaddirResult
{
public:
    ReaddirResult()
        : mNext(0),
          mBuf(0),
          mLen(0),
          mCount(0)
        {}
    template<typename T>
    ReaddirResult* Set(
        T& op)
    {
        assert(
            op.contentBuf &&
            op.contentBufLen >= op.contentLength &&
            ! mNext
        );
        if (mBuf) {
            mNext = Create(op);
            return mNext;
        }
        mBuf   = op.contentBuf;
        mLen   = op.contentLength;
        mCount = op.numEntries;
        op.ReleaseContentBuf();
        op.contentLength = 0;
        return this;
    }
    ~ReaddirResult()
        { Clear(); }
    ReaddirResult* Clear()
    {
        delete [] mBuf;
        mBuf   = 0;
        mLen   = 0;
        mCount = 0;
        DeleteAll();
        return this;
    }
    template<typename T>
    int Parse(T& result)
    {
        ReaddirResult* next = this;
        int            ret  = 0;
        do {
            if (ret == 0) {
                if ((ret = next->ParseSelf(result)) != 0) {
                    result.clear();
                }
            }
            ReaddirResult* const ptr = next;
            next = ptr->mNext;
            ptr->mNext = 0;
            if (ptr != this) {
                ptr->Delete();
            }
        } while (next);
        return ret;
    }
    bool GetLast(
        string& name) const
    {
        if (! mBuf || mLen <= 1) {
            return false;
        }
        const char* const end = mBuf + mLen - 1;
        if ((*end & 0xFF) != '\n') {
            return false;
        }
        const char* ptr = end - 1;
        while (mBuf < ptr && (*ptr & 0xFF) != '\n') {
            --ptr;
        }
        if ((*ptr & 0xFF) == '\n') {
            ++ptr;
        }
        if (end <= ptr) {
            return false;
        }
        name.assign(ptr, end - ptr);
        return true;
    }
    bool GetLast(
        const PropertiesTokenizer::Token& inBeginEntry,
        const PropertiesTokenizer::Token& inName,
        string&                           name) const
    {
        if (! mBuf || mLen <= 1) {
            return false;
        }
        const char* end = mBuf + mLen - 1;
        if ((*end & 0xFF) != '\n') {
            return false;
        }
        const char* ptr = end - 1;
        while (mBuf < ptr) {
            while (mBuf < ptr && (*ptr & 0xFF) != '\n') {
                --ptr;
            }
            const char* const cur = ptr;
            if ((*ptr & 0xFF) == '\n') {
                ++ptr;
            }
            if (PropertiesTokenizer::Token(ptr, end) == inBeginEntry) {
                if (mBuf + mLen <= ++end) {
                    return false;
                }
                PropertiesTokenizer tokenizer(end, mBuf + mLen - end, false);
                while (tokenizer.Next()) {
                    if (tokenizer.GetKey() == inName) {
                        PropertiesTokenizer::Token const&
                            val = tokenizer.GetValue();
                        name.assign(val.mPtr, val.mLen);
                        return (0 < val.mLen);
                    }
                }
                return false;
            }
            end = cur;
            ptr = end - 1;
        }
        return false;
    }
private:
    typedef StdFastAllocator<ReaddirResult> Alloc;
    static Alloc& GetAllocator()
    {
        static Alloc sAlloc;
        return sAlloc;
    }
    template<typename T>
    static ReaddirResult* Create(
        T& op)
    {
        ReaddirResult* const ret = new (GetAllocator().allocate(1))
            ReaddirResult();
        return ret->Set(op);
    }
    void Delete()
    {
        this->~ReaddirResult();
        GetAllocator().deallocate(this, 1);
    }
    void DeleteAll()
    {
        ReaddirResult* next = mNext;
        mNext = 0;
        while (next) {
            ReaddirResult* const ptr = next;
            next = ptr->mNext;
            ptr->mNext = 0;
            ptr->Delete();
        }
    }
    int ParseSelf(
        vector<string>& result)
    {
        const char*       ptr = mBuf;
        const char* const end = ptr + mLen;
        for (int i = 0; i < mCount; i++) {
            const char* const ne = reinterpret_cast<const char*>(
                memchr(ptr, '\n', end - ptr));
            if (! ne) {
                return -EIO;
            }
            result.push_back(string(ptr, ne - ptr));
            ptr = ne + (ne < end ? 1 : 0);
        }
        return 0;
    }
    template<typename T>
    int ParseSelf(
        T& result)
        { return result(mBuf, mLen, mCount); }
private:
    ReaddirResult* mNext;
    const char*    mBuf;
    int            mLen;
    int            mCount;
private:
    ReaddirResult(
        const ReaddirResult&);
    ReaddirResult& operator=(
        const ReaddirResult&);
};

///
/// Read a directory's contents.  This is analogous to READDIR in
/// NFS---just reads the directory contents and returns the names;
/// you'll need to lookup the attributes next.  The resulting
/// directory entries are sorted lexicographically.
///
/// XXX NFS READDIR also returns the file ids, and we should do
/// the same here.
///
/// @param[in] pathname The full pathname such as /.../dir
/// @param[out] result  The filenames in the directory
/// @retval 0 if readdir is successful; -errno otherwise
int
KfsClientImpl::Readdir(const char* pathname, vector<string>& result)
{
    QCStMutexLocker l(mMutex);

    result.clear();
    KfsFileAttr attr;
    const int res = StatSelf(pathname, attr, false);
    if (res < 0) {
        return res;
    }
    if (! attr.isDirectory) {
        return -ENOTDIR;
    }
    ReaddirOp      op(0, attr.fileId);
    ReaddirResult  opResult;
    ReaddirResult* last  = &opResult;
    int            count = 0;
    for (int retryCnt = kMaxReadDirRetries; ;) {
        op.seq                = 0;
        op.numEntries         = kMaxReaddirEntries;
        op.contentLength      = 0;
        op.hasMoreEntriesFlag = false;

        DoMetaOpWithRetry(&op);
        if (op.status < 0) {
            if (op.fnameStart.empty() ||
                    (op.status != -ENOENT && op.status != -EAGAIN)) {
                break;
            }
            if (--retryCnt <= 0) {
                KFS_LOG_STREAM_ERROR <<
                    pathname << ": id: " << attr.fileId <<
                    " directory has changed " <<
                    kMaxReadDirRetries <<
                    " times while attempting to list it; giving up" <<
                KFS_LOG_EOM;
                op.status = -EAGAIN;
                break;
            }
            op.fnameStart.clear();
            last = opResult.Clear();
            continue;
        }
        if (op.numEntries <= 0) {
            break;
        }
        if (op.contentLength <= 0) {
            op.status = -EIO;
            break;
        }
        last = last->Set(op);
        count += op.numEntries;
        if (! op.hasMoreEntriesFlag) {
            break;
        }
        if (! last->GetLast(op.fnameStart)) {
            op.status = -EIO;
            break;
        }
    }
    if (op.status == 0) {
        result.reserve(count);
        KFS_LOG_STREAM_DEBUG <<
            "readdir parse: entries:" << count <<
        KFS_LOG_EOM;
        op.status = opResult.Parse(result);
        KFS_LOG_STREAM_DEBUG <<
            "readdir parse: entries:" << result.size() <<
        KFS_LOG_EOM;
        if (op.status == 0) {
            sort(result.begin(), result.end());
            if (! op.fnameStart.empty()) {
                result.erase(
                    unique(result.begin(), result.end()), result.end());
            }
        }
    } else {
        result.clear();
    }
    return op.status;
}

///
/// Read a directory's contents and get the attributes.  This is
/// analogous to READDIRPLUS in NFS.  The resulting directory entries
/// are sort lexicographically.
///
/// @param[in] pathname The full pathname such as /.../dir
/// @param[out] result  The filenames in the directory and their attributes
/// @retval 0 if readdir is successful; -errno otherwise
int
KfsClientImpl::ReaddirPlus(const char* pathname, vector<KfsFileAttr>& result,
    bool computeFilesize, bool updateClientCache, bool fileIdAndTypeOnly)
{
    QCStMutexLocker l(mMutex);

    result.clear();
    KfsFileAttr attr;
    string path;
    const int res = StatSelf(pathname, attr, false, &path);
    if (res < 0) {
        return res;
    }
    if (! attr.isDirectory) {
        return -ENOTDIR;
    }
    return ReaddirPlus(path, attr.fileId, result,
        computeFilesize, updateClientCache, fileIdAndTypeOnly);
}

class ReaddirPlusParser
{
public:
    ReaddirPlusParser(
        BufferInputStream& tmpInputStream)
        : mEntry(),
          mHexParserFlag(false),
          mTmpInputStream(tmpInputStream)
        {}
    void SetUseHexParser() { mHexParserFlag = true; }
    bool Parse(
        PropertiesTokenizer& tokenizer)
    {
        mEntry.Reset();
        if (mHexParserFlag) {
            sHexParser.Parse(tokenizer, &mEntry);
        } else {
            sParser.Parse(tokenizer, &mEntry);
        }
        return mEntry.Validate();
    }
    void LastChunkInfo(ChunkAttr& info) const
    {
        info.chunkOffset   = mEntry.lastChunkOffset;
        info.chunkId       = mEntry.chunkId;
        info.chunkVersion  = mEntry.chunkVersion;
        info.chunkServerLoc.clear();

        const int numReplicas = mEntry.lastchunkNumReplicas;
        if (numReplicas <= 0 || mEntry.lastChunkReplicas.empty()) {
            return;
        }
        istream& is = mTmpInputStream.Set(
            mEntry.lastChunkReplicas.mPtr,
            mEntry.lastChunkReplicas.mLen
        );
        if (mHexParserFlag) {
            is.flags(istream::hex | istream::skipws);
        }
        info.chunkServerLoc.resize(numReplicas);
        for (int i = 0; i < numReplicas; ++i) {
            is >> info.chunkServerLoc[i].hostname;
            is >> info.chunkServerLoc[i].port;
        }
    }
    const FileAttr& GetFattr() const
        { return mEntry; }
    const char* GetNamePtr() const
        { return mEntry.filename.mPtr; }
    size_t GetNameLen() const
        { return mEntry.filename.mLen; }
    const TokenValue& GetUserName() const
        { return mEntry.userName; }
    const TokenValue& GetGroupName() const
        { return mEntry.groupName; }
private:
    class Entry : public FileAttr
    {
    public:
        enum { kCTimeUndef = 2 * 1000 * 1000 + 1 };
        chunkOff_t  lastChunkOffset;
        kfsFileId_t chunkId;
        int64_t     chunkVersion;
        int         lastchunkNumReplicas;
        TokenValue  lastChunkReplicas;
        TokenValue  type;
        TokenValue  filename;
        TokenValue  userName;
        TokenValue  groupName;

        Entry()
            : FileAttr(),
              lastChunkOffset(0),
              chunkId(-1),
              chunkVersion(-1),
              lastchunkNumReplicas(0),
              lastChunkReplicas(),
              type(),
              filename(),
              userName(),
              groupName()
          {}
        void Reset()
        {
            FileAttr::Reset();
            lastChunkOffset      = 0;
            chunkId              = -1;
            chunkVersion         = -1;
            lastchunkNumReplicas = 0;
            minSTier             = kKfsSTierMax;
            maxSTier             = kKfsSTierMax;
            ctime.tv_usec        = kCTimeUndef;
            lastChunkReplicas.clear();
            type.clear();
            filename.clear();
            userName.clear();
            groupName.clear();
        }
        bool Validate()
        {
            isDirectory = type.mLen == 3 && memcmp(type.mPtr, "dir", 3) == 0;
            if (isDirectory) {
                if (fileSize < 0) {
                    fileSize = 0;
                }
                if (subCount2 < 0) {
                    subCount1 = -1;
                }
            } else if (subCount1 <= 0) {
                subCount1 = 0;
            }
            if (ctime.tv_usec == kCTimeUndef) {
                ctime = crtime;
            }
            return (! filename.empty());
        }
        bool HandleUnknownField(
            const char* /* key */, size_t /* keyLen */,
            const char* /* val */, size_t /* valLen */)
            { return true; }
    };
    template<typename INT_PARSER>
    class VParser
    {
    public:
        // Specialization for StripedFileType
        typedef ValueParserT<INT_PARSER> ValueParser;

        template<typename T>
        static void SetValue(
            const char* inPtr,
            size_t      inLen,
            const T&    inDefaultValue,
            T&          outValue)
        {
            ValueParser::SetValue(inPtr, inLen, inDefaultValue, outValue);
        }
        template<typename T>
        static bool ParseInt(
            const char*& ioPtr,
            size_t       inLen,
            T&           outValue)
        {
            return ValueParser::ParseInt(ioPtr, inLen, outValue);
        }
        static void SetValue(
            const char*            inPtr,
            size_t                 inLen,
            const StripedFileType& inDefaultValue,
            StripedFileType&       outValue)
        {
            int theVal = 0;
            if (! ValueParser::ParseInt(inPtr, inLen, theVal)) {
                outValue = inDefaultValue;
                return;
            }
            if (KFS_STRIPED_FILE_TYPE_UNKNOWN < theVal &&
                    theVal < KFS_STRIPED_FILE_TYPE_COUNT) {
                outValue = StripedFileType(theVal);
            } else {
                outValue = inDefaultValue;
            }
        }
    };
    class VParserDec
    {
    public:
        // Specialization for struct timeval
        typedef struct timeval        TimeVal;
        typedef VParser<DecIntParser> ValueParser;

        template<typename T>
        static void SetValue(
            const char* inPtr,
            size_t      inLen,
            const T&    inDefaultValue,
            T&          outValue)
        {
            ValueParser::SetValue(inPtr, inLen, inDefaultValue, outValue);
        }
        static void SetValue(
            const char*    inPtr,
            size_t         inLen,
            const TimeVal& inDefaultValue,
            TimeVal&       outValue)
        {
            if (! ValueParser::ParseInt(inPtr, inLen, outValue.tv_sec) ||
                    ! ValueParser::ParseInt(inPtr, inLen, outValue.tv_usec)) {
                outValue = inDefaultValue;
            }
        }
    };
    class VParserHex
    {
    public:
        // Specialization for struct timeval
        typedef struct timeval        TimeVal;
        typedef VParser<HexIntParser> ValueParser;

        template<typename T>
        static void SetValue(
            const char* inPtr,
            size_t      inLen,
            const T&    inDefaultValue,
            T&          outValue)
        {
            ValueParser::SetValue(inPtr, inLen, inDefaultValue, outValue);
        }
        static void SetValue(
            const char*    inPtr,
            size_t         inLen,
            const TimeVal& inDefaultValue,
            TimeVal&       outValue)
        {
            const int64_t kUses        = 1000000;
            int64_t       theTimeUsecs = 0;
            if (ValueParser::ParseInt(inPtr, inLen, theTimeUsecs)) {
                outValue.tv_sec  = theTimeUsecs / kUses;
                outValue.tv_usec = theTimeUsecs % kUses;
            } else {
                outValue = inDefaultValue;
            }
        }
    };
    typedef ObjectParser<Entry, VParserDec> Parser;
    typedef ObjectParser<Entry, VParserHex> HexParser;

    Entry              mEntry;
    bool               mHexParserFlag;
    BufferInputStream& mTmpInputStream;

    static const Parser&    sParser;
    static const HexParser& sHexParser;

    static const Parser& MakeParser()
    {
        static Parser sParser;
        return sParser
            .Def("Name",                 &Entry::filename                      )
            .Def("File-handle",          &Entry::fileId,        kfsFileId_t(-1))
            .Def("Type",                 &Entry::type                          )
            .Def("M-Time",               &Entry::mtime                         )
            .Def("C-Time",               &Entry::ctime                         )
            .Def("CR-Time",              &Entry::crtime                        )
            .Def("Replication",          &Entry::numReplicas,        int16_t(1))
            .Def("Chunk-count",          &Entry::subCount1                     )
            .Def("File-size",            &Entry::fileSize,       chunkOff_t(-1))
            .Def("Striper-type",         &Entry::striperType, KFS_STRIPED_FILE_TYPE_UNKNOWN)
            .Def("Num-stripes",          &Entry::numStripes                    )
            .Def("Num-recovery-stripes", &Entry::numRecoveryStripes            )
            .Def("Stripe-size",          &Entry::stripeSize                    )
            .Def("Chunk-offset",         &Entry::lastChunkOffset               )
            .Def("Chunk-handle",         &Entry::chunkId,       kfsFileId_t(-1))
            .Def("Chunk-version",        &Entry::chunkVersion,      int64_t(-1))
            .Def("Num-replicas",         &Entry::lastchunkNumReplicas          )
            .Def("Replicas",             &Entry::lastChunkReplicas             )
            .Def("User",                 &Entry::user,             kKfsUserNone)
            .Def("Group",                &Entry::group,           kKfsGroupNone)
            .Def("Mode",                 &Entry::mode,            kKfsModeUndef)
            .Def("File-count",           &Entry::subCount1,         int64_t(-1))
            .Def("Dir-count",            &Entry::subCount2,         int64_t(-1))
            .Def("Min-tier",             &Entry::minSTier,         kKfsSTierMax)
            .Def("Max-tier",             &Entry::maxSTier,         kKfsSTierMax)
            .Def("User-name",            &Entry::userName                      )
            .Def("Group-name",           &Entry::groupName                     )
            .DefDone()
        ;
    };
    // Short keys to save bandwidth / memory
    static const HexParser& MakeHexParser()
    {
        static HexParser sParser;
        return sParser
            .Def("N",  &Entry::filename                        )
            .Def("H",  &Entry::fileId,          kfsFileId_t(-1))
            .Def("T",  &Entry::type                            )
            .Def("M",  &Entry::mtime                           )
            .Def("C",  &Entry::ctime                           )
            .Def("CR", &Entry::crtime                          )
            .Def("R",  &Entry::numReplicas,          int16_t(1))
            .Def("CC", &Entry::subCount1                       )
            .Def("S",  &Entry::fileSize,         chunkOff_t(-1))
            .Def("ST", &Entry::striperType, KFS_STRIPED_FILE_TYPE_UNKNOWN)
            .Def("SC", &Entry::numStripes                      )
            .Def("SR", &Entry::numRecoveryStripes              )
            .Def("SS", &Entry::stripeSize                      )
            .Def("LO", &Entry::lastChunkOffset                 )
            .Def("LH", &Entry::chunkId,         kfsFileId_t(-1))
            .Def("LV", &Entry::chunkVersion,        int64_t(-1))
            .Def("LN", &Entry::lastchunkNumReplicas            )
            .Def("LR", &Entry::lastChunkReplicas               )
            .Def("U",  &Entry::user,               kKfsUserNone)
            .Def("G",  &Entry::group,             kKfsGroupNone)
            .Def("A",  &Entry::mode,              kKfsModeUndef)
            .Def("FC", &Entry::subCount1,           int64_t(-1))
            .Def("DC", &Entry::subCount2,           int64_t(-1))
            .Def("FT", &Entry::minSTier,           kKfsSTierMax)
            .Def("LT", &Entry::maxSTier,           kKfsSTierMax)
            .Def("UN", &Entry::userName                        )
            .Def("GN", &Entry::groupName                       )
            .DefDone()
        ;
    }
};

const ReaddirPlusParser::Parser&    ReaddirPlusParser::sParser =
    ReaddirPlusParser::MakeParser();
const ReaddirPlusParser::HexParser& ReaddirPlusParser::sHexParser =
    ReaddirPlusParser::MakeHexParser();

bool
KfsClientImpl::Cache(time_t now, const string& dirname, kfsFileId_t dirFid,
    const KfsFileAttr& attr, bool staleSubCountsFlag)
{
    if (attr.filename == "." || attr.filename == "..") {
        return true;
    }
    const string path = dirname + "/" + attr.filename;
    FAttr* fa = LookupFAttr(dirFid, attr.filename);
    if (fa && staleSubCountsFlag && attr.fileId == fa->fileId) {
        return true;
    }
    if (UpdateFattr(dirFid, attr.filename, fa, path, attr, now) != 0) {
        return false;
    }
    if (fa && staleSubCountsFlag) {
        fa->staleSubCountsFlag = true;
    }
    return true;
}

class KfsClientImpl::ReadDirPlusResponseParser
{
public:
    typedef KfsClientImpl::FAttr FAttr;

    ReadDirPlusResponseParser(
        KfsClientImpl&       inOuter,
        vector<KfsFileAttr>& inResult,
        bool                 inComputeFilesize,
        kfsFileId_t          inDirFid,
        time_t               inNow)
        : fileChunkInfo(),
          beginEntry("Begin-entry"),
          shortBeginEntry("B"),
          hasDirs(false),
          parser(inOuter.mTmpInputStream),
          result(inResult),
          computeFilesize(inComputeFilesize),
          outer(inOuter),
          dirFid(inDirFid),
          now(inNow)
        { result.clear(); }
    int operator()(
        const char* inBuf,
        size_t      inLen,
        int         inCount)
    {
        PropertiesTokenizer tokenizer(inBuf, inLen, false);
        tokenizer.Next();
        const PropertiesTokenizer::Token& beginToken =
            tokenizer.GetKey() == shortBeginEntry ?
                shortBeginEntry : beginEntry;
        if (&beginToken == &shortBeginEntry) {
            parser.SetUseHexParser();
        }
        for (int i = 0; i < inCount; i++) {
            if (tokenizer.GetKey() != beginToken) {
                return -EIO;
            }
            if (! parser.Parse(tokenizer)) {
                continue; // Skip empty entries.
            }
            result.push_back(KfsFileAttr(
                parser.GetFattr(), parser.GetNamePtr(), parser.GetNameLen()));
            KfsFileAttr& attr = result.back();
            if (attr.filename.empty()) {
                return -EIO;
            }
            const TokenValue& uname = parser.GetUserName();
            if (0 < uname.mLen) {
                outer.UpdateUserId(
                    string(uname.mPtr, uname.mLen), attr.user, now);
            }
            const TokenValue& gname = parser.GetGroupName();
            if (0 < gname.mLen) {
                outer.UpdateGroupId(
                    string(gname.mPtr, gname.mLen), attr.group, now);
            }
            if (attr.isDirectory) {
                if (hasDirs) {
                    continue;
                }
                if (attr.filename != "." && attr.filename != "..") {
                    hasDirs = true;
                }
                continue;
            }
            if (! computeFilesize || attr.fileSize >= 0) {
                continue;
            }
            FAttr* const fa = outer.LookupFAttr(dirFid, attr.filename);
            if (fa && ! fa->isDirectory && fa->fileSize >= 0) {
                if (outer.IsValid(*fa, now)) {
                    attr.fileSize = fa->fileSize;
                    continue;
                }
            }
            fileChunkInfo.resize(result.size());
            parser.LastChunkInfo(fileChunkInfo.back());
        }
        return 0;
    }
    void clear()
        { result.clear(); }

    vector<ChunkAttr>                fileChunkInfo;
    const PropertiesTokenizer::Token beginEntry;
    const PropertiesTokenizer::Token shortBeginEntry;
    bool                             hasDirs;
private:
    ReaddirPlusParser                parser;
    vector<KfsFileAttr>&             result;
    const bool                       computeFilesize;
    KfsClientImpl&                   outer;
    kfsFileId_t const                dirFid;
    const time_t                     now;
private:
    ReadDirPlusResponseParser(
        const ReadDirPlusResponseParser&);
    ReadDirPlusResponseParser& operator=(
        const ReadDirPlusResponseParser&);
};

int
KfsClientImpl::ReaddirPlus(const string& pathname, kfsFileId_t dirFid,
    vector<KfsFileAttr>& result, bool computeFilesize, bool updateClientCache,
    bool fileIdAndTypeOnly)
{
    assert(mMutex.IsOwned());
    if (pathname.empty() || pathname[0] != '/') {
        KFS_LOG_STREAM_ERROR <<
            "ReaddirPlus invalid path name: " << pathname <<
        KFS_LOG_EOM;
        return -EINVAL;
    }

    time_t const                      now = time(0);
    ReadDirPlusResponseParser         parser(
        *this, result, computeFilesize && ! fileIdAndTypeOnly, dirFid, now);
    const PropertiesTokenizer::Token* beginEntryToken = 0;
    const PropertiesTokenizer::Token* nameEntryToken  = 0;
    const PropertiesTokenizer::Token  nameToken("Name");
    const PropertiesTokenizer::Token  nameTokenShort("N");
    const bool                        kGetLastChunkInfoIfSizeUnknown = true;
    const bool                        kOmitLastChunkInfo = ! computeFilesize;
    ReaddirPlusOp                     op(
        0, dirFid, kGetLastChunkInfoIfSizeUnknown, kOmitLastChunkInfo,
        fileIdAndTypeOnly);
    ReaddirResult                     opResult;
    ReaddirResult*                    last  = &opResult;
    int                               count = 0;
    for (int retryCnt = kMaxReadDirRetries; ;) {
        op.seq                = 0;
        op.numEntries         = kMaxReaddirEntries;
        op.contentLength      = 0;
        op.hasMoreEntriesFlag = false;

        DoMetaOpWithRetry(&op);

        if (op.status < 0) {
            if (op.fnameStart.empty() ||
                    (op.status != -ENOENT && op.status != -EAGAIN)) {
                break;
            }
            if (--retryCnt <= 0) {
                KFS_LOG_STREAM_ERROR <<
                    pathname << ": id: " << dirFid <<
                    " directory has changed " <<
                    kMaxReadDirRetries <<
                    " times while attempting to list it; giving up" <<
                KFS_LOG_EOM;
                op.status = -EAGAIN;
                break;
            }
            last = opResult.Clear();
            op.fnameStart.clear();
            continue;
        }
        if (op.numEntries <= 0) {
            break;
        }
        if (op.contentLength <= 0) {
            op.status = -EIO;
            break;
        }
        // The response format:
        // Begin-entry <values> Begin-entry <values>
        // The last entry doesn't have a end-marker.
        if (! beginEntryToken && op.hasMoreEntriesFlag) {
            PropertiesTokenizer tokenizer(
                op.contentBuf, op.contentLength, false);
            tokenizer.Next();
            if (tokenizer.GetKey() == parser.shortBeginEntry) {
                nameEntryToken  = &nameTokenShort;
                beginEntryToken = &parser.shortBeginEntry;
            } else {
                nameEntryToken  = &nameToken;
                beginEntryToken = &parser.beginEntry;
            }
        }
        last = last->Set(op);
        count += op.numEntries;
        if (! op.hasMoreEntriesFlag) {
            break;
        }
        if (! last->GetLast(*beginEntryToken, *nameEntryToken, op.fnameStart)) {
            op.status = -EIO;
            break;
        }
    }
    if (op.status == 0) {
        result.reserve(count);
        KFS_LOG_STREAM_DEBUG <<
            "readdirplus parse: entries: " << count <<
        KFS_LOG_EOM;
        op.status = opResult.Parse(parser);
        KFS_LOG_STREAM_DEBUG <<
            "readdirplus parse done: entries: " << result.size() <<
        KFS_LOG_EOM;
    }
    if (op.status != 0) {
        result.clear();
        return op.status;
    }
    ComputeFilesizes(result, parser.fileChunkInfo);

    // if there are too many entries in the dir, then the caller is
    // probably scanning the directory.  don't put it in the cache
    string dirname(pathname);
    for (string::size_type len = dirname.size();
            len > 0 && dirname[len - 1] == '/';
            ) {
        dirname.erase(--len);
    }
    const size_t kMaxUpdateSize = 1 << 10;
    if (updateClientCache && ! fileIdAndTypeOnly &&
            result.size() <= kMaxUpdateSize &&
            mFidNameToFAttrMap.size() < kMaxUpdateSize) {
        InvalidateCachedAttrsWithPathPrefix(dirname);
        for (size_t i = 0; i < result.size(); i++) {
            if (! Cache(now, dirname, dirFid, result[i])) {
                break;
            }
        }
    } else if (updateClientCache && parser.hasDirs) {
        size_t dirCnt            = 0;
        size_t kMaxDirUpdateSize = 1024;
        InvalidateCachedAttrsWithPathPrefix(dirname);
        for (size_t i = 0; i < result.size(); i++) {
            if (! result[i].isDirectory) {
                continue;
            }
            if (! Cache(now, dirname, dirFid, result[i], fileIdAndTypeOnly) ||
                    kMaxDirUpdateSize <= ++dirCnt) {
                break;
            }
        }
    }
    sort(result.begin(), result.end());
    if (! op.fnameStart.empty()) {
        // The meta server doesn't guarantee that listing restarts from the
        // exact same position if there were entry names hash collisions,
        // and the the name where collision occurred was used as cursor (the
        // restart point), and the entry was removed and added back right
        // before the next readdir rpc execution started.
        // This is due to b+tree ordering where newly added entries inserted
        // before the existing entries with the same keys. The name hash is
        // used as part of the b+tree key.
        // Remove duplicate entries, if any.
        result.erase(unique(result.begin(), result.end(),
            bind(&KfsFileAttr::filename, _1) ==
            bind(&KfsFileAttr::filename, _2)
        ), result.end());
    }
    return 0;
}

int
KfsClientImpl::Stat(const char *pathname, KfsFileAttr& kfsattr, bool computeFilesize)
{
    QCStMutexLocker l(mMutex);
    const bool kValidSubCountsRequiredFlag = true;
    return StatSelf(pathname, kfsattr, computeFilesize, 0, 0,
        kValidSubCountsRequiredFlag);
}

int
KfsClientImpl::Stat(int fd, KfsFileAttr& kfsattr)
{
    QCStMutexLocker l(mMutex);

    if (! valid_fd(fd)) {
        return -EBADF;
    }
    FileTableEntry& entry = *(mFileTable[fd]);
    kfsattr          = entry.fattr;
    kfsattr.filename = entry.name;
    return 0;
}

int
KfsClientImpl::StatSelf(const char* pathname, KfsFileAttr& kfsattr,
    bool computeFilesize, string* path, KfsClientImpl::FAttr** cattr,
    bool validSubCountsRequiredFlag)
{
    assert(mMutex.IsOwned());

    if (! pathname) {
        return -EFAULT;
    }
    if (! *pathname) {
        return -EINVAL;
    }
    KFS_LOG_STREAM_DEBUG <<
        "stat self: " << pathname <<
    KFS_LOG_EOM;
    if (pathname[0] == '/') {
        mTmpAbsPathStr = pathname;
    } else {
        mTmpAbsPathStr.assign(mCwd.data(), mCwd.length());
        mTmpAbsPathStr.append("/", 1);
        mTmpAbsPathStr.append(pathname);
    }
    FAttr* fa = LookupFAttr(mTmpAbsPathStr, path);
    if (! fa || (computeFilesize && ! fa->isDirectory && fa->fileSize < 0) ||
            (validSubCountsRequiredFlag && fa->staleSubCountsFlag) ||
            ! IsValid(*fa, time(0))) {
        kfsFileId_t parentFid;
        string      filename;
        string      tmpPath;
        string&     fpath = path ? *path : tmpPath;
        mDeleteClearFattr = &fa;
        int res = GetPathComponents(
            mTmpAbsPathStr.c_str(), &parentFid, filename, &fpath);
        assert(mDeleteClearFattr ? *mDeleteClearFattr == fa : ! fa);
        Validate(fa);
        mDeleteClearFattr = 0;
        if (res == 0) {
            res = LookupAttr(parentFid, filename, fa, computeFilesize, fpath,
                validSubCountsRequiredFlag);
        }
        if (res < 0) {
            return res;
        }
    }
    if (fa) {
        kfsattr          = *fa;
        kfsattr.filename = fa->fidNameIt->first.second;
    }
    if (cattr) {
        *cattr = fa;
    }
    KFS_LOG_STREAM_DEBUG <<
        pathname << ": size: " << kfsattr.fileSize <<
    KFS_LOG_EOM;
    return 0;
}

int
KfsClientImpl::GetNumChunks(const char *pathname)
{
    QCStMutexLocker l(mMutex);

    KfsFileAttr attr;
    string      path;
    const int res = StatSelf(pathname, attr, false);
    if (res != 0) {
        return (res < 0 ? res : -res);
    }
    if (attr.isDirectory) {
        return -EISDIR;
    }
    return attr.chunkCount();
}


bool
KfsClientImpl::Exists(const char *pathname)
{
    KfsFileAttr attr;
    return (Stat(pathname, attr, false) == 0);
}

bool
KfsClientImpl::IsFile(const char *pathname)
{
    KfsFileAttr attr;
    return (Stat(pathname, attr, false) == 0 && ! attr.isDirectory);
}

bool
KfsClientImpl::IsDirectory(const char *pathname)
{
    KfsFileAttr attr;
    return (Stat(pathname, attr, false) == 0 && attr.isDirectory);
}

int
KfsClientImpl::LookupAttr(kfsFileId_t parentFid, const string& filename,
    KfsClientImpl::FAttr*& fa, bool computeFilesize, const string& path,
    bool validSubCountsRequiredFlag)
{
    assert(mMutex.IsOwned());

    if (parentFid < 0) {
        return -EINVAL;
    }
    if (! fa) {
        fa = LookupFAttr(path, 0);
        if (fa && (! validSubCountsRequiredFlag || ! fa->staleSubCountsFlag) &&
                (! computeFilesize || fa->isDirectory || fa->fileSize >= 0) &&
                IsValid(*fa, time(0))) {
            return 0;
        }
    }
    LookupOp op(0, parentFid, filename.c_str());
    DoMetaOpWithRetry(&op);
    if (op.status < 0) {
        Delete(fa);
        fa = 0;
        return op.status;
    }
    const time_t now = time(0);
    UpdateUserAndGroup(op, now);
    if (! op.fattr.isDirectory && computeFilesize && op.fattr.fileSize < 0) {
        op.fattr.fileSize = ComputeFilesize(op.fattr.fileId);
        if (op.fattr.fileSize < 0) {
            // We are asked for filesize and if we can't compute it, fail
            return -EIO;
        }
    }
    if (! fa) {
        fa = LookupFAttr(parentFid, filename);
    }
    return UpdateFattr(parentFid, filename, fa, path, op.fattr, now);
}

int
KfsClientImpl::Create(const char *pathname, int numReplicas, bool exclusive,
    int numStripes, int numRecoveryStripes, int stripeSize, int stripedType,
    bool forceTypeFlag, kfsMode_t mode, kfsSTier_t minSTier, kfsSTier_t maxSTier)
{
    QCStMutexLocker l(mMutex);
    return CreateSelf(pathname, numReplicas, exclusive,
        numStripes, numRecoveryStripes, stripeSize, stripedType, forceTypeFlag,
        mode, minSTier, maxSTier);
}

int
KfsClientImpl::CreateSelf(const char *pathname, int numReplicas, bool exclusive,
    int numStripes, int numRecoveryStripes, int stripeSize, int stripedType,
    bool forceTypeFlag, kfsMode_t mode, kfsSTier_t minSTier, kfsSTier_t maxSTier)
{
    if (! pathname || ! *pathname) {
        return -EINVAL;
    }

    assert(mMutex.IsOwned());
    int res = ValidateCreateParams(numReplicas, numStripes, numRecoveryStripes,
        stripeSize, stripedType, minSTier, maxSTier);
    if (res < 0) {
        return res;
    }
    kfsFileId_t parentFid;
    string      filename;
    string      path;
    const bool  kInvalidateSubCountsFlag = true;
    res = GetPathComponents(pathname, &parentFid, filename, &path,
        kInvalidateSubCountsFlag);
    Delete(LookupFAttr(parentFid, filename));
    if (res < 0) {
        KFS_LOG_STREAM_DEBUG <<
            pathname << ": GetPathComponents: " << res <<
        KFS_LOG_EOM;
        return res;
    }
    CreateOp op(0, parentFid, filename.c_str(), numReplicas, exclusive,
        Permissions(
            mUseOsUserAndGroupFlag ? mEUser  : kKfsUserNone,
            mUseOsUserAndGroupFlag ? mEGroup : kKfsGroupNone,
            mode != kKfsModeUndef ? (mode & ~mUMask) : mode
        ),
        exclusive ? NextCreateId() : -1,
        minSTier, maxSTier
    );
    if (stripedType != KFS_STRIPED_FILE_TYPE_NONE) {
        string errMsg;
        if (! RSStriperValidate(stripedType, numStripes,
                numRecoveryStripes, stripeSize, &errMsg)) {
            KFS_LOG_STREAM_ERROR <<
                pathname << ": " <<
                (errMsg.empty() ? string("invalid parameters") : errMsg) <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        op.striperType        = stripedType;
        op.numStripes         = numStripes;
        op.numRecoveryStripes = numRecoveryStripes;
        op.stripeSize         = stripeSize;
    }
    DoMetaOpWithRetry(&op);
    if (op.status < 0) {
        KFS_LOG_STREAM_ERROR <<
            pathname << ": create: " << op.status << " " << op.statusMsg <<
        KFS_LOG_EOM;
        return op.status;
    }
    if (op.striperType != op.metaStriperType && forceTypeFlag) {
        KFS_LOG_STREAM_ERROR <<
            pathname << ": create: " << "striped file type " << op.striperType <<
            " is not supported " << " got: " << op.metaStriperType <<
        KFS_LOG_EOM;
        // Cleanup the file.
        RemoveOp rm(0, parentFid, filename.c_str(), pathname);
        DoMetaOpWithRetry(&rm);
        return -ENXIO;
    }

    // Do not attempt to re-use possibly existing file table entry.
    // If file existed and being written into it is moved into the dumpster by
    // the meta server.
    // An attempt to re-use the same file table entry would route the ios to the
    // previously existed file into newly created one.
    const int fte = AllocFileTableEntry(parentFid, filename, path);
    if (fte < 0) {      // XXX Too many open files
        KFS_LOG_STREAM_DEBUG <<
            pathname << ": AllocFileTableEntry: " << fte <<
        KFS_LOG_EOM;
        return fte;
    }

    // make it the same as creat(): equivalent to open(O_CREAT|O_WRONLY|O_TRUNC).
    FileTableEntry& entry = *mFileTable[fte];
    entry.openMode        = O_WRONLY;
    FileAttr& fa = entry.fattr;
    fa.Init(false);    // is an ordinary file
    fa.fileId      = op.fileId;
    fa.numReplicas = numReplicas;
    fa.fileSize    = 0; // presently CreateOp always deletes file if exists.
    fa.minSTier    = op.minSTier;
    fa.maxSTier    = op.maxSTier;
    if (op.metaStriperType != KFS_STRIPED_FILE_TYPE_NONE) {
        fa.numStripes         = (int16_t)numStripes;
        fa.numRecoveryStripes = (int16_t)numRecoveryStripes;
        fa.striperType        = (StripedFileType)stripedType;
        fa.stripeSize         = stripeSize;
    }
    static_cast<Permissions&>(fa) = op.permissions;
    time_t now = 0;
    if (! op.userName.empty()) {
        now = time(0);
        UpdateUserId(op.userName, fa.user, now);
    }
    if (! op.groupName.empty()) {
        if (op.userName.empty()) {
            now = time(0);
        }
        UpdateGroupId(op.groupName, fa.group, now);
    }
    // Set optimal io size, like open does.
    SetOptimalReadAheadSize(entry, mDefaultReadAheadSize);
    SetOptimalIoBufferSize(entry, mDefaultIoBufferSize);
    KFS_LOG_STREAM_DEBUG <<
        "created:"
        " fd: "       << fte <<
        " fileId: "   << entry.fattr.fileId <<
        " instance: " << entry.instance <<
        " mode: "     << entry.openMode <<
        " striper: "  << fa.striperType <<
    KFS_LOG_EOM;

    return fte;
}

int
KfsClientImpl::Remove(const char* pathname)
{
    if (! pathname || ! *pathname) {
        return -EINVAL;
    }

    QCStMutexLocker l(mMutex);

    kfsFileId_t parentFid;
    string      filename;
    string      path;
    const bool  kInvalidateSubCountsFlag = true;
    int res = GetPathComponents(pathname, &parentFid, filename, &path,
        kInvalidateSubCountsFlag);
    if (res < 0) {
        return res;
    }
    RemoveOp op(0, parentFid, filename.c_str(), path.c_str());
    DoMetaOpWithRetry(&op);
    Delete(LookupFAttr(parentFid, filename));
    return op.status;
}

bool
KfsClientImpl::InvalidateCachedAttrsWithPathPrefix(
    const string&               path,
    const KfsClientImpl::FAttr* faDoNotDelete)
{
    if (mPathCache.size() <= 1) {
        assert(mPathCache.begin() == mPathCacheNone);
        return true;
    }
    const size_t len = path.length();
    if (len == 0 || path == "/") {
        InvalidateAllCachedAttrs();
        return true;
    }
    if (path[0] != '/') {
        return false;
    }
    string prefix = path;
    if (prefix[len - 1] != '/') {
        prefix += "/";
    }
    int maxInval = (int)min(size_t(256), mFidNameToFAttrMap.size() / 2 + 1);
    for (NameToFAttrMap::iterator it = mPathCache.lower_bound(prefix);
            it != mPathCache.end();
            ) {
        const string& cp = it->first;
        if (cp.length() < len || cp.compare(0, len, prefix) != 0) {
            break;
        }
        FAttr* const fa = it->second;
        if (--maxInval < 0) {
            InvalidateAllCachedAttrs();
            return true;
        }
        ++it;
        if (fa == faDoNotDelete) {
            if (fa->generation == mFAttrCacheGeneration) {
                fa->generation--;
            }
        } else {
            Delete(fa);
        }
    }
    return false;
}

int
KfsClientImpl::Rename(const char* src, const char* dst, bool overwrite)
{
    if (! src || ! *src || ! dst || ! *dst) {
        return -EINVAL;
    }

    QCStMutexLocker l(mMutex);

    kfsFileId_t srcParentFid;
    string      srcFileName;
    string      srcPath;
    const bool  kInvalidateSubCountsFlag = true;
    int res = GetPathComponents(src, &srcParentFid, srcFileName, &srcPath,
        kInvalidateSubCountsFlag);
    if (res < 0) {
        KFS_LOG_STREAM_DEBUG << "reanme: " <<
            src << " " << dst << " status: " << res <<
        KFS_LOG_EOM;
        return res;
    }
    string      dstFileName;
    string      dstPath;
    kfsFileId_t dstParentFid;
    res = GetPathComponents(dst, &dstParentFid, dstFileName, &dstPath,
        kInvalidateSubCountsFlag);
    if (res < 0) {
        KFS_LOG_STREAM_DEBUG << "reanme: " <<
            src << " " << dst << " status: " << res <<
        KFS_LOG_EOM;
        return res;
    }
    if (srcParentFid == dstParentFid && dstFileName == srcFileName) {
        return 0; // src and dst are the same.
    }
    RenameOp op(0, srcParentFid, srcFileName.c_str(),
                dstPath.c_str(), srcPath.c_str(), overwrite);
    DoMetaOpWithRetry(&op);

    KFS_LOG_STREAM_DEBUG << "reanme: " <<
        srcPath << " " << dstPath << " status: " << op.status <<
    KFS_LOG_EOM;

    // Invalidate file attribute and the path cache
    bool invalidateFlag = true;
    for (string* pp = &srcPath; ; pp = &dstPath) {
        string& path = *pp;
        if (path.empty()) {
            continue;
        }
        if (*path.rbegin() != '/') {
            path += "/";
        }
        if (InvalidateCachedAttrsWithPathPrefix(path, 0)) {
            invalidateFlag = false;
            break;
        }
        if (pp == &dstPath) {
            break;
        }
    }
    if (invalidateFlag) {
        Delete(LookupFAttr(srcParentFid, srcFileName));
        Delete(LookupFAttr(dstParentFid, dstFileName));
    }
    return op.status;
}

int
KfsClientImpl::UpdateFattr(
    kfsFileId_t            parentFid,
    const string&          name,
    KfsClientImpl::FAttr*& fa,
    const string&          path,
    const FileAttr&        fattr,
    time_t                 now,
    bool                   copyPathFlag)
{
    Validate(fa);
    if (fa) {
        if (fattr.fileId != fa->fileId &&
                (fa->isDirectory || fattr.isDirectory)) {
            const bool invalPathFlag = fa->nameIt->first != path;
            if ((! fa->isDirectory || ! InvalidateCachedAttrsWithPathPrefix(
                    fa->nameIt->first, fa)) && invalPathFlag &&
                    fattr.isDirectory) {
                InvalidateCachedAttrsWithPathPrefix(path, fa);
            }
        }
        UpdatePath(fa, path, copyPathFlag);
        FAttrLru::PushBack(mFAttrLru, *fa);
    } else if (! (fa = NewFAttr(parentFid, name,
            copyPathFlag ? path : string(path.data(), path.size())))) {
        return -ENOMEM;
    }
    fa->validatedTime      = now;
    fa->generation         = mFAttrCacheGeneration;
    fa->staleSubCountsFlag = false;
    *fa                    = fattr;
    return 0;
}

void
KfsClientImpl::InvalidateAttribute(const string& pathname,
    bool countsOnlyFlag, bool deleteAttrFlag)
{
    string pathstr(pathname);
    kfsFileId_t parentFid;
    string      name;
    bool        startFlag = true;
    for (size_t len = pathstr.length(); len > 0;  pathstr.erase(len)) {
        size_t            pos = len - 1;
        const char* const p   = pathstr.c_str();
        while (pos > 0 && p[pos] == '/') {
            --pos;
        }
        if (pos < len - 1) {
            pathstr.erase(pos);
        }
        len = pathstr.rfind('/');
        if (len == string::npos) {
            break;
        }
        if (len == 0 && pos > 0) {
            len++;
        }
        const bool deleteFlag = ! countsOnlyFlag ||
            (startFlag && deleteAttrFlag);
        startFlag = false;
        NameToFAttrMap::iterator const it = mPathCache.find(pathstr);
        if (it != mPathCache.end()) {
            if (deleteFlag) {
                Delete(it->second);
            } else {
                it->second->staleSubCountsFlag = true;
            }
            continue;
        }
        if (GetPathComponents(pathstr.c_str(), &parentFid, name) != 0) {
            continue;
        }
        FAttr* const fa = LookupFAttr(parentFid, name);
        if (! fa) {
            continue;
        }
        if (deleteFlag) {
            Delete(fa);
        } else {
            fa->staleSubCountsFlag = true;
        }
    }
}

int
KfsClientImpl::CoalesceBlocks(const char* src, const char* dst, chunkOff_t *dstStartOffset)
{
    if (! src || ! dst || ! *src || ! *dst) {
        return -EINVAL;
    }

    QCStMutexLocker l(mMutex);

    kfsFileId_t srcParentFid;
    string      srcFileName;
    string      srcPath;
    const bool  kInvalidateSubCountsFlag = true;
    int res = GetPathComponents(src, &srcParentFid, srcFileName, &srcPath,
        kInvalidateSubCountsFlag);
    if (res < 0) {
        KFS_LOG_STREAM_DEBUG << "coalesce: " <<
            src << " " << dst << " status: " << res <<
        KFS_LOG_EOM;
        return res;
    }
    string      dstFileName;
    string      dstPath;
    kfsFileId_t dstParentFid;
    res = GetPathComponents(dst, &dstParentFid, dstFileName, &dstPath,
        kInvalidateSubCountsFlag);
    if (res < 0) {
        KFS_LOG_STREAM_DEBUG << "coalesce: " <<
            src << " " << dst << " status: " << res <<
        KFS_LOG_EOM;
        return res;
    }
    if (srcParentFid == dstParentFid && dstFileName == srcFileName) {
        return 0; // src and dst are the same.
    }
    CoalesceBlocksOp op(0, srcPath.c_str(), dstPath.c_str());
    DoMetaOpWithRetry(&op);
    if (dstStartOffset) {
        *dstStartOffset = op.dstStartOffset;
    }
    Delete(LookupFAttr(srcParentFid, srcFileName));
    Delete(LookupFAttr(dstParentFid, dstFileName));
    return op.status;
}

int
KfsClientImpl::SetMtime(const char *pathname, const struct timeval &mtime)
{
    if (! pathname || ! *pathname) {
        return -EINVAL;
    }

    QCStMutexLocker l(mMutex);

    kfsFileId_t parentFid;
    string      fileName;
    string      path;
    const int res = GetPathComponents(pathname, &parentFid, fileName, &path);
    if (res < 0) {
        return res;
    }
    SetMtimeOp op(0, path.c_str(), mtime);
    DoMetaOpWithRetry(&op);
    Delete(LookupFAttr(parentFid, fileName));
    return op.status;
}

int
KfsClientImpl::OpenDirectory(const char *pathname)
{
    if (! pathname || ! *pathname) {
        return -EINVAL;
    }

    QCStMutexLocker l(mMutex);

    string path;
    const bool kCacheAttributesFlag = false;
    const int fd = OpenSelf(pathname, O_RDONLY,
        3, 0, 0, 0, KFS_STRIPED_FILE_TYPE_NONE, kKfsSTierMax, kKfsSTierMax,
        kCacheAttributesFlag, kKfsModeUndef, &path);
    if (fd < 0) {
        return fd;
    }
    FileTableEntry& entry = *(mFileTable[fd]);
    if (! entry.fattr.isDirectory) {
        ReleaseFileTableEntry(fd);
        return -ENOTDIR;
    }
    assert(! entry.dirEntries);
    entry.dirEntries = new vector<KfsFileAttr>();
    const int res = ReaddirPlus(path, entry.fattr.fileId, *entry.dirEntries);
    if (res < 0) {
        Close(fd);
        return res;
    }
    return fd;
}

static inline int WriteInt16(int8_t* ptr, int16_t val)
{
    ptr[1] = (int8_t)val;
    val >>= 8;
    ptr[0] = (int8_t)val;
    return 2;
}

static inline int WriteInt32(int8_t* ptr, int32_t val)
{
    ptr[3] = (int8_t)val;
    val >>= 8;
    ptr[2] = (int8_t)val;
    val >>= 8;
    ptr[1] = (int8_t)val;
    val >>= 8;
    ptr[0] = (int8_t)val;
    return 4;
}

static inline int WriteInt64(int8_t* ptr, int64_t val)
{
    WriteInt32(ptr,     (int32_t)(val >> 32));
    WriteInt32(ptr + 4, (int32_t)val);
    return 8;
}

int
KfsClientImpl::ReadDirectory(int fd, char* buf, size_t numBytes)
{
    if (! valid_fd(fd)) {
        return -EBADF;
    }
    FileTableEntry& entry = *(mFileTable[fd]);
    if (! entry.fattr.isDirectory) {
        return -ENOTDIR;
    }
    if (! entry.dirEntries) {
        return -EISDIR; // not opened with OpenDirectory().
    }
    const vector<KfsFileAttr>& dirEntries = *entry.dirEntries;
    if (entry.currPos.fileOffset < 0) {
        entry.currPos.fileOffset = 0;
    }
    int8_t*       ptr = (int8_t*)buf;
    int8_t* const end = ptr + numBytes;
    kfsUid_t      uid = kKfsUserNone;
    kfsGid_t      gid = kKfsGroupNone;
    time_t        now = time(0);
    string        uname;
    string        gname;
    for ( ; (size_t)entry.currPos.fileOffset < dirEntries.size();
                entry.currPos.fileOffset++) {
        const KfsFileAttr& attr = dirEntries[entry.currPos.fileOffset];
        int32_t unameLen;
        if (attr.user != uid) {
            uid      = attr.user;
            uname    = UidToName(uid, now);
            unameLen = (int32_t)uname.length();
        } else {
            unameLen = 0;
        }
        int32_t gnameLen;
        if (attr.group != gid) {
            gid      = attr.group;
            gname    = GidToName(gid, now);
            gnameLen = (int32_t)gname.length();
        } else {
            gnameLen = 0;
        }
        const int32_t nameLen   = (int32_t)attr.filename.length();
        const size_t  entrySize =
            (64 + 64 + 32 + 32 + 8 + 32 * 6 + 16 + 32 * 2 + 64) / 8 +
            nameLen + unameLen + gnameLen +
            (attr.isDirectory ? 2 * 64/8 : 0);
        if (nameLen <= 0) {
            continue;
        }
        if (ptr + entrySize > end) {
            break;
        }
        ptr += WriteInt64(ptr, (int64_t)attr.mtime.tv_sec * 1000 +
            attr.mtime.tv_usec / 1000);
        ptr += WriteInt64(ptr, attr.fileSize);
        ptr += WriteInt32(ptr, attr.numReplicas);
        ptr += WriteInt32(ptr, nameLen);
        *ptr++ = (int8_t)(attr.isDirectory ? 1 : 0);
        ptr += WriteInt32(ptr, attr.numStripes);
        ptr += WriteInt32(ptr, attr.numRecoveryStripes);
        ptr += WriteInt32(ptr, attr.striperType);
        ptr += WriteInt32(ptr, attr.stripeSize);
        ptr += WriteInt32(ptr, attr.user);
        ptr += WriteInt32(ptr, attr.group);
        ptr += WriteInt16(ptr, attr.mode);
        ptr += WriteInt64(ptr, attr.fileId);
        if (attr.isDirectory) {
            ptr += WriteInt64(ptr, attr.fileCount());
            ptr += WriteInt64(ptr, attr.dirCount());
        }
        ptr += WriteInt32(ptr, unameLen);
        ptr += WriteInt32(ptr, gnameLen);
        memcpy(ptr, attr.filename.data(), nameLen);
        ptr += nameLen;
        memcpy(ptr, uname.data(), unameLen);
        ptr += unameLen;
        memcpy(ptr, gname.data(), gnameLen);
        ptr += gnameLen;
    }
    if (ptr <= (int8_t*)buf && (size_t)entry.currPos.fileOffset <
            dirEntries.size()) {
        return -EINVAL; // buffer too small.
    }
    return (int)(ptr - (int8_t*)buf);
}

int
KfsClientImpl::Open(const char *pathname, int openMode, int numReplicas,
    int numStripes, int numRecoveryStripes, int stripeSize, int stripedType,
    kfsMode_t mode, kfsSTier_t minSTier, kfsSTier_t maxSTier)
{
    QCStMutexLocker l(mMutex);
    const bool kCacheAttributesFlag = false;
    return OpenSelf(pathname, openMode, numReplicas,
        numStripes, numRecoveryStripes, stripeSize, stripedType,
        minSTier, maxSTier, kCacheAttributesFlag, mode);
}

int
KfsClientImpl::CacheAttributes(const char *pathname)
{
    const bool kCacheAttributesFlag = true;
    return OpenSelf(pathname, 0, 0, 0, 0, 0, KFS_STRIPED_FILE_TYPE_NONE,
        kKfsSTierMax, kKfsSTierMax, kCacheAttributesFlag);
}

inline bool
CheckAccess(int openMode, kfsUid_t euser, kfsGid_t egroup, const FileAttr& fattr)
{
    return (
        ! fattr.IsAnyPermissionDefined() ||
        (((openMode & (O_WRONLY | O_RDWR | O_APPEND)) == 0 ||
            fattr.CanWrite(euser, egroup)) &&
        ((openMode != O_RDONLY && openMode != O_RDWR) ||
            fattr.CanRead(euser, egroup)))
    );
}

int
KfsClientImpl::OpenSelf(const char *pathname, int openMode, int numReplicas,
    int numStripes, int numRecoveryStripes, int stripeSize, int stripedType,
    kfsSTier_t minSTier, kfsSTier_t maxSTier,
    bool cacheAttributesFlag, kfsMode_t mode, string* path)
{
    if ((openMode & O_TRUNC) != 0 &&
            (openMode & (O_RDWR | O_WRONLY | O_APPEND)) == 0) {
        return -EINVAL;
    }

    kfsFileId_t parentFid = -1;
    string      filename;
    string      fpath;
    const int res = GetPathComponents(pathname, &parentFid, filename, &fpath);
    if (res < 0) {
        return res;
    }
    assert(! fpath.empty() && *fpath.begin() == '/' &&
        filename != "." && filename != "..");
    if (path) {
        *path = fpath;
    }
    LookupOp     op(0, parentFid, filename.c_str());
    FAttr*       fa                        = LookupFAttr(parentFid, filename);
    time_t const now                       = time(0);
    time_t const kShortenFileRevalidateSec = 8;
    if (fa && IsValid(*fa, now +
                (fa->isDirectory ? time_t(0) : kShortenFileRevalidateSec)) &&
            (fa->isDirectory || fa->fileSize > 0 ||
                (fa->fileSize == 0 && fa->chunkCount() <= 0)) &&
            (! mUseOsUserAndGroupFlag ||
                CheckAccess(openMode, mEUser, mEGroup, *fa))) {
        UpdatePath(fa, fpath);
        op.fattr = *fa;
    } else {
        if (openMode == O_RDONLY) {
            op.status = LookupSelf(op, parentFid, filename, fa, now, fpath);
        } else {
            if (fa) {
                Delete(fa);
                fa = 0;
            }
            DoMetaOpWithRetry(&op);
            UpdateUserAndGroup(op, now);
        }
        if (op.status < 0) {
            if (! cacheAttributesFlag && (openMode & O_CREAT) != 0 &&
                    op.status == -ENOENT) {
                // file doesn't exist.  Create it
                const int fte = CreateSelf(pathname, numReplicas,
                    openMode & O_EXCL,
                    numStripes, numRecoveryStripes, stripeSize, stripedType,
                    false, mode, minSTier, maxSTier);
                if (fte >= 0 && (openMode & O_APPEND) != 0) {
                    FileTableEntry& entry = *mFileTable[fte];
                    assert(! entry.fattr.isDirectory);
                    entry.openMode |= O_APPEND;
                }
                return fte;
            }
            return op.status;
        }
        if (mUseOsUserAndGroupFlag && ! CheckAccess(
                openMode,
                op.euser == kKfsUserNone ? mEUser : op.euser,
                op.egroup,
                op.fattr)) {
            KFS_LOG_STREAM_DEBUG <<
                "permission denied:"
                " fileId: "  << op.fattr.fileId <<
                " euser: "   << op.euser <<
                " egroup: "  << op.egroup <<
                " mode: "    << oct << op.fattr.mode << dec <<
                " user: "    << op.fattr.user <<
                " group: "   << op.fattr.group <<
            KFS_LOG_EOM;
            return -EACCES;
        }
        if (fa && openMode == O_RDONLY &&
                ! fa->isDirectory && 0 <= mFileAttributeRevalidateTime) {
            // Update the validate time in case if the meta server lookup took
            // non trivial amount of time, to make sure that the attribute is
            // still valid after open returns.
            fa->validatedTime = time(0);
        }
    }
    const FileAttr& fattr = op.fattr;
    // file exists; now fail open if: O_CREAT | O_EXCL
    if ((openMode & (O_CREAT|O_EXCL)) == (O_CREAT|O_EXCL)) {
        return -EEXIST;
    }
    if (fattr.isDirectory && openMode != O_RDONLY) {
        return -EISDIR;
    }

    const int fte = AllocFileTableEntry(parentFid, filename, fpath);
    if (fte < 0) { // Too many open files
        return fte;
    }

    FileTableEntry& entry = *mFileTable[fte];
    if (cacheAttributesFlag) {
        entry.openMode = 0;
    } else if ((openMode & O_RDWR) != 0) {
        entry.openMode = openMode & (O_RDWR | O_APPEND);
    } else if ((openMode & O_WRONLY) != 0) {
        entry.openMode = openMode & (O_WRONLY | O_APPEND);
    } else if ((openMode & O_APPEND) != 0) {
        entry.openMode = O_APPEND;
    } else if ((openMode & O_RDONLY) != 0) {
        entry.openMode = O_RDONLY;
    } else {
        entry.openMode = 0;
    }
    entry.fattr = fattr;
    const bool truncateFlag =
        ! cacheAttributesFlag && (openMode & O_TRUNC) != 0;
    if (truncateFlag) {
        if (entry.fattr.chunkCount() > 0 || entry.fattr.fileSize != 0) {
            const int res = TruncateSelf(fte, 0);
            if (res < 0) {
                ReleaseFileTableEntry(fte);
                Delete(fa);
                return res;
            }
        }
    } else if (entry.fattr.fileSize < 0 &&
            ! entry.fattr.isDirectory && entry.fattr.chunkCount() > 0) {
        entry.fattr.fileSize = ComputeFilesize(fattr.fileId);
        if (entry.fattr.fileSize < 0) {
            ReleaseFileTableEntry(fte);
            return -EIO;
        }
        // Update attribute cache file size.
        if (fa && entry.openMode == O_RDONLY) {
            fa->fileSize = entry.fattr.fileSize;
        }
    }
    if (! entry.fattr.isDirectory) {
        SetOptimalIoBufferSize(entry, mDefaultIoBufferSize);
        SetOptimalReadAheadSize(entry, mDefaultReadAheadSize);
        if (fa && entry.openMode != O_RDONLY) {
            Delete(fa); // Invalidate attribute cache entry if isn't read only.
        }
    }
    KFS_LOG_STREAM_DEBUG <<
        "opened:"
        " fd: "       << fte <<
        " fileId: "   << entry.fattr.fileId <<
        " instance: " << entry.instance <<
        " mode: "     << entry.openMode <<
    KFS_LOG_EOM;
    return fte;
}

int
KfsClientImpl::Close(int fd)
{
    KfsProtocolWorker::FileInstance fileInstance;
    KfsProtocolWorker::FileId       fileId;
    KfsProtocolWorker::RequestType  closeType;
    bool                            readCloseFlag;
    bool                            writeCloseFlag;
    int                             status = 0;
    {
        QCStMutexLocker l(mMutex);

        if (! valid_fd(fd)) {
            KFS_LOG_STREAM_DEBUG << "close: invalid fd: " << fd << KFS_LOG_EOM;
            return -EBADF;
        }
        FileTableEntry& entry = *mFileTable[fd];
        closeType      = (entry.openMode & O_APPEND) != 0 ?
            KfsProtocolWorker::kRequestTypeWriteAppendClose :
            KfsProtocolWorker::kRequestTypeWriteClose;
        fileId         = entry.fattr.fileId;
        fileInstance   = entry.instance;
        readCloseFlag  = entry.readUsedProtocolWorkerFlag && mProtocolWorker;
        writeCloseFlag = entry.usedProtocolWorkerFlag && mProtocolWorker;
        KFS_LOG_STREAM_DEBUG <<
            "closing:"
            " fd: "       << fd <<
            " fileId: "   << fileId <<
            " instance: " << fileInstance <<
            " read: "     << readCloseFlag <<
            " write: "    << writeCloseFlag <<
        KFS_LOG_EOM;
        if (writeCloseFlag) {
            // Invalidate the corresponding attribute if any.
            InvalidateAttributeAndCounts(entry.pathname);
            Delete(LookupFAttr(entry.parentFid, entry.name));
        }
        ReleaseFileTableEntry(fd);
    }
    if (writeCloseFlag) {
        const int ret = (int)mProtocolWorker->Execute(
            closeType,
            fileInstance,
            fileId
        );
        if (status == 0) {
            status = ret;
        }
    }
    if (readCloseFlag) {
        const int ret = (int)mProtocolWorker->Execute(
            KfsProtocolWorker::kRequestTypeReadShutdown,
            fileInstance + 1, // reader's instance always +1
            fileId
        );
        if (! writeCloseFlag && ret != 0 && status == 0) {
            status = ret;
        }
    }
    KFS_LOG_STREAM_DEBUG <<
        "closed:"
        " fd: "       << fd <<
        " fileId: "   << fileId <<
        " instance: " << fileInstance <<
        " read: "     << readCloseFlag <<
        " write: "    << writeCloseFlag <<
        " status: "   << status <<
    KFS_LOG_EOM;
    return status;
}

void
KfsClientImpl::SkipHolesInFile(int fd)
{
    QCStMutexLocker l(mMutex);

    if (! valid_fd(fd)) {
        return;
    }
    FileTableEntry& entry = *(mFileTable[fd]);
    entry.skipHoles          = true;
    entry.failShortReadsFlag = false;
}

int
KfsClientImpl::Sync(int fd)
{
    QCStMutexLocker l(mMutex);

    if (! valid_fd(fd)) {
        return -EBADF;
    }
    FileTableEntry& entry = *mFileTable[fd];
    if (entry.pending > 0 &&
            mProtocolWorker && entry.usedProtocolWorkerFlag) {
        const KfsProtocolWorker::FileId       fileId       = entry.fattr.fileId;
        const KfsProtocolWorker::FileInstance fileInstance = entry.instance;
        entry.pending = 0;
        l.Unlock();
        return (int)mProtocolWorker->Execute(
            (entry.openMode & O_APPEND) != 0 ?
                KfsProtocolWorker::kRequestTypeWriteAppend :
                KfsProtocolWorker::kRequestTypeWrite,
            fileInstance,
            fileId,
            0,
            0,
            0
        );
    }
    return 0;
}

int
KfsClientImpl::Truncate(int fd, chunkOff_t offset)
{
    const int syncRes = Sync(fd);
    if (syncRes < 0) {
        return syncRes;
    }
    QCStMutexLocker l(mMutex);
    return TruncateSelf(fd, offset);
}

int
KfsClientImpl::Truncate(const char* pathname, chunkOff_t offset)
{
    QCStMutexLocker l(mMutex);

    KfsFileAttr attr;
    string      path;
    const int   res = StatSelf(pathname, attr, false, &path);
    if (res != 0) {
        return (res < 0 ? res : -res);
    }
    if (attr.isDirectory) {
        return -EISDIR;
    }
    TruncateOp op(0, path.c_str(), attr.fileId, offset);
    op.checkPermsFlag = true;
    op.setEofHintFlag = attr.numStripes > 1;
    DoMetaOpWithRetry(&op);
    if (op.status != 0) {
        return op.status;
    }
    InvalidateAttributeAndCounts(path);
    return 0;
}

int
KfsClientImpl::TruncateSelf(int fd, chunkOff_t offset)
{
    assert(mMutex.IsOwned());

    if (! valid_fd(fd)) {
        return -EBADF;
    }
    // for truncation, file should be opened for writing
    if ((mFileTable[fd]->openMode & (O_RDWR | O_WRONLY | O_APPEND)) == 0) {
        return -EINVAL;
    }
    FdInfo(fd)->buffer.Invalidate();

    FileAttr *fa = FdAttr(fd);
    TruncateOp op(0, FdInfo(fd)->pathname.c_str(), fa->fileId, offset);
    op.setEofHintFlag = fa->numStripes > 1;
    DoMetaOpWithRetry(&op);
    int res = op.status;

    if (res == 0) {
        fa->fileSize = offset;
        if (fa->fileSize == 0) {
            fa->subCount1 = 0;
        }
        // else
        // chunkcount is off...but, that is ok; it is never exposed to
        // the end-client.

        gettimeofday(&fa->mtime, 0);
    }
    return res;
}

int
KfsClientImpl::PruneFromHead(int fd, chunkOff_t offset)
{
    const int syncRes = Sync(fd);
    if (syncRes < 0) {
        return syncRes;
    }

    QCStMutexLocker l(mMutex);

    if (! valid_fd(fd)) {
        return -EBADF;
    }
    // for truncation, file should be opened for writing
    if (mFileTable[fd]->openMode == O_RDONLY) {
        return -EINVAL;
    }
    FdInfo(fd)->buffer.Invalidate();

    // round-down to the nearest chunk block start offset
    offset = (offset / CHUNKSIZE) * CHUNKSIZE;

    FileAttr *fa = FdAttr(fd);
    TruncateOp op(0, FdInfo(fd)->pathname.c_str(), fa->fileId, offset);
    op.pruneBlksFromHead = true;
    DoMetaOpWithRetry(&op);
    int res = op.status;

    if (res == 0) {
        // chunkcount is off...but, that is ok; it is never exposed to
        // the end-client.
        gettimeofday(&fa->mtime, 0);
    }
    return res;
}

int
KfsClientImpl::GetDataLocation(
    const char*                pathname,
    chunkOff_t                 start,
    chunkOff_t                 len,
    vector< vector <string> >& locations,
    chunkOff_t*                outBlkSize)
{
    QCStMutexLocker l(mMutex);

    // Open the file and cache the attributes
    const int fd = CacheAttributes(pathname);
    if (fd < 0) {
        return fd;
    }
    const int ret = GetDataLocationSelf(fd, start, len, locations, outBlkSize);
    ReleaseFileTableEntry(fd);
    return ret;
}

int
KfsClientImpl::GetDataLocation(int fd, chunkOff_t start, chunkOff_t len,
        vector< vector <string> >& locations, chunkOff_t* outBlkSize)
{
    QCStMutexLocker l(mMutex);
    return GetDataLocationSelf(fd, start, len, locations, outBlkSize);
}

int
KfsClientImpl::AddChunkLocation(
    int                      inFd,
    chunkOff_t               inChunkPos,
    bool                     inNewEntryFlag,
    vector<vector<string> >& inLocations)
{
    ChunkAttr theChunk;
    int       theRes;
    if ((theRes = LocateChunk(inFd, inChunkPos, theChunk)) < 0) {
        return theRes;
    }
    if (inLocations.empty() || inNewEntryFlag) {
        inLocations.push_back(vector<string>());
    }
    vector<string>& theHosts = inLocations.back();
    const size_t theCnt = theChunk.chunkServerLoc.size();
    for (size_t i = 0; i < theCnt; i++) {
        theHosts.push_back(string());
        string& str = theHosts.back();
        str.swap(theChunk.chunkServerLoc[i].hostname);
        str += ':';
        AppendDecIntToString(str, theChunk.chunkServerLoc[i].port);
    }
    return 0;
}

int
KfsClientImpl::GetDataLocationSelf(int fd, chunkOff_t start, chunkOff_t len,
    vector<vector<string> >& locations, chunkOff_t* outBlkSize)
{
    assert(mMutex.IsOwned());

    if (! valid_fd(fd)) {
        return -EBADF;
    }
    const FileAttr& fattr = mFileTable[fd]->fattr;
    if (fattr.isDirectory) {
        return -EISDIR;
    }
    if (fattr.striperType != KFS_STRIPED_FILE_TYPE_NONE &&
            0 < fattr.numStripes && 0 < fattr.stripeSize) {
        chunkOff_t const lblksz = (chunkOff_t)CHUNKSIZE * (int)fattr.numStripes;
        if (outBlkSize) {
            *outBlkSize = lblksz;
        }
        if (fattr.fileSize < start || len <= 0) {
            return 0;
        }
        chunkOff_t const pblksz = (chunkOff_t)CHUNKSIZE *
            (int)(fattr.numStripes + max(0, (int)fattr.numRecoveryStripes));
        chunkOff_t const fstart = max(chunkOff_t(0), start);
        chunkOff_t lbpos        = fstart / lblksz;
        chunkOff_t pbpos        = lbpos * pblksz;
        lbpos *= lblksz;
        int        idx          = (int)((fstart %
            (fattr.stripeSize * fattr.numStripes)) / fattr.stripeSize);
        chunkOff_t cpos         = pbpos + idx * (chunkOff_t)CHUNKSIZE;
        chunkOff_t pos          = fstart - fstart % fattr.stripeSize;
        chunkOff_t const end    = min(fstart + len, fattr.fileSize);
        chunkOff_t const locbsz = outBlkSize ? lblksz : (chunkOff_t)CHUNKSIZE;
        chunkOff_t inspos       = fstart - fstart % locbsz;
        chunkOff_t pinspos      = inspos - locbsz;
        KFS_LOG_STREAM_DEBUG <<
            "get data location:"
            " fid: "        << fattr.fileId <<
            " begin: "      << pos <<
            " end: "        << end <<
            " block size:"
            " logical: "    << lblksz <<
            " physical: "   << pblksz <<
            " location: "   << locbsz <<
        KFS_LOG_EOM;
        while (pos < end) {
            lbpos += lblksz;
            const int sidx = idx;
            do {
                const bool newEntryFlag = pinspos + locbsz <= inspos;
                if (newEntryFlag) {
                    pinspos = inspos;
                }
                const int res = AddChunkLocation(
                    fd, cpos, newEntryFlag, locations);
                if (res == -EAGAIN || res == -ENOENT) {
                    locations.push_back(vector<string>());
                } else if (res < 0) {
                    return res;
                }
                ++idx;
                if (fattr.numStripes <= idx) {
                    idx  = 0;
                    cpos = pbpos;
                } else {
                    cpos += (chunkOff_t)CHUNKSIZE;
                }
                pos += fattr.stripeSize;
                inspos += fattr.stripeSize;
            } while (idx != sidx && pos < end && pos < lbpos);
            if (end <= pos) {
                break;
            }
            assert(! locations.empty());
            inspos = max(inspos, lbpos - lblksz + locbsz);
            // Duplicating the last entry is rough approximation, sufficient for
            // location hints. It is possible to query precise location by
            // specifying smaller length and/or aligning start position.
            while (inspos < lbpos && inspos < end) {
                locations.push_back(locations.back());
                inspos += locbsz;
            }
            pinspos = inspos - locbsz;
            pbpos += pblksz;
            cpos = pbpos;
            idx = 0;
            pos = lbpos;
        }
    } else {
        if (outBlkSize) {
            *outBlkSize = (chunkOff_t)CHUNKSIZE;
        }
        if (fattr.fileSize < start || len <= 0) {
            return 0;
        }
        for (chunkOff_t pos = max(chunkOff_t(0), start) /
                        (chunkOff_t)CHUNKSIZE * (chunkOff_t)CHUNKSIZE,
                    end = min(start + len, fattr.fileSize);
                pos < end;
                pos += (chunkOff_t)CHUNKSIZE) {
            const int res = AddChunkLocation(fd, pos, true, locations);
            if (res == -EAGAIN || res == -ENOENT) {
                locations.push_back(vector<string>());
            } else if (res < 0) {
                return res;
            }
        }
    }
    return 0;
}

int
KfsClientImpl::GetReplicationFactor(const char *pathname)
{
    KfsFileAttr attr;
    const int res = Stat(pathname, attr, false);
    if (res != 0) {
        return (res < 0 ? res : -res);
    }
    if (attr.isDirectory) {
        return -EISDIR;
    }
    return attr.numReplicas;
}

int
KfsClientImpl::SetReplicationFactor(const char *pathname, int16_t numReplicas)
{
    QCStMutexLocker l(mMutex);

    KfsFileAttr attr;
    string      path;
    const int res = StatSelf(pathname, attr, false, &path);
    if (res != 0) {
        return (res < 0 ? res : -res);
    }
    if (attr.isDirectory) {
        return -EISDIR;
    }
    ChangeFileReplicationOp op(0, attr.fileId, numReplicas);
    DoMetaOpWithRetry(&op);
    InvalidateAttributeAndCounts(path);
    return (op.status <= 0 ? op.status : -op.status);
}

int
KfsClientImpl::SetStorageTierRange(
    const char *pathname, kfsSTier_t minSTier, kfsSTier_t maxSTier)
{
    QCStMutexLocker l(mMutex);

    KfsFileAttr attr;
    string      path;
    const int res = StatSelf(pathname, attr, false, &path);
    if (res != 0) {
        return (res < 0 ? res : -res);
    }
    ChangeFileReplicationOp op(0, attr.fileId, 0);
    if (0 <= minSTier) {
        op.minSTier = (kfsSTier_t)minSTier;
    }
    if (0 <= maxSTier) {
        op.maxSTier = (kfsSTier_t)maxSTier;
    }
    DoMetaOpWithRetry(&op);
    InvalidateAttributeAndCounts(path);
    return (op.status <= 0 ? op.status : -op.status);
}

void
KfsClientImpl::SetDefaultIOTimeout(int nsecs)
{
    QCStMutexLocker l(mMutex);
    const int kMaxTimeout = numeric_limits<int>::max() / 1000;
    const int timeout = nsecs >= 0 ? min(kMaxTimeout, nsecs) : kMaxTimeout;
    if (timeout == mDefaultOpTimeout) {
        return;
    }
    mDefaultOpTimeout = timeout;
    if (mProtocolWorker) {
        mProtocolWorker->SetOpTimeoutSec(mDefaultOpTimeout);
    }
}

int
KfsClientImpl::GetDefaultIOTimeout() const
{
    QCStMutexLocker l(const_cast<KfsClientImpl*>(this)->mMutex);
    return mDefaultOpTimeout;
}

void
KfsClientImpl::SetDefaultMetaOpTimeout(int nsecs)
{
    QCStMutexLocker l(mMutex);
    const int kMaxTimeout = numeric_limits<int>::max() / 1000;
    const int timeout = nsecs >= 0 ? min(kMaxTimeout, nsecs) : kMaxTimeout;
    if (timeout == mDefaultMetaOpTimeout) {
        return;
    }
    mDefaultMetaOpTimeout = timeout;
    if (mProtocolWorker) {
        mProtocolWorker->SetMetaOpTimeoutSec(mDefaultMetaOpTimeout);
    }
}

int
KfsClientImpl::GetDefaultMetaOpTimeout() const
{
    QCStMutexLocker l(const_cast<KfsClientImpl*>(this)->mMutex);
    return mDefaultMetaOpTimeout;
}

void
KfsClientImpl::SetRetryDelay(int nsecs)
{
    QCStMutexLocker l(mMutex);
    if (mRetryDelaySec == nsecs) {
        return;
    }
    mRetryDelaySec = nsecs;
    if (mProtocolWorker) {
        mProtocolWorker->SetTimeSecBetweenRetries(mRetryDelaySec);
        mProtocolWorker->SetMetaTimeSecBetweenRetries(mRetryDelaySec);
    }
}

int
KfsClientImpl::GetRetryDelay() const
{
    QCStMutexLocker l(const_cast<KfsClientImpl*>(this)->mMutex);
    return mRetryDelaySec;
}

void
KfsClientImpl::SetMaxRetryPerOp(int retryCount)
{
    QCStMutexLocker l(mMutex);
    if (mMaxNumRetriesPerOp == retryCount) {
        return;
    }
    mMaxNumRetriesPerOp = retryCount;
    if (mProtocolWorker) {
        mProtocolWorker->SetMaxRetryCount(mMaxNumRetriesPerOp);
        mProtocolWorker->SetMetaMaxRetryCount(mMaxNumRetriesPerOp);
    }
}

void
KfsClientImpl::StartProtocolWorker()
{
    assert(mMutex.IsOwned());
    if (mProtocolWorker) {
        return;
    }
    KfsProtocolWorker::Parameters params;
    if (mProtocolWorkerAuthCtx.IsEnabled()) {
        params.mAuthContextPtr = &mProtocolWorkerAuthCtx;
    }
    // Make content length limit large enough to ensure backward compatibility
    // with the previous versions of the meta server that don't support
    // partial readdir and getalloc.
    params.mMaxMetaServerContentLength = 512 << 20;
    const int kChecksumBlockSize = (int)CHECKSUM_BLOCKSIZE;
    const int maxWriteSize       = mConfig.getValue(
        "client.maxWriteSize", -1);
    if (0 < maxWriteSize) {
        params.mMaxWriteSize = (maxWriteSize + kChecksumBlockSize - 1) /
            kChecksumBlockSize * kChecksumBlockSize;
    } else {
        params.mMaxWriteSize = mTargetDiskIoSize;
    }
    const int writeThreshold = mConfig.getValue(
        "client.randomWriteThreshold", -1);
    if (0 <= writeThreshold) {
        params.mRandomWriteThreshold = (writeThreshold + kChecksumBlockSize - 1) /
            kChecksumBlockSize * kChecksumBlockSize;
    } else if (0 < maxWriteSize) {
        params.mRandomWriteThreshold = params.mMaxWriteSize;
    }
    const int maxReadSize = mConfig.getValue("client.maxReadSize", -1);
    if (kChecksumBlockSize <= maxReadSize) {
        params.mMaxReadSize = (maxReadSize + kChecksumBlockSize - 1) /
            kChecksumBlockSize * kChecksumBlockSize;
    } else {
        params.mMaxReadSize = mTargetDiskIoSize;
    }
    params.mUseClientPoolFlag = mConfig.getValue(
        "client.connectionPool", params.mUseClientPoolFlag ? 1 : 0) != 0;
    mProtocolWorker = new KfsProtocolWorker(
        mMetaServerLoc.hostname,
        mMetaServerLoc.port,
        &params
    );
    mProtocolWorker->SetOpTimeoutSec(mDefaultOpTimeout);
    mProtocolWorker->SetMetaOpTimeoutSec(mDefaultMetaOpTimeout);
    mProtocolWorker->SetMaxRetryCount(mMaxNumRetriesPerOp);
    mProtocolWorker->SetMetaMaxRetryCount(mMaxNumRetriesPerOp);
    mProtocolWorker->SetTimeSecBetweenRetries(mRetryDelaySec);
    mProtocolWorker->SetMetaTimeSecBetweenRetries(mRetryDelaySec);
    mProtocolWorker->Start();
}

int
KfsClientImpl::GetMaxRetryPerOp() const
{
    QCStMutexLocker l(const_cast<KfsClientImpl*>(this)->mMutex);
    return mMaxNumRetriesPerOp;
}

void
KfsClientImpl::SetEOFMark(int fd, chunkOff_t offset)
{
    QCStMutexLocker l(mMutex);

    if (! valid_fd(fd) || FdAttr(fd)->isDirectory) {
        return;
    }
    FdInfo(fd)->eofMark = offset;
}

chunkOff_t
KfsClientImpl::Seek(int fd, chunkOff_t offset)
{
    return Seek(fd, offset, SEEK_SET);
}

chunkOff_t
KfsClientImpl::Seek(int fd, chunkOff_t offset, int whence)
{
    QCStMutexLocker l(mMutex);

    if (! valid_fd(fd)) {
        return -EBADF;
    }
    FileTableEntry& entry = *(mFileTable[fd]);
    if (entry.fattr.isDirectory) {
        return -EINVAL;
    }

    chunkOff_t newOff;
    switch (whence) {
    case SEEK_SET:
        newOff = offset;
        break;
    case SEEK_CUR:
        newOff = entry.currPos.fileOffset + offset;
        break;
    case SEEK_END:
        newOff = entry.fattr.fileSize + offset;
        break;
    default:
        return -EINVAL;
    }

    if (newOff < 0) {
        return -EINVAL;
    }
    entry.currPos.fileOffset = newOff;

    return newOff;
}

chunkOff_t
KfsClientImpl::Tell(int fd)
{
    QCStMutexLocker l(mMutex);

    if (! valid_fd(fd)) {
        return -EBADF;
    }
    FileTableEntry& entry = *(mFileTable[fd]);
    if (entry.fattr.isDirectory) {
        return -EINVAL;
    }

    return entry.currPos.fileOffset;
}

void
KfsClientImpl::SetMaxNumRetriesPerOp(int maxNumRetries)
{
    QCStMutexLocker l(mMutex);
    mMaxNumRetriesPerOp = maxNumRetries;
}

///
/// Given a chunk of file, find out where the chunk is hosted.
/// @param[in] fd  The index for an entry in mFileTable[] for which
/// we are trying find out chunk location info.
///
/// @param[in] chunkNum  The index in
/// mFileTable[fd]->cattr[] corresponding to the chunk for
/// which we are trying to get location info.
///
///
int
KfsClientImpl::LocateChunk(int fd, chunkOff_t chunkOffset, ChunkAttr& chunk)
{
    assert(mMutex.IsOwned() && valid_fd(fd) &&
        ! mFileTable[fd]->fattr.isDirectory);

    if (chunkOffset < 0) {
        return -EINVAL;
    }
    GetAllocOp op(0, mFileTable[fd]->fattr.fileId, chunkOffset);
    op.filename = mFileTable[fd]->pathname;
    DoMetaOpWithRetry(&op);
    if (op.status < 0) {
        KFS_LOG_STREAM_DEBUG <<
            "locate chunk failure: " << op.status <<
            " " <<  ErrorCodeToStr(op.status) <<
        KFS_LOG_EOM;
        return op.status;
    }
    chunk.chunkId        = op.chunkId;
    chunk.chunkVersion   = op.chunkVersion;
    chunk.chunkServerLoc.swap(op.chunkServers);
    chunk.chunkSize      = -1;
    chunk.chunkOffset    = chunkOffset;
    return 0;
}

ssize_t
KfsClientImpl::SetDefaultIoBufferSize(size_t size)
{
    QCStMutexLocker lock(mMutex);
    mDefaultIoBufferSize = min((size_t)numeric_limits<int>::max(),
        (size + CHECKSUM_BLOCKSIZE - 1) /
                CHECKSUM_BLOCKSIZE * CHECKSUM_BLOCKSIZE);
    return mDefaultIoBufferSize;
}

ssize_t
KfsClientImpl::GetDefaultIoBufferSize() const
{
    QCStMutexLocker lock(const_cast<KfsClientImpl*>(this)->mMutex);
    return mDefaultIoBufferSize;
}

ssize_t
KfsClientImpl::SetIoBufferSize(int fd, size_t size)
{
    QCStMutexLocker lock(mMutex);
    if (! valid_fd(fd)) {
        return -EBADF;
    }
    return SetIoBufferSize(*mFileTable[fd], size);
}

ssize_t
KfsClientImpl::SetIoBufferSize(FileTableEntry& entry, size_t size, bool optimalFlag)
{
    int bufSize = (int)min((size_t)numeric_limits<int>::max(),
        size / CHECKSUM_BLOCKSIZE * CHECKSUM_BLOCKSIZE);
    FileAttr& attr = entry.fattr;
    if (bufSize > 0 &&
            attr.striperType != KFS_STRIPED_FILE_TYPE_NONE &&
            attr.stripeSize > 0 &&
            attr.numStripes > 0 &&
            attr.stripeSize < bufSize) {
        const int stripes =
            attr.numStripes + max(0, int(attr.numRecoveryStripes));
        const int stride  = attr.stripeSize * stripes;
        bufSize = (max(optimalFlag ? mTargetDiskIoSize * stripes : 0, bufSize) +
            stride - 1) / stride * stride;
    }
    entry.ioBufferSize = max(0, bufSize);
    return entry.ioBufferSize;
}

ssize_t
KfsClientImpl::GetIoBufferSize(int fd) const
{
    QCStMutexLocker lock(const_cast<KfsClientImpl*>(this)->mMutex);
    if (! valid_fd(fd)) {
        return -EBADF;
    }
    return mFileTable[fd]->ioBufferSize;
}

ssize_t
KfsClientImpl::SetDefaultReadAheadSize(size_t size)
{
    QCStMutexLocker lock(mMutex);
    mDefaultReadAheadSize = (size + CHECKSUM_BLOCKSIZE - 1) /
                CHECKSUM_BLOCKSIZE * CHECKSUM_BLOCKSIZE;
    return mDefaultReadAheadSize;
}

ssize_t
KfsClientImpl::GetDefaultReadAheadSize() const
{
    QCStMutexLocker lock(const_cast<KfsClientImpl*>(this)->mMutex);
    return mDefaultReadAheadSize;
}

void
KfsClientImpl::SetDefaultFullSparseFileSupport(bool flag)
{
    QCStMutexLocker lock(mMutex);
    mFailShortReadsFlag = ! flag;
}

int
KfsClientImpl::SetFullSparseFileSupport(int fd, bool flag)
{
    QCStMutexLocker lock(mMutex);
    if (! valid_fd(fd)) {
        return -EBADF;
    }
    mFileTable[fd]->failShortReadsFlag = ! flag;
    return 0;
}

void
KfsClientImpl::SetFileAttributeRevalidateTime(int secs)
{
    QCStMutexLocker lock(mMutex);
    mFileAttributeRevalidateTime = secs;
}

///
/// To compute the size of a file, determine what the last chunk in
/// the file happens to be (from the meta server); then, for the last
/// chunk, find its size and then add the size of remaining chunks
/// (all of which are assumed to be full).  The reason for asking the
/// meta server about the last chunk (and simply using chunkCount) is
/// that random writes with seeks affect where the "last" chunk of the
/// file happens to be: for instance, a file could have chunkCount = 1, but
/// that chunk could be the 10th chunk in the file---the first 9
/// chunks are just holes.
//
struct RespondingServer
{
    KfsClientImpl&         client;
    const ChunkLayoutInfo& layout;
    int&                   status;
    chunkOff_t&            size;
    RespondingServer(KfsClientImpl& cli, const ChunkLayoutInfo& lay,
            chunkOff_t& sz, int& st)
        : client(cli), layout(lay), status(st), size(sz)
        {}
    bool operator() (ServerLocation loc)
    {
        bool usedLeaseLocationsFlag = false;
        size = client.GetChunkSize(
            loc, layout.chunkId, layout.chunkVersion, &usedLeaseLocationsFlag);
        if (size < 0) {
            status = (int)size;
            size = -1;
        } else {
            status = 0;
        }
        return (status == 0 || usedLeaseLocationsFlag);
    }
};

struct RespondingServer2
{
    KfsClientImpl&         client;
    const ChunkLayoutInfo& layout;
    RespondingServer2(KfsClientImpl& cli, const ChunkLayoutInfo& lay)
        : client(cli), layout(lay)
        {}
    ssize_t operator() (const ServerLocation& loc)
    {
        return client.GetChunkSize(loc, layout.chunkId, layout.chunkVersion);
    }
};

int
KfsClientImpl::UpdateFilesize(int fd)
{
    QCStMutexLocker l(mMutex);

    if (! valid_fd(fd)) {
        return -EBADF;
    }
    FileTableEntry& entry = *(mFileTable[fd]);
    FAttr* fa = LookupFAttr(entry.parentFid, entry.name);
    if (fa && fa->generation != mFAttrCacheGeneration) {
        Delete(fa);
        fa = 0;
    }
    if (entry.fattr.isDirectory ||
            entry.fattr.striperType != KFS_STRIPED_FILE_TYPE_NONE) {
        LookupOp op(0, entry.parentFid, entry.name.c_str());
        DoMetaOpWithRetry(&op);
        if (op.status < 0) {
            Delete(fa);
            return op.status;
        }
        const time_t now = time(0);
        UpdateUserAndGroup(op, now);
        if (op.fattr.fileId != entry.fattr.fileId ||
                op.fattr.isDirectory != entry.fattr.isDirectory) {
            Delete(fa);
            return 0; // File doesn't exists anymore, or in the dumpster.
        }
        entry.fattr = op.fattr;
        if (fa) {
            *fa                    = op.fattr;
            fa->validatedTime      = now;
            fa->generation         = mFAttrCacheGeneration;
            fa->staleSubCountsFlag = false;
            FAttrLru::PushBack(mFAttrLru, *fa);
        }
        if (entry.fattr.fileSize >= 0 || entry.fattr.isDirectory) {
            return 0;
        }
    }
    const chunkOff_t res = ComputeFilesize(entry.fattr.fileId);
    if (res < 0) {
        return (int)res;
    }
    FdAttr(fd)->fileSize = res;
    if (fa) {
        fa->fileSize = res;
    }
    return 0;
}

void
KfsClientImpl::GetLayout(GetLayoutOp& inOp)
{
    inOp.chunks.clear();
    inOp.maxChunks = 384;
    for (; ;) {
        inOp.contentLength = 0;
        DoMetaOpWithRetry(&inOp);
        if (inOp.status < 0) {
            break;
        }
        const bool kClearFlag = false;
        if (inOp.startOffset == 0 && ! inOp.hasMoreChunksFlag) {
            inOp.chunks.reserve(inOp.numChunks);
        }
        if (inOp.ParseLayoutInfo(kClearFlag)) {
            KFS_LOG_STREAM_ERROR <<
                "failed to parse layout info fid: " << inOp.fid <<
            KFS_LOG_EOM;
            inOp.status = -EINVAL;
            break;
        }
        if (! inOp.hasMoreChunksFlag) {
            break;
        }
        if (inOp.chunks.empty()) {
            break;
        }
        chunkOff_t const thePos = inOp.chunks.back().fileOffset;
        if (thePos < inOp.startOffset) {
            break;
        }
        inOp.startOffset = thePos + (chunkOff_t)CHUNKSIZE;
    }
    inOp.numChunks = (int)inOp.chunks.size();
}

chunkOff_t
KfsClientImpl::ComputeFilesize(kfsFileId_t kfsfid)
{
    GetLayoutOp lop(0, kfsfid);
    time_t       startTime = time(0);
    time_t const endTime   = startTime + mRetryDelaySec * mMaxNumRetriesPerOp;
    for (int retry = 0; ; retry++) {
        lop.lastChunkOnlyFlag = true;
        GetLayout(lop);
        if (mMaxNumRetriesPerOp <= retry ||
                (lop.status != -EAGAIN && lop.status != -EHOSTUNREACH)) {
            break;
        }
        time_t const curTime = time(0);
        if (endTime <= curTime) {
            break;
        }
        KFS_LOG_STREAM_INFO <<
            " compute file size:"
            " fid: "    << kfsfid <<
            " retry: "  << retry <<
            " status: " << lop.status <<
            " "         << lop.statusMsg <<
        KFS_LOG_EOM;
        startTime += mRetryDelaySec;
        if (curTime < startTime) {
            Sleep((int)(startTime - curTime));
        } else {
            startTime = curTime;
        }
        lop.status = 0;
        lop.statusMsg.clear();
    }
    if (lop.status < 0) {
        KFS_LOG_STREAM_ERROR <<
            "failed to compute file size:"
            " fid: "    << kfsfid <<
            " status: " << lop.status <<
            " "         << lop.statusMsg <<
        KFS_LOG_EOM;
        return lop.status;
    }
    if (lop.chunks.empty()) {
        return 0;
    }
    const ChunkLayoutInfo& last     = *lop.chunks.rbegin();
    chunkOff_t             filesize = last.fileOffset;
    chunkOff_t             endsize  = 0;
    int                    rstatus  = 0;
    if (last.chunkServers.empty()) {
        rstatus = -EAGAIN;
    } else if (find_if(last.chunkServers.begin(), last.chunkServers.end(),
                    RespondingServer(*this, last, endsize, rstatus)) ==
                last.chunkServers.end()) {
        KFS_LOG_STREAM_INFO <<
            "failed to connect to any server to get size of"
            " fid: "   << kfsfid <<
            " chunk: " << last.chunkId <<
            " max: "   << mMaxNumRetriesPerOp <<
        KFS_LOG_EOM;
    }
    if (rstatus < 0) {
        KFS_LOG_STREAM_ERROR <<
            "failed to get size for"
            " fid: "    << kfsfid <<
            " status: " << rstatus <<
        KFS_LOG_EOM;
        return -1;
    }

    if (filesize == 0 && endsize == 0 && ! lop.chunks.empty()) {
        // Make sure that the filesize is really 0: the file has one
        // chunk, but the size of that chunk is 0.  Sanity check with
        // all the servers that is really the case
        vector<ssize_t> chunksize;
        chunksize.resize(last.chunkServers.size(), -1);
        transform(last.chunkServers.begin(), last.chunkServers.end(),
            chunksize.begin(), RespondingServer2(*this, last));
        for (size_t i = 0; i < chunksize.size(); i++) {
            if (chunksize[i] > 0) {
                endsize = chunksize[i];
                break;
            }
        }
    }
    filesize += endsize;

    return filesize;
}

void
KfsClientImpl::ComputeFilesizes(vector<KfsFileAttr>& fattrs,
    const vector<ChunkAttr>& lastChunkInfo)
{
    const size_t cnt = lastChunkInfo.size();
    assert(cnt <= fattrs.size());
    for (size_t i = 0; i < cnt; i++) {
        KfsFileAttr& fa = fattrs[i];
        if (fa.isDirectory || fa.fileSize >= 0) {
            continue;
        }
        if (fa.chunkCount() == 0) {
            fa.fileSize = 0;
            continue;
        }
        const ChunkAttr& cattr  = lastChunkInfo[i];
        const size_t     locCnt = cattr.chunkServerLoc.size();
        for (size_t j = 0; j < locCnt; j++) {
            // get all the filesizes we can from this server
            ComputeFilesizes(fattrs, lastChunkInfo, i, cattr.chunkServerLoc[j]);
        }
        if (fa.fileSize < 0) {
            // If no replicas available return max possible size.
            fa.fileSize = cattr.chunkOffset + (chunkOff_t)CHUNKSIZE;
        }
    }
}

void
KfsClientImpl::ComputeFilesizes(vector<KfsFileAttr>& fattrs,
    const vector<ChunkAttr>& lastChunkInfo, size_t startIdx,
    const ServerLocation& loc)
{
    const size_t cnt = lastChunkInfo.size();
    for (size_t i = startIdx; i < cnt; i++) {
        KfsFileAttr& fa = fattrs[i];
        if (fa.isDirectory || fa.fileSize >= 0) {
            continue;
        }
        if (fa.chunkCount() == 0) {
            fa.fileSize = 0;
            continue;
        }
        const ChunkAttr& cattr = lastChunkInfo[i];
        vector<ServerLocation>::const_iterator const iter = find_if(
            cattr.chunkServerLoc.begin(),
            cattr.chunkServerLoc.end(),
            MatchingServer(loc)
        );
        if (iter == cattr.chunkServerLoc.end()) {
            continue;
        }
        chunkOff_t const size = GetChunkSize(
            loc, cattr.chunkId, cattr.chunkVersion);
        if (size <= 0) {
            return;
        }
        fa.fileSize = lastChunkInfo[i].chunkOffset + size;
    }
}

void
KfsClientImpl::OpDone(
    KfsOp*    inOpPtr,
    bool      inCanceledFlag,
    IOBuffer* inBufferPtr)
{
    assert(inOpPtr && ! inBufferPtr);
    if (inCanceledFlag && inOpPtr->status == 0) {
        inOpPtr->status    = -ECANCELED;
        inOpPtr->statusMsg = "canceled";
    }
    KFS_LOG_STREAM_DEBUG <<
        (inCanceledFlag ? "op canceled: " : "op completed: ") <<
        inOpPtr->Show() << " status: " << inOpPtr->status <<
        " msg: " << inOpPtr->statusMsg <<
    KFS_LOG_EOM;
    // Exit service loop.
    if (mMetaServer) {
        mMetaServer->GetNetManager().Shutdown();
    }
    mNetManager.Shutdown();
}

int
KfsClientImpl::InitUserAndGroupMode()
{
    if (! mInitLookupRootFlag ||
            (! mMetaServerLoc.IsValid() && ! mMetaServer)) {
        return 0;
    }
    // Root directory lookup to determine user and group mode.
    // If no root directory exists or not searchable, then the results is
    // doesn't have much effect on subsequent ops.
    LookupOp lrop(0, ROOTFID, ".");
    lrop.getAuthInfoOnlyFlag = true;
    ExecuteMeta(lrop);
    const time_t now = time(0);
    UpdateUserAndGroup(lrop, now);
    KFS_LOG_STREAM(0 <= lrop.status ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        mMetaServerLoc.hostname << ":" << mMetaServerLoc.port <<
        " auth info"
        " "         << lrop.Show() <<
        " status: " << lrop.status <<
        " "         << lrop.statusMsg <<
        " user: "   << lrop.fattr.user <<
        " "         << lrop.userName <<
        " group: "  << lrop.fattr.group <<
        " "         << lrop.groupName <<
        " euser: "  << lrop.euser <<
        " "         << lrop.euserName <<
        " egroup: " << lrop.egroup <<
        " "         << lrop.egroupName <<
        " use host user and group db: " << mUseOsUserAndGroupFlag <<
    KFS_LOG_EOM;
    if (0 <= lrop.status &&
            ((! mUseOsUserAndGroupFlag && lrop.euser != kKfsUserNone) ||
                ! lrop.euserName.empty())) {
        mEUser  = lrop.euser;
        mEGroup = lrop.egroup;
        if (! lrop.euserName.empty()) {
            UpdateUserId(lrop.euserName, mEUser, now);
            if (! lrop.egroupName.empty()) {
                UpdateGroupId(lrop.egroupName, mEGroup, now);
            }
       }
    }
    return lrop.status;
}

///
/// Wrapper for retrying ops with the metaserver.
///
void
KfsClientImpl::DoMetaOpWithRetry(KfsOp* op)
{
    if (! op) {
        KFS_LOG_STREAM_FATAL << "DoMetaOpWithRetry: invalid null oo" <<
        KFS_LOG_EOM;
        MsgLogger::Stop();
        abort();
        return;
    }
    InitUserAndGroupMode();
    ExecuteMeta(*op);
}

void
KfsClientImpl::ExecuteMeta(KfsOp& op)
{
    if (mMetaServer) {
        mMetaServer->GetNetManager().UpdateTimeNow();
        if (! mMetaServer->Enqueue(&op, this)) {
            if (0 <= op.status) {
                op.status    = -EFAULT;
                op.statusMsg = "failed to enqueue";
            }
            KFS_LOG_STREAM_ERROR << op.statusMsg <<
                " op: " << op.Show() <<
            KFS_LOG_EOM;
            return;
        }
        const bool     kWakeupAndCleanupFlag = false;
        QCMutex* const kNullMutexPtr         = 0;
        mMetaServer->GetNetManager().MainLoop(
            kNullMutexPtr, kWakeupAndCleanupFlag);
    } else {
        StartProtocolWorker();
        mProtocolWorker->ExecuteMeta(op);
    }
    KFS_LOG_STREAM_DEBUG <<
        "meta op done:" <<
        " seq: "    << op.seq <<
        " status: " << op.status <<
        " msg: "    << op.statusMsg <<
        " "         << op.Show() <<
    KFS_LOG_EOM;
}

void
KfsClientImpl::DoChunkServerOp(const ServerLocation& loc, KfsOp& op)
{
    DoServerOp(mChunkServer, loc, op);
}

void
KfsClientImpl::DoServerOp(
    KfsNetClient& server, const ServerLocation& loc, KfsOp& op)
{
    server.GetNetManager().UpdateTimeNow();
    server.SetOpTimeoutSec(mDefaultOpTimeout);
    server.SetMaxRetryCount(mMaxNumRetriesPerOp);
    server.SetTimeSecBetweenRetries(mRetryDelaySec);
    server.SetServer(loc); // Ignore return, and let Enqueue() deal with retries.
    if (! server.Enqueue(&op, this)) {
        KFS_LOG_STREAM_FATAL << "failed to enqueue op: " <<
            op.Show() <<
        KFS_LOG_EOM;
        MsgLogger::Stop();
        abort();
        return;
    }
    const bool     kWakeupAndCleanupFlag = false;
    QCMutex* const kNullMutexPtr         = 0;
    server.GetNetManager().MainLoop(kNullMutexPtr, kWakeupAndCleanupFlag);
    server.Cancel();
    KFS_LOG_STREAM_DEBUG <<
        op.Show() << " status: " << op.status <<
    KFS_LOG_EOM;
}

int
KfsClientImpl::FindFreeFileTableEntry()
{
    if (! mFreeFileTableEntires.empty()) {
        const int fte = mFreeFileTableEntires.back();
        assert(mFileTable[fte] == 0);
        mFreeFileTableEntires.pop_back();
        return fte;
    }
    int last = (int)mFileTable.size();
    if (last == 0) {
        // Do not use 0 slot to make Hypertable work.
        // Hypertable assumes that 0 is invalid file descriptor.
        mFileTable.reserve(64);
        mFileTable.push_back(0);
        last++;
    }
    if (last < MAX_FILES) {     // Grow vector up to max. size
        mFileTable.push_back(0);
        return last;
    }
    return -EMFILE;             // No luck
}

void
KfsClientImpl::ValidateFAttrCache(time_t now, int maxScan)
{
    FAttr*       p;
    const time_t expire = now - mFileAttributeRevalidateTime;
    int          rem    = maxScan;
    while ((p = FAttrLru::Front(mFAttrLru)) &&
            (p->validatedTime < expire ||
                p->generation != mFAttrCacheGeneration)) {
        Delete(p);
        if (--rem < 0) {
            break;
        }
    }
}

KfsClientImpl::FAttr*
KfsClientImpl::LookupFAttr(kfsFileId_t parentFid, const string& name)
{
    FidNameToFAttrMap::const_iterator const it = mFidNameToFAttrMap.find(
        make_pair(parentFid, name));
    return (it == mFidNameToFAttrMap.end() ? 0 : it->second);
}

KfsClientImpl::FAttr*
KfsClientImpl::LookupFAttr(const string& pathname, string* path)
{
    if (pathname.empty() || pathname[0] != '/') {
        return 0;
    }
    NameToFAttrMap::const_iterator const it = mPathCache.find(pathname);
    if (it != mPathCache.end()) {
        if (path) {
            *path = it->first;
        }
        return it->second;
    }
    return 0;
}

KfsClientImpl::FAttr*
KfsClientImpl::NewFAttr(kfsFileId_t parentFid, const string& name,
    const string& pathname)
{
    if (mFidNameToFAttrMap.size() > mFileAttributeRevalidateScan * 2 &&
            ++mFattrCacheSkipValidateCnt > mFileAttributeRevalidateScan * 8) {
        mFattrCacheSkipValidateCnt = 0;
        ValidateFAttrCache(time(0), mFileAttributeRevalidateScan);
    }
    const size_t kMaxInodeCacheSize = 16 << 10;
    for (size_t sz = mFidNameToFAttrMap.size();
            kMaxInodeCacheSize <= sz;
            sz--) {
        Delete(FAttrLru::Front(mFAttrLru));
    }
    FAttr* const fa = new (mFAttrPool.Allocate()) FAttr(mFAttrLru);
    pair<FidNameToFAttrMap::iterator, bool> const res =
        mFidNameToFAttrMap.insert(make_pair(make_pair(parentFid, name), fa));
    if (! res.second) {
        const FAttr* const cfa = res.first->second;
        KFS_LOG_STREAM_FATAL << "fattr entry already exists: " <<
            " parent: "  << parentFid <<
            " name: "    << name <<
            " path: "    << pathname <<
            " gen: "     << mFAttrCacheGeneration <<
            " entry:"
            " gen: "     << cfa->generation <<
            " path: "    << cfa->nameIt->first <<
        KFS_LOG_EOM;
        MsgLogger::Stop();
        abort();
    }
    fa->fidNameIt = res.first;
    if (! pathname.empty() && pathname[0] == '/' &&
            name != ".." && name != ".") {
        pair<NameToFAttrMap::iterator, bool> const
            res = mPathCache.insert(make_pair(pathname, fa));
        if (! res.second) {
            FAttr* const cfa = res.first->second;
            if (cfa->nameIt != res.first) {
                KFS_LOG_STREAM_FATAL << "fattr path entry already exists: " <<
                    " parent: "  << parentFid <<
                    " name: "    << name <<
                    " path: "    << pathname <<
                    " gen: "     << mFAttrCacheGeneration <<
                    " entry:"
                    " gen: "     << cfa->generation <<
                    " parent: "  << cfa->fidNameIt->first.first <<
                    " name: "    << cfa->fidNameIt->first.second <<
                KFS_LOG_EOM;
                MsgLogger::Stop();
                abort();
            }
            if (cfa->generation == mFAttrCacheGeneration) {
                string path = pathname;
                const size_t pos = path.rfind('/');
                if (pos != string::npos && pos + 1 < path.length()) {
                    path.erase(pos + 1);
                }
                InvalidateCachedAttrsWithPathPrefix(path, cfa);
            }
            cfa->nameIt = mPathCacheNone;
            Delete(cfa);
            res.first->second = fa;
        }
        fa->nameIt = res.first;
    } else {
        fa->nameIt = mPathCacheNone;
    }
    return fa;
}

void
KfsClientImpl::Delete(KfsClientImpl::FAttr* fa)
{
    if (! fa) {
        return;
    }
    Validate(fa);
    mFidNameToFAttrMap.erase(fa->fidNameIt);
    if (fa->nameIt != mPathCacheNone) {
        mPathCache.erase(fa->nameIt);
    }
    FAttrLru::Remove(mFAttrLru, *fa);
    if (mDeleteClearFattr && fa == *mDeleteClearFattr) {
        *mDeleteClearFattr = 0;
        mDeleteClearFattr = 0;
    }
    fa->~FAttr();
    mFAttrPool.Deallocate(fa);
}

int
KfsClientImpl::AllocFileTableEntry(kfsFileId_t parentFid, const string& name,
    const string& pathname)
{
    const int fte = FindFreeFileTableEntry();
    if (fte < 0) {
        return fte;
    }
    mFileInstance += 2;
    FileTableEntry& entry =
        *(new FileTableEntry(parentFid, name, mFileInstance));
    mFileTable[fte] = &entry;
    InitPendingRead(entry);
    entry.pathname = pathname;
    entry.ioBufferSize = mDefaultIoBufferSize;
    entry.failShortReadsFlag = mFailShortReadsFlag;
    KFS_LOG_STREAM_DEBUG <<
        "allocated:"
        " fd: "       << fte <<
        " instance: " << entry.instance <<
        " mode: "     << entry.openMode <<
        " path: "     << entry.pathname <<
    KFS_LOG_EOM;
    return fte;
}

void
KfsClientImpl::ReleaseFileTableEntry(int fte)
{
    assert(valid_fd(fte));
    FileTableEntry& entry = *(mFileTable[fte]);
    mFileTable[fte] = 0;
    mFreeFileTableEntires.push_back(fte);
    KFS_LOG_STREAM_DEBUG <<
        "releasing:"
        " fd: "       << fte <<
        " instance: " << entry.instance <<
        " mode: "     << entry.openMode <<
        " path: "     << entry.pathname <<
        " fileId: "   << entry.fattr.fileId <<
    KFS_LOG_EOM;
    CancelPendingRead(entry);
    delete &entry;
}

void
KfsClientImpl::UpdatePath(KfsClientImpl::FAttr* fa, const string& path,
    bool copyPathFlag)
{
    Validate(fa);
    if (! fa || fa->nameIt->first == path) {
        return;
    }
    if (fa->nameIt != mPathCacheNone) {
        mPathCache.erase(fa->nameIt);
    }
    const string& name = fa->fidNameIt->first.second;
    if (name == "." || name == ".." || path.empty() || path[0] != '/') {
        fa->nameIt = mPathCacheNone;
        return;
    }
    pair<NameToFAttrMap::iterator, bool> const res = mPathCache.insert(
        make_pair(copyPathFlag ? string(path.data(), path.length()) : path, fa)
    );
    if (res.second) {
        fa->nameIt = res.first;
        return;
    }
    res.first->second->nameIt = mPathCacheNone;
    res.first->second = fa;
    fa->nameIt = res.first;
}

///
/// Given a parentFid and a file in that directory, return the
/// corresponding entry in the file table.  If such an entry has not
/// been seen before, download the file attributes from the server and
/// save it in the file table.
///
int
KfsClientImpl::Lookup(kfsFileId_t parentFid, const string& name,
    KfsClientImpl::FAttr*& fa, time_t now, const string& path)
{
    assert(! path.empty() && *path.begin() == '/' &&
        name != "." && name != "..");

    fa = LookupFAttr(parentFid, name);
    if (fa && IsValid(*fa, now)) {
        UpdatePath(fa, path);
        return 0;
    }
    LookupOp op(0, parentFid, name.c_str());
    return LookupSelf(op, parentFid, name, fa, now, path);
}

int
KfsClientImpl::LookupSelf(LookupOp& op,
    kfsFileId_t parentFid, const string& name,
    KfsClientImpl::FAttr*& fa, time_t now, const string& path)
{
    DoMetaOpWithRetry(&op);
    if (op.status < 0) {
        if (fa) {
            Delete(fa);
        }
        return op.status;
    }
    UpdateUserAndGroup(op, now);
    // Update i-node cache.
    // This method presently called only from the path traversal.
    // Force new path string allocation to keep "path" buffer mutable,
    // assuming string class implementation with ref. counting, of course.
    const bool kCopyPathFlag = true;
    return UpdateFattr(parentFid, name, fa, path, op.fattr, now, kCopyPathFlag);
}

int
KfsClientImpl::ValidateName(const string& name)
{
    const size_t len = name.length();
    if (len <= 0) {
        return -EINVAL;
    }
    if (len > MAX_FILENAME_LEN) {
        return -ENAMETOOLONG;
    }
    // For now do not allow leading or trailing spaces and \r \n anywhere,
    // as these aren't escaped yet.
    const char* nm = name.c_str();
    if (nm[0] <= ' ' || nm[len - 1] <= ' ') {
        return -ENOENT;
    }
    if (name.find_first_of("/\n\r") != string::npos) {
        return -ENOENT;
    }
    return 0;
}

///
/// Given a path, break it down into: parentFid and filename.  If the
/// path does not begin with "/", the current working directory is
/// inserted in front of it.
/// @param[in] path     The path string that needs to be extracted
/// @param[out] parentFid  The file-id corresponding to the parent dir
/// @param[out] name    The filename following the final "/".
/// @retval 0 on success; -errno on failure
///
int
KfsClientImpl::GetPathComponents(const char* pathname, kfsFileId_t* parentFid,
    string& name, string* path, bool invalidateSubCountsFlag,
    bool enforceLastDirFlag)
{
    if (! pathname) {
        return -EFAULT;
    }
    size_t       len = strlen(pathname);
    const char*  ptr = GetTmpAbsPath(pathname, len);
    if (! mTmpAbsPath.Set(ptr, len)) {
        return -EINVAL;
    }
    const size_t sz  = mTmpAbsPath.size();
    if (sz < 1 || mTmpAbsPath[0] != Path::Token("/", 1)) {
        return -EINVAL;
    }
    string& npath = path ? *path : mTmpCurPath;
    npath.clear();
    npath.reserve(min(MAX_PATH_NAME_LENGTH, len));
    mTmpPath.clear();
    mTmpPath.reserve(sz);
    mTmpPath.push_back(make_pair(ROOTFID, 0));
    *parentFid = ROOTFID;
    name       = mSlash;
    FAttr* fa;
    if (invalidateSubCountsFlag && (fa = LookupFAttr(*parentFid, name))) {
        fa->staleSubCountsFlag = true;
    }
    const Path::Token kThisDir(".",    1);
    const Path::Token kParentDir("..", 2);
    const time_t      now = sz <= 1 ? 0 : time(0);
    int               res = 0;
    for (size_t i = 1; i < sz; i++) {
        const Path::Token& dname = mTmpAbsPath[i];
        if (dname == kThisDir || dname.mLen <= 0) {
            continue;
        }
        if (dname == kParentDir) {
            const size_t psz = mTmpPath.size();
            if (psz <= 1) {
                assert(psz == 1);
                continue;
            }
            npath.erase(npath.length() -
                mTmpAbsPath[mTmpPath.back().second].mLen - 1);
            mTmpPath.pop_back();
            const TmpPath::value_type& back = mTmpPath.back();
            *parentFid = back.first;
            const Path::Token& nm = mTmpAbsPath[back.second];
            name.assign(nm.mPtr, nm.mLen);
            continue;
        }
        name.assign(dname.mPtr, dname.mLen);
        if ((res = ValidateName(name)) != 0) {
            break;
        }
        npath += mSlash;
        npath.append(dname.mPtr, dname.mLen);
        if (npath.length() > MAX_PATH_NAME_LENGTH) {
            res = -ENAMETOOLONG;
            break;
        }
        const bool lastFlag = i + 1 == sz;
        if (lastFlag && (! enforceLastDirFlag || ! mTmpAbsPath.IsDir())) {
            break;
        }
        fa = 0;
        if ((res = Lookup(*parentFid, name, fa, now, npath)) != 0) {
            break;
        }
        if (! fa->isDirectory) {
            res = -ENOTDIR;
            break;
        }
        if (invalidateSubCountsFlag) {
            fa->staleSubCountsFlag = true;
        }
        if (lastFlag) {
            break;
        }
        *parentFid = fa->fileId;
        mTmpPath.push_back(make_pair(*parentFid, i));
    }
    if (path && res == 0 && npath.empty()) {
        npath = name;
    }
    mTmpAbsPath.Clear();

    KFS_LOG_STREAM_DEBUG <<
        "path: "    << pathname <<
        " file: "   << name <<
        " npath: "  << (npath.empty() ? name : npath) <<
        " parent: " << *parentFid <<
        " ret: "    << res <<
    KFS_LOG_EOM;
    return res;
}

int
KfsClientImpl::GetReplication(const char* pathname,
    KfsFileAttr& attr, int& minChunkReplication, int& maxChunkReplication)
{
    QCStMutexLocker l(mMutex);

    int ret;
    if ((ret = StatSelf(pathname, attr, false))  < 0) {
        KFS_LOG_STREAM_DEBUG << (pathname ?  pathname : "null") << ": " <<
            ErrorCodeToStr(ret) <<
        KFS_LOG_EOM;
        return -ENOENT;
    }
    if (attr.isDirectory) {
        KFS_LOG_STREAM_DEBUG << pathname << ": is a directory" << KFS_LOG_EOM;
        return -EISDIR;
    }
    KFS_LOG_STREAM_DEBUG << "path: " << pathname <<
        " file id: " << attr.fileId <<
    KFS_LOG_EOM;

    GetLayoutOp lop(0, attr.fileId);
    lop.continueIfNoReplicasFlag = 0 < attr.numRecoveryStripes;
    lop.chunks.reserve((size_t)max(int64_t(0), attr.chunkCount()));
    GetLayout(lop);
    if (lop.status < 0) {
        KFS_LOG_STREAM_ERROR << "get layout failed on path: " << pathname << " "
             << ErrorCodeToStr(lop.status) <<
        KFS_LOG_EOM;
        return lop.status;
    }
    maxChunkReplication = 0;
    if (lop.chunks.empty()) {
        minChunkReplication = 0;
    } else {
        minChunkReplication = numeric_limits<int>::max();
        for (vector<ChunkLayoutInfo>::const_iterator i = lop.chunks.begin();
                i != lop.chunks.end();
                ++i) {
            const int numReplicas = (int)i->chunkServers.size();
            if (numReplicas < minChunkReplication) {
                minChunkReplication = numReplicas;
            }
            if (numReplicas > maxChunkReplication) {
                maxChunkReplication = numReplicas;
            }
        }
    }
    if (attr.subCount1 < (int64_t)lop.chunks.size()) {
        attr.subCount1 = (int64_t)lop.chunks.size();
    }
    return 0;
}

static inline int
HandleDelegationResponse(
    DelegateOp&      delegateOp,
    DelegationToken& token,
    string&          outToken,
    string&          outKey,
    string*          outErrMsg)
{
    if (delegateOp.status != 0) {
        KFS_LOG_STREAM_ERROR << delegateOp.Show() << ": " <<
            ErrorCodeToStr(delegateOp.status) <<
            " " << delegateOp.statusMsg <<
        KFS_LOG_EOM;
        if (outErrMsg) {
            *outErrMsg = delegateOp.statusMsg;
        }
        return delegateOp.status;
    }
    if (delegateOp.access.empty()) {
        const char* const msg = "invalid empty response access token";
        KFS_LOG_STREAM_ERROR <<
            delegateOp.Show() << ": " << msg <<
        KFS_LOG_EOM;
        if (outErrMsg) {
            *outErrMsg = msg;
        }
        return -EINVAL;
    }
    const char*       p = delegateOp.access.data();
    const char* const e = p + delegateOp.access.size();
    for (int i = 0; i < 2; i++) {
        while (p < e && (*p & 0xFF) <= ' ') {
            ++p;
        }
        const char* const b = p;
        while (p < e && ' ' < (*p & 0xFF)) {
            ++p;
        }
        (i == 0 ? outToken : outKey).assign(b, p - b);
    }
    if (outToken.empty() || outKey.empty() ||
            ! token.FromString(outToken, 0, 0, 0)) {
        const char* const msg = "invalid response access format";
        KFS_LOG_STREAM_ERROR <<
            delegateOp.Show() << ": " << msg <<
        KFS_LOG_EOM;
        if (outErrMsg) {
            *outErrMsg = msg;
        }
        return -EINVAL;
    }
    KFS_LOG_STREAM_DEBUG <<
        "token: " << DelegationToken::ShowToken(token) <<
    KFS_LOG_EOM;
    return 0;
}

    int
KfsClientImpl::CreateDelegationToken(
    bool      allowDelegationFlag,
    uint32_t  maxValidForSec,
    bool&     outDelegationAllowedFlag,
    uint64_t& outIssuedTime,
    uint32_t& outTokenValidForSec,
    uint32_t& outDelegationValidForSec,
    string&   outToken,
    string&   outKey,
    string*   outErrMsg)
{
    QCStMutexLocker l(mMutex);

    DelegateOp delegateOp(0);
    delegateOp.allowDelegationFlag   = allowDelegationFlag;
    delegateOp.requestedValidForTime = maxValidForSec;
    DoMetaOpWithRetry(&delegateOp);
    DelegationToken token;
    const int status = HandleDelegationResponse(
        delegateOp, token, outToken, outKey, outErrMsg);
    if (status == 0) {
        outDelegationValidForSec = delegateOp.validForTime;
        outTokenValidForSec      = delegateOp.tokenValidForTime;
        outIssuedTime            = delegateOp.issuedTime;
        outDelegationAllowedFlag =
            (token.GetFlags() & DelegationToken::kAllowDelegationFlag) != 0;
    }
    return status;
}

int
KfsClientImpl::RenewDelegation(
    string&   ioToken,
    string&   ioKey,
    bool&     outDelegationAllowedFlag,
    uint64_t& outIssuedTime,
    uint32_t& outTokenValidForSec,
    uint32_t& outDelegationValidForSec,
    string*   outErrMsg)
{
    if (ioToken.empty() || ioKey.empty()) {
        if (outErrMsg) {
            *outErrMsg = ioToken.empty() ?
                "invalid empty token string" :
                "invalid empty key string";
        }
        return -EINVAL;
    }

    QCStMutexLocker l(mMutex);

    DelegateOp delegateOp(0);
    delegateOp.renewTokenStr = ioToken;
    delegateOp.renewKeyStr   = ioKey;
    DoMetaOpWithRetry(&delegateOp);
    DelegationToken token;
    const int status = HandleDelegationResponse(
        delegateOp, token, ioToken, ioKey, outErrMsg);
    if (status == 0) {
        outDelegationValidForSec = delegateOp.validForTime;
        outTokenValidForSec      = delegateOp.tokenValidForTime;
        outIssuedTime            = delegateOp.issuedTime;
        outDelegationAllowedFlag =
            (token.GetFlags() & DelegationToken::kAllowDelegationFlag) != 0;
    }
    return status;
}

int
KfsClientImpl::CancelDelegation(
    const string& token,
    const string& key,
    string*       outErrMsg)
{
    QCStMutexLocker l(mMutex);

    DelegateCancelOp delegateCancelOp(0);
    delegateCancelOp.tokenStr = token;
    delegateCancelOp.keyStr   = key;
    DoMetaOpWithRetry(&delegateCancelOp);
    if (delegateCancelOp.status <= 0 && outErrMsg) {
        *outErrMsg = delegateCancelOp.statusMsg;
    }
    return delegateCancelOp.status;
}

int
KfsClientImpl::GetDelegationTokenInfo(
    const char* inTokenStrPtr,
    kfsUid_t&   outUid,
    uint32_t&   outSeq,
    kfsKeyId_t& outKeyId,
    int16_t&    outFlags,
    uint64_t&   outIssuedTime,
    uint32_t&   outValidForSec)
{
    const char* const               kKeyPtr     = 0;
    int const                       kKeyLen     = 0;
    DelegationToken::Subject* const kSubjectPtr = 0;
    DelegationToken                 theToken;
    if ( ! theToken.FromString(
            inTokenStrPtr,
            inTokenStrPtr ? (int)strlen(inTokenStrPtr) : 0,
            kKeyPtr,
            kKeyLen,
            kSubjectPtr)) {
        return -EINVAL;
    }
    outUid         = theToken.GetUid();
    outSeq         = theToken.GetSeq();
    outKeyId       = theToken.GetKeyId();
    outFlags       = theToken.GetFlags();
    outIssuedTime  = theToken.GetIssuedTime();
    outValidForSec = theToken.GetValidForSec();
    return 0;
}

int
KfsClientImpl::EnumerateBlocks(
    const char* pathname, KfsClient::BlockInfos& res, bool getChunkSizesFlag)
{
    QCStMutexLocker l(mMutex);

    KfsFileAttr attr;
    int ret;
    if ((ret = StatSelf(pathname, attr, false))  < 0) {
        KFS_LOG_STREAM_DEBUG << (pathname ?  pathname : "null") << ": " <<
            ErrorCodeToStr(ret) <<
        KFS_LOG_EOM;
        return -ENOENT;
    }
    if (attr.isDirectory) {
        KFS_LOG_STREAM_DEBUG << pathname << ": is a directory" << KFS_LOG_EOM;
        return -EISDIR;
    }
    KFS_LOG_STREAM_DEBUG << "path: " << pathname <<
        " file id: " << attr.fileId <<
    KFS_LOG_EOM;

    GetLayoutOp lop(0, attr.fileId);
    lop.continueIfNoReplicasFlag = true;
    lop.chunks.reserve((size_t)max(int64_t(0), attr.chunkCount()));
    GetLayout(lop);
    if (lop.status < 0) {
        KFS_LOG_STREAM_ERROR << "get layout failed on path: " << pathname << " "
             << ErrorCodeToStr(lop.status) <<
        KFS_LOG_EOM;
        return lop.status;
    }

    vector<ssize_t> chunksize;
    for (vector<ChunkLayoutInfo>::const_iterator i = lop.chunks.begin();
            i != lop.chunks.end();
            ++i) {
        if (i->chunkServers.empty() || ! getChunkSizesFlag) {
            res.push_back(KfsClient::BlockInfo());
            KfsClient::BlockInfo& chunk = res.back();
            chunk.offset  = i->fileOffset;
            chunk.id      = i->chunkId;
            chunk.version = i->chunkVersion;
            continue;
        }
        chunksize.clear();
        // Get the size for the chunk from all the responding servers
        chunksize.resize(i->chunkServers.size(), -1);
        transform(i->chunkServers.begin(), i->chunkServers.end(),
            chunksize.begin(), RespondingServer2(*this, *i));
        for (size_t k = 0; k < chunksize.size(); k++) {
            res.push_back(KfsClient::BlockInfo());
            KfsClient::BlockInfo& chunk = res.back();
            chunk.offset  = i->fileOffset;
            chunk.id      = i->chunkId;
            chunk.version = i->chunkVersion;
            chunk.size    = chunksize[k];
            chunk.server  = i->chunkServers[k];
        }
    }
    return 0;
}

int
KfsClientImpl::GetDataChecksums(const ServerLocation &loc,
    kfsChunkId_t chunkId, uint32_t *checksums, bool readVerifyFlag)
{
    GetChunkMetadataOp op(0, chunkId, readVerifyFlag);
    int64_t leaseId   = -1;
    int     theStatus = GetChunkAccess(loc, chunkId, op.access, leaseId);
    if (theStatus < 0) {
        return theStatus;
    }
    DoChunkServerOp(loc, op);
    if (0 <= leaseId) {
        LeaseRelinquishOp theLeaseRelinquishOp(0, chunkId, leaseId);
        DoMetaOpWithRetry(&theLeaseRelinquishOp);
    }
    if (op.status == -EBADCKSUM) {
        KFS_LOG_STREAM_INFO <<
            "Server " << loc <<
            " reports checksum mismatch for scrub read on"
            " chunk: " << chunkId <<
        KFS_LOG_EOM;
    }
    if (op.status < 0) {
        return op.status;
    }
    const size_t numChecksums = CHUNKSIZE / CHECKSUM_BLOCKSIZE;
    if (op.contentLength < numChecksums * sizeof(*checksums)) {
        return -EINVAL;
    }
    memcpy(checksums, op.contentBuf, numChecksums * sizeof(*checksums));
    return 0;
}

int
KfsClientImpl::VerifyDataChecksums(const char* pathname)
{
    KfsFileAttr attr;
    int         res;

    QCStMutexLocker l(mMutex);

    if ((res = StatSelf(pathname, attr, false))  < 0) {
        return res;
    }
    if (attr.isDirectory) {
        return -EISDIR;
    }
    return VerifyDataChecksumsFid(attr.fileId);
}

int
KfsClientImpl::VerifyDataChecksums(int fd)
{
    QCStMutexLocker l(mMutex);

    if (! valid_fd(fd)) {
        return -EBADF;
    }
    FileTableEntry& entry = *(mFileTable[fd]);
    if (entry.fattr.isDirectory) {
        return -EISDIR;
    }
    return VerifyDataChecksumsFid(entry.fattr.fileId);
}

int
KfsClientImpl::VerifyDataChecksumsFid(kfsFileId_t fileId)
{
    GetLayoutOp lop(0, fileId);
    lop.continueIfNoReplicasFlag = true;
    GetLayout(lop);
    if (lop.status < 0) {
        KFS_LOG_STREAM_ERROR << "Get layout failed with error: "
             << ErrorCodeToStr(lop.status) <<
        KFS_LOG_EOM;
        return lop.status;
    }
    const size_t numChecksums = CHUNKSIZE / CHECKSUM_BLOCKSIZE;
    scoped_array<uint32_t> chunkChecksums1;
    chunkChecksums1.reset(new uint32_t[numChecksums]);
    scoped_array<uint32_t> chunkChecksums2;
    chunkChecksums2.reset(new uint32_t[numChecksums]);
    int status = 0;
    for (vector<ChunkLayoutInfo>::const_iterator i = lop.chunks.begin();
            i != lop.chunks.end();
            ++i) {
        int ret;
        if (i->chunkServers.empty()) {
            if (status == 0) {
                status = -EAGAIN;
            }
            KFS_LOG_STREAM_ERROR << "no replicas chunk: " <<
                i->chunkId <<
            KFS_LOG_EOM;
            continue;
        }
        if ((ret = GetDataChecksums(
                i->chunkServers[0], i->chunkId, chunkChecksums1.get())) < 0) {
            KFS_LOG_STREAM_ERROR << "failed to get checksums from server " <<
                i->chunkServers[0] << " " << ErrorCodeToStr(ret) <<
            KFS_LOG_EOM;
            if (status == 0) {
                status = ret;
            }
            continue;
        }
        for (size_t k = 1; k < i->chunkServers.size(); k++) {
            if ((ret = GetDataChecksums(
                    i->chunkServers[k], i->chunkId,
                    chunkChecksums2.get())) < 0) {
                KFS_LOG_STREAM_ERROR << "failed get checksums from server: " <<
                    i->chunkServers[k] << " " << ErrorCodeToStr(ret) <<
                KFS_LOG_EOM;
                if (status == 0) {
                    status = ret;
                }
                continue;
            }
            for (size_t v = 0; v < numChecksums; v++) {
                if (chunkChecksums1[v] != chunkChecksums2[v]) {
                    KFS_LOG_STREAM_ERROR <<
                        "checksum mismatch between servers: " <<
                        i->chunkServers[0] << " " << i->chunkServers[k] <<
                    KFS_LOG_EOM;
                    if (status == 0) {
                        status = -EINVAL;
                    }
                }
            }
        }
    }
    return status;
}

int
KfsClientImpl::GetFileOrChunkInfo(kfsFileId_t fileId, kfsChunkId_t chunkId,
    KfsFileAttr& fattr, chunkOff_t& offset, int64_t& chunkVersion,
    vector<ServerLocation>& servers)
{
    QCStMutexLocker l(mMutex);
    GetPathNameOp op(0, fileId, chunkId);
    DoMetaOpWithRetry(&op);
    fattr          = op.fattr;
    fattr.filename = op.pathname;
    offset         = op.offset;
    chunkVersion   = op.chunkVersion;
    servers        = op.servers;
    return op.status;
}

int
KfsClientImpl::Chmod(const char* pathname, kfsMode_t mode)
{
    if (mode == kKfsModeUndef) {
        return -EINVAL;
    }

    QCStMutexLocker l(mMutex);

    KfsFileAttr attr;
    int         res;
    FAttr*      fa = 0;
    if ((res = StatSelf(pathname, attr, false, 0, &fa)) < 0) {
        return res;
    }
    if (! attr.IsAnyPermissionDefined()) {
        return 0; // permissions aren't supported by the meta server.
    }
    ChmodOp op(0, attr.fileId, mode & (attr.isDirectory ?
        kfsMode_t(Permissions::kDirModeMask) :
        kfsMode_t(Permissions::kFileModeMask)));
    DoMetaOpWithRetry(&op);
    if (op.status != 0) {
        return op.status;
    }
    if (fa && fa->isDirectory) {
        InvalidateAllCachedAttrs();
    }
    return 0;
}

kfsUid_t
KfsClientImpl::GetUserId()
{
    QCStMutexLocker l(mMutex);
    InitUserAndGroupMode();
    return mEUser;
}

int
KfsClientImpl::GetUserAndGroup(const char* user, const char* group,
    kfsUid_t& uid, kfsGid_t& gid)
{
    uid = kKfsUserNone;
    gid = kKfsGroupNone;

    QCStMutexLocker l(mMutex);

    const int ret = InitUserAndGroupMode();
    if (ret < 0) {
        return ret;
    }
    const time_t now = time(0);
    if (user && *user) {
        uid = NameToUid(user, now);
        if (uid == kKfsUserNone) {
            return -EINVAL;
        }
    }
    if (group && *group) {
        gid = NameToGid(group, now);
        if (gid == kKfsGroupNone) {
            return -EINVAL;
        }
    }
    return 0;
}

template<typename T>
int
KfsClientImpl::RecursivelyApply(
    string& path, const KfsFileAttr& attr, T& functor,
    bool fileIdAndTypeOnly /* = false */)
{
    int          status   = 0;
    const size_t prevSize = path.size();
    if (! attr.filename.empty() && path != "/") {
        path += "/";
        path += attr.filename;
    }
    if (attr.isDirectory) {
        // don't compute any filesize; don't update client cache
        vector<KfsFileAttr> entries;
        if ((status = ReaddirPlus(
                path, attr.fileId, entries, false, false, fileIdAndTypeOnly)) < 0) {
            entries.clear();
        }
        int res = 0;
        for (vector<KfsFileAttr>::const_iterator it = entries.begin();
                it != entries.end();
                ++it) {
            if (it->filename == "." || it->filename == "..") {
                continue;
            }
            if ((res = RecursivelyApply(path, *it, functor)) != 0) {
                path.resize(prevSize);
                return res;
            }
        }
    }
    status = functor(path, attr, status);
    path.resize(prevSize);
    return status;
}

template<typename T>
int
KfsClientImpl::RecursivelyApply(const char* pathname, T& functor,
    bool fileIdAndTypeOnly /* = false */)
{
    string path;
    path.reserve(MAX_PATH_NAME_LENGTH);
    path = pathname;
    KfsFileAttr attr;
    int res = StatSelf(pathname, attr, false);
    if (res != 0) {
        return res;
    }
    attr.filename.clear();
    res = RecursivelyApply(path, attr, functor, fileIdAndTypeOnly);
    InvalidateAllCachedAttrs();
    return res;
}

int
KfsClientImpl::Chmod(int fd, kfsMode_t mode)
{
    QCStMutexLocker l(mMutex);

    if (! valid_fd(fd)) {
        return -EBADF;
    }
    if (mode == kKfsModeUndef) {
        return -EINVAL;
    }
    FileTableEntry& entry = *(mFileTable[fd]);
    if (! entry.fattr.IsAnyPermissionDefined()) {
        return 0; // permissions aren't supported by the meta server.
    }
    ChmodOp op(0, entry.fattr.fileId, mode & (entry.fattr.isDirectory ?
        kfsMode_t(Permissions::kDirModeMask) :
        kfsMode_t(Permissions::kFileModeMask)));
    DoMetaOpWithRetry(&op);
    if (op.status != 0) {
        return op.status;
    }
    entry.fattr.mode = op.mode;
    if (entry.fattr.isDirectory) {
        InvalidateAllCachedAttrs();
    }
    return 0;
}

class ChmodFunc
{
public:
    typedef KfsClient::ErrorHandler ErrorHandler;

    ChmodFunc(KfsClientImpl& cli, kfsMode_t mode,
        ErrorHandler& errHandler)
        : mCli(cli),
          mMode(mode),
          mErrHandler(errHandler)
        {}
    int operator()(const string& path, const KfsFileAttr& attr,
            int status) const
    {
        if (status != 0) {
            const int ret = mErrHandler(path, status);
            if (ret != 0) {
                return ret;
            }
        }
        ChmodOp op(0, attr.fileId, mMode & (attr.isDirectory ?
            kfsMode_t(Permissions::kDirModeMask) :
            kfsMode_t(Permissions::kFileModeMask)));
        mCli.DoMetaOpWithRetry(&op);
        if (op.status != 0) {
            const int ret = mErrHandler(path, op.status);
            if (ret != 0) {
                return ret;
            }
        }
        return 0;
    }
private:
    KfsClientImpl&  mCli;
    const kfsMode_t mMode;
    ErrorHandler&   mErrHandler;
};

int
KfsClientImpl::ChmodR(const char* pathname, kfsMode_t mode,
    KfsClientImpl::ErrorHandler* errHandler)
{
    QCStMutexLocker l(mMutex);

    DefaultErrHandler errorHandler;
    ChmodFunc funct(*this, mode, errHandler ? *errHandler : errorHandler);
    const int ret = RecursivelyApply(pathname, funct);
    return (errHandler ? ret : (ret != 0 ? ret : errorHandler.GetStatus()));
}

int
KfsClientImpl::Chown(int fd, const char* user, const char* group)
{
    return ChownSelf(fd, user, group, kKfsUserNone, kKfsGroupNone);
}

int
KfsClientImpl::Chown(int fd, kfsUid_t user, kfsGid_t group)
{
    return ChownSelf(fd, 0, 0, user, group);
}

int
KfsClientImpl::ChownSelf(int fd,
    const char* userName, const char* groupName, kfsUid_t user, kfsGid_t group)
{
    if (user == kKfsUserNone && group == kKfsGroupNone &&
            (! userName || ! *userName) && (! groupName || ! *groupName)) {
        return -EINVAL;
    }

    QCStMutexLocker l(mMutex);

    if (! valid_fd(fd)) {
        return -EBADF;
    }
    FileTableEntry& entry = *(mFileTable[fd]);
    if (! entry.fattr.IsAnyPermissionDefined()) {
        return 0; // permissions aren't supported by the meta server.
    }
    return ChownSelf(
        entry.fattr.fileId,
        userName,
        groupName,
        user,
        group,
        &entry.fattr.user,
        &entry.fattr.group
    );
}

int
KfsClientImpl::ChownSetParams(
    const char*& userName,
    const char*& groupName,
    kfsUid_t&    user,
    kfsGid_t&    group)
{
    const int ret = InitUserAndGroupMode();
    if (ret < 0) {
        return ret;
    }
    kfsUid_t    uid = user;
    kfsGid_t    gid = group;
    const char* un  = userName  ? userName  : "";
    const char* gn  = groupName ? groupName : "";
    if (mUseOsUserAndGroupFlag) {
        if (*un || *gn) {
            const time_t now = time(0);
            if (*un) {
                uid = NameToUid(un, now);
                if (uid == kKfsUserNone) {
                    return -EINVAL;
                }
            }
            if (*gn) {
                gid = NameToGid(gn, now);
                if (gid == kKfsGroupNone) {
                    return -EINVAL;
                }
            }
            un = "";
            gn = "";
        }
        if (mEUser != kKfsUserRoot &&
                (uid == kKfsUserNone || uid == mEUser) &&
                find(mGroups.begin(), mGroups.end(), gid) == mGroups.end()) {
            return -EPERM;
        }
    } else {
        if (*un) {
            char* end = 0;
            unsigned long const id = strtoul(un, &end, 0);
            if (un < end && *end == 0) {
                uid = (kfsUid_t)id;
                un = "";
            }
        }
        if (*gn) {
            char* end = 0;
            unsigned long const id = strtoul(gn, &end, 0);
            if (gn < end && *end == 0) {
                gid = (kfsGid_t)id;
                gn = "";
            }
        }
    }
    userName  = un;
    groupName = gn;
    user      = uid;
    group     = gid;
    return 0;
}

int
KfsClientImpl::ChownSelf(
    kfsFileId_t fid,
    const char* userName,
    const char* groupName,
    kfsUid_t    user,
    kfsGid_t    group,
    kfsUid_t*   outUser,
    kfsUid_t*   outGroup)
{
    const char* un     = userName;
    const char* gn     = groupName;
    kfsUid_t    uid    = user;
    kfsGid_t    gid    = group;
    const int   status = ChownSetParams(un, gn, uid, gid);
    if (status != 0) {
        return status;
    }
    ChownOp op(0, fid, uid, gid);
    op.userName  = un;
    op.groupName = gn;
    DoMetaOpWithRetry(&op);
    if (op.status != 0) {
        return op.status;
    }
    if (outUser) {
        *outUser = op.user;
    }
    if (outGroup) {
        *outGroup = op.group;
    }
    return 0;
}

int
KfsClientImpl::Chown(const char* pathname, const char* user, const char* group)
{
    return ChownSelf(pathname, user, group, kKfsUserNone, kKfsGroupNone);
}

int
KfsClientImpl::Chown(const char* pathname, kfsUid_t user, kfsGid_t group)
{
    return ChownSelf(pathname, 0, 0, user, group);
}

int
KfsClientImpl::ChownSelf(const char* pathname,
    const char* userName, const char* groupName, kfsUid_t user, kfsGid_t group)
{
    if (user == kKfsUserNone && group == kKfsGroupNone &&
            (! userName || ! *userName) && (! groupName || ! *groupName)) {
        return -EINVAL;
    }

    QCStMutexLocker l(mMutex);

    KfsFileAttr attr;
    int         res;
    FAttr*      fa = 0;
    if ((res = StatSelf(pathname, attr, false, 0, &fa)) < 0) {
        return res;
    }
    if (! fa) {
        return -EFAULT;
    }
    if (! attr.IsAnyPermissionDefined()) {
        return 0; // permissions aren't supported by the meta server.
    }
    const int status = ChownSelf(
        attr.fileId,
        userName,
        groupName,
        user,
        group,
        &fa->user,
        &fa->group
    );
    if (status != 0) {
        return status;
    }
    if (fa->isDirectory) {
        InvalidateAllCachedAttrs();
    }
    return 0;
}

int
KfsClientImpl::ChownR(const char* pathname, const char* user, const char* group,
    KfsClientImpl::ErrorHandler* errHandler)
{
    return ChownRSelf(pathname, user, group,
        kKfsUserNone, kKfsGroupNone, errHandler);
}

int
KfsClientImpl::ChownR(const char* pathname, kfsUid_t user, kfsGid_t group,
    KfsClientImpl::ErrorHandler* errHandler)
{
    return ChownRSelf(pathname, 0, 0, user, group, errHandler);
}

class ChownFunc
{
public:
    typedef KfsClient::ErrorHandler ErrorHandler;

    ChownFunc(
        KfsClientImpl& cli,
        kfsUid_t       user,
        kfsGid_t       group,
        const char*    userName,
        const char*    groupName,
        ErrorHandler&  errHandler)
        : mCli(cli),
          mNow(0),
          mSetNowFlag(true),
          mUser(user),
          mGroup(group),
          mUserName(userName),
          mGroupName(groupName),
          mErrHandler(errHandler)
        {}
    int operator()(const string& path, const KfsFileAttr& attr,
        int status) const
    {
        if (status != 0) {
            const int ret = mErrHandler(path, status);
            if (ret != 0) {
                return ret;
            }
        }
        ChownOp op(0, attr.fileId, mUser, mGroup);
        op.userName  = mUserName;
        op.groupName = mGroupName;
        mCli.DoMetaOpWithRetry(&op);
        if (op.status != 0) {
            const int ret = mErrHandler(path, op.status);
            if (ret != 0) {
                return ret;
            }
        }
        if (! op.userName.empty()) {
            mCli.UpdateUserId(op.userName, op.user, Now());
        }
        if (! op.groupName.empty()) {
            mCli.UpdateGroupId(op.groupName, op.group, Now());
        }
        return 0;
    }
private:
    KfsClientImpl& mCli;
    time_t         mNow;
    bool           mSetNowFlag;
    const kfsUid_t mUser;
    const kfsGid_t mGroup;
    string const   mUserName;
    string const   mGroupName;
    ErrorHandler&  mErrHandler;

    time_t Now() const
    {
        if (mSetNowFlag) {
            const_cast<bool&>(mSetNowFlag) = false;
            const_cast<time_t&>(mNow)      = time(0);
        }
        return mNow;
    }
};

int
KfsClientImpl::ChownRSelf(
    const char*                  pathname,
    const char*                  userName,
    const char*                  groupName,
    kfsUid_t                     user,
    kfsGid_t                     group,
    KfsClientImpl::ErrorHandler* errHandler)
{
    QCStMutexLocker l(mMutex);

    kfsUid_t    uid    = user;
    kfsGid_t    gid    = group;
    const char* un     = userName;
    const char* gn     = groupName;
    const int   status = ChownSetParams(un, gn, uid, gid);
    if (status != 0) {
        return status;
    }
    DefaultErrHandler errorHandler;
    ChownFunc funct(*this, uid, gid, un, gn,
        errHandler ? *errHandler : errorHandler);
    const int ret = RecursivelyApply(pathname, funct);
    return (errHandler ? ret : (ret != 0 ? ret : errorHandler.GetStatus()));
}

class SetReplicationFactorFunc
{
public:
    typedef KfsClient::ErrorHandler ErrorHandler;

    SetReplicationFactorFunc(KfsClientImpl& cli, int16_t repl,
        ErrorHandler& errHandler)
        : mCli(cli),
          mReplication(repl),
          mErrHandler(errHandler)
        {}
    int operator()(const string& path, const KfsFileAttr& attr,
        int status) const
    {
        if (status != 0) {
            const int ret = mErrHandler(path, status);
            if (ret != 0) {
                return ret;
            }
        }
        if (attr.isDirectory) {
            return 0;
        }
        ChangeFileReplicationOp op(0, attr.fileId, mReplication);
        mCli.DoMetaOpWithRetry(&op);
        if (op.status != 0) {
            const int ret = mErrHandler(path, op.status);
            if (ret != 0) {
                return ret;
            }
        }
        return 0;
    }
private:
    KfsClientImpl& mCli;
    const int16_t  mReplication;
    ErrorHandler&  mErrHandler;
};

int
KfsClientImpl::SetReplicationFactorR(const char *pathname, int16_t numReplicas,
    KfsClientImpl::ErrorHandler* errHandler)
{
    QCStMutexLocker l(mMutex);

    // Even though meta server supports recursive set replication, do it one
    // file at a time, in order to prevent "DoS".
    // For this reason meta server might not support recursive version in the
    // future releases.
    DefaultErrHandler errorHandler;
    SetReplicationFactorFunc funct(
        *this, numReplicas, errHandler ? *errHandler : errorHandler);
    const int ret = RecursivelyApply(pathname, funct);
    return (errHandler ? ret : (ret != 0 ? ret : errorHandler.GetStatus()));
}

void
KfsClientImpl::SetUMask(kfsMode_t mask)
{
    QCStMutexLocker l(mMutex);
    mUMask = mask & Permissions::kAccessModeMask;
}

kfsMode_t
KfsClientImpl::GetUMask()
{
    QCStMutexLocker l(mMutex);
    return mUMask;
}

int
KfsClientImpl::SetEUserAndEGroup(kfsUid_t user, kfsGid_t group,
    kfsGid_t* groups, int groupsCnt)
{
    return (mMetaServer ? -EINVAL :
        ClientsList::SetEUserAndEGroup(user, group, groups, groupsCnt)
    );
}

int
KfsClientImpl::CompareChunkReplicas(const char* pathname, string& md5sum)
{
    QCStMutexLocker l(mMutex);

    KfsFileAttr attr;
    int         res;
    if ((res = StatSelf(pathname, attr, false))  < 0) {
        KFS_LOG_STREAM_ERROR <<
            (pathname ? pathname : "null") << ": " << ErrorCodeToStr(res) <<
        KFS_LOG_EOM;
        return res;
    }
    if (attr.isDirectory) {
        KFS_LOG_STREAM_ERROR << pathname << " is a directory" << KFS_LOG_EOM;
        return -EISDIR;
    }

    GetLayoutOp lop(0, attr.fileId);
    lop.continueIfNoReplicasFlag = true;
    lop.chunks.reserve((size_t)max(int64_t(0), attr.chunkCount()));
    GetLayout(lop);
    if (lop.status < 0) {
        KFS_LOG_STREAM_ERROR << "get layout error: " <<
            ErrorCodeToStr(lop.status) <<
        KFS_LOG_EOM;
        return lop.status;
    }
    MdStream mdsAll;
    MdStream mds;
    int      ret = 0;
    for (vector<ChunkLayoutInfo>::const_iterator i = lop.chunks.begin();
         i != lop.chunks.end();
         ++i) {
        if (i->chunkServers.empty()) {
            KFS_LOG_STREAM_ERROR <<
                "chunk: " << i->chunkId << " no replica available" <<
            KFS_LOG_EOM;
            ret = -EAGAIN;
            continue;
        }
        ChunkServerAccess  chunkServerAccess;
        int64_t            leaseId = -1;
        const int status = GetChunkLease(
            i->chunkId, pathname, -1, chunkServerAccess, leaseId);
        if (status != 0) {
            return status;
        }
        mdsAll.flush();
        mds.Reset(&mdsAll);
        const int nbytes = GetChunkFromReplica(
            chunkServerAccess,
            i->chunkServers[0],
            i->chunkId,
            i->chunkVersion,
            mds
        );
        if (nbytes < 0) {
            KFS_LOG_STREAM_ERROR << i->chunkServers[0] <<
                 ": " << ErrorCodeToStr(nbytes) <<
            KFS_LOG_EOM;
            ret = nbytes;
        } else {
            const string md5sumFirst = mds.GetMd();
            mds.Reset();
            KFS_LOG_STREAM_DEBUG <<
                "chunk: "    << i->chunkId <<
                " replica: " << i->chunkServers[0] <<
                " size: "    << nbytes <<
                " md5sum: "  << md5sumFirst <<
            KFS_LOG_EOM;
            for (uint32_t k = 1; k < i->chunkServers.size(); k++) {
                mds.Reset();
                const int n = GetChunkFromReplica(
                    chunkServerAccess,
                    i->chunkServers[k],
                    i->chunkId,
                    i->chunkVersion,
                    mds
                );
                if (n < 0) {
                    KFS_LOG_STREAM_ERROR << i->chunkServers[0] <<
                         ": " << ErrorCodeToStr(n) <<
                    KFS_LOG_EOM;
                    ret = n;
                } else {
                    const string md5sumCur = mds.GetMd();
                    KFS_LOG_STREAM_DEBUG <<
                        "chunk: "    << i->chunkId <<
                        " replica: " << i->chunkServers[k] <<
                        " size: "    << nbytes <<
                        " md5sum: "  << md5sumCur <<
                    KFS_LOG_EOM;
                    if (nbytes != n || md5sumFirst != md5sumCur) {
                        ret = -EINVAL;
                        KFS_LOG_STREAM_ERROR <<
                            "chunk: " << i->chunkId <<
                            (nbytes != n ? " size" : " data") <<
                            " mismatch: " << i->chunkServers[0] <<
                            " size: "     << nbytes <<
                            " md5sum: "   << md5sumFirst <<
                            " vs "        << i->chunkServers[k] <<
                            " size: "     << n <<
                            " md5sum: "   << md5sumCur <<
                        KFS_LOG_EOM;
                    }
                }
            }
        }
        if (0 <= leaseId) {
            LeaseRelinquishOp lrelOp(0, i->chunkId, leaseId);
            DoMetaOpWithRetry(&lrelOp);
            if (lrelOp.status < 0) {
                KFS_LOG_STREAM_ERROR << "failed to relinquish lease:" <<
                    " chunk: " << i->chunkId <<
                    " lease: " << leaseId <<
                    " msg: "   << lrelOp.statusMsg <<
                    " "        << ErrorCodeToStr(lrelOp.status) <<
                KFS_LOG_EOM;
            }
        }
    }
    md5sum = mdsAll.GetMd();
    return ret;
}

int
KfsClientImpl::GetChunkLease(
    kfsChunkId_t       inChunkId,
    const char*        inPathNamePtr,
    int                inLeaseTime,
    ChunkServerAccess& inChunkServerAccess,
    int64_t&           outLeaseId)
{
    LeaseAcquireOp theLeaseOp(0, inChunkId, inPathNamePtr);
    theLeaseOp.leaseTimeout = inLeaseTime;
    const int maxLeaseWaitTimeSec = max(LEASE_INTERVAL_SECS * 3 / 2,
        (mMaxNumRetriesPerOp - 1) * (mRetryDelaySec + mDefaultOpTimeout));
    const int leseRetryDelaySec    = min(3, max(1, mRetryDelaySec));
    time_t    endTime              = maxLeaseWaitTimeSec;
    for (int retryCnt = 0; ; retryCnt++) {
        DoMetaOpWithRetry(&theLeaseOp);
        if (theLeaseOp.status != -EBUSY && theLeaseOp.status != -EAGAIN) {
            break;
        }
        const time_t now = time(0);
        if (retryCnt == 0) {
            endTime += now;
        }
        if (endTime < now) {
            break;
        }
        KFS_LOG_STREAM((endTime - maxLeaseWaitTimeSec + 15 < now) ?
                MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelDEBUG) <<
            "chunk: "        << inChunkId <<
            " lease: "       << theLeaseOp.statusMsg <<
            " retrying in: " << leseRetryDelaySec << " sec." <<
            " retry: "       << retryCnt <<
        KFS_LOG_EOM;
        Sleep(leseRetryDelaySec);
        theLeaseOp.status = 0;
        theLeaseOp.statusMsg.clear();
    }
    if (theLeaseOp.status == 0 && 0 < theLeaseOp.chunkAccessCount) {
        const bool         kHasChunkServerAccessFlag = true;
        const int          kBufPos                   = 0;
        const bool         kOwnsBufferFlag           = true;
        const kfsChunkId_t kChunkId                  = -1;
        const int          theRet = inChunkServerAccess.Parse(
            theLeaseOp.chunkAccessCount,
            kHasChunkServerAccessFlag,
            kChunkId,
            theLeaseOp.contentBuf,
            kBufPos,
            theLeaseOp.contentLength,
            kOwnsBufferFlag
        );
        theLeaseOp.ReleaseContentBuf();
        if (theRet < 0) {
            theLeaseOp.status    = theRet;
            theLeaseOp.statusMsg = "invalid chunk access response";
        }
    } else {
        inChunkServerAccess.Clear();
    }
    if (theLeaseOp.status == 0) {
        outLeaseId = theLeaseOp.leaseId;
    } else {
        KFS_LOG_STREAM_ERROR <<
            "failed to acquire lease:"
            " chunk: "  << inChunkId <<
            " msg: "    << theLeaseOp.statusMsg <<
            " status: " << ErrorCodeToStr(theLeaseOp.status) <<
        KFS_LOG_EOM;
    }
    return theLeaseOp.status;
}

int
KfsClientImpl::SetChunkAccess(
    const ChunkServerAccess& inChunkServerAccess,
    const ServerLocation&    inLocation,
    kfsChunkId_t             inChunkId,
    string&                  outChunkAccess)
{
    if (inChunkServerAccess.IsEmpty()) {
        mChunkServer.SetKey(0, 0, 0, 0);
        outChunkAccess.clear();
        return 0;
    }
    CryptoKeys::Key theKey;
    const ChunkServerAccess::Entry* const thePtr =
        inChunkServerAccess.Get(inLocation, inChunkId, theKey);
    if (thePtr) {
        mChunkServer.SetKey(
            thePtr->chunkServerAccessId.mPtr,
            thePtr->chunkServerAccessId.mLen,
            theKey.GetPtr(),
            theKey.GetSize()
        );
        outChunkAccess.assign(
            thePtr->chunkAccess.mPtr, thePtr->chunkAccess.mLen);
        return 0;
    }
    KFS_LOG_STREAM_ERROR <<
        "server: " << inLocation <<
        " chunk: " << inChunkId <<
        ": no chunk server access" <<
    KFS_LOG_EOM;
    return -EACCES;
}

int
KfsClientImpl::GetChunkAccess(
    const ServerLocation& inLocation,
    kfsChunkId_t          inChunkId,
    string&               outAccess,
    int64_t&              outLeaseId,
    ChunkServerAccess*    outAccessPtr)
{
    outLeaseId = -1;
    ChunkServerAccess  theCSAccess;
    ChunkServerAccess& theChunkServerAccess =
        outAccessPtr ? *outAccessPtr : theCSAccess;
    if (! mUseOsUserAndGroupFlag && mAuthCtx.IsEnabled()) {
        // Get chunk server and chunk access if authenticated.
        int const         kLeaseTime   = -1;
        const char* const kPathNamePtr = "";
        const int theStatus = GetChunkLease(
            inChunkId,
            kPathNamePtr,
            kLeaseTime,
            theChunkServerAccess,
            outLeaseId
        );
        if (theStatus < 0) {
            return theStatus;
        }
    }
    return SetChunkAccess(
        theChunkServerAccess, inLocation, inChunkId, outAccess);
}

chunkOff_t
KfsClientImpl::GetChunkSize(
    const ServerLocation& inLocation,
    kfsChunkId_t          inChunkId,
    int64_t               inChunkVersion,
    bool*                 outUsedLeaseLocationsFlagPtr)
{
    SizeOp theSizeOp(0, inChunkId, inChunkVersion);
    int64_t           theLeaseId = -1;
    ChunkServerAccess theAccess;
    int               theStatus  = GetChunkAccess(
        inLocation, inChunkId, theSizeOp.access, theLeaseId, &theAccess);
    if (theStatus < 0) {
        if (outUsedLeaseLocationsFlagPtr && ! theAccess.IsEmpty()) {
            *outUsedLeaseLocationsFlagPtr = true;
            ServerLocation theLocation;
            for (size_t i = 0; ; i++) {
                CryptoKeys::Key theKey;
                const ChunkServerAccess::Entry* const thePtr = theAccess.Get(
                    i, theLocation, inChunkId, theKey);
                if (! thePtr) {
                    break;
                }
                mChunkServer.SetKey(
                    thePtr->chunkServerAccessId.mPtr,
                    thePtr->chunkServerAccessId.mLen,
                    theKey.GetPtr(),
                    theKey.GetSize()
                );
                theSizeOp.access.assign(
                    thePtr->chunkAccess.mPtr, thePtr->chunkAccess.mLen);
                theSizeOp.status = 0;
                theSizeOp.statusMsg.clear();
                DoChunkServerOp(theLocation, theSizeOp);
                if (0 <= theSizeOp.status) {
                    break;
                }
            }
        } else {
            theSizeOp.status = theStatus;
        }
    } else {
        DoChunkServerOp(inLocation, theSizeOp);
    }
    if (0 <= theLeaseId) {
        LeaseRelinquishOp theLeaseRelinquishOp(0, inChunkId, theLeaseId);
        DoMetaOpWithRetry(&theLeaseRelinquishOp);
    }
    if (theSizeOp.status < 0) {
        return theSizeOp.status;
    }
    if (theSizeOp.size <= 0) {
        return 0;
    }
    if ((chunkOff_t)CHUNKSIZE < theSizeOp.size) {
        KFS_LOG_STREAM_ERROR <<
            "chunk size: " << theSizeOp.size <<
            " exceeds max chunk size: " << CHUNKSIZE <<
        KFS_LOG_EOM;
        return -EINVAL;
    }
    return theSizeOp.size;
}

int
KfsClientImpl::GetChunkFromReplica(
    const ChunkServerAccess& chunkServerAccess,
    const ServerLocation&    loc,
    kfsChunkId_t             chunkId,
    int64_t                  chunkVersion,
    ostream&                 os)
{
    SizeOp sizeOp(0, chunkId, chunkVersion);
    int status = SetChunkAccess(
        chunkServerAccess, loc, chunkId, sizeOp.access);
    if (status < 0) {
        return status;
    }
    DoChunkServerOp(loc, sizeOp);
    if (sizeOp.status < 0) {
        return sizeOp.status;
    }
    if (sizeOp.size <= 0) {
        return 0;
    }
    if ((chunkOff_t)CHUNKSIZE < sizeOp.size) {
        KFS_LOG_STREAM_ERROR <<
            "chunk size: " << sizeOp.size <<
            " exceeds max chunk size: " << CHUNKSIZE <<
        KFS_LOG_EOM;
        return -EINVAL;
    }
    chunkOff_t nread = 0;
    ReadOp op(0, chunkId, chunkVersion);
    op.access = sizeOp.access;
    while (nread < sizeOp.size) {
        op.seq           = 0;
        op.numBytes      = min(size_t(1) << 20, (size_t)(sizeOp.size - nread));
        op.offset        = nread;
        op.contentLength = 0;
        DoChunkServerOp(loc, op);
        if (op.status < 0) {
            nread = op.status;
            break;
        }
        if (op.numBytes != op.contentLength) {
            KFS_LOG_STREAM_ERROR <<
                "invalid read return: " << op.contentLength <<
                " exected: "            << op.numBytes <<
            KFS_LOG_EOM;
            nread = -EINVAL;
            break;
        }
        os.write(op.contentBuf, op.contentLength);
        nread += op.contentLength;
    }
    return (int)nread;
}

const string&
KfsClientImpl::UidToName(kfsUid_t uid, time_t now)
{
    if (mUserNamesIt == mUserNames.end() || mUserNamesIt->first != uid) {
        mUserNamesIt = mUserNames.find(uid);
    }
    if (mUserNamesIt == mUserNames.end() ||
            (mUseOsUserAndGroupFlag && mUserNamesIt->second.second < now)) {
        struct passwd  pwebuf = {0};
        struct passwd* pwe    = 0;
        const int      err    = mUseOsUserAndGroupFlag ?
            getpwuid_r((uid_t)uid, &pwebuf, mNameBuf, mNameBufSize, &pwe) :
            ENOENT;
        string name;
        if (err || ! pwe) {
            if (uid == kKfsUserNone) {
                static string empty;
                return empty;
            }
            ostream& os = mTmpOutputStream.Set(mTmpBuffer, kTmpBufferSize);
            os << uid;
            name.assign(mTmpBuffer, mTmpOutputStream.GetLength());
            mTmpOutputStream.Set();
        } else {
            name = pwe->pw_name;
        }
        pair<string, time_t> const val(
            name, now + mFileAttributeRevalidateTime);
        pair<UserNames::iterator, bool> const res = mUserNames.insert(
            make_pair(uid, val));
        if (! res.second) {
            res.first->second = val;
        }
        mUserNamesIt = res.first;
    }
    return mUserNamesIt->second.first;
}

const string&
KfsClientImpl::GidToName(kfsGid_t gid, time_t now)
{
    if (mGroupNamesIt == mUserNames.end() || mGroupNamesIt->first != gid) {
        mGroupNamesIt = mGroupNames.find(gid);
    }
    if (mGroupNamesIt == mGroupNames.end() ||
            (mUseOsUserAndGroupFlag && mGroupNamesIt->second.second < now)) {
        struct group  gbuf = {0};
        struct group* pge  = 0;
        const int     err  = mUseOsUserAndGroupFlag ?
            getgrgid_r((gid_t)gid, &gbuf, mNameBuf, mNameBufSize, &pge) :
            ENOENT;
        string name;
        if (err || ! pge) {
            if (gid == kKfsGroupNone) {
                static string empty;
                return empty;
            }
            ostream& os = mTmpOutputStream.Set(mTmpBuffer, kTmpBufferSize);
            os << gid;
            name.assign(mTmpBuffer, mTmpOutputStream.GetLength());
            mTmpOutputStream.Set();
        } else {
            name = pge->gr_name;
        }
        pair<string, time_t> const val(
            name, now + mFileAttributeRevalidateTime);
        pair<UserNames::iterator, bool> const res = mGroupNames.insert(
            make_pair(gid, val));
        if (! res.second) {
            res.first->second = val;
        }
        mGroupNamesIt = res.first;
    }
    return mGroupNamesIt->second.first;
}


kfsUid_t
KfsClientImpl::NameToUid(const string& name, time_t now)
{
    if (mUidIt == mUserIds.end() || mUidIt->first != name) {
        mUidIt = mUserIds.find(name);
    }
    if (mUidIt == mUserIds.end() ||
            (mUseOsUserAndGroupFlag && mUidIt->second.second < now)) {
        struct passwd  pwebuf = {0};
        struct passwd* pwe    = 0;
        const int err = mUseOsUserAndGroupFlag ?
            getpwnam_r(name.c_str(), &pwebuf, mNameBuf, mNameBufSize, &pwe) :
            ENOENT;
        kfsUid_t uid;
        if (err || ! pwe) {
            char* end = 0;
            uid = (kfsUid_t)strtol(name.c_str(), &end, 0);
            if (! end || *end > ' ') {
                uid = kKfsUserNone;
            }
        } else {
            uid = (kfsUid_t)pwe->pw_uid;
        }
        pair<kfsUid_t, time_t> const val(
            uid, now + mFileAttributeRevalidateTime);
        pair<UserIds::iterator, bool> const res = mUserIds.insert(
            make_pair(name, val));
        if (! res.second) {
            res.first->second = val;
        }
        mUidIt = res.first;
    }
    return mUidIt->second.first;
}

kfsGid_t
KfsClientImpl::NameToGid(const string& name, time_t now)
{
    if (mGidIt == mGroupIds.end() || mGidIt->first != name) {
        mGidIt = mGroupIds.find(name);
    }
    if (mGidIt == mGroupIds.end() ||
            (mUseOsUserAndGroupFlag && mGidIt->second.second < now)) {
        struct group  gbuf = {0};
        struct group* pge  = 0;
        const int     err  = mUseOsUserAndGroupFlag ?
            getgrnam_r(name.c_str(), &gbuf, mNameBuf, mNameBufSize, &pge) :
            ENOENT;
        kfsGid_t gid;
        if (err || ! pge) {
            char* end = 0;
            gid = (kfsUid_t)strtol(name.c_str(), &end, 0);
            if (! end || *end > ' ') {
                gid = kKfsGroupNone;
            }
        } else {
            gid = pge->gr_gid;
        }
        pair<kfsGid_t, time_t> const val(
            gid, now + mFileAttributeRevalidateTime);
        pair<GroupIds::iterator, bool> const res = mGroupIds.insert(
            make_pair(name, val));
        if (! res.second) {
            res.first->second = val;
        }
        mGidIt = res.first;
    }
    return mGidIt->second.first;
}

void
KfsClientImpl::UpdateUserAndGroup(const LookupOp& op, time_t now)
{
    if (op.status < 0) {
        return;
    }
    mInitLookupRootFlag = false;
    if (! op.userName.empty()) {
        UpdateUserId(op.userName, op.fattr.user, now);
    }
    if (! op.groupName.empty()) {
        UpdateGroupId(op.groupName, op.fattr.group, now);
    }
}

void
KfsClientImpl::DoNotUseOsUserAndGroup()
{
    if (! mUseOsUserAndGroupFlag) {
        return;
    }
    mUseOsUserAndGroupFlag = false;
    mInitLookupRootFlag    = false;
    mUserIds.clear();
    mUserNames.clear();
    mGroupIds.clear();
    mGroupNames.clear();
    mUidIt             = mUserIds.end();
    mGidIt             = mGroupIds.end();
    mUserNamesIt       = mUserNames.end();
    mGroupNamesIt      = mGroupNames.end();
    mUpdateUidIt       = mUserIds.end();
    mUpdateGidIt       = mGroupIds.end();
    mUpdateUserNameIt  = mUserNames.end();
    mUpdateGroupNameIt = mGroupNames.end();
}

template <typename T, typename IT, typename KT, typename VT>
static inline void
UpdateMap(T& inMap, IT& inIt, const KT& inKey, const VT& inVal)
{
    pair<IT, bool> const res = inMap.insert(make_pair(inKey, inVal));
    inIt = res.first;
    if (! res.second) {
        inIt->second = inVal;
    }
}

void
KfsClientImpl::UpdateUserId(const string& userName, kfsUid_t uid, time_t now)
{
    if (userName.empty()) {
        return;
    }
    if (mUseOsUserAndGroupFlag) {
        DoNotUseOsUserAndGroup();
    }
    const time_t exp = now + mFileAttributeRevalidateTime;
    if (mUpdateUserNameIt != mUserNames.end() &&
            mUpdateUserNameIt->first == uid &&
            mUpdateUidIt != mUserIds.end() &&
            mUpdateUidIt->first == userName) {
        mUpdateUidIt->second.first       = uid;
        mUpdateUidIt->second.second      = exp;
        mUpdateUserNameIt->second.first  = userName;
        mUpdateUserNameIt->second.second = exp;
        return;
    }
    UpdateMap(mUserIds, mUpdateUidIt, userName, make_pair(uid, exp));
    UpdateMap(mUserNames, mUpdateUserNameIt, uid, make_pair(userName, exp));
}

void
KfsClientImpl::UpdateGroupId(const string& groupName, kfsGid_t gid, time_t now)
{
    if (groupName.empty()) {
        return;
    }
    if (mUseOsUserAndGroupFlag) {
        DoNotUseOsUserAndGroup();
    }
    const time_t exp = now + mFileAttributeRevalidateTime;
    if (mUpdateGroupNameIt != mGroupNames.end() &&
            mUpdateGroupNameIt->first == gid &&
            mUpdateGidIt != mGroupIds.end() &&
            mUpdateGidIt->first == groupName) {
        mUpdateGidIt->second.first        = gid;
        mUpdateGidIt->second.second       = exp;
        mUpdateGroupNameIt->second.first  = groupName;
        mUpdateGroupNameIt->second.second = exp;
        return;
    }
    UpdateMap(mGroupIds, mUpdateGidIt, groupName, make_pair(gid, exp));
    UpdateMap(mGroupNames, mUpdateGroupNameIt, gid, make_pair(groupName, exp));
}

int
KfsClientImpl::GetUserAndGroupNames(kfsUid_t user, kfsGid_t group,
    string& uname, string& gname)
{
    QCStMutexLocker l(mMutex);
    const int ret = InitUserAndGroupMode();
    if (ret < 0) {
        return ret;
    }
    const time_t now = time(0);
    if (user != kKfsUserNone) {
        uname = UidToName(user, now);
    }
    if (group != kKfsGroupNone) {
        gname = GidToName(group, now);
    }
    return 0;
}

Properties*
KfsClientImpl::GetStats()
{
    QCStMutexLocker l(mMutex);
    StartProtocolWorker();
    Properties stats = mProtocolWorker->GetStats();
    if (stats.empty()) {
        return 0;
    }
    Properties* const ret = new Properties();
    ret->swap(stats);
    return ret;
}

} // client
} // KFS
