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

#include "common/config.h"
#include "common/Properties.h"
#include "common/MsgLogger.h"
#include "common/hsieh_hash.h"
#include "common/kfsatomic.h"
#include "common/MdStream.h"
#include "qcdio/qcstutils.h"
#include "qcdio/QCUtils.h"
#include "kfsio/checksum.h"
#include "kfsio/Globals.h"
#include "kfsio/requestio.h"
#include "Path.h"
#include "utils.h"
#include "KfsProtocolWorker.h"
#include "qcrs/rs.h"

#include <signal.h>
#include <openssl/rand.h>
#include <stdlib.h>

#include <cstdio>
#include <cstdlib>
#include <limits>
#include <cerrno>
#include <iostream>
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

using boost::scoped_array;
using boost::bind;

const int kMaxReaddirEntries = 1 << 10;
const int kMaxReadDirRetries = 16;

KfsClient*
Connect(const char* propFile)
{
    bool verbose = false;
#ifdef DEBUG
    verbose = true;
#endif
    Properties p;
    if (p.loadProperties(propFile, '=', verbose) != 0) {
        return 0;
    }
    return Connect(p.getValue("metaServer.name", ""),
                   p.getValue("metaServer.port", -1));
}

KfsClient*
Connect(const string& metaServerHost, int metaServerPort)
{
    KfsClient* const clnt = new KfsClient();
    clnt->Init(metaServerHost, metaServerPort);
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
    if (RAND_pseudo_bytes(
            reinterpret_cast<unsigned char*>(&ret), int(sizeof(ret))) < 0) {
        KFS_LOG_STREAM_WARN << "RAND_pseudo_bytes failure" << KFS_LOG_EOM;
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
    int stripeSize, int stripedType)
{
    return (
        (numReplicas <= 0 ||
        (stripedType != KFS_STRIPED_FILE_TYPE_NONE &&
            stripedType != KFS_STRIPED_FILE_TYPE_RS) ||
        (stripedType == KFS_STRIPED_FILE_TYPE_RS &&
                (numStripes <= 0 ||
                stripeSize < KFS_MIN_STRIPE_SIZE ||
                stripeSize > KFS_MAX_STRIPE_SIZE ||
                stripeSize % KFS_STRIPE_ALIGNMENT != 0 ||
                (numRecoveryStripes != 0 &&
                    (numRecoveryStripes != RS_LIB_MAX_RECOVERY_BLOCKS ||
                    numStripes > RS_LIB_MAX_DATA_BLOCKS))))
        ) ? -EINVAL : 0
    );
}

static MsgLogger::LogLevel
GetLogLevel(const char* logLevel)
{
    if (! logLevel || strcmp(logLevel, "INFO") == 0) {
        return MsgLogger::kLogLevelINFO;
    }
    if (strcmp(logLevel, "DEBUG") == 0) {
        return MsgLogger::kLogLevelDEBUG;
    }
    if (strcmp(logLevel, "WARN") == 0) {
        return MsgLogger::kLogLevelWARN;
    }
    return MsgLogger::kLogLevelINFO;
}

KfsClient::KfsClient()
    : mImpl(new KfsClientImpl())
{
}

KfsClient::~KfsClient()
{
    delete mImpl;
}

void
KfsClient::SetLogLevel(const string &logLevel)
{
    MsgLogger::SetLevel(GetLogLevel(logLevel.c_str()));
}

int
KfsClient::Init(const string &metaServerHost, int metaServerPort)
{
    return mImpl->Init(metaServerHost, metaServerPort);
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
    bool computeFilesize)
{
    return mImpl->ReaddirPlus(pathname, result, computeFilesize);
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
KfsClient::EnumerateBlocks(const char* pathname, KfsClient::BlockInfos& res)
{
    return mImpl->EnumerateBlocks(pathname, res);
}

int
KfsClient::GetReplication(const char* pathname,
    KfsFileAttr& attr, int& minChunkReplication, int& maxChunkReplication)
{
    return mImpl->GetReplication(pathname, attr,
        minChunkReplication, maxChunkReplication);
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
    int& stripeSize, int& stripedType)
{
    numReplicas        = 2;
    numStripes         = 0;
    numRecoveryStripes = 0;
    stripeSize         = 0;
    stripedType        = KFS_STRIPED_FILE_TYPE_NONE;
    if (! params || ! *params) {
        return 0;
    }
    if (params[0] == 'S' && params[1] == 0) {
        numReplicas        = 1;
        numStripes         = 6;
        numRecoveryStripes = 3;
        stripeSize         = 64 << 10,
        stripedType        = KFS_STRIPED_FILE_TYPE_RS;
        return 0;
    }
    char* p = 0;
    numReplicas = (int)strtol(params, &p, 10);
    if (numReplicas <= 0) {
        return -EINVAL;
    }
    if (*p == ',') numStripes         = (int)strtol(p + 1, &p, 10);
    if (*p == ',') numRecoveryStripes = (int)strtol(p + 1, &p, 10);
    if (*p == ',') stripeSize         = (int)strtol(p + 1, &p, 10);
    if (*p == ',') stripedType        = (int)strtol(p + 1, &p, 10);
    if (stripedType == KFS_STRIPED_FILE_TYPE_NONE) {
        numStripes         = 0;
        numRecoveryStripes = 0;
        stripeSize         = 0;
    }
    return ValidateCreateParams(numReplicas, numStripes, numRecoveryStripes,
        stripeSize, stripedType);
}

int
KfsClient::Create(const char *pathname, int numReplicas, bool exclusive,
    int numStripes, int numRecoveryStripes, int stripeSize, int stripedType,
    bool forceTypeFlag, kfsMode_t mode)
{
    return mImpl->Create(pathname, numReplicas, exclusive,
        numStripes, numRecoveryStripes, stripeSize, stripedType, forceTypeFlag,
        mode);
}


int
KfsClient::Create(const char *pathname, bool exclusive, const char *params)
{
    int numReplicas;
    int numStripes;
    int numRecoveryStripes;
    int stripeSize;
    int stripedType;
    const int ret = ParseCreateParams(
        params, numReplicas, numStripes, numRecoveryStripes,
        stripeSize, stripedType);
    if (ret) {
        return ret;
    }
    return mImpl->Create(pathname, numReplicas, exclusive,
        numStripes, numRecoveryStripes, stripeSize, stripedType, true);
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
    kfsMode_t mode)
{
    return mImpl->Open(pathname, openFlags, numReplicas,
        numStripes, numRecoveryStripes, stripeSize, stripedType, mode);
}

int
KfsClient::Open(const char *pathname, int openFlags, const char *params,
    kfsMode_t mode)
{
    int numReplicas;
    int numStripes;
    int numRecoveryStripes;
    int stripeSize;
    int stripedType;
    const int ret = ParseCreateParams(
        params, numReplicas, numStripes, numRecoveryStripes,
        stripeSize, stripedType);
    if (ret) {
        return ret;
    }
    return mImpl->Open(pathname, openFlags, numReplicas,
        numStripes, numRecoveryStripes, stripeSize, stripedType, mode);
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
                           vector< vector <string> > &locations)
{
    return mImpl->GetDataLocation(pathname, start, len, locations);
}

int
KfsClient::GetDataLocation(int fd, chunkOff_t start, chunkOff_t len,
                           vector< vector <string> > &locations)
{
    return mImpl->GetDataLocation(fd, start, len, locations);
}

int16_t
KfsClient::GetReplicationFactor(const char *pathname)
{
    return mImpl->GetReplicationFactor(pathname);
}

int16_t
KfsClient::SetReplicationFactor(const char *pathname, int16_t numReplicas)
{
    return mImpl->SetReplicationFactor(pathname, numReplicas);
}

int16_t
KfsClient::SetReplicationFactorR(const char *pathname, int16_t numReplicas,
    ErrorHandler* errHandler)
{
    return mImpl->SetReplicationFactorR(pathname, numReplicas, errHandler);
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
              mDefaultFileAttributeRevalidateTime(30)
        {
            signal(SIGPIPE, SIG_IGN);
            libkfsio::InitGlobals();
            const int maxGroups = min((int)sysconf(_SC_NGROUPS_MAX), 1 << 16);
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
            const char* p = getenv("KFS_CLIENT_DEFAULT_FATTR_REVALIDATE_TIME");
            if (p) {
                char* e = 0;
                const long v = strtol(p, &e, 10);
                if (p < e && (*e & 0xFF) <= ' ') {
                    mDefaultFileAttributeRevalidateTime = (int)v;
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
        QCStMutexLocker locker(mMutex);
        KfsOp::SetExtraRequestHeaders(string());
    }
    void InitSelf(KfsClientImpl& /* client */)
    {
        QCStMutexLocker locker(mMutex);
        if (! MsgLogger::IsLoggerInited()) {
            MsgLogger::Init(0, GetLogLevel(getenv("KFS_CLIENT_LOG_LEVEL")));
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

//
// Now, the real work is done by the impl object....
//

KfsClientImpl::KfsClientImpl()
    : mMutex(),
      mIsInitialized(false),
      mMetaServerLoc(),
      mMetaServerSock(),
      mCmdSeqNum(0),
      mCwd("/"),
      mFileTable(),
      mFidNameToFAttrMap(),
      mPathCache(),
      mPathCacheNone(mPathCache.insert(
        make_pair(string(), static_cast<KfsClientImpl::FAttr*>(0))).first),
      mFAttrPool(),
      mFreeFileTableEntires(),
      mFattrCacheSkipValidateCnt(0),
      mFileAttributeRevalidateTime(30),
      mFAttrCacheGeneration(0),
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
      mFreeCondVarsHead(0),
      mEUser(kKfsUserNone),
      mEGroup(kKfsGroupNone),
      mUMask(0),
      mGroups(),
      mCreateId(RandomSeqNo()),
      mUserNames(),
      mGroupNames(),
      mUserIds(),
      mGroupIds(),
      mTmpInputStream(),
      mTmpOutputStream(),
      mNameBufSize((size_t)max(max(
        sysconf(_SC_GETPW_R_SIZE_MAX),
        sysconf(_SC_GETGR_R_SIZE_MAX)),
        long(8) << 10)
      ),
      mNameBuf(new char[mNameBufSize])
{
    ClientsList::Insert(*this);

    QCStMutexLocker l(mMutex);

    FAttrLru::Init(mFAttrLru);
    mTmpPath.reserve(32);
    mTmpAbsPathStr.reserve(MAX_PATH_NAME_LENGTH);
    mTmpBuffer[kTmpBufferSize] = 0;
}

KfsClientImpl::~KfsClientImpl()
{
    ClientsList::Remove(*this);
    KfsClientImpl::Shutdown();

    QCStMutexLocker l(mMutex);
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
    if (! mProtocolWorker) {
        return;
    }
    l.Unlock();
    mProtocolWorker->Stop();
}

int KfsClientImpl::Init(const string &metaServerHost, int metaServerPort)
{
    ClientsList::Init(*this);

    mMetaServerLoc.hostname = metaServerHost;
    mMetaServerLoc.port = metaServerPort;

    KFS_LOG_STREAM_DEBUG <<
        "connecting to metaserver at: " <<
        metaServerHost << ":" << metaServerPort <<
    KFS_LOG_EOM;

    if (!mMetaServerLoc.IsValid()) {
        mIsInitialized = false;
        KFS_LOG_STREAM_ERROR <<
            "invalid metaserver location: " <<
            metaServerHost << ":" << metaServerPort <<
        KFS_LOG_EOM;
        return -1;
    }

    for (int attempt = 0; ;) {
        if (ConnectToMetaServer()) {
            mIsInitialized = true;
            break;
        }
        mIsInitialized = false;
        if (++attempt >= mMaxNumRetriesPerOp) {
            KFS_LOG_STREAM_ERROR <<
                "unable to connect to metaserver at: " <<
                metaServerHost << ":" << metaServerPort <<
                "; retrying..." <<
            KFS_LOG_EOM;
            break;
        }
        Sleep(mRetryDelaySec);
    }
    if (!mIsInitialized) {
        KFS_LOG_STREAM_ERROR <<
            "unable to connect to metaserver at: " <<
            metaServerHost << ":" << metaServerPort <<
            "; giving up" <<
        KFS_LOG_EOM;
        return -1;
    }

    mIsInitialized = true;
    return 0;
}

bool
KfsClientImpl::ConnectToMetaServer()
{
    return mMetaServerSock.Connect(mMetaServerLoc) >= 0;
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
        MkdirOp op(nextSeq(), mTmpPath.back().first, mTmpDirName.c_str(),
            Permissions(mEUser, mEGroup,
                mode != kKfsModeUndef ? (mode & ~mUMask) : mode),
            NextCreateId()
        );
        DoMetaOpWithRetry(&op);
        if ((res = op.status) == 0) {
            mTmpPath.push_back(make_pair(op.fileId, i));
            if (! createdFlag && (fa = LookupFAttr(ROOTFID, mSlash))) {
                fa->staleSubCountsFlag = true;
            }
            createdFlag = true;
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
    MkdirOp op(nextSeq(), parentFid, dirname.c_str(),
        Permissions(mEUser, mEGroup,
            mode != kKfsModeUndef ? (mode & ~mUMask) : mode),
        NextCreateId()
    );
    DoMetaOpWithRetry(&op);
    if (op.status < 0) {
        return op.status;
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
    RmdirOp op(nextSeq(), parentFid, dirname.c_str(), path.c_str());
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
    int res = ReaddirPlus(p, dirFid, entries, false, false);
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
    RmdirOp op(nextSeq(), parentFid, dirname.c_str(), p.c_str());
    DoMetaOpWithRetry(&op);
    return (op.status < 0 ? errHandler(p, op.status) : 0);
}

int
KfsClientImpl::Remove(const string& dirname, kfsFileId_t dirFid,
    const string& filename)
{
    string pathname = dirname + "/" + filename;
    RemoveOp op(nextSeq(), dirFid, filename.c_str(), pathname.c_str());
    DoMetaOpWithRetry(&op);
    return op.status;
}

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

    ReaddirOp op(0, attr.fileId);
    for (int retryCnt = kMaxReadDirRetries; ;) {
        op.seq                = nextSeq();
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
            result.clear();
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
        assert(op.contentBuf && op.contentBufLen >= op.contentLength);
        istream& ist = mTmpInputStream.Set(op.contentBuf, op.contentLength);
        result.reserve(result.size() + op.numEntries);
        for (int i = 0; i < op.numEntries; i++) {
            string line;
            if (! getline(ist, line) || line.empty()) {
                op.status = -EIO;
                break;
            }
            result.push_back(line);
        }
        if (! op.hasMoreEntriesFlag || op.status != 0) {
            break;
        }
        op.fnameStart = result.back();
    }
    if (op.status == 0) {
        sort(result.begin(), result.end());
        if (! op.fnameStart.empty()) {
            unique(result.begin(), result.end());
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
    bool computeFilesize)
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
    return ReaddirPlus(path, attr.fileId, result, computeFilesize);
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
            mEntry.lastChunkReplicas.GetPtr(),
            mEntry.lastChunkReplicas.GetSize()
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
    const KfsFileAttr& GetFattr() const
        { return mEntry; }
private:
    class Entry : public KfsFileAttr
    {
    public:
        enum { kCTimeUndef = 2 * 1000 * 1000 + 1 };
        chunkOff_t      lastChunkOffset;
        kfsFileId_t     chunkId;
        int64_t         chunkVersion;
        int             lastchunkNumReplicas;
        StringBufT<128> lastChunkReplicas;
        StringBufT<32>  type;

        Entry()
            : KfsFileAttr(),
              lastChunkOffset(0),
              chunkId(-1),
              chunkVersion(-1),
              lastchunkNumReplicas(0),
              lastChunkReplicas(),
              type()
          {}
        void Reset()
        {
            Clear();
            lastChunkOffset      = 0;
            chunkId              = -1;
            chunkVersion         = -1;
            lastchunkNumReplicas = 0;
            ctime.tv_usec        = kCTimeUndef;
            lastChunkReplicas.clear();
            type.clear();
        }
        bool Validate()
        {
            isDirectory = type.Compare("dir") == 0;
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
            switch (theVal) {
                case KFS_STRIPED_FILE_TYPE_NONE:
                    outValue = KFS_STRIPED_FILE_TYPE_NONE;
                    return;
                case KFS_STRIPED_FILE_TYPE_RS:
                    outValue = KFS_STRIPED_FILE_TYPE_RS;
                    return;
                default:
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
    const KfsFileAttr& attr)
{
    if (attr.filename == "." || attr.filename == "..") {
        return true;
    }
    const string path = dirname + "/" + attr.filename;
    FAttr* fa = LookupFAttr(dirFid, attr.filename);
    if (fa) {
        UpdatePath(fa, path, false);
        FAttrLru::PushBack(mFAttrLru, *fa);
    } else {
        fa = NewFAttr(dirFid, attr.filename, path);
        if (! fa) {
            return false;
        }
    }
    *fa                    = attr;
    fa->validatedTime      = now;
    fa->generation         = mFAttrCacheGeneration;
    fa->staleSubCountsFlag = false;
    return true;
}

int
KfsClientImpl::ReaddirPlus(const string& pathname, kfsFileId_t dirFid,
    vector<KfsFileAttr>& result, bool computeFilesize, bool updateClientCache)
{
    assert(mMutex.IsOwned());

    vector<ChunkAttr>                fileChunkInfo;
    ReaddirPlusParser                parser(mTmpInputStream);
    const PropertiesTokenizer::Token beginEntry("Begin-entry");
    const PropertiesTokenizer::Token shortBeginEntry("B");
    const bool                       kGetLastChunkInfoIfSizeUnknown = true;
    ReaddirPlusOp                    op(
        0, dirFid, kGetLastChunkInfoIfSizeUnknown);
    const time_t                     now     = time(0);
    bool                             hasDirs = false;
    for (int retryCnt = kMaxReadDirRetries; ;) {
        op.seq                = nextSeq();
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
            result.clear();
            fileChunkInfo.clear();
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
        if (op.numEntries <= 0) {
            break;
        }
        // The response format:
        // Begin-entry <values> Begin-entry <values>
        // The last entry doesn't have a end-marker.
        result.reserve(result.size() + op.numEntries);
        PropertiesTokenizer tokenizer(op.contentBuf, op.contentLength, false);
        tokenizer.Next();
        const PropertiesTokenizer::Token& beginToken =
            tokenizer.GetKey() == shortBeginEntry ?
                shortBeginEntry : beginEntry;
        if (&beginToken == &shortBeginEntry) {
            parser.SetUseHexParser();
        }
        for (int i = 0; i < op.numEntries; i++) {
            if (tokenizer.GetKey() != beginToken) {
                op.status = -EIO;
                break;
            }
            if (! parser.Parse(tokenizer)) {
                continue; // Skip empty entries.
            }
            result.push_back(parser.GetFattr());
            KfsFileAttr& attr = result.back();
            if (attr.filename.empty()) {
                op.status = -EIO;
                break;
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
            FAttr* const fa = LookupFAttr(dirFid, attr.filename);
            if (fa && ! fa->isDirectory && fa->fileSize >= 0) {
                if (IsValid(*fa, now)) {
                    attr.fileSize = fa->fileSize;
                    continue;
                }
            }
            fileChunkInfo.resize(result.size());
            parser.LastChunkInfo(fileChunkInfo.back());
        }
        if (! op.hasMoreEntriesFlag || op.status != 0) {
            break;
        }
        op.fnameStart = result.back().filename;
    }
    if (op.status != 0) {
        result.clear();
        return op.status;
    }
    ComputeFilesizes(result, fileChunkInfo);

    // if there are too many entries in the dir, then the caller is
    // probably scanning the directory.  don't put it in the cache
    string                 dirname(pathname);
    for (string::size_type len = dirname.size();
            len > 0 && dirname[len - 1] == '/';
            ) {
        dirname.erase(--len);
    }
    const size_t kMaxUpdateSize = 1 << 10;
    if (updateClientCache &&
            result.size() <= kMaxUpdateSize &&
            mFidNameToFAttrMap.size() < kMaxUpdateSize) {
        for (size_t i = 0; i < result.size(); i++) {
            if (! Cache(now, dirname, dirFid, result[i])) {
                break;
            }
        }
    } else if (updateClientCache && hasDirs) {
        size_t dirCnt            = 0;
        size_t kMaxDirUpdateSize = 1024;
        for (size_t i = 0; i < result.size(); i++) {
            if (! result[i].isDirectory) {
                continue;
            }
            if (! Cache(now, dirname, dirFid, result[i]) ||
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
        unique(result.begin(), result.end(),
            bind(&KfsFileAttr::filename, _1) ==
            bind(&KfsFileAttr::filename, _2)
        );
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
        int res = GetPathComponents(
            mTmpAbsPathStr.c_str(), &parentFid, filename, &fpath);
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
    LookupOp op(nextSeq(), parentFid, filename.c_str());
    DoMetaOpWithRetry(&op);
    if (op.status < 0) {
        Delete(fa);
        fa = 0;
        return op.status;
    }
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
    if (fa) {
        // Update i-node cache.
        UpdatePath(fa, path);
        FAttrLru::PushBack(mFAttrLru, *fa);
    } else {
        fa = NewFAttr(parentFid, filename, path);
        if (! fa) {
            return -ENOMEM;
        }
    }
    *fa                    = op.fattr;
    fa->validatedTime      = time(0);
    fa->generation         = mFAttrCacheGeneration;
    fa->staleSubCountsFlag = false;
    return 0;
}

int
KfsClientImpl::Create(const char *pathname, int numReplicas, bool exclusive,
    int numStripes, int numRecoveryStripes, int stripeSize, int stripedType,
    bool forceTypeFlag, kfsMode_t mode)
{
    QCStMutexLocker l(mMutex);
    return CreateSelf(pathname, numReplicas, exclusive,
        numStripes, numRecoveryStripes, stripeSize, stripedType, forceTypeFlag,
        mode);
}

int
KfsClientImpl::CreateSelf(const char *pathname, int numReplicas, bool exclusive,
    int numStripes, int numRecoveryStripes, int stripeSize, int stripedType,
    bool forceTypeFlag, kfsMode_t mode)
{
    if (! pathname || ! *pathname) {
        return -EINVAL;
    }

    assert(mMutex.IsOwned());
    int res = ValidateCreateParams(numReplicas, numStripes, numRecoveryStripes,
        stripeSize, stripedType);
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
    CreateOp op(nextSeq(), parentFid, filename.c_str(), numReplicas, exclusive,
        Permissions(mEUser, mEGroup,
            mode != kKfsModeUndef ? (mode & ~mUMask) : mode),
        exclusive ? NextCreateId() : -1
    );
    if (stripedType == KFS_STRIPED_FILE_TYPE_RS) {
        if (numStripes <= 0) {
            KFS_LOG_STREAM_DEBUG <<
                pathname << ": invalid stripe count: " << numStripes <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        if (numRecoveryStripes < 0) {
            KFS_LOG_STREAM_DEBUG <<
                pathname << ": invalid recovery stripe count: " <<
                    numRecoveryStripes <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        if (stripeSize < KFS_MIN_STRIPE_SIZE ||
                stripeSize > KFS_MAX_STRIPE_SIZE ||
                stripeSize % KFS_STRIPE_ALIGNMENT != 0 ||
                CHUNKSIZE % stripeSize != 0) {
            KFS_LOG_STREAM_DEBUG <<
                pathname << ": invalid stripe size: " << stripeSize <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        op.striperType        = KFS_STRIPED_FILE_TYPE_RS;
        op.numStripes         = numStripes;
        op.numRecoveryStripes = numRecoveryStripes;
        op.stripeSize         = stripeSize;
    } else if (stripedType != KFS_STRIPED_FILE_TYPE_NONE) {
        KFS_LOG_STREAM_DEBUG <<
            pathname << ": invalid striped file type: " << stripedType <<
        KFS_LOG_EOM;
        return -EINVAL;
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
        RemoveOp rm(nextSeq(), parentFid, filename.c_str(), pathname);
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
    if (op.metaStriperType != KFS_STRIPED_FILE_TYPE_NONE) {
        fa.numStripes         = (int16_t)numStripes;
        fa.numRecoveryStripes = (int16_t)numRecoveryStripes;
        fa.striperType        = (StripedFileType)stripedType;
        fa.stripeSize         = stripeSize;
    }
    static_cast<Permissions&>(fa) = op.permissions;
    // Set optimal io size, like open does.
    SetOptimalReadAheadSize(entry, mDefaultReadAheadSize);
    SetOptimalIoBufferSize(entry, mDefaultIoBufferSize);
    KFS_LOG_STREAM_DEBUG <<
        "created:"
        " fd: "       << fte <<
        " fileId: "   << entry.fattr.fileId <<
        " instance: " << entry.instance <<
        " mode: "     << entry.openMode <<
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
    RemoveOp op(nextSeq(), parentFid, filename.c_str(), path.c_str());
    DoMetaOpWithRetry(&op);
    Delete(LookupFAttr(parentFid, filename));
    return op.status;
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
    RenameOp op(nextSeq(), srcParentFid, srcFileName.c_str(),
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
        const size_t len      = path.length();
        int          maxInval =
            (int)min(size_t(256), mFidNameToFAttrMap.size() / 2 + 1);
        for (NameToFAttrMap::iterator it = mPathCache.lower_bound(path);
                it != mPathCache.end();
                ) {
            const string& cp = it->first;
            if (cp.length() < len || cp.compare(0, len, path) != 0) {
                break;
            }
            if (--maxInval < 0) {
                break;
            }
            FAttr* const fa = it->second;
            ++it;
            Delete(fa);
        }
        if (maxInval < 0) {
            InvalidateAllCachedAttrs();
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
    CoalesceBlocksOp op(nextSeq(), srcPath.c_str(), dstPath.c_str());
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
    SetMtimeOp op(nextSeq(), path.c_str(), mtime);
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
    const int fd = OpenSelf(pathname, O_RDONLY,
        3, 0, 0, 0, KFS_STRIPED_FILE_TYPE_NONE, false, kKfsModeUndef,
        &path);
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
    const int res = ReaddirPlus(path, entry.fattr.fileId, *entry.dirEntries, true);
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
    kfsMode_t mode)
{
    QCStMutexLocker l(mMutex);
    const bool kCacheAttributesFlag = false;
    return OpenSelf(pathname, openMode, numReplicas,
        numStripes, numRecoveryStripes, stripeSize, stripedType,
        kCacheAttributesFlag, mode);
}

int
KfsClientImpl::CacheAttributes(const char *pathname)
{
    return OpenSelf(pathname, 0, 0, 0, 0, 0, KFS_STRIPED_FILE_TYPE_NONE, true);
}

int
KfsClientImpl::OpenSelf(const char *pathname, int openMode, int numReplicas,
    int numStripes, int numRecoveryStripes, int stripeSize, int stripedType,
    bool cacheAttributesFlag, kfsMode_t mode, string* path)
{
    if ((openMode & O_TRUNC) != 0 && (openMode & (O_RDWR | O_WRONLY)) == 0) {
        return -EINVAL;
    }

    kfsFileId_t parentFid = -1;
    string      filename;
    string      fpath;
    const int res = GetPathComponents(pathname, &parentFid, filename, &fpath);
    if (res < 0) {
        return res;
    }
    if (path) {
        *path = fpath;
    }
    LookupOp op(0, parentFid, filename.c_str());
    FAttr* const fa    = LookupFAttr(parentFid, filename);
    time_t const faNow = fa ? time(0) : 0;
    if (fa && IsValid(*fa, faNow) &&
            (fa->isDirectory || fa->fileSize > 0 ||
                (fa->fileSize == 0 && fa->chunkCount() <= 0))) {
        UpdatePath(fa, fpath);
        op.fattr = *fa;
    } else {
        op.seq = nextSeq();
        DoMetaOpWithRetry(&op);
        if (op.status < 0) {
            Delete(fa);
            if (! cacheAttributesFlag && (openMode & O_CREAT) != 0 &&
                    op.status == -ENOENT) {
                // file doesn't exist.  Create it
                const int fte = CreateSelf(pathname, numReplicas, openMode & O_EXCL,
                    numStripes, numRecoveryStripes, stripeSize, stripedType, false,
                    mode);
                if (fte >= 0 && (openMode & O_APPEND) != 0) {
                    FileTableEntry& entry = *mFileTable[fte];
                    assert(! entry.fattr.isDirectory);
                    entry.openMode |= O_APPEND;
                }
                return fte;
            }
            return op.status;
        }
        if (fa) {
            UpdatePath(fa, fpath);
            *fa                    = op.fattr;
            fa->validatedTime      = faNow;
            fa->generation         = mFAttrCacheGeneration;
            fa->staleSubCountsFlag = false;
            FAttrLru::PushBack(mFAttrLru, *fa);
        }
    }
    // file exists; now fail open if: O_CREAT | O_EXCL
    if ((openMode & (O_CREAT|O_EXCL)) == (O_CREAT|O_EXCL)) {
        return -EEXIST;
    }
    if (op.fattr.isDirectory && openMode != O_RDONLY) {
        return -ENOTDIR;
    }

    const int fte = AllocFileTableEntry(parentFid, filename, fpath);
    if (fte < 0) { // Too many open files
        return fte;
    }

    FileTableEntry& entry = *mFileTable[fte];
    if (cacheAttributesFlag) {
        entry.openMode = 0;
    } else if ((openMode & O_RDWR) != 0) {
        entry.openMode = O_RDWR;
    } else if ((openMode & O_WRONLY) != 0) {
        entry.openMode = O_WRONLY;
    } else if ((openMode & O_RDONLY) != 0) {
        entry.openMode = O_RDONLY;
    } else {
        entry.openMode = 0;
    }
    entry.fattr = op.fattr;
    const bool truncateFlag =
        ! cacheAttributesFlag && (openMode & O_TRUNC) != 0;
    if (truncateFlag) {
        if (entry.fattr.chunkCount() > 0 || entry.fattr.fileSize != 0) {
            const int res = TruncateSelf(fte, 0);
            if (res < 0) {
                ReleaseFileTableEntry(fte);
                return res;
            }
        }
    } else if (entry.fattr.fileSize < 0 &&
            ! entry.fattr.isDirectory && entry.fattr.chunkCount() > 0) {
        entry.fattr.fileSize = ComputeFilesize(op.fattr.fileId);
        if (entry.fattr.fileSize < 0) {
            ReleaseFileTableEntry(fte);
            return -EIO;
        }
    }
    if (! cacheAttributesFlag &&
            (openMode & O_APPEND) != 0  &&
            ! entry.fattr.isDirectory &&
            (entry.openMode & (O_RDWR | O_WRONLY)) != 0) {
        entry.openMode |= O_APPEND;
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
            KfsProtocolWorker::kRequestTypeReadClose,
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
    if (! attr.CanWrite(mEUser, mEGroup)) {
        return -EACCES;
    }
    TruncateOp op(nextSeq(), pathname, attr.fileId, offset);
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
    if ((mFileTable[fd]->openMode & (O_RDWR | O_WRONLY)) == 0) {
        return -EINVAL;
    }
    FdInfo(fd)->buffer.Invalidate();

    FileAttr *fa = FdAttr(fd);
    TruncateOp op(nextSeq(), FdInfo(fd)->pathname.c_str(), fa->fileId, offset);
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
    TruncateOp op(nextSeq(), FdInfo(fd)->pathname.c_str(), fa->fileId, offset);
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
KfsClientImpl::GetDataLocation(const char *pathname, chunkOff_t start, chunkOff_t len,
                           vector< vector <string> > &locations)
{
    QCStMutexLocker l(mMutex);

    // Open the file and cache the attributes
    const int fd = CacheAttributes(pathname);
    if (fd < 0) {
        return fd;
    }
    const int ret = GetDataLocationSelf(fd, start, len, locations);
    ReleaseFileTableEntry(fd);
    return ret;
}

int
KfsClientImpl::GetDataLocation(int fd, chunkOff_t start, chunkOff_t len,
                               vector< vector <string> > &locations)
{
    QCStMutexLocker l(mMutex);
    return GetDataLocationSelf(fd, start, len, locations);
}

int
KfsClientImpl::GetDataLocationSelf(int fd, chunkOff_t start, chunkOff_t len,
    vector<vector<string> > &locations)
{
    assert(mMutex.IsOwned());

    if (! valid_fd(fd)) {
        return -EBADF;
    }
    int       res;
    ChunkAttr chunk;
    // locate each chunk and get the hosts that are storing the chunk.
    for (chunkOff_t pos = start / (chunkOff_t)CHUNKSIZE * (chunkOff_t)CHUNKSIZE;
            pos < start + len;
            pos += CHUNKSIZE) {
        if ((res = LocateChunk(fd, pos, chunk)) < 0) {
            return res;
        }
        locations.push_back(vector<string>());
        vector<string>& hosts = locations.back();
        const size_t cnt = chunk.chunkServerLoc.size();
        for (size_t i = 0; i < cnt; i++) {
            hosts.push_back(chunk.chunkServerLoc[i].hostname);
        }
    }

    return 0;
}

int16_t
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

int16_t
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
    ChangeFileReplicationOp op(nextSeq(), attr.fileId, numReplicas);
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
        mProtocolWorker->SetMetaOpTimeoutSec(mDefaultOpTimeout);
    }
}

int
KfsClientImpl::GetDefaultIOTimeout() const
{
    QCStMutexLocker l(const_cast<KfsClientImpl*>(this)->mMutex);
    return mDefaultOpTimeout;
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
    mProtocolWorker = new KfsProtocolWorker(
        mMetaServerLoc.hostname, mMetaServerLoc.port);
    mProtocolWorker->SetOpTimeoutSec(mDefaultOpTimeout);
    mProtocolWorker->SetMetaOpTimeoutSec(mDefaultOpTimeout);
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
    GetAllocOp op(nextSeq(), mFileTable[fd]->fattr.fileId, chunkOffset);
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
    chunk.chunkServerLoc = op.chunkServers;
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
        bufSize = (max(optimalFlag ? (1 << 20) * stripes : 0, bufSize) +
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
/// Helper function that does the work for sending out an op to the
/// server.
///
/// @param[in] op the op to be sent out
/// @param[in] sock the socket on which we communicate with server
/// @retval 0 on success; -1 on failure
/// (On failure, op->status contains error code.)
///
int
KfsClientImpl::DoOpSend(KfsOp *op, TcpSocket *sock)
{
    if (! sock || ! sock->IsGood()) {
        KFS_LOG_STREAM_DEBUG << "op send socket closed" << KFS_LOG_EOM;
        op->status = -EHOSTUNREACH;
        return -1;
    }
    ostream& os = mTmpOutputStream.Set(mTmpBuffer, kTmpBufferSize);
    op->Request(os);
    const size_t len = mTmpOutputStream.GetLength();
    mTmpOutputStream.Set();
    if (len > (size_t)MAX_RPC_HEADER_LEN) {
        KFS_LOG_STREAM_WARN <<
            "request haeder exceeds max. allowed size: " <<
            MAX_RPC_HEADER_LEN <<
            " op: " << op->Show() <<
        KFS_LOG_EOM;
        op->status = -EINVAL;
        return op->status;
    }
    const int ret = SendRequest(mTmpBuffer, len,
        op->contentBuf, op->contentLength, sock);
    if (ret <= 0) {
        op->status = -EHOSTUNREACH;
    }
    return ret;
}

int
KfsClientImpl::GetResponse(char *buf, int bufSize, int *delims, TcpSocket *sock)
{
    return RecvResponseHeader(buf, bufSize, sock, mDefaultOpTimeout, delims);
}

///
/// From a response, extract out seq # and content-length.
///
static void
GetSeqContentLen(const char* buf, size_t len,
    kfsSeq_t *seq, int *contentLength, Properties& prop)
{
    const char separator = ':';
    prop.clear();
    prop.loadProperties(buf, len, separator);
    *seq = prop.getValue("Cseq", (kfsSeq_t) -1);
    *contentLength = prop.getValue("Content-length", 0);
}

///
/// Helper function that does the work of getting a response from the
/// server and parsing it out.
///
/// @param[in] op the op for which a response is to be gotten
/// @param[in] sock the socket on which we communicate with server
/// @retval 0 on success; -1 on failure
/// (On failure, op->status contains error code.)
///
int
KfsClientImpl::DoOpResponse(KfsOp *op, TcpSocket *sock)
{
    if (! sock || ! sock->IsGood()) {
        op->status = -EHOSTUNREACH;
        KFS_LOG_STREAM_DEBUG << "op recv socket closed" << KFS_LOG_EOM;
        return -1;
    }

    Properties prop;
    int        numIO;
    bool       printMatchingResponse = false;
    int        len;
    for (; ;) {
        len = 0;
        numIO = GetResponse(mTmpBuffer, kTmpBufferSize, &len, sock);
        if (numIO <= 0) {
            KFS_LOG_STREAM_DEBUG <<
                sock->GetPeerName() << ": read failed: " << numIO <<
                    " " << QCUtils::SysError(-numIO) <<
            KFS_LOG_EOM;
            op->status = numIO == -ETIMEDOUT ? -ETIMEDOUT : -EHOSTUNREACH;
            sock->Close();
            return -1;
        }
        if (len <= 0) {
            KFS_LOG_STREAM_DEBUG <<
                sock->GetPeerName() << ": invalid response length: " << len <<
            KFS_LOG_EOM;
            sock->Close();
            op->status = -EINVAL;
            return -1;
        }
        kfsSeq_t resSeq     = -1;
        int      contentLen = 0;
        GetSeqContentLen(mTmpBuffer, len, &resSeq, &contentLen, prop);
        if (resSeq == op->seq) {
            if (printMatchingResponse) {
                KFS_LOG_STREAM_DEBUG <<
                    sock->GetPeerName() << ": response seq: " << resSeq <<
                KFS_LOG_EOM;
            }
            break;
        }
        KFS_LOG_STREAM_DEBUG <<
            sock->GetPeerName() << ": unexpected response seq:"
            " expect: " << op->seq <<
            " got "     << resSeq <<
        KFS_LOG_EOM;
        printMatchingResponse = true;
        if (contentLen > 0) {
            struct timeval timeout = {0};
            timeout.tv_sec = mDefaultOpTimeout;
            int len = sock->DoSynchDiscard(contentLen, timeout);
            if (len != contentLen) {
                sock->Close();
                op->status = -EHOSTUNREACH;
                return -1;
            }
        }
    }

    const int contentLen = op->contentLength;
    op->ParseResponseHeader(prop);
    if (op->contentLength == 0) {
        // restore it back: when a write op is sent out and this
        // method is invoked with the same op to get the response, the
        // op's status should get filled in; we shouldn't be stomping
        // over content length.
        op->contentLength = contentLen;
        return numIO;
    }

    if (! op->contentBuf || op->contentBufLen < op->contentLength + 1) {
        delete [] op->contentBuf;
        op->contentBuf = 0;
        op->contentBuf = new char[op->contentLength + 1];
        op->contentBuf[op->contentLength] = '\0';
        op->contentBufLen = op->contentLength + 1;
    }

    // len bytes belongs to the RPC reply.  Whatever is left after
    // stripping that data out is the data.
    const ssize_t navail = numIO - len;
    if (navail > 0) {
        assert(navail <= (ssize_t)op->contentLength);
        memcpy(op->contentBuf, mTmpBuffer + len, navail);
    }
    ssize_t nleft = op->contentLength - navail;

    assert(nleft >= 0);

    int nread = 0;
    if (nleft > 0) {
        struct timeval timeout = {0};
        timeout.tv_sec = mDefaultOpTimeout;
        nread = sock->DoSynchRecv(op->contentBuf + navail, nleft, timeout);
        if (nread <= 0) {
            KFS_LOG_STREAM_DEBUG <<
                sock->GetPeerName() << ": read failed: " << nread <<
                    " " << QCUtils::SysError(-nread) <<
            KFS_LOG_EOM;
            op->status = nread == -ETIMEDOUT ? -ETIMEDOUT : -EHOSTUNREACH;
            sock->Close();
            return -1;
        }
    }

    return nread + numIO;
}


///
/// Common work for each op: build a request; send it to server; get a
/// response; parse it.
///
/// @param[in] op the op to be done
/// @param[in] sock the socket on which we communicate with server
///
/// @retval # of bytes read from the server.
///
int
KfsClientImpl::DoOpCommon(KfsOp *op, TcpSocket *sock)
{
    assert(sock);
    int res = DoOpSend(op, sock);
    if (res < 0) {
        return res;
    }
    res = DoOpResponse(op, sock);
    if (res < 0) {
        return res;
    }
    if (op->status < 0) {
        KFS_LOG_STREAM_DEBUG << op->Show() <<
            " failed: " << op->status << " " << ErrorCodeToStr(op->status) <<
        KFS_LOG_EOM;
    }
    return res;
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
struct RespondingServer {
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
        status = -EIO;
        size   = -1;

        TcpSocket sock;
        if (sock.Connect(loc) < 0) {
            size = 0;
            return false;
        }
        SizeOp sop(client.nextSeq(), layout.chunkId, layout.chunkVersion);
        sop.status = -1;
        const int numIO = client.DoOpCommon(&sop, &sock);
        if (numIO < 0 || ! sock.IsGood()) {
            return false;
        }
        status = sop.status;
        if (status >= 0) {
            size = sop.size;
        }
        return status >= 0;
    }
};

struct RespondingServer2 {
    KfsClientImpl&         client;
    const ChunkLayoutInfo& layout;
    RespondingServer2(KfsClientImpl& cli, const ChunkLayoutInfo& lay)
        : client(cli), layout(lay)
        {}
    ssize_t operator() (const ServerLocation& loc)
    {
        TcpSocket sock;
        if (sock.Connect(loc) < 0) {
            return -1;
        }

        SizeOp sop(client.nextSeq(), layout.chunkId, layout.chunkVersion);
        int numIO = client.DoOpCommon(&sop, &sock);
        if ((numIO < 0 && ! sock.IsGood()) || sop.status < 0) {
            return -1;
        }
        return sop.size;
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
        LookupOp op(nextSeq(), entry.parentFid, entry.name.c_str());
        DoMetaOpWithRetry(&op);
        if (op.status < 0) {
            Delete(fa);
            return op.status;
        }
        if (op.fattr.fileId != entry.fattr.fileId) {
            Delete(fa);
            return 0; // File doesn't exists anymore, or in the dumpster.
        }
        entry.fattr = op.fattr;
        if (fa) {
            *fa                    = op.fattr;
            fa->validatedTime      = time(0);
            fa->generation         = mFAttrCacheGeneration;
            fa->staleSubCountsFlag = false;
            FAttrLru::PushBack(mFAttrLru, *fa);
        }
        if (entry.fattr.fileSize >= 0 || entry.fattr.isDirectory) {
            return 0;
        }
    }
    const chunkOff_t res = ComputeFilesize(entry.fattr.fileId);
    if (res >= 0) {
        FdAttr(fd)->fileSize = res;
        if (fa) {
            fa->fileSize = res;
        }
    }
    return 0;
}

chunkOff_t
KfsClientImpl::ComputeFilesize(kfsFileId_t kfsfid)
{
    GetLayoutOp lop(nextSeq(), kfsfid);
    lop.lastChunkOnlyFlag = true;
    DoMetaOpWithRetry(&lop);
    if (lop.status < 0) {
        KFS_LOG_STREAM_ERROR <<
            "failed to compute filesize fid: " << kfsfid <<
            " status: " << lop.status <<
        KFS_LOG_EOM;
        return -1;
    }
    if (lop.ParseLayoutInfo()) {
        KFS_LOG_STREAM_ERROR <<
            "failed to parse layout info fid: " << kfsfid <<
        KFS_LOG_EOM;
        return -1;
    }
    if (lop.chunks.empty()) {
        return 0;
    }
    const ChunkLayoutInfo& last = *lop.chunks.rbegin();
    chunkOff_t filesize = last.fileOffset;
    chunkOff_t endsize  = 0;
    int        rstatus  = 0;
    for (int retry = 0; retry < max(1, mMaxNumRetriesPerOp); retry++) {
        if (retry > 0) {
            Sleep(mRetryDelaySec);
        }
        if (find_if(last.chunkServers.begin(), last.chunkServers.end(),
                        RespondingServer(*this, last, endsize, rstatus)) !=
                    last.chunkServers.end()) {
            break;
        }
        KFS_LOG_STREAM_INFO <<
            "failed to connect to any server to get size of"
            " fid: "   << kfsfid <<
            " chunk: " << last.chunkId <<
            " retry: " << retry <<
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
    TcpSocket sock;
    if (sock.Connect(loc) < 0) {
        return;
    }
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
        SizeOp sop(nextSeq(), cattr.chunkId, cattr.chunkVersion);
        const int numIO = DoOpCommon(&sop, &sock);
        if (numIO < 0 && ! sock.IsGood()) {
            return;
        }
        fa.fileSize = lastChunkInfo[i].chunkOffset;
        if (sop.status >= 0) {
            fa.fileSize += sop.size;
        }
    }
}

///
/// Wrapper for retrying ops with the metaserver.
///
void
KfsClientImpl::DoMetaOpWithRetry(KfsOp *op)
{
    time_t start = time(0);
    for (int attempt = -1; ;) {
        if (! mMetaServerSock.IsGood()) {
            ConnectToMetaServer();
        }
        op->status = 0;
        const int res = DoOpCommon(op, &mMetaServerSock);
        if (res < 0 && op->status == 0) {
            op->status = res;
        }
        if (op->status != -EHOSTUNREACH && op->status != -ETIMEDOUT) {
            break;
        }
        mMetaServerSock.Close();
        const time_t now = time(0);
        if (++attempt == 0) {
            if (now <= start + 1) {
                continue; // Most likely idle connection timeout.
            }
            ++attempt;
        }
        if (attempt >= mMaxNumRetriesPerOp) {
            break;
        }
        start += mRetryDelaySec;
        if (now < start) {
            Sleep(start - now);
        } else {
            start = now;
        }
        // re-issue the op with a new sequence #
        op->seq = nextSeq();
    }
    KFS_LOG_STREAM_DEBUG <<
        op->Show() << " status: " << op->status <<
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
    const int last = (int)mFileTable.size();
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
    if (mFidNameToFAttrMap.size() > 128 && ++mFattrCacheSkipValidateCnt > 512) {
        mFattrCacheSkipValidateCnt = 0;
        ValidateFAttrCache(time(0), 64);
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
            if (cfa->generation == mFAttrCacheGeneration ||
                    cfa->nameIt != res.first) {
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
    mFidNameToFAttrMap.erase(fa->fidNameIt);
    if (fa->nameIt != mPathCacheNone) {
        mPathCache.erase(fa->nameIt);
    }
    FAttrLru::Remove(mFAttrLru, *fa);
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
    LookupOp op(nextSeq(), parentFid, name.c_str());
    DoMetaOpWithRetry(&op);
    if (op.status < 0) {
        if (fa) {
            Delete(fa);
        }
        return op.status;
    }
    // Update i-node cache.
    // This method presently called only from the path traversal.
    // Force new path string allocation to keep "path" buffer mutable,
    // assuming string class implementation with ref. counting, of course.
    if (fa) {
        UpdatePath(fa, path);
        FAttrLru::PushBack(mFAttrLru, *fa);
    } else if (! (fa = NewFAttr(parentFid, name,
            string(path.data(), path.length())))) {
        return -ENOMEM;
    }
    fa->validatedTime      = now;
    fa->generation         = mFAttrCacheGeneration;
    fa->staleSubCountsFlag = false;
    *fa                    = op.fattr;
    return 0;
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

    GetLayoutOp lop(nextSeq(), attr.fileId);
    DoMetaOpWithRetry(&lop);
    if (lop.status < 0) {
        KFS_LOG_STREAM_ERROR << "get layout failed on path: " << pathname << " "
             << ErrorCodeToStr(lop.status) <<
        KFS_LOG_EOM;
        return lop.status;
    }
    if (lop.ParseLayoutInfo()) {
        KFS_LOG_STREAM_ERROR << "unable to parse layout for path: " << pathname <<
        KFS_LOG_EOM;
        return -EFAULT;
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

int
KfsClientImpl::EnumerateBlocks(const char* pathname, KfsClient::BlockInfos& res)
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

    GetLayoutOp lop(nextSeq(), attr.fileId);
    DoMetaOpWithRetry(&lop);
    if (lop.status < 0) {
        KFS_LOG_STREAM_ERROR << "get layout failed on path: " << pathname << " "
             << ErrorCodeToStr(lop.status) <<
        KFS_LOG_EOM;
        return lop.status;
    }

    if (lop.ParseLayoutInfo()) {
        KFS_LOG_STREAM_ERROR << "unable to parse layout for path: " << pathname <<
        KFS_LOG_EOM;
        return -EINVAL;
    }

    vector<ssize_t> chunksize;
    for (vector<ChunkLayoutInfo>::const_iterator i = lop.chunks.begin();
            i != lop.chunks.end();
            ++i) {
        if (i->chunkServers.empty()) {
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
    TcpSocket sock;
    int ret;
    if ((ret = sock.Connect(loc)) < 0) {
        return ret;
    }
    GetChunkMetadataOp op(nextSeq(), chunkId, readVerifyFlag);
    const int numIO = DoOpCommon(&op, &sock);
    if (numIO <= 0) {
        return (numIO < 0 ? numIO : -EINVAL);
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
    GetLayoutOp lop(nextSeq(), fileId);
    DoMetaOpWithRetry(&lop);
    if (lop.status < 0) {
        KFS_LOG_STREAM_ERROR << "Get layout failed with error: "
             << ErrorCodeToStr(lop.status) <<
        KFS_LOG_EOM;
        return lop.status;
    }
    if (lop.ParseLayoutInfo()) {
        KFS_LOG_STREAM_ERROR << "unable to parse layout info" <<
        KFS_LOG_EOM;
        return -EINVAL;
    }
    const size_t numChecksums = CHUNKSIZE / CHECKSUM_BLOCKSIZE;
    scoped_array<uint32_t> chunkChecksums1;
    chunkChecksums1.reset(new uint32_t[numChecksums]);
    scoped_array<uint32_t> chunkChecksums2;
    chunkChecksums2.reset(new uint32_t[numChecksums]);
    for (vector<ChunkLayoutInfo>::const_iterator i = lop.chunks.begin();
            i != lop.chunks.end();
            ++i) {
        int ret;
        if ((ret = GetDataChecksums(
                i->chunkServers[0], i->chunkId, chunkChecksums1.get())) < 0) {
            KFS_LOG_STREAM_ERROR << "failed to get checksums from server " <<
                i->chunkServers[0] << " " << ErrorCodeToStr(ret) <<
            KFS_LOG_EOM;
            return ret;
        }
        for (size_t k = 1; k < i->chunkServers.size(); k++) {
            if ((ret = GetDataChecksums(
                    i->chunkServers[k], i->chunkId,
                    chunkChecksums2.get())) < 0) {
                KFS_LOG_STREAM_ERROR << "didn't get checksums from server: " <<
                    i->chunkServers[k] << " " << ErrorCodeToStr(ret) <<
                KFS_LOG_EOM;
                return ret;
            }
            bool mismatch = false;
            for (size_t v = 0; v < numChecksums; v++) {
                if (chunkChecksums1[v] != chunkChecksums2[v]) {
                    KFS_LOG_STREAM_ERROR <<
                        "checksum mismatch between servers: " <<
                        i->chunkServers[0] << " " << i->chunkServers[k] <<
                    KFS_LOG_EOM;
                    mismatch = true;
                }
            }
            if (mismatch) {
                return 1;
            }
        }
    }
    return 0;
}

int
KfsClientImpl::GetFileOrChunkInfo(kfsFileId_t fileId, kfsChunkId_t chunkId,
    KfsFileAttr& fattr, chunkOff_t& offset, int64_t& chunkVersion,
    vector<ServerLocation>& servers)
{
    QCStMutexLocker l(mMutex);
    GetPathNameOp op(nextSeq(), fileId, chunkId);
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
    ChmodOp op(nextSeq(), attr.fileId, mode & (attr.isDirectory ?
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
    return mEUser;
}

int
KfsClientImpl::GetUserAndGroup(const char* user, const char* group,
    kfsUid_t& uid, kfsGid_t& gid)
{
    uid = kKfsUserNone;
    gid = kKfsGroupNone;

    QCStMutexLocker l(mMutex);

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
KfsClientImpl::RecursivelyApply(string& path, const KfsFileAttr& attr, T& functor)
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
                path, attr.fileId, entries, false, false)) < 0) {
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
KfsClientImpl::RecursivelyApply(const char* pathname, T& functor)
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
    res = RecursivelyApply(path, attr, functor);
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
    ChmodOp op(nextSeq(), entry.fattr.fileId, mode & (entry.fattr.isDirectory ?
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
        ChmodOp op(mCli.nextSeq(), attr.fileId, mMode & (attr.isDirectory ?
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
    kfsUid_t uid = kKfsUserNone;
    kfsGid_t gid = kKfsGroupNone;
    int      ret = GetUserAndGroup(user, group, uid, gid);
    if (ret != 0) {
        return ret;
    }
    return Chown(fd, uid, gid);
}

int
KfsClientImpl::Chown(int fd, kfsUid_t user, kfsGid_t group)
{
    if (user == kKfsUserNone && group == kKfsGroupNone) {
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
    if (mEUser != kKfsUserRoot && (user == kKfsUserNone || user == mEUser) &&
            find(mGroups.begin(), mGroups.end(), group) ==
                mGroups.end()) {
        return -EPERM;
    }
    ChownOp op(nextSeq(), entry.fattr.fileId, user, group);
    DoMetaOpWithRetry(&op);
    if (op.status != 0) {
        return op.status;
    }
    if (user != kKfsUserNone) {
        entry.fattr.user = user;
    }
    if (group != kKfsGroupNone) {
        entry.fattr.group = group;
    }
    return 0;
}

int
KfsClientImpl::Chown(const char* pathname, const char* user, const char* group)
{
    kfsUid_t uid = kKfsUserNone;
    kfsGid_t gid = kKfsGroupNone;
    int      ret = GetUserAndGroup(user, group, uid, gid);
    if (ret != 0) {
        return ret;
    }
    return Chown(pathname, uid, gid);
}

int
KfsClientImpl::Chown(const char* pathname, kfsUid_t user, kfsGid_t group)
{
    if (user == kKfsUserNone && group == kKfsGroupNone) {
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
    if (mEUser != kKfsUserRoot && (user == kKfsUserNone || user == mEUser) &&
            find(mGroups.begin(), mGroups.end(), group) ==
                mGroups.end()) {
        return -EPERM;
    }
    ChownOp op(nextSeq(), attr.fileId, user, group);
    DoMetaOpWithRetry(&op);
    if (op.status != 0 || ! fa) {
        return op.status;
    }
    if (user != kKfsUserNone) {
        fa->user = user;
    }
    if (group != kKfsGroupNone) {
        fa->group = group;
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
    kfsUid_t uid = kKfsUserNone;
    kfsGid_t gid = kKfsGroupNone;
    int      ret = GetUserAndGroup(user, group, uid, gid);
    if (ret != 0) {
        return ret;
    }
    return ChownR(pathname, uid, gid, errHandler);
}

class ChownFunc
{
public:
    typedef KfsClient::ErrorHandler ErrorHandler;

    ChownFunc(KfsClientImpl& cli, kfsUid_t user, kfsGid_t group,
        ErrorHandler& errHandler)
        : mCli(cli),
          mUser(user),
          mGroup(group),
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
        ChownOp op(mCli.nextSeq(), attr.fileId, mUser, mGroup);
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
    const kfsUid_t mUser;
    const kfsGid_t mGroup;
    ErrorHandler&  mErrHandler;
};

int
KfsClientImpl::ChownR(const char* pathname, kfsUid_t user, kfsGid_t group,
    KfsClientImpl::ErrorHandler* errHandler)
{
    QCStMutexLocker l(mMutex);

    if (mEUser != kKfsUserRoot && (user == kKfsUserNone || user == mEUser) &&
            find(mGroups.begin(), mGroups.end(), group) ==
                mGroups.end()) {
        return -EPERM;
    }
    DefaultErrHandler errorHandler;
    ChownFunc funct(*this, user, group, errHandler ? *errHandler : errorHandler);
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
        ChangeFileReplicationOp op(mCli.nextSeq(), attr.fileId, mReplication);
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

int16_t
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
    return ClientsList::SetEUserAndEGroup(user, group, groups, groupsCnt);
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

    GetLayoutOp lop(nextSeq(), attr.fileId);
    DoMetaOpWithRetry(&lop);
    if (lop.status < 0) {
        KFS_LOG_STREAM_ERROR << "get layout error: " <<
            ErrorCodeToStr(lop.status) <<
        KFS_LOG_EOM;
        return lop.status;
    }
    if (lop.ParseLayoutInfo()) {
        KFS_LOG_STREAM_ERROR << "Unable to parse layout info!" << KFS_LOG_EOM;
        return -EINVAL;
    }
    MdStream mdsAll;
    MdStream mds;
    bool     match = true;
    for (vector<ChunkLayoutInfo>::const_iterator i = lop.chunks.begin();
         i != lop.chunks.end();
         ++i) {
        LeaseAcquireOp leaseOp(nextSeq(), i->chunkId, pathname);
        DoMetaOpWithRetry(&leaseOp);
        if (leaseOp.status < 0) {
            KFS_LOG_STREAM_ERROR << "failed to acquire lease: " <<
                " chunk: " << i->chunkId <<
                ErrorCodeToStr(leaseOp.status) <<
            KFS_LOG_EOM;
            return leaseOp.status;
        }
        mdsAll.flush();
        mds.Reset(&mdsAll);
        const int nbytes = GetChunkFromReplica(
            i->chunkServers[0], i->chunkId, i->chunkVersion, mds);
        if (nbytes < 0) {
            KFS_LOG_STREAM_ERROR << i->chunkServers[0] <<
                 ": " << ErrorCodeToStr(nbytes) <<
            KFS_LOG_EOM;
            match = false;
            continue;
        }
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
                i->chunkServers[k], i->chunkId, i->chunkVersion, mds);
            if (n < 0) {
                KFS_LOG_STREAM_ERROR << i->chunkServers[0] <<
                     ": " << ErrorCodeToStr(n) <<
                KFS_LOG_EOM;
                match = false;
                continue;
            }
            const string md5sumCur = mds.GetMd();
            KFS_LOG_STREAM_DEBUG <<
                "chunk: "    << i->chunkId <<
                " replica: " << i->chunkServers[k] <<
                " size: "    << nbytes <<
                " md5sum: "  << md5sumCur <<
            KFS_LOG_EOM;
            if (nbytes != n || md5sumFirst != md5sumCur) {
                match = false;
            }
            if (! match) {
                KFS_LOG_STREAM_ERROR <<
                    "chunk: " << i->chunkId <<
                    (nbytes != n ? "size" : "data") <<
                    " mismatch: " << i->chunkServers[0] <<
                    " size: "     << nbytes <<
                    " md5sum: "   << md5sumFirst <<
                    " vs "        << i->chunkServers[k] <<
                    " size: "     << n <<
                    " md5sum: "   << md5sumCur <<
                KFS_LOG_EOM;
            }
        }
        LeaseRelinquishOp lrelOp(nextSeq(), i->chunkId, leaseOp.leaseId);
        DoMetaOpWithRetry(&lrelOp);
        if (leaseOp.status < 0) {
            KFS_LOG_STREAM_ERROR << "failed to relinquish lease: " <<
                " chunk: " << i->chunkId <<
                ErrorCodeToStr(lrelOp.status) <<
            KFS_LOG_EOM;
        }
    }
    md5sum = mdsAll.GetMd();
    return (match ? 0 : 1);
}

int
KfsClientImpl::GetChunkFromReplica(const ServerLocation& loc,
    kfsChunkId_t chunkId, int64_t chunkVersion, ostream& os)
{
    TcpSocket sock;
    int res;
    if ((res = sock.Connect(loc)) < 0) {
        return res;
    }
    SizeOp sizeOp(nextSeq(), chunkId, chunkVersion);
    res = DoOpCommon(&sizeOp, &sock);
    if (res < 0) {
        return res;
    }
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
    while (nread < sizeOp.size) {
        op.seq           = nextSeq();
        op.numBytes      = min(size_t(1) << 20, (size_t)(sizeOp.size - nread));
        op.offset        = nread;
        op.contentLength = 0;
        const int res = DoOpCommon(&op, &sock);
        if (res < 0) {
            nread = res;
            break;
        }
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
    if (uid == kKfsUserNone) {
        static string empty;
        return empty;
    }
    UserNames::const_iterator it = mUserNames.find(uid);
    if (it == mUserNames.end() || it->second.second < now) {
        struct passwd  pwebuf = {0};
        struct passwd* pwe    = 0;
        const int err = getpwuid_r((uid_t)uid,
            &pwebuf, mNameBuf, mNameBufSize, &pwe);
        string name;
        if (err || ! pwe) {
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
        it = res.first;
    }
    return it->second.first;
}

const string&
KfsClientImpl::GidToName(kfsGid_t gid, time_t now)
{
    if (gid == kKfsGroupNone) {
        static string empty;
        return empty;
    }
    GroupNames::const_iterator it = mGroupNames.find(gid);
    if (it == mGroupNames.end() || it->second.second < now) {
        struct group  gbuf = {0};
        struct group* pge  = 0;
        const int err = getgrgid_r((gid_t)gid,
            &gbuf, mNameBuf, mNameBufSize, &pge);
        string name;
        if (err || ! pge) {
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
        it = res.first;
    }
    return it->second.first;
}


kfsUid_t
KfsClientImpl::NameToUid(const string& name, time_t now)
{
    UserIds::const_iterator it = mUserIds.find(name);
    if (it == mUserIds.end() || it->second.second < now) {
        struct passwd  pwebuf = {0};
        struct passwd* pwe    = 0;
        const int err = getpwnam_r(name.c_str(),
            &pwebuf, mNameBuf, mNameBufSize, &pwe);
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
        it = res.first;
    }
    return it->second.first;
}

kfsGid_t
KfsClientImpl::NameToGid(const string& name, time_t now)
{
    GroupIds::const_iterator it = mGroupIds.find(name);
    if (it == mGroupIds.end() || it->second.second < now) {
        struct group  gbuf = {0};
        struct group* pge  = 0;
        const int err = getgrnam_r(name.c_str(),
            &gbuf, mNameBuf, mNameBufSize, &pge);
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
        it = res.first;
    }
    return it->second.first;
}

int
KfsClientImpl::GetUserAndGroupNames(kfsUid_t user, kfsGid_t group,
    string& uname, string& gname)
{
    QCStMutexLocker l(mMutex);
    const time_t now = time(0);
    if (user != kKfsUserNone) {
        uname = UidToName(user, now);
    }
    if (group != kKfsGroupNone) {
        gname = GidToName(group, now);
    }
    return 0;
}

} // client
} // KFS
