//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/22
// Author: Blake Lewis, Mike Ovsiannikov
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
// \brief Driver code that starts up the meta server
//
//----------------------------------------------------------------------------

#include "NetDispatch.h"
#include "ChunkServer.h"
#include "LayoutManager.h"
#include "Logger.h"
#include "Checkpoint.h"
#include "kfstree.h"
#include "Replay.h"
#include "Restorer.h"
#include "AuditLog.h"
#include "util.h"

#include "common/Properties.h"
#include "common/MemLock.h"
#include "common/MsgLogger.h"
#include "common/MdStream.h"
#include "common/nofilelimit.h"

#include "kfsio/NetManager.h"
#include "kfsio/Globals.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/SslFilter.h"
#include "kfsio/CryptoKeys.h"

#include "qcdio/QCUtils.h"
#include "qcdio/QCIoBufferPool.h"

#include <fstream>
#include <iostream>

#include <sys/resource.h>
#include <signal.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <limits.h>

namespace KFS
{

using std::cerr;
using std::ofstream;
using libkfsio::globalNetManager;
using libkfsio::SetIOBufferAllocator;

class BufferAllocator : public libkfsio::IOBufferAllocator
{
public:
    BufferAllocator()
        : mBufferPool()
        {}
    virtual size_t GetBufferSize() const
        { return mBufferPool.GetBufferSize(); }
    virtual char* Allocate()
    {
        char* const buf = mBufferPool.Get();
        if (! buf) {
            QCUtils::FatalError("out of io buffers", 0);
        }
        return buf;
    }
    virtual void Deallocate(
    char* inBufferPtr)
        { mBufferPool.Put(inBufferPtr); }
    QCIoBufferPool& GetBufferPool()
        { return mBufferPool; }
private:
    QCIoBufferPool mBufferPool;

private:
    BufferAllocator(
        const BufferAllocator& inAllocator);
    BufferAllocator& operator=(
        const BufferAllocator& inAllocator);
};

static const BufferAllocator* sAllocatorForGdbToFind = 0;

static BufferAllocator&
GetIoBufAllocator()
{
    static BufferAllocator sAllocator;
    if (! sAllocatorForGdbToFind) {
        sAllocatorForGdbToFind = &sAllocator;
    }
    return sAllocator;
}

class MetaServer : public ITimeout
{
public:
    static int Run(int argsc, char** argsv)
    {
        char** argv = argsv;
        int    argc = argsc;
        const char* myname = "metaserver";
        if (argc >= 1) {
            myname = argv[0];
            argv++;
            argc--;
        }
        bool createEmptyFsFlag = false;
        if (argc >= 1 && strcmp(argv[0], "-c") == 0) {
            argv++;
            argc--;
            createEmptyFsFlag = true;
        }
        if (argc < 1) {
            cerr << "Usage: " << myname <<
                " [-c] <properties file> [<msg log file>]\n";
            return 0;
        }
        libkfsio::InitGlobals();
        MdStream::Init();
        SslFilter::Error sslErr = SslFilter::Initialize();
        if (sslErr) {
            cerr << "failed to initialize ssl: " <<
                " error: " << sslErr <<
                " " << SslFilter::GetErrorMsg(sslErr) << "\n";
            return 1;
        }
        if (argc > 1) {
            MsgLogger::Init(argv[1]);
        } else {
            MsgLogger::Init(0);
        }
        const bool okFlag = sInstance.Startup(argv[0], createEmptyFsFlag);
        AuditLog::Stop();
        sslErr = SslFilter::Cleanup();
        if (sslErr) {
            KFS_LOG_STREAM_ERROR << "failed to cleanup ssl: " <<
                " error: " << sslErr <<
                " " << SslFilter::GetErrorMsg(sslErr) <<
            KFS_LOG_EOM;
        }
        MsgLogger::Stop();
        MdStream::Cleanup();
        return (okFlag ? 0 : 1);
    }
    virtual void Timeout()
    {
        if (mCheckpointFlag) {
            mCheckpointFlag = false;
            gLayoutManager.DoCheckpoint();
        }
        if (mRestartChunkServersFlag) {
            mRestartChunkServersFlag = false;
            gLayoutManager.ScheduleRestartChunkServers();
        }
        if (! mSetParametersFlag) {
            return;
        }
        mSetParametersFlag = false;
        const int res = mProperties.loadProperties(
            mFileName.c_str(), (char)'=');
        KFS_LOG_STREAM_INFO <<
            "loading configuration from: " << mFileName <<
            (res == 0 ? "" : " failed") <<
        KFS_LOG_EOM;
        if (res) {
            return;
        }
        SetParameters(mProperties);
        gLayoutManager.SetParameters(mProperties);
        mSetParametersCount++;
    }
private:
    MetaServer()
        : mFileName(),
          mProperties(0),
          mStartupProperties(0),
          mCheckpointFlag(false),
          mSetParametersFlag(false),
          mRestartChunkServersFlag(false),
          mSetParametersCount(0),
          mClientListenerLocation(),
          mChunkServerListenerLocation(),
          mClientListenerIpV6OnlyFlag(false),
          mChunkServerListenerIpV6OnlyFlag(false),
          mLogDir(),
          mCPDir(),
          mMinChunkservers(1),
          mMaxChunkServers(-1),
          mMaxChunkServersSocketCount(-1),
          mMinReplicasPerFile(1),
          mIsPathToFidCacheEnabled(false),
          mStartupAbortOnPanicFlag(false),
          mAbortOnPanicFlag(true),
          mLogRotateIntervalSec(600),
          mMaxLockedMemorySize(0),
          mMaxFdLimit(-1)
        {}
    ~MetaServer()
    {
        globalNetManager().UnRegisterTimeoutHandler(this);
        signal(SIGHUP, SIG_DFL);
    }
    bool Startup(const char* fileName, bool createEmptyFsFlag)
    {
        if (! fileName) {
            return false;
        }
        if (signal(SIGUSR1, &MetaServer::Usr1Signal) == SIG_ERR) {
            cerr << QCUtils::SysError(
                errno, "signal(SIGUSR1):") << "\n";
            return false;
        }
        if (signal(SIGHUP, &MetaServer::HupSignal) == SIG_ERR) {
            cerr << QCUtils::SysError(
                errno, "signal(SIGHUP):") << "\n";
            return false;
        }
        if (signal(SIGQUIT, &MetaServer::QuitSignal) == SIG_ERR) {
            cerr << QCUtils::SysError(
                errno, "signal(SIGQUIT):") << "\n";
            return false;
        }
        if (signal(SIGCHLD, &MetaServer::ChildSignal) == SIG_ERR) {
            cerr << QCUtils::SysError(
                errno, "signal(SIGCHLD):") << "\n";
            return false;
        }
        if (signal(SIGALRM, &MetaServer::AlarmSignal) == SIG_ERR) {
            cerr << QCUtils::SysError(
                errno, "signal(SIGALRM):") << "\n";
            return false;
        }
        // Ignore SIGPIPE's that generated when clients break TCP
        // connection.
        //
        if (signal(SIGPIPE, SIG_IGN)  == SIG_ERR) {
            cerr << QCUtils::SysError(
                errno, "signal(SIGPIPE):") << "\n";
            return false;
        }
        mFileName = GetFullPath(fileName);
        if (mFileName.empty() ||
                mStartupProperties.loadProperties(
                    mFileName.c_str(), '=', &cerr)) {
            cerr << "invalid properties file: " << mFileName <<  "\n";
            return false;
        }
        return Startup(mStartupProperties, createEmptyFsFlag);
    }
    bool Startup(const Properties& props, bool createEmptyFsFlag);
    void SetParameters(const Properties& props);
    static void Usr1Signal(int)
        { sInstance.mCheckpointFlag = true; }
    static void HupSignal(int)
        { sInstance.mSetParametersFlag = true; }
    static void QuitSignal(int)
        { globalNetManager().Shutdown(); }
    static void ChildSignal(int)
        { /* nothing, process tracker does waitpd */ }
    static void AlarmSignal(int)
        { sInstance.mRestartChunkServersFlag = true; }
    static string GetFullPath(string fileName)
    {
        if (! fileName.empty() && fileName[0] == '/') {
            return fileName;
        }
        char buf[PATH_MAX];
        const char* const cwd = getcwd(buf, sizeof(buf));
        if (! cwd) {
            const int err = errno;
            cerr << "getcwd: " << strerror(err) << "\n";
            return string();
        }
        string ret = cwd;
        return (ret + "/" + fileName);
    }
    bool Startup(bool createEmptyFsFlag, bool createEmptyFsIfNoCpExistsFlag);

    // This is to get settings from the core file.
    string         mFileName;
    Properties     mProperties;
    Properties     mStartupProperties;
    bool           mCheckpointFlag;
    bool           mSetParametersFlag;
    bool           mRestartChunkServersFlag;
    int            mSetParametersCount;
    // Port at which KFS clients connect and send RPCs
    ServerLocation mClientListenerLocation;
    // Port at which Chunk servers connect
    ServerLocation mChunkServerListenerLocation;
    bool           mClientListenerIpV6OnlyFlag;
    bool           mChunkServerListenerIpV6OnlyFlag;
    // paths for logs and checkpoints
    string         mLogDir;
    string         mCPDir;
    // min # of chunk servers to exit recovery mode
    uint32_t       mMinChunkservers;
    int            mMaxChunkServers;
    int            mMaxChunkServersSocketCount;
    int16_t        mMinReplicasPerFile;
    bool           mIsPathToFidCacheEnabled;
    bool           mStartupAbortOnPanicFlag;
    bool           mAbortOnPanicFlag;
    int            mLogRotateIntervalSec;
    int64_t        mMaxLockedMemorySize;
    int            mMaxFdLimit;

    static MetaServer sInstance;
} MetaServer::sInstance;

void
MetaServer::SetParameters(const Properties& props)
{
    // min # of chunkservers that should connect to exit recovery mode
    mMinChunkservers = props.getValue(
        "metaServer.minChunkservers", mMinChunkservers);
    mMaxChunkServers = props.getValue(
        "metaServer.maxChunkservers", mMaxChunkServers);
    if (0 <= mMaxChunkServersSocketCount) {
        KFS_LOG_STREAM_INFO <<
            "setting chunk servers limit:"
            " max: " << mMaxChunkServers <<
            " min: " << mMinChunkservers <<
            " chunk servers socket limit: " << mMaxChunkServersSocketCount <<
        KFS_LOG_EOM;
        mMaxChunkServers = min(mMaxChunkServersSocketCount, mMaxChunkServers);
        ChunkServer::SetMaxChunkServerCount(mMaxChunkServers);
        // Allow to set min greater than max to force "recoverY" mode.
    }
    KFS_LOG_STREAM_INFO << "min chunk servers that should connect: " <<
        mMinChunkservers <<
    KFS_LOG_EOM;
    gLayoutManager.SetMinChunkserversToExitRecovery(mMinChunkservers);

    // desired min. # of replicas per file
    mMinReplicasPerFile = props.getValue(
        "metaServer.minReplicasPerFile", mMinReplicasPerFile);
    KFS_LOG_STREAM_INFO << "min. # of replicas per file: " <<
        mMinReplicasPerFile <<
    KFS_LOG_EOM;

    const bool wormMode = props.getValue("metaServer.wormMode", 0) != 0;
    if (wormMode) {
        KFS_LOG_STREAM_INFO << "Enabling WORM mode" << KFS_LOG_EOM;
        setWORMMode(wormMode);
    }

    mLogRotateIntervalSec = max(3,
        props.getValue("metaServer.mLogRotateInterval",
            mLogRotateIntervalSec));

    logger_set_rotate_interval(mLogRotateIntervalSec);

    string chunkmapDumpDir = props.getValue("metaServer.chunkmapDumpDir", ".");
    setChunkmapDumpDir(chunkmapDumpDir);
    metatree.setUpdatePathSpaceUsage(props.getValue(
        "metaServer.updateDirSizes",
        metatree.getUpdatePathSpaceUsageFlag() ? 1 : 0) != 0);
}

///
/// Read and validate the configuration settings for the meta
/// server. The configuration file is assumed to contain lines of the
/// form: xxx.yyy.zzz = <value>
/// @result 0 on success; -1 on failure
/// @param[in] fileName File that contains configuration information
/// for the chunk server.
///
bool
MetaServer::Startup(const Properties& props, bool createEmptyFsFlag)
{
    MsgLogger::GetLogger()->SetLogLevel(
        props.getValue("metaServer.loglevel",
        MsgLogger::GetLogLevelNamePtr(MsgLogger::GetLogger()->GetLogLevel()))
    );
    MsgLogger::GetLogger()->SetUseNonBlockingIo(true);
    MsgLogger::GetLogger()->SetMaxLogWaitTime(0);
    MsgLogger::GetLogger()->SetParameters(props, "metaServer.msgLogWriter.");

    // bump up the # of open fds to as much as possible
    mMaxFdLimit = SetMaxNoFileLimit();
    if (mMaxFdLimit < 32) {
        KFS_LOG_STREAM_FATAL <<
            "insufficient file descripro limit: " << mMaxFdLimit <<
        KFS_LOG_EOM;
        return false;
    }
    mMaxLockedMemorySize = (int64_t)props.getValue(
        "metaServer.maxLockedMemory", (double)mMaxLockedMemorySize);
    string errMsg;
    int err = LockProcessMemory(
        mMaxLockedMemorySize,
        min(int64_t(128) << 20, mMaxLockedMemorySize / 3),
        min(int64_t(128) << 20, mMaxLockedMemorySize / 4),
        &errMsg
    );
    if (err != 0) {
        KFS_LOG_STREAM_FATAL <<
            errMsg <<
            (errMsg.empty() ? QCUtils::SysError(
                err, "lock process memory") : string()) <<
        KFS_LOG_EOM;
        return false;
    }
    err = GetIoBufAllocator().GetBufferPool().Create(
        props.getValue("metaServer.bufferPool.partitions", 1),
        props.getValue("metaServer.bufferPool.partionBuffers",
            (sizeof(long) < 8 ? 32 : 256) << 10),
        props.getValue("metaServer.bufferPool.bufferSize", 4 << 10),
        props.getValue("metaServer.bufferPool.lockMemory",0) != 0 ||
            mMaxLockedMemorySize > 0
    );
    globalNetManager().SetMaxAcceptsPerRead(1024);
    if (err != 0) {
        KFS_LOG_STREAM_FATAL <<
            QCUtils::SysError(err, "io buffer pool create: ") <<
        KFS_LOG_EOM;
        return false;
    }
    if (! SetIOBufferAllocator(&GetIoBufAllocator())) {
        KFS_LOG_STREAM_FATAL <<
            "failed to set io buffer allocatior" <<
        KFS_LOG_EOM;
        return false;
    }
    mClientListenerLocation.port = props.getValue("metaServer.clientPort",
        mClientListenerLocation.port);
    if (mClientListenerLocation.port < 0) {
        KFS_LOG_STREAM_FATAL <<
            "invalid client port: " << mClientListenerLocation.port <<
        KFS_LOG_EOM;
        return false;
    }
    mClientListenerLocation.hostname = props.getValue("metaServer.clientIp",
        mClientListenerLocation.hostname);
    mClientListenerIpV6OnlyFlag =
        props.getValue("metaServer.clientIpV6Only",
        mClientListenerIpV6OnlyFlag ? 1 : 0) != 0;
    KFS_LOG_STREAM_INFO << "meta server client listner: " <<
        mClientListenerLocation <<
        (mClientListenerIpV6OnlyFlag ? " ipv6 only" : "") <<
    KFS_LOG_EOM;
    mChunkServerListenerLocation.port =
        props.getValue("metaServer.chunkServerPort",
        mChunkServerListenerLocation.port);
    if (mChunkServerListenerLocation.port < 0) {
        KFS_LOG_STREAM_FATAL << "invalid chunk server port: " <<
            mChunkServerListenerLocation.port <<
        KFS_LOG_EOM;
        return false;
    }
    mChunkServerListenerLocation.hostname =
        props.getValue("metaServer.chunkServerIp",
        mChunkServerListenerLocation.hostname);
    mChunkServerListenerIpV6OnlyFlag =
        props.getValue("metaServer.chunkServerIpV6Only",
        mChunkServerListenerIpV6OnlyFlag ? 1 : 0) != 0;
    KFS_LOG_STREAM_INFO << "meta server chunk server listener: " <<
        mChunkServerListenerLocation <<
        (mChunkServerListenerIpV6OnlyFlag ? " ipv6 only" : "") <<
    KFS_LOG_EOM;
    mLogDir = props.getValue("metaServer.logDir", mLogDir);
    mCPDir = props.getValue("metaServer.cpDir", mCPDir);
    // By default, path->fid cache is disabled.
    mIsPathToFidCacheEnabled = props.getValue("metaServer.enablePathToFidCache",
        mIsPathToFidCacheEnabled ? 1 : 0) != 0;
    KFS_LOG_STREAM_INFO << "path->fid cache " <<
        (mIsPathToFidCacheEnabled ? "enabled" : "disabled") <<
    KFS_LOG_EOM;
    mStartupAbortOnPanicFlag = props.getValue("metaServer.startupAbortOnPanic",
        mStartupAbortOnPanicFlag ? 1 : 0) != 0;
    mAbortOnPanicFlag        = props.getValue("metaServer.abortOnPanicFlag",
        mAbortOnPanicFlag ? 1 : 0) != 0;

    // Enable directory space update by default.
    metatree.setUpdatePathSpaceUsage(true);

    SetParameters(props);

    const int maxSocketFd = mMaxFdLimit - min(256, (mMaxFdLimit + 2) / 3);
    if (mMaxChunkServers < 0) {
        mMaxChunkServers = min(max(4 << 10, 2 * (int)mMinChunkservers),
            maxSocketFd / (512 < maxSocketFd ? 8 : 4));
    }
    const int kMinClientSocketCount = 16;
    if (mMaxChunkServers < (int)mMinChunkservers ||
            maxSocketFd < mMaxChunkServers + kMinClientSocketCount) {
        KFS_LOG_STREAM_FATAL <<
            "insufficient file descriptors limit: " << mMaxFdLimit <<
            " for number of chunk servers:"
            " min: " << mMinChunkservers <<
            " max: " << mMaxChunkServers <<
            " min client socket count: " << kMinClientSocketCount <<
        KFS_LOG_EOM;
        return false;
    }
    const int maxClientSocketCount = maxSocketFd - mMaxChunkServers;
    mMaxChunkServersSocketCount = mMaxChunkServers;
    KFS_LOG_STREAM_INFO <<
        "hard limits:"
        " open files: "    << mMaxFdLimit <<
        " chunk servers: " << mMaxChunkServersSocketCount <<
        " clients: "       << maxClientSocketCount <<
    KFS_LOG_EOM;
    ChunkServer::SetMaxChunkServerCount(mMaxChunkServers);
    gNetDispatch.SetMaxClientSockets(maxClientSocketCount);

    gLayoutManager.SetBufferPool(&GetIoBufAllocator().GetBufferPool());
    bool okFlag = gLayoutManager.SetParameters(
        props, mClientListenerLocation.port);
    if (okFlag) {
        globalNetManager().RegisterTimeoutHandler(this);
        okFlag = gNetDispatch.Bind(
            mClientListenerLocation,
            mClientListenerIpV6OnlyFlag,
            mChunkServerListenerLocation,
            mChunkServerListenerIpV6OnlyFlag
        );
        if (okFlag) {
            KFS_LOG_STREAM_INFO << (createEmptyFsFlag ?
                "creating empty file system" :
                "starting metaserver") <<
            KFS_LOG_EOM;
            if ((okFlag = Startup(createEmptyFsFlag,
                    props.getValue("metaServer.createEmptyFs", 0) != 0))) {
                if (! createEmptyFsFlag) {
                    KFS_LOG_STREAM_INFO << "start servicing" << KFS_LOG_EOM;
                    // The following only returns after receiving SIGQUIT.
                    okFlag = gNetDispatch.Start();
                }
            }
        } else {
            KFS_LOG_STREAM_FATAL <<
                "failed to bind to " << mClientListenerLocation <<
                " or " << mChunkServerListenerLocation <<
            KFS_LOG_EOM;
        }
    } else {
        KFS_LOG_STREAM_FATAL <<
            "failed to set parameters " <<
        KFS_LOG_EOM;
    }
    gLayoutManager.Shutdown();
    return okFlag;
}

static bool
CheckDirWritable(
    const char*   inErrMsgPrefixPtr,
    const string& inDir)
{
    string testFile = inDir + "/.dirtest";
    int fd;
    if ((fd = open(testFile.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644)) < 0 ||
            write(fd, "0", 1) != 1 ||
            close(fd) != 0 ||
            remove(testFile.c_str()) != 0) {
        const int status = errno;
        if (0 <= fd) {
            close(fd);
            remove(testFile.c_str());
        }
        KFS_LOG_STREAM_FATAL << inErrMsgPrefixPtr <<
            inDir << ": " <<
            QCUtils::SysError(status) <<
        KFS_LOG_EOM;
        return false;
    }
    return true;
}

struct MetaSetFsInfo : public MetaRequest
{
public:
    MetaSetFsInfo(
        int64_t fsid,
        int64_t crTime)
        : MetaRequest(META_SET_FILE_SYSTEM_INFO, true),
          fileSystemId(fsid),
          createTime(crTime)
        {}
    virtual void handle()
    {
        if (status != 0) {
            return;
        }
        if (fileSystemId < 0 || 0 < metatree.GetFsId()) {
            status = -EINVAL;
            return;
        }
        metatree.SetFsInfo(fileSystemId, createTime);
    }
    virtual int log(ostream& os) const
    {
        if (status == 0) {
            os << "filesysteminfo"
                "/fsid/"   << fileSystemId <<
                "/crtime/" << ShowTime(createTime) <<
            "\n";
        }
        return (os.fail() ? -EIO : 0);
    }
    virtual ostream& ShowSelf(ostream& os) const
    {
        return (os <<
            "fsid: "  << fileSystemId <<
            " time: " << createTime
        );
    }
private:
    const int64_t fileSystemId;
    const int64_t createTime;
};

bool
MetaServer::Startup(bool createEmptyFsFlag, bool createEmptyFsIfNoCpExistsFlag)
{
    if (! CheckDirWritable("log directory: ", mLogDir) ||
            ! CheckDirWritable("checkpoint directory: ", mCPDir)) {
        return false;
    }

    seq_t fsid = 0;
    if (! CryptoKeys::PseudoRand(&fsid, sizeof(fsid))) {
        KFS_LOG_STREAM_FATAL <<
            "failed to initialize pseudo random number generator" <<
        KFS_LOG_EOM;
        return false;
    }
    if (fsid == 0) {
        fsid = 1;
    } else if (fsid < 0) {
        fsid = -fsid;
    }
    metatree.disableFidToPathname();
    const bool updateSpaceUsageFlag =
        metatree.getUpdatePathSpaceUsageFlag();
    metatree.setUpdatePathSpaceUsage(false);
    logger_setup_paths(mLogDir);
    checkpointer_setup_paths(mCPDir);

    const char* const pidFileName = mStartupProperties.getValue(
        "metaServer.pidFile",
        ""
    );
    if (pidFileName && *pidFileName) {
        ofstream pidf(pidFileName,
            ofstream::out | ofstream::trunc | ofstream::binary);
        if (pidf) {
            pidf << getpid() << "\n";
            pidf.close();
            if (pidf.fail()) {
                const int err = errno;
                KFS_LOG_STREAM_FATAL << "failed to write pid file " <<
                    pidFileName << ": " << QCUtils::SysError(err) <<
                KFS_LOG_EOM;
                return false;
            }
        } else {
            const int err = errno;
            KFS_LOG_STREAM_FATAL << "failed to create pid file " <<
                pidFileName << ": " << QCUtils::SysError(err) <<
            KFS_LOG_EOM;
            return false;
        }
    }
    setAbortOnPanic(mStartupAbortOnPanicFlag);
    int  status;
    bool rollChunkIdSeedFlag;
    if (createEmptyFsFlag && file_exists(LASTCP)) {
        KFS_LOG_STREAM_INFO <<
            "failed to crete empty files system:"
            " checkpoint already exists: " << LASTCP <<
        KFS_LOG_EOM;
        return false;
    }
    if (! createEmptyFsFlag &&
            (! createEmptyFsIfNoCpExistsFlag || file_exists(LASTCP))) {
        // Init fs id if needed, leave create time 0, restorer will set these
        // unless fsinfo entry doesn't exit.
        Restorer r;
        status = r.rebuild(LASTCP, mMinReplicasPerFile) ? 0 : -EIO;
        rollChunkIdSeedFlag = true;
    } else {
        KFS_LOG_STREAM_INFO <<
            "creating new empty file system" <<
        KFS_LOG_EOM;
        metatree.SetFsInfo(fsid, microseconds());
        status = metatree.new_tree(
            mStartupProperties.getValue(
                "metaServer.rootDirUser",
                kKfsUserRoot),
            mStartupProperties.getValue(
                "metaServer.rootDirGroup",
                kKfsGroupRoot),
            mStartupProperties.getValue(
                "metaServer.rootDirMode",
                0755)
        );
        rollChunkIdSeedFlag = false;
    }
    if (status != 0) {
        KFS_LOG_STREAM_FATAL << "checkpoint load failed: " <<
            QCUtils::SysError(-status) <<
        KFS_LOG_EOM;
        return false;
    }
    KFS_LOG_STREAM_INFO << "replaying logs" << KFS_LOG_EOM;
    status = replayer.playAllLogs();
    if (status != 0) {
        KFS_LOG_STREAM_FATAL << "log replay failed: " <<
            QCUtils::SysError(-status) <<
        KFS_LOG_EOM;
        return false;
    }
    if (rollChunkIdSeedFlag) {
        const int64_t minRollChunkIdSeed = mStartupProperties.getValue(
            "metaServer.rollChunkIdSeed", int64_t(32) << 10);
        if (0 < minRollChunkIdSeed &&
                replayer.getRollSeeds() < minRollChunkIdSeed) {
            chunkID.setseed(chunkID.getseed() +
                minRollChunkIdSeed - max(int64_t(0), replayer.getRollSeeds()));
        }
    }
    // get the sizes of all dirs up-to-date
    KFS_LOG_STREAM_INFO << "updating space utilization" << KFS_LOG_EOM;
    metatree.setUpdatePathSpaceUsage(true);
    metatree.setUpdatePathSpaceUsage(updateSpaceUsageFlag);
    metatree.enableFidToPathname();
    if (mIsPathToFidCacheEnabled) {
        metatree.enablePathToFidCache();
    }
    // empty the dumpster dir on startup; if it doesn't exist, create it
    // whatever is in the dumpster needs to be nuked anyway; if we
    // remove all the file entries from that dir, the space for the
    // chunks of the file will get reclaimed: chunkservers will tell us
    // about chunks we don't know and those will nuked due to staleness
    emptyDumpsterDir();
    logger_init(mLogRotateIntervalSec);
    if ((status = checkpointer_init()) != 0) {
        KFS_LOG_STREAM_FATAL << "checkpoint initialization failure: " <<
            QCUtils::SysError(-status) <<
        KFS_LOG_EOM;
        return false;
    }
    if (metatree.GetFsId() <= 0) {
        submit_request(new MetaSetFsInfo(fsid, 0));
    }
    setAbortOnPanic(mAbortOnPanicFlag);
    gLayoutManager.InitRecoveryStartTime();
    return true;
}

}

int
main(int argc, char** argv)
{
    return KFS::MetaServer::Run(argc, argv);
}
