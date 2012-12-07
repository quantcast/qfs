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

#include "common/Properties.h"
#include "common/MemLock.h"
#include "kfsio/NetManager.h"
#include "kfsio/Globals.h"
#include "kfsio/IOBuffer.h"

#include "NetDispatch.h"
#include "ChunkServer.h"
#include "LayoutManager.h"
#include "common/MsgLogger.h"
#include "qcdio/QCUtils.h"
#include "qcdio/QCIoBufferPool.h"
#include "common/MdStream.h"
#include "common/nofilelimit.h"
#include "Logger.h"
#include "Checkpoint.h"
#include "kfstree.h"
#include "Replay.h"
#include "Restorer.h"
#include "AuditLog.h"

#include <sys/resource.h>
#include <signal.h>
#include <unistd.h>
#include <stdio.h>

namespace KFS
{

using std::cerr;
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
        if (argc > 1) {
            MsgLogger::Init(argv[1]);
        } else {
            MsgLogger::Init(0);
        }
        const bool okFlag = sInstance.Startup(argv[0], createEmptyFsFlag);
        AuditLog::Stop();
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
            mFileName.c_str(), '=', false);
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
          mClientPort(-1),
          mChunkServerPort(-1),
          mLogDir(),
          mCPDir(),
          mMinChunkservers(1),
          mMinReplicasPerFile(1),
          mIsPathToFidCacheEnabled(false),
          mLogRotateIntervalSec(600),
          mMaxLockedMemorySize(0)
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
                    mFileName.c_str(), '=', true)) {
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
    bool Startup(bool createEmptyFsFlag);

    // This is to get settings from the core file.
    string     mFileName;
    Properties mProperties;
    Properties mStartupProperties;
    bool       mCheckpointFlag;
    bool       mSetParametersFlag;
    bool       mRestartChunkServersFlag;
    int        mSetParametersCount;
    // Port at which KFS clients connect and send RPCs
    int        mClientPort;
    // Port at which Chunk servers connect
    int        mChunkServerPort;
    // paths for logs and checkpoints
    string     mLogDir;
    string     mCPDir;
    // min # of chunk servers to exit recovery mode
    uint32_t   mMinChunkservers;
    int16_t    mMinReplicasPerFile;
    bool       mIsPathToFidCacheEnabled;
    int        mLogRotateIntervalSec;
    int64_t    mMaxLockedMemorySize;

    static MetaServer sInstance;
} MetaServer::sInstance;

void
MetaServer::SetParameters(const Properties& props)
{
    // min # of chunkservers that should connect to exit recovery mode
    mMinChunkservers = props.getValue("metaServer.minChunkservers", 1);
    KFS_LOG_STREAM_INFO << "min. # of chunkserver that should connect: " <<
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
    MsgLogger::GetLogger()->SetMaxLogWaitTime(0);
    MsgLogger::GetLogger()->SetParameters(props, "metaServer.msgLogWriter.");

    // bump up the # of open fds to as much as possible
    SetMaxNoFileLimit();

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
    mClientPort = props.getValue("metaServer.clientPort", mClientPort);
    if (mClientPort < 0) {
        KFS_LOG_STREAM_FATAL <<
            "invalid client port: " << mClientPort <<
        KFS_LOG_EOM;
        return false;
    }
    KFS_LOG_STREAM_INFO << "Using meta server client port: " <<
        mClientPort <<
    KFS_LOG_EOM;
    mChunkServerPort =
        props.getValue("metaServer.chunkServerPort", mChunkServerPort);
    if (mChunkServerPort < 0) {
        KFS_LOG_STREAM_FATAL << "invalid chunk server port: " <<
            mChunkServerPort <<
        KFS_LOG_EOM;
        return false;
    }
    KFS_LOG_STREAM_INFO << "Using meta server chunk server port: " <<
        mChunkServerPort <<
    KFS_LOG_EOM;
    mLogDir = props.getValue("metaServer.logDir", mLogDir);
    mCPDir = props.getValue("metaServer.cpDir", mCPDir);
    // By default, path->fid cache is disabled.
    mIsPathToFidCacheEnabled = (props.getValue("metaServer.enablePathToFidCache",
        mIsPathToFidCacheEnabled ? 1 : 0)) != 0;
    KFS_LOG_STREAM_INFO << "path->fid cache " <<
        (mIsPathToFidCacheEnabled ? "enabled" : "disabled") <<
    KFS_LOG_EOM;

    // Enable directory space update by default.
    metatree.setUpdatePathSpaceUsage(true);

    SetParameters(props);

    gLayoutManager.SetBufferPool(&GetIoBufAllocator().GetBufferPool());
    gLayoutManager.SetParameters(props, mClientPort);
    globalNetManager().RegisterTimeoutHandler(this);
    bool okFlag = gNetDispatch.Bind(mClientPort, mChunkServerPort);
    if (okFlag) {
        KFS_LOG_STREAM_INFO << "starting metaserver" << KFS_LOG_EOM;
        if ((okFlag = Startup(createEmptyFsFlag ||
                props.getValue("metaServer.createEmptyFs", 0) != 0))) {
            KFS_LOG_STREAM_INFO << "start servicing" << KFS_LOG_EOM;
            // The following only returns after receiving SIGQUIT.
            okFlag = gNetDispatch.Start();
        }
    } else {
        KFS_LOG_STREAM_FATAL <<
            "failed to bind to port " <<
            mClientPort << " or " << mChunkServerPort <<
        KFS_LOG_EOM;
    }
    gLayoutManager.Shutdown();
    return okFlag;
}

bool
MetaServer::Startup(bool createEmptyFsFlag)
{
    metatree.disableFidToPathname();
    const bool updateSpaceUsageFlag =
        metatree.getUpdatePathSpaceUsageFlag();
    metatree.setUpdatePathSpaceUsage(false);
    // get the paths setup before we get going
    logger_setup_paths(mLogDir);
    checkpointer_setup_paths(mCPDir);

    int status;
    errno = 0;
    if (! createEmptyFsFlag || file_exists(LASTCP)) {
        Restorer r;
        status = r.rebuild(LASTCP, mMinReplicasPerFile) ? 0 : -EIO;
    } else {
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
    checkpointer_init();
    gLayoutManager.InitRecoveryStartTime();
    return true;
}

}

int
main(int argc, char** argv)
{
    return KFS::MetaServer::Run(argc, argv);
}
