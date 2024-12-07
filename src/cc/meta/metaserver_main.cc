//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/22
// Author: Blake Lewis, Mike Ovsiannikov
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
// \brief Driver code that starts up the meta server
//
//----------------------------------------------------------------------------

#include "NetDispatch.h"
#include "ChunkServer.h"
#include "LayoutManager.h"
#include "LogWriter.h"
#include "Checkpoint.h"
#include "kfstree.h"
#include "Replay.h"
#include "Restorer.h"
#include "AuditLog.h"
#include "util.h"
#include "MetaDataStore.h"
#include "MetaDataSync.h"
#include "MetaVrSM.h"
#include "MetaVrOps.h"
#include "ChildProcessTracker.h"

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
#include "kfsio/ProcessRestarter.h"

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
using std::ifstream;
using libkfsio::globalNetManager;
using libkfsio::SetIOBufferAllocator;

class BufferAllocator : public libkfsio::IOBufferAllocator
{
public:
    BufferAllocator()
        : mBufferPool()
        {}
    ~BufferAllocator()
    {
        if (0 != mBufferPool.GetUsedBufferCount()) {
            panic("IO buffers still in use");
        }
    }
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

const char* const kLogWriterParamsPrefix = "metaServer.log.";
const char* const kNewLogDirPropName     = "metaServer.log.logDir";

class MetaServer : public ITimeout
{
public:
    static int Run(int argsc, char** argsv)
    {
        if (! ValidateMetaReplayIoHandler(cerr)) {
            return 1;
        }
        char** argv = argsv;
        int    argc = argsc;
        const char* myname = "metaserver";
        if (argc >= 1) {
            myname = argv[0];
            argv++;
            argc--;
        }
        bool        createEmptyFsFlag    = false;
        const char* resetVrConfigTypePtr = 0;
        if (argc >= 1) {
            if (0 == strcmp(argv[0], "-c") ||
                    0 == strcmp(argv[0], "-create-fs")) {
                createEmptyFsFlag = true;
            } else if (0 == strcmp(argv[0], "-clear-vr-config")) {
                resetVrConfigTypePtr = MetaVrReconfiguration::GetResetOpName();
            } else if (0 == strcmp(argv[0], "-vr-inactivate-all-nodes")) {
                resetVrConfigTypePtr =
                    MetaVrReconfiguration::GetInactivateAllNodesName();
            }
            if (createEmptyFsFlag || resetVrConfigTypePtr) {
                argv++;
                argc--;
            }
        }
        if (argc < 1) {
            cerr << "Usage: " << myname <<
                " [-c|-create-fs|-reset-vr-config]"
                " <properties file> [<msg log file>]\n"
                " -c|-create-fs            -- create new empty file system,"
                " and exit\n"
                " -clear-vr-config         -- append an entry to the end of the"
                    " transaction log to clear VR configuration, and exit\n"
                " -vr-inactivate-all-nodes -- append an entry to the end of the"
                    " transaction log to inactivate all VR nodes, and exit\n"
                "\n";
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
        // Use data segment (.bss) for meta server instance, in order to
        // increase the chances for it to be part of partially written core
        // file, as kernel/os typically begins writing core file from lower
        // addresses.
        static class MetaServerStorage
        {
        public:
            void* Get() { return mStorage; }
        private:
            size_t mStorage[
                (sizeof(MetaServer) + sizeof(size_t) - 1) / sizeof(size_t)
            ];
        } sStorage;
        MetaServer& server = *(new (sStorage.Get()) MetaServer());
        const int status = server.mProcessRestarter.Init(argsc, argsv);
        if (0 != status) {
            cerr << "failed to initialize process restarter: " <<
                " error: " << QCUtils::SysError(status) << "\n";
            return 1;
        }
        if (argc > 1) {
            MsgLogger::Init(argv[1]);
        } else {
            MsgLogger::Init(0);
        }
        AuditLog::Init();
        const bool okFlag = server.Startup(
            myname, argv[0], createEmptyFsFlag, resetVrConfigTypePtr);
        server.Cleanup();
        AuditLog::Stop();
        server.~MetaServer();
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
    static void Restart()
    {
        if (sInstance) {
            sInstance->RestartSelf();
        }
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
        mProperties.clear();
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
          mMaxLockedMemorySize(0),
          mMaxFdLimit(-1),
          mLogWriterRunningFlag(false),
          mTimerRegisteredFlag(false),
          mCleanupDoneFlag(false),
          mMetaDataSync(globalNetManager()),
          mMetaMd(),
          mProcessRestarter(
                false, // inCloseFdsAtInitFlag
                false, // inSaveRestoreEnvFlag
                false, // inExitOnRestartFlag
                true,  // inCloseFdsBeforeExecFlag
                -30    // inMaxGracefulRestartSeconds
          )
    {
        if (! sInstance) {
            sInstance = this;
        }
    }
    ~MetaServer()
    {
        MetaProcessRestart::SetRestartPtr(0);
        MetaServer::Cleanup();
        if (this == sInstance) {
            sInstance = 0;
        }
    }
    void Cleanup()
    {
        if (mCleanupDoneFlag) {
            return;
        }
        mCleanupDoneFlag = true;
        if (mTimerRegisteredFlag) {
            mTimerRegisteredFlag = false;
            globalNetManager().UnRegisterTimeoutHandler(this);
        }
        signal(SIGUSR1, SIG_DFL);
        signal(SIGHUP,  SIG_DFL);
        signal(SIGQUIT, SIG_DFL);
        signal(SIGALRM, SIG_DFL);
        signal(SIGCHLD, SIG_DFL);
    }
    bool Startup(
        const char* myname, const char* fileName, bool createEmptyFsFlag,
        const char* resetVrConfigTypePtr)
    {
        if (! fileName || ! *fileName) {
            KFS_LOG_STREAM_FATAL <<
                "no configuration file name" <<
            KFS_LOG_EOM;
            return false;
        }
        if (signal(SIGUSR1, &MetaServer::Usr1Signal) == SIG_ERR) {
            KFS_LOG_STREAM_FATAL <<
                QCUtils::SysError(errno, "signal(SIGUSR1):") <<
            KFS_LOG_EOM;
            return false;
        }
        if (signal(SIGHUP, &MetaServer::HupSignal) == SIG_ERR) {
            KFS_LOG_STREAM_FATAL <<
                QCUtils::SysError(errno, "signal(SIGHUP):") <<
            KFS_LOG_EOM;
            return false;
        }
        if (signal(SIGQUIT, &MetaServer::QuitSignal) == SIG_ERR) {
            KFS_LOG_STREAM_FATAL <<
                QCUtils::SysError(errno, "signal(SIGQUIT):") <<
            KFS_LOG_EOM;
            return false;
        }
        if (signal(SIGCHLD, &MetaServer::ChildSignal) == SIG_ERR) {
            KFS_LOG_STREAM_FATAL <<
                QCUtils::SysError(errno, "signal(SIGCHLD):") <<
            KFS_LOG_EOM;
            return false;
        }
        if (signal(SIGALRM, &MetaServer::AlarmSignal) == SIG_ERR) {
            KFS_LOG_STREAM_FATAL <<
                QCUtils::SysError(errno, "signal(SIGALRM):") <<
            KFS_LOG_EOM;
            return false;
        }
        // Ignore SIGPIPE's that generated when clients break TCP
        // connection.
        if (signal(SIGPIPE, SIG_IGN)  == SIG_ERR) {
            KFS_LOG_STREAM_FATAL <<
                QCUtils::SysError(errno, "signal(SIGPIPE):") <<
            KFS_LOG_EOM;
            return false;
        }
        mFileName = GetFullPath(fileName);
        if (mFileName.empty()) {
            return false;
        }
        int err;
        {
            MsgLogger::StStream log(
                *MsgLogger::GetLogger(), MsgLogger::kLogLevelINFO, 0);
            err = mStartupProperties.loadProperties(mFileName.c_str(), '=',
                MsgLogger::GetLogger()->IsLogLevelEnabled(
                    MsgLogger::kLogLevelINFO) ?
                        &(log.GetStream() << mFileName << "\n") : 0
            );
        }
        if (0 != err) {
            KFS_LOG_STREAM_FATAL <<
                "failed to read configuration file: " << mFileName <<
                " " << QCUtils::SysError(-err, 0) <<
            KFS_LOG_EOM;
            return false;
        }
#ifdef KFS_OS_NAME_LINUX
        mMetaMd = ComputeMd("/proc/self/exe");
#endif
        if (mMetaMd.empty()) {
            mMetaMd = ComputeMd(myname);
        }
        mLogDir = mStartupProperties.getValue("metaServer.logDir", mLogDir);
        mLogDir = mStartupProperties.getValue(kNewLogDirPropName, mLogDir);
        mStartupProperties.setValue(kNewLogDirPropName, mLogDir);
        MetaProcessRestart::SetRestartPtr(&MetaServer::Restart);
        return Startup(
            mStartupProperties, createEmptyFsFlag, resetVrConfigTypePtr);
    }
    bool Startup(const Properties& props, bool createEmptyFsFlag,
        const char* resetVrConfigTypePtr);
    void SetParameters(const Properties& props);
    static void Usr1Signal(int)
        { if (sInstance) { sInstance->mCheckpointFlag = true; } }
    static void HupSignal(int)
        { if (sInstance) { sInstance->mSetParametersFlag = true; } }
    static void QuitSignal(int)
        { if (sInstance) { globalNetManager().Shutdown(); } }
    static void ChildSignal(int)
        { /* nothing, process tracker does waitpd */ }
    static void AlarmSignal(int)
        { if (sInstance) { sInstance->mRestartChunkServersFlag = true; } }
    static string GetFullPath(const string& fileName)
    {
        if (! fileName.empty() && fileName[0] == '/') {
            return fileName;
        }
        char buf[PATH_MAX];
        const char* const cwd = getcwd(buf, sizeof(buf));
        if (! cwd) {
            const int err = errno;
            KFS_LOG_STREAM_ERROR <<
                "getcwd: " << QCUtils::SysError(err, 0) <<
            KFS_LOG_EOM;
            return string();
        }
        string ret = cwd;
        return (ret + "/" + fileName);
    }
    static string ComputeMd(const char* pathname)
    {
        const size_t kBufSize = size_t(1) << 20;
        char* const  buf      = new char[kBufSize];
        ifstream     is(pathname, ifstream::in | ifstream::binary);
        MdStream     mds(0, false, string(), 0);

        while (is && mds) {
            is.read(buf, kBufSize);
            mds.write(buf, is.gcount());
        }
        delete [] buf;
        string ret;
        if (! is.eof() || ! mds) {
            const int err = errno;
            KFS_LOG_STREAM_ERROR <<
                "md5sum " << QCUtils::SysError(err, pathname) <<
            KFS_LOG_EOM;
        } else {
            ret = mds.GetMd();
            KFS_LOG_STREAM_INFO <<
                "md5sum " << pathname << ": " << ret <<
            KFS_LOG_EOM;
        }
        is.close();
        return ret;
    }
    void RestartSelf()
    {
        KFS_LOG_STREAM_WARN <<
            "attempting meta server process restart" <<
        KFS_LOG_EOM;
        MsgLogger* const logger = MsgLogger::GetLogger();
        if (logger) {
            logger->Flush();
        }
        if (gNetDispatch.IsRunning()) {
            // "Park" all threads
            gNetDispatch.PrepareCurrentThreadToFork();
            MetaRequest::GetLogWriter().PrepareToFork();
            gLayoutManager.GetUserAndGroup().PrepareToFork();
            AuditLog::PrepareToFork();
        }
        if (logger) {
            logger->Flush();
            logger->PrepareToFork();
        }
        gChildProcessTracker.KillAll(SIGKILL);
        const string errMsg = mProcessRestarter.Restart();
        if (logger) {
            logger->ForkDone();
        }
        if (! errMsg.empty()) {
            panic("restart failure: " + errMsg);
            _exit(1);
        }
        // Attempt graceful exit.
        globalNetManager().Shutdown();
        AuditLog::ForkDone();
        gLayoutManager.GetUserAndGroup().ForkDone();
        MetaRequest::GetLogWriter().ForkDone();
        gNetDispatch.CurrentThreadForkDone();
    }
    bool Startup(bool createEmptyFsFlag, bool createEmptyFsIfNoCpExistsFlag,
        const char* resetVrConfigTypePtr);

    // This is to get settings from the core file.
    string           mFileName;
    Properties       mProperties;
    Properties       mStartupProperties;
    bool             mCheckpointFlag;
    bool             mSetParametersFlag;
    bool             mRestartChunkServersFlag;
    int              mSetParametersCount;
    // Port at which KFS clients connect and send RPCs
    ServerLocation   mClientListenerLocation;
    // Port at which Chunk servers connect
    ServerLocation   mChunkServerListenerLocation;
    bool             mClientListenerIpV6OnlyFlag;
    bool             mChunkServerListenerIpV6OnlyFlag;
    // paths for logs and checkpoints
    string           mLogDir;
    string           mCPDir;
    // min # of chunk servers to exit recovery mode
    uint32_t         mMinChunkservers;
    int              mMaxChunkServers;
    int              mMaxChunkServersSocketCount;
    int16_t          mMinReplicasPerFile;
    bool             mIsPathToFidCacheEnabled;
    bool             mStartupAbortOnPanicFlag;
    bool             mAbortOnPanicFlag;
    int64_t          mMaxLockedMemorySize;
    int              mMaxFdLimit;
    bool             mLogWriterRunningFlag;
    bool             mTimerRegisteredFlag;
    bool             mCleanupDoneFlag;
    MetaDataSync     mMetaDataSync;
    string           mMetaMd;
    ProcessRestarter mProcessRestarter;

    static MetaServer* sInstance;
}* MetaServer::sInstance = 0;

void
MetaServer::SetParameters(const Properties& props)
{
    mProcessRestarter.SetParameters("metaServer.", props);
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
    string chunkmapDumpDir = props.getValue("metaServer.chunkmapDumpDir", ".");
    setChunkmapDumpDir(chunkmapDumpDir);
    metatree.setUpdatePathSpaceUsage(props.getValue(
        "metaServer.updateDirSizes",
        metatree.getUpdatePathSpaceUsageFlag() ? 1 : 0) != 0);

    globalNetManager().SetResolverParameters(
        props.getValue(
            "metaServer.useOsResolver",
            globalNetManager().GetResolverOsFlag() ? 1 : 0) != 0,
        props.getValue(
            "metaServer.resolverMaxCacheSize",
            globalNetManager().GetResolverCacheSize()),
        props.getValue(
           "metaServer.resolverCacheExpiration",
            globalNetManager().GetResolverCacheExpiration())
    );
    if (mLogWriterRunningFlag) {
        MetaLogWriterControl* const op = new MetaLogWriterControl(
            MetaLogWriterControl::kSetParameters);
        op->paramsPrefix            = kLogWriterParamsPrefix;
        op->params                  = props;
        op->resolverOsFlag          = globalNetManager().GetResolverOsFlag();
        op->resolverCacheSize       = globalNetManager().GetResolverCacheSize();
        op->resolverCacheExpiration = globalNetManager().GetResolverCacheExpiration();
        submit_request(op);
    }
    const int status = mMetaDataSync.SetParameters(
        "metaServer.metaDataSync.", props, gLayoutManager.GetMaxResponseSize());
    if (0 != status) {
        KFS_LOG_STREAM_ERROR << "meta data sync set parameters: " <<
            QCUtils::SysError(-status) <<
        KFS_LOG_EOM;
    }
}

bool
MetaServer::Startup(const Properties& props,
    bool createEmptyFsFlag, const char* resetVrConfigTypePtr)
{
    MsgLogger::GetLogger()->SetLogLevel(
        props.getValue("metaServer.loglevel",
        MsgLogger::GetLogLevelNamePtr(MsgLogger::GetLogger()->GetLogLevel()))
    );
    MsgLogger::GetLogger()->SetUseNonBlockingIo(true);
    MsgLogger::GetLogger()->SetMaxLogWaitTime(0);
    MsgLogger::GetLogger()->SetParameters(props, "metaServer.msgLogWriter.");

    if (props.getValue("metaServer.wormMode")) {
        KFS_LOG_STREAM_FATAL <<
            "parameter metaServer.wormMode deprecated and is no longer"
            " supported."
            " Please use qfsadmin or qfstoggleworm to set WORM mode."
            " WORM mode can also be changed with logcomactor" <<
        KFS_LOG_EOM;
        return false;
    }
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
        props.getValue("metaServer.bufferPool.partitionBuffers",
            props.getValue("metaServer.bufferPool.partionBuffers",
                (sizeof(long) < 8 ? 32 : 256) << 10)),
        props.getValue("metaServer.bufferPool.bufferSize", 4 << 10),
        props.getValue("metaServer.bufferPool.lockMemory", 0) != 0 ||
            mMaxLockedMemorySize > 0
    );
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
    globalNetManager().SetMaxAcceptsPerRead(1024);
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
    KFS_LOG_STREAM_INFO << "meta server client listener: " <<
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
    mCPDir  = props.getValue("metaServer.cpDir", mCPDir);
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
    if (0 != MetaRequest::GetLogWriter().GetMetaVrSM().SetParameters(
            kMetaVrParametersPrefixPtr, props, mMetaMd.c_str())) {
        KFS_LOG_STREAM_FATAL <<
            "failed to set VR parameters" <<
        KFS_LOG_EOM;
        return false;
    }
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
        mTimerRegisteredFlag = true;
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
            if ((okFlag = Startup(
                    createEmptyFsFlag,
                    props.getValue("metaServer.createEmptyFs", 0) != 0,
                    resetVrConfigTypePtr))) {
                if (createEmptyFsFlag) {
                    KFS_LOG_STREAM_INFO <<
                        "created new file system, id: " << metatree.GetFsId() <<
                    KFS_LOG_EOM;
                } else if (! resetVrConfigTypePtr) {
                    KFS_LOG_STREAM_INFO << "start servicing" << KFS_LOG_EOM;
                    // The following only returns after receiving SIGQUIT.
                    okFlag = gNetDispatch.Start(mMetaDataSync);
                    gChildProcessTracker.KillAll(SIGKILL);
                }
            } else {
                mMetaDataSync.Shutdown();
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
    MetaRequest::GetLogWriter().Shutdown();
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

const char* const kCovertLogCpMsgPtr =
    "Possible incompatible transaction log and / or checkpoint format."
    " Log compactor can be used to convert prior"
    " versions checkpoint and log format"
    " logcompactor -T new-log-dir -C new-checkpoint-dir";

bool
MetaServer::Startup(bool createEmptyFsFlag,
    bool createEmptyFsIfNoCpExistsFlag, const char* resetVrConfigTypePtr)
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
    const bool verifyAllLogSegmentsPresentFlag = mStartupProperties.getValue(
        "metaServer.verifyAllLogSegmentsPresent", 0) != 0;
    replayer.verifyAllLogSegmentsPresent(verifyAllLogSegmentsPresentFlag);
    replayer.setLogDir(mLogDir.c_str());
    bool writeCheckpointFlag = false;
    if (! createEmptyFsFlag &&
            (! createEmptyFsIfNoCpExistsFlag || file_exists(LASTCP))) {
        if (0 != (status = mMetaDataSync.Start(
                mCPDir.c_str(), mLogDir.c_str(),
                0 <= MetaRequest::GetLogWriter().GetMetaVrSM().GetNodeId()))) {
            return false;
        }
        if (0 != (status = gNetDispatch.GetMetaDataStore().Load(
                mCPDir.c_str(),
                mLogDir.c_str(),
                mStartupProperties.getValue(
                    "metaServer.cleanupTempFiles", 1) != 0,
                ! verifyAllLogSegmentsPresentFlag,
                mMetaMd.c_str()))) {
            if (-ENXIO == status) {
                KFS_LOG_STREAM_FATAL <<
                    kCovertLogCpMsgPtr <<
                KFS_LOG_EOM;
            }
            return false;
        }
        Restorer r;
        r.setVrSequenceRequired(true); // Ensure format with VR sequence.
        status = r.rebuild(LASTCP, mMinReplicasPerFile) ? 0 : -EIO;
        rollChunkIdSeedFlag = true;
    } else {
        KFS_LOG_STREAM_INFO <<
            "creating new empty file system" <<
        KFS_LOG_EOM;
        if (! gNetDispatch.EnsureHasValidCryptoKey()) {
            KFS_LOG_STREAM_ERROR <<
                "crypto key generation has failed" <<
            KFS_LOG_EOM;
            return false;
        }
        const int64_t now = microseconds();
        metatree.SetFsInfo(fsid, now);
        status = metatree.new_tree(
            mStartupProperties.getValue(
                "metaServer.rootDirUser",
                kKfsUserRoot),
            mStartupProperties.getValue(
                "metaServer.rootDirGroup",
                kKfsGroupRoot),
            mStartupProperties.getValue(
                "metaServer.rootDirMode",
                0755),
            now
        );
        metatree.makeDumpsterDir();
        rollChunkIdSeedFlag = false;
        writeCheckpointFlag = true;
    }
    if (status != 0) {
        KFS_LOG_STREAM_FATAL << "checkpoint load failed: " <<
            QCUtils::SysError(-status) <<
        KFS_LOG_EOM;
        return false;
    }
    if (! writeCheckpointFlag && ! replayer.logSegmentHasLogSeq()) {
        KFS_LOG_STREAM_FATAL <<
            "invalid log segment name: " << replayer.getCurLog() <<
            " " << kCovertLogCpMsgPtr <<
        KFS_LOG_EOM;
        return false;
    }
    // get the sizes of all dirs up-to-date
    KFS_LOG_STREAM_INFO << "updating space utilization" << KFS_LOG_EOM;
    metatree.setUpdatePathSpaceUsage(true);
    metatree.enableFidToPathname();
    // Check whether the log segment pointed by checkpoint has sequence number.
    KFS_LOG_STREAM_INFO << "replaying logs" << KFS_LOG_EOM;
    status = replayer.playAllLogs();
    if (status != 0) {
        KFS_LOG_STREAM_FATAL << "log replay failed: " <<
            QCUtils::SysError(-status) <<
        KFS_LOG_EOM;
        return false;
    }
    if (metatree.GetFsId() <= 0) {
        KFS_LOG_STREAM_FATAL <<
            "invalid file system id: " << metatree.GetFsId() <<
            " " << kCovertLogCpMsgPtr <<
        KFS_LOG_EOM;
        return false;
    }
    if (! writeCheckpointFlag && ! replayer.logSegmentHasLogSeq()) {
        KFS_LOG_STREAM_FATAL <<
            "invalid log segment name: " << replayer.getCurLog() <<
            " " << kCovertLogCpMsgPtr <<
        KFS_LOG_EOM;
        return false;
    }
    if (0 != metatree.checkDumpsterExists()) {
        KFS_LOG_STREAM_FATAL <<
            "invalid file system structure: no dumpster directory" <<
        KFS_LOG_EOM;
        return false;
    }
    if (rollChunkIdSeedFlag && replayer.getLastLogSeq().mEpochSeq <= 0) {
        // Roll seeds only with prior log format with no chunk server inventory.
        const int64_t minRollChunkIdSeed = mStartupProperties.getValue(
            "metaServer.rollChunkIdSeed", int64_t(64) << 10);
        if (0 < minRollChunkIdSeed &&
                replayer.getRollSeeds() < minRollChunkIdSeed) {
            chunkID.setseed(chunkID.getseed() +
                minRollChunkIdSeed - max(int64_t(0), replayer.getRollSeeds()));
        }
    }
    metatree.setUpdatePathSpaceUsage(updateSpaceUsageFlag);
    if (mIsPathToFidCacheEnabled) {
        metatree.enablePathToFidCache();
    }
    string logFileName;
    if ((status = MetaRequest::GetLogWriter().Start(
            globalNetManager(),
            gNetDispatch.GetMetaDataStore(),
            mMetaDataSync,
            fileID,
            replayer,
            replayer.getLogNum() + ((writeCheckpointFlag ||
                replayer.getAppendToLastLogFlag()) ? 0 : 1),
            kLogWriterParamsPrefix,
            mStartupProperties,
            metatree.GetFsId(),
            mClientListenerLocation,
            mMetaMd,
            writeCheckpointFlag ? 0 : resetVrConfigTypePtr,
            &gNetDispatch.GetWatchdog(),
            gLayoutManager.GetBufferPool()->GetTotalBufferCount() / 16,
            logFileName)) != 0) {
        KFS_LOG_STREAM_FATAL <<
            "transaction log writer initialization failure: " <<
            QCUtils::SysError(-status) <<
        KFS_LOG_EOM;
        return false;
    }
    mLogWriterRunningFlag = true;
    const int err = gLayoutManager.GetUserAndGroup().Start(writeCheckpointFlag);
    if (err != 0) {
        KFS_LOG_STREAM_ERROR <<
            "failed to load user and grop info: " <<
                QCUtils::SysError(err) <<
        KFS_LOG_EOM;
        return false;
    }
    if (writeCheckpointFlag) {
        if ((status = cp.write(logFileName,
                replayer.getCommitted(), replayer.getErrChksum())) != 0) {
            KFS_LOG_STREAM_FATAL << "checkpoint initialization failure: " <<
                QCUtils::SysError(-status) <<
            KFS_LOG_EOM;
            return false;
        }
        if (gNetDispatch.GetMetaDataStore().Load(
                mCPDir.c_str(),
                mLogDir.c_str(),
                mStartupProperties.getValue(
                    "metaServer.cleanupTempFiles", 1) != 0,
                ! verifyAllLogSegmentsPresentFlag,
                mMetaMd.c_str())) {
            return false;
        }
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
