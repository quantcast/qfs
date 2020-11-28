//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/22
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
//
//----------------------------------------------------------------------------

#include "ChunkServer.h"
#include "ChunkManager.h"
#include "AtomicRecordAppender.h"
#include "RemoteSyncSM.h"
#include "MetaServerSM.h"
#include "KfsOps.h"
#include "LeaseClerk.h"
#include "ClientManager.h"

#include "common/Properties.h"
#include "common/MdStream.h"
#include "common/MemLock.h"
#include "common/computemd5.h"

#include "kfsio/NetManager.h"
#include "kfsio/Globals.h"
#include "kfsio/SslFilter.h"
#include "kfsio/NetErrorSimulator.h"
#include "kfsio/ProcessRestarter.h"

#include "qcdio/QCUtils.h"

#include <signal.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <limits.h>

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <new>

namespace KFS {

using std::string;
using std::vector;
using std::cout;
using std::cerr;
using std::fstream;
using std::ofstream;
using std::ostream;
using std::istringstream;
using std::min;
using std::set_new_handler;
using KFS::libkfsio::globalNetManager;
using KFS::libkfsio::InitGlobals;

static ProcessRestarter&
GetRestarter()
{
    static ProcessRestarter sRestarter(
        false, // inCloseFdsAtInitFlag
        false, // inSaveRestoreEnvFlag
        false, // inExitOnRestartFlag
        true   // inCloseFdsBeforeExecFlag,
        -30    // inMaxGracefulRestartSeconds
    );
    return sRestarter;
}

string
RestartChunkServer()
{
    globalNetManager().Shutdown();
    return GetRestarter().Restart();
}

class StdErrAndOutRedirector
{
public:
    StdErrAndOutRedirector(const char* outName, const char* errName)
        : mCout(outName),
          mCerr(errName),
          mPrevCout(*outName ? cout.tie(&mCout) : 0),
          mPrevCerr(*errName ? cerr.tie(&mCerr) : 0)
    {
        if (outName && *outName) {
            if (! freopen(outName, "a", stdout)) {
                KFS_LOG_STREAM_ERROR <<
                    "freopen: " <<  outName <<
                    ": " << QCUtils::SysError(errno) <<
                KFS_LOG_EOM;
            }
        }
        if (errName && *errName) {
            if (! freopen(errName, "a", stderr)) {
                KFS_LOG_STREAM_ERROR <<
                    "freopen: " << errName <<
                    ": " << QCUtils::SysError(errno) <<
                KFS_LOG_EOM;
            }
        }
    }
    ~StdErrAndOutRedirector()
    {
        if (mPrevCerr) {
            cerr.tie(mPrevCerr);
        }
        if (mPrevCout) {
            cout.tie(mPrevCout);
        }
    }
private:
    fstream        mCout;
    fstream        mCerr;
    ostream* const mPrevCout;
    ostream* const mPrevCerr;
};

class ChunkServerMain
{
public:
    int Run(int argc, char **argv);

private:
    Properties     mProp;
    vector<string> mChunkDirs;
    string         mMD5Sum;
    ServerLocation mClientListener;
    bool           mClientListenerIpV6OnlyFlag;
    int            mClientThreadCount;
    int            mFirstCpuIndex;
    string         mChunkServerHostname;
    string         mClusterKey;
    string         mNodeId;
    int            mChunkServerRackId;
    int64_t        mMaxLockedMemorySize;

    ChunkServerMain()
        : mProp(),
          mChunkDirs(),
          mMD5Sum(),
          mClientListener(),
          mClientListenerIpV6OnlyFlag(false),
          mClientThreadCount(0),
          mFirstCpuIndex(-1),
          mChunkServerHostname(),
          mClusterKey(),
          mNodeId(),
          mChunkServerRackId(-1),
          mMaxLockedMemorySize(0)
        {}
    ~ChunkServerMain()
        {}
    bool LoadParams(const char *fileName);
    friend class ChunkServerGlobals;
private:
    ChunkServerMain(const ChunkServerMain&);
    ChunkServerMain& operator=(const ChunkServerMain&);
};

///
/// Read and validate the configuration settings for the chunk
/// server. The configuration file is assumed to contain lines of the
/// form: xxx.yyy.zzz = <value>
/// @result 0 on success; -1 on failure
/// @param[in] fileName File that contains configuration information
/// for the chunk server.
bool
ChunkServerMain::LoadParams(const char* fileName)
{
    static StdErrAndOutRedirector redirector(
        mProp.getValue("chunkServer.stdout", ""),
        mProp.getValue("chunkServer.stderr", "")
    );

    if (mProp.loadProperties(fileName, '=') != 0) {
        KFS_LOG_STREAM_FATAL <<
            "Invalid properties file: " << fileName <<
        KFS_LOG_EOM;
        return false;
    }

    MsgLogger::GetLogger()->SetLogLevel(
        mProp.getValue("chunkServer.loglevel",
        MsgLogger::GetLogLevelNamePtr(MsgLogger::GetLogger()->GetLogLevel())));
    MsgLogger::GetLogger()->SetUseNonBlockingIo(true);
    MsgLogger::GetLogger()->SetMaxLogWaitTime(0);
    MsgLogger::GetLogger()->SetParameters(mProp, "chunkServer.msgLogWriter.");
    GetRestarter().SetParameters("chunkServer.", mProp);

    string displayProps(fileName);
    displayProps += ":\n";
    mProp.getList(displayProps, string());
    KFS_LOG_STREAM_INFO << displayProps << KFS_LOG_EOM;

    mClientListener.port = mProp.getValue(
        "chunkServer.clientPort", mClientListener.port);
    if (mClientListener.port < 0) {
        KFS_LOG_STREAM_FATAL << "invalid client listener: " <<
            mClientListener <<
        KFS_LOG_EOM;
        return false;
    }
    mClientListener.hostname = mProp.getValue(
        "chunkServer.clientIp", mClientListener.hostname);
    mClientListenerIpV6OnlyFlag = mProp.getValue(
        "chunkServer.clientIpV6Only", mClientListenerIpV6OnlyFlag ? 1 : 0) != 0;
    KFS_LOG_STREAM_INFO << "chunk server client listener: " <<
        mClientListener <<
        (mClientListenerIpV6OnlyFlag ? " ipv6 only" : "") <<
    KFS_LOG_EOM;
    mClientThreadCount = mProp.getValue(
        "chunkServer.clientThreadCount", mClientThreadCount);
    mFirstCpuIndex = mProp.getValue(
        "chunkServer.clientThreadFirstCpuIndex", mFirstCpuIndex);
    KFS_LOG_STREAM_INFO << "chunk server client thread count: " <<
        mClientThreadCount <<  " first cpu: " << mFirstCpuIndex <<
    KFS_LOG_EOM;

    mChunkServerHostname = mProp.getValue("chunkServer.hostname",
        mChunkServerHostname);
    if (! mChunkServerHostname.empty()) {
        KFS_LOG_STREAM_INFO << "chunk server hostname: " <<
            mChunkServerHostname <<
        KFS_LOG_EOM;
    }

    // Paths are space separated directories for storing chunks
    istringstream is(mProp.getValue("chunkServer.chunkDir", "chunks"));
    string        dir;
    while ((is >> dir)) {
        KFS_LOG_STREAM_INFO << "chunk dir: " << dir << KFS_LOG_EOM;
        mChunkDirs.push_back(dir);
    }

    mNodeId = mProp.getValue("chunkServer.nodeId", mNodeId);
    const char* const kFilePrefix    = "FILE:";
    size_t      const kFilePrefixLen = strlen(kFilePrefix);
    if (0 == mNodeId.compare(0, kFilePrefixLen, kFilePrefix)) {
        const char* const fileName = mNodeId.c_str() + kFilePrefixLen;
        string      const md5      = ComputeMD5(fileName);
        if (md5.empty()) {
            KFS_LOG_STREAM_FATAL <<
                mNodeId << " " << fileName << ": read failure" <<
            KFS_LOG_EOM;
            return false;
        }
        mNodeId = md5;
    } else if ((size_t)MAX_RPC_HEADER_LEN / 8 < mNodeId.size()) {
        KFS_LOG_STREAM_FATAL <<
            mNodeId << ": exceeds size limit of " <<
                MAX_RPC_HEADER_LEN / 8 <<
        KFS_LOG_EOM;
        return false;
    }
    KFS_LOG_STREAM_INFO << "nodeId: " << mNodeId <<
    KFS_LOG_EOM;
    mChunkServerRackId = mProp.getValue("chunkServer.rackId", mChunkServerRackId);
    KFS_LOG_STREAM_INFO << "rack: " << mChunkServerRackId <<
    KFS_LOG_EOM;

    mClusterKey = mProp.getValue("chunkServer.clusterKey", mClusterKey);
    KFS_LOG_STREAM_INFO << "cluster key: " << mClusterKey << KFS_LOG_EOM;

    mMD5Sum = mProp.getValue("chunkServer.md5sum", mMD5Sum);
    NetErrorSimulatorConfigure(
        globalNetManager(),
        mProp.getValue("chunkServer.netErrorSimulator", "")
    );

    mMaxLockedMemorySize = (int64_t)mProp.getValue(
        "chunkServer.maxLockedMemory", (double)mMaxLockedMemorySize);
    string errMsg;
    int err = LockProcessMemory(
        mMaxLockedMemorySize,
        min(int64_t(16) << 20, mMaxLockedMemorySize / 3),
        min(int64_t(16) << 20, mMaxLockedMemorySize / 4),
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
    const char* const pidFileName = mProp.getValue(
        "chunkServer.pidFile",
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
    return true;
}

static void SigQuitHandler(int /* sig */)
{
    if (write(1, "SIGQUIT\n", 8) < 0) {
        QCUtils::SetLastIgnoredError(errno);
    }
    globalNetManager().Shutdown();
}

static void SigHupHandler(int /* sig */)
{
    gMetaServerSM.Reconnect();
}

int
ChunkServerMain::Run(int argc, char **argv)
{
    if (argc < 2) {
        cout << "Usage: " << argv[0] <<
            " <properties file> {<msg log file>}"
        "\n";
        return 0;
    }

    const int rstatus = GetRestarter().Init(argc, argv);
    if (0 != rstatus) {
        cout << "restarter init: " << QCUtils::SysError(rstatus) << "\n";
        return 1;
    }
    MsgLogger::Init(argc > 2 ? argv[2] : 0);
    srand((int)microseconds());
    MdStream::Init();

    SslFilter::Error err = SslFilter::Initialize();
    if (err != 0) {
        KFS_LOG_STREAM_ERROR << "ssl initialization failure: " <<
            SslFilter::GetErrorMsg(err) <<
        KFS_LOG_EOM;
        MsgLogger::Stop();
        MdStream::Cleanup();
        return 1;
    }

    // set the coredump size to unlimited
    struct rlimit rlim;
    rlim.rlim_cur = RLIM_INFINITY;
    rlim.rlim_max = RLIM_INFINITY;
    if (setrlimit(RLIMIT_CORE, &rlim)) {
        KFS_LOG_STREAM_INFO << "unable to increase coredump file size: " <<
            QCUtils::SysError(errno, "RLIMIT_CORE") <<
        KFS_LOG_EOM;
    }

    // compute the MD5 of the binary
#ifdef KFS_OS_NAME_LINUX
    mMD5Sum = ComputeMD5("/proc/self/exe");
#endif
    if (mMD5Sum.empty()) {
        mMD5Sum = ComputeMD5(argv[0]);
    }
    if (! LoadParams(argv[1])) {
        return 1;
    }

    KFS_LOG_STREAM_INFO << "starting chunkserver..." << KFS_LOG_EOM;
    KFS_LOG_STREAM_INFO <<
        "md5sum to send to metaserver: " << mMD5Sum <<
    KFS_LOG_EOM;

    signal(SIGPIPE, SIG_IGN);
    signal(SIGQUIT, &SigQuitHandler);
    signal(SIGHUP,  &SigHupHandler);
    int ret = 1;
    if (gMetaServerSM.SetMetaInfo(
                mClusterKey,
                mChunkServerRackId,
                mMD5Sum,
                mNodeId,
                mProp) == 0 &&
            gChunkServer.Init(
                mClientListener,
                mClientListenerIpV6OnlyFlag,
                mChunkServerHostname,
                mClientThreadCount,
                mFirstCpuIndex)) {
        ret = gChunkServer.MainLoop(mChunkDirs, mProp) ? 0 : 1;
    }
    NetErrorSimulatorConfigure(globalNetManager());
    err = SslFilter::Cleanup();
    if (err != 0) {
        KFS_LOG_STREAM_ERROR << "ssl cleanup failure: " <<
            SslFilter::GetErrorMsg(err) <<
        KFS_LOG_EOM;
    }
    MdStream::Cleanup();
    MsgLogger::Stop();

    return ret;
}

// Enforce construction and destruction order here.
class ChunkServerGlobals
{
public:
    ChunkServerGlobals()
        : mAtomicRecordAppendManager(),
          mLeaseClerk(),
          mClientManager(),
          mMetaServerSM(),
          mChunkServer(),
          mChunkManager(),
          mChunkServerMain()
        {}
    AtomicRecordAppendManager mAtomicRecordAppendManager;
    LeaseClerk                mLeaseClerk;
    ClientManager             mClientManager;
    MetaServerSM              mMetaServerSM;
    ChunkServer               mChunkServer;
    ChunkManager              mChunkManager;
    ChunkServerMain           mChunkServerMain;
};

static void
NewHandler()
{
    const int err = errno;
    die(QCUtils::SysError(err, "memory allocation failed"));
}

static ChunkServerGlobals&
InitChunkServerGlobals()
{
    set_new_handler(&NewHandler);
    GetRestarter();
    InitGlobals();
    globalNetManager();
    KfsOp::Init();
    static ChunkServerGlobals sChunkServerGlobals;
    return sChunkServerGlobals;
}
static ChunkServerGlobals& sChunkServerGlobals = InitChunkServerGlobals();

AtomicRecordAppendManager& gAtomicRecordAppendManager =
    sChunkServerGlobals.mAtomicRecordAppendManager;
LeaseClerk&    gLeaseClerk    = sChunkServerGlobals.mLeaseClerk;
ClientManager& gClientManager = sChunkServerGlobals.mClientManager;
MetaServerSM&  gMetaServerSM  = sChunkServerGlobals.mMetaServerSM;
ChunkServer&   gChunkServer   = sChunkServerGlobals.mChunkServer;
ChunkManager&  gChunkManager  = sChunkServerGlobals.mChunkManager;

} // namespace KFS

int
main(int argc, char **argv)
{
    return KFS::sChunkServerGlobals.mChunkServerMain.Run(argc, argv);
}
