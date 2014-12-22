//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/22
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

#include "ChunkServer.h"
#include "ChunkManager.h"
#include "Logger.h"
#include "AtomicRecordAppender.h"
#include "RemoteSyncSM.h"
#include "MetaServerSM.h"

#include "common/Properties.h"
#include "common/MdStream.h"
#include "common/MemLock.h"
#include "kfsio/NetManager.h"
#include "kfsio/Globals.h"
#include "kfsio/SslFilter.h"
#include "kfsio/NetErrorSimulator.h"
#include "qcdio/QCUtils.h"

#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
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

extern char **environ;

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
using KFS::libkfsio::globalNetManager;
using KFS::libkfsio::InitGlobals;

// Restart the chunk chunk server process by issuing exec when / if requested.
// Fork is more reliable, but might confuse existing scripts. Using debugger
// with fork is a little bit more involved.
// The intention here is to do graceful restart, this is not intended as an
// external "nanny" / monitoring / watchdog.
class Restarter
{
public:
    Restarter()
        : mCwd(0),
          mArgs(0),
          mEnv(0),
          mMaxGracefulRestartSeconds(60 * 6),
          mExitOnRestartFlag(false)
        {}
    ~Restarter()
        { Cleanup(); }
    bool Init(int argc, char **argv)
    {
        ::alarm(0);
        if (::signal(SIGALRM, &Restarter::SigAlrmHandler) == SIG_ERR) {
            QCUtils::FatalError("signal(SIGALRM)", errno);
        }
        Cleanup();
        if (argc < 1 || ! argv) {
            return false;
        }
        for (int len = PATH_MAX; len < PATH_MAX * 1000; len += PATH_MAX) {
            mCwd = (char*)::malloc(len);
            if (! mCwd || ::getcwd(mCwd, len)) {
                break;
            }
            const int err = errno;
            ::free(mCwd);
            mCwd = 0;
            if (err != ERANGE) {
                break;
            }
        }
        if (! mCwd) {
            return false;
        }
        mArgs = new char*[argc + 1];
        int i;
        for (i = 0; i < argc; i++) {
            if (! (mArgs[i] = ::strdup(argv[i]))) {
                Cleanup();
                return false;
            }
        }
        mArgs[i] = 0;
        char** ptr = environ;
        for (i = 0; *ptr; i++, ptr++)
            {}
        mEnv = new char*[i + 1];
        for (i = 0, ptr = environ; *ptr; ) {
            if (! (mEnv[i++] = ::strdup(*ptr++))) {
                Cleanup();
                return false;
            }
        }
        mEnv[i] = 0;
        return true;
    }
    void SetParameters(const Properties& props, string prefix)
    {
        mMaxGracefulRestartSeconds = props.getValue(
            prefix + "maxGracefulRestartSeconds",
            mMaxGracefulRestartSeconds
        );
        mExitOnRestartFlag = props.getValue(
            prefix + "exitOnRestartFlag",
            mExitOnRestartFlag
        );
    }
    string Restart()
    {
        if (! mCwd || ! mArgs || ! mEnv || ! mArgs[0] || ! mArgs[0][0]) {
            return string("not initialized");
        }
        if (! mExitOnRestartFlag) {
            struct stat res = {0};
            if (::stat(mCwd, &res) != 0) {
                return QCUtils::SysError(errno, mCwd);
            }
            if (! S_ISDIR(res.st_mode)) {
                return (mCwd + string(": not a directory"));
            }
            string execpath(mArgs[0][0] == '/' ? mArgs[0] : mCwd);
            if (mArgs[0][0] != '/') {
                if (! execpath.empty() &&
                        execpath.at(execpath.length() - 1) != '/') {
                    execpath += "/";
                }
                execpath += mArgs[0];
            }
            if (::stat(execpath.c_str(), &res) != 0) {
                return QCUtils::SysError(errno, execpath.c_str());
            }
            if (! S_ISREG(res.st_mode)) {
                return (execpath + string(": not a file"));
            }
        }
        if (::signal(SIGALRM, &Restarter::SigAlrmHandler) == SIG_ERR) {
            QCUtils::FatalError("signal(SIGALRM)", errno);
        }
        if (mMaxGracefulRestartSeconds > 0) {
            if (sInstance) {
                return string("restart in progress");
            }
            sInstance = this;
            if (::atexit(&Restarter::RestartSelf)) {
                sInstance = 0;
                return QCUtils::SysError(errno, "atexit");
            }
            ::alarm((unsigned int)mMaxGracefulRestartSeconds);
            globalNetManager().Shutdown();
        } else {
            ::alarm((unsigned int)-mMaxGracefulRestartSeconds);
            Exec();
        }
        return string();
    }
private:
    char*  mCwd;
    char** mArgs;
    char** mEnv;
    int    mMaxGracefulRestartSeconds;
    bool   mExitOnRestartFlag;

    static Restarter* sInstance;

    static void FreeArgs(char** args)
    {
        if (! args) {
            return;
        }
        char** ptr = args;
        while (*ptr) {
            ::free(*ptr++);
        }
        delete [] args;
    }
    void Cleanup()
    {
        free(mCwd);
        mCwd = 0;
        FreeArgs(mArgs);
        mArgs = 0;
        FreeArgs(mEnv);
        mEnv = 0;
    }
    void Exec()
    {
        if (mExitOnRestartFlag) {
            _exit(0);
        }
#ifdef KFS_OS_NAME_LINUX
        ::clearenv();
#else
        environ = 0;
#endif
        if (mEnv) {
            for (char** ptr = mEnv; *ptr; ptr++) {
                if (::putenv(*ptr)) {
                    QCUtils::FatalError("putenv", errno);
                }
            }
        }
        if (::chdir(mCwd) != 0) {
            QCUtils::FatalError(mCwd, errno);
        }
        execvp(mArgs[0], mArgs);
        QCUtils::FatalError(mArgs[0], errno);
    }
    static void RestartSelf()
    {
        if (! sInstance) {
            ::abort();
        }
        sInstance->Exec();
    }
    static void SigAlrmHandler(int /* sig */)
    {
        if (write(2, "SIGALRM\n", 8) < 0) {
            QCUtils::SetLastIgnoredError(errno);
        }
        ::abort();
    }
};
Restarter* Restarter::sInstance = 0;
static Restarter sRestarter;

string RestartChunkServer()
{
    return sRestarter.Restart();
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
    ChunkServerMain()
        : mProp(),
          mLogDir(),
          mChunkDirs(),
          mMD5Sum(),
          mMetaServerLoc(),
          mClientListener(),
          mClientListenerIpV6OnlyFlag(false),
          mClientThreadCount(0),
          mFirstCpuIndex(-1),
          mChunkServerHostname(),
          mClusterKey(),
          mChunkServerRackId(-1),
          mMaxLockedMemorySize(0)
        {}
    int Run(int argc, char **argv);

private:
    Properties     mProp;
    string         mLogDir;
    vector<string> mChunkDirs;
    string         mMD5Sum;
    ServerLocation mMetaServerLoc;
    ServerLocation mClientListener;
    bool           mClientListenerIpV6OnlyFlag;
    int            mClientThreadCount;
    int            mFirstCpuIndex;
    string         mChunkServerHostname;
    string         mClusterKey;
    int            mChunkServerRackId;
    int64_t        mMaxLockedMemorySize;

    void ComputeMD5(const char *pathname);
    bool LoadParams(const char *fileName);
};

void
ChunkServerMain::ComputeMD5(const char* pathname)
{
    const size_t kBufSize = size_t(1) << 20;
    char* const  buf      = new char[kBufSize];
    fstream      is(pathname, fstream::in | fstream::binary);
    MdStream     mds(0, false, string(), 0);

    while (is && mds) {
        is.read(buf, kBufSize);
        mds.write(buf, is.gcount());
    }
    delete [] buf;

    if (! is.eof() || ! mds) {
        KFS_LOG_STREAM_ERROR <<
            "md5sum " << QCUtils::SysError(errno, pathname) <<
        KFS_LOG_EOM;
    } else {
        mMD5Sum = mds.GetMd();
        KFS_LOG_STREAM_INFO <<
            "md5sum " << pathname << ": " << mMD5Sum <<
        KFS_LOG_EOM;
    }
    is.close();
}

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
    sRestarter.SetParameters(mProp, "chunkServer.");

    string displayProps(fileName);
    displayProps += ":\n";
    mProp.getList(displayProps, string());
    KFS_LOG_STREAM_INFO << displayProps << KFS_LOG_EOM;

    mMetaServerLoc.hostname = mProp.getValue("chunkServer.metaServer.hostname", "");
    mMetaServerLoc.port = mProp.getValue("chunkServer.metaServer.port", -1);
    if (! mMetaServerLoc.IsValid()) {
        KFS_LOG_STREAM_FATAL << "invalid meta-server host or port: " <<
            mMetaServerLoc.hostname << ':' << mMetaServerLoc.port <<
        KFS_LOG_EOM;
        return false;
    }

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

    mLogDir = mProp.getValue("chunkServer.logDir", "logs");
    KFS_LOG_STREAM_INFO << "log dir: " << mLogDir << KFS_LOG_EOM;

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

    sRestarter.Init(argc, argv);
    MsgLogger::Init(argc > 2 ? argv[2] : 0);
    srand((int)microseconds());
    InitGlobals();
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
        KFS_LOG_STREAM_INFO << "Unable to increase coredump file size: " <<
            QCUtils::SysError(errno, "RLIMIT_CORE") <<
        KFS_LOG_EOM;
    }

    // compute the MD5 of the binary
#ifdef KFS_OS_NAME_LINUX
    ComputeMD5("/proc/self/exe");
#endif
    if (mMD5Sum.empty()) {
        ComputeMD5(argv[0]);
    }
    if (! LoadParams(argv[1])) {
        return 1;
    }

    KFS_LOG_STREAM_INFO << "Starting chunkserver..." << KFS_LOG_EOM;
    KFS_LOG_STREAM_INFO <<
        "md5sum to send to metaserver: " << mMD5Sum <<
    KFS_LOG_EOM;

    signal(SIGPIPE, SIG_IGN);
    signal(SIGQUIT, &SigQuitHandler);
    signal(SIGHUP,  &SigHupHandler);
    int ret = 1;
    if (gMetaServerSM.SetMetaInfo(
                mMetaServerLoc,
                mClusterKey,
                mChunkServerRackId,
                mMD5Sum,
                mProp) == 0 &&
            gChunkServer.Init(
                mClientListener,
                mClientListenerIpV6OnlyFlag,
                mChunkServerHostname,
                mClientThreadCount,
                mFirstCpuIndex)) {
        ret = gChunkServer.MainLoop(mChunkDirs, mProp, mLogDir) ? 0 : 1;
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
static ChunkServerMain sChunkServerMain;

} // namespace KFS

int
main(int argc, char **argv)
{
    return KFS::sChunkServerMain.Run(argc, argv);
}
