//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/01
// Author: Sriram Rao
//         Mike Ovsiannikov. Re-implement. Implement "client threads".
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
// \file NetDispatch.cc
//
// \brief Meta-server request processing threads implementation.
//
//----------------------------------------------------------------------------

#include "NetDispatch.h"
#include "LayoutManager.h"
#include "ClientSM.h"
#include "Logger.h"

#include "kfsio/Acceptor.h"
#include "kfsio/KfsCallbackObj.h"
#include "kfsio/Globals.h"
#include "kfsio/IOBuffer.h"
#include "common/Properties.h"
#include "common/MsgLogger.h"
#include "common/time.h"
#include "common/rusage.h"
#include "qcdio/QCThread.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"

#include <inttypes.h>
#include <algorithm>
#include <vector>

namespace KFS
{
using std::max;
using std::vector;

using KFS::libkfsio::globalNetManager;
using KFS::libkfsio::globals;

NetDispatch gNetDispatch;

NetDispatch::NetDispatch()
    : mClientManager(),
      mChunkServerFactory(),
      mMutex(0),
      mClientManagerMutex(0),
      mRunningFlag(false),
      mClientThreadCount(0),
      mClientThreadsStartCpuAffinity(-1)
{
}

NetDispatch::~NetDispatch()
{
    delete mMutex;
}

bool
NetDispatch::Bind(int clientAcceptPort, int chunkServerAcceptPort)
{
    return (mClientManager.Bind(clientAcceptPort) &&
        mChunkServerFactory.Bind(chunkServerAcceptPort));
}

//
// Open up the server for connections.
//
bool
NetDispatch::Start()
{
    mMutex = mClientThreadCount > 0 ? new QCMutex() : 0;
    mClientManagerMutex = mClientThreadCount > 0 ?
        &mClientManager.GetMutex() : 0;
    mRunningFlag = true;
    // Start the acceptors so that it sets up a connection with the net
    // manager for listening.
    int err = 0;
    if (mClientThreadsStartCpuAffinity >= 0 &&
            (err = QCThread::SetCurrentThreadAffinity(
                QCThread::CpuAffinity(mClientThreadsStartCpuAffinity)))) {
        KFS_LOG_STREAM_ERROR <<
            "failed to set main thread affinity: " <<
                mClientThreadsStartCpuAffinity <<
            " error: " << QCUtils::SysError(err) <<
        KFS_LOG_EOM;
    } else if (mClientManager.StartAcceptor(
                mClientThreadCount,
                mClientThreadsStartCpuAffinity >= 0 ?
                    mClientThreadsStartCpuAffinity + 1 :
                    mClientThreadsStartCpuAffinity
            ) &&
            mChunkServerFactory.StartAcceptor()) {
        // Start event processing.
        globalNetManager().MainLoop(GetMutex());
    } else {
        err = -EINVAL;
    }
    mClientManager.Shutdown();
    mRunningFlag = false;
    mClientManagerMutex = 0;
    delete mMutex;
    mMutex = 0;
    return (err == 0);
}

void
NetDispatch::ChildAtFork()
{
    mClientManager.ChildAtFork();
}

void
NetDispatch::PrepareCurrentThreadToFork()
{
    mClientManager.PrepareCurrentThreadToFork();
}

// Counters for the various ops
struct MetaOpCounters : private map<MetaOp, Counter*>
{
    static void Update(MetaOp opName, int64_t time)
    {
        Counter* const c = GetCounter(opName);
        if (! c) {
            return;
        }
        c->Update(1);
        c->UpdateTime(time);
    }
    static void UpdateNumDirs(int count)
    {
        if (sInstance) {
            UpdateCtr(sInstance->mNumDirs, count);
        }
    }
    static void UpdateNumFiles(int count)
    {
        if (sInstance) {
            UpdateCtr(sInstance->mNumFiles, count);
        }
    }
    static void UpdateNumChunks(int count)
    {
        if (sInstance) {
            UpdateCtr(sInstance->mNumChunks, count);
        }
    }
    static void UpdatePathToFidCacheHit(int count)
    {
        if (sInstance) {
            UpdateCtr(sInstance->mPathToFidCacheHit, count);
        }
    }
    static void UpdatePathToFidCacheMiss(int count)
    {
        if (sInstance) {
            UpdateCtr(sInstance->mPathToFidCacheMiss, count);
        }
    }
    static int64_t GetNumFiles()
    {
        return (sInstance ?
            sInstance->mNumFiles.GetValue() : int64_t(0));
    }
    static int64_t GetNumDirs()
    {
        return (sInstance ?
            sInstance->mNumDirs.GetValue() : int64_t(0));
    }

private:
    Counter mNumFiles;
    Counter mNumDirs;
    Counter mNumChunks;
    Counter mPathToFidCacheHit;
    Counter mPathToFidCacheMiss;
    static MetaOpCounters* sInstance;

    MetaOpCounters()
        : map<MetaOp, Counter*>(),
          mNumFiles("Number of Files"),
          mNumDirs("Number of Directories"),
          mNumChunks("Number of Chunks"),
          mPathToFidCacheHit("Number of Hits in Path->Fid Cache"),
          mPathToFidCacheMiss("Number of Misses in Path->Fid Cache")
    {}
    ~MetaOpCounters()
    {
        for (iterator i = begin(); i != end(); ++i) {
            if (sInstance == this) {
                globals().counterManager.RemoveCounter(i->second);
            }
            delete i->second;
        }
        if (sInstance == this) {
            globals().counterManager.RemoveCounter(&mNumFiles);
            globals().counterManager.RemoveCounter(&mNumDirs);
            globals().counterManager.RemoveCounter(&mNumChunks);
            globals().counterManager.RemoveCounter(&mPathToFidCacheHit);
            globals().counterManager.RemoveCounter(&mPathToFidCacheMiss);
            sInstance = 0;
        }
    }
    void AddCounter(const char *name, MetaOp opName)
    {
        Counter* const c = new Counter(name);
        if (! insert(make_pair(opName, c)).second) {
            delete c;
            return;
        }
        globals().counterManager.AddCounter(c);
    }
    static Counter* GetCounter(MetaOp opName)
    {
        if (! sInstance) {
            return 0;
        }
        MetaOpCounters::iterator iter = sInstance->find(opName);
        if (iter == sInstance->end()) {
            return 0;
        }
        return iter->second;
    }
    static void UpdateCtr(Counter& ctr, int count)
    {
        if ((int64_t) ctr.GetValue() + count < 0) {
            ctr.Reset();
        } else {
            ctr.Update(count);
        }
    }
    static MetaOpCounters* MakeInstance()
    {
        // ensure that globals constructed first
        globals();
        static MetaOpCounters instance;
        instance.Init();
        return &instance;
    }
    void Init()
    {
        AddCounter("Get alloc", META_GETALLOC);
        AddCounter("Get layout", META_GETLAYOUT);
        AddCounter("Lookup", META_LOOKUP);
        AddCounter("Lookup Path", META_LOOKUP_PATH);
        AddCounter("Allocate", META_ALLOCATE);
        AddCounter("Truncate", META_TRUNCATE);
        AddCounter("Create", META_CREATE);
        AddCounter("Remove", META_REMOVE);
        AddCounter("Rename", META_RENAME);
        AddCounter("Set Mtime", META_SETMTIME);
        AddCounter("Mkdir", META_MKDIR);
        AddCounter("Rmdir", META_RMDIR);
        AddCounter("Change File Replication", META_CHANGE_FILE_REPLICATION);
        AddCounter("Lease Acquire", META_LEASE_ACQUIRE);
        AddCounter("Lease Renew", META_LEASE_RENEW);
        AddCounter("Lease Cleanup", META_LEASE_CLEANUP);
        AddCounter("Corrupt Chunk ", META_CHUNK_CORRUPT);
        AddCounter("Chunkserver Hello ", META_HELLO);
        AddCounter("Chunkserver Bye ", META_BYE);
        AddCounter("Chunkserver Retire Start", META_RETIRE_CHUNKSERVER);
        AddCounter("Replication Checker ", META_CHUNK_REPLICATION_CHECK);
        AddCounter("Replication Done ", META_CHUNK_REPLICATE);

        globals().counterManager.AddCounter(&mNumFiles);
        globals().counterManager.AddCounter(&mNumDirs);
        globals().counterManager.AddCounter(&mNumChunks);
        globals().counterManager.AddCounter(&mPathToFidCacheHit);
        globals().counterManager.AddCounter(&mPathToFidCacheMiss);
    }
}* MetaOpCounters::sInstance(MetaOpCounters::MakeInstance());

static class RequestStatsGatherer
{
public:
    RequestStatsGatherer()
        : mNextTime(0),
          mStatsIntervalMicroSec(30000000),
          mOpTimeWarningThresholdMicroSec(200000),
          mUserCpuMicroSec(0),
          mSystemCpuMicroSec(0),
          mWOStream()
        {}
    void OpDone(
        const MetaRequest& op)
    {
        const int64_t timeNowUsec     = microseconds();
        const int64_t reqTimeUsec     = timeNowUsec - op.submitTime;
        const int64_t reqProcTimeUsec = timeNowUsec - op.processTime;
        MetaOpCounters::Update(op.op, reqProcTimeUsec);
        if (reqProcTimeUsec > mOpTimeWarningThresholdMicroSec) {
            KFS_LOG_STREAM_INFO <<
                "Time spent processing: " << op.Show() <<
                " is: "            << (reqProcTimeUsec * 1e-6) <<
                " total: "         << (reqTimeUsec * 1e-6) <<
                " was submitted: " << op.submitCount <<
            KFS_LOG_EOM;
        }
        const int idx =
            ((op.op < 0 || op.op >= META_NUM_OPS_COUNT) ?
                (int)kOtherReqId :
            ((op.op == META_ALLOCATE &&
                ! static_cast<const MetaAllocate&>(op).logFlag) ?
                (int)kReqTypeAllocNoLog : (int)op.op + 1));
        const int64_t reqTime     = reqTimeUsec > 0 ? reqTimeUsec : 0;
        const int64_t reqProcTime =
            reqProcTimeUsec > 0 ? reqProcTimeUsec : 0;
        mRequest[  0].mCnt++;
        mRequest[  0].mTime     += reqTime;
        mRequest[  0].mProcTime += reqProcTime;
        mRequest[idx].mCnt++;
        mRequest[idx].mTime     += reqTime;
        mRequest[idx].mProcTime += reqProcTime;
        if (op.status < 0) {
            mRequest[  0].mErr++;
            mRequest[idx].mErr++;
        }
        if (timeNowUsec < mNextTime) {
            return;
        }
        if (cputime(&mUserCpuMicroSec, &mSystemCpuMicroSec) < 0) {
            mUserCpuMicroSec   = -1;
            mSystemCpuMicroSec = -1;
        }
        mNextTime = timeNowUsec + mStatsIntervalMicroSec;
        const char* kDelim = " ";
        KFS_LOG_STREAM_START(MsgLogger::kLogLevelINFO, logStream);
            ostream& os = logStream.GetStream();
            os << "===request=counters:" <<
                kDelim << timeNowUsec <<
                kDelim << mUserCpuMicroSec <<
                kDelim << mSystemCpuMicroSec
            ;
            for (int i = 0; i <= kReqTypeAllocNoLog; i++) {
                os <<
                    kDelim << mRequest[i].mCnt <<
                    kDelim << mRequest[i].mErr <<
                    kDelim << mRequest[i].mTime <<
                    kDelim << mRequest[i].mProcTime
                ;
            }
        KFS_LOG_STREAM_END;
        const bool kRusageSelfFlag = true;
        KFS_LOG_STREAM_START(MsgLogger::kLogLevelINFO, logStream);
            ostream& os = logStream.GetStream();
            os << "===rusage=self: ";
            showrusage(os, ": ", kDelim, kRusageSelfFlag);
        KFS_LOG_STREAM_END;
        KFS_LOG_STREAM_START(MsgLogger::kLogLevelINFO, logStream);
            ostream& os = logStream.GetStream();
            os << "===rusage=chidren: ";
            showrusage(os, ": ", kDelim, ! kRusageSelfFlag);
        KFS_LOG_STREAM_END;
    }
    void SetParameters(
        const Properties& props)
    {
        mNextTime -= mStatsIntervalMicroSec;
        mStatsIntervalMicroSec = props.getValue(
            "metaServer.statsGatherer.statsIntervalMicroSec",
            mStatsIntervalMicroSec
        );
        mOpTimeWarningThresholdMicroSec = props.getValue(
            "metaServer.statsGatherer.opTimeWarningThresholdMicroSec",
            mOpTimeWarningThresholdMicroSec
        );
        mNextTime += mStatsIntervalMicroSec;
    }
    void GetStatsCsv(
        ostream& os)
    {
        if (cputime(&mUserCpuMicroSec, &mSystemCpuMicroSec) < 0) {
            mUserCpuMicroSec   = -1;
            mSystemCpuMicroSec = -1;
            mRequest[kCpuUser].mErr++;
            mRequest[kCpuSys ].mErr++;
        } else {
            mRequest[kCpuUser].mTime     = mUserCpuMicroSec;
            mRequest[kCpuSys ].mTime     = mSystemCpuMicroSec;
            mRequest[kCpuUser].mProcTime = mUserCpuMicroSec;
            mRequest[kCpuSys ].mProcTime = mSystemCpuMicroSec;
        }
        os << "Name,Total,%-total,Errors,%-errors,Time-total,Time-CPU\n";
        const double ptotal  =
            100. / (double)max(int64_t(1), mRequest[0].mCnt);
        const double perrors =
            100. / (double)max(int64_t(1), mRequest[0].mErr);
        const char* kDelim   = ",";
        for (int i = 0; i < kReqTypesCnt; i++) {
            os <<
                GetRowName(i) <<
                kDelim << mRequest[i].mCnt <<
                kDelim << (mRequest[i].mCnt * ptotal)  <<
                kDelim << mRequest[i].mErr <<
                kDelim << (mRequest[i].mErr * perrors) <<
                kDelim << mRequest[i].mTime <<
                kDelim << mRequest[i].mProcTime <<
                "\n"
            ;
        }
    }
    void GetStatsCsv(
        IOBuffer& buf)
    {
        GetStatsCsv(mWOStream.Set(buf));
        mWOStream.Reset();
    }
    int64_t GetUserCpuMicroSec() const
        { return mUserCpuMicroSec; }
    int64_t GetSystemCpuMicroSec() const
        { return mSystemCpuMicroSec; }
private:
    enum
    {
        kOtherReqId        = META_NUM_OPS_COUNT + 1,
        kReqTypeAllocNoLog = kOtherReqId + 1,
        kCpuUser           = kReqTypeAllocNoLog + 1,
        kCpuSys            = kCpuUser + 1,
        kReqTypesCnt       = kCpuSys + 1
    };
    struct Counter {
        Counter()
            : mCnt(0),
              mErr(0),
              mTime(0),
              mProcTime(0)
            {}
        int64_t mCnt;
        int64_t mErr;
        int64_t mTime;
        int64_t mProcTime;
    };
    int64_t            mNextTime;
    int64_t            mStatsIntervalMicroSec;
    int64_t            mOpTimeWarningThresholdMicroSec;
    int64_t            mUserCpuMicroSec;
    int64_t            mSystemCpuMicroSec;
    Counter            mRequest[kReqTypesCnt];
    IOBuffer::WOStream mWOStream;

    static const char* GetRowName(
        int idx)
    {
        static const char* const kNames[kReqTypesCnt] =
        {
            "TOTAL",
#           define KfsMakeMetaOpName(name) #name,
            KfsForEachMetaOpId(KfsMakeMetaOpName)
#           undef KfsMakeMetaOpName
            "OTHER",
            "ALLOCATE_NO_LOG",
            "CPU_USER",
            "CPU_SYS"
        };
        return ((idx < 0 || idx >= kReqTypesCnt) ? "" : kNames[idx]);
    }
} sReqStatsGatherer;


void NetDispatch::SetParameters(const Properties& props)
{
    if (! mRunningFlag) {
        mClientThreadCount = props.getValue(
            "metaServer.clientThreadCount",
            mClientThreadCount);
        mClientThreadsStartCpuAffinity = props.getValue(
            "metaServer.clientThreadStartCpuAffinity",
            mClientThreadsStartCpuAffinity);
    }

    // Only main thread listens, and accepts.
    TcpSocket::SetDefaultRecvBufSize(props.getValue(
        "metaServer.tcpSocket.recvBufSize",
        TcpSocket::GetDefaultRecvBufSize()));
    TcpSocket::SetDefaultSendBufSize(props.getValue(
        "metaServer.tcpSocket.sendBufSize",
        TcpSocket::GetDefaultSendBufSize()));

    globalNetManager().SetMaxAcceptsPerRead(props.getValue(
        "metaServer.net.maxAcceptsPerRead",
        globalNetManager().GetMaxAcceptsPerRead()));

    sReqStatsGatherer.SetParameters(props);
}

void NetDispatch::GetStatsCsv(ostream& os)
{
    sReqStatsGatherer.GetStatsCsv(os);
}

void NetDispatch::GetStatsCsv(IOBuffer& buf)
{
    sReqStatsGatherer.GetStatsCsv(buf);
}

int64_t NetDispatch::GetUserCpuMicroSec() const
{
    return sReqStatsGatherer.GetUserCpuMicroSec();
}

int64_t NetDispatch::GetSystemCpuMicroSec() const
{
    return sReqStatsGatherer.GetSystemCpuMicroSec();
}

void
UpdateNumDirs(int count)
{
    MetaOpCounters::UpdateNumDirs(count);
}

int64_t
GetNumFiles()
{
    return MetaOpCounters::GetNumFiles();
}

int64_t
GetNumDirs()
{
    return MetaOpCounters::GetNumDirs();
}

void
UpdateNumFiles(int count)
{
    MetaOpCounters::UpdateNumFiles(count);
}

void
UpdateNumChunks(int count)
{
    MetaOpCounters::UpdateNumChunks(count);
}

void
UpdatePathToFidCacheMiss(int count)
{
    MetaOpCounters::UpdatePathToFidCacheMiss(count);
}

void
UpdatePathToFidCacheHit(int count)
{
    MetaOpCounters::UpdatePathToFidCacheHit(count);
}

///
/// Poll the logger to see if any op's have finished execution.  For
/// such ops, send a response back to the client.  Also, if there any
/// layout related RPCs, dispatch them now.
///
void
NetDispatch::Dispatch(MetaRequest *r)
{
    sReqStatsGatherer.OpDone(*r);
    // Reset count for requests like replication check, where the same
    // request reused.
    r->submitCount = 0;
    // The Client will send out a response and destroy r.
    if (r->clnt) {
        r->clnt->HandleEvent(EVENT_CMD_DONE, r);
    } else {
        delete r;
    }
}

class ClientManager::Impl : public IAcceptorOwner, public ITimeout
{
public:
    Impl()
        : IAcceptorOwner(),
          ITimeout(),
          mAcceptor(0),
          mClientThreads(0),
          mClientThreadCount(-1),
          mNextThreadIdx(0),
          mMutex(),
          mPrepareToForkDoneCond(),
          mForkDoneCond(),
          mPrepareToForkFlag(false),
          mPrepareToForkCnt(0)
        {};
    virtual ~Impl();
    bool Bind(int port);
    bool StartAcceptor(int threadCount, int startCpuAffinity);
    virtual KfsCallbackObj* CreateKfsCallbackObj(NetConnectionPtr &conn);
    void Shutdown();
    void ChildAtFork();
    QCMutex& GetMutex()
        { return mMutex; }
    void PrepareCurrentThreadToFork();
    inline void PrepareToFork(bool mainThreadFlag = false)
    {
        if (! mPrepareToForkFlag) {
            return;
        }
        QCMutex* const mutex = gNetDispatch.GetMutex();
        if (! mutex) {
            return;
        }
        assert(! mainThreadFlag || mutex->IsOwned());
        QCStMutexLocker locker(mainThreadFlag ? 0 : mutex);
        // The prepare thread count includes "main" thread.
        if (++mPrepareToForkCnt >= mClientThreadCount) {
            mPrepareToForkDoneCond.Notify();
        }
        mForkDoneCond.Wait(*mutex);
    }
    virtual void Timeout()
    {
        const bool kMainThreadFlag = true;
        PrepareToFork(kMainThreadFlag);
    }
private:
    class ClientThread;
    // The socket object which is setup to accept connections.
    Acceptor*                    mAcceptor;
    ClientManager::ClientThread* mClientThreads;
    int                          mClientThreadCount;
    int                          mNextThreadIdx;
    QCMutex                      mMutex;
    QCCondVar                    mPrepareToForkDoneCond;
    QCCondVar                    mForkDoneCond;
    volatile bool                mPrepareToForkFlag;
    volatile int                 mPrepareToForkCnt;
};

inline void
ClientManager::PrepareToFork()
{
    mImpl.PrepareToFork();
}

inline void
NetDispatch::PrepareToFork()
{
    mClientManager.PrepareToFork();
}

// All acceptors run in the main thread running global net manager event loop.
// New client "connections" are passed to the client threads via the queue.
// Each client thread runs each client "connection" (ClientSM instance) in its
// own net manager event loop.
// The core of the request processing submit_request() / MetaRequest::handle()
// is serialized with the mutex. The attempt is made to process requests in
// batches in order to reduce lock acquisition frequency.
// The client thread run loop is in Timeout() method below, which is invoked
// from NetManager::MainLoop().
// The pending requests queue depth governed by the ClientSM parameters.
// ClientSM logic limits number of outstanding requests as well as pending io
// bytes to ensure request processing "fairness" in respect to all the client
// connections.
class ClientManager::ClientThread :
    public QCRunnable,
    public ITimeout
{
public:
    ClientThread()
        : QCRunnable(),
          ITimeout(),
          mMutex(0),
          mThread(),
          mNetManager(),
          mWOStream(),
          mReqHead(0),
          mReqTail(0),
          mCliHead(0),
          mCliTail(0),
          mReqPendingHead(0),
          mReqPendingTail(0),
          mFlushQueue(8 << 10)
    {
        mNetManager.RegisterTimeoutHandler(this);
    }
    virtual ~ClientThread()
    {
        if (mThread.IsStarted()) {
            mNetManager.Shutdown();
            mNetManager.Wakeup();
            mThread.Join();
        }
        ClientThread::Timeout();
        assert(! mCliHead && ! mCliTail);
        mNetManager.UnRegisterTimeoutHandler(this);
    }
    bool Start(QCMutex* mutex, int cpuIndex)
    {
        if (mThread.IsStarted()) {
            return true;
        }
        mMutex = mutex;
        const int kStackSize = 256 << 10;
        const int err = mThread.TryToStart(
            this, kStackSize, "ClientThread",
            cpuIndex >= 0 ?
                QCThread::CpuAffinity(cpuIndex) :
                QCThread::CpuAffinity::None()
        );
        if (err) {
            KFS_LOG_STREAM_ERROR << QCUtils::SysError(
                err, "failed to start thread") <<
            KFS_LOG_EOM;
        }
        return (err == 0);
    }
    virtual void Run()
    {
        mNetManager.MainLoop();
    }
    virtual void Timeout()
    {
        gNetDispatch.PrepareToFork();
        MetaRequest* nextReq;
        if (mReqPendingHead) {
            // Dispatch requests.
            nextReq = mReqPendingHead;
            mReqPendingHead = 0;
            mReqPendingTail = 0;
            QCStMutexLocker locker(gNetDispatch.GetMutex());
            while (nextReq) {
                MetaRequest& op = *nextReq;
                nextReq = op.next;
                op.next = 0;
                submit_request(&op);
            }
        }
        ClientSM* nextCli;
        {
            QCStMutexLocker locker(mMutex);
            nextReq  = mReqHead;
            mReqHead = 0;
            mReqTail = 0;
            nextCli  = mCliHead;
            mCliHead = 0;
            mCliTail = 0;
        }
        // Send responses. Try to minimize number of system calls by
        // attempting to send multiple responses in single write.
        FlushQueue::iterator it = mFlushQueue.begin();
        NetConnectionPtr conn;
        while (nextReq) {
            MetaRequest& op = *nextReq;
            nextReq = op.next;
            op.next = &op;
            const NetConnectionPtr& cn = GetConnection(op);
            if (cn && ! cn->IsWriteReady()) {
                conn = cn; // Has no data pending.
            }
            op.clnt->HandleEvent(EVENT_CMD_DONE, &op);
            if (! conn) {
                continue;
            }
            if (! conn->CanStartFlush()) {
                conn.reset();
                continue;
            }
            if (it == mFlushQueue.end()) {
                mFlushQueue.push_back(NetConnectionPtr());
                it = mFlushQueue.end();
                conn.swap(mFlushQueue.back());
                continue;
            }
            conn.swap(*it++);
        }
        for (FlushQueue::iterator cit = mFlushQueue.begin();
                cit != it;
                ++cit) {
            (*cit)->StartFlush();
            cit->reset();
        }
        // Add new connections to the net manager.
        const bool runningFlag = mNetManager.IsRunning();
        while (nextCli) {
            ClientSM& cli = *nextCli;
            nextCli = cli.GetNext();
            cli.GetNext() = 0;
            const NetConnectionPtr& conn = cli.GetConnection();
            assert(conn);
            conn->SetOwningKfsCallbackObj(&cli);
            if (runningFlag) {
                mNetManager.AddConnection(conn);
            } else {
                conn->HandleErrorEvent();
            }
        }
        // Wake main thread if need to process requests waiting for
        // io buffers, if any.
        CheckIfIoBuffersAvailable();
    }
    void Enqueue(MetaRequest& op)
    {
        if (! op.clnt) {
            delete &op;
            return;
        }
        QCStMutexLocker locker(mMutex);
        op.next = 0;
        if (mReqTail) {
            mReqTail->next = &op;
            mReqTail = &op;
            return;
        }
        mReqHead = &op;
        mReqTail = &op;
        locker.Unlock();
        mNetManager.Wakeup();
    }
    void Add(NetConnectionPtr& conn)
    {
        if (! conn || ! conn->IsGood() || ! mThread.IsStarted()) {
            return;
        }
        QCStMutexLocker locker(mMutex);
        ClientSM* const cli = new ClientSM(
            conn, this, &mWOStream, mParseBuffer);
        assert(cli->GetConnection() == conn);
        conn.reset(); // Take the ownership. ClientSM ref. self.
        if (mCliTail) {
            mCliTail->GetNext() = cli;
            mCliTail = cli;
            return;
        }
        mCliHead = cli;
        mCliTail = cli;
        locker.Unlock();
        mNetManager.Wakeup();
    }
    void Add(MetaRequest& op)
    {
        // This method must be called from the client thread: ClientSM
        // adds request to the pending processing queue.
        if (mReqPendingTail) {
            mReqPendingTail->next = &op;
            mReqPendingTail = &op;
            return;
        }
        mReqPendingHead = &op;
        mReqPendingTail = &op;
        mNetManager.Wakeup();
    }
    bool IsStarted() const
        { return mThread.IsStarted(); }
    void ChildAtFork()
    {
        mNetManager.ChildAtFork();
    }
    void Wakeup()
        { mNetManager.Wakeup(); }
private:
    typedef vector<NetConnectionPtr> FlushQueue;

    QCMutex*           mMutex;
    QCThread           mThread;
    NetManager         mNetManager;
    IOBuffer::WOStream mWOStream;
    MetaRequest*       mReqHead;
    MetaRequest*       mReqTail;
    ClientSM*          mCliHead;
    ClientSM*          mCliTail;
    MetaRequest*       mReqPendingHead;
    MetaRequest*       mReqPendingTail;
    FlushQueue         mFlushQueue;
    char               mParseBuffer[MAX_RPC_HEADER_LEN];

    const NetConnectionPtr& GetConnection(MetaRequest& op)
    {
        return static_cast<ClientSM*>(op.clnt)->GetConnection();
    }
private:
    ClientThread(const ClientThread&);
    ClientThread& operator=(const ClientThread&);
};

ClientManager::Impl::~Impl()
{
    Impl::Shutdown();
}

bool
ClientManager::Impl::Bind(int port)
{
    delete mAcceptor;
    mAcceptor = 0;
    const bool kBindOnlyFlag = true;
    mAcceptor = new Acceptor(port, this, kBindOnlyFlag);
    return mAcceptor->IsAcceptorStarted();
}

bool
ClientManager::Impl::StartAcceptor(int threadCount, int startCpuAffinity)
{
    if (! mAcceptor) {
        return false;
    }
    mAcceptor->StartListening();
    if (! mAcceptor->IsAcceptorStarted()) {
        return false;
    }
    if (mClientThreadCount >= 0 || mClientThreads) {
        return true;
    }
    mClientThreadCount = max(threadCount, 0);
    if (mClientThreadCount <= 0) {
        return true;
    }
    int cpuIndex = startCpuAffinity;
    mClientThreads = new ClientManager::ClientThread[mClientThreadCount];
    for (int i = 0; i < mClientThreadCount; i++) {
        if (! mClientThreads[i].Start(&mMutex, cpuIndex)) {
            delete [] mClientThreads;
            mClientThreads     = 0;
            mClientThreadCount = -1;
            return false;
        }
        if (cpuIndex >= 0) {
            cpuIndex++;
        }
    }
    if (mClientThreadCount > 0) {
        globalNetManager().RegisterTimeoutHandler(this);
    }
    return true;
};

KfsCallbackObj*
ClientManager::Impl::CreateKfsCallbackObj(NetConnectionPtr &conn)
{
    if (mClientThreadCount < 0) {
        return 0;
    } else if (mClientThreadCount == 0) {
        return new ClientSM(conn);
    }
    int idx = mNextThreadIdx;
    if (idx >= mClientThreadCount || idx < 0) {
        idx = 0;
        mNextThreadIdx = idx + 1;
    } else {
        mNextThreadIdx++;
    }
    mClientThreads[idx].Add(conn);
    return 0;
}

void
ClientManager::Impl::Shutdown()
{
    if (mClientThreadCount > 0) {
        globalNetManager().UnRegisterTimeoutHandler(this);
    }
    delete mAcceptor;
    mAcceptor = 0;
    delete [] mClientThreads;
    mClientThreads = 0;
    mClientThreadCount = -1;
}

void
ClientManager::Impl::ChildAtFork()
{
    for (int i = 0; i < mClientThreadCount; i++) {
        mClientThreads[i].ChildAtFork();
    }
}

void
ClientManager::Impl::PrepareCurrentThreadToFork()
{
    QCMutex* const mutex = gNetDispatch.GetMutex();
    if (! mutex) {
        return;
    }
    assert(! mPrepareToForkFlag && mutex->IsOwned());
    mPrepareToForkFlag = true;
    mPrepareToForkCnt  = 0;
    for (int i = 0; i < mClientThreadCount; i++) {
        mClientThreads[i].Wakeup();
    }
    globalNetManager().Wakeup();
    while (mPrepareToForkCnt < mClientThreadCount) {
        mPrepareToForkDoneCond.Wait(*mutex);
    }
    mPrepareToForkFlag = false;
    mPrepareToForkCnt  = 0;
    // Resume threads after fork completes and the logck gets released.
    mForkDoneCond.NotifyAll();
}

ClientManager::ClientManager()
    : mImpl(*(new Impl()))
{
}

/* virtual */
ClientManager::~ClientManager()
{
    delete &mImpl;
};

bool
ClientManager::Bind(int port)
{
    return mImpl.Bind(port);
}


bool
ClientManager::StartAcceptor(int threadCount, int startCpuAffinity)
{
    return mImpl.StartAcceptor(threadCount, startCpuAffinity);
}

void
ClientManager::Shutdown()
{
    mImpl.Shutdown();
}

void
ClientManager::ChildAtFork()
{
    mImpl.ChildAtFork();
}

void
ClientManager::PrepareCurrentThreadToFork()
{
    mImpl.PrepareCurrentThreadToFork();
}

QCMutex&
ClientManager::GetMutex()
{
    return mImpl.GetMutex();
}

/* static */ bool
ClientManager::EnqueueSelf(ClientManager::ClientThread* thread, MetaRequest& op)
{
    assert(thread);
    if (! op.clnt) {
        delete &op;
    } else if (thread->IsStarted()) {
        thread->Enqueue(op);
    } else {
        op.next = &op;
        op.clnt->HandleEvent(EVENT_CMD_DONE, &op);
    }
    return true;
}

/* static */ void
ClientManager::SubmitRequestSelf(ClientManager::ClientThread* thread,
    MetaRequest& op)
{
    assert(thread);
    thread->Add(op);
}

} // namespace KFS
