//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/01
// Author: Sriram Rao
//         Mike Ovsiannikov. Re-implement. Implement "client threads".
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
// \file NetDispatch.cc
//
// \brief Meta-server request processing threads implementation.
//
//----------------------------------------------------------------------------

#include "NetDispatch.h"
#include "LayoutManager.h"
#include "ClientSM.h"
#include "LogWriter.h"
#include "LogReceiver.h"
#include "Replay.h"
#include "MetaDataSync.h"
#include "ChildProcessTracker.h"
#include "MetaVrSM.h"
#include "kfstree.h"

#include "kfsio/Acceptor.h"
#include "kfsio/KfsCallbackObj.h"
#include "kfsio/Globals.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/SslFilter.h"
#include "kfsio/CryptoKeys.h"
#include "kfsio/NetManagerWatcher.h"

#include "common/Properties.h"
#include "common/MsgLogger.h"
#include "common/time.h"
#include "common/rusage.h"
#include "common/SingleLinkedQueue.h"
#include "common/StdAllocator.h"

#include "qcdio/QCThread.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"

#include <inttypes.h>
#include <algorithm>
#include <vector>
#include <set>

namespace KFS
{
using std::max;
using std::vector;

using KFS::libkfsio::globalNetManager;
using KFS::libkfsio::globals;

class NetDispatch::CanceledTokens : public ITimeout
{
public:
    CanceledTokens()
        : ITimeout(),
          mTokens(),
          mNetManagerPtr(0),
          mMutexPtr(0),
          mUpdateCount(0)
        {}
    ~CanceledTokens()
        { Set(0, 0); }
    void Set(
        NetManager* inNetManagerPtr,
        QCMutex*    inMutexPtr)
    {
        mMutexPtr = inMutexPtr;
        if (mNetManagerPtr) {
            mNetManagerPtr->UnRegisterTimeoutHandler(this);
        }
        mNetManagerPtr = inNetManagerPtr;
        if (mNetManagerPtr) {
            mNetManagerPtr->RegisterTimeoutHandler(this);
        }
    }
    void RemoveExpired()
    {
        QCStMutexLocker theLocker(mMutexPtr);
        Expire();
    }
    virtual void Timeout()
        { Expire(); }
    void Expire()
    {
        Tokens::iterator theIt = mTokens.begin();
        if (theIt == mTokens.end()) {
            return;
        }
        const time_t theNow = mNetManagerPtr ? mNetManagerPtr->Now() : time(0);
        const time_t kExtra = 5 * 60; // Keep around for 5 more minutes.
        while (theIt != mTokens.end() && theIt->mExpiration + kExtra < theNow) {
            ++theIt;
        }
        if (mTokens.begin() != theIt) {
            QCStMutexLocker theLocker(mMutexPtr);
            mTokens.erase(mTokens.begin(), theIt);
        }
    }
    bool Cancel(
        const DelegationToken& inToken)
    {
        if (inToken.GetValidForSec() <= 0) {
            return false;
        }
        QCStMutexLocker theLocker(mMutexPtr);
        mUpdateCount++;
        return mTokens.insert(Token(inToken)).second;
    }
    bool Cancel(
        int64_t                   inExpiration,
        int64_t                   inIssued,
        kfsUid_t                  inUid,
        DelegationToken::TokenSeq inSeq,
        uint16_t                  inFlags)
    {
        QCStMutexLocker theLocker(mMutexPtr);
        mUpdateCount++;
        return mTokens.insert(Token(
            inExpiration,
            inIssued,
            inUid,
            inSeq,
            inFlags
        )).second;
    }
    bool IsCanceled(
        int64_t                   inExpiration,
        int64_t                   inIssued,
        kfsUid_t                  inUid,
        DelegationToken::TokenSeq inSeq,
        uint16_t                  inFlags,
        uint64_t&                 outUpdateCount)
    {
        QCStMutexLocker theLocker(mMutexPtr);
        outUpdateCount = mUpdateCount;
        return (mTokens.find(Token(
            inExpiration,
            inIssued,
            inUid,
            inSeq,
            inFlags
        )) != mTokens.end());
    }
    bool IsCanceled(
        const DelegationToken& inToken,
        uint64_t&              outUpdateCount)
    {
        QCStMutexLocker theLocker(mMutexPtr);
        outUpdateCount = mUpdateCount;
        return (mTokens.find(Token(inToken)) != mTokens.end());
    }
    int Write(
        ostream& inStream)
    {
        Expire();
        for (Tokens::iterator theIt = mTokens.begin();
                theIt != mTokens.end() && inStream;
                ++theIt) {
            inStream <<
                "delegatecancel"
                "/exp/"    << theIt->mExpiration <<
                "/issued/" << theIt->mIssuedTime <<
                "/uid/"    << theIt->mUid <<
                "/seq/"    << theIt->mSeq <<
                "/flags/"  << theIt->mFlags <<
            "\n";
        }
        return (inStream.fail() ? -EIO : 0);
    }
    uint64_t GetUpdateCount() const
        { return mUpdateCount; }
private:
    struct Token
    {
    public:
        Token(
            const DelegationToken& inToken)
            : mExpiration(inToken.GetIssuedTime() + inToken.GetValidForSec()),
              mIssuedTime(inToken.GetIssuedTime()),
              mUid(inToken.GetUid()),
              mSeq(inToken.GetSeq()),
              mFlags(inToken.GetFlags())
            {}
        Token(
            int64_t                   inExpiration,
            int64_t                   inIssued,
            kfsUid_t                  inUid,
            DelegationToken::TokenSeq inSeq,
            uint16_t                  inFlags)
            : mExpiration(inExpiration),
              mIssuedTime(inIssued),
              mUid(inUid),
              mSeq(inSeq),
              mFlags(inFlags)
            {}
        bool operator<(
            const Token& inRhs) const
        {
            return (
            mExpiration < inRhs.mExpiration ||
                (mExpiration == inRhs.mExpiration &&
                    (mIssuedTime < inRhs.mIssuedTime ||
                        (mIssuedTime == inRhs.mIssuedTime &&
                            (mUid < inRhs.mUid ||
                                (mUid == inRhs.mUid &&
                                    (mSeq < inRhs.mSeq ||
                                        (mSeq == inRhs.mSeq &&
                                            mFlags < inRhs.mFlags)))))))
            );
        }
        bool operator==(
            const Token& inRhs) const
        {
            return (
                mExpiration == inRhs.mExpiration &&
                mIssuedTime == inRhs.mIssuedTime &&
                mUid        == inRhs.mUid        &&
                mSeq        == inRhs.mSeq        &&
                mFlags      == inRhs.mFlags
            );
        }
        int64_t                   mExpiration;
        int64_t                   mIssuedTime;
        kfsUid_t                  mUid;
        DelegationToken::TokenSeq mSeq;
        uint16_t                  mFlags;
    };
    typedef set<
        Token,
        std::less<Token>,
        StdFastAllocator<Token>
    > Tokens;
    Tokens      mTokens;
    NetManager* mNetManagerPtr;
    QCMutex*    mMutexPtr;
    uint64_t    mUpdateCount;
};

class NetDispatch::KeyStore : public CryptoKeys::KeyStore
{
public:
    typedef CryptoKeys::KeyId KeyId;
    typedef CryptoKeys::Key   Key;

    KeyStore(
        NetManager& inNetManager)
        : CryptoKeys::KeyStore(),
          mCryptoKeys(inNetManager, this),
          mRestoreCount(0),
          mInFlightExpiredPtr(0)
        {}
    virtual ~KeyStore()
        {}
    virtual bool NewKey(
        KeyId      inKeyId,
        const Key& inKey,
        int64_t    inKeyTime)
    {
        if (! IsActive()) {
            panic("invalid crypto keys new key invocation");
            return true;
        }
        submit_request(new MetaCryptoKeyNew(inKeyId, inKey, inKeyTime));
        return true;
    }
    virtual bool Expired(
        KeyId inKeyId)
    {
        if (! IsActive()) {
            panic("invalid crypto keys expired invocation");
            return true;
        }
        if (mInFlightExpiredPtr) {
            return true;
        }
        mInFlightExpiredPtr = new MetaCryptoKeyExpired(inKeyId);
        submit_request(mInFlightExpiredPtr);
        return true;
    }
    virtual void WriteKey(
        ostream*   inStreamPtr,
        KeyId      inKeyId,
        const Key& inKey,
        int64_t    inKeyTime)
    {
        if (! inStreamPtr) {
            return;
        }
        ostream&                          theStream = *inStreamPtr;
        CryptoKeys::Key::UrlSafeFmt const theKeyFmt(inKey);
        theStream <<
            "ckey"
            "/" << inKeyTime <<
            "/" << inKeyId <<
            "/" << theKeyFmt <<
        "\n";
    }
    bool Restore(
        KeyId      inKeyId,
        const Key& inKey,
        int64_t    inKeyTime)
    {
        if (mRestoreCount <= 0) {
            mCryptoKeys.Clear();
        }
        mRestoreCount++;
        return mCryptoKeys.Add(inKeyId, inKey, inKeyTime);
    }
    void Handle(
        MetaCryptoKeyNew& inReq)
    {
        if (0 != inReq.status) {
            if (inReq.replayFlag) {
                panic("invalid new crypto key op status in replay");
                return;
            }
            const bool kPendingKeyFlag = true;
            mCryptoKeys.Remove(inReq.keyId, kPendingKeyFlag);
            return;
        }
        if (! mCryptoKeys.Add(inReq.keyId, inReq.key, inReq.time)) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "add failed";
        }
    }
    void Handle(
        MetaCryptoKeyExpired& inReq)
    {
        if (&inReq == mInFlightExpiredPtr) {
            mInFlightExpiredPtr = 0;
        }
        if (0 != inReq.status) {
            return;
        }
        const bool kPendingKeyFlag = false;
        if (! mCryptoKeys.Remove(inReq.keyId, kPendingKeyFlag)) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "remove failed";
        }
    }
    int Write(
        ostream& inStream)
    {
        mCryptoKeys.Write(inStream, 0);
        return (inStream ? 0 : -EIO);
    }
    int Start()
    {
        mActiveFlagPtr = &gLayoutManager.GetPrimaryFlag();
        if (mRestoreCount <= 0 && IsActive() &&
                ! MetaRequest::GetLogWriter().GetMetaVrSM().HasValidNodeId()) {
            string theErrMsg;
            const int theErr = mCryptoKeys.LoadKeysFile(theErrMsg);
            if (theErr < 0 && -ENOENT != theErr) {
                KFS_LOG_STREAM_ERROR <<
                    "load crypto keys error: " << theErrMsg <<
                KFS_LOG_EOM;
                mActiveFlagPtr = 0;
                return theErr;
            }
        }
        return mCryptoKeys.Start();
    }
    void Stop()
    {
        mActiveFlagPtr = 0;
        mCryptoKeys.Stop();
    }
    int SetParameters(
        const Properties& inProperties,
        string&           outErrMsg)
    {
        return mCryptoKeys.SetParameters(
             "metaServer.cryptoKeys.", inProperties, outErrMsg);
    }
    bool EnsureHasValidCryptoKey()
        { return mCryptoKeys.EnsureHasCurrentKey(); }
    const CryptoKeys& GetCryptoKeys() const
        { return mCryptoKeys; }
private:
    CryptoKeys            mCryptoKeys;
    int                   mRestoreCount;
    MetaCryptoKeyExpired* mInFlightExpiredPtr;
private:
    KeyStore(
        const KeyStore& inStore);
    KeyStore& operator=(
        const KeyStore& inStore);
};

void
MetaCryptoKeyNew::handle()
{
    gNetDispatch.GetKeyStore().Handle(*this);
}


void
MetaCryptoKeyExpired::handle()
{
    gNetDispatch.GetKeyStore().Handle(*this);
}

int
NetDispatch::CheckpointCryptoKeys(ostream& os)
{
    return GetKeyStore().Write(os);
}

bool
NetDispatch::EnsureHasValidCryptoKey()
{
    return GetKeyStore().EnsureHasValidCryptoKey();
}

bool
NetDispatch::Restore(
    CryptoKeys::KeyId      inKeyId,
    const CryptoKeys::Key& inKey,
    int64_t                inKeyTime)
{
    return gNetDispatch.GetKeyStore().Restore(inKeyId, inKey, inKeyTime);
}

bool
NetDispatch::CancelToken(
    const DelegationToken& token)
{
    return mCanceledTokens.Cancel(token);
}

bool
NetDispatch::CancelToken(
    int64_t                   inExpiration,
    int64_t                   inIssued,
    kfsUid_t                  inUid,
    DelegationToken::TokenSeq inSeq,
    uint16_t                  inFlags)
{
    return mCanceledTokens.Cancel(
        inExpiration,
        inIssued,
        inUid,
        inSeq,
        inFlags
    );
}

bool
NetDispatch::IsCanceled(
    const DelegationToken& inToken,
    uint64_t&              outUpdateCount)
{
    return mCanceledTokens.IsCanceled(inToken, outUpdateCount);
}

bool
NetDispatch::IsCanceled(
    int64_t                   inExpiration,
    int64_t                   inIssued,
    kfsUid_t                  inUid,
    DelegationToken::TokenSeq inSeq,
    uint16_t                  inFlags,
    uint64_t&                 outUpdateCount)
{
    return mCanceledTokens.IsCanceled(
        inExpiration,
        inIssued,
        inUid,
        inSeq,
        inFlags,
        outUpdateCount
    );
}

int
NetDispatch::WriteCanceledTokens(ostream& os)
{
    return mCanceledTokens.Write(os);
}

uint64_t
NetDispatch::GetCanceledTokensUpdateCount() const
{
    return mCanceledTokens.GetUpdateCount();
}

NetDispatch::NetDispatch()
    : mClientManager(),
      mMetaDataStore(globalNetManager()),
      mChunkServerFactory(),
      mMutex(0),
      mClientManagerMutex(0),
      mKeyStore(*(new KeyStore(globalNetManager()))),
      mCryptoKeys(mKeyStore.GetCryptoKeys()),
      mCanceledTokens(*(new CanceledTokens())),
      mRunningFlag(false),
      mClientThreadCount(0),
      mClientThreadsStartCpuAffinity(-1),
      mWatchdog(),
      mNetManagerWatcher("main", globalNetManager())
{
    mWatchdog.Register(mNetManagerWatcher);
}

NetDispatch::~NetDispatch()
{
    mWatchdog.Unregister(mNetManagerWatcher);
    delete &mCanceledTokens;
    delete &mKeyStore;
}

bool
NetDispatch::Bind(
    const ServerLocation& clientListenerLocation,
    bool                  clientListenerIpV6OnlyFlag,
    const ServerLocation& chunkServerListenerLocation,
    bool                  chunkServerListenerIpV6OnlyFlag)
{
    return (mClientManager.Bind(
            clientListenerLocation, clientListenerIpV6OnlyFlag) &&
        mChunkServerFactory.Bind(globalNetManager(),
            chunkServerListenerLocation, chunkServerListenerIpV6OnlyFlag)
    );
}

int
NetDispatch::GetMaxClientCount() const
{
    return mClientManager.GetMaxClientCount();
}

class MainThreadPrepareToFork : public NetManager::Dispatcher
{
public:
    MainThreadPrepareToFork(
        ClientManager& inClientManager)
        : NetManager::Dispatcher(),
          mClientManager(inClientManager)
        {}
    virtual void DispatchStart();
    virtual void DispatchEnd();
    virtual void DispatchExit()
        {}
private:
    ClientManager& mClientManager;
private:
    MainThreadPrepareToFork(
        const MainThreadPrepareToFork& inPrepare);
    MainThreadPrepareToFork& operator=(
        const MainThreadPrepareToFork& inPrepare);
};

//
// Open up the server for connections.
//
bool
NetDispatch::Start(MetaDataSync& metaDataSync)
{
    if (mMutex || mRunningFlag) {
        KFS_LOG_STREAM_ERROR <<
            "already running:" << mRunningFlag <<
            " mutex: "         << reinterpret_cast<const void*>(mMutex) <<
        KFS_LOG_EOM;
        return false;
    }
    QCMutex dispatchMutex;
    QCStValueChanger<QCMutex*> setDispatchMutex(
        mMutex, 0 < mClientThreadCount ? &dispatchMutex : 0);
    int err;
    if ((err = mKeyStore.Start()) != 0) {
        KFS_LOG_STREAM_ERROR <<
            "invalid crypto keys parameters:" <<
            " status: " << err <<
            " "         << QCUtils::SysError(err < 0 ? -err : err) <<
        KFS_LOG_EOM;
        return false;
    }
    QCStValueChanger<QCMutex*> setClientManagerMutex(
        mClientManagerMutex, GetMutex() ? &mClientManager.GetMutex() : 0);
    mRunningFlag        = true;
    // Start the acceptors so that it sets up a connection with the net
    // manager for listening.
    QCMutex cancelTokensMutex;
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
                    mClientThreadsStartCpuAffinity,
                metaDataSync
            ) &&
            mChunkServerFactory.StartAcceptor()) {
        if (0 != (err = mMetaDataStore.Start())) {
            KFS_LOG_STREAM_ERROR <<
                "failed to start meta data store: " <<
                QCUtils::SysError(err < 0 ? -err : err) <<
            KFS_LOG_EOM;
        } else {
            mCanceledTokens.RemoveExpired();
            mCanceledTokens.Set(
                &globalNetManager(),
                GetMutex() ? &cancelTokensMutex : 0
            );
            const bool              kWakeupAndCleanupFlag = true;
            MainThreadPrepareToFork prepareToFork(mClientManager);
            gLayoutManager.StartServicing();
            mWatchdog.Start();
            // Run main thread event processing.
            globalNetManager().MainLoop(
                GetMutex(),
                kWakeupAndCleanupFlag,
                &prepareToFork
            );
            mWatchdog.Stop();
        }
    } else {
        err = -EINVAL;
    }
    gChildProcessTracker.CancelAll();
    mMetaDataStore.Shutdown();
    mClientManager.Shutdown();
    metaDataSync.Shutdown();
    mCanceledTokens.Set(0, 0);
    mKeyStore.Stop();
    mRunningFlag = false;
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

void
NetDispatch::CurrentThreadForkDone()
{
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
    void AddCounter(const char* name, MetaOp opName)
    {
        if (! name || ! *name) {
            return;
        }
        string nm;
        nm.reserve(strlen(name));
        for (const char* p = name; *p != 0; ++p) {
            if (name == p) {
                nm += *p;
            } else {
                const int s = *p & 0xFF;
                if ('_' == s) {
                    nm += ' ';
                } else  if ('A' <= s && s <= 'Z') {
                    nm += (char)(s - 'A' + 'a');
                }
            }
        }
        Counter* const c = new Counter(nm.c_str());
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
#       define KfsAddMetaRpcCounter(name) AddCounter("OP_" #name, META_##name);
            KfsForEachMetaOpId(KfsAddMetaRpcCounter)
#       undef KfsAddMetaRpcCounter

        globals().counterManager.AddCounter(&mNumFiles);
        globals().counterManager.AddCounter(&mNumDirs);
        globals().counterManager.AddCounter(&mNumChunks);
        globals().counterManager.AddCounter(&mPathToFidCacheHit);
        globals().counterManager.AddCounter(&mPathToFidCacheMiss);
    }
private:
    MetaOpCounters(const MetaOpCounters&);
    MetaOpCounters& operator=(const MetaOpCounters&);
}* MetaOpCounters::sInstance(MetaOpCounters::MakeInstance());

class RequestStatsGatherer
{
public:
    static RequestStatsGatherer& Instance()
    {
        static RequestStatsGatherer sRequestStatsGatherer;
        return sRequestStatsGatherer;
    }
    void OpDone(
        const MetaRequest& op)
    {
        if (! gNetDispatch.IsRunning()) {
            // Do not count RPCs during initialization restore / replay.
            return;
        }
        const int64_t timeNowUsec     = microseconds();
        const int64_t reqTimeUsec     = timeNowUsec - op.submitTime;
        const int64_t reqProcTimeUsec = timeNowUsec - op.processTime;
        MetaOpCounters::Update(op.op, reqProcTimeUsec);
        if (reqProcTimeUsec > mOpTimeWarningThresholdMicroSec) {
            KFS_LOG_STREAM_INFO <<
                "time spent processing: " << op.Show() <<
                " is: "            << (reqProcTimeUsec * 1e-6) <<
                " total: "         << (reqTimeUsec * 1e-6) <<
                " was submitted: " << op.submitCount <<
            KFS_LOG_EOM;
        }
        const int idx =
            ((op.op < 0 || op.op >= META_NUM_OPS_COUNT) ?
                (int)kOtherReqId :
            ((op.op == META_ALLOCATE &&
                op.logAction == MetaRequest::kLogNever) ?
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
        LogWriter::Counters logCtrs;
        MetaRequest::GetLogWriter().GetCounters(logCtrs);
        mNextTime = timeNowUsec + mStatsIntervalMicroSec;
        const char* kDelim = " ";
        KFS_LOG_STREAM_START(mLogLevel, logStream);
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
            os <<
                kDelim << logCtrs.mLogTimeOpsCount <<
                kDelim << logCtrs.mLogErrorOpsCount <<
                kDelim << logCtrs.mLogTimeUsec <<
                kDelim << logCtrs.mLogTimeUsec
            ;
        KFS_LOG_STREAM_END;
        const bool kRusageSelfFlag = true;
        KFS_LOG_STREAM_START(mLogLevel, logStream);
            ostream& os = logStream.GetStream();
            os << "===rusage=self: ";
            showrusage(os, ": ", kDelim, kRusageSelfFlag);
        KFS_LOG_STREAM_END;
        KFS_LOG_STREAM_START(mLogLevel, logStream);
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
        MsgLogger::LogLevel const logLevel = MsgLogger::GetLogLevelId(
        props.getValue(
            "metaServer.statsGatherer.countersLogLevel",
            MsgLogger::GetLogLevelNamePtr(mLogLevel)
        ));
        if (MsgLogger::kLogLevelUndef != logLevel) {
            mLogLevel = logLevel;
        }
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
        LogWriter::Counters logCtrs;
        MetaRequest::GetLogWriter().GetCounters(logCtrs);
        os <<
            "LOG_WRITER" <<
            kDelim << logCtrs.mLogTimeOpsCount <<
            kDelim << (logCtrs.mLogTimeOpsCount * ptotal)  <<
            kDelim << logCtrs.mLogErrorOpsCount <<
            kDelim << (logCtrs.mLogErrorOpsCount * perrors) <<
            kDelim << logCtrs.mLogTimeUsec <<
            kDelim << logCtrs.mLogTimeUsec <<
            "\n"
        ;
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
    struct Counter
    {
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
    int64_t             mNextTime;
    int64_t             mStatsIntervalMicroSec;
    int64_t             mOpTimeWarningThresholdMicroSec;
    int64_t             mUserCpuMicroSec;
    int64_t             mSystemCpuMicroSec;
    MsgLogger::LogLevel mLogLevel;
    Counter             mRequest[kReqTypesCnt];
    IOBuffer::WOStream  mWOStream;

    RequestStatsGatherer()
        : mNextTime(0),
          mStatsIntervalMicroSec(30000000),
          mOpTimeWarningThresholdMicroSec(200000),
          mUserCpuMicroSec(0),
          mSystemCpuMicroSec(0),
          mLogLevel(MsgLogger::kLogLevelNOTICE),
          mWOStream()
        {}

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
private:
    RequestStatsGatherer(const RequestStatsGatherer&);
    RequestStatsGatherer& operator=(const RequestStatsGatherer&);
};
static RequestStatsGatherer& sReqStatsGatherer =
    RequestStatsGatherer::Instance();

int NetDispatch::SetParameters(const Properties& props)
{
    mWatchdog.SetParameters("metaServer.watchdog.", props);
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
    mClientManager.SetParameters(props);
    mMetaDataStore.SetParameters("metaServer.dataStore.", props);

    string    errMsg;
    const int err = mKeyStore.SetParameters(props, errMsg);
    if (0 != err) {
        KFS_LOG_STREAM_ERROR <<
            "crypto keys set parameters:"
            " status: " << err << " " << errMsg <<
        KFS_LOG_EOM;
    }
    return err;
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
        MetaRequest::Release(r);
    }
}

void NetDispatch::SetMaxClientSockets(int count)
{
    mClientManager.SetMaxClientSockets(count);
}

const char* const kLogReciverParamsPrefix = "metaServer.log.receiver.";

class LogReceiverThread :
    private QCRunnable,
    private NetManager::Dispatcher,
    private LogReceiver::Waker
{
public:
    LogReceiverThread()
        : QCRunnable(),
          NetManager::Dispatcher(),
          LogReceiver::Waker(),
          mParameters(),
          mNetManager(),
          mLogReceiver(),
          mThread(),
          mWakeupFlag(false),
          mStartedFlag(false),
          mParametersUpdatePendingFlag(false),
          mSignalCnt(0),
          mNetManagerWatcher("LogRecv", mNetManager)
          {}
    ~LogReceiverThread()
        { LogReceiverThread::Shutdown(); }
    void ChildAtFork()
        { mNetManager.ChildAtFork(); }
    bool IsThreadStarted() const
        { return mThread.IsStarted(); }
    int Start(
        int inMaxSocketsCount)
    {
        if (mStartedFlag || mThread.IsStarted()) {
            return -EINVAL;
        }
        QCMutex* const mutex = gNetDispatch.GetMutex();
        QCStMutexLocker lock(mutex);
        SetParameters(mParameters);
        mParametersUpdatePendingFlag = false;
        if (! mLogReceiver.SetParameters(
                kLogReciverParamsPrefix, mParameters)) {
            return -EINVAL;
        }
        const vrNodeId_t vrId =
            MetaRequest::GetLogWriter().GetMetaVrSM().GetNodeId();
        if (vrId < 0 || ! mLogReceiver.GetListenerAddress().IsValid()) {
            // No listener address, or valid node id -- do not start receiver.
            return 0;
        }
        const int err = mLogReceiver.Start(
            mutex ? mNetManager : globalNetManager(), *this,
            replayer.getCommitted(),
            replayer.getLastLogSeq(),
            metatree.GetFsId(),
            vrId,
            inMaxSocketsCount
        );
        if (err) {
            return err;
        }
        mStartedFlag = true;
        if (mutex) {
            Properties::String cpIdxParamName(kLogReciverParamsPrefix);
            int kStackSize = 64 << 10;
            mThread.Start(this, kStackSize, "LogReceiver",
                QCThread::CpuAffinity(mParameters.getValue(
                    cpIdxParamName.Append("cpuAffinityIndex"), -1)));
        }
        return 0;
    }
    void SetParameters(
        const Properties& inParameters)
    {
        assert(! gNetDispatch.GetMutex() || gNetDispatch.GetMutex()->IsOwned());
        inParameters.copyWithPrefix(kLogReciverParamsPrefix, mParameters);
        if (! mStartedFlag) {
            return;
        }
        if (mThread.IsStarted()) {
            mParametersUpdatePendingFlag = true;
            SyncAddAndFetch(mSignalCnt, 1);
            mNetManager.Wakeup();
        } else {
            mLogReceiver.SetParameters(kLogReciverParamsPrefix, mParameters);
        }
    }
    void Shutdown()
    {
        if (! mStartedFlag) {
            return;
        }
        mStartedFlag = false;
        if (mThread.IsStarted()) {
            mNetManager.Shutdown();
            mNetManager.Wakeup();
            mThread.Join();
        }
        mLogReceiver.Shutdown();
    }
    void PrepareToFork()
    {
        if (! mThread.IsStarted()) {
            return;
        }
        QCMutex* const mutex = gNetDispatch.GetMutex();
        if (! mutex) {
            return;
        }
        assert(mutex->IsOwned());
        SyncAddAndFetch(mSignalCnt, 1);
        mNetManager.Wakeup();
    }
    virtual void Wakeup()
    {
        mWakeupFlag = true;
        if (gNetDispatch.GetMutex()) {
            SyncAddAndFetch(mSignalCnt, 1);
            mNetManager.Wakeup();
        } else {
            globalNetManager().Wakeup();
        }
    }
    void Dispatch()
    {
        assert(! gNetDispatch.GetMutex());
        if (! mWakeupFlag) {
            return;
        }
        mWakeupFlag = false;
        mLogReceiver.Dispatch();
        // The main thread unconditionally schedules flush and the end of event
        // processing in MainThreadPrepareToFork::DispatchEnd()
    }
private:
    Properties        mParameters;
    NetManager        mNetManager;
    LogReceiver       mLogReceiver;
    QCThread          mThread;
    bool              mWakeupFlag;
    bool              mStartedFlag;
    bool              mParametersUpdatePendingFlag;
    volatile int      mSignalCnt;
    NetManagerWatcher mNetManagerWatcher;

    virtual void Run()
    {
        QCMutex* const kMutex                = 0;
        const bool     kWakeupAndCleanupFlag = true;
        gNetDispatch.GetWatchdog().Register(mNetManagerWatcher);
        mNetManager.MainLoop(kMutex, kWakeupAndCleanupFlag, this);
        gNetDispatch.GetWatchdog().Unregister(mNetManagerWatcher);
    }
    virtual void DispatchStart()
    {
        if (SyncAddAndFetch(mSignalCnt, 0) == 0) {
            return;
        }
        QCStMutexLocker lock(gNetDispatch.GetMutex());
        mSignalCnt = 0;
        gNetDispatch.PrepareToFork();
        bool theFlushFlag;
        if (mWakeupFlag) {
            mWakeupFlag = false;
            theFlushFlag = mLogReceiver.Dispatch();
        } else {
            theFlushFlag = false;
        }
        if (mParametersUpdatePendingFlag) {
            mParametersUpdatePendingFlag = false;
            mLogReceiver.SetParameters(kLogReciverParamsPrefix, mParameters);
        }
        if (theFlushFlag) {
            MetaRequest::GetLogWriter().ScheduleFlush();
        }
        gNetDispatch.ForkDone();
    }
    virtual void DispatchEnd()
        {}
    virtual void DispatchExit()
        {}
};

class ClientManager::Impl : public IAcceptorOwner
{
public:
    Impl()
        : IAcceptorOwner(),
          mAcceptor(0),
          mClientThreads(0),
          mClientThreadCount(-1),
          mNextThreadIdx(0),
          mMaxClientCount(64 << 10),
          mMaxClientSocketCount(mMaxClientCount),
          mMutex(),
          mPrepareToForkDoneCond(),
          mForkDoneCond(),
          mForkDoneCount(0),
          mLogReceiverThread(),
          mPrepareToForkFlag(false),
          mPrepareToForkCnt(0)
        {};
    virtual ~Impl();
    bool Bind(const ServerLocation& location, bool ipV6OnlyFlag);
    bool StartAcceptor(int threadCount, int startCpuAffinity,
        MetaDataSync& metaDataSync);
    virtual KfsCallbackObj* CreateKfsCallbackObj(NetConnectionPtr &conn);
    void Shutdown();
    void ChildAtFork();
    QCMutex& GetMutex()
        { return mMutex; }
    void PrepareCurrentThreadToFork();
    // The prepare thread count includes the "main" and log receiver threads.
    inline int GetPrepareToForkCount() const
    {
        return (mClientThreadCount +
            (mLogReceiverThread.IsThreadStarted() ? 1 : 0));
    }
    inline void PrepareToFork()
    {
        QCMutex* const mutex = gNetDispatch.GetMutex();
        if (! mutex) {
            mLogReceiverThread.Dispatch();
            return;
        }
        assert(mutex->IsOwned());
        while (mPrepareToForkFlag) {
            if (GetPrepareToForkCount() <= ++mPrepareToForkCnt) {
                mPrepareToForkDoneCond.Notify();
            }
            const uint64_t forkDoneCount = mForkDoneCount;
            while (forkDoneCount == mForkDoneCount) {
                mForkDoneCond.Wait(*mutex);
            }
        }
    }
    inline void ForkDone()
    {
        QCMutex* const mutex = gNetDispatch.GetMutex();
        if (! mutex) {
            return;
        }
        assert(mutex->IsOwned());
        if (! mPrepareToForkFlag) {
            return;
        }
        mPrepareToForkFlag = false;
        mPrepareToForkCnt  = 0;
        mForkDoneCount++;
        // Resume threads after fork(s) completes and the lock gets released.
        mForkDoneCond.NotifyAll();
    }
    void SetParameters(const Properties& params)
    {
        mMaxClientCount = params.getValue(
            "metaServer.maxClientCount", mMaxClientCount);
        mLogReceiverThread.SetParameters(params);
    }
    void SetMaxClientSockets(int count)
    {
        mMaxLogRecvSocketsCount = max(min(64, count / 3), count / 16);
        mMaxClientSocketCount   = count - mMaxLogRecvSocketsCount;
        mMaxLogRecvSocketsCount /= 2; // Reserve half for log transmitter.
        KFS_LOG_STREAM_INFO <<
            "socket limits:"
            " clients: "     << mMaxClientSocketCount <<
            " log:"
            " receiver: "    << mMaxLogRecvSocketsCount <<
            " transmitter: " <<
                (count - mMaxClientSocketCount - mMaxLogRecvSocketsCount) <<
        KFS_LOG_EOM;
    }
    int GetMaxClientCount() const
        { return min(mMaxClientSocketCount, mMaxClientCount); }
private:
    class ClientThread;
    // The socket object which is setup to accept connections.
    Acceptor*                    mAcceptor;
    ClientManager::ClientThread* mClientThreads;
    int                          mClientThreadCount;
    int                          mNextThreadIdx;
    int                          mMaxClientCount;
    int                          mMaxClientSocketCount;
    int                          mMaxLogRecvSocketsCount;
    QCMutex                      mMutex;
    QCCondVar                    mPrepareToForkDoneCond;
    QCCondVar                    mForkDoneCond;
    uint64_t                     mForkDoneCount;
    LogReceiverThread            mLogReceiverThread;
    volatile bool                mPrepareToForkFlag;
    volatile int                 mPrepareToForkCnt;
};

void
ClientManager::SetParameters(const Properties& props)
{
    mImpl.SetParameters(props);
}

void
ClientManager::SetMaxClientSockets(int count)
{
    mImpl.SetMaxClientSockets(count);
}

int
ClientManager::GetMaxClientCount() const
{
    return mImpl.GetMaxClientCount();
}

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

inline void
ClientManager::ForkDone()
{
    mImpl.ForkDone();
}

inline void
NetDispatch::ForkDone()
{
    mClientManager.ForkDone();
}

/* virtual */ void
MainThreadPrepareToFork::DispatchStart()
{
    if (gLayoutManager.GetUserAndGroup().GetUpdateCount() !=
            gLayoutManager.GetClientAuthContext().GetUserAndGroupUpdateCount()
            ) {
        gLayoutManager.GetClientAuthContext().SetUserAndGroup(
            gLayoutManager.GetUserAndGroup());
    }
    mClientManager.PrepareToFork();
}

/* virtual */ void
MainThreadPrepareToFork::DispatchEnd()
{
    MetaRequest::GetLogWriter().ScheduleFlush();
    mClientManager.ForkDone();
}

// All acceptors run in the main thread running global net manager event loop.
// New client "connections" are passed to the client threads via the queue.
// Each client thread runs each client "connection" (ClientSM instance) in its
// own net manager event loop.
// The core of the request processing submit_request() / MetaRequest::handle()
// is serialized with the mutex. The attempt is made to process requests in
// batches in order to reduce lock acquisition frequency.
// The client thread run loop is in DispatchStart() method below, which is
// invoked from NetManager::MainLoop().
// The pending requests queue depth governed by the ClientSM parameters.
// ClientSM logic limits number of outstanding requests as well as pending io
// bytes to ensure request processing "fairness" in respect to all the client
// connections.
class ClientManager::ClientThread :
    public QCRunnable,
    private NetManager::Dispatcher
{
public:
    ClientThread()
        : QCRunnable(),
          NetManager::Dispatcher(),
          mMutex(0),
          mThread(),
          mNetManager(),
          mWOStream(),
          mReqQueue(),
          mCliQueue(),
          mReqPendingQueue(),
          mFlushQueue(8 << 10),
          mAuthContext(),
          mAuthCtxUpdateCount(gLayoutManager.GetAuthCtxUpdateCount() - 1),
          mNetManagerWatcher("client", mNetManager)
    {
        gLayoutManager.UpdateClientAuthContext(
            mAuthCtxUpdateCount, mAuthContext);
    }
    virtual ~ClientThread()
    {
        if (mThread.IsStarted()) {
            mNetManager.Shutdown();
            mNetManager.Wakeup();
            mThread.Join();
        }
        ClientThread::DispatchStart();
        assert(mCliQueue.IsEmpty());
    }
    bool Start(QCMutex* mutex, int cpuIndex)
    {
        if (mThread.IsStarted()) {
            return true;
        }
        mMutex = mutex;
        const int kStackSize = 384 << 10;
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
        QCMutex* const kMutex                = 0;
        bool     const kWakeupAndCleanupFlag = true;
        gNetDispatch.GetWatchdog().Register(mNetManagerWatcher);
        mNetManager.MainLoop(kMutex, kWakeupAndCleanupFlag, this);
        gNetDispatch.GetWatchdog().Unregister(mNetManagerWatcher);
    }
    virtual void DispatchStart()
    {
        ReqQueue reqPendingQueue;
        reqPendingQueue.PushBack(mReqPendingQueue);

        // Keep the lock acquisition and PrepareToFork() next to each other, in
        // order to ensure that the mutex is locked while dispatching requests
        // and prevent prepare to fork recursion, as PrepareToFork() can release
        // and re-acquire the mutex by waiting on the "fork done" condition.
        QCStMutexLocker dispatchLocker(gNetDispatch.GetMutex());
        gNetDispatch.PrepareToFork();
        gLayoutManager.UpdateClientAuthContext(mAuthCtxUpdateCount, mAuthContext);
        if (gLayoutManager.GetUserAndGroup().GetUpdateCount() !=
                mAuthContext.GetUserAndGroupUpdateCount()) {
            mAuthContext.SetUserAndGroup(gLayoutManager.GetUserAndGroup());
        }
        assert(mReqPendingQueue.IsEmpty());
        // Dispatch requests.
        MetaRequest* op;
        while ((op = reqPendingQueue.PopFront())) {
            submit_request(op);
        }
        MetaRequest::GetLogWriter().ScheduleFlush();
        gNetDispatch.ForkDone();
        mPrimaryFlag = gLayoutManager.IsPrimary() &&
            MetaRequest::GetLogWriter().IsPrimary(mNetManager.NowUsec());
        dispatchLocker.Unlock();

        CliQueue cliQueue;
        ReqQueue reqQueue;
        QCStMutexLocker threadQueuesLocker(mMutex);
        reqQueue.PushBack(mReqQueue);
        cliQueue.PushBack(mCliQueue);
        threadQueuesLocker.Unlock();

        // Send responses. Try to minimize number of system calls by
        // attempting to send multiple responses with single write call.
        FlushQueue::iterator it = mFlushQueue.begin();
        NetConnectionPtr conn;
        while ((op = reqQueue.PopFront())) {
            op->next = op; // Mark op, for the client manager's dispatch.
            const NetConnectionPtr& cn = GetConnection(*op);
            if (cn && ! cn->IsWriteReady()) {
                conn = cn; // Has no data pending.
            }
            op->clnt->HandleEvent(EVENT_CMD_DONE, op);
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
        ClientSM*  cli;
        while ((cli = cliQueue.PopFront())) {
            const NetConnectionPtr& conn = cli->GetConnection();
            assert(conn);
            conn->SetOwningKfsCallbackObj(cli);
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
    virtual void DispatchEnd()
        {}
    virtual void DispatchExit()
        {}
    void Enqueue(MetaRequest& op)
    {
        if (! op.clnt) {
            MetaRequest::Release(&op);
            return;
        }
        QCStMutexLocker locker(mMutex);
        const bool wasEmptyFlag = mReqQueue.IsEmpty();
        op.next = 0;
        mReqQueue.PushBack(op);
        locker.Unlock();
        if (wasEmptyFlag) {
            mNetManager.Wakeup();
        }
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
        const bool wasEmptyFlag = mCliQueue.IsEmpty();
        mCliQueue.PushBack(*cli);
        locker.Unlock();
        if (wasEmptyFlag) {
            mNetManager.Wakeup();
        }
    }
    void Add(MetaRequest& op)
    {
        // This method must be called from the client thread: ClientSM
        // adds request to the pending processing queue.
        const bool wasEmptyFlag = mReqPendingQueue.IsEmpty();
        mReqPendingQueue.PushBack(op);
        if (wasEmptyFlag) {
            mNetManager.Wakeup();
        }
    }
    bool IsStarted() const
        { return mThread.IsStarted(); }
    void ChildAtFork()
        { mNetManager.ChildAtFork(); }
    void Wakeup()
        { mNetManager.Wakeup(); }
    AuthContext& GetAuthContext()
        { return mAuthContext; }
    bool IsPrimary() const
        { return mPrimaryFlag; }
private:
    class CliAccessor
    {
    public:
        static ClientSM*& Next(ClientSM& cli)
            { return cli.GetNext(); }
    };
    typedef vector<NetConnectionPtr>                             FlushQueue;
    typedef SingleLinkedQueue<MetaRequest, MetaRequest::GetNext> ReqQueue;
    typedef SingleLinkedQueue<ClientSM,    CliAccessor>          CliQueue;

    QCMutex*           mMutex;
    QCThread           mThread;
    NetManager         mNetManager;
    IOBuffer::WOStream mWOStream;
    ReqQueue           mReqQueue;
    CliQueue           mCliQueue;
    ReqQueue           mReqPendingQueue;
    FlushQueue         mFlushQueue;
    AuthContext        mAuthContext;
    uint64_t           mAuthCtxUpdateCount;
    bool               mPrimaryFlag;
    NetManagerWatcher  mNetManagerWatcher;
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
ClientManager::Impl::Bind(const ServerLocation& location, bool ipV6OnlyFlag)
{
    delete mAcceptor;
    mAcceptor = 0;
    const bool kBindOnlyFlag = true;
    mAcceptor = new Acceptor(
        globalNetManager(), location, ipV6OnlyFlag, this, kBindOnlyFlag);
    return mAcceptor->IsAcceptorStarted();
}

bool
ClientManager::Impl::StartAcceptor(int threadCount, int startCpuAffinity,
    MetaDataSync& metaDataSync)
{
    if (! mAcceptor) {
        return false;
    }
    mAcceptor->StartListening();
    if (! mAcceptor->IsAcceptorStarted()) {
        return false;
    }
    if (mLogReceiverThread.Start(mMaxLogRecvSocketsCount) != 0) {
        return false;
    }
    if (0 <= mClientThreadCount || mClientThreads) {
        return true;
    }
    mClientThreadCount = max(threadCount, 0);
    if (0 < mClientThreadCount) {
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
    }
    metaDataSync.StartLogSync(
        MetaRequest::GetLogWriter().GetCommittedLogSeq()
    );
    return true;
};

KfsCallbackObj*
ClientManager::Impl::CreateKfsCallbackObj(NetConnectionPtr& conn)
{
    if (mClientThreadCount < 0 || ! conn || ! conn->IsGood()) {
        return 0;
    }
    const int connCount = ClientSM::GetClientCount();
    if (GetMaxClientCount() <= connCount) {
        // The value doesn't reflect the active connection count, but rather
        // number of existing client state machines. This should be OK here, as
        // with no state machines "leak" it wouldn't make much difference.
        // The leak, if exists, must be fixed, of course.
        KFS_LOG_STREAM_ERROR << conn->GetPeerName() <<
            " over connection limit: " << mMaxClientCount <<
            " max sockets: "           << mMaxClientSocketCount <<
            " connection count: "      << connCount <<
            " closing connection" <<
        KFS_LOG_EOM;
        return 0;
    }
    if (mClientThreadCount == 0) {
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
    mLogReceiverThread.Shutdown();
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
    mLogReceiverThread.ChildAtFork();
}

void
ClientManager::Impl::PrepareCurrentThreadToFork()
{
    QCMutex* const mutex = gNetDispatch.GetMutex();
    if (! mutex) {
        return;
    }
    assert(mutex->IsOwned());
    if (mPrepareToForkFlag) {
        assert(GetPrepareToForkCount() == mPrepareToForkCnt);
        return;
    }
    mPrepareToForkFlag = true;
    mPrepareToForkCnt  = 0;
    for (int i = 0; i < mClientThreadCount; i++) {
        mClientThreads[i].Wakeup();
    }
    globalNetManager().Wakeup();
    mLogReceiverThread.PrepareToFork();
    while (mPrepareToForkCnt < GetPrepareToForkCount()) {
        mPrepareToForkDoneCond.Wait(*mutex);
    }
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
ClientManager::Bind(
    const ServerLocation& location,
    bool                  ipV6OnlyFlag)
{
    return mImpl.Bind(location, ipV6OnlyFlag);
}


bool
ClientManager::StartAcceptor(int threadCount, int startCpuAffinity,
    MetaDataSync& metaDataSync)
{
    return mImpl.StartAcceptor(threadCount, startCpuAffinity, metaDataSync);
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
        MetaRequest::Release(&op);
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

/* static */ AuthContext&
ClientManager::GetAuthContext(ClientThread* inThread)
{
    return (inThread ? inThread->GetAuthContext() :
        gLayoutManager.GetClientAuthContext());
}

/* static */ bool
ClientManager::IsPrimary(ClientThread* thread)
{
    return (thread ? thread->IsPrimary() : gLayoutManager.IsPrimary() &&
        MetaRequest::GetLogWriter().IsPrimary(globalNetManager().NowUsec()));
}

} // namespace KFS
