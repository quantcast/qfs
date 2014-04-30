//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2014/04/23
// Author: Mike Ovsiannikov
//
// Copyright 2014 Quantcast Corp.
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

#include "ClientThread.h"
#include "ClientSM.h"

#include "common/kfsatomic.h"

#include "qcdio/QCThread.h"
#include "qcdio/QCMutex.h"
#include "qcdio/QCStUtils.h"
#include "qcdio/QCDebug.h"
#include "qcdio/QCUtils.h"

#include "kfsio/NetManager.h"
#include "kfsio/ITimeout.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/Globals.h"
#include "kfsio/Checksum.h"

namespace KFS
{
using libkfsio::globalNetManager;

ClientThreadListEntry::~ClientThreadListEntry()
{
    QCRTASSERT(! mOpsHeadPtr && ! mOpsTailPtr && ! mNextPtr && ! mGrantedFlag);
}

ClientThreadRemoteSyncListEntry::~ClientThreadRemoteSyncListEntry()
{
    QCRTASSERT(! mOpsHeadPtr && ! mOpsTailPtr && ! mNextPtr && ! mFinishFlag);
}

class ClientThread::Impl : public QCRunnable, public ITimeout
{
public:
    class StMutexLocker
    {
    public:
        StMutexLocker(
            Impl& inImpl)
            : mLockedFlag(true)
        {
            Impl::GetMutex().Lock();
            QCASSERT(
                (! Impl::sCurrentClientThreadPtr && sLockCnt == 0) ||
                (&inImpl.mOuter == sCurrentClientThreadPtr && 0 < sLockCnt)
            );
            if (sLockCnt++ == 0) {
                Impl::sCurrentClientThreadPtr = &inImpl.mOuter;
            }
        }
        ~StMutexLocker()
            { StMutexLocker::Unlock(); }
        void Unlock()
        {
            if (! mLockedFlag) {
                return;
            }
            QCASSERT(0 < sLockCnt);
            if (--sLockCnt == 0) {
                Impl::sCurrentClientThreadPtr = 0;
            }
            mLockedFlag = false;
            Impl::GetMutex().Unlock();
        }
    private:
        bool       mLockedFlag;
        static int sLockCnt;
    private:
        StMutexLocker(
            const StMutexLocker& inLocker);
        StMutexLocker& operator=(
            const StMutexLocker& inLocker);
    };
    friend class StMutexLocker;

    typedef ClientThread Outer;

    Impl(
        ClientThread& inOuter)
        : QCRunnable(),
          mThread(),
          mNetManager(),
          mAddQueueTailPtr(0),
          mAddQueueHeadPtr(0),
          mRunQueueTailPtr(0),
          mRunQueueHeadPtr(0),
          mSyncQueueTailPtr(0),
          mSyncQueueHeadPtr(0),
          mTmpDispatchQueue(),
          mWakeupCnt(0),
          mOuter(inOuter)
    {
        QCASSERT( GetMutex().IsOwned());
        mTmpDispatchQueue.reserve(1 << 10);
    }
    ~Impl()
        { Impl::Stop(); } 
    void Add(
        ClientSM& inClient)
    {
        QCASSERT( GetMutex().IsOwned());
        if (Enqueue(inClient, mAddQueueHeadPtr, mAddQueueTailPtr)) {
            Wakeup();
        }
    }
    virtual void Run()
    {
        mNetManager.MainLoop();
    }
    bool IsStarted() const
        { return mThread.IsStarted(); }
    void Start()
    {
        QCASSERT( GetMutex().IsOwned());
        if (! IsStarted()) {
            mRunFlag = true;
            const int kStackSize = 32 << 10;
            mThread.Start(this, kStackSize, "ClientThread");
        }
    }
    void Stop()
    {
        QCASSERT( GetMutex().IsOwned());
        if (! mRunFlag) {
            return;
        }
        mRunFlag = false;
        Wakeup();

        QCStMutexUnlocker theUnlocker(GetMutex());
        mThread.Join();
    }
    virtual void Timeout()
    {
        if (SyncAddAndFetch(mWakeupCnt, 0) <= 0) {
            return;
        }
        QCASSERT( ! GetMutex().IsOwned());
        StMutexLocker theLocker(*this);

        mWakeupCnt = 0;
        ClientSM* thePtr = mAddQueueHeadPtr;
        mAddQueueTailPtr = 0;
        mAddQueueHeadPtr = 0;
        while (thePtr) {
            ClientSM& theCur = *thePtr;
            thePtr = GetNextPtr(theCur);
            GetNextPtr(theCur) = 0;
            const NetConnectionPtr& theConnPtr = GetConnection(theCur);
            QCASSERT(theConnPtr);
            theConnPtr->SetOwningKfsCallbackObj(&theCur);
            mNetManager.AddConnection(theConnPtr);
        }
        if (! mRunFlag) {
            mNetManager.Shutdown();
        }
        thePtr = mRunQueueHeadPtr;
        mRunQueueTailPtr = 0;
        mRunQueueHeadPtr = 0;
        TmpDispatchQueue& theQueue = GetTmpDispatchQueue();
        theQueue.clear();
        while (thePtr) {
            ClientSM& theCur = *thePtr;
            thePtr = GetNextPtr(theCur);
            GetNextPtr(theCur) = 0;
            theQueue.push_back(&theCur);
        }
        RemoteSyncSM* theSyncPtr = mSyncQueueHeadPtr;
        mSyncQueueHeadPtr = 0;
        mSyncQueueTailPtr = 0;
        mTmpSyncSMQueue.clear();
        while (theSyncPtr) {
            RemoteSyncSM& theCur = *theSyncPtr;
            theSyncPtr = GetNextPtr(theCur);
            GetNextPtr(theCur) = 0;
            mTmpSyncSMQueue.push_back(&theCur);
        }
        for (TmpDispatchQueue::const_iterator theIt = theQueue.begin();
                theIt != theQueue.end();
                ++theIt) {
            RunPending(**theIt);
        }
        for (TmpSyncSMQueue::const_iterator theIt = mTmpSyncSMQueue.begin();
                theIt != mTmpSyncSMQueue.end();
                ++theIt) {
            RunPending(**theIt);
        }
        theLocker.Unlock();
        for (TmpDispatchQueue::const_iterator theIt = theQueue.begin();
                theIt != theQueue.end();
                ++theIt) {
            GetConnection(**theIt)->StartFlush();
        }
    }
    bool Handle(
        ClientSM& inClient,
        int       inCode,
        void*     inDataPtr)
    {
        if (inCode == EVENT_CMD_DONE) {
            if (GetCurrentClientThreadPtr() == &mOuter) {
                return false;
            }
            QCASSERT(inDataPtr);
            if (AddPending(*reinterpret_cast<KfsOp*>(inDataPtr), inClient) &&
                    Enqueue(inClient, mRunQueueHeadPtr, mRunQueueTailPtr)) {
                Wakeup();
            }
            return true;
        }
        QCASSERT(! GetMutex().IsOwned());
        ClientThreadListEntry& theEntry = inClient;
        if (inCode == EVENT_NET_READ) {
            QCASSERT(inDataPtr);
            IOBuffer& theBuf = *reinterpret_cast<IOBuffer*>(inDataPtr);
            if (theEntry.mReceiveOpFlag) {
                theEntry.mReceivedHeaderLen = 0;
                if (! IsMsgAvail(&theBuf, &theEntry.mReceivedHeaderLen)) {
                    return true;
                }
                theEntry.mReceivedOpPtr = 0;
                if (ParseClientCommand(
                        theBuf,
                        theEntry.mReceivedHeaderLen,
                        &theEntry.mReceivedOpPtr,
                        mParseBuffer) != 0) {
                    theEntry.mReceivedOpPtr     = 0;
                    theEntry.mReceiveOpFlag     = false;
                    theEntry.mReceivedHeaderLen = 0;
                }
            } else if (0 <= theEntry.mReceiveByteCount) {
                if (theBuf.BytesConsumable() < theEntry.mReceiveByteCount) {
                    return true;
                }
                if (theEntry.mComputeChecksumFlag) {
                    theEntry.mBlocksChecksums = ComputeChecksums(
                        &theBuf,
                        theEntry.mReceiveByteCount,
                        &theEntry.mChecksum,
                        theEntry.mFirstChecksumBlockLen
                    );
                }
            }
        }
        StMutexLocker theLocker(*this);
        ClientThreadListEntry::HandleRequest(inClient, inCode, inDataPtr);
        theLocker.Unlock();
        GetConnection(inClient)->StartFlush();
        return true;
    }
    void Granted(
        ClientSM& inClient)
    {
        QCASSERT( GetMutex().IsOwned());
        ClientThreadListEntry& theEntry = inClient;
        if (theEntry.mGrantedFlag) {
            return;
        }
        theEntry.mGrantedFlag = true;
        if (! theEntry.mOpsHeadPtr &&
                Enqueue(inClient, mRunQueueHeadPtr, mRunQueueTailPtr)) {
            Wakeup();
        }
    }
    NetManager& GetNetManager()
        { return mNetManager; }
    void Enqueue(
        RemoteSyncSM& inSyncSM,
        KfsOp&        inOp)
    {
        ClientThreadRemoteSyncListEntry& theEntry = inSyncSM;
        if (GetCurrentClientThreadPtr() == &mOuter && ! theEntry.IsPending()) {
            ClientThreadRemoteSyncListEntry::Enqueue(inSyncSM, inOp);
            return;
        }
        if (AddPending(inOp, inSyncSM) &&
                Enqueue(inSyncSM, mSyncQueueHeadPtr, mSyncQueueTailPtr)) {
            Wakeup();
        }
    }
    void Finish(
        RemoteSyncSM& inSyncSM)
    {
        ClientThreadRemoteSyncListEntry& theEntry = inSyncSM;
        if (GetCurrentClientThreadPtr() == &mOuter && ! theEntry.IsPending()) {
            ClientThreadRemoteSyncListEntry::Finish(inSyncSM);
            return;
        }
        if (inSyncSM.mFinishFlag) {
            return;
        }
        inSyncSM.mFinishFlag = true;
        if (! theEntry.mOpsHeadPtr &&
                Enqueue(inSyncSM, mSyncQueueHeadPtr, mSyncQueueTailPtr)) {
            Wakeup();
        }
    }
    static ClientThread* GetCurrentClientThreadPtr()
    {
        QCASSERT(GetMutex().IsOwned());
        return sCurrentClientThreadPtr;
    }
    static QCMutex& GetMutex()
    {
        static QCMutex sMutex;
        return sMutex;
    }
private:
    typedef vector<ClientSM*>     TmpDispatchQueue;
    typedef vector<RemoteSyncSM*> TmpSyncSMQueue;

    QCThread         mThread;
    bool             mRunFlag;
    NetManager       mNetManager;
    ClientSM*        mAddQueueTailPtr;
    ClientSM*        mAddQueueHeadPtr;
    ClientSM*        mRunQueueTailPtr;
    ClientSM*        mRunQueueHeadPtr;
    RemoteSyncSM*    mSyncQueueTailPtr;
    RemoteSyncSM*    mSyncQueueHeadPtr;
    TmpDispatchQueue mTmpDispatchQueue;
    TmpSyncSMQueue   mTmpSyncSMQueue;
    volatile int     mWakeupCnt;
    ClientThread&    mOuter;
    char             mParseBuffer[MAX_RPC_HEADER_LEN];

    static ClientThread* sCurrentClientThreadPtr;

    void Wakeup()
    {
        mNetManager.Wakeup();
        SyncAddAndFetch(mWakeupCnt, 1);
    }
    TmpDispatchQueue& GetTmpDispatchQueue()
        { return mTmpDispatchQueue; }
    static void RunPending(
        ClientSM& inClient)
    {
        ClientThreadListEntry& theEntry = inClient;
        const bool theGrantedFlag = theEntry.mGrantedFlag;
        KfsOp*     thePtr         = theEntry.mOpsHeadPtr;
        theEntry.mOpsHeadPtr  = 0;
        theEntry.mOpsTailPtr  = 0;
        theEntry.mGrantedFlag = false;
        while (thePtr) {
            KfsOp& theCur = *thePtr;
            thePtr = GetNextPtr(theCur);
            GetNextPtr(theCur) = 0;
            ClientThreadListEntry::HandleRequest(
                inClient, EVENT_CMD_DONE, &theCur);
        }
        if (theGrantedFlag) {
            ClientThreadListEntry::HandleGranted(inClient);
        }
    }
    static void RunPending(
        RemoteSyncSM& inSyncSM)
    {
        ClientThreadRemoteSyncListEntry& theEntry = inSyncSM;
        const bool theFinishFlag = theEntry.mFinishFlag;
        KfsOp*     thePtr        = theEntry.mOpsHeadPtr;
        theEntry.mOpsHeadPtr = 0;
        theEntry.mOpsTailPtr = 0;
        theEntry.mFinishFlag = false;
        bool theOkFlag = false;
        while (thePtr) {
            KfsOp& theCur = *thePtr;
            thePtr = GetNextPtr(theCur);
            GetNextPtr(theCur) = 0;
            if (! theOkFlag) {
                theCur.status = -EHOSTUNREACH;
                SubmitOpResponse(&theCur);
                continue;
            }
            theOkFlag = ClientThreadRemoteSyncListEntry::Enqueue(
                inSyncSM, theCur);
        }
        if (theFinishFlag) {
            ClientThreadRemoteSyncListEntry::Finish(inSyncSM);
        }
    }
    static RemoteSyncSM*& GetNextPtr(
        RemoteSyncSM& inSyncSM)
    {
        ClientThreadRemoteSyncListEntry& theEntry = inSyncSM;
        return theEntry.mNextPtr;
    }
    static ClientSM*& GetNextPtr(
        ClientSM& inClient)
    {
        ClientThreadListEntry& theEntry = inClient;
        return theEntry.mNextPtr;
    }
    static KfsOp*& GetNextPtr(
        KfsOp& inOp)
        { return inOp.nextOp.mNextPtr; }
    template<typename T>
    static bool Enqueue(
        T&  inEntry,
        T*& inQueueHeadPtr,
        T*& inQueueTailPtr)
    {
        QCASSERT(! GetNextPtr(inEntry));
        const bool theWasEmptyFlag = ! inQueueTailPtr;
        if (inQueueTailPtr) {
            GetNextPtr(*inQueueTailPtr) = &inEntry;
        } else {
            QCASSERT(! inQueueHeadPtr);
            inQueueHeadPtr = &inEntry;
        }
        inQueueTailPtr = &inEntry;
        return theWasEmptyFlag;
    }
    static bool AddPending(
        KfsOp&    inOp,
        ClientSM& inClient)
    {
        ClientThreadListEntry& theEntry = inClient;
        return (
            Enqueue(inOp, theEntry.mOpsHeadPtr, theEntry.mOpsTailPtr) &&
            ! theEntry.mGrantedFlag
        );
    }
    static bool AddPending(
        KfsOp&        inOp,
        RemoteSyncSM& inSyncSM)
    {
        ClientThreadRemoteSyncListEntry& theEntry = inSyncSM;
        return (
            Enqueue(inOp, theEntry.mOpsHeadPtr, theEntry.mOpsTailPtr) &&
            ! theEntry.mFinishFlag
        );
    }
    static const NetConnectionPtr& GetConnection(
        const ClientSM& inClient)
    {
        return ClientThreadListEntry::GetConnection(inClient);
    }
};

ClientThread* ClientThread::Impl::sCurrentClientThreadPtr = 0;
int           ClientThread::Impl::StMutexLocker::sLockCnt = 0;

ClientThread::ClientThread()
    : mImpl(*(new Impl(*this)))
    {}

ClientThread::~ClientThread()
{
    delete &mImpl;
}

    void
ClientThread::Start()
{
    mImpl.Start();
}

    void
ClientThread::Stop()
{
    mImpl.Stop();
}

    void
ClientThread::Add(
    ClientSM& inClient)
{
    mImpl.Add(inClient);
}

    bool
ClientThread::Handle(
    ClientSM& inClient,
    int       inCode,
    void*     inDataPtr)
{
    return mImpl.Handle(inClient, inCode, inDataPtr);
}

    void
ClientThread::Granted(
    ClientSM& inClient)
{
    mImpl.Granted(inClient);
}

    void
ClientThread::Enqueue(
    RemoteSyncSM& inSyncSM,
    KfsOp&        inOp)
{
    mImpl.Enqueue(inSyncSM, inOp);
}

    void
ClientThread::Finish(
    RemoteSyncSM& inSyncSM)
{
    mImpl.Finish(inSyncSM);
}

    NetManager&
ClientThread::GetNetManager()
{
    return mImpl.GetNetManager();
}

    /* static */ QCMutex&
ClientThread::GetMutex()
{
    return Impl::GetMutex();
}

    /* static */ ClientThread*
ClientThread::GetCurrentClientThreadPtr()
{
    return Impl::GetCurrentClientThreadPtr();
}

}
