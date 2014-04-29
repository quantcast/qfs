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

#include "qcdio/QCThread.h"
#include "qcdio/QCMutex.h"
#include "qcdio/QCStUtils.h"
#include "qcdio/QCDebug.h"

#include "kfsio/NetManager.h"
#include "kfsio/ITimeout.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/Globals.h"
#include "kfsio/Checksum.h"
#include "common/kfsatomic.h"

namespace KFS
{
using KFS::libkfsio::globalNetManager;

ClientThreadListEntry::~ClientThreadListEntry()
{
    QCASSERT(! mOpsHeadPtr && ! mOpsTailPtr && ! mNextPtr && ! mGrantedFlag);
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
                (! Impl::sCurrentNetManagerPtr && sLockCnt == 0) ||
                (&inImpl.mNetManager == sCurrentNetManagerPtr && 0 < sLockCnt)
            );
            if (sLockCnt++ == 0) {
                Impl::sCurrentNetManagerPtr = &inImpl.mNetManager;
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
                Impl::sCurrentNetManagerPtr = 0;
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

    Impl()
        : QCRunnable(),
          mThread(),
          mNetManager(),
          mAddQueueTailPtr(0),
          mAddQueueHeadPtr(0),
          mRunQueueTailPtr(0),
          mRunQueueHeadPtr(0),
          mTmpDispatchQueue(),
          mWakeupCnt(0)
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
            const NetConnectionPtr& theConnPtr = theCur.GetConnection();
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
        for (TmpDispatchQueue::const_iterator theIt = theQueue.begin();
                theIt != theQueue.end();
                ++theIt) {
            RunPending(**theIt);
        }
        theLocker.Unlock();
        for (TmpDispatchQueue::const_iterator theIt = theQueue.begin();
                theIt != theQueue.end();
                ++theIt) {
            (**theIt).GetConnection()->StartFlush();
        }
    }
    bool Handle(
        ClientSM& inClient,
        int       inCode,
        void*     inDataPtr)
    {
        if (inCode == EVENT_CMD_DONE) {
            if (&GetCurrentNetManager() == &mNetManager) {
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
        inClient.HandleRequestSelf(inCode, inDataPtr);
        theLocker.Unlock();
        inClient.GetConnection()->StartFlush();
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
        if (Enqueue(inClient, mRunQueueHeadPtr, mRunQueueTailPtr)) {
            Wakeup();
        }
    }
    static NetManager& GetCurrentNetManager()
    {
        QCASSERT(GetMutex().IsOwned());
        return (sCurrentNetManagerPtr ?
            *sCurrentNetManagerPtr : globalNetManager());
    }
    static QCMutex& GetMutex()
    {
        static QCMutex sMutex;
        return sMutex;
    }
private:
    typedef vector<ClientSM*> TmpDispatchQueue;

    QCThread         mThread;
    bool             mRunFlag;
    NetManager       mNetManager;
    ClientSM*        mAddQueueTailPtr;
    ClientSM*        mAddQueueHeadPtr;
    ClientSM*        mRunQueueTailPtr;
    ClientSM*        mRunQueueHeadPtr;
    TmpDispatchQueue mTmpDispatchQueue;
    volatile int     mWakeupCnt;
    char             mParseBuffer[MAX_RPC_HEADER_LEN];

    static NetManager* sCurrentNetManagerPtr;

    void Wakeup()
    {
        mNetManager.Wakeup();
        SyncAddAndFetch(mWakeupCnt, 1);
    }
    TmpDispatchQueue& GetTmpDispatchQueue()
        { return mTmpDispatchQueue; }
    void RunPending(
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
            inClient.HandleRequestSelf(EVENT_CMD_DONE, &theCur);
        }
        if (theGrantedFlag) {
            inClient.HandleGranted();
        }
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
};

NetManager* ClientThread::Impl::sCurrentNetManagerPtr   = 0;
int         ClientThread::Impl::StMutexLocker::sLockCnt = 0;

ClientThread::ClientThread()
    : mImpl(*(new Impl()))
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

    /* static */ QCMutex&
ClientThread::GetMutex()
{
    return Impl::GetMutex();
}

    /* static */ NetManager&
ClientThread::GetCurrentNetManager()
{
    return Impl::GetCurrentNetManager();
}

}
