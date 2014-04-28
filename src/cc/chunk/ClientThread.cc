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
#include "common/kfsatomic.h"

namespace KFS
{

class ClientThread::Impl : public QCRunnable, public ITimeout
{
public:
    typedef ClientThread Outer;

    Impl()
        : QCRunnable(),
          mThread(),
          mNetManager(),
          mAddQueueTailPtr(0),
          mAddQueueHeadPtr(0),
          mRunQueueTailPtr(0),
          mRunQueueHeadPtr(0),
          mRunQueuesCnt(0)
    {
        QCASSERT( GetMutex().IsOwned());
    }
    ~Impl()
        { Impl::Stop(); } 
    void Add(
        ClientSM& inClient)
    {
        QCASSERT( GetMutex().IsOwned());
        if (Enqueue(inClient, mAddQueueHeadPtr, mAddQueueTailPtr)) {
            mNetManager.Wakeup();
        }
        SyncAddAndFetch(mRunQueuesCnt, 1);
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
        mNetManager.Wakeup();
        SyncAddAndFetch(mRunQueuesCnt, 1);

        QCStMutexUnlocker theUnlocker(GetMutex());
        mThread.Join();
    }
    virtual void Timeout()
    {
        if (SyncAddAndFetch(mRunQueuesCnt, 0) <= 0) {
            return;
        }
        QCASSERT( ! GetMutex().IsOwned());
        QCStMutexLocker theLocker(GetMutex());

        mRunQueuesCnt = 0;
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
        while (thePtr) {
            ClientSM& theCur = *thePtr;
            thePtr = GetNextPtr(theCur);
            GetNextPtr(theCur) = 0;
            RunPending(theCur);
        }
    }
    bool Handle(
        ClientSM& inClient,
        int       inCode,
        void*     inDataPtr)
    {
        if (inCode == EVENT_CMD_DONE) {
            QCASSERT(GetMutex().IsOwned());
            QCASSERT(inDataPtr);
            if (AddPending(*reinterpret_cast<KfsOp*>(inDataPtr), inClient) &&
                    Enqueue(inClient, mRunQueueHeadPtr, mRunQueueTailPtr)) {
                mNetManager.Wakeup();
            }
            return true;
        }
        QCASSERT(! GetMutex().IsOwned());
        ClientThreadListEntry& theEntry = inClient;
        if (inCode == EVENT_NET_READ) {
            QCASSERT(inDataPtr);
            IOBuffer& theBuf = *reinterpret_cast<IOBuffer*>(inDataPtr);
            if (theEntry.mReceiveOpFlag) {
                // Implement op parsing.
            } else if (theEntry.mReceiveByteCount > 0) {
                if (theBuf.BytesConsumable() < theEntry.mReceiveByteCount) {
                    return true;
                }
                if (theEntry.mComputeChecksumFlag) {
                    // Implement checksum computation.
                }
            }
        }
        QCStMutexLocker theLocker(GetMutex());
        inClient.HandleRequestSelf(inCode, inDataPtr);
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
            mNetManager.Wakeup();
        }
    }
    static QCMutex& GetMutex()
    {
        static QCMutex sMutex;
        return sMutex;
    }
private:
    QCThread     mThread;
    bool         mRunFlag;
    NetManager   mNetManager;
    ClientSM*    mAddQueueTailPtr;
    ClientSM*    mAddQueueHeadPtr;
    ClientSM*    mRunQueueTailPtr;
    ClientSM*    mRunQueueHeadPtr;
    volatile int mRunQueuesCnt;

    void RunPending(
        ClientSM& inClient)
    {
        ClientThreadListEntry& theEntry = inClient;
        if (theEntry.mGrantedFlag) {
            theEntry.mGrantedFlag = false;
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

}
