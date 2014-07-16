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
#include "RemoteSyncSM.h"
#include "Replicator.h"

#include "common/kfsatomic.h"

#include "qcdio/QCThread.h"
#include "qcdio/QCMutex.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"
#include "qcdio/qcdebug.h"

#include "kfsio/NetManager.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/Globals.h"
#include "kfsio/checksum.h"

namespace KFS
{

class ClientThreadImpl : public QCRunnable, public NetManager::Dispatcher
{
public:
    typedef ClientThread Outer;

    static void Lock(
        Outer& inThread)
    {
        GetMutex().Lock();
        QCASSERT(
            (! sCurrentClientThreadPtr && sLockCnt == 0) ||
            (&inThread == sCurrentClientThreadPtr && 0 < sLockCnt)
        );
        if ((0 != sLockCnt ? &inThread : 0) != sCurrentClientThreadPtr) {
            die("lock: invalid client thread lock state");
            return;
        }
        if (sLockCnt++ == 0) {
            sCurrentClientThreadPtr = &inThread;
        }
    }
    static void Unlock(
        Outer& inThread)
    {
        if (sLockCnt <= 0 || &inThread != sCurrentClientThreadPtr) {
            die("unlock: invalid client thread lock state");
            return;
        }
        if (--sLockCnt == 0) {
            sCurrentClientThreadPtr = 0;
        }
        GetMutex().Unlock();
    }

    class StMutexLocker
    {
    public:
        StMutexLocker(
            Outer& inThread)
            : mThreadPtr(&inThread)
            { ClientThreadImpl::Lock(inThread); }
        ~StMutexLocker()
            { StMutexLocker::Unlock(); }
        void Unlock()
        {
            if (mThreadPtr) {
                ClientThreadImpl::Unlock(*mThreadPtr);
                mThreadPtr = 0;
            }
        }
    private:
        Outer* mThreadPtr;
    private:
        StMutexLocker(
            const StMutexLocker& inLocker);
        StMutexLocker& operator=(
            const StMutexLocker& inLocker);
    };

    ClientThreadImpl(
        ClientThread& inOuter)
        : QCRunnable(),
          Dispatcher(),
          mThread(),
          mRunFlag(false),
          mShutdownFlag(false),
          mNetManager(),
          mAddQueueTailPtr(0),
          mAddQueueHeadPtr(0),
          mRunQueueTailPtr(0),
          mRunQueueHeadPtr(0),
          mSyncQueueTailPtr(0),
          mSyncQueueHeadPtr(0),
          mRSReplicatorQueueHeadPtr(0),
          mRSReplicatorQueueTailPtr(0),
          mTmpDispatchQueue(),
          mWakeupCnt(0),
          mOuter(inOuter)
    {
        QCASSERT(GetMutex().IsOwned());
        mTmpDispatchQueue.reserve(1 << 10);
        mTmpSyncSMQueue.reserve(1 << 10);
    }
    ~ClientThreadImpl()
        { ClientThreadImpl::Stop(); }
    void Add(
        ClientSM& inClient)
    {
        QCASSERT(GetMutex().IsOwned());
        if (Enqueue(inClient, mAddQueueHeadPtr, mAddQueueTailPtr)) {
            Wakeup();
        }
    }
    void Enqueue(
        RSReplicatorEntry& inEntry)
    {
        QCASSERT(GetMutex().IsOwned());
        if (GetNextPtr(inEntry)) {
            // Already in the queue, possible state change.
            return;
        }
        if (Enqueue(inEntry,
                mRSReplicatorQueueHeadPtr, mRSReplicatorQueueTailPtr)) {
            Wakeup();
        }
    }
    virtual void Run()
    {
        QCMutex* const kNullMutexPtr         = 0;
        bool     const kWakeupAndCleanupFlag = true;
        mNetManager.MainLoop(kNullMutexPtr, kWakeupAndCleanupFlag, this);
    }
    bool IsStarted() const
        { return mThread.IsStarted(); }
    void Start()
    {
        QCASSERT(GetMutex().IsOwned());
        if (! IsStarted()) {
            mShutdownFlag = false;
            mRunFlag      = true;
            const int kStackSize = 32 << 10;
            mThread.Start(this, kStackSize, "ClientThread");
        }
    }
    void Stop()
    {
        QCASSERT(GetMutex().IsOwned());
        if (! mRunFlag) {
            return;
        }
        mRunFlag = false;
        Wakeup();

        QCStMutexUnlocker theUnlocker(GetMutex());
        mThread.Join();
    }
    virtual void DispatchEnd()
        {}
    virtual void DispatchExit()
        { mShutdownFlag = true; }
    virtual void DispatchStart()
    {
        if (SyncAddAndFetch(mWakeupCnt, 0) <= 0) {
            return;
        }
        QCASSERT( ! GetMutex().IsOwned());
        StMutexLocker theLocker(mOuter);
        QCASSERT(! mShutdownFlag || ! mRunFlag);

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
            if (mShutdownFlag) {
                theConnPtr->HandleErrorEvent();
            } else {
                theConnPtr->SetOwningKfsCallbackObj(&theCur);
                mNetManager.AddConnection(theConnPtr);
            }
        }
        if (! mRunFlag && ! mShutdownFlag) {
            mNetManager.Shutdown();
        }
        thePtr = mRunQueueHeadPtr;
        mRunQueueTailPtr = 0;
        mRunQueueHeadPtr = 0;
        mTmpDispatchQueue.clear();
        while (thePtr) {
            ClientSM& theCur = *thePtr;
            thePtr = GetNextPtr(theCur);
            if (thePtr == &theCur) {
                thePtr = 0;
            }
            GetNextPtr(theCur) = 0;
            mTmpDispatchQueue.push_back(&theCur);
        }
        RemoteSyncSM* theSyncPtr = mSyncQueueHeadPtr;
        mSyncQueueHeadPtr = 0;
        mSyncQueueTailPtr = 0;
        mTmpSyncSMQueue.clear();
        while (theSyncPtr) {
            RemoteSyncSM& theCur = *theSyncPtr;
            theSyncPtr = GetNextPtr(theCur);
            if (theSyncPtr == &theCur) {
                theSyncPtr = 0;
            }
            GetNextPtr(theCur) = 0;
            mTmpSyncSMQueue.push_back(&theCur);
        }
        for (TmpDispatchQueue::iterator theIt = mTmpDispatchQueue.begin();
                theIt != mTmpDispatchQueue.end();
                ++theIt) {
            if (! RunPending(**theIt)) {
                *theIt = 0;
            }
        }
        for (TmpSyncSMQueue::const_iterator theIt = mTmpSyncSMQueue.begin();
                theIt != mTmpSyncSMQueue.end();
                ++theIt) {
            RunPending(**theIt);
        }
        RSReplicatorEntry* theNextPtr = mRSReplicatorQueueHeadPtr;
        mRSReplicatorQueueHeadPtr = 0;
        mRSReplicatorQueueTailPtr = 0;
        theLocker.Unlock();
        for (TmpDispatchQueue::const_iterator theIt = mTmpDispatchQueue.begin();
                theIt != mTmpDispatchQueue.end();
                ++theIt) {
            if (*theIt) {
                GetConnection(**theIt)->StartFlush();
            }
        }
        while (theNextPtr) {
            RSReplicatorEntry& theCur = *theNextPtr;
            theNextPtr = GetNextPtr(theCur);
            if (&theCur == theNextPtr) {
                theNextPtr = 0;
            }
            GetNextPtr(theCur) = 0;
            theCur.Handle();
        }
    }
    int Handle(
        ClientSM& inClient,
        int       inCode,
        void*     inDataPtr)
    {
        if (inCode == EVENT_CMD_DONE) {
            if (GetCurrentClientThreadPtr() == &mOuter) {
                const bool theFlushFlag =
                    ! GetConnection(inClient)->IsWriteReady();
                const int theRet = ClientThreadListEntry::HandleRequest(
                    inClient, inCode, inDataPtr);
                if (theFlushFlag && theRet == 0) {
                    GetConnection(inClient)->Flush();
                }
                return theRet;
            }
            QCASSERT(inDataPtr);
            if (AddPending(*reinterpret_cast<KfsOp*>(inDataPtr), inClient) &&
                    Enqueue(inClient, mRunQueueHeadPtr, mRunQueueTailPtr)) {
                Wakeup();
            }
            return 0;
        }
        QCASSERT(! GetMutex().IsOwned());
        ClientThreadListEntry& theEntry = inClient;
        if (inCode == EVENT_NET_READ) {
            QCASSERT(inDataPtr);
            IOBuffer& theBuf = *reinterpret_cast<IOBuffer*>(inDataPtr);
            if (theEntry.mReceiveOpFlag) {
                theEntry.mReceivedHeaderLen = 0;
                if (! IsMsgAvail(&theBuf, &theEntry.mReceivedHeaderLen)) {
                    return 0;
                }
                theEntry.mReceivedOpPtr = 0;
                if (ParseClientCommand(
                        theBuf,
                        theEntry.mReceivedHeaderLen,
                        &theEntry.mReceivedOpPtr,
                        mParseBuffer) != 0) {
                    theEntry.ReceiveClear();
                }
            } else if (0 <= theEntry.mReceiveByteCount) {
                if (theBuf.BytesConsumable() < theEntry.mReceiveByteCount) {
                    return 0;
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
        StMutexLocker theLocker(mOuter);
        const int theRet = ClientThreadListEntry::HandleRequest(
            inClient, inCode, inDataPtr);
        theLocker.Unlock();
        if (theRet == 0) {
            GetConnection(inClient)->StartFlush();
        }
        return theRet;
    }
    void Granted(
        ClientSM& inClient)
    {
        QCASSERT(GetMutex().IsOwned());
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
        ClientThreadRemoteSyncListEntry::RemoveFromList(inSyncSM);
        if (inSyncSM.mFinishFlag) {
            return;
        }
        inSyncSM.mFinishFlag = true;
        if (! theEntry.mOpsHeadPtr &&
                Enqueue(inSyncSM, mSyncQueueHeadPtr, mSyncQueueTailPtr)) {
            Wakeup();
        }
    }
    bool IsWorkPending() const
    {
        return (0 < mWakeupCnt);
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
    static ClientThreadImpl& GetImpl(
        ClientThread& inThread)
        { return inThread.mImpl; }
private:
    typedef vector<ClientSM*>     TmpDispatchQueue;
    typedef vector<RemoteSyncSM*> TmpSyncSMQueue;

    QCThread           mThread;
    bool               mRunFlag;
    bool               mShutdownFlag;
    NetManager         mNetManager;
    ClientSM*          mAddQueueTailPtr;
    ClientSM*          mAddQueueHeadPtr;
    ClientSM*          mRunQueueTailPtr;
    ClientSM*          mRunQueueHeadPtr;
    RemoteSyncSM*      mSyncQueueTailPtr;
    RemoteSyncSM*      mSyncQueueHeadPtr;
    RSReplicatorEntry* mRSReplicatorQueueHeadPtr;
    RSReplicatorEntry* mRSReplicatorQueueTailPtr;
    TmpDispatchQueue   mTmpDispatchQueue;
    TmpSyncSMQueue     mTmpSyncSMQueue;
    volatile int       mWakeupCnt;
    ClientThread&      mOuter;
    char               mParseBuffer[MAX_RPC_HEADER_LEN];

    static ClientThread* sCurrentClientThreadPtr;
    static int           sLockCnt;

    void Wakeup()
    {
        if (SyncAddAndFetch(mWakeupCnt, 1) <= 1) {
            mNetManager.Wakeup();
        }
    }
    static bool RunPending(
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
            if (&theCur == thePtr) {
                thePtr = 0;
            }
            if (ClientThreadListEntry::HandleRequest(
                    inClient, EVENT_CMD_DONE, &theCur) != 0) {
                QCRTASSERT(! thePtr);
                return false; // Deleted.
            }
        }
        return (! theGrantedFlag ||
            ClientThreadListEntry::HandleGranted(inClient) == 0);
    }
    static void RunPending(
        RemoteSyncSM& inSyncSM)
    {
        ClientThreadRemoteSyncListEntry& theEntry      = inSyncSM;
        KfsOp*                           thePtr        = theEntry.mOpsHeadPtr;
        const bool                       theFinishFlag = theEntry.mFinishFlag;
        theEntry.mOpsHeadPtr = 0;
        theEntry.mOpsTailPtr = 0;
        theEntry.mFinishFlag = false;
        if (theFinishFlag) {
            // Call Finish first, to fail pending ops, to maintain Enqueue()
            // order.
            ClientThreadRemoteSyncListEntry::Finish(inSyncSM);
            // theEntry and inSyncSM might be deleted at this point.
        }
        bool theOkFlag = ! theFinishFlag;
        while (thePtr) {
            KfsOp& theCur = *thePtr;
            thePtr = GetNextPtr(theCur);
            GetNextPtr(theCur) = 0;
            if (&theCur == thePtr) {
                thePtr = 0;
            }
            if (theOkFlag) {
                theOkFlag = ClientThreadRemoteSyncListEntry::Enqueue(
                    inSyncSM, theCur);
            } else {
                theCur.status = -EHOSTUNREACH;
                SubmitOpResponse(&theCur);
            }
        }
    }
    static RemoteSyncSM*& GetNextPtr(
        ClientThreadRemoteSyncListEntry& inEntry)
        { return inEntry.mNextPtr; }
    static ClientSM*& GetNextPtr(
        ClientThreadListEntry& inEntry)
        { return inEntry.mNextPtr; }
    static RSReplicatorEntry*& GetNextPtr(
        RSReplicatorEntry& inEntry)
        { return inEntry.mNextPtr; }
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
        GetNextPtr(inEntry) = &inEntry;
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
private:
    ClientThreadImpl(
        const ClientThreadImpl& inImpl);
    ClientThreadImpl& operator=(
        const ClientThreadImpl& inImpl);
};

ClientThread* ClientThreadImpl::sCurrentClientThreadPtr = 0;
int           ClientThreadImpl::sLockCnt                = 0;

ClientThreadListEntry::~ClientThreadListEntry()
{
    if (mOpsHeadPtr || mOpsTailPtr || mNextPtr || mGrantedFlag) {
        die("invalid client thread list entry destructor invocation");
    }
    mNextPtr = static_cast<ClientSM*>(this); // To catch double delete.
}

    int
ClientThreadListEntry::DispatchEvent(
    ClientSM& inClient,
    int       inCode,
    void*     inDataPtr)
{
    return ClientThreadImpl::GetImpl(*mClientThreadPtr).Handle(
        inClient, inCode, inDataPtr);
}

    void
ClientThreadListEntry::DispatchGranted(
    ClientSM& inClient)
{
    ClientThreadImpl::GetImpl(*mClientThreadPtr).Granted(inClient);
}

ClientThreadRemoteSyncListEntry::~ClientThreadRemoteSyncListEntry()
{
    if (mOpsHeadPtr || mOpsTailPtr || mNextPtr || mFinishFlag) {
        die("invalid remote sync list entry destructor invocation");
    }
    mNextPtr = static_cast<RemoteSyncSM*>(this); // To catch double delete.
}

    void
ClientThreadRemoteSyncListEntry::DispatchEnqueue(
    RemoteSyncSM& inSyncSM,
    KfsOp&        inOp)
{
    ClientThreadImpl::GetImpl(*mClientThreadPtr).Enqueue(inSyncSM, inOp);
}

    void
ClientThreadRemoteSyncListEntry::DispatchFinish(
    RemoteSyncSM& inSyncSM)
{
    ClientThreadImpl::GetImpl(*mClientThreadPtr).Finish(inSyncSM);
}

RSReplicatorEntry::~RSReplicatorEntry()
{
    if (mNextPtr) {
        die("invalid rs replicator list entry destructor invocation");
    }
    mNextPtr = this; // To catch double delete.
}

    void
RSReplicatorEntry::Enqueue()
{
    ClientThreadImpl::GetImpl(*mClientThreadPtr).Enqueue(*this);
}

ClientThread::ClientThread()
    : mImpl(*(new ClientThreadImpl(*this)))
    {}

ClientThread::~ClientThread()
{
    delete &mImpl;
}

    void
ClientThread::Lock()
{
    ClientThreadImpl::Lock(*this);
}

    void
ClientThread::Unlock()
{
    ClientThreadImpl::Unlock(*this);
}

    void
ClientThread::Add(
    ClientSM& inClient)
{
    mImpl.Add(inClient);
}

    NetManager&
ClientThread::GetNetManager()
{
    return mImpl.GetNetManager();
}

    /* static */ const QCMutex&
ClientThread::GetMutex()
{
    return ClientThreadImpl::GetMutex();
}

    /* static */ ClientThread*
ClientThread::GetCurrentClientThreadPtr()
{
    return ClientThreadImpl::GetCurrentClientThreadPtr();
}

    /* static */ ClientThread*
ClientThread::CreateThreads(
    int       inThreadCount,
    QCMutex*& outMutexPtr)
{
    if (inThreadCount <= 0) {
        outMutexPtr = 0;
        return 0;
    }
    outMutexPtr = &ClientThreadImpl::GetMutex();
    QCStMutexLocker theLocker(outMutexPtr);
    ClientThread* const theThreadsPtr = new ClientThread[inThreadCount];
    for (int i = 0; i < inThreadCount; i++) {
        theThreadsPtr[i].mImpl.Start();
    }
    return theThreadsPtr;
}

    /* static */ void
ClientThread::Stop(
    ClientThread* inThreadsPtr,
    int           inThreadCount)
{
    if (inThreadCount <= 0) {
        return;
    }
    for (int i = 0; i < inThreadCount; i++) {
        inThreadsPtr[i].mImpl.Stop();
    }
    // Run dispatch to empty all pending queues.
    QCStMutexUnlocker theUnlocker(ClientThreadImpl::GetMutex());
    const int kMaxTries = 1 << 10;
    int k;
    for (k = 0; k < kMaxTries; k++) {
        for (int i = 0; i < inThreadCount; i++) {
            inThreadsPtr[i].mImpl.DispatchStart();
        }
        int i;
        for (i = 0; i < inThreadCount; i++) {
            if (inThreadsPtr[i].mImpl.IsWorkPending()) {
                break;
            }
        }
        if (inThreadCount <= i) {
            break;
        }
        KFS_LOG_STREAM_DEBUG <<
            "client threads: work pending attempt: " << k <<
        KFS_LOG_EOM;
    }
    if (kMaxTries <= k) {
        KFS_LOG_STREAM_ERROR <<
            "client threads: work still pending after running" <<
        KFS_LOG_EOM;
    }
}

}
