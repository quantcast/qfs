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

#include <sstream>

namespace KFS
{
using std::ostringstream;

    inline int
ClientThreadListEntry::HandleRequest(
    ClientSM& inClient,
    int       inCode,
    void*     inDataPtr,
    bool&     outRecursionFlag)
{
    outRecursionFlag = 0 < inClient.mRecursionCnt;
    return inClient.HandleRequest(inCode, inDataPtr);
}

    inline int
ClientThreadListEntry::HandleGranted(
    ClientSM& inClient)
{
    return inClient.HandleGranted();
}

    inline const NetConnectionPtr&
ClientThreadListEntry::GetConnection(
    const ClientSM& inClient)
{
    return inClient.mNetConnection;
}

    inline ClientSM&
ClientThreadListEntry::GetClient()
{
    return *static_cast<ClientSM*>(this);
}

    inline bool
ClientThreadRemoteSyncListEntry::Enqueue(
    RemoteSyncSM& inSyncSM,
    KfsOp&        inOp)
{
    return inSyncSM.EnqueueSelf(&inOp);
}

    inline void
ClientThreadRemoteSyncListEntry::Finish(
    RemoteSyncSM& inSyncSM)
{
    inSyncSM.FinishSelf();
}

    inline bool
ClientThreadRemoteSyncListEntry::RemoveFromList(
    RemoteSyncSM& inSyncSM)
{
    return inSyncSM.RemoveFromList();
}

class ClientThreadImpl : public QCRunnable, public NetManager::Dispatcher
{
public:
    typedef ClientThread Outer;

    static void Lock(
        Outer& inThread)
    {
        GetMutex().Lock();
        if ((0 != sLockCnt ? &inThread : 0) != sCurrentClientThreadPtr) {
            die("client thread lock: invalid client thread lock state");
        }
        if (sLockCnt++ == 0) {
            if (! inThread.mImpl.mShutdownFlag &&
                    ! inThread.mImpl.mThread.IsCurrentThread()) {
                die("client thread lock: client thread is not current thread");
            }
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
            if (! inThread.mImpl.mShutdownFlag &&
                    ! inThread.mImpl.mThread.IsCurrentThread()) {
                die("client thread unlock:"
                    "client thread is not current thread");
            }
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
          mSyncQueueHeadPtr(0),
          mRSReplicatorQueueHeadPtr(0),
          mTmpDispatchQueue(),
          mTmpSyncSMQueue(),
          mTmpRSReplicatorQueue(),
          mWakeupCnt(0),
          mOuter(inOuter)
    {
        QCASSERT(GetMutex().IsOwned());
        DispatchQueue::Init(mDispatchQueuePtr);
        DispatchQueue::Init(mAddQueuePtr);
        mTmpDispatchQueue.reserve(2 << 10);
        mTmpSyncSMQueue.reserve(2 << 10);
        mTmpRSReplicatorQueue.reserve(1 << 8);
    }
    ~ClientThreadImpl()
    {
        if (IsStarted()) {
            ClientThreadImpl::Stop();
        }
    }
    void Add(
        ClientSM& inClient)
    {
        QCASSERT(
            GetMutex().IsOwned() &&
            ! DispatchQueue::IsInList(mAddQueuePtr,      inClient) &&
            ! DispatchQueue::IsInList(mDispatchQueuePtr, inClient)
        );
        const bool theWakeupFlag = DispatchQueue::IsEmpty(mAddQueuePtr);
        DispatchQueue::PushBack(mAddQueuePtr, inClient);
        if (theWakeupFlag) {
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
        if (Enqueue(inEntry, mRSReplicatorQueueHeadPtr)) {
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
    void Start(
        int inCpuIndex)
    {
        QCASSERT(GetMutex().IsOwned());
        if (! IsStarted()) {
            mShutdownFlag = false;
            mRunFlag      = true;
            const int kStackSize = 256 << 10;
            mThread.Start(
                this,
                kStackSize,
                "ClientThread",
                inCpuIndex < 0 ?
                    QCThread::CpuAffinity::None() :
                    QCThread::CpuAffinity(inCpuIndex)
            );
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
        QCASSERT(! GetMutex().IsOwned());
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
        ClientThreadListEntry* theAddQueuePtr[kDispatchQueueCount];
        DispatchQueue::Init(theAddQueuePtr);
        DispatchQueue::PushBackList(theAddQueuePtr, mAddQueuePtr);
        ClientThreadListEntry* thePtr;
        int theCnt = 0;
        while ((thePtr = DispatchQueue::PopFront(theAddQueuePtr))) {
            CheckQueueSize(++theCnt, "add");
            ClientSM& theCur = thePtr->GetClient();
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
        mTmpDispatchQueue.clear();
        theCnt = 0;
        while ((thePtr = DispatchQueue::PopBack(mDispatchQueuePtr))) {
            CheckQueueSize(++theCnt, "sync");
            mTmpDispatchQueue.push_back(&thePtr->GetClient());
        }
        RemoteSyncSM* theSyncPtr = mSyncQueueHeadPtr;
        mSyncQueueHeadPtr = 0;
        mTmpSyncSMQueue.clear();
        theCnt = 0;
        while (theSyncPtr) {
            CheckQueueSize(++theCnt, "sync");
            RemoteSyncSM& theCur = *theSyncPtr;
            theSyncPtr = GetNextPtr(theCur);
            if (theSyncPtr == &theCur) {
                theSyncPtr = 0;
            }
            GetNextPtr(theCur) = 0;
            mTmpSyncSMQueue.push_back(&theCur);
        }
        bool theLastFlag = true;
        for (TmpDispatchQueue::iterator theIt = mTmpDispatchQueue.end();
                theIt != mTmpDispatchQueue.begin(); ) {
            --theIt;
            if (! RunPending(**theIt)) {
                if (theLastFlag) {
                    theIt = mTmpDispatchQueue.erase(theIt);
                    continue;
                }
                *theIt = 0;
            }
            theLastFlag = false;
        }
        while (! mTmpSyncSMQueue.empty()) {
            RunPending(*mTmpSyncSMQueue.back());
            mTmpSyncSMQueue.pop_back();
        }
        RSReplicatorEntry* theNextPtr = mRSReplicatorQueueHeadPtr;
        mRSReplicatorQueueHeadPtr = 0;
        mTmpRSReplicatorQueue.clear();
        theCnt = 0;
        while (theNextPtr) {
            CheckQueueSize(++theCnt, "replicate");
            RSReplicatorEntry& theCur = *theNextPtr;
            theNextPtr = GetNextPtr(theCur);
            if (&theCur == theNextPtr) {
                theNextPtr = 0;
            }
            GetNextPtr(theCur) = 0;
            mTmpRSReplicatorQueue.push_back(&theCur);
        }
        theLocker.Unlock();
        while (! mTmpDispatchQueue.empty()) {
            ClientSM* const thePtr = mTmpDispatchQueue.back();
            if (thePtr) {
                GetConnection(*thePtr)->StartFlush();
            }
            mTmpDispatchQueue.pop_back();
        }
        while (! mTmpRSReplicatorQueue.empty()) {
            mTmpRSReplicatorQueue.back()->Handle();
            mTmpRSReplicatorQueue.pop_back();
        }
    }
    int Handle(
        ClientSM& inClient,
        int       inCode,
        void*     inDataPtr)
    {
        if (inCode == EVENT_CMD_DONE) {
            if (GetCurrentClientThreadPtr() == &mOuter) {
                int theRet = DispatchGrantedIfPendingAndNoOps(inClient);
                if (theRet != 0) {
                    die("pending granted: invalid non zero return:"
                        " op completion pending");
                }
                const bool theFlushFlag    =
                    ! GetConnection(inClient)->IsWriteReady();
                bool      theRecursionFlag = false;
                theRet = ClientThreadListEntry::HandleRequest(
                    inClient, inCode, inDataPtr, theRecursionFlag);
                if (theFlushFlag && theRet == 0) {
                    GetConnection(inClient)->Flush();
                }
                return theRet;
            }
            QCASSERT(inDataPtr);
            if (AddPending(*reinterpret_cast<KfsOp*>(inDataPtr), inClient) &&
                    Enqueue(inClient)) {
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
        int theRet = DispatchGrantedIfPendingAndNoOps(inClient);
        if (theRet != 0) {
            return theRet;
        }
        bool      theRecursionFlag = false;
        theRet = ClientThreadListEntry::HandleRequest(
            inClient, inCode, inDataPtr, theRecursionFlag);
        theLocker.Unlock();
        if (theRet == 0 && ! theRecursionFlag) {
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
        if (! theEntry.mOpsHeadPtr && Enqueue(inClient)) {
            Wakeup();
        }
    }
    NetManager& GetNetManager()
        { return mNetManager; }
    const QCThread& GetThread() const
        { return mThread; }
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
                Enqueue(inSyncSM, mSyncQueueHeadPtr)) {
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
                Enqueue(inSyncSM, mSyncQueueHeadPtr)) {
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
    typedef ClientThreadListEntry::DispatchQueue DispatchQueue;
    typedef vector<ClientSM*>                    TmpDispatchQueue;
    typedef vector<RemoteSyncSM*>                TmpSyncSMQueue;
    typedef vector<RSReplicatorEntry*>           TmpRSReplicatorQueue;
    enum { kDispatchQueueCount = ClientThreadListEntry::kDispatchQueueCount };

    QCThread               mThread;
    bool                   mRunFlag;
    bool                   mShutdownFlag;
    NetManager             mNetManager;
    RemoteSyncSM*          mSyncQueueHeadPtr;
    RSReplicatorEntry*     mRSReplicatorQueueHeadPtr;
    TmpDispatchQueue       mTmpDispatchQueue;
    TmpSyncSMQueue         mTmpSyncSMQueue;
    TmpRSReplicatorQueue   mTmpRSReplicatorQueue;
    volatile int           mWakeupCnt;
    ClientThread&          mOuter;
    ClientThreadListEntry* mAddQueuePtr[kDispatchQueueCount];
    ClientThreadListEntry* mDispatchQueuePtr[kDispatchQueueCount];
    char                   mParseBuffer[MAX_RPC_HEADER_LEN];

    static ClientThread* sCurrentClientThreadPtr;
    static int           sLockCnt;

    void CheckQueueSize(
        int         inSize,
        const char* inNamePtr)
    {
        const int kMaxSize = 256 << 10;
        const int theMax   = kMaxSize + mNetManager.GetConnectionCount();
        if (theMax < inSize) {
            KFS_LOG_STREAM_FATAL <<
                "queue: " << inNamePtr << " exceeded max. allowed size" <<
                " connections: " << theMax - kMaxSize <<
            KFS_LOG_EOM;
            MsgLogger::Stop();
            abort();
        }
    }
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
            bool theRecursionFlag = false;
            if (ClientThreadListEntry::HandleRequest(
                    inClient, EVENT_CMD_DONE, &theCur, theRecursionFlag) != 0) {
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
    int DispatchGrantedIfPendingAndNoOps(
        ClientSM& inClient)
    {
        // If "granted" event pending, with no ops completion pending, then
        // dispatch "granted" event before any other events, to maintain the
        // same order as with no client threads, and ensure that if the current
        // event is timeout or connection close / error the client is removed
        // from the dispatch queue before processing the current event, as with
        // no pending ops the client can simply go away by deleting self.
        ClientThreadListEntry& theEntry = inClient;
        if (theEntry.mGrantedFlag && ! theEntry.mOpsHeadPtr) {
            DispatchQueue::Remove(mDispatchQueuePtr, theEntry);
            theEntry.mGrantedFlag = false;
            return ClientThreadListEntry::HandleGranted(inClient);
        }
        return 0;
    }
    bool Enqueue(
        ClientSM& inClientSm)
    {
        const bool theWasEmptyFlag = DispatchQueue::IsEmpty(mDispatchQueuePtr);
        QCASSERT(! DispatchQueue::IsInList(mDispatchQueuePtr, inClientSm));
        DispatchQueue::PushBack(mDispatchQueuePtr, inClientSm);
        return theWasEmptyFlag;
    }
    static RemoteSyncSM*& GetNextPtr(
        ClientThreadRemoteSyncListEntry& inEntry)
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
        T*& inQueueHeadPtr)
    {
        QCASSERT(! GetNextPtr(inEntry));
        const bool theWasEmptyFlag = ! inQueueHeadPtr;
        GetNextPtr(inEntry) = inQueueHeadPtr ? inQueueHeadPtr : &inEntry;
        inQueueHeadPtr = &inEntry;
        return theWasEmptyFlag;
    }
    template<typename T>
    static bool EnqueueBack(
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
            EnqueueBack(inOp, theEntry.mOpsHeadPtr, theEntry.mOpsTailPtr) &&
            ! theEntry.mGrantedFlag
        );
    }
    static bool AddPending(
        KfsOp&        inOp,
        RemoteSyncSM& inSyncSM)
    {
        ClientThreadRemoteSyncListEntry& theEntry = inSyncSM;
        return (
            EnqueueBack(inOp, theEntry.mOpsHeadPtr, theEntry.mOpsTailPtr) &&
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
    if (mOpsHeadPtr || mOpsTailPtr || mGrantedFlag ||
            &DispatchQueue::GetPrev(*this) != this ||
            &DispatchQueue::GetNext(*this) != this) {
        ostringstream theStream;
        theStream <<
            "invalid client thread list entry destructor invocation" <<
            " entry: "   << (const void*)this <<
            " ops: "     << (const void*)mOpsHeadPtr <<
            " "          << (const void*)mOpsTailPtr <<
            " prev: "    << (const void*)&DispatchQueue::GetPrev(*this) <<
            " next: "    << (const void*)&DispatchQueue::GetNext(*this) <<
            " granted: " << mGrantedFlag
        ;
        die(theStream.str());
    }
    // To catch double delete.
    mPrevPtr[kDispatchQueueIdx] = 0;
    mNextPtr[kDispatchQueueIdx] = 0;
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
        ostringstream theStream;
        theStream <<
            "invalid remote sync list entry destructor invocation" <<
            " entry: "    << (const void*)this <<
            " ops: "      << (const void*)mOpsHeadPtr <<
            " "           << (const void*)mOpsTailPtr <<
            " next: "     << (const void*)mNextPtr <<
            " finished: " << mFinishFlag
        ;
        die(theStream.str());
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
        ostringstream theStream;
        theStream <<
            "invalid rs replicator list entry destructor invocation" <<
            " entry: " << (const void*)this <<
            " next: "  << (const void*)mNextPtr
        ;
        die(theStream.str());
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

    const QCThread&
ClientThread::GetThread() const
{
    return mImpl.GetThread();
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
    int       inFirstCpuIdx,
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
        theThreadsPtr[i].mImpl.Start(
            inFirstCpuIdx < 0 ? -1 : inFirstCpuIdx + i);
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
