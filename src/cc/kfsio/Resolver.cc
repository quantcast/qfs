//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2016/11/15
// Author: Mike Ovsiannikov
//
// Copyright 2016-2017 Quantcast Corporation. All rights reserved.
//
// This file is part of Quantcast File System.
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
//----------------------------------------------------------------------------

#include "Resolver.h"
#include "NetManager.h"
#include "Globals.h"

#include "common/MsgLogger.h"
#include "common/SingleLinkedQueue.h"
#include "common/kfsatomic.h"

#include "qcdio/QCMutex.h"
#include "qcdio/QCUtils.h"
#include "qcdio/QCThread.h"
#include "qcdio/qcstutils.h"
#include "qcdio/QCDLList.h"

#include <algorithm>
#include <set>
#include <vector>

#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

#ifndef QFS_OMIT_EXT_DNS_RESOLVER
extern "C" {
#include <stdbool.h>
#define class d_class;
#include "dns.h"
#undef class
}
#endif

namespace KFS
{
using KFS::libkfsio::globals;
using std::max;
using std::pair;

class Resolver::Impl
{
public:
    Impl(
        NetManager& inNetManager)
        : mNetManager(inNetManager),
          mRunFlag(false),
          mCache(),
          mExpirationList(),
          mSearchEntry(),
          mMaxCacheSize(8 << 10),
          mExpirationTimeSec(-1)
    {
        memset(&mAddrInfoHints, 0, sizeof(mAddrInfoHints));
        mAddrInfoHints.ai_family   = AF_UNSPEC;   // Allow IPv4 or IPv6
        mAddrInfoHints.ai_socktype = SOCK_STREAM; // Datagram socket
        mAddrInfoHints.ai_flags    = 0;
        mAddrInfoHints.ai_protocol = 0;           // Any protocol
    }
    virtual ~Impl()
        {}
    virtual int Start() = 0;
    virtual void Shutdown() = 0;
    int Enqueue(
        Request& inRequest,
        int      inTimeout)
    {
        inRequest.mStartUsec = mNetManager.NowUsec();
        if (! mRunFlag) {
            return -EINVAL;
        }
        if (Find(inRequest)) {
            return 0;
        }
        return EnqueueSelf(inRequest, inTimeout);
    }
    void SetCacheSizeAndTimeout(
        size_t inMaxCacheSize,
        int    inTimeoutSec)
    {
        mMaxCacheSize      = inMaxCacheSize;
        mExpirationTimeSec = inTimeoutSec;
        if (mMaxCacheSize <= 0 || mExpirationTimeSec <= 0) {
            mCache.clear();
        }
    }
protected:
    NetManager&     mNetManager;
    volatile bool   mRunFlag;
    struct addrinfo mAddrInfoHints;
    char            mNameBuf[(INET_ADDRSTRLEN < INET6_ADDRSTRLEN ?
            INET6_ADDRSTRLEN : INET_ADDRSTRLEN) + 1];

    void Done(
        Request& inRequest,
        bool     inExpireFlag = true)
    {
        const int64_t theTime    = mNetManager.NowUsec() - inRequest.mStartUsec;
        Counter&      theCounter = 0 == inRequest.mStatus ?
            globals().ctrNetDnsErrors : globals().ctrNetDnsErrors;
        theCounter.Update(1);
        theCounter.UpdateTime(theTime);
        if (0 < mExpirationTimeSec && 0 < mMaxCacheSize) {
            if (inExpireFlag) {
                const time_t theExpTime =
                    mNetManager.Now() - mExpirationTimeSec;
                Entry*       thePtr     = &mExpirationList;
                while ((thePtr = &Entry::List::GetNext(mExpirationList)) !=
                        &mExpirationList && thePtr->mTime < theExpTime) {
                    mCache.erase(*thePtr);
                }
            }
            if (0 == inRequest.mStatus && mMaxCacheSize < mCache.size()) {
                Entry theEntry(inRequest.mHostName);
                theEntry.mTime        = mNetManager.Now();
                theEntry.mIpAddresses = inRequest.mIpAddresses;
                theEntry.mMaxResults  = inRequest.mMaxResults;
                pair<Cache::iterator, bool> const
                    theRes = mCache.insert(theEntry);
                Entry& theInserted = const_cast<Entry&>(*theRes.first);
                if (! theRes.second) {
                    theInserted = theEntry;
                }
                Entry::List::Insert(
                    theInserted, Entry::List::GetPrev(mExpirationList));
            }
        }
        inRequest.Done();
    }
    virtual int EnqueueSelf(
        Request& inRequest,
        int      inTimeout) = 0;
private:
    typedef Request::IpAddresses IpAddresses;

    struct Entry
    {
    public:
        Entry(
            const string& inHostName = string())
            : mMaxResults(),
              mTime(),
              mHostName(inHostName),
              mIpAddresses()
            { List::Init(*this); }
        ~Entry()
            { List::Remove(*this); }
        bool operator<(
            const Entry& inRhs) const
            { return (mHostName < inRhs.mHostName); }
        bool operator==(
            const Entry& inRhs) const
            { return (mHostName == inRhs.mHostName); }
    private:
        typedef QCDLListOp<Entry, 0> List;

        int         mMaxResults;
        time_t      mTime;
        string      mHostName;
        IpAddresses mIpAddresses;
        Entry*      mPrevPtr[1];
        Entry*      mNextPtr[1];

        friend class QCDLListOp<Entry, 0>;
        friend class Impl;
    };
    typedef std::set<
        Entry,
        std::less<Entry>,
        StdFastAllocator< Entry>
    > Cache;

    Cache  mCache;
    Entry  mExpirationList;
    Entry  mSearchEntry;
    size_t mMaxCacheSize;
    int    mExpirationTimeSec;

    bool Find(
        Request& inRequest)
    {
        if (mExpirationTimeSec <= 0 || mMaxCacheSize <= 0) {
            return false;
        }
        mSearchEntry.mHostName = inRequest.mHostName;
        const Cache::iterator theIt = mCache.find(mSearchEntry);
        mSearchEntry.mHostName = string();
        if (mCache.end() == theIt) {
            return false;
        }
        if (theIt->mTime + mExpirationTimeSec < mNetManager.Now()) {
            mCache.erase(theIt);
            return false;
        }
        if (0 < theIt->mMaxResults &&
                (inRequest.mMaxResults <= 0 ||
                theIt->mMaxResults < inRequest.mMaxResults) &&
                (size_t)theIt->mMaxResults <
                    theIt->mIpAddresses.size()) {
            return false;
        }
        inRequest.mIpAddresses = theIt->mIpAddresses;
        inRequest.mStatus      = 0;
        inRequest.mStatusMsg.clear();
        Done(inRequest);
        return true;
    }
protected:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

class Resolver::OsImpl :
    public Resolver::Impl,
    public QCRunnable,
    public ITimeout
{
public:
    OsImpl(
        NetManager& inNetManager)
        : Resolver::Impl(inNetManager),
          QCRunnable(),
          ITimeout(),
          mQueue(),
          mThread(),
          mMutex(),
          mCondVar(),
          mDoneCount(0)
        {}
    virtual ~OsImpl()
        { OsImpl::Shutdown(); }
    virtual int Start()
    {
        if (mRunFlag) {
            return -EINVAL;
        }
        mRunFlag = true;
        const int kStackSize = 64 << 10;
        mNetManager.RegisterTimeoutHandler(this);
        mThread.Start(this, kStackSize, "Resolver");
        return 0;
    }
    virtual void Shutdown()
    {
        QCStMutexLocker theLock(mMutex);
        if (! mRunFlag) {
            return;
        }
        mRunFlag = false;
        mCondVar.Notify();
        theLock.Unlock();
        mThread.Join();
        mNetManager.UnRegisterTimeoutHandler(this);
    }
    virtual int EnqueueSelf(
        Request& inRequest,
        int      inTimeout)
    {
        inRequest.mEndUsec = inRequest.mStartUsec +
            ((int64_t)inTimeout * 1000 * 1000);
        QCStMutexLocker theLock(mMutex);
        if (! mRunFlag) {
            return -EINVAL;
        }
        const bool theWakeFlag = mQueue.IsEmpty();
        mQueue.PushBack(inRequest);
        theLock.Unlock();
        if (theWakeFlag) {
            mCondVar.Notify();
        }
        return 0;
    }
    virtual void Timeout()
    {
        if (0 == SyncAddAndFetch(mDoneCount, 0)) {
            return;
        }
        Queue           theDoneQueue;
        QCStMutexLocker theLock(mMutex);
        theDoneQueue.PushBack(mDoneQueue);
        mDoneCount = 0;
        theLock.Unlock();
        bool     theExpireFlag = true;
        Request* thePtr;
        while ((thePtr = theDoneQueue.PopFront())) {
            Done(*thePtr, theExpireFlag);
            theExpireFlag = false;
        }
    }
    virtual void Run()
    {
        QCStMutexLocker theLock(mMutex);
        for (; ;) {
            while (mRunFlag && mQueue.IsEmpty()) {
                mCondVar.Wait(mMutex);
            }
            Queue theQueue;
            theQueue.PushBack(mQueue);
            QCStMutexUnlocker theUnlocker(mMutex);
            Request* thePtr = theQueue.Front();
            while (thePtr) {
                Process(*thePtr);
                thePtr = Queue::GetNext(*thePtr);
            }
            theUnlocker.Lock();
            const bool theWakeupFlag =
                ! theQueue.IsEmpty() && mDoneQueue.IsEmpty();
            mDoneQueue.PushBack(theQueue);
            if (theWakeupFlag) {
                SyncAddAndFetch(mDoneCount, 1);
                mNetManager.Wakeup();
            }
            if (! mRunFlag && mQueue.IsEmpty()) {
                break;
            }
        }
    }
    static Request*& Next(
        Request& inRequest)
        { return inRequest.mNextPtr; }
    static int Initialize()
        { return 0; }
    static void Cleanup()
        {}
private:
    typedef SingleLinkedQueue<Request, OsImpl> Queue;

    Queue        mQueue;
    Queue        mDoneQueue;
    QCThread     mThread;
    QCMutex      mMutex;
    QCCondVar    mCondVar;
    volatile int mDoneCount;

    void Process(
        Request& inReq)
    {
        if (! mRunFlag) {
            inReq.mStatus    = -ECANCELED;
            inReq.mStatusMsg = "resolver shutdown";
            return;
        }
        struct addrinfo* theResPtr = 0;
        inReq.mStatus = getaddrinfo(
            inReq.mHostName.c_str(), 0, &mAddrInfoHints, &theResPtr);
        inReq.mIpAddresses.clear();
        if (0 != inReq.mStatus) {
            inReq.mStatusMsg = gai_strerror(inReq.mStatus);
            if (theResPtr) {
                freeaddrinfo(theResPtr);
            }
            return;
        }
        inReq.mStatusMsg.clear();
        int theErr = 0;
        for (struct addrinfo const* thePtr = theResPtr;
                thePtr;
                thePtr = thePtr->ai_next) {
            if (AF_INET != thePtr->ai_family && AF_INET6 != thePtr->ai_family) {
                continue;
            }
            const socklen_t theSize = thePtr->ai_family == AF_INET ?
                INET6_ADDRSTRLEN : INET6_ADDRSTRLEN;
            const int theStatus = getnameinfo(
                thePtr->ai_addr, thePtr->ai_addrlen, mNameBuf, theSize,
                0, 0, NI_NUMERICHOST | NI_NUMERICSERV
            );
            if (0 != theStatus) {
                theErr = theStatus;
                continue;
            }
            mNameBuf[theSize] = 0;
            inReq.mIpAddresses.push_back(string(mNameBuf));
            if (0 < inReq.mMaxResults &&
                    (size_t)inReq.mMaxResults <= inReq.mIpAddresses.size()) {
                break;
            }
        }
        freeaddrinfo(theResPtr);
        if (inReq.mIpAddresses.empty() && 0 != theErr) {
            inReq.mStatus    = theErr;
            inReq.mStatusMsg = gai_strerror(theErr);
        }
    }
private:
    OsImpl(
        const OsImpl& inImpl);
    OsImpl& operator=(
        const OsImpl& inImpl);
};

#ifndef QFS_OMIT_EXT_DNS_RESOLVER

class Resolver::ExtImpl : public Resolver::Impl
{
public:
    ExtImpl(
        NetManager& inNetManager)
        : Resolver::Impl(inNetManager),
          mReqCount(0)
        {}
    virtual ~ExtImpl()
    {
        ExtImpl::Shutdown();
        QCRTASSERT(0 == mReqCount);
        mReqCount = -1000;
    }
    virtual int Start()
    {
        if (! mNetManager.IsRunning()) {
            return -EINVAL;
        }
        if (mRunFlag) {
            return 0;
        }
        if (! sDnsResolverInitPtr) {
            QCStMutexLocker theLocker(sDnsResolverInitMutex);
            const int theStatus = Initialize();
            if (0 != theStatus) {
                return theStatus;
            }
        }
        const int theStatus = sDnsResolverInitPtr->GetError();
        if (theStatus) {
            return theStatus;
        }
        mRunFlag = true;
        return 0;
    }
    virtual void Shutdown()
        { mRunFlag = false; }
    virtual int EnqueueSelf(
        Request& inRequest,
        int      inTimeout)
    {
        DnsReq::Run(*this, inRequest, inTimeout);
        return 0;
    }
    static int Initialize()
    {
        if (! sDnsResolverInitPtr) {
            static DnsResolverInit sDnsResolverInit;
            sDnsResolverInitPtr = &sDnsResolverInit;
            sDnsResolverInitPtr->Init();
        }
        return sDnsResolverInitPtr->GetError();
    }
    static void Cleanup()
    {
        if (! sDnsResolverInitPtr) {
            return;
        }
        sDnsResolverInitPtr->Cleanup();
        sDnsResolverInitPtr = 0;
    }
private:
    class DnsReq : public NetConnection::Filter, public KfsCallbackObj
    {
    public:
        static void Run(
            ExtImpl& inImpl,
            Request& inRequest,
            int      inTimeout)
        {
            inRequest.mEndUsec = inRequest.mStartUsec + Sec2Usec(inTimeout);
            (new DnsReq(inImpl, inRequest))->Start();
        }
        virtual bool WantRead(
            const NetConnection& inConnection) const
            { return mWantReadFlag; }
        virtual bool WantWrite(
            const NetConnection& inConnection) const
            { return mWantWriteFlag; }
        virtual int Read(
            NetConnection& inConnection,
            TcpSocket&     inSocket,
            IOBuffer&      /* inBuffer */,
            int            /* inMaxRead */)
            { return Process(inConnection, inSocket); }
        virtual int Write(
            NetConnection& inConnection,
            TcpSocket&     inSocket,
            IOBuffer&      /* inBuffer */,
            bool&          outForceInvokeErrHandlerFlag)
        {
            outForceInvokeErrHandlerFlag = false;
            return Process(inConnection, inSocket);
        }
        virtual void Close(
            NetConnection& inConnection,
            TcpSocket*     inSocketPtr)
        {
            QCRTASSERT((! mNetConnectionPtr ||
                &*mNetConnectionPtr == &inConnection) &&
                (! inSocketPtr || inSocketPtr == &mSocket));
            if (mAiPtr) {
                Error();
            }
        }
        virtual int Shutdown(
            NetConnection& inConnection,
            TcpSocket&     inSocket)
        {
            Close(inConnection, &inSocket);
            return 0;
        }
        int HandleEvent(
            int   inEventCode,
            void* /* inDataPtr */)
        {
            if (mAiPtr) {
                if (EVENT_INACTIVITY_TIMEOUT == inEventCode) {
                    mRequest.mStatus    = -ETIMEDOUT;
                    mRequest.mStatusMsg = "request timed out";
                    RequestDone();
                } else if (EVENT_NET_ERROR == inEventCode) {
                    Error();
                }
            }
            if (! mAiPtr) {
                delete this;
            }
            return 0;
        }
    private:
        ExtImpl&         mImpl;
        Request&         mRequest;
        dns_addrinfo*    mAiPtr;
        TcpSocket        mSocket;
        NetConnectionPtr mNetConnectionPtr;
        bool             mWantReadFlag;
        bool             mWantWriteFlag;

        static int64_t Usec2Sec(
            int64_t inUsec)
            { return (inUsec >> 20); }
        static int64_t Sec2Usec(
            int64_t inUsec)
            { return (inUsec << 20); }
        DnsReq(
            ExtImpl& inImpl,
            Request& inRequest)
            : NetConnection::Filter(),
              KfsCallbackObj(),
              mImpl(inImpl),
              mRequest(inRequest),
              mAiPtr(0),
              mSocket(),
              mNetConnectionPtr(),
              mWantReadFlag(false),
              mWantWriteFlag(false)
        {
            SET_HANDLER(this, &DnsReq::HandleEvent);
            mImpl.mReqCount++;
            QCRTASSERT(0 < mImpl.mReqCount);
            mRequest.mIpAddresses.clear();
            mRequest.mStatus = 0;
            mRequest.mStatusMsg.clear();
        }
        virtual ~DnsReq()
        {
            QCRTASSERT(! mAiPtr && 0 < mImpl.mReqCount);
            mImpl.mReqCount--;
            CloseConnection();
        }
        int Process(
            const NetConnection& inConnection,
            const TcpSocket&     inSocket)
        {
            QCRTASSERT((! mNetConnectionPtr ||
                &*mNetConnectionPtr == &inConnection) && &inSocket == &mSocket);
            ProcessSelf();
            // Return one byte read or written if done, to invoke handler
            // in order to do the cleanup.
            return (mAiPtr ? -EAGAIN : 1);
        }
        void Error()
        {
            const int theStatus = mNetConnectionPtr ?
                mNetConnectionPtr->GetErrorCode() : 0;
            if (0 == theStatus) {
                RequestDone(mImpl.mNetManager.IsRunning() ? -EIO : -ECANCELED);
            } else {
                mRequest.mStatus    = theStatus;
                mRequest.mStatusMsg = mNetConnectionPtr->GetErrorMsg();
                RequestDone();
            }
        }
        void ProcessSelf()
        {
            if (! mAiPtr) {
                return;
            }
            for (; ;) {
                struct addrinfo* theResPtr = 0;
                const int theStatus = dns_ai_nextent(&theResPtr, mAiPtr);
                if (EAGAIN == theStatus) {
                    SetConnection();
                    return;
                }
                if (0 == theStatus) {
                    SetResult(theResPtr);
                    free(theResPtr);
                    if (0 < mRequest.mMaxResults &&
                            (size_t)mRequest.mMaxResults <=
                                mRequest.mIpAddresses.size()) {
                        break;
                    }
                    continue;
                }
                if (ENOENT == theStatus) {
                    if (mRequest.mIpAddresses.empty() &&
                            0 == mRequest.mStatus) {
                        mRequest.mStatus    = theStatus;
                        mRequest.mStatusMsg = QCUtils::SysError(theStatus);
                    }
                } else {
                    mRequest.mStatus    = theStatus;
                    mRequest.mStatusMsg = dns_strerror(theStatus);
                }
                break;
            }
            RequestDone();
        }
        void Start()
        {
            int         theError = 0;
            dns_options theDnsOptions;
            memset(&theDnsOptions, 0, sizeof(theDnsOptions));
            theDnsOptions.closefd.arg = this;
            theDnsOptions.closefd.cb  = &DnsReq::CloseFd;
            theDnsOptions.events      = dns_options::DNS_SYSPOLL;
            dns_resolver* const theDnsPtr = dns_res_open(
                sDnsResolverInitPtr->GetConf(),
                sDnsResolverInitPtr->GetHosts(),
                dns_hints_mortal(dns_hints_local(
                    sDnsResolverInitPtr->GetConf(), &theError)),
                    0, &theDnsOptions, &theError);
            if (theDnsPtr) {
                const dns_type kTypeCheckHints = (dns_type)0;
                mAiPtr = dns_ai_open(
                    mRequest.mHostName.c_str(), "0",
                    kTypeCheckHints, &mImpl.mAddrInfoHints, theDnsPtr,
                    &theError);
                dns_res_close(theDnsPtr); // De-reference.
            }
            if (mAiPtr) {
                ProcessSelf();
            } else {
                mRequest.mStatus    = theError;
                mRequest.mStatusMsg = dns_strerror(theError);
                RequestDoneSelf();
            }
            if (! mAiPtr) {
                delete this;
            }
        }
        void UpdateIoFlags()
        {
            if (! mAiPtr) {
                return;
            }
            if (mSocket.IsGood()) {
                const int theEvents = dns_ai_events(mAiPtr);
                mWantReadFlag  = 0 != (theEvents & DNS_POLLIN);
                mWantWriteFlag = 0 != (theEvents & DNS_POLLOUT);
            } else {
                mWantReadFlag  = false;
                mWantWriteFlag = false;
            }
            if (mNetConnectionPtr) {
                mNetConnectionPtr->SetInactivityTimeout(
                    mRequest.mEndUsec <= mRequest.mStartUsec ? -1 :
                    max(0, (int)Usec2Sec(mRequest.mEndUsec -
                        mImpl.mNetManager.NowUsec())));
            }
        }
        void SetConnection()
        {
            if (! mAiPtr) {
                return;
            }
            const int theFd = dns_ai_pollfd(mAiPtr);
            if (mNetConnectionPtr) {
                if (mSocket.GetFd() == theFd &&
                        (mNetConnectionPtr->IsGood() || theFd < 0)) {
                    UpdateIoFlags();
                    return;
                }
                CloseConnection();
            }
            if (mSocket.GetFd() != theFd) {
                TcpSocket theSocket(theFd);
                mSocket = theSocket;
                theSocket = TcpSocket();
            }
            const bool kListenOnlyFlag = false;
            const bool kOwnsSocketFlag = false;
            mNetConnectionPtr.reset(new NetConnection(&mSocket,
                this, kListenOnlyFlag, kOwnsSocketFlag, this));
            UpdateIoFlags();
            mImpl.mNetManager.AddConnection(mNetConnectionPtr);
        }
        void SetResult(
            const struct addrinfo* inResPtr)
        {
            if (! inResPtr) {
                return;
            }
            if (AF_INET != inResPtr->ai_family && AF_INET6 !=
                    inResPtr->ai_family) {
                return;
            }
            const socklen_t theSize = inResPtr->ai_family == AF_INET ?
                INET6_ADDRSTRLEN : INET6_ADDRSTRLEN;
            const int theStatus = getnameinfo(
                inResPtr->ai_addr, inResPtr->ai_addrlen, mImpl.mNameBuf,
                theSize, 0, 0, NI_NUMERICHOST | NI_NUMERICSERV
            );
            if (0 != theStatus) {
                if (mRequest.mIpAddresses.empty() && 0 != theStatus) {
                    if (EAI_SYSTEM == theStatus) {
                        mRequest.mStatus    = 0 != errno ? errno : -EINVAL;
                        mRequest.mStatusMsg = QCUtils::SysError(theStatus);
                    } else {
                        mRequest.mStatus    = theStatus;
                        mRequest.mStatusMsg = gai_strerror(theStatus);
                    }
                }
                return;
            }
            mImpl.mNameBuf[theSize] = 0;
            mRequest.mIpAddresses.push_back(string(mImpl.mNameBuf));
            mRequest.mStatus  = 0;
            mRequest.mStatusMsg.clear();
        }
        void RequestDone(
            int inStatus = 0)
        {
            if (! mAiPtr) {
                return;
            }
            if (0 != inStatus) {
                mRequest.mStatus    = inStatus;
                mRequest.mStatusMsg =
                    QCUtils::SysError(inStatus);
            }
            RequestDoneSelf();
        }
        void CloseConnection()
        {
            if (! mNetConnectionPtr) {
                return;
            }
            mNetConnectionPtr->SetFilter(0, 0);
            mNetConnectionPtr->Close();
            mNetConnectionPtr.reset();
        }
        void RequestDoneSelf()
        {
            mWantReadFlag  = false;
            mWantWriteFlag = false;
            dns_ai_close(mAiPtr);
            mAiPtr = 0;
            CloseConnection();
            mImpl.Done(mRequest);
        }
        int CloseFdSelf(
            int* inIoFdPtr)
        {
            if (! inIoFdPtr || mSocket.GetFd() != *inIoFdPtr) {
                return 0; // Let dns resolver handle this.
            }
            CloseConnection();
            if (mSocket.GetFd() < 0) {
                return EBADF;
            }
            mSocket.Close();
            *inIoFdPtr = -1;
            return 0;
        }
        static int CloseFd(
            int*  inIoFdPtr,
            void* inArgPtr)
        {
            return reinterpret_cast<DnsReq*>(inArgPtr)->CloseFdSelf(inIoFdPtr);
        }
    private:
        DnsReq(
            const DnsReq& inReq);
        DnsReq& operator=(
            const DnsReq& inReq);
    friend class ExtImpl;
    };
    friend class DnsReq;

    int mReqCount;

    class DnsResolverInit
    {
    public:
        DnsResolverInit()
            : mDnsConfPtr(0),
              mHostsPtr(0),
              mError(0)
            {}
        ~DnsResolverInit()
        {
            sDnsResolverInitPtr = 0;
            DnsResolverInit::Cleanup();
        }
        void Init()
        {
            if (! (mDnsConfPtr = dns_resconf_local(&mError)) ||
                    ! (mHostsPtr = dns_hosts_local(&mError))) {
                CleanupSetError();
            }
        }
        void Cleanup()
        {
            dns_resconf_close(mDnsConfPtr);
            mDnsConfPtr = 0;
            dns_hosts_close(mHostsPtr);
            mHostsPtr = 0;
        }
        int GetError() const
            { return mError; }
        struct dns_resolv_conf* GetConf() const
            { return mDnsConfPtr; }
        struct dns_hosts* GetHosts() const
            { return mHostsPtr; }
    private:
        struct dns_resolv_conf* mDnsConfPtr;
        struct dns_hosts*       mHostsPtr;
        int                     mError;

        void CleanupSetError()
        {
            Cleanup();
            if (0 == mError) {
                mError = -EFAULT;
            } else if (0 < mError) {
                mError = -mError;
            }
        }
    private:
        DnsResolverInit(
            const DnsResolverInit& inInit);
        DnsResolverInit& operator=(
            const DnsResolverInit& inInit);
    };
    static DnsResolverInit* sDnsResolverInitPtr;
    static QCMutex          sDnsResolverInitMutex;

private:
    ExtImpl(
        const ExtImpl& inImpl);
    ExtImpl& operator=(
        const ExtImpl& inImpl);
};
Resolver::ExtImpl::DnsResolverInit*
    Resolver::ExtImpl::sDnsResolverInitPtr = 0;
QCMutex
    Resolver::ExtImpl::sDnsResolverInitMutex;

#endif

Resolver::Resolver(
    NetManager&            inNetManager,
    Resolver::ResolverType inType)
    : mImpl(
#ifndef QFS_OMIT_EXT_DNS_RESOLVER
        inType == Resolver::ResolverTypeExt ?
        *static_cast<Resolver::Impl*>(
            new Resolver::ExtImpl(inNetManager)) :
#endif
        *static_cast<Resolver::Impl*>(
            new Resolver::OsImpl(inNetManager)))
{}

Resolver::~Resolver()
{
    delete &mImpl;
}

    int
Resolver::Start()
{
    return mImpl.Start();
}

    void
Resolver::Shutdown()
{
    mImpl.Shutdown();
}

    int
Resolver::Enqueue(
    Resolver::Request& inRequest,
    int                inTimeout)
{
    return mImpl.Enqueue(inRequest, inTimeout);
}

    void
Resolver::SetCacheSizeAndTimeout(
    size_t inMaxCacheSize,
    int    inTimeoutSec)
{
    return mImpl.SetCacheSizeAndTimeout(inMaxCacheSize, inTimeoutSec);
}

    void
Resolver::ChildAtFork()
{
}

    /* static */ int
Resolver::Initialize()
{
    int theRet = Resolver::OsImpl::Initialize();
#ifndef QFS_OMIT_EXT_DNS_RESOLVER
    const int theExtRet = Resolver::ExtImpl::Initialize();
    if (0 == theRet && 0 != theExtRet) {
        theRet = theExtRet;
    }
#endif
    return theRet;
}

    /* static */ void
Resolver::Cleanup()
{
    Resolver::OsImpl::Cleanup();
#ifndef QFS_OMIT_EXT_DNS_RESOLVER
    Resolver::ExtImpl::Cleanup();
#endif
}

};
