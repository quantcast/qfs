#include "Resolver.h"
#include "NetManager.h"

#include "common/kfsatomic.h"
#include "common/SingleLinkedQueue.h"

#include "qcdio/QCMutex.h"
#include "qcdio/QCThread.h"
#include "qcdio/qcstutils.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <string.h>
#include <unistd.h>

namespace KFS
{

class Resolver::Impl : public QCRunnable, public ITimeout
{
public:
    Impl(
        NetManager& inNetManager)
        : QCRunnable(),
          ITimeout(),
          mNetManager(inNetManager),
          mQueue(),
          mThread(),
          mMutex(),
          mCondVar(),
          mRunFlag(false),
          mDoneCount(0)
        {}
    virtual ~Impl()
        { Impl::Shutdown(); }
    int Start()
    {
        if (mRunFlag) {
            return -EINVAL;
        }
        mRunFlag = true;
        const int kStackSize = 64 << 10;
        mThread.Start(this, kStackSize, "Resolver");
        return 0;
    }
    void Shutdown()
    {
        QCStMutexLocker theLock(mMutex);
        if (! mRunFlag) {
            return;
        }
        mRunFlag = false;
        mCondVar.Notify();
        theLock.Unlock();
        mThread.Join();
    }
    int Enqueue(
        Request& inRequest)
    {
        QCStMutexLocker theLock(mMutex);
        if (! mRunFlag) {
            return -EINVAL;
        }
        mQueue.PushBack(inRequest);
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
        Request* thePtr;
        while ((thePtr = theDoneQueue.PopFront())) {
            thePtr->Done();
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
            while ((thePtr = Queue::GetNext(*thePtr))) {
                Process(*thePtr);
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
private:
    typedef SingleLinkedQueue<Request, Impl> Queue;

    NetManager&     mNetManager;
    Queue           mQueue;
    Queue           mDoneQueue;
    QCThread        mThread;
    QCMutex         mMutex;
    QCCondVar       mCondVar;
    bool            mRunFlag;
    volatile int    mDoneCount;
    struct addrinfo mAddrInfoHints;
    char            mNameBuf[(INET_ADDRSTRLEN < INET6_ADDRSTRLEN ?
            INET6_ADDRSTRLEN : INET_ADDRSTRLEN) + 1];

    void Process(
        Request& inReq)
    {
        memset(&mAddrInfoHints, 0, sizeof(mAddrInfoHints));
        mAddrInfoHints.ai_family   = AF_UNSPEC;   // Allow IPv4 or IPv6
        mAddrInfoHints.ai_socktype = SOCK_STREAM; // Datagram socket
        mAddrInfoHints.ai_flags    = 0;
        mAddrInfoHints.ai_protocol = 0;           // Any protocol
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
            if (! inet_ntop(
                    thePtr->ai_family,
                    thePtr->ai_addr,
                    mNameBuf,
                    theSize)) {
                if (0 == theErr) {
                    theErr = errno;
                }
                continue;
            }
            mNameBuf[theSize] = 0;
            inReq.mIpAddresses.push_back(string(mNameBuf));
        }
        freeaddrinfo(theResPtr);
        if (inReq.mIpAddresses.empty() && 0 != theErr) {
            inReq.mStatus    = theErr;
            inReq.mStatusMsg = QCUtils::SysError(theErr);
        }
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

Resolver::Resolver(
    NetManager& inNetManager)
    : mImpl(*(new Impl(inNetManager)))
{
}

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
    Resolver::Request& inRequest)
{
    return mImpl.Enqueue(inRequest);
}

};
