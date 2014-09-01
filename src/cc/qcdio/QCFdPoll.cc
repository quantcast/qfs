//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2009/03/16
// Author: Mike Ovsiannikov
//
// Copyright 2008-2011 Quantcast Corp.
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
// QCFdPoll implementations with dev poll, epoll, and poll.
// 
//----------------------------------------------------------------------------

#include "QCFdPoll.h"
#include "QCUtils.h"
#include "qcdebug.h"

#include <cerrno>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#ifndef QC_OS_NAME_LINUX
#   include <map>
#   ifdef QC_USE_BOOST
#       include <boost/pool/pool_alloc.hpp> 
#   endif
#endif

#ifdef QC_OS_NAME_SUNOS
#include <sys/filio.h>
#include <sys/devpoll.h>

class QCFdPoll::Impl
{
public:
    Impl() 
        : mFdMap(),
          mPollVecPtr(0),
          mGeneration(0),
          mDevpollFd(-1),
          mPollVecSize(0),
          mNextIdx(0),
          mLastIdx(0)
    {
        mDevpollFd = open("/dev/poll", O_RDWR);
        if (mDevpollFd < 0) {
            QCUtils::FatalError("Unable to open /dev/poll device:", errno);
        } else {
            fcntl(mDevpollFd, F_SETFD, FD_CLOEXEC);
        }
    }
    ~Impl()
    {
    }
    int Close()
    {
        int theRet = 0;
        if (mDevpollFd >= 0) {
            if (close(mDevpollFd)) {
                theRet = errno;
            }
            mDevpollFd = -1;
        }
        delete [] mPollVecPtr;
        mPollVecPtr  = 0;
        mGeneration  = 0;
        mPollVecSize = 0;
        mNextIdx     = 0;
        mLastIdx     = 0;
        return theRet;
    }
    int Add(
        Fd    inFd,
        int   inOpType,
        void* inUserDataPtr)
    {
        if (inFd < 0) {
            return EBADF;
        }
        // mGeneration is used to keep track of the entries added while
        // iterating trough the event list.
        const std::pair<FdMap::iterator, bool> theRes = mFdMap.insert(
            std::make_pair(inFd, std::make_pair(inUserDataPtr, mGeneration)));
        if (! theRes.second) {
            return EEXIST;
        }
        const int theRet = Ctl(inFd, inOpType, false);
        if (theRet != 0) {
            mFdMap.erase(theRes.first);
        }
        return theRet;
    }
    int Set(
        Fd    inFd,
        int   inOpType,
        void* inUserDataPtr)
    {
        if (inFd < 0) {
            return EBADF;
        }
        FdMap::iterator const theIt = mFdMap.find(inFd);
        if (theIt == mFdMap.end()) {
            return ENOENT;
        }
        const int theRet = Ctl(inFd, inOpType, true);
        if (theRet == 0) {
            theIt->second.first = inUserDataPtr;
        }
        return theRet;
    }
    int Remove(
        Fd inFd)
    {
        if (inFd < 0) {
            return EBADF;
        }
        FdMap::iterator const theIt = mFdMap.find(inFd);
        if (theIt == mFdMap.end()) {
            return ENOENT;
        }
        const int theRet = Ctl(inFd, 0, true);
        if (theRet == 0) {
            mFdMap.erase(theIt);
        }
        return theRet;
    }
    int Poll(
        int /* inMaxEventCountHint */,
        int inWaitMilliSec)
    {
        mNextIdx = 0;
        mLastIdx = 0;
        mGeneration++;
        if (mDevpollFd < 0) {
            return mDevpollFd;
        }
        int theEventCount(mFdMap.size());
        if (theEventCount < 1) {
            theEventCount = 1;
        }
        if (! mPollVecPtr || theEventCount > mPollVecSize) {
            delete [] mPollVecPtr;
            const int theAllocCount = theEventCount + 256;
            mPollVecPtr = new struct pollfd[theAllocCount];
            mPollVecSize = theAllocCount;
        }
        struct dvpoll theDvPoll = {0};
        theDvPoll.dp_fds     = mPollVecPtr;
        theDvPoll.dp_nfds    = theEventCount;
        theDvPoll.dp_timeout = inWaitMilliSec;
        const int theRet = ioctl(mDevpollFd, DP_POLL, &theDvPoll);
        if (theRet > 0) {
            mLastIdx = theRet;
        }
        return (theRet >= 0 ? theRet :
            (errno > 0 ? -errno : (errno == 0 ? -1 : errno)));
    }
    bool Next(
        int&   outOpType,
        void*& outUserDataPtr)
    {
        for ( ; mNextIdx < mLastIdx; mNextIdx++) {
            struct pollfd&              theEntry = mPollVecPtr[mNextIdx];
            FdMap::const_iterator const theIt    = mFdMap.find(theEntry.fd);
            if (theIt != mFdMap.end() && theIt->second.second != mGeneration) {
                outOpType      = FdPollMask(theEntry.revents);
                outUserDataPtr = theIt->second.first;
                mNextIdx++;
                return true;
            }
        }
        outOpType      = 0;
        outUserDataPtr = 0;
        return false;
    }

private:
    typedef std::map<Fd, std::pair<void*, size_t>
#ifdef QC_USE_BOOST
        , std::less<Fd>, boost::fast_pool_allocator<
            std::pair<const Fd, std::pair<void*, size_t> > >
#endif
    > FdMap;

    FdMap          mFdMap;
    struct pollfd* mPollVecPtr;
    size_t         mGeneration;
    int            mDevpollFd;
    int            mPollVecSize;
    int            mNextIdx;
    int            mLastIdx;

    int Ctl(Fd inFd, int inOpType, bool inRemoveFlag) 
    {
        // Solaris doesn't have a "modify" mechanism when changing the
        // types of events we are interested in polling. You have to
        // remove the fd and then put it back with the new flags
        struct pollfd entry[2];
        int           n   = 0;
        ssize_t       nWr = 0;
        if (inRemoveFlag) {
            entry[n].fd      = inFd;
            entry[n].events  = POLLREMOVE;
            entry[n].revents = 0;
            nWr += sizeof(entry[0]);
            n++;
        }
        if (! inRemoveFlag || inOpType != 0) {
            entry[n].fd      = inFd;
            entry[n].events  = PollEventMask(inOpType);
            entry[n].revents = 0;
            nWr += sizeof(entry[0]);
        }
        return (
            write(mDevpollFd, entry, nWr) == nWr ? 0 :
            (errno != 0 ? errno : -1)
        );
    }
    int PollEventMask(
        int inOpType)
    {
        int theRet = 0;
        if ((inOpType & kOpTypeIn) != 0) {
            theRet += POLLIN;
        }
        if ((inOpType & kOpTypeOut) != 0) {
            theRet += POLLOUT;
        }
        if ((inOpType & kOpTypePri) != 0) {
            theRet += POLLPRI;
        }
        return theRet;
    }
    int FdPollMask(
        int inFlags)
    {
        int theRet = 0;
        if ((inFlags & POLLIN) != 0) {
            theRet += kOpTypeIn;
        }
        if ((inFlags & POLLOUT) != 0) {
            theRet += kOpTypeOut;
        }
        if ((inFlags & POLLPRI) != 0) {
            theRet += kOpTypePri;
        }
        if ((inFlags & (POLLERR | POLLNVAL)) != 0) {
            theRet += kOpTypeError;
        }
        if ((inFlags & POLLHUP) != 0) {
            theRet += kOpTypeHup;
        }
        return theRet;
    }
private:
    Impl(
        const Impl& inImpl);
    Impl operator=(
        const Impl& inImpl);
};

#elif defined (QC_OS_NAME_LINUX)

#include <stdlib.h>
#include <pthread.h>
#include <sys/epoll.h>

class QCFdPoll::Impl
{
public:
    enum { kFdCountHint = 1 << 10 };

    Impl()
        : mEpollFd(epoll_create(kFdCountHint)),
          mEpollEventCount(0),
          mMaxEventCount(0),
          mNextEventIdx(0),
          mEventsPtr(0)
    {
        if (mEpollFd < 0 && errno != 0 && (mEpollFd = -errno) > 0) {
            mEpollFd = -mEpollFd;
        }
        if (mEpollFd >= 0) {
            fcntl(mEpollFd, F_SETFD, FD_CLOEXEC);
        }
    }
    ~Impl()
    {
        Impl::Close();
    }
    int Close()
    {
        int theRet = 0;
        if (mEpollFd >= 0) {
            if (close(mEpollFd)) {
                theRet = errno;
            }
            mEpollFd = -1;
        }
        mEpollEventCount = 0;
        mMaxEventCount   = 0;
        mNextEventIdx    = 0;
        delete [] mEventsPtr;
        mEventsPtr = 0;
        return theRet;
    }
    int Add(
        Fd    inFd,
        int   inOpType,
        void* inUserDataPtr)
        { return Ctl(EPOLL_CTL_ADD, inFd, inOpType, inUserDataPtr); }
    int Set(
        Fd    inFd,
        int   inOpType,
        void* inUserDataPtr)
        { return Ctl(EPOLL_CTL_MOD, inFd, inOpType, inUserDataPtr); }
    int Remove(
        Fd inFd)
        { return Ctl(EPOLL_CTL_DEL, inFd, 0, 0); }
    int Poll(
        int inMaxEventCountHint,
        int inWaitMilliSec)
    {
        mNextEventIdx = mEpollEventCount;
        if (mEpollFd < 0) {
            return mEpollFd;
        }
        const int theEventCount =
            inMaxEventCountHint > 1 ? inMaxEventCountHint : 1;
        if (! mEventsPtr || theEventCount > mMaxEventCount) {
            delete [] mEventsPtr;
            const int theAllocCount = theEventCount + 256;
            mEventsPtr = new struct epoll_event[theAllocCount];
            mMaxEventCount = theAllocCount;
        }
        mEpollEventCount = epoll_wait(
            mEpollFd, mEventsPtr, theEventCount, inWaitMilliSec);
        mNextEventIdx = 0;
        QCASSERT(mEpollEventCount <= theEventCount);
        return (mEpollEventCount >= 0 ? mEpollEventCount :
            (errno > 0 ? -errno : (errno == 0 ? mEpollEventCount : errno)));
    }
    bool Next(
        int&   outOpType,
        void*& outUserDataPtr)
    {
        if (mNextEventIdx >= mEpollEventCount) {
            return false;
        }
        QCASSERT(mEventsPtr);
        outOpType      = FdPollMask(mEventsPtr[mNextEventIdx].events);
        outUserDataPtr = mEventsPtr[mNextEventIdx].data.ptr;
        mNextEventIdx++;
        return true;
    }

private:
    int                 mEpollFd;
    int                 mEpollEventCount;
    int                 mMaxEventCount;
    int                 mNextEventIdx;
    struct epoll_event* mEventsPtr;
    static bool         sForkedFlag;
    static int          sCtlErrors;
    static int          sLastCtlOp;
    static int          sLastCtlError;

    int EPollEventMask(
        int inOpType)
    {
        int theRet = 0;
        if ((inOpType & kOpTypeIn) != 0) {
            theRet += EPOLLIN;
        }
        if ((inOpType & kOpTypeOut) != 0) {
            theRet += EPOLLOUT;
        }
        if ((inOpType & kOpTypePri) != 0) {
            theRet += EPOLLPRI;
        }
        return theRet;
    }
    int FdPollMask(
        int inFlags)
    {
        int theRet = 0;
        if ((inFlags & EPOLLIN) != 0) {
            theRet += kOpTypeIn;
        }
        if ((inFlags & EPOLLOUT) != 0) {
            theRet += kOpTypeOut;
        }
        if ((inFlags & EPOLLPRI) != 0) {
            theRet += kOpTypePri;
        }
        if ((inFlags & EPOLLERR) != 0) {
            theRet += kOpTypeError;
        }
        if ((inFlags & EPOLLHUP) != 0) {
            theRet += kOpTypeHup;
        }
        return theRet;
    }
    int Ctl(
        int   inEpollOp,
        Fd    inFd,
        int   inOpType,
        void* inUserDataPtr)
    {
        if (inFd < 0) {
            return EBADF;
        }
        if (mEpollFd < 0) {
            return EFAULT;
        }
        // Clear event to make valgrind on 32 bit platform happy.
        struct epoll_event theEpollEvent = {0};
        theEpollEvent.data.ptr = inUserDataPtr;
        theEpollEvent.events   = EPollEventMask(inOpType);
        if (! epoll_ctl(mEpollFd, inEpollOp, inFd, &theEpollEvent)) {
            return 0;
        }
        // Looks like fork() randomly screws kernell epoll vector.
        // epoll_ctl() starts returning various errors.
        // For now assume that the fd is removed from epoll vector. 
        if (! sForkedFlag) {
            return ((errno ? errno : EFAULT) & ~kEpollFailureAfterFork);
        }
        sCtlErrors++;
        sLastCtlError = errno;
        sLastCtlOp    = inEpollOp;
        if (inEpollOp != EPOLL_CTL_DEL) {
            return ((errno ? errno : EFAULT) | kEpollFailureAfterFork);
        }
        return 0;
    }
    static void PrepareToFork()
        { sForkedFlag = true; }
    static bool InitAtFork()
    {
        sForkedFlag = false;
        if (pthread_atfork(&PrepareToFork, 0, 0)) {
            abort();
            return sForkedFlag;
        }
        return sForkedFlag;
    }

private:
    Impl(
        const Impl& inImpl);
    Impl operator=(
        const Impl& inImpl);
};

bool QCFdPoll::Impl::sForkedFlag(QCFdPoll::Impl::InitAtFork());
int  QCFdPoll::Impl::sCtlErrors(0);
int  QCFdPoll::Impl::sLastCtlOp(0);
int  QCFdPoll::Impl::sLastCtlError(0);

#else /* QC_OS_NAME_LINUX */
/* #ifndef QC_OS_NAME_LINUX */

#include <poll.h>

class QCFdPoll::Impl
{
public:
    Impl()
        : mFdMap(),
          mPollVecPtr(0),
          mPollVecSize(0),
          mFdCount(0),
          mHolesCnt(0),
          mNextIdx(0),
          mLastIdx(0),
          mDummyFd(open("/dev/null", O_RDONLY)),
          mDoCompactionFlag(false)
    {
        fcntl(mDummyFd, F_SETFD, FD_CLOEXEC);
    }
    ~Impl()
    {
        Impl::Close();
    }
    int Close()
    {
        int theRet = 0;
        if (mDummyFd >= 0) {
            if (close(mDummyFd)) {
                theRet = errno;
            }
            mDummyFd = -1;
        }
        delete [] mPollVecPtr;
        mPollVecPtr       = 0;
        mPollVecSize      = 0;
        mFdCount          = 0;
        mHolesCnt         = 0;
        mNextIdx          = 0;
        mLastIdx          = 0;
        mDoCompactionFlag = false;
        return theRet;
    }
    int Add(
        Fd    inFd,
        int   inOpType,
        void* inUserDataPtr)
    {
        if (inFd < 0) {
            return EBADF;
        }
        QCASSERT(int(mFdMap.size()) == mFdCount && mHolesCnt >= 0);
        FdMap::iterator const theIt = mFdMap.find(inFd);
        if (theIt == mFdMap.end()) {
            Alloc(inFd, inOpType, inUserDataPtr);
            return 0;
        }
        if (! theIt->second.mHoleFlag) {
            return EEXIST;
        }
        mHolesCnt--; // Non 0 flags for this fd again.
        struct pollfd& theEntry = mPollVecPtr[theIt->second.mIdx];
        theEntry.fd      = inFd;
        theEntry.events  = PollEventMask(inOpType);
        theEntry.revents = 0;
        theIt->second.mUserDataPtr = inUserDataPtr;
        theIt->second.mHoleFlag    = false;
        QCASSERT(int(mFdMap.size()) == mFdCount && mHolesCnt >= 0);
        return 0;
    }
    int Set(
        Fd    inFd,
        int   inOpType,
        void* inUserDataPtr)
    {
        if (inFd < 0) {
            return EBADF;
        }
        QCASSERT(int(mFdMap.size()) == mFdCount && mHolesCnt >= 0);
        FdMap::iterator const theIt = mFdMap.find(inFd);
        if (theIt == mFdMap.end() || theIt->second.mHoleFlag) {
            return ENOENT;
        }
        struct pollfd& theEntry = mPollVecPtr[theIt->second.mIdx];
        theEntry.fd     = inFd;
        theEntry.events = PollEventMask(inOpType);
        // Do not reset revents -- these might be used by Next().
        theIt->second.mUserDataPtr = inUserDataPtr;
        return 0;
    }
    int Remove(
        Fd inFd)
    {
        if (inFd < 0) {
            return EBADF;
        }
        QCASSERT(int(mFdMap.size()) == mFdCount && mHolesCnt >= 0);
        FdMap::iterator const theIt = mFdMap.find(inFd);
        if (theIt == mFdMap.end() || theIt->second.mHoleFlag) {
            return ENOENT;
        }
        const int theIdx = theIt->second.mIdx;
        QCRTASSERT(theIdx >= 0 && theIdx < mFdCount);
        if (theIdx + 1 == mFdCount) {
            // Remove last entry.
            mFdCount--;
            if (mLastIdx > mFdCount) {
                mLastIdx = mFdCount;
            } 
            mFdMap.erase(theIt);
        } else {
            struct pollfd& theEntry = mPollVecPtr[theIdx];
            if (mDummyFd >= 0) {
                theEntry.fd = mDummyFd;
            }
            theEntry.revents = 0; // Do not process pending events.
            theEntry.events  = 0;
            theIt->second.mUserDataPtr = 0;
            theIt->second.mHoleFlag    = true;
            mHolesCnt++;
        }
        QCASSERT(int(mFdMap.size()) == mFdCount && mHolesCnt >= 0);
        return 0;
    }
    int Poll(
        int /* inMaxEventCountHint */,
        int inWaitMilliSec)
    {
        mNextIdx = 0;
        mLastIdx = 0;
        Compact();
        const int theRet = poll(mPollVecPtr, mFdCount, inWaitMilliSec);
        if (theRet > 0) {
            mLastIdx = mFdCount;
        }
        return (theRet >= 0 ? theRet :
            (errno > 0 ? -errno : (errno == 0 ? -1 : errno)));
    }
    bool Next(
        int&   outOpType,
        void*& outUserDataPtr)
    {
        for ( ; mNextIdx < mLastIdx; mNextIdx++) {
            if (mPollVecPtr[mNextIdx].revents == 0) {
                continue;
            }
            if (mPollVecPtr[mNextIdx].fd == mDummyFd) {
                // Ignore events on dummy fd.
                mDoCompactionFlag = true;
            } else {
                struct pollfd&              theEntry = mPollVecPtr[mNextIdx];
                FdMap::const_iterator const theIt    = mFdMap.find(theEntry.fd);
                QCRTASSERT(theIt != mFdMap.end());
                if (theIt->second.mHoleFlag) {
                    mDoCompactionFlag = true;
                } else {
                    outOpType      = FdPollMask(theEntry.revents);
                    outUserDataPtr = theIt->second.mUserDataPtr;
                    mNextIdx++;
                    return true;
                }
            }
        }
        outOpType      = 0;
        outUserDataPtr = 0;
        return false;
    }

private:
    struct FdMapEnry
    {
        int   mIdx;
        bool  mHoleFlag;
        void* mUserDataPtr;
    };
    typedef std::map<Fd, FdMapEnry
#ifdef QC_USE_BOOST
        , std::less<Fd>,
        boost::fast_pool_allocator<std::pair<const Fd, FdMapEnry> >
#endif
    > FdMap;

    FdMap          mFdMap;
    struct pollfd* mPollVecPtr;
    int            mPollVecSize;
    int            mFdCount;
    int            mHolesCnt;
    int            mNextIdx;
    int            mLastIdx;
    int            mDummyFd;
    bool           mDoCompactionFlag;

    void Alloc(
        Fd    inFd,
        int   inOpType,
        void* inUserDataPtr)
    {
        if (mFdCount >= mPollVecSize) {
            const int kQuantum = 256;
            struct pollfd* const thePtr =
                new struct pollfd[mPollVecSize + kQuantum];
            memcpy(thePtr, mPollVecPtr, mFdCount * sizeof(thePtr[0]));
            delete [] mPollVecPtr;
            mPollVecPtr = thePtr;
            mPollVecSize += kQuantum;
        }
        FdMapEnry& theEntry = mFdMap[inFd];
        theEntry.mIdx         = mFdCount;
        theEntry.mUserDataPtr = inUserDataPtr;
        theEntry.mHoleFlag    = false;
        struct pollfd& thePollEntry = mPollVecPtr[theEntry.mIdx];
        thePollEntry.fd      = inFd;
        thePollEntry.events  = PollEventMask(inOpType);
        thePollEntry.revents = 0;
        mFdCount++;
    }
    int PollEventMask(
        int inOpType)
    {
        int theRet = 0;
        if ((inOpType & kOpTypeIn) != 0) {
            theRet += POLLIN;
        }
        if ((inOpType & kOpTypeOut) != 0) {
            theRet += POLLOUT;
        }
        if ((inOpType & kOpTypePri) != 0) {
            theRet += POLLPRI;
        }
        return theRet;
    }
    int FdPollMask(
        int inFlags)
    {
        int theRet = 0;
        if ((inFlags & POLLIN) != 0) {
            theRet += kOpTypeIn;
        }
        if ((inFlags & POLLOUT) != 0) {
            theRet += kOpTypeOut;
        }
        if ((inFlags & POLLPRI) != 0) {
            theRet += kOpTypePri;
        }
        if ((inFlags & (POLLERR | POLLNVAL)) != 0) {
            theRet += kOpTypeError;
        }
        if ((inFlags & POLLHUP) != 0) {
            theRet += kOpTypeHup;
        }
        return theRet;
    }
    void Compact()
    {
        QCASSERT(int(mFdMap.size()) == mFdCount && mHolesCnt >= 0);
        const int kMaxHolesCnt = 16;
        if (mHolesCnt <= kMaxHolesCnt && ! mDoCompactionFlag) {
            return;
        }
        int k = 0;
        for (int i = 0; i < mFdCount; i++) {
            if (mPollVecPtr[i].events == 0) {
                continue;
            }
            if (k != i) {
                mPollVecPtr[k] = mPollVecPtr[i];
                mFdMap[mPollVecPtr[k].fd].mIdx = k;
            }
            k++;
        }
        for (FdMap::iterator theIt = mFdMap.begin();
                theIt != mFdMap.end(); ) {
            if (theIt->second.mHoleFlag) {
                mFdMap.erase(theIt++);
            } else {
                ++theIt;
            }
        }
        mFdCount = k;
        mHolesCnt = 0;
        mDoCompactionFlag = false;
        QCASSERT(int(mFdMap.size()) == mFdCount && mHolesCnt >= 0);
    }
private:
    Impl(
        const Impl& inImpl);
    Impl operator=(
        const Impl& inImpl);
};

#endif

QCFdPoll::QCFdPoll()
    : mImpl(*new(Impl))
{}

QCFdPoll::~QCFdPoll()
{
    delete &mImpl;
}

    int
QCFdPoll::Add(
    QCFdPoll::Fd inFd,
    int          inOpType,
    void*        inUserDataPtr)
{
    return mImpl.Add(inFd, inOpType, inUserDataPtr);
}

    int
QCFdPoll::Set(
    QCFdPoll::Fd inFd,
    int          inOpType,
    void*        inUserDataPtr)
{
    return mImpl.Set(inFd, inOpType, inUserDataPtr);
}

    int
QCFdPoll::Poll(
    int inMaxEventCountHint,
    int inWaitMilliSec)
{
    return mImpl.Poll(inMaxEventCountHint, inWaitMilliSec);
}

    bool
QCFdPoll::Next(
    int&   outOpType,
    void*& outUserDataPtr)
{
    return mImpl.Next(outOpType, outUserDataPtr);
}

    int
QCFdPoll::Remove(
    QCFdPoll::Fd inFd)
{
    return mImpl.Remove(inFd);
}

    int
QCFdPoll::Close()
{
    return mImpl.Close();
}
