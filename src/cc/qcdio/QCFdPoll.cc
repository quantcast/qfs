//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2009/03/16
// Author: Mike Ovsiannikov
//
// Copyright 2008-2011,2016 Quantcast Corporation. All rights reserved.
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
#include "QCMutex.h"
#include "qcstutils.h"
#include "qcdebug.h"

#include <cerrno>
#include <algorithm>

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

using std::min;

class QCFdPollImplBase
{
public:
    class Waker;
    QCFdPollImplBase(
        Waker* inWakerPtr)
        : mWakerPtr(inWakerPtr)
        {}
    ~QCFdPollImplBase()
        {}
    Waker* GetWakerPtr() const
        { return mWakerPtr; }
private:
    Waker* const mWakerPtr;
};

#ifdef QC_OS_NAME_SUNOS
#include <sys/filio.h>
#include <sys/devpoll.h>

class QCFdPoll::Impl : public QCFdPollImplBase
{
public:
    Impl(
        QCFdPollImplBase::Waker* inWakerPtr)
        : QCFdPollImplBase(inWakerPtr),
          mFdMap(),
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
        Impl::Close();
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
#include <sys/epoll.h>

class QCFdPoll::Impl : public QCFdPollImplBase
{
public:
    enum { kFdCountHint = 1 << 10 };

    Impl(
        QCFdPollImplBase::Waker* inWakerPtr)
        : QCFdPollImplBase(inWakerPtr),
          mEpollFd(epoll_create(kFdCountHint)),
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
        return (errno ? errno : EFAULT);
    }
private:
    Impl(
        const Impl& inImpl);
    Impl operator=(
        const Impl& inImpl);
};

#else /* QC_OS_NAME_LINUX */
/* #ifndef QC_OS_NAME_LINUX */

#include <poll.h>

class QCFdPoll::Impl : public QCFdPollImplBase
{
public:
    Impl(
        QCFdPollImplBase::Waker* inWakerPtr)
        : QCFdPollImplBase(inWakerPtr),
          mFdMap(),
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

class QCFdPollImplBase::Waker
{
public:
    Waker()
        : mMutex(),
          mWritten(0),
          mLastWriteError(0),
          mSleepingFlag(false),
          mWakeFlag(false)
    {
        const int res = pipe(mPipeFds);
        if (res < 0) {
            mPipeFds[0] = -1;
            mPipeFds[1] = -1;
            QCUtils::FatalError("pipe", errno);
            return;
        }
        fcntl(mPipeFds[0], F_SETFL, O_NONBLOCK);
        fcntl(mPipeFds[1], F_SETFL, O_NONBLOCK);
        fcntl(mPipeFds[0], F_SETFD, FD_CLOEXEC);
        fcntl(mPipeFds[1], F_SETFD, FD_CLOEXEC);
    }
    ~Waker()
        { Waker::Close(); }
    bool Sleep()
    {
        QCStMutexLocker lock(mMutex);
        mSleepingFlag = ! mWakeFlag;
        mWakeFlag = false;
        return mSleepingFlag;
    }
    int Wake()
    {
        QCStMutexLocker lock(mMutex);
        mSleepingFlag = false;
        while (mWritten > 0) {
            char buf[64];
            const int res = read(mPipeFds[0], buf, sizeof(buf));
            if (res > 0) {
                mWritten -= min(mWritten, res);
            } else {
                break;
            }
        }
        return (mWritten);
    }
    void Wakeup()
    {
        QCStMutexLocker lock(mMutex);
        mWakeFlag = true;
        if (mSleepingFlag && mWritten <= 0) {
            const char buf = 'k';
            const ssize_t res = write(mPipeFds[1], &buf, sizeof(buf));
            if (0 < res) {
                mWritten += res;
            } else {
                mLastWriteError = errno;
            }
        }
    }
    int GetFd() const
        { return mPipeFds[0]; }
    void Close()
    {
        for (int i = 0; i < 2; i++) {
            if (mPipeFds[i] >= 0) {
                close(mPipeFds[i]);
                mPipeFds[i] = -1;
            }
        }
    }
private:
    QCMutex mMutex;
    int     mWritten;
    int     mLastWriteError;
    int     mPipeFds[2];
    bool    mSleepingFlag;
    bool    mWakeFlag;

private:
    Waker(
        const Waker&);
    Waker& operator=(
        const Waker&);
};

    /* static */ QCFdPoll::Impl&
QCFdPoll::Create(
    bool inWakeableFlag)
{
    char* const thePtr = new char[sizeof(Impl) +
        (inWakeableFlag ? sizeof(Impl::Waker) : 0)];
    Impl& theImpl = *(new (thePtr) Impl(inWakeableFlag ?
        new (thePtr + sizeof(Impl)) Impl::Waker() : 0));
    if (inWakeableFlag) {
        const int theErr = theImpl.Add(
            theImpl.GetWakerPtr()->GetFd(),
            QCFdPoll::kOpTypeIn,
            theImpl.GetWakerPtr()
        );
        if (theErr) {
            QCUtils::FatalError("poll add waker fd", theErr);
        }
    }
    return theImpl;
}

QCFdPoll::QCFdPoll(
    bool inWakeableFlag)
    : mImpl(Create(inWakeableFlag))
{}

QCFdPoll::~QCFdPoll()
{
    Impl::Waker* const theWakerPtr = mImpl.GetWakerPtr();
    mImpl.~Impl();
    if (theWakerPtr) {
        theWakerPtr->~Waker();
    }
    delete [] reinterpret_cast<char*>(&mImpl);
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
    Impl::Waker* const theWakerPtr = mImpl.GetWakerPtr();
    const int theRet = mImpl.Poll(
        (theWakerPtr && 0 <= inMaxEventCountHint) ?
            inMaxEventCountHint + 1 : inMaxEventCountHint,
        (! theWakerPtr || theWakerPtr->Sleep()) ? inWaitMilliSec : 0);
    if (theWakerPtr) {
        theWakerPtr->Wake();
    }
    return theRet;
}

    bool
QCFdPoll::Next(
    int&   outOpType,
    void*& outUserDataPtr)
{
    bool theRetFlag;
    while ((theRetFlag = mImpl.Next(outOpType, outUserDataPtr)) &&
            outUserDataPtr && mImpl.GetWakerPtr() == outUserDataPtr)
        {}
    return theRetFlag;
}

    int
QCFdPoll::Remove(
    QCFdPoll::Fd inFd)
{
    return mImpl.Remove(inFd);
}

    bool
QCFdPoll::Wakeup()
{
    Impl::Waker* const theWakerPtr = mImpl.GetWakerPtr();
    if (! theWakerPtr) {
        return false;
    }
    theWakerPtr->Wakeup();
    return true;
}

    int
QCFdPoll::Close()
{
    // Do not remove waker's pipe fds from poll set.
    // Close poll set first, then close pipe fds, in order to allow to close
    // poll set from child process without affecting parent with linux epoll
    // implementation.
    const int theRet = mImpl.Close();
    Impl::Waker* const theWakerPtr = mImpl.GetWakerPtr();
    if (theWakerPtr) {
        theWakerPtr->Close();
    }
    return theRet;
}
