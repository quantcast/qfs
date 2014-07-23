//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/14
// Author: Sriram Rao
//         Mike Ovsiannikov -- re-implement by separating poll/select/epoll
//         os specific logic, implement timer wheel to get rid of linear
//         connection list scans on every event, add Timer class.
//
// Copyright 2008-2012 Quantcast Corp.
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
// Generic network io event loop implementation.
//
//----------------------------------------------------------------------------

#include <cerrno>
#include <limits>
#include <unistd.h>
#include <fcntl.h>

#include "NetManager.h"
#include "TcpSocket.h"
#include "ITimeout.h"

#include "common/MsgLogger.h"
#include "qcdio/QCFdPoll.h"
#include "qcdio/QCUtils.h"
#include "qcdio/QCMutex.h"
#include "qcdio/qcstutils.h"

namespace KFS
{
using std::min;
using std::max;
using std::numeric_limits;

class NetManager::Waker
{
public:
    Waker()
        : mMutex(),
          mWritten(0),
          mSleepingFlag(false),
          mWakeFlag(false)
    {
        const int res = pipe(mPipeFds);
        if (res < 0) {
            perror("pipe");
            mPipeFds[0] = -1;
            mPipeFds[1] = -1;
            abort();
            return;
        }
        fcntl(mPipeFds[0], F_SETFL, O_NONBLOCK);
        fcntl(mPipeFds[1], F_SETFL, O_NONBLOCK);
        fcntl(mPipeFds[0], F_SETFD, FD_CLOEXEC);
        fcntl(mPipeFds[1], F_SETFD, FD_CLOEXEC);
    }
    ~Waker() { Waker::Close(); }
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
                const int err = errno;
                KFS_LOG_STREAM_ERROR << "wakeup: write: " <<
                    res << " " << QCUtils::SysError(err) <<
                KFS_LOG_EOM;
            }
        }
    }
    int GetFd() const { return mPipeFds[0]; }
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
    int     mPipeFds[2];
    bool    mSleepingFlag;
    bool    mWakeFlag;

private:
   Waker(const Waker&);
   Waker& operator=(const Waker&);
};

NetManager::NetManager(int timeoutMs)
    : mRemove(),
      mTimerWheelBucketItr(mRemove.end()),
      mCurConnection(0),
      mCurTimerWheelSlot(0),
      mConnectionsCount(0),
      mDiskOverloaded(false),
      mNetworkOverloaded(false),
      mIsOverloaded(false),
      mRunFlag(true),
      mShutdownFlag(false),
      mTimerRunningFlag(false),
      mPollFlag(false),
      mTimeoutMs(timeoutMs),
      mStartTime(time(0)),
      mNow(mStartTime),
      mMaxOutgoingBacklog(0),
      mNumBytesToSend(0),
      mTimerOverrunCount(0),
      mTimerOverrunSec(0),
      mMaxAcceptsPerRead(1),
      mPoll(*(new QCFdPoll())),
      mWaker(*(new Waker())),
      mPollEventHook(0),
      mPendingReadList(),
      mPendingUpdate(),
      mCurTimeoutHandler(0),
      mEpollError()
{
    TimeoutHandlers::Init(mTimeoutHandlers);
    mPendingUpdate.reserve(1 << 10);
}

NetManager::~NetManager()
{
    NetManager::CleanUp();
    assert(! PendingReadList::IsInList(mPendingReadList));
    delete &mPoll;
    delete &mWaker;
}

void
NetManager::AddConnection(const NetConnectionPtr& conn)
{
    if (mShutdownFlag) {
        conn->HandleErrorEvent();
        return;
    }
    NetConnection::NetManagerEntry* const entry =
        conn->GetNetManagerEntry();
    if (! entry) {
        return;
    }
    if (entry->mNetManager && entry->mNetManager != this) {
        KFS_LOG_STREAM_FATAL <<
            "attempt to add connection to different net manager" <<
        KFS_LOG_EOM;
        MsgLogger::Stop();
        abort();
    }
    if (! entry->mAdded) {
        entry->mTimerWheelSlot = kTimerWheelSize;
        entry->mListIt = mTimerWheel[kTimerWheelSize].insert(
            mTimerWheel[kTimerWheelSize].end(), conn);
        mConnectionsCount++;
        assert(mConnectionsCount > 0);
        entry->mAdded = true;
        entry->mNetManager = this;
        if (mPollEventHook) {
            mPollEventHook->Add(*this, *conn);
        }
    }
    conn->Update();
}

void
NetManager::RegisterTimeoutHandler(ITimeout* handler)
{
    if (handler) {
        TimeoutHandlers::PushBack(mTimeoutHandlers, *handler);
    }
}

void
NetManager::UnRegisterTimeoutHandler(ITimeout* handler)
{
    if (! handler) {
        return;
    }
    if (mCurTimeoutHandler == handler) {
        mCurTimeoutHandler = &ITimeout::List::GetNext(*handler);
        if (mCurTimeoutHandler == TimeoutHandlers::Front(mTimeoutHandlers)) {
            mCurTimeoutHandler = 0;
        }
    }
    TimeoutHandlers::Remove(mTimeoutHandlers, *handler);
}

inline void
NetManager::UpdateTimer(NetConnection::NetManagerEntry& entry, int timeOut)
{
    assert(entry.mAdded);

    if (mShutdownFlag) {
        return;
    }
    int timerWheelSlot;
    if (timeOut < 0) {
        timerWheelSlot = kTimerWheelSize;
    } else if ((timerWheelSlot = mCurTimerWheelSlot +
            // When the timer is running the effective wheel size "grows" by 1:
            // leave (move) entries with timeouts >= kTimerWheelSize in (to) the
            // current slot.
            min((kTimerWheelSize - (mTimerRunningFlag ? 0 : 1)), timeOut)) >=
            kTimerWheelSize) {
        timerWheelSlot -= kTimerWheelSize;
    }
    // This method can be invoked from timeout handler.
    // Make sure  that the entry doesn't get moved to the end of the current
    // list, which can be traversed by the timer.
    if (timerWheelSlot != entry.mTimerWheelSlot) {
        if (mTimerWheelBucketItr == entry.mListIt) {
            ++mTimerWheelBucketItr;
        }
        mTimerWheel[timerWheelSlot].splice(
            mTimerWheel[timerWheelSlot].end(),
            mTimerWheel[entry.mTimerWheelSlot], entry.mListIt);
        entry.mTimerWheelSlot = timerWheelSlot;
    }
}

void
NetManager::Update(NetConnection::NetManagerEntry& entry, int fd,
    bool resetTimer)
{
    if (entry.mNetManager) {
        entry.mNetManager->UpdateSelf(entry, fd, resetTimer, false);
    }
}

inline static int
CheckFatalPollSysError(int err, const char* msg)
{
    if (! err) {
        return err;
    }
    if ((err & QCFdPoll::kEpollFailureAfterFork) == 0) {
        KFS_LOG_STREAM_FATAL << QCUtils::SysError(err, msg) << KFS_LOG_EOM;
        MsgLogger::Stop();
        abort();
        return err;
    }
    const int ret = err & ~QCFdPoll::kEpollFailureAfterFork;
    KFS_LOG_STREAM_ERROR << "epoll error: " << QCUtils::SysError(ret, msg) <<
    KFS_LOG_EOM;
    return ret;
}

void
NetManager::PollRemove(int fd)
{
    CheckFatalPollSysError(
        fd < 0 ? EINVAL : mPoll.Remove(fd),
        "failed to remove fd from poll set"
    );
}

void
NetManager::UpdateSelf(NetConnection::NetManagerEntry& entry, int fd,
    bool resetTimer, bool epollError)
{
    if (! entry.mAdded) {
        return;
    }
    assert(*entry.mListIt);
    NetConnection& conn = **entry.mListIt;
    if (mPollFlag) {
        if (0 <= entry.mFd && (fd < 0 || ! conn.IsGood())) {
            entry.SetPendingClose(conn);
        }
        entry.mPendingResetTimerFlag =
            resetTimer || entry.mPendingResetTimerFlag;
        if (! entry.mPendingUpdateFlag) {
            if ((size_t)mConnectionsCount < mPendingUpdate.size()) {
                KFS_LOG_STREAM_FATAL <<
                    "invalid pending update list"
                    " size: "        << mPendingUpdate.size() <<
                    " connections: " << mConnectionsCount <<
                KFS_LOG_EOM;
                MsgLogger::Stop();
                abort();
            }
            mPendingUpdate.push_back(&conn);
            entry.mPendingUpdateFlag = true;
        }
        return;
    }
    const bool pendingCloseFlag = entry.mPendingCloseFlag;
    entry.mPendingUpdateFlag     = false;
    entry.mPendingCloseFlag      = false;
    entry.mPendingResetTimerFlag = false;
    assert(fd >= 0 || ! conn.IsGood());
    // Always check if connection has to be removed: this method always
    // called before socket fd gets closed.
    if (! conn.IsGood() || fd < 0 || epollError) {
        PendingReadList::Remove(entry);
        if (entry.mFd >= 0) {
            PollRemove(entry.mFd);
            if (pendingCloseFlag) {
                // Create socket and close it, in order to update the socket fd
                // counter, as the counter wasn't decremented when the fd was
                // "detached" from the original socket by
                // NetConnection::Close().
                TcpSocket socket(entry.mFd);
                socket.Close();
            }
            entry.mFd = -1;
        }
        if (mTimerWheelBucketItr == entry.mListIt) {
            ++mTimerWheelBucketItr;
        }
        if (epollError) {
            assert(conn.IsGood());
            mEpollError.splice(mEpollError.end(),
                mTimerWheel[entry.mTimerWheelSlot], entry.mListIt);
            return;
        }
        assert(mConnectionsCount > 0 &&
            entry.mWriteByteCount >= 0 &&
            entry.mWriteByteCount <= mNumBytesToSend);
        entry.mAdded = false;
        mConnectionsCount--;
        mNumBytesToSend -= entry.mWriteByteCount;
        mRemove.splice(mRemove.end(),
            mTimerWheel[entry.mTimerWheelSlot], entry.mListIt);
        // Do not reset entry->mNetManager, it is an error to add connection to
        // a different net manager even after close.
        if (mPollEventHook) {
            mPollEventHook->Remove(*this, **entry.mListIt);
        }
        return;
    }
    if (&conn == mCurConnection) {
        // Defer all updates for the currently dispatched connection until the
        // end of the event dispatch loop.
        return;
    }
    // Update timer.
    if (resetTimer) {
        const int timeOut = conn.GetInactivityTimeout();
        if (timeOut >= 0) {
            entry.mExpirationTime = mNow + timeOut;
        }
        UpdateTimer(entry, timeOut);
    }
    // Update pending send.
    assert(entry.mWriteByteCount >= 0 &&
        entry.mWriteByteCount <= mNumBytesToSend);
    mNumBytesToSend -= entry.mWriteByteCount;
    entry.mWriteByteCount = max(0, conn.GetNumBytesToWrite());
    mNumBytesToSend += entry.mWriteByteCount;
    // Update poll set.
    const bool in  = (! mIsOverloaded || entry.mEnableReadIfOverloaded) &&
        conn.WantRead();
    const bool out = entry.mConnectPending || conn.WantWrite();
    if (in != entry.mIn || out != entry.mOut) {
        assert(fd >= 0);
        const int op =
            (in ? QCFdPoll::kOpTypeIn : 0) + (out ? QCFdPoll::kOpTypeOut : 0);
        if ((fd != entry.mFd || op == 0) && entry.mFd >= 0) {
            PollRemove(entry.mFd);
            entry.mFd = -1;
        }
        if (entry.mFd < 0) {
            if (op) {
                if (CheckFatalPollSysError(
                        mPoll.Add(fd, op, &conn),
                        "failed to add fd to poll set") == 0) {
                    entry.mFd = fd;
                } else {
                    UpdateSelf(entry, fd, false, true);
                    return; // Tail recursion
                }
            }
        } else {
            if (CheckFatalPollSysError(
                    mPoll.Set(fd, op, &conn),
                    "failed to change poll flags"
                    ) != 0) {
                UpdateSelf(entry, fd, false, true);
                return; // Tail recursion
            }
        }
        entry.mIn  = in  && entry.mFd >= 0;
        entry.mOut = out && entry.mFd >= 0;
    }
    if (conn.IsReadPending()) {
        PendingReadList::Insert(
            entry, PendingReadList::GetPrev(mPendingReadList));
    } else {
        PendingReadList::Remove(entry);
    }
}

void
NetManager::Wakeup()
{
    mWaker.Wakeup();
}

void
NetManager::MainLoop(
    QCMutex*                mutex                /* = 0 */,
    bool                    wakeupAndCleanupFlag /* = true */,
    NetManager::Dispatcher* dispatcher           /* = 0 */)
{
    QCStMutexLocker locker(mutex);

    mNow = time(0);
    time_t lastTimerTime = mNow;
    if (wakeupAndCleanupFlag) {
        CheckFatalPollSysError(
            mPoll.Add(mWaker.GetFd(), QCFdPoll::kOpTypeIn),
            "failed to add net waker's fd to the poll set"
        );
    } else {
        mRunFlag = true;
    }
    const int timerOverrunWarningTime(mTimeoutMs / (1000/2));
    while (mRunFlag) {
        const bool wasOverloaded = mIsOverloaded;
        CheckIfOverloaded();
        if (mIsOverloaded != wasOverloaded) {
            KFS_LOG_STREAM_INFO <<
                (mIsOverloaded ?
                    "System is now in overloaded state" :
                    "Clearing system overload state") <<
                " " << mNumBytesToSend << " bytes to send" <<
            KFS_LOG_EOM;
            // Turn on read only if returning from overloaded state.
            // Turn off read in the event processing loop if overloaded, and
            // read event is pending.
            // The "lazy" processing here is to reduce number of system calls.
            if (! mIsOverloaded) {
                for (int i = 0; i <= kTimerWheelSize; i++) {
                    for (List::iterator c = mTimerWheel[i].begin();
                            c != mTimerWheel[i].end(); ) {
                        assert(*c);
                        NetConnection& conn = **c;
                        ++c;
                        conn.Update(false);
                    }
                }
            }
        }
        if (dispatcher) {
            dispatcher->DispatchEnd();
        }
        const int timeout = (! PendingReadList::IsInList(mPendingReadList)
            && mWaker.Sleep()) ? mTimeoutMs : 0;
        const int fdCount = mConnectionsCount + 1;
        assert(mPendingUpdate.empty());
        mPollFlag = true;
        QCStMutexUnlocker unlocker(mutex);
        const int ret = mPoll.Poll(fdCount, timeout);
        if (ret < 0 && ret != -EINTR && ret != -EAGAIN) {
            KFS_LOG_STREAM_ERROR <<
                QCUtils::SysError(-ret, "poll error") <<
            KFS_LOG_EOM;
        }
        mWaker.Wake();
        unlocker.Lock();
        mPollFlag = false;
        const int64_t nowMs = ITimeout::NowMs();
        mNow = time_t(nowMs / 1000);
        for (PendingUpdate::const_iterator it = mPendingUpdate.begin();
                it != mPendingUpdate.end();
                ++it) {
            NetConnection& conn = **it;
            conn.Update(conn.GetNetManagerEntry()->mPendingResetTimerFlag);
        }
        mPendingUpdate.clear();
        if (dispatcher) {
            dispatcher->DispatchStart();
        }
        mCurTimeoutHandler = TimeoutHandlers::Front(mTimeoutHandlers);
        while (mCurTimeoutHandler) {
            ITimeout& cur = *mCurTimeoutHandler;
            mCurTimeoutHandler = &ITimeout::List::GetNext(cur);
            if (mCurTimeoutHandler == TimeoutHandlers::Front(mTimeoutHandlers)) {
                mCurTimeoutHandler = 0;
            }
            cur.TimerExpired(nowMs);
        }
        // Move pending read list into temporary list, as the pending read might
        // change as a result of event dispatch.
        NetManagerEntry pendingRead;
        PendingReadList::Insert(pendingRead, mPendingReadList);
        PendingReadList::Remove(mPendingReadList);
        /// Process poll events.
        int   op;
        void* ptr;
        while (mPoll.Next(op, ptr)) {
            if (op == 0 || ! ptr) {
                continue;
            }
            NetConnection& conn = *reinterpret_cast<NetConnection*>(ptr);
            if (! conn.GetNetManagerEntry()->mAdded) {
                // Skip stale event, the conection should be in mRemove list.
                continue;
            }
            // Defer update for this connection.
            mCurConnection = &conn;
            if (mPollEventHook) {
                mPollEventHook->Event(*this, conn, op);
            }
            const bool hupError = op == QCFdPoll::kOpTypeHup &&
                ! conn.WantRead() && ! conn.WantWrite();
            if (((op & (QCFdPoll::kOpTypeIn | QCFdPoll::kOpTypeHup)) != 0 ||
                    PendingReadList::IsInList(*conn.GetNetManagerEntry())) &&
                    conn.IsGood() && (! mIsOverloaded ||
                    conn.GetNetManagerEntry()->mEnableReadIfOverloaded)) {
                conn.HandleReadEvent(mMaxAcceptsPerRead);
            }
            if ((op & (QCFdPoll::kOpTypeOut | QCFdPoll::kOpTypeHup)) != 0 &&
                    conn.IsGood()) {
                conn.HandleWriteEvent();
            }
            if (((op & QCFdPoll::kOpTypeError) != 0 || hupError) &&
                    conn.IsGood()) {
                conn.HandleErrorEvent();
            }
            // Try to write, if the last write was sucessfull.
            conn.StartFlush();
            // Update the connection.
            mCurConnection = 0;
            conn.Update();
        }
        // Process connections with pending read (inside filter).
        while (PendingReadList::IsInList(pendingRead)) {
            NetManagerEntry& cur = PendingReadList::GetNext(pendingRead);
            PendingReadList::Remove(cur);
            NetConnection& conn = **cur.mListIt;
            conn.HandleReadEvent(mMaxAcceptsPerRead);
        }
        while (! mEpollError.empty()) {
            assert(mEpollError.front());
            NetConnection& conn = *mEpollError.front();
            assert(conn.IsGood());
            conn.HandleErrorEvent();
        }
        mRemove.clear();
        mNow = time(0);
        int slotCnt = min(int(kTimerWheelSize), int(mNow - lastTimerTime));
        if (lastTimerTime + timerOverrunWarningTime < mNow) {
            KFS_LOG_STREAM_INFO <<
                "timer overrun " << (mNow - lastTimerTime) <<
                " seconds detected" <<
            KFS_LOG_EOM;
            mTimerOverrunCount++;
            mTimerOverrunSec += mNow - lastTimerTime;
        }
        mTimerRunningFlag = true;
        while (slotCnt-- > 0) {
            List& bucket = mTimerWheel[mCurTimerWheelSlot];
            mTimerWheelBucketItr = bucket.begin();
            while (mTimerWheelBucketItr != bucket.end()) {
                assert(*mTimerWheelBucketItr);
                NetConnection& conn = **mTimerWheelBucketItr;
                assert(conn.IsGood());
                ++mTimerWheelBucketItr;
                NetConnection::NetManagerEntry& entry =
                    *conn.GetNetManagerEntry();
                const int timeOut = conn.GetInactivityTimeout();
                if (timeOut < 0) {
                    // No timeout, move it to the corresponding list.
                    UpdateTimer(entry, timeOut);
                } else if (entry.mExpirationTime <= mNow) {
                    conn.HandleTimeoutEvent();
                } else {
                    // Not expired yet, move to the new slot, taking into the
                    // account possible timer overrun.
                    UpdateTimer(entry,
                        slotCnt + int(entry.mExpirationTime - mNow));
                }
            }
            if (++mCurTimerWheelSlot >= kTimerWheelSize) {
                mCurTimerWheelSlot = 0;
            }
            mRemove.clear();
        }
        mTimerRunningFlag = false;
        lastTimerTime = mNow;
        mTimerWheelBucketItr = mRemove.end();
    }
    if (wakeupAndCleanupFlag) {
        CheckFatalPollSysError(
            mPoll.Remove(mWaker.GetFd()),
            "failed to removed net kicker's fd from poll set"
        );
        CleanUp();
    } else {
        mRunFlag = true;
    }
    if (dispatcher) {
        dispatcher->DispatchExit();
    }
}

void
NetManager::CheckIfOverloaded()
{
    if (mMaxOutgoingBacklog > 0) {
        if (! mNetworkOverloaded) {
            mNetworkOverloaded = mNumBytesToSend > mMaxOutgoingBacklog;
        } else if (mNumBytesToSend <= mMaxOutgoingBacklog / 2) {
            // network was overloaded and that has now cleared
            mNetworkOverloaded = false;
        }
    } else {
        mNetworkOverloaded = false;
    }
    mIsOverloaded = mDiskOverloaded || mNetworkOverloaded;
}

void
NetManager::ChangeDiskOverloadState(bool v)
{
    mDiskOverloaded = v;
}

void
NetManager::CleanUp(bool childAtForkFlag, bool onlyCloseFdFlag)
{
    mShutdownFlag = true;
    while (! TimeoutHandlers::IsEmpty(mTimeoutHandlers)) {
        TimeoutHandlers::PopFront(mTimeoutHandlers);
    }
    if (childAtForkFlag) {
        mPoll.Close();
        mWaker.Close();
    }
    for (int i = 0; i <= kTimerWheelSize; i++) {
        for (mTimerWheelBucketItr = mTimerWheel[i].begin();
                mTimerWheelBucketItr != mTimerWheel[i].end(); ) {
            NetConnection* const conn = mTimerWheelBucketItr->get();
            ++mTimerWheelBucketItr;
            if (conn) {
                if (childAtForkFlag) {
                    if (onlyCloseFdFlag) {
                        conn->GetNetManagerEntry()->CloseSocket(*conn);
                    } else {
                        conn->GetNetManagerEntry()->mAdded = false;
                    }
                }
                if (conn->IsGood()) {
                    conn->HandleErrorEvent();
                }
            }
        }
        assert((childAtForkFlag && onlyCloseFdFlag) || mTimerWheel[i].empty());
        mRemove.clear();
    }
    mTimerWheelBucketItr = mRemove.end();
}

void
NetManager::ChildAtFork(bool onlyCloseFdFlag)
{
    CleanUp(true, onlyCloseFdFlag);
}

inline const NetManager*
NetManager::GetNetManager(const NetConnection& conn)
{
    return conn.GetNetManagerEntry()->mNetManager;
}

inline time_t
NetManager::Timer::Handler::Now() const
{
    return GetNetManager(*mConn)->Now();
}

NetManager::Timer::Handler::Handler(NetManager& netManager, KfsCallbackObj& obj, int tmSec)
    : KfsCallbackObj(),
      mObj(obj),
      mStartTime(tmSec >= 0 ? netManager.Now() : 0),
      mSock(numeric_limits<int>::max()), // Fake fd, for IsGood()
      mConn(new NetConnection(&mSock, this, false, false))
{
    SET_HANDLER(this, &Handler::EventHandler);
    mConn->SetMaxReadAhead(0); // Do not add this to poll.
    mConn->SetInactivityTimeout(tmSec);
    netManager.AddConnection(mConn);
}

void
NetManager::Timer::Handler::SetTimeout(int tmSec)
{
    const int prevTm = mConn->GetInactivityTimeout();
    mStartTime = Now();
    if (prevTm != tmSec) {
        mConn->SetInactivityTimeout(tmSec); // Reset timer.
    } else {
        mConn->Update(); // Reset timer.
    }
}

time_t
NetManager::Timer::Handler::GetRemainingTime() const
{
    const int tmSec = mConn->GetInactivityTimeout();
    if (tmSec < 0) {
        return tmSec;
    }
    const time_t next = mStartTime + tmSec;
    const time_t now  = Now();
    return (next > now ? next - now : 0);
}

int
NetManager::Timer::Handler::EventHandler(int type, void* /* data */)
{
    switch (type) {
        case EVENT_NET_ERROR: // Invoked from net manager cleanup code.
            Cleanup();
            // Fall through
        case EVENT_INACTIVITY_TIMEOUT:
            mStartTime = Now();
            return mObj.HandleEvent(EVENT_INACTIVITY_TIMEOUT, 0);
        default:
            assert(! "unexpected event type");
    }
    return 0;
}

void
NetManager::Timer::Handler::Cleanup()
{
    mConn->Close();
    // Reset fd to prevent calling close().
    mSock = TcpSocket();
}

void
NetManager::Timer::Handler::ResetTimeout()
{
    if (mConn->GetInactivityTimeout() >= 0) {
        mStartTime = Now();
        mConn->Update();
    }
}

void
NetManager::Timer::Handler::ScheduleTimeoutNoLaterThanIn(int tmSec)
{
    if (tmSec < 0) {
        return;
    }
    const int    curTimeout = mConn->GetInactivityTimeout();
    const time_t now        = Now();
    if (curTimeout < 0 || now + tmSec < mStartTime + curTimeout) {
        mStartTime = now;
        if (curTimeout != tmSec) {
            mConn->SetInactivityTimeout(tmSec);
        } else {
            mConn->Update();
        }
    }
}
}
