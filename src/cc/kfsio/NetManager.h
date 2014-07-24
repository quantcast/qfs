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
//
//----------------------------------------------------------------------------

#ifndef _LIBIO_NETMANAGER_H
#define _LIBIO_NETMANAGER_H

#include "NetConnection.h"
#include "ITimeout.h"

#include <list>
#include <vector>

class QCFdPoll;
class QCMutex;

namespace KFS
{
using std::list;
using std::vector;

///
/// \file NetManager.h
/// The net manager provides facilities for multiplexing I/O on network
/// connections.  It keeps a list of connections on which it has to
/// call select.  Whenever an "event" occurs on a connection (viz.,
/// read/write/error), it calls back the connection to handle the
/// event.
///
/// The net manager also provides support for timeout notification, and
/// connection inactivity timeout.
/// In the present implemenation the worst case timeout resolution is
/// mSelectTimeout.
///
//

class NetManager
{
public:
    class Dispatcher
    {
    public:
        virtual void DispatchStart() = 0;
        virtual void DispatchEnd()   = 0;
        virtual void DispatchExit()  = 0;
    protected:
        virtual ~Dispatcher()
            {}
    };
    typedef NetConnection::NetManagerEntry NetManagerEntry;

    NetManager(int timeoutMs = 1000);
    ~NetManager();
    /// Add a connection to the net manager's list of connections that
    /// are used for building poll vector.
    /// @param[in] conn The connection that should be added.
    void AddConnection(const NetConnectionPtr &conn);
    void RegisterTimeoutHandler(ITimeout *handler);
    void UnRegisterTimeoutHandler(ITimeout *handler);

    void SetBacklogLimit(int64_t v)
        { mMaxOutgoingBacklog = v; }
    void ChangeDiskOverloadState(bool v);

    ///
    /// This function never returns.  It builds a poll vector, calls
    /// select(), and then evaluates the result of select():  for
    /// connections on which data is I/O is possible---either for
    /// reading or writing are called back.  In the callback, the
    /// connections should take appropriate action.
    ///
    /// NOTE: When a connection is closed (such as, via a call to
    /// NetConnection::Close()), then it automatically falls out of
    /// the net manager's list of connections that are polled.
    ///
    void MainLoop(
        QCMutex*    mutex                = 0,
        bool        wakeupAndCleanupFlag = true,
        Dispatcher* dispatcher           = 0);
    void Wakeup();

    void Shutdown()
        { mRunFlag = false; }
    time_t GetStartTime() const
        { return mStartTime; }
    time_t Now() const
        { return mNow; }
    time_t UpTime() const
        { return (mNow - mStartTime); }
    bool IsRunning() const
        { return mRunFlag; }
    int64_t GetTimerOverrunCount() const
        { return mTimerOverrunCount; }
    int64_t GetTimerOverrunSec() const
        { return mTimerOverrunSec; }
    int GetMaxAcceptsPerRead() const
        { return mMaxAcceptsPerRead; }
    void SetMaxAcceptsPerRead(int maxAcceptsPerRead)
        { mMaxAcceptsPerRead = maxAcceptsPerRead <= 0 ? 1 : maxAcceptsPerRead; }
    void ChildAtFork(bool onlyCloseFdFlag = true);
    void UpdateTimeNow() { mNow = time(0); }
    int GetConnectionCount() const
        { return mConnectionsCount; }

    // Primarily for debugging, to simulate network failures.
    class PollEventHook
    {
    public:
        virtual void Add(NetManager& netMgr, NetConnection& conn)    {}
        virtual void Remove(NetManager& netMgr, NetConnection& conn) {}
        virtual void Event(
            NetManager& netMgr, NetConnection& conn, int& pollEvent) = 0;
    protected:
        PollEventHook()  {}
        virtual ~PollEventHook() {}
    };
    PollEventHook* SetPollEventHook(PollEventHook* hook = 0)
    {
        PollEventHook* const prev = mPollEventHook;
        mPollEventHook = hook;
        return prev;
    }
    // Use net manager's timer wheel, with no fd/socket.
    // Has about 100 bytes overhead.
    class Timer
    {
    public:
        Timer(NetManager& netManager, KfsCallbackObj& obj, int tmSec = -1)
            : mHandler(netManager, obj, tmSec)
            {}
        void RemoveTimeout()
            { SetTimeout(-1); }
        void SetTimeout(int tmSec)
            { mHandler.SetTimeout(tmSec); }
        void ResetTimeout()
            { mHandler.ResetTimeout(); }
        time_t GetRemainingTime() const
            { return mHandler.GetRemainingTime(); }
        time_t GetStartTime() const
            { return mHandler.mStartTime; }
        int GetTimeout() const
            { return mHandler.mConn->GetInactivityTimeout(); }
        void ScheduleTimeoutNoLaterThanIn(int tmSec)
            { mHandler.ScheduleTimeoutNoLaterThanIn(tmSec); }
        // Negative timeouts are infinite, always greater than non negative.
        static int MinTimeout(int tmL, int tmR)
            { return ((tmR < 0 || (tmL < tmR && tmL >= 0)) ? tmL : tmR); }

    private:
        struct Handler : public KfsCallbackObj
        {
            Handler(NetManager& netManager, KfsCallbackObj& obj, int tmSec);
            ~Handler()
                { Handler::Cleanup(); }
            void SetTimeout(int tmSec);
            time_t GetRemainingTime() const;
            int EventHandler(int type, void* data);
            void Cleanup();
            void ResetTimeout();
            void ScheduleTimeoutNoLaterThanIn(int tmSec);
            inline time_t Now() const;

            KfsCallbackObj&  mObj;
            time_t           mStartTime;
            TcpSocket        mSock;
            NetConnectionPtr mConn;
        private:
            Handler(const Handler&);
            Handler& operator=(const Handler&);
        };
        Handler mHandler;
    private:
        Timer(const Timer&);
        Timer& operator=(const Timer&);
    };

    /// Method used by NetConnection only.
    static void Update(NetManagerEntry& entry, int fd,
        bool resetTimer);
    static inline const NetManager* GetNetManager(const NetConnection& conn);
private:
    class Waker;
    typedef NetManagerEntry::List            List;
    typedef QCDLList<ITimeout>               TimeoutHandlers;
    typedef NetManagerEntry::PendingReadList PendingReadList;
    typedef vector<NetConnection*>           PendingUpdate;
    enum { kTimerWheelSize = (1 << 8) };

    List            mRemove;
    List::iterator  mTimerWheelBucketItr;
    NetConnection*  mCurConnection;
    int             mCurTimerWheelSlot;
    int             mConnectionsCount;
    /// when the system is overloaded--either because of disk or we
    /// have too much network I/O backlogged---we avoid polling fd's for
    /// read.  this causes back-pressure and forces the clients to
    /// slow down
    bool            mDiskOverloaded;
    bool            mNetworkOverloaded;
    bool            mIsOverloaded;
    volatile bool   mRunFlag;
    bool            mShutdownFlag;
    bool            mTimerRunningFlag;
    bool            mPollFlag;
    /// timeout interval specified in the call to select().
    const int       mTimeoutMs;
    const time_t    mStartTime;
    time_t          mNow;
    int64_t         mMaxOutgoingBacklog;
    int64_t         mNumBytesToSend;
    int64_t         mTimerOverrunCount;
    int64_t         mTimerOverrunSec;
    int             mMaxAcceptsPerRead;
    QCFdPoll&       mPoll;
    Waker&          mWaker;
    PollEventHook*  mPollEventHook;
    NetManagerEntry mPendingReadList;
    PendingUpdate   mPendingUpdate;
    /// Handlers that are notified whenever a call to select()
    /// returns.  To the handlers, the notification is a timeout signal.
    ITimeout*       mCurTimeoutHandler;
    ITimeout*       mTimeoutHandlers[1];
    List            mEpollError;
    List            mTimerWheel[kTimerWheelSize + 1];

    void CheckIfOverloaded();
    void CleanUp(bool childAtForkFlag = false, bool onlyCloseFdFlag = false);
    inline void UpdateTimer(NetManagerEntry& entry, int timeOut);
    void UpdateSelf(NetManagerEntry& entry, int fd,
        bool resetTimer, bool epollError);
    void PollRemove(int fd);
private:
    NetManager(const NetManager&);
    NetManager& operator=(const NetManager&);
};

}

#endif // _LIBIO_NETMANAGER_H
