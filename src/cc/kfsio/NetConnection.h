//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/14
// Author: Sriram Rao
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

#ifndef _LIBIO_NETCONNECTION_H
#define _LIBIO_NETCONNECTION_H

#include "KfsCallbackObj.h"
#include "event.h"
#include "IOBuffer.h"
#include "TcpSocket.h"
#include "common/StdAllocator.h"
#include "qcdio/QCDLList.h"

#include <time.h>
#include <errno.h>

#include <list>
#include <boost/shared_ptr.hpp>

namespace KFS
{
using std::list;
using std::string;

class NetManager;
///
/// \file NetConnection.h
/// \brief A network connection uses TCP sockets for doing I/O.
///
/// A network connection contains a socket and data in buffers.
/// Whenever data is read from the socket it is held in the "in"
/// buffer; whenever data needs to be written out on the socket, that
/// data should be dropped into the "out" buffer and it will
/// eventually get sent out.
///

///
/// \class NetConnection
/// A net connection contains an underlying socket and is associated
/// with a KfsCallbackObj.  Whenever I/O is done on the socket (either
/// for read or write) or when an error occurs (such as the remote
/// peer closing the connection), t;he associated KfsCallbackObj is
/// called back with an event notification.
///
class NetConnection
{
public:
    typedef boost::shared_ptr<NetConnection> NetConnectionPtr;
    class Filter
    {
    public:
        virtual bool WantRead(const NetConnection& con) const = 0;
        virtual bool WantWrite(const NetConnection& con) const = 0;
        virtual int Read(
            NetConnection& con,
            TcpSocket&     sock,
            IOBuffer&      buffer,
            int            maxRead) = 0;
        virtual int Write(
            NetConnection& con,
            TcpSocket&     sock,
            IOBuffer&      buffer,
            bool&          outForceInvokeErrHandlerFlag) = 0;
        virtual void Close(NetConnection& con, TcpSocket* sock) = 0;
        virtual int Shutdown(NetConnection& con, TcpSocket& sock) = 0;
        virtual int Attach(NetConnection& /* con */,
            TcpSocket* /* sock */, string* /* outErrMsg */)
            { return 0; }
        virtual void Detach(NetConnection& /* con */,
            TcpSocket* /* sock */)
            {}
        virtual string GetAuthName() const
            { return string(); }
        virtual bool IsAuthFailure() const
            { return false; }
        virtual string GetErrorMsg() const
            { return string(); }
        virtual int GetErrorCode() const
            { return 0; }
        virtual bool IsShutdownReceived() const
            { return false; }
        virtual ~Filter()
            {}
        virtual int64_t GetSessionExpirationTime() const
            { return ((int64_t)time(0) + 365 * 24 * 60 * 60); }
        virtual bool RenewSession()
            { return false; }
        virtual string GetPeerId() const
            { return string(); }
        bool IsReadPending() const
            { return mReadPendingFlag; }
    protected:
        Filter()
            : mReadPendingFlag(false)
            {}
        bool mReadPendingFlag;
    };

    /// @param[in] sock TcpSocket on which I/O can be done
    /// @param[in] c KfsCallbackObj associated with this connection
    /// @param[in] listenOnly boolean that specifies whether this
    /// connection is setup only for accepting new connections.
    NetConnection(TcpSocket* sock, KfsCallbackObj* c,
        bool listenOnly = false, bool ownsSocket = true,
        Filter* filter = 0)
        : mNetManagerEntry(),
          mListenOnly(listenOnly),
          mOwnsSocket(ownsSocket),
          mTryWrite(false),
          mAuthFailureFlag(false),
          mCallbackObj(c),
          mSock(sock),
          mInBuffer(),
          mOutBuffer(),
          mInactivityTimeoutSecs(-1),
          maxReadAhead(-1),
          mPeerName(),
          mLstErrorMsg(),
          mFilter(filter) {
        assert(mSock);
    }

    Filter* GetFilter() const {
        return mFilter;
    }

    int SetFilter(Filter* filter, string* outErrMsg) {
        if (mFilter == filter) {
            return 0;
        }
        if (mFilter) {
            mFilter->Detach(*this, mSock);
        }
        mFilter = filter;
        return (mFilter ? mFilter->Attach(*this, mSock, outErrMsg) : 0);
    }

    ~NetConnection() {
        NetConnection::Close();
    }

    void SetOwningKfsCallbackObj(KfsCallbackObj* c) {
        mCallbackObj = c;
    }

    void EnableReadIfOverloaded() {
        mNetManagerEntry.EnableReadIfOverloaded();
        Update(false);
    }

    void SetDoingNonblockingConnect() {
        mNetManagerEntry.SetConnectPending(true);
        mTryWrite = false;
        Update(false);
    }

    /// If there is no activity on this socket for nsecs, then notify
    /// the owning object; maybe time to close the connection
    /// Setting new timeout resets the timer.
    void SetInactivityTimeout(int nsecs) {
        if (mInactivityTimeoutSecs != nsecs) {
            mInactivityTimeoutSecs = nsecs;
            Update();
        }
    }

    int GetInactivityTimeout() const {
        return mInactivityTimeoutSecs;
    }

    /// Callback for handling a read.  That is, select() thinks that
    /// data is available for reading. So, do something.  If system is
    /// overloaded and we don't have a special pass, leave the data in
    /// the buffer alone.
    void HandleReadEvent(int maxAcceptsPerRead = 1);

    /// Callback for handling a writing.  That is, select() thinks that
    /// data can be sent out.  So, do something.
    void HandleWriteEvent();

    /// Callback for handling errors.  That is, select() thinks that
    /// an error occurred.  So, do something.
    void HandleErrorEvent();

    /// Timeout call back.
    void HandleTimeoutEvent();

    /// Do we expect data to be read in?
    bool IsReadReady() const {
        return (maxReadAhead != 0);
    };

    /// Is data available for reading?
    bool HasPendingRead() const {
        return (! mInBuffer.IsEmpty());
    }

    /// Is data available for writing?
    bool IsWriteReady() const {
        return (! mOutBuffer.IsEmpty());
    }

    /// # of bytes available for writing(false),
    int GetNumBytesToWrite() const {
        return mOutBuffer.BytesConsumable();
    }

    /// Is the connection still good?
    bool IsGood() const {
        return (mSock && mSock->IsGood());
    }

    string GetPeerName() const {
        if (IsGood()) {
            if (mPeerName.empty()) {
                // mutable
                const_cast<NetConnection*>(this)->mPeerName =
                    mSock->GetPeerName();
            }
            return mPeerName;
        } else {
            return (mPeerName.empty() ? string("not connected") :
                ("was connected to " + mPeerName));
        }
    }

    string GetSockName() const {
        return (IsGood() ? mSock->GetSockName() : string("not connected"));
    }

    int GetPeerLocation(ServerLocation& loc) const {
        return (IsGood() ? mSock->GetPeerLocation(loc) : -ENOTCONN);
    }

    int GetSockLocation(ServerLocation& loc) const {
        return (IsGood() ? mSock->GetSockLocation(loc) : -ENOTCONN);
    }

    /// Enqueue data to be sent out.
    void Write(const IOBufferData &ioBufData, bool resetTimerFlag = true) {
        if (! ioBufData.IsEmpty()) {
            const bool resetTimer = resetTimerFlag && mOutBuffer.IsEmpty();
            mOutBuffer.Append(ioBufData);
            Update(resetTimer);
        }
    }

    /// Enqueue data to be sent out.
    void Write(IOBuffer* ioBuf) {
        const int numBytes = ioBuf ? ioBuf->BytesConsumable() : 0;
        if (numBytes > 0) {
            const bool resetTimer = mOutBuffer.IsEmpty();
            mOutBuffer.Move(ioBuf);
            Update(resetTimer);
        }
    }

    /// Enqueue data to be sent out.
    void Write(IOBuffer* ioBuf, int numBytes, bool resetTimerFlag = true) {
        const bool resetTimer = resetTimerFlag && mOutBuffer.IsEmpty();
        if (ioBuf && numBytes > 0 && mOutBuffer.Move(ioBuf, numBytes) > 0) {
            Update(resetTimer);
        }
    }

    /// Enqueue data to be sent out.
    void WriteCopy(const IOBuffer* ioBuf, int numBytes,
            bool resetTimerFlag = true) {
        const bool resetTimer = resetTimerFlag && mOutBuffer.IsEmpty();
        if (ioBuf && numBytes > 0 && mOutBuffer.Copy(ioBuf, numBytes) > 0) {
            Update(resetTimer);
        }
    }

    /// Enqueue data to be sent out.
    void Write(const char *data, int numBytes, bool resetTimerFlag = true) {
        const bool resetTimer = resetTimerFlag && mOutBuffer.IsEmpty();
        if (mOutBuffer.CopyIn(data, numBytes) > 0) {
            Update(resetTimer);
        }
    }

    bool CanStartFlush() const {
        return (mTryWrite && IsWriteReady() && IsGood());
    }

    /// If there is any data to be sent out, start the send.
    void StartFlush() {
        if (CanStartFlush()) {
            HandleWriteEvent();
        }
    }

    string GetErrorMsg() const;

    bool IsAuthFailure() const {
        const_cast<NetConnection*>(this)->mAuthFailureFlag =
            mAuthFailureFlag ||
            (mFilter && mFilter->IsAuthFailure());
        return mAuthFailureFlag;
    }

    int GetSocketError() const {
        return (mSock ? mSock->GetSocketError() : 0);
    }

    /// Close the connection.
    void Close(bool clearOutBufferFlag = true) {
        if (mFilter) {
            mFilter->Close(*this, mSock);
            return;
        }
        if (! mSock) {
            return;
        }
        // To avoid race with file descriptor number re-use by the OS,
        // remove the socket from poll set first, then close the socket.
        TcpSocket* const sock = mOwnsSocket ? mSock : 0;
        mSock = 0;
        // Clear data that can not be sent, but keep input data if any.
        if (clearOutBufferFlag) {
            mOutBuffer.Clear();
        }
        Update();
        if (sock) {
            if (mNetManagerEntry.IsPendingClose()) {
                // Keep fd open, will be closed by pending update.
                *sock = TcpSocket();
            } else {
                sock->Close();
            }
            delete sock;
        }
    }

    int Shutdown();

    void StartListening(bool nonBlockingAccept = false) {
        if (! mSock) {
            return;
        }
        if (! mListenOnly || mSock->StartListening(nonBlockingAccept) != 0) {
            Close();
        }
    }

    int GetNumBytesToRead() const {
        return mInBuffer.BytesConsumable();
    }

    /// Set max read ahead.
    /// @param[in] read ahead amount, < 0 -- unlimited.
    void SetMaxReadAhead(int readAhead) {
        const bool update = (maxReadAhead != 0) != (readAhead != 0);
        maxReadAhead = readAhead;
        if (update) {
            Update(false);
        }
    }

    void DiscardRead() {
        const bool resetTimer = ! mInBuffer.IsEmpty();
        mInBuffer.Clear();
        Update(resetTimer);
    }

    void DiscardWrite() {
        mOutBuffer.Clear();
        Update();
    }

    IOBuffer& GetInBuffer() {
        return mInBuffer;
    }

    // StartFlush() or Flush() must called, to initial buffer send, if something
    // gets written into the buffer externally, rather than using Write()
    // methods the above.
    IOBuffer& GetOutBuffer() {
        return mOutBuffer;
    }

    void Flush(bool resetTimerFlag = true) {
        if (CanStartFlush()) {
            Update(resetTimerFlag);
        }
    }

    bool WantRead() const {
        return (mFilter ? mFilter->WantRead(*this) : IsReadReady());
    }

    bool WantWrite() const {
        return (mFilter ? mFilter->WantWrite(*this) : IsWriteReady());
    }

    time_t TimeNow() const
        { return mNetManagerEntry.TimeNow(); }

    bool IsReadPending() const
        { return (mFilter && IsReadReady() && mFilter->IsReadPending()); }

    class NetManagerEntry
    {
    public:
        typedef list<NetConnectionPtr,
            StdFastAllocator<NetConnectionPtr> > List;
        typedef QCDLListOp<NetManagerEntry, 0>   PendingReadList;

        NetManagerEntry()
            : mIn(false),
              mOut(false),
              mAdded(false),
              mEnableReadIfOverloaded(false),
              mConnectPending(false),
              mPendingUpdateFlag(false),
              mPendingCloseFlag(false),
              mPendingResetTimerFlag(false),
              mFd(-1),
              mWriteByteCount(0),
              mTimerWheelSlot(-1),
              mExpirationTime(-1),
              mNetManager(0),
              mListIt()
            { PendingReadList::Init(*this); }
        ~NetManagerEntry()
            { PendingReadList::Remove(*this); }
        void EnableReadIfOverloaded()     { mEnableReadIfOverloaded  = true; }
        void SetConnectPending(bool flag) { mConnectPending = flag; }
        bool IsConnectPending() const     { return mConnectPending; }
        bool IsIn() const                 { return mIn; }
        bool IsOut() const                { return mOut; }
        bool IsAdded() const              { return mAdded; }
        bool IsPendingClose() const       { return mPendingCloseFlag; }
        time_t TimeNow() const;

    private:
        bool             mIn:1;
        bool             mOut:1;
        bool             mAdded:1;
        /// should we add this connection to the poll vector for reads
        /// even when the system is overloaded?
        bool             mEnableReadIfOverloaded:1;
        bool             mConnectPending:1;
        bool             mPendingUpdateFlag:1;
        bool             mPendingCloseFlag:1;
        bool             mPendingResetTimerFlag:1;
        int              mFd;
        int              mWriteByteCount;
        int              mTimerWheelSlot;
        time_t           mExpirationTime;
        NetManager*      mNetManager;
        List::iterator   mListIt;
        NetManagerEntry* mPrevPtr[1];
        NetManagerEntry* mNextPtr[1];

        void CloseSocket(NetConnection& con)
        {
            if (con.mSock) {
                con.mSock->Close();
                mAdded = false;
                mIn    = false;
                mOut   = false;
                mFd    = -1;
            } 
        }
        void SetPendingClose(const NetConnection& conn)
        {
            mPendingCloseFlag = conn.mOwnsSocket;
        }
        friend class NetManager;
        friend class QCDLListOp<NetManagerEntry, 0>;

    private:
        NetManagerEntry(const NetManagerEntry&);
        NetManagerEntry operator=(const NetManagerEntry&);
    };
    NetManagerEntry* GetNetManagerEntry() {
        return &mNetManagerEntry;
    }
    const NetManagerEntry* GetNetManagerEntry() const {
        return &mNetManagerEntry;
    }
    void Update(bool resetTimer = true);

private:
    NetManagerEntry mNetManagerEntry;
    const bool      mListenOnly:1;
    const bool      mOwnsSocket:1;
    bool            mTryWrite:1;
    bool            mAuthFailureFlag:1;
    /// KfsCallbackObj that will be notified whenever "events" occur.
    KfsCallbackObj* mCallbackObj;
    /// Socket on which I/O will be done.
    TcpSocket*      mSock;
    /// Buffer that contains data read from the socket
    IOBuffer        mInBuffer;
    /// Buffer that contains data that should be sent out on the socket.
    IOBuffer        mOutBuffer;
    /// When was the last activity on this connection
    /// # of bytes from the out buffer that should be sent out.
    int             mInactivityTimeoutSecs;
    int             maxReadAhead;
    string          mPeerName;
    string          mLstErrorMsg;
    Filter*         mFilter;

    friend class NetManagerEntry;
private:
    // No copies.
    NetConnection(const NetConnection&);
    NetConnection& operator=(const NetConnection&);
};

typedef NetConnection::NetConnectionPtr NetConnectionPtr;


}
#endif // LIBIO_NETCONNECTION_H
