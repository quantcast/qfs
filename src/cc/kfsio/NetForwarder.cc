//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/04/18
// Author: Mike Ovsiannikov
//
// Copyright 2015 Quantcast Corp.
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
// Network byte stream forwader / tcp proxy.
//
//
//----------------------------------------------------------------------------

#include "NetForwarder.h"

#include "NetManager.h"
#include "event.h"
#include "NetConnection.h"
#include "KfsCallbackObj.h"
#include "TcpSocket.h"
#include "Acceptor.h"

#include "qcdio/QCUtils.h"
#include "qcdio/QCDLList.h"
#include "qcdio/QCDebug.h"
#include "qcdio/qcutils.h"
#include "qcdio/qcstutils.h"

#include "common/Properties.h"

#include <errno.h>
#include <algorithm>

namespace KFS
{
using std::max;

class NetForwarder::Impl : public IAcceptorOwner
{
public:
    Impl(
        NetManager& inNetManager)
        : IAcceptorOwner(),
          mNetManager(inNetManager),
          mAcceptorPtr(0),
          mAcceptAddr(),
          mConnectAddr(),
          mMaxPendingBytes(128 << 10),
          mInactivityTimeout(5 * 60),
          mConnectionCount(0),
          mMaxConnections(8 << 10),
          mIpV6OnlyFlag(false)
        {}
    ~Impl()
        { Impl::Shutdown(); }
    int Start(
        const char*       inParametersPrefixPtr,
        const Properties& inParameters)
    {
        Shutdown();
        SetParameters(inParametersPrefixPtr, inParameters);
        if (! mAcceptAddr.IsValid() || ! mConnectAddr.IsValid()) {
            return -EINVAL;
        }
        const bool kBindOnlyFlag = false;
        mAcceptorPtr = new Acceptor(
            mNetManager, mAcceptAddr, mIpV6OnlyFlag, this, kBindOnlyFlag);
        if (! mAcceptorPtr->IsAcceptorStarted()) {
            delete mAcceptorPtr;
            mAcceptorPtr = 0;
            return -ENOTCONN;
        }
        return 0;
    }
    void SetParameters(
        const char*       inParametersPrefixPtr,
        const Properties& inParameters)
    {
        Properties::String theParamName;
        if (inParametersPrefixPtr) {
            theParamName.Append(inParametersPrefixPtr);
        }
        const size_t thePrefixLen = theParamName.GetSize();
        mMaxPendingBytes = inParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "maxPendingBytes"), mMaxPendingBytes);
        mInactivityTimeout = inParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "inactivityTimeoutSec"), mInactivityTimeout);
        mMaxConnections = inParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "maxConnections"), mMaxConnections);
        const bool kHexFormatFlag = false;
        ServerLocation theLocation;
        mAcceptAddr.FromString(inParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "listenOn"), mAcceptAddr.ToString()), kHexFormatFlag);
        mConnectAddr.FromString(inParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "connectTo"), mConnectAddr.ToString()), kHexFormatFlag);
    }
    virtual KfsCallbackObj* CreateKfsCallbackObj(
        NetConnectionPtr& inConnectionPtr)
    {
        if (! mNetManager.IsRunning() ||
                ! inConnectionPtr->IsGood() ||
                mMaxConnections <= mConnectionCount) {
            return 0;
        }
        Connection* const theConnPtr = new Connection(*this, inConnectionPtr);
        return theConnPtr->Connect();
    }
private:
    class Connection
    {
    public:
        typedef QCDLList<Connection> List;

        Connection(
            Impl&             inImpl,
            NetConnectionPtr& inConnectionPtr)
            : mImpl(inImpl),
              mAcceptConnectionPtr(inConnectionPtr),
              mConnectionPtr(),
              mAcceptHandler(*this, true),
              mConnectHandler(*this, false),
              mSocket(),
              mRecursionCount(0),
              mAcceptEofFlag(false),
              mConnectEofFlag(false),
              mDeleteFlagPtr(0)
        {
            List::Init(*this);
            mImpl.New(*this);
        }
        ~Connection()
        {
            if (mDeleteFlagPtr) {
                *mDeleteFlagPtr = true;
            }
            mImpl.Dispose(*this);
        }
        int EventHandler(
            bool  inAcceptFlag, 
            int   inType,
            void* inDataPtr)
        {
            return EventHandler(
                inAcceptFlag ? mAcceptConnectionPtr : mConnectionPtr,
                inAcceptFlag ? mConnectionPtr       : mAcceptConnectionPtr,
                inType,
                inDataPtr
            );
        }
        int EventHandler(
            const NetConnectionPtr& inFromPtr,
            const NetConnectionPtr& inToPtr,
            int                     inType,
            void*                   inDataPtr)
        {
            mRecursionCount++;
            bool theTryWriteFlag = false;
            switch (inType)
            {
                case EVENT_NET_READ: {
                    QCASSERT(inDataPtr == &inFromPtr->GetInBuffer());
                    IOBuffer& theDst = inToPtr->GetOutBuffer();
                    theTryWriteFlag = theDst.IsEmpty();
                    theDst.Move(&inFromPtr->GetInBuffer());
                    break;
                }
                case EVENT_NET_WROTE:
                    break;
                case EVENT_TIMEOUT:
                    inFromPtr->Close();
                    inToPtr->Close();
                    break;
                case EVENT_NET_ERROR:
                    if (! inFromPtr->IsGood()) {
                        inToPtr->Close();
                        break;
                    }
                    if (inFromPtr == mAcceptConnectionPtr) {
                        mAcceptEofFlag = true;
                    } else {
                        mConnectEofFlag = true;
                    }
                    break;
                default:
                    QCRTASSERT(! "NetForwarder: invalid event");
                    break;
            }
            if (mRecursionCount <= 1) {
                if (theTryWriteFlag) {
                    inToPtr->StartFlush();
                }
                if ((! inFromPtr->IsGood() || ! inToPtr->IsGood()) ||
                        (mAcceptEofFlag && mConnectEofFlag &&
                            ! inFromPtr->IsWriteReady() &&
                            ! inToPtr->IsWriteReady())) {
                    inFromPtr->Close();
                    inToPtr->Close();
                    delete this;
                    return 0;
                }
                const int theTimeout = mImpl.GetInactivityTimeout();
                mAcceptConnectionPtr->SetInactivityTimeout(theTimeout);
                mConnectionPtr->SetInactivityTimeout(theTimeout);
                SetMaxReadAhead(
                    mAcceptEofFlag, mAcceptConnectionPtr, mConnectionPtr);
                SetMaxReadAhead(
                    mConnectEofFlag, mConnectionPtr, mAcceptConnectionPtr);
            }
            mRecursionCount--;
            return 0;
        }
        KfsCallbackObj* Connect()
        {
            const bool kNonblockingConnectFlag = true;
            const int  theStatus = mSocket.Connect(
                mImpl.GetConnectAddress(), kNonblockingConnectFlag);
            if (theStatus != 0 && theStatus != -EINPROGRESS) {
                delete this;
                return 0;
            }
            const bool kListenOnlyFlag = false;
            const bool kOwnsSocketFlag = false;
            mConnectionPtr.reset(new NetConnection(
                &mSocket, &mConnectHandler, kListenOnlyFlag, kOwnsSocketFlag));
            QCStDeleteNotifier theDeleteNotifier(mDeleteFlagPtr);
            mImpl.GetNetManager().AddConnection(mConnectionPtr);
            if (theDeleteNotifier.IsDeleted()) {
                return 0;
            }
            const int theTimeout = mImpl.GetInactivityTimeout();
            mAcceptConnectionPtr->SetInactivityTimeout(theTimeout);
            if (theDeleteNotifier.IsDeleted()) {
                return 0;
            }
            mConnectionPtr->SetInactivityTimeout(theTimeout);
            if (theDeleteNotifier.IsDeleted()) {
                return 0;
            }
            return &mAcceptHandler;
        }
        void Delete()
        {
            if (mAcceptConnectionPtr) {
                mAcceptConnectionPtr->Close();
            }
            if (mConnectionPtr) {
                mConnectionPtr->Close();
            }
            delete this;
        }
    private:
        class Handler : public KfsCallbackObj
        {
        public:
            Handler(
                Connection& inConnection,
                bool        inAcceptFlag)
                : mConnection(inConnection),
                  mAcceptFlag(inAcceptFlag)
                { SET_HANDLER(this, &Handler::EventHandler); }
            int EventHandler(
                int   inType,
                void* inDataPtr)
            {
               return mConnection.EventHandler(
                    mAcceptFlag, inType, inDataPtr);
            }
        private:
            Connection& mConnection;
            const bool  mAcceptFlag;
        };
        Impl&            mImpl;
        NetConnectionPtr mAcceptConnectionPtr;
        NetConnectionPtr mConnectionPtr;
        Handler          mAcceptHandler;
        Handler          mConnectHandler;
        TcpSocket        mSocket;
        int              mRecursionCount;
        bool             mAcceptEofFlag;
        bool             mConnectEofFlag;
        bool*            mDeleteFlagPtr;
        Connection*      mPrevPtr[1];
        Connection*      mNextPtr[1];

        friend class QCDLListOp<Connection>;

        void SetMaxReadAhead(
            bool                    inEofFlag,
            const NetConnectionPtr& inFromPtr,
            const NetConnectionPtr& inToPtr)
        {
            if (inEofFlag) {
                inFromPtr->SetMaxReadAhead(0);
                return;
            }
            inFromPtr->SetMaxReadAhead(
                max(0, inToPtr->GetNumBytesToWrite() - mImpl.GetMaxPending()));
        }
                
    };
    NetManager&    mNetManager;
    Acceptor*      mAcceptorPtr;
    ServerLocation mAcceptAddr;
    ServerLocation mConnectAddr;
    int            mMaxPendingBytes;
    int            mInactivityTimeout;
    int            mConnectionCount;
    int            mMaxConnections;
    bool           mIpV6OnlyFlag;
    Connection*    mConnectionsPtr[1];

public:
    int GetMaxPending() const
        { return mMaxPendingBytes; }
    int GetInactivityTimeout() const
        { return mInactivityTimeout; }
    NetManager& GetNetManager()
        { return mNetManager; }
    const ServerLocation& GetConnectAddress() const
        { return mConnectAddr; }
    int GetMaxConnections() const
        { return mMaxConnections; }
    void New(
        Connection& inConnection)
    {
        mConnectionCount++;
        Connection::List::PushBack(mConnectionsPtr, inConnection);
    }
    void Dispose(
        Connection& inConnection)
    {
        mConnectionCount--;
        Connection::List::Remove(mConnectionsPtr, inConnection);
    }
    void Shutdown()
    {
        delete mAcceptorPtr;
        mAcceptorPtr = 0;
        Connection* thePtr;
        while ((thePtr = Connection::List::Back(mConnectionsPtr))) {
           thePtr->Delete();
        }
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

NetForwarder::NetForwarder(
    NetManager& inNetManager)
    : mImpl(*new Impl(inNetManager))
    {}

NetForwarder::~NetForwarder()
{
    delete &mImpl;
}

    int
NetForwarder::Start(
    const char*       inParametersPrefixPtr,
    const Properties& inParameters)
{
    return mImpl.Start(inParametersPrefixPtr, inParameters);
}

    void
NetForwarder::SetParameters(
    const char*       inParametersPrefixPtr,
    const Properties& inParameters)
{
    mImpl.SetParameters(inParametersPrefixPtr, inParameters);
}

    void
NetForwarder::Shutdown()
{
    mImpl.Shutdown();
}

}

