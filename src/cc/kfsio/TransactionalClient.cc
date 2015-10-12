//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/09/28
// Author:  Mike Ovsiannikov
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
// \brief Http like generic request response client with optional ssl transport.
//
//----------------------------------------------------------------------------

#include "TransactionalClient.h"

#include "NetManager.h"
#include "IOBuffer.h"
#include "NetConnection.h"
#include "SslFilter.h"
#include "KfsCallbackObj.h"
#include "event.h"

#include "common/kfsdecls.h"
#include "common/MsgLogger.h"
#include "common/Properties.h"

#include "qcdio/qcdebug.h"
#include "qcdio/QCUtils.h"
#include "qcdio/QCDLList.h"

#include <string>
#include <set>

namespace KFS
{

using std::set;
using std::string;

class TransactionalClient::Impl : public SslFilterVerifyPeer
{
public:
    Impl(
        NetManager& inNetManager)
        : SslFilterVerifyPeer(),
          mNetManager(inNetManager),
          mLocation(),
          mSslCtxPtr(0),
          mTimeout(20),
          mIdleTimeout(60),
          mHttpsHostNameFlag(true),
          mVerifyServerFlag(true),
          mServerName(),
          mPeerNames(),
          mSslCtxParameters(),
          mError(0),
          mErrorMsg()
    {
        mLocation.port = 443;
        List::Init(mInUseListPtr);
        List::Init(mIdleListPtr);
    }
    ~Impl()
    {
        Impl::Stop(-EIO, "shutdown");
        if (mSslCtxPtr) {
            SslFilter::FreeCtx(mSslCtxPtr);
        }
    }
    void Stop(
        int         inError,
        const char* inMsgPtr)
    {
        ClientSM* theClientPtr;
        while ((theClientPtr = List::PopFront(mIdleListPtr))) {
            theClientPtr->Stop(inError, inMsgPtr);
        }
        while ((theClientPtr = List::PopFront(mInUseListPtr))) {
            theClientPtr->Stop(inError, inMsgPtr);
        }
    }
    int SetServer(
        const ServerLocation& inLocation,
        bool                  inHttpsHostNameFlag)
    {
        if (inLocation == mLocation &&
                inHttpsHostNameFlag == mHttpsHostNameFlag) {
            return mError;
        }
        mHttpsHostNameFlag = inHttpsHostNameFlag;
        mLocation          = inLocation;
        if (mHttpsHostNameFlag) {
            UpdateHttpsPeerNames();
        }
        UpdateStatus();
        Stop(-EAGAIN, "server location changed");
        return mError;
    }
    int SetParameters(
        const char*       inParamsPrefixPtr,
        const Properties& inParameters,
        string*           inErrMsgPtr)
    {
        Properties::String theName;
        if (inParamsPrefixPtr) {
            theName.Append(inParamsPrefixPtr);
        }
        const size_t thePrefixSize = theName.GetSize();
        const string thePrevHostName = mLocation.hostname;
        mLocation.hostname = inParameters.getValue(
            theName.Truncate(thePrefixSize).Append("host"),
            mLocation.hostname
        );
        mLocation.port = inParameters.getValue(
            theName.Truncate(thePrefixSize).Append("port"),
            mLocation.port
        );
        mTimeout = inParameters.getValue(
            theName.Truncate(thePrefixSize).Append("timeout"),
            mTimeout
        );
        mIdleTimeout = inParameters.getValue(
            theName.Truncate(thePrefixSize).Append("idleTImeout"),
            mIdleTimeout
        );
        mHttpsHostNameFlag = inParameters.getValue(
            theName.Truncate(thePrefixSize).Append("httpsHostName"),
            mHttpsHostNameFlag ? 1 : 0
        ) != 0;
        mVerifyServerFlag = inParameters.getValue(
            theName.Truncate(thePrefixSize).Append("ssl.verifyPeer"),
            mVerifyServerFlag ? 1 : 0
        ) != 0;
        bool                            theStopFlag =
            thePrevHostName != mLocation.hostname;
        const Properties::String* const theValPtr   = inParameters.getValue(
            theName.Truncate(thePrefixSize).Append("peerNames"));
        if (theValPtr) {
            PeerNames thePeerNames;
            const char*       thePtr    = theValPtr->GetPtr();
            const char* const theEndPtr = thePtr + theValPtr->GetSize();
            while (thePtr < theEndPtr) {
                while (thePtr < theEndPtr && (*thePtr & 0xFF) <= ' ') {
                    ++thePtr;
                }
                const char* const theStartPtr = thePtr;
                while (thePtr < theEndPtr && ' ' < (*thePtr & 0xFF)) {
                    ++thePtr;
                }
                if (theStartPtr < thePtr) {
                    thePeerNames.insert(
                        string(theStartPtr, thePtr - theStartPtr));
                }
            }
            if (thePeerNames != mPeerNames) {
                theStopFlag = true;
                mPeerNames = thePeerNames;
            }
        } else if (mHttpsHostNameFlag && (mPeerNames.empty() || theStopFlag)) {
            UpdateHttpsPeerNames();
        }
        const Properties::String* const theSrvNamePtr = inParameters.getValue(
            theName.Truncate(thePrefixSize).Append("serverName"));
        if (theSrvNamePtr) {
            mServerName.assign(
                theSrvNamePtr->GetPtr(), theSrvNamePtr->GetSize());
        } else if (mHttpsHostNameFlag) {
            mServerName = mLocation.hostname;
        }
        theName.Truncate(thePrefixSize).Append("ssl.");
        Properties theSslCtxParameters;
        inParameters.copyWithPrefix(
            theName.GetPtr(), theName.GetSize(), theSslCtxParameters);
        if (theSslCtxParameters != mSslCtxParameters) {
            mSslCtxParameters.swap(theSslCtxParameters);
            if (mSslCtxPtr) {
                SslFilter::FreeCtx(mSslCtxPtr);
                mSslCtxPtr = 0;
            }
            if (! mSslCtxParameters.empty()) {
                const bool kServerFlag  = false;
                const bool kPskOnlyFlag = false;
                mSslCtxPtr = SslFilter::CreateCtx(
                    kServerFlag,
                    kPskOnlyFlag,
                    theName.GetPtr(),
                    mSslCtxParameters,
                    &mErrorMsg
                );
            }
            theStopFlag = true;
        }
        UpdateStatus();
        if (inErrMsgPtr) {
            *inErrMsgPtr = mErrorMsg;
        }
        if (theStopFlag) {
            Stop(-EAGAIN, "configuration changed");
        }
        return mError;
    }
    void Run(
        Transaction& inTransaction)
    {
        if (mError) {
            inTransaction.Error(mError, mErrorMsg.c_str());
            return;
        }
        ClientSM* theClientPtr = List::PopFront(mIdleListPtr);
        if (theClientPtr) {
            List::PushFront(mInUseListPtr, *theClientPtr);
            theClientPtr->Run(inTransaction);
            return;
        }
        if (mSslCtxPtr) {
            theClientPtr = new SslClientSM(*this);
        } else {
            theClientPtr = new ClientSM(*this);
        }
        List::PushFront(mInUseListPtr, *theClientPtr);
        theClientPtr->Connect(inTransaction);
    }
private:
    virtual bool Verify(
	string&       ioFilterAuthName,
        bool          inPreverifyOkFlag,
        int           inCurCertDepth,
        const string& inPeerName,
        int64_t       inEndTime,
        bool          inEndTimeValidFlag)
    {
        if (0 < inCurCertDepth) {
            return (inPreverifyOkFlag || ! mVerifyServerFlag);
        }
        const bool theRetFlag = ! mVerifyServerFlag ||
            (inPreverifyOkFlag && (mPeerNames.empty() ||
            mPeerNames.find(inPeerName) != mPeerNames.end()));
        KFS_LOG_STREAM(theRetFlag ? 
                MsgLogger::kLogLevelDEBUG :
                MsgLogger::kLogLevelERROR) <<
            "peer verify: " << (theRetFlag ? "ok" : "failed") <<
             " peer: "           << inPeerName <<
             " prev name: "      << ioFilterAuthName <<
             " preverify ok: "   << inPreverifyOkFlag <<
             " depth: "          << inCurCertDepth <<
             " end time: +"      << (inEndTime - mNetManager.Now()) <<
             " end time valid: " << inEndTimeValidFlag <<
        KFS_LOG_EOM;
        if (theRetFlag) {
            ioFilterAuthName = inPeerName;
        } else {
            ioFilterAuthName.clear();
        }
        return theRetFlag;
    }
    class ClientSM : public KfsCallbackObj
    {
    public:
        typedef QCDLList<ClientSM> List;
        ClientSM(
            Impl& inImpl)
            : KfsCallbackObj(),
              mImpl(inImpl),
              mConnectionPtr(),
              mRecursionCount(0),
              mIdleFlag(false),
              mTransactionPtr(0)
        {
            SET_HANDLER(this, &ClientSM::EventHandler);
            List::Init(*this);
        }
        virtual ~ClientSM()
        {
            QCRTASSERT(0 == mRecursionCount && ! mTransactionPtr &&
                    (! mConnectionPtr || ! mConnectionPtr->IsGood()));
            --mRecursionCount; // To catch double delete.
        }
        void Connect(
            Transaction& inTransaction)
        {
            QCASSERT(! mTransactionPtr);
            mIdleFlag = false;
            const bool theNonBlockingFlag = true;
            TcpSocket& theSocket          = *(new TcpSocket());
            const int theErr              = theSocket.Connect(
                mImpl.mLocation, theNonBlockingFlag);
            if (theErr && theErr != -EINPROGRESS) {
                const string theError = QCUtils::SysError(-theErr);
                KFS_LOG_STREAM_ERROR <<
                    "failed to connect to server " << mImpl.mLocation <<
                    " : " << theError <<
                KFS_LOG_EOM;
                delete &theSocket;
                mImpl.Remove(*this);
                inTransaction.Error(theErr, theError.c_str());
                return;
            }
            mTransactionPtr = &inTransaction;
            KFS_LOG_STREAM_DEBUG <<
                "connecting to server: " << mImpl.mLocation <<
            KFS_LOG_EOM;
            mConnectionPtr.reset(new NetConnection(&theSocket, this));
            mConnectionPtr->EnableReadIfOverloaded();
            mConnectionPtr->SetDoingNonblockingConnect();
            mConnectionPtr->SetMaxReadAhead(1);
            mConnectionPtr->SetInactivityTimeout(mImpl.mTimeout);
            // Add connection to the poll vector
            mImpl.mNetManager.AddConnection(mConnectionPtr);
        }
        void Run(
            Transaction& inTransaction)
        {
            QCASSERT(! mTransactionPtr && mIdleFlag);
            mIdleFlag = false;
            mTransactionPtr = &inTransaction;
            mConnectionPtr->SetInactivityTimeout(mImpl.mTimeout);
            EventHandler(EVENT_NET_WROTE, &mConnectionPtr->GetOutBuffer());
        }
        void Stop(
            int         inError,
            const char* inMsgPtr)
        {
            if (mConnectionPtr) {
                mConnectionPtr->Close();
            }
            if (mTransactionPtr) {
                mTransactionPtr->Error(inError, inMsgPtr);
            }
            mImpl.Remove(*this);
        }
        int EventHandler(
            int   inEventCode,
            void* inEventDataPtr)
        {
            mRecursionCount++;
            QCASSERT(mRecursionCount >= 1);

            switch (inEventCode) {
	        case EVENT_NET_READ: {
                    IOBuffer& theIoBuf = mConnectionPtr->GetInBuffer();
                    QCASSERT(&theIoBuf == inEventDataPtr);
                    int theRet;
                    if (! mTransactionPtr ||
                            (theRet = mTransactionPtr->Response(
                                theIoBuf, false)) < 0) {
                        mTransactionPtr = 0;
                        mConnectionPtr->Close();
                    } else if (0 < theRet) {
                        mConnectionPtr->SetMaxReadAhead(theRet);
                    } else {
                        mTransactionPtr = 0;
                    }
                    break;
                }

	        case EVENT_NET_WROTE:
                    if (mTransactionPtr) {
                        IOBuffer& theIoBuf = mConnectionPtr->GetOutBuffer();
                        const bool theUpdateFlag = theIoBuf.IsEmpty();
                        QCASSERT(&theIoBuf == inEventDataPtr);
                        const int theRet = mTransactionPtr->Request(
                            theIoBuf,
                            mConnectionPtr->GetInBuffer(),
                            mImpl.mLocation
                        );
                        if (theRet < 0) {
                            mTransactionPtr = 0;
                            mConnectionPtr->Close();
                        } else {
                            if (0 < theRet) {
                                mConnectionPtr->SetMaxReadAhead(theRet);
                            }
                            if (theUpdateFlag && ! theIoBuf.IsEmpty()) {
                                const bool kResetTimerFlag = true;
                                mConnectionPtr->Update(kResetTimerFlag);
                            }
                        }
                    }
                    break;

	        case EVENT_NET_ERROR:
                    if (mConnectionPtr->IsGood()) {
                        // EOF
                        if (mTransactionPtr && mTransactionPtr->Response(
                                mConnectionPtr->GetInBuffer(), true) <= 0) {
                            mTransactionPtr = 0;
                        }
                    }
                    // Fall through.
                case EVENT_INACTIVITY_TIMEOUT:
                    mConnectionPtr->Close();
                    mConnectionPtr->GetInBuffer().Clear();
                    break;

	        default:
                    QCASSERT(!"Unexpected event code");
                    break;
            }
            if (1 == mRecursionCount) {
                mConnectionPtr->StartFlush();
                if (! mConnectionPtr->IsGood()) {
                    if (mTransactionPtr) {
                        const string theErrMsg(
                            inEventCode == EVENT_INACTIVITY_TIMEOUT ?
                            string() : mConnectionPtr->GetErrorMsg()
                        );
                        mTransactionPtr->Error(
                            inEventCode == EVENT_INACTIVITY_TIMEOUT ?
                                -ETIMEDOUT : -EIO,  
                            inEventCode == EVENT_INACTIVITY_TIMEOUT ? 
                                "network timeout" :
                                (theErrMsg.empty() ?
                                    "network error" : theErrMsg.c_str())
                        );
                    }
                    mTransactionPtr = 0;
                    mConnectionPtr->Close();
                    mRecursionCount--;
                    mImpl.Remove(*this);
                    return 0;
                }
                if (! mTransactionPtr) {
                    mConnectionPtr->SetMaxReadAhead(1);
                    mConnectionPtr->SetInactivityTimeout(mImpl.mIdleTimeout);
                    mConnectionPtr->GetOutBuffer().Clear();
                    mConnectionPtr->GetInBuffer().Clear();
                    mRecursionCount--;
                    QCASSERT(! mIdleFlag);
                    mIdleFlag = true;
                    mImpl.Add(*this);
                    return 0;
                }
            }
            QCASSERT(1 <= mRecursionCount);
            mRecursionCount--;
            return 0;
        }
        bool IsIdle() const
            { return mIdleFlag; }
    protected:
        Impl&            mImpl;
        NetConnectionPtr mConnectionPtr;
        int              mRecursionCount;
        bool             mIdleFlag;
        Transaction*     mTransactionPtr;
        ClientSM*        mPrevPtr[1];
        ClientSM*        mNextPtr[1];

        friend class QCDLListOp<ClientSM>;
    };
    friend class ClientSM;
    class SslClientSM : public ClientSM
    {
    public:
        SslClientSM(
            Impl& inImpl)
            : ClientSM(inImpl),
              mSslFilter(
                *inImpl.mSslCtxPtr,
                0,       // inPskDataPtr
                0,       // inPskDataLen
                0,       // inPskCliIdendityPtr
                0,       // inServerPskPtr
                &inImpl, // inVerifyPeerPtr
                false,   // inDeleteOnCloseFlag,
                inImpl.mServerName.empty() ? 0 : inImpl.mServerName.c_str()
              )
            { SET_HANDLER(this, &SslClientSM::EventHandler); }
        virtual ~SslClientSM()
            {}
        int EventHandler(
            int   inEventCode,
            void* inEventDataPtr)
        {
            if (! mConnectionPtr->GetFilter()) {
                SET_HANDLER(this, &ClientSM::EventHandler);
                string    theErrMsg;
                const int theErr = mConnectionPtr->SetFilter(
                    &mSslFilter, &theErrMsg);
                if (theErr) {
                    if (theErrMsg.empty()) {
                        theErrMsg = QCUtils::SysError(
                            theErr < 0 ? -theErr : theErr);
                    }
                    KFS_LOG_STREAM_ERROR <<
                        "connect to " << mImpl.mLocation <<
                        " error: "    << theErrMsg <<
                    KFS_LOG_EOM;
                    mConnectionPtr->Close();
                    return ClientSM::EventHandler(EVENT_NET_ERROR, 0);
                }
            }
            return ClientSM::EventHandler(inEventCode, inEventDataPtr);
        }
    private:
        SslFilter mSslFilter;
    };
    friend class SslClientSM;

    typedef ClientSM::List List;
    typedef set<string>    PeerNames;

    NetManager&     mNetManager;
    ServerLocation  mLocation;
    SslFilter::Ctx* mSslCtxPtr;
    int             mTimeout;
    int             mIdleTimeout;
    bool            mHttpsHostNameFlag;
    bool            mVerifyServerFlag;
    string          mServerName;
    PeerNames       mPeerNames;
    Properties      mSslCtxParameters;
    int             mError;
    string          mErrorMsg;
    ClientSM*       mInUseListPtr[1];
    ClientSM*       mIdleListPtr[1];

    void UpdateHttpsPeerNames()
    {
        mPeerNames.clear();
        if (mLocation.hostname.empty()) {
            return;
        }
        mPeerNames.insert(mLocation.hostname);
        const size_t thePos = mLocation.hostname.find('.');
        if (string::npos != thePos && 0 < thePos &&
                thePos + 1 < mLocation.hostname.size()) {
            string theName("*");
            theName.append(mLocation.hostname, thePos, string::npos);
            mPeerNames.insert(theName);
        }
    }
    void UpdateStatus()
    {
        mError = (mLocation.IsValid() &&
            (mSslCtxParameters.empty() || 0 != mSslCtxPtr)) ? 0 : -EINVAL;
        if (0 == mError) {
            mErrorMsg.clear();
        } else if (mErrorMsg.empty()) {
            if (mLocation.IsValid()) {
                mErrorMsg = "invalid ssl configation";
            } else {
                mErrorMsg = "invalid server address";
            }
        }
    }
    void Add(
        ClientSM& inClient)
    {
        QCASSERT(inClient.IsIdle());
        List::Remove(mInUseListPtr, inClient);
        List::PushFront(mIdleListPtr, inClient);
    }
    void Remove(
        ClientSM& inClient)
    {
        List::Remove(inClient.IsIdle() ? mIdleListPtr : mInUseListPtr,
            inClient);
        delete &inClient;
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

TransactionalClient::TransactionalClient(
    NetManager& inNetManager)
    : mImpl(*(new Impl(inNetManager)))
    {}

TransactionalClient::~TransactionalClient()
{
    delete &mImpl;
}

    int
TransactionalClient::SetServer(
    const ServerLocation& inLocation,
    bool                  inHttpsHostNameFlag)
{
    return mImpl.SetServer(inLocation, inHttpsHostNameFlag);
}

    void
TransactionalClient::Stop()
{
    mImpl.Stop(-EIO, "stop");
}

    int
TransactionalClient::SetParameters(
    const char*       inPrefixPtr,
    const Properties& inParameters,
    string*           inErrMsgPtr)
{
    return mImpl.SetParameters(inPrefixPtr, inParameters, inErrMsgPtr);
}

    void
TransactionalClient::Run(
    Transaction& inTransaction)
{
    mImpl.Run(inTransaction);
}

} // namespace KFS
