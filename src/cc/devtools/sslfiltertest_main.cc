//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/07/15
// Author:  Mike Ovsiannikov 
//
// Copyright 2013 Quantcast Corp.
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
// \brief Ssl socket layer unit test.
//
//----------------------------------------------------------------------------

#include "kfsio/SslFilter.h"
#include "kfsio/Globals.h"
#include "kfsio/NetConnection.h"
#include "kfsio/Acceptor.h"
#include "kfsio/NetManager.h"
#include "qcdio/qcdebug.h"
#include "common/MsgLogger.h"
#include "common/Properties.h"
#include "common/kfsdecls.h"
#include "qcdio/QCUtils.h"

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <fcntl.h>

#include <iostream>
#include <string>
#include <sstream>

namespace KFS
{

using std::cerr;
using std::cout;
using std::string;
using std::istringstream;

class SslFilterTest :
    private IAcceptorOwner,
    private SslFilterServerPsk
{
public:
    static int Run(
        int    inArgsCount,
        char** inArgsPtr)
    {
        signal(SIGINT,  &SslFilterTest::Shutdown);
        signal(SIGQUIT, &SslFilterTest::Shutdown);
        signal(SIGPIPE, SIG_IGN);
        libkfsio::InitGlobals();
        SslFilter::Error theErr = SslFilter::Initialize();
        int theRet;
        if (theErr) {
            cerr << "SslFilter init error: " <<
                SslFilter::GetErrorMsg(theErr) << "\n";
            theRet = 1;
        } else {
            SslFilterTest theTest;
            theRet = theTest.RunSelf(inArgsCount, inArgsPtr);
        }
        theErr = SslFilter::Cleanup();
        if (theErr) {
            cerr << "SslFilter cleanup error: " <<
                SslFilter::GetErrorMsg(theErr) << "\n";
            if (theRet == 0) {
                theRet = 1;
            }
        }
        libkfsio::DestroyGlobals();
        return theRet;
    }
    static void Shutdown(int /* inSignal */)
    {
        if (sInstancePtr) {
            sInstancePtr->ShutdownSelf();
        }
    };
private:
    Properties      mProperties;
    NetManager      mNetManager;
    Acceptor*       mAcceptorPtr;
    SslFilter::Ctx* mSslCtxPtr;
    string          mPskIdentity;
    string          mPskKey;
    int             mMaxReadAhead;
    int             mMaxWriteBehind;
    bool            mUseFilterFlag;

    static SslFilterTest* sInstancePtr;

    class Responder : public KfsCallbackObj
    {
    public:
        Responder(
            SslFilter::Ctx&       inCtx,
            SslFilter::ServerPsk& inServerPsk,
            NetConnectionPtr&     inConnectionPtr,
            int                   inMaxReadAhead,
            int                   inMaxWriteBehind,
            bool                  inUseFilterFlag)
            : mConnectionPtr(inConnectionPtr),
              mPeerName(mConnectionPtr->GetPeerName() + " "),
              mSslFilter(
                inCtx,
                0,    // inPskDataPtr
                0,    // inPskDataLen
                0,    // inPskCliIdendityPtr
                &inServerPsk,
                0,    // inVerifyPeerPtr
                false // inDeleteOnCloseFlag
              ),
              mRecursionCount(0),
              mCloseConnectionFlag(false),
              mMaxReadAhead(inMaxReadAhead),
              mMaxWriteBehind(inMaxWriteBehind)
        {
            QCASSERT(mConnectionPtr);
            SET_HANDLER(this, &Responder::EventHandler);
            if (inUseFilterFlag) {
                string theErrMsg;
                const int theErr = mConnectionPtr->SetFilter(
                    &mSslFilter, &theErrMsg);
                if (theErr) {
                    if (theErrMsg.empty()) {
                        theErrMsg = QCUtils::SysError(
                            theErr < 0 ? -theErr : theErr);
                    }
                    KFS_LOG_STREAM_ERROR << mPeerName << "Responder()" <<
                        " error: " << theErrMsg <<
                    KFS_LOG_EOM;
                    mConnectionPtr->Close();
                    return;
                }
            }
            mConnectionPtr->SetMaxReadAhead(mMaxReadAhead);
            KFS_LOG_STREAM_DEBUG << mPeerName << "Responder()" <<
            KFS_LOG_EOM;
        }
        virtual ~Responder()
        {
            KFS_LOG_STREAM_DEBUG << mPeerName << "~Responder()" <<
            KFS_LOG_EOM;
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
                    // Simple echo.
                    mConnectionPtr->Write(&theIoBuf);
                    break;
                }
	        case EVENT_NET_WROTE:
                    if (mCloseConnectionFlag &&
                            ! mConnectionPtr->IsWriteReady()) {
                        mConnectionPtr->Close();
                    }
                    break;

	        case EVENT_NET_ERROR:
                    mConnectionPtr->SetMaxReadAhead(0);
                    if (mConnectionPtr->IsGood() &&
                            mConnectionPtr->IsWriteReady()) {
                        mCloseConnectionFlag = mCloseConnectionFlag ||
                            ! mConnectionPtr->HasPendingRead();
                        break;
                    }
                    // Fall through
                case EVENT_INACTIVITY_TIMEOUT:
                    mConnectionPtr->Close();
                    mConnectionPtr->GetInBuffer().Clear();
                    break;

	        default:
                    QCASSERT(!"Unexpected event code");
                    break;
            }

            QCASSERT(mRecursionCount >= 1);
            if (mRecursionCount <= 1) {
                mConnectionPtr->StartFlush();
                if (mConnectionPtr->IsGood()) {
                    const int kIoTimeout   = 60;
                    const int kIdleTimeout = 600;
                    mConnectionPtr->SetInactivityTimeout(
                        mConnectionPtr->IsWriteReady() ?
                            kIoTimeout : kIdleTimeout);
                    if (mConnectionPtr->IsReadReady()) {
                        if (IsOverWriteBehindLimit()) {
                            // Shut down read until client unloads the data.
                            mConnectionPtr->SetMaxReadAhead(0);
                        }
                    } else {
                        if (! mCloseConnectionFlag &&
                                ! IsOverWriteBehindLimit()) {
                            // Set read back again.
                            mConnectionPtr->SetMaxReadAhead(mMaxReadAhead);
                        }
                    }
                } else {
                    delete this;
                    return 0;
                }
            }
            mRecursionCount--;
            return 0;
        }
    private:
        NetConnectionPtr const mConnectionPtr;
        string const           mPeerName;
        SslFilter              mSslFilter;
        int                    mRecursionCount;
        bool                   mCloseConnectionFlag;
        const int              mMaxReadAhead;
        const int              mMaxWriteBehind;

        bool IsOverWriteBehindLimit() const
        {
            return (mConnectionPtr->GetNumBytesToWrite() > mMaxWriteBehind);
        }
    private:
        Responder(
            const Responder& inResponder);
        Responder& operator=(
            const Responder& inResponder);
    };

    class Initiator
    {
    public:
        Initiator(
            int                   inInputFd,
            int                   inOutputFd,
            SslFilter::Ctx&       inCtx,
            const string&         inPsk,
            const string&         inIdentity,
            const ServerLocation& inServerLocation,
            int                   inMaxReadAhead,
            int                   inMaxWriteBehind,
            bool                  inShutdownFlag,
            bool                  inUserFilterFlag,
            NetManager&           inNetManager)
            : mConnectionPtr(),
              mSslFilter(
                inCtx,
                inPsk.data(),
                inPsk.size(),
                inIdentity.c_str(),
                0,    // inServerPskPtr
                0,    // inVerifyPeerPtr
                false // inDeleteOnCloseFlag
              ),
              mServerLocation(inServerLocation),
              mRecursionCount(0),
              mInputSocket(inInputFd),
              mOutputSocket(inOutputFd),
              mInputConnectionPtr(),
              mOutputConnectionPtr(),
              mCloseConnectionFlag(false),
              mMaxReadAhead(inMaxReadAhead),
              mMaxWriteBehind(inMaxWriteBehind),
              mShutdownFlag(inShutdownFlag),
              mUseFilterFlag(inUserFilterFlag),
              mNetManager(inNetManager),
              mInputCB(),
              mOutputCB(),
              mNetCB()
        {
            QCASSERT(mInputSocket.IsGood());
            QCASSERT(mOutputSocket.IsGood());
            if (fcntl(inInputFd, F_SETFL, O_NONBLOCK)) {
                const int theErr = errno;
                KFS_LOG_STREAM_ERROR << "input set non block: " <<
                    QCUtils::SysError(theErr) << KFS_LOG_EOM;
            }
            if (fcntl(inOutputFd, F_SETFL, O_NONBLOCK)) {
                const int theErr = errno;
                KFS_LOG_STREAM_ERROR << "output set non block: " <<
                    QCUtils::SysError(theErr) << KFS_LOG_EOM;
            }

            mInputCB.SetHandler(this, &Initiator::InputHandler);
            mOutputCB.SetHandler(this, &Initiator::OutputHandler);
            mNetCB.SetHandler(this, &Initiator::NetHandler);
            const bool kOwnsSocketFlag = false;
            const bool kListenOnlyFlag = false;
            mInputConnectionPtr.reset(new NetConnection(
                &mInputSocket, &mInputCB, kListenOnlyFlag, kOwnsSocketFlag));
            mOutputConnectionPtr.reset(new NetConnection(
                &mOutputSocket, &mOutputCB, kListenOnlyFlag, kOwnsSocketFlag));
            mInputConnectionPtr->SetMaxReadAhead(mMaxReadAhead);
            mOutputConnectionPtr->SetMaxReadAhead(0);
        }
        ~Initiator()
        {
            mInputConnectionPtr->Close();
            mOutputConnectionPtr->Close();
            if (mShutdownFlag) {
                mNetManager.Shutdown();
            }
        }
        bool Connect(
            string* inErrMsgPtr)
        {
            const bool theNonBlockingFlag = true;
            TcpSocket& theSocket          = *(new TcpSocket());
            const int theErr              = theSocket.Connect(
                mServerLocation, theNonBlockingFlag);
            if (theErr && theErr != -EINPROGRESS) {
                if (inErrMsgPtr) {
                    *inErrMsgPtr = QCUtils::SysError(-theErr);
                }
                KFS_LOG_STREAM_ERROR <<
                    "failed to connect to server " << mServerLocation.ToString() <<
                    " : " << QCUtils::SysError(-theErr) <<
                KFS_LOG_EOM;
                delete &theSocket;
                return false;
            }
            KFS_LOG_STREAM_DEBUG <<
                "connecting to server: " << mServerLocation.ToString() <<
            KFS_LOG_EOM;
            mConnectionPtr.reset(new NetConnection(&theSocket, &mNetCB));
            mConnectionPtr->EnableReadIfOverloaded();
            mConnectionPtr->SetDoingNonblockingConnect();
            mConnectionPtr->SetMaxReadAhead(mMaxReadAhead);
            const int kConnectTimeout = 120;
            mConnectionPtr->SetInactivityTimeout(kConnectTimeout);
            // Add connection to the poll vector
            mNetManager.AddConnection(mConnectionPtr);
            mNetManager.AddConnection(mInputConnectionPtr);
            mNetManager.AddConnection(mOutputConnectionPtr);
            return true;
        }
        int InputHandler(
            int   inEventCode,
            void* inEventDataPtr)
        {
            mRecursionCount++;
            QCASSERT(mRecursionCount >= 1);

            switch (inEventCode) {
	        case EVENT_NET_READ: {
                    IOBuffer& theIoBuf = mInputConnectionPtr->GetInBuffer();
                    QCASSERT(&theIoBuf == inEventDataPtr);
                    mConnectionPtr->Write(&theIoBuf);
                    break;
                }

	        case EVENT_NET_ERROR:
                    // Fall through
                case EVENT_INACTIVITY_TIMEOUT:
                    KFS_LOG_STREAM_ERROR << "input: " <<
                        (inEventCode == EVENT_INACTIVITY_TIMEOUT  ?
                            string("input timed out") :
                            (mInputConnectionPtr->IsGood() ?
                                string("EOF") : 
                                QCUtils::SysError(errno, ""))
                            ) <<
                    KFS_LOG_EOM;
                    mCloseConnectionFlag = true;
                    mInputConnectionPtr->Close();
                    mInputConnectionPtr->GetInBuffer().Clear();
                    if (! mConnectionPtr->IsWriteReady()) {
                        mConnectionPtr->Close();
                    }
                    if (! mOutputConnectionPtr->IsWriteReady()) {
                        mOutputConnectionPtr->Close();
                    }
                    break;

	        default:
                    QCASSERT(!"Unexpected event code");
                    break;
            }
            return FlowControl();
        }
        int OutputHandler(
            int   inEventCode,
            void* inEventDataPtr)
        {
            mRecursionCount++;
            QCASSERT(mRecursionCount >= 1);

            switch (inEventCode) {
	        case EVENT_NET_WROTE:
                    if (mCloseConnectionFlag &&
                            ! mOutputConnectionPtr->IsWriteReady()) {
                        mOutputConnectionPtr->Close();
                    }
                    break;

	        case EVENT_NET_ERROR:
                    // Fall through
                case EVENT_INACTIVITY_TIMEOUT:
                    KFS_LOG_STREAM_ERROR << "output: " <<
                        (inEventCode == EVENT_INACTIVITY_TIMEOUT  ?
                            string("input timed out") :
                            QCUtils::SysError(errno, "")) <<
                    KFS_LOG_EOM;
                    mCloseConnectionFlag = true;
                    mOutputConnectionPtr->Close();
                    break;

	        default:
                    QCASSERT(!"Unexpected event code");
                    break;
            }
            return FlowControl();
        }
        int NetHandler(
            int   inEventCode,
            void* inEventDataPtr)
        {
            mRecursionCount++;
            QCASSERT(mRecursionCount >= 1);

            switch (inEventCode) {
	        case EVENT_NET_READ: {
                    IOBuffer& theIoBuf = mConnectionPtr->GetInBuffer();
                    QCASSERT(&theIoBuf == inEventDataPtr);
                    mOutputConnectionPtr->Write(&theIoBuf);
                    break;
                }
	        case EVENT_NET_WROTE:
                    if (mUseFilterFlag && ! mConnectionPtr->GetFilter()) {
                        string theErrMsg;
                        const int theErr = mConnectionPtr->SetFilter(
                            &mSslFilter, &theErrMsg);
                        if (theErr) {
                            if (theErrMsg.empty()) {
                                theErrMsg = QCUtils::SysError(
                                    theErr < 0 ? -theErr : theErr);
                            }
                            KFS_LOG_STREAM_ERROR <<
                                mConnectionPtr->GetPeerName() << " Initiator" <<
                                " error: " << theErrMsg <<
                            KFS_LOG_EOM;
                            mConnectionPtr->Close();
                            break;
                        }
                    }
                    if (mCloseConnectionFlag &&
                            ! mConnectionPtr->IsWriteReady()) {
                        mConnectionPtr->Close();
                    }
                    break;

	        case EVENT_NET_ERROR:
                    mConnectionPtr->SetMaxReadAhead(0);
                    if (mConnectionPtr->IsGood() &&
                            mConnectionPtr->IsWriteReady()) {
                        mCloseConnectionFlag = mCloseConnectionFlag ||
                            ! mConnectionPtr->HasPendingRead();
                        break;
                    }
                    // Fall through
                case EVENT_INACTIVITY_TIMEOUT:
                    mConnectionPtr->Close();
                    mConnectionPtr->GetInBuffer().Clear();
                    break;

	        default:
                    QCASSERT(!"Unexpected event code");
                    break;
            }
            return FlowControl();
        }
    private:
        NetConnectionPtr     mConnectionPtr;
        SslFilter            mSslFilter;
        ServerLocation const mServerLocation;
        int                  mRecursionCount;
        TcpSocket            mInputSocket;
        TcpSocket            mOutputSocket;
        NetConnectionPtr     mInputConnectionPtr;
        NetConnectionPtr     mOutputConnectionPtr;
        bool                 mCloseConnectionFlag;
        const int            mMaxReadAhead;
        const int            mMaxWriteBehind;
        const bool           mShutdownFlag;
        const bool           mUseFilterFlag;
        NetManager&          mNetManager;
        KfsCallbackObj       mInputCB;
        KfsCallbackObj       mOutputCB;
        KfsCallbackObj       mNetCB;

        bool IsOverWriteBehindLimit() const
        {
            return (
                mOutputConnectionPtr->GetNumBytesToWrite() > mMaxWriteBehind);
        }
        bool IsInputOverWriteBehindLimit() const
        {
            return (mConnectionPtr->GetNumBytesToWrite() > mMaxWriteBehind);
        }
        int FlowControl()
        {
            if (mRecursionCount > 1) {
                mRecursionCount--;
                return 0;
            }
            QCASSERT(mRecursionCount >= 1);
            mConnectionPtr->StartFlush();
            if (mConnectionPtr->IsGood()) {
                const int kIoTimeout   = 60;
                const int kIdleTimeout = 600;
                mConnectionPtr->SetInactivityTimeout(
                    mConnectionPtr->IsWriteReady() ?
                        kIoTimeout : kIdleTimeout);
                if (mConnectionPtr->IsReadReady()) {
                    if (IsOverWriteBehindLimit()) {
                        // Shut down read until client unloads the data.
                        mConnectionPtr->SetMaxReadAhead(0);
                    }
                } else {
                    if (! mCloseConnectionFlag &&
                            ! IsOverWriteBehindLimit()) {
                        // Set read back again.
                        mConnectionPtr->SetMaxReadAhead(mMaxReadAhead);
                    }
                }
                if (mInputConnectionPtr->IsReadReady()) {
                    if (IsInputOverWriteBehindLimit()) {
                        // Shut down read until client unloads the data.
                        mInputConnectionPtr->SetMaxReadAhead(0);
                    }
                } else {
                    if (! mCloseConnectionFlag &&
                            ! IsInputOverWriteBehindLimit()) {
                        // Set read back again.
                        mInputConnectionPtr->SetMaxReadAhead(mMaxReadAhead);
                    }
                }
            } else {
                delete this;
                return 0;
            }
            QCASSERT(mRecursionCount >= 1);
            mRecursionCount--;
            return 0;
        }
    private:
        Initiator(
            const Initiator& inInitiator);
        Initiator& operator=(
            const Initiator& inInitiator);
    };

    SslFilterTest()
        : IAcceptorOwner(),
          SslFilterServerPsk(),
          mProperties(),
          mNetManager(),
          mAcceptorPtr(0),
          mSslCtxPtr(0),
          mPskIdentity("testid"),
          mPskKey("test"),
          mMaxReadAhead((8 << 10) - 1),
          mMaxWriteBehind((8 << 10) - 1),
          mUseFilterFlag(true)
        {}
    virtual ~SslFilterTest()
    {
        delete mAcceptorPtr;
        SslFilter::FreeCtx(mSslCtxPtr);
    }
    int RunSelf(
        int    inArgsCount,
        char** inArgsPtr)
    {
        delete mAcceptorPtr;
        mAcceptorPtr = 0;
        string thePropsStr;
        const char kDelim = '=';
        for (int i = 1; i < inArgsCount; ++i) {
            if (strcmp(inArgsPtr[i], "-c") == 0) {
                if (inArgsCount <= ++i) {
                    Usage(inArgsPtr[0]);
                    return 1;
                }
                if (mProperties.loadProperties(
                        inArgsPtr[i], kDelim, &cout)) {
                    cerr << "error reading properties file: " <<
                        inArgsPtr[i] << "\n";
                    return 1;
                }
            } else if (strcmp(inArgsPtr[i], "-D") == 0) {
                if (inArgsCount <= ++i) {
                    Usage(inArgsPtr[0]);
                    return 1;
                }
                thePropsStr += inArgsPtr[i];
                thePropsStr += "\n";
            } else {
                Usage(inArgsPtr[0]);
                return 1;
            }
        }
        if (! thePropsStr.empty()) {
            istringstream theInStream(thePropsStr);
            if (mProperties.loadProperties(
                    theInStream, kDelim, &cout)) {
                cerr << "error parsing arguments\n";
                return 1;
            }
        }
        MsgLogger::Init(mProperties, "sslFilterTest.");
        if (! MsgLogger::GetLogger()) {
            cerr << "messsage logger initialization failure\n";
            return 1;
        }
        const int kCommPort = 14188;
        const int theAcceptPort = mProperties.getValue(
            "sslFilterTest.acceptor.port", kCommPort);
        mPskKey = mProperties.getValue(
            "sslFilterTest.psk.key", mPskKey);
        mPskIdentity = mProperties.getValue(
            "sslFilterTest.psk.id", mPskIdentity);
        mMaxReadAhead = mProperties.getValue(
            "sslFilterTest.maxReadAhead", mMaxReadAhead);
        mMaxWriteBehind = mProperties.getValue(
            "sslFilterTest.maxWriteBehind", mMaxWriteBehind);
        mUseFilterFlag = mProperties.getValue(
            "sslFilterTest.useFilter", mUseFilterFlag ? 0 : 1) != 0;
        int theRet = 0;
        if (0 <= theAcceptPort) {
            const bool kServerFlag  = true;
            const bool kPskOnlyFlag = true;
            string theErrMsg;  
            if (! (mSslCtxPtr = SslFilter::CreateCtx(
                    kServerFlag,
                    kPskOnlyFlag,
                    "sslFilterTest.",
                    mProperties,
                    &theErrMsg
                    ))) {
                KFS_LOG_STREAM_ERROR << "create server ssl context error: " <<
                    theErrMsg <<
                KFS_LOG_EOM;
                theRet = 1;
            } else {
                mAcceptorPtr = new Acceptor(mNetManager, theAcceptPort, this);
                if (! mAcceptorPtr->IsAcceptorStarted()) {
                    KFS_LOG_STREAM_ERROR << "listen: port: " << theAcceptPort <<
                        ": " << QCUtils::SysError(errno) <<
                    KFS_LOG_EOM;
                    theRet = 1;
                }
            }
        }
        SslFilter::Ctx* theSslCtxPtr = 0;
        if (theRet == 0) {
            const ServerLocation theServerLocation(
                mProperties.getValue("sslFilterTest.connect.host",
                    string("127.0.0.1")),
                mProperties.getValue("sslFilterTest.connect.port", kCommPort)
            );
            if (theServerLocation.IsValid()) {
                const bool kServerFlag  = false;
                const bool kPskOnlyFlag = true;
                string     theErrMsg;
                if (! (theSslCtxPtr = SslFilter::CreateCtx(
                        kServerFlag,
                        kPskOnlyFlag,
                        "sslFilterTest.",
                        mProperties,
                        &theErrMsg
                        ))) {
                    KFS_LOG_STREAM_ERROR <<
                        "create client ssl context error: " <<
                        theErrMsg <<
                    KFS_LOG_EOM;
                    theRet = 1;
                } else {
                    Initiator* const theClientPtr = new Initiator(
                        fileno(stdin),  //inInputFd,
                        fileno(stdout), // inOutputFd,
                        *theSslCtxPtr,
                        mPskKey,
                        mPskIdentity,
                        theServerLocation,
                        mMaxReadAhead,
                        mMaxWriteBehind,
                        ! mAcceptorPtr, // Shutdown if no acceptor.
                        mUseFilterFlag,
                        mNetManager
                    );
                    if (! theClientPtr->Connect(&theErrMsg)) {
                        KFS_LOG_STREAM_ERROR <<
                            "connect to server error: " <<
                            theErrMsg <<
                        KFS_LOG_EOM;
                        theRet = 1;
                        delete theClientPtr;
                    }
                }
            }
        }
        if (theRet == 0) {
            sInstancePtr = this;
            mNetManager.MainLoop();
            sInstancePtr = 0;
        }
        SslFilter::FreeCtx(theSslCtxPtr);
        MsgLogger::Stop();
        return 0;
    }
    void ShutdownSelf()
        { mNetManager.Shutdown(); }
    void Usage(
        const char* inNamePtr)
    {
        cerr <<
            "Usage " << (inNamePtr ? inNamePtr : "") << ":\n"
            " -c <config file name>\n"
            " -D config-key=config-value\n"
        ;
    }
    virtual KfsCallbackObj* CreateKfsCallbackObj(
        NetConnectionPtr& inConnPtr)
    {
        return (mSslCtxPtr ? new Responder(
            *mSslCtxPtr,
            *this,
            inConnPtr,
            mMaxReadAhead,
            mMaxWriteBehind,
            mUseFilterFlag
        ) : 0);
    }
    virtual unsigned long GetPsk(
        const char*    inIdentityPtr,
	unsigned char* inPskBufferPtr,
        unsigned int   inPskBufferLen,
        string&        outAuthName)
    {
        KFS_LOG_STREAM_DEBUG << "GetPsk:"
            " identity: " << (inIdentityPtr ? inIdentityPtr : "null") <<
            " buffer: "   << (const void*)inPskBufferPtr <<
            " buflen: "   << inPskBufferLen <<
        KFS_LOG_EOM;
        if (inPskBufferLen <= mPskKey.size()) {
            return 0;
        }
        if (mPskIdentity != (inIdentityPtr ? inIdentityPtr : "")) {
            return 0;
        }
        memcpy(inPskBufferPtr, mPskKey.data(), mPskKey.size());
        outAuthName = "test";
        return mPskKey.size();
    }

private:
    SslFilterTest(
        const SslFilterTest& inTest);
    SslFilterTest& operator=(
        const SslFilterTest& inTest);
};
SslFilterTest* SslFilterTest::sInstancePtr = 0;

}

    int
main(
    int    inArgsCount,
    char** inArgsPtr)
{
    return KFS::SslFilterTest::Run(inArgsCount, inArgsPtr);
}
