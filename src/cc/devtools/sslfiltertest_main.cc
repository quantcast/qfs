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
    private SslFilter::ServerPsk
{
public:
    static int Run(
        int    inArgsCount,
        char** inArgsPtr)
    {
        libkfsio::InitGlobals();
        SslFilter::Error theErr = SslFilter::Initialize();
        int theRet;
        if (theErr) {
            cerr << "SslFilter init error: " <<
                SslFilter::GetErrorMsg(theErr) << "\n";
            theRet = 1;
        } else {
            SslFilterTest theTest;
            theRet = theTest.Run(inArgsCount, inArgsPtr);
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
private:
    Properties      mProperties;
    NetManager      mNetManager;
    Acceptor*       mAcceptorPtr;
    SslFilter::Ctx* mSslCtxPtr;
    string          mPskIdentity;
    string          mPskKey;
    int             mMaxReadAhead;
    int             mMaxWriteBehind;

    class Responder : public KfsCallbackObj
    {
    public:
        Responder(
            SslFilter::Ctx&       inCtx,
            SslFilter::ServerPsk& inServerPsk,
            NetConnectionPtr&     inConnectionPtr,
            int                   inMaxReadAhead,
            int                   inMaxWriteBehind)
            : mConnectionPtr(inConnectionPtr),
              mSslFilter(
                inCtx,
                0, // inPskDataPtr
                0, // inPskDataLen
                0, // inPskCliIdendityPtr
                &inServerPsk,
                false // inDeleteOnCloseFlag
              ),
              mRecursionCount(0),
              mCloseConnectionFlag(false),
              mMaxReadAhead(inMaxReadAhead),
              mMaxWriteBehind(inMaxWriteBehind)
        {
            QCASSERT(inConnectionPtr);
            SET_HANDLER(this, &Responder::EventHandler);
            mConnectionPtr->SetFilter(&mSslFilter);
            mConnectionPtr->SetMaxReadAhead(mMaxReadAhead);
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

    SslFilterTest()
        : IAcceptorOwner(),
          ServerPsk(),
          mProperties(),
          mNetManager(),
          mAcceptorPtr(0),
          mSslCtxPtr(0),
          mPskIdentity(),
          mPskKey(),
          mMaxReadAhead((8 << 10) - 1),
          mMaxWriteBehind((8 << 10) - 1)
        {}
    virtual ~SslFilterTest()
        { delete mAcceptorPtr; }
    int RunSelf(
        int    inArgsCount,
        char** inArgsPtr)
    {
        delete mAcceptorPtr;
        mAcceptorPtr = 0;
        string thePropsStr;
        const char kDelim = '=';
        const bool kVerboseFlag = true;
        for (int i = 1; i < inArgsCount; ) {
            if (strcmp(inArgsPtr[i], "-c") == 0) {
                if (inArgsCount <= ++i) {
                    Usage(inArgsPtr[0]);
                    return 1;
                }
                if (mProperties.loadProperties(
                        inArgsPtr[i], kDelim, kVerboseFlag)) {
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
                    theInStream, kDelim, kVerboseFlag)) {
                cerr << "error parsing arguments\n";
                return 1;
            }
        }
        MsgLogger::Init(mProperties, "SslFilterTest.");
        if (! MsgLogger::GetLogger()) {
            cerr << "messsage logger initialization failure\n";
            return 1;
        }
        MsgLogger::Stop();
        return 0;
    }
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
        return (mSslCtxPtr ? 0 : new Responder(
            *mSslCtxPtr,
            *this,
            inConnPtr,
            mMaxReadAhead,
            mMaxWriteBehind
        ));
    }
    virtual unsigned long GetPsk(
        const char*    inIdentityPtr,
	unsigned char* inPskBufferPtr,
        unsigned int   inPskBufferLen)
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
        return mPskKey.size();
    }

private:
    SslFilterTest(
        const SslFilterTest& inTest);
    SslFilterTest& operator=(
        const SslFilterTest& inTest);
};

}

    int
main(
    int    inArgsCount,
    char** inArgsPtr)
{
    return KFS::SslFilterTest::Run(inArgsCount, inArgsPtr);
}
