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
// \brief Trivial HTTP(s) get test.
//
//----------------------------------------------------------------------------

#include "kfsio/TransactionalClient.h"

#include "kfsio/NetManager.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/SslFilter.h"
#include "kfsio/HttpResponseHeaders.h"

#include "common/MsgLogger.h"
#include "common/Properties.h"
#include "common/kfsdecls.h"
#include "common/RequestParser.h"
#include "common/httputils.h"

#include "qcdio/qcdebug.h"
#include "qcdio/QCUtils.h"

#include <stdio.h>
#include <string.h>

namespace KFS
{
using namespace KFS::httputils;

class HttpsTest : public ITimeout
{
public:
    enum { kMaxHdrLen = 16 << 10 };
    HttpsTest()
        : ITimeout(),
          mNetManager(),
          mClient(mNetManager),
          mWOStream(),
          mOutFd(fileno(stdout)),
          mInFlightCount(0),
          mDoneCount(0)
        {}
    ~HttpsTest()
    {
        QCASSERT(0 == mInFlightCount);
    }
    int Run(
        int    inArgsCount,
        char** inArgsPtr)
    {
        MsgLogger::Init(0, MsgLogger::kLogLevelDEBUG);
        if (3 != inArgsCount) {
            KFS_LOG_STREAM_ERROR <<
                "Usage: {-cfg <config> | <host> <port>}" <<
            KFS_LOG_EOM;
            return 1;
        }
        SslFilter::Error const theSslErr = SslFilter::Initialize();
        if (theSslErr) {
            KFS_LOG_STREAM_ERROR <<
                "ssl initialization error: " << theSslErr <<
                 " " << SslFilter::GetErrorMsg(theSslErr) <<
            KFS_LOG_EOM;
            return 1;
        }
        if (strcmp(inArgsPtr[1], "-cfg") == 0) {
            Properties theProps;
            if (theProps.loadProperties(inArgsPtr[2], '=')) {
                return 1;
            }
            string theErr;
            if (mClient.SetParameters("https.", theProps, &theErr) < 0) {
                KFS_LOG_STREAM_ERROR <<
                    "set parameters failure: " << theErr <<
                KFS_LOG_EOM;
                return 1;
            }
        } else {
            ServerLocation theLocation;
            theLocation.hostname = inArgsPtr[1];
            const char* thePtr = inArgsPtr[2];
            if (! DecIntParser::Parse(
                    thePtr, strlen(inArgsPtr[2]),
                    theLocation.port)) {
                KFS_LOG_STREAM_ERROR << "invalid port: " << inArgsPtr[2] <<
                KFS_LOG_EOM;
                return 1;
            }
            if (443 == theLocation.port) {
                Properties theProps;
                theProps.setValue("host", theLocation.hostname);
                theProps.setValue("port", inArgsPtr[2]);
                theProps.setValue("ssl.verifyServer", "0");
                string theErr;
                if (mClient.SetParameters(0, theProps, &theErr)) {
                    KFS_LOG_STREAM_ERROR <<
                        "set parameters error: " << theErr <<
                    KFS_LOG_EOM;
                    return 1;
                }
            } else {
                mClient.SetServer(theLocation, true);
            }
        }
        mNetManager.RegisterTimeoutHandler(this);
        mNetManager.MainLoop();
        mNetManager.UnRegisterTimeoutHandler(this);
        mClient.Stop();
        SslFilter::Cleanup();
        return 0;
    }
    virtual void Timeout()
    {
        if (5 <= mDoneCount) {
            KFS_LOG_STREAM_DEBUG <<
                " shutdown: "  <<
                " in flight: " << mInFlightCount <<
                " done: "      << mDoneCount <<
            KFS_LOG_EOM;
            mNetManager.Shutdown();
        }
        int k = 3 - mInFlightCount;
        while (0 < k--) {
            mInFlightCount++;
            mClient.Run(*(new HttpReq(*this)));
        }
    }
private:
    class HttpReq : public TransactionalClient::Transaction
    {
    public:
        HttpReq(
            HttpsTest& inOuter)
            : mOuter(inOuter),
              mSentFlag(false),
              mHeaderLength(-1),
              mHeaders()
            {}
        virtual ~HttpReq()
        {
            QCASSERT(0 < mOuter.mInFlightCount);
            mOuter.mInFlightCount--;
            mOuter.mDoneCount++;
        }
        virtual int Request(
            IOBuffer&             inBuffer,
            IOBuffer&             inResponseBuffer,
            const ServerLocation& inServer)
        {
            if (mSentFlag) {
                return 0;
            }
            mHeaders.Reset();
            ostream& theStream = mOuter.mWOStream.Set(inBuffer);
            theStream <<
                "GET / HTTP/1.1\r\n"
                "Host: " << inServer.hostname << "\r\n"
                "\r\n"
                "\r\n"
            ;
            mOuter.mWOStream.Reset();
            mSentFlag = true;
            return kMaxHdrLen;
        }
        virtual int Response(
            IOBuffer& inBuffer)
        {
            if (mHeaderLength <= 0 &&
                    ((mHeaderLength = GetHeaderLength(inBuffer)) <= 0 ||
                    kMaxHdrLen < mHeaderLength)) {
                if (kMaxHdrLen < inBuffer.BytesConsumable()) {
                    KFS_LOG_STREAM_ERROR <<
                        "exceeded max header length: " <<
                        inBuffer.BytesConsumable() << " bytes" <<
                    KFS_LOG_EOM;
                    inBuffer.Clear();
                    delete this;
                    return -1;
                }
                return kMaxHdrLen;
            }
            if (mHeaders.GetContentLength() < 0) {
                const char* const thePtr = inBuffer.CopyOutOrGetBufPtr(
                        mOuter.mHdrBuffer, mHeaderLength);
                if (! mHeaders.Parse(thePtr, mHeaderLength) ||
                        mHeaders.IsUnsupportedEncoding() ||
                        (mHeaders.IsHttp11OrGreater() &&
                        (mHeaders.GetContentLength() < 0 &&
                        ! mHeaders.IsChunkedEconding()))) {
                    if (thePtr != mOuter.mHdrBuffer) {
                        memcpy(mOuter.mHdrBuffer, thePtr, mHeaderLength);
                    }
                    mOuter.mHdrBuffer[mHeaderLength] = 0;
                    KFS_LOG_STREAM_ERROR << " failed to parse content length:" <<
                        " discarding: " << inBuffer.BytesConsumable() << " bytes"
                        " header length: " << mHeaderLength <<
                        " header: " << mOuter.mHdrBuffer <<
                    KFS_LOG_EOM;
                    inBuffer.Clear();
                    delete this;
                    return -1;
                }
                if (inBuffer.BytesConsumable() <
                        mHeaders.GetContentLength() + mHeaderLength) {
                    return ((mHeaders.GetContentLength() + mHeaderLength) -
                        inBuffer.BytesConsumable());
                }
            }
            KFS_LOG_STREAM_DEBUG <<
                "response:"
                " headers: "   << mHeaderLength <<
                " body: "      << mHeaders.GetContentLength() <<
                " buffer: "    << inBuffer.BytesConsumable() <<
                " status: "    << mHeaders.GetStatus() <<
                " http/1.1 "   << mHeaders.IsHttp11OrGreater() <<
                " close: "     << mHeaders.IsConnectionClose() <<
                " chunked: "   << mHeaders.IsChunkedEconding() <<
                " in flight: " << mOuter.mInFlightCount <<
                " done: "      << mOuter.mDoneCount <<
            KFS_LOG_EOM;
            while (! inBuffer.IsEmpty()) {
                const int theErr = inBuffer.Write(mOuter.mOutFd);
                if (theErr < 0) {
                    KFS_LOG_STREAM_ERROR << " write error: " <<
                        QCUtils::SysError(-theErr) <<
                        " discarding: " << inBuffer.BytesConsumable() <<
                        " bytes" <<
                    KFS_LOG_EOM;
                    inBuffer.Clear();
                    break;
                }
            }
            delete this;
            return 0;
        }
        virtual void Error(
            int         inStatus,
            const char* inMsgPtr)
        {
            KFS_LOG_STREAM_ERROR <<
                "network error: " << inStatus  <<
                " message: "      << (inMsgPtr ? inMsgPtr : "") <<
                " in flight: "    << mOuter.mInFlightCount <<
                " done: "         << mOuter.mDoneCount <<
            KFS_LOG_EOM;
            delete this;
        }
    private:
        HttpsTest&          mOuter;
        bool                mSentFlag;
        int                 mHeaderLength;
        HttpResponseHeaders mHeaders;
    private:
        HttpReq(
            const HttpReq& inRequest);
        HttpReq& operator=(
            const HttpReq& inRequest);
    };
    friend class HttpReq;

    NetManager          mNetManager;
    TransactionalClient mClient;
    IOBuffer::WOStream  mWOStream;
    int const           mOutFd;
    int                 mInFlightCount;
    int                 mDoneCount;
    char                mHdrBuffer[kMaxHdrLen + 1];
};

}

    int
main(
    int    inArgsCount,
    char** inArgsPtr)
{
    KFS::HttpsTest theTest;
    return theTest.Run(inArgsCount, inArgsPtr);
}
