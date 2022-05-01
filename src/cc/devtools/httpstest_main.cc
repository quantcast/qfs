//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/09/28
// Author:  Mike Ovsiannikov
//
// Copyright 2015,2016 Quantcast Corporation. All rights reserved.
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
#include "kfsio/HttpChunkedDecoder.h"

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
          mPath("/"),
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
        if (3 != inArgsCount && 4 != inArgsCount) {
            KFS_LOG_STREAM_ERROR <<
                "Usage: {-cfg <config> | <host> <port>} [<uri path>]" <<
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
                theProps.setValue("ssl.verifyPeer", "0");
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
            if (4 <= inArgsCount) {
                mPath = inArgsPtr[3];
            }
        }
        bool theResolverUseOsFlag           = true;
        int  theResolverCacheSize           = 8 << 10;
        int  theResolverCacheExpirationTime = 2;
        mNetManager.SetResolverParameters(theResolverUseOsFlag,
                theResolverCacheSize, theResolverCacheExpirationTime);
        mNetManager.RegisterTimeoutHandler(this);
        mNetManager.MainLoop();
        mNetManager.UnRegisterTimeoutHandler(this);
        mClient.Stop();
        SslFilter::Cleanup();
        return 0;
    }
    virtual void Timeout()
    {
        const int kMaxDoneReqCount   = 5;
        const int kStartConcurrently = 3;
        if (kMaxDoneReqCount <= mDoneCount) {
            KFS_LOG_STREAM_DEBUG <<
                "shutdown:"
                " in flight: " << mInFlightCount <<
                " done: "      << mDoneCount <<
            KFS_LOG_EOM;
            mNetManager.Shutdown();
        }
        int k = kStartConcurrently - mInFlightCount;
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
              mIoBuffer(),
              mHeaders(),
              mHttpChunkedDecoder(mIoBuffer)
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
                "GET "   << mOuter.mPath << " HTTP/1.1\r\n"
                "Host: " << inServer.hostname << "\r\n"
                "\r\n"
                "\r\n"
            ;
            mOuter.mWOStream.Reset();
            mSentFlag = true;
            return kMaxHdrLen;
        }
        virtual int Response(
            IOBuffer& inBuffer,
            bool      inEofFlag,
            IOBuffer& /* inOutBuffer */)
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
            if (mHeaders.GetContentLength() < 0 &&
                    ! mHeaders.IsChunkedEconding()) {
                IOBuffer::BufPos  theLen = mHeaderLength;
                const char* const thePtr = inBuffer.CopyOutOrGetBufPtr(
                        mOuter.mHdrBuffer, theLen);
                if (mHeaderLength != theLen ||
                        ! mHeaders.Parse(thePtr, mHeaderLength) ||
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
                if (mHeaders.IsChunkedEconding()) {
                    mIoBuffer.Move(&inBuffer, mHeaderLength);
                }
            }
            if (mHeaders.IsChunkedEconding()) {
                const int theRet = mHttpChunkedDecoder.Parse(inBuffer);
                if (theRet < 0) {
                    KFS_LOG_STREAM_ERROR <<
                        "failed to parse chunk encoded content:" <<
                        " discarding: " << inBuffer.BytesConsumable() <<
                        " bytes header length: " << mHeaderLength <<
                    KFS_LOG_EOM;
                    inBuffer.Clear();
                    delete this;
                    return -1;
                } else if (0 < theRet) {
                    KFS_LOG_STREAM_DEBUG <<
                        " chunked:"
                        " read ahead: " << theRet <<
                        " buffer rem: " << inBuffer.BytesConsumable() <<
                        " decoded: "    << mIoBuffer.BytesConsumable() <<
                    KFS_LOG_EOM;
                    return theRet;
                }
                if (! inBuffer.IsEmpty()) {
                    KFS_LOG_STREAM_ERROR <<
                        "failed to completely parse chunk encoded content:" <<
                        " discarding: " << inBuffer.BytesConsumable() <<
                        " bytes header length: " << mHeaderLength <<
                    KFS_LOG_EOM;
                }
            } else {
                if (inBuffer.BytesConsumable() <
                        mHeaders.GetContentLength() + mHeaderLength) {
                    return ((mHeaders.GetContentLength() + mHeaderLength) -
                        inBuffer.BytesConsumable());
                }
            }
            IOBuffer& theBuffer = mHeaders.IsChunkedEconding() ? mIoBuffer : inBuffer;
            KFS_LOG_STREAM_DEBUG <<
                "response:"
                " headers: "   << mHeaderLength <<
                " body: "      << mHeaders.GetContentLength() <<
                " buffer: "    << theBuffer.BytesConsumable() <<
                " status: "    << mHeaders.GetStatus() <<
                " http/1.1 "   << mHeaders.IsHttp11OrGreater() <<
                " close: "     << mHeaders.IsConnectionClose() <<
                " chunked: "   << mHeaders.IsChunkedEconding() <<
                " in flight: " << mOuter.mInFlightCount <<
                " done: "      << mOuter.mDoneCount <<
            KFS_LOG_EOM;
            while (! theBuffer.IsEmpty()) {
                const int theErr = theBuffer.Write(mOuter.mOutFd);
                if (theErr < 0) {
                    KFS_LOG_STREAM_ERROR << " write error: " <<
                        QCUtils::SysError(-theErr) <<
                        " discarding: " << theBuffer.BytesConsumable() <<
                        " bytes" <<
                    KFS_LOG_EOM;
                    theBuffer.Clear();
                    break;
                }
            }
            const int theRet = mHeaders.IsConnectionClose() ? -1 : 0;
            delete this;
            return theRet;
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
        IOBuffer            mIoBuffer;
        HttpResponseHeaders mHeaders;
        HttpChunkedDecoder  mHttpChunkedDecoder;
    private:
        HttpReq(
            const HttpReq& inRequest);
        HttpReq& operator=(
            const HttpReq& inRequest);
    };
    friend class HttpReq;

    NetManager          mNetManager;
    TransactionalClient mClient;
    string              mPath;
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
