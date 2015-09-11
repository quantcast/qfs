//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/09/09
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
//
//----------------------------------------------------------------------------

#include "s3io.h"

#include "common/MsgLogger.h"
#include "common/time.h"

#include "qcdio/QCFdPoll.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcdebug.h"

#include "../../../../libs3/build/include/libs3.h"
#include <curl/curl.h>
#include <errno.h>

namespace KFS
{

class S3IO::Impl
{
public:
    Impl()
        : mS3CtxPtr(0),
          mCurlCtxPtr(0),
          mFdPoll(),
          mPollWaitMilliSec(1000),
          mPollSocketCount(0)
        {}
    ~Impl()
        {}
    bool Init(
        const char* inUrlPtr)
    {
        if (! mS3CtxPtr &&
                S3StatusOK != S3_create_request_context(&mS3CtxPtr)) {
            return false;
        }
        mCurlCtxPtr = reinterpret_cast<CURLM*>(
            S3_get_curl_request_context(mS3CtxPtr));
        CURLMcode theStatus;
        if (CURLM_OK != (theStatus = curl_multi_setopt(
                mCurlCtxPtr, CURLMOPT_SOCKETDATA, this))) {
            InternalError("curl_multi_setopt(CURLMOPT_SOCKETDATA)", theStatus);
        }
        if (CURLM_OK != (theStatus = curl_multi_setopt(mCurlCtxPtr,
                CURLMOPT_SOCKETFUNCTION, &CurlSocketCB))) {
            InternalError("curl_multi_setopt(CURLMOPT_SOCKETFUNCTION)", theStatus);
        }
        if (CURLM_OK != (theStatus = curl_multi_setopt(
                mCurlCtxPtr, CURLMOPT_TIMERDATA, this))) {
            InternalError("curl_multi_setopt(CURLMOPT_TIMERDATA)", theStatus);
        }
        if (CURLM_OK != (theStatus = curl_multi_setopt(
                mCurlCtxPtr, CURLMOPT_TIMERFUNCTION, &CurlTimerCB))) {
            InternalError("curl_multi_setopt(CURLMOPT_TIMERFUNCTION)", theStatus);
        }
        return true;
    }
    void SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters)
    {
    }
    void ProcessAndWait()
    {
        const int64_t thePollTime = microseconds();
        const int     theErr      =
            mFdPoll.Poll(mPollSocketCount, (int)mPollWaitMilliSec);
        if (theErr < 0 && theErr != -EINTR && theErr != -EAGAIN) {
            KFS_LOG_STREAM_ERROR <<
                QCUtils::SysError(-theErr, "poll error") <<
            KFS_LOG_EOM;
        }
        int               theEvents;
        void*             thePtr;
        CURLMcode         theStatus;
        const char* const kNullPtr    = 0;
        int               theRemCount = 0;
        int               theCount    = 0;
        while (mFdPoll.Next(theEvents, thePtr)) {
            theCount++;
            curl_socket_t theFd = (curl_socket_t)(
                reinterpret_cast<const char*>(thePtr) - kNullPtr);
            while ((theStatus = curl_multi_socket_action(mCurlCtxPtr,
                        theFd,
                        ConvertEvents(theEvents),
                        &theRemCount)) == CURLM_CALL_MULTI_PERFORM)
                {}
            if (CURLM_OK != theStatus) {
                InternalError("curl_multi_socket_action", theStatus);
            }
        }
        if (theCount <= 0 && (mPollWaitMilliSec == 0 ||
                (0 < mPollWaitMilliSec &&
                thePollTime + mPollWaitMilliSec * 1000 + 500 <=
                    microseconds()))) {
            if ((theStatus = curl_multi_socket_action(
                    mCurlCtxPtr, CURL_SOCKET_TIMEOUT, 0, &theRemCount))) {
                InternalError("curl_multi_socket_action(CURL_SOCKET_TIMEOUT)",
                    theStatus);
            }
        }
    }
    void Wakeup()
    {
    }
    void Stop()
    {
    }
    int Open(
        const char* inFileNamePtr,
        bool        inReadOnlyFlag,
        bool        inCreateFlag,
        bool        inCreateExclusiveFlag,
        int64_t&    ioMaxFileSize)
    {
        return -1;
    }
    int Close(
        int     inFd,
        int64_t inEof)
    {
        return -1;
    }
    void StartIo(
        Request&        inRequest,
        ReqType         inReqType,
        int             inFd,
        BlockIdx        inStartBlockIdx,
        int             inBufferCount,
        InputIterator*  inInputIteratorPtr,
        int64_t         inSpaceAllocSize,
        int64_t         inEof)
    {
    }
    void StartMeta(
        Request&    inRequest,
        ReqType     inReqType,
        const char* inNamePtr,
        const char* inName2Ptr)
    {
    }
private:
    S3RequestContext* mS3CtxPtr;
    CURLM*            mCurlCtxPtr;
    QCFdPoll          mFdPoll;
    long              mPollWaitMilliSec;
    int               mPollSocketCount;

    static int CurlTimerCB(
        CURLM* inCurlCtxPtr,
        long   inTimeoutMs,
        void*  inUserDataPtr)
    {
        return reinterpret_cast<Impl*>(inUserDataPtr)->SetTimeout(
            inCurlCtxPtr, inTimeoutMs
        );
    }
    static int CurlSocketCB(
        CURL*         /* inCurlPtr */,
        curl_socket_t inFd,
        int           inAction,
        void*         inUserDataPtr,
        void*         inSocketPtr)
    {
        return reinterpret_cast<Impl*>(inUserDataPtr)->SetSocket(
            inFd, inAction, inSocketPtr
        );
    }
    int SetTimeout(
        CURLM* inCurlCtxPtr,
        long   inTimeoutMs)
    {
        QCRTASSERT(mCurlCtxPtr == inCurlCtxPtr);
        mPollWaitMilliSec = inTimeoutMs;
        return 0;
    }
    int SetSocket(
        curl_socket_t inFd,
        int           inAction,
        void*         inSocketPtr)
    {
        const char* kSocketAddedPtr = "ADDED";
        int theStatus = 0;
        int theEvents = 0;
        switch(inAction) {
            case CURL_POLL_REMOVE:
                if (inSocketPtr) {
                    theStatus = mFdPoll.Remove(inFd);
                    QCASSERT(0 < mPollSocketCount);
                    if (CURLM_OK != curl_multi_assign(mCurlCtxPtr, inFd, 0)) {
                        InternalError("curl_multi_assign");
                    }
                    mPollSocketCount--;
                }
                break;
            case CURL_POLL_IN:
                theEvents |= QCFdPoll::kOpTypeIn;
                break;
            case CURL_POLL_OUT:
                theEvents |= QCFdPoll::kOpTypeOut;
                break;
            case CURL_POLL_INOUT:
                theEvents |= QCFdPoll::kOpTypeIn | QCFdPoll::kOpTypeOut;
                break;
            case CURL_POLL_NONE:
                break;
            default:
                InternalError("invalid set socket action", inAction);
                break;
        }
        if (theEvents) {
            char* const kNullPtr = 0;
            if (inSocketPtr) {
                theStatus = mFdPoll.Set(inFd, theEvents, kNullPtr + inFd);
            } else {
                theStatus = mFdPoll.Add(inFd, theEvents, kNullPtr + inFd);
                if (CURLM_OK != curl_multi_assign(
                        mCurlCtxPtr, inFd,
                        const_cast<char*>(kSocketAddedPtr + inFd))) {
                    InternalError("curl_multi_assign");
                }
                mPollSocketCount++;
            }
        }
        if (0 != theStatus) {
            KFS_LOG_STREAM_ERROR <<
                "S3: "      << (const void*)mS3CtxPtr <<
                " socket: " << inFd <<
                " action: " << inAction <<
                " "         << QCUtils::SysError(theStatus) <<
            KFS_LOG_EOM;
        }
        return 0;
    }
    int ConvertEvents(
            int inEvents)
    {
        int theEvents = 0;
        if ((theEvents & QCFdPoll::kOpTypeIn) != 0 ||
                (theEvents & QCFdPoll::kOpTypeHup) != 0) {
            theEvents |= CURL_CSELECT_IN;
        }
        if ((theEvents & QCFdPoll::kOpTypeOut) != 0) {
            theEvents |= CURL_CSELECT_OUT;
        }
        if ((theEvents & QCFdPoll::kOpTypeError) != 0) {
            theEvents |= CURL_CSELECT_ERR;
        }
        return theEvents;
    }
    void InternalError(
        const char* inMsgPtr,
        int         inStatus = 0)
    {
        const char* theMsgPtr = inMsgPtr ? inMsgPtr : "unspecified";
        KFS_LOG_STREAM_FATAL <<
            "S3: " << (const void*)mS3CtxPtr <<
            "internal error: " << theMsgPtr   <<
            " status: " << inStatus <<
        KFS_LOG_EOM;
        MsgLogger::Stop();
        QCUtils::FatalError(theMsgPtr, 0);
    }

private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

S3IO::S3IO()
    : mImpl(*(new Impl()))
    {}

    /* virtual */
S3IO::~S3IO()
{
    delete &mImpl;
}

    bool
S3IO::Init(
    const char* inUrlPtr)
{
   return mImpl.Init(inUrlPtr);
}

    void
S3IO::SetParameters(
    const char*       inPrefixPtr,
    const Properties& inParameters)
{
    mImpl.SetParameters(inPrefixPtr, inParameters);
}

    /* virtual */ void
S3IO::ProcessAndWait()
{
    mImpl.ProcessAndWait();
}

    /* virtual */ void
S3IO::Wakeup()
{
    mImpl.Wakeup();
}

    /* virtual */ void
S3IO::Stop()
{
    mImpl.Stop();
}

    /* virtual */ int
S3IO::Open(
    const char* inFileNamePtr,
    bool        inReadOnlyFlag,
    bool        inCreateFlag,
    bool        inCreateExclusiveFlag,
    int64_t&    ioMaxFileSize)
{
    return mImpl.Open(
        inFileNamePtr,
        inReadOnlyFlag,
        inCreateFlag,
        inCreateExclusiveFlag,
        ioMaxFileSize
    );
}

    /* virtual */ int
S3IO::Close(
    int     inFd,
    int64_t inEof)
{
    return mImpl.Close(inFd, inEof);
}

    /* virtual */ void
S3IO::StartIo(
    S3IO::Request&       inRequest,
    S3IO::ReqType        inReqType,
    int                  inFd,
    S3IO::BlockIdx       inStartBlockIdx,
    int                  inBufferCount,
    S3IO::InputIterator* inInputIteratorPtr,
    int64_t              inSpaceAllocSize,
    int64_t              inEof)
{
    mImpl.StartIo(
        inRequest,
        inReqType,
        inFd,
        inStartBlockIdx,
        inBufferCount,
        inInputIteratorPtr,
        inSpaceAllocSize,
        inEof
    );
}

    /* virtual */ void
S3IO::StartMeta(
    S3IO::Request& inRequest,
    S3IO::ReqType  inReqType,
    const char*    inNamePtr,
    const char*    inName2Ptr)
{
    mImpl.StartMeta(
        inRequest,
        inReqType,
        inNamePtr,
        inName2Ptr
    );
}

} // namespace KFS
