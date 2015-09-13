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

#include "chunk/IOMethodDef.h"

#include "common/MsgLogger.h"
#include "common/IntToString.h"

#include "qcdio/QCFdPoll.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcdebug.h"

#include <libs3.h>
#include <curl/curl.h>

#include <errno.h>

#include <string>
#include <vector>

namespace KFS
{

using std::string;
using std::vector;

char* const kS3IOFdOffset = (char*)0 + (size_t(1) << (sizeof(char*) * 8 - 2));

class S3IO : public IOMethod
{
public:
    typedef QCDiskQueue::Request       Request;
    typedef QCDiskQueue::ReqType       ReqType;
    typedef QCDiskQueue::BlockIdx      BlockIdx;
    typedef QCDiskQueue::InputIterator InputIterator;

    static IOMethod* New(
        const char*       inUrlPtr,
        const char*       inLogPrefixPtr,
        const char*       inParamsPrefixPtr,
        const Properties& inParameters)
    {
        if (! inUrlPtr) {
            return 0;
        }
        S3IO* const thePtr = new S3IO(inLogPrefixPtr);
        thePtr->SetParameters(inParamsPrefixPtr, inParameters);
        return thePtr;
    }
    virtual ~S3IO()
        { S3IO::Stop(); }
    virtual bool Init(
        QCDiskQueue& inDiskQueue)
    {
        mDiskQueuePtr = &inDiskQueue;
        mBlockSize = mDiskQueuePtr->GetBlockSize();
        if (mBlockSize <= 0) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                " invalid block size: " << mBlockSize <<
            KFS_LOG_EOM;
            return false;
        }
        if (! mS3CtxPtr &&
                S3StatusOK != S3_create_request_context(&mS3CtxPtr)) {
            return false;
        }
        mCurlCtxPtr = reinterpret_cast<CURLM*>(
            S3_get_curl_request_context(mS3CtxPtr));
        CURLMcode theStatus;
        if (CURLM_OK != (theStatus = curl_multi_setopt(
                mCurlCtxPtr, CURLMOPT_SOCKETDATA, this))) {
            FatalError("curl_multi_setopt(CURLMOPT_SOCKETDATA)", theStatus);
        }
        if (CURLM_OK != (theStatus = curl_multi_setopt(mCurlCtxPtr,
                CURLMOPT_SOCKETFUNCTION, &CurlSocketCB))) {
            FatalError("curl_multi_setopt(CURLMOPT_SOCKETFUNCTION)", theStatus);
        }
        if (CURLM_OK != (theStatus = curl_multi_setopt(
                mCurlCtxPtr, CURLMOPT_TIMERDATA, this))) {
            FatalError("curl_multi_setopt(CURLMOPT_TIMERDATA)", theStatus);
        }
        if (CURLM_OK != (theStatus = curl_multi_setopt(
                mCurlCtxPtr, CURLMOPT_TIMERFUNCTION, &CurlTimerCB))) {
            FatalError("curl_multi_setopt(CURLMOPT_TIMERFUNCTION)", theStatus);
        }
        return true;
    }
    virtual void SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters)
    {
    }
    virtual void ProcessAndWait()
    {
        const int theErr =
            mFdPoll.Poll(mPollSocketCount, (int)mPollWaitMilliSec);
        if (theErr < 0 && theErr != -EINTR && theErr != -EAGAIN) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                QCUtils::SysError(-theErr, "poll error") <<
            KFS_LOG_EOM;
        }
        int       theEvents;
        void*     thePtr;
        CURLMcode theStatus;
        int       theRemCount = 0;
        int       theCount    = 0;
        while (mFdPoll.Next(theEvents, thePtr)) {
            if (! thePtr) {
                continue;
            }
            theCount++;
            curl_socket_t theFd = (curl_socket_t)(
                reinterpret_cast<const char*>(thePtr) - kS3IOFdOffset);
            while ((theStatus = curl_multi_socket_action(
                    mCurlCtxPtr,
                    theFd,
                    ConvertEvents(theEvents),
                    &theRemCount)) == CURLM_CALL_MULTI_PERFORM)
                {}
            if (CURLM_OK != theStatus) {
                FatalError("curl_multi_socket_action", theStatus);
            }
        }
        if (theCount <= 0 &&  0 <= mPollWaitMilliSec) {
            if ((theStatus = curl_multi_socket_action(
                    mCurlCtxPtr, CURL_SOCKET_TIMEOUT, 0, &theRemCount))) {
                FatalError("curl_multi_socket_action(CURL_SOCKET_TIMEOUT)",
                    theStatus);
            }
        }
        const S3Status theS3Status = S3_finish_request_context(mS3CtxPtr);
        if (S3StatusOK != theS3Status) {
            FatalError("S3_finish_request_context", theS3Status);
        }
    }
    virtual void Wakeup()
    {
        if (! mFdPoll.Wakeup()) {
            FatalError("wake failure");
        }
    }
    virtual void Stop()
    {
        Wakeup();
    }
    virtual int Open(
        const char* inFileNamePtr,
        bool        inReadOnlyFlag,
        bool        inCreateFlag,
        bool        inCreateExclusiveFlag,
        int64_t&    ioMaxFileSize)
    {
        if (! inFileNamePtr || ! *inFileNamePtr) {
            return -EINVAL;
        }
        const int theFd = NewFd();
        if (0 <= theFd) {
            QCASSERT(! mFileTable[theFd]);
            mFileTable[theFd] = new File(
                inFileNamePtr,
                inReadOnlyFlag,
                inCreateFlag,
                inCreateExclusiveFlag,
                ioMaxFileSize
            );
        }
        return theFd;
    }
    virtual int Close(
        int     inFd,
        int64_t inEof)
    {
        if (inFd < 0 || mFileTable.size() < (size_t)inFd ||
                ! mFileTable[inFd]) {
            return -EBADF;
        }
        int theRet = 0;
        File& theFile = *mFileTable[inFd];
        if (! theFile.mReadOnlyFlag &&  theFile.mMaxFileSize < inEof) {
            theRet = -EINVAL;
        }
        if (mFileTable.size() == (size_t)inFd + 1) {
            mFileTable.pop_back();
        } else {
            mFileTable[inFd] = 0;
            mFreeFdList.push_back(inFd);
        }
        theFile.Close(inEof);
        return theRet;
    }
    virtual void StartIo(
        Request&        inRequest,
        ReqType         inReqType,
        int             inFd,
        BlockIdx        inStartBlockIdx,
        int             inBufferCount,
        InputIterator*  inInputIteratorPtr,
        int64_t         inSpaceAllocSize,
        int64_t         inEof)
    {
        QCDiskQueue::Error theError   = QCDiskQueue::kErrorNone;
        int                theSysErr  = 0;
        File*              theFilePtr = GetFilePtr(inFd);
        switch (inReqType) {
            case QCDiskQueue::kReqTypeRead:
                if (! theFilePtr) {
                    theError  = QCDiskQueue::kErrorRead;
                    theSysErr = EBADF;
                    break;
                }
                break;
            case QCDiskQueue::kReqTypeWrite:
            case QCDiskQueue::kReqTypeWriteSync:
                if (! theFilePtr || theFilePtr->mReadOnlyFlag) {
                    theError  = QCDiskQueue::kErrorWrite;
                    theSysErr = theFilePtr ? EINVAL : EBADF;
                    break;
                }
                if (QCDiskQueue::kReqTypeWriteSync == inReqType && inEof < 0) {
                    theError  = QCDiskQueue::kErrorParameter;
                    break;
                }
                if (0 <= inEof) {
                    theFilePtr->mMaxFileSize = inEof;
                }
                break;
            default:
                theError  = QCDiskQueue::kErrorParameter;
                theSysErr = theFilePtr ? ENXIO : EBADF;
                break;
        }
        if (QCDiskQueue::kErrorNone != theError || 0 != theSysErr) {
            int64_t theIoByteCount = 0;
            mDiskQueuePtr->Done(
                *this,
                inRequest,
                theError,
                theSysErr,
                theIoByteCount,
                inStartBlockIdx
            );
        }
    }
    virtual void StartMeta(
        Request&    inRequest,
        ReqType     inReqType,
        const char* inNamePtr,
        const char* inName2Ptr)
    {
    }
private:
    class File
    {
    public:
        File(
            const char* inFileNamePtr,
            bool        inReadOnlyFlag,
            bool        inCreateFlag,
            bool        inCreateExclusiveFlag,
            int64_t     ioMaxFileSize)
            : mFileName(inFileNamePtr ? inFileNamePtr : ""),
              mReadOnlyFlag(inReadOnlyFlag),
              mCreateExclusiveFlag(inCreateExclusiveFlag),
              mMaxFileSize(ioMaxFileSize)
            {}
        void Close(
            int64_t inEof)
        {
        }
        string  mFileName;
        bool    mReadOnlyFlag;
        bool    mCreateFlag;
        bool    mCreateExclusiveFlag;
        int64_t mMaxFileSize;
    };
    typedef vector<File*> FileTable;
    typedef vector<int>   FreeFdList;

    QCDiskQueue*      mDiskQueuePtr;
    int               mBlockSize;
    string            mLogPrefix;
    S3RequestContext* mS3CtxPtr;
    CURLM*            mCurlCtxPtr;
    QCFdPoll          mFdPoll;
    long              mPollWaitMilliSec;
    int               mPollSocketCount;
    FileTable         mFileTable;
    FreeFdList        mFreeFdList;
    string            mBucketName;
    string            mAccessKeyId;
    string            mSecretAccessKey;

    S3IO(
        const char* inLogPrefixPtr)
        : IOMethod(),
          mDiskQueuePtr(0),
          mBlockSize(0),
          mLogPrefix(inLogPrefixPtr ? inLogPrefixPtr : ""),
          mS3CtxPtr(0),
          mCurlCtxPtr(0),
          mFdPoll(true), // Wakeable
          mPollWaitMilliSec(1000),
          mPollSocketCount(0),
          mFileTable(),
          mFreeFdList()
    {
        if (! inLogPrefixPtr) {
            mLogPrefix += "S3IO ";
            AppendHexIntToString(mLogPrefix, this - (S3IO*)0);
            mLogPrefix +=  ' ';
        } else if (! mLogPrefix.empty() && *mLogPrefix.rbegin() != ' ') {
            mLogPrefix += ' ';
        }
        mFileTable.reserve(2 << 10);
        mFileTable.push_back(0); // Reserve first slot, to fds start from 1.
        mFreeFdList.reserve(2 << 10);
    }
    int NewFd()
    {
        if (mFreeFdList.empty()) {
            mFileTable.push_back(0);
            return (int)(mFileTable.size() - 1);
        }
        const int theFd = mFreeFdList.back();
        mFreeFdList.pop_back();
        return theFd;
    }
    void RemvoeFile(
        int inFd)
    {
    }
    File* GetFilePtr(
        int inFd)
    {
        return ((inFd < 0 || mFileTable.size() <= (size_t)inFd) ?
            0 : mFileTable[inFd]);
        
    }
    static int CurlTimerCB(
        CURLM* inCurlCtxPtr,
        long   inTimeoutMs,
        void*  inUserDataPtr)
    {
        return reinterpret_cast<S3IO*>(inUserDataPtr)->SetTimeout(
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
        return reinterpret_cast<S3IO*>(inUserDataPtr)->SetSocket(
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
                        FatalError("curl_multi_assign");
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
                FatalError("invalid set socket action", inAction);
                break;
        }
        if (theEvents) {
            if (inSocketPtr) {
                theStatus = mFdPoll.Set(inFd, theEvents, kS3IOFdOffset + inFd);
            } else {
                theStatus = mFdPoll.Add(inFd, theEvents, kS3IOFdOffset + inFd);
                if (CURLM_OK != curl_multi_assign(
                        mCurlCtxPtr, inFd,
                        const_cast<char*>(kSocketAddedPtr + inFd))) {
                    FatalError("curl_multi_assign");
                }
                mPollSocketCount++;
            }
        }
        if (0 != theStatus) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
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
    void FatalError(
        const char* inMsgPtr,
        int         inStatus = 0)
    {
        const char* theMsgPtr = inMsgPtr ? inMsgPtr : "unspecified";
        KFS_LOG_STREAM_FATAL << mLogPrefix <<
            "internal error: " << theMsgPtr   <<
            " status: "        << inStatus <<
        KFS_LOG_EOM;
        MsgLogger::Stop();
        QCUtils::FatalError(theMsgPtr, 0);
    }

private:
    S3IO(
        const S3IO& inS3IO);
    S3IO& operator=(
        const S3IO& inS3IO);
};

KFS_REGISTER_IO_METHOD(KFS_IO_METHOD_NAME_S3IO, S3IO::New);

} // namespace KFS
