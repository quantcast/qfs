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
#include "common/MdStream.h"

#include "qcdio/QCFdPoll.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcdebug.h"

#include <libs3.h>
#include <curl/curl.h>

#include <errno.h>

#include <string>
#include <vector>
#include <algorithm>

namespace KFS
{

using std::string;
using std::vector;
using std::max;
using std::min;

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
        S3Status theS3Status;
        if (! mS3CtxPtr && S3StatusOK !=
                (theS3Status = S3_create_request_context(&mS3CtxPtr))) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "S3_create_request_context failure: " << theS3Status <<
            KFS_LOG_EOM;
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
                ioMaxFileSize,
                mGeneration++
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
        int64_t         /* inSpaceAllocSize */,
        int64_t         inEof)
    {
        QCDiskQueue::Error theError   = QCDiskQueue::kErrorNone;
        int                theSysErr  = 0;
        File*              theFilePtr = GetFilePtr(inFd);
        S3Req*             theReqPtr  = 0;
        int64_t            theEnd;
        switch (inReqType) {
            case QCDiskQueue::kReqTypeRead:
                if (! theFilePtr) {
                    theError  = QCDiskQueue::kErrorRead;
                    theSysErr = EBADF;
                    break;
                }
                if (inStartBlockIdx < 0) {
                    theError  = QCDiskQueue::kErrorRead;
                    theSysErr = EINVAL;
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "invalid read start position: " << inStartBlockIdx <<
                    KFS_LOG_EOM;
                    break;
                }
                theReqPtr = S3Req::Get(
                    *this,
                    inFd,
                    inStartBlockIdx * mBlockSize,
                    inBufferCount,
                    inInputIteratorPtr,
                    inBufferCount * mBlockSize,
                    theFilePtr->mGeneration
                );
                if (! theReqPtr) {
                    theError  = QCDiskQueue::kErrorRead;
                    theSysErr = EINVAL;
                    break;
                }
                Read(*theReqPtr);
                return;
            case QCDiskQueue::kReqTypeWrite:
            case QCDiskQueue::kReqTypeWriteSync:
                if (! theFilePtr || theFilePtr->mReadOnlyFlag ||
                        inBufferCount <= 0) {
                    theError  = QCDiskQueue::kErrorWrite;
                    theSysErr = theFilePtr ? EINVAL : EBADF;
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "invalid write attempt into read only file" <<
                    KFS_LOG_EOM;
                    break;
                }
                if (QCDiskQueue::kReqTypeWrite == inReqType ||
                        inStartBlockIdx != 0) {
                    theError  = QCDiskQueue::kErrorWrite;
                    theSysErr = EINVAL;
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "partial write is not spported yet" <<
                        " block index: " << inStartBlockIdx <<
                    KFS_LOG_EOM;
                    break;
                }
                if (QCDiskQueue::kReqTypeWriteSync == inReqType && inEof < 0) {
                    theError  = QCDiskQueue::kErrorParameter;
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "invalid sync write EOF: " << inEof <<
                        " max: " << theFilePtr->mMaxFileSize <<
                    KFS_LOG_EOM;
                    break;
                }
                if (0 <= inEof) {
                    theFilePtr->mMaxFileSize = inEof;
                }
                theEnd = (inStartBlockIdx + inBufferCount) * mBlockSize;
                if (theFilePtr->mMaxFileSize + mBlockSize < theEnd) {
                    theError  = QCDiskQueue::kErrorParameter;
                    theSysErr = EINVAL;
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "write past last block: " << theEnd <<
                        " max: " << theFilePtr->mMaxFileSize <<
                    KFS_LOG_EOM;
                    break;
                }
                theReqPtr = S3Req::Get(
                    *this,
                    inFd,
                    inStartBlockIdx * mBlockSize,
                    inBufferCount,
                    inInputIteratorPtr,
                    inBufferCount * mBlockSize -
                        (theFilePtr->mMaxFileSize < theEnd ?
                            theEnd - theFilePtr->mMaxFileSize : 0),
                    theFilePtr->mGeneration
                );
                if (! theReqPtr) {
                    theError  = QCDiskQueue::kErrorWrite;
                    theSysErr = EINVAL;
                    break;
                }
                Write(*theReqPtr);
                return;
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
            int64_t     inMaxFileSize,
            uint64_t    inGeneration)
            : mFileName(inFileNamePtr ? inFileNamePtr : ""),
              mReadOnlyFlag(inReadOnlyFlag),
              mCreateExclusiveFlag(inCreateExclusiveFlag),
              mMaxFileSize(inMaxFileSize),
              mGeneration(inGeneration)
            {}
        void Close(
            int64_t inEof)
        {
        }
        string         mFileName;
        bool           mReadOnlyFlag;
        bool           mCreateFlag;
        bool           mCreateExclusiveFlag;
        int64_t        mMaxFileSize;
        uint64_t const mGeneration;
    };
    typedef vector<File*> FileTable;
    typedef vector<int>   FreeFdList;
    class S3Req
    {
    public:
        static S3Req* Get(
            S3IO&          inOuter,
            int            inFd,
            BlockIdx       inStartBlockIdx,
            int            inBufferCount,
            InputIterator* inInputIteratorPtr,
            size_t         inSize,
            uint64_t       inGeneration)
        {
            char* const theMemPtr = new char[
                sizeof(S3Req) + sizeof(char*) * max(0, inBufferCount)];
            S3Req& theReq = *(new (theMemPtr) S3Req(
                inOuter,
                inFd,
                inStartBlockIdx,
                inBufferCount,
                inSize,
                inGeneration
            ));
            if (0 < inBufferCount) {
                if (inSize <= 0) {
                    KFS_LOG_STREAM_ERROR << inOuter.mLogPrefix <<
                        "invalid empty write attempt" <<
                    KFS_LOG_EOM;
                    theReq.Dispose();
                    return 0;
                }
                if (! inInputIteratorPtr) {
                    KFS_LOG_STREAM_ERROR << inOuter.mLogPrefix <<
                        "invalid null buffer iterator"
                        " buffer count: " << inBufferCount <<
                    KFS_LOG_EOM;
                    theReq.Dispose();
                    return 0;
                }
                char** theBuffersPtr = theReq.GetBuffers();
                for (int i = 0; i < inBufferCount; i++) {
                    char* const thePtr = inInputIteratorPtr->Get();
                    if (! thePtr) {
                        KFS_LOG_STREAM_ERROR << inOuter.mLogPrefix <<
                            "invalid null buffer, pos:" << i <<
                            " expected: " << inBufferCount << " buffers" <<
                        KFS_LOG_EOM;
                        theReq.Dispose();
                        return 0;
                    }
                    *theBuffersPtr++ = thePtr;
                }
            }
            return &theReq;
        }
        void Dispose()
        {
            char* const thePtr = reinterpret_cast<char*>(this);
            this->~S3Req();
            delete [] thePtr;
        }
        const char* GetMd5Sum()
        {
            if (mMd5Sum.empty()) {
                MdStream theStream(0, true, string(), 0);
                char** thePtr = GetBuffers();
                size_t thePos;
                for (thePos = 0;
                        thePos + mOuter.mBlockSize <= mSize;
                        thePos += mOuter.mBlockSize) {
                    theStream.write(*thePtr++, mOuter.mBlockSize);
                }
                if (thePos < mSize) {
                    theStream.write(*thePtr, mSize - thePos);
                }
                mMd5Sum = theStream.GetMd();
            }
            return mMd5Sum.c_str();
        }
        size_t GetSize() const
            { return mSize; }
        int GetFd() const
            { return mFd; }
        size_t GetStartPos()
            { return mStartPos; }
        uint64_t GetGeneration() const
            { return mGeneration; }
        int Write(
            int   inBufferSize,
            char* inBufferPtr)
        {
            if (mSize <= mPos || inBufferSize <= 0) {
                return 0;
            }
            char*       thePtr    = inBufferPtr;
            char* const theEndPtr = thePtr + inBufferSize;
            size_t      theSize;
            if (mPos <= 0) {
                mBufferPtr = GetBuffers();
                mBufRem    = 0;
            } else if (0 < mBufRem) {
                theSize = min(mBufRem, (size_t)(theEndPtr - thePtr));
                memcpy(thePtr,
                    *mBufferPtr + mOuter.mBlockSize - mBufRem, theSize);
                mBufRem -= theSize;
                if (0 < mBufRem) {
                    return (int)theSize;
                }
                mBufferPtr++;
                thePtr += theSize;
            }
            theSize = mOuter.mBlockSize;
            while (mPos + theSize <= mSize && thePtr + theSize <= theEndPtr) {
                memcpy(thePtr, *mBufferPtr++, theSize);
                mPos   += theSize;
                thePtr += theSize;
            }
            theSize = min(mSize - mPos, (size_t)(theEndPtr - thePtr));
            if (0 < theSize) {
                memcpy(thePtr, *mBufferPtr, theSize);
                thePtr += theSize;
                mPos   += theSize;
                mBufRem = mOuter.mBlockSize - theSize;
            }
            return (thePtr - (theEndPtr - inBufferSize));
        }
        void Done(
            S3Status              inStatus,
            const S3ErrorDetails* inErrorPtr)
        {
            if (S3_status_is_retryable(inStatus)) {
            }
        }
    private:
        S3IO&          mOuter;
        int const      mFd;
        size_t         mStartPos;
        size_t         mPos;
        size_t         mSize;
        size_t         mBufRem;
        char**         mBufferPtr;
        uint64_t const mGeneration;
        string         mMd5Sum;

        S3Req(
            S3IO&          inOuter,
            int            inFd,
            size_t         inStartPos,
            int            inBufferCount,
            size_t         inSize,
            uint64_t       inGeneration)
            : mOuter(inOuter),
              mFd(inFd),
              mStartPos(inStartPos),
              mPos(0),
              mSize(inSize),
              mBufRem(0),
              mBufferPtr(0),
              mGeneration(inGeneration),
              mMd5Sum()
            {}

        char** GetBuffers()
            { return reinterpret_cast<char**>(this + 1); }
        ~S3Req()
            {}
    };
    friend class S3Req;

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
    string            mS3HostName;
    string            mBucketName;
    string            mAccessKeyId;
    string            mSecretAccessKey;
    string            mSecurityToken;
    string            mContentType;
    string            mCacheControl;
    string            mContentDispositionFilename;
    string            mContentEncoding;
    int64_t           mObjectExpires;
    S3CannedAcl       mCannedAcl;
    bool              mUseServerSideEncryptionFlag;
    S3Protocol        mS3Protocol;
    S3UriStyle        mS3UriStyle;
    uint64_t          mGeneration;

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
          mFreeFdList(),
          mS3HostName(),
          mBucketName(),
          mAccessKeyId(),
          mSecretAccessKey(),
          mSecurityToken(),
          mContentType(),
          mCacheControl(),
          mContentDispositionFilename(),
          mContentEncoding(),
          mObjectExpires(-1),
          mCannedAcl(S3CannedAclPrivate),
          mUseServerSideEncryptionFlag(false),
          mS3Protocol(S3ProtocolHTTPS),
          mS3UriStyle(S3UriStyleVirtualHost),
          mGeneration(1)
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
    File* GetFilePtr(
        int inFd)
    {
        return ((inFd < 0 || mFileTable.size() <= (size_t)inFd) ?
            0 : mFileTable[inFd]);
    }
    void Read(
        S3Req& inReq)
    {
    }
    void Write(
        S3Req& inReq)
    {
        S3BucketContext theBucketContext =
        {
            mS3HostName.empty() ? 0 : mS3HostName.c_str(),
            mBucketName.c_str(),
            mS3Protocol,
            mS3UriStyle,
            mAccessKeyId.c_str(),
            mSecretAccessKey.c_str(),
            mSecurityToken.empty() ? 0 : mSecurityToken.c_str()
        };
        S3PutProperties thePutProperties =
        {
            mContentType.empty()  ? 0 : mContentType.c_str(),
            inReq.GetMd5Sum(),
            mCacheControl.empty() ? 0 : mCacheControl.c_str(),
            mContentDispositionFilename.empty() ?
                0 : mContentDispositionFilename.c_str(),
            mContentEncoding.empty() ? 0  : mContentEncoding.c_str(),
            mObjectExpires,
            mCannedAcl,
            0, // metaPropertiesCount,
            0, // metaProperties,
            mUseServerSideEncryptionFlag ? 1 : 0
        };
        S3PutObjectHandler thePutObjectHandler =
        {
            {
                &S3ResponsePropertiesCB,
                &S3ResponseCompleteCB
            },
            &S3PutObjectDataCB
        };
        S3_put_object(
            &theBucketContext,
            mFileTable[inReq.GetFd()]->mFileName.c_str(),
            inReq.GetSize(),
            &thePutProperties,
            0,
            &thePutObjectHandler,
            &inReq
        );
    }
    static S3Status S3ResponsePropertiesCB(
        const S3ResponseProperties* inPropertiesPtr,
        void*                       inUserDataPtr)
    {
       // return reinterpret_cast<S3Req*>(inUserDataPtr)->
       return S3StatusOK;
    }
    static void S3ResponseCompleteCB(
        S3Status              inStatus,
        const S3ErrorDetails* inErrorPtr,
        void*                 inUserDataPtr)
    {
        reinterpret_cast<S3Req*>(inUserDataPtr)->Done(
            inStatus, inErrorPtr);
    }
    static int S3PutObjectDataCB(
        int    inBufferSize,
        char*  inBufferPtr,
        void*  inUserDataPtr)
    {
        return reinterpret_cast<S3Req*>(inUserDataPtr)->Write(
            inBufferSize, inBufferPtr);
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
