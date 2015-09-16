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
#include "common/TimerWheel.h"
#include "common/Properties.h"

#include "qcdio/QCFdPoll.h"
#include "qcdio/QCUtils.h"
#include "qcdio/QCDLList.h"
#include "qcdio/QCMutex.h"
#include "qcdio/qcdebug.h"
#include "qcdio/qcstutils.h"

#include <libs3.h>
#include <curl/curl.h>

#include <errno.h>
#include <string.h>

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

    enum {
        kTimerResolutionSec = 1,
        kMaxTimerTimeSec    = 512
    };

    static IOMethod* New(
        const char*       inUrlPtr,
        const char*       inLogPrefixPtr,
        const char*       inParamsPrefixPtr,
        const Properties& inParameters)
    {
        if (! inUrlPtr ||
                inUrlPtr[0] != 's' ||
                inUrlPtr[1] != '3' ||
                inUrlPtr[2] != ':' ||
                inUrlPtr[3] != '/' ||
                inUrlPtr[4] != '/') {
            return 0;
        }
        const char* kDefaultHostName = 0;
        if (S3_initialize("s3", S3_INIT_ALL, kDefaultHostName) != S3StatusOK) {
            KFS_LOG_STREAM_ERROR <<
                "S3_initialize failure: " << S3StatusOK <<
            KFS_LOG_EOM;
            return 0;
        }
        string theConfigPrefix = inUrlPtr + 5;
        while (! theConfigPrefix.empty() && *theConfigPrefix.rbegin() == '/') {
            theConfigPrefix.resize(theConfigPrefix.size() - 1);
        }
        S3IO* const thePtr = new S3IO(
            inUrlPtr, theConfigPrefix.c_str(), inLogPrefixPtr);
        thePtr->SetParameters(inParamsPrefixPtr, inParameters);
        return thePtr;
    }
    virtual ~S3IO()
    {
        S3IO::Stop();
        S3_deinitialize();
    }
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
        QCStMutexLocker theLock(mMutex);
        mUpdatedParameters.Set(inPrefixPtr, mConfigPrefix, inParameters);
        mParametersUpdatedFlag = true;
    }
    virtual void ProcessAndWait()
    {
        if (mStopFlag) {
            Stop();
            return;
        }
        const int kMaxPollSleepMs = kTimerResolutionSec * 1000;
        const int theErr =
            mFdPoll.Poll(mPollSocketCount, kMaxPollSleepMs);
        if (theErr < 0 && theErr != -EINTR && theErr != -EAGAIN) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                QCUtils::SysError(-theErr, "poll error") <<
            KFS_LOG_EOM;
        }
        QCStMutexLocker theLock(mMutex);
        if (mParametersUpdatedFlag) {
            mParameters = mUpdatedParameters;
        }
        theLock.Unlock();
        mNow = time(0);
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
        Timer();
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
        mStopFlag = true;
        int theRemCount;
        do {
            int theStatus = 0;
            for (curl_socket_t theFd = 0;
                    0 < mPollSocketCount && 0 < theRemCount &&
                        theFd < mCurlFdTable.size();
                    ++theFd) {
                if (! mCurlFdTable[theFd]) {
                    continue;
                }
                theRemCount = 0;
                while ((theStatus = curl_multi_socket_action(
                        mCurlCtxPtr,
                        theFd,
                        ConvertEvents(QCFdPoll::kOpTypeError),
                        &theRemCount)) == CURLM_CALL_MULTI_PERFORM)
                    {}
            }
            if (mCurlTimer.IsScheduled()) {
                mCurlTimer.Schedule(-1);
                theRemCount = CurlTimer();
            }
        } while(0 < theRemCount);
    }
    virtual int Open(
        const char* inFileNamePtr,
        bool        inReadOnlyFlag,
        bool        inCreateFlag,
        bool        inCreateExclusiveFlag,
        int64_t&    ioMaxFileSize)
    {
        if (! inFileNamePtr) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "invalid null file name" <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        const size_t theLen = strlen(inFileNamePtr);
        if (theLen <= mFilePrefix.size() ||
                mFilePrefix.compare(0, theLen, inFileNamePtr) != 0) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "invalid file name: " << inFileNamePtr <<
                " file prefix: "      << mFilePrefix <<
            KFS_LOG_EOM;
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
                        " " << theFilePtr->mFileName <<
                    KFS_LOG_EOM;
                    break;
                }
                if (theFilePtr->mWriteOnlyFlag) {
                    theError  = QCDiskQueue::kErrorRead;
                    theSysErr = EINVAL;
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "invalid read on write only file: " <<
                            theFilePtr->mFileName <<
                    KFS_LOG_EOM;
                    break;
                }
                theReqPtr = S3Req::Get(
                    *this,
                    inRequest,
                    inReqType,
                    inFd,
                    inStartBlockIdx,
                    inBufferCount,
                    inInputIteratorPtr,
                    inBufferCount * mBlockSize,
                    *theFilePtr
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
                        "invalid write attempt into read only file: " <<
                        theFilePtr->mFileName <<
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
                        " : " << theFilePtr->mFileName <<
                    KFS_LOG_EOM;
                    break;
                }
                if (QCDiskQueue::kReqTypeWriteSync == inReqType && inEof < 0) {
                    theError  = QCDiskQueue::kErrorParameter;
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "invalid sync write EOF: " << inEof <<
                        " max: " << theFilePtr->mMaxFileSize <<
                        " : " << theFilePtr->mFileName <<
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
                        " : " << theFilePtr->mFileName <<
                    KFS_LOG_EOM;
                    break;
                }
                theFilePtr->mWriteOnlyFlag = true;
                theReqPtr = S3Req::Get(
                    *this,
                    inRequest,
                    inReqType,
                    inFd,
                    inStartBlockIdx,
                    inBufferCount,
                    inInputIteratorPtr,
                    inBufferCount * mBlockSize -
                        (theFilePtr->mMaxFileSize < theEnd ?
                            theEnd - theFilePtr->mMaxFileSize : 0),
                    *theFilePtr
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
              mWriteOnlyFlag(false),
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
        bool           mWriteOnlyFlag;
        int64_t        mMaxFileSize;
        uint64_t const mGeneration;
    };
    typedef vector<File*> FileTable;
    typedef vector<int>   FreeFdList;
    class Parameters
    {
    public:
        Parameters()
            : mS3HostName(),
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
              mMaxRetryCount(10),
              mRetryInterval(10)
            {}
        void Set(
            const char*       inPrefixPtr,
            const string&     inConfigPrefix,
            const Properties& inParameters)
        {
            Properties::String theName;
            if (inPrefixPtr) {
                theName.Append(inPrefixPtr);
            }
            theName.Append(inConfigPrefix);
            const size_t thePrefixSize = theName.GetSize();
            mS3HostName = inParameters.getValue(
                theName.Truncate(thePrefixSize).Append("hostName"),
                mS3HostName
            );
            mBucketName = inParameters.getValue(
                theName.Truncate(thePrefixSize).Append("bucketName"),
                mBucketName
            );
            mAccessKeyId = inParameters.getValue(
                theName.Truncate(thePrefixSize).Append("accessKeyId"),
                mAccessKeyId
            );
            mSecurityToken = inParameters.getValue(
                theName.Truncate(thePrefixSize).Append("securityToken"),
                mSecurityToken
            );
            mContentType = inParameters.getValue(
                theName.Truncate(thePrefixSize).Append("contentType"),
                mContentType
            );
            mCacheControl = inParameters.getValue(
                theName.Truncate(thePrefixSize).Append("cacheControl"),
                mCacheControl
            );
            mContentDispositionFilename = inParameters.getValue(
                theName.Truncate(thePrefixSize).Append(
                    "contentDispositionFilename"),
                mContentDispositionFilename
            );
            mContentEncoding = inParameters.getValue(
                theName.Truncate(thePrefixSize).Append("contentEncoding"),
                mContentEncoding
            );
            mObjectExpires = inParameters.getValue(
                theName.Truncate(thePrefixSize).Append("objectExpires"),
                mObjectExpires
            );
            mUseServerSideEncryptionFlag = inParameters.getValue(
                theName.Truncate(thePrefixSize).Append("useServerSideEncryption"),
                mUseServerSideEncryptionFlag ? 1 : 0
            ) != 0;
            const Properties::String* theValPtr;
            if ((theValPtr = inParameters.getValue(
                    theName.Truncate(thePrefixSize).Append("cannedAcl")))) {
                if (*theValPtr == "private") {
                    mCannedAcl = S3CannedAclPrivate;
                } else if (*theValPtr == "publicRead") {
                    mCannedAcl = S3CannedAclPublicRead;
                } else if (*theValPtr == "publicReadWrite") {
                    mCannedAcl = S3CannedAclPublicReadWrite;
                } else if (*theValPtr == "authenticatedRead") {
                    mCannedAcl = S3CannedAclAuthenticatedRead;
                }
            }
            if ((theValPtr = inParameters.getValue(
                    theName.Truncate(thePrefixSize).Append("protocol")))) {
                if (*theValPtr == "https") {
                    mS3Protocol = S3ProtocolHTTPS;
                } else if (*theValPtr == "http") {
                    mS3Protocol = S3ProtocolHTTP;
                }
            }
            if ((theValPtr = inParameters.getValue(
                    theName.Truncate(thePrefixSize).Append("uriStyle")))) {
                if (*theValPtr == "virtualHost") {
                    mS3UriStyle = S3UriStyleVirtualHost;
                } else if (*theValPtr == "path") {
                    mS3UriStyle = S3UriStylePath;
                }
            }
            mMaxRetryCount = inParameters.getValue(
                theName.Truncate(thePrefixSize).Append("maxRetryCount"),
                mMaxRetryCount
            );
            mRetryInterval = inParameters.getValue(
                theName.Truncate(thePrefixSize).Append("retryInterval"),
                mRetryInterval
            );
        }
        string      mS3HostName;
        string      mBucketName;
        string      mAccessKeyId;
        string      mSecretAccessKey;
        string      mSecurityToken;
        string      mContentType;
        string      mCacheControl;
        string      mContentDispositionFilename;
        string      mContentEncoding;
        int64_t     mObjectExpires;
        S3CannedAcl mCannedAcl;
        bool        mUseServerSideEncryptionFlag;
        S3Protocol  mS3Protocol;
        S3UriStyle  mS3UriStyle;
        int         mMaxRetryCount;
        int         mRetryInterval;
    };
    class S3ReqBase
    {
    public:
        typedef QCDLListOp<S3ReqBase> List;
        S3ReqBase()
            { List::Init(*this); }
    private:
        S3ReqBase* mPrevPtr[1];
        S3ReqBase* mNextPtr[1];
        friend class QCDLListOp<S3ReqBase>;
    };
    class S3Req : public S3ReqBase
    {
    public:
        typedef S3ReqBase::List List;
        S3Req(
            S3IO* inOuterPtr = 0)
            : S3ReqBase(),
              mOuterPtr(inOuterPtr),
              mRequestPtr(0),
              mReqType(QCDiskQueue::kReqTypeNone),
              mFd(-1),
              mGeneration(0),
              mStartBlockIdx(0),
              mFileName(),
              mParameters(),
              mMd5Sum(),
              mPos(0),
              mSize(0),
              mBufRem(0),
              mBufferPtr(0),
              mRetryCount(0),
              mStartTime(mOuterPtr ? mOuterPtr->mNow : 0)
            {}
        static S3Req* Get(
            S3IO&          inOuter,
            Request&       inRequest,
            ReqType        inReqType,
            int            inFd,
            BlockIdx       inStartBlockIdx,
            int            inBufferCount,
            InputIterator* inInputIteratorPtr,
            size_t         inSize,
            File&          inFile)
        {
            char* const theMemPtr = new char[
                sizeof(S3Req) + sizeof(char*) * max(0, inBufferCount)];
            S3Req& theReq = *(new (theMemPtr) S3Req(
                inOuter,
                inRequest,
                inReqType,
                inFd,
                inStartBlockIdx,
                inBufferCount,
                inSize,
                inFile
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
                theReq.mBufferPtr = theReq.GetBuffers();
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
                ostream& theStream = mOuterPtr->mMdStream.Reset();
                char** thePtr = GetBuffers();
                size_t thePos;
                for (thePos = 0;
                        thePos + mOuterPtr->mBlockSize <= mSize;
                        thePos += mOuterPtr->mBlockSize) {
                    theStream.write(*thePtr++, mOuterPtr->mBlockSize);
                }
                if (thePos < mSize) {
                    theStream.write(*thePtr, mSize - thePos);
                }
                if (theStream) {
                    mMd5Sum = mOuterPtr->mMdStream.GetMd();
                } else {
                    mOuterPtr->FatalError("md5 sum failure");
                }
            }
            return mMd5Sum.c_str();
        }
        size_t GetSize() const
            { return mSize; }
        int GetFd() const
            { return mFd; }
        size_t GetStartBlockIdx()
            { return mStartBlockIdx; }
        uint64_t GetGeneration() const
            { return mGeneration; }
        template<typename T, typename FT>
        int IO(
            int       inBufferSize,
            T*        inBufferPtr,
            const FT& inFunc)
        {
            if (mSize <= mPos || inBufferSize <= 0) {
                return 0;
            }
            T*           thePtr = inBufferPtr;
            size_t const theRem = min((size_t)inBufferSize, mSize - mPos);
            size_t       theSize;
            if (0 < mBufRem) {
                theSize = min(mBufRem, theRem);
                inFunc(thePtr,
                    *mBufferPtr + mOuterPtr->mBlockSize - mBufRem, theSize);
                mBufRem -= theSize;
                if (0 < mBufRem) {
                    return (int)theSize;
                }
                mBufferPtr++;
                thePtr += theSize;
            }
            T* const theEndPtr = thePtr + theRem;
            theSize = mOuterPtr->mBlockSize;
            while (thePtr + theSize <= theEndPtr) {
                inFunc(thePtr, *mBufferPtr++, theSize);
                mPos   += theSize;
                thePtr += theSize;
            }
            theSize = (size_t)(theEndPtr - thePtr);
            if (0 < theSize) {
                inFunc(thePtr, *mBufferPtr, theSize);
                thePtr += theSize;
                mPos   += theSize;
                mBufRem = mOuterPtr->mBlockSize - theSize;
            }
            return (int)(thePtr - (theEndPtr - theRem));
        }
        class ReadFunc
        {
        public:
            void operator()(
                const char* inFromPtr,
                char*       inToPtr,
                size_t      inSize) const
                { memcpy(inToPtr, inFromPtr, inSize); }
        };
        S3Status Read(
            int         inBufferSize,
            const char* inBufferPtr)
        {
            if (mOuterPtr->mStopFlag) {
                return S3StatusAbortedByCallback;
            }
            IO(inBufferSize, inBufferPtr, ReadFunc());
            return S3StatusOK;
        }
        class WriteFunc
        {
        public:
            void operator()(
                char*       inToPtr,
                const char* inFromPtr,
                size_t      inSize) const
                { memcpy(inToPtr, inFromPtr, inSize); }
        };
        int Write(
            int   inBufferSize,
            char* inBufferPtr)
        {
            return IO(inBufferSize, inBufferPtr, WriteFunc());
        }
        void Run()
        {
            List::Remove(*this);
            if (this == &mOuterPtr->mCurlTimer) {
                mOuterPtr->CurlTimer();
                return;
            }
            mRetryCount++;
            mBufferPtr = GetBuffers();
            mPos       = 0;
            mBufRem    = 0;
            switch (mReqType) {
                case QCDiskQueue::kReqTypeRead:
                    mOuterPtr->Read(*this);
                    return;
                case QCDiskQueue::kReqTypeWrite:
                case QCDiskQueue::kReqTypeWriteSync:
                    mOuterPtr->Write(*this);
                    return;
                case QCDiskQueue::kReqTypeDelete:
                    mOuterPtr->Delete(*this);
                    return;
                default:
                    mOuterPtr->FatalError("invalid request type");
                    return;
            }
        }
        void Done(
            S3Status              inStatus,
            const S3ErrorDetails* inErrorPtr)
        {
            if (inErrorPtr) {
                KFS_LOG_STREAM_START(MsgLogger::kLogLevelERROR, theMsg) <<
                    mOuterPtr->mLogPrefix <<
                    " "           << mFileName <<
                    " error: "    << inStatus <<
                    " message: "  <<
                        (inErrorPtr->message ? inErrorPtr->message : "") <<
                    " resource: " <<
                        (inErrorPtr->resource ? inErrorPtr->resource : "") <<
                    " details: "  << (inErrorPtr->furtherDetails ?
                            inErrorPtr->furtherDetails : "")
                    ;
                    ostream& theStream = theMsg.GetStream();
                    for (int i = 0; i < inErrorPtr->extraDetailsCount; i++) {
                        theStream <<
                            " "   << inErrorPtr->extraDetails[i].name <<
                            ": "  << inErrorPtr->extraDetails[i].value;
                    }
                KFS_LOG_STREAM_END;
            }
            if (S3StatusOK != inStatus && S3_status_is_retryable(inStatus) &&
                    mOuterPtr->ScheduleRetry(*this)) {
                return;
            }
            int64_t            theIoByteCount;
            int                theSysErr;
            QCDiskQueue::Error theError;
            if (inStatus == S3StatusOK) {
                theIoByteCount =
                    QCDiskQueue::kReqTypeRead == mReqType ? mSize : mSize;
                theSysErr      = 0;
                theError       = QCDiskQueue::kErrorNone;
            } else {
                theIoByteCount = 0;
                switch (mReqType) {
                    case QCDiskQueue::kReqTypeRead:
                        theError  = QCDiskQueue::kErrorRead;
                        theSysErr = -EIO;
                        break;
                    case QCDiskQueue::kReqTypeWrite:
                    case QCDiskQueue::kReqTypeWriteSync:
                        theError  = QCDiskQueue::kErrorWrite;
                        theSysErr = -EIO;
                        break;
                    case QCDiskQueue::kReqTypeDelete:
                        theError  = QCDiskQueue::kErrorDelete;
                        theSysErr = -EIO; // FIXME
                        break;
                    default:
                        mOuterPtr->FatalError("invalid request type");
                        return;
                }
                theIoByteCount = 0;
            }
            mOuterPtr->mDiskQueuePtr->Done(
                *mOuterPtr,
                *mRequestPtr,
                theError,
                theSysErr,
                theIoByteCount,
                mStartBlockIdx
            );
        }
        void Schedule(
            int inTimeSec)
        {
            if (inTimeSec < 0) {
                List::Remove(*this);
            } else {
                mOuterPtr->mTimer.Schedule(*this, inTimeSec);
                if (inTimeSec == 0) {
                    mOuterPtr->Wakeup();
                }
            }
        }
        bool IsScheduled() const
            { return List::IsInList(*this); }
        time_t GetStartTime() const
            { return mStartTime; }
        int GetRetryCount() const
            { return mRetryCount; }
        const string& GetFileName() const
            { return mFileName; }
        const S3PutProperties& InitPutProperties(
            S3PutProperties& outProps)
        {
            outProps.contentType                =
                mParameters.mContentType.empty()  ? 0 :
                mParameters.mContentType.c_str();
            outProps.md5 = GetMd5Sum();
            outProps.cacheControl               =
                mParameters.mCacheControl.empty() ? 0 :
                mParameters.mCacheControl.c_str();
            outProps.contentDispositionFilename =
                mParameters.mContentDispositionFilename.empty() ? 0 :
                mParameters.mContentDispositionFilename.c_str();
            outProps.contentEncoding            =
                mParameters.mContentEncoding.empty() ? 0  :
                mParameters.mContentEncoding.c_str();
            outProps.expires                    = mParameters.mObjectExpires;
            outProps.cannedAcl                  = mParameters.mCannedAcl;
            outProps.metaDataCount              = 0;
            outProps.metaData                   = 0;
            outProps.useServerSideEncryption    =
                mParameters.mUseServerSideEncryptionFlag ? 1 : 0;
            return outProps;
        }
        const S3BucketContext& InitBucketContext(
            S3BucketContext& outCtx) const
        {
            outCtx.hostName        =
                mParameters.mS3HostName.empty() ? 0 :
                mParameters.mS3HostName.c_str();
            outCtx.bucketName      = mParameters.mBucketName.c_str();
            outCtx.protocol        = mParameters.mS3Protocol;
            outCtx.uriStyle        = mParameters.mS3UriStyle;
            outCtx.accessKeyId     = mParameters.mAccessKeyId.c_str();
            outCtx.secretAccessKey =
                mParameters.mSecretAccessKey.c_str();
            outCtx.securityToken   =
                mParameters.mSecurityToken.empty() ? 0 :
                mParameters.mSecurityToken.c_str();
            return outCtx;
        }
        ~S3Req()
            { List::Remove(*this); }
        S3Status Properties(
            const S3ResponseProperties* inPropertiesPtr)
        {
            KFS_LOG_STREAM_START(MsgLogger::kLogLevelDEBUG, theMsg) <<
                mOuterPtr->mLogPrefix <<
                " "       << mFileName <<
                " type: " << mReqType <<
                " Content-Type: " <<
                    (inPropertiesPtr->contentType ?
                        inPropertiesPtr->contentType : "null") <<
                " Request-Id: " <<
                     (inPropertiesPtr->requestId ?
                        inPropertiesPtr->requestId : "null") <<
                " Request-Id-2: " <<
                    (inPropertiesPtr->requestId2 ?
                        inPropertiesPtr->requestId2 : "null") <<
                " Content-Length: " <<
                    inPropertiesPtr->contentLength <<
                " Server: " <<
                    (inPropertiesPtr->server ?
                        inPropertiesPtr->server : "null") <<
                " ETag: " <<
                    (inPropertiesPtr->eTag ?
                        inPropertiesPtr->eTag : "null") <<
                " Last-Modified: " << inPropertiesPtr->lastModified <<
                " UsesServerSideEncryption: " <<
                    inPropertiesPtr->usesServerSideEncryption
                ;
                ostream& theStream = theMsg.GetStream();
                for (int i = 0; i < inPropertiesPtr->metaDataCount; i++) {
                    theStream << "x-amz-meta-" <<
                        inPropertiesPtr->metaData[i].name <<
                        ": " << inPropertiesPtr->metaData[i].value;
                }
            KFS_LOG_STREAM_END;
            return S3StatusOK;
        }
    private:
        S3IO*      const mOuterPtr;
        Request*   const mRequestPtr;
        ReqType    const mReqType;
        int        const mFd;
        uint64_t   const mGeneration;
        BlockIdx   const mStartBlockIdx;
        string     const mFileName;
        Parameters const mParameters;
        string           mMd5Sum;
        size_t           mPos;
        size_t           mSize;
        size_t           mBufRem;
        char**           mBufferPtr;
        int              mRetryCount;
        time_t           mStartTime;

        S3Req(
            S3IO&          inOuter,
            Request&       inRequest,
            ReqType        inReqType,
            int            inFd,
            BlockIdx       inStartBlockIdx,
            int            inBufferCount,
            size_t         inSize,
            const File&    inFile)
            : S3ReqBase(),
              mOuterPtr(&inOuter),
              mRequestPtr(&inRequest),
              mReqType(inReqType),
              mFd(inFd),
              mGeneration(inFile.mGeneration),
              mStartBlockIdx(inStartBlockIdx),
              mFileName(inFile.mFileName),
              mParameters(inOuter.mParameters),
              mMd5Sum(),
              mPos(0),
              mSize(inSize),
              mBufRem(0),
              mBufferPtr(0),
              mRetryCount(0),
              mStartTime(mOuterPtr->mNow)
            {}
        char** GetBuffers()
            { return reinterpret_cast<char**>(this + 1); }
        void operator delete(void* inPtr);
    };
    friend class S3Req;

    typedef TimerWheel<
        S3ReqBase,
        S3ReqBase::List,
        time_t,
        kMaxTimerTimeSec,
        kTimerResolutionSec
    > TimerW;
    class TimerFunc
    {
    public:
        void operator()(
            S3ReqBase& inReq) const
            { static_cast<S3Req&>(inReq).Run(); }
    };
    typedef vector<bool> CurlFdTable;

    QCDiskQueue*      mDiskQueuePtr;
    int               mBlockSize;
    string const      mConfigPrefix;
    string const      mFilePrefix;
    string            mLogPrefix;
    S3RequestContext* mS3CtxPtr;
    CURLM*            mCurlCtxPtr;
    QCFdPoll          mFdPoll;
    int               mPollSocketCount;
    FileTable         mFileTable;
    FreeFdList        mFreeFdList;
    uint64_t          mGeneration;
    MdStream          mMdStream;
    time_t            mNow;
    TimerW            mTimer;
    S3Req             mCurlTimer;
    CurlFdTable       mCurlFdTable;
    bool              mStopFlag;
    bool              mParametersUpdatedFlag;
    Parameters        mParameters;
    Parameters        mUpdatedParameters;
    QCMutex           mMutex;

    S3IO(
        const char* inUrlPtr,
        const char* inConfigPrefixPtr,
        const char* inLogPrefixPtr)
        : IOMethod(),
          mDiskQueuePtr(0),
          mBlockSize(0),
          mConfigPrefix(inConfigPrefixPtr ? inConfigPrefixPtr : ""),
          mFilePrefix(inUrlPtr ? inUrlPtr : ""),
          mLogPrefix(inLogPrefixPtr ? inLogPrefixPtr : ""),
          mS3CtxPtr(0),
          mCurlCtxPtr(0),
          mFdPoll(true), // Wakeable
          mPollSocketCount(0),
          mFileTable(),
          mFreeFdList(),
          mGeneration(1),
          mMdStream(0, true, string(), 0),
          mNow(time(0)),
          mTimer(mNow),
          mCurlTimer(this),
          mCurlFdTable(),
          mStopFlag(false),
          mParametersUpdatedFlag(false),
          mParameters(),
          mUpdatedParameters(),
          mMutex()
    {
        if (! inLogPrefixPtr) {
            mLogPrefix += "S3IO ";
            AppendHexIntToString(mLogPrefix, this - (S3IO*)0);
            mLogPrefix +=  ' ';
        } else if (! mLogPrefix.empty() && *mLogPrefix.rbegin() != ' ') {
            mLogPrefix += ' ';
        }
        mFileTable.reserve(1 << 10);
        mFileTable.push_back(0); // Reserve first slot, to fds start from 1.
        mFreeFdList.reserve(1 << 10);
        mCurlFdTable.reserve(1 << 10);
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
        S3BucketContext       theBucketContext = { 0 };
        const S3GetConditions theGetConditions =
        {
            -1, // ifModifiedSince,
            -1, // ifNotModifiedSince,
            0,  // ifMatch,
            0   // ifNotMatch
        };
        const S3GetObjectHandler theGetObjectHandler =
        {
            {
                &S3ResponsePropertiesCB,
                &S3ResponseCompleteCB
            },
            &S3GetObjectDataCB
        };
        S3_get_object(
            &inReq.InitBucketContext(theBucketContext),
            inReq.GetFileName().c_str(),
            &theGetConditions,
            inReq.GetStartBlockIdx() * mBlockSize,
            inReq.GetSize(),
            mS3CtxPtr,
            &theGetObjectHandler,
            &inReq
        );
    }
    void Write(
        S3Req& inReq)
    {
        S3BucketContext theBucketContext = { 0 };
        S3PutProperties thePutProperties = { 0 };
        const S3PutObjectHandler thePutObjectHandler =
        {
            {
                &S3ResponsePropertiesCB,
                &S3ResponseCompleteCB
            },
            &S3PutObjectDataCB
        };
        S3_put_object(
            &inReq.InitBucketContext(theBucketContext),
            inReq.GetFileName().c_str(),
            inReq.GetSize(),
            &inReq.InitPutProperties(thePutProperties),
            mS3CtxPtr,
            &thePutObjectHandler,
            &inReq
        );
    }
    void Delete(
        S3Req& inReq)
    {
        S3BucketContext theBucketContext = { 0 };
        const S3ResponseHandler theResponseHandler =
        {
            0, // &3ResponsePropertiesCB
            &S3ResponseCompleteCB
        };
        S3_delete_object(
            &inReq.InitBucketContext(theBucketContext),
            inReq.GetFileName().c_str(),
            mS3CtxPtr,
            &theResponseHandler,
            &inReq
        );
    }
    bool ScheduleRetry(
        S3Req& inReq)
    {
        if (! mStopFlag && inReq.GetRetryCount() < mParameters.mMaxRetryCount) {
            inReq.Schedule(max(time_t(1), mParameters.mRetryInterval -
                (mNow - inReq.GetStartTime())));
            return true;
        }
        return false;
    }
    void Timer()
    {
        TimerFunc theFunc;
        mTimer.Run(mNow, theFunc);
    }
    int CurlTimer()
    {
        int theStatus;
        int theRemCount = 0;
        if ((theStatus = curl_multi_socket_action(
                mCurlCtxPtr, CURL_SOCKET_TIMEOUT, 0, &theRemCount))) {
            FatalError("curl_multi_socket_action(CURL_SOCKET_TIMEOUT)",
                theStatus);
        }
        return theRemCount;
    }
    static S3Status S3ResponsePropertiesCB(
        const S3ResponseProperties* inPropertiesPtr,
        void*                       inUserDataPtr)
    {
        return reinterpret_cast<S3Req*>(inUserDataPtr
            )->Properties(inPropertiesPtr);
    }
    static void S3ResponseCompleteCB(
        S3Status              inStatus,
        const S3ErrorDetails* inErrorPtr,
        void*                 inUserDataPtr)
    {
        reinterpret_cast<S3Req*>(inUserDataPtr)->Done(
            inStatus, inErrorPtr);
    }
    static S3Status S3GetObjectDataCB(
        int         inBufferSize,
        const char* inBufferPtr,
        void*       inUserDataPtr)
    {
        return reinterpret_cast<S3Req*>(inUserDataPtr)->Read(
            inBufferSize, inBufferPtr);
    }
    static int S3PutObjectDataCB(
        int   inBufferSize,
        char* inBufferPtr,
        void* inUserDataPtr)
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
        void*         /* inSocketPtr */)
    {
        return reinterpret_cast<S3IO*>(inUserDataPtr)->SetSocket(
            inFd, inAction
        );
    }
    int SetTimeout(
        CURLM* inCurlCtxPtr,
        long   inTimeoutMs)
    {
        QCRTASSERT(mCurlCtxPtr == inCurlCtxPtr);
        const long kMilliSec = 1000;
        if (kMaxTimerTimeSec * kMilliSec < inTimeoutMs ||
                inTimeoutMs < kMilliSec) {
            KFS_LOG_STREAM_DEBUG <<
                " shortening curl timer from: " << inTimeoutMs <<
                " to: " << (inTimeoutMs < kMilliSec ?
                    0 : (int)kMaxTimerTimeSec * kMilliSec) << " ms." <<
            KFS_LOG_EOM;
        }
        mCurlTimer.Schedule(inTimeoutMs / kMilliSec);
        return 0;
    }
    bool IsPollingSocket(
        curl_socket_t inFd)
    {
        return (0 <= inFd && inFd < mCurlFdTable.size() && mCurlFdTable[inFd]);
    }
    int SetSocket(
        curl_socket_t inFd,
        int           inAction)
    {
        int theStatus = 0;
        int theEvents = 0;
        switch(inAction) {
            case CURL_POLL_REMOVE:
                if (IsPollingSocket(inFd)) {
                    theStatus = mFdPoll.Remove(inFd);
                    QCASSERT(0 < mPollSocketCount);
                    mPollSocketCount--;
                    mCurlFdTable[inFd] = false;
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
        if (CURL_POLL_REMOVE != inAction) {
            if (IsPollingSocket(inFd)) {
                theStatus = mStopFlag ? 0 :
                    mFdPoll.Set(inFd, theEvents, kS3IOFdOffset + inFd);
            } else {
                theStatus = mStopFlag ? 0 :
                    mFdPoll.Add(inFd, theEvents, kS3IOFdOffset + inFd);
                mCurlFdTable.resize(inFd);
                mCurlFdTable[inFd] = true;
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
