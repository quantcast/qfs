//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/10/04
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
// AWS S3 IO method implementation.
//
//----------------------------------------------------------------------------

#include "chunk/IOMethodDef.h"

#include "common/kfsdecls.h"
#include "common/MsgLogger.h"
#include "common/IntToString.h"
#include "common/MdStream.h"
#include "common/Properties.h"
#include "common/httputils.h"

#include "qcdio/QCUtils.h"
#include "qcdio/QCDLList.h"
#include "qcdio/QCMutex.h"
#include "qcdio/qcdebug.h"
#include "qcdio/qcstutils.h"

#include "kfsio/Base64.h"
#include "kfsio/TransactionalClient.h"
#include "kfsio/NetManager.h"
#include "kfsio/HttpResponseHeaders.h"
#include "kfsio/HttpChunkedDecoder.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/KfsCallbackObj.h"
#include "kfsio/event.h"

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
using KFS::httputils::GetHeaderLength;

template<typename T>
class S3ION_ObjDisplay
{
public:
    S3ION_ObjDisplay(
        const T& inObj)
        : mObj(inObj)
        {}
    template<typename ST>
    ST& Show(
        ST& inStream) const
        { return mObj.Display(inStream); }
private:
    const T& mObj;
};

template<typename ST, typename T>
    ST&
operator<<(
    ST&                        inStream,
    const S3ION_ObjDisplay<T>& inDisplay)
    { return inDisplay.Show(inStream); }

class S3ION : public IOMethod
{
public:
    typedef S3ION                      Outer;
    typedef QCDiskQueue::Request       Request;
    typedef QCDiskQueue::ReqType       ReqType;
    typedef QCDiskQueue::BlockIdx      BlockIdx;
    typedef QCDiskQueue::InputIterator InputIterator;

    enum
    {
        kTimerResolutionSec = 1,
        kMaxTimerTimeSec    = 512
    };

    template<typename T>
    static S3ION_ObjDisplay<T> Show(
        const T& inReq)
        { return S3ION_ObjDisplay<T>(inReq); }
    static IOMethod* New(
        const char*       inUrlPtr,
        const char*       inLogPrefixPtr,
        const char*       inParamsPrefixPtr,
        const Properties& inParameters)
    {
        if (! inUrlPtr ||
                inUrlPtr[0] != 's' ||
                inUrlPtr[1] != '3' ||
                inUrlPtr[2] != 'n' ||
                inUrlPtr[3] != ':' ||
                inUrlPtr[4] != '/' ||
                inUrlPtr[5] != '/') {
            return 0;
        }
        string theConfigPrefix = inUrlPtr + 6;
        while (! theConfigPrefix.empty() && *theConfigPrefix.rbegin() == '/') {
            theConfigPrefix.resize(theConfigPrefix.size() - 1);
        }
        S3ION* const thePtr = new S3ION(
            inUrlPtr, theConfigPrefix.c_str(), inLogPrefixPtr);
        thePtr->SetParameters(inParamsPrefixPtr, inParameters);
        return thePtr;
    }
    virtual ~S3ION()
    {
        KFS_LOG_STREAM_DEBUG << mLogPrefix << "~S3ION" << KFS_LOG_EOM;
        S3ION::Stop();
        delete [] mHdrBufferPtr;
    }
    virtual bool Init(
        QCDiskQueue& inDiskQueue,
        int          inBlockSize,
        int64_t      inMinWriteBlkSize,
        int64_t      inMaxFileSize,
        bool&        outCanEnforceIoTimeoutFlag)
    {
        if (inMaxFileSize <= 0 || (int64_t(5) << 30) < inMaxFileSize) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "invalid max file size: " << inMaxFileSize <<
            KFS_LOG_EOM;
            return false;
        }
        if (inMinWriteBlkSize < inMaxFileSize) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "write block size: " << inMinWriteBlkSize <<
                " less than max file size: " << inMaxFileSize <<
                " is not supported" <<
            KFS_LOG_EOM;
            return false;
        }
        mBlockSize = inBlockSize;
        if (mBlockSize <= 0) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "invalid block size: " << mBlockSize <<
            KFS_LOG_EOM;
            return false;
        }

        mDiskQueuePtr = &inDiskQueue;
        outCanEnforceIoTimeoutFlag = true;
        return true;
    }
    virtual void SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters)
    {
        QCStMutexLocker theLock(mMutex);
        mUpdatedFullConfigPrefix = inPrefixPtr ? inPrefixPtr : "";
        mUpdatedFullConfigPrefix += mConfigPrefix;
        inParameters.copyWithPrefix(mFullConfigPrefix, mUpdatedParameters);
        mParametersUpdatedFlag = true;
    }
    virtual void ProcessAndWait()
    {
        bool   theUpdateParametersFlag = false;
        string theConfigPrefix;
        QCStMutexLocker theLock(mMutex);
        if (mParametersUpdatedFlag) {
            if (mUpdatedFullConfigPrefix != mFullConfigPrefix ||
                    mParameters != mUpdatedParameters) {
                mParameters       = mUpdatedParameters;
                mFullConfigPrefix = mUpdatedFullConfigPrefix;
                theUpdateParametersFlag = true;
            }
            mParametersUpdatedFlag = false;
        }
        theLock.Unlock();
        if (theUpdateParametersFlag) {
            SetParameters();
        }
        QCMutex*                const kMutexPtr             = 0;
        bool                    const kWakeupAndCleanupFlag = true;
        NetManager::Dispatcher* const kDispatcherPtr        = 0;
        bool                    const kRunOnceFlag          = true;
        mNetManager.MainLoop(
            kMutexPtr, kWakeupAndCleanupFlag, kDispatcherPtr, kRunOnceFlag);
    }
    virtual void Wakeup()
    {
        mNetManager.Wakeup();
    }
    virtual void Stop()
    {
        mNetManager.Shutdown();
        ProcessAndWait(); // Run net manager's clieanup.
        mClient.Stop();
    }
    virtual int Open(
        const char* inFileNamePtr,
        bool        inReadOnlyFlag,
        bool        inCreateFlag,
        bool        inCreateExclusiveFlag,
        int64_t&    ioMaxFileSize)
    {
        const int theErr = ValidateFileName(inFileNamePtr);
        if (theErr) {
            return theErr;
        }
        const int theFd = NewFd();
        if (0 <= theFd) {
            QCASSERT(mFileTable[theFd].mFileName.empty());
            mFileTable[theFd].Set(
                inFileNamePtr + mFilePrefix.length(),
                inReadOnlyFlag,
                inCreateFlag,
                inCreateExclusiveFlag,
                ioMaxFileSize,
                mGeneration
            );
            if (0 == ++mGeneration) {
                mGeneration++;
            }
        }
        KFS_LOG_STREAM(0 <= theFd ?
                MsgLogger::kLogLevelDEBUG :
                MsgLogger::kLogLevelERROR) << mLogPrefix <<
            "open:"
            " fd: "     << theFd <<
            " gen: "    << (0 <= theFd ? mGeneration - 1 : mGeneration) <<
            " name: "   << (inFileNamePtr + mFilePrefix.length()) <<
            " ro: "     << inReadOnlyFlag <<
            " create: " << inCreateFlag <<
            " excl: "   << inCreateExclusiveFlag <<
            " max sz: " << ioMaxFileSize <<
        KFS_LOG_EOM;
        return theFd;
    }
    virtual int Close(
        int     inFd,
        int64_t inEof)
    {
        File* const theFilePtr = GetFilePtr(inFd);
        if (! theFilePtr) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "close:"
                " fd: "  << inFd <<
                " eof: "  << inEof <<
                " bad file descriptor" <<
            KFS_LOG_EOM;
            return EBADF;
        }
        int theRet = 0;
        File& theFile = *theFilePtr;
        if (! theFile.mReadOnlyFlag &&  theFile.mMaxFileSize < inEof) {
            theRet = EINVAL;
        }
        KFS_LOG_STREAM(0 == theRet ?
                MsgLogger::kLogLevelDEBUG :
                MsgLogger::kLogLevelERROR) << mLogPrefix <<
            "close:"
            " fd: "     << inFd <<
            " gen: "    << theFile.mGeneration <<
            " "         << theFile.mFileName <<
            " eof: "    << inEof <<
            " ro: "     << theFile.mReadOnlyFlag <<
            " create: " << theFile.mCreateFlag <<
            " excl: "   << theFile.mCreateExclusiveFlag <<
            " max sz: " << theFile.mMaxFileSize <<
            " status: " << theRet <<
        KFS_LOG_EOM;
        if (mFileTable.size() == (size_t)inFd + 1) {
            mFileTable.pop_back();
        } else {
            theFile.Reset();
            theFile.mMaxFileSize = mFreeFdListIdx;
            mFreeFdListIdx       = inFd;
        }
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
        int64_t            theEnd;
        switch (inReqType) {
            case QCDiskQueue::kReqTypeRead:
                if (! theFilePtr) {
                    theError  = QCDiskQueue::kErrorRead;
                    theSysErr = EBADF;
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "invalid read attempt:"
                        " fd: " << inFd <<
                        " is not valid" <<
                    KFS_LOG_EOM;
                    break;
                }
                if (inStartBlockIdx < 0) {
                    theError  = QCDiskQueue::kErrorRead;
                    theSysErr = EINVAL;
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "invalid read start position: " << inStartBlockIdx <<
                        " fd: "   << inFd <<
                        " gen: "  << theFilePtr->mGeneration <<
                        " file: " << theFilePtr->mFileName <<
                    KFS_LOG_EOM;
                    break;
                }
                if (theFilePtr->mWriteOnlyFlag) {
                    theError  = QCDiskQueue::kErrorRead;
                    theSysErr = EINVAL;
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "invalid read on write only file: " <<
                        " fd: "  << inFd <<
                        " gen: " << theFilePtr->mGeneration <<
                        " "      << theFilePtr->mFileName <<
                    KFS_LOG_EOM;
                    break;
                }
                if (inBufferCount <= 0 && ! theFilePtr->mReadOnlyFlag) {
                    // 0 size read is used to get open status. The object
                    // that corresponds to "file" might not exists yet.
                    break;
                }
                if (Start(
                        inRequest,
                        inReqType,
                        inStartBlockIdx,
                        inBufferCount,
                        inInputIteratorPtr,
                        inBufferCount * mBlockSize,
                        *theFilePtr,
                        inFd)) {
                    return;
                }
            case QCDiskQueue::kReqTypeWrite:
            case QCDiskQueue::kReqTypeWriteSync:
                if (! theFilePtr || theFilePtr->mReadOnlyFlag ||
                        inBufferCount <= 0) {
                    theError  = QCDiskQueue::kErrorWrite;
                    theSysErr = theFilePtr ? EINVAL : EBADF;
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        RequestTypeToName(inReqType) <<
                        "/" << inReqType <<
                        " invalid write attempt:" <<
                        " fd: "   << inFd <<
                        " file: " <<
                            reinterpret_cast<const void*>(theFilePtr) <<
                        " name: " << (theFilePtr ?
                            theFilePtr->mFileName : string()) <<
                        " buffers: " << inBufferCount <<
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
                        " fd: "          << inFd <<
                        " gen: "         << theFilePtr->mGeneration <<
                        " "              << theFilePtr->mFileName <<
                    KFS_LOG_EOM;
                    break;
                }
                if (QCDiskQueue::kReqTypeWriteSync == inReqType && inEof < 0) {
                    theError  = QCDiskQueue::kErrorParameter;
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "invalid sync write EOF: " << inEof <<
                        " max: " << theFilePtr->mMaxFileSize <<
                        " fd: "  << inFd <<
                        " gen: " << theFilePtr->mGeneration <<
                        " "      << theFilePtr->mFileName <<
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
                        " fd: "  << inFd <<
                        " gen: " << theFilePtr->mGeneration <<
                        " "      << theFilePtr->mFileName <<
                    KFS_LOG_EOM;
                    break;
                }
                theFilePtr->mWriteOnlyFlag = true;
                if (Start(
                        inRequest,
                        inReqType,
                        inStartBlockIdx,
                        inBufferCount,
                        inInputIteratorPtr,
                        inBufferCount * mBlockSize -
                            (theFilePtr->mMaxFileSize < theEnd ?
                                theEnd - theFilePtr->mMaxFileSize : 0),
                        *theFilePtr,
                        inFd)) {
                    return;
                }
            default:
                theError  = QCDiskQueue::kErrorParameter;
                theSysErr = theFilePtr ? ENXIO : EBADF;
                KFS_LOG_STREAM_ERROR << mLogPrefix <<
                    "start meta:"     <<
                    " fd: "           << inFd <<
                    " gen: "          << (theFilePtr ?
                        theFilePtr->mGeneration : Generation(0)) <<
                    " name: "         << (theFilePtr ?
                        theFilePtr->mFileName : string()) <<
                    " request type: " << inReqType <<
                    " is not supported" <<
                KFS_LOG_EOM;
                break;
        }
        int64_t const theIoByteCount = 0;
        mDiskQueuePtr->Done(
            *this,
            inRequest,
            theError,
            theSysErr,
            theIoByteCount,
            inStartBlockIdx
        );
    }
    virtual void StartMeta(
        Request&    inRequest,
        ReqType     inReqType,
        const char* inNamePtr,
        const char* /* inName2Ptr */)
    {
        QCDiskQueue::Error theError  = QCDiskQueue::kErrorNone;
        int                theSysErr = 0;
        switch (inReqType) {
            case QCDiskQueue::kReqTypeDelete:
                if ((theSysErr = ValidateFileName(inNamePtr)) != 0) {
                    theError  = QCDiskQueue::kErrorDelete;
                    break;
                }
                if (Start(
                        inRequest,
                        inReqType,
                        0, // inStartBlockIdx,
                        0, // inBufferCount,
                        0, // inInputIteratorPtr,
                        0, // size,
                        File(inNamePtr + mFilePrefix.length()),
                        -1)) {
                    break;
                }
            default:
                theError  = QCDiskQueue::kErrorParameter;
                theSysErr = ENXIO;
                KFS_LOG_STREAM_ERROR << mLogPrefix <<
                    "start meta:"     <<
                    " request type: " << inReqType <<
                    " is not supported" <<
                KFS_LOG_EOM;
                break;
        }
        if (QCDiskQueue::kErrorNone != theError || 0 != theSysErr) {
            int64_t const theIoByteCount = 0;
            mDiskQueuePtr->Done(
                *this,
                inRequest,
                theError,
                theSysErr,
                theIoByteCount,
                0  // inStartBlockIdx
            );
        }
    }
private:
    typedef uint64_t Generation;

    class File
    {
    public:
        File(
            const char* inFileNamePtr = 0)
            : mFileName(inFileNamePtr ? inFileNamePtr : ""),
              mReadOnlyFlag(false),
              mCreateFlag(false),
              mCreateExclusiveFlag(false),
              mWriteOnlyFlag(false),
              mMaxFileSize(-1),
              mGeneration(0)
            {}
        void Set(
            const char* inFileNamePtr,
            bool        inReadOnlyFlag,
            bool        inCreateFlag,
            bool        inCreateExclusiveFlag,
            int64_t     inMaxFileSize,
            Generation  inGeneration)
        {
            mFileName            = inFileNamePtr;
            mReadOnlyFlag        = inReadOnlyFlag;
            mCreateFlag          = inCreateFlag;
            mCreateExclusiveFlag = inCreateExclusiveFlag;
            mWriteOnlyFlag       = false;
            mMaxFileSize         = inMaxFileSize;
            mGeneration          = inGeneration;
        }
        void Reset()
        {
            mFileName            = string(); // De-allocate.
            mReadOnlyFlag        = false;
            mCreateFlag          = false;
            mCreateExclusiveFlag = false;
            mWriteOnlyFlag       = false;
            mMaxFileSize         = -1;
            mGeneration          = 0;
        }
        string     mFileName;
        bool       mReadOnlyFlag:1;
        bool       mCreateFlag:1;
        bool       mCreateExclusiveFlag:1;
        bool       mWriteOnlyFlag:1;
        int64_t    mMaxFileSize;
        Generation mGeneration;
    };
    typedef vector<File> FileTable;
    class S3Req : public KfsCallbackObj, public TransactionalClient::Transaction
    {
    public:
        typedef QCDiskQueue::Request Request;

        S3Req(
            Outer&        inOuter,
            Request&      inRequest,
            ReqType       inReqType,
            const string& inFileName)
            : KfsCallbackObj(),
              TransactionalClient::Transaction(),
              mOuter(inOuter),
              mRequest(inRequest),
              mReqType(inReqType),
              mFileName(inFileName),
              mLogPrefix(),
              mRetryCount(0),
              mStartTime(mOuter.Now()),
              mTimer(mOuter.mNetManager, *this),
              mSentFlag(false),
              mReceivedHeadersFlag(false),
              mReadTillEofFlag(false),
              mHeaderLength(-1),
              mIOBuffer(),
              mHeaders(),
              mHttpChunkedDecoder(mIOBuffer, mOuter.mMaxReadAhead)
        {
            mOuter.mRequestCount++;
            SET_HANDLER(this, &S3Req::Timeout);
        }
        int GetRetryCount() const
            { return mRetryCount; }
        int Timeout(
            int   inEvent,
            void* inDataPtr)
        {
            QCRTASSERT(EVENT_INACTIVITY_TIMEOUT == inEvent && ! inDataPtr);
            mOuter.mClient.Run(*this);
            return 0;
        }
        virtual ostream& Display(
            ostream& inStream) const = 0;
    protected:
        typedef NetManager::Timer Timer;

        Outer&              mOuter;
        Request&            mRequest;
        ReqType       const mReqType;
        string        const mFileName;
        string        const mLogPrefix;
        int                 mRetryCount;
        time_t              mStartTime;
        Timer               mTimer;
        bool                mSentFlag;
        bool                mReceivedHeadersFlag;
        bool                mReadTillEofFlag;
        int                 mHeaderLength;
        IOBuffer            mIOBuffer;
        HttpResponseHeaders mHeaders;
        HttpChunkedDecoder  mHttpChunkedDecoder;

        virtual ~S3Req()
        {
            mOuter.mRequestCount--;
        }
        int SendRequest(
            const char*           inVerbPtr,
            IOBuffer&             inBuffer,
            const ServerLocation& inServer)
        {
            if (mSentFlag) {
                return 0;
            }
            mHeaders.Reset();
            ostream& theStream = mOuter.mWOStream.Set(inBuffer);
            theStream <<
                inVerbPtr << " /" << mFileName << " HTTP/1.1\r\n"
                "Host: "  << inServer.hostname << "\r\n"
                "\r\n"
                "\r\n"
            ;
            mOuter.mWOStream.Reset();
            mSentFlag = true;
            return mOuter.mMaxReadAhead;
        }
        int ParseResponse(
            IOBuffer& inBuffer,
            bool      inEofFlag)
        {
            if (mHeaderLength <= 0 &&
                    ((mHeaderLength = GetHeaderLength(inBuffer)) <= 0 ||
                    mOuter.mMaxHdrLen < mHeaderLength)) {
                if (mOuter.mMaxHdrLen < inBuffer.BytesConsumable()) {
                    KFS_LOG_STREAM_ERROR << mLogPrefix << Show(*this) <<
                        "exceeded max header length: " <<
                        inBuffer.BytesConsumable() << " bytes" <<
                    KFS_LOG_EOM;
                    Error(-EINVAL, "exceeded max header length");
                    return -1;
                }
                return mOuter.mMaxReadAhead;
            }
            if (! mReceivedHeadersFlag) {
                const char* const thePtr = inBuffer.CopyOutOrGetBufPtr(
                        mOuter.mHdrBufferPtr, mHeaderLength);
                if (! mHeaders.Parse(thePtr, mHeaderLength) ||
                        mHeaders.IsUnsupportedEncoding() ||
                        (mHeaders.IsHttp11OrGreater() &&
                        (mHeaders.GetContentLength() < 0 &&
                        ! mHeaders.IsChunkedEconding())) ||
                        (mOuter.mMaxResponseSize <
                            mHeaders.GetContentLength())) {
                    if (thePtr != mOuter.mHdrBufferPtr) {
                        memcpy(mOuter.mHdrBufferPtr, thePtr, mHeaderLength);
                    }
                    mOuter.mHdrBufferPtr[mHeaderLength] = 0;
                    KFS_LOG_STREAM_ERROR << mLogPrefix << Show(*this) <<
                        " invalid response:" <<
                        " header length: " << mHeaderLength <<
                        " header: " << mOuter.mHdrBufferPtr <<
                        " max response length: " << mOuter.mMaxResponseSize <<
                    KFS_LOG_EOM;
                    Error(-EINVAL, "invalid response");
                    return -1;
                }
                mReadTillEofFlag = ! mHeaders.IsChunkedEconding() &&
                    mHeaders.GetContentLength() < 0;
                inBuffer.Consume(mHeaderLength);
            }
            if (mHeaders.IsChunkedEconding()) {
                const int theRet = mHttpChunkedDecoder.Parse(inBuffer);
                if (theRet < 0) {
                    const char* const theMsgPtr =
                        "chunked encoded parse failure";
                    KFS_LOG_STREAM_ERROR << mLogPrefix << Show(*this) <<
                        " " << theMsgPtr << ":" <<
                        " discarding: " << inBuffer.BytesConsumable() <<
                        " bytes header length: " << mHeaderLength <<
                    KFS_LOG_EOM;
                    Error(-EINVAL, "chunked encoded parse failure");
                    return -1;
                } else if (0 < theRet) {
                    if (mOuter.mMaxResponseSize + mOuter.mMaxReadAhead <
                            mIOBuffer.BytesConsumable() + theRet) {
                        const char* const theMsgPtr =
                            "exceeded max response size";
                        KFS_LOG_STREAM_ERROR << mLogPrefix << Show(*this) <<
                            " " << theMsgPtr << ":" <<
                                mOuter.mMaxResponseSize +
                                mOuter.mMaxReadAhead <<
                            " discarding: " << inBuffer.BytesConsumable() <<
                            " bytes header length: " << mHeaderLength <<
                        KFS_LOG_EOM;
                        Error(-EINVAL, theMsgPtr);
                        return -1;
                    }
                    KFS_LOG_STREAM_DEBUG << mLogPrefix << Show(*this) <<
                        " chunked:"
                        " read ahead: " << theRet <<
                        " buffer rem: " << inBuffer.BytesConsumable() <<
                        " decoded: "    << mIOBuffer.BytesConsumable() <<
                    KFS_LOG_EOM;
                    return theRet;
                }
                if (! inBuffer.IsEmpty()) {
                    const char* const theMsgPtr =
                        "failed to parse completely chunk encoded content";
                    KFS_LOG_STREAM_ERROR << mLogPrefix << Show(*this) <<
                        " " << theMsgPtr << ":" <<
                        " discarding: " << inBuffer.BytesConsumable() <<
                        " bytes header length: " << mHeaderLength <<
                    KFS_LOG_EOM;
                    Error(-EINVAL, theMsgPtr);
                    return -1;
                }
            } else if (mReadTillEofFlag) {
                if (! inEofFlag) {
                    if (mOuter.mMaxResponseSize < inBuffer.BytesConsumable()) {
                        const char* const theMsgPtr =
                            "exceeded max response size";
                        KFS_LOG_STREAM_ERROR << mLogPrefix << Show(*this) <<
                            " " << theMsgPtr << ":" <<
                                mOuter.mMaxResponseSize <<
                            " discarding: " << inBuffer.BytesConsumable() <<
                            " bytes header length: " << mHeaderLength <<
                        KFS_LOG_EOM;
                        Error(-EINVAL, theMsgPtr);
                        return -1;
                    }
                    return (mOuter.mMaxResponseSize + 1 -
                        inBuffer.BytesConsumable());
                }
            } else {
                if (inBuffer.BytesConsumable() < mHeaders.GetContentLength()) {
                    return (mHeaders.GetContentLength() -
                        inBuffer.BytesConsumable());
                }
                mIOBuffer.Move(&inBuffer, mHeaders.GetContentLength());
            }
            KFS_LOG_STREAM_DEBUG << mLogPrefix << Show(*this) <<
                "response:"
                " headers: "   << mHeaderLength <<
                " body: "      << mHeaders.GetContentLength() <<
                " buffer: "    << mIOBuffer.BytesConsumable() <<
                " status: "    << mHeaders.GetStatus() <<
                " http/1.1 "   << mHeaders.IsHttp11OrGreater() <<
                " close: "     << mHeaders.IsConnectionClose() <<
                " chunked: "   << mHeaders.IsChunkedEconding() <<
            KFS_LOG_EOM;
            return ((mReadTillEofFlag || mHeaders.IsConnectionClose()) ?
                -1 : 0);
        }
        void Reset()
        {
            mSentFlag            = false;
            mReceivedHeadersFlag = false;
            mReadTillEofFlag     = false;
            mHeaderLength        = -1;
            mIOBuffer.Clear();
            mHeaders.Reset();
            mHttpChunkedDecoder.Reset();
        }
    private:
        S3Req(
            const S3Req& inReq);
        S3Req& operator=(
            const S3Req& inReq);
    };
    class S3Delete : public S3Req
    {
    public:
        S3Delete(
            Outer&        inOuter,
            Request&      inRequest,
            ReqType       inReqType,
            const string& inFileName)
            : S3Req(inOuter, inRequest, inReqType, inFileName)
            {}
        virtual ostream& Display(
            ostream& inStream) const
            { return (inStream << "delete: " << mFileName); }
        virtual int Request(
            IOBuffer&             inBuffer,
            IOBuffer&             /* inResponseBuffer */,
            const ServerLocation& inServer)
        {
            return SendRequest("DELETE", inBuffer, inServer);
        }
        virtual int Response(
            IOBuffer& inBuffer,
            bool      inEofFlag)
        {
            int theRet = ParseResponse(inBuffer, inEofFlag);
            if (0 == theRet) {
                
                // return ResponseDone
            }
            return theRet;
        }
        virtual void Error(
            int         inStatus,
            const char* inMsgPtr)
        {
            KFS_LOG_STREAM_ERROR << mLogPrefix << Show(*this) <<
                "network error: " << inStatus  <<
                " message: "      << (inMsgPtr ? inMsgPtr : "") <<
            KFS_LOG_EOM;
        }
    private:
        
    };
    friend class S3Delete;
    class S3Put : public S3Req
    {
    public:
        S3Put(
            Outer&        inOuter,
            Request&      inRequest,
            ReqType       inReqType,
            const string& inFileName)
            : S3Req(inOuter, inRequest, inReqType, inFileName)
        {
            SET_HANDLER(this, &S3Put::Timeout);
        }
        int Timeout(
            int   inEvent,
            void* inDataPtr)
        {
            QCRTASSERT(EVENT_INACTIVITY_TIMEOUT == inEvent && ! inDataPtr);
            mOuter.mClient.Run(*this);
            return 0;
        }
        virtual ostream& Display(
            ostream& inStream) const
            { return (inStream << "put: " << mFileName); }
        virtual int Request(
            IOBuffer&             inBuffer,
            IOBuffer&             /* inResponseBuffer */,
            const ServerLocation& inServer)
        {
            return SendRequest("PUT", inBuffer, inServer);
        }
        virtual int Response(
            IOBuffer& inBuffer,
            bool      inEofFlag)
        {
            const int theRet = ParseResponse(inBuffer, inEofFlag);
            return theRet;
        }
        virtual void Error(
            int         inStatus,
            const char* inMsgPtr)
        {
            KFS_LOG_STREAM_ERROR << mLogPrefix << Show(*this) <<
                "network error: " << inStatus  <<
                " message: "      << (inMsgPtr ? inMsgPtr : "") <<
            KFS_LOG_EOM;
        }
    private:
        //char mMd5Sum[(128 / 8 + 2) / 3 * 4 + 1];
                         // Base64::EncodedLength(128 / 8) + 1
    };
    friend class S3Put;

    QCDiskQueue*        mDiskQueuePtr;
    int                 mBlockSize;
    string const        mConfigPrefix;
    string const        mFilePrefix;
    string              mLogPrefix;
    NetManager          mNetManager;
    TransactionalClient mClient;
    FileTable           mFileTable;
    int64_t             mFreeFdListIdx;
    Generation          mGeneration;
    MdStream            mMdStream;
    int                 mRequestCount;
    bool                mParametersUpdatedFlag;
    Properties          mParameters;
    Properties          mUpdatedParameters;
    string              mFullConfigPrefix;
    string              mUpdatedFullConfigPrefix;
    string              mS3HostName;
    string              mBucketName;
    string              mAccessKeyId;
    string              mSecretAccessKey;
    string              mSecurityToken;
    string              mContentType;
    string              mCacheControl;
    string              mContentDispositionFilename;
    string              mContentEncoding;
    int64_t             mObjectExpires;
    //S3CannedAcl       mCannedAcl;
    bool                mUseServerSideEncryptionFlag;
    //S3Protocol        mS3Protocol;
    //S3UriStyle        mS3UriStyle;
    int                 mMaxRetryCount;
    int                 mRetryInterval;
    int                 mMaxReadAhead;
    int                 mMaxHdrLen;
    char*               mHdrBufferPtr;
    int                 mMaxResponseSize;
    long                mLowSpeedLimit;
    long                mLowSpeedTime;
    IOBuffer::WOStream  mWOStream;
    QCMutex             mMutex;
    MdStream::MD        mTmpMdBuf;

    static bool IsDebugLogLevel()
    {
        return (
            MsgLogger::GetLogger() &&
            MsgLogger::GetLogger()->IsLogLevelEnabled(
                    MsgLogger::kLogLevelDEBUG)
        );
    }
    static const char* RequestTypeToName(
        ReqType inReqType)
    {
        switch (inReqType) {
            case QCDiskQueue::kReqTypeRead:      return "read";
            case QCDiskQueue::kReqTypeWrite:     return "write";
            case QCDiskQueue::kReqTypeWriteSync: return "wrsync";
            case QCDiskQueue::kReqTypeDelete:    return "delete";
            default: break;
        }
        return "invalid";
    }
    S3ION(
        const char* inUrlPtr,
        const char* inConfigPrefixPtr,
        const char* inLogPrefixPtr)
        : IOMethod(true), // Do not allocate read buffers.
          mDiskQueuePtr(0),
          mBlockSize(0),
          mConfigPrefix(inConfigPrefixPtr ? inConfigPrefixPtr : ""),
          mFilePrefix(inUrlPtr ? inUrlPtr : ""),
          mLogPrefix(inLogPrefixPtr ? inLogPrefixPtr : ""),
          mNetManager(),
          mClient(mNetManager),
          mFileTable(),
          mFreeFdListIdx(-1),
          mGeneration(1),
          mMdStream(0, true, string(), 0),
          mRequestCount(0),
          mParametersUpdatedFlag(false),
          mParameters(),
          mUpdatedParameters(),
          mFullConfigPrefix(),
          mUpdatedFullConfigPrefix(),
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
          mUseServerSideEncryptionFlag(false),
          mMaxRetryCount(10),
          mRetryInterval(10),
          mMaxReadAhead(4 << 10),
          mMaxHdrLen(16 << 10),
          mHdrBufferPtr(new char[mMaxHdrLen + 1]),
          mMaxResponseSize(64 << 20),
          mLowSpeedLimit(4 << 10),
          mLowSpeedTime(10),
          mWOStream(),
          mMutex()
    {
        if (! inLogPrefixPtr) {
            mLogPrefix += "S3ION ";
            AppendHexIntToString(mLogPrefix, reinterpret_cast<uint64_t>(this));
            mLogPrefix +=  ' ';
        } else if (! mLogPrefix.empty() && *mLogPrefix.rbegin() != ' ') {
            mLogPrefix += ' ';
        }
        KFS_LOG_STREAM_DEBUG << mLogPrefix << "S3ION" << KFS_LOG_EOM;
        const size_t kFdReserve = 256;
        mFileTable.reserve(kFdReserve);
        mFileTable.push_back(File()); // Reserve first slot, to fds start from 1.
    }
    void SetParameters()
    {
        Properties::String theName(mFullConfigPrefix);
        const size_t       thePrefixSize = theName.GetSize();
        mS3HostName = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("hostName"),
            mS3HostName
        );
        mBucketName = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("bucketName"),
            mBucketName
        );
        mAccessKeyId = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("accessKeyId"),
            mAccessKeyId
        );
        mSecretAccessKey = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("secretAccessKey"),
            mSecretAccessKey
        );
        mSecurityToken = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("securityToken"),
            mSecurityToken
        );
        mContentType = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("contentType"),
            mContentType
        );
        mCacheControl = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("cacheControl"),
            mCacheControl
        );
        mContentDispositionFilename = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append(
                "contentDispositionFilename"),
            mContentDispositionFilename
        );
        mContentEncoding = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("contentEncoding"),
            mContentEncoding
        );
        mObjectExpires = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("objectExpires"),
            mObjectExpires
        );
        mUseServerSideEncryptionFlag = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("useServerSideEncryption"),
            mUseServerSideEncryptionFlag ? 1 : 0
        ) != 0;
        mMaxRetryCount = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("maxRetryCount"),
            mMaxRetryCount
        );
        mRetryInterval = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("retryInterval"),
            mRetryInterval
        );
        mLowSpeedLimit = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("lowSpeedLimit"),
            mLowSpeedLimit
        );
        mLowSpeedTime = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("lowSpeedTime"),
            mLowSpeedTime
        );
#if 0
        const Properties::String* theValPtr;
        if ((theValPtr = mParameters.getValue(
                theName.Truncate(thePrefixSize).Append("cannedAcl")))) {
            if (*theValPtr == "private") {
                mCannedAcl = S3CannedAclPrivate;
            } else if (*theValPtr == "publicRead") {
                mCannedAcl = S3CannedAclPublicRead;
            } else if (*theValPtr == "publicReadWrite") {
                mCannedAcl = S3CannedAclPublicReadWrite;
            } else if (*theValPtr == "authenticatedRead") {
                mCannedAcl = S3CannedAclAuthenticatedRead;
            } else {
                KFS_LOG_STREAM_WARN << inLogPrefix <<
                    " invalid parameter " << theName << " = " <<
                    *theValPtr <<
                KFS_LOG_EOM;
            }
        }
        if ((theValPtr = mParameters.getValue(
                theName.Truncate(thePrefixSize).Append("protocol")))) {
            if (*theValPtr == "https") {
                mS3Protocol = S3ProtocolHTTPS;
            } else if (*theValPtr == "http") {
                mS3Protocol = S3ProtocolHTTP;
            } else {
                KFS_LOG_STREAM_WARN << inLogPrefix <<
                    " invalid parameter " << theName << " = " <<
                    *theValPtr <<
                KFS_LOG_EOM;
            }
        }
        if ((theValPtr = mParameters.getValue(
                theName.Truncate(thePrefixSize).Append("uriStyle")))) {
            if (*theValPtr == "virtualHost") {
                mS3UriStyle = S3UriStyleVirtualHost;
            } else if (*theValPtr == "path") {
                mS3UriStyle = S3UriStylePath;
            } else {
                KFS_LOG_STREAM_WARN << inLogPrefix <<
                    " invalid parameter " << theName << " = " <<
                    *theValPtr <<
                KFS_LOG_EOM;
            }
        }
        mVerifyCertStatusFlag = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("verifyCertStatus"),
            mVerifyCertStatusFlag ? 1 : 0
        ) != 0;
        mVerifyPeerFlag = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("verifyPeer"),
            mVerifyPeerFlag ? 1 : 0
        ) != 0;
        mSslCiphers = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("sslCiphers"),
            mSslCiphers
        );
        mCABundle = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("CABundle"),
            mCABundle
        );
        mCAPath = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("CAPath"),
            mCAPath
        );
        if ((theValPtr = mParameters.getValue(
                theName.Truncate(thePrefixSize).Append("sslVersion")))) {
            if (*theValPtr == "tls1") {
                mSslVersion = CURL_SSLVERSION_TLSv1;
            } else if (*theValPtr == "ssl2") {
                mSslVersion = CURL_SSLVERSION_SSLv2;
            } else if (*theValPtr == "ssl3") {
                mSslVersion = CURL_SSLVERSION_SSLv3;
            } else if (*theValPtr == "tls10") {
                mSslVersion = WarnIfNotSupported(CURL_SSLVERSION_TLSv1_0,
                    inLogPrefix);
            } else if (*theValPtr == "tls11") {
                mSslVersion = WarnIfNotSupported(CURL_SSLVERSION_TLSv1_1,
                    inLogPrefix);
            } else if (*theValPtr == "tls12") {
                mSslVersion = WarnIfNotSupported(CURL_SSLVERSION_TLSv1_2,
                    inLogPrefix);
            } else if (*theValPtr == "") {
                mSslVersion = CURL_SSLVERSION_DEFAULT;
            } else {
                KFS_LOG_STREAM_WARN << inLogPrefix <<
                    " invalid parameter " << theName << " = " <<
                    *theValPtr <<
                KFS_LOG_EOM;
            }
        }
#endif
        string theErrMsg;
        const int theStatus = mClient.SetParameters(
            mFullConfigPrefix.c_str(), mParameters, &theErrMsg);
        if (0 != theStatus) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "set parameters failure: " <<
                " status: " << theStatus <<
                " "         << theErrMsg <<
            KFS_LOG_EOM;
        }
    }
    time_t Now() const
        { return mNetManager.Now(); }
    int NewFd()
    {
        if (mFreeFdListIdx < 0) {
            mFileTable.push_back(File());
            return (int)(mFileTable.size() - 1);
        }
        const int theFd = (int)mFreeFdListIdx;
        QCASSERT((size_t)theFd < mFileTable.size());
        mFreeFdListIdx = mFileTable[mFreeFdListIdx].mMaxFileSize;
        mFileTable[theFd].mMaxFileSize = -1;
        return theFd;
    }
    File* GetFilePtr(
        int inFd)
    {
        return ((inFd < 0 || mFileTable.size() <= (size_t)inFd ||
                mFileTable[inFd].mFileName.empty()) ?
            0 : &mFileTable[inFd]);
    }
    int ValidateFileName(
        const char* inFileNamePtr) const
    {
        if (! inFileNamePtr) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "invalid nul file name" <<
            KFS_LOG_EOM;
            return EINVAL;
        }
        const size_t theLen     = strlen(inFileNamePtr);
        const size_t thePrefLen = mFilePrefix.length();
        if (theLen <= mFilePrefix.length() ||
                mFilePrefix.compare(
                    0, thePrefLen, inFileNamePtr, thePrefLen) != 0) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "invalid file name: " << inFileNamePtr <<
                " file prefix: "      << mFilePrefix <<
            KFS_LOG_EOM;
            return EINVAL;
        }
        return 0;
    }
    bool Start(
        Request&        inRequest,
        ReqType         inReqType,
        BlockIdx        inStartBlockIdx,
        int             inBufferCount,
        InputIterator*  inInputIteratorPtr,
        size_t          inSize,
        const File&     inFile,
        int             inFd)
    {
        return false;
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
    S3ION(
        const S3ION& inS3io);
    S3ION& operator=(
        const S3ION& inS3io);
};

KFS_REGISTER_IO_METHOD(KFS_IO_METHOD_NAME_S3ION, S3ION::New);

} // namespace KFS
