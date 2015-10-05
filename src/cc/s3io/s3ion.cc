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

#include "common/MsgLogger.h"
#include "common/IntToString.h"
#include "common/MdStream.h"
#include "common/Properties.h"

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
        S3Req*             theReqPtr  = 0;
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
                theReqPtr = S3Req::Get(
                    *this,
                    inRequest,
                    inReqType,
                    inStartBlockIdx,
                    inBufferCount,
                    inInputIteratorPtr,
                    inBufferCount * mBlockSize,
                    *theFilePtr,
                    inFd
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
                theReqPtr = S3Req::Get(
                    *this,
                    inRequest,
                    inReqType,
                    inStartBlockIdx,
                    inBufferCount,
                    inInputIteratorPtr,
                    inBufferCount * mBlockSize -
                        (theFilePtr->mMaxFileSize < theEnd ?
                            theEnd - theFilePtr->mMaxFileSize : 0),
                    *theFilePtr,
                    inFd
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
                Delete (*S3Req::Get(
                    *this,
                    inRequest,
                    inReqType,
                    0, // inStartBlockIdx,
                    0, // inBufferCount,
                    0, // inInputIteratorPtr,
                    0, // size,
                    File(inNamePtr + mFilePrefix.length()),
                    -1
                ));
                break;
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
            mFileName = string();
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
    class S3Req : public KfsCallbackObj
    {
    public:
        static S3Req* Get(
            Outer&         inOuter,
            Request&       inRequest,
            ReqType        inReqType,
            BlockIdx       inStartBlockIdx,
            int            inBufferCount,
            InputIterator* inInputIteratorPtr,
            size_t         inSize,
            const File&    inFile,
            int            inFd)
        {
            char* const theMemPtr = new char[
                sizeof(S3Req) + sizeof(char*) * max(0, inBufferCount)];
            S3Req& theReq = *(new (theMemPtr) S3Req(
                inOuter,
                inRequest,
                inReqType,
                inStartBlockIdx,
                inBufferCount,
                inSize,
                inFile,
                inFd
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
        const char* GetMd5Sum()
        {
            if (! *mMd5Sum) {
                ostream& theStream = mOuter.mMdStream.Reset();
                char**   thePtr    = GetBuffers();
                size_t   thePos;
                for (thePos = 0;
                        thePos + mOuter.mBlockSize <= mSize;
                        thePos += mOuter.mBlockSize) {
                    theStream.write(*thePtr++, mOuter.mBlockSize);
                }
                if (thePos < mSize) {
                    theStream.write(*thePtr, mSize - thePos);
                }
                if (theStream) {
                    const size_t theLen =
                        mOuter.mMdStream.GetMdBin(mOuter.mTmpMdBuf);
                    QCRTASSERT(theLen <= sizeof(mOuter.mTmpMdBuf));
                    const int theB64Len = Base64::Encode(
                        reinterpret_cast<const char*>(mOuter.mTmpMdBuf),
                        theLen, mMd5Sum);
                    QCRTASSERT(
                        0 < theB64Len &&
                        (size_t)theB64Len < sizeof(mMd5Sum)
                    );
                    mMd5Sum[theB64Len] = 0;
                } else {
                    mOuter.FatalError("md5 sum failure");
                }
            }
            return mMd5Sum;
        }
        size_t GetSize() const
            { return mSize; }
        size_t GetStartBlockIdx() const
            { return mStartBlockIdx; }
        void Run()
        {
            mTimer.RemoveTimeout();
            if (! mOuter.mNetManager.IsRunning()) {
                Done(-EIO, "shutdown");
            }
            mRetryCount++;
            mStartTime = mOuter.Now();
            mBufferPtr = GetBuffers();
            mPos       = 0;
            mBufRem    = 0;
            switch (mReqType) {
                case QCDiskQueue::kReqTypeRead:
                    mOuter.Read(*this);
                    return;
                case QCDiskQueue::kReqTypeWrite:
                case QCDiskQueue::kReqTypeWriteSync:
                    mOuter.Write(*this);
                    return;
                case QCDiskQueue::kReqTypeDelete:
                    mOuter.Delete(*this);
                    return;
                default:
                    mOuter.FatalError("invalid request type");
                    return;
            }
        }
        void Done(
            int         inStatus,
            const char* inErrorMsgPtr)
        {
#if 0
            KFS_LOG_STREAM_START(S3StatusOK == inStatus ?
                    MsgLogger::kLogLevelDEBUG :
                    MsgLogger::kLogLevelERROR, theMsg) << mOuter.mLogPrefix <<
                reinterpret_cast<const void*>(this) <<
                " done: "    << RequestTypeToName(mReqType) <<
                "/"          << mReqType <<
                " "          << mFileName <<
                " startb: "  << mStartBlockIdx <<
                " size: "    << mSize <<
                " pos: "     << mPos <<
                " attempt: " << mRetryCount <<
                " curl: "    << reinterpret_cast<const void*>(mCurlPtr) <<
                " status: "  << inStatus <<
                " "          << S3_get_status_name(inStatus)
                ;
                ostream& theStream = theMsg.GetStream();
                if (inErrorPtr) {
                    theStream <<
                    " message: "  <<
                        (inErrorPtr->message ? inErrorPtr->message : "nil") <<
                    " resource: " <<
                        (inErrorPtr->resource ? inErrorPtr->resource : "nil") <<
                    " details: "  << (inErrorPtr->furtherDetails ?
                            inErrorPtr->furtherDetails : "nil")
                    ;
                    for (int i = 0; i < inErrorPtr->extraDetailsCount; i++) {
                        theStream <<
                            " "   << (inErrorPtr->extraDetails[i].name ?
                                inErrorPtr->extraDetails[i].name : "nil") <<
                            ": "  << (inErrorPtr->extraDetails[i].value ?
                            inErrorPtr->extraDetails[i].value : "nil");
                    }
                }
            KFS_LOG_STREAM_END;
#endif
            if (mOuter.mNetManager.IsRunning() &&
                    mRetryCount < mOuter.mMaxRetryCount) {
                const File* theFilePtr;
                if (mFd < 0 || ((theFilePtr = mOuter.GetFilePtr(mFd)) &&
                        theFilePtr->mGeneration == mGeneration)) {
                    const time_t theFutureDelta = max(time_t(1),
                        mOuter.mRetryInterval -
                        (mOuter.Now() - mStartTime));
                    KFS_LOG_STREAM_DEBUG << mOuter.mLogPrefix <<
                        reinterpret_cast<const void*>(this) <<
                        " "               << RequestTypeToName(mReqType) <<
                        "/"               << mReqType <<
                        " "               << mFileName <<
                        " scheduling retry in " <<
                            theFutureDelta << " seconds" <<
                    KFS_LOG_EOM;
                    mTimer.SetTimeout(theFutureDelta);
                    return;
                }
            }
            int64_t            theIoByteCount;
            int                theSysErr;
            QCDiskQueue::Error theError;
            if (0 == inStatus) {
                theIoByteCount = mPos;
                theSysErr      = 0;
                theError       = QCDiskQueue::kErrorNone;
            } else {
                theIoByteCount = 0;
                theSysErr      = inStatus < 0 ? -inStatus : inStatus;
                switch (mReqType) {
                    case QCDiskQueue::kReqTypeRead:
                        theError  = QCDiskQueue::kErrorRead;
                        break;
                    case QCDiskQueue::kReqTypeWrite:
                    case QCDiskQueue::kReqTypeWriteSync:
                        theError  = QCDiskQueue::kErrorWrite;
                        break;
                    case QCDiskQueue::kReqTypeDelete:
                        theError  = QCDiskQueue::kErrorDelete;
                        break;
                    default:
                        mOuter.FatalError("invalid request type");
                        return;
                }
            }
            Outer&         theOuter         = mOuter;
            Request&       theRequest       = mRequest;
            BlockIdx const theStartBlockIdx = mStartBlockIdx;
            Dispose();
            theOuter.mDiskQueuePtr->Done(
                theOuter,
                theRequest,
                theError,
                theSysErr,
                theIoByteCount,
                theStartBlockIdx
            );
        }
        int GetRetryCount() const
            { return mRetryCount; }
        const string& GetFileName() const
            { return mFileName; }
        int Timeout(
            int   inEvent,
            void* inDataPtr)
        {
            QCRTASSERT(EVENT_INACTIVITY_TIMEOUT == inEvent && ! inDataPtr);
            Run();
            return 0;
        }
    private:
        class ReadFunc
        {
        public:
            void operator()(
                const char* inFromPtr,
                char*       inToPtr,
                size_t      inSize) const
                { memcpy(inToPtr, inFromPtr, inSize); }
        };
        class WriteFunc
        {
        public:
            void operator()(
                char*       inToPtr,
                const char* inFromPtr,
                size_t      inSize) const
                { memcpy(inToPtr, inFromPtr, inSize); }
        };
        typedef NetManager::Timer Timer;

        Outer&           mOuter;
        Request&         mRequest;
        ReqType    const mReqType;
        BlockIdx   const mStartBlockIdx;
        Generation const mGeneration;
        int        const mFd;
        string     const mFileName;
        size_t           mPos;
        size_t           mSize;
        size_t           mBufRem;
        char**           mBufferPtr;
        int              mRetryCount;
        time_t           mStartTime;
        Timer            mTimer;
        char             mMd5Sum[(128 / 8 + 2) / 3 * 4 + 1];
                         // Base64::EncodedLength(128 / 8) + 1

        S3Req(
            Outer&         inOuter,
            Request&       inRequest,
            ReqType        inReqType,
            BlockIdx       inStartBlockIdx,
            int            inBufferCount,
            size_t         inSize,
            const File&    inFile,
            int            inFd)
            : KfsCallbackObj(),
              mOuter(inOuter),
              mRequest(inRequest),
              mReqType(inReqType),
              mStartBlockIdx(inStartBlockIdx),
              mGeneration(inFile.mGeneration),
              mFd(inFd),
              mFileName(inFile.mFileName),
              mPos(0),
              mSize(inSize),
              mBufRem(0),
              mBufferPtr(0),
              mRetryCount(0),
              mStartTime(mOuter.Now()),
              mTimer(mOuter.mNetManager, *this)
        {
            SET_HANDLER(this, &S3Req::Timeout);
            mMd5Sum[0] = 0;
            KFS_LOG_STREAM_DEBUG << mOuter.mLogPrefix <<
                reinterpret_cast<const void*>(this) <<
                " S3Req: " << RequestTypeToName(mReqType) <<
                "/"        << mReqType <<
                " "        << mFileName <<
                " fd: "    << mFd <<
                " gen: "   << mGeneration <<
            KFS_LOG_EOM;
            mOuter.mRequestCount++;
        }
        ~S3Req()
        {
            KFS_LOG_STREAM_DEBUG << mOuter.mLogPrefix <<
                reinterpret_cast<const void*>(this) <<
                " ~S3Req: " << RequestTypeToName(mReqType) <<
                "/"         << mReqType <<
                " "         << mFileName <<
                " fd: "     << mFd <<
                " gen: "    << mGeneration <<
            KFS_LOG_EOM;
            mOuter.mRequestCount--;
        }
        char** GetBuffers()
            { return reinterpret_cast<char**>(this + 1); }
        void Dispose()
        {
            char* const thePtr = reinterpret_cast<char*>(this);
            this->~S3Req();
            delete [] thePtr;
        }
        template<typename T, typename FT>
        int IOSelf(
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
                    *mBufferPtr + mOuter.mBlockSize - mBufRem, theSize);
                mBufRem -= theSize;
                mPos    += theSize;
                if (0 < mBufRem) {
                    return (int)theSize;
                }
                mBufferPtr++;
                thePtr += theSize;
            }
            T* const theEndPtr = inBufferPtr + theRem;
            theSize = mOuter.mBlockSize;
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
                mBufRem = mOuter.mBlockSize - theSize;
            }
            return (int)(thePtr - (theEndPtr - theRem));
        }
        template<typename T, typename FT>
        int IO(
            int       inBufferSize,
            T*        inBufferPtr,
            const FT& inFunc)
        {
            const int theRet = IOSelf(inBufferSize, inBufferPtr, inFunc);
            KFS_LOG_STREAM_DEBUG << mOuter.mLogPrefix <<
                reinterpret_cast<const void*>(this) <<
                " "         << RequestTypeToName(mReqType) <<
                "/"         << mReqType <<
                " startb: " << mStartBlockIdx <<
                " bytes: "  << inBufferSize <<
                " / "       << theRet <<
                " pos: "    << mPos <<
                " size: "   << mSize <<
                " brem: "   << mBufRem <<
                " bidx: "   << (mBufferPtr - GetBuffers()) <<
            KFS_LOG_EOM;
            return theRet;
        }
        const char* ShowData(
            char*       inBufferPtr,
            size_t      inBufSize,
            const char* inDataPtr,
            size_t      inDataSize)
        {
            if (inBufSize <= 0) {
                return "";
            }
            const char* const kHexPtr = "0123456789ABCDEF";
            size_t            i       = 0;
            for (size_t k = 0; k < inDataSize && i + 1 < inBufSize; k++) {
                const int theSym = inDataPtr[k] & 0xFF;
                if (' ' <= theSym && theSym < 127) {
                    inBufferPtr[i++] = (char)theSym;
                } else {
                    if (inBufSize <= i + 3) {
                        break;
                    }
                    inBufferPtr[i++] = '\\';
                    inBufferPtr[i++] = kHexPtr[(theSym >> 4) & 0xF];
                    inBufferPtr[i++] = kHexPtr[theSym & 0xF];
                }
            }
            inBufferPtr[i] = 0;
            return inBufferPtr;
        }
    };
    friend class S3Req;

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
    long                mLowSpeedLimit;
    long                mLowSpeedTime;
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
          mLowSpeedLimit(4 << 10),
          mLowSpeedTime(10),
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
    void Read(
        S3Req& inReq)
    {
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "get: "      << reinterpret_cast<const void*>(&inReq) <<
            " "          << inReq.GetFileName() <<
            " pos: "     << inReq.GetStartBlockIdx() <<
            " * "        << mBlockSize <<
            " size: "    << inReq.GetSize() <<
            " attempt: " << inReq.GetRetryCount() <<
        KFS_LOG_EOM;
    }
    void Write(
        S3Req& inReq)
    {
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            reinterpret_cast<const void*>(&inReq) <<
            " put: "     << inReq.GetFileName() <<
            " pos: "     << inReq.GetStartBlockIdx() <<
            " * "        << mBlockSize <<
            " size: "    << inReq.GetSize() <<
            " md5: "     << inReq.GetMd5Sum() <<
            " attempt: " << inReq.GetRetryCount() <<
        KFS_LOG_EOM;
    }
    void Delete(
        S3Req& inReq)
    {
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "delete: "   << reinterpret_cast<const void*>(&inReq) <<
            " "          << inReq.GetFileName() <<
            " attempt: " << inReq.GetRetryCount() <<
        KFS_LOG_EOM;
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
