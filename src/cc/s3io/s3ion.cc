//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/10/04
// Author: Mike Ovsiannikov
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
// AWS S3 IO method implementation.
//
//----------------------------------------------------------------------------

#include "chunk/IOMethodDef.h"

#include "common/kfsdecls.h"
#include "common/MsgLogger.h"
#include "common/IntToString.h"
#include "common/Properties.h"
#include "common/httputils.h"
#include "common/XmlScanner.h"

#include "qcdio/QCUtils.h"
#include "qcdio/QCDLList.h"
#include "qcdio/QCMutex.h"
#include "qcdio/qcdebug.h"
#include "qcdio/qcstutils.h"
#include "qcdio/qcdefs.h"

#include "kfsio/Base64.h"
#include "kfsio/TransactionalClient.h"
#include "kfsio/NetManager.h"
#include "kfsio/HttpResponseHeaders.h"
#include "kfsio/HttpChunkedDecoder.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/KfsCallbackObj.h"
#include "kfsio/event.h"
#include "kfsio/IOBufferWriter.h"

#include <errno.h>
#include <string.h>
#include <time.h>

#include <openssl/hmac.h>
#include <openssl/err.h>
#include <openssl/evp.h>

#include <string>
#include <vector>
#include <algorithm>

namespace KFS
{

using std::string;
using std::vector;
using std::max;
using std::min;
using std::lower_bound;
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

const char* const kS3IODaateWeekDays[7] =
    { "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
const char* const kS3IODateMonths[12] = {
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov",
    "Dec"
};

typedef PropertiesTokenizer::Token S3StrToken;

const int64_t kS3MinPartSize = int64_t(5) << 20;
const string  kS3EmptyString;

const S3StrToken kS3StrMPutInitResultResultBucket(
    "/InitiateMultipartUploadResult/Bucket");
const S3StrToken kS3StrMPutInitResultResultKey(
    "/InitiateMultipartUploadResult/Key");
const S3StrToken kS3StrMPutInitResultResulUploadId(
    "/InitiateMultipartUploadResult/UploadId");

const S3StrToken kS3MPutCompleteStart          ("<CompleteMultipartUpload>");
const S3StrToken kS3MPutCompleteEnd            ("</CompleteMultipartUpload>");
const S3StrToken kS3MPutCompletePartStart      ("<Part>");
const S3StrToken kS3MPutCompletePartEnd        ("</Part>");
const S3StrToken kS3MPutCompletePartNumberStart("<PartNumber>");
const S3StrToken kS3MPutCompletePartNumberEnd  ("</PartNumber>");
const S3StrToken kS3MPutCompleteETagStart      ("<ETag>");
const S3StrToken kS3MPutCompleteETagEnd        ("</ETag>");

const S3StrToken kS3StrMPutCompleteResultBucket(
    "/CompleteMultipartUploadResult/Bucket");
const S3StrToken kS3StrMPutCompleteResultKey(
    "/CompleteMultipartUploadResult/Key");
const S3StrToken kS3StrMPutCompleteResultETag(
    "/CompleteMultipartUploadResult/ETag");

const S3StrToken kS3StrGetUploadsResultBucket(
    "/ListMultipartUploadsResult/Bucket");
const S3StrToken kS3StrGetUploadsResultMaxUploads(
    "/ListMultipartUploadsResult/MaxUploads");
const S3StrToken kS3StrGetUploadsResultIsTruncated(
    "/ListMultipartUploadsResult/IsTruncated");
const S3StrToken kS3StrGetUploadsResultUploadKey(
    "/ListMultipartUploadsResult/Upload/Key");
const S3StrToken kS3StrGetUploadsResultUploadUploadId(
    "/ListMultipartUploadsResult/Upload/UploadId");

const char* const kS3AmzSecurityTokenNamePtr        = "x-amz-security-token";
const char* const kS3AmzServerSideEncryptionNamePtr =
    "x-amz-server-side-encryption";
const char* const kS3AmzStorageClassNamePtr         = "x-amz-storage-class";
const char* const kS3EncryptionTypePtr              = "aws:kms";
const char* const kS3ValidStorageClasses[]          = {
    "", // empty string -- no storage class header sent
    "STANDARD",
    "STANDARD_IA",
    "REDUCED_REDUNDANCY",
    0   // Sentinel
};

class S3ION : public IOMethod
{
public:
    typedef S3ION                      Outer;
    typedef QCDiskQueue::Request       Request;
    typedef QCDiskQueue::ReqType       ReqType;
    typedef QCDiskQueue::BlockIdx      BlockIdx;
    typedef QCDiskQueue::InputIterator InputIterator;

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
                inUrlPtr[2] != ':' ||
                inUrlPtr[3] != '/' ||
                inUrlPtr[4] != '/') {
            return 0;
        }
        string theConfigPrefix = inUrlPtr + 5;
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
#if OPENSSL_VERSION_NUMBER < 0x10100000L
        HMAC_CTX_cleanup(&mHmacCtx);
        EVP_MD_CTX_cleanup(&mMdCtx);
#else
#if OPENSSL_VERSION_NUMBER < 0x30000000L
        HMAC_CTX_free(&mHmacCtx);
#else
        EVP_MAC_CTX_free(&mHmacCtx);
#endif
        EVP_MD_CTX_free(&mMdCtx);
#endif
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
        if (inMinWriteBlkSize < kS3MinPartSize) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "write block size: " << inMinWriteBlkSize <<
                " less than part size: " << kS3MinPartSize <<
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
        mMaxFileSize  = inMaxFileSize;
        outCanEnforceIoTimeoutFlag = true;
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "prefix:"
            " file: "   << mFilePrefix <<
            " config: " << mConfigPrefix <<
        KFS_LOG_EOM;
        return true;
    }
    virtual void SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters)
    {
        QCStMutexLocker theLock(mMutex);
        mUpdatedFullConfigPrefix = inPrefixPtr ? inPrefixPtr : "";
        mUpdatedFullConfigPrefix += mConfigPrefix;
        inParameters.copyWithPrefix(
            mUpdatedFullConfigPrefix, mUpdatedParameters);
        mParametersUpdatedFlag = true;
    }
    virtual void ProcessAndWait()
    {
        bool theUpdateParametersFlag = false;
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
        while (! mRunList.empty()) {
            mRunList.swap(mCurRunList);
            for (RunList::const_iterator theIt = mCurRunList.begin();
                    theIt != mCurRunList.end();
                    ++theIt) {
                if (IsRunning()) {
                    mClient.Run(**theIt);
                } else {
                    (*theIt)->Error(-EIO, "canceled");
                }
            }
            mCurRunList.clear();
        }
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
        if (! theFile.mReadOnlyFlag && theFile.mMaxFileSize < inEof) {
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
            " upload: " << theFile.mUploadId <<
            " parts: "  << theFile.mMPutParts.size() <<
            " commit: " << theFile.mCommitFlag <<
            " error: "  << theFile.mErrorFlag <<
            " status: " << theRet <<
        KFS_LOG_EOM;
        if (theFile.mErrorFlag &&
                ! theFile.mUploadId.empty() &&
                File::List::IsEmpty(theFile.mPendingListPtr) &&
                IsRunning()) {
            mClient.Run(*(new S3Delete(
                *this, theFile.mFileName, theFile.mUploadId)));
        }
        MPPut* thePtr;
        while ((thePtr = File::List::Back(theFile.mPendingListPtr))) {
            thePtr->Cancel(theFile);
        }
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
                        "invalid read on write only file:" <<
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
                if (inInputIteratorPtr && inInputIteratorPtr->Get()) {
                    FatalError("read buffer pre-allocation is not supported");
                    theError  = QCDiskQueue::kErrorRead;
                    theSysErr = EINVAL;
                    break;
                }
                if (IsRunning()) {
                    mClient.Run(*(new S3Get(
                        *this,
                        inRequest,
                        inReqType,
                        theFilePtr->mFileName,
                        inStartBlockIdx,
                        inBufferCount,
                        theFilePtr->mGeneration,
                        inFd
                    )));
                    return;
                }
                theError  = QCDiskQueue::kErrorRead;
                theSysErr = EIO;
                break;
            case QCDiskQueue::kReqTypeWrite:
            case QCDiskQueue::kReqTypeWriteSync:
                theError = QCDiskQueue::kErrorWrite;
                if (! theFilePtr || theFilePtr->mReadOnlyFlag ||
                        inBufferCount <= 0) {
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
                theSysErr = EINVAL;
                if (theFilePtr->mCommitFlag) {
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        " write sync already issued"
                        " invalid write attempt:"
                        " type: "  << RequestTypeToName(inReqType) <<
                        " blocks:"
                        " pos: "   << inStartBlockIdx <<
                        " count: " << inBufferCount <<
                        " eof: "   << theFilePtr->mMaxFileSize <<
                        " / "      << inEof <<
                        " fd: "    << inFd <<
                        " gen: "   << theFilePtr->mGeneration <<
                        " "        << theFilePtr->mFileName <<
                    KFS_LOG_EOM;
                    break;
                }
                if (0 <= inEof) {
                    if (mMaxFileSize < inEof) {
                        KFS_LOG_STREAM_ERROR << mLogPrefix <<
                            "eof exceeds specified max file size"
                            " invalid write attempt:"
                            " type: "  << RequestTypeToName(inReqType) <<
                            " blocks:"
                            " pos: "   << inStartBlockIdx <<
                            " count: " << inBufferCount <<
                            " eof: "   << theFilePtr->mMaxFileSize <<
                            " => "     << inEof <<
                            " fd: "    << inFd <<
                            " gen: "   << theFilePtr->mGeneration <<
                            " "        << theFilePtr->mFileName <<
                        KFS_LOG_EOM;
                        break;
                    }
                    if (0 <= theFilePtr->mMaxFileSize &&
                            theFilePtr->mMaxFileSize < inEof) {
                        KFS_LOG_STREAM_ERROR << mLogPrefix <<
                            "eof exceeds previously set max file size"
                            " invalid write attempt:"
                            " type: "  << RequestTypeToName(inReqType) <<
                            " blocks:"
                            " pos: "   << inStartBlockIdx <<
                            " count: " << inBufferCount <<
                            " eof: "   << theFilePtr->mMaxFileSize <<
                            " => "     << inEof <<
                            " fd: "    << inFd <<
                            " gen: "   << theFilePtr->mGeneration <<
                            " "        << theFilePtr->mFileName <<
                        KFS_LOG_EOM;
                        break;
                    }
                    if (! theFilePtr->mMPutParts.empty() && inEof <
                            theFilePtr->mMPutParts.back().mEnd * mBlockSize) {
                        KFS_LOG_STREAM_ERROR << mLogPrefix <<
                            "eof is less than previously submited "
                                "partial write"
                            " invalid write attempt:"
                            " type: "  << RequestTypeToName(inReqType) <<
                            " blocks:"
                            " pos: "   << inStartBlockIdx <<
                            " count: " << inBufferCount <<
                            " eof: "   << theFilePtr->mMaxFileSize <<
                            " => "     << inEof <<
                            " write"
                            " end: "   << theFilePtr->mMPutParts.back().mEnd *
                                mBlockSize <<
                            " fd: "    << inFd <<
                            " gen: "   << theFilePtr->mGeneration <<
                            " "        << theFilePtr->mFileName <<
                        KFS_LOG_EOM;
                        break;
                    }
                    theFilePtr->mMaxFileSize = inEof;
                }
                if (QCDiskQueue::kReqTypeWriteSync == inReqType &&
                        theFilePtr->mMaxFileSize < 0) {
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
                theEnd = (inStartBlockIdx + inBufferCount) * mBlockSize;
                if ((theEnd < theFilePtr->mMaxFileSize ||
                        (theFilePtr->mMaxFileSize < 0 &&
                        QCDiskQueue::kReqTypeWrite == inReqType)) &&
                        (0 != (inStartBlockIdx * mBlockSize) % kS3MinPartSize ||
                            0 != theEnd % kS3MinPartSize)) {
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "invalid partial write attempt:" <<
                        " type: "  << RequestTypeToName(inReqType) <<
                        " blocks:"
                        " pos: "   << inStartBlockIdx <<
                        " count: " << inBufferCount <<
                        " eof: "   << theFilePtr->mMaxFileSize <<
                        " / "      << inEof <<
                        " fd: "    << inFd <<
                        " gen: "   << theFilePtr->mGeneration <<
                        " "        << theFilePtr->mFileName <<
                    KFS_LOG_EOM;
                    break;
                }
                if (theFilePtr->mMaxFileSize + mBlockSize <= theEnd) {
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "write past last block"
                        " pos: " << theEnd <<
                        " max: " << theFilePtr->mMaxFileSize <<
                        " fd: "  << inFd <<
                        " gen: " << theFilePtr->mGeneration <<
                        " "      << theFilePtr->mFileName <<
                    KFS_LOG_EOM;
                    break;
                }
                if (theFilePtr->mErrorFlag) {
                    theSysErr = EIO;
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "unrecoverable write error has already occurred"
                        " type: "  << RequestTypeToName(inReqType) <<
                        " blocks:"
                        " pos: "   << inStartBlockIdx <<
                        " count: " << inBufferCount <<
                        " eof: "   << inEof <<
                        " fd: "    << inFd <<
                        " gen: "   << theFilePtr->mGeneration <<
                        " "        << theFilePtr->mFileName <<
                    KFS_LOG_EOM;
                    break;
                }
                theFilePtr->mWriteOnlyFlag = true;
                if (IsRunning()) {
                    IOBuffer theBuf;
                    char*    thePtr;
                    int      theRem = (int)(
                        min(theFilePtr->mMaxFileSize, theEnd) -
                        inStartBlockIdx * mBlockSize);
                    while (0 < theRem && (thePtr = inInputIteratorPtr->Get())) {
                        const int theLen = min(theRem, mBlockSize);
                        theBuf.Append(IOBufferData(
                            IOBufferData::IOBufferBlockPtr(
                                thePtr, DoNotDeallocate()),
                            mBlockSize, 0, theLen
                        ));
                        theRem -= theLen;
                    }
                    if (0 < theRem) {
                        KFS_LOG_STREAM_ERROR << mLogPrefix <<
                            "write invalid buffer count: " << inBufferCount <<
                            " end: " << theEnd <<
                            " max: " << theFilePtr->mMaxFileSize <<
                            " fd: "  << inFd <<
                            " gen: " << theFilePtr->mGeneration <<
                            " "      << theFilePtr->mFileName <<
                        KFS_LOG_EOM;
                        break;
                    }
                    if (QCDiskQueue::kReqTypeWriteSync == inReqType &&
                            theFilePtr->mMPutParts.empty()) {
                        QCASSERT(theFilePtr->mUploadId.empty());
                        mClient.Run(*(new S3Put(
                            *this,
                            inRequest,
                            inReqType,
                            theFilePtr->mFileName,
                            inStartBlockIdx,
                            theFilePtr->mGeneration,
                            inFd,
                            theBuf
                        )));
                        return;
                    }
                    const bool theFirstFlag = theFilePtr-> mMPutParts.empty();
                    const BlockIdx theEnd   = inStartBlockIdx + inBufferCount;
                    size_t     theCurIdx    = 0;
                    if (theFirstFlag) {
                        theFilePtr->mMPutParts.reserve(
                            (mMaxFileSize + kS3MinPartSize - 1) /
                            kS3MinPartSize);
                        theFilePtr->mMPutParts.push_back(
                            MPutPart(inStartBlockIdx, theEnd));
                    } else {
                        MPutParts::iterator const theIt = lower_bound(
                            theFilePtr->mMPutParts.begin(),
                            theFilePtr->mMPutParts.end(),
                            inStartBlockIdx
                        );
                        if (theIt != theFilePtr->mMPutParts.end() &&
                                theIt->mStart < theEnd) {
                            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                                "invalid partial write attempt:" <<
                                " reigion already written:"
                                " type: "  <<
                                    RequestTypeToName(inReqType) <<
                                " blocks:"
                                " start: " << theIt->mStart <<
                                " => "     << inStartBlockIdx <<
                                " end: "   << theIt->mEnd <<
                                " => "     << theEnd <<
                                " eof: "   << inEof <<
                                " fd: "    << inFd <<
                                " gen: "   << theFilePtr->mGeneration <<
                                " "        << theFilePtr->mFileName <<
                            KFS_LOG_EOM;
                            break;
                        }
                        theCurIdx = theFilePtr->mMPutParts.insert(
                            theIt, MPutPart(inStartBlockIdx, theEnd)) -
                            theFilePtr->mMPutParts.begin();
                    }
                    if (QCDiskQueue::kReqTypeWriteSync == inReqType) {
                        // Validate that there are no gaps.
                        bool     theErrorFlag = false;
                        BlockIdx thePrevEnd   = 0;
                        for (MPutParts::const_iterator
                                theIt = theFilePtr->mMPutParts.begin();
                                theFilePtr->mMPutParts.end() != theIt;
                                ++theIt) {
                            if (theIt->mStart != thePrevEnd) {
                                theErrorFlag = true;
                                KFS_LOG_STREAM_ERROR << mLogPrefix <<
                                    "invalid sync write attempt:" <<
                                    " non ajacent region:"
                                    " blocks:"
                                    " prior:"
                                    " end: "   << thePrevEnd <<
                                    " start: " << theIt->mStart <<
                                    " end: "   << theIt->mEnd <<
                                    " eof: "   << theFilePtr->mMaxFileSize <<
                                    " fd: "    << inFd <<
                                    " gen: "   << theFilePtr->mGeneration <<
                                    " "        << theFilePtr->mFileName <<
                                KFS_LOG_EOM;
                            }
                            thePrevEnd = theIt->mEnd;
                        }
                        if (theErrorFlag) {
                            theFilePtr->mMPutParts.erase(
                                theFilePtr->mMPutParts.begin() + theCurIdx);
                            break;
                        }
                        theFilePtr->mCommitFlag = true;
                        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                            "commit write:"
                            " eof: "   << theFilePtr->mMaxFileSize <<
                            " fd: "    << inFd <<
                            " gen: "   << theFilePtr->mGeneration <<
                            " "        << theFilePtr->mFileName <<
                        KFS_LOG_EOM;
                    }
                    MPPut& theReq = *(new MPPut(
                        *this,
                        inRequest,
                        inReqType,
                        theFilePtr->mFileName,
                        inStartBlockIdx,
                        theFilePtr->mGeneration,
                        inFd,
                        theBuf
                    ));
                    File::List::PushBack(
                        theFilePtr->mPendingListPtr, theReq);
                    if (theFilePtr->mUploadId.empty()) {
                        if (! theFirstFlag) {
                            return; // Wait for get id completion.
                        }
                        // Enqueue get id request.
                        IOBuffer theBuf;
                        MPPut&   theGetIdReq = *(new MPPut(
                            *this,
                            inRequest,
                            inReqType,
                            theFilePtr->mFileName,
                            inStartBlockIdx,
                            theFilePtr->mGeneration,
                            inFd,
                            theBuf
                        ));
                        File::List::PushFront(
                            theFilePtr->mPendingListPtr, theGetIdReq);
                        mClient.Run(theGetIdReq);
                    } else {
                        mClient.Run(theReq);
                    }
                    return;
                }
                theSysErr = EIO;
                break;
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
                if (IsRunning()) {
                    mClient.Run(*(new S3Delete(
                        *this, inRequest, inReqType,
                        string(inNamePtr + mFilePrefix.length()),
                        ! mDeleteNoUploadListFlag
                    )));
                } else {
                    theError  = QCDiskQueue::kErrorDelete;
                    theSysErr = EIO;
                }
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
    enum
    {
        kMd5Len            = 128 / 8,
        kSha1Len           = 160 / 8,
        kSha256Len         = 256 / 8,
        kMaxMdLen          = kSha256Len,
        kISODateLen        = 8,
        kV4SignDateLen     = kISODateLen,
        kMd5Base64Len      = (kMd5Len + 2) / 3 * 4,
        kSha256HexLen      = kSha256Len * 2,
        kMaxDateTimeLen    = 32,
        kMaxUploadIdLength = 4 << 10
    };
    typedef char Md5Buf[kMd5Len];
    typedef char Sha256Buf[kSha256Len];
    typedef char Sha1Buf[kSha1Len];

    typedef uint64_t Generation;
    class MPPut;
    class MPutPart
    {
    public:
        typedef StringBufT<66> ETag;
        MPutPart(
            BlockIdx inStart = 0,
            BlockIdx inEnd   = 0)
            : mStart(inStart),
              mEnd(inEnd),
              mETag()
            {}
        bool operator<(
            const BlockIdx& inVal) const
            { return (mStart < inVal); }
        BlockIdx mStart;
        BlockIdx mEnd;
        ETag     mETag;
    };
    typedef vector<MPutPart> MPutParts;

    class File
    {
    public:
        typedef QCDLList<MPPut> List;

        File(
            const char* inFileNamePtr = 0)
            : mFileName(inFileNamePtr ? inFileNamePtr : ""),
              mUploadId(),
              mReadOnlyFlag(false),
              mCreateFlag(false),
              mCreateExclusiveFlag(false),
              mWriteOnlyFlag(false),
              mCommitFlag(false),
              mErrorFlag(false),
              mMaxFileSize(-1),
              mGeneration(0),
              mMPutParts()
            { List::Init(mPendingListPtr); }
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
            mFileName            = kS3EmptyString; // De-allocate.
            mUploadId            = kS3EmptyString;
            mReadOnlyFlag        = false;
            mCreateFlag          = false;
            mCreateExclusiveFlag = false;
            mWriteOnlyFlag       = false;
            mCommitFlag          = false;
            mErrorFlag           = false;
            mMaxFileSize         = -1;
            mGeneration          = 0;
            MPutParts theTmp;
            mMPutParts.swap(theTmp); // De-allocate.
            QCASSERT(List::IsEmpty(mPendingListPtr));
            List::Init(mPendingListPtr);
        }
        void MakeReadOnly()
        {
            string theFileName;
            theFileName.swap(mFileName);
            Generation const theGeneration = mGeneration;
            Reset();
            theFileName.swap(mFileName);
            mGeneration = theGeneration;
            mReadOnlyFlag = true;
        }
        string     mFileName;
        string     mUploadId;
        bool       mReadOnlyFlag:1;
        bool       mCreateFlag:1;
        bool       mCreateExclusiveFlag:1;
        bool       mWriteOnlyFlag:1;
        bool       mCommitFlag:1;
        bool       mErrorFlag:1;
        int64_t    mMaxFileSize;
        Generation mGeneration;
        MPutParts  mMPutParts;
        MPPut*     mPendingListPtr[1];
    };
    typedef vector<File> FileTable;
    class IOBufferInputIterator : private IOBuffer::ByteIterator
    {
    public:
        IOBufferInputIterator(
            const IOBuffer& inBuffer)
            : IOBuffer::ByteIterator(inBuffer)
            {}
        int Next()
        {
            const char* const thePtr = IOBuffer::ByteIterator::Next();
            return (thePtr ? (*thePtr & 0xFF) : -1);
        }
    };
    class S3Req : public KfsCallbackObj, public TransactionalClient::Transaction
    {
    public:
        typedef QCDiskQueue::Request Request;

        S3Req(
            Outer&        inOuter,
            Request*      inRequestPtr,
            ReqType       inReqType,
            const string& inFileName,
            BlockIdx      inStartBlockIdx = 0,
            Generation    inGeneration    = 0,
            int           inFd            = -1)
            : KfsCallbackObj(),
              TransactionalClient::Transaction(),
              mOuter(inOuter),
              mRequestPtr(inRequestPtr),
              mReqType(inReqType),
              mFileName(inFileName),
              mGeneration(inGeneration),
              mFd(inFd),
              mRetryCount(0),
              mStartTime(mOuter.Now()),
              mTimer(mOuter.mNetManager, *this),
              mSentFlag(false),
              mReceivedHeadersFlag(false),
              mReadTillEofFlag(false),
              mHeaderLength(-1),
              mError(QCDiskQueue::kErrorNone),
              mSysError(0),
              mStartBlockIdx(inStartBlockIdx),
              mIOBuffer(),
              mHeaders(),
              mHttpChunkedDecoder(mIOBuffer, mOuter.mMaxReadAhead)
        {
            mOuter.mRequestCount++;
            QCASSERT(0 < mOuter.mRequestCount);
            SET_HANDLER(this, &S3Req::Timeout);
        }
        int Timeout(
            int   inEvent,
            void* inDataPtr)
        {
            QCRTASSERT(EVENT_INACTIVITY_TIMEOUT == inEvent && ! inDataPtr);
            mTimer.RemoveTimeout();
            if (mOuter.IsRunning()) {
                mStartTime = mOuter.Now();
                mOuter.mClient.Run(*this);
            } else {
                Error(-EIO, "canceled by shutdown");
            }
            return 0;
        }
        virtual ostream& Display(
            ostream& inStream) const = 0;
        virtual void Error(
            int         inStatus,
            const char* inMsgPtr)
        {
            const int  kMinAgainRetryInterval = 2;
            const bool theResubmiNowFlag      = -EAGAIN == inStatus &&
                mOuter.Now() <= mStartTime + kMinAgainRetryInterval &&
                mOuter.IsRunning();
            KFS_LOG_STREAM(theResubmiNowFlag ?
                MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
                mOuter.mLogPrefix << Show(*this) <<
                " network error: " << inStatus  <<
                " message: "       << (inMsgPtr ? inMsgPtr : "") <<
                " started: "       << (mOuter.Now() - mStartTime) <<
                    " secs. ago" <<
            KFS_LOG_EOM;
            if (theResubmiNowFlag) {
                Reset();
                mTimer.RemoveTimeout();
                mOuter.ScheduleNext(*this);
            } else {
                Retry();
            }
        }
        void Retry()
        {
            const File* theFilePtr;
            bool const  theRetryFlag =
                mOuter.IsRunning() && 0 <= mOuter.mRetryInterval &&
                ++mRetryCount < mOuter.mMaxRetryCount &&
                (mFd < 0 || ((theFilePtr = mOuter.GetFilePtr(mFd)) &&
                        theFilePtr->mGeneration == mGeneration));
            if (theRetryFlag) {
                const int theHttpStatus =  mHeaders.GetStatus();
                const int theTime       =
                    (500 <= theHttpStatus && theHttpStatus <= 599) ?
                    (mOuter.mExponentialBackoffStartInterval <<
                        min(mOuter.mExponentialBackoffMaxExponent,
                            mRetryCount - 1)) :
                    max(0, (int)(mStartTime + mOuter.mRetryInterval -
                        mOuter.Now()));
                KFS_LOG_STREAM_ERROR << mOuter.mLogPrefix << Show(*this) <<
                    " scheduling retry: " << mRetryCount <<
                    " http status: "      << theHttpStatus <<
                    " of " << mOuter.mMaxRetryCount <<
                    " in " << theTime << " sec." <<
                KFS_LOG_EOM;
                Reset();
                if (theTime <= 0) {
                    mTimer.RemoveTimeout();
                    mOuter.ScheduleNext(*this);
                } else {
                    mTimer.SetTimeout(theTime);
                }
            } else {
                mTimer.RemoveTimeout();
                if (0 == mSysError) {
                    mSysError = EIO;
                }
                Done();
            }
        }
    protected:
        typedef NetManager::Timer     Timer;
        typedef IOBuffer::DisplayData ShowData;

        Outer&              mOuter;
        Request*      const mRequestPtr;
        ReqType       const mReqType;
        string        const mFileName;
        Generation    const mGeneration;
        int           const mFd;
        int                 mRetryCount;
        time_t              mStartTime;
        Timer               mTimer;
        bool                mSentFlag;
        bool                mReceivedHeadersFlag;
        bool                mReadTillEofFlag;
        int                 mHeaderLength;
        QCDiskQueue::Error  mError;
        int                 mSysError;
        BlockIdx            mStartBlockIdx;
        IOBuffer            mIOBuffer;
        HttpResponseHeaders mHeaders;
        HttpChunkedDecoder  mHttpChunkedDecoder;

        virtual ~S3Req()
        {
            QCASSERT(0 < mOuter.mRequestCount);
            mOuter.mRequestCount--;
        }
        void Done(
            int64_t        inIoByteCount      = 0,
            InputIterator* inInputIteratorPtr = 0)
            { DoneSelf(inIoByteCount, inInputIteratorPtr); }
        virtual void DoneSelf(
            int64_t        inIoByteCount,
            InputIterator* inInputIteratorPtr)
        {
            if (! mRequestPtr) {
                delete this;
                return;
            }
            if (0 != mSysError && QCDiskQueue::kErrorNone == mError) {
                switch (mReqType) {
                    case QCDiskQueue::kReqTypeRead:
                        mError = QCDiskQueue::kErrorRead;
                        break;
                    case QCDiskQueue::kReqTypeWrite:
                    case QCDiskQueue::kReqTypeWriteSync:
                        mError = QCDiskQueue::kErrorWrite;
                        break;
                    case QCDiskQueue::kReqTypeDelete:
                        mError = QCDiskQueue::kErrorDelete;
                        break;
                    default:
                        mOuter.FatalError("invalid request type");
                        mError = QCDiskQueue::kErrorParameter;
                        break;
                }
            }
            QCDiskQueue::Error const theError         = mError;
            int                const theSysErr        = mSysError;
            BlockIdx           const theStartBlockIdx = mStartBlockIdx;
            Request*           const theRequestPtr    = mRequestPtr;
            Outer&                   theOuter         = mOuter;
            delete this;
            theOuter.mDiskQueuePtr->Done(
                theOuter,
                *theRequestPtr,
                theError,
                theSysErr,
                QCDiskQueue::kErrorNone == theError ? inIoByteCount      : 0,
                theStartBlockIdx,
                QCDiskQueue::kErrorNone == theError ? inInputIteratorPtr : 0
            );
        }
        int SendRequest(
            const char*           inVerbPtr,
            IOBuffer&             inBuffer,
            const ServerLocation& inServer,
            const char*           inMdPtr                      = 0,
            const char*           inContentTypePtr             = 0,
            const char*           inContentEncodingPtr         = 0,
            bool                  inServerSideEncryptionFlag   = false,
            int64_t               inContentLength              = -1,
            int64_t               inRangeStart                 = -1,
            int64_t               inRangeEnd                   = -1,
            const char*           inQueryStringPtr             = 0,
            const char*           inUriPtr                     = 0,
            const char*           inV2QueryToSignPtr           = 0,
            bool                  inEmitStorageClassHeaderFlag = false)
        {
            if (mSentFlag) {
                return 0;
            }
            mHeaders.Reset();
            if (mOuter.mRegion.empty()) {
                SendRequestAuthV2(
                    inVerbPtr,
                    inBuffer,
                    inServer,
                    inMdPtr,
                    inContentTypePtr,
                    inContentEncodingPtr,
                    inServerSideEncryptionFlag,
                    inContentLength,
                    inRangeStart,
                    inRangeEnd,
                    inQueryStringPtr,
                    inUriPtr,
                    inV2QueryToSignPtr,
                    inEmitStorageClassHeaderFlag
                );
            } else {
                SendRequestAuthV4(
                    inVerbPtr,
                    inBuffer,
                    inServer,
                    inMdPtr,
                    inContentTypePtr,
                    inContentEncodingPtr,
                    inServerSideEncryptionFlag,
                    inContentLength,
                    inRangeStart,
                    inRangeEnd,
                    inQueryStringPtr,
                    inUriPtr,
                    inEmitStorageClassHeaderFlag
                );
            }
            mOuter.mWOStream.Reset();
            mSentFlag = true;
            if (mOuter.mDebugTraceRequestHeadersFlag) {
                KFS_LOG_STREAM_DEBUG << mOuter.mLogPrefix << Show(*this) <<
                    " request header: " << ShowData(inBuffer) <<
                KFS_LOG_EOM;
            }
            return mOuter.mMaxReadAhead;
        }
        void SendRequestAuthV2(
            const char*           inVerbPtr,
            IOBuffer&             inBuffer,
            const ServerLocation& inServer,
            const char*           inMd5Ptr,
            const char*           inContentTypePtr,
            const char*           inContentEncodingPtr,
            bool                  inServerSideEncryptionFlag,
            int64_t               inContentLength,
            int64_t               inRangeStart,
            int64_t               inRangeEnd,
            const char*           inQueryStringPtr,
            const char*           inUriPtr,
            const char*           inV2QueryToSignPtr,
            bool                  inEmitStorageClassHeaderFlag)
        {
            const char* const theDatePtr = mOuter.DateNow();
            string& theSignBuf = mOuter.mTmpSignBuffer;
            theSignBuf = inVerbPtr;
            theSignBuf += '\n';
            if (inMd5Ptr && *inMd5Ptr) {
                theSignBuf += inMd5Ptr;
            }
            theSignBuf += '\n';
            if (inContentTypePtr && *inContentTypePtr) {
                theSignBuf += inContentTypePtr;
            }
            theSignBuf += '\n';
            theSignBuf += theDatePtr;
            theSignBuf += '\n';
            if (! mOuter.mSecurityToken.empty()) {
                theSignBuf += kS3AmzSecurityTokenNamePtr;
                theSignBuf += ':';
                theSignBuf += mOuter.mSecurityToken;
                theSignBuf += '\n';
            }
            if (inServerSideEncryptionFlag) {
                theSignBuf += kS3AmzServerSideEncryptionNamePtr;
                theSignBuf += ':';
                theSignBuf += kS3EncryptionTypePtr;
                theSignBuf += '\n';
            }
            if (inEmitStorageClassHeaderFlag &&
                    ! mOuter.mStorageClass.empty()) {
                theSignBuf += kS3AmzStorageClassNamePtr;
                theSignBuf += ':';
                theSignBuf += mOuter.mStorageClass;
                theSignBuf += '\n';
            }
            theSignBuf += '/';
            theSignBuf += mOuter.mBucketName;
            if (inUriPtr) {
                theSignBuf += inUriPtr;
            } else {
                theSignBuf += '/';
                theSignBuf += mFileName;
            }
            if (inV2QueryToSignPtr && *inV2QueryToSignPtr) {
                theSignBuf += '?';
                theSignBuf += inV2QueryToSignPtr;
            }
            ostream& theStream = mOuter.mWOStream.Set(inBuffer);
            theStream << inVerbPtr;
            if (inUriPtr) {
                theStream << " " << inUriPtr;
            } else {
                theStream << " /" << mFileName;
            }
            if (inQueryStringPtr && *inQueryStringPtr) {
                theStream << "?" << inQueryStringPtr;
            }
            theStream  << " HTTP/1.1\r\n"
                "Host: "  << inServer.hostname
            ;
            if ((mOuter.mHttpsFlag ? 443 : 80) != inServer.port) {
                theStream << ':' << inServer.port;
            }
            theStream <<
                "\r\n"
                "Date: "  << theDatePtr << "\r\n"
            ;
            if (inMd5Ptr && *inMd5Ptr) {
                theStream << "Content-MD5: " << inMd5Ptr << "\r\n";
            }
            if (inContentTypePtr && *inContentTypePtr) {
                theStream << "Content-Type: " << inContentTypePtr << "\r\n";
            }
            if (! mOuter.mUserAgent.empty()) {
                theStream << "User-Agent: " << mOuter.mUserAgent << "\r\n";
            }
            if (inContentEncodingPtr && *inContentEncodingPtr) {
                theStream << "Content-Encoding: " <<
                    inContentEncodingPtr << "\r\n";
            }
            if (0 <= inContentLength) {
                theStream << "Content-Length: " << inContentLength << "\r\n";
            }
            if (0 <= inRangeStart) {
                theStream << "Range: bytes=" <<
                    inRangeStart << "-" << inRangeEnd << "\r\n";
            }
            if (! mOuter.mSecurityToken.empty()) {
                theStream << kS3AmzSecurityTokenNamePtr <<
                    ": " << mOuter.mSecurityToken << "\r\n";
            }
            if (inServerSideEncryptionFlag) {
                theStream << kS3AmzServerSideEncryptionNamePtr <<
                    ": " << kS3EncryptionTypePtr << "\r\n";
            }
            if (inEmitStorageClassHeaderFlag &&
                    ! mOuter.mStorageClass.empty()) {
                theStream << kS3AmzStorageClassNamePtr <<
                    ": " << mOuter.mStorageClass << "\r\n";
            }
            if (! mOuter.mCacheControl.empty()) {
                theStream << "Cache-Control: " <<
                    mOuter.mCacheControl << "\r\n";
            }
            theStream <<
                "Authorization: AWS " << mOuter.mAccessKeyId << ":" <<
                    mOuter.Sign(theSignBuf, mOuter.mSecretAccessKey) << "\r\n"
            "\r\n";
            theStream.flush();
        }
        void SendRequestAuthV4(
            const char*           inVerbPtr,
            IOBuffer&             inBuffer,
            const ServerLocation& inServer,
            const char*           inMdPtr,
            const char*           inContentTypePtr,
            const char*           inContentEncodingPtr,
            bool                  inServerSideEncryptionFlag,
            int64_t               inContentLength,
            int64_t               inRangeStart,
            int64_t               inRangeEnd,
            const char*           inQueryStringPtr,
            const char*           inUriPtr,
            bool                  inEmitStorageClassHeaderFlag)
        {
            const char* const kEmptyShaPtr    =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
            const char* const kHostHNamePtr   = "host";
            const char* const kHostHRangePtr  = "range";
            const char* const kHostHRBytesPtr = ":bytes=";
            const char* const kAmzShaNamePtr  = "x-amz-content-sha256";
            const char* const kAmzDateNamePtr = "x-amz-date";
            const char* const theTimePtr      = mOuter.ISOTimeNow();
            const char* const theContShaPtr   =
                (inMdPtr && *inMdPtr) ? inMdPtr : kEmptyShaPtr;
            string&           theSignBuf      = mOuter.mTmpSignBuffer;
            theSignBuf = inVerbPtr;
            theSignBuf += '\n';
            // URI -- bucket and file name should not contain characters that
            // need to be escaped.
            if (inUriPtr) {
                theSignBuf += inUriPtr;
            } else {
                theSignBuf += '/';
                theSignBuf += mFileName;
            }
            theSignBuf += '\n';
            // Query string.
            if (inQueryStringPtr && *inQueryStringPtr) {
                theSignBuf += inQueryStringPtr;
            }
            theSignBuf += '\n';
            theSignBuf += kHostHNamePtr;
            theSignBuf += ':';
            theSignBuf += inServer.hostname;
            if ((mOuter.mHttpsFlag ? 443 : 80) != inServer.port) {
                theSignBuf += ':';
                AppendDecIntToString(theSignBuf, inServer.port);
            }
            theSignBuf += '\n';
            if (0 <= inRangeStart) {
                theSignBuf += kHostHRangePtr;
                theSignBuf += kHostHRBytesPtr;
                AppendDecIntToString(theSignBuf, inRangeStart);
                theSignBuf += '-';
                AppendDecIntToString(theSignBuf, inRangeEnd);
                theSignBuf += '\n';
            }
            theSignBuf += kAmzShaNamePtr;
            theSignBuf += ':';
            theSignBuf += theContShaPtr;
            theSignBuf += '\n';
            theSignBuf += kAmzDateNamePtr;
            theSignBuf += ':';
            theSignBuf += theTimePtr;
            theSignBuf += '\n';
            if (! mOuter.mSecurityToken.empty()) {
                theSignBuf += kS3AmzSecurityTokenNamePtr;
                theSignBuf += ':';
                theSignBuf += mOuter.mSecurityToken;
                theSignBuf += '\n';
            }
            if (inServerSideEncryptionFlag) {
                theSignBuf += kS3AmzServerSideEncryptionNamePtr;
                theSignBuf += ':';
                theSignBuf += kS3EncryptionTypePtr;
                theSignBuf += '\n';
            }
            if (inEmitStorageClassHeaderFlag &&
                    ! mOuter.mStorageClass.empty()) {
                theSignBuf += kS3AmzStorageClassNamePtr;
                theSignBuf += ':';
                theSignBuf += mOuter.mStorageClass;
                theSignBuf += '\n';
            }
            theSignBuf += '\n';
            // List of signed headers.
            size_t const theSHPos = theSignBuf.size();
            theSignBuf += kHostHNamePtr;
            theSignBuf += ';';
            if (0 <= inRangeStart) {
                theSignBuf += kHostHRangePtr;
                theSignBuf += ';';
            }
            theSignBuf += kAmzShaNamePtr;
            theSignBuf += ';';
            theSignBuf += kAmzDateNamePtr;
            if (! mOuter.mSecurityToken.empty()) {
                theSignBuf += ';';
                theSignBuf += kS3AmzSecurityTokenNamePtr;
            }
            if (inServerSideEncryptionFlag) {
                theSignBuf += ';';
                theSignBuf += kS3AmzServerSideEncryptionNamePtr;
            }
            if (inEmitStorageClassHeaderFlag &&
                    ! mOuter.mStorageClass.empty()) {
                theSignBuf += ';';
                theSignBuf += kS3AmzStorageClassNamePtr;
            }
            size_t const theSHLen = theSignBuf.size() - theSHPos;
            theSignBuf += '\n';
            theSignBuf += theContShaPtr;
            mOuter.Sha256Start();
            mOuter.MdAdd(theSignBuf.data(), theSignBuf.size());
            char              theShaHex[kSha256HexLen + 1];
            const char* const theShaHexPtr =
                Sha256Hex(mOuter.MdEnd(kSha256Len), theShaHex);
            size_t const theSPos = theSignBuf.size();
            theSignBuf += "AWS4-HMAC-SHA256\n";
            theSignBuf += theTimePtr;
            theSignBuf += '\n';
            size_t const theCtxPos = theSignBuf.size();
            theSignBuf.append(theTimePtr, kISODateLen);
            theSignBuf += '/';
            theSignBuf += mOuter.mRegion;
            theSignBuf += "/s3/aws4_request\n";
            size_t const theCtxLen = theSignBuf.size() - 1 - theCtxPos;
            theSignBuf += theShaHexPtr;
            Sha256Buf theShaBuf;
            const char* const theAuthSignPtr = Sha256Hex(
                mOuter.HmacSha256(mOuter.GetV4SignKey(), kSha256Len,
                    theSignBuf.data() + theSPos, theSignBuf.size() - theSPos, theShaBuf),
                theShaHex
            );
            ostream& theStream = mOuter.mWOStream.Set(inBuffer);
            theStream << inVerbPtr;
            if (inUriPtr) {
                theStream << " " << inUriPtr;
            } else {
                theStream << " /" << mFileName;
            }
            if (inQueryStringPtr && *inQueryStringPtr) {
                theStream << "?" << inQueryStringPtr;
            }
            theStream << " HTTP/1.1\r\n"
                "Host: "  << inServer.hostname
            ;
            if ((mOuter.mHttpsFlag ? 443 : 80) != inServer.port) {
                theStream << ':' << inServer.port;
            }
            theStream << "\r\n";
            if (! mOuter.mCacheControl.empty()) {
                theStream << "Cache-Control: " <<
                    mOuter.mCacheControl << "\r\n";
            }
            if (inContentEncodingPtr && *inContentEncodingPtr) {
                theStream << "Content-Encoding: " <<
                    inContentEncodingPtr << "\r\n";
            }
            if (0 <= inContentLength) {
                theStream << "Content-Length: " << inContentLength << "\r\n";
            }
            if (inContentTypePtr && *inContentTypePtr) {
                theStream << "Content-Type: " << inContentTypePtr << "\r\n";
            }
            if (0 <= inRangeStart) {
                theStream << "Range: bytes=" <<
                    inRangeStart << "-" << inRangeEnd << "\r\n";
            }
            if (! mOuter.mUserAgent.empty()) {
                theStream << "User-Agent: " << mOuter.mUserAgent << "\r\n";
            }
            theStream <<
                kAmzShaNamePtr  << ": " << theContShaPtr << "\r\n" <<
                kAmzDateNamePtr << ": " << theTimePtr    << "\r\n";
            if (! mOuter.mSecurityToken.empty()) {
                theStream << kS3AmzSecurityTokenNamePtr << ": " <<
                    mOuter.mSecurityToken << "\r\n";
            }
            if (inServerSideEncryptionFlag) {
                theStream << kS3AmzServerSideEncryptionNamePtr <<
                    ": " << kS3EncryptionTypePtr << "\r\n";
            }
            if (inEmitStorageClassHeaderFlag &&
                    ! mOuter.mStorageClass.empty()) {
                theStream << kS3AmzStorageClassNamePtr <<
                    ": " << mOuter.mStorageClass << "\r\n";
            }
            theStream <<
                "Authorization: AWS4-HMAC-SHA256 Credential=" <<
                mOuter.mAccessKeyId << "/";
                theStream.write(theSignBuf.data() + theCtxPos, theCtxLen) <<
                ",SignedHeaders=";
                theStream.write(theSignBuf.data() + theSHPos, theSHLen) <<
                ",Signature=" << theAuthSignPtr << "\r\n"
            "\r\n";
            theStream.flush();
        }
        template<typename T>
        int ParseResponse(
            IOBuffer& inBuffer,
            bool      inEofFlag,
            bool&     outDoneFlag,
            T*        outETagPtr)
        {
            if (mOuter.mDebugTraceRequestProgressFlag) {
                KFS_LOG_STREAM_DEBUG << mOuter.mLogPrefix << Show(*this) <<
                    " response buffer:"
                    " length: " << inBuffer.BytesConsumable() <<
                    " eof: "    << inEofFlag <<
                KFS_LOG_EOM;
            }
            const int kCloseConnection = -1;
            outDoneFlag = false;
            if (mHeaderLength <= 0 &&
                    ((mHeaderLength = GetHeaderLength(inBuffer)) <= 0 ||
                    mOuter.mMaxHdrLen < mHeaderLength)) {
                if (mOuter.mMaxHdrLen < inBuffer.BytesConsumable()) {
                    KFS_LOG_STREAM_ERROR << mOuter.mLogPrefix << Show(*this) <<
                        " exceeded max header length: " << mOuter.mMaxHdrLen <<
                         " / " << inBuffer.BytesConsumable() <<
                        " data: " << ShowData(
                            inBuffer, mOuter.mDebugTraceMaxErrorDataSize) <<
                        " ..." <<
                    KFS_LOG_EOM;
                    Error(-EINVAL, "exceeded max header length");
                    return kCloseConnection;
                }
                return mOuter.mMaxReadAhead;
            }
            if (! mReceivedHeadersFlag) {
                mReceivedHeadersFlag = true;
                IOBuffer::BufPos theLen = mHeaderLength;
                const char* const thePtr = inBuffer.CopyOutOrGetBufPtr(
                        mOuter.mHdrBufferPtr, theLen);
                if (theLen != mHeaderLength ||
                        ! mHeaders.Parse(thePtr, mHeaderLength) ||
                        mHeaders.IsUnsupportedEncoding() ||
                        (mHeaders.IsHttp11OrGreater() &&
                        (mHeaders.GetContentLength() < 0 &&
                        ! mHeaders.IsChunkedEconding())) ||
                        (mOuter.mMaxResponseSize <
                            mHeaders.GetContentLength())) {
                    KFS_LOG_STREAM_ERROR << mOuter.mLogPrefix << Show(*this) <<
                        " invalid response:"
                        " header length: "       << mHeaderLength <<
                        " max response length: " << mOuter.mMaxResponseSize <<
                        " header: " << ShowData(inBuffer,
                            min(mOuter.mDebugTraceMaxErrorDataSize,
                                    mHeaderLength)) <<
                    KFS_LOG_EOM;
                    Error(-EINVAL, "invalid response");
                    return kCloseConnection;
                }
                if (mOuter.mMaxResponseSize < mHeaders.GetContentLength()) {
                    KFS_LOG_STREAM_ERROR << mOuter.mLogPrefix << Show(*this) <<
                        " response size: "       <<
                            mHeaders.GetContentLength() <<
                        " exceeds limit: "       << mOuter.mMaxResponseSize <<
                        " header length: "       << mHeaderLength <<
                        " max response length: " << mOuter.mMaxResponseSize <<
                        " header: " << ShowData(inBuffer,
                            min(mOuter.mDebugTraceMaxErrorDataSize,
                                    mHeaderLength)) <<
                    KFS_LOG_EOM;
                    Error(-EINVAL, "response size exceeds limit");
                    return kCloseConnection;
                }
                if (mOuter.mDebugTraceRequestHeadersFlag) {
                    KFS_LOG_STREAM_DEBUG << mOuter.mLogPrefix << Show(*this) <<
                        " response header: " <<
                            ShowData(inBuffer, mHeaderLength) <<
                    KFS_LOG_EOM;
                }
                mReadTillEofFlag = ! mHeaders.IsChunkedEconding() &&
                    mHeaders.GetContentLength() < 0;
                if (outETagPtr) {
                    if (0 < mHeaders.GetETagLength()) {
                        outETagPtr->Copy(
                            thePtr + mHeaders.GetETagPosition(),
                            mHeaders.GetETagLength()
                        );
                    } else {
                        outETagPtr->clear();
                    }
                }
                inBuffer.Consume(mHeaderLength);
                if (! mHeaders.IsChunkedEconding()) {
                    inBuffer.MakeBuffersFull();
                }
            }
            if (mHeaders.IsChunkedEconding()) {
                const int theRet = mHttpChunkedDecoder.Parse(inBuffer);
                if (theRet < 0) {
                    const char* const theMsgPtr =
                        "chunked encoded parse failure";
                    KFS_LOG_STREAM_ERROR << mOuter.mLogPrefix << Show(*this) <<
                        " " << theMsgPtr << ":" <<
                        " discarding: " << inBuffer.BytesConsumable() <<
                        " bytes header length: " << mHeaderLength <<
                    KFS_LOG_EOM;
                    Error(-EINVAL, "chunked encoded parse failure");
                    return kCloseConnection;
                } else if (0 < theRet) {
                    if (mOuter.mMaxResponseSize + mOuter.mMaxReadAhead <
                            mIOBuffer.BytesConsumable() + theRet) {
                        const char* const theMsgPtr =
                            "exceeded max response size";
                        KFS_LOG_STREAM_ERROR <<
                            mOuter.mLogPrefix << Show(*this) <<
                            " " << theMsgPtr << ":" <<
                                mOuter.mMaxResponseSize +
                                mOuter.mMaxReadAhead <<
                            " discarding: " << inBuffer.BytesConsumable() <<
                            " bytes header length: " << mHeaderLength <<
                        KFS_LOG_EOM;
                        Error(-EINVAL, theMsgPtr);
                        return kCloseConnection;
                    }
                    KFS_LOG_STREAM_DEBUG << mOuter.mLogPrefix << Show(*this) <<
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
                    KFS_LOG_STREAM_ERROR << mOuter.mLogPrefix << Show(*this) <<
                        " " << theMsgPtr << ":" <<
                        " discarding: " << inBuffer.BytesConsumable() <<
                        " bytes header length: " << mHeaderLength <<
                    KFS_LOG_EOM;
                    Error(-EINVAL, theMsgPtr);
                    return kCloseConnection;
                }
            } else if (mReadTillEofFlag) {
                if (inEofFlag) {
                    mIOBuffer.Move(&inBuffer);
                } else {
                    if (mOuter.mMaxResponseSize < inBuffer.BytesConsumable()) {
                        const char* const theMsgPtr =
                            "exceeded max response size";
                        KFS_LOG_STREAM_ERROR <<
                            mOuter.mLogPrefix << Show(*this) <<
                            " " << theMsgPtr << ":" <<
                                mOuter.mMaxResponseSize <<
                            " discarding: " << inBuffer.BytesConsumable() <<
                            " bytes header length: " << mHeaderLength <<
                        KFS_LOG_EOM;
                        Error(-EINVAL, theMsgPtr);
                        return kCloseConnection;
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
            const int theStatus = mHeaders.GetStatus();
            KFS_LOG_STREAM(IsHttpStatusOk() ?
                     MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
                mOuter.mLogPrefix << Show(*this) <<
                " response:"
                " headers: "   << mHeaderLength <<
                " body: "      << mHeaders.GetContentLength() <<
                " buffer: "    << mIOBuffer.BytesConsumable() <<
                " status: "    << theStatus <<
                " http/1.1 "   << mHeaders.IsHttp11OrGreater() <<
                " close: "     << mHeaders.IsConnectionClose() <<
                " chunked: "   << mHeaders.IsChunkedEconding() <<
                " data: "      << ShowData(mIOBuffer, IsHttpStatusOk() ?
                        mOuter.mDebugTraceMaxDataSize :
                        mOuter.mDebugTraceMaxErrorDataSize) <<
            KFS_LOG_EOM;
            outDoneFlag = true;
            return ((mReadTillEofFlag || mHeaders.IsConnectionClose()) ?
                kCloseConnection : 0);
        }
        int ParseResponse(
            IOBuffer& inBuffer,
            bool      inEofFlag,
            bool&     outDoneFlag)
        {
            MPutPart::ETag* const kNoEtagPtr = 0;
            return ParseResponse(inBuffer, inEofFlag, outDoneFlag, kNoEtagPtr);
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
        bool IsHttpStatusOk() const
        {
            const int theStatus = mHeaders.GetStatus();
            return (200 <= theStatus && theStatus <= 299);
        }
        bool IsStatusOk()
            { return IsHttpStatusOk(); }
        void TraceProgress(
            IOBuffer&             inBuffer,
            IOBuffer&             inResponseBuffer)
        {
            if (! mOuter.mDebugTraceRequestProgressFlag) {
                return;
            }
            KFS_LOG_STREAM_DEBUG << mOuter.mLogPrefix << Show(*this) <<
                " buffers:"
                " in: "  << inResponseBuffer.BytesConsumable() <<
                " out: " << inBuffer.BytesConsumable() <<
            KFS_LOG_EOM;
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
            const string& inFileName,
            bool          inGetUploadsFlag)
            : S3Req(inOuter, &inRequest, inReqType, inFileName),
              mGetUploadsFlag(inGetUploadsFlag),
              mUploadId()
            {}
        S3Delete(
            Outer&        inOuter,
            const string& inFileName,
            const string& inUploadId)
            : S3Req(inOuter, 0, QCDiskQueue::kReqTypeNone, inFileName),
              mGetUploadsFlag(false),
              mUploadId(inUploadId)
            {}
        virtual ostream& Display(
            ostream& inStream) const
        {
            return (inStream <<
                reinterpret_cast<const void*>(this) <<
                " delete: " << mFileName <<
                " upload: " << mUploadId <<
                " get: "    << mGetUploadsFlag
            );
        }
        virtual int Request(
            IOBuffer&             inBuffer,
            IOBuffer&             inResponseBuffer,
            const ServerLocation& inServer)
        {
            TraceProgress(inBuffer, inResponseBuffer);
            if (mSentFlag) {
                return 0;
            }
            const char* const kContentMdPtr       = 0;
            const char* const kContentTypePtr     = 0;
            const char* const kContentEcondingPtr = 0;
            bool        const kUseEncryptionFlag  = false;
            int         const kContentLength      = -1;
            int64_t     const kRangeStart         = -1;
            int64_t     const kRangeEnd           = -1;
            bool              theGetUploadsFlag   = false;
            const char*       theV2QueryToSignPtr;
            // Maintain AWS authorization v4 "canonical" form of the query.
            if (! mUploadId.empty()) {
                mOuter.mTmpBuffer = "uploadId=";
                mOuter.mTmpBuffer += mUploadId;
                theV2QueryToSignPtr = mOuter.mTmpBuffer.data();
            } else if (mGetUploadsFlag) {
                mOuter.mTmpBuffer = "max-uploads=1&prefix=";
                mOuter.mTmpBuffer += mFileName;
                mOuter.mTmpBuffer += "&uploads";
                if (! mOuter.mRegion.empty()) {
                    mOuter.mTmpBuffer += '=';
                }
                theV2QueryToSignPtr = "uploads";
                theGetUploadsFlag = true;
            } else {
                mOuter.mTmpBuffer.clear();
                theV2QueryToSignPtr = 0;
            }
            return SendRequest(
                theGetUploadsFlag ? "GET" : "DELETE",
                inBuffer,
                inServer,
                kContentMdPtr,
                kContentTypePtr,
                kContentEcondingPtr,
                kUseEncryptionFlag,
                kContentLength,
                kRangeStart,
                kRangeEnd,
                mOuter.mTmpBuffer.c_str(),
                theGetUploadsFlag ? "/" : 0,
                theV2QueryToSignPtr
            );
        }
        virtual int Response(
            IOBuffer& inBuffer,
            bool      inEofFlag,
            IOBuffer& /* inOutBuffer */)
        {
            bool      theDoneFlag = false;
            const int theRet = ParseResponse(inBuffer, inEofFlag, theDoneFlag);
            if (theDoneFlag && ! HandleGetUploadsResponse(inBuffer)) {
                if (IsStatusOk() ||
                        (! mUploadId.empty() && 404 == mHeaders.GetStatus())) {
                    if (mRequestPtr &&
                            (! mUploadId.empty() || mGetUploadsFlag) &&
                            mOuter.IsRunning()) {
                        mUploadId.clear();
                        mRetryCount = 0;
                        Reset();
                        mOuter.ScheduleNext(*this);
                    } else {
                        Done();
                    }
                } else {
                    Retry();
                }
            }
            return theRet;
        }
    private:
        bool   mGetUploadsFlag;
        string mUploadId;

        bool HandleGetUploadsResponse(
            IOBuffer& inBuffer)
        {
            if (! mGetUploadsFlag || ! mUploadId.empty()) {
                return false;
            }
            if (! IsStatusOk() || ! ParseGetUploadsResponse()) {
                KFS_LOG_STREAM_ERROR <<
                    mOuter.mLogPrefix << Show(*this) <<
                    " failed to parse get uploads response:" <<
                    " at: " << mOuter.GetXmlLastParsedKey() <<
                    " response length: " << mIOBuffer.BytesConsumable() <<
                    " data: " << ShowData(mIOBuffer,
                        mOuter.mDebugTraceMaxErrorDataSize) <<
                KFS_LOG_EOM;
                Retry();
            } else if (mOuter.IsRunning()) {
                mRetryCount = 0;
                Reset();
                mOuter.ScheduleNext(*this);
            }
            return true;
        }
        class GetUploadsResponseParser
        {
        public:
            GetUploadsResponseParser(
                const string& inBucketName,
                const string& inFileName,
                string&       inUploadId)
                : mBucketName(inBucketName),
                  mFileName(inFileName),
                  mUploadId(inUploadId),
                  mGotBucketFlag(false),
                  mGotKeyFlag(false),
                  mGotIsTruncatedFlag(false),
                  mGotMaxResultsFlag(false),
                  mHasMoreFlag(false)
                {}
            bool operator()(
                const string& inKey,
                const string& inValue)
            {
                if (kS3StrGetUploadsResultBucket == inKey) {
                    mGotBucketFlag = true;
                    return (inValue == mBucketName);
                }
                if (kS3StrGetUploadsResultMaxUploads == inKey) {
                    mGotMaxResultsFlag = true;
                    return (inValue == "1");
                }
                if (kS3StrGetUploadsResultUploadKey == inKey) {
                    mGotKeyFlag = true;
                    return (inValue == mFileName);
                }
                if (kS3StrGetUploadsResultIsTruncated == inKey) {
                    mGotIsTruncatedFlag = true;
                    mHasMoreFlag = inValue == "true";
                    return (mHasMoreFlag || inValue == "false");
                }
                if (kS3StrGetUploadsResultUploadUploadId == inKey) {
                    mUploadId = XmlToUrlEncoding(
                        inValue.data(), inValue.size());
                    return ! mUploadId.empty();
                }
                return true;
            }
            bool HasMore() const
                { return mHasMoreFlag; }
            bool IsOk() const
            {
                return (
                  mGotBucketFlag &&
                  mGotIsTruncatedFlag &&
                  mGotMaxResultsFlag &&
                  (mGotKeyFlag != mUploadId.empty())
                );
            }
        private:
            const string& mBucketName;
            const string& mFileName;
            string&       mUploadId;
            bool          mGotBucketFlag:1;
            bool          mGotKeyFlag:1;
            bool          mGotIsTruncatedFlag:1;
            bool          mGotMaxResultsFlag:1;
            bool          mHasMoreFlag:1;
        };
        bool ParseGetUploadsResponse()
        {
            GetUploadsResponseParser theParser(
                mOuter.mBucketName, mFileName, mUploadId);
            if (mOuter.ParseXmlResponse(mIOBuffer, theParser) &&
                    theParser.IsOk()) {
                mGetUploadsFlag = theParser.HasMore();
                return true;
            }
            mUploadId.clear();
            return false;
        }
    private:
        S3Delete(
            const S3Delete& inDelete);
        S3Delete& operator=(
            const S3Delete& inDelete);
    };
    friend class S3Delete;
    class S3Put : public S3Req
    {
    public:
        class StClearBuffer
        {
        public:
            StClearBuffer(
                S3Put&    inReq,
                IOBuffer& inBuffer)
                : mBufferPtr(&inBuffer),
                  mReq(inReq)
            {
                if (mReq.mClearBufferPtr) {
                    mReq.mClearBufferPtr->mBufferPtr = 0;
                }
QC_GCC_13_PRAGMA(GCC diagnostic push)
QC_GCC_13_PRAGMA(GCC diagnostic ignored "-Wdangling-pointer")
                mReq.mClearBufferPtr = this;
QC_GCC_13_PRAGMA(GCC diagnostic pop)
            }
            ~StClearBuffer()
            {
                if (mBufferPtr) {
                    mReq.mClearBufferPtr = 0;
                }
            }
            void Clear()
            {
                // Cleanup request's output buffer, since the write request's
                // IO buffers aren't really reference counted, and might go away
                // when request completion is invoked.
                if (mBufferPtr) {
                    QCASSERT(this == mReq.mClearBufferPtr);
                    mReq.mClearBufferPtr = 0;
                    IOBuffer& theBuf = *mBufferPtr;
                    mBufferPtr = 0;
                    theBuf.Clear();
                }
            }
        private:
            IOBuffer* mBufferPtr;
            S3Put&    mReq;
        };
        friend class StClearBuffer;

        S3Put(
            Outer&        inOuter,
            Request&      inRequest,
            ReqType       inReqType,
            const string& inFileName,
            BlockIdx      inStartBlockIdx,
            Generation    inGeneration,
            int           inFd,
            IOBuffer&     inIOBuffer)
            : S3Req(inOuter, &inRequest, inReqType, inFileName,
                inStartBlockIdx, inGeneration, inFd),
              mDataBuf(),
              mClearBufferPtr(0),
              mIsSha256Flag(false)
        {
            mMdBuf[0] = 0;
            mDataBuf.Move(&inIOBuffer);
        }
        virtual ~S3Put()
        {
            if (mClearBufferPtr) {
                mClearBufferPtr->Clear();
            }
        }
        virtual ostream& Display(
            ostream& inStream) const
        {
            return (inStream <<
                reinterpret_cast<const void*>(this) <<
                " put: "  << mFileName <<
                " fd: "   << mFd <<
                " gen: "  << mGeneration <<
                " size: " << mDataBuf.BytesConsumable()
            );
        }
        virtual int Request(
            IOBuffer&             inBuffer,
            IOBuffer&             inResponseBuffer,
            const ServerLocation& inServer)
        {
            TraceProgress(inBuffer, inResponseBuffer);
            if (mSentFlag) {
                return 0;
            }
            int64_t     const kRangeStart                 = -1;
            int64_t     const kRangeEnd                   = -1;
            const char* const kQueryStringPtr             = 0;
            const char* const kUriPtr                     = 0;
            const char* const kV2QueryToSignPtr           = 0;
            bool        const kEmitStorageClassHeaderFlag = true;
            const int theRet = SendRequest(
                "PUT",
                inBuffer,
                inServer,
                mOuter.mRegion.empty() ? GetMd5Sum() : GetSha256(),
                mOuter.mContentType.c_str(),
                mOuter.mContentEncoding.c_str(),
                mOuter.mUseServerSideEncryptionFlag,
                mDataBuf.BytesConsumable(),
                kRangeStart,
                kRangeEnd,
                kQueryStringPtr,
                kUriPtr,
                kV2QueryToSignPtr,
                kEmitStorageClassHeaderFlag
            );
            inBuffer.Copy(&mDataBuf, mDataBuf.BytesConsumable());
            return theRet;
        }
        virtual int Response(
            IOBuffer& inBuffer,
            bool      inEofFlag,
            IOBuffer& inOutBuffer)
        {
            StClearBuffer theCleanup(*this, inOutBuffer);
            bool          theDoneFlag = false;
            const int     theRet      = ParseResponse(
                inBuffer, inEofFlag, theDoneFlag);
            if (theDoneFlag) {
                if (IsStatusOk()) {
                    File* const theFilePtr = mOuter.GetFilePtr(mFd);
                    if (theFilePtr && mGeneration == theFilePtr->mGeneration) {
                        theFilePtr->MakeReadOnly();
                    }
                    Done(mDataBuf.BytesConsumable());
                } else {
                    Retry();
                }
            }
            return theRet;
        }
        const char* GetMd5Sum()
        {
            if (*mMdBuf && ! mIsSha256Flag) {
                return mMdBuf;
            }
            mIsSha256Flag = false;
            mOuter.Md5Start();
            for (IOBuffer::iterator theIt = mDataBuf.begin();
                    theIt != mDataBuf.end();
                    ++theIt) {
                mOuter.MdAdd(theIt->Consumer(), theIt->BytesConsumable());
            }
            const int theB64Len = Base64::Encode(
                mOuter.MdEnd(kMd5Len), kMd5Len, mMdBuf);
            QCRTASSERT(
                0 < theB64Len &&
                (size_t)theB64Len < sizeof(mMdBuf) / sizeof(mMdBuf[0])
            );
            mMdBuf[theB64Len] = 0;
            return mMdBuf;
        }
        const char* GetSha256()
        {
            if (*mMdBuf && mIsSha256Flag) {
                return mMdBuf;
            }
            mIsSha256Flag = true;
            mOuter.Sha256Start();
            for (IOBuffer::iterator theIt = mDataBuf.begin();
                    theIt != mDataBuf.end();
                    ++theIt) {
                mOuter.MdAdd(theIt->Consumer(), theIt->BytesConsumable());
            }
            Sha256Hex(mOuter.MdEnd(kSha256Len), mMdBuf);
            return mMdBuf;
        }
    protected:
        IOBuffer       mDataBuf;
        StClearBuffer* mClearBufferPtr;
        bool           mIsSha256Flag;
        char           mMdBuf[1 +
            (kMd5Base64Len < kSha256HexLen ? kSha256HexLen : kMd5Base64Len)];
    private:
        S3Put(
            const S3Put& inPut);
        S3Put& operator=(
            const S3Put& inPut);
    };
    friend class S3Put;
    class S3Get : public S3Req
    {
    public:
        S3Get(
            Outer&        inOuter,
            Request&      inRequest,
            ReqType       inReqType,
            const string& inFileName,
            BlockIdx      inStartBlockIdx,
            int           inBufferCount,
            Generation    inGeneration,
            int           inFd)
            : S3Req(inOuter, &inRequest, inReqType, inFileName,
                inStartBlockIdx, inGeneration, inFd),
              mRangeStart(inStartBlockIdx * mOuter.mBlockSize),
              mRangeEnd(mRangeStart +
                max(0, inBufferCount) * mOuter.mBlockSize - 1)
            {}
        virtual ostream& Display(
            ostream& inStream) const
        {
            return (inStream <<
                reinterpret_cast<const void*>(this) <<
                " get: "   << mFileName <<
                " fd: "    << mFd <<
                " gen: "   << mGeneration <<
                " pos: "   << mRangeStart <<
                " size: "  << (mRangeEnd - mRangeStart + 1)
            );
        }
        virtual int Request(
            IOBuffer&             inBuffer,
            IOBuffer&             inResponseBuffer,
            const ServerLocation& inServer)
        {
            TraceProgress(inBuffer, inResponseBuffer);
            if (mSentFlag) {
                return 0;
            }
            const char* const kMdSumPtr                 = 0;
            const char* const kContentTypePtr           = 0;
            const char* const kContentEncondingPtr      = 0;
            bool        const kServerSideEncryptionFlag = false;
            int         const kContentLength            = -1;
            return SendRequest("GET", inBuffer, inServer,
                kMdSumPtr,
                kContentTypePtr,
                kContentEncondingPtr,
                kServerSideEncryptionFlag,
                kContentLength,
                mRangeStart,
                mRangeEnd
            );
        }
        virtual int Response(
            IOBuffer& inBuffer,
            bool      inEofFlag,
            IOBuffer& /* inOutBuffer */)
        {
            bool      theDoneFlag = false;
            const int theRet = ParseResponse(inBuffer, inEofFlag, theDoneFlag);
            if (theDoneFlag) {
                if (IsStatusOk()) {
                     // Even though the input buffer should be empty, clear it,
                     // to ensure that the last possibly partial buffer is not
                     // shared between input buffer and and IO buffer, in order
                     // to prevent buffer detach failure.
                    inBuffer.Clear();
                    mIOBuffer.Trim((int)(mRangeEnd + 1 - mRangeStart));
                    int const theIoByteCount = mIOBuffer.BytesConsumable();
                    IOBufInputIterator theIterator(mIOBuffer);
                    Done(theIoByteCount, &theIterator);
                } else {
                    Retry();
                }
            }
            return theRet;
        }
    private:
        class IOBufInputIterator : public InputIterator
        {
        public:
            IOBufInputIterator(
                IOBuffer& inIOBuffer)
                : InputIterator(),
                  mIOBuffer()
                { mIOBuffer.Move(&inIOBuffer); }
            virtual char* Get()
            {
                const bool  kFullOrPartialLastBufferFlag = true;
                char* const thePtr = mIOBuffer.DetachFrontBuffer(
                    kFullOrPartialLastBufferFlag);
                QCRTASSERT(thePtr || mIOBuffer.IsEmpty());
                return thePtr;
            }
        private:
            IOBuffer mIOBuffer;
        };
        const int64_t mRangeStart;
        const int64_t mRangeEnd;
    private:
        S3Get(
            const S3Get& inGet);
        S3Get& operator=(
            const S3Get& inGet);
    };
    friend class S3Get;
    class DoNotDeallocate
    {
    public:
        DoNotDeallocate()
            {}
        void operator()(
            char* /* inBufferPtr */)
            {}
    };
    class MPPut : public S3Put
    {
    public:
        typedef File::List List;
        MPPut(
            Outer&          inOuter,
            S3Req::Request& inRequest,
            ReqType         inReqType,
            const string&   inFileName,
            BlockIdx        inStartBlockIdx,
            Generation      inGeneration,
            int             inFd,
            IOBuffer&       inIOBuffer)
            : S3Put(
                inOuter,
                inRequest,
                inReqType,
                inFileName,
                inStartBlockIdx,
                inGeneration,
                inFd,
                inIOBuffer),
                mCommitFlag(false),
                mCommitInFlightFlag(false),
                mETag(),
                mTmpWrite()
            { List::Init(*this); }
        virtual ostream& Display(
            ostream& inStream) const
        {
            return (inStream <<
                reinterpret_cast<const void*>(this) <<
                " mput: " << mFileName <<
                " type: " << RequestTypeToName(mReqType) <<
                " fd: "   << mFd <<
                " gen: "  << mGeneration <<
                " pos: "  << mStartBlockIdx * mOuter.mBlockSize <<
                " size: " << mDataBuf.BytesConsumable()
            );
        }
        virtual int Request(
            IOBuffer&             inBuffer,
            IOBuffer&             inResponseBuffer,
            const ServerLocation& inServer)
        {
            TraceProgress(inBuffer, inResponseBuffer);
            const File* const theFilePtr = GetFilePtr();
            if (! theFilePtr) {
                return -1;
            }
            if (mSentFlag) {
                return 0;
            }
            const bool theGetIdFlag = mDataBuf.IsEmpty();
            string&    theQueryStr  = mOuter.mTmpBuffer;
            theQueryStr.clear();
            // Maintain AWS authorization v4 "canonical" form of the query.
            if (! theGetIdFlag) {
                if (theFilePtr->mUploadId.empty()) {
                    mOuter.FatalError("invocation without upload id");
                    Error(-EINVAL, "no upload id");
                    return -1;
                }
                if (! mCommitFlag) {
                    const int64_t thePartNum =
                        mStartBlockIdx * mOuter.mBlockSize / kS3MinPartSize;
                    QCASSERT(thePartNum * kS3MinPartSize ==
                        mStartBlockIdx * mOuter.mBlockSize);
                    theQueryStr = "partNumber=";
                    AppendDecIntToString(theQueryStr, thePartNum + 1);
                    theQueryStr += '&';
                }
                theQueryStr += "uploadId=";
                theQueryStr += theFilePtr->mUploadId;
            }
            if (mCommitFlag && mTmpWrite.IsEmpty()) {
                mMdBuf[0] = 0; // Invalidate to force re-computation.
                mTmpWrite.Move(&mDataBuf); // Save write buffers.
                IOBufferWriter theWriter(mDataBuf);
                theWriter.Write(kS3MPutCompleteStart);
                for (MPutParts::const_iterator
                        theIt = theFilePtr->mMPutParts.begin();
                        theFilePtr->mMPutParts.end() != theIt;
                        ++theIt) {
                    // Write commit request.
                    theWriter.Write(kS3MPutCompletePartStart);
                    theWriter.Write(kS3MPutCompletePartNumberStart);
                    ConvertInt<int64_t, 10> const thePartNum(
                        theIt->mStart * mOuter.mBlockSize / kS3MinPartSize + 1);
                    theWriter.Write(thePartNum);
                    theWriter.Write(kS3MPutCompletePartNumberEnd);
                    theWriter.Write(kS3MPutCompleteETagStart);
                    theWriter.Write(theIt->mETag);
                    theWriter.Write(kS3MPutCompleteETagEnd);
                    theWriter.Write(kS3MPutCompletePartEnd);
                }
                theWriter.Write(kS3MPutCompleteEnd);
                theWriter.Close();
            }
            const int64_t     kRangeStart = -1;
            const int64_t     kRangeEnd   = -1;
            const char* const kUriPtr     = 0;
            const int theRet = SendRequest(
                (mCommitFlag || theGetIdFlag) ? "POST" : "PUT",
                inBuffer,
                inServer,
                theGetIdFlag ? 0 :
                    (mOuter.mRegion.empty() ? GetMd5Sum() : GetSha256()),
                theGetIdFlag ? mOuter.mContentType.c_str()     : 0,
                theGetIdFlag ? mOuter.mContentEncoding.c_str() : 0,
                theGetIdFlag && mOuter.mUseServerSideEncryptionFlag,
                theGetIdFlag ? -1 : mDataBuf.BytesConsumable(),
                kRangeStart,
                kRangeEnd,
                theGetIdFlag ?
                    (mOuter.mRegion.empty() ? "uploads" : "uploads=") :
                    theQueryStr.c_str(),
                kUriPtr,
                theGetIdFlag ? "uploads"  : theQueryStr.c_str(),
                theGetIdFlag
            );
            if (! theGetIdFlag) {
                inBuffer.Copy(&mDataBuf, mDataBuf.BytesConsumable());
            }
            return theRet;
        }
        virtual int Response(
            IOBuffer& inBuffer,
            bool      inEofFlag,
            IOBuffer& inOutBuffer)
        {
            StClearBuffer theCleanup(*this, inOutBuffer);
            File* const   theFilePtr = GetFilePtr();
            if (! theFilePtr) {
                return -1;
            }
            bool      theDoneFlag = false;
            const int theRet      = ParseResponse(
                inBuffer, inEofFlag, theDoneFlag, &mETag);
            if (theDoneFlag) {
                if (IsStatusOk()) {
                    if (mDataBuf.IsEmpty()) {
                        theFilePtr->mUploadId = ParseUploadId();
                        if (theFilePtr->mUploadId.empty()) {
                            KFS_LOG_STREAM_ERROR <<
                                mOuter.mLogPrefix << Show(*this) <<
                                " failed to parse upload id:"
                                " at: " << mOuter.GetXmlLastParsedKey() <<
                                " response length: " <<
                                    mIOBuffer.BytesConsumable() <<
                                " data: " << ShowData(mIOBuffer,
                                    mOuter.mDebugTraceMaxErrorDataSize) <<
                            KFS_LOG_EOM;
                            Retry();
                        } else {
                            Done();
                        }
                    } else {
                        if (mCommitFlag) {
                            if (! ParseCommitResponse()) {
                                KFS_LOG_STREAM_ERROR <<
                                    mOuter.mLogPrefix << Show(*this) <<
                                    " commit error:"
                                    " at: " << mOuter.GetXmlLastParsedKey() <<
                                    " response length: " <<
                                        mIOBuffer.BytesConsumable() <<
                                    " data: " << ShowData(mIOBuffer,
                                        mOuter.mDebugTraceMaxErrorDataSize) <<
                                KFS_LOG_EOM;
                                Retry();
                                return theRet;
                            }
                        } else {
                            MPutParts::iterator const theIt = lower_bound(
                                theFilePtr->mMPutParts.begin(),
                                theFilePtr->mMPutParts.end(),
                                mStartBlockIdx
                            );
                            if (theIt == theFilePtr->mMPutParts.end() ||
                                    theIt->mStart != mStartBlockIdx ||
                                    (! theIt->mETag.empty())) {
                                mOuter.FatalError(
                                    "invalid multipart put completion");
                            } else {
                                XmlEncode(
                                    mETag.data(), mETag.size(), theIt->mETag);
                            }
                        }
                        Done();
                    }
                } else {
                    Retry();
                }
            }
            return theRet;
        }
        void Cancel(
            File& inFile)
        {
            List::Remove(inFile.mPendingListPtr, *this);
            if (mCommitFlag && ! mCommitInFlightFlag) {
                // Commit is not in flight (or retry state) -- schedule it to
                // fail after file close completion. All other pending list
                // entries must be in flight, and therefore it is sufficient to
                // just remove them from list, as request or reply handler
                // will discover file close and fail these.
                mCommitInFlightFlag = true;
                if (mOuter.IsRunning()) {
                    mOuter.ScheduleNext(*this);
                } else {
                    // Fail synchronously.
                    Error(-EIO, "file close in shutdown");
                }
            }
        }
    private:
        bool           mCommitFlag;
        bool           mCommitInFlightFlag;
        MPutPart::ETag mETag;
        IOBuffer       mTmpWrite;
        MPPut*         mPrevPtr[1];
        MPPut*         mNextPtr[1];
        friend class QCDLListOp<MPPut>;

        File* GetFilePtr(
            bool inInvokeErrorHandlerFlag = true)
        {
            File*      theFilePtr;
            bool const theOkFlag = mOuter.IsRunning() &&
                (theFilePtr = mOuter.GetFilePtr(mFd)) &&
                theFilePtr->mGeneration == mGeneration;
            if (inInvokeErrorHandlerFlag && ! theOkFlag) {
                Error(-EINVAL, "file closed");
            }
            return (theOkFlag ? theFilePtr : 0);
        }
        virtual void DoneSelf(
            int64_t        inIoByteCount,
            InputIterator* inInputIteratorPtr)
        {
            const bool  kInvokeErrorHandlerFlag = false;
            File* const theFilePtr   = GetFilePtr(kInvokeErrorHandlerFlag);
            const bool  theGetIdFlag = mDataBuf.IsEmpty();
            if (theFilePtr) {
                if (theFilePtr->mErrorFlag != (0 != mSysError)) {
                    if (0 != mSysError) {
                        theFilePtr->mErrorFlag = true;
                    } else {
                        mSysError = EIO;
                    }
                }
                List::Remove(theFilePtr->mPendingListPtr, *this);
                if (theGetIdFlag) {
                    if (theFilePtr->mUploadId.empty() || ! mOuter.IsRunning()) {
                        theFilePtr->mErrorFlag = true;
                        MPPut* thePtr;
                        while((thePtr = List::Back(
                                theFilePtr->mPendingListPtr))) {
                            QCASSERT(! thePtr->mDataBuf.IsEmpty());
                            thePtr->mSysError = EIO;
                            thePtr->Done();
                        }
                    } else {
                        List::Iterator theIt(theFilePtr->mPendingListPtr);
                        MPPut*         thePtr;
                        while((thePtr = theIt.Next())) {
                            QCASSERT(! thePtr->mDataBuf.IsEmpty());
                            mOuter.ScheduleNext(*thePtr);
                        }
                    }
                } else if (theFilePtr->mCommitFlag && ! mCommitFlag &&
                        ! theFilePtr->mErrorFlag &&
                        QCDiskQueue::kReqTypeWriteSync == mReqType) {
                    const bool theWaitFlag =
                        ! List::IsEmpty(theFilePtr->mPendingListPtr);
                    if ((theWaitFlag && List::Front(
                                theFilePtr->mPendingListPtr)->mCommitFlag) ||
                                mCommitInFlightFlag) {
                        mOuter.FatalError(
                            "invalid multipart part upload completion");
                    }
                    // Always report upload commit result on the write sync.
                    // Write sync must be the last request to report completion,
                    // while non sync write completions can be reported out of
                    // [submit] order.
                    File::List::PushFront(theFilePtr->mPendingListPtr, *this);
                    mCommitFlag = true;
                    mRetryCount = 0;
                    Reset();
                    if (theWaitFlag) {
                        // Always reorder (even in the case of shutdown) to be
                        // the last reported completion.
                        return;
                    }
                }
            }
            if (theGetIdFlag) {
                delete this;
                return;
            }
            if (theFilePtr) {
                MPPut* const theFrontPtr =
                    List::Front(theFilePtr->mPendingListPtr);
                if (mCommitInFlightFlag) {
                    if (! mCommitFlag ||
                            QCDiskQueue::kReqTypeWriteSync != mReqType ||
                            theFrontPtr) {
                        mOuter.FatalError(
                            "invalid multipart commit completion");
                    }
                    // Commit completion.
                    mDataBuf.Clear();
                    mDataBuf.Move(&mTmpWrite);
                    if (! theFilePtr->mErrorFlag) {
                        theFilePtr->MakeReadOnly();
                    }
                } else if ((theFilePtr->mCommitFlag ||
                            theFilePtr->mErrorFlag) &&
                        ! theFilePtr->mUploadId.empty() &&
                        theFrontPtr && theFrontPtr->mCommitFlag &&
                        List::Back(theFilePtr->mPendingListPtr) ==
                            theFrontPtr) {
                    const bool theDoneFlag = this == theFrontPtr;
                    if (theFilePtr->mErrorFlag) {
                        if (mOuter.IsRunning()) {
                            // Abort: delete all upload parts.
                            mOuter.ScheduleNext(*(new S3Delete(
                                mOuter, mFileName, theFilePtr->mUploadId)));
                            theFilePtr->mUploadId = kS3EmptyString;
                        }
                        theFrontPtr->Done();
                    } else {
                        if (mOuter.IsRunning()) {
                            theFrontPtr->mCommitInFlightFlag = true;
                            mOuter.ScheduleNext(*theFrontPtr);
                        } else {
                            mSysError = EIO;
                            theFrontPtr->Done();
                        }
                    }
                    if (theDoneFlag) {
                        return;
                    }
                }
            } else if (0 == mSysError) {
                mSysError = EIO;
            }
            S3Req::DoneSelf(mDataBuf.BytesConsumable(), 0);
        }
        class UploadIdParser
        {
        public:
            UploadIdParser(
                const string& inBucketName,
                const string& inFileName,
                string&       inUploadId)
                : mBucketName(inBucketName),
                  mFileName(inFileName),
                  mUploadId(inUploadId),
                  mGotBucketFlag(false),
                  mGotKeyFlag(false)
                {}
            bool operator()(
                const string& inKey,
                const string& inValue)
            {
                if (kS3StrMPutInitResultResultBucket == inKey) {
                    mGotBucketFlag = true;
                    return (inValue == mBucketName);
                }
                if (kS3StrMPutInitResultResultKey == inKey) {
                    mGotKeyFlag = true;
                    return (inValue == mFileName);
                }
                if (kS3StrMPutInitResultResulUploadId == inKey) {
                    mUploadId = XmlToUrlEncoding(
                        inValue.data(), inValue.size());
                    return ! mUploadId.empty();
                }
                return true;
            }
            bool IsError() const
            {
                return (! mGotBucketFlag || ! mGotKeyFlag || mUploadId.empty());
            }
        private:
            const string& mBucketName;
            const string& mFileName;
            string&       mUploadId;
            bool          mGotBucketFlag:1;
            bool          mGotKeyFlag:1;
        };
        string ParseUploadId() const
        {
            string         theId;
            UploadIdParser theParser(mOuter.mBucketName, mFileName, theId);
            if (! mOuter.ParseXmlResponse(mIOBuffer, theParser) ||
                    theParser.IsError()) {
                return kS3EmptyString;
            }
            return theId;
        }
        class CommitResponseParser
        {
        public:
            CommitResponseParser(
                const string& inBucketName,
                const string& inFileName)
                : mBucketName(inBucketName),
                  mFileName(inFileName),
                  mGotBucketFlag(false),
                  mGotKeyFlag(false),
                  mGotETagFlag(false)
                {}
            bool operator()(
                const string& inKey,
                const string& inValue)
            {
                if (kS3StrMPutCompleteResultBucket == inKey) {
                    mGotBucketFlag = true;
                    return (inValue == mBucketName);
                }
                if (kS3StrMPutCompleteResultKey == inKey) {
                    mGotKeyFlag = true;
                    return (inValue == mFileName);
                }
                if (kS3StrMPutCompleteResultETag == inKey) {
                    mGotETagFlag = true;
                    return (! inValue.empty());
                }
                return true;
            }
            bool IsOk() const
                { return ( mGotBucketFlag && mGotKeyFlag && mGotETagFlag); }
        private:
            const string& mBucketName;
            const string& mFileName;
            bool          mGotBucketFlag:1;
            bool          mGotKeyFlag:1;
            bool          mGotETagFlag:1;
        };
        bool ParseCommitResponse() const
        {
            CommitResponseParser theParser(mOuter.mBucketName, mFileName);
            return (
                mOuter.ParseXmlResponse(mIOBuffer, theParser) &&
                theParser.IsOk()
            );
        }
    };

    typedef vector<TransactionalClient::Transaction*> RunList;

    QCDiskQueue*        mDiskQueuePtr;
    int64_t             mMaxFileSize;
    int                 mBlockSize;
    string const        mConfigPrefix;
    string const        mFilePrefix;
    string              mLogPrefix;
    NetManager          mNetManager;
    TransactionalClient mClient;
    FileTable           mFileTable;
    int64_t             mFreeFdListIdx;
    Generation          mGeneration;
    int                 mRequestCount;
    bool                mParametersUpdatedFlag;
    Properties          mParameters;
    Properties          mUpdatedParameters;
    RunList             mRunList;
    RunList             mCurRunList;
    string              mFullConfigPrefix;
    string              mUpdatedFullConfigPrefix;
    string              mS3HostName;
    string              mBucketName;
    string              mAccessKeyId;
    string              mSecretAccessKey;
    string              mSecurityToken;
    string              mRegion;
    string              mContentType;
    string              mCacheControl;
    string              mContentEncoding;
    string              mStorageClass;
    string              mUserAgent;
    bool                mUseServerSideEncryptionFlag;
    bool                mDebugTraceRequestHeadersFlag;
    bool                mDebugTraceRequestProgressFlag;
    bool                mHttpsFlag;
    bool                mDeleteNoUploadListFlag;
    int                 mDebugTraceMaxDataSize;
    int                 mDebugTraceMaxErrorDataSize;
    int                 mDebugTraceMaxHeaderSize;
    int                 mMaxRetryCount;
    int                 mRetryInterval;
    int                 mExponentialBackoffStartInterval;
    int                 mExponentialBackoffMaxExponent;
    int                 mMaxReadAhead;
    int                 mMaxHdrLen;
    char*               mHdrBufferPtr;
    int                 mMaxResponseSize;
    IOBuffer::WOStream  mWOStream;
    string              mTmpSignBuffer;
    string              mTmpBuffer;
    string              mTmpSignKeyBuffer;
    time_t              mLastDateTime;
    time_t              mLastDateZTime;
    time_t              mTmLastDateTime;
    QCMutex             mMutex;
    struct tm           mTmBuf;
#if OPENSSL_VERSION_NUMBER < 0x10100000L
    HMAC_CTX            mHmacCtx;
    EVP_MD_CTX          mMdCtx;
#else
#if OPENSSL_VERSION_NUMBER < 0x30000000L
    HMAC_CTX&           mHmacCtx;
#else
    EVP_MAC_CTX&        mHmacCtx;
#endif
    EVP_MD_CTX&         mMdCtx;
#endif
    char                mDateBuf[kMaxDateTimeLen];
    char                mISOTime[kMaxDateTimeLen];
    Sha256Buf           mV4SignKey;
    char                mTmpMdBuf[kMaxMdLen];
    char                mV4SignDate[kV4SignDateLen];
    char                mSignBuf[kSha256HexLen + 1];
#if 0x30000000L <= OPENSSL_VERSION_NUMBER
    char                mHmacSha1Name[8];
    char                mHmacSha256Name[8];
    OSSL_PARAM          mHmacSha1Params[2];
    OSSL_PARAM          mHmacSha256Params[2];
#endif

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
    template<typename T>
    bool ParseXmlResponse(
        const IOBuffer& inBuffer,
        T&              inTarget)
    {
        const size_t kMaxKeySize   = 4 << 10;
        const size_t kMaxValueSize = 4 << 10;
        XmlScanner::KeyValueFunc<string, T> theScanner(
            inTarget, mTmpBuffer, mTmpSignBuffer, kMaxKeySize, kMaxValueSize);
        IOBufferInputIterator               theIt(inBuffer);
        return theScanner.Scan(theIt);
    }
    const string& GetXmlLastParsedKey()
        { return mTmpBuffer; }
#if 0x30000000L <= OPENSSL_VERSION_NUMBER
    static EVP_MAC_CTX& MakeHmacCtx()
    {
        EVP_MAC* const theMac = EVP_MAC_fetch(NULL, "HMAC", NULL);
        QCRTASSERT(theMac);
        EVP_MAC_CTX&   theCtx = *EVP_MAC_CTX_new(theMac);
        EVP_MAC_free(theMac);
        return theCtx;
    }
    static void InitHmacParams(
        const char* inName,
        char*       inNameBuf,
        size_t      inNameBufLen,
        OSSL_PARAM* inParams)
    {
        QCRTASSERT(strlen(inName) < inNameBufLen);
        strncpy(inNameBuf, inName, inNameBufLen);
        inParams[0] = OSSL_PARAM_construct_utf8_string("digest", inNameBuf, 0);
        inParams[1] = OSSL_PARAM_construct_end();
    }
#endif
    S3ION(
        const char* inUrlPtr,
        const char* inConfigPrefixPtr,
        const char* inLogPrefixPtr)
        : IOMethod(true), // Do not allocate read buffers.
          mDiskQueuePtr(0),
          mMaxFileSize(0),
          mBlockSize(0),
          mConfigPrefix(inConfigPrefixPtr ? inConfigPrefixPtr : ""),
          mFilePrefix(inUrlPtr ? inUrlPtr : ""),
          mLogPrefix(inLogPrefixPtr ? inLogPrefixPtr : ""),
          mNetManager(),
          mClient(mNetManager),
          mFileTable(),
          mFreeFdListIdx(-1),
          mGeneration(1),
          mRequestCount(0),
          mParametersUpdatedFlag(false),
          mParameters(),
          mUpdatedParameters(),
          mRunList(),
          mCurRunList(),
          mFullConfigPrefix(),
          mUpdatedFullConfigPrefix(),
          mS3HostName("s3.amazonaws.com"),
          mBucketName(),
          mAccessKeyId(),
          mSecretAccessKey(),
          mSecurityToken(),
          mRegion(),
          mContentType(),
          mCacheControl(),
          mContentEncoding(),
          mStorageClass(),
          mUserAgent("QFS"),
          mUseServerSideEncryptionFlag(false),
          mDebugTraceRequestHeadersFlag(false),
          mDebugTraceRequestProgressFlag(false),
          mHttpsFlag(false),
          mDeleteNoUploadListFlag(false),
          mDebugTraceMaxDataSize(256),
          mDebugTraceMaxErrorDataSize(512),
          mDebugTraceMaxHeaderSize(512),
          mMaxRetryCount(10),
          mRetryInterval(10),
          mExponentialBackoffStartInterval(3),
          mExponentialBackoffMaxExponent(5),
          mMaxReadAhead(4 << 10),
          mMaxHdrLen(16 << 10),
          mHdrBufferPtr(new char[mMaxHdrLen + 1]),
          mMaxResponseSize((16 << 10) + (64 << 20)),
          mWOStream(),
          mTmpSignBuffer(),
          mTmpBuffer(),
          mTmpSignKeyBuffer(),
          mLastDateTime(0),
          mLastDateZTime(0),
          mTmLastDateTime(0),
          mMutex()
#if 0x10100000L <= OPENSSL_VERSION_NUMBER
          ,
#if OPENSSL_VERSION_NUMBER < 0x30000000L
          mHmacCtx(*HMAC_CTX_new()),
#else
          mHmacCtx(MakeHmacCtx()),
#endif
          mMdCtx(*EVP_MD_CTX_new())
#endif
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
        // Reserve first slot, for fds start from 1
        mFileTable.push_back(File());
        mTmpSignBuffer.reserve(1 << 10);
        mTmpBuffer.reserve(1 << 10);
        mTmpSignKeyBuffer.reserve(1 << 9);
        mRunList.reserve(128);
        mCurRunList.reserve(128);
        mTmBuf.tm_mday = -1;
        mDateBuf[0] = 0;
        mSignBuf[0] = 0;
        memset(mV4SignDate, 0, sizeof(mV4SignDate));
#if OPENSSL_VERSION_NUMBER < 0x10100000L
        HMAC_CTX_init(&mHmacCtx);
        EVP_MD_CTX_init(&mMdCtx);
#elif 0x30000000L <= OPENSSL_VERSION_NUMBER
        InitHmacParams(
            "SHA1",
            mHmacSha1Name,
            sizeof(mHmacSha1Name) / sizeof(mHmacSha1Name[0]),
            mHmacSha1Params);
        InitHmacParams(
            "SHA256",
            mHmacSha256Name,
            sizeof(mHmacSha256Name) / sizeof(mHmacSha256Name[0]),
            mHmacSha256Params);
#endif
        const int theMaxCacheSize            = 8 << 10;
        const int theResolverCacheExpiration = 1;
        mNetManager.SetResolverParameters(mNetManager.GetResolverOsFlag(),
            theMaxCacheSize, theResolverCacheExpiration);
    }
    void SetParameters()
    {
        // No backward compatibility.
        // RenameParameter("verifyPeer", "ssl.verifyPeer");
        // RenameParameter("sslCiphers", "ssl.cipher");
        // RenameParameter("CABundle",   "ssl.CAFile");
        // RenameParameter("CAPath",     "ssl.CADir");

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
        mRegion = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("region"),
            mRegion
        );
        mContentType = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("contentType"),
            mContentType
        );
        mCacheControl = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("cacheControl"),
            mCacheControl
        );
        mContentEncoding = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("contentEncoding"),
            mContentEncoding
        );
        mUserAgent = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("userAgent"),
            mUserAgent
        );
        mUseServerSideEncryptionFlag = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("useServerSideEncryption"),
            mUseServerSideEncryptionFlag ? 1 : 0
        ) != 0;
        string const theStorageClass = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("storageClass"),
            mStorageClass
        );
        const char* const* thePtr;
        for (thePtr = kS3ValidStorageClasses;
                *thePtr && theStorageClass != *thePtr;
                ++thePtr)
            {}
        if (*thePtr) {
            mStorageClass = theStorageClass;
        } else {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "ignoring invalid parameter:"
                " "        << theName         <<
                " value: " << theStorageClass <<
            KFS_LOG_EOM;
        }
        mMaxRetryCount = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("maxRetryCount"),
            mMaxRetryCount
        );
        mRetryInterval = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("retryInterval"),
            mRetryInterval
        );
        mExponentialBackoffStartInterval = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append(
                "exponentialBackoffStartInterval"),
            mExponentialBackoffStartInterval
        );
        mExponentialBackoffMaxExponent = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append(
                "exponentialBackoffMaxExponent"),
            mExponentialBackoffMaxExponent
        );
        mDebugTraceRequestHeadersFlag = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("debugTrace.requestHeaders"),
            mDebugTraceRequestHeadersFlag ? 1 : 0
        ) != 0;
        mDebugTraceRequestProgressFlag = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("debugTrace.requestProgress"),
            mDebugTraceRequestProgressFlag ? 1 : 0
        ) != 0;
        mDebugTraceMaxDataSize = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("debugTrace.maxDataSize"),
            mDebugTraceMaxDataSize
        );
        mDebugTraceMaxErrorDataSize = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("debugTrace.maxErrorDataSize"),
            mDebugTraceMaxErrorDataSize
        );
        mDebugTraceMaxHeaderSize = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("debugTrace.maxDataSize"),
            mDebugTraceMaxHeaderSize
        );
        mMaxResponseSize = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("maxResponseSize"),
            mMaxResponseSize
        );
        mDeleteNoUploadListFlag = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("deleteNoUploadList"),
            mDeleteNoUploadListFlag ? 1 : 0
        ) != 0;
        const int theMaxHdrLen = min(256 << 10, max(4 << 10,
        mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("maxHttpHeaderSize"),
            mMaxHdrLen
        )));
        const bool theUseOsResolverFlag = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("useOsResolver"),
            mNetManager.GetResolverOsFlag() ? 1 : 0) != 0;
        const int theMaxCacheSize = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("resolverMaxCacheSize"),
            mNetManager.GetResolverCacheSize());
        const int theResolverCacheExpiration = mParameters.getValue(
            theName.Truncate(thePrefixSize).Append("resolverCacheExpiration"),
            mNetManager.GetResolverCacheExpiration());
        mNetManager.SetResolverParameters(
            theUseOsResolverFlag, theMaxCacheSize, theResolverCacheExpiration);
        if (theMaxHdrLen != mMaxHdrLen) {
            mMaxHdrLen = theMaxHdrLen;
            delete [] mHdrBufferPtr;
            mHdrBufferPtr = new char[mMaxHdrLen];
        }
        mV4SignDate[0] = 0; // Force version 4 sign key update.
        if (! IsRunning()) {
            mClient.SetServer(ServerLocation(), true);
            return;
        }
        mHttpsFlag = mParameters.hasPrefix(
            theName.Truncate(thePrefixSize).Append("ssl."));
        if (! mParameters.getValue(
                theName.Truncate(thePrefixSize).Append("host"))) {
            mClient.SetServer(
                ServerLocation(mBucketName + "." + mS3HostName,
                    mHttpsFlag ? 443 : 80),
                mHttpsFlag
            );
        }
        string    theErrMsg;
        int const theStatus = mClient.SetParameters(
            mFullConfigPrefix.c_str(), mParameters, &theErrMsg);
        if (0 != theStatus) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "prefix:"
                " file: "   << mFilePrefix <<
                " config: " << mConfigPrefix <<
                " set parameters failure:" <<
                " status: " << theStatus <<
                " "         << theErrMsg <<
            KFS_LOG_EOM;
        } else {
            KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                "prefix:"
                " file: "   << mFilePrefix <<
                " config: " << mConfigPrefix <<
                " bucket: " << mBucketName <<
                " https: "  << mHttpsFlag <<
            KFS_LOG_EOM;
        }
    }
    bool IsRunning() const
    {
        return (mNetManager.IsRunning() &&
            ! mBucketName.empty() &&
            ! mSecretAccessKey.empty()
        );
    }
    void ScheduleNext(
        TransactionalClient::Transaction& inReq)
    {
        if (! IsRunning()) {
            FatalError("invalid schedule next invocation");
            return;
        }
        mRunList.push_back(&inReq);
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
    const struct tm& GetTmNow()
    {
        const time_t theNow = Now();
        if (theNow == mTmLastDateTime && 0 < mTmBuf.tm_mday) {
            return mTmBuf;
        }
        struct tm* const theTmPtr = gmtime_r(&theNow, &mTmBuf);
        if (! theTmPtr || theTmPtr->tm_wday < 0 || 6 < theTmPtr->tm_wday ||
                theTmPtr->tm_mday < 1 || 31 < theTmPtr->tm_mday ||
                theTmPtr->tm_mon < 0 || 11 < theTmPtr->tm_mon ||
                theTmPtr->tm_year + 1900 < 0 || 8099 < theTmPtr->tm_year) {
            FatalError("gmtime_r failure");
            mTmBuf.tm_mday = -1;
        }
        if (theTmPtr != &mTmBuf) {
            memcpy(&mTmBuf, theTmPtr, sizeof(mTmBuf));
        }
        mTmLastDateTime = theNow;
        return mTmBuf;
    }
    const char* DateNow()
    {
        const time_t theNow = Now();
        if (theNow == mLastDateTime && 0 != mDateBuf[0]) {
            return mDateBuf;
        }
        mLastDateTime = theNow;
        // Do not use strftime() to avoid local complications.
        const struct tm& theTm = GetTmNow();
        char* thePtr = mDateBuf;
        memcpy(thePtr, kS3IODaateWeekDays[theTm.tm_wday], 3);
        thePtr += 3;
        *thePtr++ = ',';
        *thePtr++ = ' ';
        *thePtr++ = (char)('0' + theTm.tm_mday / 10);
        *thePtr++ = (char)('0' + theTm.tm_mday % 10);
        *thePtr++ = ' ';
        memcpy(thePtr, kS3IODateMonths[theTm.tm_mon], 3);
        thePtr += 3;
        *thePtr++ = ' ';
        int theYear = theTm.tm_year + 1900;
        for (int i = 3; 0 <= i; i--) {
            thePtr[i] = (char)('0' + theYear % 10);
            theYear /= 10;
        }
        thePtr += 4;
        *thePtr++ = ' ';
        *thePtr++ = (char)('0' + theTm.tm_hour / 10);
        *thePtr++ = (char)('0' + theTm.tm_hour % 10);
        *thePtr++ = ':';
        *thePtr++ = (char)('0' + theTm.tm_min / 10);
        *thePtr++ = (char)('0' + theTm.tm_min % 10);
        *thePtr++ = ':';
        *thePtr++ = (char)('0' + theTm.tm_sec / 10);
        *thePtr++ = (char)('0' + theTm.tm_sec % 10);
        memcpy(thePtr, " GMT", 5);
        QCASSERT(thePtr + 5 <= mDateBuf +
            sizeof(mDateBuf) / sizeof(mDateBuf[0]));
        return mDateBuf;
    }
    const char* ISOTimeNow()
    {
        const time_t theNow = Now();
        if (theNow == mLastDateZTime && 0 != mISOTime[0]) {
            return mISOTime;
        }
        mLastDateZTime = theNow;
        // Do not use strftime() to avoid local complications.
        const struct tm& theTm   = GetTmNow();
        char*            thePtr  = mISOTime;
        int              theYear = theTm.tm_year + 1900;
        for (int i = 3; 0 <= i; i--) {
            thePtr[i] = (char)('0' + theYear % 10);
            theYear /= 10;
        }
        thePtr += 4;
        const int theMon = theTm.tm_mon + 1;
        *thePtr++ = (char)('0' + theMon / 10);
        *thePtr++ = (char)('0' + theMon % 10);
        *thePtr++ = (char)('0' + theTm.tm_mday / 10);
        *thePtr++ = (char)('0' + theTm.tm_mday % 10);
        *thePtr++ = 'T';
        *thePtr++ = (char)('0' + theTm.tm_hour / 10);
        *thePtr++ = (char)('0' + theTm.tm_hour % 10);
        *thePtr++ = (char)('0' + theTm.tm_min / 10);
        *thePtr++ = (char)('0' + theTm.tm_min % 10);
        *thePtr++ = (char)('0' + theTm.tm_sec / 10);
        *thePtr++ = (char)('0' + theTm.tm_sec % 10);
        *thePtr++ = (char)'Z';
        *thePtr   = 0;
        QCASSERT(thePtr < mISOTime +
            sizeof(mISOTime) / sizeof(mISOTime[0]));
        return mISOTime;
    }
    void MdStart(
        bool inSha256Flag)
    {
        if (! EVP_DigestInit_ex(&mMdCtx,
                inSha256Flag ? EVP_sha256() : EVP_md5(), 0)) {
            FatalError("EVP_DigestInit_ex failure");
        }
    }
    void Md5Start()
        { MdStart(false); }
    void Sha256Start()
        { MdStart(true); }
    void MdAdd(
        const void* inPtr,
        size_t      inSize)
    {
        if (! EVP_DigestUpdate(&mMdCtx, inPtr, inSize)) {
            FatalError("EVP_DigestUpdate failure");
        }
    }
    const char* MdEnd(
        int inLen)
    {
        unsigned int theLen = 0;
        if (! EVP_DigestFinal_ex(&mMdCtx,
                    reinterpret_cast<unsigned char*>(mTmpMdBuf),
                    &theLen) ||
                theLen <= 0 || kMaxMdLen < theLen ||
                    (unsigned int)inLen != theLen) {
            FatalError("EVP_DigestFinal_ex failure");
        }
        return mTmpMdBuf;
    }
    const char* Hmac(
        const void*   inKeyPtr,
        size_t        inKeyLen,
        const void*   inDataPtr,
        size_t        inDataLen,
        char*         inResultPtr,
        bool          inSha256Flag)
    {
#if OPENSSL_VERSION_NUMBER < 0x30000000L
        unsigned int
#else
        size_t
#endif
        theLen = 0;
#if OPENSSL_VERSION_NUMBER < 0x1000000fL
        HMAC_Init_ex(&mHmacCtx, inKeyPtr, (int)inKeyLen,
            inSha256Flag ? EVP_sha256() : EVP_sha1(), 0);
        HMAC_Update(
            &mHmacCtx,
            reinterpret_cast<const unsigned char*>(inDataPtr),
            (int)inDataLen
        );
        HMAC_Final(
            &mHmacCtx,
            reinterpret_cast<unsigned char*>(inResultPtr),
            &theLen
        );
#else
#if OPENSSL_VERSION_NUMBER < 0x30000000L
        const bool theOkFlag =
            HMAC_Init_ex(&mHmacCtx, inKeyPtr, (int)inKeyLen,
                inSha256Flag ? EVP_sha256() : EVP_sha1(), 0) &&
            HMAC_Update(
                &mHmacCtx,
                reinterpret_cast<const unsigned char*>(inDataPtr),
                (int)inDataLen
            ) &&
            HMAC_Final(
                &mHmacCtx,
                reinterpret_cast<unsigned char*>(inResultPtr),
                &theLen
            );
#else
        const bool theOkFlag =
            EVP_MAC_init(
                &mHmacCtx,
                reinterpret_cast<const unsigned char*>(inKeyPtr), inKeyLen,
                inSha256Flag ? mHmacSha256Params : mHmacSha1Params) &&
            EVP_MAC_update(
                &mHmacCtx,
                reinterpret_cast<const unsigned char*>(inDataPtr),
                inDataLen
            ) &&
            EVP_MAC_final(
                &mHmacCtx,
                reinterpret_cast<unsigned char*>(inResultPtr),
                &theLen,
                inSha256Flag ? kSha256Len : kSha1Len
            );
#endif
        if (! theOkFlag) {
            const int kBufSize = 127;
            char      theBuf[kBufSize + 1];
            theBuf[0] = 0;
            theBuf[kBufSize] = 0;
            ERR_error_string_n(ERR_get_error(), theBuf, kBufSize);
            FatalError(theBuf);
        }
#endif
        if ((inSha256Flag ? kSha256Len : kSha1Len) != theLen) {
            FatalError("invalid sha length");
        }
        return inResultPtr;
    }
    const char* HmacSha1(
        const void*   inKeyPtr,
        size_t        inKeyLen,
        const void*   inDataPtr,
        size_t        inDataLen,
        Sha1Buf       inResult)
    {
        return Hmac(inKeyPtr, inKeyLen, inDataPtr, inDataLen, inResult, false);
    }
    const char* Sign(
        const string& inData,
        const string& inKey)
    {
        Sha1Buf theBuf;
        const int theEncLen = Base64::Encode(
            HmacSha1(inKey.data(), inKey.size(),
                inData.data(), inData.size(), theBuf),
            kSha1Len, mSignBuf
        );
        if (theEncLen <= 0 || (int)(sizeof(mSignBuf) / sizeof(mSignBuf[0])) <=
                theEncLen) {
            FatalError("base64 encode failure");
            mSignBuf[0] = 0;
        }
        mSignBuf[theEncLen] = 0;
        return mSignBuf;
    }
    const char* HmacSha256(
        const void*   inKeyPtr,
        size_t        inKeyLen,
        const void*   inDataPtr,
        size_t        inDataLen,
        Sha256Buf     inResult)
    {
        return Hmac(inKeyPtr, inKeyLen, inDataPtr, inDataLen, inResult, true);
    }
    const void* GetV4SignKey()
    {
        // The key should be valid for 7 days, update it when the date changes.
        const char* const theISONowPtr = ISOTimeNow();
        if (memcmp(theISONowPtr, mV4SignDate, kV4SignDateLen) == 0) {
            return mV4SignKey;
        }
        memcpy(mV4SignDate, theISONowPtr, kV4SignDateLen);
        string& theTmpBuf = mTmpSignKeyBuffer;
        theTmpBuf = "AWS4";
        theTmpBuf += mSecretAccessKey;
        Sha256Buf theTmp;
        HmacSha256(
            HmacSha256(
                HmacSha256(
                    HmacSha256(
                        theTmpBuf.data(), theTmpBuf.size(),
                        mV4SignDate, kV4SignDateLen, theTmp), kSha256Len,
                    mRegion.data(), mRegion.size(), mV4SignKey), kSha256Len,
            "s3", 2, theTmp), kSha256Len,
            "aws4_request", 12, mV4SignKey
        );
        return mV4SignKey;
    }
    static const char* Sha256Hex(
        const Sha256Buf inSha256,
        char*           inHexBufPtr)
    {
        const char* const kHexDigits = "0123456789abcdef";
        char*             theResPtr  = inHexBufPtr;
        for (const char* thePtr = inSha256,
                * const theEndPtr = thePtr + kSha256Len;
                thePtr < theEndPtr;
                ++thePtr) {
            *theResPtr++ = kHexDigits[(*thePtr >> 4) & 0xF];
            *theResPtr++ = kHexDigits[*thePtr & 0xF];
        }
        *theResPtr = 0;
        return inHexBufPtr;
    }
    static bool IsAlnum(
        const int inSym)
    {
        return (('0' <= inSym && inSym <= '9') ||
                ('a' <= inSym && inSym <= 'z') ||
                ('A' <= inSym && inSym <= 'Z'));
    }
    static string& UrlEncode(
        int     inSym,
        string& inStr)
    {
        if (IsAlnum(inSym)) {
            inStr += (char)inSym;
            return inStr;
        }
        switch (inSym) {
            case '-':
            case '_':
            case '.':
            case '~':
                inStr += (char)inSym;
                return inStr;
            default:
                break;
        }
        const char* const kHexDigits = "0123456789ABCDEF";
        inStr += (char)'%';
        inStr += kHexDigits[((inSym >> 4) & 0xF)];
        inStr += kHexDigits[inSym & 0xF];
        return inStr;
    }
    static string XmlToUrlEncoding(
        const char* inPtr,
        size_t      inLen)
    {
        const char*       thePtr = inPtr;
        const char* const theEndPtr = inPtr + inLen;
        string            theRes;
        theRes.reserve(inLen);
        while (thePtr < theEndPtr) {
            switch (*thePtr & 0xFF) {
                case '&':
                    if (theEndPtr < thePtr + 4) {
                        return string();
                    }
                    if (thePtr[1] == '#') {
                        int theSym = 0;
                        thePtr += 2;
                        while (thePtr < theEndPtr && *thePtr != ';') {
                            const int theVal = *thePtr & 0xFF;
                            if (theVal < '0' || '9' < theVal) {
                                return string();
                            }
                            theSym *= 10;
                            theSym += theVal - '0';
                            if (theSym < 0 || 0xFFFF < theSym) {
                                return string();
                            }
                            ++thePtr;
                        }
                        if (theEndPtr <= thePtr || *thePtr != ';') {
                            return string();
                        }
                        thePtr++;
                        if (0xFF < theSym) {
                            UrlEncode((theSym >>  8) & 0xFF, theRes);
                        }
                        UrlEncode(theSym & 0xFF, theRes);
                    } else if (thePtr + 5 <= theEndPtr &&
                            memcmp(thePtr, "&amp;", 5) == 0) {
                        UrlEncode('&', theRes);
                        thePtr += 5;
                    } else if (thePtr + 6 <= theEndPtr &&
                            memcmp(thePtr, "&quot;", 6) == 0) {
                        UrlEncode('"', theRes);
                        thePtr += 6;
                    } else if (thePtr + 6 <= theEndPtr &&
                            memcmp(thePtr, "&apos;", 6) == 0) {
                        UrlEncode('\'', theRes);
                        thePtr += 6;
                    } else if (thePtr + 4 <= theEndPtr &&
                            memcmp(thePtr, "&lt;", 4) == 0) {
                        UrlEncode('<', theRes);
                        thePtr += 4;
                    } else if (thePtr + 4 <= theEndPtr &&
                            memcmp(thePtr, "&gt;", 4) == 0) {
                        UrlEncode('>', theRes);
                        thePtr += 4;
                    } else {
                        return string();
                    }
                    break;
                case '<':
                case '>':
                    return string();
                default:
                    UrlEncode(*thePtr & 0xFF, theRes);
                    ++thePtr;
            }
        }
        return theRes;
    }
    template <typename ST>
    static ST& XmlEncode(
        const char* inPtr,
        size_t      inLen,
        ST&         inStr)
    {
        const char*       thePtr    = inPtr;
        const char* const theEndPtr = thePtr + inLen;
        while (thePtr < theEndPtr) {
            switch (*thePtr & 0xFF) {
                case '&': inStr.Append("&amp;", 5); break;
                case '<': inStr.Append("&lt;",  4); break;
                default:  inStr.Append(*thePtr);    break;
            }
            ++thePtr;
        }
        return inStr;
    }
private:
    S3ION(
        const S3ION& inS3io);
    S3ION& operator=(
        const S3ION& inS3io);
};

KFS_REGISTER_IO_METHOD(KFS_IO_METHOD_NAME_S3ION, S3ION::New);

} // namespace KFS
