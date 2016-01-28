//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2016/1/11
// Author: Mike Ovsiannikov
//
// Copyright 2016 Quantcast Corp.
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
// Transaction log and checkpoint retrieval.
//
//
//----------------------------------------------------------------------------

#include "MetaDataSync.h"
#include "MetaRequest.h"
#include "LogReceiver.h"
#include "util.h"

#include "qcdio/qcdebug.h"

#include "common/MsgLogger.h"
#include "common/SingleLinkedQueue.h"

#include "kfsio/NetManager.h"
#include "kfsio/ClientAuthContext.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/checksum.h"
#include "kfsio/ITimeout.h"
#include "kfsio/KfsCallbackObj.h"
#include "kfsio/checksum.h"

#include "libclient/KfsNetClient.h"
#include "libclient/KfsOps.h"

#include <algorithm>
#include <vector>
#include <string>

#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>

namespace KFS
{
using std::max;

using client::KfsNetClient;
using client::KfsOp;

class MetaDataSync::Impl :
    public KfsNetClient::OpOwner,
    public ITimeout,
    public KfsCallbackObj
{
private:
    class ReadOp : public client::MetaReadMetaData
    {
    public:
        ReadOp()
            : client::MetaReadMetaData(-1),
              mPos(-1),
              mRetryCount(0),
              mInFlightFlag(false),
              mBuffer(),
              mNextPtr(0)
            {}
        void Reset()
        {
            statusMsg.clear();
            mBuffer.Clear();
            status      = 0;
            mPos        = -1;
            mRetryCount = 0;
            mInFlightFlag = false;
        }
        int64_t  mPos;
        int      mRetryCount;
        bool     mInFlightFlag;
        IOBuffer mBuffer;
        class GetNext
        {
        public:
            static ReadOp*& Next(
                ReadOp& inOp)
                { return inOp.mNextPtr; }
        };
        friend class GetNext;
    private:
        ReadOp* mNextPtr;
    private:
        ReadOp(
            const ReadOp& inReadOp);
        ReadOp& operator=(
            const ReadOp& inReadOp);
    };
    typedef SingleLinkedQueue<ReadOp, ReadOp::GetNext> ReadQueue;
public:
    Impl(
        NetManager& inNetManager)
        : OpOwner(),
          ITimeout(),
          KfsCallbackObj(),
          mKfsNetClient(inNetManager),
          mServers(),
          mFileName(),
          mCheckpointDir(),
          mLogDir(),
          mTmpSuffix(".tmp"),
          mAuthContext(),
          mReadOpsPtr(0),
          mFreeList(),
          mPendingList(),
          mFileSystemId(-1),
          mReadOpsCount(16),
          mMaxLogBlockSize(64 << 10),
          mMaxReadSize(64 << 10),
          mMaxRetryCount(10),
          mRetryCount(0),
          mRetryTimeout(3),
          mCurMaxReadSize(-1),
          mFd(-1),
          mServerIdx(0),
          mPos(0),
          mNextReadPos(0),
          mFileSize(-1),
          mLogSeq(-1),
          mNextLogSegIdx(-1),
          mCheckpointFlag(false),
          mBuffer(),
          mFreeWriteOpList(),
          mFreeWriteOpCount(0),
          mNextBlockChecksum(kKfsNullChecksum),
          mCurBlockChecksum(kKfsNullChecksum),
          mNextBlockSeq(0),
          mLogWritesInFlightCount(0),
          mWriteToFileFlag(false),
          mWriteSyncFlag(true),
          mMinWriteSize(4 << 20),
          mMaxReadOpRetryCount(8),
          mSleepingFlag(false)
    {
        SET_HANDLER(this, &Impl::LogWriteDone);
        mKfsNetClient.SetAuthContext(&mAuthContext);
        mKfsNetClient.SetMaxContentLength(3 * mMaxReadSize / 2);
    }
    virtual ~Impl()
    {
        Impl::Shutdown();
        QCRTASSERT(0 == mLogWritesInFlightCount);
    }
    int SetParameters(
        const char*       inParamPrefixPtr,
        const Properties& inParameters)
    {
        Properties::String theName(inParamPrefixPtr ? inParamPrefixPtr : "");
        const size_t       thePrefLen = theName.GetSize();
        mMaxReadSize = max(4 << 10, inParameters.getValue(
            theName.Truncate(thePrefLen).Append("maxReadSize"),
            mMaxReadSize));
        mKfsNetClient.SetMaxContentLength(3 * mMaxReadSize / 2);
        mMaxLogBlockSize = max(4 << 10, inParameters.getValue(
            theName.Truncate(thePrefLen).Append("maxLogBlockSize"),
            mMaxLogBlockSize));
        if (! mReadOpsPtr) {
            mReadOpsCount = max(size_t(1), inParameters.getValue(
            theName.Truncate(thePrefLen).Append("maxReadSize"),
            mReadOpsCount));
        }
        mTmpSuffix = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("tmpSuffix"),
            mTmpSuffix);
        mTmpSuffix = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("tmpSuffix"),
            mTmpSuffix);
         mMaxRetryCount = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("maxRetryCount"),
            mMaxRetryCount);
         mRetryTimeout = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("retryTimeout"),
            mRetryTimeout);
         mWriteSyncFlag = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("writeSync"),
            mWriteSyncFlag ? 1 : 0) != 0;
         mMinWriteSize = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("minWriteSize"),
            mMinWriteSize);
         mMaxReadOpRetryCount = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("maxReadOpRetryCount"),
            mMaxReadOpRetryCount);
        const Properties::String* const theServersPtr = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("servers"));
        bool theOkFlag = true;
        if (theServersPtr) {
            const char*       thePtr      = theServersPtr->GetPtr();
            const char* const theEndPtr   = thePtr + theServersPtr->GetSize();
            const bool        kHexFmtFlag = false;
            ServerLocation    theLoc;
            Servers           theServers;
            while (thePtr < theEndPtr) {
                if (! theLoc.ParseString(
                        thePtr, theEndPtr - thePtr, kHexFmtFlag)) {
                    KFS_LOG_STREAM_ERROR <<
                        "invalid parameter: " << theName <<
                        ": " << *theServersPtr <<
                    KFS_LOG_EOM;
                    theOkFlag = false;
                    break;
                }
                theServers.push_back(theLoc);
            }
            if (theOkFlag) {
                mServers.swap(theServers);
            }
        }
        const bool               kVerifyFlag = true;
        ClientAuthContext* const kNullCtxPtr = 0;
        string* const            kNullStrPtr = 0;
        const int theRet = mAuthContext.SetParameters(
            inParamPrefixPtr,
            inParameters,
            kNullCtxPtr,
            kNullStrPtr,
            kVerifyFlag
        );
        return (theOkFlag ? theRet : -EINVAL);
    }
    int Start(
        int64_t     inFileSystemId,
        const char* inCheckpointDirPtr,
        const char* inLogDirPtr)
    {
        if (mReadOpsPtr || ! inCheckpointDirPtr || ! *inCheckpointDirPtr ||
                ! inLogDirPtr || ! *inLogDirPtr) {
            return -EINVAL;
        }
        mNextBlockChecksum = ComputeBlockChecksum(kKfsNullChecksum, "\n", 1);
        mReadOpsPtr = new ReadOp[mReadOpsCount];
        for (size_t i = 0; i < mReadOpsCount; i++) {
            mFreeList.PutFront(mReadOpsPtr[i]);
        }
        mCheckpointDir = inCheckpointDirPtr;
        mLogDir        = inLogDirPtr;
        int64_t const theFsId = GetFsId(inFileSystemId);
        if (theFsId < 0) {
            return (int)theFsId;
        }
        if (inFileSystemId < 0) {
            inFileSystemId = theFsId;
        } else if (theFsId != inFileSystemId) {
            KFS_LOG_STREAM_ERROR <<
                "file system id mismatch: " <<
            KFS_LOG_EOM;
        }
        return 0;
    }
    void Shutdown()
    {
        if (mSleepingFlag) {
            mSleepingFlag = false;
            mKfsNetClient.GetNetManager().UnRegisterTimeoutHandler(this);
        }
        Reset();
        delete [] mReadOpsPtr;
        mReadOpsPtr = 0;
        MetaRequest* thePtr;
        while ((thePtr = mFreeWriteOpList.PopFront())) {
            mFreeWriteOpCount--;
            MetaRequest::Release(thePtr);
        }
        QCASSERT(0 == mFreeWriteOpCount);
    }
    virtual void OpDone(
        KfsOp*    inOpPtr,
        bool      inCanceledFlag,
        IOBuffer* inBufferPtr)
    {
        if (inOpPtr < mReadOpsPtr ||
                mReadOpsPtr + mReadOpsCount < inOpPtr ||
                ! inBufferPtr) {
            panic("metad data sync: invalid read RPC completion");
            return;
        }
        ReadOp& theOp = mReadOpsPtr[
            static_cast<const ReadOp*>(inOpPtr) - mReadOpsPtr];
        if (! theOp.mInFlightFlag || &theOp.mBuffer != inBufferPtr) {
            panic("metad data sync: invalid read RPC state");
            return;
        }
        theOp.mInFlightFlag = false;
        if (inCanceledFlag) {
            theOp.Reset();
            mFreeList.PutFront(theOp);
            return;
        }
        if (0 <= theOp.status && theOp.mPos != theOp.readPos) {
            theOp.status    = -EINVAL;
            theOp.statusMsg = "invalid read position";
        } else if (theOp.startLogSeq < 0) {
            theOp.status    = -EINVAL;
            theOp.statusMsg = "invalid log sequence";
        } else if (theOp.fileSystemId < 0) {
            theOp.status    = -EINVAL;
            theOp.statusMsg = "invalid file system id";
        } else if (0 <= theOp.endLogSeq && ! theOp.checkpointFlag &&
                theOp.endLogSeq <= theOp.startLogSeq) {
            theOp.status    = -EINVAL;
            theOp.statusMsg = "invalid log end sequence";
        }
        if (theOp.status < 0) {
            HandleError(theOp);
            return;
        }
        const uint32_t theChecksum = ComputeCrc32(
            &theOp.mBuffer, theOp.mBuffer.BytesConsumable());
        if (theChecksum != theOp.checksum) {
            theOp.status    = -EBADCKSUM;
            theOp.statusMsg = "received data checksum mismatch";
            HandleError(theOp);
            return;
        }
        Handle(theOp);
    }
    virtual void Timeout()
    {
        if (! mSleepingFlag) {
            return;
        }
        mKfsNetClient.GetNetManager().UnRegisterTimeoutHandler(this);
        mSleepingFlag = false;
        StartRead();
    }
private:
    enum { kMaxCommitLineLen = 512 };
    typedef vector<ServerLocation> Servers;
    typedef SingleLinkedQueue<
        MetaRequest,
        MetaRequest::GetNext
    > FreeWriteOpList;

    KfsNetClient      mKfsNetClient;
    Servers           mServers;
    string            mFileName;
    string            mCheckpointDir;
    string            mLogDir;
    string            mTmpSuffix;
    ClientAuthContext mAuthContext;
    ReadOp*           mReadOpsPtr;
    ReadQueue         mFreeList;
    ReadQueue         mPendingList;
    int64_t           mFileSystemId;
    size_t            mReadOpsCount;
    int               mMaxLogBlockSize;
    int               mMaxReadSize;
    int               mMaxRetryCount;
    int               mRetryCount;
    int               mRetryTimeout;
    int               mCurMaxReadSize;
    int               mFd;
    size_t            mServerIdx;
    int64_t           mPos;
    int64_t           mNextReadPos;
    int64_t           mFileSize;
    seq_t             mLogSeq;
    seq_t             mNextLogSegIdx;
    bool              mCheckpointFlag;
    IOBuffer          mBuffer;
    FreeWriteOpList   mFreeWriteOpList;
    int               mFreeWriteOpCount;
    uint32_t          mNextBlockChecksum;
    uint32_t          mCurBlockChecksum;
    seq_t             mNextBlockSeq;
    int               mLogWritesInFlightCount;
    bool              mWriteToFileFlag;
    bool              mWriteSyncFlag;
    int               mMinWriteSize;
    int               mMaxReadOpRetryCount;
    bool              mSleepingFlag;
    char              mCommmitBuf[kMaxCommitLineLen];

    void InitRead()
    {
        if (! mPendingList.IsEmpty()) {
            mKfsNetClient.Stop();
            QCRTASSERT(mPendingList.IsEmpty());
        }
        mNextReadPos      = 0;
        mPos              = 0;
        mFileSize         = -1;
        mCurMaxReadSize   = mMaxReadSize;
        mCurBlockChecksum = kKfsNullChecksum;
    }
    int GetCheckpoint()
    {
        if (! mReadOpsPtr || mServers.empty()) {
            return -EINVAL;
        }
        if (0 <= mFd) {
            close(mFd);
            mFd = -1;
        }
        if (mServers.size() < mServerIdx) {
            mServerIdx = 0;
        }
        mKfsNetClient.Stop();
        QCASSERT(mPendingList.IsEmpty());
        mKfsNetClient.SetServer(mServers[mServerIdx]);
        mLogSeq           = -1;
        mCheckpointFlag   = true;
        mWriteToFileFlag  = true;
        InitRead();
        if (! StartRead()) {
            panic("metad data sync: failed to iniate checkpoint download");
            return -EFAULT;
        }
        return 0;
    }
    bool StartRead()
        { return StartRead(mNextReadPos); }
    bool StartRead(
        int64_t inNextReadPos)
    {
        ReadOp* const theOpPtr = mFreeList.PopFront();
        if (! theOpPtr) {
            return false;
        }
        ReadOp& theOp = *theOpPtr;
        theOp.mPos = inNextReadPos;
        mPendingList.PushBack(theOp);
        return StartRead(theOp);
    }
    bool StartRead(
        ReadOp& inOp)
    {
        inOp.fileSystemId   = mFileSystemId;
        inOp.startLogSeq    = mLogSeq;
        inOp.readPos        = inOp.mPos;
        inOp.checkpointFlag = mCheckpointFlag;
        inOp.readSize       = 0 <= mFileSize ?
            (int)min(mFileSize - inOp.mPos, int64_t(mCurMaxReadSize)) :
            mCurMaxReadSize;
        inOp.mInFlightFlag  = true;
        inOp.mPos           = inOp.mPos;
        inOp.fileSize       = -1;
        inOp.status         = 0;
        inOp.fileName.clear();
        inOp.statusMsg.clear();
        inOp.mBuffer.Clear();
        if (! mKfsNetClient.Enqueue(&inOp, this, &inOp.mBuffer)) {
            panic("metad data sync: read op enqueue failure");
            return false;
        }
        return true;
    }
    void Handle(
        ReadOp& inOp)
    {
        if (0 <= mLogSeq && mLogSeq != inOp.startLogSeq) {
            KFS_LOG_STREAM_ERROR <<
                "start log sequence has chnaged:"
                " from: " << mLogSeq <<
                " to: "   << inOp.startLogSeq <<
            KFS_LOG_EOM;
            inOp.status    = -EINVAL;
            inOp.statusMsg = "invalid file size";
            HandleError(inOp);
            return;
        }
        if (0 == mPos) {
            QCASSERT(mPendingList.Front() == &inOp &&
                mPendingList.Back() == &inOp &&
                inOp.mPos == mPos && 0 == mPos);
            if (mFileSystemId < 0) {
                mFileSystemId = inOp.fileSystemId;
            } else if (mFileSystemId != inOp.fileSystemId) {
                KFS_LOG_STREAM_ERROR <<
                    "file system id mismatch:"
                    " expect: "   << mFileSystemId <<
                    " received: " << inOp.fileSystemId <<
                KFS_LOG_EOM;
                inOp.status    = -EINVAL;
                inOp.statusMsg = "file system id mismatch";
                HandleError(inOp);
                return;
            }
            mLogSeq         = inOp.startLogSeq;
            mCurMaxReadSize = inOp.mBuffer.BytesConsumable();
            mNextReadPos    = mCurMaxReadSize;
            inOp.readSize   = mCurMaxReadSize;
            if (0 <= inOp.fileSize) {
                mFileSize = inOp.fileSize;
            }
            if (mCheckpointFlag && mFileSize < 0) {
                inOp.status    = -EINVAL;
                inOp.statusMsg = "invalid checkpoint file size";
                HandleError(inOp);
                return;
            }
            if (mWriteToFileFlag && mFd < 0) {
                size_t const theDotPos = inOp.fileName.find('.');
                size_t const theEndPos = mCheckpointFlag ?
                    inOp.fileName.size() : inOp.fileName.rfind('.');
                seq_t theSeq    = -1;
                seq_t theSegIdx = -1;
                const char* thePtr = inOp.fileName.data() + theDotPos + 1;
                if (string::npos == theDotPos || string::npos == theEndPos ||
                        theEndPos <= theDotPos ||
                        ! HexIntParser::Parse(
                            thePtr, theEndPos - theDotPos - 1, theSeq) ||
                        theSeq != mLogSeq ||
                        inOp.fileName.find('/') != string::npos ||
                        (mCheckpointFlag && (! HexIntParser::Parse(
                                ++thePtr,
                                inOp.fileName.size() - theEndPos - 1,
                                theSegIdx) ||
                            (0 < mNextLogSegIdx &&
                                theSegIdx != mNextLogSegIdx)))) {
                    KFS_LOG_STREAM_ERROR <<
                        "invalid file name: " << inOp.Show() <<
                        " log sequence:"
                        " expected: " << mLogSeq <<
                        " actual: "   << theSeq <<
                        " segment: "  << theSegIdx <<
                    KFS_LOG_EOM;
                    inOp.status    = -EINVAL;
                    inOp.statusMsg = "invalid file name";
                    HandleError(inOp);
                    return;
                }
                if (mCheckpointFlag) {
                    mFileName.assign(
                        mCheckpointDir.data(), mCheckpointDir.size());
                } else {
                    mFileName.assign(mLogDir.data(), mLogDir.size());
                }
                mFileName += inOp.fileName;
                mFileName += mTmpSuffix;
                mFd = open(
                    mFileName.c_str(),
                    O_WRONLY | (mWriteSyncFlag ? O_SYNC : 0) |
                        O_CREAT | O_TRUNC,
                    0666
                );
                if (mFd < 0) {
                    const int theErr = errno;
                    KFS_LOG_STREAM_ERROR <<
                        "open failure: " << mFileName <<
                        ": " << QCUtils::SysError(theErr) <<
                    KFS_LOG_EOM;
                    inOp.status    = 0 < theErr ? -theErr : -EFAULT;
                    inOp.statusMsg = "failed to open file";
                    HandleError(inOp);
                    return;
                }
            }
        } else if (0 < mFileSize) {
            if (inOp.fileSize != mFileSize) {
                KFS_LOG_STREAM_ERROR <<
                    "file size has chnaged:"
                    " from: " << mFileSize <<
                    " to: "   << inOp.fileSize <<
                KFS_LOG_EOM;
                inOp.status    = -EINVAL;
                inOp.statusMsg = "invalid file size";
                HandleError(inOp);
                return;
            }
            if (inOp.readSize != inOp.mBuffer.BytesConsumable()) {
                KFS_LOG_STREAM_ERROR <<
                    "short read:"
                    " pos: "      << inOp.mPos <<
                    " expected: " << inOp.readSize <<
                    " received: " << inOp.mBuffer.BytesConsumable() <<
                    " max read: " << mCurMaxReadSize <<
                    " eof: "      << mFileSize <<
                KFS_LOG_EOM;
                inOp.status    = -EINVAL;
                inOp.statusMsg = "short read";
                HandleError(inOp);
                return;
            }
        }
        if (inOp.mPos != mPos) {
            return;
        }
        ReadOp* theOpPtr = mPendingList.Front();
        if (! theOpPtr || &inOp != theOpPtr || theOpPtr->mInFlightFlag) {
            panic("metad data sync: invalid empty pending list");
            return;
        }
        int const theRdSize = theOpPtr->mBuffer.BytesConsumable();
        bool      theEofFlag;
        do {
            QCASSERT(mPos == theOpPtr->mPos);
            mPos += theRdSize;
            theEofFlag = 0 <= mFileSize ? mFileSize <= mPos : theRdSize <= 0;
            if (0 <= mFd) {
                IOBuffer& theBuffer = theOpPtr->mBuffer;
                mBuffer.Move(&theBuffer);
                if (theEofFlag || mMinWriteSize <= mBuffer.BytesConsumable()) {
                    while (! mBuffer.IsEmpty()) {
                        const int theNWr = mBuffer.Write(mFd);
                        if (theNWr < 0) {
                            KFS_LOG_STREAM_ERROR <<
                                mFileName << ": write failure:" <<
                                QCUtils::SysError((int)-theNWr) <<
                            KFS_LOG_EOM;
                            HandleError();
                            return;
                        }
                        mBuffer.Consume(theNWr);
                    }
                }
            } else {
                if (theOpPtr->checkpointFlag || mCheckpointFlag) {
                    panic("metad data sync: attempt to replay checkpoint");
                } else {
                    SubmitLogBlock(theOpPtr->mBuffer);
                }
            }
            if (theEofFlag && ! mCheckpointFlag) {
                mLogSeq = theOpPtr->endLogSeq;
            }
            theOpPtr->Reset();
            mPendingList.PopFront();
            mFreeList.PutFront(*theOpPtr);
        } while (
            (theOpPtr = mPendingList.Front()) &&
            ! theOpPtr->mInFlightFlag);
        if (theEofFlag) {
            ReadDone();
        } else {
            if (mFileSize < 0) {
                if (StartRead()) {
                    mNextReadPos += theRdSize;
                }
            } else {
                while (mNextReadPos < mFileSize && StartRead()) {
                    mNextReadPos += mCurMaxReadSize;
                }
            }
        }
    }
    void ReadDone()
    {
        mKfsNetClient.Cancel();
        if (0 <= mFd) {
            const int theFd = mFd;
            mFd = -1;
            if (close(theFd)) {
                const int theErr = errno;
                KFS_LOG_STREAM_ERROR <<
                    mFileName << ": close failure:" <<
                    QCUtils::SysError(theErr) <<
                KFS_LOG_EOM;
                HandleError();
                return;
            }
        }
        mRetryCount     = 0;
        mCheckpointFlag = false;
        if (0 <= mLogSeq) {
            InitRead();
            if (! StartRead()) {
                panic("metad data sync: failed to iniate download");
            }
        } else {
            DownloadDone(0);
        }
    }
    void DownloadDone(
        int inStatus)
    {
    }
    void HandleError(
        ReadOp& inReadOp)
    {
        KFS_LOG_STREAM_ERROR <<
            "status: " << inReadOp.statusMsg <<
            " try: "   << mRetryCount <<
            " op: "    << inReadOp.mRetryCount <<
            " pos:"
            " cur: "   << mPos <<
            " op: "    << inReadOp.mPos <<
            " "        << inReadOp.Show() <<
        KFS_LOG_EOM;
        const int64_t thePos = inReadOp.mPos;
        inReadOp.Reset();
        if (++inReadOp.mRetryCount < mMaxReadOpRetryCount) {
            inReadOp.mPos = thePos;
            if (StartRead(inReadOp)) {
                return;
            }
        }
        HandleError();
    }
    void HandleError(
        MetaLogWriterControl& inOp)
    {
        inOp.status = -EINVAL;
        mLogWritesInFlightCount++;
        LogWriteDone(EVENT_CMD_DONE, &inOp);
        HandleError();
    }
    void HandleError()
    {
        Reset();
        mRetryCount++;
        if (mMaxRetryCount < mRetryCount) {
            DownloadDone(-EIO);
            return;
        }
        KFS_LOG_STREAM_ERROR <<
            "retry: " << mRetryCount <<
            " of "    << mMaxRetryCount <<
            " scheduling retry in: " << mRetryTimeout <<
        KFS_LOG_EOM;
        Sleep(mRetryTimeout);
    }
    void Sleep(
        int inTimeSec)
    {
        if (mSleepingFlag) {
            return;
        }
        mSleepingFlag = true;
        const bool kResetTimerFlag = true;
        SetTimeoutInterval(inTimeSec * 1000, kResetTimerFlag);
        mKfsNetClient.GetNetManager().RegisterTimeoutHandler(this);
    }
    void Reset()
    {
        if (0 < mFd) {
            close(mFd);
            mFd = -1;
        }
        while (mPendingList.PopFront())
            {}
        mKfsNetClient.Stop();
        mBuffer.Clear();
        mPos         = 0;
        mNextReadPos = 0;
    }
    template<typename T>
    bool ParseField(
        const char*& ioPtr,
        const char*  inEndPtr,
        T&           outVal)
    {
        const char* theStartPtr = ioPtr;
        while (ioPtr < inEndPtr && (*ioPtr & 0xFF) != '/') {
            ++ioPtr;
        }
        if (ioPtr < inEndPtr &&
                HexIntParser::Parse(
                    theStartPtr,
                    ioPtr - theStartPtr,
                    outVal)) {
            ++ioPtr;
            return true;
        }
        return false;
    }
    void SubmitLogBlock(
        IOBuffer& inBuffer)
    {
        mBuffer.Move(&inBuffer);
        for (; ;) {
            int thePos = mBuffer.IndexOf(0, "\nc/");
            if (thePos < 0) {
                break;
            }
            thePos++;
            const int theEndPos = mBuffer.IndexOf(thePos, "\n");
            if (theEndPos < 0) {
                break;
            }
            int theLen = theEndPos - theEndPos;
            if (kMaxCommitLineLen < theLen) {
                KFS_LOG_STREAM_ERROR <<
                    "log block commit line"
                    " length: "  << theLen <<
                    " exceeds: " << kMaxCommitLineLen <<
                    IOBuffer::DisplayData(mBuffer, 512) <<
                KFS_LOG_EOM;
                HandleError();
                return;
            }
            MetaLogWriterControl& theOp = GetLogWriteOp();
            const int theRem = LogReceiver::ParseBlockLines(
                mBuffer, theEndPos + 1, theOp, '\n');
            if (0 != theRem || theOp.blockLines.IsEmpty()) {
                KFS_LOG_STREAM_ERROR <<
                    "invalid log block received:" <<
                    IOBuffer::DisplayData(mBuffer, 512) <<
                KFS_LOG_EOM;
                HandleError(theOp);
                return;
            }
            // Copy log block body, i.e. except trailing / commit line.
            theOp.blockData.Move(&mBuffer, thePos);
            const char* const theStartPtr =
                mBuffer.CopyOutOrGetBufPtr(mCommmitBuf, theLen);
            const char* const theEndPtr   = theStartPtr + theLen;
            const char*       thePtr      = theStartPtr;
            thePtr += 2;
            seq_t   theCommitted   = -1;
            fid_t   theFid         = -1;
            int64_t theErrChkSum   = -1;
            int     theBlockStatus = -1;
            seq_t   theLogSeq      = -1;
            int     theBlockSeqLen = -1;
            if (! (ParseField(thePtr, theEndPtr, theCommitted) &&
                    ParseField(thePtr, theEndPtr, theFid) &&
                    ParseField(thePtr, theEndPtr, theErrChkSum) &&
                    ParseField(thePtr, theEndPtr, theBlockStatus) &&
                    ParseField(thePtr, theEndPtr, theLogSeq) &&
                    ParseField(thePtr, theEndPtr, theBlockSeqLen) &&
                    0 <= theCommitted &&
                    0 <= theBlockStatus &&
                    theCommitted <= theLogSeq &&
                    theBlockSeqLen <= theLogSeq)) {
                KFS_LOG_STREAM_ERROR <<
                    "invalid log commit line:" <<
                    IOBuffer::DisplayData(mBuffer, theLen) <<
                KFS_LOG_EOM;
                HandleError(theOp);
                return;
            }
            theOp.blockStartSeq = theLogSeq - theBlockSeqLen;
            theOp.blockEndSeq   = theLogSeq;
            const int theCopyLen  = (int)(thePtr - theStartPtr);
            int64_t   theBlockSeq = -1;
            if (ParseField(thePtr, theEndPtr, theBlockSeq) ||
                    mNextBlockSeq != theBlockSeq) {
                KFS_LOG_STREAM_ERROR <<
                    "invalid log commit block sequence:" <<
                    " expected: " << mNextBlockSeq <<
                    " actual: "   << theBlockSeq <<
                    " data: " << IOBuffer::DisplayData(mBuffer, theLen) <<
                KFS_LOG_EOM;
                HandleError(theOp);
                return;
            }
            mNextBlockSeq++;
            const int theBlkSeqLen = thePtr - (theStartPtr + theCopyLen);
            uint32_t  theChecksum  = 0;
            if (! HexIntParser::Parse(
                    thePtr, theEndPtr - thePtr, theChecksum)) {
                KFS_LOG_STREAM_ERROR <<
                    "log commit block checksum parse failure:" <<
                    " data: " << IOBuffer::DisplayData(mBuffer, theLen) <<
                KFS_LOG_EOM;
                HandleError(theOp);
                return;
            }
            theOp.blockData.Move(&mBuffer, theCopyLen);
            theOp.blockChecksum = ComputeBlockChecksum(
                &theOp.blockData,
                theOp.blockData.BytesConsumable(),
                kKfsNullChecksum
            );
            if (mCurBlockChecksum != kKfsNullChecksum) {
                mCurBlockChecksum = ChecksumBlocksCombine(
                    mCurBlockChecksum,
                    theOp.blockChecksum,
                    theOp.blockData.BytesConsumable()
                );
            }
            mCurBlockChecksum = ComputeBlockChecksum(
                &mBuffer, theBlkSeqLen, mCurBlockChecksum);
            if (mCurBlockChecksum != theChecksum) {
                KFS_LOG_STREAM_ERROR <<
                    "log block checksum mismatch:"
                    " expected: " << theChecksum <<
                    " computed: " << mCurBlockChecksum <<
                KFS_LOG_EOM;
                HandleError(theOp);
                return;
            }
            theOp.blockLines.Back() -=
                mBuffer.Consume(theLen - theCopyLen + 1);
            mCurBlockChecksum = mNextBlockChecksum;
            mLogWritesInFlightCount++;
            if (theBlockSeqLen <= 0) {
                LogWriteDone(EVENT_CMD_DONE, &theOp);
            } else {
                submit_request(&theOp);
            }
        }
        if (mMaxLogBlockSize < mBuffer.BytesConsumable()) {
            KFS_LOG_STREAM_ERROR <<
                "log block size: " << mBuffer.BytesConsumable() <<
                " exceeds: "       << mMaxLogBlockSize <<
                " data: "          << IOBuffer::DisplayData(mBuffer, 256) <<
            KFS_LOG_EOM;
            HandleError();
        }
    }
    MetaLogWriterControl& GetLogWriteOp()
    {
        MetaLogWriterControl* thePtr =
            static_cast<MetaLogWriterControl*>(mFreeWriteOpList.PopFront());
        if (thePtr) {
            mFreeWriteOpCount--;
        } else {
            thePtr = new MetaLogWriterControl(
                MetaLogWriterControl::kWriteBlock);
            thePtr->clnt = this;
        }
        return *thePtr;
    }
    int LogWriteDone(
        int   inEvent,
        void* inDataPtr)
    {
        if (! inDataPtr || EVENT_CMD_DONE != inEvent ||
                mLogWritesInFlightCount <= 0) {
            panic("metad data sync: invalid log write completion");
            return -1;
        }
        mLogWritesInFlightCount--;
        MetaLogWriterControl& theOp = *reinterpret_cast<MetaLogWriterControl*>(
            inDataPtr);
        theOp.blockData.Clear();
        theOp.blockLines.Clear();
        theOp.statusMsg.clear();
        theOp.status      = 0;
        theOp.submitCount = 0;
        theOp.seqno       = -1;
        theOp.logseq      = -1;
        theOp.suspended   = false;
        theOp.logAction   = MetaLogWriterControl::kLogAlways;
        if (mReadOpsPtr) {
            mFreeWriteOpCount++;
            mFreeWriteOpList.PutFront(theOp);
        } else {
            MetaRequest::Release(&theOp);
        }
        return 0;
    }
    int PrepareToSync()
    {
        const bool kDeleteTmpFlag = true;
        const bool kRenameFlag    = false;
        return DeleteOrRenameTmp(mLogDir, kDeleteTmpFlag, kRenameFlag);
    }
    int CommitSync()
    {
        const bool kDeleteTmpFlag = false;
        bool       theRenameFlag  = false;
        const int theRet = DeleteOrRenameTmp(
            mLogDir, kDeleteTmpFlag, theRenameFlag);
        theRenameFlag = true;
        return (0 != theRet ? theRet : DeleteOrRenameTmp(
            mLogDir, kDeleteTmpFlag, theRenameFlag));
    }
    int DeleteOrRenameTmp(
        const string& inDirName,
        bool          inDeleteTmpFlag,
        bool          inRenameTmpFlag)
    {
        if (! inDirName.empty() || mTmpSuffix.empty()) {
            KFS_LOG_STREAM_ERROR <<
                "invalid parameters"
                " direcotry: "  << inDirName <<
                " siffix: "     << mTmpSuffix <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        DIR* const theDirPtr = opendir(inDirName.c_str());
        if (! theDirPtr) {
            const int theErr = errno;
            KFS_LOG_STREAM_ERROR <<
                "opendir: " << inDirName <<
                ": " << QCUtils::SysError(theErr) <<
            KFS_LOG_EOM;
            return (0 < theErr ? -theErr : -EINVAL);
        }
        string theName(inDirName.data(), inDirName.size());
        if ('/' != *theName.rbegin()) {
            theName += '/';
        }
        string theDestName;
        const size_t         thePrefixLen = theName.size();
        const size_t         theSuffixLen = mTmpSuffix.size();
        const char* const    theSuffixPtr = mTmpSuffix.data();
        int                  theRet       = 0;
        const struct dirent* thePtr;
        while ((thePtr = readdir(theDirPtr))) {
            const char* const theNamePtr = thePtr->d_name;
            if ('.' == theNamePtr[0] &&
                    (0 == theNamePtr[1] ||
                        ('.' == theNamePtr[1] && 0 == theNamePtr[2]))) {
                continue;
            }
            const size_t theLen     = strlen(theNamePtr);
            const bool  theTempFlag = theSuffixLen <= theLen ||
                0 == memcmp(
                    theSuffixPtr,
                    theNamePtr + theLen - theSuffixLen,
                    theSuffixLen);
            theName.erase(thePrefixLen);
            theName += theNamePtr;
            if (inRenameTmpFlag) {
                if (! theTempFlag) {
                    KFS_LOG_STREAM_ERROR <<
                        "unexpected directory entry: " << theName <<
                    KFS_LOG_EOM;
                    theRet = -EINVAL;
                    break;
                }
                theDestName.assign(theName.data(),
                    theName.size() - theSuffixLen);
                if (rename(theName.c_str(), theDestName.c_str())) {
                    const int theErr = errno;
                    KFS_LOG_STREAM_ERROR <<
                        "rename: " << theName <<
                        " => "     << theDestName <<
                        ": " << QCUtils::SysError(theErr) <<
                    KFS_LOG_EOM;
                    theRet = theErr < 0 ? theErr :
                        theErr == 0 ? -EINVAL : -theErr;
                    break;
                }
            } else  if ((inDeleteTmpFlag ? theTempFlag : ! theTempFlag)) {
                if (unlink(theName.c_str())) {
                    const int theErr = errno;
                    KFS_LOG_STREAM_ERROR <<
                        "unlink: " << theName <<
                        ": " << QCUtils::SysError(theErr) <<
                    KFS_LOG_EOM;
                    theRet = theErr < 0 ? theErr :
                        theErr == 0 ? -EINVAL : -theErr;
                    break;
                }
            }
        }
        closedir(theDirPtr);
        return theRet;
    }
    int CountDirEntries(
        const char* inDirNamePtr)
    {
        DIR* const theDirPtr = opendir(inDirNamePtr);
        if (! theDirPtr) {
            const int theErr = errno;
            KFS_LOG_STREAM_ERROR <<
                "opendir " << mCheckpointDir << ": " <<
                QCUtils::SysError(theErr) <<
            KFS_LOG_EOM;
            return -theErr;
        }
        int                  theRet = 0;
        const struct dirent* thePtr;
        while ((thePtr = readdir(theDirPtr))) {
            const char* const theNamePtr = thePtr->d_name;
            if ('.' == theNamePtr[0] &&
                    (0 == theNamePtr[1] ||
                        ('.' == theNamePtr[1] && 0 == theNamePtr[2]))) {
                continue;
            }
            theRet++;
        }
        closedir(theDirPtr);
        return theRet;
    }
    int64_t GetFsId(
        int64_t inFsId)
    {
        string theFileName(mCheckpointDir.data(), mCheckpointDir.size());
        if (! theFileName.empty() && '/' != *theFileName.rbegin()) {
            theFileName += '/';
        }
        theFileName +=  "latest";
        const int theFd = open(theFileName.c_str(), O_RDONLY);
        if (theFd < 0) {
            const int theErr = errno;
            if (theErr != ENOENT || inFsId < 0) {
                KFS_LOG_STREAM_ERROR <<
                    "open " << theFileName << ": " <<
                    QCUtils::SysError(theErr) <<
                KFS_LOG_EOM;
                return -theErr;
            }
            int theCnt = CountDirEntries(mCheckpointDir.c_str());
            if (theCnt < 0) {
                return theCnt;
            }
            if (0 < theCnt) {
                KFS_LOG_STREAM_ERROR <<
                    "no latest file in non empty checkpoint directory: " <<
                        mCheckpointDir << ": " <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            if ((theCnt = CountDirEntries(mLogDir.c_str())) < 0) {
                return theCnt;
            }
            if (0 < theCnt) {
                KFS_LOG_STREAM_ERROR <<
                    "no latest file while log directory is not empty: " <<
                        mCheckpointDir << ": " <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            return inFsId;
        }
        StBufferT<char, 1> theBuf;
        theBuf.Resize(4 << 10);
        ssize_t theNRd = read(theFd, theBuf.GetPtr(), theBuf.GetSize() - 1);
        int64_t theRet;
        if (theNRd < 0) {
            const int theErr = errno;
            KFS_LOG_STREAM_ERROR <<
                "read " << theFileName << ": " <<
                QCUtils::SysError(theErr) <<
            KFS_LOG_EOM;
            theRet = -theErr;
        } else {
            theBuf.GetPtr()[theNRd] = 0;
            const char* thePtr = strstr(
                theBuf.GetPtr(), "\nfilesysteminfo/fsid/");
            if (! thePtr) {
                KFS_LOG_STREAM_ERROR <<
                    "no file system id found: " << theFileName << ": " <<
                KFS_LOG_EOM;
                theRet = -EINVAL;
            } else {
                thePtr += 21;
                const char* theEndPtr = strchr(thePtr, '/');
                if (! theEndPtr ||
                        ! HexIntParser::Parse(
                            thePtr, theEndPtr - thePtr, theRet)) {
                    KFS_LOG_STREAM_ERROR <<
                        "file system id parse failure: " <<
                            theFileName << ": " <<
                    KFS_LOG_EOM;
                    theRet = -EINVAL;
                }
            }
        }
        close(theFd);
        return theRet;
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

MetaDataSync::MetaDataSync(
    NetManager& inNetManager)
    : mImpl(*(new Impl(inNetManager)))
    {}

MetaDataSync::~MetaDataSync()
{
    delete &mImpl;
}

    int
MetaDataSync::SetParameters(
    const char*       inParamPrefixPtr,
    const Properties& inParameters)
{
    return mImpl.SetParameters(inParamPrefixPtr, inParameters);
}

    int
MetaDataSync::Start(
    int64_t     inFileSystemId,
    const char* inCheckpointDirPtr,
    const char* inLogDirPtr)
{
    return mImpl.Start(inFileSystemId, inCheckpointDirPtr, inLogDirPtr);
}

    void
MetaDataSync::Shutdown()
{
    mImpl.Shutdown();
}

} // namespace KFS
