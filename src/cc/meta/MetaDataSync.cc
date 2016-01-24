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
              mInFlightFlag(false),
              mBuffer(),
              mNextPtr(0)
            {}
        bool operator<(
            const ReadOp& inRhs)
            { return (mPos < inRhs.mPos); }
        int64_t  mPos;
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
          mAuthContext(),
          mReadOpsPtr(0),
          mFreeList(),
          mPendingList(),
          mFileSystemId(-1),
          mReadOpsCount(16),
          mMaxLogBlockSize(64 << 10),
          mMaxReadSize(64 << 10),
          mMaxRetryCount(10),
          mRetryCount(-1),
          mRetryTimeout(3),
          mCurMaxReadSize(-1),
          mFd(-1),
          mServerIdx(0),
          mPos(-1),
          mFileSize(-1),
          mLogSeq(-1),
          mCheckpointFlag(false),
          mLogBuffer(),
          mFreeWriteOpList(),
          mFreeWriteOpCount(0),
          mNextBlockChecksum(kKfsNullChecksum),
          mCurBlockChecksum(kKfsNullChecksum),
          mNextBlockSeq(0)
    {
        SET_HANDLER(this, &Impl::LogWriteDone);
        mKfsNetClient.SetAuthContext(&mAuthContext);
        mKfsNetClient.SetMaxContentLength(3 * mMaxReadSize / 2);
    }
    virtual ~Impl()
        { Impl::Shutdown(); }
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
        const bool               kVerifyFlag = true;
        ClientAuthContext* const kNullCtxPtr = 0;
        string* const            kNullStrPtr = 0;
        return mAuthContext.SetParameters(
            inParamPrefixPtr,
            inParameters,
            kNullCtxPtr,
            kNullStrPtr,
            kVerifyFlag
        );
    }
    int Start(
        int64_t        inFileSystemId,
        MetaDataStore& inMetaDataStore)
    {
        if (mReadOpsPtr) {
            return -EINVAL;
        }
        mNextBlockChecksum = ComputeBlockChecksum(kKfsNullChecksum, "\n", 1);
        mReadOpsPtr = new ReadOp[mReadOpsCount];
        for (size_t i = 0; i < mReadOpsCount; i++) {
            mFreeList.PutFront(mReadOpsPtr[i]);
        }
        return 0;
    }
    void Shutdown()
    {
        Reset();
        delete [] mReadOpsPtr;
        mReadOpsPtr = 0;
        MetaRequest* thePtr;
        while ((thePtr = mFreeWriteOpList.PopFront())) {
            MetaRequest::Release(thePtr);
        }
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
            theOp.mBuffer.Clear();
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
    bool              mCheckpointFlag;
    IOBuffer          mLogBuffer;
    FreeWriteOpList   mFreeWriteOpList;
    int               mFreeWriteOpCount;
    uint32_t          mNextBlockChecksum;
    uint32_t          mCurBlockChecksum;
    seq_t             mNextBlockSeq;
    char              mCommmitBuf[kMaxCommitLineLen];

    int GetCheckpoint()
    {
        if (! mReadOpsPtr || mServers.empty()) {
            return -EINVAL;
        }
        if (0 <= mFd) {
            close(mFd);
            mFd = -1;
        }
        if (mServers.size() < ++mServerIdx) {
            mServerIdx = 0;
        }
        mKfsNetClient.Stop();
        QCASSERT(mPendingList.IsEmpty());
        mKfsNetClient.SetServer(mServers[mServerIdx]);
        mLogSeq           = -1;
        mCheckpointFlag   = true;
        mNextReadPos      = 0;
        mPos              = 0;
        mFileSize         = -1;
        mCurMaxReadSize   = mMaxReadSize;
        mCurBlockChecksum = kKfsNullChecksum;
        if (! StartRead()) {
            panic("metad data sync: failed to iniate checkpoint download");
            return -EFAULT;
        }
        return 0;
    }
    bool StartRead()
    {
        ReadOp* const theOpPtr = mFreeList.PopFront();
        if (! theOpPtr) {
            return false;
        }
        ReadOp& theOp = *theOpPtr;
        theOp.fileSystemId   = mFileSystemId;
        theOp.startLogSeq    = mLogSeq;
        theOp.readPos        = mNextReadPos;
        theOp.checkpointFlag = mCheckpointFlag;
        theOp.readSize       = 0 <= mFileSize ?
            (int)min(mFileSize - mNextReadPos, int64_t(mCurMaxReadSize)) :
            mCurMaxReadSize;
        theOp.mInFlightFlag  = true;
        theOp.mPos           = mNextReadPos;
        theOp.fileSize       = -1;
        theOp.status         = 0;
        theOp.statusMsg.clear();
        theOp.mBuffer.Clear();
        mPendingList.PushBack(theOp);
        if (! mKfsNetClient.Enqueue(&theOp, this, &theOp.mBuffer)) {
            panic("metad data sync: read op enqueue failure");
            return false;
        }
        return true;
    }
    void Handle(
        ReadOp& inOp)
    {
        if (inOp.mPos != mPos) {
            return;
        }
        if (mLogSeq < 0) {
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
        } else if (mLogSeq != inOp.startLogSeq) {
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
        if (0 < mFileSize) {
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
        ReadOp* theOpPtr = mPendingList.Front();
        if (! theOpPtr) {
            panic("metad data sync: invalid empty pending list");
            return;
        }
        if (theOpPtr->mInFlightFlag) {
            return;
        }
        const int theRdSize = theOpPtr->mBuffer.BytesConsumable();
        do {
            if (0 <= mFd) {
                IOBuffer& theBuffer = theOpPtr->mBuffer;
                mPos += theBuffer.BytesConsumable();
                while (! theBuffer.IsEmpty()) {
                    const int theNWr = theBuffer.Write(mFd);
                    if (theNWr < 0) {
                        KFS_LOG_STREAM_ERROR <<
                            mFileName << ": write failure:" <<
                            QCUtils::SysError((int)-theNWr) <<
                        KFS_LOG_EOM;
                        HandleError();
                        return;
                    }
                    theBuffer.Consume(theNWr);
                }
            } else {
                if (theOpPtr->checkpointFlag || mCheckpointFlag) {
                    panic("metad data sync: attempt to replay checkpoint");
                } else {
                    SubmitLogBlock(theOpPtr->mBuffer);
                }
            }
            theOpPtr->mBuffer.Clear();
            mPendingList.PopFront();
            mFreeList.PutFront(*theOpPtr);
        } while (
            (theOpPtr = mPendingList.Front()) &&
            ! theOpPtr->mInFlightFlag);
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
    void HandleError(
        ReadOp& inReadOp)
    {
        KFS_LOG_STREAM_ERROR <<
            "status: " << inReadOp.statusMsg <<
            " try: "   << mRetryCount <<
            " pos:"
            " cur: "   << mPos <<
            " op: "    << inReadOp.mPos <<
            " "        << inReadOp.Show() <<
        KFS_LOG_EOM;
        HandleError();
    }
    void HandleError(
        MetaLogWriterControl& inOp)
    {
        inOp.status = -EINVAL;
        LogWriteDone(EVENT_CMD_DONE, &inOp);
        HandleError();
    }
    void HandleError()
    {
        if (++mRetryCount < mMaxRetryCount) {
        }
        Reset();
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
        mLogBuffer.Move(&inBuffer);
        for (; ;) {
            int thePos = mLogBuffer.IndexOf(0, "\nc/");
            if (thePos < 0) {
                break;
            }
            thePos++;
            const int theEndPos = mLogBuffer.IndexOf(thePos, "\n");
            if (theEndPos < 0) {
                break;
            }
            int theLen = theEndPos - theEndPos;
            if (kMaxCommitLineLen < theLen) {
                KFS_LOG_STREAM_ERROR <<
                    "log block commit line"
                    " length: "  << theLen <<
                    " exceeds: " << kMaxCommitLineLen <<
                    IOBuffer::DisplayData(mLogBuffer, 512) <<
                KFS_LOG_EOM;
                HandleError();
                return;
            }
            MetaLogWriterControl& theOp = GetLogWriteOpPtr();
            const int theRem = LogReceiver::ParseBlockLines(
                mLogBuffer, theEndPos + 1, theOp, '\n');
            if (0 != theRem || theOp.blockLines.IsEmpty()) {
                KFS_LOG_STREAM_ERROR <<
                    "invalid log block received:" <<
                    IOBuffer::DisplayData(mLogBuffer, 512) <<
                KFS_LOG_EOM;
                HandleError(theOp);
                return;
            }
            // Copy log block body, i.e. except trailing / commit line.
            theOp.blockData.Move(&mLogBuffer, thePos);
            const char* const theStartPtr =
                mLogBuffer.CopyOutOrGetBufPtr(mCommmitBuf, theLen);
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
                    IOBuffer::DisplayData(mLogBuffer, theLen) <<
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
                    " data: " << IOBuffer::DisplayData(mLogBuffer, theLen) <<
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
                    " data: " << IOBuffer::DisplayData(mLogBuffer, theLen) <<
                KFS_LOG_EOM;
                HandleError(theOp);
                return;
            }
            theOp.blockData.Move(&mLogBuffer, theCopyLen);
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
                &mLogBuffer, theBlkSeqLen, mCurBlockChecksum);
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
                mLogBuffer.Consume(theLen - theCopyLen + 1);
            mCurBlockChecksum = mNextBlockChecksum;
            if (theBlockSeqLen <= 0) {
                LogWriteDone(EVENT_CMD_DONE, &theOp);
            } else {
                submit_request(&theOp);
            }
        }
        if (mMaxLogBlockSize < mLogBuffer.BytesConsumable()) {
            KFS_LOG_STREAM_ERROR <<
                "log block size: " << mLogBuffer.BytesConsumable() <<
                " exceeds: "       << mMaxLogBlockSize <<
                " data: "          << IOBuffer::DisplayData(mLogBuffer, 256) <<
            KFS_LOG_EOM;
            HandleError();
        }
    }
    MetaLogWriterControl& GetLogWriteOpPtr()
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
        if (! inDataPtr || EVENT_CMD_DONE != inEvent) {
            panic("metad data sync: invalid log write completion");
            return -1;
        }
        MetaLogWriterControl& theOp = *reinterpret_cast<MetaLogWriterControl*>(
            inDataPtr);
        theOp.status = 0;
        theOp.statusMsg.clear();
        theOp.blockData.Clear();
        if (mReadOpsPtr) {
            mFreeWriteOpCount++;
            mFreeWriteOpList.PutFront(theOp);
        } else {
            MetaRequest::Release(&theOp);
        }
        return 0;
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
    int64_t        inFileSystemId,
    MetaDataStore& inMetaDataStore)
{
    return mImpl.Start(inFileSystemId, inMetaDataStore);
}

    void
MetaDataSync::Shutdown()
{
    mImpl.Shutdown();
}

} // namespace KFS
