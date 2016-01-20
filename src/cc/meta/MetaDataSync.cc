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
#include "util.h"

#include "qcdio/qcdebug.h"

#include "common/MsgLogger.h"
#include "common/SingleLinkedQueue.h"

#include "kfsio/NetManager.h"
#include "kfsio/ClientAuthContext.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/checksum.h"
#include "kfsio/ITimeout.h"

#include "libclient/KfsNetClient.h"
#include "libclient/KfsOps.h"

#include <algorithm>
#include <vector>

namespace KFS
{
using std::max;

using client::KfsNetClient;
using client::KfsOp;

class MetaDataSync::Impl : public KfsNetClient::OpOwner, public ITimeout
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
          mKfsNetClient(inNetManager),
          mServers(),
          mAuthContext(),
          mReadOpsPtr(0),
          mFreeList(),
          mPendingList(),
          mFileSystemId(-1),
          mReadOpsCount(16),
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
          mCheckpontFlag(false)
    {
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
    typedef vector<ServerLocation> Servers;

    KfsNetClient      mKfsNetClient;
    Servers           mServers;
    ClientAuthContext mAuthContext;
    ReadOp*           mReadOpsPtr;
    ReadQueue         mFreeList;
    ReadQueue         mPendingList;
    int64_t           mFileSystemId;
    size_t            mReadOpsCount;
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
    bool              mCheckpontFlag;

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
        mLogSeq         = -1;
        mCheckpontFlag  = true;
        mNextReadPos    = 0;
        mPos            = 0;
        mFileSize       = -1;
        mCurMaxReadSize = -1;
        if (! StartRead(mMaxReadSize)) {
            panic("metad data sync: failed to iniate checkpoint download");
            return -EFAULT;
        }
        return 0;
    }
    bool StartRead(
        int inMaxReadSize)
    {
        ReadOp* const theOpPtr = mFreeList.PopFront();
        if (! theOpPtr) {
            return false;
        }
        ReadOp& theOp = *theOpPtr;
        theOp.fileSystemId   = mFileSystemId;
        theOp.startLogSeq    = mLogSeq;
        theOp.readPos        = mNextReadPos;
        theOp.checkpointFlag = mCheckpontFlag;
        theOp.readSize       = inMaxReadSize;
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
        do {
            if (0 <= mFd) {
                mPos += theOpPtr->mBuffer.BytesConsumable();
                const int theNWr = theOpPtr->mBuffer.Write(mFd);
                if (theNWr < 0) {
                    KFS_LOG_STREAM_ERROR <<
                        "write failure:" <<
                        QCUtils::SysError((int)-theNWr) <<
                    KFS_LOG_EOM;
                }
            }
        } while (
            (theOpPtr = mPendingList.Front()) &&
            ! theOpPtr->mInFlightFlag);
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
        if (++mRetryCount < mMaxRetryCount) {
        }
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

}
