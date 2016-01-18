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

#include "common/MsgLogger.h"

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
using std::vector;

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
              mBuffer()
            {}
        int64_t  mPos;
        bool     mInFlightFlag;
        IOBuffer mBuffer;
    };
    typedef vector<ReadOp*> FreeList;
public:
    Impl(
        NetManager& inNetManager)
        : OpOwner(),
          ITimeout(),
          mKfsNetClient(inNetManager),
          mAuthContext(),
          mReadOpsPtr(0),
          mFreeList(),
          mFileSystemId(-1),
          mMaxReadSize(64 << 10),
          mReadOpsCount(16),
          mMaxRetryCount(10),
          mRetryCount(-1),
          mRetryTimeout(3),
          mFd(-1),
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
            mReadOpsCount = max(1, inParameters.getValue(
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
        mFreeList.clear();
        mFreeList.reserve(mReadOpsCount);
        mReadOpsPtr = new ReadOp[mReadOpsCount];
        for (int i = 0; i < mReadOpsCount; i++) {
            mFreeList.push_back(mReadOpsPtr + i);
        }
        return 0;
    }
    void Shutdown()
    {
        mKfsNetClient.Stop();
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
            mFreeList.push_back(&theOp);
            return;
        }
        if (0 <= theOp.status && theOp.mPos != theOp.readPos) {
            theOp.status    = -EINVAL;
            theOp.statusMsg = "invalid read position";
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
    KfsNetClient      mKfsNetClient;
    ClientAuthContext mAuthContext;
    ReadOp*           mReadOpsPtr;
    FreeList          mFreeList;
    int64_t           mFileSystemId;
    int               mMaxReadSize;
    int               mReadOpsCount;
    int               mMaxRetryCount;
    int               mRetryCount;
    int               mRetryTimeout;
    int               mFd;
    int64_t           mPos;
    int64_t           mFileSize;
    seq_t             mLogSeq;
    bool              mCheckpontFlag;

    ReadOp* GetOp()
    {
        if (mFreeList.empty()) {
            return 0;
        }
        ReadOp* const theRetPtr = mFreeList.back();
        mFreeList.pop_back();
        return theRetPtr;
    }
    bool StartRead(
        seq_t   inStartSeq,
        int64_t inStartPos,
        bool    inCheckpointFlag,
        int     inMaxReadSize)
    {
        ReadOp* const theOpPtr = GetOp();
        if (! theOpPtr) {
            return false;
        }
        ReadOp& theOp = *theOpPtr;
        theOp.fileSystemId   = mFileSystemId;
        theOp.startLogSeq    = inStartSeq;
        theOp.readPos        = inStartPos;
        theOp.checkpointFlag = inCheckpointFlag;
        theOp.readSize       = inMaxReadSize;
        theOp.mInFlightFlag  = true;
        theOp.mPos           = inStartPos;
        theOp.status         = 0;
        theOp.statusMsg.clear();
        theOp.mBuffer.Clear();
        if (! mKfsNetClient.Enqueue(&theOp, this, &theOp.mBuffer)) {
            panic("read op enqueue failure");
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
