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

#include "kfsio/NetManager.h"
#include "kfsio/ClientAuthContext.h"
#include "kfsio/IOBuffer.h"

#include "libclient/KfsNetClient.h"
#include "libclient/KfsOps.h"

namespace KFS
{
using client::KfsNetClient;
using client::KfsOp;

class MetaDataSync::Impl : public KfsNetClient::OpOwner
{
public:
    class ReadOp : public client::MetaReadMetaData
    {
    public:
        ReadOp()
            : client::MetaReadMetaData(-1),
              mInFlightFlag(false)
            {}
        bool     mInFlightFlag;
        IOBuffer mBuffer;
    };
    Impl(
        NetManager& inNetManager)
        : OpOwner(),
          mKfsNetClient(inNetManager),
          mAuthContext(),
          mReadOpsPtr()
    {
        mKfsNetClient.SetAuthContext(&mAuthContext);
    }
    virtual ~Impl()
        { Impl::Shutdown(); }
    int SetParameters(
        const char*       inParamPrefixPtr,
        const Properties& inParameters)
    {
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
        MetaDataStore& inMetaDataStore)
    {
        return 0;
    }
    void Shutdown()
    {
        mKfsNetClient.Stop();
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
            return;
        }
    }
private:
    KfsNetClient      mKfsNetClient;
    ClientAuthContext mAuthContext;
    ReadOp*           mReadOpsPtr;
    int               mReadOpsCount;
    int               mNextOpIdx;
    
    ReadOp* GetOp()
    {
        if (mReadOpsCount <= 0) {
            return 0;
        }
        if (mReadOpsCount <= mNextOpIdx) {
            mNextOpIdx = 0;
        }
        ReadOp& theOp = mReadOpsPtr[mNextOpIdx];
        if (theOp.mInFlightFlag) {
            return 0;
        }
        mNextOpIdx++;
        return &theOp;
    }
    int ReceiveLog(
        seq_t inStartSeq)
    {
        ReadOp* const theOpPtr = GetOp();
        if (! theOpPtr) {
            return -EFAULT;
        }
        theOpPtr->mBuffer.Clear();
        return mKfsNetClient.Enqueue(theOpPtr, this, &theOpPtr->mBuffer);
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
    MetaDataStore& inMetaDataStore)
{
    return mImpl.Start(inMetaDataStore);
}

    void
MetaDataSync::Shutdown()
{
    mImpl.Shutdown();
}

}
