//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2009/10/10
// Author: Mike Ovsiannikov
//
// Copyright 2009-2012 Quantcast Corp.
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

#ifndef KFS_PROTOCOL_WORKER_H
#define KFS_PROTOCOL_WORKER_H

#include "common/kfstypes.h"
#include "kfsio/checksum.h"
#include "qcdio/QCDLList.h"

#include <cerrno>
#include <string>

namespace KFS
{
class ClientAuthContext;
class Properties;

namespace client
{
using std::string;

struct KfsOp;
// KFS client side protocol worker thread runs client side network io state
// machines.
class KfsProtocolWorker
{
private:
    class Impl;
public:
    enum
    {
        kErrNone       = 0,
        kErrParameters = -EINVAL,
        kErrProtocol   = -EBADF,
        kErrShutdown   = -91010
    };
    enum RequestType
    {
        kRequestTypeUnknown                      = 0,
        kRequestTypeWriteAppend                  = 1,
        kRequestTypeWriteAppendClose             = 2,
        kRequestTypeWriteAppendShutdown          = 3,
        kRequestTypeWriteAppendSetWriteThreshold = 4,
        kRequestTypeWriteAppendAsync             = 5,
        kRequestTypeWriteAppendAsyncNoCopy       = 6,
        kRequestTypeWriteAppendThrottle          = 7,
        kRequestTypeWrite                        = 20,
        kRequestTypeWriteClose                   = 21,
        kRequestTypeWriteAsync                   = 22,
        kRequestTypeWriteAsyncNoCopy             = 23,
        kRequestTypeWriteThrottle                = 24,
        kRequestTypeWriteShutdown                = 25,
        kRequestTypeWriteSetWriteThreshold       = 26,
        kRequestTypeRead                         = 27,
        kRequestTypeReadAsync                    = 28,
        kRequestTypeReadClose                    = 29,
        kRequestTypeReadShutdown                 = 30,
        kRequestTypeMetaOp                       = 31, // Internal use only
        kRequestTypeGetStatsOp                   = 32  // Internal use only
    };
    typedef kfsFileId_t  FileId;
    typedef unsigned int FileInstance;
    class Request
    {
    public:
        struct Params
        {
            Params(
                string  inPathName            = string(),
                int64_t inFileSize            = -1,
                int     inStriperType         = -1,
                int     inStripeSize          = 0,
                int     inStripeCount         = 0,
                int     inRecoveryStripeCount = 0,
                int     inReplicaCount        = 0,
                bool    inSkipHolesFlag       = false,
                int     inMsgLogId            = -1,
                bool    inFailShortReadsFlag  = false)
                : mPathName(inPathName),
                  mFileSize(inFileSize),
                  mStriperType(inStriperType),
                  mStripeSize(inStripeSize),
                  mStripeCount(inStripeCount),
                  mRecoveryStripeCount(inRecoveryStripeCount),
                  mReplicaCount(inReplicaCount),
                  mSkipHolesFlag(inSkipHolesFlag),
                  mMsgLogId(inMsgLogId),
                  mFailShortReadsFlag(inFailShortReadsFlag)
                {}

            string  mPathName;
            int64_t mFileSize;
            int     mStriperType;
            int     mStripeSize;
            int     mStripeCount;
            int     mRecoveryStripeCount;
            int     mReplicaCount;
            bool    mSkipHolesFlag;
            int     mMsgLogId;
            bool    mFailShortReadsFlag;
        };
        Request(
            RequestType   inOpType       = kRequestTypeUnknown,
            FileInstance  inFileInstance = 0,
            FileId        inFileId       = -1,
            const Params* inParamsPtr    = 0,
            void*         inBufferPtr    = 0,
            int           inSize         = 0,
            int           inMaxPending   = -1,
            int64_t       inOffset       = -1);
        void Reset(
            RequestType   inOpType       = kRequestTypeUnknown,
            FileInstance  inFileInstance = 0,
            FileId        inFileId       = -1,
            const Params* inParamsPtr    = 0,
            void*         inBufferPtr    = 0,
            int           inSize         = 0,
            int           inMaxPending   = -1,
            int64_t       inOffset       = -1);
        virtual void Done(
            int64_t inStatus) = 0;
        int64_t GetOffset() const
            { return mOffset; }
        int GetSize() const
            { return mSize; }
        void* GetBufferPtr() const
            { return mBufferPtr; }
    protected:
        virtual ~Request();
    private:
        enum State
        {
            kStateNone     = 0,
            kStateInFlight = 1,
            kStateDone     = 2,
            kStateDeleted  = 3
        };
        RequestType   mRequestType;
        FileInstance  mFileInstance;
        FileId        mFileId;
        const Params* mParamsPtr;
        void*         mBufferPtr;
        int           mSize;
        State         mState;
        int64_t       mStatus;
        int64_t       mMaxPendingOrEndPos;
        int64_t       mOffset;
    private:
        Request* mPrevPtr[1];
        Request* mNextPtr[1];
        friend class QCDLListOp<Request, 0>;
        friend class Impl;
        friend class KfsProtocolWorker;
    private:
        Request(
            const Request& inReq);
        Request& operator=(
            const Request& inReq);
    };
    class Parameters
    {
    public:
        Parameters(
            int                inMetaMaxRetryCount           = 6,
            int                inMetaTimeSecBetweenRetries   = 10,
            int                inMetaOpTimeoutSec            = 3 * 60,
            int                inMetaIdleTimeoutSec          = 5 * 60,
            int64_t            inMetaInitialSeqNum           = 0,
            const char*        inMetaLogPrefixPtr            = 0,
            int                inMaxRetryCount               = 10,
            int                inWriteAppendThreshold        = KFS::CHECKSUM_BLOCKSIZE,
            int                inTimeSecBetweenRetries       = 15,
            int                inDefaultSpaceReservationSize = 1 << 20,
            int                inPreferredAppendSize         = KFS::CHECKSUM_BLOCKSIZE,
            int                inOpTimeoutSec                = 120,
            int                inIdleTimeoutSec              = 5 * 30,
            const char*        inLogPrefixPtr                = 0,
            int64_t            inChunkServerInitialSeqNum    = 0,
            bool               inPreAllocateFlag             = false,
            int                inMaxWriteSize                = 1 << 20,
            int                inRandomWriteThreshold        = 1 << 20,
            int                inMaxReadSize                 = 1 << 20,
            int                inReadLeaseRetryTimeout       = 3,
            int                inLeaseWaitTimeout            = 900,
            int                inMaxMetaServerContentLength  = 1 << 20,
            ClientAuthContext* inAuthContextPtr              = 0,
            bool               inUseClientPoolFlag           = false)
            : mMetaMaxRetryCount(inMetaMaxRetryCount),
              mMetaTimeSecBetweenRetries(inMetaTimeSecBetweenRetries),
              mMetaOpTimeoutSec(inMetaOpTimeoutSec),
              mMetaIdleTimeoutSec(inMetaIdleTimeoutSec),
              mMetaInitialSeqNum(inMetaInitialSeqNum),
              mMetaLogPrefixPtr(inMetaLogPrefixPtr),
              mMaxRetryCount(inMaxRetryCount),
              mWriteAppendThreshold(inWriteAppendThreshold),
              mTimeSecBetweenRetries(inTimeSecBetweenRetries),
              mDefaultSpaceReservationSize(inDefaultSpaceReservationSize),
              mPreferredAppendSize(inPreferredAppendSize),
              mOpTimeoutSec(inOpTimeoutSec),
              mIdleTimeoutSec(inIdleTimeoutSec),
              mLogPrefixPtr(inLogPrefixPtr),
              mChunkServerInitialSeqNum(inChunkServerInitialSeqNum),
              mPreAllocateFlag(inPreAllocateFlag),
              mMaxWriteSize(inMaxWriteSize),
              mRandomWriteThreshold(inRandomWriteThreshold),
              mMaxReadSize(inMaxReadSize),
              mReadLeaseRetryTimeout(inReadLeaseRetryTimeout),
              mLeaseWaitTimeout(inLeaseWaitTimeout),
              mMaxMetaServerContentLength(inMaxMetaServerContentLength),
              mAuthContextPtr(inAuthContextPtr),
              mUseClientPoolFlag(inUseClientPoolFlag)
            {}
            int                 mMetaMaxRetryCount;
            int                 mMetaTimeSecBetweenRetries;
            int                 mMetaOpTimeoutSec;
            int                 mMetaIdleTimeoutSec;
            int64_t             mMetaInitialSeqNum;
            const char*         mMetaLogPrefixPtr;
            int                 mMaxRetryCount;
            int                 mWriteAppendThreshold;
            int                 mTimeSecBetweenRetries;
            int                 mDefaultSpaceReservationSize;
            int                 mPreferredAppendSize;
            int                 mOpTimeoutSec;
            int                 mIdleTimeoutSec;
            const char*         mLogPrefixPtr;
            int64_t             mChunkServerInitialSeqNum;
            bool                mPreAllocateFlag;
            int                 mMaxWriteSize;
            int                 mRandomWriteThreshold;
            int                 mMaxReadSize;
            int                 mReadLeaseRetryTimeout;
            int                 mLeaseWaitTimeout;
            int                 mMaxMetaServerContentLength;
            ClientAuthContext*  mAuthContextPtr;
            bool                mUseClientPoolFlag;
    };
    KfsProtocolWorker(
        std::string       inMetaHost,
        int               inMetaPort,
        const Parameters* inParametersPtr = 0);
    ~KfsProtocolWorker();
    int64_t Execute(
        RequestType            inRequestType,
        FileInstance           inFileInstance,
        FileId                 inFileId,
        const Request::Params* inParamsPtr  = 0,
        void*                  inBufferPtr  = 0,
        int                    inSize       = 0,
        int                    inMaxPending = -1,
        int64_t                inOffset     = -1);
    void ExecuteMeta(
        KfsOp& inOp);
    Properties GetStats();
    void Enqueue(
        Request& inRequest);
    void Start();
    void Stop();
    void SetMetaMaxRetryCount(
        int inMaxRetryCount);
    void SetMetaTimeSecBetweenRetries(
        int inSecs);
    // The following two might not have effect on already opened files.
    void SetMaxRetryCount(
        int inMaxRetryCount);
    void SetTimeSecBetweenRetries(
        int inSecs);
    void SetMetaOpTimeoutSec(
        int inSecs);
    void SetOpTimeoutSec(
        int inSecs);
private:
    Impl& mImpl;
private:
    KfsProtocolWorker(
        const KfsProtocolWorker& inWorker);
    KfsProtocolWorker& operator=(
        const KfsProtocolWorker& inWorker);
};

}}

#endif /* KFS_PROTOCOL_WORKER_H */
