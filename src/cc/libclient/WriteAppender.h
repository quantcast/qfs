//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2009/05/20
// Author: Mike Ovsiannikov
//
// Copyright 2009-2011 Quantcast Corp.
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

#ifndef WRITE_APPENDER_H
#define WRITE_APPENDER_H

#include "KfsNetClient.h"
#include "common/kfstypes.h"
#include "kfsio/checksum.h"

#include <stdint.h>

namespace KFS
{
class IOBuffer;

namespace client
{

class ClientPool;

// Kfs client write append state machine.
class WriteAppender
{
public:
    class Completion
    {
    public:
        virtual void Done(
            WriteAppender& inAppender,
            int            inStatusCode) = 0;
        virtual void Unregistered(
            WriteAppender& /* inAppender */)
            {}
    protected:
        Completion()
            {}
        Completion(
            const Completion&)
            {}
        virtual ~Completion()
            {}
    };
    struct Stats
    {
        typedef int64_t Counter;
        Stats()
            : mMetaOpsQueuedCount(0),
              mMetaOpsCancelledCount(0),
              mSleepTimeSec(0),
              mChunkAllocCount(0),
              mReserveSpaceCount(0),
              mReserveSpaceDeniedCount(0),
              mOpsRecAppendCount(0),
              mAllocRetriesCount(0),
              mRetriesCount(0),
              mBufferCompactionCount(0),
              mAppendCount(0),
              mAppendByteCount(0)
            {}
        void Clear()
            { *this = Stats(); }
        Stats& Add(
            const Stats& inStats)
        {
            mMetaOpsQueuedCount      += inStats.mMetaOpsQueuedCount;
            mMetaOpsCancelledCount   += inStats.mMetaOpsCancelledCount;
            mSleepTimeSec            += inStats.mSleepTimeSec;
            mChunkAllocCount         += inStats.mChunkAllocCount;
            mReserveSpaceCount       += inStats.mReserveSpaceCount;
            mReserveSpaceDeniedCount += inStats.mReserveSpaceDeniedCount;
            mOpsRecAppendCount       += inStats.mOpsRecAppendCount;
            mAllocRetriesCount       += inStats.mAllocRetriesCount;
            mRetriesCount            += inStats.mRetriesCount;
            mBufferCompactionCount   += inStats.mBufferCompactionCount;
            mAppendCount             += inStats.mAppendCount;
            mAppendByteCount         += inStats.mAppendByteCount;
            return *this;
        }
        template<typename T>
        void Enumerate(
            T& inFunctor) const
        {
            inFunctor("MetaOpsQueued",      mMetaOpsQueuedCount);
            inFunctor("MetaOpsCancelled",   mMetaOpsCancelledCount);
            inFunctor("SleepTimeSec",       mSleepTimeSec);
            inFunctor("ChunkAlloc",         mChunkAllocCount);
            inFunctor("ReserveSpace",       mReserveSpaceCount);
            inFunctor("ReserveSpaceDenied", mReserveSpaceDeniedCount);
            inFunctor("OpsRecAppend",       mOpsRecAppendCount);
            inFunctor("AllocRetries",       mAllocRetriesCount);
            inFunctor("Retries",            mRetriesCount);
            inFunctor("BufferCompaction",   mBufferCompactionCount);
            inFunctor("AppendCount",        mAppendCount);
            inFunctor("AppendByteCount",    mAppendByteCount);
        }
        Counter mMetaOpsQueuedCount;
        Counter mMetaOpsCancelledCount;
        Counter mSleepTimeSec;
        Counter mChunkAllocCount;
        Counter mReserveSpaceCount;
        Counter mReserveSpaceDeniedCount;
        Counter mOpsRecAppendCount;
        Counter mAllocRetriesCount;
        Counter mRetriesCount;
        Counter mBufferCompactionCount;
        Counter mAppendCount;
        Counter mAppendByteCount;
    };
    typedef KfsNetClient MetaServer;
    WriteAppender(
        MetaServer& inMetaServer,
        Completion* inCompletionPtr               = 0,
        int         inMaxRetryCount               = 6,
        int         inWriteThreshold              = KFS::CHECKSUM_BLOCKSIZE,
        int         inTimeSecBetweenRetries       = 15,
        int         inDefaultSpaceReservationSize = 1 << 20,
        int         inPreferredAppendSize         = KFS::CHECKSUM_BLOCKSIZE,
        int         inMaxPartialBuffersCount      = 16,
        int         inOpTimeoutSec                = 30,
        int         inIdleTimeoutSec              = 5 * 30,
        const char* inLogPrefixPtr                = 0,
        int64_t     inChunkServerInitialSeqNum    = 1,
        bool        inPreAllocationFlag           = true,
        ClientPool* inClientPoolPtr               = 0);
    virtual ~WriteAppender();
    int Open(
        const char* inFileNamePtr,
        int         inNumReplicas  = 3,
        bool        inMakeDirsFlag = false,
        kfsSTier_t  inMinSTier     = kKfsSTierMax,
        kfsSTier_t  inMaxSTier     = kKfsSTierMax);
    int Open(
        kfsFileId_t inFileId,
        const char* inFileNamePtr,
        kfsSTier_t  inMinSTier = kKfsSTierMax,
        kfsSTier_t  inMaxSTier = kKfsSTierMax);
    int Close();
    int Append(
        IOBuffer& inBuffer,
        int       inLength);
    void Shutdown();
    bool IsOpen()     const;
    bool IsOpening()  const;
    bool IsClosing()  const;
    bool IsSleeping() const;
    bool IsActive()   const;
    int GetPendingSize() const;
    int GetErrorCode() const;
    int SetWriteThreshold(
        int inThreshold);
    void Register(
        Completion* inCompletionPtr);
    bool Unregister(
        Completion* inCompletionPtr);
    void GetStats(
        Stats&               outStats,
        KfsNetClient::Stats& outChunkServersStats) const;
    const ServerLocation& GetServerLocation() const;
    int SetPreAllocation(
        bool inFlag);
    bool GetPreAllocation() const;
    void SetForcedAllocationInterval(
        int inInterval);
private:
    class Impl;
    Impl& mImpl;
private:
    WriteAppender(
        const WriteAppender& inAppender);
    WriteAppender& operator=(
        const WriteAppender& inAppender);
};
}}

#endif /* WRITE_APPENDER_H */
