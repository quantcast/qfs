//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2010/06/22
// Author: Mike Ovsiannikov
//
// Copyright 2010-2012 Quantcast Corp.
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

#ifndef WRITER_H
#define WRITER_H

#include "KfsNetClient.h"
#include "common/kfstypes.h"

#include <string>

namespace KFS
{

class IOBuffer;

namespace client
{
using std::string;

// Kfs client write protocol state machine.
class Writer
{
public:
    typedef int64_t Offset;
    class Impl;

    class Completion
    {
    public:
        // For striped files inOffset and inSize parameters are the file
        // offset and the size of the stripe (possibly recovery stripe), not
        // the enqueued write request file offset and size.
        virtual void Done(
            Writer& inWriter,
            int     inStatusCode,
            Offset  inOffset,
            Offset  inSize) = 0;
        virtual void Unregistered(
            Writer& /* inWriter */)
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
              mChunkOpsQueuedCount(0),
              mSleepTimeSec(0),
              mChunkAllocCount(0),
              mOpsWriteCount(0),
              mAllocRetriesCount(0),
              mRetriesCount(0),
              mWriteCount(0),
              mWriteByteCount(0),
              mBufferCompactionCount(0)
            {}
        void Clear()
            { *this = Stats(); }
        Stats& Add(
            const Stats& inStats)
        {
            mMetaOpsQueuedCount    += inStats.mMetaOpsQueuedCount;
            mMetaOpsCancelledCount += inStats.mMetaOpsCancelledCount;
            mChunkOpsQueuedCount   += inStats.mChunkOpsQueuedCount;
            mSleepTimeSec          += inStats.mSleepTimeSec;
            mChunkAllocCount       += inStats.mChunkAllocCount;
            mOpsWriteCount         += inStats.mOpsWriteCount;
            mAllocRetriesCount     += inStats.mAllocRetriesCount;
            mRetriesCount          += inStats.mRetriesCount;
            mWriteCount            += inStats.mWriteCount;
            mWriteByteCount        += inStats.mWriteByteCount;
            mBufferCompactionCount += inStats.mBufferCompactionCount;
            return *this;
        }
        template<typename T>
        void Enumerate(
            T& inFunctor) const
        {
            inFunctor("MetaOpsQueued",    mMetaOpsQueuedCount);
            inFunctor("MetaOpsCancelled", mMetaOpsCancelledCount);
            inFunctor("ChunkOpsQueued",   mChunkOpsQueuedCount);
            inFunctor("SleepTimeSec",     mSleepTimeSec);
            inFunctor("ChunkAlloc",       mChunkAllocCount);
            inFunctor("OpsWrite",         mOpsWriteCount);
            inFunctor("AllocRetries",     mAllocRetriesCount);
            inFunctor("BufferCompaction", mBufferCompactionCount);
            inFunctor("Retries",          mRetriesCount);
            inFunctor("Writes" ,          mWriteCount);
            inFunctor("WriteBytes",       mWriteByteCount);
        }
        Counter mMetaOpsQueuedCount;
        Counter mMetaOpsCancelledCount;
        Counter mChunkOpsQueuedCount;
        Counter mSleepTimeSec;
        Counter mChunkAllocCount;
        Counter mOpsWriteCount;
        Counter mAllocRetriesCount;
        Counter mRetriesCount;
        Counter mWriteCount;
        Counter mWriteByteCount;
        Counter mBufferCompactionCount;
    };
    class Striper
    {
    public:
        typedef Writer::Impl   Impl;
        typedef Writer::Offset Offset;
        static Striper* Create(
            int           inType,
            int           inStripeCount,
            int           inRecoveryStripeCount,
            int           inStripeSize,
            Offset        inFileSize,
            const string& inLogPrefix,
            Impl&         inOuter,
            Offset&       outOpenChunkBlockSize,
            std::string&  outErrMsg);
        virtual ~Striper()
            {}
        virtual int Process(
            IOBuffer& inBuffer,
            Offset&   ioOffset,
            int       inWriteThreshold) = 0;
        virtual Offset GetPendingSize() const = 0;
        virtual bool IsWriteRetryNeeded(
            Offset inChunkOffset,
            int    inRetryCount,
            int    inMaxRetryCount,
            int&   ioStatus) = 0;
        virtual Offset GetFileSize() const = 0;
    protected:
        Striper(
            Impl& inOuter)
            : mOuter(inOuter),
              mWriteQueuedFlag(false)
            {}
        int QueueWrite(
            IOBuffer& inBuffer,
            int       inSize,
            Offset    inOffset,
            int       inWriteThreshold);
        void StartQueuedWrite(
            int inQueuedCount);
        bool IsWriteQueued() const
            { return mWriteQueuedFlag; }
    private:
        Impl& mOuter;
        bool  mWriteQueuedFlag;
    private:
        Striper(
            const Striper& inStriper);
        Striper& operator=(
            const Striper& inStipter);
    };
    typedef KfsNetClient MetaServer;
    Writer(
        MetaServer& inMetaServer,
        Completion* inCompletionPtr            = 0,
        int         inMaxRetryCount            = 6,
        int         inWriteThreshold           = 1 << 20,
        int         inMaxPartialBuffersCount   = 16,
        int         inTimeSecBetweenRetries    = 15,
        int         inOpTimeoutSec             = 30,
        int         inIdleTimeoutSec           = 5 * 30,
        int         inMaxWriteSize             = 1 << 20,
        const char* inLogPrefixPtr             = 0,
        int64_t     inChunkServerInitialSeqNum = 1);
    virtual ~Writer();
    int Open(
        kfsFileId_t inFileId,
        const char* inFileNamePtr,
        Offset      inFileSize,
        int         inStriperType,
        int         inStripeSize,
        int         inStripeCount,
        int         inRecoveryStripeCount,
        int         inReplicaCount);
    int Close();
    int Write(
        IOBuffer& inBuffer,
        int       inLength,
        Offset    inOffset,
        bool      inFlushFlag,
        int       inWriteThreshold = -1);
    int SetWriteThreshold(
        int inThreshold);
    int Flush();
    void Stop();
    void Shutdown();
    bool IsOpen()    const;
    bool IsClosing() const;
    bool IsActive()  const;
    Offset GetPendingSize() const;
    int GetErrorCode() const;
    void Register(
        Completion* inCompletionPtr);
    bool Unregister(
        Completion* inCompletionPtr);
    void GetStats(
        Stats&               outStats,
        KfsNetClient::Stats& outChunkServersStats) const;
private:
    Impl& mImpl;
private:
    Writer(
        const Writer& inWriter);
    Writer& operator=(
        const Writer& inWriter);
};
}}

#endif /* WRITER_H */
