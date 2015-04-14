//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2010/08/13
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

#ifndef READER_H
#define READER_H

#include "KfsNetClient.h"
#include "common/kfstypes.h"

#include <string>

namespace KFS
{
class IOBuffer;

namespace client
{

using std::string;
using std::ostream;

class ClientPool;

// Kfs client file read state machine.
class Reader
{
public:
    typedef int64_t Offset;
    union RequestId
    {
        int64_t mId;
        void*   mPtr;
    };
    class Impl;

    class Completion
    {
    public:
        virtual void Done(
            Reader&   inReader,
            int       inStatusCode,
            Offset    inOffset,
            Offset    inSize,
            IOBuffer* inBufferPtr,
            RequestId inRequestId) = 0;
        virtual void Unregistered(
            Reader& /* inReader */)
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
              mGetLeaseCount(0),
              mGetLeaseRetryCount(0),
              mOpsReadCount(0),
              mRetriesCount(0),
              mReadCount(0),
              mReadByteCount(0),
              mReadErrorsCount(0),
              mReadChecksumErrorsCount(0),
              mReadRecoveriesCount(0)
            {}
        void Clear()
            { *this = Stats(); }
        Stats& Add(
            const Stats& inStats)
        {
            mMetaOpsQueuedCount      += inStats.mMetaOpsQueuedCount;
            mMetaOpsCancelledCount   += inStats.mMetaOpsCancelledCount;
            mChunkOpsQueuedCount     += inStats.mChunkOpsQueuedCount;
            mSleepTimeSec            += inStats.mSleepTimeSec;
            mGetLeaseCount           += inStats.mGetLeaseCount;
            mGetLeaseRetryCount      += inStats.mGetLeaseRetryCount;
            mOpsReadCount            += inStats.mOpsReadCount;
            mRetriesCount            += inStats.mRetriesCount;
            mReadCount               += inStats.mReadCount;
            mReadByteCount           += inStats.mReadByteCount;
            mReadErrorsCount         += inStats.mReadErrorsCount;
            mReadChecksumErrorsCount += inStats.mReadChecksumErrorsCount;
            mReadRecoveriesCount     += inStats.mReadRecoveriesCount;
            return *this;
        }
        template<typename T>
        void Enumerate(
            T& inFunctor) const
        {
            inFunctor("MetaOpsQueued",      mMetaOpsQueuedCount);
            inFunctor("MetaOpsCancelled",   mMetaOpsCancelledCount);
            inFunctor("ChunkOpsQueued",     mChunkOpsQueuedCount);
            inFunctor("SleepTimeSec",       mSleepTimeSec);
            inFunctor("GetLeaseCount",      mGetLeaseCount);
            inFunctor("OpsRead",            mOpsReadCount);
            inFunctor("Retries",            mRetriesCount);
            inFunctor("ReadErrors",         mReadErrorsCount);
            inFunctor("ReadChecksumErrors", mReadChecksumErrorsCount);
            inFunctor("ReadRecoveries",     mReadRecoveriesCount);
            inFunctor("Reads",              mReadCount);
            inFunctor("ReadBytes",          mReadByteCount);
        }
        Counter mMetaOpsQueuedCount;
        Counter mMetaOpsCancelledCount;
        Counter mChunkOpsQueuedCount;
        Counter mSleepTimeSec;
        Counter mGetLeaseCount;
        Counter mGetLeaseRetryCount;
        Counter mOpsReadCount;
        Counter mRetriesCount;
        Counter mReadCount;
        Counter mReadByteCount;
        Counter mReadErrorsCount;
        Counter mReadChecksumErrorsCount;
        Counter mReadRecoveriesCount;
    };
    class Striper
    {
    public:
        typedef Reader::Impl      Impl;
        typedef Reader::Offset    Offset;
        typedef Reader::RequestId RequestId;
        typedef int64_t           SeqNum;
        static Striper* Create(
            int           inType,
            int           inStripeCount,
            int           inRecoveryStripeCount,
            int           inStripeSize,
            int           inMaxAtomicReadRequestSize,
            bool          inUseDefaultBufferAllocatorFlag,
            bool          inFailShortReadsFlag,
            Offset        inRecoverChunkPos,
            Offset        inFileSize,
            SeqNum        inInitialSeqNum,
            const string& inLogPrefix,
            Impl&         inOuter,
            Offset&       outOpenChunkBlockSize,
            string&       outErrMsg);
        virtual ~Striper()
            {}
        virtual int Process(
            IOBuffer& inBuffer,
            int       inLength,
            Offset    inOffset,
            RequestId inRequestId) = 0;
        virtual void ReadCompletion(
            int          inStatus,
            IOBuffer&    inBuffer,
            int          inLength,
            Offset       inOffset,
            RequestId    inRequestId,
            RequestId    inStriperRequestId,
            kfsChunkId_t inChunkId,
            int64_t      inChunkVersion,
            int64_t      inChunkSize) = 0;
        virtual bool CanCancelRead(
            RequestId inStriperRequestId) = 0;
        virtual bool IsIdle() const = 0;
    protected:
        Striper(
            Impl& inOuter)
            : mOuter(inOuter)
            {}
        int QueueRead(
            IOBuffer& inBuffer,
            int       inLength,
            Offset    inOffset,
            RequestId inOriginalRequestId,
            RequestId inRequestId,
            bool      inRetryIfFailsFlag,
            bool      inFailShortReadFlag);
        void StartQueuedRead(
            int inQueuedCount);
        bool ReportCompletion(
            int       inStatus,
            IOBuffer& inBuffer,
            int       inLength,
            Offset    inOffset,
            RequestId inRequestId,
            int64_t   inRecoveriesCount);
        void CancelRead();
        void ReportInvalidChunk(
            kfsChunkId_t inChunkId,
            int64_t      inChunkVersion,
            int          inStatus,
            const char*  inStatusMsgPtr);
    private:
        Impl& mOuter;
    private:
        Striper(
            const Striper& inStriper);
        Striper& operator=(
            const Striper& inStipter);
    };
    typedef KfsNetClient MetaServer;
    Reader(
        MetaServer& inMetaServer,
        Completion* inCompletionPtr            = 0,
        int         inMaxRetryCount            = 6,
        int         inTimeSecBetweenRetries    = 15,
        int         inOpTimeoutSec             = 30,
        int         inIdleTimeoutSec           = 5 * 30,
        int         inMaxReadSize              = 1 << 20,
        int         inLeaseRetryTimeout        = 3,
        int         inLeaseWaitTimeout         = 900,
        const char* inLogPrefixPtr             = 0,
        int64_t     inChunkServerInitialSeqNum = 1,
        ClientPool* inClientPoolPtr            = 0);
    virtual ~Reader();
    int Open(
        kfsFileId_t inFileId,
        const char* inFileNamePtr,
        Offset      inFileSize,
        int         inStriperType,
        int         inStripeSize,
        int         inStripeCount,
        int         inRecoveryStripeCount,
        bool        inSkipHolesFlag,
        bool        inUseDefaultBufferAllocatorFlag = false,
        Offset      inRecoverChunkPos               = -1,
        bool        inFailShortReadsFlag            = false);
    int Close();
    int Read(
        IOBuffer& inBuffer,
        int       inLength,
        Offset    inOffset,
        RequestId inRequestId);
    void Stop();
    void Shutdown();
    bool IsOpen()    const;
    bool IsClosing() const;
    bool IsActive()  const;
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
    Reader(
        const Reader& inReader);
    Reader& operator=(
        const Reader& inReader);
};
}}

#endif /* READER_H */
