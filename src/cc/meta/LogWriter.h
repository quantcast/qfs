//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/03/21
// Author: Mike Ovsiannikov
//
// Copyright 2015 Quantcast Corp.
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
// Transaction log writer.
//
//
//----------------------------------------------------------------------------

#ifndef KFS_META_LOG_WRITER_H
#define KFS_META_LOG_WRITER_H

#include "common/kfstypes.h"

#include <time.h>

#include <string>

namespace KFS
{
using std::string;

struct ServerLocation;
struct MetaRequest;
class UniqueID;
class Properties;
class NetManager;
class MetaDataStore;
class MetaVrSM;
class MetaDataSync;
class MetaVrLogSeq;
class Replay;

class LogWriter
{
public:
    enum { VERSION = 1 };
    class Counters
    {
    public:
        typedef int64_t Counter;
        enum { kRateFracBits = 8 };

        Counters()
            : mLogTimeUsec(0),
              mLogTimeOpsCount(0),
              mLogErrorOpsCount(0),
              mPendingOpsCount(0),
              mLog5SecAvgUsec(0),
              mLog10SecAvgUsec(0),
              mLog15SecAvgUsec(0),
              mLog5SecAvgReqRate(0),
              mLog10SecAvgReqRate(0),
              mLog15SecAvgReqRate(0),
              mDiskWriteTimeUsec(0),
              mDiskWriteByteCount(0),
              mDiskWriteCount(0),
              mLogOpWrite5SecAvgUsec(0),
              mLogOpWrite10SecAvgUsec(0),
              mLogOpWrite15SecAvgUsec(0),
              mExceedLogQueueDepthFailureCount(0)
        {}
        Counter mLogTimeUsec;
        Counter mLogTimeOpsCount;
        Counter mLogErrorOpsCount;
        Counter mPendingOpsCount;
        Counter mLog5SecAvgUsec;
        Counter mLog10SecAvgUsec;
        Counter mLog15SecAvgUsec;
        Counter mLog5SecAvgReqRate;
        Counter mLog10SecAvgReqRate;
        Counter mLog15SecAvgReqRate;
        Counter mDiskWriteTimeUsec;
        Counter mDiskWriteByteCount;
        Counter mDiskWriteCount;
        Counter mLogOpWrite5SecAvgUsec;
        Counter mLogOpWrite10SecAvgUsec;
        Counter mLogOpWrite15SecAvgUsec;
        Counter mExceedLogQueueDepthFailureCount;
    };

    LogWriter();
    ~LogWriter();
    int Start(
        NetManager&           inNetManager,
        MetaDataStore&        inMetaDataStore,
        MetaDataSync&         inMetaDataSync,
        const UniqueID&       inFileId,
        Replay&               inReplayer,
        seq_t                 inLogNum,
        const char*           inParametersPrefixPtr,
        const Properties&     inParameters,
        int64_t               inFileSystemId,
        const ServerLocation& inDataStoreLocation,
        const string&         inMetaMd,
        const char*           inVrResetTypeStrPtr,
        string&               outCurLogFileName);
    bool Enqueue(
        MetaRequest& inRequest);
    void Committed(
        MetaRequest& inRequest,
        fid_t        inFidSeed);
    void GetCommitted(
        MetaVrLogSeq& outLogSeq,
        int64_t&      outErrChecksum,
        fid_t&        outFidSeed,
        int&          outStatus) const;
    void SetCommitted(
        const MetaVrLogSeq& inLogSeq,
        int64_t             inErrChecksum,
        fid_t               inFidSeed,
        int                 inStatus,
        const MetaVrLogSeq& inLastReplayLogSeq,
        const MetaVrLogSeq& inViewStartSeq);
    MetaVrLogSeq GetCommittedLogSeq() const;
    void ScheduleFlush();
    void PrepareToFork();
    void ForkDone();
    void ChildAtFork();
    void Shutdown();
    seq_t GetNextSeq()
        { return ++mNextSeq; }
    MetaVrSM& GetMetaVrSM();
    int GetVrStatus() const
        { return mVrStatus; }
    int64_t GetPrimaryLeaseEndTimeUsec() const
        { return mPrimaryLeaseEndTimeUsec; }
    bool IsPrimary(
        int64_t inTimeNowUsec) const
    {
        return (0 == mVrStatus && inTimeNowUsec < mPrimaryLeaseEndTimeUsec);
    }
    int WriteNewLogSegment(
        const char*   inLogDirPtr,
        const Replay& inReplayer,
        string&       outLogSegmentFileName);
    void GetCounters(
        Counters& outCounters);
private:
    class Impl;
    seq_t            mNextSeq;
    volatile int     mVrStatus;
    volatile int64_t mPrimaryLeaseEndTimeUsec;
    Impl&            mImpl;
};

} // namespace KFS

#endif /* KFS_META_LOG_WRITER_H */
