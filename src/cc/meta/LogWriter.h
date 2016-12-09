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
class MdStateCtx;
class MetaDataStore;
class MetaVrSM;
class MetaDataSync;
class MetaVrLogSeq;
class Replay;

class LogWriter
{
public:
    enum { VERSION = 1 };

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
    int WriteNewLogSegment(
        const char*   inLogDirPtr,
        const Replay& inReplayer,
        string&       outLogSegmentFileName);
private:
    class Impl;
    seq_t        mNextSeq;
    volatile int mVrStatus;
    Impl&        mImpl;
};

} // namespace KFS

#endif /* KFS_META_LOG_WRITER_H */
