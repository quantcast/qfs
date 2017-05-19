//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/05/05
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
// Transaction log replication transmitter.
//
//
//----------------------------------------------------------------------------


#ifndef KFS_META_LOG_TRANSMITTER_H
#define KFS_META_LOG_TRANSMITTER_H

#include "MetaVrSM.h"

#include "common/kfstypes.h"

namespace KFS {

class Properties;
class NetManager;
class MetaVrRequest;
class MetaVrLogSeq;

class LogTransmitter
{
public:
    typedef MetaVrSM::Config Config;
    typedef Config::NodeId   NodeId;

    class CommitObserver
    {
    public:
        virtual void Notify(
            const MetaVrLogSeq& inSeq,
            int                 inPendingAckByteCount) = 0;
    protected:
        CommitObserver()
            {}
        virtual ~CommitObserver()
            {}
    };

    class StatusReporter
    {
    public:
        class Counters
        {
        public:
            typedef int64_t Counter;
            enum { kRateFracBits = 8 };

            Counters()
                : mOp5SecAvgUsec(0),
                  mOp10SecAvgUsec(0),
                  mOp15SecAvgUsec(0),
                  mOp5SecAvgRate(0),
                  mOp10SecAvgRate(0),
                  mOp15SecAvgRate(0),
                  m5SecAvgPendingOps(0),
                  m10SecAvgPendingOps(0),
                  m15SecAvgPendingOps(0),
                  m5SecAvgPendingBytes(0),
                  m10SecAvgPendingBytes(0),
                  m15SecAvgPendingByes(0),
                  mPendingBlockSeqLength(0),
                  mPendingBlockBytes(0),
                  mResponseTimeUsec(0),
                  mResponseSeqLength(0)
                {}
            Counter mOp5SecAvgUsec;
            Counter mOp10SecAvgUsec;
            Counter mOp15SecAvgUsec;
            Counter mOp5SecAvgRate;
            Counter mOp10SecAvgRate;
            Counter mOp15SecAvgRate;
            Counter m5SecAvgPendingOps;
            Counter m10SecAvgPendingOps;
            Counter m15SecAvgPendingOps;
            Counter m5SecAvgPendingBytes;
            Counter m10SecAvgPendingBytes;
            Counter m15SecAvgPendingByes;
            Counter mPendingBlockSeqLength;
            Counter mPendingBlockBytes;
            Counter mResponseTimeUsec;
            Counter mResponseSeqLength;
        };
        virtual bool Report(
            const ServerLocation& inLocation,
            NodeId                inId,
            bool                  inActiveFlag,
            NodeId                inActualId,
            NodeId                inPrimaryNodeId,
            const MetaVrLogSeq&   inAckSeq,
            const MetaVrLogSeq&   inLastSentSeq,
            const Counters&       inCounters) = 0;
    protected:
        StatusReporter()
            {}
        virtual ~StatusReporter()
            {}
    };

    LogTransmitter(
        NetManager&     inNetManager,
        CommitObserver& inCommitObserver);
    ~LogTransmitter();
    int SetParameters(
        const char*       inParamPrefixPtr,
        const Properties& inParameters);
    int TransmitBlock(
        const MetaVrLogSeq& inBlockEndSeq,
        int                 inBlockSeqLen,
        const char*         inBlockPtr,
        size_t              inBlockLen,
        uint32_t            inChecksum,
        size_t              inChecksumStartPos);
    void NotifyAck(
        LogTransmitter::NodeId inNodeId,
        const MetaVrLogSeq&    inAckSeq,
        NodeId                 inPromaryNodeId);
    bool IsUp() const
        { return mUpFlag; }
    void QueueVrRequest(
        MetaVrRequest& inVrReq,
        NodeId         inNodeId);
    int Update(
        MetaVrSM& inMetaVrSM);
    void GetStatus(
        StatusReporter& inReporter);
    void SetHeartbeatInterval(
        int inPrimaryTimeoutSec);
    void SetFileSystemId(
        int64_t inFsId);
    int GetChannelsCount() const;
    void ScheduleHelloTransmit();
    void Suspend(
        bool inFlag);
    void Shutdown();
private:
    class Impl;

    Impl&       mImpl;
    const bool& mUpFlag;
private:
    LogTransmitter(
        const LogTransmitter& inTransmitter);
    LogTransmitter& operator=(
        const LogTransmitter& inTransmitter);
};

} // namespace KFS

#endif /* KFS_META_LOG_TRANSMITTER_H */
