//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2016/05/11
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
// Veiwstamped replications state machine..
//
// http://pmg.csail.mit.edu/papers/vr-revisited.pdf
//
//----------------------------------------------------------------------------

#include "MetaVrSM.h"
#include "MetaVrOps.h"
#include "util.h"
#include "LogTransmitter.h"
#include "MetaDataSync.h"
#include "NetDispatch.h"

#include "qcdio/qcstutils.h"

#include "common/IntToString.h"
#include "common/BufferInputStream.h"
#include "common/StdAllocator.h"

#include <algorithm>
#include <set>
#include <limits>
#include <sstream>

namespace KFS
{
using std::lower_bound;
using std::find;
using std::sort;
using std::unique;
using std::set;
using std::less;
using std::numeric_limits;
using std::ostringstream;

class MetaVrSM::Impl
{
public:
    typedef MetaVrSM::Config Config;

    enum State
    {
        kStateNone             = 0,
        kStateReconfiguration  = 1,
        kStateViewChange       = 2,
        kStatePrimary          = 3,
        kStateBackup           = 4,
        kStateLogSync          = 5,
        kStateStartViewPrimary = 6,
        kStatesCount
    };
    enum { kMinActiveCount = 3 };

    Impl(
        LogTransmitter& inLogTransmitter,
        MetaVrSM&       inMetaVrSM)
        : mLogTransmitter(inLogTransmitter),
          mMetaDataSyncPtr(0),
          mMetaVrSM(inMetaVrSM),
          mState(kStateBackup),
          mNodeId(-1),
          mReconfigureReqPtr(0),
          mPendingLocations(),
          mPendingChangesList(),
          mConfig(),
          mLocations(),
          mActiveCount(0),
          mQuorum(0),
          mStartedFlag(false),
          mActiveFlag(false),
          mPendingPrimaryTimeout(0),
          mPendingBackupTimeout(0),
          mPendingChangeVewMaxLogDistance(-1),
          mEpochSeq(0),
          mViewSeq(0),
          mCommittedSeq(),
          mLastLogSeq(),
          mTimeNow(),
          mLastProcessTime(mTimeNow),
          mLastReceivedTime(mTimeNow),
          mLastUpTime(mTimeNow),
          mViewChangeStartTime(mTimeNow),
          mStartViewChangeRecvViewSeq(-1),
          mStartViewChangeMaxLastLogSeq(),
          mStartViewChangeMaxCommittedSeq(),
          mStartViewEpochMismatchCount(0),
          mStartViewReplayCount(0),
          mStartViewChangePtr(0),
          mDoViewChangePtr(0),
          mStartViewPtr(0),
          mStartViewChangeNodeIds(),
          mDoViewChangeNodeIds(),
          mStartViewCompletionIds(),
          mStartViewMaxLastLogNodeIds(),
          mSyncServers(),
          mInputStream(),
          mOutputStream(),
          mEmptyString()
        {}
    ~Impl()
    {
        Impl::CancelViewChange();
    }
    int HandleLogBlock(
        const MetaVrLogSeq& inBlockStartSeq,
        const MetaVrLogSeq& inBlockEndSeq,
        const MetaVrLogSeq& inCommittedSeq)
    {
        if (inBlockEndSeq < inBlockStartSeq ||
                inBlockStartSeq < inCommittedSeq ||
                ! inCommittedSeq.IsValid()) {
            panic("VR: invalid log block commit sequence");
            return -EINVAL;
        }
        if (kStatePrimary != mState && kStateBackup != mState &&
                kStateLogSync != mState) {
            return -EVRNOTPRIMARY;
        }
        if (mCommittedSeq < inCommittedSeq) {
            mCommittedSeq = inCommittedSeq;
        }
        if (mLastLogSeq < inBlockEndSeq) {
            mLastLogSeq = inBlockEndSeq;
            if (kStateLogSync == mState &&
                    mStartViewChangeMaxLastLogSeq <= mLastLogSeq) {
                StartViewChange();
            }
        }
        return GetStatus();
    }
    bool Handle(
        MetaRequest&        inReq,
        const MetaVrLogSeq& inLastLogSeq)
    {
        switch (inReq.op) {
            case META_VR_START_VIEW_CHANGE:
                return Handle(static_cast<MetaVrStartViewChange&>(inReq));
            case META_VR_DO_VIEW_CHANGE:
                return Handle(static_cast<MetaVrDoViewChange&>(inReq));
            case META_VR_START_VIEW:
                return Handle(static_cast<MetaVrStartView&>(inReq));
            case META_VR_RECONFIGURATION:
                return Handle(static_cast<MetaVrReconfiguration&>(inReq));
            case META_LOG_WRITER_CONTROL:
                return false;
            default:
                if (kStatePrimary != mState) {
                    inReq.status    = -EVRNOTPRIMARY;
                    inReq.statusMsg = "not primary, state: ";
                    inReq.statusMsg += GetStateName(mState);
                    return true;
                }
                if (inLastLogSeq.IsValid()) {
                    mLastLogSeq = inLastLogSeq;
                }
                break;
        }
        return false;
    }
    static string Show(
        const Properties& inProps)
    {
        string theRet;
        inProps.getList(theRet, string(), string(";"));
        return theRet;
    }
    void HandleReply(
        MetaVrStartViewChange& inReq,
        seq_t                  inSeq,
        const Properties&      inProps,
        NodeId                 inNodeId)
    {
        if (&inReq != mStartViewChangePtr || inReq.GetVrSMPtr() != &mMetaVrSM) {
            panic("VR: invalid start view change completion");
            return;
        }
        KFS_LOG_STREAM_DEBUG <<
            " seq: "      << inSeq <<
            " node: "     << inNodeId <<
            " "           << inReq.Show() <<
            " response: " << Show(inProps) <<
        KFS_LOG_EOM;
        if (! IsActive(inNodeId)) {
            return;
        }
        const bool theNewFlag = mStartViewCompletionIds.insert(inNodeId).second;
        const int  theStatus  = inProps.getValue("s", -1);
        if (0 != theStatus) {
            const int theState = inProps.getValue(
                kMetaVrStateFieldNamePtr, -1);
            if (kStatePrimary == theState || kStateViewChange == theState) {
                const seq_t theEpochSeq = inProps.getValue(
                    kMetaVrEpochSeqFieldNamePtr, seq_t(-1));
                if (theEpochSeq == mEpochSeq) {
                    const MetaVrLogSeq theCommittedSeq = inProps.parseValue(
                        kMetaVrCommittedFieldNamePtr, MetaVrLogSeq());
                    const MetaVrLogSeq theLastLogSeq   = inProps.parseValue(
                        kMetaVrLastLogSeqFieldNamePtr, MetaVrLogSeq());
                    if (theCommittedSeq.IsValid() && theLastLogSeq.IsValid() &&
                            theCommittedSeq <= theLastLogSeq) {
                        mStartViewChangeMaxCommittedSeq =
                            max(mStartViewChangeMaxCommittedSeq, theCommittedSeq);
                        mStartViewChangeMaxLastLogSeq = theCommittedSeq;
                        if (mStartViewChangeMaxLastLogSeq <= theLastLogSeq) {
                            if (mStartViewChangeMaxLastLogSeq < theLastLogSeq) {
                                mStartViewMaxLastLogNodeIds.clear();
                            }
                            mStartViewChangeMaxLastLogSeq = theLastLogSeq;
                            mStartViewMaxLastLogNodeIds.insert(inNodeId);
                        }
                    }
                    const seq_t theViewSeq = inProps.getValue(
                        kMetaVrViewSeqFieldNamePtr, seq_t(-1));
                    if (mStartViewChangeRecvViewSeq < theViewSeq) {
                        mStartViewChangeRecvViewSeq = theViewSeq;
                    }
                } else if (theNewFlag) {
                    mStartViewEpochMismatchCount++;
                }
            }
            KFS_LOG_STREAM_ERROR <<
                " seq: "            << inSeq <<
                " node: "           << inNodeId <<
                " epoch mismatch: " << mStartViewEpochMismatchCount <<
                " max:"
                " committed:"       << mStartViewChangeMaxCommittedSeq <<
                " last:"            << mStartViewChangeMaxLastLogSeq <<
                " count: "          << mStartViewMaxLastLogNodeIds.size() <<
                " responses: "      << mStartViewCompletionIds.size() <<
                " "                 << inReq.Show() <<
            KFS_LOG_EOM;
        }
        StartDoViewChangeIfPossible();
    }
    void HandleReply(
        MetaVrDoViewChange& inReq,
        seq_t               inSeq,
        const Properties&   inProps,
        NodeId              inNodeId)
    {
        if (&inReq != mDoViewChangePtr || inReq.GetVrSMPtr() != &mMetaVrSM) {
            panic("VR: invalid do view change completion");
            return;
        }
        KFS_LOG_STREAM_DEBUG <<
            " seq: "      << inSeq <<
            " node: "     << inNodeId <<
            " '"          << inReq.Show() <<
            " response: " << Show(inProps) <<
        KFS_LOG_EOM;
    }
    void HandleReply(
        MetaVrStartView&  inReq,
        seq_t             inSeq,
        const Properties& inProps,
        NodeId            inNodeId)
    {
        if (&inReq != mStartViewPtr || inReq.GetVrSMPtr() != &mMetaVrSM) {
            panic("VR: invalid start view completion");
            return;
        }
        KFS_LOG_STREAM_DEBUG <<
            " seq: "      << inSeq <<
            " node: "     << inNodeId <<
            " '"          << inReq.Show() <<
            " response: " << Show(inProps) <<
            " replies: "  << mStartViewReplayCount <<
        KFS_LOG_EOM;
        if (! IsActive(inNodeId) || kStateStartViewPrimary != mState) {
            return;
        }
        const bool theNewFlag = mStartViewCompletionIds.insert(inNodeId).second;
        if (! theNewFlag) {
            return;
        }
        const int theStatus = inProps.getValue("s", -1);
        if (0 != theStatus) {
            return;
        }
        const int theState = inProps.getValue(kMetaVrStateFieldNamePtr, -1);
        if (! (theState == kStateViewChange ||
                (theState == kStateStartViewPrimary && inNodeId == mNodeId))) {
            return;
        }
        const seq_t theEpochSeq = inProps.getValue(
            kMetaVrEpochSeqFieldNamePtr, seq_t(-1));
        if (theEpochSeq != mEpochSeq) {
            return;
        }
        const MetaVrLogSeq theLastLogSeq   = inProps.parseValue(
            kMetaVrLastLogSeqFieldNamePtr, MetaVrLogSeq());
        if (theLastLogSeq != mLastLogSeq) {
            return;
        }
        if (mStartViewReplayCount++ < mQuorum) {
            return;
        }
        CancelViewChange();
        KFS_LOG_STREAM_INFO <<
            "primary starting view: " << mEpochSeq << " " << mViewSeq <<
        KFS_LOG_EOM;
        mState = kStatePrimary;
    }
    void HandleReply(
        MetaVrReconfiguration& inReq,
        seq_t                  inSeq,
        const Properties&      inProps,
        NodeId                 inNodeId)
    {
        const char* const theMsgPtr =
            "VR: invalid reconfiguration reply handling attempt";
        KFS_LOG_STREAM_FATAL <<
            theMsgPtr <<
            " seq: "      << inSeq <<
            " node: "     << inNodeId <<
            " "           << inReq.Show() <<
            " response: " << Show(inProps) <<
        KFS_LOG_EOM;
        panic(theMsgPtr);
    }
    void SetLastLogReceivedTime(
        time_t inTime)
    {
        if (kStatePrimary == mState || kStateBackup == mState) {
            mLastReceivedTime = inTime;
        }
    }
    int GetStatus() const
    {
        return (! mActiveFlag ? -EVRNOTPRIMARY : (kStatePrimary == mState ? 0 :
            (kStateBackup == mState ? -EVRNOTPRIMARY : -ELOGFAILED)));
    }
    void Process(
        time_t inTimeNow,
        int&   outVrStatus)
    {
        mTimeNow = inTimeNow;
        if (! mActiveFlag) {
            mLastProcessTime = TimeNow();
            outVrStatus = -EVRNOTPRIMARY;
            return;
        }
        if (kStateBackup == mState) {
            if (mLastReceivedTime + mConfig.GetBackupTimeout() < TimeNow()) {
                mViewSeq++;
                StartViewChange();
            }
        } else if (kStatePrimary == mState) {
            if (mLogTransmitter.IsUp()) {
                mLastUpTime = inTimeNow;
            } else {
                if (mLastUpTime + mConfig.GetPrimaryTimeout() < TimeNow()) {
                    mViewSeq++;
                    StartViewChange();
                }
            }
        } else if (kStateViewChange == mState) {
            if (TimeNow() != mLastProcessTime) {
                StartDoViewChangeIfPossible();
            }
        }
        mLastProcessTime = TimeNow();
        outVrStatus      = GetStatus();
    }
    int Start(
        MetaDataSync&       inMetaDataSync,
        const MetaVrLogSeq& inCommittedSeq)
    {
        mMetaDataSyncPtr = &inMetaDataSync;
        if (mNodeId < 0) {
            mConfig.Clear();
            mActiveCount = 0;
            mQuorum      = 0;
            mState       = kStatePrimary;
            mLastUpTime  = TimeNow();
            mActiveFlag  = true;
        } else {
            mState = kStateBackup;
        }
        mCommittedSeq = inCommittedSeq;
        mLogTransmitter.SetHeartbeatInterval(mConfig.GetPrimaryTimeout());
        mStartedFlag = true;
        return 0;
    }
    void Shutdown()
    {
        CancelViewChange();
        mStartedFlag = false;
    }
    int SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters)
    {
        if (! mStartedFlag || mNodeId < 0) {
            mNodeId = inParameters.getValue(kMetaVrNodeIdParameterNamePtr, -1);
        }
        return 0;
    }
    void Commit(
        const MetaVrLogSeq& inLogSeq)
    {
        if (! inLogSeq.IsValid() ||
                (kStatePrimary != mState && kStateReconfiguration != mState)) {
            return;
        }
        if (kStateReconfiguration == mState && (! mReconfigureReqPtr ||
                mReconfigureReqPtr->logseq < inLogSeq)) {
            panic("VR: invalid commit sequence in reconfiguration");
            return;
        }
        mCommittedSeq = inLogSeq;
        if (! mReconfigureReqPtr || inLogSeq < mReconfigureReqPtr->logseq) {
            return;
        }
        const MetaVrReconfiguration& theReq = *mReconfigureReqPtr;
        mReconfigureReqPtr = 0;
        Commit(theReq);
    }
    const Config& GetConfig() const
        { return mConfig; }
    int GetQuorum() const
        { return mQuorum; }
    bool IsPrimary() const
        { return (kStatePrimary == mState); }
    static const char* GetStateName(
        State inState)
    {
        switch (inState)
        {
            case kStateNone:
                return "none";
            case kStateReconfiguration:
                return "reconfiguration";
            case kStateViewChange:
                return "view_change";
            case kStatePrimary:
                return "primary";
            case kStateBackup:
                return "backup";
            case kStateLogSync:
                return "log_sync";
            case kStateStartViewPrimary:
                return "start_view_primary";
            default: break;
        }
        return "invalid";
    }
    bool Restore(
        bool        inHexFmtFlag,
        int         inType,
        const char* inBufPtr,
        size_t      inLen)
    {
        if ('n' != inType && 'e' != inType) {
            return false;
        }
        istream& theStream = mInputStream.Set(inBufPtr, inLen);
        theStream.clear();
        theStream.flags(
            (inHexFmtFlag ? istream::hex :  istream::dec) | istream::skipws);
        Config::NodeId theNodeId = -1;
        Config::Node   theNode;
        if ('n' == inType) {
            if (! (theStream >> theNodeId) ||
                    theNodeId < 0 ||
                    ! theNode.Extract(theStream) ||
                    ! mConfig.AddNode(theNodeId, theNode)) {
                mConfig.Clear();
                return false;
            }
        } else {
            size_t theCount = 0;
            if (! (theStream >> theCount) ||
                    theCount != mConfig.GetNodes().size()) {
                mConfig.Clear();
                return false;
            }
            int theTimeout = -1;
            if (! (theStream >> theTimeout) || theTimeout < 0) {
                mConfig.Clear();
                return false;
            }
            mConfig.SetPrimaryTimeout(theTimeout);
            theTimeout = -1;
            if (! (theStream >> theTimeout) || theTimeout < 0) {
                mConfig.Clear();
                return false;
            }
            mConfig.SetBackupTimeout(theTimeout);
            seq_t theSeq = -1;
            if (! (theStream >> theSeq) || theSeq < 0) {
                mConfig.Clear();
                return false;
            }
            mConfig.SetChangeVewMaxLogDistance(theSeq);
            theSeq = -1;
            if (! (theStream >> theSeq) || theSeq < 0 ||
                    (0 == theSeq && 0 != theCount)) {
                mConfig.Clear();
                return false;
            }
            mEpochSeq = theSeq;
            theSeq = -1;
            if (! (theStream >> theSeq) || theSeq < 0) {
                mEpochSeq = 0;
                mConfig.Clear();
                return false;
            }
            mViewSeq = theSeq;
        }
        return true;
    }
    int Checkpoint(
        ostream& inStream) const
    {
        const Config::Nodes& theNodes = mConfig.GetNodes();
        ReqOstream theStream(inStream);
        for (Config::Nodes::const_iterator theIt = theNodes.begin();
                theNodes.end() != theIt && inStream;
                ++theIt) {
            theStream << "vrcn/" << theIt->first << " ";
            theIt->second.Insert(inStream) << "\n";
        }
        if (inStream) {
            theStream << "vrce/" << theNodes.size() <<
                " " << mConfig.GetPrimaryTimeout() <<
                " " << mConfig.GetBackupTimeout() <<
                " " << mConfig.GetChangeVewMaxLogDistance() <<
                " " << mEpochSeq <<
                " " << mViewSeq <<
            "\n";
        }
        return (inStream ? 0 : -EIO);
    }
    void Checkpoint(
        bool    inHexFlag,
        string& outStrBuf)
    {
        mOutputStream.str(mEmptyString);
        mOutputStream.flags(inHexFlag ? ostream::hex : ostream::dec);
        if (0 != Checkpoint(mOutputStream)) {
            panic("VR: checkpoint write failure");
        }
        outStrBuf = mOutputStream.str();
        mOutputStream.str(mEmptyString);
    }
    int GetEpochAndViewSeq(
        seq_t& outEpochSeq,
        seq_t& outViewSeq)
    {
        outEpochSeq = mEpochSeq;
        outViewSeq  = mViewSeq;
        return 0;
    }
private:
    typedef Config::Locations   Locations;
    typedef pair<NodeId, int>   ChangeEntry;
    typedef vector<ChangeEntry> ChangesList;
    enum ActiveCheck
    {
        kActiveCheckNone      = 0,
        kActiveCheckActive    = 1,
        kActiveCheckNotActive = 2
    };

    class NopFunc
    {
    public:
        bool operator()(
            int&                 /* ioStatus */,
            string&              /* ioErr */,
            const ChangeEntry&   /* inChange */,
            const Config::Nodes& /* inNodes */,
            const Config::Node&  /* inNode */) const
            { return true; }
    };

    class RmoveFunc
    {
    public:
        RmoveFunc(
            Impl& inImpl)
            : mImpl(inImpl)
            {}
        bool operator()(
            int&                 /* ioStatus */,
            string&              /* ioErr */,
            const ChangeEntry&   inChange,
            Config::Nodes&       inNodes,
            const Config::Node&  inNode) const
        {
            for (Locations::const_iterator
                    theIt = inNode.GetLocations().begin();
                    inNode.GetLocations().end() != theIt;
                    ++theIt) {
                mImpl.RemoveLocation(*theIt);
            }
            if (inNodes.erase(inChange.first) <= 0) {
                panic("VR: remove node failure");
            }
            return true;
        }
    private:
        Impl& mImpl;
    };
    friend class RmoveFunc;

    class ChangeActiveFunc
    {
    public:
        ChangeActiveFunc(
            const bool inActivateFlag)
            : mActivateFlag(inActivateFlag)
            {}
        bool operator()(
            int&                 /* ioStatus */,
            string&              /* ioErr */,
            const ChangeEntry&   /* inChange */,
            const Config::Nodes& /* inNodes */,
            Config::Node&        inNode) const
        {
            if (mActivateFlag) {
                inNode.SetFlags(inNode.GetFlags() | Config::kFlagActive);
            } else {
                inNode.SetFlags(inNode.GetFlags() &
                    ~Config::Flags(Config::kFlagActive));
            }
            return true;
        }
    private:
        const bool mActivateFlag;
    };

    class ChangePrimaryOrderFunc
    {
    public:
        bool operator()(
            int&                 /* ioStatus */,
            string&              /* ioErr */,
            const ChangeEntry&   inChange,
            const Config::Nodes& /* inNodes */,
            Config::Node&        inNode) const
        {
            inNode.SetPrimaryOrder(inChange.second);
            return true;
        }
    };

    class TxStatusCheck : public LogTransmitter::StatusReporter
    {
    public:
        TxStatusCheck(
            ChangesList& inList,
            MetaRequest& inReq)
            : LogTransmitter::StatusReporter(),
              mList(inList),
              mReq(inReq),
              mUpCount(0),
              mTotalUpCount(0)
            {}
        virtual ~TxStatusCheck()
            {}
        virtual bool Report(
            const ServerLocation& inLocation,
            NodeId                inId,
            bool                  inActiveFlag,
            NodeId                inActualId,
            const MetaVrLogSeq&   inAck,
            const MetaVrLogSeq&   inCommitted)
        {
            if (0 != mReq.status) {
                return false;
            }
            for (ChangesList::iterator theIt = mList.begin();
                    mList.end() != theIt;
                    ++theIt) {
                if (inId == theIt->first) {
                    if (inActiveFlag) {
                        mReq.status    = -EINVAL;
                        mReq.statusMsg =
                            "invalid transmitter active status node: ";
                        AppendDecIntToString(mReq.statusMsg, inId);
                        mReq.statusMsg += " location: ";
                        mReq.statusMsg += inLocation.ToString();
                    } else if (0 <= inActualId && inId != inActualId) {
                        mReq.status    = -EINVAL;
                        mReq.statusMsg = "node id mismatch: expected: ";
                        AppendDecIntToString(mReq.statusMsg, inId);
                        mReq.statusMsg += " actual: ";
                        AppendDecIntToString(mReq.statusMsg, inActualId);
                        mReq.statusMsg += " location: ";
                        mReq.statusMsg += inLocation.ToString();
                    }
                    if (0 != mReq.status) {
                        KFS_LOG_STREAM_ERROR <<
                            "transmit channel: " <<
                            mReq.statusMsg <<
                        KFS_LOG_EOM;
                        return false;
                    }
                    if (inAck.IsValid() && inCommitted <= inAck) {
                        if (theIt->second <= 0) {
                            theIt->second = 1;
                            mUpCount++;
                        } else {
                            theIt->second++;
                        }
                        mTotalUpCount++;
                    }
                }
            }
            return true;
        }
        size_t GetUpCount() const
            { return mUpCount; }
        size_t GetTotalUpCount() const
            { return mTotalUpCount; }
    private:
        ChangesList& mList;
        MetaRequest& mReq;
        size_t       mUpCount;
        size_t       mTotalUpCount;
    };

    typedef set<
        NodeId,
        less<NodeId>,
        StdFastAllocator<NodeId>
    > NodeIdSet;

    LogTransmitter&              mLogTransmitter;
    MetaDataSync*                mMetaDataSyncPtr;
    MetaVrSM&                    mMetaVrSM;
    State                        mState;
    NodeId                       mNodeId;
    const MetaVrReconfiguration* mReconfigureReqPtr;
    Locations                    mPendingLocations;
    ChangesList                  mPendingChangesList;
    Config                       mConfig;
    Locations                    mLocations;
    int                          mActiveCount;
    int                          mQuorum;
    bool                         mStartedFlag;
    bool                         mActiveFlag;
    int                          mPendingPrimaryTimeout;
    int                          mPendingBackupTimeout;
    seq_t                        mPendingChangeVewMaxLogDistance;
    seq_t                        mEpochSeq;
    seq_t                        mViewSeq;
    MetaVrLogSeq                 mCommittedSeq;
    MetaVrLogSeq                 mLastLogSeq;
    time_t                       mTimeNow;
    time_t                       mLastProcessTime;
    time_t                       mLastReceivedTime;
    time_t                       mLastUpTime;
    time_t                       mViewChangeStartTime;
    seq_t                        mStartViewChangeRecvViewSeq;
    MetaVrLogSeq                 mStartViewChangeMaxLastLogSeq;
    MetaVrLogSeq                 mStartViewChangeMaxCommittedSeq;
    int                          mStartViewEpochMismatchCount;
    int                          mStartViewReplayCount;
    MetaVrStartViewChange*       mStartViewChangePtr;
    MetaVrDoViewChange*          mDoViewChangePtr;
    MetaVrStartView*             mStartViewPtr;
    NodeIdSet                    mStartViewChangeNodeIds;
    NodeIdSet                    mDoViewChangeNodeIds;
    NodeIdSet                    mStartViewCompletionIds;
    NodeIdSet                    mStartViewMaxLastLogNodeIds;
    MetaDataSync::Servers        mSyncServers;
    BufferInputStream            mInputStream;
    ostringstream                mOutputStream;
    string const                 mEmptyString;

    time_t TimeNow() const
        { return mTimeNow; }
    void Show(
        const MetaVrRequest& inReq)
    {
        KFS_LOG_STREAM(0 == inReq.status ?
                MsgLogger::kLogLevelINFO :
                MsgLogger::kLogLevelERROR) <<
            "seq: "     << inReq.opSeqno        <<
            " status: " << inReq.status         <<
            " "         << inReq.statusMsg      <<
            " state: "  << GetStateName(mState) <<
            " active: " << mActiveFlag          <<
            " epoch: "  << mEpochSeq            <<
            " viw: "    << mViewSeq             <<
            " "         << inReq.Show()         <<
        KFS_LOG_EOM;
    }
    void RetryStartViewChange(
        const char* inMsgPtr)
    {
        if (TimeNow() <=
                mViewChangeStartTime + mConfig.GetBackupTimeout()) {
            // Start next round after timeout.
            return;
        }
        KFS_LOG_STREAM_ERROR <<
            "view change failed: " << inMsgPtr <<
        KFS_LOG_EOM;
        if (mStartViewCompletionIds.size() < mActiveCount) {
            mLastReceivedTime = TimeNow();
            if (mViewSeq < mStartViewChangeRecvViewSeq) {
                mViewSeq = mStartViewChangeRecvViewSeq + 1;
            }
            StartViewChange();
        }
    }
    void StartDoViewChangeIfPossible()
    {
        if (mDoViewChangePtr) {
            return;
        }
        if (mLastLogSeq < mStartViewChangeMaxLastLogSeq) {
            // Need to feetch log / checkpoint.
            if (mActiveFlag && mStartViewMaxLastLogNodeIds.empty()) {
                panic("VR: invalid empty committed node ids");
            }
            if (mActiveFlag && mMetaDataSyncPtr) {
                mSyncServers.clear();
                const Config::Nodes&  theNodes = mConfig.GetNodes();
                for (NodeIdSet::const_iterator theIt =
                        mStartViewMaxLastLogNodeIds.begin();
                        mStartViewMaxLastLogNodeIds.end() != theIt;
                        ++theIt) {
                    Config::Nodes::const_iterator theNodeIt;
                    if (*theIt != mNodeId &&
                            (theNodeIt = theNodes.find(*theIt)) !=
                            theNodes.end() &&
                            0 != (Config::kFlagActive &
                                theNodeIt->second.GetFlags())) {
                        const Config::Locations& theLocs =
                            theNodeIt->second.GetLocations();
                       for (Config::Locations::const_iterator
                                theIt = theLocs.begin();
                                theLocs.end() != theIt;
                                ++theIt) {
                            if (find(mSyncServers.begin(),
                                    mSyncServers.end(),
                                    *theIt) == mSyncServers.end()) {
                                mSyncServers.push_back(*theIt);
                            }
                        }
                    }
                }
                mState = kStateLogSync;
                mMetaDataSyncPtr->ScheduleLogSync(
                    mSyncServers, mLastLogSeq, mStartViewChangeMaxLastLogSeq);
            }
            return;
        }
        const size_t theSz = mStartViewChangeNodeIds.size();
        if (theSz < (size_t)(mActiveCount - mQuorum)) {
            RetryStartViewChange("not sufficient number of noded responded");
            return;
        }
        if (theSz < (size_t)mActiveCount &&
                mStartViewCompletionIds.size() < mActiveCount &&
                TimeNow() <=
                    mViewChangeStartTime + mConfig.GetPrimaryTimeout()) {
            // Wait for more nodes to repsond.
            return;
        }
        const NodeId thePrimaryId = GetPrimaryId();
        if (thePrimaryId < 0) {
            RetryStartViewChange("no primary available");
            return;
        }
        StartDoViewChange(thePrimaryId);
    }
    bool IsActive(
        NodeId inNodeId) const
    {
        if (inNodeId < 0) {
            return false;
        }
        const Config::Nodes&                theNodes = mConfig.GetNodes();
        Config::Nodes::const_iterator const theIt    = theNodes.find(inNodeId);
        return (theNodes.end() != theIt &&
                0 != (Config::kFlagActive & theIt->second.GetFlags()));
    }
    void SetReturnState(
        MetaVrRequest& inReq)
    {
        inReq.mRetCurViewSeq   = mViewSeq;
        inReq.mRetCurEpochSeq  = mEpochSeq;
        inReq.mRetCurState     = mState;
        inReq.mRetCommittedSeq = mCommittedSeq;
        inReq.mRetLastLogSeq   = mLastLogSeq;
    }
    bool VerifyViewChange(
        MetaVrRequest& inReq)
    {
        if (! mActiveFlag) {
            inReq.status    = -ENOENT;
            inReq.statusMsg = "node inactive";
        } else if (IsActive(inReq.mNodeId)) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "request from inactive node";
        } else if (inReq.mEpochSeq < inReq.mLastLogSeq.mEpochSeq ||
                inReq.mViewSeq <= inReq.mLastLogSeq.mViewSeq) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "invalid request: last log sequence";
        } else if (inReq.mLastLogSeq < mCommittedSeq) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "invalid request: committed or last log sequence";
        } else if (mEpochSeq != inReq.mEpochSeq) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "epoch does not match";
        } else if (inReq.mViewSeq < mViewSeq) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "lower view sequence";
        } else if (mLastLogSeq != inReq.mLastLogSeq &&
                (kStatePrimary != mState ||
                    (inReq.mLastLogSeq < mCommittedSeq &&
                        (inReq.mLastLogSeq.mEpochSeq != mLastLogSeq.mEpochSeq ||
                        inReq.mLastLogSeq.mViewSeq != mLastLogSeq.mViewSeq ||
                        inReq.mLastLogSeq.mLogSeq +
                                mConfig.GetChangeVewMaxLogDistance() <
                            mLastLogSeq.mLogSeq
                    )))) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "log sequence mismatch";
        } else {
            inReq.status = 0;
            return true;
        }
        SetReturnState(inReq);
        return false;
    }
    bool Handle(
        MetaVrStartViewChange& inReq)
    {
        Show(inReq);
        if (VerifyViewChange(inReq)) {
            if (mViewSeq != inReq.mViewSeq) {
                if (kStatePrimary == mState ||
                        (kStateBackup == mState &&
                            mLastReceivedTime + mConfig.GetBackupTimeout() <
                                TimeNow())) {
                    mViewSeq = inReq.mViewSeq;
                    StartViewChange();
                }
            } else {
                if (kStateViewChange == mState) {
                    if (! mDoViewChangePtr &&
                            inReq.mNodeId != mNodeId &&
                            mStartViewChangeNodeIds.insert(
                                inReq.mNodeId).second) {
                        StartDoViewChangeIfPossible();
                    }
                } else {
                    inReq.status    = -EINVAL;
                    inReq.statusMsg = "ignored state: ";
                    inReq.statusMsg += GetStateName(mState);
                    SetReturnState(inReq);
                }
            }
        }
        Show(inReq);
        return true;
    }
    bool Handle(
        MetaVrDoViewChange& inReq)
    {
        Show(inReq);
        if (VerifyViewChange(inReq)) {
            if (mNodeId != inReq.mPimaryNodeId) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "primary node id mismatch, state: ";
                inReq.statusMsg += GetStateName(mState);
            } else if (mViewSeq != inReq.mViewSeq) {
                mViewSeq = inReq.mViewSeq;
                StartViewChange();
                mDoViewChangeNodeIds.insert(inReq.mNodeId);
            } else {
                if (kStateViewChange == mState) {
                    if (mDoViewChangeNodeIds.insert(inReq.mNodeId).second &&
                            (size_t)mQuorum <= mDoViewChangeNodeIds.size()) {
                        KFS_LOG_STREAM_INFO <<
                            "start view: " << mViewSeq <<
                            " epoch: "     << mEpochSeq <<
                        KFS_LOG_EOM;
                        StartView();
                    }
                } else {
                    inReq.status    = -EINVAL;
                    inReq.statusMsg = "ignored, state: ";
                    inReq.statusMsg += GetStateName(mState);
                }
            }
        }
        Show(inReq);
        return true;
    }
    bool Handle(
        MetaVrStartView& inReq)
    {
        Show(inReq);
        if (VerifyViewChange(inReq)) {
            if (mViewSeq != inReq.mViewSeq || kStateViewChange != mState) {
                if (kStatePrimary != mState || mNodeId != inReq.mNodeId) {
                    inReq.status    = -EINVAL;
                    inReq.statusMsg = "ignored, state: ";
                    inReq.statusMsg += GetStateName(mState);
                }
            } else {
                mState = kStateBackup;
            }
        }
        Show(inReq);
        return true;
    }
    bool Handle(
        MetaVrReconfiguration& inReq)
    {
        if (0 != inReq.status) {
            if (inReq.replayFlag) {
                panic("VR: invalid reconfiguration request replay");
            }
            return true;
        }
        if (! inReq.logseq.IsValid()) {
            if (inReq.replayFlag) {
                panic("VR: invalid reconfiguration request");
                inReq.status    = -EFAULT;
                inReq.statusMsg = "internal error";
                return true;
            }
            if (MetaRequest::kLogIfOk != inReq.logAction &&
                    MetaRequest::kLogAlways != inReq.logAction) {
                return false;
            }
            StartReconfiguration(inReq);
        } else {
            if (inReq.replayFlag) {
                StartReconfiguration(inReq);
                Commit(inReq);
            }
            // If not replay, then prepare and commit are handled by the log
            // writer.
        }
        return (0 != inReq.status);
    }
    void StartReconfiguration(
        MetaVrReconfiguration& inReq)
    {
        if ((inReq.logseq.IsValid() ? kStateBackup : kStatePrimary) != mState) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "reconfiguration is not possible, state: ";
            inReq.statusMsg += GetStateName(mState);
            return;
        }
        if (mReconfigureReqPtr) {
            panic("VR: invalid reconfiguration state");
            inReq.status    = -EAGAIN;
            inReq.statusMsg = "reconfiguration is in progress";
            return;
        }
        switch (inReq.mOpType)
        {
            case MetaVrReconfiguration::kOpTypeAddNode:
                AddNode(inReq);
                break;
            case MetaVrReconfiguration::kOpTypeRemoveNodes:
                RemoveNodes(inReq);
                break;
            case MetaVrReconfiguration::kOpTypeActivateNodes:
                ModifyActiveStatus(inReq, true);
                break;
            case MetaVrReconfiguration::kOpTypeInactivateNodes:
                ModifyActiveStatus(inReq, false);
                break;
            case MetaVrReconfiguration::kOpTypeSetPrimaryOrder:
                SetPrimaryOrder(inReq);
                break;
            case MetaVrReconfiguration::kOpTypeSetParameters:
                SetParameters(inReq);
                break;
            default:
                inReq.status    = -EINVAL;
                inReq.statusMsg = "invalid operation type";
                break;
        }
        if (0 == inReq.status) {
            mReconfigureReqPtr = &inReq;
        }
    }
    void AddNode(
        MetaVrReconfiguration& inReq)
    {
        if (inReq.mListSize <= 0) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "add node: no listeners specified";
            return;
        }
        const Config::Nodes& theNodes = mConfig.GetNodes();
        if (theNodes.find(inReq.mNodeId) != theNodes.end()) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "add node: node already exists";
            return;
        }
        if (0 != (inReq.mNodeFlags & Config::kFlagActive)) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "add node: node active flag must not be set";
            return;
        }
        mPendingLocations.clear();
        const char*       thePtr    = inReq.mListStr.GetPtr();
        const char* const theEndPtr = thePtr + inReq.mListStr.GetSize();
        ServerLocation    theLocation;
        while (thePtr < theEndPtr) {
            if (theLocation.ParseString(
                    thePtr, theEndPtr - thePtr, inReq.shortRpcFormatFlag)) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "add node: node active flag must not set";
                mPendingLocations.clear();
                return;
            }
            if (HasLocation(theLocation)) {
                inReq.status = -EINVAL;
                inReq.statusMsg = "add node: litener: ";
                inReq.statusMsg += theLocation.ToString();
                inReq.statusMsg += " already assigned to ";
                AppendDecIntToString(
                    inReq.statusMsg, FindNodeByLocation(theLocation));
                mPendingLocations.clear();
                return;
            }
            mPendingLocations.push_back(theLocation);
        }
        if ((int)mPendingLocations.size() != inReq.mListSize) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "add node: listeners list parse failure";
            mPendingLocations.clear();
            return;
        }
    }
    void RemoveNodes(
        MetaVrReconfiguration& inReq)
    {
        if (inReq.mListSize <= 0) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "change active status: empty node id list";
            return;
        }
        if (! ParseNodeIdList(inReq)) {
            return;
        }
        ApplyT(inReq, kActiveCheckNotActive, NopFunc());
    }
    void ModifyActiveStatus(
        MetaVrReconfiguration& inReq,
        bool                   inActivateFlag)
    {
        if (inReq.mListSize <= 0) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "change active status: empty node id list";
            return;
        }
        if (! ParseNodeIdList(inReq)) {
            return;
        }
        ApplyT(
            inReq,
            inActivateFlag ? kActiveCheckNotActive : kActiveCheckActive,
            NopFunc()
        );
        if (0 == inReq.status && kStatePrimary == mState) {
            TxStatusCheck theCheck(mPendingChangesList, inReq);
            mLogTransmitter.GetStatus(theCheck);
            if (0 == inReq.status) {
                if (theCheck.GetUpCount() != mPendingChangesList.size()) {
                    if (theCheck.GetUpCount() < mPendingChangesList.size()) {
                        for (ChangesList::const_iterator
                                theIt = mPendingChangesList.begin();
                                mPendingChangesList.end() != theIt;
                                ++theIt) {
                            if (theIt->second <= 0) {
                                if (0 == inReq.status) {
                                    inReq.status    = -EINVAL;
                                    inReq.statusMsg =
                                        "no communication with nodes:";
                                }
                                inReq.statusMsg += " ";
                                AppendDecIntToString(
                                    inReq.statusMsg, theIt->first);
                            }
                        }
                    }
                    if (0 == inReq.status) {
                        panic("VR: invalid status check result");
                        inReq.status    = -EINVAL;
                        inReq.statusMsg = "internal error";
                    }
                } else {
                    const int theActiveCount = inActivateFlag ?
                        mActiveCount + mPendingChangesList.size() :
                        mActiveCount - mPendingChangesList.size();
                    if (theActiveCount < kMinActiveCount &&
                            (0 != theActiveCount || inActivateFlag)) {
                        inReq.status    = -EINVAL;
                        inReq.statusMsg =
                            "configuration must have at least 3 active nodes";
                    } else {
                        mState = kStateReconfiguration;
                    }
                }
            }
        }
    }
    void SetPrimaryOrder(
        MetaVrReconfiguration& inReq)
    {
        if (inReq.mListSize <= 0) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "modify primary order: empty list";
            return;
        }
        const char*       thePtr    = inReq.mListStr.GetPtr();
        const char* const theEndPtr = thePtr + inReq.mListStr.GetSize();
        while (thePtr < theEndPtr) {
            NodeId theNodeId = -1;
            int    theOrder  = 0;
            if (! inReq.ParseInt(thePtr, theEndPtr - thePtr, theNodeId) ||
                    theNodeId < 0 ||
                    ! inReq.ParseInt(thePtr, theEndPtr - thePtr, theOrder)) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "invalid node id order pair";
                mPendingChangesList.clear();
                return;
            }
            mPendingChangesList.push_back(make_pair(theNodeId, theOrder));
        }
        sort(mPendingChangesList.begin(), mPendingChangesList.end());
        if (! mPendingChangesList.empty()) {
            for (ChangesList::const_iterator
                    theIt = mPendingChangesList.begin(), theNIt = theIt;
                    mPendingChangesList.end() != ++theNIt;
                    theIt = theNIt) {
                if (theIt->first == theNIt->first) {
                    inReq.status    = -EINVAL;
                    inReq.statusMsg = "duplicate node id";
                    mPendingChangesList.clear();
                    return;
                }
            }
        }
        if ((int)mPendingChangesList.size() != inReq.mListSize) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "id list list size mismatch";
            mPendingChangesList.clear();
            return;
        }
        ApplyT(inReq, kActiveCheckNone, NopFunc());
        if (0 == inReq.status && kStatePrimary == mState) {
            mState = kStateReconfiguration;
        }
    }
    static bool ValidateTimeouts(
        int inPrimaryTimeout,
        int inBackupTimeout)
    {
        return (
            0 < inPrimaryTimeout &&
            inPrimaryTimeout + 3 <= inBackupTimeout
        );
    }
    void SetParameters(
        MetaVrReconfiguration& inReq)
    {
        if (2 != inReq.mListSize) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "set timeouts: invalid list size";
            return;
        }
        int               thePrimaryTimeout                 = -1;
        int               theBackupTimeout                  = -1;
        seq_t             thePendingChangeVewMaxLogDistance = -1;
        const char*       thePtr    = inReq.mListStr.GetPtr();
        const char* const theEndPtr = thePtr + inReq.mListStr.GetSize();
        if (! inReq.ParseInt(thePtr, theEndPtr - thePtr, thePrimaryTimeout) ||
                ! inReq.ParseInt(
                    thePtr, theEndPtr - thePtr, theBackupTimeout) ||
                ! ValidateTimeouts(theBackupTimeout, theBackupTimeout) ||
                ! inReq.ParseInt(thePtr, theEndPtr - thePtr,
                    thePendingChangeVewMaxLogDistance)) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "set timeouts: timeout values; "
                "primary timeout must be greater than 0; backup "
                "timeout must be at least 3 seconds greater than primary "
                "timeout";
            return;
        }
        mPendingPrimaryTimeout          = thePrimaryTimeout;
        mPendingBackupTimeout           = theBackupTimeout;
        mPendingChangeVewMaxLogDistance = thePendingChangeVewMaxLogDistance;
        if (kStatePrimary == mState && kMinActiveCount <= mActiveCount) {
            mState = kStateReconfiguration;
        }
    }
    template<typename T>
    void ApplyT(
        int&        outStatus,
        string&     outErr,
        ActiveCheck inActiveCheck,
        const T&    inFunc)
    {
        Config::Nodes& theNodes = mConfig.GetNodes();
        for (ChangesList::const_iterator theIt = mPendingChangesList.begin();
                mPendingChangesList.end() != theIt;
                ++theIt) {
            const NodeId                  theNodeId = theIt->first;
            Config::Nodes::iterator const theNodeIt = theNodes.find(theNodeId);
            if (theNodes.end() == theNodeIt) {
                outStatus = -EINVAL;
                outErr    = "no such node: ";
                AppendDecIntToString(outErr, theNodeId);
                return;
            }
            Config::Node& theNode = theNodeIt->second;
            if (kActiveCheckNone != inActiveCheck &&
                    (0 != (theNode.GetFlags() & Config::kFlagActive)) !=
                    (kActiveCheckActive == inActiveCheck)) {
                outStatus = -EINVAL;
                outErr    = kActiveCheckActive == inActiveCheck ?
                    "node not active: " : "node active: ";
                AppendDecIntToString(outErr, theNodeId);
                return;
            }
            if (! inFunc(outStatus, outErr, *theIt, theNodes, theNode)) {
                return;
            }
        }
    }
    template<typename RT, typename T>
    void ApplyT(
        RT&         inReq,
        ActiveCheck inActiveCheck,
        const T&    inFunc)
    {
        ApplyT(inReq.status, inReq.statusMsg, inActiveCheck, inFunc);
    }
    template<typename T>
    void ApplyT(
        ActiveCheck inActiveCheck,
        const T&    inFunc)
    {
        int    status;
        string statusMsg;
        ApplyT(status, statusMsg, inActiveCheck, inFunc);
        if (0 != status) {
            panic("VR: " + statusMsg);
        }
    }
    bool ParseNodeIdList(
        MetaVrReconfiguration& inReq)
    {
        mPendingChangesList.clear();
        const char*       thePtr    = inReq.mListStr.GetPtr();
        const char* const theEndPtr = thePtr + inReq.mListStr.GetSize();
        while (thePtr < theEndPtr) {
            NodeId theNodeId = -1;
            if (! inReq.ParseInt(thePtr, theEndPtr - thePtr, theNodeId) ||
                    theNodeId < 0) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "invalid node id";
                mPendingChangesList.clear();
                return false;
            }
            mPendingChangesList.push_back(make_pair(theNodeId, -1));
        }
        if ((int)mPendingChangesList.size() != inReq.mListSize) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "node id list size mismatch";
            mPendingChangesList.clear();
            return false;
        }
        sort(mPendingChangesList.begin(), mPendingChangesList.end());
        if (mPendingChangesList.end() !=
                unique(mPendingChangesList.begin(),
                    mPendingChangesList.end())) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "invalid duplicate node id";
            mPendingChangesList.clear();
            return false;
        }
        return true;
    }
    void Commit(
        const MetaVrReconfiguration& inReq)
    {
        if (0 != inReq.status) {
            return;
        }
        if (! inReq.logseq.IsValid()) {
            panic("VR: invalid commit reconfiguration log sequence");
            return;
        }
        switch (inReq.mOpType) {
            case MetaVrReconfiguration::kOpTypeAddNode:
                CommitAddNode(inReq);
                break;
            case MetaVrReconfiguration::kOpTypeRemoveNodes:
                CommitRemoveNodes(inReq);
                break;
            case MetaVrReconfiguration::kOpTypeActivateNodes:
                CommitModifyActiveStatus(inReq, true);
                break;
            case MetaVrReconfiguration::kOpTypeInactivateNodes:
                CommitModifyActiveStatus(inReq, false);
                break;
            case MetaVrReconfiguration::kOpTypeSetPrimaryOrder:
                CommitSetPrimaryOrder(inReq);
                break;
            case MetaVrReconfiguration::kOpTypeSetParameters:
                CommitSetParameters(inReq);
                break;
            default:
                panic("VR: invalid reconfiguration commit attempt");
                break;
        }
        if (0 != inReq.status) {
            panic("VR: reconfiguration commit failure");
        }
        mLogTransmitter.Update(mMetaVrSM);
    }
    void CommitAddNode(
        const MetaVrReconfiguration& inReq)
    {
        if ((int)mPendingLocations.size() != inReq.mListSize) {
            panic("VR: commit add node: invalid locations");
            return;
        }
        if (! mConfig.AddNode(inReq.mNodeId, Config::Node(
                inReq.mNodeFlags, inReq.mPrimaryOrder, mPendingLocations))) {
            panic("VR: commit add node: duplicate node id");
            return;
        }
        for (Locations::const_iterator theIt = mPendingLocations.begin();
                mPendingLocations.end() != theIt;
                ++theIt) {
            AddLocation(*theIt);
        }
    }
    void CommitRemoveNodes(
        const MetaVrReconfiguration& inReq)
    {
        ApplyT(kActiveCheckNotActive, RmoveFunc(*this));
    }
    void CommitModifyActiveStatus(
        const MetaVrReconfiguration& inReq,
        bool                         inActivateFlag)
    {
        ApplyT(inActivateFlag ? kActiveCheckNotActive : kActiveCheckActive,
            ChangeActiveFunc(inActivateFlag));
        if (0 == inReq.status) {
            if (inActivateFlag) {
                mActiveCount += mPendingChangesList.size();
            } else {
                mActiveCount -= mPendingChangesList.size();
            }
            if (mActiveCount < kMinActiveCount &&
                    (0 != mActiveCount || inActivateFlag)) {
                panic("VR: invalid actvie node count");
            }
            mQuorum = mActiveCount - (mActiveCount - 1) / 2;
        }
        CommitReconfiguration(inReq);
    }
    void CommitSetPrimaryOrder(
        const MetaVrReconfiguration& inReq)
    {
        ApplyT(kActiveCheckNotActive, ChangePrimaryOrderFunc());
        CommitReconfiguration(inReq);
    }
    void CommitReconfiguration(
        const MetaVrReconfiguration& inReq)
    {
        if (0 != inReq.status) {
            return;
        }
        mEpochSeq++;
        mViewSeq = 1;
        const Config::Nodes&                theNodes = mConfig.GetNodes();
        Config::Nodes::const_iterator const theIt    = theNodes.find(mNodeId);
        if (kStateReconfiguration == mState) {
            if (theNodes.end() == theIt) {
                panic("VR: invalid primary reconfiguration completion");
            } else {
                mActiveFlag =
                    0 != (Config::kFlagActive & theIt->second.GetFlags());
                PrimaryReconfigurationStartViewChange();
            }
        } else {
            mActiveFlag = theNodes.end() != theIt &&
                0 != (Config::kFlagActive & theIt->second.GetFlags());
        }
    }
    void CommitSetParameters(
        const MetaVrReconfiguration& inReq)
    {
        if (! ValidateTimeouts(
                mPendingPrimaryTimeout, mPendingBackupTimeout)) {
            panic("VR: invalid timeouts");
        }
        if (0 == inReq.status) {
            const Config::Nodes&                theNodes = mConfig.GetNodes();
            Config::Nodes::const_iterator const theIt    =
                theNodes.find(mNodeId);
            if ((theNodes.end() != theIt &&
                    0 != (Config::kFlagActive & theIt->second.GetFlags())) !=
                    mActiveFlag) {
                panic("VR: set timeouts completion:"
                    " invalid node activity change");
                return;
            }
            mEpochSeq++;
            mViewSeq = 1;
            if (kStateReconfiguration == mState || kStateBackup == mState) {
                mConfig.SetPrimaryTimeout(mPendingPrimaryTimeout);
                mConfig.SetBackupTimeout(mPendingBackupTimeout);
                if (0 <= mPendingChangeVewMaxLogDistance) {
                    mConfig.SetChangeVewMaxLogDistance(
                        mPendingChangeVewMaxLogDistance);
                }
                mLogTransmitter.SetHeartbeatInterval(
                    mConfig.GetPrimaryTimeout());
            }
            if (kStateReconfiguration == mState) {
                PrimaryReconfigurationStartViewChange();
            }
        }
    }
    bool HasLocation(
        const ServerLocation& inLocation) const
    {
        Locations::const_iterator const theIt = lower_bound(
            mLocations.begin(), mLocations.end(), inLocation);
        return (theIt != mLocations.end() && inLocation == *theIt);
    }
    void AddLocation(
        const ServerLocation& inLocation)
    {
        mLocations.insert(lower_bound(
            mLocations.begin(), mLocations.end(), inLocation), inLocation);
    }
    bool RemoveLocation(
        const ServerLocation& inLocation)
    {
        Locations::iterator const theIt = lower_bound(
            mLocations.begin(), mLocations.end(), inLocation);
        if (mLocations.end() == theIt || inLocation != *theIt) {
            return false;
        }
        mLocations.erase(theIt);
        return true;
    }
    NodeId FindNodeByLocation(
        const ServerLocation& inLocation)
    {
        const Config::Nodes& theNodes = mConfig.GetNodes();
        for (Config::Nodes::const_iterator theIt = theNodes.begin();
                theNodes.end() != theIt;
                ++theIt) {
            const Config::Locations& theLocations =
                theIt->second.GetLocations();
            if (find(theLocations.begin(), theLocations.end(), inLocation) !=
                    theLocations.end()) {
                return theIt->first;
            }
        }
        return -1;
    }
    void PrimaryReconfigurationStartViewChange()
    {
        if (kStateReconfiguration != mState) {
            panic("VR: invalid end reconfiguration primary state");
            return;
        }
        if (! mActiveFlag) {
            // Primary starts view change.
            mState = kStateBackup;
            return;
        }
        StartViewChange();
    }
    NodeId GetPrimaryId()
    {
        NodeId theId       = -1;
        int    theMinOrder = numeric_limits<int>::max();
        const Config::Nodes& theNodes = mConfig.GetNodes();
        for (Config::Nodes::const_iterator theIt = theNodes.begin();
                theNodes.end() != theIt;
                ++theIt) {
            const NodeId        theNodeId = theIt->first;
            const Config::Node& theNode   = theIt->second;
            const Config::Flags theFlags  = theNode.GetFlags();
            if (0 == (Config::kFlagActive & theFlags) ||
                    0 != (Config::kFlagWitness & theFlags)) {
                continue;
            }
            if (theNodeId != mNodeId &&
                    mStartViewChangeNodeIds.find(theNodeId) ==
                        mStartViewChangeNodeIds.end() &&
                    mDoViewChangeNodeIds.find(theNodeId) ==
                        mDoViewChangeNodeIds.end()) {
                continue;
            }
            const int theOrder = theNode.GetPrimaryOrder();
            if (theMinOrder <= theOrder && 0 <= theId) {
                continue;
            }
            theMinOrder = theOrder;
            theId       = theNodeId;
        }
        return theId;
    }
    template<typename T>
    static void Cancel(
        T*& inReqPtr)
    {
        if (! inReqPtr) {
            return;
        }
        T* const thePtr = inReqPtr;
        inReqPtr = 0;
        thePtr->SetVrSMPtr(0);
        MetaRequest::Release(thePtr);
    }
    void CancelViewChange()
    {
        mReconfigureReqPtr = 0;
        Cancel(mStartViewChangePtr);
        mStartViewCompletionIds.clear();
        Cancel(mDoViewChangePtr);
        Cancel(mStartViewPtr);
        mStartViewChangeRecvViewSeq     = -1;
        mStartViewChangeMaxLastLogSeq   = MetaVrLogSeq();
        mStartViewChangeMaxCommittedSeq = MetaVrLogSeq();
        mStartViewEpochMismatchCount    = 0;
        mStartViewReplayCount           = 0;
    }
    void Init(
        MetaVrRequest& inReq)
    {
        inReq.mNodeId       = mNodeId;
        inReq.mEpochSeq     = mEpochSeq;
        inReq.mViewSeq      = mViewSeq;
        inReq.mCommittedSeq = mCommittedSeq;
        inReq.mLastLogSeq   = mLastLogSeq;
        inReq.SetVrSMPtr(&mMetaVrSM);
    }
    void StartViewChange()
    {
        if (! mActiveFlag) {
            panic("VR: start view change: node non active");
            return;
        }
        CancelViewChange();
        mState               = kStateViewChange;
        mViewChangeStartTime = TimeNow();
        mStartViewChangeNodeIds.clear();
        mDoViewChangeNodeIds.clear();
        mStartViewMaxLastLogNodeIds.clear();
        MetaVrStartViewChange& theOp = *(new MetaVrStartViewChange());
        Init(theOp);
        mStartViewChangePtr = &theOp;
        const NodeId kBroadcast = -1;
        mLogTransmitter.QueueVrRequest(theOp, kBroadcast);
    }
    void StartDoViewChange(
        NodeId inPrimaryId)
    {
        if (! mActiveFlag || inPrimaryId < 0) {
            panic("VR: do view change: node non active");
            return;
        }
        CancelViewChange();
        MetaVrDoViewChange& theOp = *(new MetaVrDoViewChange());
        Init(theOp);
        theOp.mPimaryNodeId = inPrimaryId;
        mDoViewChangePtr = &theOp;
        mLogTransmitter.QueueVrRequest(theOp, theOp.mPimaryNodeId);
    }
    void StartView()
    {
        if (! mActiveFlag) {
            panic("VR: start view: node non active");
            return;
        }
        CancelViewChange();
        mState = kStateStartViewPrimary;
        MetaVrStartView& theOp = *(new MetaVrStartView());
        Init(theOp);
        mStartViewPtr = &theOp;
        const NodeId kBroadcast = -1;
        mLogTransmitter.QueueVrRequest(theOp, kBroadcast);
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

MetaVrSM::MetaVrSM(
    LogTransmitter& inLogTransmitter)
    : mImpl(*(new Impl(inLogTransmitter, *this)))
{
}

MetaVrSM::~MetaVrSM()
{
    delete &mImpl;
}

    int
MetaVrSM::HandleLogBlock(
    const MetaVrLogSeq& inBlockStartSeq,
    const MetaVrLogSeq& inBlockEndSeq,
    const MetaVrLogSeq& inCommittedSeq)
{
    return mImpl.HandleLogBlock(
        inBlockStartSeq, inBlockEndSeq, inCommittedSeq);
}

    bool
MetaVrSM::Handle(
    MetaRequest&        inReq,
    const MetaVrLogSeq& inNextLogSeq)
{
    return mImpl.Handle(inReq, inNextLogSeq);
}

    void
MetaVrSM::HandleReply(
    MetaVrStartViewChange& inReq,
    seq_t                  inSeq,
    const Properties&      inProps,
    MetaVrSM::NodeId       inNodeId)
{
    mImpl.HandleReply(inReq, inSeq, inProps, inNodeId);
}

    void
MetaVrSM::HandleReply(
    MetaVrDoViewChange& inReq,
    seq_t               inSeq,
    const Properties&   inProps,
    MetaVrSM::NodeId    inNodeId)
{
    mImpl.HandleReply(inReq, inSeq, inProps, inNodeId);
}

    void
MetaVrSM::HandleReply(
    MetaVrStartView&  inReq,
    seq_t             inSeq,
    const Properties& inProps,
    MetaVrSM::NodeId  inNodeId)
{
    mImpl.HandleReply(inReq, inSeq, inProps, inNodeId);
}

    void
MetaVrSM::HandleReply(
    MetaVrReconfiguration& inReq,
    seq_t                  inSeq,
    const Properties&      inProps,
    MetaVrSM::NodeId       inNodeId)
{
    mImpl.HandleReply(inReq, inSeq, inProps, inNodeId);
}

    void
MetaVrSM::SetLastLogReceivedTime(
    time_t inTime)
{
    mImpl.SetLastLogReceivedTime(inTime);
}

    void
MetaVrSM::Process(
    time_t inTimeNow,
    int&   outVrStatus)
{
    mImpl.Process(inTimeNow, outVrStatus);
}

    int
MetaVrSM::Start(
    MetaDataSync&       inMetaDataSync,
    const MetaVrLogSeq& inCommittedSeq)
{
    return mImpl.Start(inMetaDataSync, inCommittedSeq);
}

    void
MetaVrSM::Shutdown()
{
    mImpl.Shutdown();
}

    void
MetaVrSM::Commit(
    const MetaVrLogSeq& inLogSeq)
{
    mImpl.Commit(inLogSeq);
}

    int
MetaVrSM::SetParameters(
    const char*       inPrefixPtr,
    const Properties& inParameters)
{
    return mImpl.SetParameters(inPrefixPtr, inParameters);
}

    const MetaVrSM::Config&
MetaVrSM::GetConfig() const
{
    return mImpl.GetConfig();
}

    int
MetaVrSM::GetQuorum() const
{
    return mImpl.GetQuorum();
}

    bool
MetaVrSM::IsPrimary() const
{
    return mImpl.IsPrimary();
}

    bool
MetaVrSM::Restore(
    bool        inHexFmtFlag,
    int         inType,
    const char* inBufPtr,
    size_t      inLen)
{
    return mImpl.Restore(inHexFmtFlag, inType, inBufPtr, inLen);
}

    int
MetaVrSM::Checkpoint(
    ostream& inStream) const
{
    return mImpl.Checkpoint(inStream);
}

    void
MetaVrSM::Checkpoint(
    bool    inHexFlag,
    string& outStrBuf) const
{
    return mImpl.Checkpoint(inHexFlag, outStrBuf);
}

    int
MetaVrSM::GetStatus() const
{
    return mImpl.GetStatus();
}

} // namespace KFS
