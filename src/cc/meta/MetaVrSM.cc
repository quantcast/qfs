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
#include "NetDispatch.h"

#include "qcdio/QCMutex.h"
#include "qcdio/qcstutils.h"

#include "common/IntToString.h"
#include "common/BufferInputStream.h"

#include <algorithm>

namespace KFS
{
using std::lower_bound;
using std::find;
using std::sort;
using std::unique;

class MetaVrSM::Impl
{
public:
    typedef MetaVrSM::Config Config;

    enum State
    {
        kStateNone            = 0,
        kStateReconfiguration = 1,
        kStatePrimary         = 2,
        kStateBackup          = 3
    };
    enum { kMinActiveCount = 3 };

    Impl(
        LogTransmitter& inLogTransmitter,
        MetaVrSM&       inMetaVrSM)
        : mLogTransmitter(inLogTransmitter),
          mMetaVrSM(inMetaVrSM),
          mMutexPtr(0),
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
          mPendingPrimaryTimeout(0),
          mPendingBackupTimeout(0),
          mEpochSeq(0),
          mViewSeq(0),
          mInputStream()
        {}
    ~Impl()
        {}
    int HandleLogBlock(
        seq_t  inEpochSeq,
        seq_t  inViewSeq,
        seq_t  inLogSeq,
        seq_t& outEpochSeq,
        seq_t& outViewSeq)
    {
        outEpochSeq = mEpochSeq;
        outViewSeq  = mViewSeq;
        return (IsPrimary() ? 0 : -EVRNOTPRIMARY);
    }
    bool Handle(
        MetaRequest& inReq)
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
            case META_VR_START_EPOCH:
                return Handle(static_cast<MetaVrStartEpoch&>(inReq));
            default:
                break;
        }
        return false;
    }
    void HandleReply(
        MetaVrStartViewChange& inReq,
        seq_t                  inSeq,
        const Properties&      inProps,
        NodeId                 inNodeId)
    {
    }
    void HandleReply(
        MetaVrDoViewChange& inReq,
        seq_t               inSeq,
        const Properties&   inProps,
        NodeId              inNodeId)
    {
    }
    void HandleReply(
        MetaVrStartView&  inReq,
        seq_t             inSeq,
        const Properties& inProps,
        NodeId            inNodeId)
    {
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
            " seq: "   << inSeq <<
            " props: " << inProps.size() <<
            " node: "  << inNodeId <<
            " "        << inReq <<
        KFS_LOG_EOM;
        panic(theMsgPtr);
    }
    void HandleReply(
        MetaVrStartEpoch& inReq,
        seq_t             inSeq,
        const Properties& inProps,
        NodeId            inNodeId)
    {
    }
    void SetLastLogReceivedTime(
        time_t inTime)
    {
    }
    void Process(
        time_t inTimeNow)
    {
    }
    int Start()
    {
        if (mNodeId < 0) {
            mConfig.Clear();
            mActiveCount = 0;
            mQuorum      = 0;
            mState       = kStatePrimary;
        } else {
            mState = kStateBackup;
        }
        mStartedFlag = true;
        return 0;
    }
    void Shutdown()
    {
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
        seq_t inLogSeq)
    {
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
            case kStatePrimary:
                return "primary";
            case kStateBackup:
                return "backup";
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
            if (! (theStream >> theSeq) || theSeq <= 0) {
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
        if (theNodes.empty()) {
            return (inStream ? 0 : -EIO);
        }
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
                " " << mEpochSeq <<
                " " << mViewSeq <<
            "\n";
        }
        return (inStream ? 0 : -EIO);
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
            seq_t                 inAck,
            seq_t                 inCommitted)
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
                            "tranmit channel: " <<
                            mReq.statusMsg <<
                        KFS_LOG_EOM;
                        return false;
                    }
                    if (0 <= inAck && inCommitted <= inAck) {
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

    LogTransmitter&              mLogTransmitter;
    MetaVrSM&                    mMetaVrSM;
    QCMutex*                     mMutexPtr;
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
    int                          mPendingPrimaryTimeout;
    int                          mPendingBackupTimeout;
    seq_t                        mEpochSeq;
    seq_t                        mViewSeq;
    BufferInputStream            mInputStream;

    bool Handle(
        MetaVrStartViewChange& inReq)
    {
        return true;
    }
    bool Handle(
        MetaVrDoViewChange& inReq)
    {
        return true;
    }
    bool Handle(
        MetaVrStartView& inReq)
    {
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
        if (inReq.logseq <= 0) {
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
        if (kStatePrimary != mState && kStateBackup != mState) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "reconfiguration is not possible, state: ";
            inReq.statusMsg += GetStateName(mState);
            return;
        }
        if (inReq.mListSize <= 0) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "add node: no listeners specified";
            return;
        }
        if (mReconfigureReqPtr) {
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
            case MetaVrReconfiguration::kOpTypeSetTimeouts:
                SetTimeouts(inReq);
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
    void SetTimeouts(
        MetaVrReconfiguration& inReq)
    {
        if (2 != inReq.mListSize) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "set timeouts: invalid list size";
            return;
        }
        int               thePrimaryTimeout = -1;
        int               theBackupTimeout  = -1;
        const char*       thePtr    = inReq.mListStr.GetPtr();
        const char* const theEndPtr = thePtr + inReq.mListStr.GetSize();
        if (! inReq.ParseInt(thePtr, theEndPtr - thePtr, thePrimaryTimeout) ||
                ! inReq.ParseInt(
                    thePtr, theEndPtr - thePtr, theBackupTimeout) ||
                ! ValidateTimeouts(theBackupTimeout, theBackupTimeout)) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "set timeouts: timeout values; "
                "primary timeout must be greater than 0; backup "
                "timeout must be at least 3 seconds greater than primary "
                "timeout";
            return;
        }
        mPendingPrimaryTimeout = thePrimaryTimeout;
        mPendingBackupTimeout  = theBackupTimeout;
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
    bool Handle(
        MetaVrStartEpoch& inReq)
    {
        return false;
    }
    void Commit(
        const MetaVrReconfiguration& inReq)
    {
        if (0 != inReq.status) {
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
            case MetaVrReconfiguration::kOpTypeSetTimeouts:
                CommitSetTimeouts(inReq);
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
        if (kStateReconfiguration == mState) {
            const Config::Nodes& theNodes = mConfig.GetNodes();
            Config::Nodes::const_iterator const theIt =
                theNodes.find(mNodeId);
            if (theNodes.end() == theIt) {
                panic("VR: invalid primary reconfiguration completion");
            } else {
                if (0 != (Config::kFlagActive & theIt->second.GetFlags())) {
                    mState = kStatePrimary;
                } else {
                    mState = kStateBackup;
                }
            }
        }
    }
    void CommitSetTimeouts(
        const MetaVrReconfiguration& inReq)
    {
        if (! ValidateTimeouts(
                mPendingPrimaryTimeout, mPendingBackupTimeout)) {
            panic("VR: invalid timeouts");
        }
        if (0 == inReq.status) {
            if (kStateReconfiguration == mState ||
                kStateBackup == mState) {
                mConfig.SetPrimaryTimeout(mPendingPrimaryTimeout);
                mConfig.SetBackupTimeout(mPendingBackupTimeout);
            }
            if (kStateReconfiguration == mState) {
                mState = kStatePrimary;
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
    seq_t  inLogSeq,
    seq_t  inBlockLenSeq,
    seq_t  inCommitSeq,
    seq_t& outEpochSeq,
    seq_t& outViewSeq)
{
    return mImpl.HandleLogBlock(
        inLogSeq, inBlockLenSeq, inCommitSeq, outEpochSeq, outViewSeq);
}

    bool
MetaVrSM::Handle(
    MetaRequest& inReq)
{
    return mImpl.Handle(inReq);
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
MetaVrSM::HandleReply(
    MetaVrStartEpoch& inReq,
    seq_t             inSeq,
    const Properties& inProps,
    MetaVrSM::NodeId  inNodeId)
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
    time_t inTimeNow)
{
    mImpl.Process(inTimeNow);
}

    int
MetaVrSM::Start()
{
    return mImpl.Start();
}

    void
MetaVrSM::Shutdown()
{
    mImpl.Shutdown();
}

    void
MetaVrSM::Commit(
    seq_t inLogSeq)
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

} // namespace KFS
