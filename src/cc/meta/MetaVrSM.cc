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
// Veiwstamped replication state machine.
//
// http://pmg.csail.mit.edu/papers/vr-revisited.pdf
//
//----------------------------------------------------------------------------

#include "MetaVrSM.h"
#include "MetaVrOps.h"
#include "LogTransmitter.h"
#include "MetaDataSync.h"
#include "Replay.h"
#include "meta.h"
#include "util.h"

#include "qcdio/QCMutex.h"
#include "qcdio/qcstutils.h"
#include "qcdio/QCUtils.h"

#include "common/IntToString.h"
#include "common/RequestParser.h"
#include "common/BufferInputStream.h"
#include "common/StdAllocator.h"
#include "common/MsgLogger.h"
#include "common/kfserrno.h"

#include "kfsio/NetManager.h"
#include "kfsio/checksum.h"

#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>

#include <algorithm>
#include <set>
#include <limits>

namespace KFS
{
using std::lower_bound;
using std::find;
using std::sort;
using std::unique;
using std::set;
using std::less;
using std::numeric_limits;

class MetaVrResponse
{
public:
    int            mStatus;
    StringBufT<64> mStatusMsg;
    seq_t          mEpochSeq;
    seq_t          mViewSeq;
    MetaVrLogSeq   mCommittedSeq;
    int64_t        mCommittedErrChecksum;
    fid_t          mCommittedFidSeed;
    int            mCommittedStatus;
    MetaVrLogSeq   mLastLogSeq;
    MetaVrLogSeq   mLastViewEndSeq;
    MetaVrLogSeq   mLastNonEmptyViewEndSeq;
    int            mState;
    int64_t        mFileSystemId;
    StringBufT<64> mClusterKey;
    StringBufT<32> mMetaMd;
    ServerLocation mMetaDataStoreLocation;

    MetaVrResponse()
        : mStatus(-ETIMEDOUT),
          mStatusMsg(),
          mEpochSeq(-1),
          mViewSeq(-1),
          mCommittedSeq(),
          mCommittedErrChecksum(0),
          mCommittedFidSeed(-1),
          mCommittedStatus(0),
          mLastLogSeq(),
          mLastViewEndSeq(),
          mLastNonEmptyViewEndSeq(),
          mState(-1),
          mFileSystemId(-1),
          mClusterKey(),
          mMetaMd(),
          mMetaDataStoreLocation()
        {}
    void Reset()
        { *this = MetaVrResponse(); }
    bool Set(
        const Properties& inProps)
    {
        Reset();
        if (inProps.empty()) {
            return false;
        }
        Get(inProps, "s",                           mStatus);
        Get(inProps, "m",                           mStatusMsg);
        Get(inProps, kMetaVrEpochSeqFieldNamePtr,   mEpochSeq);
        Get(inProps, kMetaVrViewSeqFieldNamePtr,    mViewSeq);
        Get(inProps, kMetaVrStateFieldNamePtr,      mState);
        Get(inProps, kMetaVrCommittedFieldNamePtr,  mCommittedSeq);
        Get(inProps, kMetaVrCommittedErrChecksumFieldNamePtr,
            mCommittedErrChecksum);
        Get(inProps, kMetaVrCommittedFidSeedFieldNamePtr,
            mCommittedFidSeed);
        Get(inProps, kMetaVrCommittedStatusFieldNamePtr,
            mCommittedStatus);
        Get(inProps, kMetaVrLastLogSeqFieldNamePtr,      mLastLogSeq);
        Get(inProps, kMetaVrLastViewEndgSeqFieldNamePtr, mLastViewEndSeq);
        Get(inProps, kMetaVrLastNEViewEndSeqFieldNamePtr,
            mLastNonEmptyViewEndSeq);
        Get(inProps, kMetaVrFsIdFieldNamePtr,       mFileSystemId);
        Get(inProps, kMetaVrMDSLocationPortFieldNamePtr,
            mMetaDataStoreLocation.port);
        Get(inProps, kMetaVrMDSLocationHostFieldNamePtr,
            mMetaDataStoreLocation.hostname);
        Get(inProps, kMetaVrClusterKeyFieldNamePtr, mClusterKey);
        Get(inProps, kMetaVrMetaMdFieldNamePtr,     mMetaMd);
        if (mStatus < 0) {
            mStatus = -KfsToSysErrno(-mStatus);
        }
        return true;
    }
    template<typename T>
    T& Display(
        T& inStream) const
    {
        inStream << "status: "  << mStatus;
        if (! mStatusMsg.empty()) {
            inStream << " " << mStatusMsg;
        } else if (mStatus < 0) {
            inStream << " " << ErrorCodeToString(mStatus);
        }
        return (inStream <<
            " epoch: "  << mEpochSeq <<
            " view: "   << mViewSeq <<
            " state: "  << MetaVrSM::GetStateName(mState) <<
            " log:"
            " last: "   << mLastLogSeq <<
            " view: "   << mLastViewEndSeq <<
            " / "       << mLastNonEmptyViewEndSeq <<
            " committed:"
            " seq: "    << mCommittedSeq <<
            " ec: "     << mCommittedErrChecksum <<
            " fids: "   << mCommittedFidSeed <<
            " s: "      << mCommittedStatus <<
            " fsid: "   << mFileSystemId <<
            " mds: "    << mMetaDataStoreLocation <<
            " ckey: "   << mClusterKey <<
            " md: "     << mMetaMd
        );
    }
    template<size_t SZ>
    static void Get(
        const Properties& inProps,
        const char*       inKeyPtr,
        StringBufT<SZ>&   outVal)
    {
        const Properties::String* thePtr = inProps.getValue(inKeyPtr);
        if (thePtr) {
            outVal = *thePtr;
        }
    }
    static void Get(
        const Properties& inProps,
        const char*       inKeyPtr,
        MetaVrLogSeq&     outVal)
        { outVal = inProps.parseValue(inKeyPtr, outVal); }
    static void Get(
        const Properties& inProps,
        const char*       inKeyPtr,
        bool&             outVal)
        { outVal = inProps.getValue(inKeyPtr, outVal ? 1 : 0) != 0; }
    template<typename T>
    static void Get(
        const Properties& inProps,
        const char*       inKeyPtr,
        T&                outVal)
        { outVal = inProps.getValue(inKeyPtr, outVal); }
};

MetaVrLogSeq const kInvalidVrLogSeq;

template<typename T>
inline static T& operator<<(
    T&                    inStream,
    const MetaVrResponse& inResponse)
{ return inResponse.Display(inStream); }

static const MetaVrSM::Config::NodeId kBootstrapPrimaryNodeId = 0;

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
    enum
    {
        kVrReconfCountNotHandled      = 0,
        kVrReconfCountCommitAdvance   = 1,
        kVrReconfCountNoCommitAdvance = 2
    };

    Impl(
        LogTransmitter& inLogTransmitter,
        MetaVrSM&       inMetaVrSM,
        int&            inStatus)
        : mLogTransmitter(inLogTransmitter),
          mMetaDataSyncPtr(0),
          mNetManagerPtr(0),
          mMetaVrSM(inMetaVrSM),
          mStatus(inStatus),
          mFileSystemId(-1),
          mState(kStateBackup),
          mNodeId(-1),
          mClusterKey(),
          mMetaMd(),
          mMetaMds(),
          mReconfigureReqPtr(0),
          mPendingLocations(),
          mPendingChangesList(),
          mConfig(),
          mAllUniqueLocations(),
          mActiveCount(0),
          mQuorum(0),
          mStartedFlag(false),
          mActiveFlag(false),
          mPendingPrimaryTimeout(0),
          mPendingBackupTimeout(0),
          mPendingMaxListenersPerNode(0),
          mPendingChangeVewMaxLogDistance(-1),
          mEpochSeq(0),
          mViewSeq(0),
          mCommittedSeq(),
          mPendingCommitSeq(),
          mCommittedErrChecksum(0),
          mCommittedFidSeed(0),
          mCommittedStatus(0),
          mLastLogSeq(),
          mLastCommitSeq(),
          mLastViewEndSeq(),
          mLastLogStartViewViewEndSeq(),
          mLastNonEmptyViewEndSeq(),
          mPrimaryViewStartSeq(),
          mHelloCommitSeq(),
          mLastCommitTime(0),
          mTimeNow(),
          mLastProcessTime(mTimeNow),
          mLastReceivedTime(mTimeNow),
          mLastStartViewTime(mTimeNow),
          mLastUpTime(mTimeNow),
          mViewChangeStartTime(mTimeNow),
          mStateSetTime(mTimeNow),
          mStartViewChangeRecvViewSeq(-1),
          mStartViewChangeMaxLastLogSeq(),
          mStartViewChangeMaxCommittedSeq(),
          mDoViewChangeViewEndSeq(),
          mChannelsCount(0),
          mStartViewEpochMismatchCount(0),
          mReplyCount(0),
          mLogTransmittersSuspendedFlag(false),
          mLogStartViewPendingRecvFlag(false),
          mCheckLogSyncStatusFlag(false),
          mSyncVrStateFileFlag(true),
          mPanicOnIoErrorFlag(true),
          mIgnoreInvalidVrStateFlag(false),
          mScheduleViewChangeFlag(false),
          mPrimaryNodeId(-1),
          mStartViewChangePtr(0),
          mDoViewChangePtr(0),
          mStartViewPtr(0),
          mMetaVrLogStartViewPtr(0),
          mStartViewChangeNodeIds(),
          mDoViewChangeNodeIds(),
          mRespondedIds(),
          mNodeIdAndMDSLocations(),
          mLogFetchEndSeq(),
          mMetaDataStoreLocation(),
          mSyncServers(),
          mViewChangeInitiationTime(mTimeNow),
          mConfigAlowHostNamesFlag(false),
          mVrStateFileName("vrstate"),
          mVrStateTmpFileName(mVrStateFileName + ".tmp"),
          mVrStateIoStr(),
          mTmpBuffer(),
          mViewChangeReason("restart"),
          mEmptyString(),
          mInactiveNodeStatusMsg("meta server node is inactive"),
          mBackupNodeStatusMsg("meta server node is backup"),
          mInputStream(),
          mWOStream(),
          mMutex(),
          mReconfigureCompletionCondVar(),
          mPendingReconfigureReqPtr(0),
          mVrResponse()
    {
        mStatus = GetStatus();
        mTmpBuffer.reserve(256);
        mVrStateIoStr.reserve(1024);
    }
    ~Impl()
    {
        Impl::CancelViewChange();
    }
    int HandleLogBlock(
        const MetaVrLogSeq& inBlockStartSeq,
        const MetaVrLogSeq& inBlockEndSeq,
        const MetaVrLogSeq& inCommittedSeq,
        NodeId              inTransmitterId)
    {
        if (inBlockEndSeq < inBlockStartSeq ||
                inBlockStartSeq < inCommittedSeq ||
                ! inCommittedSeq.IsValid()) {
            panic("VR: invalid log block commit sequence");
            return -EINVAL;
        }
        const NodeId thePrimaryId = GetPrimaryNodeId(inBlockEndSeq);
        if (0 <= thePrimaryId && inTransmitterId != thePrimaryId) {
            if (inBlockStartSeq < inBlockEndSeq) {
                KFS_LOG_STREAM_ERROR <<
                    "received log block" <<
                    " [" <<          inBlockStartSeq <<
                    "," <<           inBlockEndSeq <<
                    "] from a different"
                    " primary: " <<  thePrimaryId <<
                KFS_LOG_EOM;
                ScheduleViewChange("received block from different primary");
            }
            return -EROFS;
        }
        if (kStateReconfiguration == mState && mReconfigureReqPtr &&
                inBlockEndSeq <= mReconfigureReqPtr->logseq) {
            return 0;
        }
        if (mActiveFlag &&
                kStateBackup == mState &&
                inTransmitterId == thePrimaryId &&
                mLastLogSeq <= inBlockEndSeq) {
            mLastReceivedTime = TimeNow();
        }
        if (inBlockEndSeq.IsSameView(mLastViewEndSeq) &&
                ! inBlockEndSeq.IsSameView(mLastLogSeq) &&
                inBlockStartSeq < inBlockEndSeq) {
            KFS_LOG_STREAM_DEBUG <<
                "rejecting log block"
                " ["          << inBlockStartSeq <<
                ","           << inBlockEndSeq <<
                "] past last closed "
                " view end: " << mLastViewEndSeq <<
                " last log: " << mLastLogSeq <<
            KFS_LOG_EOM;
            return -EROFS;
        }
        return mStatus;
    }
    NodeId LogBlockWriteDone(
        const MetaVrLogSeq& inBlockStartSeq,
        const MetaVrLogSeq& inBlockEndSeq,
        const MetaVrLogSeq& inCommittedSeq,
        const MetaVrLogSeq& inLastViewEndSeq,
        bool                inWriteOkFlag)
    {
        if (inBlockEndSeq < inBlockStartSeq ||
                inBlockStartSeq < inCommittedSeq ||
                ! inCommittedSeq.IsValid() ||
                ! inLastViewEndSeq.IsPastViewStart()) {
            panic("VR: invalid log block commit sequence");
            return -1;
        }
        if (! inWriteOkFlag) {
            if (mActiveFlag &&
                    (kStatePrimary == mState || kStateBackup == mState)) {
                AdvanceView("log write failure");
            }
            return -1;
        }
        mLastLogStartViewViewEndSeq = inLastViewEndSeq;
        if (kStatePrimary != mState && kStateBackup != mState &&
                kStateReconfiguration != mState && kStateLogSync != mState) {
            if (mActiveFlag &&
                    inBlockStartSeq < inBlockEndSeq &&
                    mLastLogSeq < inBlockEndSeq &&
                    ! mLastLogSeq.IsSameView(mLastViewEndSeq)) {
                SetLastLogSeq(inBlockEndSeq);
                ScheduleViewChange();
            }
            return -1;
        }
        if (mLastLogSeq < inBlockEndSeq) {
            if (mLastLogSeq <= mLastCommitSeq) {
                // Reset timer,
                mLastCommitTime = TimeNow();
            }
            SetLastLogSeq(inBlockEndSeq);
        }
        return GetPrimaryNodeId(inBlockEndSeq);
    }
    bool HandleLogBlockFailed(
        const MetaVrLogSeq& inBlockEndSeq,
        NodeId              inTransmitterId)
    {
        if ((mActiveFlag && 0 < mQuorum) ||
                kStatePrimary == mState || kStateLogSync == mState ||
                mNodeId < 0 || inTransmitterId < 0 ||
                mNodeId == inTransmitterId ||
                inBlockEndSeq <= mLastLogSeq ||
                mSyncServers.empty()) {
            return kStateLogSync == mState && (! mActiveFlag || mQuorum <= 0);
        }
        const bool kAllowNonPrimaryFlag = false;
        KFS_LOG_STREAM_INFO <<
            "re-scheduling log fetch:"
            " transmitter: "       << inTransmitterId <<
            " state: "             << GetStateName(mState) <<
            " servers: "           << mSyncServers.size() <<
            " server: "            << mSyncServers.front() <<
            " ["                   << mLastLogSeq <<
            ","                    << inBlockEndSeq <<
            "]"
            " allow non primary: " << kAllowNonPrimaryFlag <<
        KFS_LOG_EOM;
        SetState(kStateLogSync);
        mLogFetchEndSeq = inBlockEndSeq;
        mMetaDataSyncPtr->ScheduleLogSync(
            mSyncServers,
            mLastLogSeq,
            inBlockEndSeq,
            kAllowNonPrimaryFlag
        );
        return true;
    }
    bool Handle(
        MetaRequest&        inReq,
        const MetaVrLogSeq& /* inLastLogSeq */)
    {
        switch (inReq.op) {
            case META_VR_HELLO:
                return Handle(static_cast<MetaVrHello&>(inReq));
            case META_VR_START_VIEW_CHANGE:
                return Handle(static_cast<MetaVrStartViewChange&>(inReq));
            case META_VR_DO_VIEW_CHANGE:
                return Handle(static_cast<MetaVrDoViewChange&>(inReq));
            case META_VR_START_VIEW:
                return Handle(static_cast<MetaVrStartView&>(inReq));
            case META_VR_RECONFIGURATION:
                return Handle(static_cast<MetaVrReconfiguration&>(inReq));
            case META_VR_GET_STATUS:
                return Handle(static_cast<MetaVrGetStatus&>(inReq));
            case META_PING:
                return Handle(static_cast<MetaPing&>(inReq));
            case META_LOG_WRITER_CONTROL:
                return Handle(static_cast<const MetaLogWriterControl&>(inReq));
            case META_READ_META_DATA:
                return Handle(static_cast<const MetaReadMetaData&>(inReq));
            case META_VR_LOG_START_VIEW:
                return false;
            default:
                if (kStateBackup == mState) {
                    inReq.status    = mStatus;
                    inReq.statusMsg = mActiveFlag ?
                        mBackupNodeStatusMsg : mInactiveNodeStatusMsg;
                    return true;
                }
                break;
        }
        return false;
    }
    bool Init(
        MetaVrHello&          inReq,
        const ServerLocation& inPeer,
        LogTransmitter&       inLogTransmitter)
    {
        Init(inReq);
        KFS_LOG_STREAM_DEBUG <<
            "init"
            " peer: " << inPeer <<
            " "       << inReq.Show() <<
        KFS_LOG_EOM;
        return true;
    }
    void HandleReply(
        MetaVrHello&          inReq,
        seq_t                 inSeq,
        const Properties&     inProps,
        NodeId                inNodeId,
        const ServerLocation& inPeer)
    {
        if (inReq.GetVrSMPtr() != &mMetaVrSM) {
            panic("VR: invalid hello completion");
            return;
        }
        inReq.SetVrSMPtr(0);
        if (mStartViewChangePtr || mDoViewChangePtr || mStartViewPtr) {
            return;
        }
        mVrResponse.Set(inProps);
        Show(inReq, inSeq, inProps, inNodeId, inPeer);
    }
    void HandleReply(
        MetaVrStartViewChange& inReq,
        seq_t                  inSeq,
        const Properties&      inProps,
        NodeId                 inNodeId,
        const ServerLocation&  inPeer)
    {
        if (&inReq != mStartViewChangePtr || inReq.GetVrSMPtr() != &mMetaVrSM ||
                ! mActiveFlag ||
                (kStateViewChange != mState && kStateLogSync != mState)) {
            panic("VR: invalid start view change completion");
            return;
        }
        mReplyCount++;
        const bool theOkFlag = mVrResponse.Set(inProps);
        Show(inReq, inSeq, inProps, inNodeId, inPeer);
        if (! theOkFlag || ! IsActive(inNodeId) ||
                ! ValidateClusterKeyAndMd(inReq, inSeq, inNodeId)) {
            return;
        }
        const bool theNewFlag = mRespondedIds.insert(inNodeId).second;
        if (0 == mVrResponse.mStatus) {
            if (inNodeId != mNodeId &&
                    mVrResponse.mEpochSeq == mEpochSeq &&
                    mVrResponse.mViewSeq == mViewSeq &&
                    kStateViewChange == mVrResponse.mState &&
                    ! mDoViewChangePtr &&
                    (mIgnoreInvalidVrStateFlag ||
                        (mVrResponse.mLastViewEndSeq.IsValid() &&
                        mLastViewEndSeq.IsValid()))) {
                int    theStatus = 0;
                string theStatusMsg;
                if (VerifyViewChange(
                        mVrResponse,
                        inReq.op,
                        inNodeId,
                        theStatus,
                        theStatusMsg)) {
                    mStartViewChangeNodeIds.insert(inNodeId);
                    StartDoViewChangeIfPossible(mVrResponse);
                    return;
                }
                KFS_LOG_STREAM_DEBUG <<
                    "start view change response verify:"
                    " "         << theStatusMsg <<
                    " status: " << theStatus <<
                    " "         << mVrResponse <<
                KFS_LOG_EOM;
            }
        } else {
            if (kStatePrimary == mVrResponse.mState ||
                    kStateBackup == mVrResponse.mState ||
                    ((mIgnoreInvalidVrStateFlag || mLastViewEndSeq.IsValid()) &&
                        kStateViewChange == mVrResponse.mState)) {
                if (mEpochSeq <= mVrResponse.mEpochSeq) {
                    if (mVrResponse.mCommittedSeq.IsValid() &&
                            mVrResponse.mLastLogSeq.IsValid() &&
                            mVrResponse.mCommittedSeq <=
                                mVrResponse.mLastLogSeq) {
                        mStartViewChangeMaxCommittedSeq = max(
                            mStartViewChangeMaxCommittedSeq,
                            mVrResponse.mCommittedSeq
                        );
                        if (mStartViewChangeMaxLastLogSeq <=
                                mVrResponse.mLastLogSeq) {
                            if (mStartViewChangeMaxLastLogSeq <
                                    mVrResponse.mLastLogSeq) {
                                mNodeIdAndMDSLocations.clear();
                            }
                            mStartViewChangeMaxLastLogSeq =
                                mVrResponse.mLastLogSeq;
                            mNodeIdAndMDSLocations.insert(make_pair(inNodeId,
                                GetDataStoreLocation(mVrResponse, inPeer)));
                        }
                    }
                    if (mStartViewChangeRecvViewSeq < mVrResponse.mViewSeq) {
                        mStartViewChangeRecvViewSeq = mVrResponse.mViewSeq;
                    }
                }
                if (theNewFlag && mEpochSeq != mVrResponse.mEpochSeq) {
                    mStartViewEpochMismatchCount++;
                }
            }
            KFS_LOG_STREAM_ERROR <<
                "start view change: " << mVrResponse.mStatusMsg <<
                " status: "           << mVrResponse.mStatus <<
                " node: "             << inNodeId <<
                " / "                 << mNodeId <<
                " view: "             << mVrResponse.mEpochSeq <<
                " "                   << mVrResponse.mViewSeq <<
                " / "                 << mEpochSeq <<
                " "                   << mViewSeq <<
            KFS_LOG_EOM;
            KFS_LOG_STREAM_DEBUG <<
                "seq: "             << inSeq <<
                " node: "           << inNodeId <<
                " epoch mismatch: " << mStartViewEpochMismatchCount <<
                " max:"
                " committed: "      << mStartViewChangeMaxCommittedSeq <<
                " last: "           << mStartViewChangeMaxLastLogSeq <<
                " count: "          << mNodeIdAndMDSLocations.size() <<
                " responses: "      << mRespondedIds.size() <<
                " "                 << inReq.Show() <<
                " response: "       << mVrResponse <<
            KFS_LOG_EOM;
        }
        StartDoViewChangeIfPossible();
    }
    void HandleReply(
        MetaVrDoViewChange&   inReq,
        seq_t                 inSeq,
        const Properties&     inProps,
        NodeId                inNodeId,
        const ServerLocation& inPeer)
    {
        if (&inReq != mDoViewChangePtr || inReq.GetVrSMPtr() != &mMetaVrSM) {
            panic("VR: invalid do view change completion");
            return;
        }
        mReplyCount++;
        mVrResponse.Set(inProps);
        Show(inReq, inSeq, inProps, inNodeId, inPeer);
    }
    void HandleReply(
        MetaVrStartView&      inReq,
        seq_t                 inSeq,
        const Properties&     inProps,
        NodeId                inNodeId,
        const ServerLocation& inPeer)
    {
        if (&inReq != mStartViewPtr || inReq.GetVrSMPtr() != &mMetaVrSM) {
            panic("VR: invalid start view completion");
            return;
        }
        mReplyCount++;
        const bool theOkFlag = mVrResponse.Set(inProps);
        Show(inReq, inSeq, inProps, inNodeId, inPeer);
        KFS_LOG_STREAM_DEBUG <<
            "start view:"
            " replies: " << mRespondedIds.size() <<
            " total: "   << mReplyCount <<
            (theOkFlag ? " ok" : " failed") <<
        KFS_LOG_EOM;
        if (! theOkFlag ||
                ! IsActive(inNodeId) ||
                kStateStartViewPrimary != mState ||
                ! ValidateClusterKeyAndMd(inReq, inSeq, inNodeId) ||
                0 != mVrResponse.mStatus ||
                (inNodeId == mNodeId ? kStateStartViewPrimary : kStateBackup) !=
                    mVrResponse.mState ||
                mVrResponse.mEpochSeq != mEpochSeq ||
                mVrResponse.mViewSeq != mViewSeq ||
                mVrResponse.mLastLogSeq < mDoViewChangeViewEndSeq ||
                (! mLastLogSeq.IsPastViewStart() &&
                    mVrResponse.mLastLogSeq != mLastLogSeq)) {
            KFS_LOG_STREAM_ERROR <<
                "start view: " <<
                    (theOkFlag ?
                        mVrResponse.mStatusMsg :
                        "communication error") <<
                " status: "    << mVrResponse.mStatus <<
                " node: "      << inNodeId <<
                " / "          << mNodeId <<
                " view: "      << mVrResponse.mEpochSeq <<
                " "            << mVrResponse.mViewSeq <<
                " / "          << mEpochSeq <<
                " "            << mViewSeq <<
                " last log: "  << mVrResponse.mLastLogSeq <<
                " / "          << mLastLogSeq <<
                " ldvc: "      << mDoViewChangeViewEndSeq <<
            KFS_LOG_EOM;
            return;
        }
        mRespondedIds.insert(inNodeId);
        if ((int)mRespondedIds.size() < mQuorum) {
            return;
        }
        PirmaryCommitStartView();
    }
    NodeId GetNodeId() const
        { return mNodeId; }
    NodeId GetPrimaryNodeId(
        const MetaVrLogSeq& inSeq) const
    {
        if (kStateReconfiguration == mState &&
                (mActiveFlag || mQuorum <= 0) &&
                mReconfigureReqPtr && inSeq <= mReconfigureReqPtr->logseq) {
            return mNodeId;
        }
        return GetPrimaryNodeId();
    }
    NodeId GetPrimaryNodeId() const
    {
        if (mActiveFlag || mQuorum <= 0) {
            if (kStatePrimary == mState) {
                return mNodeId;
            }
            if (kStateBackup == mState) {
                return mPrimaryNodeId;
            }
        }
        return -1;
    }
    void ProcessReplay(
        time_t inTimeNow)
    {
        mTimeNow = inTimeNow;
        if (! mPendingReconfigureReqPtr) {
            return;
        }
        QCStMutexLocker theLocker(mMutex);
        MetaVrReconfiguration& theReq = *mPendingReconfigureReqPtr;
        if (! theReq.logseq.IsValid()) {
            panic("VR: invalid pending reconfiguration request");
            return;
        }
        if (mActiveFlag && kStatePrimary == mState) {
            ScheduleViewChange("reconfiguration replay ", theReq.mOpType);
            mLastUpTime     = TimeNow();
            mLastCommitTime = mLastUpTime;
        }
        StartReconfiguration(theReq);
        mReconfigureReqPtr = 0;
        Commit(theReq);
        mPendingReconfigureReqPtr = 0;
        mReconfigureCompletionCondVar.Notify();
    }
    time_t Process(
        time_t              inTimeNow,
        const MetaVrLogSeq& inCommittedSeq,
        int64_t             inErrChecksum,
        fid_t               inCommittedFidSeed,
        int                 inCommittedStatus,
        const MetaVrLogSeq& inReplayLastLogSeq,
        int&                outVrStatus,
        MetaRequest*&       outReqPtr)
    {
        ProcessReplay(inTimeNow);
        outReqPtr = 0;
        if (mCommittedSeq < inCommittedSeq) {
            if (kStatePrimary == mState &&
                    inCommittedSeq.IsSameView(mPrimaryViewStartSeq) &&
                    inCommittedSeq.mLogSeq <= mPrimaryViewStartSeq.mLogSeq) {
                panic("VR: invalid committed sequence");
            } else {
                mCommittedSeq         = inCommittedSeq;
                mCommittedErrChecksum = inErrChecksum;
                mCommittedStatus      = inCommittedStatus;
                mCommittedFidSeed     = inCommittedFidSeed;
            }
        }
        const bool theReplayWasPendingFlag = mLastLogSeq != mReplayLastLogSeq;
        if (mLastLogSeq < inReplayLastLogSeq) {
            SetLastLogSeq(inReplayLastLogSeq);
        }
        if (mReplayLastLogSeq < inReplayLastLogSeq) {
            mReplayLastLogSeq = inReplayLastLogSeq;
            if (mLogStartViewPendingRecvFlag &&
                    kStateBackup == mState && mActiveFlag &&
                    0 < mActiveCount && 0 < mQuorum) {
                // Mark primary node ID valid on backup after the first
                // successful reply to ignore start view change from all nodes
                // except primary.
                mLogStartViewPendingRecvFlag = false;
            }
        }
        if ((mLastNonEmptyViewEndSeq != mLastLogSeq &&
                mLastLogSeq.IsPastViewStart()) ||
                mLastLogSeq < mCommittedSeq ||
                ! mCommittedSeq.IsPastViewStart() ||
                ! mLastNonEmptyViewEndSeq.IsPastViewStart() ||
                (! mLastViewEndSeq.IsPastViewStart() &&
                    mLastViewEndSeq.IsValid())) {
            panic("VR: invalid log sequence or state");
            return (inTimeNow - 3600);
        }
        if (mScheduleViewChangeFlag &&
                (! mActiveFlag || kStateLogSync == mState)) {
            mScheduleViewChangeFlag = false;
        }
        if (kStateBackup == mState) {
            if (mActiveFlag && mLastReceivedTime + mConfig.GetBackupTimeout() <
                    TimeNow()) {
                AdvanceView("backup receive timed out");
            } else if (! mLogTransmittersSuspendedFlag &&
                    (! mActiveFlag || (0 <= mPrimaryNodeId &&
                        ! mLogStartViewPendingRecvFlag)) &&
                    mStateSetTime + 2 * mConfig.GetBackupTimeout() <
                        TimeNow()) {
                KFS_LOG_STREAM_DEBUG <<
                    "suspending log transmitter" <<
                KFS_LOG_EOM;
                mLogTransmittersSuspendedFlag = true;
                mLogTransmitter.Suspend(mLogTransmittersSuspendedFlag);
            }
        } else if (kStatePrimary == mState ||
                kStateReconfiguration == mState) {
            if (mQuorum <= 0) {
                mLastUpTime = TimeNow();
            } else if (mActiveFlag) {
                if (HasPrimaryTimedOut()) {
                    AdvanceView("primary lease timed out");
                } else if (mLogTransmitter.IsUp()) {
                    if (mLastCommitSeq < mLastLogSeq &&
                            mLastCommitTime + mConfig.GetPrimaryTimeout() <
                                TimeNow()) {
                        AdvanceView("primary commit timed out");
                    } else {
                        mLastUpTime = TimeNow();
                        if (mCommittedSeq == mLastLogSeq &&
                                mLastLogSeq.IsPastViewStart() &&
                                mLastCommitTime + mConfig.GetBackupTimeout() <
                                    TimeNow() &&
                                mHelloCommitSeq < mLastLogSeq) {
                            // Flush replay queue on backups by sending VR hello
                            // after period of inactivity.
                            KFS_LOG_STREAM_DEBUG <<
                                "flush replay queue: " << mLastLogSeq <<
                                " prior flush: "       << mHelloCommitSeq <<
                            KFS_LOG_EOM;
                            mHelloCommitSeq = mLastLogSeq;
                            mLogTransmitter.ScheduleHelloTransmit();
                        }
                    }
                }
                outReqPtr = mMetaVrLogStartViewPtr;
                mMetaVrLogStartViewPtr = 0;
            }
        } else if (mLastLogSeq != mReplayLastLogSeq) {
            KFS_LOG_STREAM_DEBUG <<
                "replay pending:"
                " [" << mReplayLastLogSeq <<
                ":"  << mLastLogSeq <<
                "]" <<
                " state: " << GetStateName(mState) <<
            KFS_LOG_EOM;
        } else if (kStateLogSync == mState) {
            if (mCheckLogSyncStatusFlag) {
                mCheckLogSyncStatusFlag = false;
                bool      theProgressFlag = false;
                const int theStatus       =
                    mMetaDataSyncPtr->GetLogFetchStatus(theProgressFlag);
                if (theStatus < 0 || ! theProgressFlag) {
                    KFS_LOG_STREAM(0 == theStatus ?
                            MsgLogger::kLogLevelINFO :
                            MsgLogger::kLogLevelERROR) <<
                        "log fetch done:"
                        " status: " << theStatus <<
                        " end: "    << mLogFetchEndSeq <<
                        " last: "   << mLastLogSeq <<
                    KFS_LOG_EOM;
                    if (mEpochSeq < mLastLogSeq.mEpochSeq) {
                        panic("VR: invalid committed epoch");
                    }
                    mLogFetchEndSeq = MetaVrLogSeq();
                    if (mActiveFlag) {
                        UpdateViewAndStartViewChange();
                    } else {
                        if (mEpochSeq == mLastLogSeq.mEpochSeq &&
                                mViewSeq < mLastLogSeq.mViewSeq) {
                            mViewSeq  = mLastLogSeq.mViewSeq;
                        }
                        SetState(kStateBackup);
                        mPrimaryNodeId = AllInactiveFindPrimary();
                    }
                }
            }
        } else if (mScheduleViewChangeFlag) {
            if (mPendingCommitSeq <= mCommittedSeq) {
                mScheduleViewChangeFlag = false;
                UpdateViewAndStartViewChange();
            } else {
                KFS_LOG_STREAM_DEBUG <<
                    "commit pending:"
                    " [" << mCommittedSeq <<
                    ":"  << mPendingCommitSeq <<
                    "]" <<
                    " state: " << GetStateName(mState) <<
                KFS_LOG_EOM;
            }
        } else {
            if (mActiveFlag) {
                if (TimeNow() != mLastProcessTime || theReplayWasPendingFlag) {
                    if (theReplayWasPendingFlag ||
                            ! mDoViewChangeViewEndSeq.IsValid()) {
                        UpdateViewAndStartViewChange();
                    } else if ((kStateViewChange != mState &&
                            mViewChangeStartTime + mConfig.GetPrimaryTimeout() <
                                TimeNow())) {
                        UpdateViewAndStartViewChange(
                            GetStateName(mState), " timed out");
                    } else {
                        StartDoViewChangeIfPossible();
                    }
                }
            } else {
                CancelViewChange();
                SetState(kStateBackup);
                mPrimaryNodeId = AllInactiveFindPrimary();
            }
        }
        mLastProcessTime = TimeNow();
        outVrStatus      = mStatus;
        if (kStatePrimary != mState && (mMetaVrLogStartViewPtr || outReqPtr)) {
            panic("VR: invalid outstanding log start view");
        }
        return (kStatePrimary == mState ?
            (mNodeId < 0 ? inTimeNow + 3600 : GetPrimaryLeaseEndTime()) :
            inTimeNow - 3600
        );
    }
    int Start(
        MetaDataSync&         inMetaDataSync,
        NetManager&           inNetManager,
        const UniqueID&       inFileId,
        Replay&               inReplayer,
        int64_t               inFileSystemId,
        const ServerLocation& inDataStoreLocation,
        const string&         inMetaMd)
    {
        if (mStartedFlag) {
            // Already started.
            return -EINVAL;
        }
        if (! mMetaMds.empty() && mMetaMds.end() == mMetaMds.find(inMetaMd)) {
            KFS_LOG_STREAM_ERROR <<
                "meta server md5: " << inMetaMd <<
                " is not int the allowed md5 list" <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        if ((mNodeId < 0 ?
                mConfig.IsEmpty() :
                (0 <= mNodeId && AllInactiveFindPrimary() == mNodeId))) {
            inReplayer.commitAll();
        }
        mMetaDataSyncPtr = &inMetaDataSync;
        mNetManagerPtr   = &inNetManager;
        mMetaMd          = inMetaMd;
        mTimeNow         = mNetManagerPtr->Now();
        if (mConfig.IsEmpty() && mNodeId < 0) {
            mActiveCount = 0;
            mQuorum      = 0;
            SetState(kStatePrimary);
            mLastUpTime          = TimeNow();
            mActiveFlag          = true;
            mPrimaryNodeId       = mNodeId;
            mPrimaryViewStartSeq = MetaVrLogSeq();
        } else {
            if (! mConfig.IsEmpty() && mNodeId < 0) {
                KFS_LOG_STREAM_ERROR <<
                    "invalid or unspecified VR node id: " << mNodeId <<
                    " with non empty VR config" <<
                KFS_LOG_EOM;
                mPrimaryNodeId = -1;
                return -EINVAL;
            }
            mActiveFlag    = IsActiveSelf(mNodeId);
            mPrimaryNodeId = AllInactiveFindPrimary();
            if (mPrimaryNodeId == mNodeId) {
                SetState(kStatePrimary);
                mLastUpTime          = TimeNow();
                mPrimaryViewStartSeq = MetaVrLogSeq();
            } else {
                if (mQuorum <= 0 && mConfig.IsEmpty()) {
                    KFS_LOG_STREAM_WARN <<
                        "transition into backup state with empty VR"
                        " configuration, and node id non 0"
                        " node id: "  << mNodeId <<
                        " primary: "  << mPrimaryNodeId <<
                        " active: "   << mActiveFlag <<
                    KFS_LOG_EOM;
                    SetState(kStateBackup);
                } else {
                    if (mActiveFlag) {
                        ScheduleViewChange("restart");
                    } else {
                        SetState(kStateBackup);
                    }
                }
            }
        }
        mLastReceivedTime       = TimeNow() - 2 * mConfig.GetBackupTimeout();
        mLastCommitTime         = TimeNow();
        mCommittedSeq           = inReplayer.getCommitted();
        mLastCommitSeq          = mCommittedSeq;
        mPendingCommitSeq       = mLastCommitSeq;
        mCommittedErrChecksum   = inReplayer.getErrChksum();
        mCommittedFidSeed       = inFileId.getseed();
        mCommittedStatus        = inReplayer.getLastCommittedStatus();
        mLastNonEmptyViewEndSeq = inReplayer.getLastNonEmptyViewEndSeq();
        SetLastLogSeq(inReplayer.getLastLogSeq());
        mReplayLastLogSeq       = mLastLogSeq;
        mFileSystemId           = inFileSystemId;
        if (mEpochSeq < mCommittedSeq.mEpochSeq) {
            panic("VR: invalid  committed sequence epoch");
            return -EINVAL;
        }
        if (mLastLogSeq < mCommittedSeq) {
            panic("VR: last log sequence is less than committed");
            return -EINVAL;
        }
        if (mEpochSeq < mLastLogSeq.mEpochSeq) {
            panic("VR: invalid epoch");
            return -EINVAL;
        }
        if (mEpochSeq == mLastLogSeq.mEpochSeq &&
                mViewSeq < mLastLogSeq.mViewSeq) {
            mViewSeq = mLastLogSeq.mViewSeq;
        }
        mLastLogStartViewViewEndSeq = mCommittedSeq;
        if (! mMetaDataStoreLocation.IsValid()) {
            mMetaDataStoreLocation = inDataStoreLocation;
        }
        if (kStateViewChange == mState) {
            // Start view has not been queued, pretend that all nodes have
            // failed to respond in order to start new start view change round.
            mReplyCount          = mChannelsCount;
            mViewChangeStartTime = mLastReceivedTime;
        }
        mHelloCommitSeq  = mCommittedSeq;
        NodeId theNodeId = -1;
        mStartedFlag     = true;
        int theRet = mConfig.IsEmpty() ? -EINVAL : ReadVrState(theNodeId);
        if (0 == theRet) {
            if (! CheckNodeId(theNodeId)) {
                theRet = -EINVAL;
            }
        } else {
            mLastViewEndSeq = ((0 < mQuorum && mActiveFlag) || mEpochSeq <= 0 ||
                    mConfig.IsEmpty()) ?
                MetaVrLogSeq() :
                (mLastNonEmptyViewEndSeq.IsSameView(mLastLogSeq) ?
                    mLastNonEmptyViewEndSeq : mLastLogStartViewViewEndSeq);
            if (-ENOENT == theRet) {
                theRet = 0;
            } else if (! mVrStateFileName.empty()) {
                if (unlink(mVrStateFileName.c_str())) {
                    theRet = GetErrno();
                    if (ENOENT == theRet) {
                        theRet = 0;
                    } else {
                        KFS_LOG_STREAM_ERROR <<
                            mVrStateFileName << ": " <<
                            QCUtils::SysError(theRet) <<
                        KFS_LOG_EOM;
                    }
                } else {
                    theRet = 0;
                }
            }
        }
        mStartedFlag = 0 == theRet;
        ConfigUpdate();
        return (0 < theRet ? -theRet : theRet);
    }
    void Shutdown()
    {
        if (! mStartedFlag) {
            return;
        }
        CancelViewChange();
        if (! mLogTransmittersSuspendedFlag) {
            mLogTransmittersSuspendedFlag = true;
            mLogTransmitter.Suspend(mLogTransmittersSuspendedFlag);
        }
        mStartedFlag = false;
    }
    int SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters,
        const char*       inMetaMdPtr)
    {
        Properties::String theName(inPrefixPtr ? inPrefixPtr : "");
        const size_t       thePrefixLen = theName.length();
        const string       theVrStateFileName = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("stateFileName"),
            mVrStateFileName
        );
        int theRet = 0;
        if (theVrStateFileName.empty()) {
            KFS_LOG_STREAM_ERROR <<
                "invalid empty vr state path name"
                " parameter: " << theName <<
            KFS_LOG_EOM;
            theRet = -EINVAL;
        } else {
            mVrStateFileName = theVrStateFileName;
            mVrStateTmpFileName.reserve(mVrStateFileName.size() + 4);
            mVrStateTmpFileName.assign(
                mVrStateFileName.data(), mVrStateFileName.size());
            mVrStateTmpFileName += ".tmp";
        }
        mConfigAlowHostNamesFlag = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("configAlowHostNames"),
            mConfigAlowHostNamesFlag ? 1 : 0
        ) != 0;
        if (! mStartedFlag || mNodeId < 0) {
            mNodeId = inParameters.getValue(
                theName.Truncate(thePrefixLen).Append("id"), NodeId(-1));
            if (mNodeId < 0) {
                const Properties::String* theStrPtr = inParameters.getValue(
                    theName.Truncate(thePrefixLen).Append("hostnameToId"));
                if (theStrPtr) {
                    char         theHostName[256 + 1];
                    const size_t theMaxLen =
                        sizeof(theHostName) / sizeof(theHostName[0]) - 1;
                    if (gethostname(theHostName, theMaxLen)) {
                        theRet = -errno;
                        KFS_LOG_STREAM_ERROR <<
                            "gethostname failure: " <<
                            QCUtils::SysError(-theRet) <<
                        KFS_LOG_EOM;
                    } else {
                        theHostName[theMaxLen] = 0;
                        const size_t      theLen    = strlen(theHostName);
                        const char*       thePtr    = theStrPtr->data();
                        const char* const theEndPtr =
                            thePtr + theStrPtr->size();
                        int               theCnt    = 0;
                        const int         kSpace    = ' ';
                        while (thePtr < theEndPtr) {
                            while (thePtr < theEndPtr &&
                                    (*thePtr & 0xFF) <= kSpace) {
                                ++thePtr;
                            }
                            const char* const theStartPtr = thePtr;
                            while (thePtr < theEndPtr &&
                                    kSpace < (*thePtr & 0xFF)) {
                                ++thePtr;
                            }
                            ++theCnt;
                            NodeId theNodeId = -1;
                            if (theCnt % 2 != 0 &&
                                    theStartPtr + theLen == thePtr &&
                                    0 == memcmp(
                                        theStartPtr, theHostName, theLen)) {
                                const char* const theIdPtr = thePtr;
                                if (DecIntParser::Parse(thePtr,
                                        theEndPtr - thePtr, theNodeId) &&
                                        0 <= theNodeId) {
                                    mNodeId = theNodeId;
                                    break;
                                }
                                thePtr = theIdPtr;
                            }
                        }
                    }
                }
            }
            if (0 <= mNodeId && ! theVrStateFileName.empty()) {
                NodeId theNodeId = -1;
                if (0 == ReadVrState(theNodeId) && ! CheckNodeId(theNodeId)) {
                    if (0 == theRet) {
                        theRet = -EINVAL;
                    }
                }
            }
        }
        mPanicOnIoErrorFlag = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("panicOnIoError"),
            mPanicOnIoErrorFlag ? 1 : 0) != 0;
        mSyncVrStateFileFlag = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("syncVrStateFile"),
            mSyncVrStateFileFlag ? 1 : 0) != 0;
        mIgnoreInvalidVrStateFlag = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("ignoreInvalidVrState"),
            mIgnoreInvalidVrStateFlag ? 1 : 0) != 0;
        const Properties::String* const theStrPtr = inParameters.getValue(
            theName.Truncate(thePrefixLen).Append("metaDataStoreLocation"));
        if (theStrPtr) {
            ServerLocation theLocation;
            if (theLocation.FromString(
                    theStrPtr->GetPtr(), theStrPtr->GetSize())) {
                mMetaDataStoreLocation = theLocation;
            }
        }
        const string theClusterKey = inParameters.getValue(
            kMetaClusterKeyParamNamePtr, mClusterKey);
        if (isValidClusterKey(theClusterKey.c_str())) {
            mClusterKey = theClusterKey;
        } else {
            if (0 == theRet) {
                theRet = -EINVAL;
            }
            KFS_LOG_STREAM_ERROR <<
                "parameter: "            << kMetaClusterKeyParamNamePtr <<
                " invalid cluster key: " << theClusterKey <<
            KFS_LOG_EOM;
        }
        const Properties::String* const theMdStrPtr =
            inParameters.getValue(kMetaserverMetaMdsParamNamePtr);
        if (theMdStrPtr) {
            mMetaMds.clear();
            istream& theStream = mInputStream.Set(
                theMdStrPtr->GetPtr(), theMdStrPtr->GetSize());
            string theMd;
            while ((theStream >> theMd)) {
                mMetaMds.insert(MetaMds::value_type(theMd));
            }
            mInputStream.Reset();
        }
        if (inMetaMdPtr && *inMetaMdPtr && ! mStartedFlag) {
            mMetaMd = inMetaMdPtr;
        }
        if (! mMetaMds.empty() && ! mMetaMd.empty() &&
                mMetaMds.end() == mMetaMds.find(mMetaMd)) {
            KFS_LOG_STREAM_ERROR <<
                "meta server md5: " << mMetaMd <<
                " is not in the allowed md5 list: " <<
                kMetaserverMetaMdsParamNamePtr <<
            KFS_LOG_EOM;
            if (0 == theRet) {
                theRet = -EINVAL;
            }
        }
        return theRet;
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
        if (mQuorum <= 0 && mStartedFlag && mLastLogSeq < inLogSeq) {
            SetLastLogSeq(inLogSeq);
        }
        if (mLastCommitSeq < inLogSeq && inLogSeq <= mLastLogSeq) {
            mLastCommitSeq  = inLogSeq;
            mLastCommitTime = TimeNow();
        }
        // Commit sequence will be set by process, once re-configuration goes
        // through the replay.
        if (mReconfigureReqPtr && mReconfigureReqPtr->logseq <= inLogSeq) {
            MetaVrReconfiguration& theReq = *mReconfigureReqPtr;
            mReconfigureReqPtr = 0;
            Commit(theReq);
        }
        if (kStatePrimary == mState && mActiveFlag) {
            if (! mLogTransmitter.IsUp() && HasPrimaryTimedOut()) {
                AdvanceView("primary log transmitter down");
            }
        }
    }
    const Config& GetConfig() const
        { return mConfig; }
    int GetQuorum() const
        { return mQuorum; }
    bool IsPrimary() const
        { return (kStatePrimary == mState || kStateReconfiguration == mState); }
    static const char* GetStateName(
        int inState)
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
            (inHexFmtFlag ? istream::hex : istream::dec) | istream::skipws);
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
            mAllUniqueLocations.clear();
            size_t theCount = 0;
            if (! (theStream >> theCount) ||
                    theCount != mConfig.GetNodes().size()) {
                ResetConfig();
                return false;
            }
            int theTimeout = -1;
            if (! (theStream >> theTimeout) || theTimeout < 0) {
                ResetConfig();
                return false;
            }
            mConfig.SetPrimaryTimeout(theTimeout);
            theTimeout = -1;
            if (! (theStream >> theTimeout) || theTimeout < 0) {
                ResetConfig();
                return false;
            }
            mConfig.SetBackupTimeout(theTimeout);
            seq_t theSeq = -1;
            if (! (theStream >> theSeq) || theSeq < 0) {
                ResetConfig();
                return false;
            }
            mConfig.SetChangeVewMaxLogDistance(theSeq);
            theSeq = -1;
            if (! (theStream >> theSeq) || theSeq < 0) {
                ResetConfig();
                return false;
            }
            mEpochSeq = theSeq;
            theSeq = -1;
            if (! (theStream >> theSeq) || theSeq < 0) {
                ResetConfig();
                return false;
            }
            mViewSeq = theSeq;
            uint32_t theMaxListenersPerNode = 0;
            if (! (theStream >> theMaxListenersPerNode) ||
                    theMaxListenersPerNode <= 0) {
                ResetConfig();
                return false;
            }
            mConfig.SetMaxListenersPerNode(theMaxListenersPerNode);
            mActiveCount = 0;
            const Config::Nodes& theNodes = mConfig.GetNodes();
            for (Config::Nodes::const_iterator theIt = theNodes.begin();
                    theNodes.end() != theIt;
                    ++theIt) {
                const Config::Locations& theLocs = theIt->second.GetLocations();
                for (Locations::const_iterator theLocIt = theLocs.begin();
                        theLocs.end() != theLocIt;
                        ++theLocIt) {
                    const ServerLocation& theLoc = *theLocIt;
                    if (HasLocation(theLoc)) {
                        KFS_LOG_STREAM_ERROR <<
                            "duplicate location: " << theLoc <<
                            " node id: "           << theIt->first <<
                        KFS_LOG_EOM;
                        ResetConfig();
                        return false;
                    }
                }
                for (Locations::const_iterator theLocIt = theLocs.begin();
                        theLocs.end() != theLocIt;
                        ++theLocIt) {
                    AddLocation(*theLocIt);
                }
                if (0 != (Config::kFlagActive & theIt->second.GetFlags())) {
                    mActiveCount++;
                }
            }
            if (0 < mActiveCount && mEpochSeq <= 0) {
                KFS_LOG_STREAM_ERROR <<
                    "invalid epoch: " << mEpochSeq <<
                    " active nodes: " << mActiveCount <<
                KFS_LOG_EOM;
                ResetConfig();
                return false;
            }
            if (0 < mActiveCount && mActiveCount < kMinActiveCount) {
                KFS_LOG_STREAM_ERROR <<
                    "invalid active node count: " << mActiveCount <<
                KFS_LOG_EOM;
                ResetConfig();
                return false;
            }
            mQuorum = CalcQuorum(mActiveCount);
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
            theIt->second.Insert(theStream) << "\n";
        }
        if (inStream) {
            theStream << "vrce/" << theNodes.size() <<
                " " << mConfig.GetPrimaryTimeout() <<
                " " << mConfig.GetBackupTimeout() <<
                " " << mConfig.GetChangeVewMaxLogDistance() <<
                " " << mEpochSeq <<
                " " << mViewSeq <<
                " " << mConfig.GetMaxListenersPerNode() <<
            "\n";
        }
        return (inStream ? 0 : -EIO);
    }
    int GetEpochAndViewSeq(
        seq_t& outEpochSeq,
        seq_t& outViewSeq)
    {
        outEpochSeq = mEpochSeq;
        outViewSeq  = mViewSeq;
        return 0;
    }
    MetaVrLogSeq GetLastLogSeq() const
        { return mLastLogSeq; }
    const ServerLocation& GetMetaDataStoreLocation() const
        { return mMetaDataStoreLocation; }
    bool ValidateAckPrimaryId(
        NodeId inNodeId,
        NodeId inPrimaryNodeId)
    {
        if (! mActiveFlag || ! IsActive(inNodeId)) {
            return true;
        }
        if (kStatePrimary == mState && 0 <= inPrimaryNodeId &&
                inPrimaryNodeId != mNodeId) {
            AdvanceView("received ACK from a different primary");
            return false;
        }
        return true;
    }
    static NodeId GetNodeId(
        const MetaRequest& inReq)
    {
        switch (inReq.op) {
            case META_VR_HELLO:
            case META_VR_START_VIEW_CHANGE:
            case META_VR_DO_VIEW_CHANGE:
            case META_VR_START_VIEW:
                return static_cast<const MetaVrRequest&>(inReq).mNodeId;
            default:
                break;
        }
        return -1;
    }
private:
    typedef set<
        NodeId,
        less<NodeId>,
        StdFastAllocator<NodeId>
    > NodeIdSet;
    typedef Config::Locations   Locations;
    typedef pair<NodeId, int>   ChangeEntry;
    typedef vector<ChangeEntry> ChangesList;
    enum ActiveCheck
    {
        kActiveCheckNone      = 0,
        kActiveCheckActive    = 1,
        kActiveCheckNotActive = 2
    };

    class NameToken
    {
    public:
        NameToken(
            const char* inNamePtr)
            : mNamePtr(inNamePtr ? inNamePtr : "")
            {}
        bool operator<(
            const NameToken& inRhs) const
            { return (strcmp(mNamePtr, inRhs.mNamePtr) < 0); }
        bool operator>(
            const NameToken& inRhs) const
            { return (strcmp(mNamePtr, inRhs.mNamePtr) > 0); }
        bool operator==(
            const NameToken& inRhs) const
            { return (strcmp(mNamePtr, inRhs.mNamePtr) == 0); }
    private:
        const char* const mNamePtr;
    };

    typedef void (Impl::*VrReconfigurationMethodPtr)(MetaVrReconfiguration&);

    typedef map<
        NameToken,
        pair<VrReconfigurationMethodPtr, VrReconfigurationMethodPtr>,
        less<NameToken>,
        StdFastAllocator<
            pair<
                const NameToken,
                pair<VrReconfigurationMethodPtr, VrReconfigurationMethodPtr>
            >
        >
    > VrReconfigurationTable;

    class AddVrMethodFunc
    {
    public:
        AddVrMethodFunc(
            VrReconfigurationTable& inTable)
            : mTable(inTable)
            {}
        AddVrMethodFunc& Def(
            const char*                inNamePtr,
            const char*                /* inHelpStrPtr */,
            VrReconfigurationMethodPtr inMethodPtr,
            VrReconfigurationMethodPtr inCommitMethodPtr)
        {
            if (! mTable.insert(make_pair(
                    NameToken(inNamePtr),
                    make_pair(inMethodPtr, inCommitMethodPtr))).second) {
                // Method name collision..
                abort();
            }
            return *this;
        }
    private:
        VrReconfigurationTable& mTable;
    private:
        AddVrMethodFunc(
            const AddVrMethodFunc& inFunc);
        AddVrMethodFunc& operator=(
            const AddVrMethodFunc& inFunc);
    };

    class HelpFunc
    {
    public:
        HelpFunc(
            ostream& inStream)
            : mStream(inStream)
            {}
        HelpFunc& Def(
            const char*                inNamePtr,
            const char*                inHelpStrPtr,
            VrReconfigurationMethodPtr /* inMethodPtr */,
            VrReconfigurationMethodPtr /* inCommitMethodPtr */)
        {
            if (inNamePtr && *inNamePtr) {
                mStream <<
                    inNamePtr <<
                    ((inHelpStrPtr && *inHelpStrPtr) ? "\n" : "") <<
                    (inHelpStrPtr ? inHelpStrPtr : "") <<
                "\n";
            }
            return *this;
        }
        template<typename T, typename OT>
        HelpFunc& Def2(
            const char* inNamePtr,
            const char* /* inShortNamePtr */,
            T OT::*     /* inMethodPtr */,
            const T&    inDefault    = T(),
            const char* inHelpStrPtr = 0)
        {
            if (inNamePtr && *inNamePtr) {
                mStream <<
                    inNamePtr <<
                    (inHelpStrPtr ? inHelpStrPtr : "") <<
                "\n";
            }
            return *this;
        }
    private:
        ostream& mStream;
    private:
        HelpFunc(
            const HelpFunc& inFunc);
        HelpFunc& operator=(
            const HelpFunc& inFunc);
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
        typedef LogTransmitter::StatusReporter::Counters Counters;

        TxStatusCheck(
            bool         inActivateFlag,
            NodeId       inPrimaryNodeId,
            ChangesList& inList,
            MetaRequest& inReq)
            : LogTransmitter::StatusReporter(),
              mActivateFlag(inActivateFlag),
              mPrimaryNodeId(inPrimaryNodeId),
              mReq(inReq),
              mList(inList),
              mActiveUpSet(),
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
            NodeId                inPrimaryNodeId,
            const MetaVrLogSeq&   inAckSeq,
            const MetaVrLogSeq&   inLastSentSeq,
            const Counters&       /* inCounters */)
        {
            if (0 != mReq.status) {
                return false;
            }
            size_t theCnt = 0;
            for (ChangesList::iterator theIt = mList.begin();
                    mList.end() != theIt;
                    ++theIt) {
                if (inId == theIt->first) {
                    if (inActiveFlag == mActivateFlag) {
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
                    theCnt++;
                    if (mActivateFlag &&
                            inAckSeq.IsValid() && inLastSentSeq <= inAckSeq &&
                            (inPrimaryNodeId < 0 ||
                                mPrimaryNodeId == inPrimaryNodeId)) {
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
            if (! mActivateFlag && theCnt <= 0 && inActiveFlag &&
                    0 <= inActualId && inActualId == inId &&
                    inAckSeq.IsValid() && inLastSentSeq <= inAckSeq &&
                    mPrimaryNodeId == inPrimaryNodeId) {
                if (mActiveUpSet.insert(inId).second) {
                    mUpCount++;
                }
                mTotalUpCount++;
            }
            return true;
        }
        size_t GetUpCount() const
            { return mUpCount; }
        size_t GetTotalUpCount() const
            { return mTotalUpCount; }
    private:
        const bool   mActivateFlag;
        const NodeId mPrimaryNodeId;
        MetaRequest& mReq;
        ChangesList& mList;
        NodeIdSet    mActiveUpSet;
        size_t       mUpCount;
        size_t       mTotalUpCount;
    private:
        TxStatusCheck(
            const TxStatusCheck& inCheck);
        TxStatusCheck& operator=(
            const TxStatusCheck& inCheck);
    };

    class TxStatusCheckSwap : public LogTransmitter::StatusReporter
    {
    public:
        typedef LogTransmitter::StatusReporter::Counters Counters;

        TxStatusCheckSwap(
            NodeId             inPrimaryNodeId,
            const ChangesList& inList,
            MetaRequest&       inReq)
            : mInactivateList(),
              mActivateList(),
              mCheckInactivate(
                false, inPrimaryNodeId, mInactivateList, inReq),
              mCheckActivate(
                true, -1, mActivateList, inReq)
        {
            if (2 != inList.size()) {
                inReq.status    = -EFAULT;
                inReq.statusMsg = "internal error";
                return;
            }
            mInactivateList.push_back(inList.front());
            mActivateList.push_back(inList.back());
        }
        virtual ~TxStatusCheckSwap()
            {}
        virtual bool Report(
            const ServerLocation& inLocation,
            NodeId                inId,
            bool                  inActiveFlag,
            NodeId                inActualId,
            NodeId                inPrimaryNodeId,
            const MetaVrLogSeq&   inAckSeq,
            const MetaVrLogSeq&   inLastSentSeq,
            const Counters&       inCounters)
        {
            return (
                mCheckInactivate.Report(
                    inLocation,
                    inId,
                    inActiveFlag,
                    inActualId,
                    inPrimaryNodeId,
                    inAckSeq,
                    inLastSentSeq,
                    inCounters) &&
                mCheckActivate.Report(
                    inLocation,
                    inId,
                    inActiveFlag,
                    inActualId,
                    inPrimaryNodeId,
                    inAckSeq,
                    inLastSentSeq,
                    inCounters)
            );
        }
    private:
        ChangesList   mInactivateList;
        ChangesList   mActivateList;
    public:
        TxStatusCheck mCheckInactivate;
        TxStatusCheck mCheckActivate;
    private:
        TxStatusCheckSwap(
            const TxStatusCheckSwap& inCheck);
        TxStatusCheckSwap& operator=(
            const TxStatusCheckSwap& inCheck);
    };

    class TxStatusCheckNode : public LogTransmitter::StatusReporter
    {
    public:
        typedef LogTransmitter::StatusReporter::Counters Counters;

        TxStatusCheckNode(
            NodeId                   inNodeId,
            const Config::Locations& inLocations,
            NodeId                   inPrimaryNodeId)
            : LogTransmitter::StatusReporter(),
              mNodeId(inNodeId),
              mPrimaryNodeId(inPrimaryNodeId),
              mLocations(inLocations),
              mUpCount(0)
            {}
        virtual ~TxStatusCheckNode()
            {}
        virtual bool Report(
            const ServerLocation& inLocation,
            NodeId                inId,
            bool                  inActiveFlag,
            NodeId                inActualId,
            NodeId                inPrimaryNodeId,
            const MetaVrLogSeq&   inAckSeq,
            const MetaVrLogSeq&   inLastSentSeq,
            const Counters&       /* inCounters */)
        {
            if (mNodeId != inId) {
                return true;
            }
            Locations::iterator const theIt = find(
                mLocations.begin(), mLocations.end(), inLocation);
            if (theIt == mLocations.end()) {
                if (0 <= inActualId && inId == inActualId &&
                        inAckSeq.IsValid() && inLastSentSeq <= inAckSeq &&
                        (mPrimaryNodeId < 0 ||
                            mPrimaryNodeId == inPrimaryNodeId)) {
                    mUpCount++;
                }
            } else {
                mLocations.erase(theIt);
            }
            return true;
        }
        size_t GetUpCount() const
            { return mUpCount; }
    private:
        NodeId const      mNodeId;
        NodeId const      mPrimaryNodeId;
        Config::Locations mLocations;
        size_t            mUpCount;
    private:
        TxStatusCheckNode(
            const TxStatusCheckNode& inCheck);
        TxStatusCheckNode& operator=(
            const TxStatusCheckNode& inCheck);
    };

    class TxStatusReporter : public LogTransmitter::StatusReporter
    {
    public:
        typedef LogTransmitter::StatusReporter::Counters Counters;

        TxStatusReporter(
            NodeId      inPrimaryNodeId,
            string&     inPrefixBuf,
            ostream&    inStream,
            const char* inSepPtr,
            const char* inDelimPtr)
            : LogTransmitter::StatusReporter(),
              mPrimaryNodeId(inPrimaryNodeId),
              mPrefixBuf(inPrefixBuf),
              mStream(inStream),
              mPrefixLen(),
              mChanCnt(0),
              mSepPtr(inSepPtr),
              mDelimPtr(inDelimPtr),
              mPrevId(-1),
              mActiveUpChannelsCount(0),
              mActiveUpNodesCount(0)
        {
            mPrefixBuf += "channel.";
            mPrefixLen = mPrefixBuf.size();
        }
        virtual ~TxStatusReporter()
            {}
        virtual bool Report(
            const ServerLocation& inLocation,
            NodeId                inId,
            bool                  inActiveFlag,
            NodeId                inActualId,
            NodeId                inPrimaryNodeId,
            const MetaVrLogSeq&   inAckSeq,
            const MetaVrLogSeq&   inLastSentSeq,
            const Counters&       inCounters)
        {
            mPrefixBuf.resize(mPrefixLen);
            AppendDecIntToString(mPrefixBuf, mChanCnt++);
            const double kRateMult = 1. / (1 << Counters::kRateFracBits);
            mPrefixBuf += ".";
            mStream <<
                mPrefixBuf << "location"   << mSepPtr << inLocation      <<
                     mDelimPtr <<
                mPrefixBuf << "id"         << mSepPtr << inId            <<
                    mDelimPtr <<
                mPrefixBuf << "receivedId" << mSepPtr << inActualId      <<
                    mDelimPtr <<
                mPrefixBuf << "primaryId"  << mSepPtr << inPrimaryNodeId <<
                    mDelimPtr <<
                mPrefixBuf << "active"     << mSepPtr << inActiveFlag    <<
                    mDelimPtr <<
                mPrefixBuf << "ack"        << mSepPtr << inAckSeq        <<
                    mDelimPtr <<
                mPrefixBuf << "sent"       << mSepPtr << inLastSentSeq   <<
                    mDelimPtr
                ;
            mPrefixBuf += "ctrs.";
            mStream <<
                mPrefixBuf << "op5SecAvgUsec"              << mSepPtr   <<
                    inCounters.mOp5SecAvgUsec              << mDelimPtr <<
                mPrefixBuf << "op10SecAvgUsec"             << mSepPtr   <<
                    inCounters.mOp10SecAvgUsec             << mDelimPtr <<
                mPrefixBuf << "op15SecAvgUsec"             << mSepPtr   <<
                    inCounters.mOp15SecAvgUsec             << mDelimPtr <<
                mPrefixBuf << "opAvgUsec"                  << mSepPtr   <<
                    (0 < inCounters.mResponseSeqLength ?
                    inCounters.mResponseTimeUsec /
                        inCounters.mResponseSeqLength :
                    Counters::Counter(0))                  << mDelimPtr <<
                mPrefixBuf << "op5SecAvgRate"              << mSepPtr   <<
                    inCounters.mOp5SecAvgRate  * kRateMult << mDelimPtr <<
                mPrefixBuf << "op10SecAvgRate"             << mSepPtr   <<
                    inCounters.mOp10SecAvgRate * kRateMult << mDelimPtr <<
                mPrefixBuf << "op15SecAvgRate"             << mSepPtr   <<
                    inCounters.mOp15SecAvgRate * kRateMult << mDelimPtr <<
                mPrefixBuf << "opsTotal"                   << mSepPtr   <<
                    inCounters.mResponseSeqLength          << mDelimPtr <<
                mPrefixBuf << "opsTimeTotalUsec"           << mSepPtr   <<
                    inCounters.mResponseTimeUsec           << mDelimPtr <<
                mPrefixBuf << "5SecAvgPendingOps"          << mSepPtr   <<
                    inCounters.m5SecAvgPendingOps          << mDelimPtr <<
                mPrefixBuf << "10SecAvgPendingOps"         << mSepPtr   <<
                    inCounters.m10SecAvgPendingOps         << mDelimPtr <<
                mPrefixBuf << "15SecAvgPendingOps"         << mSepPtr   <<
                    inCounters.m15SecAvgPendingOps         << mDelimPtr <<
                mPrefixBuf << "pendingOps"                 << mSepPtr   <<
                    inCounters.mPendingBlockSeqLength      << mDelimPtr <<
                mPrefixBuf << "5SecAvgPendingBytes"        << mSepPtr   <<
                    inCounters.m5SecAvgPendingBytes        << mDelimPtr <<
                mPrefixBuf << "10SecAvgPendingBytes"       << mSepPtr   <<
                    inCounters.m10SecAvgPendingBytes       << mDelimPtr <<
                mPrefixBuf << "15SecAvgPendingBytes"       << mSepPtr   <<
                    inCounters.m15SecAvgPendingByes        << mDelimPtr <<
                mPrefixBuf << "pendingBytes"               << mSepPtr   <<
                    inCounters.mPendingBlockBytes          << mDelimPtr
           ;
            if (inActiveFlag && 0 <= inActualId && inId == inActualId &&
                    inAckSeq.IsValid() && (inLastSentSeq <= inAckSeq ||
                        inLastSentSeq.IsSameView(inAckSeq)) &&
                    (mPrimaryNodeId < 0 || mPrimaryNodeId == inPrimaryNodeId)) {
                mActiveUpChannelsCount++;
                if (mPrevId != inId) {
                    mActiveUpNodesCount++;
                }
            }
            mPrevId = inId;
            return true;
        }
        size_t GetActiveUpNodesCount() const
            { return mActiveUpNodesCount; }
        size_t GetActiveUpChannelsCount() const
            { return mActiveUpChannelsCount; }
    private:
        NodeId const      mPrimaryNodeId;
        string&           mPrefixBuf;
        ostream&          mStream;
        size_t            mPrefixLen;
        size_t            mChanCnt;
        const char* const mSepPtr;
        const char* const mDelimPtr;
        NodeId            mPrevId;
        size_t            mActiveUpChannelsCount;
        size_t            mActiveUpNodesCount;
    private:
        TxStatusReporter(
            const TxStatusReporter& inReporter);
        TxStatusReporter& operator=(
            const TxStatusReporter& inReporter);
    };

    typedef set<
        pair<NodeId, ServerLocation>,
        less<pair<NodeId, ServerLocation> >,
        StdFastAllocator<pair<NodeId, ServerLocation> >
    > NodeIdAndMDSLocations;
    typedef set<
        StringBufT<32>,
        less<StringBufT<32> >,
        StdFastAllocator<StringBufT<32> >
    > MetaMds;
    enum { kMaxVrStateSize = 512 };

    LogTransmitter&        mLogTransmitter;
    MetaDataSync*          mMetaDataSyncPtr;
    NetManager*            mNetManagerPtr;
    MetaVrSM&              mMetaVrSM;
    int&                   mStatus;
    int64_t                mFileSystemId;
    State                  mState;
    NodeId                 mNodeId;
    string                 mClusterKey;
    string                 mMetaMd;
    MetaMds                mMetaMds;
    MetaVrReconfiguration* mReconfigureReqPtr;
    Locations              mPendingLocations;
    ChangesList            mPendingChangesList;
    Config                 mConfig;
    Locations              mAllUniqueLocations;
    int                    mActiveCount;
    int                    mQuorum;
    bool                   mStartedFlag;
    bool                   mActiveFlag;
    int                    mPendingPrimaryTimeout;
    int                    mPendingBackupTimeout;
    uint32_t               mPendingMaxListenersPerNode;
    seq_t                  mPendingChangeVewMaxLogDistance;
    seq_t                  mEpochSeq;
    seq_t                  mViewSeq;
    MetaVrLogSeq           mCommittedSeq;
    MetaVrLogSeq           mPendingCommitSeq;
    int64_t                mCommittedErrChecksum;
    fid_t                  mCommittedFidSeed;
    int                    mCommittedStatus;
    MetaVrLogSeq           mLastLogSeq;
    MetaVrLogSeq           mReplayLastLogSeq;
    MetaVrLogSeq           mLastCommitSeq;
    MetaVrLogSeq           mLastViewEndSeq;
    MetaVrLogSeq           mLastLogStartViewViewEndSeq;
    MetaVrLogSeq           mLastNonEmptyViewEndSeq;
    MetaVrLogSeq           mPrimaryViewStartSeq;
    MetaVrLogSeq           mHelloCommitSeq;
    time_t                 mLastCommitTime;
    time_t                 mTimeNow;
    time_t                 mLastProcessTime;
    time_t                 mLastReceivedTime;
    time_t                 mLastStartViewTime;
    time_t                 mLastUpTime;
    time_t                 mViewChangeStartTime;
    time_t                 mStateSetTime;
    seq_t                  mStartViewChangeRecvViewSeq;
    MetaVrLogSeq           mStartViewChangeMaxLastLogSeq;
    MetaVrLogSeq           mStartViewChangeMaxCommittedSeq;
    MetaVrLogSeq           mDoViewChangeViewEndSeq;
    int                    mChannelsCount;
    int                    mStartViewEpochMismatchCount;
    int                    mReplyCount;
    bool                   mLogTransmittersSuspendedFlag;
    bool                   mLogStartViewPendingRecvFlag;
    bool                   mCheckLogSyncStatusFlag;
    bool                   mSyncVrStateFileFlag;
    bool                   mPanicOnIoErrorFlag;
    bool                   mIgnoreInvalidVrStateFlag;
    bool                   mScheduleViewChangeFlag;
    NodeId                 mPrimaryNodeId;
    MetaVrStartViewChange* mStartViewChangePtr;
    MetaVrDoViewChange*    mDoViewChangePtr;
    MetaVrStartView*       mStartViewPtr;
    MetaVrLogStartView*    mMetaVrLogStartViewPtr;
    NodeIdSet              mStartViewChangeNodeIds;
    NodeIdSet              mDoViewChangeNodeIds;
    NodeIdSet              mRespondedIds;
    NodeIdAndMDSLocations  mNodeIdAndMDSLocations;
    MetaVrLogSeq           mLogFetchEndSeq;
    ServerLocation         mMetaDataStoreLocation;
    MetaDataSync::Servers  mSyncServers;
    time_t                 mViewChangeInitiationTime;
    bool                   mConfigAlowHostNamesFlag;
    string                 mVrStateFileName;
    string                 mVrStateTmpFileName;
    string                 mVrStateIoStr;
    string                 mTmpBuffer;
    string                 mViewChangeReason;
    string const           mEmptyString;
    string const           mInactiveNodeStatusMsg;
    string const           mBackupNodeStatusMsg;
    BufferInputStream      mInputStream;
    IOBuffer::WOStream     mWOStream;
    QCMutex                mMutex;
    QCCondVar              mReconfigureCompletionCondVar;
    MetaVrReconfiguration* mPendingReconfigureReqPtr;
    MetaVrResponse         mVrResponse;
    char                   mVrStateReadBuffer[kMaxVrStateSize];

    static const VrReconfigurationTable& sVrReconfigurationTable;

    template<typename T>
    static T& DefVrReconfiguration(
        T& inArg)
    {
        return inArg
        .Def("add-node",
            " -- add inactive node with the specified log listeners in"
            " args: ip port ip1 port1 ..., node ID, and, optionally,"
            " primary order",
            &Impl::AddNode,          &Impl::CommitAddNode)
        .Def("remove-nodes",
            " -- remove inactive nodes with the specified IDs",
            &Impl::RemoveNodes,      &Impl::CommitRemoveNodes)
        .Def("activate-nodes",
            " -- activate inactive nodes with the specified IDs in args",
            &Impl::ActivateNodes,    &Impl::CommitActivateNodes)
        .Def("inactivate-nodes",
            " -- inactivate active nodes with the specified IDs",
            & Impl::InactivateNodes, &Impl::CommitInactivateNodes)
        .Def("set-primary-order",
            " -- set / change active or inactive node primary order"
            " args: space separated pairs of <node id> <node order>",
            &Impl::SetPrimaryOrder,  &Impl::CommitSetPrimaryOrder)
        .Def("set-parameters",
            " -- set parameters:\n"
            " primary timeout\n"
            " backup timeout\n"
            " maximum view change log distance\n"
            " maximum number of log listeners per node" ,
            &Impl::SetParameters,    &Impl::CommitSetParameters)
        .Def("add-node-listeners",
            " -- add specified listener for a given node ID",
            &Impl::AddNodeListeners, &Impl::CommitAddNodeListeners)
        .Def("remove-node-listeners",
            " -- remove specified listeners for a given node ID"
            " args: ip port ip1 port1 ...",
            &Impl::RemoveNodeListeners, &Impl::CommitRemoveNodeListeners)
        .Def(MetaVrReconfiguration::GetResetOpName(),
            " -- clear VR configuration; cannot be performed at run time",
            &Impl::ResetConfig,      &Impl::CommitResetConfig)
        .Def(MetaVrReconfiguration::GetInactivateAllNodesName(),
            " -- inactivate all nodes; cannot be performed at run time",
            &Impl::InactivateAllNodes,      &Impl::CommitInactivateAllNodes)
        .Def("swap-nodes",
            " -- swap specified inactive and active nodes, by"
            " making inactive node active, and active node inactive"
            " args: <node ID> <node ID>",
            &Impl::SwapActiveNode,  &Impl::CommitSwapActiveNode)
        .Def("help",
            " -- display help message",
            &Impl::Help,            &Impl::CommitHelp)
        .Def("",
            "",
            &Impl::Help,            &Impl::CommitHelp)
        ;
    }
    static const VrReconfigurationTable& MakVrReconfigurationTable()
    {
        static VrReconfigurationTable sTable;
        if (sTable.empty()) {
            AddVrMethodFunc theFunc(sTable);
            DefVrReconfiguration(theFunc);
        }
        return sTable;
    }
    static int CalcQuorum(
        int inActiveCount)
    {
        return (kMinActiveCount <= inActiveCount ? inActiveCount / 2 + 1 : 0);
    }
    size_t GetUpChannelsCount(
        NodeId inNodeId)
    {
        TxStatusCheckNode theCheck(inNodeId, Locations(), NodeId(-1));
        mLogTransmitter.GetStatus(theCheck);
        return theCheck.GetUpCount();
    }
    void ActivateNodes(
        MetaVrReconfiguration& inReq)
        { ModifyActiveStatus(inReq, true); }
    void InactivateNodes(
        MetaVrReconfiguration& inReq)
        { ModifyActiveStatus(inReq, false); }
    void CommitActivateNodes(
        MetaVrReconfiguration& inReq)
        { CommitModifyActiveStatus(inReq, true); }
    void CommitInactivateNodes(
        MetaVrReconfiguration& inReq)
        { CommitModifyActiveStatus(inReq, false); }
    void Help(
        MetaVrReconfiguration& inReq)
    {
        if (inReq.replayFlag) {
            panic("VR: invalid reconfiguration in replay");
            inReq.status    = -EFAULT;
            inReq.statusMsg = "invalid reconfiguration type";
            return;
        }
        if (0 != inReq.status || inReq.mResponse.BytesConsumable()) {
            return;
        }
        inReq.logAction = MetaRequest::kLogNever;
        ostream& theStream = mWOStream.Set(inReq.mResponse);
        theStream << "VR reconfiguration arguments:\n";
        HelpFunc theFunc(theStream);
        MetaVrReconfiguration::ParserDefSelf(theFunc);
        theStream << "supported operations"
            " (i.e. possible values of op-type argument):\n";
        DefVrReconfiguration(theFunc);
        theStream << "\n";
        mWOStream.Reset();
    }
    void CommitHelp(
        MetaVrReconfiguration& inReq)
    {
        panic("VR: invalid reconfiguration type commit");
        inReq.status    = -EFAULT;
        inReq.statusMsg = "invalid reconfiguration type";
    }
    void SetLastLogSeq(
        const MetaVrLogSeq& inLogSeq)
    {
        mLastLogSeq = inLogSeq;
        if (mLastLogSeq.IsPastViewStart()) {
            mLastNonEmptyViewEndSeq = mLastLogSeq;
        }
    }
    void ScheduleViewChange(
        const char* inMsgPtr   = 0,
        const char* inMsgExPtr = 0)
    {
        if (kStateLogSync == mState || ! mActiveFlag) {
            return;
        }
        if (inMsgPtr && *inMsgPtr &&
                (kStatePrimary == mState || kStateBackup == mState ||
                    kStateReconfiguration == mState)) {
            mViewChangeReason = inMsgPtr;
            if (inMsgExPtr) {
                mViewChangeReason += inMsgExPtr;
            }
            mViewChangeReason += ", node: ";
            AppendDecIntToString(mViewChangeReason, mNodeId);
            mViewChangeInitiationTime = TimeNow();
        }
        mPrimaryNodeId = -1;
        CancelViewChange();
        SetState(kStateViewChange);
        mDoViewChangeViewEndSeq = GetLastViewEndSeq();
        mScheduleViewChangeFlag = true;
        Wakeup();
    }
    template<typename T>
    void ScheduleViewChange(
        const char* inMsgPtr,
        const T&    inMsgEx)
        { ScheduleViewChange(inMsgPtr, inMsgEx.c_str()); }
    bool CheckNodeId(
        NodeId inNodeId)
    {
        if (inNodeId == mNodeId) {
            return true;
        }
        KFS_LOG_STREAM_ERROR <<
            mVrStateFileName <<
            ": valid VR state node id: " << inNodeId <<
            " does not match configuration node id: " << mNodeId <<
            " please ensure that configuration is correct,"
            " node IDs are unique, and if they are, then"
            " remove stale " << mVrStateFileName << " file" <<
        KFS_LOG_EOM;
        return false;
    }
    void Wakeup()
        { mNetManagerPtr->Wakeup(); }
    int GetStatus() const
    {
        return ((! mActiveFlag && 0 < mQuorum) ? -EVRNOTPRIMARY :
            (kStatePrimary == mState ? 0 :
            (kStateBackup == mState ? -EVRBACKUP : -ELOGFAILED)));
    }
    void SetState(
        State inState)
    {
        if (mState != inState) {
            mState  = inState;
            mStatus = GetStatus();
            if (kStatePrimary == mState || kStateBackup == mState) {
                CancelViewChange();
                mDoViewChangeViewEndSeq = MetaVrLogSeq();
            }
        }
        mStateSetTime = TimeNow();
    }
    time_t GetPrimaryLeaseEndTime() const
        {   return (mLastUpTime + mConfig.GetPrimaryTimeout()); }
    bool HasPrimaryTimedOut() const
    {
        return (GetPrimaryLeaseEndTime() < TimeNow());
    }
    void ConfigUpdate()
    {
        if (mStartedFlag) {
            mChannelsCount = mLogTransmitter.Update(mMetaVrSM);
            mStatus        = GetStatus();
        }
    }
    bool ValidateClusterKeyAndMd(
        MetaVrRequest&    inReq,
        seq_t             inSeq,
        NodeId            inNodeId) const
    {
        if (0 != mVrResponse.mClusterKey.Compare(mClusterKey)) {
            KFS_LOG_STREAM_ERROR <<
                " seq: "      << inSeq <<
                " node: "     << inNodeId <<
                " cluster key mismatch: " <<
                " expected: " << mClusterKey <<
                " actual: "   << mVrResponse.mClusterKey <<
                " "           << inReq.Show() <<
            KFS_LOG_EOM;
            return false;
        }
        if (! mMetaMds.empty() &&
                mMetaMds.find(mVrResponse.mMetaMd) == mMetaMds.end()) {
            KFS_LOG_STREAM_ERROR <<
                " seq: "                    << inSeq <<
                " node: "                   << inNodeId <<
                " invalid meta server md: " << mVrResponse.mMetaMd <<
                " "                         << inReq.Show() <<
            KFS_LOG_EOM;
            return false;
        }
        return true;
    }
    void PirmaryCommitStartView()
    {
        CancelViewChange();
        KFS_LOG_STREAM_INFO <<
            "primary: "        << mNodeId <<
            " starting view: " << mEpochSeq <<
            " "                << mViewSeq <<
            " committed: "     << mCommittedSeq <<
            " view end: "      << mLastViewEndSeq <<
            " last log: "      << mLastLogSeq <<
        KFS_LOG_EOM;
        const MetaVrLogSeq theStartSeq(
            mEpochSeq, mViewSeq, kMetaVrLogStartViewLogSeq);
        if (theStartSeq <= mLastLogSeq ||
                theStartSeq.IsSameView(mLastLogSeq) ||
                mLastViewEndSeq < mCommittedSeq ||
                ! mLastViewEndSeq.IsPastViewStart()) {
            panic("VR: invalid epoch, view, last log. or committed sequence");
            return;
        }
        SetState(kStatePrimary);
        mPrimaryNodeId  = mNodeId;
        mLastUpTime     = TimeNow();
        mLastCommitTime = mLastUpTime;
        MetaRequest::Release(mMetaVrLogStartViewPtr);
        mPrimaryViewStartSeq   = theStartSeq;
        mMetaVrLogStartViewPtr = new MetaVrLogStartView();
        mMetaVrLogStartViewPtr->mCommittedSeq = mLastViewEndSeq;
        mMetaVrLogStartViewPtr->mNewLogSeq    = mPrimaryViewStartSeq;
        mMetaVrLogStartViewPtr->mNodeId       = mNodeId;
        mMetaVrLogStartViewPtr->mTime         = TimeNow();
        if (! mMetaVrLogStartViewPtr->Validate()) {
            panic("VR: invalid log start op");
        }
        // Force heartbeats now.
        mLogTransmitter.SetHeartbeatInterval(mConfig.GetPrimaryTimeout());
        Wakeup();
    }
    void AdvanceView(
        const char* inMsgPtr)
    {
        if (mQuorum <= 0 || ! mActiveFlag) {
            return;
        }
        KFS_LOG_STREAM_INFO <<
            "advance view: " << (inMsgPtr ? inMsgPtr : "") <<
            " epoch: "       << mEpochSeq <<
            " view: "        << mViewSeq <<
            " state: "       << GetStateName(mState) <<
            " quorum: "      << mQuorum <<
            " active: "      << mActiveCount <<
            " last log: "    << mLastLogSeq <<
            " committed: "   << mCommittedSeq <<
        KFS_LOG_EOM;
        mViewSeq++;
        UpdateViewAndStartViewChange(inMsgPtr);
    }
    void ResetConfig()
    {
        mConfig.Clear();
        mAllUniqueLocations.clear();
        mActiveCount = 0;
        mQuorum      = 0;
        mEpochSeq    = 0;
        mViewSeq     = 0;
    }
    time_t TimeNow() const
        { return mTimeNow; }
    void Show(
        const MetaVrRequest& inReq,
        const char*          inPrefixPtr = "")
    {
        KFS_LOG_STREAM(0 == inReq.status ?
                MsgLogger::kLogLevelINFO :
                MsgLogger::kLogLevelERROR) <<
            inPrefixPtr <<
            "seq: "     << inReq.opSeqno        <<
            " status: " << inReq.status         <<
            " "         << inReq.statusMsg      <<
            " state: "  << GetStateName(mState) <<
            " DVC: "    <<
                reinterpret_cast<const void*>(mDoViewChangePtr) <<
            " active: " << mActiveFlag          <<
            " epoch: "  << mEpochSeq            <<
            " view: "   << mViewSeq             <<
            " last: "   << mLastLogSeq          <<
            " "         << inReq.Show()         <<
        KFS_LOG_EOM;
    }
    void Show(
        MetaVrRequest&        inReq,
        seq_t                 inSeq,
        const Properties&     inProps,
        NodeId                inNodeId,
        const ServerLocation& inPeer) const
    {
        KFS_LOG_STREAM_DEBUG <<
            "-seq: "      << inSeq <<
            " node: "     << inNodeId <<
            " peer: "     << inPeer <<
            " state: "    << GetStateName(mState) <<
            " DVC: "      <<
                reinterpret_cast<const void*>(mDoViewChangePtr) <<
            " view: "     << mEpochSeq <<
            " "           << mViewSeq <<
            " replies: "  << mReplyCount <<
            " reponse: "  << mVrResponse <<
            " "           << inReq.Show() <<
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
            "state: "           << GetStateName(mState) <<
            " epoch: "          << mEpochSeq <<
            " view: "           << mViewSeq <<
            " change started: " << (TimeNow() - mViewChangeStartTime) <<
                " sec. ago"
            " responded: "      << mRespondedIds.size() <<
            " replies: "        << mReplyCount <<
            " DVC: "            <<
                reinterpret_cast<const void*>(mDoViewChangePtr) <<
            " error: "          << inMsgPtr <<
        KFS_LOG_EOM;
        if ((mDoViewChangePtr ? 0 < mReplyCount :
                (mActiveCount <= (int)mRespondedIds.size() ||
                mChannelsCount <= mReplyCount))) {
            mLastReceivedTime = TimeNow();
            if (mDoViewChangePtr) {
                mViewSeq++;
            } else if (mViewSeq < mStartViewChangeRecvViewSeq) {
                mViewSeq = mStartViewChangeRecvViewSeq + 1;
            }
            UpdateViewAndStartViewChange();
        }
    }
    const MetaVrLogSeq& GetLastViewEndSeq() const
    {
        return (
            mLastViewEndSeq.IsSameView(mLastLogSeq) ?
            mLastViewEndSeq :
            (mLastNonEmptyViewEndSeq.IsSameView(mLastLogSeq) ?
                mLastNonEmptyViewEndSeq :
                mLastLogStartViewViewEndSeq)
        );
    }
    template<typename T>
    static const MetaVrLogSeq& GetReqLastViewEndSeq(
        const T& inReq)
    {
        return (
            inReq.mLastViewEndSeq.IsSameView(inReq.mLastLogSeq) ?
            inReq.mLastViewEndSeq :
            (inReq.mLastLogSeq.IsPastViewStart() ?
                inReq.mLastLogSeq : kInvalidVrLogSeq)
        );
    }
    template<typename T>
    void StartDoViewChangeIfPossible(
        const T& inReq)
    {
        const MetaVrLogSeq& theReqViewEndSeq = GetReqLastViewEndSeq(inReq);
        if (theReqViewEndSeq < mDoViewChangeViewEndSeq &&
                theReqViewEndSeq.IsValid()) {
            if (theReqViewEndSeq < mCommittedSeq ||
                    ! theReqViewEndSeq.IsPastViewStart()) {
                panic("VR: invalid do view chane view end sequence");
                return;
            }
            mDoViewChangeViewEndSeq = theReqViewEndSeq;
        }
        StartDoViewChangeIfPossible();
    }
    void StartDoViewChangeIfPossible()
    {
        if (! mActiveFlag || ! mDoViewChangeViewEndSeq.IsValid()) {
            panic("VR: invalid start view change attempt");
            return;
        }
        if (mDoViewChangePtr) {
            RetryStartViewChange("do view change timed out");
            return;
        }
        if (mLastLogSeq < mStartViewChangeMaxLastLogSeq) {
            // Need to feetch log / checkpoint.
            if (mActiveFlag && mNodeIdAndMDSLocations.empty()) {
                panic("VR: invalid empty committed node ids");
            }
            if (mActiveFlag && mMetaDataSyncPtr) {
                mSyncServers.clear();
                for (NodeIdAndMDSLocations::const_iterator theIt =
                        mNodeIdAndMDSLocations.begin();
                        mNodeIdAndMDSLocations.end() != theIt;
                        ++theIt) {
                    if (! theIt->second.IsValid()) {
                        continue;
                    }
                    NodeId const          theNodeId   = theIt->first;
                    const ServerLocation& theLocation = theIt->second;
                    if (theNodeId != mNodeId && IsActive(theNodeId) &&
                            find(mSyncServers.begin(), mSyncServers.end(),
                                theLocation) == mSyncServers.end()) {
                        mSyncServers.push_back(theLocation);
                    }
                }
                if (mSyncServers.empty()) {
                    KFS_LOG_STREAM_ERROR <<
                        GetStateName(mState) <<
                        ": no valid meta data store addresses available" <<
                    KFS_LOG_EOM;
                    return;
                }
                if (kStateLogSync != mState ||
                        mLogFetchEndSeq < mStartViewChangeMaxLastLogSeq) {
                    KFS_LOG_STREAM_INFO <<
                        "scheduling log fetch:"
                        " state: "    << GetStateName(mState) <<
                        " servers:"
                        " total: "    << mSyncServers.size() <<
                        " first: "    << mSyncServers.front() <<
                        " ["          << mLastLogSeq <<
                        ","           << mStartViewChangeMaxLastLogSeq <<
                        "]"           <<
                        " prev end: " << mLogFetchEndSeq <<
                    KFS_LOG_EOM;
                    SetState(kStateLogSync);
                    mLogFetchEndSeq = mStartViewChangeMaxLastLogSeq;
                    const bool theAllowNonPrimaryFlag =
                        mIgnoreInvalidVrStateFlag || mLastViewEndSeq.IsValid();
                    mMetaDataSyncPtr->ScheduleLogSync(
                        mSyncServers,
                        mLastLogSeq,
                        mStartViewChangeMaxLastLogSeq,
                        theAllowNonPrimaryFlag
                    );
                }
            }
            return;
        }
        if (mReplayLastLogSeq != mLastLogSeq) {
            return;
        }
        if (mLastLogSeq.IsPastViewStart() &&
                ! mLastLogSeq.IsSameView(mDoViewChangeViewEndSeq)) {
            mStartViewChangeNodeIds.clear();
            RetryStartViewChange("fetched and replayed next non empty view");
            return;
        }
        const int theSz = (int)mStartViewChangeNodeIds.size();
        if (theSz < mQuorum) {
            RetryStartViewChange("not sufficient number of nodes responded");
            return;
        }
        if (theSz < mActiveCount &&
                (int)mRespondedIds.size() < mActiveCount &&
                mReplyCount < mChannelsCount &&
                TimeNow() <=
                    mViewChangeStartTime + mConfig.GetPrimaryTimeout()) {
            // Wait for more nodes to repsond.
            return;
        }
        if (mStartViewChangeNodeIds.find(mNodeId) ==
                mStartViewChangeNodeIds.end()) {
            RetryStartViewChange("no start view change from self received");
            return;
        }
        if (! mIgnoreInvalidVrStateFlag && ! mLastViewEndSeq.IsValid() &&
                theSz - 1 < mQuorum) {
            RetryStartViewChange("not sufficient number of noded responded,"
                " excluding this node");
            return;
        }
        const NodeId thePrimaryId = GetPrimaryId();
        if (thePrimaryId < 0) {
            RetryStartViewChange("no primary available");
            return;
        }
        StartDoViewChange(thePrimaryId);
    }
    bool IsActiveSelf(
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
    bool IsActive(
        NodeId inNodeId) const
        { return (mStartedFlag && IsActiveSelf(inNodeId)); }
    void SetReturnState(
        MetaVrRequest& inReq) const
    {
        inReq.mRetCurViewSeq             = mViewSeq;
        inReq.mRetCurEpochSeq            = mEpochSeq;
        inReq.mRetCurState               = mState;
        inReq.mRetCommittedSeq           = mCommittedSeq;
        inReq.mRetCommittedErrChecksum   = mCommittedErrChecksum;
        inReq.mRetCommittedStatus        = mCommittedStatus;
        inReq.mRetCommittedFidSeed       = mCommittedFidSeed;
        inReq.mRetLastLogSeq             = mLastLogSeq;
        inReq.mRetFileSystemId           = mFileSystemId;
        inReq.mRetClusterKey             = mClusterKey;
        inReq.mRetMetaMd                 = mMetaMd;
        inReq.mRetLastViewEndSeq         = mLastViewEndSeq;
        inReq.mRetLastNonEmptyViewEndSeq = mLastNonEmptyViewEndSeq;
        inReq.mRetMetaDataStoreLocation  = mMetaDataStoreLocation;
    }
    template<typename T>
    bool VerifyClusterKey(
        const T& inReq,
        int&     outStatus,
        string&  outStatusMsg) const
    {
        if (inReq.mClusterKey != mClusterKey) {
            outStatusMsg = "cluster key does not match";
        } else if (! mMetaMds.empty() &&
                mMetaMds.find(inReq.mMetaMd) == mMetaMds.end()) {
            outStatusMsg = "invalid meta server executable's MD";
        } else if (mFileSystemId != inReq.mFileSystemId) {
            outStatusMsg = "file system ID does not match";
        } else {
            return true;
        }
        outStatus = -EBADCLUSTERKEY;
        return false;
    }
    template<typename T>
    bool VerifyViewChange(
        const T& inReq,
        MetaOp   inOp,
        NodeId   inNodeId,
        int&     outStatus,
        string&  outStatusMsg) const
    {
        if (! mActiveFlag) {
            outStatus    = -ENOENT;
            outStatusMsg = "node inactive";
        } else if (! IsActive(inNodeId)) {
            outStatus    = -EINVAL;
            outStatusMsg = "request from inactive node";
        } else if (inReq.mEpochSeq < inReq.mLastLogSeq.mEpochSeq ||
                (inReq.mEpochSeq == inReq.mLastLogSeq.mEpochSeq &&
                inReq.mViewSeq <= inReq.mLastLogSeq.mViewSeq)) {
            outStatus    = -EINVAL;
            outStatusMsg = "invalid request: last log sequence";
        } else if (inReq.mLastViewEndSeq.IsValid() &&
                ! inReq.mLastViewEndSeq.IsPastViewStart()) {
            outStatus    = -EINVAL;
            outStatusMsg = "invalid request: last view end sequence";
        } else if (inReq.mLastLogSeq < inReq.mLastViewEndSeq) {
            outStatus    = -EINVAL;
            outStatusMsg = "invalid request: last view end sequence";
        } else if (inReq.mLastLogSeq < mCommittedSeq &&
                (kStatePrimary != mState && kStateBackup != mState)) {
            outStatus    = -EINVAL;
            outStatusMsg = "last log less than current committed";
        } else if (mEpochSeq != inReq.mEpochSeq) {
            outStatus    = -EINVAL;
            outStatusMsg = "epoch does not match";
        } else if (inReq.mViewSeq < mViewSeq) {
            outStatus    = -EINVAL;
            outStatusMsg = "lower view sequence";
        } else if (mLastLogSeq != inReq.mLastLogSeq &&
                (kStatePrimary != mState ||
                    (inReq.mLastLogSeq < mCommittedSeq &&
                        (inReq.mLastLogSeq.mEpochSeq != mLastLogSeq.mEpochSeq ||
                        inReq.mLastLogSeq.mViewSeq != mLastLogSeq.mViewSeq ||
                        inReq.mLastLogSeq.mLogSeq +
                                mConfig.GetChangeVewMaxLogDistance() <
                            mLastLogSeq.mLogSeq
                )))) {
            outStatus    = -EINVAL;
            outStatusMsg = "log sequence mismatch";
        } else {
            const MetaVrLogSeq theViewEndSeq = GetReqLastViewEndSeq(inReq);
            if (theViewEndSeq.IsValid()) {
                if (theViewEndSeq < mCommittedSeq) {
                    outStatus    = -EINVAL;
                    outStatusMsg = "end less than committed";
                } else if (mLastLogSeq < theViewEndSeq) {
                    outStatus    = -EINVAL;
                    outStatusMsg = "end past last log";
                }
            } else {
                if (mLastLogSeq != inReq.mLastLogSeq &&
                        kStatePrimary != mState &&
                        (META_VR_START_VIEW != inOp ||
                                mLastLogSeq.mEpochSeq != inReq.mEpochSeq ||
                                mLastLogSeq.mViewSeq != inReq.mViewSeq)) {
                    outStatus    = -EINVAL;
                    outStatusMsg = "ignored, last log mismatch with"
                        " no valid end view sequence";
                }
            }
            if (0 == outStatus) {
                return true;
            }
        }
        return false;
    }
    bool VerifyClusterKey(
        MetaVrRequest& inReq) const
    {
        return VerifyClusterKey(inReq, inReq.status, inReq.statusMsg);
    }
    bool VerifyViewChange(
        MetaVrRequest& inReq) const
    {
        if (VerifyClusterKey(inReq) &&
                VerifyViewChange(
                    inReq,
                    inReq.op,
                    inReq.mNodeId,
                    inReq.status,
                    inReq.statusMsg)) {
            return true;
        }
        SetReturnState(inReq);
        return false;
    }
    static ServerLocation GetDataStoreLocation(
        const MetaVrResponse& inVrResponse,
        const ServerLocation& inPeer)
    {
        ServerLocation theLocation;
        theLocation.port = inVrResponse.mMetaDataStoreLocation.port;
        if (0 < theLocation.port) {
            theLocation.hostname = inVrResponse.mMetaDataStoreLocation.hostname;
            if (theLocation.hostname.empty() ||
                    ! TcpSocket::IsValidConnectToAddress(theLocation)) {
                theLocation.hostname = inPeer.hostname;
                if (! TcpSocket::IsValidConnectToAddress(theLocation)) {
                    theLocation.hostname.clear();
                    theLocation.port = -theLocation.port;
                }
            }
        }
        return theLocation;
    }
    static ServerLocation GetDataStoreLocation(
        const MetaVrRequest& inReq)
    {
        if (inReq.mMetaDataStorePort < 0) {
            return ServerLocation();
        }
        ServerLocation theLocation(
            inReq.mMetaDataStoreHost.empty() ?
                inReq.clientIp : inReq.mMetaDataStoreHost,
            inReq.mMetaDataStorePort
        );
        if (! inReq.mMetaDataStoreHost.empty() &&
                ! TcpSocket::IsValidConnectToAddress(theLocation)) {
            theLocation.hostname = inReq.clientIp;
        }
        if (! TcpSocket::IsValidConnectToAddress(theLocation)) {
            theLocation.hostname.clear();
            theLocation.port = - theLocation.port;
        }
        return theLocation;
    }
    void ScheduleLogFetch(
        const MetaVrLogSeq&   inLogSeq,
        const ServerLocation& inLocation,
        bool                  inAllowNonPrimaryFlag)
    {
        if (inLocation.IsValid() && mLogFetchEndSeq < inLogSeq) {
            KFS_LOG_STREAM_INFO <<
                "scheduling log fetch:"
                " "                    << inLocation <<
                " ["                   << mLastLogSeq <<
                ","                    << inLogSeq <<
                "]"
                " allow non primary: " << inAllowNonPrimaryFlag <<
            KFS_LOG_EOM;
            SetState(kStateLogSync);
            mSyncServers.clear();
            mSyncServers.push_back(inLocation);
            mLogFetchEndSeq = inLogSeq;
            mMetaDataSyncPtr->ScheduleLogSync(
                mSyncServers,
                mLastLogSeq,
                inLogSeq,
                inAllowNonPrimaryFlag
            );
        }
    }
    void CheckPrimaryState(
        const MetaVrRequest& inReq)
    {
        if (mActiveFlag &&
                kStateBackup == mState &&
                inReq.mNodeId == mPrimaryNodeId &&
                kStatePrimary != inReq.mCurState &&
                kStateReconfiguration != inReq.mCurState &&
                mLastStartViewTime + mConfig.GetBackupTimeout() < TimeNow()) {
            AdvanceView("backup primary timed out");
        }
    }
    void ScheduleCommitIfNeeded(
        MetaVrRequest& inReq)
    {
        if (mNodeId != inReq.mNodeId &&
                mCommittedSeq < inReq.mCommittedSeq &&
                inReq.mCommittedSeq <= mLastLogSeq &&
                0 <= inReq.mCommittedFidSeed &&
                0 <= inReq.mCommittedStatus) {
            KFS_LOG_STREAM_DEBUG <<
                "scheduling commit: " << mCommittedSeq <<
                " => "                << inReq.mCommittedSeq <<
                " last log: "         << mLastLogSeq <<
                " "                   << inReq.Show() <<
            KFS_LOG_EOM;
            inReq.SetScheduleCommit();
            if (kStateBackup != mState ||
                    inReq.mCurState != kStatePrimary ||
                    inReq.mNodeId != mPrimaryNodeId) {
                ScheduleViewChange("primary mismatch");
                mPendingCommitSeq = inReq.mCommittedSeq;
            }
        }
    }
    bool Handle(
        MetaVrHello& inReq)
    {
        if (0 == inReq.status && VerifyClusterKey(inReq) &&
                kStatePrimary == inReq.mCurState &&
                0 < mQuorum && mActiveFlag) {
            if (kStatePrimary == mState && inReq.mNodeId != mNodeId) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "cuuent node state: ";
                inReq.statusMsg += GetStateName(mState);
            }
        }
        Show(inReq);
        SetReturnState(inReq);
        if (0 == inReq.status &&
                mNodeId != inReq.mNodeId &&
                kStatePrimary == inReq.mCurState &&
                kStateLogSync != mState) {
            if (mLastLogSeq < inReq.mLastLogSeq &&
                    (! mActiveFlag || mQuorum <= 0)) {
                const bool kAllowNonPrimaryFlag = false;
                ScheduleLogFetch(
                    inReq.mLastLogSeq,
                    GetDataStoreLocation(inReq),
                    kAllowNonPrimaryFlag
                );
            } else {
                ScheduleCommitIfNeeded(inReq);
            }
        }
        CheckPrimaryState(inReq);
        return true;
    }
    bool Handle(
        MetaVrStartViewChange& inReq)
    {
        Show(inReq, "+");
        if (VerifyViewChange(inReq)) {
            if (mViewSeq != inReq.mViewSeq) {
                if (kStatePrimary != mState &&
                        ! mIgnoreInvalidVrStateFlag &&
                        ! inReq.mLastViewEndSeq.IsValid()) {
                    inReq.status    = -EINVAL;
                    inReq.statusMsg = "not primary, state: ";
                    inReq.statusMsg += GetStateName(mState);
                } else if (kStatePrimary == mState &&
                        inReq.mNodeId != mNodeId &&
                        GetUpChannelsCount(inReq.mNodeId) <= 0) {
                    inReq.status    = -EINVAL;
                    inReq.statusMsg = "primary has no up channels with: ";
                    AppendDecIntToString(inReq.statusMsg, inReq.mNodeId);
                } else if (kStateLogSync != mState &&
                        (kStateBackup != mState ||
                            mPrimaryNodeId < 0 ||
                            mPrimaryNodeId == inReq.mNodeId ||
                            mLogStartViewPendingRecvFlag ||
                            inReq.mNodeId == mPrimaryNodeId ||
                            mLastReceivedTime + mConfig.GetBackupTimeout() <
                                TimeNow())) {
                    mViewSeq = inReq.mViewSeq;
                    StartViewChange(inReq);
                    if (kStateViewChange == mState &&
                            ! mDoViewChangePtr &&
                            mStartViewChangeNodeIds.insert(
                                inReq.mNodeId).second) {
                        StartDoViewChangeIfPossible(inReq);
                    }
                }
            } else {
                if (kStateViewChange == mState) {
                    if (! mIgnoreInvalidVrStateFlag &&
                            ! inReq.mLastViewEndSeq.IsValid() &&
                            ! mLastViewEndSeq.IsValid() &&
                            mNodeId != inReq.mNodeId) {
                        inReq.status    = -EINVAL;
                        inReq.statusMsg =
                            "not primary; no valid saved VR state; state: ";
                        inReq.statusMsg += GetStateName(mState);
                    } else  if (! mDoViewChangePtr &&
                            mStartViewChangeNodeIds.insert(
                                inReq.mNodeId).second) {
                        StartDoViewChangeIfPossible(inReq);
                    }
                } else {
                    inReq.status    = -EINVAL;
                    inReq.statusMsg = "ignored, state: ";
                    inReq.statusMsg += GetStateName(mState);
                }
            }
            SetReturnState(inReq);
        } else if (inReq.status != -EBADCLUSTERKEY &&
                (kStateViewChange == mState || mEpochSeq < inReq.mEpochSeq) &&
                ! mDoViewChangePtr) {
            if (mLastLogSeq < inReq.mLastLogSeq &&
                    (mIgnoreInvalidVrStateFlag || mLastViewEndSeq.IsValid())) {
                const bool theAllowNonPrimaryFlag = true;
                ScheduleLogFetch(
                    inReq.mLastLogSeq,
                    GetDataStoreLocation(inReq),
                    theAllowNonPrimaryFlag
                );
            } else {
                ScheduleCommitIfNeeded(inReq);
            }
        }
        Show(inReq, "=");
        CheckPrimaryState(inReq);
        return true;
    }
    bool Handle(
        MetaVrDoViewChange& inReq)
    {
        Show(inReq, "+");
        if (VerifyViewChange(inReq)) {
            if (mViewSeq != inReq.mViewSeq) {
                if (kStateLogSync != mState) {
                    mViewSeq = inReq.mViewSeq;
                    StartViewChange(inReq);
                }
            } else if (mNodeId != inReq.mPrimaryNodeId) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "primary node id mismatch, state: ";
                inReq.statusMsg += GetStateName(mState);
            } else if (mLastLogSeq < inReq.mLastViewEndSeq) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "invalid last view end sequence, state: ";
                inReq.statusMsg += GetStateName(mState);
            } else if (inReq.mLastViewEndSeq != mLastViewEndSeq) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "last view end mismatch, state: ";
                inReq.statusMsg += GetStateName(mState);
            } else {
                if (kStateViewChange == mState) {
                    if (mDoViewChangeNodeIds.insert(inReq.mNodeId).second &&
                            mQuorum <= (int)mDoViewChangeNodeIds.size()) {
                        KFS_LOG_STREAM_INFO <<
                            "start view: " << mViewSeq <<
                            " epoch: "     << mEpochSeq <<
                            " log seq: "   << mLastLogSeq <<
                        KFS_LOG_EOM;
                        StartView();
                    } else if (! mDoViewChangePtr &&
                            mStartViewChangeNodeIds.insert(
                                inReq.mNodeId).second) {
                        // Treat as start view change, in the case when do view
                        // change has not been started yet.
                        StartDoViewChangeIfPossible(inReq);
                    }
                } else if (kStateStartViewPrimary != mState &&
                        kStatePrimary != mState) {
                    if (kStateBackup == mState) {
                        AdvanceView("backup do view change");
                    }
                    inReq.status    = -EINVAL;
                    inReq.statusMsg = "ignored, state: ";
                    inReq.statusMsg += GetStateName(mState);
                }
            }
            SetReturnState(inReq);
        }
        Show(inReq, "=");
        CheckPrimaryState(inReq);
        return true;
    }
    bool Handle(
        MetaVrStartView& inReq)
    {
        Show(inReq, "+");
        if (VerifyViewChange(inReq)) {
            if (mViewSeq != inReq.mViewSeq ||
                    ! inReq.mLastViewEndSeq.IsValid() ||
                    (! inReq.mLastViewEndSeq.IsSameView(
                            mLastNonEmptyViewEndSeq) &&
                        inReq.mLastViewEndSeq != mLastViewEndSeq
                    ) ||
                    inReq.mLastViewEndSeq < mCommittedSeq ||
                    mLastLogSeq < inReq.mLastViewEndSeq ||
                    inReq.mLastViewEndSeq < mLastViewEndSeq ||
                    (kStateViewChange != mState &&
                        kStateBackup != mState &&
                        kStatePrimary != mState &&
                        kStateStartViewPrimary != mState) ||
                    (mDoViewChangePtr &&
                        inReq.mNodeId != mDoViewChangePtr->mPrimaryNodeId) ||
                    ((kStateStartViewPrimary == mState ||
                        kStatePrimary == mState) && mNodeId != inReq.mNodeId)) {
                if (kStateBackup == mState && inReq.mLastViewEndSeq.IsValid()) {
                    mViewSeq = inReq.mViewSeq;
                    AdvanceView("backup start view");
                }
                if (kStatePrimary != mState || mNodeId != inReq.mNodeId) {
                    inReq.status    = -EINVAL;
                    inReq.statusMsg = "ignored, state: ";
                    inReq.statusMsg += GetStateName(mState);
                }
            } else {
                if (mNodeId == inReq.mNodeId) {
                    if (kStateStartViewPrimary != mState &&
                            kStatePrimary != mState) {
                        inReq.status    = -EINVAL;
                        inReq.statusMsg = "primary invalid state: ";
                        inReq.statusMsg += GetStateName(mState);
                    }
                } else {
                    const Config::Nodes& theNodes = mConfig.GetNodes();
                    const Config::Nodes::const_iterator theOtherIt =
                        theNodes.find(inReq.mNodeId);
                    const Config::Nodes::const_iterator thisIt     =
                        theNodes.find(mNodeId);
                    if (theNodes.end() == theOtherIt ||
                            theNodes.end() == thisIt) {
                        panic("VR: validation failure");
                        inReq.status    = -EFAULT;
                        inReq.statusMsg = "validation failure";
                    } else if (mReplayLastLogSeq != mLastLogSeq ||
                            thisIt->second.GetPrimaryOrder() <
                                theOtherIt->second.GetPrimaryOrder() ||
                            (thisIt->second.GetPrimaryOrder() ==
                                theOtherIt->second.GetPrimaryOrder() &&
                                mNodeId < inReq.mNodeId)) {
                        const char* const kMsgPtr =
                            mReplayLastLogSeq != mLastLogSeq ?
                            "start view while log replay is in progress" :
                            "start view from grepater primary order node";
                        AdvanceView(kMsgPtr);
                        inReq.status    = -EINVAL;
                        inReq.statusMsg = kMsgPtr;
                        inReq.statusMsg += ", ignored, state: ";
                        inReq.statusMsg += GetStateName(mState);
                    } else {
                        if (! mDoViewChangePtr) {
                            WriteVrState(inReq.mNodeId, inReq.mLastViewEndSeq);
                        }
                        CancelViewChange();
                        SetState(kStateBackup);
                        // Mark as not valid until replay
                        mLogStartViewPendingRecvFlag = true;
                        mPrimaryNodeId               = inReq.mNodeId;
                        mLastReceivedTime            = TimeNow();
                        mLastStartViewTime           = mLastReceivedTime;
                    }
                }
            }
            SetReturnState(inReq);
        }
        Show(inReq, "=");
        CheckPrimaryState(inReq);
        return true;
    }
    bool Handle(
        MetaVrReconfiguration& inReq)
    {
        if (0 != inReq.status ||
                kVrReconfCountNoCommitAdvance <= inReq.mHandledCount) {
            if (inReq.replayFlag) {
                panic("VR: invalid reconfiguration request replay");
            }
            return true;
        }
        if (inReq.logseq.IsValid()) {
            if (mStartedFlag) {
                if (kVrReconfCountNotHandled < inReq.mHandledCount) {
                    inReq.mHandledCount = kVrReconfCountNoCommitAdvance;
                    if (! inReq.replayFlag && 0 == inReq.status) {
                        // Submit request to flush log.
                        submit_request(new MetaNoop());
                    }
                    return true;
                }
                QCStMutexLocker theLocker(mMutex);
                if (mPendingReconfigureReqPtr) {
                    // Invocations of MetaVrReconfiguration::handle()
                    // (one that effectively ends up here) must be serialized.
                    panic("VR: invalid pending reconfiguration in replay");
                    inReq.status = -EFAULT;
                } else {
                    mReconfigureReqPtr = 0;
                    mPendingReconfigureReqPtr = &inReq;
                    Wakeup();
                    while (&inReq == mPendingReconfigureReqPtr) {
                        mReconfigureCompletionCondVar.Wait(mMutex);
                    }
                }
            } else {
                mReconfigureReqPtr = 0;
                StartReconfiguration(inReq);
                mReconfigureReqPtr = 0;
                Commit(inReq);
            }
            // If request is not coming from replay, then prepare and commit are
            // handled by the log writer, below in the else close and by
            // Commit().
        } else {
            if (inReq.replayFlag ||
                    kVrReconfCountNotHandled < inReq.mHandledCount) {
                panic("VR: invalid reconfiguration request");
                inReq.status    = -EFAULT;
                inReq.statusMsg = "invalid reconfiguration request";
                return true;
            }
            if (MetaRequest::kLogIfOk != inReq.logAction &&
                    MetaRequest::kLogAlways != inReq.logAction) {
                return false;
            }
            StartReconfiguration(inReq);
            if (0 != inReq.status) {
                inReq.logAction = MetaRequest::kLogNever;
            }
        }
        return (0 != inReq.status);
    }
    void StartReconfiguration(
        MetaVrReconfiguration& inReq)
    {
        if (0 != inReq.status) {
            return;
        }
        if ((inReq.logseq.IsValid() ?
                (! inReq.replayFlag && ! mStartedFlag) :
                kStatePrimary != mState)) {
            inReq.status    = inReq.logseq.IsValid() ?
                -EINVAL : (0 == mStatus ? -EVRNOTPRIMARY : mStatus);
            inReq.statusMsg = "reconfiguration is not possible, state: ";
            inReq.statusMsg += GetStateName(mState);
            return;
        }
        if (mReconfigureReqPtr) {
            inReq.status    = -EAGAIN;
            inReq.statusMsg = "reconfiguration is in progress";
            return;
        }
        if (! inReq.logseq.IsValid() && mNodeId < 0) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "reconfiguration: node id is not configured or"
                " valid; node id can be configured by setting the following"
                " configuration file parameter: ";
            inReq.statusMsg += kMetaVrParametersPrefixPtr;
            inReq.statusMsg += "id";
            return;
        }
        VrReconfigurationTable::const_iterator const theIt =
            sVrReconfigurationTable.find(NameToken(inReq.mOpType.c_str()));
        if (sVrReconfigurationTable.end() == theIt) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "invalid operation type: ";
            inReq.statusMsg.append(inReq.mOpType.data(), inReq.mOpType.size());
            return;
        }
        (this->*(theIt->second.first))(inReq);
        if (0 == inReq.status && MetaRequest::kLogNever != inReq.logAction) {
            mReconfigureReqPtr = &inReq;
            if (! inReq.logseq.IsValid()) {
                mLastUpTime     = TimeNow();
                mLastCommitTime = mLastUpTime;
            }
        }
    }
    bool Handle(
        MetaVrGetStatus& inReq)
    {
        inReq.logAction = MetaRequest::kLogNever;
        if (0 != inReq.status) {
            return true;
        }
        if (mConfig.IsEmpty()) {
            inReq.status    = -ENOENT;
            inReq.statusMsg = "VR is not configured";
            return true;
        }
        ostream& theStream = mWOStream.Set(inReq.mResponse);
        WriteStatus(theStream, ": ", "\n", "\n");
        theStream << "\n";
        mWOStream.Reset();
        return true;
    }
    bool Handle(
        MetaPing& inReq)
    {
        inReq.logAction = MetaRequest::kLogNever;
        if (0 == inReq.status && ! mConfig.IsEmpty() && inReq.resp.IsEmpty()) {
            ostream& theStream = mWOStream.Set(inReq.resp);
            WriteStatus(theStream, "=", ";", "");
            mWOStream.Reset();
        }
        inReq.vrNodeId        = mNodeId;
        inReq.vrPrimaryNodeId = mPrimaryNodeId;
        inReq.vrActiveFlag    = mActiveFlag;
        return true;
    }
    void WriteStatus(
        ostream&    inStream,
        const char* inSepPtr,
        const char* inDelimPtr,
        const char* inSectionsDelimPtr)
    {
        mTmpBuffer = "vr.";
        inStream << mTmpBuffer <<
            "nodeId"               << inSepPtr << mNodeId                   <<
                inDelimPtr << mTmpBuffer <<
            "status"               << inSepPtr << mStatus                   <<
                inDelimPtr << mTmpBuffer <<
            "active"               << inSepPtr << mActiveFlag               <<
                inDelimPtr << mTmpBuffer <<
            "state"                << inSepPtr << GetStateName(mState)      <<
                inDelimPtr << mTmpBuffer <<
            "primaryId"            << inSepPtr << mPrimaryNodeId            <<
                inDelimPtr << mTmpBuffer <<
            "epoch"                << inSepPtr << mEpochSeq                 <<
                inDelimPtr << mTmpBuffer <<
            "view"                 << inSepPtr << mViewSeq                  <<
                inDelimPtr << mTmpBuffer <<
            "log"                  << inSepPtr << mLastLogSeq               <<
                inDelimPtr << mTmpBuffer <<
            "commit"               << inSepPtr << mCommittedSeq             <<
                inDelimPtr << mTmpBuffer <<
            "lastViewEnd"          << inSepPtr << mLastViewEndSeq           <<
                inDelimPtr << mTmpBuffer <<
            "quorum"               << inSepPtr << mQuorum                   <<
                inDelimPtr << mTmpBuffer <<
            "ignoreInvalidVrState" << inSepPtr << mIgnoreInvalidVrStateFlag <<
                inDelimPtr << mTmpBuffer <<
            "fileSystemId"         << inSepPtr << mFileSystemId             <<
                inDelimPtr << mTmpBuffer <<
            "clusterKey"           << inSepPtr << mClusterKey               <<
                inDelimPtr << mTmpBuffer <<
            "metaMd5"              << inSepPtr << mMetaMd                   <<
                inDelimPtr << mTmpBuffer <<
            "viewChangeReason"     << inSepPtr << escapeString(
                mViewChangeReason.data(), mViewChangeReason.size(), '%', "=;") <<
                inDelimPtr << mTmpBuffer <<
            "viewChangeStartTime"  << inSepPtr << mViewChangeInitiationTime <<
                inDelimPtr << mTmpBuffer <<
            "currentTime"          << inSepPtr << mTimeNow                  <<
                inDelimPtr <<
            inSectionsDelimPtr
        ;
        mTmpBuffer = "logTransmitter.";
        const size_t theSize = mTmpBuffer.size();
        TxStatusReporter theReporter(
            mPrimaryNodeId, mTmpBuffer, inStream, inSepPtr, inDelimPtr);
        mLogTransmitter.GetStatus(theReporter);
        mTmpBuffer.resize(theSize);
        inStream << inSectionsDelimPtr <<
            mTmpBuffer << "activeUpNodesCount"         << inSepPtr   <<
                theReporter.GetActiveUpNodesCount()    << inDelimPtr <<
            mTmpBuffer << "activeUpChannelsCount"      << inSepPtr   <<
                theReporter.GetActiveUpChannelsCount() << inDelimPtr
        ;
        mTmpBuffer = "configuration.";
        inStream << inSectionsDelimPtr <<
            mTmpBuffer << "primaryTimeout"           << inSepPtr   <<
                mConfig.GetPrimaryTimeout()          << inDelimPtr <<
            mTmpBuffer << "backupTimeout"            << inSepPtr   <<
                mConfig.GetBackupTimeout()           << inDelimPtr <<
            mTmpBuffer << "changeViewMaxLogDistance" << inSepPtr   <<
                mConfig.GetChangeVewMaxLogDistance() << inDelimPtr <<
            mTmpBuffer << "maxListenersPerNode"      << inSepPtr   <<
                mConfig.GetMaxListenersPerNode()     << inDelimPtr
        ;
        mTmpBuffer += "node.";
        const size_t theLen = mTmpBuffer.size();
        size_t       theCnt = 0;
        const Config::Nodes& theNodes = mConfig.GetNodes();
        for (Config::Nodes::const_iterator theIt = theNodes.begin();
                theNodes.end() != theIt;
                ++theIt) {
            mTmpBuffer.resize(theLen);
            AppendDecIntToString(mTmpBuffer, theCnt++);
            mTmpBuffer += ".";
            inStream <<
                mTmpBuffer << "id"       << inSepPtr << theIt->first <<
                    inDelimPtr <<
                mTmpBuffer << "flags"    << inSepPtr <<
                    theIt->second.GetFlags() << inDelimPtr <<
                mTmpBuffer << "active"   << inSepPtr <<
                    (0 != (theIt->second.GetFlags() & Config::kFlagActive)) <<
                    inDelimPtr <<
                mTmpBuffer << "primaryOrder" << inSepPtr <<
                    theIt->second.GetPrimaryOrder() << inDelimPtr
            ;
            const Config::Locations& theLocations =
                theIt->second.GetLocations();
            for (Config::Locations::const_iterator theIt = theLocations.begin();
                    theLocations.end() != theIt;
                    ++theIt) {
                inStream << mTmpBuffer << "listener" << inSepPtr <<
                    *theIt << inDelimPtr;
            }
        }
    }
    bool Handle(
        const MetaLogWriterControl& inReq)
    {
        if (MetaLogWriterControl::kLogFetchDone == inReq.type &&
                kStateLogSync == mState) {
            mCheckLogSyncStatusFlag = true;
            Wakeup();
        }
        return false;
    }
    bool Handle(
        const MetaReadMetaData& inReq)
    {
        // If primary, then use normal RPC handling path by returning false.
        return (kStatePrimary != mState && (
            inReq.allowNotPrimaryFlag ||
            (mActiveFlag && kStateBackup == mState)
        ));
    }
    void ParseLocations(
        MetaVrReconfiguration&   inReq,
        const char*              inErrMsgPrefixPtr,
        const Config::Locations& inLocations)
    {
        mPendingLocations.clear();
        if (mConfig.GetMaxListenersPerNode() <
                inLocations.size() + inReq.mListSize) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = inErrMsgPrefixPtr;
            inReq.statusMsg += " exceeded maximum number of listeners per node";
            return;
        }
        const char*       thePtr    = inReq.mListStr.GetPtr();
        const char* const theEndPtr = thePtr + inReq.mListStr.GetSize();
        ServerLocation    theLocation;
        while (thePtr < theEndPtr) {
            if (! theLocation.ParseString(
                        thePtr, theEndPtr - thePtr, inReq.shortRpcFormatFlag) ||
                    theLocation.port <= 0) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = inErrMsgPrefixPtr;
                inReq.statusMsg += " listener address parse error";
                mPendingLocations.clear();
                return;
            }
            if (! mConfigAlowHostNamesFlag && ! inReq.logseq.IsValid() &&
                    ! TcpSocket::IsValidConnectToAddress(theLocation)) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = inErrMsgPrefixPtr;
                inReq.statusMsg += " is not valid IP address: ";
                inReq.statusMsg += theLocation.ToString();
                mPendingLocations.clear();
                return;
            }
            // Duplicate listeners are allowed, in order to create parallel TCP
            // connections / log transmit channels.
            if (find(inLocations.begin(), inLocations.end(), theLocation) ==
                    inLocations.end() && HasLocation(theLocation)) {
                inReq.status = -EINVAL;
                inReq.statusMsg = inErrMsgPrefixPtr;
                inReq.statusMsg += " listener: ";
                inReq.statusMsg += theLocation.ToString();
                inReq.statusMsg += " is already assigned to node ";
                AppendDecIntToString(
                    inReq.statusMsg, FindNodeByLocation(theLocation));
                mPendingLocations.clear();
                return;
            }
            mPendingLocations.push_back(theLocation);
        }
        if (mPendingLocations.size() != (size_t)inReq.mListSize) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = inErrMsgPrefixPtr;
            inReq.statusMsg += " listeners list parse failure";
            mPendingLocations.clear();
            return;
        }
    }
    void AddNode(
        MetaVrReconfiguration& inReq)
    {
        if (inReq.mNodeId < 0) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "add node: invalid node id";
            return;
        }
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
        // Initial boot strap: do no allow other node to become primary
        // in the case if all the nodes are inactive.
        if (! inReq.logseq.IsValid() &&
                mConfig.IsEmpty() && mNodeId != inReq.mNodeId) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "add node: first node id to add must"
                " be equal to ";
            AppendDecIntToString(inReq.statusMsg, mNodeId);
            return;
        }
        ParseLocations(inReq, "add node:", Config::Locations());
    }
    void AddNodeListeners(
        MetaVrReconfiguration& inReq)
    {
        if (inReq.mListSize <= 0) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "add node listeners: no listeners specified";
            return;
        }
        const Config::Nodes&                theNodes = mConfig.GetNodes();
        Config::Nodes::const_iterator const theIt    =
            theNodes.find(inReq.mNodeId);
        if (theIt == theNodes.end()) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "add node listeners: no such node";
            return;
        }
        ParseLocations(inReq, "add node listeners:",
            theIt->second.GetLocations());
    }
    void RemoveNodeListeners(
        MetaVrReconfiguration& inReq)
    {
        if (inReq.mListSize <= 0) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "remove node listeners: no listeners specified";
            return;
        }
        const Config::Nodes&                theNodes = mConfig.GetNodes();
        Config::Nodes::const_iterator const theIt    =
            theNodes.find(inReq.mNodeId);
        if (theIt == theNodes.end()) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "add node listener: no such node";
            return;
        }
        mPendingLocations.clear();
        const Config::Locations theLocs   = theIt->second.GetLocations();
        const char*             thePtr    = inReq.mListStr.GetPtr();
        const char* const       theEndPtr = thePtr + inReq.mListStr.GetSize();
        ServerLocation          theLocation;
        while (thePtr < theEndPtr) {
            if (! theLocation.ParseString(
                    thePtr, theEndPtr - thePtr, inReq.shortRpcFormatFlag)) {
                inReq.status    = -EINVAL;
                inReq.statusMsg =
                    "remove node listeners: listener address parse error";
                mPendingLocations.clear();
                return;
            }
            Config::Locations::const_iterator const theIt =
                find(theLocs.begin(), theLocs.end(), theLocation);
            if (theIt == theLocs.end()) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "remove node listeners: no such litener: ";
                inReq.statusMsg += theLocation.ToString();
                mPendingLocations.clear();
                return;
            }
            mPendingLocations.push_back(theLocation);
        }
        if (mPendingLocations.size() != (size_t)inReq.mListSize) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "remove node listeners:"
                " listeners list parse failure";
            mPendingLocations.clear();
            return;
        }
        if (theLocs.size() <= mPendingLocations.size()) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "remove node listeners:"
                " removing all listeners is not supported";
            mPendingLocations.clear();
            return;
        }
        if (inReq.logseq.IsValid() || kStatePrimary != mState) {
            return;
        }
        TxStatusCheckNode theCheck(inReq.mNodeId, mPendingLocations,
            IsActive(inReq.mNodeId) ? mNodeId : NodeId(-1));
        mLogTransmitter.GetStatus(theCheck);
        if (theCheck.GetUpCount() <= 0) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "remove node listeners:"
                " no up channels left after removal";
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
        if (0 != inReq.status || inReq.logseq.IsValid()) {
            return;
        }
        if (mPendingChangesList.size() != mConfig.GetNodes().size() &&
                mQuorum <= 0) {
            // Initial boot strap: do no allow to remove primary
            // in the case if all the nodes are inactive.
            for (ChangesList::const_iterator theIt =
                    mPendingChangesList.begin();
                    mPendingChangesList.end() != theIt;
                    ++theIt) {
                if (theIt->first == mNodeId) {
                    inReq.status    = -EINVAL;
                    inReq.statusMsg =
                        "removing boot strap primary node is not supported,"
                        "unless all nodes removed";
                    mPendingChangesList.clear();
                    return;
                }
            }
        }
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
        if (0 < mQuorum && mQuorum <= inReq.mListSize) {
            inReq.status    = -EINVAL;
            inReq.statusMsg =
                "change active status: activity change list"
                " size must be less than present quorum: ";
            AppendDecIntToString(inReq.statusMsg, mQuorum);
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
        if (0 != inReq.status) {
            return;
        }
        const int theActiveCount = inActivateFlag ?
            mActiveCount + mPendingChangesList.size() :
            mActiveCount - mPendingChangesList.size();
        if (theActiveCount < kMinActiveCount) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "change active status: "
                "configuration must have at least 3 active nodes";
            return;
        }
        if (inReq.logseq.IsValid()) {
            return;
        }
        TxStatusCheck theCheck(
            inActivateFlag, mNodeId, mPendingChangesList, inReq);
        mLogTransmitter.GetStatus(theCheck);
        if (0 != inReq.status) {
            return;
        }
        if (inActivateFlag) {
            if (theCheck.GetUpCount() != mPendingChangesList.size()) {
                if (theCheck.GetUpCount() < mPendingChangesList.size()) {
                    for (ChangesList::const_iterator
                            theIt = mPendingChangesList.begin();
                            mPendingChangesList.end() != theIt;
                            ++theIt) {
                        if (theIt->second <= 0) {
                            if (0 == inReq.status) {
                                inReq.status    = -EINVAL;
                                inReq.statusMsg = "change active status: "
                                    "channels down:";
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
                    inReq.statusMsg = "change active status: internal error";
                }
                return;
            }
        } else {
            const int theNewQuorum = CalcQuorum(theActiveCount);
            if ((int)theCheck.GetUpCount() < theNewQuorum) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "change active status: remaining up nodes: ";
                AppendDecIntToString(inReq.statusMsg, theCheck.GetUpCount());
                inReq.statusMsg += " count is less than new quorum: ";
                AppendDecIntToString(inReq.statusMsg, theNewQuorum);
                return;
            }
        }
        if (kStatePrimary == mState) {
            SetState(kStateReconfiguration);
        }
    }
    void SetPrimaryOrder(
        MetaVrReconfiguration& inReq)
    {
        if (inReq.mListSize <= 0) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "set primary order: empty order list";
            return;
        }
        mPendingChangesList.clear();
        const char*       thePtr    = inReq.mListStr.GetPtr();
        const char* const theEndPtr = thePtr + inReq.mListStr.GetSize();
        while (thePtr < theEndPtr) {
            NodeId theNodeId = -1;
            int    theOrder  = 0;
            if (! inReq.ParseInt(thePtr, theEndPtr - thePtr, theNodeId) ||
                    theNodeId < 0 ||
                    ! inReq.ParseInt(thePtr, theEndPtr - thePtr, theOrder)) {
                inReq.status    = -EINVAL;
                inReq.statusMsg =
                    "set primary order: invalid node id order pair";
                mPendingChangesList.clear();
                return;
            }
            mPendingChangesList.push_back(make_pair(theNodeId, theOrder));
        }
        if (mPendingChangesList.size() != (size_t)inReq.mListSize) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "set primary order: id list size mismatch";
            mPendingChangesList.clear();
            return;
        }
        sort(mPendingChangesList.begin(), mPendingChangesList.end());
        for (ChangesList::const_iterator
                theIt = mPendingChangesList.begin(), theNIt = theIt;
                mPendingChangesList.end() != ++theNIt;
                theIt = theNIt) {
            if (theIt->first == theNIt->first) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "set primary order: duplicate node id ";
                AppendDecIntToString(inReq.statusMsg, theNIt->first);
                mPendingChangesList.clear();
                return;
            }
        }
        const size_t theActiveCnt = ApplyT(inReq, kActiveCheckNone, NopFunc());
        if (0 != inReq.status || inReq.logseq.IsValid()) {
            return;
        }
        if (kStatePrimary == mState && 0 < theActiveCnt) {
            SetState(kStateReconfiguration);
        }
    }
    static bool ValidateTimeouts(
        int inPrimaryTimeout,
        int inBackupTimeout)
    {
        return (
            0 < inPrimaryTimeout &&
            3 <= inBackupTimeout - inPrimaryTimeout
        );
    }
    void SetParameters(
        MetaVrReconfiguration& inReq)
    {
        if (4 != inReq.mListSize) {
            inReq.status    = -EINVAL;
            inReq.statusMsg =
                "set parameters: incorrect number of arguments, expected:"
                " primary_timeout"
                " backup_timeout"
                " start_view_change_max_log_distance"
                " max_listeners_per_node"
            ;
            return;
        }
        int               thePrimaryTimeout                 = -1;
        int               theBackupTimeout                  = -1;
        seq_t             thePendingChangeVewMaxLogDistance = -1;
        uint32_t          theMaxListenersPerNode            = 0;
        const char*       thePtr    = inReq.mListStr.GetPtr();
        const char* const theEndPtr = thePtr + inReq.mListStr.GetSize();
        if (! inReq.ParseInt(thePtr, theEndPtr - thePtr, thePrimaryTimeout) ||
                ! inReq.ParseInt(
                    thePtr, theEndPtr - thePtr, theBackupTimeout) ||
                ((0 <= thePrimaryTimeout || 0 <= theBackupTimeout) &&
                ! ValidateTimeouts(thePrimaryTimeout, theBackupTimeout)) ||
                ! inReq.ParseInt(thePtr, theEndPtr - thePtr,
                    thePendingChangeVewMaxLogDistance) ||
                ! inReq.ParseInt(thePtr, theEndPtr - thePtr,
                    theMaxListenersPerNode)) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "set parameters: timeout values; "
                "primary timeout must be greater than 0; backup "
                "timeout must be at least 3 seconds greater than primary "
                "timeout, max log distance must be an integer,"
                " max listeners must be an integer";
            return;
        }
        mPendingPrimaryTimeout          = thePrimaryTimeout;
        mPendingBackupTimeout           = theBackupTimeout;
        mPendingChangeVewMaxLogDistance = thePendingChangeVewMaxLogDistance;
        mPendingMaxListenersPerNode     = theMaxListenersPerNode;
    }
    void ResetConfig(
        MetaVrReconfiguration& inReq)
    {
        if (0 != inReq.mListSize) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "reset VR configuration:"
                " non 0 number of arguments";
            return;
        }
        if (! inReq.logseq.IsValid()) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "resetting VR configuration is not supported"
                " at run time. Meta server command line option"
                " -clear-vr-config can be used to clear VR configuration";
            return;
        }
        if (mConfig.IsEmpty()) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "resetting VR configuration:"
                " the configuration is empty";
            return;
        }
    }
    void InactivateAllNodes(
        MetaVrReconfiguration& inReq)
    {
        if (0 != inReq.mListSize) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "inactivate all:nodes:"
                " non 0 number of arguments";
            return;
        }
        if (! inReq.logseq.IsValid()) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "inactivation of all nodes is not supported"
                " at run time. Meta server command line option"
                " -vr-inactivate-all-nodes can be used to inactivate all nodes";
            return;
        }
        if (mConfig.IsEmpty()) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "inactivate all:nodes:"
                " the configuration is empty";
            return;
        }
        if (mActiveCount <= 0) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "inactivate all:nodes:"
                " no active nodes";
            return;
        }
    }
    void SwapActiveNode(
        MetaVrReconfiguration& inReq)
    {
        if (2 != inReq.mListSize) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "swap active node:"
                " arguments must have two node ids to swap";
            return;
        }
        if (! ParseNodeIdList(inReq)) {
            return;
        }
        if (1 != ApplyT(inReq, kActiveCheckNone, NopFunc()) ||
                0 != inReq.status) {
            if (0 == inReq.status) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "swap active node:"
                    " one node must be active while the other inactive";
            }
            return;
        }
        if (! IsActiveSelf(mPendingChangesList.front().first)) {
            swap(mPendingChangesList.front(), mPendingChangesList.back());
        }
        if (inReq.logseq.IsValid()) {
            return;
        }
        TxStatusCheckSwap theCheck(mNodeId, mPendingChangesList, inReq);
        mLogTransmitter.GetStatus(theCheck);
        if (0 != inReq.status) {
            return;
        }
        const size_t theRemActiveCnt = theCheck.mCheckInactivate.GetUpCount();
        if (theRemActiveCnt < (size_t)mQuorum) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "swap active node: remaining up nodes: ";
            AppendDecIntToString(inReq.statusMsg, theRemActiveCnt);
            inReq.statusMsg += " count is less than quorum: ";
            AppendDecIntToString(inReq.statusMsg, mQuorum);
            return;
        }
        if (theCheck.mCheckActivate.GetUpCount() < 1) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "swap active node: inactive node is down";
            return;
        }
        if (kStatePrimary == mState) {
            SetState(kStateReconfiguration);
        }
    }
    template<typename T>
    size_t ApplyT(
        int&        outStatus,
        string&     outErr,
        ActiveCheck inActiveCheck,
        const T&    inFunc)
    {
        size_t theActiveCount = 0;
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
                return 0;
            }
            Config::Node& theNode       = theNodeIt->second;
            const bool    theActiveFlag =
                (0 != (theNode.GetFlags() & Config::kFlagActive));
            if (theActiveFlag) {
                theActiveCount++;
            }
            if (kActiveCheckNone != inActiveCheck &&
                    theActiveFlag != (kActiveCheckActive == inActiveCheck)) {
                outStatus = -EINVAL;
                outErr    = kActiveCheckActive == inActiveCheck ?
                    "node not active: " : "node active: ";
                AppendDecIntToString(outErr, theNodeId);
                return 0;
            }
            if (! inFunc(outStatus, outErr, *theIt, theNodes, theNode)) {
                return 0;
            }
        }
        return theActiveCount;
    }
    template<typename RT, typename T>
    size_t ApplyT(
        RT&         inReq,
        ActiveCheck inActiveCheck,
        const T&    inFunc)
    {
        return ApplyT(inReq.status, inReq.statusMsg, inActiveCheck, inFunc);
    }
    template<typename T>
    size_t ApplyT(
        ActiveCheck inActiveCheck,
        const T&    inFunc)
    {
        string       statusMsg;
        int          status = 0;
        const size_t theCnt = ApplyT(status, statusMsg, inActiveCheck, inFunc);
        if (0 != status) {
            panic("VR: " + statusMsg);
        }
        return theCnt;
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
        MetaVrReconfiguration& inReq)
    {
        if (0 != inReq.status ||
                kVrReconfCountNotHandled < inReq.mHandledCount) {
            return;
        }
        inReq.mHandledCount = kVrReconfCountNoCommitAdvance;
        if (! inReq.logseq.IsValid() || inReq.logseq.mEpochSeq != mEpochSeq) {
            panic("VR: invalid commit reconfiguration log sequence");
            return;
        }
        if (kStateReconfiguration == mState &&
                (inReq.logseq != mLastNonEmptyViewEndSeq || inReq.replayFlag)) {
            panic("VR: commit reconfiguration: invalid state");
            return;
        }
        if (IsActive(mNodeId) != mActiveFlag) {
            panic("VR: commit reconfiguration: invalid node activity change");
            return;
        }
        VrReconfigurationTable::const_iterator const theIt =
            sVrReconfigurationTable.find(NameToken(inReq.mOpType.c_str()));
        if (sVrReconfigurationTable.end() == theIt) {
            panic("VR: invalid reconfiguration commit attempt");
            inReq.status    = -EFAULT;
            inReq.statusMsg = "internal error";
            return;
        }
        (this->*(theIt->second.second))(inReq);
        if (0 != inReq.status) {
            panic("VR: reconfiguration commit failure");
            return;
        }
        mPendingChangesList.clear();
        mPendingLocations.clear();
        ConfigUpdate();
        if (kStatePrimary == mState && ! inReq.replayFlag &&
                mStartedFlag && mActiveFlag) {
            // Drop count to 1 to schedule to emit no-op to advance commit on
            // backups.
            inReq.mHandledCount = kVrReconfCountCommitAdvance;
        }
    }
    void CommitAddNode(
        MetaVrReconfiguration& inReq)
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
    void CommitAddNodeListeners(
        MetaVrReconfiguration& inReq)
    {
        if (mPendingLocations.size() != (size_t)inReq.mListSize) {
            panic("VR: commit add node listeners: invalid locations");
            return;
        }
        Config::Nodes&                theNodes = mConfig.GetNodes();
        Config::Nodes::iterator const theIt    = theNodes.find(inReq.mNodeId);
        if (theIt == theNodes.end()) {
            panic("VR: add node listener: no such node");
            return;
        }
        Config::Node& theNode = theIt->second;
        for (Locations::const_iterator theIt = mPendingLocations.begin();
                mPendingLocations.end() != theIt;
                ++theIt) {
            AddLocation(*theIt);
            theNode.AddLocation(*theIt);
        }
    }
    void CommitRemoveNodeListeners(
        MetaVrReconfiguration& inReq)
    {
        if (mPendingLocations.size() != (size_t)inReq.mListSize) {
            panic("VR: commit remove node listeners: invalid locations");
            return;
        }
        Config::Nodes&                theNodes = mConfig.GetNodes();
        Config::Nodes::iterator const theIt    = theNodes.find(inReq.mNodeId);
        if (theIt == theNodes.end()) {
            panic("VR: add node listener: no such node");
            return;
        }
        Config::Node& theNode = theIt->second;
        for (Locations::const_iterator theIt = mPendingLocations.begin();
                mPendingLocations.end() != theIt;
                ++theIt) {
            const ServerLocation& theLocation = *theIt;
            theNode.RemoveLocation(theLocation);
            const Config::Locations& theLocations = theNode.GetLocations();
            if (find(theLocations.begin(), theLocations.end(), theLocation) ==
                    theLocations.end()) {
                if (! RemoveLocation(*theIt)) {
                    panic("VR: commit remove listeners:"
                        " invalid unique locations");
                }
            }
        }
    }
    void CommitResetConfig(
        MetaVrReconfiguration& inReq)
    {
        if (! inReq.replayFlag || mStartedFlag ||
                ! inReq.logseq.IsValid() ||
                inReq.logseq.mEpochSeq != mEpochSeq) {
            panic("VR: invalid configuration reset commit");
            return;
        }
        mConfig.Clear();
        mAllUniqueLocations.clear();
        mActiveCount = 0;
        mQuorum      = 0;
        mActiveFlag  = false;
        CommitReconfiguration(inReq);
    }
    void CommitInactivateAllNodes(
        MetaVrReconfiguration& inReq)
    {
        if (! inReq.replayFlag || mStartedFlag ||
                ! inReq.logseq.IsValid() ||
                inReq.logseq.mEpochSeq != mEpochSeq) {
            panic("VR: invalid inactivate all nodes");
            return;
        }
        Config::Nodes& theNodes = mConfig.GetNodes();
        for (Config::Nodes::iterator theIt = theNodes.begin();
                theNodes.end() != theIt;
                ++theIt) {
            theIt->second.SetFlags(theIt->second.GetFlags() &
                    ~Config::Flags(Config::kFlagActive));
        }
        mActiveCount = 0;
        mQuorum      = 0;
        mActiveFlag  = false;
        CommitReconfiguration(inReq);
    }
    void CommitSwapActiveNode(
        MetaVrReconfiguration& inReq)
    {
        if (2 != mPendingChangesList.size()) {
            panic("VR: swap active node invalid pending list size");
            return;
        }
        const NodeId theInactiveId = mPendingChangesList.back().first;
        mPendingChangesList.pop_back();
        ApplyT(kActiveCheckActive, ChangeActiveFunc(false));
        mPendingChangesList.front() = make_pair(theInactiveId, -1);
        ApplyT(kActiveCheckNotActive, ChangeActiveFunc(true));
        CommitReconfiguration(inReq);
    }
    void CommitRemoveNodes(
        MetaVrReconfiguration& inReq)
    {
        ApplyT(kActiveCheckNotActive, RmoveFunc(*this));
    }
    void CommitModifyActiveStatus(
        MetaVrReconfiguration& inReq,
        bool                   inActivateFlag)
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
            mQuorum = CalcQuorum(mActiveCount);
        }
        CommitReconfiguration(inReq);
    }
    void CommitSetPrimaryOrder(
        MetaVrReconfiguration& inReq)
    {
        if (0 < ApplyT(kActiveCheckNone, ChangePrimaryOrderFunc())) {
            CommitReconfiguration(inReq);
        }
    }
    void CommitReconfiguration(
        MetaVrReconfiguration& inReq)
    {
        if ((kStateReconfiguration == mState &&
                (inReq.logseq != mLastNonEmptyViewEndSeq ||
                inReq.replayFlag)) ||
                inReq.logseq.mEpochSeq != mEpochSeq) {
            panic("VR: commit reconfiguration: invalid state");
            return;
        }
        mActiveFlag = IsActive(mNodeId);
        mEpochSeq++;
        mViewSeq = kMetaVrLogStartEpochViewSeq;
        if (mActiveFlag && mStartedFlag &&
                (! inReq.replayFlag || kStateBackup == mState)) {
            WriteVrState(-1, inReq.logseq);
        }
        KFS_LOG_STREAM_DEBUG <<
            "done: "   << inReq.logseq <<
            " epoch: " << mEpochSeq <<
            " view: "  << mViewSeq <<
            " state: " << GetStateName(mState) <<
            " "        << inReq.Show() <<
        KFS_LOG_EOM;
        if (mStartedFlag) {
            if (mActiveFlag) {
                if (kStateReconfiguration == mState ||
                        mLastLogSeq == inReq.logseq) {
                    ScheduleViewChange("reconfiguration ", inReq.mOpType);
                }
            } else {
                if (mActiveCount <= 0) {
                    mPrimaryNodeId = AllInactiveFindPrimary();
                    if (0 <= mNodeId && mPrimaryNodeId == mNodeId) {
                        SetState(kStatePrimary);
                    } else {
                        SetState(kStateBackup);
                    }
                } else {
                    SetState(kStateBackup);
                    mPrimaryNodeId = -1;
                }
            }
        }
    }
    void CommitSetParameters(
        MetaVrReconfiguration& inReq)
    {
        if (0 != inReq.status) {
            return;
        }
        if ((0 <= mPendingPrimaryTimeout || 0 <= mPendingBackupTimeout) &&
                ! ValidateTimeouts(
                    mPendingPrimaryTimeout, mPendingBackupTimeout)) {
            panic("VR: commit set parameters: invalid timeouts");
        }
        if (0 <= mPendingPrimaryTimeout || 0 <= mPendingBackupTimeout) {
            mConfig.SetPrimaryTimeout(mPendingPrimaryTimeout);
            mConfig.SetBackupTimeout(mPendingBackupTimeout);
            mLogTransmitter.SetHeartbeatInterval(mConfig.GetPrimaryTimeout());
        }
        if (0 <= mPendingChangeVewMaxLogDistance) {
            mConfig.SetChangeVewMaxLogDistance(mPendingChangeVewMaxLogDistance);
        }
        if (0 < mPendingMaxListenersPerNode) {
            mConfig.SetMaxListenersPerNode(mPendingMaxListenersPerNode);
        }
    }
    bool HasLocation(
        const ServerLocation& inLocation) const
    {
        Locations::const_iterator const theIt = lower_bound(
            mAllUniqueLocations.begin(), mAllUniqueLocations.end(), inLocation);
        return (theIt != mAllUniqueLocations.end() && inLocation == *theIt);
    }
    bool AddLocation(
        const ServerLocation& inLocation)
    {
        Locations::iterator const theIt = lower_bound(
            mAllUniqueLocations.begin(), mAllUniqueLocations.end(), inLocation);
        if (mAllUniqueLocations.end() == theIt || inLocation != *theIt) {
            mAllUniqueLocations.insert(theIt, inLocation);
            return true;
        }
        return false;
    }
    bool RemoveLocation(
        const ServerLocation& inLocation)
    {
        Locations::iterator const theIt = lower_bound(
            mAllUniqueLocations.begin(), mAllUniqueLocations.end(), inLocation);
        if (mAllUniqueLocations.end() == theIt || inLocation != *theIt) {
            return false;
        }
        mAllUniqueLocations.erase(theIt);
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
    NodeId GetPrimaryId() const
    {
        if (mQuorum <= 0) {
            return -1;
        }
        NodeId               theId       = -1;
        int                  theMinOrder = numeric_limits<int>::max();
        const Config::Nodes& theNodes    = mConfig.GetNodes();
        int                  theCnt      = 0;
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
            if (0 <= mQuorum && mStartViewChangeNodeIds.find(theNodeId) ==
                    mStartViewChangeNodeIds.end()) {
                continue;
            }
            theCnt++;
            const int theOrder = theNode.GetPrimaryOrder();
            if (theMinOrder <= theOrder && 0 <= theId) {
                continue;
            }
            theMinOrder = theOrder;
            theId       = theNodeId;
        }
        return (theCnt < mQuorum ? -1 : theId);
    }
    NodeId AllInactiveFindPrimary() const
    {
        if (0 < mQuorum || 0 < mActiveCount) {
            return -1;
        }
        return (mConfig.IsEmpty() ?
            kBootstrapPrimaryNodeId :
            mConfig.GetNodes().begin()->first
        );
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
        mRespondedIds.clear();
        Cancel(mDoViewChangePtr);
        Cancel(mStartViewPtr);
        MetaRequest::Release(mMetaVrLogStartViewPtr);
        mMetaVrLogStartViewPtr = 0;
        mStartViewChangeRecvViewSeq     = -1;
        mStartViewChangeMaxLastLogSeq   = MetaVrLogSeq();
        mStartViewChangeMaxCommittedSeq = MetaVrLogSeq();
        mStartViewEpochMismatchCount    = 0;
        mReplyCount                     = 0;
    }
    void Init(
        MetaVrRequest& inReq)
    {
        inReq.mCurState             = mState;
        inReq.mNodeId               = mNodeId;
        inReq.mEpochSeq             = mEpochSeq;
        inReq.mViewSeq              = mViewSeq;
        inReq.mCommittedSeq         = mCommittedSeq;
        inReq.mCommittedErrChecksum = mCommittedErrChecksum;
        inReq.mCommittedStatus      = mCommittedStatus;
        inReq.mCommittedFidSeed     = mCommittedFidSeed;
        inReq.mLastLogSeq           = mLastLogSeq;
        inReq.mLastViewEndSeq       = mLastViewEndSeq;
        inReq.mFileSystemId         = mFileSystemId;
        inReq.mMetaDataStoreHost    = mMetaDataStoreLocation.hostname;
        inReq.mMetaDataStorePort    = mMetaDataStoreLocation.port;
        inReq.mClusterKey           = mClusterKey;
        inReq.mMetaMd               = mMetaMd;
        inReq.mReason               = mViewChangeReason;
        inReq.SetVrSMPtr(&mMetaVrSM);
    }
    void UpdateViewAndStartViewChange(
        const char* inReasonPtr   = 0,
        const char* inExReasonPtr = 0)
    {
        if (mActiveFlag &&
                mEpochSeq == mLastLogSeq.mEpochSeq &&
                mViewSeq <= mLastLogSeq.mViewSeq) {
            mViewSeq = mLastLogSeq.mViewSeq + 1;
        }
        StartViewChange(inReasonPtr, inExReasonPtr);
    }
    void StartViewChange(
        const MetaVrRequest& inReq)
    {
        if (kStatePrimary == mState || kStateBackup == mState ||
                kStateReconfiguration == mState) {
            if (inReq.mReason.empty()) {
                const char* theNamePtr = "";
                switch (inReq.op) {
                    case META_VR_START_VIEW_CHANGE:
                        theNamePtr = "start view change";
                        break;
                    case META_VR_DO_VIEW_CHANGE:
                        theNamePtr = "do view change";
                        break;
                    case META_VR_START_VIEW:
                        theNamePtr = "start view";
                        break;
                    default:
                        theNamePtr = "request";
                        break;
                }
                mViewChangeReason = theNamePtr;
                mViewChangeReason += ", node: ";
                AppendDecIntToString(mViewChangeReason, inReq.mNodeId);
            } else {
                mViewChangeReason = inReq.mReason;
            }
            mViewChangeInitiationTime = TimeNow();
        }
        StartViewChange();
    }
    void StartViewChange(
        const char* inReasonPtr   = 0,
        const char* inExReasonPtr = 0)
    {
        if (! mActiveFlag) {
            panic("VR: start view change: node non active");
            return;
        }
        if (inReasonPtr && *inReasonPtr &&
                (kStatePrimary == mState || kStateBackup == mState ||
                    kStateReconfiguration == mState)) {
            mViewChangeReason = inReasonPtr;
            if (inExReasonPtr) {
                mViewChangeReason += inExReasonPtr;
            }
            mViewChangeReason += ", node: ";
            AppendDecIntToString(mViewChangeReason, mNodeId);
            mViewChangeInitiationTime = TimeNow();
        }
        mLogStartViewPendingRecvFlag = false;
        mPrimaryNodeId               = -1;
        CancelViewChange();
        if (mQuorum <= 0) {
            return;
        }
        if ((mEpochSeq != mLastLogSeq.mEpochSeq &&
                mEpochSeq != mLastLogSeq.mEpochSeq + 1) ||
                (mEpochSeq == mLastLogSeq.mEpochSeq &&
                mViewSeq <= mLastLogSeq.mViewSeq)) {
            panic("VR: start view change: invalid epoch or view");
        }
        const State thePrevState = mState;
        SetState(kStateViewChange);
        mViewChangeStartTime = TimeNow();
        mStartViewChangeNodeIds.clear();
        mDoViewChangeNodeIds.clear();
        mNodeIdAndMDSLocations.clear();
        mDoViewChangeViewEndSeq = GetLastViewEndSeq();
        if (mDoViewChangeViewEndSeq < mCommittedSeq) {
            panic("VR: invalid do view change sequence");
        }
        MetaVrStartViewChange& theOp = *(new MetaVrStartViewChange());
        Init(theOp);
        mStartViewChangePtr = &theOp;
        const NodeId kBroadcast = -1;
        KFS_LOG_STREAM_INFO <<
            "start view change: " << mViewChangeReason <<
            " prior state: "      << GetStateName(thePrevState) <<
            " initiated: "        <<
                (TimeNow() - mViewChangeInitiationTime) << " sec. ago"
            " "                   << theOp.Show() <<
        KFS_LOG_EOM;
        QueueVrRequest(theOp, kBroadcast);
    }
    void WriteVrState(
        NodeId              inPrimaryId,
        const MetaVrLogSeq& inViewEndSeq)
    {
        KFS_LOG_STREAM_INFO <<
            "write vr state:"
            " primary: "        << inPrimaryId <<
            " epoch: "          << mEpochSeq <<
            " view: "           << mViewSeq <<
            " committed: "      << mCommittedSeq <<
            " view end: "       << inViewEndSeq <<
            " last log:"        << mLastLogSeq <<
            " prior view end: " << mLastViewEndSeq <<
            " lneve: "          << mLastNonEmptyViewEndSeq  <<
        KFS_LOG_EOM;
        if (mLastLogSeq < inViewEndSeq ||
                (mLastLogSeq.IsPastViewStart() &&
                    ! mLastLogSeq.IsSameView(inViewEndSeq)) ||
                ! inViewEndSeq.IsPastViewStart()) {
            panic("VR: invalid view end sequence");
            mLastViewEndSeq = MetaVrLogSeq();
        }
        if (0 != WriteVrStateSelf(inPrimaryId, inViewEndSeq)) {
            if (mPanicOnIoErrorFlag) {
                panic("VR: write state IO error");
            }
            if (! mVrStateFileName.empty() ||
                    (0 != unlink(mVrStateFileName.c_str()) &&
                    ENOENT != errno)) {
                const int theErr = errno;
                KFS_LOG_STREAM_FATAL <<
                    mVrStateFileName << ": " << QCUtils::SysError(theErr) <<
                KFS_LOG_EOM;
                MsgLogger::Stop();
                abort();
            }
        }
        mLastViewEndSeq = inViewEndSeq;
    }
    void StartDoViewChange(
        NodeId inPrimaryId)
    {
        if (! mActiveFlag || inPrimaryId < 0 ||
                mReplayLastLogSeq != mLastLogSeq) {
            panic("VR: do view change: invalid invocation");
            return;
        }
        CancelViewChange();
        mViewChangeStartTime = TimeNow();
        SetState(kStateViewChange);
        WriteVrState(inPrimaryId, mDoViewChangeViewEndSeq);
        MetaVrDoViewChange& theOp = *(new MetaVrDoViewChange());
        Init(theOp);
        theOp.mPrimaryNodeId = inPrimaryId;
        mDoViewChangePtr = &theOp;
        QueueVrRequest(theOp, theOp.mPrimaryNodeId);
    }
    void StartView()
    {
        if (! mActiveFlag) {
            panic("VR: start view: node non active");
            return;
        }
        CancelViewChange();
        SetState(kStateStartViewPrimary);
        mViewChangeStartTime = TimeNow();
        MetaVrStartView& theOp = *(new MetaVrStartView());
        Init(theOp);
        mStartViewPtr = &theOp;
        const NodeId kBroadcast = -1;
        QueueVrRequest(theOp, kBroadcast);
    }
    void QueueVrRequest(
        MetaVrRequest& inOp,
        NodeId         inNodeId)
    {
        if (mLogTransmittersSuspendedFlag) {
            mLogTransmittersSuspendedFlag = false;
            mLogTransmitter.Suspend(mLogTransmittersSuspendedFlag);
        }
        mLogTransmitter.QueueVrRequest(inOp, inNodeId);
    }
    static string& AppendHexLogSeqToString(
        string&             inStr,
        const MetaVrLogSeq& inLogSeq)
    {
        AppendHexIntToString(inStr, inLogSeq.mEpochSeq);
        inStr += " ";
        AppendHexIntToString(inStr, inLogSeq.mViewSeq);
        inStr += " ";
        AppendHexIntToString(inStr, inLogSeq.mLogSeq);
        return inStr;
    }
    static int GetErrno()
    {
        const int theRet = errno;
        return (0 == theRet ? EIO : theRet);
    }
    int WriteVrStateSelf(
        NodeId              inPrimaryId,
        const MetaVrLogSeq& inViewEndSeq)
    {
        if (mVrStateFileName.empty()) {
            KFS_LOG_STREAM_ERROR <<
                "invalid empty vr state file name" <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        if (mLastLogSeq < mCommittedSeq) {
            panic("VR: committed sequence greater than last log sequence");
            return -EINVAL;
        }
        int theError = 0;
        if (0 != unlink(mVrStateTmpFileName.c_str())) {
            theError = GetErrno();
            if (ENOENT == theError) {
                theError = 0;
            } else {
                KFS_LOG_STREAM_ERROR <<
                    mVrStateTmpFileName << ": " <<
                        QCUtils::SysError(theError) <<
                KFS_LOG_EOM;
                return (theError < 0 ? theError : -theError);
            }
        }
        bool      theRenameErrorFlag = false;
        const int theFd = open(mVrStateTmpFileName.c_str(),
            O_WRONLY | O_CREAT | O_TRUNC, 0666);
        if (theFd < 0) {
            theError = errno;
            if (0 == theError) {
                theError = EIO;
            }
        } else {
            mVrStateIoStr.clear();
            AppendHexIntToString(mVrStateIoStr, mFileSystemId);
            mVrStateIoStr += " ";
            AppendHexIntToString(mVrStateIoStr, mEpochSeq);
            mVrStateIoStr += " ";
            AppendHexIntToString(mVrStateIoStr, mViewSeq);
            mVrStateIoStr += " ";
            AppendHexIntToString(mVrStateIoStr, mNodeId);
            mVrStateIoStr += " ";
            AppendHexIntToString(mVrStateIoStr, inPrimaryId);
            mVrStateIoStr += " ";
            AppendHexLogSeqToString(mVrStateIoStr, mLastLogSeq);
            mVrStateIoStr += " ";
            AppendHexLogSeqToString(mVrStateIoStr, mCommittedSeq);
            mVrStateIoStr += " ";
            AppendHexLogSeqToString(mVrStateIoStr, inViewEndSeq);
            mVrStateIoStr += " ";
            const int64_t theTimeNow = TimeNow();
            AppendHexIntToString(mVrStateIoStr, theTimeNow);
            mVrStateIoStr += " ";
            const uint32_t theCrc32 = ComputeCrc32(
                mVrStateIoStr.data(), mVrStateIoStr.size());
            AppendHexIntToString(mVrStateIoStr, theCrc32);
            mVrStateIoStr += "\n";
            const size_t theSize = mVrStateIoStr.size();
            if (sizeof(mVrStateReadBuffer) <= theSize) {
                close(theFd);
                panic("vr state exceeded max length");
                return -EFAULT;
            }
            const char*       thePtr    = mVrStateIoStr.data();
            const char* const theEndPtr = thePtr + theSize;
            while (thePtr < theEndPtr) {
                const ssize_t theNWr = write(theFd, thePtr, theEndPtr - thePtr);
                if (theNWr < 0) {
                    theError = GetErrno();
                    break;
                }
                thePtr += theNWr;
            }
            bool theCloseErrorFlag = false;
            if (0 != theError ||
                    (mSyncVrStateFileFlag && 0 != fsync(theFd)) ||
                    (theCloseErrorFlag = 0 != close(theFd)) ||
                    (theRenameErrorFlag = 0 != rename(
                        mVrStateTmpFileName.c_str(),
                        mVrStateFileName.c_str()))) {
                if (0 == theError) {
                    theError = GetErrno();
                }
                if (! theRenameErrorFlag && ! theCloseErrorFlag) {
                    close(theFd);
                }
                unlink(mVrStateTmpFileName.c_str());
            }
        }
        if (0 != theError) {
            KFS_LOG_STREAM_ERROR <<
                (theRenameErrorFlag ? "rename " : "") <<
                mVrStateTmpFileName <<
                (theRenameErrorFlag ? " to " : "") <<
                (theRenameErrorFlag ? mVrStateFileName.c_str() : "") <<
                " : " << QCUtils::SysError(theError) <<
            KFS_LOG_EOM;
        }
        return (0 < theError ? -theError : theError);
    }
    int ReadVrState(
        NodeId& outNodeId)
    {
        outNodeId = -1;
        if (mVrStateFileName.empty()) {
            KFS_LOG_STREAM_ERROR <<
                "invalid empty vr state file name" <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        int       theRet = 0;
        const int theFd  = open(mVrStateFileName.c_str(), O_RDONLY);
        if (0 <= theFd) {
            const ssize_t theNRd = read(
                theFd, mVrStateReadBuffer, sizeof(mVrStateReadBuffer));
            if (theNRd < 0) {
                theRet = GetErrno();
                KFS_LOG_STREAM_ERROR <<
                    mVrStateFileName << ": " << QCUtils::SysError(theRet) <<
                KFS_LOG_EOM;
            } else {
                MetaVrLogSeq      theLastLogSeq;
                MetaVrLogSeq      theCommittedSeq;
                MetaVrLogSeq      theViewEndSeq;
                int64_t           theFsId      = -1;
                seq_t             theEpochSeq  = -1;
                seq_t             theViewSeq   = -1;
                NodeId            thePrimaryId = -1;
                NodeId            theNodeId    = -1;
                int64_t           theTime      = -1;
                const char*       thePtr       = mVrStateReadBuffer;
                const char* const theEndPtr    = thePtr + theNRd;
                if (theNRd == 0 ||
                        sizeof(mVrStateReadBuffer) <= (size_t)theNRd ||
                        (mVrStateReadBuffer[theNRd - 1] & 0xFF) != '\n' ||
                        ! HexIntParser::Parse(
                            thePtr, theEndPtr - thePtr, theFsId) ||
                        theFsId < 0 ||
                        ! HexIntParser::Parse(
                            thePtr, theEndPtr - thePtr, theEpochSeq) ||
                        theEpochSeq < 0 ||
                        ! HexIntParser::Parse(
                            thePtr, theEndPtr - thePtr, theViewSeq) ||
                        theViewSeq < 0 ||
                        ! HexIntParser::Parse(
                            thePtr, theEndPtr - thePtr, theNodeId) ||
                        theNodeId < 0 ||
                        ! HexIntParser::Parse(
                            thePtr, theEndPtr - thePtr, thePrimaryId) ||
                        ! theLastLogSeq.Parse<HexIntParser>(
                            thePtr, theEndPtr - thePtr) ||
                        ! theLastLogSeq.IsValid() ||
                        ! theCommittedSeq.Parse<HexIntParser>(
                            thePtr, theEndPtr - thePtr) ||
                        ! theCommittedSeq.IsValid() ||
                        ! theViewEndSeq.Parse<HexIntParser>(
                            thePtr, theEndPtr - thePtr) ||
                        ! theViewEndSeq.IsValid() ||
                        ! HexIntParser::Parse(
                            thePtr, theEndPtr - thePtr, theTime) ||
                        theEndPtr <= thePtr ||
                        (*thePtr++ & 0xFF) != ' ' ||
                        theEndPtr <= thePtr ||
                        (*thePtr & 0xFF) <= ' ') {
                    theRet = EINVAL;
                } else {
                    const uint32_t theActCrc32 = ComputeCrc32(
                        mVrStateReadBuffer, thePtr - mVrStateReadBuffer);
                    uint32_t       theCrc32    = 0;
                    if (! HexIntParser::Parse(
                            thePtr, theEndPtr - thePtr, theCrc32)) {
                        theRet = EINVAL;
                    } else if (theActCrc32 != theCrc32) {
                        theRet = EBADCKSUM;
                        KFS_LOG_STREAM_ERROR <<
                            mVrStateFileName << ": checksum mismatch:"
                            " expected:" << theCrc32 <<
                            " actual: "  << theActCrc32 <<
                        KFS_LOG_EOM;
                    } else if (theLastLogSeq < theCommittedSeq) {
                        theRet = EINVAL;
                        KFS_LOG_STREAM_ERROR <<
                            mVrStateFileName << ":"
                            " committed:" << theCommittedSeq <<
                            " exceeds"
                            " last log: " << theLastLogSeq <<
                        KFS_LOG_EOM;
                    } else if (! theViewEndSeq.IsPastViewStart() ||
                            theLastLogSeq < theViewEndSeq ||
                            theViewEndSeq < theCommittedSeq) {
                        theRet = EINVAL;
                        KFS_LOG_STREAM_ERROR <<
                            mVrStateFileName << ":"
                            " invalid last view end:" << theViewEndSeq <<
                            " committed: "            << theCommittedSeq <<
                            " last log:"              << theLastLogSeq <<
                        KFS_LOG_EOM;
                    } else if (! mStartedFlag) {
                        outNodeId = theNodeId;
                    } else if (theFsId != mFileSystemId ||
                                mLastLogSeq < theLastLogSeq ||
                                mLastLogSeq < theViewEndSeq ||
                                (theEpochSeq != mEpochSeq &&
                                    theEpochSeq + 1 != mEpochSeq &&
                                    theViewSeq !=
                                        kMetaVrLogStartEpochViewSeq) ||
                                (theViewSeq < mViewSeq &&
                                    theEpochSeq == mEpochSeq)) {
                        KFS_LOG_STREAM_ERROR <<
                            mVrStateFileName <<
                            ": invalid / stale state:"
                            " fs id: "    << theFsId <<
                            " / "         << mFileSystemId <<
                            " node: "     << theNodeId <<
                            " / "         << mNodeId <<
                            " last log: " << mLastLogSeq <<
                            " / "         << theLastLogSeq <<
                            " commited: " << theCommittedSeq <<
                            " / "         << mCommittedSeq <<
                            " view end: " << theViewEndSeq <<
                            " / "         << mLastViewEndSeq <<
                            " epoch: "    << theEpochSeq <<
                            " / "         << mEpochSeq <<
                            " view: "     << theViewSeq <<
                            " / "         << mViewSeq <<
                            " primary: "  << thePrimaryId <<
                        KFS_LOG_EOM;
                        theRet = ENXIO;
                    } else {
                        if (mEpochSeq == theEpochSeq) {
                            mViewSeq = theViewSeq;
                        }
                        mLastViewEndSeq = theViewEndSeq;
                        outNodeId       = theNodeId;
                    }
                }
            }
            close(theFd);
            if (EINVAL == theRet) {
                KFS_LOG_STREAM_ERROR <<
                    mVrStateFileName << ": invalid VR state file format" <<
                KFS_LOG_EOM;
            }
        } else {
            theRet = GetErrno();
            if (ENOENT != theRet) {
                KFS_LOG_STREAM_ERROR <<
                    mVrStateFileName << ": " << QCUtils::SysError(theRet) <<
                KFS_LOG_EOM;
            }
        }
        if (0 != theRet && mStartedFlag) {
            mLastViewEndSeq = MetaVrLogSeq();
        }
        return (0 < theRet ? -theRet : theRet);
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};
const MetaVrSM::Impl::VrReconfigurationTable&
    MetaVrSM::Impl::sVrReconfigurationTable =
    MetaVrSM::Impl::MakVrReconfigurationTable();

MetaVrSM::MetaVrSM(
    LogTransmitter& inLogTransmitter)
    : mStatus(0),
      mImpl(*(new Impl(inLogTransmitter, *this, mStatus)))
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
    const MetaVrLogSeq& inCommittedSeq,
    MetaVrSM::NodeId    inTransmitterId)
{
    return mImpl.HandleLogBlock(
        inBlockStartSeq, inBlockEndSeq, inCommittedSeq, inTransmitterId);
}

    MetaVrSM::NodeId
MetaVrSM::LogBlockWriteDone(
    const MetaVrLogSeq& inBlockStartSeq,
    const MetaVrLogSeq& inBlockEndSeq,
    const MetaVrLogSeq& inCommittedSeq,
    const MetaVrLogSeq& inLastViewEndSeq,
    bool                inWriteOkFlag)
{
    return mImpl.LogBlockWriteDone(
        inBlockStartSeq,
        inBlockEndSeq,
        inCommittedSeq,
        inLastViewEndSeq,
        inWriteOkFlag
    );
}

    bool
MetaVrSM::HandleLogBlockFailed(
    const MetaVrLogSeq& inBlockEndSeq,
    MetaVrSM::NodeId    inTransmitterId)
{
    return mImpl.HandleLogBlockFailed(inBlockEndSeq, inTransmitterId);
}

    bool
MetaVrSM::Handle(
    MetaRequest&        inReq,
    const MetaVrLogSeq& inNextLogSeq)
{
    return mImpl.Handle(inReq, inNextLogSeq);
}

    bool
MetaVrSM::Init(
    MetaVrHello&          inReq,
    const ServerLocation& inPeer,
    LogTransmitter&       inLogTransmitter)
{
    return mImpl.Init(inReq, inPeer, inLogTransmitter);
}

    void
MetaVrSM::HandleReply(
    MetaVrHello&          inReq,
    seq_t                 inSeq,
    const Properties&     inProps,
    MetaVrSM::NodeId      inNodeId,
    const ServerLocation& inPeer)
{
    mImpl.HandleReply(inReq, inSeq, inProps, inNodeId, inPeer);
}

    void
MetaVrSM::HandleReply(
    MetaVrStartViewChange& inReq,
    seq_t                  inSeq,
    const Properties&      inProps,
    MetaVrSM::NodeId       inNodeId,
    const ServerLocation&  inPeer)
{
    mImpl.HandleReply(inReq, inSeq, inProps, inNodeId, inPeer);
}

    void
MetaVrSM::HandleReply(
    MetaVrDoViewChange&   inReq,
    seq_t                 inSeq,
    const Properties&     inProps,
    MetaVrSM::NodeId      inNodeId,
    const ServerLocation& inPeer)
{
    mImpl.HandleReply(inReq, inSeq, inProps, inNodeId, inPeer);
}

    void
MetaVrSM::HandleReply(
    MetaVrStartView&      inReq,
    seq_t                 inSeq,
    const Properties&     inProps,
    MetaVrSM::NodeId      inNodeId,
    const ServerLocation& inPeer)
{
    mImpl.HandleReply(inReq, inSeq, inProps, inNodeId, inPeer);
}

    void
MetaVrSM::ProcessReplay(
    time_t inTimeNow)
{
    mImpl.ProcessReplay(inTimeNow);
}

    time_t
MetaVrSM::Process(
    time_t              inTimeNow,
    const MetaVrLogSeq& inCommittedSeq,
    int64_t             inErrChecksum,
    fid_t               inCommittedFidSeed,
    int                 inCommittedStatus,
    const MetaVrLogSeq& inReplayLastLogSeq,
    int&                outVrStatus,
    MetaRequest*&       outReqPtr)
{
    return mImpl.Process(inTimeNow,
        inCommittedSeq, inErrChecksum, inCommittedFidSeed, inCommittedStatus,
        inReplayLastLogSeq, outVrStatus, outReqPtr
    );
}

    int
MetaVrSM::Start(
    MetaDataSync&         inMetaDataSync,
    NetManager&           inNetManager,
    const UniqueID&       inFileId,
    Replay&               inReplayer,
    int64_t               inFileSystemId,
    const ServerLocation& inDataStoreLocation,
    const string&         inMetaMd)
{
    return mImpl.Start(
        inMetaDataSync, inNetManager, inFileId, inReplayer,
        inFileSystemId, inDataStoreLocation, inMetaMd
    );
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
    const Properties& inParameters,
    const char*       inMetaMdPtr)
{
    return mImpl.SetParameters(inPrefixPtr, inParameters, inMetaMdPtr);
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

    MetaVrSM::NodeId
MetaVrSM::GetNodeId() const
{
    return mImpl.GetNodeId();
}

    MetaVrLogSeq
MetaVrSM::GetLastLogSeq() const
{
    return mImpl.GetLastLogSeq();
}

    const ServerLocation&
MetaVrSM::GetMetaDataStoreLocation() const
{
    return mImpl.GetMetaDataStoreLocation();
}

    MetaVrSM::NodeId
MetaVrSM::GetPrimaryNodeId(
    const MetaVrLogSeq& inSeq) const
{
    return mImpl.GetPrimaryNodeId(inSeq);
}

    MetaVrSM::NodeId
MetaVrSM::GetPrimaryNodeId() const
{
    return mImpl.GetPrimaryNodeId();
}

    bool
MetaVrSM::ValidateAckPrimaryId(
    MetaVrSM::NodeId inNodeId,
    MetaVrSM::NodeId inPrimaryNodeId)
{
    return mImpl.ValidateAckPrimaryId(inNodeId, inPrimaryNodeId);
}

    /* static */ const char*
MetaVrSM::GetStateName(
    int inState)
{
    return Impl::GetStateName(inState);
}

    /* static */ MetaVrSM::NodeId
MetaVrSM::GetNodeId(
    const MetaRequest& inReq)
{
    return Impl::GetNodeId(inReq);
}

} // namespace KFS
