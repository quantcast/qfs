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
#include "LogTransmitter.h"
#include "MetaDataSync.h"
#include "Replay.h"
#include "meta.h"
#include "util.h"

#include "qcdio/QCMutex.h"
#include "qcdio/qcstutils.h"

#include "common/IntToString.h"
#include "common/BufferInputStream.h"
#include "common/StdAllocator.h"
#include "common/MsgLogger.h"
#include "common/kfserrno.h"

#include "kfsio/NetManager.h"

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
    seq_t          mStatus;
    StringBufT<64> mStatusMsg;
    seq_t          mEpochSeq;
    seq_t          mViewSeq;
    MetaVrLogSeq   mCommittedSeq;
    int64_t        mCommittedErrChecksum;
    fid_t          mCommittedFidSeed;
    int            mCommittedStatus;
    MetaVrLogSeq   mLastLogSeq;
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
        Get(inProps, kMetaVrLastLogSeqFieldNamePtr, mLastLogSeq);
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
        return (inStream <<
            "status: "  << mStatus <<
            " "         << ((mStatusMsg.empty() && mStatus < 0) ?
                ErrorCodeToString(mStatus).c_str() : mStatusMsg.c_str()) <<
            " epoch: "  << mEpochSeq <<
            " view: "   << mViewSeq <<
            " state: "  << MetaVrSM::GetStateName(mState) <<
            " log:"
            " committed:"
            " seq: "    << mCommittedSeq <<
            " ec: "     << mCommittedErrChecksum <<
            " fids: "   << mCommittedFidSeed <<
            " s: "      << mCommittedStatus <<
            " last: "   << mLastLogSeq <<
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

template<typename T>
inline static T& operator<<(
    T&                    inStream,
    const MetaVrResponse& inResponse)
{   return inResponse.Display(inStream); }

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
    static const NodeId kBootstrapPrimaryNodeId = 0;

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
          mPendingChangeVewMaxLogDistance(-1),
          mEpochSeq(0),
          mViewSeq(0),
          mCommittedSeq(),
          mCommittedErrChecksum(0),
          mCommittedFidSeed(0),
          mCommittedStatus(0),
          mLastLogSeq(),
          mLastCommitSeq(),
          mLastCommitTime(0),
          mTimeNow(),
          mLastProcessTime(mTimeNow),
          mLastReceivedTime(mTimeNow),
          mLastUpTime(mTimeNow),
          mViewChangeStartTime(mTimeNow),
          mStateSetTime(mTimeNow),
          mStartViewChangeRecvViewSeq(-1),
          mStartViewChangeMaxLastLogSeq(),
          mStartViewChangeMaxCommittedSeq(),
          mChannelsCount(0),
          mStartViewEpochMismatchCount(0),
          mReplyCount(0),
          mLogTransmittersSuspendedFlag(false),
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
          mInputStream(),
          mEmptyString(),
          mMutex(),
          mReconfigureCompletionCondVar(),
          mPendingReconfigureReqPtr(0),
          mVrResponse()
        { mStatus = GetStatus(); }
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
        return mStatus;
    }
    void LogBlockWriteDone(
        const MetaVrLogSeq& inBlockStartSeq,
        const MetaVrLogSeq& inBlockEndSeq,
        const MetaVrLogSeq& inCommittedSeq,
        bool                inWriteOkFlag)
    {
        if (inBlockEndSeq < inBlockStartSeq ||
                inBlockStartSeq < inCommittedSeq ||
                ! inCommittedSeq.IsValid()) {
            panic("VR: invalid log block commit sequence");
            return;
        }
        if (! inWriteOkFlag && mActiveFlag) {
            if (kStatePrimary == mState || kStateBackup == mState) {
                AdvanceView("log write failure");
            }
            return;
        }
        if (kStatePrimary != mState && kStateBackup != mState &&
                kStateLogSync != mState) {
            return;
        }
        if (mLastLogSeq < inBlockEndSeq) {
            if (mLastLogSeq <= mLastCommitSeq) {
                // Reset timer,
                mLastCommitTime = TimeNow();
            }
            mLastLogSeq = inBlockEndSeq;
        }
        if (kStatePrimary == mState) {
            // Update only if primary, backups are updated after queuing into
            // replay, in Process().
            mReplayLastLogSeq = mLastLogSeq;
        }
    }
    bool Handle(
        MetaRequest&        inReq,
        const MetaVrLogSeq& inLastLogSeq)
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
            case META_LOG_WRITER_CONTROL:
            case META_VR_LOG_START_VIEW:
                return false;
            default:
                if (kStateBackup == mState) {
                    inReq.status    = -EVRNOTPRIMARY;
                    inReq.statusMsg = "not primary, state: ";
                    inReq.statusMsg += GetStateName(mState);
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
                ! ValidateClusterKeyAndMd(inReq, inSeq, inProps, inNodeId)) {
            return;
        }
        const bool theNewFlag = mRespondedIds.insert(inNodeId).second;
        if (0 != mVrResponse.mStatus) {
            if (kStatePrimary == mVrResponse.mState ||
                    kStateBackup == mVrResponse.mState ||
                    kStateViewChange == mVrResponse.mState) {
                if (mEpochSeq <= mVrResponse.mEpochSeq) {
                    if (mVrResponse.mCommittedSeq.IsValid() &&
                            mVrResponse.mLastLogSeq.IsValid() &&
                            mVrResponse.mCommittedSeq <=
                                mVrResponse.mLastLogSeq) {
                        mStartViewChangeMaxCommittedSeq =
                            max(mStartViewChangeMaxCommittedSeq,
                                mVrResponse.mCommittedSeq);
                        mStartViewChangeMaxLastLogSeq =
                            mVrResponse.mCommittedSeq;
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
                " seq: "            << inSeq <<
                " node: "           << inNodeId <<
                " epoch mismatch: " << mStartViewEpochMismatchCount <<
                " max:"
                " committed: "      << mStartViewChangeMaxCommittedSeq <<
                " last: "           << mStartViewChangeMaxLastLogSeq <<
                " count: "          << mNodeIdAndMDSLocations.size() <<
                " responses: "      << mRespondedIds.size() <<
                " "                 << inReq.Show() <<
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
                ! ValidateClusterKeyAndMd(inReq, inSeq, inProps, inNodeId) ||
                0 != mVrResponse.mStatus ||
                (inNodeId == mNodeId ? kStateStartViewPrimary : kStateBackup) !=
                    mVrResponse.mState ||
                mVrResponse.mEpochSeq != mEpochSeq ||
                mVrResponse.mLastLogSeq != mLastLogSeq) {
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
    NodeId GetPrimaryNodeId() const
    {
        if (mActiveFlag && 0 < mQuorum) {
            if (kStatePrimary == mState) {
                return mNodeId;
            }
            if (kStateBackup == mState) {
                if (0 <= mPrimaryNodeId) {
                    return mPrimaryNodeId;
                }
                return (-mPrimaryNodeId - 1);
            }
        }
        return -1;
    }
    void Process(
        time_t              inTimeNow,
        time_t              inLastReceivedTime,
        const MetaVrLogSeq& inCommittedSeq,
        int64_t             inErrChecksum,
        fid_t               inCommittedFidSeed,
        int                 inCommittedStatus,
        const MetaVrLogSeq& inLastLogSeq,
        int&                outVrStatus,
        MetaRequest*&       outReqPtr)
    {
        if (mPendingReconfigureReqPtr) {
            QCStMutexLocker theLocker(mMutex);
            MetaVrReconfiguration& theReq = *mPendingReconfigureReqPtr;
            StartReconfiguration(theReq);
            mReconfigureReqPtr = 0;
            Commit(theReq);
            mPendingReconfigureReqPtr = 0;
            mReconfigureCompletionCondVar.Notify();
        }
        if ((kStatePrimary == mState || kStateBackup == mState) &&
                mLastReceivedTime < inLastReceivedTime) {
            mLastReceivedTime = inLastReceivedTime;
        }
        if (mCommittedSeq < inCommittedSeq) {
            mCommittedSeq         = inCommittedSeq;
            mCommittedErrChecksum = inErrChecksum;
            mCommittedStatus      = inCommittedStatus;
            mCommittedFidSeed     = inCommittedFidSeed;
        }
        if (mLastLogSeq < inLastLogSeq) {
            mLastLogSeq = inLastLogSeq;
        }
        if (mReplayLastLogSeq < inLastLogSeq) {
            mReplayLastLogSeq = inLastLogSeq;
            if (mPrimaryNodeId < 0 && kStateBackup == mState && mActiveFlag &&
                    0 < mActiveCount && 0 < mQuorum) {
                // Make primary node ID valid on backup after the first
                // successful reply to ignore start view change from all nodes
                // except primary.
                mPrimaryNodeId = -mPrimaryNodeId - 1;
            }
        }
        outReqPtr = 0;
        mTimeNow  = inTimeNow;
        if (kStateLogSync == mState) {
            bool      theProgressFlag = false;
            const int theStatus       =
                mMetaDataSyncPtr->GetLogFetchStatus(theProgressFlag);
            if (theStatus < 0 || ! theProgressFlag) {
                KFS_LOG_STREAM(theStatus < 0 ?
                        MsgLogger::kLogLevelINFO :
                        MsgLogger::kLogLevelERROR) <<
                    "log fetch done:"
                    " status: " << theStatus <<
                    " end: "    << mLogFetchEndSeq <<
                    " last: "   << mLastLogSeq <<
                KFS_LOG_EOM;
                if (mEpochSeq == mLastLogSeq.mEpochSeq &&
                        mViewSeq <= mLastLogSeq.mViewSeq) {
                    mViewSeq = mLastLogSeq.mViewSeq + 1;
                }
                mLogFetchEndSeq = MetaVrLogSeq();
                if (mActiveFlag) {
                    StartViewChange();
                } else {
                    SetState(kStateBackup);
                    mPrimaryNodeId = -1;
                }
            }
        } else if (kStateBackup == mState) {
            if (mActiveFlag && mLastReceivedTime + mConfig.GetBackupTimeout() <
                    TimeNow()) {
                AdvanceView("backup receive timed out");
            } else if (! mLogTransmittersSuspendedFlag &&
                    (! mActiveFlag || 0 <= mPrimaryNodeId) &&
                    mStateSetTime + 4 * mConfig.GetBackupTimeout() <
                        TimeNow()) {
                KFS_LOG_STREAM_DEBUG <<
                    "suspending log transmitter" <<
                KFS_LOG_EOM;
                mLogTransmittersSuspendedFlag = true;
                mLogTransmitter.Suspend(mLogTransmittersSuspendedFlag);
            }
        } else if (kStatePrimary == mState) {
            if (mActiveFlag) {
                if (mLogTransmitter.IsUp()) {
                    mLastUpTime = inTimeNow;
                    if (0 < mQuorum && mLastCommitSeq < mLastLogSeq &&
                            mLastCommitTime + mConfig.GetPrimaryTimeout() <
                                TimeNow()) {
                        AdvanceView("primary commit timed out");
                    }
                } else {
                    if (HasPrimaryTimedOut()) {
                        AdvanceView("primary timed out");
                    }
                }
                outReqPtr = mMetaVrLogStartViewPtr;
                mMetaVrLogStartViewPtr = 0;
            }
        } else {
            if (mActiveFlag) {
                if (TimeNow() != mLastProcessTime) {
                    StartDoViewChangeIfPossible();
                }
            } else {
                CancelViewChange();
                if (mQuorum <= 0 && mActiveCount < 0) {
                    NullQuorumStartView();
                } else {
                    SetState(kStateBackup);
                    mPrimaryNodeId = -1;
                }
            }
        }
        mLastProcessTime = TimeNow();
        outVrStatus      = mStatus;
        if (kStatePrimary != mState && (mMetaVrLogStartViewPtr || outReqPtr)) {
            panic("VR: invalid outstanding log start view");
        }
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
        if ((mConfig.IsEmpty() && mNodeId < 0) ||
                (mActiveCount <= 0 && mQuorum <= 0 &&
                    ((mConfig.IsEmpty() && 0 == mNodeId) ||
                        AllInactiveFindPrimary() == mNodeId))) {
            inReplayer.commitAll();
        }
        mMetaDataSyncPtr = &inMetaDataSync;
        mNetManagerPtr   = &inNetManager;
        mMetaMd          = inMetaMd;
        mPrimaryNodeId   = -1;
        mTimeNow         = mNetManagerPtr->Now();
        if (mConfig.IsEmpty() && mNodeId < 0) {
            mActiveCount = 0;
            mQuorum      = 0;
            SetState(kStatePrimary);
            mLastUpTime  = TimeNow();
            mActiveFlag  = true;
        } else {
            if (! mConfig.IsEmpty() && mNodeId < 0) {
                KFS_LOG_STREAM_ERROR <<
                    "invalid or unspecified VR node id: " << mNodeId <<
                    " with non empty VR config" <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            if (mActiveCount <= 0 && mQuorum <= 0 &&
                    ((mConfig.IsEmpty() &&
                        kBootstrapPrimaryNodeId == mNodeId) ||
                        AllInactiveFindPrimary() == mNodeId)) {
                SetState(kStatePrimary);
                mLastUpTime = TimeNow();
            } else {
                if (mActiveCount <= 0 && mQuorum <= 0 && mConfig.IsEmpty()) {
                    KFS_LOG_STREAM_WARN <<
                        "transition into backup state with empty VR"
                        " configuration, and node id non 0"
                        " node id: " << mNodeId <<
                    KFS_LOG_EOM;
                }
                SetState(kStateBackup);
                mLastReceivedTime = TimeNow() - 2 * mConfig.GetBackupTimeout();
            }
            mActiveFlag = IsActive(mNodeId);
        }
        mLastCommitTime       = TimeNow();
        mCommittedSeq         = inReplayer.getCommitted();
        mLastCommitSeq        = mCommittedSeq;
        mCommittedErrChecksum = inReplayer.getErrChksum();
        mCommittedFidSeed     = inFileId.getseed();
        mCommittedStatus      = inReplayer.getLastCommittedStatus();
        mLastLogSeq           = inReplayer.getLastLogSeq();
        mReplayLastLogSeq     = mLastLogSeq;
        mFileSystemId         = inFileSystemId;
        if (mEpochSeq < mCommittedSeq.mEpochSeq) {
            mEpochSeq = mCommittedSeq.mEpochSeq;
            mViewSeq  = mCommittedSeq.mViewSeq;
        } else if (mEpochSeq == mCommittedSeq.mEpochSeq &&
                mViewSeq < mCommittedSeq.mViewSeq) {
            mViewSeq = mCommittedSeq.mViewSeq;
        }
        if (mEpochSeq == mLastLogSeq.mEpochSeq &&
                mViewSeq < mLastLogSeq.mViewSeq) {
            mViewSeq = mLastLogSeq.mViewSeq;
        }
        if (! mMetaDataStoreLocation.IsValid()) {
            mMetaDataStoreLocation = inDataStoreLocation;
        }
        ConfigUpdate();
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
        const Properties::String* const theStrPtr = inParameters.getValue(
            "metaServer.Vr.metaDataStoreLocation");
        if (theStrPtr) {
            ServerLocation theLocation;
            if (theLocation.FromString(
                    theStrPtr->GetPtr(), theStrPtr->GetSize())) {
                mMetaDataStoreLocation = theLocation;
            }
        }
        mClusterKey = inParameters.getValue(
            kMetaClusterKeyParamNamePtr, mClusterKey);
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
        // Commit sequence will be set by process, once re-configuration goes
        // through the replay.
        if (mReconfigureReqPtr && mReconfigureReqPtr->logseq <= inLogSeq) {
            const MetaVrReconfiguration& theReq = *mReconfigureReqPtr;
            mReconfigureReqPtr = 0;
            Commit(theReq);
        }
        if (kStatePrimary == mState && mActiveFlag) {
            if (mLastCommitSeq < inLogSeq && inLogSeq <= mLastLogSeq) {
                mLastCommitSeq  = inLogSeq;
                mLastCommitTime = TimeNow();
            }
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
            if (! (theStream >> theSeq) || theSeq < 0) {
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
            mQuorum = mActiveCount - (mActiveCount - 1) / 2;
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

    LogTransmitter&              mLogTransmitter;
    MetaDataSync*                mMetaDataSyncPtr;
    NetManager*                  mNetManagerPtr;
    MetaVrSM&                    mMetaVrSM;
    int&                         mStatus;
    int64_t                      mFileSystemId;
    State                        mState;
    NodeId                       mNodeId;
    string                       mClusterKey;
    string                       mMetaMd;
    MetaMds                      mMetaMds;
    const MetaVrReconfiguration* mReconfigureReqPtr;
    Locations                    mPendingLocations;
    ChangesList                  mPendingChangesList;
    Config                       mConfig;
    Locations                    mAllUniqueLocations;
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
    int64_t                      mCommittedErrChecksum;
    fid_t                        mCommittedFidSeed;
    int                          mCommittedStatus;
    MetaVrLogSeq                 mLastLogSeq;
    MetaVrLogSeq                 mReplayLastLogSeq;
    MetaVrLogSeq                 mLastCommitSeq;
    time_t                       mLastCommitTime;
    time_t                       mTimeNow;
    time_t                       mLastProcessTime;
    time_t                       mLastReceivedTime;
    time_t                       mLastUpTime;
    time_t                       mViewChangeStartTime;
    time_t                       mStateSetTime;
    seq_t                        mStartViewChangeRecvViewSeq;
    MetaVrLogSeq                 mStartViewChangeMaxLastLogSeq;
    MetaVrLogSeq                 mStartViewChangeMaxCommittedSeq;
    int                          mChannelsCount;
    int                          mStartViewEpochMismatchCount;
    int                          mReplyCount;
    bool                         mLogTransmittersSuspendedFlag;
    NodeId                       mPrimaryNodeId;
    MetaVrStartViewChange*       mStartViewChangePtr;
    MetaVrDoViewChange*          mDoViewChangePtr;
    MetaVrStartView*             mStartViewPtr;
    MetaVrLogStartView*          mMetaVrLogStartViewPtr;
    NodeIdSet                    mStartViewChangeNodeIds;
    NodeIdSet                    mDoViewChangeNodeIds;
    NodeIdSet                    mRespondedIds;
    NodeIdAndMDSLocations        mNodeIdAndMDSLocations;
    MetaVrLogSeq                 mLogFetchEndSeq;
    ServerLocation               mMetaDataStoreLocation;
    MetaDataSync::Servers        mSyncServers;
    BufferInputStream            mInputStream;
    string const                 mEmptyString;
    QCMutex                      mMutex;
    QCCondVar                    mReconfigureCompletionCondVar;
    MetaVrReconfiguration*       mPendingReconfigureReqPtr;
    MetaVrResponse               mVrResponse;

    void Wakeup()
        { mNetManagerPtr->Wakeup(); }
    int GetStatus() const
    {
        return ((! mActiveFlag && 0 < mQuorum) ? -EVRNOTPRIMARY :
            (kStatePrimary == mState ? 0 :
            (kStateBackup == mState ? -EVRNOTPRIMARY : -ELOGFAILED)));
    }
    void SetState(
        State inState)
    {
        if (mState != inState) {
            mState  = inState;
            mStatus = GetStatus();
        }
        mStateSetTime = TimeNow();
    }
    bool HasPrimaryTimedOut() const
    {
        return (mLastUpTime + (mConfig.GetPrimaryTimeout() + 1) / 2 <
            TimeNow());
    }
    void ConfigUpdate()
    {
        mChannelsCount = mLogTransmitter.Update(mMetaVrSM);
        mStatus        = GetStatus();
    }
    bool ValidateClusterKeyAndMd(
        MetaVrRequest&    inReq,
        seq_t             inSeq,
        const Properties& inProps,
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
            " last log: "      << mLastLogSeq <<
        KFS_LOG_EOM;
        if (MetaVrLogSeq(mEpochSeq, mViewSeq, 0) <= mLastLogSeq ||
                mLastLogSeq.mLogSeq < 0) {
            panic("VR: invalid epoch, view, or last log sequence");
            return;
        }
        SetState(kStatePrimary);
        mPrimaryNodeId  = mNodeId;
        mLastUpTime     = TimeNow();
        mLastCommitTime = mLastUpTime;
        MetaRequest::Release(mMetaVrLogStartViewPtr);
        mMetaVrLogStartViewPtr = new MetaVrLogStartView();
        mMetaVrLogStartViewPtr->mCommittedSeq = mLastLogSeq;
        mMetaVrLogStartViewPtr->mNewLogSeq    =
            MetaVrLogSeq(mEpochSeq, mViewSeq, mLastLogSeq.mLogSeq + 1);
        mMetaVrLogStartViewPtr->mNodeId       = mNodeId;
        mMetaVrLogStartViewPtr->mTime         = TimeNow();
        if (! mMetaVrLogStartViewPtr->Validate()) {
            panic("VR: invalid log start op");
        }
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
        StartViewChange();
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
            " do view change: " <<
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
            if (mEpochSeq == mLastLogSeq.mEpochSeq &&
                    mViewSeq <= mLastLogSeq.mViewSeq) {
                mViewSeq = mLastLogSeq.mViewSeq + 1;
            }
            StartViewChange();
        }
    }
    void StartDoViewChangeIfPossible()
    {
        if (! mActiveFlag) {
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
                        " servers: "
                        " total: "    << mSyncServers.size() <<
                        " first: "    << mSyncServers.front() <<
                        " ["          << mLastLogSeq <<
                        ","           << mStartViewChangeMaxLastLogSeq <<
                        "]"           <<
                        " prev end: " << mLogFetchEndSeq <<
                    KFS_LOG_EOM;
                    SetState(kStateLogSync);
                    mLogFetchEndSeq = mStartViewChangeMaxLastLogSeq;
                    const bool kAllowNonPrimaryFlag = true;
                    mMetaDataSyncPtr->ScheduleLogSync(
                        mSyncServers,
                        mLastLogSeq,
                        mStartViewChangeMaxLastLogSeq,
                        kAllowNonPrimaryFlag
                    );
                }
            }
            return;
        }
        const size_t theSz = mStartViewChangeNodeIds.size();
        if (theSz < mQuorum) {
            RetryStartViewChange("not sufficient number of noded responded");
            return;
        }
        if (theSz < (size_t)mActiveCount &&
                (int)mRespondedIds.size() < mActiveCount &&
                mChannelsCount <= mReplyCount &&
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
        inReq.mRetCurViewSeq            = mViewSeq;
        inReq.mRetCurEpochSeq           = mEpochSeq;
        inReq.mRetCurState              = mState;
        inReq.mRetCommittedSeq          = mCommittedSeq;
        inReq.mRetCommittedErrChecksum  = mCommittedErrChecksum;
        inReq.mRetCommittedStatus       = mCommittedStatus;
        inReq.mRetCommittedFidSeed      = mCommittedFidSeed;
        inReq.mRetLastLogSeq            = mLastLogSeq;
        inReq.mRetFileSystemId          = mFileSystemId;
        inReq.mRetClusterKey            = mClusterKey;
        inReq.mRetMetaMd                = mMetaMd;
        inReq.mRetMetaDataStoreLocation = mMetaDataStoreLocation;
    }
    bool VerifyClusterKey(
        MetaVrRequest& inReq)
    {
        if (mClusterKey != inReq.mClusterKey) {
            inReq.statusMsg = "cluster key does not match";
        } else if (! mMetaMds.empty() &&
                mMetaMds.find(inReq.mMetaMd) == mMetaMds.end()) {
            inReq.statusMsg = "invalid meta server executable's MD";
        } else if (mFileSystemId != inReq.mFileSystemId) {
            inReq.statusMsg = "file system ID does not match";
        } else {
            return true;
        }
        inReq.status = -EBADCLUSTERKEY;
        return false;
    }
    bool VerifyViewChange(
        MetaVrRequest& inReq)
    {
        if (! VerifyClusterKey(inReq)) {
            // Set return state and return false at the end.
        } else if (! mActiveFlag) {
            inReq.status    = -ENOENT;
            inReq.statusMsg = "node inactive";
        } else if (! IsActive(inReq.mNodeId)) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "request from inactive node";
        } else if (inReq.mEpochSeq < inReq.mLastLogSeq.mEpochSeq ||
                inReq.mViewSeq <= inReq.mLastLogSeq.mViewSeq) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "invalid request: last log sequence";
        } else if (inReq.mLastLogSeq < mCommittedSeq &&
                (kStatePrimary != mState && kStateBackup != mState)) {
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
    bool Handle(
        MetaVrHello& inReq)
    {
        if (0 == inReq.status && VerifyClusterKey(inReq) &&
                kStatePrimary == inReq.mCurState &&
                0 < mQuorum && mActiveFlag) {
            if (kStatePrimary == mState) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "cuuent node state: ";
                inReq.statusMsg += GetStateName(mState);
            } else if (inReq.mLastLogSeq < mCommittedSeq) {
                inReq.status    = -EINVAL;
                inReq.statusMsg =
                    "last log sequence is less than committed";
            }
        }
        Show(inReq);
        SetReturnState(inReq);
        if (0 == inReq.status &&
                mNodeId != inReq.mNodeId &&
                kStatePrimary == inReq.mCurState &&
                kStateLogSync != mState &&
                (! mActiveFlag || (mQuorum <= 0 && mActiveCount <= 0))) {
            if (mLastLogSeq < inReq.mLastLogSeq) {
                const bool kAllowNonPrimaryFlag = false;
                ScheduleLogFetch(
                    inReq.mLastLogSeq,
                    GetDataStoreLocation(inReq),
                    kAllowNonPrimaryFlag
                );
            } else if (mCommittedSeq < inReq.mCommittedSeq &&
                    inReq.mCommittedSeq <= mLastLogSeq &&
                    0 <= inReq.mCommittedFidSeed &&
                    0 <= inReq.mCommittedStatus) {
                inReq.SetScheduleCommit();
            }
        }
        return true;
    }
    bool Handle(
        MetaVrStartViewChange& inReq)
    {
        Show(inReq, "+");
        if (VerifyViewChange(inReq)) {
            if (mViewSeq != inReq.mViewSeq) {
                if (kStateLogSync != mState &&
                        (kStateBackup != mState ||
                            mPrimaryNodeId < 0 ||
                            inReq.mNodeId == mPrimaryNodeId ||
                            mLastReceivedTime + mConfig.GetBackupTimeout() <
                                TimeNow())) {
                    mViewSeq = inReq.mViewSeq;
                    StartViewChange();
                    if (kStateViewChange == mState &&
                            ! mDoViewChangePtr &&
                            mStartViewChangeNodeIds.insert(
                                inReq.mNodeId).second) {
                        StartDoViewChangeIfPossible();
                    }
                }
            } else {
                if (kStateViewChange == mState) {
                    if (! mDoViewChangePtr &&
                            mStartViewChangeNodeIds.insert(
                                inReq.mNodeId).second) {
                        StartDoViewChangeIfPossible();
                    }
                } else {
                    inReq.status    = -EINVAL;
                    inReq.statusMsg = "ignored state: ";
                    inReq.statusMsg += GetStateName(mState);
                }
            }
            SetReturnState(inReq);
        } else if (inReq.status != -EBADCLUSTERKEY) {
            if (mLastLogSeq < inReq.mLastLogSeq) {
                if (kStateLogSync != mState) {
                    const bool kAllowNonPrimaryFlag = true;
                    ScheduleLogFetch(
                        inReq.mLastLogSeq,
                        GetDataStoreLocation(inReq),
                        kAllowNonPrimaryFlag
                    );
                }
            } else if (mCommittedSeq < inReq.mCommittedSeq &&
                    inReq.mCommittedSeq <= mLastLogSeq &&
                    0 <= inReq.mCommittedFidSeed &&
                    0 <= inReq.mCommittedStatus) {
                inReq.SetScheduleCommit();
            }
        }
        Show(inReq, "=");
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
                    StartViewChange();
                }
            } else if (mNodeId != inReq.mPimaryNodeId) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "primary node id mismatch, state: ";
                inReq.statusMsg += GetStateName(mState);
            } else if (mLastLogSeq < inReq.mLastLogSeq) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "invalid last log sequence, state: ";
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
                        // Threat as start view change, in the do view change
                        // has not been started yet.
                        StartDoViewChangeIfPossible();
                    }
                } else if (kStateStartViewPrimary != mState &&
                        kStatePrimary != mState) {
                    inReq.status    = -EINVAL;
                    inReq.statusMsg = "ignored, state: ";
                    inReq.statusMsg += GetStateName(mState);
                }
            }
            SetReturnState(inReq);
        }
        Show(inReq, "=");
        return true;
    }
    bool Handle(
        MetaVrStartView& inReq)
    {
        Show(inReq, "+");
        if (VerifyViewChange(inReq)) {
            if (mViewSeq != inReq.mViewSeq || (kStateViewChange != mState &&
                    kStateStartViewPrimary != mState)) {
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
                    CancelViewChange();
                    SetState(kStateBackup);
                    mPrimaryNodeId    = -inReq.mNodeId - 1;
                    mLastReceivedTime = TimeNow();
                }
            }
            SetReturnState(inReq);
        }
        Show(inReq, "=");
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
            if (inReq.replayFlag && 0 == inReq.status) {
                if (mStartedFlag) {
                    QCStMutexLocker theLocker(mMutex);
                    if (mPendingReconfigureReqPtr) {
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
            }
            // If not replay, then prepare and commit are handled by the log
            // writer.
        }
        return (0 != inReq.status);
    }
    void StartReconfiguration(
        MetaVrReconfiguration& inReq)
    {
        if ((inReq.logseq.IsValid() ?
                ((kStateBackup != mState && kStateLogSync != mState) ||
                    ! inReq.replayFlag) : (kStatePrimary != mState))) {
            inReq.status    = -EINVAL;
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
            inReq.statusMsg += kMetaVrNodeIdParameterNamePtr;
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
        if (! inReq.logseq.IsValid()) {
            // Initial boot strap: do no allow other node to become primary
            // in the case if all the nodes are inactive.
            if (mConfig.IsEmpty()) {
                if (mNodeId != inReq.mNodeId) {
                    inReq.status    = -EINVAL;
                    inReq.statusMsg = "add node: first node id to add must"
                        " be equal to this node id; and must have"
                        " smallest node id, or smallest primary order";
                    return;
                }
            } else {
                if (mActiveCount <= 0 && mQuorum <= 0 &&
                        inReq.mNodeId != mNodeId) {
                    Config::Nodes::const_iterator const theIt =
                        mConfig.GetNodes().find(mNodeId);
                    if (inReq.mPrimaryOrder < theIt->second.GetPrimaryOrder() ||
                            inReq.mNodeId < mNodeId) {
                        inReq.status    = -EINVAL;
                        inReq.statusMsg = "node order cannot be lower than this"
                            " node order, or if order is equal, the node id"
                            " cannot be smaller that this node id";
                        return;
                    }
                }
            }
        }
        mPendingLocations.clear();
        const char*       thePtr    = inReq.mListStr.GetPtr();
        const char* const theEndPtr = thePtr + inReq.mListStr.GetSize();
        ServerLocation    theLocation;
        while (thePtr < theEndPtr) {
            if (! theLocation.ParseString(
                    thePtr, theEndPtr - thePtr, inReq.shortRpcFormatFlag)) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "add node: listener address parse error";
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
        if (! inReq.logseq.IsValid() &&
                0 == inReq.status &&
                mPendingChangesList.size() != mConfig.GetNodes().size() &&
                mActiveCount <= 0 && mQuorum <= 0) {
            // Initial boot strap: do no allow to remove primary
            // in the case if all the nodes are inactive.
            for (ChangesList::const_iterator theIt =
                    mPendingChangesList.begin();
                    mPendingChangesList.end() != theIt;
                    ++theIt) {
                if (theIt->first == mNodeId) {
                    inReq.status    = -EINVAL;
                    inReq.statusMsg = "removing primary node is not supported"
                        " all nodes are inactive,"
                        " and configuration with more than one node";
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
                        SetState(kStateReconfiguration);
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
        if (mPendingChangesList.empty()) {
            inReq.status    = -EINVAL;
            inReq.statusMsg = "no changes specified";
            return;
        }
        sort(mPendingChangesList.begin(), mPendingChangesList.end());
        ChangesList::const_iterator theMinIt = mPendingChangesList.begin();
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
            if (theIt->second < theMinIt->second) {
                theMinIt = theIt;
            }
        }
        if (! inReq.logseq.IsValid() &&
                size_t(1) < mConfig.GetNodes().size() &&
                mPendingChangesList.end() != theMinIt &&
                mActiveCount <= 0 && mQuorum <= 0) {
            // Initial boot strap: do no allow to choose new primary by changing
            // the order.
            Config::Nodes::const_iterator const theIt =
                mConfig.GetNodes().find(mNodeId);
            if (theIt != mConfig.GetNodes().end() &&
                    ((theMinIt->first == mNodeId &&
                        theIt->second.GetPrimaryOrder() < theMinIt->second) ||
                    theMinIt->second < theIt->second.GetPrimaryOrder() ||
                    (theMinIt->second == theIt->second.GetPrimaryOrder() &&
                        theMinIt->first < theIt->first))) {
                inReq.status    = -EINVAL;
                inReq.statusMsg = "changing node primary order is not supported"
                    " when all nodes are inactive,"
                    " and setting primary order makes primary a different node,"
                    " or increases order of this / current primary node";
                mPendingChangesList.clear();
                return;
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
            SetState(kStateReconfiguration);
        }
    }
    static bool ValidateTimeouts(
        int inPrimaryTimeout,
        int inBackupTimeout)
    {
        return (
            0 < inPrimaryTimeout &&
            (int64_t)inPrimaryTimeout + 3 <= inBackupTimeout
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
            SetState(kStateReconfiguration);
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
        int    status = 0;
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
        mPendingChangesList.clear();
        mPendingLocations.clear();
        ConfigUpdate();
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
    void PrimaryReconfigurationStartViewChange()
    {
        if (kStateReconfiguration != mState) {
            panic("VR: invalid end reconfiguration primary state");
            return;
        }
        if (! mActiveFlag) {
            // Primary starts view change.
            SetState(kStateBackup);
            return;
        }
        StartViewChange();
    }
    NodeId GetPrimaryId()
    {
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
            if (mStartViewChangeNodeIds.find(theNodeId) ==
                        mStartViewChangeNodeIds.end() &&
                    mDoViewChangeNodeIds.find(theNodeId) ==
                        mDoViewChangeNodeIds.end()) {
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
    NodeId AllInactiveFindPrimary()
    {
        if (0 < mQuorum || 0 < mActiveCount) {
            return -1;
        }
        NodeId theId       = -1;
        int    theMinOrder = numeric_limits<int>::max();
        const Config::Nodes& theNodes = mConfig.GetNodes();
        for (Config::Nodes::const_iterator theIt = theNodes.begin();
                theNodes.end() != theIt;
                ++theIt) {
            const NodeId        theNodeId = theIt->first;
            const Config::Node& theNode   = theIt->second;
            const int           theOrder  = theNode.GetPrimaryOrder();
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
        inReq.mFileSystemId         = mFileSystemId;
        inReq.mMetaDataStoreHost    = mMetaDataStoreLocation.hostname;
        inReq.mMetaDataStorePort    = mMetaDataStoreLocation.port;
        inReq.mClusterKey           = mClusterKey;
        inReq.mMetaMd               = mMetaMd;
        inReq.SetVrSMPtr(&mMetaVrSM);
    }
    void NullQuorumStartView()
    {
        if (0 < mQuorum) {
            panic("VR: null quorum start view invalid invocation");
            return;
        }
        if ((mActiveCount <= 0 ?
                (mConfig.IsEmpty() ?
                    kBootstrapPrimaryNodeId :
                    AllInactiveFindPrimary()) :
                GetPrimaryId()) == mNodeId) {
            PirmaryCommitStartView();
        } else {
            SetState(kStateBackup);
            mPrimaryNodeId = -1;
        }
    }
    void StartViewChange()
    {
        if (! mActiveFlag) {
            panic("VR: start view change: node non active");
            return;
        }
        mPrimaryNodeId = -1;
        CancelViewChange();
        if (mQuorum <= 0) {
            NullQuorumStartView();
            return;
        }
        SetState(kStateViewChange);
        mViewChangeStartTime = TimeNow();
        mStartViewChangeNodeIds.clear();
        mDoViewChangeNodeIds.clear();
        mNodeIdAndMDSLocations.clear();
        MetaVrStartViewChange& theOp = *(new MetaVrStartViewChange());
        Init(theOp);
        mStartViewChangePtr = &theOp;
        const NodeId kBroadcast = -1;
        KFS_LOG_STREAM_INFO <<
            "start view change: " << theOp.Show() <<
        KFS_LOG_EOM;
        QueueVrRequest(theOp, kBroadcast);
    }
    void StartDoViewChange(
        NodeId inPrimaryId)
    {
        if (! mActiveFlag || inPrimaryId < 0) {
            panic("VR: do view change: node non active");
            return;
        }
        CancelViewChange();
        if (mState != kStateViewChange) {
            mViewChangeStartTime = TimeNow();
            SetState(kStateViewChange);
        }
        MetaVrDoViewChange& theOp = *(new MetaVrDoViewChange());
        Init(theOp);
        theOp.mPimaryNodeId = inPrimaryId;
        mDoViewChangePtr = &theOp;
        QueueVrRequest(theOp, theOp.mPimaryNodeId);
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
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

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
    const MetaVrLogSeq& inCommittedSeq)
{
    return mImpl.HandleLogBlock(
        inBlockStartSeq, inBlockEndSeq, inCommittedSeq);
}

    void
MetaVrSM::LogBlockWriteDone(
    const MetaVrLogSeq& inBlockStartSeq,
    const MetaVrLogSeq& inBlockEndSeq,
    const MetaVrLogSeq& inCommittedSeq,
    bool                inWriteOkFlag)
{
    mImpl.LogBlockWriteDone(
        inBlockStartSeq, inBlockEndSeq, inCommittedSeq, inWriteOkFlag);
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
MetaVrSM::Process(
    time_t              inTimeNow,
    time_t              inLastReceivedTime,
    const MetaVrLogSeq& inCommittedSeq,
    int64_t             inErrChecksum,
    fid_t               inCommittedFidSeed,
    int                 inCommittedStatus,
    const MetaVrLogSeq& inLastLogSeq,
    int&                outVrStatus,
    MetaRequest*&       outReqPtr)
{
    mImpl.Process(inTimeNow, inLastReceivedTime,
        inCommittedSeq, inErrChecksum, inCommittedFidSeed, inCommittedStatus,
        inLastLogSeq, outVrStatus, outReqPtr
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
MetaVrSM::GetPrimaryNodeId() const
{
    return mImpl.GetPrimaryNodeId();
}

    /* static */ const char*
MetaVrSM::GetStateName(
    int inState)
{
    return Impl::GetStateName(Impl::State(inState));
}

    /* static */ MetaVrSM::NodeId
MetaVrSM::GetNodeId(
    const MetaRequest& inReq)
{
    return Impl::GetNodeId(inReq);
}

} // namespace KFS
