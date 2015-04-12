//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2009/05/20
// Author: Mike Ovsiannikov
//
// Copyright 2009-2012 Quantcast Corp.
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

#include "KfsNetClient.h"

#include "kfsio/IOBuffer.h"
#include "kfsio/NetConnection.h"
#include "kfsio/NetManager.h"
#include "kfsio/ITimeout.h"
#include "kfsio/ClientAuthContext.h"
#include "kfsio/DelegationToken.h"
#include "common/kfstypes.h"
#include "common/kfsdecls.h"
#include "common/MsgLogger.h"
#include "common/StdAllocator.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"
#include "qcdio/QCDLList.h"
#include "qcdio/QCThread.h"
#include "KfsOps.h"
#include "utils.h"

#include <sstream>
#include <algorithm>
#include <map>
#include <deque>
#include <string>
#include <cerrno>
#include <iomanip>

#include <stdint.h>
#include <time.h>
#include <stdlib.h>

namespace KFS
{
namespace client
{
using std::string;
using std::map;
using std::list;
using std::pair;
using std::make_pair;
using std::less;
using std::max;
using std::hex;
using std::showbase;

const int64_t kMaxSessionTimeout             = int64_t(10) * 365 * 24 * 60 * 60;
const int64_t kSessionUpdateResolutionSec    = LEASE_INTERVAL_SECS / 2;
const int64_t kSessionChangeStartIntervalSec = 30 * 60;

class KfsClientRefCount
{
public:
    class StRef
    {
    public:
        StRef(
            KfsClientRefCount& inObj)
            : mObj(inObj)
            { mObj.Ref(1); }
        ~StRef()
            { mObj.UnRef(); }
    private:
        KfsClientRefCount& mObj;
    private:
        StRef(
            StRef& inRef);
        StRef& operator=(
            StRef& inRef);
    };

    KfsClientRefCount(
        const QCThread* inThreadPtr)
        : mRefCount(0),
          mThreadPtr(inThreadPtr)
        {}
    void Ref(
        int inMinRefCount = 0)
    {
        ++mRefCount;
        QCRTASSERT(
            (! mThreadPtr || mThreadPtr->IsCurrentThread()) &&
            inMinRefCount < mRefCount
        );
    }
    void UnRef()
    {
        QCRTASSERT(
            (! mThreadPtr || mThreadPtr->IsCurrentThread()) &&
            0 < mRefCount
        );
        if (--mRefCount == 0) {
            delete this;
        }
    }
    int GetRefCount() const
        { return mRefCount; }
    void SetThread(
        const QCThread* inThreadPtr)
        { mThreadPtr = inThreadPtr; }
protected:
    virtual ~KfsClientRefCount()
        { mRefCount = -23456; }
private:
    int             mRefCount;
    const QCThread* mThreadPtr;
private:
    KfsClientRefCount(
        const KfsClientRefCount& inObj);
    KfsClientRefCount& operator=(
        const KfsClientRefCount& inObj);
};

// Generic KFS request / response protocol state machine implementation.
class KfsNetClient::Impl :
    public KfsCallbackObj,
    public KfsClientRefCount,
    private ITimeout,
    private OpOwner
{
public:
    typedef KfsClientRefCount::StRef StRef;

    Impl(
        string             inHost,
        int                inPort,
        int                inMaxRetryCount,
        int                inTimeSecBetweenRetries,
        int                inOpTimeoutSec,
        int                inIdleTimeoutSec,
        kfsSeq_t           inInitialSeqNum,
        const char*        inLogPrefixPtr,
        NetManager&        inNetManager,
        bool               inResetConnectionOnOpTimeoutFlag,
        int                inMaxContentLength,
        bool               inFailAllOpsOnOpTimeoutFlag,
        bool               inMaxOneOutstandingOpFlag,
        ClientAuthContext* inAuthContextPtr,
        const QCThread*    inThreadPtr)
        : KfsCallbackObj(),
          KfsClientRefCount(inThreadPtr),
          ITimeout(),
          OpOwner(),
          mServerLocation(inHost, inPort),
          mPendingOpQueue(),
          mQueueStack(),
          mConnPtr(),
          mNextSeqNum(max(kfsSeq_t(100), // allow to insert auth op(s) in front
            (inInitialSeqNum < 0 ? -inInitialSeqNum : inInitialSeqNum) >> 1)),
          mReadHeaderDoneFlag(false),
          mSleepingFlag(false),
          mDataReceivedFlag(false),
          mDataSentFlag(false),
          mAllDataSentFlag(false),
          mRetryConnectOnlyFlag(false),
          mIdleTimeoutFlag(false),
          mResetConnectionOnOpTimeoutFlag(inResetConnectionOnOpTimeoutFlag),
          mFailAllOpsOnOpTimeoutFlag(inFailAllOpsOnOpTimeoutFlag),
          mMaxOneOutstandingOpFlag(inMaxOneOutstandingOpFlag),
          mShutdownSslFlag(false),
          mSslShutdownInProgressFlag(false),
          mTimeSecBetweenRetries(inTimeSecBetweenRetries),
          mOpTimeoutSec(inOpTimeoutSec),
          mIdleTimeoutSec(inIdleTimeoutSec),
          mRetryCount(0),
          mNonAuthRetryCount(0),
          mContentLength(0),
          mMaxRetryCount(inMaxRetryCount),
          mMaxContentLength(inMaxContentLength),
          mAuthFailureCount(0),
          mMaxRpcHeaderLength(MAX_RPC_HEADER_LEN),
          mPendingBytesSend(0),
          mInFlightOpPtr(0),
          mOutstandingOpPtr(0),
          mInFlightRecvBufPtr(0),
          mCurOpIt(),
          mIstream(),
          mOstream(),
          mProperties(),
          mStats(),
          mDisconnectCount(0),
          mEventObserverPtr(0),
          mLogPrefix((inLogPrefixPtr && inLogPrefixPtr[0]) ?
                (inLogPrefixPtr + string(" ")) : string()),
          mNetManager(inNetManager),
          mAuthContextPtr(inAuthContextPtr),
          mSessionExpirationTime(-1),
          mKeyExpirationTime(-1),
          mKeyId(),
          mKeyData(),
          mSessionKeyId(),
          mSessionKeyData(),
          mLookupOp(-1, ROOTFID, "/"),
          mAuthOp(-1, kAuthenticationTypeUndef)
    {
        SET_HANDLER(this, &KfsNetClient::Impl::EventHandler);
    }
    bool IsConnected() const
        { return (mConnPtr && mConnPtr->IsGood()); }
    int64_t GetDisconnectCount() const
        { return mDisconnectCount; }
    bool Start(
        string             inServerName,
        int                inServerPort,
        string*            inErrMsgPtr,
        bool               inRetryPendingOpsFlag,
        int                inMaxRetryCount,
        int                inTimeSecBetweenRetries,
        bool               inRetryConnectOnlyFlag,
        ClientAuthContext* inAuthContextPtr)
    {
        if (! inRetryPendingOpsFlag) {
            Cancel();
        }
        mRetryConnectOnlyFlag  = inRetryConnectOnlyFlag;
        mMaxRetryCount         = inMaxRetryCount;
        mTimeSecBetweenRetries = inTimeSecBetweenRetries;
        mAuthContextPtr        = inAuthContextPtr;
        return SetServer(ServerLocation(inServerName, inServerPort),
            false, inErrMsgPtr, true);
    }
    bool SetServer(
        const ServerLocation& inLocation,
        bool                  inCancelPendingOpsFlag,
        string*               inErrMsgPtr,
        bool                  inForceConnectFlag)
    {
        if (inLocation == mServerLocation) {
            if (! inForceConnectFlag && mPendingOpQueue.empty()) {
                return inLocation.IsValid();
            }
            EnsureConnected(inErrMsgPtr);
            return (mSleepingFlag || IsConnected());
        }
        if (inCancelPendingOpsFlag) {
            Cancel();
        }
        if (mSleepingFlag || IsConnected()) {
            Reset();
        }
        mServerLocation    = inLocation;
        mAuthFailureCount  = 0;
        mRetryCount        = 0;
        mNonAuthRetryCount = 0;
        mNextSeqNum += 100;
        if (! inForceConnectFlag && mPendingOpQueue.empty()) {
            return inLocation.IsValid();
        }
        EnsureConnected(inErrMsgPtr);
        return (mSleepingFlag || IsConnected());
    }
    void SetAuthContext(
        ClientAuthContext* inAuthContextPtr)
        { mAuthContextPtr = inAuthContextPtr; }
    ClientAuthContext* GetAuthContext() const
        { return mAuthContextPtr; }
    void SetKey(
        const char* inKeyIdPtr,
        const char* inKeyDataPtr,
        int         inKeyDataSize)
        { SetKey(inKeyIdPtr, strlen(inKeyIdPtr), inKeyDataPtr, inKeyDataSize); }
    void SetKey(
        const char* inKeyIdPtr,
        int         inKeyIdLen,
        const char* inKeyDataPtr,
        int         inKeyDataSize)
    {
        if (inKeyIdPtr && inKeyIdLen > 0) {
            mKeyId.assign(inKeyIdPtr, (size_t)inKeyIdLen);
        } else {
            mKeyId.clear();
        }
        if (inKeyDataPtr && inKeyDataSize > 0) {
            mKeyData.assign(inKeyDataPtr, (size_t)inKeyDataSize);
            if (mKeyId.empty()) {
                mKeyExpirationTime = Now() + kMaxSessionTimeout;
            } else if (mKeyId != mSessionKeyId) {
                mKeyExpirationTime = GetTokenExpirationTime(mKeyId);
            }
        } else {
            mKeyData.clear();
        }
    }
    const string& GetKey() const
        { return mKeyData; }
    const string& GetKeyId() const
        { return mKeyId; }
    const string& GetSessionKey() const
        { return mSessionKeyData; }
    const string& GetSessionKeyId() const
        { return mSessionKeyId; }
    bool IsShutdownSsl() const
        { return mShutdownSslFlag; }
    void SetShutdownSsl(
        bool inFlag)
    {
        if (inFlag == mShutdownSslFlag) {
            return;
        }
        mShutdownSslFlag = inFlag;
        if (mShutdownSslFlag) {
            if (IsConnected() && mConnPtr->GetFilter()) {
                mSslShutdownInProgressFlag = true;
                const int theErr = mConnPtr->Shutdown();
                if (theErr != 0) {
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "ssl shutdown failure: " <<
                            theErr <<
                    KFS_LOG_EOM;
                    Reset();
                    if (! mPendingOpQueue.empty()) {
                        EnsureConnected();
                    }
                }
            }
        } else if (IsConnected()) {
            Reset();
            if (! mPendingOpQueue.empty()) {
                EnsureConnected();
            }
        }
    }
    void Reset()
    {
        if (mSleepingFlag) {
            mNetManager.UnRegisterTimeoutHandler(this);
            mSleepingFlag = false;
        }
        ResetConnection();
    }
    void CancelAllWithOwner(
        OpOwner* inOwnerPtr)
    {
        if (mInFlightOpPtr && mInFlightOpPtr->mOwnerPtr == inOwnerPtr) {
            CancelInFlightOp();
        }
        const bool thePendingEmptyFlag = mPendingOpQueue.empty();
        if (thePendingEmptyFlag && mQueueStack.empty()) {
            return;
        }
        if (! thePendingEmptyFlag) {
            OpQueue theQeue;
            for (OpQueue::iterator theIt = mPendingOpQueue.begin();
                    theIt != mPendingOpQueue.end();
                    ) {
                OpQueue::iterator const theCur = theIt++;
                if (theCur->second.mOwnerPtr == inOwnerPtr) {
                    theQeue.insert(*theCur);
                    mPendingOpQueue.erase(theCur);
                }
            }
            if (! theQeue.empty()) {
                QueueStack::iterator const theIt =
                    mQueueStack.insert(mQueueStack.end(), OpQueue());
                theQeue.swap(*theIt);
            }
        }
        for (QueueStack::iterator theStIt = mQueueStack.begin();
                theStIt != mQueueStack.end();
                ) {
            for (OpQueue::iterator theIt = theStIt->begin();
                    theIt != theStIt->end();
                    ++theIt) {
                if (theIt->second.mOwnerPtr != inOwnerPtr ||
                        ! theIt->second.mOpPtr) {
                    continue;
                }
                mStats.mOpsCancelledCount++;
                theIt->second.Cancel();
            }
            if (theStIt->empty()) {
                mQueueStack.erase(theStIt++);
            } else {
                ++theStIt;
            }
        }
    }
    void Stop()
    {
        Reset();
        mAuthFailureCount = 0;
        Cancel();
    }
    int GetMaxRetryCount() const
        { return mMaxRetryCount; }
    void SetMaxRetryCount(
        int inMaxRetryCount)
        { mMaxRetryCount = inMaxRetryCount; }
    int GetOpTimeoutSec() const
        { return mOpTimeoutSec; }
    void SetOpTimeoutSec(
        int inTimeout)
    {
        mOpTimeoutSec = inTimeout;
        if (IsConnected() && ! mPendingOpQueue.empty()) {
            mConnPtr->SetInactivityTimeout(mOpTimeoutSec);
        }
    }
    int GetIdleTimeoutSec() const
        { return mIdleTimeoutSec; }
    void SetIdleTimeoutSec(
        int inTimeout)
    {
        mIdleTimeoutSec = inTimeout;
        if (IsConnected() && mPendingOpQueue.empty()) {
            mConnPtr->SetInactivityTimeout(mIdleTimeoutSec);
        }
    }
    int GetTimeSecBetweenRetries() const
        { return mTimeSecBetweenRetries; }
    void SetTimeSecBetweenRetries(
        int inTimeSec)
        { mTimeSecBetweenRetries = inTimeSec; }
    bool IsAllDataSent() const
        { return (mDataSentFlag && mAllDataSentFlag); }
    bool IsDataReceived() const
        { return mDataReceivedFlag; }
    bool IsDataSent() const
        { return mDataSentFlag; }
    bool IsRetryConnectOnly() const
        { return mRetryConnectOnlyFlag; }
    bool WasDisconnected() const
        { return ((mDataSentFlag || mDataReceivedFlag) && ! IsConnected()); }
    void SetRetryConnectOnly(
        bool inFlag)
        { mRetryConnectOnlyFlag = inFlag; }
    void SetOpTimeout(
        int inOpTimeoutSec)
        { mOpTimeoutSec = inOpTimeoutSec; }
    int GetOpTimeout() const
        { return mOpTimeoutSec; }
    void GetStats(
        Stats& outStats) const
        { outStats = mStats; }
    const ServerLocation& GetServerLocation() const
        { return mServerLocation; }
    bool Enqueue(
        KfsOp*    inOpPtr,
        OpOwner*  inOwnerPtr,
        IOBuffer* inBufferPtr = 0)
    {
        const time_t theNow = Now();
        if (mPendingOpQueue.empty() && IsConnected() &&
                (mSessionKeyId.empty() ?
                (mSessionExpirationTime < theNow + kSessionUpdateResolutionSec) :
                (((mSessionExpirationTime <
                        theNow + kSessionChangeStartIntervalSec &&
                    mSessionExpirationTime + kSessionUpdateResolutionSec <
                        mKeyExpirationTime) ||
                    mSessionExpirationTime < theNow) &&
                ! mKeyId.empty() && theNow < mKeyExpirationTime))) {
            KFS_LOG_STREAM_INFO << mLogPrefix <<
                "updating session by initiating re-connect" <<
                " expires: +" << (mSessionExpirationTime - theNow) <<
            KFS_LOG_EOM;
            ResetConnection();
        }
        // Ensure that the op is in the queue before attempting to re-establish
        // connection, as later can fail other ops, and invoke the op cancel.
        // The op has to be in the queue in order for cancel to work.
        mStats.mOpsQueuedCount++;
        const bool theOkFlag = EnqueueSelf(inOpPtr, inOwnerPtr, inBufferPtr, 0);
        if (theOkFlag) {
            EnsureConnected(0, inOpPtr);
        }
        return theOkFlag;
    }
    bool Cancel(
        KfsOp*   inOpPtr,
        OpOwner* inOwnerPtr)
    {
        if (! inOpPtr) {
            return true; // Nothing to do.
        }
        OpQueue::iterator theIt = mPendingOpQueue.find(inOpPtr->seq);
        if (theIt != mPendingOpQueue.end()) {
            if (theIt->second.mOwnerPtr != inOwnerPtr ||
                    theIt->second.mOpPtr != inOpPtr) {
                return false;
            }
            Cancel(theIt);
            return true;
        }
        for (QueueStack::iterator theStIt = mQueueStack.begin();
                theStIt != mQueueStack.end();
                ++theStIt) {
            if ((theIt = theStIt->find(inOpPtr->seq)) != theStIt->end()) {
                if (theIt->second.mOwnerPtr != inOwnerPtr ||
                        theIt->second.mOpPtr != inOpPtr) {
                    return false;
                }
                if (&theIt->second == mInFlightOpPtr) {
                    CancelInFlightOp();
                }
                theIt->second.Cancel();
                break;
            }
        }
        return true;
    }
    bool Cancel()
        { return CancelOrFailAll(0, string()); }
    bool Fail(
        int           inStatus,
        const string& inStatusMsg,
        bool          inDisconnectFlag = true)
    {
        if (inDisconnectFlag) {
            Reset();
        }
        return CancelOrFailAll(inStatus, inStatusMsg);
    }
    bool CancelOrFailAll(
        int           inStatus,
        const string& inStatusMsg)
    {
        CancelInFlightOp();
        const bool thePendingEmptyFlag = mPendingOpQueue.empty();
        if (thePendingEmptyFlag && mQueueStack.empty()) {
            return false;
        }
        QueueStack::iterator theIt;
        if (! thePendingEmptyFlag) {
            theIt = mQueueStack.insert(mQueueStack.end(), OpQueue());
            mPendingOpQueue.swap(*theIt);
        }
        for (QueueStack::iterator theStIt = mQueueStack.begin();
                theStIt != mQueueStack.end();
                ++theStIt) {
            for (OpQueue::iterator theIt = theStIt->begin();
                    theIt != theStIt->end();
                    ++theIt) {
                if (! theIt->second.mOpPtr) {
                    continue;
                }
                mStats.mOpsCancelledCount++;
                if (inStatus == 0) {
                    theIt->second.Cancel();
                } else {
                    theIt->second.mOpPtr->status    = inStatus;
                    theIt->second.mOpPtr->statusMsg = inStatusMsg;
                    theIt->second.Done();
                }
            }
        }
        if (! thePendingEmptyFlag) {
            mQueueStack.erase(theIt);
        }
        return true;
    }
    int EventHandler(
        int   inCode,
        void* inDataPtr)
    {
        const int thePrefRefCount = GetRefCount();
        StRef theRef(*this);

        if (mEventObserverPtr && mEventObserverPtr->Event(inCode, inDataPtr)) {
            return 0;
        }

        const char*         theReasonPtr        = "network error";
        OpQueueEntry* const theOutstandingOpPtr = mOutstandingOpPtr;
        switch (inCode) {
            case EVENT_NET_READ: {
                    assert(inDataPtr && mConnPtr &&
                        &mConnPtr->GetInBuffer() == inDataPtr);
                    IOBuffer& theBuffer = mConnPtr->GetInBuffer();
                    mDataReceivedFlag = mDataReceivedFlag ||
                        (! theBuffer.IsEmpty() && ! IsAuthInFlight());
                    HandleResponse(theBuffer);
                }
                break;

            case EVENT_NET_WROTE:
                if (mConnPtr) {
                    const int theRem = mConnPtr->GetOutBuffer().BytesConsumable();
                    if (theRem < mPendingBytesSend) {
                        mStats.mBytesSentCount += mPendingBytesSend - theRem;
                    }
                    mPendingBytesSend = theRem;
                }
                assert(inDataPtr && mConnPtr);
                mDataSentFlag = mDataSentFlag || ! IsAuthInFlight();
                break;

            case EVENT_INACTIVITY_TIMEOUT:
                if (! mIdleTimeoutFlag &&
                        IsConnected() && mPendingOpQueue.empty()) {
                    mConnPtr->SetInactivityTimeout(mIdleTimeoutSec);
                    mIdleTimeoutFlag = true;
                    break;
                }
                theReasonPtr = "inactivity timeout";
                // Fall through.
            case EVENT_NET_ERROR:
                if (mConnPtr) {
                    if (mSslShutdownInProgressFlag && mConnPtr->IsGood()) {
                        KFS_LOG_STREAM(mConnPtr->GetFilter() ?
                                MsgLogger::kLogLevelERROR :
                                MsgLogger::kLogLevelDEBUG) << mLogPrefix <<
                            "ssl shutdown completion:"
                            " filter: " << reinterpret_cast<const void*>(
                                mConnPtr->GetFilter()) <<
                        KFS_LOG_EOM;
                        mSslShutdownInProgressFlag = false;
                        if (! mConnPtr->GetFilter()) {
                            mConnPtr->StartFlush();
                            break;
                        }
                    }
                    if (mAuthContextPtr && mConnPtr->IsAuthFailure()) {
                        mAuthFailureCount++;
                    } else {
                        mAuthFailureCount = 0;
                    }
                    mAllDataSentFlag = ! mConnPtr->IsWriteReady();
                    KFS_LOG_STREAM(mPendingOpQueue.empty() ?
                            MsgLogger::kLogLevelDEBUG :
                            MsgLogger::kLogLevelERROR) << mLogPrefix <<
                        "closing connection: " << mConnPtr->GetSockName() <<
                        " to: "            << mServerLocation <<
                        " due to "         << theReasonPtr <<
                        " pending:"
                        " read: "          << mConnPtr->GetNumBytesToRead() <<
                        " write: "         << mConnPtr->GetNumBytesToWrite() <<
                        " ops: "           << mPendingOpQueue.size() <<
                        " auth failures: " << mAuthFailureCount <<
                        " error: "         << mConnPtr->GetErrorMsg() <<
                    KFS_LOG_EOM;
                    Reset();
                }
                if (mIdleTimeoutFlag) {
                    mStats.mConnectionIdleTimeoutCount++;
                } else if (mPendingOpQueue.empty()) {
                    break;
                } else if (mDataSentFlag || mDataReceivedFlag) {
                    mStats.mNetErrorCount++;
                    if (inCode == EVENT_INACTIVITY_TIMEOUT) {
                        mStats.mResponseTimeoutCount++;
                    }
                } else {
                    mStats.mConnectFailureCount++;
                }
                if (! mPendingOpQueue.empty()) {
                    RetryConnect(theOutstandingOpPtr);
                }
                break;

            default:
                assert(!"Unknown event");
                break;
        }
        if (thePrefRefCount <= GetRefCount()) {
            OpsTimeout();
        }
        return 0;
    }
    void SetEventObserver(
        EventObserver* inEventObserverPtr)
        { mEventObserverPtr = inEventObserverPtr; }
    time_t Now() const
        { return mNetManager.Now(); }
    NetManager& GetNetManager() const
        { return mNetManager; }
    void SetMaxContentLength(
        int inMax)
        { mMaxContentLength = inMax; }
    void SetMaxRpcHeaderLength(
        int inMaxLength)
        { mMaxRpcHeaderLength = inMaxLength; }
    void ClearMaxOneOutstandingOpFlag()
    {
        if (! mMaxOneOutstandingOpFlag) {
            return;
        }
        mMaxOneOutstandingOpFlag = false;
        OpQueueEntry* const theOutstandingOpPtr = mOutstandingOpPtr;
        mOutstandingOpPtr = 0;
        bool theResetTimerFlag = ! mOutstandingOpPtr;
        for (OpQueue::iterator theIt = mPendingOpQueue.begin();
                theIt != mPendingOpQueue.end() && IsConnected();
                ++theIt) {
            OpQueueEntry& theEntry = theIt->second;
            if (&theEntry != theOutstandingOpPtr) {
                Request(theEntry, theResetTimerFlag, theEntry.mRetryCount);
                theResetTimerFlag = false;
            }
        }
    }
    void SetFailAllOpsOnOpTimeoutFlag(
        bool inFlag)
    {
        mFailAllOpsOnOpTimeoutFlag = inFlag;
    }
    virtual void OpDone(
        KfsOp*    inOpPtr,
        bool      inCanceledFlag,
        IOBuffer* inBufferPtr)
    {
        assert(! inBufferPtr && ! mOutstandingOpPtr &&
            (inOpPtr == &mLookupOp || inOpPtr == &mAuthOp));
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            (inCanceledFlag ? "op canceled: " : "op done: ") <<
            inOpPtr->Show() <<
            " now: "    << Now() <<
            " status: " << inOpPtr->status <<
            " msg: "    << inOpPtr->statusMsg <<
        KFS_LOG_EOM;
        const kfsSeq_t theSeq = inOpPtr->seq;
        inOpPtr->seq = -1;
        if (inCanceledFlag) {
            return;
        }
        if (inOpPtr->status == 0 && ! IsConnected()) {
            if (! mPendingOpQueue.empty()) {
                EnsureConnected();
            }
            return;
        }
        if (inOpPtr == &mLookupOp) {
            if ((mLookupOp.status == 0 || mLookupOp.status == -EACCES) &&
                    mLookupOp.authType == kAuthenticationTypeUndef) {
                // Does not support or understand authentication.
                if (! IsAuthEnabled()) {
                    return;
                }
                // Reset the status -- use auth type.
                mLookupOp.status = 0;
            }
            bool theDoAuthFlag = true;
            if (mLookupOp.status != 0 ||
                    (mAuthContextPtr && (mLookupOp.status =
                        mAuthContextPtr->CheckAuthType(
                            mLookupOp.authType,
                            theDoAuthFlag,
                            &mLookupOp.statusMsg)) != 0)) {
                KFS_LOG_STREAM_ERROR << mLogPrefix <<
                    "authentication negotiation failure: " <<
                        mLookupOp.statusMsg <<
                KFS_LOG_EOM;
                Fail(mLookupOp.status, mLookupOp.statusMsg);
                return;
            }
            if (! theDoAuthFlag) {
                KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                    "no auth. supported and/or required"
                    " auth. type: " << showbase << hex << mLookupOp.authType <<
                KFS_LOG_EOM;
                SubmitPending();
                return;
            }
            if (mAuthContextPtr) {
                assert(mAuthOp.seq < 0);
                const char* theBufPtr = 0;
                int         theBufLen = 0;
                mAuthOp.statusMsg.clear();
                if ((mAuthOp.status = mAuthContextPtr->Request(
                        mLookupOp.authType,
                        mAuthOp.requestedAuthType,
                        theBufPtr,
                        theBufLen,
                        mAuthRequestCtx,
                        &mAuthOp.statusMsg)) != 0) {
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "authentication request failure: " <<
                            mAuthOp.status <<
                        " " << mAuthOp.statusMsg <<
                    KFS_LOG_EOM;
                    Fail(mAuthOp.status, mAuthOp.statusMsg);
                    return;
                }
                const bool kOwnsContentBufFlag = false;
                mAuthOp.AttachContentBuf(
                    theBufPtr, theBufLen, kOwnsContentBufFlag);
                mAuthOp.contentLength = theBufLen;
                mAuthOp.seq           = theSeq + 1;
                EnqueueAuth(mAuthOp);
                return;
            }
        }
        if (inOpPtr != &mAuthOp) {
            KFS_LOG_STREAM_FATAL << "invalid op completion: " <<
                inOpPtr->Show() <<
            KFS_LOG_EOM;
            MsgLogger::Stop();
            abort();
            return;
        }
        if (! IsAuthEnabled() || ! mConnPtr) {
            Reset();
            if (! mPendingOpQueue.empty()) {
                EnsureConnected();
            }
            return;
        }
        if (mAuthOp.status == 0) {
            if ((mAuthOp.status = mAuthContextPtr->Response(
                    mAuthOp.chosenAuthType,
                    mAuthOp.useSslFlag,
                    mAuthOp.contentBuf,
                    mAuthOp.contentLength,
                    *mConnPtr,
                    mAuthRequestCtx,
                    &mAuthOp.statusMsg)) == 0 &&
                    IsConnected()) {
                const int kMaxAuthResponseTimeSec = 4;
                bool      theUseEndTimeFlag       = false;
                int64_t   theEndTime              = 0;
                if (mAuthOp.chosenAuthType == kAuthenticationTypePSK) {
                    theUseEndTimeFlag = ParseTokenExpirationTime(
                            mAuthContextPtr->GetPskId(), theEndTime);
                } else if (mAuthOp.chosenAuthType == kAuthenticationTypeX509) {
                    theUseEndTimeFlag =
                        mAuthContextPtr->GetX509EndTime(theEndTime);
                }
                if (mAuthOp.currentTime < mAuthOp.sessionEndTime) {
                    mSessionExpirationTime = Now() - kMaxAuthResponseTimeSec -
                        mAuthOp.currentTime + mAuthOp.sessionEndTime;
                    if (theUseEndTimeFlag) {
                        mSessionExpirationTime =
                            min(mSessionExpirationTime, theEndTime);
                    }
                } else if (theUseEndTimeFlag) {
                    mSessionExpirationTime = theEndTime;
                }
                mSessionExpirationTime = max(
                    mSessionExpirationTime,
                    Now() + kSessionUpdateResolutionSec + 4
                );
                mRetryCount = mNonAuthRetryCount;
                mNonAuthRetryCount = 0;
                SubmitPending();
                return;
            }
            if (mAuthOp.status == -EAGAIN) {
                EnsureConnected(); // Retry authentication.
                return;
            }
        }
        KFS_LOG_STREAM_ERROR << mLogPrefix <<
            "authentication response failure:"
            " seq: " << theSeq <<
            " "      << mAuthOp.status <<
            " "      << mAuthOp.statusMsg <<
            " "      << mAuthOp.Show() <<
        KFS_LOG_EOM;
        Fail(mAuthOp.status, mAuthOp.statusMsg);
    }
private:
    class DoNotDeallocate
    {
    public:
        DoNotDeallocate()
            {}
        void operator()(
            char* /* inBufferPtr */)
            {}
    };
    struct OpQueueEntry
    {
        OpQueueEntry(
            KfsOp*    inOpPtr     = 0,
            OpOwner*  inOwnerPtr  = 0,
            IOBuffer* inBufferPtr = 0)
            : mOpPtr(inOpPtr),
              mOwnerPtr(inOwnerPtr),
              mBufferPtr(inBufferPtr),
              mTime(0),
              mRetryCount(0)
            {}
        void Cancel()
            { OpDone(true); }
        void Done()
            { OpDone(false); }
        void OpDone(
            bool inCanceledFlag)
        {
            KfsOp*    const theOpPtr     = mOpPtr;
            OpOwner*  const theOwnerPtr  = mOwnerPtr;
            IOBuffer* const theBufferPtr = mBufferPtr;
            Clear();
            if (theOwnerPtr) {
                if (theOpPtr) {
                    theOwnerPtr->OpDone(theOpPtr, inCanceledFlag, theBufferPtr);
                }
            } else {
                delete theOpPtr;
                delete theBufferPtr;
            }
        }
        void Clear()
        {
            mOpPtr     = 0;
            mOwnerPtr  = 0;
            mBufferPtr = 0;
        }
        KfsOp*    mOpPtr;
        OpOwner*  mOwnerPtr;
        IOBuffer* mBufferPtr;
        time_t    mTime;
        int       mRetryCount;
    };
    typedef map<kfsSeq_t, OpQueueEntry, less<kfsSeq_t>,
        StdFastAllocator<pair<const kfsSeq_t, OpQueueEntry> >
    > OpQueue;
    typedef list<OpQueue,
        StdFastAllocator<OpQueue>
    > QueueStack;
    enum { kMaxReadAhead = 4 << 10 };
    typedef ClientAuthContext::RequestCtx AuthRequestCtx;

    ServerLocation     mServerLocation;
    OpQueue            mPendingOpQueue;
    QueueStack         mQueueStack;
    NetConnectionPtr   mConnPtr;
    kfsSeq_t           mNextSeqNum;
    bool               mReadHeaderDoneFlag;
    bool               mSleepingFlag;
    bool               mDataReceivedFlag;
    bool               mDataSentFlag;
    bool               mAllDataSentFlag;
    bool               mRetryConnectOnlyFlag;
    bool               mIdleTimeoutFlag;
    bool               mPrevAuthFailureFlag;
    bool               mResetConnectionOnOpTimeoutFlag;
    bool               mFailAllOpsOnOpTimeoutFlag;
    bool               mMaxOneOutstandingOpFlag;
    bool               mShutdownSslFlag;
    bool               mSslShutdownInProgressFlag;
    int                mTimeSecBetweenRetries;
    int                mOpTimeoutSec;
    int                mIdleTimeoutSec;
    int                mRetryCount;
    int                mNonAuthRetryCount;
    int                mContentLength;
    int                mMaxRetryCount;
    int                mMaxContentLength;
    int                mAuthFailureCount;
    int                mMaxRpcHeaderLength;
    int                mPendingBytesSend;
    OpQueueEntry*      mInFlightOpPtr;
    OpQueueEntry*      mOutstandingOpPtr;
    char*              mInFlightRecvBufPtr;
    OpQueue::iterator  mCurOpIt;
    IOBuffer::IStream  mIstream;
    IOBuffer::WOStream mOstream;
    Properties         mProperties;
    Stats              mStats;
    int64_t            mDisconnectCount;
    EventObserver*     mEventObserverPtr;
    const string       mLogPrefix;
    NetManager&        mNetManager;
    ClientAuthContext* mAuthContextPtr;
    int64_t            mSessionExpirationTime;
    int64_t            mKeyExpirationTime;
    string             mKeyId;
    string             mKeyData;
    string             mSessionKeyId;
    string             mSessionKeyData;
    AuthRequestCtx     mAuthRequestCtx;
    LookupOp           mLookupOp;
    AuthenticateOp     mAuthOp;

    virtual ~Impl()
        { Impl::Reset(); }
    bool IsAuthInFlight()
        { return (0 <= mLookupOp.seq || 0 <= mAuthOp.seq); }
    void SetMaxWaitTime(
        KfsOp& inOp)
    {
        inOp.maxWaitMillisec = mOpTimeoutSec > 0 ?
            int64_t(mOpTimeoutSec) * 1000 : int64_t(-1);
    }
    bool EnqueueSelf(
        KfsOp*    inOpPtr,
        OpOwner*  inOwnerPtr,
        IOBuffer* inBufferPtr,
        int       inRetryCount)
    {
        if (! inOpPtr) {
            return false;
        }
        mIdleTimeoutFlag = false;
        SetMaxWaitTime(*inOpPtr);
        inOpPtr->seq = mNextSeqNum++;
        const bool theResetTimerFlag = mPendingOpQueue.empty();
        pair<OpQueue::iterator, bool> const theRes =
            mPendingOpQueue.insert(make_pair(
                inOpPtr->seq, OpQueueEntry(inOpPtr, inOwnerPtr, inBufferPtr)
            ));
        if (! theRes.second || ! IsConnected() || IsAuthInFlight()) {
            return theRes.second;
        }
        if (mMaxOneOutstandingOpFlag) {
            if (mOutstandingOpPtr) {
                return theRes.second;
            }
            mOutstandingOpPtr = &(mPendingOpQueue.begin()->second);
        }
        Request(mOutstandingOpPtr ? *mOutstandingOpPtr : theRes.first->second,
            theResetTimerFlag || mOutstandingOpPtr, inRetryCount);
        return theRes.second;
    }
    void EnqueueAuth(
        KfsOp& inOp)
    {
        assert(! mOutstandingOpPtr && mConnPtr && ! mConnPtr->IsWriteReady());
        SetMaxWaitTime(inOp);
        pair<OpQueue::iterator, bool> const theRes =
            mPendingOpQueue.insert(make_pair(
                inOp.seq, OpQueueEntry(&inOp, this, 0)
            ));
        if (! theRes.second || theRes.first != mPendingOpQueue.begin()) {
            KFS_LOG_STREAM_FATAL << "invalid auth. enqueue attempt:" <<
                " duplicate seq. number: " << theRes.second <<
            KFS_LOG_EOM;
            MsgLogger::Stop();
            abort();
        }
        const bool kResetTimerFlag = true;
        Request(theRes.first->second, kResetTimerFlag, mRetryCount);
    }
    void Request(
        OpQueueEntry& inEntry,
        bool          inResetTimerFlag,
        int           inRetryCount,
        bool          inFlushFlag = true)
    {
        KfsOp& theOp = *inEntry.mOpPtr;
        theOp.Request(mOstream.Set(mConnPtr->GetOutBuffer()));
        mOstream.Reset();
        if (theOp.contentLength > 0) {
            if (theOp.contentBuf && theOp.contentBufLen > 0) {
                assert(theOp.contentBufLen >= theOp.contentLength);
                mConnPtr->Write(theOp.contentBuf, theOp.contentLength,
                    inResetTimerFlag);
            } else if (inEntry.mBufferPtr) {
                assert(size_t(inEntry.mBufferPtr->BytesConsumable()) >=
                    theOp.contentLength);
                mConnPtr->WriteCopy(inEntry.mBufferPtr, theOp.contentLength,
                    inResetTimerFlag);
            }
        }
        while (theOp.NextRequest(
                mNextSeqNum,
                mOstream.Set(mConnPtr->GetOutBuffer()))) {
            mNextSeqNum++;
            mOstream.Reset();
        }
        mOstream.Reset();
        // Start the timer.
        inEntry.mTime       = Now();
        inEntry.mRetryCount = inRetryCount;
        mPendingBytesSend = mConnPtr->GetOutBuffer().BytesConsumable();
        if (inFlushFlag) {
            mConnPtr->SetInactivityTimeout(mOpTimeoutSec);
            mConnPtr->Flush(inResetTimerFlag);
        }
    }
    void SubmitPending()
    {
        const bool kFlushFlag             = false;
        bool       theFlushConnectionFlag = false;
        for (OpQueue::iterator theIt = mPendingOpQueue.begin();
                ! mOutstandingOpPtr && theIt != mPendingOpQueue.end();
                ++theIt) {
            if (mMaxOneOutstandingOpFlag) {
                mOutstandingOpPtr = &(theIt->second);
            }
            Request(theIt->second, kFlushFlag,
                theIt->second.mRetryCount, kFlushFlag);
            theFlushConnectionFlag = true;
        }
        if (theFlushConnectionFlag) {
            mConnPtr->SetInactivityTimeout(mOpTimeoutSec);
            mConnPtr->Flush();
        }
    }
    void HandleResponse(
       IOBuffer& inBuffer)
    {
        for (; ;) {
            if (! mReadHeaderDoneFlag && ! ReadHeader(inBuffer)) {
                break;
            }
            if (mContentLength > inBuffer.BytesConsumable()) {
                if (! mInFlightOpPtr) {
                    // Discard content.
                    const int theCount = inBuffer.Consume(mContentLength);
                    mContentLength -= theCount;
                    mStats.mBytesReceivedCount += theCount;
                }
                if (mConnPtr) {
                    mConnPtr->SetMaxReadAhead(max(int(kMaxReadAhead),
                        mContentLength - inBuffer.BytesConsumable()));
                }
                break;
            }
            // Get ready for next op.
            if (mConnPtr) {
                mConnPtr->SetMaxReadAhead(kMaxReadAhead);
            }
            mReadHeaderDoneFlag = false;
            if (! mInFlightOpPtr) {
                mStats.mBytesReceivedCount += inBuffer.Consume(mContentLength);
                mContentLength = 0;
                mProperties.clear();
                // Don't rely on compiler to properly handle tail recursion,
                // use for loop instead.
                continue;
            }
            assert(&mCurOpIt->second == mInFlightOpPtr);
            assert(mInFlightOpPtr->mOpPtr);
            KfsOp&          theOp     = *mInFlightOpPtr->mOpPtr;
            IOBuffer* const theBufPtr = mInFlightOpPtr->mBufferPtr;
            mInFlightOpPtr = 0;
            theOp.ParseResponseHeader(mProperties);
            mProperties.clear();
            if (mContentLength > 0) {
                mStats.mBytesReceivedCount +=
                    min(mContentLength, inBuffer.BytesConsumable());
                if (theBufPtr) {
                    IOBuffer theBuf;
                    theBuf.MoveSpaceAvailable(theBufPtr, mContentLength);
                    theBuf.Clear();
                    const int kMaxInt = ~(int(1) << (sizeof(int) * 8 - 1));
                    theBuf.MoveSpaceAvailable(theBufPtr, kMaxInt);
                    QCVERIFY(mContentLength ==
                        theBufPtr->MoveSpace(&inBuffer, mContentLength)
                    );
                    theBufPtr->Move(&theBuf);
                } else {
                    IOBuffer theBuf;
                    QCVERIFY(mContentLength ==
                        theBuf.MoveSpace(&inBuffer, mContentLength)
                    );
                    if (mInFlightRecvBufPtr) {
                        theOp.AttachContentBuf(
                            mInFlightRecvBufPtr, mContentLength);
                        mInFlightRecvBufPtr = 0;
                    }
                }
                mContentLength = 0;
            }
            mRetryCount = 0;
            HandleOp(mCurOpIt);
        }
    }
    bool ReadHeader(
        IOBuffer& inBuffer)
    {
        const int theIdx = inBuffer.IndexOf(0, "\r\n\r\n");
        if (theIdx < 0) {
            if (mMaxRpcHeaderLength < inBuffer.BytesConsumable()) {
               KFS_LOG_STREAM_ERROR << mLogPrefix <<
                    "error: " << mServerLocation <<
                    ": exceeded max. response header size: " <<
                    mMaxRpcHeaderLength << "; got " <<
                    inBuffer.BytesConsumable() << " resetting connection" <<
                KFS_LOG_EOM;
                Reset();
                EnsureConnected();
            }
            return false;
        }
        const int  theHdrLen    = theIdx + 4;
        const char theSeparator = ':';
        mProperties.clear();
        IOBuffer::iterator const theIt = inBuffer.begin();
        if (theIt != inBuffer.end() && theHdrLen <= theIt->BytesConsumable()) {
            mProperties.loadProperties(
                theIt->Consumer(), (size_t)theHdrLen, theSeparator);
        } else {
            mProperties.loadProperties(
                mIstream.Set(inBuffer, theHdrLen), theSeparator);
            mIstream.Reset();
        }
        mStats.mBytesReceivedCount += inBuffer.Consume(theHdrLen);
        mReadHeaderDoneFlag = true;
        mContentLength = mProperties.getValue("Content-length", 0);
        const kfsSeq_t theOpSeq = mProperties.getValue("Cseq", kfsSeq_t(-1));
        if (mContentLength > mMaxContentLength) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "error: " << mServerLocation <<
                ": exceeded max. response content length: " << mContentLength <<
                " > " << mMaxContentLength <<
                " seq: " << theOpSeq <<
                ", resetting connection" <<
            KFS_LOG_EOM;
            Reset();
            EnsureConnected();
            return false;
        }
        mCurOpIt = mPendingOpQueue.find(theOpSeq);
        mInFlightOpPtr =
            mCurOpIt != mPendingOpQueue.end() ? &mCurOpIt->second : 0;
        if (! mInFlightOpPtr) {
            // Discard canceled op reply.
            KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                "no operation found with seq: " << theOpSeq <<
                ", discarding response " <<
                " content length: " << mContentLength <<
            KFS_LOG_EOM;
            const int theCount = inBuffer.Consume(mContentLength);
            mContentLength -= theCount;
            mStats.mBytesReceivedCount += theCount;
            return true;
        }
        if (mOutstandingOpPtr && mOutstandingOpPtr != mInFlightOpPtr) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "error: "     << mServerLocation <<
                " seq: "      << theOpSeq <<
                " op:"
                " expected: " << static_cast<const void*>(mOutstandingOpPtr) <<
                " actual: "   << static_cast<const void*>(mInFlightOpPtr) <<
                " seq:" <<
                " expected: " << mOutstandingOpPtr->mOpPtr->seq <<
                " actual: "   << mInFlightOpPtr->mOpPtr->seq <<
                ", resetting connection" <<
            KFS_LOG_EOM;
            Reset();
            EnsureConnected();
            return false;
        }
        if (mContentLength <= 0) {
            return true;
        }
        if (mInFlightOpPtr->mBufferPtr) {
            if (inBuffer.UseSpaceAvailable(
                    mInFlightOpPtr->mBufferPtr, mContentLength) <= 0) {
                // Move the payload, if any, to the beginning of the new buffer.
                inBuffer.MakeBuffersFull();
            }
            return true;
        }
        KfsOp& theOp = *mInFlightOpPtr->mOpPtr;
        assert(! mInFlightRecvBufPtr);
        if (theOp.contentLength <= 0) {
            theOp.EnsureCapacity(size_t(mContentLength));
        } else {
            mInFlightRecvBufPtr = new char [mContentLength + 1];
            mInFlightRecvBufPtr[mContentLength] = 0;
        }
        IOBuffer theBuf;
        theBuf.Append(IOBufferData(
            IOBufferData::IOBufferBlockPtr(
                mInFlightRecvBufPtr ? mInFlightRecvBufPtr : theOp.contentBuf,
                DoNotDeallocate()
            ),
            mContentLength,
            0,
            0
        ));
        inBuffer.UseSpaceAvailable(&theBuf, mContentLength);
        return true;
    }
    void CancelInFlightOp()
    {
        if (! mInFlightOpPtr) {
            return;
        }
        if (0 < mContentLength && mConnPtr) {
            // Detach shared buffers, if any.
            IOBuffer& theBuf = mConnPtr->GetInBuffer();
            mContentLength -= theBuf.BytesConsumable();
            assert(0 <= mContentLength);
            theBuf.Clear();
        }
        delete [] mInFlightRecvBufPtr;
        mInFlightRecvBufPtr = 0;
        mInFlightOpPtr = 0;
    }
    bool IsAuthEnabled() const
        { return (mAuthContextPtr && mAuthContextPtr->IsEnabled()); }
    bool IsPskAuth() const
        { return (IsAuthEnabled() && ! mKeyData.empty()); }
    void EnsureConnected(
        string*      inErrMsgPtr = 0,
        const KfsOp* inLastOpPtr = 0)
    {
        if (mSleepingFlag || IsConnected()) {
            return;
        }
        assert(mLookupOp.seq < 0 && mAuthOp.seq < 0);
        mDataReceivedFlag = false;
        mDataSentFlag     = false;
        mAllDataSentFlag  = true;
        mIdleTimeoutFlag  = false;
        ResetConnection();
        mStats.mConnectCount++;
        const bool theNonBlockingFlag = true;
        TcpSocket& theSocket          = *(new TcpSocket());
        const int theErr              = theSocket.Connect(
            mServerLocation, theNonBlockingFlag);
        if (theErr && theErr != -EINPROGRESS) {
            if (inErrMsgPtr) {
                *inErrMsgPtr = QCUtils::SysError(-theErr);
            }
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "failed to connect to server " << mServerLocation <<
                " : " << QCUtils::SysError(-theErr) <<
            KFS_LOG_EOM;
            delete &theSocket;
            mStats.mConnectFailureCount++;
            RetryConnect();
            return;
        }
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "connecting to server: " << mServerLocation <<
            " auth: " << (IsAuthEnabled() ?
                (IsPskAuth() ? "psk" : "on") : "off") <<
        KFS_LOG_EOM;
        mConnPtr.reset(new NetConnection(&theSocket, this));
        mConnPtr->EnableReadIfOverloaded();
        if (theErr) {
            mConnPtr->SetDoingNonblockingConnect();
        }
        mConnPtr->SetMaxReadAhead(kMaxReadAhead);
        mConnPtr->SetInactivityTimeout(mOpTimeoutSec);
        // Add connection to the poll vector
        mNetManager.AddConnection(mConnPtr);
        mSslShutdownInProgressFlag = false;
        if (IsPskAuth()) {
            KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                "psk key:"
                " size: " << mKeyData.size() <<
                " id: "   << mKeyId <<
            KFS_LOG_EOM;
            string    theErrMsg;
            const int theStatus = mAuthContextPtr->StartSsl(
                *mConnPtr,
                mKeyId.c_str(),
                mKeyData.data(),
                (int)mKeyData.size(),
                &theErrMsg
            );
            if (theStatus != 0) {
                KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                    "failed to start ssl:"
                    " error: " << theStatus <<
                    " "        << theErrMsg <<
                KFS_LOG_EOM;
                Fail(theStatus, theErrMsg);
                if (inErrMsgPtr) {
                    *inErrMsgPtr = theErrMsg;
                }
                return;
            }
            if (mShutdownSslFlag && mConnPtr->IsGood()) {
                mSslShutdownInProgressFlag = true;
                const int theStatus = mConnPtr->Shutdown();
                if (theStatus != 0) {
                    mSslShutdownInProgressFlag = false;
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "ssl shutdown failure: " <<
                            theErr <<
                    KFS_LOG_EOM;
                    // Assume communication failure.
                    mStats.mConnectFailureCount++;
                    RetryConnect();
                    return;
                }
            }
            mSessionExpirationTime = mKeyExpirationTime;
            mSessionKeyId          = mKeyId;
            mSessionKeyData        = mKeyData;
            mNonAuthRetryCount     = 0;
        } else {
            mSessionKeyId          = string();
            mSessionKeyData        = mSessionKeyId;
            mSessionExpirationTime = Now() + kMaxSessionTimeout;
            if (IsAuthEnabled()) {
                assert(! IsAuthInFlight());
                mLookupOp.DeallocContentBuf();
                mLookupOp.contentLength = 0;
                mLookupOp.status        = 0;
                mLookupOp.statusMsg.clear();
                mLookupOp.authType      = kAuthenticationTypeNone;
                mLookupOp.seq           = mNextSeqNum++;
                mNextSeqNum++; // Leave one slot for mAuthOp
            } else {
                mNonAuthRetryCount = 0;
            }
        }
        const kfsSeq_t theLookupSeq = mLookupOp.seq;
        RetryAll(inLastOpPtr);
        if (! mConnPtr) {
            ResetConnection();
        } else if (0 <= theLookupSeq && theLookupSeq == mLookupOp.seq) {
            EnqueueAuth(mLookupOp);
        }
    }
    void RetryAll(
        const KfsOp* inLastOpPtr = 0)
    {
        if (mPendingOpQueue.empty()) {
            return;
        }
        CancelInFlightOp();
        mNextSeqNum += 1000; // For debugging to see retries.
        QueueStack::iterator const theIt =
            mQueueStack.insert(mQueueStack.end(), OpQueue());
        OpQueue& theQueue = *theIt;
        theQueue.swap(mPendingOpQueue);
        for (OpQueue::iterator theIt = theQueue.begin();
                theIt != theQueue.end();
                ++theIt) {
            OpQueueEntry& theEntry = theIt->second;
            if (! theEntry.mOpPtr) {
                continue;
            }
            if (theEntry.mRetryCount > mMaxRetryCount) {
                mStats.mOpsTimeoutCount++;
                theEntry.mOpPtr->status = kErrorMaxRetryReached;
                theEntry.Done();
            } else {
                if (inLastOpPtr != theEntry.mOpPtr &&
                        theEntry.mRetryCount > 0) {
                    mStats.mOpsRetriedCount++;
                }
                EnqueueSelf(theEntry.mOpPtr, theEntry.mOwnerPtr,
                    theEntry.mBufferPtr, theEntry.mRetryCount);
                theEntry.Clear();
            }
        }
        mQueueStack.erase(theIt);
    }
    void ResetConnection()
    {
        CancelInFlightOp();
        mOutstandingOpPtr = 0;
        if (mConnPtr) {
            mConnPtr->Close();
            mConnPtr->GetInBuffer().Clear();
            mConnPtr->SetOwningKfsCallbackObj(0);
            mConnPtr.reset();
            mDisconnectCount++;
        }
        if (0 <= mLookupOp.seq) {
            Cancel(&mLookupOp, this);
            mLookupOp.seq = -1;
        }
        if (0 <= mAuthOp.seq) {
            Cancel(&mAuthOp, this);
            mAuthOp.seq = -1;
        }
        mReadHeaderDoneFlag        = false;
        mContentLength             = 0;
        mSslShutdownInProgressFlag = false;
    }
    void HandleOp(
        OpQueue::iterator inIt,
        bool              inCanceledFlag = false)
    {
        if (inCanceledFlag) {
            mStats.mOpsCancelledCount++;
        }
        const bool theScheduleNextOpFlag = mOutstandingOpPtr == &inIt->second;
        if (theScheduleNextOpFlag) {
            mOutstandingOpPtr = 0;
        }
        if (&inIt->second == mInFlightOpPtr) {
            CancelInFlightOp();
        }
        OpQueueEntry theOpEntry = inIt->second;
        mPendingOpQueue.erase(inIt);
        const int thePrefRefCount = GetRefCount();
        theOpEntry.OpDone(inCanceledFlag);
        if (! mOutstandingOpPtr &&
                theScheduleNextOpFlag && thePrefRefCount <= GetRefCount() &&
                ! mPendingOpQueue.empty() && IsConnected()) {
            mOutstandingOpPtr = &(mPendingOpQueue.begin()->second);
            const bool kResetTimerFlag = true;
            Request(*mOutstandingOpPtr, kResetTimerFlag,
                mOutstandingOpPtr->mRetryCount);
        }
    }
    void Cancel(
        OpQueue::iterator inIt)
        { HandleOp(inIt, true); }
    virtual void Timeout()
    {
        if (mSleepingFlag) {
            mNetManager.UnRegisterTimeoutHandler(this);
            mSleepingFlag = false;
        }
        if (mPendingOpQueue.empty()) {
            return;
        }
        EnsureConnected();
    }
    void RetryConnect(
        OpQueueEntry* inOutstandingOpPtr = 0)
    {
        if (mSleepingFlag) {
            return;
        }
        CancelInFlightOp();
        if (mRetryCount < mMaxRetryCount &&
                (! mAuthContextPtr ||
                mAuthFailureCount < mAuthContextPtr->GetMaxAuthRetryCount()) &&
                (! mRetryConnectOnlyFlag ||
                (! mDataSentFlag && ! mDataReceivedFlag) ||
                IsAuthInFlight())) {
            mRetryCount++;
            if (! IsAuthInFlight()) {
                mNonAuthRetryCount = mRetryCount;
            }
            ResetConnection();
            if (mTimeSecBetweenRetries > 0) {
                KFS_LOG_STREAM_INFO << mLogPrefix <<
                    "retry attempt " << mRetryCount <<
                    " of " << mMaxRetryCount <<
                    ", will retry " << mPendingOpQueue.size() <<
                    " pending operation(s) in " << mTimeSecBetweenRetries <<
                    " seconds" <<
                KFS_LOG_EOM;
                mStats.mSleepTimeSec += mTimeSecBetweenRetries;
                SetTimeoutInterval(mTimeSecBetweenRetries * 1000, true);
                mSleepingFlag = true;
                mNetManager.RegisterTimeoutHandler(this);
            } else {
                Timeout();
            }
        } else if (IsAuthInFlight()) {
            OpQueue::iterator const theIt = mPendingOpQueue.begin();
            assert(
                theIt != mPendingOpQueue.end() &&
                theIt->second.mOpPtr ==
                    (0 <= mLookupOp.seq ?
                        static_cast<KfsOp*>(&mLookupOp) :
                        static_cast<KfsOp*>(&mAuthOp))
            );
            const bool kAllowRetryFlag = false;
            HandleSingleOpTimeout(theIt, kAllowRetryFlag);
        } else if (inOutstandingOpPtr && ! mFailAllOpsOnOpTimeoutFlag &&
                ! mPendingOpQueue.empty() &&
                &(mPendingOpQueue.begin()->second) == inOutstandingOpPtr) {
            const bool kAllowRetryFlag = true;
            HandleSingleOpTimeout(mPendingOpQueue.begin(), kAllowRetryFlag,
                mAuthFailureCount ? -EPERM : kErrorMaxRetryReached);
        } else {
            const int theStatus = 0 < mAuthFailureCount ?
                -EPERM : kErrorMaxRetryReached;
            QueueStack::iterator const theIt =
                mQueueStack.insert(mQueueStack.end(), OpQueue());
            OpQueue& theQueue = *theIt;
            theQueue.swap(mPendingOpQueue);
            for (OpQueue::iterator theIt = theQueue.begin();
                    theIt != theQueue.end();
                    ++theIt) {
                if (! theIt->second.mOpPtr) {
                    continue;
                }
                theIt->second.mOpPtr->status = theStatus;
                theIt->second.Done();
            }
            mQueueStack.erase(theIt);
        }
    }
    void HandleSingleOpTimeout(
        OpQueue::iterator inIt,
        bool              inAllowRetryFlag = true,
        int               inStatus         = kErrorMaxRetryReached)
    {
        OpQueueEntry& theEntry = inIt->second;
        if (inAllowRetryFlag && theEntry.mRetryCount < mMaxRetryCount) {
            theEntry.mRetryCount++;
        } else {
            theEntry.mOpPtr->status = inStatus;
            const int thePrefRefCount = GetRefCount();
            HandleOp(inIt);
            if (thePrefRefCount > GetRefCount()) {
                return;
            }
        }
        if (! mPendingOpQueue.empty()) {
            EnsureConnected();
        }
    }
    void OpsTimeout()
    {
        if (mOpTimeoutSec <= 0 || ! IsConnected()) {
            return;
        }
        const time_t theNow = Now();
        if (IsAuthInFlight()) {
            OpQueue::iterator const theIt = mPendingOpQueue.begin();
            assert(
                theIt != mPendingOpQueue.end() &&
                theIt->second.mOpPtr ==
                    (0 <= mLookupOp.seq ?
                        static_cast<KfsOp*>(&mLookupOp) :
                        static_cast<KfsOp*>(&mAuthOp))
            );
            if (theIt->second.mTime + mOpTimeoutSec < theNow) {
                const OpQueueEntry& theEntry = theIt->second;
                KFS_LOG_STREAM_INFO << mLogPrefix <<
                    "auth. op timed out:"
                    " seq: "               << theEntry.mOpPtr->seq <<
                    " "                    << theEntry.mOpPtr->Show() <<
                    " wait time: "         << (theNow - theEntry.mTime) <<
                    " pending ops: "       << mPendingOpQueue.size() <<
                    " resetting connecton" <<
                KFS_LOG_EOM;
                RetryConnect();
            }
            return;
        }
        // Timeout ops waiting for response.
        // The ops in the queue are ordered by op seq. number.
        // The code ensures (initial seq. number in ctor) that seq. numbers
        // never wrap around, and are monotonically increase, so that the last
        // (re)queued operation seq. number is always the largest.
        // First move all timed out ops into the temporary queue, then process
        // the temporary queue. This is less error prone than dealing with
        // completion changing mPendingOpQueue while iterating.
        QueueStack::iterator theStIt       = mQueueStack.end();
        time_t               theExpireTime = theNow - mOpTimeoutSec;
        const bool           theMaxOneOutstandingOpFlag =
            mMaxOneOutstandingOpFlag;
        for (OpQueue::iterator theIt = mPendingOpQueue.begin();
                theIt != mPendingOpQueue.end() &&
                theIt->second.mTime < theExpireTime; ) {
            OpQueueEntry& theEntry = theIt->second;
            assert(theEntry.mOpPtr);
            if (&theEntry == mInFlightOpPtr) {
                CancelInFlightOp();
            }
            if (mResetConnectionOnOpTimeoutFlag) {
                KFS_LOG_STREAM_INFO << mLogPrefix <<
                    "op timed out: seq: "  << theEntry.mOpPtr->seq <<
                    " "                    << theEntry.mOpPtr->Show() <<
                    " retry count: "       << theEntry.mRetryCount <<
                    " wait time: "         << (theNow - theEntry.mTime) <<
                    " pending ops: "       << mPendingOpQueue.size() <<
                    " resetting connecton" <<
                KFS_LOG_EOM;
                // Increment the retry count only for the op that timed out.
                // This mode assumes all other ops were blocked by the first
                // one.
                Reset();
                if (mFailAllOpsOnOpTimeoutFlag) {
                    // Fail all ops.
                    assert(! mOutstandingOpPtr && ! mInFlightOpPtr);
                    theStIt = mQueueStack.insert(mQueueStack.end(), OpQueue());
                    theStIt->swap(mPendingOpQueue);
                    break;
                }
                HandleSingleOpTimeout(theIt);
                return;
            }
            if (theStIt == mQueueStack.end()) {
                theStIt = mQueueStack.insert(mQueueStack.end(), OpQueue());
            }
            theStIt->insert(*theIt);
            mPendingOpQueue.erase(theIt++);
            if (&theEntry == mOutstandingOpPtr) {
                break;
            }
        }
        if (theStIt == mQueueStack.end()) {
            return;
        }
        int theStatus         = kErrorMaxRetryReached;
        int theRetryIncrement = 1;
        for (OpQueue::iterator theIt = theStIt->begin();
                theIt != theStIt->end();
                ++theIt) {
            const int theCurStatus = theStatus;
            if (theMaxOneOutstandingOpFlag) {
                theStatus         = kErrorRequeueRequired;
                theRetryIncrement = 0;
            }
            OpQueueEntry& theEntry = theIt->second;
            if (! theEntry.mOpPtr) {
                continue;
            }
            KFS_LOG_STREAM_INFO << mLogPrefix <<
                "op " << (theStatus == kErrorRequeueRequired ?
                    "re-queue" : "timed out") <<
                " seq: "              << theEntry.mOpPtr->seq <<
                " "                   << theEntry.mOpPtr->Show() <<
                " retry count: "      << theEntry.mRetryCount <<
                " max: "              << mMaxRetryCount <<
                " wait time: "        << (theNow - theEntry.mTime) <<
            KFS_LOG_EOM;
            if (theStatus != kErrorRequeueRequired) {
                mStats.mOpsTimeoutCount++;
            }
            if (theEntry.mRetryCount >= mMaxRetryCount) {
                theEntry.mOpPtr->status = theCurStatus;
                theEntry.Done();
            } else {
                mStats.mOpsRetriedCount += theRetryIncrement;
                const OpQueueEntry theTmp = theEntry;
                theEntry.Clear();
                EnqueueSelf(theTmp.mOpPtr, theTmp.mOwnerPtr,
                    theTmp.mBufferPtr, theTmp.mRetryCount + theRetryIncrement);
            }
        }
        mQueueStack.erase(theStIt);
        if (! mPendingOpQueue.empty()) {
            EnsureConnected();
        }
    }
    int64_t GetTokenExpirationTime(
            const string& inKeyId) const
    {
        int64_t theRet = 0;
        if (! ParseTokenExpirationTime(inKeyId, theRet)) {
            const int64_t kDefaultSessionExpirationTimeSec = 10 * 24 * 60 * 60;
            KFS_LOG_STREAM_INFO << mLogPrefix <<
                "failed to parse delegation token,"
                " setting expriation time to " <<
                    kDefaultSessionExpirationTimeSec << " sec." <<
            KFS_LOG_EOM;
            return Now() + kDefaultSessionExpirationTimeSec;
        }
        return theRet;
    }
    bool ParseTokenExpirationTime(
            const string& inKeyId,
            int64_t&      outTime) const
    {
        DelegationToken theToken;
        if (inKeyId.empty() ||
                ! theToken.FromString(inKeyId.data(), inKeyId.size(), 0, 0)) {
            return false;
        }
        outTime = theToken.GetIssuedTime() + theToken.GetValidForSec();
        return true;
    }
    friend class StImplRef;
private:
    Impl(
        const Impl& inClient);
    Impl& operator=(
        const Impl& inClient);
};

KfsNetClient::KfsNetClient(
        NetManager&        inNetManager,
        string             inHost                           /* = string() */,
        int                inPort                           /* = 0 */,
        int                inMaxRetryCount                  /* = 0 */,
        int                inTimeSecBetweenRetries          /* = 10 */,
        int                inOpTimeoutSec                   /* = 5  * 60 */,
        int                inIdleTimeoutSec                 /* = 30 * 60 */,
        int64_t            inInitialSeqNum                  /* = 1 */,
        const char*        inLogPrefixPtr                   /* = 0 */,
        bool               inResetConnectionOnOpTimeoutFlag /* = true */,
        int                inMaxContentLength               /* = MAX_RPC_HEADER_LEN */,
        bool               inFailAllOpsOnOpTimeoutFlag      /* = false */,
        bool               inMaxOneOutstandingOpFlag        /* = false */,
        ClientAuthContext* inAuthContextPtr                 /* = 0 */,
        const QCThread*    inThreadPtr                      /* = 0 */)
    : mImpl(*new Impl(
        inHost,
        inPort,
        inMaxRetryCount,
        inTimeSecBetweenRetries,
        inOpTimeoutSec,
        inIdleTimeoutSec,
        kfsSeq_t(inInitialSeqNum),
        inLogPrefixPtr,
        inNetManager,
        inResetConnectionOnOpTimeoutFlag,
        inMaxContentLength,
        inFailAllOpsOnOpTimeoutFlag,
        inMaxOneOutstandingOpFlag,
        inAuthContextPtr,
        inThreadPtr
    ))
{
    mImpl.Ref();
}

    /* virtual */
KfsNetClient::~KfsNetClient()
{
    mImpl.Stop();
    mImpl.UnRef();
}

    bool
KfsNetClient::IsConnected() const
{
    Impl::StRef theRef(mImpl);
    return mImpl.IsConnected();
}

    int64_t
KfsNetClient::GetDisconnectCount() const
{
    Impl::StRef theRef(mImpl);
    return mImpl.GetDisconnectCount();
}

    bool
KfsNetClient::Start(
    string             inServerName,
    int                inServerPort,
    string*            inErrMsgPtr,
    bool               inRetryPendingOpsFlag,
    int                inMaxRetryCount,
    int                inTimeSecBetweenRetries,
    bool               inRetryConnectOnlyFlag,
    ClientAuthContext* inAuthContextPtr)
{
    Impl::StRef theRef(mImpl);
    return mImpl.Start(
        inServerName,
        inServerPort,
        inErrMsgPtr,
        inRetryPendingOpsFlag,
        inMaxRetryCount,
        inTimeSecBetweenRetries,
        inRetryConnectOnlyFlag,
        inAuthContextPtr
    );
}

    bool
KfsNetClient::SetServer(
    const ServerLocation& inLocation,
    bool                  inCancelPendingOpsFlag /* = true */,
    string*               inErrMsgPtr            /* = 0 */,
    bool                  inForceConnectFlag     /* = true */)
{
    Impl::StRef theRef(mImpl);
    return mImpl.SetServer(
        inLocation, inCancelPendingOpsFlag, inErrMsgPtr, inForceConnectFlag);
}

    void
KfsNetClient::SetKey(
    const char* inKeyIdPtr,
    const char* inKeyDataPtr,
    int         inKeyDataSize)
{
    Impl::StRef theRef(mImpl);
    mImpl.SetKey(inKeyIdPtr, inKeyDataPtr, inKeyDataSize);
}

    void
KfsNetClient::SetKey(
    const char* inKeyIdPtr,
    int         inKeyIdLen,
    const char* inKeyDataPtr,
    int         inKeyDataSize)
{
    Impl::StRef theRef(mImpl);
    mImpl.SetKey(inKeyIdPtr, inKeyIdLen, inKeyDataPtr, inKeyDataSize);
}

    const string&
KfsNetClient::GetKey() const
{
    return mImpl.GetKey();
}

    const string&
KfsNetClient::GetKeyId() const
{
    return mImpl.GetKeyId();
}

    const string&
KfsNetClient::GetSessionKey() const
{
    return mImpl.GetSessionKey();
}

    const string&
KfsNetClient::GetSessionKeyId() const
{
    return mImpl.GetSessionKeyId();
}

    void
KfsNetClient::SetShutdownSsl(
    bool inFlag)
{
    Impl::StRef theRef(mImpl);
    mImpl.SetShutdownSsl(inFlag);
}

    bool
KfsNetClient::IsShutdownSsl() const
{
    return mImpl.IsShutdownSsl();
}

    void
KfsNetClient::SetAuthContext(
    ClientAuthContext* inAuthContextPtr)
{
    Impl::StRef theRef(mImpl);
    mImpl.SetAuthContext(inAuthContextPtr);
}

    ClientAuthContext*
KfsNetClient::GetAuthContext() const
{
    return mImpl.GetAuthContext();
}

    void
KfsNetClient::Stop()
{
    Impl::StRef theRef(mImpl);
    mImpl.Stop();
}

    int
KfsNetClient::GetMaxRetryCount() const
{
    return mImpl.GetMaxRetryCount();
}

    void
KfsNetClient::SetMaxRetryCount(
    int inMaxRetryCount)
{
    Impl::StRef theRef(mImpl);
    mImpl.SetMaxRetryCount(inMaxRetryCount);
}

    int
KfsNetClient::GetOpTimeoutSec() const
{
    return mImpl.GetOpTimeoutSec();
}

    void
KfsNetClient::SetOpTimeoutSec(
    int inTimeout)
{
    Impl::StRef theRef(mImpl);
    mImpl.SetOpTimeoutSec(inTimeout);
}

    int
KfsNetClient::GetIdleTimeoutSec() const
{
    return mImpl.GetIdleTimeoutSec();
}

    void
KfsNetClient::SetIdleTimeoutSec(
    int inTimeout)
{
    Impl::StRef theRef(mImpl);
    mImpl.SetIdleTimeoutSec(inTimeout);
}

    int
KfsNetClient::GetTimeSecBetweenRetries()
{
    return mImpl.GetTimeSecBetweenRetries();
}

    void
KfsNetClient::SetTimeSecBetweenRetries(
    int inTimeSec)
{
    Impl::StRef theRef(mImpl);
    mImpl.SetTimeSecBetweenRetries(inTimeSec);
}

    bool
KfsNetClient::IsAllDataSent() const
{
    return mImpl.IsAllDataSent();
}

    bool
KfsNetClient::IsDataReceived() const
{
    return mImpl.IsDataReceived();
}

    bool
KfsNetClient::IsDataSent() const
{
    return mImpl.IsDataSent();
}

    bool
KfsNetClient::IsRetryConnectOnly() const
{
    return mImpl.IsRetryConnectOnly();
}

    bool
KfsNetClient::WasDisconnected() const
{
    return mImpl.WasDisconnected();
}

    void
KfsNetClient::SetRetryConnectOnly(
    bool inFlag)
{
    Impl::StRef theRef(mImpl);
    mImpl.SetRetryConnectOnly(inFlag);
}

    int
KfsNetClient::GetOpTimeout() const
{
    return mImpl.GetOpTimeout();
}

    void
KfsNetClient::SetOpTimeout(
    int inOpTimeoutSec)
{
    Impl::StRef theRef(mImpl);
    mImpl.SetOpTimeout(inOpTimeoutSec);
}

    void
KfsNetClient::GetStats(
    Stats& outStats) const
{
    mImpl.GetStats(outStats);
}

    bool
KfsNetClient::Enqueue(
    KfsOp*    inOpPtr,
    OpOwner*  inOwnerPtr,
    IOBuffer* inBufferPtr /* = 0 */)
{
    Impl::StRef theRef(mImpl);
    return mImpl.Enqueue(inOpPtr, inOwnerPtr, inBufferPtr);
}

    bool
KfsNetClient::Cancel(
    KfsOp*   inOpPtr,
    OpOwner* inOwnerPtr)
{
    Impl::StRef theRef(mImpl);
    return mImpl.Cancel(inOpPtr, inOwnerPtr);
}

    bool
KfsNetClient::Cancel()
{
    Impl::StRef theRef(mImpl);
    return mImpl.Cancel();
}

    void
KfsNetClient::CancelAllWithOwner(
    OpOwner* inOwnerPtr)
{
    Impl::StRef theRef(mImpl);
    return mImpl.CancelAllWithOwner(inOwnerPtr);
}

    const ServerLocation&
KfsNetClient::GetServerLocation() const
{
    return mImpl.GetServerLocation();
}

    void
KfsNetClient::SetEventObserver(
    KfsNetClient::EventObserver* inEventObserverPtr)
{
    Impl::StRef theRef(mImpl);
    mImpl.SetEventObserver(inEventObserverPtr);
}

    NetManager&
KfsNetClient::GetNetManager() const
{
    // This method must not change any variables including the reference count.
    // The thread logic in chunk server relies on this, when it constructs RS
    // replicator reader: the reader's constructor calls this method, but must
    // not modify in any way the meta server state machine state, including the
    // ref. count, as the state machine might be running withing a different
    // thread.
    return mImpl.GetNetManager();
}

    void
KfsNetClient::SetMaxContentLength(
    int inMax)
{
    Impl::StRef theRef(mImpl);
    mImpl.SetMaxContentLength(inMax);
}

    void
KfsNetClient::ClearMaxOneOutstandingOpFlag()
{
    Impl::StRef theRef(mImpl);
    mImpl.ClearMaxOneOutstandingOpFlag();
}

    void
KfsNetClient::SetFailAllOpsOnOpTimeoutFlag(
    bool inFlag)
{
    Impl::StRef theRef(mImpl);
    mImpl.SetFailAllOpsOnOpTimeoutFlag(inFlag);
}

    void
KfsNetClient::SetMaxRpcHeaderLength(
    int inMaxRpcHeaderLength)
{
    Impl::StRef theRef(mImpl);
    mImpl.SetMaxRpcHeaderLength(inMaxRpcHeaderLength);
}

    void
KfsNetClient::SetThread(
    const QCThread* inThreadPtr)
{
    // Do not change ref count, in order to allow to set different thread than
    // the current.
    mImpl.SetThread(inThreadPtr);
}

}} /* namespace cient KFS */
