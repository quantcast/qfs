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

#include <sstream>
#include <algorithm>
#include <map>
#include <deque>
#include <string>
#include <cerrno>

#include <stdint.h>
#include <time.h>

#include "kfsio/IOBuffer.h"
#include "kfsio/NetConnection.h"
#include "kfsio/NetManager.h"
#include "kfsio/ITimeout.h"
#include "common/kfstypes.h"
#include "common/kfsdecls.h"
#include "common/MsgLogger.h"
#include "common/StdAllocator.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"
#include "qcdio/QCDLList.h"
#include "KfsOps.h"
#include "utils.h"

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

// Generic KFS request / response protocol state machine implementation.
class KfsNetClient::Impl :
    public KfsCallbackObj,
    public QCRefCountedObj,
    private ITimeout
{
public:
    typedef QCRefCountedObj::StRef StRef;

    Impl(
        string      inHost,
        int         inPort,
        int         inMaxRetryCount,
        int         inTimeSecBetweenRetries,
        int         inOpTimeoutSec,
        int         inIdleTimeoutSec,
        kfsSeq_t    inInitialSeqNum,
        const char* inLogPrefixPtr,
        NetManager& inNetManager,
        bool        inResetConnectionOnOpTimeoutFlag,
        int         inMaxContentLength,
        bool        inFailAllOpsOnOpTimeoutFlag,
        bool        inMaxOneOutstandingOpFlag)
        : KfsCallbackObj(),
          QCRefCountedObj(),
          ITimeout(),
          mServerLocation(inHost, inPort),
          mPendingOpQueue(),
          mQueueStack(),
          mConnPtr(),
          mNextSeqNum(
            (inInitialSeqNum < 0 ? -inInitialSeqNum : inInitialSeqNum) >> 1),
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
          mTimeSecBetweenRetries(inTimeSecBetweenRetries),
          mOpTimeoutSec(inOpTimeoutSec),
          mIdleTimeoutSec(inIdleTimeoutSec),
          mRetryCount(0),
          mContentLength(0),
          mMaxRetryCount(inMaxRetryCount),
          mMaxContentLength(inMaxContentLength),
          mInFlightOpPtr(0),
          mOutstandingOpPtr(0),
          mCurOpIt(),
          mIstream(),
          mOstream(),
          mProperties(),
          mStats(),
          mEventObserverPtr(0),
          mLogPrefix((inLogPrefixPtr && inLogPrefixPtr[0]) ?
                (inLogPrefixPtr + string(" ")) : string()),
          mNetManager(inNetManager)
    {
        SET_HANDLER(this, &KfsNetClient::Impl::EventHandler);
    }
    bool IsConnected() const
        { return (mConnPtr && mConnPtr->IsGood()); }
    bool Start(
        string  inServerName,
        int     inServerPort,
        string* inErrMsgPtr,
        bool    inRetryPendingOpsFlag,
        int     inMaxRetryCount,
        int     inTimeSecBetweenRetries,
        bool    inRetryConnectOnlyFlag)
    {
        if (! inRetryPendingOpsFlag) {
            Cancel();
        }
        mRetryConnectOnlyFlag  = inRetryConnectOnlyFlag;
        mMaxRetryCount         = inMaxRetryCount;
        mTimeSecBetweenRetries = inTimeSecBetweenRetries;
        return SetServer(ServerLocation(inServerName, inServerPort),
            false, inErrMsgPtr);
    }
    bool SetServer(
        const ServerLocation& inLocation,
        bool                  inCancelPendingOpsFlag = true,
        string*               inErrMsgPtr            = 0)
    {
        if (inLocation == mServerLocation) {
            EnsureConnected();
            return (mSleepingFlag || IsConnected());
        }
        if (inCancelPendingOpsFlag) {
            Cancel();
        }
        if (mSleepingFlag || IsConnected()) {
            Reset();
        }
        mServerLocation = inLocation;
        mRetryCount = 0;
        mNextSeqNum += 100;
        EnsureConnected(inErrMsgPtr);
        return (mSleepingFlag || IsConnected());
    }
    void Reset()
    {
        if (mSleepingFlag) {
            mNetManager.UnRegisterTimeoutHandler(this);
            mSleepingFlag = false;
        }
        ResetConnection();
    }
    void Stop()
    {
        Reset();
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
    void GetStats(
        Stats& outStats) const
        { outStats = mStats; }
    string GetServerLocation() const
        { return mServerLocation.ToString(); }
    bool Enqueue(
        KfsOp*    inOpPtr,
        OpOwner*  inOwnerPtr,
        IOBuffer* inBufferPtr = 0)
    {
        // Ensure that the op is in the queue before attempting to re-establish
        // connection, as later can fail other ops, and invoke the op cancel.
        // The op has to be in the queue in order for cancel to work.
        mStats.mOpsQueuedCount++;
        const bool theOkFlag = EnqueueSelf(inOpPtr, inOwnerPtr, inBufferPtr, 0);
        EnsureConnected(0, inOpPtr);
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
                theIt->second.Cancel();
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
                    assert(inDataPtr && mConnPtr);
                    IOBuffer& theBuffer = *reinterpret_cast<IOBuffer*>(inDataPtr);
                    mDataReceivedFlag = mDataReceivedFlag || ! theBuffer.IsEmpty();
                    HandleResponse(theBuffer);
                }
                break;

            case EVENT_NET_WROTE:
                assert(inDataPtr && mConnPtr);
                mDataSentFlag = true;
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
                    mAllDataSentFlag = ! mConnPtr->IsWriteReady();
                    KFS_LOG_STREAM(mPendingOpQueue.empty() ?
                            MsgLogger::kLogLevelDEBUG :
                            MsgLogger::kLogLevelERROR) << mLogPrefix <<
                        "closing connection: " << mConnPtr->GetSockName() <<
                        " to: " <<
                        mServerLocation.ToString() <<
                        " due to " << theReasonPtr <<
                        " error: " <<
                            QCUtils::SysError(mConnPtr->GetSocketError()) <<
                        " pending read: " << mConnPtr->GetNumBytesToRead() <<
                        " write: " << mConnPtr->GetNumBytesToWrite() <<
                        " ops: " << mPendingOpQueue.size() <<
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
    NetManager& GetNetManager()
        { return mNetManager; }
    const NetManager& GetNetManager() const
        { return mNetManager; }
    void SetMaxContentLength(
        int inMax)
        { mMaxContentLength = inMax; }
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
            if (mOwnerPtr) {
                if (mOpPtr) {
                    KfsOp* const theOpPtr = mOpPtr;
                    mOpPtr = 0;
                    mOwnerPtr->OpDone(theOpPtr, inCanceledFlag, mBufferPtr);
                }
                mOwnerPtr  = 0;
                mBufferPtr = 0;
            } else {
                delete mOpPtr;
                delete mBufferPtr;
                mBufferPtr = 0;
                mOpPtr     = 0;
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
    bool               mResetConnectionOnOpTimeoutFlag;
    bool               mFailAllOpsOnOpTimeoutFlag;
    bool               mMaxOneOutstandingOpFlag;
    int                mTimeSecBetweenRetries;
    int                mOpTimeoutSec;
    int                mIdleTimeoutSec;
    int                mRetryCount;
    int                mContentLength;
    int                mMaxRetryCount;
    int                mMaxContentLength;
    OpQueueEntry*      mInFlightOpPtr;
    OpQueueEntry*      mOutstandingOpPtr;
    OpQueue::iterator  mCurOpIt;
    IOBuffer::IStream  mIstream;
    IOBuffer::WOStream mOstream;
    Properties         mProperties;
    Stats              mStats;
    EventObserver*     mEventObserverPtr;
    const string       mLogPrefix;
    NetManager&        mNetManager;

    virtual ~Impl()
        { Impl::Reset(); }
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
        inOpPtr->seq = mNextSeqNum++;
        const bool theResetTimerFlag = mPendingOpQueue.empty();
        pair<OpQueue::iterator, bool> const theRes =
            mPendingOpQueue.insert(make_pair(
                inOpPtr->seq, OpQueueEntry(inOpPtr, inOwnerPtr, inBufferPtr)
            ));
        if (! theRes.second || ! IsConnected()) {
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
    void Request(
        OpQueueEntry& inEntry,
        bool          inResetTimerFlag,
        int           inRetryCount)
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
        mConnPtr->SetInactivityTimeout(mOpTimeoutSec);
        mConnPtr->Flush(inResetTimerFlag);
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
                    mContentLength -= inBuffer.Consume(mContentLength);
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
                inBuffer.Consume(mContentLength);
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
                if (theBufPtr) {
                    theBufPtr->RemoveSpaceAvailable();
                    QCVERIFY(mContentLength ==
                        theBufPtr->MoveSpace(&inBuffer, mContentLength)
                    );
                } else {
                    IOBuffer theBuf;
                    QCVERIFY(mContentLength ==
                        theBuf.MoveSpace(&inBuffer, mContentLength)
                    );
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
            if (inBuffer.BytesConsumable() > MAX_RPC_HEADER_LEN) {
               KFS_LOG_STREAM_ERROR << mLogPrefix <<
                    "error: " << mServerLocation.ToString() <<
                    ": exceeded max. response header size: " <<
                    MAX_RPC_HEADER_LEN << "; got " <<
                    inBuffer.BytesConsumable() << " resetting connection" <<
                KFS_LOG_EOM;
                Reset();
                EnsureConnected();
            }
            return false;
        }
        const int  theHdrLen = theIdx + 4;
        const char theSeparator     = ':';
        const bool theMultiLineFlag = false;
        mProperties.clear();
        mProperties.loadProperties(
            mIstream.Set(inBuffer, theHdrLen), theSeparator, theMultiLineFlag);
        mIstream.Reset();
        inBuffer.Consume(theHdrLen);
        mReadHeaderDoneFlag = true;
        mContentLength = mProperties.getValue("Content-length", 0);
        const kfsSeq_t theOpSeq = mProperties.getValue("Cseq", kfsSeq_t(-1));
        if (mContentLength > mMaxContentLength) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "error: " << mServerLocation.ToString() <<
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
            KFS_LOG_STREAM_INFO << mLogPrefix <<
                "no operation found with seq: " << theOpSeq <<
                ", discarding response " <<
                " content length: " << mContentLength <<
            KFS_LOG_EOM;
            mContentLength -= inBuffer.Consume(mContentLength);
            return true;
        }
        if (mOutstandingOpPtr && mOutstandingOpPtr != mInFlightOpPtr) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "error: " << mServerLocation.ToString() <<
                " seq: " << theOpSeq <<
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
        theOp.EnsureCapacity(size_t(mContentLength));
        IOBuffer theBuf;
        theBuf.Append(IOBufferData(
            IOBufferData::IOBufferBlockPtr(
                theOp.contentBuf,
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
        if (mContentLength > 0 && mConnPtr && mInFlightOpPtr->mBufferPtr) {
            // Detach shared buffers, if any.
            IOBuffer& theBuf = mConnPtr->GetInBuffer();
            mContentLength -= theBuf.BytesConsumable();
            theBuf.Clear();
        }
        mInFlightOpPtr = 0;
    }
    void EnsureConnected(
        string*      inErrMsgPtr = 0,
        const KfsOp* inLastOpPtr = 0)
    {
        if (mSleepingFlag || IsConnected()) {
            return;
        }
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
                "failed to connect to server " << mServerLocation.ToString() <<
                " : " << QCUtils::SysError(-theErr) <<
            KFS_LOG_EOM;
            delete &theSocket;
            mStats.mConnectFailureCount++;
            RetryConnect();
            return;
        }
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "connecting to server: " << mServerLocation.ToString() <<
        KFS_LOG_EOM;
        mConnPtr.reset(new NetConnection(&theSocket, this));
        mConnPtr->EnableReadIfOverloaded();
        mConnPtr->SetDoingNonblockingConnect();
        mConnPtr->SetMaxReadAhead(kMaxReadAhead);
        mConnPtr->SetInactivityTimeout(mOpTimeoutSec);
        // Add connection to the poll vector
        mNetManager.AddConnection(mConnPtr);
        RetryAll(inLastOpPtr);
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
            if (theIt->second.mOwnerPtr) {
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
            } else {
                mStats.mOpsCancelledCount++;
                theEntry.Cancel();
            }
        }
        mQueueStack.erase(theIt);
    }
    void ResetConnection()
    {
        CancelInFlightOp();
        mOutstandingOpPtr = 0;
        if (! mConnPtr) {
            return;
        }
        mConnPtr->Close();
        mConnPtr->GetInBuffer().Clear();
        mConnPtr->SetOwningKfsCallbackObj(0);
        mConnPtr.reset();
        mReadHeaderDoneFlag = false;
        mContentLength      = 0;
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
        if (mRetryCount < mMaxRetryCount && (! mRetryConnectOnlyFlag ||
                (! mDataSentFlag && ! mDataReceivedFlag))) {
            mRetryCount++;
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
        } else if (inOutstandingOpPtr && ! mFailAllOpsOnOpTimeoutFlag &&
                ! mPendingOpQueue.empty() &&
                &(mPendingOpQueue.begin()->second) == inOutstandingOpPtr) {
            HandleSingleOpTimeout(mPendingOpQueue.begin());
        } else {
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
                theIt->second.mOpPtr->status = kErrorMaxRetryReached;
                theIt->second.Done();
            }
            mQueueStack.erase(theIt);
        }
    }
    void HandleSingleOpTimeout(
        OpQueue::iterator inIt)
    {
        OpQueueEntry& theEntry = inIt->second;
        if (theEntry.mRetryCount < mMaxRetryCount) {
            theEntry.mRetryCount++;
        } else {
            theEntry.mOpPtr->status = kErrorMaxRetryReached;
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
        // Timeout ops waiting for response.
        // The ops in the queue are ordered by op seq. number.
        // The code ensures (initial seq. number in ctor) that seq. numbers
        // never wrap around, and are monotonically increase, so that the last
        // (re)queued operation seq. number is always the largest.
        // First move all timed out ops into the temporary queue, then process
        // the temporary queue. This is less error prone than dealing with
        // completion changing mPendingOpQueue while iterating.
        QueueStack::iterator theStIt       = mQueueStack.end();
        const time_t         theNow        = Now();
        time_t               theExpireTime = theNow - mOpTimeoutSec;
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
        for (OpQueue::iterator theIt = theStIt->begin();
                theIt != theStIt->end();
                ++theIt) {
            OpQueueEntry& theEntry = theIt->second;
            if (! theEntry.mOpPtr) {
                continue;
            }
            KFS_LOG_STREAM_INFO << mLogPrefix <<
                "op timed out: seq: " << theEntry.mOpPtr->seq <<
                " "                   << theEntry.mOpPtr->Show() <<
                " retry count: "      << theEntry.mRetryCount <<
                " max: "              << mMaxRetryCount <<
                " wait time: "        << (theNow - theEntry.mTime) <<
            KFS_LOG_EOM;
            mStats.mOpsTimeoutCount++;
            if (theEntry.mRetryCount >= mMaxRetryCount) {
                theEntry.mOpPtr->status = kErrorMaxRetryReached;
                theEntry.Done();
            } else {
                mStats.mOpsRetriedCount++;
                const OpQueueEntry theTmp = theEntry;
                theEntry.Clear();
                EnqueueSelf(theTmp.mOpPtr, theTmp.mOwnerPtr,
                    theTmp.mBufferPtr, theTmp.mRetryCount + 1);
            }
        }
        mQueueStack.erase(theStIt);
        if (! mPendingOpQueue.empty()) {
            EnsureConnected();
        }
    }
    friend class StImplRef;
private:
    Impl(
        const Impl& inClient);
    Impl& operator=(
        const Impl& inClient);
};

KfsNetClient::KfsNetClient(
        NetManager& inNetManager,
        string      inHost                           /* = string() */,
        int         inPort                           /* = 0 */,
        int         inMaxRetryCount                  /* = 0 */,
        int         inTimeSecBetweenRetries          /* = 10 */,
        int         inOpTimeoutSec                   /* = 5  * 60 */,
        int         inIdleTimeoutSec                 /* = 30 * 60 */,
        int64_t     inInitialSeqNum                  /* = 1 */,
        const char* inLogPrefixPtr                   /* = 0 */,
        bool        inResetConnectionOnOpTimeoutFlag /* = true */,
        int         inMaxContentLength               /* = MAX_RPC_HEADER_LEN */,
        bool        inFailAllOpsOnOpTimeoutFlag      /* = false */,
        bool        inMaxOneOutstandingOpFlag        /* = false */)
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
        inMaxOneOutstandingOpFlag
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

    bool
KfsNetClient::Start(
    string  inServerName,
    int     inServerPort,
    string* inErrMsgPtr,
    bool    inRetryPendingOpsFlag,
    int     inMaxRetryCount,
    int     inTimeSecBetweenRetries,
    bool    inRetryConnectOnlyFlag)
{
    return mImpl.Start(
        inServerName,
        inServerPort,
        inErrMsgPtr,
        inRetryPendingOpsFlag,
        inMaxRetryCount,
        inTimeSecBetweenRetries,
        inRetryConnectOnlyFlag
    );
}

    bool
KfsNetClient::SetServer(
    const ServerLocation& inLocation,
    bool                  inCancelPendingOpsFlag /* = true */)
{
    Impl::StRef theRef(mImpl);
    return mImpl.SetServer(inLocation, inCancelPendingOpsFlag);
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
    Impl::StRef theRef(mImpl);
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
    Impl::StRef theRef(mImpl);
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
    Impl::StRef theRef(mImpl);
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
    Impl::StRef theRef(mImpl);
    return mImpl.IsDataReceived();
}

    bool
KfsNetClient::IsDataSent() const
{
    Impl::StRef theRef(mImpl);
    return mImpl.IsDataSent();
}

    bool
KfsNetClient::IsRetryConnectOnly() const
{
    Impl::StRef theRef(mImpl);
    return mImpl.IsRetryConnectOnly();
}

    bool
KfsNetClient::WasDisconnected() const
{
    Impl::StRef theRef(mImpl);
    return mImpl.WasDisconnected();
}

    void
KfsNetClient::SetRetryConnectOnly(
    bool inFlag)
{
    Impl::StRef theRef(mImpl);
    mImpl.SetRetryConnectOnly(inFlag);
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
    Impl::StRef theRef(mImpl);
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

    string
KfsNetClient::GetServerLocation() const
{
    Impl::StRef theRef(mImpl);
    return mImpl.GetServerLocation();
}

    void
KfsNetClient::SetEventObserver(
    KfsNetClient::EventObserver* inEventObserverPtr)
{
    Impl::StRef theRef(mImpl);
    return mImpl.SetEventObserver(inEventObserverPtr);
}

    NetManager&
KfsNetClient::GetNetManager()
{
    Impl::StRef theRef(mImpl);
    return mImpl.GetNetManager();
}

    const NetManager&
KfsNetClient::GetNetManager() const
{
    Impl::StRef theRef(mImpl);
    return mImpl.GetNetManager();
}

    void
KfsNetClient::SetMaxContentLength(
    int inMax)
{
    Impl::StRef theRef(mImpl);
    return mImpl.SetMaxContentLength(inMax);
}

    void
KfsNetClient::ClearMaxOneOutstandingOpFlag()
{
    Impl::StRef theRef(mImpl);
    return mImpl.ClearMaxOneOutstandingOpFlag();
}

    void
KfsNetClient::SetFailAllOpsOnOpTimeoutFlag(
    bool inFlag)
{
    Impl::StRef theRef(mImpl);
    return mImpl.SetFailAllOpsOnOpTimeoutFlag(inFlag);
}

}} /* namespace cient KFS */
