//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/04/21
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
// Transaction log replication reciver.
//
//
//----------------------------------------------------------------------------

#include "LogReceiver.h"
#include "AuthContext.h"
#include "MetaRequest.h"
#include "MetaVrSM.h"
#include "MetaVrOps.h"
#include "util.h"

#include "common/kfstypes.h"
#include "common/MsgLogger.h"
#include "common/RequestParser.h"
#include "common/StBuffer.h"
#include "common/SingleLinkedQueue.h"

#include "kfsio/NetManager.h"
#include "kfsio/ITimeout.h"
#include "kfsio/NetConnection.h"
#include "kfsio/SslFilter.h"
#include "kfsio/Acceptor.h"
#include "kfsio/checksum.h"
#include "kfsio/NetErrorSimulator.h"

#include "qcdio/QCUtils.h"
#include "qcdio/qcdebug.h"

#include <string>
#include <algorithm>
#include <iomanip>
#include <vector>

#include <time.h>
#include <errno.h>

namespace KFS
{
using std::string;
using std::vector;
using std::max;
using std::hex;

class LogReceiver::Impl :
    public IAcceptorOwner,
    public KfsCallbackObj,
    public ITimeout
{
private:
    class Connection;
public:
    typedef QCDLList<Connection> List;
    typedef MetaVrSM::NodeId     NodeId;

    Impl()
        : IAcceptorOwner(),
          KfsCallbackObj(),
          ITimeout(),
          mReAuthTimeout(20),
          mMaxReadAhead(MAX_RPC_HEADER_LEN),
          mTimeout(60),
          mConnectionCount(0),
          mMaxConnectionCount(8 << 10),
          mMaxSocketsCount(mMaxConnectionCount),
          mMaxPendingOpsCount(1 << 10),
          mIpV6OnlyFlag(false),
          mListenerAddress(),
          mAcceptorPtr(0),
          mAuthContext(),
          mLastLogSeq(),
          mLastWriteSeq(),
          mSubmittedWriteSeq(),
          mDeleteFlag(false),
          mAckBroadcastFlag(false),
          mDispatchAckBroadcastFlag(false),
          mWakerPtr(0),
          mParseBuffer(),
          mFileSystemId(-1),
          mId(-1),
          mPrimaryId(-1),
          mInFlightWriteCount(0),
          mWriteOpFreeList(),
          mPendingResponseQueue(),
          mResponseQueue(),
          mPendingSubmitQueue(),
          mErrorSimulatorConfig()
    {
        List::Init(mConnectionsHeadPtr);
        mParseBuffer.Resize(mParseBuffer.Capacity());
        SET_HANDLER(this, &Impl::HandleEvent);
    }
    virtual KfsCallbackObj* CreateKfsCallbackObj(
        NetConnectionPtr& inConnectionPtr);
    bool SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters)
    {
        Properties::String theParamName;
        if (inPrefixPtr) {
            theParamName.Append(inPrefixPtr);
        }
        const size_t thePrefixLen   = theParamName.GetSize();
        const bool   kHexFormatFlag = false;
        const string theListenOn    = inParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "listenOn"), mListenerAddress.IsValid() ?
                mListenerAddress.ToString() : string());
        mListenerAddress.FromString(theListenOn, kHexFormatFlag);
        mReAuthTimeout = inParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "reAuthTimeout"), mReAuthTimeout);
        mIpV6OnlyFlag = inParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "ipV6Only"), mIpV6OnlyFlag ? 1 : 0) != 0;
        mMaxReadAhead = max(512, min(64 << 20, inParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "maxReadAhead"), mMaxReadAhead)));
        mMaxConnectionCount = max(16, inParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "maxConnectionCount"), mMaxConnectionCount));
        mMaxPendingOpsCount = inParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "maxPendingOpsCount"), mMaxPendingOpsCount);
        mTimeout = inParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "timeout"), mTimeout);
        const string thePrevErrorSimulatorConfig = mErrorSimulatorConfig;
        mErrorSimulatorConfig = inParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "netErrorSimulator"), mErrorSimulatorConfig);
        if (thePrevErrorSimulatorConfig != mErrorSimulatorConfig &&
                mAcceptorPtr &&
                NetErrorSimulatorConfigure(
                    mAcceptorPtr->GetNetManager(),
                    mErrorSimulatorConfig.c_str())) {
            KFS_LOG_STREAM_INFO <<
                "network error simulator configured: " <<
                mErrorSimulatorConfig <<
            KFS_LOG_EOM;
        }
        return mAuthContext.SetParameters(
            theParamName.Truncate(thePrefixLen).Append("auth.").c_str(),
            inParameters);
    }
    ServerLocation GetListenerAddress() const
    {
        return (mAcceptorPtr ? mAcceptorPtr->GetLocation() : mListenerAddress);
    }
    int Start(
        NetManager&         inNetManager,
        Waker&              inWaker,
        const MetaVrLogSeq& inCommittedLogSeq,
        const MetaVrLogSeq& inLastLogSeq,
        int64_t             inFileSystemId,
        NodeId              inNodeId,
        int                 inMaxSocketsCount)
    {
        if (mDeleteFlag) {
            panic("LogReceiver::Impl::Start delete pending");
            return -EINVAL;
        }
        ShutdownSelf();
        if (! inNetManager.IsRunning()) {
            KFS_LOG_STREAM_ERROR <<
                "net manager shutdown" <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        if (! mListenerAddress.IsValid()) {
            KFS_LOG_STREAM_ERROR <<
                "invalid listen address: " << mListenerAddress <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        if (inNodeId < 0) {
            KFS_LOG_STREAM_ERROR <<
                "server node id is not set: " << inNodeId <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        if (inFileSystemId < 0) {
            KFS_LOG_STREAM_ERROR <<
                "file sytem id is not set: " << inFileSystemId <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        if (inMaxSocketsCount <= 0) {
            KFS_LOG_STREAM_ERROR <<
                "invalid max sockets limit: " << inMaxSocketsCount <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        const bool kBindOnlyFlag = false;
        mAcceptorPtr = new Acceptor(
            inNetManager,
            mListenerAddress,
            mIpV6OnlyFlag,
            this,
            kBindOnlyFlag
        );
        if (! mAcceptorPtr->IsAcceptorStarted()) {
            delete mAcceptorPtr;
            mAcceptorPtr = 0;
            KFS_LOG_STREAM_ERROR <<
                "failed to start acceptor: " << mListenerAddress <<
            KFS_LOG_EOM;
            return -ENOTCONN;
        }
        if (! mErrorSimulatorConfig.empty() &&
                NetErrorSimulatorConfigure(
                    mAcceptorPtr->GetNetManager(),
                    mErrorSimulatorConfig.c_str())) {
            KFS_LOG_STREAM_INFO <<
                "network error simulator configured: " <<
                mErrorSimulatorConfig <<
            KFS_LOG_EOM;
        }
        mAcceptorPtr->GetNetManager().RegisterTimeoutHandler(this);
        mLastLogSeq        = inLastLogSeq;
        mLastWriteSeq      = inLastLogSeq;
        mSubmittedWriteSeq = inLastLogSeq;
        mFileSystemId      = inFileSystemId;
        mId                = inNodeId;
        mMaxSocketsCount   = inMaxSocketsCount;
        mWakerPtr          = &inWaker;
        return 0;
    }
    void Shutdown();
    int GetReAuthTimeout() const
        { return mReAuthTimeout; }
    int GetMaxReadAhead() const
        { return mMaxReadAhead; }
    int GetTimeout() const
        { return mTimeout; }
    AuthContext& GetAuthContext()
        { return mAuthContext; }
    void New(
        Connection& inConnection);
    void Done(
        Connection& inConnection);
    MetaVrLogSeq GetLastWriteLogSeq() const
        { return mLastWriteSeq; }
    MetaVrLogSeq GetLastLogSeq() const
        { return mLastLogSeq; }
    MetaVrLogSeq GetSubmittedWriteSeq() const
        { return mSubmittedWriteSeq; }
    int64_t GetFileSystemId() const
        { return mFileSystemId; }
    NodeId GetPrimaryId() const
        { return mPrimaryId; }
    int GetMaxPendingOpsCount() const
        { return mMaxPendingOpsCount; }
    void Delete()
    {
        Shutdown();
        mDeleteFlag = true;
        if (0 < mConnectionCount) {
            return;
        }
        delete this;
    }
    NodeId GetId() const
        { return mId; }

    enum { kMaxBlockHeaderLen  = 5 * ((int)sizeof(seq_t) * 2 + 1) + 1 + 16 };
    enum { kMinParseBufferSize = kMaxBlockHeaderLen <= MAX_RPC_HEADER_LEN ?
        MAX_RPC_HEADER_LEN : kMaxBlockHeaderLen };

    char* GetParseBufferPtr()
        { return mParseBuffer.GetPtr(); }
    char* GetParseBufferPtr(
        int inSize)
    {
        if (0 < inSize && mParseBuffer.GetSize() < (size_t)inSize) {
            mParseBuffer.Clear(); // To prevent copy.
            mParseBuffer.Resize(
                (inSize + kMinParseBufferSize - 1) /
                kMinParseBufferSize * kMinParseBufferSize
            );
        }
        return mParseBuffer.GetPtr();
    }
    MetaLogWriterControl* GetBlockWriteOp(
        const MetaVrLogSeq& inStartSeq,
        const MetaVrLogSeq& inEndSeq)
    {
        if (inEndSeq <= mSubmittedWriteSeq &&
                (inEndSeq != mSubmittedWriteSeq || inStartSeq != inEndSeq)) {
            return 0;
        }
        MetaLogWriterControl* thePtr =
            static_cast<MetaLogWriterControl*>(mWriteOpFreeList.PopFront());
        if (! thePtr) {
            thePtr = new MetaLogWriterControl(
                MetaLogWriterControl::kWriteBlock);
        }
        thePtr->logReceiverFlag = true;
        thePtr->clnt            = this;
        return thePtr;
    }
    bool Dispatch()
    {
        Queue thePendingSubmitQueue;
        thePendingSubmitQueue.PushBack(mPendingSubmitQueue);
        const bool   theRetFlag = ! thePendingSubmitQueue.IsEmpty();
        MetaRequest* thePtr;
        while ((thePtr = thePendingSubmitQueue.PopFront())) {
            KFS_LOG_STREAM_DEBUG <<
                "submit: " << thePtr->Show() <<
            KFS_LOG_EOM;
            submit_request(thePtr);
        }
        mAckBroadcastFlag = mAckBroadcastFlag || mDispatchAckBroadcastFlag;
        mDispatchAckBroadcastFlag = false;
        mResponseQueue.PushBack(mPendingResponseQueue);
        return theRetFlag;
    }
    void Release(
        MetaLogWriterControl& inOp)
    {
        inOp.Reset(MetaLogWriterControl::kWriteBlock);
        mWriteOpFreeList.PutFront(inOp);
    }
    int HandleEvent(
        int   inType,
        void* inDataPtr)
    {
        if (inType != EVENT_CMD_DONE || ! inDataPtr) {
            panic("LogReceiver::Impl: unexpected event");
            return 0;
        }
        MetaRequest& theOp = *reinterpret_cast<MetaRequest*>(inDataPtr);
        if (! mWakerPtr) {
            MetaRequest::Release(&theOp);
            return 0;
        }
        mDispatchAckBroadcastFlag = mDispatchAckBroadcastFlag ||
            META_LOG_WRITER_CONTROL == theOp.op;
        const bool theWakeupFlag = ! IsAwake();
        mPendingResponseQueue.PushBack(theOp);
        KFS_LOG_STREAM_DEBUG <<
            reinterpret_cast<void*>(&theOp) <<
            " "                << theOp.Show() <<
            " wakeup: "        << theWakeupFlag <<
            " ack broadcast: " << mDispatchAckBroadcastFlag <<
        KFS_LOG_EOM;
        if (theWakeupFlag) {
            Wakeup();
        }
        return 0;
    }
    void SubmitLogWrite(
        MetaLogWriterControl& inOp)
    {
        mInFlightWriteCount++;
        mSubmittedWriteSeq = inOp.blockEndSeq;
        inOp.clnt = this;
        Submit(inOp);
    }
    void Submit(
        MetaRequest& inOp)
    {
        const bool theWakeupFlag = ! IsAwake();
        KFS_LOG_STREAM_DEBUG <<
            "pending submit: " << inOp.Show()   <<
            " wakeup: "        << theWakeupFlag <<
        KFS_LOG_EOM;
        mPendingSubmitQueue.PushBack(inOp);
        if (theWakeupFlag) {
            Wakeup();
        }
    }
    void Wakeup()
    {
        if (mWakerPtr) {
            mWakerPtr->Wakeup();
        }
    }
    virtual void Timeout()
    {
        MetaRequest* theReqPtr;
        while ((theReqPtr = mResponseQueue.PopFront())) {
            if (this == theReqPtr->clnt || ! theReqPtr->clnt) {
                if (META_LOG_WRITER_CONTROL == theReqPtr->op) {
                    MetaLogWriterControl& theCur =
                        *static_cast<MetaLogWriterControl*>(theReqPtr);
                    mLastLogSeq = max(theCur.lastLogSeq, mLastLogSeq);
                    mPrimaryId  = theCur.primaryNodeId;
                    if (--mInFlightWriteCount <= 0) {
                        if (mInFlightWriteCount < 0) {
                            panic("log receiver:"
                                " invalid in flight write op count");
                        }
                        mSubmittedWriteSeq = mLastLogSeq;
                    } else {
                        mSubmittedWriteSeq =
                            max(mSubmittedWriteSeq, mLastLogSeq);
                    }
                    Release(theCur);
                } else {
                    MetaRequest::Release(theReqPtr);
                }
            } else {
                // Mark to let connection's handle event logic know that it
                // should process it.
                theReqPtr->next = theReqPtr;
                theReqPtr->clnt->HandleEvent(EVENT_CMD_DONE, theReqPtr);
            }
        }
        if (mAckBroadcastFlag) {
            mAckBroadcastFlag = false;
            BroadcastAck();
        }
    }
private:
    typedef StBufferT<char, kMinParseBufferSize>                 ParseBuffer;
    typedef SingleLinkedQueue<MetaRequest, MetaRequest::GetNext> Queue;

    int            mReAuthTimeout;
    int            mMaxReadAhead;
    int            mTimeout;
    int            mConnectionCount;
    int            mMaxConnectionCount;
    int            mMaxSocketsCount;
    int            mMaxPendingOpsCount;
    bool           mIpV6OnlyFlag;
    ServerLocation mListenerAddress;
    Acceptor*      mAcceptorPtr;
    AuthContext    mAuthContext;
    MetaVrLogSeq   mLastLogSeq;
    MetaVrLogSeq   mLastWriteSeq;
    MetaVrLogSeq   mSubmittedWriteSeq;
    bool           mDeleteFlag;
    bool           mAckBroadcastFlag;
    bool           mDispatchAckBroadcastFlag;
    Waker*         mWakerPtr;
    ParseBuffer    mParseBuffer;
    int64_t        mFileSystemId;
    NodeId         mId;
    NodeId         mPrimaryId;
    int            mInFlightWriteCount;
    Queue          mWriteOpFreeList;
    Queue          mPendingResponseQueue;
    Queue          mResponseQueue;
    Queue          mPendingSubmitQueue;
    string         mErrorSimulatorConfig;
    Connection*    mConnectionsHeadPtr[1];

    ~Impl()
    {
        if (mConnectionCount != 0) {
            panic("LogReceiver::~Impl: invalid connection count");
        }
        if (mAcceptorPtr) {
            mAcceptorPtr->GetNetManager().UnRegisterTimeoutHandler(this);
        }
        delete mAcceptorPtr;
        ClearQueues();
    }
    void ClearQueues()
    {
        ClearQueue(mPendingResponseQueue);
        ClearQueue(mResponseQueue);
        ClearQueue(mPendingSubmitQueue);
        ClearQueue(mWriteOpFreeList);
    }
    static void ClearQueue(
        Queue& inQueue)
    {
        Queue theQueue;
        theQueue.PushBack(inQueue);
        MetaRequest* thePtr;
        while ((thePtr = theQueue.PopFront())) {
            MetaRequest::Release(thePtr);
        }
    }
    bool IsAwake() const
    {
        return (
            ! mPendingSubmitQueue.IsEmpty() ||
            ! mPendingResponseQueue.IsEmpty()
        );
    }
    void BroadcastAck();
    void ShutdownSelf();
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

class LogReceiver::Impl::Connection :
    public KfsCallbackObj,
    public SslFilterVerifyPeer
{
public:
    typedef Impl::List   List;
    typedef Impl::NodeId NodeId;

    Connection(
        Impl&                   inImpl,
        const NetConnectionPtr& inConnectionPtr)
        : KfsCallbackObj(),
          SslFilterVerifyPeer(),
          mImpl(inImpl),
          mAuthName(),
          mSessionExpirationTime(0),
          mConnectionPtr(inConnectionPtr),
          mPeerLocation(GetPeerLocation(inConnectionPtr)),
          mAuthenticateOpPtr(0),
          mAuthCount(0),
          mAuthCtxUpdateCount(0),
          mRecursionCount(0),
          mBlockLength(-1),
          mPendingOpsCount(0),
          mBlockChecksum(0),
          mBodyChecksum(0),
          mBlockStartSeq(),
          mBlockEndSeq(),
          mDownFlag(false),
          mIdSentFlag(false),
          mReAuthPendingFlag(false),
          mFirstAckFlag(true),
          mTransmitterId(-1),
          mAuthPendingResponsesQueue(),
          mIStream(),
          mOstream()
    {
        List::Init(*this);
        SET_HANDLER(this, &Connection::HandleEvent);
        if (! mConnectionPtr || ! mConnectionPtr->IsGood()) {
            panic("LogReceiver::Impl::Connection; invalid connection poiner");
        }
        mSessionExpirationTime = TimeNow() - int64_t(60) * 60 * 24 * 365 * 10;
        mConnectionPtr->SetInactivityTimeout(mImpl.GetTimeout());
        mConnectionPtr->SetMaxReadAhead(mImpl.GetMaxReadAhead());
        mImpl.New(*this);
    }
    ~Connection()
    {
        if (mRecursionCount != 0 || mConnectionPtr->IsGood() ||
                mPendingOpsCount != 0) {
            panic("LogReceiver::~Impl::Connection invalid invocation");
        }
        MetaRequest* thePtr;
        while ((thePtr = mAuthPendingResponsesQueue.PopFront())) {
            MetaRequest::Release(thePtr);
        }
        MetaRequest::Release(mAuthenticateOpPtr);
        mImpl.Done(*this);
        mRecursionCount  = 0xDEAD;
        mPendingOpsCount = 0xDEAD;
    }
    virtual bool Verify(
        string&       ioFilterAuthName,
        bool          inPreverifyOkFlag,
        int           inCurCertDepth,
        const string& inPeerName,
        int64_t       inEndTime,
        bool          inEndTimeValidFlag)
    {
        KFS_LOG_STREAM_DEBUG << mPeerLocation <<
            " log receiver auth. verify:"
            " name: "           << inPeerName <<
            " prev: "           << ioFilterAuthName <<
            " preverify: "      << inPreverifyOkFlag <<
            " depth: "          << inCurCertDepth <<
            " end time: +"      << (inEndTime - time(0)) <<
            " end time valid: " << inEndTimeValidFlag <<
        KFS_LOG_EOM;
        // Do no allow to renegotiate and change the name.
        string theAuthName = inPeerName;
        if (! inPreverifyOkFlag ||
                (inCurCertDepth == 0 &&
                ((GetAuthContext().HasUserAndGroup() ?
                    GetAuthContext().GetUid(theAuthName) == kKfsUserNone :
                    ! GetAuthContext().RemapAndValidate(theAuthName)) ||
                (! mAuthName.empty() && theAuthName != mAuthName)))) {
            KFS_LOG_STREAM_ERROR << mPeerLocation <<
                " log receiver authentication failure:"
                " peer: "  << inPeerName <<
                " name: "  << theAuthName <<
                " depth: " << inCurCertDepth <<
                " is not valid" <<
                (mAuthName.empty() ? "" : "prev name: ") << mAuthName <<
            KFS_LOG_EOM;
            mAuthName.clear();
            ioFilterAuthName.clear();
            return false;
        }
        if (inCurCertDepth == 0) {
            ioFilterAuthName = inPeerName;
            mAuthName        = theAuthName;
            if (inEndTimeValidFlag && inEndTime < mSessionExpirationTime) {
                mSessionExpirationTime = inEndTime;
            }
        }
        return true;
    }
    int HandleEvent(
        int   inType,
        void* inDataPtr)
    {
        if (EVENT_CMD_DONE == inType) {
            // Command completion can be invoked from a different thread.
            MetaRequest* const theReqPtr =
                reinterpret_cast<MetaRequest*>(inDataPtr);
            if (theReqPtr) {
                if (theReqPtr->next != theReqPtr) {
                    return mImpl.HandleEvent(inType, inDataPtr);
                }
                theReqPtr->next = 0;
            }
        }
        mRecursionCount++;
        QCASSERT(0 < mRecursionCount);
        switch (inType) {
            case EVENT_NET_READ:
                QCASSERT(&mConnectionPtr->GetInBuffer() == inDataPtr);
                HandleRead();
                break;
            case EVENT_NET_WROTE:
                if (mAuthenticateOpPtr) {
                    HandleAuthWrite();
                }
                break;
            case EVENT_CMD_DONE:
                if (! inDataPtr) {
                    panic("invalid null command completion");
                    break;
                }
                HandleCmdDone(*reinterpret_cast<MetaRequest*>(inDataPtr));
                break;
            case EVENT_NET_ERROR:
                if (HandleSslShutdown()) {
                    break;
                }
                Error("network error");
                break;
            case EVENT_INACTIVITY_TIMEOUT:
                Error("connection timed out");
                break;
            default:
                panic("LogReceiver: unexpected event");
                break;
        }
        if (mRecursionCount <= 1) {
            if (mConnectionPtr->IsGood()) {
                mConnectionPtr->StartFlush();
            } else if (! mDownFlag) {
                Error();
            }
            if (mDownFlag && mPendingOpsCount <= 0) {
                mRecursionCount--;
                delete this;
                return 0;
            }
        }
        mRecursionCount--;
        QCASSERT(0 <= mRecursionCount);
        return 0;
    }
    void SendAck()
    {
        QCASSERT(0 == mRecursionCount);
        if (! mConnectionPtr) {
            return;
        }
        SendAckSelf();
    }
    time_t TimeNow() const
        { return mConnectionPtr->TimeNow(); }
    NodeId GetTransmitterId() const
        { return mTransmitterId; }
    void Close()
        { Error("close"); }
private:
    typedef MetaLogWriterControl::Lines                          Lines;
    typedef uint32_t                                             Checksum;
    typedef SingleLinkedQueue<MetaRequest, MetaRequest::GetNext> Queue;

    Impl&                  mImpl;
    string                 mAuthName;
    int64_t                mSessionExpirationTime;
    NetConnectionPtr const mConnectionPtr;
    ServerLocation   const mPeerLocation;
    MetaAuthenticate*      mAuthenticateOpPtr;
    uint64_t               mAuthCount;
    uint64_t               mAuthCtxUpdateCount;
    int                    mRecursionCount;
    int                    mBlockLength;
    int                    mPendingOpsCount;
    Checksum               mBlockChecksum;
    Checksum               mBodyChecksum;
    MetaVrLogSeq           mBlockStartSeq;
    MetaVrLogSeq           mBlockEndSeq;
    bool                   mDownFlag;
    bool                   mIdSentFlag;
    bool                   mReAuthPendingFlag;
    bool                   mFirstAckFlag;
    NodeId                 mTransmitterId;
    Queue                  mAuthPendingResponsesQueue;
    IOBuffer::IStream      mIStream;
    IOBuffer::WOStream     mOstream;
    Connection*            mPrevPtr[1];
    Connection*            mNextPtr[1];

    friend class QCDLListOp<Connection>;

    static ServerLocation GetPeerLocation(
        const NetConnectionPtr& inConnPtr)
    {
        ServerLocation theLocation;
        if (inConnPtr) {
            inConnPtr->GetPeerLocation(theLocation);
        }
        return theLocation;
    }
    AuthContext& GetAuthContext()
        { return mImpl.GetAuthContext(); }
    int Authenticate(
        IOBuffer& inBuffer)
    {
        if (! mAuthenticateOpPtr) {
            return 0;
        }
        if (mAuthenticateOpPtr->doneFlag) {
            if (mConnectionPtr->GetFilter()) {
                HandleEvent(EVENT_NET_WROTE, &mConnectionPtr->GetOutBuffer());
            }
            return 0;
        }
        if (mAuthenticateOpPtr->contentBufPos <= 0) {
            GetAuthContext().Validate(*mAuthenticateOpPtr);
        }
        const int theRem = mAuthenticateOpPtr->Read(inBuffer);
        if (0 < theRem) {
            mConnectionPtr->SetMaxReadAhead(theRem);
            return theRem;
        }
        if (! inBuffer.IsEmpty() && mAuthenticateOpPtr->status == 0) {
            mAuthenticateOpPtr->status    = -EINVAL;
            mAuthenticateOpPtr->statusMsg = "out of order data received";
        }
        GetAuthContext().Authenticate(*mAuthenticateOpPtr, this, 0);
        if (mAuthenticateOpPtr->status == 0) {
            if (mAuthName.empty()) {
                mAuthName = mAuthenticateOpPtr->authName;
            } else if (! mAuthenticateOpPtr->authName.empty() &&
                    mAuthName != mAuthenticateOpPtr->authName) {
                mAuthenticateOpPtr->status    = -EINVAL;
                mAuthenticateOpPtr->statusMsg = "authenticated name mismatch";
            } else if (! mAuthenticateOpPtr->filter &&
                    mConnectionPtr->GetFilter()) {
                // An attempt to downgrade to clear text connection.
                mAuthenticateOpPtr->status    = -EINVAL;
                mAuthenticateOpPtr->statusMsg =
                    "clear text communication not allowed";
            }
        }
        mAuthenticateOpPtr->doneFlag = true;
        mAuthCtxUpdateCount = GetAuthContext().GetUpdateCount();
        KFS_LOG_STREAM(mAuthenticateOpPtr->status == 0 ?
            MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
            mPeerLocation           << " log receiver authentication"
            " type: "               << mAuthenticateOpPtr->sendAuthType <<
            " name: "               << mAuthenticateOpPtr->authName <<
            " filter: "             <<
                reinterpret_cast<const void*>(mAuthenticateOpPtr->filter) <<
            " session expires in: " <<
                (mAuthenticateOpPtr->sessionExpirationTime - TimeNow()) <<
            " response length: "    << mAuthenticateOpPtr->sendContentLen <<
            " msg: "                << mAuthenticateOpPtr->statusMsg <<
        KFS_LOG_EOM;
        mConnectionPtr->SetMaxReadAhead(mImpl.GetMaxReadAhead());
        SendResponse(*mAuthenticateOpPtr);
        return (mDownFlag ? -1 : 0);
    }
    void HandleAuthWrite()
    {
        if (! mAuthenticateOpPtr) {
            return;
        }
        if (mConnectionPtr->IsWriteReady()) {
            return;
        }
        if (mAuthenticateOpPtr->status != 0 ||
                mConnectionPtr->HasPendingRead() ||
                mConnectionPtr->IsReadPending()) {
            const string theMsg = mAuthenticateOpPtr->statusMsg;
            Error(theMsg.empty() ?
                (mConnectionPtr->HasPendingRead() ?
                    "out of order data received" :
                    "authentication error") :
                theMsg.c_str()
            );
            return;
        }
        if (mConnectionPtr->GetFilter()) {
            if (! mAuthenticateOpPtr->filter) {
                Error("no clear text communication allowed");
            }
            // Wait for [ssl] shutdown with the current filter to complete.
            return;
        }
        if (mAuthenticateOpPtr->filter) {
            NetConnection::Filter* const theFilterPtr =
                mAuthenticateOpPtr->filter;
            mAuthenticateOpPtr->filter = 0;
            string    theErrMsg;
            const int theErr = mConnectionPtr->SetFilter(
                theFilterPtr, &theErrMsg);
            if (theErr) {
                if (theErrMsg.empty()) {
                    theErrMsg = QCUtils::SysError(theErr < 0 ? -theErr : theErr);
                }
                Error(theErrMsg.c_str());
                return;
            }
        }
        mSessionExpirationTime = mAuthenticateOpPtr->sessionExpirationTime;
        MetaRequest::Release(mAuthenticateOpPtr);
        mAuthenticateOpPtr  = 0;
        KFS_LOG_STREAM_INFO << mPeerLocation <<
            (0 < mAuthCount ? " re-" : " ") <<
            "authentication [" << mAuthCount << "]"
            " complete:"
            " session expires in: " <<
                (mSessionExpirationTime - TimeNow()) << " sec." <<
        KFS_LOG_EOM;
        mAuthCount++;
        mRecursionCount++;
        MetaRequest* thePtr;
        while ((thePtr = mAuthPendingResponsesQueue.PopFront())) {
            SendResponse(*thePtr);
            MetaRequest::Release(thePtr);
        }
        if (--mRecursionCount <= 0) {
            mConnectionPtr->StartFlush();
        }
    }
    bool HandleSslShutdown()
    {
        NetConnection::Filter* theFilterPtr;
        if (mDownFlag ||
                ! mAuthenticateOpPtr ||
                ! mConnectionPtr->IsGood() ||
                ! (theFilterPtr = mConnectionPtr->GetFilter()) ||
                ! theFilterPtr->IsShutdownReceived()) {
            return false;
        }
        // Do not allow to shutdown filter with data in flight.
        if (mConnectionPtr->GetInBuffer().IsEmpty() &&
                mConnectionPtr->GetOutBuffer().IsEmpty()) {
            // Ssl shutdown from the other side.
            if (mConnectionPtr->Shutdown() != 0) {
                return false;
            }
            KFS_LOG_STREAM_DEBUG << mPeerLocation <<
                " log receiver: shutdown filter: " <<
                    reinterpret_cast<const void*>(
                        mConnectionPtr->GetFilter()) <<
            KFS_LOG_EOM;
            if (mConnectionPtr->GetFilter()) {
                return false;
            }
            HandleAuthWrite();
            return (! mDownFlag);
        }
        KFS_LOG_STREAM_ERROR << mPeerLocation <<
            " log receiver: "
            " invalid filter (ssl) shutdown: "
            " error: " << mConnectionPtr->GetErrorMsg() <<
            " read: "  << mConnectionPtr->GetNumBytesToRead() <<
            " write: " << mConnectionPtr->GetNumBytesToWrite() <<
        KFS_LOG_EOM;
        return false;
    }
    void HandleRead()
    {
        IOBuffer& theBuf = mConnectionPtr->GetInBuffer();
        if (mAuthenticateOpPtr) {
            Authenticate(theBuf);
            if (mAuthenticateOpPtr || mDownFlag) {
                return;
            }
        }
        if (0 < mBlockLength) {
            const int theRet = ReceiveBlock(theBuf);
            if (0 != theRet || mDownFlag) {
                return;
            }
        }
        bool theMsgAvailableFlag;
        int  theMsgLen = 0;
        while ((theMsgAvailableFlag = IsMsgAvail(&theBuf, &theMsgLen))) {
            const int theRet = HandleMsg(theBuf, theMsgLen);
            if (theRet < 0) {
                theBuf.Clear();
                Error(mAuthenticateOpPtr ?
                    (mAuthenticateOpPtr->statusMsg.empty() ?
                        "invalid authenticate message" :
                        mAuthenticateOpPtr->statusMsg.c_str()) :
                    "request parse error"
                );
                return;
            }
            if (0 < theRet || mAuthenticateOpPtr || mDownFlag) {
                return; // Need more data, or down
            }
            theMsgLen = 0;
        }
        if (mBlockLength < 0 && ! mAuthenticateOpPtr && ! mDownFlag &&
                MAX_RPC_HEADER_LEN < theBuf.BytesConsumable()) {
            Error("header size exceeds max allowed");
        }
    }
    void HandleCmdDone(
        MetaRequest& inReq)
    {
        if (mPendingOpsCount <= 0) {
            panic("invalid outstanding ops count");
            return;
        }
        if (inReq.next) {
            panic("invalid request next field");
        }
        mPendingOpsCount--;
        KFS_LOG_STREAM_DEBUG << mPeerLocation <<
            " pending: " << mPendingOpsCount <<
            " -seq "     << inReq.opSeqno <<
            " "          << inReq.Show() <<
            " down: "    << mDownFlag <<
            " auth: "    << (mReAuthPendingFlag || mAuthenticateOpPtr) <<
        KFS_LOG_EOM;
        SendAReAuthenticationAckIfNeeded();
        if ((mReAuthPendingFlag || mAuthenticateOpPtr) && ! mDownFlag) {
            mAuthPendingResponsesQueue.PushBack(inReq);
            return;
        }
        SendResponse(inReq);
        MetaRequest::Release(&inReq);
    }
    void SendResponse(
        MetaRequest& inReq)
    {
        if (mDownFlag) {
            return;
        }
        IOBuffer& theBuf = mConnectionPtr->GetOutBuffer();
        ReqOstream theStream(mOstream.Set(theBuf));
        inReq.response(theStream, theBuf);
        mOstream.Reset();
        if (mRecursionCount <= 0) {
            mConnectionPtr->StartFlush();
        }
    }
    bool ValidateSession()
    {
        if (GetAuthContext().IsAuthRequired() &&
                mSessionExpirationTime + mImpl.GetReAuthTimeout() * 3 / 2 <
                    TimeNow() ) {
            Error("authenticated session has expired");
            return false;
        }
        return true;
    }
    int HandleMsg(
        IOBuffer& inBuffer,
        int       inMsgLen)
    {
        const int kSeparatorLen = 4;
        const int kPrefixLen    = 2;
        if (kSeparatorLen + kPrefixLen < inMsgLen) {
            IOBuffer::BufPos  theLen       = inMsgLen - kSeparatorLen;
            const char* const theHeaderPtr = inBuffer.CopyOutOrGetBufPtr(
                mImpl.GetParseBufferPtr(), theLen);
            QCRTASSERT(inMsgLen - kSeparatorLen == theLen);
            const char*       thePtr    = theHeaderPtr;
            const char* const theEndPtr = thePtr + inMsgLen - kSeparatorLen;
            if ('l' == (*thePtr++ & 0xFF) && ':' == (*thePtr++ & 0xFF) &&
                    HexIntParser::Parse(
                        thePtr, theEndPtr - thePtr, mBlockLength) &&
                    HexIntParser::Parse(
                        thePtr, theEndPtr - thePtr, mBlockChecksum)) {
                if (IsAuthError()) {
                    return -1;
                }
                inBuffer.Consume(inMsgLen);
                return ReceiveBlock(inBuffer);
            }
        }
        MetaRequest* theReqPtr = 0;
        if (ParseLogRecvCommand(
                    inBuffer,
                    inMsgLen,
                    &theReqPtr,
                    mImpl.GetParseBufferPtr()) ||
                ! theReqPtr) {
            MetaRequest::Release(theReqPtr);
            const string thePrefix =
                mPeerLocation.ToString() + " invalid request: ";
            MsgLogLines(
                MsgLogger::kLogLevelERROR,
                thePrefix.c_str(),
                inBuffer,
                inMsgLen
            );
            return -1;
        }
        inBuffer.Consume(inMsgLen);
        theReqPtr->shortRpcFormatFlag = true;
        theReqPtr->clientIp           = mPeerLocation.hostname;
        KFS_LOG_STREAM_DEBUG << mPeerLocation <<
            " pending: " << mPendingOpsCount <<
            " +seq "     << theReqPtr->opSeqno <<
            " "          << theReqPtr->Show() <<
        KFS_LOG_EOM;
        if (META_AUTHENTICATE == theReqPtr->op) {
            mAuthenticateOpPtr = static_cast<MetaAuthenticate*>(theReqPtr);
            mReAuthPendingFlag = false;
            return Authenticate(inBuffer);
        }
        if (IsAuthError()) {
            MetaRequest::Release(theReqPtr);
            return -1;
        }
        if (! ValidateSession()) {
            MetaRequest::Release(theReqPtr);
            return -1;
        }
        if (mImpl.GetMaxPendingOpsCount() < mPendingOpsCount) {
            MetaRequest::Release(theReqPtr);
            Error("exceeded max pending RPC limit");
            return -1;
        }
        mPendingOpsCount++;
        if (mTransmitterId < 0) {
            mTransmitterId = MetaVrSM::GetNodeId(*theReqPtr);
        }
        theReqPtr->clnt = this;
        mImpl.Submit(*theReqPtr);
        return 0;
    }
    int ReceiveBlock(
        IOBuffer& inBuffer)
    {
        if (mBlockLength < 0) {
            Error("invalid negative block lenght");
            return -1;
        }
        if (! ValidateSession()) {
            return -1;
        }
        const int theRem = mBlockLength - inBuffer.BytesConsumable();
        if (0 < theRem) {
            mConnectionPtr->SetMaxReadAhead(
                max(theRem, mImpl.GetMaxReadAhead()));
            return theRem;
        }
        IOBuffer::BufPos theMaxHdrLen    =
            min(mBlockLength, (int)kMaxBlockHeaderLen);
        const char*      theStartPtr     = inBuffer.CopyOutOrGetBufPtr(
                mImpl.GetParseBufferPtr(), theMaxHdrLen);
        const char* const theEndPtr = theStartPtr + theMaxHdrLen;
        MetaVrLogSeq theBlockEndSeq;
        int          theBlockSeqLen = -1;
        const char* thePtr          = theStartPtr;
        int64_t     theFileSystemId = -1;
        if (! HexIntParser::Parse(
                    thePtr, theEndPtr - thePtr, theFileSystemId) ||
                ! theBlockEndSeq.Parse<HexIntParser>(
                    thePtr, theEndPtr - thePtr) ||
                ! HexIntParser::Parse(
                    thePtr, theEndPtr - thePtr, theBlockSeqLen) ||
                theBlockSeqLen < 0 ||
                (theBlockEndSeq.mLogSeq < theBlockSeqLen &&
                    0 < theBlockSeqLen)) {
            KFS_LOG_STREAM_ERROR << mPeerLocation <<
                " invalid block:"
                " fsid: "     << theFileSystemId <<
                " start: "    << mBlockStartSeq <<
                " / "         << theBlockEndSeq <<
                " end: "      << mBlockEndSeq <<
                " / "         << theBlockSeqLen <<
                " length: "   << mBlockLength <<
            KFS_LOG_EOM;
            MsgLogLines(MsgLogger::kLogLevelERROR,
                "invalid block: ", inBuffer, mBlockLength);
            Error("invalid block");
            return -1;
        }
        while (thePtr < theEndPtr && (*thePtr & 0xFF) <= ' ') {
            thePtr++;
        }
        if (theEndPtr <= thePtr && theMaxHdrLen < mBlockLength) {
            KFS_LOG_STREAM_ERROR << mPeerLocation <<
                " invalid block header:" << theBlockEndSeq  <<
                " length: "              << mBlockLength <<
            KFS_LOG_EOM;
            MsgLogLines(MsgLogger::kLogLevelERROR,
                "invalid block header: ", inBuffer, mBlockLength);
            Error("invalid block header");
            return -1;
        }
        const int      theHdrLen       = (int)(thePtr - theStartPtr);
        const Checksum theHdrChecksum  =
            ComputeBlockChecksum(&inBuffer, theHdrLen);
        mBlockLength -= inBuffer.Consume(theHdrLen);
        mBodyChecksum = ComputeBlockChecksum(&inBuffer, mBlockLength);
        const Checksum theChecksum     = ChecksumBlocksCombine(
            theHdrChecksum, mBodyChecksum, mBlockLength);
        if (theChecksum != mBlockChecksum) {
            KFS_LOG_STREAM_ERROR << mPeerLocation <<
                " received block checksum: " << theChecksum <<
                " expected: "                << mBlockChecksum <<
                " length: "                  << mBlockLength <<
            KFS_LOG_EOM;
            Error("block checksum mimatch");
            return -1;
        }
        if (theFileSystemId != mImpl.GetFileSystemId()) {
            KFS_LOG_STREAM_ERROR << mPeerLocation <<
                " file system id mismatch: " << theFileSystemId <<
                " expected: "                << mImpl.GetFileSystemId() <<
            KFS_LOG_EOM;
            Error("file system id mimatch");
            return -1;
        }
        if (! theBlockEndSeq.IsValid() ||
                    (mBlockEndSeq.IsValid() && theBlockEndSeq < mBlockEndSeq)) {
            KFS_LOG_STREAM_ERROR << mPeerLocation <<
                " invalid block:"
                " sequence: " << theBlockEndSeq <<
                " last: "     << mBlockEndSeq <<
                " length: "   << mBlockLength <<
            KFS_LOG_EOM;
            MsgLogLines(MsgLogger::kLogLevelERROR,
                "invalid block sequence: ", inBuffer, mBlockLength);
            Error("invalid block sequence");
            return -1;
        }
        mBlockEndSeq   = theBlockEndSeq;
        mBlockStartSeq = theBlockEndSeq;
        mBlockStartSeq.mLogSeq -= theBlockSeqLen;
        return ProcessBlock(inBuffer);
    }
    void Error(
        const char* inMsgPtr = 0)
    {
        if (mDownFlag) {
            return;
        }
        KFS_LOG_STREAM_ERROR << mPeerLocation <<
            " id: "    << mImpl.GetId() <<
            " error: " << (inMsgPtr ? inMsgPtr : "")  <<
            " closing connection"
            " last block end: " << mBlockEndSeq <<
            " socket error: "   << mConnectionPtr->GetErrorMsg() <<
        KFS_LOG_EOM;
        mConnectionPtr->Close();
        mDownFlag   = true;
        mIdSentFlag = false;
    }
    void SendAckSelf()
    {
        const bool kSendOnlyIfReAuthenticationNeededFlag = false;
        SendAckIf(kSendOnlyIfReAuthenticationNeededFlag);
    }
    void SendAReAuthenticationAckIfNeeded()
    {
        const bool kSendOnlyIfReAuthenticationNeededFlag = true;
        SendAckIf(kSendOnlyIfReAuthenticationNeededFlag);
    }
    void SendAckIf(
        bool inSendOnlyIfReAuthenticationNeededFlag)
    {
        if (mAuthenticateOpPtr || mReAuthPendingFlag) {
            return;
        }
        if (GetAuthContext().IsAuthRequired()) {
            uint64_t const theUpdateCount = GetAuthContext().GetUpdateCount();
            mReAuthPendingFlag = theUpdateCount != mAuthCtxUpdateCount ||
                mSessionExpirationTime < TimeNow() + mImpl.GetReAuthTimeout();
            if (mReAuthPendingFlag) {
                KFS_LOG_STREAM_INFO << mPeerLocation <<
                    " requesting re-authentication:"
                    " update count: " << theUpdateCount <<
                    " / "             << mAuthCtxUpdateCount <<
                    " expires in: "   << (mSessionExpirationTime - TimeNow()) <<
                    " timeout: "      << mImpl.GetReAuthTimeout() <<
                KFS_LOG_EOM;
                mSessionExpirationTime = min(
                    mSessionExpirationTime, (int64_t)TimeNow());
            }
        }
        if (inSendOnlyIfReAuthenticationNeededFlag && ! mReAuthPendingFlag) {
            return;
        }
        uint64_t theAckFlags = 0;
        if (mReAuthPendingFlag) {
            theAckFlags |= uint64_t(1) << kLogBlockAckReAuthFlagBit;
        }
        if (! mIdSentFlag) {
            theAckFlags |= uint64_t(1) << kLogBlockAckHasServerIdBit;
        }
        IOBuffer& theBuf = mConnectionPtr->GetOutBuffer();
        const int thePos = theBuf.BytesConsumable();
        ReqOstream theStream(mOstream.Set(theBuf));
        theStream << hex <<
            "A " << mImpl.GetLastLogSeq() <<
            " "  << theAckFlags <<
            " "  << mImpl.GetPrimaryId() <<
            " ";
        if (! mIdSentFlag) {
            mIdSentFlag = true;
            theStream << mImpl.GetId() << " ";
        }
        theStream.flush();
        const Checksum theChecksum = ComputeBlockChecksumAt(
            &theBuf, thePos, theBuf.BytesConsumable() - thePos);
        theStream << theChecksum;
        theStream << "\r\n\r\n";
        theStream.flush();
        if (mFirstAckFlag) {
            mFirstAckFlag = false;
        }
        KFS_LOG_STREAM_DEBUG << mPeerLocation <<
            " id: "      << mImpl.GetId() <<
            " ack: "     << mImpl.GetLastLogSeq()  <<
            " primary: " << mImpl.GetPrimaryId() <<
        KFS_LOG_EOM;
        if (mRecursionCount <= 0) {
            mConnectionPtr->StartFlush();
        }
    }
    int ProcessBlock(
        IOBuffer& inBuffer)
    {
        if (mBlockLength < 0) {
            Error("invalid negative block length");
            return -1;
        }
        MetaLogWriterControl* const theOpPtr = mImpl.GetBlockWriteOp(
                mBlockStartSeq, mBlockEndSeq);
        if (theOpPtr) {
            MetaLogWriterControl& theOp = *theOpPtr;
            const int theRem = 0 < mBlockLength ? LogReceiver::ParseBlockLines(
                inBuffer, mBlockLength, theOp, '/') : 0;
            if (0 < theRem) {
                panic("LogReceiver::Impl::Connection::ProcessBlock:"
                    " internal error");
                mImpl.Release(theOp);
                return -1;
            }
            if (theRem < 0) {
                const char* theMsgPtr =
                    "invalid log block format: no trailing /";
                KFS_LOG_STREAM_ERROR <<
                    theMsgPtr <<
                    " lines: " << theOp.blockLines.GetSize() <<
                KFS_LOG_EOM;
                mImpl.Release(theOp);
                Error(theMsgPtr);
                return -1;
            }
            theOp.blockChecksum = mBodyChecksum;
            theOp.blockStartSeq = mBlockStartSeq;
            theOp.blockEndSeq   = mBlockEndSeq;
            theOp.transmitterId = mTransmitterId;
            theOp.blockData.Move(&inBuffer, mBlockLength);
            mImpl.SubmitLogWrite(theOp);
        } else {
            KFS_LOG_STREAM_DEBUG <<
                "discarding block:"
                " [" << mBlockStartSeq <<
                ","  << mBlockEndSeq   <<
                "]"
                " length: "          << mBlockLength <<
                " last write: "      << mImpl.GetLastWriteLogSeq() <<
                " last log: "        << mImpl.GetLastLogSeq() <<
                " submitted write: " << mImpl.GetSubmittedWriteSeq() <<
            KFS_LOG_EOM;
            inBuffer.Consume(mBlockLength);
        }
        mBlockLength = -1;
        if (! theOpPtr) {
            SendAckSelf();
        }
        mConnectionPtr->SetMaxReadAhead(mImpl.GetMaxReadAhead());
        return 0;
    }
    void MsgLogLines(
        MsgLogger::LogLevel inLogLevel,
        const char*         inPrefixPtr,
        IOBuffer&           inBuffer,
        int                 inBufLen,
        int                 inMaxLines = 64)
    {
        const char* const thePrefixPtr = inPrefixPtr ? inPrefixPtr : "";
        istream&          theStream    = mIStream.Set(inBuffer, inBufLen);
        int               theRemCnt    = inMaxLines;
        string            theLine;
        while (--theRemCnt >= 0 && getline(theStream, theLine)) {
            string::iterator theIt = theLine.end();
            if (theIt != theLine.begin() && *--theIt <= ' ') {
                theLine.erase(theIt);
            }
            KFS_LOG_STREAM(inLogLevel) <<
                thePrefixPtr << theLine <<
            KFS_LOG_EOM;
        }
        mIStream.Reset();
    }
    bool IsAuthError()
    {
        if (mAuthName.empty() && GetAuthContext().IsAuthRequired()) {
            Error("autentication required");
            return true;
        }
        return false;
    }
private:
    Connection(
        const Connection& inConnection);
    Connection& operator=(
        const Connection& inConnection);
};

    /* virtual */ KfsCallbackObj*
LogReceiver::Impl::CreateKfsCallbackObj(
    NetConnectionPtr& inConnectionPtr)
{
    if (! inConnectionPtr || ! inConnectionPtr->IsGood()) {
        return 0;
    }
    if (min(mMaxSocketsCount, mMaxConnectionCount) <= mConnectionCount) {
        KFS_LOG_STREAM_ERROR <<
            "log receiver: reached connections limit"
            " of: "                 << mMaxConnectionCount <<
            " and sockets: "        << mMaxSocketsCount <<
            " connections: "        << mConnectionCount <<
            " closing connection: " << inConnectionPtr->GetPeerName() <<
        KFS_LOG_EOM;
        return 0;
    }
    return new Connection(*this, inConnectionPtr);
}

    void
LogReceiver::Impl::New(
    LogReceiver::Impl::Connection& inConnection)
{
    mConnectionCount++;
    List::PushBack(mConnectionsHeadPtr, inConnection);
    if (mConnectionCount <= 0) {
        panic("LogReceiver::Impl::New: invalid connections count");
    }
}

    void
LogReceiver::Impl::Done(
    LogReceiver::Impl::Connection& inConnection)
{
    if (mConnectionCount <= 0) {
        panic("LogReceiver::Impl::Done: invalid connections count");
    }
    List::Remove(mConnectionsHeadPtr, inConnection);
    mConnectionCount--;
    if (mDeleteFlag && mConnectionCount <= 0) {
        delete this;
    }
}

    void
LogReceiver::Impl::ShutdownSelf()
{
    if (mAcceptorPtr) {
        mAcceptorPtr->GetNetManager().UnRegisterTimeoutHandler(this);
    }
    delete mAcceptorPtr;
    mAcceptorPtr = 0;
    List::Iterator theIt(mConnectionsHeadPtr);
    Connection* thePtr;
    while (0 < mConnectionCount && (thePtr = theIt.Next())) {
        thePtr->Close();
    }
    mAckBroadcastFlag = false;
    if (0 < mConnectionCount) {
        mResponseQueue.PushBack(mPendingResponseQueue);
        Timeout();
    }
    ClearQueues();
    mInFlightWriteCount = 0;
    mWakerPtr = 0;
}

    void
LogReceiver::Impl::Shutdown()
{
    if (mAcceptorPtr) {
        NetErrorSimulatorConfigure(mAcceptorPtr->GetNetManager(), 0);
    }
    ShutdownSelf();
    mAuthContext.Clear();
}

    void
LogReceiver::Impl::BroadcastAck()
{
    List::Iterator theIt(mConnectionsHeadPtr);
    Connection* thePtr;
    while (0 < mConnectionCount && (thePtr = theIt.Next())) {
        thePtr->SendAck();
    }
}

LogReceiver::LogReceiver()
    : mImpl(*(new Impl()))
{
}

LogReceiver::~LogReceiver()
{
    mImpl.Delete();
}

    bool
LogReceiver::SetParameters(
    const char*       inPrefixPtr,
    const Properties& inParameters)
{
    return mImpl.SetParameters(inPrefixPtr, inParameters);
}

    int
LogReceiver::Start(
    NetManager&         inNetManager,
    LogReceiver::Waker& inWaker,
    const MetaVrLogSeq& inCommittedLogSeq,
    const MetaVrLogSeq& inLastLogSeq,
    int64_t             inFileSystemId,
    vrNodeId_t          inNodeId,
    int                 inMaxSocketsCount)
{
    return mImpl.Start(inNetManager, inWaker,
        inCommittedLogSeq, inLastLogSeq, inFileSystemId, inNodeId,
        inMaxSocketsCount);
}

    void
LogReceiver::Shutdown()
{
    mImpl.Shutdown();
}

    bool
LogReceiver::Dispatch()
{
    return mImpl.Dispatch();
}

    ServerLocation
LogReceiver::GetListenerAddress() const
{
    return mImpl.GetListenerAddress();
}

    int
LogReceiver::ParseBlockLines(
    const IOBuffer&       inBuffer,
    int                   inLength,
    MetaLogWriterControl& inOp,
    int                   inLastSym)
{
    MetaLogWriterControl::Lines& theLines = inOp.blockLines;
    theLines.Clear();
    int  theRem        = inLength;
    bool theAppendFlag = false;
    int  theLastSym    = 0;
    for (IOBuffer::iterator theIt = inBuffer.begin();
            0 < theRem && theIt != inBuffer.end();
            ++theIt) {
        const char* const theStartPtr = theIt->Consumer();
        const char* const theEndPtr   =
            min(theIt->Producer(), theStartPtr + theRem);
        if (theEndPtr <= theStartPtr) {
            continue;
        }
        theRem -= theEndPtr - theStartPtr;
        const char* thePtr  = theStartPtr;
        const char* theNPtr;
        while (thePtr < theEndPtr &&
                (theNPtr = reinterpret_cast<const char*>(
                    memchr(thePtr, '\n', theEndPtr - thePtr)))) {
            ++theNPtr;
            const int theLen = (int)(theNPtr - thePtr);
            if (theAppendFlag) {
                theAppendFlag = false;
                theLines.Back() += theLen;
            } else {
                theLines.Append(theLen);
            }
            thePtr = theNPtr;
        }
        if (thePtr < theEndPtr) {
            const int theLen = (int)(theEndPtr - thePtr);
            if (theAppendFlag) {
                theLines.Back() += theLen;
            } else {
                theLines.Append(theLen);
            }
            theAppendFlag = true;
            theLastSym = theEndPtr[-1] & 0xFF;
        }
    }
    return (((theAppendFlag && theLastSym == inLastSym) ||
        (! theAppendFlag && '\n' == inLastSym)) ? theRem : -EINVAL);
}

} // namespace KFS
