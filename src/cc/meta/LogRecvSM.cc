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

#include "AuthContext.h"
#include "MetaRequest.h"
#include "util.h"

#include "common/kfstypes.h"
#include "common/MsgLogger.h"

#include "kfsio/NetManager.h"
#include "kfsio/NetConnection.h"
#include "kfsio/SslFilter.h"
#include "kfsio/checksum.h"

#include "qcdio/QCUtils.h"
#include "qcdio/qcdebug.h"

#include <string>

#include <time.h>

namespace KFS
{

class LogRecvSM
{
    class Impl;
};

class LogRecvSM::Impl :
    public KfsCallbackObj,
    public SslFilterVerifyPeer
{
public:
    Impl(
        const NetConnectionPtr& inConnectionPtr)
        : KfsCallbackObj(),
          SslFilterVerifyPeer(),
          mAuthName(),
          mSessionExpirationTime(0),
          mConnectionPtr(inConnectionPtr),
          mAuthenticateOpPtr(0),
          mAuthCount(0),
          mAuthCtxUpdateCount(0),
          mRecursionCount(0),
          mBlockLength(-1),
          mDownFlag(false),
          mIStream()
    {
        if (! mConnectionPtr || ! mConnectionPtr->IsGood()) {
            panic("LogRecvSM::Impl: invalid connection poiner");
        }
    }
    ~Impl()
    {
        MetaRequest::Release(mAuthenticateOpPtr);
    }
    virtual bool Verify(
        string&       ioFilterAuthName,
        bool          inPreverifyOkFlag,
        int           inCurCertDepth,
        const string& inPeerName,
        int64_t       inEndTime,
        bool          inEndTimeValidFlag)
    {
        KFS_LOG_STREAM_DEBUG << GetPeerName() <<
            " log auth. verify:" <<
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
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
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
            case EVENT_NET_ERROR:
                Error();
                break;
            case EVENT_TIMEOUT:
                Error("connection timed out");
                break;
            default:
                panic("LogRecvSM: unexpected event");
                break;
        }
        mRecursionCount--;
        QCASSERT(0 <= mRecursionCount);
        return 0;
    }
private:
    string                 mAuthName;
    int64_t                mSessionExpirationTime;
    NetConnectionPtr const mConnectionPtr;
    MetaAuthenticate*      mAuthenticateOpPtr;
    int64_t                mAuthCount;
    uint64_t               mAuthCtxUpdateCount;
    int                    mRecursionCount;
    int                    mBlockLength;
    int32_t                mBlockChecksum;
    bool                   mDownFlag;
    IOBuffer::IStream      mIStream;
    char                   mParseBuffer[MAX_RPC_HEADER_LEN];

    string GetPeerName()
        { return mConnectionPtr->GetPeerName(); }
    time_t TimeNow()
        { return mConnectionPtr->TimeNow(); }
    AuthContext& GetAuthContext();
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
        mAuthenticateOpPtr->clnt     = this;
        mAuthenticateOpPtr->doneFlag = true;
        mAuthCtxUpdateCount = GetAuthContext().GetUpdateCount();
        KFS_LOG_STREAM(mAuthenticateOpPtr->status == 0 ?
            MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
            GetPeerName()           << " log receiver authentication"
            " type: "               << mAuthenticateOpPtr->responseAuthType <<
            " name: "               << mAuthenticateOpPtr->authName <<
            " filter: "             <<
                reinterpret_cast<const void*>(mAuthenticateOpPtr->filter) <<
            " session expires in: " <<
                (mAuthenticateOpPtr->sessionExpirationTime - TimeNow()) <<
            " response length: "    << mAuthenticateOpPtr->responseContentLen <<
            " msg: "                << mAuthenticateOpPtr->statusMsg <<
        KFS_LOG_EOM;
        const int theRet = mAuthenticateOpPtr->status != 0 ?  -1 : 0;
        if (theRet != 0) {
            Error("authentication failure");
        }
        MetaRequest::Release(mAuthenticateOpPtr);
        mAuthenticateOpPtr = 0;
        return theRet;
    }
    void HandleAuthWrite()
    {
        if (! mAuthenticateOpPtr) {
            return;
        }
        if (! mConnectionPtr) {
            MetaRequest::Release(mAuthenticateOpPtr);
            mAuthenticateOpPtr = 0;
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
        KFS_LOG_STREAM_INFO << GetPeerName() <<
            (0 < mAuthCount ? " re-" : " ") <<
            "authentication complete:"
            " session expires in: " <<
                (mSessionExpirationTime - TimeNow()) << " sec." <<
        KFS_LOG_EOM;
    }
    void HandleRead()
    {
        IOBuffer& theBuf = mConnectionPtr->GetInBuffer();
        if (mAuthenticateOpPtr) {
            Authenticate(theBuf);
            if (mAuthenticateOpPtr) {
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
    }
    int HandleMsg(
        IOBuffer& inBuffer,
        int       inMsgLen)
    {
        const int kSeparatorLen = 4;
        const int kPrefixLen    = 2;
        if (kSeparatorLen + kPrefixLen < inMsgLen) {
            int               theLen       = inMsgLen - kSeparatorLen;
            const char* const theHeaderPtr = inBuffer.CopyOutOrGetBufPtr(
                mParseBuffer, theLen);
            QCRTASSERT(inMsgLen - kSeparatorLen == theLen);
            const char*       thePtr    = theHeaderPtr;
            const char* const theEndPtr = thePtr + inMsgLen - kSeparatorLen;
            if ('l' == (*thePtr++ & 0xFF) && ':' == (*thePtr++ & 0xFF) &&
                    HexIntParser::Parse(
                        thePtr, theEndPtr - thePtr, mBlockLength) &&
                    HexIntParser::Parse(
                        thePtr, theEndPtr - thePtr, mBlockChecksum)) {
                if (mBlockLength < 0) {
                    Error("invalid negative block lenght");
                    return -1;
                }
                inBuffer.Consume(inMsgLen);
                return ReceiveBlock(inBuffer);
            }
        }
        MetaRequest* theReqPtr = 0;
        if (ParseLogRecvCommand(
                    inBuffer, inMsgLen, &theReqPtr, mParseBuffer) ||
                ! theReqPtr ||
                META_AUTHENTICATE != theReqPtr->op) {
            MetaRequest::Release(theReqPtr);
            const string thePrefix = GetPeerName() + " invalid request: ";
            MsgLogLines(
                MsgLogger::kLogLevelERROR,
                thePrefix.c_str(),
                inBuffer,
                inMsgLen
            );
            return -1;
        }
        inBuffer.Consume(inMsgLen);
        if (META_AUTHENTICATE == theReqPtr->op) {
            mAuthenticateOpPtr = static_cast<MetaAuthenticate*>(theReqPtr);
            return Authenticate(inBuffer);
        }
        return 0;
    }
    int ReceiveBlock(
        IOBuffer& inBuffer)
    {
        if (mBlockLength < 0) {
            return -1;
        }
        const int theRem = mBlockLength - inBuffer.BytesConsumable();
        if (0 < theRem) {
            return theRem;
        }
        const uint32_t theChecksum = ComputeBlockChecksum(
            &inBuffer, mBlockLength, kKfsNullChecksum);
        if (theChecksum != mBlockChecksum) {
            KFS_LOG_STREAM_ERROR << GetPeerName() <<
                " received block checksum: " << theChecksum <<
                " expected: "                << mBlockChecksum <<
                " length: "                  << mBlockLength <<
            KFS_LOG_EOM;
            Error("block checksum mimatch");
            return -1;
        }
        SendAck();
        if (mDownFlag) {
            return -1;
        }
        return ProcessBlock(inBuffer);
    }
    void Error(
        const char* inMsgPtr = 0)
    {
        if (mDownFlag) {
            return;
        }
    }
    void SendAck()
    {
    }
    int ProcessBlock(
        IOBuffer& inBuffer)
    {
        if (mBlockLength < 0) {
            return -1;
        }
        inBuffer.Consume(mBlockLength);
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
};

} // namespace KFS
