//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/28
// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
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

#ifndef _CLIENTMANAGER_H
#define _CLIENTMANAGER_H

#include <cassert>
#include <inttypes.h>
#include "kfsio/Acceptor.h"
#include "KfsOps.h"

class QCMutex;

namespace KFS
{

class Properties;
class ClientSM;
class ClientThread;

// Client connection listener.
class ClientManager : public IAcceptorOwner {
public:
    struct Counters
    {
        typedef int64_t Counter;

        Counter mAcceptCount;
        Counter mClientCount;
        Counter mBadRequestCount;
        Counter mBadRequestHeaderCount;
        Counter mRequestLengthExceededCount;
        Counter mIdleTimeoutCount;
        Counter mOtherRequestCount;
        Counter mOtherRequestTimeMicroSecs;
        Counter mOtherRequestErrors;
        Counter mReadRequestCount;
        Counter mReadRequestTimeMicroSecs;
        Counter mReadRequestBytes;
        Counter mReadRequestErrors;
        Counter mWriteRequestCount;
        Counter mWriteRequestTimeMicroSecs;
        Counter mWriteRequestBytes;
        Counter mWriteRequestErrors;
        Counter mAppendRequestCount;
        Counter mAppendRequestTimeMicroSecs;
        Counter mAppendRequestBytes;
        Counter mAppendRequestErrors;
        Counter mWaitTimeExceededCount;
        Counter mDiscardedBytesCount;
        Counter mOverClientLimitCount;

        void Clear()
        {
            mAcceptCount                = 0;
            mClientCount                = 0;
            mBadRequestCount            = 0;
            mBadRequestHeaderCount      = 0;
            mRequestLengthExceededCount = 0;
            mIdleTimeoutCount           = 0;
            mOtherRequestCount          = 0;
            mOtherRequestTimeMicroSecs  = 0;
            mOtherRequestErrors         = 0;
            mReadRequestCount           = 0;
            mReadRequestTimeMicroSecs   = 0;
            mReadRequestBytes           = 0;
            mReadRequestErrors          = 0;
            mWriteRequestCount          = 0;
            mWriteRequestTimeMicroSecs  = 0;
            mWriteRequestBytes          = 0;
            mWriteRequestErrors         = 0;
            mAppendRequestCount         = 0;
            mAppendRequestTimeMicroSecs = 0;
            mAppendRequestBytes         = 0;
            mAppendRequestErrors        = 0;
            mWaitTimeExceededCount      = 0;
            mDiscardedBytesCount        = 0;
            mOverClientLimitCount       = 0;
        }
    };
    ClientManager();
    virtual ~ClientManager();
    bool BindAcceptor(
        const ServerLocation& clientListener,
        bool                  ipV6OnlyFlag,
        int                   inThreadCount,
        int                   inFirstCpuIdx,
        QCMutex*&             outMutexPtr);
    bool StartListening();
    virtual KfsCallbackObj* CreateKfsCallbackObj(
        NetConnectionPtr& inConnPtr);
    void GetCounters(
        Counters& outCounters) const;
    bool SetParameters(
        const char*       inParamsPrefixPtr,
        const Properties& inProps,
        bool              inAuthEnabledFlag);
    int GetIdleTimeoutSec() const
        { return mIdleTimeoutSec; }
    int GetIoTimeoutSec() const
        { return mIoTimeoutSec; }
    int GetPort() const
        { return (mAcceptorPtr ? mAcceptorPtr->GetPort() : -1); }
    void Stop();
    void Remove(
        ClientSM* /* inClientPtr */)
    {
        assert(mCounters.mClientCount > 0);
        mCounters.mClientCount--;
    }
    void BadRequest()
        { mCounters.mBadRequestCount++; }
    void BadRequestHeader()
        { mCounters.mBadRequestHeaderCount++; }
    void RequestLengthExceeded()
        { mCounters.mRequestLengthExceededCount++; }
    void IdleTimeout()
        { mCounters.mIdleTimeoutCount++; }
    void WaitTimeExceeded()
        { mCounters.mWaitTimeExceededCount++; }
    void RequestDone(
        int64_t      inRequestTimeMicroSecs,
        const KfsOp& inOp)
    {
        const int64_t theTime =
            inRequestTimeMicroSecs > 0 ? inRequestTimeMicroSecs : 0;
        switch (inOp.op) {
            case CMD_READ:
                mCounters.mReadRequestCount++;
                mCounters.mReadRequestTimeMicroSecs += theTime;
                if (inOp.status >= 0) {
                    const int64_t theLen =
                        static_cast<const ReadOp&>(inOp).numBytesIO;
                    if (theLen > 0) {
                        mCounters.mReadRequestBytes += theLen;
                    }
                } else {
                    mCounters.mReadRequestErrors++;
                }
            break;
            case CMD_WRITE_PREPARE:
                mCounters.mWriteRequestCount++;
                mCounters.mWriteRequestTimeMicroSecs += theTime;
                if (inOp.status >= 0) {
                    const int64_t theLen =
                        static_cast<const WritePrepareOp&>(inOp).numBytes;
                    if (theLen > 0) {
                        mCounters.mWriteRequestBytes += theLen;
                    }
                } else {
                    mCounters.mWriteRequestErrors++;
                }
            break;
            case CMD_RECORD_APPEND:
                mCounters.mAppendRequestCount++;
                mCounters.mAppendRequestTimeMicroSecs += theTime;
                if (inOp.status >= 0) {
                    const int64_t theLen =
                        static_cast<const RecordAppendOp&>(inOp).numBytes;
                    if (theLen > 0) {
                        mCounters.mAppendRequestBytes += theLen;
                    }
                } else {
                    mCounters.mAppendRequestErrors++;
                }
            break;
            default:
                mCounters.mOtherRequestCount++;
                mCounters.mOtherRequestTimeMicroSecs += theTime;
                if (inOp.status < 0) {
                    mCounters.mOtherRequestErrors++;
                }
            break;
        }
    }
    void Discarded(
        int inByteCount)
    {
        if (inByteCount > 0) {
            mCounters.mDiscardedBytesCount += inByteCount;
        }
    }
    int GetClientThreadCount() const
        { return mThreadCount; }
    const QCMutex* GetMutexPtr() const;
    ClientThread* GetCurrentClientThreadPtr();
    ClientThread* GetNextClientThreadPtr();
    ClientThread* GetClientThread(
        int inIdx);
    bool IsAuthEnabled() const;
    bool SetParameters(
        const char*       inParamsPrefixPtr,
        const Properties& inProps,
        bool              inAuthEnabledFlag,
        int               inMaxClientCount);
    void Shutdown();
    int GetMaxClientCount() const
        { return mMaxClientCount; }
private:
    class Auth;

    Acceptor*     mAcceptorPtr;
    int           mIoTimeoutSec;
    int           mIdleTimeoutSec;
    int           mMaxClientCount;
    Counters      mCounters;
    Auth&         mAuth;
    int           mCurThreadIdx;
    int           mFirstClientThreadIndex;
    int           mThreadCount;
    ClientThread* mThreadsPtr;

private:
    // No copy.
    ClientManager(const ClientManager&);
    ClientManager& operator=(const ClientManager&);
};

extern ClientManager gClientManager;

}

#endif // _CLIENTMANAGER_H
