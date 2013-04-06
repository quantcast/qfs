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
#include "ClientSM.h"

namespace KFS
{

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

        void Clear() {
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
        }
    };
    ClientManager()
        : mAcceptor(0), mIoTimeoutSec(-1), mIdleTimeoutSec(-1), mCounters() {
        mCounters.Clear();
    }
    void SetTimeouts(int ioTimeoutSec, int idleTimeoutSec)  {
        mIoTimeoutSec = ioTimeoutSec;
        mIdleTimeoutSec = idleTimeoutSec;
    }
    virtual ~ClientManager() {
        assert(mCounters.mClientCount == 0);
        delete mAcceptor;
    };
    bool BindAcceptor(int port);
    bool StartListening();
    KfsCallbackObj *CreateKfsCallbackObj(NetConnectionPtr &conn) {
        ClientSM *clnt = new ClientSM(conn);
        assert(mCounters.mClientCount >= 0);
        mCounters.mAcceptCount++;
        mCounters.mClientCount++;
        return clnt;
    }
    void Remove(ClientSM * /* clnt */) {
        assert(mCounters.mClientCount > 0);
        mCounters.mClientCount--;
    }
    int GetIdleTimeoutSec() const {
        return mIdleTimeoutSec;
    }
    int GetIoTimeoutSec() const {
        return mIoTimeoutSec;
    }
    void BadRequest() {
        mCounters.mBadRequestCount++;
    }
    void BadRequestHeader() {
        mCounters.mBadRequestHeaderCount++;
    }
    void RequestLengthExceeded() {
        mCounters.mRequestLengthExceededCount++;
    }
    void IdleTimeout() {
        mCounters.mIdleTimeoutCount++;
    }
    void WaitTimeExceeded() {
        mCounters.mWaitTimeExceededCount++;
    }
    void RequestDone(int64_t requestTimeMicroSecs, const KfsOp& op) {
        const int64_t tm = requestTimeMicroSecs > 0 ? requestTimeMicroSecs : 0;
        switch (op.op) {
            case CMD_READ:
                mCounters.mReadRequestCount++;
                mCounters.mReadRequestTimeMicroSecs += tm;
                if (op.status >= 0) {
                    const int64_t len =
                        static_cast<const ReadOp&>(op).numBytesIO;
                    if (len > 0) {
                        mCounters.mReadRequestBytes += len;
                    }
                } else {
                    mCounters.mReadRequestErrors++;
                }
            break;
            case CMD_WRITE_PREPARE:
                mCounters.mWriteRequestCount++;
                mCounters.mWriteRequestTimeMicroSecs += tm;
                if (op.status >= 0) {
                    const int64_t len =
                        static_cast<const WritePrepareOp&>(op).numBytes;
                    if (len > 0) {
                        mCounters.mWriteRequestBytes += len;
                    }
                } else {
                    mCounters.mWriteRequestErrors++;
                }
            break;
            case CMD_RECORD_APPEND:
                mCounters.mAppendRequestCount++;
                mCounters.mAppendRequestTimeMicroSecs += tm;
                if (op.status >= 0) {
                    const int64_t len =
                        static_cast<const RecordAppendOp&>(op).numBytes;
                    if (len > 0) {
                        mCounters.mAppendRequestBytes += len;
                    }
                } else {
                    mCounters.mAppendRequestErrors++;
                }
            break;
            default:
                mCounters.mOtherRequestCount++;
                mCounters.mOtherRequestTimeMicroSecs += tm;
                if (op.status < 0) {
                    mCounters.mOtherRequestErrors++;
                }
            break;
        }
    }
    void GetCounters(Counters& counters) {
        counters = mCounters;
    }
    int GetPort() const
        { return (mAcceptor ? mAcceptor->GetPort() : -1); }
private:
    Acceptor* mAcceptor;
    int       mIoTimeoutSec;
    int       mIdleTimeoutSec;
    Counters  mCounters;
private:
    // No copy.
    ClientManager(const ClientManager&);
    ClientManager& operator=(const ClientManager&);
};

extern ClientManager gClientManager;

}

#endif // _CLIENTMANAGER_H
