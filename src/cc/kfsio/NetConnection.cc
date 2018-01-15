//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/14
// Author: Sriram Rao
//
// Copyright 2008-2011,2016 Quantcast Corporation. All rights reserved.
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
// Network connection implementation.
//
//----------------------------------------------------------------------------

#include "Globals.h"
#include "NetConnection.h"
#include "common/kfsdecls.h"
#include "common/MsgLogger.h"
#include "qcdio/QCUtils.h"

#include <cerrno>
#include <time.h>

namespace KFS
{

using namespace KFS::libkfsio;

#ifndef NET_CONNECTION_LOG_STREAM_DEBUG
#define NET_CONNECTION_LOG_STREAM_DEBUG \
    KFS_LOG_STREAM_DEBUG << "netconn: " << (mSock ? mSock->GetFd() : -1) << " "
#endif

inline bool
IsFatalError(int err)
{
    return (err != EAGAIN && err != EWOULDBLOCK && err != EINTR);
}

inline void
NetConnection::SetLastError(int status)
{
    if (0 != mLastError) {
        return;
    }
    if (mFilter) {
        const int err = mFilter->GetErrorCode();
        if (0 != err) {
            mLastError = err;
        }
        return;
    }
    mLastError = status;
}

/* static */ NetConnectionPtr
NetConnection::Connect(
    NetManager&           inNetManager,
    const ServerLocation& inLocation,
    KfsCallbackObj*       inCbObj,
    Filter*               inFilter,
    bool                  inEnableReadIfOverloadedFlag,
    int                   inMaxReadAhead,
    int                   inInactivityTimeoutSecs,
    NetConnectionPtr&     ioConnection)
{
    const bool listenOnlyFlag = false;
    const bool ownsSocketFlag = true;
    NetConnectionPtr ret;
    ret.reset(new NetConnection(
        new TcpSocket(), inCbObj, listenOnlyFlag, ownsSocketFlag, inFilter));
    if (inEnableReadIfOverloadedFlag) {
        ret->mNetManagerEntry.EnableReadIfOverloaded();
    }
    ret->mMaxReadAhead = inMaxReadAhead;
    ret->mInactivityTimeoutSecs = inInactivityTimeoutSecs;
    ioConnection = ret;
    inNetManager.AddConnection(ret, &inLocation);
    return ret;
}

void
NetConnection::NameResolutionDone(const ServerLocation& loc,
    int status, const char* errMsg)
{
    if (mSock) {
        NET_CONNECTION_LOG_STREAM_DEBUG <<
            "resolved: "   << loc <<
            " status: "    << status <<
            (errMsg ? " " : "") << (errMsg ? errMsg : "") <<
        KFS_LOG_EOM;
        if (0 != status) {
            mLastErrorMsg = loc.hostname;
            if (mLastErrorMsg.empty()) {
                mLastErrorMsg = "host";
            }
            if (errMsg && *errMsg) {
                mLastErrorMsg += ": ";
                mLastErrorMsg += errMsg;
            } else {
                mLastErrorMsg +=  ": name resolution error";
            }
            if (0 < status) {
                status =  -status;
            }
            mLastError = status;
            Close();
            mCallbackObj->HandleEvent(EVENT_NET_ERROR, &status);
        } else {
            const bool nonBlockingFlag = true;
            int        res             = mSock->Connect(loc, nonBlockingFlag);
            if (0 != res && -EINPROGRESS != res) {
                mLastError = res;
                Close();
                mCallbackObj->HandleEvent(EVENT_NET_ERROR, &res);
            } else {
                if (-EINPROGRESS == res) {
                    mNetManagerEntry.SetConnectPending(true);
                    mTryWrite = false;
                }
                if (mFilter) {
                    mLastError = mFilter->Attach(*this, mSock, &mLastErrorMsg);
                    if (0 != mLastError) {
                        res = mLastError;
                        Close();
                        mCallbackObj->HandleEvent(EVENT_NET_ERROR, &res);
                    }
                }
                if (mPendingShutdownFlag && 0 == mLastError &&
                        0 != (mLastError = Shutdown(
                            mPendingShutdownReadFlag,
                            mPendingShutdownWriteFlag))) {
                    Close();
                    mCallbackObj->HandleEvent(EVENT_NET_ERROR, &res);
                }
            }
        }
    }
    Update();
}

void
NetConnection::HandleReadEvent(int maxAcceptsPerRead /* = 1 */)
{
    if (! IsGood()) {
        NET_CONNECTION_LOG_STREAM_DEBUG << "read event ignored: fd closed" <<
        KFS_LOG_EOM;
    } else if (mListenOnly) {
        int i = 0;
        do {
            int err = 0;
            TcpSocket* const sock = mSock->Accept(&err);
            if (sock) {
                NetConnectionPtr conn(new NetConnection(sock, 0));
                conn->mTryWrite = true; // Connected, and good to write.
                mCallbackObj->HandleEvent(EVENT_NEW_CONNECTION, &conn);
                if (conn) {
                    conn->Update();
                }
            } else {
                if (i == 0 || IsFatalError(err)) {
                    NET_CONNECTION_LOG_STREAM_DEBUG <<
                        " accept failure: " << QCUtils::SysError(err) <<
                        " open fd:"
                        " net: "  << globals().ctrOpenDiskFds.GetValue() <<
                        " disk: " << globals().ctrOpenNetFds.GetValue() <<
                    KFS_LOG_EOM;
                    SetLastError(err);
                }
                break;
            }
        } while (++i < maxAcceptsPerRead && IsGood());
    } else if (WantRead()) {
        const int nread = mFilter ?
            mFilter->Read(*this, *mSock, mInBuffer, mMaxReadAhead) :
            mInBuffer.Read(mSock->GetFd(), mMaxReadAhead);
        if (nread <= 0 && IsFatalError(-nread)) {
            if (nread != 0) {
                GetErrorMsg();
                IsAuthFailure();
            }
            NET_CONNECTION_LOG_STREAM_DEBUG <<
                "read: " << (nread == 0 ? "EOF" : QCUtils::SysError(-nread)) <<
                (mAuthFailureFlag ? " auth failure" : "") <<
                (mLastErrorMsg.empty() ? "" : " ") << mLastErrorMsg <<
            KFS_LOG_EOM;
            if (nread != 0) {
                SetLastError(nread);
                Close();
            }
            int err = nread;
            mCallbackObj->HandleEvent(EVENT_NET_ERROR, &err);
        } else if (nread > 0) {
            mCallbackObj->HandleEvent(EVENT_NET_READ, &mInBuffer);
        }
    }
    Update();
}

void
NetConnection::HandleWriteEvent()
{
    const bool wasConnectPending = mNetManagerEntry.IsConnectPending();
    mNetManagerEntry.SetConnectPending(false);
    int nwrote = 0;
    if (IsGood()) {
        mTryWrite = false; // Reset to prevent possible recursion.
        bool forceInvokeErrHandlerFlag = false;
        nwrote = WantWrite() ? (mFilter ?
            mFilter->Write(*this, *mSock, mOutBuffer,
                forceInvokeErrHandlerFlag) :
            mOutBuffer.Write(mSock->GetFd())
        ) : 0;
        if (nwrote < 0 && IsFatalError(-nwrote)) {
            GetErrorMsg();
            IsAuthFailure();
            NET_CONNECTION_LOG_STREAM_DEBUG <<
                "write: error: " << QCUtils::SysError(-nwrote) <<
                (mAuthFailureFlag ? " auth failure" : "") <<
                (mLastErrorMsg.empty() ? "" : " ") << mLastErrorMsg <<
            KFS_LOG_EOM;
            SetLastError(nwrote);
            Close();
            mCallbackObj->HandleEvent(EVENT_NET_ERROR, &nwrote);
        } else if (forceInvokeErrHandlerFlag) {
            NET_CONNECTION_LOG_STREAM_DEBUG <<
                "write: forcing error handler invocation, wrote: " << nwrote <<
            KFS_LOG_EOM;
            mCallbackObj->HandleEvent(EVENT_NET_ERROR, 0);
        } else if (nwrote > 0 || wasConnectPending) {
            mCallbackObj->HandleEvent(EVENT_NET_WROTE, &mOutBuffer);
        }
    }
    mTryWrite = ! WantWrite();
    Update(nwrote != 0);
}

void
NetConnection::HandleErrorEvent()
{
    if (IsGood()) {
        GetErrorMsg();
        IsAuthFailure();
        int status = mAuthFailureFlag ? -EPERM : -GetSocketError();
        NET_CONNECTION_LOG_STREAM_DEBUG <<
            "closing connection due to error" <<
            (mAuthFailureFlag ? " auth failure" : "") <<
            (mLastErrorMsg.empty() ? "" : " ") << mLastErrorMsg <<
        KFS_LOG_EOM;
        if (status < 0) {
            SetLastError(status);
        }
        Close();
        mCallbackObj->HandleEvent(EVENT_NET_ERROR, &status);
    } else {
        Update();
    }
}

void
NetConnection::HandleTimeoutEvent()
{
    const int timeOut = GetInactivityTimeout();
    if (timeOut < 0) {
        NET_CONNECTION_LOG_STREAM_DEBUG <<
            "ignoring timeout event, time out value: " << timeOut <<
        KFS_LOG_EOM;
    } else if (mSock) {
        NET_CONNECTION_LOG_STREAM_DEBUG << "inactivity timeout:" <<
            " read-ahead: " << mMaxReadAhead <<
            " in: "  << mInBuffer.BytesConsumable() <<
            " out: " << mOutBuffer.BytesConsumable() <<
            (mNetManagerEntry.IsIn()    ? " +r" : "") <<
            (mNetManagerEntry.IsOut()   ? " +w" : "") <<
            (mNetManagerEntry.IsAdded() ? ""    : " -a") <<
        KFS_LOG_EOM;
        mCallbackObj->HandleEvent(EVENT_INACTIVITY_TIMEOUT, 0);
    }
    Update();
}

void
NetConnection::Update(bool resetTimer)
{
    NetManager::Update(
        mNetManagerEntry, IsGood() ? mSock->GetFd() : -1, resetTimer);
}

int
NetConnection::GetErrorCode() const
{
    if (0 != mLastError) {
        return mLastError;
    }
    // Mutable
    const_cast<NetConnection*>(this)->SetLastError(0);
    if (0 == mLastError && mSock && mSock->IsGood()) {
        const int err = mSock->GetSocketError();
        if (0 != err) {
            const_cast<NetConnection*>(this)->mLastError = err;
        }
    }
    return mLastError;
}

string
NetConnection::GetErrorMsg() const
{
    if (! mLastErrorMsg.empty()) {
        return mLastErrorMsg;
    }
    string msg;
    if (mFilter) {
        msg = mFilter->GetErrorMsg();
    }
    if (msg.empty()) {
        const int err = GetErrorCode();
        if (0 != err) {
            msg = QCUtils::SysError(err < 0 ? -err : err);
        }
    }
    if (! msg.empty()) {
        // Mutable
        const_cast<NetConnection*>(this)->mLastErrorMsg = msg;
    }
    return mLastErrorMsg;
}

int
NetConnection::Shutdown(bool readFlag, bool writeFlag)
{
    if (! mSock) {
        return -EINVAL;
    }
    if (IsNameResolutionPending()) {
        mPendingShutdownFlag      = true;
        mPendingShutdownReadFlag  = readFlag;
        mPendingShutdownWriteFlag = writeFlag;
        return 0;
    }
    if (mFilter) {
        return mFilter->Shutdown(*this, *mSock);
    }
    return mSock->Shutdown(readFlag, writeFlag);
}

time_t
NetConnection::NetManagerEntry::TimeNow() const
{
    return (mNetManager ? mNetManager->Now() : time(0));
}


}
