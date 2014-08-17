//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/14
// Author: Sriram Rao
//
// Copyright 2008-2011 Quantcast Corp.
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
                }
                break;
            }
        } while (++i < maxAcceptsPerRead && IsGood());
    } else if (WantRead()) {
        const int nread = mFilter ?
            mFilter->Read(*this, *mSock, mInBuffer, maxReadAhead) : 
            mInBuffer.Read(mSock->GetFd(), maxReadAhead);
        if (nread <= 0 && IsFatalError(-nread)) {
            if (nread != 0) {
                GetErrorMsg();
                IsAuthFailure();
            }
            NET_CONNECTION_LOG_STREAM_DEBUG <<
                "read: " << (nread == 0 ? "EOF" : QCUtils::SysError(-nread)) <<
                (mAuthFailureFlag ? " auth failure" : "") <<
                (mLstErrorMsg.empty() ? "" : " ") << mLstErrorMsg <<
            KFS_LOG_EOM;
            if (nread != 0) {
                Close();
            }
            mCallbackObj->HandleEvent(EVENT_NET_ERROR, NULL);
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
                (mLstErrorMsg.empty() ? "" : " ") << mLstErrorMsg <<
            KFS_LOG_EOM;
            Close();
            mCallbackObj->HandleEvent(EVENT_NET_ERROR, NULL);
        } else if (forceInvokeErrHandlerFlag) {
            NET_CONNECTION_LOG_STREAM_DEBUG <<
                "write: forcing error handler invocation, wrote: " << nwrote <<
            KFS_LOG_EOM;
            mCallbackObj->HandleEvent(EVENT_NET_ERROR, NULL);
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
        NET_CONNECTION_LOG_STREAM_DEBUG <<
            "closing connection due to error" <<
            (mAuthFailureFlag ? " auth failure" : "") <<
            (mLstErrorMsg.empty() ? "" : " ") << mLstErrorMsg <<
        KFS_LOG_EOM;
        Close();
        mCallbackObj->HandleEvent(EVENT_NET_ERROR, NULL);
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
    } else {
        NET_CONNECTION_LOG_STREAM_DEBUG << "inactivity timeout:" <<
            " read-ahead: " << maxReadAhead <<
            " in: "  << mInBuffer.BytesConsumable() <<
            " out: " << mOutBuffer.BytesConsumable() <<
            (mNetManagerEntry.IsIn()    ? " +r" : "") <<
            (mNetManagerEntry.IsOut()   ? " +w" : "") <<
            (mNetManagerEntry.IsAdded() ? ""    : " -a") <<
        KFS_LOG_EOM;
        mCallbackObj->HandleEvent(EVENT_INACTIVITY_TIMEOUT, NULL);
    }
    Update();
}

void
NetConnection::Update(bool resetTimer)
{
    NetManager::Update(
        mNetManagerEntry, IsGood() ? mSock->GetFd() : -1, resetTimer);
}

string
NetConnection::GetErrorMsg() const
{
    string msg;
    if (mFilter) {
        msg = mFilter->GetErrorMsg();
    }
    if (mSock && msg.empty()) {
        const int err = mSock->GetSocketError();
        if (err) {
            msg = QCUtils::SysError(err);
        }
    }
    if (! msg.empty()) {
        // Mutable
        const_cast<NetConnection*>(this)->mLstErrorMsg = msg;
    }
    return mLstErrorMsg;
}

int
NetConnection::Shutdown()
{
    if (! mSock) {
        return -EINVAL;
    }
    if (mFilter) {
        return mFilter->Shutdown(*this, *mSock);
    }
    return mSock->Shutdown();
}

time_t
NetConnection::NetManagerEntry::TimeNow() const
{
    return (mNetManager ? mNetManager->Now() : time(0));
}


}
