//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/23
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
//
//----------------------------------------------------------------------------

#include "Acceptor.h"
#include "NetManager.h"
#include "common/MsgLogger.h"
#include "qcdio/QCUtils.h"

#include <stdlib.h>

namespace KFS
{
///
/// Create a TCP socket, bind it to the port, and listen for incoming connections.
///
Acceptor::Acceptor(
    NetManager&           netManager,
    const ServerLocation& location,
    bool                  ipV6OnlyFlag,
    IAcceptorOwner*       owner,
    bool                  bindOnlyFlag)
    : mLocation(location),
      mIpV6OnlyFlag(ipV6OnlyFlag),
      mAcceptorOwner(owner),
      mConn(),
      mNetManager(netManager)
{
    SET_HANDLER(this, &Acceptor::RecvConnection);
    Acceptor::Bind();
    if (! bindOnlyFlag) {
        Acceptor::StartListening();
    }
}

Acceptor::Acceptor(
    NetManager&     netManager,
    int             port,
    IAcceptorOwner* owner,
    bool            bindOnlyFlag /* = false */)
    : mLocation(string(), port),
      mIpV6OnlyFlag(false),
      mAcceptorOwner(owner),
      mConn(),
      mNetManager(netManager)
{
    SET_HANDLER(this, &Acceptor::RecvConnection);
    Acceptor::Bind();
    if (! bindOnlyFlag) {
        Acceptor::StartListening();
    }
}

Acceptor::~Acceptor()
{
    if (mConn) {
        mConn->Close();
        mConn.reset();
    }
}

void
Acceptor::Bind()
{
    if (! mNetManager.IsRunning()) {
        return;
    }
    if (mConn) {
        mConn->Close();
        mConn.reset();
    }
    TcpSocket* const sock = new TcpSocket();
    const int res = sock->Bind(
        mLocation,
        (mLocation.hostname.empty() && mIpV6OnlyFlag) ?
            TcpSocket::kTypeIpV6 : TcpSocket::kTypeIpV4,
        mIpV6OnlyFlag
    );
    if (res < 0) {
        KFS_LOG_STREAM_ERROR <<
            "failed to bind to: " << mLocation <<
            " error: " << QCUtils::SysError(-res) <<
        KFS_LOG_EOM;
        delete sock;
        return;
    }
    if (mLocation.port == 0) {
        ServerLocation loc;
        const int err = sock->GetSockLocation(loc);
        if (err) {
            KFS_LOG_STREAM_ERROR <<
                "failed to get socket address: " << QCUtils::SysError(err) <<
            KFS_LOG_EOM;
        } else {
            mLocation.port = loc.port;
        }
    }
    const bool kListenOnlyFlag = true;
    mConn.reset(new NetConnection(sock, this, kListenOnlyFlag));
}

void
Acceptor::StartListening()
{
    if (! mConn || ! mNetManager.IsRunning() || ! mConn->IsGood()) {
        mConn.reset();
        return;
    }
    mConn->EnableReadIfOverloaded();
    const bool kNonBlockingAcceptFlag = true;
    mConn->StartListening(kNonBlockingAcceptFlag);
    if (! mConn->IsGood()) {
        mConn.reset();
        return;
    }
    mNetManager.AddConnection(mConn);
}

///
/// Event handler that gets called back whenever a new connection is
/// received.  In response, the AcceptorOwner object is first notified of
/// the new connection and then, the new connection is added to the
/// list of connections owned by the NetManager. @see NetManager
///
int
Acceptor::RecvConnection(int code, void* data)
{
    switch (code) {
        case EVENT_NEW_CONNECTION:
        break;
        case EVENT_NET_ERROR:
            // Under normal circumstances it would come up here only with the
            // error simulator enabled.
            KFS_LOG_STREAM_INFO <<
                "acceptor on: " << mLocation <<
                " error: " <<
                    QCUtils::SysError(mConn ? mConn->GetSocketError() : 0) <<
                (mNetManager.IsRunning() ? ", restarting" : ", exiting") <<
            KFS_LOG_EOM;
            if (mConn) {
                mConn->Close();
                mConn.reset();
            }
            if (mNetManager.IsRunning()) {
                Bind();
                StartListening();
                if (! IsAcceptorStarted()) {
                    KFS_LOG_STREAM_FATAL <<
                        "failed to restart acceptor on: " << mLocation <<
                    KFS_LOG_EOM;
                    MsgLogger::Stop();
                    abort();
                }
            }
        return 0;
        case EVENT_INACTIVITY_TIMEOUT:
            KFS_LOG_STREAM_DEBUG <<
                "acceptror inactivity timeout event ignored" <<
            KFS_LOG_EOM;
        return 0;
        default:
            KFS_LOG_STREAM_FATAL <<
                "invalid event code: " << code <<
            KFS_LOG_EOM;
            MsgLogger::Stop();
            abort();
        return 0;
    }
    if (! data) {
        KFS_LOG_STREAM_FATAL <<
            "invalid null argument, event code: " << code <<
        KFS_LOG_EOM;
        MsgLogger::Stop();
        abort();
    }
    NetConnectionPtr& conn = *reinterpret_cast<NetConnectionPtr*>(data);
    KfsCallbackObj* const obj = mAcceptorOwner->CreateKfsCallbackObj(conn);
    if (conn) {
        if (obj) {
            conn->SetOwningKfsCallbackObj(obj);
            mNetManager.AddConnection(conn);
        } else {
            conn->Close();
        }
    }
    return 0;
}
}
