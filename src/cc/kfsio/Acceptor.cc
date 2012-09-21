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
#include "Globals.h"
#include "common/MsgLogger.h"
#include "qcdio/QCUtils.h"
#include <stdlib.h>

namespace KFS
{
using namespace KFS::libkfsio;
///
/// Create a TCP socket, bind it to the port, and listen for incoming connections.
///
Acceptor::Acceptor(NetManager& netManager, int port, IAcceptorOwner* owner,
    bool bindOnlyFlag /* = false */)
    : mPort(port),
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

Acceptor::Acceptor(int port, IAcceptorOwner *owner,
    bool bindOnlyFlag /* = false */)
    : mPort(port),
      mAcceptorOwner(owner),
      mConn(),
      mNetManager(globalNetManager())
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
    const int res = sock->Bind(mPort);
    if (res < 0) {
        KFS_LOG_STREAM_ERROR <<
            "Unable to bind to port: " << mPort <<
            " error: " << QCUtils::SysError(-res) <<
        KFS_LOG_EOM;
        delete sock;
        return;
    }
    if (mPort == 0) {
        const string sockName = sock->GetSockName();
        const size_t pos = sockName.rfind(":");
        if (pos != string::npos) {
            mPort = atoi(sockName.c_str() + pos + 1);
        }
    }
    const bool kListenOnlyFlag = true;
    mConn.reset(new NetConnection(sock, this, kListenOnlyFlag));
}

void
Acceptor::StartListening()
{
    if (! mConn || ! mNetManager.IsRunning() || ! mConn->IsGood()) {
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
            KFS_LOG_STREAM_INFO <<
                "acceptor on port: " << mPort <<
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
                "Unexpected event code: " << code <<
            KFS_LOG_EOM;
            abort();
        break;
    }
    if (! data) {
        KFS_LOG_STREAM_FATAL <<
            "Unexpected null argument, event code: " << code <<
        KFS_LOG_EOM;
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
