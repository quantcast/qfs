//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/22
// Author: Sriram Rao
//
// Copyright 2008 Quantcast Corp.
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

#ifndef _LIBIO_ACCEPTOR_H
#define _LIBIO_ACCEPTOR_H

#include "common/kfsdecls.h"
#include "KfsCallbackObj.h"
#include "NetConnection.h"

namespace KFS
{

///
/// \file Acceptor.h
/// \brief Mechanism for accepting TCP connections.
///
/// Accepting a TCP connection consists of two pieces:
///
///   1. Setting up a TCP socket and marking it for listen: This is
/// handled by the Acceptor.
///
///   2.  Once a connection is received, "doing something" with it:
/// This is handled by the IAcceptorOwner.
///

///
/// \class IAcceptorOwner
/// Abstract class defines the interface that must be implemented by an
/// owner of an acceptor object.
///

class IAcceptorOwner {
public:
    virtual ~IAcceptorOwner() { };

    ///
    /// Callback that will be invoked whenever a new connection is
    /// received.  The callback is expected to create a continuation
    /// and return that as the result.
    /// @param conn A smart pointer that encapsulates the connection
    /// that was received.  @see NetConnectionPtr
    ///
    virtual KfsCallbackObj* CreateKfsCallbackObj(NetConnectionPtr &conn) = 0;
};

class NetManager;

///
/// \class Acceptor
/// A continuation for receiving connections on a TCP port.  Calls
/// back the associated IAcceptorOwner whenever a connection is received.
///
class Acceptor : public KfsCallbackObj {
public:

    ///
    /// @param port Port number used to listen for incoming
    /// connections.
    /// @param owner The IAcceptorOwner object that "owns" this Acceptor.
    ///
    Acceptor(
        NetManager&     netManager,
        int             port,
        IAcceptorOwner* owner,
        bool            bindOnlyFlag = false);
    Acceptor(
        NetManager&           netManager,
        const ServerLocation& location,
        bool                  ipV6OnlyFlag,
        IAcceptorOwner*       owner,
        bool                  bindOnlyFlag);
    ~Acceptor();
    void StartListening();

    /// Return true if we were able to bind to the acceptor port
    bool IsAcceptorStarted() const {
        return (mConn && mConn->IsGood());
    }
    ///
    /// Event handler to handle incoming connections.  @see KfsCallbackObj
    /// @param code Unused argument
    /// @param data NetConnectionPtr object that encapsulates the
    /// accepted connection.
    /// @result Returns 0.
    ///
    int RecvConnection(int code, void *data);
    int GetPort() const
        { return mLocation.port; }
    const ServerLocation& GetLocation() const
        { return mLocation; }
private:
    ///
    /// The encapsulated connection object that corresponds to the TCP
    /// port on which the Acceptor is listening for connections.
    ///
    ServerLocation        mLocation;
    bool                  mIpV6OnlyFlag;
    IAcceptorOwner* const mAcceptorOwner;
    NetConnectionPtr      mConn;
    NetManager&           mNetManager;

    void Bind();
};

}

#endif // _LIBIO_ACCEPTOR_H
