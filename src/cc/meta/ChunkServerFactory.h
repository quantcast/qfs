//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/06
// Author: Sriram Rao, Mike Ovsiannikov
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
// \file ChunkServerFactory.h
// \brief Create ChunkServer objects whenever a chunk server connects
// to us (namely, the meta server).
//
//----------------------------------------------------------------------------

#ifndef META_CHUNKSERVERFACTORY_H
#define META_CHUNKSERVERFACTORY_H

#include "kfsio/Acceptor.h"
#include "kfsio/KfsCallbackObj.h"
#include "kfstypes.h"
#include "ChunkServer.h"

namespace KFS
{

///
/// ChunkServerFactory creates a ChunkServer object whenever
/// a chunk server connects to us.  The ChunkServer object is
/// responsible for all the communication with that chunk
/// server.
///
class ChunkServerFactory : public IAcceptorOwner
{
public:
    ChunkServerFactory()
        : mAcceptor(0)
        {}
    virtual ~ChunkServerFactory()
        { delete mAcceptor; }
    bool Bind(
        NetManager&           inNetManager,
        const ServerLocation& location,
        bool                  isIpV6OnlyFlag)
    {
        delete mAcceptor;
        const bool kBindOnlyFlag = true;
        mAcceptor = new Acceptor(
            inNetManager, location, isIpV6OnlyFlag, this, kBindOnlyFlag);
        return mAcceptor->IsAcceptorStarted();
    }
    /// Start an acceptor to listen on the specified port.
    bool StartAcceptor()
    {
        if (! mAcceptor) {
            return false;
        }
        mAcceptor->StartListening();
        return mAcceptor->IsAcceptorStarted();
    }
    /// Callback that gets invoked whenever a chunkserver
    /// connects to the acceptor port.  The accepted socket
    /// connection is passed in.
    /// @param[in] conn: The accepted connection
    /// @retval The continuation object that was created as a
    /// result of this call.
    KfsCallbackObj *CreateKfsCallbackObj(NetConnectionPtr &conn)
        { return ChunkServer::Create(conn); }
private:
    // The socket object which is setup to accept connections from
    /// chunkserver.
    Acceptor* mAcceptor;
};

}

#endif // META_CHUNKSERVERFACTORY_H
