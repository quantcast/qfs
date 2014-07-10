//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2014/01/20
// Author: Mike Ovsiannikov
//
// Copyright 2014 Quantcast Corp.
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
// \brief "Client" for monitoring and administering from meta and chunk servers.
//
//----------------------------------------------------------------------------

#ifndef MON_CLIENT_H
#define MON_CLIENT_H

#include "libclient/KfsNetClient.h"
#include "kfsio/ClientAuthContext.h"
#include "kfsio/NetManager.h"

namespace KFS_MON
{
using namespace KFS;
using namespace KFS::client;

class MonClient :
    private NetManager,
    public KfsNetClient,
    public KfsNetClient::OpOwner
{
public:
    MonClient();
    virtual ~MonClient();
    int SetParameters(
        const ServerLocation& inMetaLocation,
        const char*           inConfigFileNamePtr);
    int Execute(
        const ServerLocation& inLocation,
        KfsOp&                inOp);
    virtual void OpDone(
        KfsOp*    inOpPtr,
        bool      inCanceledFlag,
        IOBuffer* inBufferPtr);
    bool IsAuthEnabled() const
        { return mAuthContext.IsEnabled(); }
private:
    ClientAuthContext mAuthContext;
private:
    MonClient(
        const MonClient& inClient);
    MonClient& operator=(
        const MonClient& inClient);
};

}

#endif /* MON_CLIENT_H */
