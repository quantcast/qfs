//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/02
// Author: Sriram Rao
//         Mike Ovsiannikov implement multiple outstanding request processing,
//         and "client threads".
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
// \file ClientManager.h
// \brief Create client state machines whenever clients connect to meta server.
//
//----------------------------------------------------------------------------

#ifndef META_CLIENTMANAGER_H
#define META_CLIENTMANAGER_H

#include "MetaRequest.h"

class QCMutex;

namespace KFS
{

class ClientSM;
class AuthContext;
class Properties;

class ClientManager
{
public:
    class ClientThread;

    ClientManager();
    virtual ~ClientManager();
    void SetMaxClientSockets(int count);
    int GetMaxClientCount() const;
    bool Bind(const ServerLocation& location, bool ipV6OnlyFlag);
    bool StartAcceptor(int threadCount, int startCpuAffinity);
    void Shutdown();
    void ChildAtFork();
    QCMutex& GetMutex();
    void SetParameters(const Properties& params);
    static AuthContext& GetAuthContext(ClientThread* inThread);
    static bool Enqueue(ClientThread* thread, MetaRequest& op)
    {
        if (op.next == &op) {
            return false;
        }
        if (! thread) {
            op.next = &op;
            return false;
        }
        return EnqueueSelf(thread, op);
    }
    static void SubmitRequest(ClientThread* thread, MetaRequest& op)
    {
        if (thread) {
            SubmitRequestSelf(thread, op);
        } else {
            submit_request(&op);
        }
    }
    static bool Flush(ClientThread* thread, ClientSM& /* cli */)
    {
        return (thread != 0); // Thread invokes flush.
    }
    void PrepareCurrentThreadToFork();
    inline void PrepareToFork();
    inline void ForkDone();
private:
    class Impl;
    Impl& mImpl;

    static bool EnqueueSelf(ClientThread* thread, MetaRequest& op);
    static void SubmitRequestSelf(ClientThread* thread, MetaRequest& op);
private:
    ClientManager(const ClientManager&);
    ClientManager& operator=(const ClientManager&);
};


}

#endif // META_CLIENTMANAGER_H
