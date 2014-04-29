//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2014/04/23
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
// 
//----------------------------------------------------------------------------

#ifndef CLIENT_THREAD_H
#define CLIENT_THREAD_H

#include "ClientManager.h"
#include "KfsOps.h"

class QCMutex;

namespace KFS
{

class ClientSM;
class NetManager;

class ClientThread
{
public:
    ClientThread();
    ~ClientThread();
    void Start();
    void Stop();
    void Add(
        ClientSM& inClient);
    bool Handle(
        ClientSM& inClient,
        int       inCode,
        void*     inDataPtr);
    void Granted(
        ClientSM& inClient);
    static NetManager& GetCurrentNetManager();
    static QCMutex& GetMutex();
private:
    class Impl;
    Impl& mImpl;

    friend class Impl;
private:
    ClientThread(
        const ClientThread& inThread);
    ClientThread& operator=(
        const ClientThread& inThread);
};

}

#endif /* CLIENT_THREAD_H */

