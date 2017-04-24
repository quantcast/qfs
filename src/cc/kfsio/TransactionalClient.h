//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/09/28
// Author:  Mike Ovsiannikov
//
// Copyright 2015,2016 Quantcast Corporation. All rights reserved.
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
// \brief Http like generic request response client with optional ssl transport.
//
//----------------------------------------------------------------------------

#ifndef KFSIO_TRANSACTIONAL_CLIENT_H
#define KFSIO_TRANSACTIONAL_CLIENT_H

#include <string>

namespace KFS
{
using std::string;

class NetManager;
class Properties;
class IOBuffer;
struct ServerLocation;

class TransactionalClient
{
public:
    class Transaction
    {
    public:
        virtual int Request(
            IOBuffer&             inBuffer,
            IOBuffer&             inResponseBuffer,
            const ServerLocation& inServer) = 0;
        virtual int Response(
            IOBuffer& inBuffer,
            bool      inEofFlag,
            IOBuffer& inOutBuffer) = 0;
        virtual void Error(
            int         inStatus,
            const char* inMsgPtr) = 0;
    protected:
        Transaction()
            {}
        virtual ~Transaction()
            {}
    };
    TransactionalClient(
        NetManager& inNetManager);
    ~TransactionalClient();
    int SetServer(
        const ServerLocation& inLocation,
        bool                  inHttpsHostNameFlag);
    void Stop();
    int SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters,
        string*           inErrMsgPtr);
    void Run(
        Transaction& inTransaction);
private:
    class Impl;
    Impl& mImpl;
private:
    TransactionalClient(
        const TransactionalClient& inClient);
    TransactionalClient& operator=(
        const TransactionalClient& inClient);
};

}

#endif /* KFSIO_TRANSACTIONAL_CLIENT_H */
