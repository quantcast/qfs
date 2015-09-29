//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/09/28
// Author:  Mike Ovsiannikov
//
// Copyright 2015 Quantcast Corp.
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

namespace KFS
{
class NetManager;
class Properties;
class IOBuffer;

class TransactionalClient
{
public:
    class Transaction
    {
    public:
        virtual int Request(
            IOBuffer& inBuffer,
            IOBuffer& inResponseBuffer) = 0;
        virtual int Response(
            IOBuffer& inBuffer) = 0;
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
    void Stop();
    bool SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters);
    void Run(
        Transaction& inTransaction);
private:
    class Impl;
};

}

#endif /* KFSIO_TRANSACTIONAL_CLIENT_H */
