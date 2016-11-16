//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2016/11/15
// Author: Mike Ovsainnikov
//
// Copyright 2016 Quantcast Corp.
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
// Singly linked queue.
//
//----------------------------------------------------------------------------

#ifndef KFSIO_RESOLVER_H
#define KFSIO_RESOLVER_H

#include <string>
#include <vector>

namespace KFS
{
using std::string;
using std::vector;

class NetManager;

class Resolver
{
private:
    class Impl;
public:
    class Request
    {
    public:
        Request(
            const string& inHostName)
            : mHostName(inHostName),
              mIpAddresses(),
              mStatus(0),
              mStatusMsg(),
              mNextPtr(0)
            {}
        virtual void Done() = 0;
    protected:
        typedef vector<string> IpAddresses;

        string      mHostName;
        IpAddresses mIpAddresses;
        int         mStatus;
        string      mStatusMsg;
        virtual ~Request()
            {}
    private:
        Request* mNextPtr;
        Request(
            const Request& inRequest);
        Request& operator=(
            const Request& inRequest);
        friend class Impl;
    };
    Resolver(
        NetManager& inNetManager);
    ~Resolver();
    int Start();
    void Shutdown();
    int Enqueue(
        Request& inRequest);
private:
    Impl& mImpl;
private:
    Resolver(
        const Resolver& inResolver);
    Resolver& operator=(
        const Resolver& inResolver);
};

}

#endif /* KFSIO_RESOLVER_H */

