//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/06/08
// Author: Mike Ovsiannikov
//
// Copyright 2013 Quantcast Corp.
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
// Kerberos 5 client side authentication implementation.
//
//----------------------------------------------------------------------------

#ifndef KFS_KRB_CLIENT_H
#define KFS_KRB_CLIENT_H

#include <time.h>

namespace KFS
{

class KrbClient
{
public:
    KrbClient();
    ~KrbClient();
    const char* Init(
        const char* inServiceHostNamePtr,
        const char* inServeiceNamePtr,
        const char* inKeyTabNamePtr,
        const char* inClientNamePtr,
        bool        inForceCacheInitFlag);
    const char* Cleanup();
    const char* Request(
        const char*& outDataPtr,
        int&         outDataLen,
        const char*& outSessionKeyPtr,
        int&         outSessionKeyLen);
    const char* Reply(
        const char*  inReplyPtr,
        int          inReplyLen);
    int GetErrorCode() const;
    time_t GetLastCredEndTime() const;
private:
    class Impl;
    Impl& mImpl;

private:
    KrbClient(
        const KrbClient& inClient);
    KrbClient& operator=(
        const KrbClient& inClient);
};

}

#endif /* KFS_KRB_CLIENT_H */
