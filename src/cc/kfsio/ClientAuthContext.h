//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/8/9
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
//
//----------------------------------------------------------------------------

#ifndef KFS_CLIENT_AUTH_CONTEXT_H
#define KFS_CLIENT_AUTH_CONTEXT_H

#include <string>

namespace KFS
{
using std::string;

class Properties;
class NetConnection;

class ClientAuthContext
{
public:
    ClientAuthContext();
    ~ClientAuthContext();
    bool IsEnabled() const;
    bool IsShared() const;
    void SetShared(
        bool inFlag);
    int SetParameters(
        const char*       inParamsPrefixPtr,
        const Properties& inParameters);
    int CheckAuthType(
        int     inAuthType,
        string* outErrMsgPtr);
    int Request(
        int          inAuthType,
        int&         outAuthType,
        const char*& outBufPtr,
        int&         outBufLen,
        string*      outErrMsgPtr);
    int StartSsl(
        NetConnection& inNetConnection,
        const char*    inKeyIdPtr,
        const char*    inKeyDataPtr,
        int            inKeyDataSize,
        string*        outErrMsgPtr);
    int Response(
        int            inAuthType,
        bool           inUseSslFlag,
        const char*    inBufPtr,
        int            inBufLen,
        NetConnection& inNetConnection,
        string*        outErrMsgPtr);
private:
    class Impl;
    Impl& mImpl;
private:
    ClientAuthContext(
        const ClientAuthContext& inContext);
    ClientAuthContext& operator=(
        const ClientAuthContext& inContext);
};

} // namespace KFS

#endif /* KFS_CLIENT_AUTH_CONTEXT_H */
