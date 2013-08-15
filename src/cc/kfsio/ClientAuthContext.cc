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

#include "ClientAuthContext.h"

#include "common/Properties.h"
#include "kfsio/NetConnection.h"

namespace KFS
{

class Properties;
class NetConnection;

class ClientAuthContext::Impl
{
public:
    Impl()
        {}
    ~Impl()
        {}
    int SetParameters(
        const char*       inParamsPrefixPtr,
        const Properties& inParameters)
    {
        return 0;
    }
    int Request(
        int          inAuthType,
        int&         outAuthType,
        const char*& outBufPtr,
        int&         outBufLen,
        string*      outErrMsgPtr)
    {
        return 0;
    }
    int StartSsl(
        NetConnection& inNetConnection,
        const char*    inKeyIdPtr,
        const char*    inKeyPtr,
        int            inKeyLength)
    {
        return 0;
    }
    int Response(
        int            inAuthType,
        bool           inUseSslFlag,
        const char*    inBufPtr,
        int            inBufLen,
        NetConnection& inNetConnection,
        string*        outErrMsgPtr)
    {
        return 0;
    }
    bool IsEnabled() const
    {
        return false;
    }
    bool IsShared() const
    {
        return false;
    }
    void SetShared(
        bool inFlag)
    {
    }
    int CheckAuthType(
        int     inAuthType,
        string* outErrMsgPtr)
    {
        return 0;
    }
private:
};

ClientAuthContext::ClientAuthContext()
    : mImpl(*(new Impl()))
    {}

ClientAuthContext::~ClientAuthContext()
{
    delete &mImpl;
}

    bool
ClientAuthContext::IsShared() const
{
    return mImpl.IsShared();
}

    void
ClientAuthContext::SetShared(
    bool inFlag)
{
    return mImpl.SetShared(inFlag);
}

    int
ClientAuthContext::CheckAuthType(
    int     inAuthType,
    string* outErrMsgPtr)
{
    return mImpl.CheckAuthType(inAuthType, outErrMsgPtr);
}

    int
ClientAuthContext::SetParameters(
    const char*       inParamsPrefixPtr,
    const Properties& inParameters)
{
    return mImpl.SetParameters(inParamsPrefixPtr, inParameters);
}

    int
ClientAuthContext::Request(
    int          inAuthType,
    int&         outAuthType,
    const char*& outBufPtr,
    int&         outBufLen,
    string*      outErrMsgPtr)
{
    return mImpl.Request(
        inAuthType, outAuthType, outBufPtr, outBufLen, outErrMsgPtr);
}

    int
ClientAuthContext::StartSsl(
    NetConnection& inNetConnection,
    const char*    inKeyIdPtr,
    const char*    inKeyPtr,
    int            inKeyLength)
{
    return mImpl.StartSsl(inNetConnection, inKeyIdPtr, inKeyPtr, inKeyLength);
}

    int
ClientAuthContext::Response(
    int            inAuthType,
    bool           inUseSslFlag,
    const char*    inBufPtr,
    int            inBufLen,
    NetConnection& inNetConnection,
    string*        outErrMsgPtr)
{
    return mImpl.Response(
        inAuthType, inUseSslFlag, inBufPtr, inBufLen, inNetConnection,
        outErrMsgPtr);
}

    bool
ClientAuthContext::IsEnabled() const
{
    return mImpl.IsEnabled();
}

} // namespace KFS
