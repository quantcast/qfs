//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/07/26
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
// \file AuthContext.cc
//
//----------------------------------------------------------------------------

#include "AuthContext.h"

#include "common/Properties.h"
#include "MetaRequest.h"

namespace KFS
{
using std::string;

class AuthContext::Impl
{
public:
    Impl()
        {}
    ~Impl()
        {}
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

AuthContext::AuthContext()
    : mImplPtr(0)
{
}

AuthContext::~AuthContext()
{
    delete mImplPtr;
}

    bool
AuthContext::Validate(
    MetaAuthenticate& inOp)
{
    delete [] inOp.contentBuf;
    inOp.contentBuf    = 0;
    inOp.contentBufPos = 0;
    if (inOp.authType != kAuthenticationTypeKrb5 &&
            inOp.authType != kAuthenticationTypeX509) {
        inOp.status    = -EINVAL;
        inOp.statusMsg = "authentication type is not supported";
    } else {
        if (kMaxAuthenticationContentLength < inOp.contentLength) {
            inOp.status    = -EINVAL;
            inOp.statusMsg = "content length exceeds limit";
        } else if (inOp.contentLength > 0) {
            if (inOp.authType != kAuthenticationTypeKrb5) {
                inOp.status    = -EINVAL;
                inOp.statusMsg = "invalid non zero content"
                    " length with x509 authentication";
            } else {
                inOp.contentBuf = new char [inOp.contentLength];
                inOp.contentBuf = 0;
            }
        } else {
            if (inOp.authType == kAuthenticationTypeKrb5) {
                inOp.status    = -EINVAL;
                inOp.statusMsg = "invalid zero content"
                    " length with kerberos authentication";
            }
        }
    }
    return (inOp.status == 0);
}

    bool
AuthContext::Authenticate(
    MetaAuthenticate& inOp)
{
    return true;
}

    bool
AuthContext::Validate(
    const string& inUserName) const
{
    return true;
}

    bool
AuthContext::SetParameters(
    const char*       inParamNamePrefixPtr,
    const Properties& inParameters)
{
    return true;
}

}

