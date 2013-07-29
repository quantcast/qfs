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

#include "MetaRequest.h"
#include "common/Properties.h"
#include "kfsio/SslFilter.h"
#include "krb/KrbService.h"
#include "qcdio/qcdebug.h"

#include <algorithm>
#include <string>

namespace KFS
{
using std::string;
using std::max;

class AuthContext::Impl
{
public:
    Impl()
        : mKrbServicePtr(0),
          mSslCtxPtr(0),
          mX509SslCtxPtr(0),
          mPrincipalUnparseFlags(0)
        {}
    ~Impl()
    {
        delete mKrbServicePtr;
        SslFilter::FreeCtx(mSslCtxPtr);
        SslFilter::FreeCtx(mX509SslCtxPtr);
    }
    bool Validate(
        MetaAuthenticate& inOp)
    {
        if (inOp.status != 0) {
            return false;
        }
        if (inOp.authType == kAuthenticationTypeKrb5) {
            if (! mKrbServicePtr) {
                inOp.status    = -ENOENT;
                inOp.statusMsg = "authentication type is not configured";
                return false;
            }
            QCASSERT(0 < inOp.contentLength);
        } else if (inOp.authType == kAuthenticationTypeX509) {
            if (! mX509SslCtxPtr) {
                inOp.status    = -ENOENT;
                inOp.statusMsg = "authentication type is not configured";
                return false;
            }
            QCASSERT(0 >= inOp.contentLength);
        } else {
            QCASSERT(inOp.authType == kAuthenticationTypeNone &&
                0 >= inOp.contentLength);
        }
        return true;
    }
    bool Authenticate(
        MetaAuthenticate& inOp)
    {
        if (! Validate(inOp)) {
            return false;
        }
        inOp.authName.clear();
        delete inOp.filter;
        inOp.filter = 0;
        inOp.responseContentPtr = 0;
        inOp.responseContentLen = 0;
        if (0 < inOp.contentLength &&
                (inOp.contentBufPos < inOp.contentLength ||
                ! inOp.contentBuf)) {
            inOp.status    = -EINVAL;
            inOp.statusMsg = "partial content read";
            return false;
        }
        if (inOp.authType == kAuthenticationTypeKrb5) {
            QCASSERT(mKrbServicePtr);
            const char* theSessionKeyPtr    = 0;
            int         theSessionKeyLen    = 0;
            const char* thePeerPrincipalPtr = 0;
            const char* const theErrPtr = mKrbServicePtr->RequestReply(
                inOp.contentBuf,
                inOp.contentLength,
                mPrincipalUnparseFlags,
                inOp.responseContentPtr,
                inOp.responseContentLen,
                theSessionKeyPtr,
                theSessionKeyLen,
                thePeerPrincipalPtr
            );
            if (theErrPtr) {
                inOp.status    = -EINVAL;
                inOp.statusMsg = theErrPtr;
                return false;
            }
            const string theAuthName(
                thePeerPrincipalPtr ? thePeerPrincipalPtr : "");
            if (! Validate(theAuthName)) {
                inOp.status    = -EACCES;
                inOp.statusMsg = "access denied for '" + theAuthName + "'";
                inOp.responseContentPtr = 0;
                inOp.responseContentLen = 0;
                return false;
            }
            if (! mSslCtxPtr) {
                if (inOp.status == 0) {
                    inOp.authName         = theAuthName;
                    inOp.responseAuthType = inOp.authType;
                }
                return true;
            }
            // Do not send kerberos AP_REP, as TLS-PSK handshake is sufficient
            // for mutual authentication / replay attack protection.
            inOp.responseContentPtr = 0;
            inOp.responseContentLen = 0;
            const char*           kPskClientIdentityPtr = "";
            SslFilter::ServerPsk* kServerPskPtr         = 0;
            const bool            kDeleteOnCloseFlag    = true;
            SslFilter* const theFilterPtr = new SslFilter(
                *mSslCtxPtr,
                theSessionKeyPtr,
                (size_t)max(0, theSessionKeyLen),
                kPskClientIdentityPtr,
                kServerPskPtr,
                kDeleteOnCloseFlag
            );
            const SslFilter::Error theErr = theFilterPtr->GetError();
            if (theErr) {
                inOp.statusMsg = SslFilter::GetErrorMsg(theErr);
                inOp.status    = -EINVAL;
                if (inOp.statusMsg.empty()) {
                    inOp.statusMsg = "failed to create ssl filter";
                }
                delete theFilterPtr;
            } else {
                inOp.filter           = theFilterPtr;
                inOp.responseAuthType = inOp.authType;
                inOp.authName         = theAuthName;
            }
            return (inOp.status == 0);
        }
        if (inOp.authType == kAuthenticationTypeX509) {
            QCASSERT(mX509SslCtxPtr);
            const char*           kSessionKeyPtr        = 0;
            size_t                kSessionKeyLen        = 0;
            const char*           kPskClientIdentityPtr = 0;
            SslFilter::ServerPsk* kServerPskPtr         = 0;
            const bool            kDeleteOnCloseFlag    = true;
            SslFilter* const theFilterPtr = new SslFilter(
                *mX509SslCtxPtr,
                kSessionKeyPtr,
                kSessionKeyLen,
                kPskClientIdentityPtr,
                kServerPskPtr,
                kDeleteOnCloseFlag
            );
            const SslFilter::Error theErr = theFilterPtr->GetError();
            if (theErr) {
                inOp.statusMsg = SslFilter::GetErrorMsg(theErr);
                inOp.status    = -EINVAL;
                if (inOp.statusMsg.empty()) {
                    inOp.statusMsg = "failed to create ssl filter";
                }
                delete theFilterPtr;
            } else {
                inOp.filter           = theFilterPtr;
                inOp.responseAuthType = inOp.authType;
            }
            return (inOp.status == 0);
        }
        QCASSERT(inOp.authType == kAuthenticationTypeNone);
        return (inOp.status == 0);
    }
    bool Validate(
        const string& inAuthName) const
    {
        return ((! mKrbServicePtr && ! mX509SslCtxPtr) || ! inAuthName.empty());
    }
    bool SetParameters(
        const char*       inParamNamePrefixPtr,
        const Properties& inParameters)
    {
        return true;
    }
private:
    KrbService*     mKrbServicePtr;
    SslFilter::Ctx* mSslCtxPtr;
    SslFilter::Ctx* mX509SslCtxPtr;
    int             mPrincipalUnparseFlags;
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

AuthContext::AuthContext()
    : mImpl(*(new Impl()))
{
}

AuthContext::~AuthContext()
{
    delete &mImpl;
}

    bool
AuthContext::Validate(
    MetaAuthenticate& inOp)
{
    delete [] inOp.contentBuf;
    inOp.contentBuf    = 0;
    inOp.contentBufPos = 0;
    if (inOp.authType != kAuthenticationTypeKrb5 &&
            inOp.authType != kAuthenticationTypeX509 &&
            inOp.authType != kAuthenticationTypeNone) {
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
                    " length with non krb5 authentication";
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
    return mImpl.Validate(inOp);
}

    bool
AuthContext::Authenticate(
    MetaAuthenticate& inOp)
{
    return mImpl.Validate(inOp);
}

    bool
AuthContext::Validate(
    const string& inAuthName) const
{
    return mImpl.Validate(inAuthName);
}

    bool
AuthContext::SetParameters(
    const char*       inParamNamePrefixPtr,
    const Properties& inParameters)
{
    return mImpl.SetParameters(inParamNamePrefixPtr, inParameters);
}

}

