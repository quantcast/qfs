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
#include "common/MsgLogger.h"
#include "kfsio/SslFilter.h"
#include "krb/KrbService.h"
#include "qcdio/qcdebug.h"

#include <algorithm>
#include <string>
#include <sstream>
#include <iomanip>

#include <boost/scoped_ptr.hpp>

namespace KFS
{
using std::string;
using std::max;
using std::ostringstream;
using std::hex;
using boost::scoped_ptr;

class AuthContext::Impl
{
public:
    Impl()
        : mKrbProps(),
          mKrbSslProps(),
          mX509SslProps(),
          mKrbServicePtr(),
          mSslCtxPtr(0),
          mX509SslCtxPtr(0),
          mPrincipalUnparseFlags(0),
          mMemKeytabGen(0)
        {}
    ~Impl()
        {}
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
            if (! mX509SslCtxPtr.Get()) {
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
            if (! mSslCtxPtr.Get()) {
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
                *mSslCtxPtr.Get(),
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
            QCASSERT(mX509SslCtxPtr.Get());
            const char*           kSessionKeyPtr        = 0;
            size_t                kSessionKeyLen        = 0;
            const char*           kPskClientIdentityPtr = 0;
            SslFilter::ServerPsk* kServerPskPtr         = 0;
            const bool            kDeleteOnCloseFlag    = true;
            SslFilter* const theFilterPtr = new SslFilter(
                *mX509SslCtxPtr.Get(),
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
        return ((! mKrbServicePtr &&
            ! mX509SslCtxPtr.Get()) || ! inAuthName.empty());
    }
    bool SetParameters(
        const char*       inParamNamePrefixPtr,
        const Properties& inParameters)
    {
        KrbServicePtr      theKrbServicePtr;
        SslCtxPtr          theSslCtxPtr;
        SslCtxPtr          theX509SslCtxPtr;
        Properties::String theParamName;
        if (inParamNamePrefixPtr) {
            theParamName.Append(inParamNamePrefixPtr);
        }
        Properties theKrbProps  = mKrbProps;
        const size_t thePrefLen = theParamName.GetSize();
        inParameters.copyWithPrefix(
            theParamName.Truncate(thePrefLen).Append("krb5.").GetPtr(),
            theKrbProps
        );
        const bool theKrbChangedFlag        = theKrbProps != mKrbProps ||
            theParamName.Truncate(thePrefLen).Append(
            "krb5.forceReload", 0) != 0;
        int        thePrincipalUnparseFlags = 0;
        if (theKrbChangedFlag) {
            const char* const theNullStrPtr     = 0;
            const char* const theServiceNamePtr = inParameters.getValue(
                theParamName.Truncate(thePrefLen).Append("krb5.service"),
                theNullStrPtr
            );
            if (theServiceNamePtr && theServiceNamePtr[0]) {
                mMemKeytabGen++;
                ostringstream theStream;
                theStream << (void*)this << hex << mMemKeytabGen;
                const string theMemTabName     = theStream.str();
                const bool   kDetectReplayFlag = false;
                // No replay detection is needed, as either AP_REP or TLS-PSK are
                // used. Both these mechanisms are sufficient to protect against
                // replay attack as both provide mutual authentication.
                // With no TLS once assume that party other than QFS protects
                // against replay, man-in-the-middle attacks etc.
                theKrbServicePtr.reset(new KrbService());
                const char* theErrMsgPtr = theKrbServicePtr->Init(
                    inParameters.getValue(
                        theParamName.Truncate(thePrefLen).Append(
                            "krb5.host"), theNullStrPtr),
                    theServiceNamePtr,
                    inParameters.getValue(
                        theParamName.Truncate(thePrefLen).Append(
                            "krb5.keytab"), theNullStrPtr),
                    inParameters.getValue(
                        theParamName.Truncate(thePrefLen).Append(
                            "krb5.copyToMemKeytab"), 1) != 0 ?
                        theMemTabName.c_str() : theNullStrPtr,
                    kDetectReplayFlag
                );
                if (theErrMsgPtr) {
                    KFS_LOG_STREAM_ERROR <<
                        theParamName.Truncate(thePrefLen) <<
                            "krb5.* configuration error: " <<
                        theErrMsgPtr <<
                    KFS_LOG_EOM;
                    return false;
                } else {
                    const char* thePrincUnparseModePtr = inParameters.getValue(
                        theParamName.Truncate(thePrefLen).Append(
                            "krb5.princUnparseMode"), theNullStrPtr
                    );
                    if (thePrincUnparseModePtr && thePrincUnparseModePtr[0]) {
                        int theCnt = 0;
                        if (strstr(thePrincUnparseModePtr, "short")) {
                            thePrincipalUnparseFlags |=
                                KrbService::kPrincipalUnparseShort;
                            theCnt++;
                        }
                        if (strstr(thePrincUnparseModePtr, "noRealm")) {
                            thePrincipalUnparseFlags |=
                                KrbService::kPrincipalUnparseNoRealm;
                            theCnt++;
                        }
                        if (strstr(thePrincUnparseModePtr, "display")) {
                            thePrincipalUnparseFlags |=
                                KrbService::kPrincipalUnparseDisplay;
                            theCnt++;
                        }
                        if (theCnt <= 0) {
                            KFS_LOG_STREAM_ERROR <<
                                theParamName.Truncate(thePrefLen) <<
                                "krb5.* configuration error: "
                                "invalid principal unparse mode: " <<
                                thePrincUnparseModePtr <<
                            KFS_LOG_EOM;
                            return false;
                        }
                    }
                }
            }
        }
        Properties theKrbSslProps = mKrbSslProps;
        theParamName.Truncate(thePrefLen).Append("krb5.tls.");
        inParameters.copyWithPrefix(theParamName.GetPtr(), theKrbSslProps);
        const bool theKrbSslChangedFlag =
            (theKrbChangedFlag &&
                    (mKrbServicePtr.get() != 0) != (theKrbServicePtr.get() != 0)) ||
            mKrbSslProps != theKrbSslProps ||
            theParamName.Truncate(thePrefLen).Append(
            "krb5.tls.forceReload", 0) != 0;
        if (theKrbSslChangedFlag && theKrbSslProps.getValue(
                theParamName.Truncate(thePrefLen).Append(
                    "krb5.tls.disable"), 0) == 0 &&
                mKrbServicePtr) {
            const bool kServerFlag  = true;
            const bool kPskOnlyFlag = true;
            string     theErrMsg;
            mSslCtxPtr.Set(SslFilter::CreateCtx(
                kServerFlag,
                kPskOnlyFlag,
                theParamName.Truncate(thePrefLen).Append("krb5.tls.").GetPtr(),
                theKrbSslProps,
                &theErrMsg
            ));
            if (! mSslCtxPtr.Get()) {
                KFS_LOG_STREAM_ERROR <<
                    theParamName.Truncate(thePrefLen) <<
                    "krb5.* configuration error: " << theErrMsg <<
                KFS_LOG_EOM;
                return false;
            }
        }
        Properties theX509SslProps = mX509SslProps;
        theParamName.Truncate(thePrefLen).Append("X509.");
        inParameters.copyWithPrefix(theParamName.GetPtr(), theX509SslProps);
        const bool theX509ChangedFlag = theX509SslProps != mX509SslProps ||
            theParamName.Truncate(thePrefLen).Append(
            "X509.forceReload", 0) != 0;
        if (theX509ChangedFlag) {
            const bool kServerFlag  = true;
            const bool kPskOnlyFlag = false;
            string     theErrMsg;
            mSslCtxPtr.Set(SslFilter::CreateCtx(
                kServerFlag,
                kPskOnlyFlag,
                theParamName.Truncate(thePrefLen).Append("X509.").GetPtr(),
                theKrbSslProps,
                &theErrMsg
            ));
            if (! mSslCtxPtr.Get()) {
                KFS_LOG_STREAM_ERROR <<
                    theParamName.Truncate(thePrefLen) <<
                    "krb5.* configuration error: " << theErrMsg <<
                KFS_LOG_EOM;
                return false;
            }
        }
        if (theKrbChangedFlag) {
            mPrincipalUnparseFlags = thePrincipalUnparseFlags;
            mKrbServicePtr.swap(theKrbServicePtr);
            mKrbProps.swap(theKrbProps);
        }
        if (theKrbSslChangedFlag) {
            mSslCtxPtr.Swap(theSslCtxPtr);
            mKrbSslProps.swap(theKrbSslProps);
        }
        if (theX509ChangedFlag) {
            mX509SslCtxPtr.Swap(theX509SslCtxPtr);
            mX509SslProps.swap(theX509SslProps);
        }
        return true;
    }
private:
    typedef scoped_ptr<KrbService> KrbServicePtr;

    class SslCtxPtr
    {
    public:
        SslCtxPtr(
            SslFilter::Ctx* inCtxPtr = 0)
            : mCtxPtr(inCtxPtr)
            {}
        ~SslCtxPtr()
            { SslFilter::FreeCtx(mCtxPtr); }
        void Swap(
            SslCtxPtr& inCtx)
        {
            SslFilter::Ctx* const theTmpPtr = inCtx.mCtxPtr;
            inCtx.mCtxPtr = mCtxPtr;
            mCtxPtr = theTmpPtr;
        }
        SslFilter::Ctx* Get() const
            { return mCtxPtr; }
        void Set(
            SslFilter::Ctx* inCtxPtr)
        {
            SslFilter::FreeCtx(mCtxPtr);
            mCtxPtr = inCtxPtr;
        }
    private:
        SslFilter::Ctx* mCtxPtr;
    private:
        SslCtxPtr(
            const SslCtxPtr& inCtxPtr);
        SslCtxPtr& operator=(
            const SslCtxPtr& inCtxPtr);
    };

    Properties    mKrbProps;
    Properties    mKrbSslProps;
    Properties    mX509SslProps;
    KrbServicePtr mKrbServicePtr;
    SslCtxPtr     mSslCtxPtr;
    SslCtxPtr     mX509SslCtxPtr;
    int           mPrincipalUnparseFlags;
    unsigned int  mMemKeytabGen;

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
