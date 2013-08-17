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
#include "common/StdAllocator.h"
#include "kfsio/SslFilter.h"
#include "krb/KrbService.h"
#include "qcdio/qcdebug.h"

#include <algorithm>
#include <string>
#include <sstream>
#include <iomanip>
#include <set>
#include <map>

#include <boost/scoped_ptr.hpp>

namespace KFS
{
using std::string;
using std::max;
using std::ostringstream;
using std::istringstream;
using std::hex;
using std::set;
using std::pair;
using std::less;
using boost::scoped_ptr;

class AuthContext::Impl
{
public:
    Impl()
        : mKrbProps(),
          mPskSslProps(),
          mX509SslProps(),
          mKrbServicePtr(),
          mSslCtxPtr(0),
          mX509SslCtxPtr(0),
          mNameRemap(),
          mBlackList(),
          mWhiteList(),
          mNameRemapParam(),
          mBlackListParam(),
          mWhiteListParam(),
          mPrincipalUnparseFlags(0),
          mAuthNoneFlag(false),
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
        const bool theRetFlag =
            (mKrbServicePtr &&
                (inOp.authType & kAuthenticationTypeKrb5) != 0) ||
            (mX509SslCtxPtr.Get() &&
                (inOp.authType & kAuthenticationTypeX509) != 0) ||
            (mAuthNoneFlag &&
                    (inOp.authType & kAuthenticationTypeNone) != 0);
        if (! theRetFlag) {
            inOp.status    = -ENOENT;
            inOp.statusMsg = "authentication type is not configured";
        }
        return theRetFlag;
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
        if (mKrbServicePtr && (inOp.authType & kAuthenticationTypeKrb5) != 0) {
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
            string theAuthName(thePeerPrincipalPtr ? thePeerPrincipalPtr : "");
            if (! RemapAndValidate(theAuthName)) {
                inOp.status    = -EACCES;
                inOp.statusMsg = "access denied for '" + theAuthName + "'";
                inOp.responseContentPtr = 0;
                inOp.responseContentLen = 0;
                return false;
            }
            if (! mSslCtxPtr.Get()) {
                inOp.authName         = theAuthName;
                inOp.responseAuthType = inOp.authType;
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
            if (inOp.status == 0) {
                inOp.responseAuthType = kAuthenticationTypeKrb5;
            }
            return (inOp.status == 0);
        }
        if (mX509SslCtxPtr.Get() &&
                (inOp.authType & kAuthenticationTypeX509) != 0) {
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
            if (inOp.status == 0) {
                inOp.responseAuthType = kAuthenticationTypeX509;
            }
            return (inOp.status == 0);
        }
        QCASSERT((inOp.authType & kAuthenticationTypeNone) != 0 &&
            mAuthNoneFlag);
        if (inOp.status == 0) {
            inOp.responseAuthType = kAuthenticationTypeNone;
        }
        return (inOp.status == 0);
    }
    bool RemapAndValidate(
        string& ioAuthName) const
    {
        const NameRemap::const_iterator theIt = mNameRemap.find(ioAuthName);
        if (theIt != mNameRemap.end()) {
            ioAuthName = theIt->second;
        }
        return (
            mBlackList.find(ioAuthName) == mBlackList.end() &&
            (mWhiteList.empty() ||
                mWhiteList.find(ioAuthName) != mWhiteList.end()) &&
            ((! mKrbServicePtr && ! mX509SslCtxPtr.Get()) ||
                ! ioAuthName.empty())
        );
    }
    bool SetParameters(
        const char*       inParamNamePrefixPtr,
        const Properties& inParameters)
    {
        Properties::String theParamName;
        if (inParamNamePrefixPtr) {
            theParamName.Append(inParamNamePrefixPtr);
        }
        const size_t thePrefLen = theParamName.GetSize();
        const bool theAuthNoneFlag = inParameters.getValue(
            theParamName.Truncate(thePrefLen).Append(
            "noAuth"), mAuthNoneFlag ? 1 : 0) != 0;
        NameRemap theNameRemap;
        const string theNameRemapParam = inParameters.getValue(
            theParamName.Truncate(thePrefLen).Append(
            "nameRemap"), mNameRemapParam);
        if (! theNameRemapParam.empty() &&
                mNameRemapParam != theNameRemapParam) {
            istringstream theStream(theNameRemapParam);
            string theFrom;
            string theTo;
            while ((theStream >> theFrom >> theTo)) {
                theNameRemap[theFrom] = theTo;
            }
        }
        NameList     theBlackList;
        const string theBlackListParam = inParameters.getValue(
            theParamName.Truncate(thePrefLen).Append(
            "blackList"), mBlackListParam);
        if (! theBlackListParam.empty() &&
                mBlackListParam != theBlackListParam) {
            istringstream theStream(theBlackListParam);
            string        theName;
            while ((theStream >> theName)) {
                mBlackList.insert(theName);
            }
        }
        NameList     theWhiteList;
        const string theWhiteListParam = inParameters.getValue(
            theParamName.Truncate(thePrefLen).Append(
            "whiteList"), mWhiteListParam);
        if (! theWhiteListParam.empty() &&
                mWhiteListParam != theWhiteListParam) {
            istringstream theStream(theWhiteListParam);
            string        theName;
            while ((theStream >> theName)) {
                mWhiteList.insert(theName);
            }
        }
        Properties theKrbProps(mKrbProps);
        theParamName.Truncate(thePrefLen).Append("krb5.");
        inParameters.copyWithPrefix(
            theParamName.GetPtr(), theParamName.GetSize(), theKrbProps);
        const bool    theKrbChangedFlag        = theKrbProps != mKrbProps ||
            inParameters.getValue(
                theParamName.Truncate(thePrefLen).Append(
                "krb5.forceReload"), 0) != 0;
        int           thePrincipalUnparseFlags = 0;
        KrbServicePtr theKrbServicePtr;
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
                // No replay detection is needed, as either AP_REP or TLS-PSK
                // are used. Both these mechanisms are sufficient to protect
                // against replay attack as both provide mutual authentication.
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
        Properties thePskSslProps(mPskSslProps);
        theParamName.Truncate(thePrefLen).Append("psk.tls.");
        inParameters.copyWithPrefix(
            theParamName.GetPtr(), theParamName.GetSize(), thePskSslProps);
        const bool thePskSslChangedFlag =
            (theKrbChangedFlag &&
                    (mKrbServicePtr.get() != 0) !=
                    (theKrbServicePtr.get() != 0)) ||
            mPskSslProps != thePskSslProps ||
            inParameters.getValue(
                theParamName.Truncate(thePrefLen).Append(
                "psk.tls.forceReload"), 0) != 0;
        SslCtxPtr theSslCtxPtr;
        if (thePskSslChangedFlag && thePskSslProps.getValue(
                theParamName.Truncate(thePrefLen).Append(
                    "psk.tls.disable"), 0) == 0) {
            const bool kServerFlag  = true;
            const bool kPskOnlyFlag = true;
            string     theErrMsg;
            mSslCtxPtr.Set(SslFilter::CreateCtx(
                kServerFlag,
                kPskOnlyFlag,
                theParamName.Truncate(thePrefLen).Append("psk.tls.").GetPtr(),
                thePskSslProps,
                &theErrMsg
            ));
            if (! mSslCtxPtr.Get()) {
                KFS_LOG_STREAM_ERROR <<
                    theParamName.Truncate(thePrefLen) <<
                    "psk.tls.* configuration error: " << theErrMsg <<
                KFS_LOG_EOM;
                return false;
            }
        }
        Properties theX509SslProps(mX509SslProps);
        theParamName.Truncate(thePrefLen).Append("X509.");
        inParameters.copyWithPrefix(
            theParamName.GetPtr(), theParamName.GetSize(), theX509SslProps);
        const bool theX509ChangedFlag = theX509SslProps != mX509SslProps ||
            inParameters.getValue(
                theParamName.Truncate(thePrefLen).Append(
                "X509.forceReload"), 0) != 0;
        SslCtxPtr theX509SslCtxPtr;
        if (theX509ChangedFlag) {
            const bool kServerFlag  = true;
            const bool kPskOnlyFlag = false;
            string     theErrMsg;
            mSslCtxPtr.Set(SslFilter::CreateCtx(
                kServerFlag,
                kPskOnlyFlag,
                theParamName.Truncate(thePrefLen).Append("X509.").GetPtr(),
                theX509SslProps,
                &theErrMsg
            ));
            if (! mSslCtxPtr.Get()) {
                KFS_LOG_STREAM_ERROR <<
                    theParamName.Truncate(thePrefLen) <<
                    "X509.* configuration error: " << theErrMsg <<
                KFS_LOG_EOM;
                return false;
            }
        }
        if (theKrbChangedFlag) {
            mPrincipalUnparseFlags = thePrincipalUnparseFlags;
            mKrbServicePtr.swap(theKrbServicePtr);
            mKrbProps.swap(theKrbProps);
        }
        if (thePskSslChangedFlag) {
            mSslCtxPtr.Swap(theSslCtxPtr);
            mPskSslProps.swap(thePskSslProps);
        }
        if (theX509ChangedFlag) {
            mX509SslCtxPtr.Swap(theX509SslCtxPtr);
            mX509SslProps.swap(theX509SslProps);
        }
        if (mNameRemapParam != theNameRemapParam) {
            mNameRemap.swap(theNameRemap);
            mNameRemapParam = theNameRemapParam;
        }
        if (mBlackListParam != theBlackListParam) {
            mBlackList.swap(theBlackList);
            mBlackListParam = theBlackListParam;
        }
        if (mWhiteListParam != theWhiteListParam) {
            mWhiteList.swap(theWhiteList);
            mWhiteListParam = theWhiteListParam;
        }
        mAuthNoneFlag = theAuthNoneFlag;
        return true;
    }
private:
    typedef scoped_ptr<KrbService> KrbServicePtr;
    typedef map<
        string,
        string,
        less<string>,
        StdFastAllocator<pair<const string, string> >
    > NameRemap;
    typedef set<
        string,
        less<string>,
        StdFastAllocator<string>
    > NameList;
    typedef class SslFilter::CtxPtr SslCtxPtr;

    Properties    mKrbProps;
    Properties    mPskSslProps;
    Properties    mX509SslProps;
    KrbServicePtr mKrbServicePtr;
    SslCtxPtr     mSslCtxPtr;
    SslCtxPtr     mX509SslCtxPtr;
    NameRemap     mNameRemap;
    NameList      mBlackList;
    NameList      mWhiteList;
    string        mNameRemapParam;
    string        mBlackListParam;
    string        mWhiteListParam;
    int           mPrincipalUnparseFlags;
    bool          mAuthNoneFlag;
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
    if ((inOp.authType & kAuthenticationTypeKrb5)== 0 &&
            (inOp.authType & kAuthenticationTypeX509) == 0 &&
            (inOp.authType != kAuthenticationTypeNone) == 0) {
        inOp.status    = -EINVAL;
        inOp.statusMsg = "authentication type is not supported";
    } else {
        if (kMaxAuthenticationContentLength < inOp.contentLength) {
            inOp.status    = -EINVAL;
            inOp.statusMsg = "content length exceeds limit";
        } else if (inOp.contentLength > 0) {
            if ((inOp.authType & kAuthenticationTypeKrb5) == 0) {
                inOp.status    = -EINVAL;
                inOp.statusMsg = "invalid non zero content"
                    " length with non kerberos authentication";
            } else {
                inOp.contentBuf = new char [inOp.contentLength];
                inOp.contentBuf = 0;
            }
        } else {
            if ((inOp.authType & kAuthenticationTypeKrb5) != 0) {
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
    return mImpl.Authenticate(inOp);
}

    bool
AuthContext::RemapAndValidate(
    string& ioAuthName) const
{
    return mImpl.RemapAndValidate(ioAuthName);
}

    bool
AuthContext::SetParameters(
    const char*       inParamNamePrefixPtr,
    const Properties& inParameters)
{
    return mImpl.SetParameters(inParamNamePrefixPtr, inParameters);
}

}
