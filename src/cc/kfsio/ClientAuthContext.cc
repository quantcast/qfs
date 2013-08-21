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

#include "common/kfstypes.h"
#include "common/Properties.h"
#include "common/MsgLogger.h"
#include "kfsio/NetConnection.h"
#include "kfsio/SslFilter.h"
#include "krb/KrbClient.h"
#include "qcdio/qcdebug.h"

#include <boost/shared_ptr.hpp>
#include <errno.h>

namespace KFS
{

using std::string;
using boost::shared_ptr;

class ClientAuthContext::RequestCtxImpl
{
private:
    typedef shared_ptr<KrbClient> KrbClientPtr;
    RequestCtxImpl()
        : mOuterPtr(0),
          mKrbClientPtr(),
          mSessionKeyPtr(0),
          mSessionKeyLen(0),
          mAuthType(kAuthenticationTypeUndef),
          mInvalidFlag(false)
        {}
    void Reset()
        { *this = RequestCtxImpl(); }
    RequestCtx*  mOuterPtr;
    KrbClientPtr mKrbClientPtr;
    const char*  mSessionKeyPtr;
    int          mSessionKeyLen;
    int          mAuthType;
    bool         mInvalidFlag;
friend class ClientAuthContext::Impl;
};

class ClientAuthContext::Impl
{
public:
    Impl()
        : mCurRequest(),
          mEnabledFlag(false),
          mAuthNoneEnabledFlag(false),
          mParams(),
          mKrbClientPtr(),
          mSslCtxPtr(),
          mX509SslCtxPtr()
        {}
    ~Impl()
    {
        if (mCurRequest.mOuterPtr) {
            RequestCtxImpl*& theImplPtr = mCurRequest.mOuterPtr->mImplPtr;
            if (theImplPtr->mKrbClientPtr) {
                // If kerberos request is in flight, keep kerberos client
                // context around until outer destructor is invoked to ensure
                // that the request buffers are still valid.
                theImplPtr = new RequestCtxImpl();
                theImplPtr->mKrbClientPtr.swap(mCurRequest.mKrbClientPtr);
            } else {
                theImplPtr = 0;
            }
            mCurRequest.mOuterPtr = 0;
        }
    }
    int SetParameters(
        const char*       inParamsPrefixPtr,
        const Properties& inParameters,
        string*           outErrMsgPtr)
    {
        Properties::String theParamName;
        if (inParamsPrefixPtr) {
            theParamName.Append(inParamsPrefixPtr);
        }
        const size_t thePrefLen = theParamName.GetSize();
        Properties theParams(mParams);
        inParameters.copyWithPrefix(
            theParamName.GetPtr(), theParamName.GetSize(), theParams);
        const char* theNullStr         = 0;
        const char* theServeiceNamePtr = theParams.getValue(
            theParamName.Truncate(thePrefLen).Append(
            "krb5.serviceName"), theNullStr);
        theParamName.Truncate(thePrefLen).Append("krb5.");
        size_t theCurLen = theParamName.GetSize();
        const bool theKrbChangedFlag =
            theParams.getValue(theParamName.Append("forceReload"), 0) != 0 ||
            ! theParams.equalsWithPrefix(
                theParamName.Truncate(theCurLen).GetPtr(), theCurLen, mParams);
        KrbClientPtr theKrbClientPtr;
        if (theKrbChangedFlag && theServeiceNamePtr && theServeiceNamePtr[0]) {
            theKrbClientPtr.reset(new KrbClient());
            const char* const theErrMsgPtr = theKrbClientPtr->Init(
                theParams.getValue(
                    theParamName.Truncate(thePrefLen).Append(
                    "krb5.serviceHost"), theNullStr),
                theServeiceNamePtr,
                theParams.getValue(
                    theParamName.Truncate(thePrefLen).Append(
                    "krb5.keytab"), theNullStr),
                theParams.getValue(
                    theParamName.Truncate(thePrefLen).Append(
                    "krb5.clientName"), theNullStr),
                theParams.getValue(
                    theParamName.Truncate(thePrefLen).Append(
                    "krb5.initClientCache"), 0) != 0
            );
            if (theErrMsgPtr) {
                if (outErrMsgPtr) {
                    *outErrMsgPtr = theErrMsgPtr;
                }
                KFS_LOG_STREAM_ERROR <<
                    theParamName.Truncate(thePrefLen) <<
                    "krb5.* configuration error: " << theErrMsgPtr <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
        }
        theParamName.Truncate(thePrefLen).Append("psk.tls.");
        theCurLen = theParamName.GetSize();
        const bool thePskSslChangedFlag =
            (theKrbChangedFlag &&
                    (mKrbClientPtr.get() != 0) !=
                    (theKrbClientPtr.get() != 0)) ||
            theParams.getValue(
                theParamName.Append("forceReload"), 0) != 0 ||
            ! theParams.equalsWithPrefix(
                theParamName.Truncate(theCurLen).GetPtr(), theCurLen, mParams);
        SslCtxPtr theSslCtxPtr;
        if (thePskSslChangedFlag && theParams.getValue(
                theParamName.Truncate(thePrefLen).Append(
                    "psk.tls.disable"), 0) == 0) {
            const bool kServerFlag  = false;
            const bool kPskOnlyFlag = true;
            string     theErrMsg;
            mSslCtxPtr.Set(SslFilter::CreateCtx(
                kServerFlag,
                kPskOnlyFlag,
                theParamName.Truncate(thePrefLen).Append("psk.tls.").GetPtr(),
                theParams,
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
        theParamName.Truncate(thePrefLen).Append("X509.");
        theCurLen = theParamName.GetSize();
        const bool theX509ChangedFlag =
            theParams.getValue(
                theParamName.Append("forceReload"), 0) != 0 ||
            ! theParams.equalsWithPrefix(
                theParamName.Truncate(theCurLen).GetPtr(), theCurLen, mParams);
        SslCtxPtr theX509SslCtxPtr;
        if (theX509ChangedFlag) {
            const bool kServerFlag  = false;
            const bool kPskOnlyFlag = false;
            string     theErrMsg;
            mSslCtxPtr.Set(SslFilter::CreateCtx(
                kServerFlag,
                kPskOnlyFlag,
                theParamName.Truncate(thePrefLen).Append("X509.").GetPtr(),
                theParams,
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
        mAuthNoneEnabledFlag = theParams.getValue(
            theParamName.Truncate(thePrefLen).Append("authNone.enabled"),
            0) != 0;
        mParams.swap(theParams);
        if (theKrbChangedFlag) {
            mKrbClientPtr.swap(theKrbClientPtr);
        }
        if (thePskSslChangedFlag) {
            mSslCtxPtr.Swap(theSslCtxPtr);
        }
        if (theX509ChangedFlag) {
            mX509SslCtxPtr.Swap(theX509SslCtxPtr);
        }
        if (theKrbChangedFlag || thePskSslChangedFlag || theX509ChangedFlag) {
           mCurRequest.mInvalidFlag = true;
        }
        return 0;
    }
    int Request(
        int          inAuthType,
        int&         outAuthType,
        const char*& outBufPtr,
        int&         outBufLen,
        RequestCtx&  inRequestCtx,
        string*      outErrMsgPtr)
    {
        Dispose(inRequestCtx);
        outAuthType = kAuthenticationTypeUndef;
        outBufPtr   = 0;
        outBufLen   = 0;
        if (mCurRequest.mOuterPtr) {
            if (outErrMsgPtr) {
                *outErrMsgPtr =
                    "request: invalid client auth. context use / invocation";
            }
            return -EINVAL;
        }
        mCurRequest.Reset();
        if (! mEnabledFlag) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "client auth. disabled";
            }
            return -EINVAL;
        }
        if ((inAuthType & kAuthenticationTypeKrb5) != 0 && mKrbClientPtr) {
            const char* const theErrMsgPtr = mKrbClientPtr->Request(
                outBufPtr,
                outBufLen,
                mCurRequest.mSessionKeyPtr,
                mCurRequest.mSessionKeyLen
            );
            if (theErrMsgPtr) {
                if (outErrMsgPtr) {
                    *outErrMsgPtr = theErrMsgPtr;
                }
                return mKrbClientPtr->GetErrorCode();
            }
            outAuthType = kAuthenticationTypeKrb5;
            mCurRequest.mAuthType = kAuthenticationTypeKrb5;
            mCurRequest.mOuterPtr = &inRequestCtx;
            inRequestCtx.mImplPtr = &mCurRequest;
            if (outBufPtr && outBufLen > 0) {
                mCurRequest.mKrbClientPtr = mKrbClientPtr;
            }
            return 0;
        }
        if ((inAuthType & kAuthenticationTypeX509) != 0 &&
                mX509SslCtxPtr.Get()) {
            outAuthType = kAuthenticationTypeX509;
            mCurRequest.mAuthType = kAuthenticationTypeX509;
            mCurRequest.mOuterPtr = &inRequestCtx;
            inRequestCtx.mImplPtr = &mCurRequest;
            return 0;
        }
        if ((inAuthType & kAuthenticationTypeNone) != 0 &&
                mAuthNoneEnabledFlag) {
            outAuthType = kAuthenticationTypeNone;
            mCurRequest.mAuthType = kAuthenticationTypeNone;
            mCurRequest.mOuterPtr = &inRequestCtx;
            inRequestCtx.mImplPtr = &mCurRequest;
            return 0;
        }
        if (outErrMsgPtr) {
            *outErrMsgPtr = "no common auth. method";
        }
        return -ENOENT;
    }
    int Response(
        int            inAuthType,
        bool           inUseSslFlag,
        const char*    inBufPtr,
        int            inBufLen,
        NetConnection& inNetConnection,
        RequestCtx&    inRequestCtx,
        string*        outErrMsgPtr)
    {
        if (&mCurRequest != inRequestCtx.mImplPtr) {
            if (outErrMsgPtr) {
                *outErrMsgPtr =
                    "response: invalid client auth. context use / invocation";
            }
            return -EINVAL;
        }
        if (mCurRequest.mInvalidFlag) {
            if (outErrMsgPtr) {
                *outErrMsgPtr =
                    "client auth. configuration has changed, try again";
            }
            return -EAGAIN;
        }
        return 0;
    }
    int StartSsl(
        NetConnection& inNetConnection,
        const char*    inKeyIdPtr,
        const char*    inKeyDataPtr,
        int            inKeyDataSize,
        string*        outErrMsgPtr)
    {
        return 0;
    }
    bool IsEnabled() const
        { return mEnabledFlag; }
    int CheckAuthType(
        int     inAuthType,
        string* outErrMsgPtr)
    {
        return 0;
    }
    static void Dispose(
        RequestCtx& inRequestCtx)
    {
        if (! inRequestCtx.mImplPtr) {
            return;
        }
        Dispose(*inRequestCtx.mImplPtr);
        inRequestCtx.mImplPtr = 0;
    }
    static void Dispose(
        RequestCtxImpl& inRequestCtxImpl)
    {
        if (inRequestCtxImpl.mOuterPtr) {
            inRequestCtxImpl.Reset();
            return;
        }
        delete &inRequestCtxImpl;
    }
private:
    typedef ClientAuthContext::RequestCtxImpl::KrbClientPtr KrbClientPtr;
    typedef SslFilter::CtxPtr                               SslCtxPtr;
    typedef ClientAuthContext::RequestCtxImpl               RequestCtxImpl;

    RequestCtxImpl  mCurRequest;
    bool            mEnabledFlag;
    bool            mAuthNoneEnabledFlag;
    Properties      mParams;
    KrbClientPtr    mKrbClientPtr;
    SslCtxPtr       mSslCtxPtr;
    SslCtxPtr       mX509SslCtxPtr;
};

ClientAuthContext::ClientAuthContext()
    : mImpl(*(new Impl()))
    {}

ClientAuthContext::~ClientAuthContext()
{
    delete &mImpl;
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
    const Properties& inParameters,
    string*           outErrMsgPtr)
{
    return mImpl.SetParameters(inParamsPrefixPtr, inParameters, outErrMsgPtr);
}

    int
ClientAuthContext::Request(
    int                            inAuthType,
    int&                           outAuthType,
    const char*&                   outBufPtr,
    int&                           outBufLen,
    ClientAuthContext::RequestCtx& inRequestCtx,
    string*                        outErrMsgPtr)
{
    return mImpl.Request(
        inAuthType, outAuthType, outBufPtr, outBufLen, inRequestCtx,
        outErrMsgPtr);
}

    int
ClientAuthContext::Response(
    int                            inAuthType,
    bool                           inUseSslFlag,
    const char*                    inBufPtr,
    int                            inBufLen,
    NetConnection&                 inNetConnection,
    ClientAuthContext::RequestCtx& inRequestCtx,
    string*                        outErrMsgPtr)
{
    return mImpl.Response(
        inAuthType, inUseSslFlag, inBufPtr, inBufLen, inNetConnection,
        inRequestCtx, outErrMsgPtr);
}

    int
ClientAuthContext::StartSsl(
    NetConnection& inNetConnection,
    const char*    inKeyIdPtr,
    const char*    inKeyDataPtr,
    int            inKeyDataSize,
    string*        outErrMsgPtr)
{
    return mImpl.StartSsl(
        inNetConnection, inKeyIdPtr, inKeyDataPtr, inKeyDataSize, outErrMsgPtr);
}

    bool
ClientAuthContext::IsEnabled() const
{
    return mImpl.IsEnabled();
}

    /* static */ void
ClientAuthContext::Dispose(
    ClientAuthContext::RequestCtxImpl& inRequestCtxImpl)
{
    Impl::Dispose(inRequestCtxImpl);
}


} // namespace KFS
