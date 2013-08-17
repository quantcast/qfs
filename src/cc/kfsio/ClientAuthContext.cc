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
#include "common/Properties.h"
#include "common/MsgLogger.h"
#include "kfsio/SslFilter.h"
#include "krb/KrbClient.h"
#include "qcdio/qcdebug.h"

#include <boost/scoped_ptr.hpp>
#include <errno.h>

namespace KFS
{

using std::string;
using boost::scoped_ptr;

class ClientAuthContext::Impl
{
public:
    Impl()
        : mSharedFlag(false),
          mEnabledFlag(false),
          mParams(),
          mKrbClientPtr(),
          mSslCtxPtr(),
          mX509SslCtxPtr()
        {}
    ~Impl()
        {}
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
        const char*    inKeyDataPtr,
        int            inKeyDataSize,
        string*        outErrMsgPtr)
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
        { return mEnabledFlag; }
    bool IsShared() const
        { return mSharedFlag; }
    void SetShared(
        bool inFlag)
    {
        mSharedFlag = inFlag;
    }
    int CheckAuthType(
        int     inAuthType,
        string* outErrMsgPtr)
    {
        return 0;
    }
private:
    typedef scoped_ptr<KrbClient> KrbClientPtr;
    typedef SslFilter::CtxPtr     SslCtxPtr;

    bool         mSharedFlag;
    bool         mEnabledFlag;
    Properties   mParams;
    KrbClientPtr mKrbClientPtr;
    SslCtxPtr    mSslCtxPtr;
    SslCtxPtr    mX509SslCtxPtr;
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
    const Properties& inParameters,
    string*           outErrMsgPtr)
{
    return mImpl.SetParameters(inParamsPrefixPtr, inParameters, outErrMsgPtr);
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
    const char*    inKeyDataPtr,
    int            inKeyDataSize,
    string*        outErrMsgPtr)
{
    return mImpl.StartSsl(
        inNetConnection, inKeyIdPtr, inKeyDataPtr, inKeyDataSize, outErrMsgPtr);
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
