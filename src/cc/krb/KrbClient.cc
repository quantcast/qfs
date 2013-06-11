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

#include "KrbClient.h"


#include <krb5/krb5.h>

#include <string.h>
#include <string>
#include <algorithm>

namespace KFS
{

using std::string;
using std::max;

class KrbClient::Impl
{
public:
    Impl()
        : mServiceHost(),
          mCtx(),
          mAuthCtx(),
          mErrCode(0),
          mOutBuf(),
          mCreds(),
          mServerPtr(0),
          mKeyBlockPtr(0),
          mInitedFlag(false),
          mServiceName(),
          mErrorMsg()
    {
        mOutBuf.data   = 0;
        mOutBuf.length = 0;
    }
    ~Impl()
        {}
    const char* Init(
        const char* inServiceHostNamePtr,
        const char* inServeiceNamePtr)
    {
        CleanupSelf();
        mServiceHost = inServiceHostNamePtr ? inServiceHostNamePtr : "";
        mServiceName = inServeiceNamePtr    ? inServeiceNamePtr    : "";
        mErrCode     = 0;
        mErrorMsg.clear();
        InitSelf();
        if (mErrCode) {
            mErrorMsg = ErrToStr(mErrCode);
            return mErrorMsg.c_str();
        }
        return 0;
    }
    const char* Cleanup()
    {
        mErrorMsg.clear();
        mErrCode = CleanupSelf();
        if (mErrCode) {
            return ErrStr();
        }
        return 0;
    }
    const char* Request(
        const char*& outDataPtr,
        int&         outDataLen)
    {
        if (! mInitedFlag) {
            mErrCode  = KRB5_CONFIG_BADFORMAT;
            mErrorMsg = "not initialized yet, invoke KrbClient::Init";
            return mErrorMsg.c_str();
        }
        CleanupAuth();
        krb5_free_data_contents(mCtx, &mOutBuf);
        krb5_ccache theCachePtr = 0;
	if ((mErrCode = krb5_cc_default(mCtx, &theCachePtr)) != 0) {
            return ErrStr();
        }
        mCreds.server = mServerPtr;
	if ((mErrCode = krb5_cc_get_principal(
                mCtx, theCachePtr, &mCreds.client)) != 0) {
            krb5_cc_close(mCtx, theCachePtr);
            return ErrStr();
        }
        krb5_creds* theCredsPtr = 0;
	if ((mErrCode = krb5_get_credentials(
                mCtx, 0, theCachePtr, &mCreds, &theCredsPtr)) != 0) {
            krb5_cc_close(mCtx, theCachePtr);
            return ErrStr();
        }
        if ((mErrCode = krb5_cc_close(mCtx, theCachePtr))) {
            krb5_free_creds(mCtx, theCredsPtr);
            return ErrStr();
        }
        krb5_data theAppData = { 0 };
        theAppData.data   = 0;
        theAppData.length = 0;
        mErrCode = krb5_mk_req_extended(
            mCtx,
            &mAuthCtx,
            AP_OPTS_MUTUAL_REQUIRED,
            &theAppData,
            theCredsPtr,
            &mOutBuf
        );
        krb5_free_creds(mCtx, theCredsPtr);
        if (mErrCode != 0) {
            return ErrStr();
        }
        outDataPtr = (const char*)mOutBuf.data;
        outDataLen = (int)mOutBuf.length;
        return 0;
    }
    const char* Reply(
        const char*  inReplyPtr,
        int          inReplyLen,
        const char*& outSessionKeyPtr,
        int&         outSessionKeyLen)
    {
        if (! mInitedFlag) {
            mErrCode  = KRB5_CONFIG_BADFORMAT;
            mErrorMsg = "not initialized yet, invoke KrbClient::Init";
            return mErrorMsg.c_str();
        }
        if (mOutBuf.length <= 0 || ! mOutBuf.data) {
            mErrCode  = KRB5_CONFIG_BADFORMAT;
            mErrorMsg = "not ready to process reply, invoke KrbClient::Request";
            return mErrorMsg.c_str();
        }
        if (mKeyBlockPtr) {
            mErrCode  = KRB5_CONFIG_BADFORMAT;
            mErrorMsg = "possible extraneous invocation of KrbClient::Reply";
            return mErrorMsg.c_str();
        }
        krb5_data theData = { 0 };
        theData.length = max(0, inReplyLen);
        theData.data   = const_cast<char*>(inReplyPtr);
        krb5_ap_rep_enc_part* theReplPtr = 0;
        if ((mErrCode = krb5_rd_rep(
                mCtx,
                mAuthCtx,
                &theData,
                &theReplPtr))) {
            return ErrStr();
        }
        if ((mErrCode = krb5_auth_con_getkey(mCtx, mAuthCtx, &mKeyBlockPtr))) {
            return ErrStr();
        }
        krb5_free_ap_rep_enc_part(mCtx, theReplPtr);
        krb5_free_data_contents(mCtx, &mOutBuf);
        outSessionKeyPtr =
            reinterpret_cast<const char*>(mKeyBlockPtr->contents);
        outSessionKeyLen = (int)mKeyBlockPtr->length;
        return 0;
    }
    int GetErrorCode() const
    {
        return mErrCode;
    }
private:
    string            mServiceHost;
    krb5_context      mCtx;
    krb5_auth_context mAuthCtx;
    krb5_error_code   mErrCode;
    krb5_data         mOutBuf;
    krb5_creds        mCreds;
    krb5_principal    mServerPtr;
    krb5_keyblock*    mKeyBlockPtr;
    bool              mInitedFlag;
    string            mServiceName;
    string            mErrorMsg;

    void InitSelf()
    {
        mErrCode = krb5_init_context(&mCtx);
        if (mErrCode) {
            return;
        }
        mInitedFlag = true;
        memset(&mCreds, 0, sizeof(mCreds));
	if ((mErrCode = krb5_sname_to_principal(
                mCtx,
                mServiceHost.c_str(),
                mServiceName.c_str(),
                KRB5_NT_UNKNOWN, // KRB5_NT_SRV_HST,
                &mServerPtr
            ))) {
            return;
        }
    }
    krb5_error_code CleanupSelf()
    {
        if (! mInitedFlag) {
            return 0;
        }
        krb5_error_code theErr = CleanupAuth();
        mInitedFlag = false;
        memset(&mCreds, 0, sizeof(mCreds));
	if (mServerPtr) {
            krb5_free_principal(mCtx, mServerPtr);
            mServerPtr = 0;
        }
        krb5_free_context(mCtx);
        return theErr;
    }
    krb5_error_code CleanupAuth()
    {
        if (! mInitedFlag) {
            return 0;
        }
	if (mCreds.client) {
            krb5_free_principal(mCtx, mCreds.client);
            mCreds.client = 0;
        }
        if (mKeyBlockPtr) {
            krb5_free_keyblock(mCtx, mKeyBlockPtr);
            mKeyBlockPtr = 0;
        }
        krb5_free_data_contents(mCtx, &mOutBuf);
        if (! mAuthCtx) {
            return 0;
        }
        const krb5_error_code theErr = krb5_auth_con_free(mCtx, mAuthCtx);
        memset(&mCreds, 0, sizeof(mCreds));
        mAuthCtx = 0;
        return theErr;
    }
    const char* ErrStr()
    {
        mErrorMsg = ErrToStr(mErrCode);
        return mErrorMsg.c_str();
    }
    string ErrToStr(
        krb5_error_code inErrCode) const
    {
        if (! inErrCode) {
            return string();
        }
        if ( ! mCtx) {
            return string("no kerberos context");
        }
        const char* const theMsgPtr = krb5_get_error_message(mCtx, inErrCode);
        return string(theMsgPtr);
    }

private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

KrbClient::KrbClient()
    : mImpl(*(new Impl()))
{
}

KrbClient::~KrbClient()
{
    delete &mImpl;
}

    const char*
KrbClient::Init(
    const char* inServiceHostNamePtr,
    const char* inServeiceNamePtr)
{
    return mImpl.Init(inServiceHostNamePtr, inServeiceNamePtr);
}

    const char*
KrbClient::Cleanup()
{
    return mImpl.Cleanup();
}

    const char*
KrbClient::Request(
    const char*& outDataPtr,
    int&         outDataLen)
{
    return mImpl.Request(outDataPtr, outDataLen);
}

    const char*
KrbClient::Reply(
    const char*  inReplyPtr,
    int          inReplyLen,
    const char*& outSessionKeyPtr,
    int&         outSessionKeyLen)
{
    return mImpl.Reply(
        inReplyPtr, inReplyLen, outSessionKeyPtr, outSessionKeyLen);
}

    int
KrbClient::GetErrorCode() const
{
    return mImpl.GetErrorCode();
}

}
