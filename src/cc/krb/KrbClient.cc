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
#include <errno.h>

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
          mKeyTabFileName(),
          mClientName(),
          mCtx(),
          mAuthCtx(),
          mErrCode(0),
          mOutBuf(),
          mCreds(),
          mServerPtr(0),
          mClientPtr(0),
          mKeyBlockPtr(0),
          mCachePtr(0),
          mInitOptionsPtr(0),
          mKeyTabPtr(0),
          mInitedFlag(false),
          mUseKeyTabFlag(false),
          mFreeCredsFlag(false),
          mServiceName(),
          mErrorMsg()
    {
        mOutBuf.data   = 0;
        mOutBuf.length = 0;
        memset(&mCreds, 0, sizeof(mCreds));
    }
    ~Impl()
        { Impl::CleanupSelf(); }
    const char* Init(
        const char* inServiceHostNamePtr,
        const char* inServeiceNamePtr,
        const char* inKeyTabNamePtr,
        const char* inClientNamePtr)
    {
        CleanupSelf();
        mServiceHost    = inServiceHostNamePtr ? inServiceHostNamePtr : "";
        mServiceName    = inServeiceNamePtr    ? inServeiceNamePtr    : "";
        mKeyTabFileName = inKeyTabNamePtr ? inKeyTabNamePtr : "";
        mClientName     = inClientNamePtr ? inClientNamePtr : "";
        mUseKeyTabFlag  = inKeyTabNamePtr != 0;
        mErrCode     = 0;
        mErrorMsg.clear();
        InitSelf();
        if (mErrCode) {
            return ErrStr();
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
        int&         outDataLen,
        const char*& outSessionKeyPtr,
        int&         outSessionKeyLen)
    {
        if (! mInitedFlag) {
            mErrCode  = EINVAL;
            mErrorMsg = "not initialized yet, invoke KrbClient::Init";
            return mErrorMsg.c_str();
        }
        CleanupAuth();
        mCreds.server = mServerPtr;
        if (mClientPtr && ! mCreds.client) {
            mCreds.client = mClientPtr;
        }
	if (! mClientPtr &&
                (mErrCode = krb5_cc_get_principal(
                    mCtx, mCachePtr, &mCreds.client)) != 0) {
            return ErrStr();
        }
        krb5_creds* theCredsPtr = 0;
	if ((mErrCode = krb5_get_credentials(
                mCtx, 0, mCachePtr, &mCreds, &theCredsPtr)) != 0) {
            return ErrStr();
        }
#ifdef _KFS_KRB_CLIENT_SET_AUTH_FLAGS
        krb5_int32 theFlags = 0;
        if ((mErrCode = krb5_auth_con_init(mCtx, &mAuthCtx)) ||
               (mErrCode = krb5_auth_con_getflags(mCtx, mAuthCtx, &theFlags))) {
            krb5_free_creds(mCtx, theCredsPtr);
            return ErrStr();
        }
        theFlags |= KRB5_AUTH_CONTEXT_DO_SEQUENCE;
        theFlags &= ~(KRB5_AUTH_CONTEXT_DO_TIME | KRB5_AUTH_CONTEXT_RET_TIME);
        if ((mErrCode = krb5_auth_con_setflags(mCtx, mAuthCtx, theFlags))) {
            krb5_free_creds(mCtx, theCredsPtr);
            return ErrStr();
        }
#endif
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
        if ((mErrCode = krb5_auth_con_getkey(mCtx, mAuthCtx, &mKeyBlockPtr))) {
            return ErrStr();
        }
        outDataPtr       = (const char*)mOutBuf.data;
        outDataLen       = (int)mOutBuf.length;
        outSessionKeyPtr =
            reinterpret_cast<const char*>(mKeyBlockPtr->contents);
        outSessionKeyLen = (int)mKeyBlockPtr->length;
        return 0;
    }
    const char* Reply(
        const char*  inReplyPtr,
        int          inReplyLen)
    {
        if (! mInitedFlag) {
            mErrCode  = EINVAL;
            mErrorMsg = "not initialized yet, invoke KrbClient::Init";
            return mErrorMsg.c_str();
        }
        if (mOutBuf.length <= 0 || ! mOutBuf.data) {
            mErrCode  = EINVAL;
            mErrorMsg = "not ready to process reply, invoke KrbClient::Request";
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
        krb5_free_ap_rep_enc_part(mCtx, theReplPtr);
        krb5_free_data_contents(mCtx, &mOutBuf);
        return 0;
    }
    int GetErrorCode() const
    {
        return mErrCode;
    }
private:
    string                   mServiceHost;
    string                   mKeyTabFileName;
    string                   mClientName;
    krb5_context             mCtx;
    krb5_auth_context        mAuthCtx;
    krb5_error_code          mErrCode;
    krb5_data                mOutBuf;
    krb5_creds               mCreds;
    krb5_principal           mServerPtr;
    krb5_principal           mClientPtr;
    krb5_keyblock*           mKeyBlockPtr;
    krb5_ccache              mCachePtr;
    krb5_get_init_creds_opt* mInitOptionsPtr;
    krb5_keytab              mKeyTabPtr;
    bool                     mInitedFlag;
    bool                     mUseKeyTabFlag;
    bool                     mFreeCredsFlag;
    string                   mServiceName;
    string                   mErrorMsg;

    void InitCredCacheKeytab()
    {
        if (! mInitedFlag) {
            mErrCode  = EINVAL;
            return;
        }
        mInitOptionsPtr = 0;
#ifdef _KFS_KRB_CLIENT_USE_INIT_OPTIONS
        if ((mErrCode = krb5_get_init_creds_opt_alloc(
                mCtx, &mInitOptionsPtr))) {
            return;
        }
        if ((mErrCode = krb5_get_init_creds_opt_set_out_ccache(
                mCtx, mInitOptionsPtr, mCachePtr))) {
            return;
        }
#endif
        mKeyTabPtr = 0;
        if ((mErrCode = mKeyTabFileName.empty() ?
                krb5_kt_default(mCtx, &mKeyTabPtr) :
                krb5_kt_resolve(mCtx, mKeyTabFileName.c_str(),
                &mKeyTabPtr))) {
            return;
        }
        mClientPtr = 0;
        if ((mErrCode = krb5_parse_name(
                mCtx, mClientName.c_str(), &mClientPtr))) {
            return;
        }
        mErrCode = krb5_get_init_creds_keytab(
                mCtx,
                &mCreds,
                mClientPtr,
                mKeyTabPtr,
                0,
                0,
                mInitOptionsPtr
        );
        mFreeCredsFlag = mErrCode == 0;
    }
    void InitSelf()
    {
        mErrCode = krb5_init_context(&mCtx);
        if (mErrCode) {
            return;
        }
        mInitedFlag = true;
        memset(&mCreds, 0, sizeof(mCreds));
        mCachePtr = 0;
	if ((mErrCode = krb5_sname_to_principal(
                mCtx,
                mServiceHost.c_str(),
                mServiceName.c_str(),
                KRB5_NT_UNKNOWN, // KRB5_NT_SRV_HST,
                &mServerPtr
            ))) {
            return;
        }
	if ((mErrCode = krb5_cc_default(mCtx, &mCachePtr)) != 0) {
            return;
        }
        if (mUseKeyTabFlag) {
            InitCredCacheKeytab();
        }
    }
    krb5_error_code CleanupSelf()
    {
        if (! mInitedFlag) {
            return 0;
        }
        krb5_error_code theErr = CleanupAuth();
        mInitedFlag = false;
        if (mFreeCredsFlag) {
            if (mClientPtr && mClientPtr == mCreds.client) {
                mCreds.client = 0;
            }
            krb5_free_cred_contents(mCtx, &mCreds);
            mFreeCredsFlag = false;
        }
        memset(&mCreds, 0, sizeof(mCreds));
	if (mServerPtr) {
            krb5_free_principal(mCtx, mServerPtr);
            mServerPtr = 0;
        }
	if (mClientPtr) {
            krb5_free_principal(mCtx, mClientPtr);
            mClientPtr = 0;
        }
        if (mCachePtr) {
            krb5_error_code const theCErr = krb5_cc_close(mCtx, mCachePtr);
            mCachePtr = 0;
            if (! theErr) {
                theErr = theCErr;
            }
        }
#ifdef _KFS_KRB_CLIENT_USE_INIT_OPTIONS
        if (mInitOptionsPtr) {
            krb5_get_init_creds_opt_free(mCtx, mInitOptionsPtr);
            mInitOptionsPtr = 0;
        }
#endif
        if (mKeyTabPtr) {
            krb5_error_code const theCloseErr = krb5_kt_close(mCtx, mKeyTabPtr);
            mKeyTabPtr = 0;
            if (! theErr && theCloseErr) {
                theErr = theCloseErr;
            }
        }
        krb5_free_context(mCtx);
        return theErr;
    }
    krb5_error_code CleanupAuth()
    {
        if (! mInitedFlag) {
            return 0;
        }
	if (! mClientPtr && mCreds.client) {
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
        const string theMsg((theMsgPtr && *theMsgPtr) ?
            theMsgPtr : "unspecified kerberos error");
        if (theMsgPtr) {
            // cast away const to make it compatible with older krb5 releases.
            krb5_free_error_message(mCtx, const_cast<char*>(theMsgPtr));
        }
        return theMsg;
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
    const char* inServeiceNamePtr,
    const char* inKeyTabNamePtr,
    const char* inClientNamePtr)
{
    return mImpl.Init(
        inServiceHostNamePtr,
        inServeiceNamePtr,
        inKeyTabNamePtr,
        inClientNamePtr
    );
}

    const char*
KrbClient::Cleanup()
{
    return mImpl.Cleanup();
}

    const char*
KrbClient::Request(
    const char*& outDataPtr,
    int&         outDataLen,
    const char*& outSessionKeyPtr,
    int&         outSessionKeyLen)
{
    return mImpl.Request(
        outDataPtr, outDataLen, outSessionKeyPtr, outSessionKeyLen);
}

    const char*
KrbClient::Reply(
    const char* inReplyPtr,
    int         inReplyLen)
{
    return mImpl.Reply(
        inReplyPtr, inReplyLen);
}

    int
KrbClient::GetErrorCode() const
{
    return mImpl.GetErrorCode();
}

}
