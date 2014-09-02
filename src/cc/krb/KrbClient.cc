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

#include "KfsKrb5.h"

#include <string.h>
#include <errno.h>
#include <time.h>

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
          mKeyBlockPtr(0),
          mCachePtr(0),
          mInitedFlag(false),
          mUseKeyTabFlag(false),
          mLastCredEndTime(-1),
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
        const char* inClientNamePtr,
        bool        inForceCacheInitFlag)
    {
        CleanupSelf();
        mServiceHost    = inServiceHostNamePtr ? inServiceHostNamePtr : "";
        mServiceName    = inServeiceNamePtr    ? inServeiceNamePtr    : "";
        mKeyTabFileName = inKeyTabNamePtr ? inKeyTabNamePtr : "";
        mClientName     = inClientNamePtr ? inClientNamePtr : "";
        mUseKeyTabFlag  = inKeyTabNamePtr != 0;
        mErrCode     = 0;
        mErrorMsg.clear();
        InitSelf(inForceCacheInitFlag);
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
        if (mUseKeyTabFlag && mLastCredEndTime <= time(0) + 5) {
            InitCredCacheKeyTab();
            if (mErrCode) {
                return ErrStr();
            }
        }
        mCreds.server = mServerPtr;
	if ((mErrCode = krb5_cc_get_principal(
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
        if (theCredsPtr) {
            mLastCredEndTime = theCredsPtr->times.endtime;
        }
        krb5_free_creds(mCtx, theCredsPtr);
        if (mErrCode != 0) {
            return ErrStr();
        }
        if ((mErrCode = krb5_auth_con_getkey(mCtx, mAuthCtx, &mKeyBlockPtr))) {
            return ErrStr();
        }
        outDataPtr       = (const char*)mOutBuf.data;
        outDataLen       = (int)mOutBuf.length;
        outSessionKeyPtr = KfsKrb5::get_key_block_contents(mKeyBlockPtr);
        outSessionKeyLen = KfsKrb5::get_key_block_length(mKeyBlockPtr);
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
        KfsKrb5::free_data_contents(mCtx, &mOutBuf);
        return 0;
    }
    int GetErrorCode() const
        { return (int)mErrCode; }
    time_t GetLastCredEndTime() const
        { return mLastCredEndTime; }
private:
    string            mServiceHost;
    string            mKeyTabFileName;
    string            mClientName;
    krb5_context      mCtx;
    krb5_auth_context mAuthCtx;
    krb5_error_code   mErrCode;
    krb5_data         mOutBuf;
    krb5_creds        mCreds;
    krb5_principal    mServerPtr;
    krb5_keyblock*    mKeyBlockPtr;
    krb5_ccache       mCachePtr;
    bool              mInitedFlag;
    bool              mUseKeyTabFlag;
    time_t            mLastCredEndTime;
    string            mServiceName;
    string            mErrorMsg;

    void InitCredCacheKeyTab()
    {
        if (! mInitedFlag) {
            mErrCode = EINVAL;
            return;
        }
        krb5_get_init_creds_opt* theInitOptionsPtr = 0;
#ifdef KFS_KRB_USE_KRB5_GET_INIT_CREDS_OPT
        if ((mErrCode = krb5_get_init_creds_opt_alloc(
                mCtx, &theInitOptionsPtr))) {
            return;
        }
        if ((mErrCode = KfsKrb5::get_init_creds_opt_set_out_ccache(
                mCtx, theInitOptionsPtr, mCachePtr)) == 0) {
#else
            krb5_get_init_creds_opt theInitOptions;
            krb5_get_init_creds_opt_init(&theInitOptions);
            theInitOptionsPtr = &theInitOptions;
#endif
            krb5_keytab theKeyTabPtr = 0;
            if ((mErrCode = mKeyTabFileName.empty() ?
                    krb5_kt_default(mCtx, &theKeyTabPtr) :
                    krb5_kt_resolve(mCtx, mKeyTabFileName.c_str(),
                        &theKeyTabPtr))) {
                return;
            }
            krb5_principal theClientPtr = 0;
            if ((mErrCode = krb5_parse_name(
                    mCtx, mClientName.c_str(), &theClientPtr)) == 0) {
                if ((mErrCode = krb5_get_init_creds_keytab(
                        mCtx,
                        &mCreds,
                        theClientPtr,
                        theKeyTabPtr,
                        0,
                        0,
                        theInitOptionsPtr)) == 0) {
#ifndef KFS_KRB_USE_KRB5_GET_INIT_CREDS_OPT
                    if ((mErrCode = krb5_cc_initialize(
                                mCtx, mCachePtr, mCreds.client)) == 0) {
                            mErrCode = krb5_cc_store_cred(
                                mCtx, mCachePtr, &mCreds);
                    }
#endif
                    if (theClientPtr && theClientPtr == mCreds.client) {
                        mCreds.client = 0;
                    }
                    krb5_free_cred_contents(mCtx, &mCreds);
                }
                memset(&mCreds, 0, sizeof(mCreds));
	        if (theClientPtr) {
                    krb5_free_principal(mCtx, theClientPtr);
                    theClientPtr = 0;
                }
            }
            if (theKeyTabPtr) {
                krb5_error_code const theErr = krb5_kt_close(
                    mCtx, theKeyTabPtr);
                if (! mErrCode && theErr) {
                    mErrCode = theErr;
                }
            }
#ifdef KFS_KRB_USE_KRB5_GET_INIT_CREDS_OPT
        }
        if (theInitOptionsPtr) {
            krb5_get_init_creds_opt_free(mCtx, theInitOptionsPtr);
        }
#endif
    }
    bool ComparePrincipal(
        const char*    inNamePtr,
        krb5_principal inPrinPtr)
    {
        krb5_principal thePrinPtr = 0;
        if ((mErrCode = krb5_parse_name(
                mCtx, inNamePtr, &thePrinPtr)) == 0) {
            const bool theRet = krb5_principal_compare(
                mCtx, inPrinPtr, thePrinPtr);
            krb5_free_principal(mCtx, thePrinPtr);
            return theRet;
        }
        return false;
    }
    void InitSelf(
        bool inForceCacheInitFlag)
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
            const char*  theDataPtr       = 0;
            int          theDataLen       = 0;
            const char*  theSessionKeyPtr = 0;
            int          theSessionKeyLen = 0;
            time_t const theNow           = time(0);
            mLastCredEndTime = theNow + 24 * 3600;
            if (inForceCacheInitFlag ||
                    Request(
                        theDataPtr,
                        theDataLen,
                        theSessionKeyPtr,
                        theSessionKeyLen) ||
                    ! mCreds.client ||
                    mLastCredEndTime <= theNow + 10 ||
                    ! ComparePrincipal(mClientName.c_str(),
                        mCreds.client)) {
                CleanupAuth();
                mErrorMsg.clear();
                mErrCode = 0;
                InitCredCacheKeyTab();
            }
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
        if (mCachePtr) {
            krb5_error_code const theCErr = krb5_cc_close(mCtx, mCachePtr);
            mCachePtr = 0;
            if (! theErr) {
                theErr = theCErr;
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
	if (mCreds.client) {
            krb5_free_principal(mCtx, mCreds.client);
            mCreds.client = 0;
        }
        if (mKeyBlockPtr) {
            krb5_free_keyblock(mCtx, mKeyBlockPtr);
            mKeyBlockPtr = 0;
        }
        KfsKrb5::free_data_contents(mCtx, &mOutBuf);
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
    const char* inClientNamePtr,
    bool        inForceCacheInitFlag)
{
    return mImpl.Init(
        inServiceHostNamePtr,
        inServeiceNamePtr,
        inKeyTabNamePtr,
        inClientNamePtr,
        inForceCacheInitFlag
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

    time_t
KrbClient::GetLastCredEndTime() const
{
    return mImpl.GetLastCredEndTime();
}

}
