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
#include "common/RequestParser.h"
#include "common/StBuffer.h"
#include "kfsio/NetConnection.h"
#include "kfsio/SslFilter.h"
#include "kfsio/Base64.h"
#include "krb/KrbClient.h"
#include "qcdio/qcdebug.h"

#include <boost/shared_ptr.hpp>
#include <errno.h>

#include <algorithm>

namespace KFS
{

using std::string;
using std::max;
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
          mKrbAuthRequireSslFlag(false),
          mAuthRequiredFlag(false),
          mAllowCSClearTextFlag(true),
          mMaxAuthRetryCount(3),
          mParams(),
          mKrbClientPtr(),
          mSslCtxPtr(),
          mX509SslCtxPtr(),
          mPskKeyId(),
          mPskKey()
        {}
    ~Impl()
        { Impl::Clear(); }
    int SetParameters(
        const char*       inParamsPrefixPtr,
        const Properties& inParameters,
        Impl*             inOtherCtxPtr,
        string*           outErrMsgPtr,
        bool              inVerifyFlag)
    {
        Properties::String theParamName;
        if (inParamsPrefixPtr) {
            theParamName.Append(inParamsPrefixPtr);
        }
        const size_t thePrefLen = theParamName.GetSize();
        Properties theParams(mParams);
        inParameters.copyWithPrefix(
            theParamName.GetPtr(), theParamName.GetSize(), theParams);
        size_t theCurLen = theParamName.Truncate(thePrefLen).Append(
            "krb5.").GetSize();
        const char* theNullStr         = 0;
        const char* theServeiceNamePtr = theParams.getValue(
            theParamName.Append("service"), theNullStr);
        const bool theKrbChangedFlag =
            theParams.getValue(theParamName.Truncate(theCurLen).Append(
                "forceReload"), 0) != 0 ||
            ! theParams.equalsWithPrefix(
                theParamName.Truncate(theCurLen).GetPtr(), theCurLen, mParams);
        KrbClientPtr theKrbClientPtr;
        if (theKrbChangedFlag && theServeiceNamePtr && theServeiceNamePtr[0]) {
            theKrbClientPtr.reset(new KrbClient());
            const char* const theErrMsgPtr = theKrbClientPtr->Init(
                theParams.getValue(
                    theParamName.Truncate(theCurLen).Append(
                    "host"), theNullStr),
                theServeiceNamePtr,
                theParams.getValue(
                    theParamName.Truncate(theCurLen).Append(
                    "keytab"), theNullStr),
                theParams.getValue(
                    theParamName.Truncate(theCurLen).Append(
                    "clientName"), theNullStr),
                theParams.getValue(
                    theParamName.Truncate(theCurLen).Append(
                    "initClientCache"), 0) != 0
            );
            if (theErrMsgPtr) {
                if (outErrMsgPtr) {
                    *outErrMsgPtr = theErrMsgPtr;
                }
                KFS_LOG_STREAM_ERROR <<
                    theParamName.Truncate(theCurLen) <<
                    "* configuration error: " << theErrMsgPtr <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            if (inVerifyFlag) {
                const char* theBufPtr        = 0;
                int         theBufLen        = 0;
                const char* theSessionKeyPtr = 0;
                int         theSessionKeyLen = 0;
                const char* const theReqErrMsgPtr = theKrbClientPtr->Request(
                    theBufPtr,
                    theBufLen,
                    theSessionKeyPtr,
                    theSessionKeyLen);
                if (theReqErrMsgPtr) {
                    if (outErrMsgPtr) {
                        *outErrMsgPtr = "kerberos error: ";
                        *outErrMsgPtr += theReqErrMsgPtr;
                    }
                    KFS_LOG_STREAM_ERROR <<
                        "krb5 configuration error: " << theReqErrMsgPtr <<
                    KFS_LOG_EOM;
                    return -EINVAL;
                }
                if (theBufLen <= 0 || theSessionKeyLen <= 0) {
                    KFS_LOG_STREAM_ERROR <<
                        "krb5 request internal error:"
                        " key size: "     << theSessionKeyLen  <<
                        " request size: " << theBufLen <<
                    KFS_LOG_EOM;
                    return -EFAULT;
                }
            }
        }
        const bool theKrbRequireSslFlag = theParams.getValue(
            theParamName.Truncate(theCurLen).Append("requireSsl"),
            0) != 0;
        theCurLen = theParamName.Truncate(thePrefLen).Append("X509.").GetSize();
        const bool theCreateX509CtxFlag = theParams.getValue(
                theParamName.Append("PKeyPemFile")) != 0;
        const bool theX509ChangedFlag =
            theCreateX509CtxFlag != (mX509SslCtxPtr != 0) ||
             theParams.getValue(
                theParamName.Truncate(theCurLen).Append(
                    "forceReload"), 0) != 0 ||
           ! theParams.equalsWithPrefix(
                theParamName.Truncate(theCurLen).GetPtr(), theCurLen, mParams);
        SslCtxPtr theX509SslCtxPtr;
        string    theX509ExpectedName;
        if (theCreateX509CtxFlag && theX509ChangedFlag) {
            if (inOtherCtxPtr && inOtherCtxPtr->mX509SslCtxPtr) {
                theX509SslCtxPtr = inOtherCtxPtr->mX509SslCtxPtr;
            } else {
                const bool kServerFlag  = false;
                const bool kPskOnlyFlag = false;
                string     theErrMsg;
                theX509SslCtxPtr = SslFilter::MakeCtxPtr(SslFilter::CreateCtx(
                    kServerFlag,
                    kPskOnlyFlag,
                    theParamName.Truncate(theCurLen).GetPtr(),
                    theParams,
                    &theErrMsg
                ));
                if (! theX509SslCtxPtr) {
                    if (outErrMsgPtr) {
                        *outErrMsgPtr = theErrMsg;
                    }
                    KFS_LOG_STREAM_ERROR <<
                        theParamName.Truncate(theCurLen) <<
                        "* configuration error: " << theErrMsg <<
                    KFS_LOG_EOM;
                    return -EINVAL;
                }
            }
            theX509ExpectedName = theParams.getValue(
                theParamName.Truncate(theCurLen).Append("name"),
                string()
            );
        }
        theCurLen = theParamName.Truncate(thePrefLen).Append("psk.").GetSize();
        const string thePskKeyId = theParams.getValue(
            theParamName.Truncate(theCurLen).Append("keyId"),
            string()
        );
        const Properties::String* const theKeyStrPtr = theParams.getValue(
            theParamName.Truncate(theCurLen).Append("key"));
        string thePskKey;
        if (theKeyStrPtr) {
            StBufferT<char, 64> theKeyBuf;
            char* const thePtr = theKeyBuf.Resize(
                Base64::GetMaxDecodedLength((int)theKeyStrPtr->GetSize()));
            const int   theLen = Base64::Decode(
                theKeyStrPtr->GetPtr(), theKeyStrPtr->GetSize(), thePtr);
            if (theLen <= 0) {
                const char* const kMsgPtr = "psk: invalid key encoding";
                if (outErrMsgPtr) {
                    *outErrMsgPtr = kMsgPtr;
                }
                KFS_LOG_STREAM_ERROR <<
                    theParamName << ": " << kMsgPtr <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            thePskKey.assign(thePtr, theLen);
        }
        const bool theCreatSslPskFlag =
            theParams.getValue(
                theParamName.Truncate(theCurLen).Append(
                "disable"), 0) == 0 &&
            ((theX509ChangedFlag ? theX509SslCtxPtr : mX509SslCtxPtr) ||
                (theKrbChangedFlag ? theKrbClientPtr : mKrbClientPtr) ||
            ! thePskKey.empty());
        const bool thePskSslChangedFlag =
            (theCreatSslPskFlag != (mSslCtxPtr != 0)) ||
            theParams.getValue(
                theParamName.Truncate(theCurLen).Append(
                    "forceReload"), 0) != 0 ||
            ! theParams.equalsWithPrefix(
                theParamName.Truncate(theCurLen).GetPtr(), theCurLen, mParams);
        SslCtxPtr theSslCtxPtr;
        if (thePskSslChangedFlag && theCreatSslPskFlag) {
            const bool kServerFlag  = false;
            const bool kPskOnlyFlag = true;
            string     theErrMsg;
            theSslCtxPtr = SslFilter::MakeCtxPtr(SslFilter::CreateCtx(
                kServerFlag,
                kPskOnlyFlag,
                theParamName.Truncate(theCurLen).GetPtr(),
                theParams,
                &theErrMsg
            ));
            if (! theSslCtxPtr) {
                if (outErrMsgPtr) {
                    *outErrMsgPtr = theErrMsg;
                }
                KFS_LOG_STREAM_ERROR <<
                    theParamName.Truncate(theCurLen) <<
                    "* configuration error: " << theErrMsg <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
        }
        const bool theEnabledFlag =
            (theKrbChangedFlag ? theKrbClientPtr : mKrbClientPtr) ||
            (thePskSslChangedFlag ?
                (theSslCtxPtr && ! thePskKey.empty()) :
                (mSslCtxPtr   && ! mPskKey.empty())) ||
            (theX509ChangedFlag ? theX509SslCtxPtr : mX509SslCtxPtr);
        const bool theAuthRequiredFlag = theParams.getValue(
            theParamName.Truncate(thePrefLen).Append("required"),
            theEnabledFlag ? 1 : 0) != 0;
        if (theAuthRequiredFlag && ! theEnabledFlag) {
            KFS_LOG_STREAM_ERROR <<
                theParamName <<
                    " is on, but no valid authentication method specified" <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        const Properties::String theReqParamName(theParamName);
        const bool theAuthNoneEnabledFlag = theParams.getValue(
            theParamName.Truncate(thePrefLen).Append("authNone"),
            0) != 0;
        if (theAuthRequiredFlag && theAuthNoneEnabledFlag) {
            KFS_LOG_STREAM_ERROR <<
                theParamName <<
                    " is on, conflicts with " << theReqParamName <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        const bool theAllowCSClearTextFlag = theParams.getValue(
            theParamName.Truncate(thePrefLen).Append(
                "allowChunkServerClearText"), 1) != 0;
        if (! theAllowCSClearTextFlag && (! theEnabledFlag ||
                ! theAuthRequiredFlag || theAuthNoneEnabledFlag)) {
            KFS_LOG_STREAM_ERROR <<
                theParamName <<
                    " is off, conflicts with " << theReqParamName <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        mMaxAuthRetryCount = max(1, theParams.getValue(
            theParamName.Truncate(thePrefLen).Append("maxAuthRetries"),
            mMaxAuthRetryCount));
        mParams.swap(theParams);
        if (theKrbChangedFlag) {
            mKrbClientPtr.swap(theKrbClientPtr);
        }
        if (thePskSslChangedFlag) {
            mSslCtxPtr.swap(theSslCtxPtr);
        }
        if (theX509ChangedFlag) {
            mX509SslCtxPtr.swap(theX509SslCtxPtr);
            mX509ExpectedName = theX509ExpectedName;
        }
        if (theKrbChangedFlag || thePskSslChangedFlag || theX509ChangedFlag ||
                mPskKeyId != thePskKeyId || thePskKey != mPskKey) {
           mCurRequest.mInvalidFlag = true;
        }
        mKrbAuthRequireSslFlag = theKrbRequireSslFlag && mSslCtxPtr;
        mAuthNoneEnabledFlag   = theAuthNoneEnabledFlag;
        mPskKeyId              = thePskKeyId;
        mPskKey                = thePskKey;
        mEnabledFlag           = theEnabledFlag;
        mAuthRequiredFlag      = theAuthRequiredFlag;
        mAllowCSClearTextFlag  = theAllowCSClearTextFlag;
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
        if ((inAuthType & kAuthenticationTypePSK) != 0 && ! mPskKey.empty()) {
            outAuthType = RequestInFlight(
                kAuthenticationTypePSK, inRequestCtx);
            return 0;
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
                const int theErr = mKrbClientPtr->GetErrorCode();
                return (theErr > 0 ? -theErr :
                    (theErr == 0 ? -EINVAL : theErr));
            }
            outAuthType = RequestInFlight(
                kAuthenticationTypeKrb5, inRequestCtx);
            if (outBufPtr && outBufLen > 0) {
                mCurRequest.mKrbClientPtr = mKrbClientPtr;
            }
            return 0;
        }
        if ((inAuthType & kAuthenticationTypeX509) != 0 &&
                mX509SslCtxPtr) {
            outAuthType = RequestInFlight(
                kAuthenticationTypeX509, inRequestCtx);
            return 0;
        }
        if ((inAuthType & kAuthenticationTypeNone) != 0 &&
                mAuthNoneEnabledFlag) {
            outAuthType = RequestInFlight(
                kAuthenticationTypeNone, inRequestCtx);
            return 0;
        }
        if (outErrMsgPtr) {
            *outErrMsgPtr = "no common auth. method";
        }
        return -EPERM;
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
        const int theStatus = ResponseSelf(
            inAuthType,
            inUseSslFlag,
            inBufPtr,
            inBufLen,
            inNetConnection,
            inRequestCtx,
            outErrMsgPtr
        );
        Dispose(inRequestCtx);
        return theStatus;
    }
    int StartSsl(
        NetConnection& inNetConnection,
        const char*    inKeyIdPtr,
        const char*    inKeyDataPtr,
        int            inKeyDataSize,
        string*        outErrMsgPtr)
    {
        if (! mSslCtxPtr) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "no tls psk configured";
            }
            return -EINVAL;
        }
        if (! inKeyDataPtr || inKeyDataSize <= 0) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "empty key specified";
            }
            return -EINVAL;
        }
        if (inNetConnection.GetFilter()) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "connection already has filter";
            }
            return -EINVAL;
        }
        if (inNetConnection.HasPendingRead() ||
                inNetConnection.IsWriteReady()) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "connection has pending read and/or write data";
            }
            return -EINVAL;
        }
        SslFilter::ServerPsk*  kServerPskPtr          = 0;
        SslFilter::VerifyPeer* kVerifyPeerPtr         = 0;
        const char*            kExpectedServerNamePtr = 0;
        const bool             kDeleteOnCloseFlag     = true;
        SslFilter& theFilter = SslFilter::Create(
            *mSslCtxPtr,
            inKeyDataPtr,
            (size_t)inKeyDataSize,
            inKeyIdPtr,
            kServerPskPtr,
            kVerifyPeerPtr,
            kExpectedServerNamePtr,
            kDeleteOnCloseFlag
        );
        const SslFilter::Error theErr = theFilter.GetError();
        if (theErr) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = SslFilter::GetErrorMsg(theErr);
                if (outErrMsgPtr->empty()) {
                    *outErrMsgPtr = "failed to create ssl filter";
                }
            }
            delete &theFilter;
            return -EFAULT;
        }
        return inNetConnection.SetFilter(&theFilter, outErrMsgPtr);
    }
    bool IsChunkServerClearTextAllowed() const
        { return mAllowCSClearTextFlag; }
    bool IsEnabled() const
        { return mEnabledFlag; }
    int GetMaxAuthRetryCount() const
        { return mMaxAuthRetryCount; }
    int CheckAuthType(
        int     inAuthType,
        bool&   outDoAuthFlag,
        string* outErrMsgPtr)
    {
        if (! mAuthRequiredFlag && inAuthType == kAuthenticationTypeUndef) {
            outDoAuthFlag = false;
            return 0;
        }
        outDoAuthFlag = true;
        if (((inAuthType & kAuthenticationTypePSK) != 0 && ! mPskKey.empty()) ||
                ((inAuthType & kAuthenticationTypeNone) != 0 &&
                    mAuthNoneEnabledFlag) ||
                ((inAuthType & kAuthenticationTypeKrb5) != 0 &&
                    mKrbClientPtr) ||
                ((inAuthType & kAuthenticationTypeX509) != 0 &&
                    mX509SslCtxPtr)
                ) {
            return 0;
        }
        if (outErrMsgPtr) {
            *outErrMsgPtr = "no common auth. method found";
        }
        return -EPERM;
    }
    string GetPskId() const
        { return mPskKeyId; }
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
    bool GetX509EndTime(
        int64_t& outEndTime) const
    {
        return (mX509SslCtxPtr &&
            SslFilter::GetCtxX509EndTime(*mX509SslCtxPtr, outEndTime));
    }
    void Clear()
    {
        if (mCurRequest.mOuterPtr) {
            RequestCtxImpl*& theImplPtr = mCurRequest.mOuterPtr->mImplPtr;
            QCASSERT(theImplPtr);
            if (theImplPtr && theImplPtr->mKrbClientPtr) {
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
        mParams.clear();
        mKrbClientPtr.reset();
        mSslCtxPtr.reset();
        mX509SslCtxPtr.reset();
        mPskKeyId.clear();
        mPskKey.clear();
        mX509ExpectedName.clear();
        mEnabledFlag           = false;
        mAuthNoneEnabledFlag   = false;
        mKrbAuthRequireSslFlag = false;
        mAuthRequiredFlag      = false;
        mAllowCSClearTextFlag  = true;
        mMaxAuthRetryCount     = 3;
    }
private:
    typedef RequestCtxImpl::KrbClientPtr KrbClientPtr;
    typedef SslFilter::CtxPtr            SslCtxPtr;

    RequestCtxImpl  mCurRequest;
    bool            mEnabledFlag;
    bool            mAuthNoneEnabledFlag;
    bool            mKrbAuthRequireSslFlag;
    bool            mAuthRequiredFlag;
    bool            mAllowCSClearTextFlag;
    int             mMaxAuthRetryCount;
    Properties      mParams;
    KrbClientPtr    mKrbClientPtr;
    SslCtxPtr       mSslCtxPtr;
    SslCtxPtr       mX509SslCtxPtr;
    string          mPskKeyId;
    string          mPskKey;
    string          mX509ExpectedName;

    int RequestInFlight(
        int         inAuthType,
        RequestCtx& inRequestCtx)
    {
        QCASSERT(! inRequestCtx.mImplPtr && ! mCurRequest.mOuterPtr);
        mCurRequest.mAuthType = inAuthType;
        mCurRequest.mOuterPtr = &inRequestCtx;
        inRequestCtx.mImplPtr = &mCurRequest;
        return inAuthType;
    }
    int ResponseSelf(
        int            inAuthType,
        bool           inUseSslFlag,
        const char*    inBufPtr,
        int            inBufLen,
        NetConnection& inNetConnection,
        RequestCtx&    inRequestCtx,
        string*        outErrMsgPtr)
    {
        if (mCurRequest.mInvalidFlag) {
            if (outErrMsgPtr) {
                *outErrMsgPtr =
                    "client auth. configuration has changed, try again";
            }
            return -EAGAIN;
        }
        if (mCurRequest.mAuthType != inAuthType) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "response authentication type mismatch";
            }
            return -EINVAL;
        }
        if ((! inUseSslFlag &&
                inAuthType != kAuthenticationTypeKrb5 &&
                inAuthType != kAuthenticationTypeNone) ||
                (inAuthType == kAuthenticationTypeNone && inUseSslFlag)) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "response: invalid use ssl flag value";
            }
            return -EINVAL;
        }
        if (0 < inBufLen &&
                (inUseSslFlag || inAuthType != kAuthenticationTypeKrb5)) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "response: invalid non empty content";
            }
            return -EINVAL;
        }
        if (inAuthType == kAuthenticationTypePSK) {
            return StartSsl(
                inNetConnection,
                mPskKeyId.c_str(),
                mPskKey.data(),
                (int)mPskKey.size(),
                outErrMsgPtr
            );
        }
        if (inAuthType == kAuthenticationTypeKrb5) {
            if (inUseSslFlag) {
                return StartSsl(
                    inNetConnection,
                    0,
                    mCurRequest.mSessionKeyPtr,
                    mCurRequest.mSessionKeyLen,
                    outErrMsgPtr
                );
            }
            if (! mKrbClientPtr) {
                if (outErrMsgPtr) {
                    *outErrMsgPtr = "response: internal error no krb5 context";
                }
                return -EFAULT;
            }
            const char* const theErrMsgPtr =
                mKrbClientPtr->Reply(inBufPtr, inBufLen);
            if (theErrMsgPtr) {
                if (outErrMsgPtr) {
                    *outErrMsgPtr = theErrMsgPtr;
                }
                return -EPERM;
            }
            if (mKrbAuthRequireSslFlag) {
                if (outErrMsgPtr) {
                    *outErrMsgPtr =
                        "ssl with kerberos is required by configuration but"
                        " is not enabled on the server";
                }
                return -EPERM;
            }
            return 0;
        }
        if (inAuthType == kAuthenticationTypeX509) {
            if (inNetConnection.GetFilter()) {
                if (outErrMsgPtr) {
                    *outErrMsgPtr = "connection already has filter";
                }
                return -EINVAL;
            }
            if (! mX509SslCtxPtr) {
                if (outErrMsgPtr) {
                    *outErrMsgPtr = "internal error: null x509 ssl context";
                }
                return -EFAULT;
            }
            SslFilter::ServerPsk*  const kServerPskPtr      = 0;
            SslFilter::VerifyPeer* const kVerifyPeerPtr     = 0;
            const char*                  kKeyDataPtr        = 0;
            const int                    kKeyDataSize       = 0;
            const char*                  kKeyIdPtr          = 0;
            const char*                  kNullStr           = 0;
            const bool                   kDeleteOnCloseFlag = true;
            SslFilter& theFilter = SslFilter::Create(
                *mX509SslCtxPtr,
                kKeyDataPtr,
                kKeyDataSize,
                kKeyIdPtr,
                kServerPskPtr,
                kVerifyPeerPtr,
                mX509ExpectedName.empty() ?
                    kNullStr : mX509ExpectedName.c_str(),
                kDeleteOnCloseFlag
            );
            const SslFilter::Error theErr = theFilter.GetError();
            if (theErr) {
                if (outErrMsgPtr) {
                    *outErrMsgPtr = SslFilter::GetErrorMsg(theErr);
                    if (outErrMsgPtr->empty()) {
                        *outErrMsgPtr = "failed to create ssl filter";
                    }
                }
                delete &theFilter;
                return -EFAULT;
            }
            return inNetConnection.SetFilter(&theFilter, outErrMsgPtr);
        }
        if (inAuthType == kAuthenticationTypeNone) {
            return 0;
        }
        if (outErrMsgPtr) {
            *outErrMsgPtr = "internal error: invalid auth. type";
        }
        return -EFAULT;
    }
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
    bool&   outDoAuthFlag,
    string* outErrMsgPtr)
{
    return mImpl.CheckAuthType(inAuthType, outDoAuthFlag, outErrMsgPtr);
}

    int
ClientAuthContext::SetParameters(
    const char*        inParamsPrefixPtr,
    const Properties&  inParameters,
    ClientAuthContext* inOtherCtxPtr,
    string*            outErrMsgPtr,
    bool               inVerifyFlag)
{
    return mImpl.SetParameters(inParamsPrefixPtr, inParameters,
        inOtherCtxPtr ? &inOtherCtxPtr->mImpl : 0, outErrMsgPtr, inVerifyFlag);
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

    bool
ClientAuthContext::IsChunkServerClearTextAllowed() const
{
    return mImpl.IsChunkServerClearTextAllowed();
}

    int
ClientAuthContext::GetMaxAuthRetryCount() const
{
    return mImpl.GetMaxAuthRetryCount();
}

    string
ClientAuthContext::GetPskId() const
{
    return mImpl.GetPskId();
}

    /* static */ void
ClientAuthContext::Dispose(
    ClientAuthContext::RequestCtxImpl& inRequestCtxImpl)
{
    Impl::Dispose(inRequestCtxImpl);
}

    bool
ClientAuthContext::GetX509EndTime(
    int64_t& outEndTime) const
{
    return mImpl.GetX509EndTime(outEndTime);
}

    void
ClientAuthContext::Clear()
{
    mImpl.Clear();
}

} // namespace KFS
