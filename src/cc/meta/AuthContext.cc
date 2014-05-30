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
#include "UserAndGroup.h"

#include "common/Properties.h"
#include "common/MsgLogger.h"
#include "common/StdAllocator.h"
#include "kfsio/SslFilter.h"
#include "kfsio/Base64.h"
#include "krb/KrbService.h"
#include "qcdio/qcdebug.h"
#include "qcdio/QCUtils.h"

#include <algorithm>
#include <string>
#include <sstream>
#include <iomanip>
#include <set>
#include <map>

#include <ctype.h>

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
    typedef UserAndGroup::NameUidPtr   NameUidPtr;
    typedef UserAndGroup::UidNamePtr   UidNamePtr;
    typedef UserAndGroup::GidNamePtr   GidNamePtr;
    typedef UserAndGroup::UidAndGid    UidAndGid;
    typedef UserAndGroup::NameAndGid   NameAndGid;
    typedef UserAndGroup::RootUsersPtr RootUsersPtr;
    typedef UserAndGroup::DelegationRenewAndCancelUsersPtr
        DelegationRenewAndCancelUsersPtr;

    Impl(
        bool               inAllowPskFlag,
        const int*         inDefaultNoAuthMetaOpsPtr,
        const char* const* inDefaultNoAuthMetaOpsHostsPtr,
        bool&              inAuthRequiredFlag)
        : mKrbProps(),
          mPskSslProps(),
          mX509SslProps(),
          mKrbServicePtr(),
          mSslCtxPtr(),
          mX509SslCtxPtr(),
          mNameRemap(),
          mBlackList(),
          mWhiteList(),
          mNameUidPtr(new UserAndGroup::NameUidMap()),
          mUidNamePtr(new UserAndGroup::UidNameMap()),
          mGidNamePtr(new UserAndGroup::GidNameMap()),
          mRootUsersPtr(new UserAndGroup::RootUsers()),
          mDelegationRenewAndCancelUsersPtr(new UserAndGroup::UserIdsSet()),
          mNameRemapParam(),
          mBlackListParam(),
          mWhiteListParam(),
          mPrincipalUnparseFlags(0),
          mAuthNoneFlag(false),
          mKrbUseSslFlag(true),
          mAllowPskFlag(inAllowPskFlag),
          mHasUserAndGroupFlag(false),
          mMemKeytabGen(0),
          mMaxDelegationValidForTime(60 * 60 * 24),
          mDelegationIgnoreCredEndTimeFlag(false),
          mMaxAuthenticationValidTime(60 * 60 * 24),
          mReDelegationAllowedFlag(false),
          mAuthTypes(kAuthenticationTypeUndef),
          mAuthRequiredFlag(inAuthRequiredFlag),
          mUserAndGroupNames(*mUidNamePtr, *mGidNamePtr),
          mNoAuthMetaOpHosts(),
          mNoAuthMetaOps(),
          mPskKey(),
          mPskId()
    {
        if (inDefaultNoAuthMetaOpsPtr) {
            for (const int* thePtr = inDefaultNoAuthMetaOpsPtr;
                    0 <= *thePtr && *thePtr < META_NUM_OPS_COUNT; ++thePtr) {
                mNoAuthMetaOps.insert(MetaOp(*thePtr));
            }
        }
        if (inDefaultNoAuthMetaOpsHostsPtr) {
            for (const char* const* thePtr = inDefaultNoAuthMetaOpsHostsPtr;
                    *thePtr; ++thePtr) {
                mNoAuthMetaOpHosts.insert(string(*thePtr));
            }
        }
    }
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
            (mX509SslCtxPtr &&
                (inOp.authType & kAuthenticationTypeX509) != 0) ||
            (IsPskAllowed() && mSslCtxPtr &&
                (inOp.authType & kAuthenticationTypePSK) != 0) ||
            (mAuthNoneFlag &&
                    (inOp.authType & kAuthenticationTypeNone) != 0);
        if (! theRetFlag) {
            inOp.status    = -EPERM;
            inOp.statusMsg = "requested authentication type is not enabled";
        }
        return theRetFlag;
    }
    bool Authenticate(
        MetaAuthenticate&    inOp,
        SslFilterVerifyPeer* inVerifyPeerPtr,
        SslFilterServerPsk*  inServerPskPtr)
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
        if (IsPskAllowed() && mSslCtxPtr &&
                (inOp.authType & kAuthenticationTypePSK) != 0) {
            if (! inServerPskPtr && mPskKey.empty()) {
                inOp.status    = -EINVAL;
                inOp.statusMsg = "no PSK key configured";
                return false;
            }
            inOp.responseContentPtr = 0;
            inOp.responseContentLen = 0;
            SslFilter::VerifyPeer* const kVerifyPeerPtr        = 0;
            const char*                  kPskClientIdentityPtr = "";
            const bool                   kDeleteOnCloseFlag    = true;
            const char*                  kSessionKeyPtr        = 0;
            int                          kSessionKeyLen        = 0;
            SslFilter* const theFilterPtr = new SslFilter(
                *mSslCtxPtr,
                inServerPskPtr ? kSessionKeyPtr        : mPskKey.data(),
                inServerPskPtr ? kSessionKeyLen        : mPskKey.size(),
                inServerPskPtr ? kPskClientIdentityPtr : mPskId.c_str(),
                inServerPskPtr,
                kVerifyPeerPtr,
                kDeleteOnCloseFlag
            );
            const SslFilter::Error theErr = theFilterPtr->GetError();
            if (theErr) {
                inOp.statusMsg = SslFilter::GetErrorMsg(theErr);
                inOp.status    = -EFAULT;
                if (inOp.statusMsg.empty()) {
                    inOp.statusMsg = "failed to create ssl filter";
                }
                delete theFilterPtr;
            } else {
                inOp.authName.clear();
                inOp.filter                = theFilterPtr;
                inOp.responseAuthType      = kAuthenticationTypePSK;
                inOp.sessionExpirationTime = int64_t(time(0)) +
                    mMaxAuthenticationValidTime;
                if (! inServerPskPtr) {
                    inOp.authName = mPskId;
                }
            }
            return (inOp.status == 0);
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
            if (! RemapAndValidate(theAuthName) || (mHasUserAndGroupFlag &&
                    (inOp.authUid = GetUidSelf(
                        theAuthName, inOp.authGid, inOp.euser, inOp.egroup)) ==
                    kKfsUserNone)) {
                inOp.status    = -EACCES;
                inOp.statusMsg = "access denied for '" + theAuthName + "'";
                inOp.responseContentPtr = 0;
                inOp.responseContentLen = 0;
                return false;
            }
            inOp.credExpirationTime    = mKrbServicePtr->GetTicketEndTime();
            inOp.sessionExpirationTime = min(
                int64_t(time(0)) + mMaxAuthenticationValidTime,
                inOp.credExpirationTime
            );
            if (! mSslCtxPtr || ! mKrbUseSslFlag) {
                inOp.authName         = theAuthName;
                inOp.responseAuthType = inOp.authType;
                return true;
            }
            // Do not send kerberos AP_REP, as TLS-PSK handshake is sufficient
            // for mutual authentication / replay attack protection.
            inOp.responseContentPtr = 0;
            inOp.responseContentLen = 0;
            const char*                  kPskClientIdentityPtr = "";
            SslFilter::ServerPsk* const  kServerPskPtr         = 0;
            SslFilter::VerifyPeer* const kVerifyPeerPtr        = 0;
            const bool                   kDeleteOnCloseFlag    = true;
            SslFilter* const theFilterPtr = new SslFilter(
                *mSslCtxPtr,
                theSessionKeyPtr,
                (size_t)max(0, theSessionKeyLen),
                kPskClientIdentityPtr,
                kServerPskPtr,
                kVerifyPeerPtr,
                kDeleteOnCloseFlag
            );
            const SslFilter::Error theErr = theFilterPtr->GetError();
            if (theErr) {
                inOp.statusMsg = SslFilter::GetErrorMsg(theErr);
                inOp.status    = -EFAULT;
                if (inOp.statusMsg.empty()) {
                    inOp.statusMsg = "failed to create ssl filter";
                }
                delete theFilterPtr;
            } else {
                inOp.filter           = theFilterPtr;
                inOp.responseAuthType = kAuthenticationTypeKrb5;
                inOp.authName         = theAuthName;
            }
            return (inOp.status == 0);
        }
        if (mX509SslCtxPtr &&
                (inOp.authType & kAuthenticationTypeX509) != 0) {
            const char*                 kSessionKeyPtr        = 0;
            size_t                      kSessionKeyLen        = 0;
            const char*                 kPskClientIdentityPtr = 0;
            SslFilter::ServerPsk* const kServerPskPtr         = 0;
            const bool                  kDeleteOnCloseFlag    = true;
            SslFilter* const theFilterPtr = new SslFilter(
                *mX509SslCtxPtr,
                kSessionKeyPtr,
                kSessionKeyLen,
                kPskClientIdentityPtr,
                kServerPskPtr,
                inVerifyPeerPtr,
                kDeleteOnCloseFlag
            );
            const SslFilter::Error theErr = theFilterPtr->GetError();
            if (theErr) {
                inOp.statusMsg = SslFilter::GetErrorMsg(theErr);
                inOp.status    = -EFAULT;
                if (inOp.statusMsg.empty()) {
                    inOp.statusMsg = "failed to create ssl filter";
                }
                delete theFilterPtr;
            } else {
                inOp.filter                = theFilterPtr;
                inOp.responseAuthType      = inOp.authType;
                inOp.sessionExpirationTime = int64_t(time(0)) +
                    mMaxAuthenticationValidTime;
                int64_t theEndTime = 0;
                if (SslFilter::GetCtxX509EndTime(*mX509SslCtxPtr, theEndTime)) {
                    inOp.sessionExpirationTime =
                        min(theEndTime, inOp.sessionExpirationTime);
                }
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
        return Validate(ioAuthName);
    }
    bool Validate(
        const string& inAuthName) const
    {
        return (
            mBlackList.find(inAuthName) == mBlackList.end() &&
            (mWhiteList.empty() ||
                mWhiteList.find(inAuthName) != mWhiteList.end()) &&
            ((! mKrbServicePtr && ! mX509SslCtxPtr) ||
                ! inAuthName.empty())
        );
    }
    kfsUid_t GetUid(
        const string& inAuthName) const
    {
        string theAuthName = inAuthName;
        if (! RemapAndValidate(theAuthName)) {
            return kKfsUserNone;
        }
        kfsGid_t theGid = kKfsGroupNone;
        return GetUidSelf(theAuthName, theGid);
    }
    kfsUid_t GetUid(
        const string& inAuthName,
        kfsGid_t&     outGid,
        kfsUid_t&     outEUid,
        kfsGid_t&     outEGid) const
    {
        string theAuthName = inAuthName;
        if (! RemapAndValidate(theAuthName)) {
            return kKfsUserNone;
        }
        return GetUidSelf(theAuthName, outGid, outEUid, outEGid);
    }
    void SetUserAndGroup(
        const UserAndGroup& inUserAndGroup)
    {
        mHasUserAndGroupFlag = true;
        mNameUidPtr = inUserAndGroup.GetNameUidPtr();
        QCRTASSERT(mNameUidPtr);
        mUidNamePtr = inUserAndGroup.GetUidNamePtr();
        QCRTASSERT(mUidNamePtr);
        mRootUsersPtr = inUserAndGroup.GetRootUsersPtr();
        QCRTASSERT(mRootUsersPtr);
        mGidNamePtr = inUserAndGroup.GetGidNamePtr();
        QCRTASSERT(mGidNamePtr);
        mDelegationRenewAndCancelUsersPtr =
            inUserAndGroup.GetDelegationRenewAndCancelUsersPtr();
        QCRTASSERT(mDelegationRenewAndCancelUsersPtr);
        mUserAndGroupNames.Set(*mUidNamePtr, *mGidNamePtr);
    }
    void DontUseUserAndGroup()
    {
        mNameUidPtr.reset(new UserAndGroup::NameUidMap());
        mUidNamePtr.reset((new UserAndGroup::UidNameMap()));
        mGidNamePtr.reset((new UserAndGroup::GidNameMap()));
        mRootUsersPtr.reset((new UserAndGroup::RootUsers()));
        mDelegationRenewAndCancelUsersPtr.reset((new UserAndGroup::UserIdsSet()));
        mUserAndGroupNames.Set(*mUidNamePtr, *mGidNamePtr);
        mHasUserAndGroupFlag = false;
    }
    bool HasUserAndGroup() const
        { return mHasUserAndGroupFlag; }
    bool SetParameters(
        const char*       inParamNamePrefixPtr,
        const Properties& inParameters,
        Impl*             inOtherCtxPtr)
    {
        Properties::String theParamName;
        if (inParamNamePrefixPtr) {
            theParamName.Append(inParamNamePrefixPtr);
        }
        const size_t thePrefLen = theParamName.GetSize();
        bool              theNoAuthMetaOpHostsChangedFlag = true;
        NoAuthMetaOpHosts theNoAuthMetaOpHosts;
        const Properties::String* const theNoAuthHostIpsPtr =
            inParameters.getValue(
                theParamName.Truncate(thePrefLen).Append(
                "noAuthOpsHostIps"));
        if (theNoAuthHostIpsPtr) {
            HostIpsInserter theInserter(theNoAuthMetaOpHosts);
            if (! Tokenize(
                    theNoAuthHostIpsPtr->GetPtr(),
                    theNoAuthHostIpsPtr->GetSize(),
                    theInserter)) {
                KFS_LOG_STREAM_ERROR <<
                    theParamName << ": invalid confiruation: " <<
                    theNoAuthHostIpsPtr->GetPtr() <<
                KFS_LOG_EOM;
                return false;
            }
        } else if (inOtherCtxPtr) {
            theNoAuthMetaOpHosts = inOtherCtxPtr->mNoAuthMetaOpHosts;
        } else {
            theNoAuthMetaOpHostsChangedFlag = false;
        }
        bool          theNoAuthMetaOpsChangedFlag = true;
        NoAuthMetaOps theNoAuthMetaOps;
        const Properties::String* const theNoAuthOpsPtr =
            inParameters.getValue(
                theParamName.Truncate(thePrefLen).Append(
                "noAuthOps"));
        if (theNoAuthOpsPtr) {
            OpNamesInserter theInserter(sOpNamesMap, theNoAuthMetaOps);
            if (! Tokenize(
                    theNoAuthOpsPtr->GetPtr(),
                    theNoAuthOpsPtr->GetSize(),
                    theInserter)) {
                KFS_LOG_STREAM_ERROR <<
                    theParamName << ": invalid confiruation: " <<
                    theNoAuthOpsPtr->GetPtr() <<
                KFS_LOG_EOM;
                return false;
            }
        } else if (inOtherCtxPtr) {
            theNoAuthMetaOps = inOtherCtxPtr->mNoAuthMetaOps;
        } else {
            theNoAuthMetaOpsChangedFlag = false;
        }
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
        size_t theCurLen = theParamName.GetSize();
        inParameters.copyWithPrefix(
            theParamName.GetPtr(), theCurLen, theKrbProps);
        const bool    theKrbChangedFlag        = theKrbProps != mKrbProps ||
            inParameters.getValue(
                theParamName.Truncate(theCurLen).Append(
                "forceReload"), 0) != 0;
        int           thePrincipalUnparseFlags = 0;
        KrbServicePtr theKrbServicePtr;
        if (theKrbChangedFlag) {
            const char* const theNullStrPtr     = 0;
            const char* const theServiceNamePtr = inParameters.getValue(
                theParamName.Truncate(theCurLen).Append("service"),
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
                        theParamName.Truncate(theCurLen).Append(
                            "host"), theNullStrPtr),
                    theServiceNamePtr,
                    inParameters.getValue(
                        theParamName.Truncate(theCurLen).Append(
                            "keytab"), theNullStrPtr),
                    inParameters.getValue(
                        theParamName.Truncate(theCurLen).Append(
                            "copyToMemKeytab"), 1) != 0 ?
                        theMemTabName.c_str() : theNullStrPtr,
                    kDetectReplayFlag
                );
                if (theErrMsgPtr) {
                    KFS_LOG_STREAM_ERROR <<
                        theParamName.Truncate(theCurLen) <<
                            "* configuration error: " <<
                        theErrMsgPtr <<
                    KFS_LOG_EOM;
                    return false;
                } else {
                    const char* thePrincUnparseModePtr = inParameters.getValue(
                        theParamName.Truncate(theCurLen).Append(
                            "princUnparseMode"), theNullStrPtr
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
                                theParamName.Truncate(theCurLen) <<
                                "* configuration error: "
                                "invalid principal unparse mode: " <<
                                thePrincUnparseModePtr <<
                            KFS_LOG_EOM;
                            return false;
                        }
                    }
                }
            }
        }
        const bool theKrbUseSslFlag = inParameters.getValue(
            theParamName.Truncate(theCurLen).Append("useSsl"), 1) != 0;
        Properties theX509SslProps(mX509SslProps);
        theParamName.Truncate(thePrefLen).Append("X509.");
        theCurLen = theParamName.GetSize();
        inParameters.copyWithPrefix(
            theParamName.GetPtr(), theCurLen, theX509SslProps);
        const bool theCreateX509CtxFlag = theX509SslProps.getValue(
                theParamName.Append("PKeyPemFile")) != 0;
        const bool theX509ChangedFlag =
            theCreateX509CtxFlag != (mX509SslCtxPtr != 0) ||
            theX509SslProps != mX509SslProps ||
            theX509SslProps.getValue(
                theParamName.Truncate(theCurLen).Append(
                "forceReload"), 0) != 0;
        SslCtxPtr theX509SslCtxPtr;
        if (theCreateX509CtxFlag && theX509ChangedFlag) {
            // Share x509 context in order to use the same session cache
            // and tls session ticket keys with all threads.
            if (inOtherCtxPtr && inOtherCtxPtr->mX509SslCtxPtr) {
                theX509SslCtxPtr = inOtherCtxPtr->mX509SslCtxPtr;
            } else {
                const bool kServerFlag  = true;
                const bool kPskOnlyFlag = false;
                string     theErrMsg;
                theX509SslCtxPtr = SslFilter::MakeCtxPtr(SslFilter::CreateCtx(
                    kServerFlag,
                    kPskOnlyFlag,
                    theParamName.Truncate(theCurLen).GetPtr(),
                    theX509SslProps,
                    &theErrMsg
                ));
                if (! theX509SslCtxPtr) {
                    KFS_LOG_STREAM_ERROR <<
                        theParamName.Truncate(theCurLen) <<
                        "* configuration error: " << theErrMsg <<
                    KFS_LOG_EOM;
                    return false;
                }
            }
        }
        Properties thePskSslProps(mPskSslProps);
        theParamName.Truncate(thePrefLen).Append("psk.");
        theCurLen = theParamName.GetSize();
        inParameters.copyWithPrefix(
            theParamName.GetPtr(), theCurLen, thePskSslProps);
        const Properties::String* const theKeyStrPtr = thePskSslProps.getValue(
            theParamName.Truncate(theCurLen).Append("key"));
        string thePskKey;
        if (theKeyStrPtr) {
            StBufferT<char, 64> theKeyBuf;
            char* const thePtr = theKeyBuf.Resize(
                Base64::GetMaxDecodedLength((int)theKeyStrPtr->GetSize()));
            const int   theLen = Base64::Decode(
                theKeyStrPtr->GetPtr(), theKeyStrPtr->GetSize(), thePtr);
            if (theLen <= 0) {
                KFS_LOG_STREAM_ERROR <<
                    theParamName << ": " << "invalid key encoding" <<
                KFS_LOG_EOM;
                return false;
            }
            thePskKey.assign(thePtr, theLen);
        }
        const bool theCreateSslPskFlag  =
            (((theKrbChangedFlag ? theKrbServicePtr : mKrbServicePtr) &&
                    theKrbUseSslFlag) ||
                (mAllowPskFlag && // delegation uses psk
                    ((theKrbChangedFlag ? theKrbServicePtr : mKrbServicePtr) ||
                    (theX509ChangedFlag ? theX509SslCtxPtr : mX509SslCtxPtr))) ||
                ! thePskKey.empty()) &&
            thePskSslProps.getValue(
                theParamName.Truncate(theCurLen).Append(
                "disable"), 0) == 0;
        const bool thePskSslChangedFlag =
            theCreateSslPskFlag != (mSslCtxPtr != 0) ||
            mPskSslProps != thePskSslProps ||
            thePskSslProps.getValue(
                theParamName.Truncate(theCurLen).Append(
                "forceReload"), 0) != 0;
        SslCtxPtr theSslCtxPtr;
        if (theCreateSslPskFlag && thePskSslChangedFlag) {
            const bool kServerFlag  = true;
            const bool kPskOnlyFlag = true;
            string     theErrMsg;
            theSslCtxPtr = SslFilter::MakeCtxPtr(SslFilter::CreateCtx(
                kServerFlag,
                kPskOnlyFlag,
                theParamName.Truncate(theCurLen).GetPtr(),
                thePskSslProps,
                &theErrMsg
            ));
            if (! theSslCtxPtr) {
                KFS_LOG_STREAM_ERROR <<
                    theParamName.Truncate(theCurLen) <<
                    "* configuration error: " << theErrMsg <<
                KFS_LOG_EOM;
                return false;
            }
        }
        if (theKrbChangedFlag) {
            mPrincipalUnparseFlags = thePrincipalUnparseFlags;
            mKrbUseSslFlag         = theKrbUseSslFlag;
            mKrbServicePtr.swap(theKrbServicePtr);
            mKrbProps.swap(theKrbProps);
        }
        if (thePskSslChangedFlag) {
            mPskKey = thePskKey;
            mPskId  = thePskSslProps.getValue(
                theParamName.Truncate(theCurLen).Append(
                "keyId"), string());
            mSslCtxPtr.swap(theSslCtxPtr);
            mPskSslProps.swap(thePskSslProps);
        }
        if (theX509ChangedFlag) {
            mX509SslCtxPtr.swap(theX509SslCtxPtr);
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
        if (theNoAuthMetaOpHostsChangedFlag) {
            mNoAuthMetaOpHosts.swap(theNoAuthMetaOpHosts);
        }
        if (theNoAuthMetaOpsChangedFlag) {
            mNoAuthMetaOps.swap(theNoAuthMetaOps);
        }
        mMaxDelegationValidForTime = inParameters.getValue(
            theParamName.Truncate(thePrefLen).Append(
            "maxDelegationValidForTimeSec"), mMaxDelegationValidForTime);
        mDelegationIgnoreCredEndTimeFlag = inParameters.getValue(
            theParamName.Truncate(thePrefLen).Append(
            "delegationIgnoreCredEndTime"),
            mDelegationIgnoreCredEndTimeFlag ? 1 : 0) != 0;
        mReDelegationAllowedFlag   = inParameters.getValue(
            theParamName.Truncate(thePrefLen).Append(
            "reDelegationAllowedFlag"), mReDelegationAllowedFlag ? 1 : 0) != 0;
        mAuthNoneFlag = inParameters.getValue(
            theParamName.Truncate(thePrefLen).Append(
            "authNone"), (mX509SslCtxPtr || mKrbServicePtr ||
                (! mAuthNoneFlag && ! mPskKey.empty())) ? 0 : 1) != 0;
        mAuthTypes =
            (mAuthNoneFlag ? int(kAuthenticationTypeNone) : 0) |
            (mSslCtxPtr ? int(kAuthenticationTypePSK) : 0) |
            (mX509SslCtxPtr ? int(kAuthenticationTypeX509) : 0) |
            (mKrbServicePtr ? int(kAuthenticationTypeKrb5) : 0)
        ;
        mMaxAuthenticationValidTime = max(int64_t(5), inParameters.getValue(
            theParamName.Truncate(thePrefLen).Append(
            "maxAuthenticationValidTimeSec"), mMaxAuthenticationValidTime));
        mAuthRequiredFlag = ! mAuthNoneFlag &&
            (mAuthTypes & ~(mAllowPskFlag ?
                int(kAuthenticationTypePSK) : 0)) != 0;
        return true;
    }
    uint32_t GetMaxDelegationValidForTime(
        int64_t inCredValidForTime) const
    {
        if (mDelegationIgnoreCredEndTimeFlag) {
            return mMaxDelegationValidForTime;
        }
        if (inCredValidForTime <= 0) {
            return 0;
        }
        return (uint32_t)min(
            (int64_t)mMaxDelegationValidForTime, inCredValidForTime);
    }
    bool IsReDelegationAllowed() const
        { return mReDelegationAllowedFlag; }
    const char* GetUserNameAndGroup(
        kfsUid_t  inUid,
        kfsGid_t& outGid,
        kfsUid_t& outEUid,
        kfsGid_t& outEGid) const
    {
        const NameAndGid* const thePtr = mUidNamePtr->Find(inUid);
        if (! thePtr || ! Validate(thePtr->mName)) {
            return 0;
        }
        outGid = thePtr->mGid;
        GetEUGid(inUid, outGid, outEUid, outEGid);
        return thePtr->mName.c_str();
    }
    const UserAndGroupNames& GetUserAndGroupNames() const
        { return mUserAndGroupNames; }
    int GetAuthTypes() const
        { return mAuthTypes; }
    bool IsAuthRequired(
        const MetaRequest& inOp) const
    {
        return (mAuthRequiredFlag && (
            mNoAuthMetaOps.find(inOp.op) == mNoAuthMetaOps.end() ||
            mNoAuthMetaOpHosts.find(inOp.clientIp) == mNoAuthMetaOpHosts.end()
        ));
    }
    bool CanRenewAndCancelDelegation(
        kfsUid_t inUid) const
        { return (mDelegationRenewAndCancelUsersPtr->Find(inUid) != 0); }
    void Clear()
    {
        DontUseUserAndGroup();
        mKrbProps.clear();
        mPskSslProps.clear();
        mX509SslProps.clear();
        mKrbServicePtr.reset();
        mSslCtxPtr.reset();
        mX509SslCtxPtr.reset();
        mNameRemap.clear();
        mBlackList.clear();
        mWhiteList.clear();
        mNameRemapParam.clear();
        mBlackListParam.clear();
        mWhiteListParam.clear();
        mPrincipalUnparseFlags           = 0;
        mAuthNoneFlag                    = false;
        mKrbUseSslFlag                   = true;
        mMemKeytabGen                    = 0;
        mMaxDelegationValidForTime       = 60 * 60 * 24;
        mDelegationIgnoreCredEndTimeFlag = false;
        mMaxAuthenticationValidTime      = 60 * 60 * 24;
        mReDelegationAllowedFlag         = false;
        mAuthTypes                       = kAuthenticationTypeUndef;
        mNoAuthMetaOpHosts.clear();
        mNoAuthMetaOps.clear();
        mPskKey.clear();
        mPskId.clear();
    }
private:
    typedef scoped_ptr<KrbService> KrbServicePtr;
    typedef map<
        string,
        string,
        less<string>,
        StdFastAllocator<pair<const string, string> >
    > NameRemap;
    typedef map<
        string,
        kfsUid_t,
        less<string>,
        StdFastAllocator<pair<const string, kfsUid_t> >
    > UidMap;
    typedef set<
        string,
        less<string>,
        StdFastAllocator<string>
    > NameList;
    typedef SslFilter::CtxPtr  SslCtxPtr;
    typedef SslFilterServerPsk ServerPsk;
    typedef map<
        string,
        MetaOp
    > OpNamesMap;
    typedef set<
        MetaOp,
        less<MetaOp>,
        StdFastAllocator<MetaOp>
    > NoAuthMetaOps;
    typedef set<
        string,
        less<string>,
        StdFastAllocator<string>
    > NoAuthMetaOpHosts;
    class OpNamesInserter
    {
    public:
        OpNamesInserter(
            const OpNamesMap& inOpNamesMap,
            NoAuthMetaOps&    inNoAuthMetaOps)
            : mOpNamesMap(inOpNamesMap),
              mNoAuthMetaOps(inNoAuthMetaOps),
              mCurName()
            {}
        bool operator()(
            const char* inPtr,
            const char* inEndPtr)
        {
            mCurName.clear();
            mCurName.reserve(inEndPtr - inPtr);
            for (const char* thePtr = inPtr; thePtr < inEndPtr; ++thePtr) {
                mCurName.push_back((char)toupper(*thePtr & 0xFF));
            }
            OpNamesMap::const_iterator theIt = mOpNamesMap.find(mCurName);
            if (theIt == mOpNamesMap.end()) {
                KFS_LOG_STREAM_ERROR <<
                    mCurName << ": no such meta op" <<
                KFS_LOG_EOM;
                return false;
            }
            mNoAuthMetaOps.insert(theIt->second);
            return true;
        }
    private:
        const OpNamesMap& mOpNamesMap;
        NoAuthMetaOps&    mNoAuthMetaOps;
        string            mCurName;
    };
    class HostIpsInserter
    {
    public:
        HostIpsInserter(
            NoAuthMetaOpHosts& inNoAuthMetaOpHosts)
            : mNoAuthMetaOpHosts(inNoAuthMetaOpHosts),
              mCurName()
            {}
        bool operator()(
            const char* inPtr,
            const char* inEndPtr)
        {
            mCurName.assign(inPtr, inEndPtr - inPtr);
            mNoAuthMetaOpHosts.insert(mCurName);
            return true;
        }
    private:
        NoAuthMetaOpHosts& mNoAuthMetaOpHosts;
        string             mCurName;
    };

    Properties                       mKrbProps;
    Properties                       mPskSslProps;
    Properties                       mX509SslProps;
    KrbServicePtr                    mKrbServicePtr;
    SslCtxPtr                        mSslCtxPtr;
    SslCtxPtr                        mX509SslCtxPtr;
    NameRemap                        mNameRemap;
    NameList                         mBlackList;
    NameList                         mWhiteList;
    NameUidPtr                       mNameUidPtr;
    UidNamePtr                       mUidNamePtr;
    GidNamePtr                       mGidNamePtr;
    RootUsersPtr                     mRootUsersPtr;
    DelegationRenewAndCancelUsersPtr mDelegationRenewAndCancelUsersPtr;
    string                           mNameRemapParam;
    string                           mBlackListParam;
    string                           mWhiteListParam;
    int                              mPrincipalUnparseFlags;
    bool                             mAuthNoneFlag;
    bool                             mKrbUseSslFlag;
    const bool                       mAllowPskFlag;
    bool                             mHasUserAndGroupFlag;
    unsigned int                     mMemKeytabGen;
    uint32_t                         mMaxDelegationValidForTime;
    bool                             mDelegationIgnoreCredEndTimeFlag;
    int64_t                          mMaxAuthenticationValidTime;
    bool                             mReDelegationAllowedFlag;
    int                              mAuthTypes;
    bool&                            mAuthRequiredFlag;
    UserAndGroupNames                mUserAndGroupNames;
    NoAuthMetaOpHosts                mNoAuthMetaOpHosts;
    NoAuthMetaOps                    mNoAuthMetaOps;
    string                           mPskKey;
    string                           mPskId;

    static const OpNamesMap& sOpNamesMap;

    kfsUid_t GetUidSelf(
        const string& inAuthName,
        kfsGid_t&     outGid) const
    {
        const UidAndGid* const thePtr = mNameUidPtr->Find(inAuthName);
        if (! thePtr) {
            outGid = kKfsGroupNone;
            return kKfsUserNone;
        }
        outGid = thePtr->mGid;
        return thePtr->mUid;
    }
    kfsUid_t GetUidSelf(
        const string& inAuthName,
        kfsGid_t&     outGid,
        kfsUid_t&     outEUid,
        kfsGid_t&     outEGid) const
    {
        const kfsUid_t theUid = GetUidSelf(inAuthName, outGid);
        GetEUGid(theUid, outGid, outEUid, outEGid);
        return theUid;
    }
    void GetEUGid(
        kfsUid_t  inUid,
        kfsGid_t  inGid,
        kfsUid_t& outEUid,
        kfsGid_t& outEGid) const
    {
        outEUid = inUid;
        outEGid = inGid;
        if (inUid != kKfsUserRoot && mRootUsersPtr->Find(inUid)) {
            outEUid = kKfsUserRoot;
        }
    }
    static const OpNamesMap& CreateOpNamesMap()
    {
        static OpNamesMap sMap;
        if (sMap.empty()) {
#           define KfsCreateOpNamesMapEntry(name) \
                sMap[string(#name)] = META_##name;
            KfsForEachMetaOpId(KfsCreateOpNamesMapEntry)
#           undef KfsCreateOpNamesMapEntry
        }
        return sMap;
    }
    template<typename T>
    static bool Tokenize(
        const char* inPtr,
        size_t      inSize,
        T&          inFunctor)
    {
        const char*       thePtr    = inPtr;
        const char* const theEndPtr = inPtr + inSize;
        while (thePtr < theEndPtr) {
            while (thePtr < theEndPtr && (*thePtr & 0xFF) <= ' ') {
                ++thePtr;
            }
            const char* const theStartPtr = thePtr;
            while (thePtr < theEndPtr && ' ' < (*thePtr & 0xFF)) {
                ++thePtr;
            }
            if (theStartPtr < thePtr) {
                if (! inFunctor(theStartPtr, thePtr)) {
                    return false;
                }
            }
        }
        return true;
    }
    bool IsPskAllowed() const
        { return (mAllowPskFlag || ! mPskKey.empty()); }

private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

AuthContext::Impl::OpNamesMap const& AuthContext::Impl::sOpNamesMap =
    AuthContext::Impl::CreateOpNamesMap();

AuthContext::AuthContext(
    bool               inAllowPskFlag                 /* = true */,
    const int*         inDefaultNoAuthMetaOpsPtr      /* = 0 */,
    const char* const* inDefaultNoAuthMetaOpsHostsPtr /* = 0 */)
    : mAuthRequiredFlag(false),
      mImpl(*(new Impl(
        inAllowPskFlag,
        inDefaultNoAuthMetaOpsPtr,
        inDefaultNoAuthMetaOpsHostsPtr,
        mAuthRequiredFlag
      ))),
      mUpdateCount(0),
      mUserAndGroupUpdateCount(0),
      mUserAndGroupNames(mImpl.GetUserAndGroupNames())
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
    if ((inOp.authType &
            (kAuthenticationTypeKrb5 |
            kAuthenticationTypeX509 |
            kAuthenticationTypeNone |
            kAuthenticationTypePSK))== 0) {
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
            }
        } else if ((inOp.authType & kAuthenticationTypeKrb5) != 0) {
            inOp.status    = -EINVAL;
            inOp.statusMsg = "invalid zero content"
                " length with kerberos authentication";
        }
    }
    const int theRetFlag = mImpl.Validate(inOp);
    if (theRetFlag && 0 < inOp.contentLength) {
        inOp.contentBuf = new char [inOp.contentLength];
    }
    return theRetFlag;
}

    bool
AuthContext::Authenticate(
    MetaAuthenticate&    inOp,
    SslFilterVerifyPeer* inVerifyPeerPtr,
    SslFilterServerPsk*  inServerPskPtr)
{
    return mImpl.Authenticate(inOp, inVerifyPeerPtr, inServerPskPtr);
}

    bool
AuthContext::RemapAndValidate(
    string& ioAuthName) const
{
    return mImpl.RemapAndValidate(ioAuthName);
}

    bool
AuthContext::Validate(
    const string& inAuthName) const
{
    return mImpl.Validate(inAuthName);
}

    kfsUid_t
AuthContext::GetUid(
    const string& inAuthName) const
{
    return mImpl.GetUid(inAuthName);
}

    kfsUid_t
AuthContext::GetUid(
    const string& inAuthName,
    kfsGid_t&     outGid,
    kfsUid_t&     outEUid,
    kfsGid_t&     outEGid) const
{
    return mImpl.GetUid(inAuthName, outGid, outEUid, outEGid);
}

    void
AuthContext::SetUserAndGroup(
    const UserAndGroup& inUserAndGroup)
{
    mImpl.SetUserAndGroup(inUserAndGroup);
    mUserAndGroupUpdateCount = inUserAndGroup.GetUpdateCount();
}

    void
AuthContext::DontUseUserAndGroup()
{
    mImpl.DontUseUserAndGroup();
}

    bool
AuthContext::HasUserAndGroup() const
{
    return mImpl.HasUserAndGroup();
}

    bool
AuthContext::SetParameters(
    const char*       inParamNamePrefixPtr,
    const Properties& inParameters,
    AuthContext*      inOtherCtxPtr)
{
    if (! mImpl.SetParameters(
            inParamNamePrefixPtr,
            inParameters,
            inOtherCtxPtr ? &inOtherCtxPtr->mImpl : 0)) {
        return false;
    }
    mUpdateCount++;
    return true;
}

    uint32_t
AuthContext::GetMaxDelegationValidForTime(
    int64_t inCredValidForTime) const
{
    return mImpl.GetMaxDelegationValidForTime(inCredValidForTime);
}

    bool
AuthContext::IsReDelegationAllowed() const
{
    return mImpl.IsReDelegationAllowed();
}

    const char*
AuthContext::GetUserNameAndGroup(
    kfsUid_t  inUid,
    kfsGid_t& outGid,
    kfsUid_t& outEUid,
    kfsGid_t& outEGid) const
{
    return mImpl.GetUserNameAndGroup(inUid, outGid, outEUid, outEGid);
}

    int
AuthContext::GetAuthTypes() const
{
    return mImpl.GetAuthTypes();
}

    bool
AuthContext::IsAuthRequiredSelf(
    const MetaRequest& inOp) const
{
    return mImpl.IsAuthRequired(inOp);
}

    bool
AuthContext::CanRenewAndCancelDelegation(
    kfsUid_t inUid) const
{
    return mImpl.CanRenewAndCancelDelegation(inUid);
}

    void
AuthContext::Clear()
{
    mUpdateCount++;
    mUserAndGroupUpdateCount++;
    mImpl.Clear();
}

}
