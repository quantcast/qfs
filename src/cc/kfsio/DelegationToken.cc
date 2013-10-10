//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/9/8
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

#include "DelegationToken.h"
#include "Base64.h"
#include "CryptoKeys.h"

#include "common/MsgLogger.h"
#include "qcdio/qcdebug.h"
#include "qcdio/QCUtils.h"

#include <openssl/hmac.h>
#include <openssl/err.h>
#include <openssl/evp.h>

#include <string.h>

#include <ostream>
#include <istream>
#include <iomanip>
#include <string>
#include <algorithm>

namespace KFS
{
using std::ostream;
using std::istream;
using std::hex;
using std::noshowbase;
using std::setfill;
using std::setw;
using std::min;

class EvpError
{
public:
    typedef unsigned long Error;
    EvpError(
        Error inErr)
        : mError(inErr)
        {}
    EvpError()
        : mError(ERR_get_error())
        {}
    ostream& Display(
        ostream& inStream) const
    {
        const int kBufSize = 127;
        char      theBuf[kBufSize + 1];
        theBuf[0] = 0;
        theBuf[kBufSize] = 0;
        ERR_error_string_n(mError, theBuf, kBufSize);
        return (inStream << theBuf);
    }
private:
    Error mError;
};

    void
EvpErrorStr(
    const char*   inPrefixPtr,
    string*       outErrMsgPtr,
    unsigned long inError)
{
    if (! outErrMsgPtr) {
        return;
    }
    const int kBufSize = 127;
    char      theBuf[kBufSize + 1];
    theBuf[0] = 0;
    theBuf[kBufSize] = 0;
    ERR_error_string_n(inError, theBuf, kBufSize);
    if (inPrefixPtr) {
        *outErrMsgPtr += inPrefixPtr;
    }
    *outErrMsgPtr += theBuf;
}

    void
EvpErrorStr(
    const char* inPrefixPtr,
    string*     outErrMsgPtr)
{
    return EvpErrorStr(inPrefixPtr, outErrMsgPtr, ERR_get_error());
}

static inline ostream& operator << (
    ostream&        inStream,
    const EvpError& inError)
{ return inError.Display(inStream); }

class DelegationToken::WorkBuf
{
public:
    WorkBuf()
        {}
    bool SerializeAndSign(
        const DelegationToken& inToken,
        const char*            inKeyPtr,
        int                    inKeyLen,
        const char*            inSubjectPtr,
        int                    inSubjectLen,
        char*                  inSignBufPtr)
    {
        Serialize(inToken);
        return Sign(inKeyPtr, inKeyLen, inSubjectPtr, inSubjectLen,
            inSignBufPtr);
    }
    bool Sign(
        const char* inKeyPtr,
        int         inKeyLen,
        const char* inSubjectPtr,
        int         inSubjectLen,
        char*       inSignBufPtr,
        string*     inErrMsgPtr = 0)
    {
        HMAC_CTX theCtx;
        HMAC_CTX_init(&theCtx);
        unsigned int theLen = 0;
#if OPENSSL_VERSION_NUMBER < 0x1000000fL
        const bool theRetFlag = true;
        HMAC_Init_ex(&theCtx, inKeyPtr, inKeyLen, EVP_sha1(), 0);
        if (0 < inSubjectLen) {
            HMAC_Update(
                &theCtx,
                reinterpret_cast<const unsigned char*>(inSubjectPtr),
                inSubjectLen
            );
        }
        HMAC_Update(
            &theCtx,
            reinterpret_cast<const unsigned char*>(mBuffer),
            kTokenFiledsSize
        );
        HMAC_Final(
            &theCtx,
            reinterpret_cast<unsigned char*>(inSignBufPtr),
            &theLen
        );
#else
        const bool theRetFlag =
            HMAC_Init_ex(&theCtx, inKeyPtr, inKeyLen, EVP_sha1(), 0) &&
            (inSubjectLen <= 0 ||
                HMAC_Update(
                    &theCtx,
                    reinterpret_cast<const unsigned char*>(inSubjectPtr),
                    inSubjectLen
                )) &&
            HMAC_Update(
                &theCtx,
                reinterpret_cast<const unsigned char*>(mBuffer),
                kTokenFiledsSize
            ) &&
            HMAC_Final(
                &theCtx,
                reinterpret_cast<unsigned char*>(inSignBufPtr),
                &theLen
            );
#endif
        if (! theRetFlag) {
            if (inErrMsgPtr) {
                EvpErrorStr("HMAC failure: ", inErrMsgPtr);
            } else {
                KFS_LOG_STREAM_ERROR <<
                    "HMAC failure: " << EvpError() <<
                KFS_LOG_EOM;
            }
        }
        QCRTASSERT(! theRetFlag || theLen == kSignatureLength);
        HMAC_CTX_cleanup(&theCtx);
        return theRetFlag;
    }
    void Serialize(
        const DelegationToken& inToken)
    {
        char* thePtr = mBuffer;
        Write(thePtr, inToken.GetUid());
        Write(thePtr, inToken.GetSeq());
        Write(thePtr, inToken.GetKeyId());
        Write(thePtr, inToken.GetIssuedTimeAndFlags());
        Write(thePtr, inToken.GetValidForSec());
        QCASSERT(mBuffer + kTokenFiledsSize == thePtr);
    }
    bool FromBase64(
        DelegationToken& inToken,
        const char*      inPtr,
        int              inLen)
    {
        const char* thePtr    = inPtr;
        const char* theEndPtr = thePtr + inLen;
        while (thePtr < theEndPtr && (*thePtr & 0xFF) <= ' ' && *thePtr != 0) {
            thePtr++;
        }
        while (thePtr < theEndPtr && (*theEndPtr & 0xFF) <= ' ') {
            theEndPtr--;
        }
        int theLen = (int)(theEndPtr - thePtr);
        if (theLen <= 0 || kTokenSize < Base64::GetMaxDecodedLength(theLen)) {
            return false;
        }
        theLen = Base64::Decode(thePtr, theLen, mBuffer);
        if (theLen != kTokenSize) {
            return false;
        }
        thePtr = mBuffer;
        Read(thePtr, inToken.mUid);
        Read(thePtr, inToken.mSeq);
        Read(thePtr, inToken.mKeyId);
        Read(thePtr, inToken.mIssuedTimeAndFlags);
        Read(thePtr, inToken.mValidForSec);
        memcpy(inToken.mSignature, thePtr, kSignatureLength);
        return true;
    }
    int ToBase64(
        const DelegationToken& inToken,
        const char*            inSignaturePtr,
        char*                  inBufPtr)
    {
        Serialize(inToken);
        memcpy(mBuffer + kTokenFiledsSize, inSignaturePtr, kSignatureLength);
        return Base64::Encode(mBuffer, kTokenSize, inBufPtr);
    }
    string ToString(
        const DelegationToken& inToken,
        const char*            inSignaturePtr)
    {
        char theBuf[Base64::GetEncodedMaxBufSize(kTokenSize)];
        const int theLen = ToBase64(inToken, inSignaturePtr, theBuf);
        QCRTASSERT(0 < theLen);
        if (theLen <= 0) {
            return string();
        }
        return string(theBuf, theLen);
    }
    ostream& Display(
        ostream&               inStream,
        const DelegationToken& inToken,
        const char*            inSignaturePtr)
    {
        char theBuf[Base64::GetEncodedMaxBufSize(kTokenSize)];
        const int theLen = ToBase64(inToken, inSignaturePtr, theBuf);
        QCRTASSERT(0 < theLen);
        if (theLen <= 0) {
            return inStream;
        }
        return inStream.write(theBuf, theLen);
    }
    int MakeSessionKey(
        const DelegationToken& inToken,
        const char*            inSignaturePtr,
        const char*            inKeyPtr,
        int                    inKeyLen,
        const char*            inSubjectPtr,
        int                    inSubjectLen,
        char*                  inKeyBufferPtr,
        int                    inMaxKeyLen,
        string*                outKeyPtr,
        string*                outErrMsgPtr)
    {
        Serialize(inToken);
        memcpy(mBuffer + kTokenFiledsSize, inSignaturePtr, kSignatureLength);
        return MakeSessionKey(
            inKeyPtr,
            inKeyLen,
            inSubjectPtr,
            inSubjectLen,
            inKeyBufferPtr,
            inMaxKeyLen,
            outKeyPtr,
            outErrMsgPtr
        );
    }
    int MakeSessionKey(
        const char* inKeyPtr,
        int         inKeyLen,
        const char* inSubjectPtr,
        int         inSubjectLen,
        char*       inKeyBufferPtr,
        int         inMaxKeyLen,
        string*     outKeyPtr,
        string*     outErrMsgPtr)
    {
        if (! inKeyPtr || inKeyLen <= 0) {
            KFS_LOG_STREAM_ERROR <<
                "invalid key: " << (void*)inKeyPtr << " len: " << inKeyLen <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        if (0 < inSubjectLen && ! inSubjectPtr) {
            KFS_LOG_STREAM_ERROR <<
                "invalid subject: " << (void*)inSubjectPtr <<
                " len: " << inSubjectLen <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        EVP_MD_CTX theCtx;
        EVP_MD_CTX_init(&theCtx);
        if (! EVP_DigestInit_ex(&theCtx, EVP_sha384(), 0)) {
            if (outErrMsgPtr) {
                EvpErrorStr("EVP_DigestInit_ex failure: ", outErrMsgPtr);
            } else {
                KFS_LOG_STREAM_ERROR <<
                    "EVP_DigestInit_ex failure: " << EvpError() <<
                KFS_LOG_EOM;
            }
            return -EFAULT;
        }
        if ( (0 < inSubjectLen &&
                ! EVP_DigestUpdate(&theCtx, inSubjectPtr, inSubjectLen)) ||
                ! EVP_DigestUpdate(&theCtx, mBuffer, kTokenSize) ||
                ! EVP_DigestUpdate(&theCtx, inKeyPtr, inKeyLen)) {
            if (outErrMsgPtr) {
                EvpErrorStr("EVP_DigestUpdate failure: ", outErrMsgPtr);
            } else {
                KFS_LOG_STREAM_ERROR <<
                    "EVP_DigestUpdate failure: " << EvpError() <<
                KFS_LOG_EOM;
            }
            EVP_MD_CTX_cleanup(&theCtx);
            return -EFAULT;
        }
        unsigned char theMd[EVP_MAX_MD_SIZE];
        unsigned int  theLen = 0;
        if (! EVP_DigestFinal_ex(&theCtx, theMd, &theLen) || theLen <= 0) {
            if (outErrMsgPtr) {
                EvpErrorStr("EVP_DigestFinal_ex failure: ", outErrMsgPtr);
            } else {
                KFS_LOG_STREAM_ERROR <<
                    "EVP_DigestFinal_ex failure: " << EvpError() <<
                KFS_LOG_EOM;
            }
            EVP_MD_CTX_cleanup(&theCtx);
            return -EFAULT;
        }
        QCRTASSERT(theLen <= EVP_MAX_MD_SIZE);
        EVP_MD_CTX_cleanup(&theCtx);
        if (theLen <= 0) {
            return theLen;
        }
        if (outKeyPtr) {
            outKeyPtr->assign(reinterpret_cast<const char*>(theMd), theLen);
            return theLen;
        }
        if (inMaxKeyLen < theLen || ! inKeyBufferPtr) {
            if (outErrMsgPtr) {
                *outErrMsgPtr += "insufficent key buffer size";
            } else {
                KFS_LOG_STREAM_ERROR <<
                    " insufficent key buffer size: " << inMaxKeyLen <<
                    " required: "                    << theLen <<
                KFS_LOG_EOM;
            }
            return -EINVAL;
        }
        memcpy(inKeyBufferPtr, theMd, theLen);
        return theLen;
    }
    string GetSessionKey(
        const DelegationToken& inToken,
        const char*            inSignaturePtr,
        const char*            inKeyPtr,
        int                    inKeyLen,
        const char*            inSubjectPtr,
        int                    inSubjectLen)
    {
        string theRet;
        MakeSessionKey(
            inToken, inSignaturePtr, inKeyPtr, inKeyLen,
            inSubjectPtr, inSubjectLen, 0, 0, &theRet, 0);
        return theRet;
    }
private:
    enum {
        kTokenFiledsSize =
            sizeof(kfsUid_t) +
            sizeof(uint32_t) +
            sizeof(kfsKeyId_t) +
            sizeof(int64_t) +
            sizeof(uint32_t),
        kTokenSize = kTokenFiledsSize + kSignatureLength
    };
    char mBuffer[kTokenSize + 1];

    template<typename T>
    static void Write(
        char*& ioPtr,
        T      inVal)
    {
        char* const theStartPtr = ioPtr;
        ioPtr += sizeof(inVal);
        char*       thePtr      = ioPtr;
        while (theStartPtr <= --thePtr) {
            *thePtr = (char)(inVal & 0xFF);
            inVal >>= 8;
        }
    }
    template<typename T>
    static void Read(
        const char*& ioPtr,
        T&           outVal)
    {
        const char*       thePtr    = ioPtr;
        ioPtr += sizeof(outVal);
        const char* const theEndPtr = ioPtr;
        outVal = 0;
        while (thePtr < theEndPtr) {
            outVal <<= 8;
            outVal |= (*thePtr++ & 0xFF);
        }
    }
};

DelegationToken::DelegationToken(
    kfsUid_t    inUid,
    uint32_t    inSeq,
    kfsKeyId_t  inKeyId,
    int64_t     inIssuedTime,
    uint16_t    inFlags,
    uint32_t    inValidForSec,
    const char* inKeyPtr,
    int         inKeyLen,
    const char* inSubjectPtr,
    int         inSubjectLen)
    : mUid(inUid),
      mSeq(inSeq),
      mKeyId(inKeyId),
      mIssuedTimeAndFlags((inIssuedTime << kIssuedTimeShift) | inFlags),
      mValidForSec(inValidForSec)
{
    if (! inKeyPtr || inKeyLen <= 0) {
        return;
    }
    WorkBuf theBuf;
    if (! theBuf.SerializeAndSign(*this, inKeyPtr, inKeyLen,
            inSubjectPtr, inSubjectLen, mSignature)) {
        mValidForSec = 0;
    }
}

    bool
DelegationToken::Init(
    kfsUid_t    inUid,
    uint32_t    inSeq,
    kfsKeyId_t  inKeyId,
    int64_t     inIssueTime,
    uint16_t    inFlags,
    uint32_t    inValidForSec,
    const char* inKeyPtr,
    int         inKeyLen,
    const char* inSubjectPtr,
    int         inSubjectLen)
{
    mUid                = inUid;
    mSeq                = inSeq;
    mKeyId              = inKeyId;
    mIssuedTimeAndFlags = (inIssueTime << kIssuedTimeShift) | inFlags;
    mValidForSec        = inValidForSec;
    if (! inKeyPtr || inKeyLen <= 0) {
        return false;
    }
    WorkBuf theBuf;
    return theBuf.SerializeAndSign(*this, inKeyPtr, inKeyLen,
        inSubjectPtr, inSubjectLen, mSignature);
}

    string
DelegationToken::ToString()
{
    WorkBuf theBuf;
    return theBuf.ToString(*this, mSignature);
}

    bool
DelegationToken::FromString(
    const string& inString,
    const char*   inKeyPtr,
    int           inKeyLen,
    const char*   inSubjectPtr,
    int           inSubjectLen)
{
    return FromString(
        inString.data(), (int)inString.size(), inKeyPtr, inKeyLen,
        inSubjectPtr, inSubjectLen
    );
}

    bool
DelegationToken::FromString(
    const char* inPtr,
    int         inLen,
    const char* inKeyPtr,
    int         inKeyLen,
    const char* inSubjectPtr,
    int         inSubjectLen)
{
    WorkBuf theBuf;
    if (! theBuf.FromBase64(*this, inPtr, inLen)) {
        return false;
    }
    char theSignature[kSignatureLength];
    return (! inKeyPtr ||
        (theBuf.Sign(inKeyPtr, inKeyLen,
            inSubjectPtr, inSubjectLen, theSignature) &&
        memcmp(theSignature, mSignature, kSignatureLength) == 0)
    );
}

    int
DelegationToken::Process(
    const char*       inPtr,
    int               inLen,
    int64_t           inTimeNowSec,
    const CryptoKeys& inKeys,
    char*             inSessionKeyPtr,
    int               inMaxSessionKeyLength,
    string*           outErrMsgPtr,
    const char*       inSubjectPtr,
    int               inSubjectLen)
{
    WorkBuf theBuf;
    if (! theBuf.FromBase64(*this, inPtr, inLen)) {
        if (outErrMsgPtr) {
            *outErrMsgPtr = "invalid format";
        }
        return false;
    }
    const uint32_t theValidForSec = GetValidForSec();
    if (theValidForSec <= 0) {
        if (outErrMsgPtr) {
            *outErrMsgPtr = "expired: 0 valid for time";
        }
        return false;
    }
    const uint32_t theMaxClockSkewSec = min(uint32_t(5 * 60), theValidForSec);
    if (inTimeNowSec + theMaxClockSkewSec < GetIssuedTime()) {
        if (outErrMsgPtr) {
            *outErrMsgPtr = "issue time is in the future";
        }
        return false;
    }
    if (GetIssuedTime() + theValidForSec < inTimeNowSec) {
        if (outErrMsgPtr) {
            *outErrMsgPtr = "exired";
        }
        return false;
    }
    CryptoKeys::Key theKey;
    if (! inKeys.Find(GetKeyId(), theKey)) {
        if (outErrMsgPtr) {
            *outErrMsgPtr = "no key found";
        }
        return false;
    }
    char theSignature[kSignatureLength];
    if (! theBuf.Sign(
            theKey.GetPtr(),
            theKey.GetSize(),
            inSubjectPtr,
            inSubjectLen,
            theSignature,
            outErrMsgPtr)) {
        return false;
    }
    if (memcmp(theSignature, mSignature, kSignatureLength) != 0) {
        if (outErrMsgPtr) {
            *outErrMsgPtr = "invalid signature";
        }
        return false;
    }
    return (inMaxSessionKeyLength < 0 || theBuf.MakeSessionKey(
        theKey.GetPtr(),
        theKey.GetSize(),
        inSubjectPtr,
        inSubjectLen,
        inSessionKeyPtr,
        inMaxSessionKeyLength,
        0,
        outErrMsgPtr
    ));
}

    ostream&
DelegationToken::Display(
    ostream& inStream) const
{
    WorkBuf theBuf;
    return theBuf.Display(inStream, *this, mSignature);
}

    istream&
DelegationToken::Parse(
    istream&    inStream,
    const char* inKeyPtr,
    int         inKeyLen,
    const char* inSubjectPtr,
    int         inSubjectLen)
{
    string theStr;
    if ((inStream >> theStr) && ! FromString(theStr, inKeyPtr, inKeyLen,
            inSubjectPtr, inSubjectLen)) {
        inStream.setstate(ostream::failbit);
    }
    return inStream;
}

    bool
DelegationToken::Validate(
    const char* inKeyPtr,
    int         inKeyLen,
    const char* inSubjectPtr,
    int         inSubjectLen) const
{
    char theSignBuf[kSignatureLength];
    WorkBuf theBuf;
    theBuf.Sign(inKeyPtr, inKeyLen, inSubjectPtr, inSubjectLen, theSignBuf);
    return (memcmp(mSignature, theSignBuf, kSignatureLength) == 0);
}

    string
DelegationToken::CalcSessionKey(
    const char* inKeyPtr,
    int         inKeyLen,
    const char* inSubjectPtr,
    int         inSubjectLen) const
{
    WorkBuf theBuf;
    return theBuf.GetSessionKey(*this, mSignature, inKeyPtr, inKeyLen,
        inSubjectPtr, inSubjectLen);
}

    ostream&
DelegationToken:: ShowSelf(
    ostream& inStream) const
{
    const ostream::fmtflags theFlags = inStream.flags();
    inStream <<
    "uid: "   << mUid <<
    " seq: "   << mSeq <<
    " keyId: " << mKeyId <<
    " time:  " << GetIssuedTime() << "+" << mValidForSec <<
    " flags: 0x" << hex << GetFlags() <<
    " sign: ";
    if (0u < mValidForSec) {
        inStream << hex << noshowbase;
        for (int i = 0; i < kSignatureLength; i++) {
            inStream << setfill('0') << setw(2) <<
                ((int)mSignature[i] & 0xFF);
        }
    }
    inStream.setf(theFlags);
    return inStream;
}

}
