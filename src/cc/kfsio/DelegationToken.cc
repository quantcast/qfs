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
#include "qcdio/qcdebug.h"
#include "qcdio/QCUtils.h"

#include "Base64.h"

#include <openssl/hmac.h>
#include <string.h>

#include <iostream>
#include <iomanip>
#include <string>

namespace KFS
{
using std::ostream;
using std::hex;
using std::noshowbase;
using std::setfill;
using std::setw;

class DelegationToken::WorkBuf
{
public:
    WorkBuf()
        {}
    bool SerializeAndSign(
        const DelegationToken& inToken,
        const char*            inKeyPtr,
        int                    inKeyLen,
        char*                  inSignBufPtr)
    {
        Serialize(inToken);
        return Sign(inKeyPtr, inKeyLen, inSignBufPtr);
    }
    bool Sign(
        const char* inKeyPtr,
        int         inKeyLen,
        char*       inSignBufPtr)
    {
        HMAC_CTX theCtx;
        HMAC_CTX_init(&theCtx);
        if (! HMAC_Init_ex(
                &theCtx, inKeyPtr, inKeyLen, EVP_sha1(), 0)) {
            return false;
        }
        unsigned int theLen   = 0;
        const int  theRetFlag = HMAC_Update(
                &theCtx,
                reinterpret_cast<const unsigned char*>(mBuffer),
                kTokenFiledsSize
            ) &&
            HMAC_Final(
                &theCtx,
                reinterpret_cast<unsigned char*>(inSignBufPtr),
                &theLen
            );
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
        const string& inStr)
    {
        const char* thePtr    = inStr.c_str();
        const char* theEndPtr = thePtr + inStr.size();
        while ((*thePtr & 0xFF) <= ' ' && *thePtr != 0) {
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
        return (theLen == kTokenSize);
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
        while (theStartPtr <= thePtr) {
            *thePtr-- = (char)(inVal & 0xFF);
            inVal >>= 8;
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
    int         inKeyLen)
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
    if (! theBuf.SerializeAndSign(*this, inKeyPtr, inKeyLen, mSignature)) {
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
    int         inKeyLen)
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
    return theBuf.SerializeAndSign(*this, inKeyPtr, inKeyLen, mSignature);
}

    string
DelegationToken::ToString()
{
    WorkBuf theBuf;
    return theBuf.ToString(*this, mSignature);
}

    bool
DelegationToken::FromString(
    const string& inString)
{
    WorkBuf theBuf;
    return theBuf.FromBase64(inString);
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
    istream& inStream)
{
    string theStr;
    if ((inStream >> theStr) && ! FromString(theStr)) {
        inStream.setstate(ostream::failbit);
    }
    return inStream;
}

    bool
DelegationToken::Validate(
    const char* inKeyPtr,
    int         inKeyLen) const
{
    char theSignBuf[kSignatureLength];
    WorkBuf theBuf;
    theBuf.Sign(inKeyPtr, inKeyLen, theSignBuf);
    return (memcmp(mSignature, theSignBuf, kSignatureLength) == 0);
}

    ostream&
DelegationToken:: Show(
    ostream& inStream)
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
