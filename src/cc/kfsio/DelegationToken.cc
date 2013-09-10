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

namespace KFS
{
using std::ostream;

class DelegationToken::WorkBuf
{
public:
    WorkBuf()
        : mLength(0)
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
        Write(thePtr, inToken.GetIssuedTime());
        Write(thePtr, inToken.GetValidForSec());
        QCASSERT(mBuffer + kTokenFiledsSize == thePtr);
    }
    const char* GetBufPtr() const
        { return mBuffer; }
    int GetLength()
        { return mLength; }
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
    int  mLength;
    char mBuffer[kTokenSize];

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
    uint32_t    inValidForSec,
    const char* inKeyPtr,
    int         inKeyLen)
    : mUid(inUid),
      mSeq(inSeq),
      mKeyId(inKeyId),
      mIssuedTime(inIssuedTime),
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
    uint32_t    inValidForSec,
    const char* inKeyPtr,
    int         inKeyLen)
{
    mUid         = inUid;
    mSeq         = inSeq;
    mKeyId       = inKeyId;
    mIssuedTime  = inIssueTime;
    mValidForSec = inValidForSec;
    if (! inKeyPtr || inKeyLen <= 0) {
        return;
    }
    WorkBuf theBuf;
    return theBuf.SerializeAndSign(*this, inKeyPtr, inKeyLen, mSignature);
}

    string
DelegationToken::ToString()
{
    return string();
}

    bool
DelegationToken::FromString(
    const string& inString)
{
    return false;
}

    ostream&
DelegationToken::Display(
    ostream& inStream) const
{
    return inStream;
}

    istream&bc 
DelegationToken::Parse(
    istream& inStream)
{
    return inStream;
}

    bool
DelegationToken::Validate(
    const char* inKeyPtr,
    int         inKeyLen) const
{
    return false;
}

    ostream&
DelegationToken:: Show(
    ostream& inStream)
{
    return inStream;
}

}
