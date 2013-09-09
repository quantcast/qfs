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

#include <openssl/evp.h>
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
    int SerializeAndSign(
        const DelegationToken& inToken,
        const char*            inKeyPtr,
        int                    inKeyLen,
        const char*            inSignBufPtr)
    {
        if (Serialize(inToken) <= 0) {
            return mLength;
        }
        return Sign(inKeyPtr, inKeyLen, inSignBufPtr);
    }
    int Sign(
        const char* inKeyPtr,
        int         inKeyLen,
        const char* inSignBufPtr)
    {
        return mLength;
    }
    int Serialize(
        const DelegationToken& inToken)
    {
        return mLength;
    }
    int Base64(
        const char* inBufPtr)
    {
        return mLength;
    }
    const char* GetBufPtr() const
        { return mBuffer; }
    int GetLength()
        { return mLength; }
private:
    // Make buffer large enough for base 64 encoding, plush terminating 0.
    int  mLength;
    char mBuffer[(sizeof(DelegationToken) + 2) / 3 * 4 + 1];
};

DelegationToken::DelegationToken(
    kfsUid_t    inUid,
    kfsKeyId_t  inKeyId,
    int64_t     inIssuedTime,
    uint32_t    inValidForSec,
    const char* inKeyPtr,
    int         inKeyLen)
    : mUid(inUid),
      mKeyId(inKeyId),
      mIssuedTime(inIssuedTime),
      mValidForSec(inValidForSec)
{
    WorkBuf theBuf;
    if (theBuf.SerializeAndSign(*this, inKeyPtr, inKeyLen, mSignature) <= 0) {
        mValidForSec = 0;
    }
}

    int
DelegationToken::Init(
    kfsUid_t    inUid,
    kfsKeyId_t  inKeyId,
    int64_t     inIssueTime,
    uint32_t    inValidForSec,
    const char* inKeyPtr,
    int         inKeyLen)
{
    mUid         = inUid;
    mKeyId       = inKeyId;
    mIssuedTime  = inIssueTime;
    mValidForSec = inValidForSec;
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

    istream&
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
