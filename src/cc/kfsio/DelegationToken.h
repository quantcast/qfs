//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/9/7
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

#ifndef KFSIO_DELEGATION_TOKEN_H
#define KFSIO_DELEGATION_TOKEN_H

#include "common/kfstypes.h"

#include <stddef.h>

#include <string>
#include <istream>
#include <ostream>

namespace KFS
{
using std::string;
using std::istream;
using std::ostream;

class DelegationToken
{
public:
    enum { kSignatureLength = 20 };
    enum {
        kAllowDelegationFlag = 0x1
    };

    DelegationToken()
        : mUid(kKfsUserNone),
          mSeq(0),
          mKeyId(-1),
          mIssuedTimeAndFlags(0),
          mValidForSec(0)
        { mSignature[0] = 0; }
    DelegationToken(
        kfsUid_t    inUid,
        uint32_t    inSeq,
        kfsKeyId_t  inKeyId,
        int64_t     inIssueTime,
        uint16_t    inFlags,
        uint32_t    inValidForSec,
        const char* inKeyPtr,
        int         inKeyLen);
    ~DelegationToken()
        {}
    bool Init(
        kfsUid_t    inUid,
        uint32_t    inSeq,
        kfsKeyId_t  inKeyId,
        int64_t     inIssuedTime,
        uint16_t    inFlags,
        uint32_t    inValidForSec,
        const char* inKeyPtr,
        int         inKeyLen);
    string ToString();
    bool FromString(
        const string& inString,
        const char*   inKeyPtr,
        int           inKeyLen);
    bool FromString(
        const char* inPtr,
        int         inLen,
        const char* inKeyPtr,
        int         inKeyLen);
    ostream& Display(
        ostream& inStream) const;
    istream& Parse(
        istream&    inStream,
        const char* inKeyPtr,
        int         inKeyLen);
    kfsUid_t GetUid() const
        { return mUid; }
    kfsUid_t GetSeq() const
        { return mSeq; }
    kfsKeyId_t GetKeyId() const
        { return mKeyId; }
    int64_t GetIssuedTimeAndFlags() const
        { return mIssuedTimeAndFlags; }
    int64_t GetIssuedTime() const
        { return (mIssuedTimeAndFlags >> kIssuedTimeShift); }
    uint16_t GetFlags() const
        { return (uint16_t)mIssuedTimeAndFlags; }
    uint32_t GetValidForSec() const
        { return mValidForSec; }
    bool Validate(
        const char* inKeyPtr,
        int         inKeyLen) const;
    string GetSessionKey(
        const char* inKeyPtr,
        int         inKeyLen) const;
    ostream& Show(
        ostream& inStream);
    string CalcSessionKey(
        const char* inKeyPtr,
        int         inKeyLen) const;
private:
    enum { kIssuedTimeShift = 16 };
    kfsUid_t   mUid;
    uint32_t   mSeq;
    kfsKeyId_t mKeyId;
    int64_t    mIssuedTimeAndFlags;
    uint32_t   mValidForSec;
    char       mSignature[kSignatureLength];

    class WorkBuf;
    friend class WorkBuf;
};

ostream& operator << (
    ostream&               inStream,
    const DelegationToken& inToken)
{ return inToken.Display(inStream); }

istream& operator >> (
    istream&         inStream,
    DelegationToken& inToken)
{ return inToken.Parse(inStream, 0, 0); }

} // namespace KFS

#endif /* KFSIO_DELEGATION_TOKEN_H */
