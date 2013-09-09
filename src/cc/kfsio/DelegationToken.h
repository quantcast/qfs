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

#ifndef DELEGATION_TOKEN_H
#define DELEGATION_TOKEN_H

#include "common/kfstypes.h"

#include <string>
#include <istream>
#include <ostream>

namespace KFS
{
using std::string;

class DelegationToken
{
public:
    DelegationToken()
        : mUid(kKfsUserNone),
          mKeyId(-1),
          mIssueTime(0),
          mValidForSec(0),
        { mSignature[0] = 0; }
    DelegationToken(
        kfsUid_t    inUid,
        kfsKeyId_T  inKeyId,
        int64_t     inIssueTime,
        int32_t     inValidTime
        const char* inKeyPtr,
        int         inKeyLen);
    ~DelegationToken()
        {}
    int Init(
        kfsUid_t    inUid,
        kfsKeyId_T  inKeyId,
        int64_t     inIssueTime,
        int32_t     inValidTime
        const char* inKeyPtr,
        int         inKeyLen);
    string ToString();
    bool FromString(
        const string& inString);
    ostream& Display(
        ostream& inStream) const;
    istream& Parse(
        istream& inStream);
    kfsUid_t GetUid() const
        { return mUid; }
    kfsKeyId_t GetKeyId() const
        { return mKeyId; }
    int64_t GetIssuedTime() const
        { return mIssuedTime; }
    uint32_t GetValidForSec() const
        { return mValidForSec; }
    bool Validate(
        const char* inKeyPtr,
        int         inKeyLen) const;
    ostream& Show(
        ostream& inStream);
private:
    enum { kSignatureLength = 20 };

    kfsUid_t   mUid;
    kfsKeyId_t mKeyId;
    int64_t    mIssuedTime;
    int32_t    mValidForSec;
    char       mSignature[kSignatureLength];
};

ostream& operator << (
    ostream&               inStream,
    const DelegationToken& inToken)
{ return inToken.Display(inStream); }

istream& operator >> (
    istream&               inStream,
    const DelegationToken& inToken)
{ return inToken.Parse(inStream); }

} // namespace KFS

#endif /* DELEGATION_TOKEN_H */
