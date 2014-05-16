//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/10/9
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

#ifndef KFSIO_CHUNK_ACCESS_TOKEN_H
#define KFSIO_CHUNK_ACCESS_TOKEN_H

#include "common/kfstypes.h"
#include "DelegationToken.h"

#include <string>
#include <ostream>

namespace KFS
{
using std::string;
using std::ostream;

class CryptoKeys;
class IOBufferWriter;

class ChunkAccessToken
{
public:
    typedef DelegationToken::TokenSeq TokenSeq;
    enum {
        kAllowReadFlag      = 0x0100,
        kAllowWriteFlag     = 0x0200,
        kAllowClearTextFlag = 0x0400,
        kUsesWriteIdFlag    = 0x0800,
        kUsesLeaseIdFlag    = 0x1000,
        kAppendRecoveryFlag = 0x2000
    };
    class ShowToken
    {
    public:
        ShowToken(
            const ChunkAccessToken& inToken)
            : mToken(inToken)
            {}
        ostream& Display(
            ostream& inStream) const
            { return mToken.ShowSelf(inStream); }
    private:
        const ChunkAccessToken& mToken;
    };

    ChunkAccessToken()
        : mChunkId(),
          mDelegationToken()
        {}
    ChunkAccessToken(
        kfsChunkId_t inChunkId,
        kfsUid_t     inUid,
        TokenSeq     inSeq,
        kfsKeyId_t   inKeyId,
        int64_t      inIssueTime,
        uint16_t     inFlags,
        uint32_t     inValidForSec,
        const char*  inKeyPtr,
        int          inKeyLen,
        int64_t      inId = -1);
    bool Process(
        kfsChunkId_t      inChunkId,
        const char*       inBufPtr,
        int               inBufLen,
        int64_t           inTimeNowSec,
        const CryptoKeys& inKeys,
        string*           outErrMsgPtr,
        int64_t           inId = -1);
    bool Process(
        kfsChunkId_t      inChunkId,
        kfsUid_t          inUid,
        const char*       inBufPtr,
        int               inBufLen,
        int64_t           inTimeNowSec,
        const CryptoKeys& inKeys,
        string*           outErrMsgPtr,
        int64_t           inId = -1);
    ShowToken Show() const
        { return ShowToken(*this); }
    ostream& ShowSelf(
        ostream& inStream) const;
    ostream& Display(
        ostream& inStream) const
        { return mDelegationToken.Display(inStream); }
    const DelegationToken& Get() const
        { return mDelegationToken; }
    static bool WriteToken(
        IOBufferWriter& inWriter,
        kfsChunkId_t    inChunkId,
        kfsUid_t        inUid,
        TokenSeq        inSeq,
        kfsKeyId_t      inKeyId,
        int64_t         inIssuedTime,
        uint16_t        inFlags,
        uint32_t        inValidForSec,
        const char*     inKeyPtr,
        int             inKeyLen,
        int64_t         inId = -1);
    static bool WriteToken(
        ostream&     inSteram,
        kfsChunkId_t inChunkId,
        kfsUid_t     inUid,
        TokenSeq     inSeq,
        kfsKeyId_t   inKeyId,
        int64_t      inIssuedTime,
        uint16_t     inFlags,
        uint32_t     inValidForSec,
        const char*  inKeyPtr,
        int          inKeyLen,
        int64_t      inId = -1);
private:
    kfsChunkId_t    mChunkId;
    DelegationToken mDelegationToken;

    class Subject;
private:
    ChunkAccessToken(
        const ChunkAccessToken& inToken);
    ChunkAccessToken& operator=(
        const ChunkAccessToken& inToken);
};

inline static ostream& operator << (
    ostream&                           inStream,
    const ChunkAccessToken::ShowToken& inShowToken)
{ return inShowToken.Display(inStream); }

inline static ostream& operator << (
    ostream&                inStream,
    const ChunkAccessToken& inToken)
{ return inToken.Display(inStream); }

}

#endif /* KFSIO_CHUNK_ACCESS_TOKEN_Hf */
