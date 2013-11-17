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

#include "ChunkAccessToken.h"

#include "common/MsgLogger.h"

namespace KFS
{
using std::ostream;

class ChunkAccessToken::Subject : public DelegationToken::Subject
{
public:
    Subject(
        kfsChunkId_t inChunkId,
        int64_t      inId)
    {
        char*             thePtr    = mBuf;
        const char* const theEndPtr = thePtr + kSubjectLength;
        *thePtr++ = 'C';
        *thePtr++ = 'A';
        kfsChunkId_t theId = inChunkId;
        while (thePtr < theEndPtr - sizeof(inId)) {
            *thePtr++ = (char)(theId & 0xFF);
            theId >>= 8;
        }
        int64_t theWId = inId;
        while (thePtr < theEndPtr) {
            *thePtr++ = (char)(theWId & 0xFF);
            theWId >>= 8;
        }
    }
    virtual int Get(
        const DelegationToken& inToken,
        const char*&           outPtr)
    {
        outPtr = mBuf;
        return (((inToken.GetFlags() &
            (kUsesWriteIdFlag | kUsesLeaseIdFlag)) != 0) ?
            kSubjectLength : kSubjectLength - (int)sizeof(int64_t));
    }
    operator DelegationToken::Subject* ()
        { return this; }
private:
    enum {
        kSubjectLength = 2 + (int)sizeof(kfsChunkId_t) + (int)sizeof(int64_t)
    };
    char mBuf[kSubjectLength];
};

ChunkAccessToken::ChunkAccessToken(
    kfsChunkId_t inChunkId,
    kfsUid_t     inUid,
    TokenSeq     inSeq,
    kfsKeyId_t   inKeyId,
    int64_t      inIssueTime,
    uint16_t     inFlags,
    uint32_t     inValidForSec,
    const char*  inKeyPtr,
    int          inKeyLen,
    int64_t      inId)
    : mChunkId(inChunkId),
      mDelegationToken(
        inUid,
        inSeq,
        inKeyId,
        inIssueTime,
        inFlags,
        inValidForSec,
        inKeyPtr,
        inKeyLen,
        Subject(inChunkId, inId)
    )
{
}

    bool
ChunkAccessToken::Process(
    kfsChunkId_t      inChunkId,
    const char*       inBufPtr,
    int               inBufLen,
    int64_t           inTimeNowSec,
    const CryptoKeys& inKeys,
    string*           outErrMsgPtr,
    int64_t           inId)
{
    Subject theSubject(inChunkId, inId);
    return (0 <= mDelegationToken.Process(
        inBufPtr,
        inBufLen,
        inTimeNowSec,
        inKeys,
        0,  // inSessionKeyPtr
        -1, // inMaxSessionKeyLength
        outErrMsgPtr,
        &theSubject
    ));
}

    bool
ChunkAccessToken::Process(
    kfsChunkId_t      inChunkId,
    kfsUid_t          inUid,
    const char*       inBufPtr,
    int               inBufLen,
    int64_t           inTimeNowSec,
    const CryptoKeys& inKeys,
    string*           outErrMsgPtr,
    int64_t           inId)
{
    if (! Process(
            inChunkId,
            inUid,
            inBufPtr,
            inBufLen,
            inTimeNowSec,
            inKeys,
            outErrMsgPtr,
            inId)) {
        return false;
    }
    if (inUid != mDelegationToken.GetUid()) {
        if (outErrMsgPtr) {
            *outErrMsgPtr = "user id mismatch";
        } else {
            KFS_LOG_STREAM_ERROR <<
                "user id mismatch: uid: " << inUid <<
                " token uid: " << mDelegationToken.GetUid() <<
            KFS_LOG_EOM;
        }
        return false;
    }
    return true;
}

    ostream&
ChunkAccessToken::ShowSelf(
    ostream& inStream) const
{
    return (inStream <<
        "chunkId: " << mChunkId <<
        " " << mDelegationToken.Show()
    );
}

    /* static */ bool
ChunkAccessToken::WriteToken(
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
    int64_t         inId)
{
    Subject theSubject(inChunkId, inId);
    return DelegationToken::WriteToken(
        inWriter,
        inUid,
        inSeq,
        inKeyId,
        inIssuedTime,
        inFlags,
        inValidForSec,
        inKeyPtr,
        inKeyLen,
        &theSubject
    );
}

    /* static */ bool
ChunkAccessToken::WriteToken(
    ostream&     inStream,
    kfsChunkId_t inChunkId,
    kfsUid_t     inUid,
    TokenSeq     inSeq,
    kfsKeyId_t   inKeyId,
    int64_t      inIssuedTime,
    uint16_t     inFlags,
    uint32_t     inValidForSec,
    const char*  inKeyPtr,
    int          inKeyLen,
    int64_t      inId)
{
    Subject theSubject(inChunkId, inId);
    return DelegationToken::WriteToken(
        inStream,
        inUid,
        inSeq,
        inKeyId,
        inIssuedTime,
        inFlags,
        inValidForSec,
        inKeyPtr,
        inKeyLen,
        &theSubject
    );
}

}
