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

class ChunkAccessToken::Subject
{
public:
    Subject(
        kfsChunkId_t inChunkId)
    {
        char*             thePtr    = mBuf;
        const char* const theEndPtr = thePtr + kSubjectLength;
        *thePtr++ = 'C';
        *thePtr++ = 'A';
        kfsChunkId_t theId = inChunkId;
        while (thePtr < theEndPtr) {
            *thePtr++ = (char)(theId & 0xFF);
            theId >>= 8;
        }
    }
    const char* GetPtr() const
        { return mBuf; }
    static int GetSize()
        { return kSubjectLength; }
    operator const char*() const
        { return mBuf; }
private:
    enum { kSubjectLength = (int)sizeof(kfsChunkId_t) + 2 };
    char mBuf[kSubjectLength];
};

ChunkAccessToken::ChunkAccessToken(
    kfsChunkId_t inChunkId,
    kfsUid_t     inUid,
    uint32_t     inSeq,
    kfsKeyId_t   inKeyId,
    int64_t      inIssueTime,
    uint16_t     inFlags,
    uint32_t     inValidForSec,
    const char*  inKeyPtr,
    int          inKeyLen)
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
        Subject(inChunkId),
        Subject::GetSize()
    )
{
}

    bool
ChunkAccessToken::Process(
    kfsChunkId_t      inChunkId,
    kfsUid_t          inUid,
    const char*       inBufPtr,
    int               inBufLen,
    int64_t           inTimeNowSec,
    const CryptoKeys& inKeys,
    string*           outErrMsgPtr)
{
    Subject const theSubject(inChunkId);
    const bool theValidFlag = mDelegationToken.Process(
        inBufPtr,
        inBufLen,
        inTimeNowSec,
        inKeys,
        0,  // inSessionKeyPtr
        -1, // inMaxSessionKeyLength
        outErrMsgPtr,
        theSubject.GetPtr(),
        theSubject.GetSize()
    );
    if (! theValidFlag) {
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

}
