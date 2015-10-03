//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/10/1
// Author:  Mike Ovsiannikov
//
// Copyright 2015 Quantcast Corp.
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

#include "HttpChunkedDecoder.h"

#include "IOBuffer.h"

#include "common/RequestParser.h"

#include <string.h>
#include <errno.h>
#include <algorithm>

namespace KFS
{
using std::min;
using std::max;

    int
HttpChunkedDecoder::Parse(
    IOBuffer& inBuffer)
{
    const int kCrLfLen = 2;
    for (; ;) {
        if (0 == mLength) {
            // Discard trailing headers, if any.
            const int k2CrLfLen = 2 * kCrLfLen;
            const int theRem    = inBuffer.BytesConsumable();
            if (k2CrLfLen <= theRem) {
                const int theIdx = inBuffer.IndexOf(0, "\r\n\r\n");
                inBuffer.Consume(theIdx < 0 ?
                    theRem - (k2CrLfLen - 1) : theIdx + k2CrLfLen);
                if (0 <= theIdx) {
                    mLength = -1;
                    return 0;
                }
            }
            return mMaxReadAhead;
        }
        if (0 < mChunkRem) {
            const int theLen = min(
                inBuffer.BytesConsumable(), mChunkRem - kCrLfLen);
            if (0 < theLen) {
                mIOBuffer.ReplaceKeepBuffersFull(
                    &inBuffer,
                    mIOBuffer.BytesConsumable(),
                    theLen
                );
                mChunkRem -= theLen;
            }
            if (mChunkRem <= kCrLfLen) {
                mChunkRem -= inBuffer.Consume(mChunkRem);
            }
            if (0 < mChunkRem) {
                break;
            }
        }
        const int theIdx = inBuffer.IndexOf(0, "\r\n");
        if (theIdx <= 0) {
            return mMaxReadAhead;
        }
        int               theLen    = min(theIdx, (int)kLengthBufSize);
        const char* const theLenPtr = inBuffer.CopyOutOrGetBufPtr(mBuf, theLen);
        const int         theSym    = theLenPtr[0] & 0xFF;
        if (! (0 <= theSym && theSym <= '9') &&
                ! ('A' <= theSym && theSym <= 'F') &&
                ! ('a' <= theSym && theSym <= 'f')) {
            return -EINVAL;
        }
        const char*       theEndPtr = theLenPtr + theLen;
        const char* const thePtr    =
            (const char*)memchr(theLenPtr, ';', theEndPtr - theLenPtr);
        if (thePtr) {
            theEndPtr = thePtr;
        }
        const char* theCurPtr = theLenPtr;
        if (! HexIntParser::Parse(
                    theCurPtr, theEndPtr - theCurPtr, mLength) ||
                mLength < 0) {
            return -EINVAL;
        }
        inBuffer.Consume(0 < mLength ? theIdx + kCrLfLen : theIdx);
        if (0 < mLength) {
            mChunkRem = mLength + kCrLfLen;
        }
    }
    return max(mMaxReadAhead, mChunkRem);
}

}
