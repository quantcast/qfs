//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2016/8/9
// Author: Mike Ovsiannikov
//
// Copyright 2014,2016 Quantcast Corporation. All rights reserved.
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
// Chunk / object store block's file name / key generation implementation.
//
//----------------------------------------------------------------------------

#include "blockname.h"
#include "Base64.h"
#include "checksum.h"

#include "common/IntToString.h"

#include <algorithm>

namespace KFS
{
using std::max;

template<typename T>
    inline static char*
IntToBytes(
    char*    inBufPtr,
    const T& inVal)
{
    char* thePtr  = inBufPtr;
    T     theByte = inVal;
    for (char* const theEndPtr = thePtr + sizeof(T); ;) {
        *thePtr++ = (char)theByte;
        if (theEndPtr <= thePtr) {
            break;
        }
        theByte >>= 8;
    }
    return thePtr;
}

    static inline size_t
UrlSafeBase64Encode(
    const char* inDataPtr,
    size_t      inSize,
    char*       inBufPtr)
{
    const bool kUrlSafeFmtFlag = true;
    int theLen = Base64::Encode(
        inDataPtr, (int)inSize, inBufPtr, kUrlSafeFmtFlag);
    // Remove padding.
    while (0 < theLen && (inBufPtr[theLen - 1] & 0xFF) == '=') {
        theLen--;
    }
    return (theLen < 0 ? 0 : theLen);
}

    bool
AppendChunkFileNameOrObjectStoreBlockKey(
    string&      inName,
    int64_t      inFileSystemId,
    kfsFileId_t  inFileId,
    kfsChunkId_t inId,
    kfsSeq_t     inVersion,
    string&      ioFileSystemIdSuffix)
{
    const char kSeparator = '.';
    if (inVersion < 0) {
        uint32_t     theCrc32;
        const size_t kBlockIdSize = sizeof(inId) + sizeof(inVersion);
        char         theBuf[max(
            (size_t)Base64::EncodedLength(sizeof(theCrc32)) + 1, kBlockIdSize)];
        char         theCrc32Buf[sizeof(theCrc32)];
        IntToBytes(IntToBytes(theBuf, inId), inVersion);
        theCrc32 = ComputeCrc32(theBuf, kBlockIdSize);
        size_t theLen = IntToBytes(theCrc32Buf, theCrc32) - theCrc32Buf;
        theLen = UrlSafeBase64Encode(theCrc32Buf, theLen, theBuf);
        if (theLen <= 0) {
            return false;
        }
        theBuf[theLen++] = kSeparator;
        inName.append(theBuf, theLen);
    }
    if (0 <= inVersion || inFileId != inId) {
        AppendDecIntToString(inName, inFileId);
        inName += kSeparator;
    }
    AppendDecIntToString(inName, inId);
    inName += kSeparator;
    AppendDecIntToString(inName, inVersion);
    if (inVersion < 0 && 0 <= inFileSystemId) {
        if (ioFileSystemIdSuffix.empty()) {
            char bytes[sizeof(inFileSystemId)];
            const size_t theLen = IntToBytes(bytes, inFileSystemId) - bytes;
            char theBuf[Base64::EncodedLength(sizeof(inFileSystemId)) + 1];
            theBuf[0] = kSeparator;
            // The "theLen" below is one byte short, therefore the 4 most
            // significant bits of file system ID are not included in the
            // suffix. The most significant bit is always 0, i.e. only 3 bits
            // are actually lost. Do not attempt to "fix" / change this, as
            // doing now so will break backward compatibility with the existing
            // object store QFS file systems.
            ioFileSystemIdSuffix.assign(theBuf, UrlSafeBase64Encode(
                bytes, theLen, theBuf + 1));
            if (ioFileSystemIdSuffix.empty()) {
                return false;
            }
        }
        inName += ioFileSystemIdSuffix;
    }
    return true;
}

} // namespace KFS
