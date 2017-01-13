//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/9/9
// Author: Mike Ovsiannikov
//
// Copyright 2013,2016 Quantcast Corporation. All rights reserved.
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

#include "Base64.h"
#include "common/StBuffer.h"
#include <openssl/evp.h>

namespace KFS
{

    int
Base64::Encode(
    const char* inBufPtr,
    int         inBufLength,
    char*       inEncodedBufPtr,
    bool        inUrlSafeFmtFlag)
{
    const int theLen = EVP_EncodeBlock(
        reinterpret_cast<unsigned char*>(inEncodedBufPtr),
        reinterpret_cast<const unsigned char*>(inBufPtr),
        inBufLength
    );
    if (inUrlSafeFmtFlag) {
        for (int i = 0; i < theLen; i++) {
            switch (inEncodedBufPtr[i] & 0xFF) {
                case '/': inEncodedBufPtr[i] = '_'; break;
                case '+': inEncodedBufPtr[i] = '-'; break;
                default:                            break;
            }
        }
    }
    return theLen;
}

    int 
Base64::Decode(
    const char* inBufPtr,
    int         inBufLength,
    char*       inDecodedBufPtr,
    bool        inUrlSafeFmtFlag)
{
    int thePadding = 0;
    if (3 < inBufLength) {
        if (inBufPtr[inBufLength-1] == '=') {
            thePadding++;
        }
        if (inBufPtr[inBufLength-2] == '=') {
            thePadding++;
        }
    }
    int theLen;
    if (inUrlSafeFmtFlag) {
        if (inBufLength <= 0) {
            return 0;
        }
        StBufferT<unsigned char, 128> theBuf;
        unsigned char* const theBufPtr = theBuf.Resize(inBufLength);
        unsigned char*       thePtr    = theBufPtr;
        for (int i = 0; i < inBufLength; i++, thePtr++) {
            switch (inBufPtr[i] & 0xFF) {
                case '_': *thePtr = '/';         break;
                case '-': *thePtr = '+';         break;
                default:  *thePtr = inBufPtr[i]; break;
            }
        }
        theLen = EVP_DecodeBlock(
            reinterpret_cast<unsigned char*>(inDecodedBufPtr),
            theBufPtr,
            inBufLength
        );
    } else  {
        theLen = EVP_DecodeBlock(
            reinterpret_cast<unsigned char*>(inDecodedBufPtr),
            reinterpret_cast<const unsigned char*>(inBufPtr),
            inBufLength
        );
    }
    return (2 < theLen ? theLen - thePadding : theLen);
}

}

