//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/9/9
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

#include "Base64.h"
#include <openssl/evp.h>

namespace KFS
{

    int
Base64::Encode(
    const char* inBufPtr,
    int         inBufLength,
    char*       inEncodedBufPtr)
{
    return EVP_EncodeBlock(
        reinterpret_cast<unsigned char*>(inEncodedBufPtr),
        reinterpret_cast<const unsigned char*>(inBufPtr),
        inBufLength
    );
}

    int 
Base64::Decode(
    const char* inBufPtr,
    int         inBufLength,
    char*       inDecodedBufPtr)
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
    const int theLen = EVP_DecodeBlock(
        reinterpret_cast<unsigned char*>(inDecodedBufPtr),
        reinterpret_cast<const unsigned char*>(inBufPtr),
        inBufLength
    );
    return (2 < theLen ? theLen - thePadding : theLen);
}

}

