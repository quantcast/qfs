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

#ifndef KFSIO_BASE64_H
#define KFSIO_BASE64_H

namespace KFS
{

class Base64
{
public:
    static int EncodedLength(
        int inLength)
        { return ((inLength + 2) / 3 * 4); }
    static int GetEncodedMaxBufSize(
        int inLength)
        { return EncodedLength(inLength) + 1; }
    static int GetMaxDecodedLength(
        int inLength)
        { return ((inLength + 3) / 4 * 3); }
    static int Encode(
        const char* inBufPtr,
        int         inBufLength,
        char*       inEncodedBufPtr);
    static int Decode(
        const char* inBufPtr,
        int         inBufLength,
        char*       inDecodedBufPtr);
};

} // namespace KFS

#endif /* KFSIO_BASE64_H */
