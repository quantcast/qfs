//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/10/1
// Author:  Mike Ovsiannikov
//
// Copyright 2015,2016 Quantcast Corporation. All rights reserved.
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
// \brief Http chunked encoding parser.
//
//----------------------------------------------------------------------------

#ifndef KFSIO_HTTP_CHUNKED_DECODER_H
#define KFSIO_HTTP_CHUNKED_DECODER_H

namespace KFS
{

class IOBuffer;

class HttpChunkedDecoder
{
public:
    HttpChunkedDecoder(
        IOBuffer& inIOBuffer,
        int       inMaxReadAhead = 2 << 10)
        : mIOBuffer(inIOBuffer),
          mMaxReadAhead(inMaxReadAhead),
          mLength(-1),
          mChunkRem(0),
          mAlignedFlag(false)
        {}
    ~HttpChunkedDecoder()
        {}
    void Reset()
    {
        mLength      = -1;
        mChunkRem    = 0;
        mAlignedFlag = false;
    }
    int Parse(
        IOBuffer& inBuffer);
private:
    IOBuffer& mIOBuffer;
    int       mMaxReadAhead;
    int       mLength;
    int       mChunkRem;
    bool      mAlignedFlag;
};

} // namespace KFS

#endif /* KFSIO_HTTP_CHUNKED_DECODER_H */
