//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/09/30
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
// \brief Minimal http response headers parser.
//
//----------------------------------------------------------------------------

#ifndef KFSIO_HTTP_RESPONSE_HEADERS_H
#define KFSIO_HTTP_RESPONSE_HEADERS_H

namespace KFS
{
class HttpResponseHeaders
{
public:
    HttpResponseHeaders()
        : mStatus(-1),
          mContentLength(-1),
          mETagPos(-1),
          mETagLen(-1),
          mChunkedEcondingFlag(false),
          mUnsupportedEncodingFlag(false),
          mHttp11OrGreaterFlag(false),
          mConnectionCloseFlag(false)
        {}
    bool Parse(
        const char* inPtr,
        int         inLength);
    void Reset()
    {
        mStatus                  = -1;
        mContentLength           = -1;
        mETagPos                 = -1;
        mETagLen                 = -1;
        mChunkedEcondingFlag     = false;
        mUnsupportedEncodingFlag = false;
        mHttp11OrGreaterFlag     = false;
        mConnectionCloseFlag     = false;
    }
    int GetStatus() const
        { return mStatus; }
    int GetContentLength() const
        { return mContentLength; }
    int GetETagPosition() const
        { return mETagPos; }
    int GetETagLength() const
        { return mETagLen; }
    bool IsChunkedEconding() const
        { return mChunkedEcondingFlag; }
    bool IsUnsupportedEncoding() const
        { return mUnsupportedEncodingFlag; }
    bool IsHttp11OrGreater() const
        { return mHttp11OrGreaterFlag; }
    bool IsConnectionClose() const
        { return mConnectionCloseFlag; }
private:
    int  mStatus;
    int  mContentLength;
    int  mETagPos;
    int  mETagLen;
    bool mChunkedEcondingFlag;
    bool mUnsupportedEncodingFlag;
    bool mHttp11OrGreaterFlag;
    bool mConnectionCloseFlag;
};

} // namespace KFS

#endif /* KFSIO_HTTP_RESPONSE_HEADERS_H */
