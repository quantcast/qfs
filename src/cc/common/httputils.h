//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2011/04/09
// Author: Mike Ovsiannikov
//
// Copyright 2010-2011 Quantcast Corp.
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
// Very basic http utility functions.
//
//----------------------------------------------------------------------------

#ifndef HTTPUTILS_H
#define HTTPUTILS_H

#include "RequestParser.h"
#include <string>

namespace KFS
{
namespace httputils
{

using std::string;

template <typename IOBufferT>
inline static int
GetHeaderLength(const IOBufferT& iobuf)
{
    int idx = iobuf.IndexOf(0, "\r\n\r\n");
    if (idx >= 0) {
        return idx + 4;
    }
    idx = iobuf.IndexOf(0, "\n\n");
    if (idx >= 0) {
        return idx + 2;
    }
    idx = iobuf.IndexOf(0, "\r\r");
    return (idx >= 0 ? idx + 2 : idx);
}

inline static int
HexToChar(int c)
{
    return  (c >= '0' && c <= '9' ? c - '0'
        : c >= 'A' && c <= 'F' ? c - 'A' + 10
        : c - 'a' + 10
    );
}

inline static int
AsciiCharToLower(int c)
{
    return ((c >= 'A' && c <= 'Z') ? 'a' + (c - 'A') : c);
}

inline static bool
EqualsAsciiIgnoreCase(const char* s1, const char* s2, int len)
{
    while (len-- > 0) {
        if (AsciiCharToLower(*s1++) != AsciiCharToLower(*s2++)) {
            return false;
        }
    }
    return true;
}

inline static bool
EqualsToLowerAscii(const char* s1, const char* s2, int len)
{
    while (len-- > 0) {
        if (AsciiCharToLower(*s1++) != *s2++) {
            return false;
        }
    }
    return true;
}

inline static bool
IsProtoAtLeast11(const char* ptr, const char* end)
{
    while (ptr < end && *ptr <= ' ') {
        ++ptr;
    }
    while (ptr < end && *ptr == '/') {
        ++ptr;
    }
    int major = -1;
    int minor = -1;
    return (
        ValueParser::ParseInt(ptr, end - ptr, major) &&
        (major > 1 ||
        (ptr < end && *ptr++ == '.' &&
            ValueParser::ParseInt(ptr, end - ptr, minor) &&
            minor >= 1
        )));
}

class ByteIterator
{
public:
    ByteIterator(
        const char* start,
        const char* end)
        : mPtr(start),
          mEnd(end)
        {}
    const char* Next()
        { return (mPtr < mEnd ? mPtr++ : 0); }
private:
    const char*       mPtr;
    const char* const mEnd;
};

template <typename T, typename PropertiesT>
inline static void
LoadUrlEncodedParams(T& it, PropertiesT& props)
{
    const char* ptr;
    while ((ptr = it.Next()) && *ptr <= ' ')
        {}

    string buf;
    string key;
    for (; ptr && *ptr > ' '; ptr = it.Next()) {
        int sym = *ptr;
        if (sym == '%') {
            if (! (ptr = it.Next())) {
                break;
            }
            sym = (HexToChar(*ptr & 0xFF) & 0xF) << 4;
            if (! (ptr = it.Next())) {
                break;
            }
            sym += HexToChar(*ptr & 0xFF) & 0xF;
        } else if (sym == '+') {
            sym = ' ';
        } else if (sym == '=') {
            key = buf;
            buf.clear();
            continue;
        } else if (sym == '&') {
            if (! key.empty()) {
                props.setValue(key, buf);
                key.clear();
            }
            buf.clear();
            continue;
        }
        buf.append(1, char(sym));
    }
    if (! key.empty()) {
        props.setValue(key, buf);
    }
}

const PropertiesTokenizer::Token kHttpContentLengthKey("content-length");
const PropertiesTokenizer::Token kHttpHostKey("host");

template <typename LengthT, typename HostT>
inline static bool
GetContentLengthAndHost(const char* ptr, const char* end,
    LengthT& contentLength, HostT* host)
{
    bool                found = true;
    PropertiesTokenizer tokenizer(ptr, end - ptr);
    while (tokenizer.Next()) {
        if (tokenizer.GetKey().mLen == kHttpContentLengthKey.mLen &&
                EqualsToLowerAscii(
                    tokenizer.GetKey().mPtr,
                    kHttpContentLengthKey.mPtr,
                    kHttpContentLengthKey.mLen
                )) {
            const char* ptr = tokenizer.GetValue().mPtr;
            found = ValueParser::ParseInt(
                ptr, tokenizer.GetValue().mLen, contentLength
            ) || found;
        } else if (host &&
                tokenizer.GetKey().mLen == kHttpHostKey.mLen &&
                EqualsToLowerAscii(
                    tokenizer.GetKey().mPtr,
                    kHttpHostKey.mPtr,
                    kHttpHostKey.mLen
                )) {
            ValueParser::SetValue(
                tokenizer.GetValue().mPtr,
                tokenizer.GetValue().mLen,
                HostT(),
                *host
            );
        }
    }
    return found;
}

template <typename T>
inline static bool
GetContentLength(const char* ptr, const char* end, T& contentLength)
{
    return GetContentLengthAndHost<T, string>(ptr, end, contentLength, 0);
}

}}

#endif /* HTTPUTILS_H */
