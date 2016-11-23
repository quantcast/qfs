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
//
//----------------------------------------------------------------------------

#include "HttpResponseHeaders.h"

#include "common/RequestParser.h"
#include "common/httputils.h"

namespace KFS
{
using httputils::kHttpContentLengthKey;

namespace
{
using httputils::EqualsToLowerAscii;

typedef PropertiesTokenizer        Tokenizer;
typedef PropertiesTokenizer::Token Token;

const Token kHttpTransferEncodingKey     ("transfer-encoding");
const Token kHttpTransferEncodingChunked ("chunked");
const Token kHttpTransferEncodingIdentity("identity");
const Token kHttpProtocol                ("http");
const Token kHttpConnectionKey           ("connection");
const Token kHttpConnectionClose         ("close");
const Token kHttpEtagKey                 ("etag");

    static inline bool
Equals(
    const Token& inMixCase,
    const Token& inLowerCase)
{
    return (
        inMixCase.mLen == inLowerCase.mLen &&
        EqualsToLowerAscii(
            inMixCase.mPtr, inLowerCase.mPtr, inLowerCase.mLen)
    );
}

    static inline const char*
SkipSpace(
    const char* inPtr,
    const char* inEndPtr)
{
    const char* thePtr = inPtr;
    while (thePtr < inEndPtr) {
        const int theSym = *thePtr & 0xFF;
        if (' ' < theSym || theSym == '\n' || theSym == '\r') {
            break;
        }
        ++thePtr;
    }
    return thePtr;
}

    static inline const char*
SkipToEndOfLine(
    const char* inPtr,
    const char* inEndPtr)
{
    const char* thePtr = inPtr;
    while (thePtr < inEndPtr) {
        const int theSym = *thePtr++ & 0xFF;
        if (theSym == '\n' || theSym == '\r') {
            break;
        }
    }
    return thePtr;
}

    static inline const char*
SkipNonSpace(
    const char* inPtr,
    const char* inEndPtr)
{
    const char* thePtr = inPtr;
    while (thePtr < inEndPtr) {
        const int theSym = *thePtr++ & 0xFF;
        if (theSym <= ' ') {
            break;
        }
    }
    return thePtr;
}
}

    bool
HttpResponseHeaders::Parse(
    const char* inPtr,
    int         inLength)
{
    Reset();
    const char* const theEndPtr = inPtr + inLength;
    const char*       thePtr    = inPtr;
    thePtr = SkipSpace(thePtr, theEndPtr);
    if (theEndPtr <= thePtr + kHttpProtocol.mLen ||
            ! Equals(Token(thePtr, kHttpProtocol.mLen), kHttpProtocol)) {
        return false;
    }
    thePtr += kHttpProtocol.mLen;
    if (theEndPtr <= thePtr) {
        return false;
    }
    if ((*thePtr++ & 0xFF) == '/') {
        if (theEndPtr <= thePtr) {
            return false;
        }
        int theSym = *thePtr++ & 0xFF;
        if ('1' <= theSym && theSym <= '9') {
            theSym = *thePtr++ & 0xFF;
            if ('0' <= theSym && theSym <= '9') {
                mHttp11OrGreaterFlag = true;
            } else if (theSym == '.') {
                theSym = *thePtr++ & 0xFF;
                if ('1' <= theSym && theSym <= '9') {
                    mHttp11OrGreaterFlag = true;
                }
            }
        }
    }
    thePtr = SkipNonSpace(thePtr, theEndPtr);
    if (! DecIntParser::Parse(thePtr, theEndPtr - thePtr, mStatus) ||
            mStatus < 0) {
        return false;
    }
    thePtr = SkipToEndOfLine(thePtr, theEndPtr);
    Tokenizer theTokenizer(thePtr, theEndPtr - thePtr);
    while (theTokenizer.Next()) {
        const Token& theKey = theTokenizer.GetKey();
        if (Equals(theKey, kHttpContentLengthKey)) {
            const Token& theValue = theTokenizer.GetValue();
            int          theVal;
            const char*  theCurPtr = theValue.mPtr;
            if (DecIntParser::Parse(theCurPtr, theValue.mLen, theVal) &&
                    theValue.mPtr + theValue.mLen <= theCurPtr &&
                    0 <= theVal) {
                mContentLength = theVal;
            }
            continue;
        }
        if (Equals(theKey, kHttpTransferEncodingKey)) {
            const Token& theValue = theTokenizer.GetValue();
            if (Equals(theValue, kHttpTransferEncodingChunked)) {
                mChunkedEcondingFlag = true;
                continue;
            }
            if (Equals(theValue, kHttpTransferEncodingIdentity)) {
                continue;
            }
            mUnsupportedEncodingFlag = true;
            continue;
        }
        if (Equals(theKey, kHttpConnectionKey)) {
            const Token& theValue = theTokenizer.GetValue();
            if (Equals(theValue, kHttpConnectionClose)) {
                mConnectionCloseFlag = true;
            }
            continue;
        }
        if (Equals(theKey, kHttpEtagKey)) {
            const Token& theValue = theTokenizer.GetValue();
            mETagPos = theValue.mPtr - inPtr;
            mETagLen = theValue.mLen;
            continue;
        }
    }
    if (mContentLength < 0 && (
            (100 <= mStatus && mStatus <= 199) ||
            204 == mStatus || 304 == mStatus)) {
        mContentLength = 0;
    }
    return true;
}

}
