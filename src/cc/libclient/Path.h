//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/07/20
// Author: Mike Ovsiannikov
//
// Copyright 2012 Quantcast Corp.
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
// \brief I-node path. Used for path resolution, and "normalization".
//
//----------------------------------------------------------------------------

#ifndef LIBKFSCLIENT_PATH_H
#define LIBKFSCLIENT_PATH_H

#include <string.h>

#include <string>
#include <vector>

namespace KFS {
namespace client {

using std::string;
using std::vector;

class Path
{
public:
    class Token
    {
    public:
        Token(
            const char* inPtr = 0,
            size_t      inLen = 0)
            : mPtr(inPtr),
              mLen(inLen)
            {}
        bool operator==(
            const Token& inToken) const
        {
            return (inToken.mLen == mLen &&
                memcmp(inToken.mPtr, mPtr, mLen) == 0);
        }
        bool operator!=(
            const Token& inToken) const
            { return ! (*this == inToken); }
        const char* mPtr;
        size_t      mLen;
    };
    typedef vector<Token>              Components;
    typedef Components::const_iterator iterator;

    Path(
        const char* inPathPtr    = 0,
        size_t      inPathLength = 0);
    bool Set(
        const char* inPathPtr,
        size_t      inPathLength);
    void Clear() { Set(0, 0); }
    string ToString() const { return ToString(mComponents); }
    string NormPath() const { return ToString(mNormComponents); }
    iterator begin()  const { return mComponents.begin(); }
    iterator end()    const { return mComponents.end(); }
    size_t size()     const { return mComponents.size(); }
    bool empty()      const { return mComponents.empty(); }
    const Token& operator[](size_t i) const { return mComponents[i]; }
    bool IsDir()        const { return mDirFlag; }
    bool IsNormalized() const { return mNormalizedFlag; }
private:
    Components mComponents;
    Components mNormComponents;
    bool       mDirFlag;
    bool       mNormalizedFlag;

    string ToString(const Path::Components& comps) const;

private:
    Path(const Path& inPath);
    Path& operator=(const Path& inPath);
};

}}

#endif /* LIBKFSCLIENT_PATH_H */
