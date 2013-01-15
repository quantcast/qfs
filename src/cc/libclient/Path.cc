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
// \brief Generic i-node path implementation.
//
//----------------------------------------------------------------------------

#include "Path.h"

#include <cassert>
#include <cerrno>
#include <sstream>

#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>

namespace KFS
{
namespace client
{
using std::string;
using std::istringstream;

Path::Path(
    const char* inPathPtr,
    size_t      inLength)
    : mComponents(),
      mNormComponents(),
      mDirFlag(false),
      mNormalizedFlag(false)
{
    if (inLength > 0) {
        Path::Set(inPathPtr, inLength);
    }
}

bool
Path::Set(
    const char* inPathPtr,
    size_t      inLength)
{
    mComponents.clear();
    mNormComponents.clear();
    mComponents.reserve(16);
    mNormComponents.reserve(16);

    const Token       kRootDir("/",    1);
    const Token       kThisDir(".",    1);
    const Token       kParentDir("..", 2);
    const char* const theEndPtr = inPathPtr + inLength;
    mNormalizedFlag = true;
    for (const char* theStartPtr = inPathPtr;
            theStartPtr < theEndPtr;
            ++theStartPtr) {
        const char* const theSlashPtr =
            (const char*)memchr(theStartPtr, '/', theEndPtr - theStartPtr);
        if (theSlashPtr == inPathPtr) {
            const Token theRoot(theSlashPtr, 1);
            mComponents.push_back(theRoot);
            mNormComponents.push_back(theRoot);
            mNormalizedFlag = false;
            continue;
        }
        if (theSlashPtr == theStartPtr) {
            mNormalizedFlag = false;
            continue;
        }
        const char* const thePtr = theSlashPtr ? theSlashPtr : theEndPtr;
        const Token theName(theStartPtr, thePtr - theStartPtr);
        theStartPtr = thePtr;
        if (theName == kParentDir) {
            const size_t theSize = mNormComponents.size();
            if (theSize <= 0) {
                mComponents.clear();
                break; // invalid path
            }
            if (theSize > 1 || mNormComponents[0] != kRootDir) {
                mNormComponents.pop_back();
                if (theSize == 1) {
                    mComponents.clear();
                    break; // invalid relative path
                }
            }
            mComponents.push_back(theName);
            mNormalizedFlag = false;
        } else if (theName != kThisDir) {
            mComponents.push_back(theName);
            mNormComponents.push_back(theName);
        }
    }
    if (mComponents.empty()) {
        mDirFlag        = false;
        mNormalizedFlag = false;
        return false;
    }
    mDirFlag = inLength > 0 && theEndPtr[-1] == '/';
    return true;
}

string
Path::ToString(const Path::Components& inComponents) const
{
    string result;
    for (iterator theIt = inComponents.begin();
            theIt != inComponents.end();
            theIt++) {
        if (theIt > inComponents.begin() + 1 ||
                (theIt == inComponents.begin() + 1 &&
                (inComponents[0].mLen != 1 || inComponents[0].mPtr[0] != '/'))) {
            result += "/";
        }
        result.append(theIt->mPtr, theIt->mLen);
    }
    return result;
}

}}
