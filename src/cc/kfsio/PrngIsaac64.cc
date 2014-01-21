//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2014/01/18
// Author: Mike Ovsiannikov
//
// Copyright 2014 Quantcast Corp.
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
// \brief isaac64 pseudo random number generator wrapper.
//
//----------------------------------------------------------------------------

#include "PrngIsaac64.h"

#include <openssl/rand.h>
#include <stdlib.h>

namespace KFS
{

class PrngIsaac64::Impl
{
private:
    private:
    typedef uint64_t ub8;
    typedef uint8_t  ub1;
    typedef int      word;
#include "isaac64.c"

    bool mInitFlag;
public:
    Impl()
        : aa(0),
          bb(0),
          cc(0),
          mInitFlag(true)
    {
        for (int i=0; i<RANDSIZ; ++i) {
            mm[i]=(ub8)0;
        }
    }
    bool Init()
    {
        if (RAND_bytes(
                reinterpret_cast<unsigned char*>(randrsl),
                sizeof(randrsl)) == 0) {
            return false;
        }
        mInitFlag = false;
        randinit(1);
        return true;
    }
    void Isaac64()
    {
        if (mInitFlag) {
            if (! Init()) {
                abort();
            }
        } else {
            isaac64();
        }
    }
    const uint64_t* GetBuf()
        { return randrsl; }
    const uint64_t* GetBufEnd()
        { return (randrsl + RANDSIZ); }
};

PrngIsaac64::PrngIsaac64()
    : mImpl(*(new Impl())),
      mEndPtr(mImpl.GetBufEnd()),
      mPtr(mEndPtr)
{
}

PrngIsaac64::~PrngIsaac64()
{
    delete &mImpl;
}

bool
PrngIsaac64::Init()
{
    const bool theRet = mImpl.Init();
    if (theRet) {
        mPtr = mImpl.GetBuf();
    }
    return theRet;
}

void
PrngIsaac64::Isaac64()
{
    mImpl.Isaac64();
    mPtr = mImpl.GetBuf();
}

} // namespace KFS
