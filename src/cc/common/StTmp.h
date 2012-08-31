//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/06/09
// Author: Mike Ovsainnikov
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
// \brief  Temporary recursion protected "variable".
//
//----------------------------------------------------------------------------

#ifndef ST_TMP_H
#define ST_TMP_H

#include <cassert>

namespace KFS
{

template<typename T> class StTmp
{
public:
    template<typename Tp> class TmpT
    {
    public:
        TmpT()
            : mTmp(),
              mInUseFlag(false)
            {}
        ~TmpT() { assert(! mInUseFlag); }
    private:
        Tp   mTmp;
        bool mInUseFlag;

        TmpT* Get()
        {
            if (mInUseFlag) {
                return 0;
            }
            mInUseFlag = true;
            return this;
        }
        void Put()
        {
            assert(mInUseFlag);
            mTmp.clear();
            mInUseFlag = false;
        }
        TmpT(const TmpT&);
        TmpT& operator=(const TmpT&);
        friend class StTmp<T>;
    };
    typedef TmpT<T> Tmp;

    StTmp(Tmp& inTmp)
        : mTmpPtr(inTmp.Get()),
          mTmp(mTmpPtr ? mTmpPtr->mTmp :
            *(new (&mTmpStorage) T()))
        {}
    ~StTmp()
    {
        if (mTmpPtr) {
            mTmpPtr->Put();
        } else {
            mTmp.~T();
        }
    }
    T& Get()
    {
        mTmp.clear();
        return mTmp;
    }
private:
    Tmp* const mTmpPtr;
    struct {
        size_t mStorage[
            (sizeof(T) + sizeof(size_t) - 1) /
            sizeof(size_t)
        ];
    }          mTmpStorage;
    T&         mTmp;

    StTmp(const StTmp&);
    StTmp& operator=(const StTmp&);
};

}

#endif /* ST_TMP_H */
