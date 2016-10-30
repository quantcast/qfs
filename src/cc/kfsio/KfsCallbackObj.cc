//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/04/19
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

#include "KfsCallbackObj.h"

#include <stdlib.h>

namespace KFS
{

inline static const ObjectMethodBase*
MakeNullObjMethod()
{
    static const ObjectMethod<KfsCallbackObj> sObjMethod(0, 0);
    return &sObjMethod;
}

const ObjectMethodBase* const kNullObjMethod = MakeNullObjMethod();

/* virtual */
KfsCallbackObj::~KfsCallbackObj()
{
    if (mObjMeth == kNullObjMethod) {
        abort(); // Catch double delete.
        return;
    }
    if (mObjMeth) {
        mObjMeth->~ObjectMethodBase();
    }
    mObjMeth = kNullObjMethod;
}

}
