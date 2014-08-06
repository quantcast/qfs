//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2014/08/04
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
// C++ ios stream library related "helper" classes and methods.
//
//----------------------------------------------------------------------------

#ifndef KFS_COMMON_IOSUTILS_H
#define KFS_COMMON_IOSUTILS_H

#include <iosfwd>

namespace KFS
{

template<typename ST, typename T, typename MT>
class DisplayWithObjMethodT
{
public:
    DisplayWithObjMethodT(
        T&   inTarget,
        MT   inMethod,
        bool inDisplayFlag = true)
        : mTarget(inTarget),
          mMethod(inMethod),
          mDisplayFlag(inDisplayFlag)
        {}
    ST& Display(
        ST& inStream) const
        { return (mDisplayFlag ? (mTarget.*mMethod)(inStream) : inStream); }
private:
    T&         mTarget;
    MT const   mMethod;
    bool const mDisplayFlag;
};

template<typename ST, typename T, typename MT>
static inline ST& operator<<(
    ST&                                     inStream,
    const DisplayWithObjMethodT<ST, T, MT>& inDisplay)
    { return inDisplay.Display(inStream); }

template<typename ST, typename T, typename MT>
DisplayWithObjMethodT<ST, T, MT> StreamDisplay(
    T&   inTarget,
    MT   inMethod,
    bool inDisplayFlag = true,
    ST*  inStreamPtr   = 0)
{
    return DisplayWithObjMethodT<ST, T, MT>(
        inTarget, inMethod, inDisplayFlag);
}

template<typename T, typename MT>
DisplayWithObjMethodT<std::ostream, T, MT> OsDisplay(
    T&   inTarget,
    MT   inMethod,
    bool inDisplayFlag = true)
{
    return StreamDisplay<std::ostream, T, MT>(
        inTarget, inMethod, inDisplayFlag);
}

// Example: cout << OsDisplay(*this, &Obj::Method);

} // namespace KFS

#endif /* KFS_COMMON_IOSUTILS_H */
