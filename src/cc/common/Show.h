//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/9/8
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
// Invoke object's Show() method with ostream.
//
//----------------------------------------------------------------------------

#ifndef COMMON_SHOW_H
#define COMMON_SHOW_H

#include <ostream>

namespace KFS
{
using std::ostream;

template<typename ObjT, typename StreamT>
class ShowObjectT
{
public:
    ShowObjectT(
        const ObjT& inTarget)
        : mTarget(inTarget)
        {}
    StreamT& Show(
        StreamT& inStream) const
        { return mTarget.Show(inStream); }
private:
    const ObjT& mTarget;
};

template<typename ObjT, typename StreamT>
    static inline StreamT&
operator << (
    StreamT&                       inStream,
    const ShowObjectT<ObjT, StreamT>& inShow)
{ return inShow.Show(inStream); }

template<typename ObjT, typename StreamT>
    static inline ShowObjectT<ObjT, StreamT>
Show(const ObjT& inObj, StreamT* /* inNullArgPtr */)
{ return ShowObjectT<ObjT, StreamT>(inObj); }

template<typename ObjT>
    static inline ShowObjectT<ObjT, ostream>
Show(const ObjT& inObj)
{ return ShowObjectT<ObjT, ostream>(inObj); }

} // namespace KFS

#endif /* COMMON_SHOW_H */
