//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/02/10
// Author: Mike Ovsiannikov
//
// Copyright 2015 Quantcast Corp.
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
// Character buffer istream adapter.
//
//----------------------------------------------------------------------------

#ifndef COMMON_BUFFER_INPUT_STREAM_H
#define COMMON_BUFFER_INPUT_STREAM_H

#include <streambuf>
#include <istream>

namespace KFS
{
using std::streambuf;
using std::istream;

// Multiple inheritance below used only to enforce construction order.
class BufferInputStream :
    private streambuf,
    public  istream
{
public:
    BufferInputStream(
        const char* inPtr = 0,
        size_t      inLen = 0)
        : streambuf(),
          istream(this)
    {
        char* const thePtr = const_cast<char*>(inPtr);
        streambuf::setg(thePtr, thePtr, thePtr + inLen);
    }
    istream& Set(
        const char* inPtr,
        size_t      inLen)
    {
        istream::clear();
        istream::flags(istream::dec | istream::skipws);
        istream::precision(6);
        char* const thePtr = const_cast<char*>(inPtr);
        streambuf::setg(thePtr, thePtr, thePtr + inLen);
        rdbuf(this);
        return *this;
    }
    void Reset()
        { Set(0, 0); }
};

} // namespace KFS

#endif /* COMMON_BUFFER_INPUT_STREAM_H */
