//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2017/04/29
// Author: Mike Ovsiannikov
//
// Copyright 2017 Quantcast Corporation. All rights reserved.
//
// This file is part of Quantcast File System (QFS).
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
// Display data using "C" notation for all "non printable" characters.
//
//----------------------------------------------------------------------------

#ifndef KFS_COMMON_DISPLAY_DATA_H
#define KFS_COMMON_DISPLAY_DATA_H

#include <stddef.h>

namespace KFS
{

class DisplayData
{
public:
    DisplayData(
        const char* inDataPtr,
        size_t      inLength)
        : mDataPtr(inDataPtr),
          mLength(inLength)
        {}
    template<typename ST>
    ST& Display(
        ST& inStream) const
    {
        const char* const kHexDigits = "0123456789ABCDEF";
        for (const char* thePtr = mDataPtr,
                    * const theEndPtr = thePtr + mLength;
                thePtr < theEndPtr && inStream;
                thePtr++) {
            const int theSym = *thePtr & 0xFF;
            if (theSym == '\n') {
                inStream << "\\n";
            } else if (theSym == '\r') {
                inStream << "\\r";
            } else if (' ' <= theSym && theSym < 127) {
                inStream << *thePtr;
            } else {
                inStream << "\\x" <<
                    kHexDigits[(theSym >> 4) & 0xF] <<
                    kHexDigits[theSym & 0xF]
                ;
            }
        }
        return inStream;
    }
private:
    const char* const mDataPtr;
    size_t      const mLength;
};

template<typename ST>
ST& operator<<(
    ST&                inStream,
    const DisplayData& inDisplay)
{ return inDisplay.Display(inStream); }

} // namespace KFS

#endif /* KFS_COMMON_DISPLAY_DATA_H */
