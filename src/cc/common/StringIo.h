//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2010/05/14
// Author: Kate Labeeva, Mike Ovsiannikov
//
// Copyright 2010-2012,2016 Quantcast Corporation. All rights reserved.
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
// Class to convert byte sequence to and from C string representation by
// replacing "special" characters into %hh notation, and back.
//
//----------------------------------------------------------------------------

#ifndef KFS_STRING_IO_H
#define KFS_STRING_IO_H

#include <string.h>

#include "RequestParser.h"

namespace KFS
{

class StringIo
{
private:
    template<typename T>
    static void Escape(
        int inSym,
        T&  inWriter)
    {
        const char* const kHexChars = "0123456789ABCDEF";
        char              theBuf[3];
        theBuf[0] = '%';
        theBuf[1] = kHexChars[(inSym >> 4) & 0xF];
        theBuf[2] = kHexChars[inSym & 0xF];
        inWriter.write(theBuf, 3);
    }
    enum { kSpace = ' ' };
    static bool IsToBeEscaped(
        int inSym)
    {
        switch (inSym)
        {
            case 0xFF:
            case '%':
            case '=':
            case ';':
            case '/':
            case ',':
                return true;
            default: break;
        }
        return (inSym < kSpace);
    }
public:
    template<typename T>
    static bool Unescape(
        const char* inPtr,
        size_t      inLen,
        T&          inStr)
    {
        inStr.clear();
        const unsigned char* const theCharToHex = HexIntParser::GetChar2Hex();
        const char*                thePtr       = inPtr;
        const char*          const theEndPtr    = thePtr + inLen;
        const char*                theEPtr;
        while ((theEPtr = (const char*)memchr(
                thePtr, '%', theEndPtr - thePtr))) {
            inStr.append(thePtr, theEPtr - thePtr);
            if (theEndPtr < theEPtr + 3) {
                return false;
            }
            const int theFirst = theCharToHex[*++theEPtr & 0xFF];
            if (0xFF == theFirst) {
                return false;
            }
            const int theSecond = theCharToHex[*++theEPtr & 0xFF];
            if (0xFF == theSecond) {
                return false;
            }
            const char theSym = (char)((theFirst << 4) | theSecond);
            inStr.append(&theSym, 1);
            thePtr = theEPtr + 1;
        }
        inStr.append(thePtr, theEndPtr - thePtr);
        return true;
    }
    template<typename T>
    static void Escape(
        const char* inPtr,
        size_t      inLen,
        T&          inWriter)
    {
        if (inLen <= 0) {
            return;
        }
        // Always escape the first leading and the last trailing spaces, if any,
        // in order to ensure leading and trailing spaces are not discarded by
        // key value tokenizer.
        const bool        theLastSpaceFlag =
            kSpace == (inPtr[inLen - 1] & 0xFF);
        const char*       thePtr           = inPtr;
        const char* const theEndPtr        = thePtr + inLen -
            (theLastSpaceFlag ? 1 : 0);
        if (thePtr < theEndPtr && kSpace == (*thePtr & 0xFF)) {
            Escape(kSpace, inWriter);
            ++thePtr;
        }
        const char* thePPtr = thePtr;
        while (thePtr < theEndPtr) {
            const int theSym = *thePtr & 0xFF;
            if (IsToBeEscaped(theSym)) {
                if (thePPtr < thePtr) {
                    inWriter.write(thePPtr, thePtr - thePPtr);
                }
                Escape(theSym, inWriter);
                thePPtr = thePtr + 1;
            }
            ++thePtr;
        }
        if (thePPtr < theEndPtr) {
            inWriter.write(thePPtr, theEndPtr - thePPtr);
        }
        if (theLastSpaceFlag) {
            Escape(kSpace, inWriter);
        }
    }
};

template<typename T>
class EscapedStringInserterT
{
public:
    EscapedStringInserterT(
        const T& inStr)
        : mStr(inStr)
        {}
    template<typename ST>
    ST& Insert(
        ST& inStream) const
    {
        StringIo::Escape(mStr.data(), mStr.size(), inStream);
        return inStream;
    }
private:
    const T& mStr;
};

template<typename T>
static inline EscapedStringInserterT<T> MakeEscapedStringInserter(
    const T& inStr)
{
    return EscapedStringInserterT<T>(inStr);
}

template<typename ST, typename T>
static inline ST& operator<<(
    ST&                       inOs,
    EscapedStringInserterT<T> inInserter)
{
    return inInserter.Insert(inOs);
}

} // namespace KFS

#endif /* KFS_STRING_IO_H */
