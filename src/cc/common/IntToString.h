//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/10/30
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
// \file IntToString.h
// \brief Integer radix conversion.
//        Hex signed integer conversion is of particular interest. Unlike
//        "standard" hex conversion it emits minus sign for the negative
//        integers. This allows to use istream extract operator (>>) and
//        produces correct result with "extraction" into "lager" (for example
//        extraction into 64 bit integer from 32 bit integer) integer types.
//
//----------------------------------------------------------------------------

#ifndef KFS_INTTOSTRING_H
#define KFS_INTTOSTRING_H

namespace KFS {

template <int TRadix>
class IntToString
{
public:
    template<typename T>
    static char* Convert(
        T     inVal,
        char* inBufEndPtr)
    {
        return ((T(1) << (sizeof(T) * 8 - 1)) < 0 ?
            ConvertSigned(inVal, inBufEndPtr) :
            Impl<T>::Convert(inVal, inBufEndPtr));
    }
private:
    template <typename T>
    class Impl
    {
    public:
        static char* Convert(
            T     inVal,
            char* inBufEndPtr)
        {
            return (TRadix == 16 ?
                Hex(inVal, inBufEndPtr) :
                Other(inVal, inBufEndPtr));
        }
    private:
        static char* Hex(
            T     inVal,
            char* inBufEndPtr)
        {
            char* thePtr = inBufEndPtr;
            do {
                *--thePtr = "0123456789ABCDEF"[inVal & 0xF];
                inVal >>= 4;
            } while (inVal != 0);
            return thePtr;
        }
        static char* Other(
            T     inVal,
            char* inBufEndPtr)
        {
            char* thePtr = inBufEndPtr;
            do {
                *--thePtr = (char)((inVal % TRadix) + '0');
                inVal /= TRadix;
            } while (inVal != 0);
            return thePtr;
        }
    };
    template <typename T>
    static char* ConvertSigned(
        T     inVal,
        char* inBufEndPtr)
    {
        char*      thePtr     = inBufEndPtr;
        const T    kMinInt    = T(1) << (sizeof(T) * 8 - 1);
        const bool theNegFlag = (inVal & kMinInt) != 0; // < 0 generates warning
        if (theNegFlag) {
            // (inVal = -inVal) < 0 doesn't work with gcc optimization
            if (inVal == kMinInt) {
                thePtr = Impl<T>::Convert(-(kMinInt % TRadix), thePtr);
                inVal = -(kMinInt / TRadix);
            } else {
                inVal = -inVal;
            }
        }
        thePtr = Impl<T>::Convert(inVal, thePtr);
        if (theNegFlag) {
            *--thePtr = '-';
        }
        return thePtr;
    }
};

template<typename T>
    static inline char*
IntToDecString(
    T     inVal,
    char* inBufEndPtr)
{
    return IntToString<10>::Convert(inVal, inBufEndPtr);
}

template<typename T>
    static inline char*
IntToHexString(
    T     inVal,
    char* inBufEndPtr)
{
    return IntToString<16>::Convert(inVal, inBufEndPtr);
}

template<typename T>
    static inline char*
IntToOctString(
    T     inVal,
    char* inBufEndPtr)
{
    return IntToString<8>::Convert(inVal, inBufEndPtr);
}

template<typename T, int TRadix>
class DisplayInt
{
public:
    DisplayInt(
        T inVal)
        : mPtr(IntToString<TRadix>::Convert(
            inVal, mBuf + sizeof(mBuf) / sizeof(mBuf[0])))
        {}
    template<typename TStream>
    TStream& Display(
        TStream& inStream) const
    {
        return inStream.write(
            mPtr, mBuf + sizeof(mBuf) / sizeof(mBuf[0]) - mPtr);
    }
private:
    char        mBuf[TRadix < 8 ? sizeof(T) * 8  + 1 : 32];
    char* const mPtr;
};

template<typename TStream, typename T, int TRadix>
    inline static TStream&
operator<<(
    TStream&                     inStream,
    DisplayInt<T, TRadix> const& inDisplay)
{
    return inDisplay.Display(inStream);
}

}

#endif /* KFS_INTTOSTRING_H */
