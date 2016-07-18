//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2010/05/14
// Author: Kate Labeeva, Mike Ovsiannikov
//
// Copyright 2010-2012 Quantcast Corp.
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
// Class for RPC request formatting. Presently only negative hex representation
// is different from the representation produced by ostream.
//
//----------------------------------------------------------------------------

#ifndef KFS_COMMON_REQ_OSTREAM_H
#define KFS_COMMON_REQ_OSTREAM_H

#include "IntToString.h"

namespace KFS
{

struct ServerLocation;
class CIdChecksum;
class MetaVrLogSeq;

template<typename T>
class ReqOstreamT
{
public:
    typedef ReqOstreamT<T> ST;
    ReqOstreamT(T& inStream)
        : mStream(inStream)
        { mBuf[kBufSize] = 0; }
    ST& operator<<(signed short int   inVal) { return InsertInt(inVal); }
    ST& operator<<(signed int         inVal) { return InsertInt(inVal); }
    ST& operator<<(signed long        inVal) { return InsertInt(inVal); }
    ST& operator<<(signed long long   inVal) { return InsertInt(inVal); }
    ST& operator<<(unsigned short int inVal) { return InsertInt(inVal); }
    ST& operator<<(unsigned int       inVal) { return InsertInt(inVal); }
    ST& operator<<(unsigned long      inVal) { return InsertInt(inVal); }
    ST& operator<<(unsigned long long inVal) { return InsertInt(inVal); }
    ST& operator<<(const ServerLocation& inLoc)
        { mStream << inLoc; return *this; }
    ST& operator<<(const CIdChecksum& inChecksum)
        { mStream << inChecksum; return *this; }
    ST& operator<<(const MetaVrLogSeq& inChecksum)
        { mStream << inChecksum; return *this; }
    template<typename VT>
    ST& operator<<(const VT& inVal) { mStream << inVal; return *this; }
    ST& flush() { mStream.flush(); return *this; }
    template<typename CT>
    ST& put(CT inChar) { mStream.put(inChar); return *this; }
    template<typename CT, typename SST>
    ST& write(const CT* inPtr, SST inCount)
        { mStream.write(inPtr, inCount); return *this; }
    T& Get() const { return mStream; }
private:
    enum { kBufSize = 2 + 1 + sizeof(long long) * 2 };
    T&   mStream;
    char mBuf[kBufSize + 1];

    template<typename VT>
    ST& InsertInt(VT inVal)
    {
        typename T::fmtflags const theFlags = mStream.flags();
        if ((theFlags & T::hex) == 0) {
            mStream << inVal;
            return *this;
        }
        char* const thePtr = IntToString<16>::Convert(
            inVal,
            mBuf + kBufSize,
            (theFlags & T::showbase) ? "x0" : "",
            (theFlags & T::uppercase) == 0
        );
        const long theSize = (long)(mBuf + kBufSize - thePtr);
        if ((long)mStream.width() <= theSize) {
            mStream.write(thePtr, theSize);
            mStream.width(0);
        } else {
            mStream << thePtr;
        }
        return *this;
    }
};

}

#endif /* KFS_COMMON_REQ_OSTREAM_H */
