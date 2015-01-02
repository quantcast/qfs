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

namespace KFS
{

template<typename T>
class ReqOstreamT
{
public:
    typedef ReqOstreamT<T> ST;
    ReqOstreamT(T& inStream)
        : mStream(inStream)
        { mBuf[kBufSize - 1] = 0; }
    ST& operator<<(signed short int inVal) { return InsertSignedInt(inVal); }
    ST& operator<<(signed int       inVal) { return InsertSignedInt(inVal); }
    ST& operator<<(signed long      inVal) { return InsertSignedInt(inVal); }
    ST& operator<<(signed long long inVal) { return InsertSignedInt(inVal); }
    template<typename VT>
    ST& operator<<(const VT& inVal) { mStream << inVal; return *this; }
    ST& flush() { mStream.flush(); return *this; }
    template<typename CT>
    ST& put(CT inChar) { mStream.put(inChar); return *this; }
    template<typename CT, typename SS>
    ST& write(const CT* inPtr, SS inCount)
        { mStream.write(inPtr, inCount); return *this; }
    T& Get() const { return mStream; }
private:
    enum { kBufSize = 2 + 1 + sizeof(long long) * 2 + 1 };
    T&   mStream;
    char mBuf[kBufSize];

    template<typename VT>
    ST& InsertSignedInt(VT inVal)
    {
        typename T::fmtflags const theFlags = mStream.flags();
        if ((theFlags & T::hex) != 0) {
            char*             thePtr     = mBuf + kBufSize - 1;
            const char* const theHexPtr  = (theFlags & T::uppercase) != 0 ?
                "0123456789ABCDEF" : "0123456789abcdef";
            const bool        theNegFlag = inVal < 0;
            if (theNegFlag) {
                inVal = -inVal;
            } else if (inVal == 0) {
                *--thePtr = '0';
            }
            while (0 < inVal) {
                *--thePtr = theHexPtr[inVal & 0xF];
                inVal >>= 4;
            }
            if (theFlags & T::showbase) {
                *--thePtr = 'x';
                *--thePtr = '0';
            }
            if (theNegFlag) {
                *--thePtr = '-';
            }
            mStream << thePtr;
            return *this;
        }
        mStream << inVal;
        return *this;
    }
};

}

#endif /* KFS_COMMON_REQ_OSTREAM_H */
