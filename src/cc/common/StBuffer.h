//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2010/05/14
// Author: Mike Ovsiannikov
//
// Copyright 2010 Quantcast Corp.
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

#ifndef ST_BUFFER_H
#define ST_BUFFER_H

#include <string>
#include <algorithm>
#include <ostream>

#include <string.h>
#include <stdlib.h>

namespace KFS
{
using std::string;
using std::ostream;
using std::min;
using std::copy;

// Stack based buffer. The intention is to use buffer mBuf allocated on the
// stack (or as part of other object) in most cases, and do real buffer
// allocation if the size exceeds default capacity.
template<typename T, size_t DEFAULT_CAPACITY>
class StBufferT
{
public:
    StBufferT()
        : mBufPtr(mBuf),
          mCapacity(DEFAULT_CAPACITY),
          mSize(0)
        {}
    StBufferT(
        const T* inPtr,
        size_t   inLen)
        : mBufPtr(mBuf),
          mCapacity(DEFAULT_CAPACITY),
          mSize(0)
        { Copy(inPtr, inLen); }
    StBufferT(
        const StBufferT& inBuf)
        : mBufPtr(mBuf),
          mCapacity(DEFAULT_CAPACITY),
          mSize(0)
        { Copy(inBuf, inBuf.GetSize()); }
    template<size_t CAPACITY>
    StBufferT(
        const StBufferT<T, CAPACITY>& inBuf)
        : mBufPtr(mBuf),
          mCapacity(DEFAULT_CAPACITY),
          mSize(0)
        { Copy(inBuf, inBuf.GetSize()); }
    template<size_t CAPACITY>
    StBufferT& operator=(
        const StBufferT<T, CAPACITY>& inBuf)
        { return Copy(inBuf, inBuf.GetSize()); }
    ~StBufferT()
    {
        if (mBufPtr != mBuf) {
            delete [] mBufPtr;
        }
    }
    size_t Capacity() const
        { return mCapacity; }
    size_t GetSize() const
        { return mSize; }
    T* Resize(
        size_t inSize)
    {
        EnsureCapacity(inSize);
        mSize = inSize;
        return mBufPtr;
    }
    T* GetPtr()
        { return mBufPtr; }
    const T* GetPtr() const
        { return mBufPtr; }
    template<size_t CAPACITY>
    StBufferT& Copy(
        const StBufferT<T, CAPACITY>& inBuf,
        size_t                        inLen)
    {
        mSize = 0;
        copy(
            inBuf.GetPtr(),
            inBuf.GetPtr() + min(inBuf.GetSize(), inLen),
            Resize(inBuf.GetSize())
        );
        return *this;
    }
    StBufferT& Copy(
        const T* inPtr,
        size_t   inLen)
    {
        mSize = 0;
        copy(inPtr, inPtr + inLen, Resize(inLen));
        return *this;
    }
    StBufferT& Append(
        const T& inVal)
    {
        Resize(mSize + 1);
        mBufPtr[mSize - 1] = inVal;
        return *this;
    }
protected:
    T*     mBufPtr;
    size_t mCapacity;
    size_t mSize;
    T      mBuf[DEFAULT_CAPACITY];

    T* EnsureCapacity(
        size_t inCapacity)
    {
        if (inCapacity <= mCapacity) {
            return mBufPtr;
        }
        T* const theBufPtr = new T[inCapacity];
        copy(mBufPtr,  mBufPtr + mSize, theBufPtr);
        if (mBufPtr != mBuf) {
            delete [] mBufPtr;
        }
        mCapacity = inCapacity;
        mBufPtr   = theBufPtr;
        return mBufPtr;
    }
};

// String buffer, with lazy conversion to string. 
template<size_t DEFAULT_CAPACITY>
class StringBufT
{
public:
    StringBufT()
        : mStr(),
          mSize(0)
        { mBuf[mSize] = 0; }
    StringBufT(
        const char* inStr)
        : mStr(),
          mSize(-1)
        { Copy(inStr, inStr ? strlen(inStr) : 0); }
    StringBufT(
        const char* inStr,
        size_t      inLen)
        : mStr(),
          mSize(-1)
        { Copy(inStr, inLen); }
    StringBufT(
        const StringBufT& inBuf)
        : mStr(),
          mSize(-1)
        { Copy(inBuf); }
    ~StringBufT()
        {}
    template<size_t CAPACITY>
    StringBufT(
        const StringBufT<CAPACITY>& inBuf)
        : mStr(),
          mSize(-1)
        { Copy(inBuf); }
    StringBufT(
        const string& inStr)
        : mStr(inStr),
          mSize(-1)
        {}
    template<size_t CAPACITY>
    StringBufT& operator=(
        const StringBufT<CAPACITY>& inBuf)
        { return Copy(inBuf); }
    StringBufT& operator=(
        const string& inStr)
        { return Copy(inStr); }
    const char* GetPtr() const
        { return (mSize < 0 ? mStr.c_str() : mBuf); }
    size_t GetSize() const
        { return (mSize < 0 ? mStr.size() : size_t(mSize)); }
    StringBufT& Copy(
        const char* inPtr,
        size_t      inLen)
    {
        if (inLen <= DEFAULT_CAPACITY) {
            // memcpy appears slightly faster, if it isn't inlined.
            if (mBuf <= inPtr && inPtr <= mBuf + DEFAULT_CAPACITY) {
                memmove(mBuf, inPtr, inLen);
            } else {
                memcpy(mBuf, inPtr, inLen);
            }
            mSize = inLen;
            mBuf[mSize] = 0;
            mStr = string(); // Force de-allocation, clear() won't de-allocate.
        } else {
            mSize = -1;
            mStr.assign(inPtr, inLen);
        }
        return *this;
    }
    StringBufT& Copy(
        const string& inStr)
    {
        mSize = -1;
        mStr  = inStr;
        return *this;
    }
    template<size_t CAPACITY>
    StringBufT& Copy(
        const StringBufT<CAPACITY>& inBuf)
    {
        if (inBuf.mSize > 0) {
            Copy(inBuf.mBuf, inBuf.mSize);
        } else {
            mStr  = inBuf.mStr;
            mSize = inBuf.mSize;
            mBuf[0] = 0;
        }
        return *this;
    }
    string GetStr() const
    {
        if (mSize > 0) {
            string& theStr = const_cast<string&>(mStr);
            theStr.assign(mBuf, mSize);
            const_cast<int&>(mSize) = -1;
        }
        return mStr;
    }
    StringBufT& Append(
        char c)
    {
        if (mSize >= 0) {
            if ((size_t)mSize < DEFAULT_CAPACITY) {
                mBuf[++mSize] = c;
                mBuf[mSize]   = 0;
                return *this;
            }
            mStr.assign(mBuf, mSize);
            mSize = -1;
        }
        mStr += c;
        return *this;
    }
    StringBufT& Append(
        const char* inPtr,
        size_t      inLen)
    {
        if (mSize < 0) {
            mStr.append(inPtr, inLen);
            return *this;
        }
        if (mSize + inLen <= DEFAULT_CAPACITY) {
            // memcpy appears slightly faster, if it isn't inlined.
            if (mBuf <= inPtr && inPtr <= mBuf + DEFAULT_CAPACITY) {
                memmove(mBuf + mSize, inPtr, inLen);
            } else {
                memcpy(mBuf + mSize, inPtr, inLen);
            }
            mSize += inLen;
            mBuf[mSize] = 0;
            mStr = string();
        } else {
            mStr.assign(mBuf, mSize);
            mSize = -1;
            mStr.append(inPtr, inLen);
        }
        return *this;
    }
    StringBufT& Append(
        const char* inStrPtr)
        { return (inStrPtr ? Append(inStrPtr, strlen(inStrPtr)) : *this); }
    template<size_t CAPACITY>
    StringBufT& Append(
        const StringBufT<CAPACITY>& inBuf)
        { return Append(inBuf.GetPtr(), inBuf.GetSize()); }
    StringBufT& Append(
        const string& inStr)
        { return Append(inStr.data(), inStr.size()); }
    template<size_t CAPACITY>
    bool Comapre(
        const StringBufT<CAPACITY>& inBuf) const
    {
        const int theRet = memcmp(
            GetPtr(), inBuf.GetPtr(), min(GetSize(), inBuf.GetSize()));
        return (theRet == 0 ? GetSize() - inBuf.GetSize() : theRet);
    }
    template<size_t CAPACITY>
    bool operator==(
        const StringBufT<CAPACITY>& inBuf) const
    {
        return (
            GetSize() == inBuf.GetSize() &&
            memcmp(GetPtr(), inBuf.GetPtr(), GetSize()) == 0
        );
    }
    // The following two aren't necessarily the same as string.compare(),
    int Compare(
        const string& inStr) const
    {
        const int theRet = memcmp(
            GetPtr(), inStr.data(), min(GetSize(), inStr.size()));
        return (theRet == 0 ? GetSize() - inStr.size() : theRet);
    }
    bool operator==(
        const string& inStr) const
    {
        return (
            GetSize() == inStr.size() &&
            memcmp(GetPtr(), inStr.data(), GetSize()) == 0
        );
    }
    int Compare(
        const char* inStrPtr) const
        { return strcmp(GetPtr(), inStrPtr); }
    bool operator==(
        const char* inStrPtr) const
        {   return (Compare(inStrPtr) == 0); }
    template<size_t CAPACITY>
    bool operator!=(
        const StringBufT<CAPACITY>& inBuf) const
        { return !(*this == inBuf); }
    template<size_t CAPACITY>
    bool operator<(
        const StringBufT<CAPACITY>& inBuf) const
        { return (Compare(inBuf.GetPtr()) < 0); }
    bool operator<(
        const string& inStr) const
        { return (Compare(inStr) < 0); }
    const char* c_str() const
        { return GetPtr(); }
    const char* data() const
        { return GetPtr(); }
    bool empty() const
        { return (GetSize() <= 0); }
    size_t size() const
        { return GetSize(); }
    size_t length() const
        { return GetSize(); }
    void clear()
    {
        mSize = 0;
        mBuf[mSize] = 0;
        mStr = string();
    }
    //operator string () const
    //    { return GetStr(); }
private:
    string mStr;
    char   mBuf[DEFAULT_CAPACITY + 1];
    int    mSize;

    template<size_t> friend class StringBufT;
};

template<size_t DEFAULT_CAPACITY>
inline static bool operator==(
    const string&                  inStr,
    const StringBufT<DEFAULT_CAPACITY>& inBuf)
{
    return (inBuf == inStr);
}

template<size_t DEFAULT_CAPACITY>
inline static bool operator==(
    const char*                         inStrPtr,
    const StringBufT<DEFAULT_CAPACITY>& inBuf)
{
    return (inBuf == inStrPtr);
}

template<size_t DEFAULT_CAPACITY>
inline static ostream& operator<<(
    ostream&                       inStream,
    const StringBufT<DEFAULT_CAPACITY>& inBuf)
{ return inStream.write(inBuf.GetPtr(), inBuf.GetSize()); }

}

#endif /* ST_BUFFER_H */
