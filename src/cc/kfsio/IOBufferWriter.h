//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/10/18
// Author: Mike Ovsiannikov.
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
// \brief Optimized write code to write into (normally empty) IOBuffer.
//
//----------------------------------------------------------------------------

#ifndef KFSIO_IOBUFFER_WRITER_H
#define KFSIO_IOBUFFER_WRITER_H

#include "IOBuffer.h"
#include <string.h>

namespace KFS
{

class IOBufferWriter
{
public:
    IOBufferWriter(
        IOBuffer& inIoBuffer)
        : mIoBuffer(inIoBuffer),
          mBuffer(),
          mCurPtr(mBuffer.Producer()),
          mEndPtr(mCurPtr + mBuffer.SpaceAvailable())
        {}
    void Write(
        const char* inDataPtr,
        size_t      inLength)
    {
        const char*  theDataPtr = inDataPtr;
        size_t       theLen     = inLength;
        for (; ;) {
            if (mCurPtr + theLen <= mEndPtr) {
                memcpy(mCurPtr, theDataPtr, theLen);
                mCurPtr += theLen;
                return;
            }
            const size_t theNcp = mEndPtr - mCurPtr;
            memcpy(mCurPtr, theDataPtr, theNcp);
            theDataPtr += theNcp;
            theLen  -= theNcp;
            if (mCurPtr) {
                mBuffer.Fill((int)(mCurPtr - mBuffer.Producer() + theNcp));
                mIoBuffer.Append(mBuffer);
            }
            mBuffer = IOBufferData();
            mCurPtr = mBuffer.Producer();
            mEndPtr = mCurPtr + mBuffer.SpaceAvailable();
        }
    }
    template<typename T>
    void Write(
        const T& inBuf)
        { Write(inBuf.data(), inBuf.size()); }
    void Close()
    {
        if (! mCurPtr) {
            return;
        }
        const int theLen = (int)(mCurPtr - mBuffer.Producer());
        if (theLen > 0) {
            mBuffer.Fill(theLen);
            mIoBuffer.Append(mBuffer);
            mCurPtr = 0;
            mEndPtr = 0;
        }
    }
    // Does not include the size of the "last"/current buffer
    int GetSize() const
        { return mIoBuffer.BytesConsumable(); }
    int GetTotalSize() const
    {
        return (mIoBuffer.BytesConsumable() +
            (int)(mCurPtr - (mCurPtr ? mBuffer.Producer() : mCurPtr)));
    }
    void Clear(
        bool inClearBufferFlag = true)
    {
        if (inClearBufferFlag) {
            mIoBuffer.Clear();
        }
        if (! mCurPtr) {
            mBuffer = IOBufferData();
        }
        mCurPtr = mBuffer.Producer();
        mEndPtr = mCurPtr + mBuffer.SpaceAvailable();
    }
private:
    IOBuffer&    mIoBuffer;
    IOBufferData mBuffer;
    char*        mCurPtr;
    char*        mEndPtr;
private:
    IOBufferWriter(
        const IOBufferWriter&);
    IOBufferWriter& operator=(
        const IOBufferWriter&);
};

}

#endif /* KFSIO_IOBUFFER_WRITER_H */
