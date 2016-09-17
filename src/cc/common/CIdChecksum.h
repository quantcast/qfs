//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/06/16
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
// \file CIdChecksum.h
// \brief Chunks id / inventory checksum.
//
//----------------------------------------------------------------------------

#ifndef COMMON_CIDCHECKSUM_H
#define COMMON_CIDCHECKSUM_H

#include "kfstypes.h"

namespace KFS
{

class CIdChecksum
{
    // The following assumes implicitly that chunk id and version are always
    // non negative integers, therefore bit 63 can be treated as carry bit, in
    // order to avoid conditionals with long subtraction and addition, resulting
    // in 64 + 63 + 63 = 190 bit effective checksum size.
private:
    inline void AddLM(
        uint64_t inLo,
        uint64_t inMi)
    {
        mLo += inLo;
        mMi += inMi;
        mMi += (mLo >> 63) & 0x1;
        mLo &= ~((uint64_t)1 << 63);
        mHi += (mMi >> 63) & 0x1;
        mMi &= ~((uint64_t)1 << 63);
    }
    inline void RemoveLM(
        uint64_t inLo,
        uint64_t inMi)
    {
        mLo -= inLo;
        mMi -= inMi;
        mMi -= (mLo >> 63) & 0x1;
        mLo &= ~((uint64_t)1 << 63);
        mHi -= (mMi >> 63) & 0x1;
        mMi &= ~((uint64_t)1 << 63);
    }
public:
    CIdChecksum()
        : mHi(0),
          mMi(0),
          mLo(0)
        {}
    CIdChecksum& Add(
        chunkId_t inId,
        seq_t     inVersion)
    {
        AddLM((uint64_t)inVersion, (uint64_t)inId);
        return *this;
    }
    CIdChecksum& Remove(
        chunkId_t inId,
        seq_t     inVersion)
    {
        RemoveLM((uint64_t)inVersion, (uint64_t)inId);
        return *this;
    }
    CIdChecksum& Add(
        const CIdChecksum& inRhs)
    {
        AddLM(inRhs.mLo, inRhs.mMi);
        mHi += inRhs.mHi;
        return *this;
    }
    CIdChecksum& Remove(
        const CIdChecksum& inRhs)
    {
        RemoveLM(inRhs.mLo, inRhs.mMi);
        mHi -= inRhs.mHi;
        return *this;
    }
    CIdChecksum& Clear()
    {
        mHi = 0;
        mMi = 0;
        mLo = 0;
        return *this;
    }
    bool operator==(
        const CIdChecksum& inRhs) const
    {
        return (mHi == inRhs.mHi && mMi == inRhs.mMi && mLo == inRhs.mLo);
    }
    template<typename T>
    T& Display(
        T& inStream) const
    {
        inStream << mHi;
        inStream << " ";
        inStream << mMi;
        inStream << " ";
        inStream << mLo;
        return inStream;
    }
    template<typename T>
    bool Parse(
        const char*& ioPtr,
        size_t       inLen,
        T*           inParserTypePtr = 0)
    {
        const char* const theEndPtr = ioPtr + inLen;
        return (
            T::Parse(ioPtr, theEndPtr - ioPtr, mHi) &&
            T::Parse(ioPtr, theEndPtr - ioPtr, mMi) &&
            T::Parse(ioPtr, theEndPtr - ioPtr, mLo)
        );
    }
private:
    uint64_t mHi;
    uint64_t mMi;
    uint64_t mLo;
};

template<typename T>
T&
operator<<(
    T&                 inStream,
    const CIdChecksum& inCs)
{
    return inCs.Display(inStream);
}

} // namespace KFS

#endif /* COMMON_CIDCHECKSUM_H */
