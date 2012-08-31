//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2011/02/22
// Author: Mike Ovsiannikov
//
// Copyright 2011 Quantcast Corp.
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
// Memory pool of TItemSize fixed size blocks. Larger blocks are allocated
// starting from TMinStorageAlloc, and the size of the allocated block doubles
// when the pool grows until the size reaches TMaxStorageAlloc. The allocated
// space isn't accessed (added to the free list and such) until it is really
// needed to minimize cache / tlb misses with locked memory, and defer the
// actual memory allocation by the os without locked memory. Deallocate adds
// block to the free list. The free list is LIFO to reduce dram cache and tlb
// misses. If free list is empty then a new "next size" large block is
// allocated. Allocated blocks are never released back, until the pool
// destroyed. If TForceCleanupFlag set to false, and in use count is greater
// than 0 then all allocated blocks are "leaked". If element is larger or
// equal to the pointer size, then the allocation has no overhead.
// Suitable for allocating very large number of small elements.
//
//----------------------------------------------------------------------------

#ifndef POOL_ALLOCATOR_H
#define POOL_ALLOCATOR_H

#include <stddef.h>
#include <assert.h>
#include <string.h>

#include <algorithm>

namespace KFS
{

using std::max;
using std::min;

template<
    size_t TItemSize,
    size_t TMinStorageAlloc,
    size_t TMaxStorageAlloc,
    bool   TForceCleanupFlag
>
class PoolAllocator
{
public:
    PoolAllocator()
        : mFreeStoragePtr(0),
          mFreeStorageEndPtr(0),
          mStorageListPtr(0),
          mFreeListPtr(0),
          mAllocSize(max(TMinStorageAlloc, GetElemSize())),
          mStorageSize(0),
          mInUseCount(0)
        {}
    ~PoolAllocator()
    {
        if (! TForceCleanupFlag && mInUseCount > 0) {
            return; // Memory leak
        }
        while (mStorageListPtr) {
            char* const theCurPtr = mStorageListPtr;
            char** thePtr = reinterpret_cast<char**>(theCurPtr);
            mStorageListPtr = *thePtr++;
            assert(*thePtr == theCurPtr);
            delete [] *thePtr;
        }
    }
    char* Allocate()
    {
        if (mFreeListPtr) {
            mInUseCount++;
            return GetNextFree();
        }
        char* theEndPtr = mFreeStoragePtr + GetElemSize();
        if (theEndPtr > mFreeStorageEndPtr) {
            // Maintain 2 * sizeof(size_t) alignment.
            const size_t theHdrSize = 2 * sizeof(mStorageListPtr);
            const size_t theSize    = mAllocSize + 2 * sizeof(mStorageListPtr);
            mFreeStoragePtr    = new char[theSize];
            mFreeStorageEndPtr = mFreeStoragePtr + theSize;
            char** thePtr = reinterpret_cast<char**>(mFreeStoragePtr);
            *thePtr++ = mStorageListPtr;
            *thePtr   = mFreeStoragePtr; // store ptr to catch buffer overrun.
            mStorageListPtr = mFreeStoragePtr;
            mFreeStoragePtr += theHdrSize;
            mAllocSize = min(TMaxStorageAlloc, mAllocSize << 1);
            mStorageSize += theSize;
            theEndPtr = mFreeStoragePtr + GetElemSize();
        }
        char* const theRetPtr = mFreeStoragePtr;
        mFreeStoragePtr = theEndPtr;
        mInUseCount++;
        return theRetPtr;
    }
    void Deallocate(
        void* inPtr)
    {
        if (! inPtr) {
            return;
        }
        assert(mInUseCount > 0);
        mInUseCount--;
        Put(inPtr);
    }
    size_t GetInUseCount() const
        { return mInUseCount; }
    size_t GetStorageSize() const
        { return mStorageSize; }
    static size_t GetItemSize()
        { return TItemSize; }
    static size_t GetElemSize()
        { return max(TItemSize, sizeof(char*)); }
private:
    char*  mFreeStoragePtr;
    char*  mFreeStorageEndPtr;
    char*  mStorageListPtr;
    char*  mFreeListPtr;
    size_t mAllocSize;
    size_t mStorageSize;
    size_t mInUseCount;

    char* GetNextFree()
    {
        char* const theRetPtr = mFreeListPtr;
        if (GetElemSize() % sizeof(mFreeListPtr) == 0) {
            mFreeListPtr = *reinterpret_cast<char**>(theRetPtr);
        } else {
            memcpy(&mFreeListPtr, theRetPtr, sizeof(mFreeListPtr));
        }
        return theRetPtr;
    }
    void Put(void* inPtr)
    {
        if (GetElemSize() % sizeof(mFreeListPtr) == 0) {
            *reinterpret_cast<char**>(inPtr) = mFreeListPtr;
        } else {
            memcpy(inPtr, &mFreeListPtr, sizeof(mFreeListPtr));
        }
        mFreeListPtr = reinterpret_cast<char*>(inPtr);
    }

    PoolAllocator( PoolAllocator& inAlloc);
    PoolAllocator& operator=( PoolAllocator& inAlloc);
};

}

#endif /* POOL_ALLOCATOR_H */
