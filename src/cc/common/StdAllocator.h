//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2011/08/09
// Author: Mike Ovsainnikov
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
// Stl pool allocators. Boost fast allocator has a bug with 0 size allocation.
// To paper over this bug return pointer to an empty sting, and do not invoke
// de-allocation with 0 element count.
// If available use GNU pool allocator, instead of boost pool allocator.
// GNU allocator can be turned off at run time by setting environment variable
// GLIBCXX_FORCE_NEW=1 to check for memory leaks with valgrind and such.
//
//----------------------------------------------------------------------------

#ifndef STD_ALLOCATOR_H
#define STD_ALLOCATOR_H

#if ! defined(__GNUC__) || (__GNUC__ < 3 || \
        (__GNUC__ == 3 && __GNUC_MINOR__ < 4)) || \
        (defined(__clang_major__) && 5 <= __clang_major__)
#   include "boost/pool/pool_alloc.hpp"
#   define KFS_STD_POOL_ALLOCATOR_T      boost::pool_allocator
#   define KFS_STD_FAST_POOL_ALLOCATOR_T boost::fast_pool_allocator
#else
#   include <ext/pool_allocator.h>
#   define KFS_STD_POOL_ALLOCATOR_T      __gnu_cxx::__pool_alloc
#   define KFS_STD_FAST_POOL_ALLOCATOR_T __gnu_cxx::__pool_alloc
#endif

namespace KFS
{

template <
    typename T,
    typename ALLOCATOR = KFS_STD_POOL_ALLOCATOR_T<T>
>
class StdAllocator : public ALLOCATOR::template rebind<T>::other
{
private:
    typedef typename ALLOCATOR::template rebind<T>::other MySuper;
public:
    typedef typename MySuper::pointer   pointer;
    typedef typename MySuper::size_type size_type;

    pointer allocate(
        size_type inCount)
    {
        return (inCount == 0 ? (pointer)"" : MySuper::allocate(inCount));
    }
    void deallocate(
        pointer   inPtr,
        size_type inCount)
    {
        if (inCount != 0) {
            MySuper::deallocate(inPtr, inCount);
        }
    }
    template <typename U>
    struct rebind
    {
        typedef StdAllocator<U, ALLOCATOR> other;
    };
    StdAllocator()
        : MySuper()
        {}
    StdAllocator(
        const StdAllocator& inAlloc)
        : MySuper(inAlloc)
        {}
    template<typename Tp, typename Ap>
    StdAllocator(
        const StdAllocator<Tp, Ap>& inAlloc)
        : MySuper(inAlloc)
        {}
    ~StdAllocator()
        {}
};

template <
    typename T,
    typename ALLOCATOR = KFS_STD_FAST_POOL_ALLOCATOR_T<T>
>
class StdFastAllocator : public StdAllocator<T, ALLOCATOR>
{
private:
    typedef StdAllocator<T, ALLOCATOR> MySuper;
public:
    template <typename U>
    struct rebind
    {
        typedef StdFastAllocator<U, ALLOCATOR> other;
    };
    StdFastAllocator()
        : MySuper()
        {}
    StdFastAllocator(
        const StdFastAllocator& inAlloc)
        : MySuper(inAlloc)
        {}
    template<typename Tp, typename Ap>
    StdFastAllocator(
        const StdFastAllocator<Tp, Ap>& inAlloc)
        : MySuper(inAlloc)
        {}
    ~StdFastAllocator()
        {}
};

} // namespace KFS

#endif /* STD_ALLOCATOR_H */
