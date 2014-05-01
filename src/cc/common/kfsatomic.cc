//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2011/05/15
// Author: Mike Ovsiannikov
//
// Copyright 2011-2012 Quantcast Corp.
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

#include "kfsatomic.h"

#include <pthread.h>
#include <stdlib.h>

#ifndef _KFS_ATOMIC_USE_MUTEX
#   ifndef __GCC_HAVE_SYNC_COMPARE_AND_SWAP_8
#       define _KFS_ATOMIC_GCC_IMPLEMENT_ATOMIC_8_BYTE_OPS
#   endif
#   ifndef __GCC_HAVE_SYNC_COMPARE_AND_SWAP_4
#       define _KFS_ATOMIC_GCC_IMPLEMENT_ATOMIC_4_BYTE_OPS
#   endif
#endif

namespace KFS
{

#if defined(_KFS_ATOMIC_USE_MUTEX) || \
    defined(_KFS_ATOMIC_GCC_IMPLEMENT_ATOMIC_4_BYTE_OPS) || \
    defined(_KFS_ATOMIC_GCC_IMPLEMENT_ATOMIC_8_BYTE_OPS)

static pthread_mutex_t sKfsAtomicMutex = PTHREAD_MUTEX_INITIALIZER;

inline static void KfsAtomicLockImpl()
{
    const int err = pthread_mutex_lock(&sKfsAtomicMutex);
    if (err) {
        abort();
    }
}

inline static void KfsAtomicUnlockImpl()
{
    const int err = pthread_mutex_unlock(&sKfsAtomicMutex);
    if (err) {
        abort();
    }
}

#endif

#ifdef _KFS_ATOMIC_USE_MUTEX
namespace atomicmpl
{
void AtomicLock()   { KfsAtomicLockImpl();  }
void AtomicUnlock() { KfsAtomicUnlockImpl(); }
}
#endif /* _KFS_ATOMIC_USE_MUTEX */

#if defined(_KFS_ATOMIC_GCC_IMPLEMENT_ATOMIC_4_BYTE_OPS) || \
        defined(_KFS_ATOMIC_GCC_IMPLEMENT_ATOMIC_8_BYTE_OPS)
template<typename T>
    inline static T
SyncAddAndFetchT(
    volatile void* inValPtr,
    T              inInc)
{
    KfsAtomicLockImpl();
    T theRet = *reinterpret_cast<volatile T*>(inValPtr);
    theRet += inInc;
    *reinterpret_cast<volatile T*>(inValPtr) = theRet;
    KfsAtomicUnlockImpl();
    return theRet;
}
#endif

#ifdef _KFS_ATOMIC_GCC_IMPLEMENT_ATOMIC_4_BYTE_OPS
extern "C"
{
unsigned int __sync_add_and_fetch_4(
    volatile void* val, unsigned int inc)
{
    return SyncAddAndFetchT(val, inc);
}
}
#endif /* _KFS_ATOMIC_GCC_IMPLEMENT_ATOMIC_4_BYTE_OPS */

#ifdef _KFS_ATOMIC_GCC_IMPLEMENT_ATOMIC_8_BYTE_OPS
extern "C"
{
long long unsigned int __sync_add_and_fetch_8(
    volatile void* val, long long unsigned int inc)
{
    return SyncAddAndFetchT(val, inc);
}
}
#endif /* _KFS_ATOMIC_GCC_IMPLEMENT_ATOMIC_8_BYTE_OPS */

} // namespace KFS
