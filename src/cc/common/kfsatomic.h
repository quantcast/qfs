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
// "Atomic" variables of different sizes (well up to 64 bits on 64 bit
// platforms).
//
//----------------------------------------------------------------------------

#ifndef KFS_ATOMIC_H
#define KFS_ATOMIC_H

namespace KFS
{
#if ! defined(_KFS_ATOMIC_USE_MUTEX) && (\
        ! defined(__GNUC__) || (__GNUC__ < 4 || \
        (__GNUC__ == 4 && (__GNUC_MINOR__ < 1 || \
            (__GNUC_MINOR__ == 1 && __GNUC_PATCHLEVEL__ < 2)))))
#   define _KFS_ATOMIC_USE_MUTEX
#endif

#ifdef _KFS_ATOMIC_USE_MUTEX

namespace atomicmpl
{
void AtomicLock();
void AtomicUnlock();
}

template<typename T> T SyncAddAndFetch(volatile T& val, T inc)
{
    atomicmpl::AtomicLock();
    val += inc;
    const T ret = val;
    atomicmpl::AtomicUnlock();
    return ret;
}

#else

template<typename T> T SyncAddAndFetch(volatile T& val, T inc)
{
    return __sync_add_and_fetch(&val, inc);
}

#endif /* _KFS_ATOMIC_USE_MUTEX */
}

#endif /* KFS_ATOMIC_H */
