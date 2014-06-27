//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2011/10/11
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
// \file MemLock.cc
// \brief Process common memory locking code.
//
//----------------------------------------------------------------------------

#include "MemLock.h"
#include "StdAllocator.h"

#include "qcdio/QCUtils.h"

#include <sys/mman.h>
#include <sys/resource.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include <algorithm>

#ifdef KFS_OS_NAME_LINUX
#   include <malloc.h>
#endif

namespace KFS
{
using std::string;
using std::min;
using std::max;

class MallocSetup
{
public:
    MallocSetup(
        int64_t      inSize,
        int64_t      inMaxStlPoolSize,
        const char** inErrMsgPtr = 0)
        : mAllocPtr(0),
          mStdPtr(0),
          mAllocator()
    {
#ifdef KFS_OS_NAME_LINUX
        if (! mallopt(M_TRIM_THRESHOLD, -1)) {
            if (inErrMsgPtr) {
                *inErrMsgPtr = "mallopt trim_threshold:";
            }
            return;
        }
#endif
        if (inMaxStlPoolSize > 0) {
            const int64_t theAllocCnt = min(
                int64_t(10) << 20,
                inMaxStlPoolSize / kStdMaxAllocSize
            ) + 1;
            mStdPtr = new StdAlloc::pointer[theAllocCnt];
            int64_t i;
            for (i = 0; i < theAllocCnt - 1; i++) {
                if (! (mStdPtr[i] = mAllocator.allocate(kStdMaxAllocSize))) {
                    return;
                }
            }
            mStdPtr[i] = 0;
        }
        // Force sbrk / heap allocation.
        mAllocPtr = new ForcePageAlloc[
            max(int64_t(0), inSize / ForcePageAlloc::kBufSize)
        ];
    }
    ~MallocSetup()
    {
        if (mStdPtr) {
            StdAlloc::pointer* thePtr = mStdPtr;
            while (*thePtr) {
                mAllocator.deallocate(*thePtr++, kStdMaxAllocSize);
            }
            delete [] mStdPtr;
        }
        delete [] mAllocPtr;

    }
    bool IsError()
        { return (! mAllocPtr); }
private:
    enum { kStdMaxAllocSize = 256 };
    struct ForcePageAlloc
    {
        enum { kPageSize            = 4  << 10 };
        enum { kMallocMmapThreshold = 64 << 10 }; // Default is 128K
        enum { kBufSize             = kMallocMmapThreshold - kPageSize };
        ForcePageAlloc()
            : mBufPtr(new char[kBufSize])
        {
            char*       thePtr    = mBufPtr;
            char* const theEndPtr = thePtr + kBufSize;
            while (thePtr < theEndPtr) {
                *thePtr = 0xFF;
                thePtr += kPageSize;
            }
        }
        ~ForcePageAlloc()
            { delete [] mBufPtr; }
        char* const mBufPtr;
    };
    typedef StdAllocator<char> StdAlloc;

    ForcePageAlloc*    mAllocPtr;
    StdAlloc::pointer* mStdPtr;
    StdAlloc           mAllocator;

    MallocSetup(
        const MallocSetup&);
    MallocSetup& operator=(
        const MallocSetup&);
};

static void
AllocThreadStack()
{
    char theBuf[256 << 10];
    memset(theBuf, 0xFF, sizeof(theBuf));
}

int
LockProcessMemory(
    int64_t inMaxLockedMemorySize,
    int64_t inMaxHeapSize,
    int64_t inMaxStlPoolSize,
    string* outErrMsgPtr)
{
    if (inMaxLockedMemorySize == 0) {
        return 0;
    }
    int         theRet    = 0;
#ifndef KFS_OS_NAME_CYGWIN
    const char* theMsgPtr = 0;
    if (inMaxLockedMemorySize == 0 && munlockall()) {
        theRet    = errno;
        theMsgPtr = "munlockall:";
    } else {
        const int64_t kPgAlign = 8 <<10;
        struct rlimit theLim   = {0};
        if (getrlimit(RLIMIT_MEMLOCK, &theLim)) {
            theRet    = errno;
            theMsgPtr = "getrlimit memlock:";
        } else {
            theLim.rlim_cur = (rlim_t)((inMaxLockedMemorySize + kPgAlign - 1) /
                kPgAlign * kPgAlign);
            if (theLim.rlim_max < theLim.rlim_cur) {
                // An attempt to raise it should succeed with enough privileges.
                theLim.rlim_max = theLim.rlim_cur;
            }
            if (setrlimit(RLIMIT_MEMLOCK, &theLim)) {
                theRet    = errno;
                theMsgPtr = "setrlimit memlock:";
            } else {
                AllocThreadStack();
                // Try to grow the heap.
                errno = 0;
                MallocSetup mallocSetup(
                    inMaxHeapSize, inMaxStlPoolSize, &theMsgPtr);
                if (mallocSetup.IsError()) {
                    theRet = errno;
                    if (theRet == 0) {
                        theRet = EINVAL;
                    }
                    if (! theMsgPtr) {
                        theMsgPtr = "malloc:";
                    }
                }
                if (mlockall(MCL_CURRENT | MCL_FUTURE)) {
                    theRet    = errno;
                    theMsgPtr = "mlockall:";
                }
            }
        }
    }
    if (theRet != 0 && outErrMsgPtr) {
        *outErrMsgPtr = QCUtils::SysError(theRet, theMsgPtr);
    }
#else
    AllocThreadStack();
#endif
    return theRet;
}

} // namespace KFS
