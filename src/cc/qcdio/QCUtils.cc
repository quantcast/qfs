//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/11/01
// Author: Mike Ovsiannikov
//
// Copyright 2008-2010 Quantcast Corp.
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
// Miscellaneous utility functions implementation.
//
//----------------------------------------------------------------------------

#include "QCUtils.h"

#include <stdlib.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>

#if defined(QC_OS_NAME_LINUX) && ! defined(QC_USE_XFS_RESVSP)
#define QC_USE_XFS_RESVSP
#endif

#ifdef QC_USE_XFS_RESVSP
#include <xfs/xfs.h>
#endif

static inline void
StrAppend(
    const char* inStrPtr,
    char*&      ioPtr,
    size_t&     ioMaxLen)
{
    size_t theLen = inStrPtr ? strlen(inStrPtr) : 0;
    if (theLen > ioMaxLen) {
        theLen = ioMaxLen;
    }
    if (ioPtr != inStrPtr) {
        memmove(ioPtr, inStrPtr, theLen);
    }
    ioPtr    += theLen;
    ioMaxLen -= theLen;
    *ioPtr = 0;
}

static int
DoSysErrorMsg(
    const char* inMsgPtr,
    int         inSysError,
    char*       inMsgBufPtr,
    size_t      inMsgBufSize)
{
    if (inMsgBufSize <= 0) {
        return 0;
    }
    char*  theMsgPtr = inMsgBufPtr;
    size_t theMaxLen = inMsgBufSize - 1;

    theMsgPtr[theMaxLen] = 0;
    StrAppend(inMsgPtr, theMsgPtr, theMaxLen);
    if (theMaxLen > 2) {
        if (theMsgPtr != inMsgBufPtr) {
            StrAppend(" ", theMsgPtr, theMaxLen);
        }
#if defined(__EXTENSIONS__) || defined(QC_OS_NAME_DARWIN) || defined(QC_OS_NAME_FREEBSD) || \
    (! defined(_GNU_SOURCE) && (defined(_XOPEN_SOURCE) && _XOPEN_SOURCE < 600 || \
                                defined(_POSIX_C_SOURCE) && _POSIX_C_SOURCE < 200112L))
        int theErr = strerror_r(inSysError, theMsgPtr, theMaxLen);
        if (theErr != 0) {
            theMsgPtr[0] = 0;
        }
        const char* const thePtr = theMsgPtr;
#else
        const char* const thePtr = strerror_r(inSysError, theMsgPtr, theMaxLen);
#endif
        StrAppend(thePtr, theMsgPtr, theMaxLen);
        if (theMaxLen > 0) {
            snprintf(theMsgPtr, theMaxLen, " %d", inSysError);
            StrAppend(theMsgPtr, theMsgPtr, theMaxLen);
        }
    }
    return (int)(theMsgPtr - inMsgBufPtr);
}

/* static */ void
QCUtils::FatalError(
    const char* inMsgPtr,
    int         inSysError)
{
    char      theMsgBuf[1<<9];
    const int theLen =
        DoSysErrorMsg(inMsgPtr, inSysError, theMsgBuf, sizeof(theMsgBuf));
    write(2, theMsgBuf, theLen);
    abort();
}

/* static */ std::string
QCUtils::SysError(
    int         inSysError,
    const char* inMsgPtr /* = 0 */)
{
    char theMsgBuf[1<<9];
    DoSysErrorMsg(inMsgPtr, inSysError, theMsgBuf, sizeof(theMsgBuf));
    return std::string(theMsgBuf);
}

/* static */ void
QCUtils::AssertionFailure(
    const char* inMsgPtr,
    const char* inFileNamePtr,
    int         inLineNum)
{
    char         theMsgBuf[1<<9];
    char*        theMsgPtr = theMsgBuf;
    size_t       theMaxLen = sizeof(theMsgBuf) - 1;

    StrAppend("assertion failure: ", theMsgPtr, theMaxLen);
    StrAppend(inFileNamePtr ? inFileNamePtr : "???", theMsgPtr, theMaxLen);
    if (theMaxLen > 4) {
        snprintf(theMsgPtr, theMaxLen, ":%d\n", inLineNum);
        StrAppend(theMsgPtr, theMsgPtr, theMaxLen);
    }
    write(2, theMsgBuf, theMsgPtr - theMsgBuf);
    abort();
}

/* static */ int64_t
QCUtils::ReserveFileSpace(
    int     inFd,
    int64_t inSize)
{
    if (inSize <= 0) {
        return 0;
    }
#ifdef QC_USE_XFS_RESVSP
    if (platform_test_xfs_fd(inFd)) {
        xfs_flock64_t theResv = {0}; // Clear to make valgrind happy.
        theResv.l_whence = 0;
        theResv.l_start  = 0;
        theResv.l_len    = inSize;
        if (xfsctl(0, inFd, XFS_IOC_RESVSP64, &theResv)) {
            return (errno > 0 ? -errno : (errno ? errno : -1));
        }
        return inSize;
    }
#endif /* QC_USE_XFS_RESVSP */
    return (inFd < 0 ? -EINVAL : 0);
}

/* static */ int
QCUtils::AllocateFileSpace(
    int      inFd,
    int64_t  inSize,
    int64_t  inMinSize            /* = -1 */,
    int64_t* inInitialFileSizePtr /* = 0 */)
{
    int         theErr  = 0;
    const off_t theSize = lseek(inFd, 0, SEEK_END);
    if (theSize < 0 || theSize >= inSize) {
        theErr = theSize < 0 ? errno : 0;
        return theErr;
    }
    if (inInitialFileSizePtr) {
        *inInitialFileSizePtr = theSize;
    }
    // Assume direct io has to be page aligned.
    const long kPgSize = sysconf(_SC_PAGESIZE);
    const long kPgMask = kPgSize - 1;
    QCRTASSERT((kPgMask & kPgSize) == 0);
    if (ReserveFileSpace(inFd, (inSize + kPgMask) & ~kPgMask) > 0) {
        return (ftruncate(inFd, inSize) ? errno : 0);
    }
    if (inMinSize >= 0 && theSize >= inMinSize) {
        return theErr;
    }
    const ssize_t kWrLen  = 1 << 20;
    void* const   thePtr  = mmap(0, kWrLen,
        PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    off_t theOffset = theSize & ~kPgMask;
    // If logical eof is not page aligned, read partial block.
    if (thePtr == MAP_FAILED || (theOffset != theSize && (
            lseek(inFd, theOffset, SEEK_SET) != theOffset             ||
            read(inFd, thePtr, kPgSize)      != (theSize - theOffset) ||
            lseek(inFd, theOffset, SEEK_SET) != theOffset
            ))) {
        theErr = errno;
    } else {
        const off_t theMinSize = inMinSize < 0 ? inSize : inMinSize;
        while (theOffset < theMinSize && write(inFd, thePtr, kWrLen) == kWrLen) {
            theOffset += kWrLen;
        }
        if (theOffset < theMinSize || ftruncate(inFd, theMinSize)) {
            theErr = errno;
        }
    }
    if (thePtr != MAP_FAILED) {
        munmap(thePtr, kWrLen);
    }
    return theErr;
}

/* static */ int
QCUtils::AllocateFileSpace(
    const char* inFileNamePtr,
    int64_t     inSize,
    int64_t     inMinSize            /* = -1 */,
    int64_t*    inInitialFileSizePtr /* = 0 */)
{
    const int theFd = open(inFileNamePtr, O_RDWR | O_CREAT
#ifdef O_DIRECT
            | O_DIRECT
#endif
#ifdef O_NOATIME
            | O_NOATIME
#endif
            , 0666);
    if (theFd < 0) {
        return errno;
    }
    const int theErr = AllocateFileSpace(
        theFd, inSize, inMinSize, inInitialFileSizePtr);
    return (close(theFd) ? (theErr ? theErr : errno) : theErr);
}

