//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/05/01
// Author: Mike Ovsiannikov
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
// \file FdWriter.h
// \brief Basic file descriptor writer which can be used with MdStreamT.
//
//----------------------------------------------------------------------------

#ifndef FD_WRITER_H
#define FD_WRITER_H

#include <unistd.h>
#include <errno.h>

namespace KFS
{

class FdWriter
{
public:
    FdWriter(
        int inFd)
        : mFd(inFd),
          mError(0)
        {}
    ~FdWriter()
        {}
    void flush()
        {}
    bool write(
        const void* inBufPtr,
        size_t      inLength)
    {
        const char*       thePtr    = static_cast<const char*>(inBufPtr);
        const char* const theEndPtr = thePtr + inLength;
        while (thePtr < theEndPtr) {
            const ssize_t theNWr = ::write(mFd, thePtr, theEndPtr - thePtr);
            if (theNWr < 0 && errno != EINTR) {
                mError = errno;
                return false;
            }
            thePtr += theNWr;
        }
        return true;
    }
    void ClearError()
        { mError = 0; }
    int GetError() const
        { return mError; }
private:
    const int mFd;
    int       mError;
};

}

#endif /* FD_WRITER_H */
