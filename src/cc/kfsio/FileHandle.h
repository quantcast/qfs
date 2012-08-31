//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/05/13
//
// Author: Sriram Rao
//
// Copyright 2008 Quantcast Corp.
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
// \brief A ref-counted file-id object.
//
//----------------------------------------------------------------------------

#ifndef _LIBKFSIO_FILEHANDLE_H
#define _LIBKFSIO_FILEHANDLE_H

#include <boost/shared_ptr.hpp>

namespace KFS
{
struct FileHandle_t
{
    FileHandle_t() : mFd(-1) { }
    FileHandle_t(int fd) : mFd(fd) { }
    ~FileHandle_t() {
        if (mFd < 0)
            return;
        close(mFd);
        mFd = -1;
    }
    void Close() {
        if (mFd < 0)
            return;
        close(mFd);
        mFd = -1;
    }
    int mFd; // the underlying file pointer
};

typedef boost::shared_ptr<FileHandle_t> FileHandlePtr;
}

#endif // _LIBKFSIO_FILEHANDLE_H
