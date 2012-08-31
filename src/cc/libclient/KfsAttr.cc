//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/08/22
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
//
// \file KfsAttr.cc
// \brief Kfs i-node attributes class.
//
//----------------------------------------------------------------------------

#include "KfsAttr.h"

#include <string.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

namespace KFS {
namespace client {

void
FileAttr::ToStat(
    struct stat& outStat) const
{
    memset(&outStat, 0, sizeof(outStat));
    outStat.st_ino = fileId;
    // Directories are drwxrwxrwx, files are drw-rw-rw-
    if (isDirectory) {
        outStat.st_mode = S_IFDIR | (mode_t)((mode == kKfsModeUndef ?
            (kfsMode_t)0777 : mode) & 0777);
        outStat.st_size = 0;
    } else {
        outStat.st_mode = S_IFREG | (mode_t)((mode == kKfsModeUndef ?
            (kfsMode_t)0666 : mode) & 0777);
        outStat.st_size = fileSize;
    }
#ifdef S_ISVTX
    if (IsSticky()) {
        outStat.st_mode |= S_ISVTX;
    }
#endif
    outStat.st_blksize = CHUNKSIZE;
    outStat.st_blocks  = (fileSize + CHUNKSIZE - 1) / CHUNKSIZE;
    outStat.st_uid     = (uid_t)user;
    outStat.st_gid     = (gid_t)group;
#ifdef KFS_OS_NAME_DARWIN
    outStat.st_atimespec.tv_sec  = mtime.tv_sec;
    outStat.st_atimespec.tv_nsec = mtime.tv_usec * 1000;
    outStat.st_mtimespec.tv_sec  = mtime.tv_sec;
    outStat.st_mtimespec.tv_nsec = mtime.tv_usec * 1000;
    outStat.st_ctimespec.tv_sec  = ctime.tv_sec;
    outStat.st_ctimespec.tv_nsec = ctime.tv_usec * 1000;
#else
    outStat.st_atime = mtime.tv_sec;
    outStat.st_mtime = mtime.tv_sec;
    outStat.st_ctime = ctime.tv_sec;
#endif
}

}}

