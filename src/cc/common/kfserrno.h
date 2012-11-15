//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/14/11
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
// \brief Kfs platform independent / protocol error code.
//
//----------------------------------------------------------------------------

#ifndef KFS_ERRNO_H
#define KFS_ERRNO_H

namespace KFS
{

const int kKFS_EPERM           = 1;
const int kKFS_ENOENT          = 2;
const int kKFS_ESRCH           = 3;
const int kKFS_EINTR           = 4;
const int kKFS_EIO             = 5;
const int kKFS_ENXIO           = 6;
const int kKFS_E2BIG           = 7;
const int kKFS_ENOEXEC         = 8;
const int kKFS_EBADF           = 9;
const int kKFS_ECHILD          = 10;
const int kKFS_EAGAIN          = 11;
const int kKFS_ENOMEM          = 12;
const int kKFS_EACCES          = 13;
const int kKFS_EFAULT          = 14;
const int kKFS_ENOTBLK         = 15;
const int kKFS_EBUSY           = 16;
const int kKFS_EEXIST          = 17;
const int kKFS_EXDEV           = 18;
const int kKFS_ENODEV          = 19;
const int kKFS_ENOTDIR         = 20;
const int kKFS_EISDIR          = 21;
const int kKFS_EINVAL          = 22;
const int kKFS_ENFILE          = 23;
const int kKFS_EMFILE          = 24;
const int kKFS_ENOTTY          = 25;
const int kKFS_ETXTBSY         = 26;
const int kKFS_EFBIG           = 27;
const int kKFS_ENOSPC          = 28;
const int kKFS_ESPIPE          = 29;
const int kKFS_EROFS           = 30;
const int kKFS_EMLINK          = 31;
const int kKFS_EPIPE           = 32;
const int kKFS_EDOM            = 33;
const int kKFS_ERANGE          = 34;
const int kKFS_EDEADLK         = 35;
const int kKFS_ENAMETOOLONG    = 36;
const int kKFS_ENOLCK          = 37;
const int kKFS_ENOSYS          = 38;
const int kKFS_ENOTEMPTY       = 39;
const int kKFS_ELOOP           = 40;
const int kKFS_EWOULDBLOCK     = kKFS_EAGAIN;
const int kKFS_EDEADLOCK       = kKFS_EDEADLK;
const int kKFS_EOVERFLOW       = 75;
const int kKFS_EADDRINUSE      = 98;
const int kKFS_EADDRNOTAVAIL   = 99;
const int kKFS_ENETDOWN        = 100;
const int kKFS_ENETUNREACH     = 101;
const int kKFS_ENETRESET       = 102;
const int kKFS_ECONNABORTED    = 103;
const int kKFS_ECONNRESET      = 104;
const int kKFS_ENOBUFS         = 105;
const int kKFS_EISCONN         = 106;
const int kKFS_ENOTCONN        = 107;
const int kKFS_ETIMEDOUT       = 110;
const int kKFS_ECONNREFUSED    = 111;
const int kKFS_EHOSTDOWN       = 112;
const int kKFS_EHOSTUNREACH    = 113;
const int kKFS_EALREADY        = 114;
const int kKFS_EINPROGRESS     = 115;
const int kKFS_EDQUOT          = 122;
const int kKFS_ECANCELED       = 125;

int KfsToSysErrno(int inErrno);
int SysToKfsErrno(int inErrno);

}

#endif /* KFS_ERRNO_H */
