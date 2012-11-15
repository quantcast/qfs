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

#include "kfserrno.h"
#include "kfstypes.h"

#include <errno.h>

namespace KFS
{
    int
KfsToSysErrno(
    int inErrno)
{
    switch (inErrno)
    {
        case kKFS_EPERM:            return EPERM;
        case kKFS_ENOENT:           return ENOENT;
        case kKFS_ESRCH:            return ESRCH;
        case kKFS_EINTR:            return EINTR;
        case kKFS_EIO:              return EIO;
        case kKFS_ENXIO:            return ENXIO;
        case kKFS_E2BIG:            return E2BIG;
        case kKFS_ENOEXEC:          return ENOEXEC;
        case kKFS_EBADF:            return EBADF;
        case kKFS_ECHILD:           return ECHILD;
        case kKFS_EAGAIN:           return EAGAIN;
        case kKFS_ENOMEM:           return ENOMEM;
        case kKFS_EACCES:           return EACCES;
        case kKFS_EFAULT:           return EFAULT;
        case kKFS_ENOTBLK:          return ENOTBLK;
        case kKFS_EBUSY:            return EBUSY;
        case kKFS_EEXIST:           return EEXIST;
        case kKFS_EXDEV:            return EXDEV;
        case kKFS_ENODEV:           return ENODEV;
        case kKFS_ENOTDIR:          return ENOTDIR;
        case kKFS_EISDIR:           return EISDIR;
        case kKFS_EINVAL:           return EINVAL;
        case kKFS_ENFILE:           return ENFILE;
        case kKFS_EMFILE:           return EMFILE;
        case kKFS_ENOTTY:           return ENOTTY;
        case kKFS_ETXTBSY:          return ETXTBSY;
        case kKFS_EFBIG:            return EFBIG;
        case kKFS_ENOSPC:           return ENOSPC;
        case kKFS_ESPIPE:           return ESPIPE;
        case kKFS_EROFS:            return EROFS;
        case kKFS_EMLINK:           return EMLINK;
        case kKFS_EPIPE:            return EPIPE;
        case kKFS_EDOM:             return EDOM;
        case kKFS_ERANGE:           return ERANGE;
        case kKFS_EDEADLK:          return EDEADLK;
        case kKFS_ENAMETOOLONG:     return ENAMETOOLONG;
        case kKFS_ENOLCK:           return ENOLCK;
        case kKFS_ENOSYS:           return ENOSYS;
        case kKFS_ENOTEMPTY:        return ENOTEMPTY;
        case kKFS_ELOOP:            return ELOOP;
        case kKFS_EOVERFLOW:        return EOVERFLOW;
        case kKFS_EADDRINUSE:       return EADDRINUSE;
        case kKFS_EADDRNOTAVAIL:    return EADDRNOTAVAIL;
        case kKFS_ENETDOWN:         return ENETDOWN;
        case kKFS_ENETUNREACH:      return ENETUNREACH;
        case kKFS_ENETRESET:        return ENETRESET;
        case kKFS_ECONNABORTED:     return ECONNABORTED;
        case kKFS_ECONNRESET:       return ECONNRESET;
        case kKFS_ENOBUFS:          return ENOBUFS;
        case kKFS_EISCONN:          return EISCONN;
        case kKFS_ENOTCONN:         return ENOTCONN;
        case kKFS_ETIMEDOUT:        return ETIMEDOUT;
        case kKFS_ECONNREFUSED:     return ECONNREFUSED;
        case kKFS_EHOSTDOWN:        return EHOSTDOWN;
        case kKFS_EHOSTUNREACH:     return EHOSTUNREACH;
        case kKFS_EALREADY:         return EALREADY;
        case kKFS_EINPROGRESS:      return EINPROGRESS;
        case kKFS_EDQUOT:           return EDQUOT;
        case kKFS_ECANCELED:        return ECANCELED;
        default: break;
    }
    return inErrno;
}

    int
SysToKfsErrno(
    int inErrno)
{
    switch (inErrno)
    {
        case EPERM:            return kKFS_EPERM;
        case ENOENT:           return kKFS_ENOENT;
        case ESRCH:            return kKFS_ESRCH;
        case EINTR:            return kKFS_EINTR;
        case EIO:              return kKFS_EIO;
        case ENXIO:            return kKFS_ENXIO;
        case E2BIG:            return kKFS_E2BIG;
        case ENOEXEC:          return kKFS_ENOEXEC;
        case EBADF:            return kKFS_EBADF;
        case ECHILD:           return kKFS_ECHILD;
        case EAGAIN:           return kKFS_EAGAIN;
        case ENOMEM:           return kKFS_ENOMEM;
        case EACCES:           return kKFS_EACCES;
        case EFAULT:           return kKFS_EFAULT;
        case ENOTBLK:          return kKFS_ENOTBLK;
        case EBUSY:            return kKFS_EBUSY;
        case EEXIST:           return kKFS_EEXIST;
        case EXDEV:            return kKFS_EXDEV;
        case ENODEV:           return kKFS_ENODEV;
        case ENOTDIR:          return kKFS_ENOTDIR;
        case EISDIR:           return kKFS_EISDIR;
        case EINVAL:           return kKFS_EINVAL;
        case ENFILE:           return kKFS_ENFILE;
        case EMFILE:           return kKFS_EMFILE;
        case ENOTTY:           return kKFS_ENOTTY;
        case ETXTBSY:          return kKFS_ETXTBSY;
        case EFBIG:            return kKFS_EFBIG;
        case ENOSPC:           return kKFS_ENOSPC;
        case ESPIPE:           return kKFS_ESPIPE;
        case EROFS:            return kKFS_EROFS;
        case EMLINK:           return kKFS_EMLINK;
        case EPIPE:            return kKFS_EPIPE;
        case EDOM:             return kKFS_EDOM;
        case ERANGE:           return kKFS_ERANGE;
        case EDEADLK:          return kKFS_EDEADLK;
        case ENAMETOOLONG:     return kKFS_ENAMETOOLONG;
        case ENOLCK:           return kKFS_ENOLCK;
        case ENOSYS:           return kKFS_ENOSYS;
        case ENOTEMPTY:        return kKFS_ENOTEMPTY;
        case ELOOP:            return kKFS_ELOOP;
        case EOVERFLOW:        return kKFS_EOVERFLOW;
        case EADDRINUSE:       return kKFS_EADDRINUSE;
        case EADDRNOTAVAIL:    return kKFS_EADDRNOTAVAIL;
        case ENETDOWN:         return kKFS_ENETDOWN;
        case ENETUNREACH:      return kKFS_ENETUNREACH;
        case ENETRESET:        return kKFS_ENETRESET;
        case ECONNABORTED:     return kKFS_ECONNABORTED;
        case ECONNRESET:       return kKFS_ECONNRESET;
        case ENOBUFS:          return kKFS_ENOBUFS;
        case EISCONN:          return kKFS_EISCONN;
        case ENOTCONN:         return kKFS_ENOTCONN;
        case ETIMEDOUT:        return kKFS_ETIMEDOUT;
        case ECONNREFUSED:     return kKFS_ECONNREFUSED;
        case EHOSTDOWN:        return kKFS_EHOSTDOWN;
        case EHOSTUNREACH:     return kKFS_EHOSTUNREACH;
        case EALREADY:         return kKFS_EALREADY;
        case EINPROGRESS:      return kKFS_EINPROGRESS;
        case EDQUOT:           return kKFS_EDQUOT;
        case ECANCELED:        return kKFS_ECANCELED;
        // Ensure that the following do not clash with the sytem error values.
        case EBADVERS:
        case ELEASEEXPIRED:
        case EBADCKSUM:
        case EDATAUNAVAIL:
        case ESERVERBUSY:
        case EALLOCFAILED:
        case EBADCLUSTERKEY:
        case EINVALCHUNKSIZE:
        default:
            break;
    }
    return inErrno;
}

}
