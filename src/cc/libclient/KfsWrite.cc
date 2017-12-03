//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/10/02
// Author: Sriram Rao
//
// Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
// Copyright 2006-2008 Kosmix Corp.
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
// All the code to deal with writes from the client.
//----------------------------------------------------------------------------

#include "KfsClientInt.h"
#include "KfsProtocolWorker.h"
#include "common/MsgLogger.h"
#include "qcdio/qcstutils.h"

#include <cerrno>
#include <string>
#include <limits>
#include <algorithm>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace KFS {
namespace client {

using std::string;
using std::min;
using std::numeric_limits;

int
KfsClientImpl::RecordAppend(int fd, const char *buf, int numBytes)
{
    const bool asyncFlag      = false;
    const bool appendOnlyFlag = false;
    return Write(fd, buf, numBytes, asyncFlag, appendOnlyFlag);
}

int
KfsClientImpl::AtomicRecordAppend(int fd, const char *buf, int numBytes)
{
    const bool asyncFlag      = false;
    const bool appendOnlyFlag = true;
    return Write(fd, buf, numBytes, asyncFlag, appendOnlyFlag);
}

ssize_t
KfsClientImpl::Write(int fd, const char *buf, size_t numBytes, chunkOff_t* pos)
{
    const bool asyncFlag      = false;
    const bool appendOnlyFlag = false;
    return Write(fd, buf, numBytes, asyncFlag, appendOnlyFlag, pos);
}

int
KfsClientImpl::WriteAsync(int fd, const char *buf, size_t numBytes)
{
    const bool asyncFlag      = true;
    const bool appendOnlyFlag = false;
    return Write(fd, buf, numBytes, asyncFlag, appendOnlyFlag);
}

int
KfsClientImpl::WriteAsyncCompletionHandler(int fd)
{
    return Sync(fd);
}

const size_t kMaxWriteSize = numeric_limits<int>::max() / CHUNKSIZE * CHUNKSIZE;

ssize_t
KfsClientImpl::Write(int fd, const char* buf, size_t numBytes,
    bool asyncFlag, bool appendOnlyFlag, chunkOff_t* pos /* = 0 */)
{
    const char*       ptr = buf;
    const char* const end = ptr + numBytes;
    do {
        const ssize_t res = WriteSelf(fd, ptr,
            min(kMaxWriteSize, size_t(end - ptr)),
            asyncFlag, appendOnlyFlag, pos
        );
        if (res < 0) {
            return res;
        }
        ptr += res;
    } while (ptr < end);
    return numBytes;
}

ssize_t
KfsClientImpl::WriteSelf(int fd, const char* buf, size_t numBytes,
    bool asyncFlag, bool appendOnlyFlag, chunkOff_t* pos)
{
    QCStMutexLocker lock(mMutex);

    if (! valid_fd(fd)) {
        KFS_LOG_STREAM_ERROR <<
            "write error invalid fd: " << fd <<
        KFS_LOG_EOM;
        return -EBADF;
    }
    FileTableEntry& entry = *mFileTable[fd];
    if (entry.openMode == O_RDONLY) {
        return -EINVAL;
    }
    if (entry.fattr.fileId <= 0) {
        return -EBADF;
    }
    if (entry.fattr.isDirectory) {
        return -EISDIR;
    }
    if (numBytes <= 0) {
        return 0;
    }
    if (! buf) {
        return -EINVAL;
    }
    if (! entry.usedProtocolWorkerFlag &&
            0 == entry.fattr.numReplicas && 0 != entry.fattr.fileSize) {
        // Overwrite and append are not supported with object store files.
        return -ESPIPE;
    }

    chunkOff_t&   filePos    = pos ? *pos : entry.currPos.fileOffset;
    const int64_t offset     = filePos;
    const bool    appendFlag = (entry.openMode & O_APPEND) != 0;
    if ((offset < 0 || appendOnlyFlag) && ! appendFlag) {
        return -EINVAL;
    }
    if (appendFlag) {
        if (numBytes > (int)CHUNKSIZE) {
            return -EFBIG;
        }
    } else {
        if (offset < 0 || appendOnlyFlag) {
            return -EINVAL;
        }
        if (filePos + (chunkOff_t)numBytes < 0) {
            return -EFBIG;
        }
        filePos += numBytes;
    }
    StartProtocolWorker();
    KfsProtocolWorker::Request::Params        openParams;
    KfsProtocolWorker::Request::Params* const openParamsPtr =
        entry.usedProtocolWorkerFlag ? 0 : &openParams;
    if (openParamsPtr) {
        if (0 < entry.fattr.fileSize &&
                (KFS_STRIPED_FILE_TYPE_NONE != entry.fattr.striperType ||
                0 == entry.fattr.numReplicas)) {
            // Re-validate file size, in case truncate was issued, as for
            // striped and object store files logical EOF has to be updated
            // explicitly on close.
            const FAttr* const fa = LookupFAttr(entry.fattr.fileId, entry.name);
            if (! fa || fa->fileId != entry.fattr.fileId ||
                    ! IsValid(*fa, time(0))) {
                KfsFileAttr attr;
                const bool computeFileSizeFlag = false;
                const int ret = StatSelf(
                    entry.pathname.c_str(), attr, computeFileSizeFlag);
                if (0 == ret && entry.fattr.fileId == attr.fileId &&
                        ! attr.isDirectory) {
                    entry.fattr.fileSize = attr.fileSize;
                }
            } else {
                entry.fattr.fileSize = fa->fileSize;
            }
        }
        openParams.mPathName            = entry.pathname;
        openParams.mFileSize            = entry.fattr.fileSize;
        openParams.mStriperType         = entry.fattr.striperType;
        openParams.mStripeSize          = entry.fattr.stripeSize;
        openParams.mStripeCount         = entry.fattr.numStripes;
        openParams.mRecoveryStripeCount = entry.fattr.numRecoveryStripes;
        openParams.mReplicaCount        = entry.fattr.numReplicas;
        openParams.mMsgLogId            = fd;
        if(entry.fattr.striperType == KFS_STRIPED_FILE_TYPE_NONE) {
            openParams.mDiskIoSize = entry.ioBufferSize;
        } else {
            const int kChecksumBlockSize = (int)CHECKSUM_BLOCKSIZE;
            const int totalStripeCount   =
               entry.fattr.numStripes + entry.fattr.numRecoveryStripes;
            openParams.mDiskIoSize = (entry.ioBufferSize / totalStripeCount
               + kChecksumBlockSize - 1) /
               kChecksumBlockSize * kChecksumBlockSize;
        }
    }
    entry.usedProtocolWorkerFlag = true;
    entry.pending += numBytes;
    const KfsProtocolWorker::FileId       fileId       = entry.fattr.fileId;
    const KfsProtocolWorker::FileInstance fileInstance = entry.instance;
    const string                          pathName     = entry.pathname;
    const int                             bufsz        = entry.ioBufferSize;
    const int64_t                         prevPending  = entry.pending;
    const bool                            throttle     =
        ! asyncFlag && bufsz > 0 && bufsz <= entry.pending;
    if ((throttle || bufsz <= 0) && ! asyncFlag) {
        entry.pending = 0;
    }
    lock.Unlock();

    KFS_LOG_STREAM_DEBUG <<
        fd << "," << fileId << "," << fileInstance << "," << pathName <<
        (asyncFlag  ? " async" : "") <<
        (appendFlag ? " append ->" : " write ->") <<
        " offset: "   << offset <<
        " size: "     << numBytes <<
        " throttle: " << throttle <<
        " pending: "  << prevPending <<
        " bufsz: "    << bufsz <<
    KFS_LOG_EOM;

    const int64_t status = mProtocolWorker->Execute(
        asyncFlag ?
            (appendFlag ?
                KfsProtocolWorker::kRequestTypeWriteAppendAsyncNoCopy :
                KfsProtocolWorker::kRequestTypeWriteAsyncNoCopy) :
            (bufsz <= 0 ?
                (appendFlag ?
                    KfsProtocolWorker::kRequestTypeWriteAppend :
                    KfsProtocolWorker::kRequestTypeWrite) :
                (throttle ?
                    (appendFlag ?
                        KfsProtocolWorker::kRequestTypeWriteAppendThrottle :
                        KfsProtocolWorker::kRequestTypeWriteThrottle) :
                    (appendFlag ?
                        KfsProtocolWorker::kRequestTypeWriteAppendAsync :
                        KfsProtocolWorker::kRequestTypeWriteAsync)
            )),
        fileInstance,
        fileId,
        openParamsPtr,
        const_cast<char*>(buf),
        numBytes,
        (throttle || (! appendFlag && bufsz >= 0)) ? bufsz : -1,
        offset
    );
    if (status < 0) {
        return (ssize_t)status;
    }
    if (throttle && status > 0) {
        QCStMutexLocker lock(mMutex);
        // File can be closed by other thread, fd entry can be re-used.
        // In this cases close / sync should have returned the corresponding
        // status.
        // Throttle returns current number of bytes pending.
        if (valid_fd(fd) && &entry == mFileTable[fd] &&
                entry.instance == fileInstance) {
            KFS_LOG_STREAM_DEBUG <<
                fd << "," << fileId << "," << fileInstance << "," << pathName <<
                (appendFlag ?  " append <+" : " write <+") <<
                " offset: " << offset <<
                " size: "   << numBytes <<
                " pending:"
                " prev: "   << prevPending <<
                " cur: "    << entry.pending <<
                " add: "    << status <<
            KFS_LOG_EOM;
            entry.pending += status;
        }
    }
    return numBytes;
}

}}
