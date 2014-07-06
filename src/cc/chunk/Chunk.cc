//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/10/07
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
//----------------------------------------------------------------------------

#include "Chunk.h"
#include "qcdio/QCUtils.h"

#include <string>

#include <stdlib.h>
#include <unistd.h>
#include <time.h>

#include <sys/types.h>
#include <fcntl.h>

namespace KFS
{
using std::string;

bool IsValidChunkFile(
    const string&      dirname,
    const char*        filename,
    int64_t            infilesz,
    bool               requireChunkHeaderChecksumFlag,
    bool               forceReadFlag,
    ChunkHeaderBuffer& chunkHeaderBuffer,
    kfsFileId_t&       outFileId,
    chunkId_t&         outChunkId,
    kfsSeq_t&          outChunkVers,
    int64_t&           outChunkSize,
    int64_t&           outFileSystemId,
    int&               outIoTimeSec,
    bool&              outReadFlag)
{
    const int   kNumComponents = 3;
    long long   components[kNumComponents];
    const char* ptr    = filename;
    char*       end    = 0;
    int64_t     filesz = infilesz;
    int         i;

    outReadFlag = false;
    for (i = 0; i < kNumComponents; i++) {
        components[i] = strtoll(ptr, &end, 10);
        if (components[i] < 0) {
            break;
        }
        if ((*end & 0xFF) != '.') {
            if (*end == 0) {
                i++;
            }
            break;
        }
        ptr = end + 1;
    }
    if (i != kNumComponents || *end) {
        KFS_LOG_STREAM_INFO <<
            "ignoring malformed chunk file name: " <<
                dirname << filename <<
        KFS_LOG_EOM;
        return false;
    }
    // Allow files bigger than chunk size. If file wasn't properly closed,
    // but was in the stable directory, its header needs to be read,
    // validated and proper size must be set.
    // The file might be bigger by one io buffer size, and io buffer size is
    // guaranteed to be less or equal to the KFS_CHUNK_HEADER_SIZE.
    const int64_t kMaxChunkFileSize =
        (int64_t)(KFS_CHUNK_HEADER_SIZE + CHUNKSIZE);
    if (filesz < (int64_t)KFS_CHUNK_HEADER_SIZE ||
            filesz > (int64_t)(kMaxChunkFileSize + KFS_CHUNK_HEADER_SIZE)) {
        KFS_LOG_STREAM_INFO <<
            "ignoring invalid chunk file: " << dirname << filename <<
            " size: " << filesz <<
        KFS_LOG_EOM;
        return false;
    }
    const chunkId_t chunkId   = components[1];
    const kfsSeq_t  chunkVers = components[2];
    outFileId    = components[0];
    outChunkId   = chunkId;
    outChunkVers = chunkVers;
    outChunkSize = filesz - KFS_CHUNK_HEADER_SIZE;
    if (filesz > kMaxChunkFileSize || forceReadFlag) {
        outReadFlag = true;
        // Load and validate chunk header, and set proper file size.
        const string cf(dirname + filename);
        const time_t start = time(0);
        const int    fd    = open(cf.c_str(), O_RDONLY);
        if (fd < 0) {
            const int err = errno;
            KFS_LOG_STREAM_INFO <<
                "ignoring invalid chunk file: " << cf <<
                    " size: " << filesz <<
                    " :" << QCUtils::SysError(err) <<
            KFS_LOG_EOM;
            return false;
        }
        const ssize_t rd = read(fd, chunkHeaderBuffer.GetPtr(),
            chunkHeaderBuffer.GetSize());
        close(fd);
        outIoTimeSec = time(0) - start;
        if (rd != chunkHeaderBuffer.GetSize()) {
            const int err = rd < 0 ? errno : EINVAL;
            KFS_LOG_STREAM_INFO <<
                "ignoring invalid chunk file: " << cf <<
                    " size: "                   << filesz <<
                    " read: "                   << rd <<
                    " :"                        << QCUtils::SysError(err) <<
            KFS_LOG_EOM;
            return false;
        }
        const DiskChunkInfo_t& dci      =
            *reinterpret_cast<const DiskChunkInfo_t*>(
                chunkHeaderBuffer.GetPtr());
        const uint64_t         checksum =
            *reinterpret_cast<const uint64_t*>(&dci + 1);
        const int res = dci.Validate(chunkId, chunkVers);
        if (res < 0) {
            KFS_LOG_STREAM_INFO <<
                "ignoring invalid chunk file: " << cf <<
                    " size: "                   << filesz <<
                    " invalid chunk header"
                    " status: "                 << res <<
            KFS_LOG_EOM;
            return false;
        }
        uint32_t hdrChecksum = 0;
        if ((checksum != 0 || requireChunkHeaderChecksumFlag) &&
                ((hdrChecksum = ComputeBlockChecksum(
                    chunkHeaderBuffer.GetPtr(), sizeof(dci))) != checksum)) {
            KFS_LOG_STREAM_INFO <<
                "ignoring invalid chunk file: " << cf <<
                    " invalid header:"
                    " size: "                   << filesz <<
                    " chunk size: "             << dci.chunkSize <<
                    " checksum: "               << checksum <<
                    " expect: "                 << hdrChecksum <<
            KFS_LOG_EOM;
            return false;
        }
        outFileSystemId = dci.GetFsId();
        filesz = dci.chunkSize + KFS_CHUNK_HEADER_SIZE;
        if (filesz < infilesz) {
            if (truncate(cf.c_str(), filesz)) {
                const int err = errno;
                KFS_LOG_STREAM_ERROR <<
                    "failed truncate chunk file: " << cf <<
                        " size: "                  << infilesz <<
                        " to: "                    << filesz <<
                        " :"                       << QCUtils::SysError(err) <<
                KFS_LOG_EOM;
            } else {
                KFS_LOG_STREAM_INFO <<
                    "truncated chunk file: " << cf <<
                        " size: "            << infilesz <<
                        " to: "              << filesz <<
                KFS_LOG_EOM;
            }
        }
    }
    return true;
}

}
