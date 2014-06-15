//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/06/11
//
// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
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
// \brief A tool that scrubs the chunks in a directory and validates checksums.
//
//----------------------------------------------------------------------------

#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <string>
#include <iostream>

#include "kfsio/checksum.h"
#include "common/MsgLogger.h"
#include "qcdio/QCUtils.h"
#include "Chunk.h"
#include "ChunkManager.h"

namespace KFS
{
#ifndef O_DIRECT
const int O_DIRECT = 0;
#endif

using std::cout;

const unsigned int kIoBlkSize = 4 << 10;

static int
Deserialize(ChunkInfo_t& chunkInfo, int fd, char* buf, bool hdrChksumRequiredFlag)
{
    const DiskChunkInfo_t& dci      =
        *reinterpret_cast<const DiskChunkInfo_t*>(buf);
    const uint64_t&        checksum =
        *reinterpret_cast<const uint64_t*>(&dci + 1);
    const size_t           readsz   = (sizeof(dci) + sizeof(checksum) +
        kIoBlkSize - 1) / kIoBlkSize * kIoBlkSize;

    const ssize_t res = pread(fd, buf, readsz, 0);
    if (res != (ssize_t)readsz) {
        return (res < 0 ? -errno : -EINVAL);
    }
    uint32_t headerChecksum = 0;
    if ((checksum != 0 || hdrChksumRequiredFlag) &&
            (headerChecksum = ComputeBlockChecksum(buf, sizeof(dci))) !=
            checksum) {
        KFS_LOG_STREAM_ERROR <<
            "chunk header checksum mismatch:"
            " computed: " << headerChecksum <<
            " expected: " << checksum <<
        KFS_LOG_EOM;
        return -EBADCKSUM;
    }
    KFS_LOG_STREAM_DEBUG <<
        " chunk header checksum: " << checksum <<
    KFS_LOG_EOM;
    return chunkInfo.Deserialize(dci, true);
}

static bool
scrubFile(const string& fn, bool hdrChksumRequiredFlag,
    char* buf, chunkOff_t infilesz)
{
    const int   kNumComponents = 3;
    long long   components[kNumComponents];
    const char* ptr    = fn.c_str();
    char*       end    = 0;
    int64_t     filesz = infilesz;
    int         i;
    const char* p;

    if ((p = strrchr(ptr, '/'))) {
        ptr = p + 1;
    }
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
        KFS_LOG_STREAM_ERROR <<
            fn << ": malformed chunk file name" <<
        KFS_LOG_EOM;
        return false;
    }
    // Allow files bigger than chunk size. If file wasn't properly closed,
    // but was in the stable directory, its header needs to be read,
    // validated and proper size must be set.
    // The file might be bigger by one io buffer size, and io buffer size is
    // guaranteed to be less or equal to the KFS_CHUNK_HEADER_SIZE.
    const int64_t kMaxChunkFileSize = (int64_t)(KFS_CHUNK_HEADER_SIZE + CHUNKSIZE);
    if (filesz < (int64_t)KFS_CHUNK_HEADER_SIZE ||
            filesz > (int64_t)(kMaxChunkFileSize + KFS_CHUNK_HEADER_SIZE)) {
        KFS_LOG_STREAM_ERROR <<
            fn << ": invalid file size: " << filesz <<
        KFS_LOG_EOM;
        return false;
    }
    const chunkId_t chunkId   = components[1];
    const kfsSeq_t  chunkVers = components[2];

    const int fd = open(fn.c_str(), O_RDONLY | O_DIRECT);
    if (fd < 0) {
        const int err = errno;
        KFS_LOG_STREAM_ERROR <<
            fn << ":" << QCUtils::SysError(err) <<
        KFS_LOG_EOM;
        return false;
    }

    ChunkInfo_t chunkInfo;
    const int err = Deserialize(chunkInfo, fd, buf, hdrChksumRequiredFlag);
    if (err != 0) {
        KFS_LOG_STREAM_ERROR <<
            fn << ":" << QCUtils::SysError(-err) <<
        KFS_LOG_EOM;
        close(fd);
        return false;
    }
    if (chunkInfo.chunkId != chunkId) {
        KFS_LOG_STREAM_ERROR <<
            fn << ":" << "chunk id mismatch: " << chunkInfo.chunkId <<
        KFS_LOG_EOM;
        close(fd);
        return false;
    }
    if (chunkInfo.chunkVersion != chunkVers) {
        KFS_LOG_STREAM_ERROR <<
            fn << ":" << "chunk id version: " << chunkInfo.chunkId <<
        KFS_LOG_EOM;
        close(fd);
        return false;
    }
    KFS_LOG_STREAM_DEBUG <<
        "fid: "     << chunkInfo.fileId <<
        " chunkId: " << chunkInfo.chunkId <<
        " size: "    << chunkInfo.chunkSize <<
        " version: " << chunkInfo.chunkVersion <<
    KFS_LOG_EOM;
    if (chunkInfo.chunkSize < 0 ||
            chunkInfo.chunkSize > (chunkOff_t)CHUNKSIZE) {
        KFS_LOG_STREAM_ERROR <<
            fn << ":" << " invalid chunk size: " << chunkInfo.chunkSize <<
        KFS_LOG_EOM;
        close(fd);
        return false;
    }
    const ssize_t res = pread(fd, buf, CHUNKSIZE, KFS_CHUNK_HEADER_SIZE);
    if (res < 0) {
        const int err = errno;
        KFS_LOG_STREAM_ERROR <<
            fn << ":" << QCUtils::SysError(err) <<
        KFS_LOG_EOM;
        close(fd);
        return false;
    }
    if (res != chunkInfo.chunkSize) {
        KFS_LOG_STREAM_ERROR <<
            "chunk size mismatch:"
            " chunk header: " << chunkInfo.chunkSize <<
            " read returned: " << res <<
        KFS_LOG_EOM;
        if (res < chunkInfo.chunkSize) {
            close(fd);
            return false;
        }
    }
    const size_t off = chunkInfo.chunkSize % CHECKSUM_BLOCKSIZE;
    if (off > 0) {
        memset(buf + chunkInfo.chunkSize, 0, CHECKSUM_BLOCKSIZE - off);
    }
    // go thru block by block and verify checksum
    bool ok  = true;
    for (int i = 0, b = 0;
            i < chunkInfo.chunkSize;
            i += CHECKSUM_BLOCKSIZE, b++) {
        const uint32_t cksum = ComputeBlockChecksum(buf + i, CHECKSUM_BLOCKSIZE);
        if (cksum != chunkInfo.chunkBlockChecksum[b]) {
            KFS_LOG_STREAM_ERROR <<
                fn << ": checksum mismatch"
                " block: " << b <<
                " pos: "   << i <<
                " computed: " << cksum <<
                " expected: " << chunkInfo.chunkBlockChecksum[b] <<
            KFS_LOG_EOM;
            ok = false;
        }
    }
    close(fd);
    return ok;
}

static int
ChunkScrubberMain(int argc, char **argv)
{
    int         optchar;
    bool        help                  = false;
    bool        verbose               = false;
    bool        hdrChksumRequiredFlag = false;
    bool        throttleFlag          = false;
    double      sampling              = -1;

    while ((optchar = getopt(argc, argv, "hvtcs:")) != -1) {
        switch (optchar) {
            case 'v':
                verbose = true;
                break;
            case 's':
                sampling = atof(optarg);
                break;
            case 't':
                throttleFlag = true;
                break;
            case 'h':
            default:
                help = true;
                break;
        }
    }

    if (help || optind >= argc) {
        cout <<
            "Usage: " << argv[0] << "{-v} {-s 0.1} {-t} <chunkdir> <chunkdir>...\n"
            " -s -- sampling: scrub only about 10% of the files\n"
            " -v -- verbose\n"
            " -t -- throttle\n"
        ;
        return 1;
    }

    MsgLogger::Init(0, verbose ?
        MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelINFO);

    char* const allocBuf =
        new char[CHUNKSIZE + KFS_CHUNK_HEADER_SIZE + kIoBlkSize];
    char* const buf      = allocBuf +
        (kIoBlkSize - (unsigned int)(allocBuf - (char*)0) % kIoBlkSize);
    srand48(time(0));

    int ret = 0;
    while (optind < argc) {
        const char* const chunkDir  = argv[optind++];
        DIR* const        dirStream = opendir(chunkDir);
        if (! dirStream) {
            const int err = errno;
            KFS_LOG_STREAM_ERROR <<
                chunkDir << ": " << QCUtils::SysError(err) <<
            KFS_LOG_EOM;
            ret = 1;
            continue;
        }
        double               randval;
        struct stat          statBuf = {0};
        struct dirent const* dent;
        int                  i       = 0;
        while ((dent = readdir(dirStream))) {
            ++i;
            string fn(chunkDir);
            fn = fn + "/" + dent->d_name;
            if (stat(fn.c_str(), &statBuf)) {
                const int err = errno;
                KFS_LOG_STREAM_ERROR <<
                    fn << ":" << QCUtils::SysError(err) <<
                KFS_LOG_EOM;
                ret = 1;
                continue;
            }
            if (! S_ISREG(statBuf.st_mode)) {
                continue;
            }
            if (sampling > 0) {
                randval = drand48();
                if (randval > 0.1) {
                    continue;
                }
            }
            if (! scrubFile(fn, hdrChksumRequiredFlag, buf, statBuf.st_size)) {
                ret = 1;
            }
            // scrubs will keep the disk very busy; slow it down so that
            // the system isn't overwhelmed
            if (throttleFlag && (i % 10) == 0) {
                sleep(1);
            }
        }
        closedir(dirStream);
    }
    delete [] allocBuf;

    MsgLogger::Stop();
    return ret;
}

}

int
main(int argc, char** argv)
{
    return KFS::ChunkScrubberMain(argc, argv);
}
