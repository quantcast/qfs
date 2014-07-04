//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/22
// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
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
// 
//----------------------------------------------------------------------------

#ifndef _CHUNKSERVER_CHUNK_H
#define _CHUNKSERVER_CHUNK_H

#include "common/MsgLogger.h"
#include "common/kfstypes.h"
#include "kfsio/checksum.h"
#include "utils.h"

#include <stdint.h>
#include <unistd.h>
#include <string.h>

#include <iomanip>
#include <boost/static_assert.hpp>

namespace KFS
{
using std::hex;
using std::dec;

///
/// \file Chunk.h
/// \brief Declarations related to a Chunk in KFS.
///


/// 
/// \brief ChunkInfo_t
/// For each chunk, the chunkserver maintains a meta-data file.  This
/// file defines the chunk attributes such as, the file it is
/// associated with, the chunk version #, and the checksums for each
/// block of the file.  For each chunk, this structure is read in at
/// startup time.
///

/// We allow a chunk header upto 16K in size
const size_t KFS_CHUNK_HEADER_SIZE = 16 << 10;

/// The max # of checksum blocks we have for a given chunk
const uint32_t MAX_CHUNK_CHECKSUM_BLOCKS = CHUNKSIZE /  CHECKSUM_BLOCKSIZE;

/// In the chunk header, we store upto 256 char of the file that
/// originally created the chunk.  
const size_t CHUNK_META_MAX_FILENAME_LEN = 256;

const uint32_t CHUNK_META_MAGIC = 0xCAFECAFE;
const uint32_t CHUNK_META_VERSION = 0x1;
static const char* const kKfsChunkFsIdPrefix       =
    "\0QFSFsId\xe4\x5e\x23\x0e\x34\x9a\x07\xce";
static size_t const      kKfsChunkFsIdPrefixLength = 16;

// This structure is on-disk
struct DiskChunkInfo_t
{
    DiskChunkInfo_t(kfsFileId_t f, kfsChunkId_t c, int64_t s, kfsSeq_t v)
        : metaMagic(CHUNK_META_MAGIC),
          metaVersion(CHUNK_META_VERSION),
          fileId(f),
          chunkId(c),
          chunkVersion(v),
          chunkSize(s),
          numReads(0),
          unused(0) {
            memset(filename, 0, CHUNK_META_MAX_FILENAME_LEN);
    }

    void SetChecksums(const uint32_t* checksums) {
        memcpy(chunkBlockChecksum, checksums,
            MAX_CHUNK_CHECKSUM_BLOCKS * sizeof(chunkBlockChecksum[0]));
    }

    int Validate() const {
        if (metaMagic != CHUNK_META_MAGIC) {
            KFS_LOG_STREAM_INFO <<
                "Magic # mismatch (got: " << hex << metaMagic <<
                ", expect: " << CHUNK_META_MAGIC << ")" << dec <<
            KFS_LOG_EOM;
            return -KFS::EBADCKSUM;
        }
        if (metaVersion != CHUNK_META_VERSION) {
            KFS_LOG_STREAM_INFO <<
                "Version # mismatch (got: << " << hex << metaVersion <<
                ", expect: << " << CHUNK_META_VERSION << ")" << dec <<
            KFS_LOG_EOM;
            return -KFS::EBADCKSUM;
        }
        if (chunkSize > (uint64_t)CHUNKSIZE) {
            KFS_LOG_STREAM_INFO <<
                "Invlid chunk size: " << chunkSize <<
            KFS_LOG_EOM;
            return -KFS::EBADCKSUM;
        }
        return 0;
    }

    int Validate(kfsChunkId_t cid, kfsSeq_t vers) const {
        const int ret = Validate();
        if (ret < 0) {
            return ret;
        }
        if ((kfsChunkId_t)chunkId != cid) {
            KFS_LOG_STREAM_INFO <<
                "Chunkid mismatch (got: " <<
                    chunkId << ", expect: " << cid << ")" <<
            KFS_LOG_EOM;
            return -KFS::EBADCKSUM;
        }
        if ((kfsSeq_t)chunkVersion != vers) {
            KFS_LOG_STREAM_INFO <<
                "Chunk version mismatch (got: " <<
                    chunkVersion << ", expect: " << vers << ")" <<
            KFS_LOG_EOM;
            return -KFS::EBADCKSUM;
        }
        return 0;
    }

    int64_t GetFsId() const {
        int64_t id = -1;
        if (memcmp(filename,
                kKfsChunkFsIdPrefix, kKfsChunkFsIdPrefixLength) == 0) {
            memcpy(&id, filename + kKfsChunkFsIdPrefixLength, sizeof(id));
        }
        return id;
    }

    void SetFsId(int64_t id) {
        if (id <= 0) {
            memset(filename, 0, CHUNK_META_MAX_FILENAME_LEN);
            return;
        }
        size_t pos = 0;
        memcpy(filename + pos, kKfsChunkFsIdPrefix, kKfsChunkFsIdPrefixLength);
        pos += kKfsChunkFsIdPrefixLength;
        memcpy(filename + pos, &id, sizeof(id));
        pos += sizeof(id);
        memset(filename + pos, 0, CHUNK_META_MAX_FILENAME_LEN - pos);
    }

    uint32_t metaMagic;
    uint32_t metaVersion;

    uint64_t fileId;
    uint64_t chunkId;
    uint64_t chunkVersion;
    uint64_t chunkSize; 
    uint32_t chunkBlockChecksum[MAX_CHUNK_CHECKSUM_BLOCKS];
    // some statistics about the chunk: 
    // -- version # has an estimate of the # of writes
    // -- track the # of reads
    // ...
    uint32_t numReads;
    char     filename[CHUNK_META_MAX_FILENAME_LEN];
    uint32_t unused;    // legacy padding
} __attribute__ ((__packed__));

BOOST_STATIC_ASSERT(sizeof(DiskChunkInfo_t) == 4400);
BOOST_STATIC_ASSERT(sizeof(DiskChunkInfo_t) < KFS_CHUNK_HEADER_SIZE);
BOOST_STATIC_ASSERT(
    sizeof(int64_t) + kKfsChunkFsIdPrefixLength <= CHUNK_META_MAX_FILENAME_LEN);

// This structure is in-core
struct ChunkInfo_t
{
    ChunkInfo_t()
        : fileId(0),
          chunkId(0),
          chunkVersion(0),
          chunkSize(0), 
          chunkBlockChecksum(0)
        {}

    ~ChunkInfo_t() {
        delete [] chunkBlockChecksum;
    }

    void Init(kfsFileId_t f, kfsChunkId_t c, int64_t v) {
        fileId = f;
        chunkId = c;
        chunkVersion = v;
        chunkBlockChecksum = new uint32_t[MAX_CHUNK_CHECKSUM_BLOCKS];
        memset(chunkBlockChecksum, 0, MAX_CHUNK_CHECKSUM_BLOCKS * sizeof(uint32_t));
    }

    bool AreChecksumsLoaded() const {
        return chunkBlockChecksum != 0;
    }

    void UnloadChecksums() {
        delete [] chunkBlockChecksum;
        chunkBlockChecksum = 0;
        KFS_LOG_STREAM_DEBUG <<
            "Unloading chunk checksum for chunk " << chunkId <<
        KFS_LOG_EOM;
    }

    void SetChecksums(const uint32_t* checksums) {
        delete [] chunkBlockChecksum;
        if (! checksums) {
            chunkBlockChecksum = 0;
            return;
        }
        chunkBlockChecksum = new uint32_t[MAX_CHUNK_CHECKSUM_BLOCKS];
        memcpy(chunkBlockChecksum, checksums,
            MAX_CHUNK_CHECKSUM_BLOCKS * sizeof(uint32_t));
    }

    void VerifyChecksumsLoaded() const {
        if (! chunkBlockChecksum) {
            die("Checksums are not loaded!");
        }
    }

    // save the chunk meta-data to the buffer; 
    void Serialize(IOBuffer* dataBuf, int64_t fsid) {
        DiskChunkInfo_t dci(fileId, chunkId, chunkSize, chunkVersion);
        dci.SetFsId(fsid);
        assert(chunkBlockChecksum);
        dci.SetChecksums(chunkBlockChecksum);
        dataBuf->CopyIn(reinterpret_cast<const char*>(&dci), sizeof(dci));
    }

    int Deserialize(const DiskChunkInfo_t& dci, bool validate) {
        if (validate) {
            if (dci.metaMagic != CHUNK_META_MAGIC) {
                KFS_LOG_STREAM_INFO <<
                    "Magic # mismatch (got: " << hex << dci.metaMagic <<
                    ", expect: " << CHUNK_META_MAGIC << ")" << dec <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            if (dci.metaVersion != CHUNK_META_VERSION) {
                KFS_LOG_STREAM_INFO <<
                    "Version # mismatch (got: << " << hex << dci.metaVersion <<
                    ", expect: << " << CHUNK_META_VERSION << ")" << dec <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
        }
        fileId = dci.fileId;
        chunkId = dci.chunkId;
        chunkSize = dci.chunkSize;
        chunkVersion = dci.chunkVersion;

        delete [] chunkBlockChecksum;
        chunkBlockChecksum = new uint32_t[MAX_CHUNK_CHECKSUM_BLOCKS];
        memcpy(chunkBlockChecksum, dci.chunkBlockChecksum,
               MAX_CHUNK_CHECKSUM_BLOCKS * sizeof(uint32_t));
        KFS_LOG_STREAM_DEBUG <<
            "Loading chunk checksum for chunk " << chunkId <<
        KFS_LOG_EOM;

        return 0;
    }

    kfsFileId_t  fileId;
    kfsChunkId_t chunkId;
    kfsSeq_t     chunkVersion;
    int64_t      chunkSize; 
    uint32_t*    chunkBlockChecksum;
private:
    // No copy.
    ChunkInfo_t(const ChunkInfo_t& other);
    ChunkInfo_t& operator=(const ChunkInfo_t& other);
};

class ChunkHeaderBuffer
{
public:
    ChunkHeaderBuffer()
        {}
    char* GetPtr()
        { return reinterpret_cast<char*>(mBuf); }
    static int GetSize()
        { return kChunkHeaderBufferSize; }
private:
    enum
    {
        kChunkHeaderBufferSize =
            (int)(sizeof(DiskChunkInfo_t) + sizeof(uint64_t))
    };
    size_t mBuf[(kChunkHeaderBufferSize + sizeof(size_t) - 1) / sizeof(size_t)];
};

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
    bool&              outReadFlag);

}

#endif // _CHUNKSERVER_CHUNK_H
