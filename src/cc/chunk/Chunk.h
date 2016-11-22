//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/22
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
const size_t   KFS_CHUNK_HEADER_SIZE               = 16 << 10;
/// Minimum header size, presently used with object store blocks.
const size_t   KFS_MIN_CHUNK_HEADER_SIZE           =  8 << 10;
/// The max # of checksum blocks we have for a given chunk
const uint32_t MAX_CHUNK_CHECKSUM_BLOCKS           =
    CHUNKSIZE /  CHECKSUM_BLOCKSIZE;
/// File name field now used to store file system id.
const size_t   CHUNK_META_MAX_FILENAME_LEN         = 256;
const uint32_t CHUNK_META_MAGIC                    = 0xCAFECAFE;
const uint32_t CHUNK_META_VERSION                  = 0x1;
static const char* const kKfsChunkFsIdPrefix       =
    "\0QFSFsId\xe4\x5e\x23\x0e\x34\x9a\x07\xce";
static size_t const      kKfsChunkFsIdPrefixLength = 16;

// This structure is on-disk
struct DiskChunkInfo_t
{
    enum Flags
    {
        kFlagsNone          = 0,
        kFlagsMinHeaderSize = 1,
    };

    DiskChunkInfo_t(
        kfsFileId_t f, kfsChunkId_t c, int64_t s, kfsSeq_t v, uint32_t cf)
        : metaMagic(CHUNK_META_MAGIC),
          metaVersion(CHUNK_META_VERSION),
          fileId(f),
          chunkId(c),
          chunkVersion(v),
          chunkSize(s),
          numReads(0),
          flags(cf) {
            memset(filename, 0, CHUNK_META_MAX_FILENAME_LEN);
    }

    template <typename T> static T ReverseInt(T intv) {
        if (0 == intv) {
            return intv;
        }
        T val = intv;
        T ret = 0;
        for (size_t i = sizeof(val); ;) {
            ret |= val & T(0xFF);
            if (0 == --i) {
                break;
            }
            ret <<= 8;
            val >>= 8;
        }
        return ret;
    }

    bool IsReverseByteOrder() const {
        return (ReverseInt(CHUNK_META_MAGIC) == metaMagic &&
            ReverseInt(CHUNK_META_VERSION) == metaVersion);
    }

    void SetChecksums(const uint32_t* checksums) {
        memcpy(chunkBlockChecksum, checksums,
            MAX_CHUNK_CHECKSUM_BLOCKS * sizeof(chunkBlockChecksum[0]));
    }

    int Validate() const {
        if (metaMagic != CHUNK_META_MAGIC) {
            KFS_LOG_STREAM_INFO <<
                "magic mismatch: " << hex <<
                " actual: "   << metaMagic <<
                " expected: " << CHUNK_META_MAGIC << dec <<
            KFS_LOG_EOM;
            return -EBADCKSUM;
        }
        if (metaVersion != CHUNK_META_VERSION) {
            KFS_LOG_STREAM_INFO <<
                "chunk header version mismatch:" << hex <<
                " actual: "   << metaVersion <<
                " expected: " << CHUNK_META_VERSION << dec <<
            KFS_LOG_EOM;
            return -EBADCKSUM;
        }
        if ((uint64_t)CHUNKSIZE < chunkSize) {
            KFS_LOG_STREAM_INFO <<
                "invlid chunk size: " << chunkSize <<
            KFS_LOG_EOM;
            return -EBADCKSUM;
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
                "chunkid mismatch:"
                " actual: "   << chunkId <<
                " expected: " << cid <<
            KFS_LOG_EOM;
            return -EBADCKSUM;
        }
        if ((kfsSeq_t)chunkVersion != vers) {
            KFS_LOG_STREAM_INFO <<
                "chunk version mismatch:"
                " actual: "   << chunkVersion <<
                " expected: " << vers <<
            KFS_LOG_EOM;
            return -EBADCKSUM;
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

    void ReverseByteOrder(bool cheksumsReverseFlag = true) {
        metaMagic    = ReverseInt(metaMagic);
        metaVersion  = ReverseInt(metaVersion);
        fileId       = ReverseInt(fileId);
        chunkId      = ReverseInt(chunkId);
        chunkVersion = ReverseInt(chunkVersion);
        chunkSize    = ReverseInt(chunkSize);
        if (cheksumsReverseFlag) {
            for (size_t i = 0; i < MAX_CHUNK_CHECKSUM_BLOCKS; i++) {
                chunkBlockChecksum[i] =
                    ReverseInt(chunkBlockChecksum[i]);
            }
        }
        numReads = ReverseInt(numReads);
        if (memcmp(filename,
                kKfsChunkFsIdPrefix, kKfsChunkFsIdPrefixLength) == 0) {
            int64_t id;
            memcpy(&id, filename + kKfsChunkFsIdPrefixLength, sizeof(id));
            id = ReverseInt(id);
            memcpy(filename + kKfsChunkFsIdPrefixLength, &id, sizeof(id));
        }
        flags = ReverseInt(flags);
    }

    uint32_t metaMagic;
    uint32_t metaVersion;

    uint64_t fileId;
    uint64_t chunkId;
    uint64_t chunkVersion;
    uint64_t chunkSize;
    uint32_t chunkBlockChecksum[MAX_CHUNK_CHECKSUM_BLOCKS];
    uint32_t numReads; // Not used.
    char     filename[CHUNK_META_MAX_FILENAME_LEN];
    uint32_t flags;
} __attribute__ ((__packed__));

BOOST_STATIC_ASSERT(sizeof(DiskChunkInfo_t) == 4400);
BOOST_STATIC_ASSERT(
    sizeof(DiskChunkInfo_t) + sizeof(uint64_t) <= KFS_MIN_CHUNK_HEADER_SIZE);
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
          chunkFlags(0),
          reserved(0),
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
            "unloaded chunk checksum:"
            " file: "   << fileId <<
            " chunk "   << chunkId <<
            " version " << chunkVersion <<
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
            die("checksums are not loaded!");
        }
    }

    // save the chunk meta-data to the buffer;
    void Serialize(IOBuffer* dataBuf, int64_t fsid) {
        DiskChunkInfo_t dci(fileId, chunkId, chunkSize, chunkVersion, chunkFlags);
        dci.SetFsId(fsid);
        assert(chunkBlockChecksum);
        dci.SetChecksums(chunkBlockChecksum);
        dataBuf->CopyIn(reinterpret_cast<const char*>(&dci), sizeof(dci));
    }

    int Deserialize(const DiskChunkInfo_t& dci, bool validate) {
        if (validate) {
            if (dci.metaMagic != CHUNK_META_MAGIC) {
                KFS_LOG_STREAM_ERROR <<
                    "chunk header magic mismatch:" << hex <<
                    " actual: "   << dci.metaMagic <<
                    " expected: " << CHUNK_META_MAGIC << dec <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            if (dci.metaVersion != CHUNK_META_VERSION) {
                KFS_LOG_STREAM_ERROR <<
                    "chunk header version mismatch:" << hex <<
                    " actual: "   << dci.metaVersion <<
                    " expected: " << CHUNK_META_VERSION << dec <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
        }
        fileId = dci.fileId;
        chunkId = dci.chunkId;
        chunkSize = dci.chunkSize;
        chunkVersion = dci.chunkVersion;
        chunkFlags = dci.flags;

        delete [] chunkBlockChecksum;
        chunkBlockChecksum = new uint32_t[MAX_CHUNK_CHECKSUM_BLOCKS];
        memcpy(chunkBlockChecksum, dci.chunkBlockChecksum,
               MAX_CHUNK_CHECKSUM_BLOCKS * sizeof(uint32_t));
        KFS_LOG_STREAM_DEBUG <<
            "loaded chunk checksums:"
            " file: "     << fileId <<
            " chunk: "    << chunkId <<
            " version: "  << chunkVersion <<
        KFS_LOG_EOM;
        return 0;
    }

    void SetMinHeaderSize(bool flag) {
        if (flag) {
            chunkFlags |= DiskChunkInfo_t::kFlagsMinHeaderSize;
        } else {
            chunkFlags &= ~((uint32_t)DiskChunkInfo_t::kFlagsMinHeaderSize);
        }
    }

    size_t GetHeaderSize() const {
        return ((chunkFlags & DiskChunkInfo_t::kFlagsMinHeaderSize) == 0 ?
            KFS_CHUNK_HEADER_SIZE : KFS_MIN_CHUNK_HEADER_SIZE);
    }

    kfsFileId_t  fileId;
    kfsChunkId_t chunkId;
    kfsSeq_t     chunkVersion;
    int64_t      chunkSize;
    uint32_t     chunkFlags;
    uint32_t     reserved;
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

static inline size_t GetChunkHeaderSize(kfsSeq_t chunkVersion) {
    return (chunkVersion < 0 ?
        KFS_MIN_CHUNK_HEADER_SIZE : KFS_CHUNK_HEADER_SIZE);
}

}
#endif // _CHUNKSERVER_CHUNK_H
