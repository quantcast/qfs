//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/09/12
// Author: Sriram Rao
//
// Copyright 2008-2011 Quantcast Corp.
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
// Code for computing 32-bit Adler checksums
//----------------------------------------------------------------------------

#ifndef CHUNKSERVER_CHECKSUM_H
#define CHUNKSERVER_CHECKSUM_H

#include <stdint.h>
#include <vector>
#include "kfsio/IOBuffer.h"

namespace KFS
{
using std::vector;

/// Checksums are computed on 64KB block boundaries.  We use the
/// "rolling" 32-bit Adler checksum algorithm
const uint32_t CHECKSUM_BLOCKSIZE = 65536;
const uint32_t kKfsNullChecksum   = 1;

uint32_t OffsetToChecksumBlockNum(off_t offset);
uint32_t OffsetToChecksumBlockStart(off_t offset);
uint32_t OffsetToChecksumBlockEnd(off_t offset);
uint32_t ChecksumBlocksCombine(uint32_t chksum1, uint32_t chksum2, size_t len2);

/// Call this function if you want checksum computed over CHECKSUM_BLOCKSIZE
/// bytes
uint32_t ComputeBlockChecksum(const IOBuffer* data, size_t len,
    uint32_t chksum = kKfsNullChecksum);
uint32_t ComputeBlockChecksumAt(const IOBuffer* data, int pos, size_t len,
    uint32_t chksum = kKfsNullChecksum);
uint32_t ComputeBlockChecksum(const char* data, size_t len);
uint32_t ComputeBlockChecksum(uint32_t ckhsum, const char* buf, size_t len);

/// Call this function if you want a checksums for a sequence of
/// CHECKSUM_BLOCKSIZE bytes
void AppendToChecksumVector(const IOBuffer& data, size_t len,
    uint32_t* chksum, size_t firstBlockLen, vector<uint32_t>& vec);

inline static vector<uint32_t> ComputeChecksums(const IOBuffer* data, size_t len,
    uint32_t* chksum = 0, size_t firstBlockLen = CHECKSUM_BLOCKSIZE)
{
    vector<uint32_t> ret;
    AppendToChecksumVector(*data, len, chksum, firstBlockLen, ret);
    return ret;
}
vector<uint32_t> ComputeChecksums(
    const char* data, size_t len, uint32_t* chksum = 0);

}

#endif // CHUNKSERVER_CHECKSUM_H
