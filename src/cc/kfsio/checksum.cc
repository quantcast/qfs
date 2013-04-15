//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/09/12
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
// An adaptation of the 32-bit Adler checksum algorithm
//
//----------------------------------------------------------------------------

#include "checksum.h"

#include <algorithm>
#include <vector>
#include <zlib.h>

namespace KFS {

using std::min;
using std::max;
using std::vector;
using std::list;

static inline uint32_t
KfsChecksum(uint32_t chksum, const void* buf, size_t len)
{
    return adler32(chksum, reinterpret_cast<const Bytef*>(buf), len);
}

#ifndef _KFS_NO_ADDLER32_COMBINE

// Copied from adler32.c
// This is needed to make it work with versions 1.2.3 and prior that have a bug
// in adler32_combine.

#define BASE 65521      /* largest prime smaller than 65536 */
#define MOD(a) a %= BASE
#define MOD63(a) a %= BASE

static inline uint32_t
bug_fix_for_adler32_combine(uint32_t adler1, uint32_t adler2, int64_t len2)
{
    unsigned long sum1;
    unsigned long sum2;
    unsigned rem;

    /* for negative len, return invalid adler32 as a clue for debugging */
    if (len2 < 0)
        return 0xffffffffUL;

    /* the derivation of this formula is left as an exercise for the reader */
    MOD63(len2);                /* assumes len2 >= 0 */
    rem = (unsigned)len2;
    sum1 = adler1 & 0xffff;
    sum2 = rem * sum1;
    MOD(sum2);
    sum1 += (adler2 & 0xffff) + BASE - 1;
    sum2 += ((adler1 >> 16) & 0xffff) + ((adler2 >> 16) & 0xffff) + BASE - rem;
    if (sum1 >= BASE) sum1 -= BASE;
    if (sum1 >= BASE) sum1 -= BASE;
    if (sum2 >= (BASE << 1)) sum2 -= (BASE << 1);
    if (sum2 >= BASE) sum2 -= BASE;
    return sum1 | (sum2 << 16);
}

#endif

static inline uint32_t
KfsChecksumCombine(uint32_t chksum1, uint32_t chksum2, size_t len2)
{
#ifndef _KFS_NO_ADDLER32_COMBINE
    return bug_fix_for_adler32_combine(chksum1, chksum2, (int64_t)len2);
#else
    return adler32_combine(chksum1, chksum2, len2);
#endif
}

uint32_t
ChecksumBlocksCombine(uint32_t chksum1, uint32_t chksum2, size_t len2)
{
    return KfsChecksumCombine(chksum1, chksum2, len2);
}

uint32_t
OffsetToChecksumBlockNum(off_t offset)
{
    return offset / CHECKSUM_BLOCKSIZE;
}

uint32_t
OffsetToChecksumBlockStart(off_t offset)
{
    return (offset / CHECKSUM_BLOCKSIZE) *
        CHECKSUM_BLOCKSIZE;
}

uint32_t
OffsetToChecksumBlockEnd(off_t offset)
{
    return ((offset / CHECKSUM_BLOCKSIZE) + 1) *
        CHECKSUM_BLOCKSIZE;
}

uint32_t
ComputeBlockChecksum(const char* buf, size_t len)
{
    return KfsChecksum(kKfsNullChecksum, buf, len);
}

uint32_t
ComputeBlockChecksum(uint32_t ckhsum, const char* buf, size_t len)
{
    return KfsChecksum(ckhsum, buf, len);
}

vector<uint32_t>
ComputeChecksums(const char* buf, size_t len, uint32_t* chksum)
{
    vector <uint32_t> cksums;

    if (len <= CHECKSUM_BLOCKSIZE) {
        uint32_t cks = ComputeBlockChecksum(buf, len);
        if (chksum) {
            *chksum = cks;
        }
        cksums.push_back(cks);
        return cksums;
    }
    if (chksum) {
        *chksum = kKfsNullChecksum;
    }
    cksums.reserve((len + CHECKSUM_BLOCKSIZE - 1) / CHECKSUM_BLOCKSIZE);
    size_t curr = 0;
    while (curr < len) {
        const size_t   tlen = min((size_t) CHECKSUM_BLOCKSIZE, len - curr);
        const uint32_t cks  = ComputeBlockChecksum(buf + curr, tlen);
        if (chksum) {
            *chksum = KfsChecksumCombine(*chksum, cks, tlen);
        }
        cksums.push_back(cks);
        curr += tlen;
    }
    return cksums;
}

uint32_t
ComputeBlockChecksum(const IOBuffer* data, size_t len, uint32_t chksum)
{
    uint32_t res = chksum;
    for (IOBuffer::iterator iter = data->begin();
            len > 0 && (iter != data->end()); ++iter) {
        const size_t tlen = min((size_t) iter->BytesConsumable(), len);
        if (tlen == 0) {
            continue;
        }
        res = KfsChecksum(res, iter->Consumer(), tlen);
        len -= tlen;
    }
    return res;
}

uint32_t
ComputeBlockChecksumAt(
    const IOBuffer* data, int pos, size_t len, uint32_t chksum)
{
    IOBuffer::iterator const end = data->end();
    IOBuffer::iterator       it  = data->begin();
    uint32_t                 res = chksum;
    size_t                   l   = len;
    for (int rem = max(0, pos); 0 < l && it != end; ++it) {
        const int nb = it->BytesConsumable();
        if (rem < nb) {
            const size_t sz = min((size_t)(nb - rem), l);
            res = KfsChecksum(res, it->Consumer() + rem, sz);
            l -= sz;
            rem = 0;
        } else if (nb > 0) {
            rem -= nb;
        }
    }
    return res;
}

void
AppendToChecksumVector(const IOBuffer& data, size_t inlen,
    uint32_t* chksum, size_t firstBlockLen, vector<uint32_t>& cksums)
{
    size_t len = min(inlen, size_t(max(0, data.BytesConsumable())));
    if (len <= firstBlockLen) {
        const uint32_t cks = ComputeBlockChecksum(&data, len);
        if (chksum) {
            *chksum = cks;
        }
        cksums.push_back(cks);
        return;
    }
    if (chksum) {
        *chksum = kKfsNullChecksum;
    }
    IOBuffer::iterator iter = data.begin();
    if (iter == data.end()) {
        return;
    }
    cksums.reserve(cksums.size() + 1 +
        (len - firstBlockLen + CHECKSUM_BLOCKSIZE - 1) / CHECKSUM_BLOCKSIZE);
    const char* buf = iter->Consumer();
    /// Compute checksum block by block
    size_t rem = firstBlockLen;
    while (0 < len && iter != data.end()) {
        size_t   currLen = 0;
        uint32_t res     = kKfsNullChecksum;
        while (currLen < rem) {
            size_t navail = min((size_t) (iter->Producer() - buf), len);
            if (currLen + navail > rem) {
                navail = rem - currLen;
            }
            if (navail == 0) {
                iter++;
                if (iter == data.end()) {
                    break;
                }
                buf = iter->Consumer();
                continue;
            }
            currLen += navail;
            len -= navail;
            res = KfsChecksum(res, buf, navail);
            buf += navail;
        }
        if (chksum) {
            *chksum = KfsChecksumCombine(*chksum, res, currLen);
        }
        cksums.push_back(res);
        rem = CHECKSUM_BLOCKSIZE;
    }
    return;
}

}

