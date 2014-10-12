//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/15
// Author: Sriram Rao
//         Mike Ovsiannikov -- iostream, aligned buffers support for direct IO,
//         scatter / gather io with readv and writev, make IOBuffer generic scatter
//         gather list with *SpaceAvailable* methods.
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
// Scatter / gatherer io list implementation.
//
//----------------------------------------------------------------------------

#include <sys/types.h>
#include <sys/uio.h>
#include <limits.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#include <cerrno>
#include <iostream>
#include <algorithm>

#include "IOBuffer.h"
#include "Globals.h"

namespace KFS
{

using std::min;
using std::max;
using std::list;

using namespace KFS::libkfsio;

// To conserve memory, by default, we allocate IOBufferData in 4K
// blocks.  However, applications are free to change this default unit
// to what they like.
static libkfsio::IOBufferAllocator* sIOBufferAllocator = 0;
static volatile bool sIsIOBufferAllocatorUsed = false;
int IOBufferData::sDefaultBufferSize = 4 << 10;

struct IOBufferArrayDeallocator
{
    void operator()(char* buf) { delete [] buf; }
};

struct IOBufferDeallocator
{
    void operator()(char* buf) { sIOBufferAllocator->Deallocate(buf); }
};

struct IOBufferDeallocatorCustom
{
    IOBufferDeallocatorCustom(
        libkfsio::IOBufferAllocator& allocator)
        : mAllocator(allocator)
        {}
    void operator()(char* buf) { mAllocator.Deallocate(buf); }
private:
    libkfsio::IOBufferAllocator& mAllocator;
};

// Call this function if you want to change the default allocator.
bool
libkfsio::SetIOBufferAllocator(libkfsio::IOBufferAllocator* allocator)
{
    if (sIsIOBufferAllocatorUsed ||
            (allocator && (int)allocator->GetBufferSize() <= 0)) {
        return false;
    }
    sIOBufferAllocator = allocator;
    return true;
}

inline int
IOBufferData::MaxAvailable(int numBytes) const
{
    return max(0, min(int(SpaceAvailable()), numBytes));
}

inline int
IOBufferData::MaxConsumable(int numBytes) const
{
    return max(0, min(BytesConsumable(), numBytes));
}

inline void
IOBufferData::Init(char* buf, int bufSize)
{
    // glibc malloc returns 2 * sizeof(size_t) aligned blocks.
    const int size = max(0, bufSize);
    mData.reset(buf ? buf : new char [size], IOBufferArrayDeallocator());
    mProducer = mData.get();
    mEnd      = mProducer + size;
    mConsumer = mProducer;
}

inline void
IOBufferData::Init(char* buf, libkfsio::IOBufferAllocator& allocator)
{
    if (&allocator == sIOBufferAllocator) {
        if (! sIsIOBufferAllocatorUsed) {
            sDefaultBufferSize = sIOBufferAllocator->GetBufferSize();
        }
        sIsIOBufferAllocatorUsed = true;
        mData.reset(buf ? buf : allocator.Allocate(),
            IOBufferDeallocator());
    } else {
        mData.reset(buf ? buf : allocator.Allocate(),
            IOBufferDeallocatorCustom(allocator));
    }
    if (! (mProducer = mData.get())) {
        abort();
    }
    mEnd      = mProducer + allocator.GetBufferSize();
    mConsumer = mProducer;
}

// setup a new IOBufferData for access by block sharing.
IOBufferData::IOBufferData(const IOBufferData& other,
    char* c, char* e, char* p /* = 0 */)
    : mData(other.mData),
      mEnd(e),
      mProducer(p ? p : e),
      mConsumer(c)
{
    if (! (mData.get() <= mConsumer &&
                mConsumer <= mProducer &&
                mProducer <= mEnd &&
                mEnd <= other.mEnd)) {
        abort();
    }
}

IOBufferData::IOBufferData()
    : mData(),
      mEnd(0),
      mProducer(0),
      mConsumer(0)
{
    if (sIOBufferAllocator) {
        IOBufferData::Init(0, *sIOBufferAllocator);
    } else {
        IOBufferData::Init(0, sDefaultBufferSize);
    }
}

IOBufferData::IOBufferData(int bufsz)
    : mData(),
      mEnd(0),
      mProducer(0),
      mConsumer(0)
{
    IOBufferData::Init(0, bufsz);
}

IOBufferData::IOBufferData(char* buf, int offset, int size, libkfsio::IOBufferAllocator& allocator)
    : mData(),
      mEnd(0),
      mProducer(0),
      mConsumer(0)
{
    IOBufferData::Init(buf, allocator);
    IOBufferData::Fill(offset + size);
    IOBufferData::Consume(offset);
}

IOBufferData::IOBufferData(char* buf, int bufSize, int offset, int size)
    : mData(),
      mEnd(0),
      mProducer(0),
      mConsumer(0)
{
    IOBufferData::Init(buf, bufSize);
    IOBufferData::Fill(offset + size);
    IOBufferData::Consume(offset);
}

IOBufferData::IOBufferData(const IOBufferBlockPtr& data, int bufSize, int offset, int size)
    : mData(data),
      mEnd(0),
      mProducer(0),
      mConsumer(0)
{
    char* const buf = data.get();
    mEnd      = buf + bufSize;
    mProducer = buf;
    mConsumer = buf;
    IOBufferData::Fill(offset + size);
    IOBufferData::Consume(offset);
}

IOBufferData::~IOBufferData()
{
}

int
IOBufferData::ZeroFill(int numBytes)
{
    const int nbytes = MaxAvailable(numBytes);
    memset(mProducer, '\0', nbytes);
    mProducer += nbytes;
    return nbytes;
}

int
IOBufferData::Fill(int numBytes)
{
    const int nbytes = MaxAvailable(numBytes);
    mProducer += nbytes;
    return nbytes;
}

int
IOBufferData::Consume(int numBytes)
{
    const int nbytes = MaxConsumable(numBytes);
    mConsumer += nbytes;
    assert(mConsumer <= mProducer);
    return nbytes;
}

int
IOBufferData::Trim(int numBytes)
{
    const int nbytes = MaxConsumable(numBytes);
    mProducer = mConsumer + nbytes;
    return nbytes;
}

int
IOBufferData::Read(int fd, int maxReadAhead /* = -1 */)
{
    int numBytes = mEnd - mProducer;
    int nread;

    if (maxReadAhead >= 0 && numBytes > maxReadAhead) {
        numBytes = maxReadAhead;
    }
    assert(numBytes >= 0);

    if (numBytes <= 0) {
        return -1;
    }
    nread = read(fd, mProducer, numBytes);

    if (nread > 0) {
        mProducer += nread;
        globals().ctrNetBytesRead.Update(nread);
    }

    return (nread >= 0 ? nread : (errno > 0 ? -errno : nread));
}

int
IOBufferData::Write(int fd)
{
    int numBytes = mProducer - mConsumer;
    int nwrote;

    assert(numBytes >= 0);

    if (numBytes <= 0) {
        return -1;
    }
    nwrote = write(fd, mConsumer, numBytes);

    if (nwrote > 0) {
        mConsumer += nwrote;
        globals().ctrNetBytesWritten.Update(nwrote);
    }

    return (nwrote >= 0 ? nwrote : (errno > 0 ? -errno : nwrote));
}

int
IOBufferData::CopyIn(const char *buf, int numBytes)
{
    const int nbytes = MaxAvailable(numBytes);
    if (buf != mProducer) {
        memmove(mProducer, buf, nbytes);
    }
    mProducer += nbytes;
    return nbytes;
}

int
IOBufferData::CopyIn(const IOBufferData *other, int numBytes)
{
    const int nbytes = MaxAvailable(min(numBytes, other->BytesConsumable()));
    memmove(mProducer, other->mConsumer, nbytes);
    mProducer += nbytes;
    return nbytes;
}

int
IOBufferData::CopyOut(char *buf, int numBytes) const
{
    const int nbytes = MaxConsumable(numBytes);
    memmove(buf, mConsumer, nbytes);
    return nbytes;
}

#ifdef DEBUG_IOBuffer

#include <zlib.h>

inline void
IOBuffer::DebugChecksum(const char* buf, int len)
{
    DebugVerify();
    if (len > 0) {
        mDebugChecksum = adler32(mDebugChecksum, (const Bytef *)buf, len);
    }
}

inline void
IOBuffer::DebugChecksum(const IOBufferData& buf)
{
    DebugVerify();
    const int nb = buf.BytesConsumable();
    if (nb > 0) {
        mDebugChecksum =
            adler32(mDebugChecksum, (const Bytef *)buf.Consumer(), nb);
    }
}

inline void
IOBuffer::DebugChecksum(const IOBuffer& buf, int numBytes)
{
    buf.DebugVerify();
    DebugVerify();
    int rem = numBytes;
    for (iterator i = buf.begin(); rem > 0 && i != buf.end(); ++i) {
        const int nb = min(rem, i->BytesConsumable());
        if (nb <= 0) {
            continue;
        }
        mDebugChecksum =
            adler32(mDebugChecksum, (const Bytef*)i->Consumer(), nb);
        rem -= nb;
    }
}

inline void
IOBuffer::DebugVerify(bool updateChecksum)
{
    int          byteCount = 0;
    unsigned int checksum  = adler32(0L, Z_NULL, 0);
    for (iterator i = begin(); i != end(); ++i) {
        const int nb = i->BytesConsumable();
        if (nb <= 0) {
            continue;
        }
        checksum = adler32(checksum, (const Bytef*)i->Consumer(), nb);
        byteCount += nb;
    }
    if (updateChecksum) {
        mDebugChecksum = checksum;
    }
    if (checksum != mDebugChecksum || byteCount != mByteCount) {
        abort();
    }
}

inline void IOBuffer::DebugVerify() const
{ const_cast<IOBuffer*>(this)->DebugVerify(false); }

void
IOBuffer::Clear()
{
    DebugVerify();
    mBuf.clear();
    mByteCount = 0;
    DebugVerify(true);
}

#else
inline void IOBuffer::DebugChecksum(const char* buf, int len)          {}
inline void IOBuffer::DebugChecksum(const IOBufferData& buf)           {}
inline void IOBuffer::DebugChecksum(const IOBuffer& buf, int numBytes) {}
inline void IOBuffer::DebugVerify(bool updateChecksum)                 {}
inline void IOBuffer::DebugVerify() const                              {}
#endif

IOBuffer::IOBuffer()
    : mBuf(), mByteCount(0)
#ifdef DEBUG_IOBuffer
        , mDebugChecksum(0)
#endif
{
    DebugVerify(true);
}

IOBuffer::~IOBuffer()
{
    DebugVerify();
}

void
IOBuffer::Append(const IOBufferData &buf)
{
    DebugChecksum(buf);
    mBuf.push_back(buf);
    assert(mByteCount >= 0);
    mByteCount += buf.BytesConsumable();
    DebugVerify();
}

int
IOBuffer::Append(IOBuffer *ioBuf)
{
    DebugChecksum(*ioBuf, ioBuf->mByteCount);
    int nBytes = 0;
    BList::iterator it;
    for (it = ioBuf->mBuf.begin(); it != ioBuf->mBuf.end(); ) {
        const int nb = it->BytesConsumable();
        if (nb > 0) {
            mBuf.splice(mBuf.end(), ioBuf->mBuf, it++);
            nBytes += nb;
        } else {
            it = ioBuf->mBuf.erase(it);
        }
    }
    assert(mByteCount >= 0 &&
        ioBuf->mByteCount == nBytes && ioBuf->mBuf.empty());
    ioBuf->mByteCount = 0;
    mByteCount += nBytes;
    ioBuf->DebugVerify(true);
    DebugVerify();
    return nBytes;
}

inline IOBuffer::BList::iterator
IOBuffer::BeginSpaceAvailable(int* nBytes /* = 0 */)
{
    if (! nBytes && IsEmpty()) {
        return mBuf.begin();
    }
    BList::iterator it = mBuf.end();
    while (it != mBuf.begin() && (--it)->IsEmpty()) {
        if (it->IsFull()) {
            it = mBuf.erase(it);
        } else if (nBytes) {
            *nBytes += it->SpaceAvailable();
        }
    }
    if (it != mBuf.end() && it->IsFull()) {
        assert(! it->IsEmpty());
        ++it;
    }
    return it;
}

int
IOBuffer::MoveSpaceAvailable(IOBuffer *other, int numBytes)
{
    other->DebugVerify();
    DebugVerify();
    if (numBytes <= 0) {
        return 0;
    }
    BList&          buf = other->mBuf;
    BList::iterator it  = other->BeginSpaceAvailable();
    int nBytes = numBytes;
    while (it != buf.end() && nBytes > 0) {
        IOBufferData& d = *it;
        const int n = (int)d.SpaceAvailable();
        if (n <= 0) {
            ++it;
            continue;
        }
        if (n <= nBytes) {
            if (d.IsEmpty()) {
                mBuf.splice(mBuf.end(), buf, it++);
            } else {
                char* const p = d.Producer();
                mBuf.push_back(IOBufferData(d, p, p + n, p));
                d = IOBufferData(d, d.Consumer(), p);
                ++it;
            }
            nBytes -= n;
        } else {
            ++it;
            char* const p = d.Producer();
            mBuf.push_back(IOBufferData(d, p, p + nBytes, p));
            const IOBufferData nd(d, p + nBytes, p + n, p + nBytes);
            if (d.IsEmpty()) {
                d = nd;
            } else {
                d = IOBufferData(d, d.Consumer(), p);
                buf.insert(it, nd);
            }
            nBytes = 0;
        }
    }
    other->DebugVerify();
    DebugVerify();
    return (numBytes - nBytes);
}

int
IOBuffer::EnsureSpaceAvailable(int numBytes)
{
    int nBytes = 0;
    BeginSpaceAvailable(&nBytes);
    while (nBytes < numBytes) {
        IOBufferData buf;
        const int nb = buf.SpaceAvailable();
        assert(nb > 0);
        if (nBytes + nb > numBytes) {
            char* const p = buf.Producer();
            mBuf.push_back(IOBufferData(buf, p, p + numBytes - nBytes, p));
            nBytes = numBytes;
        } else {
            mBuf.push_back(buf);
            nBytes += nb;
        }
    }
    return nBytes;
}

void
IOBuffer::RemoveSpaceAvailable()
{
    if (IsEmpty()) {
        Clear();
        return;
    }
    DebugVerify();
    while (! mBuf.empty() && mBuf.back().IsEmpty()) {
        mBuf.pop_back();
    }
    if (! mBuf.empty()) {
        IOBufferData& d = mBuf.back();
        if (! d.IsFull()) {
            d = IOBufferData(d, d.Consumer(), d.Producer());
        }
    }
    DebugVerify();
}

int
IOBuffer::UseSpaceAvailable(const IOBuffer* other, int numBytes)
{
    other->DebugVerify();
    DebugVerify();
    if (numBytes <= 0) {
        return 0;
    }
    const BList&          obuf = other->mBuf;
    BList::const_iterator oit  =
        other->mByteCount <= 0 ? obuf.begin() : obuf.end();
    while (oit != obuf.begin() && (--oit)->IsEmpty())
        {}
    BList::iterator it     = mBuf.begin();
    int             nBytes = numBytes;
    while (oit != obuf.end() && nBytes > 0) {
        int nb = min(nBytes, (int)oit->SpaceAvailable());
        if (nb > 0) {
            char* const p = const_cast<char*>(oit->Producer());
            IOBufferData d(*oit, p, p + nb, p);
            while (it != mBuf.end()) {
                if (it->IsEmpty()) {
                    it = mBuf.erase(it);
                    continue;
                }
                if (nb <= 0) {
                    break;
                }
                const int n = it->Consume(d.CopyIn(&(*it), nb));
                nb -= n;
                nBytes -= n;
            }
            mBuf.insert(it, d);
            nBytes -= nb;
        }
        ++oit;
    }
    while (it != mBuf.end()) {
        if (it->IsEmpty()) {
            it = mBuf.erase(it);
        } else {
            ++it;
        }
    }
    assert(0 <= nBytes && nBytes <= numBytes);
    other->DebugVerify();
    DebugVerify();
    return (numBytes - nBytes);
}

int
IOBuffer::ZeroFillSpaceAvailable(int numBytes)
{
    DebugVerify();
    if (numBytes <= 0) {
        return 0;
    }
    BList::iterator it = BeginSpaceAvailable();
    int nBytes = numBytes;
    while (nBytes > 0 && it != mBuf.end()) {
        nBytes -= it->ZeroFill(nBytes);
        ++it;
    }
    assert(0 <= nBytes && nBytes <= numBytes);
    nBytes = numBytes - nBytes;
    mByteCount += nBytes;
    assert(mByteCount >= 0);
    DebugVerify(true);
    return nBytes;
}

int
IOBuffer::Move(IOBuffer* other, int numBytes)
{
    int nBytes = other->mByteCount;
    if (numBytes >= nBytes) {
        Move(other);
        return nBytes;
    }
    DebugChecksum(*other, numBytes);
    nBytes = numBytes;
    while (! other->mBuf.empty() && nBytes > 0) {
        IOBufferData& s  = other->mBuf.front();
        const int     nb = s.BytesConsumable();
        if (nBytes >= nb) {
            if (nb > 0) {
                mBuf.splice(mBuf.end(), other->mBuf, other->mBuf.begin());
                nBytes -= nb;
            } else {
                other->mBuf.pop_front();
            }
        } else {
            // this is the last buffer being moved; only partial data
            // from the buffer needs to be moved.  do the move by
            // sharing the block (and therby avoid data copy)
            mBuf.push_back(IOBufferData(
                s, s.Consumer(), s.Consumer() + nBytes));
            nBytes -= s.Consume(nBytes);
            assert(nBytes == 0);
        }
    }
    while (! other->mBuf.empty() && other->mBuf.front().IsEmpty()) {
        other->mBuf.pop_front();
    }
    nBytes = numBytes - nBytes;
    assert(mByteCount >= 0 && other->mByteCount >= 0);
    mByteCount += nBytes;
    other->mByteCount -= nBytes;
    other->DebugVerify(true);
    DebugVerify();
    return nBytes;
}

void
IOBuffer::Move(IOBuffer* other)
{
    DebugChecksum(*other, other->mByteCount);
    assert(mByteCount >= 0 && other->mByteCount >= 0);
    mBuf.splice(mBuf.end(), other->mBuf);
    mByteCount += other->mByteCount;
    other->mByteCount = 0;
    other->DebugVerify(true);
    DebugVerify();
}

int
IOBuffer::MoveSpace(IOBuffer* other, int numBytes)
{
    if (numBytes <= 0 || other->mBuf.empty()) {
        return 0;
    }
    int nBytes;
    if (other->mByteCount <= numBytes &&
            ! other->mBuf.back().IsEmpty() &&
             (nBytes = other->mByteCount +
                other->mBuf.back().SpaceAvailable()) <= numBytes) {
        Move(other);
        return nBytes;
    }
    DebugChecksum(*other, numBytes);
    nBytes = numBytes;
    while (! other->mBuf.empty() && nBytes > 0) {
        IOBufferData& s  = other->mBuf.front();
        const int     nb = s.BytesConsumable();
        const int     sa = (int)s.SpaceAvailable();
        const int     st = nb + sa;
        if (nBytes >= st) {
            if (st > 0) {
                mBuf.splice(mBuf.end(), other->mBuf, other->mBuf.begin());
                mByteCount += nb;
                other->mByteCount -= nb;
                nBytes -= st;
            } else {
                other->mBuf.pop_front();
            }
        } else {
            // this is the last buffer being moved; only partial data
            // from the buffer needs to be moved.  do the move by
            // sharing the block (and therby avoid data copy)
            char* const c = s.Consumer();
            mBuf.push_back(IOBufferData(s, c, c + nBytes, c + min(nBytes, nb)));
            s = IOBufferData(s, c + nBytes, c + st, c + max(nBytes, nb));
            const int n = mBuf.back().BytesConsumable();
            other->mByteCount -= n;
            mByteCount += n;
            nBytes = 0;
        }
    }
    assert(mByteCount >= 0 && other->mByteCount >= 0);
    other->DebugVerify(true);
    DebugVerify();
    return (numBytes - nBytes);
}

int
IOBuffer::TrimAndConvertRemainderToAvailableSpace(int numBytes)
{
    DebugVerify();
    assert(0 <= mByteCount);
    if (mByteCount <= numBytes) {
        return mByteCount;
    }
    int nBytes = max(0, numBytes);
    mByteCount = nBytes;
    for (BList::iterator it = mBuf.begin(); it != mBuf.end(); ++it) {
        const int nb = it->BytesConsumable();
        if ((nBytes -= nBytes < nb ? it->Trim(nBytes) : nb) <= 0) {
            break;
        }
    }
    assert(nBytes == 0);
    DebugVerify(true);
    return mByteCount;
}

inline IOBuffer::BList::iterator
IOBuffer::SplitBufferListAt(IOBuffer::BList& buf, int& nBytes)
{
    IOBuffer::BList::iterator iter = buf.begin();
    while (nBytes > 0 && iter != buf.end()) {
        IOBufferData& data = *iter;
        const int nb = data.BytesConsumable();
        if (nb <= 0) {
            iter = buf.erase(iter);
            continue;
        }
        if (nb > nBytes) {
            buf.insert(iter, IOBufferData(
                data, data.Consumer(), data.Consumer() + nBytes));
            nBytes -= data.Consume(nBytes);
            assert(nBytes == 0);
        } else {
            nBytes -= nb;
            ++iter;
        }
    }
    return iter;
}

void
IOBuffer::Replace(IOBuffer* other, int offset, int numBytes)
{
    other->DebugVerify();
    DebugVerify();
    // find the insertion point
    int nBytes = offset;
    BList::iterator iter = SplitBufferListAt(mBuf, nBytes);
    // extend buffer if needed
    if (nBytes > 0) {
        ZeroFill(nBytes);
    }
    // split "other" at numBytes
    nBytes = numBytes;
    BList::iterator const otherEnd =
        SplitBufferListAt(other->mBuf, nBytes);

    // remove min(numBytes, other->BytesCounsumable()) starting from offset:
    // [offset, offset + min(numBytes, other->BytesCounsumable())
    nBytes = numBytes - nBytes;
    other->mByteCount -= nBytes;
    assert(other->mByteCount >= 0);
    while (iter != mBuf.end() && nBytes > 0) {
        IOBufferData& data = *iter;
        nBytes -= data.Consume(nBytes);
        if (data.IsEmpty()) {
            iter = mBuf.erase(iter);
        } else {
            assert(nBytes == 0);
            break;
        }
    }
    mByteCount += nBytes;

    // now, put the thing at insertPt
    mBuf.splice(iter, other->mBuf, other->mBuf.begin(), otherEnd);
    assert(mByteCount >= 0);
    other->DebugVerify(true);
    DebugVerify(true);
}

void
IOBuffer::ReplaceKeepBuffersFull(IOBuffer* srcBuf, int inOffset, int numBytes)
{
    srcBuf->DebugVerify();
    DebugVerify();
    const int offset  = max(0, inOffset);
    const int moveLen = min(max(0, numBytes), srcBuf->mByteCount);
    const int dstLen  = max(mByteCount, offset + moveLen);
    assert(moveLen >= 0 && dstLen >= 0 &&
        mByteCount >= 0 && srcBuf->mByteCount >= moveLen);

    BList&          dst = mBuf;
    BList&          src = srcBuf->mBuf;
    BList::iterator di  = offset == mByteCount ? dst.end() : dst.begin();
    int             off = offset == mByteCount ? offset    : 0;
    while (di != dst.end()) {
        const int nb = di->BytesConsumable();
        if (nb <= 0) {
            di = dst.erase(di);
        } else {
            off += nb;
            if (off >= offset) {
                break;
            }
            ++di;
        }
    }
    int rem = numBytes;
    if (offset > off) {
        int nFill = offset - off;
        if (! dst.empty()) {
            nFill -= dst.back().ZeroFill(nFill);
        }
        while (nFill > 0) {
            dst.push_back(IOBufferData());
            IOBufferData& d = dst.back();
            nFill -= d.ZeroFill(nFill);
        }
        assert(nFill == 0);
        // Fill the last buffer.
        IOBufferData& d = dst.back();
        while (rem > 0 && ! src.empty() && ! d.IsFull()) {
            IOBufferData& s = src.front();
            rem -= s.Consume(d.CopyIn(&s, rem));
            if (s.IsEmpty()) {
                src.pop_front();
            }
        }
        assert(rem == 0 || d.IsFull());
        di = dst.end();
        off = 0;
    } else if ((off -= offset) != 0) {
        assert(di != dst.end());
        off = di->BytesConsumable() - off;
        assert(off >= 0);
    } else {
        // Find the last buffer, and make sure it is full.
        IOBufferData* d = 0;
        if (di != dst.end()) {
            while (di != dst.end() && di->IsEmpty()) {
                di = dst.erase(di);
            }
            if (di != dst.end()) {
                d = &*di;
                ++di;
            }
        } else {
            while (! dst.empty() && dst.back().IsEmpty()) {
                dst.pop_back();
            }
            if (! dst.empty()) {
                d = &dst.back();
            }
        }
        if (d) {
            while (rem > 0 && ! src.empty() && ! d->IsFull()) {
                IOBufferData& s = src.front();
                rem -= s.Consume(d->CopyIn(&s, rem));
                if (s.IsEmpty()) {
                    src.pop_front();
                }
            }
        }
        // Move whole buffers from src to dst if possible.
        while (rem > 0 && ! src.empty()) {
            IOBufferData& s = src.front();
            const int nb = s.BytesConsumable();
            if (nb <= 0) {
                src.pop_front();
                continue;
            }
            if (rem < nb || ! s.HasCompleteBuffer() ||
                    (! s.IsFull() && &s != &src.back())) {
                break;
            }
            if (di != dst.end() && nb != di->BytesConsumable()) {
                break;
            }
            dst.splice(di, src, src.begin());
            if (di != dst.end()) {
                di = dst.erase(di);
                while (di != dst.end() && di->IsEmpty()) {
                    di = dst.erase(di);
                }
            }
            rem -= nb;
        }
    }
    // Replace.
    while (rem > 0 && ! src.empty() && di != dst.end()) {
        IOBufferData* s = &src.front();
        if (s->IsEmpty()) {
            src.pop_front();
            continue;
        }
        int dl;
        while ((dl = di->BytesConsumable()) <= 0 &&
            (di = dst.erase(di)) != dst.end())
        {}
        if (dl <= 0) {
            break;
        }
        // Un-share if needed.
        if (di->IsShared()) {
            BList::iterator in = di;
            IOBufferData fp(*in); // Make a shallow copy.
            *in = IOBufferData(); // Replace with new buffer.
            while ((dl -= fp.Consume(in->CopyIn(fp.Consumer(), dl))) > 0) {
                in = dst.insert(++in, IOBufferData());
            }
            // If more than one buffer was created, then postion to the one
            // at the requested offset.
            while ((dl = di->BytesConsumable()) < off) {
                off -= dl;
                ++di;
                assert(di != dst.end());
            }
            assert(dl > 0);
        }
        assert(dl >= off);
        dl -= off;
        char* d = di->Consumer() + off;
        off = 0;
        if (rem < dl) {
            dl = rem;
        }
        rem -= dl;
        while (dl > 0) {
            const int n = s->Consume(s->CopyOut(d, dl));
            d += n;
            dl -= n;
            while (s->IsEmpty()) {
                src.pop_front();
                if (src.empty()) {
                    dl = 0;
                    break;
                }
                s = &src.front();
            }
        }
        ++di;
    }
    // Append.
    while (rem > 0 && ! src.empty()) {
        IOBufferData& s = src.front();
        if (s.IsEmpty()) {
            src.pop_front();
            continue;
        }
        if (dst.empty() || dst.back().IsFull()) {
            dst.push_back(IOBufferData());
        }
        rem -= s.Consume(dst.back().CopyIn(&s, rem));
    }
    // Clean up left over empty buffers if any.
    for ( ; ! src.empty() && src.front().IsEmpty(); src.pop_front())
    {}
    mByteCount = dstLen;
    srcBuf->mByteCount -= moveLen;
    srcBuf->DebugVerify(true);
    DebugVerify(true);
}

void
IOBuffer::ZeroFill(int numBytes)
{
    if (numBytes <= 0) {
        return;
    }
    DebugVerify();
    BList::iterator it = BeginSpaceAvailable();
    int nBytes = numBytes;
    for (; ;) {
        if (it == mBuf.end()) {
            it = mBuf.insert(it, IOBufferData());
        }
        if ((nBytes -= it->ZeroFill(nBytes)) <= 0) {
            break;
        }
        assert(it->IsFull());
        ++it;
    }
    assert(0 <= mByteCount && nBytes == 0);
    mByteCount += numBytes;
    DebugVerify(true);
}

inline static void*
AllocBuffer(size_t allocSize)
{
    return (sIOBufferAllocator ?
        sIOBufferAllocator->Allocate() : new char[allocSize]);
}

int
IOBuffer::Read(int fd, int maxReadAhead, IOBuffer::Reader* reader)
{
    DebugVerify();
    if (sIOBufferAllocator && ! sIsIOBufferAllocatorUsed) {
        IOBufferData initWithAllocator;
    }
    // Read into available space at the end, if any.
    BList::iterator it = BeginSpaceAvailable();
    const size_t bufSize =
        sIOBufferAllocator ? sIOBufferAllocator->GetBufferSize() :
        IOBufferData::GetDefaultBufferSize();
    if (maxReadAhead > 0 && maxReadAhead <= int(bufSize)) {
        const bool addBufFlag = it == mBuf.end();
        if (addBufFlag) {
            it = mBuf.insert(mBuf.end(), IOBufferData());
        }
        if (it->SpaceAvailable() >= size_t(maxReadAhead)) {
            const int nRd = reader ?
                reader->Read(fd, it->Producer(), maxReadAhead) :
                it->Read(fd, maxReadAhead);
            if (nRd > 0) {
                mByteCount += nRd;
                if (reader) {
                    it->Fill(nRd);
                    globals().ctrNetBytesRead.Update(nRd);
                }
            } else if (addBufFlag) {
                mBuf.erase(it);
            }
            DebugVerify(true);
            return nRd;
        }
    }

    const ssize_t kMaxReadv     = 64 << 10;
    const int     kMaxReadvBufs(kMaxReadv / (4 << 10) + 1);
    const int     maxReadvBufs  = min(reader ? 1 : IOV_MAX,
        min(kMaxReadvBufs, int(kMaxReadv / bufSize + 1)));
    struct iovec  readVec[kMaxReadvBufs];
    ssize_t       totRead = 0;
    ssize_t       maxRead(maxReadAhead >= 0 ?
        maxReadAhead : std::numeric_limits<int>::max());

    while (maxRead > 0) {
        assert(it == mBuf.end() || ! it->IsFull());
        int     nVec    = 0;
        ssize_t numRead = maxRead;
        size_t  nBytes(numRead);
        for (BList::iterator i = it;
                i != mBuf.end() && nBytes > 0 && nVec < maxReadvBufs;
                ++i) {
            IOBufferData& buf = *i;
            const size_t  nb  = min(nBytes, buf.SpaceAvailable());
            if (nb > 0) {
                readVec[nVec].iov_len  = nb;
                readVec[nVec].iov_base = buf.Producer();
                nVec++;
                nBytes -= nb;
            }
        }
        const int allocBegin = nVec;
        for ( ; nBytes > 0 && nVec < maxReadvBufs; nVec++) {
            const size_t nb = min(nBytes, bufSize);
            readVec[nVec].iov_len = nb;
            if (! (readVec[nVec].iov_base = AllocBuffer(bufSize))) {
                if (totRead <= 0 && nVec <= 0) {
                    abort(); // Allocation falure.
                }
                break;
            }
            nBytes -= nb;
        }
        numRead -= nBytes;
        const ssize_t nRd = reader ?
            reader->Read(fd, readVec[0].iov_base, readVec[0].iov_len) :
            readv(fd, readVec, nVec);
        if (nRd < numRead) {
            maxRead = 0; // short read, eof, or error: we're done
        } else if (maxRead > 0) {
            maxRead -= nRd;
            assert(maxRead >= 0);
        }
        numRead = max(ssize_t(0), nRd);
        for ( ; it != mBuf.end() && numRead > 0; ++it) {
            numRead -= it->Fill(numRead);
            if (numRead <= 0) {
                if (it->IsFull()) {
                    ++it;
                }
                break;
            }
        }
        for (int i = allocBegin; i < nVec; i++) {
            char* const buf = reinterpret_cast<char*>(readVec[i].iov_base);
            if (numRead > 0) {
                if (sIOBufferAllocator) {
                    mBuf.push_back(
                        IOBufferData(buf, 0, numRead, *sIOBufferAllocator));
                } else {
                    mBuf.push_back(
                        IOBufferData(buf, bufSize, 0, numRead));
                }
                numRead -= mBuf.back().BytesConsumable();
            } else {
                if (sIOBufferAllocator) {
                    sIOBufferAllocator->Deallocate(buf);
                } else {
                    delete [] buf;
                }
            }
        }
        assert(numRead == 0);
        if (nRd > 0) {
            totRead += nRd;
            globals().ctrNetBytesRead.Update(nRd);
        } else if (totRead == 0 && nRd < 0 &&
                (totRead = reader ?
                    (int)nRd : -(errno == 0 ? EAGAIN : errno)) > 0) {
            totRead = -totRead;
        }
    }
    assert(mByteCount >= 0);
    if (totRead > 0) {
        mByteCount += totRead;
    }
    DebugVerify(true);
    return totRead;
}

int
IOBuffer::Write(int fd)
{
    DebugVerify();
    const int    kMaxWritevBufs      = 32;
    const int    maxWriteBufs        = min(IOV_MAX, kMaxWritevBufs);
    const int    kPreferredWriteSize = 64 << 10;
    struct iovec writeVec[kMaxWritevBufs];
    ssize_t      totWr = 0;

    while (! mBuf.empty()) {
        BList::iterator it;
        int             nVec;
        ssize_t         toWr;
        for (it = mBuf.begin(), nVec = 0, toWr = 0;
                it != mBuf.end() && nVec < maxWriteBufs &&
                    toWr < kPreferredWriteSize;
                ) {
            const int nBytes = it->BytesConsumable();
            if (nBytes <= 0) {
                it = mBuf.erase(it);
                continue;
            }
            writeVec[nVec].iov_base = it->Consumer();
            writeVec[nVec].iov_len  = (size_t)nBytes;
            toWr += nBytes;
            nVec++;
            ++it;
        }
        if (nVec <= 0) {
            assert(it == mBuf.end());
            mBuf.clear();
            break;
        }
        const ssize_t nWr = writev(fd, writeVec, nVec);
        if (nWr == toWr && it == mBuf.end()) {
            mBuf.clear();
        } else {
            ssize_t nBytes = nWr;
            int nb;
            while ((nb = mBuf.front().BytesConsumable()) <= nBytes) {
                nBytes -= nb;
                mBuf.pop_front();
            }
            if (nBytes > 0) {
                nBytes -= mBuf.front().Consume(nBytes);
                assert(nBytes == 0);
            }
        }
        if (nWr > 0) {
            totWr += nWr;
            globals().ctrNetBytesWritten.Update(nWr);
        } else if (totWr <= 0 && (totWr = -(errno == 0 ? EAGAIN : errno)) > 0) {
            totWr = -totWr;
        }
        if (nWr != toWr) {
            break;
        }
    }
    assert(mByteCount >= 0);
    if (totWr > 0) {
        assert(mByteCount >= totWr);
        mByteCount -= totWr;
    }
    DebugVerify(true);
    return totWr;
}

void
IOBuffer::Verify() const
{
#ifdef DEBUG_IOBuffer
    DebugVerify();
#else
    BList::const_iterator it;
    int numBytes = 0;
    for (it = mBuf.begin(); it != mBuf.end(); ++it) {
        numBytes += it->BytesConsumable();
    }
    if (numBytes != mByteCount) {
        abort();
    }
#endif
}

int
IOBuffer::ZeroFillLast()
{
    DebugVerify();
    int nBytes = 0;
    while (! mBuf.empty()) {
        IOBufferData& b = mBuf.back();
        if (b.IsEmpty()) {
            mBuf.pop_back();
        } else {
            nBytes = b.ZeroFill(b.SpaceAvailable());
            break;
        }
    }
    assert(mByteCount >= 0);
    mByteCount += nBytes;
    DebugVerify(true);
    return nBytes;
}

int
IOBuffer::Consume(int numBytes)
{
    DebugVerify();
    if (numBytes >= mByteCount) {
        mBuf.clear();
        const int nBytes = mByteCount;
        mByteCount = 0;
        DebugVerify(true);
        return nBytes;
    }
    int             nBytes = numBytes;
    BList::iterator it     = mBuf.begin();
    while (numBytes > 0 && it != mBuf.end()) {
        nBytes -= it->Consume(nBytes);
        if (it->IsEmpty()) {
            it = mBuf.erase(it);
        } else {
            ++it;
        }
    }
    nBytes = numBytes - nBytes;
    assert(mByteCount >= 0);
    mByteCount -= nBytes;
    DebugVerify(true);
    return nBytes;
}

int
IOBuffer::Trim(int numBytes)
{
    DebugVerify();
    if (mByteCount <= numBytes) {
        return mByteCount;
    }
    if (numBytes <= 0) {
        mBuf.clear();
        mByteCount = 0;
        DebugVerify(true);
        return mByteCount;
    }
    int             nBytes = numBytes;
    BList::iterator iter   = mBuf.begin();
    while (iter != mBuf.end()) {
        const int nb = iter->BytesConsumable();
        if (nb <= 0) {
            iter = mBuf.erase(iter);
        } else {
            if (nb > nBytes) {
                iter->Trim(nBytes);
                if (! iter->IsEmpty()) {
                    ++iter;
                }
                break;
            }
            nBytes -= nb;
            ++iter;
        }
    }
    iter = mBuf.erase(iter, mBuf.end());
    assert(mByteCount >= 0);
    mByteCount = numBytes;
    DebugVerify(true);
    return mByteCount;
}

inline bool
IOBuffer::IsValidCopyInPos(const IOBuffer::iterator& pos)
{
    IOBuffer::iterator cur = pos;
    if (cur != mBuf.begin() && (--cur)->IsEmpty()) {
        return false;
    }
    cur = pos;
    return (cur == mBuf.end() ||
        cur->IsEmpty() || ++cur == mBuf.end() || cur->IsEmpty());
}

int
IOBuffer::CopyIn(const char* buf, int numBytes, IOBuffer::iterator pos)
{
    assert(IsValidCopyInPos(pos));
    if (numBytes <= 0) {
        return 0;
    }
    DebugChecksum(buf, numBytes);
    int nBytes = numBytes;
    for (; ;) {
        if (pos == mBuf.end()) {
            pos = mBuf.insert(mBuf.end(), IOBufferData());
        }
        if ((nBytes -= const_cast<IOBufferData&>(*pos).CopyIn(
                buf + numBytes - nBytes, nBytes)) <= 0) {
            break;
        }
        assert(pos->IsFull());
        ++pos;
    }
    mByteCount += numBytes;
    assert(nBytes == 0 && 0 < mByteCount && ! mBuf.empty());
    DebugVerify(true);
    return numBytes;
}

int
IOBuffer::CopyInOnlyIntoBufferAtPos(
    const char* buf, int numBytes, IOBuffer::iterator pos)
{
    if (numBytes <= 0 || pos == mBuf.end()) {
        return 0;
    }
    assert(! mBuf.empty());
    DebugChecksum(buf, numBytes);
    const int nb = const_cast<IOBufferData&>(*pos).CopyIn(buf, numBytes);
    mByteCount += nb;
    DebugVerify(true);
    return nb;
}

int
IOBuffer::CopyIn(const char *buf, int numBytes)
{
    DebugChecksum(buf, numBytes);
    if (numBytes <= 0) {
        return 0;
    }
    int defaultBufSz;
    if (! sIOBufferAllocator && mBuf.empty() && numBytes >
            (defaultBufSz = IOBufferData::GetDefaultBufferSize()) * 32) {
        IOBufferData bd(
            (numBytes + defaultBufSz - 1) / defaultBufSz * defaultBufSz);
        mByteCount += bd.CopyIn(buf, numBytes);
        assert(mByteCount == numBytes);
        mBuf.push_back(bd);
        DebugVerify(true);
        return mByteCount;
    }
    // Copy into available space at the end, if any.
    BList::iterator it = BeginSpaceAvailable();
    if (it == mBuf.end()) {
        it = mBuf.insert(it, IOBufferData());
    }
    int nBytes = numBytes;
    const char* cur = buf;
    for (; ;) {
        const int nb = it->CopyIn(cur, nBytes);
        cur += nb;
        nBytes -= nb;
        if (nBytes <= 0) {
            break;
        }
        assert(it->IsFull());
        if (++it == mBuf.end()) {
            it = mBuf.insert(it, IOBufferData());
        }
    }
    nBytes = numBytes - nBytes;
    assert(mByteCount >= 0);
    mByteCount += nBytes;
    DebugVerify(true);
    return nBytes;
}

int
IOBuffer::CopyOut(char *buf, int numBytes) const
{
    BList::const_iterator it;
    char* cur    = buf;
    int   nBytes = numBytes;
    if (nBytes > 0) {
        *cur = '\0';
    }
    for (it = mBuf.begin(); nBytes > 0 && it != mBuf.end(); ++it) {
        const int nb = it->CopyOut(cur, nBytes);
        cur += nb;
        nBytes -= nb;
    }
    DebugVerify();
    return (cur - buf);
}

int
IOBuffer::Copy(const IOBuffer* buf, int numBytes)
{
    DebugChecksum(*buf, numBytes);
    int                   rem = numBytes;
    BList::const_iterator it;
    for (it = buf->mBuf.begin(); it != buf->mBuf.end() && rem > 0; ++it) {
        const int nb = min(rem, it->BytesConsumable());
        if (nb <= 0) {
            continue;
        }
        char* const c = const_cast<char*>(it->Consumer());
        mBuf.push_back(IOBufferData(*it, c, c + nb));
        rem -= nb;
    }
    rem = numBytes - rem;
    mByteCount += rem;
    assert(mByteCount >= 0);
    buf->DebugVerify();
    DebugVerify();
    return rem;
}

//
// Clone the contents of an IOBuffer by block sharing
//
IOBuffer*
IOBuffer::Clone() const
{
    DebugVerify();
    IOBuffer* const clone = new IOBuffer();
    BList::const_iterator  it;
    for (it = mBuf.begin(); it != mBuf.end(); ++it) {
        if (! it->IsEmpty()) {
            clone->mBuf.push_back(IOBufferData(*it,
                const_cast<char*>(it->Consumer()),
                const_cast<char*>(it->Producer())));
        }
    }
    assert(mByteCount >= 0);
    clone->mByteCount = mByteCount;
#ifdef DEBUG_IOBuffer
    clone->mDebugChecksum = mDebugChecksum;
#endif
    clone->DebugVerify();
    DebugVerify();
    return clone;
}

void
IOBuffer::MakeBuffersFull()
{
    DebugVerify();
    if (mBuf.empty()) {
        return;
    }
    // Move write data to the start of the buffers, to make it aligned.
    BList buf;
    buf.swap(mBuf);
    while (! buf.empty()) {
        IOBufferData& s  = buf.front();
        const int     nb = s.BytesConsumable();
        if (nb <= 0) {
            buf.pop_front();
            continue;
        }
        if (mBuf.empty() || mBuf.back().IsFull()) {
            if (s.HasCompleteBuffer() && (s.IsFull() || &s == &buf.back())) {
                mBuf.splice(mBuf.end(), buf, buf.begin());
                continue;
            }
            mBuf.push_back(IOBufferData());
        }
        s.Consume(mBuf.back().CopyIn(&s, nb));
    }
    DebugVerify();
}

void
IOBuffer::TrimAtBufferBoundaryLeaveOnly(int& offset, int& numBytes)
{
    // Trim data at the buffer boundary at the beginning.
    DebugVerify();
    int nBytes = offset;
    while (! mBuf.empty()) {
        const int nb = mBuf.front().BytesConsumable();
        if (nb > nBytes) {
            break;
        }
        nBytes -= nb;
        mBuf.pop_front();
    }
    offset -= nBytes;
    // Trim data at the buffer boundary from the end.
    nBytes = max(0, nBytes) + numBytes;
    numBytes = 0;
    for (BList::iterator i = mBuf.begin(); i != mBuf.end(); ) {
        if (nBytes > numBytes) {
            numBytes += i->BytesConsumable();
            ++i;
        } else {
            i = mBuf.erase(i, mBuf.end());
        }
    }
    assert(mByteCount >= 0 && numBytes > 0);
    mByteCount = numBytes;
    DebugVerify(true);
}

int
IOBuffer::IndexOf(int offset, const char* str) const
{
    DebugVerify();
    const char* const     ss     = str ? str : "";
    const int             soff   = max(0, offset);
    int                   nBytes = soff;
    BList::const_iterator it;
    for (it = mBuf.begin(); it != mBuf.end(); ++it) {
        const int nb = it->BytesConsumable();
        if (nb > nBytes) {
            break;
        }
        nBytes -= nb;
    }
    if (*ss == 0) {
        // Nothing to search for.
        return (it != mBuf.end() ? soff : -1);
    }
    int                   off = soff - nBytes;
    const char*           s   = ss;
    int                   idx = -1;
    int                   pbo = -1;
    BList::const_iterator pit;
    while (it != mBuf.end()) {
        const int         nb = it->BytesConsumable();
        const char* const c  = it->Consumer();
        const char*       n  = c + nBytes;
        const char* const e  = c + nb;
        nBytes = 0;
        if (idx >= 0) {
            while (n < e && *s == *n) {
                if (*++s == 0) {
                    // Found.
                    DebugVerify();
                    return idx;
                }
                n++;
            }
            if (n < e) {
                // Start over, from prefix start index plus one.
                s = ss;
                assert(pbo >= 0);
                it     = pit;
                off    = idx - pbo;
                nBytes = pbo + 1;
                pbo    = -1;
                idx    = -1;
                continue;
            }
        } else {
            while (n < e && (n = (const char*)memchr(n, *s, e - n))) {
                const char* const f = n;
                while (*++s != 0 && ++n < e && *n == *s)
                {}
                if (*s == 0) {
                    // Found.
                    DebugVerify();
                    return (off + int(f - c));
                }
                if (n < e) {
                    // Start over, from prefix start index plus one.
                    s = ss;
                    n = f + 1;
                } else {
                    // Prefix start, end of buffer: remember the prefix position.
                    pbo = int(f - c);
                    pit = it;
                    idx = off + pbo;
                }
            }
        }
        off += nb;
        ++it;
    }
    DebugVerify();
    return -1;
}

int
IOBuffer::StreamBuffer::underflow()
{
    if (mMaxReadLength <= 0 || mCur == mIoBuf->end()) {
        return EOF;
    }
    int nb;
    while ((nb = mCur->BytesConsumable()) <= 0) {
        if (++mCur == mIoBuf->end()) {
            return EOF;
        }
    }
    char* const c = const_cast<char*>(mCur->Consumer());
    nb = min(mMaxReadLength, nb);
    setg(c, c, c + nb);
    mMaxReadLength -= nb;
    ++mCur;
    return (*c & 0xFF);
}

int
IOBuffer::StreamBuffer::overflow(int c)
{
    if (c == EOF || ! mIoBuf || mWriteRem <= 0) {
        return EOF;
    }
    char ch(c);
    const int ret = mIoBuf->CopyIn(&ch, 1);
    if (ret <= 0) {
        return EOF;
    }
    mWriteRem -= ret;
    return c;
}

std::streamsize
IOBuffer::StreamBuffer::xsputn(const char* s, std::streamsize n)
{
    if (! mIoBuf || mWriteRem < (int)n) {
        return 0;
    }
    const int ret = mIoBuf->CopyIn(s, int(n));
    if (ret > 0) {
        mWriteRem -= ret;
    }
    return ret;
}

}
