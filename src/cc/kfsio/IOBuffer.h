//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/14
// Author: Sriram Rao
//         Mike Ovsiannikov -- iostream, aligned buffer support for direct IO,
//         scatter / gather io with readv and writev, make IOBuffer generic scatter
//         gather list with *SpaceAvailable* methods.
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
// \brief Scatter / gather KFS I/O.
//
//----------------------------------------------------------------------------

#ifndef _LIBIO_IOBUFFER_H
#define _LIBIO_IOBUFFER_H

#include "common/DisplayData.h"
#include "common/StdAllocator.h"

#include <stdint.h>

#include <list>
#include <streambuf>
#include <ostream>
#include <istream>
#include <limits>

#include <boost/shared_ptr.hpp>

namespace KFS
{
using std::ostream;
using std::istream;
using std::streambuf;
using std::streamsize;
using std::list;
using std::numeric_limits;
using boost::shared_ptr;

class IOBuffer;

namespace libkfsio
{
// IO buffer allocator. Typically used with io buffer pool.
class IOBufferAllocator
{
protected:
    IOBufferAllocator()
        {}
    virtual ~IOBufferAllocator()
        {}
    IOBufferAllocator& operator=(const IOBufferAllocator&)
        { return *this; }
public:
    virtual size_t GetBufferSize() const = 0;
    virtual char*  Allocate()            = 0;
    virtual void   Deallocate(char* buf) = 0;
};

/// API to set the default allocation when allocating
/// IOBufferData().  The default allocation unit is 4K unless
/// changed by this API call.
/// Can only be called once, prior to any buffer allocation.
bool SetIOBufferAllocator(IOBufferAllocator* allocator);

// IO buffer debug.
class IOBufferVerifier
{
protected:
    IOBufferVerifier()
        {}
    virtual ~IOBufferVerifier()
        {}
    IOBufferVerifier& operator=(const IOBufferVerifier&)
        { return *this; }
public:
    virtual void Verify(const IOBuffer& inBuffer, bool inModifiedFlag) = 0;
    virtual void DoRead(const char* inPtr, bool inStartFlag) = 0;
};

bool SetIOBufferVerifier(IOBufferVerifier* verifier);
} // namespace libkfsio

///
/// \class IOBufferData
/// \brief An IOBufferData contains a buffer and associated
/// producer/consumer points.
class IOBufferData
{
public:
    typedef int64_t BufPos;
    /// Data buffer that is ref-counted for sharing.
    typedef shared_ptr<char> IOBufferBlockPtr;

    IOBufferData();
    IOBufferData(BufPos bufsz);
    IOBufferData(char* buf, BufPos offset, BufPos size,
        libkfsio::IOBufferAllocator& allocator);
    IOBufferData(char* buf, BufPos bufSize, BufPos offset, BufPos size);
    IOBufferData(const IOBufferBlockPtr& data, BufPos bufSize,
        BufPos offset, BufPos size);

    /// Create an IOBufferData blob by sharing data block from other;
    /// set the producer/consumer based on the start/end positions
    /// that are passed in
    IOBufferData(const IOBufferData &other, char *s, char *e, char* p = 0);
    ~IOBufferData();

    ///
    /// Read data from file descriptor into the buffer.
    /// @param[in] fd file descriptor to be used for reading.
    /// @result Returns the # of bytes read
    ///
    BufPos Read(int fd, BufPos maxReadAhead /* = -1 */);

    ///
    /// Write data from the buffer to the file descriptor.
    /// @param[in] fd file descriptor to be used for writing.
    /// @result Returns the # of bytes written
    ///
    BufPos Write(int fd);

    ///
    /// Copy data into the buffer.  For doing a copy, data is appended
    /// to the buffer starting at the offset corresponding to
    /// mProducer.  # of bytes copied is min (# of bytes, space
    /// avail), where space avail = mEnd - mProducer.
    ///
    /// NOTE: As a result of copy, the "producer" pointer is not
    /// advanced.
    ///
    /// @param[out] buf A containing the data to be copied in.
    /// @param[in] numBytes # of bytes to be copied.
    /// @retval Returns the # of bytes copied.
    ///
    BufPos CopyIn(const char *buf, BufPos numBytes);
    BufPos CopyIn(const IOBufferData *other, BufPos numBytes);
    ///
    /// Copy data out the buffer.  For doing a copy, data is copied
    /// out of the buffer starting at the offset corresponding to
    /// mConsumer.  # of bytes copied is min (# of bytes, bytes
    /// avail), where bytes avail = mProducer - mConsumer.
    ///
    /// NOTE: As a result of copy, the "consumer" pointer is not
    /// advanced.
    ///
    /// @param[out] buf A containing the data to be copied in.
    /// @param[in] numBytes # of bytes to be copied.
    /// @retval Returns the # of bytes copied.
    ///
    BufPos CopyOut(char *buf, BufPos numBytes) const;

    char *Producer() { return mProducer; }
    char *Consumer() { return mConsumer; }
    const char *Producer() const { return mProducer; }
    const char *Consumer() const { return mConsumer; }

    ///
    /// Some data has been filled in the buffer.  So, advance
    /// mProducer.
    /// @param[in] nbytes # of bytes of data filled
    /// @retval # of bytes filled in this buffer.
    ///
    BufPos Fill(BufPos nbytes);
    BufPos ZeroFill(BufPos nbytes);

    ///
    /// Some data has been consumed from the buffer.  So, advance
    /// mConsumer.
    /// @param[in] nbytes # of bytes of data consumed
    /// @retval # of bytes consumed from this buffer.
    ///
    BufPos Consume(BufPos nbytes);

    ///
    /// Remove some data from the end of the buffer.  So, pull back
    /// mProducer
    /// @param[in] nbytes # of bytes of data to be trimmed
    /// @retval # of bytes in this buffer.
    ///
    BufPos Trim(BufPos nbytes);

    /// Returns the # of bytes available for consumption.
    BufPos BytesConsumable() const { return mProducer - mConsumer; }

    /// Return the space available in the buffer
    size_t SpaceAvailable() const { return mEnd - mProducer; }
    bool IsFull() const { return mProducer >= mEnd; }
    bool IsEmpty() const { return mProducer <= mConsumer; }
    /// Returns true if has whole data buffer.
    bool HasCompleteBuffer() const {
        return (mData.get() == mConsumer &&
            mConsumer + sDefaultBufferSize == mEnd);
    }
    bool IsShared() const {
        return (! mData.unique());
    }
    const char* GetBufferPtr() const {
        return mData.get();
    }
    static BufPos GetDefaultBufferSize() {
        return sDefaultBufferSize;
    }
    /// Detach buffer can only detach non shared buffers. The caller assumes
    /// full responsibility for releasing the buffer correctly. The method
    /// should not be called if and object was created with IOBufferData(const
    ///g IOBufferBlockPtr& data, ...) constructor.
    char* DetachBuffer(bool consumerAtBufferStartFlag);
private:
    IOBufferBlockPtr mData;
    /// Pointers that correspond to the start/end of the buffer
    char*            mEnd;
    /// Pointers into mData that correspond to producer/consumer
    char*            mProducer;
    char*            mConsumer;

    /// Allocate memory and init the pointers.
    inline void Init(char* buf, BufPos bufSize);
    inline void Init(char* buf,
        libkfsio::IOBufferAllocator& allocator);

    inline BufPos MaxAvailable(BufPos numBytes) const;
    inline BufPos MaxConsumable(BufPos numBytes) const;

    static BufPos sDefaultBufferSize;
};


///
/// \class IOBuffer -- scatter gather list.
/// An IOBuffer consists of a list of IOBufferData. It provides
/// API's for reading/writing data to/from the buffer.  Operations on
/// IOBuffer translates to operations on appropriate IOBufferData.
///
class IOBuffer
{
private:
    typedef list<
        IOBufferData,
        StdFastAllocator<IOBufferData>
    > BList;
public:
    typedef IOBufferData::BufPos BufPos;
    typedef BList::const_iterator iterator;
    class Reader
    {
    public:
        virtual int Read(int fd, void* buf, BufPos numRead) = 0;
    protected:
        Reader() {}
        virtual ~Reader() {}
    };

    IOBuffer();
    ~IOBuffer();

    IOBuffer *Clone() const;

    /// Append the IOBufferData block to the list stored in this buffer.
    /// Unlike methods with IOBuffer as argument, this method will not
    /// Consume() or change buf in any way, the underlying buffer will be
    /// shared.
    void Append(const IOBufferData& buf);

    /// Append the contents of ioBuf to this buffer.
    BufPos Append(IOBuffer *ioBuf);

    /// Move data buffers with space available at the end of ioBuf.
    /// @param[in] other  Buffer from which the available space to move
    /// @param[in] numBytes  # of bytes of available space to be used
    /// @retval Returns the # of bytes moved.
    ///
    BufPos MoveSpaceAvailable(IOBuffer* other, BufPos numBytes);
    /// Remove space available at the end of ioBuf.
    ///
    void RemoveSpaceAvailable();
    /// Use available buffer space at the end of "other" buffer.
    /// Copy data, if any, into "other"'s available space, but
    /// do not advance / modify IOBufferData buffer pointers of "other".
    /// @param[in] other  Buffer from which the available space to be used
    /// @param[in] numBytes  # of bytes of available space to be used
    /// @retval Returns the # of bytes used.
    ///
    BufPos UseSpaceAvailable(const IOBuffer* other, BufPos numBytes);
    /// Zero fill the buffer for length
    /// min(numBytes, <space available at the end>).
    /// @param[in] numBytes  # of bytes to be zero-filled.
    /// @retval Returns the # of bytes filled.
    ///
    BufPos ZeroFillSpaceAvailable(BufPos numBytes);
    /// Ensure that at least numBytes, is available.
    /// If more than numBytes is always available do nothing,
    /// otherwise add buffer space to make exactly numBytes available.
    /// @param[in] numBytes size of the available space.
    /// @retval Returns actual available space size.
    ///
    BufPos EnsureSpaceAvailable(BufPos numBytes);


    BufPos Read(int fd, BufPos maxReadAhead, Reader* reader);
    BufPos Read(int fd, BufPos maxReadAhead = -1)
        { return Read(fd, maxReadAhead, 0); }
    BufPos Write(int fd);

    /// Move data from one buffer to another.  This involves (mostly)
    /// shuffling pointers without incurring data copying.
    /// The requirement is that "other" better have as much bytes as
    /// we are trying to move.
    /// @param[in] other  Buffer from which data has to be moved
    /// @param[in] numBytes  # of bytes of data to be moved over
    /// @retval Returns the # of bytes moved.
    ///
    BufPos Move(IOBuffer* other, BufPos numBytes);
    /// Move whole buffer.
    ///
    void Move(IOBuffer *other);
    /// Move data and available space from one buffer to another.
    /// @param[in] other  Buffer from which space has to be moved
    /// @param[in] numBytes  # of bytes of space to be moved over
    /// @retval Returns the # of space moved.
    ///
    BufPos MoveSpace(IOBuffer* other, BufPos numBytes);

    /// Replace data in the range
    /// [offset, offset + min(numBytes, other->BytesConsumable())
    /// The range [BytesConsumable(), offset) is zero filled.
    /// In addition this method has the same effect as other->Consume(numBytes).
    /// @param[in] other  Buffer from which data has to be spliced
    /// @param[in] offset  The offset at which data has to be spliced in
    /// @param[in] numBytes  # of bytes of data to be moved over
    ///
    void Replace(IOBuffer* other, BufPos offset, BufPos numBytes);
    /// Same as Replace, except it ensures that all buffers in the destination
    /// fully utilized: IsFull() && HasCompleteBuffer()
    /// It copies over min(srcBuf->BytesConsumable(), numBytes) into this.
    /// If offset > this->BytesConsumable(), the this is zero filled.
    /// This method "consumes" min(srcBuf->BytesConsumable(), numBytes) from
    /// srcBuf.
    void ReplaceKeepBuffersFull(IOBuffer* srcBuf, BufPos offset,
        BufPos numBytes);

    /// Zero fill the buffer for length numBytes.
    /// @param[in] numBytes  # of bytes to be zero-filled.
    void ZeroFill(BufPos numBytes);

    ///
    /// Copy data into the buffer.  For doing a copy, data is appended
    /// to the last buffer in mBuf.  If the amount of data to be
    /// copied exceeds space in the last buffer, additional buffers
    /// are allocated and copy operation runs to finish.
    ///
    /// NOTE: As a result of copy, the "producer" portion of an
    /// IOBufferData is not advanced.
    ///
    /// @param[in] buf A containing the data to be copied in.
    /// @param[in] numBytes # of bytes to be copied in.
    /// @retval Returns the # of bytes copied.
    ///
    BufPos CopyIn(const char* buf, BufPos numBytes);
    /// Pos must be valid boundary between used and available space.
    BufPos CopyIn(const char* buf, BufPos numBytes, IOBuffer::iterator pos);
    /// Append only to the buffer at the specified position.
    BufPos CopyInOnlyIntoBufferAtPos(const char* buf, BufPos numBytes,
        IOBuffer::iterator pos);

    BufPos Copy(const IOBuffer* buf, BufPos numBytes);

    ///
    /// Copy data out of the buffer.  For doing a copy, data is copied
    /// from the first buffer in mBuf.  If the amount of data to be
    /// copied exceeds what is available in the first buffer, the list
    /// of buffers is walked to copy out data.
    ///
    /// NOTE: As a result of copy, the "consumer" portion of an
    /// IOBufferData is not advanced.
    ///
    /// @param[out] buf A null-terminated buffer containing the data
    /// copied out.
    /// @param[in] bufLen Length of buf passed in.  At most bufLen
    /// bytes are copied out.
    /// @retval Returns the # of bytes copied.
    ///
    BufPos CopyOut(char* buf, BufPos bufLen) const;

    /// Copy the data into buf, or get buffer pointer if the data is
    /// contiguous in one buffer.
    const char* CopyOutOrGetBufPtr(char* buf, BufPos& nbytes) const
    {
        if (nbytes > mByteCount) {
            nbytes = mByteCount;
        }
        if (! mBuf.empty() && mBuf.front().BytesConsumable() >= nbytes) {
            return mBuf.front().Consumer();
        }
        nbytes = CopyOut(buf, nbytes);
        return buf;
    }

    ///
    /// Consuming data in the IOBuffer translates to advancing the
    /// "consumer" point on underlying IOBufferData.  From the head
    /// of the list, the consumer point will be advanced on sufficient
    /// # of buffers.
    /// @retval Returns the # of bytes consumed.
    ///
    BufPos Consume(BufPos nbytes);

    /// Returns the # of bytes that are available for consumption.
    BufPos BytesConsumable() const
        { return mByteCount; }

    /// Trim data from the end of the buffer to nbytes.  This is the
    /// converse of consume, where data is removed from the front of
    /// the buffer.
    BufPos Trim(BufPos nbytes);
    BufPos TrimAndConvertRemainderToAvailableSpace(BufPos numBytes);

    /// Ensures HasCompleteBuffer() returns true for all buffers,
    /// and all buffers possibly except the last one are full.
    void MakeBuffersFull();

    /// Trim at buffer boundary
    void TrimAtBufferBoundaryLeaveOnly(BufPos& offset, BufPos& numBytes);

    /// Searches for a string in the buffer, strstr() equivalent.
    /// @param[in] offset to start search from.
    /// @param[in] str    string to search for.
    /// @retval Returns position of  the beginning of the "str" if found,
    /// or -1 if not.
    BufPos IndexOf(BufPos offset, const char* str) const;

    /// Returns true if buffer has no data.
    bool IsEmpty() const
        { return mByteCount <= 0; }

    /// Zero fill, if needed the last buffer to make it full.
    /// @retval Returns number of bytes added.
    BufPos ZeroFillLast();

    /// Returns bytes available for consumption in the last buffer
    /// @retval # of bytes consumable in the last buffer.
    BufPos BytesConsumableLast() const
        { return (mBuf.empty() ? 0 : mBuf.back().BytesConsumable()); }

    /// Returns available space in the last buffer.
    /// @retval available space in the last buffer.
    BufPos SpaceAvailableLast() const
        { return (mBuf.empty() ? 0 : mBuf.back().SpaceAvailable()); }

    /// Retruns true if the last the buffer is full
    bool IsLastFull() const
        { return mBuf.empty() ? true : mBuf.back().IsFull(); }

    /// Remove all data.
    void Clear();

#ifdef DEBUG_IOBuffer
    static bool IsDebugVerify() { return true; }
#else
    static bool IsDebugVerify() { return false; }
#endif
    /// Buffer list iterator.
    /// Do not modify IOBufferData pointed by the iterator, or its content.
    iterator begin() const { return mBuf.begin(); }
    iterator end()   const { return mBuf.end();   }

    /// Detach first / front buffer, if not shared.
    /// See IOBufferData::DetachBuffer() description for more details.
    char* DetachFrontBuffer(bool fullOrPartialLastBufferFlag);
    /// Debug
    void Verify() const;

    /// This is to create istream ostream with StreamBuffer(iobuffer);
    class StreamBuffer : public streambuf
    {
    public:
        StreamBuffer(
            IOBuffer& iobuf,
            BufPos    maxReadLength  = numeric_limits<BufPos>::max(),
            BufPos    maxWriteLength = numeric_limits<BufPos>::max())
            : streambuf(),
              mMaxReadLength(maxReadLength),
              mWriteRem(maxWriteLength),
              mCur(iobuf.begin()),
              mIoBuf(&iobuf)
            {}
        StreamBuffer()
            : streambuf(),
              mMaxReadLength(0),
              mWriteRem(0),
              mCur(),
              mIoBuf(0)
            {}
        void Reset(BufPos maxReadLength, BufPos maxWriteLength)
        {
            setg(0, 0, 0);
            setp(0, 0);
            if (mIoBuf) {
                mCur = mIoBuf->begin();
                mMaxReadLength = maxReadLength;
                mWriteRem      = maxWriteLength;
            } else {
                mMaxReadLength = 0;
                mWriteRem      = 0;
            }
        }
        void SetReadOnly(IOBuffer* iobuf, BufPos maxReadLength)
        {
            // Make sure that overflow() will always return EOF.
            setg(0, 0, 0);
            setp(0, 0);
            mMaxReadLength = iobuf ? maxReadLength : 0;
            mWriteRem      = 0;
            mIoBuf         = iobuf;
            if (mIoBuf) {
                mCur = mIoBuf->begin();
            }
        }
        void SetWriteOnly(IOBuffer* iobuf, BufPos maxWriteLength)
        {
            // Make sure that underflow() will always return EOF.
            mMaxReadLength = 0;
            mWriteRem      = iobuf ? maxWriteLength : 0;
            mIoBuf         = iobuf;
        }
    protected:
        virtual int underflow();
        virtual int overflow(int c = EOF);
        virtual streamsize xsgetn (char* s, streamsize n);
        virtual streamsize xsputn(const char* s, streamsize n);
    private:
        BufPos    mMaxReadLength;
        BufPos    mWriteRem;
        iterator  mCur;
        IOBuffer* mIoBuf;
    private:
        StreamBuffer(const StreamBuffer&);
        StreamBuffer& operator=(const StreamBuffer&);
    };
    class OStream;
    class IStream;
    class WOStream;
    class DisplayData;
    class ByteIterator
    {
    public:
        ByteIterator(const IOBuffer& buf)
            : mBuf(buf),
              mIt(mBuf.begin()),
              mCur(mIt != mBuf.end() ? mIt->Consumer() : 0),
              mEnd(mIt != mBuf.end() ? mIt->Producer() : 0)
            {}
        const char* Next()
        {
            for (; ;) {
                if (mCur < mEnd) {
                    return mCur++;
                }
                if (! mCur || ++mIt == mBuf.end()) {
                    mCur = 0;
                    mEnd = 0;
                    return mCur;
                }
                mCur = mIt->Consumer();
                mEnd = mIt->Producer();
            }
        }
    private:
        const IOBuffer& mBuf;
        iterator        mIt;
        const char*     mCur;
        const char*     mEnd;
    };
private:
    BList  mBuf;
    BufPos mByteCount;
#ifdef DEBUG_IOBuffer
    unsigned int mDebugChecksum;
#endif
    inline void DebugChecksum(const char* buf, BufPos len);
    inline void DebugChecksum(const IOBufferData& buf);
    inline void DebugChecksum(const IOBuffer& buf, BufPos numBytes);
    inline void DebugVerify() const;
    inline void DebugVerify(bool updateChecksum);

    inline static BList::iterator SplitBufferListAt(BList& buf, BufPos& nBytes);
    inline BList::iterator BeginSpaceAvailable(BufPos* nBytes = 0);
    inline bool IsValidCopyInPos(const IOBuffer::iterator& pos);
    IOBuffer(const IOBuffer& buf);
    IOBuffer& operator=(const IOBuffer& buf);
};

class IOBuffer::OStream :
    public  IOBuffer,
    private IOBuffer::StreamBuffer,
    public  ostream
{
public:
    OStream()
        : IOBuffer(),
          IOBuffer::StreamBuffer(*this, 0),
          ostream(this)
        {}
};

class IOBuffer::WOStream :
    private IOBuffer::StreamBuffer,
    public  ostream
{
public:
    WOStream()
        : IOBuffer::StreamBuffer(),
          ostream(this)
        {}
    ostream& Set(
        IOBuffer* iobuf,
        BufPos    maxWriteLength = numeric_limits<BufPos>::max())
    {
        StreamBuffer::SetWriteOnly(iobuf, maxWriteLength);
        ostream::clear();
        ostream::flags(ostream::dec | ostream::skipws);
        ostream::precision(6);
        ostream::width(0);
        ostream::fill(' ');
        ostream::tie(0);
        ostream::rdbuf(this);
        return *this;
    }
    ostream& Set(
        IOBuffer& iobuf,
        BufPos    maxWriteLength = numeric_limits<BufPos>::max())
        { return Set(&iobuf, maxWriteLength); }
    ostream& Reset()
        { return Set(0, 0); }
};

class IOBuffer::IStream :
    private IOBuffer::StreamBuffer,
    public  istream
{
public:
    IStream(
        IOBuffer& iobuf,
        BufPos    maxReadLength = numeric_limits<BufPos>::max())
        : IOBuffer::StreamBuffer(iobuf, maxReadLength, 0),
          istream(this)
        {}
    IStream()
        : IOBuffer::StreamBuffer(),
          istream(this)
        {}
    void Rewind(BufPos maxReadLength)
    {
        StreamBuffer::Reset(maxReadLength, 0);
        istream::clear();
        istream::rdbuf(this);
    }
    istream& Set(
        IOBuffer* iobuf,
        BufPos    maxReadLength = numeric_limits<BufPos>::max())
    {
        StreamBuffer::SetReadOnly(iobuf, maxReadLength);
        istream::clear();
        istream::flags(ostream::dec | istream::skipws);
        istream::precision(6);
        istream::width(0);
        istream::fill(' ');
        istream::tie(0);
        istream::rdbuf(this);
        return *this;
    }
    istream& Set(
        IOBuffer& iobuf,
        BufPos    maxReadLength = numeric_limits<BufPos>::max())
        { return Set(&iobuf, maxReadLength); }
    istream& Reset()
        { return Set(0, 0); }
};

class IOBuffer::DisplayData
{
public:
    DisplayData(
        const IOBuffer& inBuffer,
        BufPos          inLength = numeric_limits<BufPos>::max())
        : mIOBuffer(inBuffer),
          mLength(Min(inLength, inBuffer.BytesConsumable()))
        {}
    template<typename ST>
    ST& Display(
        ST& inStream) const
    {
        BufPos theRem = mLength;
        for (IOBuffer::iterator theIt = mIOBuffer.begin();
                inStream && 0 < theRem && theIt != mIOBuffer.end();
                ++theIt) {
            const BufPos theCnt = Min(theRem, theIt->BytesConsumable());
            if (0 < theCnt) {
                inStream << KFS::DisplayData(theIt->Consumer(), (size_t)theCnt);
                theRem -= theCnt;
            }
        }
        return inStream;
    }
private:
    const IOBuffer& mIOBuffer;
    BufPos          mLength;

    static BufPos Min(
        BufPos inLfs,
        BufPos inRhs)
        { return (inLfs < inRhs ? inLfs : inRhs); }
};

template<typename ST>
ST& operator<<(
    ST&                          inStream,
    const IOBuffer::DisplayData& inDisplay)
{ return inDisplay.Display(inStream); }

}

#endif // _LIBIO_IOBUFFER_H
