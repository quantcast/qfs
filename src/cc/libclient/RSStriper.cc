//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2010/07/27
// Author: Mike Ovsiannikov
//
// Copyright 2010-2012 Quantcast Corp.
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

#include "RSStriper.h"
#include "Writer.h"
#include "ECMethod.h"

#include "kfsio/IOBuffer.h"
#include "kfsio/checksum.h"

#include "common/MsgLogger.h"
#include "common/StBuffer.h"

#include "qcdio/qcdebug.h"
#include "qcdio/QCDLList.h"
#include "qcdio/QCUtils.h"

#include <sstream>
#include <algorithm>
#include <cerrno>
#include <set>
#include <string.h>
#include <boost/static_assert.hpp>

namespace KFS
{
namespace client
{
using std::min;
using std::max;
using std::string;
using std::ostringstream;

// Stripe iterator / positioning logic used for both read and write striped
// files io. The main goal is to avoid division (and to lesser extend
// multiplication) if possible.
class RSStriper
{
public:
    typedef int64_t Offset;

    enum
    {
        kErrorNone              = 0,
        kErrorParameters        = -EINVAL,
        kErrorIO                = -EIO,
        kErrorCanceled          = -EINTR,
        kErrorNoEntry           = -ENOENT,
        kErrorInvalChunkSize    = -EINVALCHUNKSIZE,
        kErrorInvalidChunkSizes = -(10000 + EINVAL)
    };

    static bool Validate(
        int     inType,
        int     inStripeCount,
        int     inRecoveryStripeCount,
        int     inStripeSize,
        string* outErrMsgPtr = 0)
    {
        if (inType == KFS_STRIPED_FILE_TYPE_NONE) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "invalid striper type";
            }
            return false;
        }
        const bool theRet = ValidateStripeParameters(
                inType, inStripeCount, inRecoveryStripeCount, inStripeSize) &&
            (inRecoveryStripeCount <= 0 ||
            ECMethod::IsValid(
                inType, inStripeCount, inRecoveryStripeCount, outErrMsgPtr));
        if (! theRet && outErrMsgPtr && outErrMsgPtr->empty()) {
            ostringstream theErrStream;
            theErrStream << "invalid parameters:"
                " striper: "               << inType <<
                " stripe count: "          << inStripeCount <<
                " recovery stripe count: " << inRecoveryStripeCount <<
                " stripe size: "           << inStripeSize
            ;
            *outErrMsgPtr = theErrStream.str();
        }
        return theRet;
    }
    RSStriper(
        int           inStripeSize,
        int           inStripeCount,
        int           inRecoveryStripeCount,
        const string& inLogPrefix)
        : mLogPrefix(inLogPrefix),
          mStripeSize(inStripeSize),
          mStripeCount(inStripeCount),
          mRecoveryStripeCount(inRecoveryStripeCount),
          mStrideSize(inStripeSize * inStripeCount),
          mChunkBlockSize((Offset)CHUNKSIZE * mStripeCount),
          mChunkBlockTotalSize(
            (Offset)CHUNKSIZE * (mStripeCount + mRecoveryStripeCount)),
          mPos(0),
          mChunkBlockPos(0),
          mStripePos(0),
          mStripeIdx(0),
          mChunkBlockStartFilePos(0),
          mFilePos(0),
          mTempBufAllocPtr(0),
          mTempBufPtr(0),
          mBufPtr(inRecoveryStripeCount > 0 ?
            new void*[inStripeCount + inRecoveryStripeCount] : 0)
        {}
    virtual ~RSStriper()
    {
        delete [] mTempBufAllocPtr;
        delete [] mBufPtr;
    }
    void SetPos(
        Offset inPos)
    {
        QCRTASSERT(inPos >= 0);
        if (mPos == inPos) {
            return;
        }
        mPos                    = inPos;
        mChunkBlockPos          = mPos % mChunkBlockSize;
        mStripePos              = mChunkBlockPos % mStripeSize;
        mStripeIdx              = (mChunkBlockPos / mStripeSize) % mStripeCount;
        mChunkBlockStartFilePos = mPos / mChunkBlockSize * mChunkBlockTotalSize;
        mFilePos                =
            mChunkBlockStartFilePos +
            mChunkBlockPos / mStrideSize * mStripeSize +
            mStripeIdx * (Offset)CHUNKSIZE +
            mStripePos;
    }
    bool SeekStripe(
        Offset inCount)
    {
        QCRTASSERT(inCount >= 0 && mStripePos + inCount <= mStripeSize);
        mPos           += inCount;
        mChunkBlockPos += inCount;
        mStripePos     += inCount;
        mFilePos       += inCount;
        if (mStripePos < mStripeSize) {
            return false;
        }
        QCASSERT(mStripePos == mStripeSize);
        mStripePos = 0;
        mFilePos += (Offset)CHUNKSIZE - mStripeSize;
        if (++mStripeIdx < mStripeCount) {
            return false;
        }
        mStripeIdx = 0;
        mFilePos -= mChunkBlockSize - mStripeSize;
        if (mChunkBlockPos >= mChunkBlockSize) {
            mChunkBlockPos = 0;
            mFilePos += mChunkBlockTotalSize - (Offset)CHUNKSIZE;
            mChunkBlockStartFilePos = mFilePos;
            QCASSERT(mFilePos % mChunkBlockTotalSize == 0);
            return true; // Next chunk block start.
        }
        return false;
    }
    bool Seek(
        Offset inCount)
    {
        if (inCount >= 0 && mStripePos + inCount <= mStripeSize) {
            return SeekStripe(inCount);
        }
        SetPos(mPos + inCount);
        return true;
    }
    int GetStripeRemaining() const
        { return (int)(mStripeSize - mStripePos); }
    Offset GetPos() const
        { return mPos; }
    Offset GetChunkBlockPos() const
        { return mChunkBlockPos; }
    Offset GetStripePos() const
        { return mStripePos; }
    int GetStripeIdx() const
        { return (int)mStripeIdx; }
    Offset GetFilePos() const
        { return mFilePos; }
    Offset GetChunkBlockStartFilePos() const
        { return mChunkBlockStartFilePos; }
    int GetNextStripeIdx(
        int inIdx) const
    {
        QCASSERT(inIdx >= 0 && inIdx < mStripeCount);
        return ((inIdx + 1 >= mStripeCount) ? 0 : inIdx + 1);
    }
    Offset GetChunkSize(
        int    inStripeIdx,
        Offset inBlockPos,
        Offset inFileSize)
    {
        const Offset kChunkSize = (Offset)CHUNKSIZE;
        if (inFileSize < 0 || inStripeIdx < 0 || inBlockPos < 0) {
            return kChunkSize;
        }
        QCASSERT(
            inStripeIdx < mStripeCount + mRecoveryStripeCount &&
            inBlockPos % mChunkBlockSize == 0
        );
        const Offset theSize = inFileSize - inBlockPos;
        if (theSize <= 0 || theSize >= mChunkBlockSize) {
            return kChunkSize;
        }
        const Offset theStrideCount = theSize / mStrideSize;
        const Offset theStrideHead  = theSize % mStrideSize;
        const Offset theStripeIdx   = theStrideHead / mStripeSize;
        const int    theIdx         =
            inStripeIdx < mStripeCount ? inStripeIdx : 0;
        Offset theChunkSize = theStrideCount * mStripeSize;
        if (theIdx < theStripeIdx) {
            theChunkSize += mStripeSize;
        } else if (theIdx == theStripeIdx) {
            theChunkSize += theStrideHead % mStripeSize;
        }
        QCASSERT(theChunkSize <= kChunkSize);
        return theChunkSize;
    }
    static int GetChunkPos(
        Offset inPos)
        { return (int)(inPos % (Offset)CHUNKSIZE); }
    static IOBufferData NewDataBuffer(
        int inSize)
    {
        const unsigned int kPtrAlign   = kAlign;
        const char* const  kNullPtr    = 0;
        char* const        thePtr      = new char [inSize + kPtrAlign];
        const unsigned int thePtrAlign =
            (unsigned int)(thePtr - kNullPtr) % kPtrAlign;
        const int          theOffset   = 0 < thePtrAlign ?
            (int)(kPtrAlign - thePtrAlign) : 0;
        return IOBufferData(thePtr, inSize + theOffset, theOffset, 0);
    }
    static void InternalError(
            const char* inMsgPtr = 0)
    {
        if (inMsgPtr) {
            KFS_LOG_STREAM_FATAL << inMsgPtr << KFS_LOG_EOM;
        }
        MsgLogger::Stop();
        abort();
    }

    enum { kAlign = 16 };
    BOOST_STATIC_ASSERT(
        KFS_STRIPE_ALIGNMENT % kAlign == 0);
    enum { kMaxRecoveryStripes = KFS_MAX_RECOVERY_STRIPE_COUNT };
    typedef int16_t StripeIdx;
    BOOST_STATIC_ASSERT(
        (KFS_MAX_DATA_STRIPE_COUNT + KFS_MAX_RECOVERY_STRIPE_COUNT) <=
        (1 << (sizeof(StripeIdx) * 8 - 1)) - 1
    );

    const string mLogPrefix;
    const int    mStripeSize;
    const int    mStripeCount;
    const int    mRecoveryStripeCount;
    const int    mStrideSize;
    const Offset mChunkBlockSize;
    const Offset mChunkBlockTotalSize;

private:
    // Stripe iterator.
    Offset       mPos;
    Offset       mChunkBlockPos;
    Offset       mStripePos;
    Offset       mStripeIdx;
    Offset       mChunkBlockStartFilePos;
    Offset       mFilePos; // Meta server / linear file position.
    char*        mTempBufAllocPtr;
    char*        mTempBufPtr;

protected:
    void** const mBufPtr;

    enum { kTempBufSize = kAlign * 16 };

    char* GetTempBufSelfPtr(
        int inIndex,
        int inBufsCount)
    {
        if (! mTempBufAllocPtr) {
            const size_t       theSize   = kTempBufSize * inBufsCount;
            const unsigned int kPtrAlign = kAlign;
            mTempBufAllocPtr = new char [theSize + kPtrAlign];
            const char* const kNullPtr = 0;
            mTempBufPtr      = mTempBufAllocPtr + (kPtrAlign -
                (unsigned int)(mTempBufAllocPtr - kNullPtr) % kPtrAlign);
            memset(mTempBufPtr, 0, theSize);
        }
        QCASSERT(0 <= inIndex && inIndex < inBufsCount);
        return (mTempBufPtr + inIndex * kTempBufSize);
    }
};
const char* const kNullCharPtr = 0;

// Striped files with and without Reed-Solomon recovery writer implementation.
class RSWriteStriper : public Writer::Striper, private RSStriper
{
public:
    typedef RSStriper::Offset Offset;

    static Striper* Create(
        int                      inType,
        int                      inStripeCount,
        int                      inRecoveryStripeCount,
        int                      inStripeSize,
        Writer::Striper::Offset  inFileSize,
        const string&            inLogPrefix,
        Impl&                    inOuter,
        Writer::Striper::Offset& outOpenChunkBlockSize,
        string&                  outErrMsg)
    {
        if (! Validate(
                inType,
                inStripeCount,
                inRecoveryStripeCount,
                inStripeSize,
                &outErrMsg)) {
            return 0;
        }
        ECMethod::Encoder* const theEncoderPr = 0 < inRecoveryStripeCount ?
            ECMethod::FindEncoder(
                inType, inStripeCount, inRecoveryStripeCount, &outErrMsg) : 0;
        if (0 < inRecoveryStripeCount && ! theEncoderPr) {
            return 0;
        }
        outOpenChunkBlockSize =
            Offset(CHUNKSIZE) * (inStripeCount + inRecoveryStripeCount);
        return new RSWriteStriper(
            inStripeSize,
            inStripeCount,
            inRecoveryStripeCount,
            inFileSize,
            inLogPrefix,
            inOuter,
            theEncoderPr
        );
    }
    virtual ~RSWriteStriper()
    {
        delete [] mBuffersPtr;
        if (mEncoderPtr) {
            mEncoderPtr->Release();
        }
    }
    virtual int Process(
        IOBuffer& inBuffer,
        Offset&   ioOffset,
        int       inWriteThreshold)
    {
        const int theSize = inBuffer.BytesConsumable();
        if (ioOffset < 0 && theSize > 0) {
            return kErrorParameters;
        }
        if (mRecoveryStripeCount > 0 && ioOffset != mOffset) {
            if (theSize > 0 &&
                    (mRecoveryEndPos < mOffset ||
                    ioOffset % mStrideSize != 0)) {
                NotSupported(ioOffset, theSize,
                    "non sequential unaligned write");
                return kErrorParameters;
            }
            Flush(0);
            QCASSERT(mPendingCount == 0);
            mOffset         = ioOffset;
            mRecoveryEndPos = mOffset;
        }
        while (! inBuffer.IsEmpty()) {
            mOffset = Stripe(inBuffer, ioOffset);
            ioOffset = mOffset;
            if (mOffset > mFileSize) {
                mFileSize = mOffset;
            }
            if (! ComputeRecovery()) {
                return kErrorIO;
            }
            if (inBuffer.IsEmpty()) {
                break;
            }
            KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                " end chunk block: " << mOffset <<
                " rem: "             << inBuffer.BytesConsumable() <<
            KFS_LOG_EOM;
            // Flush at the end of chunk block.
            QCRTASSERT(
                ioOffset % (CHUNKSIZE * mStripeCount) == 0 &&
                mOffset == mRecoveryEndPos
            );
            for (int i = 0; i < mStripeCount + mRecoveryStripeCount; i++) {
                Write(mBuffersPtr[i]);
            }
        }
        if (mOffset - mRecoveryEndPos < max(1, inWriteThreshold)) {
            Flush(inWriteThreshold);
            return 0;
        }
        QCASSERT(mRecoveryStripeCount > 0);
        if (mOffset < mFileSize) {
            NotSupported(ioOffset, theSize,
                "non sequential unaligned write/flush");
            return kErrorParameters;
        }
        // Zero padd to full stride, and compute recovery.
        QCASSERT(mOffset - mRecoveryEndPos < mStrideSize);
        const int thePaddSize = (int)(mStrideSize - mOffset % mStrideSize);
        int       theAlign    = (int)(mOffset % kAlign);
        const int theBufSize  =
            min(thePaddSize, ((4 << 10) + kAlign - 1) / kAlign * kAlign);
        IOBufferData theBuf = NewDataBuffer(theBufSize);
        theBuf.ZeroFill(theBufSize);
        SetPos(mOffset);
        const int theRecoveryPadd =
            GetStripeIdx() > 0 ? 0 : GetStripeRemaining();
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            " pos: "      << mOffset <<
            " padd: "     << thePaddSize <<
            " rec padd: " << theRecoveryPadd <<
        KFS_LOG_EOM;
        IOBuffer theIoBuf;
        for (int theRem = thePaddSize; theRem > 0; ) {
            theIoBuf.Append(theBuf);
            if (theAlign > 0) {
                theIoBuf.Consume(theAlign);
                theAlign = 0;
            }
            theIoBuf.Trim(theRem);
            theRem -= theIoBuf.BytesConsumable();
            mOffset = Stripe(theIoBuf, mOffset);
            QCASSERT(theIoBuf.IsEmpty());
        }
        int theFrontTrim = 0;
        if (! ComputeRecovery(&theFrontTrim)) {
            return kErrorIO;
        }
        // Undo the padding and write the buffers.
        QCRTASSERT(mOffset == mRecoveryEndPos && mOffset % mStrideSize == 0);
        mOffset         -= thePaddSize;
        mPendingCount   -= thePaddSize;
        mRecoveryEndPos -= mStrideSize;
        int theStrideHead = mStrideSize - thePaddSize;
        QCASSERT(mRecoveryEndPos + theStrideHead == mOffset);
        for (int i = 0; i < mStripeCount; i++) {
            const int theHeadSize = min(theStrideHead, mStripeSize);
            theStrideHead -= theHeadSize;
            const int theBPaddSize = mStripeSize - theHeadSize;
            Buffer&   theBuf = mBuffersPtr[i];
            const int theLen = theBuf.mBuffer.BytesConsumable() - theBPaddSize;
            QCASSERT(theLen >= theHeadSize);
            theBuf.mBuffer.Trim(theLen);
            if (theLen > 0) {
                // Write the buffer, keep only the stride head piece, to handle
                // possible next sequential write.
                theBuf.mEndPos -= theBPaddSize;
                Buffer theWrBuf;
                theWrBuf.mWriteLen = theLen;
                theWrBuf.mEndPos   = theBuf.mEndPos;
                theWrBuf.mBuffer.Move(&theBuf.mBuffer, theLen - theHeadSize);
                if (theHeadSize > 0) {
                    // The stripe head is always aligned, make sure the new
                    // buffer is aligned too.
                    // Copy the data into the new buffer as write request
                    // nomally owns the buffer.
                    IOBufferData theHead = NewDataBuffer(theHeadSize);
                    theHead.Fill(theBuf.mBuffer.CopyOut(
                        theHead.Producer(), theHeadSize));
                    theWrBuf.mBuffer.Move(&theBuf.mBuffer, theHeadSize);
                    QCASSERT(
                        theBuf.mBuffer.IsEmpty() &&
                        theHead.BytesConsumable() == theHeadSize
                    );
                    theBuf.mBuffer.Clear();
                    theBuf.mBuffer.Append(theHead);
                }
                TrimBufferFront(theWrBuf, theFrontTrim, mPendingCount);
                Write(theWrBuf);
                QCASSERT(theWrBuf.mBuffer.IsEmpty());
            } else {
                QCASSERT(theBuf.mBuffer.IsEmpty());
                theBuf.mEndPos = 0;
            }
            theBuf.mWriteLen = 0;
        }
        for (int i = mStripeCount;
                i < mStripeCount + mRecoveryStripeCount;
                i++) {
            Buffer& theBuf = mBuffersPtr[i];
            QCASSERT(
                theBuf.mWriteLen >= theRecoveryPadd &&
                theBuf.mEndPos >= theRecoveryPadd
            );
            theBuf.mWriteLen -= theRecoveryPadd;
            theBuf.mEndPos   -= theRecoveryPadd;
            theBuf.mBuffer.Trim(theBuf.mWriteLen);
            mPendingCount -= theRecoveryPadd;
            Write(theBuf);
            QCASSERT(theBuf.mBuffer.IsEmpty());
        }
        QCASSERT(mPendingCount == 0);
        mLastPartialFlushPos = mOffset;
        return 0;
    }
    virtual Offset GetPendingSize() const
        { return mPendingCount; }
    virtual bool IsWriteRetryNeeded(
        Offset inChunkOffset,
        int    inRetryCount,
        int    inMaxRetryCount,
        int&   ioStatus)
    {
        QCASSERT(inChunkOffset % (Offset)CHUNKSIZE == 0);
        if (inMaxRetryCount < 1) {
            // Do not use chunk invalidation with no retries to prevent
            // data loss in the case of rewrite. If the app. doesn't
            // need retries it can always delete or truncate the file
            // in the case of write failure.
            return true;
        }
        if (mRecoveryStripeCount <= 0) {
            return true;
        }
        if (mWriteFailures.find(inChunkOffset) != mWriteFailures.end()) {
            return false;
        }
        // Retry all but allocation failures, where the chunk being written into
        // is not available (chunk server down or lost chunk).
        if (ioStatus != -EDATAUNAVAIL && inRetryCount < inMaxRetryCount) {
            return true;
        }
        Offset theChunkBlockStart = 0;
        Offset theChunkBlockEnd   = 0;
        int    theFailedCount     = GetWriteFailuresCount(
            inChunkOffset, theChunkBlockStart, theChunkBlockEnd);
        // Allow one failure, after 1 retry.
        if (mRecoveryStripeCount <= theFailedCount ||
                (inRetryCount < inMaxRetryCount && inRetryCount > 0 &&
                (mRecoveryStripeCount > 1 ? 1 : 0) <= theFailedCount)) {
            return true;
        }
        mWriteFailures.insert(inChunkOffset);
        theFailedCount++;
        QCASSERT(theFailedCount > 0);
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "turn off write to chunk"
            " offset: "        << inChunkOffset <<
            " block: [" << theChunkBlockStart <<
                "," << theChunkBlockEnd << ")"
            " failures: "     << theFailedCount <<
            " status: "       << ioStatus <<
            " retry: "        << inRetryCount <<
            " of "            << inMaxRetryCount <<
        KFS_LOG_EOM;
        ioStatus = 0;
        return false;
    }
    virtual Offset GetFileSize() const
        { return mFileSize; }

private:
    struct Buffer
    {
        Buffer()
            : mEndPos(0),
              mBuffer(),
              mCurIt(mBuffer.end()),
              mCurPos(0),
              mWriteLen(0)
            {}
        Offset             mEndPos;
        IOBuffer           mBuffer;
        IOBuffer::iterator mCurIt;
        int                mCurPos;
        int                mWriteLen;
    };
    typedef std::set<
        Offset,
        std::less<Offset>,
        StdFastAllocator<Offset>
    > WriteFailures;

    Offset                   mFileSize;
    Offset                   mPendingCount;
    Offset                   mOffset;
    Offset                   mRecoveryEndPos;
    Offset                   mLastPartialFlushPos;
    WriteFailures            mWriteFailures;
    Buffer* const            mBuffersPtr;
    ECMethod::Encoder* const mEncoderPtr;

    RSWriteStriper(
        int                inStripeSize,
        int                inStripeCount,
        int                inRecoveryStripeCount,
        Offset             inFileSize,
        string             inFilePrefix,
        Impl&              inOuter,
        ECMethod::Encoder* inEncoderPtr)
        : Striper(inOuter),
          RSStriper(
            inStripeSize,
            inStripeCount,
            inRecoveryStripeCount,
            inFilePrefix),
          mFileSize(inFileSize),
          mPendingCount(0),
          mOffset(0),
          mRecoveryEndPos(0),
          mLastPartialFlushPos(0),
          mWriteFailures(),
          mBuffersPtr(new Buffer[inStripeCount + inRecoveryStripeCount]),
          mEncoderPtr(inEncoderPtr)
        {}
    bool IsChunkWriterFailed(
        Offset inOffset) const
    {
        if (mWriteFailures.empty()) {
            return false;
        }
        WriteFailures::const_iterator theIt =
            mWriteFailures.upper_bound(inOffset);
        if (theIt != mWriteFailures.begin()) {
            --theIt;
        }
        return (*theIt <= inOffset && inOffset < *theIt + (Offset)CHUNKSIZE);
    }
    void NotSupported(
        Offset      inOffset,
        int         inSize,
        const char* inMsgPtr)
    {
        KFS_LOG_STREAM_ERROR << mLogPrefix <<
            (inMsgPtr ? inMsgPtr : "io alignment") <<
            " is not supported:" <<
            " offset: "       << inOffset <<
                " (" << inOffset % mStrideSize << ")" <<
            " size: "         << inSize <<
                " (" << inSize % mStrideSize << ")" <<
            " current: "      << mOffset  <<
                " (" << mOffset % mStrideSize << ")" <<
            " sripe: "        << mStripeSize <<
            " stripe count: " << mStripeCount <<
            " file size: "    << mFileSize <<
        KFS_LOG_EOM;
    }
    Offset Stripe(
        IOBuffer& inBuffer,
        Offset    inOffset)
    {
        if (inBuffer.IsEmpty()) {
            return inOffset;
        }
        SetPos(inOffset);
        do {
            Buffer& theBuffer = mBuffersPtr[GetStripeIdx()];
            KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                " pos: "    << GetPos() <<
                " stripe: " << GetStripeIdx() << " " << GetStripePos() <<
                " off: "    << GetFilePos() <<
                " in: "     << inBuffer.BytesConsumable() <<
                " end: "    << theBuffer.mEndPos <<
            KFS_LOG_EOM;
            if (theBuffer.mEndPos != GetFilePos() &&
                    ! theBuffer.mBuffer.IsEmpty()) {
                if (mRecoveryStripeCount > 0) {
                    InternalError("non sequential write is not supported");
                }
                Write(theBuffer); // Flush
            }
            const int theCnt =
                theBuffer.mBuffer.Move(&inBuffer, GetStripeRemaining());
            mPendingCount += theCnt;
            theBuffer.mEndPos = GetFilePos() + theCnt;
            if (SeekStripe(theCnt)) {
                // Always stop and write buffers before moving to the next chunk
                // block. This is needed as the recovery stripes are at the end
                // of the block, thus the recovery has to be computed and
                // written before moving to the next chunk block.
                break;
            }
        } while (! inBuffer.IsEmpty());
        return GetPos();
    }
    void Write(
        Buffer& inBuffer,
        int     inWriteThreshold = 0)
    {
        if (inBuffer.mWriteLen < 0) {
            return;
        }
        const int theSize = inBuffer.mBuffer.BytesConsumable();
        QCASSERT(
            mPendingCount >= theSize &&
            inBuffer.mWriteLen <= theSize &&
            (inBuffer.mEndPos - 1) % (int)CHUNKSIZE >= (theSize - 1)
        );
        const Offset theOffset = inBuffer.mEndPos - theSize;
        if (IsChunkWriterFailed(theOffset)) {
            KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                "discarding: " << inBuffer.mWriteLen << " bytes"
                " at: "        << theOffset <<
                " due to chunk writer failure" <<
            KFS_LOG_EOM;
            const int theDiscCnt = inBuffer.mBuffer.Consume(inBuffer.mWriteLen);
            QCASSERT(theDiscCnt == inBuffer.mWriteLen);
            mPendingCount      -= theDiscCnt;
            inBuffer.mWriteLen -= theDiscCnt;
            return;
        }
        QCRTASSERT(! IsWriteQueued());
        const int theQueuedCount = QueueWrite(
            inBuffer.mBuffer,
            inBuffer.mWriteLen,
            theOffset,
            inWriteThreshold
        );
        QCRTASSERT(
            theQueuedCount <= inBuffer.mWriteLen &&
            inBuffer.mBuffer.BytesConsumable() + theQueuedCount == theSize
        );
        if (theQueuedCount <= 0) {
            return;
        }
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "queued write:"
            " len: "    << inBuffer.mWriteLen <<
            " thresh: " << inWriteThreshold <<
            " queued: " << theQueuedCount <<
        KFS_LOG_EOM;
        inBuffer.mWriteLen -= theQueuedCount;
        mPendingCount -= theQueuedCount;
        StartQueuedWrite(theQueuedCount);
    }
    void Flush(
        int inWriteThreshold)
    {
        const int theThreshold = max(1, max(
            (int)(mOffset - mRecoveryEndPos), inWriteThreshold));
        int theCurThreshold = max(0, inWriteThreshold);
        while (mPendingCount >= theThreshold) {
            for (int i = 0; i < mStripeCount + mRecoveryStripeCount; i++) {
                Write(mBuffersPtr[i], theCurThreshold);
            }
            if (mPendingCount < theThreshold || theCurThreshold <= 0) {
                break;
            }
            theCurThreshold /= (mStripeCount + mRecoveryStripeCount);
        }
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            " flush: thresh:"
            " min: "     << (mOffset - mRecoveryEndPos) <<
            " in: "      << inWriteThreshold <<
            " pending: " << mPendingCount <<
            " cur: "     << theCurThreshold <<
        KFS_LOG_EOM;
    }
    bool ComputeRecovery(
        int* ioPaddSizeWriteFrontTrimPtr = 0)
    {
        if (mRecoveryStripeCount <= 0) {
            for (int i = 0; i < mStripeCount; i++) {
                mBuffersPtr[i].mWriteLen =
                    mBuffersPtr[i].mBuffer.BytesConsumable();
            }
            mRecoveryEndPos = mOffset;
            return true;
        }
        if (! mEncoderPtr) {
            InternalError("no encoder");
            return false;
        }
        if (mOffset < mRecoveryEndPos + mStrideSize) {
            return true; // At least one full stride required.
        }
        QCASSERT(mRecoveryEndPos % mStrideSize == 0);
        int       theStrideHead = (int)(mOffset % mStrideSize);
        const int theTotalSize  =
            (int)((mOffset - theStrideHead) - mRecoveryEndPos);
        const int theSize       = theTotalSize / mStripeCount;
        QCASSERT(theSize * mStripeCount == theTotalSize);
        Offset thePendingCount = 0;
        for (int i = mStripeCount;
                i < mStripeCount + mRecoveryStripeCount;
                i++) {
            IOBufferData theBuf = NewDataBuffer(theSize);
            mBufPtr[i] = theBuf.Producer();
            if (IOBuffer::IsDebugVerify()) {
                thePendingCount += theSize;
            } else {
                theBuf.Fill(theSize);
            }
            if (mBuffersPtr[i].mBuffer.IsEmpty()) {
                const Offset thePos = mBuffersPtr[i - 1].mEndPos + CHUNKSIZE;
                mBuffersPtr[i].mEndPos = thePos;
                if (i == mStripeCount) {
                    mBuffersPtr[i].mEndPos -= thePos % mStripeSize;
                }
                mBuffersPtr[i].mWriteLen = theSize;
            } else {
                mBuffersPtr[i].mWriteLen += theSize;
                mBuffersPtr[i].mEndPos   += theSize;
            }
            mBuffersPtr[i].mBuffer.Append(theBuf);
            thePendingCount += mBuffersPtr[i].mBuffer.BytesConsumable();
        }
        for (int thePos = 0, thePrevLen = 0; thePos < theSize; ) {
            int theLen = theSize - thePos;
            for (int i = 0; i < mStripeCount; i++) {
                IOBuffer&           theBuf  = mBuffersPtr[i].mBuffer;
                IOBuffer::iterator& theIt   = mBuffersPtr[i].mCurIt;
                int&                theSkip = mBuffersPtr[i].mCurPos;
                if (thePos == 0) {
                    const int theTail = min(theStrideHead, mStripeSize);
                    theStrideHead -= theTail;
                    const int theBufSize = theBuf.BytesConsumable();
                    thePendingCount += theBufSize;
                    theSkip = theBufSize - (theSize + theTail);
                    theIt   = theBuf.begin();
                    mBuffersPtr[i].mWriteLen = theSkip + theSize;
                } else {
                    theSkip += thePrevLen;
                }
                int theBufSize;
                while ((theBufSize = theIt->BytesConsumable()) <= theSkip) {
                    theSkip -= theBufSize;
                    ++theIt;
                }
                theBufSize -= theSkip;
                QCRTASSERT(
                    theIt != theBuf.end() &&
                    theSkip >= 0 &&
                    theBufSize > 0
                );
                const char* const thePtr = theIt->Consumer() + theSkip;
                if (theBufSize < kAlign) {
                    char* theDestPtr = GetTempBufPtr(i);
                    mBufPtr[i] = memcpy(theDestPtr, thePtr, theBufSize);
                    theDestPtr += theBufSize;
                    int theRem = kAlign - theBufSize;
                    do {
                        ++theIt;
                        QCASSERT(theIt != theBuf.end());
                        theSkip = theIt->CopyOut(theDestPtr, theRem);
                        theDestPtr += theSkip;
                        theRem -= theSkip;
                    } while (theRem > 0);
                    theLen = kAlign;
                    theSkip -= theLen; // To cancel the thePrevLen addition.
                } else {
                    theBufSize -= theBufSize % kAlign;
                    theLen = min(theLen, theBufSize);
                    if ((thePtr - kNullCharPtr) % kAlign != 0) {
                        theLen = min((int)kTempBufSize, theLen);
                        mBufPtr[i] = memcpy(GetTempBufPtr(i), thePtr, theLen);
                    } else {
                        mBufPtr[i] = const_cast<char*>(thePtr);
                    }
                }
            }
            QCASSERT(theLen > 0 && theLen % kAlign == 0);
            if (thePos == 0 || thePos + theLen == theSize) {
                KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                    " recovery:"
                    " off: " << mRecoveryEndPos <<
                    " pos: " << thePos <<
                    " len: " << theLen <<
                KFS_LOG_EOM;
            }
            const int theStatus = mEncoderPtr->Encode(
                mStripeCount, mRecoveryStripeCount, theLen, mBufPtr);
            if (theStatus != 0) {
                KFS_LOG_STREAM_ERROR << mLogPrefix <<
                    " recovery:"
                    " encode error: " << theStatus <<
                    " off: "          << mRecoveryEndPos <<
                    " pos: "          << thePos <<
                    " len: "          << theLen <<
                KFS_LOG_EOM;
                return false;
            }
            for (int i = mStripeCount;
                    i < mStripeCount + mRecoveryStripeCount;
                    i++) {
                mBufPtr[i] = reinterpret_cast<char*>(mBufPtr[i]) + theLen;
            }
            thePos += theLen;
            thePrevLen = theLen;
        }
        if (IOBuffer::IsDebugVerify()) {
            for (int i = mStripeCount;
                    i < mStripeCount + mRecoveryStripeCount;
                    i++) {
                IOBuffer&          theBuf = mBuffersPtr[i].mBuffer;
                IOBuffer::iterator theIt  = theBuf.end();
                QCVERIFY(
                    theIt-- != theBuf.begin() &&
                    theSize == mBuffersPtr[i].mBuffer.CopyInOnlyIntoBufferAtPos(
                        theIt->Producer(), theSize, theIt
                ));
            }
        }
        mRecoveryEndPos += theTotalSize;
        if (mLastPartialFlushPos + mStrideSize > mRecoveryEndPos) {
            // The partial stride was previously written / flushed.
            int theHead = (int)(
                mLastPartialFlushPos + mStrideSize - mRecoveryEndPos);
            QCRTASSERT(theHead > 0 && theHead < mStrideSize);
            if (ioPaddSizeWriteFrontTrimPtr) {
                *ioPaddSizeWriteFrontTrimPtr = theHead;
                // Do not trim if padded, the caller will do the trimming.
                theHead = 0;
            }
            // Trim only the data buffers, as the recovery stripes may have
            // changed.
            for (int i = 0; i < mStripeCount && theHead > 0; i++) {
                QCASSERT(mBuffersPtr[i].mBuffer.BytesConsumable() >=
                    min(theHead, mStripeSize));
                TrimBufferFront(mBuffersPtr[i], theHead, thePendingCount);
            }
        }
        mPendingCount = thePendingCount;
        return true;
    }
    void TrimBufferFront(
        Buffer& inBuf,
        int&    ioStrideTrim,
        Offset& ioPendingCount)
    {
        if (ioStrideTrim <= 0) {
            return;
        }
        // Try to align the write position to the checksum boundary to avoid
        // read modify write, at the expense of the extra network transfer.
        const int kChecksumBlockSize = (int)CHECKSUM_BLOCKSIZE;
        int theTrim = min(ioStrideTrim, mStripeSize);
        ioStrideTrim -= theTrim;
        const int theChecksumBlockHead = (int)((theTrim +
            inBuf.mEndPos - inBuf.mBuffer.BytesConsumable()) %
            kChecksumBlockSize
        );
        if (theChecksumBlockHead <= theTrim) {
            theTrim -= theChecksumBlockHead;
            if (theTrim <= 0) {
                return;
            }
        }
        theTrim = inBuf.mBuffer.Consume(theTrim);
        inBuf.mWriteLen -= theTrim;
        ioPendingCount  -= theTrim;
        QCASSERT(
            ioPendingCount >= 0 &&
            inBuf.mWriteLen >= 0 &&
            inBuf.mBuffer.BytesConsumable() >= inBuf.mWriteLen
        );
    }
    char* GetTempBufPtr(
        int inIndex)
        { return GetTempBufSelfPtr(inIndex, mStripeCount); }
    int GetWriteFailuresCount(
        Offset  inChunkOffset,
        Offset& outChunkBlockStart,
        Offset& outChunkBlockEnd) const
    {
        outChunkBlockStart =
            inChunkOffset - inChunkOffset % mChunkBlockTotalSize;
        outChunkBlockEnd   =  outChunkBlockStart + mChunkBlockTotalSize;
        int theFailedCount = 0;
        for (WriteFailures::const_iterator theIt =
                        mWriteFailures.lower_bound(outChunkBlockStart);
                    theIt != mWriteFailures.end() && *theIt < outChunkBlockEnd;
                    ++theIt, theFailedCount++)
            {}
        return theFailedCount;
    }
private:
    RSWriteStriper(
        const RSWriteStriper& inRSWriteStriper);
    RSWriteStriper& operator=(
        const RSWriteStriper& inRSWriteStriper);
};

// Striped files with and without Reed-Solomon recovery reader implementation.
// The reader is used by chunk server for RS recovery of both "data" and
// "recovery" chunks.
class RSReadStriper : public Reader::Striper, private RSStriper
{
public:
    typedef RSStriper::Offset Offset;

    static Striper* Create(
        int                      inType,
        int                      inStripeCount,
        int                      inRecoveryStripeCount,
        int                      inStripeSize,
        int                      inMaxAtomicReadRequestSize,
        bool                     inUseDefaultBufferAllocatorFlag,
        bool                     inFailShortReadsFlag,
        Reader::Striper::Offset  inRecoverChunkPos,
        Reader::Striper::Offset  inFileSize,
        Reader::Striper::SeqNum  inInitialSeqNum,
        const string&            inLogPrefix,
        Impl&                    inOuter,
        Reader::Striper::Offset& outOpenChunkBlockSize,
        string&                  outErrMsg)
    {
        if (inMaxAtomicReadRequestSize <= 0) {
            outErrMsg = "invalid max. read request size";
            return 0;
        }
        if (inRecoverChunkPos > 0 && inRecoverChunkPos % CHUNKSIZE != 0) {
            outErrMsg = "invalid chunk recovery position";
            return 0;
        }
        if (! Validate(
                inType,
                inStripeCount,
                inRecoveryStripeCount,
                inStripeSize,
                &outErrMsg)) {
            return 0;
        }
        ECMethod::Decoder* const theDecoderPtr = 0 < inRecoveryStripeCount ?
            ECMethod::FindDecoder(
                inType, inStripeCount, inRecoveryStripeCount, &outErrMsg) : 0;
        if (0 < inRecoveryStripeCount && ! theDecoderPtr) {
            return 0;
        }
        outOpenChunkBlockSize =
            Offset(CHUNKSIZE) * (inStripeCount + inRecoveryStripeCount);
        return new RSReadStriper(
            inStripeSize,
            inStripeCount,
            inRecoveryStripeCount,
            inMaxAtomicReadRequestSize,
            inUseDefaultBufferAllocatorFlag,
            inFailShortReadsFlag,
            inRecoverChunkPos,
            inFileSize,
            inInitialSeqNum,
            inLogPrefix,
            inOuter,
            theDecoderPtr
        );
    }
    virtual ~RSReadStriper()
    {
        Request* thePtr;
        while ((thePtr = Requests::Front(mPendingQueue))) {
            thePtr->Delete(*this, mPendingQueue);
        }
        while ((thePtr = Requests::Front(mFreeList))) {
            thePtr->Delete(*this, mFreeList);
        }
        while ((thePtr = Requests::Front(mInFlightList))) {
            thePtr->Delete(*this, mInFlightList);
        }
        delete [] mBufIteratorsPtr;
        delete mZeroBufferPtr;
        if (mDecoderPtr) {
            mDecoderPtr->Release();
        }
    }
    virtual int Process(
        IOBuffer& inBuffer,
        int       inLength,
        Offset    inOffset,
        RequestId inRequestId)
    {
        if (inLength <= 0) {
            return 0;
        }
        if (inOffset < 0) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "invalid read offset: " << inOffset <<
            KFS_LOG_EOM;
            return kErrorParameters;
        }
        if (mRecoverStripeIdx >= 0) {
            return RecoverChunk(inBuffer, inLength, inOffset, inRequestId);
        }
        // Ensure that all reads in the request including recovery read are
        // "atomic", i.e less or equal than mMaxReadSize.
        SetPos(inOffset);
        int       theMin                = 0;
        int       theMax                = 0;
        Offset    theChunkBlockStartPos = 0;
        int       theLen                = inLength;
        Request*  theRequestPtr         = 0;
        while (theLen > 0) {
            const int theChunkPos = GetChunkPos(GetFilePos());
            if (! theRequestPtr) {
                theRequestPtr = &GetRequest(inRequestId, GetPos(), 0);
                theMin = theChunkPos;
                theMax = theChunkPos;
                theChunkBlockStartPos = GetChunkBlockStartFilePos();
            }
            QCASSERT(theChunkPos <= theMax); // max at stride beginning
            Buffer&   theBuf  = theRequestPtr->GetBuffer(GetStripeIdx());
            const int theRem  = GetStripeRemaining();
            const int theSize = min(
                min(theLen, theRem),
                theChunkPos >= theMin ?
                    theMin + mMaxReadSize - theChunkPos :
                    ((theChunkPos >= theMax - mMaxReadSize) ? mMaxReadSize : 0)
            );
            // Align request on the stripe block boundary if possible.
            if (theSize <= 0 ||
                    (theSize < theRem && theSize < theLen && theMin < theMax)) {
                theRequestPtr->mRecoverySize = -(theMax - theMin);
                theRequestPtr->mRecoveryPos  = theChunkBlockStartPos + theMin;
                QueueRequest(*theRequestPtr);
                theRequestPtr = 0;
                continue;
            }
            if (theBuf.mBuf.mSize <= 0) {
                theBuf.mBuf.mSize = theSize;
                theBuf.mPos       = GetFilePos();
            } else {
                theBuf.mBuf.mSize += theSize;
            }
            theBuf.mBuf.mBuffer.MoveSpaceAvailable(&inBuffer, theSize);
            theLen -= theSize;
            theRequestPtr->mSize         += theSize;
            theRequestPtr->mPendingCount += theSize;
            theMin = min(theMin, theChunkPos);
            theMax = max(theMax, theChunkPos + theSize);
            QCASSERT(theMin < theMax && theMax - theMin <= mMaxReadSize);
            if (SeekStripe(theSize) || theLen <= 0) {
                // Create new request when moving to the next chunk block.
                theRequestPtr->mRecoverySize = -(theMax - theMin);
                theRequestPtr->mRecoveryPos  = theChunkBlockStartPos + theMin;
                QueueRequest(*theRequestPtr);
                theRequestPtr = 0;
                continue;
            }
        }
        QCASSERT(! theRequestPtr);
        Read();
        return inLength;
    }
    virtual void ReadCompletion(
        int          inStatus,
        IOBuffer&    inBuffer,
        int          inLength,
        Offset       inOffset,
        RequestId    inOriginalRequestId,
        RequestId    inRequestId,
        kfsChunkId_t inChunkId,
        int64_t      inChunkVersion,
        int64_t      inChunkSize)
    {
        QCASSERT(inRequestId.mPtr && inLength <= mPendingCount);
        PBuffer& theBuf = *reinterpret_cast<PBuffer*>(inRequestId.mPtr);
        mPendingCount -= inLength;
        theBuf.ReadDone(
            *this,
            inStatus,
            inBuffer,
            inLength,
            inOffset,
            inOriginalRequestId,
            inChunkId,
            inChunkVersion,
            inChunkSize
        );
    }
    virtual bool CanCancelRead(
        RequestId inRequestId)
    {
        QCASSERT(inRequestId.mPtr);
        PBuffer& theBuf = *reinterpret_cast<PBuffer*>(inRequestId.mPtr);
        return theBuf.CancelRead(*this);
    }
    virtual bool IsIdle() const
    {
        return (
            Requests::IsEmpty(mPendingQueue) &&
            Requests::IsEmpty(mInFlightList)
        );
    }
    int GetBufferCount() const
        { return (mStripeCount + mRecoveryStripeCount); }

private:
    class Buffer;
    class PBuffer
    {
    public:
        typedef RSReadStriper Outer;

        Buffer&  mParent;
        IOBuffer mBuffer;
        int      mSize;
        int      mStatus;
        bool     mInFlightFlag:1;
        bool     mDoneFlag:1;
        bool     mCancelFlag:1;

        PBuffer(
            Buffer& inParent)
            : mParent(inParent),
              mBuffer(),
              mSize(0),
              mStatus(0),
              mInFlightFlag(false),
              mDoneFlag(false),
              mCancelFlag(false)
            {}
        void Clear()
        {
            mBuffer.Clear();
            mSize         = 0;
            mStatus       = 0;
            mInFlightFlag = false;
            mDoneFlag     = false;
            mCancelFlag   = false;
        }
        bool IsFailed() const
            { return Outer::IsFailure(mStatus); }
        Offset GetPos() const
            { return mParent.GetPos(*this); }
        int GetStripeIdx() const
            { return (mParent.GetStripeIdx()); }
        int Retry(
            bool inClearFlag)
        {
            QCASSERT(! mInFlightFlag && mSize >= 0);
            if (! IsFailed()) {
                return 0;
            }
            if (inClearFlag) {
                mBuffer.Clear();
            }
            mDoneFlag = false;
            mStatus   = 0;
            return mSize;
        }
        int MarkFailed()
        {
            QCASSERT(! mInFlightFlag);
            if (mSize <= 0 || IsFailed()) {
                return 0;
            }
            mStatus   = kErrorIO;
            mDoneFlag = true;
            return mSize;
        }
        void ReadDone(
            Outer&       inOuter,
            int          inStatus,
            IOBuffer&    inBuffer,
            int          inSize,
            Offset       inOffset,
            RequestId    inRequestId,
            kfsChunkId_t inChunkId,
            int64_t      inChunkVersion,
            int64_t      inChunkSize)
        {
            QCRTASSERT(
                mInFlightFlag   &&
                inSize == mSize &&
                inOffset == GetPos() &&
                mBuffer.IsEmpty()
            );
            mInFlightFlag = false;
            mDoneFlag     = true;
            const bool thePrevOk = ! mParent.IsFailed();
            mStatus = inStatus;
            if (! IsFailed() && inBuffer.BytesConsumable() < mSize) {
                KFS_LOG_STREAM_DEBUG << inOuter.mLogPrefix    <<
                    "short read:"
                    " req: "     << mParent.mRequest.mPos      <<
                    ","          << mParent.mRequest.mSize     <<
                    " got: "     << inBuffer.BytesConsumable() <<
                    " of: "      << mSize                      <<
                    " stripe: "  << GetStripeIdx()             <<
                    " pos: "     << GetPos()                   <<
                    " chunk: "   << inChunkId                  <<
                    " version: " << inChunkVersion             <<
                KFS_LOG_EOM;
            }
            mBuffer.Clear();
            mBuffer.Move(&inBuffer);
            mParent.ReadDone(
                inOuter,
                *this,
                thePrevOk && IsFailed(),
                inRequestId,
                inChunkId,
                inChunkVersion,
                inChunkSize
            );
        }
        int InitRecoveryRead(
            Offset inSize)
        {
            if (inSize <= 0) {
                return 0;
            }
            QCASSERT(! mInFlightFlag);
            // If read has failed already mark the extra read as failed.
            mStatus = mParent.GetStatus();
            mSize   = (int)inSize;
            if (IsFailed() || mBuffer.BytesConsumable() == mSize) {
                mDoneFlag = true;
                return 0;
            }
            mBuffer.Clear();
            mDoneFlag = false;
            return mSize;
        }
        int Read(
            Outer& inOuter)
        {
            if (mSize <= 0 || mInFlightFlag || mDoneFlag) {
                return 0;
            }
            QCASSERT(GetPos() >= 0);
            const bool theRetryFlag = mParent.mRequest.mRecoveryRound > 0;
            RequestId theId = RequestId();
            theId.mPtr      = this;
            IOBuffer theBuffer;
            if (this != &mParent.mBuf) {
                mBuffer.Clear();
                if (! inOuter.mUseDefaultBufferAllocatorFlag) {
                    theBuffer.Append(NewDataBuffer(mSize));
                }
            } else {
                QCASSERT(mBuffer.IsEmpty());
                if (theRetryFlag) {
                    // This is needed to cancel the requests with no completion
                    // invocation: the buffers list must be saved.
                    theBuffer.UseSpaceAvailable(&mBuffer, mSize);
                } else {
                    theBuffer.Move(&mBuffer);
                }
            }
            mParent.mRequest.mInFlightCount += mSize;
            QCASSERT(mParent.mRequest.mInFlightCount <=
                mParent.mRequest.mPendingCount);
            inOuter.mPendingCount += mSize;
            mInFlightFlag = true;
            // Recovery validates stripe sizes, and wont work if short reads
            // fail -- do not fail short reads if recovery is running.
            const int theQueuedCount = inOuter.QueueRead(
                theBuffer,
                mSize,
                GetPos(),
                mParent.mRequest.mRequestId,
                theId,
                theRetryFlag ||
                    inOuter.mRecoveryStripeCount <= 0,
                inOuter.mFailShortReadsFlag &&
                    mParent.mRequest.mRecoverySize <= 0
            );
            if (theQueuedCount != mSize) {
                if (theQueuedCount > 0) {
                    InternalError("failed to queue complete chunk read");
                }
                inOuter.ReadCompletion(
                    theQueuedCount < 0 ? theQueuedCount : kErrorParameters,
                    theBuffer,
                    mSize,
                    GetPos(),
                    mParent.mRequest.mRequestId,
                    theId,
                    -1,
                    -1,
                    -1
                );
                return 0;
            }
            return theQueuedCount;
        }
        void Cancel(
            Outer& inOuter)
        {
            if (! mInFlightFlag) {
                return;
            }
            QCASSERT(! mDoneFlag);
            if (mDoneFlag) {
                return;
            }
            mCancelFlag = true;
        }
        bool CancelRead(
            Outer& inOuter)
        {
            if (! mInFlightFlag || mDoneFlag || ! mCancelFlag) {
                return false;
            }
            QCASSERT(
                mSize >= 0 &&
                mParent.mRequest.mPendingCount >= mSize &&
                mParent.mRequest.mInFlightCount >= mSize
            );
            mParent.mRequest.mPendingCount -= mSize;
            mParent.mRequest.mInFlightCount -= mSize;
            mStatus       = kErrorCanceled;
            mInFlightFlag = false;
            mDoneFlag     = true;
            mCancelFlag   = false;
            return true;
        }
        bool CancelPendingRead(
            Outer& inOuter)
        {
            if (mSize <= 0 || mInFlightFlag || mDoneFlag) {
                return false;
            }
            mParent.mRequest.mInFlightCount += mSize;
            QCASSERT(mParent.mRequest.mInFlightCount <=
                mParent.mRequest.mPendingCount);
            mInFlightFlag = true;
            mCancelFlag   = true;
            const bool theRetFlag = CancelRead(inOuter);
            QCASSERT(theRetFlag);
            return theRetFlag;
        }
    private:
        PBuffer(
            const PBuffer& inBuf);
        PBuffer& operator=(
            const PBuffer& inBuf);
    };
    friend class PBuffer;

    class Request;
    class Buffer
    {
    public:
        typedef RSReadStriper Outer;

        Request&     mRequest;
        Offset       mPos;
        PBuffer      mBufL;
        PBuffer      mBuf;
        PBuffer      mBufR;
        kfsChunkId_t mChunkId;
        int64_t      mChunkVersion;
        int64_t      mChunkSize;

        Buffer(
            Request& inRequest,
            Offset   inPos  = -1,
            int      inSize = 0)
            : mRequest(inRequest),
              mBufL(*this),
              mBuf(*this),
              mBufR(*this),
              mChunkId(-1),
              mChunkVersion(-1),
              mChunkSize(0)
            {}
        ~Buffer()
            {}
        void Clear()
        {
            mBufL.Clear();
            mBuf.Clear();
            mBufR.Clear();
            mPos       = -1;
            mChunkSize = 0;
        }
        int GetStripeIdx() const
            { return mRequest.GetStripeIdx(*this); }
        int Retry()
        {
            return (mBufL.Retry(true) + mBuf.Retry(false) + mBufR.Retry(true));
        }
        Offset GetPos(
            const PBuffer& inBuffer) const
        {
            Offset theRet = -1;
            if (&inBuffer == &mBufL) {
                theRet = mPos - mBufL.mSize;
            } else if (&inBuffer == &mBuf) {
                theRet = mPos;
            } else if (&inBuffer == &mBufR) {
                theRet = mPos + mBuf.mSize;
            }
            QCASSERT(theRet >= 0);
            return theRet;
        }
        void ReadDone(
            Outer&       inOuter,
            PBuffer&     inBuffer,
            bool         inNewFailureFlag,
            RequestId    inRequestId,
            kfsChunkId_t inChunkId,
            int64_t      inChunkVersion,
            int64_t      inChunkSize)
        {
            if (inBuffer.IsFailed()) {
                if (&inBuffer != &mBuf) {
                    inBuffer.mBuffer.Clear();
                }
            } else {
                mChunkId      = inChunkId;
                mChunkVersion = inChunkVersion;
                mChunkSize    = max(int64_t(0), inChunkSize);
            }
            mRequest.ReadDone(
                inOuter,
                inBuffer,
                *this,
                inNewFailureFlag,
                inRequestId
            );
        }
        int InitRecoveryRead(
            Outer& inOuter,
            Offset inOffset,
            int    inSize)
        {
            if (inSize <= 0) {
                return 0;
            }
            QCASSERT(inOffset >= 0);
            if (mBuf.mSize <= 0) {
                mBuf.mSize = 0;
                QCASSERT(GetSize() == 0);
                mPos = inOffset + inSize;
            }
            const int theRet =
                mBufL.InitRecoveryRead(mPos - inOffset) +
                mBufR.InitRecoveryRead(
                    inOffset + inSize - (mPos + mBuf.mSize));
            if (mBufL.mSize > inOuter.mMaxReadSize ||
                    mBufR.mSize > inOuter.mMaxReadSize) {
                InternalError(
                    "failed to start recovery: invalid request boundary");
                mBufL.mStatus = kErrorParameters;
                mBufR.mStatus = kErrorParameters;
                return 0;
            }
            return theRet;
        }
        int GetSize() const
            { return (mBufL.mSize + mBuf.mSize + mBufR.mSize); }
        bool IsInFlight() const
        {
            return (
                mBufL.mInFlightFlag ||
                mBuf.mInFlightFlag ||
                mBufR.mInFlightFlag
            );
        }
        bool IsFailed() const
        {
            return (
                mBufL.IsFailed() ||
                mBuf.IsFailed() ||
                mBufR.IsFailed()
            );
        }
        bool IsReadyForRecovery() const
        {
            return (
                ! IsInFlight() &&
                ! IsFailed() &&
                GetSize() == mRequest.mRecoverySize
            );
        }
        int GetStatus() const
        {
            if (mBuf.IsFailed()) {
                return mBuf.mStatus;
            }
            if (mBufL.IsFailed()) {
                return mBufL.mStatus;
            }
            if (mBufR.IsFailed()) {
                return mBufR.mStatus;
            }
            return 0;
        }
        void SetSetatus(
            int inStatus)
        {
            if (mBuf.mSize > 0) {
                mBuf.mStatus = inStatus;
            }
            if (mBufL.mSize > 0) {
                mBufL.mStatus = inStatus;
            }
            if (mBufR.mSize > 0) {
                mBufR.mStatus = inStatus;
            }
        }
        void Read(
            Outer& inOuter)
        {
            if (IsFailed()) {
                return;
            }
            const int theQueuedCnt =
                mBufL.Read(inOuter) +
                mBuf.Read(inOuter)  +
                mBufR.Read(inOuter);
            if (theQueuedCnt > 0) {
                inOuter.StartQueuedRead(theQueuedCnt);
            }
        }
        void Cancel(
            Outer& inOuter)
        {
            mBufL.Cancel(inOuter);
            mBuf.Cancel(inOuter);
            mBufR.Cancel(inOuter);
        }
        void CancelPendingRead(
            Outer& inOuter)
        {
            mBufL.CancelPendingRead(inOuter);
            mBuf.CancelPendingRead(inOuter);
            mBufR.CancelPendingRead(inOuter);
        }
        int MarkFailed()
        {
            return (
                mBufL.MarkFailed() +
                mBuf.MarkFailed() +
                mBufR.MarkFailed()
            );
        }
        bool MakeBufferForRecovery(
            Outer&    inOuter,
            IOBuffer& inBuffer,
            int&      outSize,
            int&      outRdSize)
        {
            inBuffer.Clear();
            outRdSize = 0;
            outSize   = 0;
            const bool theReadFailedFlag = IsFailed();
            if (theReadFailedFlag) {
                if (mBuf.mSize > 0) {
                    // If only the extra read failed, but read succeeded then
                    // return the read result to the caller, and make new temp.
                    // buffer to run recovery with, just to keep it simple.
                    // Such "half" failures are expected to be rare.
                    if (inOuter.mUseDefaultBufferAllocatorFlag &&
                            mBuf.IsFailed()) {
                        mBuf.mBuffer.EnsureSpaceAvailable(mBuf.mSize);
                    }
                    const int theAvail = mBuf.IsFailed() ?
                        inBuffer.UseSpaceAvailable(&mBuf.mBuffer, mBuf.mSize)
                        : 0;
                    if (theAvail < mBuf.mSize) {
                        if (inOuter.mUseDefaultBufferAllocatorFlag) {
                            inBuffer.EnsureSpaceAvailable(mBuf.mSize);
                        } else {
                            IOBufferData theBuf =
                                NewDataBuffer(mBuf.mSize - theAvail);
                            inBuffer.Append(theBuf);
                            if (mBuf.IsFailed()) {
                                mBuf.mBuffer.Append(theBuf);
                            }
                        }
                    }
                    outSize += mBuf.mSize;
                }
                if (mBufL.mSize > 0) {
                    IOBuffer theBuf;
                    theBuf.Move(&inBuffer);
                    if (inOuter.mUseDefaultBufferAllocatorFlag) {
                        inBuffer.EnsureSpaceAvailable(mBufL.mSize);
                    } else {
                        inBuffer.Append(NewDataBuffer(mBufL.mSize));
                    }
                    inBuffer.Move(&theBuf);
                    outSize += mBufL.mSize;
                }
                if (mBufR.mSize > 0) {
                    if (inOuter.mUseDefaultBufferAllocatorFlag) {
                        inBuffer.EnsureSpaceAvailable(mBufR.mSize + outSize);
                    } else {
                        inBuffer.Append(NewDataBuffer(mBufR.mSize));
                    }
                    outSize += mBufR.mSize;
                }
            } else {
                if (mBufL.mSize > 0) {
                    const int theSize = mBufL.mBuffer.BytesConsumable();
                    outSize   += mBufL.mSize;
                    outRdSize += theSize;
                    inBuffer.Copy(&mBufL.mBuffer, theSize);
                    inOuter.ZeroPaddTo(inBuffer, outSize);
                }
                if (mBuf.mSize > 0) {
                    const int theSize = mBuf.mBuffer.BytesConsumable();
                    outSize   += mBuf.mSize;
                    outRdSize += theSize;
                    // Copy to save the original length and buffer space of
                    // short read.
                    inBuffer.Copy(&mBuf.mBuffer, theSize);
                    inOuter.ZeroPaddTo(inBuffer, outSize);
                }
                if (mBufR.mSize > 0) {
                    const int theSize = mBufR.mBuffer.BytesConsumable();
                    outSize   += mBufR.mSize;
                    outRdSize += theSize;
                    inBuffer.Copy(&mBufR.mBuffer, theSize);
                    inOuter.ZeroPaddTo(inBuffer, outSize);
                }
                QCASSERT(inBuffer.BytesConsumable() == outSize);
            }
            return theReadFailedFlag;
        }
        int GetReadSize()
        {
            if (IsFailed()) {
                return -1;
            }
            return (
                (mBufL.mSize > 0 ? mBufL.mBuffer.BytesConsumable() : 0) +
                ( mBuf.mSize > 0 ?  mBuf.mBuffer.BytesConsumable() : 0) +
                (mBufR.mSize > 0 ? mBufR.mBuffer.BytesConsumable() : 0)
            );
        }
        void SetRecoveryResult(
            IOBuffer& inBuffer)
        {
            if (mBuf.mSize <= 0 || ! mBuf.IsFailed()) {
                // Result is already there.
                inBuffer.Clear();
                return;
            }
            QCASSERT(inBuffer.BytesConsumable() <= GetSize());
            // The missing chunk size is not known, thus the recovery might zero
            // fill the hole.
            // In other words "skip holes" mode will not work, as the
            // holes boundary is known only with stripe size precision, in the
            // case when the trailing data stripes in the block are missing.
            //
            // dddd    dddd    dddd
            // d... => ???? => d000
            // rrrr    rrrr    rrrr
            if (mBufL.mSize > 0) {
                IOBuffer theBuf;
                const int theCnt = theBuf.MoveSpace(&inBuffer, mBufL.mSize);
                if (theCnt != mBufL.mSize) {
                    InternalError("invalid left buffer space");
                }
            }
            mBuf.mBuffer.Clear();
            const int theCnt = mBuf.mBuffer.MoveSpace(&inBuffer, mBuf.mSize);
            if (theCnt != mBuf.mSize) {
                InternalError("invalid middle buffer space");
            }
        }
    private:
        Buffer(
            const Buffer& inBuffer);
        Buffer& operator=(
            const Buffer& inBuffer);
    };
    typedef QCDLList<Request, 0> Requests;

    class RecoveryInfo;
    class Request
    {
    public:
        typedef RSReadStriper Outer;

        RequestId mRequestId;
        Offset    mPos;
        Offset    mRecoveryPos;
        int       mSize;
        int       mPendingCount;
        int       mInFlightCount;
        int       mStatus;
        int       mRecoveryRound;
        int       mRecursionCount;
        int       mRecoverySize;
        int       mBadStripeCount;

        static Request& Create(
            Outer&    inOuter,
            RequestId inRequestId,
            Offset    inPos,
            int       inSize)
        {
            const size_t theSize =
                sizeof(Request) + sizeof(Buffer) * inOuter.GetBufferCount();
            char* const  thePtr = new char[theSize];
            Request& theRet = *new(thePtr) Request(inRequestId, inPos, inSize);
            for (size_t i = sizeof(Request); i < theSize; i += sizeof(Buffer)) {
                new (thePtr + i) Buffer(theRet);
            }
            return theRet;
        }
        Buffer& GetBuffer(
            int inIdx)
        {
            QCASSERT(inIdx >= 0);
            return reinterpret_cast<Buffer*>(this + 1)[inIdx];
        }
        const Buffer& GetBuffer(
            int inIdx) const
        {
            QCASSERT(inIdx >= 0);
            return reinterpret_cast<const Buffer*>(this + 1)[inIdx];
        }
        int GetStripeIdx(
            const Buffer& inBuffer) const
        {
            const int theIdx = (int)(&inBuffer - &GetBuffer(0));
            QCASSERT(theIdx >= 0);
            return theIdx;
        }
        void Delete(
            Outer&    inOuter,
            Request** inListPtr)
        {
            Requests::Remove(inListPtr, *this);
            const int theBufCount = inOuter.GetBufferCount();
            for (int i = 0; i < theBufCount; i++) {
                GetBuffer(i).~Buffer();
            }
            this->~Request();
            delete [] reinterpret_cast<char*>(this);
        }
        void Reset(
            Outer&    inOuter,
            RequestId inRequestId,
            Offset    inPos,
            int       inSize)
        {
            mRequestId      = inRequestId;
            mPos            = inPos;
            mRecoveryPos    = 0;
            mSize           = inSize;
            mPendingCount   = 0;
            mInFlightCount  = 0;
            mStatus         = 0;
            mRecoveryRound  = 0;
            mRecursionCount = 0;
            mRecoverySize   = 0;
            mBadStripeCount = 0;
            const int theBufCount = inOuter.GetBufferCount();
            for (int i = 0; i < theBufCount; i++) {
                GetBuffer(i).Clear();
            }
        }
        void ReadDone(
            Outer&    inOuter,
            PBuffer&  inPBuffer,
            Buffer&   inBuffer,
            bool      inNewFailureFlag,
            RequestId inRequestId)
        {
            const int theStripeIdx = inBuffer.GetStripeIdx();
            const int theBufCount  = inOuter.GetBufferCount();
            QCRTASSERT(
                inPBuffer.mSize >= 0 &&
                theStripeIdx >= 0 &&
                theStripeIdx < theBufCount &&
                mPendingCount >= inPBuffer.mSize &&
                mInFlightCount >= inPBuffer.mSize &&
                mRequestId.mId == inRequestId.mId
            );
            mPendingCount  -= inPBuffer.mSize;
            mInFlightCount -= inPBuffer.mSize;
            if (inPBuffer.IsFailed()) {
                KFS_LOG_STREAM_INFO << inOuter.mLogPrefix <<
                    "read failure:"
                    " req: "         << mPos              <<
                    ","              << mSize             <<
                    " status: "      << inPBuffer.mStatus <<
                    " stripe: "      << theStripeIdx      <<
                    " bad: "         << mBadStripeCount   <<
                    " round: "       << mRecoveryRound    <<
                KFS_LOG_EOM;
                if (inNewFailureFlag && mRecoveryRound <= 0 &&
                        Recovery(inOuter)) {
                    return;
                }
            } else if (mRecoveryRound > 0 &&
                    inBuffer.IsReadyForRecovery() &&
                    --mBadStripeCount <= inOuter.mRecoveryStripeCount) {
                // Can finish recovery now, cancel all pending reads if any.
                if (mRecursionCount > 0) {
                    // Cancel pending reads that are possibly in the process of
                    // being scheduled by Read() below.
                    for (int i = 0; i < theBufCount; i++) {
                        GetBuffer(i).CancelPendingRead(inOuter);
                    }
                }
                QCRTASSERT(mPendingCount == mInFlightCount);
                KFS_LOG_STREAM_INFO << inOuter.mLogPrefix <<
                    "can finish recovery:"
                    " req: "       << mPos            <<
                    ","            << mSize           <<
                    " bad: "       << mBadStripeCount <<
                    " round: "     << mRecoveryRound  <<
                    " in flight: " << mInFlightCount  <<
                KFS_LOG_EOM;
                if (mInFlightCount > 0) {
                    for (int i = 0; i < theBufCount; i++) {
                        GetBuffer(i).Cancel(inOuter);
                    }
                    inOuter.CancelRead();
                    QCRTASSERT(mPendingCount == 0 && mInFlightCount == 0);
                }
            }
            if (mPendingCount <= 0 && mRecursionCount <= 0) {
                Done(inOuter);
            }
        }
        void Read(
            Outer& inOuter)
        {
            if (mRecursionCount > 0 ||
                    mPendingCount <= 0 ||
                    mInFlightCount >= mPendingCount) {
                return;
            }
            mRecursionCount++;
            const int theBufCount = inOuter.GetBufferCount();
            while (mPendingCount > 0 && mInFlightCount < mPendingCount) {
                for (int i = 0;
                        i < theBufCount &&
                            mInFlightCount < mPendingCount;
                        i++) {
                    GetBuffer(i).Read(inOuter);
                }
            }
            mRecursionCount--;
            if (mPendingCount <= 0) {
                Done(inOuter);
            }
        }
        bool IsFailed() const
            { return Outer::IsFailure(mStatus); }
        void InitRecovery(
            Outer& inOuter)
        {
            if (mSize <= 0 || mRecoverySize > 0) {
                return;
            }
            // Process() ensures that the "recovery width" does not exceed max
            // atomic read size, and calculates recovery size.
            mRecoverySize = -mRecoverySize;
            if (mRecoverySize <= 0 ||
                    mRecoverySize > inOuter.mMaxReadSize ||
                    mRecoveryPos < 0) {
                InternalError(
                    "failed to start recovery: invalid request");
                return;
            }
            KFS_LOG_STREAM_INFO << inOuter.mLogPrefix <<
                "init recovery:"
                " req: "  << mPos                 <<
                ","       << mSize                <<
                " pos: "  << mRecoveryPos         <<
                " size: " << mRecoverySize        <<
                " [" << GetChunkPos(mRecoveryPos) << "," <<
                        (GetChunkPos(mRecoveryPos) + mRecoverySize) << ")" <<
            KFS_LOG_EOM;
            Offset theOffset = mRecoveryPos;
            for (int i = 0; i < inOuter.mStripeCount; i++) {
                mPendingCount += GetBuffer(i).InitRecoveryRead(
                    inOuter, theOffset, mRecoverySize);
                theOffset += (Offset)CHUNKSIZE;
            }
        }

    private:
        Request* mPrevPtr[1];
        Request* mNextPtr[1];
        friend class QCDLListOp<Request, 0>;

        Request(
            RequestId inRequestId,
            Offset    inPos,
            int       inSize)
            : mRequestId(inRequestId),
              mPos(inPos),
              mRecoveryPos(0),
              mSize(inSize),
              mPendingCount(0),
              mInFlightCount(0),
              mStatus(0),
              mRecoveryRound(0),
              mRecursionCount(0),
              mRecoverySize(0),
              mBadStripeCount(0)
            { Requests::Init(*this); }
        ~Request()
            {}
        bool Recovery(
            Outer& inOuter)
        {
            if (++mBadStripeCount > inOuter.mRecoveryStripeCount) {
                return false;
            }
            if (mBadStripeCount <= 1 && mRecoverySize <= 0) {
                InitRecovery(inOuter);
            }
            if (mRecoverySize <= 0) {
                return false;
            }
            const int i = inOuter.mStripeCount + mBadStripeCount - 1;
            mPendingCount += GetBuffer(i).InitRecoveryRead(
                inOuter, mRecoveryPos + i * (Offset)CHUNKSIZE, mRecoverySize);
            Read(inOuter);
            return true;
        }
        void Done(
            Outer& inOuter)
        {
            if (mPendingCount > 0) {
                return;
            }
            if (mRecoverySize > 0 ||
                    inOuter.mStripeCount <= inOuter.mRecoverStripeIdx) {
                inOuter.FinishRecovery(*this);
                QCRTASSERT(mPendingCount == 0 && mInFlightCount == 0);
                if (IsFailed() &&
                        (inOuter.mRecoverStripeIdx < 0 ||
                            mStatus != kErrorInvalidChunkSizes)) {
                    bool theTryAgainFlag = mRecoveryRound <= 0 ||
                        mStatus == kErrorInvalidChunkSizes;
                    const int theBufCount = inOuter.GetBufferCount();
                    if (! theTryAgainFlag) {
                        // Retry canceled reads, if any.
                        for (int i = 0; i < theBufCount; i++) {
                            if (GetBuffer(i).GetStatus() == kErrorCanceled) {
                                theTryAgainFlag = true;
                                break;
                            }
                        }
                    }
                    if (theTryAgainFlag) {
                        KFS_LOG_STREAM_INFO  << inOuter.mLogPrefix <<
                            "read recovery failed:"
                            " req: "         << mPos            <<
                            ","              << mSize           <<
                            " status: "      << mStatus         <<
                            " round: "       << mRecoveryRound  <<
                            " bad stripes: " << mBadStripeCount <<
                            " "              << (mRecoveryRound <= 0 ?
                                    "turning on read retries" :
                                    "get remaining stripes")    <<
                        KFS_LOG_EOM;
                        mRecoveryRound++;
                        mStatus = 0;
                        int theInvalidChunkSizeCount = 0;
                        for (int i = 0; i < theBufCount; i++) {
                            Buffer&   theBuf    = GetBuffer(i);
                            const int theStatus = theBuf.GetStatus();
                            if (theStatus == kErrorInvalidChunkSizes ||
                                    theStatus == kErrorInvalChunkSize) {
                                theInvalidChunkSizeCount++;
                            } else {
                                if (theStatus == 0 &&
                                        theBuf.GetSize() != mRecoverySize) {
                                    // Ensure that reads on all stripes are
                                    // scheduled. Size check in FinishRecovery()
                                    // might fail extra stripes without invoking
                                    // Request::Recovery()
                                    mPendingCount += theBuf.InitRecoveryRead(
                                        inOuter,
                                        mRecoveryPos + i * (Offset)CHUNKSIZE,
                                        mRecoverySize);
                                    mBadStripeCount++;
                                } else {
                                    mPendingCount += theBuf.Retry();
                                }
                            }
                        }
                        if (theInvalidChunkSizeCount >
                                    inOuter.mRecoveryStripeCount ||
                                    mPendingCount <= 0) {
                            KFS_LOG_STREAM_ERROR << inOuter.mLogPrefix <<
                                "read recovery failed:"
                                " req: "            << mPos            <<
                                ","                 << mSize           <<
                                " status: "         << mStatus         <<
                                " bad stripes: "    << mBadStripeCount <<
                                " invalid chunks: " <<
                                              theInvalidChunkSizeCount <<
                                " pending: "        << mPendingCount   <<
                            KFS_LOG_EOM;
                            mPendingCount = 0;
                            mStatus       = kErrorIO;
                        }
                        if (mPendingCount > 0) {
                            Read(inOuter);
                            return;
                        }
                    }
                }
            } else if (mStatus == 0 &&
                    mBadStripeCount > inOuter.mRecoveryStripeCount) {
                mStatus = kErrorIO;
            }
            inOuter.RequestCompletion(*this);
        }
    private:
        Request(
            const Request& inRequest);
        Request& operator=(
            const Request& inRequest);

    friend class RecoveryInfo;
    };
    friend class Request;

    class BufIterator
    {
    public:
        typedef RSReadStriper Outer;

        BufIterator()
            : mBuffer(),
              mCurIt(),
              mEndIt(),
              mCurPtr(0),
              mEndPtr(0),
              mSize(0),
              mReadFailureFlag(false)
            {}
        int Set(
            Outer&  inOuter,
            Buffer& inBuffer,
            int&    outRdSize)
        {
            mBuffer.Clear();
            mSize = 0;
            mReadFailureFlag = inBuffer.MakeBufferForRecovery(
                inOuter, mBuffer, mSize, outRdSize);
            return Reset();
        }
        int MakeScratchBuffer(
            Outer& inOuter,
            int    inSize)
        {
            mReadFailureFlag = true;
            mSize            = inSize;
            mBuffer.Clear();
            if (IOBuffer::IsDebugVerify()) {
                if (inOuter.mUseDefaultBufferAllocatorFlag) {
                    mBuffer.EnsureSpaceAvailable(mSize);
                } else {
                    mBuffer.Append(NewDataBuffer(mSize));
                }
            } else {
                MakeScratchBuffer(inOuter.mUseDefaultBufferAllocatorFlag ?
                    IOBufferData() : NewDataBuffer(4096 / kAlign * kAlign));
            }
            return Reset();
        }
        char* Advance(
            int inLen)
        {
            QCASSERT(inLen >= 0);
            int theLen = inLen;
            for (; ;) {
                if (mCurPtr && mCurPtr + theLen < mEndPtr) {
                    mCurPtr += theLen;
                    break;
                }
                if (mCurIt == mEndIt || ++mCurIt == mEndIt) {
                    mCurPtr = 0;
                    mEndPtr = 0;
                    break;
                }
                theLen -= mEndPtr - mCurPtr;
                SetPtrs();
            }
            return mCurPtr;
        }
        int CopyIn(
            const void* inPtr,
            int         inLen)
        {
            // Copy should use available buffer space, allocated by
            // MakeBufferForRecovery(), thus the current buffer must have
            // enough available space.
            // In the most cases no actual coy will be done, the call
            // will only update the buffer pointers and byte count, as the
            // recovery, in most, cases runs the underlying buffer.
            if (! mReadFailureFlag) {
                return 0;
            }
            const int         theLen =
                min(inLen, mSize - mBuffer.BytesConsumable());
            const char* const thePtr = static_cast<const char*>(inPtr);
            return (theLen <= (int)mCurIt->SpaceAvailable() ?
                mBuffer.CopyInOnlyIntoBufferAtPos(thePtr, theLen, mCurIt) :
                mBuffer.CopyIn(thePtr, theLen, mCurIt)
            );
        }
        int CopyOut(
            char* inPtr,
            int   inLen)
        {
            if (inLen <= 0 || mReadFailureFlag) {
                return 0;
            }
            int theLen = min(inLen, GetCurRemaining());
            memcpy(inPtr, mCurPtr, theLen);
            if (theLen >= inLen) {
                return theLen;
            }
            for (It theIt = mCurIt; theLen < inLen && ++theIt != mEndIt; ) {
                theLen += theIt->CopyOut(inPtr + theLen, inLen - theLen);
            }
            return theLen;
        }
        int GetCurRemaining() const
            { return (int)(mEndPtr - mCurPtr); }
        bool IsFailure() const
            { return mReadFailureFlag; }
        bool IsRequested() const
            { return (mSize > 0); }
        void SetRecoveryResult(
            Buffer& inBuffer)
        {
            inBuffer.SetRecoveryResult(mBuffer);
            Clear();
        }
        void Clear()
        {
            mBuffer.Clear();
            mCurIt           = mBuffer.end();
            mEndIt           = mCurIt;
            mCurPtr          = 0;
            mEndPtr          = 0;
            mSize            = 0;
            mReadFailureFlag = false;
        }
        const IOBuffer& GetBuffer() const
            { return mBuffer; }
    private:
        typedef IOBuffer::iterator It;

        IOBuffer mBuffer;
        It       mCurIt;
        It       mEndIt;
        char*    mCurPtr;
        char*    mEndPtr;
        int      mSize;
        bool     mReadFailureFlag;

        void SetPtrs()
        {
            mCurPtr = const_cast<char*>(mCurIt->Consumer());
            mEndPtr = mCurPtr + mCurIt->BytesConsumable();
            if (mReadFailureFlag) {
                mEndPtr += mCurIt->SpaceAvailable();
            }
        }
        void MakeScratchBuffer(
            const IOBufferData& inBuf)
        {
            const int theAvail = inBuf.SpaceAvailable();
            QCASSERT(theAvail % kAlign == 0);
            for (int theRem = mSize; theRem > 0; theRem -= theAvail) {
                if (theAvail <= theRem) {
                    mBuffer.Append(inBuf);
                } else {
                    char* const thePtr = const_cast<char*>(inBuf.Producer());
                    mBuffer.Append(
                        IOBufferData(inBuf, thePtr, thePtr + theRem, thePtr));
                    break;
                }
            }
        }
        int Reset()
        {
            mCurIt = mBuffer.begin();
            mEndIt = mBuffer.end();
            if (mCurIt == mEndIt) {
                mCurPtr = 0;
                mEndPtr = 0;
            } else {
                SetPtrs();
            }
            return mSize;
        }
    private:
        BufIterator(
            const BufIterator& inIt);
        BufIterator& operator=(
            const BufIterator& inIt);
    };

    class RecoveryInfo
    {
    public:
        typedef RSReadStriper        Outer;
        typedef RSStriper::StripeIdx StripeIdx;

        IOBuffer  mBuffer;
        Offset    mPos;
        Offset    mChunkBlockStartPos;
        int       mSize;
        int       mMissingCnt;
        StripeIdx mMissingIdx[kMaxRecoveryStripes];

        RecoveryInfo()
            : mBuffer(),
              mPos(-1),
              mChunkBlockStartPos(-1),
              mSize(0),
              mMissingCnt(0)
            {}
        void ClearBuffer()
        {
            mBuffer.Clear();
            mSize = 0;
            mPos  = -1;
        }
        void Clear()
        {
            ClearBuffer();
            mMissingCnt         = 0;
            mChunkBlockStartPos = -1;
        }
        void Set(
            Outer&   inOuter,
            Request& inRequest)
        {
            if (inRequest.IsFailed() || inRequest.mRecoverySize <= 0 ||
                    (inRequest.mBadStripeCount <= 0 &&
                    inOuter.mRecoverStripeIdx < inOuter.mStripeCount)) {
                Clear();
                return;
            }
            mPos                = inRequest.mPos;
            mChunkBlockStartPos = mPos - mPos % inOuter.mChunkBlockSize;
            mMissingCnt         = 0;
            const int theBufCount = inOuter.GetBufferCount();
            for (int i = 0;
                    i < theBufCount &&
                        mMissingCnt < inOuter.mRecoveryStripeCount;
                    i++) {
                Buffer& theBuf = inRequest.GetBuffer(i);
                if (theBuf.IsFailed()) {
                    mMissingIdx[mMissingCnt++] = i;
                }
            }
            if (mMissingCnt <= 0) {
                Clear();
            }
            // TODO: use mBuffer to save recovery result if request is not
            // aligned.
        }
        void SetIfEmpty(
            Outer& inOuter,
            Offset inPos,
            int    inBadStripeIdx)
        {
            QCASSERT(
                inPos >= 0 &&
                inBadStripeIdx >= 0 &&
                inBadStripeIdx < inOuter.GetBufferCount()
            );
            if (mChunkBlockStartPos >= 0 && mChunkBlockStartPos <= inPos &&
                    inPos < mChunkBlockStartPos + inOuter.mChunkBlockSize) {
                return;
            }
            Clear();
            mPos                = inPos;
            mChunkBlockStartPos = mPos - mPos % inOuter.mChunkBlockSize;
            const int theCnt    = inOuter.GetBufferCount();
            StBufferT<StripeIdx, (32 + 1) * 2> theTmpBuf;
            StripeIdx* const theSwappedIdx = theTmpBuf.Resize(
                (inOuter.mRecoveryStripeCount + 1) * 2);
            StripeIdx* const theMissingIdx = theSwappedIdx +
                inOuter.mRecoveryStripeCount + 1;
            int       theMissingCnt    = 0;
            int       theMaxMissingCnt = inOuter.mRecoveryStripeCount;
            int const theSkipStripeIdx =
                inBadStripeIdx < inOuter.mStripeCount ? inBadStripeIdx - 1 : -1;
            if (0 <= theSkipStripeIdx) {
                // Ensure that the data stripe prior to the recovered data
                // stripe is *not* selected as bad stripe, as it might define
                // the hole position in the case when the file is sparse.
                theMissingIdx[0] = theSkipStripeIdx;
                theSwappedIdx[0] = theCnt - 1;
                ++theMissingCnt;
                ++theMaxMissingCnt;
            }
            theMissingIdx[theMissingCnt] = inBadStripeIdx;
            theSwappedIdx[theMissingCnt] = theCnt - (theMissingCnt + 1);
            ++theMissingCnt;
            // Randomly select which stripes to use for RS recovery in order to
            // attempt to uniformly distribute the chunk server read load.
            while (theMissingCnt < theMaxMissingCnt) {
                int theIdx = inOuter.Rand() % (theCnt - theMissingCnt);
                int i;
                for (i = 0; i < theMissingCnt; i++) {
                    if (theIdx == theMissingIdx[i]) {
                        theIdx = theSwappedIdx[i];
                        theSwappedIdx[i] = theCnt - (theMissingCnt + 1);
                        while (i > 0 && theIdx < theMissingIdx[i - 1]) {
                            i--;
                        }
                    }
                    // Insertion sort.
                    if (theIdx < theMissingIdx[i]) {
                        for (int k = theMissingCnt; i < k; k--) {
                            theMissingIdx[k] = theMissingIdx[k - 1];
                            theSwappedIdx[k] = theSwappedIdx[k - 1];
                        }
                        theMissingIdx[i] = theIdx;
                        theMissingCnt++;
                        theSwappedIdx[i] = theCnt - theMissingCnt;
                        break;
                    }
                }
                if (i >= theMissingCnt) {
                    theMissingIdx[theMissingCnt] = theIdx;
                    theSwappedIdx[theMissingCnt] = theCnt - (theMissingCnt + 1);
                    theMissingCnt++;
                }
            }
            for (int i = 0; i < theMaxMissingCnt; i++) {
                if (theSkipStripeIdx != theMissingIdx[i]) {
                    mMissingIdx[mMissingCnt++] = theMissingIdx[i];
                }
            }
        }
        void Get(
            Outer&   inOuter,
            Request& inRequest)
        {
            if (mChunkBlockStartPos < 0 || inRequest.mRecoverySize > 0) {
                return;
            }
            if (inRequest.mPos < mChunkBlockStartPos ||
                    inRequest.mPos >=
                        mChunkBlockStartPos + inOuter.mChunkBlockSize) {
                Clear();
                return;
            }
            QCASSERT(
                inRequest.mBadStripeCount == 0 &&
                inRequest.mInFlightCount == 0
            );
            const int theBufCount = inOuter.GetBufferCount();
            int i;
            for (i = 0; ; i++) {
                const int theIdx = i < mMissingCnt ?
                    mMissingIdx[i] : inOuter.mStripeCount;
                QCASSERT(theIdx >= 0);
                if (theIdx >= inOuter.mStripeCount) {
                    QCASSERT(theIdx < theBufCount);
                    if (inRequest.mRecoverySize > 0 ||
                            (inRequest.mBadStripeCount <= 0 &&
                            inOuter.mRecoverStripeIdx <
                                inOuter.mStripeCount)) {
                        break;
                    }
                    inRequest.InitRecovery(inOuter);
                    if (inRequest.mRecoverySize <= 0 ||
                            inRequest.mBadStripeCount >= mMissingCnt) {
                        break;
                    }
                    i = 0;
                    continue;
                }
                QCASSERT(theIdx < inOuter.mStripeCount);
                Buffer& theBuf    = inRequest.GetBuffer(theIdx);
                const int theSize = theBuf.MarkFailed();
                if (theSize <= 0) {
                    continue;
                }
                QCASSERT(
                    inRequest.mBadStripeCount < mMissingCnt &&
                    inRequest.mPendingCount >= theSize
                );
                inRequest.mPendingCount -= theSize;
                inRequest.mBadStripeCount++;
                KFS_LOG_STREAM_DEBUG << inOuter.mLogPrefix <<
                    "get read recovery info:"
                    " req: "     << inRequest.mPos            <<
                    ","          << inRequest.mSize           <<
                    " block: "   << mChunkBlockStartPos       <<
                    " stripe: "  << theIdx                    <<
                    " bad: "     << inRequest.mBadStripeCount <<
                    " pending: " << inRequest.mPendingCount   <<
                KFS_LOG_EOM;
            }
            if (inRequest.mRecoverySize <= 0) {
                return;
            }
            for (int l = 0, k = inOuter.mStripeCount;
                    l < inRequest.mBadStripeCount && k < theBufCount;
                    l++, k++) {
                Buffer& theBuf = inRequest.GetBuffer(k);
                inRequest.mPendingCount += theBuf.InitRecoveryRead(
                    inOuter,
                    inRequest.mRecoveryPos + k * (Offset)CHUNKSIZE,
                    inRequest.mRecoverySize
                );
                if (i < mMissingCnt && mMissingIdx[i] == k) {
                    const int theSize = theBuf.MarkFailed();
                    QCASSERT(
                        theSize > 0 &&
                        inRequest.mBadStripeCount < mMissingCnt &&
                        inRequest.mPendingCount >= theSize
                    );
                    inRequest.mPendingCount -= theSize;
                    inRequest.mBadStripeCount++;
                    i++;
                    KFS_LOG_STREAM_DEBUG << inOuter.mLogPrefix <<
                        "get read recovery info:"
                        " req: "     << inRequest.mPos            <<
                        ","          << inRequest.mSize           <<
                        " block: "   << mChunkBlockStartPos       <<
                        " stripe: "  << k                         <<
                        " bad: "     << inRequest.mBadStripeCount <<
                        " pending: " << inRequest.mPendingCount   <<
                    KFS_LOG_EOM;
                }
            }
        }
    private:
        RecoveryInfo(
            const RecoveryInfo& inInfo);
        RecoveryInfo& operator=(
            const RecoveryInfo& inInfo);
    };
    friend class RecoveryInfo;

    // Chunk read request split threshold.
    const int                mMaxReadSize;
    const bool               mUseDefaultBufferAllocatorFlag;
    const bool               mFailShortReadsFlag;
    const int                mRecoverStripeIdx;
    const Offset             mFileSize;
    const Offset             mRecoverBlockPos;
    const Offset             mRecoverChunkEndPos;
    RecoveryInfo             mRecoveryInfo;
    BufIterator*             mBufIteratorsPtr;
    IOBufferData*            mZeroBufferPtr;
    Offset                   mPendingCount;
    uint32_t                 mNextRand;
    uint32_t                 mRecoveriesCount;
    ECMethod::Decoder* const mDecoderPtr;
    Request*                 mPendingQueue[1];
    Request*                 mFreeList[1];
    Request*                 mInFlightList[1];

    RSReadStriper(
        int                inStripeSize,
        int                inStripeCount,
        int                inRecoveryStripeCount,
        int                inMaxAtomicReadRequestSize,
        bool               inUseDefaultBufferAllocatorFlag,
        bool               inFailShortReadsFlag,
        Offset             inRecoverChunkPos,
        Offset             inFileSize,
        SeqNum             inInitialSeqNum,
        string             inFilePrefix,
        Impl&              inOuter,
        ECMethod::Decoder* inDecoderPtr)
        : Striper(inOuter),
          RSStriper(
            inStripeSize,
            inStripeCount,
            inRecoveryStripeCount,
            inFilePrefix),
          mMaxReadSize(inMaxAtomicReadRequestSize),
          mUseDefaultBufferAllocatorFlag(inUseDefaultBufferAllocatorFlag),
          mFailShortReadsFlag(inFailShortReadsFlag),
          mRecoverStripeIdx(
            inRecoverChunkPos < 0 ? -1 :
            (int)(inRecoverChunkPos / (Offset)CHUNKSIZE %
                (inStripeCount + inRecoveryStripeCount))
          ),
          mFileSize(inFileSize),
          mRecoverBlockPos(
            inRecoverChunkPos < 0 ? Offset(-1) :
                inRecoverChunkPos / (Offset)CHUNKSIZE /
                (inStripeCount + inRecoveryStripeCount) *
                inStripeCount * (Offset)CHUNKSIZE
          ),
          mRecoverChunkEndPos(inRecoverChunkPos < 0 ? Offset(-1) :
            inRecoverChunkPos +
            GetChunkSize(mRecoverStripeIdx, mRecoverBlockPos, mFileSize)),
          mRecoveryInfo(),
          mBufIteratorsPtr(0),
          mZeroBufferPtr(0),
          mPendingCount(0),
          mNextRand((uint32_t)inInitialSeqNum),
          mRecoveriesCount(0),
          mDecoderPtr(inDecoderPtr)
    {
        QCASSERT(inRecoverChunkPos < 0 || inRecoverChunkPos % CHUNKSIZE == 0);
        Requests::Init(mPendingQueue);
        Requests::Init(mFreeList);
        Requests::Init(mInFlightList);
    }
    void QueueRequest(
        Request& inRequest)
    {
        mRecoveryInfo.Get(*this, inRequest);
        Requests::PushBack(mPendingQueue, inRequest);
    }
    void Read()
    {
        Request* thePtr;
        while((thePtr = Requests::PopFront(mPendingQueue))) {
            Requests::PushBack(mInFlightList, *thePtr);
            thePtr->Read(*this);
        }
    }
    int RecoverChunk(
        IOBuffer& inBuffer,
        int       inLength,
        Offset    inChunkOffset,
        RequestId inRequestId)
    {
        QCASSERT(
            mRecoverStripeIdx >= 0 &&
            mRecoverStripeIdx < mStripeCount + mRecoveryStripeCount &&
            mRecoverBlockPos >= 0
        );
        const int kChunkSize = (int)CHUNKSIZE;
        if (inChunkOffset < 0 || inChunkOffset >= kChunkSize ||
                inLength > mMaxReadSize || inLength <= 0 ||
                (inLength > mStripeSize && (inLength % mStripeSize != 0 ||
                    inChunkOffset % mStripeSize != 0)) ||
                inBuffer.begin() != inBuffer.end()) {
            return kErrorParameters;
        }
        SetPos(
            mRecoverBlockPos +
            inChunkOffset / mStripeSize * mStrideSize +
            inChunkOffset % mStripeSize +
            ((mRecoverStripeIdx < mStripeCount && inLength <= mStripeSize) ?
                mStripeSize * mRecoverStripeIdx : 0)
        );
        Request& theRequest = GetRequest(inRequestId, GetPos(), 0);
        theRequest.mRecoverySize = -inLength;
        theRequest.mRecoveryPos  = GetChunkBlockStartFilePos() + inChunkOffset;
        mRecoveryInfo.SetIfEmpty(*this, theRequest.mPos, mRecoverStripeIdx);
        Offset thePos = theRequest.mRecoveryPos;
        if (mStripeSize < inLength) {
            for (int i = 0; i < mStripeCount; i++) {
                Buffer& theBuf = theRequest.GetBuffer(i);
                theBuf.mBuf.mSize = inLength;
                theBuf.mPos       = thePos;
                theRequest.mSize += inLength;
                thePos += kChunkSize;
            }
            theRequest.mPendingCount = theRequest.mSize;
        } else {
            const int theStripeToReadIdx =
                mRecoverStripeIdx < mStripeCount ? mRecoverStripeIdx : 0;
            Buffer& theBuf = theRequest.GetBuffer(theStripeToReadIdx);
            theBuf.mBuf.mSize = inLength;
            theBuf.mPos       = GetFilePos();
            theRequest.mSize += inLength;
            theRequest.mPendingCount = theRequest.mSize;
            if (mStripeCount <= mRecoverStripeIdx) {
                mRecoveryInfo.Get(*this, theRequest);
                theRequest.InitRecovery(*this);
                QCASSERT(theRequest.mRecoverySize == inLength);
            }
        }
        QueueRequest(theRequest);
        Read();
        return inLength;
    }
    void InvalidChunkSize(
         Request& inRequest,
         Buffer&  inBuf,
         int      inExpectedSize,
         int      inPrevStripeIdx = 0)
    {
        if (inBuf.GetSize() > 0) {
            const int theSize = inBuf.GetReadSize();
            if (theSize >= 0 && theSize < inExpectedSize) {
                InvalidChunkSize(inRequest, inBuf);
                return;
            }
        }
        const int theCnt = GetBufferCount();
        for (int i = inPrevStripeIdx; i < theCnt; i++) {
            Buffer& theCBuf = inRequest.GetBuffer(i);
            int theSize;
            if (&theCBuf == &inBuf || ((theSize = theCBuf.GetReadSize()) >= 0 &&
                    theSize < inExpectedSize)) {
                // Invalidate only one at a time.
                InvalidChunkSize(inRequest, theCBuf);
                return;
            }
        }
        InvalidChunkSize(inRequest, inBuf);
    }
    void InvalidChunkSize(
         Request& inRequest,
         Buffer&  inBuf)
    {
        if (! inBuf.IsFailed()) {
            inRequest.mBadStripeCount++;
        }
        inRequest.mStatus = kErrorInvalidChunkSizes;
        inBuf.SetSetatus(kErrorInvalidChunkSizes);
        if (mRecoverStripeIdx < 0) {
            ReportInvalidChunk(
                inBuf.mChunkId,
                inBuf.mChunkVersion,
                kErrorInvalidChunkSizes,
                "rs recovery: invalid chunk size"
            );
        }
    }
    static void UpdateMaxLength(
        int  inSize,
        int& ioMaxLength)
    {
        if (ioMaxLength <= inSize) {
            return;
        }
        ioMaxLength = inSize;
        if (ioMaxLength > kAlign) {
            ioMaxLength -= ioMaxLength % kAlign;
        }
    }
    bool SetBufIterator(
        Request& inRequest,
        int      inIdx,
        int&     ioMissingCnt,
        int&     ioRecoverySize,
        int&     ioMaxRd,
        int&     ioFirstGoodRecoveryStripeIdx,
        int&     ioMaxLength,
        int*     inMissingIdxPtr,
        int&     ioEndPosIdx,
        int&     ioEndPos,
        int&     ioEndPosHead)
    {
        BufIterator& theIt     = mBufIteratorsPtr[inIdx];
        int          theRdSize = 0;
        Buffer&      theBuf    = inRequest.GetBuffer(inIdx);
        if (theIt.Set(*this, theBuf, theRdSize) != inRequest.mRecoverySize &&
                (theIt.IsRequested() ||
                inIdx < mStripeCount ||
                ioMissingCnt >= mRecoveryStripeCount)) {
            InternalError("invalid recovery buffer length");
            inRequest.mStatus = kErrorIO;
            return false;
        }
        if (theIt.IsFailure() || ! theIt.IsRequested()) {
            if (ioMissingCnt >= mRecoveryStripeCount) {
                KFS_LOG_STREAM_ERROR << mLogPrefix   <<
                    "read recovery failure:"
                    " req: "      << inRequest.mPos  <<
                    ","           << inRequest.mSize <<
                    " more than " << inMissingIdxPtr <<
                        " stripe reads failed"       <<
                KFS_LOG_EOM;
                inRequest.mStatus = kErrorIO;
                return false;
            }
            inMissingIdxPtr[ioMissingCnt++] = inIdx;
            if (inIdx == GetBufferCount() - 1 &&
                    ioFirstGoodRecoveryStripeIdx < 0 &&
                    ioMaxRd < ioRecoverySize) {
                KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                    "read recovery:"
                    " no recovery stripes available:"
                    " req: "      << inRequest.mPos  <<
                    ","           << inRequest.mSize <<
                    " size: "     << ioRecoverySize  <<
                    " max read: " << ioMaxRd         <<
                    " end:"
                    " stripe: "   << ioEndPosIdx     <<
                    " pos: "      << ioEndPos        <<
                KFS_LOG_EOM;
                if (ioMaxRd < 0) {
                    InternalError("invalid max read size");
                    inRequest.mStatus = kErrorIO;
                    return false;
                }
                ioRecoverySize = ioMaxRd;
                UpdateMaxLength(ioMaxRd, ioMaxLength);
            }
            return true;
        }
        if (inIdx >= mStripeCount) {
            if (ioFirstGoodRecoveryStripeIdx < 0) {
                ioFirstGoodRecoveryStripeIdx = inIdx;
                if (ioMaxRd < theRdSize) {
                    if (ioMaxRd >= 0) {
                        for (int i = mStripeCount - 1; i >= 0; i--) {
                            Buffer&   theCBuf = inRequest.GetBuffer(i);
                            const int theSize = theCBuf.GetReadSize();
                            if (theSize < 0) {
                                continue;
                            }
                            const int theExtraSize =
                                i <= 0 ?
                                    0 :
                                ((ioEndPosHead <= 0 || ioEndPosIdx < i) ?
                                    mStripeSize :
                                (ioEndPosIdx == i ?
                                    mStripeSize - ioEndPosHead :
                                    0));
                            if (theSize + theExtraSize < theRdSize) {
                                KFS_LOG_STREAM_ERROR << mLogPrefix          <<
                                    "read recovery failure:"
                                    " req: "      << inRequest.mPos         <<
                                    ","           << inRequest.mSize        <<
                                    " wrong stripe read size:"
                                    " got: "      << theSize                <<
                                    " extra: "    << theExtraSize           <<
                                    " expected: " << theRdSize              <<
                                    " stripe: "   << i                      <<
                                    " pos: "      << theCBuf.mBufL.GetPos() <<
                                    " size: "     << theCBuf.GetSize()      <<
                                    " chunk: "    << theCBuf.mChunkId       <<
                                    " version: "  << theCBuf.mChunkVersion  <<
                                    " size: "     << theCBuf.mChunkSize     <<
                                    " chunk: "    << theBuf.mChunkId        <<
                                    " version: "  << theBuf.mChunkVersion   <<
                                    " size: "     << theBuf.mChunkSize      <<
                                    " end:"
                                    " stripe: "   << ioEndPosIdx            <<
                                    " size: "     << ioEndPos               <<
                                    " head: "     << ioEndPosHead           <<
                                    " eof: "      << mFileSize              <<
                                KFS_LOG_EOM;
                                InvalidChunkSize(inRequest, theCBuf);
                                return false;
                            }
                        }
                    }
                    ioMaxRd = theRdSize;
                }
            }
            if (theRdSize != ioMaxRd) {
                KFS_LOG_STREAM_ERROR << mLogPrefix                   <<
                    "read recovery failure:"
                    " req: "         << inRequest.mPos               <<
                    ","              << inRequest.mSize              <<
                    " wrong recovery stripe read size:"
                    " got: "         << theRdSize                    <<
                    " expected: "    << ioMaxRd                      <<
                    " stripe: "      << inIdx                        <<
                    " pos: "         << theBuf.mBufL.GetPos()        <<
                    " size: "        << theBuf.GetSize()             <<
                    " first recov: " << ioFirstGoodRecoveryStripeIdx <<
                    " chunk: "       << theBuf.mChunkId              <<
                    " version: "     << theBuf.mChunkVersion         <<
                    " size: "        << theBuf.mChunkSize            <<
                    " eof: "         << mFileSize                    <<
                KFS_LOG_EOM;
                InvalidChunkSize(inRequest, theBuf,
                    max(theRdSize, ioMaxRd), mStripeCount);
                return false;
            }
        } else if (ioMaxRd < theRdSize) {
            if (ioMaxRd >= 0) {
                if (1 < inIdx && 0 <= ioEndPosHead &&
                        theRdSize <= ioMaxRd + mStripeSize - ioEndPosHead &&
                        theBuf.mChunkSize % mStripeSize == 0 &&
                        IsTailAllZeros(
                            theIt.GetBuffer(), theRdSize - ioMaxRd)) {
                    // NOTE 1.
                    // Stripe 1 cannot ever be larger than stripe 0. If the
                    // start position of the hole is in stripe 0, then its
                    // position cannot ever be lost, if the RS block remains
                    // recoverable, or course, as the hole position
                    // effectively has replication 4 due to recovery stripe
                    // sizes matching the size of stripe 0 in the case if the
                    // stripe 0 is partial (smaller than stripe size).
                    //
                    // It is possible that the original stripe containing the
                    // hole boundary re-appeared, and the current stripe was
                    // recovered without exact knowledge of the size of
                    // the original stripe. In such case the stripe / chunk
                    // must end at the stripe boundary.
                    KFS_LOG_STREAM_INFO << mLogPrefix <<
                        "read recovery possible zero padded larger stripe:"
                        " req: "        << inRequest.mPos        <<
                        ","             << inRequest.mSize       <<
                        " previous short read:"
                        " got: "        << ioMaxRd               <<
                        " expected: "   << theRdSize             <<
                        " stripe: "     << inIdx                 <<
                        " pos: "        << theBuf.mBufL.GetPos() <<
                        " size: "       << theBuf.GetSize()      <<
                        " chunk: "      << theBuf.mChunkId       <<
                        " version: "    << theBuf.mChunkVersion  <<
                        " size: "       << theBuf.mChunkSize     <<
                        " eof: "        << mFileSize             <<
                    KFS_LOG_EOM;
                    theRdSize = ioMaxRd;
                } else {
                    KFS_LOG_STREAM_ERROR << mLogPrefix <<
                        "read recovery failure:"
                        " req: "        << inRequest.mPos        <<
                        ","             << inRequest.mSize       <<
                        " previous short read:"
                        " got: "        << ioMaxRd               <<
                        " expected: "   << theRdSize             <<
                        " stripe: "     << inIdx                 <<
                        " pos: "        << theBuf.mBufL.GetPos() <<
                        " size: "       << theBuf.GetSize()      <<
                        " chunk: "      << theBuf.mChunkId       <<
                        " version: "    << theBuf.mChunkVersion  <<
                        " size: "       << theBuf.mChunkSize     <<
                        " eof: "        << mFileSize             <<
                    KFS_LOG_EOM;
                    InvalidChunkSize(inRequest, theBuf, theRdSize);
                    return false;
                }
            } else {
                ioMaxRd = theRdSize;
            }
        } else if (theRdSize + mStripeSize < ioMaxRd) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "read recovery failure:"
                " req: "        << inRequest.mPos        <<
                ","             << inRequest.mSize       <<
                " short read:"
                " got: "        << theRdSize             <<
                " expected: "   << ioMaxRd               <<
                " stripe: "     << inIdx                 <<
                " pos: "        << theBuf.mBufL.GetPos() <<
                " size: "       << theBuf.GetSize()      <<
                " chunk: "      << theBuf.mChunkId       <<
                " version: "    << theBuf.mChunkVersion  <<
                " size: "       << theBuf.mChunkSize     <<
                " eof: "        << mFileSize             <<
            KFS_LOG_EOM;
            InvalidChunkSize(inRequest, theBuf);
            return false;
        } else if (ioEndPosHead >= 0 && theRdSize > 0 &&
                ioEndPos - ioEndPosHead < theRdSize) {
            // If only part of the stripe is being recovered, then ioEndPosHead
            // can be larger than ioEndPos. In other words, the stripe starts
            // to the left from the beginning of the read buffer.
            const int theFrontSize = max(0, ioEndPos - ioEndPosHead);
            if (1 < inIdx &&
                    theRdSize <= ioEndPos - ioEndPosHead + mStripeSize &&
                    theBuf.mChunkSize % mStripeSize == 0 &&
                    IsTailAllZeros(
                        theIt.GetBuffer(), theRdSize - theFrontSize)) {
                // See NOTE 1. the above, the asme applies here.
                KFS_LOG_STREAM_INFO << mLogPrefix <<
                    "read recovery possible zero padded larger stripe:"
                    " req: "         << inRequest.mPos        <<
                    ","              << inRequest.mSize       <<
                    " previous short read:"
                    " stripe: "      << ioEndPosIdx           <<
                    " stripe head: " << ioEndPosHead          <<
                    " got: "         << ioEndPos              <<
                    " expected: "    << theRdSize             <<
                    " cur stripe: "  << inIdx                 <<
                    " pos: "         << theBuf.mBufL.GetPos() <<
                    " size: "        << theBuf.GetReadSize()  <<
                    " chunk: "       << theBuf.mChunkId       <<
                    " version: "     << theBuf.mChunkVersion  <<
                    " size: "        << theBuf.mChunkSize     <<
                    " eof: "         << mFileSize             <<
               KFS_LOG_EOM;
                theRdSize = theFrontSize;
            } else {
                KFS_LOG_STREAM_ERROR << mLogPrefix <<
                    "read recovery failure:"
                    " req: "         << inRequest.mPos        <<
                    ","              << inRequest.mSize       <<
                    " previous short read:"
                    " stripe: "      << ioEndPosIdx           <<
                    " stripe head: " << ioEndPosHead          <<
                    " got: "         << ioEndPos              <<
                    " expected: "    << theRdSize             <<
                    " cur stripe: "  << inIdx                 <<
                    " pos: "         << theBuf.mBufL.GetPos() <<
                    " size: "        << theBuf.GetReadSize()  <<
                    " chunk: "       << theBuf.mChunkId       <<
                    " version: "     << theBuf.mChunkVersion  <<
                    " size: "        << theBuf.mChunkSize     <<
                    " eof: "         << mFileSize             <<
                KFS_LOG_EOM;
                InvalidChunkSize(inRequest, inRequest.GetBuffer(ioEndPosIdx));
                return false;
            }
        }
        if (theRdSize >= ioRecoverySize) {
            // Ensure that the chunk end position is not withing the last
            // stripe, in the cases when recovery end position isn't stripe
            // aligned, and/or less than stripe size, otherwise use chunk sizes
            // to determine the stripe position where RS block ends.
            // The two if conditions and the first part of the second if
            // condition below is to avoid modulo operation (%), when possible.
            if ((Offset)CHUNKSIZE <= theBuf.mChunkSize ||
                    (0 <= ioEndPosHead &&
                        inIdx != ioFirstGoodRecoveryStripeIdx)) {
                return true;
            }
            const int theChunkPos = GetChunkPos(inRequest.mRecoveryPos);
            if (theChunkPos + mStripeSize <= theBuf.mChunkSize ||
                    theChunkPos - theChunkPos % mStripeSize +
                        mStripeSize <= theBuf.mChunkSize) {
                return true;
            }
        }
        if (inIdx < mStripeCount) {
            if (ioEndPosHead < 0) {
                ioEndPosIdx  = inIdx;
                ioEndPos     = theRdSize;
                ioEndPosHead =
                    (inRequest.mRecoveryPos + theRdSize) % mStripeSize;
                if (ioEndPosHead == 0 &&
                        theBuf.mChunkSize < (Offset)CHUNKSIZE) {
                    // If end is stripe aligned, see if the end is in preceding
                    // stripes. The stripe with the end that isn't stripe
                    // aligned is where the end is. If the recovery size is less
                    // than stripe size, the read of this stripe might not not
                    // return less that recovery size.
                    const Offset theMinSize = theBuf.mChunkSize + mStripeSize;
                    for (int i = inIdx - 2; 0 <= i; i--) {
                        const Buffer& theCBuf = inRequest.GetBuffer(i);
                        if (! theCBuf.IsFailed() &&
                                theCBuf.mChunkSize < theMinSize) {
                            // Set end position to the stripe after this one,
                            // as the read of this stripe was not less than
                            // recovery size.
                            // RS block should have only one partial stripe.
                            ioEndPosIdx = i + 1;
                            break;
                        }
                    }
                }
            } else if (ioEndPosHead == 0 && ioMaxRd <= ioEndPos) {
                if (ioEndPos != theRdSize) {
                    ioEndPosHead = mStripeSize - (ioEndPos - theRdSize);
                    if (ioEndPosHead < 0 || ioEndPosHead >= mStripeSize) {
                        InternalError("undetected previous short read");
                        inRequest.mStatus = kErrorIO;
                        return false;
                    }
                }
                ioEndPosIdx = inIdx;
                ioEndPos    = theRdSize;
            }
        } else {
            if (ioEndPosHead == 0) {
                if (ioEndPos == theRdSize) {
                    // The recovery stripe ends at the stripe boundary.
                    if (ioEndPosIdx < mStripeCount - 1) {
                        // Then next stripe could be the last, and it *must* be
                        // missing.
                        ioEndPosIdx++;
                        if (! inRequest.GetBuffer(ioEndPosIdx).IsFailed()) {
                            InternalError(
                                "undetected previous invalid read");
                            inRequest.mStatus = kErrorIO;
                            return false;
                        }
                    }
                } else if (ioEndPosIdx == 0 ||
                        theRdSize < ioEndPos ||
                        ioEndPos + mStripeSize < theRdSize ||
                        (ioEndPos + mStripeSize != theRdSize &&
                        ! inRequest.GetBuffer(0).IsFailed())) {
                    InternalError("undetected previous invalid read size");
                    inRequest.mStatus = kErrorIO;
                    return false;
                } else {
                    ioEndPosHead = theRdSize - ioEndPos;
                    if (ioEndPosHead < mStripeSize) {
                        ioEndPosIdx = 0; // Partial first stripe.
                        ioEndPos    = theRdSize;
                    } else {
                        ioEndPosHead = 0; // Last stripe index doesn't change.
                    }
                }
            } else if (ioEndPosHead < 0) {
                ioEndPosHead =
                    (inRequest.mRecoveryPos + theRdSize) % mStripeSize;
                ioEndPos     = theRdSize;
                if (ioEndPosHead != 0) {
                    // Partial first stripe, otherwise recovery end is stripe
                    // aligned. It is possible to get here with 1+3 encoding
                    // (which probably makes sense for testing only) when the
                    // first stripe is missing.
                    ioEndPosIdx = 0;
                } else {
                    ioEndPosIdx = mStripeCount - 1;
                }
            }
            KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                "read recovery:"
                " req: "                       << inRequest.mPos  <<
                ","                            << inRequest.mSize <<
                " size: "                      << ioRecoverySize  <<
                " recovery stripe read size: " << theRdSize       <<
                " end:"
                " stripe: "                    << ioEndPosIdx     <<
                " pos: "                       << ioEndPos        <<
                " head: "                      << ioEndPosHead    <<
            KFS_LOG_EOM;
            ioRecoverySize = theRdSize;
            UpdateMaxLength(theRdSize, ioMaxLength);
        }
        return true;
    }
    void InitRecoveryStripeRestore(
        Request& inRequest,
        int      inIdx,
        int      inRecoverySize,
        int      inMissingCnt,
        bool     inAllRecoveryStripesRebuildFlag,
        int&     ioRRdSize)
    {
        // Init recovery stripe buffers and iterators, if recovery stripe itself
        // needs to be computed. RecoverChunk() the above must schedule the
        // first stripe read in order for "hole" / block end logic to work
        // properly: the recovery stripes length is always greater or equal to
        // the the first stripe.
        const int theBufCount = GetBufferCount();
        QCRTASSERT(
            inIdx >= mStripeCount &&
            inIdx <= theBufCount &&
            inRequest.GetBuffer(0).mBuf.mSize > 0
        );
        if (ioRRdSize < 0) {
            if (inMissingCnt >= inIdx + 1) {
                // All preceding stripes and the current one are missing or
                // absent (hole), find the size from any successfully read
                // recovery stripe.
                // SetBufIterator() will declare an error later if recovery
                // stripe reads don't have the same size.
                for (int k = inIdx + 1; k < theBufCount; k++) {
                    ioRRdSize = max(
                        ioRRdSize,
                        inRequest.GetBuffer(k).GetReadSize()
                    );
                }
            } else {
                ioRRdSize = inRecoverySize;
            }
        }
        BufIterator& theIt = mBufIteratorsPtr[inIdx];
        theIt.Clear();
        if (inIdx == mRecoverStripeIdx) {
            Buffer& theBuf = inRequest.GetBuffer(inIdx);
            theBuf.Clear();
            theBuf.mPos = inRequest.mRecoveryPos + ioRRdSize +
                inIdx * (Offset)CHUNKSIZE;
            theBuf.mBufL.mSize = ioRRdSize;
            theBuf.MarkFailed();
            int theRdSize = 0;
            QCVERIFY(theIt.Set(*this, theBuf, theRdSize) == ioRRdSize);
        } else {
            if (inAllRecoveryStripesRebuildFlag) {
                theIt.MakeScratchBuffer(*this, ioRRdSize);
            }
        }
    }
    void FinishRecovery(
        Request& inRequest)
    {
        if (mStripeCount <= mRecoverStripeIdx && inRequest.mRecoverySize <= 0) {
            QCASSERT(mRecoverStripeIdx < GetBufferCount());
            inRequest.mRecoverySize = -inRequest.mRecoverySize;
        }
        if ((inRequest.mBadStripeCount <= 0 &&
                mRecoverStripeIdx < mStripeCount) ||
                inRequest.mRecoverySize <= 0) {
            return;
        }
        if (inRequest.mBadStripeCount > mRecoveryStripeCount) {
            if (inRequest.mStatus == 0) {
                inRequest.mStatus = kErrorIO;
            }
            return;
        }
        if (! mDecoderPtr) {
            InternalError("no decoder");
            inRequest.mStatus = kErrorParameters;
            return;
        }
        const int theBufCount = GetBufferCount();
        if (! mBufIteratorsPtr) {
            mBufIteratorsPtr = new BufIterator[theBufCount];
        }
        const bool theAllRecoveryStripesRebuildFlag =
            mStripeCount <= mRecoverStripeIdx &&
            ! mDecoderPtr->SupportsOneRecoveryStripeRebuild();
        StBufferT<int, 32> theTmpBuf;
        int* const theMissingIdx     =
            theTmpBuf.Resize(mRecoveryStripeCount + 1);
        int        theMissingCnt     = 0;
        int        theSize           = inRequest.mRecoverySize;
        int        thePrevLen        = 0;
        int        theMaxRd          = -1;
        int        theRecovIdx       = -1;
        int        theEndPosIdx      = mStripeCount;
        int        theEndPos         = theSize;
        int        theNextEndPos     = theSize;
        int        theRSize          = -1;
        int        theBufToCopyCount = mStripeCount;
        int        theEndPosHead     = -1;
        theMissingIdx[mRecoveryStripeCount] = -1; // Jerasure end of list.
        for (int thePos = 0; thePos < theSize; ) {
            int theLen = theSize - thePos;
            if (theLen > kAlign) {
                theLen -= theLen % kAlign;
            }
            for (int i = 0; i < theBufCount; i++) {
                if (thePos == 0 &&
                    ! SetBufIterator(
                        inRequest,
                        i,
                        theMissingCnt,
                        theSize,
                        theMaxRd,
                        theRecovIdx,
                        theLen,
                        theMissingIdx,
                        theEndPosIdx,
                        theEndPos,
                        theEndPosHead)) {
                    QCASSERT(
                        mRecoverStripeIdx < mStripeCount ||
                        inRequest.mStatus != 0
                    );
                    for (int k = 0; k <= i; k++) {
                        mBufIteratorsPtr[k].Clear();
                    }
                    mRecoveryInfo.Set(*this, inRequest);
                    return;
                }
                BufIterator& theIt = mBufIteratorsPtr[i];
                if (i >= mStripeCount &&
                        (theIt.IsFailure() || ! theIt.IsRequested())) {
                    if (mRecoverStripeIdx < mStripeCount) {
                        mBufPtr[i] = 0;
                        continue;
                    }
                    if (thePos == 0) {
                        InitRecoveryStripeRestore(
                            inRequest, i, theSize, theMissingCnt,
                            theAllRecoveryStripesRebuildFlag, theRSize);
                        theBufToCopyCount = theBufCount;
                    }
                }
                if (theSize <= 0) {
                    QCASSERT(thePos == 0);
                    mBufPtr[i] = 0;
                    continue; // Hole of eof, continue to check the chunk sizes.
                }
                char*     thePtr = theIt.Advance(thePrevLen);
                const int theRem = theIt.GetCurRemaining();
                if (! thePtr) {
                    if (theRem != 0 || theAllRecoveryStripesRebuildFlag ||
                            i < mStripeCount) {
                        InternalError("null recovery buffer");
                    }
                    mBufPtr[i] = 0;
                    continue;
                }
                if (theRem < kAlign || (thePtr - kNullCharPtr) % kAlign != 0) {
                    thePtr = GetTempBufPtr(i);
                    if (theLen > kTempBufSize) {
                        theLen = kTempBufSize;
                        QCASSERT(theLen % kAlign == 0);
                    }
                    if (theIt.IsFailure()) {
                        QCASSERT(
                            i < mStripeCount ||
                            mRecoverStripeIdx >= mStripeCount
                        );
                    } else {
                        const int theSize = theIt.CopyOut(thePtr, theLen);
                        if (theSize < theLen) {
                            theLen = theSize;
                            if (theLen > kAlign) {
                                theLen -= theLen % kAlign;
                            }
                        }
                    }
                } else if (theRem < theLen) {
                    theLen = theRem - theRem % kAlign;
                }
                mBufPtr[i] = thePtr;
            }
            if (thePos == 0) {
                mRecoveriesCount++;
                if (theEndPosHead >= 0) {
                    const int    theChunkStridePos  =
                        GetChunkPos(inRequest.mRecoveryPos) + theEndPos -
                        (theEndPosHead > 0 ? theEndPosHead :
                            (theEndPos < theSize ? 0 : mStripeSize));
                    const Offset theBlockPos        =
                        inRequest.mPos - inRequest.mPos % mChunkBlockSize;
                    const Offset theHolePos         =
                        theBlockPos +
                        theChunkStridePos * mStripeCount +
                        theEndPosIdx * mStripeSize +
                        theEndPosHead;
                    if (! mFailShortReadsFlag) {
                        if (theHolePos <= inRequest.mPos) {
                            KFS_LOG_STREAM_INFO << mLogPrefix <<
                                "read recovery:"
                                " req: "  << inRequest.mPos                  <<
                                ","       << inRequest.mSize                 <<
                                " size: " << theSize                         <<
                                " hole [" << theHolePos                      <<
                                ","       << (theBlockPos + mChunkBlockSize) <<
                                ")" <<
                            KFS_LOG_EOM;
                            break; // Hole or eof -- nothing to do.
                        }
                    } else if (theHolePos < inRequest.mPos + inRequest.mSize) {
                        Buffer& theBuf = inRequest.GetBuffer(theEndPosIdx);
                        KFS_LOG_STREAM_ERROR << mLogPrefix <<
                            "read recovery:"
                            " short read detected:"
                            " req: "    << inRequest.mPos       <<
                            ","         << inRequest.mSize      <<
                            " hole: "   << theHolePos           <<
                            " size: "   << theSize              <<
                            " end:"
                            " stripe: " << theEndPosIdx         <<
                            " size: "   << theEndPos            <<
                            " read: "   << theBuf.GetReadSize() <<
                            " head: "   << theEndPosHead        <<
                        KFS_LOG_EOM;
                        if (theEndPosIdx < mStripeCount &&
                                ! theBuf.IsFailed()) {
                            InvalidChunkSize(inRequest, theBuf);
                        } else {
                            // Assume that the recovery stripe size is invalid.
                            InvalidChunkSize(
                                inRequest,
                                inRequest.GetBuffer(theBufCount - 1),
                                theSize + 1,
                                mStripeCount
                            );
                        }
                        for (int i = 0; i < theBufCount; i++) {
                            mBufIteratorsPtr[i].Clear();
                        }
                        mRecoveryInfo.Set(*this, inRequest);
                        return;
                    }
                }
                QCASSERT(
                    theMissingCnt == mRecoveryStripeCount ||
                    inRequest.mRecoveryRound > 0
                );
                for (int i = theBufCount - 1;
                        theMissingCnt < mRecoveryStripeCount &&
                            mStripeCount <= i;
                        i--) {
                    if (mBufPtr[i]) {
                        // If recovery stripe restore requested, and all
                        // recovery stripe buffers are *not* required to be
                        // present for Decode() to work, then declare the stripe
                        // missing and clear the buffers.
                        if (! theAllRecoveryStripesRebuildFlag &&
                                i != mRecoverStripeIdx) {
                            mBufIteratorsPtr[i].Clear();
                            mBufPtr[i] = 0;
                        }
                        theMissingIdx[theMissingCnt++] = i;
                    }
                }
                if (theEndPosIdx + 1 < mStripeCount) {
                    theNextEndPos = max(0, theEndPos - theEndPosHead);
                }
            }
            QCRTASSERT(
                theLen > 0 &&
                (theLen % kAlign == 0 || theLen < kAlign) &&
                theMissingCnt == mRecoveryStripeCount
            );
            if (thePos == 0 || thePos + theLen >= theSize) {
                KFS_LOG_STREAM_INFO << mLogPrefix       <<
                    "read recovery"
                    " req: "  << inRequest.mPos         <<
                    ","       << inRequest.mSize        <<
                    " pos: "  << inRequest.mRecoveryPos <<
                    "+"       << thePos                 <<
                    " size: " << theLen                 <<
                    " of: "   << theSize                <<
                KFS_LOG_EOM;
            }
            const int theRet = mDecoderPtr->Decode(
                mStripeCount,
                mRecoveryStripeCount,
                max(theLen, (int)kAlign),
                mBufPtr,
                theMissingIdx
            );
            if (theRet != 0) {
                KFS_LOG_STREAM_ERROR << mLogPrefix       <<
                    "read reocvery decode failure"
                    " status: " << theRet <<
                    " req: "    << inRequest.mPos         <<
                    ","         << inRequest.mSize        <<
                    " pos: "    << inRequest.mRecoveryPos <<
                    "+"         << thePos                 <<
                    " size: "   << theLen                 <<
                    " of: "     << theSize                <<
                KFS_LOG_EOM;
                inRequest.mStatus = kErrorIO;
                return;
            }
            for (int i = 0; i < theBufToCopyCount; i++) {
                BufIterator& theIt = mBufIteratorsPtr[i];
                const int theCpLen = (i < theEndPosIdx || i >= mStripeCount) ?
                    theLen :
                    min(theLen, (i == theEndPosIdx ?
                        theEndPos : theNextEndPos) - thePos);
                if (theCpLen <= 0) {
                    QCRTASSERT(i < mStripeCount);
                    i = mStripeCount - 1;
                    continue;
                }
                if (! theIt.IsFailure()) {
                    continue;
                }
                if (theIt.CopyIn(mBufPtr[i], theCpLen) != theCpLen) {
                    InternalError("invalid copy size");
                }
            }
            thePos += theLen;
            thePrevLen = theLen;
        }
        mRecoveryInfo.Set(*this, inRequest);
        for (int i = 0; i < mStripeCount; i++) {
            mBufIteratorsPtr[i].SetRecoveryResult(inRequest.GetBuffer(i));
        }
        for (int i = mStripeCount; i < theBufCount; i++) {
            if (mRecoverStripeIdx == i) {
                Buffer& theBuf = inRequest.GetBuffer(i);
                // Use middle buffer for RequestCompletion() to work.
                if (theBuf.mBuf.mSize <= 0) {
                    QCASSERT(
                        theBuf.mPos >= 0 &&
                        theBuf.mPos >= theBuf.mBufL.mSize &&
                        theBuf.mBufL.mSize >= theSize
                    );
                    theBuf.mPos -= theBuf.mBufL.mSize;
                    theBuf.mBuf.mSize = theSize;
                    theBuf.mBuf.mBuffer.Clear();
                    theBuf.mBuf.MarkFailed();
                    // Set the right buffer size to the padded size, if any, to
                    // ensure that the total size matches the iterator buffer
                    // size in the case when no recovery run on this stripe, as
                    // otherwise assertion in SetRecoveryResult() will fail.
                    theBuf.mBufR.Clear();
                    theBuf.mBufR.mSize = theBuf.mBufL.mSize - theSize;
                    theBuf.mBufR.MarkFailed();
                    theBuf.mBufL.mSize = 0;
                    theBuf.mBufL.mBuffer.Clear();
                    mBufIteratorsPtr[i].SetRecoveryResult(theBuf);
                }
            } else {
                inRequest.GetBuffer(i).Clear();
                mBufIteratorsPtr[i].Clear();
            }
        }
    }
    template <typename T>
    static void IOBufferWrite(
        IOBuffer& inBuffer,
        const T&  inVal)
    {
        inBuffer.CopyIn(reinterpret_cast<const char*>(&inVal), sizeof(inVal));
    }
    static bool IsTailAllZeros(
        const IOBuffer& inBuffer,
        int             inSize)
    {
        int theRem = inSize;
        IOBuffer::iterator theIt = inBuffer.end();
        while (0 < theRem && inBuffer.begin() != theIt) {
            --theIt;
            const char* const theEndPtr = theIt->Producer();
            const char*       thePtr    = theIt->Consumer();
            if (theEndPtr <= thePtr) {
                continue;
            }
            if (thePtr + theRem < theEndPtr) {
                thePtr = theEndPtr - theRem;
            }
            theRem -= (int)(theEndPtr - thePtr);
            const char*const  kNullPtr       = 0;
            const size_t      kAlign         = 2 * sizeof(uint64_t);
            const char* const theFrontEndPtr =
                min(theEndPtr, thePtr + (thePtr - kNullPtr) % kAlign);
            while (thePtr < theFrontEndPtr && *thePtr == 0) {
                ++thePtr;
            }
            if (thePtr < theFrontEndPtr) {
                return false;
            }
            const char* const theTailStartPtr =
                theEndPtr - (theEndPtr - kNullPtr) % kAlign;
            while (thePtr < theTailStartPtr &&
                    (reinterpret_cast<const uint64_t*>(thePtr)[0] |
                     reinterpret_cast<const uint64_t*>(thePtr)[1]) == 0) {
                thePtr += 2 * sizeof(uint64_t);
            }
            if (thePtr < theTailStartPtr) {
                return false;
            }
            while (thePtr < theEndPtr && *thePtr == 0) {
                ++thePtr;
            }
            if (thePtr < theEndPtr) {
                return false;
            }
        }
        return (theRem <= 0);
    }
    void RequestCompletion(
        Request& inRequest)
    {
        QCASSERT(inRequest.mPendingCount == 0);
        Requests::Remove(mInFlightList, inRequest);
        IOBuffer theBuffer;
        if (mRecoverStripeIdx >= 0) {
            const int       theStatus = inRequest.mStatus;
            const Offset    thePos    =
                inRequest.mRecoveryPos + mRecoverStripeIdx * (Offset)CHUNKSIZE;
            const int       theSize   = inRequest.mRecoverySize >= 0 ?
                inRequest.mRecoverySize : -inRequest.mRecoverySize;
            const RequestId theId     = inRequest.mRequestId;
            if (theStatus == 0) {
                Buffer& theBuf = inRequest.GetBuffer(mRecoverStripeIdx);
                QCASSERT(thePos == theBuf.mPos);
                theBuffer.Move(&theBuf.mBuf.mBuffer,
                    (int)(mRecoverChunkEndPos - thePos));
            } else if (theStatus == kErrorInvalidChunkSizes) {
                // Report chunks with invalid sizes.
                const int theBufCount = GetBufferCount();
                for (int i = 0; i < theBufCount; i++) {
                    const Buffer& theBuf = inRequest.GetBuffer(i);
                    if (theBuf.GetStatus() != kErrorInvalidChunkSizes) {
                        continue;
                    }
                    IOBufferWrite(theBuffer, i);
                    IOBufferWrite(theBuffer, theBuf.mChunkId);
                    IOBufferWrite(theBuffer, theBuf.mChunkVersion);
                }
            }
            PutRequest(inRequest);
            ReportCompletion(theStatus, theBuffer, theSize, thePos, theId,
                mRecoveriesCount);
            return;
        }
        SetPos(inRequest.mPos);
        int        theLen            = inRequest.mSize;
        bool       theEndOfBlockFlag = false;
        const bool theOkFlag         = inRequest.mStatus == 0;
        while (theLen > 0) {
            const int theSize = min(theLen, GetStripeRemaining());
            Buffer&   theBuf  = inRequest.GetBuffer(GetStripeIdx());
            if (theEndOfBlockFlag) {
                // No holes withing RS blocks are currently supported, therefore
                // all subsequent stripes must be shorter by stripe size, or
                // zero padded.
                if (! theBuf.mBuf.mBuffer.IsEmpty()) {
                    if (! IsTailAllZeros(theBuf.mBuf.mBuffer,
                            theBuf.mBuf.mBuffer.BytesConsumable())) {
                        if (inRequest.mStatus == 0) {
                            inRequest.mStatus = kErrorInvalChunkSize;
                        }
                        KFS_LOG_STREAM_ERROR << mLogPrefix <<
                            "chunk sizes mismatch detected:" <<
                            " pos: "       << GetPos() <<
                            " chunk: "     << theBuf.mChunkId <<
                            " version: "   << theBuf.mChunkVersion <<
                            " size: "      << theBuf.mChunkSize <<
                            " chunk pos: " << GetChunkPos(GetFilePos()) <<
                            " bytes: "     <<
                                theBuf.mBuf.mBuffer.BytesConsumable() <<
                            " are not all zeros,"
                            " the previous stipe / "
                            " chunk size might be truncated" <<
                        KFS_LOG_EOM;
                        InvalidChunkSize(inRequest, theBuf);
                    } else {
                        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                            " pos: "       << GetPos() <<
                            " chunk: "     << theBuf.mChunkId <<
                            " version: "   << theBuf.mChunkVersion <<
                            " size: "      << theBuf.mChunkSize <<
                            " chunk pos: " << GetChunkPos(GetFilePos()) <<
                            " bytes: "     <<
                                theBuf.mBuf.mBuffer.BytesConsumable() <<
                            " discarded at the end of RS block" <<
                        KFS_LOG_EOM;
                    }
                    theBuf.mBuf.mBuffer.TrimAndConvertRemainderToAvailableSpace(
                        0);
                }
            } else {
                theEndOfBlockFlag = theOkFlag &&
                    theBuf.mBuf.mBuffer.BytesConsumable() < theSize;
            }
            theBuffer.MoveSpace(&theBuf.mBuf.mBuffer, theSize);
            theLen -= theSize;
            // The last seek sets position for the next sequential read request.
            // The next SetPos() call will have nothing to do in this case.
            if (SeekStripe(theSize) && theLen != 0) {
                InternalError("invalid request size");
            }
        }
        const int       theStatus = inRequest.mStatus;
        const int       theSize   = inRequest.mSize;
        const Offset    thePos    = inRequest.mPos;
        const RequestId theId     = inRequest.mRequestId;
        PutRequest(inRequest);
        ReportCompletion(theStatus, theBuffer, theSize, thePos, theId,
            mRecoveriesCount);
    }
    Request& GetRequest(
        RequestId inRequestId,
        Offset    inPos,
        int       inSize)
    {
        if (Requests::IsEmpty(mFreeList)) {
            return Request::Create(*this, inRequestId, inPos, inSize);
        }
        Request& theRet = *Requests::PopFront(mFreeList);
        theRet.Reset(*this, inRequestId, inPos, inSize);
        return theRet;
    }
    void PutRequest(
        Request& inRequest)
    {
        inRequest.Reset(*this, RequestId(), 0, 0);
        Requests::PushFront(mFreeList, inRequest);
    }
    static bool IsFailure(
        int inStatus)
        { return (inStatus != 0 && inStatus != kErrorNoEntry); }
    void ZeroPaddTo(
        IOBuffer& inBuffer,
        int       inSize)
    {
        int theRem = inSize - inBuffer.BytesConsumable();
        theRem -= inBuffer.ZeroFillSpaceAvailable(theRem);
        inBuffer.RemoveSpaceAvailable();
        while (theRem > 0) {
            IOBufferData theBuf = GetZeroBuffer();
            theRem -= theBuf.Trim(theRem);
            inBuffer.Append(theBuf);
        }
    }
    IOBufferData GetZeroBuffer()
    {
        if (! mZeroBufferPtr) {
            if (mUseDefaultBufferAllocatorFlag) {
                mZeroBufferPtr = new IOBufferData();
                QCASSERT(
                    (mZeroBufferPtr->Producer() - kNullCharPtr) % kAlign == 0 &&
                    mZeroBufferPtr->SpaceAvailable() > 0 &&
                    mZeroBufferPtr->SpaceAvailable() % kAlign == 0
                );
            } else {
                mZeroBufferPtr = new IOBufferData(NewDataBuffer(4 << 10));
            }
            mZeroBufferPtr->ZeroFill(mZeroBufferPtr->SpaceAvailable());
        }
        return *mZeroBufferPtr;
    }
    char* GetTempBufPtr(
        int inIndex)
    {
        return GetTempBufSelfPtr(inIndex, mStripeCount + mRecoveryStripeCount);
    }
    int Rand()
    {
        mNextRand = mNextRand * 1103515245 + 12345;
        return (int)((uint32_t)(mNextRand / 65536) % 32768);
    }
private:
    RSReadStriper(
        const RSReadStriper& inRSReadStriper);
    RSReadStriper& operator=(
        const RSReadStriper& inRSReadStriper);
};

    Writer::Striper*
RSStriperCreate(
    int                      inType,
    int                      inStripeCount,
    int                      inRecoveryStripeCount,
    int                      inStripeSize,
    Writer::Striper::Offset  inFileSize,
    const string&            inLogPrefix,
    Writer::Striper::Impl&   inOuter,
    Writer::Striper::Offset& outOpenChunkBlockSize,
    string&                  outErrMsg)
{
    return RSWriteStriper::Create(
        inType,
        inStripeCount,
        inRecoveryStripeCount,
        inStripeSize,
        inFileSize,
        inLogPrefix,
        inOuter,
        outOpenChunkBlockSize,
        outErrMsg
    );
}

    Reader::Striper*
RSStriperCreate(
    int                      inType,
    int                      inStripeCount,
    int                      inRecoveryStripeCount,
    int                      inStripeSize,
    int                      inMaxReadRequestSize,
    bool                     inUseDefaultBufferAllocatorFlag,
    bool                     inFailShortReadsFlag,
    Reader::Striper::Offset  inRecoverChunkPos,
    Reader::Striper::Offset  inFileSize,
    Reader::Striper::SeqNum  inInitialSeqNum,
    const string&            inLogPrefix,
    Reader::Striper::Impl&   inOuter,
    Reader::Striper::Offset& outOpenChunkBlockSize,
    string&                  outErrMsg)
{
    return RSReadStriper::Create(
        inType,
        inStripeCount,
        inRecoveryStripeCount,
        inStripeSize,
        inMaxReadRequestSize,
        inUseDefaultBufferAllocatorFlag,
        inFailShortReadsFlag,
        inRecoverChunkPos,
        inFileSize,
        inInitialSeqNum,
        inLogPrefix,
        inOuter,
        outOpenChunkBlockSize,
        outErrMsg
    );
}

    bool
RSStriperValidate(
    int     inType,
    int     inStripeCount,
    int     inRecoveryStripeCount,
    int     inStripeSize,
    string* outErrMsgPtr)
{
    return RSStriper::Validate(
        inType, inStripeCount, inRecoveryStripeCount, inStripeSize,
        outErrMsgPtr);
}

}} /* namespace client KFS */
