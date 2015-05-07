//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2011/09/16
// Author: Mike Ovsiannikov
//
// Copyright 2011-2012 Quantcast Corp.
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
// \file MdStream.h
// \brief Message digest stream object.
//
//----------------------------------------------------------------------------

#ifndef MD_STREAM_H
#define MD_STREAM_H

#include <ostream>
#include <string>
#include <algorithm>

#include <string.h>
#include <openssl/evp.h>

namespace KFS
{
using std::string;
using std::ostream;
using std::streambuf;
using std::streamsize;
using std::max;

class MdStateCtx
{
public:
    MdStateCtx(
        EVP_MD_CTX const& inCtx)
        : mCtx(inCtx)
        {}
    EVP_MD_CTX const& mCtx;
};

template<typename OStreamT>
class MdStreamT :
    private streambuf,
    public ostream
{
public:
    typedef unsigned char MD[EVP_MAX_MD_SIZE];

    static void Init()
    {
        OpenSSL_add_all_digests();
    }
    static void Cleanup()
    {
        EVP_cleanup();
    }
    static size_t GetMinBufSize(
        size_t inBufSize)
        { return max(size_t(128), inBufSize); }
    MdStreamT(
        OStreamT*     inStreamPtr  = 0,
        bool          inSyncFlag   = true,
        const string& inDigestName = string(),
        size_t        inBufSize    = (1 << 20),
        bool          inResizeFlag = false)
        : streambuf(),
          ostream(this),
          mDigestName(inDigestName),
          mBufferPtr(new char[GetMinBufSize(inBufSize)]),
          mCurPtr(mBufferPtr),
          mEndPtr(mCurPtr + GetMinBufSize(inBufSize)),
          mSyncFlag(inSyncFlag),
          mWriteTroughFlag(inBufSize <= 0),
          mStreamPtr(inStreamPtr),
          mNextSize(inResizeFlag ? (8 << 10) : 0)
    {
        EVP_MD_CTX_init(&mCtx);
        MdStreamT::InitMd();
    }
    virtual ~MdStreamT()
    {
        EVP_MD_CTX_cleanup(&mCtx);
        delete [] mBufferPtr;
    }
    void SetMdState(
        MdStateCtx inState)
    {
        EVP_MD_CTX_cleanup(&mCtx);
        EVP_MD_CTX_init(&mCtx);
        if (! EVP_MD_CTX_copy_ex(&mCtx, &(inState.mCtx))) {
            setstate(failbit);
        }
    }
    MdStateCtx GetMdState() const
        { return MdStateCtx(mCtx); }
    size_t GetMdBin(
        MD& inMd)
    {
        flush();
        SyncSelf();
        if (fail()) {
            return 0;
        }
        EVP_MD_CTX theCtx;
        EVP_MD_CTX_init(&theCtx);
        if (! EVP_MD_CTX_copy_ex(&theCtx, &mCtx)) {
            setstate(failbit);
            return 0;
        }
        unsigned int theLen = 0;
        if (! EVP_DigestFinal_ex(&theCtx, inMd, &theLen)) {
            setstate(failbit);
        }
        EVP_MD_CTX_cleanup(&theCtx);
        if (fail()) {
            return 0;
        }
        return theLen;
    }
    string GetMd()
    {
        MD           theMd;
        const size_t theLen = GetMdBin(theMd);
        string       theRet;
        theRet.resize(2 * theLen);
        string::iterator theIt = theRet.begin();
        const char* const kHexDigits = "0123456789abcdef";
        for (unsigned int i = 0; i < theLen; i++) {
            const unsigned int theDigit = theMd[i] & 0xFF;
            *theIt++ = kHexDigits[(theDigit >> 4) & 0xF];
            *theIt++ = kHexDigits[theDigit & 0xF];
        }
        return theRet;
    }
    ostream& SetSync(
        bool inFlag)
    {
        mSyncFlag = inFlag;
        return *this;
    }
    bool IsSync() const
        { return mSyncFlag; }
    ostream& SetStream(
        OStreamT* inStreamPtr)
    {
        flush();
        SyncSelf();
        mStreamPtr = inStreamPtr;
        return *this;
    }
    ostream& Reset(
        OStreamT* inStreamPtr = 0)
    {
        flush();
        SyncSelf();
        unsigned char theMd[EVP_MAX_MD_SIZE];
        EVP_DigestFinal_ex(&mCtx, theMd, 0);
        clear();
        InitMd();
        mStreamPtr = inStreamPtr;
        return *this;
    }
    ostream& SetWriteTrough(
        bool inWriteTroughFlag)
    {
        mWriteTroughFlag = inWriteTroughFlag || ! mBufferPtr;
        return *this;
    }
    const char* GetBufferedStart() const
        { return mBufferPtr; }
    const char* GetBufferedEnd() const
        { return mCurPtr; }
protected:
    bool EnsureCapacity(
        size_t inSize)
    {
        if (mNextSize <= 0) {
            return false;
        }
        if (mCurPtr + inSize < mEndPtr) {
            return true;
        }
        const size_t theCurSize = mCurPtr - mBufferPtr;
        const size_t theSize    = theCurSize + inSize;
        do {
            mNextSize += mNextSize;
        } while (mNextSize < theSize);
        char* const thePtr = new char[mNextSize];
        memcpy(thePtr, mBufferPtr, theCurSize);
        delete [] mBufferPtr;
        mBufferPtr = thePtr;
        mCurPtr    = thePtr + theCurSize;
        mEndPtr    = thePtr + mNextSize;
        return true;
    }
    virtual int overflow(
        int inSym = EOF)
    {
        if (inSym == EOF) {
            return EOF;
        }
        if (mEndPtr <= mCurPtr && ! EnsureCapacity(1)) {
            SyncSelf();
            if (mEndPtr <= mCurPtr) {
                return EOF;
            }
        }
        *mCurPtr++ = inSym;
        return inSym;
    }
    virtual streamsize xsputn(
        const char* inBufferPtr,
        streamsize  inSize)
    {
        if (inSize <= 0) {
            return inSize;
        }
        if (! mWriteTroughFlag && (EnsureCapacity(inSize) ||
                mBufferPtr + inSize * 3 / 2 < mEndPtr)) {
            streamsize theSize = 0;
            streamsize theRem  = inSize;
            if (mCurPtr < mEndPtr) {
                if (mCurPtr + inSize > mEndPtr) {
                    theSize = (streamsize)(mEndPtr - mCurPtr);
                } else {
                    theSize = inSize;
                }
                memcpy(mCurPtr, inBufferPtr, theSize);
                mCurPtr += theSize;
                theRem  -= theSize;
                if (theRem <= 0) {
                    return inSize;
                }
            }
            SyncSelf();
            memcpy(mCurPtr, inBufferPtr + theSize, theRem);
            mCurPtr += theRem;
            return inSize;
        }
        if (SyncSelf() == 0 &&
                ! fail() && ! EVP_DigestUpdate(
                &mCtx, inBufferPtr, inSize)) {
            setstate(failbit);
        }
        if (mStreamPtr) {
            mStreamPtr->write(inBufferPtr, inSize);
        }
        return inSize;
    }
    virtual int sync()
    {
        if (! mSyncFlag) {
            return 0;
        }
        const int theRet = SyncSelf();
        if (mStreamPtr) {
            mStreamPtr->flush();
        }
        return theRet;
    }
    int SyncSelf()
    {
        int theRet = 0;
        if (mCurPtr <= mBufferPtr) {
            return theRet;
        }
        const size_t theSize = mCurPtr - mBufferPtr;
        if (! fail() && ! EVP_DigestUpdate(
                &mCtx, mBufferPtr, theSize)) {
            setstate(failbit);
            theRet = -1;
        }
        mCurPtr = mBufferPtr;
        if (mStreamPtr) {
            if (! mStreamPtr->write(mBufferPtr, theSize)) {
                setstate(failbit);
            }
        }
        return theRet;
    }
    void InitMd()
    {
        const EVP_MD* const theMdPtr = mDigestName.empty() ?
            EVP_md5() :
            EVP_get_digestbyname(mDigestName.c_str());
        if (! theMdPtr || ! EVP_DigestInit_ex(&mCtx, theMdPtr, 0)) {
            setstate(failbit);
        }
    }

private:
    const string mDigestName;
    char*        mBufferPtr;
    char*        mCurPtr;
    char*        mEndPtr;
    bool         mSyncFlag;
    bool         mWriteTroughFlag;
    OStreamT*    mStreamPtr;
    EVP_MD_CTX   mCtx;
    size_t       mNextSize;

    MdStreamT(const MdStreamT& inStream);
    MdStreamT& operator=( const MdStreamT& inStream);
};

typedef MdStreamT<ostream> MdStream;

} // namespace KFS
#endif /* MD_STREAM_H */
