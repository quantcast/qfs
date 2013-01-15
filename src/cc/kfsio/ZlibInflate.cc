//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/12/22
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
// \brief Zlib inflate.
//
//----------------------------------------------------------------------------

#include "ZlibInflate.h"
#include "qcdio/QCUtils.h"

#include <zlib.h>
#include <string.h>


namespace KFS
{

class ZlibInflate::Impl
{
public:
    Impl()
        : mAllocatedFlag(false)
    {
    }
    ~Impl()
    {
        if (mAllocatedFlag) {
            inflateEnd(&mStream);
        }
    }
    int Run(
        const char* inBufferPtr,
        size_t      inBufferSize,
        Output&     inOutput,
        bool&       outDoneFlag)
    {
        outDoneFlag = false;
        if (inBufferSize <= 0) {
            return 0;
        }
        int theStatus = Allocate();
        if (theStatus != Z_OK) {
            return theStatus;
        }
        mStream.avail_in = (uInt)inBufferSize;
        mStream.next_in  = (Bytef*)inBufferPtr;
        do {
            char*  theBufferPtr  = 0;
            size_t theBufferSize = 0;
            if ((theStatus = inOutput.GetBuffer(
                    theBufferPtr, theBufferSize)) != 0) {
                theStatus = Z_ERRNO;
                break;
            }
            mStream.avail_out = (uInt)theBufferSize;
            mStream.next_out  = (Bytef*)theBufferPtr;
            theStatus = inflate(&mStream, Z_NO_FLUSH);
            if (theStatus == Z_NEED_DICT) {
                theStatus = Z_DATA_ERROR;
                break;
            }
            if (theStatus == Z_MEM_ERROR ||
                    theStatus == Z_DATA_ERROR ||
                    theStatus == Z_STREAM_ERROR) {
                break;
            }
            QCRTASSERT(mStream.avail_out <= theBufferSize);
            outDoneFlag = theStatus == Z_STREAM_END;
            if ((theStatus = inOutput.Write(
                    theBufferPtr, theBufferSize - mStream.avail_out)) != 0) {
                theStatus = Z_ERRNO;
                break;
            }
            if (outDoneFlag && mStream.avail_in > 0 &&
                    (theStatus = inflateReset(&mStream)) != Z_OK) {
                break;
            }
            theStatus = Z_OK;
        } while (mStream.avail_out == 0 || mStream.avail_in > 0);
        if (mAllocatedFlag && (theStatus != Z_OK || ! outDoneFlag)) {
            inflateEnd(&mStream);
            mAllocatedFlag = false;
        }
        return theStatus;
    }
    const char* StrError(
        int inStatus)
    {
        switch (inStatus) {
            case Z_ERRNO:
                return "zlib io error";
             case Z_BUF_ERROR:
                return "zlib invalid buffer";
           case Z_STREAM_ERROR:
                return "zlib invalid compression level";
            case Z_DATA_ERROR:
                return "zlib invalid or incomplete deflate data";
            case Z_MEM_ERROR:
                return "zlib out of memory";
            case Z_VERSION_ERROR:
                return "zlib version mismatch";
            case Z_OK:
                return "zlib no error";
            default:
                break;
        }
        return "zlib unspecified error";
    }
    void Reset()
    {
        if (mAllocatedFlag) {
            inflateEnd(&mStream);
            mAllocatedFlag = false;
        }
    }
private:
    bool       mAllocatedFlag;
    z_stream_s mStream;

    int Allocate()
    {
        if (mAllocatedFlag) {
            return 0;
        }
        memset(&mStream, 0, sizeof(mStream));
        mStream.zalloc = Z_NULL;
        mStream.zfree  = Z_NULL;
        mStream.opaque = Z_NULL;
        const int kEnableGzipAndZlibHeaders = 32;
        const int theStatus = inflateInit2(&mStream, 
            MAX_WBITS + kEnableGzipAndZlibHeaders);
        mAllocatedFlag = theStatus == Z_OK;
        return theStatus;
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};


ZlibInflate::ZlibInflate()
    : mImpl(*(new Impl()))
{}

ZlibInflate::~ZlibInflate()
{
    delete &mImpl;
}

    int
ZlibInflate::Run(
    const char* inBufferPtr,
    size_t      inBufferSize,
    Output&     inOutput,
    bool&       outDoneFlag)
{
    return mImpl.Run(inBufferPtr, inBufferSize, inOutput, outDoneFlag);
}

    void
ZlibInflate::Reset()
{
    return mImpl.Reset();
}

    const char*
ZlibInflate::StrError(
    int inStatus)
{
    return mImpl.StrError(inStatus);
}

}
