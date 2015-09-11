//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/09/09
// Author: Mike Ovsiannikov
//
// Copyright 2015 Quantcast Corp.
//
// This file is part of Kosmos File System (KFS).
//
// Licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unss required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// AWS S3 "object store" low level IO libs3 shim.
//
//----------------------------------------------------------------------------

#ifndef KFS_S3IO_H
#define KFS_S3IO_H

#include "qcdio/QCDiskQueue.h"

namespace KFS
{

class Properties;

class S3IO : public QCDiskQueue::RequestProcessor
{
public:
    typedef QCDiskQueue::Request       Request;
    typedef QCDiskQueue::ReqType       ReqType;
    typedef QCDiskQueue::BlockIdx      BlockIdx;
    typedef QCDiskQueue::InputIterator InputIterator;

    S3IO();
    virtual ~S3IO();
    bool Init(
        const char* inUrlPtr);
    void SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters);
    virtual void ProcessAndWait();
    virtual void Wakeup();
    virtual void Stop();
    virtual int Open(
        const char* inFileNamePtr,
        bool        inReadOnlyFlag,
        bool        inCreateFlag,
        bool        inCreateExclusiveFlag,
        int64_t&    ioMaxFileSize);
    virtual int Close(
        int     inFd,
        int64_t inEof);
    virtual void StartIo(
        Request&        inRequest,
        ReqType         inReqType,
        int             inFd,
        BlockIdx        inStartBlockIdx,
        int             inBufferCount,
        InputIterator*  inInputIteratorPtr,
        int64_t         inSpaceAllocSize,
        int64_t         inEof);
    virtual void StartMeta(
        Request&    inRequest,
        ReqType     inReqType,
        const char* inNamePtr,
        const char* inName2Ptr);
private:
    class Impl;
    Impl& mImpl;
};

} // namespace KFS

#endif /* KFS_S3IO_H */
