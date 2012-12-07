//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/04/10
// Author: Mike Ovsiannikov.
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
//
// \file AuditLog.cc
// \brief Kfs meta server audit log implementation.
//
//----------------------------------------------------------------------------

#include "AuditLog.h"
#include "MetaRequest.h"
#include "kfsio/IOBuffer.h"
#include "common/BufferedLogWriter.h"
#include "common/kfserrno.h"

#include <ostream>

namespace KFS
{

using std::ostream;
using std::streambuf;

class AuditLogWriter : public BufferedLogWriter::Writer
{
public:
    AuditLogWriter(
        const MetaRequest& inOp)
        : mOp(inOp)
        {}
    virtual ~AuditLogWriter()
        {}
    virtual int Write(
        char* inBufferPtr,
        int   inBufferSize)
    {
        if (inBufferSize <= 0) {
            return 0;
        }
        int theNWr = max(0,
            mOp.reqHeaders.CopyOut(inBufferPtr, inBufferSize));
        char* const theEndPtr = inBufferPtr + inBufferSize;
        // This is not re-entrant, and it doesn't have to be re-entrant due to
        // serialization in BufferedLogWriter::Append().
        // Creating and destroying output stream on every invocation is
        // rather cpu expensive most likely due to c++ lib allocations.
        static OutStream sStream;
        sStream.Set(inBufferPtr + theNWr, theEndPtr) <<
             "Client-ip: " << mOp.clientIp << "\r\n"
             "Status: "    <<
                (mOp.status < 0 ? -SysToKfsErrno(-mOp.status) : mOp.status)
        ;
        // Put terminal 0 -- record separator.
        char* const thePtr = min(theEndPtr - 1, sStream.GetCurPtr());
        *thePtr = 0;
        return (int)(thePtr + 1 - inBufferPtr);

    }
    virtual int GetMsgLength()
    {
        return (mOp.reqHeaders.BytesConsumable() + 256);
    }
private:
    class OutStream :
        private streambuf,
        public ostream
    {
    public:
        OutStream()
            : streambuf(),
            ostream(this)
            {}
        ostream& Set(
            char* inBufferPtr,
            char* inBufferEndPtr)
        {
            setp(inBufferPtr, inBufferEndPtr);
            Clear();
            return *this;
        }
        virtual streamsize xsputn(
            const char* inBufPtr,
            streamsize  inLength)
        {
            char* const theEndPtr = epptr();
            char* const theCurPtr = pptr();
            const streamsize theSize(
                min(max(streamsize(0), inLength),
                streamsize(theEndPtr - theCurPtr)));
            memcpy(theCurPtr, inBufPtr, theSize);
            pbump(theSize);
            return theSize;
        }
        char* GetCurPtr() const
            { return pptr(); }
        void Clear()
        {
            ostream::clear();
            ostream::flags(ostream::dec | ostream::skipws);
            ostream::precision(6);
            ostream::width(0);
            ostream::fill(' ');
            ostream::tie(0);
        }
    private:
        OutStream(
            const OutStream&);
        OutStream& operator=(
            const OutStream&);
    };
    const MetaRequest& mOp;
private:
    AuditLogWriter(
        const AuditLogWriter&);
    AuditLogWriter& operator=(
        const AuditLogWriter&);
};

static const BufferedLogWriter* sBufferedLogWriterForGdbToFindPtr = 0;

static BufferedLogWriter&
GetAuditMsgWriter()
{
    static BufferedLogWriter sAuditMsgWriter;
    if (! sBufferedLogWriterForGdbToFindPtr) {
        sBufferedLogWriterForGdbToFindPtr = &sAuditMsgWriter;
    }
    return sAuditMsgWriter;
}

/* static */ void
AuditLog::Log(
    const MetaRequest& inOp)
{
    AuditLogWriter theWriter(inOp);
    GetAuditMsgWriter().Append(
        inOp.status >= 0 ?
            BufferedLogWriter::kLogLevelINFO :
            BufferedLogWriter::kLogLevelERROR,
        theWriter
    );
}

/* static */ void
AuditLog::SetParameters(
    const Properties& inProps)
{
    GetAuditMsgWriter().SetParameters(inProps,
        "metaServer.auditLogWriter.");
}

/* static */ void
AuditLog::Stop()
{
    GetAuditMsgWriter().Stop();
}

/* static */ void
AuditLog::PrepareToFork()
{
    GetAuditMsgWriter().PrepareToFork();
}

/* static */ void
AuditLog::ForkDone()
{
    GetAuditMsgWriter().ForkDone();
}

/* static */ void
AuditLog::ChildAtFork()
{
    GetAuditMsgWriter().ChildAtFork();
}

}
