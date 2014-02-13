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
#include "common/IntToString.h"

namespace KFS
{

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
        const int   theBufferSize = inBufferSize - 1;
        char*       theCurPtr     = inBufferPtr +  max(0,
            mOp.reqHeaders.CopyOut(inBufferPtr, theBufferSize));
        char* const theEndPtr     = inBufferPtr + theBufferSize;
        int theRet =
            mOp.reqHeaders.BytesConsumable() +
            Append(theCurPtr, theEndPtr, "Client-ip: ", 11) +
            Append(theCurPtr, theEndPtr,
                    mOp.clientIp.data(), (int)mOp.clientIp.size());
        const int kBufSize = 32;
        char      theBuf[kBufSize];
        if (mOp.authUid != kKfsUserNone) {
            theRet += Append(theCurPtr, theEndPtr, "\r\nAuth-uid: ", 12);
            const char* const thePtr = IntToDecString(
                mOp.authUid, theBuf + kBufSize);
            theRet += Append(theCurPtr, theEndPtr,
                thePtr, (int)(theBuf + kBufSize - thePtr));
        }
        theRet += Append(theCurPtr, theEndPtr, "\r\nStatus: ", 10);
        const char* const thePtr = IntToDecString(
            mOp.status < 0 ? -SysToKfsErrno(-mOp.status) : mOp.status,
            theBuf + kBufSize
        );
        theRet += Append(theCurPtr, theEndPtr,
            thePtr, (int)(theBuf + kBufSize - thePtr));
        if (0 <= theBufferSize) {
            *theCurPtr = 0; // Null terminated records.
        }
        theRet++;
        return theRet;
    }
    virtual int GetMsgLength()
    {
        return (mOp.reqHeaders.BytesConsumable() + 256);
    }
private:
    static int Append(
        char*&      ioPtr,
        char*       inEndPtr,
        const char* inPtr,
        int         inLen)
    {
        if (inLen <= 0) {
            return 0;
        }
        if (inEndPtr < ioPtr + inLen) {
            return inLen;
        }
        memcpy(ioPtr, inPtr, inLen);
        ioPtr += inLen;
        return inLen;
    }
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
