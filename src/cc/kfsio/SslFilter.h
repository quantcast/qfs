//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/06/24
// Author:  Mike Ovsiannikov 
//
// Copyright 2013 Quantcast Corp.
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
// \brief Ssl socket layer implementation.
//
//----------------------------------------------------------------------------

#ifndef KFS_IO_SSL_FILTER_H
#define KFS_IO_SSL_FILTER_H

#include "NetConnection.h"

#include <string>
#include <boost/shared_ptr.hpp>

namespace KFS
{
using std::string;
using boost::shared_ptr;

class Properties;
class TcpSocket;
class SslFilter;

class SslFilterServerPsk
{
public:
    virtual unsigned long GetPsk(
        const char*    inIdentityPtr,
	unsigned char* inPskBufferPtr,
        unsigned int   inPskBufferLen,
        string&        outAuthName) = 0;
protected:
    SslFilterServerPsk()
        {}
    virtual ~SslFilterServerPsk()
        {}
};

class SslFilterVerifyPeer
{
public:
    virtual bool Verify(
	string&       ioFilterAuthName,
        bool          inPreverifyOkFlag,
        int           inCurCertDepth,
        const string& inPeerName,
        int64_t       inEndTime,
        bool          inEndTimeValidFlag) = 0;
protected:
    SslFilterVerifyPeer()
        {}
    virtual ~SslFilterVerifyPeer()
        {}
};

class SslFilter : public NetConnection::Filter
{
public:
    class Ctx;
    typedef unsigned long Error;
    typedef SslFilterServerPsk  ServerPsk;
    typedef SslFilterVerifyPeer VerifyPeer;
    class CtxFreeFunctor
    {
    public:
        void operator()(
            Ctx* inCtxPtr)
            { FreeCtx(inCtxPtr); }
    };
    typedef shared_ptr<Ctx> CtxPtr;
    static CtxPtr MakeCtxPtr(
        Ctx* inCtxPtr)
        { return CtxPtr(inCtxPtr, CtxFreeFunctor()); }

    static Error Initialize();
    static Error Cleanup();
    static string GetErrorMsg(
        Error inError);

    static Ctx* CreateCtx(
        const bool        inServerFlag,
        const bool        inPskOnlyFlag,
        const char*       inParamsPrefixPtr,
        const Properties& inParams,
        string*           inErrMsgPtr);
    static long GetSessionTimeout(
        Ctx& inCtx);
    static void FreeCtx(
        Ctx* inCtxPtr);

    static SslFilter& Create(
        Ctx&        inCtx,
        const char* inPskDataPtr          = 0,
        size_t      inPskDataLen          = 0,
        const char* inPskCliIdendityPtr   = 0,
        ServerPsk*  inServerPskPtr        = 0,
        VerifyPeer* inVerifyPeerPtr       = 0,
        const char* inExpectedPeerNamePtr = 0,
        bool        inDeleteOnCloseFlag   = true);
    SslFilter(
        Ctx&        inCtx,
        const char* inPskDataPtr        = 0,
        size_t      inPskDataLen        = 0,
        const char* inPskCliIdendityPtr = 0,
        ServerPsk*  inServerPskPtr      = 0,
        VerifyPeer* inVerifyPeerPtr     = 0,
        bool        inDeleteOnCloseFlag = true);
    Error GetError() const;
    void SetPsk(
        const char* inPskDataPtr,
        size_t      inPskDataLen);
    virtual ~SslFilter();
    virtual bool WantRead(
        const NetConnection& inConnection) const;
    virtual bool WantWrite(
        const NetConnection& inConnection) const;
    virtual int Read(
        NetConnection& inConnection,
        TcpSocket&     inSocket,
        IOBuffer&      inIoBuffer,
        int            inMaxRead);
    virtual int Write(
        NetConnection& inConnection,
        TcpSocket&     inSocket,
        IOBuffer&      inIoBuffer,
        bool&          outInvokeErrHandlerFlag);
    virtual void Close(
        NetConnection& inConnection,
        TcpSocket*     inSocketPtr);
    virtual int Shutdown(
        NetConnection& inConnection,
        TcpSocket&     inSocket);
    virtual int Attach(
        NetConnection& inConnection,
        TcpSocket*     inSocketPtr,
        string*        outErrMsgPtr);
    virtual void Detach(
        NetConnection& inConnection,
        TcpSocket*     inSocketPtr);
    virtual string GetAuthName() const;
    virtual bool IsAuthFailure() const;
    virtual string GetErrorMsg() const;
    virtual int GetErrorCode() const;
    virtual bool IsShutdownReceived() const;
    virtual string GetPeerId() const;
    bool IsHandshakeDone() const;
    static bool GetCtxX509EndTime(
        Ctx&     inCtx,
        int64_t& outEndTime);
    bool GetX509EndTime(
        int64_t& outEndTime) const;
private:
    class Impl;
    Impl& mImpl;
private:
    SslFilter(
        const SslFilter& inFilter);
    SslFilter& operator=(
        const SslFilter& inFilter);
};

}

#endif /* KFS_IO_SSL_FILTER_H */
