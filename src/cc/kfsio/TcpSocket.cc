//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/10
// Author: Sriram Rao
//
// Copyright 2008-2011 Quantcast Corp.
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
// Tcp socket class implementation.
//
//----------------------------------------------------------------------------

#include "TcpSocket.h"
#include "common/kfsdecls.h"
#include "common/MsgLogger.h"
#include "qcdio/QCUtils.h"
#include "qcdio/QCMutex.h"
#include "qcdio/qcstutils.h"

#include "Globals.h"

#include <cerrno>
#include <poll.h>
#include <netdb.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>

#include <algorithm>

namespace KFS {

using std::min;
using std::max;
using std::string;
using KFS::libkfsio::globals;

static inline void
UpdateSocketCount(int inc)
{
    globals().ctrOpenNetFds.Update(inc);
}

int TcpSocket::sRecvBufSize    = 64 << 10;
int TcpSocket::sSendBufSize    = 64 << 10;
int TcpSocket::sMaxOpenSockets =  1 << (sizeof(int) * 8 - 2);

TcpSocket::~TcpSocket()
{
    Close();
}

int
TcpSocket::Listen(int port, bool nonBlockingAccept /* = false */)
{
    const int err = Bind(port);
    if (err != 0) {
        return err;
    }
    return StartListening(nonBlockingAccept);
}

int
TcpSocket::StartListening(bool nonBlockingAccept /* = false */)
{
    if (mSockFd < 0) {
        return Perror("Listen", EBADF);
    }
    if (listen(mSockFd, 8192) ||
            (nonBlockingAccept && fcntl(mSockFd, F_SETFL, O_NONBLOCK))) {
        const int err = Perror("listen");
        Close();
        return err;
    }
    return 0;
}

int
TcpSocket::Bind(int port)
{
    Close();
    if (sMaxOpenSockets <= globals().ctrOpenNetFds.GetValue()) {
        return -ENFILE;
    }
    mSockFd = socket(PF_INET, SOCK_STREAM, 0);
    if (mSockFd == -1) {
        return Perror("socket");
    }
    if (fcntl(mSockFd, FD_CLOEXEC, 1)) {
        Perror("set FD_CLOEXEC");
    }

    Address ourAddr;
    memset(&ourAddr, 0, sizeof(ourAddr));
    ourAddr.sin_family = AF_INET;
    ourAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    ourAddr.sin_port = htons(port);

    int reuseAddr = 1;
    if (setsockopt(mSockFd, SOL_SOCKET, SO_REUSEADDR,
                   (char *) &reuseAddr, sizeof(reuseAddr))) {
        Perror("setsockopt");
    }

    if (bind(mSockFd, (struct sockaddr *) &ourAddr, sizeof(ourAddr))) {
        const int ret = Perror(ourAddr);
        close(mSockFd);
        mSockFd = -1;
        return ret;
    }
    UpdateSocketCount(1);
    return 0;
}

TcpSocket*
TcpSocket::Accept(int* status /* = 0 */)
{
    int fd;
    Address   cliAddr;
    TcpSocket *accSock;
    socklen_t cliAddrLen = sizeof(cliAddr);

    if ((fd = accept(mSockFd, (struct sockaddr *) &cliAddr, &cliAddrLen)) < 0) {
        const int err = errno;
        if (err != EAGAIN && err != EWOULDBLOCK) {
            Perror("accept", err);
        }
        if (status) {
            *status = err;
        }
        return 0;
    }
    if (sMaxOpenSockets <= globals().ctrOpenNetFds.GetValue()) {
        close(fd);
        if (status) {
            *status = -ENFILE;
        }
        return 0;
    }
    if (fcntl(fd, FD_CLOEXEC, 1)) {
        Perror("set FD_CLOEXEC");
    }
    accSock = new TcpSocket(fd);
    accSock->SetupSocket();
    UpdateSocketCount(1);
    if (status) {
        *status = 0;
    }
    return accSock;
}

int
TcpSocket::Connect(const TcpSocket::Address *remoteAddr, bool nonblockingConnect)
{
    Close();
    if (sMaxOpenSockets <= globals().ctrOpenNetFds.GetValue()) {
        return -ENFILE;
    }
    mSockFd = socket(PF_INET, SOCK_STREAM, 0);
    if (mSockFd < 0) {
        return (errno > 0 ? -errno : mSockFd);
    }
    if (fcntl(mSockFd, FD_CLOEXEC, 1)) {
        Perror("set FD_CLOEXEC");
    }

    if (nonblockingConnect) {
        // when we do a non-blocking connect, we mark the socket
        // non-blocking; then call connect and it wil return
        // EINPROGRESS; the fd is added to the select loop to check
        // for completion
        fcntl(mSockFd, F_SETFL, O_NONBLOCK);
    }

    int res = connect(mSockFd, (struct sockaddr *) remoteAddr, sizeof(*remoteAddr));
    if (res < 0 && errno != EINPROGRESS) {
        res = Perror(*remoteAddr);
        close(mSockFd);
        mSockFd = -1;
        return res;
    }
    if (res && nonblockingConnect) {
        res = -errno;
    }
    SetupSocket();

    UpdateSocketCount(1);

    return res;
}

static QCMutex sLookupMutex;

int
TcpSocket::Connect(const ServerLocation& location, bool nonblockingConnect)
{
    Address remoteAddr = { 0 };

    const char* const name = location.hostname.c_str();
    if (! inet_aton(name, &remoteAddr.sin_addr)) {
        QCStMutexLocker lock(sLookupMutex);
        // do the conversion if we weren't handed an IP address
        struct hostent * const hostInfo = gethostbyname(name);
        KFS_LOG_STREAM_DEBUG <<
            "connect: "  << location <<
            " hostent: " << (const void*)hostInfo <<
            " type: "    << (hostInfo ? hostInfo->h_addrtype : -1) <<
            " size: "    << (hostInfo ? hostInfo->h_length   : -1) <<
            " "  << h_errno <<
        KFS_LOG_EOM;
        if (! hostInfo || hostInfo->h_addrtype != AF_INET ||
                hostInfo->h_length < (int)sizeof(remoteAddr.sin_addr)) {
            const char* const err = hstrerror(h_errno);
            KFS_LOG_STREAM_ERROR <<
                location.hostname <<
                ": " << ((err && *err) ? err : "unspecified error") <<
            KFS_LOG_EOM;
            return -1;
        }
        memcpy(&remoteAddr.sin_addr, hostInfo->h_addr,
            sizeof(remoteAddr.sin_addr));
    }
    remoteAddr.sin_port = htons(location.port);
    remoteAddr.sin_family = AF_INET;
    return Connect(&remoteAddr, nonblockingConnect);
}

void
TcpSocket::SetupSocket()
{
    int bufSize = sRecvBufSize;
    if (bufSize > 0 &&
            setsockopt(mSockFd, SOL_SOCKET, SO_SNDBUF,
                (char *) &bufSize, sizeof(bufSize))) {
        Perror("setsockopt SO_SNDBUF");
    }
    bufSize = sSendBufSize;
    if (bufSize > 0 &&
            setsockopt(mSockFd, SOL_SOCKET, SO_RCVBUF,
                (char *) &bufSize, sizeof(bufSize)) < 0) {
        Perror("setsockopt SO_RCVBUF");
    }
    int flag = 1;
    // enable keep alive so we can socket errors due to detect network partitions
    if (setsockopt(mSockFd, SOL_SOCKET, SO_KEEPALIVE,
            (char *) &flag, sizeof(flag)) < 0) {
        Perror("setsockopt SO_KEEPALIVE");
    }
    if (fcntl(mSockFd, F_SETFL, O_NONBLOCK)) {
        Perror("set O_NONBLOCK");
    }
    // turn off NAGLE
    if (setsockopt(mSockFd, IPPROTO_TCP, TCP_NODELAY,
            (char *) &flag, sizeof(flag)) < 0) {
        Perror("setsockopt TCP_NODELAY");
    }

}

int
TcpSocket::GetPeerName(struct sockaddr *peerAddr, int len) const
{
    socklen_t peerLen = (socklen_t)len;
    if (getpeername(mSockFd, peerAddr, &peerLen) < 0) {
        return Perror("getpeername");
    }
    return 0;
}

string
TcpSocket::GetPeerName() const
{
    Address saddr = { 0 };
    if (GetPeerName((struct sockaddr*) &saddr, (int)sizeof(saddr)) < 0) {
        return "unknown";
    }
    return ToString(saddr);
}

string
TcpSocket::GetSockName() const
{
    Address   saddr = { 0 };
    socklen_t len   = (socklen_t)sizeof(saddr);
    if (getsockname(mSockFd, (struct sockaddr*) &saddr, &len) < 0) {
        return "unknown";
    }
    return ToString(saddr);
}

int
TcpSocket::Send(const char *buf, int bufLen)
{
    int nwrote;

    nwrote = bufLen > 0 ? send(mSockFd, buf, bufLen, 0) : 0;
    if (nwrote > 0) {
        globals().ctrNetBytesWritten.Update(nwrote);
    }
    return nwrote;
}

int TcpSocket::Recv(char *buf, int bufLen)
{
    int nread;

    nread = bufLen > 0 ? recv(mSockFd, buf, bufLen, 0) : 0;
    if (nread > 0) {
        globals().ctrNetBytesRead.Update(nread);
    }

    return nread;
}

int
TcpSocket::Peek(char *buf, int bufLen)
{
    return (bufLen > 0 ? recv(mSockFd, buf, bufLen, MSG_PEEK) : 0);
}

void
TcpSocket::Close()
{
    if (mSockFd < 0) {
        return;
    }
    close(mSockFd);
    mSockFd = -1;
    UpdateSocketCount(-1);
}

int
TcpSocket::DoSynchSend(const char *buf, int bufLen)
{
    int numSent = 0;
    int res = 0, nfds;
    struct pollfd pfd;
    // 1 second in ms units
    const int kTimeout = 1000;

    while (numSent < bufLen) {
        if (mSockFd < 0)
            break;
        if (res < 0) {
            pfd.fd = mSockFd;
            pfd.events = POLLOUT;
            pfd.revents = 0;
            nfds = poll(&pfd, 1, kTimeout);
            if (nfds == 0)
                continue;
        }

        res = Send(buf + numSent, bufLen - numSent);
        if (res == 0)
            return 0;
        if ((res < 0) &&
            ((errno == EAGAIN) || (errno == EWOULDBLOCK) || (errno == EINTR)))
            continue;
        if (res < 0)
            break;
        numSent += res;
        res = -1;
    }
    if (numSent > 0) {
        globals().ctrNetBytesWritten.Update(numSent);
    }
    return numSent;
}

//
// Receive data within a certain amount of time.  If the server is too slow in responding, bail
//
int
TcpSocket::DoSynchRecv(char *buf, int bufLen, struct timeval &timeout)
{
    int numRecd = 0;
    int res = 0, nfds;
    struct pollfd pfd;
    struct timeval startTime, now;

    gettimeofday(&startTime, 0);

    while (numRecd < bufLen) {
        if (mSockFd < 0)
            break;

        if (res < 0) {
            pfd.fd = mSockFd;
            pfd.events = POLLIN;
            pfd.revents = 0;
            nfds = poll(&pfd, 1, timeout.tv_sec * 1000);
            // get a 0 when timeout expires
            if (nfds == 0) {
                KFS_LOG_STREAM_DEBUG << "Timeout in synch recv" << KFS_LOG_EOM;
                return numRecd > 0 ? numRecd : -ETIMEDOUT;
            }
        }

        gettimeofday(&now, 0);
        if (now.tv_sec - startTime.tv_sec >= timeout.tv_sec) {
            return numRecd > 0 ? numRecd : -ETIMEDOUT;
        }

        res = Recv(buf + numRecd, bufLen - numRecd);
        if (res == 0)
            return 0;
        if ((res < 0) &&
            ((errno == EAGAIN) || (errno == EWOULDBLOCK) || (errno == EINTR)))
            continue;
        if (res < 0)
            break;
        numRecd += res;
    }
    if (numRecd > 0) {
        globals().ctrNetBytesRead.Update(numRecd);
    }

    return numRecd;
}


//
// Receive data within a certain amount of time and discard them.  If
// the server is too slow in responding, bail
//
int
TcpSocket::DoSynchDiscard(int nbytes, struct timeval &timeout)
{
    int numRecd = 0, ntodo, res;
    const int bufSize = 4096;
    char buf[bufSize];

    while (numRecd < nbytes) {
        ntodo = min(nbytes - numRecd, bufSize);
        res = DoSynchRecv(buf, ntodo, timeout);
        if (res == -ETIMEDOUT)
            return numRecd;
        if (res == 0)
            break;
        assert(numRecd >= 0);
        if (numRecd < 0)
            break;
        numRecd += res;
    }
    return numRecd;
}

//
// Peek data within a certain amount of time.  If the server is too slow in responding, bail
//
int
TcpSocket::DoSynchPeek(char *buf, int bufLen, struct timeval &timeout)
{
    int numRecd = 0;
    int res, nfds;
    struct pollfd pfd;
    struct timeval startTime, now;

    gettimeofday(&startTime, 0);

    for (; ;) {
        pfd.fd = mSockFd;
        pfd.events = POLLIN;
        pfd.revents = 0;
        nfds = poll(&pfd, 1, timeout.tv_sec * 1000);
        // get a 0 when timeout expires
        if (nfds == 0) {
            return -ETIMEDOUT;
        }

        gettimeofday(&now, 0);
        if (now.tv_sec - startTime.tv_sec >= timeout.tv_sec) {
            return -ETIMEDOUT;
        }

        res = Peek(buf + numRecd, bufLen - numRecd);
        if (res == 0)
            return 0;
        if ((res < 0) && (errno == EAGAIN))
            continue;
        if (res < 0)
            break;
        numRecd += res;
        if (numRecd > 0)
            break;
    }
    return numRecd;
}

int
TcpSocket::GetSocketError() const
{
    if (mSockFd < 0) {
        return EBADF;
    }
    int       err = 0;
    socklen_t len = sizeof(err);
    if (getsockopt(mSockFd, SOL_SOCKET, SO_ERROR, &err, &len)) {
        return (errno != 0 ? errno : EINVAL);
    }
    assert(len == sizeof(err));
    return err;
}

string
TcpSocket::ToString(const Address& saddr)
{
    char ipname[INET_ADDRSTRLEN + 16];
    if (! inet_ntop(AF_INET, &(saddr.sin_addr), ipname, INET_ADDRSTRLEN)) {
        return "unknown";
    }
    ipname[INET_ADDRSTRLEN] = 0;
    sprintf(ipname + strlen(ipname), ":%d", (int)htons(saddr.sin_port));
    return ipname;
}

int
TcpSocket::Perror(const char* msg, int err) const
{
    KFS_LOG_STREAM_ERROR << QCUtils::SysError(err, msg) << KFS_LOG_EOM;
    return (err > 0 ? -err : (err == 0 ? -1 : err));
}

int
TcpSocket::Perror(const char* msg) const
{
    return Perror(msg, errno);
}

int
TcpSocket::Perror(const Address& saddr) const
{
    const int    err  = errno;
    const string name = ToString(saddr);
    return Perror(name.c_str(), err);
}

}
