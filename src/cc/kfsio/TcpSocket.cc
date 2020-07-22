//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/10
// Author: Sriram Rao
//
// Copyright 2008-2011,2016 Quantcast Corporation. All rights reserved.
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
#include "common/IntToString.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"

#include "Globals.h"

#include <cerrno>
#include <string.h>
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
    const int64_t socketCount = globals().ctrOpenNetFds.Update(inc);
    QCRTASSERT(0 <= socketCount);
}

static inline void
IncrementDnsCounter(int64_t timeSpent)
{
    Counter& counter = globals().ctrNetDnsResolvedCtr;
    counter.Update(1);
    counter.UpdateTime(timeSpent);
}

static inline void
IncrementDnsErrorCounter(int64_t timeSpent)
{
    Counter& counter = globals().ctrNetDnsErrors;
    counter.Update(1);
    counter.UpdateTime(timeSpent);
}

template<typename T>
static inline bool
SetSockOpt(int fd, int level, int name, const T& val)
{
    return (setsockopt(fd, level, name, &val, sizeof(val)) != 0);
}

int TcpSocket::sRecvBufSize    = 64 << 10;
int TcpSocket::sSendBufSize    = 64 << 10;
int TcpSocket::sMaxOpenSockets =  1 << (sizeof(int) * 8 - 2);

struct TcpSocket::Address
{
    Address(TcpSocket::Type type)
        : mProto(type == TcpSocket::kTypeIpV6 ? AF_INET6 : AF_INET)
        { memset(&mIp, 0, sizeof(mIp)); }
    struct sockaddr* Ptr()
    {
        return (mProto == AF_INET ?
            reinterpret_cast<struct sockaddr*>(&mIp.v4) :
            reinterpret_cast<struct sockaddr*>(&mIp.v6)
        );
    }
    const struct sockaddr* Ptr() const
    {
        return (mProto == AF_INET ?
            reinterpret_cast<const struct sockaddr*>(&mIp.v4) :
            reinterpret_cast<const struct sockaddr*>(&mIp.v6)
        );
    }
    socklen_t Size() const
    {
        return (socklen_t)(mProto == AF_INET ? sizeof(mIp.v4) : sizeof(mIp.v6));
    }
    void SetAddrAny(int port)
    {
        if (mProto == AF_INET) {
            mIp.v4.sin_family      = AF_INET;
            mIp.v4.sin_addr.s_addr = htonl(INADDR_ANY);
            mIp.v4.sin_port        = htons(port);
        } else {
            mIp.v6.sin6_family = AF_INET6;
            mIp.v6.sin6_addr   = in6addr_any;
            mIp.v6.sin6_port   = htons(port);
        }
    }
    int GetLocation(ServerLocation& location)
    {
        char ipname[(INET_ADDRSTRLEN < INET6_ADDRSTRLEN ?
            INET6_ADDRSTRLEN : INET_ADDRSTRLEN) + 1];
        const socklen_t size = mProto == AF_INET ?
            INET_ADDRSTRLEN : INET6_ADDRSTRLEN;
        if (! inet_ntop(mProto, GetAddr(), ipname, size)) {
            return (errno < 0 ? errno : (errno == 0 ? -EINVAL : -errno));
        }
        ipname[size] = 0;
        location.hostname = ipname;
        location.port     = ntohs(
            mProto == AF_INET ? mIp.v4.sin_port : mIp.v6.sin6_port);
        return 0;
    }
    string ToString() const
    {
        char ipname[(INET_ADDRSTRLEN < INET6_ADDRSTRLEN ?
            INET6_ADDRSTRLEN : INET_ADDRSTRLEN) + 1];
        const socklen_t size = mProto == AF_INET ?
            INET6_ADDRSTRLEN : INET6_ADDRSTRLEN;
        if (! inet_ntop(mProto, GetAddr(), ipname, size)) {
            return string("unknown");
        }
        ipname[size] = 0;
        ConvertInt<int, 10> portBuf((int)ntohs(
            mProto == AF_INET ? mIp.v4.sin_port : mIp.v6.sin6_port));
        string ret;
        ret.reserve(strlen(ipname) + portBuf.GetSize() +
            mProto == AF_INET ? 1 : 3);
        if (mProto != AF_INET) {
            ret.append("[");
        }
        ret.append(ipname);
        ret.append(mProto == AF_INET ? ":" : "]:");
        ret.append(portBuf.GetPtr());
        return ret;
    }
    static bool IsValidConnectToIpAddress(const char* ipAddrStrPtr,
        bool hostNameOkFlag = false)
    {
        if (strchr(ipAddrStrPtr, ':')) {
            struct sockaddr_in6 addr = {0};
            if (inet_pton(AF_INET6, ipAddrStrPtr,
                    &addr.sin6_addr)) {
                return (0 != memcmp(
                    &addr.sin6_addr, &in6addr_any, sizeof(in6addr_any)));
            }
        } else {
            struct sockaddr_in addr = {0};
            if (inet_aton(ipAddrStrPtr, &addr.sin_addr)) {
                return (addr.sin_addr.s_addr != htonl(INADDR_ANY) &&
                    addr.sin_addr.s_addr != htonl(INADDR_NONE));
            }
        }
        return hostNameOkFlag;
    }
    static bool IsValidConnectToIpAddressOrHostName(const char* hostNameOrIpPtr)
        { return IsValidConnectToIpAddress(hostNameOrIpPtr, true); }
    static bool IsValidConnectToAddress(const ServerLocation& location)
    {
        return (0 < location.port && location.port < (1 << 16) &&
            IsValidConnectToIpAddressOrHostName(location.hostname.c_str()));
    }
    int Set(const ServerLocation& location)
    {
        bool useResolverFlag = true;
        memset(&mIp, 0, sizeof(mIp));
        if (location.hostname.find(':') != string::npos) {
            struct sockaddr_in6& addr = mIp.v6;
            if (inet_pton(AF_INET6, location.hostname.c_str(),
                    &addr.sin6_addr)) {
                mProto           = AF_INET6;
                addr.sin6_family = mProto;
                useResolverFlag  = false;
            }
        } else {
            struct sockaddr_in& addr = mIp.v4;
            if (inet_aton(location.hostname.c_str(), &addr.sin_addr)) {
                mProto          = AF_INET;
                addr.sin_family = mProto;
                useResolverFlag = false;
            }
        }
        if (useResolverFlag) {
            const int64_t startTime = microseconds();
            const int proto = mProto;
            memset(&mIp, 0, sizeof(mIp));
            struct addrinfo hints;
            memset(&hints, 0, sizeof(hints));
            hints.ai_family   = AF_UNSPEC;   // Allow IPv4 or IPv6
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_flags    = 0;
            hints.ai_protocol = 0;           // Any protocol
            struct addrinfo* res = 0;
            const int status = getaddrinfo(
                location.hostname.c_str(), 0, &hints, &res);
            if (0 != status) {
                const char* err = gai_strerror(status);
                KFS_LOG_STREAM_ERROR <<
                    location.hostname <<
                    ": " << ((err && *err) ? err :
                        "unspecified host name resolution error") <<
                KFS_LOG_EOM;
                if (res) {
                    freeaddrinfo(res);
                }
                IncrementDnsErrorCounter(microseconds() - startTime);
                return -EHOSTUNREACH;
            }
            int                    cnt = 0;
            struct addrinfo const* ptr;
            for (ptr = res; ptr; ptr = ptr->ai_next) {
                if (AF_INET != ptr->ai_family && AF_INET6 != ptr->ai_family) {
                    continue;
                }
                if (0 < cnt && proto != ptr->ai_family) {
                    continue;
                }
                mProto = ptr->ai_family;
                if (AF_INET == mProto) {
                    mIp.v4.sin_family  = mProto;
                } else {
                    mIp.v6.sin6_family = mProto;
                }
                memcpy(Ptr(), ptr->ai_addr, ptr->ai_addrlen);
                cnt++;
                if (proto == mProto) {
                    break;
                }
            }
            freeaddrinfo(res);
            if (! ptr) {
                KFS_LOG_STREAM_ERROR <<
                    location.hostname <<
                    ": " << "host name resolution failure" <<
                KFS_LOG_EOM;
                IncrementDnsErrorCounter(microseconds() - startTime);
                return -EHOSTUNREACH;
            }
            IncrementDnsCounter(microseconds() - startTime);
        }
        if (AF_INET == mProto) {
            mIp.v4.sin_port  = htons(location.port);
        } else {
            mIp.v6.sin6_port = htons(location.port);
        }
        if (useResolverFlag) {
            KFS_LOG_STREAM_DEBUG <<
                "set: " << location   <<
                " => "  << ToString() <<
            KFS_LOG_EOM;
        }
        return 0;
    }
    TcpSocket::Type GetType() const
    {
        return (mProto == AF_INET6 ?
            TcpSocket::kTypeIpV6 : TcpSocket::kTypeIpV4);
    }
    int GetProtocol() const
        { return mProto; }
private:
    int mProto;
    union
    {
        struct sockaddr_in  v4;
        struct sockaddr_in6 v6;
    } mIp;

    void* GetAddr()
    {
        if (AF_INET == mProto) {
            return &mIp.v4.sin_addr;
        }
        return &mIp.v6.sin6_addr;
    }
    const void* GetAddr() const
        { return const_cast<Address*>(this)->GetAddr(); }
    int GetAddSize(int type)
    {
        return (int)(type == AF_INET ?
            sizeof(mIp.v4.sin_addr) : sizeof(mIp.v6.sin6_addr));
    }
};

void
TcpSocket::UpdateCount(int increment)
{
    UpdateSocketCount(increment);
}

bool
TcpSocket::IsValidConnectToAddress(const ServerLocation& location)
{
    return Address::IsValidConnectToAddress(location);
}

bool
TcpSocket::IsValidConnectToIpAddress(const char* ipAddrPtr)
{
    return (ipAddrPtr && Address::IsValidConnectToIpAddress(ipAddrPtr));
}

TcpSocket::~TcpSocket()
{
    Close();
}

int
TcpSocket::StartListening(bool nonBlockingAccept, int maxQueue)
{
    if (mSockFd < 0) {
        return Perror("Listen", EBADF);
    }
    SetupSocket();
    if ((nonBlockingAccept && fcntl(mSockFd, F_SETFL, O_NONBLOCK)) ||
            listen(mSockFd, maxQueue)) {
        return PerrorFatal("listen");
    }
    return 0;
}

int
TcpSocket::Bind(
    const ServerLocation& location, TcpSocket::Type type, bool ipV6OnlyFlag)
{
    Close();
    if (sMaxOpenSockets <= globals().ctrOpenNetFds.GetValue()) {
        return -ENFILE;
    }
    if (location.port < 0) {
        return -EINVAL;
    }
    Address addr(type);
    if (location.hostname.empty()) {
        addr.SetAddrAny(location.port);
    } else {
        const int ret = addr.Set(location);
        if (ret < 0) {
            return ret;
        }
    }
    mSockFd = socket(addr.GetProtocol(), SOCK_STREAM, 0);
    if (mSockFd < 0) {
        return PerrorFatal("socket");
    }
    mType = addr.GetType();
    if (IsCountableSocketFd(mSockFd)){
        UpdateSocketCount(1);
    }
    if (fcntl(mSockFd, F_SETFD, FD_CLOEXEC)) {
        Perror("set FD_CLOEXEC");
    }
    int flag = ipV6OnlyFlag ? 1 : 0;
    if (addr.GetProtocol() == AF_INET6 &&
            SetSockOpt(mSockFd, IPPROTO_IPV6, IPV6_V6ONLY, flag)) {
        Perror("setsockopt IPV6_V6ONLY");
    }
    flag = 1;
    if (SetSockOpt(mSockFd, SOL_SOCKET, SO_REUSEADDR, flag)) {
        Perror("setsockopt SO_REUSEADDR");
    }
    if (bind(mSockFd, addr.Ptr(), addr.Size())) {
        return PerrorFatal(addr);
    }
    return 0;
}

TcpSocket*
TcpSocket::Accept(int* status /* = 0 */)
{
    int        fd;
    Address    cliAddr(mType);
    TcpSocket* accSock;
    socklen_t  cliAddrLen = cliAddr.Size();

    if ((fd = accept(mSockFd, cliAddr.Ptr(), &cliAddrLen)) < 0) {
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
    if (fcntl(fd, F_SETFD, FD_CLOEXEC)) {
        Perror("set FD_CLOEXEC");
    }
    if (fcntl(fd, F_SETFL, O_NONBLOCK)) {
        Perror("set O_NONBLOCK");
    }
    accSock = new TcpSocket(fd, mType);
    accSock->SetupSocket();
    if (status) {
        *status = 0;
    }
    return accSock;
}

int
TcpSocket::Connect(
    const TcpSocket::Address& remoteAddr, bool nonblockingConnect)
{
    Close();
    if (sMaxOpenSockets <= globals().ctrOpenNetFds.GetValue()) {
        return -ENFILE;
    }
    mType = remoteAddr.GetType();
    mSockFd = socket(remoteAddr.GetProtocol(), SOCK_STREAM, 0);
    if (mSockFd < 0) {
        return (errno > 0 ? -errno : mSockFd);
    }
    if (IsCountableSocketFd(mSockFd)){
        UpdateSocketCount(1);
    }
    if (fcntl(mSockFd, F_SETFD, FD_CLOEXEC)) {
        Perror("set FD_CLOEXEC");
    }
    if (nonblockingConnect) {
        // when we do a non-blocking connect, we mark the socket
        // non-blocking; then call connect and it wil return
        // EINPROGRESS; the fd is added to the select loop to check
        // for completion
        if (fcntl(mSockFd, F_SETFL, O_NONBLOCK)) {
            Perror("set O_NONBLOCK");
        }
    }
    SetupSocket();
    int res = connect(mSockFd, remoteAddr.Ptr(), remoteAddr.Size());
    if (res < 0) {
        res = -errno;
#ifdef EALREADY
        if (-EALREADY == res) {
            res = -EINPROGRESS;
        }
#endif
        if (-EINPROGRESS != res) {
            Close();
            KFS_LOG_STREAM_DEBUG <<
                "connect to " << remoteAddr.ToString() <<
                " :" << QCUtils::SysError(-res) <<
            KFS_LOG_EOM;
            return res;
        }
    }
    if (! nonblockingConnect) {
        if (fcntl(mSockFd, F_SETFL, O_NONBLOCK)) {
            Perror("set O_NONBLOCK");
        }
    }
    return res;
}

int
TcpSocket::Connect(const ServerLocation& location, bool nonblockingConnect)
{
    Address remoteAddr(mType);
    const int ret = remoteAddr.Set(location);
    return (ret < 0 ? ret : Connect(remoteAddr, nonblockingConnect));
}

void
TcpSocket::SetupSocket()
{
    int bufSize = sRecvBufSize;
    if (bufSize > 0 && SetSockOpt(mSockFd, SOL_SOCKET, SO_SNDBUF, bufSize)) {
        Perror("setsockopt SO_SNDBUF");
    }
    bufSize = sSendBufSize;
    if (bufSize > 0 && SetSockOpt(mSockFd, SOL_SOCKET, SO_RCVBUF, bufSize)) {
        Perror("setsockopt SO_RCVBUF");
    }
    int flag = 1;
#ifdef _KFS_USE_TCP_KEEP_ALIVE
    // enable keep alive so we can socket errors due to detect network partitions
    if (SetSockOpt(mSockFd, SOL_SOCKET, SO_KEEPALIVE, flag)) {
        Perror("setsockopt SO_KEEPALIVE");
    }
#endif
    // turn off NAGLE
    if (SetSockOpt(mSockFd, IPPROTO_TCP, TCP_NODELAY, flag)) {
        Perror("setsockopt TCP_NODELAY");
    }

}

int
TcpSocket::GetPeerName(TcpSocket::Address& peerAddr) const
{
    socklen_t peerLen = peerAddr.Size();
    if (getpeername(mSockFd, peerAddr.Ptr(), &peerLen)) {
        return Perror("getpeername");
    }
    return 0;
}

string
TcpSocket::GetPeerName() const
{
    Address saddr(mType);
    if (GetPeerName(saddr) < 0) {
        return "unknown";
    }
    return ToString(saddr);
}

string
TcpSocket::GetSockName() const
{
    Address   saddr(mType);
    socklen_t len = saddr.Size();
    if (getsockname(mSockFd, saddr.Ptr(), &len)) {
        return "unknown";
    }
    return ToString(saddr);
}

int
TcpSocket::GetPeerLocation(ServerLocation& location) const
{
    if (mSockFd < 0) {
        return -EBADF;
    }
    Address addr(mType);
    const int ret = GetPeerName(addr);
    return (ret < 0 ? ret : addr.GetLocation(location));
}

int
TcpSocket::GetSockLocation(ServerLocation& location) const
{
    if (mSockFd < 0) {
        return -EBADF;
    }
    Address   addr(mType);
    socklen_t len = addr.Size();
    if (getsockname(mSockFd, addr.Ptr(), &len)) {
        return Perror("getsockname");
    }
    return addr.GetLocation(location);
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
    const int cnt = IsCountableSocketFd(mSockFd) ? -1 : 0;
    close(mSockFd);
    mSockFd = -1;
    mType   = kTypeNone;
    UpdateSocketCount(cnt);
}

int
TcpSocket::Shutdown(bool readFlag, bool writeFlag)
{
    if (mSockFd < 0) {
        return -EINVAL;
    }
    const int how = (readFlag ? SHUT_RD : 0) | (writeFlag ? SHUT_WR : 0);
    if (how == 0) {
        return 0;
    }
    if (shutdown(mSockFd, how)) {
        const int err = errno;
        return (err < 0 ? err : (err != 0 ? -err : -1));
    }
    return 0;
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
    return saddr.ToString();
}

int
TcpSocket::PerrorFatal(const char* msg)
{
    return PerrorFatal(msg, errno);
}

int
TcpSocket::PerrorFatal(const char* msg, int err)
{
    const int ret = Perror(msg, err);
    Close();
    return ret;
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
TcpSocket::PerrorFatal(const Address& saddr)
{
    const int ret = Perror(saddr);
    Close();
    return ret;
}

int
TcpSocket::Perror(const Address& saddr) const
{
    const int    err  = errno;
    const string name = ToString(saddr);
    return Perror(name.c_str(), err);
}

/* static */ int
TcpSocket::Validate(const string& address)
{
    if (address.empty()) {
        return -EINVAL;
    }
    Address addr(kTypeIpV4);
    return addr.Set(ServerLocation(address, 0));
}

}
