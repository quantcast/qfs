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
/// \brief Class that hides the internals of doing socket I/O.
//
//----------------------------------------------------------------------------

#ifndef _LIBIO_TCP_SOCKET_H
#define _LIBIO_TCP_SOCKET_H

#include <boost/shared_ptr.hpp>
#include <string>

struct timeval;
struct sockaddr_in;
struct sockaddr;

namespace KFS
{
using std::string;

struct ServerLocation;

class TcpSocket
{
public:
    typedef sockaddr_in Address;
    TcpSocket()
        : mSockFd(-1)
        {}
    /// Wrap the passed in file descriptor in a TcpSocket
    /// @param[in] fd file descriptor corresponding to a TCP socket.
    TcpSocket(int fd)
        : mSockFd(fd)
        {}
    ~TcpSocket();

    /// Setup a TCP socket that listens for connections
    /// @param port Port on which to listen for incoming connections
    int Listen(int port, bool nonBlockingAccept = false);

    /// Accept connection on a socket.
    /// @retval A TcpSocket pointer that contains the accepted
    /// connection.  It is the caller's responsibility to free the
    /// pointer returned by this method.
    ///
    TcpSocket* Accept(int* status = 0);

    /// Connect to the remote address.  If non-blocking connect is
    /// set, the socket is first marked non-blocking and then we do
    /// the connect call.  Then, you use select() to check for connect() completion
    /// @retval 0 on success; -1 on failure; -EINPROGRESS if we do a
    /// nonblockingConnect and connect returned that error code
    int Connect(const Address* remoteAddr, bool nonblockingConnect = false);
    int Connect(const ServerLocation& location, bool nonblockingConnect = false);

    /// Do block-IO's, where # of bytes to be send/recd is the length
    /// of the buffer.
    /// @retval Returns # of bytes sent or -1 if there was an error.
    int DoSynchSend(const char *buf, int bufLen);

    /// For recv/peek, specify a timeout within which data should be received.
    int DoSynchRecv(char *buf, int bufLen, timeval& timeout);
    int DoSynchPeek(char *buf, int bufLen, timeval& timeout);

    /// Discard a bunch of bytes that are coming down the pipe.
    int DoSynchDiscard(int len, timeval& timeout);

    /// Peek to see if any data is available.  This call will not
    /// remove the data from the underlying socket buffers.
    /// @retval Returns # of bytes copied in or -1 if there was an error.
    int Peek(char *buf, int bufLen);

    /// Get the file descriptor associated with this socket.
    inline int GetFd() { return mSockFd; };

    /// Return true if socket is good for read/write. false otherwise.
    bool IsGood() const {
        return (mSockFd >= 0);
    }

    /// pass in the length of the buffer pointed to by peerAddr
    int GetPeerName(sockaddr *peerAddr, int len) const;
    /// Return the peer's IP address as a string
    string GetPeerName() const;
    string GetSockName() const;

    /// Sends at-most the specified # of bytes.
    /// @retval Returns the result of calling send().
    int Send(const char *buf, int bufLen);

    /// Receives at-most the specified # of bytes.
    /// @retval Returns the result of calling recv().
    int Recv(char *buf, int bufLen);

    /// Close the TCP socket.
    void Close();

    /// Get and clear pending socket error: getsockopt(SO_ERROR)
    int GetSocketError() const;
    static string ToString(const Address& addr);
    static int GetDefaultRecvBufSize() { return sRecvBufSize; }
    static int GetDefaultSendBufSize() { return sSendBufSize; }
    static void SetDefaultRecvBufSize(int size) { sRecvBufSize = size; }
    static void SetDefaultSendBufSize(int size) { sSendBufSize = size; }
private:
    int mSockFd;

    void SetupSocket();
    int Perror(const char* msg) const;
    int Perror(const Address& addr) const;
    int Perror(const char* msg, int err) const;

    static int sRecvBufSize;
    static int sSendBufSize;
};

typedef boost::shared_ptr<TcpSocket> TcpSocketPtr;

}

#endif // _LIBIO_TCP_SOCKET_H
