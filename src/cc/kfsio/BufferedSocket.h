//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/07/03
// Author: Sriram Rao
//
// Copyright 2008 Quantcast Corp.
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
// \brief Helper class that has a socket with a buffer.  This enables
// API such as readLine() where we read N bytes from the socket,
// return a line and leave the rest buffered for subsequent access.
// NOTE: This class only does buffered I/O for reads; for writes, it
// is all pass-thru to the TcpSocket class.
//----------------------------------------------------------------------------

#ifndef LIBKFSIO_BUFFEREDSOCKET_H
#define LIBKFSIO_BUFFEREDSOCKET_H

#include "TcpSocket.h"

#include <string>
#include <cerrno>
#include <cassert>
#include <string.h>

namespace KFS
{

class BufferedSocket : public TcpSocket
{
public:
    BufferedSocket() {
        Reset();
    }
    BufferedSocket(int fd) : TcpSocket(fd) {
        Reset();
    }
    /// Read a line of data which is terminated by a '\n' from the
    /// socket.
    /// @param[out] result   The string that corresponds to data read
    /// from the network
    /// @retval # of bytes read; -1 on error
    int ReadLine(std::string &result);

    /// Synchronously (blocking) receive for the desired # of bytes.
    /// Note that we first pull data out the buffer and if there is
    /// more to be read, we get it from the socket.
    /// @param[out] buf  The buffer to be filled with data from the
    /// socket.
    /// @param[in] bufLen  The desired # of bytes to be read in
    /// @param[in] timeout  The max amount of time to wait for data
    /// @retval # of bytes read; -1 on error; -ETIMEOUT if timeout
    /// expires and no data is read in
    int DoSynchRecv(char *buf, int bufLen, struct timeval &timeout);

    /// Read at most the specified # of bytes from the socket.
    /// Note that we first pull data out the buffer and if there is
    /// more to be read, we get it from the socket.  The read is
    /// non-blocking: if recv() returns EAGAIN (to indicate that no
    /// data is available), we return how much ever data we have read
    /// so far.
    /// @param[out] buf  The buffer to be filled with data from the
    /// socket.
    /// @param[in] bufLen  The desired # of bytes to be read in
    /// @retval # of bytes read; -1 on error
    int Recv(char *buf, int bufLen);

private:
    const static int BUF_SIZE = 4096;
    /// The buffer into which data has been read from the socket.
    char mBuffer[BUF_SIZE];
    /// Since we have read from the buffer, head tracks where the next
    /// character is available for read from mBuffer[]
    char *mHead;
    /// How much data is in the buffer
    int mAvail;

    void Reset() {
        mHead = mBuffer;
        mAvail = 0;
        memset(mBuffer, '\0', BUF_SIZE);
    }
    void Fill(int nbytes) {
        mAvail += nbytes;
        assert(mAvail <= BUF_SIZE);
    }
    void Consume(int nbytes) {
        mHead += nbytes;
        mAvail -= nbytes;
        if (mAvail == 0)
            Reset();
        assert(mAvail >= 0);
    }
};

}

#endif // LIBKFSIO_BUFFEREDSOCKET_H
