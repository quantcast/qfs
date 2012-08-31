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
// \brief Code for doing buffered reads from socket.
//
//----------------------------------------------------------------------------

#include <iostream>
#include <strings.h>

#include "BufferedSocket.h"

namespace KFS
{
using std::string;

int
BufferedSocket::ReadLine(string &result)
{
    int navail, nread = 0;
    char *lineEnd;

    lineEnd = index(mHead, '\n');
    if (lineEnd != NULL) {
        nread = lineEnd - mHead + 1;
        result.append(mHead, nread);
        Consume(nread);
        return nread;
    }

    // no line-end...so, copy out what is in the buffer
    if (mAvail > 0) {
        nread = mAvail;
        result.append(mHead, nread);
        Consume(nread);
    }

    // read until we get a new-line
    while (1) {
        navail = Recv(mBuffer, BUF_SIZE);
        if (navail == 0) {
            // socket is down
            return nread;
        }
        if ((navail < 0) && (errno == EAGAIN))
            continue;
        if (navail < 0)
            break;

        Fill(navail);

        lineEnd = index(mBuffer, '\n');
        if (lineEnd == NULL) {
            // haven't hit a line boundary...so, keep going
            result.append(mBuffer, navail);
            nread += navail;
            Consume(navail);
            continue;
        }
        navail = (lineEnd - mBuffer + 1);
        nread += navail;
        result.append(mBuffer, navail);
        Consume(navail);
        break;
    }
    return nread;
}

int
BufferedSocket::DoSynchRecv(char *buf, int bufLen, struct timeval &timeout)
{
    int nread = 0, res;

    // Copy out of the buffer and then, if needed, get from the socket.
    if (mAvail > 0) {
        nread = bufLen < mAvail ? bufLen : mAvail;
        memcpy(buf, mHead, nread);
        Consume(nread);
    }

    if ((bufLen - nread) <= 0)
        return nread;

    assert(mAvail == 0);

    res = TcpSocket::DoSynchRecv(buf + nread, bufLen - nread, timeout);
    if (res > 0)
        nread += res;
    return nread;

}

int
BufferedSocket::Recv(char *buf, int bufLen)
{
    int nread = 0, res;

    // Copy out of the buffer and then, if needed, get from the socket.
    if (mAvail > 0) {
        nread = bufLen < mAvail ? bufLen : mAvail;
        memcpy(buf, mHead, nread);
        Consume(nread);
    }

    if ((bufLen - nread) <= 0)
        return nread;

    assert(mAvail == 0);

    res = TcpSocket::Recv(buf + nread, bufLen - nread);
    if (res > 0) {
        nread += res;
        return nread;
    }
    if (nread == 0)
        return res;
    return nread;
}
}
