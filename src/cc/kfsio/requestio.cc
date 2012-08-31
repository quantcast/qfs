//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/08/10
// Author: Sriram Rao
//         Mike Ovsiannikov
//
// Copyright 2008-2012 Quantcast Corp.
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
// \brief requestio.h: Common synchronous request send and receive routines
// implementation.
//
//----------------------------------------------------------------------------

#include "requestio.h"
#include "common/MsgLogger.h"
#include "kfsio/TcpSocket.h"
#include "qcdio/QCUtils.h"

#include <errno.h>
#include <sys/time.h>

#include <assert.h>

#include <algorithm>

namespace KFS
{

using std::max;

int
SendRequest(const char* req, size_t rlen, const char* body, size_t blen,
    TcpSocket* sock)
{
    if (! sock || ! sock->IsGood()) {
        KFS_LOG_STREAM_DEBUG << "op send socket closed" << KFS_LOG_EOM;
        return -EINVAL;
    }
    int numIO = sock->DoSynchSend(req, rlen);
    if (blen > 0 && numIO > 0) {
        numIO = sock->DoSynchSend(body, blen);
    }
    if (numIO > 0) {
        return numIO;
    }
    KFS_LOG_STREAM_DEBUG << sock->GetPeerName() <<
        ": send failed: " << numIO << " " << QCUtils::SysError(-numIO) <<
    KFS_LOG_EOM;
    sock->Close();
    return (numIO < 0 ? numIO : -EINVAL);
}

int
RecvResponseHeader(char* buf, int bufSize, TcpSocket* sock, int opTimeout,
    int* delims)
{
    *delims = -1;
    for (int pos = 0; ;) {
        struct timeval timeout = {0};
        timeout.tv_sec = opTimeout;

        int nread = sock->DoSynchPeek(buf + pos, bufSize - pos, timeout);
        if (nread <= 0) {
            if (nread == -ETIMEDOUT) {
                return nread;
            }
            if (nread < 0 && (errno == EINTR || errno == EAGAIN)) {
                continue;
            }
            return nread;
        }
        for (int i = max(pos, 3); i < pos + nread; i++) {
            if ((buf[i - 3] == '\r') &&
                    (buf[i - 2] == '\n') &&
                    (buf[i - 1] == '\r') &&
                    (buf[i] == '\n')) {
                // valid stuff is from 0..i; so, length of resulting
                // string is i+1.
                i++;
                while (pos < i) {
                    if ((nread = sock->Recv(buf + pos, i - pos)) <= 0) {
                        if (nread < 0 && (errno == EINTR || errno == EAGAIN)) {
                            continue;
                        }
                        return nread;
                    }
                    pos += nread;
                }
                *delims = i;
                if (i < bufSize) {
                    buf[i] = 0;
                }
                return i;
            }
        }
        // Unload data from socket, otherwise peek will return immediately.
        if ((nread = sock->Recv(buf + pos, nread)) <= 0) {
            if (nread < 0 && (errno == EINTR || errno == EAGAIN)) {
                continue;
            }
            return nread;
        }
        pos += nread;
    }
    assert(! "not reached");
    return -1;
}

}
