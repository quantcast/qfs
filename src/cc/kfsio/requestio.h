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
// \brief requestio.h: Common synchronous request send and receive routines.
//
//----------------------------------------------------------------------------

#ifndef _LIBIO_REQUESTIO_H
#define _LIBIO_REQUESTIO_H

#include <stddef.h>

namespace KFS
{

class TcpSocket;

/// Synchronously send request -- data one or two buffers: req and body. 
/// 
int SendRequest(const char* req, size_t rlen, const char* body, size_t blen,
    TcpSocket* sock);

/// Get a response from the server.  The response is assumed to
/// terminate with "\r\n\r\n".
/// @param[in/out] buf that should be filled with data from server
/// @param[in] bufSize size of the buffer
///
/// @param[in] sock the socket from which data should be read
/// @retval # of bytes that were read; 0/-1 if there was an error
///
/// @param[out] delims the position in the buffer where "\r\n\r\n"
/// occurs; in particular, the length of the response string that ends
/// with last "\n" character.  If the buffer got full and we couldn't
/// find "\r\n\r\n", delims is set to -1.
///
int RecvResponseHeader(char* buf, int bufSize, TcpSocket* sock, int opTimeout,
    int* delims);

}

#endif /* _LIBIO_REQUESTIO_H */
