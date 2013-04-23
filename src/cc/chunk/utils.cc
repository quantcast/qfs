//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/09/27
// Author: Sriram Rao
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
// 
//----------------------------------------------------------------------------

#include <sys/time.h>
#include <unistd.h>
#include <openssl/rand.h>

#include "utils.h"
#include "common/config.h"
#include "common/MsgLogger.h"
#include "kfsio/IOBuffer.h"

namespace KFS
{
using std::string;

///
/// Return true if there is a sequence of "\r\n\r\n".
/// @param[in] iobuf: Buffer with data sent by the client
/// @param[out] msgLen: string length of the command in the buffer
/// @retval true if a command is present; false otherwise.
///
bool IsMsgAvail(IOBuffer *iobuf, int *msgLen)
{
    const int idx = iobuf->IndexOf(0, "\r\n\r\n");
    if (idx < 0) {
        return false;
    }
    *msgLen = idx + 4; // including terminating seq. length.
    return true;
}

void die(const string &msg)
{
    string lm = "panic: " + msg;
    KFS_LOG_STREAM_FATAL << lm << KFS_LOG_EOM;
    lm += "\n";
    const ssize_t UNUSED_ATTR r = write(2, msg.data(), msg.size());
    MsgLogger::Stop();
    abort();
}

kfsSeq_t GetRandomSeq()
{
    kfsSeq_t id = 0;
    RAND_pseudo_bytes(
        reinterpret_cast<unsigned char*>(&id), int(sizeof(id)));
    return ((id < 0 ? -id : id) >> 1);
}

}
