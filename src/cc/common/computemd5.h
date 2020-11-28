//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2020/11/26
// Author: Mike Ovsiannikov
//
// Copyright 2020 Quantcast Corporation. All rights reserved.
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
// \file computemd5.h
// \brief compute md5 of a file.
//
//----------------------------------------------------------------------------

#ifndef COMMON_COMPUTEMD5_H
#define COMMON_COMPUTEMD5_H

#include "MdStream.h"
#include "MsgLogger.h"
#include "qcdio/QCUtils.h"
#include <fstream>

namespace KFS {

using std::fstream;

inline static string
ComputeMD5(const char* const inPathnamePt, bool const inInfoMsgFlag = true)
{
    const size_t kBufSize = size_t(1) << 20;
    char* const  buf      = new char[kBufSize];
    fstream      is(inPathnamePt, fstream::in | fstream::binary);
    string       ret;
    MdStream     mds(0, false, string(), 0);

    while (is && mds) {
        is.read(buf, kBufSize);
        mds.write(buf, is.gcount());
    }
    delete [] buf;

    if (! is.eof() || ! mds) {
        KFS_LOG_STREAM_ERROR <<
            "md5sum " << QCUtils::SysError(errno, inPathnamePt) <<
        KFS_LOG_EOM;
    } else {
        ret = mds.GetMd();
        if (inInfoMsgFlag)
        {
            KFS_LOG_STREAM_INFO <<
                "md5sum " << inPathnamePt << ": " << ret <<
            KFS_LOG_EOM;
        }
    }
    is.close();
    return ret;
}

} /* namespace KFS */
 
#endif /* COMMON_COMPUTEMD5_H */
