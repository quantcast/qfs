//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/10/25
// Author: Mike Ovsiannikov
//
// Copyright 2012 Quantcast Corp.
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
// \file nofilelimit.cc
// \brief function to set process max number of open files.
//
//----------------------------------------------------------------------------


#include "nofilelimit.h"
#include "MsgLogger.h"
#include "qcdio/QCUtils.h"

#include <sys/resource.h>
#include <errno.h>

namespace KFS
{

int
SetMaxNoFileLimit()
{
    struct rlimit theRlim       = {0};
    int           theMaxOpenFds = 0;

    // bump the soft limit to the hard limit
    if (getrlimit(RLIMIT_NOFILE, &theRlim) == 0) {
        const rlim_t kMaxFdLimit = 1 << 20;
        int          theErr      = 0;
        if (theRlim.rlim_cur >= kMaxFdLimit) {
            theMaxOpenFds = (int)kMaxFdLimit;
        } else {
            theMaxOpenFds = (int)theRlim.rlim_cur;
            if (theRlim.rlim_max > kMaxFdLimit) {
                theRlim.rlim_cur = kMaxFdLimit;
            } else {
                theRlim.rlim_cur = theRlim.rlim_max;
            }
            while (theMaxOpenFds < (int)theRlim.rlim_cur &&
                   setrlimit(RLIMIT_NOFILE, &theRlim) != 0) {
                theErr = errno;
                theRlim.rlim_cur /= 2;
            }
            if ((int)theRlim.rlim_cur > theMaxOpenFds) {
                theMaxOpenFds = (int)theRlim.rlim_cur;
            }
            if (theErr) {
                KFS_LOG_STREAM_ERROR <<
                    "setrilimit(RLIMIT_NOFILE, " <<
                    theRlim.rlim_cur * 2 <<
                    "): " <<
                    QCUtils::SysError(theErr) <<
                KFS_LOG_EOM;
            }
        }
    } else {
        const int theErr = errno;
        KFS_LOG_STREAM_ERROR <<
            "getrlimit(RLIMIT_NOFILE): " <<
            QCUtils::SysError(theErr) <<
        KFS_LOG_EOM;
    }
    KFS_LOG_STREAM_INFO <<
        "max # of open files: " << theMaxOpenFds <<
    KFS_LOG_EOM;
    return theMaxOpenFds;
}
    
}
