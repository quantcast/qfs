//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/05/03
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
// \file rusage.cc
// \brief getrusage wrapper
//
//----------------------------------------------------------------------------

#ifndef RUSAGE_H
#define RUSAGE_H

#include "rusage.h"
#include <string.h>
#include <stdlib.h>
#include <ostream>

#include <sys/time.h>
#include <sys/resource.h>

namespace KFS
{
using std::ostream;

ostream&
showrusage(ostream& inStream, const char* inSeparatorPtr,
    const char* inDelimiterPtr, bool inSelfFlag)
{
    const char* const theSepPtr   = inSeparatorPtr ? inSeparatorPtr : ": ";
    const char* const theDelimPtr = inDelimiterPtr ? inDelimiterPtr : "\n";
    struct rusage theRusage;
    memset(&theRusage, 0, sizeof(theRusage));
    if (getrusage(inSelfFlag ? RUSAGE_SELF : RUSAGE_CHILDREN, &theRusage)) {
        inStream.setstate(ostream::failbit);
        return inStream;
    }
    inStream <<
        "utime" << theSepPtr <<
            theRusage.ru_utime.tv_sec * 1000 * 1000 +
            theRusage.ru_utime.tv_usec << theDelimPtr <<
        "stime" << theSepPtr <<
            theRusage.ru_stime.tv_sec * 1000 * 1000 +
            theRusage.ru_stime.tv_usec << theDelimPtr <<
        "maxrss"   << theSepPtr << theRusage.ru_maxrss   << theDelimPtr <<
        "ixrss"    << theSepPtr << theRusage.ru_ixrss    << theDelimPtr <<
        "idrss"    << theSepPtr << theRusage.ru_idrss    << theDelimPtr <<
        "isrss"    << theSepPtr << theRusage.ru_isrss    << theDelimPtr <<
        "minflt"   << theSepPtr << theRusage.ru_minflt   << theDelimPtr <<
        "majflt"   << theSepPtr << theRusage.ru_majflt   << theDelimPtr <<
        "inblock"  << theSepPtr << theRusage.ru_inblock  << theDelimPtr <<
        "oublock"  << theSepPtr << theRusage.ru_oublock  << theDelimPtr <<
        "msgsnd"   << theSepPtr << theRusage.ru_msgsnd   << theDelimPtr <<
        "msgrcv"   << theSepPtr << theRusage.ru_msgrcv   << theDelimPtr <<
        "nsignals" << theSepPtr << theRusage.ru_nsignals << theDelimPtr <<
        "nvscw"    << theSepPtr << theRusage.ru_nvcsw    << theDelimPtr <<
        "nivscw"   << theSepPtr << theRusage.ru_nivcsw   << theDelimPtr
    ;
    return inStream;
}

}

#endif /* RUSAGE_H */
