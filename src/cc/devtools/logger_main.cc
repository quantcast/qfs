//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2010/03/07
// Author: Mike Ovsiannikov
//
// Copyright 2010-2012 Quantcast Corp.
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

#include "common/BufferedLogWriter.h"
#include "qcdio/QCUtils.h"
#include "common/Properties.h"

#include <iostream>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <sys/time.h>
#include <errno.h>
#include <string.h>

using namespace KFS;
using namespace std;

static uint64_t
Now()
{
    struct timeval theTime;
    if (gettimeofday(&theTime, 0)) {
        QCUtils::FatalError("gettimeofday", errno);
    }
    return (int64_t(theTime.tv_sec) * 1000000 + theTime.tv_usec);
}

int
main(int argc, char** argv)
{
    if (argc > 1 && (!strcmp(argv[1], "-h") || !strcmp(argv[1], "--help"))) {
        return 0;
    }

    BufferedLogWriter theLogWriter(fileno(stderr), 0, 1 << 20);
    if (argc > 1) {
        Properties theProps;
        if (theProps.loadProperties(argv[1], '=', &cout) != 0) {
            return 1;
        }
        theLogWriter.SetParameters(theProps, "logWriter.");
    }
    if (argc > 2) {
        int theMax = (int)atof(argv[2]);
        uint64_t theStart = Now();
        uint64_t thePrev  = theStart;
        for (int i = 1; i <= theMax; i++) {
            theLogWriter.Append(BufferedLogWriter::kLogLevelDEBUG, "%d", i);
            if ((i & 0xFFFFF) == 0) {
                const uint64_t theNow = Now();
                cout << i * 1e6 / (double(theNow - theStart) + 1e-7) <<
                    " rec/sec avg " <<
                    1e6 * 0xFFFFF / (double(theNow - thePrev) + 1e-7) <<
                    " rec/sec\n";
                thePrev = theNow;
            }
        }
    } else {
        string theLine;
        while (getline(cin, theLine)) {
            theLogWriter.Append(BufferedLogWriter::kLogLevelDEBUG,
                "%s", theLine.c_str());
        }
    }
    return 0;
}
