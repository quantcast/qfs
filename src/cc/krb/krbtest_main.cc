//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/06/09
// Author: Mike Ovsiannikov
//
// Copyright 2013 Quantcast Corp.
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
// Kerberos 5 service / client authentication test.
//
//----------------------------------------------------------------------------

#include "KrbService.h"
#include "KrbClient.h"

#include <iostream>

namespace
{
using KFS::KrbClient;
using KFS::KrbService;

using std::cerr;
using std::cout;

class QfsKrbTest
{
public:
    QfsKrbTest()
        : mClient(),
          mService()
        {}
    int Run(
        int    inArgsCount,
        char** inArgsPtr)
    {
        if (inArgsCount != 4 && inArgsCount != 3) {
            cerr <<
                "Usage: " << (inArgsCount >= 1 ? inArgsPtr[0] : "") <<
                " <service host> <service name> [<keytab name>]\n"
            ;
            return 1;
        }
        const char* theErrMsgPtr = mClient.Init(inArgsPtr[1], inArgsPtr[2]);
        if (theErrMsgPtr) {
            cerr <<
                "client init error: " << theErrMsgPtr << "\n";
            return 1;
        }
        theErrMsgPtr = mClient.Init(
            inArgsPtr[2], inArgsCount == 3 ? 0 : inArgsPtr[3]);
        if (theErrMsgPtr) {
            cerr <<
                "service init error: " << theErrMsgPtr << "\n";
            return 1;
        }
        return 0;
    }
private:
    KrbClient  mClient;
    KrbService mService;
private:
    QfsKrbTest(
        const QfsKrbTest& inTest);
    QfsKrbTest& operator=(
        const QfsKrbTest& inTest);
};

}

    int
main(
    int    inArgsCount,
    char** inArgsPtr)
{
    QfsKrbTest theTest;
    return theTest.Run(inArgsCount, inArgsPtr);
}
