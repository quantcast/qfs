//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/10/28
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
// \brief Program that behaves like cat: cat foo | KfsPut <filename>
//----------------------------------------------------------------------------

#include "libclient/KfsClient.h"
#include "common/MsgLogger.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <iostream>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>

using std::cout;
using std::cin;
using std::string;

using namespace KFS;

static KfsClient* gKfsClient;

static ssize_t doPut(const string &kfspathname);

int
main(int argc, char **argv)
{
    int         optchar;
    string      kfspathname;
    string      serverHost;
    int         port           = -1;
    bool        help           = false;
    bool        verboseLogging = false;
    const char* config         = 0;
    ssize_t     numBytes;

    while ((optchar = getopt(argc, argv, "hs:p:f:v")) != -1) {
        switch (optchar) {
            case 'f':
                kfspathname = optarg;
                break;
            case 's':
                serverHost = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'h':
                help = true;
                break;
            case 'v':
                verboseLogging = true;
                break;
            case 'c':
                config = optarg;
                break;
            default:
                cout << "Unrecognized flag : " << optchar << "\n";
                help = true;
                break;
        }
    }

    if (help || kfspathname.empty() || serverHost.empty() || port <= 0) {
        cout << "Usage: " << argv[0] <<
            " -s <meta server name> -p <port> -f <Qfsfile> [-v]\n"
            "Reads from stdin and writes to given qfs file.\n";
        return 1;
    }

    gKfsClient = KfsClient::Connect(serverHost, port, config);
    if (!gKfsClient) {
        cout << "qfs client failed to initialize...exiting" << "\n";
        return 1;
    }

    if (verboseLogging) {
        KFS::MsgLogger::SetLevel(KFS::MsgLogger::kLogLevelDEBUG);
    } else {
        KFS::MsgLogger::SetLevel(KFS::MsgLogger::kLogLevelINFO);
    }

    numBytes = doPut(kfspathname);
    if (numBytes <= 0) {
        cout << "Wrote " << numBytes << " to " << kfspathname << "\n";
    }
    delete gKfsClient;

    return (numBytes < 0 ? 1 : 0);
}

ssize_t
doPut(const string &filename)
{
    static char dataBuf[6 * 1024 * 1024];
    ssize_t     bytesWritten = 0;

    const int fd = gKfsClient->Open(filename.c_str(), O_CREAT|O_RDWR);
    if (fd < 0) {
        cout << "Create failed: " << ErrorCodeToStr(fd) << "\n";
        return fd;
    }
    while(cin.read(dataBuf, sizeof(dataBuf))) {
        const size_t cnt = cin.gcount();
        const int    res = gKfsClient->Write(fd, dataBuf, cnt);
        if (res != (int)cnt) {
            cout << "Write failed...expect to write: " << cnt <<
                " but only wrote: " << res << "\n";
            return -1;
        }
        bytesWritten += res;
    }
    gKfsClient->Close(fd);
    return bytesWritten;
}

