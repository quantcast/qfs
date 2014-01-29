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
// \brief Program that behaves like cat...:
// Kfscat -p <kfsConfig file> [filename1...n]
// and output the files in the order of appearance to stdout.
//
//----------------------------------------------------------------------------

#include "libclient/KfsClient.h"
#include "common/MsgLogger.h"

#include <iostream>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using std::cerr;
using std::string;
using namespace KFS;

static ssize_t DoCat(KfsClient *kfsClient, const char *pahtname);

int
main(int argc, char **argv)
{
    string      serverHost;
    int         port           = -1;
    bool        help           = false;
    bool        verboseLogging = false;
    const char* config         = 0;
    int         optchar;

    while ((optchar = getopt(argc, argv, "hs:p:vf:")) != -1) {
        switch (optchar) {
            case 'h':
                help = true;
                break;
            case 'v':
                verboseLogging = true;
                break;
            case 's':
                serverHost = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'f':
                config = optarg;
                break;
            default:
                help = true;
                break;
        }
    }

    if (help || serverHost.empty() || port <= 0) {
        cerr <<
            "Usage: " << argv[0] << " -s <meta server name> -p <port>"
            " [-f <config file>]"
            " [filename1 filename2 ...]\n"
            "This tool outputs the files in the order of appearance to"
            " stdout.\n";
        return 1;
    }

    MsgLogger::Init(0, verboseLogging ?
        MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelINFO);

    KfsClient* const kfsClient = KfsClient::Connect(serverHost, port, config);
    if (!kfsClient) {
        cerr << "qfs client failed to initialize...exiting" << "\n";
        return 1;
    }

    for (int i = optind; i < argc; ++i) {
        const int res = DoCat(kfsClient, argv[i]);
        if (res != 0) {
            cerr << argv[i] << ": " << ErrorCodeToStr(res) << "\n";
        }
    }
    delete kfsClient;
    return 0;
}

ssize_t
DoCat(KfsClient *kfsClient, const char *pathname)
{
    const int bufSize = 4 << 20;
    static char dataBuf[bufSize];

    const int fd = kfsClient->Open(pathname, O_RDONLY);
    if (fd < 0) {
        return fd;
    }

    int res;
    for (; ;) {
        res = kfsClient->Read(fd, dataBuf, bufSize);
        if (res <= 0)
            break;
        for (const char* p = dataBuf, * const e = p + res; p < e; ) {
            const ssize_t n = write(1, p, e - p);
            if (n < 0) {
                if (errno != EINTR && errno != EAGAIN) {
                    res = errno;
                    break;
                }
            }
            p += n;
        }
        if (res < 0) {
            break;
        }
    }
    kfsClient->Close(fd);

    return res;
}

