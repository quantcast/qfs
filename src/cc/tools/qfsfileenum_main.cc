//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/05/05
// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
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
// \brief Debug utility to list out where the chunks of a file are.
//----------------------------------------------------------------------------

#include "common/MsgLogger.h"
#include "libclient/KfsClient.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>
#include <iomanip>
#include <string>

using namespace KFS;

using std::string;
using std::cout;
using std::setw;

int
main(int argc, char **argv)
{
    bool        help           = false;
    const char* server         = 0;
    const char* filename       = 0;
    const char* config         = 0;
    int         port           = -1;
    bool        verboseLogging = false;
    int         retval;
    int         optchar;

    while ((optchar = getopt(argc, argv, "hs:p:f:vc:")) != -1) {
        switch (optchar) {
            case 's':
                server = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'f':
                filename = optarg;
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
                help = true;
                break;
        }
    }

    if (help || ! server || port < 0 || ! filename) {
        cout << "Usage: " << argv[0] <<
            " -s <server name> -p <port> -f <path> [-v] -c <config file>\n"
            "Enumerate the chunks and sizes of the given file.\n";
        return 1;
    }

    MsgLogger::Init(0, verboseLogging ?
        MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelINFO);

    KfsClient* const kfsClient = KfsClient::Connect(server, port, config);
    if (! kfsClient) {
        cout << "qfs client failed to initialize\n";
        return 1;
    }

    KfsClient::BlockInfos infos;
    retval = kfsClient->EnumerateBlocks(filename, infos);
    if (retval < 0) {
        cout << filename << ": " << ErrorCodeToStr(retval) << "\n";
    } else {
        int64_t space = 0;
        cout << "block count: " << infos.size() << " blocks:\n";
        const KfsClient::BlockInfo* pbi = 0;
        for (KfsClient::BlockInfos::const_iterator it = infos.begin();
                it != infos.end();
                ++it) {
            KfsClient::BlockInfo bi = *it;
            cout <<
                "position: "  << setw(10) << bi.offset  << setw(0) <<
                " id: "       << setw(10) << bi.id      << setw(0) <<
                " version: "  << setw(3)  << bi.version << setw(0) <<
                " size: "     << setw(8)  << bi.size    << setw(0) <<
                " location: " << setw(12) << bi.server  << setw(0)
            ;
            if (pbi && pbi->id == bi.id) {
                if (pbi->offset != bi.offset) {
                    cout << " *position mismatch*";
                    retval = -EINVAL;
                }
                if (pbi->version != bi.version) {
                    cout << " *version mismatch*";
                    retval = -EINVAL;
                }
                if (pbi->size != bi.size) {
                    cout << " *size mismatch*";
                    retval = -EINVAL;
                }
            }
            pbi = &bi;
            cout << "\n";
            if (bi.size > 0) {
                space += bi.size;
            }
        }
        cout << "total space: " << space << "\n";
    }
    delete kfsClient;
    return (retval >= 0 ? 0 : 1);
}


