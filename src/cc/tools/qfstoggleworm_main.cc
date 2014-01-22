//---------------------------------------------------------- -*- Mode: C++ -*-
//
// Created 2008/10/06
// Author: Lohit VijayaRenu
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
// \brief Toggle WORM mode of KFS
//----------------------------------------------------------------------------

#include "MonClient.h"
#include "common/MsgLogger.h"
#include "libclient/KfsOps.h"
#include "libclient/KfsClient.h"

#include <iostream>

using std::cout;
using namespace KFS;
using namespace KFS_MON;
using namespace KFS::client;

static int
ToggleWORM(MonClient& client, const ServerLocation& server, int toggle)
{
    MetaToggleWORMOp op(1, toggle);
    const int ret = client.Execute(server, op);
    if (ret != 0) {
        KFS_LOG_STREAM_ERROR << op.statusMsg <<
            " error: " << ErrorCodeToStr(ret) <<
        KFS_LOG_EOM;
        return 1;
    }
    cout << "MetaServer " << server << " WORM mode " <<
        (toggle ? "ON" : "OFF") << "\n";
    return 0;
}

int main(int argc, char **argv)
{
    bool        help           = false;
    const char* server         = 0;
    int         port           = -1;
    bool        verboseLogging = false;
    int         toggle         = -1;
    const char* configFileName = 0;

    int optchar;
    while ((optchar = getopt(argc, argv, "hs:p:t:vf:")) != -1) {
        switch (optchar) {
            case 's':
                server = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 't':
                toggle = atoi(optarg);
                break;
            case 'h':
                help = true;
                break;
            case 'v':
                verboseLogging = true;
                break;
            case 'f':
                configFileName = optarg;
                break;
            default:
                help = true;
                break;
        }
    }

    MsgLogger::Init(0, verboseLogging ?
        MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelINFO);

    if (help || ! server || port <= 0 || (toggle != 0 && toggle != 1)) {
        cout <<
            "Usage: " << argv[0] <<
            " -s <server name> -p <port> -t [1|0] [-v]\n"
            "Toggles the WORM mode of the filesystem.\n"
            " -s : meta server\n"
            " -p : meta server port\n"
            " -t : toggle value (0 or 1)\n"
            " -v : verbose\n"
        ;
        return 1;
    }
    MonClient client;
    const ServerLocation loc(server, port);
    if (client.SetParameters(loc, configFileName) < 0) {
        return 1;
    }
    return ToggleWORM(client, loc, toggle);
}
