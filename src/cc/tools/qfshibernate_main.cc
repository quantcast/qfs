//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/06/20
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
// \brief Tool that tells the metaserver to mark a node "down" for
// planned downtime.  Nodes can either be hibernated or retired:
// hibernation is a promise that the server will be back after N secs;
// retire => node is going down and don't know when it will be back.
// When a node is "retired" in this manner, the metaserver uses the
// retiring node to proactively replicate the blocks from that server
// to other nodes.
//
//----------------------------------------------------------------------------

#include "MonClient.h"
#include "common/MsgLogger.h"
#include "libclient/KfsClient.h"
#include "libclient/KfsOps.h"

#include <iostream>
#include <string>

using std::string;
using std::cout;

using namespace KFS;
using namespace KFS::client;
using namespace KFS_MON;

int
main(
    int    argc,
    char** argv)
{
    int         optchar;
    bool        help           = false;
    const char* metaserver     = 0;
    const char* chunkserver    = 0;
    const char* configFileName = 0;
    int         metaport       = -1;
    int         chunkport      = -1;
    int         sleepTime      = -1;
    bool        verboseLogging = false;

    while ((optchar = getopt(argc, argv, "hm:p:c:d:s:vf:")) != -1) {
        switch (optchar) {
            case 'm':
                metaserver = optarg;
                break;
            case 'c':
                chunkserver = optarg;
                break;
            case 'p':
                metaport = atoi(optarg);
                break;
            case 'd':
                chunkport = atoi(optarg);
                break;
            case 's':
                sleepTime = atoi(optarg);
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
    help = help || ! metaserver || ! chunkserver || sleepTime <= 0 ||
        metaport <= 0 || chunkport <= 0;
    if (help) {
        cout <<
            "Usage: " << argv[0] << "\n"
            " -m <metaserver>\n"
            " -p <port>\n"
            " -c <chunkserver>\n"
            " -d <port>\n"
            " -s <sleeptime in seconds>\n"
            " -f <config file name>\n"
            " [-v]\n"
            "Hibernates the chunkserver for 'sleeptime' seconds.\n"
        ;
        return 1;
    }

    MsgLogger::Init(0, verboseLogging ?
        MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelINFO);

    const ServerLocation metaLoc (metaserver,  metaport);
    const ServerLocation chunkLoc(chunkserver, chunkport);
    RetireChunkserverOp  op(0, chunkLoc, sleepTime);
    MonClient            client;
    int                  status = client.SetParameters(metaLoc, configFileName);
    if (status == 0) {
        status = client.Execute(metaLoc, op);
    }
    if (status < 0) {
        KFS_LOG_STREAM_ERROR <<
            "hibernate failure: " << op.statusMsg <<
            " " << ErrorCodeToStr(status) <<
        KFS_LOG_EOM;
    }
    return (status < 0 ? 1 : 0);
}

