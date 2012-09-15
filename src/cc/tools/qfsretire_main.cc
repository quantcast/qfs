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

#include "kfsio/TcpSocket.h"
#include "common/MsgLogger.h"

#include "monutils.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>
#include <string>

using std::string;
using std::cout;
using namespace KFS;
using namespace KFS_MON;

// # of secs for which the node is being hibernated
static int
RetireChunkserver(const ServerLocation &metaLoc, const ServerLocation &chunkLoc,
                  int sleepTime)
{
    RetireChunkserverOp op(1, chunkLoc, sleepTime);
    return (ExecuteOp(metaLoc, op) < 0 ? 1 : 0);
}

int
main(int argc, char **argv)
{
    char optchar;
    bool help = false;
    const char *metaserver = NULL, *chunkserver = NULL;
    int metaport = -1, chunkport = -1, sleepTime = -1;
    bool verboseLogging = false;

    while ((optchar = getopt(argc, argv, "hm:p:c:d:s:v")) != -1) {
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
            default:
                help = true;
                break;
        }
    }

    help = help || !metaserver || !chunkserver;

    if (help) {
        cout << "Usage: " << argv[0] << " [-m <metaserver> -p <port>] [-c <chunkserver> -d <port>] "
             << " [-s <sleeptime in seconds>] [-v]" << "\n";
        return -1;
    }

    MsgLogger::Init(0, verboseLogging ?
        MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelINFO);

    ServerLocation metaLoc(metaserver, metaport);
    ServerLocation chunkLoc(chunkserver, chunkport);

    return RetireChunkserver(metaLoc, chunkLoc, sleepTime);
}

