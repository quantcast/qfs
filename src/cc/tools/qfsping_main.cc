//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/07/20
// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
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
// \brief Meta and chunk server status report.
//----------------------------------------------------------------------------

#include "MonClient.h"
#include "common/MsgLogger.h"
#include "libclient/KfsClient.h"
#include "libclient/KfsOps.h"

#include <iostream>
#include <string>
#include <unistd.h>

using std::string;
using std::cout;
using std::vector;
using namespace KFS;
using namespace KFS::client;
using namespace KFS_MON;

inline static double
convertToMB(long bytes)
{
    return bytes / (1024.0 * 1024.0);
}

static int
PingMetaServer(MonClient& client, const ServerLocation& location)
{
    // PING response is all in the "header".
    // Set the limit to a reasonably large value.
    client.SetMaxRpcHeaderLength(32 << 20);
    MetaPingOp op(1);
    const int ret = client.Execute(location, op);
    if (ret < 0) {
        KFS_LOG_STREAM_ERROR << op.statusMsg <<
            " error: " << ErrorCodeToStr(ret) <<
        KFS_LOG_EOM;
        return 1;
    }
    if (op.upServers.size() == 0) {
        cout << "No chunkservers are connected" << "\n";
    } else {
        cout << "Up servers: " << op.upServers.size() << "\n";
        for (size_t i = 0; i < op.upServers.size(); ++i) {
            cout << op.upServers[i] << "\n";
        }
    }
    if (op.downServers.size() > 0) {
        cout << "Down servers: " << op.downServers.size() << "\n";
        for (size_t i = 0; i < op.downServers.size(); ++i) {
            cout << op.downServers[i] << "\n";
        }
    }
    return 0;
}

static int
PingChunkServer(MonClient& client, const ServerLocation& location)
{
    ChunkPingOp op(1);
    if (client.Execute(location, op) < 0) {
        return 1;
    }
    cout << "Chunk-server: " << op.location.ToString().c_str() << "\n";
    cout << "Total-space: " << convertToMB(op.totalSpace) << " (MB) " << "\n";
    cout << "Used-space: " << convertToMB(op.usedSpace) << " (MB) " << "\n";
    return 0;
}

int main(int argc, char** argv)
{
    int         optchar;
    bool        help           = false;
    bool        meta           = false;
    bool        chunk          = false;
    const char* server         = 0;
    const char* configFileName = 0;
    int         port           = -1;
    bool        verboseLogging = false;

    while ((optchar = getopt(argc, argv, "hmcs:p:vf:")) != -1) {
        switch (optchar) {
            case 'm':
                meta = true;
                break;
            case 'c':
                chunk = true;
                break;
            case 's':
                server = optarg;
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
            case 'f':
                configFileName = optarg;
                break;
            default:
                help = true;
                break;
        }
    }

    help = help || (! meta && ! chunk);
    if (help || ! server || port < 0) {
        cout << "Usage: " << argv[0] <<
            " [-m|-c] -s <server name> -p <port> [-v]\n"
            "Deprecated. Please use qfsadmin instead.\n"
            "   -c : ping chunkserver. Deprecated."
                    " Chunk server ping is not supported"
                    " with QFS authentication.\n"
            "   -m : ping metaserver.\n"
        ;

        return 1;
    }

    MsgLogger::Init(0, verboseLogging ?
        MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelINFO);

    const ServerLocation loc(server, port);
    MonClient client;
    if (client.SetParameters(loc, configFileName) < 0) {
        return 1;
    }
    if (meta) {
        return PingMetaServer(client, loc);
    } else if (chunk) {
        if (client.IsAuthEnabled()) {
            KFS_LOG_STREAM_ERROR <<
                "Chunk sever ping is not supported with QFS authentication."
                " Please consider using qfsadmin get_chunk_servers_counters." <<
            KFS_LOG_EOM;
            return 1;
        }
        return PingChunkServer(client, loc);
    }
    return 0;
}
