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
// \brief Get the stats from chunk/meta servers.  Run in a loop until
// the user hits Ctrl-C.  Like iostat, results are refreshed every N secs
//----------------------------------------------------------------------------

#include "MonClient.h"
#include "common/MsgLogger.h"
#include "libclient/KfsClient.h"
#include "libclient/KfsOps.h"

#include <iostream>
#include <string>


using namespace KFS;
using namespace KFS_MON;
using namespace KFS::client;

using std::string;
using std::cout;

static int
StatsMetaServer(MonClient& client, const ServerLocation &location,
    bool rpcStats, int numSecs);

static int
BasicStatsMetaServer(MonClient& client, const ServerLocation &location,
    int numSecs);

static int
RpcStatsMetaServer(MonClient& client, const ServerLocation &location,
    int numSecs);

static int
StatsChunkServer(MonClient& client, const ServerLocation &location,
    bool rpcStats, int numSecs);

static int
BasicStatsChunkServer(MonClient& client, const ServerLocation &location,
    int numSecs);

static int
RpcStatsChunkServer(MonClient& client, const ServerLocation &location,
    int numSecs);

static void
PrintChunkBasicStatsHeader();

static void
PrintMetaBasicStatsHeader();


int
main(int argc, char **argv)
{
    int         optchar;
    bool        help           = false;
    bool        meta           = false;
    bool        chunk          = false;
    bool        rpcStats       = false;
    bool        verboseLogging = false;
    const char* server         = 0;
    const char* configFileName = 0;
    int         port           = -1;
    int         numSecs        = 10;

    while ((optchar = getopt(argc, argv, "hcmn:p:s:tvf:")) != -1) {
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
            case 'n':
                numSecs = atoi(optarg);
                break;
            case 't':
                rpcStats = true;
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

    help = help || (!meta && !chunk);

    if (help || (server == NULL) || (port < 0)) {
        cout << "Usage: " << argv[0] <<
             " [-m|-c] -s <server name> -p <port> [-n <secs>] [-t] [-v]"
             " [-f <config file>]\n"
             "Deprecated. Please use qfsadmin instead.\n"
             "Gets the stats from meta/chunk servers at given intervals.\n"
             "        Use -m for metaserver, -c for chunk server.\n"
             "        Use -t for RPC stats.\n"
             "        Use -n <seconds> to specify interval (default 10s)."
             "\n";
        return -1;
    }

    MsgLogger::Init(0, verboseLogging ?
        MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelINFO);

    const ServerLocation location(server, port);
    MonClient            client;
    if (client.SetParameters(location, configFileName) < 0) {
        return 1;
    }
    client.SetMaxContentLength(128 << 20);
    if (meta) {
        return StatsMetaServer(client, location, rpcStats, numSecs);
    }
    if (chunk) {
        return StatsChunkServer(client, location, rpcStats, numSecs);
    }
    return 0;
}

static void
PrintRpcStat(const string &statName, Properties &prop)
{
    // cout << statName << " = " << prop.getValue(statName, (long
    // long) 0) << "\n";
    cout << statName << " = " << prop.getValue(statName, "0") << "\n";
}


int
StatsMetaServer(MonClient& client, const ServerLocation& loc, bool rpcStats, int numSecs)
{
    if (rpcStats) {
        return RpcStatsMetaServer(client, loc, numSecs);
    } else {
        return BasicStatsMetaServer(client, loc, numSecs);
    }
}

int
RpcStatsMetaServer(MonClient& client, const ServerLocation& loc, int numSecs)
{
    for (; ;) {
        MetaStatsOp op(0);
        const int ret = client.Execute(loc, op);
        if (ret < 0) {
            KFS_LOG_STREAM_ERROR << op.statusMsg <<
                " " << ErrorCodeToStr(ret) <<
            KFS_LOG_EOM;
            return 1;
        }

        PrintRpcStat("Get alloc", op.stats);
        PrintRpcStat("Get layout", op.stats);
        PrintRpcStat("Lookup", op.stats);
        PrintRpcStat("Lookup Path", op.stats);
        PrintRpcStat("Allocate", op.stats);
        PrintRpcStat("Truncate", op.stats);
        PrintRpcStat("Create", op.stats);
        PrintRpcStat("Remove", op.stats);
        PrintRpcStat("Rename", op.stats);
        PrintRpcStat("Mkdir", op.stats);
        PrintRpcStat("Rmdir", op.stats);
        PrintRpcStat("Lease Acquire", op.stats);
        PrintRpcStat("Lease Renew", op.stats);
        PrintRpcStat("Lease Cleanup", op.stats);
        PrintRpcStat("Chunkserver Hello", op.stats);
        PrintRpcStat("Chunkserver Bye", op.stats);
        PrintRpcStat("Replication Checker", op.stats);
        PrintRpcStat("Num Replications Todo", op.stats);
        PrintRpcStat("Num Ongoing Replications", op.stats);
        PrintRpcStat("Num Failed Replications", op.stats);
        PrintRpcStat("Total Num Replications", op.stats);
        PrintRpcStat("Num Stale Chunks", op.stats);
        PrintRpcStat("Number of Directories", op.stats);
        PrintRpcStat("Number of Files", op.stats);
        PrintRpcStat("Number of Chunks", op.stats);
        PrintRpcStat("Number of Hits in Path->Fid Cache", op.stats);
        PrintRpcStat("Number of Misses in Path->Fid Cache", op.stats);

        cout << "----------------------------------" << "\n";
        if (numSecs == 0) {
            break;
        }
        sleep(numSecs);
    }
    return 0;
}

int
BasicStatsMetaServer(MonClient& client, const ServerLocation& loc, int numSecs)
{
    PrintMetaBasicStatsHeader();
    for (int i = 1; ; i++) {
        MetaStatsOp op(0);
        const int ret = client.Execute(loc, op);
        if (ret < 0) {
            KFS_LOG_STREAM_ERROR << op.statusMsg <<
                " " << ErrorCodeToStr(ret) <<
            KFS_LOG_EOM;
            return 1;
        }
        // useful things to have: # of connections handled
        if (i % 10 == 0) {
            PrintMetaBasicStatsHeader();
        }
        cout << op.stats.getValue("Open network fds", (long long) 0) << '\t';
        cout << op.stats.getValue("Bytes read from network", (long long) 0) << '\t';
        cout << op.stats.getValue("Bytes written to network", (long long) 0) << "\n";

        if (numSecs == 0) {
            break;
        }
        sleep(numSecs);
    }
    return 0;
}

static void
PrintMetaBasicStatsHeader()
{
    cout << "Net Fds" << '\t' << "N/w Bytes In" << '\t'
         << "N/w Bytes Out" << "\n";
}

int
StatsChunkServer(MonClient& client, const ServerLocation& loc, bool rpcStats, int numSecs)
{
    if (client.GetAuthContext() && client.GetAuthContext()->IsEnabled()) {
        KFS_LOG_STREAM_WARN <<
            "Warning: chunk server stats are not supported"
            " with authentication enabled." <<
        KFS_LOG_EOM;
    }
    if (rpcStats) {
        return RpcStatsChunkServer(client, loc, numSecs);
    } else {
        return BasicStatsChunkServer(client, loc, numSecs);
    }
}

int
RpcStatsChunkServer(MonClient& client, const ServerLocation& loc, int numSecs)
{
    for (; ;) {
        ChunkStatsOp op(0);
        const int ret = client.Execute(loc, op);
        if (ret < 0) {
            KFS_LOG_STREAM_ERROR << op.statusMsg <<
                " " << ErrorCodeToStr(ret) <<
            KFS_LOG_EOM;
            return 1;
        }

        PrintRpcStat("Alloc", op.stats);
        PrintRpcStat("Size", op.stats);
        PrintRpcStat("Open", op.stats);
        PrintRpcStat("Read", op.stats);
        PrintRpcStat("Write", op.stats);
        PrintRpcStat("Write Prepare", op.stats);
        PrintRpcStat("Write Sync", op.stats);
        PrintRpcStat("Write Duration", op.stats);
        PrintRpcStat("Write Master", op.stats);
        PrintRpcStat("Delete", op.stats);
        PrintRpcStat("Truncate", op.stats);
        PrintRpcStat("Heartbeat", op.stats);
        PrintRpcStat("Change Chunk Vers", op.stats);
        PrintRpcStat("Num ops", op.stats);
        cout << "----------------------------------" << "\n";
        if (numSecs == 0) {
            break;
        }
        sleep(numSecs);
    }
    return 0;
}

int
BasicStatsChunkServer(MonClient& client, const ServerLocation& loc, int numSecs)
{
    PrintChunkBasicStatsHeader();
    for (int i = 0; ; i++) {
        ChunkStatsOp op(0);
        const int ret = client.Execute(loc, op);
        if (ret < 0) {
            KFS_LOG_STREAM_ERROR << op.statusMsg <<
                " error: " << ErrorCodeToStr(ret) <<
            KFS_LOG_EOM;
            return 1;
        }

        if (i % 10 == 0) {
            PrintChunkBasicStatsHeader();
        }
        cout << op.stats.getValue("Open network fds", (long long) 0) << '\t';
        cout << op.stats.getValue("Bytes read from network", (long long) 0) << '\t';
        cout << op.stats.getValue("Bytes written to network", (long long) 0) << '\t';
        cout << op.stats.getValue("Open disk fds", (long long) 0) << '\t';
        cout << op.stats.getValue("Bytes read from disk", (long long) 0) << '\t';
        cout << op.stats.getValue("Bytes written to disk", (long long) 0) << "\n";

        if (numSecs == 0) {
            break;
        }
        sleep(numSecs);
    }
    return 0;
}

static void
PrintChunkBasicStatsHeader()
{
    cout << "Net Fds" << '\t' << "N/w Bytes In" << '\t'
         << "N/w Bytes Out" << '\t'
         << "Disk Fds" << '\t' << "Disk Bytes In" << '\t'
         << "Disk Bytes Out" << "\n";
}
