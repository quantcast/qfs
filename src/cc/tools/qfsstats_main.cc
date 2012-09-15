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

#include "monutils.h"
#include "kfsio/TcpSocket.h"
#include "common/MsgLogger.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>
#include <string>


using namespace KFS;
using namespace KFS_MON;

using std::string;
using std::cout;
using std::endl;

static void
StatsMetaServer(const ServerLocation &location, bool rpcStats, int numSecs);

void
BasicStatsMetaServer(TcpSocket &metaServerSock, int numSecs);

void
RpcStatsMetaServer(TcpSocket &metaServerSock, int numSecs);

static void
StatsChunkServer(const ServerLocation &location, bool rpcStats, int numSecs);

static void
BasicStatsChunkServer(TcpSocket &chunkServerSock, int numSecs);

static void
RpcStatsChunkServer(TcpSocket &chunkServerSock, int numSecs);

static void
PrintChunkBasicStatsHeader();

static void
PrintMetaBasicStatsHeader();


int
main(int argc, char **argv)
{
    char optchar;
    bool help = false, meta = false, chunk = false;
    bool rpcStats = false, verboseLogging = false;
    const char *server = NULL;
    int port = -1, numSecs = 10;

    while ((optchar = getopt(argc, argv, "hcmn:p:s:tv")) != -1) {
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
            default:
                help = true;
                break;
        }
    }

    help = help || (!meta && !chunk);

    if (help || (server == NULL) || (port < 0)) {
        cout << "Usage: " << argv[0] << " [-m|-c] -s <server name> -p <port>"
             << " [-n <secs>] [-t] [-v]" << endl
             << "        Use -m for metaserver, -c for chunk server." << endl
             << "        Use -t for RPC stats." << endl
             << "        Use -n <seconds> to specify interval (default 10s)."
             << endl;
        return -1;
    }

    MsgLogger::Init(0, verboseLogging ?
        MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelINFO);

    ServerLocation location(server, port);

    if (meta)
        StatsMetaServer(location, rpcStats, numSecs);
    else if (chunk)
        StatsChunkServer(location, rpcStats, numSecs);
    return 0;
}

static void
PrintRpcStat(const string &statName, Properties &prop)
{
    // cout << statName << " = " << prop.getValue(statName, (long
    // long) 0) << endl;
    cout << statName << " = " << prop.getValue(statName, "0") << endl;
}


void
StatsMetaServer(const ServerLocation &location, bool rpcStats, int numSecs)
{
    TcpSocket metaServerSock;

    if (metaServerSock.Connect(location) < 0) {
        KFS_LOG_STREAM_ERROR <<
            "Unable to connect to " << location.ToString() <<
        KFS_LOG_EOM;
        exit(0);
    }

    if (rpcStats) {
        RpcStatsMetaServer(metaServerSock, numSecs);
    } else {
        BasicStatsMetaServer(metaServerSock, numSecs);
    }
    metaServerSock.Close();
}

void
RpcStatsMetaServer(TcpSocket &metaServerSock, int numSecs)
{
    int numIO;
    int cmdSeqNum = 1;

    while (1) {
        MetaStatsOp op(cmdSeqNum);
        ++cmdSeqNum;
        numIO = DoOpCommon(&op, &metaServerSock);
        if (numIO < 0) {
            KFS_LOG_STREAM_ERROR << "Server isn't responding to stats" <<
            KFS_LOG_EOM;
            exit(0);
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

        cout << "----------------------------------" << endl;
        if (numSecs == 0)
            break;
        sleep(numSecs);
    }
}

void
BasicStatsMetaServer(TcpSocket &metaServerSock, int numSecs)
{
    int numIO;
    int cmdSeqNum = 1;

    PrintMetaBasicStatsHeader();
    for (; ;) {
        MetaStatsOp op(cmdSeqNum);
        ++cmdSeqNum;
        numIO = DoOpCommon(&op, &metaServerSock);
        if (numIO < 0) {
            KFS_LOG_STREAM_ERROR <<
                "Server isn't responding to stats" <<
            KFS_LOG_EOM;
            exit(0);
        }
        // useful things to have: # of connections handled
        if (cmdSeqNum % 10 == 0) {
            PrintMetaBasicStatsHeader();
        }
        cout << op.stats.getValue("Open network fds", (long long) 0) << '\t';
        cout << op.stats.getValue("Bytes read from network", (long long) 0) << '\t';
        cout << op.stats.getValue("Bytes written to network", (long long) 0) << endl;

        if (numSecs == 0) {
            break;
        }
        sleep(numSecs);
    }
}

static void
PrintMetaBasicStatsHeader()
{
    cout << "Net Fds" << '\t' << "N/w Bytes In" << '\t'
         << "N/w Bytes Out" << endl;
}

void
StatsChunkServer(const ServerLocation &location, bool rpcStats, int numSecs)
{
    TcpSocket chunkServerSock;

    if (chunkServerSock.Connect(location) < 0) {
        KFS_LOG_STREAM_ERROR <<
            "Unable to connect to " << location.ToString() <<
        KFS_LOG_EOM;
        exit(0);
    }

    if (rpcStats) {
        RpcStatsChunkServer(chunkServerSock, numSecs);
    } else {
        BasicStatsChunkServer(chunkServerSock, numSecs);
    }

    chunkServerSock.Close();
}

void
RpcStatsChunkServer(TcpSocket &chunkServerSock, int numSecs)
{
    int numIO;
    int cmdSeqNum = 1;

    while (1) {
        ChunkStatsOp op(cmdSeqNum);
        ++cmdSeqNum;
        numIO = DoOpCommon(&op, &chunkServerSock);
        if (numIO < 0) {
            KFS_LOG_STREAM_ERROR << "Server isn't responding to stats" <<
            KFS_LOG_EOM;
            exit(0);
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
        cout << "----------------------------------" << endl;
        if (numSecs == 0)
            break;
        sleep(numSecs);
    }
}

void
BasicStatsChunkServer(TcpSocket &chunkServerSock, int numSecs)
{
    int numIO;
    int cmdSeqNum = 1;

    PrintChunkBasicStatsHeader();
    while (1) {
        ChunkStatsOp op(cmdSeqNum);
        ++cmdSeqNum;
        numIO = DoOpCommon(&op, &chunkServerSock);
        if (numIO < 0) {
            KFS_LOG_STREAM_ERROR << "Server isn't responding to stats" <<
            KFS_LOG_EOM;
            exit(0);
        }

        if (cmdSeqNum % 10 == 0)
            PrintChunkBasicStatsHeader();

        cout << op.stats.getValue("Open network fds", (long long) 0) << '\t';
        cout << op.stats.getValue("Bytes read from network", (long long) 0) << '\t';
        cout << op.stats.getValue("Bytes written to network", (long long) 0) << '\t';
        cout << op.stats.getValue("Open disk fds", (long long) 0) << '\t';
        cout << op.stats.getValue("Bytes read from disk", (long long) 0) << '\t';
        cout << op.stats.getValue("Bytes written to disk", (long long) 0) << endl;

        if (numSecs == 0)
            break;

        sleep(numSecs);
    }
}

static void
PrintChunkBasicStatsHeader()
{
    cout << "Net Fds" << '\t' << "N/w Bytes In" << '\t'
         << "N/w Bytes Out" << '\t'
         << "Disk Fds" << '\t' << "Disk Bytes In" << '\t'
         << "Disk Bytes Out" << endl;
}
