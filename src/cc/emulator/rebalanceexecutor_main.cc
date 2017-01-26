//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/08/27
//
// Author: Sriram Rao
//
// Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
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
// \brief Driver program to run the meta server in emulator mode and
// executes the plan for re-balancing blocks.
// Might be used for "off-line" debugging meta server layout emulation code,
// and / or the re-balance plan / off-line re-balancer.
//
//----------------------------------------------------------------------------

#include "LayoutEmulator.h"
#include "emulator_setup.h"

#include "common/MsgLogger.h"
#include "common/Properties.h"
#include "common/MdStream.h"

#include "kfsio/SslFilter.h"

#include "meta/AuditLog.h"

#include <unistd.h>
#include <stdlib.h>

using std::string;
using std::cout;
using std::cerr;

using namespace KFS;

int
main(int argc, char** argv)
{
    string  rebalancePlanFn("rebalanceplan.txt");
    string  logdir("kfslog");
    string  cpdir("kfscp");
    string  networkFn;
    string  chunkmapFn("chunkmap.txt");
    string  propsFn;
    string  chunkMapDir;
    int64_t chunkServerTotalSpace = -1;
    int     optchar;
    bool    helpFlag  = false;
    bool    debugFlag = false;

    while ((optchar = getopt(argc, argv, "c:l:n:b:r:hdp:o:S:")) != -1) {
        switch (optchar) {
            case 'l':
                logdir = optarg;
                break;
            case 'c':
                cpdir = optarg;
                break;
            case 'n':
                networkFn = optarg;
                break;
            case 'b':
                chunkmapFn = optarg;
                break;
            case 'r':
                rebalancePlanFn = optarg;
                break;
            case 'h':
                helpFlag = true;
                break;
            case 'd':
                debugFlag = true;
                break;
            case 'p':
                propsFn = optarg;
                break;
            case 'o':
                chunkMapDir = optarg;
                break;
            case 'S':
                chunkServerTotalSpace = (int64_t)atof(optarg);
                break;
            default:
                helpFlag = true;
                break;
        }
    }

    if (helpFlag || rebalancePlanFn.empty()) {
        cout << "Usage: " << argv[0] << "\n"
            "[-l <log directory> (default " << logdir << ")]\n"
            "[-c <checkpoint directory> (default " << cpdir << ")]\n"
            "[-n <network definition file name> (default none, i.e. empty)"
                " without definition file chunk servers and chunk map from"
                " checkpoint transaction log replay are used]\n"
            "[-b <chunkmap file> (default " << chunkmapFn << ")]\n"
            "[-r <re-balance plan file> (default" << rebalancePlanFn << ")]\n"
            "[-p <configuration file> (default none)]\n"
            "[-o <new chunk map output directory> (default none)]\n"
            "[-d debug -- print chunk into stdout layout before and after]\n"
            "[-S <num> -- chunk server total space (default is -1)"
                " this value has effect without network definition file"
                " if set to negative value, then number of chunks multiplied"
                " by max. chunk size (64M) divided by the number of chunk servers"
                " plus 20% is used]\n"
        ;
        return 1;
    }

    MdStream::Init();
    SslFilter::Error sslErr = SslFilter::Initialize();
    if (sslErr) {
        cerr << "failed to initialize ssl: " <<
            " error: " << sslErr <<
            " " << SslFilter::GetErrorMsg(sslErr) << "\n";
        return 1;
    }
    MsgLogger::Init(0, MsgLogger::kLogLevelINFO);

    LayoutEmulator& emulator = LayoutEmulator::Instance();
    int             status = 0;
    Properties      props;
    if ((propsFn.empty() ||
            (status = props.loadProperties(propsFn.c_str(), char('=')))
                == 0)) {
        emulator.SetParameters(props);
        if ((status = EmulatorSetup(
                emulator, logdir, cpdir, networkFn, chunkmapFn,
                -1, false, chunkServerTotalSpace)) == 0 &&
                (status = emulator.LoadRebalancePlan(rebalancePlanFn))
                == 0) {
            if (debugFlag) {
                emulator.PrintChunkserverBlockCount(cout);
            }
            emulator.ExecuteRebalancePlan();
            if (! chunkMapDir.empty()) {
                emulator.DumpChunkToServerMap(chunkMapDir);
            }
            if (debugFlag) {
                emulator.PrintChunkserverBlockCount(cout);
            }
        }
    }

    AuditLog::Stop();
    sslErr = SslFilter::Cleanup();
    if (sslErr) {
        KFS_LOG_STREAM_ERROR << "failed to cleanup ssl: " <<
            " error: " << sslErr <<
            " " << SslFilter::GetErrorMsg(sslErr) <<
        KFS_LOG_EOM;
    }
    MsgLogger::Stop();
    MdStream::Cleanup();

    return (status == 0 ? 0 : 1);
}

