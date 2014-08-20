//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/08/27
//
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
// \brief Driver program to run the meta server on-line re-balancing off-line
// and create re-balance plan for moving / re-assigning chunks  (fs blocks).
// The plan can be executed by the meta server or layout emulator, or can be
// used for testing or debugging meta server re-balancing logic off-line.
//
//----------------------------------------------------------------------------

#include "LayoutEmulator.h"
#include "emulator_setup.h"

#include "common/MsgLogger.h"
#include "common/Properties.h"
#include "common/MdStream.h"
#include "qcdio/QCUtils.h"
#include "meta/AuditLog.h"

#include <unistd.h>
#include <signal.h>

using std::string;
using std::cout;
using std::cerr;

using namespace KFS;

static void HandleStop(int) { gLayoutEmulator.Stop(); }

int
main(int argc, char** argv)
{
    string  rebalancePlanFn("rebalanceplan.txt");
    string  logdir("kfslog");
    string  cpdir("kfscp");
    string  networkFn("network.def");
    string  chunkmapFn("chunkmap.txt");
    string  propsFn;
    string  chunkMapDir;
    int     optchar;
    int16_t minReplication   = -1;
    double  variationFromAvg = 0;
    bool    helpFlag         = false;
    bool    debugFlag        = false;

    while ((optchar = getopt(argc, argv, "c:l:n:b:r:hp:o:dm:t:")) != -1) {
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
            case 't':
                variationFromAvg = atof(optarg);
                break;
            case 'p':
                propsFn = optarg;
                break;
            case 'o':
                chunkMapDir = optarg;
                break;
            case 'd':
                debugFlag = true;
                break;
            case 'm':
                minReplication = atoi(optarg);
                break;
            default:
                cerr << "Unrecognized flag: " << (char)optchar << "\n";
                helpFlag = true;
                break;
        }
    }
    if (helpFlag || rebalancePlanFn.empty()) {
        cout <<
        "Usage: " << argv[0] << "\n"
            "[-l <log directory> (default " << logdir << ")]\n"
            "[-c <checkpoint directory> (default " << cpdir << ")]\n"
            "[-n <network definition file name> (default " <<
                networkFn << ")]\n"
            "[-b <chunkmap file> (default " << chunkmapFn << ")]\n"
            "[-r <re-balance plan file>] (default" << rebalancePlanFn << ")\n"
            "[-t <% variation from average utilization> (default " <<
                variationFromAvg << "%)"
                " 0 - use default / configured re-balance thresholds]\n"
            "[-p <[meta server] configuration file> (default none)]\n"
            "[-o <new chunk map output directory> (default none)]\n"
            "[-d debug -- print chunk layout before and after]\n"
            "[-m <min replicas per file> (default -1 -- no change)]\n"
            "To create network defininiton file and chunk map files:\n"
            "telnet to the meta server, and issue DUMP_CHUNKTOSERVERMAP\n"
            "followed by an empty line.\n"
            "Copy produced chunkmap.txt and network.def files as well as,\n"
            "latest checkpoint and transacton logs.\n"
            "The closer in time the checkpoint and logs to the chumk map\n"
            "the better -- less descrepancies due to fs modifications.\n"
        ;
        return 1;
    }

    MdStream::Init();
    MsgLogger::Init(0, MsgLogger::kLogLevelINFO);

    if (signal(SIGINT, &HandleStop) == SIG_ERR) {
        KFS_LOG_STREAM_ERROR <<
            QCUtils::SysError(errno, "signal(SIGINT):") <<
        KFS_LOG_EOM;
        return 1;
    }
    if (signal(SIGQUIT, &HandleStop) == SIG_ERR) {
        KFS_LOG_STREAM_ERROR <<
            QCUtils::SysError(errno, "signal(SIGQUIT):") <<
        KFS_LOG_EOM;
        return 1;
    }

    Properties props;
    int status = 0;
    if (propsFn.empty() ||
            (status = props.loadProperties(propsFn.c_str(), char('=')))
            == 0) {
        gLayoutEmulator.SetParameters(props);
        gLayoutEmulator.SetupForRebalancePlanning(variationFromAvg);
        status = EmulatorSetup(logdir, cpdir, networkFn, chunkmapFn,
            minReplication, minReplication > 1);
        if (status == 0 &&
                (status = gLayoutEmulator.SetRebalancePlanOutFile(
                    rebalancePlanFn)) == 0) {
            if (debugFlag) {
                gLayoutEmulator.PrintChunkserverBlockCount(cout);
            }
            KFS_LOG_STREAM_NOTICE << "creating re-balance plan: " <<
                rebalancePlanFn <<
            KFS_LOG_EOM;
            gLayoutEmulator.BuildRebalancePlan();
            if (! chunkMapDir.empty()) {
                gLayoutEmulator.DumpChunkToServerMap(chunkMapDir);
            }
            if (debugFlag) {
                gLayoutEmulator.PrintChunkserverBlockCount(cout);
            }
            KFS_LOG_STREAM_NOTICE << "replicated chunks: " <<
                gLayoutEmulator.GetNumBlksRebalanced() <<
            KFS_LOG_EOM;
        }
    }
    AuditLog::Stop();
    MdStream::Cleanup();
    return (status == 0 ? 0 : 1);
}

