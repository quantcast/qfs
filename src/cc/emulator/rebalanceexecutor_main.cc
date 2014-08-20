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
#include "meta/AuditLog.h"

#include <unistd.h>

using std::string;
using std::cout;

using namespace KFS;

int
main(int argc, char** argv)
{
    string rebalancePlanFn("rebalanceplan.txt");
    string logdir("kfslog");
    string cpdir("kfscp");
    string networkFn("network.def");
    string chunkmapFn("chunkmap.txt");
    string propsFn;
    string chunkMapDir;
    int    optchar;
    bool   helpFlag  = false;
    bool   debugFlag = false;

    while ((optchar = getopt(argc, argv, "c:l:n:b:r:hdp:o:")) != -1) {
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
            default:
                helpFlag = true;
                break;
        }
    }

    if (helpFlag || rebalancePlanFn.empty()) {
        cout << "Usage: " << argv[0] << "\n"
            "[-l <log directory> (default " << logdir << ")]\n"
            "[-c <checkpoint directory> (default " << cpdir << ")]\n"
            "[-n <network definition file name> (default " <<
                networkFn << ")]\n"
            "[-b <chunkmap file> (default " << chunkmapFn << ")]\n"
            "[-r <re-balance plan file>] (default" << rebalancePlanFn << ")\n"
                " 0 - use default / configured re-balance thresholds]\n"
            "[-p <[meta server] configuration file> (default none)]\n"
            "[-o <new chunk map output directory> (default none)]\n"
            "[-d debug -- print chunk into stdout layout before and after]\n"
        ;
        return 1;
    }

    MdStream::Init();
    MsgLogger::Init(0, MsgLogger::kLogLevelINFO);

    Properties props;
    int status = 0;
    if ((propsFn.empty() ||
            (status = props.loadProperties(propsFn.c_str(), char('=')))
                == 0)) {
        gLayoutEmulator.SetParameters(props);
        if ((status = EmulatorSetup(logdir, cpdir, networkFn, chunkmapFn))
                == 0 &&
                (status = gLayoutEmulator.LoadRebalancePlan(rebalancePlanFn))
                == 0) {
            if (debugFlag) {
                gLayoutEmulator.PrintChunkserverBlockCount(cout);
            }
            gLayoutEmulator.ExecuteRebalancePlan();
            if (! chunkMapDir.empty()) {
                gLayoutEmulator.DumpChunkToServerMap(chunkMapDir);
            }
            if (debugFlag) {
                gLayoutEmulator.PrintChunkserverBlockCount(cout);
            }
        }
    }

    AuditLog::Stop();
    MdStream::Cleanup();

    return (status == 0 ? 0 : 1);
}

