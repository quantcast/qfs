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

#include "kfsio/SslFilter.h"

#include "qcdio/QCUtils.h"

#include "meta/AuditLog.h"

#include <unistd.h>
#include <signal.h>

using std::string;
using std::cout;
using std::cerr;

using namespace KFS;

static void HandleStop(int) { LayoutEmulator::Instance().Stop(); }

int
main(int argc, char** argv)
{
    string              rebalancePlanFn("rebalanceplan.txt");
    string              logdir("kfslog");
    string              cpdir("kfscp");
    string              networkFn;
    string              chunkmapFn("chunkmap.txt");
    string              propsFn;
    string              chunkMapDir;
    int                 optchar;
    int64_t             chunkServerTotalSpace = -1;
    int16_t             minReplication        = -1;
    double              variationFromAvg      = 0;
    bool                helpFlag              = false;
    bool                debugFlag             = false;
    MsgLogger::LogLevel logLevel              = MsgLogger::kLogLevelINFO;

    while ((optchar = getopt(argc, argv, "c:l:n:b:r:hp:o:dm:t:S:L:")) != -1) {
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
            case 'S':
                chunkServerTotalSpace = (int64_t)atof(optarg);
                break;
            case 'L':
                logLevel = MsgLogger::GetLogLevelId(optarg);
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
            "[-n <network definition file name> (default none, i.e. empty)"
                " without definition file chunk servers and chunk map from"
                " checkpoint transaction log replay are used]\n"
            "[-b <chunkmap file> (default " << chunkmapFn << ")]\n"
            "[-r <re-balance plan file>] (default" << rebalancePlanFn << ")\n"
            "[-t <% variation from average utilization> (default " <<
                variationFromAvg << "%)"
                " 0 - use default / configured re-balance thresholds]\n"
            "[-p <[meta server configuration file> (default none)]\n"
            "[-o <new chunk map output directory> (default none)]\n"
            "[-d debug -- print chunk layout before and after]\n"
            "[-m <min replicas per file> (default -1 -- no change)]\n"
            "[-L DEBUG}INFO|NOTICE|ERROR|FATAL -- set log level]\n"
            "[-S <num> -- chunk server total space (default is -1)"
                " this value has effect without network definition file"
                " if set to negative value, then number of chunks multiplied"
                " by max. chunk size (64M) divided by the number of chunk servers"
                " plus 20% is used]\n"
            "To create network defininiton file and chunk map files:\n"
            "use qfsadmin to issue dump_chunktoservermap\n"
            "Copy produced chunkmap.txt and network.def files as well as,\n"
            "latest checkpoint and transacton logs.\n"
            "The closer in time the checkpoint and logs to the chumk map\n"
            "the better -- less descrepancies due to fs modifications.\n"
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
    MsgLogger::Init(0, logLevel);

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

    LayoutEmulator& emulator = LayoutEmulator::Instance();
    Properties props;
    int status = 0;
    if (! propsFn.empty() &&
            0 != (status = props.loadProperties(propsFn.c_str(), char('=')))) {
        KFS_LOG_STREAM_FATAL <<
            propsFn << ": " << QCUtils::SysError(-status) <<
        KFS_LOG_EOM;
    } else {
        emulator.SetParameters(props);
        emulator.SetupForRebalancePlanning(variationFromAvg);
        status = EmulatorSetup(emulator, logdir, cpdir, networkFn, chunkmapFn,
            minReplication, minReplication > 1, chunkServerTotalSpace);
        if (status == 0 &&
                (status = emulator.SetRebalancePlanOutFile(
                    rebalancePlanFn)) == 0) {
            if (debugFlag) {
                cout.flush();
                emulator.RunFsck(string());
                cout << "==================================================\n";
                emulator.PrintChunkserverBlockCount(cout);
                cout << "==================================================\n";
            }
            KFS_LOG_STREAM_NOTICE << "creating re-balance plan: " <<
                rebalancePlanFn <<
            KFS_LOG_EOM;
            emulator.BuildRebalancePlan();
            if (! chunkMapDir.empty()) {
                emulator.DumpChunkToServerMap(chunkMapDir);
            }
            if (debugFlag) {
                emulator.PrintChunkserverBlockCount(cout);
                cout << "==================================================\n";
                cout.flush();
                emulator.RunFsck(string());
            }
            KFS_LOG_STREAM_NOTICE << "replicated chunks: " <<
                emulator.GetNumBlksRebalanced() <<
            KFS_LOG_EOM;
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

