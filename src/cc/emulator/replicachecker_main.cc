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
// \brief Read in a network map, a block location map and verify that
// the three copies of a replica are on three different racks and server, and
// report chunk placement problems.
//
//----------------------------------------------------------------------------

#include "LayoutEmulator.h"
#include "emulator_setup.h"

#include "common/MdStream.h"
#include "common/MsgLogger.h"
#include "common/Properties.h"
#include "meta/AuditLog.h"

using std::string;
using std::cout;
using std::cerr;

using namespace KFS;

int
main(int argc, char** argv)
{
    string  logdir("kfslog");
    string  cpdir("kfscp");
    string  networkFn;
    string  chunkmapFn("chunkmap.txt");
    string  fsckFn("-");
    string  propsFn;
    int64_t chunkServerTotalSpace = -1;
    int     optchar;
    bool    helpFlag      = false;
    bool    reportAllFlag = false;
    bool    verboseFlag   = false;

    while ((optchar = getopt(argc, argv, "avc:l:n:b:r:hf:p:S:")) != -1) {
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
            case 'h':
                helpFlag = true;
                break;
            case 'a':
                reportAllFlag = true;
                break;
            case 'f':
                fsckFn = optarg;
                break;
            case 'v':
                verboseFlag = true;
                break;
            case 'p':
                propsFn = optarg;
                break;
            case 'S':
                chunkServerTotalSpace = (int64_t)atof(optarg);
                break;
            default:
                cerr << "Unrecognized flag " << (char)optchar << "\n";
                helpFlag = true;
                break;
        }
    }

    if (helpFlag) {
        cout << "Usage: " << argv[0] << "\n"
            "[-l <log directory> (default " << logdir << ")]\n"
            "[-c <checkpoint directory> (default " << cpdir << ")]\n"
            "[-n <network definition file name> (default none, i.e. empty)"
                " without definition file chunk servers and chunk map from"
                " checkpoint transaction log replay are used]\n"
            "[-b <chunkmap file> (default " << chunkmapFn << ")]\n"
            "[-p <[meta server] configuration file> (default none)]\n"
            "[-f <fsck output file name> (- stdout) (default " <<
                fsckFn << ")]\n"
            "[-v verbose replica check output]\n"
            "[-a report all placement problems]\n"
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
    LayoutEmulator& emulator   = LayoutEmulator::Instance();
    int             fsckStatus = 0;
    int             status     = 0;
    Properties      props;
    if (propsFn.empty() ||
            (status = props.loadProperties(propsFn.c_str(), char('=')))
            == 0) {
        emulator.SetParameters(props);
        if ((status = EmulatorSetup(
                emulator, logdir, cpdir, networkFn, chunkmapFn,
                -1, false, chunkServerTotalSpace)) == 0) {
            if (! fsckFn.empty()) {
                fsckStatus = emulator.RunFsck(fsckFn);
            }
            status = emulator.VerifyRackAwareReplication(
                        reportAllFlag, verboseFlag, cout);
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
    return (status ? 1 : (fsckStatus == 0 ? 0 : 1));
}
