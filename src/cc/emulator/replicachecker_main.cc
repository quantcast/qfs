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
    string logdir("kfslog");
    string cpdir("kfscp");
    string networkFn("network.def");
    string chunkmapFn("chunkmap.txt");
    string fsckFn("-");
    string propsFn;
    int    optchar;
    bool   helpFlag      = false;
    bool   reportAllFlag = false;
    bool   verboseFlag   = false;

    while ((optchar = getopt(argc, argv, "avc:l:n:b:r:hf:p:")) != -1) {
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
            "[-n <network definition file name> (default " <<
                networkFn << ")]\n"
            "[-b <chunkmap file> (default " << chunkmapFn << ")]\n"
            "[-p <[meta server] configuration file> (default none)]\n"
            "[-f <fsck output file name> (- stdout) (default " <<
                fsckFn << ")]\n"
            "[-v verbose replica check output]\n"
            "[-a report all placement problems]\n"
        ;
        return 1;
    }

    MdStream::Init();
    MsgLogger::Init(0, MsgLogger::kLogLevelINFO);
    Properties props;
    int fsckStatus = 0;
    int status     = 0;
    if (propsFn.empty() ||
            (status = props.loadProperties(propsFn.c_str(), char('=')))
            == 0) {
        gLayoutEmulator.SetParameters(props);
        if ((status = EmulatorSetup(logdir, cpdir, networkFn, chunkmapFn)) ==
                0) {
            if (! fsckFn.empty()) {
                fsckStatus = gLayoutEmulator.RunFsck(fsckFn);
            }
            status = gLayoutEmulator.VerifyRackAwareReplication(
                        reportAllFlag, verboseFlag, cout);
        }
    }
    AuditLog::Stop();
    MdStream::Cleanup();
    return (status ? 1 : (fsckStatus == 0 ? 0 : 1));
}
