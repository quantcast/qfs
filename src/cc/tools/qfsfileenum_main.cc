//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/05/05
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
// \brief Debug utility to list out where the chunks of a file are.
//----------------------------------------------------------------------------

#include "common/MsgLogger.h"
#include "libclient/KfsClient.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

#include <iostream>
#include <iomanip>
#include <string>

using namespace KFS;

using std::string;
using std::cout;
using std::setw;

int
main(int argc, char **argv)
{
    bool        help               = false;
    bool        showErrorsOnlyFlag = false;
    bool        showFileNameFlag   = true;
    const char* server             = 0;
    const char* filename           = 0;
    const char* config             = 0;
    int         port               = -1;
    bool        verboseLogging     = false;
    int         ret                = 0;
    int         optchar;

    while ((optchar = getopt(argc, argv, "hs:p:f:vc:e")) != -1) {
        switch (optchar) {
            case 's':
                server = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'f':
                filename         = optarg;
                showFileNameFlag = false;
                break;
            case 'h':
                help = true;
                break;
            case 'v':
                verboseLogging = true;
                break;
            case 'c':
                config = optarg;
                break;
            case 'e':
                showErrorsOnlyFlag = true;
                break;
            default:
                help = true;
                break;
        }
    }

    if (help || ! server || port < 0) {
        cout << "Usage: " << argv[0] <<
            " -s <server name> -p <port> [-f <path>] [-v] [-c <config file>]"
            " [-e] <path> ...\n"
            "Enumerate blocks and sizes of the files.\n"
            " -s              -- meta server name or ip\n"
            " -p              -- meta server port\n"
            " [-f <path>]     -- enumerate blocks for the given file path\n"
            "                    and don't display the filename in output\n"
            " [-v]            -- verbose debug trace\n"
            " [-c <cfg-file>] -- use given configuration file\n"
            " [-e]            -- only show missing blocks or blocks with"
                " invalid size\n"
            " <path> ...      -- one or more file paths to enumerate\n";
        return 1;
    }

    MsgLogger::Init(0, verboseLogging ?
        MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelINFO);

    KfsClient* const kfsClient = KfsClient::Connect(server, port, config);
    if (! kfsClient) {
        cout << "qfs client failed to initialize\n";
        return 1;
    }
    int i = optind;
    if (! filename && i < argc) {
        filename = argv[i++];
    }
    string msg;
    while (filename) {
        KfsClient::BlockInfos infos;
        int status = kfsClient->EnumerateBlocks(filename, infos);
        if (status < 0) {
            cout << filename << ": " << ErrorCodeToStr(status) << "\n";
        } else {
            int64_t space = 0;
            if (! showErrorsOnlyFlag) {
                if (showFileNameFlag) {
                    cout << filename << "\n";
                }
                cout << "block count: " << infos.size() << " blocks:\n";
            }
            const KfsClient::BlockInfo* pbi = 0;
            for (KfsClient::BlockInfos::const_iterator it = infos.begin();
                    it != infos.end();
                    ++it) {
                const KfsClient::BlockInfo& bi = *it;
                msg.clear();
                if (pbi && pbi->id == bi.id &&
                        (0 <= bi.version || bi.version == pbi->version)) {
                    if (pbi->offset != bi.offset) {
                        msg += " *position mismatch*";
                        status = -EINVAL;
                    }
                    if (pbi->version != bi.version) {
                        msg += " *version mismatch*";
                        status = -EINVAL;
                    }
                    if (pbi->size != bi.size) {
                        msg += " *size mismatch*";
                        status = -EINVAL;
                    }
                }
                if (! showErrorsOnlyFlag || bi.size <= 0 || ! msg.empty()) {
                    if (showErrorsOnlyFlag) {
                        cout << filename << ": ";
                        if (bi.size <= 0) {
                            msg = " *missing or invalid size";
                        }
                    }
                    cout <<
                        "position: "  << setw(10) << bi.offset  << setw(0) <<
                        " id: "       << setw(10) << bi.id      << setw(0) <<
                        " version: "  << setw(3)  << bi.version << setw(0) <<
                        " size: "     << setw(8)  << bi.size    << setw(0) <<
                        " location: " << setw(12) << bi.server  << setw(0) <<
                        msg << "\n";
                    ;
                }
                pbi = &bi;
                if (bi.size > 0) {
                    space += bi.size;
                }
            }
            if (! showErrorsOnlyFlag) {
                cout << "total space: " << space << "\n";
            }
        }
        if (0 != status) {
            ret = 1;
        }
        filename = i < argc ? argv[i] : 0;
        i++;
    }
    delete kfsClient;
    return ret;
}


