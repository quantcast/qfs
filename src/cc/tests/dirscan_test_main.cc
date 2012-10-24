//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/07/16
//
// Copyright 2008-2011 Quantcast Corporation.
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
// \brief Test that evaluates readdirplus() followed by calls to get attributes.
//----------------------------------------------------------------------------

#include <iostream>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fstream>
#include "libclient/KfsClient.h"
#include "common/MsgLogger.h"

using std::cout;
using std::endl;
using std::ifstream;
using std::vector;
using std::string;
using namespace KFS;

KfsClient * gKfsClient;

void dirListPlusAttr(const string &kfspathname);

int
main(int argc, char **argv)
{
    char optchar;
    const char *metaserver = NULL;
    int port = -1;
    string kfspathname = "";
    bool help = false;

    while ((optchar = getopt(argc, argv, "m:p:d:")) != -1) {
        switch (optchar) {
            case 'm':
                metaserver = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'd':
                kfspathname = optarg;
                break;
            default:
                cout << "Unrecognized flag: " << optchar << endl;
                help = true;
                break;
        }
    }

    if (help || !metaserver || (port < 0)) {
        cout << "Usage: " << argv[0] << " -m <metaserver host> -p <port> -d <path>" << "\n";
        return -1;
    }

    KFS::MsgLogger::Init(0);
    gKfsClient = Connect(metaserver, port);
    if (!gKfsClient) {
        cout << "KFS client failed to initialize...exiting" << endl;
        exit(-1);
    }
    dirListPlusAttr(kfspathname);
    return 0;
}

void
dirListPlusAttr(const string &kfspathname)
{
    if (gKfsClient->IsFile(kfspathname.c_str())) {
        cout << kfspathname << " is a file..." << endl;
        return;
    }

    vector<KfsFileAttr> fattrs;

    if (gKfsClient->ReaddirPlus(kfspathname.c_str(), fattrs) < 0) {
        cout << "unable to do readdirplus on " << kfspathname << endl;
        return;
    }

    KFS_LOG_STREAM_INFO <<
        "Done getting readdirplus on " << kfspathname <<
        " (" << fattrs.size() << " entries)" <<
    KFS_LOG_EOM;

    uint32_t replicas = 0;
    for (uint32_t i = 0; i < fattrs.size(); i++) {
        string abspath = kfspathname + "/" + fattrs[i].filename;

        if (gKfsClient->IsFile(abspath.c_str()))
            replicas = gKfsClient->GetReplicationFactor(abspath.c_str());
    }

    KFS_LOG_STREAM_INFO <<
        "Done getting replication factor for all entries on " << kfspathname <<
        " total replicas: " << replicas <<
    KFS_LOG_EOM;

    KfsFileAttr attr;
    uint64_t dirsz = 0;

    for (uint32_t i = 0; i < fattrs.size(); i++) {
        string abspath = kfspathname + "/" + fattrs[i].filename;

        if (gKfsClient->IsFile(abspath.c_str())) {
            int res = gKfsClient->Stat(abspath.c_str(), attr);
            if (res == 0)
                dirsz += attr.fileSize;
        }
    }

    KFS_LOG_STREAM_INFO << "Dirsize on " << kfspathname << ": " << dirsz <<
    KFS_LOG_EOM;
}
