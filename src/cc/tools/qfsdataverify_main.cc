//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/06/11
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
// \brief After we push data to KFS, we need to verify that the data
// pushed matches the source.  This tool does such a verification by
// computing checksums on the data in the way KFS-chunkserver does and
// then pulls the checksums from the (3) chunkservers and compares them
// all.  When all values agree, we know that the data we wrote to KFS
// matches what is in the source.
//
//----------------------------------------------------------------------------

#include "common/MsgLogger.h"
#include "libclient/KfsClient.h"

#include <string>
#include <iostream>
#include <unistd.h>

using std::cout;
using std::cerr;
using std::string;

using namespace KFS;

int
main(int argc, char **argv)
{
    int         optchar;
    bool        help           = false;
    int         port           = -1;
    const char* metaserver     = 0;
    const char* kfsFilename    = 0;
    bool        checkCksums    = false;
    bool        verboseLogging = false;
    bool        checkReplicas  = false;
    const char* config         = 0;

    while ((optchar = getopt(argc, argv, "s:p:k:chdvf:")) != -1) {
        switch (optchar) {
            case 's':
                metaserver = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'k':
                kfsFilename = optarg;
                break;
            case 'd':
                checkReplicas = true;
                break;
            case 'c':
                checkCksums = true;
                break;
            case 'v':
                verboseLogging = true;
                break;
            case 'f':
                config = optarg;
                break;
            case 'h':
            default:
                help = true;
                break;
        }
    }

    help = help || (!metaserver) || (port < 0) || !kfsFilename;

    if (help) {
        cout << "Usage: " << argv[0] <<
            " -s <metaserver> -p <port> -k <QFSfile> [-c|-d] [-v]"
            " [-f <config file name>]\n"
            " -c: compare checksums on the replicas.\n"
            " -d: compare the chunks and return md5 of the file.\n";
        return -1;
    }

    MsgLogger::Init(0, verboseLogging ?
        MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelINFO);

    KfsClient* const kfsClient = KfsClient::Connect(metaserver, port, config);
    if (! kfsClient) {
        cout << "qfs client failed to initialize\n";
        return 1;
    }

    int ret = 0;
    if (checkReplicas) {
        string md5sum;
        ret = kfsClient->CompareChunkReplicas(kfsFilename, md5sum);
        if (ret < 0) {
           cerr << kfsFilename << ": " << ErrorCodeToStr(ret) << "\n";
        } else if (ret == 0) {
            cout << md5sum << "\n";
            cerr << kfsFilename << ": all chunk replicas are identical\n";
        } else {
            cerr << kfsFilename << ": chunk replicas are not identical.\n";
        }
    } else if (checkCksums) {
        ret = kfsClient->VerifyDataChecksums(kfsFilename);
        if (ret < 0) {
           cerr << kfsFilename << ": " << ErrorCodeToStr(ret) << "\n";
        } else if (ret == 0) {
            cerr << kfsFilename <<
                ": all chunk replicas have identical checksums.\n";
        } else {
            cerr << kfsFilename <<
                ": chunk replicas checksums are not identical.\n";
        }
    }
    delete kfsClient;
    return (ret == 0 ? 0 : 1);
}


