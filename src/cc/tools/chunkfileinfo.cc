//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2011/06/14
// Author: Mike Ovsiannikov
//
// Copyright 2011-2012 Quantcast Corp.
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
// \brief Implements fileInfo() and chunkInfo() functions that print out
//        attributes.
//
//----------------------------------------------------------------------------

#include "libclient/KfsClient.h"
#include "tools/kfsshell.h"

#include <stdlib.h>

#include <iostream>
#include <fstream>

namespace KFS
{
namespace tools
{

using std::cout;
using std::cerr;
using std::vector;
using std::string;

static void
printInfo(const KfsFileAttr&          attr,
        chunkOff_t                    offset,
        int64_t                       chunkVersion,
        const vector<ServerLocation>& servers)
{
    if (offset >= 0) {
        cout <<
        "Offset:           " << offset << "\n"
        "Version:          " << chunkVersion << "\n"
        "Servers:          " << servers.size() << "\n";
        for (vector<ServerLocation>::const_iterator it = servers.begin();
                it != servers.end();
                ++it) {
            cout << it->ToString() << "\n";
        }
    }
    cout <<
        "File:             " << attr.filename << "\n"
        "ctime:            " << attr.ctime.tv_sec << "\n"
        "mtime:            " << attr.mtime.tv_sec << "\n"
        "Size:             " << attr.fileSize << "\n"
        "Id:               " << attr.fileId << "\n"
    ;
    if (attr.isDirectory && attr.dirCount() >= 0) {
        cout <<
            "Files:           " << attr.fileCount() << "\n"
            "Dirs:            " << attr.dirCount() << "\n"
        ;
    }
    if (! attr.isDirectory) {
        cout <<
            "Replication:      " << attr.numReplicas << "\n"
            "Chunks:           " << attr.chunkCount() << "\n"
        ;
    }
    if (attr.striperType == KFS_STRIPED_FILE_TYPE_NONE) {
        return;
    }
    cout <<
        "Stripe size:      " << attr.stripeSize << "\n"
        "Data stripes :    " << attr.numStripes << "\n"
        "Recovery stripes: " << attr.numRecoveryStripes << "\n"
        "Type:             " << attr.striperType << "\n"
    ;
}

int
fileInfo(KfsClient* kfsClient, const vector<string> &args)
{
    if (args.empty() || args[0] == "--help") {
        cout << "Usage: finfo fileid\n";
        return 0;
    }
    int ret = 0;
    for (vector<string>::const_iterator it = args.begin();
            it != args.end();
            ++it) {
        const kfsFileId_t fileId = strtoll(it->c_str(), 0, 10);
        if (fileId <= 0) {
            cerr << "skipping invalid file id: " << *it << "\n";
            continue;
        }
        KfsFileAttr            attr;
        chunkOff_t             offset       = -1;
        int64_t                chunkVersion = -1;
        vector<ServerLocation> servers;
        const int status = kfsClient->GetFileOrChunkInfo(fileId, -1,
            attr, offset, chunkVersion, servers);
        if (status != 0) {
            cout << "fileid: " << fileId <<
                " error: " << ErrorCodeToStr(status) << "\n";
            if (ret == 0) {
                ret = status;
            }
            continue;
        }
        printInfo(attr, offset, chunkVersion, servers);
    }
    return ret;
}

int
chunkInfo(KfsClient* kfsClient, const vector<string> &args)
{
    if (args.empty() || args[0] == "--help") {
        cout << "Usage: cinfo chunkid\n";
        return 0;
    }
    int ret = 0;
    for (vector<string>::const_iterator it = args.begin();
            it != args.end();
            ++it) {
        const kfsChunkId_t chunkId = strtoll(it->c_str(), 0, 10);
        if (chunkId <= 0) {
            cerr << "skipping invalid chunk id: " << *it << "\n";
            continue;
        }
        KfsFileAttr            attr;
        chunkOff_t             offset       = -1;
        int64_t                chunkVersion = -1;
        vector<ServerLocation> servers;
        const int status = kfsClient->GetFileOrChunkInfo(-1, chunkId,
            attr, offset, chunkVersion, servers);
        if (status != 0) {
            cout << "chunkId: " << chunkId <<
                " error: " << ErrorCodeToStr(status) << "\n";
            if (ret == 0) {
                ret = status;
            }
            continue;
        }
        printInfo(attr, offset, chunkVersion, servers);
    }
    return ret;
}

}
}

