//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/12/15
// Author: Sriram Rao
//
// Copyright 2008 Quantcast Corp.
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
// \brief Tool for fstat'ing a file.
//
//----------------------------------------------------------------------------

#include "kfsshell.h"
#include "libclient/KfsClient.h"

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include <iostream>
#include <fstream>
#include <iomanip>
#include <cerrno>

namespace KFS
{
namespace tools
{
using std::cout;
using std::oct;
using std::dec;
using std::endl;
using std::ofstream;
using std::vector;

int
handleFstat(KfsClient* kfsClient, const vector<string>& args)
{
    if (args.empty() || args[0] == "--help") {
        cout << "Usage: stat <path> " << "\n";
        return 0;
    }
    for (vector<string>::const_iterator
            it = args.begin(); it != args.end(); ++it) {
        KfsFileAttr attr;
        const int ret = kfsClient->Stat(it->c_str(), attr);
        if (ret) {
            cout << *it << ": " << ErrorCodeToStr(ret) << "\n";
            return ret;
        }
        cout <<
            "File:             " << args[0] << "\n"
            "ctime:            " << attr.ctime.tv_sec << "\n"
            "mtime:            " << attr.mtime.tv_sec << "\n"
            "Size:             " << attr.fileSize << "\n"
            "Id:               " << attr.fileId << "\n"
            "Replication:      " << attr.numReplicas << "\n"
            "Chunks:           " << attr.chunkCount() << "\n"
            "Files:            " << attr.fileCount() << "\n"
            "Dirs:             " << attr.dirCount() << "\n"
            "Owner:            " << attr.user << "\n"
            "Group:            " << attr.group << "\n"
            "Mode:             " << oct << attr.mode << dec << "\n"
            "MinTier:          " << (int)attr.minSTier << "\n"
            "MaxTier:          " << (int)attr.maxSTier << "\n"
        ;
        if (attr.striperType == KFS_STRIPED_FILE_TYPE_NONE) {
            continue;
        }
        cout <<
            "Stripe size:      " << attr.stripeSize << "\n"
            "Data stripes :    " << attr.numStripes << "\n"
            "Recovery stripes: " << attr.numRecoveryStripes << "\n"
            "Type:             " << attr.striperType << "\n"
        ;
    }
    return 0;
}
}} // KFS::tools
