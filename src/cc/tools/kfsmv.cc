//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2007/09/24
// Author: Sriram Rao
//
// Copyright 2008-2011 Quantcast Corp.
// Copyright 2007-2008 Kosmix Corp.
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
// \brief Tool that renames a file/directory from a KFS path to another
// KFS path.  This does the analogous of "mv".
//
//----------------------------------------------------------------------------

#include "kfsshell.h"
#include "libclient/KfsClient.h"
#include "common/MsgLogger.h"

#include <iostream>
#include <fstream>
#include <cerrno>

namespace KFS {
namespace tools {

using std::cout;
using std::vector;
using std::string;

int
handleMv(KfsClient *kfsClient, const vector<string> &args)
{
    if (args.size() != 2 || args[0] == "--help" ||
            args[0].empty() || args[1].empty()) {
        cout << "Usage: mv <source path> <dst path>" << "\n";
        return 0;
    }

    string target = args[1];
    if (kfsClient->IsDirectory(target.c_str())) {
        const char* const b = args[0].c_str();
        const char*       p = b + args[0].length() - 1;
        while (b < p && *p == '/') {
            --p;
        }
        const char* const e = p;
        while (b < p && *p != '/') {
            --p;
        }
        if (target[target.length() - 1] == '/') {
            if (*p == '/') {
                ++p;
            }
        } else if (*p != '/') {
            target.append(1, char('/'));
        }
        target.append(p, e - p + 1);
        KFS_LOG_STREAM_DEBUG << "target: " << target << KFS_LOG_EOM;
    }

    const int res = kfsClient->Rename(args[0].c_str(), target.c_str());
    if (res != 0) {
        cout << "Rename failed: " << ErrorCodeToStr(res) << "\n";
    }
    return res;
}

}
}
