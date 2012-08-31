//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/09/21
// Author: Sriram Rao
//
// Copyright 2008 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
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
// \brief Tool that implements rm -rf <path>
//----------------------------------------------------------------------------


#include "kfsshell.h"
#include "libclient/KfsClient.h"

#include <iostream>
#include <vector>
#include <string>

namespace KFS {
namespace tools {

using std::cout;
using std::vector;
using std::string;

int
handleRm(KfsClient* kfsClient, const vector<string>& args)
{
    if ((args.size() < 1) || (args[0] == "--help") || (args[0] == "")) {
        cout << "Usage: rm <path>" << "\n"
             << "       If <path> is a directory, then the"
             << " directory tree is removed." << "\n";
        return 0;
    }

    KfsFileAttr attr;
    int         res = 0;
    for (vector<string>::const_iterator it = args.begin();
            it != args.end();
            ++it) {
        const char* const pathname = it->c_str();
        res = kfsClient->Stat(pathname, attr);
        if (res >= 0) {
            res = attr.isDirectory ? kfsClient->RmdirsFast(pathname) :
                kfsClient->Remove(pathname);
        }
        if (res < 0) {
            cout << "failed to remove: " << pathname << ": " <<
                ErrorCodeToStr(res) << "\n";
            break;
        }
    }
    return res;
}

}
}
