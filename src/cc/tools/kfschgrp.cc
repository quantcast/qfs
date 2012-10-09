//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/06/14
// Author: Mike Ovsiannikov
//
// Copyright 2012 Quantcast Corp.
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
// \brief chgrp.
//
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
handleChgrp(KfsClient* kfsClient, const vector<string>& args)
{
    bool recursFlag = false;
    if (args.size() < 2 || args[0] == "--help" ||
            ((recursFlag = args[0] == "-R") && args.size() < 3)) {
        cout << "Usage: chgrp [-R] <group> <path>" << "\n";
        return 0;
    }

    size_t i = recursFlag ? 1 : 0;
    const char* const gname = args[i].c_str();
    for (++i ; i < args.size(); i++) {
        const char* const name = args[i].c_str();
        const int res = recursFlag ?
                kfsClient->ChownR(name, 0, gname) :
                kfsClient->Chown(name, 0, gname);
        if (res != 0) {
            cout << name << ": " << ErrorCodeToStr(res) << "\n";
            return res;
        }
    }
    return 0;
}

}
}

