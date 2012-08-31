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
// \brief Tool that implements mkdir -p <path>
//----------------------------------------------------------------------------

#include "kfsshell.h"
#include "libclient/KfsClient.h"

#include <iostream>

namespace KFS {
namespace tools {

using std::cout;
using std::vector;
using std::string;

int
handleMkdirs(KfsClient* client, const vector<string>& args)
{
    if ((args.size() < 1) || (args[0] == "--help") || (args[0] == "")) {
        cout << "Usage: mkdir <dir> " << "\n";
        return 0;
    }

    return doMkdirs(client, args[0].c_str());
}

}
}
