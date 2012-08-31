//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2007/09/20
// Author: Sriram Rao
//
// Copyright 2008 Quantcast Corp.
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
// \brief Common utility functions for kfsshell
//
//----------------------------------------------------------------------------

#include "kfsshell.h"
#include "libclient/KfsClient.h"

#include <iostream>
#include <cerrno>

namespace KFS {
namespace tools {

using std::cout;
using std::vector;
using std::string;


// Make the directory hierarchy in KFS defined by path.

int
doMkdirs(KfsClient* kfsClient, const char* path)
{
    const int res = kfsClient->Mkdirs(path);
    if (res < 0 && res != -EEXIST) {
        cout << path << ": mkdirs failure: " << ErrorCodeToStr(res) << "\n";
        return res;
    }
    return 0;
}

// remove a single directory in kfs

int
doRmdir(KfsClient* kfsClient, const char* dirname)
{
    const int res = kfsClient->Rmdir(dirname);
    if (res < 0) {
        cout <<  dirname << ": rmdir failure: " << ErrorCodeToStr(res) << "\n";
        return res;
    }
    return 0;
}

}
}
