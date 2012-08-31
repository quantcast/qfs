//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2007/09/26
//
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
// \brief A simple shell that lets users navigate KFS directory hierarchy.
//
//----------------------------------------------------------------------------

#ifndef TOOLS_KFSSHELL_H
#define TOOLS_KFSSHELL_H

#include <string>
#include <vector>

namespace KFS
{
class KfsClient;
namespace tools
{
using std::vector;
using std::string;

typedef int (*cmdHandler)(KfsClient *, const vector<string> &args);
// cmd handlers
int handleCd(KfsClient *, const vector<string> &args);
int handleChangeReplication(KfsClient *, const vector<string> &args);
int handleCopy(KfsClient *, const vector<string> &args);
int handleFstat(KfsClient *, const vector<string> &args);
int handleLs(KfsClient *, const vector<string> &args);
int handleMkdirs(KfsClient *, const vector<string> &args);
int handleMv(KfsClient *, const vector<string> &args);
int handleRmdir(KfsClient *, const vector<string> &args);
int handlePing(KfsClient *, const vector<string> &args);
int handleRm(KfsClient *, const vector<string> &args);
int handlePwd(KfsClient *, const vector<string> &args);
int handleAppend(KfsClient *, const vector<string> &args);
int fileInfo(KfsClient *, const vector<string> &args);
int chunkInfo(KfsClient *, const vector<string> &args);
int handleChmod(KfsClient* kfsClient, const vector<string>& args);
int handleChown(KfsClient* kfsClient, const vector<string>& args);
int handleChgrp(KfsClient* kfsClient, const vector<string>& args);
// utility functions
int doMkdirs(KfsClient *, const char *path);
int doRmdir(KfsClient *, const char *dirname);
}
}

#endif // TOOLS_KFSSHELL_H
