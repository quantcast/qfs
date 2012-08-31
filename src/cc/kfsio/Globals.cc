//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/10/09
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
// \brief Define the symbol for the KFS IO library global variables.

//----------------------------------------------------------------------------

#include "Globals.h"

namespace KFS
{
namespace libkfsio
{

Globals_t* Globals_t::sForGdbToFindInstance = 0;

Globals_t::Globals_t()
    : counterManager(),
      ctrOpenNetFds      ("Open network fds"),
      ctrOpenDiskFds     ("Open disk fds"),
      ctrNetBytesRead    ("Bytes read from network"),
      ctrNetBytesWritten ("Bytes written to network"),
      ctrDiskBytesRead   ("Bytes read from disk"),
      ctrDiskBytesWritten("Bytes written to disk"),
      ctrDiskIOErrors    ("Disk I/O errors"),
      mInitedFlag(false),
      mDestructedFlag(false),
      mForGdbToFindNetManager(0)
{
    counterManager.AddCounter(&ctrOpenNetFds);
    counterManager.AddCounter(&ctrOpenDiskFds);
    counterManager.AddCounter(&ctrNetBytesRead);
    counterManager.AddCounter(&ctrNetBytesWritten);
    counterManager.AddCounter(&ctrDiskBytesRead);
    counterManager.AddCounter(&ctrDiskBytesWritten);
    counterManager.AddCounter(&ctrDiskIOErrors);
    sForGdbToFindInstance = this;
}

Globals_t::~Globals_t()
{
    mDestructedFlag = true;
}

Globals_t&
Globals_t::Instance()
{
    static Globals_t globalsInstance;
    return globalsInstance;
}

NetManager&
Globals_t::getNetManager()
{
    // Ensure that globals are constructed before net manager.
    NetManager*& netManager = Instance().mForGdbToFindNetManager;
    static NetManager netManagerInstance;
    if (! netManager) {
        netManager = &netManagerInstance;
    }
    return netManagerInstance;
}

void
Globals_t::Init()
{
    if (mInitedFlag) {
        return;
    }
    mInitedFlag = true;
}

void
Globals_t::Destroy()
{
    // Rely on compiler / runtime to invoke static dtors in the reverse order.
}

}
}
