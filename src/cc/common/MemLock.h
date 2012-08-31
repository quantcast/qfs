//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2011/10/11
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
// \file MemLock.h
// \brief Process common memory locking interface.
//
//----------------------------------------------------------------------------

#ifndef MEM_LOCK_H
#define MEM_LOCK_H

#include <inttypes.h>
#include <string>

namespace KFS
{
using std::string;

int
LockProcessMemory(
    int64_t inMaxLockedMemorySize,
    int64_t inMaxHeapSize    = 0,
    int64_t inMaxStlPoolSize = 0,
    string* outErrMsgPtr     = 0);
}

#endif /* MEM_LOCK_H */
