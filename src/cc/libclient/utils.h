//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/08/31
// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
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
// \brief utils.h: Miscellaneous utility functions.
//
//----------------------------------------------------------------------------

#ifndef LIBKFSCLIENT_UTILS_H
#define LIBKFSCLIENT_UTILS_H

#include <sys/time.h>

namespace KFS {
namespace client {

void GetTimeval(const char* s, struct timeval& tv);
void Sleep(int secs);

}
}

#endif // LIBKFSCLIENT_UTILS_H
