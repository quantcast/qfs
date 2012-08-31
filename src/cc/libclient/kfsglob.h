//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/08/18
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
// \brief Kfs glob() equivalent.
//
//----------------------------------------------------------------------------

#ifndef LIBCLIENT_KFSGLOB_H
#define LIBCLIENT_KFSGLOB_H

#include <glob.h>

namespace KFS
{
class KfsClient;

int KfsGlob(
    KfsClient&  inClient,
    const char* inGlobPtr,
    int         inGlobFlags,
    int (*inErrorHandlerPtr)(const char* inErrPathPtr, int inError),
    glob_t*     inResultPtr);

}

#endif /* LIBCLIENT_KFSGLOB_H */
