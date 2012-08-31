//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/08/29
//
// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
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
// \brief Setup code to get an emulator up.
//
//----------------------------------------------------------------------------

#ifndef EMULATOR_EMULATORSETUP_H
#define EMULATOR_EMULATORSETUP_H

#include <string>
#include <ostream>

namespace KFS
{
using std::string;
using std::ostream;

// pass an optional argument that enables changing the degree of replication for a file.
int EmulatorSetup(
    string&  logdir,
    string&  cpdir,
    string&  networkFn,
    string&  chunkmapFn,
    int16_t  minReplicasPerFile = 1,
    bool     addChunksToReplicationChecker = false);
}

#endif // EMULATOR_EMULATORSETUP_H
