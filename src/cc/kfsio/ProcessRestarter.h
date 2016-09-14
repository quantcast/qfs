//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2016/09/14
// Author: Mike Ovsiannikov
//
// Copyright 2016 Quantcast Corp.
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
// Restart process by issuing exec with saved args, environment, and working
// directory.
//
//----------------------------------------------------------------------------

#include <string>

namespace KFS
{
using std::string;

class Properties;

class ProcessRestarter
{
public:
    ProcessRestarter();
    ~ProcessRestarter();
    bool Init(
        int    inArgCnt,
        char** inArgsPtr);
    void SetParameters(
        const char*       inPrefixPtr,
        const Properties& inProps);
    string Restart();
private:
    class Impl;
    Impl& mImpl;
};

} // namespace KFS
