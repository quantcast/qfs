//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/12/11
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
// \brief Trash a la hadoop.
//
//----------------------------------------------------------------------------


#include <string>

#include "FileSystem.h"

namespace KFS
{
class Properties;

namespace tools
{
using std::string;

class Trash
{
public:
    typedef FileSystem::ErrorHandler ErrorHandler;
    Trash(
        FileSystem&       inFs,
        const Properties& inProps,
        const string&     inPrefix);
    ~Trash();
    int SetParameters(
        const Properties& inProperties,
        const string&     inPrefix);
    int MoveTo(
        const string& inPath,
        bool&         outMovedFlag,
        string*       inErrMsgPtr = 0);
    int Expunge(
        ErrorHandler* inErrorHandlerPtr = 0);
    bool IsEnabled() const;
    int RunEmptier(
        ErrorHandler* inErrorHandlerPtr = 0);
    int GetEmptierIntervalSec() const;
private:
    class Impl;
    Impl& mImpl;

private:
    Trash(
        const Trash& inTrash);
    Trash& operator=(
        const Trash& inTrash);
};

}
}
