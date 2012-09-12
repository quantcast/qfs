//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/09/11
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
// \brief Generic file system interface.
//
//----------------------------------------------------------------------------

#ifndef TOOLS_FILE_SYSTEM_H
#define TOOLS_FILE_SYSTEM_H

#include <string>

namespace KFS {
namespace tools {

using std::string;

class FileSystem
{
public:
    static FileSystem* Create(
        const string& inUri,
        string*       outPathPtr = 0);
    int Chdir(
        const string& inDir);
    int GetCwd(
        string& outDir);
    int Open(
        const string& inFileName)
    
private:
    FileSystem(
        const FileSystem& inFileSystem);
    FileSystem operator=(
        const FileSystem& inFileSystem);
};

}
}

#endif /* TOOLS_FILE_SYSTEM_H */
