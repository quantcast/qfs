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

#include <unistd.h>

#include <string>

namespace KFS {
namespace tools {

using std::string;

class FileSystem
{
public:
    class StatBuf : public struct stat
    {
    public:
        StatBuf()
            : stat()
            {}
    };
    static FileSystem* Create(
        const string& inUri,
        string*       outPathPtr = 0);
    virtual int Chdir(
        const string& inDir);
    virtual int GetCwd(
        string& outDir);
    virtual int Open(
        const string& inFileName,
        int           inFlags,
        int           inMode,
        const string* inParamsPtr = 0);
    virtual int Close(
        int inFd);
    virtual ssize_t Read(
        int    inFd,
        void*  inBufPtr,
        size_t inBufSize);
    virtual ssize_t Write( 
        int          inFd,
        const void*  inBufPtr,
        size_t       inBufSize);
    virtual int Flush(
        int inFd);
    virtual int Stat(
        const string& inFileName,
        StatBuf&      outStat);
    virtual int 
private:
    FileSystem(
        const FileSystem& inFileSystem);
    FileSystem operator=(
        const FileSystem& inFileSystem);
};

}
}

#endif /* TOOLS_FILE_SYSTEM_H */
