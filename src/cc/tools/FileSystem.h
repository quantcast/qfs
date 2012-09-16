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

#include "common/kfstypes.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <glob.h>
#include <inttypes.h>

#include <string>

namespace KFS {
namespace tools {

using std::string;

class FileSystem
{
public:
    class StatBuf : public stat
    {
    public:
        StatBuf()
            : stat()
            {}
    };
    class DirIterator
    {
    protected:
        DirIterator()
            {}
        virtual ~DirIterator()
            {}
    };
    static int Get(
        const string& inUri,
        FileSystem*&  outFsPtr,
        string*       outPathPtr = 0);
    virtual int Chdir(
        const string& inDir) = 0;
    virtual int GetCwd(
        string& outDir) = 0;
    virtual int Open(
        const string& inFileName,
        int           inFlags,
        int           inMode,
        const string* inParamsPtr) = 0;
    int Open(
        const string& inFileName,
        int           inFlags,
        int           inMode)
        { return Open(inFileName, inFlags, inMode, 0); }
    virtual int Close(
        int inFd) = 0;
    virtual ssize_t Read(
        int    inFd,
        void*  inBufPtr,
        size_t inBufSize) = 0;
    virtual ssize_t Write( 
        int          inFd,
        const void*  inBufPtr,
        size_t       inBufSize) = 0;
    virtual int Sync(
        int inFd) = 0;
    virtual int64_t Seek(
        int     inFd,
        int64_t inOffset,
        int     inWhence) = 0;
    virtual int Stat(
        const string& inFileName,
        StatBuf&      outStat) = 0;
    virtual int Open(
        const string& inDirName,
        bool          inFetchAttributesFlag,
        DirIterator*& outDirIteratorPtr) = 0;
    virtual int Close(
        DirIterator* inDirIteratorPtr) = 0;
    virtual int Next(
        DirIterator*    inDirIteratorPtr,
        string&         outName,
        const StatBuf*& outStatPtr) = 0;
    virtual int Glob(
        const string& inPattern,
        int           inFlags,
        int (*inErrFuncPtr) (const char* inErrPathPtr, int inErrno),
        glob_t*        inGlobPtr) = 0;
    virtual int Chmod(
        const string& inPathName,
        kfsMode_t     inMode) = 0;
    virtual int Chown(
        const string& inPathName,
        kfsUid_t      inOwner,
        kfsUid_t      inGroup) = 0;
    virtual int GetUserAndGroupNames(
        kfsUid_t inUser,
        kfsGid_t inGroup,
        string&  outUserName,
        string&  outGroupName) = 0;
    virtual string StrError(
        int inError) = 0;
protected:
    virtual ~FileSystem()
        {}
};

}
}

#endif /* TOOLS_FILE_SYSTEM_H */
