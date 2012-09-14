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
// \brief "Local" and Kfs file system implementations.
//
//----------------------------------------------------------------------------


#include "FileSystem.h"

#include "qcdio/QCMutex.h"
#include "qcdio/qcstutils.h"

#include <boost/regex.hpp>

namespace KFS {
namespace tools {

using std::string;
using boost::regex;
using boost::smatch;
using boost::regex_match;
using boost::regbase;

class LocalFileSystem : public FileSystem
{
public:
    class LocalDirIterator : public DirIterator
    {
    protected:
        LocalDirIterator()
            {}
        virtual ~LocalDirIterator()
            {}
        friend class LocalFileSystem;
    };
    virtual int Chdir(
        const string& inDir)
    {
        return 0;
    }
    virtual int GetCwd(
        string& outDir)
    {
        return 0;
    }
    virtual int Open(
        const string& inFileName,
        int           inFlags,
        int           inMode,
        const string* inParamsPtr)
    {
        return -1;
    }
    virtual int Close(
        int inFd)
    {
        return 0;
    }
    virtual ssize_t Read(
        int    inFd,
        void*  inBufPtr,
        size_t inBufSize)
    {
        return 0;
    }
    virtual ssize_t Write( 
        int          inFd,
        const void*  inBufPtr,
        size_t       inBufSize)
    {
        return 0;
    }
    virtual int Flush(
        int inFd)
    {
        return 0;
    }
    virtual int Stat(
        const string& inFileName,
        StatBuf&      outStat)
    {
        return 0;
    }
    virtual int Open(
        const string& inDirName,
        bool          inFetchAttributesFlag,
        DirIterator*& outDirIteratorPtr)
    {
        return 0;
    }
    virtual int Close(
        DirIterator* inDirIteratorPtr)
    {
        return 0;
    }
    virtual int Next(
        DirIterator*    inDirIteratorPtr,
        bool&           outHasNextFlag,
        string&         outName,
        const StatBuf*& outStatPtr)
    {
        return 0;
    }
    virtual int Glob(
        const string& inPattern,
        int           inFlags,
        int (*inErrFuncPtr) (const char* inErrPathPtr, int inErrno),
        glob_t*        inGlobPtr)
    {
        return 0;
    }
private:
    LocalFileSystem(
        const LocalFileSystem& inFileSystem);
    LocalFileSystem operator=(
        const LocalFileSystem& inFileSystem);
};

static QCMutex&
GetFsMutex()
{
    static QCMutex sMutex;
    return sMutex;
}

// Force initialization before entering main.
static QCMutex& sMutex = GetFsMutex();

    /* static */ int
FileSystem::Get(
    const string& inUri,
    FileSystem*&  outFsPtr,
    string*       outPathPtr /* = 0 */)
{
    QCStMutexLocker theLock(GetFsMutex());

    // Regular expression for parsing URLS from:
    // http://tools.ietf.org/html/rfc3986#appendix-B
    static const regex kFsUriRegex(
        "^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?$",
        regbase::normal
    );

    smatch theParts;
    if(! regex_match(inUri, theParts, kFsUriRegex)) {
        return 0;
    }

    const string theScheme   (theParts[2]);
    const string theAuthority(theParts[4]);
    const string theFragment( theParts[9]);
    if (outPathPtr) {
        *outPathPtr = theParts[5];
    }
    return 0;
}

}
}
