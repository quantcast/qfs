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

#include "libclient/KfsClient.h"
#include "qcdio/QCMutex.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <dirent.h>
#include <glob.h>

#include <boost/regex.hpp>
#include <map>

namespace KFS {
namespace tools {

using std::string;
using std::map;
using std::make_pair;

using boost::regex;
using boost::smatch;
using boost::regex_match;
using boost::regbase;

class FileSystemImpl : public FileSystem
{
public:
    virtual ~FileSystemImpl()
        {}
};

class LocalFileSystem : public FileSystemImpl
{
public:
    class LocalDirIterator : public DirIterator
    {
    public:
        LocalDirIterator(
            DIR* inDirPtr,
            bool inFetchAttributesFlag)
            : mDirPtr(inDirPtr),
              mFetchAttributesFlag(inFetchAttributesFlag)
            {}
        int Delete()
        {
            const int theRet = mDirPtr ? closedir(mDirPtr) : EINVAL;
            delete this;
            return theRet;
        }
        const struct dirent* Next(
            const StatBuf*& outStatPtr)
        {
            if (mDirPtr) {
                return 0;
            }
            const struct dirent* theRetPtr = readdir(mDirPtr);
            if (! theRetPtr) {
                return 0;
            }
            if (mFetchAttributesFlag && outStatPtr) {
                // stat(mStatBuf);
            }
            return theRetPtr;
        }
    private:
        DIR* const  mDirPtr;
        bool const  mFetchAttributesFlag;
        struct stat mStatBuf;

        virtual ~LocalDirIterator()
            {}
    };
    LocalFileSystem()
        {}
    virtual ~LocalFileSystem()
        {}
    virtual int Chdir(
        const string& inDir)
    {
        return Errno(chdir(inDir.c_str()));
    }
    virtual int GetCwd(
        string& outDir)
    {
        const char* const theCwdPtr = getcwd(mPathBuf, PATH_MAX);
        if (theCwdPtr) {
            outDir = theCwdPtr;
            return 0;
        }
        return Errno(-1);
    }
    virtual int Open(
        const string& inFileName,
        int           inFlags,
        int           inMode,
        const string* /* inParamsPtr */)
    {
        return Errno(open(inFileName.c_str(), inFlags, inMode));
    }
    virtual int Close(
        int inFd)
    {
        return Errno(close(inFd));
    }
    virtual ssize_t Read(
        int    inFd,
        void*  inBufPtr,
        size_t inBufSize)
    {
        const ssize_t theRet = read(inFd, inBufPtr, inBufSize);
        if (theRet >= 0) {
            return theRet;
        }
        return Errno((int)theRet);
    }
    virtual ssize_t Write( 
        int          inFd,
        const void*  inBufPtr,
        size_t       inBufSize)
    {
        const ssize_t theRet = write(inFd, inBufPtr, inBufSize);
        if (theRet >= 0) {
            return theRet;
        }
        return Errno((int)theRet);
    }
    virtual int Flush(
        int inFd)
    {
        return Errno(fsync(inFd));
    }
    virtual int Stat(
        const string& inFileName,
        StatBuf&      outStat)
    {
        return Errno(stat(inFileName.c_str(), &outStat));
    }
    virtual int Open(
        const string& inDirName,
        bool          inFetchAttributesFlag,
        DirIterator*& outDirIteratorPtr)
    {
        outDirIteratorPtr = 0;
        DIR* const theDirPtr = opendir(inDirName.c_str());
        if (! theDirPtr) {
            return Errno(-1);
        }
        outDirIteratorPtr =
            new LocalDirIterator(theDirPtr, inFetchAttributesFlag);
        return 0;
    }
    virtual int Close(
        DirIterator* inDirIteratorPtr)
    {
        return Errno(inDirIteratorPtr ?
            static_cast<LocalDirIterator*>(inDirIteratorPtr)->Delete() :
            EINVAL
        );
    }
    virtual int Next(
        DirIterator*    inDirIteratorPtr,
        bool&           outHasNextFlag,
        string&         outName,
        const StatBuf*& outStatPtr)
    {
        if (! inDirIteratorPtr) {
            outHasNextFlag = false;
            outStatPtr     = 0;
            return Errno(EINVAL);
        }
        const struct dirent* const theDirEntPtr =
            static_cast<LocalDirIterator*>(inDirIteratorPtr)->Next(outStatPtr);
        outHasNextFlag = theDirEntPtr != 0;
        if (outHasNextFlag) {
            outName = theDirEntPtr->d_name;
        } else {
            outName.clear();
        }
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
    char mPathBuf[PATH_MAX];

    int Errno(int inVal)
    {
        if (inVal >= 0) {
            return inVal;
        }
        const int theRet = errno;
        return (theRet == 0 ? -1 : (theRet < 0 ? theRet : -theRet));
    }
    LocalFileSystem(
        const LocalFileSystem& inFileSystem);
    LocalFileSystem operator=(
        const LocalFileSystem& inFileSystem);
};

class KfsFileSystem : public FileSystemImpl,
    private KfsClient
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
    KfsFileSystem()
        : KfsClient()
        {}
    virtual ~KfsFileSystem()
        {}
    int Init(
        const string& theHostPort)
    {
        int          thePort = 20000;
        const size_t thePos  = theHostPort.find(':');
        if (thePos != string::npos) {
            thePort = atoi(theHostPort.c_str() + thePos + 1);
        }
        return KfsClient::Init(theHostPort.substr(0, thePos), thePort);
    }
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
    KfsFileSystem(
        const KfsFileSystem& inFileSystem);
    KfsFileSystem operator=(
        const KfsFileSystem& inFileSystem);
};

static QCMutex&
GetFsMutex()
{
    static QCMutex sMutex;
    return sMutex;
}

// Force initialization before entering main.
static QCMutex& sMutex = GetFsMutex();

class FSMap : public map<string, FileSystemImpl*>
{
public:
    ~FSMap()
    {
        for (iterator theIt = begin(); theIt != end(); ++theIt) {
            delete theIt->second;
            theIt->second = 0;
        }
    }
};

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
    static FSMap sFSMap;

    outFsPtr = 0;
    smatch theParts;
    if(! regex_match(inUri, theParts, kFsUriRegex)) {
        return -EINVAL;
    }

    const string theScheme   (theParts[2]);
    const string theAuthority(theParts[4]);
    const string theFragment (theParts[9]);
    if (outPathPtr) {
        *outPathPtr = theParts[5];
    }
    const string          theFsId(theScheme + theAuthority);
    FSMap::iterator const theIt = sFSMap.find(theFsId);
    if (theIt != sFSMap.end()) {
        outFsPtr = theIt->second;
        return 0;
    }
    FileSystemImpl* theImplPtr = 0;
    int             theRet     = 0;
    if (theScheme == "qfs" || theScheme == "kfs") {
        KfsFileSystem* const theFsPtr = new KfsFileSystem();
        if ((theRet = theFsPtr->Init(theAuthority)) == 0) {
            theImplPtr = theFsPtr;
        } else {
            delete theFsPtr;
        }
    } else if (theScheme.empty() ||
            theScheme == "local" ||
            theScheme == "file") {
        theImplPtr = new LocalFileSystem();
    }
    if (theRet == 0 && ! theImplPtr) {
        theRet = -EINVAL;
    }
    if (theRet == 0 &&
            ! sFSMap.insert(make_pair(theFsId, theImplPtr)).second) {
        QCRTASSERT(! "fs map insertion: duplicate entry");
    }
    outFsPtr = theImplPtr;
    return theRet;
}

}
}
