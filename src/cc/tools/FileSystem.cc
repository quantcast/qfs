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
#include "libclient/kfsglob.h"
#include "common/StBuffer.h"
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
            const string& inDirName,
            DIR*          inDirPtr,
            bool          inFetchAttributesFlag)
            : mDirName(inDirName),
              mFileName(),
              mDirPtr(inDirPtr),
              mFetchAttributesFlag(inFetchAttributesFlag),
              mError(0),
              mStatBuf()
            {}
        int Delete()
        {
            if (! mDirPtr) {
                return EINVAL;
            }
            const int theRet = closedir(mDirPtr) ? errno : 0;
            delete this;
            return theRet;
        }
        const struct dirent* Next(
            const StatBuf*& outStatPtr)
        {
            outStatPtr = 0;
            if (! mDirPtr) {
                mError = EINVAL;
                return 0;
            }
            mError = 0;
            const struct dirent* theRetPtr = readdir(mDirPtr);
            if (! theRetPtr) {
                return 0;
            }
            if (mFetchAttributesFlag && outStatPtr) {
                if (mFileName.empty()) {
                    mFileName.assign(mDirName.data(), mDirName.length());
                    if (! mFileName.empty() && *(mFileName.rbegin()) != '/') {
                        mFileName += "/";
                    }
                }
                const size_t theLen = mFileName.length();
                mFileName += theRetPtr->d_name;
                if (stat(mFileName.c_str(), &mStatBuf)) {
                    mError    = errno;
                    theRetPtr = 0;
                } else {
                    outStatPtr = &mStatBuf;
                }
                mFileName.erase(theLen);
            }
            return theRetPtr;
        }
        int GetError() const
            { return mError; }
    private:
        const string mDirName;
        string       mFileName;
        DIR* const   mDirPtr;
        bool const   mFetchAttributesFlag;
        int          mError;
        StatBuf      mStatBuf;

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
        StBufferT<char, 1> theBuf;
        char* const thePathBufPtr = theBuf.Resize(PATH_MAX);
        const char* const theCwdPtr = getcwd(thePathBufPtr, PATH_MAX);
        if (theCwdPtr) {
            outDir = theCwdPtr;
            return 0;
        }
        return RetErrno(errno);
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
    virtual int Sync(
        int inFd)
    {
        return Errno(fsync(inFd));
    }
    virtual int64_t Seek(
        int     inFd,
        int64_t inOffset,
        int     inWhence)
    {
        const int64_t thePos = lseek(inFd, inOffset, inWhence);
        return (thePos < 0 ? RetErrno(errno) : thePos);
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
            return Errno(errno);
        }
        outDirIteratorPtr =
            new LocalDirIterator(inDirName, theDirPtr, inFetchAttributesFlag);
        return 0;
    }
    virtual int Close(
        DirIterator* inDirIteratorPtr)
    {
        return RetErrno(inDirIteratorPtr ?
            static_cast<LocalDirIterator*>(inDirIteratorPtr)->Delete() :
            EINVAL
        );
    }
    virtual int Next(
        DirIterator*    inDirIteratorPtr,
        string&         outName,
        const StatBuf*& outStatPtr)
    {
        if (! inDirIteratorPtr) {
            outStatPtr = 0;
            outName.clear();
            return RetErrno(EINVAL);
        }
        LocalDirIterator& theDirIt =
            *(static_cast<LocalDirIterator*>(inDirIteratorPtr));
        const struct dirent* const theDirEntPtr = theDirIt.Next(outStatPtr);
        if (theDirEntPtr) {
            outName = theDirEntPtr->d_name;
        } else {
            outName.clear();
        }
        return RetErrno(theDirIt.GetError());
    }
    virtual int Glob(
        const string& inPattern,
        int           inFlags,
        int (*inErrFuncPtr) (const char* inErrPathPtr, int inErrno),
        glob_t*        inGlobPtr)
    {
        return glob(inPattern.c_str(), inFlags, inErrFuncPtr, inGlobPtr);
    }
    virtual int Chmod(
        const string& inPathName,
        kfsMode_t     inMode)
    {
        return Errno(chmod(inPathName.c_str(), (mode_t)inMode));
    }
    virtual int Chown(
        const string& inPathName,
        kfsUid_t      inOwner,
        kfsUid_t      inGroup)
    {
        return Errno(chown(inPathName.c_str(), (uid_t)inOwner, (gid_t)inGroup));
    }
    virtual int GetUserAndGroupNames(
        kfsUid_t inUser,
        kfsGid_t inGroup,
        string&  outUserName,
        string&  outGroupName);
    virtual string StrError(
        int inError)
    {
        return QCUtils::SysError(inError < 0 ? -inError : inError);
    }
private:

    int Errno(
        int inVal)
    {
        if (inVal >= 0) {
            return inVal;
        }
        return RetErrno(errno);
    }
    int RetErrno(
        int inErrno)
    {
        return (inErrno == 0 ? -1 : (inErrno < 0 ? inErrno : -inErrno));
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
    static void ToStat(
        const KfsFileAttr& inAttr,
        StatBuf&           outStatBuf)
    {
        inAttr.ToStat(outStatBuf);
    }
    class KfsDirIterator : public DirIterator
    {
    public:
        KfsDirIterator(
            vector<KfsFileAttr>* inAttrsPtr,
            vector<string>*      inNamesPtr)
            : mFetchAttributesFlag(inAttrsPtr != 0),
              mAttrs(),
              mNames(),
              mCur(0),
              mStatBuf()
        {
            if (mFetchAttributesFlag) {
                inAttrsPtr->swap(mAttrs);
            } else if (inNamesPtr) {
                inNamesPtr->swap(mNames);
            }
        }
        virtual ~KfsDirIterator()
            {}
        void Next(
            string&         outName,
            const StatBuf*& outStatBufPtr)
        {
            if (mFetchAttributesFlag) {
                if (mCur >= mAttrs.size()) {
                    outStatBufPtr = 0;
                    outName.clear();
                    return;
                }
                ToStat(mAttrs[mCur++], mStatBuf);
                outStatBufPtr = &mStatBuf;
                return;
            }
            outStatBufPtr = 0;
            if (mCur >= mNames.size()) {
                outName.clear();
                return;
            }
            outName = mNames[mCur++];
        }
    private:
        const bool          mFetchAttributesFlag;
        vector<KfsFileAttr> mAttrs;
        vector<string>      mNames;
        size_t              mCur;
        StatBuf             mStatBuf;
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
        return Cd(inDir.c_str());
    }
    virtual int GetCwd(
        string& outDir)
    {
        outDir = KfsClient::GetCwd();
        return 0;
    }
    virtual int Open(
        const string& inFileName,
        int           inFlags,
        int           inMode,
        const string* inParamsPtr)
    {
        if (! inParamsPtr || inParamsPtr->empty()) {
            const int kReplicaCount        = 1;
            const int kStripeCount         = 6;
            const int kRecoveryStripeCount = 3;
            const int kStripeSize          = 64 << 10;
            const int kStriperType         = KFS_STRIPED_FILE_TYPE_RS;
            return KfsClient::Open(
                inFileName.c_str(),
                inFlags,
                kReplicaCount,
                kStripeCount,
                kRecoveryStripeCount,
                kStripeSize,
                kStriperType,
                inMode
            );
        }
        return KfsClient::Open(
            inFileName.c_str(), inFlags, inParamsPtr->c_str(), inMode);
            
    }
    virtual int Close(
        int inFd)
    {
        return KfsClient::Close(inFd);
    }
    virtual ssize_t Read(
        int    inFd,
        void*  inBufPtr,
        size_t inBufSize)
    {
        return KfsClient::Read(inFd, (char*)inBufPtr, inBufSize);
    }
    virtual ssize_t Write( 
        int          inFd,
        const void*  inBufPtr,
        size_t       inBufSize)
    {
        return KfsClient::Write(inFd, (const char*)inBufPtr, inBufSize);
    }
    virtual int Sync(
        int inFd)
    {
        return KfsClient::Sync(inFd);
    }
    virtual int64_t Seek(
        int     inFd,
        int64_t inOffset,
        int     inWhence)
    {
        return KfsClient::Seek(inFd, inOffset, inWhence);
    }
    virtual int Stat(
        const string& inFileName,
        StatBuf&      outStat)
    {
        KfsFileAttr theAttr;
        const int theRet = KfsClient::Stat(inFileName.c_str(), theAttr);
        if (theRet == 0) {
            ToStat(theAttr, outStat);
        }
        return theRet;
    }
    virtual int Open(
        const string& inDirName,
        bool          inFetchAttributesFlag,
        DirIterator*& outDirIteratorPtr)
    {
        if (inFetchAttributesFlag) {
            vector<KfsFileAttr> theAttrs;
            const int theRet = ReaddirPlus(inDirName.c_str(), theAttrs);
            outDirIteratorPtr =
                theRet == 0 ? new KfsDirIterator(&theAttrs, 0) : 0;
            return theRet;
        }
        vector<string> theNames;
        const int theRet  = Readdir(inDirName.c_str(), theNames);
        outDirIteratorPtr = theRet == 0 ? new KfsDirIterator(0, &theNames) : 0;
        return theRet;
    }
    virtual int Close(
        DirIterator* inDirIteratorPtr)
    {
        if (! inDirIteratorPtr) {
            return -EINVAL;
        }
        delete static_cast<KfsDirIterator*>(inDirIteratorPtr);
        return 0;
    }
    virtual int Next(
        DirIterator*    inDirIteratorPtr,
        string&         outName,
        const StatBuf*& outStatPtr)
    {
        if (! inDirIteratorPtr) {
            outName.clear();
            outStatPtr = 0;
            return -EINVAL;
        }
        KfsDirIterator& theIt = *static_cast<KfsDirIterator*>(inDirIteratorPtr);
        theIt.Next(outName, outStatPtr);
        return 0;
    }
    virtual int Chmod(
        const string& inPathName,
        kfsMode_t     inMode)
    {
        return KfsClient::Chmod(inPathName.c_str(), inMode);
    }
    virtual int Chown(
        const string& inPathName,
        kfsUid_t      inOwner,
        kfsUid_t      inGroup)
    {
        return KfsClient::Chown(inPathName.c_str(), inOwner, inGroup);
    }
    virtual int GetUserAndGroupNames(
        kfsUid_t inUser,
        kfsGid_t inGroup,
        string&  outUserName,
        string&  outGroupName)
    {
        return KfsClient::GetUserAndGroupNames(
            inUser, inGroup, outUserName, outGroupName);
    }
    virtual int Glob(
        const string& inPattern,
        int           inFlags,
        int (*inErrFuncPtr) (const char* inErrPathPtr, int inErrno),
        glob_t*        inGlobPtr)
    {
        return KfsGlob(*this, inPattern.c_str(), inFlags,
            inErrFuncPtr, inGlobPtr);
    }
    virtual string StrError(
        int inError)
    {
        return ErrorCodeToStr(inError);
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

    /* virtual */ int
LocalFileSystem::GetUserAndGroupNames(
    kfsUid_t inUser,
    kfsGid_t inGroup,
    string&  outUserName,
    string&  outGroupName)
{
    QCStMutexLocker theLock(GetFsMutex());
    // For now use dummy un-connected kfs client.
    static KfsClient sClient;
    return sClient.GetUserAndGroupNames(
        inUser, inGroup, outUserName, outGroupName);
}

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

    string theScheme(theParts[2]);
    if (theScheme == "kfs") {
        theScheme = "qfs";
    } else if (theScheme.empty() || theScheme == "local") {
        theScheme = "file";
    }
    const string theAuthority(theParts[4]);
    const string theFragment (theParts[9]);
    if (outPathPtr) {
        *outPathPtr = theParts[5];
    }
    const string          theFsId(theScheme + ":" + theAuthority);
    FSMap::iterator const theIt = sFSMap.find(theFsId);
    if (theIt != sFSMap.end()) {
        outFsPtr = theIt->second;
        return 0;
    }
    FileSystemImpl* theImplPtr = 0;
    int             theRet     = 0;
    if (theScheme == "qfs") {
        KfsFileSystem* const theFsPtr = new KfsFileSystem();
        if ((theRet = theFsPtr->Init(theAuthority)) == 0) {
            theImplPtr = theFsPtr;
        } else {
            delete theFsPtr;
        }
    } else if (theScheme == "file") {
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
