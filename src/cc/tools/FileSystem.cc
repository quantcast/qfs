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
#include "common/Properties.h"
#include "common/StBuffer.h"
#include "qcdio/QCMutex.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <utime.h>
#include <fcntl.h>
#include <errno.h>
#include <dirent.h>
#include <glob.h>
#include <pwd.h>

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
    FileSystemImpl(
        const string& inUri)
        : mUri(inUri)
        {}
    virtual ~FileSystemImpl()
        {}
    virtual const string& GetUri() const
        { return mUri; }
    virtual bool operator==(
        const FileSystem& inFs) const
        { return (this == &inFs || mUri == inFs.GetUri()); } // FIXME
    const string mUri;
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
            if (mFetchAttributesFlag) {
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
                    // theRetPtr = 0;
                } else {
                    outStatPtr = &mStatBuf;
                    if (S_ISDIR(mStatBuf.st_mode) && mStatBuf.st_size >= 0) {
                        mStatBuf.st_size = -(mStatBuf.st_size + 1);
                    }
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
    LocalFileSystem(
        const string& inUri)
        : FileSystemImpl(inUri)
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
        StatBuf&      outStatBuf)
    {
        outStatBuf.Reset();
        const int theStatus = Errno(stat(inFileName.c_str(), &outStatBuf));
        if (theStatus < 0) {
            return theStatus;
        }
        if (S_ISDIR(outStatBuf.st_mode) && outStatBuf.st_size >= 0) {
            outStatBuf.st_size = -(outStatBuf.st_size + 1);
        }
        return theStatus;
    }
    virtual int Stat(
        int      inFd,
        StatBuf& outStatBuf)
    {
        outStatBuf.Reset();
        const int theStatus = Errno(fstat(inFd, &outStatBuf));
        if (theStatus < 0) {
            return theStatus;
        }
        if (S_ISDIR(outStatBuf.st_mode) && outStatBuf.st_size >= 0) {
            outStatBuf.st_size = -(outStatBuf.st_size + 1);
        }
        return theStatus;
    }
    virtual int Open(
        const string& inDirName,
        bool          inFetchAttributesFlag,
        DirIterator*& outDirIteratorPtr)
    {
        outDirIteratorPtr = 0;
        DIR* const theDirPtr = opendir(inDirName.c_str());
        if (! theDirPtr) {
            return RetErrno(errno);
        }
        outDirIteratorPtr =
            new LocalDirIterator(inDirName, theDirPtr, inFetchAttributesFlag);
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
        const int theErr = theDirIt.GetError();
        return (theErr != 0 ? RetErrno(theErr) : 0);
    }
    virtual int Glob(
        const string& inPattern,
        int           inFlags,
        int (*inErrFuncPtr) (const char* inErrPathPtr, int inErrno),
        glob_t*        inGlobPtr)
    {
        return glob(inPattern.c_str(), inFlags, inErrFuncPtr, inGlobPtr);
    }
    template<typename T>
    int RecursivelyApplySelf(
        string& inPath,
        T&      inFunctor)
    {
        const size_t thePrevSize = inPath.size();
        DIR* const theDirPtr     = opendir(inPath.c_str());
        if (! theDirPtr) {
            return inFunctor(inPath, true, RetErrno(errno));
        }
        inPath += "/";
        int                  theRet = 0;
        const struct dirent* thePtr;
        while ((thePtr = readdir(theDirPtr))) {
            if (strcmp(thePtr->d_name, ".") == 0 ||
                    strcmp(thePtr->d_name, "..") == 0) {
                continue;
            }
            inPath.erase(thePrevSize + 1);
            inPath += thePtr->d_name;
            bool theDirFlag;
#ifdef QC_OS_NAME_CYGWIN
            // Cygwin has no d_type, all other supported platforms have the file
            // type
            struct stat theStat;
            theDirFlag = stat(inPath.c_str(), &theStat) == 0 &&
                S_ISDIR(theStat.st_mode);
#else
            theDirFlag = thePtr->d_type == DT_DIR;
#endif
            theRet = theDirFlag ?
                RecursivelyApplySelf(inPath, inFunctor) :
                inFunctor(inPath, theDirFlag, 0);
            if (theRet != 0) {
                break;
            }
        }
        closedir(theDirPtr);
        inPath.erase(thePrevSize);
        return (theRet == 0 ? inFunctor(inPath, true, theRet) : theRet);
    }
    template<typename T>
    int RecursivelyApply(
        const string& inPath,
        T&            inFunctor)
    {
        struct stat theStat = {0};
        if (stat(inPath.c_str(), &theStat)) {
            return RetErrno(errno);
        }
        if (S_ISDIR(theStat.st_mode)) {
            string thePath;
            thePath.reserve(MAX_PATH_NAME_LENGTH);
            thePath.assign(inPath.data(), inPath.length());
            return RecursivelyApplySelf(thePath, inFunctor);
        }
        return inFunctor(inPath, false);
    }
    template<typename T> class FunctorT
    {
    public:
        FunctorT(
            T&            inFunctor,
            ErrorHandler* inErrorHandlerPtr)
            : mFunctor(inFunctor),
              mErrorHandlerPtr(inErrorHandlerPtr),
              mStatus(0)
            {}
        int operator()(
            const string& inPath,
            bool          inDirectoryFlag,
            int           inErrno = 0)
        {
            if (inErrno != 0) {
                mStatus = inErrno;
                const int theRet = HandleError(inPath, inErrno);
                if (theRet != 0) {
                    return theRet;
                }
            }
            int theRet = Errno(mFunctor(inPath, inDirectoryFlag));
            if (theRet != 0) {
                theRet = HandleError(inPath, theRet);
                if (theRet != 0) {
                    mStatus = theRet;
                }
            }
            return theRet;
        }
        int GetStatus() const
            { return mStatus; }
    private:
        T&                  mFunctor;
        ErrorHandler* const mErrorHandlerPtr;
        int                 mStatus;

        int HandleError(
            const string& inPath,
            int           inStatus)
        {
            return (mErrorHandlerPtr ?
                (*mErrorHandlerPtr)(inPath, inStatus) : inStatus);
        }
    };
    class ChmodFunctor
    {
    public:
        ChmodFunctor(
            kfsMode_t inMode)
            : mMode((mode_t)inMode)
            {}
        int operator()(
            const string& inPath,
            bool          /* inDirectoryFlag */)
        {
            return chmod(inPath.c_str(), mMode);
        }
    private:
        const mode_t mMode;
    };
    virtual int Chmod(
        const string& inPathName,
        kfsMode_t     inMode,
        bool          inRecursiveFlag,
        ErrorHandler* inErrorHandlerPtr)
    {
        if (inRecursiveFlag) {
            ChmodFunctor           theChmodFunc(inMode);
            FunctorT<ChmodFunctor> theFunc(theChmodFunc, inErrorHandlerPtr);
            const int theStatus = RecursivelyApply(inPathName, theFunc);
            return (theStatus == 0 ? theFunc.GetStatus() : theStatus);
        }
        return Errno(chmod(inPathName.c_str(), (mode_t)inMode));
    }
    class ChownFunctor
    {
    public:
        ChownFunctor(
            kfsUid_t inUid,
            kfsGid_t inGid)
            : mUid((uid_t)inUid),
              mGid((gid_t)inGid)
            {}
        int operator()(
            const string& inPath,
            bool          /* inDirectoryFlag */)
        {
            return chown(inPath.c_str(), mUid, mGid);
        }
     private:
        const uid_t mUid;
        const gid_t mGid;
    };
    virtual int Chown(
        const string& inPathName,
        kfsUid_t      inOwner,
        kfsGid_t      inGroup,
        bool          inRecursiveFlag,
        ErrorHandler* inErrorHandlerPtr)
    {
        if (inRecursiveFlag) {
            ChownFunctor           theChownFunc(inOwner, inGroup);
            FunctorT<ChownFunctor> theFunc(theChownFunc, inErrorHandlerPtr);
            const int theStatus = RecursivelyApply(inPathName, theFunc);
            return (theStatus == 0 ? theFunc.GetStatus() : theStatus);
        }
        return Errno(chown(inPathName.c_str(), (uid_t)inOwner, (gid_t)inGroup));
    }
    virtual int Rmdir(
        const string& inPathName)
    {
        return Errno(rmdir(inPathName.c_str()));
    }
    class RemoveFunctor
    {
    public:
        RemoveFunctor()
            {}
        int operator()(
            const string& inPath,
            bool          inDirectoryFlag)
        {
            return (inDirectoryFlag ?
                rmdir(inPath.c_str()) : unlink(inPath.c_str()));
        }
    };
    virtual int Remove(
        const string& inPathName,
        bool          inRecursiveFlag,
        ErrorHandler* inErrorHandlerPtr)
    {
        if (inRecursiveFlag) {
            RemoveFunctor           theRemoveFunc;
            FunctorT<RemoveFunctor> theFunc(theRemoveFunc, inErrorHandlerPtr);
            const int theStatus = RecursivelyApply(inPathName, theFunc);
            return (theStatus == 0 ? theFunc.GetStatus() : theStatus);
        }
        return Errno(remove(inPathName.c_str()));
    }
    virtual int Mkdir(
        const string& inPathName,
        kfsMode_t     inMode,
        bool          inCreateAllFlag)
    {
        if (inCreateAllFlag) {
            string theDirName;
            theDirName.reserve(inPathName.size());
            const char* thePtr   = inPathName.c_str();
            struct stat theStat  = {0};
            const int   kNoEntry = RetErrno(ENOENT);
            while (*thePtr) {
                if (*thePtr == '/') {
                    while (thePtr[1] == '/') {
                        ++thePtr;
                    }
                }
                const char* const theCurPtr = thePtr;
                if (*thePtr == '/') {
                    ++thePtr;
                }
                while (*thePtr && *thePtr != '/') {
                    ++thePtr;
                }
                if (theCurPtr == thePtr) {
                    break;
                }
                const size_t theSize = thePtr - theCurPtr;
                if (theSize == 1 && *theCurPtr == '.') {
                    continue;
                }
                theDirName.append(theCurPtr, theSize);
                if (theSize == 2 &&
                        theCurPtr[0] == '.' && theCurPtr[1] == '.') {
                    continue;
                }
                int theErr = Errno(stat(theDirName.c_str(), &theStat));
                if (theErr == 0) {
                    if (! S_ISDIR(theStat.st_mode)) {
                        return RetErrno(ENOTDIR);
                    }
                } else {
                    if (theErr != kNoEntry) {
                        return theErr;
                    }
                    if ((theErr = Errno(
                            mkdir(theDirName.c_str(), inMode & 0777))) != 0) {
                        return theErr;
                    }
                }
            }
            return 0;
        }
        return Errno(mkdir(inPathName.c_str(), (mode_t)inMode));
    }
    virtual int Rename(
        const string& inSrcName,
        const string& inDstName)
    {
        return Errno(rename(inSrcName.c_str(), inDstName.c_str()));
    }
    virtual int SetUMask(
        mode_t inUMask)
    {
        umask(inUMask & 0777);
        return 0;
    }
    virtual int GetUMask(
        mode_t& outUMask)
    {
        outUMask = umask(0);
        umask(outUMask);
        return 0;
    }
    virtual int GetUserAndGroupNames(
        kfsUid_t inUser,
        kfsGid_t inGroup,
        string&  outUserName,
        string&  outGroupName)
    {
        return GetKfsClient().GetUserAndGroupNames(
            inUser, inGroup, outUserName, outGroupName);
    }
    virtual int GetUserAndGroupIds(
        const string& inUserName,
        const string& inGroupName,
        kfsUid_t&     outUserId,
        kfsGid_t&     outGroupId)
    {
        return GetKfsClient().GetUserAndGroupIds(
            inUserName.c_str(), inGroupName.c_str(), outUserId, outGroupId);
    }
    virtual int GetUserName(
        string& outUserName)
    {
        kfsUid_t theUid = (kfsUid_t)getuid();
        string   theGroupName;
        return GetKfsClient().GetUserAndGroupNames(
            theUid, kKfsGroupNone, outUserName, theGroupName);
    }
    virtual int SetMtime(
        const string&         inPath,
        const struct timeval& inMTime)
    {
        struct utimbuf theTimes;
        theTimes.actime  = inMTime.tv_sec;
        theTimes.modtime = inMTime.tv_sec;
        return Errno(utime(inPath.c_str(), &theTimes));
    }
    virtual int SetReplication(
        const string& inPath,
        const int     inReplication,
        bool          /* inRecursiveFlag */,
        ErrorHandler* /* inErrorHandlerPtr */)
    {
        StatBuf theStat;
        const int theStatus = Stat(inPath, theStat);
        if (theStatus != 0) {
            return theStatus;
        }
        return (inReplication != 0 ? -EINVAL : 0);
    }
    virtual int GetReplication(
        const string& inPath,
        StatBuf&      outStat,
        int&          outMinReplication,
        int&          outMaxReplication)
    {
        const int theRet = Stat(inPath, outStat);
        if (theRet != 0) {
            return theRet;
        }
        if (S_ISDIR(outStat.st_mode)) {
            return -EISDIR;
        }
        outMinReplication = 1;
        outMaxReplication = 1;
        return theRet;
    }
    virtual string StrError(
        int inError) const
    {
        return QCUtils::SysError(inError < 0 ? -inError : inError);
    }
    static int Errno(
        int inVal)
    {
        if (inVal >= 0) {
            return inVal;
        }
        return RetErrno(errno);
    }
    virtual int GetHomeDirectory(
        string& outHomeDir)
    {
        outHomeDir.clear();
        const long theBufSize = sysconf(_SC_GETPW_R_SIZE_MAX);
        if (theBufSize < 0) {
            return RetErrno(errno);
        }
        StBufferT<char, 1> theBuf;
        char* const        theBufPtr = theBuf.Resize(theBufSize);
        struct passwd      thePwd    = {0};
        struct passwd*     thePwdPtr = 0;
        if (getpwuid_r(
                    getuid(),
                    &thePwd,
                    theBufPtr,
                    (size_t)theBufSize,
                    &thePwdPtr
                ) != 0 || ! thePwdPtr || ! thePwdPtr->pw_dir) {
            return RetErrno(errno);
        }
        outHomeDir = thePwdPtr->pw_dir;
        return 0;
    }
private:
    static int RetErrno(
        int inErrno)
    {
        return (inErrno == 0 ? -1 : (inErrno < 0 ? inErrno : -inErrno));
    }
    static KfsClient& GetKfsClient();
private:
    LocalFileSystem(
        const LocalFileSystem& inFileSystem);
    LocalFileSystem& operator=(
        const LocalFileSystem& inFileSystem);
};

class KfsFileSystem : public FileSystemImpl,
    private KfsClient
{
public:
    typedef FileSystem::ErrorHandler ErrorHandler;

    static void ToStat(
        const KfsFileAttr& inAttr,
        StatBuf&           outStatBuf)
    {
        inAttr.ToStat(outStatBuf);
        outStatBuf.mSubCount1          = inAttr.subCount1;
        outStatBuf.mSubCount2          = inAttr.subCount2;
        outStatBuf.mNumReplicas        = inAttr.numReplicas;
        outStatBuf.mNumStripes         = inAttr.numStripes;
        outStatBuf.mNumRecoveryStripes = inAttr.numRecoveryStripes;
        outStatBuf.mStriperType        = inAttr.striperType;
        outStatBuf.mStripeSize         = inAttr.stripeSize;
        if (inAttr.fileSize > 0) {
            outStatBuf.st_size = inAttr.fileSize;
        }
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
                const KfsFileAttr& theAttr = mAttrs[mCur++];
                outName = theAttr.filename;
                ToStat(theAttr, mStatBuf);
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
    KfsFileSystem(
        const string& inUri)
        : FileSystemImpl(inUri),
          KfsClient()
        {}
    virtual ~KfsFileSystem()
        {}
    int Init(
        const string& theHostPort)
    {
        int          thePort = 20000;
        const size_t thePos  = theHostPort.find(':');
        if (thePos != string::npos) {
            char* theEndPtr = 0;
            thePort = (int)strtol(
                theHostPort.c_str() + thePos + 1, &theEndPtr, 10);
            if (! theEndPtr || *theEndPtr != 0) {
                return -EINVAL;
            }
        }
        int theRet = KfsClient::Init(
            theHostPort.substr(0, thePos), thePort);
        if (theRet != 0) {
            return theRet;
        }
        string theHomeDir;
        theRet = GetHomeDirectory(theHomeDir);
        if (theRet != 0) {
            return theRet;
        }
        return KfsClient::SetCwd(theHomeDir.c_str());
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
        } else {
            outStat.Reset();
        }
        return theRet;
    }
    virtual int Stat(
        int      inFd,
        StatBuf& outStat)
    {
        KfsFileAttr theAttr;
        const int theRet = KfsClient::Stat(inFd, theAttr);
        if (theRet == 0) {
            ToStat(theAttr, outStat);
        } else {
            outStat.Reset();
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
        kfsMode_t     inMode,
        bool          inRecursiveFlag,
        ErrorHandler* inErrorHandlerPtr)
    {
        return (inRecursiveFlag ?
            KfsClient::ChmodR(inPathName.c_str(), inMode, inErrorHandlerPtr) :
            KfsClient::Chmod(inPathName.c_str(), inMode)
        );
    }
    virtual int Chown(
        const string& inPathName,
        kfsUid_t      inOwner,
        kfsGid_t      inGroup,
        bool          inRecursiveFlag,
        ErrorHandler* inErrorHandlerPtr)
    {
        return (inRecursiveFlag ?
            KfsClient::ChownR(inPathName.c_str(), inOwner, inGroup,
                inErrorHandlerPtr) :
            KfsClient::Chown(inPathName.c_str(), inOwner, inGroup)
        );
    }
    virtual int Rmdir(
        const string& inPathName)
    {
        return KfsClient::Rmdir(inPathName.c_str());
    }
    virtual int Remove(
        const string& inPathName,
        bool          inRecursiveFlag,
        ErrorHandler* inErrorHandlerPtr)
    {
        const bool  kComputeFileSizeFlag = false;
        KfsFileAttr theAttr;
        int         theRes = KfsClient::Stat(
            inPathName.c_str(), theAttr, kComputeFileSizeFlag);
        if (theRes != 0) {
            return theRes;
        }
        if (theAttr.isDirectory) {
            if (inRecursiveFlag) {
                return KfsClient::Rmdirs(inPathName.c_str(), inErrorHandlerPtr);
            }
            return KfsClient::Rmdir(inPathName.c_str());
        }
        return KfsClient::Remove(inPathName.c_str());
    }
    virtual int Mkdir(
        const string& inPathName,
        kfsMode_t     inMode,
        bool          inCreateAllFlag)
    {
        if (inCreateAllFlag) {
            return KfsClient::Mkdirs(inPathName.c_str(), inMode);
        }
        return KfsClient::Mkdir(inPathName.c_str(), inMode);
    }
    virtual int Rename(
        const string& inSrcName,
        const string& inDstName)
    {
        const bool kOverwriteFlag = true;
        return KfsClient::Rename(inSrcName.c_str(), inDstName.c_str(),
            kOverwriteFlag);
    }
    virtual int SetUMask(
        mode_t inUMask)
    {
        KfsClient::SetUMask(((kfsMode_t)inUMask) & 0777);
        return 0;
    }
    virtual int GetUMask(
        mode_t& outUMask)
    {
        outUMask = KfsClient::GetUMask();
        return 0;
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
    virtual int GetUserAndGroupIds(
        const string& inUserName,
        const string& inGroupName,
        kfsUid_t&     outUserId,
        kfsGid_t&     outGroupId)
    {
        return KfsClient::GetUserAndGroupIds(
            inUserName.c_str(), inGroupName.c_str(), outUserId, outGroupId);
    }
    virtual int GetUserName(
        string& outUserName)
    {
        kfsUid_t theUid = KfsClient::GetUserId();
        string   theGroupName;
        return KfsClient::GetUserAndGroupNames(
            theUid, kKfsGroupNone, outUserName, theGroupName);
    }
    virtual int SetMtime(
        const string&         inPath,
        const struct timeval& inMTime)
    {
        return KfsClient::SetMtime(inPath.c_str(), inMTime);
    }
    virtual int SetReplication(
        const string& inPath,
        const int     inReplication,
        bool          inRecursiveFlag,
        ErrorHandler* inErrorHandlerPtr)
    {
        if (inReplication <= 0 || inReplication > 0x7FFF) {
            return -EINVAL;
        }
        return (inRecursiveFlag ?
            KfsClient::SetReplicationFactorR(
                inPath.c_str(), (int16_t)inReplication, inErrorHandlerPtr) :
            KfsClient::SetReplicationFactor(
                inPath.c_str(), (int16_t)inReplication)
        );
    }
    virtual int GetReplication(
        const string& inPath,
        StatBuf&      outStat,
        int&          outMinReplication,
        int&          outMaxReplication)
    {
        KfsFileAttr theAttr;
        const int theRet = KfsClient::GetReplication(
            inPath.c_str(),
            theAttr,
            outMinReplication,
            outMaxReplication
        );
        if (theRet == 0) {
            ToStat(theAttr, outStat);
        } else {
            outStat.Reset();
        }
        return theRet;
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
        int inError) const
    {
        return ErrorCodeToStr(inError);
    }
    virtual int GetHomeDirectory(
        string& outHomeDir)
    {
        string theUserName;
        const int theStatus = GetUserName(theUserName);
        if (theStatus != 0) {
            outHomeDir.clear();
            return theStatus;
        }
        outHomeDir = "/user/" + theUserName;
        return theStatus;
    }
private:
    KfsFileSystem(
        const KfsFileSystem& inFileSystem);
    KfsFileSystem& operator=(
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

    static KfsClient&
GetKfsClient(
    const Properties* inConfigPtr = 0)
{
    QCStMutexLocker theLock(GetFsMutex());
    // For now use un-connected kfs client for uid / gid to name
    // conversions.
    static bool      sCreatedFlag = false;
    static KfsClient sClient;
    if (sCreatedFlag) {
        return sClient;
    }
    sCreatedFlag = true;
    if (inConfigPtr) {
        kfsUid_t theEUser  = kKfsUserNone;
        kfsGid_t theEGroup = kKfsGroupNone;
        theEUser  = inConfigPtr->getValue("fs.euser",  theEUser);
        theEGroup = inConfigPtr->getValue("fs.egroup", theEGroup);
        if (theEUser != kKfsUserNone || theEGroup != kKfsGroupNone) {
            sClient.SetEUserAndEGroup(theEUser, theEGroup, 0, 0);
        }
    }
    return sClient;
}
    /* static */ KfsClient&
LocalFileSystem::GetKfsClient()
{
    return tools::GetKfsClient();
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

    static string&
GetDefaultFsUri()
{
    static string sDefaultFsUri;
    return sDefaultFsUri;
}

    /* static */ int
FileSystem::SetDefault(
    const string&     inUri,
    const Properties* inPropertiesPtr /* = 0 */)
{
    QCStMutexLocker theLock(GetFsMutex());
    FileSystem* theFsPtr = 0;
    string      thePath;
    const int theRet = Get(inUri, theFsPtr, &thePath, inPropertiesPtr);
    if (theRet == 0) {
        if (! thePath.empty()) {
            theFsPtr->Chdir(thePath);
        }
        GetDefaultFsUri() = theFsPtr->GetUri();
    }
    return theRet;
}

    /* static */ int
FileSystem::Get(
    const string&     inUri,
    FileSystem*&      outFsPtr,
    string*           outPathPtr /* = 0 */,
    const Properties* inPropertiesPtr /* = 0 */)
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
    if(! regex_match(inUri, theParts, kFsUriRegex) ||
            theParts.size() < 10) {
        return -EINVAL;
    }
    if (outPathPtr) {
        *outPathPtr = theParts[5];
    }
    string theScheme(theParts[2]);
    if (theScheme.empty()) {
        const string& theDefaultUri = GetDefaultFsUri();
        if (theDefaultUri.empty()) {
            theScheme = "file";
        }  else {
            return Get(theDefaultUri, outFsPtr, 0);
        }
    } else if (theScheme == "kfs") {
        theScheme = "qfs";
    } else if (theScheme == "local") {
        theScheme = "file";
    }
    string theAuthority(theParts[4]);
    if (theScheme == "file" && ! theAuthority.empty()) {
        if (outPathPtr) {
            *outPathPtr = theAuthority + *outPathPtr;
        }
        theAuthority.clear();
    }
    // const string theFragment (theParts[9]);
    const string          theFsUri(theScheme + "://" + theAuthority);
    FSMap::iterator const theIt = sFSMap.find(theFsUri);
    if (theIt != sFSMap.end()) {
        outFsPtr = theIt->second;
        return 0;
    }
    FileSystemImpl* theImplPtr = 0;
    int             theRet     = 0;
    // Set user and group for all kfs clients: SetEUserAndEGroup() only has
    // effect only when only one kfs client with no open files exists.
    GetKfsClient(inPropertiesPtr);
    if (theScheme == "qfs") {
        KfsFileSystem* const theFsPtr = new KfsFileSystem(theFsUri);
        if ((theRet = theFsPtr->Init(theAuthority)) == 0) {
            theImplPtr = theFsPtr;
        } else {
            delete theFsPtr;
        }
    } else if (theScheme == "file") {
        theImplPtr = new LocalFileSystem(theFsUri);
    }
    if (theRet == 0 && ! theImplPtr) {
        theRet = -EINVAL;
    }
    if (theRet == 0 &&
            ! sFSMap.insert(make_pair(theFsUri, theImplPtr)).second) {
        QCRTASSERT(! "fs map insertion: duplicate entry");
    }
    outFsPtr = theImplPtr;
    return theRet;
}

    /* static */ string
FileSystem::GetStrError(
    int               inError,
    const FileSystem* inFsPtr /* = 0 */)
{
    return (inFsPtr ? inFsPtr->StrError(inError) : QCUtils::SysError(-inError));
}

} //namespace tools
} //namespace KFS
