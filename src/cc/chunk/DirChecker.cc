//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2011/7/10
// Author: Mike Ovsiannikov
//
// Copyright 2011-2012 Quantcast Corp.
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
// \file DirChecker.cc
// \brief thread periodically checks if directories / drives re-appear.
//
//----------------------------------------------------------------------------

#include "DirChecker.h"
#include "common/MsgLogger.h"
#include "common/StBuffer.h"
#include "common/time.h"
#include "qcdio/QCThread.h"
#include "qcdio/QCMutex.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"
#include "qcdio/qcdebug.h"
#include "utils.h"
#include "Chunk.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#ifndef KFS_DONT_USE_FLOCK
#include <sys/file.h>
#endif

#include <utility>
#include <map>
#include <deque>
#include <sstream>

namespace KFS
{

using std::pair;
using std::make_pair;
using std::ostringstream;

class DirChecker::Impl : public QCRunnable
{
public:
    typedef DirChecker::LockFd    LockFd;
    typedef DirChecker::FileNames FileNames;
    typedef DirChecker::DirNames  DirNames;
    typedef QCMutex::Time         Time;

    Impl()
        : QCRunnable(),
          mDeviceIds(),
          mNextDevId(1),
          mDirNames(),
          mSubDirNames(),
          mDontUseIfExistFileNames(),
          mIgnoreFileNames(),
          mAvailableDirs(),
          mThread(),
          mMutex(),
          mCond(),
          mDoneCond(),
          mCheckIntervalNanoSec(Time(60) * 1000 * 1000 * 1000),
          mLockFileName(),
          mDirLocks(),
          mRemoveFilesFlag(false),
          mRunFlag(false),
          mDoneFlag(false),
          mSleepFlag(true),
          mUpdateDirNamesFlag(false),
          mRequireChunkHeaderChecksumFlag(false),
          mIgnoreErrorsFlag(false),
          mChunkHeaderBuffer()
        {}
    virtual ~Impl()
        { Impl::Stop(); }
    virtual void Run()
    {
        const string    theLockToken = CreateLockToken();

        QCStMutexLocker theLocker(mMutex);
        DirNames        theDirNames                = mDirNames;
        DirNames        theSubDirNames             = mSubDirNames;
        FileNames       theDontUseIfExistFileNames = mDontUseIfExistFileNames;
        FileNames       theIgnoreFileNames         = mIgnoreFileNames;
        string          theLockFileName;
        DirLocks        theDirLocks;
        mUpdateDirNamesFlag = false;
        while (mRunFlag) {
            if (mSleepFlag) {
                mCond.Wait(mMutex, mCheckIntervalNanoSec);
            }
            if (! mRunFlag) {
                break;
            }
            mSleepFlag = true;
            if (mUpdateDirNamesFlag) {
                theDirNames                = mDirNames;
                theSubDirNames             = mSubDirNames;
                theDontUseIfExistFileNames = mDontUseIfExistFileNames;
                theIgnoreFileNames         = mIgnoreFileNames;
                mUpdateDirNamesFlag = false;
            }
            const bool theRemoveFilesFlag = mRemoveFilesFlag;
            theLockFileName = mLockFileName;
            DirsAvailable theAvailableDirs;
            theDirLocks.swap(mDirLocks);
            QCASSERT(mDirLocks.empty());
            const bool theIgnoreErrorsFlag = mIgnoreErrorsFlag;
            const bool theRequireChunkHeaderChecksumFlag =
                mRequireChunkHeaderChecksumFlag;
            {
                QCStMutexUnlocker theUnlocker(mMutex);
                theDirLocks.clear();
                CheckDirs(
                    theDirNames,
                    theSubDirNames,
                    theDontUseIfExistFileNames,
                    theIgnoreFileNames,
                    mDeviceIds,
                    mNextDevId,
                    theAvailableDirs,
                    theRemoveFilesFlag,
                    theIgnoreErrorsFlag,
                    theLockFileName,
                    theLockToken,
                    theRequireChunkHeaderChecksumFlag,
                    mChunkHeaderBuffer
                );
            }
            bool theUpdateDirNamesFlag = false;
            for (DirsAvailable::iterator theIt = theAvailableDirs.begin();
                    theIt != theAvailableDirs.end();
                    ) {
                if (mDirNames.erase(theIt->first) <= 0) {
                    if (mAvailableDirs.empty()) {
                        theAvailableDirs.erase(theIt++);
                    } else {
                        ++theIt;
                    }
                } else {
                    if (! mAvailableDirs.empty()) {
                        mAvailableDirs.insert(*theIt);
                    }
                    ++theIt;
                    theUpdateDirNamesFlag = true;
                }
            }
            if (theUpdateDirNamesFlag) {
                theDirNames = mDirNames;
            }
            if (mAvailableDirs.empty()) {
                mAvailableDirs.swap(theAvailableDirs);
            } else {
                theAvailableDirs.clear();
            }
            mDoneFlag = true;
            mDoneCond.Notify();
        }
    }
    void SetInterval(
        int inMilliSeconds)
    {
        QCStMutexLocker theLocker(mMutex);
        const Time theInterval = Time(inMilliSeconds) * 1000 * 1000;
        if (theInterval == mCheckIntervalNanoSec) {
            return;
        }
        const Time theWaitThreshold = mCheckIntervalNanoSec / 4;
        mCheckIntervalNanoSec = theInterval;
        if (theInterval > theWaitThreshold) {
            return;
        }
        mCond.Notify();
    }
    int GetInterval()
    {
        QCStMutexLocker theLocker(mMutex);
        return (int)(mCheckIntervalNanoSec / (1000 * 1000));
    }
    void Clear()
    {
        QCStMutexLocker theLocker(mMutex);
        mUpdateDirNamesFlag = true;
        mDirNames.clear();
        mAvailableDirs.clear();
    }
    bool Add(
        const string& inDirName,
        LockFdPtr*    inLockPtr)
    {
        QCStMutexLocker theLocker(mMutex);
        if (inLockPtr) {
            LockFdPtr& theLockPtr = *inLockPtr;
            if (theLockPtr) {
                mDirLocks.push_back(theLockPtr);
                theLockPtr.reset();
            }
        }
        if (inDirName.empty()) {
            return false;
        }
        const string theDirName = Normalize(inDirName);
        mUpdateDirNamesFlag = true;
        mAvailableDirs.erase(theDirName);
        return mDirNames.insert(theDirName).second;
    }
    bool Remove(
        const string& inDirName)
    {
        if (inDirName.empty()) {
            return false;
        }
        const string theDirName = Normalize(inDirName);
        QCStMutexLocker theLocker(mMutex);
        mUpdateDirNamesFlag = true;
        mAvailableDirs.erase(theDirName);
        return (mDirNames.erase(theDirName) != 0);
    }
    bool Add(
        const DirNames& inDirNames)
    {
        QCStMutexLocker theLocker(mMutex);
        mUpdateDirNamesFlag = true;
        const size_t theSize = mDirNames.size();
        for (DirNames::const_iterator theIt = inDirNames.begin();
                theIt != inDirNames.end();
                ++theIt) {
            if (theIt->empty()) {
                continue;
            }
            const string theDirName = Normalize(*theIt);
            mAvailableDirs.erase(theDirName);
            mDirNames.insert(theDirName);
        }
        return (theSize < mDirNames.size());
    }
    bool Remove(
        const DirNames& inDirNames)
    {
        QCStMutexLocker theLocker(mMutex);
        mUpdateDirNamesFlag = true;
        const size_t theSize = mDirNames.size();
        for (DirNames::const_iterator theIt = inDirNames.begin();
                theIt != inDirNames.end();
                ++theIt) {
            if (theIt->empty()) {
                continue;
            }
            const string theDirName = Normalize(*theIt);
            mAvailableDirs.erase(theDirName);
            mDirNames.erase(theDirName);
        }
        return (theSize > mDirNames.size());
    }
    void GetNewlyAvailable(
        DirsAvailable& outDirs,
        bool           inSyncFlag)
    {
        QCStMutexLocker theLocker(mMutex);
        if (inSyncFlag && mRunFlag) {
            mDoneFlag  = false;
            mSleepFlag = false;
            mCond.Notify();
            while (! mDoneFlag && mRunFlag) {
                mDoneCond.Wait(mMutex);
            }
        }
        if (mAvailableDirs.empty()) {
            return;
        }
        for (DirsAvailable::const_iterator theIt = mAvailableDirs.begin();
                theIt != mAvailableDirs.end();
                ++theIt) {
            mDirNames.erase(theIt->first);
        }
        mUpdateDirNamesFlag = true;
        if (outDirs.empty()) {
            outDirs.swap(mAvailableDirs);
            return;
        }
        outDirs.insert(mAvailableDirs.begin(), mAvailableDirs.end());
        mAvailableDirs.clear();
    }
    void Start(
        DirsAvailable& outDirs)
    {
        {
            QCStMutexLocker theLocker(mMutex);
            if (! mRunFlag) {
                mRunFlag = true;
                const int kStackSize = 32 << 10;
                mThread.Start(this, kStackSize);
            }
        }
        GetNewlyAvailable(outDirs, true);
    }
    void Stop()
    {
        {
            QCStMutexLocker theLocker(mMutex);
            mRunFlag = false;
            mCond.Notify();
        }
        mThread.Join();
    }
    void AddSubDir(
        const string& inDirName)
    {
        const size_t theLen = inDirName.length();
        size_t i = 0;
        while (i < theLen && inDirName[i] == '/') {
            i++;
        }
        if (theLen <= i) {
            return;
        }
        QCStMutexLocker theLocker(mMutex);
        mSubDirNames.insert(Normalize(inDirName.substr(i)));
        mUpdateDirNamesFlag = true;
    }
    void SetDontUseIfExist(
        const FileNames& inFileNames)
    {
        QCStMutexLocker theLocker(mMutex);
        mDontUseIfExistFileNames.clear();
        for (FileNames::const_iterator it = inFileNames.begin();
                it != inFileNames.end();
                ++it) {
            if (it->empty()) {
                continue;
            }
            mDontUseIfExistFileNames.insert(*it);
        }
        mUpdateDirNamesFlag = true;
    }
    void SetIgnoreFileNames(
        const FileNames& inFileNames)
    {
        QCStMutexLocker theLocker(mMutex);
        mIgnoreFileNames = inFileNames;
        mUpdateDirNamesFlag = true;
    }
    void SetRemoveFilesFlag(
        bool inFlag)
    {
        QCStMutexLocker theLocker(mMutex);
        mRemoveFilesFlag = inFlag;
    }
    void SetLockFileName(
        const string& inName)
    {
        QCStMutexLocker theLocker(mMutex);
        mLockFileName = inName;
    }
    void SetRequireChunkHeaderChecksumFlag(
        bool inFlag)
    {
        QCStMutexLocker theLocker(mMutex);
        mRequireChunkHeaderChecksumFlag = inFlag;
    }
    void SetIgnoreErrorsFlag(
        bool inFlag)
    {
        QCStMutexLocker theLocker(mMutex);
        mIgnoreErrorsFlag = inFlag;
    }

private:
    typedef std::map<dev_t, DeviceId> DeviceIds;
    typedef std::deque<LockFdPtr>     DirLocks;

    DeviceIds         mDeviceIds;
    DeviceId          mNextDevId;
    DirNames          mDirNames;
    DirNames          mSubDirNames;
    FileNames         mDontUseIfExistFileNames;
    FileNames         mIgnoreFileNames;
    DirsAvailable     mAvailableDirs;
    QCThread          mThread;
    QCMutex           mMutex;
    QCCondVar         mCond;
    QCCondVar         mDoneCond;
    Time              mCheckIntervalNanoSec;
    string            mLockFileName;
    DirLocks          mDirLocks;
    bool              mRemoveFilesFlag;
    bool              mRunFlag;
    bool              mDoneFlag;
    bool              mSleepFlag;
    bool              mUpdateDirNamesFlag;
    bool              mRequireChunkHeaderChecksumFlag;
    bool              mIgnoreErrorsFlag;
    ChunkHeaderBuffer mChunkHeaderBuffer;

    static void CheckDirs(
        const DirNames&    inDirNames,
        const DirNames&    inSubDirNames,
        const FileNames&   inDontUseIfExistFileNames,
        const FileNames&   inIgnoreFileNames,
        DeviceIds&         inDeviceIds,
        DeviceId&          ioNextDevId,
        DirsAvailable&     outDirsAvailable,
        bool               inRemoveFilesFlag,
        bool               inIgnoreErrorsFlag,
        const string&      inLockName,
        const string&      inLockToken,
        bool               inRequireChunkHeaderChecksumFlag,
        ChunkHeaderBuffer& inChunkHeaderBuffer)
    {
        for (DirNames::const_iterator theIt = inDirNames.begin();
                theIt != inDirNames.end();
                ++theIt) {
            struct stat theStat = {0};
            if (stat(theIt->c_str(), &theStat) != 0 ||
                   ! S_ISDIR(theStat.st_mode)) {
                continue;
            }
            FileNames::const_iterator theEit =
                inDontUseIfExistFileNames.begin();
            for (theEit = inDontUseIfExistFileNames.begin();
                    theEit != inDontUseIfExistFileNames.end();
                    ++theEit) {
                string theFileName = *theIt + *theEit;
                if (stat(theFileName.c_str(), &theStat) == 0) {
                    break;
                }
                const int theSysErr = errno;
                if (theSysErr != ENOENT) {
                    KFS_LOG_STREAM_ERROR <<
                        "stat " << theFileName << ": " <<
                        QCUtils::SysError(errno) <<
                    KFS_LOG_EOM;
                    break;
                }
            }
            if (theEit != inDontUseIfExistFileNames.end()) {
                continue;
            }
            LockFdPtr theLockFdPtr;
            if (! inLockName.empty()) {
                const string theLockName = *theIt + inLockName;
                const int    theLockFd   = TryLock(theLockName, inLockToken);
                if (theLockFd < 0) {
                    KFS_LOG_STREAM_ERROR <<
                        theLockName << ": " <<
                        QCUtils::SysError(-theLockFd) <<
                    KFS_LOG_EOM;
                    continue;
                }
                theLockFdPtr.reset(new LockFd(theLockFd));
            }
            DirNames::const_iterator theSit;
            for (theSit = inSubDirNames.begin();
                    theSit != inSubDirNames.end();
                    ++theSit) {
                string theDirName = *theIt + *theSit;
                if (mkdir(theDirName.c_str(), 0755)) {
                    if (errno != EEXIST) {
                        KFS_LOG_STREAM_ERROR <<
                            "mkdir " << theDirName << ": " <<
                            QCUtils::SysError(errno) <<
                        KFS_LOG_EOM;
                        break;
                    }
                    if (stat(theDirName.c_str(), &theStat) != 0) {
                        KFS_LOG_STREAM_ERROR <<
                            theDirName << ": " <<
                            QCUtils::SysError(errno) <<
                        KFS_LOG_EOM;
                        break;
                    }
                    if (! S_ISDIR(theStat.st_mode)) {
                        KFS_LOG_STREAM_ERROR <<
                            theDirName << ": " <<
                            " not a directory" <<
                        KFS_LOG_EOM;
                        break;
                    }
                    if (inRemoveFilesFlag && Remove(theDirName, true) != 0) {
                        break;
                    }
                }
            }
            if (theSit != inSubDirNames.end()) {
                continue;
            }
            ChunkInfos theChunkInfos;
            if (GetChunkFiles(
                    *theIt,
                    inLockName,
                    inIgnoreFileNames,
                    inRequireChunkHeaderChecksumFlag,
                    inRemoveFilesFlag,
                    inIgnoreErrorsFlag,
                    inChunkHeaderBuffer,
                    theChunkInfos) != 0) {
                continue;
            }
            pair<DeviceIds::iterator, bool> const theDevRes =
                inDeviceIds.insert(make_pair(theStat.st_dev, ioNextDevId));
            if (theDevRes.second) {
                ioNextDevId++;
            }
            pair<DirsAvailable::iterator, bool> const theDirRes =
                outDirsAvailable.insert(make_pair(*theIt, DirInfo(
                        theDevRes.first->second, theLockFdPtr)));
            if (! theChunkInfos.IsEmpty() && theDirRes.second) {
                theChunkInfos.Swap(theDirRes.first->second.mChunkInfos);
            }
        }
    }
    static int GetChunkFiles(
        const string&      inDirName,
        const string&      inLockName,
        const FileNames&   inIgnoreFileNames,
        bool               inRequireChunkHeaderChecksumFlag,
        bool               inRemoveFilesFlag,
        bool               inIgnoreErrorsFlag,
        ChunkHeaderBuffer& inChunkHeaderBuffer,
        ChunkInfos&        outChunkInfos)
    {
        QCASSERT(! inDirName.empty() && *(inDirName.rbegin()) == '/');
        int theErr = 0;
        DIR* const theDirStream = opendir(inDirName.c_str());
        if (! theDirStream) {
            theErr = errno;
            KFS_LOG_STREAM_ERROR <<
                "unable to open " << inDirName <<
                " error: " << QCUtils::SysError(theErr) <<
                KFS_LOG_EOM;
            return (inIgnoreErrorsFlag ? 0 : theErr);
        }
        struct dirent const* theEntryPtr;
        ChunkInfo            theChunkInfo;
        string               theName;
        theName.reserve(1024);
        while ((theEntryPtr = readdir(theDirStream))) {
            if (strcmp(theEntryPtr->d_name, ".") == 0 ||
                    strcmp(theEntryPtr->d_name, "..") == 0 ||
                    inLockName == theEntryPtr->d_name) {
                continue;
            }
            theName = theEntryPtr->d_name;
            if (inIgnoreFileNames.find(theName) != inIgnoreFileNames.end()) {
                continue;
            }
            theName = inDirName;
            theName += theEntryPtr->d_name;
            struct stat  theBuf  = { 0 };
            if (stat(theName.c_str(), &theBuf) != 0) {
                theErr = errno;
                KFS_LOG_STREAM_ERROR <<
                    theName << ": " <<  QCUtils::SysError(theErr) <<
                KFS_LOG_EOM;
                if (inIgnoreErrorsFlag) {
                    theErr = 0;
                    continue;
                }
                break;
            }
            if (S_ISDIR(theBuf.st_mode) || ! S_ISREG(theBuf.st_mode)) {
                continue;
            }
            if (! IsValidChunkFile(
                    inDirName,
                    theEntryPtr->d_name,
                    theBuf.st_size,
                    inRequireChunkHeaderChecksumFlag,
                    inChunkHeaderBuffer,
                    theChunkInfo.mFileId,
                    theChunkInfo.mChunkId,
                    theChunkInfo.mChunkVersion,
                    theChunkInfo.mChunkSize)) {
                if (inRemoveFilesFlag && unlink(theName.c_str())) {
                    theErr = errno;
                    KFS_LOG_STREAM_ERROR <<
                        theName << ": " <<  QCUtils::SysError(theErr) <<
                    KFS_LOG_EOM;
                    if (! inIgnoreErrorsFlag) {
                        break;
                    }
                }
                continue;
            }
            KFS_LOG_STREAM_DEBUG <<
                "adding: " << theName <<
            KFS_LOG_EOM;
            outChunkInfos.PushBack(theChunkInfo);
        }
        closedir(theDirStream);
        return theErr;
    }
    static int Remove(
        const string& inDirName,
        bool          inRecursiveFlag,
        const char*   inExcludeNamePtr = "")
    {
        QCASSERT(! inDirName.empty() && *(inDirName.rbegin()) == '/');
        if (inDirName == "/") {
            KFS_LOG_STREAM_ERROR <<
                "attempt to delete " << inDirName << " denied" <<
                KFS_LOG_EOM;
            return -EPERM;
        }
        int theErr = 0;
        DIR* const theDirStream = opendir(inDirName.c_str());
        if (! theDirStream) {
            theErr = errno;
            KFS_LOG_STREAM_ERROR <<
                "unable to open " << inDirName <<
                " error: " << QCUtils::SysError(theErr) <<
                KFS_LOG_EOM;
            return theErr;
        }
        struct dirent const* theEntryPtr;
        while ((theEntryPtr = readdir(theDirStream))) {
            if (strcmp(theEntryPtr->d_name, ".") == 0 ||
                    strcmp(theEntryPtr->d_name, "..") == 0 ||
                    strcmp(theEntryPtr->d_name, inExcludeNamePtr) == 0) {
                continue;
            }
            const string theName = inDirName + theEntryPtr->d_name;
            struct stat  theBuf  = { 0 };
            if (stat(theName.c_str(), &theBuf) == 0 &&
                    S_ISDIR(theBuf.st_mode)) {
                if (! inRecursiveFlag) {
                    continue;
                }
                Remove(theName, inRecursiveFlag);
            }
            KFS_LOG_STREAM_DEBUG <<
                "removing: " << theName <<
            KFS_LOG_EOM;
            if ((S_ISDIR(theBuf.st_mode) ?
                    rmdir(theName.c_str()) :
                    unlink(theName.c_str())) && errno != ENOENT) {
                theErr = errno;
                KFS_LOG_STREAM_ERROR <<
                    "unable to remove " << theName <<
                    " error: " << QCUtils::SysError(theErr) <<
                KFS_LOG_EOM;
                break;
            }
        }
        closedir(theDirStream);
        return theErr;
    }
    static int TryLock(
        const string& inFileName,
        const string& inLockToken)
    {
	const int theFd = open(inFileName.c_str(), O_CREAT|O_RDWR, 0644);
	if (theFd < 0) {
		return (errno > 0 ? -errno : -1);
	}
        if (fcntl(theFd, FD_CLOEXEC, 1)) {
            const int theErr = errno;
            KFS_LOG_STREAM_ERROR <<
                inFileName <<
                ": " << QCUtils::SysError(theErr) <<
                " enabling FD_CLOEXEC" <<
            KFS_LOG_EOM;
        }
#ifdef KFS_DONT_USE_FLOCK
	struct flock theLock = { 0 };
	theLock.l_type   = F_WRLCK;
	theLock.l_whence = SEEK_SET;
	if (fcntl(theFd, F_SETLK, &theLock)) {
	    const int theErr = errno;
	    close(theFd);
	    return (theErr > 0 ? -theErr : -1);
	}
        const size_t        theLen = inLockToken.length();
        StBufferT<char, 64> theBuf;
        char* const         theBufPtr = theBuf.Resize(theLen + 1);
        const ssize_t       theNRd    = read(theFd, theBufPtr, theLen + 1);
        if (theNRd < 0) {
	    const int theErr = errno;
	    close(theFd);
	    return (theErr > 0 ? -theErr : -1);
        }
        if ((size_t)theNRd == theLen &&
                memcmp(inLockToken.data(), theBufPtr, theLen) == 0) {
            close(theFd);
            return -EACCES;
        }
        if (lseek(theFd, 0, SEEK_SET) != 0 ||
                write(theFd, inLockToken.data(), theLen) != (ssize_t)theLen ||
                ((size_t)theNRd > theLen && ftruncate(theFd, theLen) != 0)) {
	    const int theErr = errno;
	    close(theFd);
	    return (theErr > 0 ? -theErr : -1);
        }
#else
        if (flock(theFd, LOCK_EX | LOCK_NB)) {
	    const int theErr = errno;
	    close(theFd);
	    return (theErr > 0 ? -theErr : -1);
        }
#endif
	return theFd;
    }
    static string Normalize(
        const string& inDirName)
    {
        const size_t theInitialLen = inDirName.length();
        size_t theLen = theInitialLen;
        while (theLen > 0 && inDirName[theLen - 1] == '/') {
            --theLen;
        }
        if (theInitialLen == theLen + 1) {
            return inDirName;
        } else if (theInitialLen > theLen) {
            return inDirName.substr(0, theLen + 1);
        }
        return (inDirName + "/");
    }
    static string CreateLockToken()
    {
        ostringstream theStream;
        theStream << getpid() <<
            " " << microseconds() <<
            " " << GetRandomSeq() <<
        "\n";
        return theStream.str();
    }

private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

DirChecker::LockFd::~LockFd()
{
    if (mFd >= 0) {
#ifdef KFS_DONT_USE_FLOCK
        ftruncate(mFd, 0);
#endif
        close(mFd);
    }
}

DirChecker::DirChecker()
    : mImpl(*(new Impl()))
{
}

DirChecker::~DirChecker()
{
    delete &mImpl;
}

    void
DirChecker::Clear()
{
    return mImpl.Clear();
}

    bool
DirChecker::Add(
    const DirNames& inDirNames)
{
    return mImpl.Add(inDirNames);
}

    bool
DirChecker::Remove(
    const DirNames& inDirNames)
{
    return mImpl.Remove(inDirNames);
}

    bool
DirChecker::Add(
    const string& inDirName)
{
    return mImpl.Add(inDirName, 0);
}

    bool
DirChecker::Add(
    const string&          inDirName,
    DirChecker::LockFdPtr& ioLockFdPtr)
{
    return mImpl.Add(inDirName, &ioLockFdPtr);
}

    bool
DirChecker::Remove(
    const string& inDirName)
{
    return mImpl.Remove(inDirName);
}

    void
DirChecker::GetNewlyAvailable(
    DirsAvailable& outDirs,
    bool           inSyncFlag /* = false */)
{
    mImpl.GetNewlyAvailable(outDirs, inSyncFlag);
}

    void
DirChecker::Start(
    DirsAvailable& outDirs)
{
    mImpl.Start(outDirs);
}

    void
DirChecker::Stop()
{
    mImpl.Stop();
}

    void
DirChecker::SetInterval(
    int inTimeMilliSec)
{
    mImpl.SetInterval(inTimeMilliSec);
}

    int
DirChecker::GetInterval()
{
    return mImpl.GetInterval();
}

    void
DirChecker::AddSubDir(
    const string& inDirName)
{
    mImpl.AddSubDir(inDirName);
}

    void
DirChecker::SetDontUseIfExist(
    const DirChecker::FileNames& inFileNames)
{
    mImpl.SetDontUseIfExist(inFileNames);
}

    void
DirChecker::SetIgnoreFileNames(
    const DirChecker::FileNames& inFileNames)
{
    mImpl.SetIgnoreFileNames(inFileNames);
}

    void
DirChecker::SetLockFileName(
    const string& inName)
{
    mImpl.SetLockFileName(inName);
}

    void
DirChecker::SetRemoveFilesFlag(
    bool inFlag)
{
    mImpl.SetRemoveFilesFlag(inFlag);
}

    void
DirChecker::SetRequireChunkHeaderChecksumFlag(
    bool inFlag)
{
    mImpl.SetRequireChunkHeaderChecksumFlag(inFlag);
}

    void
DirChecker::SetIgnoreErrorsFlag(
    bool inFlag)
{
    mImpl.SetIgnoreErrorsFlag(inFlag);
}

}
