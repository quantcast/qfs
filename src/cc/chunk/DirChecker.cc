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
#include "utils.h"
#include "Chunk.h"

#include "common/MsgLogger.h"
#include "common/StBuffer.h"
#include "common/time.h"
#include "common/RequestParser.h"
#include "common/IntToString.h"
#include "common/StdAllocator.h"

#include "qcdio/QCThread.h"
#include "qcdio/QCMutex.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"
#include "qcdio/qcdebug.h"

#include "kfsio/PrngIsaac64.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/file.h>

#include <utility>
#include <map>
#include <deque>

namespace KFS
{

using std::pair;
using std::make_pair;

class DirChecker::Impl : public QCRunnable
{
public:
    typedef DirChecker::LockFd    LockFd;
    typedef DirChecker::FileNames FileNames;
    typedef DirChecker::DirNames  DirNames;
    typedef QCMutex::Time         Time;
    enum {
        kTestIoBufferAlign = 4 << 10,
        kTestIoSize        = 8 * (4 << 10),
        kTestByte          = 0x55
    };

    Impl()
        : QCRunnable(),
          mDeviceIds(),
          mNextDevId(1),
          mDirInfos(),
          mSubDirNames(),
          mDontUseIfExistFileNames(),
          mIgnoreFileNames(),
          mAvailableDirs(),
          mThread(),
          mMutex(),
          mCond(),
          mDoneCond(),
          mCheckIntervalMicroSec(int64_t(60) * 1000 * 1000),
          mIoTimeoutSec(-1),
          mLockFileName(),
          mFsIdPrefix(),
          mDirLocks(),
          mFileSystemId(-1),
          mRemoveFilesFlag(false),
          mRunFlag(false),
          mDoneFlag(false),
          mSleepFlag(true),
          mUpdateDirInfosFlag(false),
          mRequireChunkHeaderChecksumFlag(false),
          mIgnoreErrorsFlag(false),
          mDeleteAllChaunksOnFsMismatchFlag(false),
          mMaxChunkFilesSampled(16),
          mRandom(),
          mChunkHeaderBuffer(),
          mTestIoBufferAllocPtr(new char[kTestIoBufferAlign + kTestIoSize]),
          mTestIoBufferPtr(mTestIoBufferAllocPtr +
            (unsigned int)kTestIoBufferAlign -
            (unsigned int)((mTestIoBufferAllocPtr - (char*)0) %
                kTestIoBufferAlign))
    {
        memset(mTestIoBufferPtr, kTestByte, kTestIoSize);
    }
    virtual ~Impl()
    {
        Impl::Stop();
        delete [] mTestIoBufferAllocPtr;
    }
    virtual void Run()
    {
        QCStMutexLocker theLocker(mMutex);
        DirInfos        theDirInfos                = mDirInfos;
        SubDirNames     theSubDirNames             = mSubDirNames;
        FileNames       theDontUseIfExistFileNames = mDontUseIfExistFileNames;
        FileNames       theIgnoreFileNames         = mIgnoreFileNames;
        string          theLockFileName;
        string          theFsIdPrefix;
        DirLocks        theDirLocks;
        mUpdateDirInfosFlag = false;
        int64_t         theLastCheckStartTime      = microseconds();
        while (mRunFlag) {
            if (mSleepFlag) {
                const int64_t theSleepMicroSec = (mCheckIntervalMicroSec -
                        (microseconds() - theLastCheckStartTime));
                if (0 < theSleepMicroSec) {
                    mCond.Wait(mMutex, (Time)theSleepMicroSec * Time(1000));
                }
            }
            if (! mRunFlag) {
                break;
            }
            bool theCheckDirsFlag = ! mSleepFlag;
            mSleepFlag = true;
            if (mUpdateDirInfosFlag) {
                theDirInfos                = mDirInfos;
                theSubDirNames             = mSubDirNames;
                theDontUseIfExistFileNames = mDontUseIfExistFileNames;
                theIgnoreFileNames         = mIgnoreFileNames;
                mUpdateDirInfosFlag = false;
            }
            const bool    theRemoveFilesFlag                  =
                mRemoveFilesFlag;
            const bool    theDeleteAllChaunksOnFsMismatchFlag =
                mDeleteAllChaunksOnFsMismatchFlag;
            const bool    theIgnoreErrorsFlag                 =
                mIgnoreErrorsFlag;
            const bool    theRequireChunkHeaderChecksumFlag   =
                mRequireChunkHeaderChecksumFlag;
            const int64_t theFileSystemId                     = mFileSystemId;
            const int     theIoTimeoutSec                     = mIoTimeoutSec;
            const size_t  theMaxChunkFilesSampled             =
                mMaxChunkFilesSampled;
            theLockFileName = mLockFileName;
            theFsIdPrefix   = mFsIdPrefix;
            DirsAvailable theAvailableDirs;
            theDirLocks.swap(mDirLocks);
            QCASSERT(mDirLocks.empty());
            {
                QCStMutexUnlocker theUnlocker(mMutex);
                const int64_t theNow = microseconds();
                theCheckDirsFlag = theCheckDirsFlag ||
                    mCheckIntervalMicroSec <=
                    ((theNow - theLastCheckStartTime) + 5 * 1000);
                theDirLocks.clear();
                if (theCheckDirsFlag) {
                    theLastCheckStartTime = theNow;
                    CheckDirs(
                        theDirInfos,
                        theSubDirNames,
                        theDontUseIfExistFileNames,
                        theIgnoreFileNames,
                        mDeviceIds,
                        mNextDevId,
                        theRemoveFilesFlag,
                        theIgnoreErrorsFlag,
                        theLockFileName,
                        theRequireChunkHeaderChecksumFlag,
                        mChunkHeaderBuffer,
                        theFsIdPrefix,
                        theFileSystemId,
                        theDeleteAllChaunksOnFsMismatchFlag,
                        theIoTimeoutSec,
                        mTestIoBufferPtr,
                        theMaxChunkFilesSampled,
                        mRandom,
                        theAvailableDirs
                    );
                }
                theUnlocker.Lock();
            }
            bool theUpdateDirInfosFlag = false;
            for (DirsAvailable::iterator theIt = theAvailableDirs.begin();
                    theIt != theAvailableDirs.end();
                    ) {
                if (mDirInfos.erase(theIt->first) <= 0) {
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
                    theUpdateDirInfosFlag = true;
                }
            }
            if (theUpdateDirInfosFlag) {
                theDirInfos = mDirInfos;
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
        const Time theInterval = Time(inMilliSeconds) * 1000;
        if (theInterval == mCheckIntervalMicroSec) {
            return;
        }
        mCond.Notify();
    }
    int GetInterval()
    {
        QCStMutexLocker theLocker(mMutex);
        return (int)(mCheckIntervalMicroSec / 1000);
    }
    void Clear()
    {
        QCStMutexLocker theLocker(mMutex);
        mUpdateDirInfosFlag = true;
        mDirInfos.clear();
        mAvailableDirs.clear();
    }
    bool Add(
        const string& inDirName,
        bool          inBufferedIoFlag,
        LockFdPtr*    inLockPtr)
    {
        QCStMutexLocker theLocker(mMutex);
        if (inLockPtr) {
            LockFdPtr& theLockPtr = *inLockPtr;
            if (theLockPtr) {
                mDirLocks.push_back(theLockPtr);
                theLockPtr.reset();
                mCond.Notify();
            }
        }
        if (inDirName.empty()) {
            return false;
        }
        const string theDirName = Normalize(inDirName);
        mUpdateDirInfosFlag = true;
        mAvailableDirs.erase(theDirName);
        pair<DirInfos::iterator, bool> const
            theRes = mDirInfos.insert(
                make_pair(theDirName, inBufferedIoFlag));
        theRes.first->second = inBufferedIoFlag;
        return theRes.second;
    }
    bool Remove(
        const string& inDirName)
    {
        if (inDirName.empty()) {
            return false;
        }
        const string theDirName = Normalize(inDirName);
        QCStMutexLocker theLocker(mMutex);
        mUpdateDirInfosFlag = true;
        mAvailableDirs.erase(theDirName);
        return (mDirInfos.erase(theDirName) != 0);
    }
    bool Remove(
        const DirNames& inDirNames)
    {
        QCStMutexLocker theLocker(mMutex);
        mUpdateDirInfosFlag = true;
        const size_t theSize = mDirInfos.size();
        for (DirNames::const_iterator theIt = inDirNames.begin();
                theIt != inDirNames.end();
                ++theIt) {
            if (theIt->empty()) {
                continue;
            }
            const string theDirName = Normalize(*theIt);
            mAvailableDirs.erase(theDirName);
            mDirInfos.erase(theDirName);
        }
        return (theSize > mDirInfos.size());
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
            mDirInfos.erase(theIt->first);
        }
        mUpdateDirInfosFlag = true;
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
        const string& inDirName,
        bool          inRemoveFilesFlag)
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
        mSubDirNames[Normalize(inDirName.substr(i))] = inRemoveFilesFlag;
        mUpdateDirInfosFlag = true;
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
        mUpdateDirInfosFlag = true;
    }
    void SetIgnoreFileNames(
        const FileNames& inFileNames)
    {
        QCStMutexLocker theLocker(mMutex);
        mIgnoreFileNames = inFileNames;
        mUpdateDirInfosFlag = true;
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
    void SetFsIdPrefix(
        const string& inFsIdPrefix)
    {
        QCStMutexLocker theLocker(mMutex);
        mFsIdPrefix = inFsIdPrefix;
    }
    void SetDeleteAllChaunksOnFsMismatch(
        int64_t inFsId,
        bool    inDeleteFlag)
    {
        QCStMutexLocker theLocker(mMutex);
        mFileSystemId                     = inFsId;
        mDeleteAllChaunksOnFsMismatchFlag = inDeleteFlag;
    }
    void SetIoTimeout(
        int inTimeoutSec)
    {
        QCStMutexLocker theLocker(mMutex);
        mIoTimeoutSec = inTimeoutSec;
    }
    void SetMaxChunkFilesSampled(
        int inValue)
    {
        QCStMutexLocker theLocker(mMutex);
        mMaxChunkFilesSampled = inValue < 0 ? 0 : (size_t)inValue;
    }
    int GetMaxChunkFilesSampled()
    {
        QCStMutexLocker theLocker(mMutex);
        return (int)mMaxChunkFilesSampled;
    }
    void Wakeup()
    {
        QCStMutexLocker theLocker(mMutex);
        mCond.Notify();
    }

private:
    typedef std::map<dev_t, DeviceId> DeviceIds;
    typedef std::deque<LockFdPtr>     DirLocks;
    typedef std::map<string, bool>    DirInfos;
    typedef std::map<string, bool>    SubDirNames;

    DeviceIds         mDeviceIds;
    DeviceId          mNextDevId;
    DirInfos          mDirInfos;
    SubDirNames       mSubDirNames;
    FileNames         mDontUseIfExistFileNames;
    FileNames         mIgnoreFileNames;
    DirsAvailable     mAvailableDirs;
    QCThread          mThread;
    QCMutex           mMutex;
    QCCondVar         mCond;
    QCCondVar         mDoneCond;
    int64_t           mCheckIntervalMicroSec;
    int               mIoTimeoutSec;
    string            mLockFileName;
    string            mFsIdPrefix;
    DirLocks          mDirLocks;
    int64_t           mFileSystemId;
    bool              mRemoveFilesFlag;
    bool              mRunFlag;
    bool              mDoneFlag;
    bool              mSleepFlag;
    bool              mUpdateDirInfosFlag;
    bool              mRequireChunkHeaderChecksumFlag;
    bool              mIgnoreErrorsFlag;
    bool              mDeleteAllChaunksOnFsMismatchFlag;
    size_t            mMaxChunkFilesSampled;
    PrngIsaac64       mRandom;
    ChunkHeaderBuffer mChunkHeaderBuffer;
    char* const       mTestIoBufferAllocPtr;
    char* const       mTestIoBufferPtr;

    static void CheckDirs(
        const DirInfos&    inDirInfos,
        const SubDirNames& inSubDirNames,
        const FileNames&   inDontUseIfExistFileNames,
        const FileNames&   inIgnoreFileNames,
        DeviceIds&         inDeviceIds,
        DeviceId&          ioNextDevId,
        bool               inRemoveFilesFlag,
        bool               inIgnoreErrorsFlag,
        const string&      inLockName,
        bool               inRequireChunkHeaderChecksumFlag,
        ChunkHeaderBuffer& inChunkHeaderBuffer,
        const string       inFsIdPrefix,
        int64_t            inFileSystemId,
        bool               inDeleteAllChaunksOnFsMismatchFlag,
        int                inIoTimeout,
        char*              inTestBufferPtr,
        size_t             inMaxChunkFilesSampled,
        PrngIsaac64&       inRandom,
        DirsAvailable&     outDirsAvailable)
    {
        for (DirInfos::const_iterator theIt = inDirInfos.begin();
                theIt != inDirInfos.end();
                ++theIt) {
            struct stat theStat = {0};
            if (stat(theIt->first.c_str(), &theStat) != 0 ||
                   ! S_ISDIR(theStat.st_mode)) {
                continue;
            }
            FileNames::const_iterator theEit =
                inDontUseIfExistFileNames.begin();
            for (theEit = inDontUseIfExistFileNames.begin();
                    theEit != inDontUseIfExistFileNames.end();
                    ++theEit) {
                string theFileName = theIt->first + *theEit;
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
            bool      theSupportsSpaceReservatonFlag = false;
            int       theIoTimeSec                   = -1;
            if (! inLockName.empty()) {
                const string theLockName = theIt->first + inLockName;
                const int    theLockFd   = TryLock(
                    theLockName,
                    theIt->second,
                    inTestBufferPtr,
                    theSupportsSpaceReservatonFlag,
                    theIoTimeSec);
                if (theLockFd < 0) {
                    KFS_LOG_STREAM_ERROR <<
                        theLockName << ": " <<
                        QCUtils::SysError(-theLockFd) <<
                    KFS_LOG_EOM;
                    continue;
                }
                theLockFdPtr.reset(new LockFd(theLockFd));
                if (0 < inIoTimeout && inIoTimeout < theIoTimeSec) {
                    KFS_LOG_STREAM_ERROR <<
                        theLockName << ": " <<
                        "test io time: "         << theIoTimeSec <<
                        " exceeded time limit: " << inIoTimeout  <<
                    KFS_LOG_EOM;
                    theLockFdPtr.reset();
                    continue;
                }
            }
            SubDirNames::const_iterator theSit;
            for (theSit = inSubDirNames.begin();
                    theSit != inSubDirNames.end();
                    ++theSit) {
                string theDirName = theIt->first + theSit->first;
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
                    if (inRemoveFilesFlag && theSit->second &&
                            Remove(theDirName, true) != 0) {
                        break;
                    }
                }
            }
            if (theSit != inSubDirNames.end()) {
                continue;
            }
            int64_t    theFsId = -1;
            ChunkInfos theChunkInfos;
            string     theFsIdPathName;
            if (GetChunkFiles(
                    theIt->first,
                    inLockName,
                    inIgnoreFileNames,
                    inRequireChunkHeaderChecksumFlag,
                    inRemoveFilesFlag,
                    inIgnoreErrorsFlag,
                    inFsIdPrefix,
                    inChunkHeaderBuffer,
                    inIoTimeout,
                    inMaxChunkFilesSampled,
                    inRandom,
                    theFsId,
                    theFsIdPathName,
                    theChunkInfos) != 0) {
                continue;
            }
            if (0 < inFileSystemId && 0 < theFsId &&
                    inFileSystemId != theFsId) {
                const int theCleanupFlag =
                    inDeleteAllChaunksOnFsMismatchFlag || theChunkInfos.IsEmpty();
                KFS_LOG_STREAM(theCleanupFlag ?
                    MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
                    theIt->first <<
                    " file system id: "             << theFsId <<
                    " does not match expected id: " << inFileSystemId <<
                    (theCleanupFlag ? " deleting all chunks" : "") <<
                KFS_LOG_EOM;
                if (! theCleanupFlag) {
                    continue;
                }
                string                    theName = theIt->first;
                const size_t              theSize = theName.size();
                ChunkInfos::ConstIterator theCIt(theChunkInfos);
                const ChunkInfo*          thePtr;
                char                      theBuf[32];
                char* const               theBufEndPtr =
                    theBuf + sizeof(theBuf) / sizeof(theBuf[0]) - 1;
                *theBufEndPtr = 0;
                while ((thePtr = theCIt.Next())) {
                    theName.resize(theSize);
                    theName += IntToDecString(thePtr->mFileId, theBufEndPtr);
                    theName += ".";
                    theName += IntToDecString(thePtr->mChunkId, theBufEndPtr);
                    theName += ".";
                    theName += IntToDecString(
                        thePtr->mChunkVersion, theBufEndPtr);
                    if (unlink(theName.c_str())) {
                        const int theErr = errno;
                        KFS_LOG_STREAM_ERROR <<
                            theName <<
                            " error: " << QCUtils::SysError(theErr) <<
                        KFS_LOG_EOM;
                        break;
                    }
                }
                if (thePtr) {
                    // Cleanup error.
                    continue;
                }
                if (! theFsIdPathName.empty() &&
                        unlink(theFsIdPathName.c_str())) {
                    const int theErr = errno;
                    KFS_LOG_STREAM_ERROR <<
                        theFsIdPathName <<
                        " error: " << QCUtils::SysError(theErr) <<
                    KFS_LOG_EOM;
                    continue;
                }
                theFsIdPathName.clear();
                theChunkInfos.Clear();
                theFsId = inFileSystemId;
            }
            if ((0 < inFileSystemId || 0 < theFsId) &&
                    theFsIdPathName.empty() &&
                    ! inFsIdPrefix.empty()) {
                string theName = theIt->first;
                theName += inFsIdPrefix;
                char        theBuf[32];
                char* const theBufEndPtr =
                    theBuf + sizeof(theBuf) / sizeof(theBuf[0]) - 1;
                *theBufEndPtr = 0;
                theName += IntToDecString(
                    0 < inFileSystemId ? inFileSystemId : theFsId,
                    theBufEndPtr
                );
                const int theFd = open(theName.c_str(),
                    O_CREAT|O_RDWR|O_TRUNC, 0644);
                if (theFd < 0 || close(theFd)) {
                    const int theErr = errno;
                    KFS_LOG_STREAM_ERROR <<
                        theName <<
                        " error: " << QCUtils::SysError(theErr) <<
                    KFS_LOG_EOM;
                    continue;
                }
            }
            pair<DeviceIds::iterator, bool> const theDevRes =
                inDeviceIds.insert(make_pair(theStat.st_dev, ioNextDevId));
            if (theDevRes.second) {
                ioNextDevId++;
            }
            pair<DirsAvailable::iterator, bool> const theDirRes =
                outDirsAvailable.insert(make_pair(theIt->first,
                    DirInfo(
                        theDevRes.first->second,
                        theLockFdPtr,
                        theIt->second,
                        theSupportsSpaceReservatonFlag,
                        theFsId
                    )));
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
        const string&      inFsIdPrefix,
        ChunkHeaderBuffer& inChunkHeaderBuffer,
        int                inIoTimeout,
        size_t             inMaxChunkFilesSampled,
        PrngIsaac64&       inRandom,
        int64_t&           outFileSystemId,
        string&            outFsIdPathName,
        ChunkInfos&        outChunkInfos)
    {
        QCASSERT(! inDirName.empty() && *(inDirName.rbegin()) == '/');
        outFileSystemId = -1;
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
        bool                 theReadMaxChunkFlag = false;
        size_t               theMaxChunkIndex    = 0;
        kfsChunkId_t         theMaxChunkId       = -1;
        size_t               theGoodCnt          = 0;
        size_t               theReadCnt          = 0;
        size_t               theFrontIdx         = 0;
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
            if (! inFsIdPrefix.empty() &&
                    inFsIdPrefix.length() < theName.length() &&
                    inFsIdPrefix.compare(0, string::npos, theName, 0,
                        inFsIdPrefix.length()) == 0) {
                int64_t     theFsId = -1;
                const char* thePtr  = theName.data() + inFsIdPrefix.length();
                if (DecIntParser::Parse(
                        thePtr,
                        theName.length() - inFsIdPrefix.length(),
                        theFsId) &&
                        theName.c_str() + theName.length() <= thePtr) {
                    if (0 < outFileSystemId && 0 < theFsId &&
                            theFsId != outFileSystemId) {
                        KFS_LOG_STREAM_ERROR << inDirName <<
                            " error: inconsistent file system id: " <<
                            theFsId << " vs " << outFileSystemId <<
                        KFS_LOG_EOM;
                        theErr = -EINVAL;
                        break;
                    }
                    outFileSystemId = theFsId;
                    outFsIdPathName = inDirName + theName;
                    continue;
                } else {
                    KFS_LOG_STREAM_ERROR << inDirName <<
                        " error: unable to parse file system id: " <<
                        theName <<
                    KFS_LOG_EOM;
                    theErr = -EINVAL;
                    break;
                }
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
            int64_t    theChunkFileFsId = -1;
            int        theIoTimeSec     = -1;
            bool       theReadFlag      = false;
            const bool kForceReadFlag   = false;
            theChunkInfo.mChunkSize = -1;
            if (! IsValidChunkFile(
                    inDirName,
                    theEntryPtr->d_name,
                    theBuf.st_size,
                    inRequireChunkHeaderChecksumFlag,
                    kForceReadFlag,
                    inChunkHeaderBuffer,
                    theChunkInfo.mFileId,
                    theChunkInfo.mChunkId,
                    theChunkInfo.mChunkVersion,
                    theChunkInfo.mChunkSize,
                    theChunkFileFsId,
                    theIoTimeSec,
                    theReadFlag)) {
                if (0 <= theChunkInfo.mChunkSize) {
                    // Invalid header or read error. Mark entry as invalid by
                    // making the size negative.
                    theChunkInfo.mChunkSize = -(theChunkInfo.mChunkSize + 1);
                    ChunkInfo& theBack = outChunkInfos.PushBack(theChunkInfo);
                    if (theReadCnt < inMaxChunkFilesSampled) {
                        Swap(theBack, outChunkInfos[theFrontIdx], theChunkInfo);
                        if (theReadMaxChunkFlag &&
                                theMaxChunkIndex == theFrontIdx) {
                            theMaxChunkIndex = outChunkInfos.GetSize() - 1;
                        }
                        theFrontIdx++;
                    }
                } else if (inRemoveFilesFlag && unlink(theName.c_str())) {
                    const int theCurErr = errno;
                    KFS_LOG_STREAM_ERROR <<
                        theName << ": " <<  QCUtils::SysError(theCurErr) <<
                    KFS_LOG_EOM;
                    if (! inIgnoreErrorsFlag) {
                        theErr = theCurErr;
                        break;
                    }
                }
                continue;
            }
            if (0 < theChunkFileFsId) {
                if (0 < outFileSystemId) {
                    if (outFileSystemId != theChunkFileFsId) {
                        KFS_LOG_STREAM_ERROR << theName <<
                            " error: inconsistent file system id: " <<
                            theChunkFileFsId << " vs " << outFileSystemId <<
                        KFS_LOG_EOM;
                        theErr = -EINVAL;
                        break;
                    }
                } else {
                    outFileSystemId = theChunkFileFsId;
                }
            }
            if (0 < inIoTimeout && inIoTimeout < theIoTimeSec) {
                KFS_LOG_STREAM_ERROR << theName <<
                    " error: io time: "      << theIoTimeSec <<
                    " exceeded time limit: " << inIoTimeout <<
                KFS_LOG_EOM;
                theErr = -ETIMEDOUT;
                break;
            }
            KFS_LOG_STREAM_DEBUG <<
                "adding: "  << theName <<
                " iotime: " << theIoTimeSec <<
            KFS_LOG_EOM;
            ChunkInfo& theBack = outChunkInfos.PushBack(theChunkInfo);
            theGoodCnt++;
            if (theMaxChunkId < theChunkInfo.mChunkId) {
                theMaxChunkId       = theChunkInfo.mChunkId;
                theReadMaxChunkFlag = ! theReadFlag;
                if (theReadMaxChunkFlag) {
                    theMaxChunkIndex = outChunkInfos.GetSize() - 1;
                }
            }
            if (theReadFlag && theReadCnt < inMaxChunkFilesSampled) {
                Swap(theBack, outChunkInfos[theFrontIdx], theChunkInfo);
                if (theReadMaxChunkFlag && theMaxChunkIndex == theFrontIdx) {
                    theMaxChunkIndex = outChunkInfos.GetSize() - 1;
                }
                theFrontIdx++;
                theReadCnt++;
            }
        }
        closedir(theDirStream);
        if (theErr != 0) {
            return theErr;
        }
        // Get fs id from the chunk with the largest id, which is likely the
        // most recently created chunk, and from randomly selected chunk files.
        for (; ;) {
            const size_t theCnt = min(inMaxChunkFilesSampled, theGoodCnt);
            size_t       theIdx;
            if (theReadMaxChunkFlag) {
                theIdx              = theMaxChunkIndex;
                theReadMaxChunkFlag = false;
            } else {
                if (theCnt <= theReadCnt) {
                    break;
                }
                const uint64_t theRand = inRandom.Rand();
                theIdx = theFrontIdx +
                    (size_t)(theRand % (theGoodCnt - theReadCnt));
            }
            QCASSERT(theIdx < outChunkInfos.GetSize());
            ChunkInfo& theCur = outChunkInfos[theIdx];
            theName.clear();
            AppendDecIntToString(theName, theCur.mFileId);
            theName += '.';
            AppendDecIntToString(theName, theCur.mChunkId);
            theName += '.';
            AppendDecIntToString(theName, theCur.mChunkVersion);
            int64_t    theChunkFileFsId = -1;
            int        theIoTimeSec     = -1;
            bool       theReadFlag      = false;
            const bool kForceReadFlag   = true;
            if (IsValidChunkFile(
                    inDirName,
                    theName.c_str(),
                    theCur.mChunkSize + KFS_CHUNK_HEADER_SIZE,
                    inRequireChunkHeaderChecksumFlag,
                    kForceReadFlag,
                    inChunkHeaderBuffer,
                    theChunkInfo.mFileId,
                    theChunkInfo.mChunkId,
                    theChunkInfo.mChunkVersion,
                    theChunkInfo.mChunkSize,
                    theChunkFileFsId,
                    theIoTimeSec,
                    theReadFlag
                    )) {
                if (0 < theChunkFileFsId) {
                    if (0 < outFileSystemId) {
                        if (outFileSystemId != theChunkFileFsId) {
                            KFS_LOG_STREAM_ERROR << theName <<
                                " error: inconsistent file system id: " <<
                                theChunkFileFsId << " vs " << outFileSystemId <<
                            KFS_LOG_EOM;
                            theErr = -EINVAL;
                            break;
                        }
                    } else {
                        outFileSystemId = theChunkFileFsId;
                    }
                }
                if (0 < inIoTimeout && inIoTimeout < theIoTimeSec) {
                    KFS_LOG_STREAM_ERROR << inDirName << theName <<
                        " error: io time: "      << theIoTimeSec <<
                        " exceeded time limit: " << inIoTimeout <<
                    KFS_LOG_EOM;
                    theErr = -ETIMEDOUT;
                    break;
                }
                theReadCnt++;
            } else {
                // Mark entry as invalid.
                QCRTASSERT(
                    0 <= theCur.mChunkId &&
                    theChunkInfo.mChunkId == theCur.mChunkId
                );
                theCur.mChunkSize = -(theCur.mChunkSize + 1);
                theGoodCnt--;
            }
            KFS_LOG_STREAM_DEBUG <<
                "read header: " << theReadCnt <<
                " good: "       << theGoodCnt <<
                " frontIdx: "   << theFrontIdx <<
                " chunk: "      << theCur.mChunkId <<
                " iotime: "     << theIoTimeSec <<
            KFS_LOG_EOM;
            if (theCnt <= theReadCnt || theGoodCnt <= theReadCnt) {
                break;
            }
            Swap(theCur, outChunkInfos[theFrontIdx], theChunkInfo);
            theFrontIdx++;
        }
        return theErr;
    }
    template<typename T>
    static void Swap(
        T& inLeft,
        T& inRight,
        T& inTmp)
    {
        if (&inLeft == &inRight) {
            return;
        }
        inTmp   = inLeft;
        inLeft  = inRight;
        inRight = inTmp;
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
    static int CheckSpaceReservationSupport(
        const string& inFileName,
        int           inFd,
        bool&         outSupportsSpaceReservationFlag)
    {
        outSupportsSpaceReservationFlag =
            QCUtils::IsReserveFileSpaceSupported(inFd);
        if (! outSupportsSpaceReservationFlag) {
            return 0;
        }
        const off_t   theTestSize = kTestIoSize;
        const int64_t theRet      =
            QCUtils::ReserveFileSpace(inFd, theTestSize);
        if (theRet != theTestSize) {
            KFS_LOG_STREAM_NOTICE <<
                inFileName <<
                ": " << QCUtils::SysError((int)-theRet) <<
                " will not use space reservation" <<
            KFS_LOG_EOM;
            outSupportsSpaceReservationFlag = false;
        } else {
            struct stat theStat = {0};
            if (ftruncate(inFd, theTestSize) || fstat(inFd, &theStat)) {
                const int theErr = errno;
                KFS_LOG_STREAM_ERROR <<
                    inFileName <<
                    ": " << QCUtils::SysError(theErr) <<
                KFS_LOG_EOM;
                close(inFd);
                return (theErr > 0 ? -theErr : (theErr < 0 ? theErr : -1));
            }
            outSupportsSpaceReservationFlag =
                theStat.st_size == theTestSize &&
                theTestSize <= (int64_t)theStat.st_blocks *
                theStat.st_blksize;
            if (! outSupportsSpaceReservationFlag) {
                KFS_LOG_STREAM_NOTICE <<
                    inFileName <<
                    ": "
                    " considering space reservation not supported: " <<
                    " size: " << theStat.st_size <<
                    " disk utilization: " <<
                        (int64_t)theStat.st_blocks * theStat.st_blksize <<
                KFS_LOG_EOM;
            }
        }
        return 0;
    }
    static int TryLock(
        const string& inFileName,
        bool          inBufferedIoFlag,
        char*         inTestIoBufferPtr,
        bool&         outSupportsSpaceReservationFlag,
        int&          outIoTestTime)
    {
        KFS_LOG_STREAM_DEBUG <<
            "lock: "   << inFileName <<
            " bufio: " << inBufferedIoFlag <<
        KFS_LOG_EOM;
        const int theFd = open(inFileName.c_str(), O_CREAT|O_RDWR, 0644);
        if (theFd < 0) {
            return (errno > 0 ? -errno : -1);
        }
        if (fcntl(theFd, F_SETFD, FD_CLOEXEC)) {
            const int theErr = errno;
            KFS_LOG_STREAM_ERROR <<
                inFileName <<
                ": " << QCUtils::SysError(theErr) <<
                " enabling FD_CLOEXEC" <<
            KFS_LOG_EOM;
        }
        if (flock(theFd, LOCK_EX | LOCK_NB)) {
            const int theErr = errno;
            close(theFd);
            return (theErr > 0 ? -theErr : -1);
        }
        const time_t theStart    = time(0);
        int          theErr      = 0;
        const string theFileName = inFileName + ".tmp";
        const int    theTmpFd    = open(theFileName.c_str(),
#ifdef O_DIRECT
            (inBufferedIoFlag ? 0 : O_DIRECT) |
#endif
            O_CREAT | O_RDWR | O_TRUNC, 0644);
        if (theTmpFd < 0) {
            theErr = errno;
            if (theErr == 0) {
                theErr = -1;
            }
        } else {
            theErr = CheckSpaceReservationSupport(
                theFileName, theTmpFd, outSupportsSpaceReservationFlag);
            if (theErr == 0) {
                const ssize_t theNWr = write(
                    theTmpFd, inTestIoBufferPtr, kTestIoSize);
                if (theNWr != kTestIoSize) {
                    if (theNWr < 0) {
                        theErr = errno;
                    } else {
                        theErr = EIO;
                    }
                    KFS_LOG_STREAM_ERROR << theFileName <<
                        ": write failure:"
                        " ret: " << theNWr <<
                        " "      << QCUtils::SysError(theErr) <<
                    KFS_LOG_EOM;
                } else {
                    off_t thePos;
                    if ((thePos = lseek(theTmpFd, 0, SEEK_SET)) != 0) {
                        theErr = errno;
                        if (theErr == 0) {
                            theErr = EIO;
                        }
                        KFS_LOG_STREAM_ERROR << theFileName <<
                            ": seek failure:"
                            " ret: " << thePos <<
                            " "      << QCUtils::SysError(theErr) <<
                        KFS_LOG_EOM;
                    } else {
                        const ssize_t theNRd = read(
                            theTmpFd, inTestIoBufferPtr, kTestIoSize);
                        if (theNRd != kTestIoSize) {
                            if (theNRd < 0) {
                                theErr = errno;
                            } else {
                                theErr = EIO;
                            }
                            KFS_LOG_STREAM_ERROR << theFileName <<
                                " read failure:"
                                " ret: " << theNRd <<
                                " "      << QCUtils::SysError(theErr) <<
                            KFS_LOG_EOM;
                        } else {
                            for (int i = 0; i < kTestIoSize; i++) {
                                if ((inTestIoBufferPtr[i] & 0xFF) !=
                                        kTestByte) {
                                     KFS_LOG_STREAM_ERROR << theFileName <<
                                        ": read data mismatch:"
                                        " pos: "    << i <<
                                        " byte: "   <<
                                            (inTestIoBufferPtr[i] & 0xFF) <<
                                        " expect: " << kTestByte <<
                                    KFS_LOG_EOM;
                                    theErr = EIO;
                                    memset(inTestIoBufferPtr,
                                        kTestByte, kTestIoSize);
                                    break;
                               }
                            }
                        }
                    }
                }
            }
        }
        if (0 <= theTmpFd) {
            if (close(theTmpFd) && theErr == 0) {
                theErr = errno;
                if (theErr == 0) {
                    theErr = -1;
                }
            }
            if (unlink(theFileName.c_str()) && theErr == 0) {
                theErr = errno;
                if (theErr == 0) {
                    theErr = -1;
                }
            }
        }
        outIoTestTime = time(0) - theStart;
        if (theErr) {
            close(theFd);
            return (theErr > 0 ? -theErr : (theErr < 0 ? theErr : -1));
        }
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
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

DirChecker::LockFd::~LockFd()
{
    if (mFd >= 0) {
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
DirChecker::Remove(
    const DirChecker::DirNames& inDirNames)
{
    return mImpl.Remove(inDirNames);
}

    bool
DirChecker::Add(
    const string& inDirName,
    bool          inBufferedIoFlag)
{
    return mImpl.Add(inDirName, inBufferedIoFlag, 0);
}

    bool
DirChecker::Add(
    const string&          inDirName,
    bool                   inBufferedIoFlag,
    DirChecker::LockFdPtr& ioLockFdPtr)
{
    return mImpl.Add(inDirName, inBufferedIoFlag, &ioLockFdPtr);
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
    DirChecker::DirsAvailable& outDirs)
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
    const string& inDirName,
    bool          inRemoveFilesFlag)
{
    mImpl.AddSubDir(inDirName, inRemoveFilesFlag);
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

    void
DirChecker::SetFsIdPrefix(
    const string& inFsIdPrefix)
{
    mImpl.SetFsIdPrefix(inFsIdPrefix);
}

    void
DirChecker::SetDeleteAllChaunksOnFsMismatch(
    int64_t inFsId,
    bool    inDeleteFlag)
{
    mImpl.SetDeleteAllChaunksOnFsMismatch(inFsId, inDeleteFlag);
}

    void
DirChecker::SetIoTimeout(
    int inTimeoutSec)
{
    mImpl.SetIoTimeout(inTimeoutSec);
}

    void
DirChecker::SetMaxChunkFilesSampled(
    int inValue)
{
    mImpl.SetMaxChunkFilesSampled(inValue);
}

    int
DirChecker::GetMaxChunkFilesSampled()
{
    return mImpl.GetMaxChunkFilesSampled();
}

    void
DirChecker::Wakeup()
{
    mImpl.Wakeup();
}

}
