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

#include "Trash.h"

#include "FileSystem.h"
#include "common/Properties.h"
#include "common/RequestParser.h"
#include "libclient/Path.h"

#include <string>

#include <errno.h>
#include <unistd.h>

namespace KFS
{
namespace tools
{
using std::string;
using KFS::client::Path;

class Trash::Impl
{
public:
    Impl(
        FileSystem& inFs)
        : mFs(inFs),
          mCurrent("Current"),
          mTrash(".Trash"),
          mHomePrefix("/user/"),
          mUserName(),
          mEmptierIntervalSec(60),
          mTrashPrefix(),
          mCurrentTrashPrefix(),
          mDirMode(0700),
          mStatus(0),
          mRetryCount(2)
    {
        mStatus = inFs.GetUserName(mUserName);
        SetParameters(Properties(), string());
    }
    ~Impl()
        {}
    int SetParameters(
        const Properties& inProperties,
        const string&     inPrefix)
    {
        mCurrent = inProperties.getValue(
            inPrefix + "current", mCurrent);
        mTrash   = inProperties.getValue(
            inPrefix + "trash", mCurrent);
        mHomePrefix = inProperties.getValue(
            inPrefix + "homePrefix", mHomePrefix);
        mEmptierIntervalSec = inProperties.getValue(
            inPrefix + "emptierIntervalSec", mEmptierIntervalSec);
        if (! mHomePrefix.empty() && *mHomePrefix.rbegin() != '/') {
            mHomePrefix += "/";
        }
        if (mEmptierIntervalSec <= 0) {
            mStatus = 0;
            return mStatus;
        }
        mHomePrefix = NormPath(mHomePrefix);
        if (mHomePrefix.empty() ||
                ! IsValidName(mCurrent) ||
                ! IsValidName(mTrash) ||
                ! IsValidName(mUserName)) {
            mStatus = -EINVAL;
        } else {
            mStatus = 0;
        }
        mHomePrefix += "/";
        mTrashPrefix        = mHomePrefix  + mTrash   + "/";
        mCurrentTrashPrefix = mTrashPrefix + mCurrent;
        return mStatus;
    }
    bool IsEnabled() const
        { return (mEmptierIntervalSec > 0); }
    int MoveTo(
        const string& inPath,
        bool&         outMovedFlag,
        string*       inErrMsgPtr)
    {
        outMovedFlag = false;
        if (mStatus || ! IsEnabled()) {
            return mStatus;
        }
        size_t thePathDepth = 0;
        string thePath      = NormPath(inPath, &thePathDepth);
        if (thePath.empty()) {
            return -EINVAL;
        }
        thePath += "/";
        if (thePath.length() <= mTrashPrefix.length()) {
            if (mTrashPrefix.compare(0, thePath.length(), thePath) == 0) {
                if (inErrMsgPtr) {
                    *inErrMsgPtr = "Cannot move \"" + inPath +
                        "\" to the trash, as it contains the trash";
                }
                return -EINVAL;
            }
        } else {
            if (thePath.compare(0, mTrashPrefix.length(), mTrashPrefix) == 0) {
                return 0; // Already in the trash.
            }
        }
        thePath.erase(thePath.size() - 1);
        const size_t theParentLen = thePath.rfind('/');
        if (theParentLen == string::npos) {
            if (inErrMsgPtr) {
                *inErrMsgPtr = "internal error \"" + inPath +
                    "\" => \"" + thePath + "\"";
            }
            return -EINVAL;
        }
        const string        theParent  = thePath.substr(0, theParentLen);
        string              theDestDir = mCurrentTrashPrefix + theParent;
        string              theDest    =
            theDestDir + "/"  + thePath.substr(theParentLen + 1);
        const size_t        theDestLen = theDest.length();
        int                 theRetry   = 0;
        FileSystem::StatBuf theStat;
        int                 theStatus;
        do {
            theStatus = Mkdir(theDestDir);
            if (theStatus != 0) {
                if (theStatus == -ENOTDIR) {
                    continue;
                }
                return theStatus;
            }
            int k = 0;
            while ((theStatus = mFs.Stat(theDest, theStat)) == 0) {
                theDest.resize(theDestLen);
                ostringstream theOs;
                theOs << "." << ++k;
                theDest += theOs.str();
            }
            if (theStatus != -ENOENT) {
                if (theStatus == -ENOTDIR) {
                    continue;
                }
                break;
            }
            if ((theStatus = mFs.Rename(thePath, theDest)) == 0) {
                break;
            }
            theDest.resize(theDestLen);
        } while (++theRetry <= mRetryCount);
        return theStatus;
    }
    int Expunge(
        ErrorHandler* inErrorHandlerPtr)
    {
        if (mStatus || ! IsEnabled()) {
            return mStatus;
        }
        FileSystem::DirIterator* theDirIt             = 0;
        const bool               kFetchAttributesFlag = false;
        int                      theStatus            =
            mFs.Open(mTrashPrefix, kFetchAttributesFlag, theDirIt);
        if (theStatus != 0) {
            return theStatus;
        }
        const FileSystem::StatBuf* theStatPtr    = 0;
        bool                       theSetExpFlag = true;
        time_t                     theExpTime    = 0;
        string                     theName;
        for (; ;) {
            theName.clear();
            if ((theStatus = mFs.Next(
                    theDirIt, theName, theStatPtr)) != 0) {
                return theStatus;
            }
            if (theName.empty()) {
                break;
            }
            if (theName == "." || theName == "..") {
                continue;
            }
            if (theName.length() != 2 * 5) {
                continue;
            }
            struct tm   theLocalTime = { 0 };
            const char* thePtr       = theName.c_str();
            if (
                    ! DecIntParser::Parse(thePtr, 2, theLocalTime.tm_year) ||
                    ! DecIntParser::Parse(thePtr, 2, theLocalTime.tm_mon) ||
                    ! DecIntParser::Parse(thePtr, 2, theLocalTime.tm_mday) ||
                    ! DecIntParser::Parse(thePtr, 2, theLocalTime.tm_hour) ||
                    ! DecIntParser::Parse(thePtr, 2, theLocalTime.tm_min)) {
                continue;
            }
            const time_t theTime = mktime(&theLocalTime);
            if (theTime == time_t(-1)) {
                continue;
            }
            if (theSetExpFlag) {
                theExpTime    = time(0) - mEmptierIntervalSec;
                theSetExpFlag = false;
            }
            if (theExpTime < theTime) {
                continue;
            }
            const string thePath        = mTrashPrefix + theName;
            const bool   kRecursiveFlag = true;
            theStatus = mFs.Remove(thePath, kRecursiveFlag, inErrorHandlerPtr);
            if (theStatus != 0) {
                break;
            }
        }
        mFs.Close(theDirIt);
        return theStatus;
    }
    int RunEmptier()
    {
        if (mStatus || ! IsEnabled()) {
            return mStatus;
        }
        return 0;
    }
private:
    FileSystem& mFs;
    string      mCurrent;
    string      mTrash;
    string      mHomePrefix;
    string      mUserName;
    time_t      mEmptierIntervalSec;
    string      mTrashPrefix;
    string      mCurrentTrashPrefix;
    Path        mPath;
    kfsMode_t   mDirMode;
    int         mStatus;
    int         mRetryCount;

    string NormPath(
        const string& inPath,
        size_t*       inPathDepthPtr = 0)
    {
        if (inPath.empty()) {
            return inPath;
        }
        string thePath;
        if (*inPath.begin() == '/') {
            mPath.Set(inPath.c_str(), inPath.length());
        } else {
            if (mFs.GetCwd(thePath) != 0 || thePath.empty()) {
                thePath.clear();
                return thePath;
            }
            if (*thePath.rbegin() != '/') {
                thePath += "/";
            }
            thePath += inPath;
            mPath.Set(thePath.c_str(), thePath.length());
        }
        if (inPathDepthPtr) {
            *inPathDepthPtr = mPath.size();
        }
        thePath = mPath.NormPath();
        mPath.Clear();
        return thePath;
    }
    bool StartsWith(
        const string& inString,
        const string& inPrefix)
    {
        return (inString.length() >= inPrefix.length() &&
            inString.compare(0, inPrefix.length(), inPrefix) == 0);
    }
    bool IsValidName(
        const string& inName)
    {
        return (
            ! inName.empty() &&
            inName != ".." &&
            inName != "." &&
            inName.find('/') == string::npos
        );
    }
    int Mkdir(
        string& ioPath)
    {
        string theDirName;
        theDirName.reserve(ioPath.size());
        FileSystem::StatBuf theStat;
        const char*         thePtr   = ioPath.c_str();
        const size_t theMinPrefixLen = mCurrentTrashPrefix.length();
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
            int theErr;
            for (int i = 0; ;) {
                int theErr = mFs.Stat(theDirName, theStat);
                if (theErr != 0 || S_ISDIR(theStat.st_mode) ||
                        theDirName.length() <= theMinPrefixLen) {
                    break;
                }
                ostringstream theOs;
                theOs << "." << ++i;
                theDirName += theOs.str();
            }
            const bool kCreateAllFlag = false;
            if ((theErr = mFs.Mkdir(
                    theDirName, mDirMode, kCreateAllFlag)) != 0) {
                return theErr;
            }
        }
        ioPath = theDirName;
        return 0;
    }
    int Checkpoint()
    {
        char   theBuf[128];
        const  time_t theTime      = time(0);
        struct tm     theLocalTime = {0};
        if (! localtime_r(&theTime, &theLocalTime) ||
                strftime(theBuf, sizeof(theBuf) / sizeof(theBuf[0]),
                    "%y%M%d%H%M", &theLocalTime) <= 0) {
            const int theErr = errno;
            return (theErr < 0 ? theErr : (theErr == 0 ? -EFAULT : theErr));
        }
        string theCheckpointDir = mTrashPrefix + theBuf;
        size_t theLen           = theCheckpointDir.length();
        int    theStatus        = 0;
        for (int k = 0; ;) {
            int theStatus = mFs.Rename(mCurrentTrashPrefix, theCheckpointDir);
            if (theStatus == 0 || theStatus != -EEXIST) {
                break;
            }
            theCheckpointDir.resize(theLen);
            ostringstream theOs;
            theOs << "." << ++k;
            theCheckpointDir += theOs.str();
        }
        if (theStatus == -ENOENT) {
            FileSystem::StatBuf theStat;
            const int theErr = mFs.Stat(mCurrentTrashPrefix, theStat);
            if (theErr == -ENOENT) {
                theStatus = 0;
            }
        }
        return theStatus;
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

Trash::Trash(
    FileSystem& inFs)
    : mImpl(*(new Impl(inFs)))
{
}

Trash::~Trash()
{
    delete &mImpl;
}

    int
Trash::SetParameters(
    const Properties& inProperties,
    const string&     inPrefix)
{
    return mImpl.SetParameters(inProperties, inPrefix);
}

    int
Trash::MoveTo(
    const string& inPath,
    bool&         outMovedFlag,
    string*       inErrMsgPtr)
{
    return mImpl.MoveTo(inPath, outMovedFlag, inErrMsgPtr);
}

    int
Trash::Expunge(
    Trash::ErrorHandler* inErrorHandlerPtr)
{
    return mImpl.Expunge(inErrorHandlerPtr);
}

    bool
Trash::IsEnabled() const
{
    return mImpl.IsEnabled();
}

    int
Trash::RunEmptier()
{
    return mImpl.RunEmptier();
}

}
}
