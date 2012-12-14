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
    int Expunge()
    {
        if (mStatus || ! IsEnabled()) {
            return mStatus;
        }
        return 0;
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
        const string& inPath)
    {
        return 0;
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
Trash::Expunge()
{
    return mImpl.Expunge();
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
