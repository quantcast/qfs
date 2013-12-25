//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/12/16
// Author: Mike Ovsainnikov
//
// Copyright 2013 Quantcast Corp.
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
// User and group information retrieval from host os, and lookup class.
//
//----------------------------------------------------------------------------

#include "UserAndGroup.h"
#include "common/Properties.h"
#include "common/LinearHash.h"
#include "common/MsgLogger.h"
#include "common/hsieh_hash.h"
#include "kfsio/ITimeout.h"
#include "qcdio/QCThread.h"
#include "qcdio/QCMutex.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"

#include <string>
#include <set>

#include <errno.h>
#include <grp.h>
#include <pwd.h>

namespace KFS
{

using std::string;
using std::set;
using std::less;

class UserAndGroup::Impl : public QCRunnable, public ITimeout
{
private:
    typedef set<
        string,
        less<string>,
        StdFastAllocator<string>
    > UserExcludes;
    typedef UserExcludes GroupExcludes;
public:
    Impl()
        : QCRunnable(),
          ITimeout(),
          mUpdateCount(0),
          mCurUpdateCount(0),
          mThread(),
          mMutex(),
          mCond(),
          mStopFlag(),
          mUpdateFlag(),
          mUpdatePeriodNanoSec(QCMutex::Time(2) * 1000 * 1000 * 1000),
          mMinUserId(0),
          mMaxUserId(~kfsUid_t(0)),
          mMinGroupId(0),
          mMaxGroupId(~kfsGid_t(0)),
          mUserExcludes(),
          mGroupExcludes(),
          mParametersReadCount(0),
          mUidNameMap(),
          mGidNameMap(),
          mNameUidMap(),
          mNameGidMap(),
          mGroupUsersMap(),
          mPendingUidNameMap(),
          mPendingGidNameMap(),
          mPendingNameUidMap(),
          mPendingNameGidMap(),
          mPendingGroupUsersMap(),
          mTmpUidNameMap(),
          mTmpGidNameMap(),
          mTmpNameUidMap(),
          mTmpNameGidMap(),
          mTmpGroupUsersMap(),
          mTmpGroupUserNamesMap()
        {}
    ~Impl()
        { Impl::Shutdown(); }
    int Start()
    {
        QCStMutexLocker theLock(mMutex);
        if (mThread.IsStarted()) {
            return -EINVAL;
        }
        const int theError = Update();
        if (theError != 0) {
            return theError;
        }
        mUpdateCount++;
        mStopFlag   = false;
        mUpdateFlag = false;
        const int kStackSize = 32 << 10;
        mThread.Start(this, kStackSize, "UpdateUserAndGroup");
        return 0;
    }
    void Shutdown()
    {
        QCStMutexLocker theLock(mMutex);
        if (mStopFlag) {
            return;
        }
        mStopFlag = true;
        mCond.Notify();
        theLock.Unlock();
        mThread.Join();
    }
    void ScheduleUpdate()
    {
        QCStMutexLocker theLock(mMutex);
        mUpdateFlag = true;
        mCond.Notify();
    }
    int SetParameters(
        const char*       inPrefixPtr,
        const Properties& inProperties)
    {
        Properties::String theParamName;
        if (inPrefixPtr) {
            theParamName.Append(inPrefixPtr);
        }
        QCStMutexLocker theLock(mMutex);
        const size_t thePrefixLen = theParamName.GetSize();
        mMinUserId = inProperties.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "minUserId"), mMinUserId);
        mMaxUserId = inProperties.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "maxUserId"), mMaxUserId);
        mMinGroupId = inProperties.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "minGroupId"), mMinGroupId);
        mMaxGroupId = inProperties.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "maxGroupId"), mMaxGroupId);
        const Properties::String* theUGEPtr[2];
        theUGEPtr[0] = inProperties.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "excludeUser"));
        theUGEPtr[1] = inProperties.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "excludeGroup"));
        mUserExcludes.clear();
        mGroupExcludes.clear();
        for (int i = 0; i < 2; i++) {
            const Properties::String* const theSPtr = theUGEPtr[i];
            if (! theSPtr) {
                continue;
            }
            const char* thePtr    = theSPtr->GetPtr();
            const char* theEndPtr = thePtr + theSPtr->GetSize();
            while (thePtr < theEndPtr) {
                while (thePtr < theEndPtr && (*thePtr & 0xFF) <= ' ')
                    {}
                const char* const theTokenPtr = thePtr;
                while (thePtr < theEndPtr && ' ' < (*thePtr & 0xFF))
                    {}
                if (theTokenPtr < thePtr) {
                    const string theName(theTokenPtr, thePtr - theTokenPtr);
                    if (i == 0) {
                        mUserExcludes.insert(theName);
                    } else {
                        mGroupExcludes.insert(theName);
                    }
                }
            }
        }
        mUpdateFlag = true;
        mParametersReadCount++;
        mCond.Notify();
        return 0;
    }
private:
    virtual void Run()
    {
        QCStMutexLocker theLock(mMutex);
        for (; ;) {
            while (! mStopFlag && ! mUpdateFlag &&
                mCond.Wait(mMutex, mUpdatePeriodNanoSec))
                {}
            if (mStopFlag) {
                break;
            }
            mUpdateFlag = false;
            Update();
        }
    }
    int Update()
    {
        UserExcludes theUserExcludes;
        theUserExcludes.swap(mUserExcludes);
        GroupExcludes  theGroupExcludes;
        theGroupExcludes.swap(mUserExcludes);
        const uint64_t theParametersReadCount = mParametersReadCount;
        const int      theError               = UpdateSelf(
            theUserExcludes,  theGroupExcludes);
        if (theError == 0) {
            mPendingUidNameMap.Swap(mTmpUidNameMap);
            mPendingGidNameMap.Swap(mTmpGidNameMap);
            mPendingNameUidMap.Swap(mTmpNameUidMap);
            mPendingNameGidMap.Swap(mTmpNameGidMap);
            mPendingGroupUsersMap.Swap(mTmpGroupUsersMap);
            mUpdateCount++;
        }
        if (mParametersReadCount == theParametersReadCount) {
            theUserExcludes.swap(mUserExcludes);
            theGroupExcludes.swap(mUserExcludes);
        } else {
            theUserExcludes.clear();
            theGroupExcludes.clear();
        }
        return theError;
    }
    virtual void Timeout()
    {
        if (mUpdateCount == mCurUpdateCount) {
            return;
        }
        QCStMutexLocker theLock(mMutex);
        if (mUpdateCount == mCurUpdateCount) {
            return;
        }
        mUidNameMap.Swap(mPendingUidNameMap);
        mGidNameMap.Swap(mPendingGidNameMap);
        mNameUidMap.Swap(mPendingNameUidMap);
        mNameGidMap.Swap(mPendingNameGidMap);
        mGroupUsersMap.Swap(mPendingGroupUsersMap);
        mUpdateCount = mCurUpdateCount;
    }
    int UpdateSelf(
        const UserExcludes&  inUserExcludes,
        const GroupExcludes& inGroupExcludes)
    {
        kfsUid_t const     theMinUserId  = mMinUserId;
        kfsUid_t const     theMaxUserId  = mMaxUserId;
        kfsGid_t const     theMinGroupId = mMinGroupId;
        kfsGid_t const     theMaxGroupId = mMaxGroupId;
        QCStMutexUnlocker theUnlock(mMutex);

        mTmpUidNameMap.Clear();
        mTmpGidNameMap.Clear();
        mTmpNameUidMap.Clear();
        mTmpNameGidMap.Clear();
        mTmpGroupUsersMap.Clear();
        mTmpGroupUserNamesMap.Clear();
        int theError = 0;
        setgrent();
        for (; ;) {
            errno = 0;
            const struct group* const theEntryPtr = getgrent();
            if (! theEntryPtr) {
                theError = errno;
                if (theError != 0) {
                    KFS_LOG_STREAM_ERROR <<
                        "getgrent error: " << QCUtils::SysError(theError) <<
                    KFS_LOG_EOM;
                }
                break;
            }
            const string   theName = theEntryPtr->gr_name;
            kfsGid_t const theGid  = (kfsGid_t)theEntryPtr->gr_gid;
            if (theGid < theMinGroupId || theMaxGroupId < theGid) {
                KFS_LOG_STREAM_DEBUG <<
                    "ignoring group:"
                    " id: "   << theGid <<
                    " name: " << theName <<
                    " min: "  << theMinGroupId <<
                    " max: "  << theMaxGroupId <<
                KFS_LOG_EOM;
                continue;
            }
            if (inGroupExcludes.find(theName) != inGroupExcludes.end()) {
                KFS_LOG_STREAM_DEBUG <<
                    "ignoring group:"
                    " id: "   << theGid <<
                    " name: " << theName <<
                KFS_LOG_EOM;
                continue;
            }
            bool                theInsertedFlag = false;
            const string* const theNamePtr      = mTmpGidNameMap.Insert(
                theGid, theName, theInsertedFlag);
            if (! theInsertedFlag) {
                KFS_LOG_STREAM_ERROR <<
                    "getgrent duplicate id: " << theGid <<
                    " group name: "           << *theNamePtr <<
                KFS_LOG_EOM;
            }
            theInsertedFlag = false;
            const kfsGid_t* const theGidPtr     = mTmpNameGidMap.Insert(
                theName, theGid, theInsertedFlag);
            if (! theInsertedFlag) {
                KFS_LOG_STREAM_ERROR <<
                    "getgrent duplicate group name: " << theName <<
                    " id: " << *theGidPtr <<
                KFS_LOG_EOM;
            }
            theInsertedFlag = false;
            UserNamesSet& theGrMembers = *mTmpGroupUserNamesMap.Insert(
                theGid, UserNamesSet(), theInsertedFlag);
            if (! theInsertedFlag) {
                KFS_LOG_STREAM_ERROR <<
                    "getgrent adding users rom duplicate group entry id: " <<
                        theGid <<
                KFS_LOG_EOM;
            }
            for (char** thePtr = theEntryPtr->gr_mem; thePtr; ++thePtr) {
                if (! **thePtr) {
                    continue;
                }
                const string theName(*thePtr);
                if (inUserExcludes.find(theName) != inUserExcludes.end()) {
                    continue;
                }
                if (! theGrMembers.insert(theName).second) {
                    KFS_LOG_STREAM_ERROR <<
                        "getgrent duplicate user entry: " << theName <<
                    KFS_LOG_EOM;
                }
            }
        }
        endgrent();
        if (theError != 0) {
            return (theError < 0 ? theError : -theError);
        }
        setpwent();
        UserExcludes theIdExcludes;
        for (; ;) {
            errno = 0;
            const struct passwd* const theEntryPtr = getpwent();
            if (! theEntryPtr) {
                theError = errno;
                if (theError != 0) {
                    KFS_LOG_STREAM_ERROR <<
                        "getpwent error: " << QCUtils::SysError(theError) <<
                    KFS_LOG_EOM;
                }
                break;
            }
            const string        theName = theEntryPtr->pw_name;
            kfsUid_t const      theUid  = (kfsUid_t)theEntryPtr->pw_uid;
            if (theUid < theMinUserId || theMaxUserId < theUid) {
                KFS_LOG_STREAM_DEBUG <<
                    "ignoring user:"
                    " id: "   << theUid <<
                    " name: " << theName <<
                    " min: "  << theMinUserId <<
                    " max: "  << theMaxUserId <<
                KFS_LOG_EOM;
                theIdExcludes.insert(theName);
                continue;
            }
            if (inUserExcludes.find(theName) != inUserExcludes.end()) {
                KFS_LOG_STREAM_DEBUG <<
                    "ignoring user:"
                    " id: "   << theUid <<
                    " name: " << theName <<
                KFS_LOG_EOM;
                continue;
            }
            kfsGid_t const      theGid          = (kfsGid_t)theEntryPtr->pw_gid;
            bool                theInsertedFlag = false;
            const string* const theNamePtr      = mTmpUidNameMap.Insert(
                theUid, theName, theInsertedFlag);
            if (! theInsertedFlag) {
                KFS_LOG_STREAM_ERROR <<
                    "getpwent duplicate user id: " << theUid <<
                    " name: " << *theNamePtr <<
                KFS_LOG_EOM;
            }
            theInsertedFlag = false;
            const kfsUid_t* const theUidPtr     = mTmpNameUidMap.Insert(
                theName, theUid, theInsertedFlag);
            if (! theInsertedFlag) {
                KFS_LOG_STREAM_ERROR <<
                    "getpwent duplicate user name: " << theName <<
                    " id: " << *theUidPtr <<
                KFS_LOG_EOM;
            }
            if (! mTmpGroupUserNamesMap.Find(theGid)) {
                KFS_LOG_STREAM_ERROR <<
                    "no group found in group file gid: " << theGid <<
                KFS_LOG_EOM;
            }
            theInsertedFlag = false;
            UsersSet& theUsers = *mTmpGroupUsersMap.Insert(
                theGid, UsersSet(), theInsertedFlag);
            theUsers.insert(theUid);
        }
        endpwent();
        if (theError != 0) {
            return (theError < 0 ? theError : -theError);
        }
        mTmpGroupUserNamesMap.First();
        const GroupUserNames* thePtr;
        while ((thePtr = mTmpGroupUserNamesMap.Next())) {
            const UserNamesSet& theNamesSet = thePtr->GetVal();
            bool theInsertedFlag = false;
            UsersSet& theUsers = *mTmpGroupUsersMap.Insert(
                thePtr->GetKey(), UsersSet(), theInsertedFlag);
            for (UserNamesSet::const_iterator theIt = theNamesSet.begin();
                    theIt != theNamesSet.end();
                    ++theIt) {
                const kfsUid_t* const theUidPtr = mTmpNameUidMap.Find(*theIt);
                if (theUidPtr) {
                    theUsers.insert(*theUidPtr);
                } else if (theIdExcludes.find(*theIt) != theIdExcludes.end()) {
                    KFS_LOG_STREAM_ERROR <<
                        "group id: "      << thePtr->GetKey() <<
                        " no such user: " << *theIt <<
                    KFS_LOG_EOM;
                }
            }
        }
        return theError;
    }
private:
    typedef set<
        string,
        less<string>,
        StdFastAllocator<string>
    > UserNamesSet;
    typedef KVPair<kfsGid_t, UserNamesSet> GroupUserNames;
    typedef LinearHash<
        GroupUserNames,
        KeyCompare<GroupUserNames::Key>,
        DynamicArray<SingleLinkedList<GroupUserNames>*, 9>,
        StdFastAllocator<GroupUserNames>
    > GroupUsersNamesMap;

    volatile uint64_t  mUpdateCount;
    uint64_t           mCurUpdateCount;
    QCThread           mThread;
    QCMutex            mMutex;
    QCCondVar          mCond;
    bool               mStopFlag;
    bool               mUpdateFlag;
    QCMutex::Time      mUpdatePeriodNanoSec;
    kfsUid_t           mMinUserId;
    kfsUid_t           mMaxUserId;
    kfsGid_t           mMinGroupId;
    kfsGid_t           mMaxGroupId;
    UserExcludes       mUserExcludes;
    GroupExcludes      mGroupExcludes;
    uint64_t           mParametersReadCount;
    UidNameMap         mUidNameMap;
    GidNameMap         mGidNameMap;
    NameUidMap         mNameUidMap;
    NameGidMap         mNameGidMap;
    GroupUsersMap      mGroupUsersMap;
    UidNameMap         mPendingUidNameMap;
    GidNameMap         mPendingGidNameMap;
    NameUidMap         mPendingNameUidMap;
    NameGidMap         mPendingNameGidMap;
    GroupUsersMap      mPendingGroupUsersMap;
    UidNameMap         mTmpUidNameMap;
    GidNameMap         mTmpGidNameMap;
    NameUidMap         mTmpNameUidMap;
    NameGidMap         mTmpNameGidMap;
    GroupUsersMap      mTmpGroupUsersMap;
    GroupUsersNamesMap mTmpGroupUserNamesMap;

    friend class UserAndGroup;
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

UserAndGroup::UserAndGroup()
    : mImpl(*(new Impl())),
      mUpdateCount(mImpl.mUpdateCount),
      mGroupUsersMap(mImpl.mGroupUsersMap),
      mNameUidMap(mImpl.mNameUidMap),
      mUidNameMap(mImpl.mUidNameMap),
      mGidNameMap(mImpl.mGidNameMap),
      mNameGidMap(mImpl.mNameGidMap)
{
}

UserAndGroup::~UserAndGroup()
{
    delete &mImpl;
}

}
