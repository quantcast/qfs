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

#include "common/kfstypes.h"
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

class UserAndGroup
{
public:
    UserAndGroup();
    ~UserAndGroup();
    int SetParameters(
        const Properties& inProperties);
    int Start();
    void Shutdown();
    void ScheduleUpdate();
    const volatile uint64_t& GetUpdateCount() const
        { return mUpdateCount; }
private:
    class Impl;
    volatile uint64_t mUpdateCount;
    Impl&             mImpl;
private:
    UserAndGroup(
        const UserAndGroup& inUserAndGroup);
    UserAndGroup& operator=(
        const UserAndGroup& inUserAndGroup);
};

using std::string;
using std::set;
using std::less;

class UserAndGroup::Impl : public QCRunnable, public ITimeout
{
public:
    Impl(
        volatile uint64_t& inUpdateCount)
        : QCRunnable(),
          ITimeout(),
          mUpdateCount(inUpdateCount),
          mThread()
        {}
    ~Impl()
        {}
    int Start()
    {
        if (! mStopFlag) {
            return 0;
        }
        mStopFlag   = false;
        mUpdateFlag = true;
        return 0;
    }
    void Shutdown()
    {
        QCStMutexLocker theLock(mMutex);
        mStopFlag = true;
        mCond.Notify();
    }
    void ScheduleUpdate()
    {
        QCStMutexLocker theLock(mMutex);
        mUpdateFlag = true;
        mCond.Notify();
    }
    bool IsGroupMamber(
        kfsUid_t inUser,
        kfsGid_t inGroup)
    {
        const UsersSet* const theUsersPtr = mGroupUsersMap.Find(inGroup);
        return (theUsersPtr && theUsersPtr->find(inUser) != theUsersPtr->end());
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
            if (Update()) {
                mPendingUidNameMap.Swap(mTmpUidNameMap);
                mPendingGidNameMap.Swap(mTmpGidNameMap);
                mPendingNameUidMap.Swap(mTmpNameUidMap);
                mPendingNameGidMap.Swap(mTmpNameGidMap);
                mPendingGroupUsersMap.Swap(mTmpGroupUsersMap);
                mUpdateCount++;
            }
        }
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
    bool Update()
    {
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
                if (! theGrMembers.insert(string(*thePtr)).second) {
                    KFS_LOG_STREAM_ERROR <<
                        "getgrent duplicate user entry: " << *thePtr <<
                    KFS_LOG_EOM;
                }
            }
        }
        endgrent();
        if (theError != 0) {
            return false;
        }
        setpwent();
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
            const string        theName         = theEntryPtr->pw_name;
            kfsUid_t const      theUid          = (kfsUid_t)theEntryPtr->pw_uid;
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
            return false;
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
                } else {
                    KFS_LOG_STREAM_ERROR <<
                        "group id: " << thePtr->GetKey() <<
                        " no such user: " << *theIt <<
                    KFS_LOG_EOM;
                }
            }
        }
        return true;
    }
private:
    struct StringHash
    {
        static size_t Hash(const string& inString)
            { return HsiehHash(inString.data(), inString.size()); }
    };
    typedef KVPair<kfsUid_t, string> UidName;
    typedef KVPair<kfsGid_t, string> GidName;
    typedef KVPair<string, kfsUid_t> NameUid;
    typedef KVPair<string, kfsGid_t> NameGid;
    typedef LinearHash<
        UidName,
        KeyCompare<UidName::Key>,
        DynamicArray<SingleLinkedList<UidName>*, 9>,
        StdFastAllocator<UidName>
    > UidNameMap;
    typedef LinearHash<
        GidName,
        KeyCompare<GidName::Key>,
        DynamicArray<SingleLinkedList<GidName>*, 9>,
        StdFastAllocator<GidName>
    > GidNameMap;
    typedef LinearHash<
        NameUid,
        KeyCompare<NameUid::Key, StringHash>,
        DynamicArray<SingleLinkedList<NameUid>*, 9>,
        StdFastAllocator<NameUid>
    > NameUidMap;
    typedef LinearHash<
        NameGid,
        KeyCompare<NameGid::Key, StringHash>,
        DynamicArray<SingleLinkedList<NameGid>*, 9>,
        StdFastAllocator<NameGid>
    > NameGidMap;
    typedef set<
        kfsUid_t,
        less<kfsUid_t>,
        StdFastAllocator<kfsUid_t>
    > UsersSet;
    typedef KVPair<kfsGid_t, UsersSet> GroupUsers;
    typedef LinearHash<
        GroupUsers,
        KeyCompare<GroupUsers::Key>,
        DynamicArray<SingleLinkedList<GroupUsers>*, 9>,
        StdFastAllocator<GroupUsers>
    > GroupUsersMap;
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

    volatile uint64_t& mUpdateCount;
    uint64_t           mCurUpdateCount;
    QCThread           mThread;
    QCMutex            mMutex;
    QCCondVar          mCond;
    bool               mStopFlag;
    bool               mUpdateFlag;
    QCMutex::Time      mUpdatePeriodNanoSec;
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
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

}
