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

#include "kfsio/Globals.h"
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
using libkfsio::globalNetManager;

class UserAndGroup::Impl : public QCRunnable, public ITimeout
{
private:
    typedef set<
        string,
        less<string>,
        StdFastAllocator<string>
    > NamesSet;
    typedef NamesSet RootUserNames;
    typedef NamesSet UserExcludes;
    typedef NamesSet GroupExcludes;
    typedef NamesSet RootGroups;
    typedef NamesSet MetaAdminUserNames;
    typedef NamesSet MetaAdminGroupNames;
    typedef NamesSet MetaStatsUserNames;
    typedef NamesSet MetaStatsGroupNames;
    typedef NamesSet DelegationUserNames;
    typedef NamesSet DelegationGroupNames;
public:
    Impl(
        bool inUseDefaultsFlag)
        : QCRunnable(),
          ITimeout(),
          mUpdateCount(0),
          mCurUpdateCount(0),
          mThread(),
          mMutex(),
          mCond(),
          mStopFlag(false),
          mUpdateFlag(false),
          mDisabledFlag(false),
          mOverflowFlag(false),
          mUpdatePeriodNanoSec(QCMutex::Time(10) * 365 * 24 * 60 * 60 *
            1000 * 1000 * 1000),
          mMinUserId(0),
          mMaxUserId(~kfsUid_t(0)),
          mMinGroupId(0),
          mMaxGroupId(~kfsGid_t(0)),
          mUserExcludes(),
          mGroupExcludes(),
          mParametersReadCount(0),
          mUidNameMapPtr(new UidNameMap()),
          mUidNamePtr(mUidNameMapPtr),
          mNameUidMapPtr(new NameUidMap()),
          mNameUidPtr(mNameUidMapPtr),
          mNameGidMap(),
          mGidNameMapPtr(new GidNameMap()),
          mGidNamePtr(mGidNameMapPtr),
          mRootUsersPtr(new RootUsers()),
          mDelegationRenewAndCancelUsersPtr(new UserIdsSet()),
          mGroupUsersMap(),
          mPendingUidNameMap(),
          mPendingGidNameMap(),
          mPendingNameUidMap(),
          mPendingNameGidMap(),
          mPendingGroupUsersMap(),
          mPendingRootUsers(),
          mPendingMetaAdminUsers(),
          mPendingMetaStatsUsers(),
          mPendingDelegationRenewAndCancelUsers(),
          mTmpUidNameMap(),
          mTmpGidNameMap(),
          mTmpNameUidMap(),
          mTmpNameGidMap(),
          mTmpGroupUsersMap(),
          mTmpGroupUserNamesMap(),
          mTmpRootUsers(),
          mTmpMetaAdminUsers(),
          mTmpMetaStatsUsers(),
          mTmpDelegationRenewAndCancelUsers(),
          mRootGroups(),
          mRootUserNames(),
          mOmitUserPrefix(),
          mOmitGroupPrefix(),
          mMetaServerAdminUsers(),
          mMetaServerStatsUsers(),
          mMetaAdminUserNames(),
          mMetaAdminGroupNames(),
          mMetaStatsUserNames(),
          mMetaStatsGroupNames(),
          mDelegationUserNames(),
          mDelegationGroupNames(),
          mParameters(),
          mSetDefaultsFlag(inUseDefaultsFlag)
        {}
    ~Impl()
        { Impl::Shutdown(); }
    int Start()
    {
        QCStMutexLocker theLock(mMutex);
        return StartSelf();
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
        globalNetManager().UnRegisterTimeoutHandler(this);
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
        if (mSetDefaultsFlag) {
            // Set defaults.
            mSetDefaultsFlag = false;
            const DefaultParameterEntry kDefaults [] = {
                { "metaServerAdminUsers", "root" },
                { "metaServerStatsUsers", "root" },
                { 0, 0 } // Sentinel
            };
            for (const DefaultParameterEntry* thePtr = kDefaults;
                    thePtr->mNamePtr;
                    ++thePtr) {
                mParameters.setValue(theParamName.Truncate(thePrefixLen).Append(
                    thePtr->mNamePtr), Properties::String(thePtr->mValuePtr));
            }
        }
        inProperties.copyWithPrefix(
            theParamName.GetPtr(), thePrefixLen, mParameters);
        mMinUserId = mParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "minUserId"), mMinUserId);
        mMaxUserId = mParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "maxUserId"), mMaxUserId);
        mMinGroupId = mParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "minGroupId"), mMinGroupId);
        mMaxGroupId = mParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "maxGroupId"), mMaxGroupId);
        mUpdatePeriodNanoSec = (QCMutex::Time)(mParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "updatePeriodSec"), (double)mUpdatePeriodNanoSec * 1e-9) * 1e9);
        mOmitUserPrefix = mParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "omitUserPrefix"), mOmitUserPrefix);
        mOmitGroupPrefix = mParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "omitGroupPrefix"), mOmitGroupPrefix);
        mDisabledFlag = mParameters.getValue(
            theParamName.Truncate(thePrefixLen).Append(
            "disable"), mDisabledFlag ? 1 : 0) != 0;

        const NamesSetParamEntry kNameSets[] = {
            { "excludeUser",                    &mUserExcludes         },
            { "excludeGroup",                   &mGroupExcludes        },
            { "rootGroups",                     &mRootGroups           },
            { "rootUsers",                      &mRootUserNames        },
            { "metaServerAdminUsers",           &mMetaAdminUserNames   },
            { "metaServerAdminGroups",          &mMetaAdminGroupNames  },
            { "metaServerStatsUsers",           &mMetaStatsUserNames   },
            { "metaServerStatsGroups",          &mMetaStatsGroupNames  },
            { "delegationRenewAndCancelUsers",  &mDelegationUserNames  },
            { "delegationRenewAndCancelGruops", &mDelegationGroupNames },
            { 0, 0 } // Sentinel
        };
        for (const NamesSetParamEntry* theEPtr = kNameSets;
                theEPtr->mPropNamePtr;
                ++theEPtr) {
            NamesSet& theSet = *(theEPtr->mNamesSetPtr);
            theSet.clear();
            const Properties::String* const theSPtr = mParameters.getValue(
                theParamName.Truncate(thePrefixLen).Append(
                    theEPtr->mPropNamePtr
            ));
            if (! theSPtr) {
                continue;
            }
            const char* thePtr    = theSPtr->GetPtr();
            const char* theEndPtr = thePtr + theSPtr->GetSize();
            while (thePtr < theEndPtr) {
                while (thePtr < theEndPtr && (*thePtr & 0xFF) <= ' ') {
                    thePtr++;
                }
                const char* const theTokenPtr = thePtr;
                while (thePtr < theEndPtr && ' ' < (*thePtr & 0xFF)) {
                    thePtr++;
                }
                if (theTokenPtr < thePtr) {
                    theSet.insert(string(theTokenPtr, thePtr - theTokenPtr));
                }
            }
        }
        if (! mThread.IsStarted()) {
            return StartSelf();
        }
        mUpdateFlag = true;
        mParametersReadCount++;
        mCond.Notify();
        return 0;
    }
    bool IsUpdatePending() const
    {
        QCStMutexLocker theLock(const_cast<Impl*>(this)->mMutex); // Mutable
        return (mUpdateCount != mCurUpdateCount);
    }
private:
    int StartSelf()
    {
        if (mThread.IsStarted()) {
            return -EINVAL;
        }
        const int theError = Update();
        ApplyPendingUpdate();
        mStopFlag   = false;
        mUpdateFlag = false;
        const int kStackSize = 32 << 10;
        mThread.Start(this, kStackSize, "UpdateUserAndGroup");
        globalNetManager().RegisterTimeoutHandler(this);
        return (theError == 0 ? (mOverflowFlag ? -EOVERFLOW : 0) : theError);
    }
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
        theGroupExcludes.swap(mGroupExcludes);
        RootGroups theRootGroups;
        theRootGroups.swap(mRootGroups);
        RootUserNames theRootUserNames;
        theRootUserNames.swap(mRootUserNames);
        MetaAdminUserNames theMetaAdminUserNames;
        theMetaAdminUserNames.swap(mMetaAdminUserNames);
        MetaAdminGroupNames theMetaAdminGroupNames;
        theMetaAdminGroupNames.swap(mMetaAdminGroupNames);
        MetaStatsUserNames theMetaStatsUserNames;
        theMetaStatsUserNames.swap(mMetaStatsUserNames);
        MetaStatsGroupNames theMetaStatsGroupNames;
        theMetaStatsGroupNames.swap(mMetaStatsGroupNames);
        DelegationUserNames theDelegationUserNames;
        theDelegationUserNames.swap(mDelegationUserNames);
        DelegationGroupNames theDelegationGroupNames;
        theDelegationGroupNames.swap(mDelegationGroupNames);

        const uint64_t theParametersReadCount = mParametersReadCount;
        bool           theOverflowFlag        = false;
        const int      theError               = UpdateSelf(
            theUserExcludes,
            theGroupExcludes,
            theRootGroups,
            theRootUserNames,
            theMetaAdminUserNames,
            theMetaAdminGroupNames,
            theMetaStatsUserNames,
            theMetaStatsGroupNames,
            theDelegationGroupNames,
            theDelegationUserNames,
            theOverflowFlag
        );
        if (theError == 0) {
            mPendingUidNameMap.Swap(mTmpUidNameMap);
            mPendingGidNameMap.Swap(mTmpGidNameMap);
            mPendingNameUidMap.Swap(mTmpNameUidMap);
            mPendingNameGidMap.Swap(mTmpNameGidMap);
            mPendingGroupUsersMap.Swap(mTmpGroupUsersMap);
            mPendingRootUsers.Swap(mTmpRootUsers);
            mPendingMetaAdminUsers.Swap(mTmpMetaAdminUsers);
            mPendingMetaStatsUsers.Swap(mTmpMetaStatsUsers);
            mPendingDelegationRenewAndCancelUsers.Swap(
                mTmpDelegationRenewAndCancelUsers);
            mOverflowFlag = theOverflowFlag;
            mUpdateCount++;
        }
        if (mParametersReadCount == theParametersReadCount) {
            theUserExcludes.swap(mUserExcludes);
            theGroupExcludes.swap(mUserExcludes);
            theRootGroups.swap(mRootGroups);
            theRootUserNames.swap(mRootUserNames);
            theMetaAdminUserNames.swap(mMetaAdminUserNames);
            theMetaAdminGroupNames.swap(mMetaAdminGroupNames);
            theMetaStatsUserNames.swap(mMetaStatsUserNames);
            theMetaStatsGroupNames.swap(mMetaStatsGroupNames);
            theDelegationGroupNames.swap(mDelegationGroupNames);
            theDelegationUserNames.swap(mDelegationUserNames);
        }
        return theError;
    }
    virtual void Timeout()
    {
        if (mUpdateCount == mCurUpdateCount) {
            return;
        }
        QCStMutexLocker theLock(mMutex);
        ApplyPendingUpdate();
    }
    void ApplyPendingUpdate()
    {
        if (mUpdateCount == mCurUpdateCount) {
            return;
        }
        mUidNameMapPtr = new UidNameMap();
        mUidNameMapPtr->Swap(mPendingUidNameMap);
        mUidNamePtr.reset(mUidNameMapPtr);

        mNameUidMapPtr = new NameUidMap();
        mNameUidMapPtr->Swap(mPendingNameUidMap);
        mNameUidPtr.reset(mNameUidMapPtr);

        mGidNameMapPtr = new GidNameMap();
        mGidNameMapPtr->Swap(mPendingGidNameMap);
        mGidNamePtr.reset(mGidNameMapPtr);

        RootUsers* const theRootUsersPtr = new RootUsers();
        theRootUsersPtr->Swap(mPendingRootUsers);
        mRootUsersPtr.reset(theRootUsersPtr);

        mNameGidMap.Swap(mPendingNameGidMap);
        mGroupUsersMap.Swap(mPendingGroupUsersMap);
        mCurUpdateCount = mUpdateCount;

        mMetaServerAdminUsers.Swap(mPendingMetaAdminUsers);
        mMetaServerStatsUsers.Swap(mPendingMetaStatsUsers);

        UserIdsSet* const theDelegationRenewAndCancelUsersPtr =
            new UserIdsSet();
        theDelegationRenewAndCancelUsersPtr->Swap(
            mPendingDelegationRenewAndCancelUsers);
        mDelegationRenewAndCancelUsersPtr.reset(
            theDelegationRenewAndCancelUsersPtr);
    }
    static bool StartsWith(
        const string& inString,
        const string& inPrefix)
    {
        const size_t thePrefixSize = inPrefix.size();
        return (
            size_t(0) < thePrefixSize &&
            thePrefixSize <= inString.size() &&
            inPrefix.compare(0, thePrefixSize, inString, 0, thePrefixSize) == 0
        );
    }
    static bool IsValidName(
        const string& inName)
    {
        const char* thePtr          = inName.c_str();
        const char* const theEndPtr = thePtr + inName.size();
        // Do not allow leading or trailing spaces.
        if (theEndPtr <= thePtr || (*thePtr & 0xFF) <= ' ' ||
                (theEndPtr[-1] & 0xFF) <= ' ') {
            return false;
        }
        // Do not allow control characters.
        while (thePtr < theEndPtr) {
            if ((*thePtr & 0xFF) < ' ') {
                return false;
            }
            thePtr++;
        }
        return true;
    }
    int UpdateSelf(
        const UserExcludes&         inUserExcludes,
        const GroupExcludes&        inGroupExcludes,
        const RootGroups&           inRootGroups,
        const RootUserNames&        inRootUserNames,
        const MetaAdminUserNames&   inMetaAdminUserNames,
        const MetaAdminGroupNames&  inMetaAdminGroupNames,
        const MetaStatsUserNames&   inMetaStatsUserNames,
        const MetaStatsGroupNames&  inMetaStatsGroupNames,
        const DelegationUserNames&  inDelegationUserNames,
        const DelegationGroupNames& inDelegationGroupNames,
        bool&                       outOverflowFlag)
    {
        kfsUid_t const theMinUserId       = mMinUserId;
        kfsUid_t const theMaxUserId       = mMaxUserId;
        kfsGid_t const theMinGroupId      = mMinGroupId;
        kfsGid_t const theMaxGroupId      = mMaxGroupId;
        string const   theOmitUserPrefix  = mOmitUserPrefix;
        string const   theOmitGroupPrefix = mOmitGroupPrefix;
        bool const     theDisabledFlag    = mDisabledFlag;
        QCStMutexUnlocker theUnlock(mMutex);

        mTmpUidNameMap.Clear();
        mTmpGidNameMap.Clear();
        mTmpNameUidMap.Clear();
        mTmpNameGidMap.Clear();
        mTmpGroupUsersMap.Clear();
        mTmpGroupUserNamesMap.Clear();
        mTmpRootUsers.Clear();
        mTmpMetaAdminUsers.Clear();
        mTmpMetaStatsUsers.Clear();
        mTmpDelegationRenewAndCancelUsers.Clear();

        int theError = 0;
        if (theDisabledFlag) {
            return theError;
        }

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
            if (! IsValidName(theName)) {
                KFS_LOG_STREAM_ERROR <<
                    "ignoring malformed group"
                    " name: " << theName <<
                    " id: "   << theGid <<
                KFS_LOG_EOM;
                continue;
            }
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
            if (inGroupExcludes.find(theName) != inGroupExcludes.end() ||
                    StartsWith(theName, theOmitGroupPrefix)) {
                KFS_LOG_STREAM_DEBUG <<
                    "ignoring group:"
                    " id: "   << theGid <<
                    " name: " << theName <<
                KFS_LOG_EOM;
                continue;
            }
            if (mTmpGidNameMap.MaxSize() <= mTmpGidNameMap.GetSize()) {
                KFS_LOG_STREAM_ERROR <<
                    "group table size exceed max allowed"
                    " size: " << mTmpGidNameMap.GetSize() <<
                    " ignoring group: "
                    " id: "   << theGid <<
                    " name: " << theName <<
                KFS_LOG_EOM;
                outOverflowFlag = true;;
                continue;
            }
            bool                theInsertedFlag = false;
            const string* const theNamePtr      = mTmpGidNameMap.Insert(
                theGid, theName, theInsertedFlag);
            if (! theInsertedFlag && *theNamePtr != theName) {
                KFS_LOG_STREAM_ERROR <<
                    "getgrent duplicate id: " << theGid <<
                    " group name: "           << theName <<
                    " current: "              << *theNamePtr <<
                KFS_LOG_EOM;
                continue;
            }
            const bool theNewGroupFlag = theInsertedFlag;
            theInsertedFlag = false;
            const kfsGid_t* const theGidPtr     = mTmpNameGidMap.Insert(
                theName, theGid, theInsertedFlag);
            if (! theInsertedFlag && *theGidPtr != theGid) {
                KFS_LOG_STREAM_ERROR <<
                    "getgrent duplicate group name: " << theName <<
                    " id: " << theGid <<
                    " id: " << *theGidPtr <<
                KFS_LOG_EOM;
                continue;
            }
            if (theGid == kKfsGroupNone) {
                // "No group" shouldn't have any users.
                continue;
            }
            theInsertedFlag = false;
            UserNamesSet& theGrMembers = *mTmpGroupUserNamesMap.Insert(
                theGid, UserNamesSet(), theInsertedFlag);
            for (char** thePtr = theEntryPtr->gr_mem;
                    thePtr && *thePtr;
                    ++thePtr) {
                if (! **thePtr) {
                    continue;
                }
                const string theUName(*thePtr);
                if (inUserExcludes.find(theUName) != inUserExcludes.end()) {
                    continue;
                }
                if (! theGrMembers.insert(theUName).second && theNewGroupFlag) {
                    KFS_LOG_STREAM_DEBUG <<
                        "getgrent group: "        << theName <<
                        " duplicate user entry: " << theUName <<
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
            const string   theName = theEntryPtr->pw_name;
            kfsUid_t const theUid  = (kfsUid_t)theEntryPtr->pw_uid;
            if (! IsValidName(theName)) {
                KFS_LOG_STREAM_ERROR <<
                    "ignoring malformed user"
                    " name: " << theName <<
                    " id: "   << theUid <<
                KFS_LOG_EOM;
                continue;
            }
            if (theUid < theMinUserId || theMaxUserId < theUid ||
                    theUid == kKfsUserNone) {
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
            if (inUserExcludes.find(theName) != inUserExcludes.end() ||
                    StartsWith(theName, theOmitUserPrefix)) {
                KFS_LOG_STREAM_DEBUG <<
                    "ignoring user:"
                    " id: "   << theUid <<
                    " name: " << theName <<
                KFS_LOG_EOM;
                continue;
            }
            if (mTmpUidNameMap.MaxSize() <= mTmpUidNameMap.GetSize()) {
                KFS_LOG_STREAM_ERROR <<
                    "user table size exceed max allowed"
                    " size: " << mTmpGidNameMap.GetSize() <<
                    " ignoring user: "
                    " id: "   << theUid <<
                    " name: " << theName <<
                KFS_LOG_EOM;
                outOverflowFlag = true;;
                continue;
            }
            kfsGid_t const          theGid           =
                (kfsGid_t)theEntryPtr->pw_gid;
            bool                    theInsertedFlag  = false;
            const NameAndGid* const theNameAndGidPtr = mTmpUidNameMap.Insert(
                theUid, NameAndGid(theName, theGid), theInsertedFlag);
            if (! theInsertedFlag &&
                    (theNameAndGidPtr->mName != theName ||
                        theNameAndGidPtr->mGid != theGid)) {
                KFS_LOG_STREAM_ERROR <<
                    "getpwent duplicate user"
                    " id: "    << theUid <<
                    " name: "  << theName <<
                    " cur: "   << theNameAndGidPtr->mName <<
                    " group: " << theGid <<
                    " cur: "   << theNameAndGidPtr->mGid <<
                KFS_LOG_EOM;
                continue;
            }
            const bool theNewUserFlag = theInsertedFlag;
            theInsertedFlag = false;
            const UidAndGid* const theUidGidPtr = mTmpNameUidMap.Insert(
                theName, UidAndGid(theUid, theGid), theInsertedFlag);
            if (! theInsertedFlag &&
                    (theUidGidPtr->mUid != theUid ||
                        theUidGidPtr->mGid != theGid)) {
                KFS_LOG_STREAM_ERROR <<
                    "getpwent duplicate user name: " << theName <<
                    " id: "    << theUid <<
                    " cur: "   << theUidGidPtr->mUid <<
                    " group: " << theGid <<
                    " cur: "   << theUidGidPtr->mGid <<
                KFS_LOG_EOM;
                continue;
            }
            if (theGid != kKfsGroupNone &&
                    ! mTmpGroupUserNamesMap.Find(theGid) && theNewUserFlag) {
                KFS_LOG_STREAM_ERROR <<
                    "getpwent user: "       << theName <<
                    " no group found gid: " << theGid <<
                KFS_LOG_EOM;
            }
            theInsertedFlag = false;
            UsersSet& theUsers = *mTmpGroupUsersMap.Insert(
                theGid, UsersSet(), theInsertedFlag);
            if (inRootUserNames.find(theName) != inRootUserNames.end()) {
                theInsertedFlag = false;
                mTmpRootUsers.Insert(theUid, theUid, theInsertedFlag);
                if (theInsertedFlag) {
                    KFS_LOG_STREAM_DEBUG <<
                        "added root user: " << theName <<
                        " id: "             << theUid <<
                    KFS_LOG_EOM;
                }
            }
            theUsers.insert(theUid);
        }
        endpwent();
        if (theError != 0) {
            return (theError < 0 ? theError : -theError);
        }
        const string kRootUserName("root");
        if (! mTmpUidNameMap.Find(kKfsUserRoot) &&
                inUserExcludes.find(kRootUserName) == inUserExcludes.end()) {
            bool theInsertedFlag = false;
            mTmpUidNameMap.Insert(
                kKfsUserRoot, NameAndGid(kRootUserName, kKfsGroupRoot),
                theInsertedFlag);
            theInsertedFlag = false;
            mTmpNameUidMap.Insert(
                kRootUserName, UidAndGid(kKfsUserRoot, kKfsGroupRoot),
                theInsertedFlag);
            KFS_LOG_STREAM_INFO <<
                "adding root user entry" <<
            KFS_LOG_EOM;
        }
        mTmpGroupUserNamesMap.First();
        const GroupUserNames* thePtr;
        while ((thePtr = mTmpGroupUserNamesMap.Next())) {
            if (mTmpGroupUsersMap.MaxSize() <= mTmpGroupUsersMap.GetSize()) {
                KFS_LOG_STREAM_ERROR <<
                    "user table size exceed max allowed"
                    " size: " << mTmpGroupUsersMap.GetSize() <<
                    " ignoring group:"
                    " id: " << thePtr->GetKey() <<
                KFS_LOG_EOM;
                outOverflowFlag = true;
                continue;
            }
            const UserNamesSet& theNamesSet = thePtr->GetVal();
            bool theInsertedFlag = false;
            UsersSet& theUsers = *mTmpGroupUsersMap.Insert(
                thePtr->GetKey(), UsersSet(), theInsertedFlag);
            for (UserNamesSet::const_iterator theIt = theNamesSet.begin();
                    theIt != theNamesSet.end();
                    ++theIt) {
                const UidAndGid* const theUidGidPtr =
                    mTmpNameUidMap.Find(*theIt);
                if (theUidGidPtr) {
                    theUsers.insert(theUidGidPtr->mUid);
                    const string* const theGroupNamePtr =
                        mTmpGidNameMap.Find(thePtr->GetKey());
                    if (theGroupNamePtr &&
                            theUidGidPtr->mUid != kKfsUserRoot &&
                            inRootGroups.find(*theGroupNamePtr) !=
                            inRootGroups.end()) {
                        theInsertedFlag = false;
                        mTmpRootUsers.Insert(
                            theUidGidPtr->mUid,
                            theUidGidPtr->mUid,
                            theInsertedFlag
                        );
                        if (theInsertedFlag) {
                            KFS_LOG_STREAM_DEBUG <<
                                "added root user: " << *theIt <<
                                " id: "              << theUidGidPtr->mUid <<
                            KFS_LOG_EOM;
                        }
                    }
                } else if (theIdExcludes.find(*theIt) != theIdExcludes.end()) {
                    KFS_LOG_STREAM_ERROR <<
                        "group id: "      << thePtr->GetKey() <<
                        " no such user: " << *theIt <<
                    KFS_LOG_EOM;
                }
            }
        }
        int theStatus;
        if ((theStatus = InsertTmpUsers(
                inMetaAdminUserNames.begin(),
                inMetaAdminUserNames.end(),
                mTmpMetaAdminUsers,
                outOverflowFlag)) != 0 && theError == 0) {
            theError = theStatus;
        }
        if ((theStatus = InsertTmpUsers(
                inMetaStatsUserNames.begin(),
                inMetaStatsUserNames.end(),
                mTmpMetaStatsUsers,
                outOverflowFlag)) != 0 && theError == 0) {
            theError = theStatus;
        }
        if ((theStatus = InsertTmpUsers(
                inDelegationUserNames.begin(),
                inDelegationUserNames.end(),
                mTmpDelegationRenewAndCancelUsers,
                outOverflowFlag)) != 0 && theError == 0) {
            theError = theStatus;
        }
        if ((theStatus = InsertTmpGroupsUsers(
                inMetaAdminGroupNames.begin(),
                inMetaAdminGroupNames.end(),
                mTmpMetaAdminUsers,
                outOverflowFlag)) != 0 && theError == 0) {
            theError = theStatus;
        }
        if ((theStatus = InsertTmpGroupsUsers(
                inMetaStatsGroupNames.begin(),
                inMetaStatsGroupNames.end(),
                mTmpMetaStatsUsers,
                outOverflowFlag)) != 0 && theError == 0) {
            theError = theStatus;
        }
        if ((theStatus = InsertTmpGroupsUsers(
                inDelegationGroupNames.begin(),
                inDelegationGroupNames.end(),
                mTmpDelegationRenewAndCancelUsers,
                outOverflowFlag)) != 0 && theError == 0) {
            theError = theStatus;
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
        DynamicArray<SingleLinkedList<GroupUserNames>*,
            kLog2FirstBucketSize, kLog2MaxUserAndGroupCount>,
        StdFastAllocator<GroupUserNames>
    > GroupUsersNamesMap;
    struct NamesSetParamEntry
    {
        const char* mPropNamePtr;
        NamesSet*   mNamesSetPtr;
    };
    struct DefaultParameterEntry
    {
        const char* mNamePtr;
        const char* mValuePtr;
    };

    volatile uint64_t                mUpdateCount;
    uint64_t                         mCurUpdateCount;
    QCThread                         mThread;
    QCMutex                          mMutex;
    QCCondVar                        mCond;
    bool                             mStopFlag;
    bool                             mUpdateFlag;
    bool                             mDisabledFlag;
    bool                             mOverflowFlag;
    QCMutex::Time                    mUpdatePeriodNanoSec;
    kfsUid_t                         mMinUserId;
    kfsUid_t                         mMaxUserId;
    kfsGid_t                         mMinGroupId;
    kfsGid_t                         mMaxGroupId;
    UserExcludes                     mUserExcludes;
    GroupExcludes                    mGroupExcludes;
    uint64_t                         mParametersReadCount;
    UidNameMap*                      mUidNameMapPtr;
    UidNamePtr                       mUidNamePtr;
    GidNameMap                       mGidNameMap;
    NameUidMap*                      mNameUidMapPtr;
    NameUidPtr                       mNameUidPtr;
    NameGidMap                       mNameGidMap;
    GidNameMap*                      mGidNameMapPtr;
    GidNamePtr                       mGidNamePtr;
    RootUsersPtr                     mRootUsersPtr;
    DelegationRenewAndCancelUsersPtr mDelegationRenewAndCancelUsersPtr;
    GroupUsersMap                    mGroupUsersMap;
    UidNameMap                       mPendingUidNameMap;
    GidNameMap                       mPendingGidNameMap;
    NameUidMap                       mPendingNameUidMap;
    NameGidMap                       mPendingNameGidMap;
    GroupUsersMap                    mPendingGroupUsersMap;
    RootUsers                        mPendingRootUsers;
    UserIdsSet                       mPendingMetaAdminUsers;
    UserIdsSet                       mPendingMetaStatsUsers;
    UserIdsSet                       mPendingDelegationRenewAndCancelUsers;
    UidNameMap                       mTmpUidNameMap;
    GidNameMap                       mTmpGidNameMap;
    NameUidMap                       mTmpNameUidMap;
    NameGidMap                       mTmpNameGidMap;
    GroupUsersMap                    mTmpGroupUsersMap;
    GroupUsersNamesMap               mTmpGroupUserNamesMap;
    RootUsers                        mTmpRootUsers;
    UserIdsSet                       mTmpMetaAdminUsers;
    UserIdsSet                       mTmpMetaStatsUsers;
    UserIdsSet                       mTmpDelegationRenewAndCancelUsers;
    RootGroups                       mRootGroups;
    RootUserNames                    mRootUserNames;
    string                           mOmitUserPrefix;
    string                           mOmitGroupPrefix;
    UserIdsSet                       mMetaServerAdminUsers;
    UserIdsSet                       mMetaServerStatsUsers;
    MetaAdminUserNames               mMetaAdminUserNames;
    MetaAdminGroupNames              mMetaAdminGroupNames;
    MetaStatsUserNames               mMetaStatsUserNames;
    MetaStatsGroupNames              mMetaStatsGroupNames;
    DelegationUserNames              mDelegationUserNames;
    DelegationGroupNames             mDelegationGroupNames;
    Properties                       mParameters;
    bool                             mSetDefaultsFlag;

    friend class UserAndGroup;

    template<typename IT, typename T>
    int InsertTmpUsers(
        IT    inBegin,
        IT    inEnd,
        T&    outSet,
        bool& outOverflowFlag)
    {
        int theError = 0;
        for (IT theIt = inBegin; theIt != inEnd; ++theIt) {
            const UidAndGid* theUidPtr = mTmpNameUidMap.Find(*theIt);
            if (! theUidPtr) {
                KFS_LOG_STREAM_ERROR <<
                    "no such user: " << *theIt <<
                KFS_LOG_EOM;
                if (theError == 0) {
                    theError = -EINVAL;
                    continue;
                }
            }
            if (outSet.MaxSize() <= outSet.GetSize()) {
                KFS_LOG_STREAM_ERROR <<
                    "user table size exceed max allowed"
                    " size: "         << outSet.GetSize() <<
                    " ignoring user:" << *theIt <<
                KFS_LOG_EOM;
                outOverflowFlag = true;
                continue;
            }
            bool theInsertedFlag = false;
            outSet.Insert(theUidPtr->mUid, theUidPtr->mUid, theInsertedFlag);
        }
        return theError;
    }
    template<typename IT, typename T>
    int InsertTmpGroupsUsers(
        IT    inBegin,
        IT    inEnd,
        T&    outSet,
        bool& outOverflowFlag)
    {
        int theError = 0;
        for (IT theIt = inBegin; theIt != inEnd; ++theIt) {
            const kfsGid_t* const theGidPtr = mTmpNameGidMap.Find(*theIt);
            if (! theGidPtr) {
                KFS_LOG_STREAM_ERROR <<
                    "no such group: " << *theIt <<
                KFS_LOG_EOM;
                if (theError == 0) {
                    theError = -EINVAL;
                    continue;
                }
                continue;
            }
            const UsersSet* const theUsersSetPtr =
                mTmpGroupUsersMap.Find(*theGidPtr);
            if (! theUsersSetPtr) {
                KFS_LOG_STREAM_ERROR <<
                    "no such group id: " << *theIt <<
                KFS_LOG_EOM;
                if (theError == 0) {
                    theError = -EINVAL;
                    continue;
                }
            }
            for (UsersSet::const_iterator theIdIt = theUsersSetPtr->begin();
                    theIdIt != theUsersSetPtr->end();
                    ++theIdIt) {
                if (outSet.MaxSize() <= outSet.GetSize()) {
                    KFS_LOG_STREAM_ERROR <<
                        "user table size exceed max allowed"
                        " size: "          << outSet.GetSize() <<
                        " ignoring group:" << *theIt <<
                        " user id: "       << *theIdIt <<
                    KFS_LOG_EOM;
                    outOverflowFlag = true;
                    continue;
                }
                bool theInsertedFlag = false;
                outSet.Insert(*theIdIt, *theIdIt, theInsertedFlag);
            }
        }
        return theError;
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

UserAndGroup::UserAndGroup(
    bool inUseDefaultsFlag)
    : mImpl(*(new Impl(inUseDefaultsFlag))),
      mUpdateCount(mImpl.mUpdateCount),
      mGroupUsersMap(mImpl.mGroupUsersMap),
      mNameUidMapPtr(&mImpl.mNameUidMapPtr),
      mUidNameMapPtr(&mImpl.mUidNameMapPtr),
      mGidNameMapPtr(&mImpl.mGidNameMapPtr),
      mNameGidMap(mImpl.mNameGidMap),
      mNameUidPtr(mImpl.mNameUidPtr),
      mUidNamePtr(mImpl.mUidNamePtr),
      mRootUsersPtr(mImpl.mRootUsersPtr),
      mGidNamePtr(mImpl.mGidNamePtr),
      mMetaServerAdminUsers(mImpl.mMetaServerAdminUsers),
      mMetaServerStatsUsers(mImpl.mMetaServerStatsUsers),
      mDelegationRenewAndCancelUsersPtr(
        mImpl.mDelegationRenewAndCancelUsersPtr)
{
}

UserAndGroup::~UserAndGroup()
{
    delete &mImpl;
}

    int
UserAndGroup::Start()
{
    return mImpl.Start();
}

    void
UserAndGroup::Shutdown()
{
    mImpl.Shutdown();
}

    void
UserAndGroup::ScheduleUpdate()
{
    mImpl.ScheduleUpdate();
}

    int
UserAndGroup::SetParameters(
    const char*       inPrefixPtr,
    const Properties& inProperties)
{
    return mImpl.SetParameters(inPrefixPtr, inProperties);
}

    bool
UserAndGroup::IsUpdatePending() const
{
    return mImpl.IsUpdatePending();
}

const string                    UserAndGroup::kEmptyString;
const UserAndGroup::NameAndGid  UserAndGroup::kNameAndGroupNone;

}
