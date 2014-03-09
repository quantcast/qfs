//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/24/16
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
// User and group information lookup class.
//
//----------------------------------------------------------------------------

#ifndef META_USER_AND_GROUP_H
#define META_USER_AND_GROUP_H

#include "common/kfstypes.h"
#include "common/LinearHash.h"
#include "common/hsieh_hash.h"
#include "common/StdAllocator.h"

#include <string>
#include <set>

#include <boost/shared_ptr.hpp>

namespace KFS
{
class Properties;

using std::string;
using std::set;
using std::less;
using boost::shared_ptr;

class UserAndGroup
{
public:
    enum
    {
        kLog2FirstBucketSize      = 10,
        kLog2MaxUserAndGroupCount = 24
    };
    class StringHash
    {
    public:
        static size_t Hash(const string& inString)
            { return HsiehHash(inString.data(), inString.size()); }
    };
    class NameAndGid
    {
    public:
        NameAndGid(
            const string& inName = string(),
            kfsGid_t      inGid  = kKfsGroupNone)
            : mName(inName),
              mGid(inGid)
            {}
        string   mName;
        kfsGid_t mGid;
    };
    class UidAndGid
    {
    public:
        UidAndGid(
            kfsUid_t inUid = kKfsUserNone,
            kfsGid_t inGid = kKfsGroupNone)
            : mUid(inUid),
              mGid(inGid)
            {}
        kfsUid_t mUid;
        kfsGid_t mGid;
    };
    typedef KVPair<kfsUid_t, NameAndGid> UidName;
    typedef KVPair<kfsGid_t, string    > GidName;
    typedef KVPair<string,   UidAndGid > NameUid;
    typedef KVPair<string,   kfsGid_t  > NameGid;
    typedef LinearHash<
        UidName,
        KeyCompare<UidName::Key>,
        DynamicArray<SingleLinkedList<UidName>*,
            kLog2FirstBucketSize, kLog2MaxUserAndGroupCount>,
        StdFastAllocator<UidName>
    > UidNameMap;
    typedef LinearHash<
        GidName,
        KeyCompare<GidName::Key>,
        DynamicArray<SingleLinkedList<GidName>*,
            kLog2FirstBucketSize, kLog2MaxUserAndGroupCount>,
        StdFastAllocator<GidName>
    > GidNameMap;
    typedef LinearHash<
        NameUid,
        KeyCompare<NameUid::Key, StringHash>,
        DynamicArray<SingleLinkedList<NameUid>*,
            kLog2FirstBucketSize, kLog2MaxUserAndGroupCount>,
        StdFastAllocator<NameUid>
    > NameUidMap;
    typedef LinearHash<
        NameGid,
        KeyCompare<NameGid::Key, StringHash>,
        DynamicArray<SingleLinkedList<NameGid>*,
            kLog2FirstBucketSize, kLog2MaxUserAndGroupCount>,
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
        DynamicArray<SingleLinkedList<GroupUsers>*,
            kLog2FirstBucketSize, kLog2MaxUserAndGroupCount>,
        StdFastAllocator<GroupUsers>
    > GroupUsersMap;
    typedef LinearHash<
        KeyOnly<kfsUid_t>,
        KeyCompare<kfsUid_t>,
        DynamicArray<SingleLinkedList<KeyOnly<kfsUid_t> >*,
            kLog2FirstBucketSize, kLog2MaxUserAndGroupCount>,
        StdFastAllocator<KeyOnly<kfsUid_t> >
    > UserIdsSet;
    typedef LinearHash<
        KeyOnly<kfsGid_t>,
        KeyCompare<kfsGid_t>,
        DynamicArray<SingleLinkedList<KeyOnly<kfsGid_t> >*,
            kLog2FirstBucketSize, kLog2MaxUserAndGroupCount>,
        StdFastAllocator<KeyOnly<kfsGid_t> >
    > GroupIdsSet;
    typedef UserIdsSet RootUsers;
    typedef shared_ptr<const NameUidMap> NameUidPtr;
    typedef shared_ptr<const UidNameMap> UidNamePtr;
    typedef shared_ptr<const RootUsers>  RootUsersPtr;
    typedef shared_ptr<const GidNameMap> GidNamePtr;
    typedef shared_ptr<const UserIdsSet> DelegationRenewAndCancelUsersPtr;

    UserAndGroup(
        bool inUseDefaultsFlag = true);
    ~UserAndGroup();
    int SetParameters(
        const char*       inPrefixPtr,
        const Properties& inProperties);
    int Start();
    void Shutdown();
    void ScheduleUpdate();
    const volatile uint64_t& GetUpdateCount() const
        { return mUpdateCount; }
    bool IsGroupMember(
        kfsUid_t inUser,
        kfsGid_t inGroup) const
    {
        const UsersSet* const theUsersPtr = mGroupUsersMap.Find(inGroup);
        return (theUsersPtr && theUsersPtr->find(inUser) != theUsersPtr->end());
    }
    const NameAndGid& GetUserNameAndGroup(
        const kfsUid_t inUid) const
    {
        const NameAndGid* const thePtr = (**mUidNameMapPtr).Find(inUid);
        return (thePtr ? *thePtr : kNameAndGroupNone);
    }
    const string& GetUserName(
        const kfsUid_t inUid) const
        { return GetUserNameAndGroup(inUid).mName; }
    const string& GetGroupName(
        const kfsGid_t inGid) const
    {
        const string* const thePtr = (**mGidNameMapPtr).Find(inGid);
        return (thePtr ? *thePtr : kEmptyString);
    }
    kfsUid_t GetUserId(
        const string& inUserName) const
    {
        const UidAndGid* const thePtr = (**mNameUidMapPtr).Find(inUserName);
        return (thePtr ? thePtr->mUid : kKfsUserNone);
    }
    bool GetGroupId(
        const string& inGroupName,
        kfsGid_t&     outGroupId) const
    {
        const kfsGid_t* const thePtr = mNameGidMap.Find(inGroupName);
        if (thePtr) {
            outGroupId = *thePtr;
            return true;
        }
        outGroupId = kKfsGroupNone;
        return false;
    }
    bool IsMetaServerAdminUser(
        kfsUid_t inUid)
        { return (mMetaServerAdminUsers.Find(inUid) != 0); }
    bool IsMetaServerStatsUser(
        kfsUid_t inUid)
        { return (mMetaServerStatsUsers.Find(inUid) != 0); }
    const NameUidPtr& GetNameUidPtr() const
        { return mNameUidPtr; }
    const UidNamePtr& GetUidNamePtr() const
        { return mUidNamePtr; }
    const RootUsersPtr& GetRootUsersPtr() const
        { return mRootUsersPtr; }
    const GidNamePtr& GetGidNamePtr() const
        { return mGidNamePtr; }
    const DelegationRenewAndCancelUsersPtr&
        GetDelegationRenewAndCancelUsersPtr() const
        { return mDelegationRenewAndCancelUsersPtr; }
    bool IsUpdatePending() const;
private:
    class Impl;
    Impl&                                   mImpl;
    const volatile uint64_t&                mUpdateCount;
    const GroupUsersMap&                    mGroupUsersMap;
    const NameUidMap* const* const          mNameUidMapPtr;
    const UidNameMap* const* const          mUidNameMapPtr;
    const GidNameMap* const* const          mGidNameMapPtr;
    const NameGidMap&                       mNameGidMap;
    const NameUidPtr&                       mNameUidPtr;
    const UidNamePtr&                       mUidNamePtr;
    const RootUsersPtr&                     mRootUsersPtr;
    const GidNamePtr&                       mGidNamePtr;
    const UserIdsSet&                       mMetaServerAdminUsers;
    const UserIdsSet&                       mMetaServerStatsUsers;
    const DelegationRenewAndCancelUsersPtr& mDelegationRenewAndCancelUsersPtr;

    static const string      kEmptyString;
    static const NameAndGid  kNameAndGroupNone;
private:
    UserAndGroup(
        const UserAndGroup& inUserAndGroup);
    UserAndGroup& operator=(
        const UserAndGroup& inUserAndGroup);
};

class UserAndGroupNames
{
public:
    typedef UserAndGroup::UidNameMap UidNameMap;
    typedef UserAndGroup::GidNameMap GidNameMap;

    UserAndGroupNames(
        const UidNameMap& inUidNameMap,
        const GidNameMap& inGidNameMap)
        : mUidNameMapPtr(&inUidNameMap),
          mGidNameMapPtr(&inGidNameMap)
        {}
    const string* GetUserName(
        kfsGid_t inUid) const
    {
        const UidNameMap::Val* const thePtr = mUidNameMapPtr->Find(inUid);
        return (thePtr ? &thePtr->mName : 0);
    }
    const string* GetGroupName(
        kfsGid_t inGid) const
        { return mGidNameMapPtr->Find(inGid); }
    void Set(
        const UidNameMap& inUidNameMap,
        const GidNameMap& inGidNameMap)
    {
        mUidNameMapPtr = &inUidNameMap;
        mGidNameMapPtr = &inGidNameMap;
    }
private:
    const UidNameMap* mUidNameMapPtr;
    const GidNameMap* mGidNameMapPtr;
};

}

#endif /* META_USER_AND_GROUP_H */
