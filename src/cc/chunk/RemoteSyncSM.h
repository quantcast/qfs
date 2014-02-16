//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/09/27
// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
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
// 
//----------------------------------------------------------------------------

#ifndef CHUNKSERVER_REMOTESYNCSM_H
#define CHUNKSERVER_REMOTESYNCSM_H

#include "common/kfsdecls.h"
#include "common/StdAllocator.h"
#include "kfsio/KfsCallbackObj.h"
#include "kfsio/NetConnection.h"
#include "kfsio/CryptoKeys.h"

#include <time.h>

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <map>
#include <list>
#include <algorithm>

namespace KFS
{
using std::map;
using std::list;
using std::less;
using std::find_if;

class RemoteSyncSMTimeoutImpl;
class Properties;
struct KfsOp;

// State machine for communication with other chunk servers: daisy chain rpc
// forwarding, and re-replication data and meta-data chunk read.
class RemoteSyncSM : public KfsCallbackObj,
                     public boost::enable_shared_from_this<RemoteSyncSM>
{
public:
    typedef boost::shared_ptr<RemoteSyncSM> SMPtr;
    typedef list<
        SMPtr,
        StdFastAllocator<SMPtr>
    > SMList;

    static SMPtr Create(
        const ServerLocation& location,
        const char*           sessionTokenPtr,
        int                   sessionTokenLen,
        const char*           sessionKeyPtr,
        int                   sessionKeyLen,
        bool                  writeMasterFlag,
        bool                  shutdownSslFlag,
        int&                  err,
        string&               errMsg);
    ~RemoteSyncSM();
    bool Connect();
    void SetSessionKey(
        const char*            inIdPtr,
        int                    inIdLen,
        const CryptoKeys::Key& inKey)
    {
        mSessionId.assign(inIdPtr, inIdLen);
        mSessionKey = inKey;
    }
    bool HasAuthentication() const
        { return ! mSessionId.empty(); }
    bool GetShutdownSslFlag() const
        { return mShutdownSslFlag; }
    void SetShutdownSslFlag(bool inFlag)
        { mShutdownSslFlag = inFlag; }
    void Enqueue(KfsOp* op);
    void Finish();
    int HandleEvent(int code, void *data);
    const ServerLocation& GetLocation() const {
        return mLocation;
    }
    bool UpdateSession(
        const char* sessionTokenPtr,
        int         sessionTokenLen,
        const char* sessionKeyPtr,
        int         sessionKeyLen,
        bool        writeMasterFlag,
        int&        err,
        string&     errMsg);
    static bool SetParameters(
        const char* prefix, const Properties& props, bool authEnabledFlag);
    static void Shutdown();
    static int GetResponseTimeoutSec() {
        return sOpResponseTimeoutSec;
    }
private:
    typedef map<
        kfsSeq_t,
        KfsOp*,
        less<kfsSeq_t>,
        StdFastAllocator<
            std::pair<const kfsSeq_t, KfsOp*>
        >
    > DispatchedOps;
    class Auth;

    NetConnectionPtr   mNetConnection;
    ServerLocation     mLocation;
    /// Assign a sequence # for each op we send to the remote server
    kfsSeq_t           mSeqnum;
    /// Queue of outstanding ops sent to remote server.
    DispatchedOps      mDispatchedOps;
    kfsSeq_t           mReplySeqNum;
    int                mReplyNumBytes;
    int                mRecursionCount;
    time_t             mLastRecvTime;
    string             mSessionId;
    CryptoKeys::Key    mSessionKey;
    bool               mShutdownSslFlag;
    bool               mSslShutdownInProgressFlag;
    time_t             mSessionExpirationTime;
    IOBuffer::IStream  mIStream;
    IOBuffer::WOStream mWOStream;
    SMList*            mList;
    SMList::iterator   mListIt;

    RemoteSyncSM(const ServerLocation& location);

    const SMPtr& PutInList(SMList& list)
    {
        SMPtr ptr = shared_from_this();
        RemoveFromList();
        mList = &list;
        mListIt = list.insert(list.end(), SMPtr());
        mListIt->swap(ptr);
        return *mListIt;
    }
    bool RemoveFromList()
    {
        if (! mList) {
            return false;
        }
        SMList& list = *mList;
        mList = 0;
        assert(! list.empty());
        list.erase(mListIt); // Can invoke destructor.
        return true;
    }
    bool RemoveFromList(SMList& list)
        { return (mList == &list && RemoveFromList()); }
   kfsSeq_t NextSeqnum();

    /// We (may) have got a response from the peer.  If we are doing
    /// re-replication, then we need to wait until we got all the data
    /// for the op; in such cases, we need to know if we got the full
    /// response. 
    /// @retval 0 if we got the response; -1 if we need to wait
    int HandleResponse(IOBuffer *iobuf, int cmdLen);
    void FailAllOps();
    inline void UpdateRecvTimeout();

    static bool  sTraceRequestResponse;
    static int   sOpResponseTimeoutSec;
    static Auth* sAuthPtr;

    friend class RemoteSyncSMList;
private:
    // No copy.
    RemoteSyncSM(const RemoteSyncSM&);
    RemoteSyncSM& operator=(const RemoteSyncSM&);
};

typedef RemoteSyncSM::SMPtr RemoteSyncSMPtr;
class RemoteSyncSMList
{
public:
    RemoteSyncSMList()
        : mList()
        {}
    ~RemoteSyncSMList()
        { RemoteSyncSMList::ReleaseAllServers(); }
    bool RemoveServer(RemoteSyncSM* target)
        { return (target && target->RemoveFromList(mList)); }
    void ReleaseAllServers()
    {
        while (! mList.empty()) {
            mList.front()->Finish();
        }
    }
    template<typename T>
    RemoteSyncSMPtr Find(T funct)
    {
        RemoteSyncSM::SMList::const_iterator const it = find_if(
            mList.begin(), mList.end(), funct);
        return (it == mList.end() ? RemoteSyncSMPtr() : *it);
    }
    RemoteSyncSMPtr PutInList(RemoteSyncSM& sm)
        { return sm.PutInList(mList); }
private:
    RemoteSyncSM::SMList mList;

    RemoteSyncSMList(const RemoteSyncSMList&);
    RemoteSyncSMList& operator=(const RemoteSyncSMList&);
};

RemoteSyncSMPtr FindServer(
    RemoteSyncSMList&     remoteSyncers,
    const ServerLocation& location,
    bool                  connectFlag,
    const char*           sessionTokenPtr,
    int                   sessionTokenLen,
    const char*           sessionKeyPtr,
    int                   sessionKeyLen,
    bool                  writeMasterFlag,
    bool                  shutdownSslFlag,
    int&                  err,
    string&               errMsg);

static inline bool RemoveServer(RemoteSyncSMList& remoteSyncers, RemoteSyncSM* target)
{
    return remoteSyncers.RemoveServer(target);
}

static inline void ReleaseAllServers(RemoteSyncSMList& remoteSyncers)
{
    remoteSyncers.ReleaseAllServers();
}

}

#endif // CHUNKSERVER_REMOTESYNCSM_H
