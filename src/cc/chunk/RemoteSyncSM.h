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

class QCMutex;

namespace KFS
{
using std::map;
using std::list;
using std::less;
using std::find_if;

class RemoteSyncSMTimeoutImpl;
class Properties;
class RemoteSyncSMList;
class ClientThread;
class RemoteSyncSM;
struct KfsOp;

class ClientThreadRemoteSyncListEntry
{
protected:
    typedef boost::shared_ptr<RemoteSyncSM> SMPtr;
    ClientThreadRemoteSyncListEntry(
        ClientThread* inThreadPtr)
        : mClientThreadPtr(inThreadPtr),
          mOpsHeadPtr(0),
          mOpsTailPtr(0),
          mNextPtr(0),
          mFinishPtr()
        {}
    ~ClientThreadRemoteSyncListEntry();
    inline NetManager& GetNetManager();
    void DispatchEnqueue(
        RemoteSyncSM& inSyncSM,
        KfsOp&        inOp);
    void DispatchFinish(
        RemoteSyncSM& inSyncSM);
    bool IsClientThread()
        { return (mClientThreadPtr != 0); }
private:
    ClientThread* const mClientThreadPtr;
    KfsOp*              mOpsHeadPtr;
    KfsOp*              mOpsTailPtr;
    RemoteSyncSM*       mNextPtr;
    SMPtr               mFinishPtr;

    bool IsPending() const
        { return (mOpsHeadPtr || mFinishPtr); }

    static inline bool Enqueue(
        RemoteSyncSM& inSyncSM,
        KfsOp&        inOp);
    static inline void Finish(
        RemoteSyncSM& inSyncSM);
    static inline bool RemoveFromList(
        RemoteSyncSM& inSyncSM);
private:
    ClientThreadRemoteSyncListEntry(
        const ClientThreadRemoteSyncListEntry& inEntry);
    ClientThreadRemoteSyncListEntry& operator=(
        const ClientThreadRemoteSyncListEntry& inEntry);
friend class ClientThreadImpl;
};

// State machine for communication with other chunk servers: daisy chain rpc
// forwarding, and re-replication data and meta-data chunk read.
class RemoteSyncSM : public KfsCallbackObj,
                     public ClientThreadRemoteSyncListEntry,
                     public boost::enable_shared_from_this<RemoteSyncSM>
{
public:
    typedef ClientThreadRemoteSyncListEntry::SMPtr SMPtr;
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
        string&               errMsg,
        bool                  connectFlag              = false,
        bool                  forceUseClientThreadFlag = false);
    ~RemoteSyncSM();
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
    static SMPtr FindServer(
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

    NetConnectionPtr    mNetConnection;
    ServerLocation      mLocation;
    /// Assign a sequence # for each op we send to the remote server
    kfsSeq_t            mSeqnum;
    /// Queue of outstanding ops sent to remote server.
    DispatchedOps       mDispatchedOps;
    kfsSeq_t            mReplySeqNum;
    int                 mReplyNumBytes;
    int                 mRecursionCount;
    time_t              mLastRecvTime;
    string              mSessionId;
    CryptoKeys::Key     mSessionKey;
    bool                mShutdownSslFlag;
    bool                mSslShutdownInProgressFlag;
    time_t              mCurrentSessionExpirationTime;
    time_t              mSessionExpirationTime;
    IOBuffer::IStream   mIStream;
    IOBuffer::WOStream  mWOStream;
    SMList*             mList;
    SMList::iterator    mListIt;
    int                 mConnectCount;

    RemoteSyncSM(
        const ServerLocation& location,
        ClientThread*         thread);

    void SetSessionKey(
        const char*            inIdPtr,
        int                    inIdLen,
        const CryptoKeys::Key& inKey)
    {
        mSessionId.assign(inIdPtr, inIdLen);
        mSessionKey = inKey;
    }
    bool Connect();
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
    kfsSeq_t NextSeqnum();

    /// We (may) have got a response from the peer.  If we are doing
    /// re-replication, then we need to wait until we got all the data
    /// for the op; in such cases, we need to know if we got the full
    /// response. 
    /// @retval 0 if we got the response; -1 if we need to wait
    int HandleResponse(IOBuffer *iobuf, int cmdLen);
    void FailAllOps();
    bool EnqueueSelf(KfsOp* op);
    void FinishSelf();
    inline void UpdateRecvTimeout();
    inline static QCMutex* GetMutexPtr();

    static bool  sTraceRequestResponse;
    static int   sOpResponseTimeoutSec;
    static Auth* sAuthPtr;

    friend class RemoteSyncSMList;
    friend class ClientThreadRemoteSyncListEntry;
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

inline bool
ClientThreadRemoteSyncListEntry::Enqueue(
    RemoteSyncSM& inSyncSM,
    KfsOp&        inOp)
{
    return inSyncSM.EnqueueSelf(&inOp);
}

inline void
ClientThreadRemoteSyncListEntry::Finish(
    RemoteSyncSM& inSyncSM)
{
    inSyncSM.FinishSelf();
}

inline bool
ClientThreadRemoteSyncListEntry::RemoveFromList(
    RemoteSyncSM& inSyncSM)
{
    return inSyncSM.RemoveFromList();
}

}

#endif // CHUNKSERVER_REMOTESYNCSM_H
