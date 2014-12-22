//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/01
// Author: Sriram Rao, Mike Ovsiannikov
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
// \file NetDispatch.h
// \brief Meta-server request processor(s).
//
//----------------------------------------------------------------------------

#ifndef META_NETDISPATCH_H
#define META_NETDISPATCH_H

#include "ClientManager.h"
#include "ChunkServerFactory.h"
#include "kfsio/DelegationToken.h"

#include <ostream>

class QCMutex;

namespace KFS
{
using std::ostream;
class Properties;
class IOBuffer;
class CryptoKeys;

class NetDispatch
{
public:
    NetDispatch();
    ~NetDispatch();
    void SetMaxClientSockets(int count);
    int GetMaxClientCount() const;
    bool Bind(
        const ServerLocation& clientListenerLocation,
        bool                  clientListenerIpV6OnlyFlag,
        const ServerLocation& chunkServerListenerLocation,
        bool                  chunkServerListenerIpV6OnlyFlag);
    bool Start();
    //!< Dispatch completed request.
    void Dispatch(MetaRequest* r);
    void SetParameters(const Properties& props);
    void GetStatsCsv(ostream& os);
    void GetStatsCsv(IOBuffer& buf);
    int64_t GetUserCpuMicroSec() const;
    int64_t GetSystemCpuMicroSec() const;
    QCMutex* GetMutex() const { return mMutex; }
    QCMutex* GetClientManagerMutex() const { return mClientManagerMutex; }
    const CryptoKeys* GetCryptoKeys() const { return mCryptoKeys; }
    bool IsRunning() const { return mRunningFlag; }
    void ChildAtFork();
    void PrepareCurrentThreadToFork();
    inline void PrepareToFork();
    inline void ForkDone();
    bool CancelToken(const DelegationToken& token);
    bool CancelToken(
        int64_t inExpiration, int64_t inIssued, kfsUid_t inUid,
        DelegationToken::TokenSeq inSeq, uint16_t inFlags);
    bool IsCanceled(const DelegationToken& inToken, uint64_t& outUpdateCount);
    bool IsCanceled(const DelegationToken& inToken)
    {
        uint64_t cnt = 0;
        return IsCanceled(inToken, cnt);
    }
    bool IsCanceled(
        int64_t inExpiration, int64_t inIssued, kfsUid_t inUid,
        DelegationToken::TokenSeq inSeq, uint16_t inFlags,
        uint64_t& outUpdateCount);
    bool IsCanceled(
        int64_t inExpiration, int64_t inIssued, kfsUid_t inUid,
        DelegationToken::TokenSeq inSeq, uint16_t inFlags)
    {
        uint64_t cnt = 0;
        return IsCanceled(inExpiration, inIssued, inUid, inSeq, inFlags, cnt);
    }
    int WriteCanceledTokens(ostream& os);
    uint64_t GetCanceledTokensUpdateCount() const;
private:
    class CanceledTokens;

    ClientManager      mClientManager; //!< tracks the connected clients
    ChunkServerFactory mChunkServerFactory; //!< creates chunk servers when they connect
    QCMutex*           mMutex;
    QCMutex*           mClientManagerMutex;
    CryptoKeys*        mCryptoKeys;
    CanceledTokens&    mCanceledTokens;
    bool               mRunningFlag;
    int                mClientThreadCount;
    int                mClientThreadsStartCpuAffinity;
};

extern NetDispatch gNetDispatch;
}

#endif // META_NETDISPATCH_H
