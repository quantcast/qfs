//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/02
// Author: Sriram Rao
//         Mike Ovsiannikov implement multiple outstanding request processing,
//         and "client threads".
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
// \file ClientSM.h
// \brief Kfs client protocol state machine responsible for handling the
//  corresponding network io: receiving, and parsing request, and creating and
//  sending response.
//
//----------------------------------------------------------------------------

#ifndef META_CLIENTSM_H
#define META_CLIENTSM_H

#include "ClientManager.h"
#include "kfsio/KfsCallbackObj.h"
#include "kfsio/NetConnection.h"
#include "kfsio/SslFilter.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/DelegationToken.h"
#include "qcdio/QCDLList.h"

#include <string>

namespace KFS
{
using std::string;

class Properties;
class AuthContext;
struct MetaRequest;
struct MetaAuthenticate;
struct MetaDelegate;
struct MetaLookup;
struct MetaDelegateCancel;

class ClientSM :
    public  KfsCallbackObj,
    private SslFilterVerifyPeer,
    private SslFilterServerPsk
{
public:
    ClientSM(
        const NetConnectionPtr&      conn,
        ClientManager::ClientThread* thread      = 0,
        IOBuffer::WOStream*          wostr       = 0,
        char*                        parseBuffer = 0);
    virtual ~ClientSM();

    //
    // Sequence:
    //  Client connects.
    //   - A new client sm is born
    //   - reads a request out of the connection
    //   - submit the request for execution
    //   - when the request is done, send a response back.
    //
    int HandleRequest(int code, void *data);
    ClientSM*& GetNext()
        { return mNext; }
    const NetConnectionPtr& GetConnection() const
        { return mNetConnection; }
    const AuthContext& GetAuthContext() const
        { return mAuthContext; }
    virtual bool Verify(
	string&       ioFilterAuthName,
        bool          inPreverifyOkFlag,
        int           inCurCertDepth,
        const string& inPeerName,
        int64_t       inEndTime,
        bool          inEndTimeValidFlag);
    virtual unsigned long GetPsk(
        const char*    inIdentityPtr,
        unsigned char* inPskBufferPtr,
        unsigned int   inPskBufferLen,
        string&        outAuthName);

    static void SetParameters(const Properties& prop);
    static int GetClientCount() { return sClientCount; }
    bool Handle(MetaAuthenticate& op);
    bool Handle(MetaDelegate& op);
    bool Handle(MetaLookup& op);
    bool Handle(MetaDelegateCancel& op);
    bool Handle(MetaAllocate& op);
private:
    /// A handle to a network connection
    NetConnectionPtr                   mNetConnection;
    const string                       mClientIp;
    int                                mPendingOpsCount;
    IOBuffer::WOStream&                mOstream;
    char* const                        mParseBuffer;
    int                                mRecursionCnt;
    /// used to print message about old protocol version once
    int                                mClientProtoVers;
    bool                               mDisconnectFlag:1;
    bool                               mDelegationValidFlag:1;
    int                                mLastReadLeft;
    MetaAuthenticate*                  mAuthenticateOp;
    kfsUid_t                           mAuthUid;
    kfsGid_t                           mAuthGid;
    kfsUid_t                           mAuthEUid;
    kfsGid_t                           mAuthEGid;
    uint16_t                           mDelegationFlags;
    uint32_t                           mDelegationValidForTime;
    int64_t                            mDelegationIssuedTime;
    DelegationToken::TokenSeq          mDelegationSeq;
    uint64_t                           mCanceledTokensUpdateCount;
    int64_t                            mSessionExpirationTime;
    int64_t                            mCredExpirationTime;
    ClientManager::ClientThread* const mClientThread;
    AuthContext&                       mAuthContext;
    uint64_t                           mAuthUpdateCount;
    uint64_t                           mUserAndGroupUpdateCount;
    ClientSM*                          mNext;
    ClientSM*                          mPrevPtr[1];
    ClientSM*                          mNextPtr[1];

    friend class QCDLListOp<ClientSM, 0>;
    typedef QCDLList<ClientSM, 0> ClientSMList;

    int HandleRequestSelf(int code, void *data);
    /// Given a (possibly) complete op in a buffer, run it.
    void HandleClientCmd(IOBuffer& iobuf, int cmdLen);

    /// Op has finished execution.  Send a response to the client.
    void SendResponse(MetaRequest *op);
    void CmdDone(MetaRequest& op);
    bool IsOverPendingOpsLimit() const
        { return (mPendingOpsCount >= sMaxPendingOps); }
    void HandleAuthenticate(IOBuffer& iobuf);
    void HandleDelegation(MetaDelegate& op);
    void CloseConnection(const char* msg = 0);

    static int  sMaxPendingOps;
    static int  sMaxPendingBytes;
    static int  sMaxReadAhead;
    static int  sInactivityTimeout;
    static int  sMaxWriteBehind;
    static int  sBufCompactionThreshold;
    static int  sOutBufCompactionThreshold;
    static int  sAuthMaxTimeSkew;
    static int  sClientCount;
    static bool sAuditLoggingFlag;
    static ClientSM* sClientSMPtr[1];
    static IOBuffer::WOStream sWOStream;
};

}

#endif // META_CLIENTSM_H
