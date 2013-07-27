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
#include "kfsio/IOBuffer.h"
#include "qcdio/QCDLList.h"

#include <string>

namespace KFS
{
using std::string;

class Properties;
class AuthContext;
struct MetaRequest;

class ClientSM : public KfsCallbackObj
{
public:
    ClientSM(const NetConnectionPtr&     conn,
        ClientManager::ClientThread* thread      = 0,
        IOBuffer::WOStream*          wostr       = 0,
        char*                        parseBuffer = 0);
    ~ClientSM();

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

    static void SetParameters(const Properties& prop);
    static int GetClientCount() { return sClientCount; }
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
    bool                               mDisconnectFlag;
    int                                mLastReadLeft;
    MetaAuthenticate*                  mAuthenticateOp;
    string                             mUserName;
    ClientManager::ClientThread* const mClientThread;
    ClientSM*                          mNext;
    ClientSM*                          mPrevPtr[1];
    ClientSM*                          mNextPtr[1];

    friend class QCDLListOp<ClientSM, 0>;
    typedef QCDLList<ClientSM, 0> ClientSMList;

    /// Given a (possibly) complete op in a buffer, run it.
    void HandleClientCmd(IOBuffer& iobuf, int cmdLen);

    /// Op has finished execution.  Send a response to the client.
    void SendResponse(MetaRequest *op);
    bool IsOverPendingOpsLimit() const
        { return (mPendingOpsCount >= sMaxPendingOps); }
    void HandleAuthenticate(IOBuffer& iobuf);
    inline AuthContext& GetAuthContext();

    static int  sMaxPendingOps;
    static int  sMaxPendingBytes;
    static int  sMaxReadAhead;
    static int  sInactivityTimeout;
    static int  sMaxWriteBehind;
    static int  sBufCompactionThreshold;
    static int  sOutBufCompactionThreshold;
    static int  sClientCount;
    static bool sAuditLoggingFlag;
    static ClientSM* sClientSMPtr[1];
    static IOBuffer::WOStream sWOStream;
};

}

#endif // META_CLIENTSM_H
