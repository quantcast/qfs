//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/23
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

#include "ChunkServer.h"
#include "ClientThread.h"
#include "Replicator.h"
#include "ClientManager.h"
#include "ChunkManager.h"
#include "MetaServerSM.h"
#include "Logger.h"
#include "utils.h"

#include "common/MsgLogger.h"
#include "kfsio/Globals.h"
#include "qcdio/qcstutils.h"

namespace KFS {

using std::string;
using libkfsio::globalNetManager;


ChunkServer gChunkServer;

bool
ChunkServer::Init(
    const ServerLocation& clientListener,
    bool                  ipV6OnlyFlag,
    const string&         serverIp,
    int                   threadCount,
    int                   firstCpuIdx)
{
    if (clientListener.port < 0) {
        KFS_LOG_STREAM_FATAL <<
            "invalid client listener: " << clientListener <<
        KFS_LOG_EOM;
        return false;
    }
    mUpdateServerIpFlag = serverIp.empty();
    if (! mUpdateServerIpFlag) {
        // For now support only ipv[46] addresses.
        // The ip does not have to be assigned to any local NICs.
        // The ip is valid as long as the clients can reach this particular
        // process using this ip.
        //
        // In the case when the chunk server is on the same host as the meta
        // server, but the clients aren't, the server ip must be specified.
        // Setting cnchunkServer.metaServer.hostname to the client "visible" ip
        // might also work.
        //
        // This also allows to work with NAT between the clients, and chunk and
        // meta servers.
        // The server ip can also be used for the testing purposes, so that the
        // clients always fail to connect to the chunk server, but the meta
        // server considers this server operational.
        const int err = TcpSocket::Validate(serverIp);
        if (err) {
            KFS_LOG_STREAM_FATAL <<
                "invalid server ip: " << serverIp <<
                " " <<
            KFS_LOG_EOM;
            return false;
        }
    }
    if (! gClientManager.BindAcceptor(
                clientListener,
                ipV6OnlyFlag,
                threadCount,
                firstCpuIdx,
                mMutex) ||
            gClientManager.GetPort() <= 0) {
        KFS_LOG_STREAM_FATAL <<
            "failed to bind acceptor to: " << clientListener <<
        KFS_LOG_EOM;
        return false;
    }
    mLocation.Reset(serverIp.c_str(), gClientManager.GetPort());
    return true;
}

class ClientThreadVerifier : public NetManager::Dispatcher
{
public:
    ClientThreadVerifier(
        const QCMutex* inMutex)
        : mMutex(inMutex)
        {}
    virtual void DispatchStart()
        { Verify("dispatch start"); }
    virtual void DispatchEnd()
        { Verify("dispatch end"); }
    virtual void DispatchExit()
        { Verify("dispatch exit"); }
private:
    const QCMutex* const mMutex;
    void Verify(const char* prefix)
    {
        if (! mMutex) {
            return;
        }
        if (! mMutex->IsOwned()) {
            die(prefix + string(": invalid client thread mutex state"));
        }
        if (ClientThread::GetCurrentClientThreadPtr()) {
            die(prefix + string(": invalid client thread pointer"));
        }
    }
private:
    ClientThreadVerifier(
        const ClientThreadVerifier&);
    ClientThreadVerifier& operator=(
        const ClientThreadVerifier&);
};

bool
ChunkServer::MainLoop(
    const vector<string>& chunkDirs,
    const Properties&     props,
    const string&         logDir)
{
    QCStMutexLocker lock(mMutex);

    assert(! mMutex || ! ClientThread::GetCurrentClientThreadPtr());
    if (! gChunkManager.Init(chunkDirs, props)) {
        gClientManager.Shutdown();
        return false;
    }
    gLogger.Init(logDir);
    if (gChunkManager.Restart() != 0) {
        gClientManager.Shutdown();
        return false;
    }
    gLogger.Start();
    gChunkManager.Start();
    if (! gClientManager.StartListening()) {
        KFS_LOG_STREAM_FATAL <<
            "failed to start acceptor on port: " << gClientManager.GetPort() <<
        KFS_LOG_EOM;
        gClientManager.Shutdown();
        return false;
    }
    gMetaServerSM.Init();
    {
        ClientThreadVerifier verifier(mMutex);
        QCStMutexUnlocker    unlocker(mMutex);
        const bool kWakeupAndCleanupFlag = true;
        globalNetManager().MainLoop(
            mMutex,
            kWakeupAndCleanupFlag,
            mMutex ? &verifier : 0
        );
    }
    Replicator::CancelAll();
    gClientManager.Stop();
    mRemoteSyncers.ReleaseAllServers();
    gChunkManager.Shutdown();
    RemoteSyncSM::Shutdown();
    gClientManager.Shutdown();
    Replicator::Shutdown();
    return true;
}

void
StopNetProcessor(int /* status */)
{
    globalNetManager().Shutdown();
}

RemoteSyncSMPtr
ChunkServer::FindServer(
    const ServerLocation& location,
    bool                  connectFlag,
    const char*           sessionTokenPtr,
    int                   sessionTokenLen,
    const char*           sessionKeyPtr,
    int                   sessionKeyLen,
    bool                  writeMasterFlag,
    bool                  shutdownSslFlag,
    int&                  err,
    string&               errMsg)
{
    return RemoteSyncSM::FindServer(
        mRemoteSyncers,
        location,
        connectFlag,
        sessionTokenPtr,
        sessionTokenLen,
        sessionKeyPtr,
        sessionKeyLen,
        writeMasterFlag,
        shutdownSslFlag,
        err,
        errMsg
    );
}

}
