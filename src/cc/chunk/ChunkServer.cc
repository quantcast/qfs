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

#include "kfsio/Globals.h"

#include "ChunkServer.h"
#include "Logger.h"
#include "utils.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace KFS {

using std::string;
using libkfsio::globalNetManager;


ChunkServer gChunkServer;

void
ChunkServer::SendTelemetryReport(KfsOp_t /* op */, double /* timeSpent */)
{
}

bool
ChunkServer::Init(int clientAcceptPort, const string& serverIp)
{
    if (clientAcceptPort < 0) {
        KFS_LOG_STREAM_FATAL <<
            "invalid client port: " << clientAcceptPort <<
        KFS_LOG_EOM;
        return false;
    }
    mUpdateServerIpFlag = serverIp.empty();
    if (! mUpdateServerIpFlag) {
        // For now support only ipv4 addresses.
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
        struct in_addr addr;
        if (! inet_aton(serverIp.c_str(), &addr)) {
            KFS_LOG_STREAM_FATAL <<
                "invalid server ip: " << serverIp <<
            KFS_LOG_EOM;
            return false;
        }
    }
    if (! gClientManager.BindAcceptor(clientAcceptPort) ||
            gClientManager.GetPort() <= 0) {
        KFS_LOG_STREAM_FATAL <<
            "failed to bind acceptor to port: " << clientAcceptPort <<
        KFS_LOG_EOM;
        return false;
    }
    mLocation.Reset(serverIp.c_str(), gClientManager.GetPort());
    return true;
}

bool
ChunkServer::MainLoop()
{
    if (gChunkManager.Restart() != 0) {
        return false;
    }
    gLogger.Start();
    gChunkManager.Start();
    if (! gClientManager.StartListening()) {
        KFS_LOG_STREAM_FATAL <<
            "failed to start acceptor on port: " << gClientManager.GetPort() <<
        KFS_LOG_EOM;
        return false;
    }
    gMetaServerSM.Init();

    globalNetManager().MainLoop();

    list<RemoteSyncSMPtr> serversToRelease;
    mRemoteSyncers.swap(serversToRelease);
    ReleaseAllServers(serversToRelease);

    return true;
}

void
StopNetProcessor(int /* status */)
{
    globalNetManager().Shutdown();
}

RemoteSyncSMPtr
ChunkServer::FindServer(const ServerLocation &location, bool connect)
{
    return KFS::FindServer(mRemoteSyncers, location, connect);
}

void
ChunkServer::RemoveServer(RemoteSyncSM *target)
{
    KFS::RemoveServer(mRemoteSyncers, target);
}

}
