//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/07/18
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
// \brief Utilities for monitoring and administering from meta and chunk servers.
//----------------------------------------------------------------------------

#include "monutils.h"
#include "common/kfstypes.h"
#include "common/MsgLogger.h"
#include "common/RequestParser.h"
#include "kfsio/requestio.h"
#include "libclient/KfsClient.h"
#include "common/kfserrno.h"

#include <string.h>

#include <cassert>
#include <cerrno>
#include <sstream>
#include <boost/scoped_array.hpp>


namespace KFS_MON {

using std::ostream;
using std::ostringstream;
using std::string;
using boost::scoped_array;
using namespace KFS;

static const char *KFS_VERSION_STR = "KFS/1.0";

void
KfsMonOp::HandleResponse(const char* resp, int len, Properties& prop)
{
    BufferInputStream ist(resp, len);
    const char separator = ':';
    prop.loadProperties(ist, separator, false);
    const kfsSeq_t reqSeq = prop.getValue("Cseq", kfsSeq_t(-1));
    if (reqSeq != seq) {
        status = -EINVAL;
    } else {
        status = prop.getValue("Status", -1);
    }
    if (status >= 0) {
        ParseResponse(prop);
    } else {
        status = -KfsToSysErrno(-status);
    }
}

void
MetaPingOp::Request(ostream& os)
{
    os << "PING\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Cseq: " << seq << "\r\n\r\n";
}

void
MetaToggleWORMOp::Request(ostream& os)
{
    os << "TOGGLE_WORM\r\n";
    os << "Toggle-WORM: " << value << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Cseq: " << seq << "\r\n\r\n";
}

void
MetaToggleWORMOp::ParseResponse(const Properties&)
{
}

void
MetaPingOp::ParseResponse(const Properties& prop)
{
    const char delim = '\t';
    string serv = prop.getValue("Servers", "");
    size_t start = serv.find_first_of("s=");
    if (start == string::npos) {
        return;
    }

    string serverInfo;
    size_t end;
    while (start != string::npos) {
        end = serv.find_first_of(delim, start);

        if (end != string::npos)
            serverInfo.assign(serv, start, end - start);
        else
            serverInfo.assign(serv, start, serv.size() - start);

        this->upServers.push_back(serverInfo);
        start = serv.find_first_of("s=", end);
    }

    serv = prop.getValue("Down Servers", "");
    start = serv.find_first_of("s=");
    if (start == string::npos) {
        return;
    }

    while (start != string::npos) {
        end = serv.find_first_of(delim, start);

        if (end != string::npos)
            serverInfo.assign(serv, start, end - start);
        else
            serverInfo.assign(serv, start, serv.size() - start);

        this->downServers.push_back(serverInfo);
        start = serv.find_first_of("s=", end);
    }

}

void
ChunkPingOp::Request(ostream& os)
{
    os << "PING\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Cseq: " << seq << "\r\n\r\n";
}

void
ChunkPingOp::ParseResponse(const Properties& prop)
{
    location.hostname = prop.getValue("Meta-server-host", "");
    location.port = prop.getValue("Meta-server-port", 0);
    totalSpace = prop.getValue("Total-space", (long long) 0);
    usedSpace = prop.getValue("Used-space", (long long) 0);
}

void
MetaStatsOp::Request(ostream& os)
{
    os << "STATS\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Cseq: " << seq << "\r\n\r\n";
}

void
ChunkStatsOp::Request(ostream& os)
{
    os << "STATS\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Cseq: " << seq << "\r\n\r\n";
}

void
MetaStatsOp::HandleResponse(const char *resp, int len,
    Properties& prop)
{
    stats.clear();
    KfsMonOp::HandleResponse(resp, len, stats);
}

void
ChunkStatsOp::HandleResponse(const char *resp, int len,
    Properties& prop)
{
    stats.clear();
    KfsMonOp::HandleResponse(resp, len, stats);
}

void
MetaStatsOp::ParseResponse(const Properties& prop)
{
}

void
ChunkStatsOp::ParseResponse(const Properties& prop)
{
}

void
RetireChunkserverOp::Request(ostream& os)
{
    os << "RETIRE_CHUNKSERVER\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Downtime: " << downtime << "\r\n";
    os << "Chunk-server-name: " << chunkLoc.hostname << "\r\n";
    os << "Chunk-server-port: " << chunkLoc.port << "\r\n\r\n";
}

void
RetireChunkserverOp::ParseResponse(const Properties&)
{
}

int
DoOpCommon(KfsMonOp* op, TcpSocket* sock)
{
    ostringstream os;
    op->Request(os);
    const string str = os.str();
    int numIO = SendRequest(str.data(), str.size(), 0, 0, sock);
    if (numIO <= 0) {
        op->status = numIO < 0 ? numIO : -EINVAL;
        return op->status;
    }
    scoped_array<char> buf;
    // use a big buffer so we don't have issues about server responses
    // not fitting in
    const int kMaxRespsonseSize = 8 << 20;
    buf.reset(new char[kMaxRespsonseSize]);
    int len = 0;
    numIO = GetResponse(buf.get(), kMaxRespsonseSize, &len, sock);
    if (numIO <= 0 || len <= 0) {
        op->status = numIO < 0 ? numIO : -EINVAL;
        return op->status;
    }
    Properties prop;
    op->HandleResponse(buf.get(), len, prop);
    return numIO;
}

int
GetResponse(char *buf, int bufSize, int *delims, TcpSocket *sock)
{
    const int kTimeoutSec = 300;
    return RecvResponseHeader(buf, bufSize, sock, kTimeoutSec, delims);
}

int
ExecuteOp(const ServerLocation& location, KfsMonOp& op)
{
    TcpSocket sock;
    int ret;
    if ((ret = sock.Connect(location)) < 0) {
        KFS_LOG_STREAM_ERROR <<
            location.ToString() <<
            ": " << ErrorCodeToStr(ret) <<
        KFS_LOG_EOM;
        return ret;
    }
    ret = DoOpCommon(&op, &sock);
    if (ret > 0) {
        ret = op.status;
    }
    if (ret < 0) {
        KFS_LOG_STREAM_ERROR <<
            location.ToString() <<
            ": " << ErrorCodeToStr(ret) <<
        KFS_LOG_EOM;
        return ret;
    }
    sock.Close();
    return ret;
}

}
