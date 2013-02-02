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
//
//----------------------------------------------------------------------------

#ifndef TOOLS_MONUTILS_H
#define TOOLS_MONUTILS_H

#include "kfsio/TcpSocket.h"
#include "common/Properties.h"
#include "common/kfstypes.h"
#include "common/kfsdecls.h"

#include <vector>
#include <string>
#include <ostream>

namespace KFS_MON
{
using std::ostream;
using std::vector;
using std::string;
using namespace KFS;

    enum KfsMonOp_t {
        CMD_METAPING,
        CMD_CHUNKPING,
        CMD_METASTATS,
        CMD_METATOGGLE_WORM,
        CMD_CHUNKSTATS,
        CMD_RETIRE_CHUNKSERVER
    };

    struct KfsMonOp {
        const KfsMonOp_t op;
        kfsSeq_t         seq;
        int              status;
        KfsMonOp(KfsMonOp_t o, kfsSeq_t s)
            : op(o), seq(s)
            {}
        virtual ~KfsMonOp() {}
        virtual void Request(ostream &os) = 0;
        virtual void ParseResponse(const Properties& prop) = 0;
        virtual void HandleResponse(const char *resp, int len,
            Properties& prop);
    private:
        KfsMonOp(const KfsMonOp&);
        KfsMonOp& operator=(const KfsMonOp&);
    };

    struct MetaPingOp : public KfsMonOp {
        vector<string> upServers; /// result
        vector<string> downServers; /// result
        MetaPingOp(kfsSeq_t s) :
            KfsMonOp(CMD_METAPING, s) { };
        void Request(ostream& os);
        void ParseResponse(const Properties& prop);
    };

    struct MetaToggleWORMOp : public KfsMonOp {
        int value;
        MetaToggleWORMOp(kfsSeq_t s, int v)
            : KfsMonOp(CMD_METATOGGLE_WORM, s), value(v)
            {}
        void Request(ostream& os);
        void ParseResponse(const Properties& prop);
    };


    struct ChunkPingOp : public KfsMonOp {
        ServerLocation location;
        int64_t totalSpace;
        int64_t usedSpace;
        ChunkPingOp(kfsSeq_t s)
            : KfsMonOp(CMD_CHUNKPING, s)
            {}
        void Request(ostream& os);
        void ParseResponse(const Properties& prop);
    };

    struct MetaStatsOp : public KfsMonOp {
        Properties stats; // result
        MetaStatsOp(kfsSeq_t s)
            : KfsMonOp(CMD_METASTATS, s)
            {}
        void Request(ostream& os);
        void HandleResponse(const char *resp, int len,
            Properties& prop);
        void ParseResponse(const Properties& prop);
    };

    struct ChunkStatsOp : public KfsMonOp {
        Properties stats; // result
        ChunkStatsOp(kfsSeq_t s)
            : KfsMonOp(CMD_CHUNKSTATS, s)
            {}
        void Request(ostream& os);
        void HandleResponse(const char *resp, int len,
            Properties& prop);
        void ParseResponse(const Properties& prop);
    };

    struct RetireChunkserverOp : public KfsMonOp {
        ServerLocation chunkLoc;
        int downtime; // # of seconds of downtime
        RetireChunkserverOp(kfsSeq_t s, const ServerLocation &c, int d)
            : KfsMonOp(CMD_RETIRE_CHUNKSERVER, s), chunkLoc(c), downtime(d)
            {}
        void Request(ostream& os);
        void ParseResponse(const Properties& prop);
    };

    int DoOpCommon(KfsMonOp* op, TcpSocket* sock);
    int GetResponse(char* buf, int bufSize, int* delims, TcpSocket* sock);
    int ExecuteOp(const ServerLocation& location, KfsMonOp& op);
}

#endif // TOOLS_MONUTILS_H
