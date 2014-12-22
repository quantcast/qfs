//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/16
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

#ifndef _CHUNKSERVER_H
#define _CHUNKSERVER_H

#include "common/kfsdecls.h"
#include "RemoteSyncSM.h"

#include <list>
#include <vector>
#include <string>

class QCMutex;

namespace KFS
{
using std::string;
using std::vector;
using std::list;

// Chunk server globals and main event loop.
class ChunkServer
{
public:
    ChunkServer() :
        mOpCount(0),
        mUpdateServerIpFlag(false),
        mLocation(),
        mRemoteSyncers(),
        mMutex(0)
        {}

    bool Init(
        const ServerLocation& clientListener,
        bool                  ipV6OnlyFlag,
        const string&         serverIp,
        int                   threadCount,
        int                   firstCpuIdx);
    bool MainLoop(
        const vector<string>& chunkDirs,
        const Properties&     props,
        const string&         logDir);
    bool IsLocalServer(const ServerLocation& location) const {
        return mLocation == location;
    }
    RemoteSyncSMPtr FindServer(
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

    string GetMyLocation() const {
        return mLocation.ToString();
    }
    const ServerLocation& GetLocation() const {
        return mLocation;
    }
    void OpInserted() {
        mOpCount++;
    }
    void OpFinished() {
        mOpCount--;
        if (mOpCount < 0) {
            mOpCount = 0;
        }
    }
    int GetNumOps() const {
        return mOpCount;
    }
    bool CanUpdateServerIp() const {
        return mUpdateServerIpFlag;
    }
    inline void SetLocation(const ServerLocation& loc);
private:
    // # of ops in the system
    int              mOpCount;
    bool             mUpdateServerIpFlag;
    ServerLocation   mLocation;
    RemoteSyncSMList mRemoteSyncers;
    QCMutex*         mMutex;
private:
    // No copy.
    ChunkServer(const ChunkServer&);
    ChunkServer& operator=(const ChunkServer&);
};

extern ChunkServer gChunkServer;
}

#endif // _CHUNKSERVER_H
