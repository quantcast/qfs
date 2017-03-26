//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2016/05/06
// Author: Mike Ovsiannikov
//
// Copyright 2016 Quantcast Corp.
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
// Meta server global variables initialization.
//
//----------------------------------------------------------------------------

#include "Checkpoint.h"
#include "LogWriter.h"
#include "MetaRequest.h"
#include "NetDispatch.h"
#include "ChunkServer.h"
#include "Replay.h"
#include "ChildProcessTracker.h"
#include "LayoutManager.h"
#include "MetaDataStore.h"
#include "ClientSM.h"
#include "meta.h"
#include "kfstree.h"

#include "kfsio/Globals.h"

#include <new>

#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>

namespace KFS
{
using std::set_new_handler;
using KFS::libkfsio::globalNetManager;
using KFS::libkfsio::InitGlobals;
using KFS::libkfsio::globals;

class MetaServerGlobals
{
public:
    MetaServerGlobals()
        : mCPDIR("./kfscp"),           //!< directory for CP files
          mLASTCP(mCPDIR + "/" +       //!< most recent CP file (link)
            MetaDataStore::GetCheckpointLatestFileNamePtr()),
          mFileID(0, ROOTFID),
          mChunkID(1, ROOTFID),
          mMetatree(),
          mCheckpoint(mCPDIR),
          mReplayer(),
          mChildProcessTracker(),
          mNetDispatch(),
          mLogWriter()
        {}
    string                    mCPDIR;
    string                    mLASTCP;
    UniqueID                  mFileID;
    UniqueID                  mChunkID;
    Tree                      mMetatree;
    Checkpoint                mCheckpoint;
    Replay                    mReplayer;
    ChildProcessTrackingTimer mChildProcessTracker;
    NetDispatch               mNetDispatch;
    LogWriter                 mLogWriter;
private:
    MetaServerGlobals(const MetaServerGlobals&);
    MetaServerGlobals& operator=(const MetaServerGlobals&);
};

class ObjectCountsChecker
{
public:
    ObjectCountsChecker()
        : mCheckLeaksFlag(IsLeaksChecksEnabled()),
          mWriteError(0)
        {}
    ~ObjectCountsChecker()
    {
        if (mCheckLeaksFlag) {
            Check("object leaks detected\n", &mWriteError);
        }
    }
    static void Check(
        const char* inMsgPtr,
        int*        inLastErrorPtr = 0)
    {
        if (0 != MetaNode::getPoolAllocator<Node>().GetInUseCount() ||
                0 != MetaNode::getPoolAllocator<MetaDentry>().GetInUseCount() ||
                0 != MetaNode::getPoolAllocator<MetaFattr>().GetInUseCount() ||
                0 != CSMap::Entry::GetAllocBlockCount() ||
                0 != globals().ctrOpenNetFds.GetValue() ||
                0 != globals().ctrOpenDiskFds.GetValue() ||
                0 != ClientSM::GetClientCount() ||
                0 != MetaRequest::GetRequestCount() ||
                0 != ChunkServer::GetChunkServerCount()) {
            if (inMsgPtr && *inMsgPtr) {
                if (write(2, inMsgPtr, strlen(inMsgPtr))) {
                    if (inLastErrorPtr) {
                        *inLastErrorPtr = errno;
                    }
                }
            }
            abort();
        }
    }
private:
    const bool mCheckLeaksFlag;
    int        mWriteError;

    static bool IsLeaksChecksEnabled()
    {
        const char* const thePtr = getenv("QFS_DEBUG_CHECK_LEAKS_ON_EXIT");
        if (! thePtr || ! *thePtr) {
            return false;
        }
        const int theSym = *thePtr & 0xFF;
        return ('0' != theSym && 'N' != theSym && 'n' != theSym);
    }
};

static void
NewHandler()
{
    const bool kReportSysErrorFlag = true;
    panic("memory allocation failed", kReportSysErrorFlag);
}

static MetaServerGlobals&
InitializeMetaServerGlobals()
{
    set_new_handler(&NewHandler);
    InitGlobals();
    globalNetManager();
    ObjectCountsChecker::Check("initialization: counts are not 0\n");
    static ObjectCountsChecker sObjectCountsChecker;
    static MetaServerGlobals   sMetaServerGlobals;
    return sMetaServerGlobals;
}
static MetaServerGlobals& sMetaServerGlobals = InitializeMetaServerGlobals();

const string& LASTCP                  = sMetaServerGlobals.mLASTCP;
UniqueID&     fileID                  = sMetaServerGlobals.mFileID;
UniqueID&     chunkID                 = sMetaServerGlobals.mChunkID;
Tree&         metatree                = sMetaServerGlobals.mMetatree;
Checkpoint&   cp                      = sMetaServerGlobals.mCheckpoint;
Replay&       replayer                = sMetaServerGlobals.mReplayer;
ChildProcessTrackingTimer& gChildProcessTracker =
    sMetaServerGlobals.mChildProcessTracker;
NetDispatch&  gNetDispatch            = sMetaServerGlobals.mNetDispatch;
LogWriter&    MetaRequest::sLogWriter = sMetaServerGlobals.mLogWriter;

static LayoutManager&
InitializeLayoutManager()
{
    MetaRequest::Initialize();
    return LayoutManager::Instance();
}
ChunkServer::ChunkOpsInFlight ChunkServer::sChunkOpsInFlight;
ChunkServer::HelloInFlight    ChunkServer::sHelloInFlight;
Properties::String            ChunkServer::sSrvLoadPropName(
    "Buffer-usec-wait-avg");
LayoutManager& gLayoutManager = InitializeLayoutManager();

const UserAndGroup& MetaUserAndGroup::sUserAndGroup =
    gLayoutManager.GetUserAndGroup();

void
checkpointer_setup_paths(const string& cpdir)
{
    if (! cpdir.empty()) {
        sMetaServerGlobals.mCPDIR = cpdir;
        sMetaServerGlobals.mLASTCP = cpdir + "/" +
            MetaDataStore::GetCheckpointLatestFileNamePtr();
        cp.setCPDir(cpdir);
    }
}

}
