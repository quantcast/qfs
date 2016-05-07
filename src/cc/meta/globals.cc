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
#include "Replay.h"
#include "ChildProcessTracker.h"
#include "LayoutManager.h"
#include "meta.h"
#include "kfstree.h"

#include "kfsio/Globals.h"

namespace KFS
{
using KFS::libkfsio::globalNetManager;
using KFS::libkfsio::InitGlobals;

static bool InitializeIoGlobals()
{
    InitGlobals();
    globalNetManager();
    return true;
}
static const bool sIoGlobalsInitializedFlag = InitializeIoGlobals();

static string sCPDIR("./kfscp");           //!< directory for CP files
static string sLASTCP(sCPDIR + "/latest"); //!< most recent CP file (link)
const string& CPDIR  = sCPDIR;
const string& LASTCP = sLASTCP;

/*
 * Seed the unique id generators for files/chunks to start at 2
 */
static UniqueID sFileID(0, ROOTFID);
static UniqueID sChunkID(1, ROOTFID);
UniqueID& fileID  = sFileID;
UniqueID& chunkID = sChunkID;

static Tree sMetatree;
Tree& metatree = sMetatree;

static Checkpoint sCheckpoint(CPDIR);
Checkpoint& cp = sCheckpoint;

static Replay sReplayer;
Replay& replayer = sReplayer;

static ChildProcessTrackingTimer sChildProcessTracker;
ChildProcessTrackingTimer& gChildProcessTracker = sChildProcessTracker;

static NetDispatch sNetDispatch;
NetDispatch& gNetDispatch = sNetDispatch;

static LogWriter sLogWriterInstance;
LogWriter& MetaRequest::sLogWriter = sLogWriterInstance;

static const bool sMetaRequestInitedFlag = MetaRequest::Initialize();

LayoutManager& gLayoutManager = LayoutManager::Create();
const UserAndGroup& MetaUserAndGroup::sUserAndGroup =
    gLayoutManager.GetUserAndGroup();

void
checkpointer_setup_paths(const string& cpdir)
{
    if (! cpdir.empty()) {
        sCPDIR = cpdir;
        sLASTCP = cpdir + "/latest";
        cp.setCPDir(cpdir);
    }
}

}
