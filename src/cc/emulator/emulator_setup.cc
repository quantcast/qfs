//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/08/29
//

// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
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
// \brief Code to setup the emulator: load checkpoint, replay transaction logs,
// and load "network definition" -- chunk to chunk server assigment, rack
// assignment, and chunk server space utilization.
//----------------------------------------------------------------------------

#include "LayoutEmulator.h"
#include "emulator_setup.h"

#include "meta/kfstree.h"
#include "meta/Checkpoint.h"
#include "meta/Replay.h"
#include "meta/Restorer.h"
#include "meta/Logger.h"
#include "meta/util.h"
#include "common/MsgLogger.h"

#include <string>

namespace KFS
{
using std::string;

int
EmulatorSetup(
    string&  logdir,
    string&  cpdir,
    string&  networkFn,
    string&  chunkmapFn,
    int16_t  minReplicasPerFile,
    bool     addChunksToReplicationChecker)
{
    logger_setup_paths(logdir);
    checkpointer_setup_paths(cpdir);

    KFS_LOG_STREAM_INFO << "restoring from checkpoint: " << LASTCP <<
    KFS_LOG_EOM;
    int status;
    if (file_exists(LASTCP)) {
        Restorer r;
        status = r.rebuild(LASTCP, minReplicasPerFile) ? 0 : -EIO;
        // gLayoutEmulator.InitRecoveryStartTime();
    } else {
        status = metatree.new_tree();
    }
    if (status != 0) {
        return status;
    }
    KFS_LOG_STREAM_INFO << "replaying logs from: " << logdir <<
    KFS_LOG_EOM;
    status = replayer.playAllLogs();
    if (status != 0) {
        return status;
    }
    KFS_LOG_STREAM_INFO << "updating meta tree" <<
    KFS_LOG_EOM;
    metatree.setUpdatePathSpaceUsage(true);
    metatree.enableFidToPathname();
    KFS_LOG_STREAM_INFO << "reading network defn: " << networkFn <<
    KFS_LOG_EOM;
    status = gLayoutEmulator.ReadNetworkDefn(networkFn);
    if (status != 0) {
        return status;
    }
    KFS_LOG_STREAM_INFO << "loading chunkmap: " << chunkmapFn <<
    KFS_LOG_EOM;
    status = gLayoutEmulator.LoadChunkmap(
        chunkmapFn, addChunksToReplicationChecker);
    KFS_LOG_STREAM_INFO << "fs layout emulator setup complete." << KFS_LOG_EOM;
    return status;
}

}
