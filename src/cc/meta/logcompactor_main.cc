//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/06/18
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
// \brief The metaserver writes out operational log records to a log
// file.  Every N minutes, the log file is rolled over (and a new one
// is used to write out data).  For fast recovery, it'd be desirable
// to compact the log files and produce a checkpoint file.  This tool
// provides such a capability: it takes a checkpoint file, applies the
// set of operations as defined in a sequence of one or more log files
// and produces a new checkpoint file.  When the metaserver rolls over the log
// files, it creates a symlink to point the "LAST" closed log file; when log
// compaction is done, we only compact upto the last closed log file.
//
//----------------------------------------------------------------------------

#include "kfstree.h"
#include "Logger.h"
#include "Checkpoint.h"
#include "Restorer.h"
#include "Replay.h"
#include "util.h"
#include "common/MsgLogger.h"
#include "common/MdStream.h"

#include <iostream>
#include <cassert>

namespace KFS
{
using std::cout;
using std::cerr;

static int
RestoreCheckpoint(const string& lockfn, bool allowEmptyCheckpointFlag)
{
    if (! lockfn.empty()) {
        acquire_lockfile(lockfn, 10);
    }
    if (! allowEmptyCheckpointFlag || file_exists(LASTCP)) {
        Restorer r;
        return (r.rebuild(LASTCP) ? 0 : -EIO);
    } else {
        return metatree.new_tree();
    }
}

static int
LogCompactorMain(int argc, char** argv)
{
    // use options: -l for logdir -c for checkpoint dir
    int     optchar;
    bool    help = false;
    int16_t numReplicasPerFile = -1;
    string  logdir;
    string  cpdir;
    string  lockFn;
    bool    allowEmptyCheckpointFlag = false;
    int     status = 0;

    while ((optchar = getopt(argc, argv, "hpl:c:r:L:e:")) != -1) {
        switch (optchar) {
            case 'L':
                lockFn = optarg;
                break;
            case 'l':
                logdir = optarg;
                break;
            case 'c':
                cpdir = optarg;
                break;
            case 'h':
                help = true;
                break;
            case 'p':
                // DEPRECATED
                break;
            case 'r':
                numReplicasPerFile = (int16_t)atoi(optarg);
                break;
            case 'e':
                allowEmptyCheckpointFlag = atoi(optarg) != 0;
                break;
            default:
                status = 1;
                break;
        }
    }

    if (help || status != 0) {
        (status ? cerr : cout) << "Usage: " << argv[0] <<
            "[-L <lockfile>]\n"
            "[-l <logdir>]\n"
            "[-c <cpdir>]\n"
            "[-r <# of replicas> set replication to this value for all files]\n"
            "[-e {0|1} allow empty checkpoint]\n"
        ;
        return status;
    }

    MdStream::Init();
    MsgLogger::Init(0, MsgLogger::kLogLevelINFO);

    logger_setup_paths(logdir);
    checkpointer_setup_paths(cpdir);
    if ((status = RestoreCheckpoint(lockFn, allowEmptyCheckpointFlag)) == 0) {
        const seq_t lastcp = oplog.checkpointed();
        if ((status = replayer.playLogs()) == 0) {
            metatree.recomputeDirSize();
            if (numReplicasPerFile > 0) {
                metatree.changePathReplication(ROOTFID, numReplicasPerFile,
                    kKfsSTierUndef, kKfsSTierUndef);
        }
            if (numReplicasPerFile > 0 || lastcp != oplog.checkpointed()) {
                status = cp.do_CP();
            }
        }
    }
    MdStream::Cleanup();
    return (status == 0 ? 0 : 1);
}

}

int main(int argc, char **argv)
{
    return KFS::LogCompactorMain(argc, argv);
}
