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
// \brief Given a checkpoint, write out the list of files in the tree and their
// metadata in a "ls -l" format.
//
//----------------------------------------------------------------------------

#include "kfstree.h"
#include "Logger.h"
#include "Checkpoint.h"
#include "Restorer.h"
#include "Replay.h"
#include "util.h"
#include "common/MdStream.h"
#include "common/MsgLogger.h"

#include <iostream>
#include <cassert>

namespace KFS
{
using std::cout;
using std::cerr;
using std::set;
using std::istringstream;

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
FileListerMain(int argc, char **argv)
{
    // use options: -l for logdir -c for checkpoint dir
    int        optchar;
    bool       help = false;
    string     logdir, cpdir, pathFn;
    // the name of the lock file that is used to synchronize between
    // the various tools that load the checkpoint file.
    string     lockfn;
    bool       allowEmptyCheckpointFlag = false;
    int        status = 0;
    set<fid_t> ids;

    while ((optchar = getopt(argc, argv, "hl:c:f:L:i:e:")) != -1) {
        switch (optchar) {
            case 'L':
                lockfn = optarg;
                break;
            case 'l':
                logdir = optarg;
                break;
            case 'c':
                cpdir = optarg;
                break;
            case 'f':
                pathFn = optarg;
                break;
            case 'h':
                help = true;
                break;
            case 'e':
                allowEmptyCheckpointFlag = atoi(optarg) != 0;
                break;
            case 'i': {
                    istringstream is(optarg);
                    fid_t id;
                    while ((is >> id)) {
                        ids.insert(id);
                    }
                }
                break;
            default:
                status = 1;
                break;
        }
    }

    if (help || status != 0) {
        (status ? cerr : cout) << "Usage: " << argv[0] << "\n"
            "[-L <lockfile>]\n"
            "[-l <logdir>]\n"
            "[-c <cpdir>]\n"
            "[-f <output fn>]\n"
            "[-i fid]\n"
            "[-e {0|1} allow empty checkpoint]\n"
        ;
        return status;
    }

    MdStream::Init();
    MsgLogger::Init(0, MsgLogger::kLogLevelINFO);

    logger_setup_paths(logdir);
    checkpointer_setup_paths(cpdir);
    if ((status = RestoreCheckpoint(lockfn, allowEmptyCheckpointFlag)) == 0 &&
            (status = replayer.playLogs()) == 0) {
        if (pathFn == "-") {
            metatree.listPaths(cout, ids);
            return 0;
        }
        ofstream ofs(pathFn.c_str());
        if (! ofs) {
            return 1;
        }
        metatree.listPaths(ofs, ids);
    }

    MdStream::Cleanup();
    return (status == 0 ? 0 : 1);
}

} // namespace KFS

int
main(int argc, char **argv)
{
    return KFS::FileListerMain(argc, argv);
}

