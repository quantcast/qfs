//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/06/18
//
// Author: Sriram Rao
//
// Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
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
#include "Checkpoint.h"
#include "Restorer.h"
#include "Replay.h"
#include "util.h"

#include "common/MdStream.h"
#include "common/MsgLogger.h"

#include "qcdio/QCUtils.h"

#include <sstream>
#include <iostream>
#include <fstream>
#include <errno.h>

namespace KFS
{
using std::cout;
using std::cerr;
using std::set;
using std::istringstream;
using std::ofstream;

static int
FileListerMain(int argc, char **argv)
{
    // use options: -l for logdir -c for checkpoint dir
    int         optchar;
    bool        help   = false;
    const char* logdir = 0;
    string      cpdir;
    string      pathFn;
    string      lockfn;
    bool        includeLastLogFlag = true;
    int         status = 0;
    set<fid_t>  ids;

    while ((optchar = getopt(argc, argv, "hl:c:f:L:i:a:")) != -1) {
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
            case 'a':
                includeLastLogFlag = atoi(optarg) != 0;
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
            "[-a {0|1} replay all log segments (default 1)]\n"
        ;
        return status;
    }

    MdStream::Init();
    MsgLogger::Init(0, MsgLogger::kLogLevelINFO);

    checkpointer_setup_paths(cpdir);
    replayer.setLogDir(logdir);
    const bool kAllowEmptyCheckpointFlag = false;
    if ((status = restore_checkpoint(lockfn, kAllowEmptyCheckpointFlag)) == 0 &&
            (status = replayer.playLogs(includeLastLogFlag)) == 0) {
        if (pathFn.empty() || pathFn == "-") {
            metatree.listPaths(cout, ids);
        } else {
            ofstream ofs(pathFn.c_str());
            if (ofs) {
                metatree.listPaths(ofs, ids);
                ofs.close();
            }
            if (! ofs) {
                status = errno;
                KFS_LOG_STREAM_ERROR <<
                    QCUtils::SysError(status, pathFn.c_str()) <<
                KFS_LOG_EOM;
            }
        }
    }

    MdStream::Cleanup();
    MsgLogger::Stop();
    return (status == 0 ? 0 : 1);
}

} // namespace KFS

int
main(int argc, char **argv)
{
    return KFS::FileListerMain(argc, argv);
}

