//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/06/18
//
// Author: Sriram Rao
//         Mike Ovsiannikov
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
// \brief An online fsck: report files with missing blocks and chunk placement
// problems, and allow to verify checkpoint and transaction logs by loading them
// the same way the meta server does this.
//
//----------------------------------------------------------------------------

#include "kfstree.h"
#include "Logger.h"
#include "Checkpoint.h"
#include "Restorer.h"
#include "Replay.h"

#include "tools/MonClient.h"
#include "common/MsgLogger.h"
#include "common/MdStream.h"
#include "qcdio/QCUtils.h"
#include "libclient/KfsOps.h"
#include "libclient/KfsClient.h"

#include <iostream>

namespace KFS
{
using namespace KFS_MON;
using namespace KFS::client;

using std::cout;
using std::cerr;

static bool
getFsckInfo(MonClient& client, const ServerLocation& loc,
    bool reportAbandonedFilesFlag, int timeoutSec)
{
    client.SetMaxContentLength(512 << 20);
    client.SetOpTimeout(timeoutSec);

    FsckOp op(0, reportAbandonedFilesFlag);
    const int ret = client.Execute(loc, op);
    if (ret != 0) {
        KFS_LOG_STREAM_ERROR << op.statusMsg <<
            " error: " << ErrorCodeToStr(ret) <<
        KFS_LOG_EOM;
        return false;
    }
    if (op.contentLength <= 0) {
        KFS_LOG_STREAM_ERROR <<
            "invalid response content length: " << op.contentLength <<
        KFS_LOG_EOM;
        return false;
    }
    if (! op.contentBuf || op.contentBufLen < op.contentLength) {
        panic("invalid op content buffer");
        return false;
    }

    cout.write(op.contentBuf, op.contentLength);
    const char* const okHdrs[] = {
        "Total lost files: 0\n",
        "Lost files total: 0\n",
        0
    };
    for (const char* const* hdr = okHdrs; *hdr; ++hdr) {
        const size_t len = strlen(*hdr);
        if (op.contentLength >= len &&
                memcmp(op.contentBuf, *hdr, len) == 0) {
            cout << "Filesystem is HEALTHY\n";
            return true;
        }
    }
    return false;
}

static int
restoreCheckpoint(const string& lockfn, bool allowEmptyCheckpointFlag)
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
FsckMain(int argc, char** argv)
{
    // use options: -l for logdir -c for checkpoint dir
    int         optchar;
    string      logdir;
    string      cpdir;
    string      metahost;
    string      lockFn;
    bool        ok                       = true;
    const char* configFileName           = 0;
    int         metaport                 = -1;
    bool        help                     = false;
    bool        reportAbandonedFilesFlag = true;
    bool        allowEmptyCheckpointFlag = false;
    int         timeoutSec               = 30 * 60;

    while ((optchar = getopt(argc, argv, "hl:c:m:p:L:a:t:s:e:f:")) != -1) {
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
            case 's':
            case 'm':
                metahost = optarg;
                break;
            case 'p':
                metaport = atoi(optarg);
                break;
            case 'a':
                reportAbandonedFilesFlag = atoi(optarg) != 0;
                break;
            case 't':
                timeoutSec = atoi(optarg);
                break;
            case 'e':
                allowEmptyCheckpointFlag = atoi(optarg) != 0;
                break;
            case 'f':
                configFileName = optarg;
                break;
            case 'h':
                help = true;
                break;
            default:
                ok = false;
                break;
        }
    }

    if (help || ! ok) {
        (ok ? cout : cerr) <<
            "Usage: " << argv[0] << "\n"
            "[{-s|-m} <metahost>]\n"
            "[-p <metaport>]\n"
            "[-L <lockfile>]\n"
            "[-l <logdir>]\n"
            "[-c <cpdir>]\n"
            "[-a {0|1} report abandoned files (default 1)]\n"
            "[-t <timeout seconds> default 25 min]\n"
            "[-e {0|1} allow empty checkpoint]\n"
            "[-f <config file>]\n"
        ;
        return (ok ? 0 : 1);
    }

    MdStream::Init();
    KFS::MsgLogger::Init(0, MsgLogger::kLogLevelINFO);

    ok = metahost.empty() || metaport < 0;
    if (! ok) {
        MonClient            client;
        const ServerLocation loc(metahost, metaport);
        ok =
            client.SetParameters(loc, configFileName) >= 0 &&
            getFsckInfo(client, loc, reportAbandonedFilesFlag, timeoutSec);
    }
    if (ok && (! logdir.empty() || ! cpdir.empty())) {
        metatree.disableFidToPathname();
        logger_setup_paths(logdir);
        checkpointer_setup_paths(cpdir);
        ok =
            restoreCheckpoint(lockFn, allowEmptyCheckpointFlag) == 0 &&
            replayer.playLogs() == 0
        ;
    }
    MdStream::Cleanup();

    return (ok ? 0 : 1);
}

}

int
main(int argc, char **argv)
{
    return KFS::FsckMain(argc, argv);
}
