//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/06/18
//
// Author: Sriram Rao
//         Mike Ovsiannikov
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
// \brief An online fsck: report files with missing blocks and chunk placement
// problems, and allow to verify checkpoint and transaction logs by loading them
// the same way the meta server does this.
//
//----------------------------------------------------------------------------

#include "kfstree.h"
#include "Checkpoint.h"
#include "Restorer.h"
#include "Replay.h"
#include "LayoutManager.h"

#include "tools/MonClient.h"
#include "common/MsgLogger.h"
#include "common/MdStream.h"
#include "qcdio/QCUtils.h"
#include "libclient/KfsOps.h"
#include "libclient/KfsClient.h"

#include <iostream>
#include <fstream>

#include <stdlib.h>
#include <unistd.h>

namespace KFS
{
using namespace KFS_MON;
using namespace KFS::client;

using std::cout;
using std::cerr;
using std::fstream;

static bool
GetFsckInfo(MonClient& client, bool reportAbandonedFilesFlag, int timeoutSec)
{
    client.SetMaxContentLength(512 << 20);
    client.SetOpTimeoutSec(timeoutSec);

    FsckOp op(0, reportAbandonedFilesFlag);
    const int ret = client.Execute(op);
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
FsckMain(int argc, char** argv)
{
    // use options: -l for logdir -c for checkpoint dir
    int                 optchar;
    string              logdir;
    string              cpdir;
    ServerLocation      metaLoc;
    string              lockFn;
    bool                ok                       = true;
    const char*         configFileName           = 0;
    bool                help                     = false;
    bool                reportAbandonedFilesFlag = true;
    bool                allowEmptyCheckpointFlag = false;
    int                 timeoutSec               = 30 * 60;
    bool                includeLastLogFlag       = true;
    MsgLogger::LogLevel logLevel                 = MsgLogger::kLogLevelINFO;
    string              tmpNamePrefix            = "tmp";
    bool                runFsckFlag              = false;

    while ((optchar = getopt(argc, argv,
            "hl:c:m:p:L:a:t:s:e:f:A:vT:F")) != -1) {
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
                metaLoc.hostname = optarg;
                break;
            case 'p':
                metaLoc.port = atoi(optarg);
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
            case 'A':
                includeLastLogFlag = atoi(optarg) != 0;
                break;
            case 'v':
                logLevel = MsgLogger::kLogLevelDEBUG;
                break;
            case 'T':
                tmpNamePrefix = optarg;
                break;
            case 'F':
                runFsckFlag = true;
                break;
            default:
                ok = false;
                break;
        }
    }

    ok = ok && (metaLoc.IsValid() || ! logdir.empty() || ! cpdir.empty());
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
            "[-A {0|1} include last log segment (default 1)]\n"
            "[-v verbose tracing]\n"
            "[-T temporary name prefix to produce fsck report]\n"
            "[-F emit fsck report after restore and replay]\n"
        ;
        return (ok ? 0 : 1);
    }

    MdStream::Init();
    MsgLogger::Init(0, logLevel);
    MonClient client; // This initializes SSL filter.

    if (metaLoc.IsValid()) {
        const bool kSetMetaLocationsFlag = true;
        ok = client.SetParameters(metaLoc, configFileName,
                kSetMetaLocationsFlag) >= 0 &&
            GetFsckInfo(client, reportAbandonedFilesFlag, timeoutSec);
    }
    if (ok && (! logdir.empty() || ! cpdir.empty())) {
        metatree.disableFidToPathname();
        checkpointer_setup_paths(cpdir);
        replayer.setLogDir(logdir.c_str());
        ok = restore_checkpoint(lockFn, allowEmptyCheckpointFlag) == 0 &&
            replayer.playLogs(includeLastLogFlag) == 0
        ;
        if (ok && runFsckFlag) {
            ok = 0 == gLayoutManager.RunFsck(
                tmpNamePrefix, reportAbandonedFilesFlag, cout);
        }
    }
    MsgLogger::Stop();
    MdStream::Cleanup();
    const int ret = ok ? 0 : 1;
    // Do not do graceful exit in order to save time, if b+tree / file
    // system is sufficiently large.
    if (5 < metatree.height() ||
            (int64_t(1) << 20) < (GetNumFiles() + GetNumDirs())) {
        _exit(ret);
    }
    return ret;
}

}

int
main(int argc, char **argv)
{
    return KFS::FsckMain(argc, argv);
}
