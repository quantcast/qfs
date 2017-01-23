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
RunFsck(const string& tmpName, bool reportAbandonedFilesFlag)
{
    const int cnt = gLayoutManager.FsckStreamCount(reportAbandonedFilesFlag);
    if (cnt <= 0) {
        KFS_LOG_STREAM_ERROR << "internal error" << KFS_LOG_EOM;
        return -EINVAL;
    }
    const char* const    suffix    = ".XXXXXX";
    const size_t         suffixLen = strlen(suffix);
    StBufferT<char, 128> buf;
    fstream*  const      streams   = new fstream[cnt];
    ostream** const      ostreams  = new ostream*[cnt + 1];
    int                  status    = 0;
    ostreams[cnt] = 0;
    for (int i = 0; i < cnt; i++) {
        char* const ptr = buf.Resize(tmpName.length() + suffixLen + 1);
        memcpy(ptr, tmpName.data(), tmpName.size());
        strcpy(ptr + tmpName.size(), suffix);
        const int tfd = mkstemp(ptr);
        if (tfd < 0) {
            status = errno > 0 ? -errno : -EINVAL;
            KFS_LOG_STREAM_ERROR <<
                "failed to create temporary file: " << ptr <<
                QCUtils::SysError(-status) <<
            KFS_LOG_EOM;
            close(tfd);
            break;
        }
        streams[i].open(ptr, fstream::in | fstream::out);
        close(tfd);
        unlink(ptr);
        if (! streams[i]) {
            status = errno > 0 ? -errno : -EINVAL;
            KFS_LOG_STREAM_ERROR <<
                "failed to open temporary file: " << ptr <<
                QCUtils::SysError(-status) <<
            KFS_LOG_EOM;
            break;
        }
        ostreams[i] = streams + i;
    }
    if (0 == status) {
        status = gLayoutManager.Fsck(
            ostreams, reportAbandonedFilesFlag) ? 0 : -EINVAL;
        char* const  ptr = buf.Resize(128 << 10);
        const size_t len = buf.GetSize();
        for (int i = 0; i < cnt; i++) {
            streams[i].flush();
            streams[i].seekp(0);
            while (cout && streams[i]) {
                streams[i].read(ptr, len);
                cout.write(ptr, streams[i].gcount());
            }
            if (! streams[i].eof()) {
                status = errno > 0 ? -errno : -EINVAL;
                KFS_LOG_STREAM_ERROR <<
                    "io error: " << QCUtils::SysError(-status) <<
                KFS_LOG_EOM;
                while (i < cnt) {
                    streams[i].close();
                }
                break;
            }
            streams[i].close();
        }
    }
    delete [] streams;
    delete [] ostreams;
    return status;
}

static int
FsckMain(int argc, char** argv)
{
    // use options: -l for logdir -c for checkpoint dir
    int                 optchar;
    string              logdir;
    string              cpdir;
    string              metahost;
    string              lockFn;
    bool                ok                       = true;
    const char*         configFileName           = 0;
    int                 metaport                 = -1;
    bool                help                     = false;
    bool                reportAbandonedFilesFlag = true;
    bool                allowEmptyCheckpointFlag = false;
    int                 timeoutSec               = 30 * 60;
    bool                includeLastLogFlag       = false;
    MsgLogger::LogLevel logLevel                 = MsgLogger::kLogLevelINFO;
    string              tmpNamePrefix            = "tmp";
    bool                runFsckFlag              = false;
    bool                setMetaLocationsFlag     = true;

    while ((optchar = getopt(argc, argv,
            "hl:c:m:p:L:a:t:s:e:f:A:vT:Fn")) != -1) {
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
            case 'n':
                setMetaLocationsFlag = false;
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
            "[-A {0|1} include last log segment]\n"
            "[-v verbose tracing]\n"
            "[-T temporary name prefix to produce fsck report]\n"
            "[-F emit fsck report after restore and replay]\n"
            "[-n connect to the specified meta server node]\n"
        ;
        return (ok ? 0 : 1);
    }

    MdStream::Init();
    KFS::MsgLogger::Init(0, logLevel);

    ok = metahost.empty() || metaport < 0;
    if (! ok) {
        MonClient            client;
        const ServerLocation loc(metahost, metaport);
        ok = client.SetParameters(loc, configFileName,
                setMetaLocationsFlag) >= 0 &&
            GetFsckInfo(client, reportAbandonedFilesFlag, timeoutSec);
        if (ok && ! setMetaLocationsFlag) {
            ok = client.SetServer(loc);
        }
    }
    if (ok && (! logdir.empty() || ! cpdir.empty())) {
        metatree.disableFidToPathname();
        checkpointer_setup_paths(cpdir);
        replayer.setLogDir(logdir.c_str());
        ok = restore_checkpoint(lockFn, allowEmptyCheckpointFlag) == 0 &&
            replayer.playLogs(includeLastLogFlag) == 0
        ;
        if (ok && runFsckFlag) {
            ok = 0 == RunFsck(tmpNamePrefix, reportAbandonedFilesFlag);
        }
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
