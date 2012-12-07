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
#include "util.h"
#include "common/MsgLogger.h"
#include "kfsio/TcpSocket.h"
#include "kfsio/requestio.h"
#include "common/MdStream.h"
#include "common/kfserrno.h"
#include "qcdio/QCUtils.h"

#include <sys/time.h>

#include <iostream>
#include <iterator>
#include <sstream>
#include <cassert>
#include <boost/scoped_array.hpp>

namespace KFS
{

using std::cout;
using std::cerr;
using std::ostringstream;
using std::istringstream;

const string KFS_VERSION_STR = "KFS/1.0";

static bool
getFsckInfo(string metahost, int metaport,
    bool reportAbandonedFilesFlag, int timeoutSec)
{
    TcpSocket sock;
    ServerLocation loc(metahost, metaport);
    int ret, len;
    Properties prop;
    boost::scoped_array<char> buf;

    ret = sock.Connect(loc);

    if (ret < 0) {
        cout << "Unable to connect to metaserver...exiting\n";
        return false;
    }

    ostringstream os;
    os <<
        "FSCK\r\n"
        "Version: " << KFS_VERSION_STR << "\r\n"
        "Cseq: "    << 1               << "\r\n"
        "Report-Abandoned-Files: " <<
            (reportAbandonedFilesFlag ? 1 : 0) << "\r\n"
    "\r\n";
    const string str = os.str();
    ret = sock.DoSynchSend(str.c_str(), str.length());
    if (ret <= 0) {
        cout << "Unable to send fsck rpc to metaserver\n";
        return false;
    }

    // get the response and get the data
    buf.reset(new char[MAX_RPC_HEADER_LEN]);
    int nread = RecvResponseHeader(buf.get(), MAX_RPC_HEADER_LEN, &sock,
        timeoutSec > 0 ? timeoutSec : 1000, &len);

    if (nread <= 0) {
        cout << "Unable to get fsck rpc reply from metaserver...exiting\n";
        return false;
    }
    istringstream ist(buf.get());
    const char kSeparator = ':';

    prop.loadProperties(ist, kSeparator, false);
    int status = prop.getValue("Status", 0);
    if (status < 0) {
        status = -KfsToSysErrno(-status);
        const string msg = prop.getValue("Status-message", string());
        cout << "fsck failure: " << QCUtils::SysError(status) <<
            (msg.empty() ? "" : " ") << msg <<
        "\n";
        return false;
    }

    const int contentLength = prop.getValue("Content-length", 0);
    if (contentLength <= 0) {
        cout << "invalid meta server fsck reply\n";
        return false;
    }
    // Get the body
    buf.reset(new char[contentLength + 1]);
    struct timeval timeout = {timeoutSec > 0 ? timeoutSec : 1000, 0};
    nread = sock.DoSynchRecv(buf.get(), contentLength, timeout);
    if (nread < contentLength) {
        cout << "Unable to get fsck rpc reply from metaserver, status: " <<
            nread << "\n";
        return false;
    }
    cout.write(buf.get(), contentLength);
    const char* const okHdrs[] = {
        "Total lost files: 0\n",
        "Lost files total: 0\n",
        0
    };
    for (const char* const* hdr = okHdrs; *hdr; ++hdr) {
        const int len = (int)strlen(*hdr);
        if (contentLength >= len &&
                memcmp(buf.get(), *hdr, len) == 0) {
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
    char   optchar;
    bool   help = false;
    string logdir;
    string cpdir;
    string metahost;
    string lockFn;
    int    metaport = -1;
    int    status = 0;
    bool   reportAbandonedFilesFlag = true;
    bool   allowEmptyCheckpointFlag = false;
    int    timeoutSec = 60 * 25;

    while ((optchar = getopt(argc, argv, "hl:c:m:p:L:a:t:s:e:")) != -1) {
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
            case 'h':
                help = true;
                break;
            default:
                status = 1;
                break;
        }
    }

    if (help || status != 0) {
        (status ? cerr : cout) <<
            "Usage: " << argv[0] << "\n"
            "[{-s|-m} <metahost>]\n"
            "[-p <metaport>]\n"
            "[-L <lockfile>]\n"
            "[-l <logdir>]\n"
            "[-c <cpdir>]\n"
            "[-a {0|1} report abandoned files (default 1)]\n"
            "[-t <timeout seconds> default 25 min]\n"
            "[-e {0|1} allow empty checkpoint]\n"
        ;
        return status;
    }

    MdStream::Init();
    KFS::MsgLogger::Init(0, MsgLogger::kLogLevelINFO);

    const bool ok = metahost.empty() || metaport < 0 ||
        getFsckInfo(metahost, metaport, reportAbandonedFilesFlag, timeoutSec);
    if ((logdir.empty() && cpdir.empty()) || ! ok) {
        return (ok ? 0 : 1);
    }

    metatree.disableFidToPathname();
    logger_setup_paths(logdir);
    checkpointer_setup_paths(cpdir);
    if ((status = restoreCheckpoint(lockFn, allowEmptyCheckpointFlag)) == 0) {
        status = replayer.playLogs();
    }
    MdStream::Cleanup();

    return ((status == 0 && ok) ? 0 : 1);
}

}

int
main(int argc, char **argv)
{
    return KFS::FsckMain(argc, argv);
}
