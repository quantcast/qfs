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
#include "Checkpoint.h"
#include "Restorer.h"
#include "Replay.h"
#include "MetaRequest.h"
#include "LogWriter.h"
#include "util.h"

#include "common/MsgLogger.h"
#include "common/MdStream.h"
#include "qcdio/QCUtils.h"
#include "kfsio/CryptoKeys.h"

#include <sys/stat.h>
#include <errno.h>
#include <unistd.h>

#include <iostream>
#include <cassert>

namespace KFS
{
using std::cout;
using std::cerr;

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
    string  newLogDir;
    string  newCpDir;
    bool    allowEmptyCheckpointFlag = false;
    int     status = 0;

    while ((optchar = getopt(argc, argv, "hpl:c:r:L:e:T:C:")) != -1) {
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
            case 'C':
                newCpDir = optarg;
                if (newCpDir.empty()) {
                    status = 1;
                }
                break;
            case 'T':
                newLogDir = optarg;
                if (newLogDir.empty()) {
                    status = 1;
                }
                break;
            default:
                status = 1;
                break;
        }
    }
    if (newLogDir.empty() != newCpDir.empty()) {
        status = 1;
    }
    if (help || 0 != status) {
        (status ? cerr : cout) << "Usage: " << argv[0] <<
            "[-L <lockfile>]\n"
            "[-l <log directory>]\n"
            "[-c <checkpoint directory>]\n"
            "[-r <# of replicas> set replication to this value for all files]\n"
            "[-e {0|1} allow empty checkpoint]\n"
            "[-T <new log directroy> -- requires -C]\n"
            "[-C <new checkpoint directroy> -- requires -T]\n"
            "-T and -C are intended for log and checkpoint conversion from prior"
            " versions. With these options log compactor reads all log segments,"
            " including the last partial segment, then writes checkpoint and"
            " an empty log segment. Both new log and checkpoint directories must"
            " be empty.\n"
        ;
        return status;
    }
    const bool convertFlag = ! newLogDir.empty();
    MdStream::Init();
    MsgLogger::Init(0, MsgLogger::kLogLevelINFO);
    if (convertFlag) {
        struct stat       st[2] = { {0}, {0} };
        const char* const nm[2] = { newLogDir.c_str(), newCpDir.c_str() };
        for (int i = 0; i < 2; i++) {
            if (stat(nm[i], st + i)) {
                status = -errno;
                if (-ENOENT == status) {
                    status = mkdir(nm[i], 0777) ? -errno : 0;
                    if (0 == status && stat(nm[i], st + i)) {
                        status = -errno;
                    }
                }
            } else if (! S_ISDIR(st[i].st_mode)) {
                status = -ENOTDIR;
            } else if (2 < st[i].st_nlink) {
                status = -ENOTEMPTY;
            }
            if (0 != status) {
                KFS_LOG_STREAM_FATAL <<
                    nm[i] << ": " << QCUtils::SysError(-status) <<
                KFS_LOG_EOM;
                break;
            }
        }
        if (0 == status && st[0].st_ino == st[1].st_ino) {
            KFS_LOG_STREAM_FATAL <<
                newCpDir << " and " << newLogDir <<
                    " must be two dirrent empty directories" <<
            KFS_LOG_EOM;
            status = -EINVAL;
        }
    }
    checkpointer_setup_paths(cpdir);
    if (0 == status && (status = restore_checkpoint(
            lockFn, ! convertFlag && allowEmptyCheckpointFlag)) == 0) {
        const seq_t cplognum = replayer.getLogNum();
        if ((status = replayer.playLogs(convertFlag)) == 0) {
            metatree.recomputeDirSize();
            if (numReplicasPerFile > 0) {
                metatree.changePathReplication(ROOTFID, numReplicasPerFile,
                    kKfsSTierUndef, kKfsSTierUndef);
            }
            if (convertFlag || numReplicasPerFile > 0 ||
                    cplognum != replayer.getLogNum()) {
                string logFileName;
                if (convertFlag) {
                    if (metatree.GetFsId() <= 0) {
                        seq_t fsid = 0;
                        if (! CryptoKeys::PseudoRand(&fsid, sizeof(fsid))) {
                            KFS_LOG_STREAM_FATAL <<
                                "failed to initialize pseudo random number"
                                " generator" <<
                            KFS_LOG_EOM;
                            status = -EFAULT;
                        } else {
                            if (fsid == 0) {
                                fsid = 1;
                            } else if (fsid < 0) {
                                fsid = -fsid;
                            }
                            metatree.SetFsInfo(fsid, metatree.GetCreateTime());
                            KFS_LOG_STREAM_INFO <<
                                "assigned file system id: " << fsid <<
                            KFS_LOG_EOM;
                        }
                    }
                    if (0 == status) {
                        checkpointer_setup_paths(newCpDir);
                        status = MetaRequest::GetLogWriter().WriteNewLogSegment(
                            newLogDir.c_str(), replayer, logFileName);
                        if (0 != status) {
                            KFS_LOG_STREAM_FATAL <<
                                "transaction log write failure: " <<
                                QCUtils::SysError(-status) <<
                            KFS_LOG_EOM;
                        }
                    }
                } else {
                    logFileName = replayer.getLastLogName();
                }
                if (0 == status) {
                    status = cp.write(
                        logFileName,
                        replayer.getCommitted(),
                        replayer.getErrChksum()
                    );
                    if (0 != status) {
                        KFS_LOG_STREAM_FATAL <<
                            "checkpoint write failure: " <<
                            QCUtils::SysError(-status) <<
                        KFS_LOG_EOM;
                    }
                }
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
