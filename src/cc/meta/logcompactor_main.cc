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
// \brief Convert prior versions of checkpoints and log by loading checkpoint,
// replaying all log segments, then writing new checkpoint and log segment.
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
    bool    help               = false;
    int16_t numReplicasPerFile = -1;
    int     optchar;
    string  logdir;
    string  cpdir;
    string  lockFn;
    string  newLogDir;
    string  newCpDir;
    int     status = 0;

    while ((optchar = getopt(argc, argv, "hpl:c:r:L:T:C:")) != -1) {
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
    if (newLogDir.empty() || newCpDir.empty()) {
        status = 1;
    }
    if (help || 0 != status) {
        (status ? cerr : cout) << "Usage: " << argv[0] <<
            "[-L <lockfile>]\n"
            "[-l <log directory> (default kfslog)]\n"
            "[-c <checkpoint directory> (default kfscp)]\n"
            "[-r <# of replicas> -- recursively change replication]\n"
            "-T <new log directroy> -- requires -C\n"
            "-C <new checkpoint directroy> -- requires -T\n"
            "-T and -C are intended for log and checkpoint conversion from prior"
            " versions. With these options log compactor reads all log segments,"
            " including the last partial segment, then writes checkpoint, and"
            " initial log segment. Both new log and checkpoint directories must"
            " not exist or must be empty.\n"
            "The log compactor mode where it produced checkpoint by"
            " replaying all log segments except last partial segment is"
            " no longer supported, with new log ahead format. This mode is no"
            " longer required as meta server now writes checkpoints.\n"
        ;
        return (0 == status ? 0 : 1);
    }
    MdStream::Init();
    MsgLogger::Init(0, MsgLogger::kLogLevelINFO);
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
    checkpointer_setup_paths(cpdir);
    const bool kAllowEmptyCheckpointFlag = false;
    if (0 == status && (status = restore_checkpoint(
            lockFn, kAllowEmptyCheckpointFlag)) == 0) {
        const bool kPlayAllLogsFlag = true;
        if ((status = replayer.playLogs(kPlayAllLogsFlag)) == 0) {
            metatree.recomputeDirSize();
            if (0 < numReplicasPerFile) {
                metatree.changePathReplication(ROOTFID, numReplicasPerFile,
                    kKfsSTierUndef, kKfsSTierUndef);
            }
            if (! replayer.logSegmentHasLogSeq() &&
                    replayer.getLastLogSeq().mEpochSeq <= 0) {
                // Roll seeds only with prior log format that has no chunk
                // servers inventory.
                const int64_t kMinRollChunkIdSeed = int64_t(256) << 10;
                chunkID.setseed(chunkID.getseed() +
                        max(kMinRollChunkIdSeed, replayer.getRollSeeds()));
            }
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
            string logFileName;
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
                } else {
                    KFS_LOG_STREAM_INFO <<
                        "file system id: " << metatree.GetFsId() <<
                    KFS_LOG_EOM;
                }
            }
        }
    }
    MsgLogger::Stop();
    MdStream::Cleanup();
    return (status == 0 ? 0 : 1);
}

}

int main(int argc, char **argv)
{
    return KFS::LogCompactorMain(argc, argv);
}
