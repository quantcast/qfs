//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2016/02/17
// Author: Mehmet Can Kurt
//
// Copyright 2016 Quantcast Corporation. All rights reserved.
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
//----------------------------------------------------------------------------

#include "common/kfsdecls.h"
#include "common/IntToString.h"
#include "libclient/MonitorCommon.h"
#include "qcdio/QCUtils.h"

#include <fstream>
#include <iostream>
#include <string>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>

using KFS::AppendDecIntToString;
using KFS::ChunkServerErrorMap;
using KFS::ClientCounters;
using KFS::Counter;
using KFS::ErrorCounters;
using KFS::ServerLocation;

using std::cout;
using std::cerr;
using std::endl;
using std::ofstream;
using std::string;

const char* const DEFAULT_MONITOR_LOG_DIRECTORY = "/tmp/qfs-monitor/logs";

static string getLogPath()
{
    string monitorLogDir;
    char* monitorLogDirEnv = getenv("QFS_CLIENT_MONITOR_LOG_DIR");
    if (monitorLogDirEnv) {
        monitorLogDir = monitorLogDirEnv;
    }
    else {
        monitorLogDir = DEFAULT_MONITOR_LOG_DIRECTORY;
    }
    return monitorLogDir;
}

static int prepareLogPath(const string& monitorLogDir)
{
    const size_t sz = monitorLogDir.size();
    if (sz <= 0) {
        return 0;
    }
    string path;
    path.reserve(sz);
    int mode =
        S_IRUSR | S_IWUSR | S_IXUSR |
        S_IRGRP           | S_IXGRP |
        S_IROTH           | S_IXOTH;
    for (size_t pos = 0; pos < sz; ) {
        const size_t ppos = pos;
        while (++pos < sz && '/' == monitorLogDir[pos])
            {}
        if (pos < sz && string::npos != (pos = monitorLogDir.find('/', pos))) {
            path.append(monitorLogDir, ppos, pos - ppos);
        } else {
            mode |= S_IWGRP | S_IWOTH;
            pos  = sz;
            path = monitorLogDir;
        }
        if (mkdir(path.c_str(), mode) && EEXIST != errno) {
            const int err = errno;
            cerr << "Monitor plugin can't create the log directory: " <<
                path << ": " << QCUtils::SysError(err) << endl;
            return -1;
        }
        chmod(path.c_str(), mode);
    }
    return 0;
}

extern "C" int init()
{
    const string monitorLogDir = getLogPath();
    if (0 == access(monitorLogDir.c_str(), F_OK | W_OK)) {
        return 0;
    }
    // try to create the log path, if access failed because
    // a parent directory does not exist.
    if(errno == ENOENT) {
        return prepareLogPath(monitorLogDir);
    }
    const int err = errno;
    cerr << "Monitor plugin can't access the log directory: " <<
        monitorLogDir << ": " << QCUtils::SysError(err) << endl;
    return -1;
}

inline void EmitCounter(
        ofstream& out,
        const string& prefix,
        const string& ctrName,
        const Counter& ctrValue)
{
    out << prefix << ctrName << "=" << ctrValue << "\n";
}

void WriteToStream(
        ofstream& out,
        const string& prefix,
        const ErrorCounters& counters)
{
    EmitCounter(out, prefix,
            "error_parameters_count", counters.mErrorParametersCount);
    EmitCounter(out, prefix,
            "error_io_count", counters.mErrorIOCount);
    EmitCounter(out, prefix,
            "error_try_again_count", counters.mErrorTryAgainCount);
    EmitCounter(out, prefix,
            "error_no_entry_count", counters.mErrorNoEntryCount);
    EmitCounter(out, prefix,
            "error_busy_count", counters.mErrorBusyCount);
    EmitCounter(out, prefix,
            "error_checksum_count", counters.mErrorChecksumCount);
    EmitCounter(out, prefix,
            "error_lease_expired_count", counters.mErrorLeaseExpiredCount);
    EmitCounter(out, prefix,
            "error_fault_count", counters.mErrorFaultCount);
    EmitCounter(out, prefix,
            "error_inval_chunk_size_count", counters.mErrorInvalChunkSizeCount);
    EmitCounter(out, prefix,
            "error_permissions_count", counters.mErrorPermissionsCount);
    EmitCounter(out, prefix,
            "error_max_retry_reached_count", counters.mErrorMaxRetryReachedCount);
    EmitCounter(out, prefix,
            "error_requeue_required_count", counters.mErrorRequeueRequiredCount);
    EmitCounter(out, prefix,
            "error_other_count", counters.mErrorOtherCount);
    EmitCounter(out, prefix,
            "error_total_count", counters.mTotalErrorCount);
}

extern "C" void reportStatus(
        string metaserverHost,
        int metaserverPort,
        ClientCounters& clientCounters,
        ChunkServerErrorMap& errorCounters)
{
    int pid = getpid();
    string logFilePath = getLogPath();
    logFilePath += "/";
    logFilePath += metaserverHost;
    logFilePath += "_";
    AppendDecIntToString(logFilePath, metaserverPort);
    logFilePath += "_";
    AppendDecIntToString(logFilePath, pid);
    logFilePath += ".log";
    string tmpLogFilePath = logFilePath + ".tmp";

    ofstream fileStream(tmpLogFilePath.c_str(), std::ios::out);
    if (!fileStream) {
        const int err = errno;
        cerr << "Monitor plugin can't open the log file " +
                tmpLogFilePath + " for writing: " <<
                QCUtils::SysError(err) << endl;
        return;
    }

    for (ClientCounters::const_iterator it = clientCounters.begin();
	 it != clientCounters.end() && fileStream; ++it) {
        string counterName = it->first;
        Counter counterVal = it->second;
        fileStream << counterName << "=" << counterVal << "\n";
    }

    for (ChunkServerErrorMap::const_iterator it = errorCounters.begin();
	 it != errorCounters.end() && fileStream; ++it) {
        const ServerLocation& chunkserverLoc = it->first;
        const ErrorCounters& readErrors = it->second.readErrors;
        const ErrorCounters& writeErrors = it->second.writeErrors;
        string chunkserverName = chunkserverLoc.hostname;
        chunkserverName += ":";
        AppendDecIntToString(chunkserverName, chunkserverLoc.port);
        WriteToStream(fileStream, chunkserverName + "_read_", readErrors);
        WriteToStream(fileStream, chunkserverName + "_write_", writeErrors);
    }

    fileStream.close();
    int lastError = errno;

    if (!fileStream) {
        string errMsg = "Monitor plugin can't write the log file to "
                + tmpLogFilePath + ": ";
        if(lastError == 0) {
            lastError = EIO;
        }
        cout << QCUtils::SysError(lastError, errMsg.c_str()) << endl;
        remove(tmpLogFilePath.c_str());
        return;
    }

    chmod(tmpLogFilePath.c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
    rename(tmpLogFilePath.c_str(), logFilePath.c_str());
}

