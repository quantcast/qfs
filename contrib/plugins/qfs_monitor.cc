#include "common/kfsdecls.h"
#include "common/IntToString.h"
#include "libclient/MonitorCommon.h"

#include <cstdio>
#include <fstream>
#include <iostream>
#include <string>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

using KFS::AppendDecIntToString;
using KFS::ChunkServerErrorMap;
using KFS::ClientCounters;
using KFS::Counter;
using KFS::ErrorCounters;
using KFS::ServerLocation;

using std::cout;
using std::endl;
using std::ofstream;
using std::perror;
using std::remove;
using std::rename;
using std::string;

#define DEFAULT_MONITOR_LOG_DIRECTORY "/mnt/prime/shared/ram_monitor/qfsmonitor/"

string getLogPath()
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

int prepareLogPath(string monitorLogDir)
{
    char* cstr = strdup(monitorLogDir.c_str());
    char* ptr = strtok(cstr, "/");
    string path;
    while(ptr != 0) {
        path += "/";
        path += ptr;
        int ret = mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
        if (ret == -1 && errno != EEXIST) {
            delete[] cstr;
            perror("Monitor plugin can't create the log directory: ");
            return -1;
        }
        ptr = strtok(0, "/");
    }
    delete[] cstr;
    return 0;
}

extern "C" int init()
{
    string monitorLogDir = getLogPath();
    int ret = access(monitorLogDir.c_str(), F_OK | W_OK);
    if (ret != -1) {
        return 0;
    }

    // try to create the log path, if access failed because
    // a parent directory does not exist.
    if(errno == ENOENT) {
        return prepareLogPath(monitorLogDir);
    }

    perror("Monitor plugin can't access the log directory: ");
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
    logFilePath += metaserverHost;
    logFilePath += "_";
    AppendDecIntToString(logFilePath, metaserverPort);
    logFilePath += "_";
    AppendDecIntToString(logFilePath, pid);
    logFilePath += ".log";
    string tmpLogFilePath = logFilePath + ".tmp";

    ofstream fileStream(tmpLogFilePath.c_str(), std::ios::out);
    if (fileStream.fail()) {
        string errMsg = "Monitor plugin can't open the log file " +
                tmpLogFilePath + " for writing: ";
        perror(errMsg.c_str());
        return;
    }

    for (ClientCounters::const_iterator it = clientCounters.begin();
            it != clientCounters.end(); ++it) {
        string counterName = it->first;
        Counter counterVal = it->second;
        fileStream << counterName << "=" << counterVal << endl;
    }

    for (ChunkServerErrorMap::const_iterator it = errorCounters.begin();
            it != errorCounters.end(); ++it) {
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
    rename(tmpLogFilePath.c_str(), logFilePath.c_str());
}

