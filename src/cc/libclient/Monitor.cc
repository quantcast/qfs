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

#include "Monitor.h"

#include "KfsClient.h"
#include "KfsClientInt.h"
#include "KfsOps.h"
#include "MonitorCommon.h"

#include "common/DynamicArray.h"
#include "common/kfsdecls.h"
#include "common/kfstypes.h"
#include "common/MsgLogger.h"
#include "common/Properties.h"
#include "common/time.h"

#include "qcdio/qcdebug.h"
#include "qcdio/qcstutils.h"
#include "qcdio/QCMutex.h"
#include "qcdio/QCThread.h"

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <dlfcn.h>
#include <unistd.h>

namespace KFS
{
using std::find;
using std::make_pair;
using std::map;
using std::pair;
using std::string;
using std::swap;
using std::vector;

class Monitor::Impl: public QCRunnable {

public:
    Impl() :
        QCRunnable(),
        mThread(),
        mThreadControlMutex(),
        mMonitorSetupMutex(),
        mDoubleBuffMutex(),
        mClientRemovalMutex(),
        mCondVar(),
        mRunFlag(false),
        mPluginLoaded(false),
        mPluginPath(),
        mReportIntervalSecs(15),
        mMonitorMaxErrorRecords(-1),
        mNumClientsMonitored(0),
        mPluginInitFuncHandle(0),
        mPluginReportFuncHandle(0),
        mPluginHandle(0),
        mNumDroppedErrors(0)
    {

    }
    virtual ~Impl()
    {
        Shutdown();
        ClosePlugin();
    }
    bool AddClient(
            KfsClientImpl* client,
            const char* pluginPath,
            int reportInterval,
            int maxErrorsToRecord)
    {
        QCStMutexLocker theMonitorSetupLock(mMonitorSetupMutex);
        // Lazy plugin loading logic: the first created client instance
        // tries to load the plugin. If the loading operation fails for some
        // reason, subsequent client instances will retry.
        if (!mPluginLoaded) {
            LoadPlugin(pluginPath, reportInterval, maxErrorsToRecord);
            if (!mPluginLoaded) {
                return false;
            }
        }
        // Start the monitor thread if this is the first client instance
        // created ever by this process or the first client instance after
        // the monitor thread's been shutdown previously.
        if (++mNumClientsMonitored == 1) {
            Start();
        }
        theMonitorSetupLock.Unlock();

        ServerLocation metaserverLoc = client->GetMetaserverLocation();
        QCStMutexLocker theDoubleBuffLock(mDoubleBuffMutex);
        ClientId clientId = client->GetClientId();
        mNewClients.PushBack(new Client(clientId, metaserverLoc, client));
        return true;
    }
    void RemoveClient(
            KfsClientImpl* client)
    {
        QCStMutexLocker theClientRemovalLock(mClientRemovalMutex);
        // We want to make sure that this client instance contributes to the
        // collected metrics one last time before it is destroyed. To do this,
        // we call GetStats() on the client instance one last time here. This
        // way, when the monitor thread aggregates client counters for the
        // corresponding filesystem, it will also use the last stats collected
        // from the deleted client instance. The cost we pay for that is a single
        // GetStats call each time a client instance is destroyed.
        // Note that Properties object will be disposed by the monitor thread in
        // ReportStatusToPlugin function once it is used.
        Properties* stats = client->GetStats();
        ClientId clientId = client->GetClientId();
        mRemovedClients.insert(make_pair(clientId, stats));
        theClientRemovalLock.Unlock();

        QCStMutexLocker theMonitorSetupLock(mMonitorSetupMutex);
        if (--mNumClientsMonitored == 0) {
            // Shutdown monitor thread if there are no
            // monitored client instances left.
            Shutdown();
        }
    }
    void RecordError(
            int errSource,
            const ServerLocation& metaserverLocation,
            const ServerLocation& chunkserverLocation,
            int errCode)
    {
        if (!mPluginLoaded) {
            return;
        }

        // Make a record of this incident in the next pool of error records
        // which are to be processed by the monitor thread next time it wakes up.
        // To limit the memory usage, we drop the incoming incident records
        // after some point, if a limit is specified.
        QCStMutexLocker theDoubleBuffLock(mDoubleBuffMutex);
        if(mMonitorMaxErrorRecords < 0 || mErrorRecords.GetSize() <
                (unsigned int) mMonitorMaxErrorRecords) {
            mErrorRecords.PushBack(new ErrorRecord(errSource,
                    metaserverLocation, chunkserverLocation, errCode));
        }
        else {
            ++mNumDroppedErrors;
            // Without holding a lock, attempt to notify the monitor thread here
            // . If the monitor thread is waiting, this should prevent any more
            // dropped error records. The notification will be a no-op if the
            // monitor thread is currently running.
            mCondVar.Notify();
        }
    }
private:
    void LoadPlugin(
            const char* pluginPath,
            int reportInterval,
            int maxErrorsToRecord)
    {
        mPluginPath = pluginPath;
        mReportIntervalSecs = reportInterval;
        mMonitorMaxErrorRecords = maxErrorsToRecord;

        mPluginHandle = dlopen(mPluginPath.c_str(), RTLD_LAZY);
        if (!mPluginHandle) {
            KFS_LOG_STREAM_ERROR
                 << "Monitor: Could not open monitor plugin: " << mPluginPath
                 << " error: " << dlerror() << KFS_LOG_EOM;
            return;
        }

        dlerror();
        mPluginInitFuncHandle = (init_t) dlsym(mPluginHandle, "init");
        char* err_msg = dlerror();
        if (err_msg) {
            KFS_LOG_STREAM_ERROR
                << "Monitor: Cannot load the symbol \"init\" from plugin: "
                << err_msg << KFS_LOG_EOM;
            ClosePlugin();
            return;
        }

        dlerror();
        mPluginReportFuncHandle = (reportStatus_t) dlsym(mPluginHandle,
                "reportStatus");
        err_msg = dlerror();
        if (err_msg) {
            KFS_LOG_STREAM_ERROR
                << "Monitor: Cannot load the symbol \"reportStatus\" from plugin: "
                << err_msg << KFS_LOG_EOM;
            ClosePlugin();
            return;
        }

        int ret = mPluginInitFuncHandle();
        if (ret == -1) {
            KFS_LOG_STREAM_ERROR <<
                    "Monitor: Failed initializing monitor plugin!" << KFS_LOG_EOM;
            ClosePlugin();
            return;
        }

        // successfully loaded and initialized
        mPluginLoaded = true;
        KFS_LOG_STREAM_INFO
                << "Monitor: Monitor plugin loaded and initialized: "
                << mPluginPath << " Report interval: "
                << mReportIntervalSecs << " Max Errors To Record: "
                << mMonitorMaxErrorRecords << KFS_LOG_EOM;
    }
    void ReportStatusToPlugin()
    {
        QCStMutexLocker theDoubleBuffLock(mDoubleBuffMutex);
        // Dequeue recently added clients in new clients list.
        mNewClientsTmp.Swap(mNewClients);
        // Dequeue messages in error record queue.
        mErrorRecordsTmp.Swap(mErrorRecords);
        if(mNumDroppedErrors > 0) {
            KFS_LOG_STREAM_INFO
                    << "Monitor: " << mNumDroppedErrors
                    << " errors have been dropped " << KFS_LOG_EOM;
        }
        mNumDroppedErrors = 0;
        theDoubleBuffLock.Unlock();

        // Setup metrics structures for recently added clients.
        int numNewClients = mNewClientsTmp.GetSize();
        if (numNewClients > 0) {
            KFS_LOG_STREAM_INFO
                << "Monitor: Number of clients added for monitoring "
                << "since last report: " << numNewClients << KFS_LOG_EOM;
        }
        for(int i = 0; i < numNewClients; ++i) {
            Client* newClient = mNewClientsTmp[i];
            ServerLocation metaserverLoc = newClient->metaserverLoc;
            FilesystemMetrics& fsMetrics = mFsMetricsMap[metaserverLoc];
            fsMetrics.clients.push_back(newClient);
        }
        mNewClientsTmp.Clear();

        // Process all error records and increment the corresponding error
        // counters.
        int numErrorRecords = mErrorRecordsTmp.GetSize();
        for(int i = 0; i < numErrorRecords; ++i) {
            ErrorRecord& record = *(mErrorRecordsTmp[i]);
            if (!mFsMetricsMap.count(record.metaserverLoc)) {
                // The client instance that generated this error
                // has not been not registered. ignore the error.
                // this is not a good sign; we shouldn't let such clients
                // to report errors.
                KFS_LOG_STREAM_ERROR << "Monitor: unknown metaserver"
                        << KFS_LOG_EOM;
                delete &record;
                continue;
            }
            FilesystemMetrics& fsMetrics = mFsMetricsMap[record.metaserverLoc];
            ChunkserverErrorCounters& chunkserverErrorCounters =
                    fsMetrics.errorCountersMap[record.chunkserverLoc];
            if (record.errorSource == Monitor::kReadOpError) {
                IncrementCounter(chunkserverErrorCounters.readErrors,
                        record.errorCode);
            }
            else if(record.errorSource == Monitor::kWriteOpError) {
                IncrementCounter(chunkserverErrorCounters.writeErrors,
                        record.errorCode);
            }
            delete &record;
        }
        mErrorRecordsTmp.Clear();

        int numRemovedClients = mRemovedClients.size();
        if (numRemovedClients > 0) {
            KFS_LOG_STREAM_INFO
                << "Monitor: Number of clients removed from monitoring "
                << "since last report: " << numRemovedClients << KFS_LOG_EOM;
        }

        for(FilesystemMetricsMap::iterator fsMetricsMapIt =
                mFsMetricsMap.begin(); fsMetricsMapIt != mFsMetricsMap.end();
                ++fsMetricsMapIt) {
            const ServerLocation& fsLoc = fsMetricsMapIt->first;
            FilesystemMetrics& fsMetrics = fsMetricsMapIt->second;
            // Report metrics to plugin if there are alive client instances
            // for the corresponding filesystem.
            int numClients = fsMetrics.clients.size();
            if (numClients > 0) {
                ClientCounters  newSumOfClientCounters;
                // Use the client lock to make sure no client instance is
                // being removed, while we call GetStats through them.
                QCStMutexLocker theClientRemovalLock(mClientRemovalMutex);
                int clientInd = 0;
                while (clientInd < numClients) {
                    Client* client = fsMetrics.clients[clientInd];
                    ClientId clientId = client->clientId;
                    KfsClientImpl* clientPtr = client->clientPtr;
                    if (mRemovedClients.count(clientId)) {
                        // This client is marked as removed. Accumulate the last
                        // collected metrics from this client to history.
                        Properties* removedClientCounters =
                                mRemovedClients[clientId];
                        AggregateCounters(*removedClientCounters,
                                newSumOfClientCounters);
                        KfsClient::DisposeProperties(removedClientCounters);
                        swap(fsMetrics.clients[clientInd],
                                fsMetrics.clients[--numClients]);
                        delete client;
                        mRemovedClients.erase(clientId);
                    }
                    else {
                        Properties* clientCounters = clientPtr->GetStats();
                        AggregateCounters(*clientCounters,
                                newSumOfClientCounters);
                        KfsClient::DisposeProperties(clientCounters);
                        ++clientInd;
                    }
                }
                theClientRemovalLock.Unlock();
                fsMetrics.clients.erase(fsMetrics.clients.begin()+numClients,
                        fsMetrics.clients.end());

                if (!fsMetrics.currSumOfClientCounters.empty() &&
                        newSumOfClientCounters.size() !=
                                fsMetrics.currSumOfClientCounters.size()) {
                     // For each counter in newSumOfClientCounters,
                     // there must be a matching counter in
                     // currSumOfClientCounters.
                     KFS_LOG_STREAM_ERROR
                         << "Monitor: Number of counters "
                         << "in new and current sum of client counters"
                                 "don't match!"
                         << KFS_LOG_EOM;
                }

                if (!fsMetrics.currSumOfClientCounters.empty()) {
                    // Calculate the difference between new and current sum of
                    // client counters.
                    ClientCounters diffOfCounters;
                    for(ClientCounters::iterator countersIt =
                            newSumOfClientCounters.begin();
                            countersIt != newSumOfClientCounters.end();
                            ++countersIt) {
                        string counterName  = countersIt->first;
                        Counter  newCounterVal = countersIt->second;
                        Counter  currCounterVal =
                                fsMetrics.currSumOfClientCounters[counterName];
                        Counter  reportVal;
                        if(counterName.compare("Network.Sockets") == 0) {
                            // Network.Sockets counter is not accumulative, so
                            // report the new value.
                            reportVal = newCounterVal;
                        }
                        else {
                            reportVal = newCounterVal - currCounterVal;
                        }
                        diffOfCounters.insert(make_pair(counterName, reportVal));
                    }
                    mPluginReportFuncHandle(fsLoc.hostname, fsLoc.port,
                            diffOfCounters, fsMetrics.errorCountersMap);
                }
                else {
                    // Since this is the first time that a report is sent to
                    // plugin, there are no counters in current sum. Use values
                    // in new sum directly.
                    mPluginReportFuncHandle(fsLoc.hostname, fsLoc.port,
                            newSumOfClientCounters, fsMetrics.errorCountersMap);
                }
                // Save new sum of client counters as current for next report.
                swap(fsMetrics.currSumOfClientCounters, newSumOfClientCounters);
                // Reset error counters for next report.
                for (ChunkServerErrorMap::iterator it =
                        fsMetrics.errorCountersMap.begin();
                        it != fsMetrics.errorCountersMap.end(); ++it) {
                    it->second.Clear();
                }
            }
        }
    }
    void AggregateCounters(
            Properties& countersAsProp,
            ClientCounters& aggregatedResults)
    {
        for(Properties::iterator propertiesIt = countersAsProp.begin();
                propertiesIt != countersAsProp.end(); ++propertiesIt) {
            string counterName = propertiesIt->first.GetStr();
            // Before we aggregate, we need to convert each value first to
            // a string and then to a long integer representation.
            Counter counterVal =
                    strtol(propertiesIt->second.GetStr().c_str(), 0, 10);
            ClientCounters::iterator currCounterIt =
                    aggregatedResults.find(counterName);
            if(currCounterIt == aggregatedResults.end()) {
                aggregatedResults.insert(make_pair(counterName, counterVal));
            }
            else {
                currCounterIt->second += counterVal;
            }
        }
    }
    void ClosePlugin()
    {
        if (!mPluginLoaded) {
            return;
        }

        dlclose(mPluginHandle);
        mPluginLoaded = false;
        mPluginHandle = 0;
        mPluginInitFuncHandle = 0;
        mPluginReportFuncHandle = 0;
        KFS_LOG_STREAM_INFO << "Monitor: Monitor plugin closed" << KFS_LOG_EOM;
     }
    void Start()
    {
        if (mRunFlag) {
            return;
        }
        const int kStackSize = 64 << 10;
        mRunFlag = true;
        mThread.Start(this, kStackSize, "MonitorThread");
        KFS_LOG_STREAM_INFO << "Monitor: worker thread started" << KFS_LOG_EOM;
    }
    virtual void Run()
    {
        QCStMutexLocker theLock(mThreadControlMutex);
        const int64_t kSleepTimeUsec =
                int64_t(mReportIntervalSecs) * 1000 * 1000;
        int64_t theNextTime = microseconds() + kSleepTimeUsec;
        while (mRunFlag) {
            const int64_t theStartTime = microseconds();
            if (theStartTime < theNextTime) {
                mCondVar.Wait(mThreadControlMutex,
                        QCMutex::Time(theNextTime - theStartTime) * 1000);
                continue;
            }
            theNextTime = microseconds() + kSleepTimeUsec;

            // get metrics and report to plugin
            ReportStatusToPlugin();
        }

        // to make sure metrics are sent to plugin at least once
        ReportStatusToPlugin();
    }
    void Shutdown()
    {
        QCStMutexLocker theLock(mThreadControlMutex);
        if (! mRunFlag) {
            return;
        }
        mRunFlag = false;
        mCondVar.Notify();
        theLock.Unlock();
        mThread.Join();
        KFS_LOG_STREAM_INFO <<
                "Monitor: worker thread stopped" << KFS_LOG_EOM;
    }
    void IncrementCounter(ErrorCounters& errCounters, int errCode) {
        switch(errCode) {
            case kErrorParameters:
                ++errCounters.mErrorParametersCount;
                break;
            case kErrorIO:
                ++errCounters.mErrorIOCount;
                break;
            case kErrorTryAgain:
                ++errCounters.mErrorTryAgainCount;
                break;
            case kErrorNoEntry:
                ++errCounters.mErrorNoEntryCount;
                break;
            case kErrorBusy:
                ++errCounters.mErrorBusyCount;
                break;
            case kErrorChecksum:
                ++errCounters.mErrorChecksumCount;
                break;
            case kErrorLeaseExpired:
                ++errCounters.mErrorLeaseExpiredCount;
                break;
            case kErrorFault:
                ++errCounters.mErrorFaultCount;
                break;
            case kErrorInvalChunkSize:
                ++errCounters.mErrorInvalChunkSizeCount;
                break;
            case kErrorPermissions:
                ++errCounters.mErrorPermissionsCount;
                break;
            case kErrorMaxRetryReached:
                ++errCounters.mErrorMaxRetryReachedCount;
                break;
            case kErrorRequeueRequired:
                ++errCounters.mErrorRequeueRequiredCount;
                break;
            default:
                ++errCounters.mErrorOtherCount;
                break;
        }
        ++errCounters.mTotalErrorCount;
    }
    enum
    {
        kErrorParameters      = -EINVAL,
        kErrorIO              = -EIO,
        kErrorTryAgain        = -EAGAIN,
        kErrorNoEntry         = -ENOENT,
        kErrorBusy            = -EBUSY,
        kErrorChecksum        = -EBADCKSUM,
        kErrorLeaseExpired    = -ELEASEEXPIRED,
        kErrorFault           = -EFAULT,
        kErrorInvalChunkSize  = -EINVALCHUNKSIZE,
        kErrorPermissions     = -EPERM,
        kErrorMaxRetryReached = -(10000 + ETIMEDOUT),
        kErrorRequeueRequired = -(10000 + ETIMEDOUT + 1)
    };
    QCThread mThread;
    QCMutex mThreadControlMutex;
    QCMutex mMonitorSetupMutex;
    QCMutex mDoubleBuffMutex;
    QCMutex mClientRemovalMutex;
    QCCondVar mCondVar;
    bool mRunFlag;
    bool mPluginLoaded;
    string mPluginPath;
    int mReportIntervalSecs;
    // maximum number of errors that we make a record of.
    // default value -1 means that there is no limit.
    int mMonitorMaxErrorRecords;
    // structure to contain information of a single error incident
    // that involves a chunkserver.
    struct ErrorRecord {
        // read/write op error
        int errorSource;
        ServerLocation metaserverLoc;
        ServerLocation chunkserverLoc;
        int errorCode;
        ErrorRecord(int _errorSource,const ServerLocation& _metaserverLoc,
                const ServerLocation& _chunkserverLoc, int _errorCode) {
            errorSource = _errorSource;
            metaserverLoc = _metaserverLoc;
            chunkserverLoc = _chunkserverLoc;
            errorCode = _errorCode;
        }
    };
    typedef unsigned int ClientId;
    struct Client {
        ClientId clientId;
        ServerLocation metaserverLoc;
        KfsClientImpl* clientPtr;
        Client(ClientId _clientId, ServerLocation& _metaserverLoc,
                KfsClientImpl* _client) {
            clientId = _clientId;
            metaserverLoc = _metaserverLoc;
            clientPtr = _client;
        }
    };
    // structure to maintain metrics collected for each filesystem
    typedef vector<Client*> ClientList;
    struct FilesystemMetrics {
        ClientList clients;
        ClientCounters currSumOfClientCounters;
        ChunkServerErrorMap errorCountersMap;
    };
    // Maintain metrics for every filesystem accessed by client instances
    // created within this process.
    typedef map<ServerLocation, FilesystemMetrics> FilesystemMetricsMap;
    FilesystemMetricsMap mFsMetricsMap;
    // Number of monitored client instances. This tells us when the monitor
    // thread should be started/stopped and possibly restarted.
    int mNumClientsMonitored;
    // monitor plugin interface and handles
    typedef int (*init_t)();
    typedef void (*reportStatus_t)(string, int, ClientCounters&,
            ChunkServerErrorMap&);
    init_t mPluginInitFuncHandle;
    reportStatus_t mPluginReportFuncHandle;
    void* mPluginHandle;
    // Use message queue and double buffering for error records.
    DynamicArray<ErrorRecord*> mErrorRecordsTmp;
    DynamicArray<ErrorRecord*> mErrorRecords;
    // Count the number of error records dropped to limit memory usage.
    int mNumDroppedErrors;
    // Use message queue and double buffering for adding new clients to monitor.
    DynamicArray<Client*> mNewClientsTmp;
    DynamicArray<Client*> mNewClients;
    // Flag client instances that have been removed and
    // store the last collected stats from them.
    typedef map<ClientId, Properties*> RemovedClients;
    RemovedClients mRemovedClients;
};

Monitor::Monitor()
{
    mImpl = new Impl();
}

Monitor::~Monitor()
{
    delete mImpl;
}

bool Monitor::AddClientSelf(KfsClientImpl* client, const char* pluginPath,
        int reportInterval, int maxErrorsToRecord)
{
    return mImpl->AddClient(client, pluginPath,
            reportInterval, maxErrorsToRecord);
}

void Monitor::RemoveClientSelf(KfsClientImpl* client)
{
    mImpl->RemoveClient(client);
}

void Monitor::ReportErrorSelf(
        int errSource,
        const ServerLocation& metaserverLocation,
        const ServerLocation& chunkserverLocation,
        int errCode)
{
    mImpl->RecordError(errSource, metaserverLocation,
            chunkserverLocation, errCode);
}

}
