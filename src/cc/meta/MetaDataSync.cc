//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2016/1/11
// Author: Mike Ovsiannikov
//
// Copyright 2016 Quantcast Corp.
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
// Transaction log and checkpoint retrieval.
//
//
//----------------------------------------------------------------------------

#include "MetaDataSync.h"
#include "MetaRequest.h"
#include "LogReceiver.h"
#include "MetaDataStore.h"
#include "util.h"

#include "qcdio/qcdebug.h"
#include "qcdio/QCThread.h"
#include "qcdio/QCMutex.h"
#include "qcdio/qcstutils.h"

#include "common/MsgLogger.h"
#include "common/SingleLinkedQueue.h"
#include "common/IntToString.h"
#include "common/RequestParser.h"

#include "kfsio/NetManager.h"
#include "kfsio/ClientAuthContext.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/checksum.h"
#include "kfsio/ITimeout.h"
#include "kfsio/KfsCallbackObj.h"
#include "kfsio/checksum.h"
#include "kfsio/CryptoKeys.h"

#include "libclient/KfsNetClient.h"
#include "libclient/KfsOps.h"

#include <algorithm>
#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <iomanip>

#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>

namespace KFS
{
using std::max;
using std::istringstream;
using std::ifstream;
using std::skipws;
using std::find;

using client::KfsNetClient;
using client::KfsOp;

const char* const kMetaDataSyncCommitPtr = "metadatasynccommit";

class MetaDataSync::Impl :
    public KfsNetClient::OpOwner,
    public ITimeout,
    public KfsCallbackObj,
    public QCRunnable
{
private:
    class ReadOp : public client::MetaReadMetaData
    {
    public:
        ReadOp()
            : client::MetaReadMetaData(-1),
              mPos(-1),
              mRetryCount(0),
              mInFlightFlag(false),
              mBuffer(),
              mNextPtr(0)
            {}
        void Reset()
        {
            statusMsg.clear();
            mBuffer.Clear();
            fileName.clear();
            DeallocContentBuf();
            seq            = -1;
            contentLength  = 0;
            status         = 0;
            fileSystemId   = -1;
            startLogSeq    = MetaVrLogSeq();
            endLogSeq      = MetaVrLogSeq();
            readPos        = -1;
            checkpointFlag = false;
            readSize       = -1;
            checksum       = 0;
            fileSize       = -1;
            maxReadSize    = -1;
            mPos           = -1;
            mRetryCount    = 0;
            mInFlightFlag  = false;
        }
        int64_t  mPos;
        int      mRetryCount;
        bool     mInFlightFlag;
        IOBuffer mBuffer;
        class GetNext
        {
        public:
            static ReadOp*& Next(
                ReadOp& inOp)
                { return inOp.mNextPtr; }
        };
        friend class GetNext;
    private:
        ReadOp* mNextPtr;
    private:
        ReadOp(
            const ReadOp& inReadOp);
        ReadOp& operator=(
            const ReadOp& inReadOp);
    };
    typedef SingleLinkedQueue<ReadOp, ReadOp::GetNext> ReadQueue;
    static int64_t RandomSeq()
    {
        int64_t theReq = 0;
        CryptoKeys::PseudoRand(&theReq, sizeof(theReq));
        return ((theReq < 0 ? -theReq : theReq) >> 1);
    }
public:
    typedef vector<ServerLocation> Servers;

    Impl(
        NetManager& inNetManager)
        : OpOwner(),
          ITimeout(),
          KfsCallbackObj(),
          QCRunnable(),
          mRuntimeNetManager(inNetManager),
          mStartupNetManager(),
          mKfsNetClient(
            mStartupNetManager,
            string(),          // inHost
            0,                 // inPort
            5,                 // inMaxRetryCount
            4,                 // inTimeSecBetweenRetries
            10,                // inOpTimeoutSec
            4 * 60,            // inIdleTimeoutSec
            RandomSeq()        // inInitialSeqNum
          ),
          mSetServerFlag(false),
          mFetchOnRestartFileName("metadatafetch"),
          mServers(),
          mPendingSyncServers(),
          mPendingSyncLogSeq(),
          mPendingSyncLogEndSeq(),
          mPendingAllowNotPrimaryFlag(false),
          mSyncScheduledCount(0),
          mMutex(),
          mFileName(),
          mCheckpointDir(),
          mLogDir(),
          mTmpSuffix(".tmp"),
          mCheckpointFileName(),
          mLastLogFileName(),
          mAuthContext(),
          mReadOpsPtr(0),
          mFreeList(),
          mPendingList(),
          mFileSystemId(-1),
          mReadOpsCount(16),
          mMaxLogBlockSize((sizeof(void*) < 8 ? 8 : 64) << 20),
          mMaxReadSize(2 << 20),
          mMaxRetryCount(10),
          mDebugSubmitLogBlockDropRate(0),
          mDebugSubmitLastLogWriteDropRate(0),
          mRetryCount(0),
          mRetryTimeout(3),
          mCurMaxReadSize(-1),
          mFd(-1),
          mServerIdx(0),
          mPos(0),
          mNextReadPos(0),
          mFileSize(-1),
          mLogSeq(),
          mLastSubmittedLogSeq(),
          mNextLogSegIdx(-1),
          mCheckpointFlag(false),
          mAllowNotPrimaryFlag(false),
          mBuffer(),
          mFreeWriteOpList(),
          mFreeWriteOpCount(0),
          mNextBlockChecksum(kKfsNullChecksum),
          mCurBlockChecksum(kKfsNullChecksum),
          mNextBlockSeq(0),
          mLogWritesInFlightCount(0),
          mWriteToFileFlag(false),
          mWriteSyncFlag(false),
          mMinWriteSize(4 << 20),
          mMaxReadOpRetryCount(8),
          mWakeupTime(0),
          mSleepingFlag(false),
          mShutdownNetManagerFlag(false),
          mCheckStartLogSeqFlag(false),
          mCheckLogSeqOnlyFlag(false),
          mReadPipelineFlag(false),
          mLogSyncStartedFlag(false),
          mKeepLogSegmentsInterval(10),
          mNoLogSeqCount(0),
          mStatus(0),
          mDownloadStatus(0),
          mDownloadDoneFlag(true),
          mDownloadGeneration(0),
          mCurDownloadGeneration(0),
          mWriteOpGeneration(0),
          mThread(),
          mClusterKey(),
          mSyncCommitName(),
          mStrBuffer(),
          mMetaMds()
    {
        SET_HANDLER(this, &Impl::LogWriteDone);
        mStartupNetManager.SetResolverParameters(
            mRuntimeNetManager.GetResolverOsFlag(),
            mRuntimeNetManager.GetResolverCacheSize(),
            mRuntimeNetManager.GetResolverCacheExpiration());
        mKfsNetClient.SetAuthContext(&mAuthContext);
        mKfsNetClient.SetMaxContentLength(3 * mMaxReadSize / 2);
        mKfsNetClient.SetMaxRetryCount(5);
        mKfsNetClient.SetMaxMetaLogWriteRetryCount(10);
        mKfsNetClient.SetOpTimeoutSec(10);
        mKfsNetClient.SetTimeSecBetweenRetries(4);
        mKfsNetClient.SetFailAllOpsOnOpTimeoutFlag(true);
        mStrBuffer.reserve(4 << 10);
    }
    virtual ~Impl()
    {
        Impl::Shutdown();
        QCRTASSERT(0 == mLogWritesInFlightCount);
    }
    int SetParameters(
        const char*       inParamPrefixPtr,
        const Properties& inParameters,
        int               inMaxReadSize)
    {
        if (mThread.IsStarted()) {
            KFS_LOG_STREAM_ERROR <<
                "parameters update ignored: keep logs thread is running" <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        Properties::String theName(inParamPrefixPtr ? inParamPrefixPtr : "");
        const size_t       thePrefLen = theName.GetSize();
        const int          kMinReadSize = 1 << 10;
        mMaxReadSize = max(kMinReadSize, inParameters.getValue(
            theName.Truncate(thePrefLen).Append("maxReadSize"),
            mMaxReadSize));
        mMaxLogBlockSize = max(4 << 10, inParameters.getValue(
            theName.Truncate(thePrefLen).Append("maxLogBlockSize"),
            mMaxLogBlockSize));
        if (! mReadOpsPtr) {
            mReadOpsCount = max(size_t(1), inParameters.getValue(
                theName.Truncate(thePrefLen).Append("maxReadOpsCount"),
                mReadOpsCount));
        }
        const int theReadOpsCount = (int)mReadOpsCount;
        if (kMinReadSize * theReadOpsCount <= inMaxReadSize &&
                inMaxReadSize < mMaxReadSize * theReadOpsCount) {
            mMaxReadSize = (inMaxReadSize + theReadOpsCount - 1) /
                theReadOpsCount;
        }
        mKfsNetClient.SetMaxContentLength(3 * mMaxReadSize / 2);
        mFetchOnRestartFileName = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("fetchOnRestartFileName"),
            mFetchOnRestartFileName);
        mTmpSuffix = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("tmpSuffix"),
            mTmpSuffix);
        mMaxRetryCount = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("maxRetryCount"),
            mMaxRetryCount);
        mDebugSubmitLogBlockDropRate = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("debugSubmitLogBlockDropRate"),
            mDebugSubmitLogBlockDropRate);
        mDebugSubmitLastLogWriteDropRate = inParameters.getValue(
            theName.Truncate(thePrefLen).Append(
            "debugSubmitLastLogWriteDropRate"),
            mDebugSubmitLastLogWriteDropRate);
        mRetryTimeout = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("retryTimeout"),
            mRetryTimeout);
        mWriteSyncFlag = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("writeSync"),
            mWriteSyncFlag ? 1 : 0) != 0;
        mMinWriteSize = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("minWriteSize"),
            mMinWriteSize);
        mMaxReadOpRetryCount = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("maxReadOpRetryCount"),
            mMaxReadOpRetryCount);
        mKeepLogSegmentsInterval = max(2, inParameters.getValue(
            theName.Truncate(thePrefLen).Append("keepLogSegmentsInterval"),
            mKeepLogSegmentsInterval));
        mFileSystemId = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("fileSystemId"),
            mFileSystemId);
        mKfsNetClient.SetOpTimeoutSec(max(1, inParameters.getValue(
                theName.Truncate(thePrefLen).Append("readOpTimeoutSec"),
                mKfsNetClient.GetOpTimeoutSec())));
        mKfsNetClient.SetMaxRetryCount(max(0, inParameters.getValue(
                theName.Truncate(thePrefLen).Append("maxOpRetryCount"),
                mKfsNetClient.GetMaxRetryCount())));
        mKfsNetClient.SetMaxMetaLogWriteRetryCount(max(0, inParameters.getValue(
                theName.Truncate(thePrefLen).Append("maxOpLogWriteRetryCount"),
                mKfsNetClient.GetMaxMetaLogWriteRetryCount())));
        mKfsNetClient.SetTimeSecBetweenRetries(inParameters.getValue(
                theName.Truncate(thePrefLen).Append("timeBetweenRetries"),
                mKfsNetClient.GetTimeSecBetweenRetries()));
        mStartupNetManager.SetResolverParameters(
            mRuntimeNetManager.GetResolverOsFlag(),
            mRuntimeNetManager.GetResolverCacheSize(),
            mRuntimeNetManager.GetResolverCacheExpiration());
        const Properties::String* const theMdsPtr =
            inParameters.getValue(kMetaserverMetaMdsParamNamePtr);
        if (theMdsPtr) {
            mMetaMds.clear();
            istringstream theStream(theMdsPtr->GetStr());
            string theMd;
            while ((theStream >> theMd)) {
                mMetaMds.insert(theMd);
            }
        }
        mClusterKey = inParameters.getValue(
            kMetaClusterKeyParamNamePtr, mClusterKey);
        const Properties::String* const theServersPtr =
            mLogSyncStartedFlag ? 0 :
            inParameters.getValue(
                theName.Truncate(thePrefLen).Append("servers"));
        bool theOkFlag = true;
        if (theServersPtr) {
            const char*       thePtr      = theServersPtr->GetPtr();
            const char* const theEndPtr   = thePtr + theServersPtr->GetSize();
            const bool        kHexFmtFlag = false;
            ServerLocation    theLoc;
            Servers           theServers;
            while (thePtr < theEndPtr) {
                if (! theLoc.ParseString(
                        thePtr, theEndPtr - thePtr, kHexFmtFlag)) {
                    KFS_LOG_STREAM_ERROR <<
                        "invalid parameter: " << theName <<
                        ": " << *theServersPtr <<
                    KFS_LOG_EOM;
                    theOkFlag = false;
                    break;
                }
                theServers.push_back(theLoc);
            }
            if (theOkFlag) {
                if (mServerIdx < mServers.size()) {
                    const ServerLocation& theServer = mServers[mServerIdx];
                    mServerIdx = 0;
                    while (mServerIdx < theServers.size()) {
                        if (theServer == theServers[mServerIdx]) {
                            break;
                        }
                        ++mServerIdx;
                    }
                    if (theServers.size() <= mServerIdx) {
                        mServerIdx = 0;
                    }
                } else {
                    mServerIdx = 0;
                }
                mServers.swap(theServers);
            }
        }
        const bool               kVerifyFlag = true;
        ClientAuthContext* const kNullCtxPtr = 0;
        string* const            kNullStrPtr = 0;
        const int theRet = mAuthContext.SetParameters(
            theName.Truncate(thePrefLen).Append("auth.").GetPtr(),
            inParameters,
            kNullCtxPtr,
            kNullStrPtr,
            kVerifyFlag
        );
        return (theOkFlag ? theRet : -EINVAL);
    }
    int Start(
        const char* inCheckpointDirPtr,
        const char* inLogDirPtr,
        bool        inVrNodeIdConfiguredFlag)
    {
        if (mReadOpsPtr || ! inCheckpointDirPtr || ! *inCheckpointDirPtr ||
                ! inLogDirPtr || ! *inLogDirPtr) {
            KFS_LOG_STREAM_ERROR <<
                "meta data sync: invalid parameters" <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        mNextBlockChecksum = ComputeBlockChecksum(kKfsNullChecksum, "\n", 1);
        mReadOpsPtr = new ReadOp[mReadOpsCount];
        for (size_t i = 0; i < mReadOpsCount; i++) {
            mFreeList.PutFront(mReadOpsPtr[i]);
        }
        mCheckpointDir = inCheckpointDirPtr;
        mLogDir        = inLogDirPtr;
        mSyncCommitName = mCheckpointDir;
        if (mSyncCommitName.empty()) {
            return -EINVAL;
        }
        if ('/' != *mSyncCommitName.rbegin()) {
            mSyncCommitName += "/";
        }
        mSyncCommitName += kMetaDataSyncCommitPtr;
        mCheckpointFileName.clear();
        mLastLogFileName.clear();
        ifstream theStream(mSyncCommitName.c_str());
        if (theStream) {
            int      theStep  = 0;
            uint32_t theCrc32 = 0;
            mStrBuffer.clear();
            if (getline(theStream, mCheckpointFileName) &&
                    ! mCheckpointFileName.empty() &&
                    getline(theStream, mLastLogFileName) &&
                    getline(theStream, mStrBuffer) &&
                    (theStream >> skipws) &&
                    (theStream >> theCrc32) &&
                    ComputeCommitStateChecksum(
                        mCheckpointFileName,
                        mLastLogFileName,
                        mStrBuffer) == theCrc32 &&
                    ParseDecInt(mStrBuffer, theStep) &&
                    0 <= theStep) {
                theStream.close();
                string theName = mCheckpointDir;
                if ('/' != *theName.rbegin()) {
                    theName += "/";
                }
                theName += mCheckpointFileName;
                mCheckpointFileName = theName;
                if (! mLastLogFileName.empty()) {
                    theName = mLogDir;
                    if ('/' != *theName.rbegin()) {
                        theName += "/";
                    }
                    theName += mLastLogFileName;
                    mLastLogFileName = theName;
                }
                const int theRet = CommitSync(theStep);
                if (0 != theRet) {
                    return theRet;
                }
            } else {
                mCheckpointFileName.clear();
                mLastLogFileName.clear();
                theStream.close();
                KFS_LOG_STREAM_ERROR <<
                    mSyncCommitName << ": invalid format" <<
                KFS_LOG_EOM;
                if (0 != unlink(mSyncCommitName.c_str())) {
                    const int theErr = GetErrno();
                    KFS_LOG_STREAM_ERROR <<
                        mSyncCommitName << ": " << QCUtils::SysError(theErr) <<
                    KFS_LOG_EOM;
                    return (theErr < 0 ? theErr : -theErr);
                }
            }
        } else {
            const int theFd = open(mSyncCommitName.c_str(), O_RDONLY);
            if (theFd < 0) {
                const int theErr = GetErrno();
                if (ENOENT != theErr) {
                    KFS_LOG_STREAM_ERROR <<
                        mSyncCommitName << ": " << QCUtils::SysError(theErr) <<
                    KFS_LOG_EOM;
                    return (theErr < 0 ? theErr : -theErr);
                }
            } else {
                close(theFd);
            }
        }
        bool          theEmptyFsFlag = false;
        int64_t const theFsId        = GetFsId(mFileSystemId, theEmptyFsFlag);
        if (theFsId < 0) {
            return (int)theFsId;
        }
        if (mFileSystemId < 0) {
            if (theEmptyFsFlag) {
                KFS_LOG_STREAM_ERROR <<
                    "file system ID must be set with no meta data" <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            mFileSystemId = theFsId;
        } else if (theFsId != mFileSystemId) {
            KFS_LOG_STREAM_ERROR <<
                "file system id mismatch:" <<
                " expected: " << mFileSystemId <<
                " actual: "   << theFsId <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        // Do not start fetch with config. if fs is not empty.
        bool theFetchFlag             =
            theEmptyFsFlag && inVrNodeIdConfiguredFlag;
        bool theFetchAfterRestartFlag = false;
        if (! mFetchOnRestartFileName.empty()) {
            if (inVrNodeIdConfiguredFlag) {
                ifstream theStream(mFetchOnRestartFileName.c_str());
                if (theStream) {
                    Servers theCfgServers;
                    theCfgServers.swap(mServers);
                    ServerLocation theLocation;
                    while ((theStream >> theLocation)) {
                        if (theLocation.IsValid() &&
                                mServers.end() == find(
                                    mServers.begin(), mServers.end(),
                                    theLocation)) {
                            mServers.push_back(theLocation);
                        }
                        theLocation = ServerLocation();
                    }
                    theStream.close();
                    for (Servers::const_iterator
                            theIt = theCfgServers.begin();
                            theCfgServers.end() != theIt;
                            ++theIt) {
                        if (mServers.end() == find(
                                mServers.begin(), mServers.end(), *theIt)) {
                            mServers.push_back(*theIt);
                        }
                    }
                    theFetchFlag             = ! mServers.empty();
                    theFetchAfterRestartFlag = theFetchFlag;
                }
            } else {
                const int theRet = DeleteFetchOnRestartFile();
                if (0 != theRet) {
                    return theRet;
                }
            }
        }
        if (! theFetchFlag) {
            mServers.clear();
            return 0;
        }
        if (theEmptyFsFlag || theFetchAfterRestartFlag) {
            mLogSeq = MetaVrLogSeq();
        } else {
            mLogSeq = GetLastLogSeq();
            if (! mLogSeq.IsValid()) {
                KFS_LOG_STREAM_ERROR <<
                    "failed to obtain log segment sequence" <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            mShutdownNetManagerFlag = true;
            LogSeqCheckStart();
            Run();
            mShutdownNetManagerFlag = false;
            if (0 != mStatus && mNoLogSeqCount <= 0) {
                return mStatus;
            }
        }
        if (! mLogSeq.IsValid()) {
            KFS_LOG_STREAM_INFO <<
                "attempting to fetch checkpoint and logs from other node(s)" <<
            KFS_LOG_EOM;
            mKfsNetClient.ClearMetaServerLocations();
            mServerIdx = 0;
            int theRet;
            if (0 != (theRet = PrepareToSync()) ||
                    0 != (theRet = GetCheckpoint())) {
                return theRet;
            }
            mShutdownNetManagerFlag = true;
            Run();
            mShutdownNetManagerFlag = false;
            const ServerLocation theServer = mKfsNetClient.GetServerLocation();
            const Servers::const_iterator theIt =
                find(mServers.begin(), mServers.end(), theServer);
            if (mServers.end() != theIt) {
                mServerIdx = theIt - mServers.begin();
            }
            mKfsNetClient.ClearMetaServerLocations();
            if (0 != mStatus) {
                return mStatus;
            }
            theRet = CommitSync();
            if (0 != theRet) {
                return theRet;
            }
            mLogSeq.mLogSeq = -mLogSeq.mLogSeq;
            KFS_LOG_STREAM_INFO <<
                "done fetching checkpoint and logs from: " << theServer <<
            KFS_LOG_EOM;
        }
        if (! mFetchOnRestartFileName.empty() &&
                unlink(mFetchOnRestartFileName.c_str())) {
            const int theRet = GetErrno();
            if (ENOENT != theRet) {
                KFS_LOG_STREAM_ERROR <<
                    mFetchOnRestartFileName << ": " <<
                        QCUtils::SysError(theRet) <<
                KFS_LOG_EOM;
                return theRet;
            }
        }
        const int theRet = DeleteFetchOnRestartFile();
        if (0 != theRet) {
            return theRet;
        }
        LogSeqCheckStart();
        int kStackSize = 64 << 10;
        mThread.Start(this, kStackSize, "MetaSyncKeepLogs");
        return 0;
    }
    void StartLogSync(
        const MetaVrLogSeq& inLogSeq,
        bool                inAllowNotPrimaryFlag,
        bool                inUseVrPrimarySelectorFlag)
    {
        if (! mReadOpsPtr) {
            return;
        }
        StopKeepData();
        Reset();
        mLogSyncStartedFlag  = true;
        mLogSeq              = inLogSeq;
        mLastSubmittedLogSeq = inLogSeq;
        mWriteToFileFlag     = false;
        mStatus              = 0;
        mAllowNotPrimaryFlag = inAllowNotPrimaryFlag;
        if (&mRuntimeNetManager != &mKfsNetClient.GetNetManager()) {
            mKfsNetClient.SetNetManager(mRuntimeNetManager);
            mRuntimeNetManager.RegisterTimeoutHandler(this);
        }
        if (mServers.empty()) {
            return;
        }
        if (mAllowNotPrimaryFlag || ! inUseVrPrimarySelectorFlag ||
                ! InitMetaVrPrimarySelector()) {
            mKfsNetClient.ClearMetaServerLocations();
            mSetServerFlag = true;
        }
        LogFetchStart();
    }
    void ScheduleLogSync(
        const Servers&      inServers,
        const MetaVrLogSeq& inLogStartSeq,
        const MetaVrLogSeq& inLogEndSeq,
        bool                inAllowNotPrimaryFlag)
    {
        if (! mReadOpsPtr) {
            return;
        }
        QCStMutexLocker theLocker(mMutex);
        const bool theUpdateFlag = mDownloadDoneFlag ||
                inServers != mPendingSyncServers ||
                inLogStartSeq != mPendingSyncLogSeq ||
                inLogEndSeq != mPendingSyncLogEndSeq ||
                mAllowNotPrimaryFlag != mPendingAllowNotPrimaryFlag;
        if (theUpdateFlag) {
            mPendingAllowNotPrimaryFlag = inAllowNotPrimaryFlag;
            mPendingSyncServers         = inServers;
            mPendingSyncLogSeq          = inLogStartSeq;
            mPendingSyncLogEndSeq       = inLogEndSeq;
            SyncAddAndFetch(mSyncScheduledCount, 1);
            mDownloadGeneration++;
            mDownloadStatus   = 0;
            mDownloadDoneFlag = false;
        }
        theLocker.Unlock();
        if (theUpdateFlag) {
            mRuntimeNetManager.Wakeup();
        }
    }
    int GetLogFetchStatus(
        bool& outProgressFlag)
    {
        QCStMutexLocker theLocker(mMutex);
        outProgressFlag = ! mDownloadDoneFlag;
        return mDownloadStatus;
    }
    virtual void Run()
    {
        mKfsNetClient.GetNetManager().RegisterTimeoutHandler(this);
        mKfsNetClient.GetNetManager().MainLoop();
        Reset();
        mKfsNetClient.GetNetManager().UnRegisterTimeoutHandler(this);
        mSleepingFlag = false;
    }
    void Shutdown()
    {
        StopKeepData();
        if (&mRuntimeNetManager == &mKfsNetClient.GetNetManager()) {
            mRuntimeNetManager.UnRegisterTimeoutHandler(this);
        }
        mSleepingFlag = false;
        Reset();
        delete [] mReadOpsPtr;
        mReadOpsPtr = 0;
        FreeWriteOps();
        mAuthContext.Clear();
    }
    virtual void OpDone(
        KfsOp*    inOpPtr,
        bool      inCanceledFlag,
        IOBuffer* inBufferPtr)
    {
        if (inOpPtr < mReadOpsPtr ||
                mReadOpsPtr + mReadOpsCount < inOpPtr ||
                ! inBufferPtr) {
            panic("metad data sync: invalid read RPC completion");
            return;
        }
        ReadOp& theOp = mReadOpsPtr[
            static_cast<const ReadOp*>(inOpPtr) - mReadOpsPtr];
        if (! theOp.mInFlightFlag || &theOp.mBuffer != inBufferPtr) {
            panic("metad data sync: invalid read RPC state");
            return;
        }
        KFS_LOG_STREAM_DEBUG <<
            (inCanceledFlag ? "canceled " : "") <<
            "status: " << theOp.status <<
            " "        << theOp.statusMsg <<
            " pos: "   << theOp.mPos <<
            " / "      << mPos <<
            " "        << theOp.Show() <<
        KFS_LOG_EOM;
        theOp.mInFlightFlag = false;
        if (inCanceledFlag) {
            ClearPendingList();
            return;
        }
        if (0 <= theOp.status) {
            if (theOp.mPos != theOp.readPos) {
                theOp.status    = -EINVAL;
                theOp.statusMsg = "invalid read position";
            } else if (! theOp.startLogSeq.IsValid()) {
                theOp.status    = -EINVAL;
                theOp.statusMsg = "invalid log sequence";
            } else if (theOp.fileSystemId < 0) {
                theOp.status    = -EINVAL;
                theOp.statusMsg = "invalid file system id";
            } else if (theOp.endLogSeq.IsValid() && ! theOp.checkpointFlag &&
                    theOp.endLogSeq <= theOp.startLogSeq) {
                theOp.status    = -EINVAL;
                theOp.statusMsg = "invalid log end sequence";
            }
        }
        if (theOp.status < 0) {
            HandleReadError(theOp);
            return;
        }
        const uint32_t theChecksum = ComputeCrc32(
            &theOp.mBuffer, theOp.mBuffer.BytesConsumable());
        if (theChecksum != theOp.checksum) {
            theOp.status    = -EBADCKSUM;
            theOp.statusMsg = "received data checksum mismatch";
            HandleReadError(theOp);
            return;
        }
        Handle(theOp);
    }
    virtual void Timeout()
    {
        if (mLogSyncStartedFlag &&
                0 < SyncAddAndFetch(mSyncScheduledCount, 0)) {
            QCStMutexLocker theLocker(mMutex);
            if (mPendingSyncLogSeq.IsValid() &&
                    (! mLogSeq.IsValid() ||
                    0 != mStatus ||
                    mAllowNotPrimaryFlag != mPendingAllowNotPrimaryFlag ||
                    mServers != mPendingSyncServers)) {
                mServers             = mPendingSyncServers;
                mAllowNotPrimaryFlag = mPendingAllowNotPrimaryFlag;
                mServerIdx = 0;
                mSyncScheduledCount = 0;
                mCurDownloadGeneration = mDownloadGeneration;
                const MetaVrLogSeq theLogSeq = mPendingSyncLogSeq;
                theLocker.Unlock();
                mKfsNetClient.ClearMetaServerLocations();
                const bool kUseVrPrimarySelectorFlag = false;
                StartLogSync(theLogSeq, mAllowNotPrimaryFlag,
                    kUseVrPrimarySelectorFlag);
            } else {
                mSyncScheduledCount    = 0;
                mCurDownloadGeneration = mDownloadGeneration;
                theLocker.Unlock();
                if (! mLogSeq.IsValid()) {
                    DownloadDone(0);
                }
            }
        }
        if (! mSleepingFlag ||
                mKfsNetClient.GetNetManager().Now() < mWakeupTime) {
            return;
        }
        mSleepingFlag = false;
        if (mMaxRetryCount < mRetryCount) {
            HandleError();
        } else {
            if (! mSetServerFlag || (mServerIdx < mServers.size() &&
                    mKfsNetClient.GetServerLocation() !=
                        mServers[mServerIdx])) {
                if (mCheckpointFlag) {
                    if (GetCheckpoint() < 0) {
                        panic("meta data sync: get checkpoint failure");
                    }
                } else {
                    LogFetchStart(mCheckLogSeqOnlyFlag);
                }
            } else {
                StartRead();
            }
        }
    }
private:
    enum { kMaxCommitLineLen = 512 };
    typedef SingleLinkedQueue<
        MetaRequest,
        MetaRequest::GetNext
    > FreeWriteOpList;
    typedef set<
        string,
        less<string>,
        StdFastAllocator<string>
    > MetaMds;

    NetManager&       mRuntimeNetManager;
    NetManager        mStartupNetManager;
    KfsNetClient      mKfsNetClient;
    bool              mSetServerFlag;
    string            mFetchOnRestartFileName;
    Servers           mServers;
    Servers           mPendingSyncServers;
    MetaVrLogSeq      mPendingSyncLogSeq;
    MetaVrLogSeq      mPendingSyncLogEndSeq;
    bool              mPendingAllowNotPrimaryFlag;
    int               mSyncScheduledCount;
    QCMutex           mMutex;
    string            mFileName;
    string            mCheckpointDir;
    string            mLogDir;
    string            mTmpSuffix;
    string            mCheckpointFileName;
    string            mLastLogFileName;
    ClientAuthContext mAuthContext;
    ReadOp*           mReadOpsPtr;
    ReadQueue         mFreeList;
    ReadQueue         mPendingList;
    int64_t           mFileSystemId;
    size_t            mReadOpsCount;
    int               mMaxLogBlockSize;
    int               mMaxReadSize;
    int               mMaxRetryCount;
    int               mDebugSubmitLogBlockDropRate;
    int               mDebugSubmitLastLogWriteDropRate;
    int               mRetryCount;
    int               mRetryTimeout;
    int               mCurMaxReadSize;
    int               mFd;
    size_t            mServerIdx;
    int64_t           mPos;
    int64_t           mNextReadPos;
    int64_t           mFileSize;
    MetaVrLogSeq      mLogSeq;
    MetaVrLogSeq      mLastSubmittedLogSeq;
    seq_t             mNextLogSegIdx;
    bool              mCheckpointFlag;
    bool              mAllowNotPrimaryFlag;
    IOBuffer          mBuffer;
    FreeWriteOpList   mFreeWriteOpList;
    int               mFreeWriteOpCount;
    uint32_t          mNextBlockChecksum;
    uint32_t          mCurBlockChecksum;
    seq_t             mNextBlockSeq;
    int               mLogWritesInFlightCount;
    bool              mWriteToFileFlag;
    bool              mWriteSyncFlag;
    int               mMinWriteSize;
    int               mMaxReadOpRetryCount;
    time_t            mWakeupTime;
    bool              mSleepingFlag;
    bool              mShutdownNetManagerFlag;
    bool              mCheckStartLogSeqFlag;
    bool              mCheckLogSeqOnlyFlag;
    bool              mReadPipelineFlag;
    bool              mLogSyncStartedFlag;
    int               mKeepLogSegmentsInterval;
    int               mNoLogSeqCount;
    int               mStatus;
    int               mDownloadStatus;
    bool              mDownloadDoneFlag;
    unsigned int      mDownloadGeneration;
    unsigned int      mCurDownloadGeneration;
    uint64_t          mWriteOpGeneration;
    QCThread          mThread;
    string            mClusterKey;
    string            mSyncCommitName;
    string            mStrBuffer;
    MetaMds           mMetaMds;
    char              mCommmitBuf[kMaxCommitLineLen];

    int DeleteFetchOnRestartFile()
    {
        if (! mFetchOnRestartFileName.empty() &&
                unlink(mFetchOnRestartFileName.c_str())) {
            const int theRet = GetErrno();
            if (ENOENT != theRet) {
                KFS_LOG_STREAM_ERROR <<
                    mFetchOnRestartFileName << ": " <<
                        QCUtils::SysError(theRet) <<
                KFS_LOG_EOM;
                return theRet;
            }
        }
        return 0;
    }
    void FreeWriteOps()
    {
        MetaRequest* thePtr;
        while ((thePtr = mFreeWriteOpList.PopFront())) {
            mFreeWriteOpCount--;
            MetaRequest::Release(thePtr);
        }
        QCASSERT(0 == mFreeWriteOpCount);
    }
    void ClearPendingList()
    {
        ReadOp* thePtr;
        while ((thePtr = mPendingList.Front()) &&
                ! thePtr->mInFlightFlag) {
            mPendingList.PopFront();
            thePtr->Reset();
            mFreeList.PutFront(*thePtr);
        }
    }
    void StopAndClearPending()
    {
        if (! mPendingList.IsEmpty()) {
            ClearPendingList();
            mKfsNetClient.Stop();
            QCASSERT(mPendingList.IsEmpty());
        }
    }
    void StopKeepData()
    {
        if (mThread.IsStarted()) {
            mKfsNetClient.GetNetManager().Shutdown();
            mThread.Join();
        }
    }
    void LogSeqCheckStart()
    {
        LogFetchStart(true);
    }
    void LogFetchStart(
        bool inCheckLogSeqOnlyFlag = false)
    {
        QCASSERT(mLogSeq.IsValid() && ! mServers.empty());
        mStatus = 0;
        if (0 <= mFd) {
            close(mFd);
            mFd = -1;
        }
        if (mServers.size() <= mServerIdx) {
            mServerIdx = 0;
        }
        StopAndClearPending();
        if (mSetServerFlag) {
            KFS_LOG_STREAM_DEBUG <<
                "log fetch node: " << mServers[mServerIdx] <<
                " index: "         << mServerIdx <<
            KFS_LOG_EOM;
            const bool    kCancelPendingOpsFlag = true;
            string* const kOutErrMsgPtr         = 0;
            const bool    kForceConnectFlag     = false;
            mKfsNetClient.SetServer(mServers[mServerIdx],
                kCancelPendingOpsFlag, kOutErrMsgPtr, kForceConnectFlag);
        }
        mCheckStartLogSeqFlag = false;
        mCheckLogSeqOnlyFlag  = inCheckLogSeqOnlyFlag;
        mCheckpointFlag       = false;
        mWriteToFileFlag      = false;
        mNoLogSeqCount        = 0;
        InitRead();
        mCurMaxReadSize = 256; // Just check if log sequence exists.
        StartRead();
    }
    void InitRead()
    {
        StopAndClearPending();
        mReadPipelineFlag = false;
        mNextReadPos      = 0;
        mPos              = 0;
        mFileSize         = -1;
        mNextBlockSeq     = 0;
        mCurMaxReadSize   = mMaxReadSize;
        mCurBlockChecksum = kKfsNullChecksum;
        mBuffer.Clear();
    }
    int InitCheckpoint()
    {
        if (! mReadOpsPtr || mServers.empty()) {
            return -EINVAL;
        }
        if (0 <= mFd) {
            close(mFd);
            mFd = -1;
        }
        StopAndClearPending();
        if (! InitMetaVrPrimarySelector()) {
            return -EINVAL;
        }
        mAllowNotPrimaryFlag  = false;
        mStatus               = 0;
        mLogSeq               = MetaVrLogSeq();
        mCheckpointFlag       = true;
        mWriteToFileFlag      = true;
        mCheckStartLogSeqFlag = false;
        mNextLogSegIdx        = -1;
        mCheckpointFileName.clear();
        mLastLogFileName.clear();
        InitRead();
        return 0;
    }
    bool InitMetaVrPrimarySelector()
    {
        mSetServerFlag = false;
        mKfsNetClient.Stop();
        mKfsNetClient.ClearMetaServerLocations();
        size_t theCount = 0;
        for (Servers::const_iterator theIt = mServers.begin();
                mServers.end() != theIt;
                ++theIt) {
            const bool kAllowDuplicatesFlag = false;
            if (mKfsNetClient.AddMetaServerLocation(
                    *theIt, kAllowDuplicatesFlag)) {
                theCount++;
                KFS_LOG_STREAM_DEBUG <<
                    "added VR meta node location:" << *theIt <<
                KFS_LOG_EOM;
            }
        }
        return (0 < theCount);
    }
    int GetCheckpoint()
    {
        const int theRet = InitCheckpoint();
        if (theRet < 0) {
            return theRet;
        }
        if (! StartRead()) {
            panic("metad data sync: failed to iniate checkpoint download");
            return -EFAULT;
        }
        return 0;
    }
    bool StartRead()
        { return StartRead(mNextReadPos); }
    bool StartRead(
        int64_t inNextReadPos)
    {
        ReadOp* const theOpPtr = mFreeList.PopFront();
        if (! theOpPtr) {
            return false;
        }
        ReadOp& theOp = *theOpPtr;
        theOp.mPos = inNextReadPos;
        mPendingList.PushBack(theOp);
        return StartRead(theOp);
    }
    bool StartRead(
        ReadOp& inOp)
    {
        inOp.fileSystemId        = mFileSystemId;
        inOp.startLogSeq         = mLogSeq;
        inOp.readPos             = inOp.mPos;
        inOp.checkpointFlag      = mCheckpointFlag;
        inOp.allowNotPrimaryFlag = mAllowNotPrimaryFlag;
        inOp.readSize            = 0 <= mFileSize ?
            (int)min(mFileSize - inOp.mPos, int64_t(mCurMaxReadSize)) :
            mCurMaxReadSize;
        inOp.mInFlightFlag       = true;
        inOp.mPos                = inOp.mPos;
        inOp.fileSize            = -1;
        inOp.status              = 0;
        inOp.fileName.clear();
        inOp.statusMsg.clear();
        inOp.mBuffer.Clear();
        if (! mKfsNetClient.Enqueue(&inOp, this, &inOp.mBuffer)) {
            panic("metad data sync: read op enqueue failure");
            return false;
        }
        return true;
    }
    void Handle(
        ReadOp& inOp)
    {
        if (inOp.clusterKey != mClusterKey) {
            inOp.status    = -EINVAL;
            inOp.statusMsg = "invalid cluster key";
            HandleReadError(inOp);
            return;
        }
        if (! mMetaMds.empty() &&
                mMetaMds.find(inOp.metaMd) == mMetaMds.end()) {
            inOp.status    = -EINVAL;
            inOp.statusMsg = "invalid meta md";
            HandleReadError(inOp);
            return;
        }
        if (mLogSeq.IsValid() && ((0 != mPos || mWriteToFileFlag) ?
                mLogSeq != inOp.startLogSeq : mLogSeq < inOp.startLogSeq)) {
            KFS_LOG_STREAM_ERROR <<
                "start log sequence has chnaged:"
                " from: "  << mLogSeq <<
                " to: "    << inOp.startLogSeq <<
                " write: " << mWriteToFileFlag <<
                " pos: "   << mPos <<
            KFS_LOG_EOM;
            inOp.status    = -EINVAL;
            inOp.statusMsg = "invalid sequence";
            HandleReadError(inOp);
            return;
        }
        if (0 == mPos) {
            QCASSERT(mPendingList.Front() == &inOp &&
                mPendingList.Back() == &inOp &&
                inOp.mPos == mPos && 0 == mPos);
            if (mFileSystemId < 0) {
                mFileSystemId = inOp.fileSystemId;
            } else if (mFileSystemId != inOp.fileSystemId) {
                KFS_LOG_STREAM_ERROR <<
                    "file system id mismatch:"
                    " expect: "   << mFileSystemId <<
                    " received: " << inOp.fileSystemId <<
                KFS_LOG_EOM;
                inOp.status    = -EINVAL;
                inOp.statusMsg = "file system id mismatch";
                HandleReadError(inOp);
                return;
            }
            if (mCheckStartLogSeqFlag && mLogSeq.IsValid() &&
                    mLogSeq != inOp.startLogSeq) {
                KFS_LOG_STREAM_ERROR <<
                    "start log sequence mismatch:"
                    " expect: "   << mLogSeq <<
                    " received: " << inOp.startLogSeq <<
                KFS_LOG_EOM;
                inOp.status    = -EINVAL;
                inOp.statusMsg = "log sequence mismatch";
                HandleReadError(inOp);
                return;
            }
            if (mCheckLogSeqOnlyFlag) {
                if (mLogSeq < inOp.startLogSeq) {
                    KFS_LOG_STREAM_ERROR <<
                        "start log sequence: "      << inOp.startLogSeq <<
                        " greater than requested: " << mLogSeq <<
                    KFS_LOG_EOM;
                    inOp.status    = -EINVAL;
                    inOp.statusMsg =
                        "start log sequence greater than requrested";
                    HandleReadError(inOp);
                    return;
                }
                if (mPendingList.PopFront() != &inOp ||
                        ! mPendingList.IsEmpty()) {
                    panic("metad data sync: invalid pending list");
                }
                inOp.Reset();
                mFreeList.PutFront(inOp);
                mStatus = 0;
                if (mShutdownNetManagerFlag) {
                    mKfsNetClient.GetNetManager().Shutdown();
                } else {
                    ScheduleGetLogSeqNextServer();
                }
                return;
            }
            const MetaVrLogSeq thePrevLogSeq = mLogSeq;
            mLogSeq         = inOp.startLogSeq;
            mCurMaxReadSize = min(IOBuffer::BufPos(mMaxReadSize),
                IOBuffer::BufPos(0 < inOp.maxReadSize ?
                    inOp.maxReadSize : inOp.mBuffer.BytesConsumable()));
            mNextReadPos    = inOp.mBuffer.BytesConsumable();
            if (0 <= inOp.fileSize) {
                mFileSize = inOp.fileSize;
            }
            if (mCheckpointFlag && mFileSize < 0) {
                inOp.status    = -EINVAL;
                inOp.statusMsg = "invalid checkpoint file size";
                HandleReadError(inOp);
                return;
            }
            if (mWriteToFileFlag && mFd < 0) {
                const char* const thePrefixPtr = mCheckpointFlag ?
                    MetaDataStore::GetCheckpointFileNamePrefixPtr() :
                    MetaDataStore::GetLogSegmentFileNamePrefixPtr();
                size_t const       thePrefixLen = strlen(thePrefixPtr);
                size_t const       theNameLen   = inOp.fileName.size();
                const char* const  theNamePtr   = inOp.fileName.c_str();
                MetaVrLogSeq       theLogSeq;
                seq_t              theSegIdx    = -1;
                if (theNameLen < thePrefixLen ||
                        0 != memcmp(thePrefixPtr, theNamePtr, thePrefixLen) ||
                        ! MetaDataStore::GetLogSequenceFromFileName(
                            theNamePtr, theNameLen, theLogSeq,
                            mCheckpointFlag ? 0 : &theSegIdx) ||
                        theLogSeq != mLogSeq ||
                        (! mCheckpointFlag && 0 < mNextLogSegIdx &&
                            theSegIdx != mNextLogSegIdx &&
                            (! thePrevLogSeq.IsValid() ||
                                thePrevLogSeq != mLogSeq ||
                                theSegIdx < mNextLogSegIdx)
                        )) {
                    KFS_LOG_STREAM_ERROR <<
                        "invalid file name: " << inOp.Show() <<
                        " log sequence:"
                        " expected: " << mLogSeq <<
                        " actual: "   << theLogSeq <<
                        " segment:"
                        " expected: " << mNextLogSegIdx <<
                        " actual: "   << theSegIdx <<
                    KFS_LOG_EOM;
                    inOp.status    = -EINVAL;
                    inOp.statusMsg = "invalid file name";
                    HandleReadError(inOp);
                    return;
                }
                if (mCheckpointFlag) {
                    mFileName.assign(
                        mCheckpointDir.data(), mCheckpointDir.size());
                } else {
                    if (mNextLogSegIdx < 0) {
                        mNextLogSegIdx = theSegIdx;
                    }
                    mFileName.assign(mLogDir.data(), mLogDir.size());
                }
                if (! mFileName.empty() && '/' != *mFileName.rbegin()) {
                    mFileName += '/';
                }
                mFileName += inOp.fileName;
                if (mNextLogSegIdx != theSegIdx) {
                    const size_t thePos = mFileName.rfind('.');
                    if (string::npos == thePos) {
                        inOp.status    = -EINVAL;
                        inOp.statusMsg = "invalid file name";
                        HandleReadError(inOp);
                        return;
                    }
                    mFileName.erase(thePos + 1);
                    AppendDecIntToString(mFileName, mNextLogSegIdx);
                }
                mFileName += mTmpSuffix;
                mFd = open(
                    mFileName.c_str(),
                    O_WRONLY | (mWriteSyncFlag ? O_SYNC : 0) |
                        O_CREAT | O_TRUNC,
                    0666
                );
                if (mFd < 0) {
                    const int theErr = GetErrno();
                    KFS_LOG_STREAM_ERROR <<
                        "open failure: " << mFileName <<
                        ": " << QCUtils::SysError(theErr) <<
                    KFS_LOG_EOM;
                    inOp.status    = 0 < theErr ? -theErr : -EFAULT;
                    inOp.statusMsg = "failed to open file";
                    HandleReadError(inOp);
                    return;
                }
            }
            mReadPipelineFlag = mFileSize < 0;
            if (mCheckpointFlag) {
                // Allow to transition into backup after the initial read.
                // Turn off switch over to new primary, in order to get
                // consistent checkpoint and transaction logs.
                mKfsNetClient.ClearMetaServerLocations();
                mAllowNotPrimaryFlag = true;
            }
        } else if (0 < mFileSize) {
            if (inOp.fileSize != mFileSize) {
                KFS_LOG_STREAM_ERROR <<
                    "file size has chnaged:"
                    " from: " << mFileSize <<
                    " to: "   << inOp.fileSize <<
                KFS_LOG_EOM;
                inOp.status    = -EINVAL;
                inOp.statusMsg = "invalid file size";
                HandleReadError(inOp);
                return;
            }
            if (inOp.readSize != inOp.mBuffer.BytesConsumable()) {
                KFS_LOG_STREAM_ERROR <<
                    "invalid read size:"
                    " pos: "      << inOp.mPos <<
                    " expected: " << inOp.readSize <<
                    " received: " << inOp.mBuffer.BytesConsumable() <<
                    " max read: " << mCurMaxReadSize <<
                    " eof: "      << mFileSize <<
                KFS_LOG_EOM;
                inOp.status    = -EINVAL;
                inOp.statusMsg = "short read";
                HandleReadError(inOp);
                return;
            }
        }
        if (inOp.mPos != mPos) {
            return;
        }
        ReadOp* theOpPtr = mPendingList.Front();
        if (! theOpPtr || &inOp != theOpPtr || theOpPtr->mInFlightFlag) {
            panic("metad data sync: invalid empty pending list");
            return;
        }
        int  theRdSize;
        bool theEofFlag = false;
        do {
            theRdSize = theOpPtr->mBuffer.BytesConsumable();
            if (mReadPipelineFlag && mPos != theOpPtr->mPos && 0 < theRdSize) {
                QCASSERT(mFileSize < 0);
                // Turn off read pipelining, and restart from the current
                // position, to handle current log segment grows while read
                // RPC are in flight.
                ClearPendingList();
                mKfsNetClient.Cancel();
                mNextReadPos      = mPos;
                mReadPipelineFlag = false;
                break;
            }
            QCASSERT(mPos == theOpPtr->mPos || 0 == theRdSize);
            mPos += theRdSize;
            theEofFlag = 0 <= mFileSize ? mFileSize <= mPos :
                theRdSize < max(1, min(
                    theOpPtr->readSize, theOpPtr->maxReadSize));
            if (0 <= mFd) {
                IOBuffer& theBuffer = theOpPtr->mBuffer;
                mBuffer.Move(&theBuffer);
                if (theEofFlag || mMinWriteSize <= mBuffer.BytesConsumable()) {
                    while (! mBuffer.IsEmpty()) {
                        const int theNWr = mBuffer.Write(mFd);
                        if (theNWr < 0) {
                            KFS_LOG_STREAM_ERROR <<
                                mFileName << ": write failure:" <<
                                QCUtils::SysError((int)-theNWr) <<
                            KFS_LOG_EOM;
                            HandleError();
                            return;
                        }
                    }
                }
            } else {
                if (theOpPtr->checkpointFlag || mCheckpointFlag) {
                    panic("metad data sync: attempt to replay checkpoint");
                } else {
                    if (! SubmitLogBlock(theOpPtr->mBuffer)) {
                        return;
                    }
                }
            }
            if (theEofFlag && ! mCheckpointFlag) {
                if (theOpPtr->endLogSeq.IsValid()) {
                    mLogSeq = theOpPtr->endLogSeq;
                } else {
                    // Last unfinished log segment -- mark end of download by
                    // making log sequence non valid.
                    mLogSeq = theOpPtr->startLogSeq;
                    mLogSeq.mLogSeq = -mLogSeq.mLogSeq - 1;
                }
                mNextLogSegIdx++;
            }
            theOpPtr->Reset();
            QCVERIFY(mPendingList.PopFront() == theOpPtr);
            mFreeList.PutFront(*theOpPtr);
        } while (
            ! theEofFlag &&
            (theOpPtr = mPendingList.Front()) &&
            ! theOpPtr->mInFlightFlag);
        if (theEofFlag) {
            ReadDone();
        } else {
            if (mFileSize < 0) {
                if (mReadPipelineFlag) {
                    while (StartRead()) {
                        mNextReadPos += mCurMaxReadSize;
                    }
                } else {
                    mNextReadPos    = mPos;
                    mCurMaxReadSize = mMaxReadSize;
                    if (StartRead()) {
                        mNextReadPos += mCurMaxReadSize;
                    }
                }
            } else {
                while (mNextReadPos < mFileSize && StartRead()) {
                    mNextReadPos += mCurMaxReadSize;
                }
            }
        }
    }
    void ReadDone()
    {
        ClearPendingList();
        mKfsNetClient.Cancel();
        QCRTASSERT(mPendingList.IsEmpty());
        if (0 <= mFd) {
            const int theFd = mFd;
            mFd = -1;
            if (close(theFd)) {
                const int theErr = GetErrno();
                KFS_LOG_STREAM_ERROR <<
                    mFileName << ": close failure:" <<
                    QCUtils::SysError(theErr) <<
                KFS_LOG_EOM;
                mFileName.clear();
                HandleError();
                return;
            }
            if (mCheckpointFlag) {
                mCheckpointFileName.assign(
                    mFileName.data(), mFileName.size());
            } else if (mLogSeq.IsValid()) {
                mLastLogFileName.assign(
                    mFileName.data(), mFileName.size());
            }
        }
        mBuffer.Clear();
        mRetryCount     = 0;
        mCheckpointFlag = false;
        if (mLogSeq.IsValid()) {
            mCheckStartLogSeqFlag = true;
            InitRead();
            if (! StartRead()) {
                panic("meta data sync: failed to initiate download");
            }
        } else {
            DownloadDone(0);
        }
    }
    void DownloadDone(
        int inStatus)
    {
        mStatus = inStatus;
        KFS_LOG_STREAM(0 == mStatus ?
                MsgLogger::kLogLevelDEBUG :
                MsgLogger::kLogLevelERROR)
            "fetch complete:"
            " status: "     << mStatus <<
            " seq: "        << mLogSeq <<
            " checkpoint: " << mCheckpointFlag <<
            " shutdown: "   << mShutdownNetManagerFlag <<
        KFS_LOG_EOM;
        if (mShutdownNetManagerFlag) {
            mKfsNetClient.GetNetManager().Shutdown();
        }
        QCStMutexLocker theLocker(mMutex);
        if (mCurDownloadGeneration == mDownloadGeneration) {
            mDownloadStatus   = mStatus;
            mDownloadDoneFlag = true;
        }
        theLocker.Unlock();
        if (mKfsNetClient.GetNetManager().IsRunning()) {
            MetaLogWriterControl& theOp = GetLogWriteOp();
            theOp.Reset(MetaLogWriterControl::kLogFetchDone);
            FreeWriteOps();
            submit_request(&theOp);
        } else {
            FreeWriteOps();
        }
    }
    void HandleReadError(
        ReadOp& inReadOp)
    {
        KFS_LOG_STREAM_ERROR <<
            "status: " << inReadOp.status <<
            " "        << (inReadOp.statusMsg.empty() ?
                ErrorCodeToString(inReadOp.status) : inReadOp.statusMsg) <<
            " try: "   << mRetryCount <<
            " op: "    << inReadOp.mRetryCount <<
            " pos:"
            " cur: "   << mPos <<
            " op: "    << inReadOp.mPos <<
            " "        << inReadOp.Show() <<
        KFS_LOG_EOM;
        mStatus = inReadOp.status;
        if (-ENOENT == mStatus ||
                KfsNetClient::kErrorMaxRetryReached == mStatus) {
            if (-ENOENT == mStatus) {
                mNoLogSeqCount++;
            }
            ScheduleGetLogSeqNextServer();
            return;
        }
        if (! mCheckpointFlag || 0 < inReadOp.mPos || 1 < mServers.size()) {
            const int64_t thePos        = inReadOp.mPos;
            const int     theRetryCount = inReadOp.mRetryCount;
            inReadOp.Reset();
            if (theRetryCount < mMaxReadOpRetryCount) {
                inReadOp.mPos        = thePos;
                inReadOp.mRetryCount = theRetryCount + 1;
                if (StartRead(inReadOp)) {
                    return;
                }
            }
        }
        HandleError();
    }
    int ScheduleMetaDataFetchAfterRestart()
    {
        if (mFetchOnRestartFileName.empty() || mServers.empty()) {
            return 0;
        }
        mStrBuffer.clear();
        const char* theSepPtr = "";
        for (Servers::const_iterator theIt = mServers.begin();
                mServers.end() != theIt;
                ++theIt) {
            mStrBuffer += theSepPtr;
            theIt->AppendToString(mStrBuffer);
            theSepPtr = " ";
        }
        mStrBuffer += '\n';
        return WriteToFile(mFetchOnRestartFileName, mStrBuffer);
    }
    void ScheduleGetLogSeqNextServer()
    {
        Reset();
        if (++mServerIdx < mServers.size()) {
            Sleep(0);
        } else {
            const size_t theCnt = mServers.size();
            if (theCnt <= 0 || (int)(theCnt * 3) < mNoLogSeqCount) {
                if (mShutdownNetManagerFlag) {
                    mKfsNetClient.GetNetManager().Shutdown();
                } else {
                    ScheduleMetaDataFetchAfterRestart();
                    Sleep(mKeepLogSegmentsInterval);
                }
            } else {
                if (0 == ScheduleMetaDataFetchAfterRestart()) {
                    KFS_LOG_STREAM_ERROR <<
                        "no log segment available: " << mLogSeq <<
                        " try count: "               << mNoLogSeqCount <<
                        " servers count: "           << theCnt <<
                        " scheduling meta server restart to fetch latest"
                        " checkpoint" <<
                    KFS_LOG_EOM;
                    submit_request(new MetaProcessRestart());
                    Sleep(max(60, 5 * mKeepLogSegmentsInterval));
                } else {
                    Sleep(mKeepLogSegmentsInterval);
                }
            }
        }
    }
    void HandleError()
    {
        mKfsNetClient.ClearMetaServerLocations();
        Reset();
        mRetryCount++;
        int theRetryTimeout = mRetryTimeout;
        if ((mCheckpointFlag && (0 == mPos || mPos < mFileSize / 4)) ||
                ! mWriteToFileFlag ||
                mMaxRetryCount < mRetryCount) {
            if (mWriteToFileFlag) {
                if (0 == (mStatus = PrepareToSync())) {
                    mRetryCount = 0;
                    mServerIdx++;
                    if (0 != InitCheckpoint()) {
                        panic("metad data sync: init checkpoint failure");
                    }
                    theRetryTimeout = 0;
                } else {
                    if (mShutdownNetManagerFlag) {
                        mKfsNetClient.GetNetManager().Shutdown();
                        return;
                    }
                }
            } else {
                if (mCheckLogSeqOnlyFlag) {
                    ScheduleGetLogSeqNextServer();
                } else {
                    DownloadDone(-EIO);
                }
                return;
            }
        }
        KFS_LOG_STREAM_ERROR <<
            "retry: "   << mRetryCount <<
            " of "      << mMaxRetryCount <<
            " server: " << mKfsNetClient.GetServerLocation() <<
            " scheduling retry in: " << theRetryTimeout <<
        KFS_LOG_EOM;
        Sleep(theRetryTimeout);
    }
    void Sleep(
        int inTimeSec)
    {
        if (mSleepingFlag) {
            return;
        }
        mSleepingFlag = true;
        mWakeupTime   = mKfsNetClient.GetNetManager().Now() + inTimeSec;
    }
    void Reset()
    {
        if (0 < mFd) {
            close(mFd);
            mFd = -1;
        }
        mWriteOpGeneration++;
        ClearPendingList();
        mKfsNetClient.Stop();
        QCRTASSERT(mPendingList.IsEmpty());
        mBuffer.Clear();
        mPos          = 0;
        mNextReadPos  = 0;
        mSleepingFlag = false;
    }
    class FieldParser
    {
    public:
        template<typename T>
        static bool Parse(
            const char*& ioPtr,
            size_t       inLen,
            T&           outVal)
            { return HexIntParser::Parse(ioPtr, inLen, outVal); }
        static bool Parse(
            const char*&  ioPtr,
            size_t        inLen,
            MetaVrLogSeq& outVal)
            { return outVal.Parse<HexIntParser>(ioPtr, inLen); }
    };
    template<typename T>
    bool ParseField(
        const char*& ioPtr,
        const char*  inEndPtr,
        T&           outVal)
    {
        const char* theStartPtr = ioPtr;
        while (ioPtr < inEndPtr && (*ioPtr & 0xFF) != '/') {
            ++ioPtr;
        }
        if (ioPtr < inEndPtr &&
                FieldParser::Parse(
                    theStartPtr, ioPtr - theStartPtr, outVal)) {
            ++ioPtr;
            return true;
        }
        return false;
    }
    bool SubmitLogBlock(
        IOBuffer& inBuffer)
    {
        mBuffer.Move(&inBuffer);
        if (0 < mDebugSubmitLogBlockDropRate &&
                0 == mWriteOpGeneration % mDebugSubmitLogBlockDropRate) {
            KFS_LOG_STREAM_ERROR <<
                "simulating log fetch failure by dropping incoming data"
                " size: "       << mBuffer.BytesConsumable() <<
                " generation: " << mWriteOpGeneration <<
                " drop rate: "  << mDebugSubmitLogBlockDropRate <<
                " sync end: "   << mPendingSyncLogEndSeq <<
            KFS_LOG_EOM;
            mBuffer.Clear();
        }
        const bool theDebugDropPastSyncEndFlag =
            0 < mDebugSubmitLastLogWriteDropRate &&
            mPendingSyncLogEndSeq.IsValid() &&
            0 == mWriteOpGeneration % mDebugSubmitLastLogWriteDropRate;
        for (; ;) {
            int thePos = mBuffer.IndexOf(0, "\nc/");
            if (thePos < 0) {
                break;
            }
            thePos++;
            const int theEndPos = mBuffer.IndexOf(thePos, "\n");
            if (theEndPos < 0) {
                break;
            }
            IOBuffer::BufPos theLen = theEndPos - thePos;
            if (kMaxCommitLineLen < theLen) {
                KFS_LOG_STREAM_ERROR <<
                    "log block commit line"
                    " length: "  << theLen <<
                    " exceeds: " << kMaxCommitLineLen <<
                    IOBuffer::DisplayData(mBuffer, 512) <<
                KFS_LOG_EOM;
                HandleError();
                return false;
            }
            MetaLogWriterControl& theOp = GetLogWriteOp();
            const int theRem = LogReceiver::ParseBlockLines(
                mBuffer, theEndPos + 1, theOp, '\n');
            if (0 != theRem || theOp.blockLines.IsEmpty()) {
                KFS_LOG_STREAM_ERROR <<
                    "invalid log block received:" <<
                    IOBuffer::DisplayData(mBuffer, 512) <<
                KFS_LOG_EOM;
                HandleError();
                return false;
            }
            // Copy log block body, i.e. except trailing / commit line.
            theOp.blockData.Move(&mBuffer, thePos);
            const char* const theStartPtr =
                mBuffer.CopyOutOrGetBufPtr(mCommmitBuf, theLen);
            const char* const theEndPtr   = theStartPtr + theLen;
            const char*       thePtr      = theStartPtr;
            thePtr += 2;
            MetaVrLogSeq theCommitted;
            fid_t        theFid         = -1;
            int64_t      theErrChkSum   = -1;
            int          theBlockStatus = -1;
            MetaVrLogSeq theLogSeq;
            int          theBlockSeqLen = -1;
            if (! (ParseField(thePtr, theEndPtr, theCommitted) &&
                    ParseField(thePtr, theEndPtr, theFid) &&
                    ParseField(thePtr, theEndPtr, theErrChkSum) &&
                    ParseField(thePtr, theEndPtr, theBlockStatus) &&
                    ParseField(thePtr, theEndPtr, theLogSeq) &&
                    ParseField(thePtr, theEndPtr, theBlockSeqLen) &&
                    theCommitted.IsValid() &&
                    0 <= theBlockStatus &&
                    theCommitted <= theLogSeq &&
                    theBlockSeqLen <= theLogSeq.mLogSeq)) {
                KFS_LOG_STREAM_ERROR <<
                    "invalid log commit line:" <<
                    IOBuffer::DisplayData(mBuffer, theLen) <<
                KFS_LOG_EOM;
                HandleError();
                return false;
            }
            theOp.blockStartSeq = theLogSeq;
            theOp.blockStartSeq.mLogSeq -= theBlockSeqLen;
            theOp.blockEndSeq   = theLogSeq;
            const int theCopyLen  = (int)(thePtr - theStartPtr);
            int64_t   theBlockSeq = -1;
            if (! ParseField(thePtr, theEndPtr, theBlockSeq) ||
                    mNextBlockSeq != theBlockSeq) {
                KFS_LOG_STREAM_ERROR <<
                    "invalid log commit block sequence:" <<
                    " expected: " << mNextBlockSeq <<
                    " actual: "   << theBlockSeq <<
                    " data: " << IOBuffer::DisplayData(mBuffer, theLen) <<
                KFS_LOG_EOM;
                HandleError();
                return false;
            }
            mNextBlockSeq++;
            const int theBlkSeqLen = thePtr - (theStartPtr + theCopyLen);
            uint32_t  theChecksum  = 0;
            if (! HexIntParser::Parse(
                    thePtr, theEndPtr - thePtr, theChecksum)) {
                KFS_LOG_STREAM_ERROR <<
                    "log commit block checksum parse failure:" <<
                    " data: " << IOBuffer::DisplayData(mBuffer, theLen) <<
                KFS_LOG_EOM;
                HandleError();
                return false;
            }
            theOp.blockData.Move(&mBuffer, theCopyLen);
            theOp.blockChecksum = ComputeBlockChecksum(
                &theOp.blockData,
                theOp.blockData.BytesConsumable(),
                kKfsNullChecksum
            );
            if (mCurBlockChecksum != kKfsNullChecksum) {
                mCurBlockChecksum = ChecksumBlocksCombine(
                    mCurBlockChecksum,
                    theOp.blockChecksum,
                    theOp.blockData.BytesConsumable()
                );
            } else {
                mCurBlockChecksum = theOp.blockChecksum;
            }
            mCurBlockChecksum = ComputeBlockChecksum(
                &mBuffer, theBlkSeqLen, mCurBlockChecksum);
            if (mCurBlockChecksum != theChecksum) {
                KFS_LOG_STREAM_ERROR <<
                    "log block checksum mismatch:"
                    " expected: " << theChecksum <<
                    " computed: " << mCurBlockChecksum <<
                KFS_LOG_EOM;
                HandleError();
                return false;
            }
            theOp.blockLines.Back() -=
                mBuffer.Consume(theLen - theCopyLen + 1);
            mCurBlockChecksum = mNextBlockChecksum;
            if (theBlockSeqLen <= 0 || theLogSeq < mLastSubmittedLogSeq) {
                Release(theOp);
            } else if (theDebugDropPastSyncEndFlag &&
                    mPendingSyncLogEndSeq < theOp.blockStartSeq) {
                KFS_LOG_STREAM_ERROR <<
                    "simulating log fetch latency by dropping log"
                    " write"
                    " size: "       << theOp.blockData.BytesConsumable() <<
                    " generation: " << mWriteOpGeneration <<
                    " drop rate: "  << mDebugSubmitLastLogWriteDropRate <<
                    " sync end: "   << mPendingSyncLogEndSeq <<
                    " "             << theOp.Show() <<
                KFS_LOG_EOM;
                Release(theOp);
            } else {
                mLogWritesInFlightCount++;
                submit_request(&theOp);
            }
        }
        if (mMaxLogBlockSize < mBuffer.BytesConsumable()) {
            KFS_LOG_STREAM_ERROR <<
                "log block size: " << mBuffer.BytesConsumable() <<
                " exceeds: "       << mMaxLogBlockSize <<
                " data: "          << IOBuffer::DisplayData(mBuffer, 256) <<
            KFS_LOG_EOM;
            HandleError();
            return false;
        }
        return true;
    }
    MetaLogWriterControl& GetLogWriteOp()
    {
        MetaLogWriterControl* thePtr =
            static_cast<MetaLogWriterControl*>(mFreeWriteOpList.PopFront());
        if (thePtr) {
            mFreeWriteOpCount--;
        } else {
            thePtr = new MetaLogWriterControl(
                MetaLogWriterControl::kWriteBlock);
            thePtr->clnt = this;
        }
        thePtr->generation = mWriteOpGeneration;
        return *thePtr;
    }
    int LogWriteDone(
        int   inEvent,
        void* inDataPtr)
    {
        if (! inDataPtr || EVENT_CMD_DONE != inEvent ||
                mLogWritesInFlightCount <= 0) {
            panic("metad data sync: invalid log write completion");
            return -1;
        }
        mLogWritesInFlightCount--;
        MetaLogWriterControl& theOp =
            *reinterpret_cast<MetaLogWriterControl*>(inDataPtr);
        if (theOp.generation != mWriteOpGeneration) {
            MetaRequest::Release(&theOp);
            return 0;
        }
        mLastSubmittedLogSeq = max(mLastSubmittedLogSeq, theOp.lastLogSeq);
        const bool theErrorFlag = (-EINVAL == theOp.status &&
            theOp.blockStartSeq == theOp.lastLogSeq) ||
            (0 != theOp.status && theOp.lastLogSeq < theOp.blockStartSeq);
        KFS_LOG_STREAM(theErrorFlag ?
                MsgLogger::kLogLevelERROR :
                MsgLogger::kLogLevelDEBUG) <<
            "log fetch:"
            " submitted: " << mLastSubmittedLogSeq <<
            " last log: "  << theOp.lastLogSeq <<
            " in flight: " << mLogWritesInFlightCount <<
            " "            << inDataPtr <<
            " "            << theOp.Show() <<
        KFS_LOG_EOM;
        Release(theOp);
        if (theErrorFlag && mReadOpsPtr) {
            HandleError();
        }
        return 0;
    }
    void Release(
        MetaLogWriterControl& inOp)
    {
        if (mReadOpsPtr) {
            inOp.Reset(MetaLogWriterControl::kWriteBlock);
            inOp.clnt = this;
            mFreeWriteOpCount++;
            mFreeWriteOpList.PutFront(inOp);
        } else {
            MetaRequest::Release(&inOp);
        }
        return;
    }
    int PrepareToSync()
    {
        const bool kDeleteTmpFlag = true;
        const int  theRet         = DeleteTmp(mLogDir, kDeleteTmpFlag);
        if (0 != theRet) {
            return theRet;
        }
        return DeleteTmp(mCheckpointDir, kDeleteTmpFlag);
    }
    int LinkLatest(
        const string& inName,
        const char*   inAliasPtr)
    {
        const size_t theSufSize = mTmpSuffix.size();
        const size_t theSize    = inName.size();
        string       theName(inName.data(),
            theSufSize < theSize ? theSize - theSufSize : theSize);
        size_t thePos = theName.rfind('/');
        string theAlias;
        if (string::npos != thePos) {
            ++thePos;
            theAlias.assign(theName.data(), thePos);
            theAlias += inAliasPtr;
        } else {
            theAlias = inAliasPtr;
        }
        const int theRet = link_latest(theName, theAlias);
        if (0 != theRet) {
            KFS_LOG_STREAM_ERROR <<
                "link: " << theName <<
                " => "   << theAlias <<
                ": "     << QCUtils::SysError(-theRet) <<
            KFS_LOG_EOM;
        }
        return theRet;
    }
    static int GetErrno()
    {
        int theRet = errno;
        if (0 == theRet) {
            theRet = EIO;
        }
        return theRet;
    }
    static int Delete(
        const string& inName)
    {
        if (0 == unlink(inName.c_str())) {
            const int theErr = GetErrno();
            if (ENOENT != theErr) {
                KFS_LOG_STREAM_ERROR <<
                    inName << ": " << QCUtils::SysError(theErr) <<
                KFS_LOG_EOM;
                return theErr;
            }
        }
        return 0;
    }
    template<typename T>
    static bool ParseDecInt(
        const string& inField,
        T&            outVal)
    {
        const char* thePtr = inField.data();
        return DecIntParser::Parse(thePtr, inField.size(), outVal);
    }
    static uint32_t ComputeCommitStateChecksum(
        const string& inLine1,
        const string& inLine2,
        const string& inLine3)
    {
        return ComputeCrc32(inLine3.data(), inLine3.size(), ComputeCrc32(
            inLine2.data(), inLine2.size(), ComputeCrc32(
                inLine1.data(), inLine1.size(), 0
        )));
    }
    int WriteCommitState(
        int inState)
    {
        if (mSyncCommitName.empty()) {
            panic("meta data sync: invalid write commit state invocation");
            return -EINVAL;
        }
        mStrBuffer.clear();
        uint32_t theCrc32 = 0;
        for (int theLine = 0; theLine < 2; theLine++) {
            const string& theName = 0 == theLine ?
                mCheckpointFileName : mLastLogFileName;
            size_t thePos = theName.rfind('/');
            if (string::npos == thePos) {
                thePos = 0;
            } else {
                thePos++;
            }
            const char* const thePtr = theName.data() + thePos;
            const size_t      theLen = theName.size() - thePos;
            theCrc32 = ComputeCrc32(thePtr,  theLen, theCrc32);
            mStrBuffer.append(thePtr, theLen);
            mStrBuffer += '\n';
        }
        const size_t thePos = mStrBuffer.size();
        AppendDecIntToString(mStrBuffer, inState);
        theCrc32 = ComputeCrc32(
            mStrBuffer.data() + thePos, mStrBuffer.size() - thePos, theCrc32);
        mStrBuffer += '\n';
        AppendDecIntToString(mStrBuffer, theCrc32);
        mStrBuffer += '\n';
        return WriteToFile(mSyncCommitName, mStrBuffer);
    }
    int WriteToFile(
        const string& inName,
        const string& inBuffer)
    {
        if (inName.empty()) {
            panic("meta data sync: invalid empty file name");
            return -EFAULT;
        }
        const string theName = inName + mTmpSuffix;
        int theRet = Delete(theName);
        if (0 != theRet) {
            return theRet;
        }
        const int theFd = open(
            theName.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
        if (theFd < 0) {
            theRet = GetErrno();
        } else {
            const char*       thePtr    = inBuffer.data();
            const char* const theEndPtr = thePtr + inBuffer.size();
            while (thePtr < theEndPtr) {
                const ssize_t theNWr = write(theFd, thePtr, theEndPtr - thePtr);
                if (theNWr < 0) {
                    theRet = GetErrno();
                    break;
                }
                thePtr += theNWr;
            }
            if (0 == theRet && fsync(theFd)) {
                theRet = GetErrno();
            }
            if (close(theFd) && 0 == theRet) {
                theRet = GetErrno();
            }
            if (0 == theRet && ! mTmpSuffix.empty() &&
                    0 != rename(theName.c_str(), inName.c_str())) {
                theRet = GetErrno();
            }
        }
        if (0 != theRet) {
            KFS_LOG_STREAM_ERROR <<
                theName << ": " << QCUtils::SysError(theRet) <<
            KFS_LOG_EOM;
            Delete(theName);
            theRet = theRet < 0 ? theRet : -theRet;
        }
        return theRet;
    }
    int CommitSync(
        int inStep = -1)
    {
        if (mCheckpointFileName.empty() || mSyncCommitName.empty()) {
            panic("meta data sync: invalid commit sync invocation");
            return -EINVAL;
        }
        int theRet   = 0;
        int theState = inStep;
        if (theState < 0) {
            theState = 0;
            if (0 != (theRet = WriteCommitState(theState))) {
                return theRet;
            }
        }
        const bool kDeleteTmpFlag          = false;
        const bool kRenameIgnoreNonTmpFlag = true;
        for (; ;) {
            switch (theState) {
                case 0:
                    // Remove all non temporary files..
                    if (0 != (theRet = DeleteTmp(mLogDir, kDeleteTmpFlag))) {
                        return theRet;
                    }
                    if (0 != (theRet = DeleteTmp(
                            mCheckpointDir, kDeleteTmpFlag))) {
                        return theRet;
                    }
                    break;
                case 1:
                    // Rename temporary files into target files.
                    if (0 != (theRet = RenameTmp(
                            mLogDir, kRenameIgnoreNonTmpFlag))) {
                        return theRet;
                    }
                    if (0 != (theRet = RenameTmp(
                            mCheckpointDir, kRenameIgnoreNonTmpFlag))) {
                        return theRet;
                    }
                    break;
                case 2:
                    if (0 != (theRet = LinkLatest(mCheckpointFileName,
                            MetaDataStore::GetCheckpointLatestFileNamePtr()))) {
                        return theRet;
                    }
                    if (0 != (theRet = mLastLogFileName.empty() ? 0 :
                            LinkLatest(mLastLogFileName,
                            MetaDataStore::GetLogSegmentLastFileNamePtr()))) {
                        return theRet;
                    }
                    // Cleanup temporary files, if any.
                    if (0 != (theRet = DeleteTmp(mLogDir, ! kDeleteTmpFlag))) {
                        return theRet;
                    }
                    if (0 != (theRet = DeleteTmp(
                            mCheckpointDir, ! kDeleteTmpFlag))) {
                        return theRet;
                    }
                    break;
                default:
                    KFS_LOG_STREAM_DEBUG <<
                        mSyncCommitName <<
                        ": invalid commit step: " << theState <<
                    KFS_LOG_EOM;
                    return -EINVAL;
            }
            theState++;
            if (2 < theState) {
                break;
            }
            if (0 != (theRet = WriteCommitState(theState))) {
                return theRet;
            }
        }
        if (0 != unlink(mSyncCommitName.c_str())) {
            theRet = GetErrno();
            KFS_LOG_STREAM_ERROR <<
                mSyncCommitName << ": " << QCUtils::SysError(theRet) <<
            KFS_LOG_EOM;
            theRet = theRet < 0 ? theRet : -theRet;
        }
        return theRet;
    }
    template<typename FuncT>
    int ListDirEntries(
        const char* inDirNamePtr,
        FuncT&      inFunc)
    {
        DIR* const theDirPtr = opendir(inDirNamePtr);
        if (! theDirPtr) {
            const int theErr = GetErrno();
            KFS_LOG_STREAM_ERROR <<
                "opendir " << inDirNamePtr << ": " <<
                QCUtils::SysError(theErr) <<
            KFS_LOG_EOM;
            return (theErr < 0 ? theErr : (0 != theErr ? -theErr : -EINVAL));
        }
        const size_t         theSuffixLen = mTmpSuffix.size();
        const char* const    theSuffixPtr = mTmpSuffix.data();
        int                  theRet       = 0;
        const struct dirent* thePtr;
        while ((thePtr = readdir(theDirPtr))) {
            const char* const theNamePtr = thePtr->d_name;
            if ('.' == theNamePtr[0] &&
                    (0 == theNamePtr[1] ||
                        ('.' == theNamePtr[1] && 0 == theNamePtr[2]))) {
                continue;
            }
            if (0 == strcmp(kMetaDataSyncCommitPtr, theNamePtr)) {
                continue;
            }
            const size_t theLen = strlen(theNamePtr);
            if (0 != (theRet = inFunc(
                    theNamePtr,
                    theLen,
                    0 < theSuffixLen && theSuffixLen <= theLen &&
                    0 == memcmp(
                        theSuffixPtr,
                        theNamePtr + theLen - theSuffixLen,
                        theSuffixLen)))) {
                break;
            }
        }
        closedir(theDirPtr);
        return theRet;
    }
    class SetPathName
    {
    protected:
        SetPathName(
            const string& inDirName)
            : mName(inDirName.data(), inDirName.size()),
              mPrefixLen(mName.size())
        {
            if (! mName.empty() && '/' != *mName.rbegin()) {
                mName += '/';
                mPrefixLen++;
            }
        }
        const string& SetName(
            const char* inNamePtr,
            size_t      inLength)
        {
            mName.erase(mPrefixLen);
            mName.append(inNamePtr, inLength);
            return mName;
        }
        string mName;
        size_t mPrefixLen;
    private:
        SetPathName(
            const SetPathName&);
        SetPathName& operator=(
            const SetPathName&);
    };
    class RenameTmpFunc : private SetPathName
    {
    public:
        RenameTmpFunc(
            const string& inDirName,
            size_t        inSuffixLen,
            bool          inIgnoreNonTmpFlag)
            : SetPathName(inDirName),
              mIgnoreNonTmpFlag(inIgnoreNonTmpFlag),
              mSuffixLen(inSuffixLen),
              mDestName()
            {}
        int operator()(
            const char* inNamePtr,
            size_t      inLength,
            bool        inTmpSuffixFlag)
        {
            if (! inTmpSuffixFlag && mIgnoreNonTmpFlag) {
                return 0;
            }
            SetName(inNamePtr, inLength);
            if (! inTmpSuffixFlag || inLength <= mSuffixLen) {
                KFS_LOG_STREAM_ERROR <<
                    "unexpected directory entry: " << mName <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            mDestName.assign(mName.data(), mName.size() - mSuffixLen);
            if (rename(mName.c_str(), mDestName.c_str())) {
                const int theErr = GetErrno();
                KFS_LOG_STREAM_ERROR <<
                    "rename: " << mName <<
                    " to "     << mDestName <<
                    ": " << QCUtils::SysError(theErr) <<
                KFS_LOG_EOM;
                return (theErr < 0 ? theErr : -theErr);
            }
            return 0;
        }
    private:
        bool const   mIgnoreNonTmpFlag;
        size_t const mSuffixLen;
        string       mDestName;
    };
    int RenameTmp(
        string& inDirName,
        bool    inIgnoreNonTmpFlag)
    {
        if (inDirName.empty() || mTmpSuffix.empty()) {
            KFS_LOG_STREAM_ERROR <<
                "invalid parameters"
                " direcotry: "  << inDirName <<
                " siffix: "     << mTmpSuffix <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        RenameTmpFunc theFunc(inDirName, mTmpSuffix.size(),
            inIgnoreNonTmpFlag);
        return ListDirEntries(inDirName.c_str(), theFunc);
    }
    class DeleteFunc : private SetPathName
    {
    public:
        DeleteFunc(
            const string& inDirName,
            bool          inDeleteTmpFlag)
            : SetPathName(inDirName),
              mDeleteTmpFlag(inDeleteTmpFlag)
            {}
        bool operator()(
            const char* inNamePtr,
            size_t      inLength,
            bool        inTmpSuffixFlag)
        {
            if ((mDeleteTmpFlag ? inTmpSuffixFlag : ! inTmpSuffixFlag)) {
                SetName(inNamePtr, inLength);
                if (unlink(mName.c_str())) {
                    const int theErr = GetErrno();
                    KFS_LOG_STREAM_ERROR <<
                        "unlink: " << mName <<
                        ": " << QCUtils::SysError(theErr) <<
                    KFS_LOG_EOM;
                    return (theErr < 0 ? theErr : -theErr);
                }
            }
            return 0;
        }
    private:
        const bool mDeleteTmpFlag;
    };
    int DeleteTmp(
        string& inDirName,
        bool    inDeleteTmpFlag)
    {
        if (inDirName.empty() || mTmpSuffix.empty()) {
            KFS_LOG_STREAM_ERROR <<
                "invalid parameters"
                " direcotry: "  << inDirName <<
                " siffix: "     << mTmpSuffix <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        DeleteFunc theFunc(inDirName, inDeleteTmpFlag);
        return ListDirEntries(inDirName.c_str(), theFunc);
    }
    class CounterFunc
    {
    public:
        CounterFunc()
            : mCount(0)
            {}
        int operator()(
            const char* /* inNamePtr */,
            size_t      /* inLength */,
            bool        inTmpSuffixFlag)
        {
            if (! inTmpSuffixFlag) {
                mCount++;
            }
            return 0;
        }
        int Get() const
            { return mCount; }
    private:
        int mCount;
    private:
        CounterFunc(
            const CounterFunc&);
        CounterFunc& operator=(
            const CounterFunc&);
    };
    int CountDirEntries(
        const char* inDirNamePtr)
    {
        CounterFunc theCount;
        const int theStatus = ListDirEntries(inDirNamePtr, theCount);
        if (theStatus < 0) {
            return theStatus;
        }
        return theCount.Get();
    }
    class GetMaxLogSeq
    {
    public:
        GetMaxLogSeq()
            : mPrefixPtr(MetaDataStore::GetLogSegmentFileNamePrefixPtr()),
              mPrefixLen(strlen(mPrefixPtr)),
              mSeq()
            {}
        int operator()(
            const char* inNamePtr,
            size_t      inLength,
            bool        inTmpSuffixFlag)
        {
            if (inTmpSuffixFlag) {
                return 0;
            }
            MetaVrLogSeq theLogSeq;
            seq_t        theSegIdx = -1;
            if (mPrefixLen <= inLength &&
                    0 == memcmp(mPrefixPtr, inNamePtr, mPrefixLen) &&
                    MetaDataStore::GetLogSequenceFromFileName(
                        inNamePtr, inLength, theLogSeq, &theSegIdx)) {
                mSeq = max(mSeq, theLogSeq);
            }
            return 0;
        }
        const MetaVrLogSeq& Get() const
            { return mSeq; }
    private:
        const char* const mPrefixPtr;
        const size_t      mPrefixLen;
        MetaVrLogSeq      mSeq;
    private:
        GetMaxLogSeq(
            const GetMaxLogSeq&);
        GetMaxLogSeq& operator=(
            const GetMaxLogSeq&);
    };
    MetaVrLogSeq GetLastLogSeq()
    {
        GetMaxLogSeq theFunc;
        const int theStatus = ListDirEntries(mLogDir.c_str(), theFunc);
        if (theStatus < 0) {
            return MetaVrLogSeq(0, 0, theStatus);
        }
        return theFunc.Get();
    }
    int64_t GetFsId(
        int64_t inFsId,
        bool&   outEmpytFsFlag)
    {
        string theFileName(mCheckpointDir.data(), mCheckpointDir.size());
        if (! theFileName.empty() && '/' != *theFileName.rbegin()) {
            theFileName += '/';
        }
        outEmpytFsFlag = false;
        theFileName +=  MetaDataStore::GetCheckpointLatestFileNamePtr();
        const int theFd = open(theFileName.c_str(), O_RDONLY);
        if (theFd < 0) {
            const int theErr = GetErrno();
            if (theErr != ENOENT || inFsId < 0) {
                KFS_LOG_STREAM_ERROR <<
                    "open " << theFileName << ": " <<
                    QCUtils::SysError(theErr) <<
                KFS_LOG_EOM;
                return -theErr;
            }
            int theCnt = CountDirEntries(mCheckpointDir.c_str());
            if (theCnt < 0) {
                return theCnt;
            }
            if (0 < theCnt) {
                KFS_LOG_STREAM_ERROR <<
                    "no latest file in non empty checkpoint directory: " <<
                        mCheckpointDir << ": " <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            if ((theCnt = CountDirEntries(mLogDir.c_str())) < 0) {
                return theCnt;
            }
            if (0 < theCnt) {
                KFS_LOG_STREAM_ERROR <<
                    "no latest file while log directory is not empty: " <<
                        mCheckpointDir << ": " <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            outEmpytFsFlag = true;
            return inFsId;
        }
        StBufferT<char, 1> theBuf;
        theBuf.Resize(4 << 10);
        ssize_t theNRd = read(theFd, theBuf.GetPtr(), theBuf.GetSize() - 1);
        int64_t theRet;
        if (theNRd < 0) {
            theRet = GetErrno();
            KFS_LOG_STREAM_ERROR <<
                "read " << theFileName << ": " <<
                QCUtils::SysError(theRet) <<
            KFS_LOG_EOM;
            if (0 < theRet) {
                theRet = -theRet;
            }
        } else {
            theBuf.GetPtr()[theNRd] = 0;
            const char* thePtr = strstr(
                theBuf.GetPtr(), "\nfilesysteminfo/fsid/");
            if (! thePtr) {
                KFS_LOG_STREAM_ERROR <<
                    "no file system id found: " << theFileName << ": " <<
                KFS_LOG_EOM;
                theRet = -EINVAL;
            } else {
                thePtr += 21;
                const char* theEndPtr = strchr(thePtr, '/');
                if (! theEndPtr ||
                        ! DecIntParser::Parse(
                            thePtr, theEndPtr - thePtr, theRet)) {
                    KFS_LOG_STREAM_ERROR <<
                        "file system id parse failure: " <<
                            theFileName << ": " <<
                    KFS_LOG_EOM;
                    theRet = -EINVAL;
                }
            }
        }
        close(theFd);
        return theRet;
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

MetaDataSync::MetaDataSync(
    NetManager& inNetManager)
    : mImpl(*(new Impl(inNetManager)))
    {}

MetaDataSync::~MetaDataSync()
{
    delete &mImpl;
}

    int
MetaDataSync::SetParameters(
    const char*       inParamPrefixPtr,
    const Properties& inParameters,
    int               inMaxReadSize)
{
    return mImpl.SetParameters(inParamPrefixPtr, inParameters, inMaxReadSize);
}

    int
MetaDataSync::Start(
    const char* inCheckpointDirPtr,
    const char* inLogDirPtr,
    bool        inVrNodeIdConfiguredFlag)
{
    return mImpl.Start(
        inCheckpointDirPtr, inLogDirPtr, inVrNodeIdConfiguredFlag);
}

    void
MetaDataSync::StartLogSync(
    const MetaVrLogSeq& inLogSeq)
{
    const bool kAllowNotPrimaryFlag      = false;
    const bool kUseVrPrimarySelectorFlag = true;
    mImpl.StartLogSync(
        inLogSeq, kAllowNotPrimaryFlag, kUseVrPrimarySelectorFlag);
}

    void
MetaDataSync::ScheduleLogSync(
    const MetaDataSync::Servers& inServers,
    const MetaVrLogSeq&          inLogStartSeq,
    const MetaVrLogSeq&          inLogEndSeq,
    bool                         inAllowNotPrimaryFlag)
{
    mImpl.ScheduleLogSync(
        inServers, inLogStartSeq, inLogEndSeq, inAllowNotPrimaryFlag);
}

    int
MetaDataSync::GetLogFetchStatus(
    bool& outProgressFlag)
{
    return mImpl.GetLogFetchStatus(outProgressFlag);
}

    void
MetaDataSync::Shutdown()
{
    mImpl.Shutdown();
}

} // namespace KFS
