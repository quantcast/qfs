//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2009/01/17
// Author: Mike Ovsiannikov
//
// Copyright 2009-2012,2016 Quantcast Corporation. All rights reserved.
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

#include "DiskIo.h"
#include "BufferManager.h"
#include "IOMethod.h"

#include "kfsio/IOBuffer.h"
#include "kfsio/Globals.h"
#include "kfsio/PrngIsaac64.h"
#include "common/Properties.h"
#include "common/MsgLogger.h"
#include "common/kfstypes.h"

#include "qcdio/QCDLList.h"
#include "qcdio/QCMutex.h"
#include "qcdio/qcstutils.h"
#include "qcdio/QCUtils.h"
#include "qcdio/QCIoBufferPool.h"
#include "qcdio/qcdebug.h"

#include <cerrno>
#include <algorithm>
#include <limits>
#include <set>
#include <iomanip>

namespace KFS
{
using std::max;
using std::min;
using std::string;
using std::set;
using std::numeric_limits;
using std::setw;
using std::setfill;

using libkfsio::globalNetManager;
using libkfsio::SetIOBufferAllocator;
using libkfsio::IOBufferAllocator;
using libkfsio::IOBufferVerifier;
using libkfsio::SetIOBufferVerifier;
using libkfsio::globals;

static void DiskIoReportError(
    const char* inMsgPtr,
    int         inSysError = 0);
static void DiskIoReportError(
    const string& inMsg,
    int           inSysError = 0)
{ DiskIoReportError(inMsg.c_str(), inSysError); }

static int DiskQueueToSysError(
    const QCDiskQueue::EnqueueStatus& inStatus)
{
    const int theSysError = inStatus.GetSysError();
    if (theSysError != 0) {
        return theSysError;
    }
    switch (inStatus.GetError())
    {
        case QCDiskQueue::kErrorNone:                 return 0;
        case QCDiskQueue::kErrorRead:                 return EIO;
        case QCDiskQueue::kErrorWrite:                return EIO;
        case QCDiskQueue::kErrorCancel:               return ECANCELED;
        case QCDiskQueue::kErrorSeek:                 return EIO;
        case QCDiskQueue::kErrorEnqueue:              return EAGAIN;
        case QCDiskQueue::kErrorOutOfBuffers:         return ENOMEM;
        case QCDiskQueue::kErrorParameter:            return EINVAL;
        case QCDiskQueue::kErrorQueueStopped:         return EINVAL;
        case QCDiskQueue::kErrorFileIdxOutOfRange:    return EINVAL;
        case QCDiskQueue::kErrorBlockIdxOutOfRange:   return EINVAL;
        case QCDiskQueue::kErrorBlockCountOutOfRange: return EINVAL;
        case QCDiskQueue::kErrorOutOfRequests:        return EAGAIN;
        case QCDiskQueue::kErrorOpen:                 return EIO;
        case QCDiskQueue::kErrorClose:                return EIO;
        case QCDiskQueue::kErrorHasPendingRequests:   return EINVAL;
        case QCDiskQueue::kErrorSpaceAlloc:           return EIO;
        case QCDiskQueue::kErrorDelete:               return EIO;
        case QCDiskQueue::kErrorRename:               return EIO;
        case QCDiskQueue::kErrorGetFsAvailable:       return EIO;
        case QCDiskQueue::kErrorCheckDirReadable:     return EIO;
        case QCDiskQueue::kErrorCheckDirWritable:     return EIO;
        default:                                      break;
    }
    return EINVAL;
}

// Disk error simulator. Used for testing of error handling, including
// "timing holes" and request queues "isolation".
class DiskErrorSimulator : public QCDiskQueue::IoStartObserver
{
public:
    class Config
    {
    public:
        Config(
            const Properties& inConfig)
            : mMinPeriodReq(inConfig.getValue(
                "chunkServer.diskErrorSimulator.minPeriod",      int64_t(0)
              )),
              mMaxPeriodReq(inConfig.getValue(
                "chunkServer.diskErrorSimulator.maxPeriod",      int64_t(16)
              )),
              mMinTimeMicroSec(inConfig.getValue(
                "chunkServer.diskErrorSimulator.minTimeMicroSec", int64_t(0)
              )),
              mMaxTimeMicroSec(inConfig.getValue(
                "chunkServer.diskErrorSimulator.maxTimeMicroSec", int64_t(0)
              )),
              mEnqueueFailInterval(inConfig.getValue(
                "chunkServer.diskErrorSimulator.enqueueFailInterval", int64_t(0)
              )),
              mPrefixes()
        {
            if (! IsEnabled(string())) {
                return;
            }
            const string thePrefs = inConfig.getValue(
                "chunkServer.diskErrorSimulator.chunkDirPrefixes", "");
            for (size_t theNextPos = 0; ;) {
                const size_t theEndPos = thePrefs.find(';', theNextPos);
                const string thePref = thePrefs.substr(
                    theNextPos,
                    theEndPos == string::npos ?
                        theEndPos : theEndPos - theNextPos
                );
                if (! thePref.empty()) {
                    if (mPrefixes.insert(thePref).second) {
                        KFS_LOG_STREAM_INFO <<
                            "disk error simulator: added prefix: " <<
                            thePref <<
                        KFS_LOG_EOM;
                    }
                }
                if (theEndPos == string::npos) {
                    break;
                }
                theNextPos = theEndPos + 1;
            }
            if (mPrefixes.empty()) {
                KFS_LOG_STREAM_INFO <<
                    "disk error simulator: enabled for all prefixes" <<
                KFS_LOG_EOM;
            }
        }
        bool IsEnabled(
            string inPrefix) const
        {
            return ((IsNotifyEnabled() || IsEnqueueEnabled()) &&
                (mPrefixes.empty() ||
                mPrefixes.find(inPrefix) != mPrefixes.end()));
        }
        bool IsNotifyEnabled() const
        {
            return (mMinTimeMicroSec <= mMaxTimeMicroSec && 0 < mMaxTimeMicroSec);
        }
        bool IsEnqueueEnabled() const
        {
            return (0 < mEnqueueFailInterval);
        }
        const int64_t mMinPeriodReq;
        const int64_t mMaxPeriodReq;
        const int64_t mMinTimeMicroSec;
        const int64_t mMaxTimeMicroSec;
        const int64_t mEnqueueFailInterval;
        set<string>   mPrefixes;
    };

    DiskErrorSimulator(
        const Config& inConfig)
        : QCDiskQueue::IoStartObserver(),
          mMutex(),
          mSleepCond(),
          mRandom(),
          mMinPeriodReq(inConfig.mMinPeriodReq),
          mMaxPeriodReq(inConfig.mMaxPeriodReq),
          mMinTimeMicroSec(inConfig.mMinTimeMicroSec),
          mMaxTimeMicroSec(inConfig.mMaxTimeMicroSec),
          mEnqueueFailInterval(inConfig.mEnqueueFailInterval),
          mSleepingFlag(false),
          mReqCount(0),
          mEnqueueReqCount(),
          mRand()
    {
        if (0 < mEnqueueFailInterval) {
            mEnqueueReqCount = Rand(0, mEnqueueFailInterval);
        }
    }
    virtual ~DiskErrorSimulator()
        { DiskErrorSimulator::Shutdown(); }
    void Shutdown()
    {
        QCStMutexLocker theLocker(mMutex);
        mReqCount     = numeric_limits<int>::max();
        mSleepingFlag = false;
        mSleepCond.NotifyAll();
    }
    bool Enqueue()
    {
        if (mEnqueueFailInterval <= 0 || 0 < --mEnqueueReqCount) {
            return true;
        }
        mEnqueueReqCount = Rand(0, mEnqueueFailInterval);
        return false;
    }
    bool IsNotifyEnabled() const
    {
        return (mMinTimeMicroSec <= mMaxTimeMicroSec && 0 < mMaxTimeMicroSec);
    }
    virtual void Notify(
        QCDiskQueue::ReqType   inReqType,
        QCDiskQueue::RequestId inRequestId,
        QCDiskQueue::FileIdx   inFileIdx,
        QCDiskQueue::BlockIdx  inStartBlockIdx,
        int                    inBufferCount)
    {
        // The idea is stall all disk io threads servicing io queue.
        QCStMutexLocker theLocker(mMutex);
        while (mSleepingFlag) {
            mSleepCond.Wait(mMutex);
        }
        if (0 < --mReqCount) {
            return;
        }
        mReqCount = Rand(mMinPeriodReq, mMaxPeriodReq);
        const int64_t theSleepMicroSec =
            Rand(mMinTimeMicroSec, mMaxTimeMicroSec);
        KFS_LOG_STREAM_INFO <<
            "disk error simulator:"
            " request: type: " << inReqType <<
            " id: "            << inRequestId <<
            " file: "          << inFileIdx <<
            " block: "         << inStartBlockIdx <<
            " count: "         << inBufferCount <<
            " sleeping for "   << theSleepMicroSec * 1e-6 << " sec,"
            " next after "     << mReqCount << " requests" <<
        KFS_LOG_EOM;
        if (theSleepMicroSec > 0) {
            mSleepingFlag = true;
            mSleepCond.Wait(mMutex, QCCondVar::Time(theSleepMicroSec) * 1000);
            mSleepingFlag = false;
            mSleepCond.NotifyAll(); // Wakeup all other threads if sleeping.
        }
    }
private:
    QCMutex       mMutex;
    QCCondVar     mSleepCond;
    PrngIsaac64   mRandom;
    const int64_t mMinPeriodReq;
    const int64_t mMaxPeriodReq;
    const int64_t mMinTimeMicroSec;
    const int64_t mMaxTimeMicroSec;
    const int64_t mEnqueueFailInterval;
    bool          mSleepingFlag;
    int64_t       mReqCount;
    int64_t       mEnqueueReqCount;
    PrngIsaac64   mRand;

    int64_t Rand(
        int64_t inFrom,
        int64_t inTo)
    {
        if (inFrom >= inTo) {
            return inTo;
        }
        const int64_t theInterval = inTo - inFrom;
        return (inFrom + (int64_t)(mRandom.Rand() % theInterval));
    }
private:
    DiskErrorSimulator(
        const DiskErrorSimulator&);
    DiskErrorSimulator operator=(
        const DiskErrorSimulator&);
};

const char* const kDiskQueueParametersPrefixPtr = "chunkServer.diskQueue.";
// Disk io queue.
class DiskQueue : public QCDiskQueue,
    private QCDiskQueue::DebugTracer
{
public:
    typedef QCDLList<DiskQueue, 0>        DiskQueueList;
    typedef DiskIo::DeviceId              DeviceId;
    typedef QCDiskQueue::RequestProcessor RequestProcessor;

    DiskQueue(
        DiskQueue**                       inListPtr,
        DeviceId                          inDeviceId,
        const char*                       inFileNamePrefixPtr,
        int64_t                           inMaxBuffersBytes,
        int64_t                           inMaxClientQuota,
        int                               inWaitingAvgInterval,
        const DiskErrorSimulator::Config* inSimulatorConfigPtr,
        int                               inMinWriteBlkSize,
        int64_t                           inMaxFileSize,
        bool                              inBufferDataIgnoreOverwriteFlag,
        int                               inBufferDataTailToKeepSize,
        int                               inThreadCount,
        IOMethod**                        inIoMethodsPtr)
        : QCDiskQueue(),
          QCDiskQueue::DebugTracer(),
          mFileNamePrefixes(inFileNamePrefixPtr ? inFileNamePrefixPtr : ""),
          mDeviceId(inDeviceId),
          mDeleteNullFilePtr(new DiskIo::File()),
          mRenameNullFilePtr(new DiskIo::File()),
          mGetFsSpaceAvailableNullFilePtr(new DiskIo::File()),
          mCheckDirReadableNullFilePtr(new DiskIo::File()),
          mCheckDirWritableNullFilePtr(new DiskIo::File()),
          mBufferManager(true),
          mSimulatorPtr(inSimulatorConfigPtr ?
            new DiskErrorSimulator(*inSimulatorConfigPtr) : 0),
          mMinWriteBlkSize(inMinWriteBlkSize),
          mMaxFileSize(inMaxFileSize),
          mBufferDataIgnoreOverwriteFlag(inBufferDataIgnoreOverwriteFlag),
          mBufferDataTailToKeepSize(max(0, inBufferDataTailToKeepSize)),
          mThreadCount(inThreadCount),
          mIoMethodsPtr(inIoMethodsPtr),
          mRequestProcessorsPtr(
            inIoMethodsPtr ? new RequestProcessor*[inThreadCount]: 0),
          mCanEnforceIoTimeoutFlag(false),
          mMaxReadRequests(0),
          mMaxRequests(0),
          mMaxPengingReadBytes(0),
          mMaxPendingBytes(0),
          mReadReqCount(0),
          mWriteReqCount(0),
          mReadBytesCount(0),
          mWriteBytesCount(0),
          mOverloadedFlag(false)
    {
        mFileNamePrefixes.append(1, (char)0);
        DiskQueueList::Init(*this);
        DiskQueueList::PushBack(inListPtr, *this);
        mDeleteNullFilePtr->mQueuePtr              = this;
        mRenameNullFilePtr->mQueuePtr              = this;
        mGetFsSpaceAvailableNullFilePtr->mQueuePtr = this;
        mCheckDirReadableNullFilePtr->mQueuePtr    = this;
        mCheckDirWritableNullFilePtr->mQueuePtr    = this;
        mBufferManager.Init(0, inMaxBuffersBytes, inMaxClientQuota, 0);
        mBufferManager.SetWaitingAvgInterval(inWaitingAvgInterval);
    }
    void SetParameters(
        const Properties& inProperties)
    {
        if (! mIoMethodsPtr) {
            return;
        }
        for (int i = 0; i < mThreadCount; i++) {
            mIoMethodsPtr[i]->SetParameters(
                kDiskQueueParametersPrefixPtr,
                inProperties
            );
        }
    }
    void Delete(
        DiskQueue** inListPtr)
    {
        DiskQueueList::Remove(inListPtr, *this);
        delete this;
    }
    int Start(
        int             inMaxQueueDepth,
        int             inMaxBuffersPerRequestCount,
        int             inFileCount,
        const char**    inFileNamesPtr,
        QCIoBufferPool& inBufferPool,
        CpuAffinity     inCpuAffinity,
        bool            inTraceFlag,
        bool            inCreateExclusiveFlag,
        bool            inRequestAffinityFlag,
        bool            inSerializeMetaRequestsFlag)
    {
        mCanEnforceIoTimeoutFlag = false;
        if (mIoMethodsPtr) {
            for (int i = 0; i < mThreadCount; i++) {
                if (! mIoMethodsPtr[i]->Init(
                        *this,
                        inBufferPool.GetBufferSize(),
                        mMinWriteBlkSize,
                        mMaxFileSize,
                        mCanEnforceIoTimeoutFlag)) {
                    return EIO;
                }
                mRequestProcessorsPtr[i] = mIoMethodsPtr[i];
            }
        }
        const bool kBufferedIoFlag = false;
        // Reserve 1/8 for "internal" / chunk header IOs.
        mMaxRequests     = (int)(int64_t(inMaxQueueDepth) * 7 / 8);
        mMaxPendingBytes = int64_t(mMaxRequests) *
            inMaxBuffersPerRequestCount * inBufferPool.GetBufferSize();
        // Do not allow reads to completely overtake writes.
        if (mRequestProcessorsPtr) {
            mMaxReadRequests     = mMaxRequests     * 4 / 5;
            mMaxPengingReadBytes = mMaxPendingBytes * 4 / 5;
        } else {
            mMaxReadRequests     = mMaxRequests     * 6 / 7;
            mMaxPengingReadBytes = mMaxPendingBytes * 6 / 7;
        }
        return QCDiskQueue::Start(
            mThreadCount,
            inMaxQueueDepth,
            inMaxBuffersPerRequestCount,
            inFileCount,
            inFileNamesPtr,
            inBufferPool,
            (mSimulatorPtr && mSimulatorPtr->IsNotifyEnabled()) ?
                mSimulatorPtr : 0,
            inCpuAffinity,
            inTraceFlag ? this : 0,
            kBufferedIoFlag,
            inCreateExclusiveFlag,
            inRequestAffinityFlag,
            inSerializeMetaRequestsFlag,
            mRequestProcessorsPtr
        );
    }
    EnqueueStatus DeleteFile(
        const char*   inFileNamePtr,
        IoCompletion* inIoCompletionPtr,
        Time          inTimeWaitNanoSec)
    {
        return QCDiskQueue::Delete(
            inFileNamePtr, inIoCompletionPtr, inTimeWaitNanoSec);
    }
    bool IsFileNamePrefixMatches(
        const char* inFileNamePtr) const
    {
        const char* const theFileNamePtr = inFileNamePtr ? inFileNamePtr : "";
        const char*       thePtr         = theFileNamePtr;
        const char*       thePrefPtr     = mFileNamePrefixes.data();
        const char* const thePrefsEndPtr = thePrefPtr +
            mFileNamePrefixes.length();
        while (thePrefPtr < thePrefsEndPtr) {
            while (*thePtr && *thePrefPtr && *thePtr == *thePrefPtr) {
                thePtr++;
                thePrefPtr++;
            }
            if (*thePrefPtr == 0) {
                return true;
            }
            while (*thePrefPtr++)
                {}
            thePtr = theFileNamePtr;
        }
        return false;
    }
    DeviceId GetDeviceId() const
        { return mDeviceId; }
    DeviceId SetDeviceId(
        DeviceId inDeviceId)
        { return mDeviceId = inDeviceId; }
    void AddFileNamePrefix(
        const char* inFileNamePtr)
    {
        mFileNamePrefixes.append(inFileNamePtr ? inFileNamePtr : "");
        mFileNamePrefixes.append(1, (char)0);
    }
    EnqueueStatus Read(
        FileIdx        inFileIdx,
        BlockIdx       inStartBlockIdx,
        InputIterator* inBufferIteratorPtr,
        int            inBufferCount,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec)
    {
        if (! mSimulatorPtr || mSimulatorPtr->Enqueue()) {
            return QCDiskQueue::Read(
                inFileIdx,
                inStartBlockIdx,
                inBufferIteratorPtr,
                inBufferCount,
                inIoCompletionPtr,
                inTimeWaitNanoSec);
        }
        return EnqueueStatus(kRequestIdNone, kErrorOutOfRequests);
    }
    EnqueueStatus Write(
        FileIdx        inFileIdx,
        BlockIdx       inStartBlockIdx,
        InputIterator* inBufferIteratorPtr,
        int            inBufferCount,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec,
        bool           inSyncFlag,
        int64_t        inEofHint)
    {
        if (! mSimulatorPtr || mSimulatorPtr->Enqueue()) {
            return QCDiskQueue::Write(
                inFileIdx,
                inStartBlockIdx,
                inBufferIteratorPtr,
                inBufferCount,
                inIoCompletionPtr,
                inTimeWaitNanoSec,
                inSyncFlag,
                inEofHint
            );
        }
        return EnqueueStatus(kRequestIdNone, kErrorOutOfRequests);
    }
    bool RemoveFileNamePrefix(
        const char* inPrefixPtr)
    {
        if (! inPrefixPtr || ! *inPrefixPtr) {
            return false;
        }
        const char*       thePtr         = inPrefixPtr;
        const char*       thePrefPtr     = mFileNamePrefixes.data();
        const char* const thePrefsEndPtr = thePrefPtr +
            mFileNamePrefixes.length();
        while (thePrefPtr < thePrefsEndPtr) {
            while (*thePtr && *thePrefPtr && *thePtr == *thePrefPtr) {
                thePtr++;
                thePrefPtr++;
            }
            if (*thePrefPtr == 0 && *thePtr == 0) {
                const size_t theLen = thePtr - inPrefixPtr;
                mFileNamePrefixes.erase(
                    thePrefPtr - mFileNamePrefixes.data() - theLen, theLen + 1);
                return true;
            }
            while (*thePrefPtr++)
                {}
            thePtr = inPrefixPtr;
        }
        return false;
    }
    bool IsInUse() const
        { return (! mFileNamePrefixes.empty()); }
    DiskIo::FilePtr GetDeleteNullFile()
        { return mDeleteNullFilePtr; };
    DiskIo::FilePtr GetRenameNullFile()
        { return mRenameNullFilePtr; };
    DiskIo::FilePtr GetGetFsSpaceAvailableNullFile()
        { return mGetFsSpaceAvailableNullFilePtr; };
    DiskIo::FilePtr GetCheckDirReadableNullFile()
        { return mCheckDirReadableNullFilePtr; };
    DiskIo::FilePtr GetCheckDirWritableNullFile()
        { return mCheckDirWritableNullFilePtr; }
    virtual void TraceMsg(
        const char* inMsgPtr,
        int         inLength)
    {
        KFS_LOG_STREAM_START(MsgLogger::kLogLevelDEBUG, theLogStream);
            ostream& theStream = theLogStream.GetStream();
            theStream << "QCDQ[" << setfill('0') << setw(2) << mDeviceId << "]";
            theStream.write(inMsgPtr, inLength);
        KFS_LOG_STREAM_END;
    }
    void Stop()
    {
        if (mSimulatorPtr) {
            // Make sure that io threads can proceed.
            mSimulatorPtr->Shutdown();
        }
        QCDiskQueue::Stop();
    }
    BufferManager& GetBufferManager()
        { return mBufferManager; }
    int GetMinWriteBlkSize() const
        { return mMinWriteBlkSize; }
    bool GetBufferDataIgnoreOverwriteFlag() const
        { return mBufferDataIgnoreOverwriteFlag; }
    int GetBufferDataTailToKeepSize() const
        { return mBufferDataTailToKeepSize; }
    bool CanEnforceIoTimeout() const
        { return mCanEnforceIoTimeoutFlag; }
    static DiskIo::IoBuffers& GetIoBuffers(
        DiskIo::File& inFile)
        { return inFile.mIoBuffers; }
    static void SetError(
        DiskIo::File& inFile,
        int           inError)
        { inFile.mError = inError; }
    inline void ReadPending(
        int64_t inReqBytes,
        ssize_t inRetCode,
        bool    inDontUpdateTotalsFlag);
    inline void WritePending(
        int64_t inReqBytes,
        int64_t inRetCode,
        bool    inDontUpdateTotalsFlag);
private:
    string                    mFileNamePrefixes;
    DeviceId                  mDeviceId;
    DiskIo::FilePtr           mDeleteNullFilePtr; // Pseudo files.
    DiskIo::FilePtr           mRenameNullFilePtr;
    DiskIo::FilePtr           mGetFsSpaceAvailableNullFilePtr;
    DiskIo::FilePtr           mCheckDirReadableNullFilePtr;
    DiskIo::FilePtr           mCheckDirWritableNullFilePtr;
    BufferManager             mBufferManager;
    DiskErrorSimulator* const mSimulatorPtr;
    int                 const mMinWriteBlkSize;
    int64_t             const mMaxFileSize;
    bool                const mBufferDataIgnoreOverwriteFlag;
    int                 const mBufferDataTailToKeepSize;
    int                 const mThreadCount;
    IOMethod**          const mIoMethodsPtr;
    RequestProcessor**  const mRequestProcessorsPtr;
    bool                      mCanEnforceIoTimeoutFlag;
    int                       mMaxReadRequests;
    int                       mMaxRequests;
    int64_t                   mMaxPengingReadBytes;
    int64_t                   mMaxPendingBytes;
    int                       mReadReqCount;
    int                       mWriteReqCount;
    int64_t                   mReadBytesCount;
    int64_t                   mWriteBytesCount;
    bool                      mOverloadedFlag;
    DiskQueue*                mPrevPtr[1];
    DiskQueue*                mNextPtr[1];

    ~DiskQueue()
    {
        DiskQueue::Stop();
        mDeleteNullFilePtr->mQueuePtr              = 0;
        mRenameNullFilePtr->mQueuePtr              = 0;
        mGetFsSpaceAvailableNullFilePtr->mQueuePtr = 0;
        mCheckDirReadableNullFilePtr->mQueuePtr    = 0;
        delete mSimulatorPtr;
        if (mIoMethodsPtr) {
            for (int i = 0; i < mThreadCount; i++) {
                delete mIoMethodsPtr[i];
                mIoMethodsPtr[i] = 0;
            }
        }
        delete [] mIoMethodsPtr;
        delete [] mRequestProcessorsPtr;
    }
    void UpdateOverloaded()
    {
        const bool theOverloadedFlag =
            mMaxReadRequests     <= mReadReqCount ||
            mMaxPengingReadBytes <= mReadBytesCount ||
            mMaxRequests         <= mReadReqCount + mWriteReqCount ||
            mMaxPendingBytes     <= mReadBytesCount + mWriteBytesCount;
        if (theOverloadedFlag == mOverloadedFlag) {
            return;
        }
        KFS_LOG_STREAM_DEBUG <<
            mFileNamePrefixes.c_str() <<
            " dev: " << mDeviceId <<
            (mOverloadedFlag ? " clear" : " set") <<
            " overloaded"
            " pending:"
            " reads: "  << mReadReqCount <<
            " bytes: "  << mReadReqCount <<
            " writes: " << mWriteReqCount <<
            " bytes: "  << mWriteBytesCount <<
            " max:"
            " reads: "  << mMaxReadRequests <<
            " bytes: "  << mMaxPengingReadBytes <<
            " total: "  << mMaxRequests <<
            " bytes: "  << mMaxPendingBytes <<
        KFS_LOG_EOM;
        mOverloadedFlag = theOverloadedFlag;
        mBufferManager.SetDiskOverloaded(mOverloadedFlag);
        if (! mOverloadedFlag) {
            // Schedule to run buffer manager's wait queue.
            globalNetManager().Wakeup();
        }
    }
    friend class QCDLListOp<DiskQueue, 0>;
private:
    DiskQueue(
        const DiskQueue& inQueue);
    DiskQueue& operator=(
        const DiskQueue& inQueue);
};

// Disk io globals, including io completion queue, accounting, and
// configuration.
class DiskIoQueues : private ITimeout
{
private:
    typedef DiskQueue::DiskQueueList DiskQueueList;

public:
    enum { kDiskQueueIdNone = -1 };

    typedef QCDLList<DiskIo, 0> IoQueue;
    typedef DiskIo::Counters Counters;

    DiskIoQueues(
            const Properties& inConfig)
        : ITimeout(),
          mDiskQueueThreadCount(inConfig.getValue(
            "chunkServer.diskQueue.threadCount", 2)),
          mDiskQueueMaxBuffersPerRequest(inConfig.getValue(
            "chunkServer.diskQueue.maxBuffersPerRequest", 1 << 8)),
          mDiskQueueMaxEnqueueWaitNanoSec(inConfig.getValue(
            "chunkServer.diskQueue.maxEnqueueWaitTimeMilliSec", 0) * 1000000),
          mBufferPoolPartitionCount(inConfig.getValue(
            "chunkServer.ioBufferPool.partitionCount", 1)),
          mBufferPoolPartitionBufferCount(inConfig.getValue(
            "chunkServer.ioBufferPool.partitionBufferCount",
                (sizeof(size_t) < 8 ? 64 : 192) << 10)),
          mBufferPoolBufferSize(inConfig.getValue(
            "chunkServer.ioBufferPool.bufferSize", 4 << 10)),
          mBufferPoolLockMemoryFlag(inConfig.getValue(
            "chunkServer.ioBufferPool.lockMemory", 0) != 0),
          mDiskQueueMaxQueueDepth(max(8, inConfig.getValue(
            "chunkServer.diskQueue.maxDepth",
                max(4 << 10, (int)(int64_t(mBufferPoolPartitionCount) *
                mBufferPoolPartitionBufferCount) /
                    max(1, mDiskQueueMaxBuffersPerRequest))))),
          mDiskOverloadedPendingRequestCount(inConfig.getValue(
            "chunkServer.diskIo.overloadedPendingRequestCount",
                mDiskQueueMaxQueueDepth * 3 / 4)),
          mDiskClearOverloadedPendingRequestCount(inConfig.getValue(
            "chunkServer.diskIo.clearOverloadedPendingRequestCount",
                mDiskOverloadedPendingRequestCount * 3 / 4)),
          mDiskOverloadedMinFreeBufferCount(inConfig.getValue(
            "chunkServer.diskIo.overloadedMinFreeBufferCount",
                int(int64_t(mBufferPoolPartitionCount) *
                    mBufferPoolPartitionBufferCount / 16))),
          mDiskClearOverloadedMinFreeBufferCount(inConfig.getValue(
            "chunkServer.diskIo.overloadedClearMinFreeBufferCount",
                mDiskOverloadedMinFreeBufferCount * 2 / 3)),
          mDiskOverloadedPendingWriteByteCount(inConfig.getValue(
            "chunkServer.diskIo.overloadedPendingWriteByteCount",
                int64_t(mBufferPoolPartitionBufferCount) *
                mBufferPoolBufferSize * mBufferPoolPartitionCount / 4)),
          mDiskClearOverloadedPendingWriteByteCount(inConfig.getValue(
            "chunkServer.diskIo.clearOverloadedPendingWriteByteCount",
            mDiskOverloadedPendingWriteByteCount * 2 / 3)),
          mCrashOnErrorFlag(inConfig.getValue(
            "chunkServer.diskIo.crashOnError", 0) != 0),
          mDebugVerifyIoBuffersFlag(inConfig.getValue(
            "chunkServer.diskIo.debugVerifyIoBuffers", 0) != 0),
          mDebugPinIoBuffersFlag(inConfig.getValue(
            "chunkServer.diskIo.debugPinIoBuffers", 0) != 0),
          mBufferManagerMaxRatio(inConfig.getValue(
            "chunkServer.bufferManager.maxRatio", 0.4)),
          mDiskBufferManagerMaxRatio(inConfig.getValue(
            "chunkServer.disk.bufferManager.maxRatio",
                mBufferManagerMaxRatio * .35)),
          mMaxClientQuota(inConfig.getValue(
            "chunkServer.bufferManager.maxClientQuota",
            int64_t(CHUNKSIZE + (4 << 20)))),
          mMaxIoTime(inConfig.getValue(
            "chunkServer.diskIo.maxIoTimeSec", 4 * 60 + 30)),
          mOverloadedFlag(false),
          mValidateIoBuffersFlag(false),
          mMaxRequestSize(0),
          mNextIoTimeout(Now()),
          mWriteCancelWaiterPtr(0),
          mReadPendingBytes(0),
          mWritePendingBytes(0),
          mReadReqCount(0),
          mWriteReqCount(0),
          mMutex(),
          mPutCond(),
          mBufferAllocator(),
          mBufferManager(inConfig.getValue(
            "chunkServer.bufferManager.enabled", 1) != 0),
          mNullCallback(),
          mBufferredWriteNullCallback(),
          mNullBufferDataPtr(0),
          mNullBufferDataWrittenPtr(0),
          mCounters(),
          mDiskErrorSimulatorConfig(inConfig),
          mCpuAffinity(inConfig.getValue(
            "chunkServer.diskQueue.cpuAffinity", -1)),
          mDiskQueueTraceFlag(inConfig.getValue(
            "chunkServer.diskQueue.trace", 0) != 0),
          mParameters(inConfig)
    {
        mCounters.Clear();
        IoQueue::Init(mIoInFlightQueuePtr);
        IoQueue::Init(mIoInFlightNoTimeoutQueuePtr);
        IoQueue::Init(mIoDoneQueuePtr);
        DiskQueueList::Init(mDiskQueuesPtr);
        // Call Timeout() every time NetManager goes trough its work loop.
        ITimeout::SetTimeoutInterval(0);
    }
    ~DiskIoQueues()
    {
        DiskIoQueues::Shutdown(0, false);
        globalNetManager().UnRegisterTimeoutHandler(this);
        if (mDebugVerifyIoBuffersFlag) {
            SetIOBufferVerifier(0);
        }
        delete mNullBufferDataPtr;
        delete mNullBufferDataWrittenPtr;
    }
    bool Start(
        string* inErrMessagePtr)
    {
        int theSysError = GetBufferPool().Create(
            mBufferPoolPartitionCount,
            mBufferPoolPartitionBufferCount,
            mBufferPoolBufferSize,
            mBufferPoolLockMemoryFlag
        );
        if (theSysError) {
            if (inErrMessagePtr) {
                *inErrMessagePtr = QCUtils::SysError(theSysError);
            }
        } else {
            if (mDebugVerifyIoBuffersFlag) {
                SetIOBufferVerifier(&mBufferAllocator);
            }
            if (! SetIOBufferAllocator(&mBufferAllocator)) {
                DiskIoReportError("failed to set buffer allocator");
                if (inErrMessagePtr) {
                    *inErrMessagePtr = "failed to set buffer allocator";
                    theSysError = -1;
                }
            } else {
                // Make sure that allocator works, and it isn't possible to
                // change it:
                IOBufferData theAllocatorTest;
                if (! mNullBufferDataPtr) {
                    mNullBufferDataPtr = new IOBufferData(0, 0, 0, 0);
                }
                if (! mNullBufferDataWrittenPtr) {
                    mNullBufferDataWrittenPtr = new IOBufferData(1);
                    mNullBufferDataWrittenPtr->Consume(
                        mNullBufferDataWrittenPtr->Fill(
                            mNullBufferDataWrittenPtr->BytesConsumable()));
                }
            }
        }
        if (theSysError) {
            GetBufferPool().Destroy();
        } else {
            int64_t const theMaxReqSize =
                int64_t(mBufferAllocator.GetBufferSize()) *
                mDiskQueueMaxBuffersPerRequest * mDiskQueueMaxQueueDepth / 2;
            if (theMaxReqSize > 0 &&
                    int64_t(mMaxRequestSize = size_t(theMaxReqSize)) <
                    theMaxReqSize) {
                mMaxRequestSize = numeric_limits<size_t>::max();
            }
            globalNetManager().RegisterTimeoutHandler(this);
            mBufferManager.Init(
                &GetBufferPool(),
                int64_t(mBufferManagerMaxRatio * mBufferAllocator.GetBufferSize() *
                    mBufferPoolPartitionCount * mBufferPoolPartitionBufferCount),
                mMaxClientQuota,
                mDiskOverloadedPendingWriteByteCount /
                    mBufferAllocator.GetBufferSize()
            );
        }
        return (! theSysError);
    }
    bool Shutdown(
        string* inErrMsgPtr,
        bool    inRunIoCompletionFlag)
    {
        DiskQueueList::Iterator theIt(mDiskQueuesPtr);
        DiskQueue* thePtr;
        while ((thePtr = theIt.Next())) {
            thePtr->Stop();
        }
        QCRTASSERT(IoQueue::IsEmpty(mIoInFlightQueuePtr));
        QCRTASSERT(IoQueue::IsEmpty(mIoInFlightNoTimeoutQueuePtr));
        delete mWriteCancelWaiterPtr;
        mWriteCancelWaiterPtr = 0;
        mMaxRequestSize = 0;
        if (inRunIoCompletionFlag && ! IoQueue::IsEmpty(mIoDoneQueuePtr)) {
            RunCompletion();
        }
        if (IoQueue::IsEmpty(mIoDoneQueuePtr)) {
            while (! DiskQueueList::IsEmpty(mDiskQueuesPtr)) {
                DiskQueueList::Front(mDiskQueuesPtr)->Delete(mDiskQueuesPtr);
            }
            globalNetManager().UnRegisterTimeoutHandler(this);
            return true;
        }
        DiskIoReportError("io completion queue is not empty");
        if (inErrMsgPtr) {
            *inErrMsgPtr = "io completion queue is not empty: "
                "call RunIoCompletion()";
        }
        return false;
    }
    bool RunCompletion()
    {
        bool    theRet = false;
        DiskIo* thePtr;
        while ((thePtr = Get())) {
            thePtr->RunCompletion();
            theRet = true;
        }
        return theRet;
    }
    void Put(
        DiskIo&                inIo,
        QCDiskQueue::RequestId inRequestId,
        QCDiskQueue::Error     inCompletionCode)
    {
        {
            QCStMutexLocker theLocker(mMutex);
            RemoveInFlight(inIo);
            IoQueue::PushBack(mIoDoneQueuePtr, inIo);
            inIo.mCompletionRequestId = inRequestId;
            inIo.mCompletionCode      = inCompletionCode;
            mPutCond.Notify();
        }
        globalNetManager().Wakeup();
    }
    DiskIo* Get()
    {
        QCStMutexLocker theLocker(mMutex);
        return IoQueue::PopFront(mIoDoneQueuePtr);
    }
    bool CancelOrExpire(
        DiskIo& inIo,
        bool    inExpireFlag)
    {
        if (! QCDiskQueue::IsValidRequestId(inIo.mRequestId)) {
            return false;
        }
        DiskQueue* const theQueuePtr = inIo.mFilePtr->GetDiskQueuePtr();
        QCRTASSERT(theQueuePtr);
        QCDiskQueue::IoCompletion* theComplPtr = 0;
        if (inIo.mReadLength <= 0 && ! inIo.mIoBuffers.empty()) {
            // Hold on to the write buffers, while waiting for write to
            // complete.
            if (! inExpireFlag) {
                theQueuePtr->WritePending(-int64_t(inIo.mIoBuffers.size() *
                    GetBufferAllocator().GetBufferSize()), 0, inIo.mCachedFlag);
            }
            if (! mWriteCancelWaiterPtr) {
                mWriteCancelWaiterPtr = new WriteCancelWaiter();
            }
            mWriteCancelWaiterPtr->mIoBuffers = inIo.mIoBuffers;
            theComplPtr = theQueuePtr->CancelOrSetCompletionIfInFlight(
                inIo.mRequestId,  mWriteCancelWaiterPtr);
            if (theComplPtr == mWriteCancelWaiterPtr) {
                mWriteCancelWaiterPtr = 0;
                theComplPtr = 0;
            } else {
                mWriteCancelWaiterPtr->mIoBuffers.clear();
            }
        } else {
            if (inIo.mReadLength > 0 && ! inExpireFlag) {
                theQueuePtr->ReadPending(
                    -int64_t(inIo.mReadLength), 0, inIo.mCachedFlag);
            }
            // When read completes it can just discard buffers.
            // Sync doesn't have any buffers attached.
            theComplPtr = theQueuePtr->CancelOrSetCompletionIfInFlight(
                inIo.mRequestId, 0);
        }
        QCStMutexLocker theLocker(mMutex);
        if (theComplPtr == &inIo) {
            while (inIo.mCompletionRequestId != inIo.mRequestId) {
                mPutCond.Wait(mMutex);
            }
        }
        if (inIo.mCompletionRequestId == inIo.mRequestId) {
            QCASSERT(IoQueue::IsInList(mIoDoneQueuePtr, inIo));
            if (! inExpireFlag) {
                IoQueue::Remove(mIoDoneQueuePtr, inIo);
            } else if (inIo.mCompletionCode == QCDiskQueue::kErrorCancel) {
                inIo.mIoRetCode = -ETIMEDOUT;
            }
        } else {
            QCASSERT(IsInFlight(inIo));
            RemoveInFlight(inIo);
            if (inExpireFlag) {
                inIo.mCompletionRequestId = inIo.mRequestId;
                inIo.mCompletionCode      = QCDiskQueue::kErrorCancel;
                inIo.mIoRetCode           = -ETIMEDOUT;
                IoQueue::PushBack(mIoDoneQueuePtr, inIo);
            }
        }
        if (! inExpireFlag) {
            inIo.mRequestId = QCDiskQueue::kRequestIdNone;
        }
        return true;
    }
    bool Cancel(
        DiskIo& inIo)
        { return CancelOrExpire(inIo, false); }
    bool Expire(
        DiskIo& inIo)
        { return CancelOrExpire(inIo, true); }
    DiskQueue* FindDiskQueue(
        const char* inFileNamePtr)
    {
        DiskQueueList::Iterator theItr(mDiskQueuesPtr);
        DiskQueue* thePtr;
        while ((thePtr = theItr.Next()) &&
                ! thePtr->IsFileNamePrefixMatches(inFileNamePtr))
            {}
        return thePtr;
    }
    DiskQueue* FindDiskQueue(
        DiskIo::DeviceId inDeviceId)
    {
        DiskQueueList::Iterator theItr(mDiskQueuesPtr);
        DiskQueue* thePtr;
        while ((thePtr = theItr.Next()) && thePtr->GetDeviceId() != inDeviceId)
            {}
        return thePtr;
    }
    DiskQueue* FindDiskQueueNotInUse()
    {
        DiskQueueList::Iterator theItr(mDiskQueuesPtr);
        DiskQueue* thePtr;
        while ((thePtr = theItr.Next()) && thePtr->IsInUse())
            {}
        return thePtr;
    }
    bool AddDiskQueue(
        const char*      inDirNamePtr,
        DiskIo::DeviceId inDeviceId,
        int              inMaxOpenFiles,
        string*          inErrMessagePtr,
        int              inMinWriteBlkSize,
        bool             inBufferDataIgnoreOverwriteFlag,
        int              inBufferDataTailToKeepSize,
        bool             inCreateExclusiveFlag,
        bool             inRequestAffinityFlag,
        bool             inSerializeMetaRequestsFlag,
        int              inThreadCount,
        int64_t          inMaxFileSize,
        bool             inCanUseIoMethodFlag)
    {
        DiskQueue* theQueuePtr = FindDiskQueue(inDirNamePtr);
        if (theQueuePtr) {
            return (inDeviceId == theQueuePtr->GetDeviceId());
        }
        if ((theQueuePtr = FindDiskQueue(inDeviceId))) {
            theQueuePtr->AddFileNamePrefix(inDirNamePtr);
            return true;
        }
        theQueuePtr = FindDiskQueueNotInUse();
        if (theQueuePtr) {
            theQueuePtr->AddFileNamePrefix(inDirNamePtr);
            theQueuePtr->SetDeviceId(inDeviceId);
            return true;
        }
        int theMinWriteBlkSize = inMinWriteBlkSize;
        if (theMinWriteBlkSize < 0 ||
                (size_t)theMinWriteBlkSize <=
                    mBufferAllocator.GetBufferSize()) {
            theMinWriteBlkSize = 0;
        } else {
            if (theMinWriteBlkSize % mBufferAllocator.GetBufferSize() != 0) {
                const char* const theErrMsgPtr = "invalid min write size";
                DiskIoReportError(theErrMsgPtr);
                if (inErrMessagePtr) {
                    *inErrMessagePtr = theErrMsgPtr;
                }
                return false;
            }
        }
        const int theThreadCount    = 0 < inThreadCount ?
            inThreadCount : mDiskQueueThreadCount;
        IOMethod**  theIoMethodsPtr = 0;
        const char* kLogPrefixPtr   = 0;
        for (int i = inCanUseIoMethodFlag ? 0 : theThreadCount;
                i < theThreadCount;
                i++) {
            IOMethod* const thePtr = IOMethod::Create(
                inDirNamePtr,
                kLogPrefixPtr,
                kDiskQueueParametersPrefixPtr,
                mParameters
            );
            if (0 == i) {
                if (! thePtr) {
                    break;
                }
                theIoMethodsPtr = new IOMethod*[theThreadCount];
            }
            if (! thePtr) {
                while (0 <= --i) {
                    delete theIoMethodsPtr[i];
                }
                delete [] theIoMethodsPtr;
                DiskIoReportError("IO method create failure");
                return false;
            }
            theIoMethodsPtr[i] = thePtr;
        }
        theQueuePtr = new DiskQueue(
            mDiskQueuesPtr,
            inDeviceId,
            inDirNamePtr,
            (int64_t)(
                mDiskBufferManagerMaxRatio * mBufferAllocator.GetBufferSize() *
                mBufferPoolPartitionCount * mBufferPoolPartitionBufferCount),
            mMaxClientQuota,
            mBufferManager.GetWaitingAvgInterval(),
            mDiskErrorSimulatorConfig.IsEnabled(inDirNamePtr) ?
                &mDiskErrorSimulatorConfig : 0,
            theMinWriteBlkSize,
            inMaxFileSize,
            inBufferDataIgnoreOverwriteFlag,
            inBufferDataTailToKeepSize,
            theThreadCount,
            theIoMethodsPtr
        );
        const int theSysErr = theQueuePtr->Start(
            mDiskQueueMaxQueueDepth,
            mDiskQueueMaxBuffersPerRequest,
            inMaxOpenFiles,
            0, // FileNamesPtr
            GetBufferPool(),
            mCpuAffinity,
            mDiskQueueTraceFlag,
            inCreateExclusiveFlag,
            inRequestAffinityFlag || 0 != theIoMethodsPtr,
            inSerializeMetaRequestsFlag
        );
        if (theSysErr) {
            theQueuePtr->Delete(mDiskQueuesPtr);
            const string theErrMsg = QCUtils::SysError(theSysErr);
            DiskIoReportError("failed to start queue" + theErrMsg, theSysErr);
            if (inErrMessagePtr) {
                *inErrMessagePtr = theErrMsg;
            }
            return false;
        }
        return true;
    }
    DiskQueue::Time GetMaxEnqueueWaitTimeNanoSec() const
        { return mDiskQueueMaxEnqueueWaitNanoSec; }
    IOBufferAllocator& GetBufferAllocator()
        { return mBufferAllocator; }
    BufferManager& GetBufferManager()
        { return mBufferManager; }
    void ReportError(
        const char* inMsgPtr,
        int         inErr)
    {
        if (mCrashOnErrorFlag && globalNetManager().IsRunning()) {
            FatalError(inMsgPtr, inErr);
        }
    }
    size_t GetMaxRequestSize() const
        { return mMaxRequestSize; }
    void ReadPending(
        int64_t inReqBytes,
        ssize_t inRetCode,
        bool    inDontUpdateTotalsFlag)
    {
        if (inReqBytes == 0) {
            return;
        }
        if (inReqBytes < 0 && ! inDontUpdateTotalsFlag) {
            mCounters.mReadCount++;
            if (inRetCode >= 0) {
                mCounters.mReadByteCount += inRetCode;
            } else {
                mCounters.mReadErrorCount++;
            }
        }
        mReadPendingBytes += inReqBytes;
        mReadReqCount += inReqBytes > 0 ? 1 : -1;
        QCASSERT(mReadPendingBytes >= 0 && mReadReqCount >= 0);
        CheckIfOverloaded();
    }
    void WritePending(
        int64_t inReqBytes,
        int64_t inRetCode,
        bool    inDontUpdateTotalsFlag)
    {
        if (inReqBytes == 0) {
            return;
        }
        if (inReqBytes < 0 && ! inDontUpdateTotalsFlag) {
            mCounters.mWriteCount++;
            if (inRetCode >= 0) {
                mCounters.mWriteByteCount += inRetCode;
            } else {
                mCounters.mWriteErrorCount++;
            }
        }
        mWritePendingBytes += inReqBytes;
        mWriteReqCount += inReqBytes > 0 ? 1 : -1;
        QCASSERT(mWritePendingBytes >= 0 && mWriteReqCount >= 0);
        CheckIfOverloaded();
    }
    void SyncDone(
        int64_t inRetCode)
    {
        if (inRetCode >= 0) {
            mCounters.mSyncCount++;
        } else {
            mCounters.mSyncErrorCount++;
        }
    }
    void CheckOpenStatusDone(
        int64_t inRetCode)
    {
        if (inRetCode >= 0) {
            mCounters.mCheckOpenCount++;
        } else {
            mCounters.mCheckOpenErrorCount++;
        }
    }
    void DeleteDone(
        int64_t inRetCode)
    {
        if (inRetCode >= 0) {
            mCounters.mDeleteCount++;
        } else {
            mCounters.mDeleteErrorCount++;
        }
    }
    void RenameDone(
        int64_t inRetCode)
    {
        if (inRetCode >= 0) {
            mCounters.mRenameCount++;
        } else {
            mCounters.mRenameErrorCount++;
        }
    }
    void GetGetFsSpaceAvailableDone(
        int64_t inRetCode)
    {
        if (inRetCode >= 0) {
            mCounters.mGetFsSpaceAvailableCount++;
        } else {
            mCounters.mGetFsSpaceAvailableErrorCount++;
        }
    }
    void CheckDirReadableDone(
        int64_t inRetCode)
    {
        if (inRetCode >= 0) {
            mCounters.mCheckDirReadableCount++;
        } else {
            mCounters.mCheckDirReadableErrorCount++;
        }
    }
    void CheckDirWritableDone(
        int64_t inRetCode)
    {
        if (inRetCode >= 0) {
            mCounters.mCheckDirWritableCount++;
        } else {
            mCounters.mCheckDirWritableErrorCount++;
        }
    }
    int GetFdCountPerFile() const
        { return mDiskQueueThreadCount; }
    void GetCounters(
        Counters& outCounters)
        { outCounters = mCounters; }
    void SetInFlight(
        DiskIo* inIoPtr)
    {
        if (! inIoPtr) {
            return;
        }
        Pin(*inIoPtr);
        inIoPtr->mEnqueueTime = Now();
        QCStMutexLocker theLocker(mMutex);
        AddInFlight(*inIoPtr);
    }
    void ResetInFlight(
        DiskIo* inIoPtr)
    {
        if (! inIoPtr) {
            return;
        }
        Unpin(*inIoPtr);
        QCStMutexLocker theLocker(mMutex);
        RemoveInFlight(*inIoPtr);
    }
    KfsCallbackObj* GetNullCallbackPtr()
        { return &mNullCallback; }
    KfsCallbackObj* GetBufferredWriteNullCallbackPtr()
        { return &mBufferredWriteNullCallback; }
    const IOBufferData& GetNullBufferData() const
        { return *mNullBufferDataPtr; }
    const IOBufferData& GetNullBufferDataWritten() const
        { return *mNullBufferDataWrittenPtr; }
    bool IsBufferPosWritten(
        const IOBufferData& inData) const
        { return (inData.Consumer() == mNullBufferDataWrittenPtr->Consumer()); }
    static time_t Now()
        { return globalNetManager().Now(); }
    void UpdateOpenFilesCount(
        int inDelta)
    {
        mCounters.mOpenFilesCount += inDelta;
    }
    void SetParameters(
        const Properties& inProperties)
    {
        const int theCurAvgIntervalSecs =
            mBufferManager.GetWaitingAvgInterval();
        const int theAvgIntervalSecs    = inProperties.getValue(
            "chunkServer.bufferManager.waitingAvgInterval",
            theCurAvgIntervalSecs);
        if (theCurAvgIntervalSecs != theAvgIntervalSecs) {
            mBufferManager.SetWaitingAvgInterval(theAvgIntervalSecs);
            DiskQueueList::Iterator theIt(mDiskQueuesPtr);
            DiskQueue* thePtr;
            while ((thePtr = theIt.Next())) {
                thePtr->GetBufferManager().SetWaitingAvgInterval(
                    theAvgIntervalSecs);
            }
        }
        mMaxIoTime = max(1, inProperties.getValue(
            "chunkServer.diskIo.maxIoTimeSec", mMaxIoTime));
        mValidateIoBuffersFlag = inProperties.getValue(
            "chunkServer.diskIo.debugValidateIoBuffers",
            mValidateIoBuffersFlag ? 1 : 0) != 0;
        mParameters = inProperties;
        DiskQueue* thePtr;
        DiskQueueList::Iterator theIt(mDiskQueuesPtr);
        while ((thePtr = theIt.Next())) {
            thePtr->SetParameters(mParameters);
        }
    }
    int GetMaxIoTimeSec() const
        { return mMaxIoTime; }
    void ValidateIoBuffer(
        const char* inBufferPtr)
    {
        if (! mValidateIoBuffersFlag || GetBufferPool().IsValid(inBufferPtr)) {
            return;
        }
        FatalError("invalid buffer", EINVAL);
    }
    void ValidateIoBuffers(
        const char*              inBufferPtr,
        const DiskIo::IoBuffers& inBuffers)
    {
        if (! mValidateIoBuffersFlag || GetBufferPool().IsValid(inBufferPtr)) {
            return;
        }
        IoBuffers::const_iterator theIt;
        for (theIt = inBuffers.begin(); inBuffers.end() != theIt; ++theIt) {
            if (theIt->Consumer() == inBufferPtr) {
                break;
            }
        }
        if (inBuffers.end() == theIt) {
            return;
        }
        FatalError("invalid buffer", EINVAL);
    }
    void ValidateIoBuffers(
        const DiskIo::IoBuffers& inBuffers)
    {
        if (! mValidateIoBuffersFlag) {
            return;
        }
        for (IoBuffers::const_iterator theIt = inBuffers.begin();
                inBuffers.end() != theIt;
                ++theIt) {
            ValidateIoBuffer(theIt->Consumer());
        }
    }
    void Pin(
        const DiskIo& inIo)
        { SetPinned(inIo, true); }
    void Unpin(
        const DiskIo& inIo)
        { SetPinned(inIo, false); }
    static void FatalError(
        const char* inMsgPtr,
        int         inError)
    {
        KFS_LOG_STREAM_FATAL <<
            (inMsgPtr ? inMsgPtr : "unspecified error") <<
            " " << inError <<
        KFS_LOG_EOM;
        MsgLogger::Stop();
        QCUtils::FatalError(inMsgPtr, inError);
    }

private:
    typedef DiskIo::IoBuffers IoBuffers;
    class WriteCancelWaiter : public QCDiskQueue::IoCompletion
    {
    public:
        WriteCancelWaiter()
            : QCDiskQueue::IoCompletion(),
              mIoBuffers()
            {}
        virtual bool Done(
            QCDiskQueue::RequestId      /* inRequestId */,
            QCDiskQueue::FileIdx        /* inFileIdx */,
            QCDiskQueue::BlockIdx       /* inStartBlockIdx */,
            QCDiskQueue::InputIterator& /* inBufferItr */,
            int                         /* inBufferCount */,
            QCDiskQueue::Error          /* inCompletionCode */,
            int                         /* inSysErrorCode */,
            int64_t                     /* inIoByteCount */)
        {
            delete this; // This might release buffers.
            return true; // Tell the caller not to release buffers.
        }
        IoBuffers mIoBuffers;
    };
    class BufferAllocator :
        public IOBufferAllocator,
        public IOBufferVerifier
    {
    public:
        enum
        {
            kPinnedIdQueuedWrite = QCIoBufferPool::kPinnedBufferIdMin,
            kPinnedIdQueuedRead  = kPinnedIdQueuedWrite + 1,
            kPinnedIdRead        = kPinnedIdQueuedRead  + 1
        };
        BufferAllocator()
            : IOBufferAllocator(),
              IOBufferVerifier(),
              mBufferPool()
            {}
        virtual size_t GetBufferSize() const
            { return mBufferPool.GetBufferSize(); }
        virtual char* Allocate()
        {
            char* const theBufPtr = mBufferPool.Get();
            if (! theBufPtr) {
                FatalError("out of io buffers", 0);
            }
            return theBufPtr;
        }
        virtual void Deallocate(
            char* inBufferPtr)
            { mBufferPool.Put(inBufferPtr); }
        QCIoBufferPool& GetBufferPool()
            { return mBufferPool; }
        virtual void Verify(
            const IOBuffer& inBuffer,
            bool            /* inModifiedFlag */)
        {
            for (IOBuffer::iterator theIt = inBuffer.begin();
                    inBuffer.end() != theIt;
                    ++theIt) {
                bool theFoundFlag = false;
                if (! mBufferPool.IsValid(
                            theIt->GetBufferPtr(), theFoundFlag) &&
                        theFoundFlag) {
                    KFS_LOG_STREAM_FATAL <<
                        "invalid IO buffer: " << theIt->GetBufferPtr() <<
                    KFS_LOG_EOM;
                    FatalError("invalid IO buffer", 0);
                }
            }
        }
        virtual void DoRead(
            const char* inPtr,
            bool        inStartFlag)
            { SetPinnedSelf( inPtr, kPinnedIdRead, inStartFlag); }
        void SetPinned(
            const char* inPtr,
            bool        inReadFlag,
            bool        inFlag)
        {
            SetPinnedSelf(
                inPtr,
                inReadFlag ? kPinnedIdQueuedRead : kPinnedIdQueuedWrite,
                inFlag
            );
        }
    private:
        QCIoBufferPool mBufferPool;

        void SetPinnedSelf(
            const char*                    inPtr,
            QCIoBufferPool::PinnedBufferId inId,
            bool                           inFlag)
        {
            if (! mBufferPool.SetPinned(inPtr, inId, inFlag)) {
                KFS_LOG_STREAM_FATAL <<
                    "pin: " << inFlag <<
                    " id: " << inId <<
                    " invalid IO buffer: " <<
                        reinterpret_cast<const void*>(inPtr) <<
                KFS_LOG_EOM;
                FatalError("set pinned: invalid IO buffer", 0);
            }
        }
    private:
        BufferAllocator(
            const BufferAllocator& inAllocator);
        BufferAllocator& operator=(
            const BufferAllocator& inAllocator);
    };
    class NullCallback : public KfsCallbackObj
    {
    public:
        NullCallback()
            : KfsCallbackObj()
            { SET_HANDLER(this, &NullCallback::Done); }
        int Done(int /* inCode */, void* /* inDataPtr */)
            { return 0; }
    };

    const int                      mDiskQueueThreadCount;
    const int                      mDiskQueueMaxBuffersPerRequest;
    const DiskQueue::Time          mDiskQueueMaxEnqueueWaitNanoSec;
    const int                      mBufferPoolPartitionCount;
    const int                      mBufferPoolPartitionBufferCount;
    const int                      mBufferPoolBufferSize;
    const int                      mBufferPoolLockMemoryFlag;
    const int                      mDiskQueueMaxQueueDepth;
    const int                      mDiskOverloadedPendingRequestCount;
    const int                      mDiskClearOverloadedPendingRequestCount;
    const int                      mDiskOverloadedMinFreeBufferCount;
    const int                      mDiskClearOverloadedMinFreeBufferCount;
    const int64_t                  mDiskOverloadedPendingWriteByteCount;
    const int64_t                  mDiskClearOverloadedPendingWriteByteCount;
    const bool                     mCrashOnErrorFlag;
    const bool                     mDebugVerifyIoBuffersFlag;
    const bool                     mDebugPinIoBuffersFlag;
    const double                   mBufferManagerMaxRatio;
    const double                   mDiskBufferManagerMaxRatio;
    const BufferManager::ByteCount mMaxClientQuota;
    int                            mMaxIoTime;
    bool                           mOverloadedFlag;
    bool                           mValidateIoBuffersFlag;
    size_t                         mMaxRequestSize;
    time_t                         mNextIoTimeout;
    WriteCancelWaiter*             mWriteCancelWaiterPtr;
    int64_t                        mReadPendingBytes;
    int64_t                        mWritePendingBytes;
    int                            mReadReqCount;
    int                            mWriteReqCount;
    QCMutex                        mMutex;
    QCCondVar                      mPutCond;
    BufferAllocator                mBufferAllocator;
    BufferManager                  mBufferManager;
    NullCallback                   mNullCallback;
    NullCallback                   mBufferredWriteNullCallback;
    IOBufferData*                  mNullBufferDataPtr;
    IOBufferData*                  mNullBufferDataWrittenPtr;
    DiskIo*                        mIoInFlightQueuePtr[1];
    DiskIo*                        mIoInFlightNoTimeoutQueuePtr[1];
    DiskIo*                        mIoDoneQueuePtr[1];
    DiskQueue*                     mDiskQueuesPtr[1];
    Counters                       mCounters;
    DiskErrorSimulator::Config     mDiskErrorSimulatorConfig;
    const QCDiskQueue::CpuAffinity mCpuAffinity;
    const int                      mDiskQueueTraceFlag;
    Properties                     mParameters;

    QCIoBufferPool& GetBufferPool()
        { return mBufferAllocator.GetBufferPool(); }

    DiskIo** GetInFlightQueue(
        const DiskIo& inIo)
    {
        return (inIo.mFilePtr->GetDiskQueuePtr()->CanEnforceIoTimeout() ?
            mIoInFlightNoTimeoutQueuePtr : mIoInFlightQueuePtr);
    }
    void AddInFlight(
        DiskIo& inIo)
        { IoQueue::PushBack(GetInFlightQueue(inIo), inIo); }
    void RemoveInFlight(
        DiskIo& inIo)
        { IoQueue::Remove(GetInFlightQueue(inIo), inIo); }
    bool IsInFlight(
        DiskIo& inIo)
        { return IoQueue::IsInList(GetInFlightQueue(inIo), inIo); }
    DiskIo* GetTimedOut(
            time_t inMinTime)
    {
        QCStMutexLocker theLocker(mMutex);
        DiskIo* thePtr = IoQueue::Front(mIoInFlightQueuePtr);
        if (thePtr &&
                thePtr->mCompletionRequestId == QCDiskQueue::kRequestIdNone &&
                inMinTime <= thePtr->mEnqueueTime) {
            thePtr = 0;
        }
        return thePtr;
    }
    virtual void Timeout() // ITimeout
    {
        const time_t theNow = Now();
        const int theMaxTimerOverrun = max(16, mMaxIoTime / 8);
        if (theNow > mNextIoTimeout + theMaxTimerOverrun) {
            // Reschedule
            mNextIoTimeout = theNow + max(2, theMaxTimerOverrun / 8);
        }
        if (theNow >= mNextIoTimeout) {
            // Timeout io requests.
            const time_t theMinTime = theNow - mMaxIoTime;
            DiskIo* thePtr;
            while ((thePtr = GetTimedOut(theMinTime)) && Expire(*thePtr)) {
                KFS_LOG_STREAM_ERROR <<
                    "io request " << thePtr->mRequestId <<
                    " timed out; wait: " << (theNow - thePtr->mEnqueueTime) <<
                    " sec" <<
                KFS_LOG_EOM;
                mCounters.mTimedOutErrorCount++;
                mCounters.mTimedOutErrorReadByteCount +=
                    max(size_t(0), thePtr->mReadLength);
                mCounters.mTimedOutErrorWriteByteCount +=
                    thePtr->mIoBuffers.size() * GetBufferPool().GetBufferSize();
            }
            mNextIoTimeout = theNow + 1 + mMaxIoTime / 8;
        }
        RunCompletion();
    }
    void CheckIfOverloaded()
    {
        const int theReqCount = mReadReqCount + mWriteReqCount;
        SetOverloaded(mOverloadedFlag ? // Hysteresis
            mWritePendingBytes > mDiskClearOverloadedPendingWriteByteCount ||
            theReqCount        > mDiskClearOverloadedPendingRequestCount   ||
            (mWritePendingBytes > 0 &&
                mBufferAllocator.GetBufferPool().GetFreeBufferCount() <
                mDiskClearOverloadedMinFreeBufferCount
            )
        :
            mWritePendingBytes > mDiskOverloadedPendingWriteByteCount ||
            theReqCount        > mDiskOverloadedPendingRequestCount   ||
            (mWritePendingBytes > 0 &&
                mBufferAllocator.GetBufferPool().GetFreeBufferCount() <
                mDiskOverloadedMinFreeBufferCount
            )
        );
    }
    void SetOverloaded(
        bool inFlag)
    {
        if (mOverloadedFlag == inFlag) {
            return;
        }
        mOverloadedFlag = inFlag;
        KFS_LOG_STREAM_INFO <<
            (mOverloadedFlag ? "setting" : "clearing") <<
            " disk overloaded state: pending"
            " read: "  << mReadReqCount <<
            " bytes: " << mReadPendingBytes <<
            " write: " << mWriteReqCount <<
            " bytes: " << mWritePendingBytes <<
        KFS_LOG_EOM;
        mBufferManager.SetDiskOverloaded(inFlag);
        if (! inFlag) {
            // Schedule to run buffer manager's wait queue.
            globalNetManager().Wakeup();
        }
    }
    void SetPinned(
        const DiskIo& inIo,
        bool          inFlag)
    {
        if (inIo.mCachedFlag ||
                (! mDebugPinIoBuffersFlag && ! mDebugVerifyIoBuffersFlag)) {
            return;
        }
        const bool theReadFlag = 0 < inIo.mReadLength;
        for (DiskIo::IoBuffers::const_iterator theIt = inIo.mIoBuffers.begin();
                inIo.mIoBuffers.end() != theIt;
                ++theIt) {
            mBufferAllocator.SetPinned(
                theIt->GetBufferPtr(), theReadFlag, inFlag);
        }
    }
};

static DiskIoQueues* sDiskIoQueuesPtr = 0;

    inline void
DiskQueue::ReadPending(
    int64_t inReqBytes,
    ssize_t inRetCode,
    bool    inDontUpdateTotalsFlag)
{
    if (0 != inReqBytes && ! inDontUpdateTotalsFlag) {
        mReadReqCount   += 0 < inReqBytes ? 1 : -1;
        mReadBytesCount += inReqBytes;
        UpdateOverloaded();
    }
    sDiskIoQueuesPtr->ReadPending(
        inReqBytes, inRetCode, inDontUpdateTotalsFlag);
}

    inline void
DiskQueue::WritePending(
    int64_t inReqBytes,
    int64_t inRetCode,
    bool    inDontUpdateTotalsFlag)
{
    if (0 != inReqBytes && ! inDontUpdateTotalsFlag) {
        mWriteReqCount   += 0 < inReqBytes ? 1 : -1;
        mWriteBytesCount += inReqBytes;
        UpdateOverloaded();
    }
    sDiskIoQueuesPtr->WritePending(
        inReqBytes, inRetCode, inDontUpdateTotalsFlag);
}

    /* static */ bool
DiskIo::Init(
    const Properties& inProperties,
    string*           inErrMessagePtr /* = 0 */)
{
    if (sDiskIoQueuesPtr) {
        *inErrMessagePtr = "already initialized";
        return false;
    }
    sDiskIoQueuesPtr = new DiskIoQueues(inProperties);
    if (! sDiskIoQueuesPtr->Start(inErrMessagePtr)) {
        delete sDiskIoQueuesPtr;
        sDiskIoQueuesPtr = 0;
        return false;
    }
    return (sDiskIoQueuesPtr != 0);
}

static void DiskIoReportError(
    const char* inMsgPtr,
    int         inErr)
{
    if (sDiskIoQueuesPtr) {
        sDiskIoQueuesPtr->ReportError(inMsgPtr, inErr);
    }
}

    /* static */ bool
DiskIo::StartIoQueue(
    const char*      inDirNamePtr,
    DiskIo::DeviceId inDeviceId,
    int              inMaxOpenFiles,
    string*          inErrMessagePtr                 /* = 0 */,
    int              inMinWriteBlkSize               /* = 0 */,
    bool             inBufferDataIgnoreOverwriteFlag /* = false */,
    int              inBufferDataTailToKeepSize      /* = 0 */,
    bool             inCreateExclusiveFlag           /* = true */,
    bool             inRequestAffinityFlag           /* = false */,
    bool             inSerializeMetaRequestsFlag     /* = true */,
    int              inThreadCount                   /* = -1 */,
    int64_t          inMaxFileSize                   /* = -1 */,
    bool             inCanUseIoMethodFlag            /* false */)
{
    if (! sDiskIoQueuesPtr) {
        if (inErrMessagePtr) {
            *inErrMessagePtr = "not initialized";
        }
        return false;
    }
    return sDiskIoQueuesPtr->AddDiskQueue(
        inDirNamePtr,
        inDeviceId,
        inMaxOpenFiles,
        inErrMessagePtr,
        inMinWriteBlkSize,
        inBufferDataIgnoreOverwriteFlag,
        inBufferDataTailToKeepSize,
        inCreateExclusiveFlag,
        inRequestAffinityFlag,
        inSerializeMetaRequestsFlag,
        inThreadCount,
        inMaxFileSize,
        inCanUseIoMethodFlag
    );
}

    /* static */ bool
DiskIo::StopIoQueue(
    DiskQueue*       inDiskQueuePtr,
    const char*      inDirNamePtr,
    DiskIo::DeviceId inDeviceId,
    string*          inErrMessagePtr /* = 0 */)
{
    if (! inDiskQueuePtr) {
        if (inErrMessagePtr) {
            *inErrMessagePtr = "disk queue parameter is null";
        }
        return false;
    }
    if (! sDiskIoQueuesPtr) {
        if (inErrMessagePtr) {
            *inErrMessagePtr = "not initialized";
        }
        return false;
    }
    if (inDiskQueuePtr->GetDeviceId() != inDeviceId) {
        if (inErrMessagePtr) {
            *inErrMessagePtr = "device id mismatch";
        }
        return false;
    }
    if (! inDiskQueuePtr->RemoveFileNamePrefix(inDirNamePtr)) {
        if (inErrMessagePtr) {
            *inErrMessagePtr = "no such prefix";
        }
        return false;
    }
    return true;
}

    /* static */ bool
DiskIo::Shutdown(
    string* inErrMessagePtr /* = 0 */)
{
    if (! sDiskIoQueuesPtr) {
        return true;
    }
    const bool kRunIoCompletionFlag = true;
    const bool theOkFlag = sDiskIoQueuesPtr->Shutdown(
        inErrMessagePtr, kRunIoCompletionFlag);
    delete sDiskIoQueuesPtr;
    sDiskIoQueuesPtr = 0;
    return theOkFlag;
}

    /* static */ int
DiskIo::GetFdCountPerFile()
{
    return (sDiskIoQueuesPtr ? sDiskIoQueuesPtr->GetFdCountPerFile() : -1);
}

    /* static */ bool
DiskIo::RunIoCompletion()
{
    return (sDiskIoQueuesPtr && sDiskIoQueuesPtr->RunCompletion());
}

    /* static */ size_t
DiskIo::GetMaxRequestSize()
{
    return (sDiskIoQueuesPtr ? sDiskIoQueuesPtr->GetMaxRequestSize() : 0);
}

    /* static */ BufferManager&
DiskIo::GetBufferManager()
{
    QCRTASSERT(sDiskIoQueuesPtr);
    return (sDiskIoQueuesPtr->GetBufferManager());
}

    /* static */ BufferManager*
DiskIo::GetDiskBufferManager(
    DiskQueue* inDiskQueuePtr)
{
    return ((inDiskQueuePtr && sDiskIoQueuesPtr) ?
        &inDiskQueuePtr->GetBufferManager() : 0
    );
}

    /* static */ void
DiskIo::GetCounters(
    Counters& outCounters)
{
    if (! sDiskIoQueuesPtr) {
        outCounters.Clear();
        return;
    }
    sDiskIoQueuesPtr->GetCounters(outCounters);
}

     /* static */ bool
DiskIo::Delete(
    const char*     inFileNamePtr,
    KfsCallbackObj* inCallbackObjPtr /* = 0 */,
    string*         inErrMessagePtr /* = 0 */)
{
    return EnqueueMeta(
        kMetaOpTypeDelete,
        inFileNamePtr,
        0,
        inCallbackObjPtr,
        inErrMessagePtr
    );
}

     /* static */ bool
DiskIo::Rename(
    const char*     inSrcFileNamePtr,
    const char*     inDstFileNamePtr,
    KfsCallbackObj* inCallbackObjPtr /* = 0 */,
    string*         inErrMessagePtr /* = 0 */)
{
    return EnqueueMeta(
        kMetaOpTypeRename,
        inSrcFileNamePtr,
        inDstFileNamePtr,
        inCallbackObjPtr,
        inErrMessagePtr
    );
}

     /* static */ bool
DiskIo::GetFsSpaceAvailable(
    const char*     inPathNamePtr,
    KfsCallbackObj* inCallbackObjPtr /* = 0 */,
    string*         inErrMessagePtr /* = 0 */)
{
    return EnqueueMeta(
        kMetaOpTypeGetFsSpaceAvailable,
        inPathNamePtr,
        0,
        inCallbackObjPtr,
        inErrMessagePtr
    );
}

     /* static */ bool
DiskIo::CheckDirReadable(
    const char*     inDirNamePtr,
    KfsCallbackObj* inCallbackObjPtr /* = 0 */,
    string*         inErrMessagePtr /* = 0 */)
{
    return EnqueueMeta(
        kMetaOpTypeCheckDirReadable,
        inDirNamePtr,
        0,
        inCallbackObjPtr,
        inErrMessagePtr
    );
}

     /* static */ bool
DiskIo::CheckDirWritable(
    const char*     inTestFileNamePtr,
    bool            inBufferedIoFlag,
    bool            inAllocSpaceFlag,
    int64_t         inWriteSize,
    KfsCallbackObj* inCallbackObjPtr /* = 0 */,
    string*         inErrMessagePtr /* = 0 */)
{
    return EnqueueMeta(
        kMetaOpTypeCheckDirWritable,
        inTestFileNamePtr,
        0,
        inCallbackObjPtr,
        inErrMessagePtr,
        inBufferedIoFlag,
        inAllocSpaceFlag,
        inWriteSize
    );
}

    /* static */ bool
DiskIo::GetDiskQueuePendingCount(
    DiskQueue* inDiskQueuePtr,
    int&       outFreeRequestCount,
    int&       outRequestCount,
    int64_t&   outReadBlockCount,
    int64_t&   outWriteBlockCount,
    int&       outBlockSize)
{
    if (! inDiskQueuePtr) {
        outFreeRequestCount = 0;
        outRequestCount     = 0;
        outReadBlockCount   = 0;
        outWriteBlockCount  = 0;
        outBlockSize        = 0;
        return false;
    }
    inDiskQueuePtr->GetPendingCount(
        outFreeRequestCount,
        outRequestCount,
        outReadBlockCount,
        outWriteBlockCount);
    outBlockSize = inDiskQueuePtr->GetBlockSize();
    return true;
}

    /* static */ DiskQueue*
DiskIo::FindDiskQueue(
        const char* inDirNamePtr)
{
    return (sDiskIoQueuesPtr ?
        sDiskIoQueuesPtr->FindDiskQueue(inDirNamePtr) : 0);
}

    /* static */ void
DiskIo::SetParameters(
    const Properties& inProperties)
{
    if (sDiskIoQueuesPtr) {
        sDiskIoQueuesPtr->SetParameters(inProperties);
    }
}

    /* static */ int
DiskIo::GetMaxIoTimeSec()
{
    if (sDiskIoQueuesPtr) {
        return sDiskIoQueuesPtr->GetMaxIoTimeSec();
    }
    return -1;
}

     /* static */ bool
DiskIo::EnqueueMeta(
    DiskIo::MetaOpType inOpType,
    const char*        inNamePtr,
    const char*        inNextNamePtr,
    KfsCallbackObj*    inCallbackObjPtr,
    string*            inErrMessagePtr,
    bool               inBufferedIoFlag,
    bool               inAllocSpaceFlag,
    int64_t            inWriteSize)
{
    const char* theErrMsgPtr = 0;
    if (! inNamePtr) {
        theErrMsgPtr = "file or directory name is null";
    } else if (inOpType == kMetaOpTypeRename && ! inNextNamePtr) {
        theErrMsgPtr = "destination file name is null";
    } else if (! sDiskIoQueuesPtr) {
        theErrMsgPtr = "disk queues are not initialized";
    } else {
        DiskQueue* const      theQueuePtr    =
            sDiskIoQueuesPtr->FindDiskQueue(inNamePtr);
        KfsCallbackObj* const theCallbackPtr = inCallbackObjPtr ?
            inCallbackObjPtr : sDiskIoQueuesPtr->GetNullCallbackPtr();
        if (theQueuePtr) {
            DiskIo*                  theDiskIoPtr = 0;
            DiskQueue::EnqueueStatus theStatus;
            switch (inOpType) {
                case kMetaOpTypeRename:
                    theDiskIoPtr = new DiskIo(
                        theQueuePtr->GetRenameNullFile(),
                        theCallbackPtr
                    );
                    sDiskIoQueuesPtr->SetInFlight(theDiskIoPtr);
                    theStatus = theQueuePtr->Rename(
                        inNamePtr,
                        inNextNamePtr,
                        theDiskIoPtr,
                        sDiskIoQueuesPtr->GetMaxEnqueueWaitTimeNanoSec()
                    );
                    // Completion handler needed to count errors.
                    // For now assume that the request completed successfully if
                    // no completion handler specified.
                    if (theStatus.IsError()) {
                        sDiskIoQueuesPtr->RenameDone(-1);
                    }
                    break;
                case kMetaOpTypeDelete:
                    theDiskIoPtr = new DiskIo(
                        theQueuePtr->GetDeleteNullFile(),
                        theCallbackPtr
                    );
                    sDiskIoQueuesPtr->SetInFlight(theDiskIoPtr);
                    theStatus = theQueuePtr->DeleteFile(
                        inNamePtr,
                        theDiskIoPtr,
                        sDiskIoQueuesPtr->GetMaxEnqueueWaitTimeNanoSec()
                    );
                    if (theStatus.IsError()) {
                        sDiskIoQueuesPtr->DeleteDone(-1);
                    }
                    break;
                case kMetaOpTypeGetFsSpaceAvailable:
                    theDiskIoPtr = new DiskIo(
                        theQueuePtr->GetGetFsSpaceAvailableNullFile(),
                        theCallbackPtr
                    );
                    sDiskIoQueuesPtr->SetInFlight(theDiskIoPtr);
                    theStatus = theQueuePtr->GetFsSpaceAvailable(
                        inNamePtr,
                        theDiskIoPtr,
                        sDiskIoQueuesPtr->GetMaxEnqueueWaitTimeNanoSec()
                    );
                    if (theStatus.IsError()) {
                        sDiskIoQueuesPtr->GetGetFsSpaceAvailableDone(-1);
                    }
                    break;
                case kMetaOpTypeCheckDirReadable:
                    theDiskIoPtr = new DiskIo(
                        theQueuePtr->GetCheckDirReadableNullFile(),
                        theCallbackPtr
                    );
                    sDiskIoQueuesPtr->SetInFlight(theDiskIoPtr);
                    theStatus = theQueuePtr->CheckDirReadable(
                        inNamePtr,
                        theDiskIoPtr,
                        sDiskIoQueuesPtr->GetMaxEnqueueWaitTimeNanoSec()
                    );
                    if (theStatus.IsError()) {
                        sDiskIoQueuesPtr->CheckDirReadableDone(-1);
                    }
                    break;
                case kMetaOpTypeCheckDirWritable:
                    theDiskIoPtr = new DiskIo(
                        theQueuePtr->GetCheckDirWritableNullFile(),
                        theCallbackPtr
                    );
                    sDiskIoQueuesPtr->SetInFlight(theDiskIoPtr);
                    theStatus = theQueuePtr->CheckDirWritable(
                        inNamePtr,
                        inBufferedIoFlag,
                        inAllocSpaceFlag,
                        inWriteSize,
                        theDiskIoPtr,
                        sDiskIoQueuesPtr->GetMaxEnqueueWaitTimeNanoSec()
                    );
                    if (theStatus.IsError()) {
                        sDiskIoQueuesPtr->CheckDirWritableDone(-1);
                    }
                    break;
                default:
                    QCRTASSERT(! "invalid op type");
            }
            if (theStatus.IsGood()) {
                if (theDiskIoPtr) {
                    theDiskIoPtr->mRequestId = theStatus.GetRequestId();
                    QCRTASSERT(
                        theDiskIoPtr->mRequestId != QCDiskQueue::kRequestIdNone
                    );
                }
                return true;
            }
            sDiskIoQueuesPtr->ResetInFlight(theDiskIoPtr);
            delete theDiskIoPtr;
            if (inErrMessagePtr) {
                *inErrMessagePtr = QCDiskQueue::ToString(theStatus.GetError());
            }
            DiskIoReportError(QCDiskQueue::ToString(theStatus.GetError()), 0);
            return false;
        } else {
            theErrMsgPtr = "failed to find disk queue";
        }
    }
    if (theErrMsgPtr) {
        if (inErrMessagePtr) {
            *inErrMessagePtr = theErrMsgPtr;
        }
        DiskIoReportError(theErrMsgPtr, EINVAL);
    }
    return false;
}

    /* static */ int
DiskIo::GetMinWriteBlkSize(
    const DiskQueue* inDiskQueuePtr)
{
    return (inDiskQueuePtr ? inDiskQueuePtr->GetMinWriteBlkSize() : 0);
}

    bool
DiskIo::File::Open(
    const char*    inFileNamePtr,
    DiskIo::Offset inMaxFileSize          /* = -1 */,
    bool           inReadOnlyFlag         /* = false */,
    bool           inReserveFileSpaceFlag /* = false */,
    bool           inCreateFlag           /* = false */,
    string*        inErrMessagePtr        /* = 0 */,
    bool*          inRetryFlagPtr         /* = 0 */,
    bool           inBufferedIoFlag       /* = false */)
{
    const char* theErrMsgPtr = 0;
    if (IsOpen()) {
       theErrMsgPtr = "file is already open";
    } else if (! inFileNamePtr) {
        theErrMsgPtr = "file name is null";
    } else if (! sDiskIoQueuesPtr) {
        theErrMsgPtr = "disk queues are not initialized";
    } else {
        Reset();
        if (! (mQueuePtr = sDiskIoQueuesPtr->FindDiskQueue(inFileNamePtr))) {
            theErrMsgPtr = "failed to find disk queue";
        }
    }
    if (theErrMsgPtr) {
        if (inErrMessagePtr) {
            *inErrMessagePtr = theErrMsgPtr;
        }
        if (inRetryFlagPtr) {
            *inRetryFlagPtr = false;
        }
        DiskIoReportError(theErrMsgPtr, EINVAL);
        return false;
    }
    mReadOnlyFlag      = inReadOnlyFlag;
    mSpaceReservedFlag = ! mReadOnlyFlag &&
        inMaxFileSize > 0 && inReserveFileSpaceFlag;
    QCDiskQueue::OpenFileStatus const theStatus = mQueuePtr->OpenFile(
        inFileNamePtr, mReadOnlyFlag ? -1 : inMaxFileSize, mReadOnlyFlag,
        mSpaceReservedFlag, inCreateFlag, inBufferedIoFlag);
    if (theStatus.IsError()) {
        if (inErrMessagePtr) {
            *inErrMessagePtr =
                QCUtils::SysError(theStatus.GetSysError()) + " " +
                QCDiskQueue::ToString(theStatus.GetError());
        }
        Reset();
        DiskIoReportError(QCUtils::SysError(theStatus.GetSysError()) + " " +
            QCDiskQueue::ToString(theStatus.GetError()), 0);
        if (inRetryFlagPtr) {
            *inRetryFlagPtr = true;
        }
        return false;
    }
    mFileIdx = theStatus.GetFileIdx();
    sDiskIoQueuesPtr->UpdateOpenFilesCount(+1);
    return true;
}

    bool
DiskIo::File::Close(
    DiskIo::Offset inFileSize,     /* = -1 */
    string*        inErrMessagePtr /* = 0  */)
{
    if (mFileIdx < 0 || ! mQueuePtr) {
        Reset();
        return true;
    }
    QCDiskQueue::CloseFileStatus theStatus = mQueuePtr->CloseFile(
        mFileIdx, mReadOnlyFlag ? -1 : inFileSize);
    if (theStatus.IsError()) {
        if (inErrMessagePtr) {
            *inErrMessagePtr =
                QCUtils::SysError(theStatus.GetSysError()) + " " +
                QCDiskQueue::ToString(theStatus.GetError());
        }
        DiskIoReportError(QCUtils::SysError(theStatus.GetSysError()) + " " +
            QCDiskQueue::ToString(theStatus.GetError()),
            theStatus.GetSysError());
    }
    const bool theErrorFlag = mError < 0;
    Reset();
    if (sDiskIoQueuesPtr) {
        sDiskIoQueuesPtr->UpdateOpenFilesCount(-1);
    }
    return (! theStatus.IsError() && ! theErrorFlag);
}

    void
DiskIo::File::GetDiskQueuePendingCount(
    int&     outFreeRequestCount,
    int&     outRequestCount,
    int64_t& outReadBlockCount,
    int64_t& outWriteBlockCount,
    int&     outBlockSize)
{
    if (mQueuePtr) {
        mQueuePtr->GetPendingCount(
            outFreeRequestCount,
            outRequestCount,
            outReadBlockCount,
            outWriteBlockCount);
        outBlockSize = mQueuePtr->GetBlockSize();
    } else {
        outFreeRequestCount = 0;
        outRequestCount     = 0;
        outReadBlockCount   = 0;
        outWriteBlockCount  = 0;
        outBlockSize        = 0;
    }
}

    int
DiskIo::File::GetMinWriteBlkSize() const
{
    return (mQueuePtr ? mQueuePtr->GetMinWriteBlkSize() : 0);
}

    bool
DiskIo::File::ReserveSpace(
    string* inErrMessagePtr)
{
    if (mSpaceReservedFlag) {
        return true; // Already done.
    }
    if (! IsOpen()) {
        if (inErrMessagePtr) {
            *inErrMessagePtr = "closed";
        }
        return false;
    }
    if (IsReadOnly()) {
        if (inErrMessagePtr) {
            *inErrMessagePtr = "read only";
        }
        return false;
    }
    if (! mQueuePtr) {
        if (inErrMessagePtr) {
            *inErrMessagePtr = "no queue";
        }
        return false;
    }
    const DiskQueue::Status theStatus = mQueuePtr->AllocateFileSpace(mFileIdx);
    if (theStatus.IsError() && inErrMessagePtr) {
        if (theStatus.GetError() != QCDiskQueue::kErrorNone) {
            *inErrMessagePtr = QCDiskQueue::ToString(theStatus.GetError());
        } else {
            *inErrMessagePtr = QCUtils::SysError(theStatus.GetSysError());
        }
    }
    mSpaceReservedFlag = theStatus.IsGood();
    return mSpaceReservedFlag;
}

    void
DiskIo::File::Reset()
{
    mQueuePtr          = 0;
    mFileIdx           = -1;
    mReadOnlyFlag      = false;
    mSpaceReservedFlag = false;
    mError             = 0;
    mIoBuffers.clear();
}

DiskIo::DiskIo(
    DiskIo::FilePtr inFilePtr,
    KfsCallbackObj* inCallBackObjPtr)
    : mCallbackObjPtr(inCallBackObjPtr),
      mFilePtr(inFilePtr),
      mRequestId(QCDiskQueue::kRequestIdNone),
      mIoBuffers(),
      mReadBufOffset(0),
      mReadLength(0),
      mBlockIdx(0),
      mIoRetCode(0),
      mEnqueueTime(),
      mWriteSyncFlag(false),
      mCachedFlag(false),
      mCompletionRequestId(QCDiskQueue::kRequestIdNone),
      mCompletionCode(QCDiskQueue::kErrorNone),
      mChainedPtr(0)
{
    QCRTASSERT(mCallbackObjPtr && mFilePtr);
    DiskIoQueues::IoQueue::Init(*this);
}

DiskIo::~DiskIo()
{
    QCRTASSERT(mCallbackObjPtr && mFilePtr);
    DiskIo::Close();
    mIoBuffers.clear();
    // To catch double delete.
    const_cast<KfsCallbackObj*&>(mCallbackObjPtr) = 0;
    mFilePtr.reset();
}

    void
DiskIo::Close()
{
    if (mChainedPtr) {
        QCRTASSERT(mChainedPtr->mChainedPtr == this);
        mChainedPtr->mChainedPtr = 0;
        mChainedPtr = 0;
        return;
    }
    if (sDiskIoQueuesPtr) {
        sDiskIoQueuesPtr->Cancel(*this);
    }
}

class NullBufIterator : public QCDiskQueue::InputIterator
{
public:
    NullBufIterator()
        {}
    virtual char* Get()
        { return 0; }
private:
    NullBufIterator(
        const NullBufIterator& inIterator);
    NullBufIterator& operator=(
        const NullBufIterator& inIterator);
};

    ssize_t
DiskIo::Read(
    DiskIo::Offset inOffset,
    size_t         inNumBytes)
{
    if (inOffset < 0 ||
            mRequestId != QCDiskQueue::kRequestIdNone || ! mFilePtr->IsOpen()) {
        KFS_LOG_STREAM_ERROR <<
            "file: " << mFilePtr->GetFileIdx() <<
            " " << (mFilePtr->IsOpen() ? "open" : "closed") <<
            " read request: " << mRequestId <<
            " offset: " << inOffset <<
        KFS_LOG_EOM;
        DiskIoReportError("DiskIo::Read: bad parameters", EINVAL);
        return -EINVAL;
    }
    mIoBuffers.clear();
    DiskQueue* const theQueuePtr = mFilePtr->GetDiskQueuePtr();
    if (! theQueuePtr) {
        KFS_LOG_STREAM_ERROR << "read: no queue" << KFS_LOG_EOM;
        DiskIoReportError("DiskIo::Read: no queue", EINVAL);
        return -EINVAL;
    }
    if (inNumBytes <= 0) {
        return 0; // Io completion will not be called in this case.
    }
    const int theBlockSize = theQueuePtr->GetBlockSize();
    if (theBlockSize <= 0) {
        KFS_LOG_STREAM_ERROR <<
            "bad block size " << theBlockSize <<
        KFS_LOG_EOM;
        DiskIoReportError("DiskIo::Read: bad block size", EINVAL);
        return -EINVAL;
    }
    mIoRetCode     = 0;
    mBlockIdx      = -1;
    mReadBufOffset = inOffset % theBlockSize;
    mReadLength    = inNumBytes;
    const int theBufferCnt =
        (mReadLength + mReadBufOffset + theBlockSize - 1) / theBlockSize;
    mIoBuffers.reserve(theBufferCnt);
    mWriteSyncFlag       = false;
    mCompletionRequestId = QCDiskQueue::kRequestIdNone;
    mCompletionCode      = QCDiskQueue::kErrorNone;
    sDiskIoQueuesPtr->SetInFlight(this);
    const int theMinWriteBlkSize = theQueuePtr->GetMinWriteBlkSize();
    if (0 < theMinWriteBlkSize) {
        const IoBuffers& theIoBuffers = DiskQueue::GetIoBuffers(*mFilePtr);
        if (! theIoBuffers.empty()) {
            const size_t theIdx    = inOffset / theBlockSize;
            const size_t theEndIdx =
                (inOffset + inNumBytes + theBlockSize - 1) / theBlockSize;
            int   theErr = 0;
            if (theIoBuffers.size() < theEndIdx) {
                theErr = EIO;
            } else {
                IoBuffers::const_iterator       theIt    =
                    theIoBuffers.begin() + theIdx;
                IoBuffers::const_iterator const theEndIt =
                    theIoBuffers.begin() + theEndIdx;
                mIoBuffers.reserve(theEndIdx - theIdx);
                while (theIt != theEndIt && ! theIt->IsEmpty()) {
                    mIoBuffers.push_back(*theIt);
                    ++theIt;
                }
                if (theIt != theEndIt) {
                    mIoBuffers.clear();
                    theErr = EIO;
                }
            }
            if (theErr) {
                DiskIoReportError("DiskIo::Read: no buffered data",
                    theErr);
                return -theErr;
            }
            KFS_LOG_STREAM_DEBUG <<
                "read: write cached" <<
                " buffers: " << mIoBuffers.size() <<
            KFS_LOG_EOM;
            // Report completion of cached request.
            mRequestId = 200000; // > 0 != QCDiskQueue::kRequestIdNone
            const int theBufferCount = (int)mIoBuffers.size();
            const int kSysErrorCode  = 0;
            mCompletionRequestId = mRequestId;
            mCompletionCode      = QCDiskQueue::kErrorNone;
            mCachedFlag          = true;
            theQueuePtr->ReadPending(inNumBytes, 0, mCachedFlag);
            sDiskIoQueuesPtr->SetInFlight(this);
            NullBufIterator theBufItr;
            Done(
                mRequestId,
                mFilePtr->GetFileIdx(),
                (QCDiskQueue::BlockIdx)theIdx,
                theBufItr,
                theBufferCount,
                mCompletionCode,
                kSysErrorCode,
                theBufferCount * theBlockSize
            );
            return inNumBytes;
        }
    }
    const DiskQueue::EnqueueStatus theStatus = theQueuePtr->Read(
        mFilePtr->GetFileIdx(),
        inOffset / theBlockSize,
        0, // inBufferIteratorPtr // allocate buffers just beofre read
        theBufferCnt,
        this,
        sDiskIoQueuesPtr->GetMaxEnqueueWaitTimeNanoSec()
    );
    if (theStatus.IsGood()) {
        theQueuePtr->ReadPending(inNumBytes, 0, mCachedFlag);
        mRequestId = theStatus.GetRequestId();
        QCRTASSERT(mRequestId != QCDiskQueue::kRequestIdNone);
        return inNumBytes;
    }
    sDiskIoQueuesPtr->ResetInFlight(this);
    const string theErrMsg(QCDiskQueue::ToString(theStatus.GetError()));
    KFS_LOG_STREAM_ERROR <<
        "read queuing error: " << theErrMsg <<
    KFS_LOG_EOM;

    const int theErr = DiskQueueToSysError(theStatus);
    DiskIoReportError("DiskIo::Read: " + theErrMsg, theErr);
    return -theErr;
}

    ssize_t
DiskIo::Write(
    DiskIo::Offset inOffset,
    size_t         inNumBytes,
    IOBuffer*      inBufferPtr,
    bool           inSyncFlag /* = false */,
    DiskIo::Offset inEofHint  /* = -1 */)
{
    if (inOffset < 0 || ! inBufferPtr ||
            mRequestId != QCDiskQueue::kRequestIdNone || ! mFilePtr->IsOpen()) {
        KFS_LOG_STREAM_ERROR <<
            "file: " << mFilePtr->GetFileIdx() <<
            " " << (mFilePtr->IsOpen() ? "open" : "closed") <<
            " write request: " << mRequestId <<
            " offset: " << inOffset << ","
            " buffer: " << (const void*)inBufferPtr <<
        KFS_LOG_EOM;
        DiskIoReportError("DiskIo::Write: bad parameters", EINVAL);
        return -EINVAL;
    }
    mReadLength    = 0;
    mIoRetCode     = 0;
    mBlockIdx      = -1;
    mReadBufOffset = 0;
    mIoBuffers.clear();
    if (mFilePtr->IsReadOnly()) {
        KFS_LOG_STREAM_ERROR << "write: read only mode" << KFS_LOG_EOM;
        DiskIoReportError("DiskIo::Write: read only mode", EINVAL);
        return -EINVAL;
    }
    DiskQueue* const theQueuePtr = mFilePtr->GetDiskQueuePtr();
    if (! theQueuePtr) {
        KFS_LOG_STREAM_ERROR << "write: no queue" << KFS_LOG_EOM;
        DiskIoReportError("DiskIo::Write: no queue", EINVAL);
        return -EINVAL;
    }
    const int theBlockSize = theQueuePtr->GetBlockSize();
    if (inOffset % theBlockSize != 0) {
        KFS_LOG_STREAM_ERROR <<
            "file: " << mFilePtr->GetFileIdx() <<
            " write: invalid offset: " << inOffset <<
        KFS_LOG_EOM;
        DiskIoReportError("DiskIo::Write: invalid offset", EINVAL);
        return -EINVAL;
    }
    size_t                theNWr                   = inNumBytes;
    QCDiskQueue::BlockIdx theBlkIdx                =
        (QCDiskQueue::BlockIdx)(inOffset / theBlockSize);
    const size_t          theBufferCnt             =
        (theNWr + (size_t)theBlockSize - 1) / (size_t)theBlockSize;
    const int             theMinWriteBlkSize       =
        theQueuePtr->GetMinWriteBlkSize();
    const bool            theWBIgnoreOverwriteFlag =
        theQueuePtr->GetBufferDataIgnoreOverwriteFlag();
    IoBuffers&            theIoBuffers             =
        DiskQueue::GetIoBuffers(*mFilePtr);
    const bool            theBufferFlag            =
        0 < theMinWriteBlkSize && (
        ! inSyncFlag ||
        ! theIoBuffers.empty() ||
        inNumBytes % theMinWriteBlkSize != 0 ||
        inOffset   % theMinWriteBlkSize != 0
    );
    IoBuffers::iterator   theFBufIt;
    if (theBufferFlag) {
        if (mFilePtr->GetError() < 0) {
            return mFilePtr->GetError();
        }
        const size_t theSize = (size_t)theBlkIdx + theBufferCnt;
        if (theIoBuffers.size() < theSize) {
            theIoBuffers.resize(theSize, sDiskIoQueuesPtr->GetNullBufferData());
        }
        theFBufIt = theIoBuffers.begin() + theBlkIdx;
    }
    mIoBuffers.reserve(theBufferCnt);
    const size_t kBlockAlignMask = (4 << 10) - 1;
    for (IOBuffer::iterator
            theIt = inBufferPtr->begin();
            theIt != inBufferPtr->end() && theNWr > 0;
            ++theIt) {
        const IOBufferData& theBuf = *theIt;
        if (theBuf.IsEmpty()) {
            continue;
        }
        if (theNWr < (size_t)theBlockSize ||
                theBlockSize != theBuf.BytesConsumable() ||
                (theBuf.Consumer() - (char*)0) & kBlockAlignMask) {
            KFS_LOG_STREAM_ERROR <<
                "file: " << mFilePtr->GetFileIdx() <<
                " invalid io buffer: " << (theBuf.Consumer() - (char*)0) <<
                " size: " << min((int)theNWr, (int)theBuf.BytesConsumable()) <<
            KFS_LOG_EOM;
            mIoBuffers.clear();
            DiskIoReportError("DiskIo::Write: invalid buffer", EINVAL);
            return -EINVAL;
        }
        theNWr -= theBlockSize;
        // Do not allow overwrites, if requested.
        if (theBufferFlag && (! theWBIgnoreOverwriteFlag ||
                ! sDiskIoQueuesPtr->IsBufferPosWritten(*theFBufIt))) {
            *theFBufIt++ = *theIt;
        }
        mIoBuffers.push_back(*theIt);
    }
    // Unless sync requested, buffer the last write if buffering is on.
    if (theBufferFlag && inSyncFlag) {
        int64_t theTail;
        if (inEofHint < 0 || (theTail = (int64_t)theIoBuffers.size() *
                theBlockSize - inEofHint) < 0 || theBlockSize < theTail) {
            const int theErr = EIO;
            DiskIoReportError(
                "DiskIo::Write: sync: invalid EOF",
                theErr
            );
            mIoBuffers.clear();
            return -theErr;
        }
        const int           theBlkCnt = theMinWriteBlkSize / theBlockSize;
        IoBuffers::iterator theLastIt = theIoBuffers.end();
        theFBufIt = theIoBuffers.begin();
        while (theFBufIt != theLastIt) {
            --theLastIt;
            if (! theLastIt->IsEmpty()) {
                ++theLastIt;
                break;
            }
        }
        if (theLastIt != theIoBuffers.end() &&
                (theLastIt - theFBufIt) % theBlkCnt != 0) {
            const int theErr = EIO;
            DiskIoReportError(
                "DiskIo::Write: sync: incomplete trailing buffered block",
                theErr
            );
            mIoBuffers.clear();
            return -theErr;
        }
        while (theFBufIt != theLastIt) {
            if (theFBufIt->IsEmpty()) {
                ++theFBufIt;
                continue;
            }
            const QCDiskQueue::BlockIdx theIdx =
                theFBufIt - theIoBuffers.begin();
            DiskIo* theIoPtr;
            if ((theFBufIt - theIoBuffers.begin()) % theBlkCnt == 0) {
                theIoPtr = new DiskIo(mFilePtr,
                    sDiskIoQueuesPtr->GetBufferredWriteNullCallbackPtr());
                while (theFBufIt != theLastIt && ! theFBufIt->IsEmpty()) {
                    theIoPtr->mIoBuffers.push_back(*theFBufIt);
                    *theFBufIt = theWBIgnoreOverwriteFlag ?
                        sDiskIoQueuesPtr->GetNullBufferDataWritten() :
                        sDiskIoQueuesPtr->GetNullBufferData();
                    ++theFBufIt;
                }
                if ((theFBufIt != theLastIt &&
                        theIoPtr->mIoBuffers.size() != (size_t)theBlkCnt)) {
                    // Undo copy.
                    while (theIoPtr->mIoBuffers.empty()) {
                        --theFBufIt;
                        *theFBufIt = theIoPtr->mIoBuffers.back();
                        theIoPtr->mIoBuffers.pop_back();
                    }
                    delete theIoPtr;
                    theIoPtr = 0;
                }
            } else {
                theIoPtr = 0;
            }
            if (! theIoPtr) {
                const int theErr = EIO;
                DiskIoReportError(
                    "DiskIo::Write: sync: incomplete buffered block",
                    theErr
                );
                mIoBuffers.clear();
                return -theErr;
            }
            const bool theSyncFlag = theFBufIt == theLastIt;
            if (theSyncFlag) {
                mRequestId = 300000; // > 0 != QCDiskQueue::kRequestIdNone
                mWriteSyncFlag       = theSyncFlag;
                mCompletionRequestId = mRequestId;
                mCompletionCode      = QCDiskQueue::kErrorNone;
                theIoPtr->mChainedPtr = this;
                mChainedPtr = theIoPtr;
                mBlockIdx   = theBlkIdx;
            }
            const ssize_t theStatus = theIoPtr->SubmitWrite(
                theSyncFlag,
                theIdx,
                theIoPtr->mIoBuffers.size() * theBlockSize,
                theQueuePtr,
                inEofHint
            );
            if (theStatus < 0) {
                while (theIoPtr->mIoBuffers.empty()) {
                    --theFBufIt;
                    *theFBufIt = theIoPtr->mIoBuffers.back();
                    theIoPtr->mIoBuffers.pop_back();
                }
                delete theIoPtr;
                if (theSyncFlag) {
                    QCASSERT(! mChainedPtr);
                    mBlockIdx            = -1;
                    mWriteSyncFlag       = false;
                    mRequestId           = QCDiskQueue::kRequestIdNone;
                    mCompletionRequestId = mRequestId;
                }
                mIoBuffers.clear();
                return theStatus;
            }
            if (theSyncFlag) {
                theIoBuffers.clear();
                break;
            }
        }
        return (inNumBytes - theNWr);
    }
    // Check buffered data can be written, always keep the first block with
    // chunk header until sync is requested.
    if (theBufferFlag && theMinWriteBlkSize <= inOffset) {
        const int                 theBlkCnt = theMinWriteBlkSize / theBlockSize;
        IoBuffers::iterator       theEndIt  = theIoBuffers.begin() +
            (inOffset / theMinWriteBlkSize) * theBlkCnt;
        theFBufIt = theEndIt;
         // Check the beginning of the write block -- special case for yet
         // unwritten chunk header.
        if (! (theEndIt - theBlkCnt)->IsEmpty()) {
            --theFBufIt;
            for (; ;) {
                if (theFBufIt->IsEmpty()) {
                    theFBufIt++;
                    const size_t theFront =
                        (theFBufIt - theIoBuffers.begin()) % theBlkCnt;
                    if (theFront != 0) {
                        theFBufIt += theBlkCnt - theFront;
                    }
                    break;
                }
                if (theIoBuffers.begin() == theFBufIt) {
                    break;
                }
                --theFBufIt;
            }
        }
        const int theTailCnt = (theQueuePtr->GetBufferDataTailToKeepSize()
            + theBlockSize - 1) / theBlockSize;
        while (theEndIt + theBlkCnt + theTailCnt <= theIoBuffers.end() &&
                ! theEndIt->IsEmpty() &&
                ! (theEndIt + theBlkCnt - 1)->IsEmpty()) {
            if (2 < theBlkCnt) {
                IoBuffers::iterator theIt = theEndIt + theBlkCnt - 2;
                while (theEndIt < theIt && ! theIt->IsEmpty()) {
                    --theIt;
                }
                if (theEndIt < theIt) {
                    break;
                }
            }
            theEndIt += theBlkCnt;
        }
        if (theFBufIt != theEndIt) {
            DiskIo& theIo = *(new DiskIo(mFilePtr,
                sDiskIoQueuesPtr->GetBufferredWriteNullCallbackPtr()));
            theBlkIdx =
                (QCDiskQueue::BlockIdx)(theFBufIt - theIoBuffers.begin());
            while (theFBufIt != theEndIt) {
                theIo.mIoBuffers.push_back(*theFBufIt);
                *theFBufIt = theWBIgnoreOverwriteFlag ?
                    sDiskIoQueuesPtr->GetNullBufferDataWritten() :
                    sDiskIoQueuesPtr->GetNullBufferData();
                ++theFBufIt;
            }
            const ssize_t theStatus = theIo.SubmitWrite(
                inSyncFlag,
                theBlkIdx,
                theIo.mIoBuffers.size() * theBlockSize,
                theQueuePtr,
                inEofHint
            );
            if (theStatus < 0) {
                while (theIo.mIoBuffers.empty()) {
                    --theFBufIt;
                    *theFBufIt = theIo.mIoBuffers.back();
                    theIo.mIoBuffers.pop_back();
                }
                delete &theIo;
                mIoBuffers.clear();
                return theStatus;
            }
        }
    }
    if (! mIoBuffers.empty() && ! theBufferFlag) {
        const int theStatus = SubmitWrite(
            inSyncFlag, theBlkIdx, inNumBytes - theNWr, theQueuePtr, inEofHint);
        if (theStatus < 0) {
            mIoBuffers.clear();
        }
        return theStatus;
    }
    if (! theBufferFlag || inNumBytes <= theNWr) {
        return 0;
    }
    // Report completion of cached request.
    mRequestId = 100000; // > 0 != QCDiskQueue::kRequestIdNone
    const int theBufferCount = (int)mIoBuffers.size();
    const int kSysErrorCode  = 0;
    mWriteSyncFlag       = inSyncFlag;
    mCompletionRequestId = mRequestId;
    mCompletionCode      = QCDiskQueue::kErrorNone;
    mCachedFlag          = true;
    theQueuePtr->WritePending(inNumBytes - theNWr, 0, mCachedFlag);
    sDiskIoQueuesPtr->SetInFlight(this);
    NullBufIterator theBufItr;
    Done(
        mRequestId,
        mFilePtr->GetFileIdx(),
        theBlkIdx,
        theBufItr,
        theBufferCount,
        mCompletionCode,
        kSysErrorCode,
        inNumBytes - theNWr
    );
    return (inNumBytes - theNWr);
}

    inline static bool
ValidateWriteRequest(
    bool       inSyncFlag,
    int64_t    inBlockIdx,
    size_t     inNumBytes,
    DiskQueue* inQueuePtr,
    int64_t    inEofHint)
{
    const int theMinWriteSize = inQueuePtr->GetMinWriteBlkSize();
    if (theMinWriteSize <= 0) {
        return true;
    }
    const int theBlkSize = inQueuePtr->GetBlockSize();
    int64_t   theTail;
    return (inBlockIdx * theBlkSize % theMinWriteSize == 0 &&
        ((inSyncFlag && 0 <= (theTail = inBlockIdx * theBlkSize +
            (int64_t)inNumBytes - inEofHint) && theTail < theBlkSize) ||
        inNumBytes % theMinWriteSize == 0)
    );
}

class BufIterator : public QCDiskQueue::InputIterator
{
public:
    BufIterator(
        DiskIo::IoBuffers& inBufs)
        : mCur(inBufs.begin()),
          mEnd(inBufs.end())
        {}
    virtual char* Get()
        { return (mEnd == mCur ? 0 : (mCur++)->Consumer()); }
private:
    DiskIo::IoBuffers::iterator       mCur;
    DiskIo::IoBuffers::iterator const mEnd;
private:
    BufIterator(
        const BufIterator& inIterator);
    BufIterator& operator=(
        const BufIterator& inIterator);
};

    ssize_t
DiskIo::SubmitWrite(
    bool       inSyncFlag,
    int64_t    inBlockIdx,
    size_t     inNumBytes,
    DiskQueue* inQueuePtr,
    int64_t    inEofHint)
{
    QCRTASSERT(ValidateWriteRequest(
        inSyncFlag, inBlockIdx, inNumBytes, inQueuePtr, inEofHint));
    sDiskIoQueuesPtr->ValidateIoBuffers(mIoBuffers);
    BufIterator theBufItr(mIoBuffers);
    mWriteSyncFlag       = inSyncFlag;
    mCompletionRequestId = QCDiskQueue::kRequestIdNone;
    mCompletionCode      = QCDiskQueue::kErrorNone;
    sDiskIoQueuesPtr->SetInFlight(this);
    const DiskQueue::EnqueueStatus theStatus = inQueuePtr->Write(
        mFilePtr->GetFileIdx(),
        inBlockIdx,
        &theBufItr,
        mIoBuffers.size(),
        this,
        sDiskIoQueuesPtr->GetMaxEnqueueWaitTimeNanoSec(),
        inSyncFlag,
        inEofHint
    );
    if (theStatus.IsGood()) {
        inQueuePtr->WritePending(inNumBytes, 0, mCachedFlag);
        mRequestId = theStatus.GetRequestId();
        QCRTASSERT(mRequestId != QCDiskQueue::kRequestIdNone);
        return inNumBytes;
    }
    sDiskIoQueuesPtr->ResetInFlight(this);
    const string theErrMsg = QCDiskQueue::ToString(theStatus.GetError());
    KFS_LOG_STREAM_ERROR << "write queuing error: " << theErrMsg <<
    KFS_LOG_EOM;
    const int theErr = DiskQueueToSysError(theStatus);
    DiskIoReportError("DiskIo::Write: " + theErrMsg, theErr);
    return -theErr;
}

    int
DiskIo::CheckOpenStatus()
{
    if (mRequestId != QCDiskQueue::kRequestIdNone || ! mFilePtr->IsOpen()) {
        KFS_LOG_STREAM_ERROR <<
            "file: "  << mFilePtr->GetFileIdx() <<
            " "       << (mFilePtr->IsOpen() ? "open" : "closed") <<
            " check status request: " << mRequestId <<
        KFS_LOG_EOM;
        DiskIoReportError("DiskIo::CheckStatus: bad parameters", EINVAL);
        return -EINVAL;
    }
    mIoBuffers.clear();
    DiskQueue* const theQueuePtr = mFilePtr->GetDiskQueuePtr();
    if (! theQueuePtr) {
        KFS_LOG_STREAM_ERROR << "check status: no queue" << KFS_LOG_EOM;
        DiskIoReportError("DiskIo::CheckStatus: no queue", EINVAL);
        return -EINVAL;
    }
    mCompletionRequestId = QCDiskQueue::kRequestIdNone;
    mCompletionCode      = QCDiskQueue::kErrorNone;
    sDiskIoQueuesPtr->SetInFlight(this);
    const DiskQueue::EnqueueStatus theStatus = theQueuePtr->CheckOpenStatus(
        mFilePtr->GetFileIdx(),
        this,
        sDiskIoQueuesPtr->GetMaxEnqueueWaitTimeNanoSec()
    );
    if (theStatus.IsGood()) {
        mRequestId = theStatus.GetRequestId();
        QCRTASSERT(mRequestId != QCDiskQueue::kRequestIdNone);
        return 0;
    }
    sDiskIoQueuesPtr->ResetInFlight(this);
    const string theErrMsg(QCDiskQueue::ToString(theStatus.GetError()));
    KFS_LOG_STREAM_ERROR << "check status queuing error: " << theErrMsg <<
    KFS_LOG_EOM;
    const int theErr = DiskQueueToSysError(theStatus);
    DiskIoReportError("DiskIo::CheckStatus: " + theErrMsg, theErr);
    return -theErr;
}

    /* virtual */ bool
DiskIo::Done(
    QCDiskQueue::RequestId      inRequestId,
    QCDiskQueue::FileIdx        /* inFileIdx */,
    QCDiskQueue::BlockIdx       inBlockIdx,
    QCDiskQueue::InputIterator& inBufferItr,
    int                         inBufferCount,
    QCDiskQueue::Error          inCompletionCode,
    int                         inSysErrorCode,
    int64_t                     inIoByteCount)
{
    QCASSERT(sDiskIoQueuesPtr);
    sDiskIoQueuesPtr->ValidateIoBuffers(mIoBuffers);
    bool theOwnBuffersFlag = false;
    mBlockIdx = inBlockIdx;
    if (inCompletionCode != QCDiskQueue::kErrorNone) {
        if (inSysErrorCode != 0) {
            mIoRetCode = -inSysErrorCode;
        } else {
            if (inCompletionCode == QCDiskQueue::kErrorOutOfBuffers) {
                mIoRetCode = -ENOMEM;
            } else {
                mIoRetCode = -EIO;
            }
        }
        if (mIoRetCode > 0) {
            mIoRetCode = -mIoRetCode;
        } else if (mIoRetCode == 0) {
            mIoRetCode = -EIO;
        }
        // If this is read failure, then tell caller to free the buffers.
        theOwnBuffersFlag = mReadLength <= 0;
    } else {
        mIoRetCode = inIoByteCount;
        if (mReadLength <= 0) {
            theOwnBuffersFlag = true;  // Write sync or meta done.
        } else if (inIoByteCount <= 0) {
            theOwnBuffersFlag = false; // empty read, free buffers if any.
        } else {
            IOBufferAllocator& theAlloc   =
                sDiskIoQueuesPtr->GetBufferAllocator();
            const int          theBufSize = theAlloc.GetBufferSize();
            if (mIoBuffers.empty()) {
                QCRTASSERT(inBufferCount * theBufSize >= inIoByteCount);
                int                theCnt   = inBufferCount;
                char*              thePtr;
                while (theCnt-- > 0 && (thePtr = inBufferItr.Get())) {
                    sDiskIoQueuesPtr->ValidateIoBuffers(thePtr, mIoBuffers);
                    mIoBuffers.push_back(IOBufferData(
                        thePtr, 0, theBufSize, theAlloc));
                }
                QCRTASSERT((inBufferCount - (theCnt + 1)) * theBufSize >=
                    inIoByteCount);
                sDiskIoQueuesPtr->Pin(*this);
            } else {
                QCRTASSERT(
                    (int64_t)mIoBuffers.size() * theBufSize == inIoByteCount);
            }
            theOwnBuffersFlag = true;
        }
    }
    sDiskIoQueuesPtr->Put(*this, inRequestId, inCompletionCode);
    return theOwnBuffersFlag;
}

    void
DiskIo::RunCompletion()
{
    QCASSERT(mCompletionRequestId == mRequestId && sDiskIoQueuesPtr);
    mRequestId = QCDiskQueue::kRequestIdNone;
    const char* theOpNamePtr = "";
    int         theCode       = 0;
    int         theMetaFlag   = false;
    int64_t     theMetaRet    = -1;

    DiskQueue* const theQueuePtr = mFilePtr->GetDiskQueuePtr();
    QCASSERT(theQueuePtr);
    sDiskIoQueuesPtr->Unpin(*this);
    sDiskIoQueuesPtr->ValidateIoBuffers(mIoBuffers);
    if (mFilePtr.get() == theQueuePtr->GetDeleteNullFile().get()) {
        theOpNamePtr = "delete";
        theMetaFlag = true;
        theCode = EVENT_DISK_DELETE_DONE;
        sDiskIoQueuesPtr->DeleteDone(mIoRetCode);
    } else if (mFilePtr.get() == theQueuePtr->GetRenameNullFile().get()) {
        theOpNamePtr = "rename";
        theMetaFlag = true;
        theCode = EVENT_DISK_RENAME_DONE;
        sDiskIoQueuesPtr->RenameDone(mIoRetCode);
    } else if (mFilePtr.get() ==
            theQueuePtr->GetGetFsSpaceAvailableNullFile().get()) {
        theOpNamePtr = "fs space available";
        theMetaFlag = true;
        if (mBlockIdx >= 0) {
            theMetaRet = (int64_t)mBlockIdx *
                sDiskIoQueuesPtr->GetBufferAllocator().GetBufferSize();
        }
        theCode = EVENT_DISK_GET_FS_SPACE_AVAIL_DONE;
        sDiskIoQueuesPtr->GetGetFsSpaceAvailableDone(mIoRetCode);
    } else if (mFilePtr.get() ==
            theQueuePtr->GetCheckDirReadableNullFile().get()) {
        theOpNamePtr = "check dir readable";
        theMetaFlag = true;
        theCode = EVENT_DISK_CHECK_DIR_READABLE_DONE;
        sDiskIoQueuesPtr->CheckDirReadableDone(mIoRetCode);
    } else if (mFilePtr.get() ==
            theQueuePtr->GetCheckDirWritableNullFile().get()) {
        theOpNamePtr = "check dir writable";
        theMetaFlag = true;
        theCode = EVENT_DISK_CHECK_DIR_WRITABLE_DONE;
        sDiskIoQueuesPtr->CheckDirWritableDone(mIoRetCode);
    } else if (mReadLength > 0) {
        theQueuePtr->ReadPending(
            -int64_t(mReadLength), mIoRetCode, mCachedFlag);
        theOpNamePtr = "read";
    } else if (! mIoBuffers.empty()) {
        theQueuePtr->WritePending(
            -int64_t(mIoBuffers.size() *
                sDiskIoQueuesPtr->GetBufferAllocator().GetBufferSize()),
            mIoRetCode,
            mCachedFlag
        );
        theOpNamePtr = "write";
        if (mWriteSyncFlag) {
            sDiskIoQueuesPtr->SyncDone(mIoRetCode);
        }
    } else {
        theOpNamePtr = "check status";
        sDiskIoQueuesPtr->CheckOpenStatusDone(mIoRetCode);
    }
    int theNumRead(mIoRetCode);
    if (mIoRetCode < 0) {
        string theErrMsg(QCDiskQueue::ToString(mCompletionCode));
        theErrMsg += " ";
        theErrMsg += QCUtils::SysError(-theNumRead);
        KFS_LOG_STREAM(theMetaFlag ?
                MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
            " " << reinterpret_cast<const void*>(mFilePtr.get()) <<
            " " << reinterpret_cast<const void*>(this) <<
            theOpNamePtr <<
            " (" << mReadLength << " " << mIoBuffers.size() << ")"
            " status: " << theNumRead <<
            " " << theErrMsg <<
        KFS_LOG_EOM;
    }
    if (theMetaFlag) {
        KfsCallbackObj* const theCallbackObjPtr = mCallbackObjPtr;
        int64_t               theIoMetaResult[2];
        void*                 theDataPtr;
        if (mIoRetCode < 0) {
            theCode    = EVENT_DISK_ERROR;
            theDataPtr = &theNumRead;
        } else {
            theIoMetaResult[0] = mIoRetCode;
            theIoMetaResult[1] = theMetaRet;
            theDataPtr = theIoMetaResult;
        }
        delete this;
        theCallbackObjPtr->HandleEvent(theCode, theDataPtr);
        return;
    }
    QCRTASSERT(theNumRead == mIoRetCode);
    if (mIoRetCode < 0 || mReadLength <= 0) {
        const bool theCheckStatusFlag = mReadLength == 0 && mIoBuffers.empty();
        mIoBuffers.clear();
        DiskIo* const theChainedPtr = mChainedPtr;
        mChainedPtr = 0;
        if (theChainedPtr) {
            theChainedPtr->mChainedPtr     = 0;
            theChainedPtr->mIoRetCode      = mIoRetCode;
            theChainedPtr->mCompletionCode = mCompletionCode;
            theChainedPtr->mWriteSyncFlag  = mWriteSyncFlag;
        }
        IoCompletion(0, theNumRead, theCheckStatusFlag);
        if (theChainedPtr) {
            const int     theBufferSize  =
                sDiskIoQueuesPtr->GetBufferAllocator().GetBufferSize();
            const int     theBufferCount = (int)theChainedPtr->mIoBuffers.size();
            const int64_t theBlockIdx    = theChainedPtr->mBlockIdx;
            const int     theSysErrCode  =
                QCDiskQueue::kErrorNone == theChainedPtr->mCompletionCode ?
                    0 : (int)-theChainedPtr->mIoRetCode;
            theChainedPtr->mBlockIdx = 0;
            theChainedPtr->mCompletionRequestId = theChainedPtr->mRequestId;
            theChainedPtr->mCachedFlag = true;
            theQueuePtr->WritePending(theBufferCount * theBufferSize, 0,
                theChainedPtr->mCachedFlag);
            sDiskIoQueuesPtr->SetInFlight(theChainedPtr);
            NullBufIterator theBufItr;
            theChainedPtr->Done(
                theChainedPtr->mRequestId,
                theChainedPtr->mFilePtr->GetFileIdx(),
                theBlockIdx,
                theBufItr,
                theBufferCount,
                theChainedPtr->mCompletionCode,
                theSysErrCode,
                theBufferCount * theBufferSize
            );
        }
        return;
    }
    // Read. Skip/trim first/last buffers if needed.
    if (mIoBuffers.empty()) {
        QCRTASSERT(theNumRead == 0);
        theNumRead = 0;
    } else {
        const size_t  theBufSize = mIoBuffers.front().BytesConsumable();
        QCRTASSERT((ssize_t)(mIoBuffers.size() * theBufSize) >= theNumRead);
        const int theConsumed = mIoBuffers.front().Consume(mReadBufOffset);
        QCRTASSERT(theConsumed == (int)mReadBufOffset);
        theNumRead -= min(theNumRead, theConsumed);
        if (theNumRead > (int)mReadLength) {
            const int theToTrimTo(theBufSize - (theNumRead - mReadLength));
            const int theTrimmedSize = mIoBuffers.back().Trim(theToTrimTo);
            QCRTASSERT(theToTrimTo == theTrimmedSize);
            theNumRead = mReadLength;
        }
    }
    IOBuffer theIoBuffer;
    int theRem = theNumRead;
    for (IoBuffers::iterator theItr = mIoBuffers.begin();
            theItr != mIoBuffers.end() && theRem > 0;
            ++theItr) {
        const int theSize = theItr->BytesConsumable();
        if (theSize <= 0) {
            continue;
        }
        if (theSize > theRem) {
            theItr->Trim(theRem);
        }
        theRem -= theSize;
        theIoBuffer.Append(*theItr);
    }
    mIoBuffers.clear();
    QCASSERT(theIoBuffer.BytesConsumable() <= theNumRead);
    IoCompletion(&theIoBuffer, theNumRead);
}

    void
DiskIo::IoCompletion(
    IOBuffer* inBufferPtr,
    int       inRetCode,
    bool      inCheckStatusFlag /* = false */)
{
    if (sDiskIoQueuesPtr->GetBufferredWriteNullCallbackPtr() ==
            mCallbackObjPtr) {
        if (inRetCode < 0 && 0 <= mFilePtr->GetError()) {
            DiskQueue::SetError(*mFilePtr, inRetCode);
        }
        delete this;
        return;
    }
    if (inRetCode < 0) {
        mCallbackObjPtr->HandleEvent(EVENT_DISK_ERROR, &inRetCode);
    } else if (inCheckStatusFlag) {
        mCallbackObjPtr->HandleEvent(EVENT_CHECK_OPEN_STATUS_DONE, 0);
    } else if (inBufferPtr) {
        globals().ctrDiskBytesRead.Update(mIoRetCode);
        mCallbackObjPtr->HandleEvent(EVENT_DISK_READ, inBufferPtr);
    } else {
        globals().ctrDiskBytesWritten.Update(mIoRetCode);
        mCallbackObjPtr->HandleEvent(EVENT_DISK_WROTE, &inRetCode);
    }
}

} /* namespace KFS */
