//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2009/01/17
// Author: Mike Ovsiannikov
//
// Copyright 2009-2012 Quantcast Corp.
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

#include "kfsio/IOBuffer.h"
#include "kfsio/Globals.h"
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
#include <boost/random/mersenne_twister.hpp>
#include <openssl/rand.h>

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
using libkfsio::globals;

static void DiskIoReportError(
    const char* inMsgPtr,
    int         inSysError = 0);
static void DiskIoReportError(
    string inMsg,
    int    inSysError = 0)
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
            return (
                mMinTimeMicroSec <= mMaxTimeMicroSec && mMaxTimeMicroSec > 0 &&
                (mPrefixes.empty() ||
                mPrefixes.find(inPrefix) != mPrefixes.end())
            );
        }
        const int64_t mMinPeriodReq;
        const int64_t mMaxPeriodReq;
        const int64_t mMinTimeMicroSec;
        const int64_t mMaxTimeMicroSec;
        set<string>   mPrefixes;
    };

    DiskErrorSimulator(
        const Config& inConfig)
        : QCDiskQueue::IoStartObserver(),
          mMutex(),
          mSleepCond(),
          mRandom(Seed()),
          mRandMax(mRandom.max()),
          mMinPeriodReq(inConfig.mMinPeriodReq),
          mMaxPeriodReq(inConfig.mMaxPeriodReq),
          mMinTimeMicroSec(inConfig.mMinTimeMicroSec),
          mMaxTimeMicroSec(inConfig.mMaxTimeMicroSec),
          mSleepingFlag(false),
          mReqCount(0)
        { mReqCount = Rand(mMinPeriodReq, mMaxPeriodReq); }
    virtual ~DiskErrorSimulator()
        { DiskErrorSimulator::Shutdown(); }
    void Shutdown()
    {
        QCStMutexLocker theLocker(mMutex);
        mReqCount     = numeric_limits<int>::max();
        mSleepingFlag = false;
        mSleepCond.NotifyAll();
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
        if (--mReqCount > 0) {
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
    typedef boost::mt19937 Random;

    QCMutex                   mMutex;
    QCCondVar                 mSleepCond;
    Random                    mRandom;
    const Random::result_type mRandMax;
    const int64_t             mMinPeriodReq;
    const int64_t             mMaxPeriodReq;
    const int64_t             mMinTimeMicroSec;
    const int64_t             mMaxTimeMicroSec;
    bool                      mSleepingFlag;
    int64_t                   mReqCount;

    int64_t Rand(
        int64_t inFrom,
        int64_t inTo)
    {
        if (inFrom >= inTo) {
            return inTo;
        }
        // Don't use modulo, low order bits might be "less random".
        // Though this shouldn't be a problem with Mersenne twister.
        const int64_t theInterval = inTo - inFrom;
        return (inFrom + mRandom() * theInterval / mRandMax);
    }
    static Random::result_type Seed()
    {
        Random::result_type theRet = 1;
        RAND_pseudo_bytes(
            reinterpret_cast<unsigned char*>(&theRet),
            int(sizeof(theRet))
        );
        return theRet;
    }
private:
    DiskErrorSimulator(
        const DiskErrorSimulator&);
    DiskErrorSimulator operator=(
        const DiskErrorSimulator&);
};

// Disk io queue.
class DiskQueue : public QCDiskQueue,
    private QCDiskQueue::DebugTracer
{
public:
    typedef QCDLList<DiskQueue, 0> DiskQueueList;
    typedef DiskIo::DeviceId       DeviceId;

    DiskQueue(
        DiskQueue**                       inListPtr,
        DeviceId                          inDeviceId,
        const char*                       inFileNamePrefixPtr,
        const DiskErrorSimulator::Config* inSimulatorConfigPtr)
        : QCDiskQueue(),
          QCDiskQueue::DebugTracer(),
          mFileNamePrefixes(inFileNamePrefixPtr ? inFileNamePrefixPtr : ""),
          mDeviceId(inDeviceId),
          mDeleteNullFilePtr(new DiskIo::File()),
          mRenameNullFilePtr(new DiskIo::File()),
          mGetFsSpaceAvailableNullFilePtr(new DiskIo::File()),
          mCheckDirReadableNullFilePtr(new DiskIo::File()),
          mSimulatorPtr(inSimulatorConfigPtr ?
            new DiskErrorSimulator(*inSimulatorConfigPtr) : 0)
    {
        mFileNamePrefixes.append(1, (char)0);
        DiskQueueList::Init(*this);
        DiskQueueList::PushBack(inListPtr, *this);
        mDeleteNullFilePtr->mQueuePtr              = this;
        mRenameNullFilePtr->mQueuePtr              = this;
        mGetFsSpaceAvailableNullFilePtr->mQueuePtr = this;
        mCheckDirReadableNullFilePtr->mQueuePtr    = this;
    }
    void Delete(
        DiskQueue** inListPtr)
    {
        DiskQueueList::Remove(inListPtr, *this);
        delete this;
    }
    int Start(
        int             inThreadCount,
        int             inMaxQueueDepth,
        int             inMaxBuffersPerRequestCount,
        int             inFileCount,
        const char**    inFileNamesPtr,
        QCIoBufferPool& inBufferPool,
        CpuAffinity     inCpuAffinity,
        bool            inTraceFlag)
    {
        return QCDiskQueue::Start(
            inThreadCount,
            inMaxQueueDepth,
            inMaxBuffersPerRequestCount,
            inFileCount,
            inFileNamesPtr,
            inBufferPool,
            mSimulatorPtr,
            inCpuAffinity,
            inTraceFlag ? this : 0
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
private:
    string                    mFileNamePrefixes;
    DeviceId                  mDeviceId;
    DiskIo::FilePtr           mDeleteNullFilePtr; // Pseudo files.
    DiskIo::FilePtr           mRenameNullFilePtr;
    DiskIo::FilePtr           mGetFsSpaceAvailableNullFilePtr;
    DiskIo::FilePtr           mCheckDirReadableNullFilePtr;
    DiskErrorSimulator* const mSimulatorPtr;
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
          mDiskQueueMaxQueueDepth(inConfig.getValue(
            "chunkServer.diskQueue.maxDepth", 4 << 10)),
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
            "chunkServer.ioBufferPool.lockMemory", false)),
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
            "chunkServer.diskIo.crashOnError", false)),
          mBufferManagerMaxRatio(inConfig.getValue(
            "chunkServer.bufferManager.maxRatio", 0.4)),
          mMaxClientQuota(inConfig.getValue(
            "chunkServer.bufferManager.maxClientQuota",
            int64_t(CHUNKSIZE + (4 << 20)))),
          mMaxIoTime(inConfig.getValue(
            "chunkServer.diskIo.maxIoTimeSec", 4 * 60 + 30)),
          mOverloadedFlag(false),
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
            "chunkServer.bufferManager.enabled", true)),
          mNullCallback(),
          mCounters(),
          mDiskErrorSimulatorConfig(inConfig),
          mCpuAffinity(inConfig.getValue(
            "chunkServer.diskQueue.cpuAffinity", 0)),
          mDiskQueueTraceFlag(inConfig.getValue(
            "chunkServer.diskQueue.trace", 0) != 0)
    {
        mCounters.Clear();
        IoQueue::Init(mIoInFlightQueuePtr);
        IoQueue::Init(mIoDoneQueuePtr);
        DiskQueueList::Init(mDiskQueuesPtr);
        // Call Timeout() every time NetManager goes trough its work loop.
        ITimeout::SetTimeoutInterval(0);
    }
    ~DiskIoQueues()
    {
        DiskIoQueues::Shutdown(0, false);
        globalNetManager().UnRegisterTimeoutHandler(this);
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
            if (! SetIOBufferAllocator(&GetBufferAllocator())) {
                DiskIoReportError("failed to set buffer allocator");
                if (inErrMessagePtr) {
                    *inErrMessagePtr = "failed to set buffer allocator";
                    theSysError = -1;
                }
            } else {
                // Make sure that allocator works, and it isn't possible to
                // change it:
                IOBufferData theAllocatorTest;
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
            IoQueue::Remove(mIoInFlightQueuePtr, inIo);
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
                WritePending(-int64_t(inIo.mIoBuffers.size() *
                    GetBufferAllocator().GetBufferSize()));
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
                ReadPending(-int64_t(inIo.mReadLength));
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
            QCASSERT(IoQueue::IsInList(mIoInFlightQueuePtr, inIo));
            IoQueue::Remove(mIoInFlightQueuePtr, inIo);
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
        string*          inErrMessagePtr)
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
        theQueuePtr = new DiskQueue(
            mDiskQueuesPtr,
            inDeviceId,
            inDirNamePtr,
            mDiskErrorSimulatorConfig.IsEnabled(inDirNamePtr) ?
                &mDiskErrorSimulatorConfig : 0
        );
        const int theSysErr = theQueuePtr->Start(
            mDiskQueueThreadCount,
            mDiskQueueMaxQueueDepth,
            mDiskQueueMaxBuffersPerRequest,
            inMaxOpenFiles,
            0, // FileNamesPtr
            GetBufferPool(),
            mCpuAffinity,
            mDiskQueueTraceFlag
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
        if (mCrashOnErrorFlag) {
            QCUtils::FatalError(inMsgPtr, inErr);
        }
    }
    size_t GetMaxRequestSize() const
        { return mMaxRequestSize; }
    void ReadPending(
        int64_t inReqBytes,
        ssize_t inRetCode = 0)
    {
        if (inReqBytes == 0) {
            return;
        }
        if (inReqBytes < 0) {
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
        int64_t inRetCode = 0)
    {
        if (inReqBytes == 0) {
            return;
        }
        if (inReqBytes < 0) {
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
        inIoPtr->mEnqueueTime = Now();
        QCStMutexLocker theLocker(mMutex);
        IoQueue::PushBack(mIoInFlightQueuePtr, *inIoPtr);
    }
    void ResetInFlight(
        DiskIo* inIoPtr)
    {
        if (! inIoPtr) {
            return;
        }
        QCStMutexLocker theLocker(mMutex);
        IoQueue::Remove(mIoInFlightQueuePtr, *inIoPtr);
    }
    KfsCallbackObj* GetNullCallbackPtr()
        { return &mNullCallback; }
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
        mBufferManager.SetWaitingAvgInterval(inProperties.getValue(
            "chunkServer.bufferManager.waitingAvgInterval",
            mBufferManager.GetWaitingAvgInterval()));
        mMaxIoTime = max(1, inProperties.getValue(
            "chunkServer.diskIo.maxIoTimeSec", mMaxIoTime));
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
    class BufferAllocator : public IOBufferAllocator
    {
    public:
        BufferAllocator()
            : mBufferPool()
            {}
        virtual size_t GetBufferSize() const
            { return mBufferPool.GetBufferSize(); }
        virtual char* Allocate()
        {
            char* const theBufPtr = mBufferPool.Get();
            if (! theBufPtr) {
                QCUtils::FatalError("out of io buffers", 0);
            }
            return theBufPtr;
        }
        virtual void Deallocate(
            char* inBufferPtr)
            { mBufferPool.Put(inBufferPtr); }
        QCIoBufferPool& GetBufferPool()
            { return mBufferPool; }
    private:
        QCIoBufferPool mBufferPool;

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
    const int                      mDiskQueueMaxQueueDepth;
    const int                      mDiskQueueMaxBuffersPerRequest;
    const DiskQueue::Time          mDiskQueueMaxEnqueueWaitNanoSec;
    const int                      mBufferPoolPartitionCount;
    const int                      mBufferPoolPartitionBufferCount;
    const int                      mBufferPoolBufferSize;
    const int                      mBufferPoolLockMemoryFlag;
    const int                      mDiskOverloadedPendingRequestCount;
    const int                      mDiskClearOverloadedPendingRequestCount;
    const int                      mDiskOverloadedMinFreeBufferCount;
    const int                      mDiskClearOverloadedMinFreeBufferCount;
    const int64_t                  mDiskOverloadedPendingWriteByteCount;
    const int64_t                  mDiskClearOverloadedPendingWriteByteCount;
    const bool                     mCrashOnErrorFlag;
    const double                   mBufferManagerMaxRatio;
    const BufferManager::ByteCount mMaxClientQuota;
    int                            mMaxIoTime;
    bool                           mOverloadedFlag;
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
    DiskIo*                        mIoInFlightQueuePtr[1];
    DiskIo*                        mIoDoneQueuePtr[1];
    DiskQueue*                     mDiskQueuesPtr[1];
    Counters                       mCounters;
    DiskErrorSimulator::Config     mDiskErrorSimulatorConfig;
    const QCDiskQueue::CpuAffinity mCpuAffinity;
    const int                      mDiskQueueTraceFlag;

    QCIoBufferPool& GetBufferPool()
        { return mBufferAllocator.GetBufferPool(); }

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
        const int kMaxTimerOverrun = 60;
        if (theNow > mNextIoTimeout + kMaxTimerOverrun) {
            mNextIoTimeout = theNow + kMaxTimerOverrun / 8; // Reschedule
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
            (mOverloadedFlag ? "Setting" : "Clearing") <<
            " disk overloaded state: pending"
            " read: "  << mReadReqCount <<
            " bytes: " << mReadPendingBytes <<
            " write: " << mWriteReqCount <<
            " bytes: " << mWritePendingBytes <<
        KFS_LOG_EOM;
        mBufferManager.SetDiskOverloaded(inFlag);
    }
};

static DiskIoQueues* sDiskIoQueuesPtr;

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
    string*          inErrMessagePtr /* = 0 */)
{
    if (! sDiskIoQueuesPtr) {
        if (inErrMessagePtr) {
            *inErrMessagePtr = "not initialized";
        }
        return false;
    }
    return sDiskIoQueuesPtr->AddDiskQueue(
        inDirNamePtr, inDeviceId, inMaxOpenFiles, inErrMessagePtr);
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

     /* static */ bool
DiskIo::EnqueueMeta(
    DiskIo::MetaOpType inOpType,
    const char*        inNamePtr,
    const char*        inNextNamePtr,
    KfsCallbackObj*    inCallbackObjPtr,
    string*            inErrMessagePtr)
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
    Reset();
    if (sDiskIoQueuesPtr) {
        sDiskIoQueuesPtr->UpdateOpenFilesCount(-1);
    }
    return (! theStatus.IsError());
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
      mCompletionRequestId(QCDiskQueue::kRequestIdNone),
      mCompletionCode(QCDiskQueue::kErrorNone)
{
    QCRTASSERT(mCallbackObjPtr && mFilePtr.get());
    DiskIoQueues::IoQueue::Init(*this);
}

DiskIo::~DiskIo()
{
    DiskIo::Close();
}

    void
DiskIo::Close()
{
    if (sDiskIoQueuesPtr) {
        sDiskIoQueuesPtr->Cancel(*this);
    }
}

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
    const DiskQueue::EnqueueStatus theStatus = theQueuePtr->Read(
        mFilePtr->GetFileIdx(),
        inOffset / theBlockSize,
        0, // inBufferIteratorPtr // allocate buffers just beofre read
        theBufferCnt,
        this,
        sDiskIoQueuesPtr->GetMaxEnqueueWaitTimeNanoSec()
    );
    if (theStatus.IsGood()) {
        sDiskIoQueuesPtr->ReadPending(inNumBytes);
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
    bool           inSyncFlag /* = false */)
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
    const size_t kBlockAlignMask = (4 << 10) - 1;
    size_t       theNWr          = inNumBytes;
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
        mIoBuffers.push_back(*theIt);
    }
    if (mIoBuffers.empty()) {
        return 0;
    }
    struct BufIterator : public QCDiskQueue::InputIterator
    {
        BufIterator(
            IoBuffers& inBufs)
            : mCur(inBufs.begin()),
              mEnd(inBufs.end())
            {}
            virtual char* Get()
                { return (mEnd == mCur ? 0 : (mCur++)->Consumer()); }
        IoBuffers::iterator       mCur;
        IoBuffers::iterator const mEnd;
    };
    BufIterator theBufItr(mIoBuffers);
    mWriteSyncFlag       = inSyncFlag;
    mCompletionRequestId = QCDiskQueue::kRequestIdNone;
    mCompletionCode      = QCDiskQueue::kErrorNone;
    sDiskIoQueuesPtr->SetInFlight(this);
    const DiskQueue::EnqueueStatus theStatus = theQueuePtr->Write(
        mFilePtr->GetFileIdx(),
        inOffset / theBlockSize,
        &theBufItr,
        mIoBuffers.size(),
        this,
        sDiskIoQueuesPtr->GetMaxEnqueueWaitTimeNanoSec(),
        inSyncFlag
    );
    if (theStatus.IsGood()) {
        sDiskIoQueuesPtr->WritePending(inNumBytes - theNWr);
        mRequestId = theStatus.GetRequestId();
        QCRTASSERT(mRequestId != QCDiskQueue::kRequestIdNone);
        return (inNumBytes - theNWr);
    }
    sDiskIoQueuesPtr->ResetInFlight(this);
    const string theErrMsg = QCDiskQueue::ToString(theStatus.GetError());
    KFS_LOG_STREAM_ERROR << "write queuing error: " << theErrMsg <<
    KFS_LOG_EOM;
    const int theErr = DiskQueueToSysError(theStatus);
    DiskIoReportError("DiskIo::Write: " + theErrMsg, theErr);
    return -theErr;
}

    /* virtual */ bool
DiskIo::Done(
    QCDiskQueue::RequestId      inRequestId,
    QCDiskQueue::FileIdx        inFileIdx,
    QCDiskQueue::BlockIdx       inBlockIdx,
    QCDiskQueue::InputIterator& inBufferItr,
    int                         inBufferCount,
    QCDiskQueue::Error          inCompletionCode,
    int                         inSysErrorCode,
    int64_t                     inIoByteCount)
{
    QCASSERT(sDiskIoQueuesPtr);
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
            mIoRetCode = -1000;
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
            const int theBufSize =
                sDiskIoQueuesPtr->GetBufferAllocator().GetBufferSize();
            QCRTASSERT(inBufferCount * theBufSize >= inIoByteCount);
            int   theCnt         = inBufferCount;
            char* thePtr;
            while (theCnt-- > 0 && (thePtr = inBufferItr.Get())) {
                mIoBuffers.push_back(IOBufferData(
                    thePtr, 0, theBufSize,
                    sDiskIoQueuesPtr->GetBufferAllocator()));
            }
            QCRTASSERT(
                (inBufferCount - (theCnt + 1)) * theBufSize >= inIoByteCount);
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
    } else if (mReadLength > 0) {
        sDiskIoQueuesPtr->ReadPending(-int64_t(mReadLength), mIoRetCode);
        theOpNamePtr = "read";
    } else if (! mIoBuffers.empty()) {
        sDiskIoQueuesPtr->WritePending(-int64_t(mIoBuffers.size() *
            sDiskIoQueuesPtr->GetBufferAllocator().GetBufferSize()),
            mIoRetCode);
        theOpNamePtr = "write";
        if (mWriteSyncFlag) {
            sDiskIoQueuesPtr->SyncDone(mIoRetCode);
        }
    } else {
        theOpNamePtr = "sync";
        sDiskIoQueuesPtr->SyncDone(mIoRetCode);
    }
    int theNumRead(mIoRetCode);
    if (mIoRetCode < 0) {
        string theErrMsg(QCDiskQueue::ToString(mCompletionCode));
        theErrMsg += " ";
        theErrMsg += QCUtils::SysError(-theNumRead);
        KFS_LOG_STREAM(theMetaFlag ?
                MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
            theOpNamePtr <<
            " (" << mReadLength << " " << mIoBuffers.size() << ")"
            " error: " << theNumRead <<
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
        mIoBuffers.clear();
        IoCompletion(0, theNumRead);
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
    int       inRetCode)
{
    if (inRetCode < 0) {
        mCallbackObjPtr->HandleEvent(EVENT_DISK_ERROR, &inRetCode);
    } else if (inBufferPtr) {
        globals().ctrDiskBytesRead.Update(int(mIoRetCode));
        mCallbackObjPtr->HandleEvent(EVENT_DISK_READ, inBufferPtr);
    } else {
        globals().ctrDiskBytesWritten.Update(int(mIoRetCode));
        mCallbackObjPtr->HandleEvent(EVENT_DISK_WROTE, &inRetCode);
    }
}

} /* namespace KFS */
