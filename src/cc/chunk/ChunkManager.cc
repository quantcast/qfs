//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/28
// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
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

#include "ChunkManager.h"
#include "ChunkServer.h"
#include "MetaServerSM.h"
#include "LeaseClerk.h"
#include "AtomicRecordAppender.h"
#include "utils.h"
#include "Logger.h"
#include "DiskIo.h"
#include "Replicator.h"
#include "BufferManager.h"
#include "ClientManager.h"
#include "ClientSM.h"

#include "common/MsgLogger.h"
#include "common/kfstypes.h"
#include "common/nofilelimit.h"
#include "common/IntToString.h"

#include "kfsio/Counter.h"
#include "kfsio/checksum.h"
#include "kfsio/Globals.h"
#include "qcdio/QCUtils.h"

#include <dirent.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/types.h>

#include <fstream>
#include <sstream>
#include <algorithm>
#include <string>
#include <set>

#include <boost/bind.hpp>

namespace KFS
{
using std::ofstream;
using std::ostringstream;
using std::istringstream;
using std::min;
using std::max;
using std::string;
using std::vector;
using std::make_pair;
using std::sort;
using std::unique;
using std::greater;
using std::set;
using std::binary_function;
using boost::bind;

using namespace KFS::libkfsio;

ChunkManager gChunkManager;

typedef QCDLList<ChunkInfoHandle, 0> ChunkList;
typedef QCDLList<ChunkInfoHandle, 1> ChunkDirList;
typedef ChunkList ChunkLru;

// Chunk directory state. The present production deployment use one chunk
// directory per physical disk.
struct ChunkManager::ChunkDirInfo : public ITimeout
{
    ChunkDirInfo()
        : ITimeout(),
          dirname(),
          bufferedIoFlag(false),
          storageTier(kKfsSTierUndef),
          usedSpace(0),
          availableSpace(-1),
          totalSpace(0),
          notStableSpace(0),
          totalNotStableSpace(0),
          pendingReadBytes(0),
          pendingWriteBytes(0),
          corruptedChunksCount(0),
          evacuateCheckIoErrorsCount(0),
          evacuateStartByteCount(0),
          evacuateStartChunkCount(-1),
          chunkCount(0),
          notStableOpenCount(0),
          totalNotStableOpenCount(0),
          diskTimeoutCount(0),
          evacuateInFlightCount(0),
          rescheduleEvacuateThreshold(0),
          diskQueue(0),
          deviceId(-1),
          dirLock(),
          dirCountSpaceAvailable(0),
          pendingSpaceReservationSize(0),
          fileSystemId(-1),
          supportsSpaceReservatonFlag(false),
          fsSpaceAvailInFlightFlag(false),
          checkDirFlightFlag(false),
          checkEvacuateFileInFlightFlag(false),
          evacuateChunksOpInFlightFlag(false),
          evacuateFlag(false),
          evacuateStartedFlag(false),
          stopEvacuationFlag(false),
          evacuateDoneFlag(false),
          evacuateFileRenameInFlightFlag(false),
          placementSkipFlag(false),
          availableChunksOpInFlightFlag(false),
          notifyAvailableChunksStartFlag(false),
          timeoutPendingFlag(false),
          chunksAvailableInFlightSortedFlag(false),
          lastEvacuationActivityTime(
            globalNetManager().Now() - 365 * 24 * 60 * 60),
          startTime(globalNetManager().Now()),
          stopTime(startTime),
          startCount(0),
          evacuateCompletedCount(0),
          readCounters(),
          writeCounters(),
          totalReadCounters(),
          totalWriteCounters(),
          availableChunks(),
          fsSpaceAvailCb(),
          checkDirCb(),
          checkEvacuateFileCb(),
          evacuateChunksCb(),
          renameEvacuateFileCb(),
          availableChunksCb(),
          evacuateChunksOp(0, &evacuateChunksCb),
          availableChunksOp(0, &availableChunksCb),
          chunkDirInfoOp(*this)
    {
        fsSpaceAvailCb.SetHandler(this,
            &ChunkDirInfo::FsSpaceAvailDone);
        checkDirCb.SetHandler(this,
            &ChunkDirInfo::CheckDirDone);
        checkEvacuateFileCb.SetHandler(this,
            &ChunkDirInfo::CheckEvacuateFileDone);
        evacuateChunksCb.SetHandler(this,
            &ChunkDirInfo::EvacuateChunksDone);
        renameEvacuateFileCb.SetHandler(this,
            &ChunkDirInfo::RenameEvacuateFileDone);
        availableChunksCb.SetHandler(this,
            &ChunkDirInfo::AvailableChunksDone);
        for (int i = 0; i < kChunkDirListCount; i++) {
            ChunkList::Init(chunkLists[i]);
            ChunkDirList::Init(chunkLists[i]);
        }
    }
    ~ChunkDirInfo()
    {
        if (timeoutPendingFlag) {
            timeoutPendingFlag = false;
            globalNetManager().UnRegisterTimeoutHandler(this);
        }
     }
    int FsSpaceAvailDone(int code, void* data);
    int CheckDirDone(int code, void* data);
    int CheckEvacuateFileDone(int code, void* data);
    int RenameEvacuateFileDone(int code, void* data);
    void DiskError(int sysErr);
    int EvacuateChunksDone(int code, void* data);
    int AvailableChunksDone(int code, void* data);
    void ScheduleEvacuate(int maxChunkCount = -1);
    void RestartEvacuation();
    void NotifyAvailableChunks(bool tmeoutFlag = false);
    void NotifyAvailableChunksStart()
    {
        notifyAvailableChunksStartFlag = true;
        NotifyAvailableChunks();
    }
    void UpdateLastEvacuationActivityTime()
    {
        lastEvacuationActivityTime = globalNetManager().Now();
    }
    void ChunkEvacuateDone()
    {
        UpdateLastEvacuationActivityTime();
        if (evacuateInFlightCount > 0 &&
                --evacuateInFlightCount <= rescheduleEvacuateThreshold) {
            ScheduleEvacuate();
        }
    }
    void Stop()
    {
        for (int i = 0; i < kChunkDirListCount; i++) {
            if (! ChunkDirList::IsEmpty(chunkLists[i])) {
                die("chunk dir stop: chunk list is not empty");
            }
        }
        if (chunkCount != 0) {
            die("chunk dir stop: invalid chunk count");
            chunkCount = 0;
        }
        if (notStableOpenCount != 0) {
            die("chunk dir stop: invalid not stable chunk count");
            notStableOpenCount = 0;
        }
        if (diskQueue) {
            string err;
            if (! DiskIo::StopIoQueue(
                    diskQueue, dirname.c_str(), deviceId, &err)) {
                die("failed to stop io queue: " + err);
            }
            deviceId  = -1;
            diskQueue = 0;
        }
        if (evacuateDoneFlag) {
            evacuateCompletedCount++;
        }
        if (notStableSpace > 0) {
            UpdateNotStableSpace(-notStableSpace);
        }
        const bool sendUpdateFlag      = availableSpace >= 0;
        availableSpace                 = -1;
        rescheduleEvacuateThreshold    = 0;
        evacuateFlag                   = false;
        evacuateStartedFlag            = false;
        stopEvacuationFlag             = false;
        evacuateDoneFlag               = false;
        diskTimeoutCount               = 0;
        dirCountSpaceAvailable         = 0;
        usedSpace                      = 0;
        totalSpace                     = 0;
        notStableSpace                 = 0;
        totalNotStableSpace            = 0;
        totalNotStableOpenCount        = 0;
        evacuateStartChunkCount        = -1;
        evacuateStartByteCount         = -1;
        notifyAvailableChunksStartFlag = false;
        availableChunks.Clear();
        if (timeoutPendingFlag) {
            timeoutPendingFlag = false;
            globalNetManager().UnRegisterTimeoutHandler(this);
        }
        stopTime = globalNetManager().Now();
        if (sendUpdateFlag) {
            chunkDirInfoOp.Enqueue();
        }
    }
    void Start()
    {
        startTime = globalNetManager().Now();
        startCount++;
        readCounters.Reset();
        writeCounters.Reset();
        const bool kResetLastCountersFlag = true;
        chunkDirInfoOp.Enqueue(kResetLastCountersFlag);
        NotifyAvailableChunksStart();
    }
    bool StopEvacuation();
    void SetEvacuateStarted()
    {
        stopEvacuationFlag  = false;
        evacuateStartedFlag = true;
        evacuateStartChunkCount = max(evacuateStartChunkCount, chunkCount);
        evacuateStartByteCount  = max(evacuateStartByteCount, usedSpace);
    }
    bool IsCountFsSpaceAvailable() const
        { return (this == dirCountSpaceAvailable); }
    int GetEvacuateDoneChunkCount() const
    {
        return (max(evacuateStartChunkCount, chunkCount) - chunkCount);
    }
    int64_t GetEvacuateDoneByteCount() const
    {
        return (max(evacuateStartByteCount, usedSpace) - usedSpace);
    }
    virtual void Timeout()
    {
        const bool kTimeoutFlag = true;
        NotifyAvailableChunks(kTimeoutFlag);
    }
    void UpdateNotStableSpace(int nbytes)
    {
        if (availableSpace < 0) {
            return;
        }
        if (notStableOpenCount <= 0) {
            notStableSpace = 0;
        } else {
            notStableSpace += nbytes;
            if (totalSpace < notStableSpace) {
                notStableSpace = totalSpace;
            }
            if (notStableSpace < 0) {
                notStableSpace = 0;
            } else {
                notStableSpace = min(notStableSpace,
                    notStableOpenCount *
                        (int64_t)(CHUNKSIZE + KFS_CHUNK_HEADER_SIZE));
            }
        }
        if (totalNotStableOpenCount <= 0) {
            totalNotStableOpenCount = 0;
        } else {
            totalNotStableSpace += nbytes;
            if (totalSpace < totalNotStableSpace) {
                totalNotStableSpace = totalSpace;
            }
            if (totalNotStableSpace < 0) {
                totalNotStableSpace = 0;
            } else {
                totalNotStableSpace = min(totalNotStableSpace,
                    totalNotStableOpenCount *
                        (int64_t)(CHUNKSIZE + KFS_CHUNK_HEADER_SIZE));
            }
        }
        if (dirCountSpaceAvailable && dirCountSpaceAvailable != this) {
            ChunkDirInfo& di = *dirCountSpaceAvailable;
            if (di.totalNotStableOpenCount <= 0) {
                di.totalNotStableSpace = 0;
            } else {
                di.totalNotStableSpace += nbytes;
                if (di.totalSpace < di.totalNotStableSpace) {
                    di.totalNotStableSpace = di.totalSpace;
                }
                if (di.totalNotStableSpace < 0) {
                    di.totalNotStableSpace = 0;
                } else {
                    di.totalNotStableSpace = min(di.totalNotStableSpace,
                        di.totalNotStableOpenCount *
                        (int64_t)(CHUNKSIZE + KFS_CHUNK_HEADER_SIZE));
                }
            }
        }
    }
    void UpdateAvailableSpace(int nbytes)
    {
        if (availableSpace < 0) {
            return;
        }
        availableSpace -= nbytes;
        if (availableSpace > totalSpace) {
            availableSpace = totalSpace;
        }
        if (availableSpace < 0) {
            availableSpace = 0;
        }
        if (dirCountSpaceAvailable && dirCountSpaceAvailable != this) {
            dirCountSpaceAvailable->UpdateAvailableSpace(nbytes);
        }
    }
    void UpdateNotStableCounts(int delta, int spaceDelta,
        int pendingReservationDelta)
    {
        notStableOpenCount      += delta;
        totalNotStableOpenCount += delta;
        pendingSpaceReservationSize += pendingReservationDelta;
        if (pendingSpaceReservationSize < 0) {
            pendingSpaceReservationSize = 0;
        } else if (totalSpace < pendingSpaceReservationSize) {
            pendingSpaceReservationSize = totalSpace;
        }
        assert(0 <= notStableOpenCount && 0 <= totalNotStableOpenCount);
        UpdateNotStableSpace(spaceDelta);
        if (dirCountSpaceAvailable && dirCountSpaceAvailable != this &&
                0 < availableSpace) {
            dirCountSpaceAvailable->totalNotStableOpenCount += delta;
            assert(0 <= totalNotStableOpenCount);
            dirCountSpaceAvailable->pendingSpaceReservationSize +=
                pendingReservationDelta;
            if (dirCountSpaceAvailable->pendingSpaceReservationSize < 0) {
                dirCountSpaceAvailable->pendingSpaceReservationSize = 0;
            }
            if (totalSpace <
                    dirCountSpaceAvailable->pendingSpaceReservationSize) {
                dirCountSpaceAvailable->pendingSpaceReservationSize =
                    totalSpace;
            }
        }
    }

    class Counters
    {
    public:
        typedef int64_t Counter;

        Counters()
            : mTimeoutCount(0),
              mChecksumErrorCount(0),
              mErrorCount(0),
              mErrorByteCount(0),
              mErrorTimeMicrosec(0),
              mTimeMicrosec(0),
              mIoCount(0),
              mByteCount(0)
            {}
        void Reset()
            { *this = Counters(); }
        void Update(
            int     inStatus,
            int64_t inByteCount,
            int64_t inTimeMicrosec)
        {
            if (inStatus < 0) {
                mErrorCount++;
                mErrorByteCount    += max(int64_t(0), inByteCount);
                mErrorTimeMicrosec += max(int64_t(0), inTimeMicrosec);
               if (inStatus == -ETIMEDOUT) {
                   mTimeoutCount++;
               } else if (inStatus == -EBADCKSUM) {
                   mChecksumErrorCount++;
               }
            } else {
                mIoCount++;
                mByteCount    += max(int64_t(0), inByteCount);
                mTimeMicrosec += max(int64_t(0), inTimeMicrosec);
            }
        }
        ostream& Display(
            const char* inPrefixPtr,
            const char* inSuffixPtr,
            ostream&    inStream) const
        {
            return (inStream <<
            inPrefixPtr << "io: "                << mIoCount <<
                inSuffixPtr <<
            inPrefixPtr << "bytes: "             << mByteCount <<
                inSuffixPtr <<
            inPrefixPtr << "timeout: "           << mTimeoutCount <<
                inSuffixPtr <<
            inPrefixPtr << "err-checksum: "      << mChecksumErrorCount <<
                inSuffixPtr <<
            inPrefixPtr << "err: "               << mErrorCount <<
                inSuffixPtr <<
            inPrefixPtr << "err-bytes: "         << mErrorByteCount <<
                inSuffixPtr <<
            inPrefixPtr << "err-time-microsec: " << mErrorTimeMicrosec <<
                inSuffixPtr <<
            inPrefixPtr << "time-microsec: "     << mTimeMicrosec <<
                inSuffixPtr
            );
        }

        Counter mTimeoutCount;
        Counter mChecksumErrorCount;
        Counter mErrorCount;
        Counter mErrorByteCount;
        Counter mErrorTimeMicrosec;
        Counter mTimeMicrosec;
        Counter mIoCount;
        Counter mByteCount;
    };
    class ChunkDirInfoOp : public KfsOp
    {
    public:
        ChunkDirInfoOp(
            const ChunkDirInfo& chunkDir)
            : KfsOp(CMD_CHUNKDIR_INFO, 0),
              mChunkDir(chunkDir),
              mInFlightFlag(false),
              mResetCountersFlag(false),
              mLastSent(globalNetManager().Now()),
              mLastReadCounters(),
              mLastWriteCounters()
        {
            noReply = true;
            noRetry = true;
            SET_HANDLER(this, &ChunkDirInfoOp::HandleDone);
        }
        virtual ~ChunkDirInfoOp()
        {
            if (mInFlightFlag) {
                die("ChunkDirInfoOp: attempt to delete in flight op");
            }
        }
        void Enqueue(bool resetCountersFlag = false)
        {
            mResetCountersFlag = mResetCountersFlag || resetCountersFlag;
            if (mInFlightFlag) {
                return;
            }
            mInFlightFlag = true;
            gMetaServerSM.EnqueueOp(this);
        }
        void Request(
            ostream& inStream)
        {
            if (mResetCountersFlag) {
                mLastReadCounters.Reset();
                mLastWriteCounters.Reset();
                mResetCountersFlag = false;
            }

            const time_t now              = globalNetManager().Now();
            const double avgTimeInterval  = max(0.5, (double)(now - mLastSent));
            const double oneOverTime      = 1.0 / avgTimeInterval;
            const double timeUtilMicroPct = 1e-4 * oneOverTime;
            const BufferManager* const bufMgr =
                DiskIo::GetDiskBufferManager(mChunkDir.diskQueue);
            BufferManager::Counters    ctrs;
            if (bufMgr) {
                bufMgr->GetCounters(ctrs);
            } else {
                ctrs.Clear();
            }
            inStream <<
            "CHUNKDIR_INFO\r\n"
            "Version: "            << KFS_VERSION_STR                  << "\r\n"
            "Cseq: "               << seq                              << "\r\n"
            "No-reply: "           << (noReply ? 1 : 0)                << "\r\n"
            "Dir-name: "           << mChunkDir.dirname                << "\r\n"
            "Dev-id: "             << mChunkDir.deviceId               << "\r\n"
            "Started-ago-sec: "    <<
                ((mChunkDir.availableSpace >= 0 ? now : mChunkDir.stopTime) -
                    mChunkDir.startTime) << "\r\n"
            "Stopped-ago-sec: "    <<
                ((mChunkDir.availableSpace < 0 ? now : mChunkDir.startTime) -
                    mChunkDir.stopTime) << "\r\n"
            "Start-count: "        << mChunkDir.startCount             << "\r\n"
            "Chunks: "             << mChunkDir.chunkCount             << "\r\n"
            "Chunks-writable: "    << mChunkDir.notStableOpenCount     << "\r\n"
            "Chunks-available: "   << mChunkDir.availableChunks.GetSize() <<
                "\r\n"
            "Space-avail: "        << mChunkDir.availableSpace         << "\r\n"
            "Space-total: "        << mChunkDir.totalSpace             << "\r\n"
            "Space-util-pct: "     <<
                (100. * max(int64_t(0), mChunkDir.totalSpace -
                    mChunkDir.availableSpace) /
                    (double)max(int64_t(1), mChunkDir.totalSpace))     << "\r\n"
            "Space-used: "         << (mChunkDir.usedSpace +
                    mChunkDir.chunkCount * KFS_CHUNK_HEADER_SIZE)      << "\r\n"
            "Space-not-stable: "   << mChunkDir.notStableSpace         << "\r\n"
            "Evacuate: "           << (mChunkDir.evacuateFlag ? 1 : 0) << "\r\n"
            "Evacuate-in-flight: " << mChunkDir.evacuateInFlightCount  << "\r\n"
            "Read-time-pct: " << timeUtilMicroPct * max(int64_t(0),
                mChunkDir.readCounters.mTimeMicrosec -
                mLastReadCounters.mTimeMicrosec) << "\r\n"
            "Read-err-time-pct: " << timeUtilMicroPct * max(int64_t(0),
                mChunkDir.readCounters.mErrorTimeMicrosec -
                mLastReadCounters.mErrorTimeMicrosec) << "\r\n"
            "Write-time-pct: " << timeUtilMicroPct * max(int64_t(0),
                mChunkDir.writeCounters.mTimeMicrosec -
                mLastWriteCounters.mTimeMicrosec) << "\r\n"
            "Write-err-time-pct: " << timeUtilMicroPct * max(int64_t(0),
                mChunkDir.writeCounters.mErrorTimeMicrosec -
                mLastWriteCounters.mErrorTimeMicrosec) << "\r\n"
            "Read-io-rate: " <<
                (mChunkDir.readCounters.mIoCount -
                mLastReadCounters.mIoCount) * oneOverTime << "\r\n"
            "Write-io-rate: " <<
                (mChunkDir.writeCounters.mIoCount -
                mLastWriteCounters.mIoCount) * oneOverTime << "\r\n"
            "Avg-read-io-bytes: " <<
                (mChunkDir.readCounters.mByteCount -
                    mLastReadCounters.mByteCount) /
                max(Counters::Counter(1), mChunkDir.readCounters.mIoCount -
                    mLastReadCounters.mIoCount) << "\r\n"
            "Avg-write-io-bytes: " <<
                (mChunkDir.writeCounters.mByteCount -
                    mLastWriteCounters.mByteCount) /
                max(Counters::Counter(1), mChunkDir.writeCounters.mIoCount -
                    mLastWriteCounters.mIoCount) << "\r\n"
            "Avg-time-interval-sec: " << avgTimeInterval << "\r\n"
            "Evacuate-complete-cnt: " << mChunkDir.evacuateCompletedCount <<
                "\r\n"
            "Storage-tier: "          << (unsigned int)mChunkDir.storageTier <<
                "\r\n"
            "Buffered-io: "           << (mChunkDir.bufferedIoFlag ? 1 : 0) <<
                "\r\n"
            "Space-reservation: "     <<
                (mChunkDir.supportsSpaceReservatonFlag ? 1 : 0) <<  "\r\n"
            "Pending-reservation: "   <<
                mChunkDir.pendingSpaceReservationSize << "\r\n"
            "Wait-avg-usec: "         <<
                (bufMgr ? bufMgr->GetWaitingAvgUsecs() : int64_t(0)) << "\r\n"
            "Wait-avg-bytes: "        <<
                (bufMgr ? bufMgr->GetWaitingAvgBytes() : int64_t(0)) << "\r\n"
            "Wait-avg-count: "        <<
                (bufMgr ? bufMgr->GetWaitingAvgCount() : int64_t(0)) << "\r\n"
            "Canceled-count: "        << ctrs.mReqeustCanceledCount  << "\r\n"
            "Canceled-bytes: "        << ctrs.mReqeustCanceledBytes  << "\r\n"
            "File-system-id: "        << mChunkDir.fileSystemId << "\r\n"
            ;
            mChunkDir.readCounters.Display(
                "Read-",         "\r\n", inStream);
            mChunkDir.writeCounters.Display(
                "Write-",       "\r\n", inStream);
            mChunkDir.totalReadCounters.Display(
                "Total-read-",  "\r\n", inStream);
            mChunkDir.totalWriteCounters.Display(
                "Total-write-", "\r\n", inStream);
            inStream << "\r\n";

            mLastSent          = now;
            mLastReadCounters  = mChunkDir.readCounters;
            mLastWriteCounters = mChunkDir.writeCounters;
        }
        // To be called whenever we get a reply from the server
        int HandleDone(int code, void* data)
        {
            if (code != EVENT_CMD_DONE || data != this || ! mInFlightFlag) {
                die("ChunkDirInfoOp: invalid completion");
            }
            mInFlightFlag = false;
            return 0;
        }
        void Execute()
            {}
        virtual ostream& ShowSelf(ostream& os) const
        {
            return os << "chunk dir info: " << mChunkDir.dirname;
        }
    private:
        const ChunkDirInfo& mChunkDir;
        bool                mInFlightFlag;
        bool                mResetCountersFlag;
        time_t              mLastSent;
        Counters            mLastReadCounters;
        Counters            mLastWriteCounters;
    };

    string                 dirname;
    bool                   bufferedIoFlag;
    kfsSTier_t             storageTier;
    int64_t                usedSpace;
    int64_t                availableSpace;
    int64_t                totalSpace;
    int64_t                notStableSpace;
    int64_t                totalNotStableSpace;
    int64_t                pendingReadBytes;
    int64_t                pendingWriteBytes;
    int64_t                corruptedChunksCount;
    int64_t                evacuateCheckIoErrorsCount;
    int64_t                evacuateStartByteCount;
    int32_t                evacuateStartChunkCount;
    int32_t                chunkCount;
    int32_t                notStableOpenCount;
    int32_t                totalNotStableOpenCount;
    int32_t                diskTimeoutCount;
    int32_t                evacuateInFlightCount;
    int32_t                rescheduleEvacuateThreshold;
    DiskQueue*             diskQueue;
    DirChecker::DeviceId   deviceId;
    DirChecker::LockFdPtr  dirLock;
    ChunkDirInfo*          dirCountSpaceAvailable;
    int64_t                pendingSpaceReservationSize;
    int64_t                fileSystemId;
    bool                   supportsSpaceReservatonFlag:1;
    bool                   fsSpaceAvailInFlightFlag:1;
    bool                   checkDirFlightFlag:1;
    bool                   checkEvacuateFileInFlightFlag:1;
    bool                   evacuateChunksOpInFlightFlag:1;
    bool                   evacuateFlag:1;
    bool                   evacuateStartedFlag:1;
    bool                   stopEvacuationFlag:1;
    bool                   evacuateDoneFlag:1;
    bool                   evacuateFileRenameInFlightFlag:1;
    bool                   placementSkipFlag:1;
    bool                   availableChunksOpInFlightFlag:1;
    bool                   notifyAvailableChunksStartFlag:1;
    bool                   timeoutPendingFlag:1;
    bool                   chunksAvailableInFlightSortedFlag:1;
    time_t                 lastEvacuationActivityTime;
    time_t                 startTime;
    time_t                 stopTime;
    int32_t                startCount;
    int32_t                evacuateCompletedCount;
    Counters               readCounters;
    Counters               writeCounters;
    Counters               totalReadCounters;
    Counters               totalWriteCounters;
    DirChecker::ChunkInfos availableChunks;
    KfsCallbackObj         fsSpaceAvailCb;
    KfsCallbackObj         checkDirCb;
    KfsCallbackObj         checkEvacuateFileCb;
    KfsCallbackObj         evacuateChunksCb;
    KfsCallbackObj         renameEvacuateFileCb;
    KfsCallbackObj         availableChunksCb;
    EvacuateChunksOp       evacuateChunksOp;
    AvailableChunksOp      availableChunksOp;
    ChunkDirInfoOp         chunkDirInfoOp;

    enum { kChunkInfoHDirListCount = kChunkInfoHandleListCount + 1 };
    enum ChunkListType
    {
        kChunkDirList         = 0,
        kChunkDirEvacuateList = 1,
        kChunkDirListNone     = 2
    };
    enum { kChunkDirListCount = kChunkDirEvacuateList + 1 };
    typedef ChunkInfoHandle* ChunkLists[kChunkInfoHDirListCount];
    ChunkLists chunkLists[kChunkDirListCount];

private:
    ChunkDirInfo(const ChunkDirInfo&);
    ChunkDirInfo& operator=(const ChunkDirInfo&);
};

inline
ChunkManager::ChunkDirs::~ChunkDirs()
{
    delete [] mChunkDirs;
}

inline ChunkManager::ChunkDirs::iterator
ChunkManager::ChunkDirs::end()
{
    return mChunkDirs + mSize;
}

inline ChunkManager::ChunkDirs::const_iterator
ChunkManager::ChunkDirs::end() const
{
    return mChunkDirs + mSize;
}

inline ChunkManager::ChunkDirInfo&
ChunkManager::ChunkDirs::operator[](size_t i)
{
    return mChunkDirs[i];
}

inline const ChunkManager::ChunkDirInfo&
ChunkManager::ChunkDirs::operator[](size_t i) const
{
    return mChunkDirs[i];
}

void
ChunkManager::ChunkDirs::Allocate(size_t size)
{
    delete [] mChunkDirs;
    mChunkDirs = 0;
    mSize      = 0;
    mChunkDirs = new ChunkDirInfo[size];
    mSize      = size;
}

// OP for reading/writing out the meta-data associated with each chunk.  This
// is an internally generated op (ops that generate this one are
// allocate/write/truncate/change-chunk-vers).
struct WriteChunkMetaOp : public KfsOp
{
    kfsChunkId_t const chunkId;
    DiskIo* const      diskIo;  /* disk connection used for writing data */
    IOBuffer           dataBuf; /* buffer with the data to be written */
    WriteChunkMetaOp*  next;
    int64_t            diskIOTime;
    const kfsSeq_t     targetVersion;
    const bool         renameFlag;
    const bool         stableFlag;

    WriteChunkMetaOp(
        kfsChunkId_t    c,
        KfsCallbackObj* o,
        DiskIo*         d,
        bool            rename,
        bool            stable,
        kfsSeq_t        version)
        : KfsOp(CMD_WRITE_CHUNKMETA, 0, o),
          chunkId(c),
          diskIo(d),
          dataBuf(),
          next(0),
          diskIOTime(0),
          targetVersion(version),
          renameFlag(rename),
          stableFlag(stable)
    {
        SET_HANDLER(this, &WriteChunkMetaOp::HandleDone);
    }
    ~WriteChunkMetaOp()
        { delete diskIo; }
    void Execute() {}
    inline bool IsRenameNeeded(const ChunkInfoHandle* cih) const;
    bool IsWaiting() const
        { return (! diskIo && ! renameFlag); }
    int Start(ChunkInfoHandle* cih);
    virtual ostream& ShowSelf(ostream& os) const
    {
        return os << "write-chunk-meta: "
            " chunkid: " << chunkId <<
            " rename:  " << renameFlag <<
            " stable:  " << stableFlag <<
            " version: " << targetVersion
        ;
    }
    // Notify the op that is waiting for the write to finish that all
    // is done
    int HandleDone(int code, void *data)
    {
        if (clnt) {
            clnt->HandleEvent(code, data);
        }
        delete this;
        return 0;
    }
};

/// Encapsulate a chunk file descriptor and information about the
/// chunk such as name and version #.
class ChunkInfoHandle : public KfsCallbackObj
{
public:
    typedef ChunkManager::ChunkLists   ChunkLists;
    typedef ChunkManager::ChunkDirInfo ChunkDirInfo;
    typedef ChunkDirInfo::ChunkLists   ChunkDirLists;

    ChunkInfoHandle(ChunkDirInfo& chunkdir, bool stableFlag = true)
        : KfsCallbackObj(),
          chunkInfo(),
          dataFH(),
          lastIOTime(0),
          readChunkMetaOp(0),
          mBeingReplicatedFlag(false),
          mDeleteFlag(false),
          mWriteAppenderOwnsFlag(false),
          mWaitForWritesInFlightFlag(false),
          mMetaDirtyFlag(false),
          mStableFlag(stableFlag),
          mInDoneHandlerFlag(false),
          mKeepFlag(false),
          mChunkList(ChunkManager::kChunkLruList),
          mChunkDirList(ChunkDirInfo::kChunkDirList),
          mRenamesInFlight(0),
          mWritesInFlight(0),
          mPendingSpaceReservationSize(0),
          mWriteMetaOpsHead(0),
          mWriteMetaOpsTail(0),
          mChunkDir(chunkdir)
    {
        ChunkList::Init(*this);
        ChunkDirList::Init(*this);
        ChunkDirList::PushBack(mChunkDir.chunkLists[mChunkDirList], *this);
        SET_HANDLER(this, &ChunkInfoHandle::HandleChunkMetaWriteDone);
        mChunkDir.chunkCount++;
        assert(mChunkDir.chunkCount > 0);
    }
    void Delete(ChunkLists* chunkInfoLists) {
        const bool evacuateFlag = IsEvacuate();
        ChunkList::Remove(chunkInfoLists[mChunkList], *this);
        DetachFromChunkDir(evacuateFlag);
        if (mWriteAppenderOwnsFlag) {
            mWriteAppenderOwnsFlag = false;
            gAtomicRecordAppendManager.DeleteChunk(chunkInfo.chunkId);
        }
        if (mWriteMetaOpsHead || mInDoneHandlerFlag) {
            mDeleteFlag = true;
            const bool runHanlder = ! mInDoneHandlerFlag &&
                mWritesInFlight > 0 && mWaitForWritesInFlightFlag;
            mWaitForWritesInFlightFlag = false;
            mWritesInFlight = 0;
            if (runHanlder) {
                int res = -1;
                HandleEvent(EVENT_DISK_ERROR, &res);
            }
        } else {
            delete this;
        }
    }
    bool IsEvacuate() const {
        return (! IsStale() &&
            mChunkDirList == ChunkDirInfo::kChunkDirEvacuateList);
    }
    bool SetEvacuate(bool flag) {
        if (IsStale()) {
            return false;
        }
        if (IsEvacuate() == flag) {
            return true;
        }
        mChunkDir.evacuateInFlightCount += (flag ? 1 : -1);
        if (mChunkDir.evacuateInFlightCount < 0) {
            mChunkDir.evacuateInFlightCount = 0;
        }
        ChunkDirList::Remove(mChunkDir.chunkLists[mChunkDirList], *this);
        mChunkDirList = flag ?
            ChunkDirInfo::kChunkDirEvacuateList :
            ChunkDirInfo::kChunkDirList;
        ChunkDirList::PushBack(mChunkDir.chunkLists[mChunkDirList], *this);
        return true;
    }

    ChunkInfo_t      chunkInfo;
    /// Chunks are stored as files in he underlying filesystem; each
    /// chunk file is named by the chunkId.  Each chunk has a header;
    /// this header is hidden from clients; all the client I/O is
    /// offset by the header amount
    DiskIo::FilePtr  dataFH;
    // when was the last I/O done on this chunk
    time_t           lastIOTime;
    /// keep track of the op that is doing the read
    ReadChunkMetaOp* readChunkMetaOp;

    void Release(ChunkLists* chunkInfoLists);
    bool IsFileOpen() const {
        return (dataFH && dataFH->IsOpen());
    }
    bool IsFileInUse() const {
        return (IsFileOpen() && ! dataFH.unique());
    }
    bool IsStable() const {
        return mStableFlag;
    }
    void StartWrite(WriteOp* /* op */) {
        assert(mWritesInFlight >= 0);
        mWritesInFlight++;
        mMetaDirtyFlag = true;
    }
    void SetMetaDirty() {
        mMetaDirtyFlag = true;
    }
    void WriteDone(const WriteOp* op = 0) {
        assert(mWritesInFlight > 0);
        mWritesInFlight--;
        if (0 < mPendingSpaceReservationSize) {
            const int pendingReservationSizeDelta =
                -mPendingSpaceReservationSize;
            mPendingSpaceReservationSize = 0;
            mChunkDir.UpdateNotStableCounts(
                0, 0, pendingReservationSizeDelta);
        }
        if (op) {
            WriteStats(op->status, op->numBytesIO, op->diskIOTime);
        }
        if (mWritesInFlight == 0 && mWaitForWritesInFlightFlag) {
            assert(mWriteMetaOpsHead);
            mWaitForWritesInFlightFlag = false;
            int res = mWriteMetaOpsHead->Start(this);
            if (res < 0) {
                HandleEvent(EVENT_DISK_ERROR, &res);
            }
        }
    }
    bool IsFileEquals(const DiskIo::File* file) const {
        return (file && file == dataFH.get());
    }
    bool IsFileEquals(const DiskIo* diskIo) const {
        return (diskIo && IsFileEquals(diskIo->GetFilePtr().get()));
    }
    bool IsFileEquals(const DiskIoPtr& diskIoPtr) const {
        return IsFileEquals(diskIoPtr.get());
    }
    bool SyncMeta() {
        if (mWriteMetaOpsHead || mWritesInFlight > 0) {
            return true;
        }
        if (mMetaDirtyFlag) {
            WriteChunkMetadata();
            return true;
        }
        return false;
    }
    inline void LruUpdate(ChunkLists* chunkInfoLists);
    inline void SetWriteAppenderOwns(ChunkLists* chunkInfoLists, bool flag);
    inline bool IsWriteAppenderOwns() const;
    int WriteChunkMetadata(
        KfsCallbackObj* cb,
        bool            renameFlag,
        bool            stableFlag,
        kfsSeq_t        targetVersion);
    int WriteChunkMetadata( KfsCallbackObj* cb = 0) {
        return WriteChunkMetadata(cb, false, mStableFlag,
            mStableFlag ? chunkInfo.chunkVersion : kfsSeq_t(0));
    }
    kfsSeq_t GetTargetStateAndVersion(bool& stableFlag) const {
        if (! mWriteMetaOpsTail || mRenamesInFlight <= 0) {
            stableFlag = mStableFlag;
            return chunkInfo.chunkVersion;
        }
        if (mWriteMetaOpsTail->renameFlag) {
            stableFlag = mWriteMetaOpsTail->stableFlag;
            return mWriteMetaOpsTail->targetVersion;
        }
        stableFlag = mStableFlag;
        kfsSeq_t theRet = chunkInfo.chunkVersion;
        for (const WriteChunkMetaOp*
                op = mWriteMetaOpsHead; op; op = op->next) {
            if (op->renameFlag) {
                theRet = op->targetVersion;
                stableFlag = mWriteMetaOpsTail->stableFlag;
            }
        }
        return theRet;
    }
    bool CanHaveVersion(kfsSeq_t vers) const {
        if (vers == chunkInfo.chunkVersion) {
            return true;
        }
        for (const WriteChunkMetaOp*
                op = mWriteMetaOpsHead; op; op = op->next) {
            if (op->renameFlag && vers == op->targetVersion) {
                return true;
            }
        }
        return false;
    }
    bool IsChunkReadable() const {
        return (! mWriteMetaOpsHead && mStableFlag && mWritesInFlight <= 0);
    }
    bool IsRenameInFlight() const {
        return (mRenamesInFlight > 0);
    }
    bool HasWritesInFlight() const {
        return (mWritesInFlight > 0);
    }
    bool IsStale() const {
        return (mChunkList == ChunkManager::kChunkStaleList ||
            mChunkList == ChunkManager::kChunkPendingStaleList);
    }
    bool IsKeep() const {
        return mKeepFlag;
    }
    void MakeStale(ChunkLists* chunkInfoLists, bool keepFlag) {
        if (IsStale()) {
            return;
        }
        mKeepFlag = keepFlag;
        if (mWriteAppenderOwnsFlag) {
            mWriteAppenderOwnsFlag = false;
            gAtomicRecordAppendManager.DeleteChunk(chunkInfo.chunkId);
        }
        UpdateStale(chunkInfoLists);
        // Chunk is no longer in the chunk table, no further write ops
        // completion notification will get here. Clear write op counter and
        // restart the next op if needed.
        if (mWritesInFlight > 0) {
            mWritesInFlight = 1;
            WriteDone();
        }
    }
    void UpdateStale(ChunkLists* chunkInfoLists) {
        const bool evacuateFlag = IsEvacuate();
        ChunkList::Remove(chunkInfoLists[mChunkList], *this);
        mChunkList = mRenamesInFlight > 0 ?
            ChunkManager::kChunkPendingStaleList :
            ChunkManager::kChunkStaleList;
        ChunkList::PushBack(chunkInfoLists[mChunkList], *this);
        DetachFromChunkDir(evacuateFlag);
    }
    const string& GetDirname() const       { return mChunkDir.dirname; }
    const ChunkDirInfo& GetDirInfo() const { return mChunkDir; }
    ChunkDirInfo& GetDirInfo()             { return mChunkDir; }
    bool IsBeingReplicated() const         { return mBeingReplicatedFlag; }
    void SetBeingReplicated(bool flag) {
        mBeingReplicatedFlag = flag;
    }
    void ReadStats(int status, int64_t readSize, int64_t ioTimeMicrosec) {
        if (mChunkDir.availableSpace >= 0) {
            mChunkDir.readCounters.Update(status, readSize, ioTimeMicrosec);
            mChunkDir.totalReadCounters.Update(
                status, readSize, ioTimeMicrosec);
        }
    }
    void WriteStats(int status, int64_t writeSize, int64_t ioTimeMicrosec) {
        if (mChunkDir.availableSpace >= 0) {
            mChunkDir.writeCounters.Update(status, writeSize, ioTimeMicrosec);
            mChunkDir.totalWriteCounters.Update(
                status, writeSize, ioTimeMicrosec);
        }
    }
    void UpdateDirStableCount()
    {
        if (mChunkDirList == ChunkDirInfo::kChunkDirListNone) {
            return;
        }
        int pendingReservationSizeDelta = 0;
        if (! IsStable() && IsFileOpen()) {
            if (mChunkDir.supportsSpaceReservatonFlag &&
                    mPendingSpaceReservationSize <= 0) {
                mPendingSpaceReservationSize = max(0,
                    (int)(CHUNKSIZE + KFS_CHUNK_HEADER_SIZE -
                        chunkInfo.chunkSize));
                pendingReservationSizeDelta = mPendingSpaceReservationSize;
            }
            mChunkDir.UpdateNotStableCounts(1, (int)chunkInfo.chunkSize,
                pendingReservationSizeDelta);
        } else {
            if (mPendingSpaceReservationSize > 0) {
                pendingReservationSizeDelta = -mPendingSpaceReservationSize;
                mPendingSpaceReservationSize = 0;
            }
            mChunkDir.UpdateNotStableCounts(-1, -(int)chunkInfo.chunkSize,
                pendingReservationSizeDelta);
        }
    }

private:
    bool                        mBeingReplicatedFlag:1;
    bool                        mDeleteFlag:1;
    bool                        mWriteAppenderOwnsFlag:1;
    bool                        mWaitForWritesInFlightFlag:1;
    bool                        mMetaDirtyFlag:1;
    bool                        mStableFlag:1;
    bool                        mInDoneHandlerFlag:1;
    bool                        mKeepFlag:1;
    ChunkManager::ChunkListType mChunkList:2;
    ChunkDirInfo::ChunkListType mChunkDirList:2;
    unsigned int                mRenamesInFlight:19;
    // Chunk meta data updates need to be executed in order, allow only one
    // write in flight.
    int                         mWritesInFlight;
    int                         mPendingSpaceReservationSize;
    WriteChunkMetaOp*           mWriteMetaOpsHead;
    WriteChunkMetaOp*           mWriteMetaOpsTail;
    ChunkDirInfo&               mChunkDir;
    ChunkInfoHandle*            mPrevPtr[ChunkDirInfo::kChunkInfoHDirListCount];
    ChunkInfoHandle*            mNextPtr[ChunkDirInfo::kChunkInfoHDirListCount];

    void DetachFromChunkDir(bool evacuateFlag) {
        if (mChunkDirList == ChunkDirInfo::kChunkDirListNone) {
            return;
        }
        if (! IsStable() && IsFileOpen()) {
            int pendingReservationSizeDelta = 0;
            if (0 < mPendingSpaceReservationSize) {
                pendingReservationSizeDelta = -mPendingSpaceReservationSize;
                mPendingSpaceReservationSize = 0;
            }
            mChunkDir.UpdateNotStableCounts(-1, -(int)chunkInfo.chunkSize,
                pendingReservationSizeDelta);
        }
        ChunkDirList::Remove(mChunkDir.chunkLists[mChunkDirList], *this);
        assert(mChunkDir.chunkCount > 0);
        mChunkDir.chunkCount--;
        mChunkDirList = ChunkDirInfo::kChunkDirListNone;
        if (evacuateFlag) {
            mChunkDir.ChunkEvacuateDone();
        }
    }
    int HandleChunkMetaWriteDone(int code, void *data);
    virtual ~ChunkInfoHandle() {
        if (mWriteMetaOpsHead) {
            // Object is the "client" of this op.
            die("attempt to delete chunk info handle "
                "with meta data write in flight");
        }
        if (IsFileOpen()) {
            globals().ctrOpenDiskFds.Update(-1);
        }
    }
    void UpdateState() {
        if (mInDoneHandlerFlag) {
            return;
        }
        if (mDeleteFlag || IsStale()) {
            if (! mWriteMetaOpsHead) {
                if (IsStale()) {
                    gChunkManager.UpdateStale(*this);
                } else {
                    delete this;
                }
            }
        } else {
            gChunkManager.LruUpdate(*this);
        }
    }
    friend class QCDLListOp<ChunkInfoHandle, 0>;
    friend class QCDLListOp<ChunkInfoHandle, 1>;
private:
    ChunkInfoHandle(const  ChunkInfoHandle&);
    ChunkInfoHandle& operator=(const  ChunkInfoHandle&);
};

inline ChunkInfoHandle*
ChunkManager::AddMapping(ChunkInfoHandle* cih)
{
    bool newEntryFlag = false;
    ChunkInfoHandle** const ci = mChunkTable.Insert(
        cih->chunkInfo.chunkId, cih, newEntryFlag);
    if (! ci) {
        die("add mapping failure");
        return 0; // Eliminate lint warning.
    }
    if (! newEntryFlag) {
        return *ci;
    }
    mUsedSpace += cih->chunkInfo.chunkSize;
    UpdateDirSpace(cih, cih->chunkInfo.chunkSize);
    return *ci;
}

inline bool
ChunkManager::IsInLru(const ChunkInfoHandle& cih) const
{
    return (! cih.IsStale() &&
        ChunkList::IsInList(mChunkInfoLists[kChunkLruList], cih));
}

inline void
ChunkInfoHandle::LruUpdate(ChunkInfoHandle::ChunkLists* chunkInfoLists)
{
    if (IsStale()) {
        return;
    }
    lastIOTime = globalNetManager().Now();
    if (! mWriteAppenderOwnsFlag && ! mBeingReplicatedFlag &&
            ! mWriteMetaOpsHead) {
        ChunkList::PushBack(chunkInfoLists[mChunkList], *this);
        assert(gChunkManager.IsInLru(*this));
    } else {
        ChunkList::Remove(chunkInfoLists[mChunkList], *this);
        assert(! gChunkManager.IsInLru(*this));
    }
}

inline void
ChunkInfoHandle::SetWriteAppenderOwns(
    ChunkInfoHandle::ChunkLists* chunkInfoLists, bool flag)
{
    if (mDeleteFlag || IsStale() || flag == mWriteAppenderOwnsFlag) {
        return;
    }
    mWriteAppenderOwnsFlag = flag;
    if (mWriteAppenderOwnsFlag) {
        ChunkList::Remove(chunkInfoLists[mChunkList], *this);
        assert(! gChunkManager.IsInLru(*this));
    } else {
        LruUpdate(chunkInfoLists);
    }
}

inline bool
ChunkInfoHandle::IsWriteAppenderOwns() const
{
    return mWriteAppenderOwnsFlag;
}

inline void
ChunkManager::LruUpdate(ChunkInfoHandle& cih)
{
    cih.LruUpdate(mChunkInfoLists);
}

inline void
ChunkManager::Release(ChunkInfoHandle& cih)
{
    cih.Release(mChunkInfoLists);
}

inline void
ChunkManager::DeleteSelf(ChunkInfoHandle& cih)
{
    cih.Delete(mChunkInfoLists);
}

inline void
ChunkManager::Delete(ChunkInfoHandle& cih)
{
    if (! cih.IsStale() && ! mPendingWrites.Delete(
            cih.chunkInfo.chunkId, cih.chunkInfo.chunkVersion)) {
        ostringstream os;
        os << "delete failed to cleanup pending writes: "
            " chunk: "   << cih.chunkInfo.chunkId <<
            " version: " << cih.chunkInfo.chunkVersion
        ;
        die(os.str());
    }
    DeleteSelf(cih);
}

inline bool
ChunkManager::Remove(ChunkInfoHandle& cih)
{
    if (mPendingWrites.HasChunkId(cih.chunkInfo.chunkId) ||
            mChunkTable.Erase(cih.chunkInfo.chunkId) <= 0) {
        return false;
    }
    Delete(cih);
    return true;
}

inline void
ChunkManager::UpdateStale(ChunkInfoHandle& cih)
{
    assert(cih.IsStale());
    cih.UpdateStale(mChunkInfoLists);
    RunStaleChunksQueue();
}

inline void
ChunkManager::MakeStale(ChunkInfoHandle& cih,
    bool forceDeleteFlag, bool evacuatedFlag)
{
    cih.MakeStale(mChunkInfoLists,
        (! forceDeleteFlag && ! mForceDeleteStaleChunksFlag) ||
        (evacuatedFlag && mKeepEvacuatedChunksFlag)
    );
    assert(! cih.HasWritesInFlight());
    RunStaleChunksQueue();
}

void
ChunkInfoHandle::Release(ChunkInfoHandle::ChunkLists* chunkInfoLists)
{
    chunkInfo.UnloadChecksums();
    if (! IsFileOpen()) {
        if (dataFH) {
            dataFH.reset();
        }
        return;
    }
    string errMsg;
    if (! dataFH->Close(
            chunkInfo.chunkSize + KFS_CHUNK_HEADER_SIZE,
            &errMsg)) {
        KFS_LOG_STREAM_INFO <<
            "chunk " << chunkInfo.chunkId << " close error: " << errMsg <<
        KFS_LOG_EOM;
        dataFH.reset();
    }
    if (! IsStable()) {
        UpdateDirStableCount();
    }
    KFS_LOG_STREAM_INFO <<
        "Closing chunk " << chunkInfo.chunkId << " and might give up lease" <<
    KFS_LOG_EOM;
    gLeaseClerk.RelinquishLease(chunkInfo.chunkId, chunkInfo.chunkSize);

    ChunkList::Remove(chunkInfoLists[mChunkList], *this);
    globals().ctrOpenDiskFds.Update(-1);
}

inline bool
WriteChunkMetaOp::IsRenameNeeded(const ChunkInfoHandle* cih) const
{
    return (
        renameFlag &&
        ((cih->IsStable() && cih->chunkInfo.chunkVersion != targetVersion) ||
        cih->IsStable() != stableFlag)
    );
}

int
WriteChunkMetaOp::Start(ChunkInfoHandle* cih)
{
    gChunkManager.LruUpdate(*cih);
    if (renameFlag) {
        if (! IsRenameNeeded(cih)) {
            int64_t res = 0;
            cih->HandleEvent(EVENT_DISK_RENAME_DONE, &res);
            return 0;
        }
        if (! DiskIo::Rename(
                gChunkManager.MakeChunkPathname(cih).c_str(),
                gChunkManager.MakeChunkPathname(
                    cih, stableFlag, targetVersion).c_str(),
                cih,
                &statusMsg)) {
            status = -EAGAIN;
            KFS_LOG_STREAM_ERROR <<
                Show() << " failed: " << statusMsg <<
            KFS_LOG_EOM;
        }
    } else {
        assert(diskIo);
        diskIOTime = microseconds();
        status = diskIo->Write(0, dataBuf.BytesConsumable(), &dataBuf,
            gChunkManager.IsSyncChunkHeader() &&
            (targetVersion > 0 || stableFlag));
    }
    return status;
}

int
ChunkInfoHandle::WriteChunkMetadata(
    KfsCallbackObj* cb,
    bool            renameFlag,
    bool            stableFlag,
    kfsSeq_t        targetVersion)
{
    if (renameFlag && (int)mRenamesInFlight + 1 <= 0) {
        // Overflow: too many renames in flight.
        return -ESERVERBUSY;
    }
    // If chunk is not stable and is not transitioning into stable, and there
    // are no pending ops, just assign the version and mark meta dirty.
    if (targetVersion > 0 && chunkInfo.chunkVersion != targetVersion &&
            mWritesInFlight <= 0 &&
            ! IsStable() && ! stableFlag && ! mWriteMetaOpsTail &&
            ! mInDoneHandlerFlag && IsFileOpen() &&
            ! mDeleteFlag && ! IsStale()) {
        mMetaDirtyFlag         = true;
        chunkInfo.chunkVersion = targetVersion;
        if (cb) {
            int res = 0;
            cb->HandleEvent(renameFlag ?
                EVENT_DISK_RENAME_DONE : EVENT_DISK_WROTE, &res);
        }
        UpdateState();
        return 0;
    }
    if (renameFlag) {
        // Queue the version update first, then immediately queue rename.
        // Not stable chunks on disk always have version 0.
        mMetaDirtyFlag = true;
        const int ret = WriteChunkMetadata(
            0, false, stableFlag, stableFlag ? targetVersion : kfsSeq_t(0));
        if (ret != 0) {
            return ret;
        }
    }
    DiskIo* d = 0;
    if (! renameFlag) {
        if (! mMetaDirtyFlag) {
            if (! cb) {
                return 0;
            }
            if (! mWriteMetaOpsTail) {
                assert(mRenamesInFlight <= 0);
                int res = 0;
                cb->HandleEvent(EVENT_DISK_WROTE, &res);
                UpdateState();
                return 0;
            }
        }
        if (mMetaDirtyFlag) {
            d = gChunkManager.SetupDiskIo(this, this);
            if (! d) {
                return -ESERVERBUSY;
            }
            mMetaDirtyFlag = false;
        } else {
            // Add to pending meta op to completion queue.
            assert(mWriteMetaOpsTail);
        }
    }
    WriteChunkMetaOp* const wcm = new WriteChunkMetaOp(chunkInfo.chunkId,
        cb, d, renameFlag, stableFlag, targetVersion);
    if (d) {
        const kfsSeq_t prevVersion = chunkInfo.chunkVersion;
        chunkInfo.chunkVersion = targetVersion;
        chunkInfo.Serialize(&wcm->dataBuf, gChunkManager.GetFileSystemId());
        chunkInfo.chunkVersion = prevVersion;
        const uint64_t checksum =
            ComputeBlockChecksum(&wcm->dataBuf, wcm->dataBuf.BytesConsumable());
        wcm->dataBuf.CopyIn(
            reinterpret_cast<const char*>(&checksum), (int)sizeof(checksum));
        wcm->dataBuf.ZeroFillLast();
        if ((int)KFS_CHUNK_HEADER_SIZE < wcm->dataBuf.BytesConsumable()) {
            die("invalid io buffer size");
        }
    }
    if (wcm->renameFlag) {
        mRenamesInFlight++;
        assert(mRenamesInFlight > 0);
    }
    if (mWriteMetaOpsTail) {
        assert(mWriteMetaOpsHead);
        while (mWriteMetaOpsTail->next) {
            mWriteMetaOpsTail = mWriteMetaOpsTail->next;
        }
        mWriteMetaOpsTail->next = wcm;
        mWriteMetaOpsTail = wcm;
        return 0;
    }
    assert(! mWriteMetaOpsHead);
    mWriteMetaOpsHead = wcm;
    mWriteMetaOpsTail = wcm;
    if (mWritesInFlight > 0) {
        mWaitForWritesInFlightFlag = true;
        return 0;
    }
    const int res = wcm->Start(this);
    if (res < 0) {
        mWriteMetaOpsHead = 0;
        mWriteMetaOpsTail = 0;
        delete wcm;
    }
    return (res >= 0 ? 0 : res);
}

int
ChunkInfoHandle::HandleChunkMetaWriteDone(int codeIn, void* dataIn)
{
    const bool prevInDoneHandlerFlag = mInDoneHandlerFlag;
    mInDoneHandlerFlag = true;
    int64_t res;
    int     err;
    int     code = codeIn;
    void*   data = dataIn;
    // Do not rely on compiler to unroll tail recursion, use loop.
    for (; ;) {
        assert(mWriteMetaOpsHead);
        int status = data ? *reinterpret_cast<const int*>(data) : -EIO;
        if (code == EVENT_DISK_ERROR && status >= 0) {
            status = -EIO;
        }
        if ((! mDeleteFlag && ! IsStale()) && status < 0) {
            KFS_LOG_STREAM_ERROR << mWriteMetaOpsHead->Show() <<
                " failed: status: " << status <<
                " op: status: "     << mWriteMetaOpsHead->status <<
                " msg: "            << mWriteMetaOpsHead->statusMsg <<
            KFS_LOG_EOM;
            if (! mBeingReplicatedFlag) {
                gChunkManager.ChunkIOFailed(this, status);
            }
        }
        if (0 <= mWriteMetaOpsHead->status) {
            mWriteMetaOpsHead->status = status;
        }
        if (mWriteMetaOpsHead->renameFlag) {
            assert(0 < mRenamesInFlight);
            mRenamesInFlight--;
            if (mWriteMetaOpsHead->status == 0) {
                if (code != EVENT_DISK_RENAME_DONE) {
                    ostringstream os;
                    os << "chunk meta write completion:"
                        " unexpected event code: " << code;
                    die(os.str());
                }
                const bool updateFlag =
                    mWriteMetaOpsHead->stableFlag != mStableFlag &&
                    IsFileOpen();
                mStableFlag = mWriteMetaOpsHead->stableFlag;
                chunkInfo.chunkVersion = mWriteMetaOpsHead->targetVersion;
                if (updateFlag) {
                    UpdateDirStableCount();
                }
                if (mStableFlag) {
                    mWriteAppenderOwnsFlag = false;
                    // LruUpdate below will add it back to the lru list.
                }
            }
        } else {
            const int64_t nowUsec = microseconds();
            WriteStats(status, ChunkHeaderBuffer::GetSize(), max(int64_t(0),
                nowUsec - mWriteMetaOpsHead->diskIOTime));
            mWriteMetaOpsHead->diskIOTime = nowUsec;
        }
        WriteChunkMetaOp* const cur = mWriteMetaOpsHead;
        mWriteMetaOpsHead = cur->next;
        const bool doneFlag = ! mWriteMetaOpsHead;
        if (doneFlag) {
            mWriteMetaOpsTail = 0;
        }
        cur->HandleEvent(code, data);
        if (doneFlag) {
            break;
        }
        if (mWriteMetaOpsHead->IsWaiting()) {
            // Call the completion, this op was waiting for the one that
            // just completed.
            continue;
        }
        if (mWritesInFlight > 0) {
            mWaitForWritesInFlightFlag = true;
            break;
        }
        if (mDeleteFlag || IsStale()) {
            err = -EBADF;
        } else if (status < 0) {
            err = status;
        } else if (mWriteMetaOpsHead->renameFlag &&
                ! mWriteMetaOpsHead->IsRenameNeeded(this)) {
            res  = 0;
            data = &res;
            code = EVENT_DISK_RENAME_DONE;
            continue;
        } else if (0 <= (err = mWriteMetaOpsHead->Start(this))) {
            break;
        }
        data = &err;
        code = EVENT_DISK_ERROR;
    }
    mInDoneHandlerFlag = prevInDoneHandlerFlag;
    UpdateState();
    return 0;
}

// Chunk manager implementation.
ChunkManager::ChunkManager()
    : mMaxPendingWriteLruSecs(300),
      mCheckpointIntervalSecs(120),
      mTotalSpace(int64_t(1) << 62),
      mUsedSpace(0),
      mMinFsAvailableSpace((int64_t)(CHUNKSIZE + KFS_CHUNK_HEADER_SIZE)),
      mMaxSpaceUtilizationThreshold(0.05),
      mNextCheckpointTime(0),
      mMaxOpenChunkFiles((64 << 10) - 8),
      mMaxOpenFds(1 << 10),
      mMaxClientCount(mMaxOpenFds * 2 / 3),
      mFdsPerChunk(1),
      mChunkDirs(),
      mWriteId(GetRandomSeq()), // Seed write id.
      mPendingWrites(),
      mChunkTable(),
      mMaxIORequestSize(4 << 20),
      mNextChunkDirsCheckTime(globalNetManager().Now() - 360000),
      mChunkDirsCheckIntervalSecs(120),
      mNextGetFsSpaceAvailableTime(globalNetManager().Now() - 360000),
      mGetFsSpaceAvailableIntervalSecs(25),
      mNextSendChunDirInfoTime(globalNetManager().Now() -360000),
      mSendChunDirInfoIntervalSecs(2 * 60),
      mInactiveFdsCleanupIntervalSecs(300),
      mNextInactiveFdCleanupTime(globalNetManager().Now() - 365 * 24 * 60 * 60),
      mInactiveFdFullScanIntervalSecs(2),
      mNextInactiveFdFullScanTime(globalNetManager().Now() - 365 * 24 * 60 * 60),
      mReadChecksumMismatchMaxRetryCount(0),
      mAbortOnChecksumMismatchFlag(false),
      mRequireChunkHeaderChecksumFlag(false),
      mForceDeleteStaleChunksFlag(false),
      mKeepEvacuatedChunksFlag(false),
      mStaleChunkCompletion(*this),
      mStaleChunkOpsInFlight(0),
      mMaxStaleChunkOpsInFlight(4),
      mMaxDirCheckDiskTimeouts(4),
      mChunkPlacementPendingReadWeight(0),
      mChunkPlacementPendingWriteWeight(0),
      mMaxPlacementSpaceRatio(0.2),
      mMinPendingIoThreshold(8 << 20),
      mPlacementMaxWaitingAvgUsecsThreshold(5 * 60 * 1000 * 1000),
      mAllowSparseChunksFlag(true),
      mBufferedIoFlag(false),
      mSyncChunkHeaderFlag(false),
      mCheckDirWritableFlag(true),
      mCheckDirTestWriteSize(16 << 10),
      mCheckDirWritableTmpFileName("checkdir.tmp"),
      mNullBlockChecksum(0),
      mCounters(),
      mDirChecker(),
      mCleanupChunkDirsFlag(true),
      mStaleChunksDir("lost+found"),
      mDirtyChunksDir("dirty"),
      mEvacuateFileName("evacuate"),
      mEvacuateDoneFileName(mEvacuateFileName + ".done"),
      mChunkDirLockName("lock"),
      mEvacuationInactivityTimeout(300),
      mMetaHeartbeatTime(globalNetManager().Now() - 365 * 24 * 60 * 60),
      mMetaEvacuateCount(-1),
      mMaxEvacuateIoErrors(2),
      mAvailableChunksRetryInterval(30 * 1000),
      mAllocDefaultMinTier(kKfsSTierMin),
      mAllocDefaultMaxTier(kKfsSTierMax),
      mStorageTiersPrefixes(),
      mStorageTiersSetFlag(false),
      mBufferedIoPrefixes(),
      mBufferedIoSetFlag(false),
      mDiskBufferManagerEnabledFlag(true),
      mForceVerifyDiskReadChecksumFlag(false),
      mWritePrepareReplyFlag(true),
      mCryptoKeys(globalNetManager(), 0 /* inMutexPtr */),
      mFileSystemId(-1),
      mFsIdFileNamePrefix("0-fsid-"),
      mDirCheckerIoTimeoutSec(-1),
      mDirCheckFailureSimulatorInterval(-1),
      mChunkSizeSkipHeaderVerifyFlag(false),
      mVersionChangePermitWritesInFlightFlag(true),
      mRand(),
      mChunkHeaderBuffer()
{
    mDirChecker.SetInterval(180 * 1000);
    srand48((long)globalNetManager().Now());
    for (int i = 0; i < kChunkInfoListCount; i++) {
        ChunkList::Init(mChunkInfoLists[i]);
    }
    globalNetManager().SetMaxAcceptsPerRead(4096);
}

ChunkManager::~ChunkManager()
{
    assert(mChunkTable.IsEmpty());
    globalNetManager().UnRegisterTimeoutHandler(this);
}

void
ChunkManager::Shutdown()
{
    // Force meta server connection down first.
    gMetaServerSM.Shutdown();
    mDirChecker.Stop();
    gClientManager.Shutdown();
    // Run delete queue before removing chunk table entries.
    RunStaleChunksQueue();
    for (int i = 0; ;) {
        const bool completionFlag = DiskIo::RunIoCompletion();
        if (mStaleChunkOpsInFlight <= 0) {
            break;
        }
        if (completionFlag) {
            continue;
        }
        if (++i > 1000) {
            KFS_LOG_STREAM_ERROR <<
                "ChunkManager::Shutdown pending delete timeout exceeded" <<
            KFS_LOG_EOM;
            ChunkList::Iterator it(mChunkInfoLists[kChunkStaleList]);
            ChunkInfoHandle* cih;
            while ((cih = it.Next())) {
                Delete(*cih);
            }
            break;
        }
        usleep(10000);
    }

    ScavengePendingWrites(time(0) + 2 * mMaxPendingWriteLruSecs);
    CMap tmp;
    const CMapEntry* p;
    mChunkTable.First();
    while ((p = mChunkTable.Next())) {
        ChunkInfoHandle* const cih = p->GetVal();
        if (cih->IsFileInUse()) {
            cih->SetWriteAppenderOwns(mChunkInfoLists, false);
            bool newEntryFlag = true;
            tmp.Insert(p->GetKey(), cih, newEntryFlag);
            continue;
        }
        Release(*cih);
        Delete(*cih);
    }
    mChunkTable.Clear();
    mChunkTable.Swap(tmp);
    gAtomicRecordAppendManager.Shutdown();
    for (int i = 0; ;) {
        mChunkTable.First();
        while ((p = mChunkTable.Next())) {
            ChunkInfoHandle* const cih = p->GetVal();
            if (! cih) {
                mChunkTable.Erase(p->GetKey());
                continue;
            }
            if (cih->IsFileInUse()) {
                break;
            }
            mChunkTable.Erase(p->GetKey());
            Release(*cih);
            Delete(*cih);
        }
        const bool completionFlag = DiskIo::RunIoCompletion();
        if (mChunkTable.IsEmpty()) {
            break;
        }
        if (completionFlag) {
            continue;
        }
        if (++i > 1000) {
            KFS_LOG_STREAM_ERROR <<
                "ChunkManager::Shutdown timeout exceeded" <<
            KFS_LOG_EOM;
            break;
        }
        usleep(10000);
    }
    gClientManager.Shutdown();
    DiskIo::RunIoCompletion();
    globalNetManager().UnRegisterTimeoutHandler(this);
    string errMsg;
    if (! DiskIo::Shutdown(&errMsg)) {
        KFS_LOG_STREAM_INFO <<
            "DiskIo::Shutdown failure: " << errMsg <<
        KFS_LOG_EOM;
    }
}

bool
ChunkManager::IsWriteAppenderOwns(kfsChunkId_t chunkId) const
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    return (ci && (*ci)->IsWriteAppenderOwns());
}

bool ChunkManager::sExitDebugCheckFlag = false;

void
ChunkManager::SetDirCheckerIoTimeout()
{
    int theTimeoutSec;
    if (0 < mDirCheckerIoTimeoutSec) {
        theTimeoutSec = mDirCheckerIoTimeoutSec;
    } else {
        theTimeoutSec = DiskIo::GetMaxIoTimeSec();
        if (theTimeoutSec <= 0) {
            theTimeoutSec = -1;
        } else {
            theTimeoutSec =
                (int)max(int64_t(1), int64_t(theTimeoutSec) * 3 / 4);
        }
    }
    mDirChecker.SetIoTimeout(theTimeoutSec);
}

bool
ChunkManager::SetParameters(const Properties& prop)
{
    mInactiveFdsCleanupIntervalSecs = max(0, (int)prop.getValue(
        "chunkServer.inactiveFdsCleanupIntervalSecs",
        (double)mInactiveFdsCleanupIntervalSecs));
    mInactiveFdFullScanIntervalSecs = max(0, (int)prop.getValue(
        "chunkServer.inactiveFdFullScanIntervalSecs",
        (double)mInactiveFdFullScanIntervalSecs));
    mMaxPendingWriteLruSecs = max(1, (int)prop.getValue(
        "chunkServer.maxPendingWriteLruSecs",
        (double)mMaxPendingWriteLruSecs));
    mCheckpointIntervalSecs = max(1, (int)prop.getValue(
        "chunkServer.checkpointIntervalSecs",
        (double)mCheckpointIntervalSecs));
    mChunkDirsCheckIntervalSecs = max(1, (int)prop.getValue(
        "chunkServer.chunkDirsCheckIntervalSecs",
        (double)mChunkDirsCheckIntervalSecs));
    mGetFsSpaceAvailableIntervalSecs = max(1, (int)prop.getValue(
        "chunkServer.getFsSpaceAvailableIntervalSecs",
        (double)mGetFsSpaceAvailableIntervalSecs));
    mSendChunDirInfoIntervalSecs = max(1, (int)prop.getValue(
        "chunkServer.sendChunDirInfoIntervalSecs",
        (double)mSendChunDirInfoIntervalSecs));
    mAbortOnChecksumMismatchFlag = prop.getValue(
        "chunkServer.abortOnChecksumMismatchFlag",
        mAbortOnChecksumMismatchFlag ? 1 : 0) != 0;
    mReadChecksumMismatchMaxRetryCount = prop.getValue(
        "chunkServer.readChecksumMismatchMaxRetryCount",
        mReadChecksumMismatchMaxRetryCount);
    mRequireChunkHeaderChecksumFlag = prop.getValue(
        "chunkServer.requireChunkHeaderChecksum",
        mRequireChunkHeaderChecksumFlag ? 1 : 0) != 0;
    mDirChecker.SetRequireChunkHeaderChecksumFlag(
        mRequireChunkHeaderChecksumFlag);
    const bool prevForcedeleteStaleChunksFlag = mForceDeleteStaleChunksFlag;
    mForceDeleteStaleChunksFlag = prop.getValue(
        "chunkServer.forceDeleteStaleChunks",
        mForceDeleteStaleChunksFlag ? 1 : 0) != 0;
    if (prevForcedeleteStaleChunksFlag != mForceDeleteStaleChunksFlag) {
        mDirChecker.AddSubDir(mStaleChunksDir, mForceDeleteStaleChunksFlag);
    }
    mKeepEvacuatedChunksFlag = prop.getValue(
        "chunkServer.keepEvacuatedChunksFlag",
        mKeepEvacuatedChunksFlag ? 1 : 0) != 0;
    mMaxStaleChunkOpsInFlight = prop.getValue(
        "chunkServer.maxStaleChunkOpsInFlight",
        mMaxStaleChunkOpsInFlight);
    mMaxDirCheckDiskTimeouts = prop.getValue(
        "chunkServer.maxDirCheckDiskTimeouts",
        mMaxDirCheckDiskTimeouts);
    mTotalSpace = prop.getValue(
        "chunkServer.totalSpace",
        mTotalSpace);
    mMinFsAvailableSpace = max(int64_t(CHUNKSIZE + KFS_CHUNK_HEADER_SIZE),
        prop.getValue(
            "chunkServer.minFsAvailableSpace",
            mMinFsAvailableSpace));
    mMaxSpaceUtilizationThreshold = prop.getValue(
        "chunkServer.maxSpaceUtilizationThreshold",
        mMaxSpaceUtilizationThreshold);
    mChunkPlacementPendingReadWeight = prop.getValue(
        "chunkServer.chunkPlacementPendingReadWeight",
        mChunkPlacementPendingReadWeight);
    mChunkPlacementPendingWriteWeight = prop.getValue(
        "chunkServer.chunkPlacementPendingWriteWeight",
        mChunkPlacementPendingWriteWeight);
    mMinPendingIoThreshold = prop.getValue(
        "chunkServer.minPendingIoThreshold",
        mMinPendingIoThreshold);
    mPlacementMaxWaitingAvgUsecsThreshold = (int64_t)(1e6 * prop.getValue(
        "chunkServer.placementMaxWaitingAvgSecsThreshold",
        (double)mPlacementMaxWaitingAvgUsecsThreshold * 1e-6));
    mMaxPlacementSpaceRatio = prop.getValue(
        "chunkServer.maxPlacementSpaceRatio",
        mMaxPlacementSpaceRatio);
    mAllowSparseChunksFlag = prop.getValue(
        "chunkServer.allowSparseChunks",
        mAllowSparseChunksFlag ? 1 : 0) != 0;
    mBufferedIoFlag = prop.getValue(
        "chunkServer.bufferedIo",
        mBufferedIoFlag ? 1 : 0) != 0;
    mSyncChunkHeaderFlag = prop.getValue(
        "chunkServer.syncChunkHeader",
        mSyncChunkHeaderFlag ? 1 : 0) != 0;
    mCheckDirWritableFlag = prop.getValue(
        "chunkServer.checkDirWritableFlag",
        mCheckDirWritableFlag ? 1 : 0) != 0;
    mCheckDirTestWriteSize = prop.getValue(
        "chunkServer.checkDirTestWriteSize",
        mCheckDirTestWriteSize);
    mCheckDirWritableTmpFileName = prop.getValue(
        "chunkserver.checkDirWritableTmpFileName",
        mCheckDirWritableTmpFileName);
    if (mCheckDirWritableTmpFileName.empty()) {
        mCheckDirWritableTmpFileName = "checkdir.tmp";
    }
    mEvacuateFileName = prop.getValue(
        "chunkServer.evacuateFileName",
        mEvacuateFileName);
    mEvacuateDoneFileName = prop.getValue(
        "chunkServer.evacuateDoneFileName",
        mEvacuateDoneFileName);
    mEvacuationInactivityTimeout = prop.getValue(
        "chunkServer.evacuationInactivityTimeout",
        mEvacuationInactivityTimeout);
    mDirChecker.SetInterval(prop.getValue(
        "chunkServer.dirRecheckInterval",
        mDirChecker.GetInterval() / 1000) * 1000);
    mDirChecker.SetMaxChunkFilesSampled(prop.getValue(
        "chunkServer.dirCheckMaxChunkFilesSampled",
        mDirChecker.GetMaxChunkFilesSampled()));
    mCleanupChunkDirsFlag = prop.getValue(
        "chunkServer.cleanupChunkDirs",
        mCleanupChunkDirsFlag);
    mDirChecker.SetRemoveFilesFlag(mCleanupChunkDirsFlag);

    TcpSocket::SetDefaultRecvBufSize(prop.getValue(
        "chunkServer.tcpSocket.recvBufSize",
        TcpSocket::GetDefaultRecvBufSize()));
    TcpSocket::SetDefaultSendBufSize(prop.getValue(
        "chunkServer.tcpSocket.sendBufSize",
        TcpSocket::GetDefaultSendBufSize()));

    globalNetManager().SetMaxAcceptsPerRead(prop.getValue(
        "chunkServer.net.maxAcceptsPerRead",
        globalNetManager().GetMaxAcceptsPerRead()));

    DiskIo::SetParameters(prop);
    Replicator::SetParameters(prop);

    mMaxClientCount = min(mMaxOpenFds * 2 / 3, prop.getValue(
        "chunkServer.client.maxClientCount", mMaxClientCount));
    bool ret = gClientManager.SetParameters(
        "chunkServer.client.",
        prop,
        gMetaServerSM.IsAuthEnabled(),
        mMaxClientCount
    );
    ret = RemoteSyncSM::SetParameters(
        "chunkServer.remoteSync.", prop, gMetaServerSM.IsAuthEnabled()) && ret;
    mMaxEvacuateIoErrors = max(1, prop.getValue(
        "chunkServer.maxEvacuateIoErrors",
        mMaxEvacuateIoErrors
    ));
    mAvailableChunksRetryInterval = max(1000, (int)(prop.getValue(
        "chunkServer.availableChunksRetryInterval",
        (double)mAvailableChunksRetryInterval / 1000) * 1000.));

    DirChecker::FileNames names;
    names.insert(mEvacuateDoneFileName);
    mDirChecker.SetDontUseIfExist(names);
    names.clear();
    if (! mEvacuateFileName.empty()) {
        names.insert(mEvacuateFileName);
    }
    if (! mCheckDirWritableTmpFileName.empty()) {
        names.insert(mCheckDirWritableTmpFileName);
    }
    mDirChecker.SetIgnoreFileNames(names);

    gAtomicRecordAppendManager.SetParameters(prop);

    const time_t now = globalNetManager().Now();
    mNextGetFsSpaceAvailableTime = min(mNextGetFsSpaceAvailableTime,
        now + mGetFsSpaceAvailableIntervalSecs);
    mNextChunkDirsCheckTime = min(mNextChunkDirsCheckTime,
        now + mChunkDirsCheckIntervalSecs);
    mNextSendChunDirInfoTime = min(mNextSendChunDirInfoTime,
        now + mSendChunDirInfoIntervalSecs);
    mNextInactiveFdFullScanTime = min(mNextInactiveFdFullScanTime,
        now + mInactiveFdFullScanIntervalSecs);
    mAllocDefaultMinTier = prop.getValue(
        "chunkServer.allocDefaultMinTier", mAllocDefaultMinTier);
    mAllocDefaultMaxTier = prop.getValue(
        "chunkServer.allocDefaultMaxTier", mAllocDefaultMaxTier);
    mDiskBufferManagerEnabledFlag = prop.getValue(
        "chunkServer.disk.bufferManager.enabled",
        mDiskBufferManagerEnabledFlag ? 1 : 0) != 0;
    mForceVerifyDiskReadChecksumFlag = prop.getValue(
        "chunkServer.forceVerifyDiskReadChecksum",
        mForceVerifyDiskReadChecksumFlag ? 1 : 0) != 0;
    mWritePrepareReplyFlag = prop.getValue(
        "chunkServer.debugTestWriteSync",
        mWritePrepareReplyFlag ? 0 : 1) == 0;
    mFsIdFileNamePrefix = prop.getValue(
        "chunkServer.fsIdFileNamePrefix", mFsIdFileNamePrefix);
    mDirCheckerIoTimeoutSec = prop.getValue(
        "chunkServer.dirCheckerIoTimeoutSec", mDirCheckerIoTimeoutSec);
    mDirCheckFailureSimulatorInterval = prop.getValue(
        "chunkServer.dirCheckFailureSimulatorInterval",
        mDirCheckFailureSimulatorInterval);
    mChunkSizeSkipHeaderVerifyFlag = prop.getValue(
        "chunkServer.chunkSizeSkipHeaderVerifyFlag",
        mChunkSizeSkipHeaderVerifyFlag ? 1 : 0) != 0;
    mVersionChangePermitWritesInFlightFlag = prop.getValue(
        "chunkServer.versionChangePermitWritesInFlight",
        mVersionChangePermitWritesInFlightFlag ? 1 : 0) != 0;
    mDirChecker.SetFsIdPrefix(mFsIdFileNamePrefix);
    SetDirCheckerIoTimeout();
    ClientSM::SetParameters(prop);
    SetStorageTiers(prop);
    SetBufferedIo(prop);
    string errMsg;
    const int err = mCryptoKeys.SetParameters(
        "chunkServer.cryptoKeys.", prop, errMsg);
    if (err) {
        KFS_LOG_STREAM_ERROR <<
            "failed to set crypto keys parameters: status: " << err <<
            " " << errMsg <<
        KFS_LOG_EOM;
    }
    if (! mCryptoKeys.IsCurrentKeyValid()) {
        KFS_LOG_STREAM_ERROR <<
            "no valid current crypto key" <<
        KFS_LOG_EOM;
        ret = false;
    } else {
        ret = ret && err == 0;
    }
    sExitDebugCheckFlag = prop.getValue(
        "chunkServer.exitDebugCheck", sExitDebugCheckFlag ? 1 : 0);
    return ret;
}

void
ChunkManager::SetStorageTiers(const Properties& props)
{
    const string prevPrefixes = mStorageTiersPrefixes;
    mStorageTiersPrefixes = props.getValue(
        "chunkServer.storageTierPrefixes", mStorageTiersPrefixes);
    if (prevPrefixes == mStorageTiersPrefixes && mStorageTiersSetFlag) {
        return;
    }
    if (mChunkDirs.empty()) {
        mStorageTiersSetFlag = false;
        return;
    }
    mStorageTiers.clear();
    for (ChunkDirs::iterator it = mChunkDirs.begin();
            it < mChunkDirs.end();
            ++it) {
        it->storageTier = kKfsSTierMax;
    }
    istringstream is(mStorageTiersPrefixes);
    string prefix;
    int    tier;
    while ((is >> prefix >> tier)) {
        if (tier == kKfsSTierUndef) {
            continue;
        }
        if (tier < kKfsSTierMin) {
            tier = kKfsSTierMin;
        }
        if (tier > kKfsSTierMax) {
            tier = kKfsSTierMax;
        }
        for (ChunkDirs::iterator it = mChunkDirs.begin();
                it != mChunkDirs.end();
                ++it) {
            const string& dirname = it->dirname;
            if (prefix.length() <= dirname.length() &&
                    dirname.compare(0, prefix.length(), prefix) == 0) {
                it->storageTier = (kfsSTier_t)tier;
            }
        }
    }
    for (ChunkDirs::iterator it = mChunkDirs.begin();
            it < mChunkDirs.end();
            ++it) {
        if (it->availableSpace < 0) {
            continue;
        }
        mStorageTiers[it->storageTier].push_back(&(*it));
    }
    mStorageTiersSetFlag = true;
}

void
ChunkManager::SetBufferedIo(const Properties& props)
{
    const string prevPrefixes = mBufferedIoPrefixes;
    mBufferedIoPrefixes = props.getValue(
        "chunkServer.bufferedIoDirPrefixes", mBufferedIoPrefixes);
    if (prevPrefixes == mBufferedIoPrefixes && mBufferedIoSetFlag) {
        return;
    }
    if (mChunkDirs.empty()) {
        mBufferedIoSetFlag = false;
        return;
    }
    istringstream is(mBufferedIoPrefixes);
    string prefix;
    set<string> prefixes;
    while ((is >> prefix)) {
        prefixes.insert(prefix);
    }
    for (ChunkDirs::iterator it = mChunkDirs.begin();
            it != mChunkDirs.end();
            ++it) {
        const string& dirname = it->dirname;
        set<string>::const_iterator pit = prefixes.begin();
        for (; pit != prefixes.end(); ++pit) {
            const string& prefix = *pit;
            if (prefix.length() <= dirname.length() &&
                    dirname.compare(0, prefix.length(), prefix) == 0) {
                break;
            }
        }
        const bool bufferedIoFlag = pit != prefixes.end();
        if (bufferedIoFlag != it->bufferedIoFlag) {
            it->bufferedIoFlag = bufferedIoFlag;
            if (it->availableSpace < 0 && ! it->dirLock) {
                mDirChecker.Add(it->dirname, it->availableSpace);
            }
        }
    }
    mBufferedIoSetFlag = true;
}

static string AddTrailingPathSeparator(const string& dir)
{
    return ((! dir.empty() && dir[dir.length() - 1] != '/') ?
        dir + "/" : dir);
}

struct EqualPrefixStr : public binary_function<string, string, bool>
{
    bool operator()(const string& x, const string& y) const
    {
        return x.compare(0, min(x.length(), y.length()), y) == 0;
    }
};

bool
ChunkManager::Init(const vector<string>& chunkDirs, const Properties& prop)
{
    if (chunkDirs.empty()) {
        KFS_LOG_STREAM_ERROR <<
            "no chunk directories specified" <<
        KFS_LOG_EOM;
        return false;
    }

    // allow to change dir names only before io starts.
    mStaleChunksDir = prop.getValue(
        "chunkServer.staleChunksDir",
        mStaleChunksDir);
    mDirtyChunksDir = prop.getValue(
        "chunkServer.dirtyChunksDir",
        mDirtyChunksDir);
    mChunkDirLockName = prop.getValue(
        "chunkServer.dirLockFileName",
        mChunkDirLockName);
    if (mStaleChunksDir.empty() || mStaleChunksDir.find('/') != string::npos) {
        KFS_LOG_STREAM_ERROR <<
            "invalid stale chunks dir name: " << mStaleChunksDir <<
        KFS_LOG_EOM;
        return false;
    }
    if (mDirtyChunksDir.empty() || mDirtyChunksDir.find('/') != string::npos) {
        KFS_LOG_STREAM_ERROR <<
            "invalid stale chunks dir name: " << mDirtyChunksDir <<
        KFS_LOG_EOM;
        return false;
    }
    mStaleChunksDir = AddTrailingPathSeparator(mStaleChunksDir);
    mDirtyChunksDir = AddTrailingPathSeparator(mDirtyChunksDir);

    mMaxOpenFds = SetMaxNoFileLimit();
    mMaxClientCount = mMaxOpenFds * 2 / 3;
    KFS_LOG_STREAM_INFO <<
        " max open files: "           << mMaxOpenFds <<
        " default max client count: " << mMaxClientCount <<
    KFS_LOG_EOM;
    if (! SetParameters(prop)) {
        return false;
    }

    // Normalize tailing /, and keep only longest prefixes:
    // only leave leaf directories.
    const size_t kMaxDirNameLength = MAX_RPC_HEADER_LEN / 3;
    vector<string> dirs;
    dirs.reserve(chunkDirs.size());
    for (vector<string>::const_iterator it = chunkDirs.begin();
            it < chunkDirs.end();
            ++it) {
        if (it->empty()) {
            continue;
        }
        string dir = *it;
        size_t pos = dir.length();
        while (pos > 1 && dir[pos - 1] == '/') {
            --pos;
        }
        if (++pos < dir.length()) {
            dir.erase(pos);
        }
        dir = AddTrailingPathSeparator(dir);
        if (dir.length() > kMaxDirNameLength) {
            KFS_LOG_STREAM_ERROR <<
                dir << ": chunk directory name exceeds"
                    " character length limit of " << kMaxDirNameLength <<
            KFS_LOG_EOM;
            return false;
        }
        dirs.push_back(dir);
    }
    sort(dirs.begin(), dirs.end(), greater<string>());
    size_t cnt = unique(dirs.begin(), dirs.end(), EqualPrefixStr()) -
        dirs.begin();
    mChunkDirs.Allocate(cnt);
    vector<string>::const_iterator di = dirs.begin();
    for (ChunkDirs::iterator it = mChunkDirs.begin();
            it < mChunkDirs.end();
            ++it, ++di) {
        it->dirname     = *di;
        it->storageTier = kKfsSTierMax;
    }
    SetStorageTiers(prop);
    SetBufferedIo(prop);

    string errMsg;
    if (! DiskIo::Init(prop, &errMsg)) {
        KFS_LOG_STREAM_ERROR <<
            "DiskIo::Init failure: " << errMsg <<
        KFS_LOG_EOM;
        return false;
    }
    const int kMinOpenFds = 32;
    if (mMaxOpenFds < kMinOpenFds) {
        KFS_LOG_STREAM_ERROR <<
            "file descriptor limit too small: " << mMaxOpenFds <<
        KFS_LOG_EOM;
        return false;
    }
    mFdsPerChunk = DiskIo::GetFdCountPerFile();
    if (mFdsPerChunk < 1) {
        KFS_LOG_STREAM_ERROR <<
            "invalid fd count per chunk: " << mFdsPerChunk <<
        KFS_LOG_EOM;
        return false;
    }
    mMaxOpenChunkFiles = min((mMaxOpenFds - kMinOpenFds / 2) / mFdsPerChunk,
        prop.getValue("chunkServer.maxOpenChunkFiles", mMaxOpenChunkFiles));
    TcpSocket::SetOpenLimit(mMaxOpenFds - min((mMaxOpenFds + 3) / 4, 1 << 10));
    if (mMaxOpenChunkFiles < kMinOpenFds / 2) {
        KFS_LOG_STREAM_ERROR <<
            "open chunks limit too small: " << mMaxOpenChunkFiles <<
        KFS_LOG_EOM;
        return false;
    }
    {
        IOBuffer buf;
        buf.ZeroFill((int)CHECKSUM_BLOCKSIZE);
        mNullBlockChecksum = ComputeBlockChecksum(&buf, buf.BytesConsumable());
    }
    // force a stat of the dirs and update space usage counts
    return StartDiskIo();
}

int
ChunkManager::AllocChunk(
    kfsFileId_t       fileId,
    kfsChunkId_t      chunkId,
    kfsSeq_t          chunkVersion,
    kfsSTier_t        minTier,
    kfsSTier_t        maxTier,
    bool              isBeingReplicated,
    ChunkInfoHandle** outCih,
    bool              mustExistFlag,
    AllocChunkOp*     op                            /* = 0 */,
    kfsSeq_t          chunkReplicationTargetVersion /* = -1 */,
    DiskIo::FilePtr*  outFileHandle                 /* = 0 */)
{
    ChunkInfoHandle** const cie = mChunkTable.Find(chunkId);
    if (cie) {
        ChunkInfoHandle* const cih = *cie;
        if (isBeingReplicated) {
            if (mustExistFlag || cih->IsBeingReplicated()) {
                // Replicator must cancel the replication prior to allocation
                // invocation, and set must exist to false.
                return -EINVAL;
            }
            if (0 <= chunkReplicationTargetVersion &&
                    cih->IsChunkReadable() &&
                    cih->CanHaveVersion(chunkReplicationTargetVersion)) {
                return -EEXIST;
            }
            const bool forceDeleteFlag = true;
            StaleChunk(cih, forceDeleteFlag);
        } else {
            if (cih->IsBeingReplicated() || cih->IsStable() ||
                    cih->IsWriteAppenderOwns() ||
                    cih->chunkInfo.chunkVersion != chunkVersion) {
                return -EINVAL;
            }
            if (outCih) {
                *outCih = cih;
            }
            return 0;
        }
    } else if (mustExistFlag) {
        return -EBADF;
    }

    // Find the directory to use
    ChunkDirInfo* const chunkdir = GetDirForChunk(
        minTier == kKfsSTierUndef ? mAllocDefaultMinTier : minTier,
        maxTier == kKfsSTierUndef ? mAllocDefaultMaxTier : maxTier
    );
    if (! chunkdir) {
        KFS_LOG_STREAM_INFO <<
            "no directory has space to host chunk " << chunkId <<
        KFS_LOG_EOM;
        return -ENOSPC;
    }

    // Chunks are dirty until they are made stable: A chunk becomes
    // stable when the write lease on the chunk expires and the
    // metaserver says the chunk is now stable.  Dirty chunks are
    // stored in a "dirty" dir; chunks in this dir will get nuked
    // on a chunkserver restart.  This provides a very simple failure
    // handling model.

    const bool stableFlag = false;
    ChunkInfoHandle* const cih = new ChunkInfoHandle(*chunkdir, stableFlag);
    cih->chunkInfo.Init(fileId, chunkId, chunkVersion);
    cih->SetBeingReplicated(isBeingReplicated);
    cih->SetMetaDirty();
    bool newEntryFlag = false;
    if (! mChunkTable.Insert(chunkId, cih, newEntryFlag) || ! newEntryFlag) {
        die("chunk insertion failure");
        cih->Delete(mChunkInfoLists);
        return -EFAULT;
    }
    KFS_LOG_STREAM_INFO << "Creating chunk: " << MakeChunkPathname(cih) <<
    KFS_LOG_EOM;
    int ret = OpenChunk(cih, O_RDWR | O_CREAT);
    if (ret < 0) {
        // open chunk failed: the entry in the chunk table is cleared and
        // Delete(*cih) is also called in OpenChunk().  Return the
        // error code
        return ret;
    }
    if (! cih->IsFileOpen()) {
        die("chunk is not open after successful OpenChunk invocation");
        return -EFAULT;
    }
    if (outCih) {
        *outCih = cih;
    }
    if (ret == 0 && op && ! op->diskIo) {
        op->diskIo.reset(SetupDiskIo(cih, op));
        const int status = op->diskIo ?
            op->diskIo->CheckOpenStatus() : -ESERVERBUSY;
        if (status != 0) {
            op->diskIo.reset();
            const bool forceDeleteFlag = true;
            StaleChunk(cih, forceDeleteFlag);
            return status;
        }
    }
    if (ret == 0 && outFileHandle) {
        *outFileHandle = cih->dataFH;
    }
    return ret;
}

void
ChunkManager::AllocChunkForAppend(
    AllocChunkOp*         op,
    int                   replicationPos,
    const ServerLocation& peerLoc)
{
    if (IsWritePending(op->chunkId)) {
        op->statusMsg = "random write in progress";
        op->status = -EINVAL;
    }
    const bool kIsBeingReplicatedFlag = false;
    ChunkInfoHandle *cih = 0;
    op->status = AllocChunk(
        op->fileId,
        op->chunkId,
        op->chunkVersion,
        op->minStorageTier,
        op->maxStorageTier,
        kIsBeingReplicatedFlag,
        &cih,
        op->mustExistFlag,
        op
    );
    if (op->status != 0) {
        return;
    }
    assert(cih);
    gAtomicRecordAppendManager.AllocateChunk(
        op, replicationPos, peerLoc, cih->dataFH);
    if (op->status == 0) {
        cih->SetWriteAppenderOwns(mChunkInfoLists, true);
    }
}

bool
ChunkManager::IsChunkStable(const ChunkInfoHandle* cih) const
{
    return (
        cih->IsStable() &&
        (! cih->IsWriteAppenderOwns() ||
            gAtomicRecordAppendManager.IsChunkStable(cih->chunkInfo.chunkId)) &&
        ! IsWritePending(cih->chunkInfo.chunkId) &&
        ! cih->IsBeingReplicated()
    );
}

bool
ChunkManager::IsChunkStable(kfsChunkId_t chunkId) const
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    return (! ci || IsChunkStable(*ci));
}

bool
ChunkManager::IsChunkReadable(kfsChunkId_t chunkId) const
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    return (! ci || (IsChunkStable(*ci) && (*ci)->IsChunkReadable()));
}

bool
ChunkManager::IsChunkStable(MakeChunkStableOp* op)
{
    if (op->hasChecksum) {
        return false; // Have to run make stable to compare the checksum.
    }
    ChunkInfoHandle** const ci = mChunkTable.Find(op->chunkId);
    if (! ci) {
        op->statusMsg = "no such chunk";
        op->status    = -EBADF;
        return true;
    }
    // See if it have to wait until the chunk becomes readable.
    ChunkInfoHandle* const cih = *ci;
    return (op->chunkVersion == cih->chunkInfo.chunkVersion &&
        IsChunkStable(cih) && cih->IsChunkReadable());
}

int
ChunkManager::MakeChunkStable(kfsChunkId_t chunkId, kfsSeq_t chunkVersion,
    bool appendFlag, KfsCallbackObj* cb, string& statusMsg)
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    if (! ci) {
        statusMsg = "no such chunk";
        return -EBADF;
    }
    ChunkInfoHandle* const cih = *ci;
    assert(cih);
    bool stableFlag = false;
    if (cih->IsRenameInFlight()) {
        if (chunkVersion != cih->GetTargetStateAndVersion(stableFlag)) {
            statusMsg = (stableFlag ? "" : "not ");
            statusMsg += "stable target version mismatch";
            return -EINVAL;
        }
    } else if (chunkVersion != cih->chunkInfo.chunkVersion) {
        statusMsg = "version mismatch";
        return -EINVAL;
    }
    if (cih->IsBeingReplicated()) {
        statusMsg = "chunk replication is in progress";
        return -EINVAL;
    }
    if (! cih->chunkInfo.chunkBlockChecksum) {
        statusMsg = "checksum are not loaded";
        return -EAGAIN;
    }
    if ((appendFlag ?
            ! cih->IsWriteAppenderOwns() :
            (cih->IsWriteAppenderOwns() &&
                ! gAtomicRecordAppendManager.IsChunkStable(chunkId)))) {
        ostringstream os;
        os << "make stable invalid state: "
            " chunk: "        << chunkId <<
            " version: " << cih->chunkInfo.chunkVersion <<
                "/" << chunkVersion <<
            " append: "       << appendFlag <<
            " appender owns:" << cih->IsWriteAppenderOwns()
        ;
        die(os.str());
    }
    if (! mPendingWrites.Delete(chunkId, cih->chunkInfo.chunkVersion)) {
        ostringstream os;
        os << "make stable failed to cleanup pending writes: "
            " chunk: "   << chunkId <<
            " version: " << cih->chunkInfo.chunkVersion
        ;
        die(os.str());
    }
    stableFlag = true;
    const bool renameFlag = true;
    const int  res        = cih->WriteChunkMetadata(
        cb, renameFlag, stableFlag, cih->chunkInfo.chunkVersion);
    if (res < 0) {
        statusMsg = "failed to start chunk meta data write";
    }
    return res;
}

int
ChunkManager::DeleteChunk(kfsChunkId_t chunkId)
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    if (! ci) {
        return -EBADF;
    }
    KFS_LOG_STREAM_INFO << "deleting chunk: " << chunkId <<
    KFS_LOG_EOM;
    const bool forceDeleteFlag = true;
    return StaleChunk(*ci, forceDeleteFlag);
}

void
ChunkManager::DumpChunkMap()
{
    ofstream ofs;
    ofs.open("chunkdump.txt");
    if (ofs) {
        DumpChunkMap(ofs);
    }
    ofs.flush();
    ofs.close();
}

void
ChunkManager::DumpChunkMap(ostream &ofs)
{
   // Dump chunk map in the format of
   // chunkID fileID chunkSize
    mChunkTable.First();
    const CMapEntry* p;
    while ((p = mChunkTable.Next())) {
        ChunkInfoHandle* const cih = p->GetVal();
        ofs << cih->chunkInfo.chunkId <<
            " " << cih->chunkInfo.fileId <<
            " " << cih->chunkInfo.chunkSize <<
        "\n";
   }
}

int
ChunkManager::WriteChunkMetadata(
    kfsChunkId_t chunkId, KfsCallbackObj* cb, bool forceFlag /* = false */)
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    if (! ci) {
        return -EBADF;
    }
    if (forceFlag) {
        (*ci)->SetMetaDirty();
    }
    return (*ci)->WriteChunkMetadata(cb);
}

int
ChunkManager::ReadChunkMetadata(kfsChunkId_t chunkId, KfsOp* cb)
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    if (! ci) {
        return -EBADF;
    }
    ChunkInfoHandle* const cih = *ci;
    if (cih->IsBeingReplicated()) {
        KFS_LOG_STREAM_ERROR <<
            "denied meta data read for chunk: " << chunkId <<
            " replication is in flight" <<
        KFS_LOG_EOM;
        return -EBADF;
    }

    LruUpdate(*cih);
    if (cih->chunkInfo.AreChecksumsLoaded()) {
        int res = 0;
        cb->HandleEvent(EVENT_CMD_DONE, &res);
        return 0;
    }

    if (cih->readChunkMetaOp) {
        // if we have issued a read request for this chunk's metadata,
        // don't submit another one; otherwise, we will simply drive
        // up memory usage for useless IO's
        cih->readChunkMetaOp->AddWaiter(cb);
        return 0;
    }

    ReadChunkMetaOp* const rcm = new ReadChunkMetaOp(chunkId, cb);
    DiskIo*          const d   = SetupDiskIo(cih, rcm);
    if (! d) {
        delete rcm;
        return -ESERVERBUSY;
    }
    rcm->diskIo.reset(d);

    const int res = rcm->diskIo->Read(0, KFS_CHUNK_HEADER_SIZE);
    if (res < 0) {
        cih->ReadStats(res, (int64_t)KFS_CHUNK_HEADER_SIZE, 0);
        ReportIOFailure(cih, res);
        delete rcm;
        return res;
    }
    cih->readChunkMetaOp = rcm;
    return 0;
}

void
ChunkManager::ReadChunkMetadataDone(ReadChunkMetaOp* op, IOBuffer* dataBuf)
{
    ChunkInfoHandle** const ci = mChunkTable.Find(op->chunkId);
    if (! ci) {
        if (op->status == 0) {
            op->status    = -EBADF;
            op->statusMsg = "no such chunk";
            KFS_LOG_STREAM_ERROR <<
                "chunk meta data read completion: " <<
                    op->statusMsg  << " " << op->Show() <<
            KFS_LOG_EOM;
        }
        return;
    }
    ChunkInfoHandle* const cih = *ci;
    if (op != cih->readChunkMetaOp) {
        if (op->status >= 0) {
            op->status    = -EAGAIN;
            op->statusMsg = "stale meta data read";
        }
        KFS_LOG_STREAM_ERROR <<
            "chunk meta data read completion: " <<
                op->statusMsg  << " " << op->Show() <<
        KFS_LOG_EOM;
        return;
    }
    int res;
    if (! dataBuf ||
            dataBuf->BytesConsumable() < (int)KFS_CHUNK_HEADER_SIZE ||
            dataBuf->CopyOut(mChunkHeaderBuffer.GetPtr(),
                    mChunkHeaderBuffer.GetSize()) !=
                mChunkHeaderBuffer.GetSize()) {
        if (op->status != -ETIMEDOUT) {
            op->status    = -EIO;
            op->statusMsg = "short chunk meta data read";
        } else {
            op->statusMsg = "read timed out";
        }
        KFS_LOG_STREAM_ERROR <<
            "chunk meta data read completion: " << op->statusMsg  <<
            " " << (dataBuf ? dataBuf->BytesConsumable() : 0) <<
            " " << op->Show() <<
        KFS_LOG_EOM;
    } else {
        const DiskChunkInfo_t&  dci     =
            *reinterpret_cast<const DiskChunkInfo_t*>(
                mChunkHeaderBuffer.GetPtr());
        const uint64_t&        checksum =
            *reinterpret_cast<const uint64_t*>(&dci + 1);
        uint32_t               headerChecksum = 0;
        if ((checksum != 0 || mRequireChunkHeaderChecksumFlag) &&
                (headerChecksum = ComputeBlockChecksum(
                    mChunkHeaderBuffer.GetPtr(), sizeof(dci))) != checksum) {
            op->status    = -EBADCKSUM;
            op->statusMsg = "chunk header checksum mismatch";
            ostringstream os;
            os << "chunk meta data read completion: " << op->statusMsg  <<
                " expected: " << checksum <<
                " computed: " << headerChecksum  <<
                " " << op->Show()
            ;
            const string str = os.str();
            KFS_LOG_STREAM_ERROR << str << KFS_LOG_EOM;
            if (mAbortOnChecksumMismatchFlag) {
                die(str);
            }
        } else if ((res = dci.Validate(op->chunkId, cih->IsStable() ?
                cih->chunkInfo.chunkVersion : kfsSeq_t(0))) < 0) {
            op->status    = res;
            op->statusMsg = "chunk metadata validation mismatch";
            KFS_LOG_STREAM_ERROR <<
                "chunk meta data read completion: " << op->statusMsg  <<
                " " << op->Show() <<
            KFS_LOG_EOM;
        } else {
            cih->chunkInfo.SetChecksums(dci.chunkBlockChecksum);
            if (cih->chunkInfo.chunkSize > (int64_t)dci.chunkSize) {
                const int64_t extra = cih->chunkInfo.chunkSize - dci.chunkSize;
                mUsedSpace -= extra;
                UpdateDirSpace(cih, -extra);
                cih->chunkInfo.chunkSize = dci.chunkSize;
            } else if (cih->chunkInfo.chunkSize != (int64_t)dci.chunkSize) {
                op->status    = -EIO;
                op->statusMsg = "chunk metadata size mismatch";
                KFS_LOG_STREAM_ERROR <<
                    "chunk meta data read completion: " << op->statusMsg  <<
                    " file: " << cih->chunkInfo.chunkSize <<
                    " meta: " << dci.chunkSize <<
                    " " << op->Show() <<
                KFS_LOG_EOM;
            }
        }
        if (0 <= op->status && 0 < mFileSystemId) {
            const int64_t fsId = dci.GetFsId();
            if (0 < fsId && mFileSystemId != fsId) {
                op->status    = -EINVAL;
                op->statusMsg = "file system id mismatch";
                KFS_LOG_STREAM_ERROR <<
                    "chunk meta data read completion: " << op->statusMsg  <<
                    " file: "   << fsId <<
                    " expect: " << mFileSystemId <<
                    " " << op->Show() <<
                KFS_LOG_EOM;
            }
        }
    }
    LruUpdate(*cih);
    cih->readChunkMetaOp = 0;
    cih->ReadStats(op->status, (int64_t)KFS_CHUNK_HEADER_SIZE,
        max(int64_t(1), microseconds() - op->startTime));
    if (op->status < 0 && op->status != -ETIMEDOUT) {
        mCounters.mBadChunkHeaderErrorCount++;
        ChunkIOFailed(cih, op->status);
    }
}

bool
ChunkManager::IsChunkMetadataLoaded(kfsChunkId_t chunkId)
{
    ChunkInfoHandle *cih = 0;
    return (
        GetChunkInfoHandle(chunkId, &cih) >= 0 &&
        cih->chunkInfo.AreChecksumsLoaded()
    );
}

ChunkInfo_t*
ChunkManager::GetChunkInfo(kfsChunkId_t chunkId)
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    return (ci ? &((*ci)->chunkInfo) : 0);
}

int
ChunkManager::MarkChunkStale(ChunkInfoHandle* cih, KfsCallbackObj* cb)
{
    const string s                  = MakeChunkPathname(cih);
    const string staleChunkPathname = MakeStaleChunkPathname(cih);
    string err;
    const int ret = DiskIo::Rename(
        s.c_str(), staleChunkPathname.c_str(), cb, &err) ? 0 : -1;
    KFS_LOG_STREAM_INFO <<
        "Moving chunk " << cih->chunkInfo.chunkId <<
        " to staleChunks dir " << staleChunkPathname <<
        (ret == 0 ? " ok" : " error:") << err <<
    KFS_LOG_EOM;
    return ret;
}

int
ChunkManager::StaleChunk(kfsChunkId_t chunkId,
    bool forceDeleteFlag, bool evacuatedFlag, kfsSeq_t availChunksSeq)
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    if (! ci) {
        return -EBADF;
    }
    ChunkInfoHandle* const cih = *ci;
    if (! cih) {
        die("null chunk table entry");
        return -EFAULT;
    }
    ChunkDirInfo& dir = cih->GetDirInfo();
    if (dir.availableChunksOpInFlightFlag &&
            dir.availableChunksOp.seq != availChunksSeq) {
        // The following condition and correspond warning trace message relies
        // on the protocol message order where the chunks available reply would
        // normally arrive *after* "related" stale chunks rpc requests.
        // Presently the message can arrive after the chunk available RPC reply
        // in the case of chunk server re-authentication.
        if (0 <= availChunksSeq) {
            KFS_LOG_STREAM_NOTICE <<
                "detected possible available chunks message reording:"
                " received: " << availChunksSeq <<
                " current: "  << dir.availableChunksOp.seq <<
            KFS_LOG_EOM;
        }
        if (! dir.chunksAvailableInFlightSortedFlag) {
            dir.chunksAvailableInFlightSortedFlag = true;
            if (1 < dir.availableChunksOp.numChunks) {
                sort(dir.availableChunksOp.chunks,
                    dir.availableChunksOp.chunks +
                        dir.availableChunksOp.numChunks,
                    bind(&AvailableChunksOp::Chunks::first, _1) <
                        bind(&AvailableChunksOp::Chunks::first, _2)
                );
            }
        }
        const AvailableChunksOp::Chunks* const entry = lower_bound(
            dir.availableChunksOp.chunks,
            dir.availableChunksOp.chunks +
                dir.availableChunksOp.numChunks,
            chunkId,
            bind(&AvailableChunksOp::Chunks::first, _1) < chunkId
        );
        if (entry < dir.availableChunksOp.chunks +
                dir.availableChunksOp.numChunks &&
                entry->first == chunkId) {
            if (dir.availableChunksOp.seq == availChunksSeq) {
                KFS_LOG_STREAM_DEBUG <<
                    "stale available"
                    " chunk: "   << chunkId <<
                    " version: " << entry->second <<
                    " seq: "     << availChunksSeq <<
                KFS_LOG_EOM;
            } else {
                KFS_LOG_STREAM(availChunksSeq < 0 ?
                        MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelNOTICE) <<
                    "keeping stale availabe"
                    " chunk: "   << chunkId <<
                    " version: " << entry->second <<
                    " seq: "     << availChunksSeq <<
                    " != "       << dir.availableChunksOp.seq <<
                KFS_LOG_EOM;
                // The meta server must explicitly tell to delete this chunk by
                // setting chunk available rpc sequence number in stale chunk
                // request.
                // The available chunks rpc sequence numbers is used here to
                // disambiguate possible stale chunks requests sent in response
                // to other "events". In particular in response to chunk
                // replication or recovery, or version change completion
                // [failures], or in the cases if meta server decides to
                // "timeout" such (or any other relevant) requests, and send
                // stale chunk request in order to ensure proper cleanup.
                return -EAGAIN;
            }
        }
    }
    return StaleChunk(cih, forceDeleteFlag, evacuatedFlag);
}

int
ChunkManager::StaleChunk(kfsChunkId_t chunkId,
    bool forceDeleteFlag, bool evacuatedFlag)
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    if (! ci) {
        return -EBADF;
    }
    return StaleChunk(*ci, forceDeleteFlag, evacuatedFlag);
}

int
ChunkManager::StaleChunk(ChunkInfoHandle* cih,
    bool forceDeleteFlag, bool evacuatedFlag)
{
    if (! cih) {
        die("null chunk table entry");
        return -EFAULT;
    }
    if (mChunkTable.Erase(cih->chunkInfo.chunkId) <= 0) {
        return -EBADF;
    }
    gLeaseClerk.UnRegisterLease(cih->chunkInfo.chunkId);
    if (! cih->IsStale() && ! mPendingWrites.Delete(
            cih->chunkInfo.chunkId, cih->chunkInfo.chunkVersion)) {
        ostringstream os;
        os << "make stale failed to cleanup pending writes: "
            " chunk: "   << cih->chunkInfo.chunkId <<
            " version: " << cih->chunkInfo.chunkVersion
        ;
        die(os.str());
    }
    MakeStale(*cih, forceDeleteFlag, evacuatedFlag);
    return 0;
}

int
ChunkManager::TruncateChunk(kfsChunkId_t chunkId, int64_t chunkSize)
{
    // the truncated size should not exceed chunk size.
    if (chunkSize > (int64_t)CHUNKSIZE) {
        return -EINVAL;
    }
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    if (! ci) {
        return -EBADF;
    }
    ChunkInfoHandle* const cih = *ci;
    string const chunkPathname = MakeChunkPathname(cih);

    // Cnunk close will truncate it to the cih->chunkInfo.chunkSize

    UpdateDirSpace(cih, -cih->chunkInfo.chunkSize);

    mUsedSpace -= cih->chunkInfo.chunkSize;
    mUsedSpace += chunkSize;
    cih->chunkInfo.chunkSize = chunkSize;

    UpdateDirSpace(cih, cih->chunkInfo.chunkSize);

    uint32_t const lastChecksumBlock = OffsetToChecksumBlockNum(chunkSize);

    // XXX: Could do better; recompute the checksum for this last block
    cih->chunkInfo.chunkBlockChecksum[lastChecksumBlock] = 0;
    cih->SetMetaDirty();

    return 0;
}

int
ChunkManager::ChangeChunkVers(ChangeChunkVersOp* op)
{
    ChunkInfoHandle** const ci = mChunkTable.Find(op->chunkId);
    if (! ci) {
        return -EBADF;
    }
    ChunkInfoHandle* const cih = *ci;
    bool stableFlag = cih->IsStable();
    if (cih->IsRenameInFlight()) {
        if (op->fromChunkVersion != cih->GetTargetStateAndVersion(stableFlag)) {
            op->statusMsg = (stableFlag ? "" : "not ");
            op->statusMsg += "stable target version mismatch";
            op->status    = -EINVAL;
            return op->status;
        }
    } else if (op->fromChunkVersion != cih->chunkInfo.chunkVersion) {
        op->statusMsg = "version mismatch";
        op->status    = -EINVAL;
        return op->status;
    }
    if (! mVersionChangePermitWritesInFlightFlag && cih->HasWritesInFlight()) {
        op->statusMsg = "writes in flight";
        op->status    = -EINVAL;
        return op->status;
    }
    const int ret = ChangeChunkVers(
        cih, op->chunkVersion, op->makeStableFlag || stableFlag, op);
    if (ret < 0) {
        op->status = ret;
    }
    return ret;
}

int
ChunkManager::ChangeChunkVers(
    kfsChunkId_t           chunkId,
    int64_t                chunkVersion,
    bool                   stableFlag,
    KfsCallbackObj*        cb,
    const DiskIo::FilePtr* filePtr /* = 0 */)
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    if (! ci) {
        return -EBADF;
    }
    if (filePtr && *filePtr != (*ci)->dataFH) {
        return -EINVAL;
    }
    return ChangeChunkVers(*ci, chunkVersion, stableFlag, cb);
}

int
ChunkManager::ChangeChunkVers(
    ChunkInfoHandle* cih,
    int64_t          chunkVersion,
    bool             stableFlag,
    KfsCallbackObj*  cb)
{
    if (! cih->chunkInfo.chunkBlockChecksum) {
        KFS_LOG_STREAM_ERROR <<
            "attempt to change version on chunk: " <<
                cih->chunkInfo.chunkId << " denied: checksums are not loaded" <<
        KFS_LOG_EOM;
        return -EINVAL;
    }
    if (cih->IsWriteAppenderOwns() && ! IsChunkStable(cih)) {
        KFS_LOG_STREAM_WARN <<
            "attempt to change version on unstable chunk: " <<
                cih->chunkInfo.chunkId << " owned by write appender denied" <<
        KFS_LOG_EOM;
        return -EINVAL;
    }

    KFS_LOG_STREAM_INFO <<
        "Chunk " << MakeChunkPathname(cih) <<
        " already exists; changing version #" <<
        " from " << cih->chunkInfo.chunkVersion << " to " << chunkVersion <<
        " stable: " << cih->IsStable() << "=>" << stableFlag <<
    KFS_LOG_EOM;

    if (! mPendingWrites.Delete(
            cih->chunkInfo.chunkId, cih->chunkInfo.chunkVersion)) {
        ostringstream os;
        os << "change version failed to cleanup pending writes: "
            " chunk: "   << cih->chunkInfo.chunkId <<
            " version: " << cih->chunkInfo.chunkVersion
        ;
        die(os.str());
    }
    const bool renameFlag = true;
    return cih->WriteChunkMetadata(cb, renameFlag, stableFlag, chunkVersion);
}

void
ChunkManager::ReplicationDone(kfsChunkId_t chunkId, int status,
    const DiskIo::FilePtr& filePtr)
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    if (! ci) {
        return;
    }
    ChunkInfoHandle* const cih = *ci;
    if (! cih->IsBeingReplicated() || filePtr != cih->dataFH) {
        KFS_LOG_STREAM_DEBUG <<
            "irnored stale replication completion for"
                " chunk: "    << chunkId <<
                " status: "   << status <<
                " fileH: "    << (const void*)cih->dataFH.get() <<
                " "           << (const void*)filePtr.get() <<
        KFS_LOG_EOM;
        return;
    }

    KFS_LOG_STREAM_DEBUG <<
        "Replication for chunk: " << chunkId <<
        " status: " << status <<
        " " << MakeChunkPathname(cih) <<
    KFS_LOG_EOM;
    if (status < 0) {
        const bool forceDeleteFlag = true;
        StaleChunk(cih, forceDeleteFlag);
        return;
    }

    cih->SetBeingReplicated(false);
    LruUpdate(*cih); // Add it to lru.
    if (cih->IsFileOpen() && cih->IsStable() &&
            ! cih->IsFileInUse() && ! cih->SyncMeta()) {
        Release(*cih);
    }
}

void
ChunkManager::Start()
{
    globalNetManager().RegisterTimeoutHandler(this);
}

void
ChunkManager::UpdateDirSpace(ChunkInfoHandle* cih, int64_t nbytes)
{
    ChunkDirInfo& dir = cih->GetDirInfo();
    if (dir.availableSpace < 0) {
        return; // Directory is not in use.
    }
    dir.usedSpace += nbytes;
    if (dir.usedSpace < 0) {
        dir.usedSpace = 0;
    }
    if (0 < nbytes || cih->IsStable()) {
        dir.UpdateAvailableSpace(nbytes);
    }
    if (! cih->IsStable()) {
        dir.UpdateNotStableSpace(nbytes);
    }
}

void
ChunkManager::SetChunkSize(ChunkInfo_t& ci, int64_t chunkSize)
{
    ci.chunkSize = max(int64_t(0), ci.chunkSize);
    int64_t const           delta = max(int64_t(0), chunkSize) - ci.chunkSize;
    ChunkInfoHandle** const ch    = mChunkTable.Find(ci.chunkId);
    if (ch && 0 <= (*ch)->GetDirInfo().availableSpace) {
        mUsedSpace += delta;
        if (mUsedSpace < 0) {
            mUsedSpace = 0;
        }
        UpdateDirSpace(*ch, delta);
    }
    ci.chunkSize += delta;
}

template<typename T>
ChunkManager::ChunkDirInfo*
ChunkManager::GetDirForChunkT(T start, T end)
{
    if (start == end) {
        return 0;
    }
    T          dirToUse          = end;
    int64_t    totalFreeSpace    = 0;
    int64_t    totalPendingRead  = 0;
    int64_t    totalPendingWrite = 0;
    int64_t    maxFreeSpace      = 0;
    int        dirCount          = 0;
    for (T it = start; it != end; ++it) {
        ChunkDirInfo& di = **it;
        di.placementSkipFlag = true;
        if (di.evacuateStartedFlag) {
            continue;
        }
        const int64_t space = min(di.dirCountSpaceAvailable ?
            di.dirCountSpaceAvailable->availableSpace : di.availableSpace,
            di.availableSpace);
        if (space < mMinFsAvailableSpace ||
                space <= di.totalSpace * mMaxSpaceUtilizationThreshold) {
            continue;
        }
        BufferManager* const bufMgr =
            DiskIo::GetDiskBufferManager(di.diskQueue);
        if (bufMgr && bufMgr->GetWaitingAvgUsecs() >
                mPlacementMaxWaitingAvgUsecsThreshold) {
            continue;
        }
        dirCount++;
        totalFreeSpace += space;
        if (dirToUse == end) {
            dirToUse = it;
        }
        if (maxFreeSpace < space) {
            maxFreeSpace = space;
        }
        di.placementSkipFlag = false;
        if (mChunkPlacementPendingReadWeight <= 0 &&
                mChunkPlacementPendingWriteWeight <= 0) {
            di.pendingReadBytes  = 0;
            di.pendingWriteBytes = 0;
            continue;
        }
        int     freeRequestCount;
        int     requestCount;
        int64_t readBlockCount;
        int64_t writeBlockCount;
        int     blockSize;
        if (! DiskIo::GetDiskQueuePendingCount(
                di.diskQueue,
                freeRequestCount,
                requestCount,
                readBlockCount,
                writeBlockCount,
                blockSize)) {
            die(di.dirname + ": get pending io count failed");
        }
        di.pendingReadBytes  = readBlockCount  * blockSize;
        di.pendingWriteBytes = writeBlockCount * blockSize;
        totalPendingRead  += di.pendingReadBytes;
        totalPendingWrite += di.pendingWriteBytes;
    }
    if (dirCount <= 0 || totalFreeSpace <= 0) {
        return 0;
    }
    if (dirCount == 1) {
        return &(**dirToUse);
    }
    if (mChunkPlacementPendingReadWeight > 0 ||
            mChunkPlacementPendingWriteWeight > 0) {
        // Exclude directories / drives that exceed "max io pending".
        const int64_t maxPendingIo = max(mMinPendingIoThreshold, (int64_t)
            (totalPendingRead * mChunkPlacementPendingReadWeight +
            totalPendingWrite * mChunkPlacementPendingReadWeight) / dirCount);
        ChunkDirInfo* minIoPendingDir = 0;
        for (T it = dirToUse; it != end; ++it) {
            ChunkDirInfo& di = **it;
            if (di.placementSkipFlag) {
                continue;
            }
            if (di.pendingReadBytes + di.pendingWriteBytes >  maxPendingIo) {
                if (! minIoPendingDir ||
                        di.pendingReadBytes + di.pendingWriteBytes <
                        minIoPendingDir->pendingReadBytes +
                            minIoPendingDir->pendingWriteBytes) {
                    minIoPendingDir = &di;
                }
                if (--dirCount <= 0) {
                    return minIoPendingDir;
                }
                di.placementSkipFlag = true;
                if (di.availableSpace == maxFreeSpace) {
                    maxFreeSpace = -1; // Force update.
                }
                totalFreeSpace -= di.availableSpace;
                if (it == dirToUse) {
                    dirToUse = end;
                }
            } else if (dirToUse == end) {
                dirToUse = it;
            }
        }
    }
    if (dirCount == 1) {
        return &(**dirToUse);
    }
    assert(totalFreeSpace > 0);
    int64_t minAvail = 0;
    if (mMaxPlacementSpaceRatio > 0) {
        if (maxFreeSpace < 0) {
            maxFreeSpace = 0;
            for (T it = dirToUse; it != end; ++it) {
                ChunkDirInfo& di = **it;
                if (di.placementSkipFlag) {
                    continue;
                }
                if (maxFreeSpace < di.availableSpace) {
                    maxFreeSpace = di.availableSpace;
                }
            }
        }
        minAvail = (int64_t)(maxFreeSpace * mMaxPlacementSpaceRatio);
        for (T it = dirToUse; it != end; ++it) {
            ChunkDirInfo& di = **it;
            if (di.placementSkipFlag) {
                continue;
            }
            if (minAvail <= di.availableSpace) {
                continue;
            }
            totalFreeSpace += minAvail - di.availableSpace;
        }
    }
    const double spaceWeight = double(1) / totalFreeSpace;
    const double randVal     = drand48();
    double       curVal      = 0;
    for (T it = dirToUse; it != end; ++it) {
        ChunkDirInfo& di = **it;
        if (di.placementSkipFlag) {
            continue;
        }
        curVal += max(minAvail, di.availableSpace) * spaceWeight;
        if (randVal < curVal) {
            dirToUse = it;
            break;
        }
    }
    return (dirToUse == end ? 0 : &(**dirToUse));
}

ChunkManager::ChunkDirInfo*
ChunkManager::GetDirForChunk(kfsSTier_t minTier, kfsSTier_t maxTier)
{
    for (StorageTiers::const_iterator it = mStorageTiers.lower_bound(minTier);
            it != mStorageTiers.end() && it->first <= maxTier;
            ++it) {
        ChunkDirInfo* const dir = GetDirForChunkT(
            it->second.begin(), it->second.end());
        if (dir) {
            return dir;
        }
    }
    return 0;
}

string
ChunkManager::MakeChunkPathname(ChunkInfoHandle *cih)
{
    return MakeChunkPathname(cih, cih->IsStable(), cih->chunkInfo.chunkVersion);
}

string
ChunkManager::MakeChunkPathname(
    ChunkInfoHandle *cih, bool stableFlag, kfsSeq_t targetVersion)
{
    return MakeChunkPathname(
        cih->GetDirname(),
        cih->chunkInfo.fileId,
        cih->chunkInfo.chunkId,
        stableFlag ? targetVersion : 0,
        stableFlag ? string()      : mDirtyChunksDir
    );
}

string
ChunkManager::MakeChunkPathname(const string& chunkdir,
    kfsFileId_t fid, kfsChunkId_t chunkId, kfsSeq_t chunkVersion,
    const string& subDir)
{
    string ret;
    ret.reserve(chunkdir.size() + subDir.size() + 78);
    ret.assign(chunkdir.data(), chunkdir.size());
    ret.append(subDir.data(), subDir.size());
    AppendDecIntToString(ret, fid);
    ret += '.';
    AppendDecIntToString(ret, chunkId);
    ret += '.';
    AppendDecIntToString(ret, chunkVersion);
    return ret;
}

string
ChunkManager::MakeStaleChunkPathname(ChunkInfoHandle *cih)
{
    return MakeChunkPathname(
        cih->GetDirname(),
        cih->chunkInfo.fileId,
        cih->chunkInfo.chunkId,
        cih->chunkInfo.chunkVersion,
        mStaleChunksDir
    );
}

void
ChunkManager::AddMapping(ChunkManager::ChunkDirInfo& dir,
    kfsFileId_t fileId, chunkId_t chunkId, kfsSeq_t chunkVers,
    int64_t chunkSize)
{
    ChunkInfoHandle* cih = 0;
    if (GetChunkInfoHandle(chunkId, &cih) == 0) {
        string fileName;
        string staleName;
        string keepName;
        // Keep the chunk with the higher version.
        if (cih->chunkInfo.chunkVersion < chunkVers) {
            fileName  = MakeChunkPathname(cih);
            staleName = MakeStaleChunkPathname(cih);
            keepName  = MakeChunkPathname(
                dir.dirname, fileId, chunkId, chunkVers, string());
            Delete(*cih);
            cih = 0;
        } else {
            fileName  = MakeChunkPathname(
                dir.dirname, fileId, chunkId, chunkVers, string());
            staleName = MakeChunkPathname(
                dir.dirname, fileId, chunkId, chunkVers, mStaleChunksDir);
            keepName  = MakeChunkPathname(cih);
        }
        KFS_LOG_STREAM_INFO <<
            (mForceDeleteStaleChunksFlag ? "deleting" : "moving") <<
            " duplicate"
            " chunk: "     << chunkId <<
            " file name: " << fileName <<
            " keeping: "   << keepName <<
        KFS_LOG_EOM;
        if (mForceDeleteStaleChunksFlag) {
            if (unlink(fileName.c_str())) {
                const int err = errno;
                KFS_LOG_STREAM_ERROR <<
                    "failed to remove " << fileName <<
                    " error: " << QCUtils::SysError(err) <<
                KFS_LOG_EOM;
            }
        } else {
            if (rename(fileName.c_str(), staleName.c_str())) {
                const int err = errno;
                KFS_LOG_STREAM_ERROR <<
                    "failed to rename " << fileName << " to " << staleName <<
                    " error: " << QCUtils::SysError(err) <<
                KFS_LOG_EOM;
            }
        }
        if (cih) {
            return;
        }
    }
    cih = new ChunkInfoHandle(dir);
    cih->chunkInfo.fileId       = fileId;
    cih->chunkInfo.chunkId      = chunkId;
    cih->chunkInfo.chunkVersion = chunkVers;
    cih->chunkInfo.chunkSize    = chunkSize;
    if (AddMapping(cih) != cih) {
        die("duplicate chunk table entry");
        Delete(*cih);
    }
}

int
ChunkManager::OpenChunk(kfsChunkId_t chunkId, int openFlags)
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    if (! ci) {
        KFS_LOG_STREAM_DEBUG << "no such chunk: " << chunkId << KFS_LOG_EOM;
        return -EBADF;
    }
    return OpenChunk(*ci, openFlags);
}

int
ChunkManager::OpenChunk(ChunkInfoHandle* cih, int openFlags)
{
    if (cih->IsFileOpen()) {
        return 0;
    }
    const bool kForceFlag = true;
    if (! CleanupInactiveFds(globalNetManager().Now(), kForceFlag)) {
        KFS_LOG_STREAM_ERROR <<
            "failed to " <<
                (((openFlags & O_CREAT) == 0) ? "open" : "create") <<
            " chunk file: " << MakeChunkPathname(cih) <<
            ": out of file descriptors"
            " chunk fds: "  <<
                globals().ctrOpenDiskFds.GetValue() * mFdsPerChunk <<
            " sockets: "    << globals().ctrOpenNetFds.GetValue() <<
            " fd limit: "   << mMaxOpenFds <<
        KFS_LOG_EOM;
        return -ENFILE;
    }
    if (! cih->dataFH) {
        cih->dataFH.reset(new DiskIo::File());
    }
    string errMsg;
    const string fn = MakeChunkPathname(cih);
    bool tempFailureFlag = false;
    // Set reservation size larger than max chunk size in order to detect files
    // that weren't properly closed. + 1 here will make file one io block bigger
    // QCDiskQueue::OpenFile() makes EOF block size aligned.
    if (! cih->dataFH->Open(
            fn.c_str(),
            CHUNKSIZE + KFS_CHUNK_HEADER_SIZE + 1,
            (openFlags & (O_WRONLY | O_RDWR)) == 0,
            cih->GetDirInfo().supportsSpaceReservatonFlag,
            (openFlags & O_CREAT) != 0,
            &errMsg,
            &tempFailureFlag,
            mBufferedIoFlag || cih->GetDirInfo().bufferedIoFlag)) {
        mCounters.mOpenErrorCount++;
        if ((openFlags & O_CREAT) != 0 || ! tempFailureFlag) {
            //
            // we are unable to open/create a file. notify the metaserver
            // of lost data so that it can re-replicate if needed.
            //
            NotifyMetaCorruptedChunk(cih, -EBADF);
            if (mChunkTable.Erase(cih->chunkInfo.chunkId) > 0) {
                const int64_t size = min(mUsedSpace, cih->chunkInfo.chunkSize);
                UpdateDirSpace(cih, -size);
                mUsedSpace -= size;
            }
            Delete(*cih);
        } else {
            cih->dataFH.reset();
        }
        KFS_LOG_STREAM_ERROR <<
            "failed to " << (((openFlags & O_CREAT) == 0) ? "open" : "create") <<
            " chunk file: " << fn << " :" << errMsg <<
        KFS_LOG_EOM;
        return (tempFailureFlag ? -EAGAIN : -EBADF);
    }
    globals().ctrOpenDiskFds.Update(1);
    LruUpdate(*cih);
    if (! cih->IsStable()) {
        cih->UpdateDirStableCount();
    }
    // the checksums will be loaded async
    return 0;
}

int
ChunkManager::CloseChunk(kfsChunkId_t chunkId)
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    if (! ci) {
        return -EBADF;
    }
    return CloseChunk(*ci);
}

bool
ChunkManager::CloseChunkIfReadable(kfsChunkId_t chunkId)
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    if (! ci) {
        return -EBADF;
    }
    return (
        IsChunkStable(*ci) &&
        (*ci)->IsChunkReadable() &&
        CloseChunk(*ci) == 0
    );
}

int
ChunkManager::CloseChunk(ChunkInfoHandle* cih)
{
    if (cih->IsWriteAppenderOwns()) {
        KFS_LOG_STREAM_INFO <<
            "Ignoring close chunk on chunk: " << cih->chunkInfo.chunkId <<
            " open for append " <<
        KFS_LOG_EOM;
        return -EINVAL;
    }

    // Close file if not in use.
    if (cih->IsFileOpen() && ! cih->IsFileInUse() &&
            ! cih->IsBeingReplicated() && ! cih->SyncMeta()) {
        Release(*cih);
    } else {
        KFS_LOG_STREAM_INFO <<
            "Didn't release chunk " << cih->chunkInfo.chunkId <<
            " on close;  might give up lease" <<
        KFS_LOG_EOM;
        gLeaseClerk.RelinquishLease(
            cih->chunkInfo.chunkId, cih->chunkInfo.chunkSize);
    }
    return 0;
}

bool
ChunkManager::ChunkSize(SizeOp* op)
{
    ChunkInfoHandle* cih;
    if (GetChunkInfoHandle(op->chunkId, &cih) < 0) {
        op->status    = -EBADF;
        op->statusMsg = "no such chunk";
        return true;
    }
    if (cih->IsBeingReplicated()) {
        op->status    = -EAGAIN;
        op->statusMsg = "chunk replication in progress";
        return true;
    }
    if (op->chunkVersion >= 0 &&
            op->chunkVersion != cih->chunkInfo.chunkVersion) {
        op->status    = -EBADVERS;
        op->statusMsg = "chunk version mismatch";
        return true;
    }
    if (cih->IsWriteAppenderOwns() &&
            ! gAtomicRecordAppendManager.IsChunkStable(op->chunkId)) {
        op->statusMsg = "write append in progress, returning max chunk size";
        op->size      = CHUNKSIZE;
        KFS_LOG_STREAM_DEBUG <<
            op->statusMsg <<
            " chunk: " << op->chunkId <<
            " file: "  << op->fileId  <<
            " size: "  << op->size    <<
        KFS_LOG_EOM;
        return true;
    }
    if (! mChunkSizeSkipHeaderVerifyFlag &&
            ! cih->chunkInfo.AreChecksumsLoaded()) {
        return false;
    }
    op->size = cih->chunkInfo.chunkSize;
    return true;
}

string
ChunkManager::GetDirName(chunkId_t chunkId) const
{
    ChunkInfoHandle* cih = 0;
    if (GetChunkInfoHandle(chunkId, &cih) < 0) {
        return string();
    }
    return cih->GetDirname();
}

int
ChunkManager::ReadChunk(ReadOp* op)
{
    ChunkInfoHandle* cih = 0;
    if (GetChunkInfoHandle(op->chunkId, &cih) < 0) {
        return -EBADF;
    }

    // the checksums should be loaded...
    cih->chunkInfo.VerifyChecksumsLoaded();

    if (op->chunkVersion != cih->chunkInfo.chunkVersion) {
        KFS_LOG_STREAM_INFO << "Version # mismatch (have=" <<
            cih->chunkInfo.chunkVersion << " vs asked=" << op->chunkVersion <<
            ")...failing a read" <<
        KFS_LOG_EOM;
        return -EBADVERS;
    }
    DiskIo* const d = SetupDiskIo(cih, op);
    if (! d) {
        return -ESERVERBUSY;
    }

    op->diskIo.reset(d);

    // schedule a read based on the chunk size
    if (op->offset >= cih->chunkInfo.chunkSize) {
        op->numBytesIO = 0;
    } else if ((int64_t) (op->offset + op->numBytes) > cih->chunkInfo.chunkSize) {
        op->numBytesIO = cih->chunkInfo.chunkSize - op->offset;
    } else {
        op->numBytesIO = op->numBytes;
    }

    if (op->numBytesIO == 0) {
        return -EIO;
    }
    // for checksumming to work right, reads should be in terms of
    // checksum-blocks.
    const int64_t offset = OffsetToChecksumBlockStart(op->offset);

    size_t numBytesIO = OffsetToChecksumBlockEnd(op->offset + op->numBytesIO - 1) - offset;

    // Make sure we don't try to read past EOF; the checksumming will
    // do the necessary zero-padding.
    if ((int64_t) (offset + numBytesIO) > cih->chunkInfo.chunkSize) {
        numBytesIO = cih->chunkInfo.chunkSize - offset;
    }
    op->diskIOTime = microseconds();
    const int ret = op->diskIo->Read(offset + KFS_CHUNK_HEADER_SIZE, numBytesIO);
    if (ret < 0) {
        cih->ReadStats(ret, (int64_t)numBytesIO, 0);
        ReportIOFailure(cih, ret);
        return ret;
    }
    // read was successfully scheduled
    return 0;
}

int
ChunkManager::WriteChunk(WriteOp* op, const DiskIo::FilePtr* filePtr /* = 0 */)
{
    ChunkInfoHandle* cih = 0;
    if (GetChunkInfoHandle(op->chunkId, &cih) < 0) {
        return -EBADF;
    }
    if (filePtr && *filePtr != cih->dataFH) {
        return -EINVAL;
    }
    // the checksums should be loaded...
    cih->chunkInfo.VerifyChecksumsLoaded();

    // schedule a write based on the chunk size.  Make sure that a
    // write doesn't overflow the size of a chunk.
    op->numBytesIO = min((size_t) (CHUNKSIZE - op->offset), op->numBytes);

    if (op->numBytesIO <= 0 || op->offset < 0) {
        return -EINVAL;
    }
    const int64_t addedBytes(op->offset + op->numBytesIO - cih->chunkInfo.chunkSize);
    if (addedBytes > 0 && mUsedSpace + addedBytes >= mTotalSpace) {
        KFS_LOG_STREAM_ERROR <<
            "out of disk space: " << mUsedSpace << " + " << addedBytes <<
            " = " << (mUsedSpace + addedBytes) << " >= " << mTotalSpace <<
        KFS_LOG_EOM;
        return -ENOSPC;
    }

    int64_t offset     = op->offset;
    ssize_t numBytesIO = op->numBytesIO;
    if ((OffsetToChecksumBlockStart(offset) == offset) &&
            ((size_t)numBytesIO >= (size_t)CHECKSUM_BLOCKSIZE)) {
        if (numBytesIO % CHECKSUM_BLOCKSIZE != 0) {
            op->statusMsg = "invalid request size";
            return -EINVAL;
        }
        if (op->wpop && ! op->isFromReReplication &&
                op->checksums.size() ==
                    (size_t)(numBytesIO / CHECKSUM_BLOCKSIZE)) {
            if (op->checksums.size() == 1 &&
                    op->checksums[0] != op->wpop->checksum) {
                die("invalid write op checksum");
                return -EFAULT;
            }
        } else if ((op->isFromReReplication || op->isFromRecordAppend) &&
                ! op->checksums.empty()) {
            if (op->checksums.size() !=
                    (size_t)(numBytesIO / CHECKSUM_BLOCKSIZE)) {
                die("invalid replication write op checksum vector");
                return -EFAULT;
            }
        } else {
            op->checksums = ComputeChecksums(&op->dataBuf, numBytesIO);
        }
    } else {
        if ((size_t)numBytesIO >= (size_t) CHECKSUM_BLOCKSIZE) {
            op->statusMsg = "invalid request position or size";
            return -EINVAL;
        }
        int            off     = (int)(offset % CHECKSUM_BLOCKSIZE);
        const uint32_t blkSize = (size_t(off + numBytesIO) > CHECKSUM_BLOCKSIZE) ?
            2 * CHECKSUM_BLOCKSIZE : CHECKSUM_BLOCKSIZE;

        op->checksums.clear();
        // The checksum block we are after is beyond the current
        // end-of-chunk.  So, treat that as a 0-block and splice in.
        if (offset - off >= cih->chunkInfo.chunkSize) {
            IOBuffer data;
            data.ReplaceKeepBuffersFull(&op->dataBuf, off, numBytesIO);
            data.ZeroFill(blkSize - (off + numBytesIO));
            op->dataBuf.Move(&data);
        } else {
            // Need to read the data block over which the checksum is
            // computed.
            if (! op->rop) {
                // issue a read
                ReadOp *rop = new ReadOp(op, offset - off, blkSize);
                KFS_LOG_STREAM_DEBUG <<
                    "write triggered a read for offset=" << offset <<
                KFS_LOG_EOM;
                op->rop = rop;
                rop->Execute();
                // It is possible that the both read and write ops are complete
                // at this point. This normally happens in the case of errors.
                // In such cases all error handlers are already invoked.
                // If not then the write op will be restarted once read op
                // completes.
                // Return now.
                return 0;
            }
            // If the read failed, cleanup and bail
            if (op->rop->status < 0) {
                op->status = op->rop->status;
                op->rop->wop = 0;
                delete op->rop;
                op->rop = 0;
                return op->HandleDone(EVENT_DISK_ERROR, 0);
            }
            // All is good.  So, get on with checksumming
            op->rop->dataBuf.ReplaceKeepBuffersFull(
                &op->dataBuf, off, numBytesIO);
            op->dataBuf.Clear();
            op->dataBuf.Move(&op->rop->dataBuf);
            // If the buffer doesn't have a full CHECKSUM_BLOCKSIZE worth
            // of data, zero-pad the end.  We don't need to zero-pad the
            // front because the underlying filesystem will zero-fill when
            // we read a hole.
            ZeroPad(&op->dataBuf);
        }

        assert(op->dataBuf.BytesConsumable() == (int) blkSize);
        op->checksums = ComputeChecksums(&op->dataBuf, blkSize);

        // Trim data at the buffer boundary from the beginning, to make write
        // offset close to where we were asked from.
        int numBytes(numBytesIO);
        offset -= off;
        op->dataBuf.TrimAtBufferBoundaryLeaveOnly(off, numBytes);
        offset += off;
        numBytesIO = numBytes;
    }

    DiskIo* const d = SetupDiskIo(cih, op);
    if (! d) {
        return -ESERVERBUSY;
    }
    op->diskIo.reset(d);

    /*
    KFS_LOG_STREAM_DEBUG <<
        "Checksum for chunk: " << op->chunkId << ", offset=" << op->offset <<
        ", bytes=" << op->numBytesIO << ", # of cksums=" << op->checksums.size() <<
    KFS_LOG_EOM;
    */

    op->diskIOTime = microseconds();
    int res = op->diskIo->Write(
        offset + KFS_CHUNK_HEADER_SIZE, numBytesIO, &op->dataBuf);
    if (res >= 0) {
        UpdateChecksums(cih, op);
        assert(res <= numBytesIO);
        res = min(res, int(op->numBytesIO));
        op->numBytesIO = numBytesIO;
        cih->StartWrite(op);
    } else {
        op->diskIo.reset();
        cih->WriteStats(res, numBytesIO, 0);
        ReportIOFailure(cih, res);
    }
    return res;
}

void
ChunkManager::UpdateChecksums(ChunkInfoHandle *cih, WriteOp *op)
{
    int64_t endOffset = op->offset + op->numBytesIO;

    // the checksums should be loaded...
    cih->chunkInfo.VerifyChecksumsLoaded();

    for (vector<uint32_t>::size_type i = 0; i < op->checksums.size(); i++) {
        int64_t  offset = op->offset + i * CHECKSUM_BLOCKSIZE;
        uint32_t checksumBlock = OffsetToChecksumBlockNum(offset);

        cih->chunkInfo.chunkBlockChecksum[checksumBlock] = op->checksums[i];
    }

    if (cih->chunkInfo.chunkSize < endOffset) {

        UpdateDirSpace(cih, endOffset - cih->chunkInfo.chunkSize);

        mUsedSpace += endOffset - cih->chunkInfo.chunkSize;
        cih->chunkInfo.chunkSize = endOffset;

    }
    assert(0 <= mUsedSpace && mUsedSpace <= mTotalSpace);
}

void
ChunkManager::WriteDone(WriteOp* op)
{
    ChunkInfoHandle* cih = 0;
    if (GetChunkInfoHandle(op->chunkId, &cih) < 0) {
        return;
    }
    if (! cih->IsFileEquals(op->diskIo)) {
        KFS_LOG_STREAM_DEBUG <<
            "ignoring stale write completion: " << op->Show() <<
            " disk io: " << reinterpret_cast<const void*>(op->diskIo.get()) <<
        KFS_LOG_EOM;
        return;
    }
    op->diskIOTime = max(int64_t(1), microseconds() - op->diskIOTime);
    cih->WriteDone(op);
}

bool
ChunkManager::ReadChunkDone(ReadOp* op)
{
    ChunkInfoHandle* cih = 0;

    bool staleRead = false;
    if ((GetChunkInfoHandle(op->chunkId, &cih) < 0) ||
            (op->chunkVersion != cih->chunkInfo.chunkVersion) ||
            (staleRead = ! cih->IsFileEquals(op->diskIo))) {
        op->dataBuf.Clear();
        if (cih) {
            KFS_LOG_STREAM_INFO << "Version # mismatch (have=" <<
                cih->chunkInfo.chunkVersion <<
                " vs asked=" << op->chunkVersion << ")" <<
                (staleRead ? " stale read" : "") <<
            KFS_LOG_EOM;
        }
        op->status = -EBADVERS;
        return true;
    }

    op->diskIOTime = max(int64_t(1), microseconds() - op->diskIOTime);
    const int readLen = op->dataBuf.BytesConsumable();
    if (readLen <= 0) {
        KFS_LOG_STREAM_ERROR << "Short read for" <<
            " chunk: "  << cih->chunkInfo.chunkId  <<
            " size: "   << cih->chunkInfo.chunkSize <<
            " read:"
            " offset: " << op->offset <<
            " len: "    << readLen <<
        KFS_LOG_EOM;
        if (cih->chunkInfo.chunkSize > op->offset + readLen) {
            op->status = -EIO;
            cih->ReadStats(op->status, readLen, op->diskIOTime);
            ChunkIOFailed(cih, op->status);
        } else {
            // Size has decreased while read was in flight.
            // Possible race with truncation, which could be considered valid.
            // Another possibility that read and write completed out of order,
            // which is really a bug, especially if this really is read modify
            // write.
            assert(! op->wop);
            op->status = -EAGAIN;
        }
        return true;
    }
    // Checksums should be loaded.
    if (! cih->chunkInfo.AreChecksumsLoaded()) {
        // the read took too long; the checksums got paged out.  ask the client to retry
        KFS_LOG_STREAM_INFO << "checksums for chunk: " <<
            cih->chunkInfo.chunkId  <<
            " got paged out; returning EAGAIN to client" <<
        KFS_LOG_EOM;
        cih->ReadStats(op->status, readLen, op->diskIOTime);
        op->statusMsg = "chunk closed";
        op->status    = -EAGAIN;
        return true;
    }
    if (mForceVerifyDiskReadChecksumFlag) {
        op->skipVerifyDiskChecksumFlag = false;
    }

    ZeroPad(&op->dataBuf);
    // figure out the block we are starting from and grab all the checksums
    size_t checksumBlock = OffsetToChecksumBlockNum(op->offset);
    const int bufSize    = op->dataBuf.BytesConsumable();
    int       blockCount = bufSize / (int)CHECKSUM_BLOCKSIZE;
    if (blockCount < 1 || blockCount * (int)CHECKSUM_BLOCKSIZE != bufSize ||
            MAX_CHUNK_CHECKSUM_BLOCKS < checksumBlock + blockCount) {
        die("read verify: invalid buffer size");
        op->status = -EFAULT;
        return true;
    }
    // either nothing to verify or it better match
    bool   mismatchFlag = false;
    size_t obi          = 0;
    if (op->skipVerifyDiskChecksumFlag) {
        // The buffer should always start at the checksum block boundary.
        // AdjustDataRead() below trims the front of the buffer if offset isn't
        // checksum block aligned.
        op->checksum.resize((size_t)blockCount, mNullBlockChecksum);
        int len = (int)(op->offset % CHECKSUM_BLOCKSIZE);
        if (len > 0) {
            mCounters.mReadSkipDiskVerifyChecksumByteCount +=
                CHECKSUM_BLOCKSIZE;
            IOBuffer::iterator const eit = op->dataBuf.end();
            IOBuffer::iterator       it  = op->dataBuf.begin();
            int                      el  = (int)CHECKSUM_BLOCKSIZE - len;
            int                      nb  = 0;
            int32_t                  bcs = kKfsNullChecksum;
            for ( ; it != eit; ++it) {
                nb = it->BytesConsumable();
                if(nb <= 0) {
                    continue;
                }
                const int l = min(nb, len);
                bcs = ComputeBlockChecksum(bcs, it->Consumer(), (size_t)l);
                nb  -= l;
                len -= l;
                if (len <= 0) {
                    break;
                }
            }
            if (len != 0) {
                die("read verify: internal error invalid buffer size");
                op->status = -EFAULT;
                return true;
            }
            const int ml = min(op->numBytesIO, (ssize_t)el);
            el -= ml;
            len = ml;
            uint32_t mcs = kKfsNullChecksum;
            uint32_t ecs = kKfsNullChecksum;
            uint32_t* ccs = &mcs;
            if (0 < nb) {
                const int l = min(nb, len);
                mcs = ComputeBlockChecksum(
                    mcs, it->Producer() - nb, (size_t)l);
                len -= l;
                nb  -= l;
                if (len <= 0) {
                    len = el;
                    ccs = &ecs;
                }
                if (0 < nb && 0 < len) {
                    const int l = min(nb, len);
                    ecs = ComputeBlockChecksum(
                        ecs, it->Producer() - nb, (size_t)l);
                    len -= l;
                }
            }
            while (0 < len) {
                while (++it != eit) {
                    nb = it->BytesConsumable();
                    if (nb <= 0) {
                        continue;
                    }
                    const int l = min(nb, len);
                    *ccs = ComputeBlockChecksum(
                        *ccs, it->Consumer(), (size_t)l);
                    len -= l;
                    nb  -= l;
                    if (len <= 0) {
                        break;
                    }
                }
                if (ccs == &ecs || (len = el) <= 0 || it == eit) {
                    break;
                }
                ccs = &ecs;
                if (0 < nb) {
                    const int l = min(nb, len);
                    ecs = ComputeBlockChecksum(
                        ecs, it->Producer() - nb, (size_t)l);
                    len -= l;
                }
            }
            if (len != 0) {
                die("read verify: internal error invalid verify size");
                op->status = -EFAULT;
                return true;
            }
            uint32_t cs = ChecksumBlocksCombine(bcs, mcs, (size_t)ml);
            if (el > 0) {
                cs = ChecksumBlocksCombine(cs, ecs, (size_t)el);
            }
            const uint32_t hcs =
                cih->chunkInfo.chunkBlockChecksum[checksumBlock];
            mismatchFlag = cs != hcs && (hcs != 0 ||
                cs != mNullBlockChecksum || ! mAllowSparseChunksFlag);
            if (mismatchFlag) {
                op->checksum.front() = cs;
            } else {
                op->checksum.front() = mcs;
                // Do not copy the first entry.
                obi = 1;
                checksumBlock++;
            }
        }
        if (! mismatchFlag && obi < (size_t)blockCount &&
                (len = (int)(op->offset + op->numBytesIO) %
                    (int)CHECKSUM_BLOCKSIZE) > 0) {
            mCounters.mReadSkipDiskVerifyChecksumByteCount +=
                CHECKSUM_BLOCKSIZE;
            IOBuffer::iterator const eit = op->dataBuf.end();
            IOBuffer::iterator const bit = op->dataBuf.begin();
            IOBuffer::iterator       it  = eit;
            int rem = (int)CHECKSUM_BLOCKSIZE;
            int nb  = 0;
            while (it != bit) {
                --it;
                nb = it->BytesConsumable();
                if(nb <= 0) {
                    continue;
                }
                if (rem <= nb) {
                    break;
                }
                rem -= nb;
            }
            if (nb < rem || it == eit) {
                die("read verify: invalid buffer iterator");
                op->status = -EFAULT;
                return true;
            }
            int l = min(len, rem);
            uint32_t cs  = ComputeBlockChecksum(
                kKfsNullChecksum, it->Producer() - rem, (size_t)l);
            rem -= l;
            len -= l;
            uint32_t ecs;
            if (0 < rem) {
                ecs = cs;
                cs  = ComputeBlockChecksum(
                    cs, it->Producer() - rem, (size_t)rem);
                rem = (int)CHECKSUM_BLOCKSIZE - l - rem;
            } else {
                rem = (int)CHECKSUM_BLOCKSIZE - l - len;
                nb  = 0;
                while (0 < len) {
                    ++it;
                    nb = it->BytesConsumable();
                    if (nb <= 0) {
                        continue;
                    }
                    l = min(len, nb);
                    cs = ComputeBlockChecksum(cs, it->Consumer(), (size_t)l);
                    len -= l;
                    nb  -= l;
                }
                ecs = cs;
                if (0 < nb) {
                    cs = ComputeBlockChecksum(
                        cs, it->Producer() - nb, (size_t)nb);
                    rem -= nb;
                }
            }
            while (0 < rem) {
                ++it;
                nb = it->BytesConsumable();
                if (nb <= 0) {
                    continue;
                }
                cs = ComputeBlockChecksum(cs, it->Consumer(), (size_t)nb);
                rem -= nb;
            }
            if (rem != 0) {
                die("read verify: internal error");
                op->status = -EFAULT;
                return true;
            }
            const size_t   idx = checksumBlock - obi + blockCount - 1;
            const uint32_t hcs = cih->chunkInfo.chunkBlockChecksum[idx];
            mismatchFlag = cs != hcs && (hcs != 0 ||
                cs != mNullBlockChecksum || ! mAllowSparseChunksFlag);
            if (mismatchFlag) {
                obi           = blockCount - 1;
                checksumBlock = idx;
                op->checksum.back() = cs;
            } else {
                op->checksum.back() = ecs;
                blockCount--; // Do not copy the last entry.
            }
        }
        if (mismatchFlag) {
            mCounters.mReadSkipDiskVerifyErrorCount++;
        }
        mCounters.mReadSkipDiskVerifyCount++;
        mCounters.mReadSkipDiskVerifyByteCount += op->numBytesIO;
    } else {
        mCounters.mReadChecksumCount++;
        mCounters.mReadChecksumByteCount += bufSize;
        op->checksum = ComputeChecksums(&op->dataBuf, bufSize);
        if ((size_t)blockCount != op->checksum.size()) {
            die("read verify: invalid checksum vector size");
            op->status = -EFAULT;
            return true;
        }
    }

    if (! mismatchFlag) {
        for ( ; obi < (size_t)blockCount; checksumBlock++, obi++) {
            const uint32_t checksum =
                cih->chunkInfo.chunkBlockChecksum[checksumBlock];
            if (checksum == 0 && op->checksum[obi] == mNullBlockChecksum &&
                    mAllowSparseChunksFlag) {
                KFS_LOG_STREAM_INFO <<
                    " chunk: "      << cih->chunkInfo.chunkId <<
                    " block: "      << checksumBlock <<
                    " no checksum " <<
                    " read: "       << op->checksum[obi] <<
                KFS_LOG_EOM;
                continue;
            }
            if (op->skipVerifyDiskChecksumFlag) {
                op->checksum[obi] = checksum;
            } else if (op->checksum[obi] != checksum) {
                mismatchFlag = true;
                break;
            }
        }
    }
    if (! mismatchFlag) {
        // for checksums to verify, we did reads in multiples of
        // checksum block sizes.  so, get rid of the extra
        cih->ReadStats(op->status, readLen, op->diskIOTime);
        AdjustDataRead(op);
        return true;
    }
    const bool retry = op->retryCnt++ < mReadChecksumMismatchMaxRetryCount;
    op->status = -EBADCKSUM;
    cih->ReadStats(op->status, readLen, op->diskIOTime);

    ostringstream os;
    os <<
        "checksum mismatch for chunk=" << op->chunkId <<
        " offset="    << op->offset <<
        " bytes="     << op->numBytesIO <<
        ": expect: "  << cih->chunkInfo.chunkBlockChecksum[checksumBlock] <<
        " computed: " << op->checksum[obi] <<
        " try: "      << op->retryCnt <<
        ((mAbortOnChecksumMismatchFlag && ! retry) ? " abort" : "")
    ;
    const string str = os.str();
    KFS_LOG_STREAM_ERROR << str << KFS_LOG_EOM;
    if (retry) {
        op->dataBuf.Clear();
        if (ReadChunk(op) == 0) {
            return false;
        }
    }
    if (mAbortOnChecksumMismatchFlag) {
        die(str);
    }
    op->checksum.clear();
    op->dataBuf.Clear();

    // Notify the metaserver that the chunk we have is "bad"; the
    // metaserver will re-replicate this chunk.
    mCounters.mReadChecksumErrorCount++;
    ChunkIOFailed(cih, op->status);
    return true;
}

void
ChunkManager::NotifyMetaCorruptedChunk(ChunkInfoHandle* cih, int err)
{
    assert(cih);
    if (err == 0) {
        mCounters.mLostChunksCount++;
        cih->GetDirInfo().corruptedChunksCount++;
    } else {
        mCounters.mCorruptedChunksCount++;
    }

    KFS_LOG_STREAM_ERROR <<
        (err == 0 ? "lost" : "corrupted") <<
        " chunk: "     << cih->chunkInfo.chunkId <<
        " file: "      << cih->chunkInfo.fileId <<
        " error: "     << err <<
        (err ? string() : QCUtils::SysError(-err, " ")) <<
        " dir: "       << cih->GetDirname() <<
        " total:"
        " lost: "      << mCounters.mLostChunksCount <<
        " corrupted: " << mCounters.mCorruptedChunksCount <<
    KFS_LOG_EOM;

    // This op will get deleted when we get an ack from the metaserver
    CorruptChunkOp* const op = new CorruptChunkOp(
        0, cih->chunkInfo.fileId, cih->chunkInfo.chunkId);
    op->isChunkLost = err == 0;
    gMetaServerSM.EnqueueOp(op);
    // Meta server automatically cleans up leases for corrupted chunks.
    gLeaseClerk.UnRegisterLease(cih->chunkInfo.chunkId);
}

void
ChunkManager::ChunkIOFailed(kfsChunkId_t chunkId, int err, const DiskIo::File* file)
{
    ChunkInfoHandle* cih;
    if (GetChunkInfoHandle(chunkId, &cih) < 0) {
        KFS_LOG_STREAM_ERROR <<
            "io failure: chunk: " << chunkId << " not in table" <<
        KFS_LOG_EOM;
        return;
    }
    if (! cih->IsFileEquals(file)) {
        KFS_LOG_STREAM_DEBUG <<
            "ignoring stale io failure notification: " << chunkId <<
            " file: " << reinterpret_cast<const void*>(file) <<
        KFS_LOG_EOM;
        return;
    }
    ChunkIOFailed(cih, err);
}

void
ChunkManager::ReportIOFailure(ChunkInfoHandle* cih, int err)
{
    if (err == -EAGAIN ||
            err == -ENOMEM ||
            err == -ETIMEDOUT ||
            err == -ENFILE ||
            err == -ESERVERBUSY) {
        KFS_LOG_STREAM_ERROR <<
            "assuming temporary io failure chunk: " << cih->chunkInfo.chunkId <<
            " dir: " << cih->GetDirname() <<
            " " << QCUtils::SysError(-err) <<
        KFS_LOG_EOM;
        return;
    }
    ChunkIOFailed(cih, err);
}

void
ChunkManager::ChunkIOFailed(ChunkInfoHandle* cih, int err)
{
    NotifyMetaCorruptedChunk(cih, err);
    StaleChunk(cih);
}

void
ChunkManager::ChunkIOFailed(kfsChunkId_t chunkId, int err, const DiskIo* diskIo)
{
    ChunkIOFailed(chunkId, err, diskIo ? diskIo->GetFilePtr().get() : 0);
}

//
// directory with dirname is unaccessable; maybe drive failed.  so,
// notify metaserver of lost blocks.  the metaserver will then
// re-replicate.
//
void
ChunkManager::NotifyMetaChunksLost(
    ChunkManager::ChunkDirInfo& dir,
    bool                        staleChunksFlag /* = false */)
{
    KFS_LOG_STREAM(dir.evacuateDoneFlag ?
            MsgLogger::kLogLevelWARN : MsgLogger::kLogLevelERROR) <<
        (dir.evacuateDoneFlag ? "evacuate done: " : "lost") <<
        " chunk directory: " << dir.dirname <<
    KFS_LOG_EOM;
    CorruptChunkOp* op    = 0;
    const string*   dname = &(dir.dirname);
    for (int i = 0; i < ChunkDirInfo::kChunkDirListCount; i++) {
        ChunkDirInfo::ChunkLists& list = dir.chunkLists[i];
        ChunkInfoHandle* cih;
        while ((cih = ChunkDirList::Front(list))) {
            const kfsChunkId_t chunkId = cih->chunkInfo.chunkId;
            const kfsFileId_t  fileId  = cih->chunkInfo.fileId;
            // get rid of chunkid from our list
            const bool staleFlag = staleChunksFlag || cih->IsStale();
            ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
            if (ci && *ci == cih) {
                if (mChunkTable.Erase(chunkId) <= 0) {
                    die("corrupted chunk table");
                }
            }
            const int64_t size = min(mUsedSpace, cih->chunkInfo.chunkSize);
            UpdateDirSpace(cih, -size);
            mUsedSpace -= size;
            Delete(*cih);
            if (staleFlag) {
                continue;
            }
            KFS_LOG_STREAM_INFO <<
                "lost chunk: " << chunkId <<
                " file: " << fileId <<
            KFS_LOG_EOM;
            mCounters.mDirLostChunkCount++;
            if (! gMetaServerSM.IsConnected()) {
                // If no connection exists then the meta server assumes that
                // the chunks are lost anyway, and the inventory synchronization
                // in the meta hello is sufficient on re-connect.
                continue;
            }
            if (! op) {
                op = new CorruptChunkOp(0, fileId, chunkId, dname);
                // Do not count as corrupt.
                op->isChunkLost = true;
                dname = 0;
            } else {
                op->fid     = fileId;
                op->chunkId = chunkId;
                op->chunkDir.clear();
            }
            const int ref = op->Ref();
            gMetaServerSM.EnqueueOp(op);
            assert(op->GetRef() >= ref);
            if (op->GetRef() > ref) {
                // Op in flight / queued allocate a new one.
                op->UnRef();
                op = 0;
            }
        }
    }
    if (op) {
        op->UnRef();
    }
    if (! dir.evacuateDoneFlag) {
        mCounters.mChunkDirLostCount++;
    }
    if (dir.availableSpace >= 0) {
        StorageTiers::iterator const tit = mStorageTiers.find(dir.storageTier);
        if (tit == mStorageTiers.end()) {
            die("invalid storage tiers");
        } else {
            StorageTiers::mapped_type::iterator const it = find(
                tit->second.begin(), tit->second.end(), &dir);
            if (it == tit->second.end()) {
                die("invalid storage tier");
            } else {
                tit->second.erase(it);
            }
        }
    }
    const bool updateFlag = dir.IsCountFsSpaceAvailable();
    dir.Stop();
    if (updateFlag) {
        UpdateCountFsSpaceAvailable();
    }
    mDirChecker.Add(dir.dirname, dir.bufferedIoFlag, dir.dirLock);
}

int
ChunkManager::UpdateCountFsSpaceAvailable()
{
    int ret = 0;
    for (ChunkDirs::iterator it = mChunkDirs.begin();
            it != mChunkDirs.end();
            ++it) {
        if (it->availableSpace < 0) {
            it->dirCountSpaceAvailable = 0;
            continue;
        }
        ChunkDirInfo* dirCountSpaceAvailable = &(*it);
        if (it->dirCountSpaceAvailable &&
                it->dirCountSpaceAvailable < dirCountSpaceAvailable &&
                it->dirCountSpaceAvailable->IsCountFsSpaceAvailable() &&
                it->deviceId == it->dirCountSpaceAvailable->deviceId) {
            continue;
        }
        if (it->evacuateStartedFlag &&
                (! it->dirCountSpaceAvailable ||
                it->dirCountSpaceAvailable->evacuateStartedFlag ||
                it->dirCountSpaceAvailable->availableSpace < 0)) {
            it->dirCountSpaceAvailable  = 0;
            dirCountSpaceAvailable      = 0;
        } else {
            ret++;
            it->dirCountSpaceAvailable = dirCountSpaceAvailable;
        }
        it->totalNotStableSpace     = it->notStableSpace;
        it->totalNotStableOpenCount = it->notStableOpenCount;
        for (ChunkDirs::iterator cit = it; ++cit != mChunkDirs.end(); ) {
            if (cit->availableSpace < 0 || cit->deviceId != it->deviceId) {
                continue;
            }
            cit->totalNotStableSpace     = cit->notStableSpace;
            cit->totalNotStableOpenCount = cit->notStableOpenCount;
            if (dirCountSpaceAvailable) {
                cit->dirCountSpaceAvailable = dirCountSpaceAvailable;
                dirCountSpaceAvailable->totalNotStableSpace +=
                    cit->notStableSpace;
                dirCountSpaceAvailable->totalNotStableOpenCount +=
                    cit->notStableOpenCount;
            } else if (cit->evacuateStartedFlag) {
                cit->dirCountSpaceAvailable = 0;
            } else {
                dirCountSpaceAvailable = &(*cit);
                cit->dirCountSpaceAvailable = dirCountSpaceAvailable;
                it->dirCountSpaceAvailable  = dirCountSpaceAvailable;
                cit->totalNotStableSpace     += it->notStableSpace;
                cit->totalNotStableOpenCount += it->notStableOpenCount;
            }
        }
    }
    return ret;
}

void
ChunkManager::ZeroPad(IOBuffer *buffer)
{
    const int bytesFilled = buffer->BytesConsumable();
    const int align       = bytesFilled % (int)CHECKSUM_BLOCKSIZE;
    if (align == 0) {
        return;
    }
    const int numToZero = (int)CHECKSUM_BLOCKSIZE - align;
    if (numToZero > 0) {
        // pad with 0's
        buffer->ZeroFill(numToZero);
    }
}

void
ChunkManager::AdjustDataRead(ReadOp *op)
{
    op->dataBuf.Consume(
        op->offset - OffsetToChecksumBlockStart(op->offset));
    op->dataBuf.Trim(op->numBytesIO);
}

uint32_t
ChunkManager::GetChecksum(kfsChunkId_t chunkId, int64_t offset)
{
    ChunkInfoHandle *cih;

    if (offset < 0 || GetChunkInfoHandle(chunkId, &cih) < 0)
        return 0;

    const uint32_t checksumBlock = OffsetToChecksumBlockNum(offset);
    // the checksums should be loaded...
    cih->chunkInfo.VerifyChecksumsLoaded();

    assert(checksumBlock < MAX_CHUNK_CHECKSUM_BLOCKS);

    return cih->chunkInfo.chunkBlockChecksum[
        min(MAX_CHUNK_CHECKSUM_BLOCKS - 1, checksumBlock)];
}

vector<uint32_t>
ChunkManager::GetChecksums(kfsChunkId_t chunkId, int64_t offset, size_t numBytes)
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);

    if (offset < 0 || ! ci) {
        return vector<uint32_t>();
    }

    const ChunkInfoHandle * const cih = *ci;
    // the checksums should be loaded...
    cih->chunkInfo.VerifyChecksumsLoaded();

    return (vector<uint32_t>(
        cih->chunkInfo.chunkBlockChecksum +
            OffsetToChecksumBlockNum(offset),
        cih->chunkInfo.chunkBlockChecksum +
            min(MAX_CHUNK_CHECKSUM_BLOCKS,
                OffsetToChecksumBlockNum(
                    offset + numBytes + CHECKSUM_BLOCKSIZE - 1))
    ));
}

DiskIo*
ChunkManager::SetupDiskIo(ChunkInfoHandle *cih, KfsCallbackObj *op)
{
    if (! cih->IsFileOpen()) {
        if (OpenChunk(cih, O_RDWR) < 0) {
            return 0;
        }
    }
    LruUpdate(*cih);
    return new DiskIo(cih->dataFH, op);
}

int
ChunkManager::Restart()
{
    if (gLogger.GetVersionFromCkpt() != gLogger.GetLoggerVersionNum()) {
        KFS_LOG_STREAM_FATAL <<
            "Unsupported log version. Copy out the data and copy it back in." <<
        KFS_LOG_EOM;
        return -1;
    }
    Restore();
    return 0;
}

//
// On a restart, whatever chunks were dirty need to be nuked: we may
// have had writes pending to them and we never flushed them to disk.
//
void
ChunkManager::RemoveDirtyChunks()
{
    for (ChunkDirs::iterator it = mChunkDirs.begin();
            it != mChunkDirs.end();
            ++it) {
        if (it->availableSpace < 0) {
            continue;
        }
        const string dir = it->dirname + mDirtyChunksDir;
        DIR* const dirStream = opendir(dir.c_str());
        if (! dirStream) {
            const int err = errno;
            KFS_LOG_STREAM_ERROR <<
                "unable to open " << dir <<
                " error: " << QCUtils::SysError(err) <<
                KFS_LOG_EOM;
            continue;
        }
        struct dirent const* dent;
        while ((dent = readdir(dirStream))) {
            const string name = dir + dent->d_name;
            struct stat buf;
            if (stat(name.c_str(), &buf) || ! S_ISREG(buf.st_mode)) {
                continue;
            }
            KFS_LOG_STREAM_INFO <<
                "Cleaning out dirty chunk: " << name <<
            KFS_LOG_EOM;
            if (unlink(name.c_str())) {
                const int err = errno;
                KFS_LOG_STREAM_ERROR <<
                    "unable to remove " << name <<
                    " error: " << QCUtils::SysError(err) <<
                KFS_LOG_EOM;
            }
        }
        closedir(dirStream);
    }
}

void
ChunkManager::Restore()
{
    RemoveDirtyChunks();
    bool scheduleEvacuateFlag = false;
    for (ChunkDirs::iterator it = mChunkDirs.begin();
            it != mChunkDirs.end();
            ++it) {
        if (it->availableSpace < 0) {
            continue;
        }
        DirChecker::ChunkInfos::Iterator cit(it->availableChunks);
        const DirChecker::ChunkInfo* ci;
        while ((ci = cit.Next())) {
            if (0 <= ci->mChunkSize) {
                AddMapping(
                    *it,
                    ci->mFileId,
                    ci->mChunkId,
                    ci->mChunkVersion,
                    ci->mChunkSize
                );
            } else {
                const string name  = MakeChunkPathname(
                    string(),
                    ci->mFileId, ci->mChunkId, ci->mChunkVersion,
                    string());
                const string src(it->dirname + name);
                const string dst(it->dirname + mStaleChunksDir + name);
                if (rename(src.c_str(), dst.c_str())) {
                    const int err = errno;
                    KFS_LOG_STREAM_ERROR <<
                        "failed to rename " << src << " to " << dst <<
                        " error: " << QCUtils::SysError(err) <<
                    KFS_LOG_EOM;
                }
            }
        }
        it->availableChunks.Clear();
        if (! mEvacuateFileName.empty()) {
            const string evacuateName(it->dirname + mEvacuateFileName);
            struct stat buf = {0};
            if (stat(evacuateName.c_str(), &buf) == 0) {
                KFS_LOG_STREAM_NOTICE <<
                    "evacuate directory: " << it->dirname <<
                    " file: " << mEvacuateFileName << " exists" <<
                KFS_LOG_EOM;
                it->evacuateFlag     = true;
                scheduleEvacuateFlag = true;
            }
        }
    }
    if (scheduleEvacuateFlag) {
        UpdateCountFsSpaceAvailable();
        for (ChunkDirs::iterator it = mChunkDirs.begin();
                it != mChunkDirs.end(); ++it) {
            if (it->evacuateFlag) {
                it->ScheduleEvacuate();
            }
        }
    }
}

static inline void
AppendToHostedList(
    const ChunkManager::HostedChunkList& list,
    const ChunkInfo_t&                   chunkInfo,
    kfsSeq_t                             chunkVersion,
    bool                                 noFidsFlag)
{
    (*list.first)++;
    if (! noFidsFlag) {
        (*list.second) <<
            chunkInfo.fileId  << ' ';
    }
    (*list.second) <<
        chunkInfo.chunkId << ' ' <<
        chunkVersion      << ' '
    ;
}

void
ChunkManager::GetHostedChunks(
    const ChunkManager::HostedChunkList& stable,
    const ChunkManager::HostedChunkList& notStableAppend,
    const ChunkManager::HostedChunkList& notStable,
    bool                                 noFidsFlag)
{
    // walk thru the table and pick up the chunk-ids
    mChunkTable.First();
    const CMapEntry* p;
    while ((p = mChunkTable.Next())) {
        const ChunkInfoHandle* const cih = p->GetVal();
        if (cih->IsBeingReplicated()) {
            // Do not report replicated chunks, replications should be canceled
            // on reconnect.
            continue;
        }
        if (cih->IsRenameInFlight()) {
            // Tell meta server the target version. It comes here when the
            // meta server connection breaks while make stable or version change
            // is in flight.
            // Report the target version and status, otherwise meta server might
            // think that this is stale chunk copy, and delete it.
            // This creates time gap with the client: the chunk still might be
            // transitioning when the read comes. In such case the chunk will
            // not be "readable" and the client will be asked to come back later.
            bool stableFlag = false;
            const kfsSeq_t vers = cih->GetTargetStateAndVersion(stableFlag);
            AppendToHostedList(
                stableFlag ? stable :
                    (cih->IsWriteAppenderOwns() ?
                        notStableAppend : notStable),
                    cih->chunkInfo,
                    vers,
                    noFidsFlag
            );
        } else {
            AppendToHostedList(
                IsChunkStable(cih) ?
                    stable :
                    (cih->IsWriteAppenderOwns() ?
                        notStableAppend :
                        notStable),
                cih->chunkInfo,
                cih->chunkInfo.chunkVersion,
                noFidsFlag
            );
        }
    }
}

int
ChunkManager::GetChunkInfoHandle(
    kfsChunkId_t chunkId, ChunkInfoHandle **cih) const
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    if (! ci) {
        *cih = 0;
        return -EBADF;
    }
    *cih = *ci;
    return 0;
}

int
ChunkManager::AllocateWriteId(
    WriteIdAllocOp*       wi,
    int                   replicationPos,
    const ServerLocation& peerLoc)
{
    ChunkInfoHandle *cih = 0;

    if (GetChunkInfoHandle(wi->chunkId, &cih) < 0) {
        wi->statusMsg = "no such chunk";
        wi->status = -EBADF;
    } else if (wi->chunkVersion != cih->chunkInfo.chunkVersion) {
        wi->statusMsg = "chunk version mismatch";
        wi->status = -EINVAL;
    } else if (wi->isForRecordAppend && IsWritePending(wi->chunkId)) {
        wi->statusMsg = "random write in progress";
        wi->status = -EINVAL;
    } else if (wi->isForRecordAppend && ! IsWriteAppenderOwns(wi->chunkId)) {
        wi->statusMsg = "not open for append";
        wi->status = -EINVAL;
    } else if (! wi->isForRecordAppend && cih->IsWriteAppenderOwns()) {
        wi->statusMsg = "write append in progress";
        wi->status = -EINVAL;
    } else {
        mWriteId++;
        wi->writeId = mWriteId;
        if (wi->isForRecordAppend) {
            gAtomicRecordAppendManager.AllocateWriteId(
                wi, replicationPos, peerLoc, cih->dataFH);
        } else if (cih->IsStable()) {
            wi->statusMsg = "chunk stable";
            wi->status = -EINVAL;
        } else if (cih->IsRenameInFlight()) {
            wi->statusMsg = "chunk state transition is in progress";
            wi->status = -EAGAIN;
        } else {
            WriteOp* const op = new WriteOp(
                wi->seq, wi->chunkId, wi->chunkVersion,
                wi->offset, wi->numBytes, mWriteId
            );
            op->enqueueTime     = globalNetManager().Now();
            op->isWriteIdHolder = true;
            mPendingWrites.push_back(op);
        }
    }
    if (wi->status != 0) {
        KFS_LOG_STREAM_ERROR <<
            "failed: " << wi->Show() <<
        KFS_LOG_EOM;
    }
    wi->writePrepareReplyFlag = mWritePrepareReplyFlag;
    return wi->status;
}

int64_t
ChunkManager::GetChunkVersion(kfsChunkId_t c)
{
    ChunkInfoHandle *cih;

    if (GetChunkInfoHandle(c, &cih) < 0)
        return -1;

    return cih->chunkInfo.chunkVersion;
}

WriteOp *
ChunkManager::CloneWriteOp(int64_t writeId)
{
    WriteOp* const other = mPendingWrites.find(writeId);
    if (! other || other->status < 0) {
        // if the write is "bad" already, don't add more data to it
        if (other) {
            KFS_LOG_STREAM_ERROR <<
                "clone write op failed due to status: " << other->status <<
            KFS_LOG_EOM;
        }
        return 0;
    }

    // Since we are cloning, "touch" the time
    other->enqueueTime = globalNetManager().Now();
    // offset/size/buffer are to be filled in
    return new WriteOp(other->seq, other->chunkId, other->chunkVersion,
                     0, 0, other->writeId);
}

void
ChunkManager::SetWriteStatus(int64_t writeId, int status)
{
    WriteOp* const op = mPendingWrites.find(writeId);
    if (! op) {
        return;
    }
    op->status = status;

    KFS_LOG_STREAM_INFO <<
        "setting the status of writeid: " << writeId << " to " << status <<
    KFS_LOG_EOM;
}

int
ChunkManager::GetWriteStatus(int64_t writeId)
{
    const WriteOp* const op = mPendingWrites.find(writeId);
    return (op ? op->status : -EINVAL);
}

void
ChunkManager::RunStaleChunksQueue(bool completionFlag)
{
    if (completionFlag) {
        assert(mStaleChunkOpsInFlight > 0);
        mStaleChunkOpsInFlight--;
    }
    ChunkList::Iterator it(mChunkInfoLists[kChunkStaleList]);
    ChunkInfoHandle* cih;
    while (mStaleChunkOpsInFlight < mMaxStaleChunkOpsInFlight &&
            (cih = it.Next())) {
        // If disk queue has been already stopped, then the queue directory
        // prefix has already been removed, and it will not be possible to
        // queue disk io request (delete or rename) anyway, and attempt to do
        // so will return an error.
        if (cih->GetDirInfo().diskQueue) {
            // If the chunk with target version already exists withing the same
            // chunk directory, then do not issue delete.
            // If the existing chunk is already stable but the chunk to delete
            // has the same version but it is not stable, then the file is
            // likely have already been deleted , when the existing chunk
            // transitioned into stable version. If not then unstable chunk will
            // be cleaned up on the next restart.
            const ChunkInfoHandle* const* const ci =
                mChunkTable.Find(cih->chunkInfo.chunkId);
            if (! ci ||
                    &((*ci)->GetDirInfo()) != &(cih->GetDirInfo()) ||
                    ! (*ci)->CanHaveVersion(cih->chunkInfo.chunkVersion)) {
                if (cih->IsKeep()) {
                    if (MarkChunkStale(cih, &mStaleChunkCompletion) == 0) {
                        mStaleChunkOpsInFlight++;
                    }
                } else {
                    const string fileName = MakeChunkPathname(cih);
                    string err;
                    const bool ok = DiskIo::Delete(
                        fileName.c_str(), &mStaleChunkCompletion, &err);
                    if (ok) {
                        mStaleChunkOpsInFlight++;
                    }
                    KFS_LOG_STREAM(ok ?
                            MsgLogger::kLogLevelINFO :
                            MsgLogger::kLogLevelERROR) <<
                        "deleting stale chunk: " << fileName <<
                        (ok ? " ok" : " error: ") << err <<
                        " in flight: " << mStaleChunkOpsInFlight <<
                    KFS_LOG_EOM;
                }
            }
        }
        const int64_t size = min(mUsedSpace, cih->chunkInfo.chunkSize);
        UpdateDirSpace(cih, -size);
        mUsedSpace -= size;
        Delete(*cih);
    }
}

void
ChunkManager::Timeout()
{
    const time_t now = globalNetManager().Now();

    if (now >= mNextCheckpointTime) {
        mNextCheckpointTime = globalNetManager().Now() + mCheckpointIntervalSecs;
        // if any writes have been around for "too" long, remove them
        // and reclaim memory
        ScavengePendingWrites(now);
        // cleanup inactive fd's and thereby free up fd's
        CleanupInactiveFds(now);
    }
    if (mNextChunkDirsCheckTime < now) {
        // once in a while check that the drives hosting the chunks are good.
        CheckChunkDirs();
        mNextChunkDirsCheckTime = now + mChunkDirsCheckIntervalSecs;
    }
    if (mNextGetFsSpaceAvailableTime < now) {
        GetFsSpaceAvailable();
        mNextGetFsSpaceAvailableTime = now + mGetFsSpaceAvailableIntervalSecs;
    }
    if (mNextSendChunDirInfoTime < now && gMetaServerSM.IsConnected()) {
        SendChunkDirInfo();
        mNextSendChunDirInfoTime = now + mSendChunDirInfoIntervalSecs;
    }
    gLeaseClerk.Timeout();
    gAtomicRecordAppendManager.Timeout();
}

void
ChunkManager::ScavengePendingWrites(time_t now)
{
    const time_t opExpireTime = now - mMaxPendingWriteLruSecs;

    while (! mPendingWrites.empty()) {
        WriteOp* const op = mPendingWrites.front();
        // The list is sorted by enqueue time
        if (opExpireTime < op->enqueueTime) {
            break;
        }
        // if it exceeds 5 mins, retire the op
        KFS_LOG_STREAM_DEBUG <<
            "Retiring write with id=" << op->writeId <<
            " as it has been too long" <<
        KFS_LOG_EOM;
        mPendingWrites.pop_front();

        ChunkInfoHandle *cih;
        if (GetChunkInfoHandle(op->chunkId, &cih) == 0) {
            if (now - cih->lastIOTime >= mInactiveFdsCleanupIntervalSecs) {
                // close the chunk only if it is inactive
                CloseChunk(cih);
                // CloseChunk never deletes cih
            }
            if (cih->IsFileOpen() &&
                    ! ChunkLru::IsInList(mChunkInfoLists[kChunkLruList], *cih)) {
                LruUpdate(*cih);
            }
        }
        delete op;
    }
}

bool
ChunkManager::CleanupInactiveFds(time_t now, bool forceFlag)
{
    // if we haven't cleaned up in 5 mins or if we too many fd's that
    // are open, clean up.
    time_t expireTime;
    int    releaseCnt = -1;
    if (! forceFlag) {
        if (now < mNextInactiveFdCleanupTime) {
            return true;
        }
        expireTime = now - mInactiveFdsCleanupIntervalSecs;
    } else {
        // Reserve is to deal with asynchronous close/open in the cases where
        // open and close are executed on different io queues.
        const uint64_t kReserve     = min((mMaxOpenChunkFiles + 3) / 4,
            32 + (int)mChunkDirs.size());
        const uint64_t openChunkCnt = globals().ctrOpenDiskFds.GetValue();
        if (openChunkCnt + kReserve > (uint64_t)mMaxOpenChunkFiles ||
                (openChunkCnt + kReserve) * mFdsPerChunk +
                    globals().ctrOpenNetFds.GetValue() >
                    (uint64_t)mMaxOpenFds) {
            if (mNextInactiveFdFullScanTime < now) {
                expireTime = now + 2 * mInactiveFdsCleanupIntervalSecs;
            } else {
                expireTime = now - (mInactiveFdsCleanupIntervalSecs + 3) / 4;
            }
            releaseCnt = kReserve;
        } else {
            expireTime = now - mInactiveFdsCleanupIntervalSecs;
        }
    }

    ChunkLru::Iterator it(mChunkInfoLists[kChunkLruList]);
    ChunkInfoHandle* cih;
    while ((cih = it.Next()) && cih->lastIOTime < expireTime) {
        if (! cih->IsFileOpen() || cih->IsBeingReplicated()) {
            // Doesn't belong here, if / when io completes it will be added
            // back.
            ChunkLru::Remove(mChunkInfoLists[kChunkLruList], *cih);
            continue;
        }
        bool inUseFlag;
        bool hasLeaseFlag     = false;
        bool writePendingFlag = false;
        if ((inUseFlag = cih->IsFileInUse()) ||
                (hasLeaseFlag = gLeaseClerk.IsLeaseValid(
                    cih->chunkInfo.chunkId)) ||
                (writePendingFlag = IsWritePending(cih->chunkInfo.chunkId))) {
            KFS_LOG_STREAM_DEBUG << "cleanup: stale entry in chunk lru:"
                " dataFH: "   << (const void*)cih->dataFH.get() <<
                " chunk: "    << cih->chunkInfo.chunkId <<
                " last io: "  << (now - cih->lastIOTime) << " sec. ago" <<
                (inUseFlag ?        " file in use"     : "") <<
                (hasLeaseFlag ?     " has lease"       : "") <<
                (writePendingFlag ? " wrtie pending"   : "") <<
            KFS_LOG_EOM;
            continue;
        }
        if (cih->SyncMeta()) {
            continue;
        }
        // we have a valid file-id and it has been over 5 mins since we last did
        // I/O on it.
        KFS_LOG_STREAM_DEBUG << "cleanup: closing"
            " dataFH: "  << (const void*)cih->dataFH.get() <<
            " chunk: "   << cih->chunkInfo.chunkId <<
            " last io: " << (now - cih->lastIOTime) << " sec. ago" <<
        KFS_LOG_EOM;
        const bool openFlag = releaseCnt > 0 && cih->IsFileOpen();
        Release(*cih);
        if (releaseCnt > 0 && openFlag && ! cih->IsFileOpen()) {
            if (--releaseCnt <= 0) {
                break;
            }
        }
    }
    cih = ChunkLru::Front(mChunkInfoLists[kChunkLruList]);
    mNextInactiveFdCleanupTime = mInactiveFdsCleanupIntervalSecs +
        ((cih && cih->lastIOTime > expireTime) ? cih->lastIOTime : now);
    const bool fdsAvailableFlag = releaseCnt <= 0;
    if (! fdsAvailableFlag && mNextInactiveFdFullScanTime < now) {
        // No fd available, stop scanning until the specified amount of time
        // passes.
        mNextInactiveFdFullScanTime = now + mInactiveFdFullScanIntervalSecs;
    }
    return fdsAvailableFlag;
}

typedef map<int64_t, int> FileSystemIdsCount;

bool
ChunkManager::StartDiskIo()
{
    if ((int)KFS_CHUNK_HEADER_SIZE < IOBufferData::GetDefaultBufferSize()) {
        KFS_LOG_STREAM_INFO <<
            "invalid io buffer size: " <<
                IOBufferData::GetDefaultBufferSize() <<
            " exceeds chunk header size: " << KFS_CHUNK_HEADER_SIZE <<
        KFS_LOG_EOM;
        return false;
    }
    mDirChecker.SetLockFileName(mChunkDirLockName);
    // Ignore host fs errors and do not remove files / dirs on the initial load.
    mDirChecker.SetRemoveFilesFlag(false);
    mDirChecker.SetIgnoreErrorsFlag(true);
    mDirChecker.SetDeleteAllChaunksOnFsMismatch(-1, false);
    for (ChunkDirs::iterator it = mChunkDirs.begin();
            it < mChunkDirs.end(); ++it) {
        mDirChecker.Add(it->dirname, it->bufferedIoFlag);
    }
    mDirChecker.AddSubDir(mStaleChunksDir, mForceDeleteStaleChunksFlag);
    mDirChecker.AddSubDir(mDirtyChunksDir, true);
    mDirChecker.SetIoTimeout(-1); // Turn off on startup.
    DirChecker::DirsAvailable dirs;
    mDirChecker.Start(dirs);
    // Start is synchronous. Restore the settings after start.
    mDirChecker.SetRemoveFilesFlag(mCleanupChunkDirsFlag);
    mDirChecker.SetIgnoreErrorsFlag(false);
    SetDirCheckerIoTimeout();
    FileSystemIdsCount fsCnts;
    for (DirChecker::DirsAvailable::const_iterator it = dirs.begin();
            it != dirs.end();
            ++it) {
        fsCnts[it->second.mFileSystemId]++;
    }
    int64_t fileSystemId = -1;
    int     maxCnt       = 0;
    for (FileSystemIdsCount::const_iterator it = fsCnts.begin();
            it != fsCnts.end();
            ++it) {
        if (0 < it->first && maxCnt < it->second) {
            maxCnt       = it->second;
            fileSystemId = it->first;
        }
    }
    if (0 < fileSystemId) {
        mFileSystemId = fileSystemId;
        mDirChecker.SetDeleteAllChaunksOnFsMismatch(fileSystemId, false);
        if (1 < fsCnts.size()) {
            // Set file system id, and remove directories with different fs ids,
            // and directories that have no fs id. Directory checker will
            // "assign" fs id to the directories with no fs. This code path
            // should only be executed in the cases if new empty chunk directory
            // is added, and/or in the case of "converting" non empty chunk
            // directory. For non empty chunk directories this isn't very
            // efficient way to assign the fs id. Under normal circumstances
            // this code path should almost never be invoked.
            for (ChunkDirs::iterator it = mChunkDirs.begin();
                    it != mChunkDirs.end();
                    ++it) {
                DirChecker::DirsAvailable::iterator const dit =
                    dirs.find(it->dirname);
                if (dit == dirs.end() ||
                        dit->second.mFileSystemId == fileSystemId) {
                    continue;
                }
                it->fileSystemId = dit->second.mFileSystemId;
                dirs.erase(dit);
                mDirChecker.Add(it->dirname, it->bufferedIoFlag);
            }
            if (fsCnts.begin()->first <= 0) {
                const bool kSyncFlag = true;
                mDirChecker.GetNewlyAvailable(dirs, kSyncFlag);
            }
        }
    }
    for (ChunkDirs::iterator it = mChunkDirs.begin();
            it != mChunkDirs.end();
            ++it) {
        DirChecker::DirsAvailable::iterator const dit =
            dirs.find(it->dirname);
        if (dit == dirs.end()) {
            KFS_LOG_STREAM_INFO << it->dirname <<
                ": not using" <<
                KFS_LOG_EOM;
            it->availableSpace = -1;
            NotifyMetaChunksLost(*it);
            continue;
        }
        // UpdateCountFsSpaceAvailable() below will set the following.
        it->dirCountSpaceAvailable      = 0;
        it->fileSystemId                = dit->second.mFileSystemId;
        it->deviceId                    = dit->second.mDeviceId;
        it->dirLock                     = dit->second.mLockFdPtr;
        it->availableSpace              = 0;
        it->totalSpace                  = it->usedSpace;
        it->supportsSpaceReservatonFlag =
            dit->second.mSupportsSpaceReservatonFlag;
        it->availableChunks.Clear();
        it->availableChunks.Swap(dit->second.mChunkInfos);
        string errMsg;
        if (! DiskIo::StartIoQueue(
                it->dirname.c_str(),
                it->deviceId,
                mMaxOpenChunkFiles,
                &errMsg)) {
            KFS_LOG_STREAM_FATAL <<
                "failed to start disk queue for: " << it->dirname <<
                " dev: << " << it->deviceId << " :" << errMsg <<
            KFS_LOG_EOM;
            DiskIo::Shutdown();
            return false;
        }
        if (! (it->diskQueue = DiskIo::FindDiskQueue(it->dirname.c_str()))) {
            die(it->dirname + ": failed to find disk queue");
        }
        it->startTime = globalNetManager().Now();
        it->startCount++;
        KFS_LOG_STREAM_INFO <<
            "chunk directory: " << it->dirname <<
            " tier: "           << (int)it->storageTier <<
            " devId: "          << it->deviceId <<
            " space:"
            " available: "      << it->availableSpace <<
            " used: "           << it->usedSpace <<
            " sprsrv: "         << it->supportsSpaceReservatonFlag <<
        KFS_LOG_EOM;
        StorageTiers::mapped_type& tier = mStorageTiers[it->storageTier];
        assert(find(tier.begin(), tier.end(), it) == tier.end());
        tier.push_back(&(*it));
    }
    mMaxIORequestSize = min(CHUNKSIZE, DiskIo::GetMaxRequestSize());
    UpdateCountFsSpaceAvailable();
    GetFsSpaceAvailable();
    return true;
}

bool
ChunkManager::SetFileSystemId(int64_t fileSystemId, bool deleteAllChunksFlag)
{
    if (fileSystemId <= 0) {
        die("invalid file system id");
        return false;
    }
    mFileSystemId = fileSystemId;
    mDirChecker.SetDeleteAllChaunksOnFsMismatch(
        mFileSystemId, deleteAllChunksFlag);
    bool        ret       = true;
    string      name;
    char        tmpBuf[32];
    char* const tmpBufEnd = tmpBuf + sizeof(tmpBuf) / sizeof(tmpBuf[0]) - 1;
    *tmpBufEnd = 0;
    for (ChunkDirs::iterator it = mChunkDirs.begin();
            it != mChunkDirs.end();
            ++it) {
        if (it->availableSpace < 0) {
            continue;
        }
        if (0 < it->fileSystemId && it->fileSystemId != mFileSystemId) {
            const bool kStaleChunksFlag = true;
            NotifyMetaChunksLost(*it, kStaleChunksFlag);
            continue;
        }
        if (mFileSystemId == it->fileSystemId) {
            continue;
        }
        if (mFsIdFileNamePrefix.empty()) {
            it->fileSystemId = mFileSystemId;
            continue;
        }
        // Assign fs id to chunk directory by creating fs id file.
        DiskIo::File   file;
        DiskIo::Offset kMaxFileSize          = -1;
        DiskIo::Offset kFileSize             = -1;
        bool           kReadOnlyFlag         = false;
        bool           kReserveFileSpaceFlag = false;
        bool           kCreateFlag           = true;
        string         err;
        name = it->dirname + mFsIdFileNamePrefix +
            IntToDecString(mFileSystemId, tmpBufEnd);
        if (file.Open(
                    name.c_str(),
                    kMaxFileSize,
                    kReadOnlyFlag,
                    kReserveFileSpaceFlag,
                    kCreateFlag,
                    &err) &&
                file.Close(kFileSize, &err)) {
            it->fileSystemId = mFileSystemId;
        } else {
            ret = false;
            KFS_LOG_STREAM_ERROR <<
                name << ": " << err <<
            KFS_LOG_EOM;
            NotifyMetaChunksLost(*it);
        }
    }
    mDirChecker.Wakeup();
    mNextChunkDirsCheckTime =
        min(mNextChunkDirsCheckTime, globalNetManager().Now() + 5);
    return ret;
}

int64_t
ChunkManager::GetTotalSpace(
    int64_t&                        totalFsSpace,
    int&                            chunkDirs,
    int&                            evacuateInFlightCount,
    int&                            writableDirs,
    int&                            evacuateChunks,
    int64_t&                        evacuateByteCount,
    int*                            evacuateDoneChunkCount,
    int64_t*                        evacuateDoneByteCount,
    HelloMetaOp::LostChunkDirs*     lostChunkDirs,
    ChunkManager::StorageTiersInfo* tiersInfo,
    int64_t*                        devWaitAvgUsec)
{
    totalFsSpace           = 0;
    chunkDirs              = 0;
    writableDirs           = 0;
    evacuateInFlightCount  = 0;
    evacuateChunks         = 0;
    evacuateByteCount      = 0;
    int     evacuateDoneChunks     = 0;
    int64_t evacuateDoneBytes      = 0;
    int64_t totalFsAvailableSpace  = 0;
    int64_t usedSpace              = 0;
    size_t  tierSpaceAvailableCnt  = 0;
    int64_t waitAvgUsec            = 0;
    int64_t waitAvgCnt             = 0;
    if (tiersInfo) {
        tiersInfo->clear();
    }
    for (ChunkDirs::const_iterator it = mChunkDirs.begin();
            it < mChunkDirs.end(); ++it) {
        if (it->availableSpace < 0) {
            if (lostChunkDirs) {
                lostChunkDirs->insert(lostChunkDirs->end(), it->dirname);
            }
            continue;
        }
        if (it->evacuateFlag) {
            // Never send evacuate count to the meta server <= 0 while
            // evacuation is in progress -- the meta server clears evacuation
            // queue when counter is 0.
            // The counter can be sent on heartbeat, while evacuation response
            // in flight, so the two can potentially get out of sync.
            evacuateInFlightCount += max(1, it->evacuateInFlightCount);
            evacuateChunks        += it->chunkCount;
            evacuateByteCount     += it->usedSpace;
            evacuateDoneChunks    += it->GetEvacuateDoneChunkCount();
            evacuateDoneBytes     += it->GetEvacuateDoneByteCount();
        } else {
            if (it->availableSpace > mMinFsAvailableSpace &&
                    it->availableSpace >
                        it->totalSpace * mMaxSpaceUtilizationThreshold) {
                writableDirs++;
                const BufferManager* const bufMgr =
                    DiskIo::GetDiskBufferManager(it->diskQueue);
                if (bufMgr) {
                    waitAvgUsec += bufMgr->GetWaitingAvgUsecs();
                    waitAvgCnt++;
                }
                if (tiersInfo) {
                    StorageTierInfo& ti = (*tiersInfo)[it->storageTier];
                    ti.mNotStableOpenCount += it->notStableOpenCount;
                    ti.mChunkCount         += it->chunkCount;
                    if (it->IsCountFsSpaceAvailable()) {
                        tierSpaceAvailableCnt++;
                        ti.mDeviceCount++;
                        ti.mSpaceAvailable += it->availableSpace;
                        ti.mTotalSpace     += it->totalSpace;
                    }
                }
            }
        }
        chunkDirs++;
        if (it->IsCountFsSpaceAvailable()) {
            totalFsSpace          += it->totalSpace;
            totalFsAvailableSpace += it->availableSpace;
        }
        usedSpace += it->usedSpace;
        KFS_LOG_STREAM_DEBUG <<
            "chunk directory: " << it->dirname <<
            " has space "       << it->availableSpace <<
            " total: "          << totalFsAvailableSpace <<
            " used: "           << usedSpace <<
            " limit: "          << mTotalSpace <<
        KFS_LOG_EOM;
    }
    if (evacuateDoneChunkCount) {
        *evacuateDoneChunkCount = evacuateDoneChunks;
    }
    if (evacuateDoneByteCount) {
        *evacuateDoneByteCount = evacuateDoneBytes;
    }
    // If device / host fs belongs to more than one tier (normally makes sense
    // only for testing), then count the device in all tiers it belongs to.
    if (tiersInfo && tiersInfo->size() > tierSpaceAvailableCnt) {
        for (StorageTiersInfo::iterator it = tiersInfo->begin();
                it != tiersInfo->end(); ) {
            if (it->second.mDeviceCount <= 0) {
                StorageTiers::iterator const ti = mStorageTiers.find(it->first);
                if (ti == mStorageTiers.end()) {
                    tiersInfo->erase(it++);
                    continue;
                }
                StorageTiers::mapped_type::iterator di = ti->second.begin();
                while (di != ti->second.end() && (*di)->availableSpace <= 0)
                    {}
                if (di == ti->second.end()) {
                    tiersInfo->erase(it++);
                    continue;
                }
                it->second.mDeviceCount    = 1;
                it->second.mSpaceAvailable = (*di)->availableSpace;
                it->second.mTotalSpace     = (*di)->totalSpace;
            }
            ++it;
        }
    }
    if (devWaitAvgUsec) {
        *devWaitAvgUsec = waitAvgCnt > 0 ? waitAvgUsec / waitAvgCnt : 0;
    }
    return (min(totalFsAvailableSpace, mTotalSpace) + mUsedSpace);
}

void
ChunkManager::SendChunkDirInfo()
{
    for (ChunkDirs::iterator it = mChunkDirs.begin();
            it < mChunkDirs.end();
            ++it) {
        it->chunkDirInfoOp.Enqueue();
    }
}

BufferManager*
ChunkManager::FindDeviceBufferManager(kfsChunkId_t chunkId)
{
    if (! mDiskBufferManagerEnabledFlag) {
        return 0;
    }
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    return (! ci ? 0 :
        DiskIo::GetDiskBufferManager((*ci)->GetDirInfo().diskQueue));
}

int
ChunkManager::ChunkDirInfo::CheckDirDone(int code, void* data)
{
    if ((code != EVENT_DISK_CHECK_DIR_READABLE_DONE &&
            code != EVENT_DISK_CHECK_DIR_WRITABLE_DONE &&
            code != EVENT_DISK_ERROR) || ! checkDirFlightFlag) {
        die("CheckDirDone invalid completion");
    }

    checkDirFlightFlag = false;
    if (availableSpace < 0) {
        return 0; // Ignore, already marked not in use.
    }

    const int interval = gChunkManager.GetDirCheckFailureSimulatorInterval();
    if (0 <= interval && code != EVENT_DISK_ERROR &&
            (interval == 0 || gChunkManager.Rand() % interval == 0)) {
        KFS_LOG_STREAM_NOTICE <<
            "simulating chunkd direcotry check failure:"
            " "           << dirname <<
            " interval: " << interval <<
        KFS_LOG_EOM;
        DiskError(-EIO);
        return 0;
    }

    if (code == EVENT_DISK_ERROR) {
        DiskError(*reinterpret_cast<const int*>(data));
    } else {
        KFS_LOG_STREAM_DEBUG <<
            "chunk directory: " << dirname << " is ok"
            " space: " << availableSpace <<
            " used: "  << usedSpace <<
            " dev: "   << deviceId <<
            " queue: " << (const void*)diskQueue <<
        KFS_LOG_EOM;
        diskTimeoutCount = 0;
    }
    return 0;
}

int
ChunkManager::ChunkDirInfo::FsSpaceAvailDone(int code, void* data)
{
    if ((code != EVENT_DISK_GET_FS_SPACE_AVAIL_DONE &&
            code != EVENT_DISK_ERROR) || ! fsSpaceAvailInFlightFlag) {
        die("FsSpaceAvailDone invalid completion");
    }

    fsSpaceAvailInFlightFlag = false;
    if (availableSpace < 0) {
        return 0; // Ignore, already marked not in use.
    }

    if (code == EVENT_DISK_ERROR) {
        DiskError(*reinterpret_cast<int*>(data));
    } else {
        if (availableSpace >= 0) {
            const int64_t* const ret =
                reinterpret_cast<const int64_t*>(data);
            const int64_t fsAvail = ret[0];
            const int64_t fsTotal = ret[1];
            KFS_LOG_STREAM_DEBUG <<
                "chunk directory: " << dirname <<
                " available: "      << availableSpace <<
                " => "              << fsAvail <<
                " total: "          << totalSpace <<
                " => "              << fsTotal <<
                " used: "           << usedSpace <<
            KFS_LOG_EOM;
            totalSpace     = max(int64_t(0), fsTotal);
            availableSpace = min(totalSpace, max(int64_t(0), fsAvail));
            if (supportsSpaceReservatonFlag) {
                availableSpace = max(
                    int64_t(0), availableSpace - pendingSpaceReservationSize);
            } else {
                // Virtually reserve 64MB + 16K for each not stable chunk.
                const ChunkDirInfo& dir = dirCountSpaceAvailable ?
                    *dirCountSpaceAvailable : *this;
                availableSpace = min(totalSpace,
                    max(int64_t(0), availableSpace +
                        dir.totalNotStableSpace -
                        dir.totalNotStableOpenCount *
                            (int64_t)(CHUNKSIZE + KFS_CHUNK_HEADER_SIZE))
                );
            }
        }
        diskTimeoutCount = 0;
    }
    return 0;
}

void
ChunkManager::ChunkDirInfo::DiskError(int sysErr)
{
    if (availableSpace < 0) {
        return; // Ignore, already marked not in use.
    }
    KFS_LOG_STREAM_ERROR <<
        "chunk directory: " << dirname <<
        " error: "          << QCUtils::SysError(-sysErr) <<
        " space:"
        " available: "      << availableSpace <<
        " used: "           << usedSpace <<
    KFS_LOG_EOM;
    if ((sysErr != -EMFILE && sysErr != -ENFILE) &&
            (sysErr != -ETIMEDOUT || ++diskTimeoutCount >
            gChunkManager.GetMaxDirCheckDiskTimeouts())) {
        gChunkManager.NotifyMetaChunksLost(*this);
    }
}

int
ChunkManager::ChunkDirInfo::CheckEvacuateFileDone(int code, void* data)
{
    if ((code != EVENT_DISK_GET_FS_SPACE_AVAIL_DONE &&
            code != EVENT_DISK_ERROR) || ! checkEvacuateFileInFlightFlag) {
        die("CheckEvacuateFileDone invalid completion");
    }

    checkEvacuateFileInFlightFlag = false;
    if (availableSpace < 0) {
        return 0; // Ignore, already marked not in use.
    }

    if (code == EVENT_DISK_ERROR) {
        const int sysErr = *reinterpret_cast<int*>(data);
        KFS_LOG_STREAM(sysErr == -ENOENT ?
                MsgLogger::kLogLevelDEBUG :
                MsgLogger::kLogLevelERROR) <<
            "chunk directory: " << dirname <<
            " \"evacuate\""
            " error: " << QCUtils::SysError(-sysErr) <<
            " space: " << availableSpace <<
            " used: "  << usedSpace <<
            " dev: "   << deviceId <<
            " queue: " << (const void*)diskQueue <<
        KFS_LOG_EOM;
        if (sysErr == -EIO) {
            if (++evacuateCheckIoErrorsCount >=
                    gChunkManager.GetMaxEvacuateIoErrors()) {
                DiskError(sysErr);
            }
        } else {
            if (evacuateFlag && ! stopEvacuationFlag && StopEvacuation()) {
                KFS_LOG_STREAM_NOTICE <<
                    "chunk directory: " << dirname <<
                    " stopping evacuation"
                    " space: " << availableSpace <<
                    " used: "  << usedSpace <<
                    " dev: "   << deviceId <<
                    " queue: " << (const void*)diskQueue <<
                KFS_LOG_EOM;
            }
            evacuateCheckIoErrorsCount = 0;
        }
    } else if (! evacuateFlag) {
        KFS_LOG_STREAM_NOTICE <<
            "chunk directory: " << dirname <<
            " \"evacuate\""
            " space: " << availableSpace <<
            " used: "  << usedSpace <<
            " dev: "   << deviceId <<
            " queue: " << (const void*)diskQueue <<
        KFS_LOG_EOM;
        diskTimeoutCount = 0;
        evacuateFlag     = true;
        ScheduleEvacuate();
    }
    return 0;
}

int
ChunkManager::ChunkDirInfo::EvacuateChunksDone(int code, void* data)
{
    if (code != EVENT_CMD_DONE || data != &evacuateChunksOp ||
            ! evacuateChunksOpInFlightFlag) {
        die("EvacuateChunksDone invalid completion");
    }

    evacuateChunksOpInFlightFlag = false;
    if (availableSpace < 0) {
        return 0; // Ignore, already marked not in use.
    }
    if (stopEvacuationFlag) {
        StopEvacuation();
    }
    if (! evacuateFlag) {
        return 0;
    }
    UpdateLastEvacuationActivityTime();
    if (evacuateChunksOp.status != 0) {
        if (! evacuateStartedFlag && evacuateChunksOp.status == -EAGAIN) {
            SetEvacuateStarted();
        }
        if (! evacuateStartedFlag || (evacuateInFlightCount <= 0 &&
                (evacuateChunksOp.status != -EAGAIN ||
                    evacuateChunksOp.numChunks <= 1))) {
            // Restart from the evacuate file check, in order to try again with
            // a delay.
            if (! ChunkDirList::IsEmpty(chunkLists[kChunkDirEvacuateList])) {
                die("non empty evacuate list");
            }
            evacuateStartedFlag = false;
            evacuateFlag        = false;
            KFS_LOG_STREAM_WARN <<
                "evacuate: " << dirname <<
                " status: "  << evacuateChunksOp.status <<
                " restarting from evacuation file check" <<
            KFS_LOG_EOM;
        }
        if (evacuateStartedFlag == IsCountFsSpaceAvailable()) {
            gChunkManager.UpdateCountFsSpaceAvailable();
        }
        rescheduleEvacuateThreshold = max(0,
            evacuateInFlightCount - max(0, evacuateChunksOp.numChunks));
        if (evacuateInFlightCount <= 0 && evacuateStartedFlag) {
            // Do one chunk at a time if we get -EAGAIN and no
            // evacuations are in flight at the moment.
            ScheduleEvacuate(1);
        }
        return 0;
    }

    SetEvacuateStarted();
    if (IsCountFsSpaceAvailable()) {
        gChunkManager.UpdateCountFsSpaceAvailable();
    }
    // Minor optimization: try to traverse the chunk list first, it likely
    // that all chunks that were scheduled for evacuation are still in the list
    // in the same order that they were scheduled.
    ChunkDirList::Iterator it(chunkLists[kChunkDirList]);
    int i;
    for (i = 0; i < evacuateChunksOp.numChunks; i++) {
        ChunkInfoHandle* const cih = it.Next();
        if (! cih || cih->chunkInfo.chunkId != evacuateChunksOp.chunkIds[i]) {
            break;
        }
        cih->SetEvacuate(true);
    }
    for ( ; i < evacuateChunksOp.numChunks; i++) {
        ChunkInfoHandle* cih;
        if (gChunkManager.GetChunkInfoHandle(
                evacuateChunksOp.chunkIds[i], &cih) == 0 &&
                &(cih->GetDirInfo()) == this) {
            cih->SetEvacuate(true);
        }
    }
    ScheduleEvacuate();
    return 0;
}

void
ChunkManager::ChunkDirInfo::NotifyAvailableChunks(bool timeoutFlag /* false */)
{
    if (availableSpace < 0 || availableChunksOpInFlightFlag) {
        return;
    }
    if (gMetaServerSM.IsUp()) {
        availableChunksOp.noReply = false;
        if (notifyAvailableChunksStartFlag) {
            notifyAvailableChunksStartFlag = false;
            availableChunksOp.status = 0;
        }
        if (availableChunksOp.status >= 0 || availableChunksOp.numChunks <= 0) {
            availableChunksOp.numChunks = 0;
            while (! availableChunks.IsEmpty() &&
                    availableChunksOp.numChunks <
                    AvailableChunksOp::kMaxChunkIds) {
                const DirChecker::ChunkInfo& ci  = availableChunks.Back();
                ChunkInfoHandle* const       cih = new ChunkInfoHandle(*this);
                cih->chunkInfo.fileId       = ci.mFileId;
                cih->chunkInfo.chunkId      = ci.mChunkId;
                cih->chunkInfo.chunkVersion = ci.mChunkVersion;
                if (ci.mChunkSize < 0) {
                    // Invalid chunk or io error.
                    // Set chunk size to 0, to make accounting work in stale
                    // chunk deletion, as the space utilization was not updated.
                    cih->chunkInfo.chunkSize    = 0; //-ci.mChunkSize - 1;
                    const bool kForceDeleteFlag = false;
                    const bool kEvacuatedFlag   = false;
                    gChunkManager.MakeStale(
                        *cih, kForceDeleteFlag, kEvacuatedFlag);
                    availableChunks.PopBack();
                    continue;
                }
                cih->chunkInfo.chunkSize = ci.mChunkSize;
                ChunkInfoHandle* ach = gChunkManager.AddMapping(cih);
                if (ach != cih && 0 < ci.mChunkVersion) {
                    if (! ach) {
                        // Eliminate lint warning.
                        die("invalid null chunk handle");
                        return;
                    }
                    if (ach->IsBeingReplicated()) {
                        if (Replicator::Cancel(ci.mChunkId, ci.mChunkVersion)) {
                            // Cancel should have already erase the chunk table
                            // entry, if not, stale chunk below will remove it.
                            // The previous chunk entry "ach" is no longer
                            // valid, do not use it with StaleChunk() method.
                            const bool kForceDeleteFlag = true;
                            gChunkManager.StaleChunk(
                                ci.mChunkId, kForceDeleteFlag);
                            ach = gChunkManager.AddMapping(cih);
                        }
                    } else {
                        // Keep the "old" chunk file that just became
                        // "available", in the case if recovery has completed,
                        // but meta server has not transitioned the chunk into
                        // the final version. It is impossible to tell here if
                        // the existing chunk version is stale or not. If the
                        // version is stale then the meta server will have to
                        // re-schedule chunk recovery again, when the version
                        // change fails due non zero "from" version, and make
                        // this chunk stale when chunk available rpc arrives.
                        // The chunk must not have any meta operations in
                        // flight, i.e. it must be "readable", as, otherwise,
                        // in flight rename might have already completed and
                        // replaced the "available" chunk file.
                        if (ach->IsChunkReadable() &&
                                ach->chunkInfo.chunkVersion <= 0) {
                            KFS_LOG_STREAM_NOTICE <<
                                " keeping:"
                                " chunk: "   << ci.mChunkId <<
                                " version: " << ci.mChunkVersion <<
                                " size: "    << ci.mChunkSize <<
                                " discarding:"
                                " version: " <<
                                    ach->chunkInfo.chunkVersion <<
                                " size: "    <<
                                    ach->chunkInfo.chunkVersion <<
                            KFS_LOG_EOM;
                            // Move 0 version chunk into lost+found, if
                            // configured.
                            const bool kForceDeleteFlag = false;
                            const bool kEvacuatedFlag   = false;
                            gChunkManager.StaleChunk(
                                ach, kForceDeleteFlag, kEvacuatedFlag);
                            ach = gChunkManager.AddMapping(cih);
                        }
                    }
                }
                if (ach == cih) {
                    availableChunksOp.chunks[
                        availableChunksOp.numChunks].first  = ci.mChunkId;
                    availableChunksOp.chunks[
                        availableChunksOp.numChunks].second = ci.mChunkVersion;
                    availableChunksOp.numChunks++;
                } else {
                    if (&(ach->GetDirInfo()) == this &&
                            ach->CanHaveVersion(cih->chunkInfo.chunkVersion)) {
                        // Do not attempt to delete chunk file, only free the
                        // table entry.
                        gChunkManager.DeleteSelf(*cih);
                    } else {
                        const bool kForceDeleteFlag = false;
                        const bool kEvacuatedFlag   = false;
                        gChunkManager.MakeStale(
                            *cih, kForceDeleteFlag, kEvacuatedFlag);
                    }
                }
                availableChunks.PopBack();
            }
        } else if (! timeoutFlag) {
            if (timeoutPendingFlag) {
                return;
            }
            KFS_LOG_STREAM_ERROR <<
                availableChunksOp.Show() <<
                " status: " << availableChunksOp.status <<
                " will retry in " <<
                    gChunkManager.GetAvailableChunksRetryInterval() / 1000. <<
            KFS_LOG_EOM;
            timeoutPendingFlag = true;
            globalNetManager().RegisterTimeoutHandler(this);
            SetTimeoutInterval(gChunkManager.GetAvailableChunksRetryInterval());
            return;
        }
        if (availableChunksOp.numChunks <= 0) {
            return;
        }
    } else {
       if (! timeoutFlag &&
                timeoutPendingFlag &&
                ! availableChunks.IsEmpty()) {
            return; // Restart on timeout.
        }
        // Queue an empty op, to get this method called again when meta server connection
        // gets established and chunk server hello completes
        availableChunksOp.numChunks = 0;
        availableChunksOp.noReply   = ! availableChunks.IsEmpty();
    }
    if (timeoutPendingFlag) {
        timeoutPendingFlag = false;
        globalNetManager().UnRegisterTimeoutHandler(this);
    }
    if ((availableChunksOp.numChunks <= 0 && ! availableChunksOp.noReply) ||
            ! globalNetManager().IsRunning()) {
        return;
    }
    availableChunksOpInFlightFlag     = true;
    chunksAvailableInFlightSortedFlag = false;
    availableChunksOp.status          = 0;
    gMetaServerSM.EnqueueOp(&availableChunksOp);
}

int
ChunkManager::ChunkDirInfo::AvailableChunksDone(int code, void* data)
{
    if (code != EVENT_CMD_DONE || data != &availableChunksOp ||
            ! availableChunksOpInFlightFlag) {
        die("AvailableChunksDone invalid completion");
    }
    availableChunksOpInFlightFlag = false;
    if (availableChunksOp.status < 0 && availableSpace >= 0 &&
            ! notifyAvailableChunksStartFlag &&
            ! gMetaServerSM.IsConnected()) {
        // Meta server disconnect.
        // Re-queue the chunks that were part of the last op, unless these
        // were modified or "touched" in any way.
        // The chunks need to be removed from the chunk table to effectively
        // prevent the chunk ids to be sent in the chunk server hello.
        // All this isn't strictly nesessary though as the chunk ids would be
        // send on the chunk server restart anyway if the newly available chunk
        // directory would still be "available".
        for (int i = 0; i < availableChunksOp.numChunks; i++) {
            ChunkInfoHandle* cih = 0;
            if (gChunkManager.GetChunkInfoHandle(
                        availableChunksOp.chunks[i].first, &cih) != 0 ||
                    ! cih ||
                    cih->chunkInfo.chunkVersion !=
                        availableChunksOp.chunks[i].second ||
                    &(cih->GetDirInfo()) != this ||
                    ! cih->IsChunkReadable() ||
                    cih->IsRenameInFlight() ||
                    cih->IsStale() ||
                    cih->IsBeingReplicated() ||
                    cih->readChunkMetaOp ||
                    cih->dataFH ||
                    cih->chunkInfo.AreChecksumsLoaded()) {
                continue;
            }
            DirChecker::ChunkInfo ci;
            ci.mFileId       = cih->chunkInfo.fileId;
            ci.mChunkId      = cih->chunkInfo.chunkId;
            ci.mChunkVersion = cih->chunkInfo.chunkVersion;
            ci.mChunkSize    = cih->chunkInfo.chunkSize;
            if (gChunkManager.Remove(*cih)) {
                availableChunks.PushBack(ci);
            }
        }
        availableChunksOp.numChunks = 0;
    }
    NotifyAvailableChunks();
    return 0;
}

int
ChunkManager::ChunkDirInfo::RenameEvacuateFileDone(int code, void* data)
{
    if ((code != EVENT_DISK_RENAME_DONE &&
            code != EVENT_DISK_ERROR) || ! evacuateFileRenameInFlightFlag) {
        die("RenameEvacuateFileDone invalid completion");
    }

    evacuateFileRenameInFlightFlag = false;
    if (availableSpace < 0) {
        return 0; // Ignore, already marked not in use.
    }

    if (code == EVENT_DISK_ERROR) {
        DiskError(*reinterpret_cast<int*>(data));
    } else {
        KFS_LOG_STREAM_NOTICE <<
            "chunk directory: " << dirname << " evacuation done"
            " space: " << availableSpace <<
            " used: "  << usedSpace <<
            " dev: "   << deviceId <<
            " queue: " << (const void*)diskQueue <<
        KFS_LOG_EOM;
        diskTimeoutCount = 0;
        evacuateDoneFlag = true;
        gChunkManager.NotifyMetaChunksLost(*this);
    }
    return 0;
}

void
ChunkManager::ChunkDirInfo::ScheduleEvacuate(int maxChunkCount)
{
    if (availableSpace < 0) {
        return; // Ignore, already marked not in use.
    }

    if (evacuateChunksOpInFlightFlag || ! evacuateFlag ||
            ! globalNetManager().IsRunning()) {
        return;
    }
    if (evacuateStartedFlag &&
            ChunkDirList::IsEmpty(chunkLists[kChunkDirList])) {
        if (evacuateInFlightCount > 0 ||
                ! ChunkDirList::IsEmpty(chunkLists[kChunkDirEvacuateList])) {
            return;
        }
        if (evacuateDoneFlag || evacuateFileRenameInFlightFlag) {
            return;
        }
        if (gChunkManager.GetEvacuateFileName().empty() ||
                gChunkManager.GetEvacuateDoneFileName().empty()) {
            evacuateDoneFlag = true;
            return;
        }
        const string src = dirname + gChunkManager.GetEvacuateFileName();
        const string dst = dirname + gChunkManager.GetEvacuateDoneFileName();
        string       statusMsg;
        evacuateFileRenameInFlightFlag = true;
        if (! DiskIo::Rename(
                    src.c_str(),
                    dst.c_str(),
                    &renameEvacuateFileCb,
                    &statusMsg)) {
            KFS_LOG_STREAM_ERROR <<
               "evacuate done rename " <<
               src << " to " << dst <<
               " " << statusMsg <<
            KFS_LOG_EOM;
            evacuateFileRenameInFlightFlag = false; // Retry later
        }
        return;
    }
    if (evacuateStartedFlag) {
        evacuateChunksOp.totalSpace            = -1;
        evacuateChunksOp.totalFsSpace          = -1;
        evacuateChunksOp.usedSpace             = -1;
        evacuateChunksOp.chunkDirs             = -1;
        evacuateChunksOp.writableChunkDirs     = -1;
        evacuateChunksOp.evacuateInFlightCount = -1;
        evacuateChunksOp.numChunks             = 0;
        evacuateChunksOp.evacuateChunks        = -1;
        evacuateChunksOp.evacuateByteCount     = -1;
        evacuateChunksOp.tiersInfo.clear();
        const int maxCnt = maxChunkCount > 0 ?
            min(int(EvacuateChunksOp::kMaxChunkIds), maxChunkCount) :
            EvacuateChunksOp::kMaxChunkIds;
        ChunkDirList::Iterator it(chunkLists[kChunkDirList]);
        ChunkInfoHandle*       cih;
        while (evacuateChunksOp.numChunks < maxCnt && (cih = it.Next())) {
            evacuateChunksOp.chunkIds[evacuateChunksOp.numChunks++] =
                cih->chunkInfo.chunkId;
        }
    } else {
        KFS_LOG_STREAM_WARN <<
            "evacuate: " << dirname <<
            " starting" <<
        KFS_LOG_EOM;
        // On the first evacuate update the meta server space, in order to
        // to prevent chunk allocation failures.
        // When the response comes back the evacuate started flag is set to
        // true.
        const bool updateFlag = IsCountFsSpaceAvailable();
        SetEvacuateStarted();
        if (updateFlag) {
            gChunkManager.UpdateCountFsSpaceAvailable();
        }
        evacuateChunksOp.tiersInfo.clear();
        evacuateChunksOp.totalSpace = gChunkManager.GetTotalSpace(
            evacuateChunksOp.totalFsSpace,
            evacuateChunksOp.chunkDirs,
            evacuateChunksOp.evacuateInFlightCount,
            evacuateChunksOp.writableChunkDirs,
            evacuateChunksOp.evacuateChunks,
            evacuateChunksOp.evacuateByteCount,
            0,
            0,
            0,
            &evacuateChunksOp.tiersInfo
        );
        evacuateChunksOp.usedSpace = gChunkManager.GetUsedSpace();
        evacuateStartedFlag = false;
        if (updateFlag) {
            gChunkManager.UpdateCountFsSpaceAvailable();
        }
    }
    UpdateLastEvacuationActivityTime();
    // Submit op even if the chunk list is empty in order to update meta
    // server's free space counters.
    evacuateChunksOpInFlightFlag = true;
    evacuateChunksOp.status = 0;
    gMetaServerSM.EnqueueOp(&evacuateChunksOp);
}

void
ChunkManager::ChunkDirInfo::RestartEvacuation()
{
    if (availableSpace < 0) {
        return; // Ignore, already marked not in use.
    }
    if (! evacuateStartedFlag || stopEvacuationFlag) {
        return;
    }
    KFS_LOG_STREAM_WARN <<
        "evacuate: " << dirname <<
        " restarting"
        " in flight: " << evacuateInFlightCount <<
    KFS_LOG_EOM;
    ChunkDirInfo::ChunkLists& list = chunkLists[kChunkDirEvacuateList];
    ChunkInfoHandle*          cih;
    while ((cih = ChunkDirList::Front(list))) {
        cih->SetEvacuate(false);
    }
    ScheduleEvacuate();
}

bool
ChunkManager::ChunkDirInfo::StopEvacuation()
{
    if (! evacuateFlag || availableSpace < 0 || evacuateDoneFlag ||
            evacuateFileRenameInFlightFlag ||
            (evacuateStartedFlag &&
            ChunkDirList::IsEmpty(chunkLists[kChunkDirList]) &&
            ChunkDirList::IsEmpty(chunkLists[kChunkDirEvacuateList]))) {
        stopEvacuationFlag = false;
        return false;
    }
    if (evacuateChunksOpInFlightFlag) {
        stopEvacuationFlag = true;
        return true;
    }
    ChunkDirInfo::ChunkLists& list = chunkLists[kChunkDirEvacuateList];
    ChunkInfoHandle*          cih;
    while ((cih = ChunkDirList::Front(list))) {
        cih->SetEvacuate(false);
    }
    const bool updateSpaceAvailableFlag = evacuateStartedFlag;
    rescheduleEvacuateThreshold = 0;
    evacuateFlag                = false;
    evacuateStartedFlag         = false;
    stopEvacuationFlag          = false;
    evacuateDoneFlag            = false;
    evacuateStartChunkCount     = -1;
    evacuateStartByteCount      = -1;
    if (updateSpaceAvailableFlag) {
        gChunkManager.UpdateCountFsSpaceAvailable();
    }
    return true;
}

void
ChunkManager::MetaServerConnectionLost()
{
    mMetaEvacuateCount = -1;
    for (ChunkDirs::iterator it = mChunkDirs.begin();
            it < mChunkDirs.end(); ++it) {
        if (it->availableSpace < 0 || ! it->evacuateFlag) {
            continue;
        }
        // Take directory out of allocation now. Hello will update the
        // meta server's free space parameters used in chunk placement.
        it->SetEvacuateStarted();
        if (it->IsCountFsSpaceAvailable()) {
            UpdateCountFsSpaceAvailable();
        }
        it->RestartEvacuation();
    }
    mNextSendChunDirInfoTime = globalNetManager().Now() - 36000;
}

long
ChunkManager::GetNumWritableChunks() const
{
    return (long)mPendingWrites.GetChunkIdCount();
}

void
ChunkManager::CheckChunkDirs()
{
    KFS_LOG_STREAM_DEBUG << "Checking chunk dirs" << KFS_LOG_EOM;

    DirChecker::DirsAvailable dirs;
    mDirChecker.GetNewlyAvailable(dirs);
    bool getFsSpaceAvailFlag             = false;
    bool updateCountFsSpaceAvailableFlag = false;
    for (ChunkDirs::iterator it = mChunkDirs.begin();
            it < mChunkDirs.end(); ++it) {
        if (it->availableSpace < 0 || it->checkDirFlightFlag) {
            DirChecker::DirsAvailable::iterator const dit =
                dirs.find(it->dirname);
            if (dit == dirs.end()) {
                continue;
            }
            if (it->checkDirFlightFlag ||
                    it->bufferedIoFlag != dit->second.mBufferedIoFlag ||
                    (0 < mFileSystemId &&
                        dit->second.mFileSystemId != mFileSystemId)) {
                // Add it back, and wait in flight op completion, or re-check
                // with the current buffered io flag, or if file system id
                // still doesn't match.
                mDirChecker.Add(it->dirname, it->bufferedIoFlag);
                continue;
            }
            string errMsg;
            if (DiskIo::StartIoQueue(
                    it->dirname.c_str(),
                    dit->second.mDeviceId,
                    mMaxOpenChunkFiles,
                    &errMsg)) {
                if (! (it->diskQueue = DiskIo::FindDiskQueue(
                        it->dirname.c_str()))) {
                    die(it->dirname + ": failed to find disk queue");
                }
                it->availableSpace              = 0;
                it->fileSystemId                = dit->second.mFileSystemId;
                it->deviceId                    = dit->second.mDeviceId;
                it->dirLock                     = dit->second.mLockFdPtr;
                it->supportsSpaceReservatonFlag =
                    dit->second.mSupportsSpaceReservatonFlag;
                it->corruptedChunksCount        = 0;
                it->evacuateCheckIoErrorsCount  = 0;
                it->availableChunks.Clear();
                it->availableChunks.Swap(dit->second.mChunkInfos);
                if (it->dirCountSpaceAvailable) {
                    it->dirCountSpaceAvailable = 0;
                    updateCountFsSpaceAvailableFlag = true;
                } else if (! updateCountFsSpaceAvailableFlag) {
                    ChunkDirs::iterator cit = mChunkDirs.begin();
                    while (cit != mChunkDirs.end() &&
                            (cit == it ||
                            it->deviceId != cit->deviceId ||
                            cit->availableSpace < 0)) {
                        ++cit;
                    }
                    if (cit == mChunkDirs.end()) {
                        it->dirCountSpaceAvailable = &(*it);
                    } else if (cit->dirCountSpaceAvailable &&
                            cit->dirCountSpaceAvailable
                                ->IsCountFsSpaceAvailable() &&
                            cit->dirCountSpaceAvailable->availableSpace >= 0 &&
                            ! cit->dirCountSpaceAvailable->evacuateFlag) {
                        it->dirCountSpaceAvailable = cit->dirCountSpaceAvailable;
                    } else {
                        updateCountFsSpaceAvailableFlag = true;
                    }
                }
                KFS_LOG_STREAM_INFO <<
                    "chunk directory: "  << it->dirname <<
                    " tier: "            << (unsigned int)it->storageTier <<
                    " devId: "           << it->deviceId <<
                    " space:"
                    " used: "            << it->usedSpace <<
                    " countAvail: "      << it->IsCountFsSpaceAvailable() <<
                    " updateCntAvail: "  << updateCountFsSpaceAvailableFlag <<
                    " sprsrv: "          << it->supportsSpaceReservatonFlag <<
                    " chunks: "          << it->availableChunks.GetSize() <<
                KFS_LOG_EOM;
                StorageTiers::mapped_type& tier = mStorageTiers[it->storageTier];
                assert(find(tier.begin(), tier.end(), it) == tier.end());
                tier.push_back(&(*it));
                getFsSpaceAvailFlag = true;
                // Notify meta serve that directory is now in use.
                gMetaServerSM.EnqueueOp(
                    new CorruptChunkOp(0, -1, -1, &(it->dirname), true));
                it->Start();
                continue;
            }
            KFS_LOG_STREAM_ERROR <<
                "failed to start disk queue for: " << it->dirname <<
                " dev: << " << it->deviceId << " :" << errMsg <<
            KFS_LOG_EOM;
            // For now do not keep trying.
            // mDirChecker.Add(it->dirname, it->bufferedIoFlag);
            continue;
        }
        string err;
        it->checkDirFlightFlag = true;
        string name = it->dirname;
        if (mCheckDirWritableFlag) {
            name += mCheckDirWritableTmpFileName;
        }
        if ((mCheckDirWritableFlag ? 
            ! DiskIo::CheckDirWritable(
                name.c_str(),
                it->bufferedIoFlag,
                it->supportsSpaceReservatonFlag,
                mCheckDirTestWriteSize,
                &(it->checkDirCb),
                &err) :
            ! DiskIo::CheckDirReadable(
                name.c_str(), &(it->checkDirCb), &err))) {
            it->checkDirFlightFlag = false;
            KFS_LOG_STREAM_ERROR << "failed to queue"
                " check dir request for: " << it->dirname <<
                " : " << err <<
            KFS_LOG_EOM;
            // Do not declare directory unusable on req. queueing failure.
            // DiskIo can be temp. out of requests.
        }
    }
    if (getFsSpaceAvailFlag) {
        GetFsSpaceAvailable();
    }
    if (updateCountFsSpaceAvailableFlag) {
        UpdateCountFsSpaceAvailable();
    }
}

void
ChunkManager::GetFsSpaceAvailable()
{
    for (ChunkDirs::iterator it = mChunkDirs.begin();
            it < mChunkDirs.end(); ++it) {
        if (it->availableSpace < 0) {
            continue;
        }
        string err;
        if (! it->checkEvacuateFileInFlightFlag && ! it->stopEvacuationFlag) {
            const string fn = it->dirname + mEvacuateFileName;
            it->checkEvacuateFileInFlightFlag = true;
            if (! DiskIo::GetFsSpaceAvailable(
                    fn.c_str(), &(it->checkEvacuateFileCb), &err)) {
                it->checkEvacuateFileInFlightFlag = false;
                KFS_LOG_STREAM_ERROR << "failed to queue "
                    "fs space available request for: " << fn <<
                    " : " << err <<
                KFS_LOG_EOM;
                // Do not declare directory unusable on req. queueing failure.
                // DiskIo can be temp. out of requests.
                continue;
            }
        }
        if (it->evacuateStartedFlag &&
                mEvacuationInactivityTimeout > 0 &&
                mMetaEvacuateCount == 0 &&
                ! it->evacuateChunksOpInFlightFlag &&
                it->evacuateInFlightCount > 0 &&
                it->lastEvacuationActivityTime + mEvacuationInactivityTimeout <
                    mMetaHeartbeatTime) {
            it->RestartEvacuation();
        }
        if (it->fsSpaceAvailInFlightFlag) {
            continue;
        }
        it->fsSpaceAvailInFlightFlag = true;
        if (! DiskIo::GetFsSpaceAvailable(
                it->dirname.c_str(), &(it->fsSpaceAvailCb), &err)) {
            it->fsSpaceAvailInFlightFlag = 0;
            KFS_LOG_STREAM_ERROR << "failed to queue "
                "fs space available request for: " << it->dirname <<
                " : " << err <<
            KFS_LOG_EOM;
            // Do not declare directory unusable on req. queueing failure.
            // DiskIo can be temp. out of requests.
        }
    }
}

void
ChunkManager::MetaHeartbeat(HeartbeatOp& op)
{
    mMetaHeartbeatTime = globalNetManager().Now();
    mMetaEvacuateCount = op.metaEvacuateCount;
}

} // namespace KFS
