//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/28
// Author: Sriram Rao
//
// Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
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
#include "DiskIo.h"
#include "Replicator.h"
#include "BufferManager.h"
#include "ClientManager.h"
#include "ClientSM.h"

#include "common/MsgLogger.h"
#include "common/kfstypes.h"
#include "common/nofilelimit.h"
#include "common/IntToString.h"
#include "common/SingleLinkedQueue.h"

#include "kfsio/Counter.h"
#include "kfsio/checksum.h"
#include "kfsio/Globals.h"
#include "kfsio/blockname.h"

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
using std::lower_bound;

using libkfsio::globals;
using libkfsio::globalNetManager;

typedef QCDLList<
    ChunkInfoHandle,
    ChunkManager::kChunkInfoHandleListIndex
> ChunkList;
typedef QCDLList<
    ChunkInfoHandle,
    ChunkManager::kChunkInfoHDirListIndex
> ChunkDirList;
typedef QCDLList<
    ChunkInfoHandle,
    ChunkManager::kChunkInfoHelloNotifyListIndex
> ChunkHelloNotifyList;
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
          lostChunksCount(0),
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
          evacuateChunksOp(&evacuateChunksCb),
          availableChunksOp(&availableChunksCb),
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
        ReqOstream& Display(
            const char* inPrefixPtr,
            const char* inSuffixPtr,
            ReqOstream& inStream) const
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
            : KfsOp(CMD_CHUNKDIR_INFO),
              mChunkDir(chunkDir),
              mInFlightFlag(false),
              mResetCountersFlag(false),
              mLastSent(globalNetManager().Now()),
              mLastReadCounters(),
              mLastWriteCounters()
        {
            noReply = true;
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
            ReqOstream& inStream)
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
            "CHUNKDIR_INFO\r\n";
            if (shortRpcFormatFlag) {
                inStream << hex <<
                "c:"  << seq               << "\r\n"
                "N: " << (noReply ? 1 : 0) << "\r\n"
                "D:"  << mChunkDir.dirname << "\r\n"
                << dec;
            } else {
                inStream <<
                "Version: "   << KFS_VERSION_STR   << "\r\n"
                "Cseq: "      << seq               << "\r\n"
                "No-reply: "  << (noReply ? 1 : 0) << "\r\n"
                "Dir-name: "  << mChunkDir.dirname << "\r\n"
                ;
            }
            inStream <<
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
            "Chunks-corrupt: "     << mChunkDir.corruptedChunksCount   << "\r\n"
            "Chunks-lost: "        << mChunkDir.lostChunksCount        << "\r\n"
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
            if (EVENT_CMD_DONE != code || this != data || ! mInFlightFlag) {
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
    int64_t                lostChunksCount;
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
private:
    WriteChunkMetaOp*  next;
public:
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
        : KfsOp(CMD_WRITE_CHUNKMETA),
          chunkId(c),
          diskIo(d),
          dataBuf(),
          next(0),
          diskIOTime(0),
          targetVersion(version),
          renameFlag(rename),
          stableFlag(stable)
    {
        clnt = o;
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
            " chunk: "   << chunkId <<
            " rename:  " << renameFlag <<
            " stable:  " << stableFlag <<
            " version: " << targetVersion
        ;
    }
    // Notify the op that is waiting for the write to finish that all
    // is done
    int HandleDone(int code, void* data)
    {
        if (clnt) {
            clnt->HandleEvent(code, data);
        }
        delete this;
        return 0;
    }
    class GetNext
    {
    public:
        static WriteChunkMetaOp*& Next(WriteChunkMetaOp& op)
            { return op.next; }
        static WriteChunkMetaOp*const& Next(const WriteChunkMetaOp& op)
            { return op.next; }
    };
    friend class GetNext;
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
          mPendingAvailableFlag(false),
          mHelloNotifyFlag(false),
          mForceDeleteObjectStoreBlockFlag(false),
          mWriteIdIssuedFlag(false),
          mChunkList(ChunkManager::kChunkLruList),
          mChunkDirList(ChunkDirInfo::kChunkDirList),
          mRenamesInFlight(0),
          mWritesInFlight(0),
          mPendingSpaceReservationSize(0),
          mWriteMetaOps(),
          mReadableNotify(),
          mChunkDir(chunkdir)
    {
        ChunkList::Init(*this);
        ChunkDirList::Init(*this);
        ChunkHelloNotifyList::Init(*this);
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
        if (! mWriteMetaOps.IsEmpty() || mInDoneHandlerFlag) {
            mDeleteFlag = true;
            const bool runHanlder = ! mInDoneHandlerFlag &&
                mWritesInFlight > 0 && mWaitForWritesInFlightFlag;
            mWaitForWritesInFlightFlag = false;
            mWritesInFlight = 0;
            if (runHanlder) {
                int res = -EIO;
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
    void SetPendingAvailable(bool flag) {
        mPendingAvailableFlag = flag;
    }
    bool IsPendingAvailable() const {
        return mPendingAvailableFlag;
    }
    void SetHelloNotify(bool flag) {
        mHelloNotifyFlag = flag;
    }
    bool IsHelloNotify() const {
        return mHelloNotifyFlag;
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
            assert(! mWriteMetaOps.IsEmpty());
            mWaitForWritesInFlightFlag = false;
            int res = mWriteMetaOps.Front()->Start(this);
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
    bool DiscardMeta() {
        if (! mWriteMetaOps.IsEmpty() || 0 < mWritesInFlight) {
            return false;
        }
        mMetaDirtyFlag = false;
        return true;
    }
    bool SyncMeta() {
        if (! mWriteMetaOps.IsEmpty() || 0 < mWritesInFlight) {
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
    int WriteChunkMetadata(KfsCallbackObj* cb = 0) {
        return WriteChunkMetadata(cb, false, mStableFlag,
            (mStableFlag || chunkInfo.chunkVersion < 0) ?
                chunkInfo.chunkVersion : kfsSeq_t(0));
    }
    kfsSeq_t GetTargetStateAndVersion(bool& stableFlag) const {
        if (mWriteMetaOps.IsEmpty() || mRenamesInFlight <= 0) {
            stableFlag = mStableFlag;
            return chunkInfo.chunkVersion;
        }
        if (mWriteMetaOps.Back()->renameFlag) {
            stableFlag = mWriteMetaOps.Back()->stableFlag;
            return mWriteMetaOps.Back()->targetVersion;
        }
        stableFlag = mStableFlag;
        kfsSeq_t theRet = chunkInfo.chunkVersion;
        for (const WriteChunkMetaOp* op = mWriteMetaOps.Front();
                op; op = WriteMetaOps::GetNext(*op)) {
            if (op->renameFlag) {
                theRet = op->targetVersion;
                stableFlag = mWriteMetaOps.Back()->stableFlag;
            }
        }
        return theRet;
    }
    bool CanHaveSameFileName(const ChunkInfo_t& info, bool stableFlag) const {
        // Given that single meta op can be in flight at a time, see if the
        // front op is rename, and if it is, then use its stable flag and
        // target version. Non-op renames cannot possibly be in flight as
        // these are never started / queued into disk IO, instead completion
        // for non op rename is invoked directly by the start method.
        const WriteChunkMetaOp* op;
        return (
            chunkInfo.fileId  == info.fileId  &&
            chunkInfo.chunkId == info.chunkId &&
            (((op = mWriteMetaOps.Front()) && op->IsRenameNeeded(this)) ?
                op->stableFlag    == stableFlag &&
                op->targetVersion == info.chunkVersion
                :
                chunkInfo.chunkVersion == info.chunkVersion &&
                mStableFlag            == stableFlag
            )
        );
    }
    bool CanHaveVersion(kfsSeq_t vers) const {
        if (vers == chunkInfo.chunkVersion) {
            return true;
        }
        for (const WriteChunkMetaOp* op = mWriteMetaOps.Front();
                op; op = WriteMetaOps::GetNext(*op)) {
            if (op->renameFlag && vers == op->targetVersion) {
                return true;
            }
        }
        return false;
    }
    bool IsChunkReadable(bool ignorePendingAvailable = false) const {
        return (mWriteMetaOps.IsEmpty() && mStableFlag && mWritesInFlight <= 0 &&
            (ignorePendingAvailable || ! mPendingAvailableFlag));
    }
    bool IsRenameInFlight() const {
        return (0 < mRenamesInFlight);
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
    void MakeStale(ChunkLists* chunkInfoLists, bool keepFlag, KfsOp* op)
    {
        if (IsStale()) {
            if (op) {
                die("invalid chunk handle make stable invocation");
            }
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
        WaitChunkReadableDone(-EIO);
        if (op) {
            if (! mReadableNotify.IsEmpty()) {
                die("non empty readable notify");
            }
            mReadableNotify.PushBack(*op);
        }
    }
    KfsOp* DetachStaleDeleteCompletionOp()
    {
        if (! IsStale()) {
            return 0;
        }
        KfsOp* const op = mReadableNotify.PopFront();
        if (! mReadableNotify.IsEmpty()) {
            die("detach stale completion non empty readable notify");
        }
        return op;
    }
    void UpdateStale(ChunkLists* chunkInfoLists) {
        const bool evacuateFlag = IsEvacuate();
        ChunkList::Remove(chunkInfoLists[mChunkList], *this);
        mChunkList = 0 < mRenamesInFlight ?
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
        if (0 <= mChunkDir.availableSpace && 0 < writeSize) {
            mChunkDir.writeCounters.Update(status, writeSize, ioTimeMicrosec);
            mChunkDir.totalWriteCounters.Update(
                status, writeSize, ioTimeMicrosec);
        }
    }
    void UpdateDirStableCount() {
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
    void AddWaitChunkReadable(KfsOp* op) {
        if (! op) {
            die("invalid AddWaitChunkReadable invocation");
            return;
        }
        mReadableNotify.PushBack(*op);
        if (IsChunkReadable()) {
            WaitChunkReadableDone(IsStale() ? -EIO : 0);
        }
    }
    void SubmitWaitChunkReadable(int status) {
        WaitChunkReadableDone(status);
    }
    void SetForceDeleteObjectStoreBlock(bool flag) {
        mForceDeleteObjectStoreBlockFlag = flag;
    }
    bool GetForceDeleteObjectStoreBlockFlag() const {
        return mForceDeleteObjectStoreBlockFlag;
    }
    void SetWriteIdIssuedFlag(bool flag) {
        mWriteIdIssuedFlag = flag;
    }
    bool GetWriteIdIssuedFlag() const {
        return mWriteIdIssuedFlag;
    }
    inline bool ScheduleObjTableCleanup(
        ChunkLists* chunkInfoLists);

private:
    typedef SingleLinkedQueue<
        WriteChunkMetaOp,
        WriteChunkMetaOp::GetNext
    > WriteMetaOps;
    typedef SingleLinkedQueue<
        KfsOp,
        KfsOp::GetNext
    > ReadableNotify;

    bool                        mBeingReplicatedFlag:1;
    bool                        mDeleteFlag:1;
    bool                        mWriteAppenderOwnsFlag:1;
    bool                        mWaitForWritesInFlightFlag:1;
    bool                        mMetaDirtyFlag:1;
    bool                        mStableFlag:1;
    bool                        mInDoneHandlerFlag:1;
    bool                        mKeepFlag:1;
    bool                        mPendingAvailableFlag:1;
    bool                        mHelloNotifyFlag:1;
    bool                        mForceDeleteObjectStoreBlockFlag:1;
    bool                        mWriteIdIssuedFlag:1;
    ChunkManager::ChunkListType mChunkList:2;
    ChunkDirInfo::ChunkListType mChunkDirList:2;
    // Chunk meta data updates need to be executed in order, allow only one
    // meta data op in flight.
    int                         mRenamesInFlight;
    int                         mWritesInFlight;
    int                         mPendingSpaceReservationSize;
    WriteMetaOps                mWriteMetaOps;
    ReadableNotify              mReadableNotify;
    ChunkDirInfo&               mChunkDir;
    ChunkInfoHandle*            mPrevPtr[ChunkManager::kChunkInfoAllListCount];
    ChunkInfoHandle*            mNextPtr[ChunkManager::kChunkInfoAllListCount];

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
    int HandleChunkMetaWriteDone(int code, void* data);
    virtual ~ChunkInfoHandle() {
        if (! mWriteMetaOps.IsEmpty()) {
            // Object is the "client" of this op.
            die("attempt to delete chunk info handle "
                "with meta data write in flight");
        }
        KfsOp* const op = DetachStaleDeleteCompletionOp();
        if (op) {
            int res = -EIO;
            op->HandleEvent(EVENT_DISK_ERROR, &res);
        }
        if (! mReadableNotify.IsEmpty()) {
            WaitChunkReadableDone(
                (IsStale() || ! IsChunkReadable()) ? -EIO : 0);
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
            if (mWriteMetaOps.IsEmpty()) {
                if (! mReadableNotify.IsEmpty()) {
                    WaitChunkReadableDone(
                        (IsStale() || ! IsChunkReadable()) ? -EIO : 0);
                }
                if (IsStale()) {
                    gChunkManager.UpdateStale(*this);
                } else {
                    delete this;
                }
            }
        } else {
            gChunkManager.LruUpdate(*this);
            if (! mReadableNotify.IsEmpty() && IsChunkReadable()) {
                WaitChunkReadableDone();
            }
        }
    }
    void WaitChunkReadableDone(int status = 0) {
        ReadableNotify notify;
        notify.PushBack(mReadableNotify);
        KfsOp* op;
        while ((op = notify.PopFront())) {
            if (0 == status) {
                op->HandleEvent(EVENT_CMD_DONE, op);
            } else {
                int res = status;
                op->HandleEvent(EVENT_DISK_ERROR, &res);
            }
        }
    }
    friend class QCDLListOp<ChunkInfoHandle,
        ChunkManager::kChunkInfoHandleListIndex>;
    friend class QCDLListOp<ChunkInfoHandle,
        ChunkManager::kChunkInfoHDirListIndex>;
    friend class QCDLListOp<ChunkInfoHandle,
        ChunkManager::kChunkInfoHelloNotifyListIndex>;
private:
    ChunkInfoHandle(const  ChunkInfoHandle&);
    ChunkInfoHandle& operator=(const  ChunkInfoHandle&);
};

class ChunkManager::StaleChunkDeleteCompletion : public KfsCallbackObj
{
public:
    enum
    {
        kFreeList     = 0,
        kInFlightList = 1,
        kListCount    = 2
    };
    typedef QCDLList<StaleChunkDeleteCompletion> List;
    typedef StaleChunkDeleteCompletion*          ListHead[1];
    typedef ListHead                             Lists[kListCount];

    static StaleChunkDeleteCompletion& Create(
        KfsCallbackObj* cb,
        Lists           lists)
    {
        StaleChunkDeleteCompletion* ret = List::PopFront(lists[kFreeList]);
        if (ret) {
            ret->mCbPtr = cb;
        } else {
            ret = new StaleChunkDeleteCompletion(cb);
        }
        List::PushBack(lists[kInFlightList], *ret);
        return *ret;
    }
    static void Cleanup(
        Lists lists)
    {
        if (! List::IsEmpty(lists[kInFlightList])) {
            die("chunk stale delete destroy: non empty in flight list");
        }
        StaleChunkDeleteCompletion* cur;
        while ((cur =  List::PopFront(lists[kFreeList]))) {
            delete cur;
        }
    }
    static void Release(
        StaleChunkDeleteCompletion& cb,
        Lists                       lists)
    {
        KfsCallbackObj* const op = cb.mCbPtr;
        cb.mCbPtr = 0;
        List::Remove(lists[kInFlightList], cb);
        List::PushBack(lists[kFreeList],   cb);
        if (op) {
            int res = -EIO;
            op->HandleEvent(EVENT_DISK_ERROR, &res);
        }
    }
    static void Init(
        Lists lists,
        int   freeListSize)
    {
        for (int i = 0; i < kListCount; i++) {
            List::Init(lists[i]);
        }
        for (int i = 0; i < freeListSize; i++) {
            Release(Create(0, lists), lists);
        }
    }
private:
    KfsCallbackObj*             mCbPtr;
    StaleChunkDeleteCompletion* mPrevPtr[1];
    StaleChunkDeleteCompletion* mNextPtr[1];

    friend class QCDLListOp<StaleChunkDeleteCompletion, 0>;

    StaleChunkDeleteCompletion(
        KfsCallbackObj* cb)
        : KfsCallbackObj(),
          mCbPtr(cb)
    {
        List::Init(*this);
        SET_HANDLER(this, &StaleChunkDeleteCompletion::Done);
    }
    virtual ~StaleChunkDeleteCompletion()
    {
        if (mCbPtr) {
            die("StaleChunkDeleteCompletion: invalid destructor invocation");
        }
    }
    int Done(int code, void* data)
    {
        KfsCallbackObj* const cb = mCbPtr;
        mCbPtr = 0;
        const int ret = cb ? cb->HandleEvent(code, data) : 0;
        gChunkManager.RunStaleChunksQueue(this);
        return ret;
    }
};

inline void
ForceMetaServerDown(const KfsOp& op)
{
    KFS_LOG_STREAM_ERROR <<
        "forcing meta server connection down due to protocol"
        " or communication error:"
        " status: " << op.status <<
        " "         << op.statusMsg <<
        " "         << op.Show() <<
    KFS_LOG_EOM;
    gMetaServerSM.ForceDown();
}

inline bool
ChunkManager::InsertLastInFlight(kfsChunkId_t chunkId)
{
    bool insertedFlag = false;
    mLastPendingInFlight.Insert(chunkId, insertedFlag);
    return insertedFlag;
}

inline bool
ChunkManager::NotifyLostChunk(kfsChunkId_t chunkId, kfsSeq_t vers)
{
    if (chunkId <= 0) {
        return false;
    }
    bool insertedFlag = false;
    if (! mPendingNotifyLostChunks.Insert(chunkId, vers, insertedFlag)) {
        die("failed to insert chunk entry in pending notify list");
    }
    return insertedFlag;
}

inline bool
ChunkManager::ScheduleNotifyLostChunk()
{
    if (mPendingNotifyLostChunks.IsEmpty() ||
            mCorruptChunkOp.notifyChunkManagerFlag || // In flight.
            ! gMetaServerSM.IsUp()) {
        return false;
    }
    mCorruptChunkOp.isChunkLost = true;
    mPendingNotifyLostChunks.First();
    const PendingNotifyLostChunks::KVPair* p;
    for (mCorruptChunkOp.chunkCount = 0;
            mCorruptChunkOp.chunkCount < CorruptChunkOp::kMaxChunkIds &&
                ((p = mPendingNotifyLostChunks.Next()));
            mCorruptChunkOp.chunkCount++) {
        const kfsChunkId_t chunkId = p->GetKey();
        if (mChunkTable.Find(chunkId)) {
            die("invalid pending stale notify chunk list");
        } else {
            mCorruptChunkOp.chunkIds[mCorruptChunkOp.chunkCount] = chunkId;
        }
    }
    if (mCorruptChunkOp.chunkCount <= 0) {
        return false;
    }
    mCorruptChunkOp.notifyChunkManagerFlag = true;
    gMetaServerSM.EnqueueOp(&mCorruptChunkOp);
    return true;
}

void
ChunkManager::NotifyStaleChunkDone(CorruptChunkOp& op)
{
    if (&op != &mCorruptChunkOp || ! mCorruptChunkOp.notifyChunkManagerFlag) {
        die("invalid corrupt chunk op completion");
        return;
    }
    mCorruptChunkOp.notifyChunkManagerFlag = false;
    bool upFlag = gMetaServerSM.IsUp();
    KFS_LOG_STREAM_DEBUG <<
        "done:"
        " status: "  << mCorruptChunkOp.status <<
        " "          << mCorruptChunkOp.statusMsg <<
        " meta up: " << upFlag <<
        " "          << mCorruptChunkOp.Show() <<
    KFS_LOG_EOM;
    if (upFlag && 0 != op.status) {
        ForceMetaServerDown(op);
        upFlag = false;
    }
    if (! upFlag) {
        while (0 < mCorruptChunkOp.chunkCount) {
            InsertLastInFlight(
                mCorruptChunkOp.chunkIds[--mCorruptChunkOp.chunkCount]);
        }
        return;
    }
    if (mPendingNotifyLostChunks.IsEmpty()) {
        return;
    }
    while (0 < mCorruptChunkOp.chunkCount) {
        mPendingNotifyLostChunks.Erase(
            mCorruptChunkOp.chunkIds[--mCorruptChunkOp.chunkCount]);
    }
    if (! mPendingNotifyLostChunks.IsEmpty()) {
        ScheduleNotifyLostChunk();
    }
}

void
ChunkManager::HelloDone()
{
    if (! gMetaServerSM.IsUp()) {
        return;
    }
    mLastPendingInFlight.Clear();
    // Re-enable clenup if it was disabled on startup.
    if (! mCleanupStaleChunksFlag) {
        mCleanupStaleChunksFlag = true;
        RunStaleChunksQueue();
    }
    ScheduleNotifyLostChunk();
    RunHelloNotifyQueue(0);
}

inline bool
ChunkInfoHandle::ScheduleObjTableCleanup(
    ChunkInfoHandle::ChunkLists* chunkInfoLists)
{
    if (0 <= chunkInfo.chunkVersion ||
            ChunkManager::kChunkLruList != mChunkList) {
        return false;
    }
    // Move to the front of the lru to schedule removal from the object
    // block table.
    lastIOTime = globalNetManager().Now() - 60 * 60 * 24 * 365 * 10;
    ChunkList::PushFront(chunkInfoLists[mChunkList], *this);
    return true;
}

inline ChunkInfoHandle*
ChunkManager::AddMapping(ChunkInfoHandle* cih)
{
    bool newEntryFlag = false;
    ChunkInfoHandle** const ci = 0 <= cih->chunkInfo.chunkVersion ?
        mChunkTable.Insert(cih->chunkInfo.chunkId, cih, newEntryFlag) :
        mObjTable.Insert(make_pair(
                cih->chunkInfo.chunkId, cih->chunkInfo.chunkVersion),
            cih, newEntryFlag);
    if (! ci) {
        die("add mapping failure");
        return 0; // Eliminate lint warning.
    }
    if (0 <= cih->chunkInfo.chunkVersion &&
            0 < mPendingNotifyLostChunks.Erase(cih->chunkInfo.chunkId) &&
            ! newEntryFlag) {
        die("chunk entry was in pending lost notify chunks");
    }
    if (! newEntryFlag) {
        return *ci;
    }
    if (0 <= cih->chunkInfo.chunkVersion) {
        mUsedSpace += cih->chunkInfo.chunkSize;
    }
    UpdateDirSpace(cih, cih->chunkInfo.chunkSize);
    if (cih->chunkInfo.chunkVersion < 0 &&
            ! cih->ScheduleObjTableCleanup(mChunkInfoLists)) {
        die("add object table mapping schedule cleanup failure");
    }
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
            mWriteMetaOps.IsEmpty()) {
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
ChunkManager::HelloNotifyRemove(ChunkInfoHandle& cih)
{
    if (! ChunkHelloNotifyList::IsEmpty(mChunkInfoHelloNotifyList)) {
        ChunkHelloNotifyList::Remove(mChunkInfoHelloNotifyList, cih);
    }
}

inline bool
ChunkManager::IsPendingHelloNotify(const ChunkInfoHandle& cih) const
{
    return ChunkHelloNotifyList::IsInList(mChunkInfoHelloNotifyList, cih);
}

inline void
ChunkManager::DeleteSelf(ChunkInfoHandle& cih)
{
    if (0 <= cih.chunkInfo.chunkVersion) {
        HelloNotifyRemove(cih);
    }
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
ChunkManager::RemoveFromChunkTable(ChunkInfoHandle& cih)
{
    if (mChunkTable.Erase(cih.chunkInfo.chunkId) <= 0) {
        return false;
    }
    HelloNotifyRemove(cih);
    return true;
}

inline bool
ChunkManager::RemoveFromTable(ChunkInfoHandle& cih)
{
    return (0 <= cih.chunkInfo.chunkVersion ?
        RemoveFromChunkTable(cih) :
        0 < mObjTable.Erase(make_pair(
            cih.chunkInfo.chunkId, cih.chunkInfo.chunkVersion))
    );
}

inline bool
ChunkManager::Remove(ChunkInfoHandle& cih)
{
    if (mPendingWrites.HasChunkId(
            cih.chunkInfo.chunkId, cih.chunkInfo.chunkVersion)) {
        return false;
    }
    if (! RemoveFromTable(cih)) {
        return false;
    }
    if (cih.chunkInfo.chunkVersion < 0) {
        UpdateDirSpace(&cih, -cih.chunkInfo.chunkSize);
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
    bool forceDeleteFlag, bool evacuatedFlag, KfsOp* op)
{
    if (! cih.IsStale()) {
        mStaleChunksCount++;
    }
    cih.MakeStale(mChunkInfoLists,
        (! forceDeleteFlag && ! mForceDeleteStaleChunksFlag) ||
        (evacuatedFlag && mKeepEvacuatedChunksFlag),
        op
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
    if (IsFileInUse()) {
        die("invalid release attempt: file has pending IOs");
        return;
    }
    string errMsg;
    const void* const logFH = dataFH.get();
    if (! dataFH->Close(
            0 <= chunkInfo.chunkVersion ?
                chunkInfo.chunkSize + chunkInfo.GetHeaderSize() : int64_t(-1),
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
        "closing chunk " << chunkInfo.chunkId <<
        " version: "     << chunkInfo.chunkVersion <<
        " file handle: " << logFH <<
        " and might give up lease" <<
    KFS_LOG_EOM;
    gLeaseClerk.RelinquishLease(
        chunkInfo.chunkId, chunkInfo.chunkVersion, chunkInfo.chunkSize);
    if (0 <= chunkInfo.chunkVersion ||
            ! ScheduleObjTableCleanup(chunkInfoLists)) {
        ChunkList::Remove(chunkInfoLists[mChunkList], *this);
    }
    globals().ctrOpenDiskFds.Update(-1);
}

inline bool
WriteChunkMetaOp::IsRenameNeeded(const ChunkInfoHandle* cih) const
{
    return (
        renameFlag &&
        ((cih->IsStable() && cih->chunkInfo.chunkVersion != targetVersion) ||
        (0 <= cih->chunkInfo.chunkVersion && cih->IsStable() != stableFlag))
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
        if (cih->chunkInfo.chunkVersion < 0 &&
                (! stableFlag || cih->IsStale() ||
                    targetVersion != cih->chunkInfo.chunkVersion)) {
            // Never write non stable header or version change for object
            // store block.
            int res = 0;
            cih->HandleEvent(EVENT_DISK_WROTE, &res);
            return 0;
        }
        status = diskIo->Write(0, dataBuf.BytesConsumable(), &dataBuf,
            (gChunkManager.IsSyncChunkHeader() &&
                (0 < targetVersion || stableFlag)) ||
            (stableFlag && cih->dataFH &&
                0 < cih->dataFH->GetMinWriteBlkSize()),
            cih->chunkInfo.GetHeaderSize() + cih->chunkInfo.chunkSize
        );
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
    if (renameFlag && mRenamesInFlight + 1 <= 0) {
        // Overflow: too many renames in flight.
        return -ESERVERBUSY;
    }
    // If chunk is not stable and is not transitioning into stable, and there
    // are no pending ops, just assign the version and mark meta dirty.
    if ((chunkInfo.chunkVersion < 0 ||
            (targetVersion > 0 && chunkInfo.chunkVersion != targetVersion)) &&
            mWritesInFlight <= 0 &&
            ! IsStable() && ! stableFlag && mWriteMetaOps.IsEmpty() &&
            ! mInDoneHandlerFlag && IsFileOpen() &&
            ! mDeleteFlag && ! IsStale()) {
        mMetaDirtyFlag         = true;
        chunkInfo.chunkVersion = targetVersion;
        if (cb) {
            if (renameFlag) {
                int64_t res = 0;
                cb->HandleEvent(EVENT_DISK_RENAME_DONE, &res);
            } else {
                int res = 0;
                cb->HandleEvent(EVENT_DISK_WROTE, &res);
            }
        }
        UpdateState();
        return 0;
    }
    if (renameFlag) {
        // Queue the version update first, then immediately queue rename.
        // Not stable chunks on disk always have version 0.
        mMetaDirtyFlag = true;
        const int ret = WriteChunkMetadata(
            0, false, stableFlag,
            (stableFlag || chunkInfo.chunkVersion < 0) ?
                targetVersion : kfsSeq_t(0)
        );
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
            if (mWriteMetaOps.IsEmpty()) {
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
            assert(! mWriteMetaOps.IsEmpty());
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
        const int headerSize = (int)chunkInfo.GetHeaderSize();
        if (headerSize < wcm->dataBuf.BytesConsumable()) {
            die("invalid io buffer size");
        }
        const int headerTailSize =
            min(dataFH->GetMinWriteBlkSize(), headerSize) -
            wcm->dataBuf.BytesConsumable();
        if (0 < headerTailSize) {
            wcm->dataBuf.ZeroFill(headerTailSize);
        }
    }
    if (wcm->renameFlag) {
        mRenamesInFlight++;
        assert(0 < mRenamesInFlight);
    }
    const bool wasEmptyFlag = mWriteMetaOps.IsEmpty();
    mWriteMetaOps.PushBack(*wcm);
    if (! wasEmptyFlag) {
        return 0;
    }
    if (0 < mWritesInFlight) {
        mWaitForWritesInFlightFlag = true;
        return 0;
    }
    const int res = wcm->Start(this);
    if (res < 0) {
        if (mWriteMetaOps.PopFront() != wcm) {
            die("invalid write meta queue");
        }
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
        assert(! mWriteMetaOps.IsEmpty());
        int status = (EVENT_DISK_ERROR == code || EVENT_DISK_WROTE == code) ?
            (data ? *reinterpret_cast<const int*>(data) : -EIO) : 0;
        if (EVENT_DISK_ERROR == code && 0 <= status) {
            status = -EIO;
        }
        if ((! mDeleteFlag && ! IsStale()) && status < 0) {
            KFS_LOG_STREAM_ERROR << mWriteMetaOps.Front()->Show() <<
                " failed: status: " << status <<
                " op: status: "     << mWriteMetaOps.Front()->status <<
                " msg: "            << mWriteMetaOps.Front()->statusMsg <<
            KFS_LOG_EOM;
            if (! mBeingReplicatedFlag) {
                gChunkManager.ChunkIOFailed(this, status);
            }
        }
        if (0 <= mWriteMetaOps.Front()->status) {
            mWriteMetaOps.Front()->status = status;
        }
        if (mWriteMetaOps.Front()->renameFlag) {
            assert(0 < mRenamesInFlight);
            mRenamesInFlight--;
            if (mWriteMetaOps.Front()->status == 0) {
                if (EVENT_DISK_RENAME_DONE != code) {
                    ostringstream os;
                    os << "chunk meta write completion:"
                        " unexpected event code: " << code;
                    die(os.str());
                }
                const bool updateFlag =
                    mWriteMetaOps.Front()->stableFlag != mStableFlag &&
                    IsFileOpen();
                mStableFlag = mWriteMetaOps.Front()->stableFlag;
                chunkInfo.chunkVersion = mWriteMetaOps.Front()->targetVersion;
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
            WriteStats(status, ChunkHeaderBuffer::GetSize(),
                max(int64_t(0), nowUsec - mWriteMetaOps.Front()->diskIOTime));
            mWriteMetaOps.Front()->diskIOTime = nowUsec;
        }
        WriteChunkMetaOp* const cur = mWriteMetaOps.PopFront();
        const bool doneFlag = mWriteMetaOps.IsEmpty();
        cur->HandleEvent(code, data);
        if (doneFlag) {
            break;
        }
        if (mWriteMetaOps.Front()->IsWaiting()) {
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
        } else if (mWriteMetaOps.Front()->renameFlag &&
                ! mWriteMetaOps.Front()->IsRenameNeeded(this)) {
            res  = 0;
            data = &res;
            code = EVENT_DISK_RENAME_DONE;
            continue;
        } else if (0 <= (err = mWriteMetaOps.Front()->Start(this))) {
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
    : mMaxPendingWriteLruSecs(LEASE_INTERVAL_SECS),
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
      mStorageTiers(),
      mObjDirs(),
      mObjStorageTiers(),
      mObjectStoreErrorMsg(),
      mObjectStoreStatus(0),
      mWriteId(GetRandomSeq()), // Seed write id.
      mPendingWrites(),
      mChunkTable(),
      mObjTable(),
      mMaxIORequestSize(4 << 20),
      mChunkInfoHelloNotifyList(),
      mHelloNotifyCb(),
      mHelloNotifyInFlightCount(0),
      mMaxHelloNotifyInFlightCount(10 << 10),
      mNextChunkDirsCheckTime(globalNetManager().Now() - 360000),
      mChunkDirsCheckIntervalSecs(120),
      mNextGetFsSpaceAvailableTime(globalNetManager().Now() - 360000),
      mGetFsSpaceAvailableIntervalSecs(25),
      mNextSendChunDirInfoTime(globalNetManager().Now() -360000),
      mSendChunDirInfoIntervalSecs(2 * 60),
      mInactiveFdsCleanupIntervalSecs(LEASE_INTERVAL_SECS),
      mNextInactiveFdCleanupTime(globalNetManager().Now() - 365 * 24 * 60 * 60),
      mInactiveFdFullScanIntervalSecs(2),
      mNextInactiveFdFullScanTime(globalNetManager().Now() - 365 * 24 * 60 * 60),
      mReadChecksumMismatchMaxRetryCount(0),
      mAbortOnChecksumMismatchFlag(false),
      mRequireChunkHeaderChecksumFlag(false),
      mForceDeleteStaleChunksFlag(false),
      mKeepEvacuatedChunksFlag(false),
      mStaleChunkOpsInFlight(0),
      mMaxStaleChunkOpsInFlight(4),
      mRunStaleQueueRecursionCount(0),
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
      mObjStorageTiersPrefixes(),
      mObjStorageTiersSetFlag(false),
      mBufferedIoPrefixes(),
      mBufferedIoSetFlag(false),
      mDiskBufferManagerEnabledFlag(true),
      mForceVerifyDiskReadChecksumFlag(false),
      mWritePrepareReplyFlag(true),
      mCryptoKeys(globalNetManager(), 0),
      mFileSystemId(-1),
      mFileSystemIdSuffix(),
      mFsIdFileNamePrefix("0-fsid-"),
      mDirCheckerIoTimeoutSec(-1),
      mDirCheckFailureSimulatorInterval(-1),
      mChunkSizeSkipHeaderVerifyFlag(false),
      mVersionChangePermitWritesInFlightFlag(true),
      mMinChunkCountForHelloResume(1 << 10),
      mHelloResumeFailureTraceFileName(),
      mPendingNotifyLostChunks(),
      mCorruptChunkOp(-1),
      mLastPendingInFlight(),
      mCleanupStaleChunksFlag(true),
      mDiskIoRequestAffinityFlag(false),
      mDiskIoSerializeMetaRequestsFlag(true),
      mObjStoreIoRequestAffinityFlag(true),
      mObjStoreIoSerializeMetaRequestsFlag(false),
      mObjStoreBlockWriteBufferSize(5 << 20), // Min S3 multi part upload.
      mObjStoreBufferDataIgnoreOverwriteFlag(true),
      mObjStoreMaxWritableBlocks(-1),
      mObjStoreBufferDataRatio(0.3),
      mObjStoreBufferDataMaxSizePerBlock(mObjStoreBlockWriteBufferSize),
      mObjStoreBlockMaxNonStableDisconnectedTime(LEASE_INTERVAL_SECS * 3 / 2),
      mObjBlockDiscardMinMetaUptime(90),
      mObjStoreIoThreadCount(-1),
      mRand(),
      mFlushStaleChunksOp(0),
      mFlushStaleChunksCount(0),
      mDoneStaleChunksCount(0),
      mStaleChunksCount(0),
      mResumeHelloMaxPendingStaleCount(16 << 10),
      mLogChunkServerCountersInterval(60),
      mLogChunkServerCountersLastTime(globalNetManager().Now() - 365 * 24 * 60 * 60),
      mLogChunkServerCountersLogLevel(MsgLogger::kLogLevelNOTICE),
      mChunkHeaderBuffer()
{
    mDirChecker.SetInterval(180 * 1000);
    srand48((long)globalNetManager().Now());
    for (int i = 0; i < kChunkInfoListCount; i++) {
        ChunkList::Init(mChunkInfoLists[i]);
    }
    ChunkHelloNotifyList::Init(mChunkInfoHelloNotifyList);
    globalNetManager().SetMaxAcceptsPerRead(4096);
    mHelloNotifyCb.SetHandler(this, &ChunkManager::HelloNotifyDone);
    StaleChunkDeleteCompletion::Init(
        mStaleChunkDeleteCompletionLists, mMaxStaleChunkOpsInFlight);
}

ChunkManager::~ChunkManager()
{
    assert(mChunkTable.IsEmpty());
    assert(mObjTable.IsEmpty());
    globalNetManager().UnRegisterTimeoutHandler(this);
    StaleChunkDeleteCompletion::Cleanup(mStaleChunkDeleteCompletionLists);
}

template<typename T> void
ChunkManager::ClearTable(T& table)
{
    T                                    tmp;
    typename T::Entry::value_type const* p;
    table.First();
    while ((p = table.Next())) {
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
    table.Clear();
    table.Swap(tmp);
}

template<typename T> void
ChunkManager::RunIoCompletion(T& table)
{
    typename T::Entry::value_type const* p;
    for (int i = 0; ;) {
        table.First();
        kfsChunkId_t chunkId      = -1;
        int64_t      version      = -1;
        int          requestCount = -1;
        while ((p = table.Next())) {
            ChunkInfoHandle* const cih = p->GetVal();
            if (! cih) {
                table.Erase(p->GetKey());
                continue;
            }
            const bool fileInUseFlag = cih->IsFileInUse();
            if (fileInUseFlag) {
                int     freeRequestCount;
                int64_t readBlockCount;
                int64_t writeBlockCount;
                int     blockSize;
                cih->dataFH->GetDiskQueuePendingCount(
                    freeRequestCount,
                    requestCount,
                    readBlockCount,
                    writeBlockCount,
                    blockSize
                );
                if (0 < requestCount) {
                    chunkId = cih->chunkInfo.chunkId;
                    version = cih->chunkInfo.chunkVersion;
                    break;
                }
            }
            table.Erase(p->GetKey());
            if (! fileInUseFlag) {
                Release(*cih);
            }
            Delete(*cih);
        }
        const bool completionFlag = DiskIo::RunIoCompletion();
        if (table.IsEmpty()) {
            break;
        }
        if (completionFlag) {
            continue;
        }
        if (++i > 6000) {
            KFS_LOG_STREAM_ERROR <<
                "ChunkManager::Shutdown"
                " attempts: "   << i <<
                " table size: " << table.GetSize() <<
                " requests: "   << requestCount <<
                " chunk: "      << chunkId <<
                " version: "    << version <<
            KFS_LOG_EOM;
            break;
        }
        usleep(10000);
    }
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
        if (++i > 6000) {
            KFS_LOG_STREAM_ERROR <<
                "ChunkManager::Shutdown pending delete timeout" <<
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
    ClearTable(mObjTable);
    ClearTable(mChunkTable);
    gAtomicRecordAppendManager.Shutdown();
    RunIoCompletion(mObjTable);
    RunIoCompletion(mChunkTable);
    gClientManager.Shutdown();
    Replicator::Shutdown();
    for (int i = 0; i < 256; i++) {
        DiskIo::RunIoCompletion();
        gClientManager.Shutdown();
        if (! DiskIo::RunIoCompletion()) {
            break;
        }
    }
    globalNetManager().UnRegisterTimeoutHandler(this);
    mCryptoKeys.Stop();
    string errMsg;
    if (! DiskIo::Shutdown(&errMsg)) {
        KFS_LOG_STREAM_INFO <<
            "DiskIo::Shutdown failure: " << errMsg <<
        KFS_LOG_EOM;
    }
    gClientManager.Shutdown();
}

bool
ChunkManager::IsWriteAppenderOwns(
    kfsChunkId_t chunkId, int64_t chunkVersion) const
{
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const ci = GetChunkInfoHandle(chunkId, chunkVersion,
        kAddObjectBlockMappingFlag);
    return (ci && ci->IsWriteAppenderOwns());
}

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

    NetManager& netManager = globalNetManager();
    netManager.SetMaxAcceptsPerRead(prop.getValue(
        "chunkServer.net.maxAcceptsPerRead",
        netManager.GetMaxAcceptsPerRead()));
    const bool useOsResolverFlag = prop.getValue(
        "chunkServer.useOsResolver",
        netManager.GetResolverOsFlag() ? 1 : 0) != 0;
    const int maxCacheSize = prop.getValue(
        "chunkServer.resolverMaxCacheSize",
        netManager.GetResolverCacheSize());
    const int resolverCacheExpiration = prop.getValue(
       "chunkServer.resolverCacheExpiration",
        netManager.GetResolverCacheExpiration());
    netManager.SetResolverParameters(
        useOsResolverFlag, maxCacheSize, resolverCacheExpiration);

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

    const time_t now = netManager.Now();
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
    mMinChunkCountForHelloResume = prop.getValue(
        "chunkServer.minChunkCountForHelloResume",
        mMinChunkCountForHelloResume);
    mMaxHelloNotifyInFlightCount = prop.getValue(
        "chunkServer.maxHelloNotifyInFlightCount",
        mMaxHelloNotifyInFlightCount);
    mHelloResumeFailureTraceFileName = prop.getValue(
        "chunkServer.helloResumeFailureTraceFileName",
        mHelloResumeFailureTraceFileName);
    mDiskIoRequestAffinityFlag = prop.getValue(
        "chunkServer.diskIoRequestAffinity",
        mDiskIoRequestAffinityFlag ? 1 : 0) != 0;
    mDiskIoSerializeMetaRequestsFlag = prop.getValue(
        "chunkServer.diskIoSerializeMetaRequestsFlag",
        mDiskIoSerializeMetaRequestsFlag ? 1 : 0) != 0;
    mObjStoreIoRequestAffinityFlag = prop.getValue(
        "chunkServer.ObjStoreIoRequestAffinity",
        mObjStoreIoRequestAffinityFlag ? 1 : 0) != 0;
    mObjStoreIoSerializeMetaRequestsFlag = prop.getValue(
        "chunkServer.ObjStoreIoSerializeMetaRequestsFlag",
        mObjStoreIoSerializeMetaRequestsFlag ? 1 : 0) != 0;
    mObjStoreBlockWriteBufferSize = prop.getValue(
        "chunkServer.objStoreBlockWriteBufferSize",
        mObjStoreBlockWriteBufferSize);
    mObjStoreIoThreadCount = prop.getValue(
        "chunkServer.objStoreIoThreadCount",
        mObjStoreIoThreadCount);
    mLogChunkServerCountersInterval = prop.getValue(
        "chunkServer.logChunkServerCountersInterval",
        mLogChunkServerCountersInterval);
    const Properties::String* const logLevelStr = prop.getValue(
        "chunkServer.logChunkServerCountersLogLevel");
    if (logLevelStr) {
        const MsgLogger::LogLevel logLevel =
            MsgLogger::GetLogLevelId(logLevelStr->c_str());
        if (MsgLogger::kLogLevelUndef != logLevel) {
            mLogChunkServerCountersLogLevel = logLevel;
        }
    }
    if (0 < mObjStoreBlockWriteBufferSize &&
            mObjStoreBlockWriteBufferSize < (int)KFS_CHUNK_HEADER_SIZE) {
        mObjStoreBlockWriteBufferSize = KFS_CHUNK_HEADER_SIZE;
    }
    mObjStoreBufferDataIgnoreOverwriteFlag = prop.getValue(
        "chunkServer.objStoreBufferDataIgnoreOverwriteFlag",
        mObjStoreBufferDataIgnoreOverwriteFlag);
    const double prevObjStoreBufferDataRatio = mObjStoreBufferDataRatio;
    mObjStoreBufferDataRatio = max(double(0.0), min(double(0.995), prop.getValue(
        "chunkServer.objStoreBufferDataRatio",
        mObjStoreBufferDataRatio)));
    mObjStoreBlockMaxNonStableDisconnectedTime = prop.getValue(
        "chunkServer.objStoreBlockMaxNonStableDisconnectedTime",
        mObjStoreBlockMaxNonStableDisconnectedTime);
    mObjBlockDiscardMinMetaUptime = prop.getValue(
        "chunkServer.objBlockDiscardMinMetaUptime",
        mObjBlockDiscardMinMetaUptime);
    if (prevObjStoreBufferDataRatio != mObjStoreBufferDataRatio) {
        mObjStoreMaxWritableBlocks = -1;
    }
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
    ret = ret && err == 0;
    mResumeHelloMaxPendingStaleCount = prop.getValue(
        "chunkServer.resumeHelloMaxPendingStaleCount",
        mResumeHelloMaxPendingStaleCount);
    KfsOp::SetExitDebugCheck(0 != prop.getValue(
        "chunkServer.exitDebugCheck",
        KfsOp::GetExitDebugCheckFlag() ? 1 : 0));
    return ret;
}

void
ChunkManager::SetStorageTiers(const Properties& props)
{
    const string prevPrefixes = mStorageTiersPrefixes;
    mStorageTiersPrefixes = props.getValue(
        "chunkServer.storageTierPrefixes", mStorageTiersPrefixes);
    if (prevPrefixes != mStorageTiersPrefixes || ! mStorageTiersSetFlag) {
        if (mChunkDirs.empty()) {
            mStorageTiersSetFlag = false;
        } else {
            SetStorageTiers(mStorageTiersPrefixes, mChunkDirs, mStorageTiers);
            mStorageTiersSetFlag = true;
        }
    }
    const string prevObjPrefixes = mObjStorageTiersPrefixes;
    mObjStorageTiersPrefixes = props.getValue(
        "chunkServer.objecStorageTierPrefixes", mObjStorageTiersPrefixes);
    if (prevObjPrefixes != mObjStorageTiersPrefixes ||
            ! mObjStorageTiersSetFlag) {
        if (mObjDirs.empty()) {
            mObjStorageTiersSetFlag = false;
        } else {
            SetStorageTiers(
                mObjStorageTiersPrefixes, mObjDirs, mObjStorageTiers);
            mObjStorageTiersSetFlag = true;
            for (StorageTiers::const_iterator it = mObjStorageTiers.begin();
                    it != mObjStorageTiers.end();
                    ++it) {
                if (1 < it->second.size()) {
                    KFS_LOG_STREAM_ERROR <<
                        "invalid object store tier configuration:"
                        " tier: " << (int)it->first <<
                        " configured with: " << it->second.size() <<
                        " storage IO directories / queues;"
                        " only single directory per tier supported" <<
                    KFS_LOG_EOM;
                    if (mObjStorageTiersSetFlag) {
                        mObjectStoreErrorMsg =
                            "invalid object store tier configuration:"
                            " more than single directory in tiers:";
                    }
                    mObjectStoreErrorMsg += ' ';
                    AppendDecIntToString(mObjectStoreErrorMsg, (int)it->first);
                    mObjStorageTiersSetFlag = false;
                }
            }
            if (mObjStorageTiersSetFlag) {
                mObjectStoreErrorMsg.clear();
                mObjectStoreStatus = 0;
            } else {
                mObjStorageTiers.clear();
                mObjectStoreStatus = -EINVAL;
            }
        }
    }
}

void
ChunkManager::SetStorageTiers(
    const string&               prefixes,
    ChunkManager::ChunkDirs&    dirs,
    ChunkManager::StorageTiers& tiers)
{
    tiers.clear();
    for (ChunkDirs::iterator it = dirs.begin();
            it < dirs.end();
            ++it) {
        it->storageTier = kKfsSTierMax;
    }
    istringstream is(prefixes);
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
        for (ChunkDirs::iterator it = dirs.begin();
                it != dirs.end();
                ++it) {
            const string& dirname = it->dirname;
            if (prefix.length() <= dirname.length() &&
                    dirname.compare(0, prefix.length(), prefix) == 0) {
                it->storageTier = (kfsSTier_t)tier;
            }
        }
    }
    for (ChunkDirs::iterator it = dirs.begin();
            it < dirs.end();
            ++it) {
        if (it->availableSpace < 0) {
            continue;
        }
        tiers[it->storageTier].push_back(&(*it));
    }
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

static string
AddTrailingPathSeparator(const string& dir)
{
    return ((! dir.empty() && dir[dir.length() - 1] != '/') ?
        dir + "/" : dir);
}

struct EqualPrefixStr
{
    bool operator()(const string& x, const string& y) const
    {
        return x.compare(0, min(x.length(), y.length()), y) == 0;
    }
};

bool
ChunkManager::InitStorageDirs(
    const vector<string>&    dirNames,
    ChunkManager::ChunkDirs& storageDirs)
{
    // Normalize tailing /, and keep only longest prefixes:
    // only leave leaf directories.
    const size_t kMaxDirNameLength = MAX_RPC_HEADER_LEN / 3;
    vector<string> dirs;
    dirs.reserve(dirNames.size());
    for (vector<string>::const_iterator it = dirNames.begin();
            it < dirNames.end();
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
    storageDirs.Allocate(cnt);
    vector<string>::const_iterator di = dirs.begin();
    for (ChunkDirs::iterator it = storageDirs.begin();
            it < storageDirs.end();
            ++it, ++di) {
        it->dirname     = *di;
        it->storageTier = kKfsSTierMax;
    }
    return true;
}

bool
ChunkManager::Init(const vector<string>& chunkDirs, const Properties& prop)
{
    istringstream  is(prop.getValue("chunkServer.objectDir", string()));
    string         name;
    vector<string> objDirs;
    while ((is >> name)) {
        KFS_LOG_STREAM_INFO << "object dir: " << name << KFS_LOG_EOM;
        objDirs.push_back(name);
    }
    if (chunkDirs.empty() && objDirs.empty()) {
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

    if (! InitStorageDirs(chunkDirs, mChunkDirs) ||
            ! InitStorageDirs(objDirs, mObjDirs)) {
        return false;
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
    mFdsPerChunk = mDiskIoRequestAffinityFlag ? 1 : DiskIo::GetFdCountPerFile();
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
ChunkManager::CanStartReplicationOrRecovery(kfsChunkId_t chunkId)
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    if (! ci || ! *ci) {
        return 0;
    }
    ChunkInfoHandle& cih = **ci;
    if  (cih.IsHelloNotify() || cih.IsPendingAvailable()) {
        return -EEXIST;
    }
    if (IsPendingHelloNotify(cih)) {
        // Move it to the front of the notification queue, to notify meta server
        // ASAP.
        ChunkHelloNotifyList::PushFront(mChunkInfoHelloNotifyList, cih);
        return -EEXIST;
    }
    return 0;
}

MsgLogger::LogLevel
ChunkManager::GetHeartbeatCtrsLogLevel()
{
    if (mLogChunkServerCountersInterval < 0 ||
            ! MsgLogger::GetLogger() ||
            ! MsgLogger::GetLogger()->IsLogLevelEnabled(
                mLogChunkServerCountersLogLevel)) {
        return MsgLogger::kLogLevelUndef;
    }
    const time_t now = globalNetManager().Now();
    if (now < mLogChunkServerCountersLastTime + mLogChunkServerCountersInterval) {
        return MsgLogger::kLogLevelUndef;
    }
    mLogChunkServerCountersLastTime = now;
    return mLogChunkServerCountersLogLevel;
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
    if (isBeingReplicated && chunkVersion < 0) {
        return -EINVAL;
    }
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const cie = GetChunkInfoHandle(chunkId, chunkVersion,
        kAddObjectBlockMappingFlag);
    if (cie) {
        ChunkInfoHandle* const cih = cie;
        if (isBeingReplicated) {
            if (mustExistFlag || cih->IsBeingReplicated()) {
                // Replicator must cancel the replication prior to allocation
                // invocation, and set must exist to false.
                return -EINVAL;
            }
            const bool kIgnorePendingAvailableFlag = true;
            if (0 <= chunkReplicationTargetVersion && (
                    cih->IsHelloNotify() ||
                    cih->IsPendingAvailable() ||
                    (IsPendingHelloNotify(*cih) &&
                        cih->chunkInfo.chunkVersion ==
                            chunkReplicationTargetVersion &&
                        cih->IsChunkReadable(kIgnorePendingAvailableFlag) &&
                        ! cih->IsRenameInFlight()))) {
                // Let chunk available / hello notify to finish notifying meta
                // server about this chunk. Return EEXIST to ensure that the
                // meta server will not issue stale chunk notification in
                // response to the chunk replication / recovery error.
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
            if (! mustExistFlag && chunkVersion < 0 &&
                    0 < cih->chunkInfo.chunkSize) {
                KFS_LOG_STREAM_ERROR <<
                    "not empty unstable object block already exists:"
                    " chunk: "   << chunkId <<
                    " version: " << chunkVersion <<
                    " size: "    << cih->chunkInfo.chunkSize <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            // Invalidate pending writes, if any, due to no version change for
            // object store blocks. For chunk this should be a no op, and if any
            // pending writes are still around they should be deleted.
            mPendingWrites.Delete(chunkId, chunkVersion);
            if (outCih) {
                *outCih = cih;
            }
            return 0;
        }
    } else if (mustExistFlag) {
        return -EBADF;
    }
    if (chunkVersion < 0 && 0 < mObjStoreBufferDataMaxSizePerBlock) {
        if (mObjStoreMaxWritableBlocks < 0) {
            const BufferManager& bufMgr = DiskIo::GetBufferManager();
            mObjStoreMaxWritableBlocks = (int)((
                (bufMgr.GetBufferPoolTotalBytes() - bufMgr.GetTotalCount()) *
                mObjStoreBufferDataRatio
            ) / mObjStoreBufferDataMaxSizePerBlock);
            KFS_LOG_STREAM_DEBUG <<
                " object store max writable"
                " blocks: "     << mObjStoreMaxWritableBlocks <<
                " total"
                " bytes: "      << bufMgr.GetBufferPoolTotalBytes() <<
                " buf mgr: "    << bufMgr.GetTotalCount() <<
                " ratio: "      << mObjStoreBufferDataRatio <<
                " max wr buf: " << mObjStoreBufferDataMaxSizePerBlock <<
            KFS_LOG_EOM;
        }
        int objStoreWritableBlocks = 0;
        for (ChunkDirs::iterator it = mObjDirs.begin();
                it != mObjDirs.end();
                ++it) {
            if (it->availableSpace < 0) {
                continue;
            }
            objStoreWritableBlocks += it->notStableOpenCount;
            if (mObjStoreMaxWritableBlocks <= objStoreWritableBlocks) {
                KFS_LOG_STREAM_ERROR <<
                    "exceeded writable object block"
                    " limit:  " << mObjStoreMaxWritableBlocks <<
                    " <= "      << objStoreWritableBlocks <<
                KFS_LOG_EOM;
                return -ESERVERBUSY;
            }
        }
    }
    if (0 < chunkVersion &&
            mCorruptChunkOp.notifyChunkManagerFlag &&
            mPendingNotifyLostChunks.Find(chunkId)) {
        for (int i = 0; i < mCorruptChunkOp.chunkCount; i++) {
            if (chunkId == mCorruptChunkOp.chunkIds[i]) {
                const char* const msg =
                    "pending notify lost in flight, retry later";
                KFS_LOG_STREAM_ERROR <<
                    "allocate:"
                    " chunk: "   << chunkId <<
                    " version: " << chunkVersion <<
                    " error: "   << msg <<
                KFS_LOG_EOM;
                if (op && 0 <= op->status && op->statusMsg.empty()) {
                    op->statusMsg = msg;
                }
                return -EAGAIN;
            }
        }
    }
    // Find the directory to use
    ChunkDirInfo* const chunkdir = GetDirForChunk(
        chunkVersion < 0,
        minTier == kKfsSTierUndef ? mAllocDefaultMinTier : minTier,
        maxTier == kKfsSTierUndef ? mAllocDefaultMaxTier : maxTier
    );
    if (! chunkdir) {
        KFS_LOG_STREAM_INFO <<
            "no directory has space to host chunk " << chunkId <<
            " version: " << chunkVersion <<
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
    cih->chunkInfo.SetMinHeaderSize(
        GetChunkHeaderSize(cih->chunkInfo.chunkVersion) ==
        KFS_MIN_CHUNK_HEADER_SIZE
    );
    cih->SetBeingReplicated(isBeingReplicated);
    cih->SetMetaDirty();
    if (AddMapping(cih) != cih) {
        die("chunk insertion failure");
        cih->Delete(mChunkInfoLists);
        return -EFAULT;
    }
    KFS_LOG_STREAM_INFO << "creating chunk: " << MakeChunkPathname(cih) <<
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
    if (IsWritePending(op->chunkId, op->chunkVersion)) {
        op->statusMsg = "random write in progress";
        op->status    = -EINVAL;
    }
    if (op->chunkVersion < 0) {
        op->statusMsg = "append not supported with object store blocks";
        op->status    = -EINVAL;
    }
    const bool       kIsBeingReplicatedFlag = false;
    ChunkInfoHandle* cih                    = 0;
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
        ! IsWritePending(cih->chunkInfo.chunkId, cih->chunkInfo.chunkVersion) &&
        ! cih->IsBeingReplicated()
    );
}

bool
ChunkManager::IsChunkStable(kfsChunkId_t chunkId, int64_t chunkVersion) const
{
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const ci = GetChunkInfoHandle(chunkId, chunkVersion,
        kAddObjectBlockMappingFlag);
    return (! ci || IsChunkStable(ci));
}

bool
ChunkManager::IsChunkReadable(kfsChunkId_t chunkId, int64_t chunkVersion) const
{
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const ci = GetChunkInfoHandle(chunkId, chunkVersion,
        kAddObjectBlockMappingFlag);
    return (! ci || (IsChunkStable(ci) && ci->IsChunkReadable()));
}

bool
ChunkManager::IsChunkStable(MakeChunkStableOp* op)
{
    if (op->hasChecksum) {
        return false; // Have to run make stable to compare the checksum.
    }
    // Only add object store block to the table if size is specified, as in
    // this case make stable is from chunk server initiated lease release,
    // and not from allocation failure handling path.
    const bool addObjectBlockMappingFlag = 0 <= op->chunkSize;
    ChunkInfoHandle* const cih = GetChunkInfoHandle(
        op->chunkId, op->chunkVersion, addObjectBlockMappingFlag);
    if (! cih) {
        op->statusMsg = "no such chunk";
        op->status    = -EBADF;
        return true;
    }
    // See if it have to wait until the chunk becomes readable.
    return (op->chunkVersion == cih->chunkInfo.chunkVersion &&
        IsChunkStable(cih) && cih->IsChunkReadable() &&
        (0 <= op->chunkVersion || cih->chunkInfo.AreChecksumsLoaded()));
}

int
ChunkManager::MakeChunkStable(kfsChunkId_t chunkId, kfsSeq_t chunkVersion,
    bool appendFlag, KfsCallbackObj* cb, string& statusMsg,
    bool cleanupPendingWritesOnlyFlag)
{
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const cih = GetChunkInfoHandle(chunkId, chunkVersion,
        kAddObjectBlockMappingFlag);
    if (! cih) {
        statusMsg = "no such chunk";
        return -EBADF;
    }
    bool stableFlag = false;
    if (chunkVersion < 0 || cih->IsRenameInFlight()) {
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
    if (! cih->chunkInfo.AreChecksumsLoaded()) {
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
    gLeaseClerk.UnRegisterLease(
        cih->chunkInfo.chunkId, cih->chunkInfo.chunkVersion);
    if (cleanupPendingWritesOnlyFlag || (stableFlag && chunkVersion < 0)) {
        if (cih->IsChunkReadable()) {
            int64_t res = 0;
            cb->HandleEvent(EVENT_DISK_RENAME_DONE, &res);
            return 0;
        }
        return -EAGAIN;
    }
    stableFlag = true;
    const bool renameFlag = true;
    const int  res        = cih->WriteChunkMetadata(
        cb, renameFlag, stableFlag, chunkVersion);
    if (res < 0) {
        statusMsg = "failed to start chunk meta data write";
        const DiskIo::File* const kNullFile = 0;
        ChunkIOFailed(chunkId, chunkVersion, res, kNullFile);
    }
    return res;
}

int
ChunkManager::DeleteChunk(kfsChunkId_t chunkId, int64_t chunkVersion,
    bool staleChunkIdFlag, KfsOp* op)
{
    ChunkInfoHandle* const cih = GetChunkInfoHandle(chunkId, chunkVersion);
    if (! cih) {
        return -EBADF;
    }
    KFS_LOG_STREAM_INFO << "deleting"
        " chunk: "   << chunkId <<
        " version: " << chunkVersion <<
        " / "        << cih->chunkInfo.chunkVersion <<
        " readble: " << cih->IsChunkReadable() <<
        " staleId: " << staleChunkIdFlag <<
    KFS_LOG_EOM;
    cih->SetForceDeleteObjectStoreBlock(chunkVersion < 0);
    const bool  forceDeleteFlag = true;
    const bool  evacuatedFlag   = false;
    const seq_t availSeq        = -1;
    return (staleChunkIdFlag ?
        StaleChunk(cih, forceDeleteFlag, evacuatedFlag, op) :
        StaleChunk(*cih, forceDeleteFlag, evacuatedFlag, availSeq, op)
    );
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
    kfsChunkId_t chunkId, int64_t chunkVersion, KfsCallbackObj* cb,
    bool forceFlag /* = false */)
{
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const cih = GetChunkInfoHandle(chunkId, chunkVersion,
        kAddObjectBlockMappingFlag);
    if (! cih) {
        return -EBADF;
    }
    if (forceFlag) {
        if (! cih->chunkInfo.AreChecksumsLoaded() ||
                (cih->chunkInfo.chunkVersion < 0 && cih->IsStable())) {
            return -EAGAIN;
        }
        cih->SetMetaDirty();
    }
    const int status = cih->WriteChunkMetadata(cb);
    if (0 != status) {
        const DiskIo::File* const kNullFile = 0;
        ChunkIOFailed(chunkId, chunkVersion, status, kNullFile);
    }
    return status;
}

int
ChunkManager::ReadChunkMetadata(
    kfsChunkId_t chunkId, int64_t chunkVersion, KfsOp* cb,
    bool addObjectBlockMappingFlag)
{
    ChunkInfoHandle* const cih = GetChunkInfoHandle(chunkId, chunkVersion,
        addObjectBlockMappingFlag);
    if (! cih) {
        if (0 <= cb->status) {
            cb->statusMsg = "no such chunk";
        }
        return -EBADF;
    }
    if (cih->IsBeingReplicated()) {
        KFS_LOG_STREAM_ERROR <<
            "denied meta data read for chunk: " << chunkId <<
            " replication is in flight" <<
        KFS_LOG_EOM;
        if (0 <= cb->status) {
            cb->statusMsg = "chunk replication in progress";
        }
        return -EBADF;
    }
    LruUpdate(*cih);
    if (cih->chunkInfo.AreChecksumsLoaded()) {
        cb->HandleEvent(EVENT_CMD_DONE, cb);
        return 0;
    }
    if (cih->chunkInfo.chunkVersion < 0 && ! cih->IsStable()) {
        // Non stable object block is scheduled for cleanup -- fail the read, as
        // the object block is discarded in this case.
        KFS_LOG_STREAM_ERROR <<
            "denied meta data read for object store block: " << chunkId <<
            " version: " << chunkVersion <<
            " non stable with no checksums loaded" <<
        KFS_LOG_EOM;
        if (0 <= cb->status) {
            cb->statusMsg = "chunk replication in progress";
        }
        return -EBADF;
    }
    if (cih->readChunkMetaOp) {
        // if we have issued a read request for this chunk's metadata,
        // don't submit another one; otherwise, we will simply drive
        // up memory usage for useless IO's
        cih->readChunkMetaOp->AddWaiter(cb);
        return 0;
    }

    ReadChunkMetaOp* const rcm = new ReadChunkMetaOp(
        chunkId, cih->chunkInfo.chunkVersion, cb);
    DiskIo*          const d   = SetupDiskIo(cih, rcm);
    if (! d) {
        delete rcm;
        if (0 <= cb->status) {
            cb->statusMsg = "out of requests";
        }
        return -ESERVERBUSY;
    }
    rcm->diskIo.reset(d);
    cih->readChunkMetaOp = rcm;
    const size_t headerSize = KFS_MIN_CHUNK_HEADER_SIZE;
    const int    res        = rcm->diskIo->Read(0, headerSize);
    if (res < 0) {
        cih->readChunkMetaOp = 0;
        cih->ReadStats(res, (int64_t)headerSize, 0);
        ReportIOFailure(cih, res);
        delete rcm;
        if (0 <= cb->status) {
            cb->statusMsg = "read queue error";
        }
        return res;
    }
    return 0;
}

void
ChunkManager::ReadChunkMetadataDone(ReadChunkMetaOp* op, IOBuffer* dataBuf)
{
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const cih = GetChunkInfoHandle(op->chunkId, op->chunkVersion,
        kAddObjectBlockMappingFlag);
    if (! cih || op != cih->readChunkMetaOp) {
        if (0 <= op->status) {
            op->status    = cih ? -EAGAIN                : -EBADF;
            op->statusMsg = cih ? "stale meta data read" : "no such chunk";
            KFS_LOG_STREAM_ERROR <<
                "chunk meta data read completion: " <<
                    op->statusMsg  << " " << op->Show() <<
            KFS_LOG_EOM;
        }
        return;
    }
    const int64_t headerSize = (int64_t)KFS_MIN_CHUNK_HEADER_SIZE;
    int           res;
    if (op->status < 0 || ! dataBuf ||
            dataBuf->BytesConsumable() < headerSize ||
            dataBuf->CopyOut(mChunkHeaderBuffer.GetPtr(),
                    mChunkHeaderBuffer.GetSize()) !=
                mChunkHeaderBuffer.GetSize()) {
        if (op->status != -ETIMEDOUT) {
            if (0 <= op->status) {
                op->status    = -EIO;
                op->statusMsg = "short chunk meta data read";
            } else if (op->statusMsg.empty()) {
                op->statusMsg = QCUtils::SysError(
                    -op->status, "chunk meta data read error: ");
            }
        } else {
            op->statusMsg = "read timed out";
        }
        KFS_LOG_STREAM_ERROR <<
            "chunk meta data read completion: " << op->statusMsg  <<
            " " << (dataBuf ? dataBuf->BytesConsumable() : 0) <<
            " " << op->Show() <<
        KFS_LOG_EOM;
    } else {
        DiskChunkInfo_t& dci                  =
            *reinterpret_cast<DiskChunkInfo_t*>(
                mChunkHeaderBuffer.GetPtr());
        const bool       reverseByteOrderFlag = dci.IsReverseByteOrder();
        const uint64_t&  rdChksum             =
            *reinterpret_cast<const uint64_t*>(&dci + 1);
        const uint64_t   checksum             = reverseByteOrderFlag ?
            DiskChunkInfo_t::ReverseInt(rdChksum) : rdChksum;
        uint32_t               headerChecksum = 0;
        if ((checksum != 0 || mRequireChunkHeaderChecksumFlag) &&
                (headerChecksum = ComputeBlockChecksum(
                    mChunkHeaderBuffer.GetPtr(), sizeof(dci))) != checksum) {
            op->status    = -EBADCKSUM;
            op->statusMsg = "chunk header checksum mismatch";
            ostringstream os;
            os << "chunk meta data read completion: " << op->statusMsg  <<
                " expected: "           << checksum <<
                " computed: "           << headerChecksum  <<
                " reverse byte order: " << reverseByteOrderFlag <<
                " " << op->Show()
            ;
            const string str = os.str();
            KFS_LOG_STREAM_ERROR << str << KFS_LOG_EOM;
            if (mAbortOnChecksumMismatchFlag) {
                die(str);
            }
        } else {
            if (reverseByteOrderFlag) {
                dci.ReverseByteOrder();
            }
            if ((res = dci.Validate(op->chunkId, cih->IsStable() ?
                    cih->chunkInfo.chunkVersion : kfsSeq_t(0))) < 0) {
                op->status    = res;
                op->statusMsg = "chunk metadata validation mismatch";
                KFS_LOG_STREAM_ERROR <<
                    "chunk meta data read completion: " << op->statusMsg  <<
                    " " << op->Show() <<
                KFS_LOG_EOM;
            } else {
                cih->chunkInfo.SetChecksums(dci);
                cih->chunkInfo.chunkFlags = dci.flags;
                if (cih->chunkInfo.chunkSize > (int64_t)dci.chunkSize) {
                    const int64_t extra =
                        cih->chunkInfo.chunkSize - dci.chunkSize;
                    if (0 <= cih->chunkInfo.chunkVersion) {
                        mUsedSpace -= extra;
                    }
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
    cih->ReadStats(op->status, headerSize,
        max(int64_t(1), microseconds() - op->startTime));
    if (op->status < 0 && op->status != -ETIMEDOUT) {
        mCounters.mBadChunkHeaderErrorCount++;
        ChunkIOFailed(cih, op->status);
    }
}

bool
ChunkManager::IsChunkMetadataLoaded(kfsChunkId_t chunkId, int64_t chunkVersion)
{
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const cih = GetChunkInfoHandle(chunkId, chunkVersion,
        kAddObjectBlockMappingFlag);
    return (cih && cih->chunkInfo.AreChecksumsLoaded());
}

ChunkInfo_t*
ChunkManager::GetChunkInfo(kfsChunkId_t chunkId, int64_t chunkVersion)
{
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const ci = GetChunkInfoHandle(chunkId, chunkVersion,
        kAddObjectBlockMappingFlag);
    return (ci ? &(ci->chunkInfo) : 0);
}

int
ChunkManager::MarkChunkStale(ChunkInfoHandle* cih, KfsCallbackObj* cb)
{
    const string s                  = MakeChunkPathname(cih);
    const string staleChunkPathname = MakeStaleChunkPathname(cih);
    string err;
    const int ret = DiskIo::Rename(
        s.c_str(), staleChunkPathname.c_str(), cb, &err) ? 0 : -1;
    KFS_LOG_STREAM(0 == ret ?
            MsgLogger::kLogLevelNOTICE : MsgLogger::kLogLevelERROR) <<
        "moving chunk " << cih->chunkInfo.chunkId <<
        " to stale chunks dir " << staleChunkPathname <<
        (ret == 0 ? " ok" : " error:") << err <<
    KFS_LOG_EOM;
    return ret;
}

int
ChunkManager::StaleChunk(
    kfsChunkId_t chunkId,
    bool         forceDeleteFlag,
    bool         evacuatedFlag,
    kfsSeq_t     availChunksSeq,
    KfsOp*       op)
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
    return StaleChunk(*cih, forceDeleteFlag, evacuatedFlag, availChunksSeq, op);
}

int
ChunkManager::StaleChunk(
    ChunkInfoHandle& cih,
    bool             forceDeleteFlag,
    bool             evacuatedFlag,
    kfsSeq_t         availChunksSeq,
    KfsOp*           op)
{
    // The following code relies on the protocol message order where the chunks
    // available reply would normally arrive *after* "related" stale chunks RPC
    // requests. Presently the message can arrive after the chunk available RPC
    // reply in the case of chunk server re-authentication.
    if (0 <= cih.chunkInfo.chunkVersion &&
            (cih.IsHelloNotify() || cih.IsPendingAvailable()) &&
            (availChunksSeq < 0 ||
                ! gMetaServerSM.FindInFlightOp(availChunksSeq))) {
        KFS_LOG_STREAM(availChunksSeq < 0 ?
                MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelNOTICE) <<
            "keeping stale"
            " chunk: "             << cih.chunkInfo.chunkId <<
            " version: "           << cih.chunkInfo.chunkVersion <<
            " seq: "               << availChunksSeq <<
            " pending available: " << cih.IsPendingAvailable() <<
            " hello notify: "      << cih.IsHelloNotify() <<
        KFS_LOG_EOM;
        // The meta server must explicitly tell to delete this chunk by
        // setting chunk available rpc sequence number in stale chunk
        // request.
        // The available chunks rpc sequence numbers are used here to
        // disambiguate possible stale chunks requests sent in response
        // to other "events". In particular in response to chunk
        // replication or recovery, or version change completion
        // [failures], or in the cases if meta server decides to
        // "timeout" such (or any other relevant) requests, and send
        // stale chunk request in order to ensure proper cleanup.
        return -EAGAIN;
    }
    return StaleChunk(&cih, forceDeleteFlag, evacuatedFlag, op);
}

int
ChunkManager::StaleChunk(
    kfsChunkId_t chunkId,
    bool         forceDeleteFlag,
    bool         evacuatedFlag,
    KfsOp*       op)
{
    ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
    if (! ci) {
        return -EBADF;
    }
    return StaleChunk(*ci, forceDeleteFlag, evacuatedFlag, op);
}

int
ChunkManager::StaleChunk(
    ChunkInfoHandle* cih,
    bool             forceDeleteFlag,
    bool             evacuatedFlag,
    KfsOp*           op)
{
    if (! cih) {
        die("null chunk table entry");
        return -EFAULT;
    }
    if (! RemoveFromTable(*cih)) {
        return -EBADF;
    }
    gLeaseClerk.UnRegisterLease(
        cih->chunkInfo.chunkId, cih->chunkInfo.chunkVersion);
    if (! cih->IsStale() && ! mPendingWrites.Delete(
                cih->chunkInfo.chunkId, cih->chunkInfo.chunkVersion)) {
        ostringstream os;
        os << "make stale failed to cleanup pending writes: "
            " chunk: "   << cih->chunkInfo.chunkId <<
            " version: " << cih->chunkInfo.chunkVersion
        ;
        die(os.str());
    }
    MakeStale(*cih, forceDeleteFlag, evacuatedFlag, op);
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
    if (0 <= cih->chunkInfo.chunkVersion) {
        mUsedSpace -= cih->chunkInfo.chunkSize;
        mUsedSpace += chunkSize;
    }
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
    if (op->chunkVersion < 0) {
        op->statusMsg = "invalid negative chunk version";
        op->status    = -EINVAL;
        return op->status;
    }
    ChunkInfoHandle** const ci = mChunkTable.Find(op->chunkId);
    if (! ci) {
        op->statusMsg = "no such chunk";
        op->status    = -EBADF;
        return op->status;
    }
    ChunkInfoHandle* const cih        = *ci;
    bool                   stableFlag = cih->IsStable();
    kfsSeq_t               targetVers = -1;
    if (cih->IsRenameInFlight()) {
        if (op->verifyStableFlag) {
            targetVers = cih->GetTargetStateAndVersion(stableFlag);
            if (! stableFlag) {
                op->statusMsg = "chunk is not stable";
                op->status    = -EINVAL;
                return op->status;
            }
            if (targetVers < op->fromChunkVersion ||
                    op->chunkVersion < targetVers) {
                op->statusMsg = "target version is out of range";
                op->status    = -EINVAL;
                return op->status;
            }
        } else if (op->fromChunkVersion !=
                cih->GetTargetStateAndVersion(stableFlag)) {
            op->statusMsg = (stableFlag ? "" : "not ");
            op->statusMsg += "stable target version mismatch";
            op->status    = -EINVAL;
            return op->status;
        }
    } else if (op->verifyStableFlag) {
        if (! stableFlag) {
            op->statusMsg = "chunk is not stable";
            op->status    = -EINVAL;
            return op->status;
        }
        targetVers = cih->chunkInfo.chunkVersion;
        if ((op->fromChunkVersion < op->chunkVersion ?
                (targetVers < op->fromChunkVersion ||
                    op->chunkVersion < targetVers) :
                (targetVers < op->chunkVersion ||
                    op->fromChunkVersion < targetVers))) {
            op->statusMsg = "version is out of range";
            op->status    = -EINVAL;
            return op->status;
        }
    } else if (op->fromChunkVersion != cih->chunkInfo.chunkVersion) {
        op->statusMsg = "version mismatch";
        op->status    = -EINVAL;
        return op->status;
    }
    if (op->verifyStableFlag && stableFlag && targetVers == op->chunkVersion) {
        // Nothing to do, invoke successful completion.
        int64_t res = 0;
        op->HandleEvent(cih->IsRenameInFlight() ?
            EVENT_DISK_RENAME_DONE : EVENT_DISK_WROTE, &res);
        return 0;
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
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const cih = GetChunkInfoHandle(chunkId, chunkVersion,
        kAddObjectBlockMappingFlag);
    if (! cih) {
        return -EBADF;
    }
    if (filePtr && *filePtr != cih->dataFH) {
        return -EINVAL;
    }
    return ChangeChunkVers(cih, chunkVersion, stableFlag, cb);
}

int
ChunkManager::ChangeChunkVers(
    ChunkInfoHandle* cih,
    int64_t          chunkVersion,
    bool             stableFlag,
    KfsCallbackObj*  cb)
{
    if (! cih->chunkInfo.AreChecksumsLoaded()) {
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
    bool    targetStable = false;
    int64_t targetVersion;
    if (chunkVersion < 0 &&
            (chunkVersion !=
                (targetVersion = cih->GetTargetStateAndVersion(targetStable)) ||
            targetStable != stableFlag)) {
        // For now do not support changing object store blocks, as this requires
        // read modify write support.
        KFS_LOG_STREAM(chunkVersion == targetVersion ?
                MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR)  <<
            "attempt to change version on object store denied" <<
                " block: "   << cih->chunkInfo.chunkId <<
                " version: " << cih->chunkInfo.chunkVersion <<
                " -> "       << targetVersion <<
                " <- "       << chunkVersion <<
                " stable: "  << cih->IsStable() <<
                " -> "       << targetStable <<
                " <- "       << stableFlag <<
        KFS_LOG_EOM;
        // Client / writer uses EROFS status to detect that the chunk is stable.
        return ((chunkVersion == cih->chunkInfo.chunkVersion && ! stableFlag) ?
            (cih->IsStable() ? -EROFS : -EAGAIN) : -EINVAL);
    }
    KFS_LOG_STREAM_INFO <<
        "changing version:"
        " chunk: "  << cih->chunkInfo.chunkId <<
        " from: "   << cih->chunkInfo.chunkVersion <<
        " to: "     << chunkVersion <<
        " stable: " << cih->IsStable() <<
        " -> "      << stableFlag <<
        " file: "   << MakeChunkPathname(cih) <<
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
    kfsChunkId_t const chunkId    = cih->chunkInfo.chunkId;
    const bool         renameFlag = true;
    const int          status     = cih->WriteChunkMetadata(
        cb, renameFlag, stableFlag, chunkVersion);
    if (0 != status) {
        const DiskIo::File* const kNullFile = 0;
        ChunkIOFailed(chunkId, chunkVersion, status, kNullFile);
    }
    return status;
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
        "replication done:"
        " chunk: "   << chunkId <<
        " version: " << cih->chunkInfo.chunkVersion <<
        " status: "  << status <<
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

bool
ChunkManager::Start()
{
    globalNetManager().RegisterTimeoutHandler(this);
    if (0 != mCryptoKeys.Start()) {
        gChunkManager.Shutdown();
        return false;
    }
    if (! mCryptoKeys.IsCurrentKeyValid()) {
        KFS_LOG_STREAM_ERROR <<
            "no valid current crypto key" <<
        KFS_LOG_EOM;
        gChunkManager.Shutdown();
        return false;
    }
    return true;
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
    ChunkInfoHandle* const  cih   =
        GetChunkInfoHandle(ci.chunkId, ci.chunkVersion);
    if (cih && 0 <= cih->GetDirInfo().availableSpace) {
        if (0 <= cih->chunkInfo.chunkVersion) {
            mUsedSpace += delta;
            if (mUsedSpace < 0) {
                mUsedSpace = 0;
            }
        }
        UpdateDirSpace(cih, delta);
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
ChunkManager::GetDirForChunk(
    bool objFlag, kfsSTier_t minTier, kfsSTier_t maxTier)
{
    StorageTiers& tiers = objFlag ? mObjStorageTiers : mStorageTiers;
    for (StorageTiers::const_iterator it = tiers.lower_bound(minTier);
            it != tiers.end() && it->first <= maxTier;
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
        (stableFlag || targetVersion < 0) ? targetVersion : 0,
        (stableFlag || targetVersion < 0) ? string() : mDirtyChunksDir
    );
}

string
ChunkManager::MakeChunkPathname(const string& chunkdir,
    kfsFileId_t fid, kfsChunkId_t chunkId, kfsSeq_t chunkVersion,
    const string& subDir)
{
    string ret;
    ret.reserve(
        chunkdir.size() +
        subDir.size() + 78 +
        (chunkVersion < 0 ? 9 : 0)
    );
    ret.assign(chunkdir.data(), chunkdir.size());
    ret.append(subDir.data(), subDir.size());
    if (! AppendChunkFileNameOrObjectStoreBlockKey(
            ret, mFileSystemId, fid, chunkId, chunkVersion,
            mFileSystemIdSuffix)) {
        die("failed to create chunk or block file name");
    }
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
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* cih = GetChunkInfoHandle(
        chunkId, chunkVers, kAddObjectBlockMappingFlag);
    if (cih) {
        string fileName;
        string staleName;
        string keepName;
        // Keep the chunk with the higher version.
        if (cih->chunkInfo.chunkVersion < chunkVers) {
            fileName  = MakeChunkPathname(cih);
            staleName = MakeStaleChunkPathname(cih);
            keepName  = MakeChunkPathname(
                dir.dirname, fileId, chunkId, chunkVers, string());
            RemoveFromChunkTable(*cih);
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
ChunkManager::OpenChunk(ChunkInfoHandle* cih, int openFlags)
{
    if (cih->IsFileOpen()) {
        return 0;
    }
    const bool openFlag = 0 == (openFlags & O_CREAT);
    if (! CleanupInactiveFds(globalNetManager().Now(), cih)) {
        KFS_LOG_STREAM_ERROR <<
            "failed to " << (openFlag ? "open" : "create") <<
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
            // Failed to open/create a file. notify the metaserver
            // of lost data so that it can re-replicate if needed.
            NotifyMetaCorruptedChunk(cih, -EIO);
            if (RemoveFromTable(*cih)) {
                const int64_t size = 0 <= cih->chunkInfo.chunkVersion ?
                    min(mUsedSpace, cih->chunkInfo.chunkSize) :
                    cih->chunkInfo.chunkSize;
                UpdateDirSpace(cih, -size);
                if (0 <= cih->chunkInfo.chunkVersion) {
                    mUsedSpace -= size;
                }
            }
            Delete(*cih);
            ScheduleNotifyLostChunk();
        } else {
            cih->dataFH.reset();
        }
        KFS_LOG_STREAM_ERROR <<
            "failed to " << (openFlag ? "open" : "create") <<
            " chunk file: " << fn << " :" << errMsg <<
        KFS_LOG_EOM;
        return (tempFailureFlag ? -EAGAIN : -EBADF);
    }
    globals().ctrOpenDiskFds.Update(1);
    LruUpdate(*cih);
    if (! cih->IsStable()) {
        cih->UpdateDirStableCount();
    }
    KFS_LOG_STREAM(openFlag ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelINFO) <<
        (openFlag ? "open" : "create") <<
        " chunk file: "  << fn <<
        " file handle: " << reinterpret_cast<const void*>(cih->dataFH.get()) <<
    KFS_LOG_EOM;
    // the checksums will be loaded async
    return 0;
}

int
ChunkManager::CloseChunk(kfsChunkId_t chunkId, int64_t chunkVersion,
    KfsOp* op /* = 0 */)
{
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const ci = GetChunkInfoHandle(chunkId, chunkVersion,
        kAddObjectBlockMappingFlag);
    if (! ci) {
        return -EBADF;
    }
    return CloseChunk(ci, op);
}

int
ChunkManager::CloseChunkWrite(
    kfsChunkId_t chunkId, int64_t chunkVersion, int64_t writeId,
    KfsOp* op, bool* readMetaFlag)
{
    if (PendingWrites::IsObjStoreWriteId(writeId) == (0 <= chunkVersion)) {
        return -EINVAL;
    }
    const WriteOp* const wo = mPendingWrites.find(writeId);
    if (! wo) {
        if (op && chunkVersion < 0) {
            ChunkInfoHandle* const cih =
                GetChunkInfoHandle(chunkId, chunkVersion);
            if (cih && cih->chunkInfo.AreChecksumsLoaded()) {
                bool stableFlag = false;
                if (cih->GetTargetStateAndVersion(stableFlag) != chunkVersion) {
                    return -EINVAL;
                }
                if (stableFlag) {
                    cih->AddWaitChunkReadable(op);
                    return 0;
                }
                return (gMetaServerSM.IsUp() ? -EINVAL : -ELEASEEXPIRED);
            }
            if (readMetaFlag) {
                *readMetaFlag = true;
            }
            bool kAddObjectBlockMappingFlag = true;
            return ReadChunkMetadata(chunkId, chunkVersion, op,
                kAddObjectBlockMappingFlag);
        }
        return -EBADF;
    }
    if (wo->chunkId != chunkId) {
        return -EINVAL;
    }
    return CloseChunk(wo->chunkId, wo->chunkVersion, op);
}

bool
ChunkManager::CloseChunkIfReadable(kfsChunkId_t chunkId, int64_t chunkVersion)
{
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const ci = GetChunkInfoHandle(chunkId, chunkVersion,
        kAddObjectBlockMappingFlag);
    if (! ci) {
        return -EBADF;
    }
    return (
        IsChunkStable(ci) &&
        ci->IsChunkReadable() &&
        CloseChunk(ci) == 0
    );
}

int
ChunkManager::CloseChunk(ChunkInfoHandle* cih, KfsOp* op /* = 0 */)
{
    if (cih->IsWriteAppenderOwns()) {
        KFS_LOG_STREAM_INFO <<
            "ignoring close chunk on chunk: " << cih->chunkInfo.chunkId <<
            " version: " << cih->chunkInfo.chunkVersion <<
            " open for append " <<
        KFS_LOG_EOM;
        return -EINVAL;
    }

    // Close file if not in use.
    if (cih->IsFileOpen() &&
            ! cih->IsFileInUse() &&
            ! cih->IsBeingReplicated() &&
            (cih->IsStable() || ! IsWritePending(
                cih->chunkInfo.chunkId, cih->chunkInfo.chunkVersion)) &&
            ! cih->SyncMeta()) {
        Release(*cih);
    } else {
        KFS_LOG_STREAM_INFO <<
            "chunk: " << cih->chunkInfo.chunkId <<
            " version: " << cih->chunkInfo.chunkVersion <<
            " not released on close; might give up lease" <<
        KFS_LOG_EOM;
        gLeaseClerk.RelinquishLease(
            cih->chunkInfo.chunkId, cih->chunkInfo.chunkVersion,
            cih->chunkInfo.chunkSize);
    }
    if (op) {
        cih->AddWaitChunkReadable(op);
    }
    return 0;
}

bool
ChunkManager::ChunkSize(SizeOp* op)
{
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const cih = GetChunkInfoHandle(
        op->chunkId, op->chunkVersion, kAddObjectBlockMappingFlag);
    if (! cih) {
        if (op->chunkVersion < 0) {
            return false;
        }
        op->status    = -EBADF;
        op->statusMsg = "no such chunk";
        return true;
    }
    if (cih->IsBeingReplicated()) {
        op->status    = -EAGAIN;
        op->statusMsg = "chunk replication in progress";
        return true;
    }
    if (0 <= cih->chunkInfo.chunkVersion && ! op->checkFlag &&
            op->chunkVersion != cih->chunkInfo.chunkVersion) {
        op->status    = -EBADVERS;
        op->statusMsg = "chunk version mismatch";
        return true;
    }
    if (cih->IsWriteAppenderOwns() &&
            ! gAtomicRecordAppendManager.IsChunkStable(op->chunkId)) {
        op->statusMsg = "write append in progress, returning max chunk size";
        op->size      = CHUNKSIZE;
        if (op->checkFlag) {
            op->chunkVersion = cih->chunkInfo.chunkVersion;
            op->stableFlag   = false;
        }
        KFS_LOG_STREAM_DEBUG <<
            op->statusMsg <<
            " chunk: " << op->chunkId <<
            " file: "  << op->fileId  <<
            " size: "  << op->size    <<
        KFS_LOG_EOM;
        return true;
    }
    if ((! mChunkSizeSkipHeaderVerifyFlag ||
            cih->chunkInfo.chunkVersion < 0) &&
            ! cih->chunkInfo.AreChecksumsLoaded()) {
        if (op->checkFlag) {
            op->chunkVersion = cih->chunkInfo.chunkVersion;
        }
        return false;
    }
    op->size = cih->chunkInfo.chunkSize;
    if (op->checkFlag) {
        op->chunkVersion = cih->GetTargetStateAndVersion(op->stableFlag);
    }
    return true;
}

string
ChunkManager::GetDirName(chunkId_t chunkId, int64_t chunkVersion) const
{
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const cih = GetChunkInfoHandle(chunkId, chunkVersion,
        kAddObjectBlockMappingFlag);
    if (! cih) {
        return string();
    }
    return cih->GetDirname();
}

int
ChunkManager::ReadChunk(ReadOp* op)
{
    ChunkInfoHandle* const cih = GetChunkInfoHandle(op->chunkId, op->chunkVersion);
    if (! cih) {
        return -EBADF;
    }
    // the checksums should be loaded...
    cih->chunkInfo.VerifyChecksumsLoaded();

    if (op->chunkVersion != cih->chunkInfo.chunkVersion) {
        KFS_LOG_STREAM_INFO << "Version # mismatch (have=" <<
            cih->chunkInfo.chunkVersion << " vs asked=" << op->chunkVersion <<
            ") failing read" <<
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
    const int ret = op->diskIo->Read(
        offset + cih->chunkInfo.GetHeaderSize(), numBytesIO);
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
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const cih = GetChunkInfoHandle(op->chunkId, op->chunkVersion,
        kAddObjectBlockMappingFlag);
    if (! cih) {
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
    if (0 == op->numBytes && 0 == op->numBytesIO && op->wpop) {
        // Empty write prepare can be used to keep write lease in a period of
        // write inactivity.
        op->diskIo.reset(SetupDiskIo(cih, op));
        if (! op->diskIo) {
            return -ESERVERBUSY;
        }
        cih->StartWrite(op);
        op->diskIOTime = microseconds();
        // Do not queue empty IO (it isn't presently supported anyway), invoke
        // completion instead.
        int res = 0;
        op->HandleEvent(EVENT_DISK_WROTE, &res);
        return 0;
    }
    if (op->numBytesIO <= 0 || op->offset < 0) {
        return -EINVAL;
    }
    const int64_t addedBytes(op->offset + op->numBytesIO - cih->chunkInfo.chunkSize);
    if (0 <= cih->chunkInfo.chunkVersion &&
            0 < addedBytes && mUsedSpace + addedBytes >= mTotalSpace) {
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
        IOBuffer::BufPos off     =
            (IOBuffer::BufPos)(offset % CHECKSUM_BLOCKSIZE);
        const uint32_t   blkSize =
            (size_t(off + numBytesIO) > CHECKSUM_BLOCKSIZE) ?
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
                ReadOp* const rop = new ReadOp(op, offset - off, blkSize);
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
                if (0 <= op->status) {
                    op->status = op->rop->status;
                    op->statusMsg.swap(op->rop->statusMsg);
                }
                op->rop->wop = 0;
                delete op->rop;
                op->rop = 0;
                if (op->diskIo) {
                    die("invalid read modify write completion");
                    int res = op->status;
                    op->HandleEvent(EVENT_DISK_ERROR, &res);
                    return 0;
                }
                return op->status;
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
        IOBuffer::BufPos numBytes(numBytesIO);
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
    op->diskIOTime = microseconds();
    int res = op->diskIo->Write(
        offset + cih->chunkInfo.GetHeaderSize(), numBytesIO, &op->dataBuf);
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
        if (0 <= cih->chunkInfo.chunkVersion) {
            mUsedSpace += endOffset - cih->chunkInfo.chunkSize;
        }
        cih->chunkInfo.chunkSize = endOffset;

    }
    assert(0 <= mUsedSpace && mUsedSpace <= mTotalSpace);
}

void
ChunkManager::WriteDone(WriteOp* op)
{
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const cih = GetChunkInfoHandle(
        op->chunkId, op->chunkVersion, kAddObjectBlockMappingFlag);
    if (! cih) {
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
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const cih = GetChunkInfoHandle(
        op->chunkId, op->chunkVersion, kAddObjectBlockMappingFlag);
    bool staleRead = false;
    if (! cih ||
            op->chunkVersion != cih->chunkInfo.chunkVersion ||
            (staleRead = ! cih->IsFileEquals(op->diskIo))) {
        op->dataBuf.Clear();
        if (cih) {
            KFS_LOG_STREAM_INFO <<
                "read complete:"
                " chunk: "    << cih->chunkInfo.chunkId <<
                " version:"
                " actual: "   << cih->chunkInfo.chunkVersion <<
                " expected: " << op->chunkVersion << ")" <<
                (staleRead ? " stale read" : "") <<
            KFS_LOG_EOM;
        }
        op->status    = -EBADVERS;
        op->statusMsg = staleRead ? "stale read" : "version mismatch";
        return true;
    }

    op->diskIOTime = max(int64_t(1), microseconds() - op->diskIOTime);
    const int readLen = op->dataBuf.BytesConsumable();
    if (readLen <= 0) {
        KFS_LOG_STREAM_ERROR << "short read for" <<
            " chunk: "    << cih->chunkInfo.chunkId  <<
            " version: "  << cih->chunkInfo.chunkVersion  <<
            " size: "     << cih->chunkInfo.chunkSize <<
            " read:"
            " offset: "   << op->offset <<
            " len: "      << readLen <<
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
        KFS_LOG_STREAM_INFO << "checksums for"
            " chunk: "   << cih->chunkInfo.chunkId  <<
            " version: " << cih->chunkInfo.chunkVersion  <<
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
                    " version: "    << cih->chunkInfo.chunkVersion <<
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
        "checksum mismatch:"
        " chunk: "    << op->chunkId <<
        " offset: "   << op->offset <<
        " bytes: "    << op->numBytesIO <<
        " block: "    << checksumBlock <<
        " expect: "   << cih->chunkInfo.chunkBlockChecksum[checksumBlock] <<
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
        cih->GetDirInfo().lostChunksCount++;
    } else {
        mCounters.mCorruptedChunksCount++;
        cih->GetDirInfo().corruptedChunksCount++;
    }
    KFS_LOG_STREAM_ERROR <<
        (err == 0 ? "lost" : "corrupted") <<
        " chunk: "     << cih->chunkInfo.chunkId <<
        " file: "      << cih->chunkInfo.fileId <<
        " version: "   << cih->chunkInfo.chunkVersion <<
        " error: "     << err <<
        (err ? QCUtils::SysError(-err, " ") : string()) <<
        " dir: "       << cih->GetDirname() <<
        " total:"
        " lost: "      << mCounters.mLostChunksCount <<
        " corrupted: " << mCounters.mCorruptedChunksCount <<
    KFS_LOG_EOM;
    if (0 <= cih->chunkInfo.chunkVersion) {
        if (! cih->IsBeingReplicated()) {
            bool stableFlag = false;
            NotifyLostChunk(cih->chunkInfo.chunkId,
                cih->GetTargetStateAndVersion(stableFlag));
        }
        // Meta server automatically cleans up leases for corrupted chunks.
        gLeaseClerk.UnRegisterLease(cih->chunkInfo.chunkId,
            cih->chunkInfo.chunkVersion);
    } else {
        gLeaseClerk.RelinquishLease(cih->chunkInfo.chunkId,
            cih->chunkInfo.chunkVersion, cih->chunkInfo.chunkSize);
    }
}

void
ChunkManager::ChunkIOFailed(kfsChunkId_t chunkId, int64_t chunkVersion,
    int err, const DiskIo::File* file)
{
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const cih = GetChunkInfoHandle(chunkId, chunkVersion,
        kAddObjectBlockMappingFlag);
    if (! cih) {
        KFS_LOG_STREAM_ERROR <<
            "io failure:"
            " chunk: "   << chunkId <<
            " version: " << chunkVersion <<
            " not in table" <<
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
            "assuming temporary io failure"
            " chunk: "   << cih->chunkInfo.chunkId <<
            " version: " << cih->chunkInfo.chunkVersion <<
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
    ScheduleNotifyLostChunk();
}

void
ChunkManager::ChunkIOFailed(kfsChunkId_t chunkId, int64_t chunkVersion,
    int err, const DiskIo* diskIo)
{
    ChunkIOFailed(chunkId, chunkVersion, err,
        diskIo ? diskIo->GetFilePtr().get() : 0);
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
    for (int i = 0; i < ChunkDirInfo::kChunkDirListCount; i++) {
        ChunkDirInfo::ChunkLists& list = dir.chunkLists[i];
        ChunkInfoHandle* cih;
        while ((cih = ChunkDirList::Front(list))) {
            const kfsChunkId_t chunkId    = cih->chunkInfo.chunkId;
            bool               stableFlag = false;
            const kfsSeq_t     chunkVers  =
                cih->GetTargetStateAndVersion(stableFlag);
            const kfsFileId_t  fileId     = cih->chunkInfo.fileId;
            // get rid of chunkid from our list
            const bool staleFlag = staleChunksFlag || cih->IsStale();
            ChunkInfoHandle** const ci = mChunkTable.Find(chunkId);
            if (ci && *ci == cih && ! RemoveFromChunkTable(*cih)) {
                die("corrupted chunk table");
                continue;
            }
            const int64_t size = min(mUsedSpace, cih->chunkInfo.chunkSize);
            UpdateDirSpace(cih, -size);
            mUsedSpace -= size;
            Delete(*cih);
            if (staleFlag || ! ci || *ci != cih) {
                continue;
            }
            if (NotifyLostChunk(chunkId, chunkVers)) {
                KFS_LOG_STREAM_DEBUG <<
                    "lost"
                    " chunk: "   << chunkId <<
                    " version: " << chunkVers <<
                    " file: "    << fileId <<
                KFS_LOG_EOM;
                mCounters.mDirLostChunkCount++;
            }
        }
    }
    if (gMetaServerSM.IsUp()) {
        CorruptChunkOp* const op = new CorruptChunkOp(-1, &dir.dirname);
        // Do not count as corrupt.
        op->isChunkLost = true;
        gMetaServerSM.EnqueueOp(op);
    }
    ScheduleNotifyLostChunk();
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
ChunkManager::GetChecksum(kfsChunkId_t chunkId, int64_t chunkVersion,
    int64_t offset)
{
    if (offset < 0) {
        return 0;
    }
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const cih = GetChunkInfoHandle(chunkId, chunkVersion,
        kAddObjectBlockMappingFlag);
    if (! cih) {
        return 0;
    }
    const uint32_t checksumBlock = OffsetToChecksumBlockNum(offset);
    // the checksums should be loaded...
    cih->chunkInfo.VerifyChecksumsLoaded();

    assert(checksumBlock < MAX_CHUNK_CHECKSUM_BLOCKS);

    return cih->chunkInfo.chunkBlockChecksum[
        min(MAX_CHUNK_CHECKSUM_BLOCKS - 1, checksumBlock)];
}

vector<uint32_t>
ChunkManager::GetChecksums(kfsChunkId_t chunkId, int64_t chunkVersion,
    int64_t offset, size_t numBytes)
{
    if (offset < 0) {
        return vector<uint32_t>();
    }
    const bool kAddObjectBlockMappingFlag = false;
    const ChunkInfoHandle* const cih =
        GetChunkInfoHandle(chunkId, chunkVersion, kAddObjectBlockMappingFlag);
    if (! cih) {
        return vector<uint32_t>();
    }
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
ChunkManager::SetupDiskIo(ChunkInfoHandle *cih, KfsCallbackObj* op)
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
    Restore();
    return 0;
}

void
ChunkManager::ScheduleCleanup(ChunkManager::ChunkDirInfo& dir,
    kfsFileId_t fileId, chunkId_t chunkId, kfsSeq_t chunkVers,
    int64_t chunkSize, bool stableFlag, bool forceDeleteFlag)
{
    ChunkInfoHandle* const cih  = new ChunkInfoHandle(dir, stableFlag);
    const int64_t          size =
        max(int64_t(0), min((int64_t)CHUNKSIZE, chunkSize));
    cih->chunkInfo.fileId       = fileId;
    cih->chunkInfo.chunkId      = chunkId;
    cih->chunkInfo.chunkVersion = chunkVers;
    cih->chunkInfo.chunkSize    = size;
    if (0 <= size) {
        mUsedSpace += size;
        UpdateDirSpace(cih, size);
    }
    const bool kEvacuatedFlag = false;
    MakeStale(*cih, forceDeleteFlag, kEvacuatedFlag);
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
            const  string name = dir + dent->d_name;
            struct stat   buf;
            if (stat(name.c_str(), &buf) || ! S_ISREG(buf.st_mode)) {
                continue;
            }
            // Parse file name, and add to notify queue, if valid, in order to
            // attempt hello resume after restart.
            const bool    kCheckChunkHeaderChecksumFlag = false;
            const bool    kForceReadFlag                = false;
            const int64_t kFileSize                     = KFS_CHUNK_HEADER_SIZE;
            kfsFileId_t   fileId       = -1;
            chunkId_t     chunkId      = -1;
            kfsSeq_t      chunkVers    = -1;
            int64_t       chunkSize    = -1;
            int64_t       fileSystemId = -1;
            int           ioTimeSec    = -1;
            bool          readFlag     = false;
            if (IsValidChunkFile(
                    dir,
                    dent->d_name,
                    kFileSize,
                    kCheckChunkHeaderChecksumFlag,
                    kForceReadFlag,
                    mChunkHeaderBuffer,
                    fileId,
                    chunkId,
                    chunkVers,
                    chunkSize,
                    fileSystemId,
                    ioTimeSec,
                    readFlag)) {
                const bool kStableFlag      = false;
                const bool kForceDeleteFlag = true;
                ScheduleCleanup(
                    *it, fileId, chunkId, chunkVers,
                    (int64_t)buf.st_size - (int64_t)KFS_CHUNK_HEADER_SIZE,
                    kStableFlag, kForceDeleteFlag);
                InsertLastInFlight(chunkId);
            } else {
                KFS_LOG_STREAM_INFO <<
                    "cleaning out dirty chunk: " << name <<
                KFS_LOG_EOM;
                if (unlink(name.c_str())) {
                    const int err = errno;
                    KFS_LOG_STREAM_ERROR <<
                        "unable to remove " << name <<
                        " error: " << QCUtils::SysError(err) <<
                    KFS_LOG_EOM;
                }
            }
        }
        closedir(dirStream);
    }
}

void
ChunkManager::Restore()
{
    mCleanupStaleChunksFlag = false; // Disable cleanup until hello completion.
    RemoveDirtyChunks();
    bool scheduleEvacuateFlag = false;
    for (ChunkDirs::iterator it = mChunkDirs.begin();
            it != mChunkDirs.end();
            ++it) {
        if (it->availableSpace < 0) {
            continue;
        }
        DirChecker::ChunkInfos::Iterator cit(it->availableChunks);
        const DirChecker::ChunkInfo*     ci;
        while ((ci = cit.Next())) {
            if (0 <= ci->mChunkSize && 0 <= ci->mChunkVersion) {
                AddMapping(
                    *it,
                    ci->mFileId,
                    ci->mChunkId,
                    ci->mChunkVersion,
                    ci->mChunkSize
                );
            } else {
                if (ci->mChunkVersion < 0 || ci->mChunkId < 0 ||
                        mChunkTable.Find(ci->mChunkId)) {
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
                } else {
                    const bool kStableFlag      = true;
                    const bool kForceDeleteFlag = false;
                    ScheduleCleanup(
                        *it,
                        ci->mFileId,
                        ci->mChunkId,
                        ci->mChunkVersion,
                        ci->mChunkSize,
                        kStableFlag,
                        kForceDeleteFlag
                    );
                    InsertLastInFlight(ci->mChunkId);
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
    // Re-enable cleanup if it doesn't have to wait till hello completion.
    mCleanupStaleChunksFlag = mStaleChunksCount <= mDoneStaleChunksCount;
}

static inline void
AppendToHostedListSelf(
    const ChunkManager::HostedChunkList& list,
    const ChunkInfo_t&                   chunkInfo,
    kfsSeq_t                             chunkVersion,
    bool                                 noFidsFlag)
{
    (*list.first)++;
    if (! noFidsFlag) {
        (*list.second) <<
            ' ' << chunkInfo.fileId
        ;
    }
    (*list.second) <<
        ' ' << chunkInfo.chunkId <<
        ' ' << chunkVersion
    ;
}

inline void
ChunkManager::AppendToHostedList(
    ChunkInfoHandle&                     cih,
    const ChunkManager::HostedChunkList& stable,
    const ChunkManager::HostedChunkList& notStableAppend,
    const ChunkManager::HostedChunkList& notStable,
    bool                                 noFidsFlag)
{
    HelloNotifyRemove(cih);
    if (cih.IsRenameInFlight()) {
        // Tell meta server the target version. It comes here when the
        // meta server connection breaks while make stable or version change
        // is in flight.
        // Report the target version and status, otherwise meta server might
        // think that this is stale chunk copy, and delete it.
        // This creates time gap with the client: the chunk still might be
        // transitioning when the read comes. In such case the chunk will
        // not be "readable" and the client will be asked to come back later.
        bool stableFlag = false;
        const kfsSeq_t vers = cih.GetTargetStateAndVersion(stableFlag);
        AppendToHostedListSelf(
            stableFlag ? stable :
                (cih.IsWriteAppenderOwns() ?
                    notStableAppend : notStable),
                cih.chunkInfo,
                vers,
                noFidsFlag
        );
    } else {
        AppendToHostedListSelf(
            IsChunkStable(&cih) ?
                stable :
                (cih.IsWriteAppenderOwns() ?
                    notStableAppend :
                    notStable),
            cih.chunkInfo,
            cih.chunkInfo.chunkVersion,
            noFidsFlag
        );
    }
}

bool
ChunkManager::IsTargetChunkVersionStable(
    const ChunkInfoHandle& cih, kfsSeq_t& vers) const
{
    if (cih.IsBeingReplicated()) {
        vers = -1;
        return false;
    }
    bool stableFlag = false;
    vers = cih.GetTargetStateAndVersion(stableFlag);
    return stableFlag;
}

void
ChunkManager::GetHostedChunksResume(
    HelloMetaOp&                         hello,
    const ChunkManager::HostedChunkList& stable,
    const ChunkManager::HostedChunkList& notStableAppend,
    const ChunkManager::HostedChunkList& notStable,
    const ChunkManager::HostedChunkList& missing,
    const ChunkManager::HostedChunkList& pendingStale,
    bool                                 noFidsFlag)
{
    if (0 != mHelloNotifyInFlightCount) {
        die("hello resume while pending hello notify in flight");
    }
    if (hello.resumeStep == 0) {
        // Tell meta server to exclude last in flight and all non stable chunks
        // from checksum.
        for (mLastPendingInFlight.First(); ;) {
            const kfsChunkId_t* const p = mLastPendingInFlight.Next();
            if (! p) {
                break;
            }
            (*missing.first)++;
            (*missing.second) << ' ' << *p;
        }
        for (mChunkTable.First(); ;) {
            const CMapEntry* const p = mChunkTable.Next();
            if (! p) {
                break;
            }
            const kfsChunkId_t chunkId = p->GetKey();
            if (mLastPendingInFlight.Find(chunkId)) {
                continue;
            }
            ChunkInfoHandle* const cih  = p->GetVal();
            kfsSeq_t               vers = -1;
            if (! cih->IsBeingReplicated() &&
                    (! IsTargetChunkVersionStable(*cih, vers) || vers <= 0)) {
                HelloNotifyRemove(*cih);
                (*missing.first)++;
                (*missing.second) << ' ' << chunkId;
            }
        }
        return;
    }
    mCounters.mHelloResumeCount++;
    CIdChecksum checksum;
    uint64_t    count = 0;
    for (mChunkTable.First(); ;) {
        const CMapEntry* const p = mChunkTable.Next();
        if (! p) {
            break;
        }
        ChunkInfoHandle* const cih = p->GetVal();
        if (cih->IsBeingReplicated()) {
            // Must not be in hello notify, ensure that it really isn't.
            HelloNotifyRemove(*cih);
            continue;
        }
        kfsSeq_t vers = -1;
        if (! IsTargetChunkVersionStable(*cih, vers) || vers <= 0) {
            AppendToHostedList(
                *cih, stable, notStableAppend, notStable, noFidsFlag);
            continue;
        }
        if (mLastPendingInFlight.Find(p->GetKey())) {
            continue;
        }
        if (IsPendingHelloNotify(*cih)) {
            continue;
        }
        checksum.Add(p->GetKey(), vers);
        count++;
    }
    // Add all pending notify lost chunks. The chunks should not be
    // in the chunk table: AddMapping() deletes pending lost notify entries.
    for (mPendingNotifyLostChunks.First(); ;) {
        const PendingNotifyLostChunks::KVPair* const p =
            mPendingNotifyLostChunks.Next();
        if (! p) {
            break;
        }
        const kfsChunkId_t chunkId = p->GetKey();
        if (! mLastPendingInFlight.Find(chunkId) &&
                ! mChunkTable.Find(chunkId) &&
                0 < p->GetVal()) {
            checksum.Add(chunkId, p->GetVal());
            count++;
        }
    }
    // Do not delete chunks just yet, exclude those from checksum, and
    // report those back to the meta server, to make the meta server
    // explicitly delete those after hello completion, effectively storing
    // stale queue state on the meta server side, in order to handle resume
    // after chunk server restart with non empty stale queue and resume in
    // flight.
    // Pending in flight will be re-submitted again after hello completion,
    // in flight chunks have already been removed from inventory count and
    // checksum.
    LastPendingInFlight alreadyHandled;
    for (int pass = 0; pass < 2; pass++) {
        const HelloMetaOp::ChunkIds&                ids       =
            pass == 0 ? hello.resumeDeleted : hello.resumeModified;
        for (HelloMetaOp::ChunkIds::const_iterator
                it = ids.begin(); it != ids.end(); ++it) {
            const kfsChunkId_t chunkId    = *it;
            bool               uniqueFlag = false;
            alreadyHandled.Insert(chunkId, uniqueFlag);
            if (! uniqueFlag) {
                continue; // already handled.
            }
            ChunkInfoHandle** const cih = mChunkTable.Find(chunkId);
            if (! cih || (*cih)->IsBeingReplicated()) {
                if (0 < pass) {
                    (*missing.first)++;
                    (*missing.second) << ' ' << chunkId;
                }
                const kfsSeq_t* vers;
                if (! mLastPendingInFlight.Find(chunkId) &&
                        (vers = mPendingNotifyLostChunks.Find(chunkId)) &&
                        0 < *vers) {
                    if (count <= 0) {
                        die("invalid CS chunk inventory count");
                        hello.resumeStep = -1;
                        break;
                    }
                    checksum.Remove(chunkId, *vers);
                    count--;
                }
                continue;
            }
            kfsSeq_t vers = -1;
            if (! IsTargetChunkVersionStable(**cih, vers) || vers <= 0) {
                // Only report stable chunks here, all unstable are
                // reported already the above.
                continue;
            }
            if (! mLastPendingInFlight.Find(chunkId) &&
                    ! IsPendingHelloNotify(**cih)) {
                if (count <= 0) {
                    die("invalid CS chunk inventory count");
                    hello.resumeStep = -1;
                    break;
                }
                checksum.Remove(chunkId, vers);
                count--;
            }
            AppendToHostedList(
                **cih, stable, notStableAppend, notStable, noFidsFlag);
        }
    }
    if (0 <= hello.resumeStep) {
        // Report last pending in flight here in order to insure that
        // meta server has no stale entries, in the cases where available
        // chunks become un-available again.
        for (mLastPendingInFlight.First(); ;) {
            const kfsChunkId_t* const p = mLastPendingInFlight.Next();
            if (! p) {
                break;
            }
            const kfsChunkId_t chunkId = *p;
            if (alreadyHandled.Find(chunkId)) {
                continue;
            }
            ChunkInfoHandle** const cih = mChunkTable.Find(chunkId);
            if (! cih || (*cih)->IsBeingReplicated()) {
                (*missing.first)++;
                (*missing.second) << ' ' << chunkId;
                continue;
            }
            kfsSeq_t vers = -1;
            if (! IsTargetChunkVersionStable(**cih, vers) || vers <= 0) {
                // Already reported.
                continue;
            }
            AppendToHostedList(
                **cih, stable, notStableAppend, notStable, noFidsFlag);
        }
    }
    if (0 <= hello.resumeStep &&
            count == hello.chunkCount && checksum == hello.checksum) {
        // Store chunk id that are currently in the stale queue in the meta
        // server transaction log / checkpoint, until the chunk files are
        // deleted. The meta server will add these to the in flight list on
        // its side, and exclude them from checksum if chunk server disconnects
        // or exit prior to deletion completion. This is need to handle chunk
        // server disconnect, then reconnect, followed by the exit with
        // stale chunk deletes in flight from the prior hello resume.
        // Limit the size of the list, in order to reduce meta server's side
        // processing at the cost of possible hello resume failure in the case
        // of chunk server restart previously described.
        const bool pendingNotifyFlag = hello.pendingNotifyFlag;
        hello.pendingNotifyFlag =
            ! ChunkHelloNotifyList::IsEmpty(mChunkInfoHelloNotifyList);
        if (! hello.pendingNotifyFlag &&
                mStaleChunksCount <=
                    mDoneStaleChunksCount + mResumeHelloMaxPendingStaleCount) {
            for (int i = 0; i < 2; i++) {
                ChunkList::Iterator it(mChunkInfoLists[
                    0 == i ? kChunkPendingStaleList : kChunkStaleList]);
                ChunkInfoHandle*    cih;
                while ((cih = it.Next())) {
                    if (0 <= cih->chunkInfo.chunkVersion) {
                        (*pendingStale.first)++;
                        (*pendingStale.second) << ' ' << cih->chunkInfo.chunkId;
                    }
                }
            }
        }
        KFS_LOG_STREAM_NOTICE <<
            "hello resume succeeded:"
            " chunks: "         << count <<
            " / "               << hello.chunkCount <<
            " checksum: "       << checksum <<
            " / "               << hello.checksum <<
            " deleted: "        << hello.resumeDeleted.size() <<
            " modified: "       << hello.resumeModified.size() <<
            " lastInflt: "      << mLastPendingInFlight.GetSize() <<
            " / "               << mCorruptChunkOp.chunkCount <<
            " staleInFlt: "     << *pendingStale.first <<
            " pending notify: " << pendingNotifyFlag <<
            " -> "              << hello.pendingNotifyFlag <<
        KFS_LOG_EOM;
        hello.resumeStep = 1;
        return;
    }
    if (hello.pendingNotifyFlag) {
        mCounters.mPartialHelloResumeFailedCount++;
    }
    // Do not count first resume failure after restart, if prior the restart
    // partial hello was in progress, in order to make endurance test restart
    // work.
    if (! hello.pendingNotifyFlag || 0  < gMetaServerSM.GetHelloDoneCount()) {
        mCounters.mHelloResumeFailedCount++;
    }
    ofstream* traceTee = 0;
    if (! mHelloResumeFailureTraceFileName.empty()) {
        static ofstream sTraceTee;
        sTraceTee.clear();
        sTraceTee.open(mHelloResumeFailureTraceFileName.c_str(),
            ofstream::app | ofstream::out);
        if (sTraceTee.is_open() && sTraceTee) {
            traceTee = &sTraceTee;
        }
    }
    KFS_LOG_STREAM_START_TEE(MsgLogger::kLogLevelERROR, logStream, traceTee);
        logStream.GetStream() <<
        "hello resume failure:"
        " chunks:"
        " all: "            << mChunkTable.GetSize() <<
        " cs: "             << count <<
        " meta: "           << hello.chunkCount <<
        " checksum: "       << checksum <<
        " meta: "           << hello.checksum <<
        " deleted: "        << hello.resumeDeleted.size() <<
        " modified: "       << hello.resumeModified.size() <<
        " lastInflt: "      << mLastPendingInFlight.GetSize() <<
        " / "               << mCorruptChunkOp.chunkCount <<
        " resume: "         << hello.resumeStep <<
        " pending notify: " << hello.pendingNotifyFlag
        ;
    KFS_LOG_STREAM_END;
    if (traceTee) {
        *traceTee << "\n";
    }
    KFS_LOG_STREAM_START_TEE(MsgLogger::kLogLevelDEBUG, logStream, traceTee);
        ostream& os = logStream.GetStream();
        os << "last pending in flight[" <<
            mLastPendingInFlight.GetSize() <<
        "]:";
        for (mLastPendingInFlight.First(); os;) {
            const kfsChunkId_t* const p = mLastPendingInFlight.Next();
            if (! p) {
                break;
            }
            os << ' ' << *p;
        }
        os << "\nchunks[" << mChunkTable.GetSize() << "]:\n";
        size_t helloNotifyCnt = 0;
        for (mChunkTable.First(); os; ) {
            const CMapEntry* const p = mChunkTable.Next();
            if (! p) {
                break;
            }
            const kfsChunkId_t           chunkId = p->GetKey();
            const ChunkInfoHandle* const cih     = p->GetVal();
            if (cih->IsBeingReplicated()) {
                os << chunkId << " " << cih->chunkInfo.chunkVersion << " R";
            } else if (cih->IsRenameInFlight()) {
                bool stableFlag = false;
                kfsSeq_t const vers =
                    cih->GetTargetStateAndVersion(stableFlag);
                os << chunkId << " " << vers << (stableFlag ? " S" : " N");
            } else {
                os << chunkId << " " << cih->chunkInfo.chunkVersion <<
                    (IsChunkStable(cih) ? " S" : " N");
            }
            if (IsPendingHelloNotify(*cih)) {
                os << " H";
                helloNotifyCnt++;
            }
            os << "\n";
        }
        os << "\npending lost[" << mPendingNotifyLostChunks.GetSize() << "]:";
        for (mPendingNotifyLostChunks.First(); os; ) {
            const PendingNotifyLostChunks::KVPair* const p =
                mPendingNotifyLostChunks.Next();
            if (! p) {
                break;
            }
            os << ' ' << p->GetKey() << '.' <<  p->GetVal();
        }
        os << "\n";
        os << "hello mod[" << hello.resumeModified.size() << "]:";
        for (HelloMetaOp::ChunkIds::const_iterator
                it = hello.resumeModified.begin();
                it != hello.resumeModified.end() && os;
                ++it) {
            os << ' ' << *it;
        }
        os << "\n"
            "hello del[" << hello.resumeDeleted.size() << "]:";
        for (HelloMetaOp::ChunkIds::const_iterator
                it = hello.resumeDeleted.begin();
                it != hello.resumeDeleted.end() && os;
                ++it) {
            os << ' ' << *it;
        }
        os << "\n";
        if (0 < helloNotifyCnt) {
            os << "pending hello total: " << helloNotifyCnt << "\n";
        }
    KFS_LOG_STREAM_END;
    if (traceTee) {
        *traceTee << "\n";
        traceTee->close();
    }
    hello.resumeStep         = -1;
    hello.status             = 0;
    *(stable.first)          = 0;
    *(notStableAppend.first) = 0;
    *(notStable.first)       = 0;
    *(missing.first)         = 0;
}

bool
ChunkManager::CanBeResumed(HelloMetaOp& /* hello */)
{
    return (mMinChunkCountForHelloResume < (int64_t)mChunkTable.GetSize());
}

void
ChunkManager::GetHostedChunks(
    HelloMetaOp&                         hello,
    const ChunkManager::HostedChunkList& stable,
    const ChunkManager::HostedChunkList& notStableAppend,
    const ChunkManager::HostedChunkList& notStable,
    bool                                 noFidsFlag)
{
    if (0 != mHelloNotifyInFlightCount) {
        die("hello while pending hello notify in flight");
    }
    // walk thru the table and pick up the chunk-ids
    const bool usePendingNotifyFlag = 0 < mMaxHelloNotifyInFlightCount &&
        mMinChunkCountForHelloResume < (int64_t)mChunkTable.GetSize();
    mChunkTable.First();
    const CMapEntry* p;
    while ((p = mChunkTable.Next())) {
        ChunkInfoHandle* const cih            = p->GetVal();
        const bool             replicatedFlag = cih->IsBeingReplicated();
        if (usePendingNotifyFlag && ! replicatedFlag &&
                cih->IsChunkReadable() && ! cih->IsRenameInFlight()) {
            ChunkHelloNotifyList::PushBack(mChunkInfoHelloNotifyList, *cih);
            continue;
        }
        if (replicatedFlag) {
            // Do not report replicated chunks, replications should be canceled
            // on reconnect.
            HelloNotifyRemove(*cih);
            continue;
        }
        AppendToHostedList(
            *cih, stable, notStableAppend, notStable, noFidsFlag);
    }
    hello.pendingNotifyFlag =
        ! ChunkHelloNotifyList::IsEmpty(mChunkInfoHelloNotifyList);
}

void
ChunkManager::RunHelloNotifyQueue(AvailableChunksOp* cop)
{
    if (cop) {
        for (int i = 0; i < cop->numChunks; i++) {
            ChunkInfoHandle** const ci = mChunkTable.Find(cop->chunks[i].first);
            if (ci && *ci) {
                (*ci)->SetHelloNotify(false);
            }
        }
    }
    if (! gMetaServerSM.IsUp() ||
            ChunkHelloNotifyList::IsEmpty(mChunkInfoHelloNotifyList) ||
            (0 < mMaxHelloNotifyInFlightCount &&
                gMetaServerSM.HasPendingOps())) {
        delete cop;
        return;
    }
    AvailableChunksOp* cur = cop;
    if (cur) {
        cur->status    = 0;
        cur->numChunks = 0;
        cur->statusMsg.clear();
    }
    do {
        AvailableChunksOp& op =
            cur ? *cur : *(new AvailableChunksOp(&mHelloNotifyCb));
        cur = 0;
        op.helloFlag = true;
        ChunkInfoHandle* cih;
        while (op.numChunks < AvailableChunksOp::kMaxChunkIds &&
                (cih = ChunkHelloNotifyList::PopFront(mChunkInfoHelloNotifyList))) {
            AvailableChunksOp::Chunks& chunk = op.chunks[op.numChunks++];
            chunk.first  = cih->chunkInfo.chunkId;
            chunk.second = cih->chunkInfo.chunkVersion;
            cih->SetHelloNotify(true);
        }
        mHelloNotifyInFlightCount += op.numChunks;
        op.endOfNotifyFlag         =
            ChunkHelloNotifyList::IsEmpty(mChunkInfoHelloNotifyList);
        gMetaServerSM.EnqueueOp(&op);
    } while (mHelloNotifyInFlightCount < mMaxHelloNotifyInFlightCount &&
        ! ChunkHelloNotifyList::IsEmpty(mChunkInfoHelloNotifyList) &&
        gMetaServerSM.IsUp() && ! gMetaServerSM.HasPendingOps());
}

int
ChunkManager::HelloNotifyDone(int code, void* data)
{
    if (EVENT_CMD_DONE != code || ! data || mHelloNotifyInFlightCount <= 0) {
        die("HelloNotifyDone: invalid completion");
        return -1;
    }
    AvailableChunksOp& op = *reinterpret_cast<AvailableChunksOp*>(data);
    if (op.numChunks <= mHelloNotifyInFlightCount) {
        mHelloNotifyInFlightCount -= op.numChunks;
    } else {
        die("HelloNotifyDone: invalid in flight count");
        mHelloNotifyInFlightCount = 0;
    }
    if (0 != op.status) {
        if (gMetaServerSM.IsUp()) {
            ForceMetaServerDown(op);
        }
        for (int i = 0; i < op.numChunks; i++) {
            InsertLastInFlight(op.chunks[i].first);
        }
    }
    ChunkManager::RunHelloNotifyQueue(&op);
    return 0;
}

ChunkInfoHandle*
ChunkManager::GetChunkInfoHandle(
    kfsChunkId_t chunkId, int64_t chunkVersion,
    bool addObjectBlockMappingFlag /* = true */) const
{
    ChunkInfoHandle** const ci = 0 <= chunkVersion ?
        mChunkTable.Find(chunkId) :
        mObjTable.Find(make_pair(chunkId, chunkVersion));
    if (ci && ! *ci) {
        die("invalid chunk or object table entry");
    }
    if (addObjectBlockMappingFlag && ! ci && chunkVersion < 0) {
        const kfsSTier_t storageTier =
            (kfsSTier_t)((-chunkVersion - 1) % (int64_t)CHUNKSIZE);
        ChunkDirInfo* const chunkdir =
            const_cast<ChunkManager*>(this)->GetDirForChunk(
                chunkVersion < 0, storageTier, storageTier);
        if (! chunkdir) {
            return 0;
        }
        ChunkInfoHandle* const cih = new ChunkInfoHandle(*chunkdir);
        cih->chunkInfo.fileId       = chunkId;
        cih->chunkInfo.chunkId      = chunkId;
        cih->chunkInfo.chunkVersion = chunkVersion;
        cih->chunkInfo.chunkSize    = (int64_t)CHUNKSIZE;
        if (cih != const_cast<ChunkManager*>(this)->AddMapping(cih)) {
            die("duplicate object block table entry");
            const_cast<ChunkManager*>(this)->Delete(*cih);
            return 0;
        }
        return cih;
    }
    return (ci ? *ci : 0);
}

int
ChunkManager::AllocateWriteId(
    WriteIdAllocOp*       wi,
    int                   replicationPos,
    const ServerLocation& peerLoc)
{
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const cih = GetChunkInfoHandle(
        wi->chunkId, wi->chunkVersion, kAddObjectBlockMappingFlag);
    if (! cih) {
        wi->statusMsg = "no such chunk";
        wi->status = -EBADF;
    } else if (wi->chunkVersion != cih->chunkInfo.chunkVersion) {
        wi->statusMsg = "chunk version mismatch";
        wi->status = -EINVAL;
    } else if (wi->isForRecordAppend && IsWritePending(
            wi->chunkId, wi->chunkVersion)) {
        wi->statusMsg = "random write in progress";
        wi->status = -EINVAL;
    } else if (wi->isForRecordAppend && ! IsWriteAppenderOwns(
            wi->chunkId, wi->chunkVersion)) {
        wi->statusMsg = "not open for append";
        wi->status = -EINVAL;
    } else if (! wi->isForRecordAppend && cih->IsWriteAppenderOwns()) {
        wi->statusMsg = "write append in progress";
        wi->status = -EINVAL;
    } else {
        mWriteId++;
        if (PendingWrites::IsObjStoreWriteId(mWriteId) ==
                (0 <= wi->chunkVersion)) {
            mWriteId++;
            assert(PendingWrites::IsObjStoreWriteId(mWriteId) !=
                (0 <= wi->chunkVersion));
        }
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
                wi->chunkId, wi->chunkVersion,
                wi->offset, wi->numBytes, mWriteId
            );
            op->seq             = wi->seq;
            op->enqueueTime     = globalNetManager().Now();
            op->isWriteIdHolder = true;
            mPendingWrites.push_back(op);
            LruUpdate(*cih); // Move back to prevent spurious scans.
        }
    }
    if (0 == wi->status) {
        if (cih) {
            cih->SetWriteIdIssuedFlag(true);
        }
    } else {
        KFS_LOG_STREAM_ERROR <<
            "failed: " << wi->Show() <<
        KFS_LOG_EOM;
    }
    wi->writePrepareReplyFlag = mWritePrepareReplyFlag;
    return wi->status;
}

WriteOp*
ChunkManager::CloneWriteOp(int64_t writeId)
{
    WriteOp* const other = mPendingWrites.FindAndMoveBackIfOk(writeId);
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
    WriteOp* const ret = new WriteOp(other->chunkId, other->chunkVersion,
        0, 0, other->writeId);
    ret->seq = other->seq;
    return ret;
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
ChunkManager::RunStaleChunksQueue(
    StaleChunkDeleteCompletion* completion /* = 0 */)
{
    if (completion) {
        StaleChunkDeleteCompletion::Release(
            *completion, mStaleChunkDeleteCompletionLists);
        if (mStaleChunkOpsInFlight <= 0) {
            die("invalid stale chunk queue in flight count");
        } else {
            mStaleChunkOpsInFlight--;
            mDoneStaleChunksCount++;
        }
    }
    mRunStaleQueueRecursionCount++;
    ChunkList::Iterator it(mChunkInfoLists[kChunkStaleList]);
    ChunkInfoHandle*    cih;
    while (mCleanupStaleChunksFlag && mRunStaleQueueRecursionCount <= 1 &&
            mStaleChunkOpsInFlight < mMaxStaleChunkOpsInFlight &&
            (cih = it.Next())) {
        if (cih->IsRenameInFlight()) {
            die("attempt to delete stale chunk with rename in flight");
        }
        // If disk queue has been already stopped, then the queue directory
        // prefix has already been removed, and it will not be possible to
        // queue disk io request (delete or rename) anyway, and attempt to do
        // so will return an error.
        bool inFlightFlag = false;
        if ((0 <= cih->chunkInfo.chunkVersion ||
                cih->GetForceDeleteObjectStoreBlockFlag()) &&
                cih->GetDirInfo().diskQueue) {
            // If the chunk with target version already exists withing the same
            // chunk directory, then do not issue delete. A new chunk file can
            // be created by replication or recovery while the previous chunk
            // handle as in the pending stale queue.
            const ChunkInfoHandle* const* const ci =
                0 <= cih->chunkInfo.chunkVersion ?
                mChunkTable.Find(cih->chunkInfo.chunkId) : 0;
            if (! ci ||
                    &((*ci)->GetDirInfo()) != &(cih->GetDirInfo()) ||
                    ! (*ci)->CanHaveSameFileName(
                        cih->chunkInfo, cih->IsStable())) {
                StaleChunkDeleteCompletion& cb =
                    StaleChunkDeleteCompletion::Create(
                        cih->DetachStaleDeleteCompletionOp(),
                        mStaleChunkDeleteCompletionLists
                    );
                if (cih->IsKeep()) {
                    inFlightFlag = MarkChunkStale(cih, &cb) == 0;
                } else {
                    const string fileName = MakeChunkPathname(cih);
                    string       err;
                    inFlightFlag = DiskIo::Delete(fileName.c_str(), &cb, &err);
                    KFS_LOG_STREAM(inFlightFlag ?
                            MsgLogger::kLogLevelINFO :
                            MsgLogger::kLogLevelERROR) <<
                        "deleting stale chunk: " << fileName <<
                        (inFlightFlag ? " ok" : " error: ") << err <<
                        " in flight: " << mStaleChunkOpsInFlight <<
                    KFS_LOG_EOM;
                }
                if (inFlightFlag) {
                    mStaleChunkOpsInFlight++;
                } else {
                    StaleChunkDeleteCompletion::Release(
                        cb, mStaleChunkDeleteCompletionLists);
                }
            } else {
                bool           targetStableFlag = false;
                kfsSeq_t const targetVersion    =
                    (*ci)->GetTargetStateAndVersion(targetStableFlag);
                KFS_LOG_STREAM_DEBUG <<
                    "not deleting:"
                    " chunk: "   << cih->chunkInfo.chunkId <<
                    " version: " << cih->chunkInfo.chunkVersion <<
                    " / "        << (*ci)->chunkInfo.chunkVersion <<
                    " / "        << targetVersion <<
                    " stable: "  << cih->IsStable() <<
                    " / "        << (*ci)->IsStable() <<
                    " / "        << targetStableFlag <<
                KFS_LOG_EOM;
            }
        }
        if (! inFlightFlag) {
            mDoneStaleChunksCount++;
        }
        const int64_t size = min(mUsedSpace, cih->chunkInfo.chunkSize);
        UpdateDirSpace(cih, -size);
        if (0 <= cih->chunkInfo.chunkVersion) {
            mUsedSpace -= size;
        }
        Delete(*cih);
    }
    mRunStaleQueueRecursionCount--;
    if (mFlushStaleChunksOp &&
            mFlushStaleChunksCount <= mDoneStaleChunksCount) {
        KfsOp* const op = mFlushStaleChunksOp;
        mFlushStaleChunksOp = 0;
        int64_t res = 0;
        op->HandleEvent(EVENT_DISK_DELETE_DONE, &res);
    }
}

int
ChunkManager::FlushStaleQueue(KfsOp& op)
{
    KfsOp* const cur = mFlushStaleChunksOp;
    mFlushStaleChunksOp    = &op;
    mFlushStaleChunksCount = mStaleChunksCount;
    if (cur && cur != mFlushStaleChunksOp) {
        // Submit previous op waiting.
        int64_t res = 0;
        cur->HandleEvent(EVENT_DISK_DELETE_DONE, &res);
    }
    if (mFlushStaleChunksCount <= mDoneStaleChunksCount) {
        KfsOp* const op = mFlushStaleChunksOp;
        mFlushStaleChunksOp = 0;
        int64_t res = 0;
        op->HandleEvent(EVENT_DISK_DELETE_DONE, &res);
    }
    return 0;
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
    if (mNextSendChunDirInfoTime < now && gMetaServerSM.IsUp()) {
        SendChunkDirInfo();
        mNextSendChunDirInfoTime = now + mSendChunDirInfoIntervalSecs;
    }
    if (0 < mLogChunkServerCountersInterval &&
            mLogChunkServerCountersLastTime +
                mLogChunkServerCountersInterval < now &&
            ! gMetaServerSM.IsUp()) {
        LogChunkServerCounters();
    }
    gLeaseClerk.Timeout();
    gAtomicRecordAppendManager.Timeout();
}

template<typename TT, typename WT> void
ChunkManager::ScavengePendingWrites(
    time_t now, TT& table, WT& pendingWrites)
{
    const time_t opExpireTime = now - mMaxPendingWriteLruSecs;
    while (! pendingWrites.empty()) {
        WriteOp* const op = pendingWrites.front();
        // The list is sorted by enqueue time
        if (opExpireTime < op->enqueueTime) {
            break;
        }
        // if it exceeds 5 mins, retire the op
        KFS_LOG_STREAM_DEBUG <<
            "expiring write with id: " << op->writeId <<
            " chunk: "                 << op->chunkId <<
            " version: "               << op->chunkVersion <<
        KFS_LOG_EOM;
        ChunkInfoHandle** const ce  = table.Find(pendingWrites.FrontKey());
        ChunkInfoHandle*  const cih = ce ? *ce : 0;
        pendingWrites.pop_front();
        delete op;
        if (cih) {
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
    }
}

void
ChunkManager::ScavengePendingWrites(time_t now)
{
    ScavengePendingWrites(now, mChunkTable, mPendingWrites.GetChunkWrites());
    ScavengePendingWrites(now, mObjTable,   mPendingWrites.GetObjWrites());
}

bool
ChunkManager::CleanupInactiveFds(time_t now, const ChunkInfoHandle* cur)
{
    // if we haven't cleaned up in 5 mins or if we too many fd's that
    // are open, clean up.
    time_t expireTime;
    int    releaseCnt = -1;
    if (cur) {
        // Reserve is to deal with asynchronous close/open in the cases where
        // open and close are executed on different io queues.
        const uint64_t kReserve     = min((mMaxOpenChunkFiles + 3) / 4,
            32 + (int)mChunkDirs.size());
        const uint64_t openChunkCnt = globals().ctrOpenDiskFds.GetValue();
        if (openChunkCnt + kReserve > (uint64_t)mMaxOpenChunkFiles ||
                (openChunkCnt + kReserve) * mFdsPerChunk +
                    globals().ctrOpenNetFds.GetValue() > (uint64_t)mMaxOpenFds) {
            if (mNextInactiveFdFullScanTime < now) {
                expireTime = now + 2 * mInactiveFdsCleanupIntervalSecs;
            } else {
                expireTime = now - (mInactiveFdsCleanupIntervalSecs + 3) / 4;
            }
            releaseCnt = kReserve;
        } else {
            expireTime = now - mInactiveFdsCleanupIntervalSecs;
        }
    } else {
        if (now < mNextInactiveFdCleanupTime) {
            ChunkLru::Iterator it(mChunkInfoLists[kChunkLruList]);
            ChunkInfoHandle*   cih;
            while ((cih = it.Next()) && cih->chunkInfo.chunkVersion < 0 &&
                    ! cih->IsFileOpen()) {
                Remove(*cih);
            }
            return true;
        }
        expireTime = now - mInactiveFdsCleanupIntervalSecs;
    }
    const time_t       unstableObjBlockExpireTime =
        now - mInactiveFdsCleanupIntervalSecs;
    ChunkLru::Iterator it(mChunkInfoLists[kChunkLruList]);
    ChunkInfoHandle*   cih;
    while ((cih = it.Next())) {
        if (cih == cur) {
            continue;
        }
        if (! cih->IsFileOpen() || cih->IsBeingReplicated()) {
            if (cih->chunkInfo.chunkVersion < 0) {
                Remove(*cih); // Was scheduled for object table cleanup.
            } else {
                // Doesn't belong here, if / when io completes it will be
                // added back.
                ChunkLru::Remove(mChunkInfoLists[kChunkLruList], *cih);
            }
            continue;
        }
        if (expireTime <= cih->lastIOTime) {
            break;
        }
        bool   hasLeaseFlag         = false;
        bool   writePendingFlag     = false;
        bool   objBlockMetaDownFlag = false;
        bool   unstableObjBlockFlag = false;
        time_t metaUptime           = 0;
        const bool fileInUseFlag        = cih->IsFileInUse();
        if (fileInUseFlag ||
                (hasLeaseFlag = gLeaseClerk.IsLeaseValid(
                    cih->chunkInfo.chunkId, cih->chunkInfo.chunkVersion)) ||
                (writePendingFlag = IsWritePending(cih->chunkInfo.chunkId,
                    cih->chunkInfo.chunkVersion)) ||
                ((unstableObjBlockFlag = cih->chunkInfo.chunkVersion < 0 &&
                    ! cih->IsStable()) &&
                        unstableObjBlockExpireTime < cih->lastIOTime) ||
                (objBlockMetaDownFlag = unstableObjBlockFlag &&
                    (metaUptime = gMetaServerSM.ConnectionUptime()) <
                        LEASE_INTERVAL_SECS &&
                        (metaUptime < mObjBlockDiscardMinMetaUptime ||
                        now < cih->lastIOTime +
                            (cih->GetWriteIdIssuedFlag() <= 0 ?
                            mObjStoreBlockMaxNonStableDisconnectedTime * 2 / 3 :
                            mObjStoreBlockMaxNonStableDisconnectedTime)))) {
            KFS_LOG_STREAM_DEBUG << "cleanup: ignoring entry in chunk lru:"
                " chunk: "        << cih->chunkInfo.chunkId <<
                " version: "      << cih->chunkInfo.chunkVersion <<
                " size: "         << cih->chunkInfo.chunkSize <<
                " dataFH: "       << (const void*)cih->dataFH.get() <<
                " use count: "    << cih->dataFH.use_count() <<
                " stable: "       << cih->IsStable() <<
                " had write id: " << cih->GetWriteIdIssuedFlag() <<
                " last io: "      << (now - cih->lastIOTime) << " sec. ago" <<
                " meta uptime: "  << gMetaServerSM.ConnectionUptime() <<
                (fileInUseFlag ?        " file in use"       : "") <<
                (hasLeaseFlag ?         " has lease"         : "") <<
                (writePendingFlag ?     " write pending"     : "") <<
                (objBlockMetaDownFlag ? " short meta uptime" : "") <<
            KFS_LOG_EOM;
            continue;
        }
        if ((cih->chunkInfo.chunkVersion < 0 ?
                ! cih->DiscardMeta() : cih->SyncMeta())) {
            continue;
        }
        KFS_LOG_STREAM_DEBUG << "cleanup: closing"
            " chunk: "        << cih->chunkInfo.chunkId <<
            " version: "      << cih->chunkInfo.chunkVersion <<
            " size: "         << cih->chunkInfo.chunkSize <<
            " dataFH: "       << (const void*)cih->dataFH.get() <<
            " stable: "       << cih->IsStable() <<
            " had write id: " << cih->GetWriteIdIssuedFlag() <<
            " last io: "      << (now - cih->lastIOTime) << " sec. ago" <<
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
    if ((int)KFS_MIN_CHUNK_HEADER_SIZE < IOBufferData::GetDefaultBufferSize()) {
        KFS_LOG_STREAM_INFO <<
            "invalid io buffer size: " <<
                IOBufferData::GetDefaultBufferSize() <<
            " exceeds chunk header size: " << KFS_MIN_CHUNK_HEADER_SIZE <<
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
        int    kMinWriteBlkSize               = 0;
        bool   kBufferDataIgnoreOverwriteFlag = false;
        int    kinBufferDataTailToKeepSize    = 0;
        bool   kCreateExclusiveFlag           = true;
        if (! DiskIo::StartIoQueue(
                it->dirname.c_str(),
                it->deviceId,
                mMaxOpenChunkFiles,
                &errMsg,
                kMinWriteBlkSize,
                kBufferDataIgnoreOverwriteFlag,
                kinBufferDataTailToKeepSize,
                kCreateExclusiveFlag,
                mDiskIoRequestAffinityFlag,
                mDiskIoSerializeMetaRequestsFlag
            )) {
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
    const int64_t kMaxFileSize         = KFS_CHUNK_HEADER_SIZE + CHUNKSIZE;
    mObjStoreBufferDataMaxSizePerBlock = min(
        (int)kMaxFileSize,
        2 * mObjStoreBlockWriteBufferSize +
            (int)(KFS_CHUNK_HEADER_SIZE + CHECKSUM_BLOCKSIZE)
    );
    // Ensure that device id for obj store will not collide with normal the host
    // file system ids issued by the directory checker, in order to detect
    // possible name collisions between host file system directories and object
    // store directories.
    const int64_t     kMaxSpace = int64_t(1) << 59;
    DiskIo::DeviceId  objDevId  =
        DiskIo::DeviceId(1) << (sizeof(DiskIo::DeviceId) * 8 - 2);
    for (ChunkDirs::iterator it = mObjDirs.begin();
            it != mObjDirs.end();
            ++it) {
        it->dirCountSpaceAvailable      = &*it;
        it->fileSystemId                = mFileSystemId;
        it->deviceId                    = objDevId++;
        it->availableSpace              = kMaxSpace;
        it->totalSpace                  = kMaxSpace;
        it->supportsSpaceReservatonFlag = false;
        it->availableChunks.Clear();
        const bool kCreateExclusiveFlag = false;
        const bool kCanUseIoMethodFlag  = true;
        string     errMsg;
        if (! DiskIo::StartIoQueue(
                it->dirname.c_str(),
                it->deviceId,
                mMaxOpenChunkFiles,
                &errMsg,
                mObjStoreBlockWriteBufferSize,
                mObjStoreBufferDataIgnoreOverwriteFlag &&
                    mObjStoreBlockWriteBufferSize <
                    (int)(CHUNKSIZE + KFS_CHUNK_HEADER_SIZE),
                (int)(KFS_CHUNK_HEADER_SIZE + CHECKSUM_BLOCKSIZE),
                kCreateExclusiveFlag,
                mObjStoreIoRequestAffinityFlag,
                mObjStoreIoSerializeMetaRequestsFlag,
                mObjStoreIoThreadCount,
                kMaxFileSize,
                kCanUseIoMethodFlag
            )) {
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
        const int writeBufSize = DiskIo::GetMinWriteBlkSize(it->diskQueue);
        if (0 < writeBufSize && writeBufSize < (int)KFS_CHUNK_HEADER_SIZE) {
            KFS_LOG_STREAM_FATAL <<
                "failed to start disk queue for: " << it->dirname <<
                " dev: << " << it->deviceId <<
                " : invalid write buffer size: " << writeBufSize <<
                " less than " << KFS_CHUNK_HEADER_SIZE <<
            KFS_LOG_EOM;
            DiskIo::Shutdown();
            return false;
        }
        it->startTime = globalNetManager().Now();
        it->startCount++;
        KFS_LOG_STREAM_INFO <<
            "object store: " << it->dirname <<
            " tier: "        << (int)it->storageTier <<
            " devId: "       << it->deviceId <<
            " space:"
            " available: "   << it->availableSpace <<
            " used: "        << it->usedSpace <<
            " sprsrv: "      << it->supportsSpaceReservatonFlag <<
        KFS_LOG_EOM;
        StorageTiers::mapped_type& tier = mObjStorageTiers[it->storageTier];
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
    mFileSystemIdSuffix.clear();
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
    for (ChunkDirs::iterator it = mObjDirs.begin();
            it < mObjDirs.end();
            ++it) {
        it->chunkDirInfoOp.Enqueue();
    }
}

BufferManager*
ChunkManager::FindDeviceBufferManager(
    kfsChunkId_t chunkId, int64_t chunkVersion)
{
    if (! mDiskBufferManagerEnabledFlag) {
        return 0;
    }
    const bool kAddObjectBlockMappingFlag = false;
    ChunkInfoHandle* const cih = GetChunkInfoHandle(chunkId, chunkVersion,
        kAddObjectBlockMappingFlag);
    if (cih) {
        return DiskIo::GetDiskBufferManager(cih->GetDirInfo().diskQueue);
    }
    if (0 <= chunkVersion) {
        return 0;
    }
    // Use find "directory" / queue that will likely to be used for bloc.
    const kfsSTier_t storageTier =
        (kfsSTier_t)((-chunkVersion - 1) % (int64_t)CHUNKSIZE);
    ChunkDirInfo* const dir = GetDirForChunk(
        chunkVersion < 0, storageTier, storageTier);
    return (dir ? DiskIo::GetDiskBufferManager(dir->diskQueue) : 0);
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
    if (code != EVENT_CMD_DONE || &evacuateChunksOp != data ||
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
    const bool kAddObjectBlockMappingFlag = false;
    for ( ; i < evacuateChunksOp.numChunks; i++) {
        ChunkInfoHandle* const cih =
            gChunkManager.GetChunkInfoHandle(evacuateChunksOp.chunkIds[i], 0,
                kAddObjectBlockMappingFlag);
        if (cih && &(cih->GetDirInfo()) == this) {
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
        if (0 <= availableChunksOp.status || availableChunksOp.numChunks <= 0) {
            availableChunksOp.numChunks = 0;
            while (! availableChunks.IsEmpty() &&
                    availableChunksOp.numChunks <
                    AvailableChunksOp::kMaxChunkIds) {
                const DirChecker::ChunkInfo& ci  = availableChunks.Back();
                ChunkInfoHandle* const       cih = new ChunkInfoHandle(*this);
                cih->chunkInfo.fileId       = ci.mFileId;
                cih->chunkInfo.chunkId      = ci.mChunkId;
                cih->chunkInfo.chunkVersion = ci.mChunkVersion;
                if (ci.mChunkSize < 0 || ci.mChunkVersion < 0) {
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
                cih->SetPendingAvailable(true);
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
        availableChunksOp.noReply   = true;
    }
    if (timeoutPendingFlag) {
        timeoutPendingFlag = false;
        globalNetManager().UnRegisterTimeoutHandler(this);
    }
    if ((availableChunksOp.numChunks <= 0 && ! availableChunksOp.noReply) ||
            ! globalNetManager().IsRunning() ||
            ! gMetaServerSM.IsRunning()) {
        return;
    }
    availableChunksOpInFlightFlag = true;
    availableChunksOp.status = 0;
    availableChunksOp.statusMsg.clear();
    gMetaServerSM.EnqueueOp(&availableChunksOp);
}

int
ChunkManager::ChunkDirInfo::AvailableChunksDone(int code, void* data)
{
    if (EVENT_CMD_DONE != code || &availableChunksOp != data ||
            ! availableChunksOpInFlightFlag) {
        die("AvailableChunksDone invalid completion");
        return -EINVAL;
    }
    availableChunksOpInFlightFlag = false;
    if (0 != availableChunksOp.status && gMetaServerSM.IsUp()) {
        ForceMetaServerDown(availableChunksOp);
    }
    // Re-queue to handle meta server transaction log failure.
    // If meta server connection went down then add chunks to last in flight
    // chunks regardless of the status in order for hello resume to succeed.
    const bool metaDownFlag = ! gMetaServerSM.IsUp();
    const bool kIgnorePendingAvailableFlag = true;
    for (int i = 0; i < availableChunksOp.numChunks; i++) {
        const kfsChunkId_t chunkId = availableChunksOp.chunks[i].first;
        if (metaDownFlag) {
            gChunkManager.InsertLastInFlight(chunkId);
        }
        const bool             kAddObjectBlockMappingFlag = false;
        ChunkInfoHandle* const cih                        =
            gChunkManager.GetChunkInfoHandle(
                chunkId,
                availableChunksOp.chunks[i].second,
                kAddObjectBlockMappingFlag);
            if (! cih ||
                    ! cih->IsPendingAvailable() ||
                    cih->chunkInfo.chunkVersion !=
                        availableChunksOp.chunks[i].second ||
                    &(cih->GetDirInfo()) != this ||
                    ! cih->IsChunkReadable(kIgnorePendingAvailableFlag) ||
                    cih->IsRenameInFlight() ||
                    cih->IsStale() ||
                    cih->IsBeingReplicated() ||
                    cih->readChunkMetaOp ||
                    cih->dataFH ||
                    cih->chunkInfo.AreChecksumsLoaded()) {
            if (cih && cih->IsPendingAvailable() &&
                    &(cih->GetDirInfo()) == this) {
                cih->SetPendingAvailable(false); // Reset.
            }
            continue;
        }
        cih->SetPendingAvailable(false); // Completion.
    }
    availableChunksOp.numChunks = 0;
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
    ObjTable::Entry::value_type const* p;
    mObjTable.First();
    while ((p = mObjTable.Next())) {
        ChunkInfoHandle* const cih = p->GetVal();
        if (! cih->IsChunkReadable()) {
            mPendingWrites.Delete(
                cih->chunkInfo.chunkId, cih->chunkInfo.chunkVersion);
            bool stableFlag = false;
            cih->GetTargetStateAndVersion(stableFlag);
            if (! stableFlag) {
                cih->SubmitWaitChunkReadable(-ELEASEEXPIRED);
            }
        }
    }
    mNextSendChunDirInfoTime = globalNetManager().Now() - 36000;
}

long
ChunkManager::GetNumWritableChunks() const
{
    return (long)mPendingWrites.GetChunkWrites().GetChunkIdCount();
}

long
ChunkManager::GetNumWritableObjects() const
{
    return (long)mPendingWrites.GetObjWrites().GetChunkIdCount();
}

long
ChunkManager::GetNumOpenObjects() const
{
    return (long)mObjTable.GetSize();
}

void
ChunkManager::CheckChunkDirs()
{
    KFS_LOG_STREAM_DEBUG << "checking chunk dirs" << KFS_LOG_EOM;
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
            int    kMinWriteBlkSize               = 0;
            bool   kBufferDataIgnoreOverwriteFlag = false;
            int    kinBufferDataTailToKeepSize    = 0;
            bool   kCreateExclusiveFlag           = true;
            string errMsg;
            if (DiskIo::StartIoQueue(
                    it->dirname.c_str(),
                    dit->second.mDeviceId,
                    mMaxOpenChunkFiles,
                    &errMsg,
                    kMinWriteBlkSize,
                    kBufferDataIgnoreOverwriteFlag,
                    kinBufferDataTailToKeepSize,
                    kCreateExclusiveFlag,
                    mDiskIoRequestAffinityFlag,
                    mDiskIoSerializeMetaRequestsFlag
                )) {
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
                it->lostChunksCount             = 0;
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
                    new CorruptChunkOp(-1, &(it->dirname), true));
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
