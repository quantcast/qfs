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
#include "util.h"

#include "qcdio/qcdebug.h"
#include "qcdio/QCThread.h"

#include "common/MsgLogger.h"
#include "common/SingleLinkedQueue.h"

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

#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>

namespace KFS
{
using std::max;

using client::KfsNetClient;
using client::KfsOp;

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
            startLogSeq    = -1;
            endLogSeq      = -1;
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
public:
    Impl(
        NetManager& inNetManager)
        : OpOwner(),
          ITimeout(),
          KfsCallbackObj(),
          QCRunnable(),
          mRuntimeNetManager(inNetManager),
          mStartupNetManager(),
          mKfsNetClient(mStartupNetManager),
          mServers(),
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
          mMaxLogBlockSize(64 << 10),
          mMaxReadSize(64 << 10),
          mMaxRetryCount(10),
          mRetryCount(0),
          mRetryTimeout(3),
          mCurMaxReadSize(-1),
          mFd(-1),
          mServerIdx(0),
          mPos(0),
          mNextReadPos(0),
          mFileSize(-1),
          mLogSeq(-1),
          mNextLogSegIdx(-1),
          mCheckpointFlag(false),
          mBuffer(),
          mFreeWriteOpList(),
          mFreeWriteOpCount(0),
          mNextBlockChecksum(kKfsNullChecksum),
          mCurBlockChecksum(kKfsNullChecksum),
          mNextBlockSeq(0),
          mLogWritesInFlightCount(0),
          mWriteToFileFlag(false),
          mWriteSyncFlag(true),
          mMinWriteSize(4 << 20),
          mMaxReadOpRetryCount(8),
          mSleepingFlag(false),
          mShutdownNetManagerFlag(false),
          mCheckStartLogSeqFlag(false),
          mCheckLogSeqOnlyFlag(false),
          mReadPipelineFlag(false),
          mKeepLogSegmentsInterval(10),
          mNoLogSeqCount(0),
          mStatus(0),
          mThread(),
          mReplayerPtr(0)
    {
        SET_HANDLER(this, &Impl::LogWriteDone);
        mKfsNetClient.SetAuthContext(&mAuthContext);
        mKfsNetClient.SetMaxContentLength(3 * mMaxReadSize / 2);
        mKfsNetClient.SetMaxRetryCount(5);
        mKfsNetClient.SetOpTimeoutSec(10);
        mKfsNetClient.SetTimeSecBetweenRetries(4);
        mKfsNetClient.SetFailAllOpsOnOpTimeoutFlag(true);
    }
    virtual ~Impl()
    {
        Impl::Shutdown();
        QCRTASSERT(0 == mLogWritesInFlightCount);
    }
    int SetParameters(
        const char*       inParamPrefixPtr,
        const Properties& inParameters)
    {
        if (mThread.IsStarted()) {
            KFS_LOG_STREAM_ERROR <<
                "parameters update ignored: keep logs thread is running" <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        Properties::String theName(inParamPrefixPtr ? inParamPrefixPtr : "");
        const size_t       thePrefLen = theName.GetSize();
        mMaxReadSize = max(1 << 10, inParameters.getValue(
            theName.Truncate(thePrefLen).Append("maxReadSize"),
            mMaxReadSize));
        mKfsNetClient.SetMaxContentLength(3 * mMaxReadSize / 2);
        mMaxLogBlockSize = max(4 << 10, inParameters.getValue(
            theName.Truncate(thePrefLen).Append("maxLogBlockSize"),
            mMaxLogBlockSize));
        if (! mReadOpsPtr) {
            mReadOpsCount = max(size_t(1), inParameters.getValue(
            theName.Truncate(thePrefLen).Append("maxReadSize"),
            mReadOpsCount));
        }
        mTmpSuffix = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("tmpSuffix"),
            mTmpSuffix);
        mTmpSuffix = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("tmpSuffix"),
            mTmpSuffix);
        mMaxRetryCount = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("maxRetryCount"),
            mMaxRetryCount);
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
        mKeepLogSegmentsInterval = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("keepLogSegmentsInterval"),
            mKeepLogSegmentsInterval);
        mFileSystemId = inParameters.getValue(
            theName.Truncate(thePrefLen).Append("fileSystemId"),
            mFileSystemId);
        mKfsNetClient.SetOpTimeoutSec(max(1, inParameters.getValue(
                theName.Truncate(thePrefLen).Append("readOpTimeoutSec"),
                mKfsNetClient.GetOpTimeoutSec())));
        mKfsNetClient.SetMaxRetryCount(max(0, inParameters.getValue(
                theName.Truncate(thePrefLen).Append("maxOpRetryCount"),
                mKfsNetClient.GetMaxRetryCount())));
        mKfsNetClient.SetTimeSecBetweenRetries(inParameters.getValue(
                theName.Truncate(thePrefLen).Append("timeBetweenRetries"),
                mKfsNetClient.GetTimeSecBetweenRetries()));
        const Properties::String* const theServersPtr = inParameters.getValue(
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
                    }
                }
                mServers.swap(theServers);
            }
        }
        const bool               kVerifyFlag = true;
        ClientAuthContext* const kNullCtxPtr = 0;
        string* const            kNullStrPtr = 0;
        const int theRet = mAuthContext.SetParameters(
            theName.Truncate(thePrefLen).Append("authentication.").GetPtr(),
            inParameters,
            kNullCtxPtr,
            kNullStrPtr,
            kVerifyFlag
        );
        return (theOkFlag ? theRet : -EINVAL);
    }
    int Start(
        const char* inCheckpointDirPtr,
        const char* inLogDirPtr)
    {
        if (mReadOpsPtr || ! inCheckpointDirPtr || ! *inCheckpointDirPtr ||
                ! inLogDirPtr || ! *inLogDirPtr) {
            KFS_LOG_STREAM_ERROR <<
                "meta data sync: invalid parameters" <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        if (mServers.empty()) {
            return 0;
        }
        mNextBlockChecksum = ComputeBlockChecksum(kKfsNullChecksum, "\n", 1);
        mReadOpsPtr = new ReadOp[mReadOpsCount];
        for (size_t i = 0; i < mReadOpsCount; i++) {
            mFreeList.PutFront(mReadOpsPtr[i]);
        }
        mCheckpointDir = inCheckpointDirPtr;
        mLogDir        = inLogDirPtr;
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
        if (! theEmptyFsFlag) {
            mLogSeq = GetLastLogSeq();
            if (mLogSeq < 0) {
                KFS_LOG_STREAM_ERROR <<
                    "failed to obtain log segment sequence" <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            mShutdownNetManagerFlag = true;
            LogSeqCheckStart();
            mKfsNetClient.GetNetManager().MainLoop();
            mShutdownNetManagerFlag = false;
            if (0 != mStatus && mNoLogSeqCount <= 0) {
                return mStatus;
            }
        }
        if (mLogSeq < 0) {
            const size_t theCnt = mServers.size();
            if (1 < theCnt) {
                // Randomly choose server to download from.
                unsigned int theRnd;
                CryptoKeys::PseudoRand(&theRnd, sizeof(theRnd));
                mServerIdx = theRnd % theCnt;
            }
            int theRet;
            if (0 != (theRet = PrepareToSync()) ||
                    0 != (theRet = GetCheckpoint())) {
                return theRet;
            }
            mShutdownNetManagerFlag = true;
            mKfsNetClient.GetNetManager().MainLoop();
            mShutdownNetManagerFlag = false;
            if (0 != mStatus) {
                return mStatus;
            }
            theRet = CommitSync();
            if (0 != theRet) {
                return theRet;
            }
            mLogSeq = -mLogSeq;
        }
        LogSeqCheckStart();
        int kStackSize = 64 << 10;
        mThread.Start(this, kStackSize, "MetaSyncKeepLogs");
        return 0;
    }
    void StartLogSync(
        seq_t                  inLogSeq,
        LogReceiver::Replayer& inReplayer)
    {
        if (! mReadOpsPtr) {
            return;
        }
        StopKeepData();
        Reset();
        mReplayerPtr     = &inReplayer;
        mLogSeq          = inLogSeq;
        mWriteToFileFlag = false;
        mStatus          = 0;
        mKfsNetClient.SetNetManager(mRuntimeNetManager);
        if (mServers.empty()) {
            return;
        }
        LogFetchStart();
    }
    virtual void Run()
    {
        mKfsNetClient.GetNetManager().MainLoop();
        Reset();
        if (mSleepingFlag) {
            mSleepingFlag = false;
            mKfsNetClient.GetNetManager().UnRegisterTimeoutHandler(this);
        }
    }
    void Shutdown()
    {
        StopKeepData();
        if (mSleepingFlag) {
            mSleepingFlag = false;
            mKfsNetClient.GetNetManager().UnRegisterTimeoutHandler(this);
        }
        Reset();
        delete [] mReadOpsPtr;
        mReadOpsPtr = 0;
        MetaRequest* thePtr;
        while ((thePtr = mFreeWriteOpList.PopFront())) {
            mFreeWriteOpCount--;
            MetaRequest::Release(thePtr);
        }
        QCASSERT(0 == mFreeWriteOpCount);
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
            } else if (theOp.startLogSeq < 0) {
                theOp.status    = -EINVAL;
                theOp.statusMsg = "invalid log sequence";
            } else if (theOp.fileSystemId < 0) {
                theOp.status    = -EINVAL;
                theOp.statusMsg = "invalid file system id";
            } else if (0 <= theOp.endLogSeq && ! theOp.checkpointFlag &&
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
        if (! mSleepingFlag) {
            return;
        }
        mKfsNetClient.GetNetManager().UnRegisterTimeoutHandler(this);
        mSleepingFlag = false;
        if (mMaxRetryCount < mRetryCount) {
            HandleError();
        } else {
            StartRead();
        }
    }
private:
    enum { kMaxCommitLineLen = 512 };
    typedef vector<ServerLocation> Servers;
    typedef SingleLinkedQueue<
        MetaRequest,
        MetaRequest::GetNext
    > FreeWriteOpList;
    typedef LogReceiver::Replayer Replayer;

    NetManager&       mRuntimeNetManager;
    NetManager        mStartupNetManager;
    KfsNetClient      mKfsNetClient;
    Servers           mServers;
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
    int               mRetryCount;
    int               mRetryTimeout;
    int               mCurMaxReadSize;
    int               mFd;
    size_t            mServerIdx;
    int64_t           mPos;
    int64_t           mNextReadPos;
    int64_t           mFileSize;
    seq_t             mLogSeq;
    seq_t             mNextLogSegIdx;
    bool              mCheckpointFlag;
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
    bool              mSleepingFlag;
    bool              mShutdownNetManagerFlag;
    bool              mCheckStartLogSeqFlag;
    bool              mCheckLogSeqOnlyFlag;
    bool              mReadPipelineFlag;
    int               mKeepLogSegmentsInterval;
    int               mNoLogSeqCount;
    int               mStatus;
    QCThread          mThread;
    Replayer*         mReplayerPtr;
    char              mCommmitBuf[kMaxCommitLineLen];

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
        QCASSERT(0 <= mLogSeq && ! mServers.empty());
        mStatus = 0;
        if (0 <= mFd) {
            close(mFd);
            mFd = -1;
        }
        if (mServers.size() <= mServerIdx) {
            mServerIdx = 0;
        }
        StopAndClearPending();
        mKfsNetClient.SetServer(mServers[mServerIdx]);
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
        if (mServers.size() <= mServerIdx) {
            mServerIdx = 0;
        }
        StopAndClearPending();
        mKfsNetClient.SetServer(mServers[mServerIdx]);
        mStatus               = 0;
        mLogSeq               = -1;
        mCheckpointFlag       = true;
        mWriteToFileFlag      = true;
        mCheckStartLogSeqFlag = false;
        mNextLogSegIdx        = -1;
        mCheckpointFileName.clear();
        mLastLogFileName.clear();
        InitRead();
        return 0;
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
        inOp.fileSystemId   = mFileSystemId;
        inOp.startLogSeq    = mLogSeq;
        inOp.readPos        = inOp.mPos;
        inOp.checkpointFlag = mCheckpointFlag;
        inOp.readSize       = 0 <= mFileSize ?
            (int)min(mFileSize - inOp.mPos, int64_t(mCurMaxReadSize)) :
            mCurMaxReadSize;
        inOp.mInFlightFlag  = true;
        inOp.mPos           = inOp.mPos;
        inOp.fileSize       = -1;
        inOp.status         = 0;
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
        if (0 <= mLogSeq && ((0 != mPos || mWriteToFileFlag) ?
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
            if (mCheckStartLogSeqFlag && 0 <= mLogSeq &&
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
            mLogSeq         = inOp.startLogSeq;
            mCurMaxReadSize = min(mMaxReadSize, 0 < inOp.maxReadSize ?
                inOp.maxReadSize : inOp.mBuffer.BytesConsumable());
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
                size_t const theDotPos = inOp.fileName.find('.');
                size_t const theEndPos = mCheckpointFlag ?
                    inOp.fileName.size() : inOp.fileName.rfind('.');
                seq_t theSeq    = -1;
                seq_t theSegIdx = -1;
                const char* thePtr = inOp.fileName.data() + theDotPos + 1;
                if (string::npos == theDotPos || string::npos == theEndPos ||
                        theEndPos <= theDotPos ||
                        ! DecIntParser::Parse(
                            thePtr, theEndPos - theDotPos - 1, theSeq) ||
                        theSeq != mLogSeq ||
                        inOp.fileName.find('/') != string::npos ||
                        (! mCheckpointFlag && (! DecIntParser::Parse(
                                ++thePtr,
                                inOp.fileName.size() - theEndPos - 1,
                                theSegIdx) ||
                            (0 < mNextLogSegIdx &&
                                theSegIdx != mNextLogSegIdx)))) {
                    KFS_LOG_STREAM_ERROR <<
                        "invalid file name: " << inOp.Show() <<
                        " log sequence:"
                        " expected: " << mLogSeq <<
                        " actual: "   << theSeq <<
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
                    mNextLogSegIdx = theSegIdx;
                    mFileName.assign(mLogDir.data(), mLogDir.size());
                }
                if (! mFileName.empty() && '/' != *mFileName.rbegin()) {
                    mFileName += '/';
                }
                mFileName += inOp.fileName;
                mFileName += mTmpSuffix;
                mFd = open(
                    mFileName.c_str(),
                    O_WRONLY | (mWriteSyncFlag ? O_SYNC : 0) |
                        O_CREAT | O_TRUNC,
                    0666
                );
                if (mFd < 0) {
                    const int theErr = errno;
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
        bool theEofFlag;
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
                (mCurMaxReadSize <= theOpPtr->maxReadSize ?
                    theRdSize < mCurMaxReadSize : theRdSize <= 0);
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
                        mBuffer.Consume(theNWr);
                    }
                }
            } else {
                if (theOpPtr->checkpointFlag || mCheckpointFlag) {
                    panic("metad data sync: attempt to replay checkpoint");
                } else {
                    SubmitLogBlock(theOpPtr->mBuffer);
                }
            }
            if (theEofFlag && ! mCheckpointFlag) {
                mLogSeq = theOpPtr->endLogSeq < 0 ?
                    -theOpPtr->startLogSeq : theOpPtr->endLogSeq;
                mNextLogSegIdx++;
            }
            theOpPtr->Reset();
            mPendingList.PopFront();
            mFreeList.PutFront(*theOpPtr);
        } while (
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
                const int theErr = errno;
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
            } else if (0 <= mLogSeq) {
                mLastLogFileName.assign(
                    mFileName.data(), mFileName.size());
            }
        }
        mBuffer.Clear();
        mRetryCount     = 0;
        mCheckpointFlag = false;
        if (0 <= mLogSeq) {
            mCheckStartLogSeqFlag = true;
            InitRead();
            if (! StartRead()) {
                panic("metad data sync: failed to iniate download");
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
            "sync complte:"
            " status: "   << mStatus <<
            " seq: "      << -mLogSeq <<
            " shutdown: " << mShutdownNetManagerFlag <<
        KFS_LOG_EOM;
        if (mShutdownNetManagerFlag) {
            mKfsNetClient.GetNetManager().Shutdown();
        }
    }
    void HandleReadError(
        ReadOp& inReadOp)
    {
        KFS_LOG_STREAM_ERROR <<
            "status: " << inReadOp.statusMsg <<
            " try: "   << mRetryCount <<
            " op: "    << inReadOp.mRetryCount <<
            " pos:"
            " cur: "   << mPos <<
            " op: "    << inReadOp.mPos <<
            " "        << inReadOp.Show() <<
        KFS_LOG_EOM;
        mStatus = inReadOp.status;
        if (mCheckLogSeqOnlyFlag) {
            if (-ENOENT == mStatus ||
                    KfsNetClient::kErrorMaxRetryReached == mStatus) {
                if (-ENOENT == mStatus) {
                    mNoLogSeqCount++;
                }
                ScheduleGetLogSeqNextServer();
                return;
            }
        }
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
        HandleError();
    }
    void ScheduleGetLogSeqNextServer()
    {
        Reset();
        if (++mServerIdx < mServers.size()) {
            Sleep(0);
        } else {
            if (mShutdownNetManagerFlag) {
                mKfsNetClient.GetNetManager().Shutdown();
            } else {
                Sleep(mKeepLogSegmentsInterval);
            }
        }
    }
    void HandleError(
        MetaLogWriterControl& inOp)
    {
        inOp.status = -EINVAL;
        mLogWritesInFlightCount++;
        LogWriteDone(EVENT_CMD_DONE, &inOp);
        HandleError();
    }
    void HandleError()
    {
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
        const bool kResetTimerFlag = true;
        SetTimeoutInterval(inTimeSec * 1000, kResetTimerFlag);
        mKfsNetClient.GetNetManager().RegisterTimeoutHandler(this);
    }
    void Reset()
    {
        if (0 < mFd) {
            close(mFd);
            mFd = -1;
        }
        ClearPendingList();
        mKfsNetClient.Stop();
        QCRTASSERT(mPendingList.IsEmpty());
        mBuffer.Clear();
        mPos         = 0;
        mNextReadPos = 0;
    }
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
                HexIntParser::Parse(
                    theStartPtr,
                    ioPtr - theStartPtr,
                    outVal)) {
            ++ioPtr;
            return true;
        }
        return false;
    }
    void SubmitLogBlock(
        IOBuffer& inBuffer)
    {
        mBuffer.Move(&inBuffer);
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
            int theLen = theEndPos - thePos;
            if (kMaxCommitLineLen < theLen) {
                KFS_LOG_STREAM_ERROR <<
                    "log block commit line"
                    " length: "  << theLen <<
                    " exceeds: " << kMaxCommitLineLen <<
                    IOBuffer::DisplayData(mBuffer, 512) <<
                KFS_LOG_EOM;
                HandleError();
                return;
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
                return;
            }
            // Copy log block body, i.e. except trailing / commit line.
            theOp.blockData.Move(&mBuffer, thePos);
            const char* const theStartPtr =
                mBuffer.CopyOutOrGetBufPtr(mCommmitBuf, theLen);
            const char* const theEndPtr   = theStartPtr + theLen;
            const char*       thePtr      = theStartPtr;
            thePtr += 2;
            seq_t   theCommitted   = -1;
            fid_t   theFid         = -1;
            int64_t theErrChkSum   = -1;
            int     theBlockStatus = -1;
            seq_t   theLogSeq      = -1;
            int     theBlockSeqLen = -1;
            if (! (ParseField(thePtr, theEndPtr, theCommitted) &&
                    ParseField(thePtr, theEndPtr, theFid) &&
                    ParseField(thePtr, theEndPtr, theErrChkSum) &&
                    ParseField(thePtr, theEndPtr, theBlockStatus) &&
                    ParseField(thePtr, theEndPtr, theLogSeq) &&
                    ParseField(thePtr, theEndPtr, theBlockSeqLen) &&
                    0 <= theCommitted &&
                    0 <= theBlockStatus &&
                    theCommitted <= theLogSeq &&
                    theBlockSeqLen <= theLogSeq)) {
                KFS_LOG_STREAM_ERROR <<
                    "invalid log commit line:" <<
                    IOBuffer::DisplayData(mBuffer, theLen) <<
                KFS_LOG_EOM;
                HandleError();
                return;
            }
            theOp.blockStartSeq = theLogSeq - theBlockSeqLen;
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
                return;
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
                return;
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
                return;
            }
            theOp.blockLines.Back() -=
                mBuffer.Consume(theLen - theCopyLen + 1);
            mCurBlockChecksum = mNextBlockChecksum;
            mLogWritesInFlightCount++;
            if (theBlockSeqLen <= 0) {
                LogWriteDone(EVENT_CMD_DONE, &theOp);
            } else {
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
        }
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
        MetaLogWriterControl& theOp = *reinterpret_cast<MetaLogWriterControl*>(
            inDataPtr);
        const bool theErrorFlag = 0 != theOp.status &&
            theOp.blockStartSeq == theOp.committed;
        KFS_LOG_STREAM(theErrorFlag ?
                MsgLogger::kLogLevelERROR :
                MsgLogger::kLogLevelDEBUG) <<
            "log write:"
            " status: "    << theOp.status <<
            " "            << theOp.statusMsg <<
            " committed: " << theOp.committed <<
            " in flight: " << mLogWritesInFlightCount <<
            " "            << theOp.Show() <<
        KFS_LOG_EOM;
        if (0 == theOp.status &&
                theOp.committed == theOp.blockEndSeq &&
                mReplayerPtr) {
            mReplayerPtr->Apply(theOp);
            return 0;
        }
        if (theErrorFlag) {
            Reset();
        }
        if (mReadOpsPtr) {
            theOp.Reset();
            theOp.type = MetaLogWriterControl::kWriteBlock;
            theOp.clnt = this;
            mFreeWriteOpCount++;
            mFreeWriteOpList.PutFront(theOp);
        } else {
            MetaRequest::Release(&theOp);
        }
        return 0;
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
    int CommitSync()
    {
        if (mCheckpointFileName.empty()) {
            panic("meta data sync: invalid commit sync invocation");
            return -EINVAL;
        }
        const bool kDeleteTmpFlag = false;
        int        theRet;
        if (0 != (theRet = DeleteTmp(mLogDir, kDeleteTmpFlag))) {
            return theRet;
        }
        if (0 != (theRet = RenameTmp(mLogDir))) {
            return theRet;
        }
        if (0 != (theRet = DeleteTmp(mCheckpointDir, kDeleteTmpFlag))) {
            return theRet;
        }
        if (0 != (theRet = RenameTmp(mCheckpointDir))) {
            return theRet;
        }
        if (0 != (theRet = LinkLatest(mCheckpointFileName, "latest"))) {
            return theRet;
        }
        return (mLastLogFileName.empty() ? 0 :
            LinkLatest(mLastLogFileName, "last"));
    }
    template<typename FuncT>
    int ListDirEntries(
        const char* inDirNamePtr,
        FuncT&      inFunc)
    {
        DIR* const theDirPtr = opendir(inDirNamePtr);
        if (! theDirPtr) {
            const int theErr = errno;
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
            size_t        inSuffixLen)
            : SetPathName(inDirName),
              mSuffixLen(inSuffixLen),
              mDestName()
            {}
        int operator()(
            const char* inNamePtr,
            size_t      inLength,
            bool        inTmpSuffixFlag)
        {
            SetName(inNamePtr, inLength);
            if (! inTmpSuffixFlag || inLength <= mSuffixLen) {
                KFS_LOG_STREAM_ERROR <<
                    "unexpected directory entry: " << mName <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            mDestName.assign(mName.data(), mName.size() - mSuffixLen);
            if (rename(mName.c_str(), mDestName.c_str())) {
                const int theErr = errno;
                KFS_LOG_STREAM_ERROR <<
                    "rename: " << mName <<
                    " => "     << mDestName <<
                    ": " << QCUtils::SysError(theErr) <<
                KFS_LOG_EOM;
                return (theErr < 0 ? theErr :
                    theErr == 0 ? -EINVAL : -theErr);
            }
            return 0;
        }
    private:
        size_t const mSuffixLen;
        string       mDestName;
    };
    int RenameTmp(
        string& inDirName)
    {
        if (inDirName.empty() || mTmpSuffix.empty()) {
            KFS_LOG_STREAM_ERROR <<
                "invalid parameters"
                " direcotry: "  << inDirName <<
                " siffix: "     << mTmpSuffix <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        RenameTmpFunc theFunc(inDirName, mTmpSuffix.size());
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
                    const int theErr = errno;
                    KFS_LOG_STREAM_ERROR <<
                        "unlink: " << mName <<
                        ": " << QCUtils::SysError(theErr) <<
                    KFS_LOG_EOM;
                    return (theErr < 0 ? theErr :
                        theErr == 0 ? -EINVAL : -theErr);
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
            : mSeq(-1)
            {}
        int operator()(
            const char* inNamePtr,
            size_t      inLength,
            bool        inTmpSuffixFlag)
        {
            if (inTmpSuffixFlag) {
                return 0;
            }
            const char* thePtr = strchr(inNamePtr, '.');
            if (thePtr) {
                const char* theEndPtr = strchr(++thePtr, '.');
                if (theEndPtr) {
                    seq_t theSeq = -1;
                    if (DecIntParser::Parse(
                            thePtr, theEndPtr - thePtr, theSeq)) {
                        mSeq = max(mSeq, theSeq);
                    }
                }
            }
            return 0;
        }
        seq_t Get() const
            { return mSeq; }
    private:
        seq_t mSeq;
    private:
        GetMaxLogSeq(
            const GetMaxLogSeq&);
        GetMaxLogSeq& operator=(
            const GetMaxLogSeq&);
    };
    seq_t GetLastLogSeq()
    {
        GetMaxLogSeq theFunc;
        const int theStatus = ListDirEntries(mLogDir.c_str(), theFunc);
        if (theStatus < 0) {
            return theStatus;
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
        theFileName +=  "latest";
        const int theFd = open(theFileName.c_str(), O_RDONLY);
        if (theFd < 0) {
            const int theErr = errno;
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
            const int theErr = errno;
            KFS_LOG_STREAM_ERROR <<
                "read " << theFileName << ": " <<
                QCUtils::SysError(theErr) <<
            KFS_LOG_EOM;
            theRet = -theErr;
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
    const Properties& inParameters)
{
    return mImpl.SetParameters(inParamPrefixPtr, inParameters);
}

    int
MetaDataSync::Start(
    const char* inCheckpointDirPtr,
    const char* inLogDirPtr)
{
    return mImpl.Start(inCheckpointDirPtr, inLogDirPtr);
}

    void
MetaDataSync::StartLogSync(
    seq_t                  inLogSeq,
    LogReceiver::Replayer& inReplayer)
{
    mImpl.StartLogSync(inLogSeq, inReplayer);
}

    void
MetaDataSync::Shutdown()
{
    mImpl.Shutdown();
}

} // namespace KFS
