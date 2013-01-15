//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2009/05/20
// Author: Mike Ovsiannikov
//
// Copyright 2009-2011 Quantcast Corp.
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

#include "WriteAppender.h"

#include <sstream>
#include <algorithm>
#include <cerrno>
#include <sstream>
#include <deque>

#include "kfsio/IOBuffer.h"
#include "kfsio/NetManager.h"
#include "kfsio/Globals.h"
#include "kfsio/checksum.h"
#include "kfsio/ITimeout.h"
#include "common/kfsdecls.h"
#include "common/MsgLogger.h"
#include "qcdio/QCUtils.h"
#include "KfsOps.h"
#include "utils.h"
#include "KfsClient.h"
#include "ClientPool.h"

namespace KFS
{
namespace client
{
using std::max;
using std::min;
using std::string;
using std::vector;
using std::deque;
using std::istringstream;
using std::ostringstream;

// Kfs client write append state machine implementation.
class WriteAppender::Impl : private ITimeout, private KfsNetClient::OpOwner
{
public:
    Impl(
        WriteAppender& inOuter,
        MetaServer&    inMetaServer,
        Completion*    inCompletionPtr,
        int            inMaxRetryCount,
        int            inWriteThreshold,
        int            inTimeSecBetweenRetries,
        int            inDefaultSpaceReservationSize,
        int            inPreferredAppendSize,
        int            inMaxPartialBuffersCount,
        int            inOpTimeoutSec,
        int            inIdleTimeoutSec,
        bool           inPreAllocationFlag,
        string         inLogPrefix,
        int64_t        inChunkServerInitialSeqNum,
        ClientPool*    inClientPoolPtr)
        : ITimeout(),
          KfsNetClient::OpOwner(),
          mOuter(inOuter),
          mMetaServer(inMetaServer),
          mChunkServer(
            mMetaServer.GetNetManager(),
            "", -1,
            // All chunk server retries are handled here
            0, // inMaxRetryCount
            0, // inTimeSecBetweenRetries,
            inOpTimeoutSec,
            inIdleTimeoutSec,
            inChunkServerInitialSeqNum,
            inLogPrefix.c_str()
          ),
          mPathName(),
          mFileName(),
          mWriteIds(),
          mCanceledFlag(false),
          mSleepingFlag(false),
          mOpenFlag(false),
          mOpeningFlag(false),
          mClosingFlag(false),
          mMakeDirsFlag(false),
          mPreAllocationFlag(inPreAllocationFlag),
          mErrorCode(0),
          mSpaceAvailable(0),
          mRetryCount(0),
          mAppendRestartRetryCount(0),
          mWriteThreshold(inWriteThreshold),
          mNumReplicas(0),
          mPartialBuffersCount(0),
          mAppendLength(0),
          mForcedAllocationInterval(0),
          mOpTimeoutSec(inOpTimeoutSec),
          mMaxRetryCount(inMaxRetryCount),
          mTimeSecBetweenRetries(inTimeSecBetweenRetries),
          mDefaultSpaceReservationSize(
            min((int)KFS::CHUNKSIZE, inDefaultSpaceReservationSize)),
          mMaxPartialBuffersCount(inMaxPartialBuffersCount),
          mPreferredAppendSize(min((int)KFS::CHUNKSIZE, inPreferredAppendSize)),
          mPathNamePos(0),
          mOpStartTime(0),
          mCurOpPtr(0),
          mCompletionPtr(inCompletionPtr),
          mBuffer(),
          mWriteQueue(),
          mLookupOp(0, 0, ""),
          mMkdirOp(0, 0, ""),
          mCreateOp(0, 0, "", mNumReplicas, false),
          mLookupPathOp(0, 0, ""),
          mAllocOp(0, 0, ""),
          mCloseOp(0, 0),
          mWriteIdAllocOp(0, 0, 0, 0, 0),
          mSpaceReserveOp(0, 0, 0, mWriteIds, 0),
          mRecAppendOp(0, 0, 0, -1, mWriteIds),
          mSpaceReleaseOp(0, 0, 0, mWriteIds, 0),
          mGetRecordAppendOpStatusOp(0, 0, 0),
          mPrevRecordAppendOpSeq(-1),
          mGetRecordAppendOpStatusIndex(0u),
          mLogPrefix(inLogPrefix),
          mStats(),
          mLastAppendActivityTime(0),
          mClientPoolPtr(inClientPoolPtr),
          mChunkServerPtr(0),
          mNetManager(mMetaServer.GetNetManager())
    {
        Impl::Reset();
        mChunkServer.SetRetryConnectOnly(true);
    }
    ~Impl()
    {
        mMetaServer.Cancel(mCurOpPtr, this);
        StopChunkServer();
        Impl::Register(0);
        if (mSleepingFlag) {
            mNetManager.UnRegisterTimeoutHandler(this);
        }
    }
    int Open(
        const char* inFileNamePtr,
        int         inNumReplicas,
        bool        inMakeDirsFlag)
    {
        if (! inFileNamePtr || ! *inFileNamePtr) {
            return -EINVAL;
        }
        if (mOpenFlag) {
            if (inFileNamePtr == mPathName &&
                    inNumReplicas == mNumReplicas) {
                return mErrorCode;
            }
            return -EINVAL;
        }
        if (mErrorCode) {
            return mErrorCode;
        }
        if (mClosingFlag || mOpeningFlag || mSleepingFlag) {
            return -EAGAIN;
        }
        mBuffer.Clear();
        mStats.Clear();
        mPartialBuffersCount   = 0;
        mOpeningFlag           = true;
        mNumReplicas           = inNumReplicas;
        mPathName              = inFileNamePtr;
        mErrorCode             = 0;
        mPathNamePos           = 0;
        mSpaceReserveOp.status = 0; // Do allocate with append flag.
        mMakeDirsFlag          = inMakeDirsFlag;
        assert(! mPathName.empty());
        LookupPath();
        return mErrorCode;
    }
    int Open(
        kfsFileId_t inFileId,
        const char* inFileNamePtr)
    {
        if (inFileId <= 0 || ! inFileNamePtr || ! *inFileNamePtr) {
            return -EINVAL;
        }
        if (mOpenFlag) {
            if (inFileId == mLookupOp.fattr.fileId &&
                    inFileNamePtr == mPathName) {
                return mErrorCode;
            }
            return -EINVAL;
        }
        if (mErrorCode) {
            return mErrorCode;
        }
        if (mClosingFlag || mOpeningFlag || mSleepingFlag) {
            return -EAGAIN;
        }
        mBuffer.Clear();
        mStats.Clear();
        mPartialBuffersCount   = 0;
        mPathName              = inFileNamePtr;
        mErrorCode             = 0;
        mPathNamePos           = 0;
        mSpaceReserveOp.status = 0;  // Do allocate with append flag.
        mMakeDirsFlag          = false;
        mNumReplicas           = 0; // Do not create if doesn't exist.
        assert(! mPathName.empty());
        mLookupOp.parentFid = -1;   // Input, not known, and not needed.
        mLookupOp.status    = 0;
        if (inFileId > 0) {
            mLookupOp.fattr.fileId      = inFileId;
            mLookupOp.fattr.isDirectory = false;
            mOpenFlag                   = true;
            mOpeningFlag                = false;
            ReportCompletion();
            StartAppend();
        } else {
            mOpeningFlag = true;
            LookupPath();
        }
        return mErrorCode;
    }
    int Close()
    {
        if (! mOpenFlag) {
            if (mOpeningFlag) {
                mClosingFlag = true;
            }
            return 0;
        }
        if (mErrorCode) {
            return mErrorCode;
        }
        if (mClosingFlag) {
            return -EAGAIN;
        }
        mClosingFlag = true;
        if (! mCurOpPtr) {
            StartAppend();
        }
        return mErrorCode;
    }
    int Append(
        IOBuffer& inBuffer,
        int       inLength)
    {
        if (mErrorCode) {
            return mErrorCode;
        }
        if (mClosingFlag || (! mOpenFlag && ! mOpeningFlag)) {
            return -EINVAL;
        }
        if (inLength <= 0) {
            return 0;
        }
        if (mMaxPartialBuffersCount == 0 ||
                inLength < IOBufferData::GetDefaultBufferSize() * 2) {
            // If record is too small, just copy it into the last buffer.
            mBuffer.ReplaceKeepBuffersFull(&inBuffer,
                mBuffer.BytesConsumable(), inLength);
        } else {
            if (mBuffer.IsEmpty()) {
                mPartialBuffersCount = 0;
            }
            mBuffer.Move(&inBuffer, inLength);
            mPartialBuffersCount++;
            if (mMaxPartialBuffersCount >= 0 &&
                    mPartialBuffersCount >= mMaxPartialBuffersCount) {
                mBuffer.MakeBuffersFull();
                mPartialBuffersCount = 0;
                mStats.mBufferCompactionCount++;
            }
        }
        const int kMinWriteQueueEntrySize = 256;
        if (mWriteQueue.empty() ||
                mWriteQueue.back() > kMinWriteQueueEntrySize) {
            mWriteQueue.push_back(inLength);
        } else {
            mWriteQueue.back() += inLength;
        }
        if (! mCurOpPtr && mOpenFlag) {
            StartAppend();
        }
        return (mErrorCode ?
            (mErrorCode < 0 ? mErrorCode : - mErrorCode) : inLength);
    }
    void Shutdown()
    {
        Reset();
        StopChunkServer();
        mMetaServer.Cancel(mCurOpPtr, this);
        if (mSleepingFlag) {
            mNetManager.UnRegisterTimeoutHandler(this);
            mSleepingFlag = false;
        }
        mClosingFlag  = false;
        mOpeningFlag  = false;
        mOpenFlag     = false;
        mErrorCode    = 0;
        mWriteQueue.clear();
        mBuffer.Clear();
    }
    bool IsOpen() const
        { return (mOpenFlag && ! mClosingFlag); }
    bool IsOpening() const
        { return (! mOpenFlag && mOpeningFlag); }
    bool IsClosing() const
        { return (mOpenFlag && mClosingFlag); }
    bool IsSleeping() const
        { return ((mOpenFlag || mOpeningFlag) && mSleepingFlag); }
    bool IsActive() const
        { return (mOpenFlag || mOpeningFlag); }
    int GetPendingSize() const
        { return mBuffer.BytesConsumable(); }
    string GetServerLocation() const
        { return GetChunkServer().GetServerLocation(); }
    int SetWriteThreshold(
        int inThreshold)
    {
        const bool theStartAppendFlag = mWriteThreshold > inThreshold;
        mWriteThreshold = inThreshold;
        if (theStartAppendFlag && ! mCurOpPtr && mOpenFlag &&
                mErrorCode == 0 && ! mWriteQueue.empty()) {
            StartAppend();
        }
        return mErrorCode;
    }
    void Register(
        Completion* inCompletionPtr)
    {
        if (inCompletionPtr == mCompletionPtr) {
            return;
        }
        if (mCompletionPtr) {
            mCompletionPtr->Unregistered(mOuter);
        }
        mCompletionPtr = inCompletionPtr;
    }
    bool Unregister(
        Completion* inCompletionPtr)
    {
        if (inCompletionPtr != mCompletionPtr) {
            return false;
        }
        mCompletionPtr = 0;
        return true;
    }
    void GetStats(
        Stats&               outStats,
        KfsNetClient::Stats& outChunkServersStats)
    {
        outStats = mStats;
        mChunkServer.GetStats(outChunkServersStats);
    }
    int SetPreAllocation(
        bool inFlag)
    {
        if (inFlag == mPreAllocationFlag) {
            return mErrorCode;
        }
        mPreAllocationFlag = inFlag;
        if (mPreAllocationFlag && ! mCurOpPtr && mOpenFlag &&
                mErrorCode == 0 && ! mWriteQueue.empty()) {
            StartAppend();
        }
        return mErrorCode;
    }
    bool GetPreAllocation() const
        {  return mPreAllocationFlag; }
    bool GetErrorCode() const
        { return mErrorCode; }
    void SetForcedAllocationInterval(
        int inInterval)
        { mForcedAllocationInterval = inInterval; }

protected:
    virtual void OpDone(
        KfsOp*    inOpPtr,
        bool      inCanceledFlag,
        IOBuffer* inBufferPtr)
    {
        if (mCurOpPtr != inOpPtr && ! mErrorCode) {
            abort();
        }
        if (inOpPtr) {
            KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                "<- " << inOpPtr->Show() <<
                (inCanceledFlag ? " canceled" : "") <<
                " buffer: " << (void*)inBufferPtr <<
                "/" << (inBufferPtr ? inBufferPtr->BytesConsumable() : 0) <<
                " status: " << inOpPtr->status <<
                " seq: " << inOpPtr->seq <<
            KFS_LOG_EOM;
        } else {
            KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                "NULL operation completion? " <<
                (inCanceledFlag ? " canceled" : "") <<
                " buffer: " << (void*)inBufferPtr <<
                "/" << (inBufferPtr ? inBufferPtr->BytesConsumable() : 0) <<
            KFS_LOG_EOM;
        }
        bool theOpFoundFlag;
        if (mErrorCode || inCanceledFlag) {
            NopDispatch theNopDispatch;
            theOpFoundFlag = Dispatch(theNopDispatch, inOpPtr, inBufferPtr);
            if (theOpFoundFlag) {
                if (inCanceledFlag) {
                    HandleCancel();
                } else {
                    mCurOpPtr = 0;
                }
            }
        } else {
            theOpFoundFlag = Dispatch(*this, inOpPtr, inBufferPtr);
        }
        assert(theOpFoundFlag);
        if (! theOpFoundFlag) {
            abort();
        }
    }

private:
    enum
    {
        kErrorAppenderBase = 100000,
        kErrorOpCanceled   = -(kErrorAppenderBase + 1),
        kErrorMetaEnqueue  = -(kErrorAppenderBase + 2),
        kErrorChunkEnqueue = -(kErrorAppenderBase + 3)
    };
    enum { kAgainRetryMinTime            = 4      };
    enum { kGetStatusOpMinTime           = 16     };
    enum { kAppendInactivityCheckTimeout = 3 * 60 };

    typedef KfsNetClient      ChunkServer;
    typedef vector<WriteInfo> WriteIds;
    typedef deque<int>        WriteQueue;
    typedef string::size_type StringPos;
    struct NopDispatch
    {
        void Done(
            KfsOp&    inOpPtr,
            IOBuffer* inBufferPtr) {}
    };

    WriteAppender&          mOuter;
    MetaServer&             mMetaServer;
    ChunkServer             mChunkServer;
    string                  mPathName;
    string                  mFileName;
    WriteIds                mWriteIds;
    bool                    mCanceledFlag;
    bool                    mSleepingFlag;
    bool                    mOpenFlag;
    bool                    mOpeningFlag;
    bool                    mClosingFlag;
    bool                    mMakeDirsFlag;
    bool                    mPreAllocationFlag;
    int                     mErrorCode;
    int                     mSpaceAvailable;
    int                     mRetryCount;
    int                     mAppendRestartRetryCount;
    int                     mWriteThreshold;
    int                     mNumReplicas;
    int                     mPartialBuffersCount;
    int                     mAppendLength;
    int                     mForcedAllocationInterval;
    const int               mOpTimeoutSec;
    const int               mMaxRetryCount;
    const int               mTimeSecBetweenRetries;
    const int               mDefaultSpaceReservationSize;
    const int               mMaxPartialBuffersCount;
    const int               mPreferredAppendSize;
    StringPos               mPathNamePos;
    time_t                  mOpStartTime;
    KfsOp*                  mCurOpPtr;
    Completion*             mCompletionPtr;
    IOBuffer                mBuffer;
    WriteQueue              mWriteQueue;
    LookupOp                mLookupOp;
    MkdirOp                 mMkdirOp;
    CreateOp                mCreateOp;
    LookupPathOp            mLookupPathOp;
    AllocateOp              mAllocOp;
    CloseOp                 mCloseOp;
    WriteIdAllocOp          mWriteIdAllocOp;
    ChunkSpaceReserveOp     mSpaceReserveOp;
    RecordAppendOp          mRecAppendOp;
    ChunkSpaceReleaseOp     mSpaceReleaseOp;
    GetRecordAppendOpStatus mGetRecordAppendOpStatusOp;
    int64_t                 mPrevRecordAppendOpSeq;
    unsigned int            mGetRecordAppendOpStatusIndex;
    string const            mLogPrefix;
    Stats                   mStats;
    time_t                  mLastAppendActivityTime;
    ClientPool*             mClientPoolPtr;
    ChunkServer*            mChunkServerPtr;
    NetManager&             mNetManager;

    template<typename T> bool Dispatch(
        T&        inObj,
        KfsOp*    inOpPtr,
        IOBuffer* inBufferPtr)
    {
        if (&mWriteIdAllocOp == inOpPtr) {
            inObj.Done(mWriteIdAllocOp, inBufferPtr);
        } else if (&mSpaceReserveOp == inOpPtr) {
            inObj.Done(mSpaceReserveOp, inBufferPtr);
        } else if (&mSpaceReleaseOp == inOpPtr) {
            inObj.Done(mSpaceReleaseOp, inBufferPtr);
        } else if (&mRecAppendOp == inOpPtr) {
            inObj.Done(mRecAppendOp, inBufferPtr);
        } else if (&mLookupOp == inOpPtr) {
            inObj.Done(mLookupOp, inBufferPtr);
        } else if (&mMkdirOp == inOpPtr) {
            inObj.Done(mMkdirOp, inBufferPtr);
        } else if (&mCreateOp == inOpPtr) {
            inObj.Done(mCreateOp, inBufferPtr);
        } else if (&mLookupPathOp == inOpPtr) {
            inObj.Done(mLookupPathOp, inBufferPtr);
        } else if (&mAllocOp == inOpPtr) {
            inObj.Done(mAllocOp, inBufferPtr);
        } else if (&mCloseOp == inOpPtr) {
            inObj.Done(mCloseOp, inBufferPtr);
        } else if (&mGetRecordAppendOpStatusOp == inOpPtr) {
            inObj.Done(mGetRecordAppendOpStatusOp, inBufferPtr);
        } else {
            return false;
        }
        return true;
    }
    void StopChunkServer()
    {
        if (mChunkServerPtr && mChunkServerPtr != &mChunkServer) {
            mChunkServerPtr->Cancel(mCurOpPtr, this);
        }
        mChunkServerPtr = 0;
        mChunkServer.Stop();
    }
    bool WasChunkServerDisconnected()
    {
        return (mClientPoolPtr ?
            (! mChunkServerPtr || mChunkServerPtr->WasDisconnected()) :
            mChunkServer.WasDisconnected()
        );
    }
    void StartAppend()
    {
        if (mSleepingFlag || mErrorCode) {
            return;
        }
        mCurOpPtr = 0;
        if (mClosingFlag && mWriteQueue.empty()) {
            if (! WasChunkServerDisconnected()) {
                if (mAllocOp.chunkId > 0 && mSpaceAvailable > 0) {
                    SpaceRelease();
                    return;
                }
                if (mAllocOp.chunkId > 0) {
                    CloseChunk();
                    return;
                }
            }
            StopChunkServer();
            mMetaServer.Cancel(mCurOpPtr, this);
            mClosingFlag = false;
            mOpeningFlag = false;
            mOpenFlag    = false;
            ReportCompletion();
            return;
        }
        if ((mDefaultSpaceReservationSize <= 0 || ! mPreAllocationFlag) &&
                ! CanAppend()) {
            return;
        }
        if (mAllocOp.chunkId > 0 && WasChunkServerDisconnected()) {
            // When chunk server disconnects it automatically cleans up
            // space reservation and write appenders. Start from the
            // beginning -- chunk allocation.
            KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                "detected chunk server disconnect: " << GetServerLocation() <<
                " starting from chunk allocation, pending:" <<
                " queue: " << mWriteQueue.size() <<
                " bytes: " << mBuffer.BytesConsumable() <<
            KFS_LOG_EOM;
            Reset();
            if (! CanAppend()) {
                // Do not try to preallocate chunk and reserve space
                // after inactivity timeout or error, if no data pending.
                return;
            }
        }
        if (mAllocOp.chunkId > 0 && mSpaceReserveOp.status == -ENOSPC) {
            if (mSpaceAvailable > 0) {
                SpaceRelease();
            } else {
                CloseChunk();
            }
            return;
        }
        if (mAllocOp.chunkId > 0 && ! mWriteIds.empty()) {
            ReserveSpace();
        } else {
            Reset();
            AllocateChunk();
        }
    }
    void Lookup()
    {
        mCurOpPtr = &mLookupOp; // For HandleError() below to work.
        const bool theStartFlag = mPathNamePos == 0;
        if (theStartFlag) {
            mFileName.clear();
            mCreateOp.status = 0;
        } else if (mFileName.empty()) {
            mLookupOp.status = -ENOENT;
            HandleError();
            return;
        } else if (mLookupOp.status == -ENOENT && mMakeDirsFlag) {
            mLookupOp.status = 0;
            Mkdir();
            return;
        } else if (mLookupOp.status != 0) {
            HandleError();
            return;
        }
        kfsFileId_t const theParentFid = theStartFlag ?
            KFS::ROOTFID : mLookupOp.fattr.fileId;
        const string      theFileName  = mFileName;

        Reset(mLookupOp);
        mLookupOp.filename  = 0;
        mLookupOp.parentFid = theParentFid;
        StringPos       theNext      = string::npos;
        StringPos const theEnd       = mPathName.length();
        const char      theSeparator = '/';
        while (mPathNamePos < theEnd &&
                (theNext = mPathName.find(theSeparator, mPathNamePos)) !=
                    string::npos &&
                theNext == mPathNamePos) {
           mPathNamePos++;
        }
        if (theNext == string::npos) {
            theNext = theEnd;
        }
        if (mPathNamePos >= theEnd) {
            mFileName.clear();
        } else {
            mFileName = mPathName.substr(mPathNamePos, theNext - mPathNamePos);
        }
        if (theNext - mPathNamePos > KFS::MAX_FILENAME_LEN) {
            mLookupOp.status = -ENAMETOOLONG;
            HandleError();
            return;
        }
        mPathNamePos = theNext;
        if (theNext == theEnd) {
            if (! mFileName.empty()) {
                Create();
                return;
            }
            if (mCreateOp.status == -EEXIST && ! theFileName.empty()) {
                mCreateOp.status = 0;
                mFileName = theFileName;
                mLookupOp.fattr.isDirectory = true;
            }
        }
        if (! theStartFlag &&
                mLookupOp.fattr.isDirectory == mFileName.empty()) {
            mLookupOp.status = mFileName.empty() ? -ENOENT : -ENOTDIR;
            HandleError();
            return;
        }
        if (mFileName.empty()) {
            mOpenFlag    = true;
            mOpeningFlag = false;
            ReportCompletion();
            StartAppend();
            return;
        }
        mLookupOp.filename = mFileName.c_str();
        assert(*mLookupOp.filename);
        EnqueueMeta(mLookupOp);
    }
    void Done(
        LookupOp& inOp,
        IOBuffer* inBufferPtr)
    {
        assert(&mLookupOp == &inOp && ! inBufferPtr);
        Lookup();
    }
    void Mkdir()
    {
        assert(mLookupOp.parentFid > 0 && ! mFileName.empty());
        Reset(mMkdirOp);
        mMkdirOp.parentFid = mLookupOp.parentFid;
        mMkdirOp.dirname   = mLookupOp.filename;
        EnqueueMeta(mMkdirOp);
    }
    void Done(
        MkdirOp&  inOp,
        IOBuffer* inBufferPtr)
    {
        assert(&mMkdirOp == &inOp && ! inBufferPtr);
        if (inOp.status == -EEXIST) {
            // Just re-queue the lookup op, it should succeed now.
            assert(mLookupOp.parentFid == mMkdirOp.parentFid &&
                mMkdirOp.dirname == mLookupOp.filename);
            EnqueueMeta(mLookupOp);
            return;
        }
        if (inOp.status != 0) {
            mAllocOp.chunkId = 0;
            HandleError();
            return;
        }
        assert(mLookupOp.parentFid == mMkdirOp.parentFid);
        mLookupOp.fattr.fileId      = mMkdirOp.fileId;
        mLookupOp.fattr.isDirectory = true;
        mLookupOp.status            = 0;
        Lookup();
    }
    void Create()
    {
        assert(mLookupOp.parentFid > 0 && ! mFileName.empty());
        Reset(mCreateOp);
        mCreateOp.parentFid   = mLookupOp.parentFid;
        mCreateOp.filename    = mFileName.c_str();
        mCreateOp.numReplicas = mNumReplicas;
        // With false it deletes the file then creates it again.
        mCreateOp.exclusive   = true;
        EnqueueMeta(mCreateOp);
    }
    void Done(
        CreateOp& inOp,
        IOBuffer* inBufferPtr)
    {
        assert(&mCreateOp == &inOp && ! inBufferPtr);
        if (inOp.status == -EEXIST) {
            Lookup();
            return;
        }
        if (inOp.status != 0) {
            mAllocOp.chunkId = 0;
            HandleError();
            return;
        }
        mLookupOp.parentFid    = inOp.parentFid;
        mLookupOp.status       = inOp.status;
        mLookupOp.fattr.fileId = inOp.fileId;
        mOpenFlag    = true;
        mOpeningFlag = false;
        ReportCompletion();
        StartAppend();
    }
    void LookupPath()
    {
        Reset(mLookupPathOp);
        mLookupPathOp.rootFid  = KFS::ROOTFID;
        mLookupPathOp.filename = mPathName.c_str();
        assert(*mLookupPathOp.filename);
        EnqueueMeta(mLookupPathOp);
    }
    void Done(
        LookupPathOp& inOp,
        IOBuffer*     inBufferPtr)
    {
        assert(&mLookupPathOp == &inOp && ! inBufferPtr);
        if (inOp.status == KfsNetClient::kErrorMaxRetryReached) {
            HandleError();
            return;
        }
        if (inOp.status != 0 && mNumReplicas > 0) {
            KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                "lookup path failed: " << inOp.status <<
                " falling back to open" <<
            KFS_LOG_EOM;
            Lookup();
            return;
        }
        if (inOp.fattr.isDirectory) {
            inOp.status = -EISDIR;
            HandleError();
            return;
        }
        inOp.filename = ""; // Reset just in case.
        // Copy result into lookup op.
        mLookupOp.parentFid = -1; // Input, not known, and not needed.
        mLookupOp.status    = inOp.status;
        mLookupOp.fattr     = inOp.fattr;
        mOpenFlag    = true;
        mOpeningFlag = false;
        ReportCompletion();
        StartAppend();
    }
    void AllocateChunk()
    {
        assert(mLookupOp.fattr.fileId > 0);
        Reset(mAllocOp);
        mSpaceAvailable = 0;
        chunkOff_t theOffset;
        if (mSpaceReserveOp.status == -ENOSPC) {
            theOffset = (mAllocOp.fileOffset + KFS::CHUNKSIZE) /
                KFS::CHUNKSIZE * KFS::CHUNKSIZE;
            mSpaceReserveOp.status = 0;
        } else {
            theOffset = -1;
        }
        mAllocOp = AllocateOp(0, mLookupOp.fattr.fileId, mPathName);
        mAllocOp.append               = true;
        mAllocOp.chunkId              = 0;
        mAllocOp.fileOffset           = theOffset;
        mAllocOp.spaceReservationSize = max(
            mClosingFlag ? 0 : mDefaultSpaceReservationSize,
            mBuffer.BytesConsumable()
        );
        mAllocOp.maxAppendersPerChunk = mDefaultSpaceReservationSize > 0 ?
            (KFS::CHUNKSIZE / mDefaultSpaceReservationSize) : 64;
        mStats.mChunkAllocCount++;
        EnqueueMeta(mAllocOp);
    }
    void Done(
        AllocateOp& inOp,
        IOBuffer*   inBufferPtr)
    {
        assert(&mAllocOp == &inOp && ! inBufferPtr);
        if (inOp.status != 0 || mAllocOp.chunkServers.empty()) {
            mAllocOp.chunkId = 0;
            HandleError();
            return;
        }
        AllocateWriteId();
    }
    void CloseChunk()
    {
        assert(mAllocOp.chunkId > 0);
        Reset(mCloseOp);
        mCloseOp.chunkId   = mAllocOp.chunkId;
        mCloseOp.writeInfo = mWriteIds;
        if (mCloseOp.writeInfo.empty()) {
            mCloseOp.chunkServerLoc = mAllocOp.chunkServers;
        } else {
            mCloseOp.chunkServerLoc.clear();
        }
        Enqueue(mCloseOp);
    }
    void Done(
        CloseOp&  inOp,
        IOBuffer* inBufferPtr)
    {
        assert(&mCloseOp == &inOp && ! inBufferPtr);
        if (mCloseOp.status != 0) {
            KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                "chunk close failure, status: " << mCloseOp.status <<
                " ignored" <<
            KFS_LOG_EOM;
            StopChunkServer();
        }
        mCurOpPtr = 0;// Graceful close, do not reset chunk server's connection.
        Reset();
        StartAppend();
    }
    bool CanAppend()
    {
        return (
            ! mWriteQueue.empty() &&
            (mClosingFlag || mBuffer.BytesConsumable() >= mWriteThreshold)
        );
    }
    bool ReserveSpace(
        bool inCheckAppenderFlag = false)
    {
        assert(mAllocOp.chunkId > 0 && ! mWriteIds.empty());
        const int theSpaceNeeded = mWriteQueue.empty() ?
            ((mSpaceAvailable <= 0 && ! mClosingFlag) ?
                mDefaultSpaceReservationSize : 0) :
            mWriteQueue.front();
        if (! inCheckAppenderFlag && theSpaceNeeded <= mSpaceAvailable) {
            if (CanAppend()) {
                Append();
                return true;
            } else {
                return false; // Nothing to do.
            }
        }
        Reset(mSpaceReserveOp);
        mSpaceReserveOp.chunkId      = mAllocOp.chunkId;
        mSpaceReserveOp.chunkVersion = mAllocOp.chunkVersion,
        mSpaceReserveOp.writeInfo    = mWriteIds;
        mSpaceReserveOp.numBytes     = theSpaceNeeded <= mSpaceAvailable ?
            size_t(0) :
            size_t(max(
                mClosingFlag ? 0 : mDefaultSpaceReservationSize,
                max(theSpaceNeeded, min(
                    max(mPreferredAppendSize, mDefaultSpaceReservationSize),
                    mBuffer.BytesConsumable()))) -
                mSpaceAvailable
            );
        mStats.mReserveSpaceCount++;
        Enqueue(mSpaceReserveOp);
        return true;
    }
    void Done(
        ChunkSpaceReserveOp& inOp,
        IOBuffer*            inBufferPtr)
    {
        assert(&mSpaceReserveOp == &inOp && ! inBufferPtr);
        if (inOp.status != 0) {
            if (inOp.status == -ENOSPC) {
                mStats.mReserveSpaceDeniedCount++;
                if (mSpaceAvailable > 0) {
                    SpaceRelease();
                } else {
                    CloseChunk();
                }
                return;
            }
            HandleError();
            return;
        }
        mSpaceAvailable += inOp.numBytes;
        mLastAppendActivityTime = Now();
        StartAppend();
    }
    void AllocateWriteId()
    {
        assert(mAllocOp.chunkId > 0 && ! mAllocOp.chunkServers.empty());
        Reset(mWriteIdAllocOp);
        mWriteIdAllocOp.chunkId           = mAllocOp.chunkId;
        mWriteIdAllocOp.chunkVersion      = mAllocOp.chunkVersion;
        mWriteIdAllocOp.isForRecordAppend = true;
        mWriteIdAllocOp.chunkServerLoc    = mAllocOp.chunkServers;
        mWriteIdAllocOp.offset            = 0;
        mWriteIdAllocOp.numBytes          = 0;
        if (mClientPoolPtr) {
            mChunkServerPtr = &mClientPoolPtr->Get(mAllocOp.chunkServers[0]);
        } else {
            mChunkServerPtr = 0;
            if (! mChunkServer.SetServer(mAllocOp.chunkServers[0])) {
                mCurOpPtr = &mWriteIdAllocOp;
                HandleError();
                return;
            }
        }
        Enqueue(mWriteIdAllocOp);
    }
    void Done(
        WriteIdAllocOp& inOp,
        IOBuffer*       inBufferPtr)
    {
        assert(&mWriteIdAllocOp == &inOp && ! inBufferPtr);
        mWriteIds.clear();
        if (inOp.status < 0) {
            HandleError();
            return;
        }
        const size_t theServerCount = inOp.chunkServerLoc.size();
        mWriteIds.reserve(theServerCount);
        istringstream theStream(inOp.writeIdStr);
        for (size_t i = 0; i <theServerCount; i++) {
            WriteInfo theWInfo;
            if (! (theStream >>
                    theWInfo.serverLoc.hostname >>
                    theWInfo.serverLoc.port >>
                    theWInfo.writeId)) {
                KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                    "write id alloc: invalid response: " << inOp.writeIdStr <<
                KFS_LOG_EOM;
                break;
            }
            mWriteIds.push_back(theWInfo);
        }
        if (theServerCount != mWriteIds.size()) {
            HandleError();
            return;
        }
        mPrevRecordAppendOpSeq = inOp.seq;
        if (! ReserveSpace()) {
            StartAppend();
        }
    }
    void Append()
    {
        while (! mWriteQueue.empty() && mWriteQueue.front() <= 0) {
            assert(! "invalid write queue");
            mWriteQueue.pop_front();
        }
        if (mWriteQueue.empty()) {
            assert(mBuffer.IsEmpty());
            StartAppend(); // Nothing to append yet.
            return;
        }
        bool theCheckAppenderFlag = false;
        if (mWriteQueue.front() > mSpaceAvailable ||
                (theCheckAppenderFlag = mLastAppendActivityTime +
                    kAppendInactivityCheckTimeout <= Now())) {
            const bool theOpQueuedFlag = ReserveSpace(theCheckAppenderFlag);
            QCRTASSERT(theOpQueuedFlag);
            return;
        }
        const int theTotal               = mBuffer.BytesConsumable();
        const int thePreferredAppendSize = min(mSpaceAvailable,
            (mPreferredAppendSize < theTotal &&
            (theTotal >> 1) < mPreferredAppendSize &&
            theTotal - mPreferredAppendSize >= mWriteThreshold) ?
            theTotal : mPreferredAppendSize
        );
        int theSum;
        while (mWriteQueue.size() > 1 &&
                (theSum = mWriteQueue[0] + mWriteQueue[1]) <=
                    thePreferredAppendSize) {
            mWriteQueue.pop_front();
            mWriteQueue.front() = theSum;
        }
        mAppendLength = mWriteQueue.front();
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "append: "          << mAppendLength <<
            " pending: queue: " << mWriteQueue.size() <<
            " bytes: "          << theTotal <<
            " wthresh: "        << mWriteThreshold <<
        KFS_LOG_EOM;
        assert(mBuffer.BytesConsumable() >= mAppendLength);
        Reset(mRecAppendOp);
        mRecAppendOp.chunkId       = mAllocOp.chunkId;
        mRecAppendOp.chunkVersion  = mAllocOp.chunkVersion;
        mRecAppendOp.offset        = -1; // Let chunk server pick offset.
        mRecAppendOp.writeInfo     = mWriteIds;
        mRecAppendOp.contentLength = size_t(mAppendLength);
        mRecAppendOp.checksum      =
            ComputeBlockChecksum(&mBuffer, mAppendLength);
        mStats.mOpsRecAppendCount++;
        Enqueue(mRecAppendOp, &mBuffer);
    }
    void Done(
        RecordAppendOp& inOp,
        IOBuffer*       inBufferPtr,
        bool            inResetFlag = false)
    {
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "append done: "     <<
                (mWriteQueue.empty() ? -1 : mWriteQueue.front()) <<
            " pending: queue: " << mWriteQueue.size() <<
            " bytes: "          << mBuffer.BytesConsumable() <<
            " wthresh: "        << mWriteThreshold <<
        KFS_LOG_EOM;
        assert(&mRecAppendOp == &inOp && inBufferPtr == &mBuffer &&
            ! mWriteQueue.empty());
        if (inOp.status != 0 || mWriteQueue.empty()) {
            HandleError();
            return;
        }
        const int theConsumed = mBuffer.Consume(mAppendLength);
        QCRTASSERT(mAppendLength > 0 && theConsumed == mAppendLength &&
                mSpaceAvailable >= mAppendLength);
        mSpaceAvailable -= mAppendLength;
        // The queue can change in the case if it had only one record when
        // append started, and then the next record arrived and the two
        // (short) records were coalesced into one.
        while (mAppendLength > 0) {
            assert(! mWriteQueue.empty());
            int& theLen = mWriteQueue.front();
            if (mAppendLength >= theLen) {
                mAppendLength -= theLen;
                mWriteQueue.pop_front();
            } else {
                theLen -= mAppendLength;
                mAppendLength = 0;
            }
        }
        mLastAppendActivityTime = Now();
        mPrevRecordAppendOpSeq  = inOp.seq;
        mStats.mAppendCount++;
        mStats.mAppendByteCount += theConsumed;
        ReportCompletion();
        if (inResetFlag || (mForcedAllocationInterval > 0 &&
                (mStats.mOpsRecAppendCount % mForcedAllocationInterval) == 0)) {
            Reset();
        }
        StartAppend();
    }
    void SpaceRelease()
    {
        if (mSpaceAvailable <= 0) {
            StartAppend();
            return;
        }
        Reset(mSpaceReleaseOp);
        mSpaceReleaseOp.chunkId      = mAllocOp.chunkId;
        mSpaceReleaseOp.chunkVersion = mAllocOp.chunkVersion,
        mSpaceReleaseOp.writeInfo    = mWriteIds;
        mSpaceReleaseOp.numBytes     = size_t(mSpaceAvailable);
        Enqueue(mSpaceReleaseOp);
    }
    void Done(
        ChunkSpaceReleaseOp& inOp,
        IOBuffer*            inBufferPtr)
    {
        assert(&mSpaceReleaseOp == &inOp && ! inBufferPtr);
        if (inOp.status != 0) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "space release error: " << inOp.status <<
                " msg: " << inOp.statusMsg <<
                " ignored; op: " <<
                inOp.Show() <<
            KFS_LOG_EOM;
            Reset();
            // HandleError();
            // return;
        } else {
            assert(size_t(mSpaceAvailable) == mSpaceReleaseOp.numBytes);
            mSpaceAvailable = 0;
        }
        StartAppend();
    }
    void GetLastRecordAppendOpStatus()
    {
        const unsigned int theIndex = mGetRecordAppendOpStatusIndex;
        assert(theIndex >= 0 && theIndex < mWriteIds.size());
        Reset(mGetRecordAppendOpStatusOp);
        mGetRecordAppendOpStatusOp.chunkId = mAllocOp.chunkId;
        mGetRecordAppendOpStatusOp.writeId = mWriteIds[theIndex].writeId;
        assert(mChunkServer.GetMaxRetryCount() <= 1);
        // <= 0 -- infinite timeout
        // For record append status always use separate / dedicated connection.
        mChunkServerPtr = 0;
        mChunkServer.SetOpTimeoutSec(
            max(int(kGetStatusOpMinTime), mOpTimeoutSec / 8));
        mChunkServer.SetServer(mWriteIds[theIndex].serverLoc);
        Enqueue(mGetRecordAppendOpStatusOp);
    }
    void Done(
        GetRecordAppendOpStatus& inOp,
        IOBuffer*                inBufferPtr)
    {
        assert(
            &mGetRecordAppendOpStatusOp == &inOp &&
            ! inBufferPtr &&
            mGetRecordAppendOpStatusIndex < mWriteIds.size()
        );
        if (inOp.status != 0) {
            KFS_LOG_STREAM_ERROR  << mLogPrefix <<
                "operation"
                " failure, seq: " << inOp.seq       <<
                " status: "       << inOp.status    <<
                " msg: "          << inOp.statusMsg <<
                " chunk server: " << GetServerLocation()  <<
                " op: "           << inOp.Show()    <<
            KFS_LOG_EOM;
        }
        // Restore chunk server settings.
        mChunkServer.SetOpTimeoutSec(mOpTimeoutSec);
        if (inOp.status != 0) {
            // If he doesn't know about this chunk and write id, then it is
            // possible that he has restarted, or protocol state got purged.
            // Do not waste time retrying in case of network errors, the
            // protocol state might get purged.
            // Move to the next chunk server.
            if (++mGetRecordAppendOpStatusIndex < mWriteIds.size()) {
                GetLastRecordAppendOpStatus();
            } else {
                // Tried all servers.
                // Use normal retry mecanism to schedule another round of
                // status recovery.
                mCurOpPtr = &mRecAppendOp;
                const bool kResetFlag = true;
                Done(mRecAppendOp, &mBuffer, kResetFlag);
            }
            return;
        }
        KFS_LOG_STREAM_INFO << mLogPrefix <<
            "record append seq:"
            " prev: " << mPrevRecordAppendOpSeq <<
            " cur: "  << mRecAppendOp.seq << 
            " recovered last record append status: " <<
                inOp.Show() <<
        KFS_LOG_EOM;
        if (inOp.opSeq != mRecAppendOp.seq &&
                inOp.opSeq != mPrevRecordAppendOpSeq) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                " status op: unexpected sequence number: "
                " got: "      << inOp.opSeq <<
                " expected: " << mPrevRecordAppendOpSeq <<
                " or " << mRecAppendOp.seq <<
            KFS_LOG_EOM;
            FatalError(-EINVAL);
            return;
        }
        if (inOp.chunkVersion != mAllocOp.chunkVersion) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                " status op: chunk version mismatch: "
                " got: "      << inOp.chunkVersion <<
                " expected: " << mAllocOp.chunkVersion <<
            KFS_LOG_EOM;
            FatalError(-EINVAL);
            return;
        }
        const int theStatus = inOp.opSeq == mRecAppendOp.seq ?
            inOp.opStatus : (inOp.widReadOnlyFlag ? -EFAULT : -EAGAIN);
        if (theStatus == -EAGAIN) {
            if (mRetryCount > 1 &&
                    ++mGetRecordAppendOpStatusIndex < mWriteIds.size()) {
                KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                    "server: " <<
                        GetServerLocation() <<
                    " status \"in progress\", trying next server" <<
                KFS_LOG_EOM;
                // If this is *not* the first recovery round, try to find
                // the server that hasn't received the append in question.
                //
                // The only reason not to do this for the first recovery
                // round is to prevent short timeout to cause problems by
                // failing replications down the replication chain, in the
                // case when replication is still in flight, but hasn't
                // reached or hasn't been processed yet by the "downstream"
                // participants.
                GetLastRecordAppendOpStatus();
                return;
            }
        } else if (mRetryCount == mMaxRetryCount && mRetryCount > 0) {
            // Give one more chance to do append seq. without a failure.
            mRetryCount--;
        }
        mRecAppendOp.status = theStatus;
        mCurOpPtr = &mRecAppendOp;
        const bool kResetFlag = true;
        Done(mRecAppendOp, &mBuffer, kResetFlag);
    }
    void Enqueue(
        KfsOp&    inOp,
        IOBuffer* inBufferPtr = 0)
        { EnqueueSelf(inOp, inBufferPtr, false); }
    void EnqueueMeta(
        KfsOp&    inOp,
        IOBuffer* inBufferPtr = 0)
        { EnqueueSelf(inOp, inBufferPtr, true); }
    time_t Now() const
        { return mNetManager.Now(); }
    KfsNetClient& GetChunkServer()
        { return (mChunkServerPtr ? *mChunkServerPtr : mChunkServer); }
    const KfsNetClient& GetChunkServer() const
        { return (mChunkServerPtr ? *mChunkServerPtr : mChunkServer); }
    void EnqueueSelf(
        KfsOp&    inOp,
        IOBuffer* inBufferPtr,
        bool      inMetaOpFlag)
    {
        mCurOpPtr    = &inOp;
        mOpStartTime = Now();
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "+> " << (inMetaOpFlag ? "meta" : "" ) <<
            " " << inOp.Show() <<
            " buffer: " << (void*)inBufferPtr <<
            "/" << (inBufferPtr ? inBufferPtr->BytesConsumable() : 0) <<
        KFS_LOG_EOM;
        if (inMetaOpFlag) {
            mStats.mMetaOpsQueuedCount++;
            if (! mMetaServer.Enqueue(&inOp, this, inBufferPtr)) {
                inOp.status = kErrorMetaEnqueue;
                HandleEnqueueError();
            }
        } else if (! (GetChunkServer().Enqueue(&inOp, this, inBufferPtr))) {
            inOp.status = kErrorChunkEnqueue;
            HandleEnqueueError();
        }
    }
    void Reset(
        KfsOp& inOp)
    {
        inOp.seq           = 0;
        inOp.status        = 0;
        inOp.statusMsg.clear();
        inOp.checksum      = 0;
        inOp.contentLength = 0;
        inOp.contentBufLen = 0;
        delete [] inOp.contentBuf;
        inOp.contentBuf    = 0;
    }
    void Reset()
    {
        if (mCurOpPtr) {
            StopChunkServer();
            mMetaServer.Cancel(mCurOpPtr, this);
        }
        Reset(mAllocOp);
        mWriteIds.clear();
        assert(mSpaceAvailable >= 0);
        mSpaceAvailable  = 0;
        mAllocOp.chunkId = 0;
        mCurOpPtr        = 0;
        mAppendLength    = 0;
    }
    void HandleEnqueueError()
        { HandleError(true); }
    int GetTimeToNextRetry(
        int inTimeSecBetweenRetries) const
    {
        return max(0, inTimeSecBetweenRetries - int(Now() - mOpStartTime));
    }
    void HandleError(
        bool inEnqueueErrorFlag = false)
    {
        if (mCurOpPtr) {
            ostringstream theOStream;
            mCurOpPtr->Request(theOStream);
            KFS_LOG_STREAM_ERROR          << mLogPrefix           <<
                "operation" << (inEnqueueErrorFlag ? " enqueue" : "") <<
                " failure, seq: "         << mCurOpPtr->seq       <<
                " status: "               << mCurOpPtr->status    <<
                " msg: "                  << mCurOpPtr->statusMsg <<
                " op: "                   << mCurOpPtr->Show()    <<
                " current chunk server: " << GetServerLocation()  <<
                " chunkserver: "          << (GetChunkServer().IsDataSent() ?
                    (GetChunkServer().IsAllDataSent() ? "all" : "partial") :
                    "no") << " data sent" <<
                "\nRequest:\n"            << theOStream.str()     <<
            KFS_LOG_EOM;
        } else {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "NULL operation " <<
                (inEnqueueErrorFlag ? "enqueue" : "") << " failure" <<
            KFS_LOG_EOM;
        }
        if (! (mErrorCode = mCurOpPtr ? mCurOpPtr->status : -1)) {
            mErrorCode = -1;
        }
        // Meta operations are automatically retried by MetaServer.
        // Declare fatal error in the case of meta op failure.
        if (&mLookupOp == mCurOpPtr || &mCreateOp == mCurOpPtr ||
                &mMkdirOp == mCurOpPtr || &mLookupPathOp == mCurOpPtr) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "meta operation failed, giving up" <<
            KFS_LOG_EOM;
        } else if (mRetryCount >= mMaxRetryCount) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "max retry reached: " << mRetryCount << ", giving up" <<
            KFS_LOG_EOM;
        } else if (! mOpenFlag) {
             KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "appender closed, giving up" <<
            KFS_LOG_EOM;
        } else if (&mRecAppendOp == mCurOpPtr && (
                (mRecAppendOp.status == KfsNetClient::kErrorMaxRetryReached &&
                   (mClientPoolPtr || GetChunkServer().IsAllDataSent())
                ) ||
                mRecAppendOp.status == -EAGAIN)
            ) {
            mRetryCount++;
            mErrorCode = 0;
            mGetRecordAppendOpStatusIndex = 0;
            if (mRecAppendOp.status == -EAGAIN) {
                const int theTimeToNextRetry = GetTimeToNextRetry(
                    min(4, mRetryCount - 1) * kAgainRetryMinTime +
                    max(int(kAgainRetryMinTime), mTimeSecBetweenRetries)
                );
                KFS_LOG_STREAM_ERROR << mLogPrefix <<
                    "record append operation status unknown,"
                    " schedule to get status in " <<
                        theTimeToNextRetry << " sec" <<
                KFS_LOG_EOM;
                mCurOpPtr = &mGetRecordAppendOpStatusOp;
                Sleep(theTimeToNextRetry);
            } else {
                // From now on for recovery purposes threat as undefined status:
                // mChunkServer.IsAllDataSent() during the recovery corresponds
                // to the "get op status", instead of "record append", and from
                // now on the retry timeout needs to be enforced.
                // For debugging set status message accordingly:
                mRecAppendOp.statusMsg = "all data sent, but no ack received";
                mRecAppendOp.status    = -EAGAIN;
            }
            if (! mSleepingFlag) {
                KFS_LOG_STREAM_ERROR << mLogPrefix <<
                    "record append operation failed to receive ack,"
                    " trying to get status" <<
                KFS_LOG_EOM;
                GetLastRecordAppendOpStatus();
            }
            return;
        } else {
            mStats.mRetriesCount++;
            mRetryCount++;
            int theTimeToNextRetry = GetTimeToNextRetry(mTimeSecBetweenRetries);
            // Treat alloc failure the same as chunk server failure.
            if (&mAllocOp == mCurOpPtr) {
                mStats.mAllocRetriesCount++;
            } else if (&mWriteIdAllocOp == mCurOpPtr ||
                        &mRecAppendOp == mCurOpPtr ||
                        (&mSpaceReserveOp == mCurOpPtr &&
                            mSpaceReserveOp.status != -ENOSPC)) {
                if (++mAppendRestartRetryCount == 2 ||
                        mAppendRestartRetryCount == 5 ||
                        mAppendRestartRetryCount == 15) {
                    // When write id or append fails the second, fifth, and
                    // fifteen times tell meta server to allocate new chunk to
                    // paper over bugs, and network connectivity problems by
                    // pretending that space reservation have failed.
                    KFS_LOG_STREAM_INFO <<
                        "force new chunk allocation"
                        " retry: " << mAppendRestartRetryCount <<
                    KFS_LOG_EOM;
                    mSpaceReserveOp.status = -ENOSPC;
                    theTimeToNextRetry = 0;
                } else if (mAppendRestartRetryCount <= 1) {
                    theTimeToNextRetry = 0;
                }
            }
            // Retry.
            KFS_LOG_STREAM_INFO << mLogPrefix <<
                "scheduling retry: " << mRetryCount <<
                " of " << mMaxRetryCount <<
                " in " << theTimeToNextRetry << " sec." <<
                " op: " <<
                (mCurOpPtr ? mCurOpPtr->Show() : string("NULL")) <<
            KFS_LOG_EOM;
            mErrorCode = 0;
            if (&mGetRecordAppendOpStatusOp != mCurOpPtr) {
                Reset();
            }
            Sleep(theTimeToNextRetry);
            if (! mSleepingFlag) {
               Timeout();
            }
            return;
        }
        FatalError();
    }
    void FatalError(
        int inErrorCode = 0)
    {
        if (inErrorCode != 0) {
            mErrorCode = inErrorCode;
        }
        if (mErrorCode == 0) {
            mErrorCode = -1;
        }
        mOpenFlag    = false;
        mOpeningFlag = false;
        mClosingFlag = false;
        mCurOpPtr    = 0;
        ReportCompletion();
    }
    void HandleCancel()
    {
        if (&mAllocOp == mCurOpPtr ||
                &mLookupOp == mCurOpPtr ||
                &mCreateOp == mCurOpPtr) {
            mStats.mMetaOpsCancelledCount++;
        }
        if (! mCurOpPtr) {
            KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                "NULL operation canceled" <<
            KFS_LOG_EOM;
        } else {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "operation canceled " << mCurOpPtr->Show() <<
            KFS_LOG_EOM;
        }
        mCurOpPtr  = 0;
        mErrorCode = kErrorOpCanceled;
    }
    void ReportCompletion()
    {
        if (mErrorCode == 0) {
            // Reset retry counts on successful completion.
            mRetryCount = 0;
            mAppendRestartRetryCount = 0;
        }
        if (mCompletionPtr) {
            mCompletionPtr->Done(mOuter, mErrorCode);
        }
    }
    bool Sleep(int inSec)
    {
        if (inSec <= 0 || mSleepingFlag) {
            return false;
        }
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "sleeping: "        << inSec <<
            " append: "         << mWriteQueue.front() <<
            " pending: queue: " << mWriteQueue.size() <<
            " bytes: "          << mBuffer.BytesConsumable() <<
            " cur op: "         <<
                (mCurOpPtr ? mCurOpPtr->Show() : string("none")) <<
        KFS_LOG_EOM;
        mSleepingFlag = true;
        mStats.mSleepTimeSec += inSec;
        const bool kResetTimerFlag = true;
        SetTimeoutInterval(inSec * 1000, kResetTimerFlag);
        mNetManager.RegisterTimeoutHandler(this);
        return true;
    }
    virtual void Timeout()
    {
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "timeout: "
            " append: "         << mWriteQueue.front() <<
            " pending: queue: " << mWriteQueue.size() <<
            " bytes: "          << mBuffer.BytesConsumable() <<
            " cur op: "         <<
                (mCurOpPtr ? mCurOpPtr->Show() : string("none")) <<
        KFS_LOG_EOM;
        if (mSleepingFlag) {
            mNetManager.UnRegisterTimeoutHandler(this);
            mSleepingFlag = false;
        }
        if (&mGetRecordAppendOpStatusOp == mCurOpPtr) {
            GetLastRecordAppendOpStatus();
        } else {
            StartAppend();
        }
    }
private:
    Impl(
        const Impl& inAppender);
    Impl& operator=(
        const Impl& inAppender);
};

WriteAppender::WriteAppender(
    MetaServer& inMetaServer,
    Completion* inCompletionPtr               /* = 0 */,
    int         inMaxRetryCount               /* = 6 */,
    int         inWriteThreshold              /* = KFS::CHECKSUM_BLOCKSIZE */,
    int         inTimeSecBetweenRetries       /* = 15 */,
    int         inDefaultSpaceReservationSize /* = 1 << 20 */,
    int         inPreferredAppendSize         /* = KFS::CHECKSUM_BLOCKSIZE */,
    int         inMaxPartialBuffersCount      /* = 16 */,
    int         inOpTimeoutSec                /* = 30 */,
    int         inIdleTimeoutSec              /* = 5 * 30 */,
    const char* inLogPrefixPtr                /* = 0 */,
    int64_t     inChunkServerInitialSeqNum    /* = 1 */,
    bool        inPreAllocationFlag           /* = true */,
    ClientPool* inClientPoolPtr               /* = 0 */)
    : mImpl(*new WriteAppender::Impl(
        *this,
        inMetaServer,
        inCompletionPtr,
        inMaxRetryCount,
        inWriteThreshold,
        inTimeSecBetweenRetries,
        inDefaultSpaceReservationSize,
        inPreferredAppendSize,
        inMaxPartialBuffersCount,
        inOpTimeoutSec,
        inIdleTimeoutSec,
        inPreAllocationFlag,
        (inLogPrefixPtr && inLogPrefixPtr[0]) ?
            (inLogPrefixPtr + string(" ")) : string(),
        inChunkServerInitialSeqNum,
        inClientPoolPtr
    ))
{
}

/* virtual */
WriteAppender::~WriteAppender()
{
    delete &mImpl;
}

int
WriteAppender::Open(
    const char* inFileNamePtr,
    int         inNumReplicas  /* = 3 */,
    bool        inMakeDirsFlag /* = false */)
{
    return mImpl.Open(inFileNamePtr, inNumReplicas, inMakeDirsFlag);
}

int
WriteAppender::Open(
    kfsFileId_t inFileId,
    const char* inFileNamePtr)
{
    return mImpl.Open(inFileId, inFileNamePtr);
}

int
WriteAppender::Close()
{
    return mImpl.Close();
}

int
WriteAppender::Append(
    IOBuffer& inBuffer,
    int       inLength)
{
    return mImpl.Append(inBuffer, inLength);
}

void
WriteAppender::Shutdown()
{
    mImpl.Shutdown();
}

bool
WriteAppender::IsOpen() const
{
    return mImpl.IsOpen();
}

bool
WriteAppender::IsOpening() const
{
    return mImpl.IsOpening();
}

bool
WriteAppender::IsClosing() const
{
    return mImpl.IsClosing();
}

bool
WriteAppender::IsSleeping() const
{
    return mImpl.IsSleeping();
}

bool
WriteAppender::IsActive() const
{
    return mImpl.IsActive();
}

int
WriteAppender::GetPendingSize() const
{
    return mImpl.GetPendingSize();
}

int
WriteAppender::GetErrorCode() const
{
    return mImpl.GetErrorCode();
}

int
WriteAppender::SetWriteThreshold(
    int inThreshold)
{
    return mImpl.SetWriteThreshold(inThreshold);
}

void
WriteAppender::Register(
    Completion* inCompletionPtr)
{
    mImpl.Register(inCompletionPtr);
}

bool
WriteAppender::Unregister(
    Completion* inCompletionPtr)
{
    return mImpl.Unregister(inCompletionPtr);
}

void
WriteAppender::GetStats(
    Stats&               outStats,
    KfsNetClient::Stats& outChunkServersStats)
{
    mImpl.GetStats(outStats, outChunkServersStats);
}

string
WriteAppender::GetServerLocation() const
{
    return mImpl.GetServerLocation();
}

int
WriteAppender::SetPreAllocation(
    bool inFlag)
{
    return mImpl.SetPreAllocation(inFlag);
}

bool
WriteAppender::GetPreAllocation() const
{
    return mImpl.GetPreAllocation();
}

void
WriteAppender::SetForcedAllocationInterval(
    int inInterval)
{
    return mImpl.SetForcedAllocationInterval(inInterval);
}

}
}
