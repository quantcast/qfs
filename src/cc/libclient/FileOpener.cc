//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2010/06/25
// Author: Mike Ovsiannikov
//
// Copyright 2010 Quantcast Corp.
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

#include "FileOpener.h"

#include <sstream>
#include <algorithm>
#include <cerrno>
#include <sstream>

#include "kfsio/NetManager.h"
#include "common/kfsdecls.h"
#include "common/MsgLogger.h"
#include "KfsOps.h"
#include "KfsClient.h"

namespace KFS
{
namespace client
{

// File open / create state machine implementation.
class FileOpener::Impl : private KfsNetClient::OpOwner
{
public:
    Impl(
        FileOpener& inOuter,
        MetaServer& inMetaServer,
        Completion* inCompletionPtr,
        std::string inLogPrefix)
        : KfsNetClient::OpOwner(),
          mOuter(inOuter),
          mMetaServer(inMetaServer),
          mPathName(),
          mFileName(),
          mCanceledFlag(false),
          mOpenFlag(false),
          mOpeningFlag(false),
          mMakeDirsFlag(false),
          mErrorCode(0),
          mNumReplicas(0),
          mPathNamePos(0),
          mCurOpPtr(0),
          mCompletionPtr(inCompletionPtr),
          mLookupOp(0, 0, ""),
          mMkdirOp(0, 0, ""),
          mCreateOp(0, 0, "", mNumReplicas, false),
          mLookupPathOp(0, 0, ""),
          mLogPrefix(inLogPrefix),
          mStats()
        { Impl::Reset(); }
    ~Impl()
    {
        mMetaServer.Cancel(mCurOpPtr, this);
        Impl::Register(0);
    }
    int Open(
        const char* inFileNamePtr,
        int         inNumReplicas,
        bool        inMakeDirsFlag)
    {
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
        if (mOpeningFlag) {
            return -EAGAIN;
        }
        mStats.Clear();
        mOpeningFlag  = true;
        mNumReplicas  = inNumReplicas;
        mPathName     = inFileNamePtr;
        mErrorCode    = 0;
        mPathNamePos  = 0;
        mMakeDirsFlag = inMakeDirsFlag;
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
        if (mOpeningFlag) {
            return -EAGAIN;
        }
        mStats.Clear();
        mPathName     = inFileNamePtr;
        mErrorCode    = 0;
        mPathNamePos  = 0;
        mMakeDirsFlag = false;
        mNumReplicas  = 0; // Do not create if doesn't exist.
        mLookupOp.parentFid = -1;   // Input, not known, and not needed.
        mLookupOp.status    = 0;
        if (inFileId > 0) {
            mLookupOp.fattr.fileId      = inFileId;
            mLookupOp.fattr.isDirectory = false;
            mOpenFlag                   = true;
            mOpeningFlag                = false;
            ReportCompletion();
        } else {
            mOpeningFlag = true;
            LookupPath();
        }
        return mErrorCode;
    }
    void Shutdown()
    {
        Reset();
        mMetaServer.Cancel(mCurOpPtr, this);
        mOpeningFlag = false;
        mOpenFlag    = false;
        mErrorCode   = 0;
    }
    bool IsOpen() const
        { return (mOpenFlag); }
    bool IsOpening() const
        { return (! mOpenFlag && mOpeningFlag); }
    bool IsActive() const
        { return (mOpeningFlag); }
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
        Stats& outStats)
        { outStats = mStats; }
    bool GetErrorCode() const
        { return mErrorCode; }

protected:
    virtual void OpDone(
        KfsOp*    inOpPtr,
        bool      inCanceledFlag,
        IOBuffer* inBufferPtr)
    {
       if (mCurOpPtr != inOpPtr) {
            InternalError("invalid op completion");
        }
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "<- " << (inOpPtr ? inOpPtr->Show() : kKfsNullOp.Show()) <<
            (inCanceledFlag ? " canceled" : "") <<
            " buffer: " << (void*)inBufferPtr <<
            "/" << (inBufferPtr ? inBufferPtr->BytesConsumable() : 0) <<
            " status: " << inOpPtr->status <<
            " seq: " << inOpPtr->seq <<
        KFS_LOG_EOM;
        Dispatch(inOpPtr, inBufferPtr);
    }

private:
    typedef std::string::size_type StringPos;

    FileOpener&       mOuter;
    MetaServer&       mMetaServer;
    std::string       mPathName;
    std::string       mFileName;
    bool              mCanceledFlag;
    bool              mSleepingFlag;
    bool              mOpenFlag;
    bool              mOpeningFlag;
    bool              mMakeDirsFlag;
    int               mErrorCode;
    int               mNumReplicas;
    StringPos         mPathNamePos;
    KfsOp*            mCurOpPtr;
    Completion*       mCompletionPtr;
    LookupOp          mLookupOp;
    MkdirOp           mMkdirOp;
    CreateOp          mCreateOp;
    LookupPathOp      mLookupPathOp;
    std::string const mLogPrefix;
    Stats             mStats;


    void InternalError(
            const char* inMsgPtr = 0)
        { abort(); }

    void Dispatch(
        KfsOp*    inOpPtr,
        IOBuffer* inBufferPtr)
    {
        if (&mLookupOp == inOpPtr) {
            Done(mLookupOp, inBufferPtr);
        } else if (&mMkdirOp == inOpPtr) {
            Done(mMkdirOp, inBufferPtr);
        } else if (&mCreateOp == inOpPtr) {
            Done(mCreateOp, inBufferPtr);
        } else if (&mLookupPathOp == inOpPtr) {
            Done(mLookupPathOp, inBufferPtr);
        } else {
            InternalError("unknown operation dispatch");
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
        StringPos       theNext      = std::string::npos;
        StringPos const theEnd       = mPathName.length();
        const char      theSeparator = '/';
        while (mPathNamePos < theEnd &&
                (theNext = mPathName.find(theSeparator, mPathNamePos)) !=
                    std::string::npos &&
                theNext == mPathNamePos) {
           mPathNamePos++;
        }
        if (theNext == std::string::npos) {
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
            return;
        }
        mLookupOp.filename = mFileName.c_str();
        Enqueue(mLookupOp);
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
        Enqueue(mMkdirOp);
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
            Enqueue(mLookupOp);
            return;
        }
        if (inOp.status != 0) {
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
        Enqueue(mCreateOp);
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
            HandleError();
            return;
        }
        mLookupOp.parentFid    = inOp.parentFid;
        mLookupOp.status       = inOp.status;
        mLookupOp.fattr.fileId = inOp.fileId;
        mOpenFlag    = true;
        mOpeningFlag = false;
        ReportCompletion();
    }
    void LookupPath()
    {
        Reset(mLookupPathOp);
        mLookupPathOp.rootFid  = KFS::ROOTFID;
        mLookupPathOp.filename = mPathName.c_str();
        Enqueue(mLookupPathOp);
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
    }
    void Enqueue(
        KfsOp& inOp)
    {
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "+> " << "meta " << inOp.Show() <<
        KFS_LOG_EOM;
        mStats.mMetaOpsQueuedCount++;
        if (! mMetaServer.Enqueue(&inOp, this, 0)) {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "meta enqueu failure: " << inOp.Show() <<
            KFS_LOG_EOM;
            inOp.status = -EINVAL;
            OpDone(&inOp, false, 0);
        }
    }
    static void Reset(
        KfsOp& inOp)
    {
        inOp.seq           = 0;
        inOp.status        = 0;
        inOp.statusMsg.clear();
        inOp.checksum      = 0;
        inOp.contentLength = 0;
        inOp.DeallocContentBuf();
    }
    void Reset()
    {
        if (mCurOpPtr) {
            mMetaServer.Cancel(mCurOpPtr, this);
        }
        mCurOpPtr = 0;
    }
    void HandleError()
    {
        if (mCurOpPtr) {
            std::ostringstream theOStream;
            mCurOpPtr->Request(theOStream);
            KFS_LOG_STREAM_ERROR  << mLogPrefix           <<
                "operation"
                " failure, seq: " << mCurOpPtr->seq       <<
                " status: "       << mCurOpPtr->status    <<
                " msg: "          << mCurOpPtr->statusMsg <<
                " op: "           << mCurOpPtr->Show()    <<
                "\nRequest:\n"    << theOStream.str()     <<
            KFS_LOG_EOM;
        } else {
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "NULL operation failure" <<
            KFS_LOG_EOM;
        }
        if (! (mErrorCode = mCurOpPtr ? mCurOpPtr->status : -1)) {
            mErrorCode = -1;
        }
        // Meta operations are automatically retried by MetaServer.
        // Declare fatal error in the case of meta op failure.
        KFS_LOG_STREAM_ERROR << mLogPrefix <<
            "meta operation failed, giving up" <<
        KFS_LOG_EOM;
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
        mCurOpPtr    = 0;
        ReportCompletion();
    }
    void HandleCancel(
        KfsOp* inOpPtr)
    {
        mStats.mMetaOpsCancelledCount++;
        if (! mCurOpPtr) {
            KFS_LOG_STREAM_DEBUG << mLogPrefix <<
                "NULL operation canceled" <<
            KFS_LOG_EOM;
        }
        KFS_LOG_STREAM_ERROR << mLogPrefix <<
            "operation canceled " << inOpPtr->Show() <<
        KFS_LOG_EOM;
        mCurOpPtr  = 0;
        mErrorCode = -ECANCELED;
    }
    void ReportCompletion()
    {
        if (mCompletionPtr) {
            mCompletionPtr->Done(mOuter, mErrorCode);
        }
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

FileOpener::FileOpener(
    FileOpener::MetaServer& inMetaServer,
    FileOpener::Completion* inCompletionPtr /* = 0 */,
    const char*             inLogPrefixPtr  /* = 0 */)
    : mImpl(*new FileOpener::Impl(
        *this,
        inMetaServer,
        inCompletionPtr,
        (inLogPrefixPtr && inLogPrefixPtr[0]) ?
            (inLogPrefixPtr + std::string(" ")) : std::string()
    ))
{
}

/* virtual */
FileOpener::~FileOpener()
{
    delete &mImpl;
}

int
FileOpener::Open(
    const char* inFileNamePtr,
    int         inNumReplicas  /* = 3 */,
    bool        inMakeDirsFlag /* = false */)
{
    return mImpl.Open(inFileNamePtr, inNumReplicas, inMakeDirsFlag);
}

int
FileOpener::Open(
    kfsFileId_t inFileId,
    const char* inFileNamePtr)
{
    return mImpl.Open(inFileId, inFileNamePtr);
}

void
FileOpener::Shutdown()
{
    mImpl.Shutdown();
}

bool
FileOpener::IsOpen() const
{
    return mImpl.IsOpen();
}

bool
FileOpener::IsOpening() const
{
    return mImpl.IsOpening();
}

bool
FileOpener::IsActive() const
{
    return mImpl.IsActive();
}

    void
FileOpener::Register(
    FileOpener::Completion* inCompletionPtr)
{
    mImpl.Register(inCompletionPtr);
}

bool
FileOpener::Unregister(
    FileOpener::Completion* inCompletionPtr)
{
    return mImpl.Unregister(inCompletionPtr);
}

void
FileOpener::GetStats(
    FileOpener::Stats& outStats)
{
    mImpl.GetStats(outStats);
}

} // namespace client

} // namespace KFS
