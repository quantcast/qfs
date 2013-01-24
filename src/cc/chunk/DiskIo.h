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

#ifndef _DISKIO_H
#define _DISKIO_H

#include <inttypes.h>

#include <boost/shared_ptr.hpp>
#include <vector>

#include "kfsio/IOBuffer.h"
#include "qcdio/QCDiskQueue.h"
#include "qcdio/QCDLList.h"

namespace KFS
{
using std::string;
using std::vector;

class KfsCallbackObj;
class IOBuffer;
class DiskQueue;
class Properties;
class BufferManager;

// Asynchronous disk io shim.
// Creates and destroys low level disk queues. Runs io completion queue in the
// main event loop.
class DiskIo : private QCDiskQueue::IoCompletion
{
public:
    struct Counters
    {
        typedef int64_t Counter;

        Counter mReadCount;
        Counter mReadByteCount;
        Counter mReadErrorCount;
        Counter mWriteCount;
        Counter mWriteByteCount;
        Counter mWriteErrorCount;
        Counter mSyncCount;
        Counter mSyncErrorCount;
        Counter mDeleteCount;
        Counter mDeleteErrorCount;
        Counter mRenameCount;
        Counter mRenameErrorCount;
        Counter mGetFsSpaceAvailableCount;
        Counter mGetFsSpaceAvailableErrorCount;
        Counter mCheckDirReadableCount;
        Counter mCheckDirReadableErrorCount;
        Counter mTimedOutErrorCount;
        Counter mTimedOutErrorReadByteCount;
        Counter mTimedOutErrorWriteByteCount;
        Counter mOpenFilesCount;
        void Clear()
        {
            mReadCount                     = 0;   
            mReadByteCount                 = 0;   
            mReadErrorCount                = 0;   
            mWriteCount                    = 0;   
            mWriteByteCount                = 0;   
            mWriteErrorCount               = 0;   
            mSyncCount                     = 0;   
            mSyncErrorCount                = 0;   
            mDeleteCount                   = 0;   
            mDeleteErrorCount              = 0;   
            mRenameCount                   = 0;   
            mRenameErrorCount              = 0;   
            mGetFsSpaceAvailableCount      = 0;
            mGetFsSpaceAvailableErrorCount = 0;
            mCheckDirReadableCount         = 0;
            mCheckDirReadableErrorCount    = 0;
            mTimedOutErrorCount            = 0;
            mTimedOutErrorReadByteCount    = 0;
            mTimedOutErrorWriteByteCount   = 0;
            mOpenFilesCount                = 0;
        }
    };
    typedef int64_t Offset;
    typedef int64_t DeviceId;

    static bool Init(
        const Properties& inProperties,
        string*           inErrMessagePtr = 0);
    static bool StartIoQueue(
        const char* inDirNamePtr,
        DeviceId    inDeviceId,
        int         inMaxOpenFiles,
        string*     inErrMessagePtr = 0);
    static bool StopIoQueue(
        DiskQueue*  inDiskQueuePtr,
        const char* inDirNamePtr,
        DeviceId    inDeviceId,
        string*     inErrMessagePtr = 0);
    static bool Shutdown(
        string* inErrMessagePtr = 0);
    static bool RunIoCompletion();
    static size_t GetMaxRequestSize();
    static int GetFdCountPerFile();
    static BufferManager& GetBufferManager();
    static void GetCounters(
        Counters& outCounters);
    static bool Delete(
        const char*     inFileNamePtr,
        KfsCallbackObj* inCallbackObjPtr = 0,
        string*    inErrMessagePtr  = 0);
    static bool Rename(
        const char*     inSrcFileNamePtr,
        const char*     inDstFileNamePtr,
        KfsCallbackObj* inCallbackObjPtr = 0,
        string*         inErrMessagePtr  = 0);
    static bool GetFsSpaceAvailable(
        const char*     inPathNamePtr,
        KfsCallbackObj* inCallbackObjPtr = 0,
        string*         inErrMessagePtr  = 0);
    static bool CheckDirReadable(
        const char*     inDirNamePtr,
        KfsCallbackObj* inCallbackObjPtr = 0,
        string*         inErrMessagePtr  = 0);
    static bool GetDiskQueuePendingCount(
        DiskQueue* inDiskQueuePtr,
        int&       outFreeRequestCount,
        int&       outRequestCount,
        int64_t&   outReadBlockCount,
        int64_t&   outWriteBlockCount,
        int&       outBlockSize);
    static DiskQueue* FindDiskQueue(
        const char* inDirNamePtr);
    static void SetParameters(
        const Properties& inProperties);

    class File
    {
    public:
        File()
            : mQueuePtr(0),
              mFileIdx(-1),
              mReadOnlyFlag(false),
              mSpaceReservedFlag(false)
            {}
        ~File()
        {
            if (File::IsOpen()) {
                File::Close();
            }
        }
        bool Open(
            const char* inFileNamePtr,
            Offset      inMaxFileSize          = -1,
            bool        inReadOnlyFlag         = false,
            bool        inReserveFileSpaceFlag = false,
            bool        inCreateFlag           = false,
            string*     inErrMessagePtr        = 0,
            bool*       inRetryFlagPtr         = 0,
            bool        inBufferedIoFlag       = false);
        bool IsOpen() const
            { return (mFileIdx >= 0); }
        bool Close(
            Offset  inFileSize      = -1,
            string* inErrMessagePtr = 0);
        DiskQueue* GetDiskQueuePtr() const
            { return mQueuePtr; }
        int GetFileIdx() const
            { return mFileIdx; }
        bool IsReadOnly() const
            { return mReadOnlyFlag; }
        bool ReserveSpace(
            string* inErrMessagePtr = 0);
        void GetDiskQueuePendingCount(
            int&     outFreeRequestCount,
            int&     outRequestCount,
            int64_t& outReadBlockCount,
            int64_t& outWriteBlockCount,
            int&     outBlockSize);
    private:
        DiskQueue* mQueuePtr;
        int        mFileIdx;
        bool       mReadOnlyFlag:1;
        bool       mSpaceReservedFlag:1;

        void Reset();
        friend class DiskQueue;
    private:
        // No copies.
        File(const File&);
        File& operator=(const File&);
    };
    typedef boost::shared_ptr<File> FilePtr;

    DiskIo(
        FilePtr         inFilePtr,
        KfsCallbackObj* inCallbackObjPtr);

    ~DiskIo();

    /// Close disk queue. This will cause cancellation of all  scheduled
    /// requests.
    void Close();

    /// Schedule a read at the specified offset for numBytes.
    /// @param[in] numBytes # of bytes that need to be read.
    /// @param[in] offset offset in the file at which to start reading data from.
    /// @retval # of bytes for which read was successfully scheduled;
    /// -1 if there was an error. 
    ssize_t Read(
        Offset inOffset,
        size_t inNumBytes);

    /// Schedule a write.  
    /// @param[in] numBytes # of bytes that need to be written
    /// @param[in] offset offset in the file at which to start writing data.
    /// @param[in] buf IOBuffer which contains data that should be written
    /// out to disk.
    /// @retval # of bytes for which write was successfully scheduled;
    /// -1 if there was an error. 
    ssize_t Write(
        Offset    inOffset,
        size_t    inNumBytes,
        IOBuffer* inBufferPtr,
        bool      inSyncFlag = false);

    FilePtr GetFilePtr() const
        { return mFilePtr; }
private:
    typedef vector<IOBufferData> IoBuffers;
    /// Owning KfsCallbackObj.
    KfsCallbackObj* const  mCallbackObjPtr;
    FilePtr                mFilePtr;
    QCDiskQueue::RequestId mRequestId;
    IoBuffers              mIoBuffers;
    size_t                 mReadBufOffset;
    size_t                 mReadLength;
    int64_t                mBlockIdx;
    int64_t                mIoRetCode;
    time_t                 mEnqueueTime;
    bool                   mWriteSyncFlag;
    QCDiskQueue::RequestId mCompletionRequestId;
    QCDiskQueue::Error     mCompletionCode;
    DiskIo*                mPrevPtr[1];
    DiskIo*                mNextPtr[1];

    void RunCompletion();
    void IoCompletion(
        IOBuffer* inBufferPtr,
        int       inRetCode);
    virtual bool Done(
        QCDiskQueue::RequestId      inRequestId,
        QCDiskQueue::FileIdx        inFileIdx,
        QCDiskQueue::BlockIdx       inStartBlockIdx,
        QCDiskQueue::InputIterator& inBufferItr,
        int                         inBufferCount,
        QCDiskQueue::Error          inCompletionCode,
        int                         inSysErrorCode,
        int64_t                     inIoByteCount);

    enum MetaOpType
    {
        kMetaOpTypeNone                = 0,
        kMetaOpTypeDelete              = 1,
        kMetaOpTypeRename              = 2,
        kMetaOpTypeGetFsSpaceAvailable = 3,
        kMetaOpTypeCheckDirReadable    = 4,
        kMetaOpTypeNumOps
    };

    static bool EnqueueMeta(
        MetaOpType      inOpType,
        const char*     inSrcFileNamePtr,
        const char*     inDstFileNamePtr,
        KfsCallbackObj* inCallbackObjPtr,
        string*         inErrMessagePtr);

    friend class QCDLListOp<DiskIo, 0>;
    friend class DiskIoQueues;

private:
    // No copies.
    DiskIo(
        const DiskIo& inDiskIo);
    DiskIo& operator=(
        const DiskIo& inDiskIo);
};

typedef boost::shared_ptr<DiskIo> DiskIoPtr;

}

#endif /* _DISKIO_H */
