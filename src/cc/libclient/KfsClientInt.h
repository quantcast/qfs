//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/04/18
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

#ifndef LIBKFSCLIENT_KFSCLIENTINT_H
#define LIBKFSCLIENT_KFSCLIENTINT_H

#include "common/MsgLogger.h"
#include "common/hsieh_hash.h"
#include "common/kfstypes.h"
#include "common/PoolAllocator.h"
#include "common/RequestParser.h"
#include "kfsio/TcpSocket.h"
#include "kfsio/checksum.h"
#include "qcdio/QCDLList.h"

#include "KfsAttr.h"
#include "KfsOps.h"
#include "KfsClient.h"
#include "Path.h"
#include "qcdio/QCMutex.h"

#include <string>
#include <vector>
#include <ostream>
#include <map>

namespace KFS {
namespace client {

using std::string;
using std::map;
using std::vector;
using std::pair;
using std::less;
using std::map;
using std::equal_to;
using std::less;
using std::ostream;
using std::streambuf;

/// If an op fails because the server crashed, retry the op.  This
/// constant defines the # of retries before declaring failure.
const int DEFAULT_NUM_RETRIES_PER_OP = 30;

/// Whenever an op fails, we need to give time for the server to
/// recover.  So, introduce a delay of 5 secs between retries.
const int RETRY_DELAY_SECS = 5;

/// Whenever we have issues with lease failures, we retry the op after 5 secs
const int LEASE_RETRY_DELAY_SECS = 5;

///
/// A KfsClient maintains a file-table that stores information about
/// KFS files on that client.  Each file in the file-table is composed
/// of some number of chunks; the meta-information about each
/// chunk is stored in a chunk-table associated with that file.  Thus, given a
/// <file-id, offset>, we can map it to the appropriate <chunk-id,
/// offset within the chunk>; we can also find where that piece of
/// data is located and appropriately access it.
///

class ReadRequest;
class ReadRequestCondVar;

///
/// \brief Read buffer class used with read ahead.
///
class ReadBuffer
{
public:
    ReadBuffer()
        : mStart(-1),
          mSize(0),
          mBufSize(0),
          mStatus(0),
          mAllocBuf(0),
          mBuf(0),
          mReadReq(0)
        {}
    ~ReadBuffer()
    {
        assert(! mReadReq);
        delete [] mAllocBuf;
    }
    void Invalidate()
        { mSize = 0; }
    char* GetBufPtr()
    {
        if (mReadReq) {
            return 0;
        }
        if (mBufSize > 0) {
            assert(mBuf);
            return mBuf;
        }
        mBufSize  = -mBufSize;
        delete [] mAllocBuf;
        mAllocBuf = 0;
        mBuf      = 0;
        mSize     = 0;
        mStatus   = 0;
        if (mBufSize <= 0) {
            return 0;
        }
        const int kAlign = 16;
        mAllocBuf = new char[mBufSize + kAlign];
        mBuf      = mAllocBuf + kAlign - (mAllocBuf - (char*)0) % kAlign;
        return mBuf;
    }
    void SetBufSize(int size)
    {
        if (GetBufSize() != size) {
            mBufSize = size < 0 ? 0 : -size;
        }
    }
    int GetBufSize() const
        { return (mBufSize < 0 ? -mBufSize : mBufSize); }
private:
    chunkOff_t   mStart;
    int          mSize;
    int          mBufSize;
    int          mStatus;
    char*        mAllocBuf;
    char*        mBuf;
    ReadRequest* mReadReq;

    friend class ReadRequest;

    char* DetachBuffer()
    {
        char* const ret = mAllocBuf;
        mStart    = -1;
        mBuf      = 0;
        mAllocBuf = 0;
        mSize     = 0;
        mStatus   = 0;
        mBufSize  = -GetBufSize();
        return ret;
    }
private:
    ReadBuffer(const ReadBuffer& buf);
    ReadBuffer& operator=(const ReadBuffer& buf);
};

class KfsClientImpl;

///
/// \brief Location of the file pointer in a file.
///
struct FilePosition {
    FilePosition()
        : fileOffset(0)
        {}
    void Reset() {
        fileOffset = 0;
    }
    chunkOff_t fileOffset; // offset within the file
};

///
/// \brief A table of entries that describe each open KFS file.
///
struct FileTableEntry {
    // the fid of the parent dir in which this entry "resides"
    kfsFileId_t          parentFid;
    // stores the name of the file/directory.
    string               name;
    // the full pathname
    string               pathname;
    // one of O_RDONLY, O_WRONLY, O_RDWR; when it is 0 for a file,
    // this entry is used for attribute caching
    int                  openMode;
    FileAttr             fattr;
    // the position in the file at which the next read/write will occur
    FilePosition         currPos;
    /// the user has set a marker beyond which reads should return EOF
    chunkOff_t           eofMark;

    bool                 skipHoles:1;
    bool                 usedProtocolWorkerFlag:1;
    bool                 readUsedProtocolWorkerFlag:1;
    bool                 cachedAttrFlag:1;
    bool                 failShortReadsFlag:1;
    unsigned int         instance;
    int64_t              pending;
    vector<KfsFileAttr>* dirEntries;
    int                  ioBufferSize;
    ReadBuffer           buffer;
    ReadRequest*         mReadQueue[1];

    FileTableEntry(kfsFileId_t p, const string& n, unsigned int instance):
        parentFid(p),
        name(n),
        pathname(),
        openMode(0),
        fattr(),
        currPos(),
        eofMark(-1),
        skipHoles(false),
        usedProtocolWorkerFlag(false),
        readUsedProtocolWorkerFlag(false),
        cachedAttrFlag(false),
        failShortReadsFlag(false),
        instance(instance),
        pending(0),
        dirEntries(0),
        ioBufferSize(0),
        buffer()
        { mReadQueue[0] = 0; }
    ~FileTableEntry()
    {
        delete dirEntries;
    }
};

class KfsProtocolWorker;
///
/// The kfs client implementation object.
///
class KfsClientImpl
{
public:
    typedef KfsClient::ErrorHandler ErrorHandler;

    KfsClientImpl();
    ~KfsClientImpl();

    ///
    /// @param[in] metaServerHost  Machine on meta is running
    /// @param[in] metaServerPort  Port at which we should connect to
    /// @retval 0 on success; -1 on failure
    ///
    int Init(const string &metaServerHost, int metaServerPort);

    ServerLocation GetMetaserverLocation() const {
        return mMetaServerLoc;
    }

    bool IsInitialized() { return mIsInitialized; };

    ///
    /// Provide a "cwd" like facility for KFS.
    /// @param[in] pathname  The pathname to change the "cwd" to
    /// @retval 0 on sucess; -errno otherwise
    ///
    int Cd(const char *pathname);

    /// Get cwd
    /// @retval a string that describes the current working dir.
    ///
    string GetCwd();

    int SetCwd(const char* pathname);

    ///
    /// Make a directory hierarcy in KFS.  If the parent dirs are not
    /// present, they are also made.
    /// @param[in] pathname The full pathname such as /.../dir
    /// @retval 0 if mkdir is successful; -errno otherwise
    int Mkdirs(const char *pathname, kfsMode_t mode);

    ///
    /// Make a directory in KFS.
    /// @param[in] pathname The full pathname such as /.../dir
    /// @retval 0 if mkdir is successful; -errno otherwise
    int Mkdir(const char *pathname, kfsMode_t mode);

    ///
    /// Remove a directory in KFS.
    /// @param[in] pathname The full pathname such as /.../dir
    /// @retval 0 if rmdir is successful; -errno otherwise
    int Rmdir(const char *pathname);

    ///
    /// Remove a directory hierarchy in KFS.
    /// @param[in] pathname The full pathname such as /.../dir
    /// @retval 0 if rmdir is successful; -errno otherwise
    int Rmdirs(const char *pathname, ErrorHandler* errHandler);

    int RmdirsFast(const char *pathname, ErrorHandler* errHandler);

    ///
    /// Read a directory's contents
    /// @param[in] pathname The full pathname such as /.../dir
    /// @param[out] result  The contents of the directory
    /// @retval 0 if readdir is successful; -errno otherwise
    int Readdir(const char *pathname, vector<string> &result);

    ///
    /// Read a directory's contents and retrieve the attributes
    /// @param[in] pathname The full pathname such as /.../dir
    /// @param[out] result  The files in the directory and their attributes.
    /// @param[in] computeFilesize  By default, compute file size
    /// @retval 0 if readdirplus is successful; -errno otherwise
    ///
    int ReaddirPlus(const char *pathname, vector<KfsFileAttr> &result,
                    bool computeFilesize = true);

    ///
    /// Read a directory's contents and retrieve the attributes
    /// @retval 0 if readdirplus is successful; -errno otherwise
    /// read() will retrieve directory entries in the form:
    /// 64 bit mod time
    /// 64 bit file size
    /// 32 bit file replication
    /// 32 bit file name length
    /// 8  bit directory flag
    /// file name: 8 bit times file name length
    ///
    int OpenDirectory(const char *pathname);

    ///
    /// Stat a file and get its attributes.
    /// @param[in] pathname The full pathname such as /.../foo
    /// @param[out] result  The attributes that we get back from server
    /// @param[in] computeFilesize  When set, for files, the size of
    /// file is computed and the value is returned in result.st_size
    /// @retval 0 if stat was successful; -errno otherwise
    ///
    int Stat(const char* pathname, KfsFileAttr& result, bool computeFilesize = true);
    int Stat(int fd, KfsFileAttr& result);

    ///
    /// Return the # of chunks in the file specified by the fully qualified pathname.
    /// -1 if there is an error.
    ///
    int GetNumChunks(const char *pathname);

    int UpdateFilesize(int fd);

    ///
    /// Helper APIs to check for the existence of (1) a path, (2) a
    /// file, and (3) a directory.
    /// @param[in] pathname The full pathname such as /.../foo
    /// @retval status: True if it exists; false otherwise
    ///
    bool Exists(const char *pathname);
    bool IsFile(const char *pathname);
    bool IsDirectory(const char *pathname);

    int EnumerateBlocks(const char* pathname, KfsClient::BlockInfos& res);

    int CompareChunkReplicas(const char *pathname, string &md5sum);

    /// API to verify that checksums on all replicas are the same.
    /// Each chunkserver scrubs the chunk and returns the adler-32
    /// checksum for its data.  After scrubbing on all chunkservers,
    /// we compare the checksums across replicas and return the result.
    /// @retval status code -- 0 OK, 1 mismatch < 0 -- error
    int VerifyDataChecksums(const char *pathname);
    int VerifyDataChecksums(int fd);

    ///
    /// Create a file which is specified by a complete path.
    /// @param[in] pathname that has to be created
    /// @param[in] numReplicas the desired degree of replication for
    /// the file.
    /// @param[in] exclusive  create will fail if the exists (O_EXCL flag)
    /// @retval on success, fd corresponding to the created file;
    /// -errno on failure.
    ///
    int Create(const char *pathname, int numReplicas = 3, bool exclusive = false,
        int numStripes = 0, int numRecoveryStripes = 0, int stripeSize = 0,
        int stripedType = KFS_STRIPED_FILE_TYPE_NONE, bool forceTypeFlag = true,
        kfsMode_t mode = kKfsModeUndef);

    ///
    /// Remove a file which is specified by a complete path.
    /// @param[in] pathname that has to be removed
    /// @retval status code
    ///
    int Remove(const char *pathname);

    ///
    /// Rename file/dir corresponding to oldpath to newpath
    /// @param[in] oldpath   path corresponding to the old name
    /// @param[in] newpath   path corresponding to the new name
    /// @param[in] overwrite  when set, overwrite the newpath if it
    /// exists; otherwise, the rename will fail if newpath exists
    /// @retval 0 on success; -1 on failure
    ///
    int Rename(const char *oldpath, const char *newpath, bool overwrite = true);

    int CoalesceBlocks(const char *srcPath, const char *dstPath, chunkOff_t *dstStartOffset);

    ///
    /// Set the mtime for a path
    /// @param[in] pathname  for which mtime has to be set
    /// @param[in] mtime     the desired mtime
    /// @retval status code
    ///
    int SetMtime(const char *pathname, const struct timeval &mtime);

    ///
    /// Open a file
    /// @param[in] pathname that has to be opened
    /// @param[in] openFlags modeled after open().  The specific set
    /// of flags currently supported are:
    /// O_CREAT, O_CREAT|O_EXCL, O_RDWR, O_RDONLY, O_WRONLY, O_TRUNC, O_APPEND
    /// @param[in] numReplicas if O_CREAT is specified, then this the
    /// desired degree of replication for the file
    /// @retval fd corresponding to the opened file; -errno on failure
    ///
    int Open(const char *pathname, int openFlags, int numReplicas,
        int numStripes, int numRecoveryStripes, int stripeSize, int stripedType,
        kfsMode_t mode);

    ///
    /// Close a file
    /// @param[in] fd that corresponds to a previously opened file
    /// table entry.
    ///
    int Close(int fd);

    ///
    /// Append a record to the chunk that we are writing to in the
    /// file with one caveat: the record should not straddle chunk
    /// boundaries.
    /// @param[in] fd that correpsonds to the file open for writing
    /// @param[in] buf the record that should be appended
    /// @param[in] reclen the length of the record
    /// @retval Status code
    ///
    int RecordAppend(int fd, const char *buf, int reclen);
    int AtomicRecordAppend(int fd, const char *buf, int reclen);

    /// See the comments in KfsClient.h
    int ReadPrefetch(int fd, char *buf, size_t numBytes);

    int WriteAsync(int fd, const char *buf, size_t numBytes);
    int WriteAsyncCompletionHandler(int fd);

    ///
    /// Read/write the desired # of bytes to the file, starting at the
    /// "current" position of the file.
    /// @param[in] fd that corresponds to a previously opened file
    /// table entry.
    /// @param buf For read, the buffer will be filled with data; for
    /// writes, this buffer supplies the data to be written out.
    /// @param[in] numBytes   The # of bytes of I/O to be done.
    /// @retval On success, return of bytes of I/O done (>= 0);
    /// on failure, return status code (< 0).
    ///
    ssize_t Read(int fd, char *buf, size_t numBytes, chunkOff_t* pos = 0);
    ssize_t Write(int fd, const char *buf, size_t numBytes, chunkOff_t* pos = 0);

    /// If there are any holes in a file, such as those at the end of
    /// a chunk, skip over them.
    void SkipHolesInFile(int fd);

    ///
    /// \brief Sync out data that has been written (to the "current" chunk).
    /// @param[in] fd that corresponds to a file that was previously
    /// opened for writing.
    ///
    int Sync(int fd);

    /// \brief Adjust the current position of the file pointer similar
    /// to the seek() system call.
    /// @param[in] fd that corresponds to a previously opened file
    /// @param[in] offset offset to which the pointer should be moved
    /// relative to whence.
    /// @param[in] whence one of SEEK_CUR, SEEK_SET, SEEK_END
    /// @retval On success, the offset to which the filer
    /// pointer was moved to; (chunkOff_t) -1 on failure.
    ///
    chunkOff_t Seek(int fd, chunkOff_t offset, int whence);
    /// In this version of seek, whence == SEEK_SET
    chunkOff_t Seek(int fd, chunkOff_t offset);

    /// Return the current position of the file pointer in the file.
    /// @param[in] fd that corresponds to a previously opened file
    /// @retval value returned is analogous to calling ftell()
    chunkOff_t Tell(int fd);

    ///
    /// Truncate a file to the specified offset.
    /// @param[in] fd that corresponds to a previously opened file
    /// @param[in] offset  the offset to which the file should be truncated
    /// @retval status code
    int Truncate(int fd, chunkOff_t offset);
    int Truncate(const char* pathname, chunkOff_t offset);

    ///
    /// Truncation, but going in the reverse direction: delete chunks
    /// from the beginning of the file to the specified offset
    /// @param[in] fd that corresponds to a previously opened file
    /// @param[in] offset  the offset before which the chunks should
    /// be deleted
    /// @retval status code
    int PruneFromHead(int fd, chunkOff_t offset);

    ///
    /// Given a starting offset/length, return the location of all the
    /// chunks that cover this region.  By location, we mean the name
    /// of the chunkserver that is hosting the chunk. This API can be
    /// used for job scheduling.
    ///
    /// @param[in] pathname   The full pathname of the file such as /../foo
    /// @param[in] start      The starting byte offset
    /// @param[in] len        The length in bytes that define the region
    /// @param[out] locations The location(s) of various chunks
    /// @retval status: 0 on success; -errno otherwise
    ///
    int GetDataLocation(const char *pathname, chunkOff_t start, chunkOff_t len,
                        vector<vector<string> > &locations);

    int GetDataLocation(int fd, chunkOff_t start, chunkOff_t len,
                        vector<vector<string> > &locations);

    ///
    /// Get the degree of replication for the pathname.
    /// @param[in] pathname The full pathname of the file such as /../foo
    /// @retval count
    ///
    int16_t GetReplicationFactor(const char *pathname);

    ///
    /// Set the degree of replication for the pathname.
    /// @param[in] pathname     The full pathname of the file such as /../foo
    /// @param[in] numReplicas  The desired degree of replication.
    /// @retval -1 on failure; on success, the # of replicas that will be made.
    ///
    int16_t SetReplicationFactor(const char *pathname, int16_t numReplicas);
    // Recursive version.
    int16_t SetReplicationFactorR(const char *pathname, int16_t numReplicas,
        ErrorHandler* errHandler = 0);

    void SetDefaultIOTimeout(int nsecs);
    int  GetDefaultIOTimeout() const;
    void SetRetryDelay(int nsecs);
    int  GetRetryDelay() const;
    void SetMaxRetryPerOp(int retryCount);
    int  GetMaxRetryPerOp() const;

    ssize_t SetDefaultIoBufferSize(size_t size);
    ssize_t GetDefaultIoBufferSize() const;
    ssize_t SetIoBufferSize(int fd, size_t size);
    ssize_t GetIoBufferSize(int fd) const;

    ssize_t SetDefaultReadAheadSize(size_t size);
    ssize_t GetDefaultReadAheadSize() const;
    ssize_t SetReadAheadSize(int fd, size_t size);
    ssize_t GetReadAheadSize(int fd) const;

    /// A read for an offset that is after the specified value will result in EOF
    void SetEOFMark(int fd, chunkOff_t offset);

    void SetMaxNumRetriesPerOp(int maxNumRetries);
    int GetFileOrChunkInfo(kfsFileId_t fileId, kfsChunkId_t chunkId,
        KfsFileAttr& fattr, chunkOff_t& offset, int64_t& chunkVersion,
        vector<ServerLocation>& servers);
    void SetDefaultFullSparseFileSupport(bool flag);
    // Must be invoked before issuing the first read.
    int SetFullSparseFileSupport(int fd, bool flag);
    void SetFileAttributeRevalidateTime(int secs);
    int Chmod(const char* pathname, kfsMode_t mode);
    int Chmod(int fd, kfsMode_t mode);
    int Chown(const char* pathname, kfsUid_t user, kfsGid_t group);
    int Chown(int fd, kfsUid_t user, kfsGid_t group);
    int Chown(const char* pathname, const char* user, const char* group);
    int Chown(int fd, const char* user, const char* group);
    int ChmodR(const char* pathname, kfsMode_t mode,
        ErrorHandler* errHandler);
    int ChownR(const char* pathname, kfsUid_t user, kfsGid_t group,
        ErrorHandler* errHandler);
    int ChownR(const char* pathname, const char* user, const char* group,
        ErrorHandler* errHandler);
    void SetUMask(kfsMode_t mask);
    kfsMode_t GetUMask();
    int SetEUserAndEGroup(kfsUid_t user, kfsGid_t group,
        kfsGid_t* groups, int groupsCnt);
    int GetUserAndGroupNames(kfsUid_t user, kfsGid_t group,
        string& uname, string& gname);
    int GetUserAndGroupIds(const char* user, const char* group,
        kfsUid_t& uid, kfsGid_t& gid)
        { return GetUserAndGroup(user, group, uid, gid); }
    kfsUid_t GetUserId();
    int GetReplication(const char* pathname,
        KfsFileAttr& attr, int& minChunkReplication, int& maxChunkReplication);

private:
     /// Maximum # of files a client can have open.
    enum { MAX_FILES = 128 << 10 };

    QCMutex mMutex;

    /// Seed to the random number generator
    bool    mIsInitialized;
    /// where is the meta server located
    ServerLocation mMetaServerLoc;

    /// a tcp socket that holds the connection with the server
    TcpSocket   mMetaServerSock;
    /// seq # that we send in each command
    kfsSeq_t    mCmdSeqNum;

    /// The current working directory in KFS
    string      mCwd;

    class FAttr;
    typedef map<
        string, FAttr*,
        less<string>,
        StdFastAllocator<pair<const string, FAttr*> >
    > NameToFAttrMap;
    typedef map<
        pair<kfsFileId_t, string>, FAttr*,
        less<pair<kfsFileId_t, string> >,
        StdFastAllocator<pair<const pair<kfsFileId_t, string>, FAttr*> >
    > FidNameToFAttrMap;
    class FAttr : public FileAttr
    {
    public:
        typedef QCDLList<FAttr, 0> List;
        FAttr(FAttr** list)
            : FileAttr(),
              validatedTime(0),
              generation(0),
              staleSubCountsFlag(false),
              fidNameIt(),
              nameIt()
        {
            List::Init(*this);
            List::PushBack(list, *this);
        }
        FAttr& operator=(const FileAttr& fa)
        {
            *static_cast<FileAttr*>(this) = fa;
            return *this;
        }
        time_t                      validatedTime;
        unsigned int                generation;
        bool                        staleSubCountsFlag;
        FidNameToFAttrMap::iterator fidNameIt;
        NameToFAttrMap::iterator    nameIt;
    private:
        FAttr* mPrevPtr[1];
        FAttr* mNextPtr[1];
        friend class QCDLListOp<FAttr, 0>;
    };
    typedef FAttr::List FAttrLru;

    /// keep a table of open files/directory handles.
    typedef vector<FileTableEntry*> FileTable;
    typedef PoolAllocator <
        sizeof(FAttr),
        1  << 20,
        32 << 20,
        true
    > FAttrPool;
    typedef vector<int>                        FreeFileTableEntires;
    typedef vector<pair<kfsFileId_t, size_t> > TmpPath;

    typedef map<kfsUid_t, pair<string, time_t>,
        less<kfsUid_t>,
        StdFastAllocator<pair<const kfsUid_t, pair<string, time_t> > >
    > UserNames;
    typedef map<kfsGid_t, pair<string, time_t>,
        less<kfsGid_t>,
        StdFastAllocator<pair<const kfsGid_t, pair<string, time_t> > >
    > GroupNames;
    typedef map<string, pair<kfsUid_t, time_t>,
        less<string>,
        StdFastAllocator<pair<const string, pair<kfsUid_t, time_t> > >
    > UserIds;
    typedef map<string, pair<kfsGid_t, time_t>,
        less<string>,
        StdFastAllocator<pair<const string, pair<kfsGid_t, time_t> > >
    > GroupIds;

    class BufferOutpuStream :
        private streambuf,
        public  ostream
    {
    public:
        BufferOutpuStream(char* ptr = 0, size_t len = 0)
            : streambuf(),
              ostream(this)
            { streambuf::setp(ptr, ptr + len); }
        ostream& Set(char* ptr = 0, size_t len = 0)
        {
            ostream::clear();
            ostream::flags(ostream::dec | ostream::skipws);
            ostream::precision(6);
            ostream::width(0);
            ostream::fill(' ');
            streambuf::setp(ptr, ptr + len);
            return *this;
        }
        size_t GetLength() const { return (pptr() - pbase()); }
    };

    FileTable                      mFileTable;
    FidNameToFAttrMap              mFidNameToFAttrMap;
    NameToFAttrMap                 mPathCache;
    NameToFAttrMap::iterator const mPathCacheNone;
    FAttrPool                      mFAttrPool;
    FAttr*                         mFAttrLru[1];
    FreeFileTableEntires           mFreeFileTableEntires;
    unsigned int                   mFattrCacheSkipValidateCnt;
    int                            mFileAttributeRevalidateTime;
    unsigned int                   mFAttrCacheGeneration;
    TmpPath                        mTmpPath;
    string                         mTmpAbsPathStr;
    Path                           mTmpAbsPath;
    string                         mTmpCurPath;
    string                         mTmpDirName;
    const string                   mSlash;
    size_t                         mDefaultIoBufferSize;
    size_t                         mDefaultReadAheadSize;
    bool                           mFailShortReadsFlag;
    unsigned int                   mFileInstance;
    KfsProtocolWorker*             mProtocolWorker;
    int                            mMaxNumRetriesPerOp;
    int                            mRetryDelaySec;
    int                            mDefaultOpTimeout;
    ReadRequestCondVar*            mFreeCondVarsHead;
    kfsUid_t                       mEUser;
    kfsGid_t                       mEGroup;
    kfsMode_t                      mUMask;
    vector<kfsGid_t>               mGroups;
    kfsSeq_t                       mCreateId;
    UserNames                      mUserNames;
    GroupNames                     mGroupNames;
    UserIds                        mUserIds;
    GroupIds                       mGroupIds;
    BufferInputStream              mTmpInputStream;
    BufferOutpuStream              mTmpOutputStream;
    const size_t                   mNameBufSize;
    char* const                    mNameBuf;
    KfsClientImpl*                 mPrevPtr[1];
    KfsClientImpl*                 mNextPtr[1];

    friend class QCDLListOp<KfsClientImpl, 0>;
    class ClientsList;
    friend class ClientsList;

    // Kfs client presently always allocated with new / malloc. Allocating large
    // buffer as part of the object should present no problem.
    enum { kTmpBufferSize = MAX_RPC_HEADER_LEN + 1 };
    char mTmpBuffer[kTmpBufferSize + 1];

    // Next sequence number for operations.
    // This is called in a thread safe manner.
    kfsSeq_t nextSeq()      { return mCmdSeqNum++; }
    kfsSeq_t NextCreateId() { return mCreateId++; }


    bool IsValid(const FAttr& fa, time_t now) const
    {
        return (fa.generation == mFAttrCacheGeneration &&
            now <= fa.validatedTime + mFileAttributeRevalidateTime);
    }

    void Shutdown();

    /// Check that fd is in range
    bool valid_fd(int fd) const {
        return (fd >= 0 && fd < MAX_FILES &&
            (size_t)fd < mFileTable.size() && mFileTable[fd]);
    }

    FAttr* NewFattr(kfsFileId_t parentFid, const string& name, const string& pathname);
    void Delete(FAttr* fa);
    bool Cache(time_t now, const string& dirname, kfsFileId_t dirFid,
        const KfsFileAttr& attr);
    int StatSelf(const char *pathname, KfsFileAttr &kfsattr, bool computeFilesize,
        string* path = 0, FAttr** fa = 0, bool validSubCountsRequiredFlag = false);
    int OpenSelf(const char *pathname, int openFlags, int numReplicas = 3,
        int numStripes = 0, int numRecoveryStripes = 0, int stripeSize = 0,
        int stripedType = KFS_STRIPED_FILE_TYPE_NONE, bool cacheAttributesFlag = false,
        kfsMode_t mode = kKfsModeUndef, string* path = 0);
    int CacheAttributes(const char* pathname);
    int GetDataLocationSelf(int fd, chunkOff_t start, chunkOff_t len,
                        vector<vector<string> > &locations);
    int TruncateSelf(int fd, chunkOff_t offset);
    int CreateSelf(const char *pathname, int numReplicas, bool exclusive,
        int numStripes, int numRecoveryStripes, int stripeSize, int stripedType,
        bool forceTypeFlag, kfsMode_t mode);
    ssize_t SetReadAheadSize(FileTableEntry& inEntry, size_t inSize, bool optimalFlag = false);
    ssize_t SetIoBufferSize(FileTableEntry& entry, size_t size, bool optimalFlag = false);
    ssize_t SetOptimalIoBufferSize(FileTableEntry& entry, size_t size) {
        return SetIoBufferSize(entry, size, true);
    }
    ssize_t SetOptimalReadAheadSize(FileTableEntry& entry, size_t size) {
        return SetReadAheadSize(entry, size, true);
    }


    /// Connect to the meta server and return status.
    /// @retval true if connect succeeds; false otherwise.
    bool ConnectToMetaServer();

    /// Lookup the attributes of a file given its parent file-id
    /// @param[in] parentFid  file-id of the parent directory
    /// @param[in] filename   filename whose attributes are being
    /// asked
    /// @param[out] result   the resultant attributes
    /// @param[in] computeFilesize  when set, for files, the size of
    /// the file is computed and returned in result.fileSize
    /// @retval 0 on success; -errno otherwise
    ///
    int LookupAttr(kfsFileId_t parentFid, const string& filename,
        FAttr*& result, bool computeFilesize, const string& path,
        bool validSubCountsRequiredFlag = false);

    FAttr* LookupFAttr(kfsFileId_t parentFid, const string& name);
    FAttr* LookupFAttr(const string& pathname, string* path);
    FAttr* NewFAttr(kfsFileId_t parentFid, const string& name,
        const string& pathname);

   /// Given a chunk, find out which chunk-server is hosting it.  It
    /// is possible that no server is hosting the chunk---if there is
    /// a hole in the file.
    /// @retval status code: 0 on success; < 0 => failure
    int LocateChunk(int fd, chunkOff_t chunkOffset, ChunkAttr& chunk);

    /// Given a kfsfid with some # of chunks, compute the size of the
    /// file.  This involves looking up the size of the last chunk of
    /// the file and then adding with the size of the remaining (full) chunks.
    chunkOff_t ComputeFilesize(kfsFileId_t kfsfid);

    /// Given the attributes for a set of files and the location info
    /// of the last chunk of each file, compute the filesizes for each file
    void ComputeFilesizes(vector<KfsFileAttr> &fattrs,
        const vector<ChunkAttr> &lastChunkInfo);

    /// Helper function: given a starting index to the two vectors,
    /// compute the file sizes for each file whose last chunk is
    /// stored in chunkserver at location loc.
    void ComputeFilesizes(vector<KfsFileAttr> &fattrs,
        const vector<ChunkAttr> &lastChunkInfo,
        size_t startIdx, const ServerLocation &loc);

    FileTableEntry* FdInfo(int fd) { return mFileTable[fd]; }
    FileAttr* FdAttr(int fd) { return &FdInfo(fd)->fattr; }

    /// Do the work for an op with the metaserver; if the metaserver
    /// dies in the middle, retry the op a few times before giving up.
    void DoMetaOpWithRetry(KfsOp *op);

    /// Get a response from the server, where, the response is
    /// terminated by "\r\n\r\n".
    int GetResponse(char *buf, int bufSize, int *delims, TcpSocket *sock);

    /// Validate file or directory name.
    int ValidateName(const string& name);

    /// Given a path, get the parent fileid and the name following the
    /// trailing "/"
    int GetPathComponents(const char *pathname, kfsFileId_t *parentFid,
        string &name, string* path = 0, bool invalidateSubCountsFlag = false,
        bool enforceLastDirFlag = true);

    /// File table management utilities: find a free entry in the
    /// table, find the entry corresponding to a pathname, "mark" an
    /// entry in the table as in use, and "mark" an entry in the table
    /// as free.
    int FindFreeFileTableEntry();

    bool IsFileTableEntryValid(int fte);


    /// Given a parent fid and name, get the corresponding entry in
    /// the file table.  Note: if needed, attributes will be
    /// downloaded from the server.
    int Lookup(kfsFileId_t parentFid, const string& name, FAttr*& fa, time_t now,
        const string& path);
    FAttr* LookupFattr(kfsFileId_t parentFid, const string& name);

    // name -- is the last component of the pathname
    int ClaimFileTableEntry(kfsFileId_t parentFid, const string& name, const string& pathname);
    int AllocFileTableEntry(kfsFileId_t parentFid, const string& name, const string& pathname);
    void ReleaseFileTableEntry(int fte);

    int GetDataChecksums(const ServerLocation &loc,
        kfsChunkId_t chunkId, uint32_t *checksums, bool readVerifyFlag = true);

    int VerifyDataChecksumsFid(kfsFileId_t fileId);

    int GetChunkFromReplica(const ServerLocation& loc, kfsChunkId_t chunkId,
            int64_t chunkVersion, ostream& os);

    int ReaddirPlus(const string& pathname, kfsFileId_t dirFid,
        vector<KfsFileAttr> &result,
        bool computeFilesize = true, bool updateClientCache = true);

    int Rmdirs(const string &parentDir, kfsFileId_t parentFid, const string &dirname, kfsFileId_t dirFid);
    int Remove(const string &parentDir, kfsFileId_t parentFid, const string &entryName);

    int ReadDirectory(int fd, char *buf, size_t bufSize);
    ssize_t Write(int fd, const char *buf, size_t numBytes,
        bool asyncFlag, bool appendOnlyFlag, chunkOff_t* pos = 0);
    void InitPendingRead(FileTableEntry& entry);
    void CancelPendingRead(FileTableEntry& entry);
    void CleanupPendingRead();
    int DoOpResponse(KfsOp *op, TcpSocket *sock);
    int DoOpCommon(KfsOp *op, TcpSocket *sock);
    int DoOpSend(KfsOp *op, TcpSocket *sock);
    int RmdirsSelf(const string& path, const string& dirname,
        kfsFileId_t parentFid, kfsFileId_t dirFid, ErrorHandler& errHandler);
    void StartProtocolWorker();
    void InvalidateAllCachedAttrs();
    int GetUserAndGroup(const char* user, const char* group, kfsUid_t& uid, kfsGid_t& gid);
    template<typename T> int RecursivelyApply(string& path, const KfsFileAttr& attr, T& functor);
    template<typename T> int RecursivelyApply(const char* pathname, T& functor);
    const string& UidToName(kfsUid_t uid, time_t now);
    const string& GidToName(kfsUid_t uid, time_t now);
    kfsUid_t NameToUid(const string& name, time_t now);
    kfsGid_t NameToGid(const string& name, time_t now);
    void InvalidateAttribute(const string& path,
        bool countsOnlyFlag, bool deleteAttrFlag);
    void InvalidateAttributeCounts(const string& path)
        { InvalidateAttribute(path, true, false); }
    void InvalidateAttributeAndCounts(const string& path)
        { InvalidateAttribute(path, true, true); }
    void ValidateFAttrCache(time_t now, int maxScan);
    void UpdatePath(KfsClientImpl::FAttr* fa, const string& path,
        bool copyPathFlag = true);
    const char* GetTmpAbsPath(const char* pathname, size_t& ioLen);

    friend struct RespondingServer;
    friend struct RespondingServer2;
    friend class ChmodFunc;
    friend class ChownFunc;
    friend class SetReplicationFactorFunc;
};

}}

#endif // LIBKFSCLIENT_KFSCLIENTINT_H
