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
// \file KfsClient.h
// \brief Kfs Client-library code.
//
//----------------------------------------------------------------------------

#ifndef LIBKFSCLIENT_KFSCLIENT_H
#define LIBKFSCLIENT_KFSCLIENT_H

#include <string>
#include <vector>

#include "KfsAttr.h"

namespace KFS {
using std::string;
using std::vector;

namespace client {
class KfsClientImpl;
}

/// Maximum length of a filename
const size_t MAX_FILENAME_LEN = 256;

///
/// \brief The KfsClient is the "bridge" between applications and the
/// KFS servers (either the metaserver or chunkserver): there can be
/// only one client per metaserver.
///
/// The KfsClientFactory class can be used to produce KfsClient
/// objects, where each client is used to interface with a different
/// metaserver. The preferred method of creating a client object is
/// thru the client factory.
///


class KfsClient
{
public:
    typedef client::KfsClientImpl KfsClientImpl;

    class ErrorHandler
    {
    public:
        virtual int operator()(const string& path, int error) = 0;
    protected:
        ErrorHandler()  {}
        virtual ~ErrorHandler() {}
        ErrorHandler(const ErrorHandler&) {}
        ErrorHandler& operator=(const ErrorHandler&) { return *this; }
    };

    KfsClient();
    ~KfsClient();

    ///
    /// @param[in] metaServerHost  Machine on meta is running
    /// @param[in] metaServerPort  Port at which we should connect to
    /// @retval 0 on success; -1 on failure
    ///
    int Init(const string &metaServerHost, int metaServerPort);

    /// Set the logging level to control message verbosity
    void SetLogLevel(const string &level);

    bool IsInitialized();

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
    int Mkdirs(const char *pathname, kfsMode_t mode = 0777);

    ///
    /// Make a directory in KFS.
    /// @param[in] pathname The full pathname such as /.../dir
    /// @retval 0 if mkdir is successful; -errno otherwise
    int Mkdir(const char *pathname, kfsMode_t mode = 0777);

    ///
    /// Remove a directory in KFS.
    /// @param[in] pathname The full pathname such as /.../dir
    /// @retval 0 if rmdir is successful; -errno otherwise
    int Rmdir(const char *pathname);

    ///
    /// Remove a directory hierarchy in KFS.
    /// @param[in] pathname The full pathname such as /.../dir
    /// @retval 0 if rmdir is successful; -errno otherwise
    int Rmdirs(const char *pathname, ErrorHandler* errHandler = 0);

    int RmdirsFast(const char *pathname, ErrorHandler* errHandler = 0);

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
    /// Given a file, return the # of chunks in the file
    /// @param[in] pathname The full pathname such as /.../foo
    /// @retval    On success, # of chunks in the file; otherwise -1
    ///
    int GetNumChunks(const char *pathname);

    int GetChunkSize(const char *pathname) {
        return KFS::CHUNKSIZE;
    }

    /// Update the size of a file that has been opened.  It is likely
    /// that the file is shared between two clients, one or more
    /// writers, and a single reader.  The reader needs to update its
    /// view of the filesize so that it knows how much data there is.
    int UpdateFilesize(int fd);

    ///
    /// Helper APIs to check for the existence of (1) a path, (2) a
    /// file, and (3) a directory.
    /// Use Stat() if more than one property needs to be tested at the same
    /// time.
    /// @param[in] pathname The full pathname such as /.../foo
    /// @retval status: True if it exists; false otherwise
    ///
    bool Exists(const char *pathname);
    bool IsFile(const char *pathname);
    bool IsDirectory(const char *pathname);

    struct BlockInfo
    {
        chunkOff_t     offset;
        kfsChunkId_t   id;
        int64_t        version;
        ServerLocation server;
        chunkOff_t     size;

        BlockInfo()
            : offset(-1),
              id(-1),
              version(-1),
              server(),
              size(-1)
            {}
    };
    typedef vector<BlockInfo> BlockInfos;

    ///
    /// For testing/debugging purposes, it might useful to know where
    /// the blocks of a file are and what their sizes happen to be.
    /// ** This method results in synchronous communication  with all chunk
    /// servers hosting chunk replicas of the file specified, no
    /// other kfs client methods will execute concurrently until this method
    /// returns.
    /// @param[in] pathname the name of the file that is being queried.
    /// @retval status code
    ///
    int EnumerateBlocks(const char* pathname, BlockInfos& res);

    ///
    /// Given a file in KFS, verify that all N copies of each chunk are
    /// identical.
    /// For files with replication 1, and for files with recovery and
    /// replication 1 this method presently effectively performs *no*
    /// verification.
    /// ** This method results in synchronous communication  with all chunk
    /// servers hosting chunk replicas of the file specified, no other kfs
    /// client methods will execute concurrently until this method returns.
    ///
    /// @param[in] pathname the name of the file that is being queried.
    /// @param[out] md5sum  A string representation of the md5sum of
    /// all chunks in the file. For striped files the returned md5sum will not
    /// match md5sum of the file content.
    /// @retval status code -- 0 OK, 1 mismatch < 0 -- error
    ///
    int CompareChunkReplicas(const char *pathname, string &md5sum);

    ///
    /// Verify that the checksums on replicas is identical.
    /// For files with replication 1, and for files with recovery and
    /// replication 1 this method presently effectively performs *no*
    /// verification.
    /// ** These methods result in synchronous communication  with all chunk
    /// servers hosting chunk replicas of the file specified, no other kfs
    /// client methods will execute concurrently until this method returns.
    ///
    /// @param[in] pathname the name of the file that is being queried.
    /// @retval status code -- 0 OK, 1 mismatch < 0 -- error
    ///
    int VerifyDataChecksums(const char* pathname);
    int VerifyDataChecksums(int fd);


    /// @param[in] params create params encoded as string:
    ///     empty or null string: replication 2
    ///     S -- 6+3 Reed Solomon 64KB stripes 1 replica
    ///     <replicas>,<data stripes>,<recovery stripes>,<stripe size>,<type>
    ///         type:
    ///         1 -- mirror replicas, all parameters except replicas ignored
    ///         2 -- reed solomon, only 0 or 3 recovery stripes are currently
    ///             supported
    /// @param[out] numReplicas
    /// @param[out] numStripes
    /// @param[out] numRecoveryStripes
    /// @param[out] stripeSize
    /// @param[out] stripedType
    /// @retval 0 on success; -errno on failure.
    static int ParseCreateParams(const char* params, int& numReplicas,
        int& numStripes, int& numRecoveryStripes, int& stripeSize,
        int& stripedType);

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
        kfsMode_t mode = 0666);

    ///
    /// Create a file which is specified by a complete path.
    /// @param[in] pathname that has to be created
    /// @param[in] exclusive  create will fail if the exists (O_EXCL flag)
    /// @param[in] params in ParseCreateParams() format
    /// @retval on success, fd corresponding to the created file;
    /// -errno on failure.
    ///
    int Create(const char *pathname, bool exclusive, const char* params);

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
    int Open(const char *pathname, int openFlags, int numReplicas = 3,
        int numStripes = 0, int numRecoveryStripes = 0, int stripeSize = 0,
        int stripedType = KFS_STRIPED_FILE_TYPE_NONE,
        kfsMode_t mode = 0666);

    ///
    /// Create a file which is specified by a complete path.
    /// @param[in] pathname that has to be created
    /// @param[in] params in ParseCreateParams() format
    /// @retval on success, fd corresponding to the created file;
    /// -errno on failure.
    ///
    int Open(const char *pathname, int openFlags, const char* params,
        kfsMode_t mode = 0666);

    ///
    /// Close a file
    /// @param[in] fd that corresponds to a previously opened file
    /// table entry.
    ///
    int Close(int fd);

    ///
    /// Append a record to the chunk that we are writing to in the
    /// file with one caveat: the record should not straddle chunk
    /// boundaries.  That is, if there is insufficient space in the
    /// chunk to hold the record, then this record will be written to
    /// a newly allocated chunk.
    ///
    int RecordAppend(int fd, const char *buf, int reclen);

    ///
    /// With atomic record appends, if multiple clients are writing to
    /// the same file, the writes are serialized by the chunk master.
    ///
    int AtomicRecordAppend(int fd, const char *buf, int reclen);

    void EnableAsyncRW();
    void DisableAsyncRW();

    ///
    /// To mask network latencies, applications can prefetch data from
    /// a chunk.  A request is enqueued and a prefetch thread will
    /// start pulling the data.  For the case of read, there can only
    /// be one outstanding prefetch for a chunk:  Data is read from
    /// the current file position in the chunk.  As a result of the
    /// read request, the file pointer is NOT modified.  Furthermore,
    /// the prefetch will not straddle chunk boundaries.  When a
    /// subsequent read is issued, that read will do the completion
    /// handling; if data prefetched is less than what was asked, the
    /// sync read call will read the additional data/do failover.
    ///
    /// @param[in] fd that corresponds to a previously opened file
    /// table entry.
    /// @param buf For read, the buffer will be filled with data.  The
    /// caller is expected to provide sufficient buffer to hold the
    /// prefetch data AND the buffer shouldn't be mutated while the
    /// read is on-going.
    /// @param[in] numBytes   The # of bytes of I/O to be done.
    /// @retval status code
    ///
    int ReadPrefetch(int fd, char *buf, size_t numBytes);

    ///
    /// Similar to read prefetch, queue a write to a chunk.  In
    /// contrast to the read case, there are several differences:
    /// 1. Each time an async write is issued, the write starts at the
    /// "current" file pointer for N bytes; the call WILL advance the
    /// file pointer.
    /// 2. The write may straddle chunk boundaries.   This call will
    /// breakup the write into multiple requests.
    /// 3. When an async write request is queued, the app is expected
    /// to NOT mutate the buffer until the write is complete.
    ///
    /// @param[in] fd that corresponds to a previously opened file
    /// table entry.
    /// @param buf For writes, the buffer containing data to be written.
    /// @param[in] numBytes   The # of bytes of I/O to be done.
    /// @retval status code
    ///
    int WriteAsync(int fd, const char *buf, size_t numBytes);

    ///
    /// A set of async writes were issued to a file.  Call this method
    /// to do completion handling.  If any of the async writes had
    /// failed, this method will do a sync write.  If the sync write
    /// fails, this method will return an error.
    ///
    /// @param[in] fd that corresponds to a previously opened file
    /// table entry.
    /// @retval 0 on success; -1 if the writes failed
    ///
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
    ssize_t Read(int fd, char *buf, size_t numBytes);
    ssize_t Write(int fd, const char *buf, size_t numBytes);

    ssize_t PRead(int fd, chunkOff_t pos, char *buf, size_t numBytes);
    ssize_t PWrite(int fd, chunkOff_t pos, const char *buf, size_t numBytes);

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
                        vector< vector <string> > &locations);

    int GetDataLocation(int fd, chunkOff_t start, chunkOff_t len,
                        vector< vector <string> > &locations);

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

    ServerLocation GetMetaserverLocation() const;

    /// Set a timeout of nsecs for an IO op.  If the op doesn't
    /// complete in nsecs, it returns an error to the client.
    /// @param[in] desired op timeout in secs
    ///
    void SetDefaultIOTimeout(int nsecs);
    int  GetDefaultIOTimeout() const;

    void SetRetryDelay(int nsecs);
    int  GetRetryDelay() const;

    void SetMaxRetryPerOp(int retryCount);
    int  GetMaxRetryPerOp() const;

    ///
    /// Set default io buffer size.
    /// This has no effect on already opened files.
    /// SetIoBufferSize() can be used to change buffer size for opened file.
    /// @param[in] desired buffer size
    /// @retval actual buffer size
    //
    ssize_t SetDefaultIoBufferSize(size_t size);

    ///
    /// Get read ahead / write behind default buffer size.
    /// @retval buffer size
    //
    ssize_t GetDefaultIoBufferSize() const;

    ///
    /// Set file io buffer size.
    /// @param[in] fd that corresponds to a previously opened file
    /// @param[in] desired buffer size
    /// @retval actual buffer size
    //
    ssize_t SetIoBufferSize(int fd, size_t size);

    ///
    /// Get file io buffer size.
    /// @param[in] fd that corresponds to a previously opened file
    /// @retval buffer size
    //
    ssize_t GetIoBufferSize(int fd) const;

    ///
    /// Set default read ahead size.
    /// This has no effect on already opened files.
    /// @param[in] desired read ahead size
    /// @retval actual default read ahead size
    //
    ssize_t SetDefaultReadAheadSize(size_t size);

    ///
    /// Get read ahead / write behind default buffer size.
    /// @retval buffer size
    //
    ssize_t GetDefaultReadAheadSize() const;

    ///
    /// Set file read ahead size.
    /// @param[in] fd that corresponds to a previously opened file
    /// @param[in] desired read ahead size
    /// @retval actual read ahead size
    //
    ssize_t SetReadAheadSize(int fd, size_t size);

    /// A read for an offset that is after the specified value will result in EOF
    void SetEOFMark(int fd, chunkOff_t offset);

    ///
    /// Get file read ahead size.
    /// @param[in] fd that corresponds to a previously opened file
    /// @retval read ahead size
    //
    ssize_t GetReadAheadSize(int fd) const;

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
            ErrorHandler* errHandler = 0);
    int ChownR(const char* pathname, kfsUid_t user, kfsGid_t group,
            ErrorHandler* errHandler = 0);
    int ChownR(const char* pathname, const char* user, const char* group,
            ErrorHandler* errHandler = 0);
    void SetUMask(kfsMode_t mask);
    kfsMode_t GetUMask() const;
    // Must be invoked before invoking any other method.
    int SetEUserAndEGroup(kfsUid_t user, kfsGid_t group,
        kfsGid_t* groups, int groupsCnt);
    int GetUserAndGroupNames(kfsUid_t user, kfsGid_t group,
        string& uname, string& gname);
    int GetUserAndGroupIds(const char* user, const char* group,
        kfsUid_t& uid, kfsGid_t& gid);
    kfsUid_t GetUserId();
    int GetReplication(const char* pathname,
        KfsFileAttr& attr, int& minChunkReplication, int& maxChunkReplication);
private:
    KfsClientImpl* const mImpl;
};

///
/// @param[in] propFile that describes where the server is and
/// other client configuration info.
///
KfsClient *Connect(const char *propFile);

///
/// Get the client object corresponding to the specified
/// metaserver.  If an object hasn't been created previously,
/// create a new one and return it.  The client object returned is
/// all setup---connected to metaserver and such.
/// @retval if connection to metaserver succeeds, a client object
/// that is "ready" for use; NULL if there was an error
///
KfsClient *Connect(const string &metaServerHost, int metaServerPort);

/// Given a error status code, return a string describing the error.
/// @param[in] status  The status code for an error.
/// @retval String that describes what the error is.
extern string ErrorCodeToStr(int status);
}

#endif // LIBKFSCLIENT_KFSCLIENT_H
