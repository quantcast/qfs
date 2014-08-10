// QFS C API
//
// This file contains C compatible binds for the KfsClient C++ API. It
// attempts to provide a simple and idiomatic mapping into C function space,
// reducing allocations and copies where possible.
//
// At this time, these bindings are considered experimental and there is no
// guarantee that the ABI format will remain the same between releases.

#ifndef LIBQFSC
#define LIBQFSC

#ifdef __cplusplus

#include <string>
#include <vector>

extern "C" {
#endif // __cplusplus

#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

// TODO(sday): Add size checks at compile for size of off_t, size_t, ssize_t,
// uid_t, gid_t and mode_t to ensure they match qfs. We're not going to add a
// bunch of specialized types to port these to other systems, but compilation
// should fail if the storage sizes are too small.

  // QFS is the opaque handle used to manage a callers usage of the qfs
  // library. QFS should only be allocated and freed via qfs_connect and
  // qfs_release.
  struct QFS;

  enum qfs_striper_type {
      KFS_STRIPED_FILE_TYPE_UNKNOWN     = 0,
      KFS_STRIPED_FILE_TYPE_NONE        = 1,
      KFS_STRIPED_FILE_TYPE_RS          = 2,
      KFS_STRIPED_FILE_TYPE_RS_JERASURE = 3
  };

// From KfsClient.h
#define QFS_MAX_FILENAME_LEN 256

  // qfs_attr provides data about a file in qfs.
  struct qfs_attr {
    // From KFS::KfsFileAttr
    const char filename[QFS_MAX_FILENAME_LEN];

    // From KFS::Permissions
    uid_t  uid;
    gid_t  gid;
    mode_t  mode;

    // From KFS::FileAttr
    int64_t           id;      /// i-node number
    struct timeval    mtime;       /// modification time
    struct timeval    ctime;       /// attribute change time
    struct timeval    crtime;      /// creation time
    bool              directory; /// is this a directory?
    size_t            size;    /// logical eof
    size_t            chunks;   /// number of chunks in the file or files in directory
    size_t            directories;   /// directories count
    int16_t           replicas;
    int16_t           stripes;
    int16_t           recovery_stripes;
    enum qfs_striper_type striper_type;
    int32_t           stripe_size;
    int8_t            min_stier;
    int8_t            max_stier;
  };

  // qfs_iter is an opaque, iterator type, used for reentrant iteration.
  struct qfs_iter;

  // qfs_connect connects to the specified metaserver, returning a QFS handle.
  // On error, this function will return NULL.
  struct QFS* qfs_connect(const char* host, int port);

  // qfs_release disconnects and deallocates any resources or memory
  // associated with this QFS handle.
  void qfs_release(struct QFS* qfs);

  // qfs_strerror will return the a human-readable string corresponding to
  // status. This function can be applied safely to the result of most qfs
  // methods.
  const char* qfs_strerror(int status, char* buffer, size_t len);

  // qfs_cd changes the current working directory for the provided QFS handle.
  int qfs_cd(struct QFS* qfs, const char* path);

  // qfs_setcwd sets the current working directory for the QFS handle.
  int qfs_setwd(struct QFS* qfs, const char* path);

  // qfs_getwd writes the working directory into wd, up to len characters.
  // The full written length is returned including a trailing null byte. If
  // the return value is greater than or equal to the len argument, the
  // working directory was truncated.
  int qfs_getwd(struct QFS* qfs, char* wd, size_t len);

  // qfs_mkdir creates the directory at path, with mode, returning a negative
  // integer on error.
  int qfs_mkdir(struct QFS* qfs, const char* path, mode_t mode);

  // qfs_mkdirs will create a directory and any non-existent parents specified
  // by path. It is the analogue to 'mkdir -p'.
  int qfs_mkdirs(struct QFS* qfs, const char* path, mode_t mode);

  // qfs_rmdir removes the directory at path.
  int qfs_rmdir(struct QFS* qfs, const char* path);

  // qfs_rmdirs recursively removes the directory at path and any of its
  // children.
  int qfs_rmdirs(struct QFS* qfs, const char* path);

  // qfs_iter_free releases any memory allocated by any iterable function,
  // returning the iterator to a fresh state.
  void qfs_iter_free(struct qfs_iter** iter);

  // qfs_readdir iterates through all directory entries in the given path,
  // writing the result into attr after each call, returning the 1-based,
  // reverse index (see below for recommend usage). If an error occurred, the
  // return value will be negative and contain an errno. The parameter iter
  // should be freed with qfs_iter_free regardless of the result.
  //
  // If any results in attr need to be used, they must be copied out before
  // the next iteration, as the storage location may be reused or freed.
  //
  // Note that this scheme is meant to reduce memory copies from C++ data
  // structures; qfs_readdir will still require enough memory internally to
  // store all the directory entries at a given path.
  //
  // Usage:
  //
  //  qfs_iter iter = NULL;
  //  struct qfs_attr attr;
  //  int left;
  //  while((left = qfs_readdir(qfs, "/", &iter, &attr) > 0) {
  //    // ... work with attr
  //    if(some condition) {
  //      // qfs_readdir_iter_free must be called if breaking iteration.
  //      qfs_iter_free(&iter);
  //      break;
  //    }
  //  }
  //
  //  if(left < 0) { // handle error condition }
  //
  int qfs_readdir(struct QFS* qfs, const char* path, struct qfs_iter** iter, struct qfs_attr* attr);

  // qfs_readdirnames iterates through all the file names in the given path if
  // it is a directory, pointing the result at the directing name, returning
  // the reverse, 1-based index of the entry. The iter parameter should be
  // freed with qfs_iter_free, regardless of the result. If value pointed to
  // by dentry is to be used after the next following call to
  // qfs_readdirnames, it should be copied before continuing to the next
  // iteration.
  //
  // The semantics of this function are similar to qfs_readdir and the same
  // conventions should be followed in its use.
  //
  // If only the filenames are required, use this function as opposed to
  // qfs_readdir to reduce to memory storage requirements. The memory required
  // is still linear in the number of directory entries, but only the file
  // names are stored, rather than the entire attribute structure.
  int qfs_readdirnames(struct QFS* qfs, const char* path, struct qfs_iter** iter, const char** dentry);

  // Omitted OpenDirectory has been omitted because its similar in
  // functionality to ReaddirPlus, doesn't save on KfsClient/FileTable memory
  // usage and is harder to use.

  // qfs_stat/qfs_stat_fd provide the qfs_attr for the file at path or
  // reference by id.
  int qfs_stat(struct QFS* qfs, const char* path, struct qfs_attr* attr);
  int qfs_stat_fd(struct QFS* qfs, int fd, struct qfs_attr* attr);

  // Omitted GetNumChunks in favor of call to Stat

  ssize_t qfs_get_chunksize(struct QFS* qfs, const char* path);

  // Omitted UpdateFileSize due to lack of difference from Sync.

  // qfs_exists returns true if the file or directory referenced by path
  // exists.
  bool qfs_exists(struct QFS* qfs, const char* path);

  // qfs_isfile returns true if the entry at path is a regular file.
  bool qfs_isfile(struct QFS* qfs, const char* path);

  // qfs_isdirectory returns true if path is a directory.
  bool qfs_isdirectory(struct QFS* qfs, const char* path);

  // Omitted BlockInfo, EnumerateBlocks, CompareChunkReplicas

  // qfs_verify_checksums/qfs_verify_checksums_fd verifies the checksums for
  // all replicas specified by path or fd, returning 0 on OK, 1 on mismatch
  // and negative on error.
  int qfs_verify_checksums(struct QFS* qfs, const char* path);
  int qfs_verify_checksums_fd(struct QFS* qfs, int fd);

  // qfs_remove removes the file specified by path.
  int qfs_remove(struct QFS* qfs, const char* path);

  // qfs_rename renames the file or directory at oldpath to newpath.
  int qfs_rename(struct QFS* qfs, const char* oldpath, const char* newpath);

  // Omitted ColaesceBlocks

  // qfs_set_mtime sets the modification time to timeval.
  int qfs_set_mtime(struct QFS* qfs, const char* path, struct timeval* mtime);

  // qfs_create will create a file at path, returning an opening fd for
  // writing. qfs_create will fail if the file already exists.
  int qfs_create(struct QFS* qfs, const char* path);

  // qfs_open opens a file for reading, returning a valid file descriptor, if
  // successful.
  int qfs_open(struct QFS* qfs, const char* path);

  // qfs_open_file opens a file with the specified path, mode, and params.
  int qfs_open_file(struct QFS* qfs, const char* path, int oflags, uint16_t mode, const char* params);

  // qfs_close closes the provided fd.
  int qfs_close(struct QFS* qfs, int fd);

  // TODO(sday): qfs_record_append/qfs_atomic_record_append should be omitted
  // as they are really deprecated in the C++ API.

  // qfs_record_append appends the record, defined by buf and len, to the
  // provided fd.
  int qfs_record_append(struct QFS* qfs, int fd, const char* buf, size_t len);

  // qfs_atomic_record_append atomically appends the record, defined by buf
  // and len, to the provided fd.
  int qfs_atomic_record_append(struct QFS* qfs, int fd, const char* buf, size_t len);

  // qfs_read reads up to len bytes from fd into buf at the current file
  // position.
  ssize_t qfs_read(struct QFS* qfs, int fd, void* buf, size_t len);

  // qfs_pread reads up len bytes from fd into buf at offset without updating
  // the current file position.
  ssize_t qfs_pread(struct QFS* qfs, int fd, void *buf, size_t len, off_t offset);

  // qfs_write writes len bytes from buf to fd at the current file position.
  ssize_t qfs_write(struct QFS* qfs, int fd, const void *buf, size_t len);

  // qfs_pwrite writes len bytes to fd at offset without updating the currect
  // file position.
  ssize_t qfs_pwrite(struct QFS* qfs, int fd, const void *buf, size_t len, off_t offset);

  // qfs_set_skipholes instructs the client to skip holes when reading fd.
  void qfs_set_skipholes(struct QFS* qfs, int fd);

  // qfs_sync syncs out any unwritten data to fd.
  int qfs_sync(struct QFS* qfs, int fd);

  // qfs_set_eofmark sets the end of file mark for the given fd.
  void qfs_set_eofmark(struct QFS* qfs, int fd, off_t offset);

  // qfs_seek changes the current file position based on offset and whence,
  // returning the resulting seek offset.
  off_t qfs_seek(struct QFS* qfs, int fd, off_t offset, int whence);

  // qfs_tell returns the current position in fd.
  off_t qfs_tell(struct QFS* qfs, int fd);

  // qfs_truncate/qfs_truncate_fd truncates the file specified by fd to the
  // provided offset.
  int qfs_truncate(struct QFS* qfs, const char* path, off_t offset);
  int qfs_truncate_fd(struct QFS* qfs, int fd, off_t offset);

  // qfs_prune removes all data up to offset from the head of fd.
  int qfs_prune(struct QFS* qfs, int fd, int64_t offset);

  // qfs_chmod, et. al change the file mode for the path or fd. qfs_chmodr
  // does so recursively.
  int qfs_chmod(struct QFS* qfs, const char* path, mode_t mode);
  int qfs_chmod_fd(struct QFS* qfs, int fd, mode_t mode);
  int qfs_chmod_r(struct QFS* qfs, const char* path, mode_t mode);

  // qfs_chown, et. al changes the owner of the file specified by path or fd.
  // qfs_chown_r does so recursively.
  int qfs_chown(struct QFS* qfs, const char* name, uint32_t user, uint32_t group);
  int qfs_chown_r(struct QFS* qfs, const char* name, uint32_t user, uint32_t group);
  int qfs_chown_fd(struct QFS* qfs, int fd, uint32_t user, uint32_t group);

  // qfs_get_metaserver_location returns the current metaserver location in
  // host:port format. Up to len-1 data will be written to location, followed
  // by a terminating \0. If the return value is greater than or equal to len,
  // the write was truncated.
  int qfs_get_metaserver_location(struct QFS* qfs, char* location, size_t len);

  // Omitted GetReplicationFactor omitted in favor of stat call

  // qfs_set_replicationfactor sets the replication factor for the node at
  // path. qfs_set_replicationfactor_r does so for path and its children.
  int16_t qfs_set_replicationfactor(struct QFS* qfs, const char* path, int16_t replicas);
  int16_t qfs_set_replicationfactor_r(struct QFS* qfs, const char* path, int16_t replicas);

  // qfs_get_data_locations/_fd iterates over the inner product of the block
  // and all its locations that make up the region of the file for the
  // specified path or fd. The iteration state is managed via iter, which
  // should be freed by qfs_locations_iter_free if the iteration does not
  // complete. The semantics are similar to that of qfs_readdir, except the
  // return value corresponds to the number of blocks left, rather than the
  // number of calls, returning 0 when complete and -errno on error.
  //
  // Usage:
  //
  // qfs_iter iter;
  // char** location;
  // int nentries;
  // int res;
  //
  // while((res = qfs_get_data_locations(qfs,
  //   "/test", 0, 134217728, &iter, &chunk, &location)) > 0) {
  //   printf("chunk: %lli location: %s\n", chunk, location);
  //
  //   if(some condition) {
  //     // Early termination requires a free of iter.
  //     break;
  //   }
  // }
  //
  // if(res < 0) {
  //   printf("error getting data locations: %s\n", qfs_strerror(res));
  // }
  //
  // qfs_iter_free(iter);
  //
  int qfs_get_data_locations(struct QFS* qfs,
    const char* path, off_t offset, size_t len,
    struct qfs_iter** iter, off_t* chunk, const char** locations);
  int qfs_get_data_locations_fd(struct QFS* qfs,
    int fd, off_t offset, size_t len,
    struct qfs_iter** iter, off_t* chunk, const char** locations);

  // qfs_get_default_iotimeout/qfs_set_default_iotimeout accesses and set the
  // default io timeout used with this instance of qfs.
  void qfs_set_default_iotimeout(struct QFS* qfs, int nsecs);
  int qfs_get_default_iotimeout(struct QFS* qfs);

  // qfs_get_retrydelay/qfs_set_retrydelay accesses and sets the retry delay
  // for filesystem operations.
  void qfs_set_retrydelay(struct QFS* qfs, int nsecs);
  int qfs_get_retrydelay(struct QFS* qfs);

  // qfs_set_max_retryperop/qfs_get_max_retryperop sets the maximum number of
  // retries per QFS operation before reporting failure.
  void qfs_set_max_retryperop(struct QFS* qfs, int retries);
  int  qfs_get_max_retryperop(struct QFS* qfs);

  // qfs_set_default_iobuffersize/qfs_get_default_iobuffersize controls the
  // default io buffer size used for new file descriptors. This does not
  // affect currently open files.
  size_t qfs_set_default_iobuffersize(struct QFS* qfs, size_t size);
  size_t qfs_get_default_iobuffersize(struct QFS* qfs);

  // qfs_set_iobuffersize/qfs_get_iobuffersize controls the buffer size used
  // for operations on fd.
  size_t qfs_set_iobuffersize(struct QFS* qfs, int fd, size_t size);
  size_t qfs_get_iobuffersize(struct QFS* qfs, int fd);

  // qfs_set_default_readaheadsize/qfs_get_default_readaheadsize controls the
  // readaheadsize applied to newly opened files.
  size_t qfs_set_default_readaheadsize(struct QFS* qfs, size_t size);
  size_t qfs_get_default_readaheadsize(struct QFS* qfs);

  // qfs_set_readaheadsize/qfs_get_readaheadsize controls the current
  // readahead size for a given file descriptor.
  size_t qfs_set_readaheadsize(struct QFS* qfs, int fd, size_t size);
  size_t qfs_get_readaheadsize(struct QFS* qfs, int fd);

  // GetFileOrChunkInfo as the specific use case is not clear.

  // qfs_set_default_sparsefilesupport enables sparse file support for new
  // file descriptors.
  void qfs_set_default_sparsefilesupport(struct QFS* qfs, bool flag);

  // qfs_set_sparsefilesupport enables sparse file support for the given fd.
  // This call must be issued before the first read.
  int qfs_set_sparsefilesupport(struct QFS* qfs, int fd, bool flag);

    // qfs_set_fileattributerevalidatetime sets the time after which file
  // attributes are considered stale and must be re-read.
  void qfs_set_fileattributerevalidatetime(struct QFS* qfs, int seconds);

  // NOTE(sday): The following methods concerning users and groups should be
  // considered extra experimental. While we've exposed a useful set of the
  // C++ functionality, this will likely change.

  void qfs_set_umask(struct QFS* qfs, mode_t mode);
  mode_t qfs_get_umask(struct QFS* qfs);

  uint32_t qfs_getuid(struct QFS* qfs);

  // qfs_get_userandgroupnames looks up the names for uid and gid and writes
  // up to ulen or glen bytes to user and group, respectively.
  int qfs_get_userandgroupnames(struct QFS* qfs,
    uid_t uid, gid_t gid,
    char* user, size_t ulen,
    char* group, size_t glen);

  // qfs_get_userandgroupids looks up the uid and gid named by user and group.
  int qfs_get_userandgroupids(struct QFS* qfs,
    char* user, char* group,
    uid_t* uid, gid_t* gid);

  // Omitted GetReplication in favor of Stat call

  // qfs_set_euserandegroup sets the effective, primary user and group and the
  // current group memberships.
  int qfs_set_euserandegroup(struct QFS* qfs,
    uid_t uid, gid_t gid,
    gid_t* gids, int ngroups);

  // TODO(sday): Provide some control over the logging system from the c
  // client. It can be handy for debugging.

#ifdef __cplusplus
}
#endif // __cplusplus

#endif // LIBQFSC
