
#include "libclient/KfsClient.h"
#include "qfs.h"

#include <vector>
#include <algorithm>

#include <stdio.h>
#include <string.h>
#include <fcntl.h> // Required for calls to Read

using std::string;
using std::vector;

using namespace KFS;

struct QFS {
  KFS::KfsClient client;
};

static void qfs_attr_from_KfsFileAttr(struct qfs_attr* dst, KfsFileAttr& src);
static int qfs_get_data_locations_inner_impl(struct QFS* qfs,
  void* path_or_fd, off_t offset, size_t len,
  vector< vector <string> > &locations);
static int qfs_get_data_locations_inner_fd_impl(struct QFS* qfs,
  void* path_or_fd, off_t offset, size_t len,
  vector< vector <string> > &locations);
static int qfs_get_data_locations_inner(struct QFS* qfs,
  void* path_or_fd, off_t offset, size_t len,
  struct qfs_iter** iter, off_t* chunk, const char** location,
  int (*op)(struct QFS* qfs,
    void* path_or_fd, off_t offset, size_t len,
    vector< vector <string> > &locations));

struct QFS* qfs_connect(const char* host, int port) {
  struct QFS* qfs = new QFS;

  qfs->client.Init(host, port);

  if (!qfs->client.IsInitialized()) {
    delete qfs;
    return NULL;
  }

  return qfs;
}

void qfs_release(struct QFS* qfs) {
  if(qfs) {
    delete qfs;
  }
}

const char* qfs_strerror(int status, char* buffer, size_t len) {
  const string ret = KFS::ErrorCodeToStr(status);
  if (0 < len) {
    const size_t sz = std::min(ret.size() + 1, len);
    memcpy(buffer, ret.data(), sz);
    if (len == sz) {
        buffer[sz-1] = 0;
    }
  }
  return buffer;
}

int qfs_cd(struct QFS* qfs, const char* path) {
  return qfs->client.Cd(path);
}

int qfs_setwd(struct QFS* qfs, const char* path) {
  return qfs->client.SetCwd(path);
}

int qfs_getwd(struct QFS* qfs, char* cwd, size_t len) {
  return snprintf(cwd, len, "%s", qfs->client.GetCwd().c_str());
}

int qfs_mkdir(struct QFS* qfs, const char* path, mode_t mode) {
  return qfs->client.Mkdir(path, mode);
}

int qfs_mkdirs(struct QFS* qfs, const char* path, mode_t mode) {
  return qfs->client.Mkdirs(path, mode);
}

int qfs_rmdir(struct QFS* qfs, const char* path) {
  return qfs->client.Rmdir(path);
}

int qfs_rmdirs(struct QFS* qfs, const char* path) {
  return qfs->client.Rmdirs(path);
}

enum qfs_iter_type {
  QFS_READDIR = 1,
  QFS_READDIRNAMES = 2,
  QFS_LOCATIONS = 3
};

struct qfs_readdir_iter {
  std::vector<KFS::KfsFileAttr>           dentries;
  std::vector<KFS::KfsFileAttr>::iterator iter;
};

struct qfs_readdirnames_iter {
  std::vector<string>           dentries;
  std::vector<string>::iterator iter;
};

struct qfs_locations_iter {
  std::vector< std::vector<string> >           locations;
  std::vector< std::vector<string> >::iterator iter;
  std::vector<std::string>::iterator           inner;
};

struct qfs_iter {
  qfs_iter_type type;
  void*         iter;

  qfs_iter(struct qfs_readdir_iter* it) {
    type = QFS_READDIR;
    iter = (void*) it;
  }

  qfs_iter(struct qfs_readdirnames_iter* it) {
    type = QFS_READDIRNAMES;
    iter = (void*) it;
  }

  qfs_iter(struct qfs_locations_iter* it) {
    type = QFS_LOCATIONS;
    iter = (void*) it;
  }

  ~qfs_iter() {
    switch (type) {
    case QFS_READDIR: {
        struct qfs_readdir_iter* readdir_iter = (struct qfs_readdir_iter*) iter;
        delete readdir_iter;
      }
      break;
    case QFS_READDIRNAMES: {
        struct qfs_readdirnames_iter* readdirnames_iter = (struct qfs_readdirnames_iter*) iter;
        delete readdirnames_iter;
      }
      break;
    case QFS_LOCATIONS: {
        struct qfs_locations_iter* locations_iter = (struct qfs_locations_iter*) iter;
        delete locations_iter;
      }
    }
  }
};

void qfs_iter_free(struct qfs_iter** iter) {
  if(!iter || !(*iter)) {
    return;
  }

  delete *iter;

  *iter = NULL;
}

int qfs_readdir(struct QFS* qfs, const char* path, struct qfs_iter** it, struct qfs_attr* attr) {
  if(!it) {
    return -EINVAL;
  }

  struct qfs_readdir_iter* iter = NULL;

  if((*it)) {
    if((*it)->type != QFS_READDIR) {
      return -EINVAL;
    }

    iter = (struct qfs_readdir_iter*)(*it)->iter;
  }

  if(!iter) {
    // First call on the given iterator: make the client call initialize iter.
    iter = new qfs_readdir_iter;
    *it = new qfs_iter(iter);

    int res = qfs->client.ReaddirPlus(path, iter->dentries);

    if(res != 0) {
      qfs_iter_free(it);
      return res;
    }

    iter->iter = iter->dentries.begin();
  }

  if(iter->iter < iter->dentries.end()) {
    int left = iter->dentries.size() - (iter->iter - iter->dentries.begin());
    qfs_attr_from_KfsFileAttr(attr, *(iter->iter));
    (iter->iter)++;

    // Return the number of items left to iterate
    return left;
  }

  // Zero-out destination parameter.
  memset(attr, 0, sizeof(struct qfs_attr));
  return 0;
}

int qfs_readdirnames(struct QFS* qfs, const char* path, struct qfs_iter** it, const char** dentry) {
if(!it) {
    return -EINVAL;
  }

  if((*it) && (*it)->type != QFS_READDIRNAMES) {
    // Method was passed incorrect type of iterator
    return -EINVAL;
  }

  struct qfs_readdirnames_iter* iter = NULL;

  if((*it)) {
    if((*it)->type != QFS_READDIRNAMES) {
      return -EINVAL;
    }

    iter = (struct qfs_readdirnames_iter*)(*it)->iter;
  }

  if(!iter) {
    // First call on the given iterator: make the client call initialize iter.
    iter = new qfs_readdirnames_iter;
    *it = new qfs_iter(iter);

    int res = qfs->client.Readdir(path, iter->dentries);

    if(res != 0) {
      qfs_iter_free(it);
      return res;
    }

    iter->iter = iter->dentries.begin();
  }

  if(iter->iter != iter->dentries.end()) {
    int left = iter->dentries.size() - (iter->iter - iter->dentries.begin());
    // Entries are still left: set dentry and march the iterator.
    *dentry = (iter->iter)->c_str();
    iter->iter++;

    // Return the number of items left to iterate
    return left;
  }

  // We are done: dealloc and return that there are zero left.
  *dentry = NULL;
  return 0;
}

int qfs_stat(struct QFS* qfs, const char* path, struct qfs_attr* attr) {
  KfsFileAttr kfsAttrs;

  int res = qfs->client.Stat(path, kfsAttrs);
  if(res < 0) {
    return res;
  }

  qfs_attr_from_KfsFileAttr(attr, kfsAttrs);
  return res;
}

int qfs_stat_fd(struct QFS* qfs, int fd, struct qfs_attr* attr) {
  KfsFileAttr kfsAttrs;

  int res = qfs->client.Stat(fd, kfsAttrs);
  if(res < 0) {
    return res;
  }

  qfs_attr_from_KfsFileAttr(attr, kfsAttrs);
  return res;
}

ssize_t qfs_get_chunksize(struct QFS* qfs, const char* path) {
  return KFS::CHUNKSIZE;
}

bool qfs_exists(struct QFS* qfs, const char* path) {
  return qfs->client.Exists(path);
}

bool qfs_isfile(struct QFS* qfs, const char* path) {
  return qfs->client.IsFile(path);
}

bool qfs_isdirectory(struct QFS* qfs, const char* path) {
  return qfs->client.IsDirectory(path);
}

int qfs_verify_checksums(struct QFS* qfs, const char* path) {
  return qfs->client.VerifyDataChecksums(path);
}

int qfs_verify_checksums_fd(struct QFS* qfs, int fd) {
  return qfs->client.VerifyDataChecksums(fd);
}

int qfs_remove(struct QFS* qfs, const char* path) {
  return qfs->client.Remove(path);
}

int qfs_rename(struct QFS* qfs, const char* oldpath, const char* newpath) {
  return qfs->client.Rename(oldpath, newpath);
}

int qfs_set_mtime(struct QFS* qfs, const char* path, struct timeval* mtime) {
  return qfs->client.SetMtime(path, *mtime);
}

int qfs_create(struct QFS* qfs, const char* path) {
  return qfs->client.Create(path, true, "");
}

int qfs_open(struct QFS* qfs, const char* path) {
  return qfs->client.Open(path, O_RDONLY, "");
}

int qfs_open_file(struct QFS* qfs, const char* path, int openFlags, uint16_t mode, const char* params) {
  return qfs->client.Open(path, openFlags, params, mode);
}

int qfs_close(struct QFS* qfs, int fd) {
    return qfs->client.Close(fd);
}

int qfs_record_append(struct QFS* qfs, int fd, const char* buf, size_t len) {
  return qfs->client.RecordAppend(fd, buf, len);

}
int qfs_atomic_record_append(struct QFS* qfs, int fd, const char* buf, size_t len) {
  return qfs->client.AtomicRecordAppend(fd, buf, len);
}

ssize_t qfs_read(struct QFS* qfs, int fd, void* buf, size_t len) {
  return qfs->client.Read(fd, (char*) buf, len);
}

ssize_t qfs_pread(struct QFS* qfs, int fd, void *buf, size_t len, off_t offset) {
  return qfs->client.PRead(fd, offset, (char*) buf, len);
}


ssize_t qfs_write(struct QFS* qfs, int fd, const void* buf, size_t len) {
    return qfs->client.Write(fd, (char*) buf, len);
}

ssize_t qfs_pwrite(struct QFS* qfs, int fd, const void* buf, size_t len, off_t offset) {
  return qfs->client.PWrite(fd, offset, (char*) buf, len);
}

void qfs_set_skipholes(struct QFS* qfs, int fd) {
  qfs->client.SkipHolesInFile(fd);
}

int qfs_sync(struct QFS* qfs, int fd) {
  return qfs->client.Sync(fd);
}

void qfs_set_eofmark(struct QFS* qfs, int fd, int64_t offset) {
  qfs->client.SetEOFMark(fd, offset);
}

off_t qfs_seek(struct QFS* qfs, int fd, off_t offset, int whence) {
  return qfs->client.Seek(fd, offset, whence);
}

off_t qfs_tell(struct QFS* qfs, int fd) {
  return qfs->client.Tell(fd);
}

int qfs_truncate(struct QFS* qfs, const char* path, off_t offset) {
  return qfs->client.Truncate(path, offset);
}

int qfs_truncate_fd(struct QFS* qfs, int fd, off_t offset) {
  return qfs->client.Truncate(fd, offset);
}

int qfs_prune(struct QFS* qfs, int fd, off_t offset) {
  return qfs->client.PruneFromHead(fd, offset);
}

int qfs_chmod(struct QFS* qfs, const char* path, mode_t mode) {
  return qfs->client.Chmod(path, mode);
}

int qfs_chmod_fd(struct QFS* qfs, int fd, mode_t mode) {
  return qfs->client.Chmod(fd, mode);
}

int qfs_chmod_r(struct QFS* qfs, const char* path, mode_t mode) {
  return qfs->client.ChmodR(path, mode);
}

int qfs_chown(struct QFS* qfs, const char* path, uid_t uid, gid_t gid) {
  return qfs->client.Chown(path, uid, gid);
}

int qfs_chown_fd(struct QFS* qfs, int fd, uid_t uid, gid_t gid) {
  return qfs->client.Chown(fd, uid, gid);
}

int qfs_chown_r(struct QFS* qfs, const char* path, uid_t uid, gid_t gid) {
  return qfs->client.Chown(path, uid, gid);
}

int qfs_get_metaserver_location(struct QFS* qfs, char* location, size_t len) {
  ServerLocation loc = qfs->client.GetMetaserverLocation();
  return snprintf(location, len, "%s:%d", loc.hostname.c_str(), loc.port);
}

int16_t qfs_set_replicationfactor(struct QFS* qfs, const char* path, int16_t replicas) {
  return qfs->client.SetReplicationFactor(path, replicas);
}

int16_t qfs_set_replicationfactor_r(struct QFS* qfs, const char* path, int16_t replicas) {
  // TODO(sday): Handle recursive errors.
  return qfs->client.SetReplicationFactorR(path, replicas);
}

int qfs_get_data_locations(struct QFS* qfs,
  const char* path, off_t offset, size_t len,
  struct qfs_iter** iter, off_t* chunk, const char** locations) {
  return qfs_get_data_locations_inner(qfs,
    (void*)path, offset, len,
    iter, chunk, locations,
    qfs_get_data_locations_inner_impl);
}

int qfs_get_data_locations_fd(struct QFS* qfs,
  int fd, off_t offset, size_t len,
  struct qfs_iter** iter, off_t* chunk, const char** locations) {
  return qfs_get_data_locations_inner(qfs,
    (void*)&fd, offset, len,
    iter, chunk, locations,
    qfs_get_data_locations_inner_fd_impl);
}

void qfs_set_default_iotimeout(struct QFS* qfs, int nsecs) {
  qfs->client.SetDefaultIOTimeout(nsecs);
}

int qfs_get_default_iotimeout(struct QFS* qfs) {
  return qfs->client.GetDefaultIOTimeout();
}

void qfs_set_retrydelay(struct QFS* qfs, int nsecs) {
  qfs->client.SetRetryDelay(nsecs);
}

int qfs_get_retrydelay(struct QFS* qfs) {
  return qfs->client.GetRetryDelay();
}

void qfs_set_max_retryperop(struct QFS* qfs, int retries) {
  qfs->client.SetMaxRetryPerOp(retries);
}

int qfs_get_max_retryperop(struct QFS* qfs) {
  return qfs->client.GetMaxRetryPerOp();
}

size_t qfs_set_default_iobuffersize(struct QFS* qfs, size_t size) {
  return qfs->client.SetDefaultIoBufferSize(size);
}

size_t qfs_get_default_iobuffersize(struct QFS* qfs) {
  return qfs->client.GetDefaultIoBufferSize();
}

size_t qfs_set_iobuffersize(struct QFS* qfs, int fd, size_t size) {
  return qfs->client.SetIoBufferSize(fd, size);
}

size_t qfs_get_iobuffersize(struct QFS* qfs, int fd) {
  return qfs->client.GetIoBufferSize(fd);
}

size_t qfs_set_default_readaheadsize(struct QFS* qfs, size_t size) {
  return qfs->client.SetDefaultReadAheadSize(size);
}

size_t qfs_get_default_readaheadsize(struct QFS* qfs) {
  return qfs->client.GetDefaultReadAheadSize();
}

size_t qfs_set_readaheadsize(struct QFS* qfs, int fd, size_t size) {
  return qfs->client.SetReadAheadSize(fd, size);
}

size_t qfs_get_readaheadsize(struct QFS* qfs, int fd) {
  return qfs->client.GetReadAheadSize(fd);
}

void qfs_set_default_sparsefilesupport(struct QFS* qfs, bool flag) {
  qfs->client.SetDefaultFullSparseFileSupport(flag);
}

int qfs_set_sparsefilesupport(struct QFS* qfs, int fd, bool flag) {
  return qfs->client.SetFullSparseFileSupport(fd, flag);
}

void qfs_set_fileattributerevalidatetime(struct QFS* qfs, int seconds) {
  qfs->client.SetFileAttributeRevalidateTime(seconds);
}

void qfs_set_umask(struct QFS* qfs, mode_t mode) {
  qfs->client.SetUMask(mode);
}

mode_t qfs_get_umask(struct QFS* qfs) {
  return qfs->client.GetUMask();
}

uint32_t qfs_getuid(struct QFS* qfs) {
  return qfs->client.GetUserId();
}

int qfs_get_userandgroupnames(struct QFS* qfs,
  uint32_t uid, uint32_t gid,
  char* user, size_t ulen, char* group, size_t glen) {
  string user_string;
  string group_string;

  int res = qfs->client.GetUserAndGroupNames(uid, gid, user_string, group_string);
  if(res < 0) {
    return res;
  }

  snprintf(user, ulen, "%s", user_string.c_str());
  snprintf(group, glen, "%s", group_string.c_str());

  return 0;
}

int qfs_get_userandgroupids(struct QFS* qfs, char* user, char* group, uint32_t* uid, uint32_t* gid) {
  return qfs->client.GetUserAndGroupIds(user, group, *uid, *gid);
}

int qfs_set_euserandegroup(struct QFS* qfs,
  uint32_t uid, uint32_t gid,
  uint32_t* gids, int ngroups) {
  return qfs->client.SetEUserAndEGroup(uid, gid, gids, ngroups);
}

static void qfs_attr_from_KfsFileAttr(struct qfs_attr* dst, KfsFileAttr& src) {
  snprintf((char*) dst->filename, sizeof(dst->filename), "%s", src.filename.c_str());

  dst->uid = src.user;
  dst->gid = src.group;
  dst->mode = src.mode;

  dst->id = src.fileId;
  dst->mtime = src.mtime;
  dst->ctime = src.ctime;
  dst->crtime = src.crtime;
  dst->directory = src.isDirectory;
  dst->size = src.fileSize;
  dst->chunks = src.subCount1;
  dst->directories = src.subCount2;
  dst->replicas = src.numReplicas;
  dst->stripes = src.numStripes;
  dst->recovery_stripes = src.numRecoveryStripes;
  dst->striper_type = (enum qfs_striper_type) src.striperType;
  dst->stripe_size = src.stripeSize;
  dst->min_stier = src.minSTier;
  dst->max_stier = src.maxSTier;
}

static int qfs_get_data_locations_inner_impl(struct QFS* qfs,
  void* path_or_fd, off_t offset, size_t len,
  vector< vector <string> > &locations) {
  const char* path = (const char*)(path_or_fd);
  return qfs->client.GetDataLocation(path, offset, len, locations);
}

static int qfs_get_data_locations_inner_fd_impl(struct QFS* qfs,
  void* path_or_fd, off_t offset, size_t len,
  vector< vector <string> > &locations) {
  int fd = *((int*)path_or_fd);
  return qfs->client.GetDataLocation(fd, offset, len, locations);
}

static int qfs_get_data_locations_inner(struct QFS* qfs,
  void* path_or_fd, off_t offset, size_t len,
  struct qfs_iter** it, off_t* chunk, const char** location,
  int (*op)(struct QFS* qfs,
    void* path_or_fd, off_t offset, size_t len,
    vector< vector <string> > &locations) ) {

  if(!it) {
    return -EINVAL;
  }

  struct qfs_locations_iter* iter = NULL;

  if(*it) {
    if((*it)->type != QFS_LOCATIONS) {
      return -EINVAL;
    }

    iter = (struct qfs_locations_iter*)((*it)->iter);
  }

  if(!iter) {
    iter = new qfs_locations_iter;
    *it = new qfs_iter(iter);

    int res = op(qfs, path_or_fd, offset, len, iter->locations);

    if(res != 0) {
      return res;
    }

    iter->iter = iter->locations.begin();
    iter->inner = iter->iter->begin();
    *chunk = 0;
  }

  if(iter->iter != iter->locations.end() &&
     iter->inner != iter->iter->end()) {
    *location = iter->inner->c_str();
    (*chunk) = iter->iter - iter->locations.begin();

    // Take the left over chunk value before moving iter
    int left = iter->locations.size() - (iter->iter - iter->locations.begin());

    iter->inner++;
    // Detect if we've incremented to the end of inner and move iter
    if(iter->inner == iter->iter->end()) {
      iter->iter++;

      // Only set begin on inner if we are not at the end outer.
      if(iter->iter != iter->locations.end()) {
        iter->inner = iter->iter->begin();
      }
    }

    // Return the number of blocks left to iterate
    return left;
  }

  // We are done: dealloc and return that there are zero left.
  return 0;
}
