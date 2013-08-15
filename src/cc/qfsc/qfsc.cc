
#include "libclient/KfsClient.h"
#include "qfs.h"

#include <vector>

#include <string.h>
#include <fcntl.h> // Required for calls to Read

using std::string;
using std::vector;

using namespace KFS;

static void qfs_attr_from_KfsFileAttr(struct qfs_attr* dst, KfsFileAttr& src);
static int qfs_get_data_locations_inner_impl(QFS qfs,
  void* path_or_fd, off_t offset, size_t len,
  vector< vector <string> > &locations);
static int qfs_get_data_locations_inner_fd_impl(QFS qfs,
  void* path_or_fd, off_t offset, size_t len,
  vector< vector <string> > &locations);
static int qfs_get_data_locations_inner(QFS qfs,
  void* path_or_fd, off_t offset, size_t len,
  struct qfs_locations_iter* iter, off_t* chunk, const char** location,
  int (*op)(QFS qfs,
    void* path_or_fd, off_t offset, size_t len,
    vector< vector <string> > &locations));

QFS qfs_connect(const char* host, int port) {
  return KFS::Connect(host, port);
}

void qfs_release(QFS qfs) {
  if(qfs) {
    delete (KFS::KfsClient*) qfs;
  }
}

const char* qfs_strerror(int status) {
  return KFS::ErrorCodeToStr(status).c_str();
}

int qfs_cd(QFS qfs, const char* path) {
  return ((KFS::KfsClient*) qfs)->Cd(path);
}

int qfs_setwd(QFS qfs, const char* path) {
  return ((KFS::KfsClient*) qfs)->SetCwd(path);
}

int qfs_getwd(QFS qfs, char* cwd, size_t len) {
  return snprintf(cwd, len, "%s", ((KFS::KfsClient*) qfs)->GetCwd().c_str());
}

int qfs_mkdir(QFS qfs, const char* path, mode_t mode) {
  return ((KFS::KfsClient*) qfs)->Mkdir(path, mode);
}

int qfs_mkdirs(QFS qfs, const char* path, mode_t mode) {
  return ((KFS::KfsClient*) qfs)->Mkdirs(path, mode);
}

int qfs_rmdir(QFS qfs, const char* path) {
  return ((KFS::KfsClient*) qfs)->Rmdir(path);
}

int qfs_rmdirs(QFS qfs, const char* path) {
  return ((KFS::KfsClient*) qfs)->Rmdirs(path);
}

void qfs_readdir_iter_free(struct qfs_readdir_iter* iter) {
  if(iter && iter->_dentries) {
    delete iter->_dentries;
  }
}

int qfs_readdir(QFS qfs, const char* path, struct qfs_readdir_iter* iter, struct qfs_attr* attr) {
  if(!iter) {
    return -EINVAL;
  }

  if(!iter->_dentries) {
    // First call on the given iterator: make the client call initialize iter.
    KFS::KfsClient* client = (KFS::KfsClient*) qfs;
    iter->_dentries = new vector<KFS::KfsFileAttr>;

    int res;
    if((res = client->ReaddirPlus(path, *(iter->_dentries))) != 0) {
      qfs_readdir_iter_free(iter);
      return res;
    }

    iter->_iter = iter->_dentries->begin();
  }

  if(iter->_iter != iter->_dentries->end()) {
    qfs_attr_from_KfsFileAttr(attr, *(iter->_iter));
    (iter->_iter)++;

    // Return the number of items left to iterate
    return iter->_dentries->size() - (iter->_iter - iter->_dentries->begin());
  }

  // We are done: dealloc and return that there are zero left.
  qfs_readdir_iter_free(iter);
  return 0;
}

void qfs_readdirnames_iter_free(struct qfs_readdirnames_iter* iter) {
  if(iter && iter->_dentries) {
    delete iter->_dentries;
    iter->_dentries = NULL;
  }
}

int qfs_readdirnames(QFS qfs, const char* path, struct qfs_readdirnames_iter* iter, const char** dentry) {
  if(!iter) {
    return -EINVAL;
  }

  if(!iter->_dentries) {
    // First call on the given iterator: make the client call initialize iter.
    KFS::KfsClient* client = (KFS::KfsClient*) qfs;
    iter->_dentries = new vector<string>;

    int res = client->Readdir(path, *(iter->_dentries));

    if(res != 0) {
      qfs_readdirnames_iter_free(iter);
      return res;
    }

    iter->_iter = iter->_dentries->begin();
  }

  if(iter->_iter != iter->_dentries->end()) {
    // Entries are still left: set dentry and march the iterator.
    *dentry = (iter->_iter)->c_str();
    iter->_iter++;

    // Return the number of items left to iterate
    return iter->_dentries->size() - (iter->_iter - iter->_dentries->begin());
  }

  // We are done: dealloc and return that there are zero left.
  *dentry = NULL;
  qfs_readdirnames_iter_free(iter);
  return 0;
}

int qfs_stat(QFS qfs, const char* path, struct qfs_attr* attr) {
  KFS::KfsClient* client = (KFS::KfsClient*) qfs;
  KfsFileAttr kfsAttrs;

  int res = client->Stat(path, kfsAttrs);
  if(res < 0) {
    return res;
  }

  qfs_attr_from_KfsFileAttr(attr, kfsAttrs);
  return res;
}

int qfs_stat_fd(QFS qfs, int fd, struct qfs_attr* attr) {
  KFS::KfsClient* client = (KFS::KfsClient*) qfs;
  KfsFileAttr kfsAttrs;

  int res = client->Stat(fd, kfsAttrs);
  if(res < 0) {
    return res;
  }

  qfs_attr_from_KfsFileAttr(attr, kfsAttrs);
  return res;
}

ssize_t qfs_get_chunksize(QFS qfs, const char* path) {
  return KFS::CHUNKSIZE;
}

bool qfs_exists(QFS qfs, const char* path) {
  return ((KFS::KfsClient*) qfs)->Exists(path);
}

bool qfs_isfile(QFS qfs, const char* path) {
  return ((KFS::KfsClient*) qfs)->IsFile(path);
}

bool qfs_isdirectory(QFS qfs, const char* path) {
  return ((KFS::KfsClient*) qfs)->IsDirectory(path);
}

int qfs_verify_checksums(QFS qfs, const char* path) {
  return ((KFS::KfsClient*) qfs)->VerifyDataChecksums(path);
}

int qfs_verify_checksums_fd(QFS qfs, int fd) {
  return ((KFS::KfsClient*) qfs)->VerifyDataChecksums(fd);
}

int qfs_remove(QFS qfs, const char* path) {
  return ((KFS::KfsClient*) qfs)->Remove(path);
}

int qfs_rename(QFS qfs, const char* oldpath, const char* newpath) {
  return ((KFS::KfsClient*) qfs)->Rename(oldpath, newpath);
}

int qfs_set_mtime(QFS qfs, const char* path, struct timeval* mtime) {
  return ((KFS::KfsClient*) qfs)->SetMtime(path, *mtime);
}

int qfs_create(QFS qfs, const char* path) {
  return ((KFS::KfsClient*) qfs)->Create(path, true, "");
}

int qfs_open(QFS qfs, const char* path) {
  return ((KFS::KfsClient*) qfs)->Open(path, O_RDONLY, "");
}

int qfs_open_file(QFS qfs, const char* path, int openFlags, uint16_t mode, const char* params) {
  return ((KFS::KfsClient*) qfs)->Open(path, openFlags, params, mode);
}

int qfs_close(QFS qfs, int fd) {
    return ((KFS::KfsClient*) qfs)->Close(fd);
}

int qfs_record_append(QFS qfs, int fd, const char* buf, size_t len) {
  return ((KFS::KfsClient*) qfs)->RecordAppend(fd, buf, len);

}
int qfs_atomic_record_append(QFS qfs, int fd, const char* buf, size_t len) {
  return ((KFS::KfsClient*) qfs)->AtomicRecordAppend(fd, buf, len);
}

ssize_t qfs_read(QFS qfs, int fd, void* buf, size_t len) {
  return ((KFS::KfsClient*) qfs)->Read(fd, (char*) buf, len);
}

ssize_t qfs_pread(QFS qfs, int fd, void *buf, size_t len, off_t offset) {
  return ((KFS::KfsClient*) qfs)->PRead(fd, offset, (char*) buf, len);
}


ssize_t qfs_write(QFS qfs, int fd, const void* buf, size_t len) {
    return ((KFS::KfsClient*) qfs)->Write(fd, (char*) buf, len);
}

ssize_t qfs_pwrite(QFS qfs, int fd, const void* buf, size_t len, off_t offset) {
  return ((KFS::KfsClient*) qfs)->PWrite(fd, offset, (char*) buf, len);
}

void qfs_set_skipholes(QFS qfs, int fd) {
  ((KFS::KfsClient*) qfs)->SkipHolesInFile(fd);
}

int qfs_sync(QFS qfs, int fd) {
  return ((KFS::KfsClient*) qfs)->Sync(fd);
}

void qfs_set_eofmark(QFS qfs, int fd, int64_t offset) {
  ((KFS::KfsClient*) qfs)->SetEOFMark(fd, offset);
}

off_t qfs_seek(QFS qfs, int fd, off_t offset, int whence) {
  return ((KFS::KfsClient*) qfs)->Seek(fd, offset, whence);
}

off_t qfs_tell(QFS qfs, int fd) {
  return ((KFS::KfsClient*) qfs)->Tell(fd);
}

int qfs_truncate(QFS qfs, const char* path, off_t offset) {
  return ((KFS::KfsClient*) qfs)->Truncate(path, offset);
}

int qfs_truncate_fd(QFS qfs, int fd, off_t offset) {
  return ((KFS::KfsClient*) qfs)->Truncate(fd, offset);
}

int qfs_prune(QFS qfs, int fd, off_t offset) {
  return ((KFS::KfsClient*) qfs)->PruneFromHead(fd, offset);
}

int qfs_chmod(QFS qfs, const char* path, mode_t mode) {
  return ((KFS::KfsClient*) qfs)->Chmod(path, mode);
}

int qfs_chmod_fd(QFS qfs, int fd, mode_t mode) {
  return ((KFS::KfsClient*) qfs)->Chmod(fd, mode);
}

int qfs_chmod_r(QFS qfs, const char* path, mode_t mode) {
  return ((KFS::KfsClient*) qfs)->ChmodR(path, mode);
}

int qfs_chown(QFS qfs, const char* path, uid_t uid, gid_t gid) {
  return ((KFS::KfsClient*) qfs)->Chown(path, uid, gid);
}

int qfs_chown_fd(QFS qfs, int fd, uid_t uid, gid_t gid) {
  return ((KFS::KfsClient*) qfs)->Chown(fd, uid, gid);
}

int qfs_chown_r(QFS qfs, const char* path, uid_t uid, gid_t gid) {
  return ((KFS::KfsClient*) qfs)->Chown(path, uid, gid);
}

int qfs_get_metaserver_location(QFS qfs, char* location, size_t len) {
  ServerLocation loc = ((KFS::KfsClient*) qfs)->GetMetaserverLocation();
  return snprintf(location, len, "%s:%d", loc.hostname.c_str(), loc.port);
}

int16_t qfs_set_replicationfactor(QFS qfs, const char* path, int16_t replicas) {
  return ((KFS::KfsClient*) qfs)->SetReplicationFactor(path, replicas);
}

int16_t qfs_set_replicationfactor_r(QFS qfs, const char* path, int16_t replicas) {
  // TODO(sday): Handle recursive errors.
  return ((KFS::KfsClient*) qfs)->SetReplicationFactorR(path, replicas);
}

void qfs_locations_iter_free(struct qfs_locations_iter* iter) {
  if(iter && iter->_locations) {
    delete iter->_locations;
    iter->_locations = NULL;
  }
}

int qfs_get_data_locations(QFS qfs,
  const char* path, off_t offset, size_t len,
  struct qfs_locations_iter* iter, off_t* chunk, const char** locations) {
  return qfs_get_data_locations_inner(qfs,
    (void*)path, offset, len,
    iter, chunk, locations,
    qfs_get_data_locations_inner_impl);
}

int qfs_get_data_locations_fd(QFS qfs,
  int fd, off_t offset, size_t len,
  struct qfs_locations_iter* iter, off_t* chunk, const char** locations) {
  return qfs_get_data_locations_inner(qfs,
    (void*)&fd, offset, len,
    iter, chunk, locations,
    qfs_get_data_locations_inner_fd_impl);
}

void qfs_set_default_iotimeout(QFS qfs, int nsecs) {
  ((KFS::KfsClient*) qfs)->SetDefaultIOTimeout(nsecs);
}

int qfs_get_default_iotimeout(QFS qfs) {
  return ((KFS::KfsClient*) qfs)->GetDefaultIOTimeout();
}

void qfs_set_retrydelay(QFS qfs, int nsecs) {
  ((KFS::KfsClient*) qfs)->SetRetryDelay(nsecs);
}

int qfs_get_retrydelay(QFS qfs) {
  return ((KFS::KfsClient*) qfs)->GetRetryDelay();
}

void qfs_set_max_retryperop(QFS qfs, int retries) {
  ((KFS::KfsClient*) qfs)->SetMaxRetryPerOp(retries);
}

int qfs_get_max_retryperop(QFS qfs) {
  return ((KFS::KfsClient*) qfs)->GetMaxRetryPerOp();
}

size_t qfs_set_default_iobuffersize(QFS qfs, size_t size) {
  return ((KFS::KfsClient*) qfs)->SetDefaultIoBufferSize(size);
}

size_t qfs_get_default_iobuffersize(QFS qfs) {
  return ((KFS::KfsClient*) qfs)->GetDefaultIoBufferSize();
}

size_t qfs_set_iobuffersize(QFS qfs, int fd, size_t size) {
  return ((KFS::KfsClient*) qfs)->SetIoBufferSize(fd, size);
}

size_t qfs_get_iobuffersize(QFS qfs, int fd) {
  return ((KFS::KfsClient*) qfs)->GetIoBufferSize(fd);
}

size_t qfs_set_default_readaheadsize(QFS qfs, size_t size) {
  return ((KFS::KfsClient*) qfs)->SetDefaultReadAheadSize(size);
}

size_t qfs_get_default_readaheadsize(QFS qfs) {
  return ((KFS::KfsClient*) qfs)->GetDefaultReadAheadSize();
}

size_t qfs_set_readaheadsize(QFS qfs, int fd, size_t size) {
  return ((KFS::KfsClient*) qfs)->SetReadAheadSize(fd, size);
}

size_t qfs_get_readaheadsize(QFS qfs, int fd) {
  return ((KFS::KfsClient*) qfs)->GetReadAheadSize(fd);
}

void qfs_set_default_sparsefilesupport(QFS qfs, bool flag) {
  ((KFS::KfsClient*) qfs)->SetDefaultFullSparseFileSupport(flag);
}

int qfs_set_sparsefilesupport(QFS qfs, int fd, bool flag) {
  return ((KFS::KfsClient*) qfs)->SetFullSparseFileSupport(fd, flag);
}

void qfs_set_fileattributerevalidatetime(QFS qfs, int seconds) {
  ((KFS::KfsClient*) qfs)->SetFileAttributeRevalidateTime(seconds);
}

void qfs_set_umask(QFS qfs, mode_t mode) {
  ((KFS::KfsClient*) qfs)->SetUMask(mode);
}

mode_t qfs_get_umask(QFS qfs) {
  return ((KFS::KfsClient*) qfs)->GetUMask();
}

uint32_t qfs_getuid(QFS qfs) {
  return ((KFS::KfsClient*) qfs)->GetUserId();
}

int qfs_get_userandgroupnames(QFS qfs,
  uint32_t uid, uint32_t gid,
  char* user, size_t ulen, char* group, size_t glen) {
  string user_string;
  string group_string;

  int res = ((KFS::KfsClient*) qfs)->GetUserAndGroupNames(uid, gid, user_string, group_string);
  if(res < 0) {
    return res;
  }

  snprintf(user, ulen, "%s", user_string.c_str());
  snprintf(group, glen, "%s", group_string.c_str());

  return 0;
}

int qfs_get_userandgroupids(QFS qfs, char* user, char* group, uint32_t* uid, uint32_t* gid) {
  return ((KFS::KfsClient*) qfs)->GetUserAndGroupIds(user, group, *uid, *gid);
}

int qfs_set_euserandegroup(QFS qfs,
  uint32_t uid, uint32_t gid,
  uint32_t* gids, int ngroups) {
  return ((KFS::KfsClient*) qfs)->SetEUserAndEGroup(uid, gid, gids, ngroups);
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
  dst->striper_type = (enum striper_type) src.striperType;
  dst->stripe_size = src.stripeSize;
  dst->min_stier = src.minSTier;
  dst->max_stier = src.maxSTier;
}

static int qfs_get_data_locations_inner_impl(QFS qfs,
  void* path_or_fd, off_t offset, size_t len,
  vector< vector <string> > &locations) {
  const char* path = (const char*)(path_or_fd);
  return ((KFS::KfsClient*) qfs)->GetDataLocation(path, offset, len, locations);
}

static int qfs_get_data_locations_inner_fd_impl(QFS qfs,
  void* path_or_fd, off_t offset, size_t len,
  vector< vector <string> > &locations) {
  int fd = *((int*)path_or_fd);
  return ((KFS::KfsClient*) qfs)->GetDataLocation(fd, offset, len, locations);
}

static int qfs_get_data_locations_inner(QFS qfs,
  void* path_or_fd, off_t offset, size_t len,
  struct qfs_locations_iter* iter, off_t* chunk, const char** location,
  int (*op)(QFS qfs,
    void* path_or_fd, off_t offset, size_t len,
    vector< vector <string> > &locations) ) {

  if(!iter) {
    return -EINVAL;
  }

  if(!iter->_locations) {
    iter->_locations = new vector< vector<string> >;

    int res = op(qfs, path_or_fd, offset, len, *(iter->_locations));
    if(res != 0) {
      qfs_locations_iter_free(iter);
      return res;
    }

    iter->_iter = iter->_locations->begin();
    iter->_inner = iter->_iter->begin();
    *chunk = 0;
  }

  if(iter->_iter != iter->_locations->end() &&
     iter->_inner != iter->_iter->end()) {
    *location = iter->_inner->c_str();
    (*chunk) = iter->_iter - iter->_locations->begin();

    // Take the left over chunk value before moving iter
    int left = iter->_locations->size() - (iter->_iter - iter->_locations->begin());

    iter->_inner++;
    // Detect if we've incremented to the end of inner and move iter
    if(iter->_inner == iter->_iter->end()) {
      iter->_iter++;

      // Only set begin on inner if we are not at the end outer.
      if(iter->_iter != iter->_locations->end()) {
        iter->_inner = iter->_iter->begin();
      }
    }

    // Return the number of blocks left to iterate
    return left;
  }

  // We are done: dealloc and return that there are zero left.
  qfs_locations_iter_free(iter);
  return 0;
}
