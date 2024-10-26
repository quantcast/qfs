//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/11/01
// Author: Blake Lewis (Kosmix Corp.)
//
// Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
// Copyright 2006 Kosmix Corp.
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
// Kfs fuse.
// Default is to mount read only, as non sequential write isn't supported with
// files created with Reed-Solomon recovery, as well as simultaneous read and
// write (O_RDWR) into the same file by a single writer.
//
//----------------------------------------------------------------------------

#ifndef FUSE_USE_VERSION
#   define FUSE_USE_VERSION 26
#endif

#include "libclient/KfsClient.h"
#include "common/Properties.h"
#include "common/MsgLogger.h"

#include <fuse.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

#include <iomanip>

/* Do not compare FUSE_USE_VERSION with FUSE_MAKE_VERSION as the later on 3.0
 * multiplies major version by 100, but on 2.0 by 10, and the conditionals int
 * the fuse includes appears to be use using 2.0 convention with
 * FUSE_USE_VERSION
 */
#if FUSE_USE_VERSION < 30
#   define QFS_FUSE_USE_MAJOR_VERSION 2
#   define FUSE2_API_ARG(x) , x
#   define FUSE3_API_ARG(x)
#   define QFS_FUSE_LOOP_MT(x) fuse_loop_mt(x)
#   if FUSE_USE_VERSION > 24
#       define FUSE_UMOUNT_ARG(x) , x
#   else
#       define FUSE_UMOUNT_ARG(x)
#   endif
#else
#   define QFS_FUSE_USE_MAJOR_VERSION 3
#   define FUSE_UMOUNT_ARG(x)
#   define FUSE2_API_ARG(x)
#   define FUSE3_API_ARG(x) , x
#   if FUSE_USE_VERSION < 32
#       define QFS_FUSE_LOOP_MT(x) fuse_loop_mt(x, 0)
#   else
#       define QFS_FUSE_LOOP_MT(x) fuse_loop_mt(x, NULL)
#   endif
#endif

using std::string;
using std::vector;
using std::hex;
using std::dec;
using std::oct;
using KFS::KfsClient;
using KFS::KfsFileAttr;
using KFS::kfsMode_t;
using KFS::kfsUid_t;
using KFS::kfsGid_t;
using KFS::kKfsUserNone;
using KFS::kKfsGroupNone;
using KFS::kKfsModeUndef;
using KFS::Permissions;
using KFS::KFS_STRIPED_FILE_TYPE_NONE;
using KFS::Properties;
using KFS::kfsSTier_t;
using KFS::MsgLogger;
using KFS::CHUNKSIZE;

static KfsClient* client;

static struct
{
    int        numReplicas;
    int        numStripes;
    int        numRecoveryStripes;
    int        stripeSize;
    int        stripedType;
    kfsSTier_t minSTier;
    kfsSTier_t maxSTier;
} sFileCreateParams;

static bool sReadOnlyFlag;

static inline kfsMode_t
mode2kfs_mode(mode_t mode)
{
    kfsMode_t km = (kfsMode_t)mode & Permissions::kAccessModeMask;
#ifdef S_ISVTX
    if ((mode & S_ISVTX) != 0) {
        km |= Permissions::kStickyBit;
    }
#endif
    return km;
}

static int
fuse_getattr(const char* path, struct stat* s
    FUSE3_API_ARG(struct fuse_file_info*))
{
    KfsFileAttr attr;
    int status = client->Stat(path, attr);
    if (status < 0) {
        return status;
    }
    attr.ToStat(*s);
    return 0;
}

#if QFS_FUSE_USE_MAJOR_VERSION < 3
static int
fuse_fgetattr(const char* path, struct stat* s,
        struct fuse_file_info* /* finfo */)
{
    return fuse_getattr(path, s);
}
#endif

static int
fuse_mkdir(const char* path, mode_t mode)
{
    return client->Mkdir(path, mode2kfs_mode(mode));
}

static int
fuse_unlink(const char* path)
{
    return client->Remove(path);
}

static int
fuse_rmdir(const char* path)
{
    return client->Rmdir(path);
}

static int
fuse_rename(const char* src, const char* dst FUSE3_API_ARG(unsigned int flags))
{
    return client->Rename(src, dst FUSE2_API_ARG(false)
        FUSE3_API_ARG(flags != RENAME_NOREPLACE));
}

#if QFS_FUSE_USE_MAJOR_VERSION < 3
static int
fuse_ftruncate(const char* path, off_t size, struct fuse_file_info* finfo)
{
    KFS_LOG_STREAM_DEBUG << "qfs_fuse"
        " ftruncate:" << (path ? path : "") <<
        " fd: "       << finfo->fh <<
        " size: "     << size <<
    KFS_LOG_EOM;
    return client->Truncate(finfo->fh, size);
}
#endif

static int
fuse_truncate(const char* path, off_t size
    FUSE3_API_ARG(struct fuse_file_info*))
{
    KFS_LOG_STREAM_DEBUG << "qfs_fuse"
        " truncate:" << (path ? path : "") <<
        " size: "    << size <<
    KFS_LOG_EOM;
    return client->Truncate(path, size);
}

static bool
IsStripedOrObjectStoreFile(int stripedType, int numReplicas)
{
    return (KFS_STRIPED_FILE_TYPE_NONE != stripedType ||
        numReplicas <= 0);
}

static bool
CanWriteFile(int stripedType, int numReplicas)
{
#ifndef QFS_FUSE_NO_PARTIAL_RS_AND_OBJ_STORE_SUPPORT
    return true;
#else
    // Striped and object store file, need to be explicitly closed only once,
    // which is impossible given fuse flush / release semantics.
    return (! IsStripedOrObjectStoreFile(stripedType, numReplicas));
#endif
}

static int
CanWrite(const char* path)
{
    KfsFileAttr attr;
    const int   res = client->Stat(path, attr);
    if (0 == client->Stat(path, attr)) {
        if (attr.isDirectory) {
            return -EISDIR;
        }
        if (! CanWriteFile(attr.striperType, attr.numReplicas)) {
            return -EPERM;
        }
    }
    return res;
}

static int
CanOpen(const char* path, int flags)
{
    if (sReadOnlyFlag) {
        return ((0 != (flags & (O_WRONLY | O_RDWR | O_APPEND))) ? -EROFS : 0);
    }
    if (0 != (flags & O_APPEND)) {
        // Do not allow append, as it has different than POSIX semantics.
        return -EPERM;
    }
    const int res = CanWrite(path);
    if (-ENOENT == res) {
        if (! CanWriteFile(sFileCreateParams.stripedType,
                sFileCreateParams.numReplicas)) {
            return -EPERM;
        }
    }
    return res;
}

static int
fuse_open(const char* path, struct fuse_file_info* finfo)
{
    int res = CanOpen(path, finfo->flags);
    if (res < 0 && -ENOENT != res) {
        return res;
    }
    const kfsMode_t kfs_mode = 0666;
    const int fd = client->Open(path,
        (finfo->flags &
            (sReadOnlyFlag ? ~((int)O_TRUNC | O_CREAT) : finfo->flags)),
        sFileCreateParams.numStripes,
        sFileCreateParams.numRecoveryStripes,
        sFileCreateParams.stripeSize,
        sFileCreateParams.stripedType,
        kfs_mode,
        sFileCreateParams.minSTier,
        sFileCreateParams.maxSTier
    );
    if (fd < 0) {
        return fd;
    }
    // Meta server can override file type, -- check create result.
    if (-ENOENT == res && (res = CanWrite(path)) < 0) {
        client->Close(fd);
        return res;
    }
    KFS_LOG_STREAM_DEBUG << "qfs_fuse"
        " open: "  << (path ? path : "") <<
        " fd: "    << fd <<
        " flags: " << oct << finfo->flags << dec <<
    KFS_LOG_EOM;
    finfo->fh = fd;
    return 0;
}

static int
fuse_create(const char* path, mode_t mode, struct fuse_file_info* finfo)
{
    int res = CanOpen(path, finfo->flags);
    if (res < 0 && -ENOENT != res) {
        return res;
    }
    const bool      exclusive     = 0 != (finfo->flags & O_EXCL);
    const bool      forceTypeFlag = false;
    const kfsMode_t kfs_mode      =
        (kfsMode_t)mode & Permissions::kAccessModeMask;
    const int fd = client->Create(path,
        sFileCreateParams.numReplicas,
        exclusive,
        sFileCreateParams.numStripes,
        sFileCreateParams.numRecoveryStripes,
        sFileCreateParams.stripeSize,
        sFileCreateParams.stripedType,
        forceTypeFlag,
        kfs_mode,
        sFileCreateParams.minSTier,
        sFileCreateParams.maxSTier
    );
    if (fd < 0) {
        return fd;
    }
    // Meta server can override file type, -- check create result.
    if (-ENOENT == res && (res = CanWrite(path)) < 0) {
        client->Close(fd);
        return res;
    }
    KFS_LOG_STREAM_DEBUG << "qfs_fuse"
        " create: " << (path ? path : "") <<
        " fd: "     << fd <<
        " flags: "  << oct << finfo->flags << dec <<
    KFS_LOG_EOM;
    finfo->fh = fd;
    return 0;
}

static int
fuse_read(const char *path, char *buf, size_t nbytes, off_t off,
          struct fuse_file_info *finfo)
{
    return (int)client->PRead(finfo->fh, off, buf, nbytes);
}

static int
fuse_write(const char *path, const char* buf, size_t nbytes, off_t off,
           struct fuse_file_info* finfo)
{
    return (int)client->PWrite(finfo->fh, off, buf, nbytes);
}

static int
fuse_flush(const char* path, struct fuse_file_info* finfo)
{
    if (! sReadOnlyFlag && finfo) {
        KFS_LOG_STREAM_DEBUG << "qfs_fuse"
            " flush: " << (path ? path : "") <<
            " fd: "    << finfo->fh <<
        KFS_LOG_EOM;
        return client->Sync(finfo->fh);
    }
    return 0;
}

static int
fuse_release(const char* path, struct fuse_file_info* finfo)
{
    KFS_LOG_STREAM_DEBUG << "qfs_fuse"
        " release: " << (path ? path : "") <<
        " fd: "      << finfo->fh <<
    KFS_LOG_EOM;
    return client->Close(finfo->fh);
}

static int
fuse_fsync(const char* path, int /* flags */,
        struct fuse_file_info* finfo)
{
    KFS_LOG_STREAM_DEBUG << "qfs_fuse"
        " fsync:" << (path ? path : "") <<
        " fd: "   << finfo->fh <<
    KFS_LOG_EOM;
    return client->Sync(finfo->fh);
}

static int
fuse_opendir(const char* path, struct fuse_file_info* /* finfo */)
{
    if (!client->IsDirectory(path)) {
        return -ENOTDIR;
    }
    return 0;
}

static int
fuse_readdir(const char* path, void* buf,
             fuse_fill_dir_t filler, off_t /* offset */,
             struct fuse_file_info* /* finfo */
             FUSE3_API_ARG(enum fuse_readdir_flags))
{
    vector <KfsFileAttr> contents;
    int status = client->ReaddirPlus(path, contents);
    if (status < 0) {
        return status;
    }
    int n = contents.size();
    for (int i = 0; i < n; i++) {
        struct stat s;
        contents[i].ToStat(s);
        if (filler(buf, contents[i].filename.c_str(), &s, 0
                    FUSE3_API_ARG(FUSE_FILL_DIR_PLUS)) != 0) {
            break;
        }
    }
    return 0;
}

static int
fuse_releasedir(const char* /* path */, struct fuse_file_info* /* finfo */)
{
    return 0;
}

static int
fuse_access(const char* path, int mode)
{
    KfsFileAttr attr;
    int status = client->Stat(path, attr);
    if (status != 0) {
        return status;
    }
    if (attr.mode == kKfsModeUndef || mode == F_OK) {
        return 0;
    }
    if (((mode & R_OK) != 0 && (attr.mode & 0400) == 0) ||
            ((mode & W_OK) != 0 && (attr.mode & 0200) == 0) ||
            ((mode & X_OK) != 0 && (attr.mode & 0100) == 0)) {
        return -EACCES;
    }
    return 0;
}

static int
fuse_chmod(const char* path, mode_t mode FUSE3_API_ARG(struct fuse_file_info*))
{
    return client->Chmod(path, mode2kfs_mode(mode));
}

static int
fuse_chown(const char* path, uid_t user, gid_t group
    FUSE3_API_ARG(struct fuse_file_info*))
{
    return client->Chown(path,
        user  == (uid_t)-1 ? kKfsUserNone  : (kfsUid_t)user,
        group == (gid_t)-1 ? kKfsGroupNone : (kfsGid_t)group
    );
}

static int
fuse_statfs(const char* path, struct statvfs* stat)
{
    KfsFileAttr attr;
    int res = path ? client->Stat(path, attr) : 0;
    if (res < 0) {
        return res;
    }
    if (! stat) {
        return 0;
    }
    memset(stat, 0, sizeof(*stat));
    if (! path || '/' != path[0] || ! path[1]) {
        res = client->Stat("/", attr);
        if (res < 0) {
            return path ? 0 : res;
        }
    }
    stat->f_bsize = CHUNKSIZE;
    if (sizeof(stat->f_blocks) < 8) {
        stat->f_frsize  = CHUNKSIZE;
        stat->f_blocks  = (attr.fileSize + CHUNKSIZE - 1) / CHUNKSIZE;
    } else {
        stat->f_frsize  = 1;
        stat->f_blocks  = attr.fileSize;
    }
    stat->f_files   = attr.fileCount() + attr.dirCount();
    stat->f_namemax = 256;
    if (sReadOnlyFlag) {
        stat->f_flag |= ST_RDONLY;
    }
    return 0;
}

struct fuse_operations ops = {
        fuse_getattr,
        NULL,                   /* readlink */
#if QFS_FUSE_USE_MAJOR_VERSION < 3
        NULL,                   /* getdir */
#endif
        NULL,                   /* mknod */
        fuse_mkdir,
        fuse_unlink,
        fuse_rmdir,
        NULL,                   /* symlink */
        fuse_rename,
        NULL,                   /* link */
        fuse_chmod,             /* chmod */
        fuse_chown,             /* chown */
        fuse_truncate,
#if QFS_FUSE_USE_MAJOR_VERSION < 3
        NULL,                   /* utime */
#endif
        fuse_open,
        fuse_read,
        fuse_write,
        fuse_statfs,            /* statfs */
        fuse_flush,             /* flush */
        fuse_release,           /* release */
        fuse_fsync,             /* fsync */
        NULL,                   /* setxattr */
        NULL,                   /* getxattr */
        NULL,                   /* listxattr */
        NULL,                   /* removexattr */
        fuse_opendir,
        fuse_readdir,
        fuse_releasedir,
        NULL,                   /* fsyncdir */
        NULL,                   /* init */
        NULL,                   /* destroy */
        fuse_access,            /* access */
        fuse_create,            /* create */
#if QFS_FUSE_USE_MAJOR_VERSION < 3
        fuse_ftruncate,         /* ftruncate */
        fuse_fgetattr,          /* fgetattr */
#else
        NULL,                   /* lock */
        NULL,                   /* utimens */
        NULL,                   /* bmap */
        NULL,                   /* ioctl */
        NULL,                   /* poll */
        NULL,                   /* write_buf */
        NULL,                   /* read_buf */
        NULL,                   /* flock */
        NULL,                   /* fallocate */
        NULL,                   /* copy_file_range */
        NULL,                   /* lseek */
#endif
};

struct fuse_operations ops_readonly = {
        fuse_getattr,
        NULL,                   /* readlink */
#if QFS_FUSE_USE_MAJOR_VERSION < 3
        NULL,                   /* getdir */
#endif
        NULL,                   /* mknod */
        NULL,                   /* mkdir */
        NULL,                   /* unlink */
        NULL,                   /* rmdir */
        NULL,                   /* symlink */
        NULL,                   /* rename */
        NULL,                   /* link */
        NULL,                   /* chmod */
        NULL,                   /* chown */
        NULL,                   /* truncate */
#if QFS_FUSE_USE_MAJOR_VERSION < 3
        NULL,                   /* utime */
#endif
        fuse_open,
        fuse_read,
        NULL,                   /* write */
        fuse_statfs,            /* statfs */
        NULL,                   /* flush */
        fuse_release,           /* release */
        NULL,                   /* fsync */
        NULL,                   /* setxattr */
        NULL,                   /* getxattr */
        NULL,                   /* listxattr */
        NULL,                   /* removexattr */
        fuse_opendir,
        fuse_readdir,
        NULL,                   /* releasedir */
        NULL,                   /* fsyncdir */
        NULL,                   /* init */
        NULL,                   /* destroy */
        fuse_access,            /* access */
        NULL,                   /* create */
#if QFS_FUSE_USE_MAJOR_VERSION < 3
        NULL,                   /* ftruncate */
        fuse_fgetattr,          /* fgetattr */
#else
        NULL,                   /* lock */
        NULL,                   /* utimens */
        NULL,                   /* bmap */
        NULL,                   /* ioctl */
        NULL,                   /* poll */
        NULL,                   /* write_buf */
        NULL,                   /* read_buf */
        NULL,                   /* flock */
        NULL,                   /* fallocate */
        NULL,                   /* copy_file_range */
        NULL,                   /* lseek */
#endif
};

static void
fatal(int status, const char* fmt, ...)
{
    va_list arg;

    fflush(stdout);

    va_start(arg, fmt);
    vfprintf(stderr, fmt, arg);
    va_end(arg);

    if (status != 0) {
        fprintf(stderr, " %s", strerror(status));
    }
    fprintf(stderr, "\n");
    exit(2);
}

static void
SetDefaults(Properties& props)
{
    struct { const char* name; const char* value; } const kDefaults[] = {
        { "client.fullSparseFileSupport", "1" },
        { 0, 0 }
    };
    for (size_t i = 0; kDefaults[i].name; i++) {
        props.setValue(
            Properties::String(kDefaults[i].name),
            Properties::String(kDefaults[i].value)
        );
    }
}

static void
initkfs(char* addr, const string& cfg_file, const string& cfg_props)
{
    char *cp;

    if (! (cp = strrchr(addr, ':'))) {
        fatal(0, "bad address: %s", addr);
        return;
    }
    string host(addr, cp - addr);
    int const  port  = atoi(cp + 1);
    const char delim = (char)'=';
    if (cfg_file.empty()) {
        if (cfg_props.empty()) {
            client = KFS::Connect(host, port);
            if (client) {
                client->SetDefaultFullSparseFileSupport(true);
            }
        } else {
            Properties props;
            SetDefaults(props);
            if (props.loadProperties(
                    cfg_props.data(), cfg_props.size(), delim) == 0) {
                client = KFS::Connect(host, port, &props);
            }
        }
    } else {
        Properties props;
        SetDefaults(props);
        if (props.loadProperties(cfg_file.c_str(), delim) == 0) {
            client = KFS::Connect(host, port, &props);
        }
    }
    if (client) {
        if (! sReadOnlyFlag) {
            client->SetCloseWriteOnRead(true);
        }
    } else {
        fatal(0, "connect: %s:%d", host.c_str(), port);
    }
}

#if QFS_FUSE_USE_MAJOR_VERSION < 3
static struct fuse_args*
get_fs_args(struct fuse_args* args)
{
    if (! args) {
        return 0;
    }
    args->argc = 1;
    args->argv = (char**)calloc(sizeof(char*), args->argc + 1);
    args->argv[0] = strdup("qfs_fuse");
#if defined(FUSE_MAJOR_VERSION)
#if FUSE_MAJOR_VERSION < 3
    args->argc++;
    args->argv[1] = strdup("-obig_writes");
#endif
#endif
    args->allocated = 1;
    return args;
}
#endif

static struct fuse_args*
get_mount_args(struct fuse_args* args, const char* options)
{
    if (!args) {
        return NULL;
    }

    args->argc = 2;
    args->argv = (char**)calloc(sizeof(char*), args->argc + 1);
    args->argv[0] = strdup("qfs_fuse");
    args->argv[1] = strdup(options);
    args->allocated = 1;
    return args;
}

/*
 * Run through the -o OPTIONS and interpret it as writable only if 'rrw' is
 * explicitly specified. We use 'rrw' instead of 'rw' because a 'default'
 * mount option entry in the fstab calls us with 'rw', but we want the default
 * behavior to be readonly.
 */
static int
massage_options(
    char** opt_argv, int opt_argc, string* options, bool* readonly,
    string& out_cfg_file, string& out_cfg_props)
{
    if (!opt_argv || !readonly || !options) {
        return -1;
    }
    if (opt_argc <= 0 || opt_argc > 2 || strncmp(opt_argv[0], "-o", 2)) {
        return -1;
    }
    *readonly = true;
    string cmdline = opt_argc == 1 ? opt_argv[0] + 2 : opt_argv[1];

    vector<string> opts;
    const string delim = " ,";
    const string create("create=");
    for(size_t start = 0; ;) {
        start = cmdline.find_first_not_of(delim, start);
        if (start == string::npos){
            break;
        }
        const size_t end   = cmdline.find_first_of(delim, start);
        const string token = cmdline.substr(start,
            end == string::npos ? string::npos : end - start);
        if (token == "rrw") {
            *readonly = false;
            opts.push_back("rw");
        } else if (0 == token.compare(0, create.size(), create)) {
           if (0 != KfsClient::ParseCreateParams(
                   token.c_str() + create.size(),
                   sFileCreateParams.numReplicas,
                   sFileCreateParams.numStripes,
                   sFileCreateParams.numRecoveryStripes,
                   sFileCreateParams.stripeSize,
                   sFileCreateParams.stripedType,
                   sFileCreateParams.minSTier,
                   sFileCreateParams.maxSTier)) {
                printf("invalid file create parameters: %s", token.c_str());
                return -1;
            }
        } else if ("rw" != token) {
            opts.push_back(token);
        }
        if (end == string::npos) {
            break;
        }
        start = end;
    }
    if (*readonly) {
        *options = "-oro";
    } else {
        *options = "-orw";
        if (! CanWriteFile(sFileCreateParams.stripedType,
                sFileCreateParams.numReplicas)) {
            fprintf(stderr,
                "The specified file type is not supported by QFS fuse"
                "in read write mode.\n");
            return -1;
        }
        if (IsStripedOrObjectStoreFile(sFileCreateParams.stripedType,
                sFileCreateParams.numReplicas)) {
            fprintf(stderr,
                "Warning: object store and RS file types read write"
                " support in QFS fuse is incomplete.\n"
                "Files of this type might not be completely written"
                " synced, unless file read is performed after file write"
                " from the same QFS fuse process.\n"
            );
            fflush(stderr);
        }
    }
    const string cfg("cfg=");
    const string cfg_file("cfg=FILE:");
    while (! opts.empty()) {
        const string token = opts.back();
        opts.pop_back();
        if (token == "rw" || token == "ro") {
            continue;
        }
        if (cfg.length() <= token.length() &&
                token.compare(0, cfg.length(), cfg) == 0) {
            if (cfg_file.length() <= token.length() &&
                    token.compare(0, cfg_file.length(), cfg_file) == 0) {
                out_cfg_file = token.substr(cfg_file.length());
            } else {
                out_cfg_props += token.substr(cfg.length());
                out_cfg_props += "\n";
            }
            continue;
        }
        options->append(",");
        options->append(token);
    }
    return 0;
}

/*
 * Fork and do the work in the child so that init will reap the process.
 * Do the KfsClient connection, fuse mount, and so on in the child process.
 */
static void
initfuse(char* kfs_host_address, const char* mountpoint,
         const char* options, bool readonly, bool fork_flag,
         const string& cfg_file, const string& cfg_props)
{
    int pid = fork_flag ? fork() : 0;
    if (pid < 0) {
        fatal(errno, "fork:");
    }
    if (pid == 0) {
        initkfs(kfs_host_address, cfg_file, cfg_props);

        struct fuse_args fs_args;

#if QFS_FUSE_USE_MAJOR_VERSION < 3
        struct fuse_args mnt_args;
        struct fuse_chan* const ch =
            fuse_mount(mountpoint, get_mount_args(&mnt_args, options));
        if (ch == NULL) {
            const int err = errno;
            delete client;
            fatal(err, "fuse_mount: %s:", mountpoint);
        }
#endif
        struct fuse* const fuse = fuse_new(
#if QFS_FUSE_USE_MAJOR_VERSION < 3
            ch,
            get_fs_args(&fs_args),
#else
            get_mount_args(&fs_args, options),
#endif
            (readonly ? &ops_readonly : &ops),
            (readonly ? sizeof(ops_readonly) : sizeof(ops)),
            NULL
        );
        if (fuse == NULL) {
            const int err = errno;
#if QFS_FUSE_USE_MAJOR_VERSION < 3
            fuse_unmount(mountpoint FUSE_UMOUNT_ARG(ch));
#endif
            delete client;
            fatal(err, "fuse_new:");
        }
#if QFS_FUSE_USE_MAJOR_VERSION > 2
        if (fuse_mount(fuse, mountpoint)) {
            const int err = errno;
            fuse_destroy(fuse);
            delete client;
            fatal(err, "fuse_mount: %s:", mountpoint);
        }
#endif
        sReadOnlyFlag = readonly;
#ifndef KFS_OS_NAME_SUNOS
        if (! readonly) {
            QFS_FUSE_LOOP_MT(fuse);
        } else
#endif
        {
            fuse_loop(fuse);
        }
        fuse_unmount(
#if QFS_FUSE_USE_MAJOR_VERSION < 3
            mountpoint FUSE_UMOUNT_ARG(ch)
#else
            fuse
#endif
        );
        fuse_destroy(fuse);
        delete client;
    }
    return;
}

static void
usage(const char* name)
{
    //Undocumented option: 'rrw'. See massage_options() above.
    fprintf(stderr,
        "usage: %s [-f][-h] qfshost mountpoint [-o opt1[,opt2..]]\n"
        "       eg: %s 127.0.0.1:20000 "
        "/mnt/qfs -o allow_other,ro,cfg=FILE:client_config_file.prp\n"
        "       rrw option can be used to enable read write mode, however, this"
        " mode has *very* limited support: only replicated files write"
        " is supported, file append is not supported.\n"
        "       File system IO in read write mode is serialized.\n"
        " -f -- do not fork, remain foreground process (debugging).\n"
        " -g -- help, emit this message and exit.\n"
        , name, name
    );
}

int
main(int argc, char **argv)
{
    const char* name = "qfs_fuse";
    if (0 < argc) {
        name = argv[0];
        argc--;
        argv++;
    }
    bool fork_flag = true;
    if (0 < argc && strcmp(argv[0], "-f") == 0) {
        argc--;
        argv++;
        fork_flag = false;
    }
    if (0 < argc && (
            strcmp("-h", argv[0]) == 0 ||
            strcmp("-help", argv[0]) == 0 ||
            strcmp("--help", argv[0]) == 0)) {
        usage(name);
        return 0;
    }
    if (argc < 2) {
        usage(name);
        return 1;
    }
    // Init defaults, and set number of replicas to 3.
    errno = 0;
    if (0 != KfsClient::ParseCreateParams(
            "",
            sFileCreateParams.numReplicas,
            sFileCreateParams.numStripes,
            sFileCreateParams.numRecoveryStripes,
            sFileCreateParams.stripeSize,
            sFileCreateParams.stripedType,
            sFileCreateParams.minSTier,
            sFileCreateParams.maxSTier)) {
        fprintf(stderr, "internal error:"
                " create parameters initialization failure");
        return 2;
    }
    sFileCreateParams.numReplicas = 3;
    // Default is readonly mount,private mount.
    string options("-oro");
    bool readonly = true;
    string cfg_file;
    string cfg_props;
    if (argc > 2) {
        if (massage_options(argv + 2, argc - 2, &options, &readonly,
                cfg_file, cfg_props) < 0) {
            usage(name);
            return 1;
        }
    }

    //setsid(); // detach from console

    initfuse(argv[0], argv[1], options.c_str(), readonly,
        fork_flag, cfg_file, cfg_props);

    return 0;
}
