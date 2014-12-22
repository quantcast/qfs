//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/11/01
// Author: Blake Lewis (Kosmix Corp.)
//
// Copyright 2008-2012 Quantcast Corp.
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

#include "libclient/KfsClient.h"
#include "common/Properties.h"

#include <fuse.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

using std::string;
using std::vector;
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

static KfsClient* client;

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
fuse_getattr(const char *path, struct stat *s)
{
    KfsFileAttr attr;
    int status = client->Stat(path, attr);
    if (status < 0)
        return status;
    attr.ToStat(*s);
    return 0;
}

static int
fuse_fgetattr(const char *path, struct stat *s, struct fuse_file_info *finfo)
{
    return fuse_getattr(path, s);
}

static int
fuse_mkdir(const char *path, mode_t mode)
{
    return client->Mkdir(path, mode2kfs_mode(mode));
}

static int
fuse_unlink(const char *path)
{
    return client->Remove(path);
}

static int
fuse_rmdir(const char *path)
{
    return client->Rmdir(path);
}

static int
fuse_rename(const char *src, const char *dst)
{
    return client->Rename(src, dst, false);
}

static int
fuse_ftruncate(const char *path, off_t size, struct fuse_file_info *finfo)
{
    return client->Truncate(finfo->fh, size);
}

static int
fuse_truncate(const char *path, off_t size)
{
    return client->Truncate(path, size);
}

static int
fuse_open(const char *path, struct fuse_file_info *finfo)
{
    int fd = client->Open(path, finfo->flags);
    if (fd < 0)
        return fd;
    finfo->fh = fd;
    return 0;
}

static int
fuse_create(const char *path, mode_t mode, struct fuse_file_info *finfo)
{
    const int       numReplicas        = 3;
    const bool      exclusive          = false;
    const int       numStripes         = 0;
    const int       numRecoveryStripes = 0;
    const int       stripeSize         = 0;
    const int       stripedType        = KFS_STRIPED_FILE_TYPE_NONE;
    const bool      forceTypeFlag      = true;
    const kfsMode_t kfs_mode           =
        (kfsMode_t)mode & Permissions::kAccessModeMask;
    int fd = client->Create(path,
        numReplicas,
        exclusive,
        numStripes,
        numRecoveryStripes,
        stripeSize,
        stripedType,
        forceTypeFlag,
        kfs_mode
    );
    if (fd < 0)
        return fd;
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
fuse_write(const char *path, const char *buf, size_t nbytes, off_t off,
           struct fuse_file_info *finfo)
{
    return (int)client->PWrite(finfo->fh, off, buf, nbytes);
}

static int
fuse_flush(const char *path, struct fuse_file_info *finfo)
{
    // NO!
    return 0;
}

static int
fuse_release(const char *path, struct fuse_file_info *finfo)
{
    return client->Close(finfo->fh);
}

static int
fuse_fsync(const char *path, int flags, struct fuse_file_info *finfo)
{
    return client->Sync(finfo->fh);
}

static int
fuse_opendir(const char *path, struct fuse_file_info *finfo)
{
    if (!client->IsDirectory(path))
        return -ENOTDIR;
    return 0;
}

static int
fuse_readdir(const char *path, void *buf,
             fuse_fill_dir_t filler, off_t offset,
             struct fuse_file_info *finfo)
{
    vector <KfsFileAttr> contents;
    int status = client->ReaddirPlus(path, contents);
    if (status < 0)
        return status;
    int n = contents.size();
    for (int i = 0; i < n; i++) {
        struct stat s;
        contents[i].ToStat(s);
        if (filler(buf, contents[i].filename.c_str(), &s, 0) != 0) {
            break;
        }
    }
    return 0;
}

static int
fuse_releasedir(const char *path, struct fuse_file_info *finfo)
{
    return 0;
}

static int
fuse_access(const char *path, int mode)
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
fuse_chmod(const char *path, mode_t mode)
{
    return client->Chmod(path, mode2kfs_mode(mode));
}

static int
fuse_chown(const char *path, uid_t user, gid_t group)
{
    return client->Chown(path,
        user  == (uid_t)-1 ? kKfsUserNone  : (kfsUid_t)user,
        group == (gid_t)-1 ? kKfsGroupNone : (kfsGid_t)group
    );
}

struct fuse_operations ops = {
        fuse_getattr,
        NULL,                   /* readlink */
        NULL,                   /* getdir */
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
        NULL,                   /* utime */
        fuse_open,
        fuse_read,
        fuse_write,
        NULL,                   /* statfs */
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
        fuse_ftruncate,         /* ftruncate */
        fuse_fgetattr,          /* fgetattr */
};

struct fuse_operations ops_readonly = {
        fuse_getattr,
        NULL,                   /* readlink */
        NULL,                   /* getdir */
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
        NULL,                   /* utime */
        fuse_open,
        fuse_read,
        NULL,                   /* write */
        NULL,                   /* statfs */
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
        NULL,                   /* ftruncate */
        fuse_fgetattr,          /* fgetattr */
};

static void
fatal(const char *fmt, ...)
{
    va_list arg;

    fflush(stdout);

    va_start(arg, fmt);
    vfprintf(stderr, fmt, arg);
    va_end(arg);

    if (errno != 0)
        fprintf(stderr, " %s", strerror(errno));
    fprintf(stderr, "\n");

    exit(2);
}

static void
initkfs(char* addr, const string& cfg_file, const string& cfg_props)
{
    char *cp;

    if (! (cp = strrchr(addr, ':'))) {
        fatal("bad address: %s", addr);
        return;
    }
    string host(addr, cp - addr);
    int const  port  = atoi(cp + 1);
    const char delim = (char)'=';
    if (cfg_file.empty()) {
        if (cfg_props.empty()) {
            client = KFS::Connect(host, port);
        } else {
            Properties props;
            if (props.loadProperties(
                    cfg_props.data(), cfg_props.size(), delim) == 0) {
                client = KFS::Connect(host, port, &props);
            }
        }
    } else {
        Properties props;
        if (props.loadProperties(
                cfg_file.c_str(), delim) == 0) {
            client = KFS::Connect(host, port, &props);
        }
    }
    if (! client) {
        fatal("connect: %s:%d", host.c_str(), port);
    }
}

static struct fuse_args*
get_fs_args(struct fuse_args* args)
{
#ifdef KFS_OS_NAME_DARWIN
    return NULL;
#else
    if (! args) {
        return 0;
    }
    args->argc = 2;
    args->argv = (char**)calloc(sizeof(char*), args->argc + 1);
    args->argv[0] = strdup("qfs_fuse");
    args->argv[1] = strdup("-obig_writes");
    args->allocated = 1;
    return args;
#endif
}

static struct fuse_args*
get_mount_args(struct fuse_args* args, const char* options)
{
    if (!args) {
        return NULL;
    }

    args->argc = 2;
    args->argv = (char**)calloc(sizeof(char*), args->argc + 1);
    args->argv[0] = strdup("unused_arg0");
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
        } else if (token != "rw") {
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
        fatal("fork:");
    }
    if (pid == 0) {
        initkfs(kfs_host_address, cfg_file, cfg_props);

        struct fuse_args fs_args;
        struct fuse_args mnt_args;

        struct fuse_chan* ch = NULL;
        ch = fuse_mount(mountpoint, get_mount_args(&mnt_args, options));
        if (ch == NULL) {
            delete client;
            fatal("fuse_mount: %s:", mountpoint);
        }

        struct fuse* fuse = NULL;
        fuse = fuse_new(ch, get_fs_args(&fs_args),
                        (readonly ? &ops_readonly : &ops),
                        (readonly ? sizeof(ops_readonly) : sizeof(ops)),
                        NULL);
        if (fuse == NULL) {
            fuse_unmount(mountpoint, ch);
            delete client;
            fatal("fuse_new:");
        }

        fuse_loop_mt(fuse);
        fuse_unmount(mountpoint, ch);
        fuse_destroy(fuse);
        delete client;
    }
    return;
}

static void
usage(int e, const char* name)
{
    //Undocumented option: 'rrw'. See massage_options() above.
    fprintf(stderr,
        "usage: %s qfshost mountpoint [-o opt1[,opt2..]]\n"
        "       eg: %s 127.0.0.1:20000 "
        "/mnt/qfs -o allow_other,ro,cfg=FILE:client_config_file.prp\n",
        name, name
    );
    exit(e);
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
      usage(0, name);
    }
    if (argc < 2) {
        usage(1, name);
    }
    // Default is readonly mount,private mount.
    string options("-oro");
    bool readonly = true;
    string cfg_file;
    string cfg_props;
    if (argc > 2) {
        if (massage_options(argv + 2, argc - 2, &options, &readonly,
                cfg_file, cfg_props) < 0) {
            usage(1, name);
        }
    }

    //setsid(); // detach from console

    initfuse(argv[0], argv[1], options.c_str(), readonly,
        fork_flag, cfg_file, cfg_props);

    return 0;
}
