/*
 * ---------------------------------------------------------- -*- Mode: C -*-
 * $Id$
 *
 * Created 2010/5/25
 * Author: Mike Ovsiannikov
 *
 * Copyright 2010 Quantcast Corp.
 *
 * This file is part of Kosmos File System (KFS).
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

#ifndef _LARGEFILE_SOURCE
#define _LARGEFILE_SOURCE
#endif
#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif
#ifndef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <limits.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/times.h>
#include <string.h>

#ifndef IOV_MAX
#define IOV_MAX 1024
#endif
#ifndef O_DIRECT
#define O_DIRECT 0
#endif
#ifndef O_NOATIME
#define O_NOATIME 0
#endif

/*
 * To makes xfs extents as large as possible:
 *
 * xfs_io -f -c 'resvsp 0 256m' -c 'truncate 256m' test.file
 * xfs_bmap test.file
 */

static int test(int argc, char** argv, int t)
{
    static struct iovec iov[IOV_MAX];
    const int     bs = 4 << 10;
    const ssize_t s  = bs * IOV_MAX;
    char*         p = (char*)mmap(
        0, bs * IOV_MAX, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    memset(p, 'a', s);
    int           i;
    for (i = 0; i < IOV_MAX; i++) {
        iov[i].iov_base = p;
        iov[i].iov_len  = bs;
    }
    for (i = 1; i < argc; i++) {
        const char* nm = "none";
        const double cs = (double)sysconf(_SC_CLK_TCK);
        struct tms ptms, ctms;
        clock_t pt, ct;
        off_t so = 16 << 10;
        off_t o = so;
        int k = 2;
        off_t fs = 10 << 20;
        int m;
        const int fd = open(argv[i],
            O_RDWR | O_CREAT | O_DIRECT | O_NOATIME, 0666);
        if (fd < 0) {
            perror(argv[i]);
            return 1;
        }
        if (lseek(fd, o, SEEK_SET) != o) {
            perror(argv[i]);
            return 1;
        }
        pt = times(&ptms);
        while (k > 0) {
            ssize_t n;
            switch (t) {
                case 0:
                    nm = "writev";
                    n = writev(fd, iov, IOV_MAX);
                    if (n != s) {
                        break;
                    }
                    if (lseek(fd, o, SEEK_SET) != o) {
                        break;
                    }
                    n = readv(fd, iov, IOV_MAX);
                    break;
                case 1:
                    nm = "write";
                    n = write(fd, iov[0].iov_base, s);
                    if (n != s) {
                        break;
                    }
                    if (lseek(fd, o, SEEK_SET) != o) {
                        break;
                    }
                    n = read(fd, iov[0].iov_base, s);
                    break;
                default:
                    n = 0;
                    break;
            }
            if (n != s) {
                perror(argv[i]);
                return 1;
            }
            for (m = 0; m < s && p[m] == 'a'; m++) {}
            if (m < s) {
                for (; m < s && p[m] != 'a'; m++) {
                    printf("mismatch at: %d %d %d %ld\n",
                        m, (int)p[m], (int)'a', (long)o);
                }
                return 1;
            }
            o += s;
            if (o >= fs) {
                o = so;
                if (lseek(fd, o, SEEK_SET) != o) {
                    perror(argv[i]);
                    return 1;
                }
                k--;
            }
        }
        ct = times(&ctms);
        {
            const double d = (ct != pt) ? ct - pt : 1e-9;
            printf("u: %6.2f%% s: %6.2f%% w: %6.2f s %6.2f MB/s %s\n",
                (ctms.tms_utime - ptms.tms_utime)/d * 100,
                (ctms.tms_stime - ptms.tms_stime)/d * 100,
                d / cs,
                4 * fs * cs / ((1<<20) * d),
                nm
            );
        }
        close(fd);
    }
    return 0;
}

int main(int argc, char** argv)
{
    int i, r = 0;
    for (i = 0; i < 1; i++) {
        r = test(argc, argv, i);
        if (r) {
            break;
        }
    }
    return r;
}
