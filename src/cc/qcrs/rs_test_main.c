/*---------------------------------------------------------- -*- Mode: C -*-----
 * $Id$
 *
 * Created 2010/07/24
 * Author: Dan Adkins
 *
 * Copyright 2010-2011 Quantcast Corp.
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
 * \file rs_table.h
 * \brief Reed Solomon encoder and decoder unit test.
 *
 *------------------------------------------------------------------------------
 */
#define _XOPEN_SOURCE 600

#include "rs.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

/* Return 0 if equal, -1 otherwise. */
static int
compare(int nblocks, int blocksize, void **x, void **y)
{
    int i;

    for (i = 0; i < nblocks; i++)
        if (memcmp(x[i], y[i], blocksize) != 0)
            return -1;
    return 0;
}

static void
mkrand(void *buf, int size)
{
    char *p;
    int i;

    p = buf;
    for (i = 0; i < size; i++)
        p[i] = rand();
}

void *data[RS_LIB_MAX_DATA_BLOCKS+3];
void *orig[RS_LIB_MAX_DATA_BLOCKS+3];

int main(int argc, char **argv)
{
    if (argc > 1 && (!strcmp(argv[1], "-h") || !strcmp(argv[1], "--help"))) {
        printf("Usage: %s [data blocks] [block size] [perf iterations]\n"
               "       This tests the Reed Solomon encoder and decoder.\n"
               "       0 < data blocks <= %d.\n"
               "       Use perf iterations for performance test.\n"
               "       Defaults: data blocks=%d, block size=%d\n", argv[0],
               RS_LIB_MAX_DATA_BLOCKS, RS_LIB_MAX_DATA_BLOCKS, (64 << 10));
        exit(0);
    }

    int i, j, k, n, m, err;
    const int N = argc > 1 ? atoi(argv[1]) : RS_LIB_MAX_DATA_BLOCKS;
    const int BLOCKSIZE = argc > 2 ? atoi(argv[2]) : (64 << 10);

    if (N <= 0 || N > RS_LIB_MAX_DATA_BLOCKS) {
        printf("0 < data blocks <= %d\n", RS_LIB_MAX_DATA_BLOCKS);
        return 1;
    }

    for (i = 0; i < N+3; i++) {
        if ((err = posix_memalign(data + i, 16, BLOCKSIZE)) ||
                (err = posix_memalign(orig + i, 16, BLOCKSIZE))) {
            printf("%s\n", strerror(err));
            return 1;
        }
        memset(data[i], 0, BLOCKSIZE);
    }

    if (argc > 3) {
        clock_t clk, tclk = 0;
        double  tbytes = 0;
        // Performance test.
        n = atoi(argv[3]);
        for (i = 0; i < N+3; i++)
            mkrand(data[i], BLOCKSIZE);
        clk = clock();
        for (i = 0; i < n; i++)
            rs_encode(N+3, BLOCKSIZE, data);
        clk = clock() - clk;
        printf("encode %.3e clocks %.3e sec %.3e bytes/sec\n",
            (double)clk, (double)clk/CLOCKS_PER_SEC,
            BLOCKSIZE * N * (double)CLOCKS_PER_SEC * n /
                ((double)clk > 0 ? (double)clk : 1e-10));
        for (i = N - (3 < N ? 3 : 0); i < N; i++) {
            for (j = i + 1; j < N + 3; j++) {
                for (k = j + 1; k < N + 3; k++) {
                    void* const p = data[k];
                    if (N <= k) {
                        data[k] = 0; /* do not encode */
                    }
                    clk = clock();
                    for (m = 0; m < n; m++)
                        rs_decode3(N + 3, BLOCKSIZE, i, j, k, data);
                    clk = clock() - clk;
                    data[k] = p;
                    printf("decode missing: %d,%d,%d"
                        " %.3e clocks %.3e sec %.3e bytes/sec\n",
                        i, j, k, (double)clk, (double)clk/CLOCKS_PER_SEC,
                        BLOCKSIZE * N * (double)CLOCKS_PER_SEC * n /
                            ((double)clk > 0 ? (double)clk : 1e-10));
                    tbytes += (double)BLOCKSIZE * N * n;
                    tclk += clk;
                    if (k < N) {
                        break;
                    }
                }
                if (j < N) {
                    break;
                }
            }
            if (i + 3 < N) {
                i++;
            }
        }
        printf("decode average:      "
            " %.3e clocks %.3e sec %.3e bytes/sec\n",
            (double)tclk, (double)tclk/CLOCKS_PER_SEC,
            tbytes * (double)CLOCKS_PER_SEC /
                ((double)tclk > 0 ? (double)tclk : 1e-10));
        return 0;
    }

    for (n = 0; n < 17; n++) {
        if (n > 0) {
            for (i = 0; i < N; i++)
                mkrand(data[i], BLOCKSIZE);
        }

        rs_encode(N+3, BLOCKSIZE, data);

        for (i = 0; i < N+3; i++)
            memmove(orig[i], data[i], BLOCKSIZE);

        // One missing block
        for (i = 0; i < N+3; i++) {
            memset(data[i], 0, BLOCKSIZE);
            rs_decode1(N+3, BLOCKSIZE, i, data);
            if (compare(N+3, BLOCKSIZE, data, orig) != 0) {
                printf("FAILED: %d missing %d\n", n, i);
                return 1;
            }
        }

        // Two missing blocks
        for (i = 0; i < N+3; i++)
            for (j = 0; j < N+3; j++) {
                if (i == j) continue;
                memset(data[i], 0, BLOCKSIZE);
                memset(data[j], 0, BLOCKSIZE);
                rs_decode2(N+3, BLOCKSIZE, i, j, data);
                if (compare(N+3, BLOCKSIZE, data, orig) != 0) {
                    printf("FAILED: %d missing: %d %d\n", n, i, j);
                    return 1;
                }
            }

        // Three missing blocks
        for (i = 0; i < N+3; i++)
            for (j = 0; j < N+3; j++) {
                if (i == j) continue;
                for (k = 0; k < N+3; k++) {
                    if (i == k || j == k) continue;
                    memset(data[i], 0, BLOCKSIZE);
                    memset(data[j], 0, BLOCKSIZE);
                    memset(data[k], 0, BLOCKSIZE);
                    rs_decode3(N+3, BLOCKSIZE, i, j, k, data);
                    if (compare(N+3, BLOCKSIZE, data, orig) != 0) {
                        printf("FAILED: %d missing %d %d %d\n", n, i, j, k);
                        return 1;
                    }
                }
            }
    }
    printf("PASS\n");
    return 0;
}
