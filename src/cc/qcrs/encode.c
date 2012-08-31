/*---------------------------------------------------------- -*- Mode: C -*-----
 * $Id$
 *
 * Created 2010/07/24
 * Author: Dan Adkins
 *
 * Copyright 2010-2012 Quantcast Corp.
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
 * \file encode.c
 * \brief Reed Solomon encoder.
 *
 *------------------------------------------------------------------------------
 */

#include <assert.h>
#include "rs.h"
#include "prim.h"

/*
 * Reed-Solomon n+3 encoder.
 * nblocks is `n' data blocks plus 3 syndrome blocks.  blocksize _must_
 * be a multiple of 16.  data contains pointers to blocks.  The first
 * n are input data blocks.  The last 3 are the P, Q, and R syndromes.
 */
void
rs_encode(int nblocks, int blocksize, void **idata)
{
    int i, j, n;
    v16 *p, *q, *r, **data = (v16**)idata;

    assert(nblocks > 3);
    assert(blocksize % 16 == 0);
    n = nblocks - 3;  // # data blocks
    p = data[n];
    q = data[n+1];
    r = data[n+2];
    for (i = 0; i < blocksize/sizeof(v16); i++) {
        p[i] = q[i] = r[i] = data[n-1][i];
        for (j = n-2; j >= 0; j--) {
            p[i] ^= data[j][i];
            q[i] = mul2(q[i]) ^ data[j][i];
            r[i] = mul2(mul2(r[i])) ^ data[j][i];
        }
    }
}
