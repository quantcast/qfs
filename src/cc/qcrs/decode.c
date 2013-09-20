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
 * \file decode.c
 * \brief Reed Solomon decoder.
 *
 *------------------------------------------------------------------------------
 */

#include <string.h>     /* for memset */

#include "rs.h"
#include "rs_table.h"
#include "prim.h"

/* Compute P syndrome over data[?][i]. */
static v16
P(v16 **data, int n, int i)
{
    int j;
    v16 p;

    p = data[n-1][i];
    for (j = n-2; j >= 0; j--)
        p ^= data[j][i];
    return p;
}

/* Compute Q syndrome over data[?][i]. */
static v16
Q(v16 **data, int n, int i)
{
    int j;
    v16 q;

    q = data[n-1][i];
    for (j = n-2; j >= 0; j--)
        q = mul2(q) ^ data[j][i];
    return q;
}

/* Compute R syndrome over data[?][i]. */
static v16
R(v16 **data, int n, int i)
{
    int j;
    v16 r;

    r = data[n-1][i];
    for (j = n-2; j >= 0; j--)
        r = mul2(mul2(r)) ^ data[j][i];
    return r;
}

static v16
mulby(uint8_t x, v16 v)
{
#ifdef LIBRS_USE_NEON

#define uint8x16_to_8x8x2(v) ((uint8x8x2_t) { vget_low_u8(v), vget_high_u8(v) })

    v16 lo, hi;

    lo = v & VEC16(0x0f);
    hi = vshrq_n_u8(v, 4);
    lo = vcombine_u8(
            vtbl2_u8(uint8x16_to_8x8x2(rs_nibmul[x].lo), vget_low_u8(lo)),
            vtbl2_u8(uint8x16_to_8x8x2(rs_nibmul[x].lo), vget_high_u8(lo)));
    hi = vcombine_u8(
            vtbl2_u8(uint8x16_to_8x8x2(rs_nibmul[x].hi), vget_low_u8(hi)),
            vtbl2_u8(uint8x16_to_8x8x2(rs_nibmul[x].hi), vget_high_u8(hi)));
    return lo ^ hi;

#elif defined(LIBRS_USE_SSSE3)

    v16 lo, hi;

    lo = v & VEC16(0x0f);
    hi = __builtin_ia32_psrawi128(v, 4);
    hi &= VEC16(0x0f);
    lo = __builtin_ia32_pshufb128(rs_nibmul[x].lo, lo);
    hi = __builtin_ia32_pshufb128(rs_nibmul[x].hi, hi);
    return lo ^ hi;

#else

    v16 vv = VEC16(0);

    while (x != 0) {
        if (x & 1)
            vv ^= v;
        x >>= 1;
        v = mul2(v);
    }
    return vv;

#endif
}

/* Recover data block x using P syndrome. */
static void
rs_decode1p(int n, int blocksize, int x, v16 **data)
{
    int i;

    memset(data[x], 0, blocksize);
    for (i = 0; i < blocksize/sizeof(v16); i++)
        data[x][i] = P(data, n, i) ^ data[n][i];
}

/* Recover data block x using Q syndrome. */
static void
rs_decode1q(int n, int blocksize, int x, v16 **data)
{
    int i;

    memset(data[x], 0, blocksize);
    for (i = 0; i < blocksize/sizeof(v16); i++)
        data[x][i] = mulby(rs_r1Q[x], Q(data, n, i) ^ data[n+1][i]);
}

/* Recover data block x using R syndrome. */
static void
rs_decode1r(int n, int blocksize, int x, v16 **data)
{
    int i;

    memset(data[x], 0, blocksize);
    for (i = 0; i < blocksize/sizeof(v16); i++)
        data[x][i] = mulby(rs_r1R[x], R(data, n, i) ^ data[n+2][i]);
}

static void
rs_encode_if_requested(int nblocks, int blocksize, void **data)
{
    if (data[nblocks - 1] && data[nblocks - 2] && data[nblocks - 3])
        rs_encode(nblocks, blocksize, data);
}

/*
 * Reed-Solomon n+3 decoder.
 * Missing block `x'.
 */
void
rs_decode1(int nblocks, int blocksize, int x, void **data)
{
    int n;

    n = nblocks - 3;

    /* If missing a syndrome, just recompute it. */
    if (x >= n) {
        rs_encode_if_requested(nblocks, blocksize, data);
        return;
    }

    /* Missing data block, use P to recover. */
    rs_decode1p(n, blocksize, x, (v16**)data);
}

/* Recover data blocks x and y using syndromes P & Q. */
static void
rs_decode2pq(int n, int blocksize, int x, int y, v16 **data)
{
    int i;
    v16 pp, qq;
    const uint8_t* const c = rs_r2PQ[rs_r2map[x][y]];
#ifndef KFS_QCRS_DONT_INLINE
    v16** pd = data + n - 1;
#endif

    memset(data[x], 0, blocksize);
    memset(data[y], 0, blocksize);
    for (i = 0; i < blocksize/sizeof(v16); i++) {
#ifndef KFS_QCRS_DONT_INLINE
        pp = (*pd)[i];
        qq = pp;
        while (data <= --pd) {
            const v16 d = (*pd)[i];
            pp ^= d;
            qq = mul2(qq) ^ d;
        }
        pd = data + n + 1;
        qq ^= (*pd--)[i];
        pp ^= (*pd--)[i];
#else
        pp = P(data, n, i) ^ data[n][i];
        qq = Q(data, n, i) ^ data[n+1][i];
#endif
        data[x][i] = mulby(c[0], pp) ^ mulby(c[1], qq);
        data[y][i] = mulby(c[2], pp) ^ mulby(c[3], qq);
    }
}

/* Recover data blocks x and y using syndromes P & R. */
static void
rs_decode2pr(int n, int blocksize, int x, int y, v16 **data)
{
    int i;
    v16 pp, rr;
    const uint8_t* const c = rs_r2PR[rs_r2map[x][y]];
#ifndef KFS_QCRS_DONT_INLINE
    v16** pd = data + n - 1;
#endif

    memset(data[x], 0, blocksize);
    memset(data[y], 0, blocksize);
    for (i = 0; i < blocksize/sizeof(v16); i++) {
#ifndef KFS_QCRS_DONT_INLINE
        pp = (*pd)[i];
        rr = pp;
        while (data <= --pd) {
            const v16 d = (*pd)[i];
            pp ^= d;
            rr = mul2(mul2(rr)) ^ d;
        }
        pd = data + n + 2;
        rr ^= (*pd--)[i];
        pd--;
        pp ^= (*pd--)[i];
#else
        pp = P(data, n, i) ^ data[n][i];
        rr = R(data, n, i) ^ data[n+2][i];
#endif
        data[x][i] = mulby(c[0], pp) ^ mulby(c[1], rr);
        data[y][i] = mulby(c[2], pp) ^ mulby(c[3], rr);
    }
}

/* Recover data blocks x and y using syndromes Q & R. */
static void
rs_decode2qr(int n, int blocksize, int x, int y, v16 **data)
{
    int i;
    v16 qq, rr;
    const uint8_t* const c = rs_r2QR[rs_r2map[x][y]];
#ifndef KFS_QCRS_DONT_INLINE
    v16** pd = data + n - 1;
#endif

    memset(data[x], 0, blocksize);
    memset(data[y], 0, blocksize);
    for (i = 0; i < blocksize/sizeof(v16); i++) {
#ifndef KFS_QCRS_DONT_INLINE
        qq = (*pd)[i];
        rr = qq;
        while (data <= --pd) {
            const v16 d = (*pd)[i];
            qq = mul2(qq) ^ d;
            rr = mul2(mul2(rr)) ^ d;
        }
        pd = data + n + 2;
        rr ^= (*pd--)[i];
        qq ^= (*pd--)[i];
        pd--;
#else
        qq = Q(data, n, i) ^ data[n+1][i];
        rr = R(data, n, i) ^ data[n+2][i];
#endif
        data[x][i] = mulby(c[0], qq) ^ mulby(c[1], rr);
        data[y][i] = mulby(c[2], qq) ^ mulby(c[3], rr);
    }
}

/*
 * Reed-Solomon n+3 decoder.
 * Missing blocks `x' and `y'.
 */
void
rs_decode2(int nblocks, int blocksize, int x, int y, void **idata)
{
    int n, tmp;
    v16 **data = (v16**)idata;

    if (x > y) { tmp = x; x = y; y = tmp; }

    n = nblocks - 3;

    /* Both x & y are syndromes: recompute. */
    if (x >= n) {
        rs_encode_if_requested(nblocks, blocksize, idata);
        return;
    }

    /* x is a data block, y is a syndrome. */
    if (y == n) {   /* P */
        rs_decode1q(n, blocksize, x, data);
        rs_encode_if_requested(nblocks, blocksize, idata);
        return;
    }
    if (y == n+1 || y == n+2) { /* Q or R */
        rs_decode1p(n, blocksize, x, data);
        rs_encode_if_requested(nblocks, blocksize, idata);
        return;
    }

    /* Otherwise, x & y are both data blocks; use P & Q */
    rs_decode2pq(n, blocksize, x, y, data);
}

/* Recover data blocks x, y, & z using syndromes P, Q & R. */
static void
rs_decode3pqr(int n, int blocksize, int x, int y, int z, v16 **data)
{
    int i;
    v16 pp, qq, rr;
    const uint8_t* const c = rs_r3[rs_r3map[x][y][z]];
#ifndef KFS_QCRS_DONT_INLINE
    v16** pd = data + n - 1;
#endif

    memset(data[x], 0, blocksize);
    memset(data[y], 0, blocksize);
    memset(data[z], 0, blocksize);
    for (i = 0; i < blocksize/sizeof(v16); i++) {
#ifndef KFS_QCRS_DONT_INLINE
        pp = (*pd)[i];
        qq = pp;
        rr = pp;
        while (data <= --pd) {
            const v16 d = (*pd)[i];
            pp ^= d;
            qq = mul2(qq) ^ d;
            rr = mul2(mul2(rr)) ^ d;
        }
        pd = data + n + 2;
        rr ^= (*pd--)[i];
        qq ^= (*pd--)[i];
        pp ^= (*pd--)[i];
#else
        pp = P(data, n, i) ^ data[n][i];
        qq = Q(data, n, i) ^ data[n+1][i];
        rr = R(data, n, i) ^ data[n+2][i];
#endif
        data[x][i] = mulby(c[0], pp) ^ mulby(c[1], qq) ^ mulby(c[2], rr);
        data[y][i] = mulby(c[3], pp) ^ mulby(c[4], qq) ^ mulby(c[5], rr);
        data[z][i] = mulby(c[6], pp) ^ mulby(c[7], qq) ^ mulby(c[8], rr);
    }
}

/*
 * Reed-Solomon n+3 decoder.
 * Missing blocks `x', `y', and `z'.
 */
void
rs_decode3(int nblocks, int blocksize, int x, int y, int z, void **idata)
{
    int n, tmp;
    v16 **data = (v16**)idata;

    if (x > y) { tmp = x; x = y; y = tmp; }
    if (x > z) { tmp = x; x = z; z = tmp; }
    if (y > z) { tmp = y; y = z; z = tmp; }

    n = nblocks - 3;

    /* All of x, y, & z are syndromes: recompute. */
    if (x >= n) {
        rs_encode_if_requested(nblocks, blocksize, idata);
        return;
    }

    /* x is a data block, y & z are syndromes. */
    if (y == n && z == n+1) {
        rs_decode1r(n, blocksize, x, data);
        rs_encode_if_requested(nblocks, blocksize, idata);
        return;
    }
    if (y == n && z == n+2) {
        rs_decode1q(n, blocksize, x, data);
        rs_encode_if_requested(nblocks, blocksize, idata);
        return;
    }
    if (y == n+1 && z == n+2) {
        rs_decode1p(n, blocksize, x, data);
        rs_encode_if_requested(nblocks, blocksize, idata);
        return;
    }

    /* x & y are data blocks, z is a syndrome. */
    if (z == n) {   /* P */
        rs_decode2qr(n, blocksize, x, y, data);
        rs_encode_if_requested(nblocks, blocksize, idata);
        return;
    }
    if (z == n+1) { /* Q */
        rs_decode2pr(n, blocksize, x, y, data);
        rs_encode_if_requested(nblocks, blocksize, idata);
        return;
    }
    if (z == n+2) { /* R */
        rs_decode2pq(n, blocksize, x, y, data);
        rs_encode_if_requested(nblocks, blocksize, idata);
        return;
    }

    /* Otherwise, x, y & x are all data blocks; use P, Q, & R*/
    rs_decode3pqr(n, blocksize, x, y, z, data);
}
