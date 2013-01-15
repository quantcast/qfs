/*---------------------------------------------------------- -*- Mode: C -*-----
 * $Id$
 *
 * Created 2010/07/24
 * Author: Dan Adkins
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
 * \file prim.h
 * \brief Vector op primitives for Reed Solomon encoder and decoder.
 *
 *------------------------------------------------------------------------------
 */

#ifndef RS_PRIM_H
#define RS_PRIM_H

#include <stdint.h>

#ifdef LIBRS_USE_NEON

#include <arm_neon.h>

typedef uint8x16_t v16;

static inline v16 VEC16(uint8_t x) { return vdupq_n_u8(x); }

#else

typedef uint8_t v16 __attribute__ ((vector_size (16)));

#define VEC16(x) ((v16){x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x})

#endif

static inline v16
mask(v16 v)
{
#ifdef LIBRS_USE_NEON
    return (v16)vcltq_s8((int8x16_t)v, vdupq_n_s8(0));
#elif defined(LIBRS_USE_SSE2) || defined(LIBRS_USE_SSSE3) &&  \
        ! defined(__clang__)
    /* clang has no corresponding builtin, but operator > */
    return __builtin_ia32_pcmpgtb128(VEC16(0), v);
#elif defined(__GNUC__) && \
        (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 7)) || \
        defined(__clang__)
    return VEC16(128) > v;
#else
    v16 res;
    int i;

    for (i = 0; i < 16; ++i)
        ((uint8_t*)&res)[i] = ((uint8_t*)&v)[i] < 128 ? 0 : 0xff;
    return res;
#endif
}

static inline v16
mul2(v16 v)
{
    v16 vv;

    vv = v + v;
    vv ^= mask(v) & VEC16(0x1d);
    return vv;
}

#endif
