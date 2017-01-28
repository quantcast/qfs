/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * Copyright (c) 2014: Janne Grunau <j@jannau.net>
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *  - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *  - Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 *  - Neither the name of the University of Tennessee nor the names of its
 *    contributors may be used to endorse or promote products derived
 *    from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * gf_w64_neon.c
 *
 * Neon routines for 64-bit Galois fields
 *
 */

#include "gf_int.h"
#include <stdio.h>
#include <stdlib.h>
#include "gf_w64.h"


#ifndef ARCH_AARCH64
#define vqtbl1q_u8(tbl, v) vcombine_u8(vtbl2_u8(tbl, vget_low_u8(v)),   \
                                       vtbl2_u8(tbl, vget_high_u8(v)))
#endif

static
inline
void
neon_w64_split_4_lazy_altmap_multiply_region(gf_t *gf, uint64_t *src,
                                             uint64_t *dst, uint64_t *d_end,
                                             uint64_t val, int xor)
{
  unsigned i, j, k;
  uint8_t btable[16];
#ifdef ARCH_AARCH64
  uint8x16_t tables[16][8];
#else
  uint8x8x2_t tables[16][8];
#endif
  uint8x16_t p[8], mask1, si;

  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  struct gf_split_4_64_lazy_data *ld = (struct gf_split_4_64_lazy_data *) h->private;

  for (i = 0; i < 16; i++) {
    for (j = 0; j < 8; j++) {
      for (k = 0; k < 16; k++) {
        btable[k] = (uint8_t) ld->tables[i][k];
        ld->tables[i][k] >>= 8;
      }
#ifdef ARCH_AARCH64
      tables[i][j] = vld1q_u8(btable);
#else
      tables[i][j].val[0] = vld1_u8(btable);
      tables[i][j].val[1] = vld1_u8(btable + 8);
#endif
    }
  }

  mask1 = vdupq_n_u8(0xf);

  while (dst < d_end) {

    if (xor) {
      for (i = 0; i < 8; i++)
        p[i] = vld1q_u8((uint8_t *) (dst + i * 2));
    } else {
      for (i = 0; i < 8; i++)
        p[i] = vdupq_n_u8(0);
    }

    i = 0;
    for (k = 0; k < 8; k++) {
      uint8x16_t v0 = vld1q_u8((uint8_t *) src);
      src += 2;

      si = vandq_u8(v0, mask1);
      for (j = 0; j < 8; j++) {
        p[j] = veorq_u8(p[j], vqtbl1q_u8(tables[i][j], si));
      }
      i++;
      si = vshrq_n_u8(v0, 4);
      for (j = 0; j < 8; j++) {
        p[j] = veorq_u8(p[j], vqtbl1q_u8(tables[i][j], si));
      }
      i++;

    }
    for (i = 0; i < 8; i++) {
      vst1q_u8((uint8_t *) dst, p[i]);
      dst += 2;
    }
  }
}

static
inline
void
neon_w64_split_4_lazy_multiply_region(gf_t *gf, uint64_t *src, uint64_t *dst,
                                      uint64_t *d_end, uint64_t val, int xor)
{
  unsigned i, j, k;
  uint8_t btable[16];
#ifdef ARCH_AARCH64
  uint8x16_t tables[16][8];
#else
  uint8x8x2_t tables[16][8];
#endif
  uint8x16_t p[8], mask1, si;
  uint64x2_t st[8];
  uint32x4x2_t s32[4];
  uint16x8x2_t s16[4];
  uint8x16x2_t s8[4];

  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  struct gf_split_4_64_lazy_data *ld = (struct gf_split_4_64_lazy_data *) h->private;

  for (i = 0; i < 16; i++) {
    for (j = 0; j < 8; j++) {
      for (k = 0; k < 16; k++) {
        btable[k] = (uint8_t) ld->tables[i][k];
        ld->tables[i][k] >>= 8;
      }
#ifdef ARCH_AARCH64
      tables[i][j] = vld1q_u8(btable);
#else
      tables[i][j].val[0] = vld1_u8(btable);
      tables[i][j].val[1] = vld1_u8(btable + 8);
#endif
    }
  }

  mask1 = vdupq_n_u8(0xf);

  while (dst < d_end) {

    for (k = 0; k < 8; k++) {
      st[k]  = vld1q_u64(src);
      src += 2;
      p[k] = vdupq_n_u8(0);
    }

    s32[0] = vuzpq_u32(vreinterpretq_u32_u64(st[0]),
                       vreinterpretq_u32_u64(st[1]));
    s32[1] = vuzpq_u32(vreinterpretq_u32_u64(st[2]),
                       vreinterpretq_u32_u64(st[3]));
    s32[2] = vuzpq_u32(vreinterpretq_u32_u64(st[4]),
                       vreinterpretq_u32_u64(st[5]));
    s32[3] = vuzpq_u32(vreinterpretq_u32_u64(st[6]),
                       vreinterpretq_u32_u64(st[7]));

    s16[0] = vuzpq_u16(vreinterpretq_u16_u32(s32[0].val[0]),
                       vreinterpretq_u16_u32(s32[1].val[0]));
    s16[1] = vuzpq_u16(vreinterpretq_u16_u32(s32[2].val[0]),
                       vreinterpretq_u16_u32(s32[3].val[0]));
    s16[2] = vuzpq_u16(vreinterpretq_u16_u32(s32[0].val[1]),
                       vreinterpretq_u16_u32(s32[1].val[1]));
    s16[3] = vuzpq_u16(vreinterpretq_u16_u32(s32[2].val[1]),
                       vreinterpretq_u16_u32(s32[3].val[1]));

    s8[0]  = vuzpq_u8(vreinterpretq_u8_u16(s16[0].val[0]),
                      vreinterpretq_u8_u16(s16[1].val[0]));
    s8[1]  = vuzpq_u8(vreinterpretq_u8_u16(s16[0].val[1]),
                      vreinterpretq_u8_u16(s16[1].val[1]));
    s8[2]  = vuzpq_u8(vreinterpretq_u8_u16(s16[2].val[0]),
                      vreinterpretq_u8_u16(s16[3].val[0]));
    s8[3]  = vuzpq_u8(vreinterpretq_u8_u16(s16[2].val[1]),
                      vreinterpretq_u8_u16(s16[3].val[1]));

    i = 0;
    for (k = 0; k < 8; k++) {
      si = vandq_u8(s8[k >> 1].val[k & 1], mask1);
      for (j = 0; j < 8; j++) {
        p[j] = veorq_u8(p[j], vqtbl1q_u8(tables[i][j], si));
      }
      i++;
      si = vshrq_n_u8(s8[k >> 1].val[k & 1], 4);
      for (j = 0; j < 8; j++) {
        p[j] = veorq_u8(p[j], vqtbl1q_u8(tables[i][j], si));
      }
      i++;
    }

    s8[0]  = vzipq_u8(p[0], p[1]);
    s8[1]  = vzipq_u8(p[2], p[3]);
    s8[2]  = vzipq_u8(p[4], p[5]);
    s8[3]  = vzipq_u8(p[6], p[7]);

    s16[0] = vzipq_u16(vreinterpretq_u16_u8(s8[0].val[0]),
                       vreinterpretq_u16_u8(s8[1].val[0]));
    s16[1] = vzipq_u16(vreinterpretq_u16_u8(s8[2].val[0]),
                       vreinterpretq_u16_u8(s8[3].val[0]));
    s16[2] = vzipq_u16(vreinterpretq_u16_u8(s8[0].val[1]),
                       vreinterpretq_u16_u8(s8[1].val[1]));
    s16[3] = vzipq_u16(vreinterpretq_u16_u8(s8[2].val[1]),
                       vreinterpretq_u16_u8(s8[3].val[1]));

    s32[0] = vzipq_u32(vreinterpretq_u32_u16(s16[0].val[0]),
                       vreinterpretq_u32_u16(s16[1].val[0]));
    s32[1] = vzipq_u32(vreinterpretq_u32_u16(s16[0].val[1]),
                       vreinterpretq_u32_u16(s16[1].val[1]));
    s32[2] = vzipq_u32(vreinterpretq_u32_u16(s16[2].val[0]),
                       vreinterpretq_u32_u16(s16[3].val[0]));
    s32[3] = vzipq_u32(vreinterpretq_u32_u16(s16[2].val[1]),
                       vreinterpretq_u32_u16(s16[3].val[1]));

    for (k = 0; k < 8; k ++) {
        st[k] = vreinterpretq_u64_u32(s32[k >> 1].val[k & 1]);
    }

    if (xor) {
      for (i = 0; i < 8; i++) {
        uint64x2_t t1 = vld1q_u64(dst);
        vst1q_u64(dst, veorq_u64(st[i], t1));
        dst += 2;
      }
    } else {
      for (i = 0; i < 8; i++) {
        vst1q_u64(dst, st[i]);
        dst += 2;
      }
    }

  }
}

static
void
gf_w64_neon_split_4_lazy_multiply_region(gf_t *gf, void *src, void *dest,
                                         uint64_t val, int bytes, int xor,
                                         int altmap)
{
  gf_internal_t *h;
  int i, j, k;
  uint64_t pp, v, *s64, *d64, *top;
  struct gf_split_4_64_lazy_data *ld;
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 128);
  gf_do_initial_region_alignment(&rd);

  s64 = (uint64_t *) rd.s_start;
  d64 = (uint64_t *) rd.d_start;
  top = (uint64_t *) rd.d_top;

  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;
  ld = (struct gf_split_4_64_lazy_data *) h->private;

  v = val;
  for (i = 0; i < 16; i++) {
    ld->tables[i][0] = 0;
    for (j = 1; j < 16; j <<= 1) {
      for (k = 0; k < j; k++) {
        ld->tables[i][k^j] = (v ^ ld->tables[i][k]);
      }
      v = (v & GF_FIRST_BIT) ? ((v << 1) ^ pp) : (v << 1);
    }
  }

  if (altmap) {
    if (xor)
      neon_w64_split_4_lazy_altmap_multiply_region(gf, s64, d64, top, val, 1);
    else
      neon_w64_split_4_lazy_altmap_multiply_region(gf, s64, d64, top, val, 0);
  } else {
    if (xor)
      neon_w64_split_4_lazy_multiply_region(gf, s64, d64, top, val, 1);
    else
      neon_w64_split_4_lazy_multiply_region(gf, s64, d64, top, val, 0);
  }

  gf_do_final_region_alignment(&rd);
}

static
void
gf_w64_split_4_64_lazy_multiply_region_neon(gf_t *gf, void *src, void *dest,
                                            uint64_t val, int bytes, int xor)
{
  gf_w64_neon_split_4_lazy_multiply_region(gf, src, dest, val, bytes, xor, 0);
}

static
void
gf_w64_split_4_64_lazy_altmap_multiply_region_neon(gf_t *gf, void *src,
                                                   void *dest, uint64_t val,
                                                   int bytes, int xor)
{
  gf_w64_neon_split_4_lazy_multiply_region(gf, src, dest, val, bytes, xor, 1);
}

void gf_w64_neon_split_init(gf_t *gf)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;

  if (h->region_type & GF_REGION_ALTMAP)
      SET_FUNCTION(gf,multiply_region,w64,gf_w64_split_4_64_lazy_altmap_multiply_region_neon)
  else
      SET_FUNCTION(gf,multiply_region,w64,gf_w64_split_4_64_lazy_multiply_region_neon)

}
