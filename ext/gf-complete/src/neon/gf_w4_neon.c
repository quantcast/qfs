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
 * gf_w4_neon.c
 *
 * Neon routines for 4-bit Galois fields
 *
 */

#include "gf_int.h"
#include <stdio.h>
#include <stdlib.h>
#include "gf_w4.h"

static
gf_val_32_t
gf_w4_neon_clm_multiply (gf_t *gf, gf_val_32_t a4, gf_val_32_t b4)
{
  gf_val_32_t rv = 0;
  poly8x8_t       result, prim_poly;
  poly8x8_t       a, b, w;
  uint8x8_t       v;
  gf_internal_t * h = gf->scratch;

  a =  vdup_n_p8 (a4);
  b =  vdup_n_p8 (b4);

  prim_poly = vdup_n_p8 ((uint32_t)(h->prim_poly & 0x1fULL));

  /* Do the initial multiply */
  result = vmul_p8 (a, b);
  v = vshr_n_u8 (vreinterpret_u8_p8(result), 4);
  w = vmul_p8 (prim_poly, vreinterpret_p8_u8(v));
  result = vreinterpret_p8_u8 (veor_u8 (vreinterpret_u8_p8(result), vreinterpret_u8_p8(w)));

  /* Extracts 32 bit value from result. */
  rv = (gf_val_32_t)vget_lane_u8 (vreinterpret_u8_p8 (result), 0);

  return rv;
}

static inline void
neon_clm_multiply_region_from_single (gf_t *gf, uint8_t *s8, uint8_t *d8,
                                      gf_val_32_t val, uint8_t *d_end, int xor)
{
  gf_internal_t * h = gf->scratch;
  poly8x8_t       prim_poly;
  poly8x8_t       a, w, even, odd;
  uint8x8_t       b, c, v, mask;

  a         = vdup_n_p8 (val);
  mask      = vdup_n_u8 (0xf);
  prim_poly = vdup_n_p8 ((uint8_t)(h->prim_poly & 0x1fULL));

  while (d8 < d_end) {
    b = vld1_u8 (s8);

    even = vreinterpret_p8_u8 (vand_u8 (b, mask));
    odd  = vreinterpret_p8_u8 (vshr_n_u8 (b, 4));

    if (xor)
        c = vld1_u8 (d8);

    even = vmul_p8 (a, even);
    odd  = vmul_p8 (a, odd);

    v = vshr_n_u8 (vreinterpret_u8_p8(even), 4);
    w = vmul_p8 (prim_poly, vreinterpret_p8_u8(v));
    even = vreinterpret_p8_u8 (veor_u8 (vreinterpret_u8_p8(even), vreinterpret_u8_p8(w)));

    v = vshr_n_u8 (vreinterpret_u8_p8(odd), 4);
    w = vmul_p8 (prim_poly, vreinterpret_p8_u8(v));
    odd = vreinterpret_p8_u8 (veor_u8 (vreinterpret_u8_p8(odd), vreinterpret_u8_p8(w)));

    v = veor_u8 (vreinterpret_u8_p8 (even), vshl_n_u8 (vreinterpret_u8_p8 (odd), 4));

    if (xor)
      v = veor_u8 (c, v);

    vst1_u8 (d8, v);

    d8 += 8;
    s8 += 8;
  }
}


static void
gf_w4_neon_clm_multiply_region_from_single (gf_t *gf, void *src, void *dest,
                                            gf_val_32_t val, int bytes, int xor)
{
  gf_region_data rd;
  uint8_t *s8;
  uint8_t *d8;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 16);
  gf_do_initial_region_alignment(&rd);

  s8 = (uint8_t *) rd.s_start;
  d8 = (uint8_t *) rd.d_start;

  if (xor)
    neon_clm_multiply_region_from_single (gf, s8, d8, val, rd.d_top, 1);
  else
    neon_clm_multiply_region_from_single (gf, s8, d8, val, rd.d_top, 0);

  gf_do_final_region_alignment(&rd);
}

#ifndef ARCH_AARCH64
#define vqtbl1q_u8(tbl, v) vcombine_u8(vtbl2_u8(tbl, vget_low_u8(v)),   \
                                       vtbl2_u8(tbl, vget_high_u8(v)))
#endif

static
inline
void
w4_single_table_multiply_region_neon(gf_t *gf, uint8_t *src, uint8_t *dst,
                                     uint8_t * d_end, gf_val_32_t val, int xor)
{
  struct gf_single_table_data *std;
  uint8_t *base;
  uint8x16_t r, va, vh, vl, loset;

#ifdef ARCH_AARCH64
  uint8x16_t th, tl;
#else
  uint8x8x2_t th, tl;
#endif

  std = (struct gf_single_table_data *) ((gf_internal_t *) (gf->scratch))->private;
  base = (uint8_t *) std->mult;
  base += (val << GF_FIELD_WIDTH);

#ifdef ARCH_AARCH64
  tl = vld1q_u8 (base);
  th = vshlq_n_u8 (tl, 4);
#else
  tl.val[0] = vld1_u8 (base);
  tl.val[1] = vld1_u8 (base + 8);
  th.val[0] =  vshl_n_u8 (tl.val[0], 4);
  th.val[1] =  vshl_n_u8 (tl.val[1], 4);
#endif

  loset = vdupq_n_u8(0xf);

  while (dst < d_end) {
      va = vld1q_u8 (src);

      vh = vshrq_n_u8 (va, 4);
      vl = vandq_u8 (va, loset);

      if (xor)
        va = vld1q_u8 (dst);

      vh = vqtbl1q_u8 (th, vh);
      vl = vqtbl1q_u8 (tl, vl);

      r = veorq_u8 (vh, vl);

      if (xor)
        r = veorq_u8 (va, r);

      vst1q_u8 (dst, r);

    dst += 16;
    src += 16;
  }
}

static
void
gf_w4_single_table_multiply_region_neon(gf_t *gf, void *src, void *dest,
                                        gf_val_32_t val, int bytes, int xor)
{
  gf_region_data rd;
  uint8_t *sptr, *dptr, *top;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 16);
  gf_do_initial_region_alignment(&rd);

  sptr = rd.s_start;
  dptr = rd.d_start;
  top  = rd.d_top;

  if (xor)
      w4_single_table_multiply_region_neon(gf, sptr, dptr, top, val, 1);
  else
      w4_single_table_multiply_region_neon(gf, sptr, dptr, top, val, 0);

  gf_do_final_region_alignment(&rd);

}


int gf_w4_neon_cfm_init(gf_t *gf)
{
  // single clm multiplication probably pointless
  SET_FUNCTION(gf,multiply,w32,gf_w4_neon_clm_multiply)
  SET_FUNCTION(gf,multiply_region,w32,gf_w4_neon_clm_multiply_region_from_single)

  return 1;
}

void gf_w4_neon_single_table_init(gf_t *gf)
{
  SET_FUNCTION(gf,multiply_region,w32,gf_w4_single_table_multiply_region_neon)
}
