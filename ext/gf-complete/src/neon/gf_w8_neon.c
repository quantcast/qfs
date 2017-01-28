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
 * gf_w8_neon.c
 *
 * Neon optimized routines for 8-bit Galois fields
 *
 */

#include "gf_int.h"
#include "gf_w8.h"
#include <stdio.h>
#include <stdlib.h>

/* ARM NEON reducing macro for the carry free multiplication
 *   vmull_p8 is the carryless multiply operation. Here vshrn_n_u16 shifts
 *   the result to the right by 1 byte. This allows us to multiply
 *   the prim_poly by the leading bits of the result. We then xor the result
 *   of that operation back with the result. */
#define NEON_CFM_REDUCE(v, w, result, prim_poly, initial)               \
  do {								        \
    if (initial)                                                        \
      v = vshrn_n_u16 (vreinterpretq_u16_p16(result), 8);               \
    else                                                                \
      v = veor_u8 (v, vshrn_n_u16 (vreinterpretq_u16_p16(result), 8));  \
    w = vmull_p8 (prim_poly, vreinterpret_p8_u8(v));                    \
    result = vreinterpretq_p16_u16 (veorq_u16 (vreinterpretq_u16_p16(result), vreinterpretq_u16_p16(w))); \
  } while (0)

static
inline
gf_val_32_t
gf_w8_neon_clm_multiply_x (gf_t *gf, gf_val_32_t a8, gf_val_32_t b8, int x)
{
  gf_val_32_t rv = 0;
  poly8x8_t       a, b;
  uint8x8_t       v;
  poly16x8_t      result;
  poly8x8_t       prim_poly;
  poly16x8_t      w;
  gf_internal_t * h = gf->scratch;

  a =  vdup_n_p8 (a8);
  b =  vdup_n_p8 (b8);

  prim_poly = vdup_n_p8 ((uint32_t)(h->prim_poly & 0x1ffULL));

  /* Do the initial multiply */
  result = vmull_p8 (a, b);

  /* Ben: Do prim_poly reduction twice. We are guaranteed that we will only
     have to do the reduction at most twice, because (w-2)/z == 2. Where
     z is equal to the number of zeros after the leading 1 */
  NEON_CFM_REDUCE (v, w, result, prim_poly, 1);
  NEON_CFM_REDUCE (v, w, result, prim_poly, 0);
  if (x >= 3) {
    NEON_CFM_REDUCE (v, w, result, prim_poly, 0);
  }
  if (x >= 4) {
    NEON_CFM_REDUCE (v, w, result, prim_poly, 0);
  }
  /* Extracts 32 bit value from result. */
  rv = (gf_val_32_t)vget_lane_u8 (vmovn_u16 (vreinterpretq_u16_p16 (result)), 0);

  return rv;
}

#define CLM_MULTIPLY(x) \
static gf_val_32_t gf_w8_neon_clm_multiply_ ## x (gf_t *gf, gf_val_32_t a8, gf_val_32_t b8) \
{\
    return gf_w8_neon_clm_multiply_x (gf, a8, b8, x);\
}

CLM_MULTIPLY(2)
CLM_MULTIPLY(3)
CLM_MULTIPLY(4)

static inline void
neon_clm_multiply_region_from_single_x(gf_t *gf, uint8_t *s8, uint8_t *d8,
                                       gf_val_32_t val, uint8_t *d_end,
                                       int xor, int x)
{
  gf_internal_t * h = gf->scratch;
  poly8x8_t       a, b;
  uint8x8_t       c, v;
  poly16x8_t      result;
  poly8x8_t       prim_poly;
  poly16x8_t      w;

  a         = vdup_n_p8 (val);
  prim_poly = vdup_n_p8 ((uint8_t)(h->prim_poly & 0xffULL));

  while (d8 < d_end) {
    b = vld1_p8 ((poly8_t *) s8);

    if (xor)
        c = vld1_u8 (d8);

    result = vmull_p8 (a, b);

    NEON_CFM_REDUCE(v, w, result, prim_poly, 1);
    NEON_CFM_REDUCE (v, w, result, prim_poly, 0);
    if (x >= 3) {
      NEON_CFM_REDUCE (v, w, result, prim_poly, 0);
    }
    if (x >= 4) {
      NEON_CFM_REDUCE (v, w, result, prim_poly, 0);
    }
    v = vmovn_u16 (vreinterpretq_u16_p16 (result));
    if (xor)
      v = veor_u8 (c, v);

    vst1_u8 (d8, v);

    d8 += 8;
    s8 += 8;
  }
}

#define CLM_MULT_REGION(x)                                              \
static void                                                             \
gf_w8_neon_clm_multiply_region_from_single_ ## x (gf_t *gf, void *src,  \
                                                  void *dest,           \
                                                  gf_val_32_t val, int bytes, \
                                                  int xor)              \
{                                                                       \
  gf_region_data rd;                                                    \
  uint8_t *s8;                                                          \
  uint8_t *d8;                                                          \
                                                                        \
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }           \
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }       \
                                                                        \
  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 16);          \
  gf_do_initial_region_alignment(&rd);                                  \
  s8 = (uint8_t *) rd.s_start;                                          \
  d8 = (uint8_t *) rd.d_start;                                          \
                                                                        \
  if (xor)                                                              \
    neon_clm_multiply_region_from_single_x (gf, s8, d8, val, rd.d_top, 1, x); \
  else                                                                  \
    neon_clm_multiply_region_from_single_x (gf, s8, d8, val, rd.d_top, 0, x);\
  gf_do_final_region_alignment(&rd);                                    \
}

CLM_MULT_REGION(2)
CLM_MULT_REGION(3)
CLM_MULT_REGION(4)


int gf_w8_neon_cfm_init(gf_t *gf)
{
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;

  if ((0xe0 & h->prim_poly) == 0){
    SET_FUNCTION(gf,multiply,w32,gf_w8_neon_clm_multiply_2)
    SET_FUNCTION(gf,multiply_region,w32,gf_w8_neon_clm_multiply_region_from_single_2)
  }else if ((0xc0 & h->prim_poly) == 0){
    SET_FUNCTION(gf,multiply,w32,gf_w8_neon_clm_multiply_3)
    SET_FUNCTION(gf,multiply_region,w32,gf_w8_neon_clm_multiply_region_from_single_3)
  }else if ((0x80 & h->prim_poly) == 0){
    SET_FUNCTION(gf,multiply,w32,gf_w8_neon_clm_multiply_4)
    SET_FUNCTION(gf,multiply_region,w32,gf_w8_neon_clm_multiply_region_from_single_4)
  }else{
    return 0;
  }
  return 1;
}

#ifndef ARCH_AARCH64
#define vqtbl1q_u8(tbl, v) vcombine_u8(vtbl2_u8(tbl, vget_low_u8(v)),   \
                                       vtbl2_u8(tbl, vget_high_u8(v)))
#endif

static
void
gf_w8_split_multiply_region_neon(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint8_t *bh, *bl, *sptr, *dptr;
  uint8x16_t r, va, vh, vl, loset;
#ifdef ARCH_AARCH64
  uint8x16_t mth, mtl;
#else
  uint8x8x2_t mth, mtl;
#endif
  struct gf_w8_half_table_data *htd;
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  htd = (struct gf_w8_half_table_data *) ((gf_internal_t *) (gf->scratch))->private;

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 16);
  gf_do_initial_region_alignment(&rd);

  bh = (uint8_t *) htd->high;
  bh += (val << 4);
  bl = (uint8_t *) htd->low;
  bl += (val << 4);

  sptr = rd.s_start;
  dptr = rd.d_start;

#ifdef ARCH_AARCH64
  mth = vld1q_u8 (bh);
  mtl = vld1q_u8 (bl);
#else
  mth.val[0] = vld1_u8 (bh);
  mtl.val[0] = vld1_u8 (bl);
  mth.val[1] = vld1_u8 (bh + 8);
  mtl.val[1] = vld1_u8 (bl + 8);
#endif

  loset = vdupq_n_u8(0xf);

  if (xor) {
    while (sptr < (uint8_t *) rd.s_top) {
      va = vld1q_u8 (sptr);

      vh = vshrq_n_u8 (va, 4);
      vl = vandq_u8 (va, loset);
      va = vld1q_u8 (dptr);

      vh = vqtbl1q_u8 (mth, vh);
      vl = vqtbl1q_u8 (mtl, vl);

      r = veorq_u8 (vh, vl);

      vst1q_u8 (dptr, veorq_u8 (va, r));

      dptr += 16;
      sptr += 16;
    }
  } else {
    while (sptr < (uint8_t *) rd.s_top) {
      va = vld1q_u8 (sptr);

      vh = vshrq_n_u8 (va, 4);
      vl = vandq_u8 (va, loset);
#ifdef ARCH_AARCH64
      vh = vqtbl1q_u8 (mth, vh);
      vl = vqtbl1q_u8 (mtl, vl);
#else
      vh = vcombine_u8 (vtbl2_u8 (mth, vget_low_u8 (vh)),
			vtbl2_u8 (mth, vget_high_u8 (vh)));
      vl = vcombine_u8 (vtbl2_u8 (mtl, vget_low_u8 (vl)),
			vtbl2_u8 (mtl, vget_high_u8 (vl)));
#endif

      r = veorq_u8 (vh, vl);

      vst1q_u8(dptr, r);

      dptr += 16;
      sptr += 16;
    }
  }

  gf_do_final_region_alignment(&rd);
}


void gf_w8_neon_split_init(gf_t *gf)
{
  SET_FUNCTION(gf,multiply_region,w32,gf_w8_split_multiply_region_neon)
}
