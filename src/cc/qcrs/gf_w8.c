/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_w8.c
 *
 * Routines for 8-bit Galois fields
 */

/*
 * C. Tian Comment 2014: removed most of the functions using alternative computation options; 
                         only default methods in GF(8) left; these procedures need not be changed for thread safety.
 */

#include "gf_int.h"
#include <stdio.h>
#include <stdlib.h>

#define GF_FIELD_WIDTH (8)
#define GF_FIELD_SIZE       (1 << GF_FIELD_WIDTH)
#define GF_HALF_SIZE       (1 << (GF_FIELD_WIDTH/2))
#define GF_MULT_GROUP_SIZE       GF_FIELD_SIZE-1

#define GF_BASE_FIELD_WIDTH (4)
#define GF_BASE_FIELD_SIZE       (1 << GF_BASE_FIELD_WIDTH)



/* Don't change the order of these relative to gf_w8_half_table_data */

struct gf_w8_default_data {
  uint8_t     high[GF_FIELD_SIZE][GF_HALF_SIZE];
  uint8_t     low[GF_FIELD_SIZE][GF_HALF_SIZE];
  uint8_t     divtable[GF_FIELD_SIZE][GF_FIELD_SIZE];
  uint8_t     multtable[GF_FIELD_SIZE][GF_FIELD_SIZE];
};

struct gf_w8_half_table_data {
  uint8_t     high[GF_FIELD_SIZE][GF_HALF_SIZE];
  uint8_t     low[GF_FIELD_SIZE][GF_HALF_SIZE];
};

struct gf_w8_single_table_data {
  uint8_t     divtable[GF_FIELD_SIZE][GF_FIELD_SIZE];
  uint8_t     multtable[GF_FIELD_SIZE][GF_FIELD_SIZE];
};


static
inline
uint32_t gf_w8_divide_from_inverse (gf_t *gf, uint32_t a, uint32_t b)
{
  b = gf->inverse.w32(gf, b);
  return gf->multiply.w32(gf, a, b);
}


static
inline
uint32_t gf_w8_euclid (gf_t *gf, uint32_t b)
{
  uint32_t e_i, e_im1, e_ip1;
  uint32_t d_i, d_im1, d_ip1;
  uint32_t y_i, y_im1, y_ip1;
  uint32_t c_i;

  if (b == 0) return -1;
  e_im1 = ((gf_internal_t *) (gf->scratch))->prim_poly;
  e_i = b;
  d_im1 = 8;
  for (d_i = d_im1; ((1 << d_i) & e_i) == 0; d_i--) ;
  y_i = 1;
  y_im1 = 0;

  while (e_i != 1) {

    e_ip1 = e_im1;
    d_ip1 = d_im1;
    c_i = 0;

    while (d_ip1 >= d_i) {
      c_i ^= (1 << (d_ip1 - d_i));
      e_ip1 ^= (e_i << (d_ip1 - d_i));
      if (e_ip1 == 0) return 0;
      while ((e_ip1 & (1 << d_ip1)) == 0) d_ip1--;
    }

    y_ip1 = y_im1 ^ gf->multiply.w32(gf, c_i, y_i);
    y_im1 = y_i;
    y_i = y_ip1;

    e_im1 = e_i;
    d_im1 = d_i;
    e_i = e_ip1;
    d_i = d_ip1;
  }

  return y_i;
}

static
gf_val_32_t gf_w8_extract_word(gf_t *gf, void *start, int bytes, int index)
{
  uint8_t *r8;

  r8 = (uint8_t *) start;
  return r8[index];
}

static
inline
uint32_t gf_w8_matrix (gf_t *gf, uint32_t b)
{
  return gf_bitmatrix_inverse(b, 8, ((gf_internal_t *) (gf->scratch))->prim_poly);
}



/* ------------------------------------------------------------
IMPLEMENTATION: FULL_TABLE:

JSP: Kevin wrote this, and I'm converting it to my structure.
*/

static
  gf_val_32_t
gf_w8_table_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_w8_single_table_data *ftd;

  ftd = (struct gf_w8_single_table_data *) ((gf_internal_t *) gf->scratch)->private;
  return (ftd->multtable[a][b]);
}

static
  gf_val_32_t
gf_w8_table_divide(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_w8_single_table_data *ftd;

  ftd = (struct gf_w8_single_table_data *) ((gf_internal_t *) gf->scratch)->private;
  return (ftd->divtable[a][b]);
}

static
  gf_val_32_t
gf_w8_default_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_w8_default_data *ftd;

  ftd = (struct gf_w8_default_data *) ((gf_internal_t *) gf->scratch)->private;
  return (ftd->multtable[a][b]);
}

static
  gf_val_32_t
gf_w8_default_divide(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{

  struct gf_w8_default_data *ftd;
  
  ftd = (struct gf_w8_default_data *) ((gf_internal_t *) gf->scratch)->private;
  return (ftd->divtable[a][b]);  
}


static
  void
gf_w8_table_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  int i;
  uint8_t lv, b, c;
  uint8_t *s8, *d8;
  struct gf_w8_single_table_data *ftd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  ftd = (struct gf_w8_single_table_data *) ((gf_internal_t *) gf->scratch)->private;
  s8 = (uint8_t *) src;
  d8 = (uint8_t *) dest;

  if (xor) {
    for (i = 0; i < bytes; i++) {
      d8[i] ^= ftd->multtable[s8[i]][val];
    }
  } else {
    for (i = 0; i < bytes; i++) {
      d8[i] = ftd->multtable[s8[i]][val];
    }
  }
}

static
  void
gf_w8_split_multiply_region_sse(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
#ifdef INTEL_SSSE3
  uint8_t *s8, *d8, *bh, *bl, *sptr, *dptr, *top;
  __m128i  tbl, loset, t1, r, va, mth, mtl;
  uint64_t altable[4];
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

  mth = _mm_loadu_si128 ((__m128i *)(bh));
  mtl = _mm_loadu_si128 ((__m128i *)(bl));
  loset = _mm_set1_epi8 (0x0f);

  if (xor) {
    while (sptr < (uint8_t *) rd.s_top) {
      va = _mm_load_si128 ((__m128i *)(sptr));
      t1 = _mm_and_si128 (loset, va);
      r = _mm_shuffle_epi8 (mtl, t1);
      va = _mm_srli_epi64 (va, 4);
      t1 = _mm_and_si128 (loset, va);
      r = _mm_xor_si128 (r, _mm_shuffle_epi8 (mth, t1));
      va = _mm_load_si128 ((__m128i *)(dptr));
      r = _mm_xor_si128 (r, va);
      _mm_store_si128 ((__m128i *)(dptr), r);
      dptr += 16;
      sptr += 16;
    }
  } else {
    while (sptr < (uint8_t *) rd.s_top) {
      va = _mm_load_si128 ((__m128i *)(sptr));
      t1 = _mm_and_si128 (loset, va);
      r = _mm_shuffle_epi8 (mtl, t1);
      va = _mm_srli_epi64 (va, 4);
      t1 = _mm_and_si128 (loset, va);
      r = _mm_xor_si128 (r, _mm_shuffle_epi8 (mth, t1));
      _mm_store_si128 ((__m128i *)(dptr), r);
      dptr += 16;
      sptr += 16;
    }
  }

  gf_do_final_region_alignment(&rd);
#endif
}


/* ------------------------------------------------------------
IMPLEMENTATION: SHIFT:

JSP: The world's dumbest multiplication algorithm.  I only
include it for completeness.  It does have the feature that it requires no
extra memory.  
 */

static
inline
  uint32_t
gf_w8_shift_multiply (gf_t *gf, uint32_t a8, uint32_t b8)
{
  uint16_t product, i, pp, a, b;
  gf_internal_t *h;

  a = a8;
  b = b8;
  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;

  product = 0;

  for (i = 0; i < GF_FIELD_WIDTH; i++) { 
    if (a & (1 << i)) product ^= (b << i);
  }
  for (i = (GF_FIELD_WIDTH*2-2); i >= GF_FIELD_WIDTH; i--) {
    if (product & (1 << i)) product ^= (pp << (i-GF_FIELD_WIDTH)); 
  }
  return product;
}


static
void
gf_w8_multiply_region_from_single(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int
    xor)
{
  gf_region_data rd;
  uint8_t *s8;
  uint8_t *d8;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 1);
  gf_do_initial_region_alignment(&rd);

  s8 = (uint8_t *) rd.s_start;
  d8 = (uint8_t *) rd.d_start;

  if (xor) {
    while (d8 < ((uint8_t *) rd.d_top)) {
      *d8 ^= gf->multiply.w32(gf, val, *s8);
      d8++;
      s8++;
    }
  } else {
    while (d8 < ((uint8_t *) rd.d_top)) {
      *d8 = gf->multiply.w32(gf, val, *s8);
      d8++;
      s8++;
    }
  }
  gf_do_final_region_alignment(&rd);
}



/* ------------------------------------------------------------
IMPLEMENTATION: FULL_TABLE:
 */
static
  void
gf_w8_split_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  unsigned long uls, uld;
  int i;
  uint8_t lv, b, c;
  uint8_t *s8, *d8;
  struct gf_w8_half_table_data *htd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  htd = (struct gf_w8_half_table_data *) ((gf_internal_t *) gf->scratch)->private;
  s8 = (uint8_t *) src;
  d8 = (uint8_t *) dest;

  if (xor) {
    for (i = 0; i < bytes; i++) {
      d8[i] ^= (htd->high[val][s8[i]>>4] ^ htd->low[val][s8[i]&0xf]);
    }
  } else {
    for (i = 0; i < bytes; i++) {
      d8[i] = (htd->high[val][s8[i]>>4] ^ htd->low[val][s8[i]&0xf]);
    }
  }
}

/* JSP: This is disgusting, but it is what it is.  If there is no SSE,
   then the default is equivalent to single table.  If there is SSE, then
   we use the "gf_w8_default_data" which is a hybrid of SPLIT & TABLE. */
   
static
int gf_w8_table_init(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_w8_single_table_data *ftd = NULL;
  //struct gf_w8_double_table_data *dtd = NULL;
  //struct gf_w8_double_table_lazy_data *ltd = NULL;
  struct gf_w8_default_data *dd = NULL;
  int a, b, c, prod, scase, issse;

  h = (gf_internal_t *) gf->scratch;

  issse = 0;
#ifdef INTEL_SSSE3
  issse = 1;
#endif

  if (h->mult_type == GF_MULT_DEFAULT && issse) {
    dd = (struct gf_w8_default_data *)h->private;
    scase = 3;
    bzero(dd->high, sizeof(uint8_t) * GF_FIELD_SIZE * GF_HALF_SIZE);
    bzero(dd->low, sizeof(uint8_t) * GF_FIELD_SIZE * GF_HALF_SIZE);
    bzero(dd->divtable, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);
    bzero(dd->multtable, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);
  } else if (h->mult_type == GF_MULT_DEFAULT || 
             h->region_type == 0 || (h->region_type & GF_REGION_CAUCHY)) {
    ftd = (struct gf_w8_single_table_data *)h->private;
    bzero(ftd->divtable, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);
    bzero(ftd->multtable, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);
    scase = 0;
  } else {
    fprintf(stderr, "Internal error in gf_w8_table_init\n");
    exit(0);
  }

  for (a = 1; a < GF_FIELD_SIZE; a++) {
    for (b = 1; b < GF_FIELD_SIZE; b++) {
      prod = gf_w8_shift_multiply(gf,a,b);
      switch (scase) {
        case 0: 
          ftd->multtable[a][b] = prod;
          ftd->divtable[prod][b] = a;
          break;        
        case 3:
          dd->multtable[a][b] = prod;
          dd->divtable[prod][b] = a;
          if ((b & 0xf) == b) { dd->low[a][b] = prod; }
          if ((b & 0xf0) == b) { dd->high[a][b>>4] = prod; }
          break;
      }
    }
  }

  gf->inverse.w32 = NULL; /* Will set from divide */

  switch (scase) {
    case 0: 
      gf->divide.w32 = gf_w8_table_divide;
      gf->multiply.w32 = gf_w8_table_multiply;
      gf->multiply_region.w32 = gf_w8_table_multiply_region;
      break;    
    case 3:
#ifdef INTEL_SSSE3
      gf->divide.w32 = gf_w8_default_divide;
      gf->multiply.w32 = gf_w8_default_multiply;
      gf->multiply_region.w32 = gf_w8_split_multiply_region_sse;
#endif
      break;
  }
  return 1;
}


/* ------------------------------------------------------------
   General procedures.
   You don't need to error check here on in init, because it's done
   for you in gf_error_check().
 */

int gf_w8_scratch_size(int mult_type, int region_type, int divide_type, int arg1, int arg2)
{
  switch(mult_type)
  {
    case GF_MULT_DEFAULT:
#ifdef INTEL_SSSE3
      return sizeof(gf_internal_t) + sizeof(struct gf_w8_default_data) + 64;
#endif
      return sizeof(gf_internal_t) + sizeof(struct gf_w8_single_table_data) + 64;
    case GF_MULT_TABLE:
      if (region_type == GF_REGION_DEFAULT) {
        return sizeof(gf_internal_t) + sizeof(struct gf_w8_single_table_data) + 64;
      }      
      return 0;
      break;        
    default:
      return 0;
  }
  return 0;
}

int gf_w8_init(gf_t *gf)
{
  gf_internal_t *h, *h_base;

  h = (gf_internal_t *) gf->scratch;

  /* Allen: set default primitive polynomial / irreducible polynomial if needed */

  if (h->prim_poly == 0) {
      h->prim_poly = 0x11d;
  }
  if (h->mult_type != GF_MULT_COMPOSITE) { 
    h->prim_poly |= 0x100;
  }

  gf->multiply.w32 = NULL;
  gf->divide.w32 = NULL;
  gf->inverse.w32 = NULL;
  gf->multiply_region.w32 = NULL;
  gf->extract_word.w32 = gf_w8_extract_word;

  if (gf_w8_table_init(gf) == 0) return 0; // CT: only do default initialization 


  if (gf->divide.w32 == NULL) {
    gf->divide.w32 = gf_w8_divide_from_inverse;
    if (gf->inverse.w32 == NULL) gf->inverse.w32 = gf_w8_euclid;
  }

  if (gf->multiply_region.w32 == NULL) {
    gf->multiply_region.w32 = gf_w8_multiply_region_from_single;
  }

  return 1;
}


/* Inline setup functions */

uint8_t *gf_w8_get_mult_table(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_w8_default_data *ftd;
  struct gf_w8_single_table_data *std;

  h = (gf_internal_t *) gf->scratch;
  if (gf->multiply.w32 == gf_w8_default_multiply) {
    ftd = (struct gf_w8_default_data *) h->private;
    return (uint8_t *) ftd->multtable;
  } else if (gf->multiply.w32 == gf_w8_table_multiply) {
    std = (struct gf_w8_single_table_data *) h->private;
    return (uint8_t *) std->multtable;
  }
  return NULL;
}

uint8_t *gf_w8_get_div_table(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_w8_default_data *ftd;
  struct gf_w8_single_table_data *std;

  h = (gf_internal_t *) gf->scratch;
  if (gf->multiply.w32 == gf_w8_default_multiply) {
    ftd = (struct gf_w8_default_data *) ((gf_internal_t *) gf->scratch)->private;
    return (uint8_t *) ftd->divtable;
  } else if (gf->multiply.w32 == gf_w8_table_multiply) {
    std = (struct gf_w8_single_table_data *) ((gf_internal_t *) gf->scratch)->private;
    return (uint8_t *) std->divtable;
  }
  return NULL;
}
