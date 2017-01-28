/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_w32.h
 *
 * Defines and data structures for 32-bit Galois fields
 */

#ifndef GF_COMPLETE_GF_W32_H
#define GF_COMPLETE_GF_W32_H

#include <stdint.h>

#define GF_FIELD_WIDTH (32)
#define GF_FIRST_BIT (1 << 31)

#define GF_BASE_FIELD_WIDTH (16)
#define GF_BASE_FIELD_SIZE       (1 << GF_BASE_FIELD_WIDTH)
#define GF_BASE_FIELD_GROUP_SIZE  GF_BASE_FIELD_SIZE-1
#define GF_MULTBY_TWO(p) (((p) & GF_FIRST_BIT) ? (((p) << 1) ^ h->prim_poly) : (p) << 1)

struct gf_split_2_32_lazy_data {
    uint32_t      tables[16][4];
    uint32_t      last_value;
};

struct gf_w32_split_8_8_data {
    uint32_t      tables[7][256][256];
    uint32_t      region_tables[4][256];
    uint32_t      last_value;
};

struct gf_w32_group_data {
    uint32_t *reduce;
    uint32_t *shift;
    int      tshift;
    uint64_t rmask;
    uint32_t *memory;
};

struct gf_split_16_32_lazy_data {
    uint32_t      tables[2][(1<<16)];
    uint32_t      last_value;
};

struct gf_split_8_32_lazy_data {
    uint32_t      tables[4][256];
    uint32_t      last_value;
};

struct gf_split_4_32_lazy_data {
    uint32_t      tables[8][16];
    uint32_t      last_value;
};

struct gf_w32_bytwo_data {
    uint64_t prim_poly;
    uint64_t mask1;
    uint64_t mask2;
};

struct gf_w32_composite_data {
  uint16_t *log;
  uint16_t *alog;
};

void gf_w32_neon_split_init(gf_t *gf);

#endif /* GF_COMPLETE_GF_W32_H */
